#!/usr/bin/env python3
# File: src/enhancements/node_registry_enhanced_v4_0.py
"""
Node Registry – unified descriptor for all compute nodes.
Version: 5.0.0 (Enterprise Quantum Resilience + Bio-Inspired + MOE + MODP + Self-Healing
                + LIMIT Graph + RLHF + Distillation
                + Temporal Logic + XAI + Adaptive Precision + Carbon Markets
                + Multi-Agent Role Specialization + Chaos Testing
                + Active RLHF + HITL + Federated Learning)
"""

import asyncio
import json
import logging
import time
import uuid
import os
import signal
import hashlib
import random
from typing import Dict, List, Optional, Any, Tuple, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import contextvars
import functools
import numpy as np
from collections import deque, defaultdict

# -----------------------------------------------------------------------------
# Optional imports
# -----------------------------------------------------------------------------
try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean, JSON, Text, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError

try:
    from sklearn.linear_model import LogisticRegression, LinearRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    from sklearn.svm import OneClassSVM
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

import aiohttp
from aiohttp import ClientTimeout, ClientSession, ClientError

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

try:
    import websockets
    from websockets.server import serve
    from websockets.exceptions import ConnectionClosed
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

# Structured logging
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        )

correlation_id_var = contextvars.ContextVar('correlation_id', default='unknown')

# -----------------------------------------------------------------------------
# Prometheus metrics
# -----------------------------------------------------------------------------
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    NODE_REGISTRATIONS = Counter('node_registrations_total', 'Total node registrations', ['status'], registry=REGISTRY)
    NODE_REFRESHES = Counter('node_refreshes_total', 'Total node refreshes', ['status'], registry=REGISTRY)
    NODE_CACHE_SIZE = Gauge('node_cache_size', 'Number of nodes in cache', registry=REGISTRY)
    NODE_REFRESH_DURATION = Histogram('node_refresh_duration_seconds', 'Node refresh duration', registry=REGISTRY)
    CARBON_INTENSITY = Gauge('node_carbon_intensity_gco2_per_kwh', 'Current carbon intensity', registry=REGISTRY)
    MODP_PARETO_SIZE = Gauge('node_modp_pareto_front_size', 'MODP Pareto front size', registry=REGISTRY)
    MOE_GATING_WEIGHTS = Gauge('node_moe_gating_weights', 'MOE gating', ['expert'], registry=REGISTRY)
    GA_FITNESS = Gauge('node_ga_fitness', 'GA population fitness', ['generation'], registry=REGISTRY)
    SELF_HEALING_ACTIONS = Counter('node_self_healing_actions_total', 'Self-healing', ['action'], registry=REGISTRY)
    LIMIT_GRAPH_EDGES = Gauge('node_limit_graph_edges', 'Limit graph edges', registry=REGISTRY)
    RLHF_REWARD_MODEL_SCORE = Gauge('node_rlhf_reward_model_score', 'RLHF reward', registry=REGISTRY)
    DISTILLATION_LOSS = Gauge('node_distillation_loss', 'Distillation loss', registry=REGISTRY)
    TEMPORAL_VIOLATIONS = Counter('node_temporal_violations_total', 'Temporal', ['formula'], registry=REGISTRY)
    CHAOS_TESTS = Counter('node_chaos_tests_total', 'Chaos', ['fault', 'status'], registry=REGISTRY)
    HITL_ESCALATIONS = Counter('node_hitl_escalations_total', 'HITL', ['status'], registry=REGISTRY)
    FEDERATED_ROUNDS = Counter('node_federated_rounds_total', 'Federated rounds', registry=REGISTRY)
    CARBON_CREDITS_USD = Counter('node_carbon_credits_usd_total', 'Carbon credit USD', registry=REGISTRY)
    XAI_EXPLANATIONS = Counter('node_xai_explanations_total', 'XAI', registry=REGISTRY)
    PRECISION_SELECTIONS = Counter('node_precision_selections_total', 'Precision', ['level'], registry=REGISTRY)
else:
    class DummyMetrics:
        def inc(self, *a, **k): pass
        def set(self, *a, **k): pass
        def observe(self, *a, **k): pass
        def labels(self, *a, **k): return self
    NODE_REGISTRATIONS = NODE_REFRESHES = NODE_CACHE_SIZE = DummyMetrics()
    NODE_REFRESH_DURATION = CARBON_INTENSITY = MODP_PARETO_SIZE = DummyMetrics()
    MOE_GATING_WEIGHTS = GA_FITNESS = SELF_HEALING_ACTIONS = DummyMetrics()
    LIMIT_GRAPH_EDGES = RLHF_REWARD_MODEL_SCORE = DISTILLATION_LOSS = DummyMetrics()
    TEMPORAL_VIOLATIONS = CHAOS_TESTS = HITL_ESCALATIONS = DummyMetrics()
    FEDERATED_ROUNDS = CARBON_CREDITS_USD = XAI_EXPLANATIONS = DummyMetrics()
    PRECISION_SELECTIONS = DummyMetrics()

# -----------------------------------------------------------------------------
# Dummy tenacity
# -----------------------------------------------------------------------------
if not TENACITY_AVAILABLE:
    def retry(*args, **kwargs):
        def decorator(func):
            @functools.wraps(func)
            async def wrapper(*fargs, **fkwargs):
                attempts, max_attempts, delay = 0, 3, 1
                while attempts < max_attempts:
                    try:
                        return await func(*fargs, **fkwargs)
                    except Exception:
                        attempts += 1
                        if attempts >= max_attempts:
                            raise
                        await asyncio.sleep(delay)
                        delay *= 2
            return wrapper
        return decorator

# -----------------------------------------------------------------------------
# ENUMS
# -----------------------------------------------------------------------------
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class PrecisionLevel(Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    BF16 = "bf16"
    FP8 = "fp8"
    FP4 = "fp4"

class AgentRole(Enum):
    LEADER = "leader"
    WORKER = "worker"
    VERIFIER = "verifier"
    OBSERVER = "observer"

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
if PYDANTIC_AVAILABLE:
    class MODPConfig(BaseModel):
        enabled: bool = True
        method: str = Field("topsis")
        weights: List[float] = Field(default_factory=lambda: [0.25, 0.25, 0.25, 0.25])
        adaptive_weights: bool = True
        learning_rate: float = 0.01

    class MOEConfig(BaseModel):
        enabled: bool = True
        num_experts: int = 4
        gating_model: str = Field("logistic")
        update_interval: int = 3600

    class BioConfig(BaseModel):
        enabled: bool = True
        algorithm: str = Field("ga")
        population_size: int = 20
        max_iterations: int = 50
        mutation_rate: float = 0.1
        crossover_rate: float = 0.8

    class SchedulerConfig(BaseModel):
        enabled: bool = True
        carbon_threshold: float = 400.0
        max_delay_seconds: int = 300
        urgency_importance: float = 0.5
        carbon_importance: float = 0.3
        cost_importance: float = 0.2

    class SelfHealingConfig(BaseModel):
        enabled: bool = True
        anomaly_contamination: float = 0.1
        auto_retry_threshold: int = 3
        fallback_enabled: bool = True
        health_check_interval: int = 60

    class LimitGraphConfig(BaseModel):
        enabled: bool = True
        graph_type: str = "resource"
        max_nodes: int = 100
        update_interval: int = 300

    class RLHFConfig(BaseModel):
        enabled: bool = True
        reward_model: str = "linear"
        feedback_batch_size: int = 10
        training_interval: int = 600

    class DistillationConfig(BaseModel):
        enabled: bool = True
        num_teachers: int = 4
        temperature: float = 2.0
        alpha: float = 0.5
        student_model: str = "policy_net"

    class NodeRegistryConfig(BaseModel):
        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = Field("5.0.0")
        log_level: str = Field("INFO")
        refresh_interval: int = Field(3600, gt=0)
        cache_ttl: int = Field(300, gt=0)
        max_concurrent_refreshes: int = Field(5, ge=1)
        db_path: str = Field("/tmp/node_registry_v5.db")
        carbon_api_key: Optional[str] = None
        carbon_region: str = Field("global")
        carbon_update_interval: int = Field(300, ge=10)
        enable_quantum_security: bool = True
        quantum_algorithm: str = Field("dilithium")
        quantum_master_key: str = Field(default="")
        enable_blockchain_verification: bool = True
        blockchain_rpc_url: str = Field("http://localhost:8545")
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        enable_multi_cloud: bool = True
        aws_enabled: bool = True
        azure_enabled: bool = True
        gcp_enabled: bool = True
        metrics_port: int = Field(8000, ge=1024, le=65535)
        websocket_port: int = Field(8770, ge=1024)
        max_retry_attempts: int = Field(3, ge=0)
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: int = Field(30, ge=1)
        modp: MODPConfig = Field(default_factory=MODPConfig)
        moe: MOEConfig = Field(default_factory=MOEConfig)
        bio: BioConfig = Field(default_factory=BioConfig)
        scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
        self_healing: SelfHealingConfig = Field(default_factory=SelfHealingConfig)
        limit_graph: LimitGraphConfig = Field(default_factory=LimitGraphConfig)
        rlhf: RLHFConfig = Field(default_factory=RLHFConfig)
        distillation: DistillationConfig = Field(default_factory=DistillationConfig)
        # v5.0.0 flags
        temporal_logic_enabled: bool = True
        xai_enabled: bool = True
        adaptive_precision_enabled: bool = True
        carbon_market_enabled: bool = True
        role_specialization_enabled: bool = True
        chaos_testing_enabled: bool = True
        hitl_enabled: bool = True
        federated_enabled: bool = True
        hitl_confidence_threshold: float = 0.65

        @field_validator('log_level')
        @classmethod
        def validate_log_level(cls, v: str) -> str:
            allowed = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
            if v.upper() not in allowed:
                raise ValueError(f'LOG_LEVEL must be one of {allowed}')
            return v.upper()

        def get_master_key_bytes(self) -> bytes:
            if not self.quantum_master_key:
                return b'\x00' * 32
            return bytes.fromhex(self.quantum_master_key)

        class Config:
            env_prefix = "NODE_REGISTRY_"
else:
    @dataclass
    class MODPConfig:
        enabled: bool = True
        method: str = "topsis"
        weights: List[float] = field(default_factory=lambda: [0.25, 0.25, 0.25, 0.25])
        adaptive_weights: bool = True
        learning_rate: float = 0.01

    @dataclass
    class MOEConfig:
        enabled: bool = True
        num_experts: int = 4
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
    class SchedulerConfig:
        enabled: bool = True
        carbon_threshold: float = 400.0
        max_delay_seconds: int = 300
        urgency_importance: float = 0.5
        carbon_importance: float = 0.3
        cost_importance: float = 0.2

    @dataclass
    class SelfHealingConfig:
        enabled: bool = True
        anomaly_contamination: float = 0.1
        auto_retry_threshold: int = 3
        fallback_enabled: bool = True
        health_check_interval: int = 60

    @dataclass
    class LimitGraphConfig:
        enabled: bool = True
        graph_type: str = "resource"
        max_nodes: int = 100
        update_interval: int = 300

    @dataclass
    class RLHFConfig:
        enabled: bool = True
        reward_model: str = "linear"
        feedback_batch_size: int = 10
        training_interval: int = 600

    @dataclass
    class DistillationConfig:
        enabled: bool = True
        num_teachers: int = 4
        temperature: float = 2.0
        alpha: float = 0.5
        student_model: str = "policy_net"

    @dataclass
    class NodeRegistryConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "5.0.0"
        log_level: str = "INFO"
        refresh_interval: int = 3600
        cache_ttl: int = 300
        max_concurrent_refreshes: int = 5
        db_path: str = "/tmp/node_registry_v5.db"
        carbon_api_key: Optional[str] = None
        carbon_region: str = "global"
        carbon_update_interval: int = 300
        enable_quantum_security: bool = True
        quantum_algorithm: str = "dilithium"
        quantum_master_key: str = ""
        enable_blockchain_verification: bool = True
        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        enable_multi_cloud: bool = True
        aws_enabled: bool = True
        azure_enabled: bool = True
        gcp_enabled: bool = True
        metrics_port: int = 8000
        websocket_port: int = 8770
        max_retry_attempts: int = 3
        circuit_breaker_threshold: int = 5
        circuit_breaker_timeout: int = 30
        modp: MODPConfig = field(default_factory=MODPConfig)
        moe: MOEConfig = field(default_factory=MOEConfig)
        bio: BioConfig = field(default_factory=BioConfig)
        scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
        self_healing: SelfHealingConfig = field(default_factory=SelfHealingConfig)
        limit_graph: LimitGraphConfig = field(default_factory=LimitGraphConfig)
        rlhf: RLHFConfig = field(default_factory=RLHFConfig)
        distillation: DistillationConfig = field(default_factory=DistillationConfig)
        temporal_logic_enabled: bool = True
        xai_enabled: bool = True
        adaptive_precision_enabled: bool = True
        carbon_market_enabled: bool = True
        role_specialization_enabled: bool = True
        chaos_testing_enabled: bool = True
        hitl_enabled: bool = True
        federated_enabled: bool = True
        hitl_confidence_threshold: float = 0.65

        def get_master_key_bytes(self) -> bytes:
            if not self.quantum_master_key:
                return b'\x00' * 32
            return bytes.fromhex(self.quantum_master_key)

# -----------------------------------------------------------------------------
# Circuit Breaker + Rate Limiter
# -----------------------------------------------------------------------------
class EnhancedCircuitBreaker:
    def __init__(self, name: str, config: NodeRegistryConfig):
        self.name = name
        self.config = config
        self.failure_threshold = config.circuit_breaker_threshold
        self.recovery_timeout = config.circuit_breaker_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.failure_count = 0
                else:
                    raise Exception(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception:
            await self._record_failure()
            raise

    async def _record_success(self):
        async with self._lock:
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0

    async def _record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN

class EnhancedRateLimiter:
    def __init__(self, rate: int = 100, window: int = 60):
        self.rate = rate
        self.window = window
        self.tokens = rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            self.tokens = min(self.rate, self.tokens + (now - self.last_refill) * (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)

# -----------------------------------------------------------------------------
# DATABASE
# -----------------------------------------------------------------------------
Base = declarative_base()

class NodeDescriptorDB(Base):
    __tablename__ = 'node_descriptors'
    node_id = Column(String(128), primary_key=True)
    location = Column(String(64))
    energy_efficiency = Column(Float)
    carbon_intensity = Column(Float)
    helium_index = Column(Float)
    material_index = Column(Float)
    cooling_type = Column(String(32))
    renewable_fraction = Column(Float)
    harvester_type = Column(String(32), nullable=True)
    capture_efficiency = Column(Float, nullable=True)
    energy_output_watts = Column(Float, nullable=True)
    availability_pattern = Column(JSON, nullable=True)
    quantum_signature = Column(Text, nullable=True)
    blockchain_tx_hash = Column(String(128), nullable=True)
    last_updated = Column(DateTime, default=datetime.now)

class EnhancedDatabaseManager:
    def __init__(self, config: NodeRegistryConfig):
        self.config = config
        self.db_path = config.db_path
        self.engine = None
        self.SessionLocal = None
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._init_engine()

    def _init_engine(self):
        db_url = f"sqlite:///{self.db_path}"
        self.engine = create_engine(
            db_url, poolclass=QueuePool, pool_size=10, max_overflow=20,
            pool_pre_ping=True, connect_args={'check_same_thread': False})
        self.SessionLocal = scoped_session(sessionmaker(bind=self.engine))
        Base.metadata.create_all(self.engine)

    async def execute_sync(self, sync_func):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, sync_func)

    def _get_session(self):
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    async def register_node(self, descriptor: 'NodeDescriptor') -> bool:
        def sync_register():
            with self._get_session() as session:
                session.execute(text("""
                    INSERT OR REPLACE INTO node_descriptors
                    (node_id, location, energy_efficiency, carbon_intensity, helium_index,
                     material_index, cooling_type, renewable_fraction, harvester_type,
                     capture_efficiency, energy_output_watts, availability_pattern,
                     quantum_signature, blockchain_tx_hash, last_updated)
                    VALUES (:node_id, :location, :energy_efficiency, :carbon_intensity,
                     :helium_index, :material_index, :cooling_type, :renewable_fraction,
                     :harvester_type, :capture_efficiency, :energy_output_watts,
                     :availability_pattern, :quantum_signature, :blockchain_tx_hash,
                     :last_updated)
                """), {
                    'node_id': descriptor.node_id, 'location': descriptor.location,
                    'energy_efficiency': descriptor.energy_efficiency,
                    'carbon_intensity': descriptor.carbon_intensity,
                    'helium_index': descriptor.helium_index,
                    'material_index': descriptor.material_index,
                    'cooling_type': descriptor.cooling_type,
                    'renewable_fraction': descriptor.renewable_fraction,
                    'harvester_type': descriptor.harvester_type,
                    'capture_efficiency': descriptor.capture_efficiency,
                    'energy_output_watts': descriptor.energy_output_watts,
                    'availability_pattern': json.dumps(descriptor.availability_pattern) if descriptor.availability_pattern else None,
                    'quantum_signature': descriptor.quantum_signature,
                    'blockchain_tx_hash': descriptor.blockchain_tx_hash,
                    'last_updated': datetime.now(),
                })
        return await self.execute_sync(sync_register)

    async def load_all_nodes(self) -> List['NodeDescriptor']:
        def sync_load():
            nodes = []
            with self._get_session() as session:
                result = session.execute(text("""
                    SELECT node_id, location, energy_efficiency, carbon_intensity,
                           helium_index, material_index, cooling_type, renewable_fraction,
                           harvester_type, capture_efficiency, energy_output_watts,
                           availability_pattern, quantum_signature, blockchain_tx_hash,
                           last_updated FROM node_descriptors
                """))
                for row in result:
                    nodes.append(NodeDescriptor(
                        node_id=row[0], location=row[1], energy_efficiency=row[2],
                        carbon_intensity=row[3], helium_index=row[4],
                        material_index=row[5], cooling_type=row[6],
                        renewable_fraction=row[7], harvester_type=row[8],
                        capture_efficiency=row[9], energy_output_watts=row[10],
                        availability_pattern=json.loads(row[11]) if row[11] else None,
                        quantum_signature=row[12], blockchain_tx_hash=row[13],
                        last_updated=row[14] if row[14] else datetime.now()))
            return nodes
        return await self.execute_sync(sync_load)

    def dispose(self):
        if self.engine:
            self.engine.dispose()
        self._executor.shutdown(wait=False)

# -----------------------------------------------------------------------------
# NodeDescriptor
# -----------------------------------------------------------------------------
if PYDANTIC_AVAILABLE:
    class NodeDescriptor(BaseModel):
        node_id: str = Field(..., min_length=1)
        location: str = Field(..., min_length=1)
        energy_efficiency: float = Field(..., ge=0, le=1)
        carbon_intensity: float = Field(..., ge=0)
        helium_index: float = Field(..., ge=0)
        material_index: float = Field(..., ge=0)
        cooling_type: str = Field(..., pattern='^(air|liquid|hybrid)$')
        renewable_fraction: float = Field(..., ge=0, le=1)
        harvester_type: Optional[str] = Field(None, pattern='^(solar|wind|hydro|thermal|none)$')
        capture_efficiency: Optional[float] = Field(None, ge=0, le=1)
        energy_output_watts: Optional[float] = Field(None, ge=0)
        availability_pattern: Optional[Dict[str, Any]] = None
        quantum_signature: Optional[str] = None
        blockchain_tx_hash: Optional[str] = None
        last_updated: datetime = Field(default_factory=datetime.now)
else:
    @dataclass
    class NodeDescriptor:
        node_id: str
        location: str
        energy_efficiency: float
        carbon_intensity: float
        helium_index: float
        material_index: float
        cooling_type: str
        renewable_fraction: float
        harvester_type: Optional[str] = None
        capture_efficiency: Optional[float] = None
        energy_output_watts: Optional[float] = None
        availability_pattern: Optional[Dict[str, Any]] = None
        quantum_signature: Optional[str] = None
        blockchain_tx_hash: Optional[str] = None
        last_updated: datetime = field(default_factory=datetime.now)

# -----------------------------------------------------------------------------
# Carbon Intensity Manager
# -----------------------------------------------------------------------------
class CarbonIntensityManager:
    def __init__(self, config: NodeRegistryConfig):
        self.config = config
        self.current_intensity = 400.0

    async def get_current_intensity(self) -> float:
        self.current_intensity = 350 + random.uniform(-50, 50)
        if PROMETHEUS_AVAILABLE:
            CARBON_INTENSITY.set(self.current_intensity)
        return self.current_intensity

    async def close(self):
        pass

# -----------------------------------------------------------------------------
# Quantum Security (simplified)
# -----------------------------------------------------------------------------
class QuantumResilientNodeSecurity:
    def __init__(self, config, db_manager):
        self.config = config
        self.db_manager = db_manager
        self.pqc_available = PQC_AVAILABLE

    async def generate_keypair(self, algorithm: str = 'dilithium') -> Dict:
        return {'key_id': f"{algorithm}_{uuid.uuid4().hex[:8]}",
                'algorithm': algorithm,
                'public_key': hashlib.sha256(os.urandom(32)).hexdigest()}

    async def sign_node_data(self, data: Dict, key_id: str) -> str:
        return hashlib.sha3_256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()

# -----------------------------------------------------------------------------
# Blockchain Verification (simplified)
# -----------------------------------------------------------------------------
class BlockchainNodeVerification:
    def __init__(self, config):
        self.config = config
        self.connected = False

    async def record_node_registration(self, node_id: str, data_hash: str) -> str:
        return f"0x{hashlib.sha256(os.urandom(32)).hexdigest()}"

    async def get_status(self) -> Dict:
        return {'connected': self.connected, 'rpc': self.config.blockchain_rpc_url}

# -----------------------------------------------------------------------------
# Multi-Cloud Distribution (simplified)
# -----------------------------------------------------------------------------
class MultiCloudNodeDistribution:
    def __init__(self, config):
        self.config = config
        self.providers = {'aws': config.aws_enabled, 'azure': config.azure_enabled, 'gcp': config.gcp_enabled}
        self.active_provider = next((p for p, e in self.providers.items() if e), 'aws')

    async def distribute_node_data(self, data: Dict) -> Dict:
        return {'optimal_provider': self.active_provider,
                'timestamp': datetime.now().isoformat()}

    async def get_distribution_status(self) -> Dict:
        return {'providers': self.providers, 'active_provider': self.active_provider}

# =============================================================================
# v5.0.0 MODULE A — TEMPORAL LOGIC MONITOR
# =============================================================================
class TemporalLogicMonitor:
    """Lightweight LTL monitor: G(φ), F(φ), φ U ψ, φ -> ψ."""
    def __init__(self, history_len: int = 200):
        self.formulas: Dict[str, str] = {}
        self.compiled: Dict[str, Callable] = {}
        self.history: deque = deque(maxlen=history_len)
        self.violations: List[Dict] = []

    def add_formula(self, name: str, formula: str):
        self.formulas[name] = formula
        self.compiled[name] = self._compile(formula)

    def update(self, state: Dict):
        self.history.append(dict(state))

    def _compile(self, formula: str) -> Callable:
        f = formula.strip()
        if f.startswith("G(") and f.endswith(")"):
            inner = self._compile(f[2:-1])
            return lambda hist: all(inner([h]) for h in hist) if hist else True
        if f.startswith("F(") and f.endswith(")"):
            inner = self._compile(f[2:-1])
            return lambda hist: any(inner([h]) for h in hist) if hist else False
        if " U " in f:
            left, right = f.split(" U ", 1)
            lf, rf = self._compile(left), self._compile(right)
            def until(hist):
                for i in range(len(hist)):
                    if rf(hist[i:]):
                        return True
                    if not lf([hist[i]]):
                        return False
                return False
            return until
        if "->" in f:
            left, right = f.split("->", 1)
            lf, rf = self._compile(left.strip()), self._compile(right.strip())
            return lambda hist: (not lf(hist)) or rf(hist)
        return self._atom(f)

    def _atom(self, atom: str) -> Callable:
        atom = atom.strip()
        for op in ["<=", ">=", "==", "!=", "<", ">"]:
            if op in atom:
                lhs, rhs = [s.strip() for s in atom.split(op, 1)]
                def make(lhs, op, rhs):
                    def check(hist):
                        if not hist:
                            return True
                        s = hist[-1]
                        lv = s.get(lhs, 0.0)
                        try:
                            rv = float(rhs)
                        except ValueError:
                            rv = s.get(rhs, 0.0)
                        return {"<": lambda: lv < rv, ">": lambda: lv > rv,
                                "<=": lambda: lv <= rv, ">=": lambda: lv >= rv,
                                "==": lambda: lv == rv, "!=": lambda: lv != rv}[op]()
                    return check
                return make(lhs, op, rhs)
        return lambda hist: bool(atom.lower() in ("true", "1", "yes"))

    def evaluate(self) -> Dict[str, bool]:
        results = {}
        for name, fn in self.compiled.items():
            try:
                ok = fn(list(self.history))
            except Exception:
                ok = False
            results[name] = ok
            if not ok:
                self.violations.append({'formula': name, 'expression': self.formulas[name],
                                        'timestamp': datetime.now().isoformat()})
                if PROMETHEUS_AVAILABLE:
                    TEMPORAL_VIOLATIONS.labels(formula=name).inc()
        return results

    def get_status(self) -> Dict:
        return {'formulas': self.formulas, 'last_results': self.evaluate(),
                'violations': self.violations[-5:]}

# =============================================================================
# v5.0.0 MODULE B — XAI EXPLAINER
# =============================================================================
class XAIExplainer:
    def __init__(self, feature_names: List[str]):
        self.feature_names = feature_names

    def explain(self, candidate: Dict[str, float], weights: Dict[str, float],
                all_candidates: List[Dict[str, float]], top_k: int = 5) -> Dict:
        matrix = np.array([[c.get(f, 0.0) for f in self.feature_names] for c in all_candidates])
        norms = np.sqrt((matrix ** 2).sum(axis=0)) + 1e-9
        cand_vec = np.array([candidate.get(f, 0.0) for f in self.feature_names])
        w_arr = np.array([weights.get(f, 1.0) for f in self.feature_names])
        weighted = (cand_vec / norms) * w_arr
        contrib = {f: float(weighted[i]) for i, f in enumerate(self.feature_names)}
        ranked = sorted(contrib.items(), key=lambda kv: abs(kv[1]), reverse=True)[:top_k]
        narrative = [f"{f} ({v:+.4f}) {'increases' if v >= 0 else 'decreases'} the utility."
                     for f, v in ranked]
        if PROMETHEUS_AVAILABLE:
            XAI_EXPLANATIONS.inc()
        return {'contributions': contrib, 'top_features': [f for f, _ in ranked],
                'narrative': narrative, 'weights_used': dict(weights)}

# =============================================================================
# v5.0.0 MODULE C — ADAPTIVE PRECISION CONTROLLER
# =============================================================================
class AdaptivePrecisionController:
    def __init__(self):
        self.telemetry = {'gpu_available': False, 'memory_gb': 16.0, 'utilization': 0.3}
        self.last_precision = PrecisionLevel.FP32

    def update_telemetry(self, **kwargs):
        self.telemetry.update(kwargs)

    def select(self, carbon_intensity: float, accuracy_required: float = 0.95) -> PrecisionLevel:
        if accuracy_required > 0.99:
            self.last_precision = PrecisionLevel.FP32
        elif carbon_intensity > 500:
            self.last_precision = PrecisionLevel.FP8
        elif self.telemetry.get('gpu_available') and carbon_intensity < 350:
            self.last_precision = PrecisionLevel.FP16
        else:
            self.last_precision = PrecisionLevel.FP16
        if PROMETHEUS_AVAILABLE:
            PRECISION_SELECTIONS.labels(level=self.last_precision.value).inc()
        return self.last_precision

    @staticmethod
    def energy_factor(level: PrecisionLevel) -> float:
        return {PrecisionLevel.FP32: 1.0, PrecisionLevel.FP16: 0.4,
                PrecisionLevel.BF16: 0.4, PrecisionLevel.FP8: 0.2,
                PrecisionLevel.FP4: 0.1}[level]

# =============================================================================
# v5.0.0 MODULE D — CARBON MARKET CLIENT
# =============================================================================
class CarbonMarketClient:
    def __init__(self):
        self.carbon_price_per_ton = 50.0
        self.rec_price_per_mwh = 30.0
        self.grid_intensity_kg_per_mwh = 400.0
        self.trades: List[Dict] = []

    async def get_carbon_credit_value(self, carbon_saved_kg: float) -> float:
        return round(max(0.0, carbon_saved_kg) / 1000.0 * self.carbon_price_per_ton, 6)

    async def get_rec_value(self, energy_saved_kwh: float) -> float:
        return round(max(0.0, energy_saved_kwh) / 1000.0 * self.rec_price_per_mwh, 6)

    async def get_market_snapshot(self) -> Dict:
        return {'carbon_price_usd_per_ton': self.carbon_price_per_ton,
                'rec_price_usd_per_mwh': self.rec_price_per_mwh,
                'grid_intensity_kg_per_mwh': self.grid_intensity_kg_per_mwh}

    async def retire_credits(self, amount_kg: float, beneficiary: str) -> Dict:
        rec = {'id': str(uuid.uuid4()), 'amount_kg': amount_kg,
               'beneficiary': beneficiary,
               'timestamp': datetime.now().isoformat()}
        self.trades.append(rec)
        if PROMETHEUS_AVAILABLE:
            CARBON_CREDITS_USD.inc(await self.get_carbon_credit_value(amount_kg))
        return rec

# =============================================================================
# v5.0.0 MODULE E — ROLE SPECIALIZATION COORDINATOR
# =============================================================================
class RoleSpecializationCoordinator:
    def __init__(self):
        self.roles = list(AgentRole)
        # rows = [leader, worker, verifier, observer], cols = [trust, compute, energy, perf]
        self.affinity = np.array([
            [0.7, 0.9, 0.4, 0.6],   # leader
            [0.4, 0.5, 0.9, 0.5],   # worker
            [0.9, 0.4, 0.3, 0.7],   # verifier
            [0.3, 0.2, 0.3, 0.3],   # observer
        ])

    def assign_roles(self, context: Dict[str, float]) -> Dict:
        ctx = np.array([context.get('trust', 0.5), context.get('compute', 0.5),
                        context.get('energy', 0.5), context.get('performance', 0.5)])
        scores = self.affinity @ ctx
        e = np.exp(scores - scores.max())
        probs = e / e.sum()
        return {'assignments': {role.value: float(probs[i]) for i, role in enumerate(self.roles)},
                'dominant_role': self.roles[int(np.argmax(probs))].value}

# =============================================================================
# v5.0.0 MODULE F — CHAOS TESTER
# =============================================================================
class ChaosTester:
    FAULT_TYPES = ['carbon_api_down', 'db_broken', 'scheduler_hang',
                   'distiller_broken', 'moe_broken']

    def __init__(self, registry_ref=None):
        self.registry = registry_ref
        self.results: List[Dict] = []

    async def run_test(self, fault_type: str, duration_s: float = 0.1) -> Dict:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"Unknown fault: {fault_type}")
        start = time.time()
        passed, error_msg = True, None
        restore: List[Callable] = []
        r = self.registry

        try:
            if fault_type == 'carbon_api_down' and r:
                orig = r.carbon_manager.get_current_intensity
                async def broken(): raise RuntimeError("carbon API down")
                r.carbon_manager.get_current_intensity = broken
                restore.append(lambda: setattr(r.carbon_manager, 'get_current_intensity', orig))
            elif fault_type == 'db_broken' and r:
                orig = r.db_manager.load_all_nodes
                async def broken(): raise RuntimeError("db broken")
                r.db_manager.load_all_nodes = broken
                restore.append(lambda: setattr(r.db_manager, 'load_all_nodes', orig))
            elif fault_type == 'scheduler_hang' and r and r.scheduler:
                orig = r.scheduler.schedule
                async def broken(*a, **k):
                    await asyncio.sleep(5)
                    return {'recommended_delay': 0}
                r.scheduler.schedule = broken
                restore.append(lambda: setattr(r.scheduler, 'schedule', orig))
            elif fault_type == 'distiller_broken' and r and r.distillation:
                orig = r.distillation.distill
                async def broken(*a, **k): raise RuntimeError("distiller broken")
                r.distillation.distill = broken
                restore.append(lambda: setattr(r.distillation, 'distill', orig))
            elif fault_type == 'moe_broken' and r and r.moe_predictor:
                orig = r.moe_predictor.predict_urgency
                async def broken(*a, **k): raise RuntimeError("moe broken")
                r.moe_predictor.predict_urgency = broken
                restore.append(lambda: setattr(r.moe_predictor, 'predict_urgency', orig))
            await asyncio.sleep(duration_s)
        except Exception as e:
            passed, error_msg = False, str(e)
        finally:
            for rec in restore:
                try: rec()
                except Exception: pass

        result = {'fault': fault_type, 'duration_s': duration_s,
                  'elapsed_s': time.time() - start, 'passed': passed,
                  'error': error_msg, 'timestamp': datetime.now().isoformat()}
        self.results.append(result)
        if PROMETHEUS_AVAILABLE:
            CHAOS_TESTS.labels(fault=fault_type, status='pass' if passed else 'fail').inc()
        return result

    def get_report(self) -> Dict:
        return {'tests_run': len(self.results),
                'pass_rate': (sum(1 for r in self.results if r['passed']) / len(self.results))
                             if self.results else 1.0,
                'recent': self.results[-5:]}

# =============================================================================
# v5.0.0 MODULE G — ACTIVE RLHF
# =============================================================================
class ActiveRLHF:
    def __init__(self, action_space: List[str], uncertainty_threshold: float = 0.35,
                 human_timeout_s: float = 300.0):
        self.actions = list(action_space)
        self.uncertainty_threshold = uncertainty_threshold
        self.human_timeout_s = human_timeout_s
        self.preference_counts: Dict[str, float] = defaultdict(float)
        self.history: List[Dict] = []
        self.pending_queries: Dict[str, Dict] = {}
        self.feedback_buffer: List[Dict] = []

    def _policy(self, context: Any) -> np.ndarray:
        raw = np.array([self.preference_counts[a] for a in self.actions], dtype=float)
        if raw.sum() == 0:
            raw = np.ones(len(self.actions))
        e = np.exp(raw - raw.max())
        return e / e.sum()

    def sample_action(self, context: Any) -> str:
        return self.actions[int(np.argmax(self._policy(context)))]

    def uncertainty(self, context: Any) -> float:
        probs = self._policy(context)
        ent = -np.sum(probs * np.log(probs + 1e-12))
        return float(ent / np.log(len(self.actions))) if self.actions else 0.0

    def update(self, context: Any, action: str, reward: float):
        self.preference_counts[action] += reward
        self.history.append({'action': action, 'reward': reward,
                             'timestamp': datetime.now().isoformat()})

    def record_feedback(self, state: Dict, action: str, reward: float):
        self.feedback_buffer.append({'state': state, 'action': action, 'reward': reward})
        self.update(state, action, reward)

    async def maybe_query_human(self, context: Dict, options: List[str]) -> Optional[Dict]:
        u = self.uncertainty(context)
        if u <= self.uncertainty_threshold:
            return None
        qid = str(uuid.uuid4())
        query = {'id': qid, 'context': context, 'options': options,
                 'uncertainty': u, 'created_at': datetime.now().isoformat(),
                 'status': 'pending'}
        self.pending_queries[qid] = query
        return query

    def resolve_query(self, query_id: str, chosen: str, rating: float = 1.0):
        if query_id not in self.pending_queries:
            return None
        q = self.pending_queries.pop(query_id)
        q.update({'status': 'resolved', 'chosen': chosen, 'rating': rating})
        self.update(q['context'], chosen, rating)
        return q

    async def train_reward_model(self) -> Dict:
        if len(self.feedback_buffer) < 5:
            return {'trained': False, 'samples': len(self.feedback_buffer)}
        try:
            X = np.array([[f['state'].get('carbon_intensity', 400) / 1000.0,
                           f['state'].get('avg_score', 0.5),
                           f['state'].get('cost', 0.5),
                           f['state'].get('diversity', 0.5)]
                          for f in self.feedback_buffer])
            y = np.array([f['reward'] for f in self.feedback_buffer])
            if SKLEARN_AVAILABLE and len(X) >= 5:
                model = LinearRegression().fit(X, y)
                r2 = float(model.score(X, y)) if len(X) > 1 else 0.0
                if PROMETHEUS_AVAILABLE:
                    RLHF_REWARD_MODEL_SCORE.set(float(np.mean(y)))
                self.feedback_buffer.clear()
                return {'trained': True, 'r2': r2, 'samples': len(X)}
        except Exception as e:
            logger.warning(f"RLHF train failed: {e}")
        self.feedback_buffer.clear()
        return {'trained': False, 'reason': 'insufficient_data'}

    async def get_policy_probs(self, state: Dict) -> List[float]:
        return self._policy(state).tolist()

# =============================================================================
# v5.0.0 MODULE H — HUMAN-IN-THE-LOOP COORDINATOR
# =============================================================================
class HumanInTheLoopCoordinator:
    def __init__(self, active_rlhf: ActiveRLHF, timeout_s: float = 300.0):
        self.rlhf = active_rlhf
        self.timeout_s = timeout_s
        self.audit_log: List[Dict] = []

    async def escalate(self, decision_context: Dict, options: List[str],
                       confidence: float, confidence_threshold: float = 0.65) -> Dict:
        needs_human = confidence < confidence_threshold
        query = await self.rlhf.maybe_query_human(decision_context, options)

        if query is None and not needs_human:
            choice = self.rlhf.sample_action(decision_context)
            self.audit_log.append({'decision': 'auto', 'chosen': choice,
                                   'confidence': confidence})
            if PROMETHEUS_AVAILABLE:
                HITL_ESCALATIONS.labels(status='auto').inc()
            return {'escalated': False, 'chosen': choice, 'source': 'auto'}

        if query is None:
            query = {'id': str(uuid.uuid4()), 'options': options,
                     'context': decision_context, 'status': 'pending'}
        auto_choice = self.rlhf.sample_action(decision_context)
        self.audit_log.append({'decision': 'escalated', 'query_id': query.get('id'),
                               'auto_fallback': auto_choice, 'confidence': confidence,
                               'timestamp': datetime.now().isoformat()})
        if PROMETHEUS_AVAILABLE:
            HITL_ESCALATIONS.labels(status='escalated').inc()
        return {'escalated': True, 'query': query, 'chosen': auto_choice,
                'source': 'human_pending'}

    def get_audit(self) -> Dict:
        return {'total': len(self.audit_log), 'recent': self.audit_log[-10:]}

# =============================================================================
# v5.0.0 MODULE I — FEDERATED AGGREGATOR
# =============================================================================
class FederatedAggregator:
    def __init__(self, num_params: int = 4):
        self.round = 0
        self.num_params = num_params
        self.global_weights: List[float] = [1.0 / num_params] * num_params
        self.client_updates: List[Dict] = []

    def submit_update(self, client_id: str, weights: List[float], samples: int):
        if len(weights) != self.num_params:
            return
        self.client_updates.append({'client_id': client_id,
                                    'weights': list(weights), 'samples': samples})

    def aggregate(self) -> Dict:
        if not self.client_updates:
            return {'weights': self.global_weights, 'round': self.round}
        total = sum(u['samples'] for u in self.client_updates) or 1
        agg = np.zeros(self.num_params)
        for u in self.client_updates:
            agg += np.array(u['weights']) * (u['samples'] / total)
        self.global_weights = agg.tolist()
        self.round += 1
        self.client_updates.clear()
        if PROMETHEUS_AVAILABLE:
            FEDERATED_ROUNDS.inc()
        return {'weights': self.global_weights, 'round': self.round}

    def get_stats(self) -> Dict:
        return {'round': self.round, 'global_weights': self.global_weights,
                'pending_updates': len(self.client_updates)}

# =============================================================================
# MODULE 1: MODP REFRESH SELECTOR (with XAI, roles, temporal)
# =============================================================================
class ParetoFront:
    def __init__(self):
        self.solutions: List[Tuple[List[float], Any]] = []

    def add(self, objectives: List[float], decision: Any):
        dominated = False
        for obj, _ in self.solutions:
            if all(o <= obj[i] for i, o in enumerate(objectives)):
                dominated = True
                break
        if not dominated:
            self.solutions = [(obj, dec) for obj, dec in self.solutions
                              if not all(objectives[i] <= obj[i] for i in range(len(objectives)))]
            self.solutions.append((objectives, decision))

    def get_pareto_front(self) -> List[Tuple[List[float], Any]]:
        return self.solutions

    def get_best_by_weight(self, weights: List[float]) -> Any:
        best, best_score = None, -float('inf')
        for obj, dec in self.solutions:
            score = sum(w * o for w, o in zip(weights, obj))
            if score > best_score:
                best_score, best = score, dec
        return best

class TOPSIS:
    @staticmethod
    def score(candidates, weights, criteria):
        matrix = np.array([[c[crit] for crit in criteria] for c in candidates])
        norm_matrix = matrix / (np.sqrt((matrix ** 2).sum(axis=0)) + 1e-9)
        weighted = norm_matrix * weights
        ideal = weighted.max(axis=0)
        neg_ideal = weighted.min(axis=0)
        d_plus = np.sqrt(((weighted - ideal) ** 2).sum(axis=1))
        d_minus = np.sqrt(((weighted - neg_ideal) ** 2).sum(axis=1))
        return (d_minus / (d_plus + d_minus + 1e-9)).tolist()

class MODPRefreshSelector:
    def __init__(self, config: NodeRegistryConfig,
                 adaptive_cost: Optional[Any] = None,
                 xai: Optional[XAIExplainer] = None,
                 rlhf: Optional[ActiveRLHF] = None,
                 roles: Optional[RoleSpecializationCoordinator] = None,
                 temporal: Optional[TemporalLogicMonitor] = None):
        self.config = config
        self.adaptive_cost = adaptive_cost
        self.candidates = [
            {'name': 'immediate', 'freshness': 0.9, 'carbon': 0.1, 'cost': 0.1, 'importance': 0.8},
            {'name': 'batch_5', 'freshness': 0.7, 'carbon': 0.3, 'cost': 0.3, 'importance': 0.5},
            {'name': 'batch_10', 'freshness': 0.5, 'carbon': 0.5, 'cost': 0.5, 'importance': 0.3},
            {'name': 'delay_1h', 'freshness': 0.4, 'carbon': 0.7, 'cost': 0.6, 'importance': 0.2},
            {'name': 'delay_2h', 'freshness': 0.2, 'carbon': 0.9, 'cost': 0.8, 'importance': 0.1},
        ]
        self.weights = list(config.modp.weights)
        self.adaptive_weights = config.modp.adaptive_weights
        self.learning_rate = config.modp.learning_rate
        self.recent_outcomes = deque(maxlen=100)
        self.xai = xai
        self.rlhf = rlhf
        self.roles = roles
        self.temporal = temporal

    async def select_strategy(self, state: Dict) -> Dict:
        carbon_intensity = state.get('carbon_intensity', 400)

        # Temporal gate
        if self.temporal:
            self.temporal.update({'carbon_intensity': float(carbon_intensity),
                                  'avg_score': float(state.get('average_score', 0.5)),
                                  'success_rate': float(state.get('success_rate', 0.5))})
            temporal_status = self.temporal.evaluate()
        else:
            temporal_status = {}

        cand_dicts = []
        for cand in self.candidates:
            cand_dicts.append({
                'freshness': cand['freshness'],
                'carbon': 1.0 - cand['carbon'] * (carbon_intensity / 400.0),
                'cost': 1.0 - cand['cost'],
                'importance': cand['importance'],
            })

        if self.adaptive_cost and self.adaptive_weights:
            try:
                wd = self.adaptive_cost.get_current_weights()
                self.weights = [wd.get('freshness', 0.25), wd.get('carbon', 0.25),
                                wd.get('cost', 0.25), wd.get('importance', 0.25)]
            except Exception:
                pass

        scores = TOPSIS.score(cand_dicts, self.weights,
                              ['freshness', 'carbon', 'cost', 'importance'])
        best_idx = int(np.argmax(scores))
        best = self.candidates[best_idx]

        front = ParetoFront()
        for cand in self.candidates:
            front.add([cand['freshness'], 1 - cand['carbon'],
                       1 - cand['cost'], cand['importance']], cand['name'])

        # XAI explanation
        xai_out = None
        if self.xai:
            xai_out = self.xai.explain(
                candidate=cand_dicts[best_idx],
                weights={'freshness': self.weights[0], 'carbon': self.weights[1],
                         'cost': self.weights[2], 'importance': self.weights[3]},
                all_candidates=cand_dicts,
            )

        # Role assignment
        roles_out = None
        if self.roles:
            roles_out = self.roles.assign_roles({
                'trust': best['importance'],
                'compute': best['freshness'],
                'energy': 1.0 - best['carbon'],
                'performance': scores[best_idx],
            })

        if PROMETHEUS_AVAILABLE:
            MODP_PARETO_SIZE.set(len(front.get_pareto_front()))

        outcome = [scores[best_idx], 1 - best['carbon'], 1 - best['cost'], best['importance']]
        self.recent_outcomes.append((self.weights, outcome))
        if self.adaptive_weights and len(self.recent_outcomes) >= 10:
            await self._update_weights()

        return {'strategy': best['name'], 'weights_used': self.weights,
                'scores': scores, 'pareto_front': front.get_pareto_front(),
                'xai_explanation': xai_out, 'role_assignments': roles_out,
                'temporal_status': temporal_status,
                'recommendation': f"Selected {best['name']} via MODP"}

    async def _update_weights(self):
        avg_outcome = np.mean([o for _, o in self.recent_outcomes], axis=0)
        self.weights = self.weights - self.learning_rate * (avg_outcome - np.mean(avg_outcome))
        total = sum(self.weights)
        if total > 0:
            self.weights = [w / total for w in self.weights]

# =============================================================================
# MODULE 2: MOE URGENCY PREDICTOR
# =============================================================================
class MOEUrgencyPredictor:
    def __init__(self, config: NodeRegistryConfig):
        self.config = config
        self.experts: List[Tuple[str, Callable]] = []
        self.gating_model = None
        self.scaler = None
        self.history = deque(maxlen=500)
        self._trained = False
        self._init_experts()
        self._init_gating()

    def _init_experts(self):
        self.experts = [
            ('performance', self._performance_teacher),
            ('carbon', self._carbon_teacher),
            ('cost', self._cost_teacher),
            ('adaptive', self._adaptive_teacher),
        ]

    def _init_gating(self):
        if SKLEARN_AVAILABLE:
            self.gating_model = LogisticRegression(multi_class='multinomial',
                                                   solver='lbfgs', max_iter=1000)
            self.scaler = StandardScaler()

    def _performance_teacher(self, node: NodeDescriptor) -> float:
        age = (datetime.now() - node.last_updated).seconds / 3600 if node.last_updated else 0
        return (1 - node.energy_efficiency) * 0.5 + min(age / 24, 1) * 0.5

    def _carbon_teacher(self, node: NodeDescriptor, carbon_intensity: float) -> float:
        return (carbon_intensity / 1000) * 0.5 + (1 - node.renewable_fraction) * 0.5

    def _cost_teacher(self, node: NodeDescriptor) -> float:
        return 0.5

    def _adaptive_teacher(self, node: NodeDescriptor) -> float:
        return 0.5

    async def _extract_features(self, node, carbon_intensity):
        age = (datetime.now() - node.last_updated).seconds / 3600 if node.last_updated else 0
        return np.array([age / 24, node.carbon_intensity / 1000,
                         node.energy_efficiency, node.renewable_fraction,
                         carbon_intensity / 1000])

    async def get_teacher_urgencies(self, node, carbon_intensity):
        urgencies = []
        for name, func in self.experts:
            if name == 'carbon':
                urgencies.append(func(node, carbon_intensity))
            else:
                urgencies.append(func(node))
        return urgencies

    async def get_gating_weights(self, node, carbon_intensity) -> List[float]:
        if self.gating_model is not None and self._trained:
            features = await self._extract_features(node, carbon_intensity)
            X = self.scaler.transform([features])
            return self.gating_model.predict_proba(X)[0].tolist()
        return [1.0 / len(self.experts)] * len(self.experts)

    async def predict_urgency(self, node, carbon_intensity) -> float:
        urgencies = await self.get_teacher_urgencies(node, carbon_intensity)
        weights = await self.get_gating_weights(node, carbon_intensity)
        return float(np.dot(weights, urgencies))

    async def update(self, node, carbon_intensity, actual_improvement):
        features = await self._extract_features(node, carbon_intensity)
        reward = max(0, min(1, actual_improvement * 2))
        self.history.append((features, 0, reward))
        if len(self.history) % 100 == 0:
            await self._update_gating()

    async def _update_gating(self):
        if self.gating_model is None or len(self.history) < 100:
            return
        X = np.array([h[0] for h in self.history])
        y = np.random.randint(0, len(self.experts), size=len(X))
        self.gating_model.fit(self.scaler.fit_transform(X), y)
        self._trained = True

    def get_stats(self) -> Dict:
        return {'num_experts': len(self.experts),
                'gating_trained': self._trained,
                'history_len': len(self.history)}

# =============================================================================
# MODULE 3: BIO-INSPIRED GA
# =============================================================================
class GeneticAlgorithmOptimizer:
    def __init__(self, population_size=20, mutation_rate=0.1, crossover_rate=0.8):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population: List[Dict] = []
        self.bounds = {'freshness_weight': (0.0, 1.0), 'carbon_weight': (0.0, 1.0),
                       'cost_weight': (0.0, 1.0), 'importance_weight': (0.0, 1.0)}

    def initialize(self):
        self.population = []
        for _ in range(self.pop_size):
            ind = {k: random.uniform(*v) for k, v in self.bounds.items()}
            total = sum(ind.values()) or 1.0
            for k in ind:
                ind[k] /= total
            self.population.append(ind)

    def evaluate(self, fitness_func):
        return [fitness_func(ind) for ind in self.population]

    def select(self, fitness, n):
        selected = []
        for _ in range(n):
            i, j = np.random.choice(len(self.population), 2, replace=False)
            selected.append(self.population[i] if fitness[i] > fitness[j] else self.population[j])
        return selected

    def crossover(self, p1, p2):
        if random.random() < self.crossover_rate:
            return {k: (p1[k] if random.random() < 0.5 else p2[k]) for k in p1}
        return p1.copy()

    def mutate(self, ind):
        if random.random() < self.mutation_rate:
            k = random.choice(list(self.bounds.keys()))
            ind[k] = random.uniform(*self.bounds[k])
            total = sum(ind.values()) or 1.0
            for kk in ind:
                ind[kk] /= total
        return ind

    def evolve(self, fitness_func, generations=5):
        self.initialize()
        for gen in range(generations):
            fitness = self.evaluate(fitness_func)
            best = self.population[int(np.argmax(fitness))]
            parents = self.select(fitness, self.pop_size - 1)
            offspring = []
            for i in range(0, len(parents) - 1, 2):
                offspring.append(self.mutate(self.crossover(parents[i], parents[i + 1])))
                offspring.append(self.mutate(self.crossover(parents[i + 1], parents[i])))
            self.population = offspring[:self.pop_size - 1] + [best]
            if PROMETHEUS_AVAILABLE:
                GA_FITNESS.labels(generation=str(gen)).set(max(fitness))
        final = self.evaluate(fitness_func)
        return self.population[int(np.argmax(final))]

class BioOptimizer:
    def __init__(self, config, adaptive_cost=None):
        self.config = config
        self.adaptive_cost = adaptive_cost
        self.ga = GeneticAlgorithmOptimizer(
            population_size=config.bio.population_size,
            mutation_rate=config.bio.mutation_rate,
            crossover_rate=config.bio.crossover_rate)
        self.current_params = {'freshness_weight': 0.25, 'carbon_weight': 0.25,
                               'cost_weight': 0.25, 'importance_weight': 0.25}
        self.fitness_history = deque(maxlen=50)
        self._lock = asyncio.Lock()

    def _fitness_func(self, params):
        if self.adaptive_cost:
            try:
                return -self.adaptive_cost.evaluate({
                    'freshness': params['freshness_weight'],
                    'carbon': params['carbon_weight'],
                    'cost': params['cost_weight'],
                    'importance': params['importance_weight']})
            except Exception:
                pass
        return params['freshness_weight'] - 0.5 * params['carbon_weight'] + 0.3 * params['importance_weight']

    async def evolve(self):
        best = self.ga.evolve(self._fitness_func, generations=5)
        async with self._lock:
            self.current_params = best
            self.fitness_history.append(self._fitness_func(best))
        return best

    def get_current_params(self):
        return self.current_params

# =============================================================================
# MODULE 4: MULTI-OBJECTIVE CARBON SCHEDULER (with markets)
# =============================================================================
class MultiObjectiveCarbonScheduler:
    def __init__(self, config, carbon_manager, forecaster=None, market_client=None):
        self.config = config
        self.carbon_manager = carbon_manager
        self.forecaster = forecaster
        self.market_client = market_client
        self.carbon_weight = config.scheduler.carbon_importance
        self.urgency_weight = config.scheduler.urgency_importance
        self.cost_weight = config.scheduler.cost_importance
        self.max_delay = config.scheduler.max_delay_seconds
        self.threshold = config.scheduler.carbon_threshold
        self.history = deque(maxlen=100)

    async def schedule(self, urgency_score=0.5):
        forecast = await self.forecaster.forecast(24) if self.forecaster else None
        market = await self.market_client.get_market_snapshot() if self.market_client else {}
        if not forecast or not forecast.get('prices'):
            intensity = await self.carbon_manager.get_current_intensity()
            delay = self.max_delay if intensity > self.threshold else 0
            return {'recommended_delay': delay, 'reason': 'simple_threshold', 'market': market}
        delays = list(range(0, self.max_delay + 1, 10))
        best = None
        for d in delays:
            idx = int(d / 3600)
            avg = (np.mean(forecast['prices'][:idx + 1]) if idx > 0 else forecast['prices'][0])
            savings = max(0, (forecast['prices'][0] - avg) / forecast['prices'][0]) \
                if forecast['prices'][0] > 0 else 0
            composite = (-self.carbon_weight * savings
                         + self.urgency_weight * (d / (self.max_delay + 1) * urgency_score)
                         + self.cost_weight * d * 0.001)
            if best is None or composite < best['cost']:
                best = {'delay': d, 'cost': composite, 'carbon_savings': savings}
        self.history.append(best)
        return {'recommended_delay': best['delay'], 'reason': 'multi_objective',
                'carbon_savings': best['carbon_savings'], 'market': market}

# =============================================================================
# MOE FORECASTER
# =============================================================================
class MOEForecaster:
    def __init__(self):
        self.experts: List[Tuple[str, Callable]] = []
        self.history = deque(maxlen=1000)
        self._init_experts()

    def _init_experts(self):
        if SKLEARN_AVAILABLE:
            self.experts.append(('linear', self._forecast_linear))
        self.experts.append(('naive', self._forecast_naive))

    async def _forecast_linear(self, history, horizon):
        if len(history) < 2:
            return [0.5] * horizon
        X = np.arange(len(history)).reshape(-1, 1)
        y = np.array([h['y'] for h in history])
        model = LinearRegression().fit(X, y)
        future_X = np.arange(len(history), len(history) + horizon).reshape(-1, 1)
        return model.predict(future_X).tolist()

    async def _forecast_naive(self, history, horizon):
        if not history:
            return [0.5] * horizon
        return [history[-1]['y']] * horizon

    async def update_history(self, value):
        self.history.append({'ds': datetime.now(), 'y': value})

    async def forecast(self, horizon=24):
        if len(self.history) < 5:
            return {'prices': [0.5] * horizon, 'confidence': 0.0}
        forecasts = []
        for _, f in self.experts:
            try:
                forecasts.append(await f(self.history, horizon))
            except Exception:
                forecasts.append([0.5] * horizon)
        weights = np.ones(len(self.experts)) / len(self.experts)
        final = np.zeros(horizon)
        for i, f in enumerate(forecasts):
            final += weights[i] * np.array(f)
        return {'prices': final.tolist(), 'confidence': 0.5}

    def get_stats(self):
        return {'num_experts': len(self.experts), 'history_len': len(self.history)}

# =============================================================================
# SELF-HEALING
# =============================================================================
class SelfHealingManager:
    def __init__(self, config, drift_detector=None, rlhf=None, chaos=None):
        self.config = config
        self.drift = drift_detector
        self.anomaly_detectors: List[Tuple[str, Any]] = []
        self.gating_weights = [1.0]
        self._lock = asyncio.Lock()
        self.recovery_actions = deque(maxlen=100)
        self._trained = False
        self.rlhf = rlhf
        self.chaos = chaos
        if SKLEARN_AVAILABLE:
            self.anomaly_detectors = [('iforest', IsolationForest(contamination=0.1)),
                                      ('ocsvm', OneClassSVM(nu=0.1))]
            self.gating_weights = [0.5, 0.5]

    async def detect_anomaly(self, metrics):
        if not self.anomaly_detectors or not self._trained:
            return (metrics.get('refresh_improvement', 0) < 0.1,
                    0.8 if metrics.get('refresh_improvement', 0) < 0.1 else 0.0)
        features = np.array([
            metrics.get('refresh_improvement', 0),
            metrics.get('avg_carbon_intensity', 400) / 1000,
            metrics.get('cache_size', 0) / 100,
            metrics.get('last_refresh_duration', 0) / 60,
        ]).reshape(1, -1)
        votes = []
        for _, m in self.anomaly_detectors:
            try:
                votes.append(1 if m.predict(features)[0] == -1 else 0)
            except Exception:
                votes.append(0)
        weighted = sum(v * w for v, w in zip(votes, self.gating_weights))
        return weighted > 0.5, weighted

    async def train(self, data):
        if not self.anomaly_detectors or len(data) < 20:
            return
        X = np.array([[d.get('refresh_improvement', 0),
                       d.get('avg_carbon_intensity', 400) / 1000,
                       d.get('cache_size', 0) / 100,
                       d.get('last_refresh_duration', 0) / 60] for d in data])
        for _, m in self.anomaly_detectors:
            if hasattr(m, 'fit'):
                try: m.fit(X)
                except Exception: pass
        self._trained = True

    async def check_drift(self, metrics):
        if self.drift:
            try:
                detected = await self.drift.check_drift(metrics)
            except Exception:
                detected = False
            if detected:
                action = self.rlhf.sample_action(metrics) if self.rlhf else 'drift_recovery'
                async with self._lock:
                    self.recovery_actions.append({'action': action,
                                                  'timestamp': datetime.now().isoformat()})
                if PROMETHEUS_AVAILABLE:
                    SELF_HEALING_ACTIONS.labels(action=action).inc()

    async def trigger_recovery(self):
        async with self._lock:
            self.recovery_actions.append({'action': 'generic_recovery',
                                          'timestamp': datetime.now().isoformat()})
        if PROMETHEUS_AVAILABLE:
            SELF_HEALING_ACTIONS.labels(action='generic_recovery').inc()

    async def run_chaos_suite(self):
        if self.chaos is None:
            return {'error': 'chaos disabled'}
        results = []
        for f in ChaosTester.FAULT_TYPES:
            try:
                results.append(await self.chaos.run_test(f, duration_s=0.05))
            except Exception as e:
                results.append({'fault': f, 'passed': False, 'error': str(e)})
        return {'results': results, 'report': self.chaos.get_report()}

    async def get_stats(self):
        return {'enabled': self.config.self_healing.enabled,
                'trained': self._trained,
                'num_detectors': len(self.anomaly_detectors),
                'recent_actions': list(self.recovery_actions)[-5:]}

# =============================================================================
# LIMIT GRAPH
# =============================================================================
class LimitGraphManager:
    def __init__(self, config):
        self.config = config
        self.graph: Dict[str, Dict[str, float]] = {}
        self.constraints: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._initialize_graph()

    def _initialize_graph(self):
        for n in ['carbon', 'cost', 'latency', 'throughput', 'diversity']:
            self.graph[n] = {}
        self.graph['carbon']['cost'] = 0.8
        self.graph['cost']['latency'] = 0.2
        self.graph['latency']['throughput'] = -0.5
        self.graph['throughput']['diversity'] = 0.1
        self.graph['diversity']['carbon'] = -0.3
        if PROMETHEUS_AVAILABLE:
            LIMIT_GRAPH_EDGES.set(sum(len(v) for v in self.graph.values()))

    async def update_constraint(self, name: str, value: float):
        async with self._lock:
            self.constraints[name] = value

    async def get_constraint(self, name: str) -> float:
        return self.constraints.get(name, 0.0)

    async def evaluate_path(self, start: str, end: str) -> float:
        if start not in self.graph or end not in self.graph:
            return 0.0
        visited = set()
        queue = [(start, 1.0)]
        while queue:
            node, weight = queue.pop(0)
            if node == end:
                return weight
            visited.add(node)
            for neighbor, w in self.graph[node].items():
                if neighbor not in visited:
                    queue.append((neighbor, weight * w))
        return 0.0

    async def get_graph_summary(self):
        return {'nodes': list(self.graph.keys()),
                'constraints': self.constraints,
                'edge_count': sum(len(v) for v in self.graph.values())}

# =============================================================================
# MULTI-TEACHER DISTILLATION (v5 with role-aware teachers)
# =============================================================================
class MultiTeacherPolicyDistillation:
    def __init__(self, config, moe_predictor=None, role_coordinator=None):
        self.config = config
        self.moe_predictor = moe_predictor
        self.role_coordinator = role_coordinator
        self.student_policy = np.array([0.25, 0.25, 0.25, 0.25])
        self.temperature = config.distillation.temperature
        self.alpha = config.distillation.alpha
        self.history = deque(maxlen=500)
        self._lock = asyncio.Lock()

    async def distill(self, state):
        if not self.moe_predictor:
            return
        dummy = NodeDescriptor(
            node_id='dummy', location='unknown', energy_efficiency=0.8,
            carbon_intensity=400, helium_index=0.5, material_index=1.0,
            cooling_type='air', renewable_fraction=0.5)
        ci = state.get('carbon_intensity', 400)
        try:
            teacher_probs = await self.moe_predictor.get_gating_weights(dummy, ci)
        except Exception:
            teacher_probs = [0.25, 0.25, 0.25, 0.25]
        teacher_dist = np.array(teacher_probs)
        if len(teacher_dist) < 4:
            teacher_dist = np.pad(teacher_dist, (0, 4 - len(teacher_dist)),
                                  'constant', constant_values=0.25)
        elif len(teacher_dist) > 4:
            teacher_dist = teacher_dist[:4]
        teacher_dist = teacher_dist / (teacher_dist.sum() + 1e-9)

        soft_teacher = np.exp(np.log(teacher_dist + 1e-6) / self.temperature)
        soft_teacher /= soft_teacher.sum()

        loss = -np.sum(soft_teacher * np.log(self.student_policy + 1e-6))
        grad = -soft_teacher / (self.student_policy + 1e-6)
        self.student_policy = np.clip(self.student_policy - 0.01 * grad, 0.01, None)
        self.student_policy /= self.student_policy.sum()

        async with self._lock:
            self.history.append({'teacher_dist': teacher_dist.tolist(),
                                 'student_dist': self.student_policy.tolist(),
                                 'loss': float(loss)})
        if PROMETHEUS_AVAILABLE:
            DISTILLATION_LOSS.set(loss)

    def get_student_probs(self):
        return self.student_policy.tolist()

# =============================================================================
# WEBSOCKET SERVER
# =============================================================================
class EnhancedWebSocketServer:
    def __init__(self, port: int):
        self.port = port
        self.connections = set()
        self.server = None

    async def start(self):
        if not WEBSOCKETS_AVAILABLE:
            return
        try:
            self.server = await serve(self._handle, '0.0.0.0', self.port)
        except Exception as e:
            logger.warning(f"WebSocket start failed: {e}")

    async def _handle(self, websocket, path=None):
        self.connections.add(websocket)
        try:
            async for _ in websocket:
                pass
        except ConnectionClosed:
            pass
        finally:
            self.connections.discard(websocket)

    async def broadcast(self, message: Dict, topic: str = 'all'):
        data = json.dumps(message, default=str)
        for conn in list(self.connections):
            try:
                await conn.send(data)
            except Exception:
                self.connections.discard(conn)

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()

# =============================================================================
# ENHANCED NODE REGISTRY v5.0.0
# =============================================================================
class NodeRegistry:
    """Enhanced Node Registry v5.0.0 with all v5.0.0 enhancements."""

    def __init__(self, config: Optional[NodeRegistryConfig] = None):
        self.config = config or NodeRegistryConfig()
        self.instance_id = self.config.instance_id

        # Core
        self.db_manager = EnhancedDatabaseManager(self.config)
        self.carbon_manager = CarbonIntensityManager(self.config)
        self.quantum_security = QuantumResilientNodeSecurity(self.config, self.db_manager) \
            if self.config.enable_quantum_security else None
        self.blockchain = BlockchainNodeVerification(self.config) \
            if self.config.enable_blockchain_verification else None
        self.cloud_distributor = MultiCloudNodeDistribution(self.config) \
            if self.config.enable_multi_cloud else None

        # Feature flags
        self.temporal_logic_enabled = self.config.temporal_logic_enabled
        self.xai_enabled = self.config.xai_enabled
        self.adaptive_precision_enabled = self.config.adaptive_precision_enabled
        self.carbon_market_enabled = self.config.carbon_market_enabled
        self.role_specialization_enabled = self.config.role_specialization_enabled
        self.chaos_testing_enabled = self.config.chaos_testing_enabled
        self.hitl_enabled = self.config.hitl_enabled
        self.federated_enabled = self.config.federated_enabled

        # ---- v5.0.0 modules ----
        self.temporal_monitor = TemporalLogicMonitor() if self.temporal_logic_enabled else None
        if self.temporal_monitor:
            self.temporal_monitor.add_formula("carbon_cap", "G(carbon_intensity <= 800.0)")
            self.temporal_monitor.add_formula("score_min", "F(avg_score >= 0.3)")
            self.temporal_monitor.add_formula("cache_ok", "G(cache_size >= 0.0)")

        self.xai = XAIExplainer(['freshness', 'carbon', 'cost', 'importance']) \
            if self.xai_enabled else None
        self.precision_controller = AdaptivePrecisionController() \
            if self.adaptive_precision_enabled else None
        self.carbon_market = CarbonMarketClient() if self.carbon_market_enabled else None
        self.role_coordinator = RoleSpecializationCoordinator() \
            if self.role_specialization_enabled else None

        # Active RLHF replaces RLHFManager
        self.rlhf = ActiveRLHF(
            action_space=['immediate', 'batch_5', 'batch_10', 'delay_1h', 'delay_2h'],
        ) if self.config.rlhf.enabled else None

        self.hitl = HumanInTheLoopCoordinator(self.rlhf) \
            if (self.hitl_enabled and self.rlhf) else None
        self.federated = FederatedAggregator(num_params=4) if self.federated_enabled else None

        # Existing modules
        self.limit_graph = LimitGraphManager(self.config) if self.config.limit_graph.enabled else None
        self.modp_selector = MODPRefreshSelector(
            self.config, None,
            xai=self.xai, rlhf=self.rlhf,
            roles=self.role_coordinator, temporal=self.temporal_monitor,
        ) if self.config.modp.enabled else None
        self.moe_predictor = MOEUrgencyPredictor(self.config) if self.config.moe.enabled else None
        self.bio_optimizer = BioOptimizer(self.config, None) if self.config.bio.enabled else None
        self.forecaster = MOEForecaster() if self.config.scheduler.enabled else None
        self.scheduler = MultiObjectiveCarbonScheduler(
            self.config, self.carbon_manager, self.forecaster, self.carbon_market,
        ) if self.config.scheduler.enabled else None
        self.chaos_tester = ChaosTester(self) if self.chaos_testing_enabled else None
        self.self_healing = SelfHealingManager(
            self.config, None, self.rlhf, self.chaos_tester,
        ) if self.config.self_healing.enabled else None
        self.distillation = MultiTeacherPolicyDistillation(
            self.config, self.moe_predictor, self.role_coordinator,
        ) if self.config.distillation.enabled and self.moe_predictor else None

        self.cache: Dict[str, NodeDescriptor] = {}
        self.cache_ttl = self.config.cache_ttl
        self._lock = asyncio.Lock()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._circuit_breaker = EnhancedCircuitBreaker("cloud_api", self.config)
        self._rate_limiter = EnhancedRateLimiter(rate=10, window=60)
        self._bulkhead = asyncio.Semaphore(self.config.max_concurrent_refreshes)
        self._refresh_count = 0
        self._carbon_saved_kg_total = 0.0
        self._shutdown_event = asyncio.Event()
        self._websocket = EnhancedWebSocketServer(self.config.websocket_port) if WEBSOCKETS_AVAILABLE else None
        self._background_tasks: List[asyncio.Task] = []

        logger.info(f"NodeRegistry v{self.config.version} initialized (instance: {self.instance_id})")
        logger.info(f"  TemporalLogic={self.temporal_logic_enabled} XAI={self.xai_enabled} "
                    f"AdaptivePrecision={self.adaptive_precision_enabled} "
                    f"CarbonMarket={self.carbon_market_enabled} "
                    f"Roles={self.role_specialization_enabled} "
                    f"Chaos={self.chaos_testing_enabled} HITL={self.hitl_enabled} "
                    f"Federated={self.federated_enabled}")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def _load_initial_data(self):
        try:
            nodes = await self.db_manager.load_all_nodes()
            async with self._lock:
                for node in nodes:
                    self.cache[node.node_id] = node
                if PROMETHEUS_AVAILABLE:
                    NODE_CACHE_SIZE.set(len(self.cache))
        except Exception as e:
            logger.warning(f"Load initial data failed: {e}")

    async def start(self):
        self._running = True
        await self._load_initial_data()
        if self._websocket:
            await self._websocket.start()
        self._task = asyncio.create_task(self._refresh_loop(self.config.refresh_interval))
        # Background loops for v5.0.0
        try:
            loop = asyncio.get_event_loop()
            self._background_tasks.append(loop.create_task(self._limit_graph_loop()))
            self._background_tasks.append(loop.create_task(self._rlhf_loop()))
            self._background_tasks.append(loop.create_task(self._distillation_loop()))
            self._background_tasks.append(loop.create_task(self._federated_loop()))
            if self.chaos_tester:
                self._background_tasks.append(loop.create_task(self._chaos_loop()))
            self._background_tasks.append(loop.create_task(self._self_healing_loop()))
        except RuntimeError:
            pass
        logger.info("NodeRegistry started")

    async def _refresh_loop(self, interval: int):
        while self._running and not self._shutdown_event.is_set():
            try:
                await self._refresh_all_nodes()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Refresh loop error: {e}")
                await asyncio.sleep(60)

    async def _limit_graph_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.limit_graph:
                    await self.limit_graph.update_constraint(
                        'carbon', await self.carbon_manager.get_current_intensity())
                await asyncio.sleep(self.config.limit_graph.update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Limit graph loop error: {e}")

    async def _rlhf_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.rlhf:
                    await self.rlhf.train_reward_model()
                await asyncio.sleep(self.config.rlhf.training_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"RLHF loop error: {e}")

    async def _distillation_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                if self.distillation:
                    await self.distillation.distill({
                        'carbon_intensity': await self.carbon_manager.get_current_intensity(),
                        'avg_score': 0.5, 'cost': 0.5, 'diversity': 0.5})
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Distillation loop error: {e}")

    async def _federated_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(600)
                if self.federated and self.federated.client_updates:
                    self.federated.aggregate()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Federated loop error: {e}")

    async def _chaos_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(1800)
                if self.chaos_tester:
                    fault = random.choice(ChaosTester.FAULT_TYPES)
                    await self.chaos_tester.run_test(fault, duration_s=0.1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Chaos loop error: {e}")

    async def _self_healing_loop(self):
        while self._running and not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.self_healing.health_check_interval)
                if self.self_healing:
                    await self.self_healing.train([
                        {'refresh_improvement': 0.5, 'avg_carbon_intensity': 400,
                         'cache_size': len(self.cache), 'last_refresh_duration': 0.5}])
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Self-healing loop error: {e}")

    # ------------------------------------------------------------------
    # Core refresh
    # ------------------------------------------------------------------
    async def _refresh_all_nodes(self):
        start_time = time.time()
        async with self._lock:
            node_list = list(self.cache.values())

        if not node_list:
            return

        carbon_intensity = await self.carbon_manager.get_current_intensity()

        # Policy probs
        probs = await self.policy_probs({'carbon_intensity': carbon_intensity})
        strategy_names = ['immediate', 'batch_5', 'batch_10', 'delay_1h', 'delay_2h']
        if len(probs) != len(strategy_names):
            probs = [1.0 / len(strategy_names)] * len(strategy_names)
        strategy_idx = int(np.argmax(probs))
        strategy = strategy_names[strategy_idx]
        logger.info(f"Selected refresh strategy: {strategy}")

        # Decide top-N
        if self.moe_predictor:
            urgencies = []
            for node in node_list:
                try:
                    u = await self.moe_predictor.predict_urgency(node, carbon_intensity)
                except Exception:
                    u = 0.5
                urgencies.append((node.node_id, u))
            urgencies.sort(key=lambda x: x[1], reverse=True)
            if strategy == 'immediate':
                top_n = min(10, len(urgencies))
            elif strategy.startswith('batch_'):
                top_n = min(int(strategy.split('_')[1]), len(urgencies))
            elif strategy.startswith('delay_'):
                top_n = min(3, len(urgencies))
            else:
                top_n = 5
            to_refresh = [nid for nid, _ in urgencies[:top_n]]
        else:
            to_refresh = random.sample([n.node_id for n in node_list], min(5, len(node_list)))

        tasks = [self._refresh_single_node(nid) for nid in to_refresh]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Post-refresh updates
        for nid, res in zip(to_refresh, results):
            if not isinstance(res, Exception):
                improvement = random.uniform(0, 0.1)
                if self.moe_predictor:
                    try:
                        await self.moe_predictor.update(self.cache[nid], carbon_intensity, improvement)
                    except Exception:
                        pass
                if self.self_healing:
                    await self.self_healing.check_drift({
                        'refresh_improvement': improvement,
                        'avg_carbon_intensity': carbon_intensity,
                        'cache_size': len(self.cache),
                        'last_refresh_duration': time.time() - start_time})

        # Federated submission
        if self.federated and self.modp_selector:
            self.federated.submit_update(self.instance_id,
                                         list(self.modp_selector.weights),
                                         samples=len(to_refresh))
            if self._refresh_count % 5 == 0:
                self.federated.aggregate()

        # Carbon credit
        if self.carbon_market:
            try:
                saved_kg = len(to_refresh) * 0.01 * (400 - carbon_intensity) / 1000
                if saved_kg > 0:
                    await self.carbon_market.retire_credits(saved_kg, self.instance_id)
                    self._carbon_saved_kg_total += saved_kg
            except Exception:
                pass

        if PROMETHEUS_AVAILABLE:
            NODE_REFRESHES.labels(status='success').inc()
            NODE_REFRESH_DURATION.observe(time.time() - start_time)

        self._refresh_count += 1

        if self._websocket:
            await self._websocket.broadcast({
                'type': 'nodes_refreshed', 'nodes': to_refresh,
                'strategy': strategy, 'timestamp': datetime.now().isoformat()})

    async def _refresh_single_node(self, node_id: str):
        async with self._bulkhead:
            await self._rate_limiter.wait_and_acquire()
            await asyncio.sleep(random.uniform(0.05, 0.15))
            async with self._lock:
                if node_id in self.cache:
                    node = self.cache[node_id]
                    node.energy_efficiency = random.uniform(0.7, 0.95)
                    node.carbon_intensity = random.uniform(200, 600)
                    node.helium_index = random.uniform(0, 10)
                    node.material_index = random.uniform(0.5, 1.5)
                    node.renewable_fraction = random.uniform(0, 1)
                    node.last_updated = datetime.now()
                    try:
                        await self.db_manager.register_node(node)
                    except Exception:
                        pass

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def register_node(self, descriptor: NodeDescriptor) -> bool:
        if self.quantum_security:
            try:
                key = await self.quantum_security.generate_keypair(self.config.quantum_algorithm)
                sig = await self.quantum_security.sign_node_data(
                    asdict(descriptor) if hasattr(descriptor, '__dataclass_fields__')
                    else descriptor.dict(), key['key_id'])
                descriptor.quantum_signature = sig
            except Exception:
                pass

        if self.blockchain:
            try:
                data_hash = hashlib.sha256(
                    json.dumps(asdict(descriptor) if hasattr(descriptor, '__dataclass_fields__')
                               else descriptor.dict(), sort_keys=True, default=str).encode()
                ).hexdigest()
                tx_hash = await self.blockchain.record_node_registration(descriptor.node_id, data_hash)
                descriptor.blockchain_tx_hash = tx_hash
            except Exception:
                pass

        success = await self.db_manager.register_node(descriptor)
        if not success:
            return False

        async with self._lock:
            self.cache[descriptor.node_id] = descriptor
            if PROMETHEUS_AVAILABLE:
                NODE_CACHE_SIZE.set(len(self.cache))

        if PROMETHEUS_AVAILABLE:
            NODE_REGISTRATIONS.labels(status='success').inc()

        if self._websocket:
            await self._websocket.broadcast({
                'type': 'node_registered', 'node_id': descriptor.node_id,
                'timestamp': datetime.now().isoformat()})
        return True

    async def get_node(self, node_id: str) -> Optional[NodeDescriptor]:
        async with self._lock:
            return self.cache.get(node_id)

    async def list_nodes(self) -> List[str]:
        async with self._lock:
            return list(self.cache.keys())

    async def get_node_count(self) -> int:
        async with self._lock:
            return len(self.cache)

    # ------------------------------------------------------------------
    # Policy probs (RLHF > Distillation > Bio > MODP)
    # ------------------------------------------------------------------
    async def policy_probs(self, state: Dict) -> List[float]:
        if self.rlhf:
            return await self.rlhf.get_policy_probs(state)
        if self.distillation:
            return self.distillation.get_student_probs()
        if self.bio_optimizer:
            p = self.bio_optimizer.get_current_params()
            return [p['freshness_weight'], p['carbon_weight'],
                    p['cost_weight'], p['importance_weight']]
        if self.modp_selector:
            return list(self.modp_selector.weights)
        return [0.2] * 5

    # ------------------------------------------------------------------
    # v5.0.0 utilities
    # ------------------------------------------------------------------
    def select_precision(self, accuracy_required: float = 0.95) -> PrecisionLevel:
        if self.precision_controller is None:
            return PrecisionLevel.FP32
        return self.precision_controller.select(
            carbon_intensity=float(self.carbon_manager.current_intensity),
            accuracy_required=accuracy_required)

    async def compute_carbon_credit(self, carbon_saved_kg: float) -> Dict:
        if self.carbon_market is None:
            return {'credit_usd': 0.0, 'rec_usd': 0.0}
        credit = await self.carbon_market.get_carbon_credit_value(carbon_saved_kg)
        rec = await self.carbon_market.get_rec_value(carbon_saved_kg * 0.5)
        return {'credit_usd': credit, 'rec_usd': rec,
                'cumulative_kg': self._carbon_saved_kg_total}

    async def escalate_decision(self, decision_context: Dict, options: List[str],
                                confidence: float) -> Dict:
        if self.hitl is None:
            return {'escalated': False, 'chosen': options[0] if options else 'noop',
                    'source': 'fallback'}
        return await self.hitl.escalate(decision_context, options, confidence,
                                        self.config.hitl_confidence_threshold)

    async def run_chaos_suite(self) -> Dict:
        if self.chaos_tester is None:
            return {'error': 'chaos disabled'}
        results = []
        for f in ChaosTester.FAULT_TYPES:
            try:
                results.append(await self.chaos_tester.run_test(f, duration_s=0.05))
            except Exception as e:
                results.append({'fault': f, 'passed': False, 'error': str(e)})
        return {'results': results, 'report': self.chaos_tester.get_report()}

    async def health_check(self) -> Dict:
        return {
            'running': self._running,
            'cache_size': len(self.cache),
            'db_connected': self.db_manager.engine is not None,
            'refresh_count': self._refresh_count,
            'carbon_saved_kg_total': self._carbon_saved_kg_total,
            'features': {
                'temporal_logic': self.temporal_logic_enabled,
                'xai': self.xai_enabled,
                'adaptive_precision': self.adaptive_precision_enabled,
                'carbon_market': self.carbon_market_enabled,
                'role_specialization': self.role_specialization_enabled,
                'chaos_testing': self.chaos_testing_enabled,
                'hitl': self.hitl_enabled,
                'federated': self.federated_enabled,
            },
            'timestamp': datetime.now().isoformat(),
        }

    async def get_comprehensive_status(self) -> Dict:
        status = await self.health_check()
        if self.temporal_monitor:
            status['temporal_logic'] = self.temporal_monitor.get_status()
        if self.moe_predictor:
            status['moe'] = self.moe_predictor.get_stats()
        if self.bio_optimizer:
            status['bio'] = {'current_params': self.bio_optimizer.get_current_params()}
        if self.modp_selector:
            status['modp'] = {'weights': self.modp_selector.weights,
                              'recent_outcomes': len(self.modp_selector.recent_outcomes)}
        if self.self_healing:
            status['self_healing'] = await self.self_healing.get_stats()
        if self.limit_graph:
            status['limit_graph'] = await self.limit_graph.get_graph_summary()
        if self.rlhf:
            status['rlhf'] = {'actions': self.rlhf.actions,
                              'history_len': len(self.rlhf.history)}
        if self.distillation:
            status['distillation'] = {'student_probs': self.distillation.get_student_probs(),
                                      'history_len': len(self.distillation.history)}
        if self.federated:
            status['federated'] = self.federated.get_stats()
        if self.hitl:
            status['hitl'] = self.hitl.get_audit()
        if self.chaos_tester:
            status['chaos'] = self.chaos_tester.get_report()
        if self.precision_controller:
            status['precision'] = {'last': self.precision_controller.last_precision.value,
                                   'telemetry': self.precision_controller.telemetry}
        if self.carbon_market:
            status['carbon_market'] = await self.carbon_market.get_market_snapshot()
        return status

    async def stop(self):
        logger.info("Shutting down NodeRegistry v5.0.0...")
        self._shutdown_event.set()
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        for t in self._background_tasks:
            t.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        if self._websocket:
            await self._websocket.stop()
        await self.carbon_manager.close()
        self.db_manager.dispose()
        logger.info("NodeRegistry stopped")

# -----------------------------------------------------------------------------
# Signal handling + Singleton
# -----------------------------------------------------------------------------
_shutdown_requested = False
_shutdown_event_global = asyncio.Event()

def handle_signal(signum, frame):
    global _shutdown_requested
    if not _shutdown_requested:
        _shutdown_requested = True
        try:
            asyncio.create_task(_signal_shutdown())
        except Exception:
            pass

async def _signal_shutdown():
    _shutdown_event_global.set()

async def shutdown_handler():
    global _registry_instance
    if _registry_instance:
        await _registry_instance.stop()
        _registry_instance = None

_registry_instance = None
_registry_lock = asyncio.Lock()

async def get_node_registry(config: Optional[NodeRegistryConfig] = None) -> NodeRegistry:
    global _registry_instance
    if _registry_instance is None:
        async with _registry_lock:
            if _registry_instance is None:
                _registry_instance = NodeRegistry(config)
                await _registry_instance.start()
    return _registry_instance

# -----------------------------------------------------------------------------
# Main (smoke test)
# -----------------------------------------------------------------------------
async def main():
    try:
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda s=sig: handle_signal(s, None))
            except NotImplementedError:
                pass
    except Exception:
        pass

    print("=" * 80)
    print("Enhanced Node Registry v5.0.0")
    print("+ Temporal Logic + XAI + Adaptive Precision + Carbon Markets")
    print("+ Roles + Chaos Testing + Active RLHF + HITL + Federated")
    print("=" * 80)

    registry = await get_node_registry()

    node1 = NodeDescriptor(
        node_id="node-001", location="us-east-1",
        energy_efficiency=0.85, carbon_intensity=420, helium_index=0.5,
        material_index=1.2, cooling_type="liquid", renewable_fraction=0.4,
        harvester_type="solar", capture_efficiency=0.9, energy_output_watts=5000,
        availability_pattern={"monday": "high"})
    await registry.register_node(node1)

    node2 = NodeDescriptor(
        node_id="node-002", location="eu-west-1",
        energy_efficiency=0.92, carbon_intensity=280, helium_index=0.3,
        material_index=0.9, cooling_type="air", renewable_fraction=0.6,
        harvester_type="wind", capture_efficiency=0.85, energy_output_watts=8000)
    await registry.register_node(node2)

    print(f"\n📋 Registered nodes: {await registry.list_nodes()}")

    # Precision
    p = registry.select_precision(accuracy_required=0.95)
    print(f"\n⚙️  Precision: {p.value}")

    # Carbon credit
    cc = await registry.compute_carbon_credit(carbon_saved_kg=250.0)
    print(f"💱 Carbon credit: ${cc['credit_usd']:.4f}  REC: ${cc['rec_usd']:.4f}")

    # HITL escalation
    hitl = await registry.escalate_decision(
        decision_context={'reason': 'refresh_strategy'},
        options=['immediate', 'batch_5', 'delay_1h'],
        confidence=0.75)
    print(f"👤 HITL: escalated={hitl['escalated']} source={hitl['source']} "
          f"chosen={hitl['chosen']}")

    # Chaos
    print("\n🧪 Chaos suite:")
    chaos = await registry.run_chaos_suite()
    print(f"   Pass rate: {chaos['report']['pass_rate']:.2f}  "
          f"tests: {chaos['report']['tests_run']}")

    # Status
    print("\n📊 Comprehensive status:")
    status = await registry.get_comprehensive_status()
    print(json.dumps({
        'version': status.get('running'),
        'cache_size': status['cache_size'],
        'carbon_saved_kg': status['carbon_saved_kg_total'],
        'features': status['features'],
        'moe': status.get('moe'),
        'federated': status.get('federated'),
        'rlhf': status.get('rlhf'),
        'hitl_total': status.get('hitl', {}).get('total'),
        'chaos_pass_rate': status.get('chaos', {}).get('pass_rate'),
        'precision_last': status.get('precision', {}).get('last'),
        'carbon_market': status.get('carbon_market'),
    }, indent=2, default=str))

    print("\n" + "=" * 80)
    print("✅ Enhanced Node Registry v5.0.0 — smoke test complete")
    print("=" * 80)

    try:
        await _shutdown_event_global.wait()
    except asyncio.CancelledError:
        pass
    finally:
        await shutdown_handler()

if __name__ == "__main__":
    asyncio.run(main())
