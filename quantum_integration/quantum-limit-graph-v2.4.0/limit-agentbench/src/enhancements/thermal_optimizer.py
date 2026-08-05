# =============================================================================
# FILE: src/enhancements/thermal_optimizer_enhanced_v13_1_0.py
# VERSION: 13.1.0 (Enterprise Quantum Resilience + Multi‑Teacher Distillation)
# COMPLETE REPLACEMENT: fills missing classes and stubs for runtime
# =============================================================================
"""
Enhanced Multi-Physics Thermal Optimizer with GPU Acceleration - Version 13.1.0
ENHANCED WITH: Multi‑Teacher On‑Policy Distillation for Autonomous Optimization

This replacement fills previously omitted implementations for Storage,
EncryptionManager, QuantumResilientThermalSecurity, BlockchainThermalVerification,
ThermalState, CircuitBreaker, DigitalTwinManager, EquipmentPredictiveMaintenance,
MultiCloudThermalDistribution, EnergyStorageOptimizer, Thermal3DVisualizer,
and the various stub managers to provide a runnable, safe, stubbed module.
"""
import asyncio
import hashlib
import json
import os
import random
import sqlite3
import time
import uuid
from collections import deque, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Set, Union
import secrets
import gc
import numpy as np
from abc import ABC, abstractmethod
import logging
import logging.handlers
import base64
import math

# -----------------------------------------------------------------------------
# Optional external dependencies (install via pip if needed)
# -----------------------------------------------------------------------------
try:
    from web3 import Web3, Account, HTTPProvider  # type: ignore
    from web3.middleware import geth_poa_middleware, gas_price_strategy  # type: ignore
    WEB3_AVAILABLE = True
except Exception:
    WEB3_AVAILABLE = False

try:
    import boto3  # type: ignore
    from botocore.exceptions import ClientError  # type: ignore
    AWS_AVAILABLE = True
except Exception:
    AWS_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient  # type: ignore
    AZURE_AVAILABLE = True
except Exception:
    AZURE_AVAILABLE = False

try:
    from google.cloud import storage  # type: ignore
    GCP_AVAILABLE = True
except Exception:
    GCP_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium, falcon, sphincs  # type: ignore
    PQC_AVAILABLE = True
except Exception:
    PQC_AVAILABLE = False

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # type: ignore
    from cryptography.hazmat.primitives.asymmetric import ec  # type: ignore
    from cryptography.hazmat.primitives import hashes  # type: ignore
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption  # type: ignore
    from cryptography.hazmat.backends import default_backend  # type: ignore
    CRYPTO_AVAILABLE = True
except Exception:
    CRYPTO_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log  # type: ignore
    TENACITY_AVAILABLE = True
except Exception:
    TENACITY_AVAILABLE = False

try:
    import torch  # type: ignore
    import torch.nn as nn  # type: ignore
    import torch.optim as optim  # type: ignore
    TORCH_AVAILABLE = True
except Exception:
    TORCH_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestClassifier  # type: ignore
    from sklearn.preprocessing import StandardScaler  # type: ignore
    SKLEARN_AVAILABLE = True
except Exception:
    SKLEARN_AVAILABLE = False

try:
    import plotly.graph_objects as go  # type: ignore
    import plotly.express as px  # type: ignore
    PLOTLY_AVAILABLE = True
except Exception:
    PLOTLY_AVAILABLE = False

try:
    import aiohttp  # type: ignore
    AIOHTTP_AVAILABLE = True
except Exception:
    AIOHTTP_AVAILABLE = False

try:
    import pandas as pd  # type: ignore
    PANDAS_AVAILABLE = True
except Exception:
    PANDAS_AVAILABLE = False

try:
    from pydantic import BaseSettings, Field, validator  # type: ignore
    PYDANTIC_AVAILABLE = True
except Exception:
    PYDANTIC_AVAILABLE = False

try:
    from scipy import integrate, interpolate  # type: ignore
    from scipy.spatial import cKDTree  # type: ignore
    SCIPY_AVAILABLE = True
except Exception:
    SCIPY_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry  # type: ignore
    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False

import structlog
from structlog.processors import JSONRenderer, TimeStamper

# -----------------------------------------------------------------------------
# Structured Logging Configuration
# -----------------------------------------------------------------------------
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        TimeStamper(fmt="iso"),
        JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)
logger = structlog.get_logger(__name__)

# Audit logger (rotating file)
audit_logger = logging.getLogger('thermal_audit')
audit_handler = logging.handlers.RotatingFileHandler('thermal_audit_v13_1.log', maxBytes=50*1024*1024, backupCount=10)
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# Configuration with Pydantic (fallback if not installed)
# -----------------------------------------------------------------------------
if PYDANTIC_AVAILABLE:
    class Config(BaseSettings):
        DB_PATH: str = Field('/tmp/thermal_optimizer.db', env='THERMAL_DB_PATH')
        OPENAI_API_KEY: str = Field('', env='OPENAI_API_KEY')
        ELECTRICITY_MAPS_API_KEY: str = Field('', env='ELECTRICITY_MAPS_API_KEY')
        CARBON_INTENSITY_API_KEY: str = Field('', env='CARBON_INTENSITY_API_KEY')
        CARBON_REGION: str = Field('global', env='CARBON_REGION')
        BLOCKCHAIN_RPC_URL: str = Field('http://localhost:8545', env='BLOCKCHAIN_RPC_URL')
        BLOCKCHAIN_CONTRACT_ADDRESS: str = Field('0x0000000000000000000000000000000000000000', env='BLOCKCHAIN_CONTRACT_ADDRESS')
        BLOCKCHAIN_PRIVATE_KEY: str = Field('', env='BLOCKCHAIN_PRIVATE_KEY')
        CLOUD_AWS_ACCESS_KEY: str = Field('', env='AWS_ACCESS_KEY_ID')
        CLOUD_AWS_SECRET_KEY: str = Field('', env='AWS_SECRET_ACCESS_KEY')
        CLOUD_AWS_REGION: str = Field('us-east-1', env='AWS_DEFAULT_REGION')
        CLOUD_AZURE_CONNECTION_STRING: str = Field('', env='AZURE_STORAGE_CONNECTION_STRING')
        CLOUD_GCP_CREDENTIALS: str = Field('', env='GOOGLE_APPLICATION_CREDENTIALS')
        MASTER_KEY_ENV: str = Field('THERMAL_MASTER_KEY', env='MASTER_KEY_ENV')
        CACHE_TTL: int = Field(300, env='CACHE_TTL')
        RETRY_ATTEMPTS: int = Field(3, env='RETRY_ATTEMPTS')
        RETRY_MIN_WAIT: int = Field(2, env='RETRY_MIN_WAIT')
        RETRY_MAX_WAIT: int = Field(10, env='RETRY_MAX_WAIT')
        LOG_LEVEL: str = Field('INFO', env='THERMAL_LOG_LEVEL')

        @validator('BLOCKCHAIN_PRIVATE_KEY')
        def validate_private_key(cls, v):
            if v and not v.startswith('0x'):
                raise ValueError('Private key must start with 0x')
            return v

        @validator('BLOCKCHAIN_CONTRACT_ADDRESS')
        def validate_contract_address(cls, v):
            if v and not v.startswith('0x'):
                raise ValueError('Contract address must start with 0x')
            return v

        class Config:
            env_file = '.env'
            case_sensitive = True

    config = Config()
else:
    # Fallback configuration
    class Config:
        DB_PATH = os.getenv('THERMAL_DB_PATH', '/tmp/thermal_optimizer.db')
        OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
        ELECTRICITY_MAPS_API_KEY = os.getenv('ELECTRICITY_MAPS_API_KEY', '')
        CARBON_INTENSITY_API_KEY = os.getenv('CARBON_INTENSITY_API_KEY', '')
        CARBON_REGION = os.getenv('CARBON_REGION', 'global')
        BLOCKCHAIN_RPC_URL = os.getenv('BLOCKCHAIN_RPC_URL', 'http://localhost:8545')
        BLOCKCHAIN_CONTRACT_ADDRESS = os.getenv('BLOCKCHAIN_CONTRACT_ADDRESS', '0x0000000000000000000000000000000000000000')
        BLOCKCHAIN_PRIVATE_KEY = os.getenv('BLOCKCHAIN_PRIVATE_KEY', '')
        CLOUD_AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', '')
        CLOUD_AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
        CLOUD_AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        CLOUD_AZURE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING', '')
        CLOUD_GCP_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')
        MASTER_KEY_ENV = os.getenv('THERMAL_MASTER_KEY', '')
        CACHE_TTL = int(os.getenv('CACHE_TTL', '300'))
        RETRY_ATTEMPTS = int(os.getenv('RETRY_ATTEMPTS', '3'))
        RETRY_MIN_WAIT = int(os.getenv('RETRY_MIN_WAIT', '2'))
        RETRY_MAX_WAIT = int(os.getenv('RETRY_MAX_WAIT', '10'))
        LOG_LEVEL = os.getenv('THERMAL_LOG_LEVEL', 'INFO')

        @classmethod
        def get_master_key(cls) -> bytes:
            key_hex = os.getenv(cls.MASTER_KEY_ENV)
            if not key_hex:
                raise ValueError(f"Master key not set in env {cls.MASTER_KEY_ENV}")
            return bytes.fromhex(key_hex)

    config = Config()

# -----------------------------------------------------------------------------
# Metrics (only if Prometheus available)
# -----------------------------------------------------------------------------
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    THERMAL_OPTIMIZATION_RUNS = Counter('thermal_optimization_runs_total', 'Total thermal optimizations', ['method', 'status'], registry=REGISTRY)
    OPTIMIZATION_DURATION = Histogram('thermal_optimization_duration_seconds', 'Optimization duration', ['method'], registry=REGISTRY)
    COOLING_ENERGY = Gauge('cooling_energy_kw', 'Cooling energy consumption', registry=REGISTRY)
    MAX_TEMPERATURE = Gauge('max_server_temperature_c', 'Maximum server temperature', registry=REGISTRY)
    PUE_METRIC = Gauge('pue_metric', 'Power Usage Effectiveness', registry=REGISTRY)
    CARBON_SAVINGS = Gauge('carbon_savings_kg', 'Carbon savings', registry=REGISTRY)
    GPU_TEMP = Gauge('gpu_temperature_c', 'GPU temperature', ['device'], registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge('thermal_circuit_breaker_state', 'Circuit breaker state (0=closed,1=half,2=open)', ['component'], registry=REGISTRY)
    HEALTH_SCORE = Gauge('thermal_system_health', 'System health score (0-100)', registry=REGISTRY)
    DB_SIZE = Gauge('thermal_db_size_mb', 'Database size in MB', registry=REGISTRY)
    DATA_QUALITY_SCORE = Gauge('thermal_data_quality', 'Sensor data quality score', registry=REGISTRY)
    OPTIMIZATION_QUEUE_SIZE = Gauge('thermal_optimization_queue_size', 'Optimization queue size', registry=REGISTRY)
    WS_CONNECTIONS = Gauge('thermal_ws_connections', 'WebSocket connections', registry=REGISTRY)
    RL_EPISODE_REWARD = Gauge('thermal_rl_episode_reward', 'RL episode reward', registry=REGISTRY)
    FORECAST_ERROR = Gauge('thermal_forecast_error', 'Thermal forecast MAPE %', registry=REGISTRY)
    CARBON_INTENSITY = Gauge('carbon_intensity_gco2_per_kwh', 'Real-time carbon intensity', registry=REGISTRY)
    HELIUM_EFFICIENCY = Gauge('helium_cooling_efficiency', 'Helium cooling efficiency', registry=REGISTRY)
    FEDERATED_ROUNDS = Counter('federated_learning_rounds_total', 'Federated learning rounds', registry=REGISTRY)
    ENSEMBLE_ACCURACY = Gauge('ensemble_forecast_accuracy', 'Ensemble forecast accuracy', registry=REGISTRY)
    SUSTAINABILITY_SCORE = Gauge('sustainability_score', 'Overall sustainability score (0-100)', registry=REGISTRY)
    DIGITAL_TWIN_UPDATES = Counter('digital_twin_updates_total', 'Digital twin updates', registry=REGISTRY)
    PREDICTIVE_MAINTENANCE_ALERTS = Counter('predictive_maintenance_alerts_total', 'Predictive maintenance alerts', ['equipment_type'], registry=REGISTRY)
    MULTI_ZONE_ACTIONS = Counter('multi_zone_actions_total', 'Multi-zone RL actions', ['zone'], registry=REGISTRY)
    ENERGY_STORAGE_CYCLES = Counter('energy_storage_cycles_total', 'Energy storage charge/discharge cycles', ['action'], registry=REGISTRY)
    THERMAL_3D_VIEWS = Counter('thermal_3d_views_total', '3D thermal visualization views', registry=REGISTRY)
    WHAT_IF_ANALYSES = Counter('what_if_analyses_total', 'What-if scenario analyses', ['scenario_type'], registry=REGISTRY)
    QUANTUM_SIGNATURES = Counter('thermal_quantum_signatures_total', 'Quantum signatures', ['algorithm', 'status'], registry=REGISTRY)
    BLOCKCHAIN_VERIFICATIONS = Counter('thermal_blockchain_verifications_total', 'Blockchain verifications', ['status'], registry=REGISTRY)
    AUTONOMOUS_OPTIMIZATIONS = Counter('thermal_autonomous_optimizations_total', 'Autonomous optimizations', ['strategy', 'status'], registry=REGISTRY)
    CLOUD_DISTRIBUTIONS = Counter('thermal_cloud_distributions_total', 'Cloud distributions', ['provider', 'status'], registry=REGISTRY)

# Constants
MAX_OPTIMIZATION_HISTORY = 10000
MAX_RL_MEMORY = 50000
MAX_CACHE_SIZE = 1000
CACHE_TTL_SECONDS = config.CACHE_TTL
MAX_RETRY_ATTEMPTS = config.RETRY_ATTEMPTS
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 60
HEALTH_CHECK_TIMEOUT = 10
MAX_CONCURRENT_OPTIMIZATIONS = 4
DATA_VERSION = 13
CACHE_CLEANUP_INTERVAL = 3600
BATCH_SIZE = 32
GAMMA = 0.99
LEARNING_RATE = 0.001
TARGET_UPDATE_FREQ = 100
REPLAY_BUFFER_SIZE = 10000
FEDERATED_AGGREGATION_INTERVAL = 3600
ENSEMBLE_MODELS = ['lstm', 'gru', 'transformer', 'prophet']

# -----------------------------------------------------------------------------
# Circuit Breaker (simple implementation)
# -----------------------------------------------------------------------------
class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = CIRCUIT_BREAKER_THRESHOLD, timeout: int = CIRCUIT_BREAKER_TIMEOUT):
        self.name = name
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failures = 0
        self.opened_at: Optional[float] = None

    def record_failure(self):
        self.failures += 1
        if self.failures >= self.failure_threshold and not self.is_open():
            self.opened_at = time.time()
            logger.warning("Circuit breaker %s opened", self.name)

    def reset(self):
        self.failures = 0
        self.opened_at = None
        logger.info("Circuit breaker %s reset", self.name)

    def is_open(self) -> bool:
        if self.opened_at is None:
            return False
        if time.time() - self.opened_at > self.timeout:
            # auto recover
            self.reset()
            return False
        return True

    def state(self) -> int:
        # 0 closed, 1 half, 2 open
        if self.is_open():
            return 2
        if self.failures > 0:
            return 1
        return 0

# -----------------------------------------------------------------------------
# Persistent Storage (SQLite) - minimal safe implementation with async wrappers
# -----------------------------------------------------------------------------
class Storage:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or config.DB_PATH
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True) if os.path.dirname(self.db_path) else None
        self._ensure_db()

    def _conn(self):
        # use check_same_thread=False for multi-threading via asyncio.to_thread
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def _ensure_db(self):
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS kv_store (
                    k TEXT PRIMARY KEY,
                    v TEXT,
                    updated_at TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS optimizations (
                    id TEXT PRIMARY KEY,
                    data TEXT,
                    created_at TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sustainability (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data TEXT,
                    created_at TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS maintenance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data TEXT,
                    created_at TIMESTAMP
                )
            """)
            conn.commit()
        finally:
            conn.close()

    def get_state(self, key: str) -> Optional[str]:
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT v FROM kv_store WHERE k = ?", (key,))
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def save_state(self, key: str, value: str):
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute("REPLACE INTO kv_store (k, v, updated_at) VALUES (?, ?, ?)", (key, value, datetime.utcnow().isoformat()))
            conn.commit()
        finally:
            conn.close()

    async def get_thermal_history(self, hours: int = 24) -> List[Any]:
        # Return list of simple objects with attributes used by optimizer
        def _sync():
            conn = self._conn()
            try:
                cur = conn.cursor()
                cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
                cur.execute("SELECT data FROM optimizations WHERE created_at > ?", (cutoff,))
                rows = cur.fetchall()
                result = []
                for (data,) in rows:
                    try:
                        obj = json.loads(data)
                        # provide attribute access
                        class R: pass
                        r = R()
                        r.avg_server_temp_c = obj.get('avg_server_temp_c', 25.0)
                        r.max_server_temp_c = obj.get('max_server_temp_c', 30.0)
                        r.pue = obj.get('pue', 1.5)
                        result.append(r)
                    except Exception:
                        continue
                return result
            finally:
                conn.close()
        return await asyncio.to_thread(_sync)

    async def get_maintenance_history(self, limit: int = 100) -> List[Dict]:
        def _sync():
            conn = self._conn()
            try:
                cur = conn.cursor()
                cur.execute("SELECT data FROM maintenance ORDER BY id DESC LIMIT ?", (limit,))
                rows = cur.fetchall()
                return [json.loads(r[0]) for r in rows if r]
            finally:
                conn.close()
        return await asyncio.to_thread(_sync)

    async def save_thermal_optimization(self, result) -> None:
        def _sync():
            conn = self._conn()
            try:
                cur = conn.cursor()
                rec_id = getattr(result, 'id', f"opt_{uuid.uuid4().hex[:8]}")
                cur.execute("INSERT OR REPLACE INTO optimizations (id, data, created_at) VALUES (?, ?, ?)",
                            (rec_id, json.dumps(asdict(result), default=str), datetime.utcnow().isoformat()))
                conn.commit()
            finally:
                conn.close()
        return await asyncio.to_thread(_sync)

    async def save_sustainability_metrics(self, metrics: Dict) -> None:
        def _sync():
            conn = self._conn()
            try:
                cur = conn.cursor()
                cur.execute("INSERT INTO sustainability (data, created_at) VALUES (?, ?)", (json.dumps(metrics), datetime.utcnow().isoformat()))
                conn.commit()
            finally:
                conn.close()
        return await asyncio.to_thread(_sync)

# -----------------------------------------------------------------------------
# AES-256-GCM Encryption Manager (minimal)
# -----------------------------------------------------------------------------
class EncryptionManager:
    def __init__(self, master_key: Optional[bytes] = None):
        if master_key:
            self.master_key = master_key
        else:
            try:
                self.master_key = config.get_master_key()
            except Exception:
                # fallback to random key (NOT for production)
                self.master_key = secrets.token_bytes(32)
        if CRYPTO_AVAILABLE:
            # AESGCM expects 32-byte key for AES-256
            self.aesgcm = AESGCM(self.master_key)
        else:
            self.aesgcm = None

    def encrypt(self, plaintext: bytes, associated_data: Optional[bytes] = None) -> Dict[str, str]:
        if self.aesgcm:
            nonce = secrets.token_bytes(12)
            ct = self.aesgcm.encrypt(nonce, plaintext, associated_data)
            return {'nonce': base64.b64encode(nonce).decode(), 'ct': base64.b64encode(ct).decode()}
        # fallback (not secure)
        return {'nonce': '', 'ct': base64.b64encode(plaintext).decode()}

    def decrypt(self, nonce_b64: str, ct_b64: str, associated_data: Optional[bytes] = None) -> bytes:
        if self.aesgcm:
            nonce = base64.b64decode(nonce_b64)
            ct = base64.b64decode(ct_b64)
            return self.aesgcm.decrypt(nonce, ct, associated_data)
        return base64.b64decode(ct_b64)

# ============================================================================
# MODULE 1: Quantum-Resilient Thermal Security (minimal stub)
# ============================================================================
class QuantumResilientThermalSecurity:
    def __init__(self, storage: Storage):
        self.storage = storage

    async def generate_keypair(self, algorithm: str = 'dilithium') -> Dict[str, Any]:
        # Minimal stub that returns a key identifier and stores a placeholder
        key_id = f"{algorithm}-{uuid.uuid4().hex[:8]}"
        key_entry = {'algorithm': algorithm, 'created_at': datetime.utcnow().isoformat()}
        self.storage.save_state(f"quantum_key_{key_id}", json.dumps(key_entry))
        return {'key_id': key_id, 'meta': key_entry}

    async def sign_thermal_data(self, data: Dict[str, Any], key_id: str) -> Dict[str, str]:
        # Deterministic pseudo-signature: sha256 of serialized data + key_id
        payload = json.dumps(data, sort_keys=True, default=str).encode()
        digest = hashlib.sha256(payload + key_id.encode()).digest()
        signature = base64.b64encode(digest).decode()
        return {'key_id': key_id, 'signature': signature, 'algorithm': 'dilithium_stub'}

    def get_quantum_status(self) -> Dict[str, Any]:
        return {'pqc_available': bool(PQC_AVAILABLE)}

# ============================================================================
# MODULE 2: Blockchain Thermal Verification (minimal stub)
# ============================================================================
class BlockchainThermalVerification:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.rpc = config.BLOCKCHAIN_RPC_URL
        self.contract_address = config.BLOCKCHAIN_CONTRACT_ADDRESS

    async def record_thermal_data(self, data_id: str, data_hash: str, meta: Dict[str, Any]) -> Dict[str, Any]:
        # Minimal stub that returns fake tx hash and stores record in local storage
        tx_hash = f"0x{hashlib.sha256((data_id + data_hash).encode()).hexdigest()[:64]}"
        record = {'data_id': data_id, 'data_hash': data_hash, 'meta': meta, 'tx_hash': tx_hash, 'created_at': datetime.utcnow().isoformat()}
        # store a record in kv for debugging
        self.storage.save_state(f"blockchain_record_{data_id}", json.dumps(record))
        return {'tx_hash': tx_hash}

    def get_blockchain_status(self) -> Dict[str, Any]:
        return {'connected': WEB3_AVAILABLE, 'rpc': self.rpc, 'contract': self.contract_address}

# ============================================================================
# NEW: Thermal Optimization State (context for distillation)
# ============================================================================
@dataclass
class ThermalOptimizationState:
    """Rich context for the multi‑teacher distillation agent."""
    # Current metrics
    pue: float
    avg_temp_c: float
    max_temp_c: float
    carbon_intensity_gco2: float
    energy_storage_level_pct: float
    workload_pct: float

    # Digital twin summaries
    node_count: int
    avg_node_power_kw: float
    cooling_capacity_utilization: float

    # Predictive maintenance
    equipment_risk_score: float  # max risk across equipment

    # Time context
    hour_of_day: int
    is_weekend: bool

    def to_feature_vector(self) -> np.ndarray:
        """Convert state to 12‑dim feature vector for ML models."""
        features = [
            min(self.pue / 2.0, 1.0),
            min(self.avg_temp_c / 40.0, 1.0),
            min(self.max_temp_c / 45.0, 1.0),
            min(self.carbon_intensity_gco2 / 1000.0, 1.0),
            self.energy_storage_level_pct / 100.0,
            self.workload_pct / 100.0,
            min(self.node_count / 100.0, 1.0),
            min(self.avg_node_power_kw / 500.0, 1.0),
            self.cooling_capacity_utilization / 100.0,
            self.equipment_risk_score,
            self.hour_of_day / 24.0,
            1.0 if self.is_weekend else 0.0,
        ]
        return np.array(features, dtype=np.float32)

# ============================================================================
# NEW: Multi‑Teacher Distillation Optimizer for Thermal
# ============================================================================
class Teacher(ABC):
    """Base class for all teachers."""
    @abstractmethod
    def predict(self, state: ThermalOptimizationState) -> np.ndarray:
        """Return probability vector over 5 strategies."""
        pass

    @abstractmethod
    def confidence(self, state: ThermalOptimizationState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class ThermalRuleBasedTeacher(Teacher):
    """Rule‑based expert: carbon‑aware, PUE‑aware, storage‑aware."""
    def predict(self, state: ThermalOptimizationState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if state.carbon_intensity_gco2 > 500:
            probs[1] = 0.8   # carbon strategy
        elif state.pue > 1.8:
            probs[0] = 0.7   # performance (reduce PUE)
        elif state.energy_storage_level_pct < 20:
            probs[2] = 0.6   # cost (avoid discharging)
        return probs / probs.sum()

    def confidence(self, state: ThermalOptimizationState) -> float:
        if state.carbon_intensity_gco2 > 500:
            return 0.6
        elif state.pue > 1.8:
            return 0.5
        return 0.4


class ThermalHistoricalMLTeacher(Teacher):
    """Offline trained classifier on historical optimal actions (minimal)."""
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        if model_path and Path(model_path).exists():
            try:
                import joblib  # type: ignore
                self.model = joblib.load(model_path)
            except Exception:
                self.model = None

    def predict(self, state: ThermalOptimizationState) -> np.ndarray:
        if self.model is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: ThermalOptimizationState) -> float:
        return 0.7 if self.model is not None else 0.0


class ThermalStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features (persistent)."""
    def __init__(self, storage: Storage, lr: float = 0.1):
        self.storage = storage
        self.lr = lr
        self.weights = np.zeros((12, 5))  # 12 features, 5 actions
        self._load_state()

    def _load_state(self):
        w = self.storage.get_state('q_teacher_weights')
        if w:
            try:
                self.weights = np.array(json.loads(w))
            except Exception:
                self.weights = np.zeros((12, 5))

    def _save_state(self):
        try:
            self.storage.save_state('q_teacher_weights', json.dumps(self.weights.tolist()))
        except Exception:
            pass

    def predict(self, state: ThermalOptimizationState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        # Softmax exploration
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: ThermalOptimizationState) -> float:
        return 0.5

    def update(self, state: ThermalOptimizationState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class DistillationStudent:
    """Student policy: linear softmax model updated via distillation + policy gradient."""
    def __init__(self, feature_dim: int = 12, n_classes: int = 5, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray) -> np.ndarray:
        logits = state_vector @ self.weights + self.biases
        # stable softmax
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
        """Single‑step SGD update combining distillation and REINFORCE."""
        current_probs = self.predict_proba(state_vector)
        logits = state_vector @ self.weights + self.biases

        # Distillation gradient (simple difference)
        grad_distill = -(teacher_probs - current_probs)

        # Policy gradient (REINFORCE)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)

        grad = distill_weight * grad_distill + rl_weight * grad_rl

        # Update
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1


class ReplayBuffer:
    def __init__(self, max_size: int = 2000):
        self.buffer = deque(maxlen=max_size)

    def push(self, state_vec: np.ndarray, action: int, reward: float, next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))

    def sample(self, batch_size: int = 32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        if not batch:
            return (np.array([]), [], np.array([]), np.array([]), np.array([]))
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), actions, np.array(rewards),
                np.array(next_states), np.array(teacher_probs))


class DistillationThermalOptimizer:
    """
    Replaces AutonomousThermalOptimizer with multi‑teacher on‑policy distillation.
    """
    ACTION_SPACE = ['performance', 'carbon', 'cost', 'hybrid', 'adaptive']

    def __init__(self, storage: Storage, state: 'ThermalState'):
        self.storage = storage
        self.global_state = state
        self.student = DistillationStudent()
        self.teachers: List[Teacher] = [
            ThermalRuleBasedTeacher(),
            ThermalHistoricalMLTeacher(),  # optionally load model
            ThermalStatefulQTeacher(storage)
        ]
        self.replay_buffer = ReplayBuffer()
        self.epsilon = 0.1
        self.train_every = 10
        self.counter = 0

    async def optimize_thermal(self, current_state: ThermalOptimizationState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        """
        Returns:
            - selected strategy name
            - action index
            - state vector
            - teacher ensemble probabilities
        """
        state_vec = current_state.to_feature_vector()

        # Ensemble teachers
        teacher_probs = np.zeros(5)
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(current_state)
            conf = teacher.confidence(current_state)
            teacher_probs += prob * conf
            total_conf += conf
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(5) / 5

        # Student distribution
        student_probs = self.student.predict_proba(state_vec)

        # Action selection (ε‑greedy over student, with teacher mixing)
        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, 4)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = int(np.argmax(combined))

        strategy = self.ACTION_SPACE[action_idx]
        return strategy, action_idx, state_vec, teacher_probs

    async def update_after_test(self, state_vec: np.ndarray, action_idx: int, reward: float,
                                next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        """Store transition, update teachers and student."""
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1

        # Periodic mini‑batch training
        if self.counter % self.train_every == 0 and len(self.replay_buffer.buffer) >= 10:
            states, actions, rewards, _, teacher_probs_batch = self.replay_buffer.sample(8)
            for i in range(len(states)):
                try:
                    self.student.update(states[i], teacher_probs_batch[i], float(rewards[i]), int(actions[i]))
                except Exception:
                    continue

    def get_stats(self) -> Dict:
        return {
            'student_counter': self.student.counter,
            'buffer_size': len(self.replay_buffer.buffer),
            'weights_norm': float(np.linalg.norm(self.student.weights))
        }

# ============================================================================
# MODULE 3: Multi-Cloud Thermal Distribution (minimal)
# ============================================================================
class MultiCloudThermalDistribution:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.providers = ['aws', 'gcp', 'azure']
        self.last_distribution = {}

    async def distribute_thermal_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        # Select minimal provider by round-robin
        provider = random.choice(self.providers)
        self.last_distribution = {'optimal_provider': provider, 'size_gb': data.get('size_gb', 0.0), 'timestamp': datetime.utcnow().isoformat()}
        return self.last_distribution

    async def get_distribution_status(self) -> Dict[str, Any]:
        return {'last': self.last_distribution, 'providers': self.providers}

# ============================================================================
# Thermal State (minimal)
# ============================================================================
class ThermalState:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.last_update = datetime.utcnow()
        self.metrics = {}

    def update_from_sensor(self, metrics: Dict[str, Any]):
        self.metrics.update(metrics)
        self.last_update = datetime.utcnow()

# ============================================================================
# Data Classes (minimal placeholders)
# ============================================================================
@dataclass
class DigitalTwinNode:
    id: str
    power_kw: float = 0.0
    temp_c: float = 25.0

@dataclass
class DigitalTwinGraph:
    nodes: Dict[str, DigitalTwinNode] = field(default_factory=dict)

@dataclass
class ThermalOptimizationResult:
    total_energy_kw: float = 0.0
    cooling_energy_kw: float = 0.0
    it_energy_kw: float = 0.0
    pue: float = 0.0
    avg_server_temp_c: float = 25.0
    max_server_temp_c: float = 27.0
    carbon_footprint_kg_per_hour: float = 0.0
    carbon_intensity_gco2_per_kwh: float = 0.0
    carbon_savings_kg: float = 0.0
    helium_usage_liters: float = 0.0
    helium_efficiency: float = 0.0
    sustainability_score: float = 0.0
    optimization_time_ms: float = 0.0
    gpu_accelerated: bool = False
    zone_temperatures: Dict[str, float] = field(default_factory=dict)
    anomaly_detected: bool = False
    rl_action_used: int = 0
    rl_action_description: str = ""
    quantum_signature: Optional[Dict[str, Any]] = None
    blockchain_tx_hash: Optional[str] = None
    cloud_distribution: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class DataCenterConfigModel:
    renewable_energy_pct: float = 50.0

# ============================================================================
# Stub components (minimal implementations)
# ============================================================================
class StubCarbonIntensityManager:
    def __init__(self):
        self.region = 'global'
        self.current = 300.0

    async def start(self):
        return

    async def update_carbon_intensity(self, region: str):
        self.region = region
        # simulate an update
        self.current = random.uniform(100, 600)

    async def get_current_intensity(self) -> float:
        # return current carbon intensity (gCO2/kWh)
        return float(self.current)

    async def calculate_carbon_savings(self, delta_energy_kw: float) -> float:
        # simplistic: delta_energy * intensity / 1000
        return max(0.0, delta_energy_kw * (self.current / 1000.0))

    async def close(self):
        return

class StubHeliumCoolingManager:
    async def get_efficiency_metrics(self) -> Dict[str, Any]:
        return {'total_usage_liters': random.uniform(0, 10), 'current_efficiency': random.uniform(0.5, 0.95)}

class StubFederatedLearningManager:
    async def close(self):
        return

class StubCacheManager:
    async def start(self):
        return

    async def stop(self):
        return

class StubDataQualityScorer:
    async def get_statistics(self) -> Dict[str, Any]:
        return {'avg_score': random.uniform(70, 100)}

class StubRateLimiter:
    async def wait_and_acquire(self):
        # minimal rate limiting (no wait)
        return

class StubThermalWebSocketDashboard:
    def __init__(self, port: int = 8780):
        self.port = port
        self.started = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.started = False

    async def broadcast_thermal_update(self, result: ThermalOptimizationResult):
        # no-op stub; in real system broadcast to clients
        return

# ============================================================================
# DeepQNetwork, DQNAgent (minimal placeholders to avoid undefined refs)
# ============================================================================
if TORCH_AVAILABLE:
    class DeepQNetwork(nn.Module):
        def __init__(self, input_dim: int, output_dim: int):
            super().__init__()
            self.net = nn.Sequential(
                nn.Linear(input_dim, 64),
                nn.ReLU(),
                nn.Linear(64, output_dim)
            )

        def forward(self, x):
            return self.net(x)
else:
    class DeepQNetwork:
        def __init__(self, *args, **kwargs):
            pass

class DQNAgent:
    def __init__(self, state_size: int, action_size: int):
        self.state_size = state_size
        self.action_size = action_size

    def select_action(self, state: np.ndarray) -> int:
        return int(random.randint(0, max(0, self.action_size - 1)))

class DQNReplayBuffer:
    def __init__(self, max_size: int = 10000):
        self.buffer = deque(maxlen=max_size)

    def push(self, *args):
        self.buffer.append(args)

    def sample(self, batch_size: int = 32):
        if len(self.buffer) < batch_size:
            return list(self.buffer)
        return random.sample(self.buffer, batch_size)

# ============================================================================
# DigitalTwinManager (minimal)
# ============================================================================
class DigitalTwinManager:
    def __init__(self):
        self.twin = DigitalTwinGraph()
        # create a few nodes for stubs
        for i in range(1, 6):
            nid = f"node-{i}"
            self.twin.nodes[nid] = DigitalTwinNode(id=nid, power_kw=random.uniform(0.5, 5.0), temp_c=25.0)

    async def get_digital_twin_summary(self) -> Dict[str, Any]:
        total_nodes = len(self.twin.nodes)
        total_power = sum(n.power_kw for n in self.twin.nodes.values())
        return {'total_nodes': total_nodes, 'total_power_kw': total_power}

    async def update_twin(self, sensor_data: Dict) -> Dict:
        # Accept sensor data and update nodes (basic)
        for nid, val in sensor_data.get('nodes', {}).items():
            if nid in self.twin.nodes:
                node = self.twin.nodes[nid]
                node.temp_c = float(val.get('temp_c', node.temp_c))
                node.power_kw = float(val.get('power_kw', node.power_kw))
        return {'status': 'updated'}

    async def run_what_if_analysis(self, scenario: Dict) -> Dict:
        # Minimal stub returns simple scenario result
        return {'scenario': scenario, 'impact': {'pue_change': random.uniform(-0.05, 0.1)}}

# ============================================================================
# EquipmentPredictiveMaintenance (minimal)
# ============================================================================
class EquipmentPredictiveMaintenance:
    def __init__(self):
        self.model = None

    async def train_model(self, history):
        # stub: pretend to train
        self.model = {'trained_on': len(history)}

    async def get_maintenance_schedule(self) -> Dict[str, Any]:
        # stub: random pending maintenance count
        pending = random.randint(0, 3)
        return {'pending_maintenance': pending}

    async def predict_failure(self, equipment_id: str, sensor_data: Dict) -> Dict:
        risk = random.uniform(0.0, 1.0)
        return {'equipment_id': equipment_id, 'risk_score': risk}

# ============================================================================
# MultiZoneDQNAgent (minimal)
# ============================================================================
class MultiZoneDQNAgent:
    def __init__(self, zone_ids: List[str], state_size: int = 10, action_size_per_zone: int = 5):
        self.zone_ids = zone_ids
        self.state_size = state_size
        self.action_size_per_zone = action_size_per_zone

    def select_zone_action(self, zone: str, state_zone: np.ndarray) -> int:
        return int(random.randint(0, max(0, self.action_size_per_zone - 1)))

# ============================================================================
# EnergyStorageOptimizer (minimal)
# ============================================================================
class EnergyStorageOptimizer:
    def __init__(self):
        self.charge_percentage = random.uniform(20, 100)

    async def get_battery_status(self) -> Dict[str, Any]:
        return {'charge_percentage': self.charge_percentage, 'health_pct': random.uniform(80, 100)}

    async def optimize_storage(self, carbon_intensity: float, cooling_energy: float) -> Dict[str, Any]:
        # Decide to charge/discharge based on carbon intensity (simple heuristic)
        if carbon_intensity > 500 and self.charge_percentage > 20:
            action = 'discharge'
            amount = min(10.0, (self.charge_percentage - 20) * 0.1)
            self.charge_percentage = max(0.0, self.charge_percentage - amount)
            carbon_saved = amount * (carbon_intensity / 1000.0) * 0.5
            return {'action': action, 'amount_kwh': amount, 'carbon_saved_kg': carbon_saved}
        # else charge lightly
        action = 'charge'
        amount = min(5.0, (100.0 - self.charge_percentage) * 0.05)
        self.charge_percentage = min(100.0, self.charge_percentage + amount)
        return {'action': action, 'amount_kwh': amount, 'carbon_saved_kg': 0.0}

# ============================================================================
# Thermal3DVisualizer (minimal)
# ============================================================================
class Thermal3DVisualizer:
    async def generate_thermal_map(self, nodes: List[DigitalTwinNode]) -> Dict[str, Any]:
        # Return a stubbed "map" (list of node temps)
        return {'nodes': [{ 'id': n.id, 'temp_c': n.temp_c, 'power_kw': n.power_kw } for n in nodes]}

# ============================================================================
# ENHANCED MAIN THERMAL OPTIMIZER V13.1.0
# ============================================================================
class EnhancedThermalOptimizerV13:
    """Enhanced thermal optimizer v13.1.0 with multi‑teacher distillation."""

    def __init__(self):
        self.instance_id = str(uuid.uuid4())[:8]

        # Central storage
        self.storage = Storage()
        self.state = ThermalState(self.storage)

        # Enhanced modules
        self.quantum_security = QuantumResilientThermalSecurity(self.storage)
        self.blockchain = BlockchainThermalVerification(self.storage)
        # REPLACED: self.autonomous_optimizer = AutonomousThermalOptimizer(...)
        self.distillation_optimizer = DistillationThermalOptimizer(self.storage, self.state)
        self.cloud_distributor = MultiCloudThermalDistribution(self.storage)

        # Advanced components
        self.digital_twin = DigitalTwinManager()
        self.predictive_maintenance = EquipmentPredictiveMaintenance()
        zone_ids = [f"zone-{i}" for i in range(1, 5)]
        self.multi_zone_agent = MultiZoneDQNAgent(zone_ids, state_size=10, action_size_per_zone=5)
        self.energy_storage = EnergyStorageOptimizer()
        self.thermal_visualizer = Thermal3DVisualizer()

        # Stubs
        self.carbon_manager = StubCarbonIntensityManager()
        self.helium_manager = StubHeliumCoolingManager()
        self.federated_manager = StubFederatedLearningManager()
        self.cache = StubCacheManager()
        self.quality_scorer = StubDataQualityScorer()
        self.rate_limiter = StubRateLimiter()
        self.circuit_breakers = {
            'gpu': CircuitBreaker(name="gpu"),
            'nvml': CircuitBreaker(name="nvml"),
            'cfd': CircuitBreaker(name="cfd"),
            'carbon_api': CircuitBreaker(name="carbon_api")
        }
        self.websocket = StubThermalWebSocketDashboard(port=8780)

        # DataCenter configuration
        self.data_center_config = DataCenterConfigModel()

        # RL parameters
        self.state_size = 10
        self.action_size = 5
        self.episode = 0
        self.total_reward = 0.0
        if TORCH_AVAILABLE:
            self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        else:
            self.device = 'cpu'

        # State
        self.optimization_history = deque(maxlen=MAX_OPTIMIZATION_HISTORY)
        self._history_lock = asyncio.Lock()
        self._optimization_semaphore = asyncio.Semaphore(MAX_CONCURRENT_OPTIMIZATIONS)
        self.operation_queue = asyncio.Queue(maxsize=100)
        self._queue_worker = None
        self._running = False
        self.background_tasks: Set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        self.sequence_length = 24

        # Light-weight DQN agent placeholder
        self.dqn_agent = DQNAgent(state_size=self.state_size, action_size=self.action_size)

        logger.info("EnhancedThermalOptimizerV13 v%d.1.0 initialized on %s", DATA_VERSION, self.device)
        logger.info("  ✅ Multi‑Teacher On‑Policy Distillation enabled (replaces bandit)")
        logger.info("     - State‑aware strategy selection with 12 features")
        logger.info("     - 3 teachers: rule‑based, historical ML, stateful Q")
        logger.info("     - Online SGD student with distillation + REINFORCE")
        logger.info("     - Experience replay for stable learning")

    async def start(self):
        self._running = True
        await self.cache.start()
        await self.carbon_manager.update_carbon_intensity('us-east')
        history = await self.storage.get_thermal_history(hours=168)
        if len(history) >= 100 and hasattr(self, 'ensemble_forecaster'):
            try:
                await self.ensemble_forecaster.train(history)
            except Exception:
                pass
        maintenance_history = await self.storage.get_maintenance_history(limit=100)
        if maintenance_history:
            await self.predictive_maintenance.train_model(maintenance_history)
        self._queue_worker = asyncio.create_task(self._process_queue())
        await self.websocket.start()

        tasks = [
            asyncio.create_task(self._health_check_loop()),
            asyncio.create_task(self._cleanup_loop()),
            asyncio.create_task(self._thermal_monitoring_loop()),
            asyncio.create_task(self._sustainability_monitoring_loop()),
            asyncio.create_task(self._federated_learning_loop()),
            asyncio.create_task(self._digital_twin_sync_loop()),
            asyncio.create_task(self._predictive_maintenance_loop()),
            asyncio.create_task(self._quantum_monitor_loop()),
            asyncio.create_task(self._blockchain_monitor_loop()),
            asyncio.create_task(self._auto_optimize_loop()),   # now uses distillation
            asyncio.create_task(self._cloud_sync_loop()),
            asyncio.create_task(self._key_rotation_loop())
        ]
        for task in tasks:
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)
        logger.info("Thermal optimizer started with %d background tasks", len(self.background_tasks))

    # ========================================================================
    # NEW: Build optimization state
    # ========================================================================
    async def _get_optimization_state(self) -> ThermalOptimizationState:
        """Gather context for the distillation agent."""
        # Current metrics
        try:
            pue = PUE_METRIC._value.get() if PROMETHEUS_AVAILABLE else 1.5
            if pue is None:
                pue = 1.5
        except Exception:
            pue = 1.5

        avg_temp = float(np.mean([r.avg_server_temp_c for r in self.optimization_history])) if self.optimization_history else 25.0
        max_temp = float(np.max([r.max_server_temp_c for r in self.optimization_history])) if self.optimization_history else 30.0
        carbon = await self.carbon_manager.get_current_intensity()
        battery = await self.energy_storage.get_battery_status()
        storage_level = battery.get('charge_percentage', 50.0)
        workload = random.uniform(50, 90)  # stub; could be from real monitoring

        # Digital twin
        twin_summary = await self.digital_twin.get_digital_twin_summary()
        node_count = twin_summary.get('total_nodes', 1)
        avg_node_power = twin_summary.get('total_power_kw', 1.0) / max(node_count, 1)
        cooling_util = 50.0  # stub

        # Predictive maintenance
        maintenance = await self.predictive_maintenance.get_maintenance_schedule()
        equipment_risk = 0.0
        if maintenance.get('pending_maintenance', 0) > 0:
            equipment_risk = min(1.0, maintenance['pending_maintenance'] / 10.0)

        # Time
        now = datetime.now()
        hour = now.hour
        weekend = now.weekday() >= 5

        return ThermalOptimizationState(
            pue=float(pue),
            avg_temp_c=avg_temp,
            max_temp_c=max_temp,
            carbon_intensity_gco2=float(carbon),
            energy_storage_level_pct=float(storage_level),
            workload_pct=float(workload),
            node_count=int(node_count),
            avg_node_power_kw=float(avg_node_power),
            cooling_capacity_utilization=float(cooling_util),
            equipment_risk_score=float(equipment_risk),
            hour_of_day=int(hour),
            is_weekend=bool(weekend)
        )

    # ========================================================================
    # Modified _execute_optimization to use distillation optimizer
    # ========================================================================
    async def _execute_optimization(self, operation: Dict) -> ThermalOptimizationResult:
        async with self._optimization_semaphore:
            await self.rate_limiter.wait_and_acquire()
            start_time = time.time()
            method = operation.get('method', 'rl')
            use_multi_zone = operation.get('use_multi_zone', False)
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            temperature = 25 + float(np.random.normal(0, 2))
            cooling_energy = 100 + float(np.random.normal(0, 10))
            it_energy = 200 + float(np.random.normal(0, 20))

            # --- Distillation: select strategy ---
            state = await self._get_optimization_state()
            strategy, action_idx, state_vec, teacher_probs = await self.distillation_optimizer.optimize_thermal(state, exploration=True)

            # Apply strategy modifications
            if strategy == 'performance':
                # Reduce cooling energy target (more aggressive cooling)
                cooling_energy = max(50.0, cooling_energy * 0.9)
            elif strategy == 'carbon':
                # If carbon high, we might use energy storage discharge
                if carbon_intensity > 500:
                    storage_result = await self.energy_storage.optimize_storage(carbon_intensity, cooling_energy)
                    if storage_result.get('action') == 'discharge':
                        cooling_energy -= storage_result.get('amount_kwh', 0.0) * 0.5
            elif strategy == 'cost':
                # Reduce cooling energy to save cost
                cooling_energy *= 0.95
            elif strategy == 'adaptive':
                # Use dynamic adjustment based on historical data
                if self.optimization_history:
                    avg_pue = float(np.mean([r.pue for r in list(self.optimization_history)[-10:]]))
                    if avg_pue > 1.6:
                        cooling_energy *= 0.95
            # (hybrid does nothing special)

            # Perform the actual optimization (simulated here)
            action = 0
            if method == 'rl' and hasattr(self, 'dqn_agent'):
                state_rl = np.random.randn(self.state_size)
                action = self.dqn_agent.select_action(state_rl)
                temperature -= action * 0.5
                cooling_energy += action * 2

            zone_temperatures = {}
            if use_multi_zone and self.multi_zone_agent:
                for zone in self.multi_zone_agent.zone_ids:
                    state_zone = np.random.randn(self.state_size)
                    action_zone = self.multi_zone_agent.select_zone_action(zone, state_zone)
                    temp = 25 + float(np.random.normal(0, 2)) - action_zone * 0.3
                    zone_temperatures[zone] = max(15.0, min(40.0, temp))
                    if PROMETHEUS_AVAILABLE:
                        try:
                            MULTI_ZONE_ACTIONS.labels(zone=zone).inc()
                        except Exception:
                            pass

            storage_result = await self.energy_storage.optimize_storage(carbon_intensity, cooling_energy)
            pue = float((cooling_energy + it_energy) / max(1.0, it_energy))
            carbon_footprint = float((cooling_energy + it_energy) * carbon_intensity / 1000.0)
            carbon_savings = await self.carbon_manager.calculate_carbon_savings(max(0.0, cooling_energy - 50.0))
            helium_metrics = await self.helium_manager.get_efficiency_metrics()
            sustainability_score = self._calculate_sustainability_score(
                pue=pue,
                renewable_pct=self.data_center_config.renewable_energy_pct,
                carbon_intensity=carbon_intensity,
                helium_efficiency=helium_metrics.get('current_efficiency', 0.0)
            )

            result = ThermalOptimizationResult(
                total_energy_kw=it_energy + cooling_energy,
                cooling_energy_kw=cooling_energy,
                it_energy_kw=it_energy,
                pue=pue,
                avg_server_temp_c=temperature,
                max_server_temp_c=temperature + 2,
                carbon_footprint_kg_per_hour=carbon_footprint,
                carbon_intensity_gco2_per_kwh=carbon_intensity,
                carbon_savings_kg=carbon_savings,
                helium_usage_liters=helium_metrics.get('total_usage_liters', 0.0),
                helium_efficiency=helium_metrics.get('current_efficiency', 0.0) * 100.0,
                sustainability_score=sustainability_score,
                optimization_time_ms=(time.time() - start_time) * 1000.0,
                gpu_accelerated=(TORCH_AVAILABLE and torch.cuda.is_available()) if TORCH_AVAILABLE else False,
                zone_temperatures=zone_temperatures,
                anomaly_detected=bool(np.random.random() > 0.95),
                rl_action_used=action if method == 'rl' else 0,
                rl_action_description=f"Cooling adjustment: {action if method == 'rl' else 0}"
            )
            result.metadata = {
                'storage_action': storage_result.get('action'),
                'storage_amount_kwh': storage_result.get('amount_kwh'),
                'storage_carbon_saved': storage_result.get('carbon_saved_kg')
            }

            # ---- Quantum signing, blockchain, cloud (unchanged) ----
            result_dict = asdict(result)
            quantum_key = await self.quantum_security.generate_keypair('dilithium')
            signature = await self.quantum_security.sign_thermal_data(result_dict, quantum_key['key_id'])
            result.quantum_signature = signature
            if PROMETHEUS_AVAILABLE:
                try:
                    QUANTUM_SIGNATURES.labels(algorithm='dilithium', status='sign_success').inc()
                except Exception:
                    pass

            data_id = f"thermal_{uuid.uuid4().hex[:8]}"
            data_hash = hashlib.sha256(json.dumps(result_dict, sort_keys=True, default=str).encode()).hexdigest()
            blockchain_result = await self.blockchain.record_thermal_data(
                data_id,
                data_hash,
                {'pue': pue, 'temperature': temperature}
            )
            result.blockchain_tx_hash = blockchain_result.get('tx_hash')
            if PROMETHEUS_AVAILABLE:
                try:
                    BLOCKCHAIN_VERIFICATIONS.labels(status='recorded').inc()
                except Exception:
                    pass

            cloud_data = {'size_gb': 0.001}
            distribution = await self.cloud_distributor.distribute_thermal_data(cloud_data)
            result.cloud_distribution = distribution
            if PROMETHEUS_AVAILABLE:
                try:
                    CLOUD_DISTRIBUTIONS.labels(provider=distribution.get('optimal_provider', 'unknown'), status='success').inc()
                except Exception:
                    pass

            # ---- Compute reward for distillation ----
            reward = 0.0
            # PUE improvement (lower is better)
            if pue < 1.5:
                reward += 0.3
            elif pue > 2.0:
                reward -= 0.1
            # Sustainability score
            reward += 0.2 * (sustainability_score / 100.0)
            # Carbon footprint reduction (lower is better)
            if carbon_footprint < 5.0:
                reward += 0.2
            # Temperature (lower is better)
            if temperature < 28.0:
                reward += 0.3
            reward = max(0.0, min(1.0, reward))

            # Update distillation optimizer
            next_state = await self._get_optimization_state()
            await self.distillation_optimizer.update_after_test(
                state_vec, action_idx, reward, next_state.to_feature_vector(), teacher_probs
            )

            # Store in memory
            async with self._history_lock:
                self.optimization_history.append(result)

            await self.storage.save_thermal_optimization(result)
            await self.storage.save_sustainability_metrics({
                'carbon_intensity': carbon_intensity,
                'carbon_savings': carbon_savings,
                'helium_efficiency': helium_metrics.get('current_efficiency', 0.0),
                'sustainability_score': sustainability_score,
                'pue': pue,
                'renewable_pct': self.data_center_config.renewable_energy_pct
            })

            if PROMETHEUS_AVAILABLE:
                try:
                    THERMAL_OPTIMIZATION_RUNS.labels(method=method, status='success').inc()
                    OPTIMIZATION_DURATION.labels(method=method).observe(result.optimization_time_ms / 1000.0)
                    COOLING_ENERGY.set(cooling_energy)
                    MAX_TEMPERATURE.set(temperature + 2.0)
                    PUE_METRIC.set(pue)
                    SUSTAINABILITY_SCORE.set(sustainability_score)
                except Exception:
                    pass

            await self.websocket.broadcast_thermal_update(result)

            audit_logger.info("Optimization completed: PUE=%.3f, Temp=%.1f°C, Score=%.1f, blockchain=%s...",
                             pue, temperature, sustainability_score,
                             result.blockchain_tx_hash[:16] if result.blockchain_tx_hash else 'N/A')
            return result

    # ========================================================================
    # Background loops (basic implementations)
    # ========================================================================
    async def _process_queue(self):
        while not self._shutdown_event.is_set():
            try:
                operation = await self.operation_queue.get()
                await self._execute_optimization(operation)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(1)

    async def _health_check_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await self.health_check()
            except Exception:
                pass
            await asyncio.sleep(30)

    async def _cleanup_loop(self):
        while not self._shutdown_event.is_set():
            try:
                # run cache cleanup or db maintenance
                await asyncio.sleep(CACHE_CLEANUP_INTERVAL)
            except Exception:
                await asyncio.sleep(5)

    async def _thermal_monitoring_loop(self):
        while not self._shutdown_event.is_set():
            try:
                # placeholder: gather sensor metrics periodically
                await asyncio.sleep(60)
            except Exception:
                await asyncio.sleep(5)

    async def _sustainability_monitoring_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(300)
            except Exception:
                await asyncio.sleep(5)

    async def _federated_learning_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(FEDERATED_AGGREGATION_INTERVAL)
            except Exception:
                await asyncio.sleep(60)

    async def _digital_twin_sync_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await self.digital_twin.get_digital_twin_summary()
            except Exception:
                pass
            await asyncio.sleep(120)

    async def _predictive_maintenance_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await self.predictive_maintenance.get_maintenance_schedule()
            except Exception:
                pass
            await asyncio.sleep(300)

    async def _quantum_monitor_loop(self):
        while not self._shutdown_event.is_set():
            try:
                _ = self.quantum_security.get_quantum_status()
            except Exception:
                pass
            await asyncio.sleep(600)

    async def _blockchain_monitor_loop(self):
        while not self._shutdown_event.is_set():
            try:
                _ = self.blockchain.get_blockchain_status()
            except Exception:
                pass
            await asyncio.sleep(600)

    async def _auto_optimize_loop(self):
        """Periodically log distillation stats."""
        while not self._shutdown_event.is_set():
            try:
                stats = self.distillation_optimizer.get_stats()
                logger.debug("Distillation stats: %s", stats)
                await asyncio.sleep(1800)
            except Exception as e:
                logger.error("Auto optimize error: %s", e)
                await asyncio.sleep(60)

    async def _cloud_sync_loop(self):
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(1800)
            except Exception:
                await asyncio.sleep(60)

    async def _key_rotation_loop(self):
        while not self._shutdown_event.is_set():
            try:
                # rotate keys occasionally (stub)
                await asyncio.sleep(86400)
            except Exception:
                await asyncio.sleep(60)

    # ========================================================================
    # Public methods (unchanged)
    # ========================================================================
    async def update_digital_twin(self, sensor_data: Dict) -> Dict:
        return await self.digital_twin.update_twin(sensor_data)

    async def run_what_if_analysis(self, scenario: Dict) -> Dict:
        return await self.digital_twin.run_what_if_analysis(scenario)

    async def predict_equipment_failure(self, equipment_id: str, sensor_data: Dict) -> Dict:
        return await self.predictive_maintenance.predict_failure(equipment_id, sensor_data)

    async def get_maintenance_schedule(self) -> Dict:
        return await self.predictive_maintenance.get_maintenance_schedule()

    async def get_energy_storage_status(self) -> Dict:
        return await self.energy_storage.get_battery_status()

    async def optimize_energy_storage(self, carbon_intensity: float, cooling_demand: float) -> Dict:
        return await self.energy_storage.optimize_storage(carbon_intensity, cooling_demand)

    async def generate_3d_thermal_map(self) -> Dict:
        nodes = list(self.digital_twin.twin.nodes.values())
        if nodes:
            return await self.thermal_visualizer.generate_thermal_map(nodes)
        return {'error': 'No nodes available'}

    async def get_multi_zone_actions(self, states: Dict[str, np.ndarray]) -> Dict[str, int]:
        zone_actions = {}
        for zone_id, state in states.items():
            if zone_id in self.multi_zone_agent.zone_ids:
                action = self.multi_zone_agent.select_zone_action(zone_id, state)
                zone_actions[zone_id] = action
        return zone_actions

    # ========================================================================
    # Health check and statistics (with distillation stats)
    # ========================================================================
    async def health_check(self) -> Dict:
        try:
            async def _check():
                async with self._history_lock:
                    opt_count = len(self.optimization_history)
                quality_stats = await self.quality_scorer.get_statistics()
                twin_summary = await self.digital_twin.get_digital_twin_summary()
                maintenance = await self.predictive_maintenance.get_maintenance_schedule()
                battery_status = await self.energy_storage.get_battery_status()
                quantum_status = self.quantum_security.get_quantum_status()
                blockchain_status = self.blockchain.get_blockchain_status()
                cloud_status = await self.cloud_distributor.get_distribution_status()
                opt_stats = self.distillation_optimizer.get_stats()
                health_score = 100
                if opt_count == 0:
                    health_score -= 30
                if quality_stats.get('avg_score', 0) < 50:
                    health_score -= 20
                if not quantum_status.get('pqc_available'):
                    health_score -= 10
                if not blockchain_status.get('connected'):
                    health_score -= 10
                if twin_summary.get('total_nodes', 0) == 0:
                    health_score -= 10
                return {
                    'healthy': opt_count > 0,
                    'instance_id': self.instance_id,
                    'version': DATA_VERSION,
                    'optimization_count': opt_count,
                    'health_score': max(0, health_score),
                    'data_quality': quality_stats.get('avg_score', 0),
                    'queue_size': self.operation_queue.qsize(),
                    'digital_twin': twin_summary,
                    'predictive_maintenance': maintenance,
                    'energy_storage': battery_status,
                    'quantum_security': quantum_status,
                    'blockchain': blockchain_status,
                    'distillation': opt_stats,
                    'cloud_distribution': cloud_status,
                    'timestamp': datetime.now().isoformat()
                }
            return await asyncio.wait_for(_check(), timeout=HEALTH_CHECK_TIMEOUT)
        except asyncio.TimeoutError:
            logger.error("Health check timed out")
            return {'healthy': False, 'status': 'timeout', 'instance_id': self.instance_id}

    async def get_statistics(self) -> Dict:
        async with self._history_lock:
            opt_count = len(self.optimization_history)
            if opt_count > 0:
                avg_pue = float(np.mean([r.pue for r in self.optimization_history]))
                avg_temp = float(np.mean([r.avg_server_temp_c for r in self.optimization_history]))
                avg_carbon = float(np.mean([r.carbon_footprint_kg_per_hour for r in self.optimization_history]))
            else:
                avg_pue = avg_temp = avg_carbon = 0.0
        quality_stats = await self.quality_scorer.get_statistics()
        twin_summary = await self.digital_twin.get_digital_twin_summary()
        maintenance = await self.predictive_maintenance.get_maintenance_schedule()
        battery_status = await self.energy_storage.get_battery_status()
        quantum_status = self.quantum_security.get_quantum_status()
        blockchain_status = self.blockchain.get_blockchain_status()
        cloud_status = await self.cloud_distributor.get_distribution_status()
        opt_stats = self.distillation_optimizer.get_stats()
        return {
            'instance_id': self.instance_id,
            'version': DATA_VERSION,
            'optimization_count': opt_count,
            'avg_pue': avg_pue,
            'avg_temperature_c': avg_temp,
            'avg_carbon_footprint_kg_per_hour': avg_carbon,
            'data_quality': quality_stats,
            'digital_twin': twin_summary,
            'predictive_maintenance': maintenance,
            'energy_storage': battery_status,
            'quantum_security': quantum_status,
            'blockchain': blockchain_status,
            'distillation': opt_stats,
            'cloud_distribution': cloud_status,
            'timestamp': datetime.now().isoformat()
        }

    # ========================================================================
    # Shutdown
    # ========================================================================
    async def shutdown(self):
        logger.info("Shutting down EnhancedThermalOptimizerV13 (instance: %s)", self.instance_id)
        self._shutdown_event.set()
        self._running = False
        if self._queue_worker:
            self._queue_worker.cancel()
        for task in self.background_tasks:
            task.cancel()
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        await self.websocket.stop()
        await self.cache.stop()
        await self.carbon_manager.close()
        await self.federated_manager.close()
        final_health = await self.health_check()
        logger.info("Final health score: %.1f", final_health.get('health_score', 0.0))
        logger.info("Shutdown complete")

    # ========================================================================
    # Helpers
    # ========================================================================
    def _calculate_sustainability_score(self, pue: float, renewable_pct: float, carbon_intensity: float, helium_efficiency: float) -> float:
        # Simple scoring function combining inputs into 0-100
        score = 50.0
        # lower pue -> better
        score += max(-20.0, (1.5 - pue) * 20.0)
        score += (renewable_pct - 50.0) * 0.2
        score += max(-10.0, (400.0 - carbon_intensity) * 0.01)
        score += (helium_efficiency - 0.5) * 10.0
        return float(min(100.0, max(0.0, score)))

# ============================================================================
# Backward compatibility alias
# ============================================================================
class EnhancedThermalOptimizerV12(EnhancedThermalOptimizerV13):
    """Legacy class - use EnhancedThermalOptimizerV13."""
    pass

# ============================================================================
# Singleton accessor
# ============================================================================
_thermal_optimizer_instance = None
_thermal_optimizer_lock = asyncio.Lock()

async def get_thermal_optimizer() -> EnhancedThermalOptimizerV13:
    global _thermal_optimizer_instance
    if _thermal_optimizer_instance is None:
        async with _thermal_optimizer_lock:
            if _thermal_optimizer_instance is None:
                _thermal_optimizer_instance = EnhancedThermalOptimizerV13()
                # start asynchronously, but don't block callers too long
                await _thermal_optimizer_instance.start()
    return _thermal_optimizer_instance

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================
async def main():
    print("=" * 80)
    print("Enhanced Thermal Optimizer v13.1.0 - Enterprise Quantum Resilience")
    print("Multi‑Teacher Distillation | Context‑Aware Strategy Selection")
    print("Digital Twin | Predictive Maintenance | Multi-Zone RL | Energy Storage | Quantum Security")
    print("=" * 80)

    optimizer = await get_thermal_optimizer()

    print(f"\n✅ v13.1.0 ENHANCEMENTS:")
    print(f"   ✅ Multi‑Teacher On‑Policy Distillation (replaces bandit)")
    print(f"   ✅ 12‑dimension state context (PUE, temps, carbon, storage, workload, risk, time)")
    print(f"   ✅ 3 teachers: rule‑based, historical ML, stateful Q")
    print(f"   ✅ Online SGD student with distillation + REINFORCE")
    print(f"   ✅ Experience replay for stable learning")
    print(f"   ✅ Improved reward function combining PUE, sustainability, carbon, and temperature")

    # Run a single optimization to demonstrate
    res = await optimizer._execute_optimization({'method': 'rl', 'use_multi_zone': True})
    print("Sample optimization result:", {'pue': res.pue, 'avg_temp': res.avg_server_temp_c, 'sustainability_score': res.sustainability_score})

if __name__ == "__main__":
    asyncio.run(main())
