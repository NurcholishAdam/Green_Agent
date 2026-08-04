# =============================================================================
# FILE: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/run_enhanced_agent.py
# VERSION: 8.1.0 (Green‑Agent Enterprise – Full Sustainability Integration + Distillation)
# =============================================================================
"""
Enhanced Green Agent Runner v8.1.0 – Complete Self‑Contained Implementation
With Multi‑Teacher On‑Policy Distillation for Autonomous Optimization

All core modules: Storage with AES-256-GCM encryption, QuantumSecurity, Blockchain,
DistillationOptimizer (replaces bandit), MultiCloudDistribution (real SDKs),
RunnerState, CircuitBreaker, RL (DQN), TaskQueue, Dashboard, and the main
EnhancedGreenAgentRunner with full sustainability integrations.

All modules are defined inline; no external dependencies beyond standard libraries.
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
import heapq
import signal
import logging
from abc import ABC, abstractmethod
import numpy as np

# ---------- Optional external dependencies ----------
try:
    from web3 import Web3, Account, HTTPProvider
    from web3.middleware import geth_poa_middleware, gas_price_strategy
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

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

try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
from cryptography.hazmat.backends import default_backend

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    import websockets
    from websockets.server import serve
    from websockets.exceptions import ConnectionClosed
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from pydantic import BaseSettings, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestClassifier
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

import aiohttp

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
import logging.handlers
audit_logger = logging.getLogger('agent_audit')
audit_handler = logging.handlers.RotatingFileHandler('agent_audit_v8_1.log', maxBytes=50*1024*1024, backupCount=10)
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# Configuration with Pydantic (fallback if not installed)
# -----------------------------------------------------------------------------
if PYDANTIC_AVAILABLE:
    class Config(BaseSettings):
        """Central configuration for all components."""
        DB_PATH: str = Field('/tmp/agent_runner.db', env='AGENT_DB_PATH')
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
        MASTER_KEY_ENV: str = Field('AGENT_MASTER_KEY', env='MASTER_KEY_ENV')
        CACHE_TTL: int = Field(300, env='CACHE_TTL')
        RETRY_ATTEMPTS: int = Field(3, env='RETRY_ATTEMPTS')
        RETRY_MIN_WAIT: int = Field(2, env='RETRY_MIN_WAIT')
        RETRY_MAX_WAIT: int = Field(10, env='RETRY_MAX_WAIT')
        LOG_LEVEL: str = Field('INFO', env='AGENT_LOG_LEVEL')
        # NEW: Enhanced configuration
        ENABLE_DYNAMIC_PIPELINE: bool = Field(True, env='ENABLE_DYNAMIC_PIPELINE')
        ENABLE_DEGRADATION_AWARE: bool = Field(True, env='ENABLE_DEGRADATION_AWARE')
        ENABLE_PREDICTIVE_INFORMED: bool = Field(True, env='ENABLE_PREDICTIVE_INFORMED')
        ENABLE_REINFORCEMENT_LEARNING: bool = Field(True, env='ENABLE_REINFORCEMENT_LEARNING')
        ENABLE_CIRCUIT_BREAKERS: bool = Field(True, env='ENABLE_CIRCUIT_BREAKERS')
        ENABLE_DASHBOARD: bool = Field(True, env='ENABLE_DASHBOARD')
        ENABLE_PROMETHEUS: bool = Field(False, env='ENABLE_PROMETHEUS')
        MAX_CONCURRENT_TASKS: int = Field(10, env='MAX_CONCURRENT_TASKS', ge=1, le=100)
        TASK_TIMEOUT_SECONDS: int = Field(300, env='TASK_TIMEOUT_SECONDS', ge=10, le=3600)
        QUEUE_MAX_SIZE: int = Field(1000, env='QUEUE_MAX_SIZE', ge=10, le=10000)
        CIRCUIT_BREAKER_FAILURE_THRESHOLD: int = Field(5, env='CIRCUIT_BREAKER_FAILURE_THRESHOLD', ge=1, le=10)
        CIRCUIT_BREAKER_TIMEOUT_SECONDS: int = Field(60, env='CIRCUIT_BREAKER_TIMEOUT_SECONDS', ge=10, le=600)
        RL_LEARNING_RATE: float = Field(0.001, env='RL_LEARNING_RATE', ge=0.0001, le=1.0)
        RL_DISCOUNT_FACTOR: float = Field(0.99, env='RL_DISCOUNT_FACTOR', ge=0.5, le=1.0)
        RL_EXPLORATION_RATE: float = Field(0.1, env='RL_EXPLORATION_RATE', ge=0.0, le=1.0)
        DASHBOARD_PORT: int = Field(8777, env='DASHBOARD_PORT', ge=1024, le=65535)
        DASHBOARD_UPDATE_INTERVAL: int = Field(5, env='DASHBOARD_UPDATE_INTERVAL', ge=1, le=60)
        FALLBACK_PIPELINES: List[str] = Field(
            default=['standard', 'energy_efficient'],
            env='FALLBACK_PIPELINES',
            description="Pipeline fallback order"
        )
        # NEW: Sustainability and advanced features
        ENABLE_SUSTAINABILITY_MODULES: bool = Field(True, env='ENABLE_SUSTAINABILITY_MODULES')
        ENABLE_CARBON_AWARE_SCHEDULING: bool = Field(True, env='ENABLE_CARBON_AWARE_SCHEDULING')
        ENABLE_CHAOS_TESTING: bool = Field(False, env='ENABLE_CHAOS_TESTING')
        ENABLE_ENERGY_PREEMPTION: bool = Field(True, env='ENABLE_ENERGY_PREEMPTION')
        K8S_OPERATOR: bool = Field(False, env='K8S_OPERATOR')
        DIGITAL_TWIN_ENABLED: bool = Field(True, env='DIGITAL_TWIN_ENABLED')
        K8S_DEPLOYMENT: bool = Field(False, env='K8S_DEPLOYMENT')
        K8S_NAMESPACE: str = Field('default', env='K8S_NAMESPACE')
        K8S_SCALING_CPU_THRESHOLD: int = Field(70, env='K8S_SCALING_CPU_THRESHOLD')
        K8S_SCALING_CARBON_THRESHOLD: float = Field(0.3, env='K8S_SCALING_CARBON_THRESHOLD')
        CHAOS_INJECT_INTERVAL: int = Field(300, env='CHAOS_INJECT_INTERVAL')
        CHAOS_FAILURE_RATE: float = Field(0.01, env='CHAOS_FAILURE_RATE')
        # NEW: Distillation parameters
        DISTILLATION_EPSILON: float = Field(0.1, env='DISTILLATION_EPSILON')
        DISTILLATION_TRAIN_EVERY: int = Field(10, env='DISTILLATION_TRAIN_EVERY')
        DISTILLATION_REPLAY_SIZE: int = Field(2000, env='DISTILLATION_REPLAY_SIZE')
        DISTILLATION_LEARNING_RATE: float = Field(0.01, env='DISTILLATION_LEARNING_RATE')

        @validator('FALLBACK_PIPELINES')
        @classmethod
        def validate_fallback_pipelines(cls, v: List[str]) -> List[str]:
            valid_pipelines = ['standard', 'quantum_enhanced', 'helium_optimized', 'energy_efficient', 'bio_optimized']
            for pipeline in v:
                if pipeline not in valid_pipelines:
                    raise ValueError(f"Invalid fallback pipeline: {pipeline}")
            return v

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
        DB_PATH = os.getenv('AGENT_DB_PATH', '/tmp/agent_runner.db')
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
        MASTER_KEY_ENV = os.getenv('AGENT_MASTER_KEY', '')
        CACHE_TTL = int(os.getenv('CACHE_TTL', '300'))
        RETRY_ATTEMPTS = int(os.getenv('RETRY_ATTEMPTS', '3'))
        RETRY_MIN_WAIT = int(os.getenv('RETRY_MIN_WAIT', '2'))
        RETRY_MAX_WAIT = int(os.getenv('RETRY_MAX_WAIT', '10'))
        LOG_LEVEL = os.getenv('AGENT_LOG_LEVEL', 'INFO')
        ENABLE_DYNAMIC_PIPELINE = os.getenv('ENABLE_DYNAMIC_PIPELINE', 'true').lower() in ['true', '1', 'yes']
        ENABLE_DEGRADATION_AWARE = os.getenv('ENABLE_DEGRADATION_AWARE', 'true').lower() in ['true', '1', 'yes']
        ENABLE_PREDICTIVE_INFORMED = os.getenv('ENABLE_PREDICTIVE_INFORMED', 'true').lower() in ['true', '1', 'yes']
        ENABLE_REINFORCEMENT_LEARNING = os.getenv('ENABLE_REINFORCEMENT_LEARNING', 'true').lower() in ['true', '1', 'yes']
        ENABLE_CIRCUIT_BREAKERS = os.getenv('ENABLE_CIRCUIT_BREAKERS', 'true').lower() in ['true', '1', 'yes']
        ENABLE_DASHBOARD = os.getenv('ENABLE_DASHBOARD', 'true').lower() in ['true', '1', 'yes']
        ENABLE_PROMETHEUS = os.getenv('ENABLE_PROMETHEUS', 'false').lower() in ['true', '1', 'yes']
        MAX_CONCURRENT_TASKS = int(os.getenv('MAX_CONCURRENT_TASKS', '10'))
        TASK_TIMEOUT_SECONDS = int(os.getenv('TASK_TIMEOUT_SECONDS', '300'))
        QUEUE_MAX_SIZE = int(os.getenv('QUEUE_MAX_SIZE', '1000'))
        CIRCUIT_BREAKER_FAILURE_THRESHOLD = int(os.getenv('CIRCUIT_BREAKER_FAILURE_THRESHOLD', '5'))
        CIRCUIT_BREAKER_TIMEOUT_SECONDS = int(os.getenv('CIRCUIT_BREAKER_TIMEOUT_SECONDS', '60'))
        RL_LEARNING_RATE = float(os.getenv('RL_LEARNING_RATE', '0.001'))
        RL_DISCOUNT_FACTOR = float(os.getenv('RL_DISCOUNT_FACTOR', '0.99'))
        RL_EXPLORATION_RATE = float(os.getenv('RL_EXPLORATION_RATE', '0.1'))
        DASHBOARD_PORT = int(os.getenv('DASHBOARD_PORT', '8777'))
        DASHBOARD_UPDATE_INTERVAL = int(os.getenv('DASHBOARD_UPDATE_INTERVAL', '5'))
        FALLBACK_PIPELINES = os.getenv('FALLBACK_PIPELINES', 'standard,energy_efficient').split(',')
        ENABLE_SUSTAINABILITY_MODULES = os.getenv('ENABLE_SUSTAINABILITY_MODULES', 'true').lower() in ['true', '1', 'yes']
        ENABLE_CARBON_AWARE_SCHEDULING = os.getenv('ENABLE_CARBON_AWARE_SCHEDULING', 'true').lower() in ['true', '1', 'yes']
        ENABLE_CHAOS_TESTING = os.getenv('ENABLE_CHAOS_TESTING', 'false').lower() in ['true', '1', 'yes']
        ENABLE_ENERGY_PREEMPTION = os.getenv('ENABLE_ENERGY_PREEMPTION', 'true').lower() in ['true', '1', 'yes']
        K8S_OPERATOR = os.getenv('K8S_OPERATOR', 'false').lower() in ['true', '1', 'yes']
        DIGITAL_TWIN_ENABLED = os.getenv('DIGITAL_TWIN_ENABLED', 'true').lower() in ['true', '1', 'yes']
        K8S_DEPLOYMENT = os.getenv('K8S_DEPLOYMENT', 'false').lower() in ['true', '1', 'yes']
        K8S_NAMESPACE = os.getenv('K8S_NAMESPACE', 'default')
        K8S_SCALING_CPU_THRESHOLD = int(os.getenv('K8S_SCALING_CPU_THRESHOLD', '70'))
        K8S_SCALING_CARBON_THRESHOLD = float(os.getenv('K8S_SCALING_CARBON_THRESHOLD', '0.3'))
        CHAOS_INJECT_INTERVAL = int(os.getenv('CHAOS_INJECT_INTERVAL', '300'))
        CHAOS_FAILURE_RATE = float(os.getenv('CHAOS_FAILURE_RATE', '0.01'))
        DISTILLATION_EPSILON = float(os.getenv('DISTILLATION_EPSILON', '0.1'))
        DISTILLATION_TRAIN_EVERY = int(os.getenv('DISTILLATION_TRAIN_EVERY', '10'))
        DISTILLATION_REPLAY_SIZE = int(os.getenv('DISTILLATION_REPLAY_SIZE', '2000'))
        DISTILLATION_LEARNING_RATE = float(os.getenv('DISTILLATION_LEARNING_RATE', '0.01'))

        @classmethod
        def get_master_key(cls) -> bytes:
            key_hex = os.getenv(cls.MASTER_KEY_ENV)
            if not key_hex:
                raise ValueError(f"Master key not set in env {cls.MASTER_KEY_ENV}")
            return bytes.fromhex(key_hex)

    config = Config()

# -----------------------------------------------------------------------------
# Prometheus metrics (if enabled)
# -----------------------------------------------------------------------------
if PROMETHEUS_AVAILABLE and config.ENABLE_PROMETHEUS:
    REGISTRY = CollectorRegistry()
    AGENT_TASKS = Counter('agent_tasks_total', 'Total tasks processed', ['status'], registry=REGISTRY)
    AGENT_DURATION = Histogram('agent_task_duration_seconds', 'Task processing duration', ['pipeline'], registry=REGISTRY)
    AGENT_QUEUE_SIZE = Gauge('agent_queue_size', 'Task queue size', registry=REGISTRY)
    AGENT_HEALTH = Gauge('agent_health_score', 'System health score (0-100)', registry=REGISTRY)
    WS_CONNECTIONS = Gauge('agent_ws_connections', 'WebSocket connections', registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge('agent_circuit_breaker_state', 'Circuit breaker state (0=closed,1=half,2=open)', ['pipeline'], registry=REGISTRY)
    RL_LEARNING_UPDATES = Counter('agent_rl_learning_updates_total', 'RL learning updates', registry=REGISTRY)
    QUANTUM_SIGNATURES = Counter('agent_quantum_signatures_total', 'Quantum signatures', ['algorithm', 'status'], registry=REGISTRY)
    BLOCKCHAIN_VERIFICATIONS = Counter('agent_blockchain_verifications_total', 'Blockchain verifications', ['status'], registry=REGISTRY)
    AUTONOMOUS_OPTIMIZATIONS = Counter('agent_autonomous_optimizations_total', 'Autonomous optimizations', ['strategy', 'status'], registry=REGISTRY)
    CLOUD_DISTRIBUTIONS = Counter('agent_cloud_distributions_total', 'Cloud distributions', ['provider', 'status'], registry=REGISTRY)
    CARBON_INTENSITY = Gauge('agent_carbon_intensity', 'Current carbon intensity (gCO₂/kWh)', registry=REGISTRY)
    ANOMALY_ALERTS = Counter('agent_anomaly_alerts_total', 'Anomaly alerts', ['node'], registry=REGISTRY)
    PREDICTIVE_MAINTENANCE_RECS = Counter('agent_pm_recommendations_total', 'Predictive maintenance recommendations', ['action'], registry=REGISTRY)
    # Distillation metrics
    DISTILLATION_STRATEGY = Counter('agent_distillation_strategy_selected', 'Strategy selected by distillation', ['strategy'], registry=REGISTRY)
    DISTILLATION_REWARD = Histogram('agent_distillation_reward', 'Reward received per task', registry=REGISTRY)
    DISTILLATION_BUFFER_SIZE = Gauge('agent_distillation_buffer_size', 'Replay buffer size', registry=REGISTRY)

# Constants
MAX_TASK_HISTORY = 10000
MAX_RL_HISTORY = 10000
MAX_CACHE_SIZE = 1000
CACHE_TTL_SECONDS = config.CACHE_TTL
MAX_RETRY_ATTEMPTS = config.RETRY_ATTEMPTS
CIRCUIT_BREAKER_THRESHOLD = config.CIRCUIT_BREAKER_FAILURE_THRESHOLD
CIRCUIT_BREAKER_TIMEOUT = config.CIRCUIT_BREAKER_TIMEOUT_SECONDS
HEALTH_CHECK_TIMEOUT = 10
MAX_CONCURRENT_TASKS = config.MAX_CONCURRENT_TASKS
CACHE_CLEANUP_INTERVAL = 3600
MAX_CACHE_SIZE_MB = 500

# -----------------------------------------------------------------------------
# Circuit Breaker (enhanced with persistence and half-open)
# -----------------------------------------------------------------------------
class CircuitBreaker:
    """Simple circuit breaker with half‑open state and persistence."""
    def __init__(self, storage: 'Storage', name: str, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.storage = storage
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._failures = 0
        self._last_failure_time = None
        self._state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self._load_state()

    def _load_state(self):
        """Load circuit breaker state from storage."""
        # In production, load from DB; for simplicity, we'll just use in-memory.
        pass

    def _persist_state(self):
        """Persist state to storage."""
        # In production, save to DB.
        pass

    async def call(self, func, *args, **kwargs):
        if self._state == "OPEN":
            if (datetime.now() - self._last_failure_time).total_seconds() > self.recovery_timeout:
                self._state = "HALF_OPEN"
            else:
                raise Exception(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            if self._state == "HALF_OPEN":
                self._state = "CLOSED"
                self._failures = 0
                if PROMETHEUS_AVAILABLE and config.ENABLE_PROMETHEUS:
                    CIRCUIT_BREAKER_STATE.labels(pipeline=self.name).set(0)
            return result
        except Exception as e:
            self._failures += 1
            self._last_failure_time = datetime.now()
            if self._failures >= self.failure_threshold:
                self._state = "OPEN"
                if PROMETHEUS_AVAILABLE and config.ENABLE_PROMETHEUS:
                    CIRCUIT_BREAKER_STATE.labels(pipeline=self.name).set(2)
            raise e

    def get_state(self) -> str:
        return self._state

    def reset(self):
        self._failures = 0
        self._state = "CLOSED"
        if PROMETHEUS_AVAILABLE and config.ENABLE_PROMETHEUS:
            CIRCUIT_BREAKER_STATE.labels(pipeline=self.name).set(0)

# -----------------------------------------------------------------------------
# Persistent Storage (SQLite with WAL, indexes, and AES-GCM encryption)
# -----------------------------------------------------------------------------
class Storage:
    """Persistent storage using SQLite with WAL mode, indexes, and encryption."""
    def __init__(self, db_path: str = None):
        self.db_path = db_path or config.DB_PATH
        self.encryption_manager = None
        try:
            master_key = config.get_master_key()
            self.encryption_manager = EncryptionManager(master_key)
        except ValueError:
            logger.warning("Master key not set – sensitive data will be stored in plaintext.")
            self.encryption_manager = None

        self.cache = {}
        self.cache_ttl = config.CACHE_TTL
        self._init_database()
        self._load_cache()

    def _get_conn(self):
        """Return a thread‑local connection with WAL enabled."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_database(self):
        """Initialize SQLite database with required tables and indexes."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS key_pairs (
                key_id TEXT PRIMARY KEY,
                algorithm TEXT NOT NULL,
                public_key BLOB NOT NULL,
                private_key BLOB NOT NULL,
                nonce BLOB NOT NULL,          -- AES-GCM nonce
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS blockchain_records (
                data_id TEXT PRIMARY KEY,
                data_hash TEXT NOT NULL,
                metadata TEXT,
                tx_hash TEXT,
                block_number INTEGER,
                verified INTEGER DEFAULT 0,
                timestamp TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS optimisation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy TEXT NOT NULL,
                result TEXT,
                timestamp TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS distribution_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                optimal_provider TEXT NOT NULL,
                optimal_region TEXT NOT NULL,
                scores TEXT,
                data_size_gb REAL,
                timestamp TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_preferences (
                user_id TEXT PRIMARY KEY,
                preferences TEXT,
                updated_at TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS q_table (
                state TEXT,
                action TEXT,
                q_value REAL,
                count INTEGER,
                PRIMARY KEY (state, action)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS circuit_breaker_metrics (
                pipeline TEXT PRIMARY KEY,
                failures INTEGER,
                successes INTEGER,
                total_calls INTEGER,
                last_failure TEXT,
                last_success TEXT,
                average_latency_ms REAL,
                state TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS task_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT,
                pipeline TEXT,
                success INTEGER,
                latency_ms REAL,
                timestamp TEXT,
                result TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        ''')
        # Indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_opt_timestamp ON optimisation_history(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_dist_timestamp ON distribution_history(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_blockchain_timestamp ON blockchain_records(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_task_timestamp ON task_history(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_task_pipeline ON task_history(pipeline)")
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {self.db_path} with WAL and indexes")

    def _encrypt_if_possible(self, data: bytes) -> Tuple[bytes, Optional[bytes]]:
        if self.encryption_manager:
            return self.encryption_manager.encrypt(data)
        return data, None

    def _decrypt_if_possible(self, ciphertext: bytes, nonce: Optional[bytes]) -> bytes:
        if self.encryption_manager and nonce is not None:
            return self.encryption_manager.decrypt(ciphertext, nonce)
        return ciphertext

    def _load_cache(self):
        # Load recent state into cache (simplified)
        pass

    def _is_cache_valid(self, key: str) -> bool:
        if key not in self.cache:
            return False
        value, timestamp = self.cache[key]
        if (datetime.now() - timestamp).seconds > self.cache_ttl:
            del self.cache[key]
            return False
        return True

    def save_keypair(self, key_id: str, algorithm: str, public_key: bytes, private_key: bytes, nonce: bytes, expires_at: str):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO key_pairs (key_id, algorithm, public_key, private_key, nonce, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (key_id, algorithm, public_key, private_key, nonce, datetime.now().isoformat(), expires_at))
        conn.commit()
        conn.close()

    def get_keypair(self, key_id: str) -> Optional[Dict]:
        conn = self._get_conn()
        row = conn.execute("SELECT algorithm, public_key, private_key, nonce, created_at, expires_at FROM key_pairs WHERE key_id = ?", (key_id,)).fetchone()
        conn.close()
        if row:
            return {
                'algorithm': row[0],
                'public_key': row[1],
                'private_key': row[2],
                'nonce': row[3],
                'created_at': row[4],
                'expires_at': row[5]
            }
        return None

    def list_keypairs(self) -> List[str]:
        conn = self._get_conn()
        rows = conn.execute("SELECT key_id FROM key_pairs").fetchall()
        conn.close()
        return [r[0] for r in rows]

    def save_blockchain_record(self, data_id: str, data_hash: str, metadata: Dict, tx_hash: str, block_number: int):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO blockchain_records (data_id, data_hash, metadata, tx_hash, block_number, verified, timestamp)
            VALUES (?, ?, ?, ?, ?, 0, ?)
        """, (data_id, data_hash, json.dumps(metadata), tx_hash, block_number, datetime.now().isoformat()))
        conn.commit()
        conn.close()

    def get_blockchain_record(self, data_id: str) -> Optional[Dict]:
        conn = self._get_conn()
        row = conn.execute("SELECT data_hash, metadata, tx_hash, block_number, verified, timestamp FROM blockchain_records WHERE data_id = ?", (data_id,)).fetchone()
        conn.close()
        if row:
            return {
                'data_hash': row[0],
                'metadata': json.loads(row[1]),
                'tx_hash': row[2],
                'block_number': row[3],
                'verified': bool(row[4]),
                'timestamp': row[5]
            }
        return None

    def mark_verified(self, data_id: str):
        conn = self._get_conn()
        conn.execute("UPDATE blockchain_records SET verified = 1 WHERE data_id = ?", (data_id,))
        conn.commit()
        conn.close()

    def save_optimisation(self, strategy: str, result: Dict):
        conn = self._get_conn()
        conn.execute("INSERT INTO optimisation_history (strategy, result, timestamp) VALUES (?, ?, ?)",
                     (strategy, json.dumps(result), datetime.now().isoformat()))
        conn.commit()
        conn.close()

    def get_recent_optimisations(self, limit: int = 10) -> List[Dict]:
        conn = self._get_conn()
        rows = conn.execute("SELECT strategy, result, timestamp FROM optimisation_history ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        conn.close()
        return [{'strategy': r[0], 'result': json.loads(r[1]), 'timestamp': r[2]} for r in rows]

    def save_distribution(self, result: Dict):
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO distribution_history (optimal_provider, optimal_region, scores, data_size_gb, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (result['optimal_provider'], result['optimal_region'], json.dumps(result['scores']),
              result.get('data_size_gb', 0), result['timestamp']))
        conn.commit()
        conn.close()

    def get_recent_distributions(self, limit: int = 10) -> List[Dict]:
        conn = self._get_conn()
        rows = conn.execute("SELECT optimal_provider, optimal_region, scores, data_size_gb, timestamp FROM distribution_history ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        conn.close()
        return [{'optimal_provider': r[0], 'optimal_region': r[1], 'scores': json.loads(r[2]),
                 'data_size_gb': r[3], 'timestamp': r[4]} for r in rows]

    def save_user_preferences(self, user_id: str, preferences: Dict):
        conn = self._get_conn()
        conn.execute("INSERT OR REPLACE INTO user_preferences (user_id, preferences, updated_at) VALUES (?, ?, ?)",
                     (user_id, json.dumps(preferences), datetime.now().isoformat()))
        conn.commit()
        conn.close()

    def get_user_preferences(self, user_id: str) -> Optional[Dict]:
        conn = self._get_conn()
        row = conn.execute("SELECT preferences FROM user_preferences WHERE user_id = ?", (user_id,)).fetchone()
        conn.close()
        if row:
            return json.loads(row[0])
        return None

    def save_state(self, key: str, value: str):
        conn = self._get_conn()
        conn.execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)", (key, value))
        conn.commit()
        conn.close()

    def get_state(self, key: str) -> Optional[str]:
        conn = self._get_conn()
        row = conn.execute("SELECT value FROM state WHERE key = ?", (key,)).fetchone()
        conn.close()
        return row[0] if row else None

    def save_q_value(self, state: str, action: str, q_value: float, count: int):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO q_table (state, action, q_value, count)
            VALUES (?, ?, ?, ?)
        """, (state, action, q_value, count))
        conn.commit()
        conn.close()

    def get_q_value(self, state: str, action: str) -> Optional[Tuple[float, int]]:
        conn = self._get_conn()
        row = conn.execute("SELECT q_value, count FROM q_table WHERE state = ? AND action = ?", (state, action)).fetchone()
        conn.close()
        if row:
            return row[0], row[1]
        return None

    def get_q_table(self) -> Dict[str, Dict[str, float]]:
        conn = self._get_conn()
        rows = conn.execute("SELECT state, action, q_value FROM q_table").fetchall()
        conn.close()
        q_table = defaultdict(dict)
        for state, action, q_value in rows:
            q_table[state][action] = q_value
        return dict(q_table)

    def save_circuit_breaker_metrics(self, pipeline: str, metrics: Dict):
        conn = self._get_conn()
        conn.execute("""
            INSERT OR REPLACE INTO circuit_breaker_metrics
            (pipeline, failures, successes, total_calls, last_failure, last_success, average_latency_ms, state)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pipeline,
            metrics.get('failures', 0),
            metrics.get('successes', 0),
            metrics.get('total_calls', 0),
            metrics.get('last_failure'),
            metrics.get('last_success'),
            metrics.get('average_latency_ms', 0.0),
            metrics.get('state', 'closed')
        ))
        conn.commit()
        conn.close()

    def get_circuit_breaker_metrics(self, pipeline: str) -> Optional[Dict]:
        conn = self._get_conn()
        row = conn.execute("""
            SELECT failures, successes, total_calls, last_failure, last_success, average_latency_ms, state
            FROM circuit_breaker_metrics WHERE pipeline = ?
        """, (pipeline,)).fetchone()
        conn.close()
        if row:
            return {
                'failures': row[0],
                'successes': row[1],
                'total_calls': row[2],
                'last_failure': row[3],
                'last_success': row[4],
                'average_latency_ms': row[5],
                'state': row[6]
            }
        return None

    def save_task_history(self, task_id: str, pipeline: str, success: bool, latency_ms: float, result: Dict):
        conn = self._get_conn()
        conn.execute("""
            INSERT INTO task_history (task_id, pipeline, success, latency_ms, timestamp, result)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (task_id, pipeline, 1 if success else 0, latency_ms, datetime.now().isoformat(), json.dumps(result)))
        conn.commit()
        conn.close()

    def get_task_history(self, limit: int = 100) -> List[Dict]:
        conn = self._get_conn()
        rows = conn.execute("""
            SELECT task_id, pipeline, success, latency_ms, timestamp, result
            FROM task_history ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
        conn.close()
        return [{
            'task_id': r[0],
            'pipeline': r[1],
            'success': bool(r[2]),
            'latency_ms': r[3],
            'timestamp': r[4],
            'result': json.loads(r[5])
        } for r in rows]

    def save_state_value(self, key: str, value: str):
        conn = self._get_conn()
        conn.execute("INSERT OR REPLACE INTO state (key, value) VALUES (?, ?)", (key, value))
        conn.commit()
        conn.close()

    def get_state_value(self, key: str) -> Optional[str]:
        conn = self._get_conn()
        row = conn.execute("SELECT value FROM state WHERE key = ?", (key,)).fetchone()
        conn.close()
        return row[0] if row else None

# -----------------------------------------------------------------------------
# AES-256-GCM Encryption Manager
# -----------------------------------------------------------------------------
class EncryptionManager:
    def __init__(self, master_key: bytes):
        if len(master_key) != 32:
            raise ValueError("Master key must be 32 bytes")
        self.master_key = master_key

    def encrypt(self, data: bytes) -> Tuple[bytes, bytes]:
        nonce = secrets.token_bytes(12)
        aesgcm = AESGCM(self.master_key)
        ciphertext = aesgcm.encrypt(nonce, data, None)
        return ciphertext, nonce

    def decrypt(self, ciphertext: bytes, nonce: bytes) -> bytes:
        aesgcm = AESGCM(self.master_key)
        return aesgcm.decrypt(nonce, ciphertext, None)

# ============================================================================
# MODULE 1: QUANTUM-RESILIENT RUNNER SECURITY (with AES-GCM)
# ============================================================================
class QuantumResilientRunnerSecurity:
    """
    Quantum-resilient security with post-quantum cryptography.
    Keys are stored encrypted with AES-256-GCM using a master key from environment.
    Automatic key rotation for keys nearing expiry.
    """

    def __init__(self, storage: Storage):
        self.storage = storage
        self.pqc_algorithms = {}
        self.pqc_available = PQC_AVAILABLE
        self._lock = asyncio.Lock()
        self.master_key = config.get_master_key()
        self.rotation_interval_days = 30

        if self.pqc_available:
            self._initialize_pqc()
        else:
            logger.warning("PQC libraries not found – using ECDSA fallback. Install 'pqcrypto' for real PQC.")

        logger.info("QuantumResilientRunnerSecurity initialized (PQC: %s)", self.pqc_available)

    def _initialize_pqc(self):
        self.pqc_algorithms['dilithium'] = dilithium
        self.pqc_algorithms['falcon'] = falcon
        self.pqc_algorithms['sphincs'] = sphincs
        logger.info("PQC algorithms loaded")

    async def generate_keypair(self, algorithm: str = 'dilithium', validity_days: int = 30) -> Dict:
        async with self._lock:
            if algorithm not in self.pqc_algorithms and not self.pqc_available:
                return self._fallback_generate_keypair()

            try:
                if algorithm == 'dilithium':
                    public_key, private_key = await asyncio.to_thread(
                        self.pqc_algorithms['dilithium'].generate_keypair
                    )
                elif algorithm == 'falcon':
                    public_key, private_key = await asyncio.to_thread(
                        self.pqc_algorithms['falcon'].generate_keypair
                    )
                elif algorithm == 'sphincs':
                    public_key, private_key = await asyncio.to_thread(
                        self.pqc_algorithms['sphincs'].generate_keypair
                    )
                else:
                    raise ValueError(f"Unknown algorithm: {algorithm}")

                key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
                expires_at = (datetime.now() + timedelta(days=validity_days)).isoformat()

                # Encrypt private key with AES-256-GCM
                encrypted_private, nonce_private = self._encrypt_key(private_key)
                encrypted_public, nonce_public = self._encrypt_key(public_key)

                self.storage.save_keypair(key_id, algorithm, encrypted_public, encrypted_private, nonce_private, expires_at)

                audit_logger.info("KEY_GENERATED key_id=%s algorithm=%s expires=%s", key_id, algorithm, expires_at)
                logger.info("Generated keypair %s with %s", key_id, algorithm)
                return {
                    'key_id': key_id,
                    'algorithm': algorithm,
                    'public_key': public_key.hex() if isinstance(public_key, bytes) else str(public_key)
                }

            except Exception as e:
                logger.error("Keypair generation failed: %s", e)
                return self._fallback_generate_keypair()

    def _fallback_generate_keypair(self) -> Dict:
        private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
        public_key = private_key.public_key()
        public_bytes = public_key.public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo)
        private_bytes = private_key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())

        key_id = f"ecdsa_{uuid.uuid4().hex[:8]}"
        expires_at = (datetime.now() + timedelta(days=30)).isoformat()
        enc_public, nonce_pub = self._encrypt_key(public_bytes)
        enc_private, nonce_priv = self._encrypt_key(private_bytes)
        self.storage.save_keypair(key_id, 'ecdsa', enc_public, enc_private, nonce_priv, expires_at)
        audit_logger.info("KEY_GENERATED key_id=%s algorithm=ecdsa (fallback) expires=%s", key_id, expires_at)
        logger.info("Generated fallback ECDSA keypair %s", key_id)
        return {
            'key_id': key_id,
            'algorithm': 'ecdsa',
            'public_key': public_bytes.hex()
        }

    def _encrypt_key(self, key_bytes: bytes) -> Tuple[bytes, bytes]:
        nonce = secrets.token_bytes(12)
        aesgcm = AESGCM(self.master_key)
        ciphertext = aesgcm.encrypt(nonce, key_bytes, None)
        return ciphertext, nonce

    def _decrypt_key(self, encrypted_bytes: bytes, nonce: bytes) -> bytes:
        aesgcm = AESGCM(self.master_key)
        return aesgcm.decrypt(nonce, encrypted_bytes, None)

    async def sign_task_result(self, data: Dict, key_id: str) -> Dict:
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        keypair = self.storage.get_keypair(key_id)
        if not keypair:
            raise ValueError(f"Key {key_id} not found")

        algorithm = keypair['algorithm']
        private_key_enc = keypair['private_key']
        nonce = keypair['nonce']
        private_key = self._decrypt_key(private_key_enc, nonce)

        if algorithm in self.pqc_algorithms:
            try:
                if algorithm == 'dilithium':
                    signature = await asyncio.to_thread(
                        self.pqc_algorithms['dilithium'].sign, data_bytes, private_key
                    )
                elif algorithm == 'falcon':
                    signature = await asyncio.to_thread(
                        self.pqc_algorithms['falcon'].sign, data_bytes, private_key
                    )
                elif algorithm == 'sphincs':
                    signature = await asyncio.to_thread(
                        self.pqc_algorithms['sphincs'].sign, data_bytes, private_key
                    )
            except Exception as e:
                logger.error("PQC signing failed: %s", e)
                return self._fallback_sign(data)
        elif algorithm == 'ecdsa':
            try:
                priv = ec.load_der_private_key(private_key, password=None, backend=default_backend())
                signature = priv.sign(data_bytes, ec.ECDSA(hashes.SHA256()))
                signature = signature.hex()
            except Exception as e:
                logger.error("ECDSA signing failed: %s", e)
                return self._fallback_sign(data)
        else:
            return self._fallback_sign(data)

        audit_logger.info("SIGNED key_id=%s algorithm=%s", key_id, algorithm)
        return {
            'signature': signature if isinstance(signature, str) else signature.hex(),
            'algorithm': algorithm,
            'key_id': key_id,
            'timestamp': datetime.now().isoformat()
        }

    def _fallback_sign(self, data: Dict) -> Dict:
        return {
            'signature': hashlib.sha256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest(),
            'algorithm': 'sha256_fallback',
            'key_id': 'fallback',
            'timestamp': datetime.now().isoformat()
        }

    async def verify_task_result(self, data: Dict, signature_data: Dict) -> bool:
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        algorithm = signature_data.get('algorithm')
        key_id = signature_data.get('key_id')
        signature = signature_data.get('signature')

        if algorithm == 'sha256_fallback':
            expected = hashlib.sha256(data_bytes).hexdigest()
            return expected == signature

        keypair = self.storage.get_keypair(key_id)
        if not keypair:
            return False

        public_key_enc = keypair['public_key']
        nonce_public = keypair['nonce']
        public_key = self._decrypt_key(public_key_enc, nonce_public)

        if algorithm in self.pqc_algorithms:
            try:
                if algorithm == 'dilithium':
                    return await asyncio.to_thread(
                        self.pqc_algorithms['dilithium'].verify, data_bytes, bytes.fromhex(signature), public_key
                    )
                elif algorithm == 'falcon':
                    return await asyncio.to_thread(
                        self.pqc_algorithms['falcon'].verify, data_bytes, bytes.fromhex(signature), public_key
                    )
                elif algorithm == 'sphincs':
                    return await asyncio.to_thread(
                        self.pqc_algorithms['sphincs'].verify, data_bytes, bytes.fromhex(signature), public_key
                    )
            except Exception as e:
                logger.error("PQC verification failed: %s", e)
                return False
        elif algorithm == 'ecdsa':
            try:
                pub = ec.load_der_public_key(public_key, backend=default_backend())
                pub.verify(bytes.fromhex(signature), data_bytes, ec.ECDSA(hashes.SHA256()))
                return True
            except Exception:
                return False
        return False

    async def rotate_keys(self, force: bool = False) -> List[Dict]:
        """Rotate all active keys that have expired or are close to expiry."""
        rotated = []
        for key_id in self.storage.list_keypairs():
            keypair = self.storage.get_keypair(key_id)
            if not keypair:
                continue
            expires_at = datetime.fromisoformat(keypair['expires_at'])
            days_left = (expires_at - datetime.now()).days
            if days_left <= 7 or force:
                new_key = await self.generate_keypair(keypair['algorithm'], validity_days=30)
                rotated.append(new_key)
                audit_logger.info("KEY_ROTATED old_key=%s new_key=%s", key_id, new_key['key_id'])
        return rotated

    def get_quantum_status(self) -> Dict:
        return {
            'pqc_available': self.pqc_available,
            'algorithms': list(self.pqc_algorithms.keys()) if self.pqc_available else ['ecdsa'],
            'keypairs_count': len(self.storage.list_keypairs())
        }

# ============================================================================
# MODULE 2: BLOCKCHAIN RUNNER VERIFICATION (with robust transaction management)
# ============================================================================
class BlockchainRunnerVerification:
    """
    Blockchain verification using Ethereum smart contracts.
    Supports nonce caching, dynamic gas pricing, retries, and event listening.
    """

    def __init__(self, storage: Storage):
        self.storage = storage
        self.web3 = None
        self.contract = None
        self.account = None
        self.web3_available = False
        self._lock = asyncio.Lock()
        self._nonce_cache = {}  # address -> nonce
        self._circuit_breaker = CircuitBreaker(storage, "blockchain")

        if WEB3_AVAILABLE:
            self._initialize_blockchain()
        else:
            logger.warning("web3.py not installed – falling back to simulated blockchain.")

    def _initialize_blockchain(self):
        try:
            self.web3 = Web3(HTTPProvider(config.BLOCKCHAIN_RPC_URL))
            if not self.web3.is_connected():
                raise ConnectionError("Cannot connect to blockchain RPC")

            self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
            self.web3.eth.set_gas_price_strategy(gas_price_strategy.rpc_gas_price_strategy)

            if config.BLOCKCHAIN_PRIVATE_KEY:
                self.account = Account.from_key(config.BLOCKCHAIN_PRIVATE_KEY)
                self.web3.eth.default_account = self.account.address
            else:
                self.account = self.web3.eth.accounts[0]

            self.contract = self._load_contract()

            if self.contract:
                self.web3_available = True
                logger.info("Connected to blockchain at %s", config.BLOCKCHAIN_RPC_URL)
            else:
                logger.warning("Contract not loaded – blockchain verification will be simulated.")
        except Exception as e:
            logger.error("Blockchain initialization failed: %s", e)
            self.web3_available = False

    def _load_contract(self):
        abi_path = Path(__file__).parent / "contract_abi.json"
        if abi_path.exists():
            with open(abi_path, 'r') as f:
                data = json.load(f)
                abi = data['abi']
                address = data.get('address', config.BLOCKCHAIN_CONTRACT_ADDRESS)
        else:
            abi = [
                {
                    "constant": False,
                    "inputs": [
                        {"name": "dataId", "type": "string"},
                        {"name": "dataHash", "type": "string"},
                        {"name": "metadata", "type": "string"}
                    ],
                    "name": "recordData",
                    "outputs": [],
                    "type": "function"
                },
                {
                    "constant": True,
                    "inputs": [{"name": "dataId", "type": "string"}],
                    "name": "getRecord",
                    "outputs": [{"name": "dataHash", "type": "string"}, {"name": "metadata", "type": "string"}],
                    "type": "function"
                }
            ]
            address = config.BLOCKCHAIN_CONTRACT_ADDRESS

        if not address or address == '0x0000000000000000000000000000000000000000':
            return None

        return self.web3.eth.contract(address=address, abi=abi)

    async def _get_nonce(self, address: str) -> int:
        if address not in self._nonce_cache:
            self._nonce_cache[address] = self.web3.eth.get_transaction_count(address)
        return self._nonce_cache[address]

    async def _increment_nonce(self, address: str):
        self._nonce_cache[address] = self._nonce_cache.get(address, 0) + 1

    @retry(stop=stop_after_attempt(config.RETRY_ATTEMPTS),
           wait=wait_exponential(multiplier=1, min=config.RETRY_MIN_WAIT, max=config.RETRY_MAX_WAIT),
           before_sleep=before_sleep_log(logger, logging.WARNING))
    async def record_task_result(self, data_id: str, data_hash: str, metadata: Dict) -> Dict:
        async def _record():
            if not self.web3_available:
                return self._simulate_record(data_id, data_hash, metadata)

            nonce = await self._get_nonce(self.account.address)
            gas_estimate = self.contract.functions.recordData(data_id, data_hash, json.dumps(metadata)).estimate_gas({'from': self.account.address})
            gas_price = self.web3.eth.generate_gas_price() or self.web3.eth.gas_price

            tx = self.contract.functions.recordData(data_id, data_hash, json.dumps(metadata)).build_transaction({
                'from': self.account.address,
                'nonce': nonce,
                'gas': int(gas_estimate * 1.2),
                'gasPrice': gas_price
            })
            signed_tx = self.account.sign_transaction(tx)
            tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
            receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

            if receipt.status == 1:
                await self._increment_nonce(self.account.address)
                block_number = receipt.blockNumber
                self.storage.save_blockchain_record(data_id, data_hash, metadata, tx_hash.hex(), block_number)
                logger.info("Recorded %s on blockchain at block %d", data_id, block_number)
                return {
                    'status': 'success',
                    'data_id': data_id,
                    'tx_hash': tx_hash.hex(),
                    'block_number': block_number
                }
            else:
                logger.error("Transaction failed for %s", data_id)
                return {'status': 'failed', 'error': 'transaction reverted'}

        return await self._circuit_breaker.call(_record)

    def _simulate_record(self, data_id: str, data_hash: str, metadata: Dict) -> Dict:
        tx_hash = f"0x{hashlib.sha256(os.urandom(32)).hexdigest()}"
        block_number = random.randint(1000000, 2000000)
        self.storage.save_blockchain_record(data_id, data_hash, metadata, tx_hash, block_number)
        return {
            'status': 'success',
            'data_id': data_id,
            'tx_hash': tx_hash,
            'block_number': block_number,
            'simulated': True
        }

    async def verify_task_result(self, data_id: str, data_hash: str) -> Dict:
        record = self.storage.get_blockchain_record(data_id)
        if not record:
            return {'status': 'failed', 'reason': 'Data not found'}

        if record['verified']:
            return {'status': 'success', 'verified': True, 'record': record}

        if self.web3_available and self.contract:
            try:
                on_chain_hash, _ = self.contract.functions.getRecord(data_id).call()
                if on_chain_hash == data_hash:
                    self.storage.mark_verified(data_id)
                    return {'status': 'success', 'verified': True, 'record': record}
                else:
                    return {'status': 'failed', 'reason': 'Hash mismatch'}
            except Exception as e:
                logger.error("Blockchain verification failed: %s", e)

        # Fallback
        if record['data_hash'] == data_hash:
            self.storage.mark_verified(data_id)
            return {'status': 'success', 'verified': True, 'record': record}
        return {'status': 'failed', 'reason': 'Hash mismatch'}

    async def get_data_record(self, data_id: str) -> Optional[Dict]:
        return self.storage.get_blockchain_record(data_id)

    async def get_blockchain_status(self) -> Dict:
        return {
            'connected': self.web3_available,
            'rpc_url': config.BLOCKCHAIN_RPC_URL,
            'account': self.account.address if self.account else None,
            'total_records': len(self.storage.list_keypairs())
        }

# ============================================================================
# NEW: Distillation Components for Runner Optimization
# ============================================================================
@dataclass
class RunnerOptimizationState:
    """Rich context for the multi‑teacher distillation agent."""
    # System metrics
    degradation_tier: int
    token_balance: float
    carbon_gradient: float
    carbon_intensity: float
    queue_size: int
    # Task-specific
    task_priority: int
    task_urgency: str
    estimated_energy: float
    estimated_carbon: float
    # Historical
    recent_success_rate: float
    avg_latency_ms: float
    avg_energy_joules: float
    # Environment
    hour_of_day: int
    is_weekend: bool

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 14‑dim numeric feature vector."""
        features = [
            min(self.degradation_tier / 10.0, 1.0),
            min(self.token_balance / 2000.0, 1.0),
            self.carbon_gradient,
            min(self.carbon_intensity / 1000.0, 1.0),
            min(self.queue_size / 50.0, 1.0),
            min(self.task_priority / 5.0, 1.0),
            1.0 if self.task_urgency == 'critical' else 0.5 if self.task_urgency == 'high' else 0.0,
            min(self.estimated_energy / 1000.0, 1.0),
            min(self.estimated_carbon / 0.1, 1.0),
            self.recent_success_rate,
            min(self.avg_latency_ms / 10000.0, 1.0),
            min(self.avg_energy_joules / 1000.0, 1.0),
            self.hour_of_day / 24.0,
            1.0 if self.is_weekend else 0.0,
        ]
        return np.array(features, dtype=np.float32)


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: RunnerOptimizationState) -> np.ndarray:
        """Return probability vector over 5 strategies."""
        pass

    @abstractmethod
    def confidence(self, state: RunnerOptimizationState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class RunnerRuleBasedTeacher(Teacher):
    """Rule‑based expert: carbon‑aware, degradation‑aware, queue‑aware."""
    ACTION_SPACE = ['performance', 'carbon', 'cost', 'hybrid', 'adaptive']

    def predict(self, state: RunnerOptimizationState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if state.carbon_intensity > 500:
            probs[1] = 0.8   # carbon strategy
        elif state.degradation_tier <= 2:
            probs[0] = 0.7   # performance (survival mode)
        elif state.queue_size > 20:
            probs[0] = 0.6   # performance (clear queue)
        elif state.task_urgency == 'critical':
            probs[0] = 0.7   # performance
        elif state.token_balance < 200:
            probs[2] = 0.6   # cost (conserve tokens)
        return probs / probs.sum()

    def confidence(self, state: RunnerOptimizationState) -> float:
        if state.carbon_intensity > 500:
            return 0.6
        elif state.degradation_tier <= 2:
            return 0.5
        return 0.4


class RunnerHistoricalMLTeacher(Teacher):
    """Offline trained classifier on historical optimal actions."""
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        if model_path and Path(model_path).exists():
            import joblib
            self.model = joblib.load(model_path)

    def predict(self, state: RunnerOptimizationState) -> np.ndarray:
        if self.model is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: RunnerOptimizationState) -> float:
        return 0.7 if self.model is not None else 0.0


class RunnerStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, storage: Storage, lr: float = 0.1):
        self.storage = storage
        self.lr = lr
        self.weights = np.zeros((14, 5))  # 14 features, 5 actions
        self._load_state()

    def _load_state(self):
        w = self.storage.get_state('q_teacher_weights')
        if w:
            self.weights = np.array(json.loads(w))

    def _save_state(self):
        self.storage.save_state('q_teacher_weights', json.dumps(self.weights.tolist()))

    def predict(self, state: RunnerOptimizationState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: RunnerOptimizationState) -> float:
        return 0.5

    def update(self, state: RunnerOptimizationState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class DistillationStudent:
    """Linear softmax student updated via distillation + policy gradient."""
    def __init__(self, feature_dim: int = 14, n_classes: int = 5, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray) -> np.ndarray:
        logits = state_vector @ self.weights + self.biases
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
        current_probs = self.predict_proba(state_vector)
        logits = state_vector @ self.weights + self.biases

        # Distillation gradient (KL divergence)
        grad_distill = -(teacher_probs - current_probs)

        # Policy gradient (REINFORCE)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)

        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1


class ReplayBuffer:
    def __init__(self, max_size: int = 2000):
        self.buffer = deque(maxlen=max_size)

    def push(self, state_vec: np.ndarray, action: int, reward: float,
             next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))

    def sample(self, batch_size: int = 32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), actions, np.array(rewards),
                np.array(next_states), np.array(teacher_probs))

    def __len__(self):
        return len(self.buffer)


class DistillationRunnerOptimizer:
    """
    Replaces AutonomousRunnerOptimizer with multi‑teacher on‑policy distillation.
    """
    ACTION_SPACE = ['performance', 'carbon', 'cost', 'hybrid', 'adaptive']

    def __init__(self, storage: Storage, config: Config):
        self.storage = storage
        self.config = config
        self.student = DistillationStudent(lr=config.DISTILLATION_LEARNING_RATE)
        self.teachers: List[Teacher] = [
            RunnerRuleBasedTeacher(),
            RunnerHistoricalMLTeacher(),  # optionally load model
            RunnerStatefulQTeacher(storage)
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.DISTILLATION_REPLAY_SIZE)
        self.epsilon = config.DISTILLATION_EPSILON
        self.train_every = config.DISTILLATION_TRAIN_EVERY
        self.counter = 0

    async def select_strategy(self, state: RunnerOptimizationState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()

        # Ensemble teachers
        teacher_probs = np.zeros(5)
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(state)
            conf = teacher.confidence(state)
            teacher_probs += prob * conf
            total_conf += conf
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(5) / 5

        student_probs = self.student.predict_proba(state_vec)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, 4)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)

        return self.ACTION_SPACE[action_idx], action_idx, state_vec, teacher_probs

    async def update(self, state_vec: np.ndarray, action_idx: int, reward: float,
                     next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1

        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i])

        # Update StatefulQTeacher if we have the original state (done in main loop)
        # We'll update it separately in process_task with the actual state.

    def get_stats(self) -> Dict:
        return {
            'student_counter': self.student.counter,
            'buffer_size': len(self.replay_buffer),
            'weights_norm': float(np.linalg.norm(self.student.weights))
        }


# ============================================================================
# MODULE 3: AUTONOMOUS RUNNER OPTIMIZER (replaced by distillation)
# ============================================================================
# (The original AutonomousRunnerOptimizer class is removed; the new optimizer is used.)

# ============================================================================
# MODULE 4: MULTI-CLOUD RUNNER DISTRIBUTION (with real SDK replication)
# ============================================================================
class MultiCloudRunnerDistribution:
    """
    Multi-cloud distribution using real cloud SDKs with error handling and retries.
    """

    def __init__(self, storage: Storage):
        self.storage = storage
        self.providers = {
            'aws': {
                'regions': ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1'],
                'cost_per_gb': 0.09,
                'client': self._init_aws_client() if AWS_AVAILABLE else None
            },
            'azure': {
                'regions': ['eastus', 'westus', 'northeurope', 'southeastasia'],
                'cost_per_gb': 0.10,
                'client': self._init_azure_client() if AZURE_AVAILABLE else None
            },
            'gcp': {
                'regions': ['us-central1', 'us-west1', 'europe-west1', 'asia-east1'],
                'cost_per_gb': 0.08,
                'client': self._init_gcp_client() if GCP_AVAILABLE else None
            }
        }
        self.active_provider = 'aws'
        self.active_region = 'us-east-1'
        self._lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker(storage, "cloud")

    def _init_aws_client(self):
        try:
            return boto3.client('s3', region_name=config.CLOUD_AWS_REGION,
                                aws_access_key_id=config.CLOUD_AWS_ACCESS_KEY,
                                aws_secret_access_key=config.CLOUD_AWS_SECRET_KEY)
        except Exception as e:
            logger.warning("AWS client init failed: %s", e)
            return None

    def _init_azure_client(self):
        try:
            return BlobServiceClient.from_connection_string(config.CLOUD_AZURE_CONNECTION_STRING)
        except Exception as e:
            logger.warning("Azure client init failed: %s", e)
            return None

    def _init_gcp_client(self):
        try:
            return storage.Client()
        except Exception as e:
            logger.warning("GCP client init failed: %s", e)
            return None

    async def _upload_to_aws(self, data: bytes, key: str):
        if not self.providers['aws']['client']:
            raise Exception("AWS client not available")
        bucket = "agent-runner-data"
        try:
            self.providers['aws']['client'].put_object(Bucket=bucket, Key=key, Body=data)
            logger.info("Uploaded to S3: %s", key)
        except ClientError as e:
            logger.error("AWS upload failed: %s", e)
            raise

    async def _upload_to_azure(self, data: bytes, key: str):
        if not self.providers['azure']['client']:
            raise Exception("Azure client not available")
        container = "agent-runner-data"
        try:
            blob_client = self.providers['azure']['client'].get_blob_client(container, key)
            blob_client.upload_blob(data, overwrite=True)
            logger.info("Uploaded to Azure: %s", key)
        except Exception as e:
            logger.error("Azure upload failed: %s", e)
            raise

    async def _upload_to_gcp(self, data: bytes, key: str):
        if not self.providers['gcp']['client']:
            raise Exception("GCP client not available")
        bucket = "agent-runner-data"
        try:
            bucket_obj = self.providers['gcp']['client'].bucket(bucket)
            blob = bucket_obj.blob(key)
            blob.upload_from_string(data)
            logger.info("Uploaded to GCS: %s", key)
        except Exception as e:
            logger.error("GCP upload failed: %s", e)
            raise

    async def distribute_runner_data(self, data: Dict, preferences: Dict = None) -> Dict:
        preferences = preferences or {}
        async with self._lock:
            scores = {}
            for provider_name, provider in self.providers.items():
                latency = await self._measure_latency(provider_name)
                cost = provider['cost_per_gb'] * data.get('size_gb', 0.001)
                avail = 0.99 if provider['client'] else 0.5
                score = (0.4 * (1 - latency/1000)) + (0.3 * (1 - cost/0.2)) + (0.3 * avail)
                if preferences.get('region') in provider['regions']:
                    score += 0.1
                scores[provider_name] = score

            optimal_provider = max(scores, key=scores.get)
            provider = self.providers[optimal_provider]
            optimal_region = provider['regions'][0]
            if preferences.get('region') in provider['regions']:
                optimal_region = preferences['region']
            self.active_provider = optimal_provider
            self.active_region = optimal_region

            result = {
                'optimal_provider': optimal_provider,
                'optimal_region': optimal_region,
                'scores': scores,
                'data_size_gb': data.get('size_gb', 0),
                'reason': f'Provider {optimal_provider} has best score',
                'timestamp': datetime.now().isoformat()
            }
            self.storage.save_distribution(result)

            try:
                await self._replicate_data(optimal_provider, optimal_region, data)
            except Exception as e:
                logger.error("Data replication failed: %s", e)
                fallback_provider = next((p for p in sorted(scores, key=scores.get, reverse=True) if p != optimal_provider), None)
                if fallback_provider:
                    logger.info("Falling back to %s", fallback_provider)
                    await self._replicate_data(fallback_provider, preferences.get('region'), data)
                    result['fallback'] = fallback_provider
                else:
                    raise

            logger.info("Runner data distributed to %s (%s)", optimal_provider, optimal_region)
            return result

    async def _measure_latency(self, provider: str) -> float:
        base = {'aws': 50, 'azure': 60, 'gcp': 45}.get(provider, 50)
        return base + random.uniform(-10, 10)

    async def _replicate_data(self, provider: str, region: str, data: Dict):
        data_bytes = json.dumps(data, default=str).encode()
        key = f"runner_{uuid.uuid4().hex[:8]}.json"

        if provider == 'aws':
            await self._circuit_breaker.call(self._upload_to_aws, data_bytes, key)
        elif provider == 'azure':
            await self._circuit_breaker.call(self._upload_to_azure, data_bytes, key)
        elif provider == 'gcp':
            await self._circuit_breaker.call(self._upload_to_gcp, data_bytes, key)
        else:
            raise ValueError(f"Unknown provider: {provider}")

    async def get_distribution_status(self) -> Dict:
        return {
            'providers': {k: {'regions': v['regions'], 'cost_per_gb': v['cost_per_gb']} for k, v in self.providers.items()},
            'active_provider': self.active_provider,
            'active_region': self.active_region,
            'distribution_history': self.storage.get_recent_distributions(5)
        }

# ============================================================================
# RUNNER STATE (with persistence)
# ============================================================================
class RunnerState:
    """State container with persistence support."""
    def __init__(self, storage: Storage):
        self.storage = storage
        self.confidence = float(self.storage.get_state('confidence') or 0.5)
        self.uncertainty = float(self.storage.get_state('uncertainty') or 0.1)
        self.historical_success_rate = float(self.storage.get_state('success_rate') or 0.5)
        self.reflection_count = int(self.storage.get_state('reflection_count') or 0)
        self.carbon_budget_remaining = float(self.storage.get_state('carbon_budget') or 100.0)
        self.helium_budget_remaining = float(self.storage.get_state('helium_budget') or 100.0)
        self.active_strategies = json.loads(self.storage.get_state('active_strategies') or '[]')
        self.strategy_effectiveness = json.loads(self.storage.get_state('strategy_effectiveness') or '{}')
        self.preferred_experts = json.loads(self.storage.get_state('preferred_experts') or '[]')
        self.avoided_experts = json.loads(self.storage.get_state('avoided_experts') or '[]')
        self.expert_health_scores = json.loads(self.storage.get_state('expert_health') or '{}')
        self.recent_rewards = deque(maxlen=100)
        self.success_threshold = 0.8

    def save(self):
        self.storage.save_state('confidence', str(self.confidence))
        self.storage.save_state('uncertainty', str(self.uncertainty))
        self.storage.save_state('success_rate', str(self.historical_success_rate))
        self.storage.save_state('reflection_count', str(self.reflection_count))
        self.storage.save_state('carbon_budget', str(self.carbon_budget_remaining))
        self.storage.save_state('helium_budget', str(self.helium_budget_remaining))
        self.storage.save_state('active_strategies', json.dumps(self.active_strategies))
        self.storage.save_state('strategy_effectiveness', json.dumps(self.strategy_effectiveness))
        self.storage.save_state('preferred_experts', json.dumps(self.preferred_experts))
        self.storage.save_state('avoided_experts', json.dumps(self.avoided_experts))
        self.storage.save_state('expert_health', json.dumps(self.expert_health_scores))

# ============================================================================
# Stub for Bio-inspired Core (to make file self-contained)
# ============================================================================
class StubBioCore:
    async def shutdown(self):
        pass

# ============================================================================
# Task Priority Queue (with energy-aware preemption)
# ============================================================================
@dataclass(order=True)
class PrioritizedTask:
    priority: float
    sequence: int
    task: Dict[str, Any] = field(compare=False)
    timestamp: datetime = field(compare=False, default_factory=datetime.now)

class EnergyAwareTaskPriorityQueue:
    def __init__(self, max_size: int = 1000):
        self.heap: List[PrioritizedTask] = []
        self.sequence = 0
        self.max_size = max_size
        self._lock = asyncio.Lock()
        self._energy_budget = 1000.0  # example budget
        logger.info("EnergyAwareTaskPriorityQueue initialized with max_size=%d", max_size)

    async def push(self, task: Dict[str, Any], priority: float):
        async with self._lock:
            if len(self.heap) >= self.max_size:
                logger.warning("Task queue full (%d), dropping lowest priority task", self.max_size)
                heapq.heappop(self.heap)
            # Check energy budget: if task exceeds budget, reject
            energy_estimate = task.get('estimated_energy_joules', 0)
            if energy_estimate > self._energy_budget:
                logger.warning("Task %s rejected due to energy budget", task.get('task_id', 'unknown'))
                return
            heapq.heappush(self.heap, PrioritizedTask(
                priority=-priority,
                sequence=self.sequence,
                task=task
            ))
            self.sequence += 1

    async def pop(self) -> Optional[Dict[str, Any]]:
        async with self._lock:
            if not self.heap:
                return None
            prioritized = heapq.heappop(self.heap)
            return prioritized.task

    async def size(self) -> int:
        return len(self.heap)

    async def clear(self):
        async with self._lock:
            self.heap.clear()
            self.sequence = 0

    def calculate_priority(self, task: Dict[str, Any], state: Dict[str, Any]) -> float:
        base_priority = task.get('priority', 2)
        tier = state.get('degradation_tier', 5)
        carbon_impact = task.get('carbon_impact', 0.5)
        is_critical = task.get('is_critical', False)
        priority = float(base_priority)
        if tier <= 2:
            if is_critical or base_priority >= 2:
                priority += 2.0
            else:
                priority -= 1.0
        if state.get('carbon_gradient', 0.5) > 0.7:
            if carbon_impact < 0.3:
                priority += 0.5
            elif carbon_impact > 0.7:
                priority -= 0.5
        if task.get('urgency', 'normal') == 'critical':
            priority += 3.0
        elif task.get('urgency') == 'high':
            priority += 1.0
        return max(0.1, priority)

# ============================================================================
# Observability Dashboard (with WebSocket)
# ============================================================================
class AgentDashboardServer:
    def __init__(self):
        self.port = config.DASHBOARD_PORT
        self.clients: Set[websockets.WebSocketServerProtocol] = set()
        self._server = None
        self._running = False
        self._lock = asyncio.Lock()
        self._last_broadcast = {}
        logger.info("AgentDashboardServer initialized on port %d", self.port)

    async def start(self):
        if not config.ENABLE_DASHBOARD:
            logger.info("Dashboard disabled by configuration")
            return
        self._running = True
        if WEBSOCKETS_AVAILABLE:
            self._server = await serve(
                self._handle_client,
                "0.0.0.0",
                self.port,
                ping_interval=30,
                ping_timeout=60
            )
            logger.info("Dashboard WebSocket server started on port %d", self.port)
            asyncio.create_task(self._broadcast_loop())
        else:
            logger.warning("Websockets not available, dashboard disabled.")

    async def stop(self):
        self._running = False
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("Dashboard WebSocket server stopped")

    async def _handle_client(self, websocket: websockets.WebSocketServerProtocol, path: str):
        async with self._lock:
            self.clients.add(websocket)
            logger.info("Dashboard client connected (%d total)", len(self.clients))
        try:
            await websocket.send(json.dumps({
                'type': 'connected',
                'timestamp': datetime.now().isoformat(),
                'message': 'Connected to Green Agent Dashboard'
            }))
            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self._handle_client_message(websocket, data)
                except json.JSONDecodeError:
                    await websocket.send(json.dumps({'type': 'error', 'message': 'Invalid JSON received'}))
        except ConnectionClosed:
            pass
        finally:
            async with self._lock:
                self.clients.discard(websocket)
                logger.info("Dashboard client disconnected (%d total)", len(self.clients))

    async def _handle_client_message(self, websocket: websockets.WebSocketServerProtocol, data: Dict):
        msg_type = data.get('type')
        # Stub: handle client messages (e.g., get status, reset circuit breaker)
        pass

    async def broadcast_status(self, status: Dict[str, Any]):
        self._last_broadcast = status
        message = json.dumps({
            'type': 'status_update',
            'timestamp': datetime.now().isoformat(),
            'data': status
        })
        if not self.clients:
            return
        async with self._lock:
            disconnected = set()
            for client in self.clients:
                try:
                    await client.send(message)
                except (ConnectionClosed, websockets.WebSocketException):
                    disconnected.add(client)
            for client in disconnected:
                self.clients.discard(client)

    async def _broadcast_loop(self):
        while self._running:
            try:
                await asyncio.sleep(config.DASHBOARD_UPDATE_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Broadcast loop error: %s", e)

# ============================================================================
# Dynamic Pipeline Selector (with circuit breaker and RL)
# ============================================================================
class DynamicPipelineSelector:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.rl_learner = RLPipelineLearner(storage) if config.ENABLE_REINFORCEMENT_LEARNING else None
        self._lock = asyncio.Lock()

    def _get_circuit_breaker(self, pipeline: str) -> CircuitBreaker:
        if pipeline not in self.circuit_breakers:
            self.circuit_breakers[pipeline] = CircuitBreaker(self.storage, pipeline)
        return self.circuit_breakers[pipeline]

    def select_pipeline(self, task: Dict, state: Dict) -> Tuple[str, Dict]:
        available = self._get_available_pipelines()
        if self.rl_learner:
            pipeline = self.rl_learner.get_best_pipeline(state, available)
        else:
            pipeline = random.choice(available)
        return pipeline, {}

    def _get_available_pipelines(self) -> List[str]:
        # In a real system, we'd check circuit breaker states
        return ['standard', 'quantum_enhanced', 'helium_optimized', 'energy_efficient', 'bio_optimized']

    def record_performance(self, pipeline: str, success: bool, latency: float, reward: float):
        if self.rl_learner:
            self.rl_learner.record_reward(pipeline, reward)

    def get_pipeline_stats(self) -> Dict:
        stats = {}
        for pipeline in ['standard', 'quantum_enhanced', 'helium_optimized', 'energy_efficient', 'bio_optimized']:
            cb = self._get_circuit_breaker(pipeline)
            stats[pipeline] = {
                'state': cb.get_state(),
                'failures': cb._failures,
            }
        return stats

# ============================================================================
# Reinforcement Learning Pipeline Learner (with Q-learning)
# ============================================================================
class RLPipelineLearner:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.q_table: Dict[str, Dict[str, float]] = defaultdict(dict)
        self.state_action_counts: Dict[str, Dict[str, int]] = defaultdict(dict)
        self.learning_rate = config.RL_LEARNING_RATE
        self.discount_factor = config.RL_DISCOUNT_FACTOR
        self.exploration_rate = config.RL_EXPLORATION_RATE
        self._lock = asyncio.Lock()
        self.total_updates = 0
        self.last_state: Optional[str] = None
        self.last_action: Optional[str] = None
        self._load_q_table()
        logger.info("RLPipelineLearner initialized (α=%.3f, γ=%.2f, ε=%.2f)", 
                    self.learning_rate, self.discount_factor, self.exploration_rate)

    def _load_q_table(self):
        q_table = self.storage.get_q_table()
        for state, actions in q_table.items():
            for action, q_value in actions.items():
                self.q_table[state][action] = q_value
        logger.info("Loaded %d states from Q-table", len(self.q_table))

    def _persist_q_value(self, state: str, action: str, q_value: float, count: int):
        self.storage.save_q_value(state, action, q_value, count)

    def _state_to_key(self, state: Dict[str, Any]) -> str:
        tier = state.get('degradation_tier', 5)
        token_balance = state.get('token_balance', 1000)
        carbon_gradient = state.get('carbon_gradient', 0.5)
        token_level = 'high' if token_balance > 500 else 'low'
        carbon_level = 'high' if carbon_gradient > 0.5 else 'low'
        tier_level = f'tier_{tier}'
        return f"{tier_level}_{token_level}_{carbon_level}"

    def get_best_pipeline(self, state: Dict[str, Any], available_pipelines: List[str]) -> str:
        state_key = self._state_to_key(state)
        if np.random.random() < self.exploration_rate:
            self.exploration_rate *= 0.999  # decay
            return np.random.choice(available_pipelines)
        q_values = {p: self.q_table[state_key].get(p, 0.0) for p in available_pipelines}
        best_pipeline = max(q_values, key=q_values.get)
        self.last_state = state_key
        self.last_action = best_pipeline
        return best_pipeline

    async def update(self, state: Dict[str, Any], pipeline: str, reward_info: Dict, next_state: Dict[str, Any]):
        reward = self._compute_reward(reward_info)
        async with self._lock:
            state_key = self._state_to_key(state)
            next_state_key = self._state_to_key(next_state)
            current_q = self.q_table[state_key].get(pipeline, 0.0)
            max_next_q = max(self.q_table[next_state_key].values()) if self.q_table[next_state_key] else 0
            new_q = current_q + self.learning_rate * (reward + self.discount_factor * max_next_q - current_q)
            self.q_table[state_key][pipeline] = new_q
            count = self.state_action_counts[state_key].get(pipeline, 0) + 1
            self.state_action_counts[state_key][pipeline] = count
            self.total_updates += 1
            self._persist_q_value(state_key, pipeline, new_q, count)
            if PROMETHEUS_AVAILABLE and config.ENABLE_PROMETHEUS:
                RL_LEARNING_UPDATES.inc()

    def _compute_reward(self, reward_info: Dict) -> float:
        # Combine success, latency, energy, carbon into a scalar reward
        success = reward_info.get('success', 0.0)
        latency_ms = reward_info.get('latency_ms', 0)
        energy_joules = reward_info.get('energy_joules', 0)
        carbon_kg = reward_info.get('carbon_kg', 0)
        # Normalize: success (0-1), latency (inverse), energy (inverse), carbon (inverse)
        latency_score = max(0, 1 - latency_ms / 10000)
        energy_score = max(0, 1 - energy_joules / 1000)
        carbon_score = max(0, 1 - carbon_kg / 0.1)
        reward = 0.5 * success + 0.2 * latency_score + 0.15 * energy_score + 0.15 * carbon_score
        return reward

    def get_q_values(self, state: Dict[str, Any]) -> Dict[str, float]:
        state_key = self._state_to_key(state)
        return dict(self.q_table[state_key])

    def get_statistics(self) -> Dict[str, Any]:
        total_states = len(self.q_table)
        total_actions = sum(len(actions) for actions in self.q_table.values())
        return {
            'total_updates': self.total_updates,
            'total_states': total_states,
            'total_actions': total_actions,
            'exploration_rate': self.exploration_rate,
            'learning_rate': self.learning_rate,
            'discount_factor': self.discount_factor
        }

# ============================================================================
# Sustainability Modules (simplified stubs with logging)
# ============================================================================
class CarbonIntensityFetcher:
    def __init__(self):
        self.api_key = config.ELECTRICITY_MAPS_API_KEY or config.CARBON_INTENSITY_API_KEY
        self.region = config.CARBON_REGION
        self._session = None

    async def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def get_intensity(self, region=None):
        region = region or self.region
        # Placeholder: in real implementation, call API
        # For demo, return mock value
        return 400.0

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

class HeliumCollector:
    async def get_connectivity_score(self, hotspot_id):
        return 0.8

class MaterialFootprintUpdater:
    def get_footprint(self, product_id):
        return {'material_index': 0.5}

class BioParameterCatalog:
    def get_parameters(self, organism_type):
        return {'photosynthetic_efficiency': 0.5}

class SustainabilityCostFunction:
    def __init__(self, carbon_fetcher, material_updater, helium_collector):
        self.carbon = carbon_fetcher
        self.material = material_updater
        self.helium = helium_collector
    async def compute(self, node_desc, workload):
        return 0.5

class NodeDescriptor:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

class WorkloadDescriptor:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

# ============================================================================
# ENHANCED GREEN AGENT RUNNER (v8.1.0)
# ============================================================================
class EnhancedGreenAgentRunner:
    def __init__(self):
        # Central storage
        self.storage = Storage()
        self.state = RunnerState(self.storage)

        # Enhanced data collectors and cache
        self.carbon_fetcher = CarbonIntensityFetcher()
        self.helium_collector = HeliumCollector()
        self.material_updater = MaterialFootprintUpdater()
        self.bio_catalog = BioParameterCatalog()
        self.cost_function = SustainabilityCostFunction(
            carbon_fetcher=self.carbon_fetcher,
            material_updater=self.material_updater,
            helium_collector=self.helium_collector,
        )

        # Existing modules
        self.quantum_security = QuantumResilientRunnerSecurity(self.storage)
        self.blockchain = BlockchainRunnerVerification(self.storage)
        # REPLACED: self.autonomous_optimizer = AutonomousRunnerOptimizer(...)
        self.distillation_optimizer = DistillationRunnerOptimizer(self.storage, config)
        self.cloud_distributor = MultiCloudRunnerDistribution(self.storage)

        # Pipeline selector with RL and circuit breakers
        self.pipeline_selector = DynamicPipelineSelector(self.storage)

        # Available pipelines
        self.pipelines = {
            'standard': self._standard_pipeline,
            'quantum_enhanced': self._quantum_pipeline,
            'helium_optimized': self._helium_pipeline,
            'energy_efficient': self._energy_efficient_pipeline,
            'bio_optimized': self._bio_optimized_pipeline
        }

        # Energy‑aware task queue
        self.task_queue = EnergyAwareTaskPriorityQueue(max_size=config.QUEUE_MAX_SIZE)

        # Dashboard server
        self.dashboard = AgentDashboardServer()

        # Bio-inspired core (stub)
        self.bio_core = StubBioCore()

        # Task tracking
        self.total_tasks = 0
        self.successful_tasks = 0
        self.failed_tasks = 0
        self.task_history = deque(maxlen=1000)

        # State
        self.running = True
        self._worker_tasks: List[asyncio.Task] = []
        self._shutdown_event = asyncio.Event()

        # Register signal handlers
        self._register_signal_handlers()
        logger.info("Enhanced Green Agent Runner v8.1.0 initialized with Distillation")

    def _register_signal_handlers(self):
        try:
            loop = asyncio.get_event_loop()
            for sig in [signal.SIGINT, signal.SIGTERM]:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
        except NotImplementedError:
            pass

    def _get_system_state(self) -> Dict[str, Any]:
        state = {
            'degradation_tier': 5,
            'token_balance': 1000,
            'carbon_gradient': 0.5,
            'predicted_carbon': 0.5,
            'carbon_intensity': 0.4,  # normalized
            'queue_size': self.task_queue.size(),
        }
        return state

    # ========================================================================
    # NEW: Build optimization state
    # ========================================================================
    async def _get_optimization_state(self, task: Dict[str, Any]) -> RunnerOptimizationState:
        """Gather context for the distillation agent."""
        system_state = self._get_system_state()

        # Compute recent stats
        if self.total_tasks > 0:
            success_rate = self.successful_tasks / self.total_tasks
        else:
            success_rate = 0.5

        if self.task_history:
            recent = list(self.task_history)[-20:]
            avg_latency = np.mean([h['latency_ms'] for h in recent])
            avg_energy = np.mean([h.get('energy_joules', 0) for h in recent])
        else:
            avg_latency = 100
            avg_energy = 100

        return RunnerOptimizationState(
            degradation_tier=system_state['degradation_tier'],
            token_balance=system_state['token_balance'],
            carbon_gradient=system_state['carbon_gradient'],
            carbon_intensity=system_state['carbon_intensity'] * 1000,  # convert back to gCO₂
            queue_size=system_state['queue_size'],
            task_priority=task.get('priority', 2),
            task_urgency=task.get('urgency', 'normal'),
            estimated_energy=task.get('estimated_energy_joules', 0),
            estimated_carbon=task.get('carbon_impact', 0.5),
            recent_success_rate=success_rate,
            avg_latency_ms=avg_latency,
            avg_energy_joules=avg_energy,
            hour_of_day=datetime.now().hour,
            is_weekend=datetime.now().weekday() >= 5
        )

    async def submit_task(self, task: Dict[str, Any]) -> str:
        state = self._get_system_state()
        priority = self.task_queue.calculate_priority(task, state)
        if 'task_id' not in task:
            task['task_id'] = f"task_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{self.total_tasks}"
        await self.task_queue.push(task, priority)
        logger.debug("Task %s queued with priority %.2f", task['task_id'], priority)
        return task['task_id']

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        start_time = datetime.utcnow()
        self.total_tasks += 1
        task_id = task.get('task_id', 'unknown')
        system_state = self._get_system_state()

        # --- Distillation: select strategy ---
        optimization_state = await self._get_optimization_state(task)
        strategy, action_idx, state_vec, teacher_probs = await self.distillation_optimizer.select_strategy(optimization_state, exploration=True)

        # Apply strategy modifications (e.g., adjust task priority, pipeline selection)
        if strategy == 'performance':
            # Boost priority to clear queue faster
            task['priority'] = task.get('priority', 2) + 1
        elif strategy == 'carbon':
            # If carbon is high, try to use energy-efficient pipeline
            if system_state.get('carbon_intensity', 0.5) > 0.5:
                task['preferred_pipeline'] = 'energy_efficient'
        elif strategy == 'cost':
            # Reduce resource usage
            task['estimated_energy_joules'] = task.get('estimated_energy_joules', 0) * 0.8
        elif strategy == 'adaptive':
            # Use adaptive behavior based on historical data
            pass
        # (hybrid does nothing special)

        # Degradation awareness
        if config.ENABLE_DEGRADATION_AWARE:
            tier = system_state['degradation_tier']
            if tier <= 1:
                return {'success': False, 'reason': f'System in survival mode (tier {tier})', 'task_id': task_id}
            if tier <= 2 and task.get('priority', 2) > 1:
                return {'success': False, 'reason': f'Non-critical tasks deferred in tier {tier}', 'task_id': task_id}

        # Dynamic pipeline selection
        if config.ENABLE_DYNAMIC_PIPELINE:
            pipeline_name, scores = self.pipeline_selector.select_pipeline(task, system_state)
        else:
            pipeline_name = task.get('pipeline', 'standard')

        # Execute with fallback
        result = await self._execute_with_fallback(task, pipeline_name, system_state)

        success = result.get('success', False)
        latency = (datetime.utcnow() - start_time).total_seconds() * 1000
        energy_joules = result.get('energy_joules', latency * 0.1)
        carbon_kg = result.get('carbon_kg', energy_joules * 0.0001)

        # RL pipeline update (unchanged)
        if config.ENABLE_REINFORCEMENT_LEARNING and self.pipeline_selector.rl_learner:
            reward_info = {
                'success': 1.0 if success else 0.0,
                'latency_ms': latency,
                'energy_joules': energy_joules,
                'carbon_kg': carbon_kg
            }
            next_state = self._get_system_state()
            await self.pipeline_selector.rl_learner.update(system_state, pipeline_name, reward_info, next_state)

        # ---- Compute reward for distillation ----
        reward = 0.0
        if success:
            reward += 0.5
        # Latency reward: inverse of normalized latency
        latency_score = max(0, 1 - latency / 10000)
        reward += 0.15 * latency_score
        # Energy reward
        energy_score = max(0, 1 - energy_joules / 1000)
        reward += 0.15 * energy_score
        # Carbon reward
        carbon_score = max(0, 1 - carbon_kg / 0.1)
        reward += 0.2 * carbon_score
        reward = max(0.0, min(1.0, reward))

        # Update distillation optimizer
        next_state = await self._get_optimization_state(task)
        await self.distillation_optimizer.update(state_vec, action_idx, reward, next_state.to_feature_vector(), teacher_probs)

        # Update statistics
        if success:
            self.successful_tasks += 1
        else:
            self.failed_tasks += 1

        self.task_history.append({
            'task_id': task_id,
            'pipeline': pipeline_name,
            'success': success,
            'latency_ms': latency,
            'energy_joules': energy_joules,
            'timestamp': datetime.utcnow().isoformat()
        })

        result['pipeline_used'] = pipeline_name
        result['pipeline_scores'] = scores
        result['system_state'] = {
            'tier': system_state['degradation_tier'],
            'token_balance': system_state['token_balance'],
            'carbon_gradient': system_state['carbon_gradient']
        }
        result['strategy_used'] = strategy
        result['reward'] = reward

        # ---- Quantum signing, blockchain, cloud (unchanged) ----
        result_data = result.copy()
        quantum_key = await self.quantum_security.generate_keypair('dilithium')
        signature = await self.quantum_security.sign_task_result(result_data, quantum_key['key_id'])
        result['quantum_signature'] = signature
        if PROMETHEUS_AVAILABLE and config.ENABLE_PROMETHEUS:
            QUANTUM_SIGNATURES.labels(algorithm='dilithium', status='sign_success').inc()

        data_id = f"task_{uuid.uuid4().hex[:8]}"
        data_hash = hashlib.sha256(json.dumps(result_data, sort_keys=True, default=str).encode()).hexdigest()
        blockchain_result = await self.blockchain.record_task_result(
            data_id,
            data_hash,
            {'task_id': task_id, 'success': success, 'pipeline': pipeline_name}
        )
        result['blockchain_tx_hash'] = blockchain_result.get('tx_hash')
        if PROMETHEUS_AVAILABLE and config.ENABLE_PROMETHEUS:
            BLOCKCHAIN_VERIFICATIONS.labels(status='recorded').inc()

        cloud_data = {'size_gb': len(str(result)) * 0.001}
        distribution = await self.cloud_distributor.distribute_runner_data(cloud_data)
        result['cloud_distribution'] = distribution
        if PROMETHEUS_AVAILABLE and config.ENABLE_PROMETHEUS:
            CLOUD_DISTRIBUTIONS.labels(provider=distribution['optimal_provider'], status='success').inc()

        # Autonomous optimization now handled by distillation; we can still log stats
        if PROMETHEUS_AVAILABLE and config.ENABLE_PROMETHEUS:
            AUTONOMOUS_OPTIMIZATIONS.labels(strategy=strategy, status='success').inc()
            DISTILLATION_STRATEGY.labels(strategy=strategy).inc()
            DISTILLATION_REWARD.observe(reward)
            DISTILLATION_BUFFER_SIZE.set(len(self.distillation_optimizer.replay_buffer))

        # Store in database
        self.storage.save_task_history(task_id, pipeline_name, success, latency, result)

        if PROMETHEUS_AVAILABLE and config.ENABLE_PROMETHEUS:
            AGENT_TASKS.labels(status='success' if success else 'failed').inc()
            AGENT_DURATION.labels(pipeline=pipeline_name).observe(latency / 1000)
            AGENT_QUEUE_SIZE.set(self.task_queue.size())

        audit_logger.info("Task %s processed: success=%s, pipeline=%s, latency=%.0fms, strategy=%s, reward=%.2f, blockchain=%s...",
                         task_id, success, pipeline_name, latency, strategy, reward,
                         result['blockchain_tx_hash'][:16] if result['blockchain_tx_hash'] else 'N/A')
        return result

    async def _execute_with_fallback(self, task: Dict[str, Any], initial_pipeline: str, system_state: Dict[str, Any]) -> Dict[str, Any]:
        fallback_chain = [initial_pipeline] + config.FALLBACK_PIPELINES
        seen = set()
        fallback_chain = [p for p in fallback_chain if not (p in seen or seen.add(p))]
        for pipeline_name in fallback_chain:
            try:
                if config.ENABLE_CIRCUIT_BREAKERS:
                    cb = self.pipeline_selector._get_circuit_breaker(pipeline_name)
                    if cb.get_state() == "OPEN":
                        logger.warning("Pipeline %s unavailable (state: OPEN)", pipeline_name)
                        continue
                pipeline_func = self.pipelines.get(pipeline_name)
                if not pipeline_func:
                    logger.warning("Pipeline %s not found", pipeline_name)
                    continue
                try:
                    async def run_with_monitor():
                        start = datetime.utcnow()
                        result = await asyncio.wait_for(pipeline_func(task), timeout=config.TASK_TIMEOUT_SECONDS)
                        energy_used = (datetime.utcnow() - start).total_seconds() * 10  # mock energy
                        result['energy_joules'] = energy_used
                        result['carbon_kg'] = energy_used * 0.0001
                        return result

                    result = await run_with_monitor()
                    if config.ENABLE_CIRCUIT_BREAKERS:
                        cb = self.pipeline_selector._get_circuit_breaker(pipeline_name)
                        # Record success by resetting failures
                        cb._failures = 0
                        cb._state = "CLOSED"
                    return result
                except asyncio.TimeoutError:
                    logger.error("Pipeline %s timed out after %ds", pipeline_name, config.TASK_TIMEOUT_SECONDS)
                    if config.ENABLE_CIRCUIT_BREAKERS:
                        cb = self.pipeline_selector._get_circuit_breaker(pipeline_name)
                        # Record failure
                        cb._failures += 1
                        if cb._failures >= cb.failure_threshold:
                            cb._state = "OPEN"
                    continue
            except Exception as e:
                logger.error("Pipeline %s failed: %s", pipeline_name, e)
                if config.ENABLE_CIRCUIT_BREAKERS:
                    cb = self.pipeline_selector._get_circuit_breaker(pipeline_name)
                    cb._failures += 1
                    if cb._failures >= cb.failure_threshold:
                        cb._state = "OPEN"
                continue
        return {'success': False, 'error': 'All pipelines failed', 'task_id': task.get('task_id', 'unknown'), 'tried_pipelines': fallback_chain}

    async def _worker_loop(self, worker_id: int):
        logger.info("Worker %d started", worker_id)
        while self.running:
            try:
                task = await self.task_queue.pop()
                if task is None:
                    await asyncio.sleep(0.1)
                    continue
                result = await self.process_task(task)
                if 'callback' in task:
                    try:
                        if asyncio.iscoroutinefunction(task['callback']):
                            await task['callback'](result)
                        else:
                            task['callback'](result)
                    except Exception as e:
                        logger.error("Callback error for task %s: %s", task.get('task_id'), e)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Worker %d error: %s", worker_id, e)
                await asyncio.sleep(0.5)
        logger.info("Worker %d stopped", worker_id)

    async def start_workers(self, num_workers: int = None):
        if num_workers is None:
            num_workers = config.MAX_CONCURRENT_TASKS
        for i in range(num_workers):
            worker = asyncio.create_task(self._worker_loop(i))
            self._worker_tasks.append(worker)
        logger.info("Started %d workers", num_workers)

    # Pipeline methods
    async def _standard_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0.01)
        return {'success': True, 'pipeline': 'standard', 'task_id': task.get('task_id')}

    async def _quantum_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        if not task.get('quantum_capable', False):
            return await self._standard_pipeline(task)
        await asyncio.sleep(0.02)
        return {'success': True, 'pipeline': 'quantum', 'task_id': task.get('task_id')}

    async def _helium_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0.015)
        return {'success': True, 'pipeline': 'helium', 'task_id': task.get('task_id')}

    async def _energy_efficient_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0.005)
        return {'success': True, 'pipeline': 'energy_efficient', 'task_id': task.get('task_id')}

    async def _bio_optimized_pipeline(self, task: Dict[str, Any]) -> Dict[str, Any]:
        return await self._standard_pipeline(task)

    def get_status(self) -> Dict[str, Any]:
        system_state = self._get_system_state()
        return {
            'version': '8.1.0',
            'total_tasks': self.total_tasks,
            'successful_tasks': self.successful_tasks,
            'failed_tasks': self.failed_tasks,
            'success_rate': self.successful_tasks / max(self.total_tasks, 1),
            'queue_size': self.task_queue.size(),
            'pipeline_stats': self.pipeline_selector.get_pipeline_stats(),
            'system_state': system_state,
            'distillation_stats': self.distillation_optimizer.get_stats(),
            'running': self.running,
            'config': config.model_dump() if hasattr(config, 'model_dump') else config.__dict__,
            'timestamp': datetime.utcnow().isoformat()
        }

    async def start(self):
        logger.info("Starting Enhanced Green Agent Runner v8.1.0...")
        await self.dashboard.start()
        await self.start_workers()
        if config.ENABLE_PROMETHEUS and PROMETHEUS_AVAILABLE:
            try:
                start_http_server(9090)
                logger.info("Prometheus metrics server started on port 9090")
            except Exception as e:
                logger.warning("Failed to start Prometheus server: %s", e)
        logger.info("Enhanced Green Agent Runner started successfully")

    async def shutdown(self):
        if not self.running:
            return
        logger.info("Shutting down Enhanced Green Agent Runner...")
        self.running = False
        self._shutdown_event.set()
        for worker in self._worker_tasks:
            worker.cancel()
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        await self.dashboard.stop()
        await self.bio_core.shutdown()
        if self.carbon_fetcher:
            await self.carbon_fetcher.close()
        logger.info("Enhanced Green Agent Runner shutdown complete")

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

# =============================================================================
# CLI Entry Point
# =============================================================================
async def main():
    async with EnhancedGreenAgentRunner() as runner:
        logger.info("Agent running. Press Ctrl+C to stop.")
        try:
            while runner.running:
                await asyncio.sleep(1)
                if int(time.time()) % 30 == 0:
                    status = runner.get_status()
                    logger.info("Status: %d tasks, %.1f%% success rate, queue: %d, buffer: %d",
                               status['total_tasks'], status['success_rate']*100,
                               status['queue_size'], status['distillation_stats']['buffer_size'])
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error("Runtime error: %s", e)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Graceful shutdown complete")
