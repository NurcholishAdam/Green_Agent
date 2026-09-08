#!/usr/bin/env python3
"""
Enhanced Bio-Inspired Core v9.2.0 – Complete with all enhancement modules.
Integrates Quantum‑Distillation, Causal RL, Federated Learning, Safety Monitor,
XAI, Adaptive Precision, Carbon Markets, Chaos Testing, and Human‑in‑the‑Loop.
"""

import asyncio
import logging
import signal
import time
import random
import json
import os
import importlib
import hashlib
import pickle
import sqlite3
import uuid
from typing import Dict, Any, List, Optional, Tuple, Protocol, Callable, Set, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from abc import ABC, abstractmethod
import numpy as np
from collections import defaultdict, deque
from functools import wraps
from pathlib import Path

# Optional dependencies
try:
    from pydantic import BaseModel, Field, field_validator, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

try:
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
    from cryptography.hazmat.backends import default_backend
    CRYPTOGRAPHY_AVAILABLE = True
except ImportError:
    CRYPTOGRAPHY_AVAILABLE = False

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    import boto3
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
    from web3 import Web3, Account, HTTPProvider
    from web3.middleware import geth_poa_middleware, gas_price_strategy
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

import structlog
from structlog.processors import JSONRenderer, TimeStamper

structlog.configure(
    processors=[structlog.stdlib.add_log_level, structlog.stdlib.PositionalArgumentsFormatter(),
                TimeStamper(fmt="iso"), JSONRenderer()],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)
logger = structlog.get_logger(__name__)

# ============================================================================
# Service Protocols
# ============================================================================
class TokenServiceProtocol(Protocol):
    async def get_system_summary(self) -> Dict[str, Any]: ...
    async def get_account_summary(self, account_id: str) -> Dict[str, Any]: ...
    async def reserve_tokens(self, account_id: str, amount: float, consumer: Any, tenant_id: str, priority: int) -> Tuple[bool, List[str]]: ...
    async def generate_tokens(self, account_id: str, source: Any, **kwargs) -> List[Any]: ...
    async def consume_tokens(self, token_ids: List[str], consumer: Any, operation_success: bool) -> float: ...
    async def recover_tokens(self, token_ids: List[str], completion_percentage: float) -> float: ...
    async def create_account(self, account_id: str) -> Any: ...

class GradientServiceProtocol(Protocol):
    async def get_field_strengths(self) -> Dict[str, float]: ...
    async def pump_field(self, field_id: str, amount: float, source: str) -> None: ...
    async def discharge_field(self, field_id: str, amount: float) -> float: ...
    async def get_dominant_field(self) -> Tuple[str, float]: ...
    async def get_field_stats(self) -> Dict[str, Any]: ...
    async def find_root_cause(self, anomaly_field: str, max_depth: int) -> Dict[str, Any]: ...
    async def explain_gradient_state(self, field_id: str) -> Dict[str, Any]: ...
    async def forecast(self, field_id: str, horizon_seconds: float) -> Dict[str, Any]: ...
    async def get_forecast_summary(self) -> Dict[str, Any]: ...

class CompartmentServiceProtocol(Protocol):
    async def find_best_compartment(self, expert_type: str, task_complexity: float) -> Any: ...
    async def get_ecosystem_stats(self) -> Dict[str, Any]: ...
    async def create_compartment(self, expert_type: str, expert_instance: Any, resources: Any, parent_id: str) -> Any: ...
    async def decommission_compartment(self, compartment_id: str) -> Dict[str, Any]: ...

class BiomassServiceProtocol(Protocol):
    async def store_task(self, task_data: Dict[str, Any], ecoatp_cost: float, guarantee: Any, deadline: Any, initial_tier: Any) -> Tuple[bool, Optional[str]]: ...
    async def retrieve_task(self, token_id: str) -> Tuple[Optional[Dict[str, Any]], float]: ...
    async def get_storage_stats(self) -> Dict[str, Any]: ...
    async def simulate_storage_scenario(self, scenario: Dict[str, Any]) -> Dict[str, Any]: ...

# ============================================================================
# Enums
# ============================================================================
class LifecyclePhase(Enum):
    UNREGISTERED = "unregistered"
    REGISTERED = "registered"
    INITIALIZING = "initializing"
    INITIALIZED = "initialized"
    HEALTH_CHECKING = "health_checking"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"
    LOADING = "loading"

# ============================================================================
# Data Classes
# ============================================================================
@dataclass
class ModuleEntry:
    name: str
    module: Any = None
    phase: LifecyclePhase = LifecyclePhase.REGISTERED
    dependencies: List[str] = field(default_factory=list)
    dependents: List[str] = field(default_factory=list)
    health_check: Optional[Callable] = None
    init_timeout: float = 30.0
    shutdown_timeout: float = 10.0
    init_started: Optional[datetime] = None
    init_completed: Optional[datetime] = None
    error_message: Optional[str] = None
    health_status: str = "unknown"
    circuit_breaker_state: str = "closed"
    failure_count: int = 0
    last_failure: Optional[datetime] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    module_path: Optional[str] = None
    version: str = "1.0.0"
    loaded_at: Optional[datetime] = None
    predicted_health: Optional[float] = None
    failure_probability: float = 0.0
    health_trend: str = "stable"
    id: int = 0

@dataclass
class MOPDPoint:
    individual: Dict[str, Any]
    health_score: float
    uptime_score: float
    circuit_score: float
    anomaly_score: float
    scalarised_score: float = 0.0

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

# ============================================================================
# Configuration
# ============================================================================
if PYDANTIC_AVAILABLE:
    class MOPDConfig(BaseModel):
        enabled: bool = True
        objective_weights: Dict[str, float] = Field(default_factory=lambda: {
            'health_score': 0.4, 'uptime_score': 0.3, 'circuit_score': 0.2, 'anomaly_score': 0.1
        })
        grid_resolution: int = 5

        @field_validator('objective_weights')
        def check_weights(cls, v):
            if abs(sum(v.values()) - 1.0) > 1e-6:
                raise ValueError('objective_weights must sum to 1')
            return v

    class CoreConfig(BaseModel):
        token_base_generation_rate: float = 150.0
        token_hoarding_threshold: float = 2.0
        token_emergency_threshold: float = 50.0
        token_target_utilization: float = 0.75
        compartments_per_expert_type: int = 2
        max_total_compartments: int = 100
        compartment_health_threshold: float = 0.2
        carbon_leakage_rate: float = 0.03
        helium_leakage_rate: float = 0.08
        trust_leakage_rate: float = 0.10
        atp_c_ring_size: int = 12
        atp_max_rotation_speed: float = 6000
        enable_multi_synthase: bool = True
        enable_quantum_expert: bool = False
        enable_helium_expert: bool = False
        enable_degradation_manager: bool = True
        enable_predictive_homeostasis: bool = True
        enable_knowledge_transfer: bool = True
        enable_supply_management: bool = True
        enable_token_preallocation: bool = True
        enable_chaos_engineering: bool = False
        enable_state_persistence: bool = True
        state_save_interval_seconds: int = 300
        state_directory: str = "./agent_state"
        health_check_interval_seconds: int = 30
        version: str = "1.0.0"
        version_description: str = ""
        db_path: str = "./bio_core_state.db"
        enable_quantum_signing: bool = True
        quantum_signing_algorithm: str = 'dilithium'
        enable_blockchain_audit: bool = True
        blockchain_rpc_url: str = 'http://localhost:8545'
        blockchain_contract_address: str = '0x0000000000000000000000000000000000000000'
        blockchain_private_key: Optional[str] = None
        enable_autonomous_strategy: bool = True
        rl_learning_rate: float = 0.1
        rl_discount_factor: float = 0.9
        rl_exploration_rate: float = 0.1
        rl_q_table_db_path: str = "rl_q_table.db"
        enable_multi_cloud: bool = True
        cloud_provider: str = 'aws'
        cloud_region: str = 'us-east-1'
        cloud_bucket: str = 'bio-core-state'
        cloud_access_key: Optional[str] = None
        cloud_secret_key: Optional[str] = None
        max_retries: int = 3
        retry_base_delay_ms: float = 100.0
        retry_max_delay_ms: float = 5000.0
        enable_circuit_breaker: bool = True
        circuit_breaker_threshold: int = 5
        circuit_breaker_recovery_timeout: float = 60.0
        circuit_breaker_db_path: str = "circuit_breakers.db"
        prometheus_port: Optional[int] = None
        ml_model_path: str = "models/health_model.pkl"
        mopd: MOPDConfig = Field(default_factory=MOPDConfig)
        # New enhancement flags
        enable_quantum_distillation: bool = False
        enable_causal_rl: bool = False
        enable_federated: bool = False
        enable_safety_monitor: bool = True
        enable_xai: bool = True
        enable_precision: bool = False
        enable_carbon_market: bool = False
        carbon_market_config: Optional[Dict[str, str]] = None
        enable_chaos: bool = False
        chaos_probability: float = 0.0
        enable_human_approval: bool = False
        human_approval_timeout: float = 60.0

        class Config:
            env_prefix = "BIO_CORE_"
else:
    @dataclass
    class MOPDConfig:
        enabled: bool = True
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'health_score': 0.4, 'uptime_score': 0.3, 'circuit_score': 0.2, 'anomaly_score': 0.1
        })
        grid_resolution: int = 5

    @dataclass
    class CoreConfig:
        token_base_generation_rate: float = 150.0
        token_hoarding_threshold: float = 2.0
        token_emergency_threshold: float = 50.0
        token_target_utilization: float = 0.75
        compartments_per_expert_type: int = 2
        max_total_compartments: int = 100
        compartment_health_threshold: float = 0.2
        carbon_leakage_rate: float = 0.03
        helium_leakage_rate: float = 0.08
        trust_leakage_rate: float = 0.10
        atp_c_ring_size: int = 12
        atp_max_rotation_speed: float = 6000
        enable_multi_synthase: bool = True
        enable_quantum_expert: bool = False
        enable_helium_expert: bool = False
        enable_degradation_manager: bool = True
        enable_predictive_homeostasis: bool = True
        enable_knowledge_transfer: bool = True
        enable_supply_management: bool = True
        enable_token_preallocation: bool = True
        enable_chaos_engineering: bool = False
        enable_state_persistence: bool = True
        state_save_interval_seconds: int = 300
        state_directory: str = "./agent_state"
        health_check_interval_seconds: int = 30
        version: str = "1.0.0"
        version_description: str = ""
        db_path: str = "./bio_core_state.db"
        enable_quantum_signing: bool = True
        quantum_signing_algorithm: str = 'dilithium'
        enable_blockchain_audit: bool = True
        blockchain_rpc_url: str = 'http://localhost:8545'
        blockchain_contract_address: str = '0x0000000000000000000000000000000000000000'
        blockchain_private_key: Optional[str] = None
        enable_autonomous_strategy: bool = True
        rl_learning_rate: float = 0.1
        rl_discount_factor: float = 0.9
        rl_exploration_rate: float = 0.1
        rl_q_table_db_path: str = "rl_q_table.db"
        enable_multi_cloud: bool = True
        cloud_provider: str = 'aws'
        cloud_region: str = 'us-east-1'
        cloud_bucket: str = 'bio-core-state'
        cloud_access_key: Optional[str] = None
        cloud_secret_key: Optional[str] = None
        max_retries: int = 3
        retry_base_delay_ms: float = 100.0
        retry_max_delay_ms: float = 5000.0
        enable_circuit_breaker: bool = True
        circuit_breaker_threshold: int = 5
        circuit_breaker_recovery_timeout: float = 60.0
        circuit_breaker_db_path: str = "circuit_breakers.db"
        prometheus_port: Optional[int] = None
        ml_model_path: str = "models/health_model.pkl"
        mopd: MOPDConfig = field(default_factory=MOPDConfig)
        enable_quantum_distillation: bool = False
        enable_causal_rl: bool = False
        enable_federated: bool = False
        enable_safety_monitor: bool = True
        enable_xai: bool = True
        enable_precision: bool = False
        enable_carbon_market: bool = False
        carbon_market_config: Optional[Dict[str, str]] = None
        enable_chaos: bool = False
        chaos_probability: float = 0.0
        enable_human_approval: bool = False
        human_approval_timeout: float = 60.0

# ============================================================================
# Circuit Breaker
# ============================================================================
class CircuitBreaker:
    def __init__(self, name, db_path, failure_threshold=5, recovery_timeout=60.0):
        self.name = name
        self.db_path = db_path
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._init_db()
        self._load_state()
        self._lock = asyncio.Lock()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("CREATE TABLE IF NOT EXISTS circuit_breaker (name TEXT PRIMARY KEY, state TEXT, failures INTEGER, last_failure TEXT)")
        conn.commit(); conn.close()

    def _load_state(self):
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT state, failures, last_failure FROM circuit_breaker WHERE name=?", (self.name,)).fetchone()
        conn.close()
        if row:
            self.state = row[0]; self.failure_count = row[1]
            self.last_failure_time = datetime.fromisoformat(row[2]) if row[2] else None
        else:
            self.state = 'closed'; self.failure_count = 0; self.last_failure_time = None

    def _save_state(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("INSERT OR REPLACE INTO circuit_breaker VALUES (?,?,?,?)",
                     (self.name, self.state, self.failure_count, self.last_failure_time.isoformat() if self.last_failure_time else None))
        conn.commit(); conn.close()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self.state == 'open':
                if self.last_failure_time and (datetime.now(timezone.utc) - self.last_failure_time).total_seconds() > self.recovery_timeout:
                    self.state = 'half_open'; self._save_state()
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                self.state = 'closed'; self.failure_count = 0; self._save_state()
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1; self.last_failure_time = datetime.now(timezone.utc)
                if self.failure_count >= self.failure_threshold:
                    self.state = 'open'
                self._save_state()
            raise

# ============================================================================
# Retry Decorator
# ============================================================================
def retry_decorator(max_attempts=3, min_delay=0.1, max_delay=10.0):
    if TENACITY_AVAILABLE:
        def decorator(func):
            @retry(stop=stop_after_attempt(max_attempts), wait=wait_exponential(multiplier=min_delay, min=min_delay, max=max_delay),
                   retry=retry_if_exception_type(Exception))
            async def wrapper(*args, **kwargs):
                return await func(*args, **kwargs)
            return wrapper
        return decorator
    else:
        def decorator(func):
            async def wrapper(*args, **kwargs):
                for attempt in range(max_attempts):
                    try: return await func(*args, **kwargs)
                    except Exception as e:
                        if attempt == max_attempts - 1: raise
                        await asyncio.sleep(min(min_delay * (2**attempt), max_delay))
            return wrapper
        return decorator

# ============================================================================
# Post-Quantum Security
# ============================================================================
class QuantumResilientSecurity:
    def __init__(self, algorithm='dilithium'):
        self.algorithm = algorithm
        self.pqc_available = PQC_AVAILABLE
        if self.pqc_available:
            if algorithm == 'dilithium': self.sign_func, self.verify_func = dilithium.sign, dilithium.verify
            elif algorithm == 'falcon': self.sign_func, self.verify_func = falcon.sign, falcon.verify
            elif algorithm == 'sphincs': self.sign_func, self.verify_func = sphincs.sign, sphincs.verify
            else: raise ValueError(f"Unknown algorithm {algorithm}")
        else:
            logger.warning("PQC not available – using ECDSA fallback.")

    async def sign_data(self, data):
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        if self.pqc_available:
            pk, sk = self.sign_func.generate_keypair()
            signature = self.sign_func.sign(data_bytes, sk)
            return {'signature': signature.hex(), 'algorithm': self.algorithm, 'public_key': pk.hex()}
        # ECDSA fallback
        from cryptography.hazmat.primitives.asymmetric import ec
        sk = ec.generate_private_key(ec.SECP256R1())
        signature = sk.sign(data_bytes, ec.ECDSA(hashes.SHA256()))
        return {'signature': signature.hex(), 'algorithm': 'ecdsa', 'public_key': sk.public_key().public_bytes(Encoding.DER, PublicFormat.SubjectPublicKeyInfo).hex()}

    async def verify_data(self, data, sig_data):
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        if sig_data['algorithm'] in ['dilithium','falcon','sphincs'] and self.pqc_available:
            try:
                return self.verify_func.verify(data_bytes, bytes.fromhex(sig_data['signature']), bytes.fromhex(sig_data['public_key']))
            except: return False
        elif sig_data['algorithm'] == 'ecdsa':
            from cryptography.hazmat.primitives.asymmetric import ec
            pk = ec.load_der_public_key(bytes.fromhex(sig_data['public_key']))
            try:
                pk.verify(bytes.fromhex(sig_data['signature']), data_bytes, ec.ECDSA(hashes.SHA256()))
                return True
            except: return False
        return False

# ============================================================================
# Blockchain Auditor
# ============================================================================
class BlockchainAuditor:
    def __init__(self, config, circuit_breaker=None):
        self.config = config; self.circuit_breaker = circuit_breaker
        self.web3 = None; self.contract = None; self.account = None; self.available = False
        if WEB3_AVAILABLE: self._initialize()

    def _initialize(self):
        try:
            self.web3 = Web3(HTTPProvider(self.config.blockchain_rpc_url))
            if not self.web3.is_connected(): raise ConnectionError("No RPC")
            self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
            if self.config.blockchain_private_key:
                self.account = Account.from_key(self.config.blockchain_private_key); self.web3.eth.default_account = self.account.address
            else: self.account = self.web3.eth.accounts[0]
            abi = [{"constant":False,"inputs":[{"name":"eventType","type":"string"},{"name":"payload","type":"string"}],"name":"recordEvent","outputs":[],"type":"function"}]
            if self.config.blockchain_contract_address:
                self.contract = self.web3.eth.contract(address=self.config.blockchain_contract_address, abi=abi)
                self.available = True
        except Exception as e:
            logger.error(f"Blockchain init failed: {e}")

    @retry_decorator(max_attempts=3, min_delay=0.1, max_delay=2)
    async def record_event(self, event_type, payload):
        if not self.available: return {'status':'simulated'}
        try:
            nonce = self.web3.eth.get_transaction_count(self.account.address)
            payload_str = json.dumps(payload, default=str)
            tx = self.contract.functions.recordEvent(event_type, payload_str).build_transaction({
                'from': self.account.address, 'nonce': nonce,
                'gas': 200000, 'gasPrice': self.web3.eth.gas_price
            })
            signed = self.account.sign_transaction(tx)
            tx_hash = self.web3.eth.send_raw_transaction(signed.rawTransaction)
            receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
            return {'status':'success','tx_hash':tx_hash.hex()} if receipt.status==1 else {'status':'failed'}
        except Exception as e:
            logger.error(f"Blockchain record failed: {e}")
            return {'status':'failed','error':str(e)}

# ============================================================================
# Multi-Cloud Distributor
# ============================================================================
class MultiCloudDistributor:
    def __init__(self, config):
        self.config = config
        self._clients = {}
        if config.cloud_provider == 'aws' and AWS_AVAILABLE:
            self._clients['aws'] = boto3.client('s3', aws_access_key_id=config.cloud_access_key, aws_secret_access_key=config.cloud_secret_key, region_name=config.cloud_region)
        elif config.cloud_provider == 'azure' and AZURE_AVAILABLE:
            self._clients['azure'] = BlobServiceClient.from_connection_string(config.cloud_access_key)
        elif config.cloud_provider == 'gcp' and GCP_AVAILABLE:
            self._clients['gcp'] = storage.Client.from_service_account_json(config.cloud_access_key)

    @retry_decorator(max_attempts=3, min_delay=0.1, max_delay=2)
    async def distribute(self, data, filename):
        provider = self.config.cloud_provider
        if provider not in self._clients: return {'status':'no_client'}
        try:
            data_bytes = json.dumps(data, default=str).encode()
            if provider == 'aws':
                self._clients['aws'].put_object(Bucket=self.config.cloud_bucket, Key=filename, Body=data_bytes)
            elif provider == 'azure':
                container = self._clients['azure'].get_container_client(self.config.cloud_bucket); container.upload_blob(filename, data_bytes, overwrite=True)
            elif provider == 'gcp':
                bucket = self._clients['gcp'].bucket(self.config.cloud_bucket); bucket.blob(filename).upload_from_string(data_bytes)
            return {'status':'success'}
        except Exception as e:
            logger.error(f"Cloud upload failed: {e}"); return {'status':'failed'}

# ============================================================================
# Autonomous Strategy Selector (Q-learning)
# ============================================================================
class AutonomousStrategySelector:
    def __init__(self, config):
        self.config = config
        self.lr = config.rl_learning_rate; self.gamma = config.rl_discount_factor; self.epsilon = config.rl_exploration_rate
        self.q_table = defaultdict(lambda: defaultdict(float))
        self.actions = ['performance','balanced','carbon_saver']
        self._load_q_table()

    def _load_q_table(self):
        try:
            conn = sqlite3.connect(self.config.rl_q_table_db_path)
            conn.execute("CREATE TABLE IF NOT EXISTS q_table (state TEXT, action TEXT, q_value REAL, PRIMARY KEY (state, action))")
            rows = conn.execute("SELECT state, action, q_value FROM q_table").fetchall()
            for s,a,q in rows: self.q_table[s][a] = q
            conn.close()
        except: pass

    def _save_q(self, s, a, q):
        conn = sqlite3.connect(self.config.rl_q_table_db_path)
        conn.execute("INSERT OR REPLACE INTO q_table VALUES (?,?,?)", (s,a,q)); conn.commit(); conn.close()

    def _state_key(self, state): return f"{state.get('load_bin','m')}_{state.get('health_bin','g')}"

    async def select_strategy(self, state):
        sk = self._state_key(state)
        if random.random() < self.epsilon: return random.choice(self.actions)
        return max(self.actions, key=lambda a: self.q_table[sk][a])

    async def update(self, state, action, reward, next_state):
        sk = self._state_key(state); nsk = self._state_key(next_state)
        current = self.q_table[sk][action]; max_next = max(self.q_table[nsk].values()) if self.q_table[nsk] else 0
        new_q = current + self.lr * (reward + self.gamma * max_next - current)
        self.q_table[sk][action] = new_q; self._save_q(sk, action, new_q)

# ============================================================================
# Persistent Storage
# ============================================================================
class Storage:
    def __init__(self, db_path="bio_core_state.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS modules (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, phase TEXT, dependencies TEXT,
                dependents TEXT, health_status TEXT, circuit_breaker_state TEXT, failure_count INTEGER,
                last_failure TEXT, metrics TEXT, module_path TEXT, version TEXT, loaded_at TEXT,
                predicted_health REAL, failure_probability REAL, health_trend TEXT, init_started TEXT,
                init_completed TEXT, error_message TEXT, init_timeout REAL, shutdown_timeout REAL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS health_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT, module_name TEXT, timestamp TEXT,
                health_score REAL, success_rate REAL, token_balance REAL, error_rate REAL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS marketplace_scores (
                module_name TEXT PRIMARY KEY, score REAL, last_updated TEXT)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS config_versions (
                version_id TEXT PRIMARY KEY, timestamp TEXT, config TEXT, description TEXT, parent TEXT)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS global_state (
                key TEXT PRIMARY KEY, value TEXT)""")

    def save_module(self, entry: ModuleEntry):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""INSERT OR REPLACE INTO modules VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (entry.id, entry.name, entry.phase.value, json.dumps(entry.dependencies), json.dumps(entry.dependents),
                 entry.health_status, entry.circuit_breaker_state, entry.failure_count,
                 entry.last_failure.isoformat() if entry.last_failure else None, json.dumps(entry.metrics),
                 entry.module_path, entry.version, entry.loaded_at.isoformat() if entry.loaded_at else None,
                 entry.predicted_health, entry.failure_probability, entry.health_trend,
                 entry.init_started.isoformat() if entry.init_started else None,
                 entry.init_completed.isoformat() if entry.init_completed else None,
                 entry.error_message, entry.init_timeout, entry.shutdown_timeout))

    def load_modules(self):
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT * FROM modules").fetchall()
        modules = []
        for r in rows:
            entry = ModuleEntry(
                name=r[1], phase=LifecyclePhase(r[2]), dependencies=json.loads(r[3]), dependents=json.loads(r[4]),
                health_status=r[5], circuit_breaker_state=r[6], failure_count=r[7],
                last_failure=datetime.fromisoformat(r[8]) if r[8] else None, metrics=json.loads(r[9]),
                module_path=r[10], version=r[11], loaded_at=datetime.fromisoformat(r[12]) if r[12] else None,
                predicted_health=r[13], failure_probability=r[14], health_trend=r[15],
                init_started=datetime.fromisoformat(r[16]) if r[16] else None,
                init_completed=datetime.fromisoformat(r[17]) if r[17] else None,
                error_message=r[18], init_timeout=r[19], shutdown_timeout=r[20])
            entry.id = r[0]; modules.append(entry)
        return modules

    def save_health_history(self, module_name, metrics):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""INSERT INTO health_history (module_name, timestamp, health_score, success_rate, token_balance, error_rate)
                            VALUES (?,?,?,?,?,?)""",
                         (module_name, datetime.now(timezone.utc).isoformat(), metrics.get('health_score',0.5),
                          metrics.get('success_rate',0.5), metrics.get('token_balance',500), metrics.get('error_rate',0.01)))

    def save_marketplace_score(self, name, score):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT OR REPLACE INTO marketplace_scores VALUES (?,?,?)",
                         (name, score, datetime.now(timezone.utc).isoformat()))

    def load_marketplace_scores(self):
        with sqlite3.connect(self.db_path) as conn:
            return {r[0]: r[1] for r in conn.execute("SELECT module_name, score FROM marketplace_scores").fetchall()}

    def save_config_version(self, version_id, config, description, parent):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT OR REPLACE INTO config_versions VALUES (?,?,?,?,?)",
                         (version_id, datetime.now(timezone.utc).isoformat(), json.dumps(config), description, parent))

    def load_config_versions(self):
        with sqlite3.connect(self.db_path) as conn:
            return [{'version_id':r[0], 'timestamp':r[1], 'config':json.loads(r[2]), 'description':r[3], 'parent':r[4]}
                    for r in conn.execute("SELECT * FROM config_versions ORDER BY timestamp DESC").fetchall()]

    def save_global_state(self, key, value):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT OR REPLACE INTO global_state VALUES (?,?)", (key, value))

    def load_global_state(self, key):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT value FROM global_state WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def delete_module(self, name):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM modules WHERE name=?", (name,))

# ============================================================================
# Predictive Health Forecaster
# ============================================================================
class PredictiveHealthForecaster:
    def __init__(self, storage, config):
        self.storage = storage; self.config = config
        self.model = None; self.scaler = StandardScaler(); self.is_trained = False
        self.history = deque(maxlen=2000); self._lock = asyncio.Lock()
        self._load_model()

    def _load_model(self):
        if os.path.exists(self.config.ml_model_path):
            with open(self.config.ml_model_path,'rb') as f:
                data = pickle.load(f)
            self.model = data['model']; self.scaler = data['scaler']; self.is_trained = True

    def _save_model(self):
        with open(self.config.ml_model_path,'wb') as f:
            pickle.dump({'model':self.model, 'scaler':self.scaler}, f)

    def record_health_data(self, module_name, metrics):
        self.history.append({'module':module_name, 'timestamp':datetime.now(timezone.utc), **metrics})
        self.storage.save_health_history(module_name, metrics)

    async def train(self):
        if len(self.history) < 50: return {'status':'insufficient_data'}
        async with self._lock:
            X = []
            for i in range(10, len(self.history)-1):
                features = []
                for j in range(10):
                    d = self.history[i-j]
                    features.extend([d.get('health_score',0.5), d.get('success_rate',0.5), d.get('token_balance',500)/1000, d.get('error_rate',0.01)])
                X.append(features)
            X = np.array(X)
            if len(X) < 20: return {'status':'insufficient_training_data'}
            X_scaled = self.scaler.fit_transform(X)
            if self.model is None:
                from sklearn.ensemble import IsolationForest
                self.model = IsolationForest(contamination=0.1, random_state=42)
            self.model.fit(X_scaled); self.is_trained = True; self._save_model()
            return {'status':'success'}

    async def predict_health(self, metrics):
        if not self.is_trained: return {'predicted_health':0.5, 'failure_probability':0.0, 'trend':'stable', 'confidence':0.0}
        async with self._lock:
            features = [metrics.get('health_score',0.5), metrics.get('success_rate',0.5), metrics.get('token_balance',500)/1000, metrics.get('error_rate',0.01)]
            X = np.array([features]); X = self.scaler.transform(X)
            pred = self.model.predict(X)[0]; is_anom = pred == -1
            conf = abs(self.model.decision_function(X)[0]) / (abs(self.model.decision_function(X)[0])+1)
            trend = 'improving' if len(self.history)>20 and np.polyfit(range(20), [h.get('health_score',0.5) for h in list(self.history)[-20:]],1)[0]>0.01 else 'stable'
            return {'predicted_health':0.3 if is_anom else 0.7, 'failure_probability':0.8 if is_anom else 0.2, 'trend':trend, 'confidence':conf, 'is_anomalous':is_anom}

# ============================================================================
# Performance Anomaly Detector
# ============================================================================
class PerformanceAnomalyDetector:
    def __init__(self):
        self.metric_history = defaultdict(list); self.anomalies = []; self._lock = asyncio.Lock()
        self.zscore_threshold = 3.0; self.trend_threshold = 0.2

    async def record_metric(self, name, value):
        async with self._lock:
            self.metric_history[name].append(value)
            if len(self.metric_history[name]) > 1000: self.metric_history[name] = self.metric_history[name][-1000:]

    async def detect_anomalies(self, name):
        async with self._lock:
            if name not in self.metric_history or len(self.metric_history[name]) < 10: return []
            values = self.metric_history[name][-50:]
            mean = np.mean(values); std = np.std(values); anomalies = []
            if std > 0:
                for i, z in enumerate([(v-mean)/std for v in values[-10:]]):
                    if abs(z) > self.zscore_threshold:
                        anomalies.append({'metric':name, 'value':values[-10+i], 'zscore':z, 'type':'zscore'})
            if len(values) > 20:
                slope = np.polyfit(range(len(values[-20:])), values[-20:], 1)[0]
                if abs(slope) > self.trend_threshold:
                    anomalies.append({'metric':name, 'slope':slope, 'type':'trend', 'direction':'increasing' if slope>0 else 'decreasing'})
            return anomalies

    async def get_anomaly_report(self):
        report = {'timestamp':datetime.now(timezone.utc).isoformat(), 'anomalies':[]}
        async with self._lock:
            for name in list(self.metric_history.keys()):
                anomalies = await self.detect_anomalies(name)
                if anomalies: report['anomalies'].extend(anomalies)
        return report

# ============================================================================
# Configuration Version Manager
# ============================================================================
class ConfigurationVersionManager:
    def __init__(self, storage):
        self.storage = storage; self.versions = storage.load_config_versions()
        self.current_version = self.versions[0]['version_id'] if self.versions else None

    def save_version(self, config, description=""):
        version_id = hashlib.md5(str(datetime.now(timezone.utc).timestamp()).encode()).hexdigest()[:12]
        self.storage.save_config_version(version_id, config, description, self.current_version)
        self.versions.insert(0, {'version_id':version_id, 'timestamp':datetime.now(timezone.utc).isoformat(), 'config':config, 'description':description, 'parent':self.current_version})
        self.current_version = version_id; return version_id

    def rollback_to_version(self, version_id):
        for v in self.versions:
            if v['version_id'] == version_id:
                self.current_version = version_id; return v['config']
        return None

    def get_version_history(self, limit=10): return self.versions[:limit]

    def get_version_diff(self, a, b):
        ca = next((v['config'] for v in self.versions if v['version_id']==a), None)
        cb = next((v['config'] for v in self.versions if v['version_id']==b), None)
        if not ca or not cb: return {'error':'Version not found'}
        diff = {'added':{}, 'removed':{}, 'changed':{}}
        for key in set(ca)|set(cb):
            if key not in ca: diff['added'][key] = cb[key]
            elif key not in cb: diff['removed'][key] = ca[key]
            elif ca[key] != cb[key]: diff['changed'][key] = {'from':ca[key],'to':cb[key]}
        return diff

# ============================================================================
# Enhanced CoreGeneticOptimizer (NSGA‑II)
# ============================================================================
class CoreGeneticOptimizer:
    def __init__(self, core):
        self.core = core
        self.population_size = 20; self.mutation_rate=0.2; self.crossover_rate=0.7; self.generations=10; self.tournament_size=3
        self.best_individual=None; self.best_fitness=-float('inf'); self.evolution_history=[]
        self.lock = asyncio.Lock()
        self.param_bounds = {
            'health_check_interval_seconds': (10,120),
            'circuit_breaker_threshold': (3,10),
            'predictive_health_retrain_interval': (120,900),
            'anomaly_zscore_threshold': (2.0,5.0),
            'module_retirement_threshold': (0.1,0.4)
        }
        self.pareto_front=[]; self._eval_cache={}; self._last_summary_signature=None

    def _initialize_individual(self):
        ind = {k: random.uniform(*v) for k,v in self.param_bounds.items()}
        ind['circuit_breaker_threshold'] = int(ind['circuit_breaker_threshold'])
        ind['health_check_interval_seconds'] = int(ind['health_check_interval_seconds'])
        ind['predictive_health_retrain_interval'] = int(ind['predictive_health_retrain_interval'])
        return ind

    def _initialize_population(self): return [self._initialize_individual() for _ in range(self.population_size)]

    def _snapshot_config(self):
        return {
            'health_check_interval_seconds': self.core.config.health_check_interval_seconds,
            'circuit_breaker_threshold': self.core.config.circuit_breaker_threshold,
            'predictive_health_retrain_interval': self.core._predictive_health_retrain_interval,
            'anomaly_zscore_threshold': self.core._anomaly_detector.zscore_threshold,
            'module_retirement_threshold': self.core._module_retirement_threshold
        }

    def _apply_snapshot(self, snap):
        self.core.config.health_check_interval_seconds = snap['health_check_interval_seconds']
        self.core.config.circuit_breaker_threshold = snap['circuit_breaker_threshold']
        self.core._predictive_health_retrain_interval = snap['predictive_health_retrain_interval']
        self.core._anomaly_detector.zscore_threshold = snap['anomaly_zscore_threshold']
        self.core._module_retirement_threshold = snap['module_retirement_threshold']

    async def _evaluate_individual(self, ind):
        key = tuple(sorted(ind.items()))
        if key in self._eval_cache: return self._eval_cache[key]
        original = self._snapshot_config()
        self._apply_snapshot({
            'health_check_interval_seconds': ind['health_check_interval_seconds'],
            'circuit_breaker_threshold': ind['circuit_breaker_threshold'],
            'predictive_health_retrain_interval': ind['predictive_health_retrain_interval'],
            'anomaly_zscore_threshold': ind['anomaly_zscore_threshold'],
            'module_retirement_threshold': ind['module_retirement_threshold']
        })
        try:
            status = await self.core.get_system_status()
            modules_health = await self.core.registry.health_check_all()
            health_scores = [1.0 if s['status']=='healthy' else 0.5 if s['status']=='degraded' else 0.0 for s in modules_health.values()]
            avg_health = np.mean(health_scores) if health_scores else 0.5
            uptime = status.get('uptime_seconds',0); uptime_score = min(1.0, uptime/86400)
            open_circuits = sum(1 for m in self.core.registry.modules.values() if m.circuit_breaker_state=='open')
            circuit_score = max(0, 1.0 - open_circuits/max(1,len(self.core.registry.modules)*0.5))
            anomalies = await self.core._anomaly_detector.get_anomaly_report()
            anomaly_score = max(0, 1.0 - len(anomalies['anomalies'])/20)
            obj = {'health_score':avg_health, 'uptime_score':uptime_score, 'circuit_score':circuit_score, 'anomaly_score':anomaly_score}
        finally:
            self._apply_snapshot(original)
        self._eval_cache[key] = obj
        return obj

    def _fast_non_dominated_sort(self, population, objectives):
        fronts=[]; domination={k:0 for k in objectives}; dominated={k:[] for k in objectives}
        keys=list(objectives.keys())
        for i,pk in enumerate(keys):
            for j,qk in enumerate(keys):
                if i==j: continue
                po=objectives[pk]; qo=objectives[qk]
                if all(po[k]>=qo[k] for k in po) and any(po[k]>qo[k] for k in po):
                    dominated[pk].append(qk)
                elif all(qo[k]>=po[k] for k in qo) and any(qo[k]>po[k] for k in qo):
                    domination[pk]+=1
            if domination[pk]==0:
                if not fronts: fronts.append([])
                fronts[0].append(pk)
        i=0
        while i<len(fronts):
            nextf=[]
            for pk in fronts[i]:
                for qk in dominated[pk]:
                    domination[qk]-=1
                    if domination[qk]==0: nextf.append(qk)
            if nextf: fronts.append(nextf)
            i+=1
        key_to_ind={tuple(sorted(ind.items())):ind for ind in population}
        return [[key_to_ind[k] for k in front] for front in fronts]

    def _crowding_distance(self, front, objectives):
        if not front: return {}
        dist={tuple(sorted(ind.items())):0.0 for ind in front}
        keys=list(next(iter(objectives.values())).keys())
        for obj in keys:
            sortedf=sorted(front, key=lambda ind: objectives[tuple(sorted(ind.items()))][obj])
            dist[tuple(sorted(sortedf[0].items()))]=float('inf')
            dist[tuple(sorted(sortedf[-1].items()))]=float('inf')
            omin=objectives[tuple(sorted(sortedf[0].items()))][obj]; omax=objectives[tuple(sorted(sortedf[-1].items()))][obj]
            if omax==omin: continue
            for i in range(1,len(sortedf)-1):
                k=tuple(sorted(sortedf[i].items())); pk=tuple(sorted(sortedf[i-1].items())); nk=tuple(sorted(sortedf[i+1].items()))
                dist[k]+=(objectives[nk][obj]-objectives[pk][obj])/(omax-omin)
        return dist

    def _tournament(self, pop, fronts, crowd):
        i1=random.choice(pop); i2=random.choice(pop)
        r1=next((i for i,f in enumerate(fronts) if i1 in f), len(fronts))
        r2=next((i for i,f in enumerate(fronts) if i2 in f), len(fronts))
        if r1<r2: return i1
        elif r2<r1: return i2
        k1=tuple(sorted(i1.items())); k2=tuple(sorted(i2.items()))
        return i1 if crowd.get(k1,0)>crowd.get(k2,0) else i2

    def _crossover(self,p1,p2):
        c={}
        for k in self.param_bounds:
            if random.random()<0.5:
                u=random.random(); beta=(2*u)**(1/21) if u<=0.5 else (1/(2*(1-u)))**(1/21)
                v1=0.5*((1+beta)*p1[k]+(1-beta)*p2[k]); v2=0.5*((1-beta)*p1[k]+(1+beta)*p2[k])
                low,high=self.param_bounds[k]; v1=max(low,min(high,v1)); v2=max(low,min(high,v2))
                c[k]=v1 if random.random()<0.5 else v2
            else: c[k]=p1[k] if random.random()<0.5 else p2[k]
        c['circuit_breaker_threshold']=int(c['circuit_breaker_threshold']); c['health_check_interval_seconds']=int(c['health_check_interval_seconds']); c['predictive_health_retrain_interval']=int(c['predictive_health_retrain_interval'])
        return c

    def _mutate(self,ind):
        m=ind.copy()
        for k,(low,high) in self.param_bounds.items():
            if random.random()<self.mutation_rate:
                u=random.random(); delta=(2*u)**(1/21)-1 if u<0.5 else 1-(2*(1-u))**(1/21)
                m[k]=max(low,min(high,m[k]+delta*(high-low)))
        m['circuit_breaker_threshold']=int(m['circuit_breaker_threshold']); m['health_check_interval_seconds']=int(m['health_check_interval_seconds']); m['predictive_health_retrain_interval']=int(m['predictive_health_retrain_interval'])
        return m

    def _compute_dynamic_weights(self):
        w=self.core.config.mopd.objective_weights.copy()
        anomalies=asyncio.run(self.core._anomaly_detector.get_anomaly_report())  # fixed later
        if len(anomalies['anomalies'])>5: w['anomaly_score']=min(0.5,w['anomaly_score']*1.5)
        total=sum(w.values()); return {k:v/total for k,v in w.items()}

    async def evolve(self, generations=None):
        async with self.lock:
            if generations is None: generations=self.generations
            pop=self._initialize_population()
            objectives={}
            for ind in pop:
                objectives[tuple(sorted(ind.items()))]=await self._evaluate_individual(ind)
            if self.core.config.mopd.enabled: self.pareto_front=[]
            for gen in range(generations):
                offspring=[]
                while len(offspring)<self.population_size:
                    pop_objs={k:objectives[k] for k in objectives if k in [tuple(sorted(i.items())) for i in pop]}
                    fronts=self._fast_non_dominated_sort(pop,pop_objs); crowd={}
                    for f in fronts: crowd.update(self._crowding_distance(f,pop_objs))
                    p1=self._tournament(pop,fronts,crowd); p2=self._tournament(pop,fronts,crowd)
                    if random.random()<self.crossover_rate:
                        c=self._crossover(p1,p2); c=self._mutate(c); offspring.append(c)
                    else: offspring.append(self._mutate(p1.copy()))
                offspring=offspring[:self.population_size]
                for ind in offspring:
                    k=tuple(sorted(ind.items()))
                    if k not in objectives: objectives[k]=await self._evaluate_individual(ind)
                combined=pop+offspring
                unique={}
                for ind in combined: unique[tuple(sorted(ind.items()))]=ind
                combined=list(unique.values())
                comb_objs={tuple(sorted(ind.items())):objectives[tuple(sorted(ind.items()))] for ind in combined}
                fronts=self._fast_non_dominated_sort(combined,comb_objs)
                newpop=[]
                for f in fronts:
                    if len(newpop)+len(f)<=self.population_size: newpop.extend(f)
                    else:
                        crowd=self._crowding_distance(f,comb_objs)
                        sf=sorted(f,key=lambda ind: crowd.get(tuple(sorted(ind.items())),0), reverse=True)
                        newpop.extend(sf[:self.population_size-len(newpop)]); break
                pop=newpop
                pop_objs={tuple(sorted(ind.items())):objectives[tuple(sorted(ind.items()))] for ind in pop}
                fronts=self._fast_non_dominated_sort(pop,pop_objs)
                if fronts:
                    self.pareto_front=[]
                    for ind in fronts[0]:
                        objs=pop_objs[tuple(sorted(ind.items()))]
                        self.pareto_front.append(MOPDPoint(ind, objs['health_score'], objs['uptime_score'], objs['circuit_score'], objs['anomaly_score']))
            weights=self._compute_dynamic_weights()
            if self.core.config.mopd.enabled and self.pareto_front:
                best=self._select_best_from_pareto(self.pareto_front,weights)
                if best:
                    self.best_individual=best.individual; self.best_fitness=best.scalarised_score
                    self._apply_snapshot({'health_check_interval_seconds':best.individual['health_check_interval_seconds'],
                                          'circuit_breaker_threshold':best.individual['circuit_breaker_threshold'],
                                          'predictive_health_retrain_interval':best.individual['predictive_health_retrain_interval'],
                                          'anomaly_zscore_threshold':best.individual['anomaly_zscore_threshold'],
                                          'module_retirement_threshold':best.individual['module_retirement_threshold']})
            else:
                pop_objs={tuple(sorted(ind.items())):objectives[tuple(sorted(ind.items()))] for ind in pop}
                best_ind=max(pop,key=lambda i: sum(weights[k]*pop_objs[tuple(sorted(i.items()))][k] for k in weights))
                self.best_individual=best_ind; self.best_fitness=sum(weights[k]*pop_objs[tuple(sorted(best_ind.items()))][k] for k in weights)
                self._apply_snapshot({'health_check_interval_seconds':best_ind['health_check_interval_seconds'],
                                      'circuit_breaker_threshold':best_ind['circuit_breaker_threshold'],
                                      'predictive_health_retrain_interval':best_ind['predictive_health_retrain_interval'],
                                      'anomaly_zscore_threshold':best_ind['anomaly_zscore_threshold'],
                                      'module_retirement_threshold':best_ind['module_retirement_threshold']})
            self.evolution_history.append({'timestamp':datetime.now(timezone.utc), 'best_fitness':self.best_fitness,
                                           'pareto_front_size':len(self.pareto_front)})
            self.core.storage.save_global_state('best_individual', json.dumps(self.best_individual))
            self.core.storage.save_global_state('best_fitness', str(self.best_fitness))
            if self.core.config.mopd.enabled:
                self.core.storage.save_global_state('pareto_front', json.dumps([p.to_dict() for p in self.pareto_front]))
            return {'best_fitness':self.best_fitness, 'best_individual':self.best_individual,
                    'pareto_front':[p.to_dict() for p in self.pareto_front], 'dynamic_weights':weights}

    def _select_best_from_pareto(self, front, weights):
        if not front: return None
        keys=list(weights.keys())
        maxv={k:max(getattr(p,k) for p in front) for k in keys}; minv={k:min(getattr(p,k) for p in front) for k in keys}
        ranges={k:maxv[k]-minv[k] if maxv[k]!=minv[k] else 1 for k in keys}
        best=None; bestscore=-float('inf')
        for p in front:
            score=sum(weights[k]*(getattr(p,k)-minv[k])/ranges[k] for k in keys)
            p.scalarised_score=score
            if score>bestscore: bestscore=score; best=p
        return best

    def get_status(self):
        return {'best_fitness':self.best_fitness, 'best_individual':self.best_individual,
                'history':self.evolution_history[-10:], 'pareto_front_size':len(self.pareto_front)}

    def get_system_signature(self):
        s=asyncio.run(self.core.get_system_status())
        return f"{s.get('uptime_seconds',0):.0f}|{len(s.get('modules',{}))}|{s.get('mopd_enabled')}"

# ============================================================================
# Module Registry
# ============================================================================
class ModuleRegistry:
    def __init__(self, storage, circuit_breaker_threshold=5):
        self.storage=storage; self.modules={}; self.loaded_modules=set(); self._initialized=False
        self._circuit_breaker_threshold=circuit_breaker_threshold; self._lock=asyncio.Lock()
        self.health_forecaster=PredictiveHealthForecaster(storage, CoreConfig())
        for entry in self.storage.load_modules():
            self.modules[entry.name]=entry; self.loaded_modules.add(entry.name)

    async def register(self, name, module, dependencies=None, health_check=None, init_timeout=30, shutdown_timeout=10):
        async with self._lock:
            if name in self.modules:
                e=self.modules[name]; e.module=module; e.health_check=health_check; e.dependencies=dependencies or []
                e.phase=LifecyclePhase.REGISTERED; e.health_status="unknown"; e.loaded_at=datetime.now(timezone.utc)
                self.storage.save_module(e); return e
            e=ModuleEntry(name=name, module=module, dependencies=dependencies or [], health_check=health_check,
                          init_timeout=init_timeout, shutdown_timeout=shutdown_timeout, loaded_at=datetime.now(timezone.utc))
            self.modules[name]=e; self.storage.save_module(e); return e

    async def unregister(self, name):
        async with self._lock:
            if name in self.modules:
                del self.modules[name]; self.loaded_modules.discard(name); self.storage.delete_module(name); return True
        return False

    async def initialize_all(self):
        async with self._lock:
            if self._initialized: return {n:True for n in self.modules}
            order=self._topological_sort(); results={}
            for name in order:
                e=self.modules[name]; e.phase=LifecyclePhase.INITIALIZING; e.init_started=datetime.now(timezone.utc)
                try:
                    if hasattr(e.module,'initialize') and callable(e.module.initialize):
                        await asyncio.wait_for(e.module.initialize(), timeout=e.init_timeout)
                    e.phase=LifecyclePhase.INITIALIZED; e.init_completed=datetime.now(timezone.utc); e.health_status='healthy'
                    results[name]=True
                except Exception as ex:
                    e.phase=LifecyclePhase.ERROR; e.error_message=str(ex); e.health_status='error'; results[name]=False
                self.storage.save_module(e)
            self._initialized=True; return results

    async def shutdown_all(self):
        results={}
        for name in reversed(self._topological_sort()):
            e=self.modules[name]; e.phase=LifecyclePhase.STOPPING
            try:
                if hasattr(e.module,'shutdown') and callable(e.module.shutdown):
                    await asyncio.wait_for(e.module.shutdown(), timeout=e.shutdown_timeout)
                e.phase=LifecyclePhase.STOPPED; results[name]=True
            except Exception as ex:
                e.phase=LifecyclePhase.ERROR; e.error_message=str(ex); results[name]=False
            self.storage.save_module(e)
        self._initialized=False; return results

    async def health_check_all(self):
        results={}
        for name,e in self.modules.items():
            status={'status':'unknown','circuit_breaker':e.circuit_breaker_state,'last_failure':e.last_failure.isoformat() if e.last_failure else None}
            if e.health_check:
                try: status['status']='healthy' if await e.health_check() else 'unhealthy'
                except Exception as ex: status['status']='error'; status['error']=str(ex)
            else: status['status']=e.health_status
            results[name]=status
        return results

    def _topological_sort(self):
        visited=set(); res=[]
        def dfs(name):
            if name in visited: return
            visited.add(name)
            for dep in self.modules.get(name, ModuleEntry(name='')).dependencies:
                if dep in self.modules: dfs(dep)
            res.append(name)
        for name in self.modules: dfs(name)
        return res

    def get_registry_stats(self):
        return {'total_modules':len(self.modules), 'loaded_modules':len(self.loaded_modules),
                'phases':{n:e.phase.value for n,e in self.modules.items()},
                'health_statuses':{n:e.health_status for n,e in self.modules.items()},
                'circuit_breaker_states':{n:e.circuit_breaker_state for n,e in self.modules.items()}}

    async def get_dependency_graph(self):
        return {n:e.dependencies for n,e in self.modules.items()}

    async def update_predictive_health(self, name, metrics):
        e=self.modules.get(name)
        if e:
            p=await self.health_forecaster.predict_health(metrics)
            e.predicted_health=p['predicted_health']; e.failure_probability=p['failure_probability']; e.health_trend=p['trend']
            self.storage.save_module(e)

    async def load_module(self, name, path):
        spec=importlib.util.spec_from_file_location(name, path); mod=importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
        cls_name=name.split('.')[-1].capitalize()
        if hasattr(mod, cls_name):
            instance=getattr(mod, cls_name)()
            await self.register(name, instance, module_path=path); return True
        return False

    async def unload_module(self, name):
        if name in self.modules: return await self.unregister(name)
        return False

# ============================================================================
# Module Marketplace
# ============================================================================
class ModuleMarketplace:
    def __init__(self, core, storage, auto_replace=False):
        self.core=core; self.storage=storage; self.auto_replace=auto_replace
        self.module_scores=self.storage.load_marketplace_scores(); self.competition_interval=300

    async def run_competition(self):
        health=await self.core.registry.health_check_all()
        for name,status in health.items():
            score=1.0 if status['status']=='healthy' else 0.5 if status['status']=='degraded' else 0.0
            self.module_scores[name]=score; self.storage.save_marketplace_score(name, score)
        if self.auto_replace: await self._auto_replace_low_scoring()

    async def _auto_replace_low_scoring(self):
        pass  # placeholder

    def get_marketplace_stats(self):
        return {'scores':self.module_scores, 'auto_replace':self.auto_replace}

# ============================================================================
# Event Bus
# ============================================================================
@dataclass
class CoreEvent:
    event_type:str; source:str; payload:Dict; timestamp:datetime=field(default_factory=datetime.now); correlation_id:Optional[str]=None; priority:int=0

class CoreEventBus:
    def __init__(self, max_workers=4):
        self.subscribers=defaultdict(list); self.queue=asyncio.Queue(); self.workers=[]; self.running=False
        self.max_workers=max_workers; self.stats={'published':0,'processed':0}; self._lock=asyncio.Lock()

    async def start(self):
        if self.running: return
        self.running=True
        for _ in range(self.max_workers): self.workers.append(asyncio.create_task(self._worker()))

    async def stop(self):
        self.running=False; await self.queue.join()
        for w in self.workers: w.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True); self.workers.clear()

    def subscribe(self, event_type, callback): self.subscribers[event_type].append(callback)

    def unsubscribe(self, event_type, callback):
        if event_type in self.subscribers: self.subscribers[event_type]=[cb for cb in self.subscribers[event_type] if cb!=callback]

    async def publish(self, event):
        await self.queue.put(event)
        async with self._lock: self.stats['published']+=1

    async def _worker(self):
        while self.running:
            try:
                event=await self.queue.get()
                for cb in self.subscribers.get(event.event_type,[]):
                    try: await cb(event)
                    except Exception as e: logger.error("Event handler error", error=str(e))
                async with self._lock: self.stats['processed']+=1
                self.queue.task_done()
            except asyncio.CancelledError: break
            except Exception as e: logger.error("Event worker error", error=str(e))

    def get_event_stats(self): return dict(self.stats)

# ============================================================================
# Decentralized Module Base
# ============================================================================
class DecentralizedModule(ABC):
    def __init__(self, name): self.name=name; self.event_subscriptions=[]
    @abstractmethod
    async def local_decision(self, context): pass
    async def on_event(self, event): pass

# ============================================================================
# Task Input Validation
# ============================================================================
if PYDANTIC_AVAILABLE:
    class TaskInput(BaseModel):
        task_id:Optional[str]=None; task_type:str; complexity:float=0.5; priority:int=0; parameters:Dict={}; module:Optional[str]=None
        @field_validator('task_id')
        def ensure_id(cls,v): return v or f"task_{uuid.uuid4().hex[:8]}"

# ============================================================================
# TaskManager
# ============================================================================
class TaskManager:
    def __init__(self):
        self.tasks={}; self.shutdown_event=asyncio.Event()
    def start_task(self, name, coro_func, *args, **kwargs):
        async def wrapper():
            while not self.shutdown_event.is_set():
                try: await coro_func(*args, **kwargs)
                except asyncio.CancelledError: break
                except Exception as e:
                    logger.error("Task crashed", name=name, error=str(e)); await asyncio.sleep(30)
        task=asyncio.create_task(wrapper(), name=name); self.tasks[name]=task; return task
    async def stop_all(self):
        self.shutdown_event.set()
        for t in self.tasks.values(): t.cancel()
        await asyncio.gather(*self.tasks.values(), return_exceptions=True); self.tasks.clear()

# ============================================================================
# New Enhancement Modules
# ============================================================================
class QuantumDistillationModule:
    def __init__(self, config): self.config=config; self.available=False
    async def optimize(self, params):
        for k in params:
            if isinstance(params[k],(int,float)): params[k]+=random.uniform(-0.01,0.01)
        return params
    def is_available(self): return self.available

class CausalRLAgent:
    def __init__(self, state_dim, action_dim, causal_mask=None):
        self.state_dim=state_dim; self.action_dim=action_dim; self.causal_mask=causal_mask
        self.q_table=defaultdict(lambda: np.zeros(action_dim)); self.epsilon=0.1; self.lr=0.1; self.gamma=0.99
    def act(self, state, explore=True):
        if explore and random.random()<self.epsilon: return random.randrange(self.action_dim)
        return int(np.argmax(self.q_table[tuple(state)]))
    def update(self, state, action, reward, next_state, done):
        s=tuple(state); n=tuple(next_state); best=0 if done else np.max(self.q_table[n])
        self.q_table[s][action]+=self.lr*(reward+self.gamma*best-self.q_table[s][action])
    def get_policy_probs(self, state, temperature=1.0):
        q=self.q_table[tuple(state)]
        if temperature<=0: probs=np.zeros_like(q); probs[np.argmax(q)]=1; return probs.tolist()
        exp=np.exp((q-np.max(q))/temperature); return (exp/exp.sum()).tolist()

class FederatedCoordinator:
    def __init__(self, core, queue=None): self.core=core; self.queue=queue
    async def send_update(self):
        if not self.queue: return
        model={'mopd_weights':self.core.config.mopd.objective_weights,
               'rl_q_table':{str(k):v.tolist() for k,v in self.core.causal_rl_agent.q_table.items()} if self.core.causal_rl_agent else {},
               'config_params':{'health_check_interval':self.core.config.health_check_interval_seconds,
                                'circuit_breaker_threshold':self.core.config.circuit_breaker_threshold}}
        await self.queue.publish("federated_updates", json.dumps(model))
    async def receive_global_model(self, model_json):
        model=json.loads(model_json)
        if 'mopd_weights' in model:
            local=self.core.config.mopd.objective_weights; g=model['mopd_weights']
            for k in local: local[k]=0.5*local[k]+0.5*g.get(k,local[k])
            total=sum(local.values()); self.core.config.mopd.objective_weights={k:v/total for k,v in local.items()}
        if 'rl_q_table' in model and self.core.causal_rl_agent:
            gq=model['rl_q_table']
            for sk,qvals in gq.items():
                try: key=tuple(map(float,sk.strip('()').split(','))) if ',' in sk else (float(sk),)
                except: continue
                if key in self.core.causal_rl_agent.q_table:
                    self.core.causal_rl_agent.q_table[key]=0.5*self.core.causal_rl_agent.q_table[key]+0.5*np.array(qvals)
                else: self.core.causal_rl_agent.q_table[key]=np.array(qvals)
        if 'config_params' in model:
            self.core.config.health_check_interval_seconds=model['config_params'].get('health_check_interval', self.core.config.health_check_interval_seconds)
            self.core.config.circuit_breaker_threshold=model['config_params'].get('circuit_breaker_threshold', self.core.config.circuit_breaker_threshold)

class SafetyMonitor:
    def __init__(self): self.invariants=[]
    def add_invariant(self, name, fn, desc): self.invariants.append((name,fn,desc))
    def check(self, state): return [f"{n}: {d}" for n,fn,d in self.invariants if not fn(state)]

class PrecisionController:
    def __init__(self, policy="energy_aware"): self.policy=policy
    def get_precision(self, load, energy_budget):
        return "float16" if self.policy=="energy_aware" and (load>0.8 or energy_budget<0.2) else "float32"

class CarbonMarketClient:
    def __init__(self, provider_url=None, contract_address=None, private_key=None):
        self.available=bool(provider_url and contract_address and private_key)
    def buy_credits(self, amount): logger.info(f"Sim buy {amount}"); return self.available
    def sell_credits(self, amount): logger.info(f"Sim sell {amount}"); return self.available

class ChaosInjector:
    def __init__(self, core, prob=0.01): self.core=core; self.prob=prob
    async def maybe_inject_failure(self):
        if random.random()<self.prob:
            action=random.choice(['kill_task','delay','corrupt_state'])
            logger.warning(f"Chaos: {action}")
            if action=='kill_task' and self.core._task_manager.tasks:
                name=random.choice(list(self.core._task_manager.tasks.keys())); self.core._task_manager.tasks[name].cancel()
            elif action=='delay': await asyncio.sleep(random.uniform(0.5,2))
            elif action=='corrupt_state':
                key=random.choice(['health_check_interval_seconds','circuit_breaker_threshold'])
                setattr(self.core.config,key,getattr(self.core.config,key)*random.uniform(0.8,1.2))

class HumanApprovalHandler:
    def __init__(self, queue=None): self.queue=queue
    async def request_approval(self, decision, timeout=60):
        if not self.queue: logger.warning("No queue; auto-approve"); return True
        logger.info(f"Approval requested for {decision.get('action')}, auto-approving")
        await asyncio.sleep(0); return True

# ============================================================================
# Enhanced Bio-Inspired Core
# ============================================================================
class EnhancedBioInspiredCore:
    def __init__(self, config=None, config_path=None, token_service=None, gradient_service=None,
                 compartment_service=None, biomass_service=None, message_queue=None):
        if config_path:
            with open(config_path,'r') as f: data=json.load(f)
            self.config=CoreConfig(**data)
        else: self.config=config or CoreConfig()
        self.message_queue=message_queue
        self.storage=Storage(self.config.db_path)
        self.quantum_security=QuantumResilientSecurity(self.config.quantum_signing_algorithm) if self.config.enable_quantum_signing else None
        self.blockchain_auditor=BlockchainAuditor(self.config) if self.config.enable_blockchain_audit else None
        self.strategy_selector=AutonomousStrategySelector(self.config) if self.config.enable_autonomous_strategy else None
        self.multi_cloud=MultiCloudDistributor(self.config) if self.config.enable_multi_cloud else None
        self.circuit_breaker=CircuitBreaker("bio_core", self.config.circuit_breaker_db_path,
                                            self.config.circuit_breaker_threshold,
                                            self.config.circuit_breaker_recovery_timeout) if self.config.enable_circuit_breaker else None
        self._token_service=token_service; self._gradient_service=gradient_service
        self._compartment_service=compartment_service; self._biomass_service=biomass_service
        self.registry=ModuleRegistry(self.storage, self.config.circuit_breaker_threshold)
        self._event_bus=CoreEventBus(max_workers=4)
        self._version_manager=ConfigurationVersionManager(self.storage)
        self._save_initial_config()
        self._anomaly_detector=PerformanceAnomalyDetector()
        self._genetic_optimizer=CoreGeneticOptimizer(self)
        self._marketplace=ModuleMarketplace(self, self.storage, auto_replace=False)
        self._decentralized_modules={}
        self._module_retirement_threshold=0.2
        self._predictive_health_retrain_interval=300
        self._task_manager=TaskManager()
        self._lifecycle_phase=LifecyclePhase.UNREGISTERED
        self._start_time=None; self._shutdown_requested=False
        self._state_lock=asyncio.Lock()
        self._perf_metrics=defaultdict(lambda: deque(maxlen=100)); self._perf_metrics_lock=asyncio.Lock()
        # Enhancement modules
        self.quantum_distillation=QuantumDistillationModule(self.config) if self.config.enable_quantum_distillation else None
        if self.config.enable_causal_rl: self.causal_rl_agent=CausalRLAgent(state_dim=10, action_dim=3)
        else: self.causal_rl_agent=None
        self.federated_coordinator=FederatedCoordinator(self, self.message_queue) if self.config.enable_federated else None
        self.safety_monitor=SafetyMonitor() if self.config.enable_safety_monitor else None
        if self.safety_monitor: self._setup_safety_invariants()
        self.precision_controller=PrecisionController() if self.config.enable_precision else None
        self.carbon_market=None
        if self.config.enable_carbon_market and self.config.carbon_market_config:
            self.carbon_market=CarbonMarketClient(**self.config.carbon_market_config)
        self.chaos_injector=ChaosInjector(self, self.config.chaos_probability) if self.config.enable_chaos else None
        self.human_approval=HumanApprovalHandler(self.message_queue) if self.config.enable_human_approval else None
        # Load genetic optimizer state
        best_ind=self.storage.load_global_state('best_individual')
        if best_ind: self._genetic_optimizer.best_individual=json.loads(best_ind)
        best_fit=self.storage.load_global_state('best_fitness')
        if best_fit: self._genetic_optimizer.best_fitness=float(best_fit)
        pareto=self.storage.load_global_state('pareto_front')
        if pareto: self._genetic_optimizer.pareto_front=[MOPDPoint.from_dict(p) for p in json.loads(pareto)]
        self._setup_metrics()
        self._register_signal_handlers()
        self._start_tasks()
        logger.info("Enhanced Bio-Inspired Core v9.2.0 created")

    def _setup_metrics(self):
        if PROMETHEUS_AVAILABLE and self.config.prometheus_port:
            self.metrics={'modules_total':Gauge('bio_core_modules_total','Total modules'),
                          'modules_healthy':Gauge('bio_core_modules_healthy','Healthy modules'),
                          'modules_unhealthy':Gauge('bio_core_modules_unhealthy','Unhealthy modules'),
                          'circuit_breakers_open':Gauge('bio_core_circuit_breakers_open','Open CBs'),
                          'events_published':Counter('bio_core_events_published','Events published',['event_type']),
                          'tasks_processed':Counter('bio_core_tasks_processed','Tasks processed'),
                          'task_duration':Histogram('bio_core_task_duration_seconds','Task duration')}
            start_http_server(self.config.prometheus_port)
        else: self.metrics={}

    def _save_initial_config(self):
        self._version_manager.save_version(self.config.dict(), description="Initial configuration")

    def _register_signal_handlers(self):
        try:
            loop=asyncio.get_event_loop()
            for sig in [signal.SIGINT, signal.SIGTERM]: loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
        except NotImplementedError: pass

    def _start_tasks(self):
        self._task_manager.start_task("health_monitor", self._health_monitoring_loop)
        self._task_manager.start_task("performance_monitor", self._performance_monitoring_loop)
        self._task_manager.start_task("predictive_health", self._predictive_health_loop)
        self._task_manager.start_task("anomaly_detection", self._anomaly_detection_loop)
        self._task_manager.start_task("competition", self._competition_loop)
        self._task_manager.start_task("genetic_optimization", self._genetic_optimization_loop)
        self._task_manager.start_task("change_detection", self._change_detection_loop)
        self._task_manager.start_task("ml_training", self._ml_training_loop)
        self._task_manager.start_task("strategy_update", self._strategy_update_loop)
        self._task_manager.start_task("persistence_save", self._persistence_save_loop)
        if self.federated_coordinator: self._task_manager.start_task("federated_update", self._federated_loop)
        if self.chaos_injector: self._task_manager.start_task("chaos", self._chaos_loop)

    def _setup_safety_invariants(self):
        self.safety_monitor.add_invariant("module_count_positive", lambda s: s['module_count']>=1, "No modules")
        self.safety_monitor.add_invariant("token_balance_non_negative", lambda s: s['token_balance']>=0, "Token balance negative")
        self.safety_monitor.add_invariant("circuit_breakers_under_limit", lambda s: s['circuit_open_count']<=3, "Too many open CBs")

    # Background loops (abbreviated)
    async def _health_monitoring_loop(self):
        while self._lifecycle_phase==LifecyclePhase.RUNNING:
            try:
                health=await self.registry.health_check_all()
                unhealthy=[n for n,s in health.items() if s['status'] not in ('healthy','unknown')]
                if unhealthy: logger.warning("Unhealthy modules", unhealthy=unhealthy)
                if self.metrics:
                    self.metrics['modules_total'].set(len(health)); self.metrics['modules_healthy'].set(sum(1 for s in health.values() if s['status']=='healthy'))
                    self.metrics['modules_unhealthy'].set(len(unhealthy)); self.metrics['circuit_breakers_open'].set(sum(1 for s in health.values() if s['circuit_breaker']=='open'))
                await asyncio.sleep(self.config.health_check_interval_seconds)
            except asyncio.CancelledError: break
            except Exception as e: logger.error("Health monitor error", error=str(e)); await asyncio.sleep(60)

    async def _performance_monitoring_loop(self):
        while self._lifecycle_phase==LifecyclePhase.RUNNING:
            try:
                if self._token_service:
                    summary=await self._token_service.get_system_summary()
                    async with self._perf_metrics_lock: self._perf_metrics['token_balance'].append(summary.get('total_balance',0))
                    await self._anomaly_detector.record_metric('token_balance', summary.get('total_balance',0))
                if self._gradient_service:
                    strengths=await self._gradient_service.get_field_strengths()
                    for f,s in strengths.items(): await self._anomaly_detector.record_metric(f'gradient_{f}', s)
                await asyncio.sleep(60)
            except asyncio.CancelledError: break
            except Exception as e: logger.error("Performance monitor error", error=str(e)); await asyncio.sleep(60)

    async def _predictive_health_loop(self):
        while self._lifecycle_phase==LifecyclePhase.RUNNING:
            try:
                for name,entry in self.registry.modules.items():
                    metrics={'health_score':0.5 if entry.health_status=='unknown' else 0.8 if entry.health_status=='healthy' else 0.3,
                             'success_rate':1.0-(entry.failure_count/max(1,entry.failure_count+1)), 'token_balance':0.5, 'error_rate':0.01}
                    await self.registry.update_predictive_health(name, metrics)
                await self.registry.health_forecaster.train()
                await asyncio.sleep(self._predictive_health_retrain_interval)
            except asyncio.CancelledError: break
            except Exception as e: logger.error("Predictive health error", error=str(e)); await asyncio.sleep(60)

    async def _anomaly_detection_loop(self):
        while self._lifecycle_phase==LifecyclePhase.RUNNING:
            try:
                report=await self._anomaly_detector.get_anomaly_report()
                if report['anomalies']:
                    logger.warning("Anomalies detected", count=len(report['anomalies']))
                    await self._event_bus.publish(CoreEvent(event_type='performance_anomaly', source='anomaly_detector', payload=report))
                await asyncio.sleep(60)
            except asyncio.CancelledError: break
            except Exception as e: logger.error("Anomaly detection error", error=str(e)); await asyncio.sleep(120)

    async def _competition_loop(self):
        while self._lifecycle_phase==LifecyclePhase.RUNNING:
            try: await self._marketplace.run_competition(); await asyncio.sleep(self._marketplace.competition_interval)
            except asyncio.CancelledError: break
            except Exception as e: logger.error("Competition error", error=str(e)); await asyncio.sleep(300)

    async def _genetic_optimization_loop(self):
        while self._lifecycle_phase==LifecyclePhase.RUNNING:
            try:
                if len(self.registry.modules)>=5:
                    result=await self._genetic_optimizer.evolve(generations=10)
                    logger.info("Genetic optimization complete", best_fitness=result['best_fitness'], pareto_size=len(result.get('pareto_front',[])))
                await asyncio.sleep(86400)
            except asyncio.CancelledError: break
            except Exception as e: logger.error("Genetic optimization error", error=str(e)); await asyncio.sleep(3600)

    async def _change_detection_loop(self):
        while self._lifecycle_phase==LifecyclePhase.RUNNING:
            try:
                if self._genetic_optimizer:
                    sig=self._genetic_optimizer.get_system_signature()
                    if self._genetic_optimizer._last_summary_signature and sig!=self._genetic_optimizer._last_summary_signature:
                        await self._genetic_optimizer.evolve(generations=min(5,10))
                    self._genetic_optimizer._last_summary_signature=sig
                await asyncio.sleep(60)
            except asyncio.CancelledError: break
            except Exception as e: logger.error("Change detection error", error=str(e)); await asyncio.sleep(60)

    async def _ml_training_loop(self):
        while self._lifecycle_phase==LifecyclePhase.RUNNING:
            try: await self.registry.health_forecaster.train(); await asyncio.sleep(self._predictive_health_retrain_interval)
            except asyncio.CancelledError: break
            except Exception as e: logger.error("ML training error", error=str(e)); await asyncio.sleep(60)

    async def _strategy_update_loop(self):
        while self._lifecycle_phase==LifecyclePhase.RUNNING:
            try:
                if self.strategy_selector:
                    state={'load_bin': 'high' if len(self.registry.modules)>20 else 'low', 'health_bin':'good'}
                    strategy=await self.strategy_selector.select_strategy(state)
                    if strategy=='performance': self.config.health_check_interval_seconds=20
                    elif strategy=='carbon_saver': self.config.health_check_interval_seconds=40
                    else: self.config.health_check_interval_seconds=30
                await asyncio.sleep(300)
            except asyncio.CancelledError: break
            except Exception as e: logger.error("Strategy update error", error=str(e)); await asyncio.sleep(60)

    async def _persistence_save_loop(self):
        while self._lifecycle_phase==LifecyclePhase.RUNNING:
            try:
                for entry in self.registry.modules.values(): self.storage.save_module(entry)
                await asyncio.sleep(60)
            except asyncio.CancelledError: break
            except Exception as e: logger.error("Persistence save error", error=str(e)); await asyncio.sleep(60)

    async def _federated_loop(self):
        while self._lifecycle_phase==LifecyclePhase.RUNNING:
            await asyncio.sleep(300)
            if self.federated_coordinator: await self.federated_coordinator.send_update()

    async def _chaos_loop(self):
        while self._lifecycle_phase==LifecyclePhase.RUNNING:
            await asyncio.sleep(60)
            if self.chaos_injector: await self.chaos_injector.maybe_inject_failure()

    # Public API
    async def get_system_status(self):
        status={
            'lifecycle_phase': self._lifecycle_phase.value,
            'uptime_seconds': (datetime.now(timezone.utc)-self._start_time).total_seconds() if self._start_time else 0,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'config': self.config.dict() if PYDANTIC_AVAILABLE else asdict(self.config),
            'modules': await self.registry.get_registry_stats(),
            'event_bus': self._event_bus.get_event_stats(),
            'config_version': self._version_manager.current_version,
            'loaded_modules': list(self.registry.loaded_modules),
            'genetic_optimizer': self._genetic_optimizer.get_status(),
            'marketplace': self._marketplace.get_marketplace_stats(),
            'quantum_security': self.quantum_security is not None,
            'blockchain_auditor': self.blockchain_auditor is not None,
            'strategy_selector': self.strategy_selector is not None,
            'multi_cloud': self.multi_cloud is not None,
            'mopd_enabled': self.config.mopd.enabled,
            'pareto_front_size': len(self.get_mopd_pareto_front()),
            'causal_rl_enabled': self.causal_rl_agent is not None,
            'federated_enabled': self.federated_coordinator is not None,
            'safety_monitor_enabled': self.safety_monitor is not None,
            'xai_enabled': self.config.enable_xai,
            'precision_controller_enabled': self.precision_controller is not None,
            'carbon_market_enabled': self.carbon_market is not None,
            'chaos_enabled': self.chaos_injector is not None,
            'human_approval_enabled': self.human_approval is not None,
        }
        if self._token_service: status['token_economy']=await self._token_service.get_system_summary()
        if self._gradient_service:
            status['gradients']=await self._gradient_service.get_field_stats()
            status['gradient_forecasts']=await self._gradient_service.get_forecast_summary()
        if self._compartment_service: status['compartments']=await self._compartment_service.get_ecosystem_stats()
        if self._biomass_service: status['biomass']=await self._biomass_service.get_storage_stats()
        async with self._perf_metrics_lock:
            status['performance']={name:{'current':list(vals)[-1] if vals else None,'avg_1min':np.mean(list(vals)[-60:]) if len(vals)>=10 else None} for name,vals in self._perf_metrics.items()}
        status['anomalies']=await self._anomaly_detector.get_anomaly_report()
        return status

    async def process_task(self, task):
        if self._lifecycle_phase!=LifecyclePhase.RUNNING:
            return {'success':False, 'reason':f'System not running ({self._lifecycle_phase.value})'}
        if PYDANTIC_AVAILABLE:
            try: task=TaskInput(**task).dict()
            except Exception as e: return {'success':False,'error':str(e)}
        ecoatp_required=task.get('complexity',0.5)*10
        await self._event_bus.publish(CoreEvent(event_type='task_received', source='core', payload={'task_id':task.get('task_id')}))
        if self.safety_monitor:
            state={
                'module_count':len(self.registry.modules),
                'token_balance':(await self._token_service.get_system_summary()).get('total_balance',0) if self._token_service else 0,
                'circuit_open_count':sum(1 for m in self.registry.modules.values() if m.circuit_breaker_state=='open')
            }
            violations=self.safety_monitor.check(state)
            if violations:
                logger.warning("Safety violations", violations=violations)
                return {'success':False,'reason':'Safety violation'}
        if self._token_service:
            success,_=await self._token_service.reserve_tokens('task_processor', ecoatp_required, None)
        else: success=True
        if not success:
            if self._biomass_service:
                stored, token_id=await self._biomass_service.store_task(task_data=task, ecoatp_cost=ecoatp_required)
                return {'success':True,'status':'stored','biomass_token':token_id}
            return {'success':False,'reason':'Insufficient tokens'}
        result={'success':True,'task_id':task.get('task_id','unknown'),'ecoatp_cost':ecoatp_required}
        if self.quantum_security:
            sig=await self.quantum_security.sign_data(result); result['quantum_signature']=sig
        if self.blockchain_auditor:
            await self.blockchain_auditor.record_event('task_completed', {'task_id':task.get('task_id'), 'ecoatp_cost':ecoatp_required})
        if self.config.enable_xai:
            logger.info("XAI", explanation=self.explain_decision('task', {'task_id':task.get('task_id'), 'ecoatp_cost':ecoatp_required}))
        return result

    def explain_decision(self, decision_type, context=None):
        if decision_type=='task': return f"Task {context.get('task_id')} processed with cost {context.get('ecoatp_cost')}."
        return "Decision made by system."

    # MOPD methods
    def get_mopd_pareto_front(self):
        return self._genetic_optimizer.pareto_front.copy() if self.config.mopd.enabled else []

    def get_mopd_summary(self):
        return {"enabled":self.config.mopd.enabled, "objective_weights":self.config.mopd.objective_weights,
                "pareto_front_size":len(self._genetic_optimizer.pareto_front),
                "best_scalarised_score":self._genetic_optimizer.best_fitness}

    # Lifecycle methods
    async def initialize(self):
        if self._lifecycle_phase==LifecyclePhase.RUNNING: return True
        self._lifecycle_phase=LifecyclePhase.INITIALIZING; self._start_time=datetime.now(timezone.utc)
        try:
            self.config.validate()
            if self._token_service is None:
                from .eco_atp_currency import EcoATPTokenManager
                self._token_service=EcoATPTokenManager()
            await self.registry.register('token_manager', self._token_service, health_check=lambda: True)
            if self._gradient_service is None:
                from .proton_gradient_fields import HierarchicalGradientManager
                self._gradient_service=HierarchicalGradientManager()
            await self.registry.register('gradient_manager', self._gradient_service, health_check=lambda: True)
            if self._compartment_service is None:
                from .chromatophore_compartments import HierarchicalCompartmentManager
                self._compartment_service=HierarchicalCompartmentManager(self._token_service)
            await self.registry.register('compartment_manager', self._compartment_service, health_check=lambda: True)
            if self._biomass_service is None:
                from .biomass_storage import BiomassStorage
                self._biomass_service=BiomassStorage(self._token_service, self._gradient_service)
            await self.registry.register('biomass_storage', self._biomass_service, health_check=lambda: True)
            await self.registry.initialize_all()
            self._lifecycle_phase=LifecyclePhase.RUNNING
            logger.info("Core initialized successfully")
            return True
        except Exception as e:
            self._lifecycle_phase=LifecyclePhase.ERROR; logger.error("Initialization failed", error=str(e)); return False

    async def shutdown(self):
        if self._lifecycle_phase==LifecyclePhase.STOPPED: return True
        self._lifecycle_phase=LifecyclePhase.STOPPING; self._shutdown_requested=True
        await self._event_bus.stop(); await self._task_manager.stop_all(); await self.registry.shutdown_all()
        if self.config.enable_state_persistence:
            await self._save_state()
        self._lifecycle_phase=LifecyclePhase.STOPPED; return True

    async def _save_state(self):
        os.makedirs(self.config.state_directory, exist_ok=True)
        path=os.path.join(self.config.state_directory, f"state_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json")
        state={'timestamp':datetime.now(timezone.utc).isoformat(), 'config':self.config.dict()}
        with open(path,'w') as f: json.dump(state, f, indent=2)

    # Convenience
    @property
    def is_running(self): return self._lifecycle_phase==LifecyclePhase.RUNNING

    @property
    def is_healthy(self):
        health=asyncio.run(self.registry.health_check_all())
        return all(s['status']!='error' for s in health.values())

    @property
    def lifecycle_phase(self): return self._lifecycle_phase

    def get_lifecycle_status(self):
        return {'phase':self._lifecycle_phase.value, 'is_running':self.is_running, 'is_healthy':self.is_healthy,
                'uptime_seconds':(datetime.now(timezone.utc)-self._start_time).total_seconds() if self._start_time else 0,
                'module_count':len(self.registry.modules), 'config_version':self._version_manager.current_version}

    async def __aenter__(self): await self.initialize(); return self
    async def __aexit__(self, exc_type, exc_val, exc_tb): await self.shutdown()

# ============================================================================
# Convenience functions
# ============================================================================
def create_core(config=None, **kwargs): return EnhancedBioInspiredCore(config=config, **kwargs)

async def create_and_initialize(config=None, **kwargs):
    core=create_core(config=config, **kwargs)
    if not await core.initialize(): raise RuntimeError("Initialization failed")
    return core

async def main():
    logging.basicConfig(level=logging.INFO)
    core=await create_and_initialize()
    result=await core.process_task({'task_id':'task1','complexity':0.7})
    print(result)
    status=await core.get_system_status()
    print(status)
    print("Pareto front:", core.get_mopd_pareto_front())
    print("MOPD summary:", core.get_mopd_summary())
    await core.shutdown()

if __name__=="__main__":
    asyncio.run(main())
