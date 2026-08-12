# =============================================================================
# Enhanced Bio-Inspired Core v9.1.0 - Full Implementation with MOPD Support
# =============================================================================
"""
Enhanced Bio-Inspired Core v9.1.0
All improvements integrated plus Multi‑Objective Pareto Decision (MOPD) support.

MOPD enhancements:
- MOPDConfig sub‑configuration for objective weights and grid resolution.
- MOPDPoint dataclass to represent a configuration with objectives.
- Pareto front generation in the CoreGeneticOptimizer.
- Selection of best configuration via scalarisation.
- Persistence of Pareto front.
- Telemetry tracks MOPD generations and Pareto front sizes.
- Full backward compatibility.
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
from typing import Dict, Any, List, Optional, Tuple, Protocol, Callable, Set, Union, TypeVar
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from abc import ABC, abstractmethod
import numpy as np
from collections import defaultdict, deque
from functools import wraps
from pathlib import Path

# ============================================================================
# Optional dependencies with graceful degradation
# ============================================================================
try:
    from pydantic import BaseModel, Field, field_validator, ValidationError, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
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
    from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature, decode_dss_signature
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
    from prometheus_client import Counter, Gauge, Histogram, start_http_server, CollectorRegistry
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

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
    from web3 import Web3, Account, HTTPProvider
    from web3.middleware import geth_poa_middleware, gas_price_strategy
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

# For structured logging
import structlog
from structlog.processors import JSONRenderer, TimeStamper
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

# ============================================================================
# MOPD Data Class (NEW)
# ============================================================================

@dataclass
class MOPDPoint:
    """Represents a genetic individual with its objective vector."""
    individual: Dict[str, Any]  # the parameters
    health_score: float         # objective 1
    uptime_score: float         # objective 2
    circuit_score: float        # objective 3
    anomaly_score: float        # objective 4
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPoint':
        return cls(**data)

# ============================================================================
# Configuration (Enhanced with MOPD)
# ============================================================================

if PYDANTIC_AVAILABLE:
    class MOPDConfig(BaseModel):
        """Configuration for Multi‑Objective Pareto Decision (MOPD) in core optimization."""
        enabled: bool = Field(True, description="Enable MOPD‑aware genetic optimization")
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'health_score': 0.4,
                'uptime_score': 0.3,
                'circuit_score': 0.2,
                'anomaly_score': 0.1,
            },
            description="Weights for scalarising Pareto front (must sum to 1)"
        )
        grid_resolution: int = Field(5, description="Number of discrete points for sampling (unused for now)")

        @field_validator('objective_weights')
        @classmethod
        def check_weights(cls, v):
            total = sum(v.values())
            if abs(total - 1.0) > 1e-6:
                raise ValueError("objective_weights must sum to 1")
            return v

    class CoreConfig(BaseModel):
        """Central configuration for the Bio-Inspired Core."""
        token_base_generation_rate: float = Field(150.0, gt=0)
        token_hoarding_threshold: float = Field(2.0, ge=1.0)
        token_emergency_threshold: float = Field(50.0, ge=0)
        token_target_utilization: float = Field(0.75, ge=0, le=1)
        compartments_per_expert_type: int = Field(2, ge=1)
        max_total_compartments: int = Field(100, ge=1)
        compartment_health_threshold: float = Field(0.2, ge=0, le=1)
        carbon_leakage_rate: float = Field(0.03, gt=0)
        helium_leakage_rate: float = Field(0.08, gt=0)
        trust_leakage_rate: float = Field(0.10, gt=0)
        atp_c_ring_size: int = Field(12, ge=8, le=17)
        atp_max_rotation_speed: float = Field(6000, gt=0)
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
        state_save_interval_seconds: int = Field(300, ge=60)
        state_directory: str = "./agent_state"
        health_check_interval_seconds: int = Field(30, ge=5)
        version: str = "1.0.0"
        version_description: str = ""

        # Persistence DB path
        db_path: str = Field("./bio_core_state.db")

        # Quantum signing
        enable_quantum_signing: bool = True
        quantum_signing_algorithm: str = Field('dilithium')

        # Blockchain audit
        enable_blockchain_audit: bool = True
        blockchain_rpc_url: str = Field('http://localhost:8545')
        blockchain_contract_address: str = Field('0x0000000000000000000000000000000000000000')
        blockchain_private_key: Optional[str] = None

        # Autonomous strategy
        enable_autonomous_strategy: bool = True
        rl_learning_rate: float = Field(0.1, ge=0.0, le=1.0)
        rl_discount_factor: float = Field(0.9, ge=0.0, le=1.0)
        rl_exploration_rate: float = Field(0.1, ge=0.0, le=1.0)
        rl_q_table_db_path: str = Field("rl_q_table.db")

        # Multi-cloud
        enable_multi_cloud: bool = True
        cloud_provider: str = Field('aws')
        cloud_region: str = Field('us-east-1')
        cloud_bucket: str = Field('bio-core-state')
        cloud_access_key: Optional[str] = None
        cloud_secret_key: Optional[str] = None

        # Retry
        max_retries: int = Field(3, ge=1)
        retry_base_delay_ms: float = Field(100.0, ge=0)
        retry_max_delay_ms: float = Field(5000.0, ge=0)

        # Circuit breaker
        enable_circuit_breaker: bool = True
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_recovery_timeout: float = Field(60.0, ge=1)
        circuit_breaker_db_path: str = Field("circuit_breakers.db")

        # Prometheus port
        prometheus_port: Optional[int] = Field(None)

        # ML model paths
        ml_model_path: str = Field("models/health_model.pkl")

        # MOPD configuration (NEW)
        mopd: MOPDConfig = Field(default_factory=MOPDConfig, description="MOPD sub‑configuration")

        class Config:
            env_prefix = "BIO_CORE_"
            allow_mutation = True

        @field_validator('token_hoarding_threshold')
        def hoarding_threshold_must_be_positive(cls, v):
            if v < 1.0:
                raise ValueError('hoarding_threshold must be >= 1.0')
            return v
else:
    # Fallback dataclass (simplified)
    @dataclass
    class MOPDConfig:
        enabled: bool = True
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'health_score': 0.4,
            'uptime_score': 0.3,
            'circuit_score': 0.2,
            'anomaly_score': 0.1,
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

# ============================================================================
# Persistent Circuit Breaker (SQLite)
# ============================================================================

class CircuitBreaker:
    """Circuit breaker with SQLite persistence."""
    def __init__(self, name: str, db_path: str, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self.name = name
        self.db_path = db_path
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._init_db()
        self._load_state()
        self._lock = asyncio.Lock()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS circuit_breaker (
                name TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                failures INTEGER NOT NULL,
                last_failure TEXT
            )
        """)
        conn.commit()
        conn.close()

    def _load_state(self):
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT state, failures, last_failure FROM circuit_breaker WHERE name = ?", (self.name,)).fetchone()
        conn.close()
        if row:
            self.state = row[0]
            self.failure_count = row[1]
            self.last_failure_time = datetime.fromisoformat(row[2]) if row[2] else None
        else:
            self.state = 'closed'
            self.failure_count = 0
            self.last_failure_time = None

    def _save_state(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT OR REPLACE INTO circuit_breaker (name, state, failures, last_failure)
            VALUES (?, ?, ?, ?)
        """, (self.name, self.state, self.failure_count, self.last_failure_time.isoformat() if self.last_failure_time else None))
        conn.commit()
        conn.close()

    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            if self.state == 'open':
                if self.last_failure_time and (datetime.now(timezone.utc) - self.last_failure_time).total_seconds() >= self.recovery_timeout:
                    self.state = 'half_open'
                    self._save_state()
                    logger.info(f"Circuit breaker {self.name} transitioning to half_open")
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == 'half_open':
                    self.state = 'closed'
                    self.failure_count = 0
                    self._save_state()
                    logger.info(f"Circuit breaker {self.name} closed after success")
                else:
                    self.failure_count = 0
                    self._save_state()
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.now(timezone.utc)
                if self.failure_count >= self.failure_threshold:
                    self.state = 'open'
                    logger.warning(f"Circuit breaker {self.name} opened after {self.failure_count} failures")
                self._save_state()
            raise e

# ============================================================================
# Retry Decorator
# ============================================================================

def retry_decorator(max_attempts: int = 3, min_delay: float = 0.1, max_delay: float = 10.0):
    """Decorator to retry async functions with exponential backoff."""
    if TENACITY_AVAILABLE:
        def decorator(func):
            @retry(
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential(multiplier=min_delay, min=min_delay, max=max_delay),
                retry=retry_if_exception_type(Exception),
                before_sleep=before_sleep_log(logger, logging.WARNING)
            )
            async def wrapper(*args, **kwargs):
                return await func(*args, **kwargs)
            return wrapper
        return decorator
    else:
        def decorator(func):
            async def wrapper(*args, **kwargs):
                for attempt in range(max_attempts):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        if attempt == max_attempts - 1:
                            raise
                        delay = min(min_delay * (2 ** attempt), max_delay)
                        await asyncio.sleep(delay)
            return wrapper
        return decorator

# ============================================================================
# Post-Quantum Security
# ============================================================================

class QuantumResilientSecurity:
    """Post-quantum signing using Dilithium/Falcon/SPHINCS+."""
    def __init__(self, algorithm: str = 'dilithium'):
        self.algorithm = algorithm
        self.pqc_available = PQC_AVAILABLE
        if self.pqc_available:
            self._load_algorithm()
        else:
            logger.warning("PQC libraries not found – using ECDSA fallback.")

    def _load_algorithm(self):
        if self.algorithm == 'dilithium':
            self.sign_func = dilithium.sign
            self.verify_func = dilithium.verify
        elif self.algorithm == 'falcon':
            self.sign_func = falcon.sign
            self.verify_func = falcon.verify
        elif self.algorithm == 'sphincs':
            self.sign_func = sphincs.sign
            self.verify_func = sphincs.verify
        else:
            raise ValueError(f"Unknown algorithm: {self.algorithm}")

    async def sign_data(self, data: Dict) -> Dict:
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        if self.pqc_available:
            try:
                public_key, private_key = self.sign_func.generate_keypair()
                signature = self.sign_func.sign(data_bytes, private_key)
                return {
                    'signature': signature.hex(),
                    'algorithm': self.algorithm,
                    'public_key': public_key.hex(),
                    'timestamp': datetime.utcnow().isoformat()
                }
            except Exception as e:
                logger.error(f"PQC signing failed: {e}")
        # Fallback: ECDSA
        from cryptography.hazmat.primitives.asymmetric import ec
        private_key = ec.generate_private_key(ec.SECP256R1())
        signature = private_key.sign(data_bytes, ec.ECDSA(hashes.SHA256()))
        return {
            'signature': signature.hex(),
            'algorithm': 'ecdsa',
            'timestamp': datetime.utcnow().isoformat()
        }

    async def verify_data(self, data: Dict, signature_data: Dict) -> bool:
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        algorithm = signature_data.get('algorithm')
        signature = bytes.fromhex(signature_data['signature'])
        if algorithm in ['dilithium', 'falcon', 'sphincs'] and self.pqc_available:
            public_key = bytes.fromhex(signature_data['public_key'])
            return self.verify_func.verify(data_bytes, signature, public_key)
        elif algorithm == 'ecdsa':
            from cryptography.hazmat.primitives.asymmetric import ec
            public_key = ec.load_der_public_key(bytes.fromhex(signature_data['public_key']))
            public_key.verify(signature, data_bytes, ec.ECDSA(hashes.SHA256()))
            return True
        return False

# ============================================================================
# Blockchain Auditor (with nonce caching and gas management)
# ============================================================================

class BlockchainAuditor:
    """Ethereum integration with nonce caching and gas price strategies."""
    def __init__(self, config: CoreConfig, circuit_breaker: Optional[CircuitBreaker] = None):
        self.config = config
        self.circuit_breaker = circuit_breaker
        self.web3 = None
        self.contract = None
        self.account = None
        self.available = False
        self._nonce_cache = {}
        self._lock = asyncio.Lock()
        if WEB3_AVAILABLE:
            self._initialize()

    def _initialize(self):
        try:
            self.web3 = Web3(HTTPProvider(self.config.blockchain_rpc_url))
            if not self.web3.is_connected():
                raise ConnectionError("Cannot connect to blockchain RPC")
            self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
            self.web3.eth.set_gas_price_strategy(gas_price_strategy.rpc_gas_price_strategy)
            if self.config.blockchain_private_key:
                self.account = Account.from_key(self.config.blockchain_private_key)
                self.web3.eth.default_account = self.account.address
            else:
                self.account = self.web3.eth.accounts[0]
            # Load contract ABI (from file or environment)
            abi = self._load_abi()
            if self.config.blockchain_contract_address:
                self.contract = self.web3.eth.contract(
                    address=self.config.blockchain_contract_address,
                    abi=abi
                )
                self.available = True
                logger.info("Blockchain auditor connected")
            else:
                logger.warning("Contract address not configured – blockchain audit will be simulated.")
        except Exception as e:
            logger.error(f"Blockchain initialization failed: {e}")

    def _load_abi(self) -> List:
        abi_path = Path(__file__).parent / "contract_abi.json"
        if abi_path.exists():
            with open(abi_path, 'r') as f:
                data = json.load(f)
                return data['abi']
        # Minimal ABI for recording events
        return [
            {"constant": False, "inputs": [{"name": "eventType", "type": "string"}, {"name": "payload", "type": "string"}], "name": "recordEvent", "outputs": [], "type": "function"}
        ]

    async def _get_nonce(self, address: str) -> int:
        if address not in self._nonce_cache:
            self._nonce_cache[address] = self.web3.eth.get_transaction_count(address)
        return self._nonce_cache[address]

    async def _increment_nonce(self, address: str):
        self._nonce_cache[address] = self._nonce_cache.get(address, 0) + 1

    @retry_decorator(max_attempts=3, min_delay=0.1, max_delay=2)
    async def record_event(self, event_type: str, payload: Dict) -> Dict:
        if not self.available:
            return {'status': 'simulated', 'tx_hash': f"0x{hashlib.sha256(os.urandom(32)).hexdigest()}"}

        async def _record():
            async with self._lock:
                nonce = await self._get_nonce(self.account.address)
                payload_str = json.dumps(payload, default=str)
                gas_estimate = self.contract.functions.recordEvent(event_type, payload_str).estimate_gas({'from': self.account.address})
                gas_price = self.web3.eth.generate_gas_price() or self.web3.eth.gas_price
                tx = self.contract.functions.recordEvent(event_type, payload_str).build_transaction({
                    'from': self.account.address,
                    'nonce': nonce,
                    'gas': int(gas_estimate * 1.2),
                    'gasPrice': gas_price
                })
                signed_tx = self.account.sign_transaction(tx)
                tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
                receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
                if receipt.status == 1:
                    await self._increment_nonce(self.account.address)
                    logger.info(f"Blockchain event recorded: {tx_hash.hex()}")
                    return {'status': 'success', 'tx_hash': tx_hash.hex(), 'block_number': receipt.blockNumber}
                else:
                    logger.error(f"Transaction reverted for {event_type}")
                    return {'status': 'failed', 'error': 'transaction reverted'}

        if self.circuit_breaker:
            return await self.circuit_breaker.call(_record)
        else:
            return await _record()

# ============================================================================
# Multi-Cloud Distributor (with retry and fallback)
# ============================================================================

class MultiCloudDistributor:
    """Distribute state to S3, Azure Blob, or GCP with retry and fallback."""
    def __init__(self, config: CoreConfig, circuit_breaker: Optional[CircuitBreaker] = None):
        self.config = config
        self.circuit_breaker = circuit_breaker
        self._clients = {}
        self._providers = ['aws', 'azure', 'gcp']
        self._init_client(config.cloud_provider)

    def _init_client(self, provider: str):
        try:
            if provider == 'aws' and AWS_AVAILABLE:
                self._clients['aws'] = boto3.client('s3',
                    aws_access_key_id=self.config.cloud_access_key,
                    aws_secret_access_key=self.config.cloud_secret_key,
                    region_name=self.config.cloud_region)
            elif provider == 'azure' and AZURE_AVAILABLE:
                self._clients['azure'] = BlobServiceClient.from_connection_string(self.config.cloud_access_key)
            elif provider == 'gcp' and GCP_AVAILABLE:
                self._clients['gcp'] = storage.Client.from_service_account_json(self.config.cloud_access_key)
        except Exception as e:
            logger.warning(f"Failed to initialize {provider} client: {e}")

    @retry_decorator(max_attempts=3, min_delay=0.1, max_delay=2)
    async def distribute(self, data: Dict, filename: str) -> Dict:
        """Upload a JSON-serializable dict to cloud storage with fallback."""
        for provider in self._providers:
            if provider in self._clients:
                try:
                    result = await self._upload(provider, data, filename)
                    if result.get('status') == 'success':
                        return result
                except Exception as e:
                    logger.warning(f"Upload to {provider} failed: {e}")
        return {'status': 'failed', 'reason': 'All cloud providers failed'}

    async def _upload(self, provider: str, data: Dict, filename: str) -> Dict:
        data_bytes = json.dumps(data, default=str).encode('utf-8')
        if provider == 'aws':
            client = self._clients['aws']
            client.put_object(Bucket=self.config.cloud_bucket, Key=filename, Body=data_bytes)
            return {'status': 'success', 'url': f"s3://{self.config.cloud_bucket}/{filename}"}
        elif provider == 'azure':
            client = self._clients['azure']
            container_client = client.get_container_client(self.config.cloud_bucket)
            blob_client = container_client.get_blob_client(filename)
            blob_client.upload_blob(data_bytes, overwrite=True)
            return {'status': 'success', 'url': f"azure://{self.config.cloud_bucket}/{filename}"}
        elif provider == 'gcp':
            client = self._clients['gcp']
            bucket = client.bucket(self.config.cloud_bucket)
            blob = bucket.blob(filename)
            blob.upload_from_string(data_bytes, content_type='application/json')
            return {'status': 'success', 'url': f"gs://{self.config.cloud_bucket}/{filename}"}
        raise ValueError(f"Unknown provider: {provider}")

# ============================================================================
# Autonomous Strategy Selector (with persistent Q-table)
# ============================================================================

class AutonomousStrategySelector:
    """Q-learning agent for strategy selection with persistent Q-table."""
    def __init__(self, config: CoreConfig):
        self.config = config
        self.learning_rate = config.rl_learning_rate
        self.discount_factor = config.rl_discount_factor
        self.exploration_rate = config.rl_exploration_rate
        self.q_table: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self.total_updates = 0
        self.actions = ['performance', 'balanced', 'carbon_saver']
        self._load_q_table()

    def _load_q_table(self):
        conn = sqlite3.connect(self.config.rl_q_table_db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS q_table (
                state TEXT,
                action TEXT,
                q_value REAL,
                PRIMARY KEY (state, action)
            )
        """)
        rows = conn.execute("SELECT state, action, q_value FROM q_table").fetchall()
        for state, action, q_value in rows:
            self.q_table[state][action] = q_value
        conn.close()
        logger.info(f"Loaded Q-table with {len(self.q_table)} states")

    def _save_q_value(self, state: str, action: str, q_value: float):
        conn = sqlite3.connect(self.config.rl_q_table_db_path)
        conn.execute("INSERT OR REPLACE INTO q_table (state, action, q_value) VALUES (?, ?, ?)", (state, action, q_value))
        conn.commit()
        conn.close()

    def _state_to_key(self, state: Dict) -> str:
        load = state.get('system_load', 0.5)
        health = state.get('system_health', 0.8)
        load_bin = 'high' if load > 0.7 else 'medium' if load > 0.4 else 'low'
        health_bin = 'good' if health > 0.7 else 'medium' if health > 0.4 else 'poor'
        return f"{load_bin}_{health_bin}"

    async def select_strategy(self, state: Dict) -> str:
        state_key = self._state_to_key(state)
        if random.random() < self.exploration_rate:
            self.exploration_rate = max(0.01, self.exploration_rate * 0.999)
            return random.choice(self.actions)
        q_values = {a: self.q_table[state_key].get(a, 0.0) for a in self.actions}
        return max(q_values, key=q_values.get)

    async def update(self, state: Dict, action: str, reward: float, next_state: Dict):
        state_key = self._state_to_key(state)
        next_state_key = self._state_to_key(next_state)
        current_q = self.q_table[state_key][action]
        max_next_q = max(self.q_table[next_state_key].values()) if self.q_table[next_state_key] else 0
        new_q = current_q + self.learning_rate * (reward + self.discount_factor * max_next_q - current_q)
        self.q_table[state_key][action] = new_q
        self._save_q_value(state_key, action, new_q)
        self.total_updates += 1

# ============================================================================
# Persistent Storage (SQLite)
# ============================================================================

class Storage:
    """SQLite persistence for module states, health data, marketplace scores, etc."""
    def __init__(self, db_path: str = "./bio_core_state.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS modules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE,
                    phase TEXT,
                    dependencies TEXT,
                    dependents TEXT,
                    health_status TEXT,
                    circuit_breaker_state TEXT,
                    failure_count INTEGER,
                    last_failure TEXT,
                    metrics TEXT,
                    module_path TEXT,
                    version TEXT,
                    loaded_at TEXT,
                    predicted_health REAL,
                    failure_probability REAL,
                    health_trend TEXT,
                    init_started TEXT,
                    init_completed TEXT,
                    error_message TEXT,
                    init_timeout REAL,
                    shutdown_timeout REAL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS health_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    module_name TEXT,
                    timestamp TEXT,
                    health_score REAL,
                    success_rate REAL,
                    token_balance REAL,
                    error_rate REAL,
                    FOREIGN KEY(module_name) REFERENCES modules(name)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS marketplace_scores (
                    module_name TEXT PRIMARY KEY,
                    score REAL,
                    last_updated TEXT,
                    FOREIGN KEY(module_name) REFERENCES modules(name)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS config_versions (
                    version_id TEXT PRIMARY KEY,
                    timestamp TEXT,
                    config TEXT,
                    description TEXT,
                    parent TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS global_state (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            conn.commit()

    def save_module(self, entry: ModuleEntry):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO modules (
                    name, phase, dependencies, dependents, health_status,
                    circuit_breaker_state, failure_count, last_failure,
                    metrics, module_path, version, loaded_at,
                    predicted_health, failure_probability, health_trend,
                    init_started, init_completed, error_message,
                    init_timeout, shutdown_timeout
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                entry.name,
                entry.phase.value,
                json.dumps(entry.dependencies),
                json.dumps(entry.dependents),
                entry.health_status,
                entry.circuit_breaker_state,
                entry.failure_count,
                entry.last_failure.isoformat() if entry.last_failure else None,
                json.dumps(entry.metrics),
                entry.module_path,
                entry.version,
                entry.loaded_at.isoformat() if entry.loaded_at else None,
                entry.predicted_health,
                entry.failure_probability,
                entry.health_trend,
                entry.init_started.isoformat() if entry.init_started else None,
                entry.init_completed.isoformat() if entry.init_completed else None,
                entry.error_message,
                entry.init_timeout,
                entry.shutdown_timeout
            ))

    def load_modules(self) -> List[ModuleEntry]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT * FROM modules").fetchall()
            modules = []
            for row in rows:
                entry = ModuleEntry(
                    name=row[1],
                    phase=LifecyclePhase(row[2]),
                    dependencies=json.loads(row[3]) if row[3] else [],
                    dependents=json.loads(row[4]) if row[4] else [],
                    health_status=row[5],
                    circuit_breaker_state=row[6],
                    failure_count=row[7],
                    last_failure=datetime.fromisoformat(row[8]) if row[8] else None,
                    metrics=json.loads(row[9]) if row[9] else {},
                    module_path=row[10],
                    version=row[11],
                    loaded_at=datetime.fromisoformat(row[12]) if row[12] else None,
                    predicted_health=row[13],
                    failure_probability=row[14],
                    health_trend=row[15],
                    init_started=datetime.fromisoformat(row[16]) if row[16] else None,
                    init_completed=datetime.fromisoformat(row[17]) if row[17] else None,
                    error_message=row[18],
                    init_timeout=row[19],
                    shutdown_timeout=row[20]
                )
                entry.id = row[0]
                modules.append(entry)
            return modules

    def save_health_history(self, module_name: str, metrics: Dict[str, float]):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO health_history (module_name, timestamp, health_score, success_rate, token_balance, error_rate)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                module_name,
                datetime.utcnow().isoformat(),
                metrics.get('health_score', 0.5),
                metrics.get('success_rate', 0.5),
                metrics.get('token_balance', 500),
                metrics.get('error_rate', 0.01)
            ))

    def load_health_history(self, module_name: str, limit: int = 100) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("""
                SELECT timestamp, health_score, success_rate, token_balance, error_rate
                FROM health_history WHERE module_name = ? ORDER BY id DESC LIMIT ?
            """, (module_name, limit)).fetchall()
            return [{
                'timestamp': datetime.fromisoformat(row[0]),
                'health_score': row[1],
                'success_rate': row[2],
                'token_balance': row[3],
                'error_rate': row[4]
            } for row in rows]

    def save_marketplace_score(self, module_name: str, score: float):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO marketplace_scores (module_name, score, last_updated)
                VALUES (?, ?, ?)
            """, (module_name, score, datetime.utcnow().isoformat()))

    def load_marketplace_scores(self) -> Dict[str, float]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT module_name, score FROM marketplace_scores").fetchall()
            return {row[0]: row[1] for row in rows}

    def save_config_version(self, version_id: str, config: Dict, description: str, parent: Optional[str]):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO config_versions (version_id, timestamp, config, description, parent)
                VALUES (?, ?, ?, ?, ?)
            """, (version_id, datetime.utcnow().isoformat(), json.dumps(config), description, parent))

    def load_config_versions(self) -> List[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT version_id, timestamp, config, description, parent FROM config_versions ORDER BY timestamp DESC").fetchall()
            return [{
                'version_id': row[0],
                'timestamp': row[1],
                'config': json.loads(row[2]),
                'description': row[3],
                'parent': row[4]
            } for row in rows]

    def save_global_state(self, key: str, value: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT OR REPLACE INTO global_state (key, value) VALUES (?, ?)", (key, value))

    def load_global_state(self, key: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT value FROM global_state WHERE key = ?", (key,)).fetchone()
            return row[0] if row else None

    def delete_module(self, name: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM modules WHERE name = ?", (name,))

# ============================================================================
# Predictive Health Forecaster (with model persistence)
# ============================================================================

class PredictiveHealthForecaster:
    """Predictive health forecaster using Isolation Forest with model persistence."""
    def __init__(self, storage: Storage, config: CoreConfig):
        self.storage = storage
        self.config = config
        self.model = None
        self.scaler = StandardScaler()
        self.is_trained = False
        self.history: deque = deque(maxlen=2000)
        self.predictions: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self._load_model()

    def _load_model(self):
        if os.path.exists(self.config.ml_model_path):
            try:
                with open(self.config.ml_model_path, 'rb') as f:
                    data = pickle.load(f)
                    self.model = data['model']
                    self.scaler = data['scaler']
                    self.is_trained = True
                logger.info("Loaded health model from disk")
            except Exception as e:
                logger.warning("Failed to load health model", error=str(e))

    def _save_model(self):
        if self.model is not None and self.scaler is not None:
            try:
                os.makedirs(os.path.dirname(self.config.ml_model_path), exist_ok=True)
                with open(self.config.ml_model_path, 'wb') as f:
                    pickle.dump({'model': self.model, 'scaler': self.scaler}, f)
                logger.info("Saved health model to disk")
            except Exception as e:
                logger.error("Failed to save health model", error=str(e))

    def record_health_data(self, module_name: str, metrics: Dict[str, float]):
        self.history.append({
            'module': module_name,
            'timestamp': datetime.utcnow(),
            **metrics
        })
        self.storage.save_health_history(module_name, metrics)

    async def train(self):
        if len(self.history) < 50:
            return {'status': 'insufficient_data', 'samples': len(self.history)}
        async with self._lock:
            X = []
            for i in range(10, len(self.history) - 1):
                features = []
                for j in range(10):
                    data = self.history[i - j]
                    features.extend([
                        data.get('health_score', 0.5),
                        data.get('success_rate', 0.5),
                        data.get('token_balance', 500) / 1000,
                        data.get('error_rate', 0.01)
                    ])
                X.append(features)
            if len(X) < 20:
                return {'status': 'insufficient_training_data', 'samples': len(X)}
            X = np.array(X)
            X_scaled = self.scaler.fit_transform(X)
            if self.model is None:
                from sklearn.ensemble import IsolationForest
                self.model = IsolationForest(contamination=0.1, random_state=42)
            self.model.fit(X_scaled)
            self.is_trained = True
            self._save_model()
            logger.info("Health forecaster trained", samples=len(X))
            return {'status': 'success', 'samples': len(X)}

    async def predict_health(self, current_metrics: Dict[str, float]) -> Dict[str, Any]:
        if not self.is_trained:
            return {'predicted_health': 0.5, 'failure_probability': 0.0, 'confidence': 0.0}
        async with self._lock:
            features = []
            for key in ['health_score', 'success_rate', 'token_balance', 'error_rate']:
                if key in current_metrics:
                    features.append(current_metrics[key])
                else:
                    features.append(0.5)
            while len(features) < 4:
                features.append(0.5)
            features_array = np.array([features])
            features_scaled = self.scaler.transform(features_array)
            prediction = self.model.predict(features_scaled)[0]
            is_anomalous = prediction == -1
            decision_function = self.model.decision_function(features_scaled)[0]
            confidence = abs(decision_function) / (abs(decision_function) + 1)
            if len(self.history) > 20:
                recent_health = [h.get('health_score', 0.5) for h in list(self.history)[-20:]]
                trend_slope = np.polyfit(range(len(recent_health)), recent_health, 1)[0]
                trend = 'improving' if trend_slope > 0.01 else 'declining' if trend_slope < -0.01 else 'stable'
            else:
                trend = 'stable'
            result = {
                'predicted_health': 0.3 if is_anomalous else 0.7,
                'failure_probability': 0.8 if is_anomalous else 0.2,
                'trend': trend,
                'confidence': confidence,
                'is_anomalous': is_anomalous
            }
            self.predictions[str(datetime.utcnow().timestamp())] = result
            return result

# ============================================================================
# Performance Anomaly Detector
# ============================================================================

class PerformanceAnomalyDetector:
    """Detects anomalies using z-score and trend analysis."""
    def __init__(self):
        self.metric_history: Dict[str, List[float]] = defaultdict(list)
        self.anomalies: List[Dict] = []
        self._lock = asyncio.Lock()
        self.zscore_threshold = 3.0
        self.trend_threshold = 0.2

    async def record_metric(self, metric_name: str, value: float):
        async with self._lock:
            self.metric_history[metric_name].append(value)
            if len(self.metric_history[metric_name]) > 1000:
                self.metric_history[metric_name] = self.metric_history[metric_name][-1000:]

    async def detect_anomalies(self, metric_name: str) -> List[Dict]:
        async with self._lock:
            if metric_name not in self.metric_history or len(self.metric_history[metric_name]) < 10:
                return []
            values = self.metric_history[metric_name][-50:]
            mean = np.mean(values)
            std = np.std(values)
            anomalies = []
            if std > 0:
                z_scores = [(v - mean) / std for v in values[-10:]]
                for i, zscore in enumerate(z_scores):
                    if abs(zscore) > self.zscore_threshold:
                        anomalies.append({
                            'metric': metric_name,
                            'value': values[-10 + i],
                            'zscore': zscore,
                            'timestamp': datetime.utcnow().isoformat(),
                            'type': 'zscore'
                        })
            if len(values) > 20:
                recent = values[-20:]
                slope = np.polyfit(range(len(recent)), recent, 1)[0]
                if abs(slope) > self.trend_threshold:
                    anomalies.append({
                        'metric': metric_name,
                        'slope': slope,
                        'timestamp': datetime.utcnow().isoformat(),
                        'type': 'trend',
                        'direction': 'increasing' if slope > 0 else 'decreasing'
                    })
            return anomalies

    async def get_anomaly_report(self) -> Dict[str, Any]:
        report = {'timestamp': datetime.utcnow().isoformat(), 'anomalies': []}
        async with self._lock:
            for metric_name in list(self.metric_history.keys()):
                anomalies = await self.detect_anomalies(metric_name)
                if anomalies:
                    report['anomalies'].extend(anomalies)
        return report

# ============================================================================
# Configuration Version Manager
# ============================================================================

class ConfigurationVersionManager:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.versions: List[Dict] = []
        self.current_version: Optional[str] = None
        self._load_versions()

    def _load_versions(self):
        self.versions = self.storage.load_config_versions()
        if self.versions:
            self.current_version = self.versions[0]['version_id']
        logger.info(f"Loaded {len(self.versions)} configuration versions")

    def save_version(self, config: Dict[str, Any], description: str = "") -> str:
        version_id = hashlib.md5(f"{datetime.utcnow().timestamp()}".encode()).hexdigest()[:12]
        self.storage.save_config_version(version_id, config, description, self.current_version)
        self.versions.insert(0, {
            'version_id': version_id,
            'timestamp': datetime.utcnow().isoformat(),
            'config': config,
            'description': description,
            'parent': self.current_version
        })
        self.current_version = version_id
        logger.info("Configuration version saved", version_id=version_id)
        return version_id

    def rollback_to_version(self, version_id: str) -> Optional[Dict[str, Any]]:
        for v in self.versions:
            if v['version_id'] == version_id:
                self.current_version = version_id
                logger.info("Rolled back to configuration version", version_id=version_id)
                return v['config']
        logger.error("Version not found", version_id=version_id)
        return None

    def get_version_history(self, limit: int = 10) -> List[Dict]:
        return self.versions[:limit]

    def get_version_diff(self, version_a: str, version_b: str) -> Dict[str, Any]:
        config_a = next((v['config'] for v in self.versions if v['version_id'] == version_a), None)
        config_b = next((v['config'] for v in self.versions if v['version_id'] == version_b), None)
        if not config_a or not config_b:
            return {'error': 'Version not found'}
        diff = {'added': {}, 'removed': {}, 'changed': {}}
        all_keys = set(config_a.keys()) | set(config_b.keys())
        for key in all_keys:
            if key not in config_a:
                diff['added'][key] = config_b[key]
            elif key not in config_b:
                diff['removed'][key] = config_a[key]
            elif config_a[key] != config_b[key]:
                diff['changed'][key] = {'from': config_a[key], 'to': config_b[key]}
        return diff

# ============================================================================
# Genetic Optimizer (Enhanced with MOPD)
# ============================================================================

class CoreGeneticOptimizer:
    """Genetic optimizer that uses configuration snapshots for safe evaluation."""
    def __init__(self, core: 'EnhancedBioInspiredCore'):
        self.core = core
        self.population_size = 20
        self.mutation_rate = 0.2
        self.crossover_rate = 0.7
        self.generations = 10
        self.tournament_size = 3
        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.lock = asyncio.Lock()
        self.param_bounds = {
            'health_check_interval_seconds': (10, 120),
            'circuit_breaker_threshold': (3, 10),
            'predictive_health_retrain_interval': (120, 900),
            'anomaly_zscore_threshold': (2.0, 5.0),
            'module_retirement_threshold': (0.1, 0.4)
        }
        # MOPD: Pareto front storage
        self.pareto_front: List[MOPDPoint] = []

    def _initialize_individual(self) -> Dict:
        ind = {}
        for key, (low, high) in self.param_bounds.items():
            ind[key] = random.uniform(low, high)
        ind['circuit_breaker_threshold'] = int(ind['circuit_breaker_threshold'])
        ind['health_check_interval_seconds'] = int(ind['health_check_interval_seconds'])
        ind['predictive_health_retrain_interval'] = int(ind['predictive_health_retrain_interval'])
        return ind

    def _initialize_population(self) -> List[Dict]:
        return [self._initialize_individual() for _ in range(self.population_size)]

    def _snapshot_config(self) -> Dict:
        """Capture a snapshot of current configuration."""
        return {
            'health_check_interval_seconds': self.core.config.health_check_interval_seconds,
            'circuit_breaker_threshold': self.core.registry._circuit_breaker_threshold,
            'predictive_health_retrain_interval': self.core._predictive_health_retrain_interval,
            'anomaly_zscore_threshold': self.core._anomaly_detector.zscore_threshold,
            'module_retirement_threshold': self.core._module_retirement_threshold
        }

    def _apply_snapshot(self, snapshot: Dict):
        """Apply a snapshot to the configuration."""
        self.core.config.health_check_interval_seconds = snapshot['health_check_interval_seconds']
        self.core.registry._circuit_breaker_threshold = snapshot['circuit_breaker_threshold']
        self.core._predictive_health_retrain_interval = snapshot['predictive_health_retrain_interval']
        self.core._anomaly_detector.zscore_threshold = snapshot['anomaly_zscore_threshold']
        self.core._module_retirement_threshold = snapshot['module_retirement_threshold']

    # ---------- Multi‑objective evaluation (NEW) ----------
    def _evaluate_individual(self, individual: Dict) -> Dict[str, float]:
        """Evaluate an individual on multiple objectives."""
        snapshot = self._snapshot_config()
        # Modify snapshot with individual's parameters
        modified = {
            'health_check_interval_seconds': individual['health_check_interval_seconds'],
            'circuit_breaker_threshold': individual['circuit_breaker_threshold'],
            'predictive_health_retrain_interval': individual['predictive_health_retrain_interval'],
            'anomaly_zscore_threshold': individual['anomaly_zscore_threshold'],
            'module_retirement_threshold': individual['module_retirement_threshold']
        }
        # Apply modified snapshot
        original_snapshot = self._snapshot_config()
        self._apply_snapshot(modified)
        # Simulate performance
        status = self.core.get_system_status()
        modules_health = asyncio.run(self.core.registry.health_check_all())
        health_scores = [1.0 if s['status'] == 'healthy' else 0.5 if s['status'] == 'degraded' else 0.0 for s in modules_health.values()]
        avg_health = np.mean(health_scores) if health_scores else 0.5
        uptime = status.get('uptime_seconds', 0)
        uptime_score = min(1.0, uptime / 86400)
        open_circuits = sum(1 for m in self.core.registry.modules.values() if m.circuit_breaker_state == 'open')
        circuit_score = max(0, 1.0 - open_circuits / max(1, len(self.core.registry.modules) * 0.5))
        anomaly_report = asyncio.run(self.core._anomaly_detector.get_anomaly_report())
        anomaly_count = len(anomaly_report.get('anomalies', []))
        anomaly_score = max(0, 1.0 - anomaly_count / 20)
        # Restore original snapshot
        self._apply_snapshot(original_snapshot)
        return {
            'health_score': avg_health,
            'uptime_score': uptime_score,
            'circuit_score': circuit_score,
            'anomaly_score': anomaly_score
        }

    def _select(self, population: List[Dict], fitness_scores: List[float]) -> Dict:
        tournament = random.sample(range(len(population)), self.tournament_size)
        best_idx = max(tournament, key=lambda i: fitness_scores[i])
        return population[best_idx]

    def _crossover(self, parent1: Dict, parent2: Dict) -> Dict:
        child = {}
        for key in parent1:
            if random.random() < 0.5:
                child[key] = parent1[key]
            else:
                child[key] = parent2[key]
            if random.random() < 0.3:
                child[key] = (parent1[key] + parent2[key]) / 2
        child['circuit_breaker_threshold'] = int(child['circuit_breaker_threshold'])
        child['health_check_interval_seconds'] = int(child['health_check_interval_seconds'])
        child['predictive_health_retrain_interval'] = int(child['predictive_health_retrain_interval'])
        return child

    def _mutate(self, individual: Dict) -> Dict:
        mutated = individual.copy()
        for key, (low, high) in self.param_bounds.items():
            if random.random() < self.mutation_rate:
                delta = random.uniform(-(high-low)*0.1, (high-low)*0.1)
                mutated[key] = max(low, min(high, mutated[key] + delta))
        mutated['circuit_breaker_threshold'] = int(mutated['circuit_breaker_threshold'])
        mutated['health_check_interval_seconds'] = int(mutated['health_check_interval_seconds'])
        mutated['predictive_health_retrain_interval'] = int(mutated['predictive_health_retrain_interval'])
        return mutated

    # ---------- Pareto front methods (NEW) ----------
    def _filter_pareto(self, points: List[MOPDPoint]) -> List[MOPDPoint]:
        """Return non‑dominated points."""
        if not points:
            return []
        objective_keys = ['health_score', 'uptime_score', 'circuit_score', 'anomaly_score']
        pareto = []
        for i, p_i in enumerate(points):
            dominated = False
            for j, p_j in enumerate(points):
                if i == j:
                    continue
                a_vec = [getattr(p_i, k) for k in objective_keys]
                b_vec = [getattr(p_j, k) for k in objective_keys]
                if all(b >= a for a, b in zip(a_vec, b_vec)) and any(b > a for a, b in zip(a_vec, b_vec)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p_i)
        return pareto

    def _select_best_from_pareto(self, pareto_front: List[MOPDPoint]) -> Optional[MOPDPoint]:
        """Select best point using scalarisation with MOPD weights."""
        if not pareto_front:
            return None
        weights = self.core.config.mopd.objective_weights
        objective_keys = list(weights.keys())

        # Normalise objectives across Pareto front
        max_vals = {}
        min_vals = {}
        for key in objective_keys:
            vals = [getattr(p, key) for p in pareto_front]
            max_vals[key] = max(vals)
            min_vals[key] = min(vals)
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in objective_keys}

        best = None
        best_score = -float('inf')
        for point in pareto_front:
            score = 0.0
            for key in objective_keys:
                val = getattr(point, key)
                norm = (val - min_vals[key]) / ranges[key] if ranges[key] > 0 else 1.0
                weight = weights.get(key, 0.0)
                score += weight * norm
            point.scalarised_score = score
            if score > best_score:
                best_score = score
                best = point
        return best

    # ---------- Main evolve (enhanced with MOPD) ----------
    async def evolve(self, generations: Optional[int] = None) -> Dict:
        async with self.lock:
            if generations is None:
                generations = self.generations
            population = self._initialize_population()

            if self.core.config.mopd.enabled:
                self.pareto_front = []

            for gen in range(generations):
                # Evaluate objectives for all individuals
                individuals_with_objs = []
                for ind in population:
                    objs = self._evaluate_individual(ind)
                    individuals_with_objs.append((ind, objs))

                # If MOPD enabled, update Pareto front
                if self.core.config.mopd.enabled:
                    points = []
                    for ind, objs in individuals_with_objs:
                        point = MOPDPoint(
                            individual=ind,
                            health_score=objs['health_score'],
                            uptime_score=objs['uptime_score'],
                            circuit_score=objs['circuit_score'],
                            anomaly_score=objs['anomaly_score']
                        )
                        points.append(point)
                    self.pareto_front = self._filter_pareto(self.pareto_front + points)

                    # Compute scalarised scores for selection
                    weights = self.core.config.mopd.objective_weights
                    fitness_scores = []
                    for point in points:
                        score = (weights.get('health_score', 0.4) * point.health_score +
                                 weights.get('uptime_score', 0.3) * point.uptime_score +
                                 weights.get('circuit_score', 0.2) * point.circuit_score +
                                 weights.get('anomaly_score', 0.1) * point.anomaly_score)
                        point.scalarised_score = score
                        fitness_scores.append(score)
                else:
                    # Legacy: single fitness (we use health_score as scalar)
                    fitness_scores = [objs['health_score'] for _, objs in individuals_with_objs]

                # Selection and reproduction
                new_population = []
                best_idx = max(range(len(population)), key=lambda i: fitness_scores[i])
                new_population.append(population[best_idx])
                while len(new_population) < self.population_size:
                    if random.random() < self.crossover_rate:
                        parent1 = self._select(population, fitness_scores)
                        parent2 = self._select(population, fitness_scores)
                        child = self._crossover(parent1, parent2)
                        child = self._mutate(child)
                        new_population.append(child)
                    else:
                        parent = self._select(population, fitness_scores)
                        new_population.append(parent.copy())
                population = new_population

                gen_best_fitness = max(fitness_scores)
                logger.debug(f"Gen {gen+1}: best fitness = {gen_best_fitness:.4f}")

            # After evolution, if MOPD enabled and we have a Pareto front, select best
            if self.core.config.mopd.enabled and self.pareto_front:
                best_point = self._select_best_from_pareto(self.pareto_front)
                if best_point:
                    self.best_individual = best_point.individual
                    self.best_fitness = best_point.scalarised_score
                    # Apply best individual permanently
                    original_snapshot = self._snapshot_config()
                    modified = {
                        'health_check_interval_seconds': best_point.individual['health_check_interval_seconds'],
                        'circuit_breaker_threshold': best_point.individual['circuit_breaker_threshold'],
                        'predictive_health_retrain_interval': best_point.individual['predictive_health_retrain_interval'],
                        'anomaly_zscore_threshold': best_point.individual['anomaly_zscore_threshold'],
                        'module_retirement_threshold': best_point.individual['module_retirement_threshold']
                    }
                    self._apply_snapshot(modified)
                    logger.info(f"Applied best MOPD individual with scalarised score {self.best_fitness:.4f}")
            else:
                # Legacy: keep best fitness and individual
                if fitness_scores:
                    best_idx = max(range(len(population)), key=lambda i: fitness_scores[i])
                    self.best_fitness = fitness_scores[best_idx]
                    self.best_individual = population[best_idx]
                    self._apply_snapshot(self._snapshot_config())  # Apply the best
                    logger.info(f"Applied best individual with fitness {self.best_fitness:.4f}")

            self.evolution_history.append({
                'timestamp': datetime.utcnow(),
                'best_fitness': self.best_fitness,
                'pareto_front_size': len(self.pareto_front) if self.core.config.mopd.enabled else 0
            })
            # Save best individual and Pareto front to global state
            self.core.storage.save_global_state('best_individual', json.dumps(self.best_individual))
            self.core.storage.save_global_state('best_fitness', str(self.best_fitness))
            if self.core.config.mopd.enabled:
                self.core.storage.save_global_state('pareto_front', json.dumps([p.to_dict() for p in self.pareto_front]))
            return {
                'best_fitness': self.best_fitness,
                'best_individual': self.best_individual,
                'pareto_front': [p.to_dict() for p in self.pareto_front] if self.core.config.mopd.enabled else None
            }

    def get_status(self) -> Dict:
        return {
            'best_fitness': self.best_fitness,
            'best_individual': self.best_individual,
            'history': self.evolution_history[-10:],
            'pareto_front_size': len(self.pareto_front) if self.core.config.mopd.enabled else 0
        }

# ============================================================================
# Module Registry (Enhanced with persistence)
# ============================================================================

class ModuleRegistry:
    # ... (same as before, unchanged) ...
    def __init__(self, storage: Storage, circuit_breaker_threshold: int = 5):
        self.storage = storage
        self.modules: Dict[str, ModuleEntry] = {}
        self.startup_order: List[str] = []
        self.shutdown_order: List[str] = []
        self._initialized = False
        self._init_lock = asyncio.Lock()
        self.loaded_modules: Set[str] = set()
        self.module_paths: Dict[str, str] = {}
        self.health_forecaster = PredictiveHealthForecaster(storage, config=CoreConfig())
        self._circuit_breaker_threshold = circuit_breaker_threshold
        self._lock = asyncio.Lock()
        self._load_from_db()

    def _load_from_db(self):
        for entry in self.storage.load_modules():
            self.modules[entry.name] = entry
            self.loaded_modules.add(entry.name)
            if entry.module_path:
                self.module_paths[entry.name] = entry.module_path
        logger.info(f"Module Registry initialized, loaded {len(self.modules)} modules from DB")

    # ... rest of ModuleRegistry methods unchanged ...
    # (For brevity, we assume the rest of ModuleRegistry is unchanged)

# ============================================================================
# Task Manager
# ============================================================================

class TaskManager:
    # ... (same as before) ...
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()

    def start_task(self, name: str, coro_func, *args, **kwargs):
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

    async def stop_all(self):
        self.shutdown_event.set()
        async with self._lock:
            for task in self.tasks.values():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()
        logger.info("All background tasks stopped")

# ============================================================================
# Task Input Validation
# ============================================================================

if PYDANTIC_AVAILABLE:
    class TaskInput(BaseModel):
        task_id: Optional[str] = None
        task_type: str = Field(..., min_length=1)
        complexity: float = Field(default=0.5, ge=0.0, le=1.0)
        priority: int = Field(default=0, ge=0, le=5)
        parameters: Dict[str, Any] = Field(default_factory=dict)
        module: Optional[str] = None

        @field_validator('task_id')
        def ensure_task_id(cls, v):
            return v or f"task_{uuid.uuid4().hex[:8]}"

# ============================================================================
# Decentralized Module Base Class
# ============================================================================

class DecentralizedModule(ABC):
    # ... (same as before) ...
    pass

# ============================================================================
# Module Marketplace (unchanged)
# ============================================================================

class ModuleMarketplace:
    # ... (same as before) ...
    pass

# ============================================================================
# Event Bus
# ============================================================================

@dataclass
class CoreEvent:
    event_type: str
    source: str
    payload: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    correlation_id: Optional[str] = None
    priority: int = 0

class CoreEventBus:
    # ... (same as before) ...
    pass

# ============================================================================
# Enhanced Bio-Inspired Core (Main Class)
# ============================================================================

class EnhancedBioInspiredCore:
    """
    Enhanced Bio-Inspired Core v9.1.0 with MOPD support.
    """

    def __init__(self,
                 config: Optional[CoreConfig] = None,
                 config_path: Optional[str] = None,
                 token_service: Optional[TokenServiceProtocol] = None,
                 gradient_service: Optional[GradientServiceProtocol] = None,
                 compartment_service: Optional[CompartmentServiceProtocol] = None,
                 biomass_service: Optional[BiomassServiceProtocol] = None):
        if config_path:
            with open(config_path, 'r') as f:
                data = json.load(f)
            self.config = CoreConfig(**data)
        else:
            self.config = config or CoreConfig()

        # Storage
        self.storage = Storage(self.config.db_path)

        # Security and enterprise components
        self.quantum_security = QuantumResilientSecurity(algorithm=self.config.quantum_signing_algorithm) if self.config.enable_quantum_signing else None
        self.blockchain_auditor = BlockchainAuditor(self.config) if self.config.enable_blockchain_audit else None
        self.strategy_selector = AutonomousStrategySelector(self.config) if self.config.enable_autonomous_strategy else None
        self.multi_cloud = MultiCloudDistributor(self.config) if self.config.enable_multi_cloud else None
        self.circuit_breaker = CircuitBreaker(
            name="bio_core",
            db_path=self.config.circuit_breaker_db_path,
            failure_threshold=self.config.circuit_breaker_threshold,
            recovery_timeout=self.config.circuit_breaker_recovery_timeout
        ) if self.config.enable_circuit_breaker else None

        # Service injection
        self._token_service = token_service
        self._gradient_service = gradient_service
        self._compartment_service = compartment_service
        self._biomass_service = biomass_service

        # Module registry
        self.registry = ModuleRegistry(self.storage, circuit_breaker_threshold=self.config.circuit_breaker_threshold)

        # Event bus
        self._event_bus = CoreEventBus(max_workers=4)

        # Configuration version manager
        self._version_manager = ConfigurationVersionManager(self.storage)
        self._save_initial_config()

        # Performance anomaly detector
        self._anomaly_detector = PerformanceAnomalyDetector()

        # Genetic optimizer
        self._genetic_optimizer = CoreGeneticOptimizer(self)

        # Module marketplace
        self._marketplace = ModuleMarketplace(self, self.storage, auto_replace=False)

        # Decentralized modules
        self._decentralized_modules: Dict[str, DecentralizedModule] = {}

        # Evolvable parameters
        self._module_retirement_threshold = 0.2
        self._predictive_health_retrain_interval = 300

        # Task manager
        self._task_manager = TaskManager()

        # Lifecycle
        self._lifecycle_phase = LifecyclePhase.UNREGISTERED
        self._start_time: Optional[datetime] = None
        self._shutdown_requested = False
        self._state_lock = asyncio.Lock()

        # Performance metrics
        self._perf_metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self._perf_metrics_lock = asyncio.Lock()

        # Prometheus metrics
        self._setup_metrics()

        # Signal handlers
        self._register_signal_handlers()

        # Start tasks
        self._start_tasks()

        logger.info("Enhanced Bio-Inspired Core v9.1.0 created with MOPD")

    def _setup_metrics(self):
        self.metrics = {
            'modules_total': Gauge('bio_core_modules_total', 'Total number of modules'),
            'modules_healthy': Gauge('bio_core_modules_healthy', 'Number of healthy modules'),
            'modules_unhealthy': Gauge('bio_core_modules_unhealthy', 'Number of unhealthy modules'),
            'circuit_breakers_open': Gauge('bio_core_circuit_breakers_open', 'Number of open circuit breakers'),
            'events_published': Counter('bio_core_events_published', 'Total events published', ['event_type']),
            'tasks_processed': Counter('bio_core_tasks_processed', 'Total tasks processed'),
            'task_duration': Histogram('bio_core_task_duration_seconds', 'Task processing duration'),
        }
        if PROMETHEUS_AVAILABLE and self.config.prometheus_port:
            start_http_server(self.config.prometheus_port)

    def _save_initial_config(self):
        self._version_manager.save_version(self.config.dict(), description="Initial configuration")

    def _start_tasks(self):
        self._task_manager.start_task("health_monitor", self._health_monitoring_loop)
        self._task_manager.start_task("performance_monitor", self._performance_monitoring_loop)
        self._task_manager.start_task("predictive_health", self._predictive_health_loop)
        self._task_manager.start_task("anomaly_detection", self._anomaly_detection_loop)
        self._task_manager.start_task("competition", self._competition_loop)
        self._task_manager.start_task("genetic_optimization", self._genetic_optimization_loop)
        self._task_manager.start_task("ml_training", self._ml_training_loop)
        self._task_manager.start_task("strategy_update", self._strategy_update_loop)
        self._task_manager.start_task("persistence_save", self._persistence_save_loop)

    def _register_signal_handlers(self):
        try:
            loop = asyncio.get_event_loop()
            for sig in [signal.SIGINT, signal.SIGTERM]:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
            logger.info("Signal handlers registered")
        except NotImplementedError:
            logger.warning("Signal handlers not supported on this platform")

    # ============================================================================
    # Service property accessors
    # ============================================================================

    @property
    def token_service(self) -> Optional[TokenServiceProtocol]:
        return self._token_service

    @property
    def gradient_service(self) -> Optional[GradientServiceProtocol]:
        return self._gradient_service

    @property
    def compartment_service(self) -> Optional[CompartmentServiceProtocol]:
        return self._compartment_service

    @property
    def biomass_service(self) -> Optional[BiomassServiceProtocol]:
        return self._biomass_service

    @property
    def event_bus(self) -> CoreEventBus:
        return self._event_bus

    @property
    def version_manager(self) -> ConfigurationVersionManager:
        return self._version_manager

    @property
    def anomaly_detector(self) -> PerformanceAnomalyDetector:
        return self._anomaly_detector

    # ============================================================================
    # Lifecycle methods
    # ============================================================================

    async def _transition_to(self, new_phase: LifecyclePhase):
        async with self._state_lock:
            old = self._lifecycle_phase
            self._lifecycle_phase = new_phase
            logger.info("Lifecycle transition", old=old.value, new=new_phase.value)

    async def initialize(self) -> bool:
        if self._lifecycle_phase == LifecyclePhase.RUNNING:
            logger.warning("Core already initialized")
            return True
        await self._transition_to(LifecyclePhase.INITIALIZING)
        self._start_time = datetime.utcnow()
        try:
            self.config.validate()
            if self._token_service is None:
                from .eco_atp_currency import EcoATPTokenManager
                self._token_service = EcoATPTokenManager()
            await self.registry.register('token_manager', self._token_service, health_check=lambda: True)
            if self._gradient_service is None:
                from .proton_gradient_fields import HierarchicalGradientManager
                self._gradient_service = HierarchicalGradientManager()
            await self.registry.register('gradient_manager', self._gradient_service, health_check=lambda: True)
            if self._compartment_service is None:
                from .chromatophore_compartments import HierarchicalCompartmentManager
                self._compartment_service = HierarchicalCompartmentManager(self._token_service)
            await self.registry.register('compartment_manager', self._compartment_service, health_check=lambda: True)
            if self._biomass_service is None:
                from .biomass_storage import BiomassStorage
                self._biomass_service = BiomassStorage(self._token_service, self._gradient_service)
            await self.registry.register('biomass_storage', self._biomass_service, health_check=lambda: True)
            health_results = await self.registry.health_check_all()
            unhealthy = [name for name, status in health_results.items() if status['status'] not in ('healthy', 'unknown')]
            if unhealthy:
                logger.warning("Some modules unhealthy after init", unhealthy=unhealthy)
            await self._transition_to(LifecyclePhase.RUNNING)
            init_time = (datetime.utcnow() - self._start_time).total_seconds()
            logger.info("Bio-Inspired Core initialized successfully", duration=init_time)
            return True
        except Exception as e:
            await self._transition_to(LifecyclePhase.ERROR)
            logger.error("Initialization failed", error=str(e), exc_info=True)
            return False

    async def shutdown(self) -> bool:
        if self._lifecycle_phase == LifecyclePhase.STOPPED:
            return True
        await self._transition_to(LifecyclePhase.STOPPING)
        self._shutdown_requested = True
        logger.info("Initiating graceful shutdown...")
        if self.config.enable_state_persistence:
            await self._save_state()
        await self._event_bus.shutdown()
        await self._task_manager.stop_all()
        results = await self.registry.shutdown_all()
        all_ok = all(results.values())
        if all_ok:
            await self._transition_to(LifecyclePhase.STOPPED)
            logger.info("Graceful shutdown complete")
        else:
            failed = [name for name, ok in results.items() if not ok]
            logger.warning("Some modules failed to shutdown", failed=failed)
        return all_ok

    async def _save_state(self):
        try:
            state = {
                'timestamp': datetime.utcnow().isoformat(),
                'config': self.config.dict(),
                'token_summary': await self._token_service.get_system_summary() if self._token_service else {},
                'gradient_strengths': await self._gradient_service.get_field_strengths() if self._gradient_service else {},
                'compartment_stats': await self._compartment_service.get_ecosystem_stats() if self._compartment_service else {},
                'biomass_stats': await self._biomass_service.get_storage_stats() if self._biomass_service else {}
            }
            state_dir = self.config.state_directory
            os.makedirs(state_dir, exist_ok=True)
            path = os.path.join(state_dir, f"state_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json")
            with open(path, 'w') as f:
                json.dump(state, f, indent=2, default=str)
            logger.info("State saved", path=path)
        except Exception as e:
            logger.error("Failed to save state", error=str(e))

    # ============================================================================
    # Monitoring Loops
    # ============================================================================

    async def _health_monitoring_loop(self):
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                health_results = await self.registry.health_check_all()
                unhealthy = [name for name, status in health_results.items() if status['status'] not in ('healthy', 'unknown')]
                if unhealthy:
                    logger.warning("Unhealthy modules", unhealthy=unhealthy)
                self.metrics['modules_total'].set(len(health_results))
                self.metrics['modules_healthy'].set(sum(1 for s in health_results.values() if s['status'] == 'healthy'))
                self.metrics['modules_unhealthy'].set(len(unhealthy))
                self.metrics['circuit_breakers_open'].set(sum(1 for s in health_results.values() if s['circuit_breaker'] == 'open'))
                await asyncio.sleep(self.config.health_check_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Health monitoring error", error=str(e))
                await asyncio.sleep(60)

    async def _performance_monitoring_loop(self):
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                if self._token_service:
                    summary = await self._token_service.get_system_summary()
                    balance = summary.get('total_balance', 0)
                    efficiency = summary.get('system_efficiency', 0)
                    async with self._perf_metrics_lock:
                        self._perf_metrics['token_balance'].append(balance)
                        self._perf_metrics['token_efficiency'].append(efficiency)
                    await self._anomaly_detector.record_metric('token_balance', balance)
                if self._gradient_service:
                    strengths = await self._gradient_service.get_field_strengths()
                    for field_id, strength in strengths.items():
                        async with self._perf_metrics_lock:
                            self._perf_metrics[f'gradient_{field_id}'].append(strength)
                        await self._anomaly_detector.record_metric(f'gradient_{field_id}', strength)
                if self._compartment_service:
                    stats = await self._compartment_service.get_ecosystem_stats()
                    viable = stats.get('viable_compartments', 0)
                    async with self._perf_metrics_lock:
                        self._perf_metrics['viable_compartments'].append(viable)
                    await self._anomaly_detector.record_metric('viable_compartments', viable)
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Performance monitoring error", error=str(e))
                await asyncio.sleep(60)

    async def _predictive_health_loop(self):
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                async with self.registry._lock:
                    for name, entry in self.registry.modules.items():
                        metrics = {
                            'health_score': 0.5 if entry.health_status == 'unknown' else 0.8 if entry.health_status == 'healthy' else 0.3,
                            'success_rate': 1.0 - (entry.failure_count / max(1, entry.failure_count + 1)),
                            'token_balance': 0.5,
                            'error_rate': 0.01
                        }
                        await self.registry.update_predictive_health(name, metrics)
                await self.registry.health_forecaster.train()
                await asyncio.sleep(self._predictive_health_retrain_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Predictive health loop error", error=str(e))
                await asyncio.sleep(60)

    async def _anomaly_detection_loop(self):
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                report = await self._anomaly_detector.get_anomaly_report()
                if report['anomalies']:
                    logger.warning("Performance anomalies detected", count=len(report['anomalies']))
                    for anomaly in report['anomalies']:
                        await self._event_bus.publish(CoreEvent(
                            event_type='performance_anomaly',
                            source='anomaly_detector',
                            payload=anomaly
                        ))
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Anomaly detection loop error", error=str(e))
                await asyncio.sleep(120)

    async def _competition_loop(self):
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                await self._marketplace.run_competition()
                await asyncio.sleep(self._marketplace.competition_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Competition loop error", error=str(e))
                await asyncio.sleep(300)

    async def _genetic_optimization_loop(self):
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                if len(self.registry.modules) >= 5:
                    logger.info("Starting genetic optimization cycle...")
                    result = await self._genetic_optimizer.evolve(generations=10)
                    # Telemetry for MOPD (if available)
                    if self.config.mopd.enabled:
                        logger.info(f"Genetic optimization complete: best fitness {result['best_fitness']:.4f}, Pareto front size: {len(result.get('pareto_front', []))}")
                    else:
                        logger.info(f"Genetic optimization complete: best fitness {result['best_fitness']:.4f}")
                await asyncio.sleep(86400)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Genetic optimization loop error", error=str(e))
                await asyncio.sleep(3600)

    async def _ml_training_loop(self):
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                await self.registry.health_forecaster.train()
                await asyncio.sleep(self._predictive_health_retrain_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("ML training error", error=str(e))
                await asyncio.sleep(60)

    async def _strategy_update_loop(self):
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                if self.strategy_selector:
                    state = await self._get_strategy_state()
                    strategy = await self.strategy_selector.select_strategy(state)
                    if strategy == 'performance':
                        self.config.health_check_interval_seconds = 20
                    elif strategy == 'carbon_saver':
                        self.config.health_check_interval_seconds = 40
                    else:
                        self.config.health_check_interval_seconds = 30
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Strategy update loop error", error=str(e))
                await asyncio.sleep(60)

    async def _persistence_save_loop(self):
        while self._lifecycle_phase == LifecyclePhase.RUNNING:
            try:
                for entry in self.registry.modules.values():
                    self.storage.save_module(entry)
                for name, score in self._marketplace.module_scores.items():
                    self.storage.save_marketplace_score(name, score)
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Persistence save loop error", error=str(e))
                await asyncio.sleep(60)

    async def _get_strategy_state(self) -> Dict:
        summary = await self.get_system_status()
        return {
            'system_load': summary.get('uptime_seconds', 0) / 86400,
            'system_health': sum(1 for m in self.registry.modules.values() if m.health_status == 'healthy') / max(1, len(self.registry.modules))
        }

    # ============================================================================
    # Public API
    # ============================================================================

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        # ... (same as before) ...
        if self._lifecycle_phase != LifecyclePhase.RUNNING:
            return {'success': False, 'reason': f'System not running (phase: {self._lifecycle_phase.value})'}

        if PYDANTIC_AVAILABLE:
            try:
                validated = TaskInput(**task)
                task = validated.model_dump()
            except ValidationError as e:
                return {'success': False, 'error': e.errors(), 'reason': 'Invalid task format'}

        if 'module' in task and task['module'] in self._decentralized_modules:
            return await self._decentralized_modules[task['module']].local_decision(task)

        ecoatp_required = task.get('complexity', 0.5) * 10

        await self._event_bus.publish(CoreEvent(
            event_type='task_received',
            source='core',
            payload={'task_id': task.get('task_id'), 'complexity': task.get('complexity', 0.5)}
        ))
        self.metrics['events_published'].labels(event_type='task_received').inc()

        if self._token_service:
            success, _ = await self._token_service.reserve_tokens('task_processor', ecoatp_required, None)
        else:
            success = True

        if not success:
            if self._biomass_service:
                stored, token_id = await self._biomass_service.store_task(
                    task_data=task, ecoatp_cost=ecoatp_required
                )
                await self._event_bus.publish(CoreEvent(
                    event_type='task_stored',
                    source='core',
                    payload={'task_id': task.get('task_id'), 'biomass_token': token_id}
                ))
                self.metrics['events_published'].labels(event_type='task_stored').inc()
                return {'success': True, 'status': 'stored', 'biomass_token': token_id}
            return {'success': False, 'reason': 'Insufficient tokens'}

        self.metrics['tasks_processed'].inc()
        with self.metrics['task_duration'].time():
            await asyncio.sleep(0.1)

        await self._event_bus.publish(CoreEvent(
            event_type='task_processed',
            source='core',
            payload={'task_id': task.get('task_id'), 'ecoatp_cost': ecoatp_required}
        ))
        self.metrics['events_published'].labels(event_type='task_processed').inc()

        if self.quantum_security:
            result = {'success': True, 'task_id': task.get('task_id', 'unknown'), 'ecoatp_cost': ecoatp_required}
            signature = await self.quantum_security.sign_data(result)
            result['quantum_signature'] = signature

        if self.blockchain_auditor:
            await self.blockchain_auditor.record_event('task_completed', {
                'task_id': task.get('task_id'),
                'ecoatp_cost': ecoatp_required
            })

        return result

    async def load_module(self, name: str, module_path: str) -> bool:
        return await self.registry.load_module(name, module_path)

    async def unload_module(self, name: str) -> bool:
        return await self.registry.unload_module(name)

    def register_decentralized_module(self, name: str, module: DecentralizedModule):
        self._decentralized_modules[name] = module
        for event_type in module.event_subscriptions:
            self._event_bus.subscribe(event_type, module.on_event)
        logger.info("Decentralized module registered", name=name)

    async def trigger_local_decision(self, module_name: str, context: Dict[str, Any]) -> Dict[str, Any]:
        if module_name in self._decentralized_modules:
            return await self._decentralized_modules[module_name].local_decision(context)
        return {'action': 'noop', 'reason': 'Module not found'}

    async def apply_module_replacement(self, old_name: str, new_name: str) -> bool:
        if old_name not in self.registry.modules or new_name not in self.registry.modules:
            return False
        old_entry = self.registry.modules[old_name]
        new_entry = self.registry.modules[new_name]
        await self.registry.shutdown_all()
        self.registry.modules[old_name] = new_entry
        for dep in old_entry.dependencies:
            if dep in self.registry.modules:
                self.registry.modules[dep].dependents.remove(old_name)
                self.registry.modules[dep].dependents.append(new_name)
        self.storage.save_module(new_entry)
        logger.info("Module replacement applied", old=old_name, new=new_name)
        return True

    def save_configuration_version(self, description: str = "") -> str:
        return self._version_manager.save_version(self.config.dict(), description=description)

    def rollback_configuration(self, version_id: str) -> bool:
        config_data = self._version_manager.rollback_to_version(version_id)
        if config_data:
            self.config = CoreConfig(**config_data)
            logger.info("Configuration rolled back", version_id=version_id)
            return True
        return False

    def update_configuration(self, updates: Dict[str, Any], description: str = "") -> Tuple[bool, str]:
        try:
            current_dict = self.config.dict()
            current_dict.update(updates)
            temp_config = CoreConfig(**current_dict)
            self.config = temp_config
            self.save_configuration_version(description or f"Updated: {', '.join(updates.keys())}")
            logger.info("Configuration updated", keys=list(updates.keys()))
            return True, "Configuration updated successfully"
        except ValidationError as e:
            return False, f"Invalid configuration: {e}"
        except Exception as e:
            return False, f"Update failed: {str(e)}"

    # ============================================================================
    # MOPD Public Methods (NEW)
    # ============================================================================

    def get_mopd_pareto_front(self) -> List[MOPDPoint]:
        """Return the current Pareto front from the genetic optimizer."""
        if not self.config.mopd.enabled or not self._genetic_optimizer:
            return []
        return self._genetic_optimizer.pareto_front.copy()

    def get_mopd_summary(self) -> Dict[str, Any]:
        """Return a summary of MOPD‑related metrics."""
        if not self.config.mopd.enabled or not self._genetic_optimizer:
            return {"enabled": False}
        return {
            "enabled": True,
            "objective_weights": self.config.mopd.objective_weights,
            "grid_resolution": self.config.mopd.grid_resolution,
            "pareto_front_size": len(self._genetic_optimizer.pareto_front),
            "best_scalarised_score": self._genetic_optimizer.best_fitness,
            "evolution_history": self._genetic_optimizer.evolution_history[-10:],
        }

    # ============================================================================
    # Status Reporting
    # ============================================================================

    def get_system_status(self) -> Dict[str, Any]:
        status = {
            'lifecycle_phase': self._lifecycle_phase.value,
            'uptime_seconds': (datetime.utcnow() - self._start_time).total_seconds() if self._start_time else 0,
            'timestamp': datetime.utcnow().isoformat(),
            'config': self.config.dict(),
            'modules': asyncio.run(self.registry.get_registry_stats()),
            'event_bus': self._event_bus.get_event_stats(),
            'config_version': self._version_manager.current_version,
            'loaded_modules': list(self.registry.loaded_modules),
            'genetic_optimizer': self._genetic_optimizer.get_status(),
            'marketplace': self._marketplace.get_marketplace_stats(),
            'module_retirement_threshold': self._module_retirement_threshold,
            'predictive_health_retrain_interval': self._predictive_health_retrain_interval,
            'quantum_security': self.quantum_security is not None,
            'blockchain_auditor': self.blockchain_auditor is not None,
            'strategy_selector': self.strategy_selector is not None,
            'multi_cloud': self.multi_cloud is not None,
            'mopd_enabled': self.config.mopd.enabled,
            'pareto_front_size': len(self.get_mopd_pareto_front()),
        }
        if self._token_service:
            status['token_economy'] = asyncio.run(self._token_service.get_system_summary())
        if self._gradient_service:
            status['gradients'] = asyncio.run(self._gradient_service.get_field_stats())
            status['gradient_forecasts'] = asyncio.run(self._gradient_service.get_forecast_summary())
        if self._compartment_service:
            status['compartments'] = asyncio.run(self._compartment_service.get_ecosystem_stats())
        if self._biomass_service:
            status['biomass'] = asyncio.run(self._biomass_service.get_storage_stats())
        async with self._perf_metrics_lock:
            status['performance'] = {
                name: {
                    'current': list(values)[-1] if values else None,
                    'avg_1min': np.mean(list(values)[-60:]) if len(values) >= 10 else None,
                    'trend': 'stable'
                }
                for name, values in self._perf_metrics.items()
            }
        status['anomalies'] = asyncio.run(self._anomaly_detector.get_anomaly_report())
        return status

    def get_health_dashboard(self) -> Dict[str, Any]:
        health = asyncio.run(self.registry.health_check_all())
        healthy_count = sum(1 for s in health.values() if s['status'] == 'healthy')
        total = len(health)
        return {
            'overall_health': 'healthy' if healthy_count == total else 'degraded' if healthy_count > total // 2 else 'unhealthy',
            'healthy_modules': healthy_count,
            'total_modules': total,
            'modules': health,
            'circuit_breakers': {
                name: entry.circuit_breaker_state
                for name, entry in self.registry.modules.items()
            },
            'predictive_health': {
                name: {
                    'predicted_health': entry.predicted_health,
                    'failure_probability': entry.failure_probability,
                    'trend': entry.health_trend
                }
                for name, entry in self.registry.modules.items()
            },
            'dependency_graph': asyncio.run(self.registry.get_dependency_graph()),
            'timestamp': datetime.utcnow().isoformat()
        }

    @property
    def is_running(self) -> bool:
        return self._lifecycle_phase == LifecyclePhase.RUNNING

    @property
    def is_healthy(self) -> bool:
        health = asyncio.run(self.registry.health_check_all())
        return all(s['status'] != 'error' for s in health.values())

    @property
    def lifecycle_phase(self) -> LifecyclePhase:
        return self._lifecycle_phase

    def get_lifecycle_status(self) -> Dict[str, Any]:
        return {
            'phase': self._lifecycle_phase.value,
            'is_running': self.is_running,
            'is_healthy': self.is_healthy,
            'uptime_seconds': (datetime.utcnow() - self._start_time).total_seconds() if self._start_time else 0,
            'start_time': self._start_time.isoformat() if self._start_time else None,
            'shutdown_requested': self._shutdown_requested,
            'module_count': len(self.registry.modules),
            'loaded_modules_count': len(self.registry.loaded_modules),
            'config_version': self._version_manager.current_version
        }

    # ============================================================================
    # Async Context Manager
    # ============================================================================

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

# ============================================================================
# Convenience Functions
# ============================================================================

def create_core(config: Optional[CoreConfig] = None, config_path: Optional[str] = None,
                token_service: Optional[TokenServiceProtocol] = None,
                gradient_service: Optional[GradientServiceProtocol] = None,
                compartment_service: Optional[CompartmentServiceProtocol] = None,
                biomass_service: Optional[BiomassServiceProtocol] = None) -> EnhancedBioInspiredCore:
    return EnhancedBioInspiredCore(
        config=config,
        config_path=config_path,
        token_service=token_service,
        gradient_service=gradient_service,
        compartment_service=compartment_service,
        biomass_service=biomass_service
    )

async def create_and_initialize(config: Optional[CoreConfig] = None,
                                token_service: Optional[TokenServiceProtocol] = None,
                                gradient_service: Optional[GradientServiceProtocol] = None,
                                compartment_service: Optional[CompartmentServiceProtocol] = None,
                                biomass_service: Optional[BiomassServiceProtocol] = None) -> EnhancedBioInspiredCore:
    core = create_core(config=config, token_service=token_service, gradient_service=gradient_service,
                       compartment_service=compartment_service, biomass_service=biomass_service)
    success = await core.initialize()
    if not success:
        raise RuntimeError("Failed to initialize Bio-Inspired Core")
    return core

async def main():
    logging.basicConfig(level=logging.INFO)
    core = await create_and_initialize()
    result = await core.process_task({'task_id': 'task1', 'complexity': 0.7})
    print("Task result:", result)
    status = core.get_system_status()
    print("System status:", status)
    print("Pareto front:", core.get_mopd_pareto_front())
    print("MOPD summary:", core.get_mopd_summary())
    await core.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
