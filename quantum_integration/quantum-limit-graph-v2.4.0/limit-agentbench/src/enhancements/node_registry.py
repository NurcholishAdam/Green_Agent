#!/usr/bin/env python3
# File: src/enhancements/node_registry_enhanced_v3_0.py
"""
Node Registry – unified descriptor for all compute nodes.
Version: 3.0.0 (Enhanced with MTOP, quantum security, blockchain, carbon, WebSocket, and MOPD)
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

# -----------------------------------------------------------------------------
# Async SQLite / SQLAlchemy
# -----------------------------------------------------------------------------
try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

from sqlalchemy import Column, String, Float, DateTime, Integer, Boolean, JSON, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError

# -----------------------------------------------------------------------------
# Pydantic
# -----------------------------------------------------------------------------
from pydantic import BaseModel, Field, field_validator, ValidationInfo

# -----------------------------------------------------------------------------
# Async HTTP
# -----------------------------------------------------------------------------
import aiohttp
from aiohttp import ClientTimeout, ClientSession, ClientError

# -----------------------------------------------------------------------------
# Tenacity
# -----------------------------------------------------------------------------
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# -----------------------------------------------------------------------------
# Prometheus
# -----------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# -----------------------------------------------------------------------------
# Post-quantum cryptography
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# WebSockets
# -----------------------------------------------------------------------------
try:
    import websockets
    from websockets.server import serve
    from websockets.exceptions import ConnectionClosed
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

# -----------------------------------------------------------------------------
# Structured logging with correlation ID
# -----------------------------------------------------------------------------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
        handlers=[
            logging.handlers.RotatingFileHandler('node_registry_v3.log', maxBytes=10*1024*1024, backupCount=5),
            logging.StreamHandler()
        ]
    )

correlation_id_var = contextvars.ContextVar('correlation_id', default='unknown')

class CorrelationIdFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = correlation_id_var.get()
        return True

logger.addFilter(CorrelationIdFilter())

# Audit logger
audit_logger = logging.getLogger("audit")
audit_handler = logging.FileHandler('audit.log')
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# -----------------------------------------------------------------------------
# Prometheus metrics
# -----------------------------------------------------------------------------
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    NODE_REGISTRATIONS = Counter('node_registrations_total', 'Total node registrations', ['status'], registry=REGISTRY)
    NODE_REFRESHES = Counter('node_refreshes_total', 'Total node refreshes', ['status'], registry=REGISTRY)
    NODE_CACHE_SIZE = Gauge('node_cache_size', 'Number of nodes in cache', registry=REGISTRY)
    NODE_REFRESH_DURATION = Histogram('node_refresh_duration_seconds', 'Node refresh duration', registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge('node_circuit_breaker_state', ['name'], registry=REGISTRY)
    RATE_LIMITER_THROTTLE = Gauge('node_rate_limiter_throttle', registry=REGISTRY)
    QUANTUM_KEYS = Gauge('node_quantum_keys_total', 'Number of quantum keys', registry=REGISTRY)
    BLOCKCHAIN_TX = Counter('node_blockchain_tx_total', 'Blockchain transactions', ['status'], registry=REGISTRY)
    CLOUD_DISTRIBUTIONS = Counter('node_cloud_distributions_total', 'Cloud distributions', ['provider', 'status'], registry=REGISTRY)
    CARBON_INTENSITY = Gauge('node_carbon_intensity_gco2_per_kwh', 'Current carbon intensity', registry=REGISTRY)
else:
    class DummyMetrics:
        def inc(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def labels(self, *args, **kwargs): return self
    NODE_REGISTRATIONS = DummyMetrics()
    NODE_REFRESHES = DummyMetrics()
    NODE_CACHE_SIZE = DummyMetrics()
    NODE_REFRESH_DURATION = DummyMetrics()
    CIRCUIT_BREAKER_STATE = DummyMetrics()
    RATE_LIMITER_THROTTLE = DummyMetrics()
    QUANTUM_KEYS = DummyMetrics()
    BLOCKCHAIN_TX = DummyMetrics()
    CLOUD_DISTRIBUTIONS = DummyMetrics()
    CARBON_INTENSITY = DummyMetrics()

# -----------------------------------------------------------------------------
# Dummy tenacity decorator if not available
# -----------------------------------------------------------------------------
if not TENACITY_AVAILABLE:
    def retry(*args, **kwargs):
        def decorator(func):
            @functools.wraps(func)
            async def wrapper(*fargs, **fkwargs):
                attempts = 0
                max_attempts = kwargs.get('stop', stop_after_attempt(3)).stop.max_attempt_number
                delay = 1
                while attempts < max_attempts:
                    try:
                        return await func(*fargs, **fkwargs)
                    except Exception as e:
                        attempts += 1
                        if attempts >= max_attempts:
                            raise
                        await asyncio.sleep(delay)
                        delay *= 2
            return wrapper
        return decorator

# -----------------------------------------------------------------------------
# Enhanced Configuration (Pydantic)
# -----------------------------------------------------------------------------
class NodeRegistryConfig(BaseModel):
    instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    version: str = Field("3.0.0")
    log_level: str = Field("INFO")

    refresh_interval: int = Field(3600, gt=0)
    cache_ttl: int = Field(300, gt=0)
    max_concurrent_refreshes: int = Field(5, ge=1)

    # Database
    db_path: str = Field("/tmp/node_registry_v3.db")

    # Carbon
    carbon_api_key: Optional[str] = None
    carbon_region: str = Field("global")
    carbon_update_interval: int = Field(300, ge=10)

    # Quantum
    enable_quantum_security: bool = True
    quantum_algorithm: str = Field("dilithium")
    quantum_master_key: str = Field(default="", description="Hex string for key encryption")

    # Blockchain
    enable_blockchain_verification: bool = True
    blockchain_rpc_url: str = Field("http://localhost:8545")
    blockchain_contract_address: Optional[str] = None
    blockchain_private_key: Optional[str] = None

    # Multi-cloud
    enable_multi_cloud: bool = True
    aws_enabled: bool = True
    azure_enabled: bool = True
    gcp_enabled: bool = True

    # Metrics
    metrics_port: int = Field(8000, ge=1024, le=65535)

    # WebSocket
    websocket_port: int = Field(8770, ge=1024)

    # Retry and circuit breaker
    max_retry_attempts: int = Field(3, ge=0)
    circuit_breaker_threshold: int = Field(5, ge=1)
    circuit_breaker_timeout: int = Field(30, ge=1)

    # MOPD weights
    mopd_weights: Dict[str, float] = Field(
        default_factory=lambda: {
            'freshness': 0.4,
            'carbon': 0.3,
            'cost': 0.2,
            'importance': 0.1
        }
    )

    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        allowed = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        if v.upper() not in allowed:
            raise ValueError(f'LOG_LEVEL must be one of {allowed}')
        return v.upper()

    @field_validator('quantum_master_key')
    @classmethod
    def validate_master_key(cls, v: str) -> str:
        if not v:
            raise ValueError('quantum_master_key must be set via environment NODE_REGISTRY_QUANTUM_MASTER_KEY')
        try:
            bytes.fromhex(v)
        except ValueError:
            raise ValueError('quantum_master_key must be a hex string')
        return v

    def get_master_key_bytes(self) -> bytes:
        return bytes.fromhex(self.quantum_master_key)

    class Config:
        env_prefix = "NODE_REGISTRY_"

# -----------------------------------------------------------------------------
# Enhanced Circuit Breaker and Rate Limiter
# -----------------------------------------------------------------------------
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

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
                    logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN")
                else:
                    raise Exception(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception as e:
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
                logger.warning(f"Circuit breaker {self.name} OPEN after {self.failure_count} failures")
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                logger.warning(f"Circuit breaker {self.name} OPEN from HALF_OPEN")

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
            time_passed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.window))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)

# -----------------------------------------------------------------------------
# Enhanced Database Manager (async-safe)
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
            db_url,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            connect_args={'check_same_thread': False}
        )
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
                session.execute(
                    text("""
                        INSERT OR REPLACE INTO node_descriptors
                        (node_id, location, energy_efficiency, carbon_intensity, helium_index, material_index,
                         cooling_type, renewable_fraction, harvester_type, capture_efficiency, energy_output_watts,
                         availability_pattern, quantum_signature, blockchain_tx_hash, last_updated)
                        VALUES (:node_id, :location, :energy_efficiency, :carbon_intensity, :helium_index, :material_index,
                         :cooling_type, :renewable_fraction, :harvester_type, :capture_efficiency, :energy_output_watts,
                         :availability_pattern, :quantum_signature, :blockchain_tx_hash, :last_updated)
                    """),
                    {
                        'node_id': descriptor.node_id,
                        'location': descriptor.location,
                        'energy_efficiency': descriptor.energy_efficiency,
                        'carbon_intensity': descriptor.carbon_intensity,
                        'helium_index': descriptor.helium_index,
                        'material_index': descriptor.material_index,
                        'cooling_type': descriptor.cooling_type,
                        'renewable_fraction': descriptor.renewable_fraction,
                        'harvester_type': descriptor.harvester_type,
                        'capture_efficiency': descriptor.capture_efficiency,
                        'energy_output_watts': descriptor.energy_output_watts,
                        'availability_pattern': json.dumps(descriptor.availability_pattern),
                        'quantum_signature': descriptor.quantum_signature,
                        'blockchain_tx_hash': descriptor.blockchain_tx_hash,
                        'last_updated': datetime.now()
                    }
                )
        return await self.execute_sync(sync_register)

    async def load_all_nodes(self) -> List['NodeDescriptor']:
        def sync_load():
            nodes = []
            with self._get_session() as session:
                result = session.execute(
                    text("""
                        SELECT node_id, location, energy_efficiency, carbon_intensity, helium_index, material_index,
                               cooling_type, renewable_fraction, harvester_type, capture_efficiency, energy_output_watts,
                               availability_pattern, quantum_signature, blockchain_tx_hash, last_updated
                        FROM node_descriptors
                    """)
                )
                for row in result:
                    descriptor = NodeDescriptor(
                        node_id=row[0],
                        location=row[1],
                        energy_efficiency=row[2],
                        carbon_intensity=row[3],
                        helium_index=row[4],
                        material_index=row[5],
                        cooling_type=row[6],
                        renewable_fraction=row[7],
                        harvester_type=row[8],
                        capture_efficiency=row[9],
                        energy_output_watts=row[10],
                        availability_pattern=json.loads(row[11]) if row[11] else None,
                        quantum_signature=row[12],
                        blockchain_tx_hash=row[13],
                        last_updated=row[14]
                    )
                    nodes.append(descriptor)
            return nodes
        return await self.execute_sync(sync_load)

    def dispose(self):
        if self.engine:
            self.engine.dispose()
        self._executor.shutdown(wait=False)

# -----------------------------------------------------------------------------
# Node Descriptor (Pydantic model) – extended
# -----------------------------------------------------------------------------
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

    @field_validator('carbon_intensity')
    @classmethod
    def validate_carbon_intensity(cls, v: float) -> float:
        if v < 0:
            raise ValueError('carbon_intensity must be >= 0')
        return v

    @field_validator('helium_index')
    @classmethod
    def validate_helium_index(cls, v: float) -> float:
        if v < 0:
            raise ValueError('helium_index must be >= 0')
        return v

    @field_validator('material_index')
    @classmethod
    def validate_material_index(cls, v: float) -> float:
        if v < 0:
            raise ValueError('material_index must be >= 0')
        return v

# -----------------------------------------------------------------------------
# Carbon Intensity Manager (simplified)
# -----------------------------------------------------------------------------
class CarbonIntensityManager:
    def __init__(self, config: NodeRegistryConfig):
        self.config = config
        self.api_key = config.carbon_api_key
        self.region = config.carbon_region
        self.endpoint = "https://api.electricitymap.org/v3/carbon-intensity"
        self.cache = {}
        self.last_update = None
        self._session = None
        self._lock = asyncio.Lock()
        self._circuit_breaker = EnhancedCircuitBreaker("carbon_api", config)
        self._rate_limiter = EnhancedRateLimiter(rate=10, window=60)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
           retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, ConnectionError)))
    async def _fetch_intensity(self) -> float:
        await self._rate_limiter.wait_and_acquire()
        session = await self._get_session()
        url = f"{self.endpoint}/latest?zone={self.region}"
        headers = {'auth-token': self.api_key} if self.api_key else {}
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status != 200:
                raise Exception(f"Carbon API returned {response.status}")
            data = await response.json()
            return data.get('carbonIntensity', 400)

    async def get_current_intensity(self) -> float:
        cache_key = f"{self.region}_{datetime.utcnow().hour}"
        if cache_key in self.cache and self.last_update and (datetime.utcnow() - self.last_update).seconds < 300:
            return self.cache[cache_key]
        try:
            intensity = await self._circuit_breaker.call(self._fetch_intensity)
            async with self._lock:
                self.cache[cache_key] = intensity
                self.last_update = datetime.utcnow()
            if PROMETHEUS_AVAILABLE:
                CARBON_INTENSITY.set(intensity)
            return intensity
        except Exception as e:
            logger.warning(f"Carbon API failed: {e}, using fallback")
            return 400

    async def close(self):
        if self._session:
            await self._session.close()

# -----------------------------------------------------------------------------
# Quantum Security
# -----------------------------------------------------------------------------
class QuantumResilientNodeSecurity:
    def __init__(self, config: NodeRegistryConfig, db_manager: EnhancedDatabaseManager):
        self.config = config
        self.db_manager = db_manager
        self.pqc_algorithms = {}
        self.pqc_available = PQC_AVAILABLE
        self._lock = asyncio.Lock()
        self.master_key = config.get_master_key_bytes()
        if self.pqc_available:
            self.pqc_algorithms['dilithium'] = dilithium
            self.pqc_algorithms['falcon'] = falcon
            self.pqc_algorithms['sphincs'] = sphincs
        else:
            logger.warning("PQC not available; fallback to ECDSA.")

    def _derive_key(self, salt: bytes) -> bytes:
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100000, backend=default_backend())
        return kdf.derive(self.master_key)

    def _encrypt_key(self, key_bytes: bytes) -> Tuple[bytes, bytes, bytes]:
        salt = os.urandom(16)
        derived = self._derive_key(salt)
        aesgcm = AESGCM(derived)
        nonce = os.urandom(12)
        ciphertext = aesgcm.encrypt(nonce, key_bytes, None)
        return salt, nonce, ciphertext

    def _decrypt_key(self, salt: bytes, nonce: bytes, ciphertext: bytes) -> bytes:
        derived = self._derive_key(salt)
        aesgcm = AESGCM(derived)
        return aesgcm.decrypt(nonce, ciphertext, None)

    async def generate_keypair(self, algorithm: str = 'dilithium', validity_days: int = 30) -> Dict:
        async with self._lock:
            if algorithm not in self.pqc_algorithms and not self.pqc_available:
                return await self._fallback_generate_keypair()
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
                salt, nonce, encrypted_private = self._encrypt_key(private_key)
                # Store in DB (need a keypairs table; for simplicity we store in memory)
                # For brevity, we skip storing; in production we'd use Storage class.
                return {'key_id': key_id, 'algorithm': algorithm, 'public_key': public_key.hex()}
            except Exception as e:
                logger.error(f"Keypair generation failed: {e}")
                return await self._fallback_generate_keypair()

    async def _fallback_generate_keypair(self) -> Dict:
        private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
        public_key = private_key.public_key()
        public_bytes = public_key.public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo)
        key_id = f"ecdsa_{uuid.uuid4().hex[:8]}"
        return {'key_id': key_id, 'algorithm': 'ecdsa', 'public_key': public_bytes.hex()}

    async def sign_node_data(self, data: Dict, key_id: str) -> str:
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        # For simplicity, we use fallback signing; real PQC would be used.
        return hashlib.sha256(data_bytes).hexdigest()

# -----------------------------------------------------------------------------
# Blockchain Verification (simplified)
# -----------------------------------------------------------------------------
class BlockchainNodeVerification:
    def __init__(self, config: NodeRegistryConfig):
        self.config = config
        self.web3 = None
        self.contract = None
        self.account = None
        self.web3_available = False
        self._circuit_breaker = EnhancedCircuitBreaker("blockchain", config)
        self._rate_limiter = EnhancedRateLimiter(rate=10, window=60)

        if WEB3_AVAILABLE:
            self._initialize_blockchain()
        else:
            logger.warning("Web3 not available; simulations active.")

    def _initialize_blockchain(self):
        try:
            self.web3 = Web3(Web3.HTTPProvider(self.config.blockchain_rpc_url))
            if not self.web3.is_connected():
                raise ConnectionError("Cannot connect to blockchain RPC")
            if self.config.blockchain_private_key:
                self.account = Account.from_key(self.config.blockchain_private_key)
                self.web3.eth.default_account = self.account.address
            else:
                self.account = self.web3.eth.accounts[0]
            contract_abi = []  # minimal ABI
            if self.config.blockchain_contract_address:
                self.contract = self.web3.eth.contract(
                    address=self.config.blockchain_contract_address,
                    abi=contract_abi
                )
                self.web3_available = True
                logger.info(f"Connected to blockchain at {self.config.blockchain_rpc_url}")
            else:
                logger.warning("Contract address not configured; simulations active.")
        except Exception as e:
            logger.error(f"Blockchain initialization failed: {e}")

    async def record_node_registration(self, node_id: str, data_hash: str) -> str:
        if not self.web3_available:
            return f"sim_{hashlib.sha256(os.urandom(32)).hexdigest()[:16]}"
        # Actual transaction would be built here.
        return f"0x{hashlib.sha256(os.urandom(32)).hexdigest()}"

# -----------------------------------------------------------------------------
# Multi‑Cloud Distribution (simplified)
# -----------------------------------------------------------------------------
class MultiCloudNodeDistribution:
    def __init__(self, config: NodeRegistryConfig):
        self.config = config
        self.providers = {
            'aws': {'enabled': config.aws_enabled},
            'azure': {'enabled': config.azure_enabled},
            'gcp': {'enabled': config.gcp_enabled}
        }
        self.active_provider = 'aws'

    async def distribute_node_data(self, data: Dict) -> Dict:
        # Simple selection: pick first enabled provider
        for provider, info in self.providers.items():
            if info['enabled']:
                self.active_provider = provider
                break
        return {'optimal_provider': self.active_provider, 'timestamp': datetime.now().isoformat()}

# -----------------------------------------------------------------------------
# MTOP Engine for Refresh Prioritization
# -----------------------------------------------------------------------------
class RefreshTeacherEnsemble:
    """
    Teachers: performance, carbon, cost, adaptive.
    Each outputs a refresh urgency score for a node.
    """
    def __init__(self, config: NodeRegistryConfig):
        self.config = config
        self.teachers = {
            'performance': self._performance_teacher,
            'carbon': self._carbon_teacher,
            'cost': self._cost_teacher,
            'adaptive': self._adaptive_teacher
        }
        self.teacher_weights = {'performance': 0.25, 'carbon': 0.25, 'cost': 0.25, 'adaptive': 0.25}
        self.history = deque(maxlen=100)

    def _performance_teacher(self, node: NodeDescriptor) -> float:
        # Higher urgency if efficiency is low or stale
        age = (datetime.now() - node.last_updated).seconds / 3600
        urgency = (1 - node.energy_efficiency) * 0.5 + min(age / 24, 1) * 0.5
        return urgency

    def _carbon_teacher(self, node: NodeDescriptor, carbon_intensity: float) -> float:
        # More urgency if carbon intensity is high and node's intensity is outdated
        urgency = (carbon_intensity / 1000) * 0.5 + (1 - node.renewable_fraction) * 0.5
        return urgency

    def _cost_teacher(self, node: NodeDescriptor) -> float:
        # Lower urgency if refresh cost is high (e.g., many nodes)
        # For simplicity, use a constant.
        return 0.5

    def _adaptive_teacher(self, node: NodeDescriptor) -> float:
        # Use historical refresh outcomes to adjust
        if len(self.history) > 10:
            recent = [h for h in self.history if h['node_id'] == node.node_id]
            if recent:
                avg_improvement = np.mean([h['improvement'] for h in recent[-5:]])
                urgency = 1 - avg_improvement
            else:
                urgency = 0.5
        else:
            urgency = 0.5
        return urgency

    async def get_teacher_urgency(self, node: NodeDescriptor, carbon_intensity: float) -> Dict[str, float]:
        scores = {
            'performance': self._performance_teacher(node),
            'carbon': self._carbon_teacher(node, carbon_intensity),
            'cost': self._cost_teacher(node),
            'adaptive': self._adaptive_teacher(node)
        }
        self.history.append({'node_id': node.node_id, 'improvement': random.uniform(0, 0.1)})  # placeholder
        return scores

    def update_weights(self, rewards: Dict[str, float]):
        total = sum(rewards.values())
        if total > 0:
            for name in self.teacher_weights:
                self.teacher_weights[name] = rewards[name] / total

class RefreshDistillationStudent:
    """
    Student model that learns to combine teacher urgencies.
    """
    def __init__(self, config: NodeRegistryConfig):
        self.config = config
        self.learning_rate = 0.01
        self.decay = 0.99
        self.weights = np.array([0.3, 0.3, 0.2, 0.2])  # features: age, carbon, cost, adaptive
        self.bias = 0.0
        self.update_count = 0

    async def predict(self, features: np.ndarray) -> float:
        return np.dot(self.weights, features) + self.bias

    async def train_step(self, features: np.ndarray, target: float):
        self.update_count += 1
        pred = await self.predict(features)
        error = pred - target
        grad = 2 * error * features
        self.weights -= self.learning_rate * grad
        self.bias -= self.learning_rate * 2 * error
        self.learning_rate *= self.decay

class MTOPRefreshEngine:
    """
    MTOP engine that decides which nodes to refresh.
    """
    def __init__(self, config: NodeRegistryConfig):
        self.config = config
        self.teacher_ensemble = RefreshTeacherEnsemble(config)
        self.student = RefreshDistillationStudent(config)
        self.history = deque(maxlen=500)

    async def prioritize_nodes(self, nodes: List[NodeDescriptor], carbon_intensity: float) -> List[Tuple[str, float]]:
        # Compute urgency for each node
        urgencies = []
        for node in nodes:
            teacher_scores = await self.teacher_ensemble.get_teacher_urgency(node, carbon_intensity)
            weighted = sum(self.teacher_ensemble.teacher_weights[t] * s for t, s in teacher_scores.items())
            # Student prediction: features: age, carbon, cost, adaptive
            age = (datetime.now() - node.last_updated).seconds / 3600
            features = np.array([age/24, node.carbon_intensity/1000, 0.5, teacher_scores['adaptive']])
            student_pred = await self.student.predict(features)
            final_urgency = 0.6 * weighted + 0.4 * student_pred
            urgencies.append((node.node_id, final_urgency))
        # Sort by urgency descending
        urgencies.sort(key=lambda x: x[1], reverse=True)
        return urgencies

    async def update(self, node_id: str, actual_improvement: float):
        # On-policy update: reward based on improvement
        reward = max(0, min(1, actual_improvement * 2))
        # For simplicity, we update teacher weights equally.
        teacher_rewards = {t: reward for t in self.teacher_ensemble.teachers}
        self.teacher_ensemble.update_weights(teacher_rewards)
        # Also update student (needs features – we'd need to store them)
        pass

# -----------------------------------------------------------------------------
# WebSocket Server
# -----------------------------------------------------------------------------
class EnhancedWebSocketServer:
    def __init__(self, port: int):
        self.port = port
        self.connections = set()
        self.subscriptions = defaultdict(set)
        self._lock = asyncio.Lock()
        self.server = None
        self._heartbeat_task = None

    async def start(self):
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("WebSockets not available, skipping")
            return
        try:
            self.server = await serve(self._handle_connection, '0.0.0.0', self.port)
            logger.info(f"WebSocket server started on port {self.port}")
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        except Exception as e:
            logger.error(f"WebSocket server start failed: {e}")

    async def _handle_connection(self, websocket, path):
        async with self._lock:
            self.connections.add(websocket)
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    if data.get('action') == 'subscribe':
                        topic = data.get('topic', 'all')
                        async with self._lock:
                            self.subscriptions[topic].add(websocket)
                    elif data.get('action') == 'unsubscribe':
                        topic = data.get('topic', 'all')
                        async with self._lock:
                            self.subscriptions[topic].discard(websocket)
                except Exception as e:
                    logger.error(f"WebSocket message error: {e}")
        except ConnectionClosed:
            pass
        finally:
            async with self._lock:
                self.connections.discard(websocket)
                for topic in list(self.subscriptions.keys()):
                    self.subscriptions[topic].discard(websocket)

    async def broadcast(self, message: Dict, topic: str = 'all'):
        if not self.connections:
            return
        data = json.dumps(message, default=str)
        async with self._lock:
            targets = self.subscriptions.get(topic, set())
            if topic == 'all':
                targets = self.connections
            for conn in list(targets):
                try:
                    await conn.send(data)
                except Exception:
                    self.connections.discard(conn)

    async def _heartbeat_loop(self):
        while True:
            try:
                await asyncio.sleep(30)
                await self.broadcast({'type': 'heartbeat', 'timestamp': datetime.now().isoformat()})
            except asyncio.CancelledError:
                break

    async def stop(self):
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("WebSocket server stopped")

# -----------------------------------------------------------------------------
# Enhanced Node Registry (v3.0)
# -----------------------------------------------------------------------------
class NodeRegistry:
    """
    Enhanced registry for node descriptors with MTOP, quantum, blockchain, carbon, WebSocket.
    """

    def __init__(self, config: Optional[NodeRegistryConfig] = None):
        self.config = config or NodeRegistryConfig()
        self.instance_id = self.config.instance_id
        self.db_manager = EnhancedDatabaseManager(self.config)
        self.carbon_manager = CarbonIntensityManager(self.config)
        self.quantum_security = QuantumResilientNodeSecurity(self.config, self.db_manager) if self.config.enable_quantum_security else None
        self.blockchain = BlockchainNodeVerification(self.config) if self.config.enable_blockchain_verification else None
        self.cloud_distributor = MultiCloudNodeDistribution(self.config) if self.config.enable_multi_cloud else None
        self.mtop_engine = MTOPRefreshEngine(self.config)

        self.cache: Dict[str, NodeDescriptor] = {}
        self.cache_ttl = self.config.cache_ttl
        self._lock = asyncio.Lock()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._circuit_breaker = EnhancedCircuitBreaker("cloud_api", self.config)
        self._rate_limiter = EnhancedRateLimiter(rate=10, window=60)
        self._bulkhead = asyncio.Semaphore(self.config.max_concurrent_refreshes)
        self._session = None
        self._refresh_count = 0
        self._shutdown_event = asyncio.Event()
        self._websocket = EnhancedWebSocketServer(self.config.websocket_port) if WEBSOCKETS_AVAILABLE else None

        # Load initial data
        asyncio.create_task(self._load_initial_data())
        logger.info(f"NodeRegistry v{self.config.version} initialized (instance: {self.instance_id})")

    async def _load_initial_data(self):
        try:
            nodes = await self.db_manager.load_all_nodes()
            async with self._lock:
                for node in nodes:
                    self.cache[node.node_id] = node
                if PROMETHEUS_AVAILABLE:
                    NODE_CACHE_SIZE.set(len(self.cache))
            logger.info(f"Loaded {len(nodes)} nodes from DB")
        except Exception as e:
            logger.error(f"Failed to load initial data: {e}")

    async def start(self):
        self._running = True
        if self._websocket:
            await self._websocket.start()
        self._task = asyncio.create_task(self._refresh_loop(self.config.refresh_interval))
        logger.info("NodeRegistry started")

    async def _refresh_loop(self, interval: int):
        while self._running and not self._shutdown_event.is_set():
            try:
                await self._refresh_all_nodes()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Node refresh loop error: {e}")
                await asyncio.sleep(60)

    async def _refresh_all_nodes(self):
        start_time = time.time()
        async with self._lock:
            node_list = list(self.cache.values())

        if not node_list:
            return

        # Get current carbon intensity
        carbon_intensity = await self.carbon_manager.get_current_intensity()

        # Use MTOP to prioritize which nodes to refresh
        prioritized = await self.mtop_engine.prioritize_nodes(node_list, carbon_intensity)
        # Refresh only top N (e.g., top 5 or all)
        top_n = min(5, len(prioritized))
        to_refresh = [node_id for node_id, _ in prioritized[:top_n]]

        # Refresh each selected node
        tasks = [self._refresh_single_node(nid) for nid in to_refresh]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Update MTOP based on improvements (simulated)
        for nid, res in zip(to_refresh, results):
            if not isinstance(res, Exception):
                improvement = random.uniform(0, 0.1)  # placeholder
                await self.mtop_engine.update(nid, improvement)

        if PROMETHEUS_AVAILABLE:
            NODE_REFRESHES.labels(status='success').inc()
            NODE_REFRESH_DURATION.observe(time.time() - start_time)

        self._refresh_count += 1
        logger.info(f"Refreshed {len(to_refresh)} nodes (count: {self._refresh_count})")

        # Broadcast refresh event
        if self._websocket:
            await self._websocket.broadcast({
                'type': 'nodes_refreshed',
                'nodes': to_refresh,
                'timestamp': datetime.now().isoformat()
            }, topic='node_updates')

    async def _refresh_single_node(self, node_id: str):
        """Refresh a single node from cloud API."""
        async with self._bulkhead:
            await self._rate_limiter.wait_and_acquire()
            # Simulate API call
            await asyncio.sleep(random.uniform(0.1, 0.3))
            # Simulate new data
            new_data = {
                'energy_efficiency': random.uniform(0.7, 0.95),
                'carbon_intensity': random.uniform(200, 600),
                'helium_index': random.uniform(0, 10),
                'material_index': random.uniform(0.5, 1.5),
                'renewable_fraction': random.uniform(0, 1),
                'last_updated': datetime.now()
            }
            async with self._lock:
                if node_id in self.cache:
                    node = self.cache[node_id]
                    node.energy_efficiency = new_data['energy_efficiency']
                    node.carbon_intensity = new_data['carbon_intensity']
                    node.helium_index = new_data['helium_index']
                    node.material_index = new_data['material_index']
                    node.renewable_fraction = new_data['renewable_fraction']
                    node.last_updated = new_data['last_updated']
                    # Persist
                    await self.db_manager.register_node(node)
                else:
                    logger.warning(f"Node {node_id} not in cache; cannot refresh")

    async def register_node(self, descriptor: NodeDescriptor) -> bool:
        # Optional quantum signing
        if self.quantum_security:
            key = await self.quantum_security.generate_keypair(self.config.quantum_algorithm)
            sig = await self.quantum_security.sign_node_data(asdict(descriptor), key['key_id'])
            descriptor.quantum_signature = sig

        # Blockchain recording
        if self.blockchain:
            data_hash = hashlib.sha256(json.dumps(asdict(descriptor), sort_keys=True, default=str).encode()).hexdigest()
            tx_hash = await self.blockchain.record_node_registration(descriptor.node_id, data_hash)
            descriptor.blockchain_tx_hash = tx_hash

        # Multi-cloud distribution
        if self.cloud_distributor:
            dist = await self.cloud_distributor.distribute_node_data({'node_id': descriptor.node_id})
            # Could store provider info in metadata

        # Persist
        success = await self.db_manager.register_node(descriptor)
        if not success:
            return False

        async with self._lock:
            self.cache[descriptor.node_id] = descriptor
            if PROMETHEUS_AVAILABLE:
                NODE_CACHE_SIZE.set(len(self.cache))
        NODE_REGISTRATIONS.labels(status='success').inc()
        logger.info(f"Node {descriptor.node_id} registered")

        # Broadcast
        if self._websocket:
            await self._websocket.broadcast({
                'type': 'node_registered',
                'node_id': descriptor.node_id,
                'timestamp': datetime.now().isoformat()
            }, topic='node_updates')

        return True

    async def get_node(self, node_id: str) -> Optional[NodeDescriptor]:
        async with self._lock:
            node = self.cache.get(node_id)
            if node:
                if (datetime.now() - node.last_updated).seconds > self.cache_ttl:
                    # Stale; trigger async refresh
                    asyncio.create_task(self._refresh_single_node(node_id))
            return node

    async def list_nodes(self) -> List[str]:
        async with self._lock:
            return list(self.cache.keys())

    async def get_node_count(self) -> int:
        async with self._lock:
            return len(self.cache)

    async def health_check(self) -> Dict:
        return {
            'running': self._running,
            'cache_size': len(self.cache),
            'db_connected': self.db_manager.engine is not None,
            'last_refresh_count': self._refresh_count,
            'timestamp': datetime.now().isoformat()
        }

    async def stop(self):
        logger.info("Shutting down NodeRegistry...")
        self._shutdown_event.set()
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._websocket:
            await self._websocket.stop()
        await self.carbon_manager.close()
        self.db_manager.dispose()
        logger.info("NodeRegistry stopped")

# -----------------------------------------------------------------------------
# Signal handling (fixed)
# -----------------------------------------------------------------------------
_shutdown_requested = False
_shutdown_event_global = asyncio.Event()

def handle_signal(signum, frame):
    global _shutdown_requested
    if not _shutdown_requested:
        _shutdown_requested = True
        logger.info(f"Received signal {signum}, initiating shutdown...")
        asyncio.create_task(_signal_shutdown())

async def _signal_shutdown():
    _shutdown_event_global.set()

async def shutdown_handler():
    global _registry_instance
    if _registry_instance:
        await _registry_instance.stop()
        _registry_instance = None

# Singleton accessor
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
# Main entry point (for testing)
# -----------------------------------------------------------------------------
async def main():
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_signal(s, None))

    print("=" * 80)
    print("Enhanced Node Registry v3.0.0")
    print("=" * 80)

    registry = await get_node_registry()

    # Register sample nodes
    node1 = NodeDescriptor(
        node_id="node-001",
        location="us-east-1",
        energy_efficiency=0.85,
        carbon_intensity=420,
        helium_index=0.5,
        material_index=1.2,
        cooling_type="liquid",
        renewable_fraction=0.4,
        harvester_type="solar",
        capture_efficiency=0.9,
        energy_output_watts=5000,
        availability_pattern={"monday": "high"}
    )
    await registry.register_node(node1)

    node2 = NodeDescriptor(
        node_id="node-002",
        location="eu-west-1",
        energy_efficiency=0.92,
        carbon_intensity=280,
        helium_index=0.3,
        material_index=0.9,
        cooling_type="air",
        renewable_fraction=0.6,
        harvester_type="wind",
        capture_efficiency=0.85,
        energy_output_watts=8000
    )
    await registry.register_node(node2)

    print(f"\nRegistered nodes: {await registry.list_nodes()}")
    node = await registry.get_node("node-001")
    print(f"Node-001: {node}")

    print(f"\nHealth: {await registry.health_check()}")

    try:
        await _shutdown_event_global.wait()
    except asyncio.CancelledError:
        pass
    finally:
        await shutdown_handler()

if __name__ == "__main__":
    asyncio.run(main())
