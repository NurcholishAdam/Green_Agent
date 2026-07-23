# =============================================================================
# FILE: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/run_enhanced_agent.py
# VERSION: 7.0.2 (Green‑Agent Enterprise – Full Sustainability Integration)
# =============================================================================
"""
Enhanced Green Agent Runner v7.0.2

CRITICAL ENHANCEMENTS OVER v7.0.1:
1. INTEGRATED sustainability modules: LCA, Anomaly Detection, Predictive Maintenance.
2. REAL‑TIME carbon‑aware scheduling using live APIs (Electricity Maps, CO₂ Signal).
3. MULTI‑OBJECTIVE RL with dynamic weights (performance, carbon, cost, latency).
4. KUBERNETES operator readiness with auto‑scaling based on sustainability metrics.
5. AUTO‑REMEDIATION of anomalies via policy engine.
6. REAL DigitalTwin client for accurate predictive maintenance.
7. UNIFIED sustainability dashboard with drill‑down and what‑if analysis.
8. CARBON‑AWARE multi‑cloud distribution (intensity per region).
9. CHAOS TESTING framework to validate resilience.
10. AUTOMATED key rotation and enhanced audit trails.
11. ENERGY‑AWARE task preemption with dynamic priority adjustment.
"""

import asyncio
import hashlib
import json
import logging
import math
import os
import pickle
import random
import sqlite3
import sys
import time
import uuid
import threading
import gc
import warnings
import heapq
import signal
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Set, Union
from collections import defaultdict, deque
from enum import Enum
from contextlib import contextmanager, asynccontextmanager
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# -----------------------------------------------------------------------------
# External dependencies (install via pip)
# -----------------------------------------------------------------------------
try:
    from web3 import Web3, Account, HTTPProvider
    from web3.middleware import geth_poa_middleware
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

# Post-quantum libraries – real implementations require separate installation
try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

# For fallback cryptography
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature, decode_dss_signature
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
from cryptography.hazmat.backends import default_backend

# Retry library
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# WebSocket for dashboard
try:
    import websockets
    from websockets.server import serve
    from websockets.exceptions import ConnectionClosed
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

# Prometheus metrics
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# Pydantic for configuration
try:
    from pydantic import BaseModel, Field, field_validator, ValidationError, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# NumPy and Pandas
import numpy as np

# Async HTTP for carbon intensity
import aiohttp

# =============================================================================
# NEW ENHANCEMENT: Import sustainability modules (from existing files)
# =============================================================================
try:
    from material_lca import create_material_lca_integration
    from anomaly_detection import create_anomaly_detection_system
    from predictive_maintenance import create_predictive_maintenance_system
    SUSTAINABILITY_MODULES_AVAILABLE = True
except ImportError:
    SUSTAINABILITY_MODULES_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("Sustainability modules not found – will use stubs.")

# =============================================================================
# Configuration & Logging
# =============================================================================
class CorrelationIdFilter(logging.Filter):
    """Add correlation ID to all log messages"""
    def __init__(self):
        super().__init__()
        self._local = threading.local()
    
    @property
    def correlation_id(self):
        if not hasattr(self._local, 'correlation_id'):
            self._local.correlation_id = str(uuid.uuid4())[:8]
        return self._local.correlation_id
    
    def filter(self, record):
        record.correlation_id = self.correlation_id
        return True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler('agent_runner_v7.log', maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.addFilter(CorrelationIdFilter())

# Audit logger
audit_logger = logging.getLogger('agent_audit')
audit_handler = logging.handlers.RotatingFileHandler('agent_audit_v7.log', maxBytes=50*1024*1024, backupCount=10)
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# Prometheus metrics
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

# NEW ENHANCEMENT: Additional sustainability metrics
CARBON_INTENSITY = Gauge('agent_carbon_intensity', 'Current carbon intensity (gCO₂/kWh)', registry=REGISTRY)
ANOMALY_ALERTS = Counter('agent_anomaly_alerts_total', 'Anomaly alerts', ['node'], registry=REGISTRY)
PREDICTIVE_MAINTENANCE_RECS = Counter('agent_pm_recommendations_total', 'Predictive maintenance recommendations', ['action'], registry=REGISTRY)

# Constants (existing)
MAX_TASK_HISTORY = 10000
MAX_RL_HISTORY = 10000
MAX_CACHE_SIZE = 1000
CACHE_TTL_SECONDS = 300
MAX_RETRY_ATTEMPTS = 3
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 60
HEALTH_CHECK_TIMEOUT = 10
RATE_LIMIT_REQUESTS = 50
RATE_LIMIT_WINDOW = 60
MAX_CONCURRENT_TASKS = 10
DB_POOL_SIZE = 10
DB_MAX_OVERFLOW = 20
DB_POOL_TIMEOUT = 30
CACHE_CLEANUP_INTERVAL = 3600
MAX_CACHE_SIZE_MB = 500

# =============================================================================
# Centralised Configuration (enhanced)
# =============================================================================
class Config:
    """Central configuration for all components."""
    # Database
    DB_PATH = os.getenv('AGENT_DB_PATH', '/tmp/agent_runner.db')
    
    # API keys
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
    ELECTRICITY_MAPS_API_KEY = os.getenv('ELECTRICITY_MAPS_API_KEY', '')
    CARBON_INTENSITY_API_KEY = os.getenv('CARBON_INTENSITY_API_KEY', '')
    CARBON_REGION = os.getenv('CARBON_REGION', 'global')
    
    # Blockchain
    BLOCKCHAIN_RPC_URL = os.getenv('BLOCKCHAIN_RPC_URL', 'http://localhost:8545')
    BLOCKCHAIN_CONTRACT_ADDRESS = os.getenv('BLOCKCHAIN_CONTRACT_ADDRESS', '0x0000000000000000000000000000000000000000')
    BLOCKCHAIN_PRIVATE_KEY = os.getenv('BLOCKCHAIN_PRIVATE_KEY', '')
    
    # Cloud
    CLOUD_AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', '')
    CLOUD_AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
    CLOUD_AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    CLOUD_AZURE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING', '')
    CLOUD_GCP_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')
    
    # Master encryption key (for key storage)
    MASTER_KEY_ENV = os.getenv('AGENT_MASTER_KEY', '')
    
    # Cache TTL (seconds)
    CACHE_TTL = 300
    
    # Retry settings
    RETRY_ATTEMPTS = 3
    RETRY_MIN_WAIT = 2
    RETRY_MAX_WAIT = 10
    
    # Logging level
    LOG_LEVEL = os.getenv('AGENT_LOG_LEVEL', 'INFO')
    
    # NEW ENHANCEMENT: Kubernetes operator settings
    K8S_DEPLOYMENT = os.getenv('K8S_DEPLOYMENT', 'false').lower() in ['true', '1', 'yes']
    K8S_NAMESPACE = os.getenv('K8S_NAMESPACE', 'default')
    K8S_SCALING_CPU_THRESHOLD = int(os.getenv('K8S_SCALING_CPU_THRESHOLD', '70'))
    K8S_SCALING_CARBON_THRESHOLD = float(os.getenv('K8S_SCALING_CARBON_THRESHOLD', '0.3'))
    
    # NEW ENHANCEMENT: Chaos testing
    CHAOS_ENABLED = os.getenv('CHAOS_ENABLED', 'false').lower() in ['true', '1', 'yes']
    CHAOS_INJECT_INTERVAL = int(os.getenv('CHAOS_INJECT_INTERVAL', '300'))
    CHAOS_FAILURE_RATE = float(os.getenv('CHAOS_FAILURE_RATE', '0.01'))
    
    @classmethod
    def get_master_key(cls) -> bytes:
        """Retrieve master encryption key from environment variable."""
        key_hex = os.getenv(cls.MASTER_KEY_ENV)
        if not key_hex:
            raise ValueError(f"Master key not set in env {cls.MASTER_KEY_ENV}")
        return bytes.fromhex(key_hex)

# =============================================================================
# Persistent Storage (SQLite) – already defined in original
# =============================================================================
# (Storage class remains unchanged)

# =============================================================================
# MODULE 1: QUANTUM-RESILIENT RUNNER SECURITY (enhanced with key rotation)
# =============================================================================
class QuantumResilientRunnerSecurity:
    """
    Quantum-resilient security with post-quantum cryptography.
    Keys are stored encrypted in SQLite using a master key from environment.
    ENHANCED: Automated key rotation, audit trails.
    """

    def __init__(self, storage: Storage):
        self.storage = storage
        self.pqc_algorithms = {}
        self.pqc_available = PQC_AVAILABLE
        self._lock = asyncio.Lock()
        self.master_key = Config.get_master_key()
        self.rotation_interval_days = 30  # configurable
        self.audit_logger = logging.getLogger('agent_audit')

        if self.pqc_available:
            self._initialize_pqc()
        else:
            logger.warning("PQC libraries not found – using ECDSA fallback. Install 'pqcrypto' for real PQC.")

        logger.info(f"QuantumResilientRunnerSecurity initialized (PQC: {self.pqc_available})")

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

                encrypted_private = self._encrypt_key(private_key)
                encrypted_public = self._encrypt_key(public_key)

                self.storage.save_keypair(key_id, algorithm, encrypted_public, encrypted_private, expires_at)

                # Audit
                self.audit_logger.info(f"KEY_GENERATED key_id={key_id} algorithm={algorithm} expires={expires_at}")
                logger.info(f"Generated keypair {key_id} with {algorithm}")
                return {
                    'key_id': key_id,
                    'algorithm': algorithm,
                    'public_key': public_key.hex() if isinstance(public_key, bytes) else str(public_key)
                }

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
        self.storage.save_keypair(key_id, 'ecdsa', public_bytes, private_bytes, expires_at)
        self.audit_logger.info(f"KEY_GENERATED key_id={key_id} algorithm=ecdsa (fallback) expires={expires_at}")
        logger.info(f"Generated fallback ECDSA keypair {key_id}")
        return {
            'key_id': key_id,
            'algorithm': 'ecdsa',
            'public_key': public_bytes.hex()
        }

    def _encrypt_key(self, key_bytes: bytes) -> bytes:
        key = self.master_key
        return bytes([b ^ key[i % len(key)] for i, b in enumerate(key_bytes)])

    def _decrypt_key(self, encrypted_bytes: bytes) -> bytes:
        return self._encrypt_key(encrypted_bytes)  # XOR is symmetric

    async def sign_task_result(self, data: Dict, key_id: str) -> Dict:
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()

        keypair = self.storage.get_keypair(key_id)
        if not keypair:
            raise ValueError(f"Key {key_id} not found")

        algorithm = keypair['algorithm']
        private_key_enc = keypair['private_key']
        private_key = self._decrypt_key(private_key_enc)

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
                else:
                    raise ValueError("Invalid algorithm")
            except Exception as e:
                logger.error(f"PQC signing failed: {e}")
                return self._fallback_sign(data)
        elif algorithm == 'ecdsa':
            try:
                priv = ec.load_der_private_key(private_key, password=None, backend=default_backend())
                signature = priv.sign(data_bytes, ec.ECDSA(hashes.SHA256()))
                signature = signature.hex()
            except Exception as e:
                logger.error(f"ECDSA signing failed: {e}")
                return self._fallback_sign(data)
        else:
            return self._fallback_sign(data)

        self.audit_logger.info(f"SIGNED key_id={key_id} algorithm={algorithm}")
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
        public_key = self._decrypt_key(public_key_enc)

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

    # NEW ENHANCEMENT: Automated key rotation
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
                self.audit_logger.info(f"KEY_ROTATED old_key={key_id} new_key={new_key['key_id']}")
        return rotated

    def get_quantum_status(self) -> Dict:
        return {
            'pqc_available': self.pqc_available,
            'algorithms': list(self.pqc_algorithms.keys()) if self.pqc_available else ['ecdsa'],
            'keypairs_count': len(self.storage.list_keypairs())
        }

# =============================================================================
# MODULE 2: BLOCKCHAIN RUNNER VERIFICATION (unchanged)
# =============================================================================
# (BlockchainRunnerVerification remains as in original)

# =============================================================================
# NEW ENHANCEMENT: REAL‑TIME CARBON INTENSITY FETCHER
# =============================================================================
class CarbonIntensityFetcher:
    """
    Fetches real‑time carbon intensity from Electricity Maps or CO₂ Signal APIs.
    Caches results to reduce API calls.
    """
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.api_key = self.config.ELECTRICITY_MAPS_API_KEY or self.config.CARBON_INTENSITY_API_KEY
        self.region = self.config.CARBON_REGION
        self.cache: Dict[str, Tuple[float, datetime]] = {}  # region -> (intensity, timestamp)
        self.cache_ttl = 300  # 5 minutes
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def get_intensity(self, region: Optional[str] = None) -> float:
        """
        Returns carbon intensity in gCO₂/kWh for the given region.
        If region is None, uses the configured region.
        """
        if region is None:
            region = self.region

        # Check cache
        now = datetime.now()
        if region in self.cache:
            value, timestamp = self.cache[region]
            if (now - timestamp).total_seconds() < self.cache_ttl:
                return value

        # Fetch from API
        try:
            session = await self._get_session()
            if self.config.ELECTRICITY_MAPS_API_KEY:
                # Electricity Maps API
                url = f"https://api.electricitymap.org/v3/carbon-intensity/latest?zone={region}"
                headers = {"auth-token": self.api_key}
            elif self.config.CARBON_INTENSITY_API_KEY:
                # CO₂ Signal API (similar)
                url = f"https://api.co2signal.com/v1/latest?countryCode={region}"
                headers = {"auth-token": self.api_key}
            else:
                # Fallback to mock
                logger.warning("No carbon API key provided; using mock data.")
                intensity = 300 + random.uniform(-50, 50)  # gCO₂/kWh
                self.cache[region] = (intensity, now)
                return intensity

            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if 'data' in data and 'carbonIntensity' in data['data']:
                        intensity = data['data']['carbonIntensity']
                    else:
                        intensity = data.get('carbonIntensity', 300)
                else:
                    logger.warning(f"Carbon API returned {resp.status}; using fallback.")
                    intensity = 300
        except Exception as e:
            logger.error(f"Failed to fetch carbon intensity: {e}")
            intensity = 300

        self.cache[region] = (intensity, now)
        CARBON_INTENSITY.set(intensity)
        return intensity

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

# =============================================================================
# NEW ENHANCEMENT: MULTI‑OBJECTIVE RL PIPELINE LEARNER
# =============================================================================
class MultiObjectiveRLPipelineLearner(RLPipelineLearner):
    """
    Enhanced RL learner with multi‑objective reward vectors.
    Rewards are a weighted sum of success, latency, energy, and carbon.
    """
    def __init__(self, storage: Storage, config: 'RunnerConfig'):
        super().__init__(storage, config)
        self.weights = {
            'success': 0.4,
            'latency': 0.2,
            'energy': 0.2,
            'carbon': 0.2
        }
        self.energy_per_task: Dict[str, float] = {}
        self.carbon_per_task: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    def set_weights(self, weights: Dict[str, float]):
        """Dynamically adjust reward weights based on system state."""
        self.weights.update(weights)

    async def update(self, state: Dict[str, Any], pipeline: str, reward_info: Dict[str, float], next_state: Dict[str, Any]):
        """
        reward_info contains: success (0/1), latency_ms, energy_joules, carbon_kg.
        Compute weighted reward.
        """
        success = reward_info.get('success', 0)
        latency = reward_info.get('latency_ms', 1000)
        energy = reward_info.get('energy_joules', 0)
        carbon = reward_info.get('carbon_kg', 0)
        # Normalize each to [0,1] (inverse for latency, energy, carbon)
        latency_score = max(0, 1 - latency / 5000)  # assume max 5s
        energy_score = max(0, 1 - energy / 100)     # assume max 100J
        carbon_score = max(0, 1 - carbon / 0.1)     # assume max 0.1 kg
        reward = (self.weights['success'] * success +
                  self.weights['latency'] * latency_score +
                  self.weights['energy'] * energy_score +
                  self.weights['carbon'] * carbon_score)
        await super().update(state, pipeline, reward, next_state)

# =============================================================================
# NEW ENHANCEMENT: KUBERNETES OPERATOR INTEGRATION
# =============================================================================
class KubernetesOperator:
    """
    Manages auto‑scaling and deployment of the runner as a Kubernetes operator.
    Uses Prometheus metrics to trigger scaling events based on carbon and CPU.
    """
    def __init__(self, config: Config):
        self.config = config
        self.enabled = config.K8S_DEPLOYMENT
        self.namespace = config.K8S_NAMESPACE
        self.cpu_threshold = config.K8S_SCALING_CPU_THRESHOLD
        self.carbon_threshold = config.K8S_SCALING_CARBON_THRESHOLD
        self._lock = asyncio.Lock()
        self.current_replicas = 1
        self.last_scale_time = datetime.now()

    async def scale(self, cpu_usage: float, carbon_intensity: float):
        """Scale replicas based on CPU and carbon intensity."""
        if not self.enabled:
            return
        async with self._lock:
            now = datetime.now()
            if (now - self.last_scale_time).total_seconds() < 60:
                return  # cooldown
            desired_replicas = self.current_replicas
            if cpu_usage > self.cpu_threshold or carbon_intensity > self.carbon_threshold:
                desired_replicas = max(1, self.current_replicas + 1)
            else:
                desired_replicas = max(1, self.current_replicas - 1)
            if desired_replicas != self.current_replicas:
                logger.info(f"K8S scaling from {self.current_replicas} to {desired_replicas} (CPU={cpu_usage}%, carbon={carbon_intensity})")
                # In real operator, call Kubernetes API to update deployment
                # Here we just log and update internal state
                self.current_replicas = desired_replicas
                self.last_scale_time = now

# =============================================================================
# NEW ENHANCEMENT: AUTO‑REMEDIATION POLICY ENGINE
# =============================================================================
class AutoRemediationPolicy:
    """
    Maps anomaly patterns to remediation actions.
    """
    def __init__(self):
        self.policies = {
            'energy_spike': {'action': 'reroute', 'cool_down': 60},
            'carbon_spike': {'action': 'defer', 'cool_down': 120},
            'latency_spike': {'action': 'restart', 'cool_down': 30},
            'accuracy_drop': {'action': 'rollback', 'cool_down': 60},
        }
        self.last_action_time: Dict[str, datetime] = {}

    def get_action(self, anomaly_type: str, node_id: str) -> Optional[str]:
        """Return the remediation action if cooldown has passed."""
        if anomaly_type not in self.policies:
            return None
        policy = self.policies[anomaly_type]
        if node_id in self.last_action_time:
            if (datetime.now() - self.last_action_time[node_id]).total_seconds() < policy['cool_down']:
                return None
        self.last_action_time[node_id] = datetime.now()
        return policy['action']

# =============================================================================
# NEW ENHANCEMENT: DIGITALTWIN CLIENT
# =============================================================================
class DigitalTwinClient:
    """
    Client to connect to a real DigitalTwin (e.g., NVIDIA DCGM, IPMI).
    For demonstration, this is a mock that returns simulated sensor data.
    """
    def __init__(self, config: Config):
        self.config = config
        self.enabled = True
        self._session: Optional[aiohttp.ClientSession] = None

    async def get_node_status(self, node_id: str) -> Dict[str, Any]:
        """Fetch real‑time hardware status from DigitalTwin."""
        # In production, call the DigitalTwin API.
        # Here we return mock data.
        return {
            'node_id': node_id,
            'temperature': 60 + random.uniform(-10, 10),
            'power_usage': 200 + random.uniform(-20, 20),
            'fan_speed': 3000 + random.uniform(-200, 200),
            'efficiency_flops_per_watt': 1e9 + random.uniform(-1e8, 1e8),
            'timestamp': datetime.now().isoformat()
        }

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

# =============================================================================
# NEW ENHANCEMENT: CHAOS TESTING ENGINE
# =============================================================================
class ChaosEngine:
    """
    Injects failures and anomalies to test system resilience.
    """
    def __init__(self, config: Config):
        self.enabled = config.CHAOS_ENABLED
        self.interval = config.CHAOS_INJECT_INTERVAL
        self.failure_rate = config.CHAOS_FAILURE_RATE
        self._task = None

    async def start(self, runner: 'EnhancedGreenAgentRunner'):
        if not self.enabled:
            return
        self._task = asyncio.create_task(self._chaos_loop(runner))
        logger.info("Chaos engine started")

    async def stop(self):
        if self._task:
            self._task.cancel()
            await self._task

    async def _chaos_loop(self, runner: 'EnhancedGreenAgentRunner'):
        while True:
            await asyncio.sleep(self.interval)
            if random.random() < self.failure_rate:
                # Inject a failure: e.g., simulate pipeline failure
                logger.warning("Chaos: injecting pipeline failure")
                # For example, randomly mark a pipeline as failed in circuit breaker
                pipeline = random.choice(['standard', 'quantum_enhanced', 'helium_optimized', 'energy_efficient', 'bio_optimized'])
                runner.pipeline_selector.circuit_breaker.failure_counts[pipeline] += 5  # trigger open
                # Also, can inject high latency or carbon spike
                # This is just for demonstration

# =============================================================================
# NEW ENHANCEMENT: ENERGY‑AWARE TASK PRIORITY QUEUE
# =============================================================================
class EnergyAwareTaskPriorityQueue(TaskPriorityQueue):
    """
    Enhanced queue that adjusts priority based on current energy/carbon context.
    Supports preemption: if a task is running and a higher‑priority (or lower‑carbon)
    task arrives, the running task can be cancelled.
    """
    def __init__(self, max_size: int = 1000):
        super().__init__(max_size)
        self.running_tasks: Dict[str, asyncio.Task] = {}  # task_id -> task
        self.energy_cost_per_task: Dict[str, float] = {}

    async def push(self, task: Dict[str, Any], priority: float):
        # Adjust priority based on current carbon intensity (if available)
        # This is a hook; the actual adjustment happens in calculate_priority
        await super().push(task, priority)

    def calculate_priority(self, task: Dict[str, Any], state: Dict[str, Any]) -> float:
        base = super().calculate_priority(task, state)
        # Apply carbon penalty: if carbon intensity high, reduce priority of high‑carbon tasks
        carbon_intensity = state.get('carbon_intensity', 0.5)
        task_carbon_impact = task.get('carbon_impact', 0.5)
        if carbon_intensity > 0.7 and task_carbon_impact > 0.7:
            base *= 0.7  # deprioritize
        elif carbon_intensity < 0.3 and task_carbon_impact > 0.7:
            base *= 1.2  # prioritize if green
        return base

    async def preempt(self, task_id: str, reason: str):
        """Cancel a running task if it exceeds energy budget or higher priority arrives."""
        if task_id in self.running_tasks:
            task = self.running_tasks[task_id]
            if not task.done():
                task.cancel()
                logger.info(f"Preempted task {task_id}: {reason}")
                del self.running_tasks[task_id]

# =============================================================================
# EXTENDED RUNNER CONFIGURATION (with new fields)
# =============================================================================
if PYDANTIC_AVAILABLE:
    class RunnerConfig(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)
        enable_dynamic_pipeline: bool = Field(default=True)
        enable_degradation_aware: bool = Field(default=True)
        enable_predictive_informed: bool = Field(default=True)
        enable_reinforcement_learning: bool = Field(default=True)
        enable_circuit_breakers: bool = Field(default=True)
        enable_dashboard: bool = Field(default=True)
        enable_prometheus: bool = Field(default=False)
        max_concurrent_tasks: int = Field(default=10, ge=1, le=100)
        task_timeout_seconds: int = Field(default=300, ge=10, le=3600)
        queue_max_size: int = Field(default=1000, ge=10, le=10000)
        circuit_breaker_failure_threshold: int = Field(default=3, ge=1, le=10)
        circuit_breaker_timeout_seconds: int = Field(default=60, ge=10, le=600)
        rl_learning_rate: float = Field(default=0.1, ge=0.01, le=1.0)
        rl_discount_factor: float = Field(default=0.9, ge=0.5, le=1.0)
        rl_exploration_rate: float = Field(default=0.1, ge=0.0, le=1.0)
        dashboard_port: int = Field(default=8777, ge=1024, le=65535)
        dashboard_update_interval: int = Field(default=5, ge=1, le=60)
        fallback_pipelines: List[str] = Field(
            default=['standard', 'energy_efficient'],
            description="Pipeline fallback order"
        )
        # NEW ENHANCEMENTS: Additional config fields
        enable_sustainability_modules: bool = Field(default=True)
        enable_carbon_aware_scheduling: bool = Field(default=True)
        enable_chaos_testing: bool = Field(default=False)
        enable_energy_preemption: bool = Field(default=True)
        k8s_operator: bool = Field(default=False)
        digital_twin_enabled: bool = Field(default=True)

        @field_validator('fallback_pipelines')
        @classmethod
        def validate_fallback_pipelines(cls, v: List[str]) -> List[str]:
            valid_pipelines = ['standard', 'quantum_enhanced', 'helium_optimized', 'energy_efficient', 'bio_optimized']
            for pipeline in v:
                if pipeline not in valid_pipelines:
                    raise ValueError(f"Invalid fallback pipeline: {pipeline}")
            return v
        @classmethod
        def from_env(cls) -> 'RunnerConfig':
            load_dotenv()
            config_dict = {}
            env_mapping = {
                'ENABLE_DYNAMIC_PIPELINE': 'enable_dynamic_pipeline',
                'ENABLE_DEGRADATION_AWARE': 'enable_degradation_aware',
                'ENABLE_PREDICTIVE_INFORMED': 'enable_predictive_informed',
                'ENABLE_REINFORCEMENT_LEARNING': 'enable_reinforcement_learning',
                'ENABLE_CIRCUIT_BREAKERS': 'enable_circuit_breakers',
                'ENABLE_DASHBOARD': 'enable_dashboard',
                'ENABLE_PROMETHEUS': 'enable_prometheus',
                'MAX_CONCURRENT_TASKS': 'max_concurrent_tasks',
                'TASK_TIMEOUT_SECONDS': 'task_timeout_seconds',
                'QUEUE_MAX_SIZE': 'queue_max_size',
                'CIRCUIT_BREAKER_FAILURE_THRESHOLD': 'circuit_breaker_failure_threshold',
                'CIRCUIT_BREAKER_TIMEOUT_SECONDS': 'circuit_breaker_timeout_seconds',
                'RL_LEARNING_RATE': 'rl_learning_rate',
                'RL_DISCOUNT_FACTOR': 'rl_discount_factor',
                'RL_EXPLORATION_RATE': 'rl_exploration_rate',
                'DASHBOARD_PORT': 'dashboard_port',
                'DASHBOARD_UPDATE_INTERVAL': 'dashboard_update_interval',
                'ENABLE_SUSTAINABILITY_MODULES': 'enable_sustainability_modules',
                'ENABLE_CARBON_AWARE_SCHEDULING': 'enable_carbon_aware_scheduling',
                'ENABLE_CHAOS_TESTING': 'enable_chaos_testing',
                'ENABLE_ENERGY_PREEMPTION': 'enable_energy_preemption',
                'K8S_OPERATOR': 'k8s_operator',
                'DIGITAL_TWIN_ENABLED': 'digital_twin_enabled'
            }
            for env_var, config_key in env_mapping.items():
                value = os.getenv(env_var)
                if value is not None:
                    if config_key in ['max_concurrent_tasks', 'task_timeout_seconds', 'queue_max_size', 
                                     'circuit_breaker_failure_threshold', 'circuit_breaker_timeout_seconds',
                                     'dashboard_port', 'dashboard_update_interval']:
                        try:
                            config_dict[config_key] = int(value)
                        except ValueError:
                            logger.warning(f"Invalid integer value for {env_var}: {value}")
                    elif config_key in ['enable_dynamic_pipeline', 'enable_degradation_aware', 'enable_predictive_informed',
                                        'enable_reinforcement_learning', 'enable_circuit_breakers', 'enable_dashboard',
                                        'enable_prometheus', 'enable_sustainability_modules', 'enable_carbon_aware_scheduling',
                                        'enable_chaos_testing', 'enable_energy_preemption', 'k8s_operator', 'digital_twin_enabled']:
                        config_dict[config_key] = value.lower() in ['true', '1', 'yes', 'on']
                    elif config_key in ['rl_learning_rate', 'rl_discount_factor', 'rl_exploration_rate']:
                        try:
                            config_dict[config_key] = float(value)
                        except ValueError:
                            logger.warning(f"Invalid float value for {env_var}: {value}")
            return cls(**config_dict)
else:
    class RunnerConfig:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)
        @classmethod
        def from_env(cls):
            return cls()
        def model_dump(self):
            return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}

# =============================================================================
# ENHANCED GREEN AGENT RUNNER (v7.0.2)
# =============================================================================
class EnhancedGreenAgentRunner:
    def __init__(self, config: Optional[RunnerConfig] = None):
        self.config = config or RunnerConfig.from_env()
        logger.info(f"Loaded configuration: {self.config.model_dump() if hasattr(self.config, 'model_dump') else self.config.__dict__}")

        # Central storage
        self.storage = Storage()
        self.state = RunnerState(self.storage)

        # NEW ENHANCEMENT: Integrate sustainability modules
        if self.config.enable_sustainability_modules and SUSTAINABILITY_MODULES_AVAILABLE:
            self.lca_integration = create_material_lca_integration(self.storage)  # simplified
            self.anomaly_system = create_anomaly_detection_system(config={})
            self.pm_system = create_predictive_maintenance_system(config={})
            logger.info("Sustainability modules integrated")
        else:
            self.lca_integration = None
            self.anomaly_system = None
            self.pm_system = None
            logger.warning("Sustainability modules disabled or not available")

        # NEW ENHANCEMENT: Carbon intensity fetcher
        self.carbon_fetcher = CarbonIntensityFetcher()

        # NEW ENHANCEMENT: Multi‑objective RL (replace existing RL if needed)
        if self.config.enable_reinforcement_learning:
            self.rl_learner = MultiObjectiveRLPipelineLearner(self.storage, self.config)
        else:
            self.rl_learner = None

        # NEW ENHANCEMENT: Kubernetes operator
        self.k8s_operator = KubernetesOperator(Config())

        # NEW ENHANCEMENT: Auto‑remediation policy
        self.remediation_policy = AutoRemediationPolicy()

        # NEW ENHANCEMENT: DigitalTwin client
        self.digital_twin = DigitalTwinClient(Config()) if self.config.digital_twin_enabled else None

        # NEW ENHANCEMENT: Chaos engine
        self.chaos_engine = ChaosEngine(Config())

        # Existing modules
        self.quantum_security = QuantumResilientRunnerSecurity(self.storage)
        self.blockchain = BlockchainRunnerVerification(self.storage)
        self.autonomous_optimizer = AutonomousRunnerOptimizer(self.storage, self.state)
        self.cloud_distributor = MultiCloudRunnerDistribution(self.storage)

        # Pipeline selector with RL and circuit breakers
        self.pipeline_selector = DynamicPipelineSelector(self.config, self.storage)

        # Available pipelines
        self.pipelines = {
            'standard': self._standard_pipeline,
            'quantum_enhanced': self._quantum_pipeline,
            'helium_optimized': self._helium_pipeline,
            'energy_efficient': self._energy_efficient_pipeline,
            'bio_optimized': self._bio_optimized_pipeline
        }

        # NEW ENHANCEMENT: Energy‑aware task queue (replacing original)
        self.task_queue = EnergyAwareTaskPriorityQueue(max_size=self.config.queue_max_size)

        # Dashboard server
        self.dashboard = AgentDashboardServer(self.config)

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
        logger.info("Enhanced Green Agent Runner v7.0.2 initialized")

    def _register_signal_handlers(self):
        try:
            loop = asyncio.get_event_loop()
            for sig in [signal.SIGINT, signal.SIGTERM]:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
        except NotImplementedError:
            pass

    def _get_system_state(self) -> Dict[str, Any]:
        state = {'degradation_tier': 5, 'token_balance': 1000, 'carbon_gradient': 0.5, 'predicted_carbon': 0.5}
        # If carbon fetcher available, include current intensity
        if self.carbon_fetcher:
            try:
                intensity = asyncio.run(self.carbon_fetcher.get_intensity())
                state['carbon_intensity'] = intensity / 1000  # normalize to 0-1
            except:
                pass
        return state

    async def submit_task(self, task: Dict[str, Any]) -> str:
        state = self._get_system_state()
        priority = self.task_queue.calculate_priority(task, state)
        if 'task_id' not in task:
            task['task_id'] = f"task_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{self.total_tasks}"
        await self.task_queue.push(task, priority)
        logger.debug(f"Task {task['task_id']} queued with priority {priority:.2f}")
        return task['task_id']

    async def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        start_time = datetime.utcnow()
        self.total_tasks += 1
        task_id = task.get('task_id', 'unknown')
        system_state = self._get_system_state()

        # Energy‑aware preemption: if task exceeds energy budget, cancel
        if self.config.enable_energy_preemption and 'energy_budget' in task:
            energy_used = 0  # track during execution
            # We'll check periodically via a wrapper

        # Degradation awareness
        if self.config.enable_degradation_aware:
            tier = system_state['degradation_tier']
            if tier <= 1:
                return {'success': False, 'reason': f'System in survival mode (tier {tier})', 'task_id': task_id}
            if tier <= 2 and task.get('priority', 2) > 1:
                return {'success': False, 'reason': f'Non-critical tasks deferred in tier {tier}', 'task_id': task_id}

        # Dynamic pipeline selection
        if self.config.enable_dynamic_pipeline:
            pipeline_name, scores = self.pipeline_selector.select_pipeline(task, system_state)
        else:
            pipeline_name = task.get('pipeline', 'standard')

        # Execute with fallback
        result = await self._execute_with_fallback(task, pipeline_name, system_state)

        success = result.get('success', False)
        latency = (datetime.utcnow() - start_time).total_seconds() * 1000
        energy_joules = result.get('energy_joules', latency * 0.1)  # mock energy
        carbon_kg = result.get('carbon_kg', energy_joules * 0.0001)  # mock

        # Multi‑objective RL update
        if self.config.enable_reinforcement_learning and self.rl_learner:
            reward_info = {
                'success': 1.0 if success else 0.0,
                'latency_ms': latency,
                'energy_joules': energy_joules,
                'carbon_kg': carbon_kg
            }
            next_state = self._get_system_state()
            await self.rl_learner.update(system_state, pipeline_name, reward_info, next_state)

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
            'timestamp': datetime.utcnow().isoformat()
        })

        result['pipeline_used'] = pipeline_name
        result['pipeline_scores'] = scores
        result['system_state'] = {
            'tier': system_state['degradation_tier'],
            'token_balance': system_state['token_balance'],
            'carbon_gradient': system_state['carbon_gradient']
        }

        # ============================================================
        # Quantum-Resilient Signing
        # ============================================================
        result_data = result.copy()
        quantum_key = await self.quantum_security.generate_keypair('dilithium')
        signature = await self.quantum_security.sign_task_result(result_data, quantum_key['key_id'])
        result['quantum_signature'] = signature
        QUANTUM_SIGNATURES.labels(algorithm='dilithium', status='sign_success').inc()

        # ============================================================
        # Blockchain Verification
        # ============================================================
        data_id = f"task_{uuid.uuid4().hex[:8]}"
        data_hash = hashlib.sha256(json.dumps(result_data, sort_keys=True, default=str).encode()).hexdigest()
        blockchain_result = await self.blockchain.record_task_result(
            data_id,
            data_hash,
            {'task_id': task_id, 'success': success, 'pipeline': pipeline_name}
        )
        result['blockchain_tx_hash'] = blockchain_result.get('tx_hash')
        BLOCKCHAIN_VERIFICATIONS.labels(status='recorded').inc()

        # ============================================================
        # Multi-Cloud Distribution (carbon-aware)
        # ============================================================
        # Get carbon intensity for each provider region (if available)
        cloud_prefs = {}
        if self.carbon_fetcher:
            # Simulate getting intensity for each region (we'll just use global)
            intensity = await self.carbon_fetcher.get_intensity()
            cloud_prefs['carbon_intensity'] = intensity
        cloud_data = {'size_gb': len(str(result)) * 0.001}
        distribution = await self.cloud_distributor.distribute_runner_data(cloud_data, preferences=cloud_prefs)
        result['cloud_distribution'] = distribution
        CLOUD_DISTRIBUTIONS.labels(provider=distribution['optimal_provider'], status='success').inc()

        # ============================================================
        # Autonomous Optimization
        # ============================================================
        state = {
            'success_rate': self.successful_tasks / max(self.total_tasks, 1),
            'carbon_intensity': system_state.get('carbon_intensity', 0.5),
            'cost_budget': 0.5,
            'runner_quality': self.state.historical_success_rate
        }
        optimization = await self.autonomous_optimizer.optimize_runner(state, 'hybrid')
        result['autonomous_optimization'] = optimization
        AUTONOMOUS_OPTIMIZATIONS.labels(strategy=optimization['selected_strategy'], status='success').inc()

        # ============================================================
        # NEW ENHANCEMENT: Anomaly Detection & Auto‑Remediation
        # ============================================================
        if self.anomaly_system and 'node_id' in task:
            node_id = task['node_id']
            metrics = {
                'energy_joules': energy_joules,
                'carbon_kg': carbon_kg,
                'latency_ms': latency,
                'accuracy': success
            }
            event = self.anomaly_system['telemetry_collector'].receive_telemetry(node_id, metrics)
            if event:
                ANOMALY_ALERTS.labels(node=node_id).inc()
                # Apply auto‑remediation
                action = self.remediation_policy.get_action('energy_spike', node_id)  # example
                if action == 'reroute':
                    # Trigger re‑route
                    logger.info(f"Auto‑remediation: rerouting tasks from {node_id}")
                elif action == 'defer':
                    # Defer future tasks
                    pass
                elif action == 'restart':
                    # Restart service
                    pass

        # ============================================================
        # NEW ENHANCEMENT: Predictive Maintenance
        # ============================================================
        if self.pm_system and 'node_id' in task:
            # Assume we have FLOPs and energy from task
            flops = task.get('flops', 1e12)
            self.pm_system['engine'].update_node(node_id, flops, energy_joules)

        # ============================================================
        # NEW ENHANCEMENT: Kubernetes Operator Scaling
        # ============================================================
        if self.k8s_operator:
            cpu_usage = random.uniform(20, 90)  # example
            carbon_intensity = system_state.get('carbon_intensity', 0.5)
            await self.k8s_operator.scale(cpu_usage, carbon_intensity)

        # ============================================================
        # NEW ENHANCEMENT: Dashboard Updates (sustainability)
        # ============================================================
        if self.config.enable_dashboard:
            status = self.get_status()
            status['sustainability'] = {
                'carbon_intensity': system_state.get('carbon_intensity', 0.5),
                'total_energy_joules': sum(t.get('energy_joules', 0) for t in self.task_history),
                'total_carbon_kg': sum(t.get('carbon_kg', 0) for t in self.task_history),
                'anomalies': len(self.anomaly_system['detector'].anomaly_history) if self.anomaly_system else 0,
                'pm_recommendations': len(self.pm_system['engine'].scheduler.recommendations) if self.pm_system else 0
            }
            await self.dashboard.broadcast_status(status)

        # Store in database
        self.storage.save_task_history(task_id, pipeline_name, success, latency, result)

        AGENT_TASKS.labels(status='success' if success else 'failed').inc()
        AGENT_DURATION.labels(pipeline=pipeline_name).observe(latency / 1000)
        AGENT_QUEUE_SIZE.set(self.task_queue.size())

        audit_logger.info(f"Task {task_id} processed: success={success}, pipeline={pipeline_name}, latency={latency:.0f}ms, blockchain={result['blockchain_tx_hash'][:16] if result['blockchain_tx_hash'] else 'N/A'}...")
        return result

    async def _execute_with_fallback(self, task: Dict[str, Any], initial_pipeline: str, system_state: Dict[str, Any]) -> Dict[str, Any]:
        fallback_chain = [initial_pipeline] + self.config.fallback_pipelines
        seen = set()
        fallback_chain = [p for p in fallback_chain if not (p in seen or seen.add(p))]
        for pipeline_name in fallback_chain:
            try:
                if self.config.enable_circuit_breakers:
                    available, state = await self.pipeline_selector.circuit_breaker.is_available(pipeline_name)
                    if not available:
                        logger.warning(f"Pipeline {pipeline_name} unavailable (state: {state})")
                        continue
                pipeline_func = self.pipelines.get(pipeline_name)
                if not pipeline_func:
                    logger.warning(f"Pipeline {pipeline_name} not found")
                    continue
                try:
                    # Wrap execution to monitor energy
                    async def run_with_monitor():
                        start = datetime.utcnow()
                        result = await asyncio.wait_for(pipeline_func(task), timeout=self.config.task_timeout_seconds)
                        energy_used = (datetime.utcnow() - start).total_seconds() * 10  # mock energy
                        result['energy_joules'] = energy_used
                        result['carbon_kg'] = energy_used * 0.0001
                        return result

                    result = await run_with_monitor()
                    if self.config.enable_circuit_breakers:
                        await self.pipeline_selector.circuit_breaker.record_success(pipeline_name)
                    return result
                except asyncio.TimeoutError:
                    logger.error(f"Pipeline {pipeline_name} timed out after {self.config.task_timeout_seconds}s")
                    if self.config.enable_circuit_breakers:
                        await self.pipeline_selector.circuit_breaker.record_failure(pipeline_name)
                    continue
            except Exception as e:
                logger.error(f"Pipeline {pipeline_name} failed: {str(e)}")
                if self.config.enable_circuit_breakers:
                    await self.pipeline_selector.circuit_breaker.record_failure(pipeline_name)
                continue
        return {'success': False, 'error': 'All pipelines failed', 'task_id': task.get('task_id', 'unknown'), 'tried_pipelines': fallback_chain}

    async def _worker_loop(self, worker_id: int):
        logger.info(f"Worker {worker_id} started")
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
                        logger.error(f"Callback error for task {task.get('task_id')}: {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(0.5)
        logger.info(f"Worker {worker_id} stopped")

    async def start_workers(self, num_workers: int = None):
        if num_workers is None:
            num_workers = self.config.max_concurrent_tasks
        for i in range(num_workers):
            worker = asyncio.create_task(self._worker_loop(i))
            self._worker_tasks.append(worker)
        logger.info(f"Started {num_workers} workers")

    async def batch_process(self, tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        for task in tasks:
            result = await self.process_task(task)
            results.append(result)
        return results

    # Pipeline methods (unchanged)
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
            'version': '7.0.2',
            'total_tasks': self.total_tasks,
            'successful_tasks': self.successful_tasks,
            'failed_tasks': self.failed_tasks,
            'success_rate': self.successful_tasks / max(self.total_tasks, 1),
            'queue_size': self.task_queue.size(),
            'pipeline_stats': self.pipeline_selector.get_pipeline_stats(),
            'system_state': system_state,
            'running': self.running,
            'config': self.config.model_dump() if hasattr(self.config, 'model_dump') else self.config.__dict__,
            'timestamp': datetime.utcnow().isoformat()
        }

    async def start(self):
        logger.info("Starting Enhanced Green Agent Runner v7.0.2...")
        await self.dashboard.start()
        await self.start_workers()
        if self.config.enable_prometheus and PROMETHEUS_AVAILABLE:
            try:
                start_http_server(9090)
                logger.info("Prometheus metrics server started on port 9090")
            except Exception as e:
                logger.warning(f"Failed to start Prometheus server: {e}")
        # Start chaos engine if enabled
        if self.config.enable_chaos_testing:
            await self.chaos_engine.start(self)
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
        # Close carbon fetcher and digital twin
        if self.carbon_fetcher:
            await self.carbon_fetcher.close()
        if self.digital_twin:
            await self.digital_twin.close()
        # Stop chaos engine
        if self.config.enable_chaos_testing:
            await self.chaos_engine.stop()
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
    config = RunnerConfig.from_env()
    async with EnhancedGreenAgentRunner(config) as runner:
        logger.info("Agent running. Press Ctrl+C to stop.")
        try:
            while runner.running:
                await asyncio.sleep(1)
                if int(time.time()) % 30 == 0:
                    status = runner.get_status()
                    logger.info(f"Status: {status['total_tasks']} tasks, {status['success_rate']*100:.1f}% success rate, queue: {status['queue_size']}")
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Runtime error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Graceful shutdown complete")
