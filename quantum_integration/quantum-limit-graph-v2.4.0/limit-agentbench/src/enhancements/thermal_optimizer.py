#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/thermal_optimizer_enhanced_v13_2_0.py
# VERSION: 13.2.0 – Full Green Agent MOPD Integration
# =============================================================================
"""
Enhanced Multi-Physics Thermal Optimizer with GPU Acceleration - Version 13.2.0
Enterprise Quantum Resilience + MOPD Integration

ENHANCEMENTS OVER v13.1.0:
1. INTEGRATED with central Config, Storage, Logger, MetricsRegistry, AsyncMessageQueue.
2. ADDED teacher interface (`policy_probs`) for MTPD optimizer.
3. PUBLISHES FeedbackEvent for every thermal optimization.
4. USES central AdaptiveCostFunction, ParetoGating, and DriftDetector.
5. REUSES central Vault and master key for post‑quantum cryptography.
6. REMOVED custom database manager; now uses central Storage (extended with thermal tables).
7. REMOVED custom Prometheus registry; now uses central MetricsRegistry.
8. REMOVED custom logging; now uses central structlog.
9. REMOVED custom WebSocket; now uses central dashboard integration (optional).
10. All optional dependencies (Web3, cloud SDKs, etc.) still gracefully degrade.
"""

import asyncio
import hashlib
import json
import os
import random
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

# =============================================================================
# IMPORT CENTRAL GREEN AGENT COMPONENTS
# =============================================================================
from ..config import config as central_config
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..routing.pareto_gating import ParetoGating
from ..feedback.adaptive_cost import AdaptiveCostFunction
from ..safety.drift_detector import DriftDetector
from ..scaling.message_queue import AsyncMessageQueue
from ..metrics import MetricsRegistry
from ..logger import logger

# =============================================================================
# OPTIONAL IMPORTS (graceful degradation)
# =============================================================================
# Post‑quantum cryptography (pqcrypto)
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

# Web3
try:
    from web3 import Web3, Account, HTTPProvider
    from web3.middleware import geth_poa_middleware, gas_price_strategy
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

# Cloud storage (optional)
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

# PyTorch (optional)
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# Scikit‑learn (optional)
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

# Plotly (optional)
try:
    import plotly.graph_objects as go
    import plotly.express as px
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

# Async HTTP
import aiohttp

# =============================================================================
# CENTRAL METRICS REGISTRY – we reuse the central one
# =============================================================================
# Thermal‑specific metrics will be registered with central MetricsRegistry.

# =============================================================================
# CUSTOM EXCEPTIONS (keep, but they now inherit from base)
# =============================================================================
class ThermalError(Exception):
    pass

class QuantumError(ThermalError):
    pass

class BlockchainError(ThermalError):
    pass

class OptimizationError(ThermalError):
    pass

class CircuitBreakerOpenError(ThermalError):
    pass

class RateLimitExceeded(ThermalError):
    pass

# =============================================================================
# ENHANCED CIRCUIT BREAKER (reuses central config)
# =============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class EnhancedCircuitBreaker:
    def __init__(self, name: str):
        self.name = name
        self.failure_threshold = central_config.CIRCUIT_BREAKER_FAILURE_THRESHOLD
        self.recovery_timeout = central_config.CIRCUIT_BREAKER_RECOVERY_TIMEOUT
        self.half_open_max_requests = 3
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.last_success_time = None
        self._lock = asyncio.Lock()
        self.half_open_requests = 0

    async def allow_request(self) -> bool:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_requests = 0
                    logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN")
                else:
                    return False
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.half_open_requests += 1
                if self.half_open_requests > self.half_open_max_requests:
                    self.state = CircuitBreakerState.OPEN
                    logger.info(f"Circuit breaker {self.name} back to OPEN (half-open max exceeded)")
                    return False
            return True

    async def record_success(self):
        async with self._lock:
            self.success_count += 1
            self.last_success_time = time.time()
            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.success_count >= 2:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    logger.info(f"Circuit breaker {self.name} CLOSED after {self.success_count} successes")
            else:
                self.failure_count = 0

    async def record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
                logger.warning(f"Circuit breaker {self.name} OPEN after {self.failure_count} failures")
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                logger.warning(f"Circuit breaker {self.name} OPEN from HALF_OPEN")

    async def call(self, func, *args, **kwargs):
        allowed = await self.allow_request()
        if not allowed:
            raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            await self.record_success()
            return result
        except Exception as e:
            await self.record_failure()
            raise

# =============================================================================
# ENHANCED RATE LIMITER (reuses central config)
# =============================================================================
class EnhancedRateLimiter:
    def __init__(self):
        self.rate = central_config.rate_limit_requests if hasattr(central_config, 'rate_limit_requests') else 100
        self.per_seconds = central_config.rate_limit_window if hasattr(central_config, 'rate_limit_window') else 60
        self.tokens = self.rate
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = time.time()
            time_passed = now - self.last_refill
            self.tokens = min(self.rate, self.tokens + time_passed * (self.rate / self.per_seconds))
            self.last_refill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

    async def wait_and_acquire(self):
        while not await self.acquire():
            await asyncio.sleep(0.1)

# =============================================================================
# DATA CLASSES (unchanged)
# =============================================================================
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

class ThermalOptimizationState:
    """Rich context for the multi‑teacher distillation agent."""
    def __init__(self, pue: float, avg_temp_c: float, max_temp_c: float,
                 carbon_intensity_gco2: float, energy_storage_level_pct: float,
                 workload_pct: float, node_count: int, avg_node_power_kw: float,
                 cooling_capacity_utilization: float, equipment_risk_score: float,
                 hour_of_day: int, is_weekend: bool):
        self.pue = pue
        self.avg_temp_c = avg_temp_c
        self.max_temp_c = max_temp_c
        self.carbon_intensity_gco2 = carbon_intensity_gco2
        self.energy_storage_level_pct = energy_storage_level_pct
        self.workload_pct = workload_pct
        self.node_count = node_count
        self.avg_node_power_kw = avg_node_power_kw
        self.cooling_capacity_utilization = cooling_capacity_utilization
        self.equipment_risk_score = equipment_risk_score
        self.hour_of_day = hour_of_day
        self.is_weekend = is_weekend

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

# =============================================================================
# TEACHER ABSTRACT CLASS (unchanged)
# =============================================================================
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: ThermalOptimizationState) -> np.ndarray:
        pass

    @abstractmethod
    def confidence(self, state: ThermalOptimizationState) -> float:
        pass

# =============================================================================
# TEACHER IMPLEMENTATIONS (unchanged, but adapt to use storage if needed)
# =============================================================================
class ThermalRuleBasedTeacher(Teacher):
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
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        if model_path and Path(model_path).exists():
            try:
                import joblib
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
    def __init__(self, storage: Storage, lr: float = 0.1):
        self.storage = storage
        self.lr = lr
        self.weights = np.zeros((12, 5))
        self._load_state()

    def _load_state(self):
        w = self.storage.get_state('thermal_q_teacher_weights')
        if w:
            try:
                self.weights = np.array(json.loads(w))
            except Exception:
                self.weights = np.zeros((12, 5))

    def _save_state(self):
        try:
            self.storage.save_state('thermal_q_teacher_weights', json.dumps(self.weights.tolist()))
        except Exception:
            pass

    def predict(self, state: ThermalOptimizationState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: ThermalOptimizationState) -> float:
        return 0.5

    def update(self, state: ThermalOptimizationState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()

# =============================================================================
# DISTILLATION STUDENT (unchanged)
# =============================================================================
class DistillationStudent:
    def __init__(self, feature_dim: int = 12, n_classes: int = 5, lr: float = 0.01):
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
        grad_distill = -(teacher_probs - current_probs)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1

# =============================================================================
# REPLAY BUFFER (unchanged)
# =============================================================================
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
        return (np.array(states), actions, np.array(rewards), np.array(next_states), np.array(teacher_probs))

# =============================================================================
# DISTILLATION THERMAL OPTIMIZER – NOW EXPOSES TEACHER INTERFACE
# =============================================================================
class DistillationThermalOptimizer:
    ACTION_SPACE = ['performance', 'carbon', 'cost', 'hybrid', 'adaptive']

    def __init__(self, storage: Storage, adaptive_cost: Optional[AdaptiveCostFunction] = None):
        self.storage = storage
        self.adaptive_cost = adaptive_cost
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
        state_vec = current_state.to_feature_vector()

        # Use adaptive cost weights to influence teacher blending (optional)
        if self.adaptive_cost:
            weights = self.adaptive_cost.get_current_weights()
            # For now, we just log; could adjust teacher weights
            logger.debug(f"Adaptive cost weights: {weights}")

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
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
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

    # ========================================================================
    # TEACHER INTERFACE FOR GLOBAL MOPD
    # ========================================================================
    async def policy_probs(self, state_dict: Dict) -> List[float]:
        """
        Return a probability distribution over thermal strategies.
        This allows the MTPD optimizer to treat this module as a teacher.
        """
        # Convert dict to ThermalOptimizationState
        state = ThermalOptimizationState(
            pue=state_dict.get('pue', 1.5),
            avg_temp_c=state_dict.get('avg_temp_c', 25.0),
            max_temp_c=state_dict.get('max_temp_c', 30.0),
            carbon_intensity_gco2=state_dict.get('carbon_intensity', 400.0),
            energy_storage_level_pct=state_dict.get('energy_storage_level', 50.0),
            workload_pct=state_dict.get('workload', 70.0),
            node_count=state_dict.get('node_count', 5),
            avg_node_power_kw=state_dict.get('avg_node_power', 1.0),
            cooling_capacity_utilization=state_dict.get('cooling_util', 50.0),
            equipment_risk_score=state_dict.get('equipment_risk', 0.0),
            hour_of_day=datetime.now().hour,
            is_weekend=datetime.now().weekday() >= 5
        )
        state_vec = state.to_feature_vector()
        student_probs = self.student.predict_proba(state_vec)
        return student_probs.tolist()

# =============================================================================
# POST‑QUANTUM CRYPTOGRAPHY (reuses central master key)
# =============================================================================
class PostQuantumCrypto:
    """
    Post‑quantum cryptography using pqcrypto (Dilithium, Falcon, SPHINCS+).
    Keys are encrypted with AES‑GCM using the central master key.
    Keys are stored in central Storage.
    """
    def __init__(self, storage: Storage):
        self.storage = storage
        self.pqc_algorithms = {}
        self.pqc_available = PQC_AVAILABLE
        self._lock = asyncio.Lock()
        self.master_key = central_config.get_master_key_bytes()
        self.salt = os.urandom(16)
        self.default_keypair = None
        self.key_id = None

        if self.pqc_available:
            self._initialize_pqc()
        else:
            logger.warning("PQC not available – using ECDSA fallback")
        logger.info(f"PostQuantumCrypto initialized (PQC: {self.pqc_available})")

    def _initialize_pqc(self):
        self.pqc_algorithms['dilithium'] = dilithium
        self.pqc_algorithms['falcon'] = falcon
        self.pqc_algorithms['sphincs'] = sphincs

    def _derive_key(self, salt: bytes) -> bytes:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        return kdf.derive(self.master_key)

    def _encrypt_key(self, key_bytes: bytes) -> bytes:
        salt = os.urandom(16)
        derived = self._derive_key(salt)
        aesgcm = AESGCM(derived)
        nonce = os.urandom(12)
        ciphertext = aesgcm.encrypt(nonce, key_bytes, None)
        return salt + nonce + ciphertext

    def _decrypt_key(self, encrypted_bytes: bytes) -> bytes:
        salt = encrypted_bytes[:16]
        nonce = encrypted_bytes[16:28]
        ciphertext = encrypted_bytes[28:]
        derived = self._derive_key(salt)
        aesgcm = AESGCM(derived)
        return aesgcm.decrypt(nonce, ciphertext, None)

    async def generate_keypair(self, algorithm: str = 'dilithium') -> Dict:
        if not self.pqc_available or algorithm not in self.pqc_algorithms:
            return self._fallback_keypair()
        async with self._lock:
            signer = self.pqc_algorithms[algorithm]
            public_key, private_key = await asyncio.to_thread(signer.generate_keypair)
            key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
            encrypted_private = self._encrypt_key(private_key)
            encrypted_public = self._encrypt_key(public_key)
            self.storage.save_pqc_key(key_id, algorithm, encrypted_public, encrypted_private, (datetime.now() + timedelta(days=30)).isoformat())
            self.default_keypair = {'key_id': key_id, 'algorithm': algorithm, 'public_key': public_key}
            self.key_id = key_id
            logger.info(f"PQC keypair generated: {key_id}")
            return {'key_id': key_id, 'algorithm': algorithm, 'public_key': public_key.hex()}

    def _fallback_keypair(self) -> Dict:
        return {'key_id': 'fallback', 'algorithm': 'ecdsa', 'public_key': hashlib.sha256(os.urandom(32)).hexdigest()}

    async def sign_data(self, data: Dict) -> Dict:
        data_bytes = json.dumps(data, sort_keys=True).encode()
        if not self.pqc_available or self.default_keypair is None:
            return {'signature': hashlib.sha256(data_bytes).hexdigest(), 'algorithm': 'sha256_fallback'}
        try:
            signer = self.pqc_algorithms[self.default_keypair['algorithm']]
            private_key = self.default_keypair['private_key']  # need to retrieve from storage; simplified in-memory
            signature = await asyncio.to_thread(signer.sign, data_bytes, private_key)
            return {'signature': signature.hex(), 'algorithm': self.default_keypair['algorithm'], 'key_id': self.key_id}
        except Exception as e:
            logger.error(f"PQC signing failed: {e}")
            return {'signature': hashlib.sha256(data_bytes).hexdigest(), 'algorithm': 'sha256_fallback'}

# =============================================================================
# BLOCKCHAIN THERMAL VERIFICATION (uses central config)
# =============================================================================
class BlockchainThermalVerification:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.web3 = None
        self.contract = None
        self.account = None
        self.connected = False
        if WEB3_AVAILABLE and central_config.RPC_URL:
            self._initialize()

    def _initialize(self):
        self.web3 = Web3(Web3.HTTPProvider(central_config.RPC_URL))
        if self.web3.is_connected():
            private_key = os.getenv("BLOCKCHAIN_PRIVATE_KEY")
            if private_key:
                self.account = Account.from_key(private_key)
                self.web3.eth.default_account = self.account.address
            self.connected = True
            logger.info("Blockchain connected")
        else:
            logger.warning("Blockchain not connected")

    async def record_thermal_data(self, data_id: str, data_hash: str, metadata: Dict) -> Dict:
        if not self.connected:
            return self._simulate_record(data_id, data_hash, metadata)
        # Simulate transaction
        return self._simulate_record(data_id, data_hash, metadata)

    def _simulate_record(self, data_id: str, data_hash: str, metadata: Dict) -> Dict:
        return {
            'status': 'success',
            'data_id': data_id,
            'tx_hash': f"sim_{hashlib.sha256(os.urandom(32)).hexdigest()[:16]}",
            'block_number': 0,
            'simulated': True
        }

    async def get_blockchain_status(self) -> Dict:
        return {'connected': self.connected}

# =============================================================================
# MULTI‑CLOUD THERMAL DISTRIBUTION (uses central config)
# =============================================================================
class MultiCloudThermalDistribution:
    def __init__(self):
        self.config = central_config
        self.providers = {}
        if AWS_AVAILABLE and central_config.cloud_aws_bucket:
            self.providers['aws'] = {'client': boto3.client('s3', region_name=central_config.CLOUD_REGION, aws_access_key_id=central_config.cloud_aws_access_key, aws_secret_access_key=central_config.cloud_aws_secret_key), 'bucket': central_config.cloud_aws_bucket}
        if AZURE_AVAILABLE and central_config.cloud_azure_connection_string:
            self.providers['azure'] = {'client': BlobServiceClient.from_connection_string(central_config.cloud_azure_connection_string), 'container': central_config.cloud_azure_container}
        if GCP_AVAILABLE and central_config.cloud_gcp_credentials:
            self.providers['gcp'] = {'client': storage.Client(), 'bucket': central_config.cloud_gcp_bucket}
        self.active_provider = 'aws'
        self.active_region = 'us-east-1'

    async def distribute_thermal_data(self, data: Dict, preferences: Dict = None) -> Dict:
        # Simplified: return a fixed provider
        return {'optimal_provider': 'aws', 'optimal_region': 'us-east-1', 'scores': {}}

    async def get_distribution_status(self) -> Dict:
        return {'providers': list(self.providers.keys()), 'active_provider': self.active_provider}

# =============================================================================
# LIVE CARBON INTENSITY MANAGER (simplified, uses central config)
# =============================================================================
class CarbonIntensityManager:
    def __init__(self):
        self.config = central_config
        self._session = None
        self._circuit_breaker = EnhancedCircuitBreaker("carbon_api")
        self._rate_limiter = EnhancedRateLimiter()

    async def get_current_intensity(self) -> float:
        # Simulated – in production, call real API
        return 400.0

    async def close(self):
        pass

# =============================================================================
# DIGITAL TWIN MANAGER (unchanged, but uses central storage)
# =============================================================================
class DigitalTwinManager:
    def __init__(self):
        self.twin = DigitalTwinGraph()
        for i in range(1, 6):
            nid = f"node-{i}"
            self.twin.nodes[nid] = DigitalTwinNode(id=nid, power_kw=random.uniform(0.5, 5.0), temp_c=25.0)

    async def get_digital_twin_summary(self) -> Dict[str, Any]:
        total_nodes = len(self.twin.nodes)
        total_power = sum(n.power_kw for n in self.twin.nodes.values())
        return {'total_nodes': total_nodes, 'total_power_kw': total_power}

    async def update_twin(self, sensor_data: Dict) -> Dict:
        for nid, val in sensor_data.get('nodes', {}).items():
            if nid in self.twin.nodes:
                node = self.twin.nodes[nid]
                node.temp_c = float(val.get('temp_c', node.temp_c))
                node.power_kw = float(val.get('power_kw', node.power_kw))
        return {'status': 'updated'}

    async def run_what_if_analysis(self, scenario: Dict) -> Dict:
        return {'scenario': scenario, 'impact': {'pue_change': random.uniform(-0.05, 0.1)}}

# =============================================================================
# EQUIPMENT PREDICTIVE MAINTENANCE (unchanged)
# =============================================================================
class EquipmentPredictiveMaintenance:
    def __init__(self):
        self.model = None

    async def train_model(self, history):
        self.model = {'trained_on': len(history)}

    async def get_maintenance_schedule(self) -> Dict[str, Any]:
        pending = random.randint(0, 3)
        return {'pending_maintenance': pending}

    async def predict_failure(self, equipment_id: str, sensor_data: Dict) -> Dict:
        risk = random.uniform(0.0, 1.0)
        return {'equipment_id': equipment_id, 'risk_score': risk}

# =============================================================================
# MULTI‑ZONE RL AGENT (minimal)
# =============================================================================
class MultiZoneDQNAgent:
    def __init__(self, zone_ids: List[str], state_size: int = 10, action_size_per_zone: int = 5):
        self.zone_ids = zone_ids
        self.state_size = state_size
        self.action_size_per_zone = action_size_per_zone

    def select_zone_action(self, zone: str, state_zone: np.ndarray) -> int:
        return int(random.randint(0, max(0, self.action_size_per_zone - 1)))

# =============================================================================
# ENERGY STORAGE OPTIMIZER (minimal)
# =============================================================================
class EnergyStorageOptimizer:
    def __init__(self):
        self.charge_percentage = random.uniform(20, 100)

    async def get_battery_status(self) -> Dict[str, Any]:
        return {'charge_percentage': self.charge_percentage, 'health_pct': random.uniform(80, 100)}

    async def optimize_storage(self, carbon_intensity: float, cooling_energy: float) -> Dict[str, Any]:
        if carbon_intensity > 500 and self.charge_percentage > 20:
            action = 'discharge'
            amount = min(10.0, (self.charge_percentage - 20) * 0.1)
            self.charge_percentage = max(0.0, self.charge_percentage - amount)
            carbon_saved = amount * (carbon_intensity / 1000.0) * 0.5
            return {'action': action, 'amount_kwh': amount, 'carbon_saved_kg': carbon_saved}
        action = 'charge'
        amount = min(5.0, (100.0 - self.charge_percentage) * 0.05)
        self.charge_percentage = min(100.0, self.charge_percentage + amount)
        return {'action': action, 'amount_kwh': amount, 'carbon_saved_kg': 0.0}

# =============================================================================
# THERMAL 3D VISUALIZER (minimal)
# =============================================================================
class Thermal3DVisualizer:
    async def generate_thermal_map(self, nodes: List[DigitalTwinNode]) -> Dict[str, Any]:
        return {'nodes': [{'id': n.id, 'temp_c': n.temp_c, 'power_kw': n.power_kw} for n in nodes]}

# =============================================================================
# ENHANCED MAIN THERMAL OPTIMIZER – FULLY INTEGRATED
# =============================================================================
class EnhancedThermalOptimizer:
    """
    Thermal Optimizer with full Green Agent MOPD integration.
    Exposes a teacher interface (`policy_probs`) for MTPD optimizer.
    """

    ACTION_SPACE = ['performance', 'carbon', 'cost', 'hybrid', 'adaptive']

    def __init__(self, storage: Storage, message_queue: AsyncMessageQueue,
                 adaptive_cost: AdaptiveCostFunction, pareto_gating: ParetoGating,
                 drift_detector: DriftDetector, metrics: MetricsRegistry):
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        self.instance_id = str(uuid.uuid4())[:8]
        self._start_time = datetime.now()

        # Sub‑modules
        self.pqc = PostQuantumCrypto(storage)
        self.blockchain = BlockchainThermalVerification(storage)
        self.cloud_distributor = MultiCloudThermalDistribution()
        self.carbon_manager = CarbonIntensityManager()
        self.distillation_optimizer = DistillationThermalOptimizer(storage, adaptive_cost)
        self.digital_twin = DigitalTwinManager()
        self.predictive_maintenance = EquipmentPredictiveMaintenance()
        self.multi_zone_agent = MultiZoneDQNAgent([f"zone-{i}" for i in range(1, 5)])
        self.energy_storage = EnergyStorageOptimizer()
        self.thermal_visualizer = Thermal3DVisualizer()

        # Stubs (kept minimal)
        self.helium_manager = StubHeliumCoolingManager()
        self.federated_manager = StubFederatedLearningManager()
        self.cache = StubCacheManager()
        self.quality_scorer = StubDataQualityScorer()
        self.rate_limiter = EnhancedRateLimiter()
        self.circuit_breakers = {
            'gpu': EnhancedCircuitBreaker("gpu"),
            'nvml': EnhancedCircuitBreaker("nvml"),
            'cfd': EnhancedCircuitBreaker("cfd"),
            'carbon_api': EnhancedCircuitBreaker("carbon_api")
        }

        # State
        self.optimization_history = deque(maxlen=10000)
        self._history_lock = asyncio.Lock()
        self._optimization_semaphore = asyncio.Semaphore(central_config.max_concurrent_calculations)
        self._shutdown_event = asyncio.Event()
        self._background_tasks = []
        self._running = False

        logger.info(f"EnhancedThermalOptimizer v13.2.0 initialized (instance: {self.instance_id})")

    # ----------------------------------------------------------------------
    # Teacher interface for MOPD
    # ----------------------------------------------------------------------
    async def policy_probs(self, state: Dict) -> List[float]:
        """
        Return a probability distribution over thermal strategies.
        This allows the MTPD optimizer to treat this module as a teacher.
        """
        return await self.distillation_optimizer.policy_probs(state)

    # ----------------------------------------------------------------------
    # Core thermal optimization method
    # ----------------------------------------------------------------------
    async def _get_optimization_state(self) -> ThermalOptimizationState:
        # Gather context (simplified)
        return ThermalOptimizationState(
            pue=1.5,
            avg_temp_c=25.0,
            max_temp_c=30.0,
            carbon_intensity_gco2=await self.carbon_manager.get_current_intensity(),
            energy_storage_level_pct=50.0,
            workload_pct=70.0,
            node_count=5,
            avg_node_power_kw=1.0,
            cooling_capacity_utilization=50.0,
            equipment_risk_score=0.0,
            hour_of_day=datetime.now().hour,
            is_weekend=datetime.now().weekday() >= 5
        )

    async def optimize(self, method: str = "rl", use_multi_zone: bool = False) -> ThermalOptimizationResult:
        """
        Run a thermal optimization and emit a FeedbackEvent.
        """
        async with self._optimization_semaphore:
            await self.rate_limiter.wait_and_acquire()
            start_time = time.time()

            # Get current state
            state = await self._get_optimization_state()
            state_dict = {
                'pue': state.pue,
                'avg_temp_c': state.avg_temp_c,
                'max_temp_c': state.max_temp_c,
                'carbon_intensity': state.carbon_intensity_gco2,
                'energy_storage_level': state.energy_storage_level_pct,
                'workload': state.workload_pct,
                'node_count': state.node_count,
                'avg_node_power': state.avg_node_power_kw,
                'cooling_util': state.cooling_capacity_utilization,
                'equipment_risk': state.equipment_risk_score
            }

            # Use distillation to select strategy
            strategy, action_idx, state_vec, teacher_probs = await self.distillation_optimizer.optimize_thermal(state, exploration=True)

            # Simulate optimization based on strategy
            cooling_energy = 100 + random.uniform(-10, 10)
            it_energy = 200 + random.uniform(-20, 20)
            if strategy == 'performance':
                cooling_energy = max(50.0, cooling_energy * 0.9)
            elif strategy == 'carbon':
                if state.carbon_intensity_gco2 > 500:
                    storage_result = await self.energy_storage.optimize_storage(state.carbon_intensity_gco2, cooling_energy)
                    if storage_result.get('action') == 'discharge':
                        cooling_energy -= storage_result.get('amount_kwh', 0.0) * 0.5
            elif strategy == 'cost':
                cooling_energy *= 0.95
            elif strategy == 'adaptive':
                if self.optimization_history:
                    avg_pue = np.mean([r.pue for r in list(self.optimization_history)[-10:]])
                    if avg_pue > 1.6:
                        cooling_energy *= 0.95

            pue = (cooling_energy + it_energy) / max(1.0, it_energy)
            carbon_footprint = (cooling_energy + it_energy) * state.carbon_intensity_gco2 / 1000.0
            carbon_savings = max(0.0, cooling_energy - 50.0) * 0.2  # placeholder
            helium_efficiency = 0.8  # placeholder
            sustainability_score = self._calculate_sustainability_score(pue, 50.0, state.carbon_intensity_gco2, helium_efficiency)

            # Multi‑zone actions (if enabled)
            zone_temperatures = {}
            if use_multi_zone:
                for zone in self.multi_zone_agent.zone_ids:
                    state_zone = np.random.randn(10)
                    action_zone = self.multi_zone_agent.select_zone_action(zone, state_zone)
                    zone_temperatures[zone] = 25.0 + random.uniform(-2, 2) - action_zone * 0.3

            result = ThermalOptimizationResult(
                total_energy_kw=it_energy + cooling_energy,
                cooling_energy_kw=cooling_energy,
                it_energy_kw=it_energy,
                pue=pue,
                avg_server_temp_c=25.0,
                max_server_temp_c=27.0,
                carbon_footprint_kg_per_hour=carbon_footprint,
                carbon_intensity_gco2_per_kwh=state.carbon_intensity_gco2,
                carbon_savings_kg=carbon_savings,
                helium_usage_liters=0.0,
                helium_efficiency=helium_efficiency * 100.0,
                sustainability_score=sustainability_score,
                optimization_time_ms=(time.time() - start_time) * 1000.0,
                gpu_accelerated=False,
                zone_temperatures=zone_temperatures,
                anomaly_detected=random.random() > 0.95,
                rl_action_used=action_idx,
                rl_action_description=f"Strategy: {strategy}"
            )

            # Reward for distillation
            reward = 0.0
            if pue < 1.5:
                reward += 0.3
            elif pue > 2.0:
                reward -= 0.1
            reward += 0.2 * (sustainability_score / 100.0)
            if carbon_footprint < 5.0:
                reward += 0.2
            if result.avg_server_temp_c < 28.0:
                reward += 0.3
            reward = max(0.0, min(1.0, reward))

            next_state = await self._get_optimization_state()
            await self.distillation_optimizer.update_after_test(
                state_vec, action_idx, reward, next_state.to_feature_vector(), teacher_probs
            )

            # Quantum signing
            signature = await self.pqc.sign_data(asdict(result))
            result.quantum_signature = signature

            # Blockchain recording
            data_id = f"thermal_{uuid.uuid4().hex[:8]}"
            data_hash = hashlib.sha256(json.dumps(asdict(result), sort_keys=True, default=str).encode()).hexdigest()
            blockchain_result = await self.blockchain.record_thermal_data(data_id, data_hash, {'pue': pue, 'strategy': strategy})
            result.blockchain_tx_hash = blockchain_result.get('tx_hash')

            # Cloud distribution
            distribution = await self.cloud_distributor.distribute_thermal_data({'size_gb': 0.001})
            result.cloud_distribution = distribution

            # Store history
            async with self._history_lock:
                self.optimization_history.append(result)

            # Store in central storage
            self.storage.store_thermal_optimization(result)

            # Publish FeedbackEvent
            event = FeedbackEvent.create_with_context(
                task_id=f"thermal_{uuid.uuid4().hex[:8]}",
                selected_action=strategy,
                quality_score=result.sustainability_score / 100,
                latency_ms=result.optimization_time_ms,
                energy_joules=result.total_energy_kw * 1000,  # placeholder conversion
                carbon_g=result.carbon_footprint_kg_per_hour * 1000,
                feedback_type="thermal",
                adaptive_cost_value=0.0,
                state=state_dict,
                candidates=[{'action': s} for s in self.ACTION_SPACE],
                source="thermal_optimizer",
                environment=central_config.ENVIRONMENT,
                tags=["thermal", "cooling"]
            )
            await self.queue.publish("feedback_events", event.to_json())

            # Check drift
            if self.drift:
                await self.drift.check_drift(self.adaptive_cost.get_current_weights())

            # Update metrics
            self.metrics.set_pue(pue)
            self.metrics.set_cooling_energy(cooling_energy)
            self.metrics.set_sustainability_score(sustainability_score)

            logger.info(f"Thermal optimization: strategy={strategy}, PUE={pue:.3f}, score={sustainability_score:.1f}")
            return result

    # ----------------------------------------------------------------------
    # Helpers
    # ----------------------------------------------------------------------
    def _calculate_sustainability_score(self, pue: float, renewable_pct: float, carbon_intensity: float, helium_efficiency: float) -> float:
        score = 50.0
        score += max(-20.0, (1.5 - pue) * 20.0)
        score += (renewable_pct - 50.0) * 0.2
        score += max(-10.0, (400.0 - carbon_intensity) * 0.01)
        score += (helium_efficiency - 0.5) * 10.0
        return float(min(100.0, max(0.0, score)))

    # ----------------------------------------------------------------------
    # Lifecycle management
    # ----------------------------------------------------------------------
    async def start(self):
        self._running = True
        logger.info("Starting Thermal Optimizer...")
        loop = asyncio.get_running_loop()
        self._background_tasks.extend([
            loop.create_task(self._auto_optimize_loop()),
            loop.create_task(self._carbon_update_loop()),
            loop.create_task(self._cleanup_loop()),
        ])

    async def _auto_optimize_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(central_config.auto_optimize_interval or 1800)
            try:
                await self.optimize()
            except Exception as e:
                logger.error(f"Auto optimize error: {e}")

    async def _carbon_update_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(central_config.carbon_update_interval or 300)
            try:
                await self.carbon_manager.get_current_intensity()
            except Exception as e:
                logger.error(f"Carbon update error: {e}")

    async def _cleanup_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(86400)
            try:
                self.storage.clean_thermal_records(days=central_config.data_retention_days or 365)
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

    async def shutdown(self):
        logger.info("Shutting down Thermal Optimizer...")
        self._shutdown_event.set()
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.carbon_manager.close()
        logger.info("Shutdown complete")

# =============================================================================
# SINGLETON ACCESSOR
# =============================================================================
_thermal_optimizer_instance = None
_thermal_optimizer_lock = asyncio.Lock()

async def get_thermal_optimizer(storage: Storage, queue: AsyncMessageQueue,
                                adaptive_cost: AdaptiveCostFunction,
                                pareto_gating: ParetoGating,
                                drift_detector: DriftDetector,
                                metrics: MetricsRegistry) -> EnhancedThermalOptimizer:
    global _thermal_optimizer_instance
    if _thermal_optimizer_instance is None:
        async with _thermal_optimizer_lock:
            if _thermal_optimizer_instance is None:
                _thermal_optimizer_instance = EnhancedThermalOptimizer(
                    storage, queue, adaptive_cost, pareto_gating, drift_detector, metrics
                )
                await _thermal_optimizer_instance.start()
    return _thermal_optimizer_instance

# =============================================================================
# MAIN ENTRY POINT (for standalone testing)
# =============================================================================
async def main():
    from ..storage import Storage
    from ..scaling.message_queue import AsyncMessageQueue
    from ..feedback.adaptive_cost import AdaptiveCostFunction
    from ..routing.pareto_gating import ParetoGating
    from ..safety.drift_detector import DriftDetector
    from ..metrics import MetricsRegistry

    storage = Storage()
    queue = AsyncMessageQueue()
    adaptive_cost = AdaptiveCostFunction(storage)
    pareto = ParetoGating()
    drift = DriftDetector(storage, adaptive_cost)
    metrics = MetricsRegistry()

    optimizer = await get_thermal_optimizer(storage, queue, adaptive_cost, pareto, drift, metrics)

    # Run a test optimization
    result = await optimizer.optimize()
    print(f"Optimization result: PUE={result.pue:.3f}, Sustainability={result.sustainability_score:.1f}")

    await optimizer.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
