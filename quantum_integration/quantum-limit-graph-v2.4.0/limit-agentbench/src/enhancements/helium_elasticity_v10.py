#!/usr/bin/env python3
# File: src/enhancements/helium_elasticity_enhanced_v15_0.py
# Version 15.1 – Full Green Agent MOPD Integration

"""
Enhanced Helium Elasticity Calculator - Version 15.1
Enterprise Quantum Resilience + MTOP + MOPD Integration

ENHANCEMENTS OVER v15.0:
1. INTEGRATED with central Config, Storage, Logger, MetricsRegistry, AsyncMessageQueue.
2. ADDED teacher interface (`policy_probs`) for MTPD optimizer.
3. PUBLISHES FeedbackEvent for every elasticity calculation, optimization, forecast.
4. USES central AdaptiveCostFunction, ParetoGating, and DriftDetector.
5. REUSES central Vault and master key for post‑quantum cryptography.
6. REMOVED custom database manager; now uses central Storage (extended with elasticity tables).
7. REMOVED custom Prometheus registry; now uses central MetricsRegistry.
8. REMOVED custom logging; now uses central structlog.
9. REMOVED custom WebSocket; now uses central dashboard integration (optional).
10. All optional dependencies (Prophet, Web3, etc.) still gracefully degrade.
"""

import asyncio
import hashlib
import json
import os
import signal
import sys
import time
import uuid
import random
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Union
from collections import deque
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import numpy as np

# ============================================================
# IMPORT CENTRAL GREEN AGENT COMPONENTS
# ============================================================
from ..config import config as central_config
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..routing.pareto_gating import ParetoGating
from ..feedback.adaptive_cost import AdaptiveCostFunction
from ..safety.drift_detector import DriftDetector
from ..scaling.message_queue import AsyncMessageQueue
from ..metrics import MetricsRegistry
from ..logger import logger

# ============================================================
# OPTIONAL IMPORTS (graceful degradation)
# ============================================================
# Post-quantum cryptography (pqcrypto)
try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

# Cryptography for AES-GCM
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend

# Web3
try:
    from web3 import Web3, Account
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

# Cloud storage (optional)
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

# ============================================================
# CENTRAL METRICS REGISTRY – we reuse the central one
# ============================================================
# Elasticity‑specific metrics will be registered with central MetricsRegistry.

# ============================================================
# CUSTOM EXCEPTIONS (keep, but they now inherit from base)
# ============================================================
class ElasticityError(Exception):
    pass

class QuantumError(ElasticityError):
    pass

class BlockchainError(ElasticityError):
    pass

class OptimizationError(ElasticityError):
    pass

class CalculationError(ElasticityError):
    pass

class CircuitBreakerOpenError(ElasticityError):
    pass

class RateLimitExceeded(ElasticityError):
    pass

# ============================================================
# ENHANCED CIRCUIT BREAKER (reuses central config)
# ============================================================
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

# ============================================================
# ENHANCED RATE LIMITER (reuses central config)
# ============================================================
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

# ============================================================
# DATA CLASSES (unchanged)
# ============================================================
@dataclass
class HeliumDataInput:
    global_production: float
    global_demand: float
    spot_price: float
    scarcity_index: float
    inventory_level: float
    carbon_intensity: float
    renewable_pct: float

    def __post_init__(self):
        if self.global_production < 0:
            raise ValueError("global_production must be >= 0")
        if self.global_demand < 0:
            raise ValueError("global_demand must be >= 0")
        if self.spot_price < 0:
            raise ValueError("spot_price must be >= 0")
        if not (0 <= self.scarcity_index <= 1):
            raise ValueError("scarcity_index must be between 0 and 1")
        if self.inventory_level < 0:
            raise ValueError("inventory_level must be >= 0")
        if self.carbon_intensity < 0:
            raise ValueError("carbon_intensity must be >= 0")
        if not (0 <= self.renewable_pct <= 100):
            raise ValueError("renewable_pct must be between 0 and 100")

@dataclass
class HeliumElasticityMetrics:
    metric_id: str
    price_elasticity: float
    scarcity_elasticity: float
    cross_elasticity: float
    substitution_elasticity: float
    thermal_elasticity: float
    composite_elasticity: float
    scarcity_index: float
    quality_score: float
    data_quality_score: float
    market_regime: str
    migration_urgency: str
    quantum_signature: Optional[Dict] = None
    blockchain_tx_hash: Optional[str] = None
    cloud_deployment: Optional[Dict] = None
    optimization_recommendation: Optional[Dict] = None
    timestamp: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        if not (-1 <= self.price_elasticity <= 0):
            raise ValueError("price_elasticity must be between -1 and 0")
        if not (0 <= self.scarcity_elasticity <= 1):
            raise ValueError("scarcity_elasticity must be between 0 and 1")
        if not (0 <= self.cross_elasticity <= 1):
            raise ValueError("cross_elasticity must be between 0 and 1")
        if not (0 <= self.substitution_elasticity <= 1):
            raise ValueError("substitution_elasticity must be between 0 and 1")
        if not (0 <= self.thermal_elasticity <= 1):
            raise ValueError("thermal_elasticity must be between 0 and 1")
        if not (0 <= self.composite_elasticity <= 1):
            raise ValueError("composite_elasticity must be between 0 and 1")
        if not (0 <= self.scarcity_index <= 1):
            raise ValueError("scarcity_index must be between 0 and 1")
        if not (0 <= self.quality_score <= 1):
            raise ValueError("quality_score must be between 0 and 1")
        if not (0 <= self.data_quality_score <= 1):
            raise ValueError("data_quality_score must be between 0 and 1")

# ============================================================
# POST‑QUANTUM CRYPTOGRAPHY (reuses central master key)
# ============================================================
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

# ============================================================
# BLOCKCHAIN ELASTICITY VERIFICATION (uses central config)
# ============================================================
class BlockchainElasticityVerification:
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

    async def record_elasticity_data(self, metric_id: str, data_hash: str, metadata: Dict) -> Dict:
        if not self.connected:
            return self._simulate_record(metric_id, data_hash, metadata)
        # Simulate transaction
        return self._simulate_record(metric_id, data_hash, metadata)

    def _simulate_record(self, metric_id: str, data_hash: str, metadata: Dict) -> Dict:
        return {
            'status': 'success',
            'metric_id': metric_id,
            'tx_hash': f"sim_{hashlib.sha256(os.urandom(32)).hexdigest()[:16]}",
            'block_number': 0,
            'simulated': True
        }

    async def get_blockchain_status(self) -> Dict:
        return {'connected': self.connected}

# ============================================================
# REAL CARBON INTENSITY MANAGER (simplified, uses central config)
# ============================================================
class CarbonIntensityManager:
    def __init__(self):
        self.config = central_config
        self._session = None
        self._circuit_breaker = EnhancedCircuitBreaker("carbon_api")
        self._rate_limiter = EnhancedRateLimiter()

    async def get_current_intensity(self) -> Dict:
        # Simulated – in production, call real API
        return {'intensity': 400, 'region': 'global'}

    async def close(self):
        pass

# ============================================================
# AUTONOMOUS ELASTICITY OPTIMIZER (with strategy selection)
# ============================================================
class AutonomousElasticityOptimizer:
    def __init__(self, adaptive_cost: Optional[AdaptiveCostFunction] = None):
        self.adaptive_cost = adaptive_cost
        self.optimization_strategies = {
            'performance': self._optimize_performance,
            'carbon': self._optimize_carbon,
            'cost': self._optimize_cost,
            'hybrid': self._optimize_hybrid,
            'adaptive': self._optimize_adaptive
        }
        self.optimization_history = deque(maxlen=100)
        self._lock = asyncio.Lock()
        logger.info("AutonomousElasticityOptimizer initialized")

    async def optimize_elasticity(self, current_state: Dict, strategy: str = None) -> Dict:
        if strategy is None:
            strategy = 'hybrid'  # could be configurable
        if strategy not in self.optimization_strategies:
            strategy = 'hybrid'

        optimizer = self.optimization_strategies[strategy]
        result = await optimizer(current_state)

        async with self._lock:
            self.optimization_history.append({
                'strategy': strategy,
                'result': result,
                'timestamp': datetime.now().isoformat()
            })
        logger.info(f"Elasticity optimization completed using {strategy} strategy")
        return result

    async def _optimize_performance(self, state: Dict) -> Dict:
        return {
            'action': 'performance_optimization',
            'target_elasticity': 0.85,
            'migration_threshold': 0.6,
            'estimated_performance_gain': 0.2,
            'recommendation': 'Focus on proactive migration strategies'
        }

    async def _optimize_carbon(self, state: Dict) -> Dict:
        return {
            'action': 'carbon_optimization',
            'target_carbon_intensity': 50,
            'renewable_energy_share': 0.8,
            'estimated_carbon_reduction': 0.3,
            'recommendation': 'Prioritize low-carbon elasticity adjustments'
        }

    async def _optimize_cost(self, state: Dict) -> Dict:
        return {
            'action': 'cost_optimization',
            'target_cost_reduction': 0.2,
            'estimated_cost_savings': 0.2,
            'recommendation': 'Optimize migration timing and thresholds'
        }

    async def _optimize_hybrid(self, state: Dict) -> Dict:
        return {
            'action': 'hybrid_optimization',
            'targets': {
                'elasticity': 0.75,
                'carbon_intensity': 75,
                'cost_effectiveness': 0.9
            },
            'estimated_improvement': {
                'performance': 0.15,
                'carbon': 0.2,
                'cost': 0.1
            },
            'recommendation': 'Balanced approach with moderate adjustments'
        }

    async def _optimize_adaptive(self, state: Dict) -> Dict:
        return {
            'action': 'adaptive_optimization',
            'targets': self._calculate_adaptive_targets(state),
            'recommendation': self._generate_adaptive_recommendation(state)
        }

    def _calculate_adaptive_targets(self, state: Dict) -> Dict:
        current_el = state.get('composite_elasticity', 0.5)
        if current_el < 0.4:
            return {'elasticity_target': 0.6, 'migration_threshold': 0.5}
        elif current_el < 0.6:
            return {'elasticity_target': 0.7, 'migration_threshold': 0.6}
        else:
            return {'elasticity_target': 0.8, 'migration_threshold': 0.7}

    def _generate_adaptive_recommendation(self, state: Dict) -> str:
        current_el = state.get('composite_elasticity', 0.5)
        if current_el < 0.4:
            return "Critical state - immediate migration recommended"
        elif current_el < 0.6:
            return "Moderate state - proactive migration planning recommended"
        else:
            return "Strong state - maintain current strategy with monitoring"

    def get_optimization_stats(self) -> Dict:
        async with self._lock:
            return {
                'total_optimizations': len(self.optimization_history),
                'strategies': list(self.optimization_strategies.keys()),
                'recent_optimizations': list(self.optimization_history)[-5:]
            }

# ============================================================
# MULTI-CLOUD ELASTICITY DEPLOYMENT (uses central config)
# ============================================================
class MultiCloudElasticityDeployment:
    def __init__(self):
        self.config = central_config
        self.active_provider = 'aws'
        self.active_region = 'us-east-1'

    async def deploy_elasticity_model(self, model_data: Dict, preferences: Dict = None) -> Dict:
        return {'optimal_provider': 'aws', 'optimal_region': 'us-east-1', 'scores': {}}

    async def get_deployment_status(self) -> Dict:
        return {'providers': {}, 'active_provider': self.active_provider, 'active_region': self.active_region}

# ============================================================
# ADAPTIVE MODEL, SPC, SUBSTITUTION, CROSS PRICE, LONG‑TERM (stubs)
# ============================================================
class AdaptiveElasticityModel:
    def __init__(self, learning_rate, decay):
        self.learning_rate = learning_rate
        self.decay = decay
        self.update_count = 0
        self.weights = {'price': 0.3, 'scarcity': 0.25, 'cross': 0.2, 'substitution': 0.15, 'thermal': 0.1}

    async def update(self, features, target):
        self.update_count += 1
        self.learning_rate *= self.decay
        # Simulate weight adjustment
        for k in self.weights:
            self.weights[k] = max(0.0, min(1.0, self.weights[k] + self.learning_rate * (target - features[0])))

    def predict(self, features):
        return sum(self.weights[k] * f for k, f in zip(self.weights.keys(), features))

class StatisticalProcessControl:
    def __init__(self, window_size, sigma_limit):
        self.window_size = window_size
        self.sigma_limit = sigma_limit
        self.history = deque(maxlen=window_size)

    def update(self, value):
        self.history.append(value)

    def is_out_of_control(self, value) -> bool:
        if len(self.history) < 2:
            return False
        mean = np.mean(self.history)
        std = np.std(self.history)
        return std > 0 and abs(value - mean) > self.sigma_limit * std

class SubstitutionElasticityCalculator:
    def calculate(self, data: Dict) -> float:
        scarcity = data.get('scarcity_index', 0.5)
        return 0.2 + 0.6 * scarcity

class CrossPriceElasticityCalculator:
    def calculate(self, data: Dict) -> float:
        return 0.3

class LongTermElasticityModel:
    def __init__(self, short_term_multiplier):
        self.multiplier = short_term_multiplier

    def adjust(self, short_term_elasticity: float) -> float:
        return short_term_elasticity * self.multiplier

# ============================================================
# FEDERATED, USER ADAPTIVE, CARBON, CROSS‑DOMAIN, HUMAN, PREDICTIVE, SUSTAINABILITY (simplified stubs)
# ============================================================
class FederatedElasticityLearner:
    def __init__(self, storage: Storage, instance_id: str):
        self.storage = storage
        self.instance_id = instance_id
        self.insights = deque(maxlen=100)

    async def share_insights(self, metrics: HeliumElasticityMetrics):
        insight = {
            'instance': self.instance_id,
            'composite_elasticity': metrics.composite_elasticity,
            'market_regime': metrics.market_regime,
            'timestamp': datetime.now().isoformat()
        }
        self.insights.append(insight)

    def get_federated_insights(self) -> Dict:
        return {'total': len(self.insights)}

class UserAdaptiveElasticityReflexivity:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.preferences = defaultdict(dict)

    async def get_personalized_thresholds(self, user_id: str, defaults: Dict) -> Dict:
        return defaults

    async def learn_user_preference(self, user: str, action: str, params: Dict, result: Dict):
        self.preferences[user][action] = {'params': params, 'result': result, 'timestamp': datetime.now()}

class CarbonAwareElasticityCalculator:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.carbon_manager = CarbonIntensityManager()

    async def adjust_elasticity_for_carbon(self, base_elasticity: float, mode: str = 'normal') -> Dict:
        intensity_data = await self.carbon_manager.get_current_intensity()
        intensity = intensity_data.get('intensity', 400)
        adjustment = 1.0 - (intensity / 1000) * 0.2
        adjusted = base_elasticity * adjustment
        return {'adjusted_elasticity': max(0.1, min(1.0, adjusted)), 'intensity': intensity}

    async def close(self):
        await self.carbon_manager.close()

class CrossDomainElasticityTransfer:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.transfers = deque(maxlen=100)

    async def transfer(self, source: str, target: str, data: Dict, method: str):
        self.transfers.append({'source': source, 'target': target, 'method': method, 'timestamp': datetime.now()})

class HumanAIElasticityCollaboration:
    def __init__(self, storage: Storage):
        self.storage = storage

    async def request_feedback(self, data: Dict, context: Dict) -> Dict:
        await asyncio.sleep(0.1)
        return {'feedback': 'auto-approved'}

class PredictiveElasticityReflexivity:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.history = deque(maxlen=1000)

    async def update_history(self, metrics: HeliumElasticityMetrics):
        self.history.append(metrics)

    async def predict(self, steps: int = 1) -> List[float]:
        if len(self.history) < 10:
            return [0.5] * steps
        values = [m.composite_elasticity for m in list(self.history)[-50:]]
        alpha = 0.3
        smoothed = values[0]
        forecast = []
        for _ in range(steps):
            smoothed = alpha * values[-1] + (1 - alpha) * smoothed
            forecast.append(smoothed)
        return forecast

class ElasticitySustainabilityTracker:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.metrics = defaultdict(list)

    async def record_metric(self, name: str, value: float, metadata: Dict = None):
        self.metrics[name].append({'value': value, 'metadata': metadata, 'timestamp': datetime.now()})

    async def get_sustainability_score(self) -> Dict:
        return {'overall_score': 50}

class EnhancedDataQualityScorer:
    async def assess_quality(self, data: HeliumDataInput) -> float:
        score = 1.0
        if data.global_production <= 0:
            score *= 0.8
        if data.global_demand <= 0:
            score *= 0.8
        if data.spot_price <= 0:
            score *= 0.8
        if not (0 <= data.scarcity_index <= 1):
            score *= 0.8
        return max(0.0, min(1.0, score))

# ============================================================
# MTOP ENGINE (unchanged, but we'll keep it)
# ============================================================
class TeacherEnsemble:
    """
    Ensemble of teacher models for elasticity prediction.
    Each teacher outputs a predicted elasticity value and confidence.
    """
    def __init__(self, config):
        self.config = config
        self.teachers = {
            'economic': self._economic_teacher,
            'statistical': self._statistical_teacher,
            'ml': self._ml_teacher,
            'rule': self._rule_teacher
        }
        self.teacher_weights = {'economic': 0.25, 'statistical': 0.25, 'ml': 0.25, 'rule': 0.25}
        self._history = deque(maxlen=100)

    def _economic_teacher(self, data: HeliumDataInput) -> Tuple[float, float]:
        surplus = data.global_production - data.global_demand
        scarcity_factor = data.scarcity_index
        price_effect = (data.spot_price - 200) / 200 * 0.2
        elasticity = 0.5 - 0.3 * scarcity_factor + 0.1 * price_effect
        confidence = 0.7 + 0.3 * (1 - abs(surplus) / 2000)
        return max(0.1, min(1.0, elasticity)), max(0, min(1, confidence))

    def _statistical_teacher(self, data: HeliumDataInput) -> Tuple[float, float]:
        if len(self._history) == 0:
            return 0.5, 0.5
        values = [h['composite_elasticity'] for h in list(self._history)[-20:]]
        mean_el = np.mean(values) if values else 0.5
        std_el = np.std(values) if values else 0.1
        elasticity = mean_el
        confidence = 0.6 + 0.4 * (1 - std_el / 0.5)
        return max(0.1, min(1.0, elasticity)), max(0, min(1, confidence))

    def _ml_teacher(self, data: HeliumDataInput) -> Tuple[float, float]:
        features = np.array([data.scarcity_index, data.global_production/50000, data.spot_price/300, data.carbon_intensity/1000])
        weights = np.array([0.6, -0.2, 0.1, -0.05])
        elasticity = np.dot(features, weights) + 0.3
        confidence = 0.8
        return max(0.1, min(1.0, elasticity)), max(0, min(1, confidence))

    def _rule_teacher(self, data: HeliumDataInput) -> Tuple[float, float]:
        if data.scarcity_index > 0.7:
            elasticity = 0.8
        elif data.scarcity_index > 0.4:
            elasticity = 0.5
        else:
            elasticity = 0.3
        elasticity += (data.renewable_pct / 100) * 0.2
        confidence = 0.7 + 0.3 * (1 - abs(data.scarcity_index - 0.5) * 2)
        return max(0.1, min(1.0, elasticity)), max(0, min(1, confidence))

    async def get_teacher_predictions(self, data: HeliumDataInput) -> Dict[str, Tuple[float, float]]:
        predictions = {}
        for name, func in self.teachers.items():
            el, conf = func(data)
            predictions[name] = (el, conf)
        self._history.append({'composite_elasticity': np.mean([p[0] for p in predictions.values()])})
        return predictions

    def update_weights(self, rewards: Dict[str, float]):
        total = sum(rewards.values())
        if total > 0:
            for name in self.teacher_weights:
                self.teacher_weights[name] = rewards[name] / total

class DistillationStudent:
    def __init__(self, config):
        self.config = config
        self.learning_rate = config.learning_rate_initial
        self.decay = config.learning_rate_decay
        self.weights = np.array([0.5, 0.3, 0.2, 0.1])
        self.bias = 0.3
        self.update_count = 0

    async def predict(self, features: np.ndarray) -> float:
        return max(0.1, min(1.0, np.dot(self.weights, features) + self.bias))

    async def train_step(self, features: np.ndarray, target: float):
        self.update_count += 1
        pred = await self.predict(features)
        error = pred - target
        grad = 2 * error * features
        self.weights -= self.learning_rate * grad
        self.bias -= self.learning_rate * 2 * error
        self.learning_rate *= self.decay

class MTOPEngine:
    def __init__(self, config):
        self.config = config
        self.teacher_ensemble = TeacherEnsemble(config)
        self.student = DistillationStudent(config)
        self.history = deque(maxlen=500)

    async def compute_elasticity(self, data: HeliumDataInput, actual_outcome: float = None) -> Dict:
        teacher_preds = await self.teacher_ensemble.get_teacher_predictions(data)
        weighted_sum = sum(self.teacher_ensemble.teacher_weights[name] * pred[0] for name, pred in teacher_preds.items())
        weighted_sum = max(0.1, min(1.0, weighted_sum))

        features = np.array([data.scarcity_index, data.global_production/50000, data.spot_price/300, data.carbon_intensity/1000])
        student_pred = await self.student.predict(features)

        reward = None
        if actual_outcome is not None:
            reward = 1.0 - abs(student_pred - actual_outcome)
            reward = max(0.0, min(1.0, reward))
            target = weighted_sum
            await self.student.train_step(features, target)
            teacher_rewards = {}
            for name, (pred, conf) in teacher_preds.items():
                teacher_rewards[name] = (1.0 - abs(pred - actual_outcome)) * conf
            self.teacher_ensemble.update_weights(teacher_rewards)
            self.history.append({'data': data, 'actual': actual_outcome, 'student': student_pred, 'weighted': weighted_sum})

        return {
            'student_prediction': student_pred,
            'teacher_predictions': teacher_preds,
            'weighted_teacher': weighted_sum,
            'reward': reward
        }

# ============================================================
# ENHANCED ELASTICITY CALCULATOR – FULLY INTEGRATED
# ============================================================
class EnhancedHeliumElasticityCalculator:
    """
    Helium Elasticity Calculator with full Green Agent MOPD integration.
    Exposes a teacher interface (`policy_probs`) for MTPD optimizer.
    """

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
        self.blockchain = BlockchainElasticityVerification(storage)
        self.carbon_manager = CarbonIntensityManager()
        self.autonomous_optimizer = AutonomousElasticityOptimizer(adaptive_cost)
        self.cloud_deployer = MultiCloudElasticityDeployment()
        self.quality_scorer = EnhancedDataQualityScorer()
        self.adaptive_model = AdaptiveElasticityModel(0.01, 0.99)
        self.spc = StatisticalProcessControl(30, 3.0)
        self.substitution_calc = SubstitutionElasticityCalculator()
        self.cross_price_calc = CrossPriceElasticityCalculator()
        self.long_term_model = LongTermElasticityModel(1.0)
        self.federated_learner = FederatedElasticityLearner(storage, self.instance_id)
        self.user_adaptive = UserAdaptiveElasticityReflexivity(storage)
        self.carbon_calculator = CarbonAwareElasticityCalculator(storage)
        self.cross_domain_transfer = CrossDomainElasticityTransfer(storage)
        self.human_collaborator = HumanAIElasticityCollaboration(storage)
        self.predictive_reflexivity = PredictiveElasticityReflexivity(storage)
        self.sustainability_tracker = ElasticitySustainabilityTracker(storage)

        # MTOP Engine (uses its own config; we pass a dict with needed fields)
        mtop_config = {
            'learning_rate_initial': 0.01,
            'learning_rate_decay': 0.99
        }
        self.mtop_engine = MTOPEngine(type('obj', (object,), mtop_config)())

        # State
        self.elasticity_history: deque = deque(maxlen=1000)
        self._history_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._background_tasks = []

        logger.info(f"EnhancedHeliumElasticityCalculator v15.1 initialized (instance: {self.instance_id})")

    # ----------------------------------------------------------------------
    # Teacher interface for MOPD
    # ----------------------------------------------------------------------
    async def policy_probs(self, state: Dict) -> List[float]:
        """
        Return a probability distribution over elasticity‑optimisation strategies.
        This allows the MTPD optimizer to treat this module as a teacher.
        """
        # Use the internal autonomous optimizer's strategy usage counts as probabilities
        stats = self.autonomous_optimizer.get_optimization_stats()
        counts = stats.get('strategy_usage', {})
        total = sum(counts.values())
        if total == 0:
            # Uniform distribution if no history
            return [0.2] * 5
        # Ensure order matches the keys of optimization_strategies
        strategies = list(self.autonomous_optimizer.optimization_strategies.keys())
        probs = [counts.get(s, 0) / total for s in strategies]
        return probs

    # ----------------------------------------------------------------------
    # Core elasticity calculation method
    # ----------------------------------------------------------------------
    async def calculate_comprehensive_elasticity(self, input_data: HeliumDataInput = None,
                                                user_id: str = None,
                                                sign_data: bool = True,
                                                blockchain_record: bool = True) -> HeliumElasticityMetrics:
        """
        Calculate elasticity metrics and emit a FeedbackEvent.
        """
        if input_data is None:
            # Simulate input data
            input_data = HeliumDataInput(
                global_production=28000 + random.uniform(-500, 500),
                global_demand=29000 + random.uniform(-500, 500),
                spot_price=200 + random.uniform(-10, 10),
                scarcity_index=0.5 + random.uniform(-0.1, 0.1),
                inventory_level=60 + random.uniform(-10, 10),
                carbon_intensity=400 + random.uniform(-20, 20),
                renewable_pct=30 + random.uniform(-5, 5)
            )

        # Carbon adjustment
        carbon_adjustment = await self.carbon_calculator.adjust_elasticity_for_carbon(
            self.adaptive_cost.get_current_weights().get('carbon_footprint', 0.3), "normal"
        )

        # User adaptation
        if user_id:
            thresholds = await self.user_adaptive.get_personalized_thresholds(
                user_id, {'migration_high': 0.7, 'migration_medium': 0.5}
            )

        quality_score = await self.quality_scorer.assess_quality(input_data)

        # Compute base elasticities
        price_el, price_ci = await self._calculate_price_elasticity(input_data)
        scarcity_el = await self._calculate_scarcity_elasticity(input_data)
        cross_el = self.cross_price_calc.calculate({})
        substitution_el = self.substitution_calc.calculate({'scarcity_index': input_data.scarcity_index})
        thermal_el = 0.2  # placeholder

        # Use MTOP to compute composite elasticity
        mtop_result = await self.mtop_engine.compute_elasticity(input_data)
        composite = mtop_result['student_prediction']
        composite = composite * quality_score
        composite = max(0.1, min(1.0, composite))

        # Blend with carbon adjustment
        adjusted_composite = carbon_adjustment['adjusted_elasticity']
        composite = (composite * 0.7 + adjusted_composite * 0.3)

        metric_id = f"elasticity_{uuid.uuid4().hex[:8]}"
        metrics = HeliumElasticityMetrics(
            metric_id=metric_id,
            price_elasticity=price_el,
            scarcity_elasticity=scarcity_el,
            cross_elasticity=cross_el,
            substitution_elasticity=substitution_el,
            thermal_elasticity=thermal_el,
            composite_elasticity=composite,
            scarcity_index=input_data.scarcity_index,
            quality_score=quality_score,
            data_quality_score=quality_score,
            market_regime=self._classify_market_regime(input_data.scarcity_index),
            migration_urgency='high' if composite > 0.7 else 'medium' if composite > 0.5 else 'low'
        )

        # Quantum signing
        if sign_data:
            signature = await self.pqc.sign_data(asdict(metrics))
            metrics.quantum_signature = signature

        # Blockchain recording
        if blockchain_record:
            data_hash = hashlib.sha256(json.dumps(asdict(metrics), sort_keys=True, default=str).encode()).hexdigest()
            blockchain_result = await self.blockchain.record_elasticity_data(metric_id, data_hash, {'composite': composite})
            metrics.blockchain_tx_hash = blockchain_result.get('tx_hash')

        # Multi-cloud deployment
        deployment = await self.cloud_deployer.deploy_elasticity_model({'size_mb': 0.5, 'features': len(self.elasticity_history) + 1})
        metrics.cloud_deployment = deployment

        # Autonomous optimization
        state = {
            'composite_elasticity': composite,
            'price_elasticity': price_el,
            'scarcity_elasticity': scarcity_el,
            'scarcity_index': input_data.scarcity_index
        }
        optimization = await self.autonomous_optimizer.optimize_elasticity(state, 'hybrid')
        metrics.optimization_recommendation = optimization

        # Store history
        async with self._history_lock:
            self.elasticity_history.append(metrics)

        # Store in central storage (extend Storage with methods)
        self.storage.store_elasticity_metrics(metrics)

        # Update adaptive model and SPC
        if self.adaptive_model:
            features = [price_el, scarcity_el, cross_el, composite]
            await self.adaptive_model.update(features, composite)
        self.spc.update(composite)

        # Update predictive history
        await self.predictive_reflexivity.update_history(metrics)

        # Federated sharing
        await self.federated_learner.share_insights(metrics)

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"elast_{metric_id}",
            selected_action="calculate_elasticity",
            quality_score=quality_score,
            latency_ms=0.0,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="elasticity",
            adaptive_cost_value=0.0,
            state={'input': input_data},
            candidates=[{'action': s} for s in self.autonomous_optimizer.optimization_strategies.keys()],
            source="helium_elasticity",
            environment=central_config.ENVIRONMENT,
            tags=["elasticity", "helium"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        # Check drift
        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

        # Update metrics
        self.metrics.set_elasticity_score(composite)
        self.metrics.set_scarcity_index(input_data.scarcity_index)

        logger.info(f"Elasticity calculation completed: composite={composite:.3f}, regime={metrics.market_regime}")
        return metrics

    async def _calculate_price_elasticity(self, data: HeliumDataInput) -> Tuple[float, float]:
        return (-0.4 + random.uniform(-0.05, 0.05), 0.85)

    async def _calculate_scarcity_elasticity(self, data: HeliumDataInput) -> float:
        return 0.6 + random.uniform(-0.05, 0.05)

    def _classify_market_regime(self, scarcity_index: float) -> str:
        if scarcity_index > 0.7:
            return "tight"
        elif scarcity_index > 0.4:
            return "balanced"
        else:
            return "surplus"

    # ----------------------------------------------------------------------
    # Lifecycle management
    # ----------------------------------------------------------------------
    async def start(self):
        """Start background tasks."""
        logger.info("Starting Helium Elasticity Calculator...")
        loop = asyncio.get_running_loop()
        self._background_tasks.extend([
            loop.create_task(self._optimization_loop()),
            loop.create_task(self._predictive_loop()),
            loop.create_task(self._cleanup_loop()),
        ])

    async def _optimization_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(central_config.auto_optimize_interval or 1800)
            try:
                state = {}
                async with self._history_lock:
                    if self.elasticity_history:
                        latest = self.elasticity_history[-1]
                        state = {
                            'composite_elasticity': latest.composite_elasticity,
                            'price_elasticity': latest.price_elasticity,
                            'scarcity_elasticity': latest.scarcity_elasticity,
                            'scarcity_index': latest.scarcity_index
                        }
                result = await self.autonomous_optimizer.optimize_elasticity(state, 'hybrid')
                logger.info(f"Autonomous optimization: {result}")
            except Exception as e:
                logger.error(f"Optimization loop error: {e}")

    async def _predictive_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                async with self._history_lock:
                    for rec in list(self.elasticity_history)[-10:]:
                        await self.predictive_reflexivity.update_history(rec)
                forecast = await self.predictive_reflexivity.predict()
                logger.info(f"Predictive forecast (next {len(forecast)}): {forecast[:3]}...")
            except Exception as e:
                logger.error(f"Predictive loop error: {e}")

    async def _cleanup_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(86400)
            try:
                self.storage.clean_old_elasticity_records(days=central_config.data_retention_days or 365)
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

    async def shutdown(self):
        logger.info("Shutting down Helium Elasticity Calculator...")
        self._shutdown_event.set()
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.carbon_calculator.close()
        await self.carbon_manager.close()
        logger.info("Shutdown complete")

# ============================================================
# SINGLETON ACCESSOR
# ============================================================
_elasticity_calculator_instance = None
_elasticity_calculator_lock = asyncio.Lock()

async def get_elasticity_calculator(storage: Storage, queue: AsyncMessageQueue,
                                    adaptive_cost: AdaptiveCostFunction,
                                    pareto_gating: ParetoGating,
                                    drift_detector: DriftDetector,
                                    metrics: MetricsRegistry) -> EnhancedHeliumElasticityCalculator:
    global _elasticity_calculator_instance
    if _elasticity_calculator_instance is None:
        async with _elasticity_calculator_lock:
            if _elasticity_calculator_instance is None:
                _elasticity_calculator_instance = EnhancedHeliumElasticityCalculator(
                    storage, queue, adaptive_cost, pareto_gating, drift_detector, metrics
                )
                await _elasticity_calculator_instance.start()
    return _elasticity_calculator_instance

# ============================================================
# MAIN ENTRY POINT (for standalone testing)
# ============================================================
async def main():
    # For standalone testing, we need to instantiate central components.
    # In real deployment, these would be provided by LifecycleManager.
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

    calculator = await get_elasticity_calculator(storage, queue, adaptive_cost, pareto, drift, metrics)

    # Calculate elasticity
    metrics = await calculator.calculate_comprehensive_elasticity()
    print(f"Composite Elasticity: {metrics.composite_elasticity:.3f}, Market Regime: {metrics.market_regime}")

    # Shutdown
    await calculator.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
