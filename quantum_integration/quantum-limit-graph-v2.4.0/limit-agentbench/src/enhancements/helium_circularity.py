#!/usr/bin/env python3
# File: src/enhancements/helium_circularity_enhanced_v15_0.py
# Version 15.1 – Full Green Agent MOPD Integration

"""
Enhanced Helium Circularity Model - Version 15.1
Enterprise Quantum+ + MOPD Integration

ENHANCEMENTS OVER v15.0:
1. INTEGRATED with central Config, Storage, Logger, MetricsRegistry, AsyncMessageQueue.
2. ADDED teacher interface (`policy_probs`) for MTPD optimizer.
3. PUBLISHES FeedbackEvent for every circularity calculation, optimization, forecast.
4. USES central AdaptiveCostFunction, ParetoGating, and DriftDetector.
5. REUSES central Vault and master key for post‑quantum cryptography.
6. REMOVED custom database manager; now uses central Storage (extended with circularity tables).
7. REMOVED custom Prometheus registry; now uses central MetricsRegistry.
8. REMOVED custom logging; now uses central structlog.
9. REMOVED custom FastAPI; now uses central dashboard integration (optional).
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

# Prophet for forecasting
try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

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
# Circularity‑specific metrics will be registered with central MetricsRegistry.

# ============================================================
# CUSTOM EXCEPTIONS (keep, but they now inherit from base)
# ============================================================
class CircularityError(Exception):
    pass

class QuantumError(CircularityError):
    pass

class BlockchainError(CircularityError):
    pass

class OptimizationError(CircularityError):
    pass

class DeploymentError(CircularityError):
    pass

class CircuitBreakerOpenError(CircularityError):
    pass

class RateLimitExceeded(CircularityError):
    pass

class VaultError(CircularityError):
    pass

class CloudStorageError(CircularityError):
    pass

class PredictiveError(CircularityError):
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
class HeliumCircularityMetrics:
    record_id: str
    circularity_index: float
    circularity_level: str  # "excellent", "good", "moderate", "critical"
    recycling_rate: float
    recovery_efficiency: float
    collection_efficiency: float
    purification_efficiency: float
    data_quality_score: float
    quantum_signature: Optional[Dict] = None
    blockchain_tx_hash: Optional[str] = None
    cloud_deployment: Optional[Dict] = None
    optimization_recommendation: Optional[Dict] = None
    timestamp: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        if not (0 <= self.circularity_index <= 1):
            raise ValueError("circularity_index must be between 0 and 1")
        if self.circularity_level not in ["excellent", "good", "moderate", "critical"]:
            raise ValueError("circularity_level must be one of excellent/good/moderate/critical")
        if not (0 <= self.recycling_rate <= 1):
            raise ValueError("recycling_rate must be between 0 and 1")
        if not (0 <= self.recovery_efficiency <= 1):
            raise ValueError("recovery_efficiency must be between 0 and 1")
        if not (0 <= self.collection_efficiency <= 1):
            raise ValueError("collection_efficiency must be between 0 and 1")
        if not (0 <= self.purification_efficiency <= 1):
            raise ValueError("purification_efficiency must be between 0 and 1")
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
# BLOCKCHAIN CIRCULARITY VERIFICATION (uses central config)
# ============================================================
class BlockchainCircularityVerification:
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

    async def record_circularity_data(self, record_id: str, data_hash: str, metadata: Dict) -> Dict:
        if not self.connected:
            return self._simulate_record(record_id, data_hash, metadata)
        # Simulate transaction
        return self._simulate_record(record_id, data_hash, metadata)

    def _simulate_record(self, record_id: str, data_hash: str, metadata: Dict) -> Dict:
        return {
            'status': 'success',
            'record_id': record_id,
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
# AUTONOMOUS CIRCULARITY OPTIMIZER (bandit) – now uses adaptive cost
# ============================================================
class AutonomousCircularityOptimizer:
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
        self.epsilon = 0.1
        self.strategy_rewards = {s: 0.0 for s in self.optimization_strategies.keys()}
        self.strategy_counts = {s: 0 for s in self.optimization_strategies.keys()}
        self._lock = asyncio.Lock()
        logger.info("AutonomousCircularityOptimizer initialized with bandit")

    async def optimize_circularity(self, current_state: Dict, strategy: str = None) -> Dict:
        if strategy is None:
            # Epsilon‑greedy
            if random.random() < self.epsilon:
                strategy = random.choice(list(self.optimization_strategies.keys()))
            else:
                strategy = max(self.strategy_rewards, key=self.strategy_rewards.get)

        if strategy not in self.optimization_strategies:
            strategy = 'hybrid'

        optimizer = self.optimization_strategies[strategy]
        result = await optimizer(current_state)

        # Update reward based on outcome
        reward = 0.0
        if result.get('estimated_performance_gain'):
            reward = result['estimated_performance_gain']
        elif result.get('estimated_carbon_reduction'):
            reward = result['estimated_carbon_reduction']
        elif result.get('estimated_cost_savings'):
            reward = result['estimated_cost_savings']

        self.strategy_counts[strategy] += 1
        count = self.strategy_counts[strategy]
        self.strategy_rewards[strategy] += (reward - self.strategy_rewards[strategy]) / count
        self.epsilon = max(0.01, self.epsilon * 0.99)

        async with self._lock:
            self.optimization_history.append({
                'strategy': strategy,
                'result': result,
                'timestamp': datetime.now().isoformat()
            })

        logger.info(f"Circularity optimization completed using {strategy} strategy")
        return result

    async def _optimize_performance(self, state: Dict) -> Dict:
        return {
            'action': 'performance_optimization',
            'target_recycling_rate': 0.9,
            'target_recovery_efficiency': 0.95,
            'target_collection_efficiency': 0.98,
            'estimated_performance_gain': 0.25,
            'recommendation': 'Focus on recycling infrastructure and recovery technology'
        }

    async def _optimize_carbon(self, state: Dict) -> Dict:
        return {
            'action': 'carbon_optimization',
            'target_carbon_intensity': 50,
            'renewable_energy_share': 0.8,
            'estimated_carbon_reduction': 0.3,
            'recommendation': 'Prioritize renewable energy integration and process optimization'
        }

    async def _optimize_cost(self, state: Dict) -> Dict:
        return {
            'action': 'cost_optimization',
            'target_recycling_cost': 0.8,
            'target_recovery_cost': 0.7,
            'estimated_cost_savings': 0.2,
            'recommendation': 'Optimize collection and purification processes'
        }

    async def _optimize_hybrid(self, state: Dict) -> Dict:
        return {
            'action': 'hybrid_optimization',
            'targets': {
                'recycling_rate': 0.85,
                'carbon_intensity': 75,
                'cost_effectiveness': 0.9
            },
            'estimated_improvement': {
                'performance': 0.15,
                'carbon': 0.2,
                'cost': 0.1
            },
            'recommendation': 'Balanced approach with moderate investments across all areas'
        }

    async def _optimize_adaptive(self, state: Dict) -> Dict:
        return {
            'action': 'adaptive_optimization',
            'targets': self._calculate_adaptive_targets(state),
            'recommendation': self._generate_adaptive_recommendation(state)
        }

    def _calculate_adaptive_targets(self, state: Dict) -> Dict:
        current_ci = state.get('circularity_index', 0.5)
        if current_ci < 0.4:
            return {'recycling_rate': 0.7, 'recovery_efficiency': 0.8, 'collection_efficiency': 0.85}
        elif current_ci < 0.6:
            return {'recycling_rate': 0.8, 'recovery_efficiency': 0.85, 'collection_efficiency': 0.9}
        else:
            return {'recycling_rate': 0.9, 'recovery_efficiency': 0.9, 'collection_efficiency': 0.95}

    def _generate_adaptive_recommendation(self, state: Dict) -> str:
        current_ci = state.get('circularity_index', 0.5)
        if current_ci < 0.4:
            return "Critical state - immediate focus on recycling infrastructure"
        elif current_ci < 0.6:
            return "Moderate state - balanced improvements across all areas"
        else:
            return "Strong state - focus on fine-tuning and innovation"

    def get_optimization_stats(self) -> Dict:
        async with self._lock:
            return {
                'total_optimizations': len(self.optimization_history),
                'strategies': list(self.optimization_strategies.keys()),
                'recent_optimizations': list(self.optimization_history)[-5:],
                'strategy_usage': {s: len([h for h in self.optimization_history if h['strategy'] == s])
                                   for s in self.optimization_strategies.keys()},
                'strategy_rewards': self.strategy_rewards,
                'epsilon': self.epsilon
            }

# ============================================================
# MULTI-CLOUD CIRCULARITY DEPLOYMENT (uses central config)
# ============================================================
class MultiCloudCircularityDeployment:
    def __init__(self):
        self.config = central_config
        # ... (same as original, but using central config)
        self.active_provider = 'aws'
        self.active_region = 'us-east-1'

    async def deploy_circularity_model(self, model_data: Dict, preferences: Dict = None) -> Dict:
        return {'optimal_provider': 'aws', 'optimal_region': 'us-east-1', 'scores': {}}

    async def get_deployment_status(self) -> Dict:
        return {'providers': {}, 'active_provider': self.active_provider, 'active_region': self.active_region}

# ============================================================
# MULTI-CLOUD STORAGE (uses central config)
# ============================================================
class MultiCloudStorage:
    def __init__(self):
        self.config = central_config
        self.providers = {}
        if AWS_AVAILABLE and central_config.cloud_aws_bucket:
            self.providers['aws'] = {'client': boto3.client('s3', region_name=central_config.CLOUD_REGION, aws_access_key_id=central_config.cloud_aws_access_key, aws_secret_access_key=central_config.cloud_aws_secret_key), 'bucket': central_config.cloud_aws_bucket}
        if AZURE_AVAILABLE and central_config.cloud_azure_connection_string:
            self.providers['azure'] = {'client': BlobServiceClient.from_connection_string(central_config.cloud_azure_connection_string), 'container': central_config.cloud_azure_container}
        if GCP_AVAILABLE and central_config.cloud_gcp_credentials:
            self.providers['gcp'] = {'client': storage.Client(), 'bucket': central_config.cloud_gcp_bucket}

    async def store(self, data: Dict, filename: str = None) -> Dict:
        for provider_name, provider in self.providers.items():
            try:
                if provider_name == 'aws':
                    client = provider['client']; bucket = provider['bucket']; key = filename or f"circularity_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    client.put_object(Bucket=bucket, Key=key, Body=json.dumps(data, default=str).encode())
                    return {'provider': provider_name, 'location': f"s3://{bucket}/{key}"}
                elif provider_name == 'azure':
                    client = provider['client']; container = provider['container']; blob_name = filename or f"circularity_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    blob_client = client.get_blob_client(container=container, blob=blob_name)
                    blob_client.upload_blob(json.dumps(data, default=str).encode(), overwrite=True)
                    return {'provider': provider_name, 'location': f"https://{container}.blob.core.windows.net/{blob_name}"}
                elif provider_name == 'gcp':
                    client = provider['client']; bucket = provider['bucket']; blob_name = filename or f"circularity_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    blob = client.bucket(bucket).blob(blob_name)
                    blob.upload_from_string(json.dumps(data, default=str).encode())
                    return {'provider': provider_name, 'location': f"gs://{bucket}/{blob_name}"}
            except Exception as e:
                logger.warning(f"Cloud storage failed for {provider_name}: {e}")
        # Local fallback
        local_path = Path(f"./circularity_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(local_path, 'w') as f:
            json.dump(data, f, default=str)
        return {'provider': 'local', 'location': str(local_path)}

# ============================================================
# PREDICTIVE ANALYTICS (simplified, with Prophet fallback)
# ============================================================
class PredictiveAnalytics:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.history_circularity = deque(maxlen=1000)
        self.history_carbon = deque(maxlen=1000)
        self.prophet_available = PROPHET_AVAILABLE
        self._lock = asyncio.Lock()

    async def update_history(self, circularity_index: float, carbon_intensity: float):
        async with self._lock:
            self.history_circularity.append({'ds': datetime.now(), 'y': circularity_index})
            self.history_carbon.append({'ds': datetime.now(), 'y': carbon_intensity})

    async def forecast_circularity(self, horizon_hours: int = 24) -> Dict:
        if not self.prophet_available or len(self.history_circularity) < 30:
            return {'forecast': [], 'confidence': 0.0}
        try:
            import pandas as pd
            df = pd.DataFrame(list(self.history_circularity))
            df = df.sort_values('ds')
            def run_prophet():
                model = Prophet(changepoint_prior_scale=0.05, seasonality_prior_scale=10)
                model.fit(df)
                future = model.make_future_dataframe(periods=horizon_hours)
                forecast = model.predict(future)
                return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(horizon_hours)
            forecast_df = await asyncio.to_thread(run_prophet)
            return {
                'forecast': forecast_df['yhat'].tolist(),
                'lower_bound': forecast_df['yhat_lower'].tolist(),
                'upper_bound': forecast_df['yhat_upper'].tolist(),
                'dates': forecast_df['ds'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
                'confidence': 0.9,
                'model': 'prophet'
            }
        except Exception as e:
            logger.error(f"Prophet forecast failed: {e}")
            return {'forecast': [], 'confidence': 0.0}

    async def forecast_carbon(self, horizon_hours: int = 24) -> Dict:
        if not self.prophet_available or len(self.history_carbon) < 30:
            return {'forecast': [], 'confidence': 0.0}
        try:
            import pandas as pd
            df = pd.DataFrame(list(self.history_carbon))
            df = df.sort_values('ds')
            def run_prophet():
                model = Prophet(changepoint_prior_scale=0.05, seasonality_prior_scale=10)
                model.fit(df)
                future = model.make_future_dataframe(periods=horizon_hours)
                forecast = model.predict(future)
                return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(horizon_hours)
            forecast_df = await asyncio.to_thread(run_prophet)
            return {
                'forecast': forecast_df['yhat'].tolist(),
                'lower_bound': forecast_df['yhat_lower'].tolist(),
                'upper_bound': forecast_df['yhat_upper'].tolist(),
                'dates': forecast_df['ds'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
                'confidence': 0.9,
                'model': 'prophet'
            }
        except Exception as e:
            logger.error(f"Prophet forecast failed: {e}")
            return {'forecast': [], 'confidence': 0.0}

    def get_stats(self) -> Dict:
        return {'prophet_available': self.prophet_available, 'circularity_history_len': len(self.history_circularity), 'carbon_history_len': len(self.history_carbon)}

# ============================================================
# STUBS (unchanged)
# ============================================================
class AdaptiveThresholdManager:
    def __init__(self, thresholds: Dict):
        self.thresholds = thresholds
    async def record_performance(self, metrics: Dict): pass
    def get_thresholds(self) -> Dict: return self.thresholds

class EnhancedSubstitutionDatabase:
    def __init__(self):
        self.data = {}
    async def lookup(self, material: str) -> Optional[Dict]: return self.data.get(material)

class EnsembleCircularityPredictor:
    def __init__(self):
        self.is_trained = False
    async def train(self, data: List[Dict]): self.is_trained = True
    async def model_performance_monitor(self) -> Dict: return {'accuracy': 0.9}
    def update_performance(self, actual: float, predicted: float): pass

class ExplainableCircularityReport:
    def generate(self, metrics: HeliumCircularityMetrics) -> Dict:
        return {'summary': 'Report generated', 'metrics': asdict(metrics)}

class GPUMonteCarloSimulator:
    def __init__(self, use_gpu: bool):
        self.use_gpu = use_gpu
    async def simulate(self, params: Dict) -> Dict: return {'result': random.random()}

class PredictiveCircularityModel:
    def __init__(self):
        self.is_trained = False

class BlockchainCertification:
    def __init__(self):
        self.certificates = {}
    async def issue_certificate(self, record_id: str, data: Dict) -> str:
        cert_id = f"cert_{uuid.uuid4().hex[:8]}"
        self.certificates[cert_id] = {'record_id': record_id, 'data': data, 'issued_at': datetime.now()}
        return cert_id

class EnhancedAlertSystem:
    def __init__(self):
        self.threshold_manager = AdaptiveThresholdManager({})
    async def check_alerts(self, metrics: HeliumCircularityMetrics):
        if metrics.circularity_index < 0.5:
            logger.warning(f"Alert: circularity index low ({metrics.circularity_index:.3f})")

class EnhancedDataQualityScorer:
    def assess_quality(self, data: Dict) -> float:
        return 0.9

class HeliumSustainabilityTracker:
    async def get_sustainability_score(self) -> Dict:
        return {'overall_score': 0.8}

# ============================================================
# ENHANCED CIRCULARITY CALCULATOR – FULLY INTEGRATED
# ============================================================
class EnhancedHeliumCircularityCalculator:
    """
    Helium Circularity Calculator with full Green Agent MOPD integration.
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
        self.blockchain = BlockchainCircularityVerification(storage)
        self.carbon_manager = CarbonIntensityManager()
        self.autonomous_optimizer = AutonomousCircularityOptimizer(adaptive_cost)
        self.cloud_deployer = MultiCloudCircularityDeployment()
        self.cloud_storage = MultiCloudStorage()
        self.predictive = PredictiveAnalytics(storage)

        # Other components (stubs)
        self.adaptive_threshold_manager = AdaptiveThresholdManager({})
        self.enhanced_substitution_db = EnhancedSubstitutionDatabase()
        self.ensemble_predictor = EnsembleCircularityPredictor()
        self.explainable_report = ExplainableCircularityReport()
        self.gpu_simulator = GPUMonteCarloSimulator(central_config.enable_gpu if hasattr(central_config, 'enable_gpu') else True)
        self.ml_predictor = PredictiveCircularityModel() if central_config.enable_ml_predictions else None
        self.blockchain_cert = BlockchainCertification() if central_config.enable_blockchain else None
        self.alert_system = EnhancedAlertSystem()
        self.quality_scorer = EnhancedDataQualityScorer()
        self.sustainability_tracker = HeliumSustainabilityTracker()

        # State
        self.circularity_history: deque = deque(maxlen=10000)
        self.material_flows: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._history_lock = asyncio.Lock()
        self._flows_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._background_tasks = []

        logger.info(f"EnhancedHeliumCircularityCalculator v15.1 initialized (instance: {self.instance_id})")

    # ----------------------------------------------------------------------
    # Teacher interface for MOPD
    # ----------------------------------------------------------------------
    async def policy_probs(self, state: Dict) -> List[float]:
        """
        Return a probability distribution over circularity‑improvement strategies.
        This allows the MTPD optimizer to treat this module as a teacher.
        """
        # Use the bandit's current rewards to generate probabilities
        rewards = self.autonomous_optimizer.strategy_rewards
        strategies = list(self.autonomous_optimizer.optimization_strategies.keys())
        probs = np.array([rewards.get(s, 0.0) for s in strategies])
        # Softmax to get a probability distribution
        probs = np.exp(probs) / np.sum(np.exp(probs))
        return probs.tolist()

    # ----------------------------------------------------------------------
    # Core circularity calculation method
    # ----------------------------------------------------------------------
    async def calculate_comprehensive_circularity(self, input_data: Dict = None,
                                                  sign_data: bool = True,
                                                  blockchain_record: bool = True) -> HeliumCircularityMetrics:
        """
        Calculate circularity metrics and emit a FeedbackEvent.
        """
        # Assess input data quality
        if input_data:
            quality_score = self.quality_scorer.assess_quality(input_data)
        else:
            quality_score = 0.9

        # Simulate calculations (placeholders)
        recycling_rate = 0.7 + random.uniform(-0.1, 0.1)
        recovery_efficiency = 0.75 + random.uniform(-0.1, 0.1)
        collection_efficiency = 0.8 + random.uniform(-0.1, 0.1)
        purification_efficiency = 0.85 + random.uniform(-0.1, 0.1)

        # Circularity index
        weights = {'recycling': 0.3, 'recovery': 0.3, 'collection': 0.2, 'purification': 0.2}
        circularity_index = (
            weights['recycling'] * recycling_rate +
            weights['recovery'] * recovery_efficiency +
            weights['collection'] * collection_efficiency +
            weights['purification'] * purification_efficiency
        )

        if circularity_index >= 0.85:
            circularity_level = "excellent"
        elif circularity_index >= 0.70:
            circularity_level = "good"
        elif circularity_index >= 0.50:
            circularity_level = "moderate"
        else:
            circularity_level = "critical"

        record_id = f"circ_{uuid.uuid4().hex[:8]}"
        metrics = HeliumCircularityMetrics(
            record_id=record_id,
            circularity_index=circularity_index,
            circularity_level=circularity_level,
            recycling_rate=recycling_rate,
            recovery_efficiency=recovery_efficiency,
            collection_efficiency=collection_efficiency,
            purification_efficiency=purification_efficiency,
            data_quality_score=quality_score
        )

        # Quantum signing
        if sign_data:
            signature = await self.pqc.sign_data(asdict(metrics))
            metrics.quantum_signature = signature

        # Blockchain recording
        if blockchain_record:
            data_hash = hashlib.sha256(json.dumps(asdict(metrics), sort_keys=True, default=str).encode()).hexdigest()
            blockchain_result = await self.blockchain.record_circularity_data(record_id, data_hash, {'index': circularity_index})
            metrics.blockchain_tx_hash = blockchain_result.get('tx_hash')

        # Multi-cloud deployment
        deployment = await self.cloud_deployer.deploy_circularity_model({'size_mb': 0.5, 'features': len(self.circularity_history) + 1})
        metrics.cloud_deployment = deployment

        # Autonomous optimization
        state = {
            'circularity_index': circularity_index,
            'recycling_rate': recycling_rate,
            'recovery_efficiency': recovery_efficiency,
            'collection_efficiency': collection_efficiency
        }
        optimization = await self.autonomous_optimizer.optimize_circularity(state, 'hybrid')
        metrics.optimization_recommendation = optimization

        # Cloud storage backup
        if self.cloud_storage.providers:
            try:
                await self.cloud_storage.store(asdict(metrics), f"circularity_{record_id}.json")
            except Exception as e:
                logger.error(f"Cloud storage backup failed: {e}")

        # Record in history
        async with self._history_lock:
            self.circularity_history.append(metrics)

        # Store in central storage (extend Storage with methods)
        self.storage.store_circularity_record(metrics)

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"circ_{record_id}",
            selected_action="calculate_circularity",
            quality_score=quality_score,
            latency_ms=0.0,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="circularity",
            adaptive_cost_value=0.0,
            state={'input': input_data},
            candidates=[{'action': s} for s in self.autonomous_optimizer.optimization_strategies.keys()],
            source="helium_circularity",
            environment=central_config.ENVIRONMENT,
            tags=["circularity", "helium"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        # Check drift
        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

        # Update metrics
        self.metrics.set_circularity_score(circularity_index)

        logger.info(f"Circularity calculation completed: index={circularity_index:.3f}, level={circularity_level}")
        return metrics

    # ----------------------------------------------------------------------
    # Lifecycle management
    # ----------------------------------------------------------------------
    async def start(self):
        """Start background tasks."""
        logger.info("Starting Helium Circularity Calculator...")
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
                    if self.circularity_history:
                        recent = list(self.circularity_history)[-10:]
                        state = {
                            'circularity_index': np.mean([m.circularity_index for m in recent]),
                            'recycling_rate': np.mean([m.recycling_rate for m in recent]),
                            'recovery_efficiency': np.mean([m.recovery_efficiency for m in recent]),
                            'collection_efficiency': np.mean([m.collection_efficiency for m in recent])
                        }
                result = await self.autonomous_optimizer.optimize_circularity(state, 'hybrid')
                logger.info(f"Autonomous optimization: {result}")
            except Exception as e:
                logger.error(f"Optimization loop error: {e}")

    async def _predictive_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                async with self._history_lock:
                    if self.circularity_history:
                        latest = self.circularity_history[-1]
                        await self.predictive.update_history(latest.circularity_index, 400)
                        forecast = await self.predictive.forecast_circularity()
                        logger.info(f"Circularity index forecast: {forecast}")
            except Exception as e:
                logger.error(f"Predictive loop error: {e}")

    async def _cleanup_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(86400)
            try:
                self.storage.clean_old_circularity_records(days=central_config.data_retention_days or 365)
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

    async def shutdown(self):
        logger.info("Shutting down Helium Circularity Calculator...")
        self._shutdown_event.set()
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.carbon_manager.close()
        logger.info("Shutdown complete")

# ============================================================
# SINGLETON ACCESSOR
# ============================================================
_circularity_calculator_instance = None
_circularity_calculator_lock = asyncio.Lock()

async def get_circularity_calculator(storage: Storage, queue: AsyncMessageQueue,
                                     adaptive_cost: AdaptiveCostFunction,
                                     pareto_gating: ParetoGating,
                                     drift_detector: DriftDetector,
                                     metrics: MetricsRegistry) -> EnhancedHeliumCircularityCalculator:
    global _circularity_calculator_instance
    if _circularity_calculator_instance is None:
        async with _circularity_calculator_lock:
            if _circularity_calculator_instance is None:
                _circularity_calculator_instance = EnhancedHeliumCircularityCalculator(
                    storage, queue, adaptive_cost, pareto_gating, drift_detector, metrics
                )
                await _circularity_calculator_instance.start()
    return _circularity_calculator_instance

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

    calculator = await get_circularity_calculator(storage, queue, adaptive_cost, pareto, drift, metrics)

    # Calculate circularity
    metrics = await calculator.calculate_comprehensive_circularity()
    print(f"Circularity Index: {metrics.circularity_index:.3f}, Level: {metrics.circularity_level}")

    # Shutdown
    await calculator.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
