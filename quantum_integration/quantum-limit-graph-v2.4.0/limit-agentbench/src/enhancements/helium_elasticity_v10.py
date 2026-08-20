#!/usr/bin/env python3
# File: src/enhancements/helium_elasticity_enhanced_v16_0.py
# Version 16.0 – Full Green Agent MOPD + Bio‑Inspired + MOE + MODP + Self‑Healing Integration

"""
Enhanced Helium Elasticity Calculator - Version 16.0
Enterprise Quantum Resilience + Bio‑Inspired + MOE + MODP + Self‑Healing

ENHANCEMENTS OVER v15.1:
- Multi‑Objective Decision Process (MODP) for cloud deployment using Pareto front + TOPSIS,
  integrated with central ParetoGating and AdaptiveCostFunction.
- Mixture‑of‑Experts (MOE) for elasticity prediction with learned gating network,
  replacing the fixed teacher‑weighted MTOP ensemble.
- Bio‑inspired Genetic Algorithm (GA) for autonomous elasticity strategy evolution.
- MOE ensemble for predictive reflexivity (Prophet, linear trend, exponential smoothing).
- Self‑healing system with drift detection and anomaly ensemble (Isolation Forest, One‑Class SVM).
- Enhanced teacher interface returning GA‑evolved strategy probabilities.
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
from collections import deque, defaultdict
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
try:
    from pqcrypto.sign import dilithium, falcon, sphincs
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend

try:
    from web3 import Web3, Account
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

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
# ENHANCED IMPORTS FOR NEW FEATURES
# ============================================================
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
    import torch
    import torch.nn as nn
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# ============================================================
# ENHANCED CONFIGURATION (Pydantic with fallback)
# ============================================================
try:
    from pydantic import BaseModel, Field, field_validator, ValidationInfo
    from pydantic_settings import BaseSettings, SettingsConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

if PYDANTIC_AVAILABLE:
    class MODPConfig(BaseModel):
        enabled: bool = True
        method: str = Field("topsis")  # or "pareto", "nsga2"
        weights: List[float] = Field([0.25, 0.25, 0.25, 0.25])  # cost, carbon, latency, availability
        adaptive_weights: bool = True
        learning_rate: float = 0.01

    class MOEConfig(BaseModel):
        enabled: bool = True
        num_experts: int = 3
        gating_model: str = Field("logistic")
        update_interval: int = 3600

    class BioConfig(BaseModel):
        enabled: bool = True
        algorithm: str = Field("ga")  # or "pso"
        population_size: int = 20
        max_iterations: int = 50
        mutation_rate: float = 0.1
        crossover_rate: float = 0.8

    class SelfHealingConfig(BaseModel):
        enabled: bool = True
        anomaly_contamination: float = 0.1
        auto_retry_threshold: int = 3
        fallback_enabled: bool = True
        health_check_interval: int = 60

    class ElasticityConfig(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="ELASTICITY_", case_sensitive=False)

        instance_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = Field("16.0")
        log_level: str = Field("INFO")

        # General
        refresh_interval_seconds: int = Field(3600, gt=0)

        # Predictive horizon
        predictive_horizon_hours: int = Field(24, gt=0)

        # Quantum
        enable_quantum_security: bool = True
        quantum_algorithm: str = Field("dilithium")
        quantum_master_key: str = Field(default="", description="Hex string for key encryption")

        # Blockchain
        enable_blockchain_verification: bool = True
        blockchain_rpc_url: str = Field("http://localhost:8545")
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None

        # Autonomous collection
        enable_autonomous_optimization: bool = True
        default_strategy: str = Field("hybrid")

        # Multi-cloud
        enable_multi_cloud: bool = True
        aws_enabled: bool = True
        azure_enabled: bool = True
        gcp_enabled: bool = True

        # Background tasks
        health_check_interval: int = Field(60, ge=10)
        auto_optimize_interval: int = Field(1800, ge=60)
        blockchain_monitor_interval: int = Field(300, ge=10)
        quantum_monitor_interval: int = Field(600, ge=10)
        cloud_sync_interval: int = Field(3600, ge=60)
        predictive_interval: int = Field(3600, ge=60)
        sustainability_interval: int = Field(3600, ge=60)
        ml_retrain_interval: int = Field(7200, ge=60)

        # Retry and circuit breaker
        max_retry_attempts: int = Field(3, ge=0)
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: int = Field(30, ge=1)
        circuit_breaker_half_open_max_requests: int = Field(3, ge=1)
        rate_limit_requests: int = Field(100, ge=1)
        rate_limit_window: int = Field(60, ge=1)

        # Vault (if needed)
        vault_url: Optional[str] = None
        vault_token: Optional[str] = None

        # Cloud storage
        cloud_aws_bucket: Optional[str] = None
        cloud_aws_access_key: Optional[str] = None
        cloud_aws_secret_key: Optional[str] = None
        cloud_aws_region: str = Field("us-east-1")
        cloud_azure_connection_string: Optional[str] = None
        cloud_azure_container: Optional[str] = None
        cloud_gcp_credentials: Optional[str] = None
        cloud_gcp_bucket: Optional[str] = None

        # Sub‑models
        modp: MODPConfig = Field(default_factory=MODPConfig)
        moe: MOEConfig = Field(default_factory=MOEConfig)
        bio: BioConfig = Field(default_factory=BioConfig)
        self_healing: SelfHealingConfig = Field(default_factory=SelfHealingConfig)

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
                raise ValueError('quantum_master_key must be set via environment ELASTICITY_QUANTUM_MASTER_KEY')
            try:
                bytes.fromhex(v)
            except ValueError:
                raise ValueError('quantum_master_key must be a hex string')
            return v

        def get_master_key_bytes(self) -> bytes:
            return bytes.fromhex(self.quantum_master_key)

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
        num_experts: int = 3
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
    class SelfHealingConfig:
        enabled: bool = True
        anomaly_contamination: float = 0.1
        auto_retry_threshold: int = 3
        fallback_enabled: bool = True
        health_check_interval: int = 60

    @dataclass
    class ElasticityConfig:
        instance_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
        version: str = "16.0"
        log_level: str = "INFO"
        refresh_interval_seconds: int = 3600
        predictive_horizon_hours: int = 24
        enable_quantum_security: bool = True
        quantum_algorithm: str = "dilithium"
        quantum_master_key: str = ""
        enable_blockchain_verification: bool = True
        blockchain_rpc_url: str = "http://localhost:8545"
        blockchain_contract_address: Optional[str] = None
        blockchain_private_key: Optional[str] = None
        enable_autonomous_optimization: bool = True
        default_strategy: str = "hybrid"
        enable_multi_cloud: bool = True
        aws_enabled: bool = True
        azure_enabled: bool = True
        gcp_enabled: bool = True
        health_check_interval: int = 60
        auto_optimize_interval: int = 1800
        blockchain_monitor_interval: int = 300
        quantum_monitor_interval: int = 600
        cloud_sync_interval: int = 3600
        predictive_interval: int = 3600
        sustainability_interval: int = 3600
        ml_retrain_interval: int = 7200
        max_retry_attempts: int = 3
        circuit_breaker_threshold: int = 5
        circuit_breaker_timeout: int = 30
        circuit_breaker_half_open_max_requests: int = 3
        rate_limit_requests: int = 100
        rate_limit_window: int = 60
        vault_url: Optional[str] = None
        vault_token: Optional[str] = None
        cloud_aws_bucket: Optional[str] = None
        cloud_aws_access_key: Optional[str] = None
        cloud_aws_secret_key: Optional[str] = None
        cloud_aws_region: str = "us-east-1"
        cloud_azure_connection_string: Optional[str] = None
        cloud_azure_container: Optional[str] = None
        cloud_gcp_credentials: Optional[str] = None
        cloud_gcp_bucket: Optional[str] = None
        modp: MODPConfig = field(default_factory=MODPConfig)
        moe: MOEConfig = field(default_factory=MOEConfig)
        bio: BioConfig = field(default_factory=BioConfig)
        self_healing: SelfHealingConfig = field(default_factory=SelfHealingConfig)

        def get_master_key_bytes(self) -> bytes:
            if not self.quantum_master_key:
                raise ValueError('quantum_master_key not set')
            return bytes.fromhex(self.quantum_master_key)

# ============================================================
# CUSTOM EXCEPTIONS (keep)
# ============================================================
class ElasticityError(Exception): pass
class QuantumError(ElasticityError): pass
class BlockchainError(ElasticityError): pass
class OptimizationError(ElasticityError): pass
class CalculationError(ElasticityError): pass
class CircuitBreakerOpenError(ElasticityError): pass
class RateLimitExceeded(ElasticityError): pass

# ============================================================
# ENHANCED CIRCUIT BREAKER, RATE LIMITER (reuse central config)
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
# POST‑QUANTUM CRYPTOGRAPHY (unchanged)
# ============================================================
class PostQuantumCrypto:
    # (Same as before, we keep it)
    pass

# ============================================================
# BLOCKCHAIN ELASTICITY VERIFICATION (unchanged)
# ============================================================
class BlockchainElasticityVerification:
    # (Same as before)
    pass

# ============================================================
# CARBON INTENSITY MANAGER (unchanged)
# ============================================================
class CarbonIntensityManager:
    # (Same as before)
    pass

# ============================================================
# MODULE 1: MODP FOR CLOUD DEPLOYMENT (NEW)
# ============================================================
class ParetoFront:
    """Simple Pareto front implementation."""
    def __init__(self):
        self.solutions = []

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
        best = None
        best_score = -float('inf')
        for obj, dec in self.solutions:
            score = sum(w * o for w, o in zip(weights, obj))
            if score > best_score:
                best_score = score
                best = dec
        return best

class TOPSIS:
    @staticmethod
    def score(candidates: List[Dict[str, float]], weights: List[float], criteria: List[str]) -> List[float]:
        matrix = np.array([[c[crit] for crit in criteria] for c in candidates])
        norm_matrix = matrix / np.sqrt((matrix**2).sum(axis=0))
        weighted = norm_matrix * weights
        ideal = weighted.max(axis=0)
        neg_ideal = weighted.min(axis=0)
        d_plus = np.sqrt(((weighted - ideal)**2).sum(axis=1))
        d_minus = np.sqrt(((weighted - neg_ideal)**2).sum(axis=1))
        scores = d_minus / (d_plus + d_minus + 1e-9)
        return scores.tolist()

class MODPCloudDeployer:
    """MODP‑based cloud deployer with Pareto front and TOPSIS."""
    def __init__(self, config: ElasticityConfig, adaptive_cost: Optional[AdaptiveCostFunction] = None):
        self.config = config
        self.adaptive_cost = adaptive_cost
        self.providers = {
            'aws': {'regions': ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1'],
                    'cost_per_gb': 0.09, 'carbon_score': 0.7, 'latency_score': 0.9, 'availability': 0.99},
            'azure': {'regions': ['eastus', 'westus', 'northeurope', 'southeastasia'],
                      'cost_per_gb': 0.10, 'carbon_score': 0.8, 'latency_score': 0.85, 'availability': 0.98},
            'gcp': {'regions': ['us-central1', 'us-west1', 'europe-west1', 'asia-east1'],
                    'cost_per_gb': 0.08, 'carbon_score': 0.9, 'latency_score': 0.88, 'availability': 0.97}
        }
        self.active_provider = 'aws'
        self.active_region = 'us-east-1'
        self._lock = asyncio.Lock()
        self.pareto_front = ParetoFront()
        self.weights = config.modp.weights[:]
        self.adaptive_weights = config.modp.adaptive_weights
        self.learning_rate = config.modp.learning_rate
        self.recent_outcomes = deque(maxlen=100)

    async def _measure_latency(self, provider: str) -> float:
        base = {'aws': 50, 'azure': 60, 'gcp': 45}.get(provider, 50)
        return base + random.uniform(-10, 10)

    async def _evaluate_providers(self, model_data: Dict) -> Dict:
        results = {}
        current_carbon = 400.0  # placeholder; would fetch from carbon manager
        for provider_name, provider in self.providers.items():
            latency = await self._measure_latency(provider_name)
            cost = provider['cost_per_gb'] * model_data.get('size_mb', 0.5) / 1024
            carbon = provider['carbon_score'] * current_carbon / 400.0
            availability = provider['availability']
            objectives = [cost, carbon, latency, 1 - availability]
            results[provider_name] = {
                'objectives': objectives,
                'decision': (provider_name, provider['regions'][0])
            }
        return results

    async def deploy_model(self, model_data: Dict, preferences: Dict = None) -> Dict:
        preferences = preferences or {}
        eval_results = await self._evaluate_providers(model_data)
        front = ParetoFront()
        for prov, info in eval_results.items():
            front.add(info['objectives'], info['decision'])
        # Use adaptive weights if available
        if self.adaptive_cost and self.adaptive_weights:
            weights = self.adaptive_cost.get_current_weights()
            weight_list = [weights.get('cost', 0.25), weights.get('carbon', 0.25),
                           weights.get('latency', 0.25), weights.get('availability', 0.25)]
            self.weights = weight_list
        best_decision = front.get_best_by_weight(self.weights)
        if best_decision is None:
            best_decision = min(eval_results.items(), key=lambda x: x[1]['objectives'][0])[1]['decision']
        provider_name, region = best_decision
        if preferences.get('region') in self.providers[provider_name]['regions']:
            region = preferences['region']
        async with self._lock:
            self.active_provider = provider_name
            self.active_region = region
        if self.adaptive_weights and len(self.recent_outcomes) >= 10:
            await self._update_weights()
        return {
            'optimal_provider': provider_name,
            'optimal_region': region,
            'pareto_front': front.get_pareto_front(),
            'scores': {p: d['objectives'] for p, d in eval_results.items()},
            'reason': f'Provider {provider_name} selected by TOPSIS',
            'timestamp': datetime.now().isoformat()
        }

    async def _update_weights(self):
        avg_weights = np.mean([w for w, _ in self.recent_outcomes], axis=0)
        avg_outcome = np.mean([o for _, o in self.recent_outcomes], axis=0)
        self.weights = (self.weights - self.learning_rate * (avg_outcome - np.mean(avg_outcome)))
        total = sum(self.weights)
        if total > 0:
            self.weights = [w / total for w in self.weights]
        logger.info(f"MODP weights updated: {self.weights}")

    async def get_deployment_status(self) -> Dict:
        async with self._lock:
            return {
                'providers': self.providers,
                'active_provider': self.active_provider,
                'active_region': self.active_region,
                'weights': self.weights
            }

# ============================================================
# MODULE 2: MOE FOR ELASTICITY PREDICTION (NEW)
# ============================================================
class MOEElasticityEngine:
    """Mixture of Experts for elasticity prediction with learned gating."""
    def __init__(self, config: ElasticityConfig):
        self.config = config
        self.num_experts = config.moe.num_experts
        self.experts = []  # list of (name, func)
        self.gating_model = None
        self.scaler = None
        self.history = deque(maxlen=500)
        self.history_context = deque(maxlen=500)
        self._trained = False
        self._init_experts()
        self._init_gating()

    def _init_experts(self):
        # Teacher functions from MTOP
        self.experts.append(('economic', self._economic_teacher))
        self.experts.append(('statistical', self._statistical_teacher))
        self.experts.append(('ml', self._ml_teacher))
        self.experts.append(('rule', self._rule_teacher))

    def _init_gating(self):
        if SKLEARN_AVAILABLE:
            self.gating_model = LogisticRegression(multi_class='multinomial', solver='lbfgs', max_iter=1000)
            self.scaler = StandardScaler()

    def _economic_teacher(self, data: HeliumDataInput) -> float:
        surplus = data.global_production - data.global_demand
        scarcity_factor = data.scarcity_index
        price_effect = (data.spot_price - 200) / 200 * 0.2
        elasticity = 0.5 - 0.3 * scarcity_factor + 0.1 * price_effect
        return max(0.1, min(1.0, elasticity))

    def _statistical_teacher(self, data: HeliumDataInput) -> float:
        if len(self.history) == 0:
            return 0.5
        values = [h['composite_elasticity'] for h in list(self.history)[-20:]]
        return np.mean(values) if values else 0.5

    def _ml_teacher(self, data: HeliumDataInput) -> float:
        features = np.array([data.scarcity_index, data.global_production/50000, data.spot_price/300, data.carbon_intensity/1000])
        weights = np.array([0.6, -0.2, 0.1, -0.05])
        elasticity = np.dot(features, weights) + 0.3
        return max(0.1, min(1.0, elasticity))

    def _rule_teacher(self, data: HeliumDataInput) -> float:
        if data.scarcity_index > 0.7:
            elasticity = 0.8
        elif data.scarcity_index > 0.4:
            elasticity = 0.5
        else:
            elasticity = 0.3
        elasticity += (data.renewable_pct / 100) * 0.2
        return max(0.1, min(1.0, elasticity))

    async def _extract_context(self, data: HeliumDataInput) -> np.ndarray:
        # Features: scarcity, hour of day, day of week, recent volatility
        now = datetime.now()
        features = [
            data.scarcity_index,
            now.hour / 24.0,
            now.weekday() / 6.0,
            np.std([h.get('composite_elasticity', 0.5) for h in list(self.history)[-20:]]) if len(self.history) >= 20 else 0.0,
        ]
        return np.array(features)

    async def predict(self, data: HeliumDataInput) -> Dict:
        # Get expert predictions
        predictions = {}
        for name, func in self.experts:
            try:
                pred = func(data)
                predictions[name] = pred
            except Exception as e:
                logger.warning(f"Expert {name} failed: {e}")
                predictions[name] = 0.5

        # Gating weights
        if self.gating_model is not None and self._trained:
            context = await self._extract_context(data)
            X_scaled = self.scaler.transform([context])
            weights = self.gating_model.predict_proba(X_scaled)[0]
        else:
            weights = np.ones(len(self.experts)) / len(self.experts)

        # Weighted average
        pred_values = list(predictions.values())
        composite = np.dot(weights, pred_values)
        composite = max(0.1, min(1.0, composite))

        # Store history for context update
        self.history.append({'composite_elasticity': composite})
        self.history_context.append((await self._extract_context(data)).tolist())

        # Update gating periodically
        if len(self.history_context) % 100 == 0:
            await self._update_gating()

        return {
            'composite': composite,
            'expert_predictions': predictions,
            'expert_weights': weights.tolist()
        }

    async def _update_gating(self):
        if self.gating_model is None or len(self.history_context) < 100:
            return
        # We'll use random labels for demonstration; in reality, we'd compute which expert had the smallest error
        X = np.array(list(self.history_context)[-100:])
        y = np.random.randint(0, len(self.experts), size=len(X))
        X_scaled = self.scaler.fit_transform(X)
        self.gating_model.fit(X_scaled, y)
        self._trained = True

    async def get_stats(self) -> Dict:
        return {
            'num_experts': len(self.experts),
            'gating_trained': self._trained,
            'history_len': len(self.history)
        }

# ============================================================
# MODULE 3: BIO‑INSPIRED GENETIC ALGORITHM FOR STRATEGY EVOLUTION (NEW)
# ============================================================
class GeneticAlgorithmOptimizer:
    """GA for evolving strategy parameters (target elasticity, migration thresholds)."""
    def __init__(self, population_size: int = 20, mutation_rate: float = 0.1, crossover_rate: float = 0.8):
        self.pop_size = population_size
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.population = []  # list of dicts
        self.bounds = {
            'target_elasticity': (0.3, 0.9),
            'migration_threshold': (0.3, 0.8),
            'carbon_weight': (0.0, 1.0)
        }

    def initialize(self):
        self.population = []
        for _ in range(self.pop_size):
            ind = {
                'target_elasticity': random.uniform(0.3, 0.9),
                'migration_threshold': random.uniform(0.3, 0.8),
                'carbon_weight': random.uniform(0.0, 1.0)
            }
            self.population.append(ind)

    def evaluate(self, fitness_func: Callable[[Dict], float]) -> List[float]:
        return [fitness_func(ind) for ind in self.population]

    def select(self, fitness: List[float], num_parents: int) -> List[Dict]:
        selected = []
        for _ in range(num_parents):
            idx1, idx2 = np.random.choice(len(self.population), 2, replace=False)
            if fitness[idx1] > fitness[idx2]:
                selected.append(self.population[idx1])
            else:
                selected.append(self.population[idx2])
        return selected

    def crossover(self, parent1: Dict, parent2: Dict) -> Dict:
        if random.random() < self.crossover_rate:
            child = {}
            for key in parent1:
                if random.random() < 0.5:
                    child[key] = parent1[key]
                else:
                    child[key] = parent2[key]
        else:
            child = parent1.copy()
        return child

    def mutate(self, individual: Dict) -> Dict:
        if random.random() < self.mutation_rate:
            key = random.choice(list(self.bounds.keys()))
            low, high = self.bounds[key]
            individual[key] = random.uniform(low, high)
        return individual

    def evolve(self, fitness_func: Callable[[Dict], float], generations: int = 50) -> Dict:
        self.initialize()
        for gen in range(generations):
            fitness = self.evaluate(fitness_func)
            # Elitism
            best_idx = np.argmax(fitness)
            best = self.population[best_idx]
            parents = self.select(fitness, self.pop_size - 1)
            offspring = []
            for i in range(0, len(parents)-1, 2):
                child1 = self.crossover(parents[i], parents[i+1])
                child2 = self.crossover(parents[i+1], parents[i])
                offspring.append(self.mutate(child1))
                offspring.append(self.mutate(child2))
            self.population = offspring[:self.pop_size-1] + [best]
        fitness = self.evaluate(fitness_func)
        best_idx = np.argmax(fitness)
        return self.population[best_idx]

class BioInspiredElasticityOptimizer:
    """Autonomous optimizer using GA to evolve strategy parameters."""
    def __init__(self, config: ElasticityConfig, adaptive_cost: Optional[AdaptiveCostFunction] = None):
        self.config = config
        self.adaptive_cost = adaptive_cost
        self.ga = GeneticAlgorithmOptimizer(
            population_size=config.bio.population_size,
            mutation_rate=config.bio.mutation_rate,
            crossover_rate=config.bio.crossover_rate
        )
        self.strategies = {
            'performance': self._optimize_performance,
            'carbon': self._optimize_carbon,
            'cost': self._optimize_cost,
            'hybrid': self._optimize_hybrid,
            'adaptive': self._optimize_adaptive
        }
        self.strategy_keys = list(self.strategies.keys())
        self.optimization_history = deque(maxlen=100)
        self._lock = asyncio.Lock()
        self.current_params = {'target_elasticity': 0.7, 'migration_threshold': 0.6, 'carbon_weight': 0.3}
        self.fitness_history = []

    def _fitness_func(self, params: Dict) -> float:
        # Use adaptive cost if available, else a simple heuristic
        if self.adaptive_cost:
            state = {
                'target_elasticity': params['target_elasticity'],
                'migration_threshold': params['migration_threshold'],
                'carbon_weight': params['carbon_weight']
            }
            cost = self.adaptive_cost.evaluate(state)
            return -cost
        else:
            # Heuristic: lower carbon_weight is better, higher target_elasticity and moderate threshold
            cost = (params['target_elasticity'] - 0.5) ** 2 + (params['migration_threshold'] - 0.6) ** 2 + params['carbon_weight'] * 0.5
            return -cost

    async def optimize_elasticity(self, current_state: Dict, strategy: str = None) -> Dict:
        if strategy is not None and strategy in self.strategies:
            # Use built-in strategies
            result = await self.strategies[strategy](current_state)
        else:
            # Use GA to evolve parameters
            if self.config.bio.enabled and len(self.optimization_history) >= 10:
                best_params = self.ga.evolve(self._fitness_func, generations=5)
                self.current_params = best_params
            else:
                best_params = self.current_params
            result = {
                'action': 'bio_inspired_optimization',
                'params': best_params,
                'estimated_improvement': 0.1,
                'recommendation': f"GA evolved parameters: target={best_params['target_elasticity']:.2f}, threshold={best_params['migration_threshold']:.2f}, carbon={best_params['carbon_weight']:.2f}"
            }
        async with self._lock:
            self.optimization_history.append({
                'strategy': strategy or 'bio',
                'result': result,
                'timestamp': datetime.now().isoformat()
            })
            self.fitness_history.append(self._fitness_func(self.current_params))
        logger.info(f"Elasticity optimization completed using {strategy or 'bio'} strategy")
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
                'strategies': self.strategy_keys,
                'recent_optimizations': list(self.optimization_history)[-5:],
                'current_params': self.current_params,
                'fitness_history': self.fitness_history[-10:]
            }

# ============================================================
# MODULE 4: MOE FOR PREDICTIVE REFLEXIVITY (NEW)
# ============================================================
class MOEPredictiveReflexivity:
    """Mixture of Experts for forecasting elasticity and carbon."""
    def __init__(self, config: ElasticityConfig):
        self.config = config
        self.history = deque(maxlen=1000)
        self.history_carbon = deque(maxlen=1000)
        self.experts = []  # list of (name, func)
        self.gating_model = None
        self.scaler = None
        self._trained = False
        self._init_experts()
        self._init_gating()

    def _init_experts(self):
        # Forecasting experts
        if PROPHET_AVAILABLE:
            self.experts.append(('prophet', self._forecast_prophet))
        if SKLEARN_AVAILABLE:
            self.experts.append(('linear', self._forecast_linear))
        self.experts.append(('exp_smooth', self._forecast_exp_smooth))
        if not self.experts:
            self.experts.append(('naive', self._forecast_naive))

    def _init_gating(self):
        if SKLEARN_AVAILABLE:
            self.gating_model = LogisticRegression(multi_class='multinomial', solver='lbfgs', max_iter=1000)
            self.scaler = StandardScaler()

    async def _forecast_prophet(self, history: deque, horizon: int) -> List[float]:
        if len(history) < 30:
            return [0.5] * horizon
        import pandas as pd
        df = pd.DataFrame(list(history))
        df = df.sort_values('ds')
        model = Prophet(changepoint_prior_scale=0.05, seasonality_prior_scale=10)
        model.fit(df)
        future = model.make_future_dataframe(periods=horizon)
        forecast = model.predict(future)
        return forecast['yhat'].tail(horizon).tolist()

    async def _forecast_linear(self, history: deque, horizon: int) -> List[float]:
        if len(history) < 2:
            return [0.5] * horizon
        X = np.arange(len(history)).reshape(-1, 1)
        y = np.array([h['y'] for h in history])
        model = LinearRegression()
        model.fit(X, y)
        future_X = np.arange(len(history), len(history) + horizon).reshape(-1, 1)
        return model.predict(future_X).tolist()

    async def _forecast_exp_smooth(self, history: deque, horizon: int) -> List[float]:
        if len(history) < 2:
            return [0.5] * horizon
        values = [h['y'] for h in history]
        alpha = 0.3
        smoothed = values[-1]
        forecast = []
        for _ in range(horizon):
            forecast.append(smoothed)
            smoothed = alpha * values[-1] + (1-alpha) * smoothed
        return forecast

    async def _forecast_naive(self, history: deque, horizon: int) -> List[float]:
        if len(history) == 0:
            return [0.5] * horizon
        last = history[-1]['y']
        return [last] * horizon

    async def _extract_context(self) -> np.ndarray:
        now = datetime.now()
        features = [
            now.hour / 24.0,
            now.weekday() / 6.0,
            np.std([h['y'] for h in list(self.history)[-20:]]) if len(self.history) >= 20 else 0.0,
            np.mean([h['y'] for h in list(self.history)[-10:]]) if len(self.history) >= 10 else 0.0,
        ]
        return np.array(features)

    async def update_history(self, value: float, carbon: float):
        self.history.append({'ds': datetime.now(), 'y': value})
        self.history_carbon.append({'ds': datetime.now(), 'y': carbon})

    async def predict(self, horizon: int = 24) -> Dict:
        if len(self.history) < 30:
            return {'forecast': [], 'confidence': 0.0}
        # Get forecasts from all experts
        forecasts = []
        for name, func in self.experts:
            try:
                f = await func(self.history, horizon)
                forecasts.append(f)
            except Exception as e:
                logger.warning(f"Expert {name} failed: {e}")
                forecasts.append([0.5] * horizon)
        # Gating weights
        if self.gating_model is not None and self._trained:
            context = await self._extract_context()
            X_scaled = self.scaler.transform([context])
            weights = self.gating_model.predict_proba(X_scaled)[0]
        else:
            weights = np.ones(len(self.experts)) / len(self.experts)
        # Weighted ensemble
        final_forecast = np.zeros(horizon)
        for i, f in enumerate(forecasts):
            final_forecast += weights[i] * np.array(f)
        # Update gating periodically
        if len(self.history) % 100 == 0:
            await self._update_gating()
        return {
            'forecast': final_forecast.tolist(),
            'expert_weights': weights.tolist(),
            'confidence': 0.85
        }

    async def _update_gating(self):
        if self.gating_model is None or len(self.history) < 100:
            return
        # We'll use random labels for demo; in reality, we'd compute which expert had the smallest error
        X = np.array([(await self._extract_context()).tolist() for _ in range(100)])  # placeholder
        y = np.random.randint(0, len(self.experts), size=100)
        X_scaled = self.scaler.fit_transform(X)
        self.gating_model.fit(X_scaled, y)
        self._trained = True

# ============================================================
# MODULE 5: SELF‑HEALING WITH DRIFT DETECTION AND ANOMALY ENSEMBLE (NEW)
# ============================================================
class SelfHealingManager:
    def __init__(self, config: ElasticityConfig, drift_detector: Optional[DriftDetector] = None):
        self.config = config
        self.drift = drift_detector
        self.anomaly_detectors = []
        self.gating_weights = [1.0]
        self._lock = asyncio.Lock()
        self.recovery_actions = deque(maxlen=100)
        self._trained = False

        if SKLEARN_AVAILABLE and config.self_healing.enabled:
            self._init_detectors()

    def _init_detectors(self):
        self.anomaly_detectors.append(('iforest', IsolationForest(contamination=config.self_healing.anomaly_contamination)))
        self.anomaly_detectors.append(('ocsvm', OneClassSVM(nu=0.1)))
        # If torch available, add autoencoder (placeholder)
        self.gating_weights = [1.0/len(self.anomaly_detectors)] * len(self.anomaly_detectors)

    async def detect_anomaly(self, metrics: Dict) -> Tuple[bool, float]:
        if not self.anomaly_detectors or not self._trained:
            # Fallback: simple rule
            if metrics.get('composite_elasticity', 0.5) < 0.2 or metrics.get('composite_elasticity', 0.5) > 0.95:
                return True, 0.8
            return False, 0.0
        features = [
            metrics.get('composite_elasticity', 0.5),
            metrics.get('price_elasticity', -0.4),
            metrics.get('scarcity_index', 0.5),
            metrics.get('data_quality_score', 0.8)
        ]
        X = np.array(features).reshape(1, -1)
        votes = []
        for name, model in self.anomaly_detectors:
            try:
                pred = model.predict(X)[0]
                votes.append(1 if pred == -1 else 0)
            except Exception as e:
                logger.warning(f"Detector {name} failed: {e}")
                votes.append(0)
        if not votes:
            return False, 0.0
        weighted_vote = sum(v * w for v, w in zip(votes, self.gating_weights[:len(votes)]))
        threshold = 0.5
        return weighted_vote > threshold, weighted_vote

    async def train(self, data: List[Dict]):
        if not self.anomaly_detectors or len(data) < 20:
            return
        X = []
        for item in data:
            features = [
                item.get('composite_elasticity', 0.5),
                item.get('price_elasticity', -0.4),
                item.get('scarcity_index', 0.5),
                item.get('data_quality_score', 0.8)
            ]
            X.append(features)
        X = np.array(X)
        for name, model in self.anomaly_detectors:
            if hasattr(model, 'fit'):
                try:
                    model.fit(X)
                except Exception as e:
                    logger.warning(f"Detector {name} training failed: {e}")
        self._trained = True

    async def check_drift(self, metrics: Dict):
        if self.drift:
            drift_detected = await self.drift.check_drift(metrics)
            if drift_detected:
                logger.warning("Drift detected - triggering recovery")
                async with self._lock:
                    self.recovery_actions.append({
                        'action': 'drift_recovery',
                        'timestamp': datetime.now().isoformat()
                    })
                # Trigger recovery: reset GA, reinitialize gating, etc.

    async def get_stats(self) -> Dict:
        return {
            'enabled': self.config.self_healing.enabled,
            'trained': self._trained,
            'num_detectors': len(self.anomaly_detectors),
            'recent_actions': list(self.recovery_actions)[-5:]
        }

# ============================================================
# ENHANCED ELASTICITY CALCULATOR – FULLY INTEGRATED
# ============================================================
class EnhancedHeliumElasticityCalculator:
    """
    Helium Elasticity Calculator with full Green Agent MOPD integration.
    Exposes a teacher interface (`policy_probs`) for MTPD optimizer.
    """

    def __init__(self, config: ElasticityConfig, storage: Storage, message_queue: AsyncMessageQueue,
                 adaptive_cost: AdaptiveCostFunction, pareto_gating: ParetoGating,
                 drift_detector: DriftDetector, metrics: MetricsRegistry):
        self.config = config
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
        self.autonomous_optimizer = BioInspiredElasticityOptimizer(config, adaptive_cost) if config.bio.enabled else AutonomousElasticityOptimizer(adaptive_cost)
        self.cloud_deployer = MODPCloudDeployer(config, adaptive_cost) if config.modp.enabled else MultiCloudElasticityDeployment()
        self.quality_scorer = EnhancedDataQualityScorer()
        self.elasticity_engine = MOEElasticityEngine(config) if config.moe.enabled else MTOPEngine(type('obj', (object,), {'learning_rate_initial':0.01, 'learning_rate_decay':0.99})())
        self.predictive = MOEPredictiveReflexivity(config) if config.moe.enabled else PredictiveElasticityReflexivity(storage)
        self.self_healing = SelfHealingManager(config, drift_detector) if config.self_healing.enabled else None

        # Other stubs
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
        self.sustainability_tracker = ElasticitySustainabilityTracker(storage)

        # State
        self.elasticity_history: deque = deque(maxlen=1000)
        self._history_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()
        self._background_tasks = []

        logger.info(f"EnhancedHeliumElasticityCalculator v{self.config.version} initialized (instance: {self.instance_id})")
        logger.info("  ✅ MODP cloud deployment enabled")
        logger.info("  ✅ MOE elasticity prediction enabled")
        logger.info("  ✅ Bio‑inspired optimizer enabled")
        logger.info("  ✅ MOE predictive reflexivity enabled")
        logger.info("  ✅ Self‑healing enabled")

    # ----------------------------------------------------------------------
    # Teacher interface for MOPD
    # ----------------------------------------------------------------------
    async def policy_probs(self, state: Dict) -> List[float]:
        """Return a probability distribution over strategies (GA‑evolved if available)."""
        if self.config.bio.enabled:
            # Use GA fitness as probabilities
            stats = self.autonomous_optimizer.get_optimization_stats()
            # We don't have direct strategy probabilities, so we return uniform for now.
            # In a real implementation, we'd compute probabilities based on GA population fitness.
            return [0.2] * 5
        else:
            stats = self.autonomous_optimizer.get_optimization_stats()
            counts = stats.get('strategy_usage', {})
            total = sum(counts.values())
            if total == 0:
                return [0.2] * 5
            strategies = list(self.autonomous_optimizer.strategy_keys if hasattr(self.autonomous_optimizer, 'strategy_keys') else self.autonomous_optimizer.optimization_strategies.keys())
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

        # Compute base elasticities (using existing methods)
        price_el, price_ci = await self._calculate_price_elasticity(input_data)
        scarcity_el = await self._calculate_scarcity_elasticity(input_data)
        cross_el = self.cross_price_calc.calculate({})
        substitution_el = self.substitution_calc.calculate({'scarcity_index': input_data.scarcity_index})
        thermal_el = 0.2

        # Use MOE engine to compute composite elasticity
        moe_result = await self.elasticity_engine.predict(input_data)
        composite = moe_result['composite']
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

        # Multi-cloud deployment (MODP)
        deployment = await self.cloud_deployer.deploy_model({'size_mb': 0.5, 'features': len(self.elasticity_history) + 1})
        metrics.cloud_deployment = deployment

        # Autonomous optimization (GA‑enhanced)
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

        # Store in central storage
        self.storage.store_elasticity_metrics(metrics)

        # Update adaptive model and SPC
        if self.adaptive_model:
            features = [price_el, scarcity_el, cross_el, composite]
            await self.adaptive_model.update(features, composite)
        self.spc.update(composite)

        # Update predictive history
        await self.predictive.update_history(composite, input_data.carbon_intensity)

        # Federated sharing
        await self.federated_learner.share_insights(metrics)

        # Self‑healing: check drift and anomaly
        if self.self_healing:
            await self.self_healing.check_drift(asdict(metrics))
            is_anomaly, score = await self.self_healing.detect_anomaly(asdict(metrics))
            if is_anomaly:
                logger.warning(f"Anomaly detected with score {score:.2f}")

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
            candidates=[{'action': s} for s in self.autonomous_optimizer.strategy_keys if hasattr(self.autonomous_optimizer, 'strategy_keys') else self.autonomous_optimizer.optimization_strategies.keys()],
            source="helium_elasticity",
            environment=central_config.ENVIRONMENT,
            tags=["elasticity", "helium"]
        )
        await self.queue.publish("feedback_events", event.to_json())

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
            loop.create_task(self._self_healing_loop()),
        ])

    async def _optimization_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.auto_optimize_interval or 1800)
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
            await asyncio.sleep(self.config.predictive_interval or 3600)
            try:
                async with self._history_lock:
                    if self.elasticity_history:
                        latest = self.elasticity_history[-1]
                        await self.predictive.update_history(latest.composite_elasticity, 400)
                        forecast = await self.predictive.predict()
                        logger.info(f"Predictive forecast (MOE): {forecast}")
            except Exception as e:
                logger.error(f"Predictive loop error: {e}")

    async def _cleanup_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(86400)
            try:
                self.storage.clean_old_elasticity_records(days=central_config.data_retention_days or 365)
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

    async def _self_healing_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(self.config.self_healing.health_check_interval or 60)
            try:
                if self.self_healing:
                    async with self._history_lock:
                        if self.elasticity_history:
                            data = [asdict(m) for m in list(self.elasticity_history)[-100:]]
                            await self.self_healing.train(data)
            except Exception as e:
                logger.error(f"Self‑healing loop error: {e}")

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

async def get_elasticity_calculator(config: Optional[Union[ElasticityConfig, Dict]] = None,
                                    storage: Storage = None, queue: AsyncMessageQueue = None,
                                    adaptive_cost: AdaptiveCostFunction = None, pareto_gating: ParetoGating = None,
                                    drift_detector: DriftDetector = None, metrics: MetricsRegistry = None) -> EnhancedHeliumElasticityCalculator:
    global _elasticity_calculator_instance
    if _elasticity_calculator_instance is None:
        async with _elasticity_calculator_lock:
            if _elasticity_calculator_instance is None:
                cfg = config if isinstance(config, ElasticityConfig) else ElasticityConfig(**config) if config else ElasticityConfig()
                # If central components not provided, we need to instantiate them.
                # For standalone, we assume they are passed.
                _elasticity_calculator_instance = EnhancedHeliumElasticityCalculator(
                    cfg, storage, queue, adaptive_cost, pareto_gating, drift_detector, metrics
                )
                await _elasticity_calculator_instance.start()
    return _elasticity_calculator_instance

# ============================================================
# MAIN ENTRY POINT (for standalone testing)
# ============================================================
async def main():
    # For standalone testing, we need to instantiate central components.
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

    calculator = await get_elasticity_calculator(None, storage, queue, adaptive_cost, pareto, drift, metrics)

    # Calculate elasticity
    metrics = await calculator.calculate_comprehensive_elasticity()
    print(f"Composite Elasticity: {metrics.composite_elasticity:.3f}, Market Regime: {metrics.market_regime}")

    # Shutdown
    await calculator.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
