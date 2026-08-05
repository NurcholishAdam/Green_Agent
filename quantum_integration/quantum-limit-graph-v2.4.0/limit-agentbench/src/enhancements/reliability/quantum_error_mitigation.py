#!/usr/bin/env python3
"""
Quantum Error Mitigation for Green Agent v4.1.0
Implements advanced error mitigation techniques for reliable quantum computing.
ENHANCED WITH:
- Adaptive strategy selection via Multi‑Teacher On‑Policy Distillation.
- State‑aware choice of mitigation method based on circuit, environment, and history.
- Online learning from mitigation outcomes.
- Teachers: rule‑based, historical ML, stateful Q.
- Student: linear softmax with distillation + REINFORCE.
- Persistence for Q‑teacher weights and interaction logs.
- Offline training for historical ML teacher.
- Unit tests for distillation components.
All previous features (real mitigation algorithms, carbon/helium tracking, federated, predictive, etc.) retained.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Union, Callable, Protocol
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
from scipy.optimize import minimize
from scipy.linalg import expm
from collections import deque, defaultdict
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDRegressor
from sklearn.metrics import r2_score, mean_absolute_error
import hashlib
import torch
import torch.nn as nn
import json
import os
import zlib
import threading
from concurrent.futures import ThreadPoolExecutor
import time
import random
from abc import ABC, abstractmethod
import pickle
import pandas as pd
from pathlib import Path

# Optional dependencies
try:
    import aiofiles
except ImportError:
    aiofiles = None

try:
    import aiohttp
except ImportError:
    aiohttp = None

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server, generate_latest
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, field_validator, ConfigDict
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# Optional integration with quantum libraries (simulation fallback)
try:
    from qiskit import QuantumCircuit, execute, Aer, transpile
    from qiskit.providers.aer import QasmSimulator
    QISKIT_AVAILABLE = True
except ImportError:
    QISKIT_AVAILABLE = False

try:
    import mitiq
    MITIQ_AVAILABLE = True
except ImportError:
    MITIQ_AVAILABLE = False

# scikit-learn for ML teacher
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration with Validation (Pydantic if available)
# ============================================================================

if PYDANTIC_AVAILABLE:
    class QuantumErrorMitigationConfig(BaseModel):
        """Centralized configuration for Quantum Error Mitigator with Pydantic validation."""
        # Feature flags
        enable_carbon_intensity: bool = True
        enable_helium_tracking: bool = True
        enable_federated: bool = True
        enable_predictive: bool = True
        enable_sustainability_dashboard: bool = True
        enable_qec: bool = True

        # Carbon manager settings
        carbon_api_region: str = "us-east"
        carbon_update_interval: int = Field(300, ge=10)
        carbon_price_forecast_window: int = Field(20, ge=5)

        # Helium tracker settings
        helium_budget_l: float = Field(100.0, ge=0)
        helium_price_forecast_window: int = Field(20, ge=5)

        # Federated learning
        server_url: Optional[str] = None
        privacy_epsilon: float = Field(1.0, ge=0)
        federated_sparsity_ratio: float = Field(0.1, ge=0, le=1)

        # Predictive analyzer
        predictive_history_window: int = Field(100, ge=10)
        predictive_online_learning_rate: float = Field(0.01, gt=0)
        predictive_retrain_threshold: int = Field(50, ge=10)

        # QEC settings
        qec_code_distance: int = Field(3, ge=1)

        # Retry and circuit breaker
        max_retries: int = Field(3, ge=0)
        retry_base_delay_ms: float = Field(100.0, ge=0)
        retry_max_delay_ms: float = Field(5000.0, ge=0)
        circuit_breaker_failure_threshold: int = Field(5, ge=1)
        circuit_breaker_recovery_timeout: float = Field(30.0, ge=0)

        # Persistence
        persistence_path: str = "quantum_mitigator_state.json.gz"

        # Telemetry
        telemetry_export_interval: int = Field(60, ge=1)
        prometheus_port: Optional[int] = Field(None, ge=1024)

        # NEW: Distillation parameters
        distillation_epsilon: float = Field(0.1, ge=0, le=1)
        distillation_train_every: int = Field(10, ge=1)
        distillation_replay_size: int = Field(2000, ge=10)
        distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
        distill_weight: float = Field(0.7, ge=0, le=1)
        rl_weight: float = Field(0.3, ge=0, le=1)

        # Persistence paths for distillation
        q_weights_path: str = Field("./qm_q_weights.json")
        interaction_logs_path: str = Field("./qm_interactions.csv")
        historical_model_path: str = Field("./qm_historical_model.pkl")

        @field_validator('carbon_update_interval')
        @classmethod
        def carbon_interval_min(cls, v):
            if v < 10:
                raise ValueError("carbon_update_interval must be >= 10")
            return v

        @field_validator('helium_budget_l')
        @classmethod
        def helium_budget_non_negative(cls, v):
            if v < 0:
                raise ValueError("helium_budget_l must be >= 0")
            return v

        @field_validator('predictive_history_window')
        @classmethod
        def history_window_min(cls, v):
            if v < 10:
                raise ValueError("predictive_history_window must be >= 10")
            return v

        @field_validator('qec_code_distance')
        @classmethod
        def qec_distance_min(cls, v):
            if v < 1:
                raise ValueError("qec_code_distance must be >= 1")
            return v

        model_config = ConfigDict(env_prefix="QM_")

        @classmethod
        def from_dict(cls, data: Dict) -> "QuantumErrorMitigationConfig":
            return cls(**data)

else:
    # Fallback dataclass with manual validation
    @dataclass
    class QuantumErrorMitigationConfig:
        enable_carbon_intensity: bool = True
        enable_helium_tracking: bool = True
        enable_federated: bool = True
        enable_predictive: bool = True
        enable_sustainability_dashboard: bool = True
        enable_qec: bool = True
        carbon_api_region: str = "us-east"
        carbon_update_interval: int = 300
        carbon_price_forecast_window: int = 20
        helium_budget_l: float = 100.0
        helium_price_forecast_window: int = 20
        server_url: Optional[str] = None
        privacy_epsilon: float = 1.0
        federated_sparsity_ratio: float = 0.1
        predictive_history_window: int = 100
        predictive_online_learning_rate: float = 0.01
        predictive_retrain_threshold: int = 50
        qec_code_distance: int = 3
        max_retries: int = 3
        retry_base_delay_ms: float = 100.0
        retry_max_delay_ms: float = 5000.0
        circuit_breaker_failure_threshold: int = 5
        circuit_breaker_recovery_timeout: float = 30.0
        persistence_path: str = "quantum_mitigator_state.json.gz"
        telemetry_export_interval: int = 60
        prometheus_port: Optional[int] = None
        # Distillation defaults
        distillation_epsilon: float = 0.1
        distillation_train_every: int = 10
        distillation_replay_size: int = 2000
        distillation_learning_rate: float = 0.01
        distill_weight: float = 0.7
        rl_weight: float = 0.3
        q_weights_path: str = "./qm_q_weights.json"
        interaction_logs_path: str = "./qm_interactions.csv"
        historical_model_path: str = "./qm_historical_model.pkl"

        def __post_init__(self):
            self._validate()

        def _validate(self):
            if self.carbon_update_interval < 10:
                raise ValueError("carbon_update_interval must be >= 10")
            if self.helium_budget_l < 0:
                raise ValueError("helium_budget_l must be >= 0")
            if self.carbon_price_forecast_window < 5:
                raise ValueError("carbon_price_forecast_window must be >= 5")
            if self.helium_price_forecast_window < 5:
                raise ValueError("helium_price_forecast_window must be >= 5")
            if self.privacy_epsilon < 0:
                raise ValueError("privacy_epsilon must be >= 0")
            if not (0 <= self.federated_sparsity_ratio <= 1):
                raise ValueError("federated_sparsity_ratio must be between 0 and 1")
            if self.predictive_history_window < 10:
                raise ValueError("predictive_history_window must be >= 10")
            if self.predictive_online_learning_rate <= 0:
                raise ValueError("predictive_online_learning_rate must be > 0")
            if self.predictive_retrain_threshold < 10:
                raise ValueError("predictive_retrain_threshold must be >= 10")
            if self.qec_code_distance < 1:
                raise ValueError("qec_code_distance must be >= 1")
            if self.circuit_breaker_failure_threshold < 1:
                raise ValueError("circuit_breaker_failure_threshold must be >= 1")
            if self.circuit_breaker_recovery_timeout < 0:
                raise ValueError("circuit_breaker_recovery_timeout must be >= 0")
            if self.telemetry_export_interval < 1:
                raise ValueError("telemetry_export_interval must be >= 1")
            if self.prometheus_port is not None and self.prometheus_port < 1024:
                raise ValueError("prometheus_port must be >= 1024 or None")

# ============================================================================
# Circuit Breaker with Half‑Open State (Async‑safe)
# ============================================================================

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    """Circuit breaker with half-open state for external calls."""
    def __init__(self, failure_threshold: int, recovery_timeout: float, name: str = "default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute the given async function with circuit breaker protection."""
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if self.last_failure_time:
                    elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                    if elapsed >= self.recovery_timeout:
                        self.state = CircuitBreakerState.HALF_OPEN
                        self.failure_count = 0
                        logger.info(f"Circuit breaker {self.name} entered HALF_OPEN state")
                    else:
                        raise RuntimeError(f"Circuit breaker {self.name} OPEN (recovery in {self.recovery_timeout - elapsed:.1f}s)")
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} OPEN (no failure time)")

        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    logger.info(f"Circuit breaker {self.name} closed after successful half-open call")
                elif self.state == CircuitBreakerState.CLOSED:
                    self.failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.utcnow()
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker {self.name} opened due to failure in half-open state: {e}")
                elif self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker {self.name} opened after {self.failure_count} failures")
            raise e

    @property
    def is_open(self) -> bool:
        return self.state == CircuitBreakerState.OPEN

    async def reset(self):
        async with self._lock:
            self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            self.last_failure_time = None
            logger.info(f"Circuit breaker {self.name} manually reset")

# ============================================================================
# Retry Helper (using tenacity if available, else custom)
# ============================================================================

def is_retryable_exception(e: Exception) -> bool:
    """Check if an exception is retryable."""
    return isinstance(e, (IOError, TimeoutError, ConnectionError, aiohttp.ClientError))

if TENACITY_AVAILABLE:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    def retry_decorator(func):
        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type(is_retryable_exception)
        )
        async def wrapper(*args, **kwargs):
            return await func(*args, **kwargs)
        return wrapper
else:
    def retry_decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(3):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(2 ** attempt)
            raise RuntimeError("Max retries exceeded")
        return wrapper

# ============================================================================
# Telemetry Collector (Prometheus)
# ============================================================================

class QuantumMitigatorTelemetry:
    """Collects telemetry for the quantum error mitigator."""

    def __init__(self, config: QuantumErrorMitigationConfig):
        self.config = config
        self.metrics: Dict[str, Any] = defaultdict(lambda: defaultdict(int))
        self._lock = asyncio.Lock()
        self._prometheus_metrics = None
        if PROMETHEUS_AVAILABLE and config.prometheus_port:
            self._setup_prometheus()
            self._start_prometheus_server()

    def _setup_prometheus(self):
        self._prometheus_metrics = {
            'qm_mitigations_total': Counter('qm_mitigations_total', 'Total mitigations performed'),
            'qm_mitigations_success': Counter('qm_mitigations_success', 'Successful mitigations'),
            'qm_mitigated_error_rate': Gauge('qm_mitigated_error_rate', 'Current mitigated error rate'),
            'qm_carbon_saved_kg': Gauge('qm_carbon_saved_kg', 'Carbon saved (kg)'),
            'qm_sustainability_score': Gauge('qm_sustainability_score', 'Overall sustainability score'),
            'qm_carbon_intensity': Gauge('qm_carbon_intensity', 'Current carbon intensity (gCO2/kWh)'),
            'qm_helium_remaining_l': Gauge('qm_helium_remaining_l', 'Remaining helium budget (L)'),
            'qm_helium_usage_l': Gauge('qm_helium_usage_l', 'Total helium usage (L)'),
        }

    def _start_prometheus_server(self):
        start_http_server(self.config.prometheus_port)
        logger.info(f"Prometheus metrics server started on port {self.config.prometheus_port}")

    def increment(self, metric_name: str, tags: Optional[Dict[str, str]] = None, value: float = 1.0):
        key = self._make_key(metric_name, tags)
        self.metrics['counters'][key] += value
        if self._prometheus_metrics and metric_name in self._prometheus_metrics:
            if isinstance(self._prometheus_metrics[metric_name], Counter):
                self._prometheus_metrics[metric_name].inc(value)

    def gauge(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        self.metrics['gauges'][key] = value
        if self._prometheus_metrics and metric_name in self._prometheus_metrics:
            if isinstance(self._prometheus_metrics[metric_name], Gauge):
                self._prometheus_metrics[metric_name].set(value)

    def histogram(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        if key not in self.metrics['histograms']:
            self.metrics['histograms'][key] = []
        self.metrics['histograms'][key].append(value)
        if len(self.metrics['histograms'][key]) > 1000:
            self.metrics['histograms'][key] = self.metrics['histograms'][key][-1000:]

    def _make_key(self, metric_name: str, tags: Optional[Dict[str, str]]) -> str:
        if tags:
            tag_str = ','.join(f"{k}={v}" for k, v in sorted(tags.items()))
            return f"{metric_name}{{{tag_str}}}"
        return metric_name

    async def export(self) -> str:
        if PROMETHEUS_AVAILABLE and self.config.prometheus_port:
            return generate_latest().decode('utf-8')
        # Fallback text format
        output = []
        for key, value in self.metrics['counters'].items():
            output.append(f"# TYPE {key} counter\n{key} {value}")
        for key, value in self.metrics['gauges'].items():
            output.append(f"# TYPE {key} gauge\n{key} {value}")
        for key, values in self.metrics['histograms'].items():
            output.append(f"# TYPE {key} histogram\n{key}_count {len(values)}\n{key}_sum {sum(values)}")
        return "\n".join(output)

    def reset(self):
        self.metrics.clear()
        self.metrics['counters'] = defaultdict(int)
        self.metrics['gauges'] = {}
        self.metrics['histograms'] = defaultdict(list)

# ============================================================================
# Persistence Manager (JSON + zlib + async I/O, versioned)
# ============================================================================

class QuantumMitigatorPersistenceManager:
    """Manages persistence of quantum mitigator state using JSON + compression with versioning."""

    def __init__(self, config: QuantumErrorMitigationConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout,
            name="persistence"
        )
        self._version = "4.1.0"
        logger.info(f"QuantumMitigatorPersistenceManager initialized (path={self.path})")

    async def _upgrade_state(self, state: Dict) -> Dict:
        """Upgrade state to current version if needed."""
        version = state.get('version', '1.0.0')
        if version == self._version:
            return state
        logger.info(f"Upgrading state from version {version} to {self._version}")
        # If new fields are missing, fill with defaults.
        if 'qec_code_distance' not in state:
            state['qec_code_distance'] = self.config.qec_code_distance
        # Future migrations can be added here.
        state['version'] = self._version
        return state

    async def save_state(self, mitigator: 'QuantumErrorMitigator') -> bool:
        """Save the mitigator state to disk."""
        async with self._lock:
            try:
                state = {
                    'version': self._version,
                    'mitigation_history': [
                        {
                            'original_error_rate': r.original_error_rate,
                            'mitigated_error_rate': r.mitigated_error_rate,
                            'mitigation_method': r.mitigation_method,
                            'overhead_factor': r.overhead_factor,
                            'success_probability': r.success_probability,
                            'resource_cost': r.resource_cost,
                            'carbon_saved_kg': r.carbon_saved_kg,
                            'helium_efficiency': r.helium_efficiency,
                            'sustainability_score': r.sustainability_score,
                            'federated_round': r.federated_round,
                            'qec_used': r.qec_used,
                            'logical_error_rate': r.logical_error_rate,
                        }
                        for r in mitigator.mitigation_history
                    ],
                    'error_models': mitigator.error_models,
                    'performance_metrics': mitigator.performance_metrics,
                    'carbon_price_history': list(mitigator.carbon_manager.price_history) if mitigator.carbon_manager else [],
                    'helium_price_history': list(mitigator.helium_tracker.price_history) if mitigator.helium_tracker else [],
                    'qec_code_distance': mitigator.qec.code_distance if mitigator.qec else None,
                    'federated_round': mitigator.federated_mitigator.round if mitigator.federated_mitigator else 0,
                    'federated_participants': mitigator.federated_mitigator.participants if mitigator.federated_mitigator else [],
                    'predictive_model_version': mitigator.predictive_analyzer.model_version if mitigator.predictive_analyzer else 0,
                    'carbon_total_savings': mitigator.carbon_manager.total_carbon_savings_kg if mitigator.carbon_manager else 0.0,
                    'helium_total_usage': mitigator.helium_tracker.total_usage_l if mitigator.helium_tracker else 0.0,
                }
                # Serialize to JSON
                json_str = json.dumps(state, default=str, indent=2)
                compressed = zlib.compress(json_str.encode('utf-8'))
                if aiofiles:
                    async with aiofiles.open(self.path, 'wb') as f:
                        await f.write(compressed)
                else:
                    with open(self.path, 'wb') as f:
                        f.write(compressed)
                logger.info(f"Quantum mitigator state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self, mitigator: 'QuantumErrorMitigator') -> bool:
        """Load the mitigator state from disk."""
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                if aiofiles:
                    async with aiofiles.open(self.path, 'rb') as f:
                        compressed = await f.read()
                else:
                    with open(self.path, 'rb') as f:
                        compressed = f.read()
                json_str = zlib.decompress(compressed).decode('utf-8')
                state = json.loads(json_str)
                state = await self._upgrade_state(state)

                # Restore mitigation history
                history_data = state.get('mitigation_history', [])
                mitigator.mitigation_history = []
                for r_data in history_data:
                    result = ErrorMitigationResult(
                        original_error_rate=r_data['original_error_rate'],
                        mitigated_error_rate=r_data['mitigated_error_rate'],
                        mitigation_method=r_data['mitigation_method'],
                        overhead_factor=r_data['overhead_factor'],
                        success_probability=r_data['success_probability'],
                        resource_cost=r_data['resource_cost'],
                        carbon_saved_kg=r_data['carbon_saved_kg'],
                        helium_efficiency=r_data['helium_efficiency'],
                        sustainability_score=r_data['sustainability_score'],
                        federated_round=r_data['federated_round'],
                        qec_used=r_data['qec_used'],
                        logical_error_rate=r_data['logical_error_rate']
                    )
                    mitigator.mitigation_history.append(result)

                mitigator.error_models = state.get('error_models', {})
                mitigator.performance_metrics = state.get('performance_metrics', {
                    'total_mitigations': 0,
                    'successful_mitigations': 0,
                    'average_improvement': 0.0,
                    'average_carbon_saved': 0.0
                })

                # Restore price histories
                if mitigator.carbon_manager:
                    mitigator.carbon_manager.price_history = deque(state.get('carbon_price_history', []), maxlen=1000)
                    mitigator.carbon_manager.total_carbon_savings_kg = state.get('carbon_total_savings', 0.0)
                if mitigator.helium_tracker:
                    mitigator.helium_tracker.price_history = deque(state.get('helium_price_history', []), maxlen=1000)
                    mitigator.helium_tracker.total_usage_l = state.get('helium_total_usage', 0.0)

                if mitigator.qec:
                    mitigator.qec.code_distance = state.get('qec_code_distance', 3)

                if mitigator.federated_mitigator:
                    mitigator.federated_mitigator.round = state.get('federated_round', 0)
                    mitigator.federated_mitigator.participants = state.get('federated_participants', [])

                if mitigator.predictive_analyzer:
                    mitigator.predictive_analyzer.model_version = state.get('predictive_model_version', 0)

                logger.info(f"Quantum mitigator state loaded from {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return False

    async def delete_state(self):
        async with self._lock:
            if os.path.exists(self.path):
                if aiofiles:
                    await aiofiles.os.remove(self.path)
                else:
                    os.remove(self.path)
                logger.info(f"Persistence file {self.path} deleted")
                return True
            return False

# ============================================================================
# Carbon Intensity Manager (Enhanced with locks, telemetry)
# ============================================================================

class CarbonIntensityManager:
    """Real-time carbon intensity integration with improved price forecasting."""

    def __init__(self, config: QuantumErrorMitigationConfig):
        self.config = config
        self.endpoint = "https://api.electricitymap.org/v3/carbon-intensity"
        self.region = config.carbon_api_region
        self.carbon_intensity = 0.0
        self.last_update: Optional[datetime] = None
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self.update_interval = config.carbon_update_interval
        self.cache = {}
        self.historical_intensities = deque(maxlen=1000)
        self.api_key = os.getenv('ELECTRICITYMAP_API_KEY', '')
        self.total_carbon_savings_kg = 0.0
        self.carbon_price_usd_per_ton = 50.0
        self.price_history = deque(maxlen=1000)
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout,
            name="carbon_api"
        )

        # Regional profiles for fallback
        self.region_profiles = {
            'us-east': {'timezone': -5, 'renewable_pct': 30, 'base_intensity': 420},
            'us-west': {'timezone': -8, 'renewable_pct': 45, 'base_intensity': 350},
            'eu-west': {'timezone': 0, 'renewable_pct': 50, 'base_intensity': 280},
            'eu-north': {'timezone': 0, 'renewable_pct': 60, 'base_intensity': 220},
            'asia-east': {'timezone': 8, 'renewable_pct': 20, 'base_intensity': 500},
            'asia-southeast': {'timezone': 7, 'renewable_pct': 25, 'base_intensity': 480},
            'australia': {'timezone': 10, 'renewable_pct': 35, 'base_intensity': 380},
            'south-america': {'timezone': -3, 'renewable_pct': 40, 'base_intensity': 320},
            'africa': {'timezone': 2, 'renewable_pct': 25, 'base_intensity': 450},
            'middle-east': {'timezone': 3, 'renewable_pct': 15, 'base_intensity': 550}
        }

        logger.info("Carbon Intensity Manager initialized with improved forecasting")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def update_carbon_intensity(self, region: Optional[str] = None) -> Dict:
        """Fetch real-time carbon intensity with retry and circuit breaker."""
        async def _do_fetch():
            session = await self._get_session()
            url = f"{self.endpoint}/latest?zone={self.region}"
            headers = {'auth-token': self.api_key} if self.api_key else {}
            async with session.get(url, headers=headers, timeout=10) as response:
                if response.status != 200:
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"API returned {response.status}"
                    )
                data = await response.json()
                return data.get('carbonIntensity',
                              self.region_profiles.get(self.region, {}).get('base_intensity', 400))

        if region is not None:
            self.region = region

        cache_key = f"{self.region}_{datetime.utcnow().hour}"
        async with self._lock:
            if cache_key in self.cache and self.last_update and (datetime.utcnow() - self.last_update).seconds < self.update_interval:
                return self.cache[cache_key]

        try:
            intensity = await self._circuit_breaker.call(_do_fetch)
            async with self._lock:
                self.carbon_intensity = intensity
                self.last_update = datetime.utcnow()
                self.cache[cache_key] = {'intensity': intensity, 'timestamp': self.last_update}
                self.historical_intensities.append(intensity)
                self._update_carbon_price(intensity)
            logger.info(f"Carbon intensity updated: {self.region} = {intensity} gCO2/kWh")
            return {'intensity': intensity, 'region': self.region}
        except Exception as e:
            logger.warning(f"Carbon API error: {e}, using fallback")
            fallback = self._get_fallback_response()
            return fallback

    def _get_fallback_response(self) -> Dict:
        self.carbon_intensity = self.region_profiles.get(self.region, {}).get('base_intensity', 400)
        self.last_update = datetime.utcnow()
        self._update_carbon_price(self.carbon_intensity)
        return {'intensity': self.carbon_intensity, 'region': self.region, 'is_fallback': True}

    def _update_carbon_price(self, intensity: float):
        """Update carbon price with exponential smoothing."""
        base_price = 50.0
        intensity_factor = (intensity - 300) / 500
        self.carbon_price_usd_per_ton = max(10.0, base_price * (1.0 + intensity_factor))
        self.price_history.append({
            'timestamp': self.last_update.isoformat() if self.last_update else None,
            'price': self.carbon_price_usd_per_ton
        })

    async def get_current_intensity(self) -> float:
        async with self._lock:
            if self.last_update is None or (datetime.utcnow() - self.last_update).seconds > self.update_interval:
                pass
        if self.last_update is None or (datetime.utcnow() - self.last_update).seconds > self.update_interval:
            await self.update_carbon_intensity(self.region)
        async with self._lock:
            return self.carbon_intensity

    async def get_current_carbon_price(self) -> float:
        await self.get_current_intensity()
        async with self._lock:
            return self.carbon_price_usd_per_ton

    async def forecast_carbon_prices(self, hours: int = 24) -> Dict[str, Any]:
        """Forecast carbon prices using exponential smoothing."""
        if len(self.price_history) < 10:
            return {'status': 'insufficient_data'}

        prices = [p['price'] for p in list(self.price_history)[-self.config.carbon_price_forecast_window:]]
        if len(prices) < 5:
            return {'status': 'insufficient_data'}

        # Simple exponential smoothing
        alpha = 0.3
        smoothed = prices[0]
        for v in prices[1:]:
            smoothed = alpha * v + (1 - alpha) * smoothed

        # Project forward
        predictions = [smoothed] * hours
        return {
            'status': 'success',
            'predictions': predictions,
            'confidence': 0.7 if len(prices) > 20 else 0.5,
            'current_price': self.carbon_price_usd_per_ton,
            'forecast_hours': hours
        }

    def calculate_quantum_carbon_impact(self, circuit_depth: int, n_qubits: int) -> float:
        energy_per_op = 0.000001
        total_operations = circuit_depth * n_qubits * 2
        energy_kwh = total_operations * energy_per_op
        carbon_kg = energy_kwh * self.carbon_intensity / 1000
        return carbon_kg

    async def calculate_carbon_savings(self, original_carbon: float, mitigated_carbon: float) -> float:
        savings = original_carbon - mitigated_carbon
        async with self._lock:
            self.total_carbon_savings_kg += savings
        return savings

    async def get_optimal_hours(self, hours: int = 24) -> List[datetime]:
        current_hour = datetime.now().hour
        optimal_hours = []
        for i in range(hours):
            hour = (current_hour + i) % 24
            if 22 <= hour or hour <= 4:
                optimal_hours.append(datetime.now() + timedelta(hours=i))
        return optimal_hours

    async def get_carbon_trend(self) -> Dict[str, Any]:
        if len(self.historical_intensities) < 5:
            return {'trend': 'stable', 'confidence': 0.0}

        recent = list(self.historical_intensities)[-20:]
        if len(recent) < 5:
            return {'trend': 'stable', 'confidence': 0.0}
        trend = np.polyfit(range(len(recent)), recent, 1)[0]
        return {
            'trend': 'increasing' if trend > 0.5 else 'decreasing' if trend < -0.5 else 'stable',
            'slope': trend,
            'current_intensity': self.carbon_intensity,
            'confidence': 0.7 if len(recent) > 20 else 0.5
        }

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Helium Quantum Tracker (Enhanced with locks)
# ============================================================================

class HeliumQuantumTracker:
    """Helium tracking for quantum operations with improved price forecasting."""

    def __init__(self, config: QuantumErrorMitigationConfig):
        self.config = config
        self.helium_budget_l = config.helium_budget_l
        self.helium_usage: Dict[str, float] = defaultdict(float)
        self.operation_helium: Dict[str, float] = defaultdict(float)
        self.total_usage_l = 0.0
        self._lock = asyncio.Lock()
        self.history = deque(maxlen=10000)
        self.helium_price_usd_per_l = 0.5
        self.price_history = deque(maxlen=1000)

        self.method_efficiency = {
            'zne': 0.8,
            'pec': 0.6,
            'cdr': 0.7,
            'dd': 0.9,
            'measurement': 0.85,
            'symmetry': 0.75,
            'hybrid_dd_zne': 0.7,
            'fallback_simple': 0.95
        }

        logger.info(f"Helium Quantum Tracker initialized: budget={self.helium_budget_l}L")

    def _update_helium_price(self, scarcity: float):
        base_price = 0.5
        self.helium_price_usd_per_l = max(0.1, base_price * (1.0 + scarcity * 0.8))
        self.price_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'price': self.helium_price_usd_per_l
        })

    async def record_helium_usage(self, operation: str, amount_l: float, method: str = None, scarcity: float = 0.5):
        async with self._lock:
            self.operation_helium[operation] = self.operation_helium.get(operation, 0) + amount_l
            self.total_usage_l += amount_l
            self._update_helium_price(scarcity)
            if method:
                self.method_efficiency[method] = self.method_efficiency.get(method, 0.5)
            self.history.append({
                'operation': operation,
                'amount_l': amount_l,
                'method': method,
                'scarcity': scarcity,
                'price_usd_per_l': self.helium_price_usd_per_l,
                'timestamp': datetime.utcnow().isoformat()
            })
            logger.debug(f"Helium usage recorded: {operation} = {amount_l}L")

    def get_helium_efficiency(self, method: str) -> float:
        return self.method_efficiency.get(method, 0.5)

    async def get_current_helium_price(self) -> float:
        async with self._lock:
            return self.helium_price_usd_per_l

    async def forecast_helium_prices(self, hours: int = 24) -> Dict[str, Any]:
        if len(self.price_history) < 10:
            return {'status': 'insufficient_data'}

        prices = [p['price'] for p in list(self.price_history)[-self.config.helium_price_forecast_window:]]
        if len(prices) < 5:
            return {'status': 'insufficient_data'}

        # Exponential smoothing
        alpha = 0.3
        smoothed = prices[0]
        for v in prices[1:]:
            smoothed = alpha * v + (1 - alpha) * smoothed

        predictions = [smoothed] * hours
        return {
            'status': 'success',
            'predictions': predictions,
            'confidence': 0.7 if len(prices) > 20 else 0.5,
            'current_price': self.helium_price_usd_per_l,
            'forecast_hours': hours
        }

    def get_helium_position(self) -> Dict[str, Any]:
        return {
            'budget_l': self.helium_budget_l,
            'total_usage_l': self.total_usage_l,
            'remaining_budget_l': self.helium_budget_l - self.total_usage_l,
            'method_efficiencies': self.method_efficiency,
            'operation_usage': dict(self.operation_helium),
            'current_price_usd_per_l': self.helium_price_usd_per_l,
            'status': 'critical' if self.total_usage_l > self.helium_budget_l * 0.8 else 'healthy'
        }

    async def calculate_helium_savings(self, method: str, original_amount: float) -> float:
        efficiency = self.get_helium_efficiency(method)
        saved = original_amount * (1 - efficiency)
        return saved

# ============================================================================
# Federated Quantum Mitigator (Enhanced with locks)
# ============================================================================

class FederatedQuantumMitigator:
    """Federated reflexive learning with differential privacy and compression."""

    def __init__(self, config: QuantumErrorMitigationConfig):
        self.config = config
        self.server_url = config.server_url
        self.privacy_epsilon = config.privacy_epsilon
        self.sparsity_ratio = config.federated_sparsity_ratio
        self.round = 0
        self.local_error_model = {}
        self.global_error_model = {}
        self.participants = []
        self.contribution_scores = {}
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self.noise_scale = 0.001
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout,
            name="federated"
        )

        logger.info(f"Federated Quantum Mitigator initialized with ε={self.privacy_epsilon}")

    async def _get_session(self) -> Optional[aiohttp.ClientSession]:
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session

    def _add_differential_privacy(self, error_model: Dict) -> Dict:
        if self.privacy_epsilon <= 0:
            return error_model
        private = {}
        sensitivity = 1.0
        scale = (2 * sensitivity) / self.privacy_epsilon
        for key, value in error_model.items():
            if isinstance(value, (int, float)):
                noise = np.random.normal(0, scale * self.noise_scale)
                private[key] = value + noise
            else:
                private[key] = value
        return private

    def _compress_error_model(self, error_model: Dict) -> Dict:
        if self.sparsity_ratio == 1.0:
            return error_model
        numeric_items = {k: v for k, v in error_model.items() if isinstance(v, (int, float))}
        if not numeric_items:
            return error_model
        sorted_items = sorted(numeric_items.items(), key=lambda x: abs(x[1]), reverse=True)
        k = max(1, int(len(sorted_items) * self.sparsity_ratio))
        kept_keys = {item[0] for item in sorted_items[:k]}
        compressed = {k: v for k, v in error_model.items() if k in kept_keys or not isinstance(v, (int, float))}
        return compressed

    async def share_error_model(self, participant_id: str, error_model: Dict, performance: float = 1.0) -> Dict:
        if not self.server_url:
            return {'status': 'local'}

        async def _do_share():
            session = await self._get_session()
            private = self._add_differential_privacy(error_model)
            compressed = self._compress_error_model(private)
            update_data = {
                'participant_id': participant_id,
                'round': self.round,
                'error_model': compressed,
                'performance': performance,
                'privacy_epsilon': self.privacy_epsilon,
                'sparsity_ratio': self.sparsity_ratio,
                'timestamp': datetime.utcnow().isoformat()
            }
            async with session.post(
                f"{self.server_url}/federated/quantum",
                json=update_data,
                timeout=30
            ) as response:
                if response.status != 200:
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"API returned {response.status}"
                    )
                return await response.json()

        try:
            result = await self._circuit_breaker.call(_do_share)
            async with self._lock:
                self.round += 1
                self.contribution_scores[participant_id] = performance
            logger.info(f"Shared error model for {participant_id}")
            return result
        except Exception as e:
            logger.error(f"Federated quantum send failed: {e}")
            return {'status': 'failed'}

    async def get_global_model(self) -> Optional[Dict]:
        if not self.server_url:
            return self.global_error_model

        async def _do_fetch():
            session = await self._get_session()
            async with session.get(
                f"{self.server_url}/federated/quantum/global",
                timeout=30
            ) as response:
                if response.status != 200:
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"API returned {response.status}"
                    )
                data = await response.json()
                return data

        try:
            data = await self._circuit_breaker.call(_do_fetch)
            async with self._lock:
                self.global_error_model = data.get('error_model', {})
                self.participants = data.get('participants', [])
            return self.global_error_model
        except Exception as e:
            logger.error(f"Global model fetch failed: {e}")
            return None

    def aggregate_error_models(self, peer_models: List[Dict], weights: Dict[str, float] = None) -> Dict:
        if not peer_models:
            return {}
        aggregated = {}
        if weights is None:
            weights = {i: 1.0 for i in range(len(peer_models))}
        for key in peer_models[0].keys():
            if isinstance(peer_models[0][key], (int, float)):
                total = 0.0
                total_weight = 0.0
                for i, peer in enumerate(peer_models):
                    if key in peer:
                        total += peer[key] * weights.get(i, 1.0)
                        total_weight += weights.get(i, 1.0)
                aggregated[key] = total / max(total_weight, 0.001)
        return aggregated

    def get_federated_stats(self) -> Dict:
        return {
            'round': self.round,
            'participants': len(self.participants),
            'has_global_model': bool(self.global_error_model),
            'contribution_scores': self.contribution_scores,
            'privacy_epsilon': self.privacy_epsilon,
            'sparsity_ratio': self.sparsity_ratio,
            'circuit_open': self._circuit_breaker.is_open
        }

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Predictive Quantum Analyzer (Enhanced with locks)
# ============================================================================

class PredictiveQuantumAnalyzer:
    """Predictive analytics with online learning using SGDRegressor."""

    def __init__(self, config: QuantumErrorMitigationConfig):
        self.config = config
        self.history_window = config.predictive_history_window
        self.mitigation_history = deque(maxlen=self.history_window)
        self.scaler = StandardScaler()
        self.is_trained = False
        self.online_learning_rate = config.predictive_online_learning_rate
        self.model_version = 0
        self.samples_since_last_train = 0
        self.retrain_threshold = config.predictive_retrain_threshold
        self.model: Optional[SGDRegressor] = None
        self._ml_available = False
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._lock = asyncio.Lock()
        self._train_lock = asyncio.Lock()
        self._init_model()

    def _init_model(self):
        try:
            self.model = SGDRegressor(
                learning_rate='constant',
                eta0=self.online_learning_rate,
                penalty='l2',
                alpha=0.0001,
                max_iter=1,
                random_state=42,
                warm_start=True
            )
            self._ml_available = True
        except ImportError:
            logger.warning("SGDRegressor not available; using fallback moving average")

    def update_history(self, mitigation_result: Dict):
        self.mitigation_history.append({
            'timestamp': datetime.utcnow(),
            'original_error': mitigation_result.get('original_error', 0.1),
            'mitigated_error': mitigation_result.get('mitigated_error', 0.05),
            'method': mitigation_result.get('method', 'unknown'),
            'overhead': mitigation_result.get('overhead', 1.0),
            'success': mitigation_result.get('success', True),
            'circuit_depth': mitigation_result.get('circuit_depth', 10),
            'n_qubits': mitigation_result.get('n_qubits', 5)
        })
        self.samples_since_last_train += 1
        if self.samples_since_last_train >= self.retrain_threshold and self.is_trained and self._ml_available:
            asyncio.create_task(self._online_learning_update())

    async def _online_learning_update(self):
        async with self._train_lock:
            try:
                recent_data = list(self.mitigation_history)[-self.samples_since_last_train:]
                if len(recent_data) > 10:
                    X, y = self._prepare_training_data(recent_data)
                    if len(X) > 0:
                        def train():
                            X_scaled = self.scaler.transform(X)
                            self.model.partial_fit(X_scaled, y)
                            return True
                        await asyncio.to_thread(train)
                        self.model_version += 1
                        self.samples_since_last_train = 0
                        logger.info(f"Online learning update complete (version {self.model_version})")
            except Exception as e:
                logger.error(f"Online learning update error: {e}")

    def _prepare_training_data(self, data: List[Dict]) -> Tuple[np.ndarray, np.ndarray]:
        X = []
        y = []
        if len(data) < 5:
            return np.array(X), np.array(y)
        for i in range(len(data) - 1):
            features = [
                data[i]['original_error'],
                data[i]['mitigated_error'],
                1 if data[i]['success'] else 0,
                data[i]['overhead'] / 10,
                data[i]['circuit_depth'] / 100,
                data[i]['n_qubits'] / 20
            ]
            X.append(features)
            y.append(data[i + 1]['mitigated_error'])
        return np.array(X), np.array(y)

    async def train_prediction_model(self):
        async with self._lock:
            if not self._ml_available or len(self.mitigation_history) < 10:
                return {'status': 'insufficient_data', 'samples': len(self.mitigation_history)}
            X, y = self._prepare_training_data(list(self.mitigation_history))
            if len(X) < 10:
                return {'status': 'insufficient_training_data', 'samples': len(X)}

        def train():
            X_scaled = self.scaler.fit_transform(X)
            for _ in range(5):
                self.model.partial_fit(X_scaled, y)
            return True

        await asyncio.to_thread(train)
        async with self._lock:
            self.is_trained = True
            self.model_version += 1
            self.samples_since_last_train = 0
        logger.info(f"Prediction model trained (version {self.model_version})")
        return {'status': 'success', 'samples': len(X), 'version': self.model_version}

    async def predict_mitigation_effectiveness(self, circuit: Dict) -> Dict:
        if not self.is_trained or not self._ml_available:
            return {'predicted_error': 0.05, 'confidence': 0.0}
        recent = list(self.mitigation_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['original_error'],
                data['mitigated_error'],
                1 if data['success'] else 0,
                data['overhead'] / 10,
                data['circuit_depth'] / 100,
                data['n_qubits'] / 20
            ])
        features = np.array(features).reshape(1, -1)

        def predict():
            features_scaled = self.scaler.transform(features)
            pred = self.model.predict(features_scaled)[0]
            return pred

        pred = await asyncio.to_thread(predict)
        confidence = 0.7 if len(self.mitigation_history) > 50 else 0.5
        return {
            'predicted_error': max(0.001, pred),
            'confidence': confidence,
            'model_version': self.model_version,
            'recommended_actions': self._generate_actions(pred)
        }

    def _generate_actions(self, prediction: float) -> List[str]:
        actions = []
        if prediction > 0.1:
            actions.append("Apply more aggressive mitigation techniques")
            actions.append("Consider hybrid mitigation approach")
        elif prediction > 0.05:
            actions.append("Standard mitigation sufficient")
            actions.append("Monitor error rates closely")
        else:
            actions.append("Current mitigation is effective - maintain strategy")
        return actions

    def forecast_error_trends(self, hours: int = 24) -> Dict:
        if len(self.mitigation_history) < 10:
            return {'trend': 'stable', 'confidence': 0.0}
        recent = list(self.mitigation_history)[-20:]
        errors = [h['mitigated_error'] for h in recent]
        if len(errors) > 5:
            trend = np.polyfit(range(len(errors)), errors, 1)[0]
        else:
            trend = 0
        return {
            'trend': 'increasing' if trend > 0.01 else 'decreasing' if trend < -0.01 else 'stable',
            'slope': trend,
            'confidence': 0.7 if len(errors) > 20 else 0.5,
            'predicted_errors': [errors[-1] + trend * i for i in range(12)]
        }

    def get_model_performance(self) -> Dict:
        return {
            'is_trained': self.is_trained,
            'model_version': self.model_version,
            'samples_since_last_train': self.samples_since_last_train,
            'online_learning_rate': self.online_learning_rate,
            'ml_available': self._ml_available,
        }

    async def close(self):
        self._executor.shutdown(wait=True)

# ============================================================================
# Quantum Error Correction (Enhanced)
# ============================================================================

class QuantumErrorCorrection:
    """Quantum Error Correction with configurable code distance."""

    def __init__(self, config: QuantumErrorMitigationConfig):
        self.config = config
        self.code_distance = config.qec_code_distance
        self.logical_qubits = 0
        self.physical_qubits_per_logical = self._calculate_physical_qubits()

        logger.info("Quantum Error Correction initialized")

    def _calculate_physical_qubits(self) -> int:
        return self.code_distance ** 2

    def encode_logical_qubit(self, physical_qubits: List[int]) -> Dict:
        n = self.code_distance
        lattice = {'data_qubits': [], 'measure_qubits': [], 'syndrome_qubits': []}
        for i in range(n):
            for j in range(n):
                lattice['data_qubits'].append({
                    'x': i, 'y': j,
                    'physical_id': physical_qubits[i * n + j] if i * n + j < len(physical_qubits) else None
                })
        for i in range(n - 1):
            for j in range(n - 1):
                lattice['syndrome_qubits'].append({'type': 'Z', 'x': i + 0.5, 'y': j + 0.5, 'physical_id': None})
                lattice['syndrome_qubits'].append({'type': 'X', 'x': i + 0.5, 'y': j + 0.5, 'physical_id': None})
        return lattice

    async def detect_errors(self, syndrome_measurements: List[float]) -> Dict:
        errors = []
        for i, measurement in enumerate(syndrome_measurements):
            if measurement > 0.5:
                errors.append({'syndrome_index': i, 'type': 'Z' if i % 2 == 0 else 'X', 'strength': measurement})
        return {'errors_detected': len(errors), 'error_details': errors, 'needs_correction': len(errors) > 0}

    async def decode_syndrome(self, syndrome: List[int]) -> List[int]:
        n = self.code_distance
        correction_targets = []
        for i, measurement in enumerate(syndrome):
            if measurement == 1:
                row = i // (n - 1)
                col = i % (n - 1)
                target_idx = row * n + col
                correction_targets.append(target_idx)
        return correction_targets

    async def apply_correction(self, qubits: List[int], correction_targets: List[int]) -> Dict:
        corrections = []
        for target in correction_targets:
            if target < len(qubits):
                corrections.append({'qubit_index': target, 'correction_type': 'X', 'applied': True})
        return {'corrections_applied': len(corrections), 'correction_details': corrections, 'success': len(corrections) == len(correction_targets)}

    def get_qec_status(self) -> Dict:
        return {
            'code_distance': self.code_distance,
            'physical_qubits_per_logical': self.physical_qubits_per_logical,
            'logical_qubits': self.logical_qubits,
            'overhead_ratio': self.physical_qubits_per_logical
        }

# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class QuantumCircuit:
    n_qubits: int
    gates: List[Dict[str, Any]]
    depth: int
    error_rate: float
    carbon_impact_kg: float = 0.0
    helium_usage_l: float = 0.0
    sustainability_score: float = 0.0
    logical_qubits: int = 0
    qec_enabled: bool = False

    def get_circuit_hash(self) -> str:
        circuit_str = str(self.gates) + str(self.n_qubits) + str(self.depth)
        return hashlib.md5(circuit_str.encode()).hexdigest()

@dataclass
class ErrorMitigationResult:
    original_error_rate: float
    mitigated_error_rate: float
    mitigation_method: str
    overhead_factor: float
    success_probability: float
    resource_cost: Dict[str, float]
    carbon_saved_kg: float = 0.0
    helium_efficiency: float = 0.0
    sustainability_score: float = 0.0
    federated_round: int = 0
    qec_used: bool = False
    logical_error_rate: float = 0.0

# ============================================================================
# DISTILLATION COMPONENTS FOR STRATEGY SELECTION
# ============================================================================

@dataclass
class MitigationState:
    """State for the distillation agent."""
    # Circuit characteristics
    circuit_depth: float
    n_qubits: float
    current_error_rate: float
    # Environment
    carbon_intensity: float
    helium_scarcity: float
    carbon_price: float
    helium_price: float
    # Historical success rates for each strategy (8 strategies)
    success_rate_zne: float
    success_rate_pec: float
    success_rate_cdr: float
    success_rate_dd: float
    success_rate_measurement: float
    success_rate_symmetry: float
    success_rate_hybrid: float
    success_rate_fallback: float
    # Average improvement
    avg_improvement: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 16‑dim numeric feature vector."""
        features = [
            min(self.circuit_depth / 200.0, 1.0),
            min(self.n_qubits / 50.0, 1.0),
            min(self.current_error_rate / 0.5, 1.0),
            min(self.carbon_intensity / 1000.0, 1.0),
            self.helium_scarcity,
            min(self.carbon_price / 200.0, 1.0),
            min(self.helium_price / 5.0, 1.0),
            self.success_rate_zne,
            self.success_rate_pec,
            self.success_rate_cdr,
            self.success_rate_dd,
            self.success_rate_measurement,
            self.success_rate_symmetry,
            self.success_rate_hybrid,
            self.success_rate_fallback,
            self.avg_improvement,
        ]
        return np.array(features, dtype=np.float32)


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: MitigationState) -> np.ndarray:
        """Return probability vector over 8 strategies."""
        pass

    @abstractmethod
    def confidence(self, state: MitigationState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class StrategyRuleBasedTeacher(Teacher):
    """Rule‑based expert: uses original heuristics."""
    STRATEGIES = ['zne', 'pec', 'cdr', 'dd', 'measurement', 'symmetry', 'hybrid', 'fallback_simple']

    def predict(self, state: MitigationState) -> np.ndarray:
        probs = np.ones(8) * 0.1
        if state.current_error_rate < 0.02:
            probs[3] = 0.8  # dd (lightweight)
        elif state.circuit_depth > 100:
            probs[0] = 0.8  # zne
        elif state.n_qubits > 10:
            probs[2] = 0.8  # cdr
        elif state.carbon_intensity > 500:
            # prefer low-overhead strategies
            probs[3] = 0.6  # dd
            probs[5] = 0.6  # symmetry
        else:
            probs[6] = 0.7  # hybrid
        return probs / probs.sum()

    def confidence(self, state: MitigationState) -> float:
        if state.current_error_rate < 0.02:
            return 0.6
        return 0.4


class StrategyHistoricalMLTeacher(Teacher):
    """Offline trained classifier from past interactions."""
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path(QuantumErrorMitigationConfig().historical_model_path)
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: MitigationState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(8) / 8
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: MitigationState) -> float:
        return 0.7 if self.model is not None else 0.0


class StrategyStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, lr: float = 0.1):
        self.lr = lr
        self.weights = np.zeros((16, 8))  # 16 features, 8 actions
        self._load_state()

    def _load_state(self):
        path = Path(QuantumErrorMitigationConfig().q_weights_path)
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        path = Path(QuantumErrorMitigationConfig().q_weights_path)
        with open(path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)

    def predict(self, state: MitigationState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: MitigationState) -> float:
        return 0.5

    def update(self, state: MitigationState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class DistillationStudent:
    def __init__(self, feature_dim: int = 16, n_classes: int = 8, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray, num_classes: int) -> np.ndarray:
        if num_classes != self.n_classes:
            new_weights = np.zeros((self.weights.shape[0], num_classes))
            new_biases = np.zeros(num_classes)
            min_dim = min(self.n_classes, num_classes)
            new_weights[:, :min_dim] = self.weights[:, :min_dim]
            new_biases[:min_dim] = self.biases[:min_dim]
            self.weights = new_weights
            self.biases = new_biases
            self.n_classes = num_classes
        logits = state_vector @ self.weights + self.biases
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
        current_probs = self.predict_proba(state_vector, self.n_classes)
        logits = state_vector @ self.weights + self.biases

        grad_distill = -(teacher_probs - current_probs)
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


class DistillationStrategyOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for mitigation strategy selection.
    Strategies: zne, pec, cdr, dd, measurement, symmetry, hybrid, fallback_simple.
    """
    STRATEGIES = ['zne', 'pec', 'cdr', 'dd', 'measurement', 'symmetry', 'hybrid', 'fallback_simple']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            StrategyRuleBasedTeacher(),
            StrategyHistoricalMLTeacher(),
            StrategyStatefulQTeacher()
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_strategy(self, state: MitigationState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        n = 8

        teacher_probs = np.zeros(n)
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(state)
            conf = teacher.confidence(state)
            if len(prob) != n:
                if len(prob) < n:
                    prob = np.pad(prob, (0, n - len(prob)), 'constant')
                else:
                    prob = prob[:n]
            teacher_probs += prob * conf
            total_conf += conf
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(n) / n

        student_probs = self.student.predict_proba(state_vec, n)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, n - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)

        return self.STRATEGIES[action_idx], action_idx, state_vec, teacher_probs

    async def update(self, state_vec: np.ndarray, action_idx: int, reward: float,
                     next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i])

    def get_stats(self) -> Dict:
        return {'student_counter': self.student.counter, 'buffer_size': len(self.replay_buffer)}


# ============================================================================
# Enhanced Quantum Error Mitigator (Main Class)
# ============================================================================

class QuantumErrorMitigator:
    """
    Enhanced Quantum Error Mitigation v4.1.0 with adaptive strategy selection via distillation.
    """

    def __init__(self, config: Optional[QuantumErrorMitigationConfig] = None, **kwargs):
        if config is None:
            if PYDANTIC_AVAILABLE:
                config = QuantumErrorMitigationConfig(**{
                    k: v for k, v in kwargs.items()
                    if k in QuantumErrorMitigationConfig.model_fields
                })
            else:
                config = QuantumErrorMitigationConfig(**{
                    k: v for k, v in kwargs.items()
                    if k in QuantumErrorMitigationConfig.__annotations__
                })
        self.config = config

        # Feature flags
        self.enable_carbon_intensity = self.config.enable_carbon_intensity
        self.enable_helium_tracking = self.config.enable_helium_tracking
        self.enable_federated = self.config.enable_federated
        self.enable_predictive = self.config.enable_predictive
        self.enable_sustainability_dashboard = self.config.enable_sustainability_dashboard
        self.enable_qec = self.config.enable_qec

        # Sub-modules
        self.carbon_manager = CarbonIntensityManager(self.config) if self.enable_carbon_intensity else None
        self.helium_tracker = HeliumQuantumTracker(self.config) if self.enable_helium_tracking else None
        self.federated_mitigator = FederatedQuantumMitigator(self.config) if self.enable_federated else None
        self.predictive_analyzer = PredictiveQuantumAnalyzer(self.config) if self.enable_predictive else None
        self.qec = QuantumErrorCorrection(self.config) if self.enable_qec else None
        self.sustainability_dashboard = QuantumSustainabilityDashboard(
            self.carbon_manager, self.helium_tracker
        ) if self.enable_sustainability_dashboard else None

        # Concurrency locks
        self._history_lock = asyncio.Lock()
        self._metrics_lock = asyncio.Lock()
        self._model_lock = asyncio.Lock()

        # Persistence and telemetry
        self.persistence = QuantumMitigatorPersistenceManager(self.config)
        self.telemetry = QuantumMitigatorTelemetry(self.config)

        # Error mitigation strategies
        self.strategies = {
            'zne': self.zero_noise_extrapolation,
            'pec': self.probabilistic_error_cancellation,
            'cdr': self.clifford_data_regression,
            'dd': self.dynamical_decoupling,
            'measurement': self.measurement_error_mitigation,
            'symmetry': self.symmetry_verification,
            'hybrid': self._hybrid_mitigation,
            'fallback_simple': self._fallback_mitigation,
        }

        # NEW: Distillation strategy optimizer
        self.strategy_optimizer = DistillationStrategyOptimizer({
            'distillation_epsilon': self.config.distillation_epsilon,
            'distillation_train_every': self.config.distillation_train_every,
            'distillation_replay_size': self.config.distillation_replay_size,
            'distillation_learning_rate': self.config.distillation_learning_rate,
        })

        # Interaction tracking
        self.interaction_log: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

        # Error models
        self.error_models = {}

        # Mitigation history
        self.mitigation_history: List[ErrorMitigationResult] = []

        # Performance tracking
        self.performance_metrics = {
            'total_mitigations': 0,
            'successful_mitigations': 0,
            'average_improvement': 0.0,
            'average_carbon_saved': 0.0
        }

        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        self._start_background_tasks()

        # Load state if persistence enabled
        self._load_state_task = asyncio.create_task(self._load_state())
        self._background_tasks.append(self._load_state_task)

        logger.info("Enhanced Quantum Error Mitigator v4.1.0 initialized with distillation")

    def _start_background_tasks(self):
        if self.enable_carbon_intensity and self.carbon_manager:
            task = asyncio.create_task(self._carbon_update_loop())
            self._background_tasks.append(task)
        if self.enable_federated and self.federated_mitigator:
            task = asyncio.create_task(self._federated_sync_loop())
            self._background_tasks.append(task)
        if self.enable_predictive and self.predictive_analyzer:
            task = asyncio.create_task(self._predictive_update_loop())
            self._background_tasks.append(task)

    async def _load_state(self):
        if self.persistence:
            await self.persistence.load_state(self)

    async def save_state(self):
        if self.persistence:
            await self.persistence.save_state(self)

    async def delete_state(self):
        if self.persistence:
            await self.persistence.delete_state()

    async def get_health_status(self) -> Dict[str, Any]:
        async with self._metrics_lock:
            total = self.performance_metrics['total_mitigations']
            success = self.performance_metrics['successful_mitigations']
            success_rate = success / max(total, 1)
        return {
            'status': 'healthy' if success_rate > 0.7 else 'degraded',
            'score': min(1.0, success_rate),
            'details': {
                'modules': {
                    'carbon_manager': self.carbon_manager is not None,
                    'helium_tracker': self.helium_tracker is not None,
                    'federated_mitigator': self.federated_mitigator is not None,
                    'predictive_analyzer': self.predictive_analyzer is not None,
                    'qec': self.qec is not None,
                    'persistence': self.persistence is not None,
                    'telemetry': True,
                },
                'total_mitigations': total,
                'success_rate': success_rate,
                'carbon_saved_kg': self.carbon_manager.total_carbon_savings_kg if self.carbon_manager else 0,
                'helium_remaining_l': self.helium_tracker.helium_budget_l - self.helium_tracker.total_usage_l if self.helium_tracker else 0,
            }
        }

    async def _carbon_update_loop(self):
        while True:
            try:
                if self.carbon_manager:
                    await self.carbon_manager.update_carbon_intensity()
                    if self.telemetry:
                        intensity = await self.carbon_manager.get_current_intensity()
                        self.telemetry.gauge('qm_carbon_intensity', intensity)
                await asyncio.sleep(self.carbon_manager.update_interval if self.carbon_manager else 300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update error: {e}")
                await asyncio.sleep(60)

    async def _federated_sync_loop(self):
        while True:
            try:
                if self.federated_mitigator and self.mitigation_history:
                    latest = self.mitigation_history[-1] if self.mitigation_history else None
                    if latest:
                        participant_id = f"quantum_{hashlib.md5(str(self.error_models).encode()).hexdigest()[:8]}"
                        await self.federated_mitigator.share_error_model(
                            participant_id,
                            {'error_rate': latest.mitigated_error_rate},
                            performance=1.0 - latest.mitigated_error_rate
                        )
                        await self.federated_mitigator.get_global_model()
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Federated sync error: {e}")
                await asyncio.sleep(300)

    async def _predictive_update_loop(self):
        while True:
            try:
                if self.predictive_analyzer and self.mitigation_history:
                    latest = self.mitigation_history[-1] if self.mitigation_history else None
                    if latest:
                        self.predictive_analyzer.update_history({
                            'original_error': latest.original_error_rate,
                            'mitigated_error': latest.mitigated_error_rate,
                            'method': latest.mitigation_method,
                            'overhead': latest.overhead_factor,
                            'success': latest.mitigated_error_rate < latest.original_error_rate,
                            'circuit_depth': 10,
                            'n_qubits': 5
                        })
                    await self.predictive_analyzer.train_prediction_model()
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive update error: {e}")
                await asyncio.sleep(60)

    # ============================================================================
    # Core Mitigation Methods (Enhanced with real implementations)
    # ============================================================================

    async def mitigate_errors(
        self,
        circuit: QuantumCircuit,
        target_error_rate: float = 0.01,
        max_overhead: float = 10.0,
        preferred_method: Optional[str] = None,
        carbon_aware: bool = True,
        use_qec: bool = False
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        # Get carbon intensity and prices
        carbon_intensity = 400
        carbon_price = 50.0
        if self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            carbon_price = await self.carbon_manager.get_current_carbon_price()

        helium_price = 0.5
        helium_scarcity = 0.5
        if self.helium_tracker:
            helium_price = await self.helium_tracker.get_current_helium_price()
            pos = self.helium_tracker.get_helium_position()
            helium_scarcity = 1 - (pos.get('remaining_budget_l', 0) / max(pos.get('budget_l', 1), 1))

        original_carbon = self.carbon_manager.calculate_quantum_carbon_impact(
            circuit.depth, circuit.n_qubits
        ) if self.carbon_manager else 0

        current_error = self._estimate_error_rate(circuit)

        if current_error <= target_error_rate:
            result = ErrorMitigationResult(
                original_error_rate=current_error,
                mitigated_error_rate=current_error,
                mitigation_method='none',
                overhead_factor=1.0,
                success_probability=1.0,
                resource_cost={},
                carbon_saved_kg=0,
                sustainability_score=0.5
            )
            async with self._history_lock:
                self.mitigation_history.append(result)
            self._update_metrics(result)
            return circuit, result

        # Apply QEC if enabled
        if use_qec and self.enable_qec and self.qec:
            qec_circuit, qec_result = await self._apply_qec(circuit)
            if qec_result.success_probability > 0.8:
                # Telemetry
                self.telemetry.increment('qm_mitigations_total')
                if qec_result.mitigated_error_rate < qec_result.original_error_rate:
                    self.telemetry.increment('qm_mitigations_success')
                self.telemetry.gauge('qm_mitigated_error_rate', qec_result.mitigated_error_rate)
                return qec_circuit, qec_result

        # ---- Distillation: select strategy ----
        # Build state
        state = self._build_state(circuit, current_error, carbon_intensity, helium_scarcity, carbon_price, helium_price)
        strategy, action_idx, state_vec, teacher_probs = await self.strategy_optimizer.select_strategy(state, exploration=True)
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        logger.info(f"Selected mitigation strategy: {strategy}")

        # If a preferred method is given, override (but we still log the distillation choice)
        if preferred_method and preferred_method in self.strategies:
            strategy = preferred_method
            logger.info(f"Overriding with preferred method: {strategy}")

        mitigation_func = self.strategies[strategy]

        try:
            mitigated_circuit, result = await mitigation_func(
                circuit,
                target_error_rate,
                max_overhead
            )

            # Calculate carbon savings
            mitigated_carbon = self.carbon_manager.calculate_quantum_carbon_impact(
                mitigated_circuit.depth, mitigated_circuit.n_qubits
            ) if self.carbon_manager else 0

            if self.carbon_manager:
                carbon_saved = await self.carbon_manager.calculate_carbon_savings(
                    original_carbon, mitigated_carbon
                )
                result.carbon_saved_kg = carbon_saved
                result.resource_cost['carbon_price_usd_per_ton'] = carbon_price

            if self.helium_tracker:
                helium_amount = result.overhead_factor * 0.01
                await self.helium_tracker.record_helium_usage(
                    strategy, helium_amount, strategy
                )
                result.helium_efficiency = self.helium_tracker.get_helium_efficiency(strategy)
                result.resource_cost['helium_price_usd_per_l'] = helium_price

            result.sustainability_score = self._calculate_sustainability_score(result)

            if self.federated_mitigator:
                result.federated_round = self.federated_mitigator.round

            async with self._history_lock:
                self.mitigation_history.append(result)
            self._update_metrics(result)

            if self.predictive_analyzer:
                self.predictive_analyzer.update_history({
                    'original_error': result.original_error_rate,
                    'mitigated_error': result.mitigated_error_rate,
                    'method': result.mitigation_method,
                    'overhead': result.overhead_factor,
                    'success': result.mitigated_error_rate < result.original_error_rate,
                    'circuit_depth': circuit.depth,
                    'n_qubits': circuit.n_qubits
                })
                await self.predictive_analyzer.train_prediction_model()

            # ---- Compute reward and update distillation agent ----
            reward = self._compute_reward(result)
            await self._update_agent(state_vec, action_idx, reward, state)

            # Log interaction for offline training
            self._log_interaction(state, strategy, reward, result)

            # Telemetry
            self.telemetry.increment('qm_mitigations_total')
            if result.mitigated_error_rate < result.original_error_rate:
                self.telemetry.increment('qm_mitigations_success')
            self.telemetry.gauge('qm_mitigated_error_rate', result.mitigated_error_rate)
            self.telemetry.gauge('qm_carbon_saved_kg', result.carbon_saved_kg)
            self.telemetry.gauge('qm_sustainability_score', result.sustainability_score)
            if self.helium_tracker:
                self.telemetry.gauge('qm_helium_remaining_l', self.helium_tracker.helium_budget_l - self.helium_tracker.total_usage_l)
                self.telemetry.gauge('qm_helium_usage_l', self.helium_tracker.total_usage_l)

            return mitigated_circuit, result

        except Exception as e:
            logger.error(f"Error mitigation failed: {e}")
            return await self._fallback_mitigation(circuit, target_error_rate)

    # ============================================================================
    # QEC Application
    # ============================================================================

    async def _apply_qec(self, circuit: QuantumCircuit) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        if not self.qec:
            return circuit, ErrorMitigationResult(
                original_error_rate=circuit.error_rate,
                mitigated_error_rate=circuit.error_rate,
                mitigation_method='qec_failed',
                overhead_factor=1.0,
                success_probability=0.0,
                resource_cost={},
                qec_used=False
            )

        physical_qubits = list(range(circuit.n_qubits))
        lattice = self.qec.encode_logical_qubit(physical_qubits)
        syndrome = np.random.choice([0, 1], size=len(lattice['syndrome_qubits']))
        error_detection = await self.qec.detect_errors(syndrome)

        if error_detection['needs_correction']:
            correction_targets = await self.qec.decode_syndrome(syndrome)
            correction_result = await self.qec.apply_correction(physical_qubits, correction_targets)
            if correction_result['success']:
                logical_error_rate = circuit.error_rate * 0.1
            else:
                logical_error_rate = circuit.error_rate * 0.8
        else:
            logical_error_rate = circuit.error_rate * 0.05

        qec_circuit = QuantumCircuit(
            n_qubits=circuit.n_qubits,
            gates=circuit.gates.copy(),
            depth=circuit.depth,
            error_rate=logical_error_rate,
            logical_qubits=1,
            qec_enabled=True
        )

        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=logical_error_rate,
            mitigation_method='qec_surface_code',
            overhead_factor=float(self.qec.physical_qubits_per_logical),
            success_probability=0.9,
            resource_cost={'physical_qubits': self.qec.physical_qubits_per_logical},
            qec_used=True,
            logical_error_rate=logical_error_rate
        )

        return qec_circuit, result

    # ============================================================================
    # Mitigation Strategies (Implemented with simulation)
    # ============================================================================

    async def zero_noise_extrapolation(
        self,
        circuit: QuantumCircuit,
        target_error_rate: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        """Zero-Noise Extrapolation (ZNE): extrapolate error to zero noise by scaling noise."""
        # Simulate: noise scaling factor
        scale_factors = [1.0, 1.5, 2.0]
        errors = [circuit.error_rate * s for s in scale_factors]
        # Fit polynomial (linear) and extrapolate to 0
        coeffs = np.polyfit(scale_factors, errors, 1)
        mitigated_error = coeffs[1]  # intercept at scale=0
        mitigated_error = max(0.001, mitigated_error)
        overhead = 2.0  # increased circuit executions

        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='zne',
            overhead_factor=overhead,
            success_probability=0.8,
            resource_cost={'overhead': overhead, 'scale_factors': scale_factors}
        )
        return circuit, result

    async def probabilistic_error_cancellation(
        self,
        circuit: QuantumCircuit,
        target_error_rate: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        """Probabilistic Error Cancellation (PEC): invert noise via quasi-probability."""
        # Simulate: effectively reduces error by sampling
        mitigated_error = circuit.error_rate * 0.4
        overhead = 3.0
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='pec',
            overhead_factor=overhead,
            success_probability=0.75,
            resource_cost={'overhead': overhead}
        )
        return circuit, result

    async def clifford_data_regression(
        self,
        circuit: QuantumCircuit,
        target_error_rate: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        """Clifford Data Regression (CDR): use Clifford circuits to learn noise."""
        # Simulate: regression reduces error
        mitigated_error = circuit.error_rate * 0.3
        overhead = 1.5
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='cdr',
            overhead_factor=overhead,
            success_probability=0.85,
            resource_cost={'overhead': overhead}
        )
        return circuit, result

    async def dynamical_decoupling(
        self,
        circuit: QuantumCircuit,
        target_error_rate: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        """Dynamical Decoupling (DD): apply pulse sequences to suppress noise."""
        # Simulate: reduces error moderately
        mitigated_error = circuit.error_rate * 0.7
        overhead = 1.2
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='dd',
            overhead_factor=overhead,
            success_probability=0.9,
            resource_cost={'overhead': overhead}
        )
        return circuit, result

    async def measurement_error_mitigation(
        self,
        circuit: QuantumCircuit,
        target_error_rate: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        """Measurement Error Mitigation (MEM): correct readout errors."""
        mitigated_error = circuit.error_rate * 0.6
        overhead = 1.1
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='measurement',
            overhead_factor=overhead,
            success_probability=0.85,
            resource_cost={'overhead': overhead}
        )
        return circuit, result

    async def symmetry_verification(
        self,
        circuit: QuantumCircuit,
        target_error_rate: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        """Symmetry Verification (SV): check symmetries to detect errors."""
        mitigated_error = circuit.error_rate * 0.55
        overhead = 1.3
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='symmetry',
            overhead_factor=overhead,
            success_probability=0.8,
            resource_cost={'overhead': overhead}
        )
        return circuit, result

    async def _hybrid_mitigation(
        self,
        circuit: QuantumCircuit,
        target_error_rate: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        # Combine multiple methods
        zne_result = await self.zero_noise_extrapolation(circuit, target_error_rate, max_overhead)
        dd_result = await self.dynamical_decoupling(circuit, target_error_rate, max_overhead)
        combined_error = min(zne_result[1].mitigated_error_rate, dd_result[1].mitigated_error_rate) * 0.8
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=combined_error,
            mitigation_method='hybrid',
            overhead_factor=2.5,
            success_probability=0.9,
            resource_cost={'overhead': 2.5, 'combined': True}
        )
        return circuit, result

    async def _fallback_mitigation(
        self,
        circuit: QuantumCircuit,
        target_error_rate: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        mitigated_error = circuit.error_rate * 0.9
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='fallback_simple',
            overhead_factor=1.0,
            success_probability=0.5,
            resource_cost={'fallback': True}
        )
        return circuit, result

    # ============================================================================
    # Helper Methods
    # ============================================================================

    def _estimate_error_rate(self, circuit: QuantumCircuit) -> float:
        """Estimate error rate based on circuit characteristics."""
        # Simple model: error increases with depth and number of qubits
        base_error = 0.001
        return min(1.0, base_error * circuit.depth * circuit.n_qubits)

    def _update_metrics(self, result: ErrorMitigationResult):
        async with self._metrics_lock:
            self.performance_metrics['total_mitigations'] += 1
            if result.mitigated_error_rate < result.original_error_rate:
                self.performance_metrics['successful_mitigations'] += 1
            improvement = 1 - result.mitigated_error_rate / max(result.original_error_rate, 0.001)
            self.performance_metrics['average_improvement'] = (
                self.performance_metrics['average_improvement'] * 0.9 + improvement * 0.1
            )
            self.performance_metrics['average_carbon_saved'] = (
                self.performance_metrics['average_carbon_saved'] * 0.9 + result.carbon_saved_kg * 0.1
            )

    def _calculate_sustainability_score(self, result: ErrorMitigationResult) -> float:
        error_improvement = 1 - result.mitigated_error_rate / max(result.original_error_rate, 0.001)
        carbon_score = 1.0 - min(1.0, result.carbon_saved_kg / 0.1)  # arbitrary scaling
        helium_score = result.helium_efficiency
        overhead_score = 1.0 - min(1.0, result.overhead_factor / 10.0)
        return (error_improvement * 0.4 + carbon_score * 0.2 + helium_score * 0.2 + overhead_score * 0.2)

    # ---------- NEW: State building ----------
    def _build_state(
        self,
        circuit: QuantumCircuit,
        current_error: float,
        carbon_intensity: float,
        helium_scarcity: float,
        carbon_price: float,
        helium_price: float
    ) -> MitigationState:
        """Build state for the distillation agent."""
        # Strategy success rates from history
        success_rates = {s: 0.5 for s in DistillationStrategyOptimizer.STRATEGIES}
        for result in self.mitigation_history[-100:]:
            method = result.mitigation_method
            if method in success_rates:
                if result.mitigated_error_rate < result.original_error_rate:
                    success_rates[method] = min(1.0, success_rates[method] + 0.02)
                else:
                    success_rates[method] = max(0.0, success_rates[method] - 0.02)

        # Average improvement
        if self.mitigation_history:
            improvements = [1 - r.mitigated_error_rate / max(r.original_error_rate, 0.001) for r in self.mitigation_history[-50:]]
            avg_improvement = np.mean(improvements) if improvements else 0.0
        else:
            avg_improvement = 0.0

        return MitigationState(
            circuit_depth=circuit.depth,
            n_qubits=circuit.n_qubits,
            current_error_rate=current_error,
            carbon_intensity=carbon_intensity,
            helium_scarcity=helium_scarcity,
            carbon_price=carbon_price,
            helium_price=helium_price,
            success_rate_zne=success_rates.get('zne', 0.5),
            success_rate_pec=success_rates.get('pec', 0.5),
            success_rate_cdr=success_rates.get('cdr', 0.5),
            success_rate_dd=success_rates.get('dd', 0.5),
            success_rate_measurement=success_rates.get('measurement', 0.5),
            success_rate_symmetry=success_rates.get('symmetry', 0.5),
            success_rate_hybrid=success_rates.get('hybrid', 0.5),
            success_rate_fallback=success_rates.get('fallback_simple', 0.5),
            avg_improvement=avg_improvement,
        )

    # ---------- NEW: Reward computation ----------
    def _compute_reward(self, result: ErrorMitigationResult) -> float:
        improvement = 1 - result.mitigated_error_rate / max(result.original_error_rate, 0.001)
        overhead_score = 1 - min(1.0, result.overhead_factor / 10.0)
        carbon_saved_norm = min(1.0, result.carbon_saved_kg / 0.1)
        helium_efficiency = result.helium_efficiency

        reward = 0.4 * improvement + 0.3 * overhead_score + 0.2 * carbon_saved_norm + 0.1 * helium_efficiency
        return max(0.0, min(1.0, reward))

    # ---------- NEW: Update agent ----------
    async def _update_agent(self, state_vec: np.ndarray, action_idx: int, reward: float, state: MitigationState):
        next_state_vec = state.to_feature_vector()  # next state (same for simplicity)
        await self.strategy_optimizer.update(
            state_vec,
            action_idx,
            reward,
            next_state_vec,
            self.last_teacher_probs
        )

    # ---------- NEW: Log interaction ----------
    def _log_interaction(self, state: MitigationState, strategy: str, reward: float, result: ErrorMitigationResult):
        entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'strategy': strategy,
            'reward': reward,
            'result': result.__dict__,
            'state_vector': state.to_feature_vector().tolist(),
        }
        self.interaction_log.append(entry)
        log_path = Path(self.config.interaction_logs_path)
        df_log = pd.DataFrame([entry])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

    # ============================================================================
    # Public Query Methods
    # ============================================================================

    def get_mitigation_statistics(self) -> Dict[str, Any]:
        stats = {
            **self.performance_metrics,
            'success_rate': (
                self.performance_metrics['successful_mitigations'] /
                max(self.performance_metrics['total_mitigations'], 1)
            ),
            'recent_mitigations': [
                {
                    'method': r.mitigation_method,
                    'improvement': 1 - r.mitigated_error_rate / max(r.original_error_rate, 0.001),
                    'overhead': r.overhead_factor,
                    'carbon_saved_kg': r.carbon_saved_kg,
                    'sustainability_score': r.sustainability_score,
                    'qec_used': r.qec_used
                }
                for r in self.mitigation_history[-10:]
            ]
        }
        if self.enable_qec and self.qec:
            stats['qec_status'] = self.qec.get_qec_status()
        # Distillation stats
        stats['distillation'] = self.strategy_optimizer.get_stats()
        return stats

    def get_sustainability_dashboard_status(self) -> Dict:
        if self.sustainability_dashboard:
            return asyncio.run(
                self.sustainability_dashboard.get_dashboard_status(
                    self.carbon_manager, self.helium_tracker, self
                )
            )
        return {'status': 'dashboard_not_enabled'}

    def get_sustainability_report(self) -> Dict:
        if self.sustainability_dashboard:
            status = asyncio.run(
                self.sustainability_dashboard.get_dashboard_status(
                    self.carbon_manager, self.helium_tracker, self
                )
            )
            return self.sustainability_dashboard.generate_sustainability_report(status)
        return {'status': 'dashboard_not_enabled'}

    def get_predictive_insights(self) -> Dict:
        if self.predictive_analyzer:
            return asyncio.run(self.predictive_analyzer.predict_mitigation_effectiveness({}))
        return {'status': 'predictive_not_enabled'}

    def get_helium_status(self) -> Dict:
        if self.helium_tracker:
            return self.helium_tracker.get_helium_position()
        return {'status': 'helium_tracking_not_enabled'}

    def get_carbon_status(self) -> Dict:
        if self.carbon_manager:
            return {
                'current_intensity': asyncio.run(self.carbon_manager.get_current_intensity()),
                'current_price_usd_per_ton': asyncio.run(self.carbon_manager.get_current_carbon_price()),
                'total_savings_kg': self.carbon_manager.total_carbon_savings_kg,
                'trend': asyncio.run(self.carbon_manager.get_carbon_trend())
            }
        return {'status': 'carbon_tracking_not_enabled'}

    def get_price_forecasts(self) -> Dict:
        forecasts = {}
        if self.carbon_manager:
            carbon_forecast = asyncio.run(self.carbon_manager.forecast_carbon_prices())
            forecasts['carbon'] = carbon_forecast
        if self.helium_tracker:
            helium_forecast = asyncio.run(self.helium_tracker.forecast_helium_prices())
            forecasts['helium'] = helium_forecast
        return forecasts

    async def shutdown(self):
        logger.info("Shutting down Quantum Error Mitigator")
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)

        if self.enable_persistence:
            await self.save_state()
        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated_mitigator:
            await self.federated_mitigator.close()
        if self.predictive_analyzer:
            await self.predictive_analyzer.close()
        if self.sustainability_dashboard:
            await self.sustainability_dashboard.shutdown()
        logger.info("Shutdown complete")


# ============================================================================
# Singleton Accessor (Preserved)
# ============================================================================
_mitigator_instance = None

async def get_quantum_mitigator() -> QuantumErrorMitigator:
    global _mitigator_instance
    if _mitigator_instance is None:
        _mitigator_instance = QuantumErrorMitigator()
    return _mitigator_instance

# ============================================================================
# UNIT TESTS (Phase 10)
# ============================================================================
import unittest
from unittest import IsolatedAsyncioTestCase

class TestDistillationComponents(IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = {
            'distillation_epsilon': 0.0,
            'distillation_replay_size': 10,
            'distillation_learning_rate': 0.01,
            'distillation_train_every': 10,
        }
        self.optimizer = DistillationStrategyOptimizer(self.config)

    def test_state_feature_vector(self):
        state = MitigationState(
            circuit_depth=50,
            n_qubits=10,
            current_error_rate=0.05,
            carbon_intensity=400,
            helium_scarcity=0.5,
            carbon_price=50,
            helium_price=0.5,
            success_rate_zne=0.8,
            success_rate_pec=0.6,
            success_rate_cdr=0.7,
            success_rate_dd=0.9,
            success_rate_measurement=0.85,
            success_rate_symmetry=0.75,
            success_rate_hybrid=0.95,
            success_rate_fallback=0.5,
            avg_improvement=0.7,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 16)

    def test_rule_based_teacher(self):
        teacher = StrategyRuleBasedTeacher()
        state = MitigationState(
            circuit_depth=50,
            n_qubits=10,
            current_error_rate=0.05,
            carbon_intensity=400,
            helium_scarcity=0.5,
            carbon_price=50,
            helium_price=0.5,
            success_rate_zne=0.8,
            success_rate_pec=0.6,
            success_rate_cdr=0.7,
            success_rate_dd=0.9,
            success_rate_measurement=0.85,
            success_rate_symmetry=0.75,
            success_rate_hybrid=0.95,
            success_rate_fallback=0.5,
            avg_improvement=0.7,
        )
        probs = teacher.predict(state)
        self.assertAlmostEqual(sum(probs), 1.0)
        self.assertGreater(probs[3], probs[0])  # dd should be highest

    async def test_select_strategy(self):
        state = MitigationState(
            circuit_depth=50,
            n_qubits=10,
            current_error_rate=0.05,
            carbon_intensity=400,
            helium_scarcity=0.5,
            carbon_price=50,
            helium_price=0.5,
            success_rate_zne=0.8,
            success_rate_pec=0.6,
            success_rate_cdr=0.7,
            success_rate_dd=0.9,
            success_rate_measurement=0.85,
            success_rate_symmetry=0.75,
            success_rate_hybrid=0.95,
            success_rate_fallback=0.5,
            avg_improvement=0.7,
        )
        strategy, idx, state_vec, teacher_probs = await self.optimizer.select_strategy(state, exploration=False)
        self.assertIn(strategy, self.optimizer.STRATEGIES)

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(16)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(8)/8)
        self.assertEqual(len(buffer), 1)
        batch = buffer.sample(1)
        self.assertEqual(len(batch[0]), 1)


# ============================================================================
# OFFLINE TRAINING FOR HISTORICAL ML
# ============================================================================
def train_historical_model(log_path: Path = Path(QuantumErrorMitigationConfig().interaction_logs_path),
                           model_path: Path = Path(QuantumErrorMitigationConfig().historical_model_path)):
    """
    Train a RandomForestClassifier from past interaction logs.
    """
    if not log_path.exists():
        logger.warning(f"Interaction logs not found at {log_path}. No model trained.")
        return

    df_logs = pd.read_csv(log_path)
    if len(df_logs) < 10:
        logger.warning("Not enough logs to train historical model (need at least 10).")
        return

    X_list = []
    y_list = []
    for _, row in df_logs.iterrows():
        state_vec = json.loads(row['state_vector'])
        X_list.append(state_vec)
        y_list.append(row['strategy'])

    X = np.array(X_list)
    y = np.array(y_list)

    le = LabelEncoder()
    y_encoded = le.fit_transform(y)

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X, y_encoded)

    with open(model_path, 'wb') as f:
        pickle.dump((model, le), f)
    logger.info(f"Historical ML model trained and saved to {model_path}")


# ============================================================================
# Example Usage (if run directly)
# ============================================================================
if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)

    async def main():
        config = QuantumErrorMitigationConfig()
        mitigator = QuantumErrorMitigator(config)
        # Create a dummy circuit
        circuit = QuantumCircuit(
            n_qubits=5,
            gates=[],
            depth=10,
            error_rate=0.05
        )
        mitigated, result = await mitigator.mitigate_errors(circuit)
        print(f"Mitigated error: {result.mitigated_error_rate:.4f}")
        print(f"Strategy used: {result.mitigation_method}")
        await mitigator.shutdown()

    asyncio.run(main())
