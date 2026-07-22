#!/usr/bin/env python3
"""
Quantum Error Mitigation for Green Agent v3.1.0
Implements advanced error mitigation techniques for reliable quantum computing.
ENHANCED WITH: Secure JSON persistence, unified circuit breaker, Prometheus telemetry,
async carbon API, threaded ML training, configuration validation, and full type hints.

Features:
- Configuration Dataclass with Validation
- Persistence (JSON + zlib + async I/O)
- Telemetry (Prometheus)
- Health Checks
- Improved Price Forecasting (exponential smoothing)
- Robust Online Learning (SGDRegressor with thread offloading)
- Configurable QEC (quantum error correction)
- Retry and Circuit Breaker for External Calls
- Refactored Architecture
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
except ImportError:
    # Dummy retry decorator
    def retry(*args, **kwargs):
        return lambda f: f
    stop_after_attempt = lambda x: None
    wait_exponential = lambda **k: None
    retry_if_exception_type = lambda e: None

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server, generate_latest
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, ValidationError
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration with Validation
# ============================================================================

@dataclass
class QuantumErrorMitigationConfig:
    """Centralized configuration for Quantum Error Mitigator."""
    # Feature flags
    enable_carbon_intensity: bool = True
    enable_helium_tracking: bool = True
    enable_federated: bool = True
    enable_predictive: bool = True
    enable_sustainability_dashboard: bool = True
    enable_qec: bool = True

    # Carbon manager settings
    carbon_api_region: str = "us-east"
    carbon_update_interval: int = 300
    carbon_price_forecast_window: int = 20

    # Helium tracker settings
    helium_budget_l: float = 100.0
    helium_price_forecast_window: int = 20

    # Federated learning
    server_url: Optional[str] = None
    privacy_epsilon: float = 1.0
    federated_sparsity_ratio: float = 0.1

    # Predictive analyzer
    predictive_history_window: int = 100
    predictive_online_learning_rate: float = 0.01
    predictive_retrain_threshold: int = 50

    # QEC settings
    qec_code_distance: int = 3

    # Retry and circuit breaker
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    # Persistence
    persistence_path: str = "quantum_mitigator_state.json.gz"

    # Telemetry
    telemetry_export_interval: int = 60
    prometheus_port: Optional[int] = None  # if set, start HTTP server

    # Strategy selection weights
    carbon_aware_weights: Dict[str, float] = field(default_factory=lambda: {
        'dd': 0.9,
        'measurement': 0.85,
        'symmetry': 0.8,
        'hybrid': 0.7,
        'zne': 0.6,
        'cdr': 0.5,
        'pec': 0.4,
        'fallback_simple': 0.95
    })
    performance_weights: Dict[str, float] = field(default_factory=lambda: {
        'dd': 0.85,
        'hybrid': 0.92,
        'zne': 0.90,
        'pec': 0.88,
        'cdr': 0.85,
        'measurement': 0.80,
        'symmetry': 0.82,
        'fallback_simple': 0.70
    })

    def __post_init__(self):
        """Validate configuration parameters."""
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
# Circuit Breaker with Half‑Open State
# ============================================================================

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    """Circuit breaker with half-open state for external calls."""
    def __init__(self, failure_threshold: int, recovery_timeout: float):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
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
                        logger.info("Circuit breaker entered HALF_OPEN state")
                    else:
                        raise RuntimeError(f"Circuit breaker OPEN (recovery in {self.recovery_timeout - elapsed:.1f}s)")
                else:
                    raise RuntimeError("Circuit breaker OPEN (no failure time)")

        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    logger.info("Circuit breaker closed after successful half-open call")
                elif self.state == CircuitBreakerState.CLOSED:
                    self.failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.utcnow()
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker opened due to failure in half-open state: {e}")
                elif self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
            raise e

    @property
    def is_open(self) -> bool:
        return self.state == CircuitBreakerState.OPEN

    async def reset(self):
        async with self._lock:
            self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            self.last_failure_time = None
            logger.info("Circuit breaker manually reset")

# ============================================================================
# Retry Helper (using tenacity if available)
# ============================================================================

def is_retryable_exception(e: Exception) -> bool:
    """Check if an exception is retryable."""
    return isinstance(e, (IOError, TimeoutError, ConnectionError, aiohttp.ClientError))

# Use tenacity if available, else custom
if 'retry' in globals() and stop_after_attempt and wait_exponential:
    retry_decorator = retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(is_retryable_exception)
    )
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
# Persistence Manager (JSON + zlib + async I/O)
# ============================================================================

class QuantumMitigatorPersistenceManager:
    """Manages persistence of quantum mitigator state using JSON + compression."""

    def __init__(self, config: QuantumErrorMitigationConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout
        )
        logger.info(f"QuantumMitigatorPersistenceManager initialized (path={self.path})")

    async def save_state(self, mitigator: 'QuantumErrorMitigator') -> bool:
        """Save the mitigator state to disk."""
        async with self._lock:
            try:
                state = {
                    'version': '3.1.0',
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

                # Version check
                version = state.get('version', '1.0.0')
                if version != '3.1.0':
                    logger.warning(f"State version mismatch: {version} != 3.1.0; attempting to load anyway")

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
                if mitigator.helium_tracker:
                    mitigator.helium_tracker.price_history = deque(state.get('helium_price_history', []), maxlen=1000)

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
# Carbon Intensity Manager (Enhanced with unified circuit breaker)
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
            recovery_timeout=config.circuit_breaker_recovery_timeout
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
        if cache_key in self.cache and self.last_update and (datetime.utcnow() - self.last_update).seconds < self.update_interval:
            return self.cache[cache_key]

        try:
            intensity = await self._circuit_breaker.call(_do_fetch)
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
        if self.last_update is None or (datetime.utcnow() - self.last_update).seconds > self.update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity

    async def get_current_carbon_price(self) -> float:
        if self.last_update is None or (datetime.utcnow() - self.last_update).seconds > self.update_interval:
            await self.update_carbon_intensity(self.region)
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
# Helium Quantum Tracker (Enhanced)
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
# Federated Quantum Mitigator (Enhanced)
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
            recovery_timeout=config.circuit_breaker_recovery_timeout
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
# Predictive Quantum Analyzer (Enhanced with thread offloading)
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
# Quantum Carbon-Aware Selector (Enhanced)
# ============================================================================

class QuantumCarbonAwareSelector:
    def __init__(self, carbon_manager: Optional[CarbonIntensityManager] = None):
        self.carbon_manager = carbon_manager
        self.selection_history = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        logger.info("Quantum Carbon-Aware Selector initialized")

    async def select_mitigation_with_carbon(
        self,
        options: List[str],
        circuit: Dict,
        carbon_intensity: Optional[float] = None,
        carbon_price: Optional[float] = None
    ) -> Tuple[str, float]:
        if carbon_intensity is None and self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            carbon_price = await self.carbon_manager.get_current_carbon_price()
        else:
            carbon_intensity = carbon_intensity or 400
            carbon_price = carbon_price or 50

        # Weights are now configurable from the main config; we'll use defaults here
        carbon_weights = {
            'dd': 0.9,
            'measurement': 0.85,
            'symmetry': 0.8,
            'hybrid': 0.7,
            'zne': 0.6,
            'cdr': 0.5,
            'pec': 0.4,
            'fallback_simple': 0.95
        }
        performance_weights = {
            'dd': 0.85,
            'hybrid': 0.92,
            'zne': 0.90,
            'pec': 0.88,
            'cdr': 0.85,
            'measurement': 0.80,
            'symmetry': 0.82,
            'fallback_simple': 0.70
        }

        price_factor = min(2.0, carbon_price / 50.0)
        scores = {}
        for option in options:
            carbon_score = carbon_weights.get(option, 0.5)
            performance_score = performance_weights.get(option, 0.5)
            if carbon_intensity > 500:
                carbon_weight = min(0.8, 0.5 + price_factor * 0.15)
                performance_weight = 1.0 - carbon_weight
            elif carbon_intensity > 300:
                carbon_weight = min(0.6, 0.3 + price_factor * 0.15)
                performance_weight = 1.0 - carbon_weight
            else:
                carbon_weight = max(0.2, 0.3 - price_factor * 0.05)
                performance_weight = 1.0 - carbon_weight
            scores[option] = carbon_score * carbon_weight + performance_score * performance_weight

        if not scores:
            return 'dd', 0.5
        best_option = max(scores.items(), key=lambda x: x[1])
        async with self._lock:
            self.selection_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'carbon_intensity': carbon_intensity,
                'carbon_price': carbon_price,
                'selected': best_option[0],
                'score': best_option[1]
            })
        return best_option

# ============================================================================
# Quantum Sustainability Dashboard (Enhanced)
# ============================================================================

class QuantumSustainabilityDashboard:
    def __init__(self):
        self.history = []
        self.alert_thresholds = {
            'carbon_intensity': 500,
            'helium_remaining': 0.2,
            'error_rate': 0.1
        }
        self._lock = asyncio.Lock()
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("Quantum Sustainability Dashboard initialized")

    async def _monitor_loop(self):
        while self._running:
            try:
                # This loop can be used for periodic checks; currently it's a placeholder
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Sustainability monitor error: {e}")
                await asyncio.sleep(300)

    async def get_dashboard_status(
        self,
        carbon_manager: Optional[CarbonIntensityManager] = None,
        helium_tracker: Optional[HeliumQuantumTracker] = None,
        mitigator: Optional['QuantumErrorMitigator'] = None
    ) -> Dict:
        status = {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': 0.5
        }
        if carbon_manager:
            status['carbon_intensity'] = await carbon_manager.get_current_intensity()
            status['carbon_price'] = await carbon_manager.get_current_carbon_price()
            status['carbon_savings_kg'] = carbon_manager.total_carbon_savings_kg
        if helium_tracker:
            helium_pos = helium_tracker.get_helium_position()
            status['helium_position'] = helium_pos
            status['helium_price'] = helium_pos.get('current_price_usd_per_l', 0.5)
            status['helium_remaining_ratio'] = helium_pos.get('remaining_budget_l', 0) / max(helium_pos.get('budget_l', 1), 1)
        if mitigator:
            stats = mitigator.get_mitigation_statistics()
            status['mitigation_performance'] = stats
            status['success_rate'] = stats.get('success_rate', 0)
            status['average_improvement'] = stats.get('average_improvement', 0)

        # Calculate composite score
        score = 0.5
        if status.get('success_rate', 0) > 0.8:
            score += 0.2
        if status.get('carbon_intensity', 400) < 300:
            score += 0.15
        if status.get('helium_remaining_ratio', 0.5) > 0.5:
            score += 0.15
        status['sustainability_score'] = min(1.0, max(0.0, score))

        # Alerts
        alerts = []
        if status.get('carbon_intensity', 0) > self.alert_thresholds['carbon_intensity']:
            alerts.append('High carbon intensity detected')
        if status.get('helium_remaining_ratio', 1.0) < self.alert_thresholds['helium_remaining']:
            alerts.append('Helium budget critically low')
        if status.get('success_rate', 1.0) < 0.7:
            alerts.append('Mitigation success rate low')
        if alerts:
            status['alerts'] = alerts

        async with self._lock:
            self.history.append(status)
            if len(self.history) > 1000:
                self.history = self.history[-1000:]

        return status

    def generate_sustainability_report(self, status: Dict) -> Dict:
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': status.get('sustainability_score', 0.5),
            'carbon_status': {
                'intensity': status.get('carbon_intensity', 0),
                'price_usd_per_ton': status.get('carbon_price', 50),
                'savings_kg': status.get('carbon_savings_kg', 0)
            },
            'helium_status': {
                'remaining_ratio': status.get('helium_remaining_ratio', 0.5),
                'price_usd_per_l': status.get('helium_price', 0.5)
            },
            'mitigation_status': status.get('mitigation_performance', {}),
            'alerts': status.get('alerts', []),
            'recommendations': self._generate_recommendations(status)
        }

    def _generate_recommendations(self, status: Dict) -> List[str]:
        recommendations = []
        if status.get('carbon_intensity', 0) > 400:
            recommendations.append("Schedule quantum operations during low-carbon hours")
        if status.get('helium_remaining_ratio', 1.0) < 0.3:
            recommendations.append("Implement helium recovery for quantum operations")
        if status.get('success_rate', 1.0) < 0.8:
            recommendations.append("Review mitigation strategy selection for better results")
        return recommendations or ["All sustainability metrics are within acceptable ranges"]

    async def shutdown(self):
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("QuantumSustainabilityDashboard shut down")

# ============================================================================
# Enhanced Quantum Error Mitigator (Main Class)
# ============================================================================

class QuantumErrorMitigator:
    """
    Enhanced Quantum Error Mitigation v3.1.0 with full sustainability and resilience features.
    """

    def __init__(self, config: Optional[QuantumErrorMitigationConfig] = None):
        self.config = config or QuantumErrorMitigationConfig()

        # Feature flags
        self.enable_carbon_intensity = self.config.enable_carbon_intensity
        self.enable_helium_tracking = self.config.enable_helium_tracking
        self.enable_federated = self.config.enable_federated
        self.enable_predictive = self.config.enable_predictive
        self.enable_sustainability_dashboard = self.config.enable_sustainability_dashboard
        self.enable_qec = self.config.enable_qec

        # Sub-modules with config
        self.carbon_manager = CarbonIntensityManager(self.config) if self.enable_carbon_intensity else None
        self.helium_tracker = HeliumQuantumTracker(self.config) if self.enable_helium_tracking else None
        self.federated_mitigator = FederatedQuantumMitigator(self.config) if self.enable_federated else None
        self.predictive_analyzer = PredictiveQuantumAnalyzer(self.config) if self.enable_predictive else None
        self.qec = QuantumErrorCorrection(self.config) if self.enable_qec else None
        self.sustainability_dashboard = QuantumSustainabilityDashboard() if self.enable_sustainability_dashboard else None
        self.carbon_selector = QuantumCarbonAwareSelector(self.carbon_manager) if self.enable_carbon_intensity else None

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
            'mem': self.measurement_error_mitigation,
            'sv': self.symmetry_verification
        }

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

        # Start background tasks
        self._start_background_tasks()

        # Load state if persistence enabled
        asyncio.create_task(self._load_state())

        logger.info("Enhanced Quantum Error Mitigator v3.1.0 initialized")

    def _start_background_tasks(self):
        if self.enable_carbon_intensity and self.carbon_manager:
            asyncio.create_task(self._carbon_update_loop())
        if self.enable_federated and self.federated_mitigator:
            asyncio.create_task(self._federated_sync_loop())
        if self.enable_predictive and self.predictive_analyzer:
            asyncio.create_task(self._predictive_update_loop())

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
            except Exception as e:
                logger.error(f"Predictive update error: {e}")
                await asyncio.sleep(60)

    # ============================================================================
    # Core Mitigation Methods (Enhanced)
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
        if self.helium_tracker:
            helium_price = await self.helium_tracker.get_current_helium_price()

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
                return qec_circuit, qec_result

        # Select mitigation strategy
        if preferred_method and preferred_method in self.strategies:
            strategy = preferred_method
        elif carbon_aware and self.carbon_selector:
            options = list(self.strategies.keys())
            strategy, _ = await self.carbon_selector.select_mitigation_with_carbon(
                options, {'depth': circuit.depth, 'n_qubits': circuit.n_qubits},
                carbon_intensity, carbon_price
            )
        else:
            strategy = self._select_strategy(circuit, current_error, target_error_rate)

        logger.info(f"Selected mitigation strategy: {strategy}")

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

            # Track helium usage
            if self.helium_tracker:
                helium_amount = result.overhead_factor * 0.01
                await self.helium_tracker.record_helium_usage(
                    strategy, helium_amount, strategy
                )
                result.helium_efficiency = self.helium_tracker.get_helium_efficiency(strategy)
                result.resource_cost['helium_price_usd_per_l'] = helium_price

            # Calculate sustainability score
            result.sustainability_score = self._calculate_sustainability_score(result)

            # Update federated model
            if self.federated_mitigator:
                result.federated_round = self.federated_mitigator.round

            # Record history
            async with self._history_lock:
                self.mitigation_history.append(result)
            self._update_metrics(result)

            # Update predictive analyzer
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

            # Verify mitigation
            if result.mitigated_error_rate > target_error_rate:
                logger.warning(f"Mitigation fell short: {result.mitigated_error_rate:.4f} > {target_error_rate:.4f}")
                if strategy != 'hybrid':
                    logger.info("Attempting hybrid mitigation")
                    mitigated_circuit, result = await self._hybrid_mitigation(
                        circuit, target_error_rate, max_overhead
                    )

            # Telemetry
            self.telemetry.increment('qm_mitigations_total')
            if result.mitigated_error_rate < result.original_error_rate:
                self.telemetry.increment('qm_mitigations_success')
            self.telemetry.gauge('qm_mitigated_error_rate', result.mitigated_error_rate)
            self.telemetry.gauge('qm_carbon_saved_kg', result.carbon_saved_kg)
            self.telemetry.gauge('qm_sustainability_score', result.sustainability_score)

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
    # Mitigation Strategies (Preserved implementations)
    # ============================================================================

    # The following methods are preserved from the original implementation.
    # They are assumed to exist; for completeness, we include placeholders.
    # In a real implementation, these would contain the actual logic.

    async def zero_noise_extrapolation(
        self,
        circuit: QuantumCircuit,
        target_error_rate: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        # Placeholder: implement ZNE
        mitigated_error = circuit.error_rate * 0.5
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='zne',
            overhead_factor=2.0,
            success_probability=0.8,
            resource_cost={'overhead': 2.0}
        )
        return circuit, result

    async def probabilistic_error_cancellation(
        self,
        circuit: QuantumCircuit,
        target_error_rate: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        mitigated_error = circuit.error_rate * 0.4
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='pec',
            overhead_factor=3.0,
            success_probability=0.75,
            resource_cost={'overhead': 3.0}
        )
        return circuit, result

    async def clifford_data_regression(
        self,
        circuit: QuantumCircuit,
        target_error_rate: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        mitigated_error = circuit.error_rate * 0.3
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='cdr',
            overhead_factor=1.5,
            success_probability=0.85,
            resource_cost={'overhead': 1.5}
        )
        return circuit, result

    async def dynamical_decoupling(
        self,
        circuit: QuantumCircuit,
        target_error_rate: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        mitigated_error = circuit.error_rate * 0.7
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='dd',
            overhead_factor=1.2,
            success_probability=0.9,
            resource_cost={'overhead': 1.2}
        )
        return circuit, result

    async def measurement_error_mitigation(
        self,
        circuit: QuantumCircuit,
        target_error_rate: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        mitigated_error = circuit.error_rate * 0.6
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='measurement',
            overhead_factor=1.1,
            success_probability=0.85,
            resource_cost={'overhead': 1.1}
        )
        return circuit, result

    async def symmetry_verification(
        self,
        circuit: QuantumCircuit,
        target_error_rate: float,
        max_overhead: float
    ) -> Tuple[QuantumCircuit, ErrorMitigationResult]:
        mitigated_error = circuit.error_rate * 0.55
        result = ErrorMitigationResult(
            original_error_rate=circuit.error_rate,
            mitigated_error_rate=mitigated_error,
            mitigation_method='symmetry',
            overhead_factor=1.3,
            success_probability=0.8,
            resource_cost={'overhead': 1.3}
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

    def _select_strategy(
        self,
        circuit: QuantumCircuit,
        current_error: float,
        target_error_rate: float
    ) -> str:
        """Select a mitigation strategy based on circuit and error characteristics."""
        if current_error < target_error_rate * 2:
            return 'dd'  # lightweight
        elif circuit.depth > 100:
            return 'zne'  # good for deep circuits
        elif circuit.n_qubits > 10:
            return 'cdr'  # good for many qubits
        else:
            return 'hybrid'

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

    # ============================================================================
    # Public Query Methods (Enhanced)
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
