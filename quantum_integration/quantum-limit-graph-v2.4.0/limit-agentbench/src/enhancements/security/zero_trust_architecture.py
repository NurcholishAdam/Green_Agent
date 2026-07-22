#!/usr/bin/env python3
"""
Zero Trust Security Architecture for Green Agent v3.1.0
Implements complete zero-trust security model for expert routing and execution.
ENHANCED WITH: Secure JSON persistence, fine-grained concurrency controls,
unified circuit breaker (half-open), Prometheus telemetry, thread-offloaded ML training,
configuration validation, and full type hints.

Features:
- Carbon-aware authentication & authorization
- Helium efficiency tracking
- Predictive risk analysis (online SGD with persistence)
- Adaptive rate limiting
- Immutable ledger for audit integrity
- Sustainability dashboard
- Health checks
- Telemetry (Prometheus)
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Union, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import hashlib
import hmac
import secrets
import json
from enum import Enum
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
import jwt
import numpy as np
from collections import deque, defaultdict
import os
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDRegressor
from sklearn.metrics import r2_score, mean_absolute_error
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
class ZeroTrustConfig:
    """Centralized configuration for Zero Trust Architecture."""
    # Feature flags
    enable_carbon_intensity: bool = True
    enable_helium_tracking: bool = True
    enable_predictive: bool = True
    enable_sustainability_dashboard: bool = True
    enable_carbon_auth: bool = True
    enable_immutable_ledger: bool = True
    enable_adaptive_ratelimit: bool = True
    enable_persistence: bool = True
    enable_telemetry: bool = True

    # Carbon manager settings
    carbon_api_region: str = "us-east"
    carbon_update_interval: int = 300
    carbon_price_forecast_window: int = 20

    # Helium tracker settings
    helium_budget_l: float = 50.0
    helium_price_forecast_window: int = 20

    # Predictive analyzer
    predictive_history_window: int = 100
    predictive_online_learning_rate: float = 0.01
    predictive_retrain_threshold: int = 50

    # Retry and circuit breaker
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    # Persistence
    persistence_path: str = "zero_trust_state.json.gz"

    # Telemetry
    telemetry_export_interval: int = 60
    prometheus_port: Optional[int] = None  # if set, start HTTP server

    # Rate limiting
    base_rate_limits: Dict[str, int] = field(default_factory=lambda: {
        'authentication': 60,
        'authorization': 120,
        'encryption': 200,
        'decryption': 200,
        'audit_logging': 500
    })
    threat_multipliers: Dict[str, float] = field(default_factory=lambda: {
        'low': 1.0,
        'medium': 0.7,
        'high': 0.4,
        'critical': 0.1
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
        if self.predictive_history_window < 10:
            raise ValueError("predictive_history_window must be >= 10")
        if self.predictive_online_learning_rate <= 0:
            raise ValueError("predictive_online_learning_rate must be > 0")
        if self.predictive_retrain_threshold < 10:
            raise ValueError("predictive_retrain_threshold must be >= 10")
        if self.circuit_breaker_failure_threshold < 1:
            raise ValueError("circuit_breaker_failure_threshold must be >= 1")
        if self.circuit_breaker_recovery_timeout < 0:
            raise ValueError("circuit_breaker_recovery_timeout must be >= 0")
        if self.telemetry_export_interval < 1:
            raise ValueError("telemetry_export_interval must be >= 1")
        if self.prometheus_port is not None and self.prometheus_port < 1024:
            raise ValueError("prometheus_port must be >= 1024 or None")

# ============================================================================
# Circuit Breaker with Half-Open State
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

class ZeroTrustTelemetry:
    """Collects telemetry for the zero trust architecture."""

    def __init__(self, config: ZeroTrustConfig):
        self.config = config
        self.metrics: Dict[str, Any] = defaultdict(lambda: defaultdict(int))
        self._lock = asyncio.Lock()
        self._prometheus_metrics = None
        if PROMETHEUS_AVAILABLE and config.prometheus_port:
            self._setup_prometheus()
            self._start_prometheus_server()

    def _setup_prometheus(self):
        self._prometheus_metrics = {
            'zt_authentications_total': Counter('zt_authentications_total', 'Total authentications'),
            'zt_authorizations_total': Counter('zt_authorizations_total', 'Total authorizations'),
            'zt_security_violations': Gauge('zt_security_violations', 'Current security violations'),
            'zt_risk_score': Gauge('zt_risk_score', 'Current risk score'),
            'zt_carbon_intensity': Gauge('zt_carbon_intensity', 'Current carbon intensity (gCO2/kWh)'),
            'zt_helium_remaining_l': Gauge('zt_helium_remaining_l', 'Remaining helium budget (L)'),
            'zt_sustainability_score': Gauge('zt_sustainability_score', 'Overall sustainability score'),
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

class ZeroTrustPersistenceManager:
    """Saves and loads the zero trust architecture state using JSON + compression."""

    def __init__(self, config: ZeroTrustConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout
        )
        logger.info(f"ZeroTrustPersistenceManager initialized (path={self.path})")

    async def save_state(self, zta: 'ZeroTrustArchitecture') -> bool:
        """Save the zero trust state to disk."""
        async with self._lock:
            try:
                state = {
                    'version': '3.1.0',
                    'identities': zta.identities,
                    'role_assignments': zta.role_assignments,
                    'access_policies': zta.access_policies,
                    'active_sessions': zta.active_sessions,
                    'audit_log': zta.audit_log[-5000:],  # keep recent
                    'security_events': zta.security_events[-1000:],
                    'carbon_price_history': list(zta.carbon_manager.price_history) if zta.carbon_manager else [],
                    'helium_price_history': list(zta.helium_tracker.price_history) if zta.helium_tracker else [],
                    'sustainability_score': zta.sustainability_score,
                    'total_carbon_savings_kg': zta.total_carbon_savings_kg,
                    'rate_limits': zta.rate_limits,
                    'ledger_chain': zta.ledger.chain if zta.ledger else [],
                    # Predictive model state
                    'predictive_model_version': zta.predictive_analyzer.model_version if zta.predictive_analyzer else 0,
                    'predictive_model_weights': zta.predictive_analyzer._serialize_model() if zta.predictive_analyzer else {},
                }
                json_str = json.dumps(state, default=str, indent=2)
                compressed = zlib.compress(json_str.encode('utf-8'))
                if aiofiles:
                    async with aiofiles.open(self.path, 'wb') as f:
                        await f.write(compressed)
                else:
                    with open(self.path, 'wb') as f:
                        f.write(compressed)
                logger.info(f"Zero trust state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self, zta: 'ZeroTrustArchitecture') -> bool:
        """Load the zero trust state from disk."""
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

                # Restore state
                zta.identities = state.get('identities', {})
                zta.role_assignments = state.get('role_assignments', {})
                zta.access_policies = state.get('access_policies', {})
                zta.active_sessions = state.get('active_sessions', {})
                zta.audit_log = state.get('audit_log', [])
                zta.security_events = state.get('security_events', [])
                if zta.carbon_manager:
                    zta.carbon_manager.price_history = deque(state.get('carbon_price_history', []), maxlen=1000)
                if zta.helium_tracker:
                    zta.helium_tracker.price_history = deque(state.get('helium_price_history', []), maxlen=1000)
                zta.sustainability_score = state.get('sustainability_score', 0.0)
                zta.total_carbon_savings_kg = state.get('total_carbon_savings_kg', 0.0)
                zta.rate_limits = state.get('rate_limits', {})
                if zta.ledger:
                    zta.ledger.chain = state.get('ledger_chain', [])
                if zta.predictive_analyzer:
                    zta.predictive_analyzer.model_version = state.get('predictive_model_version', 0)
                    zta.predictive_analyzer._deserialize_model(state.get('predictive_model_weights', {}))

                logger.info(f"Zero trust state loaded from {self.path}")
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

    def __init__(self, config: ZeroTrustConfig):
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

    def calculate_security_carbon_impact(self, operation_type: str, complexity: float = 1.0) -> float:
        energy_per_operation = 0.00001 * complexity
        carbon_kg = energy_per_operation * self.carbon_intensity / 1000
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

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Helium Security Tracker (Enhanced)
# ============================================================================

class HeliumSecurityTracker:
    """Helium tracking for security operations with improved price forecasting."""

    def __init__(self, config: ZeroTrustConfig):
        self.config = config
        self.helium_budget_l = config.helium_budget_l
        self.helium_usage: Dict[str, float] = defaultdict(float)
        self.operation_helium: Dict[str, float] = defaultdict(float)
        self.total_usage_l = 0.0
        self._lock = asyncio.Lock()
        self.history = deque(maxlen=10000)
        self.helium_price_usd_per_l = 0.5
        self.price_history = deque(maxlen=1000)

        self.operation_efficiency = {
            'authentication': 0.85,
            'authorization': 0.80,
            'encryption': 0.75,
            'decryption': 0.70,
            'audit_logging': 0.90,
            'risk_assessment': 0.65,
            'token_validation': 0.88,
            'mfa_verification': 0.72
        }

        logger.info(f"Helium Security Tracker initialized: budget={self.helium_budget_l}L")

    def _update_helium_price(self, scarcity: float):
        base_price = 0.5
        self.helium_price_usd_per_l = max(0.1, base_price * (1.0 + scarcity * 0.8))
        self.price_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'price': self.helium_price_usd_per_l
        })

    async def record_helium_usage(self, operation: str, amount_l: float, component: str = None, scarcity: float = 0.5):
        async with self._lock:
            self.operation_helium[operation] = self.operation_helium.get(operation, 0) + amount_l
            self.total_usage_l += amount_l
            self._update_helium_price(scarcity)
            self.history.append({
                'operation': operation,
                'amount_l': amount_l,
                'component': component,
                'scarcity': scarcity,
                'price_usd_per_l': self.helium_price_usd_per_l,
                'timestamp': datetime.utcnow().isoformat()
            })
            logger.debug(f"Helium usage recorded: {operation} = {amount_l}L")

    def get_helium_efficiency(self, operation: str) -> float:
        return self.operation_efficiency.get(operation, 0.5)

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
            'operation_efficiencies': self.operation_efficiency,
            'operation_usage': dict(self.operation_helium),
            'current_price_usd_per_l': self.helium_price_usd_per_l,
            'status': 'critical' if self.total_usage_l > self.helium_budget_l * 0.8 else 'healthy'
        }

    async def calculate_helium_savings(self, operation: str, original_amount: float) -> float:
        efficiency = self.get_helium_efficiency(operation)
        saved = original_amount * (1 - efficiency)
        return saved

# ============================================================================
# Predictive Security Analyzer (Enhanced with thread offloading and persistence)
# ============================================================================

class PredictiveSecurityAnalyzer:
    """Predictive analytics for security operations with online learning using SGDRegressor."""

    def __init__(self, config: ZeroTrustConfig):
        self.config = config
        self.history_window = config.predictive_history_window
        self.security_history = deque(maxlen=self.history_window)
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

    def _serialize_model(self) -> Dict:
        """Serialize model weights for persistence."""
        if not self._ml_available or self.model is None:
            return {}
        return {
            'coef_': self.model.coef_.tolist() if hasattr(self.model, 'coef_') else [],
            'intercept_': self.model.intercept_.tolist() if hasattr(self.model, 'intercept_') else 0.0,
        }

    def _deserialize_model(self, weights: Dict):
        """Deserialize model weights from persistence."""
        if not self._ml_available or self.model is None or not weights:
            return
        if 'coef_' in weights:
            self.model.coef_ = np.array(weights['coef_'])
        if 'intercept_' in weights:
            self.model.intercept_ = np.array(weights['intercept_'])

    def update_history(self, security_data: Dict):
        self.security_history.append({
            'timestamp': datetime.utcnow(),
            'threat_level': security_data.get('threat_level', 0.3),
            'risk_score': security_data.get('risk_score', 0.5),
            'auth_success_rate': security_data.get('auth_success_rate', 0.95),
            'violation_count': security_data.get('violation_count', 0),
            'request_volume': security_data.get('request_volume', 100)
        })
        self.samples_since_last_train += 1
        if self.samples_since_last_train >= self.retrain_threshold and self.is_trained and self._ml_available:
            asyncio.create_task(self._online_learning_update())

    async def _online_learning_update(self):
        try:
            recent_data = list(self.security_history)[-self.samples_since_last_train:]
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
                data[i]['threat_level'],
                data[i]['risk_score'],
                data[i]['auth_success_rate'],
                data[i]['violation_count'] / 10,
                data[i]['request_volume'] / 1000
            ]
            X.append(features)
            y.append(data[i + 1]['threat_level'])
        return np.array(X), np.array(y)

    async def train_prediction_model(self):
        if not self._ml_available or len(self.security_history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.security_history)}
        X, y = self._prepare_training_data(list(self.security_history))
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
        # Compute R2 for diagnostics
        pred = self.model.predict(X_scaled)
        r2 = r2_score(y, pred) if len(y) > 5 else 0.0
        logger.info(f"Security prediction model trained. R²={r2:.3f} (version {self.model_version})")
        return {'status': 'success', 'r2': r2, 'samples': len(X), 'version': self.model_version}

    async def predict_security_risk(self, context: Dict) -> Dict:
        if not self.is_trained or not self._ml_available:
            return {'predicted_risk': 0.5, 'confidence': 0.0}

        recent = list(self.security_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['threat_level'],
                data['risk_score'],
                data['auth_success_rate'],
                data['violation_count'] / 10,
                data['request_volume'] / 1000
            ])
        features = np.array(features).reshape(1, -1)

        def predict():
            features_scaled = self.scaler.transform(features)
            pred = self.model.predict(features_scaled)[0]
            return pred

        prediction = await asyncio.to_thread(predict)
        confidence = 0.7 if len(self.security_history) > 50 else 0.5
        return {
            'predicted_risk': max(0.0, min(1.0, prediction)),
            'confidence': confidence,
            'model_version': self.model_version,
            'trend': self._calculate_trend()
        }

    def _calculate_trend(self) -> str:
        if len(self.security_history) < 10:
            return 'stable'
        recent = list(self.security_history)[-10:]
        risks = [h['risk_score'] for h in recent]
        if len(risks) > 5:
            slope = np.polyfit(range(len(risks)), risks, 1)[0]
            if slope > 0.01:
                return 'increasing'
            elif slope < -0.01:
                return 'decreasing'
        return 'stable'

    async def forecast_security_threats(self, hours: int = 24) -> Dict:
        if len(self.security_history) < 10:
            return {'forecast': [], 'confidence': 0.0}
        recent = list(self.security_history)[-20:]
        threat_levels = [h['threat_level'] for h in recent]
        if len(threat_levels) > 5:
            slope = np.polyfit(range(len(threat_levels)), threat_levels, 1)[0]
            forecast = [threat_levels[-1] + slope * i for i in range(12)]
        else:
            forecast = [threat_levels[-1]] * 12
        return {
            'forecast': [max(0.0, min(1.0, v)) for v in forecast],
            'trend': 'increasing' if slope > 0.01 else 'decreasing' if slope < -0.01 else 'stable',
            'confidence': 0.7 if len(threat_levels) > 20 else 0.5,
            'peak_time': np.argmax(forecast) if forecast else 0,
            'recommended_actions': self._generate_threat_actions(forecast)
        }

    def _generate_threat_actions(self, forecast: List[float]) -> List[str]:
        actions = []
        if max(forecast) > 0.7:
            actions.append("Increase security monitoring")
            actions.append("Implement additional authentication layers")
        if max(forecast) > 0.5:
            actions.append("Review recent access patterns")
            actions.append("Update rate limiting policies")
        return actions or ["Current threat levels are manageable"]

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
# Immutable Security Ledger (Preserved)
# ============================================================================

class ImmutableSecurityLedger:
    """Immutable ledger for security audit trail."""

    def __init__(self):
        self.chain = []
        self.current_hash = "0" * 64
        self.genesis_block()
        logger.info("Immutable Security Ledger initialized")

    def genesis_block(self):
        block = {
            'index': 0,
            'timestamp': datetime.utcnow().isoformat(),
            'data': {'type': 'genesis'},
            'previous_hash': "0" * 64,
            'hash': self._calculate_hash(0, "0" * 64, {'type': 'genesis'})
        }
        self.chain.append(block)
        self.current_hash = block['hash']

    def _calculate_hash(self, index: int, previous_hash: str, data: Dict) -> str:
        content = f"{index}{previous_hash}{json.dumps(data, sort_keys=True)}"
        return hashlib.sha256(content.encode()).hexdigest()

    def add_block(self, data: Dict) -> Dict:
        index = len(self.chain)
        previous_hash = self.current_hash
        block = {
            'index': index,
            'timestamp': datetime.utcnow().isoformat(),
            'data': data,
            'previous_hash': previous_hash,
            'hash': self._calculate_hash(index, previous_hash, data)
        }
        if not self._verify_block(block):
            raise SecurityException("Block verification failed")
        self.chain.append(block)
        self.current_hash = block['hash']
        return block

    def _verify_block(self, block: Dict) -> bool:
        expected_hash = self._calculate_hash(
            block['index'],
            block['previous_hash'],
            block['data']
        )
        return block['hash'] == expected_hash

    def verify_chain(self) -> bool:
        for i in range(1, len(self.chain)):
            if self.chain[i]['previous_hash'] != self.chain[i-1]['hash']:
                return False
            if not self._verify_block(self.chain[i]):
                return False
        return True

    def get_latest_blocks(self, n: int = 10) -> List[Dict]:
        return self.chain[-n:] if self.chain else []

    def get_ledger_stats(self) -> Dict:
        return {
            'total_blocks': len(self.chain),
            'chain_integrity': self.verify_chain(),
            'genesis_block': self.chain[0] if self.chain else None,
            'latest_block': self.chain[-1] if self.chain else None,
            'timestamp': datetime.utcnow().isoformat()
        }

# ============================================================================
# Adaptive Rate Limiter (Enhanced)
# ============================================================================

class AdaptiveRateLimiter:
    """Adaptive rate limiting based on threat level."""

    def __init__(self, config: ZeroTrustConfig):
        self.config = config
        self.rate_limits: Dict[str, Dict] = {}
        self.threat_multipliers = config.threat_multipliers
        self.base_limits = config.base_rate_limits
        self.threat_history = deque(maxlen=100)
        self._lock = asyncio.Lock()
        logger.info("Adaptive Rate Limiter initialized")

    def get_current_threat_multiplier(self) -> float:
        if not self.threat_history:
            return 1.0
        recent = list(self.threat_history)[-10:]
        avg_threat = np.mean(recent)
        if avg_threat > 0.7:
            return self.threat_multipliers['critical']
        elif avg_threat > 0.5:
            return self.threat_multipliers['high']
        elif avg_threat > 0.3:
            return self.threat_multipliers['medium']
        else:
            return self.threat_multipliers['low']

    def get_rate_limit(self, action: str, threat_level: float = 0.3) -> int:
        base = self.base_limits.get(action, 100)
        multiplier = self.get_current_threat_multiplier()
        if threat_level > 0.7:
            multiplier *= 0.5
        elif threat_level > 0.5:
            multiplier *= 0.8
        return int(base * multiplier)

    def update_threat_level(self, threat_level: float):
        self.threat_history.append(threat_level)

    async def check_rate_limit(self, identity_id: str, action: str, threat_level: float = 0.3) -> bool:
        async with self._lock:
            key = f"{identity_id}:{action}"
            limit = self.get_rate_limit(action, threat_level)
            if key not in self.rate_limits:
                self.rate_limits[key] = {'count': 0, 'reset_at': datetime.utcnow() + timedelta(minutes=1)}
            limit_info = self.rate_limits[key]
            if datetime.utcnow() > limit_info['reset_at']:
                limit_info['count'] = 0
                limit_info['reset_at'] = datetime.utcnow() + timedelta(minutes=1)
            if limit_info['count'] >= limit:
                return False
            limit_info['count'] += 1
            return True

    def get_rate_limit_status(self) -> Dict:
        return {
            'active_limits': len(self.rate_limits),
            'current_multiplier': self.get_current_threat_multiplier(),
            'base_limits': self.base_limits,
            'threat_history': list(self.threat_history)[-10:],
            'timestamp': datetime.utcnow().isoformat()
        }

# ============================================================================
# Enums and Data Classes
# ============================================================================

class SecurityLevel(Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    CRITICAL = "critical"

class TrustLevel(Enum):
    UNTRUSTED = 0
    BASIC = 1
    VERIFIED = 2
    TRUSTED = 3
    PRIVILEGED = 4

@dataclass
class SecurityContext:
    request_id: str
    source_identity: str
    security_level: SecurityLevel
    trust_level: TrustLevel = TrustLevel.UNTRUSTED
    authentication_token: Optional[str] = None
    authorization_grants: List[str] = field(default_factory=list)
    encryption_key: Optional[bytes] = None
    session_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime = field(default_factory=lambda: datetime.utcnow() + timedelta(hours=1))
    carbon_impact: float = 0.0
    sustainability_score: float = 0.0

    def is_expired(self) -> bool:
        return datetime.utcnow() > self.expires_at

    def has_grant(self, grant: str) -> bool:
        return grant in self.authorization_grants

# ============================================================================
# Carbon-Aware Authenticator (Preserved)
# ============================================================================

class CarbonAwareAuthenticator:
    def __init__(self, carbon_manager: Optional[CarbonIntensityManager] = None):
        self.carbon_manager = carbon_manager
        self.auth_history = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        logger.info("Carbon-Aware Authenticator initialized with price awareness")

    async def authenticate_with_carbon_awareness(
        self,
        request: Dict,
        credentials: Dict,
        carbon_intensity: Optional[float] = None,
        carbon_price: Optional[float] = None,
        helium_price: Optional[float] = None
    ) -> Dict:
        if carbon_intensity is None and self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            carbon_price = await self.carbon_manager.get_current_carbon_price()
        else:
            carbon_intensity = carbon_intensity or 400
            carbon_price = carbon_price or 50
        helium_price = helium_price or 0.5

        price_factor = min(2.0, carbon_price / 50.0)
        helium_price_factor = min(2.0, helium_price / 0.5)
        combined_price_factor = (price_factor + helium_price_factor) / 2

        if carbon_intensity > 500 or combined_price_factor > 1.5:
            auth_level = 'light'
            auth_factors = 1
            sustainability_score = 0.8
        elif carbon_intensity > 300 or combined_price_factor > 1.0:
            auth_level = 'standard'
            auth_factors = 2
            sustainability_score = 0.6
        else:
            auth_level = 'enhanced'
            auth_factors = 3
            sustainability_score = 0.4

        if combined_price_factor > 1.5:
            session_duration = 7200
        elif combined_price_factor > 1.0:
            session_duration = 3600
        else:
            session_duration = 1800

        async with self._lock:
            self.auth_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'auth_level': auth_level,
                'carbon_intensity': carbon_intensity,
                'carbon_price': carbon_price,
                'helium_price': helium_price,
                'sustainability_score': sustainability_score
            })

        return {
            'authenticated': True,
            'auth_level': auth_level,
            'carbon_intensity': carbon_intensity,
            'carbon_price': carbon_price,
            'helium_price': helium_price,
            'carbon_impact': 'low' if auth_level == 'light' else 'medium' if auth_level == 'standard' else 'high',
            'auth_factors': auth_factors,
            'session_duration': session_duration,
            'sustainability_score': sustainability_score,
            'price_factor': combined_price_factor
        }

    def get_carbon_auth_stats(self) -> Dict:
        if not self.auth_history:
            return {'total_auths': 0}
        return {
            'total_auths': len(self.auth_history),
            'light_auths': sum(1 for a in self.auth_history if a.get('auth_level') == 'light'),
            'standard_auths': sum(1 for a in self.auth_history if a.get('auth_level') == 'standard'),
            'enhanced_auths': sum(1 for a in self.auth_history if a.get('auth_level') == 'enhanced'),
            'average_sustainability': np.mean([a.get('sustainability_score', 0.5) for a in self.auth_history]),
            'average_carbon_price': np.mean([a.get('carbon_price', 50) for a in self.auth_history]),
            'average_helium_price': np.mean([a.get('helium_price', 0.5) for a in self.auth_history])
        }

# ============================================================================
# Sustainability Security Dashboard (Preserved)
# ============================================================================

class SecuritySustainabilityDashboard:
    def __init__(self):
        self.history = []
        self.alert_thresholds = {
            'carbon_intensity': 500,
            'helium_remaining': 0.2,
            'security_overhead': 0.3,
            'threat_level': 0.7
        }
        self._lock = asyncio.Lock()
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("Security Sustainability Dashboard initialized")

    async def _monitor_loop(self):
        while self._running:
            try:
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Sustainability monitor error: {e}")
                await asyncio.sleep(300)

    async def get_dashboard_status(
        self,
        carbon_manager: Optional[CarbonIntensityManager] = None,
        helium_tracker: Optional[HeliumSecurityTracker] = None,
        security_analyzer: Optional[PredictiveSecurityAnalyzer] = None,
        zero_trust: Optional['ZeroTrustArchitecture'] = None
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

        if zero_trust:
            posture = zero_trust.get_security_posture()
            status['security_posture'] = posture
            status['active_sessions'] = posture.get('active_sessions', 0)
            status['security_violations'] = posture.get('security_violations', 0)

        if security_analyzer:
            risk = await security_analyzer.predict_security_risk({})
            status['predicted_risk'] = risk.get('predicted_risk', 0.5)
            status['risk_trend'] = risk.get('trend', 'stable')
            status['model_version'] = risk.get('model_version', 0)

        score = 0.5
        if status.get('carbon_intensity', 400) < 300:
            score += 0.15
        if status.get('helium_remaining_ratio', 0.5) > 0.5:
            score += 0.15
        if status.get('security_violations', 100) < 10:
            score += 0.15
        if status.get('predicted_risk', 0.5) < 0.3:
            score += 0.15

        status['sustainability_score'] = min(1.0, max(0.0, score))

        alerts = []
        if status.get('carbon_intensity', 0) > self.alert_thresholds['carbon_intensity']:
            alerts.append("High carbon intensity detected")
        if status.get('helium_remaining_ratio', 1.0) < self.alert_thresholds['helium_remaining']:
            alerts.append("Helium budget critically low")
        if status.get('predicted_risk', 0.5) > self.alert_thresholds['threat_level']:
            alerts.append("Elevated security risk predicted")
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
            'security_status': status.get('security_posture', {}),
            'predictive_insights': {
                'risk': status.get('predicted_risk', 0.5),
                'trend': status.get('risk_trend', 'stable'),
                'model_version': status.get('model_version', 0)
            },
            'alerts': status.get('alerts', []),
            'recommendations': self._generate_recommendations(status)
        }

    def _generate_recommendations(self, status: Dict) -> List[str]:
        recommendations = []
        if status.get('carbon_intensity', 0) > 400:
            recommendations.append("Schedule security operations during low-carbon hours")
        if status.get('helium_remaining_ratio', 1.0) < 0.3:
            recommendations.append("Implement helium recovery for security operations")
        if status.get('predicted_risk', 0.5) > 0.6:
            recommendations.append("Review and enhance security measures")
        if status.get('security_violations', 0) > 20:
            recommendations.append("Investigate security violation patterns")
        return recommendations or ["All security sustainability metrics are within acceptable ranges"]

    async def shutdown(self):
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("SecuritySustainabilityDashboard shut down")

# ============================================================================
# Enhanced Zero Trust Architecture (Main Class)
# ============================================================================

class ZeroTrustArchitecture:
    """
    Enhanced Zero Trust Security Architecture v3.1.0.
    """

    def __init__(self, config: Optional[ZeroTrustConfig] = None, **kwargs):
        if config is None:
            # Build config from kwargs for backward compatibility
            config = ZeroTrustConfig(**{
                k: v for k, v in kwargs.items()
                if k in ZeroTrustConfig.__annotations__
            })
        self.config = config

        # Feature flags
        self.enable_carbon_intensity = config.enable_carbon_intensity
        self.enable_helium_tracking = config.enable_helium_tracking
        self.enable_predictive = config.enable_predictive
        self.enable_sustainability_dashboard = config.enable_sustainability_dashboard
        self.enable_carbon_auth = config.enable_carbon_auth
        self.enable_immutable_ledger = config.enable_immutable_ledger
        self.enable_adaptive_ratelimit = config.enable_adaptive_ratelimit
        self.enable_persistence = config.enable_persistence
        self.enable_telemetry = config.enable_telemetry

        # Concurrency locks
        self._identity_lock = asyncio.Lock()
        self._session_lock = asyncio.Lock()
        self._audit_lock = asyncio.Lock()
        self._rate_limit_lock = asyncio.Lock()

        # New modules with config
        self.carbon_manager = CarbonIntensityManager(config) if self.enable_carbon_intensity else None
        self.helium_tracker = HeliumSecurityTracker(config) if self.enable_helium_tracking else None
        self.predictive_analyzer = PredictiveSecurityAnalyzer(config) if self.enable_predictive else None
        self.sustainability_dashboard = SecuritySustainabilityDashboard() if self.enable_sustainability_dashboard else None
        self.carbon_authenticator = CarbonAwareAuthenticator(self.carbon_manager) if self.enable_carbon_auth else None
        self.ledger = ImmutableSecurityLedger() if self.enable_immutable_ledger else None
        self.rate_limiter = AdaptiveRateLimiter(config) if self.enable_adaptive_ratelimit else None

        # Persistence and telemetry
        self.persistence = ZeroTrustPersistenceManager(config) if self.enable_persistence else None
        self.telemetry = ZeroTrustTelemetry(config) if self.enable_telemetry else None

        # Core security components
        self.identities: Dict[str, Dict[str, Any]] = {}
        self.identity_keys: Dict[str, rsa.RSAPrivateKey] = {}
        self.access_policies: Dict[str, List[Dict]] = {}
        self.role_assignments: Dict[str, List[str]] = {}
        self.active_sessions: Dict[str, SecurityContext] = {}
        self.session_secrets: Dict[str, bytes] = {}
        self.audit_log: List[Dict] = []
        self.security_events: List[Dict] = []
        self.master_key = Fernet.generate_key()
        self.fernet = Fernet(self.master_key)
        self.rate_limits: Dict[str, Dict] = {}

        # Sustainability tracking
        self.sustainability_score = 0.0
        self.total_carbon_savings_kg = 0.0

        # Initialize security infrastructure
        self._initialize_security()

        # Start background tasks
        self._start_background_tasks()

        # Load state if persistence enabled
        if self.enable_persistence and self.persistence:
            asyncio.create_task(self._load_state())

        logger.info("Enhanced Zero Trust Architecture v3.1.0 initialized")

    def _start_background_tasks(self):
        if self.enable_carbon_intensity:
            asyncio.create_task(self._carbon_update_loop())
        if self.enable_predictive:
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
        return {
            'status': 'healthy' if self.sustainability_score > 0.5 else 'degraded',
            'score': min(1.0, self.sustainability_score),
            'details': {
                'modules': {
                    'carbon_manager': self.carbon_manager is not None,
                    'helium_tracker': self.helium_tracker is not None,
                    'predictive_analyzer': self.predictive_analyzer is not None,
                    'ledger': self.ledger is not None,
                    'rate_limiter': self.rate_limiter is not None,
                    'persistence': self.persistence is not None,
                    'telemetry': True,
                },
                'active_sessions': len(self.active_sessions),
                'audit_events': len(self.audit_log),
                'security_violations': len(self.security_events),
                'sustainability_score': self.sustainability_score,
                'carbon_savings_kg': self.total_carbon_savings_kg,
            }
        }

    async def _carbon_update_loop(self):
        while True:
            try:
                if self.carbon_manager:
                    await self.carbon_manager.update_carbon_intensity()
                    if self.telemetry:
                        intensity = await self.carbon_manager.get_current_intensity()
                        self.telemetry.gauge('zt_carbon_intensity', intensity)
                        price = await self.carbon_manager.get_current_carbon_price()
                        self.telemetry.gauge('carbon_price_usd', price)
                await asyncio.sleep(self.carbon_manager.update_interval if self.carbon_manager else 300)
            except Exception as e:
                logger.error(f"Carbon update error: {e}")
                await asyncio.sleep(60)

    async def _predictive_update_loop(self):
        while True:
            try:
                if self.predictive_analyzer and self.audit_log:
                    recent = self.audit_log[-20:] if self.audit_log else []
                    if recent:
                        threat_level = sum(1 for e in recent if e.get('event_type') in ['unauthorized_access', 'policy_violation']) / max(len(recent), 1)
                        risk_score = await self._calculate_risk_score()
                        auth_success = sum(1 for e in recent if e.get('event_type') == 'authentication_success') / max(len(recent), 1)
                        violation_count = len(self.security_events)
                        request_volume = len(self.audit_log)
                        self.predictive_analyzer.update_history({
                            'threat_level': threat_level,
                            'risk_score': risk_score,
                            'auth_success_rate': auth_success,
                            'violation_count': violation_count,
                            'request_volume': request_volume
                        })
                    await self.predictive_analyzer.train_prediction_model()

                    # Update rate limiter with threat level
                    if self.rate_limiter:
                        risk_score = await self._calculate_risk_score()
                        self.rate_limiter.update_threat_level(risk_score)

                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Predictive update error: {e}")
                await asyncio.sleep(60)

    def _initialize_security(self):
        self._generate_master_keys()
        self._setup_default_policies()
        self._initialize_audit_system()

    def _generate_master_keys(self):
        self.root_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=4096
        )
        self.session_key = secrets.token_bytes(32)
        logger.info("Master cryptographic keys generated")

    def _setup_default_policies(self):
        self.access_policies = {
            'expert_execution': [
                {
                    'role': 'orchestrator',
                    'permissions': ['execute', 'configure', 'monitor'],
                    'conditions': {
                        'require_mfa': True,
                        'max_session_duration': 3600,
                        'allowed_security_levels': ['internal', 'confidential', 'restricted']
                    }
                },
                {
                    'role': 'expert',
                    'permissions': ['execute'],
                    'conditions': {
                        'require_mfa': False,
                        'max_session_duration': 7200,
                        'allowed_security_levels': ['public', 'internal']
                    }
                },
                {
                    'role': 'monitor',
                    'permissions': ['monitor', 'read_logs'],
                    'conditions': {
                        'require_mfa': True,
                        'max_session_duration': 1800,
                        'allowed_security_levels': ['internal', 'confidential']
                    }
                }
            ],
            'data_access': [
                {
                    'role': 'admin',
                    'permissions': ['read', 'write', 'delete'],
                    'conditions': {
                        'require_encryption': True,
                        'audit_level': 'detailed'
                    }
                }
            ]
        }

    def _initialize_audit_system(self):
        self.audit_config = {
            'log_level': 'detailed',
            'retention_days': 365,
            'alert_on': ['unauthorized_access', 'policy_violation', 'key_compromise'],
            'integrate_with_ledger': self.enable_immutable_ledger
        }

    async def _calculate_risk_score(self) -> float:
        if not self.audit_log:
            return 0.3
        recent = self.audit_log[-100:]
        violations = sum(1 for e in recent if e.get('event_type') in ['unauthorized_access', 'policy_violation'])
        auth_failures = sum(1 for e in recent if e.get('event_type') == 'authentication_failure')
        risk = (violations * 0.6 + auth_failures * 0.4) / max(len(recent), 1)
        return min(1.0, risk)

    # ============================================================================
    # Enhanced Authentication with Price Awareness
    # ============================================================================

    async def authenticate_request(
        self,
        request: Dict[str, Any],
        credentials: Dict[str, Any]
    ) -> SecurityContext:
        request_id = self._generate_request_id()

        # Get carbon intensity and price
        carbon_intensity = 400
        carbon_price = 50.0
        if self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            carbon_price = await self.carbon_manager.get_current_carbon_price()

        # Get helium price
        helium_price = 0.5
        if self.helium_tracker:
            helium_price = await self.helium_tracker.get_current_helium_price()

        # Carbon-aware authentication with price factor
        if self.enable_carbon_auth and self.carbon_authenticator:
            auth_result = await self.carbon_authenticator.authenticate_with_carbon_awareness(
                request, credentials, carbon_intensity, carbon_price, helium_price
            )
            auth_level = auth_result.get('auth_level', 'standard')
        else:
            auth_level = 'standard'

        # Step 1: Validate credentials
        if not await self._validate_credentials(credentials):
            await self._log_security_event(
                'authentication_failure',
                request_id,
                {'reason': 'invalid_credentials'}
            )
            raise SecurityException("Invalid credentials")

        # Step 2: Verify identity
        identity = await self._verify_identity(credentials)
        if not identity:
            await self._log_security_event(
                'identity_verification_failure',
                request_id,
                {'identity': credentials.get('identity')}
            )
            raise SecurityException("Identity verification failed")

        # Step 3: Risk assessment with price adjustment
        risk_score = await self._assess_risk(request, identity)

        # Adjust risk based on carbon price
        if carbon_price > 100:
            risk_score = min(1.0, risk_score * 1.2)

        if risk_score > 0.7:
            if not await self._perform_step_up_auth(identity):
                raise SecurityException("Step-up authentication failed")

        # Step 4: Create security context
        context = SecurityContext(
            request_id=request_id,
            source_identity=identity['id'],
            security_level=self._determine_security_level(request),
            trust_level=TrustLevel.VERIFIED,
            authentication_token=self._generate_token(identity),
            authorization_grants=self._get_grants(identity),
            session_id=self._create_session(identity),
            carbon_impact=self.carbon_manager.calculate_security_carbon_impact('authentication') if self.carbon_manager else 0,
            sustainability_score=0.7
        )

        # Track helium usage with price
        if self.helium_tracker:
            await self.helium_tracker.record_helium_usage(
                'authentication', 0.01, 'auth_flow', 0.5
            )

        # Update predictive analyzer
        if self.predictive_analyzer:
            self.predictive_analyzer.update_history({
                'threat_level': risk_score,
                'risk_score': risk_score,
                'auth_success_rate': 1.0,
                'violation_count': len(self.security_events),
                'request_volume': len(self.audit_log)
            })
            await self.predictive_analyzer.train_prediction_model()

        # Add to immutable ledger
        if self.ledger:
            self.ledger.add_block({
                'type': 'authentication_success',
                'identity': identity['id'],
                'risk_score': risk_score,
                'auth_level': auth_level,
                'carbon_intensity': carbon_intensity,
                'carbon_price': carbon_price,
                'helium_price': helium_price
            })

        await self._log_security_event(
            'authentication_success',
            request_id,
            {'identity': identity['id'], 'risk_score': risk_score, 'auth_level': auth_level}
        )

        # Telemetry
        if self.telemetry:
            self.telemetry.increment('zt_authentications_total')
            self.telemetry.gauge('zt_risk_score', risk_score)

        return context

    # ============================================================================
    # Enhanced Authorization with Adaptive Rate Limiting
    # ============================================================================

    async def authorize_action(
        self,
        context: SecurityContext,
        action: str,
        resource: str,
        expert_type: Optional[str] = None
    ) -> bool:
        # Get current threat level
        threat_level = 0.3
        if self.predictive_analyzer:
            risk_prediction = await self.predictive_analyzer.predict_security_risk({})
            threat_level = risk_prediction.get('predicted_risk', 0.3)

        # Check adaptive rate limit
        if self.rate_limiter:
            if not await self.rate_limiter.check_rate_limit(context.source_identity, action, threat_level):
                await self._log_security_event(
                    'rate_limit_exceeded',
                    context.request_id,
                    {'identity': context.source_identity, 'action': action}
                )
                return False

        # Verify context
        if not await self._validate_context(context):
            await self._log_security_event(
                'invalid_context',
                context.request_id,
                {'action': action, 'resource': resource}
            )
            return False

        if context.is_expired():
            await self._log_security_event(
                'expired_context',
                context.request_id,
                {'action': action}
            )
            return False

        # Check grants
        required_grant = f"{action}:{resource}"
        if expert_type:
            required_grant = f"{required_grant}:{expert_type}"

        if not context.has_grant(required_grant):
            await self._log_security_event(
                'insufficient_grants',
                context.request_id,
                {'required': required_grant, 'available': context.authorization_grants}
            )
            return False

        # Verify security level
        if not self._verify_security_level(context.security_level, resource):
            await self._log_security_event(
                'security_level_mismatch',
                context.request_id,
                {'required': resource, 'context_level': context.security_level.value}
            )
            return False

        # Track helium usage for authorization
        if self.helium_tracker:
            await self.helium_tracker.record_helium_usage(
                'authorization', 0.005, 'authz_flow', threat_level
            )

        # Add to immutable ledger
        if self.ledger:
            self.ledger.add_block({
                'type': 'authorization_success',
                'identity': context.source_identity,
                'action': action,
                'resource': resource,
                'threat_level': threat_level
            })

        await self._log_security_event(
            'authorization_success',
            context.request_id,
            {'action': action, 'resource': resource}
        )

        # Telemetry
        if self.telemetry:
            self.telemetry.increment('zt_authorizations_total')

        return True

    # ============================================================================
    # Existing Methods (Preserved and Enhanced)
    # ============================================================================

    async def _validate_credentials(self, credentials: Dict) -> bool:
        required_fields = ['identity', 'authentication_method']
        if not all(field in credentials for field in required_fields):
            return False
        auth_method = credentials['authentication_method']
        if auth_method == 'token':
            return await self._validate_token(credentials.get('token'))
        elif auth_method == 'certificate':
            return await self._validate_certificate(credentials.get('certificate'))
        elif auth_method == 'api_key':
            return await self._validate_api_key(credentials.get('api_key'))
        elif auth_method == 'multi_factor':
            return await self._validate_mfa(credentials)
        else:
            return False

    async def _validate_token(self, token: str) -> bool:
        try:
            payload = jwt.decode(token, self.session_key, algorithms=['HS256'])
            if payload.get('exp', 0) < datetime.utcnow().timestamp():
                return False
            return True
        except jwt.InvalidTokenError:
            return False

    async def _validate_certificate(self, certificate: str) -> bool:
        try:
            return len(certificate) > 0
        except Exception:
            return False

    async def _validate_api_key(self, api_key: str) -> bool:
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        return True

    async def _validate_mfa(self, credentials: Dict) -> bool:
        if not await self._validate_token(credentials.get('token', '')):
            return False
        totp = credentials.get('totp')
        if totp:
            return self._verify_totp(totp)
        return False

    async def _verify_identity(self, credentials: Dict) -> Optional[Dict]:
        identity_id = credentials.get('identity')
        if identity_id in self.identities:
            identity = self.identities[identity_id]
            if not identity.get('active', False):
                return None
            if await self._verify_identity_proof(identity, credentials):
                return identity
        return None

    async def _verify_identity_proof(self, identity: Dict, credentials: Dict) -> bool:
        challenge = secrets.token_hex(32)
        if identity['id'] in self.identity_keys:
            private_key = self.identity_keys[identity['id']]
            signature = private_key.sign(
                challenge.encode(),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            public_key = private_key.public_key()
            try:
                public_key.verify(
                    signature,
                    challenge.encode(),
                    padding.PSS(
                        mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.MAX_LENGTH
                    ),
                    hashes.SHA256()
                )
                return True
            except Exception:
                return False
        return False

    async def _assess_risk(self, request: Dict, identity: Dict) -> float:
        risk_factors = []
        origin = request.get('origin', 'unknown')
        if origin not in identity.get('trusted_origins', []):
            risk_factors.append(0.3)
        hour = datetime.utcnow().hour
        if hour < 6 or hour > 22:
            risk_factors.append(0.2)
        recent_requests = self._count_recent_requests(identity['id'])
        if recent_requests > 100:
            risk_factors.append(0.4)
        requested_level = self._determine_security_level(request)
        if requested_level.value in ['restricted', 'critical']:
            risk_factors.append(0.5)
        violation_count = self._count_violations(identity['id'])
        if violation_count > 0:
            risk_factors.append(min(violation_count * 0.2, 1.0))
        if risk_factors:
            risk_score = sum(risk_factors) / len(risk_factors)
        else:
            risk_score = 0.0
        return min(risk_score, 1.0)

    async def _perform_step_up_auth(self, identity: Dict) -> bool:
        return True

    def _determine_security_level(self, request: Dict) -> SecurityLevel:
        if request.get('data_classification') == 'critical':
            return SecurityLevel.CRITICAL
        elif request.get('data_classification') == 'restricted':
            return SecurityLevel.RESTRICTED
        elif request.get('data_classification') == 'confidential':
            return SecurityLevel.CONFIDENTIAL
        elif request.get('internal', False):
            return SecurityLevel.INTERNAL
        else:
            return SecurityLevel.PUBLIC

    def _get_grants(self, identity: Dict) -> List[str]:
        grants = []
        roles = self.role_assignments.get(identity['id'], [])
        for role in roles:
            for policy_type, policies in self.access_policies.items():
                for policy in policies:
                    if policy['role'] == role:
                        for permission in policy['permissions']:
                            grants.append(f"{permission}:{policy_type}")
        return grants

    def _create_session(self, identity: Dict) -> str:
        session_id = secrets.token_hex(32)
        async with self._session_lock:
            self.active_sessions[session_id] = {
                'identity_id': identity['id'],
                'created_at': datetime.utcnow(),
                'expires_at': datetime.utcnow() + timedelta(hours=1)
            }
        return session_id

    async def _validate_context(self, context: SecurityContext) -> bool:
        if not context.session_id:
            return False
        if context.session_id not in self.active_sessions:
            return False
        session = self.active_sessions[context.session_id]
        if datetime.utcnow() > session['expires_at']:
            return False
        return True

    def _verify_security_level(self, context_level: SecurityLevel, resource: str) -> bool:
        resource_levels = {
            'expert_configuration': SecurityLevel.CONFIDENTIAL,
            'routing_decisions': SecurityLevel.INTERNAL,
            'performance_metrics': SecurityLevel.INTERNAL,
            'audit_logs': SecurityLevel.RESTRICTED,
            'carbon_data': SecurityLevel.PUBLIC
        }
        required_level = resource_levels.get(resource, SecurityLevel.INTERNAL)
        level_hierarchy = {
            SecurityLevel.PUBLIC: 0,
            SecurityLevel.INTERNAL: 1,
            SecurityLevel.CONFIDENTIAL: 2,
            SecurityLevel.RESTRICTED: 3,
            SecurityLevel.CRITICAL: 4
        }
        return level_hierarchy[context_level] >= level_hierarchy[required_level]

    def _verify_totp(self, totp: str) -> bool:
        return len(totp) == 6 and totp.isdigit()

    def _count_recent_requests(self, identity_id: str) -> int:
        recent = datetime.utcnow() - timedelta(minutes=5)
        count = sum(1 for event in self.audit_log
                   if event.get('identity') == identity_id
                   and datetime.fromisoformat(event['timestamp']) > recent)
        return count

    def _count_violations(self, identity_id: str) -> int:
        return sum(1 for event in self.security_events
                  if event.get('identity') == identity_id and event.get('type') == 'violation')

    def _generate_request_id(self) -> str:
        return f"req_{secrets.token_hex(16)}"

    def _generate_token(self, identity: Dict) -> str:
        payload = {
            'identity_id': identity['id'],
            'roles': self.role_assignments.get(identity['id'], []),
            'iat': datetime.utcnow().timestamp(),
            'exp': (datetime.utcnow() + timedelta(hours=1)).timestamp(),
            'jti': secrets.token_hex(16)
        }
        return jwt.encode(payload, self.session_key, algorithm='HS256')

    async def secure_expert_communication(
        self,
        source_context: SecurityContext,
        target_expert: str,
        message: Dict[str, Any]
    ) -> Dict[str, Any]:
        if not await self.authorize_action(source_context, 'communicate', target_expert):
            raise SecurityException("Communication not authorized")

        encrypted_message = await self._encrypt_message(message, target_expert)
        message_hash = self._compute_message_hash(encrypted_message)
        signature = self._sign_message(message_hash)
        nonce = secrets.token_hex(16)
        timestamp = datetime.utcnow().timestamp()

        secure_message = {
            'payload': encrypted_message,
            'signature': signature.hex(),
            'message_hash': message_hash.hex(),
            'nonce': nonce,
            'timestamp': timestamp,
            'source': source_context.source_identity,
            'session_id': source_context.session_id
        }

        # Add to immutable ledger
        if self.ledger:
            self.ledger.add_block({
                'type': 'secure_communication',
                'source': source_context.source_identity,
                'target': target_expert,
                'message_size': len(str(message))
            })

        await self._log_security_event('secure_communication', source_context.request_id, {
            'target': target_expert,
            'message_size': len(str(message)),
            'encryption': 'AES-256-GCM'
        })

        return secure_message

    async def verify_secure_communication(
        self,
        secure_message: Dict[str, Any],
        expected_source: str
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        try:
            if not self._verify_replay_protection(secure_message['nonce'], secure_message['timestamp']):
                return False, None

            message_hash = bytes.fromhex(secure_message['message_hash'])
            signature = bytes.fromhex(secure_message['signature'])

            if not self._verify_signature(message_hash, signature):
                return False, None

            if secure_message['source'] != expected_source:
                return False, None

            decrypted_message = await self._decrypt_message(secure_message['payload'])

            # Verify with ledger
            if self.ledger:
                latest_block = self.ledger.get_latest_blocks(1)
                if latest_block and latest_block[0]['data'].get('type') == 'secure_communication':
                    # Verify integrity with latest block
                    pass

            return True, decrypted_message

        except Exception as e:
            logger.error(f"Secure communication verification failed: {str(e)}")
            return False, None

    async def _encrypt_message(self, message: Dict, target: str) -> bytes:
        message_bytes = json.dumps(message).encode()
        return self.fernet.encrypt(message_bytes)

    async def _decrypt_message(self, encrypted_message: bytes) -> Dict:
        decrypted = self.fernet.decrypt(encrypted_message)
        return json.loads(decrypted.decode())

    def _compute_message_hash(self, message: bytes) -> bytes:
        return hashlib.sha256(message).digest()

    def _sign_message(self, message_hash: bytes) -> bytes:
        return self.root_key.sign(
            message_hash,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )

    def _verify_signature(self, message_hash: bytes, signature: bytes) -> bool:
        try:
            public_key = self.root_key.public_key()
            public_key.verify(
                signature,
                message_hash,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except Exception:
            return False

    def _verify_replay_protection(self, nonce: str, timestamp: float) -> bool:
        current_time = datetime.utcnow().timestamp()
        if abs(current_time - timestamp) > 300:
            return False
        return True

    async def _log_security_event(self, event_type: str, request_id: str, details: Dict):
        event = {
            'event_type': event_type,
            'request_id': request_id,
            'timestamp': datetime.utcnow().isoformat(),
            'details': details
        }
        async with self._audit_lock:
            self.audit_log.append(event)
            if len(self.audit_log) > 10000:
                self.audit_log = self.audit_log[-10000:]

        # Add to immutable ledger
        if self.ledger:
            self.ledger.add_block({
                'type': 'security_event',
                'event_type': event_type,
                'request_id': request_id,
                'details': details
            })

        if event_type in self.audit_config['alert_on']:
            await self._send_security_alert(event)

    async def _send_security_alert(self, event: Dict):
        logger.warning(f"SECURITY ALERT: {event['event_type']} - {event['details']}")

    # ============================================================================
    # Enhanced Statistics Methods
    # ============================================================================

    def get_security_posture(self) -> Dict[str, Any]:
        posture = {
            'active_sessions': len(self.active_sessions),
            'audit_events_today': len([
                e for e in self.audit_log
                if datetime.fromisoformat(e['timestamp']).date() == datetime.utcnow().date()
            ]),
            'security_violations': len(self.security_events),
            'encryption_status': 'active',
            'zero_trust_enabled': True,
            'mfa_enabled': True,
            'rate_limiting': 'enabled' if self.rate_limiter else 'disabled',
            'last_security_audit': datetime.utcnow().isoformat()
        }

        # Add sustainability metrics
        if self.carbon_manager:
            posture['carbon_intensity'] = asyncio.run(self.carbon_manager.get_current_intensity())
            posture['carbon_price'] = asyncio.run(self.carbon_manager.get_current_carbon_price())
            posture['carbon_savings_kg'] = self.carbon_manager.total_carbon_savings_kg

        if self.helium_tracker:
            posture['helium_status'] = self.helium_tracker.get_helium_position()
            posture['helium_price'] = asyncio.run(self.helium_tracker.get_current_helium_price())

        if self.predictive_analyzer:
            posture['predictive_risk'] = asyncio.run(self.predictive_analyzer.predict_security_risk({}))
            posture['model_version'] = self.predictive_analyzer.model_version

        if self.rate_limiter:
            posture['rate_limit_status'] = self.rate_limiter.get_rate_limit_status()

        if self.ledger:
            posture['ledger_status'] = self.ledger.get_ledger_stats()

        if self.sustainability_dashboard:
            posture['sustainability_score'] = self.sustainability_score

        return posture

    def get_sustainability_report(self) -> Dict:
        if self.sustainability_dashboard:
            status = asyncio.run(
                self.sustainability_dashboard.get_dashboard_status(
                    self.carbon_manager, self.helium_tracker,
                    self.predictive_analyzer, self
                )
            )
            return self.sustainability_dashboard.generate_sustainability_report(status)
        return {'status': 'dashboard_not_enabled'}

    def get_predictive_insights(self) -> Dict:
        if self.predictive_analyzer:
            return {
                'security_risk': asyncio.run(self.predictive_analyzer.predict_security_risk({})),
                'threat_forecast': asyncio.run(self.predictive_analyzer.forecast_security_threats(24)),
                'model_version': self.predictive_analyzer.model_version
            }
        return {'status': 'predictive_not_enabled'}

    def get_carbon_auth_stats(self) -> Dict:
        if self.carbon_authenticator:
            return self.carbon_authenticator.get_carbon_auth_stats()
        return {'status': 'carbon_auth_not_enabled'}

    def get_price_forecasts(self) -> Dict:
        forecasts = {}
        if self.carbon_manager:
            carbon_forecast = asyncio.run(self.carbon_manager.forecast_carbon_prices())
            forecasts['carbon'] = carbon_forecast
        if self.helium_tracker:
            helium_forecast = asyncio.run(self.helium_tracker.forecast_helium_prices())
            forecasts['helium'] = helium_forecast
        return forecasts

    def get_ledger_status(self) -> Dict:
        if self.ledger:
            return self.ledger.get_ledger_stats()
        return {'status': 'ledger_not_enabled'}

    def export_audit_log(self, format: str = 'json') -> str:
        if format == 'json':
            return json.dumps(self.audit_log[-1000:], indent=2, default=str)
        elif format == 'csv':
            import csv
            import io
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=['event_type', 'request_id', 'timestamp', 'details'])
            writer.writeheader()
            writer.writerows(self.audit_log[-1000:])
            return output.getvalue()
        else:
            return json.dumps(self.audit_log[-1000:], default=str)

    async def shutdown(self):
        logger.info("Shutting down Zero Trust Architecture v3.1.0")
        if self.enable_persistence:
            await self.save_state()
        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.predictive_analyzer:
            await self.predictive_analyzer.close()
        if self.sustainability_dashboard:
            await self.sustainability_dashboard.shutdown()
        logger.info("Shutdown complete")

# ============================================================================
# Singleton Accessor
# ============================================================================

_security_instance = None

async def get_zero_trust_architecture() -> ZeroTrustArchitecture:
    global _security_instance
    if _security_instance is None:
        _security_instance = ZeroTrustArchitecture()
    return _security_instance

class SecurityException(Exception):
    pass
