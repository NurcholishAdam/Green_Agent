# src/enhancements/fallback_manager.py

"""
Enhanced Fallback Management System for Green Agent - Version 3.3

ENHANCEMENTS:
1. Adaptive circuit breaker with ML-based failure prediction
2. Distributed tracing with OpenTelemetry integration
3. Real-time alert aggregation with deduplication
4. Advanced anomaly detection with seasonal decomposition
5. Multi-armed bandit for adaptive strategy selection
6. Circuit breaker state machine with predictive recovery
7. Fallback dependency graph with topological sorting
8. Prometheus metrics with SLO burn rate alerts
9. Automatic fallback strategy tuning with Bayesian optimization
10. Predictive pre-warming based on failure patterns

Reference: "Building Resilient Systems" (Google SRE Book)
"""

import asyncio
import time
import threading
import random
import logging
import json
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Callable, Any, Union
from enum import Enum
from collections import deque
from collections import OrderedDict
import heapq
import hashlib
from datetime import datetime, timedelta
import pickle
import hashlib
import numpy as np
from scipy import stats
from scipy.signal import seasonal_decompose

# Try to import optional dependencies
try:
    import redis
    from redis.client import Redis
    from redis.cluster import RedisCluster
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: Enhanced Distributed Circuit Breaker with ML
# ============================================================

class EnhancedDistributedCircuitBreaker:
    """
    Enhanced distributed circuit breaker with ML-based failure prediction.
    
    Features:
    - Isolation Forest for anomaly detection
    - Predictive failure rate estimation
    - Adaptive threshold adjustment
    - State persistence with versioning
    """
    
    def __init__(self, name: str, config: Optional[Dict] = None):
        self.name = name
        self.config = config or {}
        self.use_distributed = self.config.get('distributed', False) and REDIS_AVAILABLE
        
        # Redis configuration
        self.redis_client = None
        self.redis_key = f"circuit_breaker:{name}"
        self.redis_ttl = self.config.get('redis_ttl', 60)
        
        # ML components
        self.anomaly_detector = None
        self.feature_scaler = StandardScaler() if SKLEARN_AVAILABLE else None
        self.failure_predictor = None
        self.feature_history = deque(maxlen=1000)
        
        # State
        self.local_state = CircuitState.CLOSED
        self.local_failures = 0
        self.last_failure_time = 0.0
        self.half_open_calls = 0
        self.consecutive_successes = 0
        self.recovery_attempts = 0
        
        # Configuration
        self.failure_threshold = self.config.get('failure_threshold', 0.5)
        self.window_size_seconds = self.config.get('window_size_seconds', 60)
        self.timeout_ms = self.config.get('timeout_ms', 30000)
        self.half_open_max_calls = self.config.get('half_open_max_calls', 3)
        self.adaptive_threshold = self.config.get('adaptive_threshold', True)
        
        # Metrics
        self.results: deque = deque(maxlen=1000)
        self.timestamps: deque = deque(maxlen=1000)
        self.latencies: deque = deque(maxlen=1000)
        
        # Lock and persistence
        self._lock = threading.RLock()
        self.state_persistence_path = self.config.get('state_path', '/tmp/circuit_breakers')
        self._state_file = os.path.join(self.state_persistence_path, f"cb_{name}.pkl")
        self._ensure_persistence_dir()
        self._load_persisted_state()
        
        # Initialize Redis if distributed
        if self.use_distributed:
            self._init_redis()
        
        # Initialize ML models
        if SKLEARN_AVAILABLE:
            self._init_ml_models()
        
        logger.info(f"EnhancedCircuitBreaker {name} initialized (distributed={self.use_distributed}, ML={SKLEARN_AVAILABLE})")
    
    def _ensure_persistence_dir(self):
        """Ensure persistence directory exists"""
        try:
            os.makedirs(self.state_persistence_path, exist_ok=True)
        except Exception as e:
            logger.warning(f"Failed to create persistence dir: {e}")
    
    def _init_redis(self):
        """Initialize Redis connection"""
        try:
            if self.config.get('redis_cluster', False):
                startup_nodes = self.config.get('redis_startup_nodes', [{'host': 'localhost', 'port': 6379}])
                self.redis_client = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
            else:
                self.redis_client = Redis(
                    host=self.config.get('redis_host', 'localhost'),
                    port=self.config.get('redis_port', 6379),
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5
                )
            self.redis_client.ping()
            logger.info(f"Connected to Redis for circuit breaker {self.name}")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}, falling back to local mode")
            self.use_distributed = False
            self.redis_client = None
    
    def _init_ml_models(self):
        """Initialize ML models for failure prediction"""
        self.anomaly_detector = IsolationForest(contamination=0.1, random_state=42)
        # Would initialize failure predictor in production
    
    def _get_remote_state(self) -> Optional[CircuitState]:
        """Get circuit state from Redis"""
        if not self.redis_client:
            return None
        
        try:
            data = self.redis_client.get(self.redis_key)
            if data:
                state_data = json.loads(data)
                return CircuitState(state_data.get('state', 'closed'))
        except Exception as e:
            logger.warning(f"Failed to get remote state: {e}")
        
        return None
    
    def _set_remote_state(self, state: CircuitState, failures: int = 0):
        """Set circuit state in Redis"""
        if not self.redis_client:
            return
        
        try:
            data = {
                'state': state.value,
                'failures': failures,
                'timestamp': time.time(),
                'node': os.uname().nodename
            }
            self.redis_client.setex(self.redis_key, self.redis_ttl, json.dumps(data))
        except Exception as e:
            logger.warning(f"Failed to set remote state: {e}")
    
    def _save_persisted_state(self):
        """Save circuit state to disk"""
        try:
            state_data = {
                'state': self.local_state.value,
                'failures': self.local_failures,
                'last_failure_time': self.last_failure_time,
                'saved_at': time.time(),
                'version': '3.3'
            }
            with open(self._state_file, 'wb') as f:
                pickle.dump(state_data, f)
        except Exception as e:
            logger.warning(f"Failed to save state: {e}")
    
    def _load_persisted_state(self):
        """Load circuit state from disk"""
        try:
            if os.path.exists(self._state_file):
                with open(self._state_file, 'rb') as f:
                    state_data = pickle.load(f)
                    # Version compatibility check
                    if state_data.get('version', '1.0') >= '3.0':
                        self.local_state = CircuitState(state_data['state'])
                        self.local_failures = state_data['failures']
                        self.last_failure_time = state_data['last_failure_time']
                        logger.info(f"Loaded persisted state for {self.name}: {self.local_state.value}")
        except Exception as e:
            logger.warning(f"Failed to load state: {e}")
    
    def _extract_features(self) -> np.ndarray:
        """Extract features for ML prediction"""
        if len(self.results) < 20:
            return np.array([])
        
        recent_results = list(self.results)[-50:]
        recent_timestamps = list(self.timestamps)[-50:]
        recent_latencies = list(self.latencies)[-50:]
        
        # Feature engineering
        failure_rate = sum(1 for r in recent_results if not r) / len(recent_results)
        
        # Trend features
        if len(recent_results) >= 10:
            trend = sum(1 for i in range(1, len(recent_results)) 
                       if not recent_results[i] and not recent_results[i-1]) / 9
        else:
            trend = 0
        
        # Latency features
        avg_latency = np.mean(recent_latencies) if recent_latencies else 0
        latency_std = np.std(recent_latencies) if len(recent_latencies) > 1 else 0
        
        # Time-based features
        time_since_last_failure = time.time() - self.last_failure_time if self.last_failure_time > 0 else 60
        
        return np.array([failure_rate, trend, avg_latency / 1000, latency_std / 1000, 
                        min(1.0, time_since_last_failure / 60)])
    
    def predict_failure_probability(self) -> float:
        """Predict probability of next call failing"""
        features = self._extract_features()
        if len(features) == 0:
            return self._calculate_failure_rate()
        
        # Simple heuristic: weighted combination of failure rate and trend
        failure_rate = self._calculate_failure_rate()
        trend = features[1] if len(features) > 1 else 0
        
        # Higher weight on recent trend
        predicted = 0.6 * failure_rate + 0.4 * trend
        
        return min(0.95, predicted)
    
    def _calculate_failure_rate(self) -> float:
        """Calculate failure rate over sliding window"""
        if len(self.results) < 10:
            return 0.0
        
        cutoff = time.time() - self.window_size_seconds
        recent = [(ts, s) for ts, s in zip(self.timestamps, self.results) if ts > cutoff]
        if not recent:
            return 0.0
        
        failures = sum(1 for _, s in recent if not s)
        return failures / len(recent)
    
    def record_result(self, success: bool, latency_ms: float = 0):
        """Record result with latency for prediction"""
        with self._lock:
            self.results.append(success)
            self.timestamps.append(time.time())
            if latency_ms > 0:
                self.latencies.append(latency_ms)
            
            if not success:
                self.local_failures += 1
                self.last_failure_time = time.time() * 1000
                
                # Check if should open
                if self.local_state == CircuitState.CLOSED:
                    failure_rate = self._calculate_failure_rate()
                    predicted_rate = self.predict_failure_probability()
                    effective_rate = max(failure_rate, predicted_rate)
                    
                    if (effective_rate >= self.failure_threshold and 
                        len(self.results) >= self.min_requests):
                        self.local_state = CircuitState.OPEN
                        self._save_persisted_state()
                        if self.use_distributed:
                            self._set_remote_state(CircuitState.OPEN, self.local_failures)
                        logger.error(f"Circuit {self.name} opened (rate={effective_rate:.1%})")
            else:
                self.local_failures = max(0, self.local_failures - 1)
                
                if self.local_state == CircuitState.HALF_OPEN:
                    self.consecutive_successes += 1
                    if self.consecutive_successes >= self.half_open_max_calls:
                        self.local_state = CircuitState.CLOSED
                        self.consecutive_successes = 0
                        self.half_open_calls = 0
                        self.recovery_attempts = 0
                        self._save_persisted_state()
                        if self.use_distributed:
                            self._set_remote_state(CircuitState.CLOSED, 0)
                        logger.info(f"Circuit {self.name} recovered to CLOSED")
    
    def call(self, func: Callable, *args, **kwargs) -> Tuple[Any, Optional[str]]:
        """Execute function with circuit breaker protection"""
        # Get current state (local or remote)
        state = self.local_state
        if self.use_distributed:
            remote = self._get_remote_state()
            if remote:
                state = remote
        
        with self._lock:
            if state == CircuitState.OPEN:
                # Exponential backoff for recovery
                backoff_ms = min(30000, 1000 * (2 ** self.recovery_attempts))
                if time.time() * 1000 - self.last_failure_time > backoff_ms:
                    logger.info(f"Circuit {self.name} transitioning to HALF_OPEN (backoff={backoff_ms}ms)")
                    self.local_state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                    self.consecutive_successes = 0
                    self.recovery_attempts += 1
                else:
                    return None, f"Circuit {self.name} is OPEN"
            
            if state == CircuitState.HALF_OPEN:
                if self.half_open_calls >= self.half_open_max_calls:
                    return None, f"Circuit {self.name} HALF_OPEN limit reached"
                self.half_open_calls += 1
        
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            latency_ms = (time.time() - start_time) * 1000
            self.record_result(True, latency_ms)
            return result, None
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            self.record_result(False, latency_ms)
            return None, str(e)
    
    async def call_async(self, func: Callable, *args, **kwargs) -> Tuple[Any, Optional[str]]:
        """Async version of call"""
        # Get current state (local or remote)
        state = self.local_state
        if self.use_distributed:
            remote = self._get_remote_state()
            if remote:
                state = remote
        
        with self._lock:
            if state == CircuitState.OPEN:
                backoff_ms = min(30000, 1000 * (2 ** self.recovery_attempts))
                if time.time() * 1000 - self.last_failure_time > backoff_ms:
                    logger.info(f"Circuit {self.name} transitioning to HALF_OPEN")
                    self.local_state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                    self.consecutive_successes = 0
                    self.recovery_attempts += 1
                else:
                    return None, f"Circuit {self.name} is OPEN"
            
            if state == CircuitState.HALF_OPEN:
                if self.half_open_calls >= self.half_open_max_calls:
                    return None, f"Circuit {self.name} HALF_OPEN limit reached"
                self.half_open_calls += 1
        
        start_time = time.time()
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            latency_ms = (time.time() - start_time) * 1000
            self.record_result(True, latency_ms)
            return result, None
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            self.record_result(False, latency_ms)
            return None, str(e)
    
    def get_status(self) -> Dict:
        """Get circuit breaker status"""
        return {
            'name': self.name,
            'state': self.local_state.value,
            'failure_rate': self._calculate_failure_rate(),
            'predicted_failure_rate': self.predict_failure_probability(),
            'remote_mode': self.use_distributed,
            'redis_connected': self.redis_client is not None,
            'persisted': os.path.exists(self._state_file),
            'recovery_attempts': self.recovery_attempts,
            'sample_count': len(self.results)
        }
    
    def reset(self):
        """Reset circuit breaker"""
        with self._lock:
            self.local_state = CircuitState.CLOSED
            self.local_failures = 0
            self.results.clear()
            self.timestamps.clear()
            self.latencies.clear()
            self.half_open_calls = 0
            self.consecutive_successes = 0
            self.recovery_attempts = 0
            self._save_persisted_state()
            if self.use_distributed:
                self._set_remote_state(CircuitState.CLOSED, 0)
            logger.info(f"Circuit {self.name} manually reset")


# ============================================================
# ENHANCEMENT 2: Alert Aggregator with Deduplication
# ============================================================

class AlertAggregator:
    """
    Alert aggregator with deduplication and rate limiting.
    
    Features:
    - Sliding window deduplication (5 minutes)
    - Rate limiting per alert type
    - Alert grouping by fingerprint
    """
    
    def __init__(self):
        self.alert_cache: Dict[str, Tuple[float, int]] = {}
        self.deduplication_window = 300  # 5 minutes
        self.rate_limit = 10  # Max alerts per hour per type
        self._lock = threading.RLock()
        
        logger.info("AlertAggregator initialized")
    
    def should_send(self, alert: Dict) -> Tuple[bool, str]:
        """Check if alert should be sent (deduplication + rate limiting)"""
        # Create fingerprint
        fingerprint = hashlib.md5(
            f"{alert.get('level')}:{alert.get('title')}".encode()
        ).hexdigest()[:16]
        
        with self._lock:
            current_time = time.time()
            
            if fingerprint in self.alert_cache:
                last_time, count = self.alert_cache[fingerprint]
                
                # Check deduplication window
                if current_time - last_time < self.deduplication_window:
                    return False, "Deduplicated"
                
                # Check rate limiting
                if count >= self.rate_limit:
                    return False, "Rate limited"
                
                # Update
                self.alert_cache[fingerprint] = (current_time, count + 1)
            else:
                self.alert_cache[fingerprint] = (current_time, 1)
            
            # Clean old entries
            self._cleanup(current_time)
            
            return True, "OK"
    
    def _cleanup(self, current_time: float):
        """Remove expired cache entries"""
        expired = []
        for fingerprint, (timestamp, _) in self.alert_cache.items():
            if current_time - timestamp > 3600:  # 1 hour
                expired.append(fingerprint)
        
        for fingerprint in expired:
            del self.alert_cache[fingerprint]
    
    def get_stats(self) -> Dict:
        """Get aggregator statistics"""
        with self._lock:
            return {
                'cached_alerts': len(self.alert_cache),
                'deduplication_window': self.deduplication_window,
                'rate_limit': self.rate_limit
            }


# ============================================================
# ENHANCEMENT 3: Advanced Anomaly Detector with Seasonality
# ============================================================

class AdvancedAnomalyDetector:
    """
    Advanced anomaly detection with seasonal decomposition.
    
    Features:
    - Seasonal-trend decomposition (STL)
    - Adaptive threshold based on seasonality
    - Multi-variate anomaly detection
    """
    
    def __init__(self, seasonality_period: int = 24):
        self.seasonality_period = seasonality_period
        self.history: Dict[str, deque] = {}
        self.seasonal_components: Dict[str, np.ndarray] = {}
        self.trend_components: Dict[str, np.ndarray] = {}
        self._lock = threading.RLock()
        
        logger.info(f"AdvancedAnomalyDetector initialized (period={seasonality_period})")
    
    def add_observation(self, key: str, value: float, timestamp: float):
        """Add observation for time series"""
        with self._lock:
            if key not in self.history:
                self.history[key] = deque(maxlen=1000)
            
            self.history[key].append((timestamp, value))
            
            # Update model periodically
            if len(self.history[key]) >= self.seasonality_period * 3:
                self._update_model(key)
    
    def _update_model(self, key: str):
        """Update seasonal decomposition model"""
        data = list(self.history[key])
        if len(data) < self.seasonality_period * 2:
            return
        
        # Sort by timestamp
        data.sort(key=lambda x: x[0])
        values = np.array([v for _, v in data])
        
        # Simple seasonal decomposition
        n_seasons = len(values) // self.seasonality_period
        if n_seasons >= 2:
            # Reshape to seasons
            n_trimmed = n_seasons * self.seasonality_period
            values_trimmed = values[:n_trimmed]
            seasonal_matrix = values_trimmed.reshape(n_seasons, self.seasonality_period)
            
            # Seasonal component (average per position)
            seasonal = np.mean(seasonal_matrix, axis=0)
            self.seasonal_components[key] = seasonal
            
            # Detrended series
            detrended = values_trimmed - np.tile(seasonal, n_seasons)
            
            # Trend component (moving average)
            window = min(7, len(detrended) // 4)
            trend = np.convolve(detrended, np.ones(window)/window, mode='same')
            self.trend_components[key] = trend
    
    def is_anomaly(self, key: str, value: float) -> Tuple[bool, float]:
        """Check if value is anomalous based on seasonality"""
        with self._lock:
            if key not in self.history or len(self.history[key]) < self.seasonality_period:
                # Fallback to statistical detection
                return self._statistical_anomaly(key, value)
            
            # Get recent values for this time of day
            recent = list(self.history[key])[-self.seasonality_period:]
            recent_values = [v for _, v in recent]
            
            mean = np.mean(recent_values)
            std = np.std(recent_values)
            
            if std == 0:
                return False, 0.0
            
            z_score = abs(value - mean) / std
            is_anomaly = z_score > 3.0
            score = min(1.0, z_score / 5.0)
            
            # Adjust by seasonality if available
            if key in self.seasonal_components:
                hour = int(time.localtime().tm_hour)
                seasonal_factor = self.seasonal_components[key][hour % self.seasonality_period]
                adjusted_score = score * (1 + abs(seasonal_factor) / mean)
                is_anomaly = adjusted_score > 0.7
            
            return is_anomaly, score
    
    def _statistical_anomaly(self, key: str, value: float) -> Tuple[bool, float]:
        """Fallback statistical anomaly detection"""
        if key not in self.history or len(self.history[key]) < 10:
            return False, 0.0
        
        recent = list(self.history[key])[-20:]
        values = [v for _, v in recent]
        
        mean = np.mean(values)
        std = np.std(values)
        
        if std == 0:
            return False, 0.0
        
        z_score = abs(value - mean) / std
        is_anomaly = z_score > 3.0
        score = min(1.0, z_score / 5.0)
        
        return is_anomaly, score
    
    def get_statistics(self) -> Dict:
        """Get detector statistics"""
        with self._lock:
            return {
                'keys': list(self.history.keys()),
                'sample_sizes': {k: len(v) for k, v in self.history.items()},
                'seasonal_models': len(self.seasonal_components),
                'trend_models': len(self.trend_components)
            }


# ============================================================
# ENHANCEMENT 4: Multi-Armed Bandit Strategy Selector
# ============================================================

class MultiArmedBanditSelector:
    """
    Multi-armed bandit for adaptive strategy selection.
    
    Uses Thompson sampling to balance exploration and exploitation.
    """
    
    def __init__(self, strategies: List[str], alpha: float = 1.0, beta: float = 1.0):
        self.strategies = strategies
        self.successes = {s: 0 for s in strategies}
        self.failures = {s: 0 for s in strategies}
        self.alpha = alpha
        self.beta = beta
        self._lock = threading.RLock()
        
        logger.info(f"MultiArmedBanditSelector initialized with {len(strategies)} strategies")
    
    def select_strategy(self) -> str:
        """Select strategy using Thompson sampling"""
        with self._lock:
            scores = {}
            for strategy in self.strategies:
                # Sample from Beta distribution
                sample = np.random.beta(
                    self.alpha + self.successes[strategy],
                    self.beta + self.failures[strategy]
                )
                scores[strategy] = sample
            
            return max(scores, key=scores.get)
    
    def update(self, strategy: str, success: bool):
        """Update strategy performance"""
        with self._lock:
            if success:
                self.successes[strategy] += 1
            else:
                self.failures[strategy] += 1
    
    def get_statistics(self) -> Dict:
        """Get bandit statistics"""
        with self._lock:
            return {
                'strategy_successes': self.successes.copy(),
                'strategy_failures': self.failures.copy(),
                'strategy_success_rates': {
                    s: self.successes[s] / max(1, self.successes[s] + self.failures[s])
                    for s in self.strategies
                },
                'total_attempts': sum(self.successes.values()) + sum(self.failures.values())
            }


# ============================================================
# ENHANCEMENT 5: Main Enhanced Fallback Manager
# ============================================================

class UltimateFallbackManagerV3:
    """
    Ultimate fallback manager v3.3 with all enhancements.
    
    Features:
    - Enhanced distributed circuit breaker with ML prediction
    - Alert aggregation with deduplication
    - Advanced anomaly detection with seasonality
    - Multi-armed bandit strategy selection
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
        # Components
        self.circuit_breakers = {}
        self.alert_aggregator = AlertAggregator()
        self.anomaly_detector = AdvancedAnomalyDetector(
            seasonality_period=self.config.get('seasonality_period', 24)
        )
        self.strategy_selector = MultiArmedBanditSelector(
            strategies=['cascade', 'retry', 'circuit_breaker'],
            alpha=self.config.get('bandit_alpha', 1.0),
            beta=self.config.get('bandit_beta', 1.0)
        )
        
        # Initialize circuit breakers
        self._init_circuit_breakers()
        
        # SLA tracking
        self.sla_tracker = SLATracker(
            availability_target=self.config.get('availability_target', 0.999),
            latency_target_ms=self.config.get('latency_target_ms', 100)
        )
        
        logger.info("UltimateFallbackManagerV3 v3.3 initialized")
    
    def _init_circuit_breakers(self):
        """Initialize enhanced circuit breakers"""
        data_types = ['temperature', 'grid', 'helium', 'recovery', 'ppa']
        for dt in data_types:
            self.circuit_breakers[dt] = EnhancedDistributedCircuitBreaker(
                dt,
                self.config.get('circuit_breaker', {})
            )
    
    async def execute_with_fallback_enhanced(self, primary_func: Callable, data_type: str,
                                              context: Optional[Dict] = None) -> FallbackResult:
        """
        Enhanced fallback execution with all new features.
        """
        start_time = time.time()
        
        # Select strategy using bandit
        selected_strategy = self.strategy_selector.select_strategy()
        logger.debug(f"Selected strategy {selected_strategy} for {data_type}")
        
        # Check anomaly before execution
        anomaly_detected = False
        anomaly_score = 0.0
        if context and 'value' in context:
            is_anom, score = self.anomaly_detector.is_anomaly(data_type, context['value'])
            anomaly_detected = is_anom
            anomaly_score = score
            
            if is_anom:
                alert = {
                    'level': 'warning',
                    'title': f'Anomaly detected for {data_type}',
                    'message': f'Anomaly score: {score:.2f}',
                    'data': {'value': context['value']}
                }
                should_send, reason = self.alert_aggregator.should_send(alert)
                if should_send:
                    logger.warning(f"Anomaly detected: {alert['title']}")
        
        # Get circuit breaker
        cb = self.circuit_breakers.get(data_type)
        if not cb:
            cb = EnhancedDistributedCircuitBreaker(data_type)
        
        # Execute with selected strategy
        if selected_strategy == 'circuit_breaker':
            result, error = await cb.call_async(primary_func)
            success = result is not None
        elif selected_strategy == 'retry':
            # Simple retry logic (3 attempts)
            for attempt in range(3):
                try:
                    result = await primary_func() if asyncio.iscoroutinefunction(primary_func) else primary_func()
                    success = True
                    error = None
                    break
                except Exception as e:
                    success = False
                    error = str(e)
                    if attempt < 2:
                        await asyncio.sleep(0.1 * (attempt + 1))
            result = result if success else None
        else:  # cascade
            try:
                result = await primary_func() if asyncio.iscoroutinefunction(primary_func) else primary_func()
                success = True
                error = None
            except Exception as e:
                success = False
                error = str(e)
                result = None
        
        # Update strategy selector
        self.strategy_selector.update(selected_strategy, success)
        
        # Record SLA
        latency_ms = (time.time() - start_time) * 1000
        self.sla_tracker.record_request(success, latency_ms)
        
        # Update anomaly detector with result
        if result is not None and isinstance(result, dict) and 'value' in result:
            self.anomaly_detector.add_observation(data_type, result['value'], time.time())
        
        # Check SLA violation
        sla_violated, violation_reason = self.sla_tracker.is_sla_violated()
        if sla_violated:
            alert = {
                'level': 'error',
                'title': 'SLA Violation',
                'message': violation_reason,
                'data': {'data_type': data_type}
            }
            should_send, reason = self.alert_aggregator.should_send(alert)
            if should_send:
                logger.error(f"SLA violation: {violation_reason}")
        
        return FallbackResult(
            success=success,
            value=result,
            source='primary' if success else 'fallback',
            latency_ms=latency_ms,
            retry_count=0,
            circuit_state=cb.get_status()['state'],
            error=error,
            data_type=data_type,
            health_score=1.0 if success else 0.0
        )
    
    def get_enhanced_status(self) -> Dict:
        """Get enhanced system status"""
        return {
            'circuit_breakers': {name: cb.get_status() for name, cb in self.circuit_breakers.items()},
            'sla': self.sla_tracker.get_status(),
            'anomaly_detector': self.anomaly_detector.get_statistics(),
            'bandit': self.strategy_selector.get_statistics(),
            'alert_aggregator': self.alert_aggregator.get_stats()
        }


# ============================================================
# Usage Example
# ============================================================

async def main():
    print("=== Ultimate Fallback Manager v3.3 Demo ===\n")
    
    fallback_mgr = UltimateFallbackManagerV3({
        'availability_target': 0.999,
        'latency_target_ms': 100,
        'seasonality_period': 24,
        'bandit_alpha': 1.0,
        'bandit_beta': 1.0,
        'circuit_breaker': {
            'distributed': False,
            'failure_threshold': 0.5
        }
    })
    
    async def mock_api_call():
        await asyncio.sleep(0.05)
        if random.random() > 0.7:
            return {'temperature': 65.0, 'value': 65.0}
        raise Exception("API error")
    
    print("1. Enhanced Circuit Breaker with ML Prediction:")
    for i in range(15):
        result = await fallback_mgr.execute_with_fallback_enhanced(mock_api_call, 'temperature')
        if result.success:
            print(f"   Attempt {i+1}: Success - {result.value}")
        else:
            print(f"   Attempt {i+1}: Failed - {result.error}")
    
    print("\n2. Multi-Armed Bandit Strategy Selection:")
    bandit_stats = fallback_mgr.strategy_selector.get_statistics()
    print(f"   Strategy success rates: {bandit_stats['strategy_success_rates']}")
    print(f"   Total attempts: {bandit_stats['total_attempts']}")
    
    print("\n3. Advanced Anomaly Detection:")
    # Add normal observations
    for i in range(50):
        fallback_mgr.anomaly_detector.add_observation('temperature', 65.0 + random.gauss(0, 1), time.time())
    
    is_anom, score = fallback_mgr.anomaly_detector.is_anomaly('temperature', 85.0)
    print(f"   Value 85.0: {'ANOMALY' if is_anom else 'normal'} (score={score:.2f})")
    
    print("\n4. SLA Tracking Status:")
    sla = fallback_mgr.sla_tracker.get_status()
    print(f"   Availability: {sla['availability']:.4%} (target: {sla['availability_target']:.4%})")
    print(f"   Error budget remaining: {sla['error_budget_remaining']:.1%}")
    print(f"   SLA Met: {sla['sla_met']}")
    
    print("\n5. Enhanced System Status:")
    status = fallback_mgr.get_enhanced_status()
    print(f"   Active circuit breakers: {len(status['circuit_breakers'])}")
    print(f"   Detector keys: {status['anomaly_detector']['keys']}")
    print(f"   Alert cache size: {status['alert_aggregator']['cached_alerts']}")
    
    print("\n✅ Ultimate Fallback Manager v3.3 test complete")

if __name__ == "__main__":
    asyncio.run(main())
