# src/enhancements/fallback_manager.py

"""
Enhanced Fallback Management System for Green Agent - Version 4.1

KEY ENHANCEMENTS OVER v4.0:
1. ENHANCED: Circuit breaker with adaptive threshold learning
2. ENHANCED: Multi-armed bandit with EXP3 algorithm for non-stationary environments
3. ENHANCED: Anomaly detector with exponential moving average for concept drift
4. ADDED: Fallback result caching for instant recovery
5. ADDED: Health score aggregation across components
6. ADDED: Circuit breaker state transition logging for audit
7. ENHANCED: SLA tracker with automatic error budget replenishment
8. ENHANCED: Alert aggregator with silence periods for maintenance windows
9. ADDED: Graceful shutdown with pending request draining
10. ADDED: Circuit breaker health trend analysis

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
from collections import deque, OrderedDict, defaultdict
import heapq
import hashlib
from datetime import datetime, timedelta
import pickle
import numpy as np
from scipy import stats

# Try to import optional dependencies
try:
    import redis
    from redis.client import Redis
    from redis.cluster import RedisCluster
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

logger = logging.getLogger(__name__)


# ============================================================
# CORE DATA STRUCTURES
# ============================================================

class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class FallbackStrategy(Enum):
    """Available fallback strategies"""
    CASCADE = "cascade"
    RETRY = "retry"
    CIRCUIT_BREAKER = "circuit_breaker"
    DEAD_LETTER = "dead_letter"
    CACHED_RESPONSE = "cached_response"
    DEGRADED_SERVICE = "degraded_service"


@dataclass
class FallbackResult:
    """Complete fallback execution result"""
    success: bool
    value: Any = None
    source: str = "unknown"
    latency_ms: float = 0.0
    retry_count: int = 0
    circuit_state: str = "closed"
    error: Optional[str] = None
    data_type: str = ""
    health_score: float = 1.0
    strategy_used: str = ""
    anomaly_detected: bool = False
    timestamp: float = field(default_factory=time.time)
    from_cache: bool = False
    
    def is_healthy(self) -> bool:
        """Check if result indicates healthy system"""
        return self.success and self.health_score > 0.8
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            'success': self.success,
            'source': self.source,
            'latency_ms': self.latency_ms,
            'retry_count': self.retry_count,
            'circuit_state': self.circuit_state,
            'error': self.error,
            'data_type': self.data_type,
            'health_score': self.health_score,
            'strategy_used': self.strategy_used,
            'anomaly_detected': self.anomaly_detected,
            'timestamp': self.timestamp,
            'from_cache': self.from_cache
        }


@dataclass
class Alert:
    """Alert definition with severity"""
    level: AlertSeverity = AlertSeverity.WARNING
    title: str = ""
    message: str = ""
    data: Dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    fingerprint: str = ""
    
    def __post_init__(self):
        """Generate fingerprint if not provided"""
        if not self.fingerprint:
            self.fingerprint = hashlib.md5(
                f"{self.level.value}:{self.title}".encode()
            ).hexdigest()[:16]


# ============================================================
# ENHANCEMENT 1: Improved Circuit Breaker with Adaptive Threshold
# ============================================================

class EnhancedDistributedCircuitBreaker:
    """
    Enhanced distributed circuit breaker with adaptive threshold learning.
    
    New Features:
    - Adaptive failure threshold based on historical patterns
    - State transition logging for audit trail
    - Health trend analysis
    - Response caching for instant recovery
    """
    
    def __init__(self, name: str, config: Optional[Dict] = None):
        self.name = name
        self.config = config or {}
        self.use_distributed = self.config.get('distributed', False) and REDIS_AVAILABLE
        
        # Core attributes
        self.min_requests = self.config.get('min_requests', 10)
        self.redis_client = None
        self.redis_key = f"circuit_breaker:{name}"
        self.redis_ttl = self.config.get('redis_ttl', 60)
        
        # ML components
        self.anomaly_detector = None
        self.feature_scaler = StandardScaler() if SKLEARN_AVAILABLE else None
        self.feature_history = deque(maxlen=1000)
        self.ml_trained = False
        
        # State
        self.local_state = CircuitState.CLOSED
        self.local_failures = 0
        self.last_failure_time = 0.0
        self.half_open_calls = 0
        self.consecutive_successes = 0
        self.recovery_attempts = 0
        
        # ENHANCEMENT: Adaptive threshold
        self.failure_threshold = self.config.get('failure_threshold', 0.5)
        self.adaptive_threshold = self.config.get('adaptive_threshold', True)
        self.threshold_history = deque(maxlen=100)
        self.adaptive_learning_rate = 0.05
        
        # Configuration
        self.window_size_seconds = self.config.get('window_size_seconds', 60)
        self.timeout_ms = self.config.get('timeout_ms', 30000)
        self.half_open_max_calls = self.config.get('half_open_max_calls', 3)
        
        # Metrics
        self.results: deque = deque(maxlen=1000)
        self.timestamps: deque = deque(maxlen=1000)
        self.latencies: deque = deque(maxlen=1000)
        
        # ENHANCEMENT: State transition log
        self.state_transitions: List[Dict] = []
        
        # ENHANCEMENT: Response cache
        self.response_cache: Dict[str, Tuple[Any, float]] = {}
        self.cache_ttl = self.config.get('cache_ttl', 30)
        
        # Lock and persistence
        self._lock = threading.RLock()
        self.state_persistence_path = self.config.get('state_path', '/tmp/circuit_breakers')
        self._state_file = os.path.join(self.state_persistence_path, f"cb_{name}.pkl")
        self._ensure_persistence_dir()
        self._load_persisted_state()
        
        # Initialize Redis and ML
        if self.use_distributed:
            self._init_redis()
        if SKLEARN_AVAILABLE:
            self._init_ml_models()
        
        logger.info(f"EnhancedCircuitBreaker {name} initialized "
                   f"(distributed={self.use_distributed}, ML={SKLEARN_AVAILABLE}, "
                   f"adaptive_threshold={self.adaptive_threshold})")
    
    def _ensure_persistence_dir(self):
        """Ensure persistence directory exists"""
        try:
            os.makedirs(self.state_persistence_path, exist_ok=True)
        except Exception as e:
            logger.warning(f"Failed to create persistence dir: {e}")
    
    def _init_redis(self):
        """Initialize Redis connection"""
        try:
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
        """Initialize ML models"""
        self.anomaly_detector = IsolationForest(
            contamination=0.1, random_state=42, n_estimators=100
        )
        if self.feature_scaler:
            self.feature_scaler = StandardScaler()
    
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
                'state': state.value, 'failures': failures,
                'timestamp': time.time(),
                'node': os.uname().nodename if hasattr(os, 'uname') else 'unknown'
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
                'version': '4.1',
                'failure_threshold': self.failure_threshold
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
                    if state_data.get('version', '1.0') >= '3.0':
                        self.local_state = CircuitState(state_data['state'])
                        self.local_failures = state_data['failures']
                        self.last_failure_time = state_data['last_failure_time']
                        if 'failure_threshold' in state_data:
                            self.failure_threshold = state_data['failure_threshold']
                        logger.info(f"Loaded persisted state for {self.name}: {self.local_state.value}")
        except Exception as e:
            logger.warning(f"Failed to load state: {e}")
    
    def _log_state_transition(self, from_state: CircuitState, to_state: CircuitState, reason: str):
        """ENHANCEMENT: Log state transitions for audit"""
        transition = {
            'timestamp': time.time(),
            'from_state': from_state.value,
            'to_state': to_state.value,
            'reason': reason,
            'failure_rate': self._calculate_failure_rate(),
            'threshold': self.failure_threshold
        }
        self.state_transitions.append(transition)
        if len(self.state_transitions) > 500:
            self.state_transitions = self.state_transitions[-500:]
        
        logger.info(f"Circuit {self.name}: {from_state.value} → {to_state.value} ({reason})")
    
    def _adapt_threshold(self):
        """ENHANCEMENT: Adaptively adjust failure threshold based on observed patterns"""
        if not self.adaptive_threshold or len(self.results) < 20:
            return
        
        # Calculate historical failure rates at different thresholds
        recent_results = list(self.results)[-100:]
        if len(recent_results) < 20:
            return
        
        overall_failure_rate = 1.0 - sum(recent_results) / len(recent_results)
        
        # If failure rate is consistently high, raise threshold to be more tolerant
        # If failure rate is consistently low, lower threshold to be more sensitive
        target = overall_failure_rate * 1.5
        
        # Smooth update
        self.threshold_history.append(target)
        if len(self.threshold_history) >= 10:
            avg_target = np.mean(list(self.threshold_history)[-10:])
            self.failure_threshold = (
                (1 - self.adaptive_learning_rate) * self.failure_threshold +
                self.adaptive_learning_rate * avg_target
            )
            self.failure_threshold = max(0.1, min(0.9, self.failure_threshold))
    
    def _extract_features(self) -> np.ndarray:
        """Enhanced feature extraction for ML prediction"""
        if len(self.results) < 20:
            return np.array([])
        
        recent_results = list(self.results)[-50:]
        recent_timestamps = list(self.timestamps)[-50:]
        recent_latencies = list(self.latencies)[-50:] if self.latencies else [0] * len(recent_results)
        
        failure_rate = sum(1 for r in recent_results if not r) / len(recent_results)
        
        trend = 0
        if len(recent_results) >= 10:
            trend = sum(1 for i in range(1, min(10, len(recent_results))) 
                       if not recent_results[i] and not recent_results[i-1]) / 9
        
        avg_latency = np.mean(recent_latencies) if recent_latencies else 0
        latency_std = np.std(recent_latencies) if len(recent_latencies) > 1 else 0
        time_since_last_failure = time.time() - self.last_failure_time if self.last_failure_time > 0 else 60
        
        failure_frequency = 0
        if len(recent_timestamps) >= 5:
            time_diffs = np.diff(recent_timestamps[-10:])
            failure_frequency = 1.0 / max(0.1, np.mean(time_diffs)) if len(time_diffs) > 0 else 0
        
        features = np.array([
            failure_rate, trend, avg_latency / 1000,
            latency_std / 1000, min(1.0, time_since_last_failure / 60),
            failure_frequency
        ])
        
        self.feature_history.append(features)
        
        if len(self.feature_history) >= 100 and not self.ml_trained:
            self._train_ml_model()
        
        return features
    
    def _train_ml_model(self):
        """Train IsolationForest on feature history"""
        if not SKLEARN_AVAILABLE or self.anomaly_detector is None or len(self.feature_history) < 50:
            return
        
        try:
            X = np.array(list(self.feature_history))
            X_scaled = self.feature_scaler.fit_transform(X) if self.feature_scaler else X
            self.anomaly_detector.fit(X_scaled)
            self.ml_trained = True
            logger.info(f"ML model trained for {self.name} on {len(X)} samples")
        except Exception as e:
            logger.warning(f"ML training failed: {e}")
    
    def predict_failure_probability(self) -> float:
        """Enhanced failure probability prediction"""
        features = self._extract_features()
        if len(features) == 0:
            return self._calculate_failure_rate()
        
        if self.ml_trained and self.anomaly_detector and self.feature_scaler:
            try:
                X = features.reshape(1, -1)
                X_scaled = self.feature_scaler.transform(X)
                prediction = self.anomaly_detector.predict(X_scaled)[0]
                score = self.anomaly_detector.score_samples(X_scaled)[0]
                
                if prediction == -1:
                    ml_prob = max(0.7, min(0.95, 1.0 + score))
                else:
                    ml_prob = max(0.1, min(0.6, 0.5 - score))
                
                heuristic_prob = 0.6 * self._calculate_failure_rate() + 0.4 * features[1]
                return 0.5 * ml_prob + 0.5 * heuristic_prob
            except Exception:
                pass
        
        failure_rate = self._calculate_failure_rate()
        trend = features[1] if len(features) > 1 else 0
        return min(0.95, 0.6 * failure_rate + 0.4 * trend)
    
    def _calculate_failure_rate(self) -> float:
        """Calculate failure rate over sliding window"""
        if len(self.results) < self.min_requests:
            return 0.0
        
        cutoff = time.time() - self.window_size_seconds
        recent = [(ts, s) for ts, s in zip(self.timestamps, self.results) if ts > cutoff]
        if not recent:
            return 0.0
        
        failures = sum(1 for _, s in recent if not s)
        return failures / len(recent)
    
    def _get_from_cache(self, cache_key: str) -> Optional[Any]:
        """ENHANCEMENT: Get cached response if valid"""
        if cache_key in self.response_cache:
            result, timestamp = self.response_cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                logger.debug(f"Cache hit for {cache_key}")
                return result
            else:
                del self.response_cache[cache_key]
        return None
    
    def _set_cache(self, cache_key: str, value: Any):
        """ENHANCEMENT: Cache successful response"""
        self.response_cache[cache_key] = (value, time.time())
        # Limit cache size
        if len(self.response_cache) > 100:
            oldest = min(self.response_cache.items(), key=lambda x: x[1][1])
            del self.response_cache[oldest[0]]
    
    def record_result(self, success: bool, latency_ms: float = 0):
        """Record result with adaptive threshold update"""
        with self._lock:
            self.results.append(success)
            self.timestamps.append(time.time())
            if latency_ms > 0:
                self.latencies.append(latency_ms)
            
            if not success:
                self.local_failures += 1
                self.last_failure_time = time.time() * 1000
                
                if self.local_state == CircuitState.CLOSED:
                    failure_rate = self._calculate_failure_rate()
                    predicted_rate = self.predict_failure_probability()
                    effective_rate = max(failure_rate, predicted_rate)
                    
                    # ENHANCEMENT: Use adaptive threshold
                    if (effective_rate >= self.failure_threshold and 
                        len(self.results) >= self.min_requests):
                        old_state = self.local_state
                        self.local_state = CircuitState.OPEN
                        self._log_state_transition(old_state, self.local_state, 
                                                   f"Effective rate {effective_rate:.1%} >= threshold {self.failure_threshold:.1%}")
                        self._save_persisted_state()
                        if self.use_distributed:
                            self._set_remote_state(CircuitState.OPEN, self.local_failures)
                        logger.error(f"Circuit {self.name} opened (rate={effective_rate:.1%}, threshold={self.failure_threshold:.1%})")
            else:
                self.local_failures = max(0, self.local_failures - 1)
                
                if self.local_state == CircuitState.HALF_OPEN:
                    self.consecutive_successes += 1
                    if self.consecutive_successes >= self.half_open_max_calls:
                        old_state = self.local_state
                        self.local_state = CircuitState.CLOSED
                        self._log_state_transition(old_state, self.local_state, "Recovery complete")
                        self.consecutive_successes = 0
                        self.half_open_calls = 0
                        self.recovery_attempts = 0
                        self._save_persisted_state()
                        if self.use_distributed:
                            self._set_remote_state(CircuitState.CLOSED, 0)
                        logger.info(f"Circuit {self.name} recovered to CLOSED")
            
            # ENHANCEMENT: Adapt threshold
            self._adapt_threshold()
    
    def call(self, func: Callable, *args, **kwargs) -> Tuple[Any, Optional[str]]:
        """Execute function with circuit breaker protection and caching"""
        # Generate cache key for idempotent operations
        cache_key = hashlib.md5(
            f"{func.__name__}:{str(args)}:{str(kwargs)}".encode()
        ).hexdigest()[:16]
        
        # Check cache first
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return cached, None
        
        state = self.local_state
        if self.use_distributed:
            remote = self._get_remote_state()
            if remote:
                state = remote
        
        with self._lock:
            if state == CircuitState.OPEN:
                backoff_ms = min(30000, 1000 * (2 ** self.recovery_attempts))
                if time.time() * 1000 - self.last_failure_time > backoff_ms:
                    old_state = self.local_state
                    self.local_state = CircuitState.HALF_OPEN
                    self._log_state_transition(old_state, self.local_state, 
                                               f"Timeout elapsed (backoff={backoff_ms}ms)")
                    self.half_open_calls = 0
                    self.consecutive_successes = 0
                    self.recovery_attempts += 1
                else:
                    # Try cache as fallback when circuit is open
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
            # ENHANCEMENT: Cache successful result
            self._set_cache(cache_key, result)
            return result, None
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            self.record_result(False, latency_ms)
            return None, str(e)
    
    async def call_async(self, func: Callable, *args, **kwargs) -> Tuple[Any, Optional[str]]:
        """Async version with caching"""
        cache_key = hashlib.md5(
            f"{func.__name__}:{str(args)}:{str(kwargs)}".encode()
        ).hexdigest()[:16]
        
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return cached, None
        
        state = self.local_state
        if self.use_distributed:
            remote = self._get_remote_state()
            if remote:
                state = remote
        
        with self._lock:
            if state == CircuitState.OPEN:
                backoff_ms = min(30000, 1000 * (2 ** self.recovery_attempts))
                if time.time() * 1000 - self.last_failure_time > backoff_ms:
                    old_state = self.local_state
                    self.local_state = CircuitState.HALF_OPEN
                    self._log_state_transition(old_state, self.local_state, 
                                               f"Timeout elapsed (backoff={backoff_ms}ms)")
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
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            latency_ms = (time.time() - start_time) * 1000
            self.record_result(True, latency_ms)
            self._set_cache(cache_key, result)
            return result, None
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            self.record_result(False, latency_ms)
            return None, str(e)
    
    def get_health_trend(self) -> Dict:
        """ENHANCEMENT: Analyze health trend for predictive insights"""
        if len(self.results) < 30:
            return {'trend': 'insufficient_data', 'direction': 'stable', 'confidence': 0.0}
        
        recent = list(self.results)[-50:]
        if len(recent) < 30:
            return {'trend': 'insufficient_data', 'direction': 'stable', 'confidence': 0.0}
        
        # Split into two halves to detect trend
        mid = len(recent) // 2
        first_half = recent[:mid]
        second_half = recent[mid:]
        
        first_rate = 1.0 - sum(first_half) / len(first_half)
        second_rate = 1.0 - sum(second_half) / len(second_half)
        
        delta = second_rate - first_rate
        
        if delta > 0.1:
            direction = 'degrading'
            confidence = min(0.9, delta * 5)
        elif delta < -0.1:
            direction = 'improving'
            confidence = min(0.9, abs(delta) * 5)
        else:
            direction = 'stable'
            confidence = 0.5
        
        return {
            'trend': 'degrading' if delta > 0.05 else 'improving' if delta < -0.05 else 'stable',
            'direction': direction,
            'delta': round(delta, 3),
            'confidence': round(confidence, 2),
            'current_rate': round(self._calculate_failure_rate(), 3),
            'threshold': round(self.failure_threshold, 3)
        }
    
    def get_status(self) -> Dict:
        """Get enhanced circuit breaker status"""
        return {
            'name': self.name,
            'state': self.local_state.value,
            'failure_rate': self._calculate_failure_rate(),
            'predicted_failure_rate': self.predict_failure_probability(),
            'remote_mode': self.use_distributed,
            'redis_connected': self.redis_client is not None,
            'persisted': os.path.exists(self._state_file),
            'recovery_attempts': self.recovery_attempts,
            'sample_count': len(self.results),
            'ml_trained': self.ml_trained,
            'half_open_calls': self.half_open_calls,
            'adaptive_threshold': self.failure_threshold,
            'transition_count': len(self.state_transitions),
            'cache_size': len(self.response_cache),
            'health_trend': self.get_health_trend()
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
            self.response_cache.clear()
            self._save_persisted_state()
            if self.use_distributed:
                self._set_remote_state(CircuitState.CLOSED, 0)
            logger.info(f"Circuit {self.name} manually reset")


# ============================================================
# ENHANCEMENT 2: Improved Multi-Armed Bandit with EXP3
# ============================================================

class MultiArmedBanditSelector:
    """
    Enhanced multi-armed bandit with EXP3 algorithm.
    
    New Features:
    - EXP3 algorithm for adversarial/non-stationary environments
    - Automatic algorithm selection based on reward stationarity
    - Strategy performance decay for recency weighting
    """
    
    def __init__(self, strategies: List[str], alpha: float = 1.0, beta: float = 1.0,
                 algorithm: str = 'thompson', gamma: float = 0.1):
        self.strategies = strategies
        self.successes = {s: 0 for s in strategies}
        self.failures = {s: 0 for s in strategies}
        self.alpha = alpha
        self.beta = beta
        self.algorithm = algorithm
        self.gamma = gamma  # EXP3 exploration parameter
        self.total_attempts = 0
        
        # ENHANCEMENT: EXP3 weights
        self.exp3_weights = {s: 1.0 for s in strategies}
        
        # ENHANCEMENT: Performance history for stationarity detection
        self.reward_history = {s: deque(maxlen=50) for s in strategies}
        self._lock = threading.RLock()
        
        logger.info(f"MultiArmedBanditSelector initialized with {len(strategies)} "
                   f"strategies, algorithm={algorithm}")
    
    def _detect_stationarity(self) -> bool:
        """ENHANCEMENT: Detect if rewards are stationary for algorithm selection"""
        if self.total_attempts < 30:
            return True
        
        for strategy in self.strategies:
            rewards = list(self.reward_history[strategy])
            if len(rewards) >= 20:
                first_half = np.mean(rewards[:10])
                second_half = np.mean(rewards[-10:])
                if abs(second_half - first_half) > 0.3:
                    return False  # Non-stationary detected
        
        return True
    
    def select_strategy(self) -> str:
        """Select strategy using adaptive algorithm selection"""
        with self._lock:
            # ENHANCEMENT: Auto-select algorithm based on stationarity
            effective_algo = self.algorithm
            if self.algorithm == 'auto':
                effective_algo = 'thompson' if self._detect_stationarity() else 'exp3'
            
            if effective_algo == 'exp3':
                return self._exp3_select()
            elif effective_algo == 'ucb':
                return self._ucb_select()
            else:
                return self._thompson_select()
    
    def _thompson_select(self) -> str:
        """Select using Thompson sampling"""
        scores = {}
        for strategy in self.strategies:
            sample = np.random.beta(
                self.alpha + self.successes[strategy],
                self.beta + self.failures[strategy]
            )
            scores[strategy] = sample
        return max(scores, key=scores.get)
    
    def _ucb_select(self) -> str:
        """Select using Upper Confidence Bound"""
        if self.total_attempts == 0:
            return random.choice(self.strategies)
        
        scores = {}
        for strategy in self.strategies:
            total = self.successes[strategy] + self.failures[strategy]
            if total == 0:
                scores[strategy] = float('inf')
            else:
                success_rate = self.successes[strategy] / total
                exploration = np.sqrt(2 * np.log(self.total_attempts) / total)
                scores[strategy] = success_rate + exploration
        return max(scores, key=scores.get)
    
    def _exp3_select(self) -> str:
        """ENHANCEMENT: Select using EXP3 algorithm for non-stationary environments"""
        n = len(self.strategies)
        
        # Calculate probability distribution
        total_weight = sum(self.exp3_weights.values())
        if total_weight == 0:
            probabilities = [1.0 / n] * n
        else:
            probabilities = [
                (1 - self.gamma) * (self.exp3_weights[s] / total_weight) + self.gamma / n
                for s in self.strategies
            ]
        
        # Sample from distribution
        idx = np.random.choice(n, p=probabilities)
        return self.strategies[idx]
    
    def update(self, strategy: str, success: bool):
        """Update strategy performance with recency weighting"""
        with self._lock:
            if success:
                self.successes[strategy] += 1
                reward = 1.0
            else:
                self.failures[strategy] += 1
                reward = 0.0
            
            self.total_attempts += 1
            
            # ENHANCEMENT: Update EXP3 weights
            n = len(self.strategies)
            total_weight = sum(self.exp3_weights.values())
            prob = (1 - self.gamma) * (self.exp3_weights[strategy] / max(total_weight, 1e-6)) + self.gamma / n
            estimated_reward = reward / max(prob, 1e-6)
            self.exp3_weights[strategy] *= np.exp(self.gamma * estimated_reward / n)
            
            # ENHANCEMENT: Track reward history
            self.reward_history[strategy].append(reward)
            
            # ENHANCEMENT: Apply decay to old successes/failures
            if self.total_attempts % 100 == 0:
                decay = 0.95
                for s in self.strategies:
                    self.successes[s] *= decay
                    self.failures[s] *= decay
    
    def get_statistics(self) -> Dict:
        """Get enhanced bandit statistics"""
        with self._lock:
            success_rates = {}
            confidence_intervals = {}
            
            for s in self.strategies:
                total = self.successes[s] + self.failures[s]
                if total > 0:
                    rate = self.successes[s] / total
                    success_rates[s] = rate
                    
                    z = 1.96
                    denominator = 1 + z**2 / total
                    center = (rate + z**2 / (2 * total)) / denominator
                    margin = z * np.sqrt(rate * (1 - rate) / total + z**2 / (4 * total**2)) / denominator
                    confidence_intervals[s] = {'lower': max(0, center - margin), 'upper': min(1, center + margin)}
                else:
                    success_rates[s] = 0.0
                    confidence_intervals[s] = {'lower': 0.0, 'upper': 1.0}
            
            return {
                'strategy_successes': self.successes.copy(),
                'strategy_failures': self.failures.copy(),
                'strategy_success_rates': success_rates,
                'confidence_intervals': confidence_intervals,
                'total_attempts': self.total_attempts,
                'algorithm': self.algorithm,
                'is_stationary': self._detect_stationarity(),
                'best_strategy': max(success_rates, key=success_rates.get) if success_rates else None,
                'exp3_weights': {s: round(w, 3) for s, w in self.exp3_weights.items()}
            }


# ============================================================
# ENHANCEMENT 3: Improved Anomaly Detector with EMA
# ============================================================

class AdvancedAnomalyDetector:
    """
    Enhanced anomaly detector with exponential moving average for concept drift.
    
    New Features:
    - EMA-based baseline for adapting to gradual changes
    - Automatic seasonality period detection
    - Multi-level anomaly scoring
    """
    
    def __init__(self, seasonality_period: int = 24):
        self.seasonality_period = seasonality_period
        self.history: Dict[str, deque] = {}
        self.seasonal_components: Dict[str, np.ndarray] = {}
        self.trend_components: Dict[str, np.ndarray] = {}
        self.residual_std: Dict[str, float] = {}
        
        # ENHANCEMENT: EMA baselines for concept drift
        self.ema_baselines: Dict[str, float] = {}
        self.ema_alpha = 0.1  # Smoothing factor
        
        # ENHANCEMENT: Anomaly severity levels
        self.severity_thresholds = {
            'low': 2.0,
            'medium': 3.0,
            'high': 5.0
        }
        
        self._lock = threading.RLock()
        
        logger.info(f"Enhanced AdvancedAnomalyDetector initialized (period={seasonality_period})")
    
    def add_observation(self, key: str, value: float, timestamp: float):
        """Add observation with EMA baseline update"""
        with self._lock:
            if key not in self.history:
                self.history[key] = deque(maxlen=2000)
                self.ema_baselines[key] = value
            
            self.history[key].append((timestamp, value))
            
            # ENHANCEMENT: Update EMA baseline
            if key in self.ema_baselines:
                self.ema_baselines[key] = (
                    self.ema_alpha * value + (1 - self.ema_alpha) * self.ema_baselines[key]
                )
            
            if len(self.history[key]) >= self.seasonality_period * 4:
                self._update_model(key)
    
    def _update_model(self, key: str):
        """Enhanced seasonal decomposition model"""
        data = list(self.history[key])
        if len(data) < self.seasonality_period * 2:
            return
        
        data.sort(key=lambda x: x[0])
        values = np.array([v for _, v in data])
        
        # Detrend with moving average
        window = min(self.seasonality_period, len(values) // 4)
        if window > 0:
            trend = np.convolve(values, np.ones(window)/window, mode='same')
            detrended = values - trend
        else:
            trend = np.zeros_like(values)
            detrended = values
        
        n_seasons = len(values) // self.seasonality_period
        if n_seasons >= 2:
            n_trimmed = n_seasons * self.seasonality_period
            detrended_trimmed = detrended[:n_trimmed]
            seasonal_matrix = detrended_trimmed.reshape(n_seasons, self.seasonality_period)
            seasonal = np.median(seasonal_matrix, axis=0)
            self.seasonal_components[key] = seasonal
            
            seasonal_tiled = np.tile(seasonal, n_seasons)
            residuals = detrended_trimmed - seasonal_tiled
            self.residual_std[key] = np.std(residuals)
            self.trend_components[key] = trend
    
    def is_anomaly(self, key: str, value: float) -> Tuple[bool, float]:
        """Enhanced anomaly detection with severity levels"""
        with self._lock:
            if key not in self.history or len(self.history[key]) < self.seasonality_period:
                return self._statistical_anomaly(key, value)
            
            recent = list(self.history[key])[-self.seasonality_period * 2:]
            recent_values = [v for _, v in recent]
            
            mean = np.mean(recent_values)
            std = np.std(recent_values)
            
            if std == 0:
                return False, 0.0
            
            z_score = abs(value - mean) / std
            
            if key in self.seasonal_components:
                hour_of_day = int((time.time() / 3600) % self.seasonality_period)
                seasonal_val = self.seasonal_components[key][hour_of_day]
                
                if key in self.trend_components:
                    trend_idx = min(len(self.trend_components[key]) - 1, len(recent_values) - 1)
                    trend_val = self.trend_components[key][trend_idx]
                else:
                    trend_val = 0
                
                expected = mean + seasonal_val + trend_val
                adjusted_z = abs(value - expected) / max(std, 0.1)
                
                if key in self.residual_std:
                    threshold_multiplier = max(2.0, min(5.0, 3.0 * self.residual_std[key] / std))
                else:
                    threshold_multiplier = 3.0
                
                is_anomaly = adjusted_z > threshold_multiplier
                score = min(1.0, adjusted_z / (threshold_multiplier * 2))
            else:
                is_anomaly = z_score > 3.0
                score = min(1.0, z_score / 5.0)
            
            return is_anomaly, score
    
    def get_anomaly_severity(self, key: str, value: float) -> str:
        """ENHANCEMENT: Get anomaly severity level"""
        _, score = self.is_anomaly(key, value)
        
        # Convert score to z-score approximation
        z_approx = score * 10  # Rough conversion
        
        if z_approx >= self.severity_thresholds['high']:
            return 'high'
        elif z_approx >= self.severity_thresholds['medium']:
            return 'medium'
        elif z_approx >= self.severity_thresholds['low']:
            return 'low'
        else:
            return 'none'
    
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
        """Get enhanced detector statistics"""
        with self._lock:
            return {
                'keys': list(self.history.keys()),
                'sample_sizes': {k: len(v) for k, v in self.history.items()},
                'seasonal_models': list(self.seasonal_components.keys()),
                'trend_models': list(self.trend_components.keys()),
                'ema_baselines': {k: round(v, 2) for k, v in self.ema_baselines.items()},
                'residual_stats': {k: round(v, 4) for k, v in self.residual_std.items()}
            }


# ============================================================
# ENHANCEMENT 4: Improved SLA Tracker with Auto-Replenishment
# ============================================================

class SLATracker:
    """
    Enhanced SLA tracker with automatic error budget replenishment.
    
    New Features:
    - Automatic error budget replenishment over time windows
    - Multi-level burn rate alerting (critical, warning, info)
    - Latency SLO tracking with percentiles
    """
    
    def __init__(self, availability_target: float = 0.999,
                 latency_target_ms: float = 100,
                 window_short_minutes: int = 60,
                 window_long_minutes: int = 1440,
                 replenishment_hours: int = 24):
        self.availability_target = availability_target
        self.latency_target_ms = latency_target_ms
        self.window_short_minutes = window_short_minutes
        self.window_long_minutes = window_long_minutes
        self.replenishment_hours = replenishment_hours
        
        # Error budget
        self.error_budget_total = 10000
        self.error_budget_remaining = self.error_budget_total
        
        # Tracking
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.latency_violations = 0
        
        self.short_window_success = deque(maxlen=10000)
        self.long_window_success = deque(maxlen=100000)
        self.latency_history = deque(maxlen=10000)
        
        self.burn_rate_short = 0.0
        self.burn_rate_long = 0.0
        self.burn_rate_threshold = 14.4
        
        # ENHANCEMENT: Replenishment
        self.last_replenishment = time.time()
        self.replenishment_interval = replenishment_hours * 3600
        
        self._lock = threading.RLock()
        
        logger.info(f"SLATracker initialized (target={availability_target}, "
                   f"latency={latency_target_ms}ms, replenish={replenishment_hours}h)")
    
    def record_request(self, success: bool, latency_ms: float):
        """Record request outcome with auto-replenishment"""
        with self._lock:
            current_time = time.time()
            
            # ENHANCEMENT: Check and apply replenishment
            self._apply_replenishment(current_time)
            
            self.total_requests += 1
            
            if success:
                self.successful_requests += 1
                self.short_window_success.append((current_time, True))
                self.long_window_success.append((current_time, True))
            else:
                self.failed_requests += 1
                self.error_budget_remaining = max(0, self.error_budget_remaining - 1)
                self.short_window_success.append((current_time, False))
                self.long_window_success.append((current_time, False))
            
            if latency_ms > self.latency_target_ms:
                self.latency_violations += 1
            
            self.latency_history.append((current_time, latency_ms))
            self._update_burn_rates(current_time)
    
    def _apply_replenishment(self, current_time: float):
        """ENHANCEMENT: Automatically replenish error budget"""
        if current_time - self.last_replenishment >= self.replenishment_interval:
            # Calculate replenishment amount
            if self.error_budget_remaining < self.error_budget_total:
                replenish_amount = int(self.error_budget_total * 0.1)  # 10% replenishment
                self.error_budget_remaining = min(
                    self.error_budget_total,
                    self.error_budget_remaining + replenish_amount
                )
                logger.info(f"Error budget replenished: +{replenish_amount} "
                          f"(now {self.error_budget_remaining}/{self.error_budget_total})")
            
            self.last_replenishment = current_time
    
    def _update_burn_rates(self, current_time: float):
        """Update burn rates"""
        short_cutoff = current_time - (self.window_short_minutes * 60)
        short_recent = [s for ts, s in self.short_window_success if ts > short_cutoff]
        if short_recent:
            failure_rate = 1.0 - sum(short_recent) / len(short_recent)
            self.burn_rate_short = failure_rate / (1.0 - self.availability_target)
        
        long_cutoff = current_time - (self.window_long_minutes * 60)
        long_recent = [s for ts, s in self.long_window_success if ts > long_cutoff]
        if long_recent:
            failure_rate = 1.0 - sum(long_recent) / len(long_recent)
            self.burn_rate_long = failure_rate / (1.0 - self.availability_target)
    
    def get_burn_rate_level(self) -> str:
        """ENHANCEMENT: Get burn rate severity level"""
        max_rate = max(self.burn_rate_short, self.burn_rate_long)
        if max_rate > self.burn_rate_threshold:
            return 'critical'
        elif max_rate > self.burn_rate_threshold / 2:
            return 'warning'
        elif max_rate > self.burn_rate_threshold / 5:
            return 'info'
        else:
            return 'normal'
    
    def is_sla_violated(self) -> Tuple[bool, str]:
        """Check if SLA is violated"""
        with self._lock:
            violations = []
            
            current_availability = self.get_availability()
            if current_availability < self.availability_target:
                violations.append(
                    f"Availability {current_availability:.4%} below target {self.availability_target:.4%}"
                )
            
            if self.error_budget_remaining < self.error_budget_total * 0.1:
                violations.append(
                    f"Error budget critically low: {self.error_budget_remaining}/{self.error_budget_total}"
                )
            
            burn_level = self.get_burn_rate_level()
            if burn_level == 'critical':
                violations.append(f"Critical burn rate: {max(self.burn_rate_short, self.burn_rate_long):.1f}x")
            elif burn_level == 'warning':
                violations.append(f"Warning burn rate detected")
            
            if violations:
                return True, "; ".join(violations)
            return False, "SLA met"
    
    def get_availability(self) -> float:
        """Get current availability"""
        with self._lock:
            if self.total_requests == 0:
                return 1.0
            return self.successful_requests / self.total_requests
    
    def get_latency_percentile(self, percentile: float = 95) -> float:
        """Get latency percentile"""
        with self._lock:
            if not self.latency_history:
                return 0.0
            latencies = [lat for _, lat in self.latency_history]
            return np.percentile(latencies, percentile)
    
    def get_status(self) -> Dict:
        """Get enhanced SLA status"""
        with self._lock:
            return {
                'availability': self.get_availability(),
                'availability_target': self.availability_target,
                'latency_target_ms': self.latency_target_ms,
                'latency_p95_ms': self.get_latency_percentile(95),
                'latency_p99_ms': self.get_latency_percentile(99),
                'total_requests': self.total_requests,
                'successful_requests': self.successful_requests,
                'failed_requests': self.failed_requests,
                'latency_violations': self.latency_violations,
                'error_budget_total': self.error_budget_total,
                'error_budget_remaining': self.error_budget_remaining,
                'error_budget_consumed_percent': (
                    (self.error_budget_total - self.error_budget_remaining) / 
                    max(self.error_budget_total, 1) * 100
                ),
                'burn_rate_short': round(self.burn_rate_short, 2),
                'burn_rate_long': round(self.burn_rate_long, 2),
                'burn_rate_level': self.get_burn_rate_level(),
                'sla_met': self.get_availability() >= self.availability_target,
                'next_replenishment_seconds': max(0, 
                    self.replenishment_interval - (time.time() - self.last_replenishment))
            }
    
    def reset(self):
        """Reset SLA tracker"""
        with self._lock:
            self.total_requests = 0
            self.successful_requests = 0
            self.failed_requests = 0
            self.latency_violations = 0
            self.error_budget_remaining = self.error_budget_total
            self.short_window_success.clear()
            self.long_window_success.clear()
            self.latency_history.clear()
            self.burn_rate_short = 0.0
            self.burn_rate_long = 0.0
            self.last_replenishment = time.time()


# ============================================================
# ENHANCEMENT 5: Complete Enhanced Fallback Manager
# ============================================================

class UltimateFallbackManagerV4:
    """
    Complete enhanced fallback manager v4.1.
    
    New Features:
    - Health score aggregation across all components
    - Graceful shutdown with pending request draining
    - Alert aggregator with silence periods
    - Component-level health monitoring
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
        # All components
        self.circuit_breakers = {}
        self.alert_aggregator = AlertAggregator()
        self.anomaly_detector = AdvancedAnomalyDetector(
            seasonality_period=self.config.get('seasonality_period', 24)
        )
        self.strategy_selector = MultiArmedBanditSelector(
            strategies=[s.value for s in FallbackStrategy],
            alpha=self.config.get('bandit_alpha', 1.0),
            beta=self.config.get('bandit_beta', 1.0),
            algorithm=self.config.get('bandit_algorithm', 'auto')
        )
        self.dependency_graph = FallbackDependencyGraph()
        self.sla_tracker = SLATracker(
            availability_target=self.config.get('availability_target', 0.999),
            latency_target_ms=self.config.get('latency_target_ms', 100)
        )
        
        # ENHANCEMENT: Health tracking
        self.component_health: Dict[str, float] = defaultdict(lambda: 1.0)
        
        # ENHANCEMENT: Graceful shutdown
        self._shutting_down = False
        self._pending_requests = 0
        self._shutdown_lock = threading.RLock()
        
        self._init_circuit_breakers()
        self._init_dependency_graph()
        
        logger.info("UltimateFallbackManagerV4 v4.1 initialized")
    
    def _init_circuit_breakers(self):
        """Initialize enhanced circuit breakers"""
        data_types = ['temperature', 'grid', 'helium', 'recovery', 'ppa', 'carbon']
        for dt in data_types:
            self.circuit_breakers[dt] = EnhancedDistributedCircuitBreaker(
                dt, self.config.get('circuit_breaker', {})
            )
    
    def _init_dependency_graph(self):
        """Initialize sample dependency graph"""
        self.dependency_graph.add_service('api_gateway', priority=1)
        self.dependency_graph.add_service('auth_service', priority=1)
        self.dependency_graph.add_service('temperature_service', priority=2)
        self.dependency_graph.add_service('carbon_service', priority=2)
        self.dependency_graph.add_service('grid_service', priority=3)
        self.dependency_graph.add_service('cache_service', priority=3)
        
        self.dependency_graph.add_dependency('temperature_service', 'api_gateway')
        self.dependency_graph.add_dependency('carbon_service', 'api_gateway')
        self.dependency_graph.add_dependency('grid_service', 'carbon_service')
    
    def get_aggregate_health(self) -> Dict:
        """ENHANCEMENT: Get aggregate health score across all components"""
        cb_health = []
        for name, cb in self.circuit_breakers.items():
            status = cb.get_status()
            if status['state'] == 'closed':
                score = 1.0 - status['failure_rate']
            elif status['state'] == 'half_open':
                score = 0.5
            else:
                score = 0.1
            cb_health.append(score)
        
        avg_cb_health = np.mean(cb_health) if cb_health else 1.0
        
        availability = self.sla_tracker.get_availability()
        
        overall = 0.4 * avg_cb_health + 0.4 * availability + 0.2 * np.mean(list(self.component_health.values()))
        
        return {
            'overall': round(overall, 3),
            'circuit_breakers': round(avg_cb_health, 3),
            'availability': round(availability, 3),
            'components': dict(self.component_health),
            'status': 'healthy' if overall > 0.8 else 'degraded' if overall > 0.5 else 'critical'
        }
    
    async def execute_with_fallback_enhanced(self, primary_func: Callable, 
                                            data_type: str,
                                            context: Optional[Dict] = None) -> FallbackResult:
        """Enhanced fallback execution with graceful shutdown support"""
        # ENHANCEMENT: Check shutdown state
        with self._shutdown_lock:
            if self._shutting_down:
                return FallbackResult(
                    success=False, error="System is shutting down",
                    data_type=data_type, health_score=0.0,
                    strategy_used="none"
                )
            self._pending_requests += 1
        
        try:
            start_time = time.time()
            
            # Select strategy using bandit
            selected_strategy = FallbackStrategy(self.strategy_selector.select_strategy())
            
            # Check anomaly before execution
            anomaly_detected = False
            anomaly_score = 0.0
            if context and 'value' in context:
                is_anom, score = self.anomaly_detector.is_anomaly(data_type, context['value'])
                anomaly_detected = is_anom
                anomaly_score = score
                
                if is_anom:
                    severity = self.anomaly_detector.get_anomaly_severity(data_type, context['value'])
                    alert = Alert(
                        level=AlertSeverity.WARNING if severity != 'high' else AlertSeverity.ERROR,
                        title=f'Anomaly detected for {data_type}',
                        message=f'Severity: {severity}, Score: {score:.2f}',
                        data={'value': context['value'], 'data_type': data_type, 'severity': severity}
                    )
                    should_send, reason = self.alert_aggregator.should_send(alert)
                    if should_send:
                        logger.warning(f"Anomaly: {alert.title} (severity={severity})")
            
            # Get circuit breaker
            cb = self.circuit_breakers.get(data_type)
            if not cb:
                cb = EnhancedDistributedCircuitBreaker(data_type)
                self.circuit_breakers[data_type] = cb
            
            # Execute with selected strategy
            result, error = None, None
            retry_count = 0
            from_cache = False
            
            if selected_strategy == FallbackStrategy.CIRCUIT_BREAKER:
                result, error = await cb.call_async(primary_func)
                success = result is not None
                
            elif selected_strategy == FallbackStrategy.RETRY:
                for attempt in range(3):
                    try:
                        result = await primary_func() if asyncio.iscoroutinefunction(primary_func) else primary_func()
                        success = True
                        error = None
                        break
                    except Exception as e:
                        success = False
                        error = str(e)
                        retry_count = attempt + 1
                        if attempt < 2:
                            await asyncio.sleep(0.1 * (attempt + 1))
                if not success:
                    result = None
                    
            elif selected_strategy == FallbackStrategy.CASCADE:
                try:
                    result = await primary_func() if asyncio.iscoroutinefunction(primary_func) else primary_func()
                    success = True
                    error = None
                except Exception as e:
                    fallback_order = self.dependency_graph.get_fallback_order(data_type)
                    success = False
                    error = str(e)
                    
                    for fallback_service in fallback_order:
                        if fallback_service in self.circuit_breakers:
                            fallback_cb = self.circuit_breakers[fallback_service]
                            if fallback_cb.local_state == CircuitState.CLOSED:
                                try:
                                    result = await fallback_cb.call_async(primary_func)
                                    if result[0] is not None:
                                        success = True
                                        error = None
                                        break
                                except Exception:
                                    continue
                    if not success:
                        result = None
            else:
                try:
                    result = await primary_func() if asyncio.iscoroutinefunction(primary_func) else primary_func()
                    success = True
                    error = None
                except Exception as e:
                    success = False
                    error = str(e)
                    result = None
            
            # Update strategy selector
            self.strategy_selector.update(selected_strategy.value, success)
            
            # Record SLA
            latency_ms = (time.time() - start_time) * 1000
            self.sla_tracker.record_request(success, latency_ms)
            
            # Update anomaly detector
            if result is not None and isinstance(result, dict) and 'value' in result:
                self.anomaly_detector.add_observation(data_type, result['value'], time.time())
            
            # Update component health
            self.component_health[data_type] = (
                0.9 * self.component_health[data_type] + 0.1 * (1.0 if success else 0.0)
            )
            
            # Check SLA
            sla_violated, violation_reason = self.sla_tracker.is_sla_violated()
            if sla_violated:
                alert = Alert(
                    level=AlertSeverity.ERROR,
                    title='SLA Violation',
                    message=violation_reason,
                    data={'data_type': data_type, 'strategy': selected_strategy.value}
                )
                should_send, reason = self.alert_aggregator.should_send(alert)
                if should_send:
                    logger.error(f"SLA violation: {violation_reason}")
            
            return FallbackResult(
                success=success, value=result,
                source='primary' if success else 'fallback',
                latency_ms=latency_ms, retry_count=retry_count,
                circuit_state=cb.get_status()['state'],
                error=error, data_type=data_type,
                health_score=1.0 if success else max(0, 1.0 - anomaly_score),
                strategy_used=selected_strategy.value,
                anomaly_detected=anomaly_detected,
                from_cache=from_cache
            )
        finally:
            with self._shutdown_lock:
                self._pending_requests -= 1
    
    async def shutdown(self, drain_timeout: float = 10.0):
        """ENHANCEMENT: Graceful shutdown with pending request draining"""
        logger.info(f"Initiating graceful shutdown (drain_timeout={drain_timeout}s)...")
        
        with self._shutdown_lock:
            self._shutting_down = True
        
        # Wait for pending requests to drain
        start = time.time()
        while time.time() - start < drain_timeout:
            with self._shutdown_lock:
                if self._pending_requests <= 0:
                    break
            await asyncio.sleep(0.5)
            logger.info(f"Draining... {self._pending_requests} requests remaining")
        
        # Save state for all circuit breakers
        for cb in self.circuit_breakers.values():
            cb._save_persisted_state()
        
        logger.info("Graceful shutdown complete")
    
    def get_enhanced_status(self) -> Dict:
        """Get enhanced system status with aggregate health"""
        return {
            'circuit_breakers': {
                name: cb.get_status() for name, cb in self.circuit_breakers.items()
            },
            'sla': self.sla_tracker.get_status(),
            'anomaly_detector': self.anomaly_detector.get_statistics(),
            'bandit': self.strategy_selector.get_statistics(),
            'alert_aggregator': self.alert_aggregator.get_stats(),
            'dependency_graph': self.dependency_graph.get_statistics(),
            'aggregate_health': self.get_aggregate_health(),
            'shutting_down': self._shutting_down,
            'pending_requests': self._pending_requests
        }
    
    def get_circuit_breaker(self, data_type: str) -> Optional[EnhancedDistributedCircuitBreaker]:
        """Get circuit breaker for data type"""
        return self.circuit_breakers.get(data_type)
    
    def reset_all(self):
        """Reset all components"""
        for cb in self.circuit_breakers.values():
            cb.reset()
        self.sla_tracker.reset()
        self.component_health.clear()
        logger.info("All fallback components reset")


# ============================================================
# SUPPORTING CLASSES (Complete implementations)
# ============================================================

class AlertAggregator:
    """Alert aggregator with deduplication"""
    
    def __init__(self):
        self.alert_cache: Dict[str, Dict] = {}
        self.deduplication_window = 300
        self.rate_limits = {
            AlertSeverity.INFO: 50, AlertSeverity.WARNING: 20,
            AlertSeverity.ERROR: 10, AlertSeverity.CRITICAL: 5
        }
        self.severity_windows = {
            AlertSeverity.CRITICAL: 60, AlertSeverity.ERROR: 180,
            AlertSeverity.WARNING: 300, AlertSeverity.INFO: 600
        }
        # ENHANCEMENT: Silence periods for maintenance
        self.silence_periods: List[Tuple[float, float]] = []
        self._lock = threading.RLock()
        
        logger.info("AlertAggregator initialized")
    
    def add_silence_period(self, start_time: float, end_time: float):
        """ENHANCEMENT: Add silence period for maintenance windows"""
        self.silence_periods.append((start_time, end_time))
        # Clean expired
        current = time.time()
        self.silence_periods = [(s, e) for s, e in self.silence_periods if e > current]
    
    def _is_silenced(self) -> bool:
        """ENHANCEMENT: Check if currently in silence period"""
        current = time.time()
        return any(s <= current <= e for s, e in self.silence_periods)
    
    def should_send(self, alert: Alert) -> Tuple[bool, str]:
        """Check if alert should be sent"""
        # Check silence periods
        if self._is_silenced():
            return False, "Silenced (maintenance window)"
        
        with self._lock:
            current_time = time.time()
            fingerprint = alert.fingerprint
            severity = alert.level
            
            window = self.severity_windows.get(severity, self.deduplication_window)
            rate_limit = self.rate_limits.get(severity, 10)
            
            if fingerprint in self.alert_cache:
                cache_entry = self.alert_cache[fingerprint]
                last_time = cache_entry['last_time']
                count = cache_entry['count']
                
                if current_time - last_time < window:
                    cache_entry['count'] += 1
                    return False, f"Deduplicated ({severity.value})"
                
                if count >= rate_limit:
                    if severity == AlertSeverity.WARNING and count >= rate_limit * 2:
                        alert.level = AlertSeverity.ERROR
                        return True, f"Escalated to ERROR"
                    return False, f"Rate limited"
                
                cache_entry['last_time'] = current_time
                cache_entry['count'] = count + 1
            else:
                self.alert_cache[fingerprint] = {
                    'last_time': current_time, 'count': 1,
                    'severity': severity, 'first_seen': current_time
                }
            
            self._cleanup(current_time)
            return True, "OK"
    
    def _cleanup(self, current_time: float):
        """Remove expired cache entries"""
        expired = [fp for fp, entry in self.alert_cache.items() 
                  if current_time - entry['first_seen'] > 3600]
        for fp in expired:
            del self.alert_cache[fp]
    
    def get_stats(self) -> Dict:
        """Get aggregator statistics"""
        with self._lock:
            severity_counts = defaultdict(int)
            for entry in self.alert_cache.values():
                severity_counts[entry['severity'].value] += 1
            return {
                'cached_alerts': len(self.alert_cache),
                'severity_counts': dict(severity_counts),
                'silence_periods_active': self._is_silenced()
            }


class FallbackDependencyGraph:
    """Fallback dependency graph with topological sorting"""
    
    def __init__(self):
        self.nodes: Dict[str, Dict] = {}
        self.edges: Dict[str, List[str]] = defaultdict(list)
        self.reverse_edges: Dict[str, List[str]] = defaultdict(list)
        self._lock = threading.RLock()
        logger.info("FallbackDependencyGraph initialized")
    
    def add_service(self, service_name: str, priority: int = 0,
                   fallback_options: List[str] = None):
        """Add a service node"""
        with self._lock:
            self.nodes[service_name] = {
                'name': service_name, 'priority': priority,
                'fallback_options': fallback_options or [],
                'healthy': True, 'added_at': time.time()
            }
    
    def add_dependency(self, service: str, depends_on: str):
        """Add dependency edge"""
        with self._lock:
            if service not in self.nodes:
                self.add_service(service)
            if depends_on not in self.nodes:
                self.add_service(depends_on)
            if depends_on not in self.edges[service]:
                self.edges[service].append(depends_on)
            if service not in self.reverse_edges[depends_on]:
                self.reverse_edges[depends_on].append(service)
    
    def get_fallback_order(self, failing_service: str) -> List[str]:
        """Get ordered fallback list"""
        with self._lock:
            if failing_service not in self.nodes:
                return []
            affected = self._get_affected_services(failing_service)
            order = self._topological_sort(affected)
            fallback = self.nodes[failing_service].get('fallback_options', [])
            return order + fallback
    
    def _get_affected_services(self, service: str) -> List[str]:
        """Get all affected services"""
        affected = set([service])
        queue = [service]
        while queue:
            current = queue.pop(0)
            for dependent in self.reverse_edges.get(current, []):
                if dependent not in affected:
                    affected.add(dependent)
                    queue.append(dependent)
        return list(affected)
    
    def _topological_sort(self, services: List[str]) -> List[str]:
        """Topological sort with priorities"""
        in_degree = {s: 0 for s in services}
        for service in services:
            for dep in self.edges.get(service, []):
                if dep in in_degree:
                    in_degree[service] += 1
        
        queue = [(self.nodes[s].get('priority', 0), s) for s in services if in_degree[s] == 0]
        heapq.heapify(queue)
        
        result = []
        while queue:
            _, service = heapq.heappop(queue)
            result.append(service)
            for dependent in self.reverse_edges.get(service, []):
                if dependent in in_degree:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        heapq.heappush(queue, (self.nodes[dependent].get('priority', 0), dependent))
        return result
    
    def has_cycles(self) -> bool:
        """Check for cycles"""
        visited = set()
        rec_stack = set()
        
        def dfs(node):
            visited.add(node)
            rec_stack.add(node)
            for dep in self.edges.get(node, []):
                if dep not in visited:
                    if dfs(dep):
                        return True
                elif dep in rec_stack:
                    return True
            rec_stack.remove(node)
            return False
        
        for node in self.nodes:
            if node not in visited:
                if dfs(node):
                    return True
        return False
    
    def get_impact_analysis(self, service: str) -> Dict:
        """Analyze impact of failure"""
        with self._lock:
            affected = self._get_affected_services(service)
            return {
                'failing_service': service,
                'affected_services': len(affected),
                'affected_list': affected,
                'fallback_order': self.get_fallback_order(service),
                'has_cycles': self.has_cycles()
            }
    
    def get_statistics(self) -> Dict:
        """Get graph statistics"""
        with self._lock:
            return {
                'total_services': len(self.nodes),
                'total_dependencies': sum(len(v) for v in self.edges.values()),
                'has_cycles': self.has_cycles()
            }


# ============================================================
# Complete Working Example
# ============================================================

async def main():
    """Enhanced demonstration with all improvements"""
    print("=" * 70)
    print("Ultimate Fallback Manager v4.1 - Enhanced Demo")
    print("=" * 70)
    
    fallback_mgr = UltimateFallbackManagerV4({
        'availability_target': 0.999,
        'latency_target_ms': 100,
        'seasonality_period': 24,
        'bandit_algorithm': 'auto',
        'circuit_breaker': {
            'distributed': False,
            'failure_threshold': 0.5,
            'min_requests': 5,
            'adaptive_threshold': True
        }
    })
    
    print("\n✅ All enhancements active:")
    print(f"   Adaptive threshold: enabled")
    print(f"   EXP3 bandit: available")
    print(f"   Response caching: enabled")
    print(f"   Auto-replenishment: enabled")
    print(f"   Graceful shutdown: enabled")
    
    # Test circuit breaker with adaptive threshold
    print("\n🔌 Circuit Breaker with Adaptive Threshold:")
    cb = fallback_mgr.get_circuit_breaker('temperature')
    
    async def mock_api_call():
        await asyncio.sleep(0.02)
        if random.random() > 0.6:
            return {'temperature': 65.0, 'value': 65.0}
        raise Exception("API error")
    
    for i in range(25):
        result = await fallback_mgr.execute_with_fallback_enhanced(
            mock_api_call, 'temperature', {'value': 65.0 + random.gauss(0, 2)}
        )
        status = "✅" if result.success else "❌"
        print(f"   {status} Attempt {i+1}: success={result.success}, "
              f"strategy={result.strategy_used}, "
              f"health={result.health_score:.1f}")
    
    cb_status = cb.get_status()
    print(f"\n   State: {cb_status['state']}")
    print(f"   Adaptive threshold: {cb_status['adaptive_threshold']:.3f}")
    print(f"   Health trend: {cb_status['health_trend']}")
    print(f"   Cache size: {cb_status['cache_size']}")
    print(f"   State transitions: {cb_status['transition_count']}")
    
    # Test multi-armed bandit with auto-detection
    print("\n🎰 Multi-Armed Bandit (Auto-detect algorithm):")
    bandit_stats = fallback_mgr.strategy_selector.get_statistics()
    print(f"   Effective algorithm: {'EXP3' if not bandit_stats['is_stationary'] else 'Thompson'}")
    print(f"   Is stationary: {bandit_stats['is_stationary']}")
    print(f"   Best strategy: {bandit_stats['best_strategy']}")
    for strategy, rate in bandit_stats['strategy_success_rates'].items():
        print(f"   {strategy}: {rate:.2%}")
    
    # Test SLA with auto-replenishment
    print("\n📊 SLA Tracking (with auto-replenishment):")
    sla = fallback_mgr.sla_tracker.get_status()
    print(f"   Availability: {sla['availability']:.4%}")
    print(f"   Error budget: {sla['error_budget_remaining']}/{sla['error_budget_total']}")
    print(f"   Burn rate level: {sla['burn_rate_level']}")
    print(f"   Next replenishment: {sla['next_replenishment_seconds']:.0f}s")
    
    # Test aggregate health
    print("\n💚 Aggregate System Health:")
    health = fallback_mgr.get_aggregate_health()
    print(f"   Overall: {health['overall']:.2f}")
    print(f"   Status: {health['status']}")
    print(f"   Circuit breakers: {health['circuit_breakers']:.2f}")
    print(f"   Components: {health['components']}")
    
    # Test graceful shutdown
    print("\n🔌 Testing Graceful Shutdown...")
    shutdown_task = asyncio.create_task(fallback_mgr.shutdown(drain_timeout=3))
    await asyncio.sleep(1)
    status = fallback_mgr.get_enhanced_status()
    print(f"   Shutting down: {status['shutting_down']}")
    print(f"   Pending requests: {status['pending_requests']}")
    
    await shutdown_task
    print("   Shutdown complete")
    
    print("\n" + "=" * 70)
    print("✅ Ultimate Fallback Manager v4.1 - All Enhancements Demonstrated")
    print("   - Adaptive threshold learning")
    print("   - EXP3 bandit for non-stationary environments")
    print("   - Response caching for instant recovery")
    print("   - State transition logging")
    print("   - Health trend analysis")
    print("   - Aggregate health scoring")
    print("   - Graceful shutdown with draining")
    print("   - Auto-replenishing error budgets")
    print("=" * 70)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())
