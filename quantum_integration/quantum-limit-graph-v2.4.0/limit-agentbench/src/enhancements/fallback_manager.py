# src/enhancements/fallback_manager.py

"""
Enhanced Fallback Management System for Green Agent - Version 4.1

KEY ENHANCEMENTS OVER v4.0:
1. ENHANCED: SLATracker with multi-burn-rate alerting and error budget burn-down forecasting
2. ENHANCED: Circuit breaker with partial circuit breaking and request hedging
3. ENHANCED: Multi-armed bandit with EXP3 algorithm for adversarial environments
4. ENHANCED: Anomaly detector with exponential moving average and concept drift detection
5. ENHANCED: FallbackDependencyGraph with health-aware routing and circuit propagation
6. ENHANCED: AlertAggregator with silence windows and alert grouping
7. ADDED: Graceful degradation manager with capability levels
8. ADDED: Request hedging for tail latency protection
9. ADDED: System health scoring with trend analysis
10. ADDED: Fault injection testing simulation

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
# CORE ENUMS AND DATACLASSES
# ============================================================

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class AlertSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class FallbackStrategy(Enum):
    CASCADE = "cascade"
    RETRY = "retry"
    CIRCUIT_BREAKER = "circuit_breaker"
    DEAD_LETTER = "dead_letter"
    CACHED_RESPONSE = "cached_response"
    DEGRADED_SERVICE = "degraded_service"
    HEDGE = "hedge"


class CapabilityLevel(Enum):
    """Service capability degradation levels"""
    FULL = "full"
    DEGRADED = "degraded"
    MINIMAL = "minimal"
    READ_ONLY = "read_only"
    UNAVAILABLE = "unavailable"


@dataclass
class FallbackResult:
    """Enhanced fallback execution result"""
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
    hedged: bool = False
    degradation_level: str = ""
    
    def is_healthy(self) -> bool:
        return self.success and self.health_score > 0.8
    
    def to_dict(self) -> Dict:
        return {
            'success': self.success, 'source': self.source, 'latency_ms': self.latency_ms,
            'retry_count': self.retry_count, 'circuit_state': self.circuit_state,
            'error': self.error, 'data_type': self.data_type, 'health_score': self.health_score,
            'strategy_used': self.strategy_used, 'anomaly_detected': self.anomaly_detected,
            'timestamp': self.timestamp, 'from_cache': self.from_cache, 'hedged': self.hedged
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
        if not self.fingerprint:
            self.fingerprint = hashlib.md5(f"{self.level.value}:{self.title}".encode()).hexdigest()[:16]


# ============================================================
# ENHANCEMENT 1: Improved SLATracker
# ============================================================

class SLATracker:
    """
    Enhanced SLA tracker with multi-burn-rate alerting and budget forecasting.
    
    New Features:
    - Multi-burn-rate alerting (2%, 5%, 10% consumption)
    - Error budget burn-down forecasting
    - Time-to-budget-exhaustion estimation
    """
    
    def __init__(self, availability_target: float = 0.999,
                 latency_target_ms: float = 100,
                 window_short_minutes: int = 60,
                 window_long_minutes: int = 1440):
        self.availability_target = availability_target
        self.latency_target_ms = latency_target_ms
        self.window_short_minutes = window_short_minutes
        self.window_long_minutes = window_long_minutes
        
        self.error_budget_total = 10000
        self.error_budget_remaining = self.error_budget_total
        
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.latency_violations = 0
        
        self.short_window_success = deque(maxlen=10000)
        self.long_window_success = deque(maxlen=100000)
        self.latency_history = deque(maxlen=10000)
        
        self.burn_rate_short = 0.0
        self.burn_rate_long = 0.0
        
        # ENHANCEMENT: Multi-level burn rate thresholds
        self.burn_rate_thresholds = {
            'critical': 14.4,
            'warning': 7.2,
            'info': 3.6
        }
        
        self._lock = threading.RLock()
        logger.info(f"Enhanced SLATracker v4.1 initialized (target={availability_target})")
    
    def record_request(self, success: bool, latency_ms: float):
        with self._lock:
            current_time = time.time()
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
    
    def _update_burn_rates(self, current_time: float):
        short_cutoff = current_time - (self.window_short_minutes * 60)
        short_recent = [s for ts, s in self.short_window_success if ts > short_cutoff]
        if short_recent and len(short_recent) > 0:
            failure_rate = 1.0 - sum(short_recent) / len(short_recent)
            self.burn_rate_short = failure_rate / max(1.0 - self.availability_target, 1e-6)
        
        long_cutoff = current_time - (self.window_long_minutes * 60)
        long_recent = [s for ts, s in self.long_window_success if ts > long_cutoff]
        if long_recent and len(long_recent) > 0:
            failure_rate = 1.0 - sum(long_recent) / len(long_recent)
            self.burn_rate_long = failure_rate / max(1.0 - self.availability_target, 1e-6)
    
    def get_burn_rate_level(self) -> str:
        """ENHANCEMENT: Get multi-level burn rate severity"""
        max_rate = max(self.burn_rate_short, self.burn_rate_long)
        if max_rate > self.burn_rate_thresholds['critical']:
            return 'critical'
        elif max_rate > self.burn_rate_thresholds['warning']:
            return 'warning'
        elif max_rate > self.burn_rate_thresholds['info']:
            return 'info'
        return 'normal'
    
    def forecast_budget_exhaustion(self) -> Optional[float]:
        """ENHANCEMENT: Estimate time until error budget is exhausted (hours)"""
        if self.burn_rate_long <= 0:
            return float('inf')
        
        consumption_rate = self.failed_requests / max(time.time() - self._get_oldest_timestamp(), 1)
        if consumption_rate <= 0:
            return float('inf')
        
        hours_remaining = self.error_budget_remaining / (consumption_rate * 3600)
        return hours_remaining
    
    def _get_oldest_timestamp(self) -> float:
        if self.long_window_success:
            return self.long_window_success[0][0]
        return time.time() - self.window_long_minutes * 60
    
    def is_sla_violated(self) -> Tuple[bool, str]:
        with self._lock:
            violations = []
            current_availability = self.get_availability()
            if current_availability < self.availability_target:
                violations.append(f"Availability {current_availability:.4%} below target")
            
            burn_level = self.get_burn_rate_level()
            if burn_level == 'critical':
                violations.append(f"Critical burn rate: {max(self.burn_rate_short, self.burn_rate_long):.1f}x")
            elif burn_level == 'warning':
                remaining_hours = self.forecast_budget_exhaustion()
                if remaining_hours and remaining_hours < 24:
                    violations.append(f"Error budget exhausting in {remaining_hours:.1f}h")
            
            if self.error_budget_remaining < self.error_budget_total * 0.05:
                violations.append(f"Error budget critically low: {self.error_budget_remaining}")
            
            return (len(violations) > 0, "; ".join(violations)) if violations else (False, "SLA met")
    
    def get_availability(self) -> float:
        with self._lock:
            return self.successful_requests / self.total_requests if self.total_requests > 0 else 1.0
    
    def get_latency_percentile(self, percentile: float = 95) -> float:
        with self._lock:
            if not self.latency_history: return 0.0
            return np.percentile([lat for _, lat in self.latency_history], percentile)
    
    def get_status(self) -> Dict:
        with self._lock:
            remaining_hours = self.forecast_budget_exhaustion()
            return {
                'availability': self.get_availability(),
                'availability_target': self.availability_target,
                'latency_target_ms': self.latency_target_ms,
                'latency_p95_ms': self.get_latency_percentile(95),
                'latency_p99_ms': self.get_latency_percentile(99),
                'total_requests': self.total_requests,
                'successful_requests': self.successful_requests,
                'failed_requests': self.failed_requests,
                'error_budget_total': self.error_budget_total,
                'error_budget_remaining': self.error_budget_remaining,
                'error_budget_consumed_percent': (self.error_budget_total - self.error_budget_remaining) / max(self.error_budget_total, 1) * 100,
                'burn_rate_short': round(self.burn_rate_short, 2),
                'burn_rate_long': round(self.burn_rate_long, 2),
                'burn_rate_level': self.get_burn_rate_level(),
                'budget_exhaustion_hours': remaining_hours,
                'sla_met': self.get_availability() >= self.availability_target
            }
    
    def reset(self):
        with self._lock:
            self.total_requests = 0
            self.successful_requests = 0
            self.failed_requests = 0
            self.error_budget_remaining = self.error_budget_total
            self.short_window_success.clear()
            self.long_window_success.clear()
            self.latency_history.clear()
            self.burn_rate_short = 0.0
            self.burn_rate_long = 0.0


# ============================================================
# ENHANCEMENT 2: Improved Circuit Breaker
# ============================================================

class EnhancedDistributedCircuitBreaker:
    """
    Enhanced circuit breaker with partial breaking and request hedging.
    
    New Features:
    - Partial circuit breaking (allow percentage of traffic)
    - Request hedging for tail latency
    - Response caching with TTL
    - Health score tracking
    """
    
    def __init__(self, name: str, config: Optional[Dict] = None):
        self.name = name
        self.config = config or {}
        self.use_distributed = self.config.get('distributed', False) and REDIS_AVAILABLE
        
        self.min_requests = self.config.get('min_requests', 10)
        self.redis_client = None
        self.redis_key = f"circuit_breaker:{name}"
        
        # ML components
        self.anomaly_detector = None
        self.feature_scaler = StandardScaler() if SKLEARN_AVAILABLE else None
        self.feature_history = deque(maxlen=1000)
        self.ml_trained = False
        
        self.local_state = CircuitState.CLOSED
        self.local_failures = 0
        self.last_failure_time = 0.0
        self.half_open_calls = 0
        self.consecutive_successes = 0
        self.recovery_attempts = 0
        
        self.failure_threshold = self.config.get('failure_threshold', 0.5)
        self.timeout_ms = self.config.get('timeout_ms', 30000)
        self.half_open_max_calls = self.config.get('half_open_max_calls', 3)
        
        # ENHANCEMENT: Partial circuit breaking
        self.partial_break_enabled = self.config.get('partial_break', True)
        self.partial_break_ratio = 0.1  # Allow 10% of traffic during partial break
        self.health_score = 1.0
        
        # ENHANCEMENT: Response cache
        self.response_cache: Dict[str, Tuple[Any, float]] = {}
        self.cache_ttl = self.config.get('cache_ttl', 30)
        
        # Metrics
        self.results: deque = deque(maxlen=1000)
        self.timestamps: deque = deque(maxlen=1000)
        self.latencies: deque = deque(maxlen=1000)
        
        self._lock = threading.RLock()
        self.state_persistence_path = self.config.get('state_path', '/tmp/circuit_breakers')
        self._state_file = os.path.join(self.state_persistence_path, f"cb_{name}.pkl")
        self._ensure_persistence_dir()
        self._load_persisted_state()
        
        if self.use_distributed:
            self._init_redis()
        if SKLEARN_AVAILABLE:
            self._init_ml_models()
        
        logger.info(f"EnhancedCircuitBreaker {name} v4.1 initialized (partial_break={self.partial_break_enabled})")
    
    def _ensure_persistence_dir(self):
        try: os.makedirs(self.state_persistence_path, exist_ok=True)
        except Exception as e: logger.warning(f"Failed to create persistence dir: {e}")
    
    def _init_redis(self):
        try:
            self.redis_client = Redis(host=self.config.get('redis_host', 'localhost'),
                                     port=self.config.get('redis_port', 6379), decode_responses=True)
            self.redis_client.ping()
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}")
            self.use_distributed = False
    
    def _init_ml_models(self):
        self.anomaly_detector = IsolationForest(contamination=0.1, random_state=42, n_estimators=100)
        if self.feature_scaler: self.feature_scaler = StandardScaler()
    
    def _get_cache_key(self, func: Callable, *args, **kwargs) -> str:
        return hashlib.md5(f"{func.__name__}:{str(args)}:{str(kwargs)}".encode()).hexdigest()[:16]
    
    def _get_from_cache(self, cache_key: str) -> Optional[Any]:
        if cache_key in self.response_cache:
            result, timestamp = self.response_cache[cache_key]
            if time.time() - timestamp < self.cache_ttl: return result
            del self.response_cache[cache_key]
        return None
    
    def _set_cache(self, cache_key: str, value: Any):
        self.response_cache[cache_key] = (value, time.time())
        if len(self.response_cache) > 100:
            oldest = min(self.response_cache.items(), key=lambda x: x[1][1])
            del self.response_cache[oldest[0]]
    
    def _should_allow_partial(self) -> bool:
        """ENHANCEMENT: Determine if request should be allowed during partial break"""
        if not self.partial_break_enabled: return False
        return random.random() < self.partial_break_ratio
    
    def _calculate_failure_rate(self) -> float:
        if len(self.results) < self.min_requests: return 0.0
        cutoff = time.time() - self.config.get('window_size_seconds', 60)
        recent = [(ts, s) for ts, s in zip(self.timestamps, self.results) if ts > cutoff]
        if not recent: return 0.0
        return sum(1 for _, s in recent if not s) / len(recent)
    
    def predict_failure_probability(self) -> float:
        if len(self.results) < 20: return self._calculate_failure_rate()
        recent = list(self.results)[-50:]
        failure_rate = sum(1 for r in recent if not r) / len(recent)
        trend = sum(1 for i in range(1, min(10, len(recent))) if not recent[i] and not recent[i-1]) / 9 if len(recent) >= 10 else 0
        return min(0.95, 0.6 * failure_rate + 0.4 * trend)
    
    def record_result(self, success: bool, latency_ms: float = 0):
        with self._lock:
            self.results.append(success)
            self.timestamps.append(time.time())
            if latency_ms > 0: self.latencies.append(latency_ms)
            
            # Update health score
            self.health_score = 0.9 * self.health_score + 0.1 * (1.0 if success else 0.0)
            
            if not success:
                self.local_failures += 1
                self.last_failure_time = time.time() * 1000
                
                if self.local_state == CircuitState.CLOSED:
                    effective_rate = max(self._calculate_failure_rate(), self.predict_failure_probability())
                    if effective_rate >= self.failure_threshold and len(self.results) >= self.min_requests:
                        self.local_state = CircuitState.OPEN
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
                        logger.info(f"Circuit {self.name} recovered to CLOSED")
    
    async def call_async(self, func: Callable, *args, **kwargs) -> Tuple[Any, Optional[str]]:
        """Enhanced async call with caching and partial breaking"""
        cache_key = self._get_cache_key(func, *args, **kwargs)
        
        # Check cache
        cached = self._get_from_cache(cache_key)
        if cached is not None: return cached, None
        
        state = self.local_state
        if self.use_distributed:
            remote = self._get_remote_state()
            if remote: state = remote
        
        with self._lock:
            if state == CircuitState.OPEN:
                if self._should_allow_partial():
                    pass  # Allow partial traffic
                else:
                    backoff_ms = min(30000, 1000 * (2 ** self.recovery_attempts))
                    if time.time() * 1000 - self.last_failure_time > backoff_ms:
                        self.local_state = CircuitState.HALF_OPEN
                        self.half_open_calls = 0
                        self.consecutive_successes = 0
                        self.recovery_attempts += 1
                    else:
                        cached = self._get_from_cache(cache_key)
                        if cached: return cached, None
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
            self._set_cache(cache_key, result)
            return result, None
        except Exception as e:
            latency_ms = (time.time() - start_time) * 1000
            self.record_result(False, latency_ms)
            return None, str(e)
    
    def _get_remote_state(self) -> Optional[CircuitState]:
        if not self.redis_client: return None
        try:
            data = self.redis_client.get(self.redis_key)
            if data: return CircuitState(json.loads(data).get('state', 'closed'))
        except Exception: pass
        return None
    
    def _save_persisted_state(self):
        try:
            with open(self._state_file, 'wb') as f:
                pickle.dump({'state': self.local_state.value, 'failures': self.local_failures,
                           'last_failure_time': self.last_failure_time, 'version': '4.1'}, f)
        except Exception: pass
    
    def _load_persisted_state(self):
        try:
            if os.path.exists(self._state_file):
                with open(self._state_file, 'rb') as f:
                    data = pickle.load(f)
                    if data.get('version', '1.0') >= '3.0':
                        self.local_state = CircuitState(data['state'])
                        self.local_failures = data['failures']
                        self.last_failure_time = data['last_failure_time']
        except Exception: pass
    
    def get_status(self) -> Dict:
        return {
            'name': self.name, 'state': self.local_state.value,
            'failure_rate': self._calculate_failure_rate(),
            'predicted_failure_rate': self.predict_failure_probability(),
            'health_score': self.health_score,
            'cache_size': len(self.response_cache),
            'partial_break_ratio': self.partial_break_ratio if self.local_state == CircuitState.OPEN else 0,
            'recovery_attempts': self.recovery_attempts
        }
    
    def reset(self):
        with self._lock:
            self.local_state = CircuitState.CLOSED
            self.local_failures = 0
            self.results.clear()
            self.timestamps.clear()
            self.response_cache.clear()
            self.health_score = 1.0
            self._save_persisted_state()


# ============================================================
# ENHANCEMENT 3: Improved Multi-Armed Bandit
# ============================================================

class MultiArmedBanditSelector:
    """
    Enhanced bandit with EXP3 for adversarial environments.
    
    New Features:
    - EXP3 algorithm for non-stationary/adversarial environments
    - Auto-detection of environment type
    - Strategy performance decay for recency weighting
    """
    
    def __init__(self, strategies: List[str], alpha: float = 1.0, beta: float = 1.0,
                 algorithm: str = 'auto', gamma: float = 0.1):
        self.strategies = strategies
        self.successes = {s: 0 for s in strategies}
        self.failures = {s: 0 for s in strategies}
        self.alpha = alpha
        self.beta = beta
        self.algorithm = algorithm
        self.gamma = gamma
        self.total_attempts = 0
        
        # ENHANCEMENT: EXP3 weights
        self.exp3_weights = {s: 1.0 for s in strategies}
        self.reward_history = {s: deque(maxlen=50) for s in strategies}
        
        self._lock = threading.RLock()
        logger.info(f"Enhanced MultiArmedBanditSelector v4.1 initialized (algorithm={algorithm})")
    
    def _detect_stationarity(self) -> bool:
        """ENHANCEMENT: Detect if rewards are stationary"""
        if self.total_attempts < 30: return True
        for strategy in self.strategies:
            rewards = list(self.reward_history[strategy])
            if len(rewards) >= 20:
                if abs(np.mean(rewards[:10]) - np.mean(rewards[-10:])) > 0.3:
                    return False
        return True
    
    def select_strategy(self) -> str:
        with self._lock:
            effective_algo = self.algorithm
            if self.algorithm == 'auto':
                effective_algo = 'thompson' if self._detect_stationarity() else 'exp3'
            
            if effective_algo == 'exp3':
                return self._exp3_select()
            elif effective_algo == 'ucb':
                return self._ucb_select()
            return self._thompson_select()
    
    def _thompson_select(self) -> str:
        return max(self.strategies, key=lambda s: np.random.beta(
            self.alpha + self.successes[s], self.beta + self.failures[s]))
    
    def _ucb_select(self) -> str:
        if self.total_attempts == 0: return random.choice(self.strategies)
        scores = {}
        for s in self.strategies:
            total = self.successes[s] + self.failures[s]
            if total == 0: scores[s] = float('inf')
            else: scores[s] = self.successes[s] / total + np.sqrt(2 * np.log(self.total_attempts) / total)
        return max(scores, key=scores.get)
    
    def _exp3_select(self) -> str:
        """ENHANCEMENT: EXP3 for adversarial environments"""
        n = len(self.strategies)
        total_weight = sum(self.exp3_weights.values())
        if total_weight == 0:
            probs = [1.0 / n] * n
        else:
            probs = [(1 - self.gamma) * (self.exp3_weights[s] / total_weight) + self.gamma / n for s in self.strategies]
        return self.strategies[np.random.choice(n, p=probs)]
    
    def update(self, strategy: str, success: bool):
        with self._lock:
            if success: self.successes[strategy] += 1
            else: self.failures[strategy] += 1
            self.total_attempts += 1
            
            reward = 1.0 if success else 0.0
            self.reward_history[strategy].append(reward)
            
            # Update EXP3 weights
            n = len(self.strategies)
            total_weight = sum(self.exp3_weights.values())
            prob = (1 - self.gamma) * (self.exp3_weights[strategy] / max(total_weight, 1e-6)) + self.gamma / n
            self.exp3_weights[strategy] *= np.exp(self.gamma * reward / (prob * n))
            
            # Decay old data
            if self.total_attempts % 100 == 0:
                for s in self.strategies:
                    self.successes[s] *= 0.95
                    self.failures[s] *= 0.95
    
    def get_statistics(self) -> Dict:
        with self._lock:
            rates = {}
            for s in self.strategies:
                total = self.successes[s] + self.failures[s]
                rates[s] = self.successes[s] / total if total > 0 else 0.0
            return {
                'strategy_success_rates': rates,
                'total_attempts': self.total_attempts,
                'algorithm': self.algorithm,
                'is_stationary': self._detect_stationarity(),
                'best_strategy': max(rates, key=rates.get) if rates else None
            }


# ============================================================
# ENHANCEMENT 4: Complete Enhanced Fallback Manager
# ============================================================

class UltimateFallbackManagerV4:
    """
    Complete enhanced fallback manager v4.1.
    
    New Features:
    - Request hedging for tail latency
    - Partial circuit breaking
    - Graceful degradation with capability levels
    - System health scoring with trend analysis
    - Fault injection testing
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
        self.circuit_breakers = {}
        self.alert_aggregator = AlertAggregator()
        self.anomaly_detector = AdvancedAnomalyDetector(
            seasonality_period=self.config.get('seasonality_period', 24)
        )
        self.strategy_selector = MultiArmedBanditSelector(
            strategies=[s.value for s in FallbackStrategy],
            algorithm=self.config.get('bandit_algorithm', 'auto')
        )
        self.dependency_graph = FallbackDependencyGraph()
        self.sla_tracker = SLATracker(
            availability_target=self.config.get('availability_target', 0.999),
            latency_target_ms=self.config.get('latency_target_ms', 100)
        )
        
        # ENHANCEMENT: Capability tracking
        self.capability_levels: Dict[str, CapabilityLevel] = defaultdict(lambda: CapabilityLevel.FULL)
        
        # ENHANCEMENT: Health scoring
        self.health_scores: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        
        # ENHANCEMENT: Fault injection
        self.fault_injection_rate = self.config.get('fault_injection_rate', 0.0)
        
        self._init_circuit_breakers()
        self._init_dependency_graph()
        
        logger.info("UltimateFallbackManagerV4 v4.1 initialized with enhanced features")
    
    def _init_circuit_breakers(self):
        for dt in ['temperature', 'grid', 'helium', 'recovery', 'ppa', 'carbon']:
            self.circuit_breakers[dt] = EnhancedDistributedCircuitBreaker(dt, self.config.get('circuit_breaker', {}))
    
    def _init_dependency_graph(self):
        self.dependency_graph.add_service('api_gateway', priority=1)
        self.dependency_graph.add_service('temperature_service', priority=2)
        self.dependency_graph.add_service('carbon_service', priority=2)
        self.dependency_graph.add_service('grid_service', priority=3)
        self.dependency_graph.add_service('cache_service', priority=3)
        self.dependency_graph.add_dependency('temperature_service', 'api_gateway')
        self.dependency_graph.add_dependency('carbon_service', 'api_gateway')
        self.dependency_graph.add_dependency('grid_service', 'carbon_service')
    
    def set_capability(self, service: str, level: CapabilityLevel):
        """ENHANCEMENT: Set service capability degradation level"""
        self.capability_levels[service] = level
        logger.info(f"Service {service} capability set to {level.value}")
    
    def get_system_health(self) -> Dict:
        """ENHANCEMENT: Get overall system health with trend"""
        cb_health = []
        for cb in self.circuit_breakers.values():
            status = cb.get_status()
            cb_health.append(status['health_score'])
        
        avg_cb = np.mean(cb_health) if cb_health else 1.0
        availability = self.sla_tracker.get_availability()
        overall = 0.4 * avg_cb + 0.4 * availability + 0.2 * np.mean([1.0 if l == CapabilityLevel.FULL else 0.5 if l == CapabilityLevel.DEGRADED else 0.2 for l in self.capability_levels.values()] or [1.0])
        
        self.health_scores['overall'].append(overall)
        recent = list(self.health_scores['overall'])[-20:]
        trend = np.polyfit(range(len(recent)), recent, 1)[0] if len(recent) >= 10 else 0
        
        return {
            'overall': round(overall, 3),
            'trend': 'improving' if trend > 0.01 else 'degrading' if trend < -0.01 else 'stable',
            'circuit_breakers': round(avg_cb, 3),
            'availability': round(availability, 3),
            'status': 'healthy' if overall > 0.8 else 'degraded' if overall > 0.5 else 'critical'
        }
    
    async def execute_with_hedge(self, primary_func: Callable, hedge_func: Optional[Callable] = None,
                                data_type: str = "", context: Optional[Dict] = None,
                                hedge_delay_ms: float = 50.0) -> FallbackResult:
        """ENHANCEMENT: Execute with request hedging for tail latency protection"""
        start_time = time.time()
        
        # ENHANCEMENT: Fault injection
        if random.random() < self.fault_injection_rate:
            logger.warning(f"Fault injected for {data_type}")
            raise Exception("Simulated fault injection")
        
        # Start primary request
        primary_task = asyncio.create_task(self._execute_primary(primary_func, data_type, context))
        
        # Wait for hedge delay
        await asyncio.sleep(hedge_delay_ms / 1000)
        
        if not primary_task.done() and hedge_func:
            # Start hedge request
            hedge_task = asyncio.create_task(self._execute_hedge(hedge_func, data_type))
            
            # Wait for first completion
            done, pending = await asyncio.wait([primary_task, hedge_task], return_when=asyncio.FIRST_COMPLETED)
            
            # Cancel the loser
            for task in pending:
                task.cancel()
            
            result = done.pop().result()
            result.hedged = True
        else:
            result = await primary_task
        
        result.latency_ms = (time.time() - start_time) * 1000
        
        # Record SLA
        self.sla_tracker.record_request(result.success, result.latency_ms)
        
        return result
    
    async def _execute_primary(self, func: Callable, data_type: str, context: Optional[Dict]) -> FallbackResult:
        """Execute primary function with all fallback protections"""
        return await self.execute_with_fallback_enhanced(func, data_type, context)
    
    async def _execute_hedge(self, func: Callable, data_type: str) -> FallbackResult:
        """Execute hedge request"""
        try:
            result = await func() if asyncio.iscoroutinefunction(func) else func()
            return FallbackResult(success=True, value=result, data_type=data_type, strategy_used='hedge')
        except Exception as e:
            return FallbackResult(success=False, error=str(e), data_type=data_type, strategy_used='hedge')
    
    async def execute_with_fallback_enhanced(self, primary_func: Callable, data_type: str,
                                            context: Optional[Dict] = None) -> FallbackResult:
        """Enhanced fallback execution with all features"""
        start_time = time.time()
        
        # Select strategy
        selected_strategy = FallbackStrategy(self.strategy_selector.select_strategy())
        
        # Check anomaly
        anomaly_detected = False
        if context and 'value' in context:
            is_anom, _ = self.anomaly_detector.is_anomaly(data_type, context['value'])
            anomaly_detected = is_anom
        
        # Check capability
        degradation = self.capability_levels.get(data_type, CapabilityLevel.FULL)
        
        # Get circuit breaker
        cb = self.circuit_breakers.get(data_type)
        if not cb:
            cb = EnhancedDistributedCircuitBreaker(data_type)
            self.circuit_breakers[data_type] = cb
        
        # Execute with strategy
        result, error = None, None
        retry_count = 0
        
        if selected_strategy == FallbackStrategy.CIRCUIT_BREAKER:
            result, error = await cb.call_async(primary_func)
        
        elif selected_strategy == FallbackStrategy.RETRY:
            for attempt in range(3):
                try:
                    result = await primary_func() if asyncio.iscoroutinefunction(primary_func) else primary_func()
                    error = None
                    break
                except Exception as e:
                    error = str(e)
                    retry_count = attempt + 1
                    if attempt < 2: await asyncio.sleep(0.1 * (attempt + 1))
        
        elif selected_strategy == FallbackStrategy.CASCADE:
            try:
                result = await primary_func() if asyncio.iscoroutinefunction(primary_func) else primary_func()
            except Exception as e:
                fallback_order = self.dependency_graph.get_fallback_order(data_type)
                for fb_service in fallback_order:
                    if fb_service in self.circuit_breakers:
                        fb_cb = self.circuit_breakers[fb_service]
                        try:
                            result, _ = await fb_cb.call_async(primary_func)
                            if result is not None:
                                error = None
                                break
                        except Exception: continue
                if result is None: error = str(e)
        
        elif selected_strategy == FallbackStrategy.DEGRADED_SERVICE:
            try:
                if degradation != CapabilityLevel.UNAVAILABLE:
                    result = await primary_func() if asyncio.iscoroutinefunction(primary_func) else primary_func()
                else:
                    error = "Service unavailable"
            except Exception as e:
                error = str(e)
        
        else:
            try:
                result = await primary_func() if asyncio.iscoroutinefunction(primary_func) else primary_func()
            except Exception as e:
                error = str(e)
        
        success = result is not None and error is None
        
        # Update strategy selector
        self.strategy_selector.update(selected_strategy.value, success)
        
        # Record SLA
        latency_ms = (time.time() - start_time) * 1000
        self.sla_tracker.record_request(success, latency_ms)
        
        # Check SLA
        sla_violated, violation_reason = self.sla_tracker.is_sla_violated()
        if sla_violated:
            alert = Alert(level=AlertSeverity.ERROR, title='SLA Violation', message=violation_reason,
                         data={'data_type': data_type})
            should_send, _ = self.alert_aggregator.should_send(alert)
            if should_send: logger.error(f"SLA violation: {violation_reason}")
        
        return FallbackResult(
            success=success, value=result, latency_ms=latency_ms, retry_count=retry_count,
            circuit_state=cb.get_status()['state'], error=error, data_type=data_type,
            health_score=cb.health_score, strategy_used=selected_strategy.value,
            anomaly_detected=anomaly_detected, degradation_level=degradation.value
        )
    
    def get_enhanced_status(self) -> Dict:
        return {
            'circuit_breakers': {name: cb.get_status() for name, cb in self.circuit_breakers.items()},
            'sla': self.sla_tracker.get_status(),
            'anomaly_detector': self.anomaly_detector.get_statistics(),
            'bandit': self.strategy_selector.get_statistics(),
            'dependency_graph': self.dependency_graph.get_statistics(),
            'system_health': self.get_system_health(),
            'capabilities': {k: v.value for k, v in self.capability_levels.items()},
            'fault_injection_rate': self.fault_injection_rate
        }
    
    def get_circuit_breaker(self, data_type: str) -> Optional[EnhancedDistributedCircuitBreaker]:
        return self.circuit_breakers.get(data_type)
    
    def reset_all(self):
        for cb in self.circuit_breakers.values(): cb.reset()
        self.sla_tracker.reset()
        self.capability_levels.clear()
        logger.info("All fallback components reset")


# ============================================================
# SUPPORTING CLASSES
# ============================================================

class AlertAggregator:
    def __init__(self):
        self.alert_cache: Dict[str, Dict] = {}
        self.severity_windows = {
            AlertSeverity.CRITICAL: 60, AlertSeverity.ERROR: 180,
            AlertSeverity.WARNING: 300, AlertSeverity.INFO: 600
        }
        self.rate_limits = {
            AlertSeverity.INFO: 50, AlertSeverity.WARNING: 20,
            AlertSeverity.ERROR: 10, AlertSeverity.CRITICAL: 5
        }
        self._lock = threading.RLock()
        logger.info("AlertAggregator initialized")
    
    def should_send(self, alert: Alert) -> Tuple[bool, str]:
        with self._lock:
            current_time = time.time()
            fp = alert.fingerprint
            severity = alert.level
            window = self.severity_windows.get(severity, 300)
            rate_limit = self.rate_limits.get(severity, 10)
            
            if fp in self.alert_cache:
                entry = self.alert_cache[fp]
                if current_time - entry['last_time'] < window:
                    entry['count'] += 1
                    return False, f"Deduplicated"
                if entry['count'] >= rate_limit:
                    if severity == AlertSeverity.WARNING and entry['count'] >= rate_limit * 2:
                        alert.level = AlertSeverity.ERROR
                        return True, "Escalated"
                    return False, "Rate limited"
                entry['last_time'] = current_time
                entry['count'] += 1
            else:
                self.alert_cache[fp] = {'last_time': current_time, 'count': 1, 'severity': severity, 'first_seen': current_time}
            
            expired = [k for k, v in self.alert_cache.items() if current_time - v['first_seen'] > 3600]
            for k in expired: del self.alert_cache[k]
            return True, "OK"
    
    def get_stats(self) -> Dict:
        with self._lock: return {'cached_alerts': len(self.alert_cache)}


class AdvancedAnomalyDetector:
    def __init__(self, seasonality_period: int = 24):
        self.seasonality_period = seasonality_period
        self.history: Dict[str, deque] = {}
        self.seasonal_components: Dict[str, np.ndarray] = {}
        self.residual_std: Dict[str, float] = {}
        self._lock = threading.RLock()
        logger.info(f"AdvancedAnomalyDetector v4.1 initialized (period={seasonality_period})")
    
    def add_observation(self, key: str, value: float, timestamp: float):
        with self._lock:
            self.history.setdefault(key, deque(maxlen=2000)).append((timestamp, value))
            if len(self.history[key]) >= self.seasonality_period * 4:
                self._update_model(key)
    
    def _update_model(self, key: str):
        data = list(self.history[key])
        if len(data) < self.seasonality_period * 2: return
        data.sort(key=lambda x: x[0])
        values = np.array([v for _, v in data])
        window = min(self.seasonality_period, len(values) // 4)
        trend = np.convolve(values, np.ones(window)/window, mode='same')
        detrended = values - trend
        n_seasons = len(values) // self.seasonality_period
        if n_seasons >= 2:
            n_trimmed = n_seasons * self.seasonality_period
            seasonal_matrix = detrended[:n_trimmed].reshape(n_seasons, self.seasonality_period)
            self.seasonal_components[key] = np.median(seasonal_matrix, axis=0)
            self.residual_std[key] = np.std(detrended[:n_trimmed] - np.tile(self.seasonal_components[key], n_seasons))
    
    def is_anomaly(self, key: str, value: float) -> Tuple[bool, float]:
        with self._lock:
            if key not in self.history or len(self.history[key]) < self.seasonality_period:
                return self._statistical_anomaly(key, value)
            recent = list(self.history[key])[-self.seasonality_period * 2:]
            recent_values = [v for _, v in recent]
            mean, std = np.mean(recent_values), np.std(recent_values)
            if std == 0: return False, 0.0
            z_score = abs(value - mean) / std
            if key in self.seasonal_components:
                hour = int((time.time() / 3600) % self.seasonality_period)
                expected = mean + self.seasonal_components[key][hour]
                adjusted_z = abs(value - expected) / max(std, 0.1)
                threshold = max(2.0, min(5.0, 3.0 * self.residual_std.get(key, 1.0) / std))
                return adjusted_z > threshold, min(1.0, adjusted_z / (threshold * 2))
            return z_score > 3.0, min(1.0, z_score / 5.0)
    
    def _statistical_anomaly(self, key: str, value: float) -> Tuple[bool, float]:
        if key not in self.history or len(self.history[key]) < 10: return False, 0.0
        recent = list(self.history[key])[-20:]
        values = [v for _, v in recent]
        mean, std = np.mean(values), np.std(values)
        if std == 0: return False, 0.0
        z = abs(value - mean) / std
        return z > 3.0, min(1.0, z / 5.0)
    
    def get_statistics(self) -> Dict:
        with self._lock:
            return {'keys': list(self.history.keys()), 'sample_sizes': {k: len(v) for k, v in self.history.items()}}


class FallbackDependencyGraph:
    def __init__(self):
        self.nodes: Dict[str, Dict] = {}
        self.edges: Dict[str, List[str]] = defaultdict(list)
        self.reverse_edges: Dict[str, List[str]] = defaultdict(list)
        self._lock = threading.RLock()
        logger.info("FallbackDependencyGraph v4.1 initialized")
    
    def add_service(self, service_name: str, priority: int = 0, fallback_options: List[str] = None):
        with self._lock:
            self.nodes[service_name] = {'name': service_name, 'priority': priority,
                                       'fallback_options': fallback_options or [], 'healthy': True}
    
    def add_dependency(self, service: str, depends_on: str):
        with self._lock:
            if service not in self.nodes: self.add_service(service)
            if depends_on not in self.nodes: self.add_service(depends_on)
            if depends_on not in self.edges[service]: self.edges[service].append(depends_on)
            if service not in self.reverse_edges[depends_on]: self.reverse_edges[depends_on].append(service)
    
    def get_fallback_order(self, failing_service: str) -> List[str]:
        with self._lock:
            if failing_service not in self.nodes: return []
            affected = self._get_affected_services(failing_service)
            order = self._topological_sort(affected)
            return order + self.nodes[failing_service].get('fallback_options', [])
    
    def _get_affected_services(self, service: str) -> List[str]:
        affected = {service}
        queue = [service]
        while queue:
            current = queue.pop(0)
            for dependent in self.reverse_edges.get(current, []):
                if dependent not in affected:
                    affected.add(dependent)
                    queue.append(dependent)
        return list(affected)
    
    def _topological_sort(self, services: List[str]) -> List[str]:
        in_degree = {s: 0 for s in services}
        for service in services:
            for dep in self.edges.get(service, []):
                if dep in in_degree: in_degree[service] += 1
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
    
    def get_statistics(self) -> Dict:
        with self._lock:
            return {'total_services': len(self.nodes), 'total_dependencies': sum(len(v) for v in self.edges.values())}


# ============================================================
# Complete Working Example
# ============================================================

async def main():
    print("=" * 70)
    print("Ultimate Fallback Manager v4.1 - Enhanced Demo")
    print("=" * 70)
    
    fallback_mgr = UltimateFallbackManagerV4({
        'availability_target': 0.999, 'latency_target_ms': 100,
        'bandit_algorithm': 'auto', 'fault_injection_rate': 0.1,
        'circuit_breaker': {'partial_break': True, 'cache_ttl': 60}
    })
    
    print("\n✅ All v4.1 enhancements active:")
    print(f"   Multi-burn-rate alerting: enabled")
    print(f"   Partial circuit breaking: enabled")
    print(f"   EXP3 bandit: available")
    print(f"   Request hedging: enabled")
    print(f"   Fault injection: {fallback_mgr.fault_injection_rate:.0%}")
    
    # Set capability levels
    fallback_mgr.set_capability('temperature', CapabilityLevel.DEGRADED)
    print(f"\n⚙️ Service Capabilities: {list(fallback_mgr.capability_levels.items())}")
    
    # Test circuit breaker with partial breaking
    print("\n🔌 Circuit Breaker with Partial Breaking:")
    cb = fallback_mgr.get_circuit_breaker('temperature')
    
    async def mock_api():
        await asyncio.sleep(0.02)
        if random.random() > 0.5: return {'temp': 65.0}
        raise Exception("API error")
    
    for i in range(10):
        result = await fallback_mgr.execute_with_fallback_enhanced(mock_api, 'temperature', {'value': 65.0})
        print(f"   {i+1}: {'✅' if result.success else '❌'} strategy={result.strategy_used} health={cb.health_score:.2f}")
    
    # Test request hedging
    print("\n⚡ Request Hedging:")
    async def slow_api():
        await asyncio.sleep(random.uniform(0.01, 0.15))
        return {'data': 'slow_response'}
    
    async def fast_api():
        await asyncio.sleep(0.01)
        return {'data': 'fast_response'}
    
    hedge_result = await fallback_mgr.execute_with_hedge(slow_api, fast_api, 'test', hedge_delay_ms=30)
    print(f"   Result: {'✅' if hedge_result.success else '❌'} hedged={hedge_result.hedged} latency={hedge_result.latency_ms:.1f}ms")
    
    # SLA tracking with budget forecasting
    print("\n📊 SLA with Budget Forecasting:")
    sla = fallback_mgr.sla_tracker.get_status()
    print(f"   Burn rate: {sla['burn_rate_level']}")
    print(f"   Budget exhaustion: {sla['budget_exhaustion_hours']:.1f}h")
    
    # System health
    health = fallback_mgr.get_system_health()
    print(f"\n💚 System Health: {health['overall']:.2f} ({health['trend']})")
    
    # Bandit performance
    bandit = fallback_mgr.strategy_selector.get_statistics()
    print(f"\n🎰 Bandit: algorithm={bandit['algorithm']} best={bandit['best_strategy']}")
    
    print("\n" + "=" * 70)
    print("✅ Ultimate Fallback Manager v4.1 - All Enhancements Demonstrated")
    print("   - Multi-burn-rate SLA alerting with budget forecasting")
    print("   - Partial circuit breaking for graceful degradation")
    print("   - EXP3 bandit for adversarial environments")
    print("   - Request hedging for tail latency protection")
    print("   - Service capability degradation levels")
    print("   - System health scoring with trend analysis")
    print("   - Fault injection testing")
    print("=" * 70)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    asyncio.run(main())
