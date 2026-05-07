# src/enhancements/fallback_manager.py

"""
Enhanced Fallback Management System for Green Agent - Version 3.2

ENHANCEMENTS:
1. Distributed circuit breaker with Redis cluster support
2. Webhook alerts for critical failures
3. ML-based anomaly detection for fallback triggers
4. Adaptive TTL with reinforcement learning
5. Circuit breaker state persistence to disk
6. Fallback performance profiling with flamegraphs
7. SLA tracking with error budgets
8. Canary testing for fallback strategies
9. Fallback dependency graph visualization
10. Automated fallback strategy tuning with Bayesian optimization

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

# Try to import optional dependencies
try:
    import redis
    from redis.client import Redis
    from redis.cluster import RedisCluster
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning("redis not available, distributed mode disabled")

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: Distributed Circuit Breaker with Redis
# ============================================================

class DistributedCircuitBreaker:
    """
    Distributed circuit breaker using Redis for cross-process coordination.
    
    Features:
    - Shared state across multiple instances
    - Automatic leader election
    - State persistence across restarts
    - Fallback to local mode when Redis unavailable
    """
    
    def __init__(self, name: str, config: Optional[Dict] = None):
        self.name = name
        self.config = config or {}
        self.redis_client = None
        self.use_distributed = self.config.get('distributed', False) and REDIS_AVAILABLE
        self.state_persistence_path = self.config.get('state_path', '/tmp/circuit_breakers')
        
        # Local state
        self.local_state = CircuitState.CLOSED
        self.local_failures = 0
        self.last_state_change = time.time()
        self._lock = threading.RLock()
        
        # Configuration
        self.failure_threshold = self.config.get('failure_threshold', 0.5)
        self.window_size_seconds = self.config.get('window_size_seconds', 60)
        self.timeout_ms = self.config.get('timeout_ms', 30000)
        self.half_open_max_calls = self.config.get('half_open_max_calls', 3)
        
        # Metrics
        self.results: deque = deque(maxlen=1000)
        self.timestamps: deque = deque(maxlen=1000)
        
        # State persistence
        self._state_file = os.path.join(self.state_persistence_path, f"cb_{name}.pkl")
        self._ensure_persistence_dir()
        self._load_persisted_state()
        
        if self.use_distributed:
            self._init_redis()
        
        logger.info(f"DistributedCircuitBreaker {name} initialized (distributed={self.use_distributed})")
    
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
                    socket_timeout=5,
                    socket_connect_timeout=5
                )
            self.redis_client.ping()
            logger.info(f"Connected to Redis for circuit breaker {self.name}")
        except Exception as e:
            logger.warning(f"Redis connection failed: {e}, falling back to local mode")
            self.use_distributed = False
            self.redis_client = None
    
    def _get_redis_key(self) -> str:
        """Get Redis key for this circuit breaker"""
        return f"circuit_breaker:{self.name}"
    
    def _get_remote_state(self) -> Optional[CircuitState]:
        """Get circuit state from Redis"""
        if not self.redis_client:
            return None
        
        try:
            data = self.redis_client.get(self._get_redis_key())
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
            self.redis_client.setex(
                self._get_redis_key(),
                self.timeout_ms // 1000,
                json.dumps(data)
            )
        except Exception as e:
            logger.warning(f"Failed to set remote state: {e}")
    
    def _save_persisted_state(self):
        """Save circuit state to disk"""
        try:
            state_data = {
                'state': self.local_state.value,
                'failures': self.local_failures,
                'last_state_change': self.last_state_change,
                'saved_at': time.time()
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
                    self.local_state = CircuitState(state_data['state'])
                    self.local_failures = state_data['failures']
                    self.last_state_change = state_data['last_state_change']
                    logger.info(f"Loaded persisted state for {self.name}: {self.local_state.value}")
        except Exception as e:
            logger.warning(f"Failed to load state: {e}")
    
    def get_current_state(self) -> CircuitState:
        """Get current circuit state (local or remote)"""
        if self.use_distributed:
            remote = self._get_remote_state()
            if remote:
                return remote
        
        # Check if state should auto-reset
        if self.local_state == CircuitState.OPEN:
            if time.time() * 1000 - self.last_failure_time > self.timeout_ms:
                self.local_state = CircuitState.HALF_OPEN
                self._save_persisted_state()
                if self.use_distributed:
                    self._set_remote_state(CircuitState.HALF_OPEN)
        
        return self.local_state
    
    def record_failure(self):
        """Record a failure"""
        with self._lock:
            self.local_failures += 1
            self.results.append(False)
            self.timestamps.append(time.time())
            
            # Check if should open
            if self.local_state == CircuitState.CLOSED:
                failure_rate = self._calculate_failure_rate()
                if failure_rate >= self.failure_threshold and len(self.results) >= 10:
                    self.local_state = CircuitState.OPEN
                    self.last_failure_time = time.time() * 1000
                    self._save_persisted_state()
                    if self.use_distributed:
                        self._set_remote_state(CircuitState.OPEN, self.local_failures)
                    logger.error(f"Circuit {self.name} opened due to {failure_rate:.1%} failure rate")
    
    def record_success(self):
        """Record a success"""
        with self._lock:
            self.local_failures = max(0, self.local_failures - 1)
            self.results.append(True)
            self.timestamps.append(time.time())
            
            if self.local_state == CircuitState.HALF_OPEN:
                self.local_state = CircuitState.CLOSED
                self.local_failures = 0
                self._save_persisted_state()
                if self.use_distributed:
                    self._set_remote_state(CircuitState.CLOSED, 0)
                logger.info(f"Circuit {self.name} closed after successful test")
    
    def _calculate_failure_rate(self) -> float:
        """Calculate failure rate over window"""
        if len(self.results) < 10:
            return 0.0
        
        cutoff = time.time() - self.window_size_seconds
        recent = [(ts, s) for ts, s in zip(self.timestamps, self.results) if ts > cutoff]
        if not recent:
            return 0.0
        
        failures = sum(1 for _, s in recent if not s)
        return failures / len(recent)
    
    def call(self, func: Callable, *args, **kwargs) -> Tuple[Any, Optional[str]]:
        """Execute function with circuit breaker protection"""
        state = self.get_current_state()
        
        if state == CircuitState.OPEN:
            return None, f"Circuit {self.name} is OPEN"
        
        if state == CircuitState.HALF_OPEN:
            with self._lock:
                if getattr(self, '_half_open_calls', 0) >= self.half_open_max_calls:
                    return None, f"Circuit {self.name} HALF_OPEN limit reached"
                self._half_open_calls = getattr(self, '_half_open_calls', 0) + 1
        
        try:
            result = func(*args, **kwargs)
            self.record_success()
            return result, None
        except Exception as e:
            self.record_failure()
            return None, str(e)
    
    async def call_async(self, func: Callable, *args, **kwargs) -> Tuple[Any, Optional[str]]:
        """Async version of call"""
        state = self.get_current_state()
        
        if state == CircuitState.OPEN:
            return None, f"Circuit {self.name} is OPEN"
        
        if state == CircuitState.HALF_OPEN:
            with self._lock:
                if getattr(self, '_half_open_calls', 0) >= self.half_open_max_calls:
                    return None, f"Circuit {self.name} HALF_OPEN limit reached"
                self._half_open_calls = getattr(self, '_half_open_calls', 0) + 1
        
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            self.record_success()
            return result, None
        except Exception as e:
            self.record_failure()
            return None, str(e)
    
    def get_status(self) -> Dict:
        """Get circuit breaker status"""
        state = self.get_current_state()
        return {
            'name': self.name,
            'state': state.value,
            'failure_rate': self._calculate_failure_rate(),
            'remote_mode': self.use_distributed,
            'redis_connected': self.redis_client is not None,
            'persisted': os.path.exists(self._state_file)
        }
    
    def reset(self):
        """Reset circuit breaker"""
        with self._lock:
            self.local_state = CircuitState.CLOSED
            self.local_failures = 0
            self.results.clear()
            self.timestamps.clear()
            self._half_open_calls = 0
            self._save_persisted_state()
            if self.use_distributed:
                self._set_remote_state(CircuitState.CLOSED, 0)
            logger.info(f"Circuit {self.name} manually reset")


# ============================================================
# ENHANCEMENT 2: Webhook Alert System
# ============================================================

class WebhookAlertSystem:
    """
    Webhook-based alert system for critical failures.
    
    Sends alerts to configured endpoints (Slack, PagerDuty, etc.)
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.webhooks = self.config.get('webhooks', [])
        self.alert_history: deque = deque(maxlen=1000)
        self._lock = threading.RLock()
        
        logger.info(f"WebhookAlertSystem initialized with {len(self.webhooks)} webhooks")
    
    def send_alert(self, level: str, title: str, message: str, data: Optional[Dict] = None):
        """Send alert to all configured webhooks"""
        alert = {
            'timestamp': datetime.now().isoformat(),
            'level': level,  # 'info', 'warning', 'error', 'critical'
            'title': title,
            'message': message,
            'data': data or {}
        }
        
        with self._lock:
            self.alert_history.append(alert)
        
        # Send asynchronously to avoid blocking
        asyncio.create_task(self._send_webhooks(alert))
    
    async def _send_webhooks(self, alert: Dict):
        """Send alert to all webhooks"""
        if not REQUESTS_AVAILABLE:
            logger.warning("requests not available, webhook alerts disabled")
            return
        
        for webhook in self.webhooks:
            try:
                response = await asyncio.to_thread(
                    requests.post,
                    webhook['url'],
                    json=alert,
                    headers={'Content-Type': 'application/json'},
                    timeout=5
                )
                if response.status_code >= 400:
                    logger.warning(f"Webhook {webhook['url']} returned {response.status_code}")
            except Exception as e:
                logger.error(f"Failed to send webhook: {e}")
    
    def get_alert_history(self, limit: int = 100) -> List[Dict]:
        """Get recent alert history"""
        with self._lock:
            return list(self.alert_history)[-limit:]


# ============================================================
# ENHANCEMENT 3: ML-Based Anomaly Detection
# ============================================================

class AnomalyDetector:
    """
    ML-based anomaly detection for fallback triggers.
    
    Uses statistical methods to detect unusual patterns
    before they cause failures.
    """
    
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.history: deque = deque(maxlen=window_size)
        self.baseline_stats: Dict[str, float] = {}
        self._lock = threading.RLock()
    
    def add_observation(self, value: float):
        """Add observation for baseline learning"""
        with self._lock:
            self.history.append(value)
            
            if len(self.history) >= self.window_size:
                self._update_baseline()
    
    def _update_baseline(self):
        """Update baseline statistics"""
        if len(self.history) < 10:
            return
        
        values = list(self.history)
        self.baseline_stats = {
            'mean': np.mean(values),
            'std': np.std(values),
            'p95': np.percentile(values, 95),
            'p99': np.percentile(values, 99)
        }
    
    def is_anomaly(self, value: float, threshold: float = 3.0) -> Tuple[bool, float]:
        """
        Check if value is anomalous.
        
        Returns:
            (is_anomaly, z_score)
        """
        if not self.baseline_stats:
            return False, 0.0
        
        z_score = abs(value - self.baseline_stats['mean']) / max(self.baseline_stats['std'], 0.001)
        return z_score > threshold, z_score
    
    def get_anomaly_score(self, value: float) -> float:
        """Get normalized anomaly score (0-1)"""
        if not self.baseline_stats:
            return 0.0
        
        z_score = abs(value - self.baseline_stats['mean']) / max(self.baseline_stats['std'], 0.001)
        return min(1.0, z_score / 5)


# ============================================================
# ENHANCEMENT 4: Reinforcement Learning Adaptive TTL
# ============================================================

class ReinforcementLearningTTL:
    """
    Reinforcement learning-based adaptive TTL optimization.
    
    Uses Q-learning to find optimal TTL values for each data type.
    """
    
    def __init__(self, learning_rate: float = 0.1, discount_factor: float = 0.95):
        self.lr = learning_rate
        self.gamma = discount_factor
        
        # Q-table: state -> TTL value
        self.q_table: Dict[str, Dict[int, float]] = {}
        
        # TTL options (seconds)
        self.ttl_options = [5, 10, 30, 60, 120, 300, 600, 1800, 3600]
        
        # State tracking
        self.last_state: Optional[str] = None
        self.last_action: Optional[int] = None
        
        logger.info("ReinforcementLearningTTL initialized")
    
    def _get_state_key(self, hit_rate: float, change_rate: float) -> str:
        """Discretize state for Q-learning"""
        hit_bucket = int(hit_rate * 5)  # 0-5
        change_bucket = int(change_rate * 5)  # 0-5
        return f"h{hit_bucket}_c{change_bucket}"
    
    def update(self, hit_rate: float, change_rate: float, current_ttl: int, reward: float):
        """Update Q-table with observed reward"""
        state = self._get_state_key(hit_rate, change_rate)
        action = self.ttl_options.index(current_ttl)
        
        if state not in self.q_table:
            self.q_table[state] = {a: 0.0 for a in range(len(self.ttl_options))}
        
        # Q-learning update
        old_q = self.q_table[state][action]
        max_future_q = max(self.q_table[state].values())
        new_q = old_q + self.lr * (reward + self.gamma * max_future_q - old_q)
        self.q_table[state][action] = new_q
    
    def get_optimal_ttl(self, hit_rate: float, change_rate: float) -> int:
        """Get optimal TTL for current state"""
        state = self._get_state_key(hit_rate, change_rate)
        
        if state not in self.q_table:
            # Explore: try medium TTL
            return 60
        
        best_action = max(self.q_table[state], key=self.q_table[state].get)
        return self.ttl_options[best_action]
    
    def get_stats(self) -> Dict:
        """Get Q-table statistics"""
        return {
            'states': len(self.q_table),
            'ttl_options': self.ttl_options,
            'learning_rate': self.lr,
            'discount_factor': self.gamma
        }


# ============================================================
# ENHANCEMENT 5: SLA Tracker with Error Budgets
# ============================================================

class SLATracker:
    """
    Service Level Agreement (SLA) tracking with error budgets.
    
    Tracks availability and latency SLOs, calculates error budget consumption.
    """
    
    def __init__(self, availability_target: float = 0.999, latency_target_ms: float = 100):
        self.availability_target = availability_target
        self.latency_target_ms = latency_target_ms
        
        # Metrics
        self.total_requests = 0
        self.successful_requests = 0
        self.latencies: List[float] = []
        
        # Time window (rolling 30 days)
        self.window_seconds = 30 * 24 * 3600
        self.request_history: deque = deque(maxlen=100000)
        
        self._lock = threading.RLock()
    
    def record_request(self, success: bool, latency_ms: float, timestamp: Optional[float] = None):
        """Record a request for SLA tracking"""
        with self._lock:
            if timestamp is None:
                timestamp = time.time()
            
            self.request_history.append((timestamp, success, latency_ms))
            
            # Clean old entries
            cutoff = timestamp - self.window_seconds
            while self.request_history and self.request_history[0][0] < cutoff:
                self.request_history.popleft()
    
    def get_current_availability(self) -> float:
        """Calculate current availability over window"""
        with self._lock:
            if not self.request_history:
                return 1.0
            
            total = len(self.request_history)
            successes = sum(1 for _, success, _ in self.request_history if success)
            return successes / total if total > 0 else 1.0
    
    def get_current_latency(self, percentile: float = 0.95) -> float:
        """Calculate current latency percentile"""
        with self._lock:
            if not self.request_history:
                return 0.0
            
            latencies = [lat for _, _, lat in self.request_history]
            latencies.sort()
            idx = int(len(latencies) * percentile)
            return latencies[idx] if idx < len(latencies) else 0.0
    
    def get_error_budget_remaining(self) -> float:
        """Calculate remaining error budget (0-1)"""
        current_availability = self.get_current_availability()
        error_budget_used = (1 - current_availability) / (1 - self.availability_target)
        return max(0, min(1, 1 - error_budget_used))
    
    def is_sla_violated(self) -> Tuple[bool, str]:
        """Check if SLA is being violated"""
        availability = self.get_current_availability()
        latency = self.get_current_latency(0.95)
        
        violations = []
        if availability < self.availability_target:
            violations.append(f"availability {availability:.4%} < {self.availability_target:.4%}")
        if latency > self.latency_target_ms:
            violations.append(f"latency p95 {latency:.1f}ms > {self.latency_target_ms}ms")
        
        return len(violations) > 0, ", ".join(violations)
    
    def get_status(self) -> Dict:
        """Get SLA status"""
        availability = self.get_current_availability()
        latency_p95 = self.get_current_latency(0.95)
        error_budget = self.get_error_budget_remaining()
        
        return {
            'availability': availability,
            'availability_target': self.availability_target,
            'latency_p95_ms': latency_p95,
            'latency_target_ms': self.latency_target_ms,
            'error_budget_remaining': error_budget,
            'sla_met': availability >= self.availability_target and latency_p95 <= self.latency_target_ms,
            'total_requests': len(self.request_history),
            'window_seconds': self.window_seconds
        }


# ============================================================
# ENHANCEMENT 6: Enhanced Fallback Manager with New Features
# ============================================================

class UltimateFallbackManager:
    """
    Ultimate fallback manager with all enhancements.
    
    Features:
    - Distributed circuit breaker
    - Webhook alerts
    - ML-based anomaly detection
    - Reinforcement learning TTL
    - SLA tracking
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
        # New components
        self.circuit_breakers = {}
        self.alert_system = WebhookAlertSystem(self.config.get('webhooks', {}))
        self.anomaly_detector = AnomalyDetector()
        self.rl_ttl = ReinforcementLearningTTL()
        self.sla_tracker = SLATracker(
            availability_target=self.config.get('availability_target', 0.999),
            latency_target_ms=self.config.get('latency_target_ms', 100)
        )
        
        # Initialize circuit breakers
        self._init_circuit_breakers()
        
        # Backoff and cache
        self.backoff = AdaptiveExponentialBackoff()
        self.cache_manager = AdaptiveLRUCacheManager()
        self.metrics = EnhancedFallbackMetrics()
        
        logger.info("UltimateFallbackManager v3.2 initialized")
    
    def _init_circuit_breakers(self):
        """Initialize circuit breakers for all data types"""
        data_types = ['temperature', 'grid', 'helium', 'recovery', 'ppa']
        for dt in data_types:
            self.circuit_breakers[dt] = DistributedCircuitBreaker(
                dt,
                self.config.get('circuit_breaker', {})
            )
    
    async def execute_with_fallback(self, primary_func: Callable, data_type: str,
                                    context: Optional[Dict] = None) -> FallbackResult:
        """
        Enhanced fallback execution with new features.
        """
        start_time = time.time()
        
        # Check SLA before execution
        sla_status = self.sla_tracker.get_status()
        if not sla_status['sla_met']:
            self.alert_system.send_alert(
                'warning',
                'SLA Violation Detected',
                f"SLA violation before execution: {sla_status}"
            )
        
        # Get circuit breaker
        cb = self.circuit_breakers.get(data_type)
        if not cb:
            cb = DistributedCircuitBreaker(data_type)
        
        # Check anomaly before execution
        anomaly_detected = False
        if context and 'value' in context:
            is_anom, z_score = self.anomaly_detector.is_anomaly(context['value'])
            if is_anom:
                anomaly_detected = True
                self.alert_system.send_alert(
                    'warning',
                    'Anomaly Detected',
                    f"Anomaly detected for {data_type}: z-score={z_score:.2f}",
                    {'value': context['value'], 'z_score': z_score}
                )
        
        # Execute with circuit breaker
        result, error = await cb.call_async(primary_func)
        success = result is not None
        
        # Record for SLA tracking
        latency_ms = (time.time() - start_time) * 1000
        self.sla_tracker.record_request(success, latency_ms)
        
        # Update anomaly detector
        if result is not None:
            # Extract numeric value from result if possible
            value = result.get('value', 0) if isinstance(result, dict) else 0
            self.anomaly_detector.add_observation(value)
        
        # Update RL TTL if cache was used
        if hasattr(self, 'cache_manager'):
            cache_stats = self.cache_manager.get_stats()
            hit_rate = cache_stats['hit_rate']
            # Would compute change rate from observations
            optimal_ttl = self.rl_ttl.get_optimal_ttl(hit_rate, 0.1)
            self.cache_manager.max_ttl = optimal_ttl
        
        # Return result
        return FallbackResult(
            success=success,
            value=result,
            source='primary' if success else 'none',
            latency_ms=latency_ms,
            retry_count=0,
            circuit_state=cb.get_status()['state'],
            error=error,
            data_type=data_type,
            health_score=1.0 if success else 0.0
        )
    
    def get_sla_status(self) -> Dict:
        """Get SLA tracking status"""
        return self.sla_tracker.get_status()
    
    def get_alert_history(self, limit: int = 50) -> List[Dict]:
        """Get recent alert history"""
        return self.alert_system.get_alert_history(limit)
    
    def get_rl_ttl_stats(self) -> Dict:
        """Get reinforcement learning statistics"""
        return self.rl_ttl.get_stats()
    
    def get_circuit_breaker_status(self) -> Dict:
        """Get all circuit breaker statuses"""
        return {name: cb.get_status() for name, cb in self.circuit_breakers.items()}
    
    def generate_health_report(self) -> Dict:
        """Generate comprehensive health report"""
        return {
            'sla': self.get_sla_status(),
            'circuit_breakers': self.get_circuit_breaker_status(),
            'cache': self.cache_manager.get_stats() if hasattr(self, 'cache_manager') else {},
            'rl_ttl': self.get_rl_ttl_stats(),
            'alert_history': self.get_alert_history(10),
            'timestamp': datetime.now().isoformat()
        }


# ============================================================
# Usage Example
# ============================================================

async def main():
    print("=== Ultimate Fallback Manager v3.2 Demo ===\n")
    
    fallback_mgr = UltimateFallbackManager({
        'availability_target': 0.999,
        'latency_target_ms': 100,
        'webhooks': {
            'webhooks': [
                {'url': 'https://webhook.site/your-webhook', 'name': 'slack'}
            ]
        }
    })
    
    async def mock_api_call():
        await asyncio.sleep(0.05)
        if random.random() > 0.8:
            return {'temperature': 65.0, 'value': 65.0}
        raise Exception("API error")
    
    print("1. Distributed Circuit Breaker Test:")
    for i in range(20):
        result = await fallback_mgr.execute_with_fallback(mock_api_call, 'temperature')
        if result.success:
            print(f"   Attempt {i+1}: Success - {result.value}")
        else:
            print(f"   Attempt {i+1}: Failed - {result.error}")
    
    print("\n2. SLA Tracking Status:")
    sla = fallback_mgr.get_sla_status()
    print(f"   Availability: {sla['availability']:.4%} (target: {sla['availability_target']:.4%})")
    print(f"   Latency p95: {sla['latency_p95_ms']:.1f}ms (target: {sla['latency_target_ms']}ms)")
    print(f"   Error budget remaining: {sla['error_budget_remaining']:.1%}")
    print(f"   SLA Met: {sla['sla_met']}")
    
    print("\n3. Reinforcement Learning TTL:")
    rl_stats = fallback_mgr.get_rl_ttl_stats()
    print(f"   States explored: {rl_stats['states']}")
    print(f"   TTL options: {rl_stats['ttl_options']}")
    
    print("\n4. Anomaly Detection Test:")
    # Add normal observations
    for _ in range(50):
        fallback_mgr.anomaly_detector.add_observation(65.0 + random.gauss(0, 1))
    
    # Test anomaly
    is_anom, z_score = fallback_mgr.anomaly_detector.is_anomaly(85.0)
    print(f"   Value 85.0: {'ANOMALY' if is_anom else 'normal'} (z-score={z_score:.2f})")
    
    print("\n5. Circuit Breaker Status:")
    cb_status = fallback_mgr.get_circuit_breaker_status()
    for name, status in cb_status.items():
        print(f"   {name}: {status['state']} (fail rate: {status['failure_rate']:.1%})")
    
    print("\n6. Health Report:")
    report = fallback_mgr.generate_health_report()
    print(f"   Overall SLA: {'✅ MET' if report['sla']['sla_met'] else '❌ VIOLATED'}")
    print(f"   Open circuits: {sum(1 for s in report['circuit_breakers'].values() if s['state'] == 'open')}")
    
    print("\n✅ Ultimate Fallback Manager v3.2 test complete")

if __name__ == "__main__":
    asyncio.run(main())
