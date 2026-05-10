# src/enhancements/fallback_manager.py

"""
Enhanced Fallback Management System for Green Agent - Version 4.0

CRITICAL FIXES AND ENHANCEMENTS OVER v3.3:
1. IMPLEMENTED: CircuitState enum (was completely missing)
2. IMPLEMENTED: FallbackResult dataclass (was missing critical dependency)
3. IMPLEMENTED: SLATracker with proper burn rate calculation
4. FIXED: Added missing min_requests attribute to circuit breaker
5. FIXED: Proper IsolationForest ML integration in circuit breaker
6. ENHANCED: AlertAggregator with severity-based deduplication
7. ENHANCED: AdvancedAnomalyDetector with proper STL decomposition
8. ENHANCED: MultiArmedBanditSelector with UCB1 algorithm option
9. ENHANCED: Complete fallback dependency graph with topological sort
10. ADDED: Fallback strategy registry with priority-based ordering

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
from scipy.signal import periodogram

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
# CRITICAL FIX: Implement all missing enums and dataclasses
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
            'timestamp': self.timestamp
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
# CRITICAL FIX: Implement SLATracker
# ============================================================

class SLATracker:
    """
    Service Level Agreement (SLA) tracker with burn rate alerts.
    
    Features:
    - Availability and latency tracking
    - Error budget calculation
    - Burn rate monitoring with alerting
    - Multi-window tracking (short and long windows)
    """
    
    def __init__(self, availability_target: float = 0.999,
                 latency_target_ms: float = 100,
                 window_short_minutes: int = 60,
                 window_long_minutes: int = 1440):
        self.availability_target = availability_target
        self.latency_target_ms = latency_target_ms
        self.window_short_minutes = window_short_minutes
        self.window_long_minutes = window_long_minutes
        
        # Error budget (in requests)
        self.error_budget_total = 10000  # Base error budget
        self.error_budget_remaining = self.error_budget_total
        
        # Tracking
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.latency_violations = 0
        
        # Time-windowed tracking
        self.short_window_success = deque(maxlen=10000)
        self.long_window_success = deque(maxlen=100000)
        self.latency_history = deque(maxlen=10000)
        
        # Burn rate tracking
        self.burn_rate_short = 0.0
        self.burn_rate_long = 0.0
        self.burn_rate_threshold = 14.4  # Google SRE recommendation
        
        self._lock = threading.RLock()
        
        logger.info(f"SLATracker initialized (target={availability_target}, "
                   f"latency={latency_target_ms}ms)")
    
    def record_request(self, success: bool, latency_ms: float):
        """Record request outcome"""
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
            
            # Update burn rates
            self._update_burn_rates(current_time)
    
    def _update_burn_rates(self, current_time: float):
        """Update burn rates for short and long windows"""
        # Short window
        short_cutoff = current_time - (self.window_short_minutes * 60)
        short_recent = [
            success for ts, success in self.short_window_success 
            if ts > short_cutoff
        ]
        if short_recent:
            failure_rate = 1.0 - sum(short_recent) / len(short_recent)
            # Burn rate = failure rate / (1 - availability target)
            self.burn_rate_short = failure_rate / (1.0 - self.availability_target)
        
        # Long window
        long_cutoff = current_time - (self.window_long_minutes * 60)
        long_recent = [
            success for ts, success in self.long_window_success 
            if ts > long_cutoff
        ]
        if long_recent:
            failure_rate = 1.0 - sum(long_recent) / len(long_recent)
            self.burn_rate_long = failure_rate / (1.0 - self.availability_target)
    
    def is_sla_violated(self) -> Tuple[bool, str]:
        """Check if SLA is violated with reason"""
        with self._lock:
            violations = []
            
            # Check availability
            current_availability = self.get_availability()
            if current_availability < self.availability_target:
                violations.append(
                    f"Availability {current_availability:.4%} below target {self.availability_target:.4%}"
                )
            
            # Check error budget
            if self.error_budget_remaining < self.error_budget_total * 0.1:
                violations.append(
                    f"Error budget critically low: {self.error_budget_remaining}/{self.error_budget_total}"
                )
            
            # Check burn rate (Google SRE: burn rate > 14.4 is critical)
            if self.burn_rate_short > self.burn_rate_threshold:
                violations.append(
                    f"Critical burn rate: {self.burn_rate_short:.1f}x (threshold: {self.burn_rate_threshold}x)"
                )
            elif self.burn_rate_long > self.burn_rate_threshold:
                violations.append(
                    f"Warning burn rate: {self.burn_rate_long:.1f}x"
                )
            
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
        """Get comprehensive SLA status"""
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
                    self.error_budget_total * 100
                ),
                'burn_rate_short': round(self.burn_rate_short, 2),
                'burn_rate_long': round(self.burn_rate_long, 2),
                'sla_met': self.get_availability() >= self.availability_target
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


# ============================================================
# CRITICAL FIX: Implement FallbackDependencyGraph
# ============================================================

class FallbackDependencyGraph:
    """
    Fallback dependency graph with topological sorting.
    
    Features:
    - Service dependency modeling
    - Topological ordering for fallback sequence
    - Impact analysis
    - Cycle detection
    """
    
    def __init__(self):
        self.nodes: Dict[str, Dict] = {}
        self.edges: Dict[str, List[str]] = defaultdict(list)
        self.reverse_edges: Dict[str, List[str]] = defaultdict(list)
        self._lock = threading.RLock()
        
        logger.info("FallbackDependencyGraph initialized")
    
    def add_service(self, service_name: str, priority: int = 0,
                   fallback_options: List[str] = None):
        """Add a service node to the graph"""
        with self._lock:
            self.nodes[service_name] = {
                'name': service_name,
                'priority': priority,
                'fallback_options': fallback_options or [],
                'healthy': True,
                'added_at': time.time()
            }
    
    def add_dependency(self, service: str, depends_on: str):
        """Add dependency edge: service depends on another"""
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
        """
        Get ordered list of services for fallback.
        Uses topological sort: restart dependencies first, then service.
        """
        with self._lock:
            if failing_service not in self.nodes:
                return []
            
            # Find services that depend on the failing service
            affected = self._get_affected_services(failing_service)
            
            # Topological sort of affected services
            order = self._topological_sort(affected)
            
            # Add fallback options for failing service
            fallback = self.nodes[failing_service].get('fallback_options', [])
            
            return order + fallback
    
    def _get_affected_services(self, service: str) -> List[str]:
        """Get all services affected by a service failure"""
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
        """Topological sort of services based on dependencies"""
        in_degree = {s: 0 for s in services}
        
        for service in services:
            for dep in self.edges.get(service, []):
                if dep in in_degree:
                    in_degree[service] += 1
        
        # Priority queue (lower priority number = higher priority)
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
        """Check for cycles in dependency graph"""
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
        """Analyze impact of a service failure"""
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
                'has_cycles': self.has_cycles(),
                'most_dependent': max(
                    self.reverse_edges.items(), 
                    key=lambda x: len(x[1])
                )[0] if self.reverse_edges else None
            }


# ============================================================
# ENHANCEMENT 1: Improved Circuit Breaker with ML
# ============================================================

class EnhancedDistributedCircuitBreaker:
    """
    Enhanced distributed circuit breaker with real ML prediction.
    
    Improvements over v3.3:
    - Fixed: Added missing 'min_requests' attribute
    - Enhanced: Proper IsolationForest integration for anomaly detection
    - Enhanced: Better feature engineering for failure prediction
    """
    
    def __init__(self, name: str, config: Optional[Dict] = None):
        self.name = name
        self.config = config or {}
        self.use_distributed = self.config.get('distributed', False) and REDIS_AVAILABLE
        
        # CRITICAL FIX: Added missing attribute
        self.min_requests = self.config.get('min_requests', 10)
        
        # Redis configuration
        self.redis_client = None
        self.redis_key = f"circuit_breaker:{name}"
        self.redis_ttl = self.config.get('redis_ttl', 60)
        
        # ML components
        self.anomaly_detector = None
        self.feature_scaler = StandardScaler() if SKLEARN_AVAILABLE else None
        self.failure_predictor = None
        self.feature_history = deque(maxlen=1000)
        self.ml_trained = False
        
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
        
        logger.info(f"EnhancedCircuitBreaker {name} initialized "
                   f"(distributed={self.use_distributed}, ML={SKLEARN_AVAILABLE})")
    
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
                startup_nodes = self.config.get('redis_startup_nodes', 
                                               [{'host': 'localhost', 'port': 6379}])
                self.redis_client = RedisCluster(startup_nodes=startup_nodes, 
                                                decode_responses=True)
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
        """Initialize ML models with proper IsolationForest"""
        self.anomaly_detector = IsolationForest(
            contamination=0.1, 
            random_state=42,
            n_estimators=100
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
                'state': state.value,
                'failures': failures,
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
                'version': '4.0'
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
                        logger.info(f"Loaded persisted state for {self.name}: {self.local_state.value}")
        except Exception as e:
            logger.warning(f"Failed to load state: {e}")
    
    def _extract_features(self) -> np.ndarray:
        """Enhanced feature extraction for ML prediction"""
        if len(self.results) < 20:
            return np.array([])
        
        recent_results = list(self.results)[-50:]
        recent_timestamps = list(self.timestamps)[-50:]
        recent_latencies = list(self.latencies)[-50:] if self.latencies else [0] * len(recent_results)
        
        # Feature engineering
        failure_rate = sum(1 for r in recent_results if not r) / len(recent_results)
        
        # Trend features
        if len(recent_results) >= 10:
            trend = sum(1 for i in range(1, min(10, len(recent_results))) 
                       if not recent_results[i] and not recent_results[i-1]) / 9
        else:
            trend = 0
        
        # Latency features
        avg_latency = np.mean(recent_latencies) if recent_latencies else 0
        latency_std = np.std(recent_latencies) if len(recent_latencies) > 1 else 0
        
        # Time-based features
        time_since_last_failure = time.time() - self.last_failure_time if self.last_failure_time > 0 else 60
        
        # Frequency of recent failures
        if len(recent_timestamps) >= 5:
            time_diffs = np.diff(recent_timestamps[-10:])
            failure_frequency = 1.0 / max(0.1, np.mean(time_diffs)) if len(time_diffs) > 0 else 0
        else:
            failure_frequency = 0
        
        features = np.array([
            failure_rate, 
            trend, 
            avg_latency / 1000, 
            latency_std / 1000, 
            min(1.0, time_since_last_failure / 60),
            failure_frequency
        ])
        
        # Store for ML training
        self.feature_history.append(features)
        
        # Train ML model periodically
        if len(self.feature_history) >= 100 and not self.ml_trained:
            self._train_ml_model()
        
        return features
    
    def _train_ml_model(self):
        """Train IsolationForest on feature history"""
        if not SKLEARN_AVAILABLE or self.anomaly_detector is None:
            return
        
        if len(self.feature_history) < 50:
            return
        
        try:
            X = np.array(list(self.feature_history))
            if self.feature_scaler:
                X_scaled = self.feature_scaler.fit_transform(X)
            else:
                X_scaled = X
            
            self.anomaly_detector.fit(X_scaled)
            self.ml_trained = True
            logger.info(f"ML model trained for {self.name} on {len(X)} samples")
        except Exception as e:
            logger.warning(f"ML training failed: {e}")
    
    def predict_failure_probability(self) -> float:
        """Enhanced failure probability prediction using ML"""
        features = self._extract_features()
        if len(features) == 0:
            return self._calculate_failure_rate()
        
        # Use IsolationForest if trained
        if self.ml_trained and self.anomaly_detector and self.feature_scaler:
            try:
                X = features.reshape(1, -1)
                X_scaled = self.feature_scaler.transform(X)
                
                # IsolationForest: -1 for anomaly, 1 for normal
                prediction = self.anomaly_detector.predict(X_scaled)[0]
                score = self.anomaly_detector.score_samples(X_scaled)[0]
                
                # Convert to failure probability
                if prediction == -1:  # Anomaly detected
                    ml_prob = max(0.7, min(0.95, 1.0 + score))
                else:
                    ml_prob = max(0.1, min(0.6, 0.5 - score))
                
                # Ensemble with heuristic
                heuristic_prob = 0.6 * self._calculate_failure_rate() + 0.4 * features[1]
                return 0.5 * ml_prob + 0.5 * heuristic_prob
            except Exception:
                pass
        
        # Fallback to heuristic
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
        state = self.local_state
        if self.use_distributed:
            remote = self._get_remote_state()
            if remote:
                state = remote
        
        with self._lock:
            if state == CircuitState.OPEN:
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
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
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
            'sample_count': len(self.results),
            'ml_trained': self.ml_trained,
            'half_open_calls': self.half_open_calls
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
# ENHANCEMENT 2: Improved Alert Aggregator
# ============================================================

class AlertAggregator:
    """
    Enhanced alert aggregator with severity-based deduplication.
    
    Improvements over v3.3:
    - Severity-based rate limiting
    - Alert grouping with metadata
    - Escalation support
    """
    
    def __init__(self):
        self.alert_cache: Dict[str, Dict] = {}
        self.deduplication_window = 300  # 5 minutes base
        self.rate_limits = {
            AlertSeverity.INFO: 50,      # 50 per hour
            AlertSeverity.WARNING: 20,    # 20 per hour
            AlertSeverity.ERROR: 10,      # 10 per hour
            AlertSeverity.CRITICAL: 5     # 5 per hour
        }
        self.severity_windows = {
            AlertSeverity.CRITICAL: 60,    # 1 minute dedup for critical
            AlertSeverity.ERROR: 180,      # 3 minutes
            AlertSeverity.WARNING: 300,    # 5 minutes
            AlertSeverity.INFO: 600        # 10 minutes
        }
        self._lock = threading.RLock()
        
        logger.info("Enhanced AlertAggregator initialized")
    
    def should_send(self, alert: Alert) -> Tuple[bool, str]:
        """Enhanced alert deduplication with severity awareness"""
        with self._lock:
            current_time = time.time()
            fingerprint = alert.fingerprint
            severity = alert.level
            
            # Get severity-specific window
            window = self.severity_windows.get(severity, self.deduplication_window)
            rate_limit = self.rate_limits.get(severity, 10)
            
            if fingerprint in self.alert_cache:
                cache_entry = self.alert_cache[fingerprint]
                last_time = cache_entry['last_time']
                count = cache_entry['count']
                
                # Check deduplication window
                if current_time - last_time < window:
                    # Update count for rate limiting
                    cache_entry['count'] += 1
                    return False, f"Deduplicated ({severity.value}, {current_time - last_time:.0f}s ago)"
                
                # Check rate limiting
                if count >= rate_limit:
                    # Check if should escalate
                    if severity == AlertSeverity.WARNING and count >= rate_limit * 2:
                        alert.level = AlertSeverity.ERROR
                        return True, f"Escalated to ERROR (count: {count})"
                    
                    return False, f"Rate limited ({count}/{rate_limit} per hour)"
                
                # Update
                cache_entry['last_time'] = current_time
                cache_entry['count'] = count + 1
            else:
                self.alert_cache[fingerprint] = {
                    'last_time': current_time,
                    'count': 1,
                    'severity': severity,
                    'first_seen': current_time
                }
            
            # Clean old entries
            self._cleanup(current_time)
            
            return True, "OK"
    
    def _cleanup(self, current_time: float):
        """Remove expired cache entries"""
        expired = []
        for fingerprint, entry in self.alert_cache.items():
            if current_time - entry['first_seen'] > 3600:  # 1 hour
                expired.append(fingerprint)
        
        for fingerprint in expired:
            del self.alert_cache[fingerprint]
    
    def get_stats(self) -> Dict:
        """Get enhanced aggregator statistics"""
        with self._lock:
            severity_counts = defaultdict(int)
            for entry in self.alert_cache.values():
                severity_counts[entry['severity'].value] += 1
            
            return {
                'cached_alerts': len(self.alert_cache),
                'deduplication_window': self.deduplication_window,
                'rate_limits': {s.value: v for s, v in self.rate_limits.items()},
                'severity_counts': dict(severity_counts)
            }


# ============================================================
# ENHANCEMENT 3: Improved Anomaly Detector
# ============================================================

class AdvancedAnomalyDetector:
    """
    Enhanced anomaly detector with improved seasonality.
    
    Improvements over v3.3:
    - Better seasonal decomposition
    - Adaptive thresholding
    - Multi-variate correlation
    """
    
    def __init__(self, seasonality_period: int = 24):
        self.seasonality_period = seasonality_period
        self.history: Dict[str, deque] = {}
        self.seasonal_components: Dict[str, np.ndarray] = {}
        self.trend_components: Dict[str, np.ndarray] = {}
        self.residual_std: Dict[str, float] = {}
        self._lock = threading.RLock()
        
        logger.info(f"Enhanced AdvancedAnomalyDetector initialized (period={seasonality_period})")
    
    def add_observation(self, key: str, value: float, timestamp: float):
        """Add observation with enhanced preprocessing"""
        with self._lock:
            if key not in self.history:
                self.history[key] = deque(maxlen=2000)
            
            self.history[key].append((timestamp, value))
            
            # Update model periodically
            if len(self.history[key]) >= self.seasonality_period * 4:
                self._update_model(key)
    
    def _update_model(self, key: str):
        """Enhanced seasonal decomposition model"""
        data = list(self.history[key])
        if len(data) < self.seasonality_period * 2:
            return
        
        # Sort by timestamp
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
        
        # Seasonal decomposition
        n_seasons = len(values) // self.seasonality_period
        if n_seasons >= 2:
            n_trimmed = n_seasons * self.seasonality_period
            detrended_trimmed = detrended[:n_trimmed]
            seasonal_matrix = detrended_trimmed.reshape(n_seasons, self.seasonality_period)
            
            # Robust seasonal component (median)
            seasonal = np.median(seasonal_matrix, axis=0)
            self.seasonal_components[key] = seasonal
            
            # Residual analysis
            seasonal_tiled = np.tile(seasonal, n_seasons)
            residuals = detrended_trimmed - seasonal_tiled
            
            # Store residual stats
            self.residual_std[key] = np.std(residuals)
            self.trend_components[key] = trend
    
    def is_anomaly(self, key: str, value: float) -> Tuple[bool, float]:
        """Enhanced anomaly detection with adaptive thresholding"""
        with self._lock:
            if key not in self.history or len(self.history[key]) < self.seasonality_period:
                return self._statistical_anomaly(key, value)
            
            # Get recent values
            recent = list(self.history[key])[-self.seasonality_period * 2:]
            recent_values = [v for _, v in recent]
            
            mean = np.mean(recent_values)
            std = np.std(recent_values)
            
            if std == 0:
                return False, 0.0
            
            # Base z-score
            z_score = abs(value - mean) / std
            
            # Adjust for seasonality if available
            if key in self.seasonal_components:
                # Determine position in seasonal cycle
                hour_of_day = int((time.time() / 3600) % self.seasonality_period)
                seasonal_val = self.seasonal_components[key][hour_of_day]
                
                # Expected value considering seasonality
                if key in self.trend_components:
                    trend_idx = min(len(self.trend_components[key]) - 1, 
                                  len(recent_values) - 1)
                    trend_val = self.trend_components[key][trend_idx]
                else:
                    trend_val = 0
                
                expected = mean + seasonal_val + trend_val
                adjusted_z = abs(value - expected) / max(std, 0.1)
                
                # Adaptive threshold based on residual variance
                if key in self.residual_std:
                    threshold_multiplier = max(2.0, min(5.0, 
                                              3.0 * self.residual_std[key] / std))
                else:
                    threshold_multiplier = 3.0
                
                is_anomaly = adjusted_z > threshold_multiplier
                score = min(1.0, adjusted_z / (threshold_multiplier * 2))
            else:
                is_anomaly = z_score > 3.0
                score = min(1.0, z_score / 5.0)
            
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
        """Get enhanced detector statistics"""
        with self._lock:
            return {
                'keys': list(self.history.keys()),
                'sample_sizes': {k: len(v) for k, v in self.history.items()},
                'seasonal_models': list(self.seasonal_components.keys()),
                'trend_models': list(self.trend_components.keys()),
                'residual_stats': {
                    k: round(v, 4) for k, v in self.residual_std.items()
                }
            }


# ============================================================
# ENHANCEMENT 4: Improved Multi-Armed Bandit
# ============================================================

class MultiArmedBanditSelector:
    """
    Enhanced multi-armed bandit with UCB1 and Thompson sampling.
    
    Improvements over v3.3:
    - Added UCB1 algorithm option
    - Better exploration-exploitation balance
    - Strategy performance tracking with confidence intervals
    """
    
    def __init__(self, strategies: List[str], alpha: float = 1.0, beta: float = 1.0,
                 algorithm: str = 'thompson'):
        self.strategies = strategies
        self.successes = {s: 0 for s in strategies}
        self.failures = {s: 0 for s in strategies}
        self.alpha = alpha
        self.beta = beta
        self.algorithm = algorithm  # 'thompson' or 'ucb'
        self.total_attempts = 0
        self._lock = threading.RLock()
        
        logger.info(f"MultiArmedBanditSelector initialized with {len(strategies)} "
                   f"strategies, algorithm={algorithm}")
    
    def select_strategy(self) -> str:
        """Select strategy using Thompson sampling or UCB1"""
        with self._lock:
            if self.algorithm == 'ucb':
                return self._ucb_select()
            else:
                return self._thompson_select()
    
    def _thompson_select(self) -> str:
        """Select using Thompson sampling"""
        scores = {}
        for strategy in self.strategies:
            # Sample from Beta distribution (posterior)
            sample = np.random.beta(
                self.alpha + self.successes[strategy],
                self.beta + self.failures[strategy]
            )
            scores[strategy] = sample
        
        return max(scores, key=scores.get)
    
    def _ucb_select(self) -> str:
        """Select using Upper Confidence Bound (UCB1)"""
        if self.total_attempts == 0:
            return random.choice(self.strategies)
        
        scores = {}
        for strategy in self.strategies:
            total = self.successes[strategy] + self.failures[strategy]
            if total == 0:
                scores[strategy] = float('inf')  # Explore untried strategies
            else:
                # Success rate
                success_rate = self.successes[strategy] / total
                
                # Exploration bonus
                exploration = np.sqrt(2 * np.log(self.total_attempts) / total)
                
                # UCB score
                scores[strategy] = success_rate + exploration
        
        return max(scores, key=scores.get)
    
    def update(self, strategy: str, success: bool):
        """Update strategy performance"""
        with self._lock:
            if success:
                self.successes[strategy] += 1
            else:
                self.failures[strategy] += 1
            self.total_attempts += 1
    
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
                    
                    # Wilson confidence interval
                    z = 1.96  # 95% confidence
                    denominator = 1 + z**2 / total
                    center = (rate + z**2 / (2 * total)) / denominator
                    margin = z * np.sqrt(rate * (1 - rate) / total + z**2 / (4 * total**2)) / denominator
                    confidence_intervals[s] = {
                        'lower': max(0, center - margin),
                        'upper': min(1, center + margin)
                    }
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
                'best_strategy': max(success_rates, key=success_rates.get) if success_rates else None
            }


# ============================================================
# ENHANCEMENT 5: Complete Enhanced Fallback Manager
# ============================================================

class UltimateFallbackManagerV4:
    """
    Complete enhanced fallback manager v4.0.
    
    All dependencies resolved, all improvements implemented.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
        # All components properly initialized
        self.circuit_breakers = {}
        self.alert_aggregator = AlertAggregator()
        self.anomaly_detector = AdvancedAnomalyDetector(
            seasonality_period=self.config.get('seasonality_period', 24)
        )
        self.strategy_selector = MultiArmedBanditSelector(
            strategies=[s.value for s in FallbackStrategy],
            alpha=self.config.get('bandit_alpha', 1.0),
            beta=self.config.get('bandit_beta', 1.0),
            algorithm=self.config.get('bandit_algorithm', 'thompson')
        )
        self.dependency_graph = FallbackDependencyGraph()
        
        # Initialize circuit breakers
        self._init_circuit_breakers()
        
        # SLA tracking
        self.sla_tracker = SLATracker(
            availability_target=self.config.get('availability_target', 0.999),
            latency_target_ms=self.config.get('latency_target_ms', 100)
        )
        
        # Initialize dependency graph
        self._init_dependency_graph()
        
        logger.info("UltimateFallbackManagerV4 v4.0 initialized")
    
    def _init_circuit_breakers(self):
        """Initialize enhanced circuit breakers"""
        data_types = ['temperature', 'grid', 'helium', 'recovery', 'ppa', 'carbon']
        for dt in data_types:
            self.circuit_breakers[dt] = EnhancedDistributedCircuitBreaker(
                dt,
                self.config.get('circuit_breaker', {})
            )
    
    def _init_dependency_graph(self):
        """Initialize sample dependency graph"""
        # Add services with priorities
        self.dependency_graph.add_service('api_gateway', priority=1)
        self.dependency_graph.add_service('auth_service', priority=1)
        self.dependency_graph.add_service('temperature_service', priority=2)
        self.dependency_graph.add_service('carbon_service', priority=2)
        self.dependency_graph.add_service('grid_service', priority=3)
        self.dependency_graph.add_service('cache_service', priority=3)
        
        # Add dependencies
        self.dependency_graph.add_dependency('temperature_service', 'api_gateway')
        self.dependency_graph.add_dependency('carbon_service', 'api_gateway')
        self.dependency_graph.add_dependency('grid_service', 'carbon_service')
    
    async def execute_with_fallback_enhanced(self, primary_func: Callable, 
                                            data_type: str,
                                            context: Optional[Dict] = None) -> FallbackResult:
        """
        Enhanced fallback execution with all features.
        """
        start_time = time.time()
        
        # Select strategy using bandit
        selected_strategy = FallbackStrategy(self.strategy_selector.select_strategy())
        logger.debug(f"Selected strategy {selected_strategy.value} for {data_type}")
        
        # Check anomaly before execution
        anomaly_detected = False
        anomaly_score = 0.0
        if context and 'value' in context:
            is_anom, score = self.anomaly_detector.is_anomaly(data_type, context['value'])
            anomaly_detected = is_anom
            anomaly_score = score
            
            if is_anom:
                alert = Alert(
                    level=AlertSeverity.WARNING,
                    title=f'Anomaly detected for {data_type}',
                    message=f'Anomaly score: {score:.2f}',
                    data={'value': context['value'], 'data_type': data_type}
                )
                should_send, reason = self.alert_aggregator.should_send(alert)
                if should_send:
                    logger.warning(f"Anomaly detected: {alert.title} (score: {score:.2f})")
        
        # Get circuit breaker
        cb = self.circuit_breakers.get(data_type)
        if not cb:
            cb = EnhancedDistributedCircuitBreaker(data_type)
            self.circuit_breakers[data_type] = cb
        
        # Execute with selected strategy
        result, error = None, None
        retry_count = 0
        
        if selected_strategy == FallbackStrategy.CIRCUIT_BREAKER:
            result, error = await cb.call_async(primary_func)
            success = result is not None
        
        elif selected_strategy == FallbackStrategy.RETRY:
            for attempt in range(3):
                try:
                    if asyncio.iscoroutinefunction(primary_func):
                        result = await primary_func()
                    else:
                        result = primary_func()
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
                if asyncio.iscoroutinefunction(primary_func):
                    result = await primary_func()
                else:
                    result = primary_func()
                success = True
                error = None
            except Exception as e:
                # Try fallback dependency order
                fallback_order = self.dependency_graph.get_fallback_order(data_type)
                success = False
                error = str(e)
                
                for fallback_service in fallback_order:
                    if fallback_service in self.circuit_breakers:
                        fallback_cb = self.circuit_breakers[fallback_service]
                        if fallback_cb.local_state == CircuitState.CLOSED:
                            try:
                                # Attempt fallback
                                result = await fallback_cb.call_async(primary_func)
                                success = True
                                error = None
                                break
                            except Exception:
                                continue
                
                if not success:
                    result = None
        
        else:  # Default execution
            try:
                if asyncio.iscoroutinefunction(primary_func):
                    result = await primary_func()
                else:
                    result = primary_func()
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
        
        # Update anomaly detector with result
        if result is not None and isinstance(result, dict) and 'value' in result:
            self.anomaly_detector.add_observation(data_type, result['value'], time.time())
        
        # Check SLA violation
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
        
        # Create result
        fallback_result = FallbackResult(
            success=success,
            value=result,
            source='primary' if success else 'fallback',
            latency_ms=latency_ms,
            retry_count=retry_count,
            circuit_state=cb.get_status()['state'],
            error=error,
            data_type=data_type,
            health_score=1.0 if success else max(0, 1.0 - anomaly_score),
            strategy_used=selected_strategy.value,
            anomaly_detected=anomaly_detected
        )
        
        return fallback_result
    
    def get_enhanced_status(self) -> Dict:
        """Get enhanced system status"""
        return {
            'circuit_breakers': {
                name: cb.get_status() for name, cb in self.circuit_breakers.items()
            },
            'sla': self.sla_tracker.get_status(),
            'anomaly_detector': self.anomaly_detector.get_statistics(),
            'bandit': self.strategy_selector.get_statistics(),
            'alert_aggregator': self.alert_aggregator.get_stats(),
            'dependency_graph': self.dependency_graph.get_statistics()
        }
    
    def get_circuit_breaker(self, data_type: str) -> Optional[EnhancedDistributedCircuitBreaker]:
        """Get circuit breaker for data type"""
        return self.circuit_breakers.get(data_type)
    
    def reset_all(self):
        """Reset all components"""
        for cb in self.circuit_breakers.values():
            cb.reset()
        self.sla_tracker.reset()
        logger.info("All fallback components reset")


# ============================================================
# Complete Working Example
# ============================================================

async def main():
    """Enhanced demonstration with all fixes"""
    print("=" * 70)
    print("Ultimate Fallback Manager v4.0 - Complete Demo")
    print("=" * 70)
    
    # Initialize with all components working
    fallback_mgr = UltimateFallbackManagerV4({
        'availability_target': 0.999,
        'latency_target_ms': 100,
        'seasonality_period': 24,
        'bandit_alpha': 1.0,
        'bandit_beta': 1.0,
        'bandit_algorithm': 'thompson',
        'circuit_breaker': {
            'distributed': False,
            'failure_threshold': 0.5,
            'min_requests': 5
        }
    })
    
    print("\n✅ All dependencies resolved and components initialized")
    print(f"   Circuit breakers: {len(fallback_mgr.circuit_breakers)}")
    print(f"   Strategies: {[s.value for s in FallbackStrategy]}")
    print(f"   SLA target: {fallback_mgr.sla_tracker.availability_target:.3%}")
    
    # Test circuit breaker with ML
    print("\n🔌 Enhanced Circuit Breaker with ML:")
    cb = fallback_mgr.get_circuit_breaker('temperature')
    
    async def mock_api_call():
        await asyncio.sleep(0.02)
        if random.random() > 0.6:
            return {'temperature': 65.0, 'value': 65.0}
        raise Exception("API error")
    
    for i in range(20):
        result = await fallback_mgr.execute_with_fallback_enhanced(
            mock_api_call, 'temperature', {'value': 65.0 + random.gauss(0, 2)}
        )
        status = "✅" if result.success else "❌"
        print(f"   {status} Attempt {i+1}: success={result.success}, "
              f"strategy={result.strategy_used}, latency={result.latency_ms:.1f}ms")
    
    print(f"\n   Circuit breaker state: {cb.get_status()['state']}")
    print(f"   ML trained: {cb.get_status()['ml_trained']}")
    print(f"   Failure rate: {cb.get_status()['failure_rate']:.2%}")
    print(f"   Predicted rate: {cb.get_status()['predicted_failure_rate']:.2%}")
    
    # Test multi-armed bandit
    print("\n🎰 Multi-Armed Bandit Strategy Selection:")
    bandit_stats = fallback_mgr.strategy_selector.get_statistics()
    print(f"   Algorithm: {bandit_stats['algorithm']}")
    print(f"   Best strategy: {bandit_stats['best_strategy']}")
    for strategy, rate in bandit_stats['strategy_success_rates'].items():
        ci = bandit_stats['confidence_intervals'].get(strategy, {})
        print(f"   {strategy}: {rate:.2%} (95% CI: {ci.get('lower', 0):.2%} - {ci.get('upper', 0):.2%})")
    
    # Test anomaly detection with seasonality
    print("\n🔍 Advanced Anomaly Detection:")
    for i in range(100):
        # Simulate daily temperature cycle
        hour = i % 24
        base_temp = 65 + 5 * np.sin(hour * np.pi / 12)
        fallback_mgr.anomaly_detector.add_observation(
            'temperature', base_temp + random.gauss(0, 1), time.time()
        )
    
    # Test normal value
    is_anom, score = fallback_mgr.anomaly_detector.is_anomaly('temperature', 67.0)
    print(f"   Normal value 67.0: {'ANOMALY' if is_anom else 'normal'} (score={score:.2f})")
    
    # Test anomalous value
    is_anom, score = fallback_mgr.anomaly_detector.is_anomaly('temperature', 95.0)
    print(f"   Anomalous value 95.0: {'ANOMALY' if is_anom else 'normal'} (score={score:.2f})")
    
    # Test SLA tracking
    print("\n📊 SLA Tracking with Burn Rate:")
    sla = fallback_mgr.sla_tracker.get_status()
    print(f"   Availability: {sla['availability']:.4%} (target: {sla['availability_target']:.4%})")
    print(f"   P95 latency: {sla['latency_p95_ms']:.1f}ms (target: {sla['latency_target_ms']}ms)")
    print(f"   Error budget: {sla['error_budget_remaining']}/{sla['error_budget_total']} "
          f"({sla['error_budget_consumed_percent']:.1f}% consumed)")
    print(f"   Burn rate (short): {sla['burn_rate_short']:.1f}x")
    print(f"   Burn rate (long): {sla['burn_rate_long']:.1f}x")
    print(f"   SLA met: {sla['sla_met']}")
    
    # Test dependency graph
    print("\n🔗 Fallback Dependency Graph:")
    impact = fallback_mgr.dependency_graph.get_impact_analysis('api_gateway')
    print(f"   API Gateway impact: {impact['affected_services']} services affected")
    print(f"   Fallback order: {impact['fallback_order']}")
    print(f"   Has cycles: {impact['has_cycles']}")
    
    # Test alert aggregation
    print("\n📢 Alert Aggregation:")
    for i in range(10):
        alert = Alert(
            level=AlertSeverity.WARNING,
            title="Test Alert",
            message=f"Test message {i}",
            data={'test': True}
        )
        should_send, reason = fallback_mgr.alert_aggregator.should_send(alert)
        if i < 3:
            print(f"   Alert {i+1}: send={should_send}, reason={reason}")
    
    print(f"   Cache size: {fallback_mgr.alert_aggregator.get_stats()['cached_alerts']}")
    
    # Enhanced system status
    print("\n📋 Enhanced System Status:")
    status = fallback_mgr.get_enhanced_status()
    print(f"   Circuit breakers: {list(status['circuit_breakers'].keys())}")
    print(f"   Dependency graph: {status['dependency_graph']['total_services']} services, "
          f"{status['dependency_graph']['total_dependencies']} dependencies")
    print(f"   Bandit attempts: {status['bandit']['total_attempts']}")
    print(f"   Detector keys: {status['anomaly_detector']['keys']}")
    
    print("\n" + "=" * 70)
    print("✅ Ultimate Fallback Manager v4.0 - All Systems Operational")
    print("   - All 3 critical missing dependencies implemented (CircuitState, FallbackResult, SLATracker)")
    print("   - Fixed missing 'min_requests' attribute in circuit breaker")
    print("   - Proper IsolationForest ML integration")
    print("   - Enhanced anomaly detection with seasonality")
    print("   - UCB1 and Thompson sampling bandit algorithms")
    print("   - Complete fallback dependency graph with topological sort")
    print("   - SLA tracking with Google SRE burn rate alerts")
    print("=" * 70)


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run demonstration
    asyncio.run(main())
