# src/enhancements/fallback_manager.py

"""
Enhanced Fallback Management System for Green Agent - Version 2.0

Features:
1. Multi-level fallback with graceful degradation
2. Configurable fallback strategies (CASCADE, BEST_EFFORT, CONSERVATIVE, RETRY, CIRCUIT_BREAKER)
3. Exponential backoff for retries
4. Sliding window circuit breaker (failure rate based)
5. Prometheus metrics export
6. Per-data-type cache TTL
7. Health check endpoint for monitoring
8. Adaptive strategy selection based on failure history
9. Fallback pre-warming
10. Comprehensive audit logging

Reference: "Building Resilient Systems" (Google SRE Book)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Callable, Any
from enum import Enum
import logging
import time
import threading
import random
from collections import deque
import json
import os

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: Exponential Backoff Retry
# ============================================================

class ExponentialBackoff:
    """
    Exponential backoff retry strategy with jitter.
    
    Scientific basis: Randomized exponential backoff reduces thundering herd
    and improves system recovery under load.
    """
    
    def __init__(self, base_delay_ms: float = 100, 
                 max_delay_ms: float = 10000,
                 multiplier: float = 2.0,
                 use_jitter: bool = True):
        self.base_delay_ms = base_delay_ms
        self.max_delay_ms = max_delay_ms
        self.multiplier = multiplier
        self.use_jitter = use_jitter
    
    def get_delay(self, attempt: int) -> float:
        """
        Calculate delay for a given retry attempt.
        
        Args:
            attempt: Current retry attempt number (0-indexed)
        
        Returns:
            Delay in seconds
        """
        # Exponential backoff
        delay = self.base_delay_ms * (self.multiplier ** attempt)
        
        # Cap at maximum
        delay = min(delay, self.max_delay_ms)
        
        # Add jitter to prevent thundering herd
        if self.use_jitter:
            # Random jitter between 0 and delay
            delay = random.uniform(0, delay)
        
        return delay / 1000.0  # Convert to seconds


# ============================================================
# ENHANCEMENT 2: Sliding Window Circuit Breaker
# ============================================================

class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"        # Normal operation
    OPEN = "open"            # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery


class SlidingWindowCircuitBreaker:
    """
    Enhanced circuit breaker with sliding window failure rate calculation.
    
    Scientific basis: Sliding window provides more accurate failure rate
    than simple counters, avoiding "burst tolerance" issues.
    """
    
    def __init__(self, name: str, 
                 window_size_seconds: int = 60,
                 failure_threshold: float = 0.5,
                 min_requests: int = 10,
                 timeout_ms: int = 30000,
                 half_open_max_calls: int = 3):
        self.name = name
        self.window_size_seconds = window_size_seconds
        self.failure_threshold = failure_threshold
        self.min_requests = min_requests
        self.timeout_ms = timeout_ms
        self.half_open_max_calls = half_open_max_calls
        
        # Sliding window data
        self._results: deque = deque()  # (timestamp, success)
        self._lock = threading.Lock()
        
        # Circuit state
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.half_open_calls = 0
        
        # Statistics
        self.total_calls = 0
        self.total_failures = 0
        self.total_rejections = 0
    
    def _clean_window(self, current_time: float):
        """Remove entries outside the window"""
        cutoff = current_time - self.window_size_seconds
        while self._results and self._results[0][0] < cutoff:
            self._results.popleft()
    
    def _get_failure_rate(self) -> float:
        """Calculate current failure rate from sliding window"""
        if len(self._results) < self.min_requests:
            return 0.0
        
        failures = sum(1 for _, success in self._results if not success)
        return failures / len(self._results)
    
    def record_result(self, success: bool):
        """Record a call result for failure rate calculation"""
        with self._lock:
            self.total_calls += 1
            if not success:
                self.total_failures += 1
            
            self._results.append((time.time(), success))
            self._clean_window(time.time())
    
    def call(self, func: Callable, *args, **kwargs) -> Tuple[bool, Any]:
        """
        Execute function with circuit breaker protection.
        
        Returns:
            (success, result)
        """
        current_time = time.time()
        current_time_ms = current_time * 1000
        
        with self._lock:
            self._clean_window(current_time)
            
            # Check circuit state
            if self.state == CircuitState.OPEN:
                if current_time_ms - self.last_failure_time > self.timeout_ms:
                    self.state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                    logger.info(f"Circuit {self.name} transitioning to HALF_OPEN")
                else:
                    self.total_rejections += 1
                    return False, None
            
            if self.state == CircuitState.HALF_OPEN:
                if self.half_open_calls >= self.half_open_max_calls:
                    self.total_rejections += 1
                    return False, None
                self.half_open_calls += 1
        
        # Execute the call
        try:
            result = func(*args, **kwargs)
            self.record_result(True)
            
            with self._lock:
                if self.state == CircuitState.HALF_OPEN:
                    self.state = CircuitState.CLOSED
                    logger.info(f"Circuit {self.name} recovered to CLOSED")
                elif self.state == CircuitState.CLOSED:
                    # Check if we should open based on failure rate
                    failure_rate = self._get_failure_rate()
                    if (failure_rate >= self.failure_threshold and 
                        len(self._results) >= self.min_requests):
                        self.state = CircuitState.OPEN
                        self.last_failure_time = current_time_ms
                        logger.error(f"Circuit {self.name} tripped OPEN (failure rate={failure_rate:.1%})")
            
            return True, result
            
        except Exception as e:
            self.record_result(False)
            
            with self._lock:
                failure_rate = self._get_failure_rate()
                if (self.state == CircuitState.CLOSED and 
                    failure_rate >= self.failure_threshold and 
                    len(self._results) >= self.min_requests):
                    self.state = CircuitState.OPEN
                    self.last_failure_time = current_time_ms
                    logger.error(f"Circuit {self.name} tripped OPEN (failure rate={failure_rate:.1%})")
            
            return False, None
    
    def get_state(self) -> Dict:
        """Get circuit breaker state for monitoring"""
        with self._lock:
            return {
                'name': self.name,
                'state': self.state.value,
                'failure_rate': self._get_failure_rate(),
                'failure_threshold': self.failure_threshold,
                'window_size_seconds': self.window_size_seconds,
                'total_calls': self.total_calls,
                'total_failures': self.total_failures,
                'total_rejections': self.total_rejections,
                'success_rate': (self.total_calls - self.total_failures - self.total_rejections) / max(1, self.total_calls)
            }
    
    def reset(self):
        """Manually reset circuit breaker"""
        with self._lock:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.half_open_calls = 0
            self._results.clear()
            logger.info(f"Circuit {self.name} manually reset")


# ============================================================
# ENHANCEMENT 3: Prometheus Metrics Export
# ============================================================

class FallbackMetrics:
    """
    Prometheus metrics exporter for fallback operations.
    
    Provides real-time visibility into fallback behavior and circuit breaker status.
    """
    
    def __init__(self):
        self._lock = threading.Lock()
        self._metrics = {
            'primary_success': 0,
            'primary_failure': 0,
            'retry_success': 0,
            'fallback_used': 0,
            'cache_hit': 0,
            'conservative_used': 0,
            'total_calls': 0,
            'total_latency_ms': 0.0
        }
        
        # Per-data-type metrics
        self._per_type_metrics: Dict[str, Dict] = {}
    
    def record_call(self, data_type: str, source: str, latency_ms: float, success: bool):
        """Record a call outcome"""
        with self._lock:
            self._metrics['total_calls'] += 1
            self._metrics['total_latency_ms'] += latency_ms
            
            # Record source metric
            if source == "primary":
                if success:
                    self._metrics['primary_success'] += 1
                else:
                    self._metrics['primary_failure'] += 1
            elif source.startswith("primary_retry"):
                self._metrics['retry_success'] += 1
            elif source == "fallback_synthetic":
                self._metrics['fallback_used'] += 1
            elif source.startswith("cache"):
                self._metrics['cache_hit'] += 1
            elif source == "conservative_default":
                self._metrics['conservative_used'] += 1
            
            # Per-type metrics
            if data_type not in self._per_type_metrics:
                self._per_type_metrics[data_type] = {
                    'primary_success': 0,
                    'primary_failure': 0,
                    'fallback_used': 0,
                    'cache_hit': 0
                }
            
            pt = self._per_type_metrics[data_type]
            if source == "primary":
                if success:
                    pt['primary_success'] += 1
                else:
                    pt['primary_failure'] += 1
            elif source == "fallback_synthetic":
                pt['fallback_used'] += 1
            elif source.startswith("cache"):
                pt['cache_hit'] += 1
    
    def get_metrics(self) -> Dict:
        """Get current metrics"""
        with self._lock:
            metrics_copy = self._metrics.copy()
            metrics_copy['average_latency_ms'] = (metrics_copy['total_latency_ms'] / 
                                                   max(1, metrics_copy['total_calls']))
            metrics_copy['fallback_rate'] = (metrics_copy['fallback_used'] / 
                                              max(1, metrics_copy['total_calls']))
            metrics_copy['cache_hit_rate'] = (metrics_copy['cache_hit'] / 
                                               max(1, metrics_copy['total_calls']))
            metrics_copy['per_type'] = self._per_type_metrics.copy()
            return metrics_copy
    
    def to_prometheus_text(self) -> str:
        """Export metrics in Prometheus text format"""
        metrics = self.get_metrics()
        
        lines = [
            "# HELP fallback_calls_total Total fallback calls",
            "# TYPE fallback_calls_total counter",
            f"fallback_calls_total {metrics['total_calls']}",
            "",
            "# HELP fallback_primary_success_total Successful primary calls",
            "# TYPE fallback_primary_success_total counter",
            f"fallback_primary_success_total {metrics['primary_success']}",
            "",
            "# HELP fallback_primary_failure_total Failed primary calls",
            "# TYPE fallback_primary_failure_total counter",
            f"fallback_primary_failure_total {metrics['primary_failure']}",
            "",
            "# HELP fallback_retry_success_total Successful retry calls",
            "# TYPE fallback_retry_success_total counter",
            f"fallback_retry_success_total {metrics['retry_success']}",
            "",
            "# HELP fallback_fallback_used_total Fallback provider used",
            "# TYPE fallback_fallback_used_total counter",
            f"fallback_fallback_used_total {metrics['fallback_used']}",
            "",
            "# HELP fallback_cache_hit_total Cache hits",
            "# TYPE fallback_cache_hit_total counter",
            f"fallback_cache_hit_total {metrics['cache_hit']}",
            "",
            "# HELP fallback_conservative_total Conservative defaults used",
            "# TYPE fallback_conservative_total counter",
            f"fallback_conservative_total {metrics['conservative_used']}",
            "",
            "# HELP fallback_average_latency_ms Average latency in milliseconds",
            "# TYPE fallback_average_latency_ms gauge",
            f"fallback_average_latency_ms {metrics['average_latency_ms']:.2f}",
            "",
            "# HELP fallback_fallback_rate Rate of fallback usage",
            "# TYPE fallback_fallback_rate gauge",
            f"fallback_fallback_rate {metrics['fallback_rate']:.3f}",
            "",
            "# HELP fallback_cache_hit_rate Rate of cache hits",
            "# TYPE fallback_cache_hit_rate gauge",
            f"fallback_cache_hit_rate {metrics['cache_hit_rate']:.3f}",
        ]
        
        # Per-type metrics
        for data_type, pt in metrics.get('per_type', {}).items():
            lines.extend([
                f'fallback_per_type_primary_success{{data_type="{data_type}"}} {pt["primary_success"]}',
                f'fallback_per_type_primary_failure{{data_type="{data_type}"}} {pt["primary_failure"]}',
                f'fallback_per_type_fallback_used{{data_type="{data_type}"}} {pt["fallback_used"]}',
                f'fallback_per_type_cache_hit{{data_type="{data_type}"}} {pt["cache_hit"]}',
            ])
        
        return "\n".join(lines)
    
    def reset(self):
        """Reset all metrics (for testing)"""
        with self._lock:
            for key in self._metrics:
                if isinstance(self._metrics[key], (int, float)):
                    self._metrics[key] = 0
            self._per_type_metrics.clear()


# ============================================================
# ENHANCEMENT 4: Per-Data-Type Cache TTL
# ============================================================

class CacheManager:
    """
    Enhanced cache manager with per-data-type TTL and pre-warming.
    """
    
    # Default TTLs in seconds for different data types
    DEFAULT_TTL_SECONDS = {
        'temperature': 10,      # Temperature changes quickly
        'grid': 300,            # Grid data updates every 5 minutes
        'helium': 3600,         # Helium market updates hourly
        'recovery': 60,         # Recovery data updates every minute
        'default': 60
    }
    
    def __init__(self, ttl_config: Optional[Dict[str, int]] = None):
        self.ttl_config = ttl_config or {}
        self._cache: Dict[str, Tuple[Any, float]] = {}  # key -> (value, timestamp)
        self._lock = threading.Lock()
        self._pre_warm_data: Dict[str, Any] = {}
    
    def get_ttl(self, data_type: str) -> int:
        """Get TTL for a specific data type"""
        return self.ttl_config.get(data_type, self.DEFAULT_TTL_SECONDS.get(data_type, 60))
    
    def get(self, key: str, data_type: str) -> Optional[Any]:
        """Get cached value if not expired"""
        with self._lock:
            if key not in self._cache:
                return None
            
            value, timestamp = self._cache[key]
            current_time = time.time()
            ttl = self.get_ttl(data_type)
            
            if current_time - timestamp < ttl:
                return value
            else:
                # Expired
                del self._cache[key]
                return None
    
    def set(self, key: str, value: Any, data_type: str):
        """Set cached value"""
        with self._lock:
            self._cache[key] = (value, time.time())
    
    def pre_warm(self, data_type: str, value: Any):
        """Pre-warm cache with fallback data"""
        self._pre_warm_data[data_type] = value
        self.set(data_type, value, data_type)
        logger.info(f"Pre-warmed cache for {data_type}")
    
    def get_pre_warmed(self, data_type: str) -> Optional[Any]:
        """Get pre-warmed data if available"""
        return self._pre_warm_data.get(data_type)
    
    def clear_expired(self):
        """Clear expired cache entries"""
        with self._lock:
            current_time = time.time()
            expired_keys = []
            for key, (_, timestamp) in self._cache.items():
                ttl = self.get_ttl(key)
                if current_time - timestamp >= ttl:
                    expired_keys.append(key)
            
            for key in expired_keys:
                del self._cache[key]
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        with self._lock:
            return {
                'size': len(self._cache),
                'pre_warmed_count': len(self._pre_warm_data),
                'keys': list(self._cache.keys())
            }


# ============================================================
# ENHANCEMENT 5: Adaptive Strategy Selection
# ============================================================

class AdaptiveStrategySelector:
    """
    Dynamically selects fallback strategy based on failure history.
    
    Scientific basis: Adaptive systems learn from past failures to
    optimize future behavior.
    """
    
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self._failure_history: Dict[str, deque] = {}  # data_type -> deque of (timestamp, success)
        self._current_strategy: Dict[str, FallbackStrategy] = {}
        self._lock = threading.Lock()
        
        # Default strategies per data type
        self.default_strategies = {
            'temperature': FallbackStrategy.CASCADE,
            'grid': FallbackStrategy.RETRY,
            'helium': FallbackStrategy.CIRCUIT_BREAKER,
            'recovery': FallbackStrategy.CASCADE
        }
    
    def record_outcome(self, data_type: str, success: bool):
        """Record the outcome of a call for learning"""
        with self._lock:
            if data_type not in self._failure_history:
                self._failure_history[data_type] = deque(maxlen=self.history_window)
            
            self._failure_history[data_type].append((time.time(), success))
    
    def get_failure_rate(self, data_type: str, window_seconds: int = 300) -> float:
        """Calculate recent failure rate for a data type"""
        with self._lock:
            if data_type not in self._failure_history:
                return 0.0
            
            cutoff = time.time() - window_seconds
            recent = [(ts, s) for ts, s in self._failure_history[data_type] if ts > cutoff]
            
            if not recent:
                return 0.0
            
            failures = sum(1 for _, s in recent if not s)
            return failures / len(recent)
    
    def select_strategy(self, data_type: str) -> FallbackStrategy:
        """Dynamically select the best strategy based on recent failures"""
        with self._lock:
            failure_rate = self.get_failure_rate(data_type)
            current = self._current_strategy.get(data_type)
            
            # Strategy selection logic
            if failure_rate > 0.5:
                # High failure rate: use circuit breaker
                new_strategy = FallbackStrategy.CIRCUIT_BREAKER
            elif failure_rate > 0.2:
                # Medium failure rate: retry before fallback
                new_strategy = FallbackStrategy.RETRY
            else:
                # Low failure rate: cascade (robust)
                new_strategy = FallbackStrategy.CASCADE
            
            # Log strategy changes
            if current != new_strategy:
                logger.info(f"Strategy for {data_type} changed from {current} to {new_strategy} "
                           f"(failure rate={failure_rate:.1%})")
                self._current_strategy[data_type] = new_strategy
            
            return new_strategy
    
    def get_optimal_timeout(self, data_type: str, default_ms: int = 5000) -> int:
        """Calculate optimal timeout based on historical latency"""
        # Placeholder for latency-based timeout calculation
        return default_ms


# ============================================================
# ENHANCEMENT 6: Enhanced Fallback Config
# ============================================================

class FallbackStrategy(Enum):
    """Available fallback strategies"""
    CASCADE = "cascade"              # Try primary, then fallback1, then fallback2
    BEST_EFFORT = "best_effort"      # Use whatever works, don't fail
    CONSERVATIVE = "conservative"    # Use safe default
    RETRY = "retry"                  # Retry primary before fallback
    CIRCUIT_BREAKER = "circuit_breaker"  # Trip after failures


@dataclass
class FallbackConfig:
    """Enhanced configuration for a fallback chain"""
    strategy: FallbackStrategy
    max_retries: int = 3
    base_retry_delay_ms: int = 100
    max_retry_delay_ms: int = 10000
    circuit_breaker_threshold: float = 0.5
    circuit_breaker_timeout_ms: int = 30000
    timeout_ms: int = 5000
    use_jitter: bool = True
    use_adaptive_strategy: bool = True


@dataclass
class FallbackResult:
    """Enhanced result of a fallback execution"""
    success: bool
    value: Any
    source: str          # Which fallback level succeeded
    latency_ms: float
    retry_count: int
    circuit_state: Optional[str]
    error: Optional[str] = None
    data_type: str = ""
    timestamp: float = field(default_factory=time.time)


# ============================================================
# ENHANCEMENT 7: Main Enhanced Fallback Manager
# ============================================================

class FallbackManager:
    """
    Enhanced unified fallback manager for all Green Agent modules.
    
    Features:
    - Multi-level cascading fallbacks
    - Exponential backoff with jitter
    - Sliding window circuit breakers
    - Prometheus metrics export
    - Per-data-type cache TTL
    - Adaptive strategy selection
    - Health check endpoint
    - Fallback pre-warming
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.default_config = FallbackConfig(strategy=FallbackStrategy.CASCADE)
        
        # Circuit breakers for external dependencies
        self.circuit_breakers = {
            'temperature_sensor': SlidingWindowCircuitBreaker(
                'temperature_sensor',
                window_size_seconds=60,
                failure_threshold=0.3,
                timeout_ms=10000
            ),
            'grid_api': SlidingWindowCircuitBreaker(
                'grid_api',
                window_size_seconds=300,
                failure_threshold=0.5,
                timeout_ms=30000
            ),
            'helium_api': SlidingWindowCircuitBreaker(
                'helium_api',
                window_size_seconds=300,
                failure_threshold=0.5,
                timeout_ms=30000
            ),
            'recovery_system': SlidingWindowCircuitBreaker(
                'recovery_system',
                window_size_seconds=60,
                failure_threshold=0.3,
                timeout_ms=15000
            ),
            'ppa_database': SlidingWindowCircuitBreaker(
                'ppa_database',
                window_size_seconds=60,
                failure_threshold=0.3,
                timeout_ms=10000
            )
        }
        
        # Backoff generators
        self.backoff = ExponentialBackoff(
            base_delay_ms=100,
            max_delay_ms=10000,
            multiplier=2.0,
            use_jitter=True
        )
        
        # Cache manager
        self.cache_manager = CacheManager(self.config.get('ttl_config', {}))
        
        # Metrics
        self.metrics = FallbackMetrics()
        
        # Adaptive strategy selector
        self.strategy_selector = AdaptiveStrategySelector()
        
        # Fallback providers
        self._fallback_providers = {
            'temperature': self._get_fallback_temperature,
            'grid': self._get_fallback_grid,
            'helium': self._get_fallback_helium,
            'recovery': self._get_fallback_recovery
        }
        
        # Conservative defaults
        self._conservative_defaults = {
            'temperature': self._get_conservative_temperature,
            'grid': self._get_conservative_grid,
            'helium': self._get_conservative_helium,
            'recovery': self._get_conservative_recovery
        }
        
        # Pre-warm cache with fallback data
        self._pre_warm_cache()
        
        # Start background cleanup thread
        self._cleanup_thread = None
        self._running = False
        self._start_cleanup_thread()
        
        logger.info("Enhanced Fallback Manager v2.0 initialized")
    
    def _pre_warm_cache(self):
        """Pre-warm cache with fallback data"""
        for data_type, provider in self._fallback_providers.items():
            try:
                value = provider()
                self.cache_manager.pre_warm(data_type, value)
            except Exception as e:
                logger.warning(f"Failed to pre-warm {data_type}: {e}")
    
    def _start_cleanup_thread(self):
        """Start background cache cleanup thread"""
        self._running = True
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self._cleanup_thread.start()
    
    def _cleanup_loop(self):
        """Background cache cleanup loop"""
        while self._running:
            time.sleep(60)  # Clean every minute
            self.cache_manager.clear_expired()
    
    def stop(self):
        """Stop the fallback manager"""
        self._running = False
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=2)
    
    def execute_with_fallback(self, primary_func: Callable, data_type: str,
                              config: Optional[FallbackConfig] = None,
                              context: Optional[Dict] = None) -> FallbackResult:
        """
        Execute a function with circuit breaker and cascading fallbacks.
        
        Args:
            primary_func: Primary data source function
            data_type: Type of data (temperature, grid, helium, recovery)
            config: Fallback configuration (uses default if None)
            context: Additional context for the call
            
        Returns:
            FallbackResult with data or error
        """
        start_time = time.time()
        config = config or self.default_config
        
        # Apply adaptive strategy if enabled
        if config.use_adaptive_strategy:
            adaptive_strategy = self.strategy_selector.select_strategy(data_type)
            config.strategy = adaptive_strategy
        
        # Get circuit breaker
        circuit = self.circuit_breakers.get(f"{data_type}_sensor")
        if not circuit:
            # Create temporary circuit breaker if not configured
            circuit = SlidingWindowCircuitBreaker(data_type, timeout_ms=10000)
        
        # Step 1: Try primary with circuit breaker
        primary_success = False
        primary_value = None
        
        if config.strategy != FallbackStrategy.CIRCUIT_BREAKER:
            primary_success, primary_value = circuit.call(primary_func)
        else:
            try:
                primary_value = primary_func()
                primary_success = True
            except Exception as e:
                primary_success = False
                primary_value = None
        
        if primary_success and primary_value is not None:
            # Update cache with fresh data
            self.cache_manager.set(data_type, primary_value, data_type)
            self.strategy_selector.record_outcome(data_type, True)
            self.metrics.record_call(data_type, "primary", (time.time() - start_time) * 1000, True)
            
            return FallbackResult(
                success=True,
                value=primary_value,
                source="primary",
                latency_ms=(time.time() - start_time) * 1000,
                retry_count=0,
                circuit_state=circuit.get_state()['state'],
                data_type=data_type
            )
        
        # Record failure for adaptive learning
        self.strategy_selector.record_outcome(data_type, False)
        
        # Step 2: Retry with exponential backoff if configured
        if config.strategy == FallbackStrategy.RETRY:
            for attempt in range(config.max_retries):
                delay = self.backoff.get_delay(attempt)
                time.sleep(delay)
                
                try:
                    retry_value = primary_func()
                    if retry_value is not None:
                        self.cache_manager.set(data_type, retry_value, data_type)
                        self.metrics.record_call(data_type, f"primary_retry_{attempt+1}", 
                                                  (time.time() - start_time) * 1000, True)
                        return FallbackResult(
                            success=True,
                            value=retry_value,
                            source=f"primary_retry_{attempt+1}",
                            latency_ms=(time.time() - start_time) * 1000,
                            retry_count=attempt + 1,
                            circuit_state=circuit.get_state()['state'],
                            data_type=data_type
                        )
                except Exception:
                    continue
            
            self.metrics.record_call(data_type, "retry_failed", (time.time() - start_time) * 1000, False)
        
        # Step 3: Use fallback provider
        fallback_func = self._fallback_providers.get(data_type)
        if fallback_func:
            try:
                fallback_value = fallback_func()
                if fallback_value is not None:
                    self.cache_manager.set(data_type, fallback_value, data_type)
                    self.metrics.record_call(data_type, "fallback_synthetic", 
                                              (time.time() - start_time) * 1000, True)
                    return FallbackResult(
                        success=True,
                        value=fallback_value,
                        source="fallback_synthetic",
                        latency_ms=(time.time() - start_time) * 1000,
                        retry_count=config.max_retries,
                        circuit_state=circuit.get_state()['state'],
                        data_type=data_type
                    )
            except Exception as e:
                logger.error(f"Fallback failed for {data_type}: {e}")
        
        # Step 4: Use cached data
        cached_value = self.cache_manager.get(data_type, data_type)
        if cached_value is not None:
            self.metrics.record_call(data_type, "cache_hit", (time.time() - start_time) * 1000, True)
            return FallbackResult(
                success=True,
                value=cached_value,
                source="cache",
                latency_ms=(time.time() - start_time) * 1000,
                retry_count=config.max_retries,
                circuit_state=circuit.get_state()['state'],
                data_type=data_type
            )
        
        # Step 5: Conservative default
        if config.strategy == FallbackStrategy.CONSERVATIVE:
            default_func = self._conservative_defaults.get(data_type)
            if default_func:
                default_value = default_func()
                self.metrics.record_call(data_type, "conservative_default", 
                                          (time.time() - start_time) * 1000, True)
                return FallbackResult(
                    success=True,
                    value=default_value,
                    source="conservative_default",
                    latency_ms=(time.time() - start_time) * 1000,
                    retry_count=config.max_retries,
                    circuit_state=circuit.get_state()['state'],
                    data_type=data_type
                )
        
        # Complete failure
        self.metrics.record_call(data_type, "none", (time.time() - start_time) * 1000, False)
        return FallbackResult(
            success=False,
            value=None,
            source="none",
            latency_ms=(time.time() - start_time) * 1000,
            retry_count=config.max_retries,
            circuit_state=circuit.get_state()['state'],
            error=f"No fallback available for {data_type}",
            data_type=data_type
        )
    
    # ============================================================
    # Fallback Providers (Synthetic Data)
    # ============================================================
    
    def _get_fallback_temperature(self):
        """Enhanced synthetic temperature fallback with realistic variation"""
        import random
        # Add realistic daily pattern based on time of day
        hour = time.localtime().tm_hour
        if 6 <= hour <= 18:  # Daytime
            day_factor = 1.0
        else:
            day_factor = 0.8
        
        return {
            'cpu_temp': 50 + random.uniform(-5, 15) * day_factor,
            'gpu_temp': 60 + random.uniform(-8, 25) * day_factor,
            'ambient': 20 + random.uniform(-2, 8) * day_factor,
            'timestamp': time.time(),
            'source': 'fallback_synthetic'
        }
    
    def _get_fallback_grid(self):
        """Enhanced synthetic grid fallback with region awareness"""
        import random
        region = self.config.get('region', 'us-east')
        
        base_intensities = {
            'us-east': 380,
            'us-west': 250,
            'eu-north': 80,
            'asia-pacific': 550
        }
        
        base = base_intensities.get(region, 400)
        
        return {
            'average_intensity': base + random.uniform(-20, 20),
            'marginal_intensity': base * (0.9 + random.uniform(-0.1, 0.2)),
            'renewable_percentage': 0.2 + random.uniform(-0.1, 0.3),
            'region': region,
            'timestamp': time.time(),
            'source': 'fallback_synthetic'
        }
    
    def _get_fallback_helium(self):
        """Enhanced synthetic helium fallback with realistic market dynamics"""
        import random
        return {
            'spot_price': 4.0 + random.uniform(-0.5, 3.0),
            'futures_1m': 4.5 + random.uniform(-0.5, 3.0),
            'futures_3m': 5.0 + random.uniform(-1.0, 4.0),
            'inventory_days': 20 + random.uniform(-10, 15),
            'disruption_risk': 0.2 + random.uniform(-0.15, 0.4),
            'timestamp': time.time(),
            'source': 'fallback_synthetic'
        }
    
    def _get_fallback_recovery(self):
        """Enhanced synthetic recovery fallback"""
        import random
        return {
            'efficiency': 0.6 + random.uniform(-0.1, 0.3),
            'recovered_liters': random.uniform(0, 100),
            'method': random.choice(['capture', 'recycle', 'purification']),
            'timestamp': time.time(),
            'source': 'fallback_synthetic'
        }
    
    # ============================================================
    # Conservative Defaults
    # ============================================================
    
    def _get_conservative_temperature(self) -> Dict:
        """Conservative temperature defaults (higher temps for safety)"""
        return {
            'cpu_temp': 75.0,
            'gpu_temp': 85.0,
            'ambient': 30.0,
            'timestamp': time.time(),
            'source': 'conservative_default'
        }
    
    def _get_conservative_grid(self) -> Dict:
        """Conservative grid defaults (higher carbon)"""
        region = self.config.get('region', 'us-east')
        base = 500 if region == 'us-east' else 400
        return {
            'average_intensity': base,
            'marginal_intensity': base * 1.1,
            'renewable_percentage': 0.05,
            'region': region,
            'timestamp': time.time(),
            'source': 'conservative_default'
        }
    
    def _get_conservative_helium(self) -> Dict:
        """Conservative helium defaults (higher price, lower inventory)"""
        return {
            'spot_price': 12.0,
            'futures_1m': 13.0,
            'futures_3m': 15.0,
            'inventory_days': 5,
            'disruption_risk': 0.8,
            'timestamp': time.time(),
            'source': 'conservative_default'
        }
    
    def _get_conservative_recovery(self) -> Dict:
        """Conservative recovery defaults (poor efficiency)"""
        return {
            'efficiency': 0.3,
            'recovered_liters': 0.0,
            'method': 'none',
            'timestamp': time.time(),
            'source': 'conservative_default'
        }
    
    # ============================================================
    # Public API Methods
    # ============================================================
    
    def get_circuit_breaker_status(self) -> Dict:
        """Get status of all circuit breakers"""
        return {name: cb.get_state() for name, cb in self.circuit_breakers.items()}
    
    def reset_circuit_breaker(self, name: str):
        """Manually reset a circuit breaker"""
        if name in self.circuit_breakers:
            self.circuit_breakers[name].reset()
            logger.info(f"Circuit breaker {name} manually reset")
        elif f"{name}_sensor" in self.circuit_breakers:
            self.circuit_breakers[f"{name}_sensor"].reset()
    
    def get_health_status(self) -> Dict:
        """Get comprehensive health status of all dependencies"""
        cb_status = self.get_circuit_breaker_status()
        
        # Determine overall health
        unhealthy = [name for name, status in cb_status.items() 
                    if status['state'] == 'open']
        
        if unhealthy:
            overall = 'degraded'
            message = f"Circuit breakers open: {', '.join(unhealthy)}"
        else:
            overall = 'healthy'
            message = "All dependencies operational"
        
        metrics = self.metrics.get_metrics()
        
        return {
            'overall': overall,
            'message': message,
            'timestamp': time.time(),
            'circuit_breakers': cb_status,
            'cache': self.cache_manager.get_stats(),
            'metrics': {
                'total_calls': metrics['total_calls'],
                'fallback_rate': metrics['fallback_rate'],
                'cache_hit_rate': metrics['cache_hit_rate'],
                'average_latency_ms': metrics['average_latency_ms']
            }
        }
    
    def get_metrics_text(self) -> str:
        """Get Prometheus metrics in text format"""
        return self.metrics.to_prometheus_text()
    
    def reset_metrics(self):
        """Reset all metrics (for testing)"""
        self.metrics.reset()
    
    def get_adaptive_strategy(self, data_type: str) -> str:
        """Get current adaptive strategy for a data type"""
        return self.strategy_selector.select_strategy(data_type).value
    
    def pre_warm_fallback(self, data_type: str):
        """Pre-warm fallback data for a specific type"""
        if data_type in self._fallback_providers:
            value = self._fallback_providers[data_type]()
            self.cache_manager.pre_warm(data_type, value)
            logger.info(f"Pre-warmed fallback data for {data_type}")


# ============================================================
# Usage Example
# ============================================================

if __name__ == "__main__":
    # Initialize fallback manager
    fallback_mgr = FallbackManager({
        'region': 'us-east',
        'ttl_config': {
            'temperature': 5,
            'grid': 60,
            'helium': 300
        }
    })
    
    # Mock primary function that sometimes fails
    import random
    
    def mock_primary_success():
        """Mock successful primary call"""
        return {'temperature': 65.0, 'timestamp': time.time()}
    
    def mock_primary_failing():
        """Mock failing primary call"""
        raise Exception("API timeout")
    
    def mock_primary_unreliable():
        """Mock unreliable primary (50% success)"""
        if random.random() > 0.5:
            return {'temperature': 65.0, 'timestamp': time.time()}
        raise Exception("Random failure")
    
    print("=== Enhanced Fallback Manager Demo ===\n")
    
    # Test 1: Successful primary
    print("1. Successful primary call:")
    result = fallback_mgr.execute_with_fallback(mock_primary_success, 'temperature')
    print(f"   Success: {result.success}, Source: {result.source}, Latency: {result.latency_ms:.2f}ms")
    
    # Test 2: Failing primary with cascade
    print("\n2. Failing primary with cascade fallback:")
    result = fallback_mgr.execute_with_fallback(mock_primary_failing, 'temperature')
    print(f"   Success: {result.success}, Source: {result.source}, Retries: {result.retry_count}")
    
    # Test 3: Retry strategy
    print("\n3. Retry strategy with exponential backoff:")
    config = FallbackConfig(strategy=FallbackStrategy.RETRY, max_retries=3)
    result = fallback_mgr.execute_with_fallback(mock_primary_unreliable, 'temperature', config)
    print(f"   Success: {result.success}, Source: {result.source}, Retries: {result.retry_count}")
    
    # Test 4: Health status
    print("\n4. Health status:")
    health = fallback_mgr.get_health_status()
    print(f"   Overall: {health['overall']}")
    print(f"   Message: {health['message']}")
    print(f"   Fallback rate: {health['metrics']['fallback_rate']:.2%}")
    print(f"   Cache hit rate: {health['metrics']['cache_hit_rate']:.2%}")
    
    # Test 5: Circuit breaker status
    print("\n5. Circuit breaker status:")
    for name, status in fallback_mgr.get_circuit_breaker_status().items():
        print(f"   {name}: state={status['state']}, failure_rate={status.get('failure_rate', 0):.1%}")
    
    # Test 6: Prometheus metrics
    print("\n6. Prometheus metrics (sample):")
    metrics_text = fallback_mgr.get_metrics_text()
    lines = metrics_text.split('\n')[:10]
    for line in lines:
        if line and not line.startswith('#'):
            print(f"   {line[:80]}")
    
    print("\n✅ Enhanced Fallback Manager test complete")
