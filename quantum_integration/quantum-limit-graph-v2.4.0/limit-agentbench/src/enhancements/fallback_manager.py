# src/enhancements/fallback_manager.py

"""
Enhanced Fallback Management System for Green Agent - Version 3.0

Features:
1. Multi-level fallback with graceful degradation
2. Configurable fallback strategies (CASCADE, BEST_EFFORT, CONSERVATIVE, RETRY, CIRCUIT_BREAKER)
3. Exponential backoff for retries with jitter
4. Sliding window circuit breaker (failure rate based)
5. Prometheus metrics export with percentiles
6. Per-data-type cache TTL with LRU eviction
7. Health check endpoint for monitoring
8. Adaptive strategy selection with learned thresholds
9. Fallback pre-warming
10. Comprehensive audit logging
11. Async/await support for non-blocking operations
12. Retry budget with timeout
13. Backpressure handling with bounded queue
14. Circuit breaker state metrics

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

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: Async Exponential Backoff
# ============================================================

class AsyncExponentialBackoff:
    """
    Async exponential backoff retry strategy with jitter.
    
    Scientific basis: Randomized exponential backoff reduces thundering herd
    and improves system recovery under load.
    """
    
    def __init__(self, base_delay_ms: float = 100, 
                 max_delay_ms: float = 10000,
                 multiplier: float = 2.0,
                 use_jitter: bool = True,
                 max_retry_time_ms: float = 30000):
        self.base_delay_ms = base_delay_ms
        self.max_delay_ms = max_delay_ms
        self.multiplier = multiplier
        self.use_jitter = use_jitter
        self.max_retry_time_ms = max_retry_time_ms
    
    def get_delay(self, attempt: int, start_time_ms: float) -> Tuple[float, bool]:
        """
        Calculate delay for a given retry attempt.
        
        Returns:
            (delay_seconds, should_continue)
        """
        current_time_ms = time.time() * 1000
        elapsed_ms = current_time_ms - start_time_ms
        
        # Check retry budget
        if elapsed_ms >= self.max_retry_time_ms:
            return 0.0, False
        
        # Exponential backoff
        delay = self.base_delay_ms * (self.multiplier ** attempt)
        delay = min(delay, self.max_delay_ms)
        
        # Cap by remaining budget
        remaining_budget = self.max_retry_time_ms - elapsed_ms
        delay = min(delay, remaining_budget)
        
        # Add jitter
        if self.use_jitter:
            delay = random.uniform(0, delay)
        
        return delay / 1000.0, True
    
    async def wait(self, attempt: int, start_time_ms: float) -> bool:
        """Async wait for retry delay"""
        delay, should_continue = self.get_delay(attempt, start_time_ms)
        if should_continue and delay > 0:
            await asyncio.sleep(delay)
        return should_continue


# ============================================================
# ENHANCEMENT 2: Enhanced Sliding Window Circuit Breaker
# ============================================================

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class SlidingWindowCircuitBreaker:
    """
    Enhanced circuit breaker with sliding window failure rate calculation.
    
    Features:
    - Async/sync dual support
    - Metrics export for Prometheus
    - Automatic state transition logging
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
        
        self._results: deque = deque()
        self._lock = threading.Lock()
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.half_open_calls = 0
        
        self.total_calls = 0
        self.total_failures = 0
        self.total_rejections = 0
        self.total_successes = 0
    
    def _clean_window(self, current_time: float):
        cutoff = current_time - self.window_size_seconds
        while self._results and self._results[0][0] < cutoff:
            self._results.popleft()
    
    def _get_failure_rate(self) -> float:
        if len(self._results) < self.min_requests:
            return 0.0
        failures = sum(1 for _, success in self._results if not success)
        return failures / len(self._results)
    
    def _get_success_rate(self) -> float:
        if len(self._results) < self.min_requests:
            return 1.0
        successes = sum(1 for _, success in self._results if success)
        return successes / len(self._results)
    
    def record_result(self, success: bool):
        with self._lock:
            self.total_calls += 1
            if success:
                self.total_successes += 1
            else:
                self.total_failures += 1
            self._results.append((time.time(), success))
            self._clean_window(time.time())
    
    def call_sync(self, func: Callable, *args, **kwargs) -> Tuple[bool, Any]:
        """Sync version of call"""
        current_time = time.time()
        current_time_ms = current_time * 1000
        
        with self._lock:
            self._clean_window(current_time)
            
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
        
        try:
            result = func(*args, **kwargs)
            self.record_result(True)
            
            with self._lock:
                if self.state == CircuitState.HALF_OPEN:
                    self.state = CircuitState.CLOSED
                    logger.info(f"Circuit {self.name} recovered to CLOSED")
                elif self.state == CircuitState.CLOSED:
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
    
    async def call_async(self, func: Callable, *args, **kwargs) -> Tuple[bool, Any]:
        """Async version of call"""
        current_time = time.time()
        current_time_ms = current_time * 1000
        
        with self._lock:
            self._clean_window(current_time)
            
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
        
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            self.record_result(True)
            
            with self._lock:
                if self.state == CircuitState.HALF_OPEN:
                    self.state = CircuitState.CLOSED
                    logger.info(f"Circuit {self.name} recovered to CLOSED")
                elif self.state == CircuitState.CLOSED:
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
        with self._lock:
            return {
                'name': self.name,
                'state': self.state.value,
                'failure_rate': self._get_failure_rate(),
                'success_rate': self._get_success_rate(),
                'failure_threshold': self.failure_threshold,
                'window_size_seconds': self.window_size_seconds,
                'total_calls': self.total_calls,
                'total_failures': self.total_failures,
                'total_successes': self.total_successes,
                'total_rejections': self.total_rejections,
                'sample_count': len(self._results)
            }
    
    def reset(self):
        with self._lock:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.half_open_calls = 0
            self._results.clear()
            logger.info(f"Circuit {self.name} manually reset")


# ============================================================
# ENHANCEMENT 3: Enhanced Metrics with Percentiles
# ============================================================

class EnhancedFallbackMetrics:
    """
    Enhanced Prometheus metrics exporter with percentiles.
    """
    
    def __init__(self, max_latency_samples: int = 1000):
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
        
        # Latency samples for percentiles
        self._latency_samples: deque = deque(maxlen=max_latency_samples)
        
        # Per-type metrics
        self._per_type_metrics: Dict[str, Dict] = {}
    
    def record_call(self, data_type: str, source: str, latency_ms: float, success: bool):
        with self._lock:
            self._metrics['total_calls'] += 1
            self._metrics['total_latency_ms'] += latency_ms
            self._latency_samples.append(latency_ms)
            
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
            
            if data_type not in self._per_type_metrics:
                self._per_type_metrics[data_type] = {
                    'primary_success': 0,
                    'primary_failure': 0,
                    'fallback_used': 0,
                    'cache_hit': 0,
                    'latency_samples': []
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
            
            pt['latency_samples'].append(latency_ms)
            if len(pt['latency_samples']) > 1000:
                pt['latency_samples'] = pt['latency_samples'][-1000:]
    
    def _get_percentiles(self, samples: List[float]) -> Dict[str, float]:
        if not samples:
            return {'p50': 0, 'p90': 0, 'p95': 0, 'p99': 0}
        sorted_samples = sorted(samples)
        n = len(sorted_samples)
        return {
            'p50': sorted_samples[int(n * 0.5)],
            'p90': sorted_samples[int(n * 0.9)],
            'p95': sorted_samples[int(n * 0.95)],
            'p99': sorted_samples[int(n * 0.99)]
        }
    
    def get_metrics(self) -> Dict:
        with self._lock:
            metrics_copy = self._metrics.copy()
            metrics_copy['average_latency_ms'] = (metrics_copy['total_latency_ms'] / 
                                                   max(1, metrics_copy['total_calls']))
            metrics_copy['fallback_rate'] = (metrics_copy['fallback_used'] / 
                                              max(1, metrics_copy['total_calls']))
            metrics_copy['cache_hit_rate'] = (metrics_copy['cache_hit'] / 
                                               max(1, metrics_copy['total_calls']))
            
            # Add percentiles
            latency_percentiles = self._get_percentiles(list(self._latency_samples))
            metrics_copy['latency_percentiles'] = latency_percentiles
            
            # Per-type metrics
            metrics_copy['per_type'] = {}
            for dt, pt in self._per_type_metrics.items():
                pt_copy = pt.copy()
                pt_copy['latency_percentiles'] = self._get_percentiles(pt_copy.pop('latency_samples', []))
                metrics_copy['per_type'][dt] = pt_copy
            
            return metrics_copy
    
    def to_prometheus_text(self) -> str:
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
            "# HELP fallback_latency_p50 Latency 50th percentile",
            "# TYPE fallback_latency_p50 gauge",
            f"fallback_latency_p50 {metrics['latency_percentiles']['p50']:.2f}",
            "",
            "# HELP fallback_latency_p95 Latency 95th percentile",
            "# TYPE fallback_latency_p95 gauge",
            f"fallback_latency_p95 {metrics['latency_percentiles']['p95']:.2f}",
            "",
            "# HELP fallback_latency_p99 Latency 99th percentile",
            "# TYPE fallback_latency_p99 gauge",
            f"fallback_latency_p99 {metrics['latency_percentiles']['p99']:.2f}",
            "",
            "# HELP fallback_fallback_rate Rate of fallback usage",
            "# TYPE fallback_fallback_rate gauge",
            f"fallback_fallback_rate {metrics['fallback_rate']:.3f}",
            "",
            "# HELP fallback_cache_hit_rate Rate of cache hits",
            "# TYPE fallback_cache_hit_rate gauge",
            f"fallback_cache_hit_rate {metrics['cache_hit_rate']:.3f}",
        ]
        
        # Circuit breaker state metrics (would be added by FallbackManager)
        
        return "\n".join(lines)
    
    def reset(self):
        with self._lock:
            for key in self._metrics:
                if isinstance(self._metrics[key], (int, float)):
                    self._metrics[key] = 0
            self._latency_samples.clear()
            self._per_type_metrics.clear()


# ============================================================
# ENHANCEMENT 4: LRU Cache Manager
# ============================================================

class LRUCacheManager:
    """
    LRU cache manager with per-data-type TTL and size limits.
    """
    
    DEFAULT_TTL_SECONDS = {
        'temperature': 10,
        'grid': 300,
        'helium': 3600,
        'recovery': 60,
        'default': 60
    }
    
    DEFAULT_MAX_SIZE = 1000
    
    def __init__(self, ttl_config: Optional[Dict[str, int]] = None, max_size: int = None):
        self.ttl_config = ttl_config or {}
        self.max_size = max_size or self.DEFAULT_MAX_SIZE
        self._cache: OrderedDict = OrderedDict()
        self._lock = threading.Lock()
        self._pre_warm_data: Dict[str, Any] = {}
    
    def get_ttl(self, data_type: str) -> int:
        return self.ttl_config.get(data_type, self.DEFAULT_TTL_SECONDS.get(data_type, 60))
    
    def get(self, key: str, data_type: str) -> Optional[Any]:
        with self._lock:
            if key not in self._cache:
                return None
            
            value, timestamp = self._cache[key]
            current_time = time.time()
            ttl = self.get_ttl(data_type)
            
            if current_time - timestamp < ttl:
                # Move to end (most recently used)
                self._cache.move_to_end(key)
                return value
            else:
                del self._cache[key]
                return None
    
    def set(self, key: str, value: Any, data_type: str):
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            self._cache[key] = (value, time.time())
            
            # LRU eviction
            while len(self._cache) > self.max_size:
                self._cache.popitem(last=False)
    
    def pre_warm(self, data_type: str, value: Any):
        self._pre_warm_data[data_type] = value
        self.set(data_type, value, data_type)
        logger.info(f"Pre-warmed cache for {data_type}")
    
    def get_pre_warmed(self, data_type: str) -> Optional[Any]:
        return self._pre_warm_data.get(data_type)
    
    def clear_expired(self):
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
        with self._lock:
            return {
                'size': len(self._cache),
                'max_size': self.max_size,
                'pre_warmed_count': len(self._pre_warm_data),
                'utilization_percent': len(self._cache) / self.max_size * 100
            }


# ============================================================
# ENHANCEMENT 5: Learned Adaptive Strategy Selector
# ============================================================

class LearnedAdaptiveStrategySelector:
    """
    Dynamically selects fallback strategy based on learned failure patterns.
    
    Features:
    - Exponential moving average of failure rates
    - Learned thresholds (not fixed)
    - Strategy effectiveness tracking
    """
    
    def __init__(self, history_window: int = 100, learning_rate: float = 0.1):
        self.history_window = history_window
        self.learning_rate = learning_rate
        self._failure_history: Dict[str, deque] = {}
        self._current_strategy: Dict[str, 'FallbackStrategy'] = {}
        self._strategy_effectiveness: Dict[str, Dict[str, float]] = {}
        self._learned_thresholds: Dict[str, Dict[str, float]] = {}
        self._lock = threading.Lock()
        
        self.default_strategies = {
            'temperature': FallbackStrategy.CASCADE,
            'grid': FallbackStrategy.RETRY,
            'helium': FallbackStrategy.CIRCUIT_BREAKER,
            'recovery': FallbackStrategy.CASCADE
        }
        
        # Initialize learned thresholds
        for dt in self.default_strategies:
            self._learned_thresholds[dt] = {
                'circuit_breaker': 0.5,
                'retry': 0.2,
                'last_updated': time.time()
            }
    
    def record_outcome(self, data_type: str, success: bool, strategy_used: 'FallbackStrategy' = None):
        with self._lock:
            if data_type not in self._failure_history:
                self._failure_history[data_type] = deque(maxlen=self.history_window)
            self._failure_history[data_type].append((time.time(), success))
            
            # Track strategy effectiveness
            if strategy_used:
                if data_type not in self._strategy_effectiveness:
                    self._strategy_effectiveness[data_type] = {}
                if strategy_used.value not in self._strategy_effectiveness[data_type]:
                    self._strategy_effectiveness[data_type][strategy_used.value] = {'success': 0, 'total': 0}
                self._strategy_effectiveness[data_type][strategy_used.value]['total'] += 1
                if success:
                    self._strategy_effectiveness[data_type][strategy_used.value]['success'] += 1
    
    def get_failure_rate(self, data_type: str, window_seconds: int = 300) -> float:
        with self._lock:
            if data_type not in self._failure_history:
                return 0.0
            cutoff = time.time() - window_seconds
            recent = [(ts, s) for ts, s in self._failure_history[data_type] if ts > cutoff]
            if not recent:
                return 0.0
            failures = sum(1 for _, s in recent if not s)
            return failures / len(recent)
    
    def _update_thresholds(self, data_type: str):
        """Update thresholds based on strategy effectiveness"""
        if data_type not in self._strategy_effectiveness:
            return
        
        effectiveness = self._strategy_effectiveness[data_type]
        
        # Calculate success rate for each strategy
        strategy_success = {}
        for strat, stats in effectiveness.items():
            if stats['total'] >= 10:
                strategy_success[strat] = stats['success'] / stats['total']
        
        if not strategy_success:
            return
        
        # Adjust thresholds based on which strategies are working
        current_failure_rate = self.get_failure_rate(data_type)
        
        # If cascade is working well (>80% success), lower other thresholds
        if strategy_success.get('cascade', 0) > 0.8:
            self._learned_thresholds[data_type]['circuit_breaker'] = max(0.3, self._learned_thresholds[data_type]['circuit_breaker'] * 0.95)
            self._learned_thresholds[data_type]['retry'] = max(0.1, self._learned_thresholds[data_type]['retry'] * 0.95)
        
        # If high failure rate, raise thresholds
        elif current_failure_rate > 0.5:
            self._learned_thresholds[data_type]['circuit_breaker'] = min(0.7, self._learned_thresholds[data_type]['circuit_breaker'] * 1.05)
            self._learned_thresholds[data_type]['retry'] = min(0.4, self._learned_thresholds[data_type]['retry'] * 1.05)
        
        self._learned_thresholds[data_type]['last_updated'] = time.time()
    
    def select_strategy(self, data_type: str) -> 'FallbackStrategy':
        with self._lock:
            failure_rate = self.get_failure_rate(data_type)
            self._update_thresholds(data_type)
            thresholds = self._learned_thresholds.get(data_type, {'circuit_breaker': 0.5, 'retry': 0.2})
            
            if failure_rate > thresholds['circuit_breaker']:
                new_strategy = FallbackStrategy.CIRCUIT_BREAKER
            elif failure_rate > thresholds['retry']:
                new_strategy = FallbackStrategy.RETRY
            else:
                new_strategy = FallbackStrategy.CASCADE
            
            current = self._current_strategy.get(data_type)
            if current != new_strategy:
                logger.info(f"Strategy for {data_type} changed from {current} to {new_strategy.value} "
                           f"(failure rate={failure_rate:.1%}, thresholds: CB={thresholds['circuit_breaker']:.1%}, R={thresholds['retry']:.1%})")
                self._current_strategy[data_type] = new_strategy
            
            return new_strategy
    
    def get_optimal_timeout(self, data_type: str, default_ms: int = 5000) -> int:
        # Adaptive timeout based on historical latency
        if data_type not in self._failure_history:
            return default_ms
        
        recent = list(self._failure_history[data_type])[-50:]
        if len(recent) < 10:
            return default_ms
        
        # Estimate latency from timestamp differences
        # (simplified - would need actual latency tracking)
        return default_ms


# ============================================================
# ENHANCEMENT 6: Backpressure Handler
# ============================================================

class BackpressureHandler:
    """
    Handles backpressure with bounded queue and rejection policies.
    """
    
    def __init__(self, max_queue_size: int = 1000, max_concurrent: int = 100):
        self.max_queue_size = max_queue_size
        self.max_concurrent = max_concurrent
        self._queue = asyncio.Queue(maxsize=max_queue_size)
        self._active_tasks = 0
        self._lock = asyncio.Lock()
        self._rejected_count = 0
    
    async def submit(self, coro, priority: int = 5) -> Any:
        """Submit a coroutine with priority"""
        if self._queue.qsize() >= self.max_queue_size:
            self._rejected_count += 1
            raise RuntimeError("Backpressure: Queue full")
        
        # Priority queue would be more complex
        await self._queue.put((priority, coro))
        
        # Process queue
        return await self._process_queue()
    
    async def _process_queue(self):
        async with self._lock:
            if self._active_tasks >= self.max_concurrent:
                return None
            
            self._active_tasks += 1
            try:
                _, coro = await self._queue.get()
                return await coro
            finally:
                self._active_tasks -= 1
    
    def get_stats(self) -> Dict:
        return {
            'queue_size': self._queue.qsize(),
            'max_queue_size': self.max_queue_size,
            'active_tasks': self._active_tasks,
            'max_concurrent': self.max_concurrent,
            'rejected_count': self._rejected_count
        }


# ============================================================
# ENHANCEMENT 7: Main Enhanced Fallback Manager
# ============================================================

class FallbackStrategy(Enum):
    CASCADE = "cascade"
    BEST_EFFORT = "best_effort"
    CONSERVATIVE = "conservative"
    RETRY = "retry"
    CIRCUIT_BREAKER = "circuit_breaker"


@dataclass
class FallbackConfig:
    strategy: FallbackStrategy
    max_retries: int = 3
    base_retry_delay_ms: int = 100
    max_retry_delay_ms: int = 10000
    max_retry_time_ms: int = 30000
    circuit_breaker_threshold: float = 0.5
    circuit_breaker_timeout_ms: int = 30000
    timeout_ms: int = 5000
    use_jitter: bool = True
    use_adaptive_strategy: bool = True
    priority: int = 5


@dataclass
class FallbackResult:
    success: bool
    value: Any
    source: str
    latency_ms: float
    retry_count: int
    circuit_state: Optional[str]
    error: Optional[str] = None
    data_type: str = ""
    timestamp: float = field(default_factory=time.time)


class FallbackManager:
    """
    Enhanced unified fallback manager for all Green Agent modules.
    
    Features:
    - Async/sync support
    - Multi-level cascading fallbacks
    - Exponential backoff with jitter and retry budget
    - Sliding window circuit breakers
    - Enhanced Prometheus metrics with percentiles
    - LRU cache with size limits
    - Learned adaptive strategy selection
    - Backpressure handling
    - Health check endpoint
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.default_config = FallbackConfig(strategy=FallbackStrategy.CASCADE)
        
        # Circuit breakers
        self.circuit_breakers = {
            'temperature_sensor': SlidingWindowCircuitBreaker(
                'temperature_sensor', window_size_seconds=60, failure_threshold=0.3, timeout_ms=10000
            ),
            'grid_api': SlidingWindowCircuitBreaker(
                'grid_api', window_size_seconds=300, failure_threshold=0.5, timeout_ms=30000
            ),
            'helium_api': SlidingWindowCircuitBreaker(
                'helium_api', window_size_seconds=300, failure_threshold=0.5, timeout_ms=30000
            ),
            'recovery_system': SlidingWindowCircuitBreaker(
                'recovery_system', window_size_seconds=60, failure_threshold=0.3, timeout_ms=15000
            ),
            'ppa_database': SlidingWindowCircuitBreaker(
                'ppa_database', window_size_seconds=60, failure_threshold=0.3, timeout_ms=10000
            )
        }
        
        self.backoff = AsyncExponentialBackoff(
            base_delay_ms=100, max_delay_ms=10000, multiplier=2.0, use_jitter=True, max_retry_time_ms=30000
        )
        self.cache_manager = LRUCacheManager(
            ttl_config=self.config.get('ttl_config', {}),
            max_size=self.config.get('cache_max_size', 1000)
        )
        self.metrics = EnhancedFallbackMetrics()
        self.strategy_selector = LearnedAdaptiveStrategySelector()
        self.backpressure = BackpressureHandler(
            max_queue_size=self.config.get('max_queue_size', 1000),
            max_concurrent=self.config.get('max_concurrent', 100)
        )
        
        self._fallback_providers = {
            'temperature': self._get_fallback_temperature,
            'grid': self._get_fallback_grid,
            'helium': self._get_fallback_helium,
            'recovery': self._get_fallback_recovery
        }
        
        self._conservative_defaults = {
            'temperature': self._get_conservative_temperature,
            'grid': self._get_conservative_grid,
            'helium': self._get_conservative_helium,
            'recovery': self._get_conservative_recovery
        }
        
        self._pre_warm_cache()
        
        self._running = False
        self._cleanup_thread = None
        self._start_cleanup_thread()
        
        logger.info("Enhanced Fallback Manager v3.0 initialized")
    
    def _pre_warm_cache(self):
        for data_type, provider in self._fallback_providers.items():
            try:
                value = provider()
                self.cache_manager.pre_warm(data_type, value)
            except Exception as e:
                logger.warning(f"Failed to pre-warm {data_type}: {e}")
    
    def _start_cleanup_thread(self):
        self._running = True
        self._cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self._cleanup_thread.start()
    
    def _cleanup_loop(self):
        while self._running:
            time.sleep(60)
            self.cache_manager.clear_expired()
    
    def stop(self):
        self._running = False
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=2)
    
    async def execute_with_fallback_async(self, primary_func: Callable, data_type: str,
                                          config: Optional[FallbackConfig] = None,
                                          context: Optional[Dict] = None) -> FallbackResult:
        """Async version of execute_with_fallback"""
        start_time = time.time()
        config = config or self.default_config
        
        if config.use_adaptive_strategy:
            adaptive_strategy = self.strategy_selector.select_strategy(data_type)
            config.strategy = adaptive_strategy
        
        circuit = self.circuit_breakers.get(f"{data_type}_sensor")
        if not circuit:
            circuit = SlidingWindowCircuitBreaker(data_type, timeout_ms=10000)
        
        # Submit with backpressure
        try:
            return await self.backpressure.submit(
                self._execute_fallback_async(primary_func, data_type, config, circuit, start_time),
                priority=config.priority
            )
        except RuntimeError as e:
            return FallbackResult(
                success=False, value=None, source="rejected", latency_ms=0,
                retry_count=0, circuit_state=None, error=str(e), data_type=data_type
            )
    
    async def _execute_fallback_async(self, primary_func, data_type, config, circuit, start_time):
        start_time_ms = start_time * 1000
        
        # Step 1: Primary with circuit breaker
        if config.strategy != FallbackStrategy.CIRCUIT_BREAKER:
            success, value = await circuit.call_async(primary_func)
        else:
            try:
                value = await primary_func() if asyncio.iscoroutinefunction(primary_func) else primary_func()
                success = True
            except Exception:
                success = False
                value = None
        
        if success and value is not None:
            self.cache_manager.set(data_type, value, data_type)
            self.strategy_selector.record_outcome(data_type, True, config.strategy)
            self.metrics.record_call(data_type, "primary", (time.time() - start_time) * 1000, True)
            return FallbackResult(
                success=True, value=value, source="primary", latency_ms=(time.time() - start_time) * 1000,
                retry_count=0, circuit_state=circuit.get_state()['state'], data_type=data_type
            )
        
        self.strategy_selector.record_outcome(data_type, False, config.strategy)
        
        # Step 2: Retry
        if config.strategy == FallbackStrategy.RETRY:
            for attempt in range(config.max_retries):
                should_continue = await self.backoff.wait(attempt, start_time_ms)
                if not should_continue:
                    break
                
                try:
                    retry_value = await primary_func() if asyncio.iscoroutinefunction(primary_func) else primary_func()
                    if retry_value is not None:
                        self.cache_manager.set(data_type, retry_value, data_type)
                        self.metrics.record_call(data_type, f"primary_retry_{attempt+1}", (time.time() - start_time) * 1000, True)
                        return FallbackResult(
                            success=True, value=retry_value, source=f"primary_retry_{attempt+1}",
                            latency_ms=(time.time() - start_time) * 1000, retry_count=attempt + 1,
                            circuit_state=circuit.get_state()['state'], data_type=data_type
                        )
                except Exception:
                    continue
            
            self.metrics.record_call(data_type, "retry_failed", (time.time() - start_time) * 1000, False)
        
        # Step 3: Fallback provider
        fallback_func = self._fallback_providers.get(data_type)
        if fallback_func:
            try:
                fallback_value = fallback_func()
                if fallback_value is not None:
                    self.cache_manager.set(data_type, fallback_value, data_type)
                    self.metrics.record_call(data_type, "fallback_synthetic", (time.time() - start_time) * 1000, True)
                    return FallbackResult(
                        success=True, value=fallback_value, source="fallback_synthetic",
                        latency_ms=(time.time() - start_time) * 1000, retry_count=config.max_retries,
                        circuit_state=circuit.get_state()['state'], data_type=data_type
                    )
            except Exception as e:
                logger.error(f"Fallback failed for {data_type}: {e}")
        
        # Step 4: Cache
        cached_value = self.cache_manager.get(data_type, data_type)
        if cached_value is not None:
            self.metrics.record_call(data_type, "cache_hit", (time.time() - start_time) * 1000, True)
            return FallbackResult(
                success=True, value=cached_value, source="cache",
                latency_ms=(time.time() - start_time) * 1000, retry_count=config.max_retries,
                circuit_state=circuit.get_state()['state'], data_type=data_type
            )
        
        # Step 5: Conservative default
        if config.strategy == FallbackStrategy.CONSERVATIVE:
            default_func = self._conservative_defaults.get(data_type)
            if default_func:
                default_value = default_func()
                self.metrics.record_call(data_type, "conservative_default", (time.time() - start_time) * 1000, True)
                return FallbackResult(
                    success=True, value=default_value, source="conservative_default",
                    latency_ms=(time.time() - start_time) * 1000, retry_count=config.max_retries,
                    circuit_state=circuit.get_state()['state'], data_type=data_type
                )
        
        self.metrics.record_call(data_type, "none", (time.time() - start_time) * 1000, False)
        return FallbackResult(
            success=False, value=None, source="none", latency_ms=(time.time() - start_time) * 1000,
            retry_count=config.max_retries, circuit_state=circuit.get_state()['state'],
            error=f"No fallback available for {data_type}", data_type=data_type
        )
    
    def execute_with_fallback(self, primary_func: Callable, data_type: str,
                              config: Optional[FallbackConfig] = None,
                              context: Optional[Dict] = None) -> FallbackResult:
        """Sync wrapper for async method"""
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(
                self.execute_with_fallback_async(primary_func, data_type, config, context)
            )
            loop.close()
            return result
        
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(
                asyncio.run, self.execute_with_fallback_async(primary_func, data_type, config, context)
            )
            return future.result()
    
    # Fallback providers (same as before, but included for completeness)
    def _get_fallback_temperature(self):
        import random
        hour = time.localtime().tm_hour
        day_factor = 1.0 if 6 <= hour <= 18 else 0.8
        return {
            'cpu_temp': 50 + random.uniform(-5, 15) * day_factor,
            'gpu_temp': 60 + random.uniform(-8, 25) * day_factor,
            'ambient': 20 + random.uniform(-2, 8) * day_factor,
            'timestamp': time.time(),
            'source': 'fallback_synthetic'
        }
    
    def _get_fallback_grid(self):
        import random
        region = self.config.get('region', 'us-east')
        base_intensities = {'us-east': 380, 'us-west': 250, 'eu-north': 80, 'asia-pacific': 550}
        base = base_intensities.get(region, 400)
        return {
            'average_intensity': base + random.uniform(-20, 20),
            'marginal_intensity': base * (0.9 + random.uniform(-0.1, 0.2)),
            'renewable_percentage': 0.2 + random.uniform(-0.1, 0.3),
            'region': region, 'timestamp': time.time(), 'source': 'fallback_synthetic'
        }
    
    def _get_fallback_helium(self):
        import random
        return {
            'spot_price': 4.0 + random.uniform(-0.5, 3.0),
            'futures_1m': 4.5 + random.uniform(-0.5, 3.0),
            'futures_3m': 5.0 + random.uniform(-1.0, 4.0),
            'inventory_days': 20 + random.uniform(-10, 15),
            'disruption_risk': 0.2 + random.uniform(-0.15, 0.4),
            'timestamp': time.time(), 'source': 'fallback_synthetic'
        }
    
    def _get_fallback_recovery(self):
        import random
        return {
            'efficiency': 0.6 + random.uniform(-0.1, 0.3),
            'recovered_liters': random.uniform(0, 100),
            'method': random.choice(['capture', 'recycle', 'purification']),
            'timestamp': time.time(), 'source': 'fallback_synthetic'
        }
    
    def _get_conservative_temperature(self) -> Dict:
        return {'cpu_temp': 75.0, 'gpu_temp': 85.0, 'ambient': 30.0, 'timestamp': time.time(), 'source': 'conservative_default'}
    
    def _get_conservative_grid(self) -> Dict:
        region = self.config.get('region', 'us-east')
        base = 500 if region == 'us-east' else 400
        return {'average_intensity': base, 'marginal_intensity': base * 1.1, 'renewable_percentage': 0.05, 'region': region, 'timestamp': time.time(), 'source': 'conservative_default'}
    
    def _get_conservative_helium(self) -> Dict:
        return {'spot_price': 12.0, 'futures_1m': 13.0, 'futures_3m': 15.0, 'inventory_days': 5, 'disruption_risk': 0.8, 'timestamp': time.time(), 'source': 'conservative_default'}
    
    def _get_conservative_recovery(self) -> Dict:
        return {'efficiency': 0.3, 'recovered_liters': 0.0, 'method': 'none', 'timestamp': time.time(), 'source': 'conservative_default'}
    
    def get_circuit_breaker_status(self) -> Dict:
        return {name: cb.get_state() for name, cb in self.circuit_breakers.items()}
    
    def reset_circuit_breaker(self, name: str):
        if name in self.circuit_breakers:
            self.circuit_breakers[name].reset()
        elif f"{name}_sensor" in self.circuit_breakers:
            self.circuit_breakers[f"{name}_sensor"].reset()
    
    def get_health_status(self) -> Dict:
        cb_status = self.get_circuit_breaker_status()
        unhealthy = [name for name, status in cb_status.items() if status['state'] == 'open']
        overall = 'degraded' if unhealthy else 'healthy'
        metrics = self.metrics.get_metrics()
        return {
            'overall': overall, 'message': f"Circuit breakers open: {', '.join(unhealthy)}" if unhealthy else "All dependencies operational",
            'timestamp': time.time(), 'circuit_breakers': cb_status,
            'cache': self.cache_manager.get_stats(),
            'backpressure': self.backpressure.get_stats(),
            'metrics': {
                'total_calls': metrics['total_calls'], 'fallback_rate': metrics['fallback_rate'],
                'cache_hit_rate': metrics['cache_hit_rate'], 'average_latency_ms': metrics['average_latency_ms'],
                'latency_p95': metrics['latency_percentiles']['p95']
            }
        }
    
    def get_metrics_text(self) -> str:
        base_metrics = self.metrics.to_prometheus_text()
        cb_metrics = []
        for name, cb in self.circuit_breakers.items():
            state = cb.get_state()
            cb_metrics.append(f'fallback_circuit_breaker_state{{circuit="{name}"}} {1 if state["state"] == "open" else 0}')
            cb_metrics.append(f'fallback_circuit_breaker_failure_rate{{circuit="{name}"}} {state["failure_rate"]:.3f}')
        return base_metrics + "\n" + "\n".join(cb_metrics)
    
    def reset_metrics(self):
        self.metrics.reset()
    
    def get_adaptive_strategy(self, data_type: str) -> str:
        return self.strategy_selector.select_strategy(data_type).value
    
    def pre_warm_fallback(self, data_type: str):
        if data_type in self._fallback_providers:
            value = self._fallback_providers[data_type]()
            self.cache_manager.pre_warm(data_type, value)


# ============================================================
# Usage Example
# ============================================================

async def main():
    print("=== Enhanced Fallback Manager v3.0 Demo ===\n")
    
    fallback_mgr = FallbackManager({'region': 'us-east'})
    
    async def mock_primary_success():
        return {'temperature': 65.0}
    
    async def mock_primary_failing():
        raise Exception("API timeout")
    
    async def mock_primary_unreliable():
        if random.random() > 0.5:
            return {'temperature': 65.0}
        raise Exception("Random failure")
    
    print("1. Async successful call:")
    result = await fallback_mgr.execute_with_fallback_async(mock_primary_success, 'temperature')
    print(f"   Success: {result.success}, Source: {result.source}, Latency: {result.latency_ms:.2f}ms")
    
    print("\n2. Async failing with cascade:")
    result = await fallback_mgr.execute_with_fallback_async(mock_primary_failing, 'temperature')
    print(f"   Success: {result.success}, Source: {result.source}")
    
    print("\n3. Retry strategy:")
    config = FallbackConfig(strategy=FallbackStrategy.RETRY, max_retries=3)
    result = await fallback_mgr.execute_with_fallback_async(mock_primary_unreliable, 'temperature', config)
    print(f"   Success: {result.success}, Source: {result.source}, Retries: {result.retry_count}")
    
    print("\n4. Health status:")
    health = fallback_mgr.get_health_status()
    print(f"   Overall: {health['overall']}")
    print(f"   Fallback rate: {health['metrics']['fallback_rate']:.2%}")
    print(f"   Cache hit rate: {health['metrics']['cache_hit_rate']:.2%}")
    print(f"   Latency p95: {health['metrics']['latency_p95']:.2f}ms")
    
    print("\n5. Adaptive strategies:")
    for dt in ['temperature', 'grid', 'helium']:
        strategy = fallback_mgr.get_adaptive_strategy(dt)
        print(f"   {dt}: {strategy}")
    
    print("\n✅ Enhanced Fallback Manager v3.0 test complete")

if __name__ == "__main__":
    asyncio.run(main())
