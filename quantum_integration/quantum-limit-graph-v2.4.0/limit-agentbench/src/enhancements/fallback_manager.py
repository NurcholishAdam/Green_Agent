# src/enhancements/fallback_manager.py

"""
Complete fallback management system for graceful degradation
Supports cascading fallbacks, circuit breakers, and recovery strategies
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Callable, Any
from enum import Enum
import logging
import time
import threading
from collections import deque

logger = logging.getLogger(__name__)


class FallbackStrategy(Enum):
    """Available fallback strategies"""
    CASCADE = "cascade"      # Try primary, then fallback1, then fallback2
    BEST_EFFORT = "best_effort"  # Use whatever works, don't fail
    CONSERVATIVE = "conservative"  # Use safe default
    RETRY = "retry"           # Retry primary before fallback
    CIRCUIT_BREAKER = "circuit_breaker"  # Trip after failures


class CircuitState(Enum):
    CLOSED = "closed"        # Normal operation
    OPEN = "open"           # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class FallbackConfig:
    """Configuration for a fallback chain"""
    strategy: FallbackStrategy
    max_retries: int = 3
    retry_delay_ms: int = 100
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout_ms: int = 30000
    timeout_ms: int = 5000


@dataclass
class FallbackResult:
    """Result of a fallback execution"""
    success: bool
    value: Any
    source: str  # Which fallback level succeeded
    latency_ms: float
    retry_count: int
    circuit_state: Optional[str]
    error: Optional[str] = None


class CircuitBreaker:
    """Circuit breaker pattern for external dependencies"""
    
    def __init__(self, name: str, threshold: int = 5, timeout_ms: int = 30000):
        self.name = name
        self.threshold = threshold
        self.timeout_ms = timeout_ms
        self.failure_count = 0
        self.state = CircuitState.CLOSED
        self.last_failure_time = 0.0
        self._lock = threading.Lock()
    
    def call(self, func: Callable, *args, **kwargs) -> Tuple[bool, Any]:
        """Execute function with circuit breaker protection"""
        with self._lock:
            if self.state == CircuitState.OPEN:
                if time.time() * 1000 - self.last_failure_time > self.timeout_ms:
                    self.state = CircuitState.HALF_OPEN
                    logger.info(f"Circuit {self.name} transitioning to HALF_OPEN")
                else:
                    return False, None
        
        try:
            result = func(*args, **kwargs)
            
            with self._lock:
                if self.state == CircuitState.HALF_OPEN:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    logger.info(f"Circuit {self.name} recovered to CLOSED")
                elif self.state == CircuitState.CLOSED:
                    self.failure_count = max(0, self.failure_count - 1)
            
            return True, result
            
        except Exception as e:
            with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time() * 1000
                
                if self.failure_count >= self.threshold and self.state == CircuitState.CLOSED:
                    self.state = CircuitState.OPEN
                    logger.error(f"Circuit {self.name} tripped OPEN after {self.failure_count} failures")
            
            return False, None
    
    def get_state(self) -> Dict:
        return {
            'name': self.name,
            'state': self.state.value,
            'failure_count': self.failure_count,
            'threshold': self.threshold
        }


class FallbackManager:
    """
    Unified fallback manager for all enhancement modules.
    Provides cascading fallbacks, circuit breakers, and graceful degradation.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.default_config = FallbackConfig(strategy=FallbackStrategy.CASCADE)
        
        # Circuit breakers for external dependencies
        self.circuit_breakers = {
            'temperature_sensor': CircuitBreaker('temperature_sensor', threshold=3, timeout_ms=10000),
            'grid_api': CircuitBreaker('grid_api', threshold=5, timeout_ms=30000),
            'helium_api': CircuitBreaker('helium_api', threshold=5, timeout_ms=30000),
            'recovery_system': CircuitBreaker('recovery_system', threshold=3, timeout_ms=15000),
            'ppa_database': CircuitBreaker('ppa_database', threshold=3, timeout_ms=10000)
        }
        
        # Fallback providers (simulated data sources)
        self._fallback_data = {
            'temperature': self._get_fallback_temperature,
            'grid': self._get_fallback_grid,
            'helium': self._get_fallback_helium,
            'recovery': self._get_fallback_recovery
        }
        
        self._fallback_cache = {}
        self._last_fallback_time = {}
        
        logger.info("Fallback manager initialized")
    
    def execute_with_fallback(self, primary_func: Callable, data_type: str, 
                             config: Optional[FallbackConfig] = None) -> FallbackResult:
        """
        Execute a function with circuit breaker and cascading fallbacks.
        
        Args:
            primary_func: Primary data source function
            data_type: Type of data (temperature, grid, helium, recovery)
            config: Fallback configuration
            
        Returns:
            FallbackResult with data or error
        """
        start_time = time.time()
        config = config or self.default_config
        circuit = self.circuit_breakers.get(f"{data_type}_sensor", 
                                             CircuitBreaker(data_type, 3, 10000))
        
        # Step 1: Try primary with circuit breaker
        if config.strategy != FallbackStrategy.CIRCUIT_BREAKER:
            success, value = circuit.call(primary_func)
        else:
            # Already using circuit breaker, call directly
            try:
                value = primary_func()
                success = True
            except Exception as e:
                success = False
                value = None
        
        if success and value is not None:
            return FallbackResult(
                success=True,
                value=value,
                source="primary",
                latency_ms=(time.time() - start_time) * 1000,
                retry_count=0,
                circuit_state=circuit.get_state()['state']
            )
        
        # Step 2: Retry if configured
        if config.strategy == FallbackStrategy.RETRY:
            for attempt in range(config.max_retries):
                time.sleep(config.retry_delay_ms / 1000)
                try:
                    value = primary_func()
                    if value is not None:
                        return FallbackResult(
                            success=True,
                            value=value,
                            source=f"primary_retry_{attempt+1}",
                            latency_ms=(time.time() - start_time) * 1000,
                            retry_count=attempt + 1,
                            circuit_state=circuit.get_state()['state']
                        )
                except Exception:
                    continue
        
        # Step 3: Use fallback provider
        fallback_func = self._fallback_data.get(data_type)
        if fallback_func:
            try:
                value = fallback_func()
                if value is not None:
                    self._update_cache(data_type, value)
                    return FallbackResult(
                        success=True,
                        value=value,
                        source="fallback_synthetic",
                        latency_ms=(time.time() - start_time) * 1000,
                        retry_count=config.max_retries,
                        circuit_state=circuit.get_state()['state']
                    )
            except Exception as e:
                logger.error(f"Fallback failed for {data_type}: {e}")
        
        # Step 4: Use cached data if available
        cached = self._get_cached(data_type)
        if cached is not None:
            age_ms = (time.time() - self._last_fallback_time.get(data_type, 0)) * 1000
            return FallbackResult(
                success=True,
                value=cached,
                source=f"cache_{int(age_ms)}ms_old",
                latency_ms=(time.time() - start_time) * 1000,
                retry_count=config.max_retries,
                circuit_state=circuit.get_state()['state']
            )
        
        # Step 5: Conservative default
        if config.strategy == FallbackStrategy.CONSERVATIVE:
            default = self._get_conservative_default(data_type)
            return FallbackResult(
                success=True,
                value=default,
                source="conservative_default",
                latency_ms=(time.time() - start_time) * 1000,
                retry_count=config.max_retries,
                circuit_state=circuit.get_state()['state']
            )
        
        # Complete failure
        return FallbackResult(
            success=False,
            value=None,
            source="none",
            latency_ms=(time.time() - start_time) * 1000,
            retry_count=config.max_retries,
            circuit_state=circuit.get_state()['state'],
            error=f"No fallback available for {data_type}"
        )
    
    def _get_fallback_temperature(self):
        """Synthetic temperature fallback"""
        import random
        return {
            'cpu_temp': 55 + random.uniform(-5, 5),
            'gpu_temp': 65 + random.uniform(-8, 8),
            'ambient': 22,
            'timestamp': time.time()
        }
    
    def _get_fallback_grid(self):
        """Synthetic grid fallback"""
        return {
            'average_intensity': 400,
            'marginal_intensity': 380,
            'renewable_percentage': 0.25,
            'region': 'us-east',
            'timestamp': time.time()
        }
    
    def _get_fallback_helium(self):
        """Synthetic helium fallback"""
        return {
            'spot_price': 5.0,
            'inventory_days': 25,
            'disruption_risk': 0.3,
            'timestamp': time.time()
        }
    
    def _get_fallback_recovery(self):
        """Synthetic recovery fallback"""
        return {
            'efficiency': 0.7,
            'recovered_liters': 0.0,
            'method': 'capture',
            'timestamp': time.time()
        }
    
    def _get_conservative_default(self, data_type: str) -> Dict:
        """Conservative defaults for worst-case planning"""
        defaults = {
            'temperature': {'gpu_temp': 75, 'cpu_temp': 70, 'ambient': 25},
            'grid': {'average_intensity': 500, 'renewable_percentage': 0.1},
            'helium': {'spot_price': 10.0, 'inventory_days': 10},
            'recovery': {'efficiency': 0.5}
        }
        return defaults.get(data_type, {})
    
    def _update_cache(self, data_type: str, value: Any):
        """Update cache with fallback data"""
        self._fallback_cache[data_type] = value
        self._last_fallback_time[data_type] = time.time()
    
    def _get_cached(self, data_type: str) -> Optional[Any]:
        """Get cached data if not too old"""
        if data_type in self._fallback_cache:
            age_ms = (time.time() - self._last_fallback_time.get(data_type, 0)) * 1000
            if age_ms < 60000:  # 1 minute cache TTL
                return self._fallback_cache[data_type]
        return None
    
    def get_circuit_breaker_status(self) -> Dict:
        """Get status of all circuit breakers"""
        return {name: cb.get_state() for name, cb in self.circuit_breakers.items()}
    
    def reset_circuit_breaker(self, name: str):
        """Manually reset a circuit breaker"""
        if name in self.circuit_breakers:
            self.circuit_breakers[name].state = CircuitState.CLOSED
            self.circuit_breakers[name].failure_count = 0
            logger.info(f"Circuit breaker {name} manually reset")
