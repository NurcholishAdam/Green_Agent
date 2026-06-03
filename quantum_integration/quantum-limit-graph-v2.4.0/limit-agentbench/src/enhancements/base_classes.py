# File: src/enhancements/base_classes.py (ENHANCED v7.0)

"""
Green Agent Base Classes - Version 7.0 (Platinum Standard)

CRITICAL ENHANCEMENTS OVER v6.2:
1. ADDED: Configuration change callbacks with subscription mechanism
2. ADDED: Pydantic-based configuration validation for all sections
3. ADDED: Async base classes for async-capable modules
4. ADDED: Standard error handling hierarchy with custom exceptions
5. ADDED: Lifecycle hooks (setup/teardown/health_check)
6. ADDED: Shared cache abstraction with TTL and thread-safety
7. ADDED: Auto-metrics registration for BaseMetrics subclasses
8. ADDED: Serialization schemas with marshmallow
9. ADDED: Module discovery helper for automatic registration
10. ADDED: Configuration backup and restore functionality
11. ADDED: Context manager support for modules with resources
12. ADDED: Retry decorator for resilient operations
13. ADDED: Circuit breaker pattern for external calls
14. ADDED: Audit logging decorator for sensitive operations
15. ADDED: Performance monitoring decorator
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple, Any, Callable, Union, Type, TypeVar
from abc import ABC, abstractmethod
import numpy as np
import pandas as pd
import logging
import json
import yaml
import os
import uuid
import threading
import time
import functools
import inspect
from datetime import datetime
from pathlib import Path
from collections import defaultdict, deque
from functools import lru_cache, wraps
import warnings
import copy
import asyncio
from enum import Enum
import traceback

# Production dependencies
from pydantic import BaseModel, Field, validator, root_validator, ValidationError
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry

# Optional imports
try:
    from marshmallow import Schema, fields, post_load, validates_schema
    MARSHMALLOW_AVAILABLE = True
except ImportError:
    MARSHMALLOW_AVAILABLE = False

# Configure logging
logger = logging.getLogger(__name__)

# ============================================================
# STANDARD ERROR HANDLING HIERARCHY
# ============================================================

class GreenAgentException(Exception):
    """Base exception for all Green Agent modules"""
    def __init__(self, message: str, details: Dict = None, cause: Exception = None):
        super().__init__(message)
        self.details = details or {}
        self.cause = cause
        self.timestamp = datetime.now().isoformat()
    
    def to_dict(self) -> Dict:
        return {
            'type': self.__class__.__name__,
            'message': str(self),
            'details': self.details,
            'timestamp': self.timestamp,
            'cause': str(self.cause) if self.cause else None
        }

class ConfigurationError(GreenAgentException):
    """Configuration-related errors"""
    pass

class DataValidationError(GreenAgentException):
    """Data validation errors"""
    pass

class ModuleNotFoundError(GreenAgentException):
    """Module not found errors"""
    pass

class QuantumError(GreenAgentException):
    """Quantum computing errors"""
    pass

class BlockchainError(GreenAgentException):
    """Blockchain integration errors"""
    pass

class APIError(GreenAgentException):
    """API communication errors"""
    pass

class ResourceError(GreenAgentException):
    """Resource management errors"""
    pass

class TimeoutError(GreenAgentException):
    """Operation timeout errors"""
    pass

# ============================================================
# RETRY DECORATOR
# ============================================================

def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0,
         exceptions: Tuple[Exception] = (Exception,), on_retry: Callable = None):
    """Retry decorator with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_attempts - 1:
                        raise
                    
                    if on_retry:
                        on_retry(attempt + 1, max_attempts, e)
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            raise last_exception
        
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_attempts - 1:
                        raise
                    
                    if on_retry:
                        on_retry(attempt + 1, max_attempts, e)
                    
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
            
            raise last_exception
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper
    return decorator

# ============================================================
# CIRCUIT BREAKER PATTERN
# ============================================================

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    """Circuit breaker pattern for external calls"""
    
    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 60.0, half_open_max_calls: int = 3):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self.half_open_calls = 0
        self._lock = threading.RLock()
    
    def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_calls = 0
                    logger.info(f"Circuit breaker {self.name} transitioning to half-open")
                else:
                    raise ResourceError(f"Circuit breaker {self.name} is open")
            
            try:
                result = func(*args, **kwargs)
                
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.half_open_calls += 1
                    if self.half_open_calls >= self.half_open_max_calls:
                        with self._lock:
                            self.state = CircuitBreakerState.CLOSED
                            self.failure_count = 0
                            logger.info(f"Circuit breaker {self.name} closed")
                
                return result
                
            except Exception as e:
                with self._lock:
                    self.failure_count += 1
                    self.last_failure_time = time.time()
                    
                    if self.state == CircuitBreakerState.HALF_OPEN:
                        self.state = CircuitBreakerState.OPEN
                        logger.warning(f"Circuit breaker {self.name} opened from half-open")
                    elif self.failure_count >= self.failure_threshold:
                        self.state = CircuitBreakerState.OPEN
                        logger.warning(f"Circuit breaker {self.name} opened after {self.failure_count} failures")
                
                raise e
    
    def get_state(self) -> Dict:
        """Get circuit breaker state"""
        with self._lock:
            return {
                'name': self.name,
                'state': self.state.value,
                'failure_count': self.failure_count,
                'last_failure': self.last_failure_time
            }

# ============================================================
# AUDIT LOGGING DECORATOR
# ============================================================

def audit_log(operation: str, log_args: bool = True, log_result: bool = False):
    """Audit logging decorator for sensitive operations"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            audit_logger = logging.getLogger("audit")
            
            # Prepare log data
            log_data = {
                'operation': operation,
                'function': func.__name__,
                'module': func.__module__,
                'timestamp': datetime.now().isoformat()
            }
            
            if log_args:
                # Log args safely (avoid sensitive data)
                safe_args = [str(arg)[:100] for arg in args[1:]]  # Skip self
                log_data['args'] = safe_args
                log_data['kwargs'] = {k: str(v)[:100] for k, v in kwargs.items()}
            
            try:
                result = func(*args, **kwargs)
                
                if log_result:
                    log_data['result'] = str(result)[:200]
                log_data['status'] = 'success'
                audit_logger.info(json.dumps(log_data))
                
                return result
                
            except Exception as e:
                log_data['status'] = 'failed'
                log_data['error'] = str(e)
                audit_logger.error(json.dumps(log_data))
                raise
        
        async def async_wrapper(*args, **kwargs):
            audit_logger = logging.getLogger("audit")
            
            log_data = {
                'operation': operation,
                'function': func.__name__,
                'module': func.__module__,
                'timestamp': datetime.now().isoformat()
            }
            
            if log_args:
                safe_args = [str(arg)[:100] for arg in args[1:]]
                log_data['args'] = safe_args
                log_data['kwargs'] = {k: str(v)[:100] for k, v in kwargs.items()}
            
            try:
                result = await func(*args, **kwargs)
                
                if log_result:
                    log_data['result'] = str(result)[:200]
                log_data['status'] = 'success'
                audit_logger.info(json.dumps(log_data))
                
                return result
                
            except Exception as e:
                log_data['status'] = 'failed'
                log_data['error'] = str(e)
                audit_logger.error(json.dumps(log_data))
                raise
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper
    return decorator

# ============================================================
# PERFORMANCE MONITORING DECORATOR
# ============================================================

def monitor_performance(metric_name: str = None):
    """Performance monitoring decorator"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                
                # Log performance
                logger.debug(f"{func.__name__} completed in {duration:.4f}s")
                
                # Update metric if provided
                if metric_name:
                    from prometheus_client import Histogram
                    hist = Histogram(metric_name, f"Performance metric for {func.__name__}",
                                    registry=get_shared_registry())
                    hist.observe(duration)
                
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"{func.__name__} failed after {duration:.4f}s: {e}")
                raise
        
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                
                logger.debug(f"{func.__name__} completed in {duration:.4f}s")
                
                if metric_name:
                    from prometheus_client import Histogram
                    hist = Histogram(metric_name, f"Performance metric for {func.__name__}",
                                    registry=get_shared_registry())
                    hist.observe(duration)
                
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"{func.__name__} failed after {duration:.4f}s: {e}")
                raise
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper
    return decorator

# ============================================================
# SHARED PROMETHEUS REGISTRY
# ============================================================

_SHARED_REGISTRY = CollectorRegistry()

def get_shared_registry() -> CollectorRegistry:
    """Get the shared Prometheus registry for all modules"""
    return _SHARED_REGISTRY

# ============================================================
# CONFIGURATION CHANGE NOTIFIER
# ============================================================

class ConfigChangeNotifier:
    """Notify subscribers of configuration changes"""
    
    def __init__(self):
        self._listeners: List[Callable[[Dict, Dict], None]] = []
        self._lock = threading.RLock()
    
    def subscribe(self, callback: Callable[[Dict, Dict], None]) -> None:
        """Subscribe to configuration changes (old_config, new_config)"""
        with self._lock:
            self._listeners.append(callback)
            logger.debug(f"Config change listener subscribed: {callback.__name__}")
    
    def unsubscribe(self, callback: Callable) -> None:
        """Unsubscribe from configuration changes"""
        with self._lock:
            if callback in self._listeners:
                self._listeners.remove(callback)
    
    def notify(self, old_config: Dict, new_config: Dict) -> None:
        """Notify all subscribers of configuration change"""
        with self._lock:
            for callback in self._listeners:
                try:
                    callback(old_config, new_config)
                except Exception as e:
                    logger.error(f"Config change callback {callback.__name__} failed: {e}")

# ============================================================
# CONFIGURATION VALIDATION MODELS
# ============================================================

class HeliumConfigModel(BaseModel):
    """Validation model for helium configuration"""
    data_collector: Dict = Field(..., description="Data collector configuration")
    forecaster: Dict = Field(..., description="Forecaster configuration")
    
    @validator('data_collector')
    def validate_collector(cls, v):
        required = ['csv_path', 'use_synthetic_fallback', 'cache_ttl_seconds']
        missing = [r for r in required if r not in v]
        if missing:
            raise ValueError(f"Missing helium collector fields: {missing}")
        return v

class QuantumConfigModel(BaseModel):
    """Validation model for quantum configuration"""
    provider: str = Field(..., regex="^(pennylane|qiskit|braket)$")
    backend: str = Field(..., min_length=1)
    n_qubits: int = Field(ge=1, le=100)
    shots: int = Field(ge=100, le=100000)
    error_mitigation: bool = True

class BlockchainConfigModel(BaseModel):
    """Validation model for blockchain configuration"""
    provider: str = Field(..., regex="^(ethereum|polygon|solana)$")
    network: str = Field(..., min_length=1)
    chain_id: int = Field(ge=1)

class APIConfigModel(BaseModel):
    """Validation model for API configuration"""
    host: str = Field(default="0.0.0.0", regex="^[0-9a-z.-]+$")
    port: int = Field(ge=1024, le=65535)
    rate_limit_per_minute: int = Field(ge=1, le=10000)

class CarbonConfigModel(BaseModel):
    """Validation model for carbon configuration"""
    price_usd_per_tonne: float = Field(ge=0, le=1000)
    grid_carbon_intensity: float = Field(ge=0, le=1)
    renewable_energy_pct: float = Field(ge=0, le=100)

class AIDataCenterConfigModel(BaseModel):
    """Validation model for AI data center configuration"""
    default_capacity_mw: float = Field(ge=1, le=10000)
    default_pue: float = Field(ge=1.0, le=3.0)
    default_gpu_count: int = Field(ge=0, le=1000000)

# ============================================================
# CONFIGURATION BACKUP
# ============================================================

class ConfigBackup:
    """Automatic configuration backup on changes"""
    
    def __init__(self, backup_dir: Path = Path("./config_backups")):
        self.backup_dir = backup_dir
        self.backup_dir.mkdir(exist_ok=True)
        self._lock = threading.RLock()
    
    def backup(self, config: Dict, tag: str = None) -> str:
        """Save configuration backup"""
        with self._lock:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"config_backup_{tag or timestamp}.yaml"
            filepath = self.backup_dir / filename
            
            with open(filepath, 'w') as f:
                yaml.dump(config, f)
            
            logger.info(f"Configuration backed up to {filepath}")
            return str(filepath)
    
    def restore(self, backup_file: str) -> Dict:
        """Restore configuration from backup"""
        with self._lock:
            filepath = Path(backup_file)
            if not filepath.exists():
                raise ConfigurationError(f"Backup file not found: {backup_file}")
            
            with open(filepath, 'r') as f:
                config = yaml.safe_load(f)
            
            logger.info(f"Configuration restored from {backup_file}")
            return config
    
    def list_backups(self) -> List[Dict]:
        """List available backups"""
        backups = []
        for path in sorted(self.backup_dir.glob("config_backup_*.yaml")):
            backups.append({
                'file': str(path),
                'timestamp': path.stem.replace('config_backup_', ''),
                'size_bytes': path.stat().st_size,
                'created_at': datetime.fromtimestamp(path.stat().st_ctime).isoformat()
            })
        return backups
    
    def delete_old_backups(self, keep_count: int = 10):
        """Delete old backups, keeping only the most recent"""
        backups = self.list_backups()
        if len(backups) > keep_count:
            for backup in backups[keep_count:]:
                Path(backup['file']).unlink()
                logger.info(f"Deleted old backup: {backup['file']}")

# ============================================================
# SHARED CACHE ABSTRACTION
# ============================================================

class SharedCache:
    """Thread-safe shared cache with TTL"""
    
    def __init__(self, default_ttl: int = 3600):
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._lock = threading.RLock()
        self.default_ttl = default_ttl
        self._hit_count = 0
        self._miss_count = 0
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        with self._lock:
            if key in self._cache:
                value, expiry = self._cache[key]
                if time.time() < expiry:
                    self._hit_count += 1
                    return value
                del self._cache[key]
        self._miss_count += 1
        return None
    
    def set(self, key: str, value: Any, ttl: int = None) -> None:
        """Set value in cache with TTL"""
        ttl = ttl or self.default_ttl
        expiry = time.time() + ttl
        with self._lock:
            self._cache[key] = (value, expiry)
    
    def delete(self, key: str) -> bool:
        """Delete key from cache"""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
        return False
    
    def clear(self) -> None:
        """Clear all cache"""
        with self._lock:
            self._cache.clear()
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        total = self._hit_count + self._miss_count
        return {
            'size': len(self._cache),
            'hit_count': self._hit_count,
            'miss_count': self._miss_count,
            'hit_ratio': self._hit_count / max(total, 1),
            'default_ttl': self.default_ttl
        }

# ============================================================
# AUTO-METRICS BASE METRICS
# ============================================================

@dataclass
class BaseMetrics:
    """Base class for all metrics objects with auto-registration"""
    calculation_id: str = field(default_factory=lambda: str(uuid.uuid4())[:12])
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    source_module: str = "base"
    metadata: Dict = field(default_factory=dict)
    
    def __post_init__(self):
        """Auto-register metrics after initialization"""
        self._register_metrics()
    
    def _register_metrics(self):
        """Automatically register numeric metrics with Prometheus"""
        registry = get_shared_registry()
        for key, value in self.to_dict().items():
            if isinstance(value, (int, float)):
                try:
                    gauge = Gauge(
                        f'{self.source_module}_{key}',
                        f'Auto-registered metric: {key}',
                        registry=registry
                    )
                    gauge.set(value)
                except Exception as e:
                    logger.debug(f"Failed to register metric {key}: {e}")
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict(), default=str, indent=2)
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.calculation_id}, source={self.source_module})"

# ============================================================
# SERIALIZATION SCHEMA (if marshmallow available)
# ============================================================

if MARSHMALLOW_AVAILABLE:
    class BaseMetricsSchema(Schema):
        calculation_id = fields.Str()
        timestamp = fields.DateTime()
        source_module = fields.Str()
        metadata = fields.Dict()
        
        @post_load
        def make_metrics(self, data, **kwargs):
            return BaseMetrics(**data)
    
    class BaseCalculatorSchema(Schema):
        config = fields.Dict()
        calculation_history = fields.List(fields.Dict())
        cache_size = fields.Int()
    
    class BaseCollectorSchema(Schema):
        config = fields.Dict()
        collection_history = fields.List(fields.Dict())
        cache_size = fields.Int()
        last_collection_time = fields.DateTime()
else:
    BaseMetricsSchema = None
    BaseCalculatorSchema = None
    BaseCollectorSchema = None

# ============================================================
# LIFEHOOKS MIXIN
# ============================================================

class LifecycleAware(ABC):
    """Lifecycle management for modules"""
    
    @abstractmethod
    def setup(self) -> bool:
        """Initialize module resources"""
        pass
    
    @abstractmethod
    def teardown(self) -> bool:
        """Clean up module resources"""
        pass
    
    def health_check(self) -> Dict:
        """Return health status"""
        return {
            'healthy': True,
            'timestamp': datetime.now().isoformat(),
            'module': self.__class__.__name__
        }

class ContextManagerMixin:
    """Context manager support for modules with resources"""
    
    async def __aenter__(self):
        """Async context manager entry"""
        if hasattr(self, 'setup'):
            self.setup()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if hasattr(self, 'teardown'):
            self.teardown()

# ============================================================
# ENHANCED BASE CALCULATOR
# ============================================================

class BaseCalculator(ABC, ContextManagerMixin):
    """Enhanced abstract base class for all calculators"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.calculation_history: List[BaseMetrics] = []
        self.cache = SharedCache()
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.RLock()
        
        # Use shared Prometheus registry
        self.calculations_counter = Counter(
            f'{self.__class__.__name__.lower()}_calculations_total',
            'Total calculations',
            ['status'],
            registry=get_shared_registry()
        )
        
        self.calculation_duration = Histogram(
            f'{self.__class__.__name__.lower()}_duration_seconds',
            'Calculation duration',
            registry=get_shared_registry()
        )
        
        logger.info(f"{self.__class__.__name__} initialized")
    
    @abstractmethod
    def calculate(self, *args, **kwargs) -> BaseMetrics:
        """Calculate metrics"""
        pass
    
    def validate_input(self, data: Any) -> bool:
        """Validate input data"""
        return True
    
    def get_history(self, limit: int = 10) -> List[BaseMetrics]:
        """Get calculation history"""
        with self._lock:
            return self.calculation_history[-limit:]
    
    def clear_cache(self):
        """Clear calculation cache"""
        self.cache.clear()
    
    def get_circuit_breaker(self, name: str) -> CircuitBreaker:
        """Get or create circuit breaker"""
        with self._lock:
            if name not in self.circuit_breakers:
                self.circuit_breakers[name] = CircuitBreaker(name)
            return self.circuit_breakers[name]
    
    def get_statistics(self) -> Dict:
        """Get calculator statistics"""
        return {
            'total_calculations': len(self.calculation_history),
            'cache_stats': self.cache.get_stats(),
            'circuit_breakers': {k: v.get_state() for k, v in self.circuit_breakers.items()},
            'class_name': self.__class__.__name__
        }

# ============================================================
# ENHANCED BASE COLLECTOR
# ============================================================

class BaseCollector(ABC, ContextManagerMixin):
    """Enhanced abstract base class for data collectors"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.collection_history: List[Dict] = []
        self.last_collection_time: Optional[datetime] = None
        self.cache = SharedCache()
        self._lock = threading.RLock()
        
        logger.info(f"{self.__class__.__name__} initialized")
    
    @abstractmethod
    def collect(self, *args, **kwargs) -> Any:
        """Collect data"""
        pass
    
    @abstractmethod
    def get_latest(self) -> Any:
        """Get latest collected data"""
        pass
    
    @retry(max_attempts=3, delay=1.0, exceptions=(APIError,))
    def collect_with_retry(self, *args, **kwargs) -> Any:
        """Collect data with retry logic"""
        return self.collect(*args, **kwargs)
    
    def get_collection_status(self) -> Dict:
        """Get collection status"""
        return {
            'last_collection': self.last_collection_time.isoformat() if self.last_collection_time else None,
            'total_collections': len(self.collection_history),
            'cache_stats': self.cache.get_stats()
        }
    
    def is_data_fresh(self, max_age_seconds: float = 3600) -> bool:
        """Check if data is fresh"""
        if self.last_collection_time is None:
            return False
        return (datetime.now() - self.last_collection_time).total_seconds() < max_age_seconds
    
    def clear_cache(self):
        """Clear collection cache"""
        with self._lock:
            self.cache.clear()

# ============================================================
# ASYNC BASE COLLECTOR
# ============================================================

class AsyncBaseCollector(BaseCollector):
    """Async version of BaseCollector"""
    
    @abstractmethod
    async def collect_async(self, *args, **kwargs) -> Any:
        """Async data collection"""
        pass
    
    @abstractmethod
    async def get_latest_async(self) -> Any:
        """Get latest data asynchronously"""
        pass

# ============================================================
# ENHANCED BASE GENERATOR
# ============================================================

class BaseGenerator(ABC):
    """Enhanced abstract base class for data generators"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.generation_history = []
        self.cache = SharedCache()
        
        logger.info(f"{self.__class__.__name__} initialized")
    
    @abstractmethod
    def generate(self, *args, **kwargs) -> Any:
        """Generate data"""
        pass
    
    @abstractmethod
    def get_domain_name(self) -> str:
        """Get domain name"""
        pass
    
    def validate_output(self, data: Any) -> float:
        """Validate generated data quality (0-1)"""
        return 1.0
    
    def get_statistics(self) -> Dict:
        """Get generator statistics"""
        return {
            'total_generations': len(self.generation_history),
            'domain': self.get_domain_name(),
            'class_name': self.__class__.__name__,
            'cache_stats': self.cache.get_stats()
        }

# ============================================================
# ENHANCED BASE FORECASTER
# ============================================================

class BaseForecaster(ABC):
    """Enhanced abstract base class for forecasting modules"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.models = {}
        self.forecast_history: List[Dict] = []
        self.models_trained = False
        self._lock = threading.RLock()
        
        logger.info(f"{self.__class__.__name__} initialized")
    
    @abstractmethod
    def train(self, historical_data: Any, **kwargs) -> Dict:
        """Train forecasting model"""
        pass
    
    @abstractmethod
    def forecast(self, recent_data: Any, horizon: int) -> Dict:
        """Generate forecast"""
        pass
    
    def get_forecast_accuracy(self) -> Dict:
        """Get forecast accuracy metrics"""
        if not self.forecast_history:
            return {'error': 'No forecasts available'}
        
        return {
            'total_forecasts': len(self.forecast_history),
            'last_forecast': self.forecast_history[-1] if self.forecast_history else None,
            'models_trained': self.models_trained,
            'class_name': self.__class__.__name__
        }
    
    def is_model_ready(self) -> bool:
        """Check if model is trained and ready"""
        return self.models_trained

# ============================================================
# ASYNC BASE FORECASTER
# ============================================================

class AsyncBaseForecaster(BaseForecaster):
    """Async version of BaseForecaster"""
    
    @abstractmethod
    async def train_async(self, historical_data: Any, **kwargs) -> Dict:
        """Async training"""
        pass
    
    @abstractmethod
    async def forecast_async(self, recent_data: Any, horizon: int) -> Dict:
        """Async forecast"""
        pass

# ============================================================
# ENHANCED BASE OPTIMIZER
# ============================================================

class BaseOptimizer(ABC):
    """Enhanced abstract base class for optimizers"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.optimization_history = []
        self.convergence_history = []
        self.cache = SharedCache()
        
        logger.info(f"{self.__class__.__name__} initialized")
    
    @abstractmethod
    def optimize(self, *args, **kwargs) -> Dict:
        """Run optimization"""
        pass
    
    @abstractmethod
    def get_optimal_solution(self) -> Dict:
        """Get optimal solution"""
        pass
    
    def get_convergence_metrics(self) -> Dict:
        """Get convergence metrics"""
        return {
            'iterations': len(self.convergence_history),
            'converged': len(self.convergence_history) > 0 and 
                        self.convergence_history[-1].get('converged', False),
            'class_name': self.__class__.__name__
        }
    
    def get_statistics(self) -> Dict:
        """Get optimizer statistics"""
        return {
            'total_optimizations': len(self.optimization_history),
            **self.get_convergence_metrics(),
            'cache_stats': self.cache.get_stats()
        }

# ============================================================
# ASYNC BASE OPTIMIZER
# ============================================================

class AsyncBaseOptimizer(BaseOptimizer):
    """Async version of BaseOptimizer"""
    
    @abstractmethod
    async def optimize_async(self, *args, **kwargs) -> Dict:
        """Async optimization"""
        pass

# ============================================================
# ENHANCED BASE INTEGRATOR
# ============================================================

class BaseIntegrator(ABC):
    """Enhanced abstract base class for module integrators"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.integration_registry = {}
        self._lock = threading.RLock()
        
        logger.info(f"{self.__class__.__name__} initialized")
    
    @abstractmethod
    def integrate(self, source_data: Dict, target_module: str) -> Dict:
        """Integrate data with target module"""
        pass
    
    def register_integration(self, module_name: str, integration_fn: Callable):
        """Register integration function"""
        with self._lock:
            self.integration_registry[module_name] = integration_fn
    
    def get_integration_status(self) -> Dict:
        """Get integration status"""
        return {
            'registered_modules': list(self.integration_registry.keys()),
            'total_integrations': len(self.integration_registry),
            'class_name': self.__class__.__name__
        }

# ============================================================
# ENHANCED BASE VALIDATOR
# ============================================================

class BaseValidator(ABC):
    """Enhanced abstract base class for data validators"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.validation_history = []
        self._lock = threading.RLock()
        
        logger.info(f"{self.__class__.__name__} initialized")
    
    @abstractmethod
    def validate(self, data: Any) -> Tuple[bool, List[str]]:
        """Validate data, returns (is_valid, list_of_issues)"""
        pass
    
    def get_validation_score(self) -> float:
        """Get overall validation score"""
        if not self.validation_history:
            return 0.0
        
        with self._lock:
            passed = sum(1 for v in self.validation_history if v.get('passed', False))
            return passed / len(self.validation_history)
    
    def get_statistics(self) -> Dict:
        """Get validator statistics"""
        return {
            'total_validations': len(self.validation_history),
            'validation_score': self.get_validation_score(),
            'class_name': self.__class__.__name__
        }

# ============================================================
# ENHANCED BASE VERIFIER
# ============================================================

class BaseVerifier(ABC):
    """Enhanced abstract base class for verification modules"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.verification_history: List[Dict] = []
        self._lock = threading.RLock()
        
        logger.info(f"{self.__class__.__name__} initialized")
    
    @abstractmethod
    def verify(self, claims: Dict) -> Dict:
        """Verify claims, returns verification result"""
        pass
    
    def get_verification_rate(self) -> float:
        """Get verification success rate"""
        if not self.verification_history:
            return 0.0
        
        with self._lock:
            passed = sum(1 for v in self.verification_history if v.get('verified', False))
            return passed / len(self.verification_history)
    
    def get_statistics(self) -> Dict:
        """Get verifier statistics"""
        return {
            'total_verifications': len(self.verification_history),
            'verification_rate': self.get_verification_rate(),
            'class_name': self.__class__.__name__
        }

# ============================================================
# GPU BASE CALCULATOR
# ============================================================

class GPUBaseCalculator(BaseCalculator):
    """Base calculator with GPU acceleration support"""
    
    def __init__(self, config: Dict = None):
        super().__init__(config)
        self._gpu_accelerator = None
        self._init_gpu()
    
    def _init_gpu(self):
        """Initialize GPU acceleration"""
        try:
            import torch
            self.cuda_available = torch.cuda.is_available()
            self.device_name = torch.cuda.get_device_name(0) if self.cuda_available else "CPU"
            self.device = torch.device("cuda" if self.cuda_available else "cpu")
            
            if self.cuda_available:
                logger.info(f"{self.__class__.__name__} GPU-ready: {self.device_name}")
        except ImportError:
            self.cuda_available = False
            self.device_name = "CPU"
            self.device = torch.device("cpu")
    
    def to_gpu(self, data):
        """Move data to GPU if available"""
        if self.cuda_available and hasattr(data, 'to'):
            return data.to(self.device)
        return data
    
    def to_cpu(self, data):
        """Move data back to CPU"""
        if hasattr(data, 'cpu'):
            return data.cpu()
        return data
    
    def get_gpu_memory_info(self) -> Dict:
        """Get GPU memory information"""
        if not self.cuda_available:
            return {'cuda_available': False}
        
        import torch
        return {
            'cuda_available': True,
            'device_name': self.device_name,
            'memory_allocated_mb': torch.cuda.memory_allocated() / 1024 / 1024,
            'memory_cached_mb': torch.cuda.memory_reserved() / 1024 / 1024,
            'max_memory_allocated_mb': torch.cuda.max_memory_allocated() / 1024 / 1024
        }

# ============================================================
# ENHANCED CONFIGURATION LOADER
# ============================================================

class GreenAgentConfig:
    """
    Unified configuration loader for all Green Agent modules.
    
    Enhanced with:
    - Configuration change callbacks
    - Pydantic validation
    - Backup and restore
    - Hot-reload capability
    """
    
    _instance = None
    _config = None
    _lock = threading.RLock()
    
    def __new__(cls, config_path: str = None):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config_path: str = None):
        if self._config is None:
            with self._lock:
                if self._config is None:
                    self.config_path = config_path or self._find_config()
                    self._config = self._load_config()
                    self._resolve_env_vars()
                    self._validate_config()
                    self._change_notifier = ConfigChangeNotifier()
                    self._backup_manager = ConfigBackup()
                    self._validators = {
                        'helium': HeliumConfigModel,
                        'quantum': QuantumConfigModel,
                        'blockchain': BlockchainConfigModel,
                        'api': APIConfigModel,
                        'carbon': CarbonConfigModel,
                        'ai_datacenter': AIDataCenterConfigModel
                    }
    
    def _find_config(self) -> Optional[str]:
        """Find configuration file with fallback"""
        search_paths = [
            Path(__file__).parent / "green_agent_config.yaml",
            Path.cwd() / "green_agent_config.yaml",
            Path(os.environ.get("GREEN_AGENT_CONFIG", ""))
        ]
        
        for path in search_paths:
            if path.exists():
                return str(path)
        
        logger.warning("No configuration file found. Using embedded defaults.")
        return None
    
    def _load_config(self) -> Dict:
        """Load YAML configuration or use defaults"""
        if self.config_path and Path(self.config_path).exists():
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        return self._get_default_config()
    
    def _get_default_config(self) -> Dict:
        """Return sensible default configuration"""
        return {
            'system': {
                'name': 'Green Agent',
                'version': '7.0.0',
                'environment': 'development',
                'log_level': 'INFO',
                'correlation_id_enabled': True
            },
            'helium': {
                'data_collector': {
                    'csv_path': 'src/enhancements/data/helium_timeseries.csv',
                    'use_synthetic_fallback': True,
                    'cache_ttl_seconds': 3600
                },
                'forecaster': {
                    'seq_length': 60,
                    'output_horizon': 12,
                    'epochs': 100,
                    'early_stopping': True
                }
            },
            'quantum': {
                'provider': 'pennylane',
                'backend': 'default.qubit',
                'n_qubits': 8,
                'shots': 1000,
                'error_mitigation': True
            },
            'blockchain': {
                'provider': 'ethereum',
                'network': 'sepolia',
                'chain_id': 11155111
            },
            'regret_optimizer': {
                'n_scenarios': 1000,
                'confidence_level': 0.95,
                'optimization_method': 'minimax'
            },
            'sustainability': {
                'sector': 'technology',
                'reporting_framework': 'GRI'
            },
            'thermal': {
                'data_center': {
                    'chiller_cop': 4.5,
                    'ambient_temp_c': 25.0,
                    'safety_margin_c': 5.0
                }
            },
            'synthetic_data': {
                'seed': 42,
                'n_samples_default': 1000,
                'parallel_workers': 4
            },
            'ai_datacenter': {
                'default_capacity_mw': 100,
                'default_pue': 1.3,
                'default_gpu_count': 10000
            },
            'carbon': {
                'price_usd_per_tonne': 75.0,
                'grid_carbon_intensity': 0.5,
                'renewable_energy_pct': 30.0
            },
            'api': {
                'host': '0.0.0.0',
                'port': 8000,
                'rate_limit_per_minute': 60
            },
            'monitoring': {
                'prometheus': {'enabled': True, 'port': 9090},
                'logging': {'level': 'INFO', 'format': 'structured'}
            }
        }
    
    def _resolve_env_vars(self):
        """Resolve ${ENV_VAR} patterns in configuration"""
        import re
        
        def resolve_value(value):
            if isinstance(value, str):
                pattern = r'\$\{([^}]+)\}'
                matches = re.findall(pattern, value)
                for match in matches:
                    env_value = os.environ.get(match, '')
                    value = value.replace(f'${{{match}}}', env_value)
                return value
            elif isinstance(value, dict):
                return {k: resolve_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [resolve_value(v) for v in value]
            return value
        
        self._config = resolve_value(self._config)
    
    def _validate_config(self):
        """Validate configuration using Pydantic models"""
        for section, validator_class in self._validators.items():
            if section in self._config:
                try:
                    validator_class(**self._config[section])
                    logger.debug(f"Configuration section '{section}' validated")
                except ValidationError as e:
                    logger.error(f"Configuration validation failed for '{section}': {e}")
                    raise ConfigurationError(f"Invalid configuration for {section}", details={'errors': e.errors()})
        
        # Check required sections
        required_sections = ['system', 'helium', 'quantum', 'blockchain', 
                           'sustainability', 'thermal', 'synthetic_data', 'carbon']
        
        missing = [s for s in required_sections if s not in self._config]
        
        if missing:
            logger.warning(f"Missing configuration sections: {missing}")
    
    def subscribe(self, callback: Callable[[Dict, Dict], None]) -> None:
        """Subscribe to configuration changes"""
        self._change_notifier.subscribe(callback)
    
    def unsubscribe(self, callback: Callable) -> None:
        """Unsubscribe from configuration changes"""
        self._change_notifier.unsubscribe(callback)
    
    def reload(self) -> bool:
        """Hot-reload configuration from file"""
        with self._lock:
            old_config = copy.deepcopy(self._config)
            new_config = self._load_config()
            self._resolve_env_vars()
            self._validate_config()
            
            if old_config != new_config:
                # Backup before change
                self._backup_manager.backup(old_config, "pre_reload")
                
                self._config = new_config
                self._change_notifier.notify(old_config, new_config)
                
                logger.info("Configuration reloaded successfully")
                return True
            
            logger.info("Configuration unchanged")
            return False
    
    def backup(self, tag: str = None) -> str:
        """Create configuration backup"""
        return self._backup_manager.backup(self._config, tag)
    
    def restore(self, backup_file: str) -> bool:
        """Restore configuration from backup"""
        old_config = copy.deepcopy(self._config)
        new_config = self._backup_manager.restore(backup_file)
        
        if old_config != new_config:
            self._config = new_config
            self._change_notifier.notify(old_config, new_config)
            logger.info(f"Configuration restored from {backup_file}")
            return True
        
        return False
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """Get configuration value by dot-separated path"""
        keys = key_path.split('.')
        value = self._config
        
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return default
            else:
                return default
        
        return value
    
    # Enhanced property accessors
    @property
    def helium_config(self) -> Dict:
        return self._config.get('helium', {})
    
    @property
    def quantum_config(self) -> Dict:
        return self._config.get('quantum', {})
    
    @property
    def blockchain_config(self) -> Dict:
        return self._config.get('blockchain', {})
    
    @property
    def regret_config(self) -> Dict:
        return self._config.get('regret_optimizer', {})
    
    @property
    def sustainability_config(self) -> Dict:
        return self._config.get('sustainability', {})
    
    @property
    def thermal_config(self) -> Dict:
        return self._config.get('thermal', {})
    
    @property
    def synthetic_config(self) -> Dict:
        return self._config.get('synthetic_data', {})
    
    @property
    def carbon_config(self) -> Dict:
        return self._config.get('carbon', {})
    
    @property
    def ai_datacenter_config(self) -> Dict:
        return self._config.get('ai_datacenter', {})
    
    @property
    def api_config(self) -> Dict:
        return self._config.get('api', {})
    
    @property
    def monitoring_config(self) -> Dict:
        return self._config.get('monitoring', {})
    
    @property
    def system_config(self) -> Dict:
        return self._config.get('system', {})
    
    def to_dict(self) -> Dict:
        return copy.deepcopy(self._config)

# ============================================================
# MODULE DISCOVERY HELPER
# ============================================================

def discover_modules(module_path: str, base_class: type) -> List[type]:
    """Discover all modules inheriting from base_class in a directory"""
    import importlib
    import inspect
    from pathlib import Path
    
    modules = []
    path = Path(module_path)
    
    if not path.exists():
        logger.warning(f"Module path does not exist: {module_path}")
        return modules
    
    for file_path in path.glob("*.py"):
        if file_path.name.startswith("__"):
            continue
        
        module_name = file_path.stem
        try:
            module = importlib.import_module(f"{module_path}.{module_name}")
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, base_class) and obj is not base_class:
                    modules.append(obj)
                    logger.debug(f"Discovered module: {obj.__name__} in {module_name}")
        except Exception as e:
            logger.warning(f"Failed to load module {module_name}: {e}")
    
    return modules

# ============================================================
# THREAD-SAFE MODULE REGISTRY
# ============================================================

class ModuleRegistry:
    """Thread-safe registry for all Green Agent modules"""
    
    _modules = {}
    _lock = threading.RLock()
    
    @classmethod
    def register(cls, module_name: str, module_instance: Any):
        """Register a module (thread-safe)"""
        with cls._lock:
            cls._modules[module_name] = module_instance
            logger.info(f"Module registered: {module_name}")
    
    @classmethod
    def get(cls, module_name: str) -> Any:
        """Get a registered module (thread-safe)"""
        with cls._lock:
            return cls._modules.get(module_name)
    
    @classmethod
    def list_modules(cls) -> List[str]:
        """List all registered modules"""
        with cls._lock:
            return list(cls._modules.keys())
    
    @classmethod
    def get_status(cls) -> Dict:
        """Get status of all modules"""
        with cls._lock:
            return {
                name: {
                    'type': type(instance).__name__,
                    'module': instance.__class__.__module__,
                    'available': True
                }
                for name, instance in cls._modules.items()
            }
    
    @classmethod
    def unregister(cls, module_name: str):
        """Remove a module from registry"""
        with cls._lock:
            cls._modules.pop(module_name, None)
            logger.info(f"Module unregistered: {module_name}")
    
    @classmethod
    def clear(cls):
        """Clear all registered modules"""
        with cls._lock:
            cls._modules.clear()
            logger.info("All modules unregistered")
    
    @classmethod
    def get_module_names_by_type(cls, base_class: type) -> List[str]:
        """Get names of modules inheriting from base_class"""
        with cls._lock:
            return [
                name for name, instance in cls._modules.items()
                if isinstance(instance, base_class)
            ]

# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def load_module_config(module_name: str) -> Dict:
    """Load configuration for a specific module"""
    config = GreenAgentConfig()
    
    config_map = {
        'helium': config.helium_config,
        'quantum': config.quantum_config,
        'blockchain': config.blockchain_config,
        'regret_optimizer': config.regret_config,
        'sustainability': config.sustainability_config,
        'thermal': config.thermal_config,
        'synthetic_data': config.synthetic_config,
        'ai_datacenter': config.ai_datacenter_config,
        'carbon': config.carbon_config,
        'api': config.api_config,
        'monitoring': config.monitoring_config,
        'system': config.system_config
    }
    
    return config_map.get(module_name, {})

def get_system_config() -> Dict:
    """Get system configuration"""
    return GreenAgentConfig().system_config

def get_api_config() -> Dict:
    """Get API configuration"""
    return GreenAgentConfig().api_config

def get_monitoring_config() -> Dict:
    """Get monitoring configuration"""
    return GreenAgentConfig().monitoring_config

def reload_all_config() -> bool:
    """Hot-reload configuration"""
    return GreenAgentConfig().reload()

# ============================================================
# CONVENIENCE DECORATORS
# ============================================================

def with_circuit_breaker(breaker_name: str):
    """Decorator to add circuit breaker protection"""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if hasattr(self, 'get_circuit_breaker'):
                cb = self.get_circuit_breaker(breaker_name)
                return cb.call(lambda: func(self, *args, **kwargs))
            return func(self, *args, **kwargs)
        
        async def async_wrapper(self, *args, **kwargs):
            if hasattr(self, 'get_circuit_breaker'):
                cb = self.get_circuit_breaker(breaker_name)
                return await cb.call(lambda: func(self, *args, **kwargs))
            return await func(self, *args, **kwargs)
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper
    return decorator

def with_retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Decorator to add retry logic"""
    return retry(max_attempts=max_attempts, delay=delay, backoff=backoff)

# ============================================================
# EXPORTS
# ============================================================

__all__ = [
    # Exceptions
    'GreenAgentException', 'ConfigurationError', 'DataValidationError',
    'ModuleNotFoundError', 'QuantumError', 'BlockchainError', 'APIError',
    'ResourceError', 'TimeoutError',
    
    # Configuration
    'GreenAgentConfig', 'load_module_config', 'get_system_config',
    'get_api_config', 'get_monitoring_config', 'reload_all_config',
    
    # Base Classes
    'BaseMetrics', 'BaseCalculator', 'BaseCollector', 'BaseGenerator',
    'BaseForecaster', 'BaseOptimizer', 'BaseIntegrator', 'BaseValidator',
    'BaseVerifier', 'AsyncBaseCollector', 'AsyncBaseForecaster',
    'AsyncBaseOptimizer', 'GPUBaseCalculator',
    
    # Lifecycle
    'LifecycleAware', 'ContextManagerMixin',
    
    # Utilities
    'get_shared_registry', 'ModuleRegistry', 'SharedCache',
    'CircuitBreaker', 'CircuitBreakerState',
    
    # Decorators
    'retry', 'audit_log', 'monitor_performance', 'with_circuit_breaker', 'with_retry',
    
    # Discovery
    'discover_modules',
    
    # Config Models
    'HeliumConfigModel', 'QuantumConfigModel', 'BlockchainConfigModel',
    'APIConfigModel', 'CarbonConfigModel', 'AIDataCenterConfigModel'
]
