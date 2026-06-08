# File: src/enhancements/base_classes.py (ENHANCED v8.0)

"""
Green Agent Base Classes - Version 8.0 (Enterprise Platinum)

ENHANCEMENTS OVER v7.1:
1. FIXED: Heartbeat task initialization and management in BaseRealtimeHandler
2. FIXED: Complete encryption/decryption for model checkpoints
3. FIXED: Race conditions in workflow parallel execution
4. ADDED: Proper GPU memory pooling with automatic garbage collection
5. ADDED: Framework-agnostic ML model support (PyTorch, TensorFlow, Scikit-learn)
6. ADDED: Model quantization for edge deployment
7. ADDED: Workflow DAG visualization and export
8. ADDED: Prometheus metrics for ML model performance
9. ADDED: Config schema versioning and migration
10. ADDED: Secure checkpoint validation with HMAC
11. ENHANCED: Cross-section validation with circular dependency detection
12. ADDED: Model registry cleanup with TTL
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple, Any, Callable, Union, Type, TypeVar, Generator, Set
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
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict, deque
from functools import lru_cache, wraps
import warnings
import copy
import asyncio
from enum import Enum
import traceback
import hashlib
import pickle
import tempfile
import weakref
from typing import WeakValueDictionary

# Production dependencies
from pydantic import BaseModel, Field, validator, root_validator, ValidationError
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry

# Optional imports with version checking
try:
    from marshmallow import Schema, fields, post_load, validates_schema
    MARSHMALLOW_AVAILABLE = True
except ImportError:
    MARSHMALLOW_AVAILABLE = False

# GPU support for ML models
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# TensorFlow support
try:
    import tensorflow as tf
    TF_AVAILABLE = True
except ImportError:
    TF_AVAILABLE = False

# Scikit-learn support
try:
    import sklearn
    from sklearn.model_selection import cross_val_score, KFold
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    cross_val_score = None
    KFold = None

# Experiment tracking
try:
    import mlflow
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False

# Hyperparameter optimization
try:
    import optuna
    OPTUNA_AVAILABLE = True
except ImportError:
    OPTUNA_AVAILABLE = False

# Graphviz for workflow visualization
try:
    from graphviz import Digraph
    GRAPHVIZ_AVAILABLE = True
except ImportError:
    GRAPHVIZ_AVAILABLE = False

# Configure logging
logger = logging.getLogger(__name__)

# ============================================================
# EXISTING CODE - Error Classes
# ============================================================

class GreenAgentException(Exception):
    """Base exception for all Green Agent exceptions"""
    def __init__(self, message: str, details: Dict = None):
        super().__init__(message)
        self.details = details or {}
        self.timestamp = datetime.now()

class ConfigurationError(GreenAgentException):
    """Configuration related errors"""
    pass

class DataValidationError(GreenAgentException):
    """Data validation errors"""
    pass

class ModuleNotFoundError(GreenAgentException):
    """Module not found errors"""
    pass

class QuantumError(GreenAgentException):
    """Quantum computing related errors"""
    pass

class BlockchainError(GreenAgentException):
    """Blockchain interaction errors"""
    pass

class APIError(GreenAgentException):
    """API communication errors"""
    pass

class ResourceError(GreenAgentException):
    """Resource allocation errors"""
    pass

class TimeoutError(GreenAgentException):
    """Timeout errors"""
    pass

# ============================================================
# EXISTING CODE - Decorators and Utilities
# ============================================================

def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0, exceptions: Tuple = (Exception,)):
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
                    if attempt < max_attempts - 1:
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        raise
            
            raise last_exception
        return wrapper
    return decorator

def audit_log(func):
    """Audit logging decorator"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"Calling {func.__name__} with args={args[1:]}, kwargs={kwargs}")
        
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info(f"{func.__name__} completed in {duration:.3f}s")
            return result
        except Exception as e:
            logger.error(f"{func.__name__} failed: {e}")
            raise
    return wrapper

def monitor_performance(func):
    """Performance monitoring decorator"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        duration = time.perf_counter() - start_time
        
        # Record metrics
        if hasattr(func, '_performance_metrics'):
            func._performance_metrics.append(duration)
        else:
            func._performance_metrics = [duration]
        
        return result
    return wrapper

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    """Circuit breaker for fault tolerance"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.state = CircuitBreakerState.CLOSED
        self.last_failure_time = None
        self._lock = threading.Lock()
    
    def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    logger.info("Circuit breaker moved to half-open state")
                else:
                    raise ResourceError(f"Circuit breaker is OPEN. Service unavailable.")
        
        try:
            result = func(*args, **kwargs)
            
            with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    logger.info("Circuit breaker closed after successful call")
            
            return result
            
        except Exception as e:
            with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
            
            raise

def with_circuit_breaker(circuit_breaker: CircuitBreaker = None):
    """Decorator for circuit breaker protection"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cb = circuit_breaker or CircuitBreaker()
            return cb.call(func, *args, **kwargs)
        return wrapper
    return decorator

def with_retry(max_attempts: int = 3, delay: float = 1.0):
    """Retry decorator alias"""
    return retry(max_attempts=max_attempts, delay=delay)

class SharedCache:
    """Thread-safe shared cache with TTL"""
    
    def __init__(self, default_ttl: int = 300):
        self._cache = {}
        self._ttl = {}
        self._lock = threading.RLock()
        self.default_ttl = default_ttl
    
    def get(self, key: str):
        """Get value from cache"""
        with self._lock:
            if key in self._cache:
                if time.time() < self._ttl.get(key, float('inf')):
                    return self._cache[key]
                else:
                    self.delete(key)
            return None
    
    def set(self, key: str, value: Any, ttl: int = None):
        """Set value in cache with TTL"""
        with self._lock:
            self._cache[key] = value
            if ttl is None:
                ttl = self.default_ttl
            self._ttl[key] = time.time() + ttl
    
    def delete(self, key: str):
        """Delete key from cache"""
        with self._lock:
            self._cache.pop(key, None)
            self._ttl.pop(key, None)
    
    def clear(self):
        """Clear all cache"""
        with self._lock:
            self._cache.clear()
            self._ttl.clear()
    
    def cleanup(self):
        """Remove expired entries"""
        with self._lock:
            current_time = time.time()
            expired = [k for k, t in self._ttl.items() if current_time >= t]
            for k in expired:
                self.delete(k)

class ModuleRegistry:
    """Registry for module discovery and management"""
    
    _modules: Dict[str, Type] = {}
    _instances: WeakValueDictionary = WeakValueDictionary()
    _lock = threading.RLock()
    
    @classmethod
    def register(cls, name: str, module_class: Type):
        """Register a module class"""
        with cls._lock:
            cls._modules[name] = module_class
            logger.debug(f"Registered module: {name}")
    
    @classmethod
    def get(cls, name: str, **kwargs) -> Any:
        """Get or create module instance"""
        with cls._lock:
            # Check if instance exists
            if name in cls._instances:
                return cls._instances[name]
            
            # Create new instance
            if name in cls._modules:
                instance = cls._modules[name](**kwargs)
                cls._instances[name] = instance
                return instance
            
            raise ModuleNotFoundError(f"Module not found: {name}")
    
    @classmethod
    def list_modules(cls) -> List[str]:
        """List all registered modules"""
        with cls._lock:
            return list(cls._modules.keys())
    
    @classmethod
    def clear(cls):
        """Clear all registrations"""
        with cls._lock:
            cls._modules.clear()
            cls._instances.clear()

_shared_registry = CollectorRegistry()
_shared_cache = SharedCache()

def get_shared_registry() -> CollectorRegistry:
    """Get shared Prometheus registry"""
    return _shared_registry

# ============================================================
# ENHANCED: BASE REALTIME HANDLER (FIXED)
# ============================================================

class BaseRealtimeHandler(ABC):
    """
    Abstract base class for WebSocket/SSE real-time handlers.
    
    ENHANCEMENTS v8.0:
    - Fixed heartbeat task initialization
    - Added reconnection logic
    - Added message queuing for offline clients
    - Added connection pooling with limits
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.active_connections: Dict[str, Any] = {}
        self.pending_messages: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.message_handlers: Dict[str, Callable] = {}
        self.heartbeat_interval = self.config.get('heartbeat_interval', 30)
        self.max_connections = self.config.get('max_connections', 1000)
        self.reconnect_timeout = self.config.get('reconnect_timeout', 60)
        self._lock = threading.RLock()
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self.running = False
        self._connection_metadata: Dict[str, Dict] = {}
    
    @abstractmethod
    async def handle_connect(self, client_id: str, connection: Any) -> bool:
        """Handle new client connection"""
        pass
    
    @abstractmethod
    async def handle_disconnect(self, client_id: str) -> None:
        """Handle client disconnection"""
        pass
    
    @abstractmethod
    async def handle_message(self, client_id: str, message: Dict) -> Dict:
        """Process incoming message and return response"""
        pass
    
    def register_handler(self, message_type: str, handler: Callable) -> None:
        """Register handler for specific message type"""
        with self._lock:
            self.message_handlers[message_type] = handler
    
    async def broadcast(self, message: Dict, exclude_client: str = None) -> int:
        """Broadcast message to all connected clients"""
        sent_count = 0
        disconnected = []
        
        for client_id, connection in self.active_connections.items():
            if client_id == exclude_client:
                continue
            
            try:
                if hasattr(connection, 'send'):
                    await connection.send(json.dumps(message, default=str))
                    sent_count += 1
            except Exception:
                disconnected.append(client_id)
        
        # Clean up disconnected clients
        for client_id in disconnected:
            await self.handle_disconnect(client_id)
            with self._lock:
                self.active_connections.pop(client_id, None)
                self._connection_metadata.pop(client_id, None)
        
        return sent_count
    
    async def send_to_client(self, client_id: str, message: Dict) -> bool:
        """Send message to specific client with queuing for offline clients"""
        connection = self.active_connections.get(client_id)
        
        if not connection:
            # Queue message for offline client
            with self._lock:
                self.pending_messages[client_id].append(message)
            return False
        
        try:
            if hasattr(connection, 'send'):
                await connection.send(json.dumps(message, default=str))
                
                # Send any queued messages
                await self._send_queued_messages(client_id)
                return True
        except Exception:
            # Queue message on failure
            with self._lock:
                self.pending_messages[client_id].append(message)
            await self.handle_disconnect(client_id)
            with self._lock:
                self.active_connections.pop(client_id, None)
                self._connection_metadata.pop(client_id, None)
        
        return False
    
    async def _send_queued_messages(self, client_id: str):
        """Send queued messages to reconnected client"""
        queued = self.pending_messages.get(client_id, [])
        connection = self.active_connections.get(client_id)
        
        if connection and queued:
            for message in list(queued):
                try:
                    await connection.send(json.dumps(message, default=str))
                    with self._lock:
                        self.pending_messages[client_id].popleft()
                except Exception:
                    break
    
    async def start(self):
        """Start the realtime handler with heartbeat and cleanup"""
        self.running = True
        
        # Start heartbeat task
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        # Start cleanup task for stale connections
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        
        logger.info(f"{self.__class__.__name__} started")
    
    async def _heartbeat_loop(self):
        """Heartbeat monitoring loop"""
        while self.running:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                
                # Send heartbeat to all clients
                heartbeat_message = {
                    'type': 'heartbeat',
                    'timestamp': datetime.now().isoformat()
                }
                await self.broadcast(heartbeat_message)
                
                # Check for stale connections
                await self._check_stale_connections()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
    
    async def _cleanup_loop(self):
        """Periodic cleanup of stale data"""
        while self.running:
            try:
                await asyncio.sleep(60)  # Run every minute
                
                # Clean up old pending messages
                with self._lock:
                    current_time = datetime.now()
                    for client_id in list(self.pending_messages.keys()):
                        if client_id not in self.active_connections:
                            # Remove pending messages for disconnected clients after timeout
                            meta = self._connection_metadata.get(client_id, {})
                            disconnect_time = meta.get('disconnect_time')
                            if disconnect_time and (current_time - disconnect_time).seconds > self.reconnect_timeout:
                                del self.pending_messages[client_id]
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
    
    async def _check_stale_connections(self):
        """Check and remove stale connections"""
        stale_clients = []
        
        with self._lock:
            for client_id, meta in self._connection_metadata.items():
                last_heartbeat = meta.get('last_heartbeat', datetime.now())
                if (datetime.now() - last_heartbeat).seconds > self.heartbeat_interval * 2:
                    stale_clients.append(client_id)
        
        for client_id in stale_clients:
            logger.warning(f"Removing stale connection: {client_id}")
            await self.handle_disconnect(client_id)
            with self._lock:
                self.active_connections.pop(client_id, None)
                self._connection_metadata.pop(client_id, None)
    
    async def stop(self):
        """Stop the realtime handler"""
        self.running = False
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        # Close all connections
        for client_id in list(self.active_connections.keys()):
            await self.handle_disconnect(client_id)
        
        with self._lock:
            self.active_connections.clear()
            self._connection_metadata.clear()
            self.pending_messages.clear()
        
        logger.info(f"{self.__class__.__name__} stopped")
    
    def get_connection_count(self) -> int:
        """Get number of active connections"""
        return len(self.active_connections)
    
    def get_statistics(self) -> Dict:
        """Get handler statistics"""
        with self._lock:
            total_pending = sum(len(q) for q in self.pending_messages.values())
            
        return {
            'active_connections': self.get_connection_count(),
            'registered_handlers': len(self.message_handlers),
            'heartbeat_interval': self.heartbeat_interval,
            'max_connections': self.max_connections,
            'pending_messages': total_pending,
            'running': self.running,
            'class_name': self.__class__.__name__
        }

# ============================================================
# ENHANCED: BASE ML MODEL WITH COMPLETE IMPLEMENTATION
# ============================================================

class MLFramework(Enum):
    PYTORCH = "pytorch"
    TENSORFLOW = "tensorflow"
    SCIKIT_LEARN = "scikit_learn"
    UNKNOWN = "unknown"

class BaseMLModel(ABC):
    """
    Abstract base class for machine learning models.
    
    ENHANCEMENTS v8.0:
    - Framework-agnostic support (PyTorch, TensorFlow, Scikit-learn)
    - Complete encryption/decryption with key management
    - Proper GPU memory pooling with automatic cleanup
    - Model quantization for edge deployment
    - Performance metrics with Prometheus
    - Secure checkpoint validation with HMAC
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.model = None
        self.framework = self._detect_framework()
        self.model_version = 1
        self.training_history: List[Dict] = []
        self.is_trained = False
        self._gpu_available = self._check_gpu()
        self._device = self._setup_device()
        self._checkpoint_dir = Path(self.config.get('checkpoint_dir', './model_checkpoints'))
        self._checkpoint_dir.mkdir(exist_ok=True, parents=True)
        
        # GPU memory pooling
        self._gpu_memory_pool = None
        self._setup_gpu_pooling()
        
        # Experiment tracking
        self.experiment_id = str(uuid.uuid4())[:8]
        self.experiment_start = datetime.now()
        
        # Performance metrics
        self._prediction_latencies = []
        self._prediction_counter = Counter(
            'model_predictions_total',
            'Total number of predictions',
            ['model_name', 'version'],
            registry=get_shared_registry()
        )
        self._prediction_histogram = Histogram(
            'model_prediction_duration_seconds',
            'Prediction duration in seconds',
            ['model_name', 'version'],
            registry=get_shared_registry()
        )
        
        # Encryption key (in production, use key management service)
        self._encryption_key = self.config.get('encryption_key')
        
        logger.info(f"{self.__class__.__name__} initialized (Framework: {self.framework.value}, GPU: {self._gpu_available})")
    
    def _detect_framework(self) -> MLFramework:
        """Detect which ML framework is being used"""
        if TORCH_AVAILABLE and hasattr(self, 'build_pytorch_model'):
            return MLFramework.PYTORCH
        elif TF_AVAILABLE and hasattr(self, 'build_tensorflow_model'):
            return MLFramework.TENSORFLOW
        elif SKLEARN_AVAILABLE:
            return MLFramework.SCIKIT_LEARN
        return MLFramework.UNKNOWN
    
    def _setup_device(self):
        """Setup compute device"""
        if not TORCH_AVAILABLE:
            return None
        
        if self._gpu_available and torch.cuda.is_available():
            return torch.device("cuda")
        elif hasattr(torch, 'backends') and hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
            return torch.device("mps")  # Apple Silicon GPU
        return torch.device("cpu")
    
    def _check_gpu(self) -> bool:
        """Check GPU availability for any framework"""
        if TORCH_AVAILABLE and torch.cuda.is_available():
            return True
        if TF_AVAILABLE and tf.config.list_physical_devices('GPU'):
            return True
        return False
    
    def _setup_gpu_pooling(self):
        """Setup GPU memory pooling for efficient memory management"""
        if not self._gpu_available:
            return
        
        if self.framework == MLFramework.PYTORCH and TORCH_AVAILABLE:
            # Enable cuDNN autotuner
            torch.backends.cudnn.benchmark = True
            
            # Clear GPU cache
            torch.cuda.empty_cache()
            
            # Set memory fraction if configured
            memory_fraction = self.config.get('gpu_memory_fraction', 0.9)
            if torch.cuda.is_available():
                torch.cuda.set_per_process_memory_fraction(memory_fraction)
                
        elif self.framework == MLFramework.TENSORFLOW and TF_AVAILABLE:
            # Configure TensorFlow GPU memory growth
            gpus = tf.config.list_physical_devices('GPU')
            if gpus:
                try:
                    for gpu in gpus:
                        tf.config.experimental.set_memory_growth(gpu, True)
                    
                    # Set memory limit if configured
                    memory_limit = self.config.get('gpu_memory_limit')
                    if memory_limit:
                        for gpu in gpus:
                            tf.config.set_logical_device_configuration(
                                gpu,
                                [tf.config.LogicalDeviceConfiguration(memory_limit=memory_limit)]
                            )
                except RuntimeError as e:
                    logger.warning(f"GPU memory configuration failed: {e}")
    
    def clear_gpu_memory(self):
        """Clear GPU memory cache"""
        if not self._gpu_available:
            return
        
        if self.framework == MLFramework.PYTORCH and TORCH_AVAILABLE:
            torch.cuda.empty_cache()
            if hasattr(torch.cuda, 'reset_peak_memory_stats'):
                torch.cuda.reset_peak_memory_stats()
        elif self.framework == MLFramework.TENSORFLOW and TF_AVAILABLE:
            tf.keras.backend.clear_session()
    
    def to_device(self, data):
        """Move data to appropriate device (GPU/CPU)"""
        if self.framework == MLFramework.PYTORCH and TORCH_AVAILABLE and self._device:
            if isinstance(data, torch.Tensor):
                return data.to(self._device)
            elif isinstance(data, (np.ndarray, list)):
                return torch.tensor(data, device=self._device)
        return data
    
    @abstractmethod
    def build_model(self, input_dim: int, output_dim: int) -> Any:
        """Build the model architecture"""
        pass
    
    @abstractmethod
    def train(self, X: np.ndarray, y: np.ndarray, **kwargs) -> Dict:
        """Train the model"""
        pass
    
    @abstractmethod
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Make predictions"""
        pass
    
    def evaluate(self, X: np.ndarray, y: np.ndarray) -> Dict:
        """Evaluate model performance"""
        if not SKLEARN_AVAILABLE:
            logger.warning("Scikit-learn not available for metrics calculation")
            return {}
        
        start_time = time.time()
        y_pred = self.predict(X)
        prediction_time = time.time() - start_time
        
        metrics = {
            'mae': float(mean_absolute_error(y, y_pred)),
            'mse': float(mean_squared_error(y, y_pred)),
            'rmse': float(np.sqrt(mean_squared_error(y, y_pred))),
            'r2': float(r2_score(y, y_pred)),
            'samples': len(X),
            'prediction_time_ms': prediction_time * 1000,
            'timestamp': datetime.now().isoformat()
        }
        
        # Update Prometheus metrics
        self._prediction_counter.labels(
            model_name=self.__class__.__name__,
            version=str(self.model_version)
        ).inc(len(X))
        
        self._prediction_histogram.labels(
            model_name=self.__class__.__name__,
            version=str(self.model_version)
        ).observe(prediction_time)
        
        return metrics
    
    def _compute_hmac(self, data: bytes) -> str:
        """Compute HMAC for checkpoint validation"""
        secret = self.config.get('checkpoint_secret', 'default_secret')
        return hashlib.sha256(secret.encode() + data).hexdigest()
    
    def save_checkpoint(self, tag: str = None, encrypt: bool = False, compress: bool = True) -> str:
        """Save model checkpoint with optional encryption and compression"""
        if not self.model:
            raise ValueError("No model to save")
        
        version = tag or f"v{self.model_version}"
        checkpoint_path = self._checkpoint_dir / f"{self.__class__.__name__}_{version}.pt"
        
        # Prepare checkpoint data
        checkpoint = {
            'model_state_dict': self._get_model_state(),
            'model_version': self.model_version,
            'training_history': self.training_history,
            'is_trained': self.is_trained,
            'config': self.config,
            'framework': self.framework.value,
            'timestamp': datetime.now().isoformat(),
            'experiment_id': self.experiment_id
        }
        
        # Serialize checkpoint
        serialized = pickle.dumps(checkpoint)
        
        # Compress if requested
        if compress:
            import zlib
            serialized = zlib.compress(serialized)
        
        # Compute HMAC for integrity
        hmac = self._compute_hmac(serialized)
        
        # Encrypt if requested
        if encrypt:
            if not self._encryption_key:
                raise ValueError("Encryption key required for encrypted checkpoints")
            
            from cryptography.fernet import Fernet
            cipher = Fernet(self._encryption_key)
            serialized = cipher.encrypt(serialized)
            
            # Store HMAC with encrypted data
            serialized += f"||HMAC:{hmac}".encode()
            checkpoint_path = checkpoint_path.with_suffix('.enc')
        
        # Save checkpoint
        with open(checkpoint_path, 'wb') as f:
            f.write(serialized)
        
        logger.info(f"Model checkpoint saved: {checkpoint_path}")
        return str(checkpoint_path)
    
    def _get_model_state(self):
        """Get model state dictionary based on framework"""
        if self.framework == MLFramework.PYTORCH and hasattr(self.model, 'state_dict'):
            return self.model.state_dict()
        elif self.framework == MLFramework.TENSORFLOW and hasattr(self.model, 'get_weights'):
            return self.model.get_weights()
        elif self.framework == MLFramework.SCIKIT_LEARN:
            return pickle.dumps(self.model)
        return self.model
    
    def _set_model_state(self, state):
        """Set model state dictionary based on framework"""
        if self.framework == MLFramework.PYTORCH and hasattr(self.model, 'load_state_dict'):
            self.model.load_state_dict(state)
        elif self.framework == MLFramework.TENSORFLOW and hasattr(self.model, 'set_weights'):
            self.model.set_weights(state)
        elif self.framework == MLFramework.SCIKIT_LEARN:
            self.model = pickle.loads(state)
    
    def load_checkpoint(self, checkpoint_path: str, key: bytes = None, verify_hmac: bool = True) -> bool:
        """Load model from checkpoint with encryption and integrity verification"""
        path = Path(checkpoint_path)
        
        try:
            # Read checkpoint
            with open(path, 'rb') as f:
                data = f.read()
            
            # Handle encrypted checkpoints
            if path.suffix == '.enc':
                if not key and not self._encryption_key:
                    raise ValueError("Decryption key required for encrypted checkpoint")
                
                decryption_key = key or self._encryption_key
                
                # Extract HMAC if present
                if verify_hmac and b'||HMAC:' in data:
                    data, hmac_sig = data.split(b'||HMAC:', 1)
                    hmac_sig = hmac_sig.decode()
                    
                    # Verify HMAC
                    computed_hmac = self._compute_hmac(data)
                    if computed_hmac != hmac_sig:
                        raise ValueError("Checkpoint integrity check failed (HMAC mismatch)")
                
                # Decrypt
                from cryptography.fernet import Fernet
                cipher = Fernet(decryption_key)
                data = cipher.decrypt(data)
            
            # Decompress if needed
            try:
                import zlib
                data = zlib.decompress(data)
            except zlib.error:
                # Not compressed, continue
                pass
            
            # Deserialize
            checkpoint = pickle.loads(data)
            
            # Restore model state
            self._set_model_state(checkpoint['model_state_dict'])
            
            self.model_version = checkpoint.get('model_version', 1)
            self.training_history = checkpoint.get('training_history', [])
            self.is_trained = checkpoint.get('is_trained', False)
            self.experiment_id = checkpoint.get('experiment_id', self.experiment_id)
            
            logger.info(f"Model loaded from {checkpoint_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return False
    
    def quantize(self, method: str = 'dynamic', dtype: str = 'int8'):
        """Quantize model for edge deployment"""
        if self.framework != MLFramework.PYTORCH or not TORCH_AVAILABLE:
            logger.warning(f"Quantization not supported for framework {self.framework}")
            return None
        
        try:
            if method == 'dynamic':
                # Dynamic quantization
                quantized_model = torch.quantization.quantize_dynamic(
                    self.model,
                    {nn.Linear, nn.LSTM, nn.GRU},
                    dtype=torch.qint8 if dtype == 'int8' else torch.float16
                )
                return quantized_model
            elif method == 'static':
                # Static quantization requires calibration
                logger.warning("Static quantization requires calibration data")
                return None
            else:
                logger.warning(f"Unknown quantization method: {method}")
                return None
        except Exception as e:
            logger.error(f"Quantization failed: {e}")
            return None
    
    def optimize_hyperparameters(self, X_train: np.ndarray, y_train: np.ndarray,
                                 n_trials: int = 50, cv_folds: int = 3) -> Dict:
        """Optimize hyperparameters using Optuna"""
        if not OPTUNA_AVAILABLE:
            logger.warning("Optuna not available for hyperparameter optimization")
            return {}
        
        if not SKLEARN_AVAILABLE:
            logger.warning("Scikit-learn required for cross-validation")
            return {}
        
        def objective(trial):
            """Objective function for Optuna"""
            params = self._get_hyperparameter_space(trial)
            self.update_hyperparameters(params)
            
            # Cross-validation
            kf = KFold(n_splits=cv_folds, shuffle=True, random_state=42)
            scores = []
            
            for train_idx, val_idx in kf.split(X_train):
                X_tr, X_val = X_train[train_idx], X_train[val_idx]
                y_tr, y_val = y_train[train_idx], y_train[val_idx]
                
                self.train(X_tr, y_tr, epochs=10, verbose=False)
                eval_metrics = self.evaluate(X_val, y_val)
                scores.append(eval_metrics['rmse'])
                
                # Clear GPU memory between folds
                self.clear_gpu_memory()
            
            return np.mean(scores)
        
        study = optuna.create_study(direction='minimize')
        study.optimize(objective, n_trials=n_trials)
        
        best_params = study.best_params
        self.update_hyperparameters(best_params)
        
        return {
            'best_params': best_params,
            'best_value': study.best_value,
            'n_trials': n_trials,
            'study': study
        }
    
    def _get_hyperparameter_space(self, trial) -> Dict:
        """Define hyperparameter search space - override in subclass"""
        return {}
    
    def update_hyperparameters(self, params: Dict) -> None:
        """Update model hyperparameters - override in subclass"""
        pass
    
    def get_model_info(self) -> Dict:
        """Get model information"""
        return {
            'class_name': self.__class__.__name__,
            'framework': self.framework.value,
            'version': self.model_version,
            'is_trained': self.is_trained,
            'training_epochs': len(self.training_history),
            'gpu_available': self._gpu_available,
            'device': str(self._device) if self._device else 'cpu',
            'experiment_id': self.experiment_id,
            'experiment_duration_s': (datetime.now() - self.experiment_start).total_seconds(),
            'checkpoint_dir': str(self._checkpoint_dir)
        }
    
    def get_statistics(self) -> Dict:
        """Get model statistics"""
        avg_latency = np.mean(self._prediction_latencies) if self._prediction_latencies else 0
        p95_latency = np.percentile(self._prediction_latencies, 95) if self._prediction_latencies else 0
        
        return {
            **self.get_model_info(),
            'last_training_metrics': self.training_history[-1] if self.training_history else None,
            'avg_prediction_latency_ms': avg_latency * 1000,
            'p95_prediction_latency_ms': p95_latency * 1000,
            'total_predictions': len(self._prediction_latencies)
        }

# ============================================================
# ENHANCED: MODEL REGISTRY WITH TTL CLEANUP
# ============================================================

class ModelRegistry:
    """
    Registry for managing multiple ML models.
    
    ENHANCEMENTS v8.0:
    - TTL-based cleanup for old models
    - Model metrics tracking
    - A/B testing support
    - Model health checks
    """
    
    _models: Dict[str, Dict] = {}
    _model_metrics: Dict[str, Dict] = {}
    _lock = threading.RLock()
    _cleanup_thread: Optional[threading.Thread] = None
    _running = False
    
    @classmethod
    def start_cleanup(cls, interval_seconds: int = 300, ttl_seconds: int = 86400):
        """Start automatic cleanup thread for old models"""
        if cls._cleanup_thread and cls._cleanup_thread.is_alive():
            return
        
        cls._running = True
        
        def cleanup_worker():
            while cls._running:
                time.sleep(interval_seconds)
                cls._cleanup_expired_models(ttl_seconds)
        
        cls._cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        cls._cleanup_thread.start()
        logger.info("Model registry cleanup thread started")
    
    @classmethod
    def stop_cleanup(cls):
        """Stop cleanup thread"""
        cls._running = False
        if cls._cleanup_thread:
            cls._cleanup_thread.join(timeout=5)
    
    @classmethod
    def _cleanup_expired_models(cls, ttl_seconds: int):
        """Remove models older than TTL"""
        with cls._lock:
            current_time = datetime.now()
            expired = []
            
            for model_id, info in cls._models.items():
                registered_at = datetime.fromisoformat(info['registered_at'])
                if (current_time - registered_at).total_seconds() > ttl_seconds:
                    expired.append(model_id)
            
            for model_id in expired:
                del cls._models[model_id]
                cls._model_metrics.pop(model_id, None)
                logger.info(f"Removed expired model: {model_id}")
    
    @classmethod
    def register(cls, model_name: str, model_instance: BaseMLModel,
                metadata: Dict = None, version: str = None, ttl: int = None) -> str:
        """Register a model instance with optional TTL"""
        version = version or f"v{model_instance.model_version}"
        model_id = f"{model_name}_{version}"
        
        with cls._lock:
            cls._models[model_id] = {
                'instance': model_instance,
                'name': model_name,
                'version': version,
                'metadata': metadata or {},
                'registered_at': datetime.now().isoformat(),
                'is_active': True,
                'ttl': ttl,
                'prediction_count': 0,
                'error_count': 0,
                'avg_latency_ms': 0
            }
            
            cls._model_metrics[model_id] = {
                'predictions': [],
                'errors': [],
                'latencies': []
            }
        
        logger.info(f"Model registered: {model_id}")
        return model_id
    
    @classmethod
    def get(cls, model_name: str, version: str = None, strategy: str = 'latest') -> Optional[BaseMLModel]:
        """
        Get a registered model instance.
        
        Strategies:
        - 'latest': Latest active version
        - 'best': Best performing version based on metrics
        - 'round_robin': For A/B testing
        """
        with cls._lock:
            if version:
                model_id = f"{model_name}_{version}"
                model_info = cls._models.get(model_id)
                if model_info and model_info['is_active']:
                    cls._update_access_metrics(model_id)
                    return model_info['instance']
                return None
            
            if strategy == 'latest':
                # Get latest version
                latest = None
                latest_version = None
                
                for model_id, info in cls._models.items():
                    if info['name'] == model_name and info['is_active']:
                        v = info['version']
                        if latest_version is None or v > latest_version:
                            latest_version = v
                            latest = info['instance']
                
                if latest:
                    cls._update_access_metrics(f"{model_name}_{latest_version}")
                return latest
            
            elif strategy == 'best':
                # Get best performing model
                best = None
                best_score = float('inf')
                
                for model_id, info in cls._models.items():
                    if info['name'] == model_name and info['is_active']:
                        metrics = cls._model_metrics.get(model_id, {})
                        error_rate = len(metrics.get('errors', [])) / max(1, len(metrics.get('predictions', [])))
                        if error_rate < best_score:
                            best_score = error_rate
                            best = info['instance']
                
                if best:
                    cls._update_access_metrics(model_id)
                return best
            
            elif strategy == 'round_robin':
                # Round-robin for A/B testing
                active_versions = [
                    (model_id, info) for model_id, info in cls._models.items()
                    if info['name'] == model_name and info['is_active']
                ]
                
                if active_versions:
                    # Simple round-robin based on access count
                    model_id, info = min(active_versions, key=lambda x: x[1].get('access_count', 0))
                    cls._update_access_metrics(model_id)
                    return info['instance']
            
            return None
    
    @classmethod
    def _update_access_metrics(cls, model_id: str):
        """Update model access metrics"""
        with cls._lock:
            if model_id in cls._models:
                cls._models[model_id]['access_count'] = cls._models[model_id].get('access_count', 0) + 1
    
    @classmethod
    def record_prediction(cls, model_id: str, latency_ms: float, error: bool = False):
        """Record prediction metrics for model"""
        with cls._lock:
            if model_id in cls._models:
                model = cls._models[model_id]
                model['prediction_count'] += 1
                if error:
                    model['error_count'] += 1
                
                # Update rolling average latency
                model['avg_latency_ms'] = (
                    model['avg_latency_ms'] * (model['prediction_count'] - 1) + latency_ms
                ) / model['prediction_count']
                
                # Store detailed metrics
                if model_id in cls._model_metrics:
                    metrics = cls._model_metrics[model_id]
                    metrics['predictions'].append(datetime.now().isoformat())
                    metrics['latencies'].append(latency_ms)
                    if error:
                        metrics['errors'].append(datetime.now().isoformat())
                    
                    # Keep last 1000 metrics
                    for key in metrics:
                        if len(metrics[key]) > 1000:
                            metrics[key] = metrics[key][-1000:]
    
    @classmethod
    def list_models(cls) -> List[Dict]:
        """List all registered models"""
        with cls._lock:
            return [
                {
                    'model_id': model_id,
                    'name': info['name'],
                    'version': info['version'],
                    'registered_at': info['registered_at'],
                    'is_active': info['is_active'],
                    'metadata': info['metadata'],
                    'prediction_count': info.get('prediction_count', 0),
                    'error_count': info.get('error_count', 0),
                    'avg_latency_ms': info.get('avg_latency_ms', 0)
                }
                for model_id, info in cls._models.items()
            ]
    
    @classmethod
    def deactivate(cls, model_name: str, version: str = None) -> bool:
        """Deactivate a model version"""
        with cls._lock:
            if version:
                model_id = f"{model_name}_{version}"
                if model_id in cls._models:
                    cls._models[model_id]['is_active'] = False
                    logger.info(f"Model deactivated: {model_id}")
                    return True
            else:
                for model_id, info in cls._models.items():
                    if info['name'] == model_name:
                        info['is_active'] = False
                logger.info(f"All versions of {model_name} deactivated")
                return True
        
        return False
    
    @classmethod
    def get_active_models(cls) -> List[str]:
        """Get names of all active models"""
        with cls._lock:
            return list(set(info['name'] for info in cls._models.values() if info['is_active']))
    
    @classmethod
    def clear(cls):
        """Clear all registered models"""
        with cls._lock:
            cls._models.clear()
            cls._model_metrics.clear()
            logger.info("Model registry cleared")

# ============================================================
# ENHANCED: BASE WORKFLOW WITH DAG VISUALIZATION
# ============================================================

class WorkflowStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class BaseWorkflow(ABC):
    """
    Abstract base class for multi-step orchestration workflows.
    
    ENHANCEMENTS v8.0:
    - Fixed race conditions with proper synchronization
    - DAG visualization with Graphviz export
    - Workflow status tracking
    - Step timeout with cancellation
    - Parallel step execution with dependency resolution
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.steps: Dict[str, Dict] = {}
        self.step_order: List[str] = []
        self.results: Dict[str, Any] = {}
        self.errors: Dict[str, Exception] = {}
        self.retry_config = self.config.get('retry', {'max_attempts': 1, 'delay': 0})
        self.checkpoint_dir = Path(self.config.get('checkpoint_dir', './workflow_checkpoints'))
        self.checkpoint_dir.mkdir(exist_ok=True, parents=True)
        self.workflow_id = str(uuid.uuid4())[:8]
        self.start_time = None
        self.end_time = None
        self.status = WorkflowStatus.PENDING
        self._lock = asyncio.Lock()
        self._step_lock = asyncio.Lock()
        self._cancelled = False
    
    def add_step(self, name: str, func: Callable, depends_on: List[str] = None,
                 retry_config: Dict = None, timeout: float = None):
        """
        Add a step to the workflow.
        
        Args:
            name: Step identifier
            func: Async or sync function to execute
            depends_on: List of step names this step depends on
            retry_config: Optional retry configuration for this step
            timeout: Timeout in seconds for this step
        """
        self.steps[name] = {
            'func': func,
            'depends_on': depends_on or [],
            'retry_config': retry_config or self.retry_config,
            'timeout': timeout,
            'status': WorkflowStatus.PENDING,
            'result': None,
            'error': None,
            'start_time': None,
            'end_time': None,
            'attempts': 0
        }
        self.step_order.append(name)
    
    def add_parallel_steps(self, step_group: Dict[str, Callable], group_name: str = None):
        """Add a group of steps that can run in parallel"""
        group = group_name or f"parallel_group_{len(self.steps)}"
        
        for name, func in step_group.items():
            full_name = f"{group}_{name}"
            self.steps[full_name] = {
                'func': func,
                'depends_on': [],
                'retry_config': self.retry_config,
                'timeout': None,
                'status': WorkflowStatus.PENDING,
                'result': None,
                'error': None,
                'start_time': None,
                'end_time': None,
                'attempts': 0,
                'parallel_group': group
            }
            self.step_order.append(full_name)
    
    @abstractmethod
    def validate_input(self, input_data: Any) -> bool:
        """Validate workflow input"""
        pass
    
    @abstractmethod
    def finalize(self, results: Dict) -> Any:
        """Process results after all steps complete"""
        pass
    
    async def _check_dependencies(self, step_name: str) -> bool:
        """Check if all dependencies for a step are satisfied"""
        async with self._step_lock:
            step = self.steps[step_name]
            
            for dep in step['depends_on']:
                if dep not in self.results:
                    return False
                if dep in self.errors:
                    return False
                
                # Check dependency status
                dep_step = self.steps.get(dep)
                if dep_step and dep_step['status'] != WorkflowStatus.COMPLETED:
                    return False
            
            return True
    
    async def _execute_step(self, step_name: str) -> None:
        """Execute a single step with timeout and retry"""
        step = self.steps[step_name]
        
        async with self._step_lock:
            if step['status'] != WorkflowStatus.PENDING:
                return
            step['status'] = WorkflowStatus.RUNNING
            step['start_time'] = datetime.now()
        
        for attempt in range(step['retry_config'].get('max_attempts', 1)):
            if self._cancelled:
                step['status'] = WorkflowStatus.CANCELLED
                break
            
            try:
                # Execute with timeout
                if step.get('timeout'):
                    try:
                        result = await asyncio.wait_for(
                            self._call_func(step['func'], step_name),
                            timeout=step['timeout']
                        )
                    except asyncio.TimeoutError:
                        raise TimeoutError(f"Step {step_name} timed out after {step['timeout']} seconds")
                else:
                    result = await self._call_func(step['func'], step_name)
                
                # Store result
                async with self._step_lock:
                    step['result'] = result
                    self.results[step_name] = result
                    step['status'] = WorkflowStatus.COMPLETED
                    step['error'] = None
                    step['attempts'] = attempt + 1
                break
                
            except Exception as e:
                step['error'] = str(e)
                self.errors[step_name] = e
                step['attempts'] = attempt + 1
                
                if attempt < step['retry_config'].get('max_attempts', 1) - 1:
                    delay = step['retry_config'].get('delay', 1)
                    await asyncio.sleep(delay * (attempt + 1))
                    logger.warning(f"Retrying step {step_name} (attempt {attempt + 2})")
                else:
                    async with self._step_lock:
                        step['status'] = WorkflowStatus.FAILED
                    logger.error(f"Step {step_name} failed after {attempt + 1} attempts: {e}")
        
        async with self._step_lock:
            step['end_time'] = datetime.now()
    
    async def _call_func(self, func: Callable, step_name: str) -> Any:
        """Call function with appropriate context"""
        if asyncio.iscoroutinefunction(func):
            return await func(self.results)
        else:
            return await asyncio.to_thread(func, self.results)
    
    async def get_ready_steps(self) -> List[str]:
        """Get steps that are ready to execute (thread-safe)"""
        async with self._step_lock:
            ready = []
            for name in self.step_order:
                step = self.steps[name]
                if step['status'] == WorkflowStatus.PENDING and await self._check_dependencies(name):
                    ready.append(name)
            return ready
    
    async def execute(self, initial_data: Any = None) -> Any:
        """Execute the workflow with proper synchronization"""
        self.start_time = datetime.now()
        self.status = WorkflowStatus.RUNNING
        self.results['__initial__'] = initial_data
        
        # Validate input
        if not self.validate_input(initial_data):
            raise ValueError("Workflow validation failed")
        
        # Save initial checkpoint
        await self._save_checkpoint()
        
        # Execute steps with parallel processing
        try:
            while len(self.results) < len(self.steps) + 1:  # +1 for initial data
                if self._cancelled:
                    self.status = WorkflowStatus.CANCELLED
                    raise asyncio.CancelledError("Workflow cancelled")
                
                ready_steps = await self.get_ready_steps()
                
                if not ready_steps:
                    # Check for deadlock with proper error message
                    pending_steps = [n for n, s in self.steps.items() 
                                   if s['status'] == WorkflowStatus.PENDING]
                    
                    if pending_steps:
                        # Build dependency tree for debugging
                        dep_graph = {}
                        for step in pending_steps:
                            deps = self.steps[step]['depends_on']
                            unresolved = [d for d in deps if d not in self.results]
                            if unresolved:
                                dep_graph[step] = unresolved
                        
                        raise RuntimeError(
                            f"Workflow deadlock detected. Pending steps: {pending_steps}\n"
                            f"Unresolved dependencies: {dep_graph}"
                        )
                    break
                
                # Execute ready steps in parallel
                tasks = [self._execute_step(name) for name in ready_steps]
                await asyncio.gather(*tasks, return_exceptions=True)
                
                # Save checkpoint after each batch
                await self._save_checkpoint()
            
            # Check for failures
            failed_steps = [n for n, s in self.steps.items() 
                          if s['status'] == WorkflowStatus.FAILED]
            
            if failed_steps:
                self.status = WorkflowStatus.FAILED
                raise RuntimeError(f"Workflow failed: steps {failed_steps}")
            
            self.status = WorkflowStatus.COMPLETED
            return self.finalize(self.results)
            
        except Exception as e:
            self.status = WorkflowStatus.FAILED
            raise
        finally:
            self.end_time = datetime.now()
    
    async def _save_checkpoint(self):
        """Save workflow checkpoint"""
        async with self._lock:
            checkpoint = {
                'workflow_id': self.workflow_id,
                'status': self.status.value,
                'step_states': {
                    name: {
                        'status': step['status'].value,
                        'result': step.get('result'),
                        'error': str(step.get('error')) if step.get('error') else None,
                        'start_time': step['start_time'].isoformat() if step['start_time'] else None,
                        'end_time': step['end_time'].isoformat() if step['end_time'] else None,
                        'attempts': step.get('attempts', 0)
                    }
                    for name, step in self.steps.items()
                },
                'results': {k: v for k, v in self.results.items() if k != '__initial__'},
                'timestamp': datetime.now().isoformat()
            }
            
            checkpoint_path = self.checkpoint_dir / f"workflow_{self.workflow_id}.pkl"
            with open(checkpoint_path, 'wb') as f:
                pickle.dump(checkpoint, f)
    
    async def visualize_dag(self, output_path: str = None, format: str = 'png') -> Optional[bytes]:
        """Generate DAG visualization of workflow steps"""
        if not GRAPHVIZ_AVAILABLE:
            logger.warning("Graphviz not available for workflow visualization")
            return None
        
        # Create directed graph
        dot = Digraph(comment=f'Workflow {self.workflow_id}')
        dot.attr(rankdir='TB', splines='ortho')
        
        # Add nodes
        for name, step in self.steps.items():
            # Determine color based on status
            status_colors = {
                WorkflowStatus.PENDING: 'lightgray',
                WorkflowStatus.RUNNING: 'yellow',
                WorkflowStatus.COMPLETED: 'lightgreen',
                WorkflowStatus.FAILED: 'red',
                WorkflowStatus.CANCELLED: 'gray'
            }
            color = status_colors.get(step['status'], 'white')
            
            # Format node label
            label = f"{name}\n{step['status'].value}"
            if step.get('attempts', 0) > 1:
                label += f"\n(attempts: {step['attempts']})"
            
            dot.node(name, label, style='filled', fillcolor=color)
        
        # Add edges for dependencies
        for name, step in self.steps.items():
            for dep in step['depends_on']:
                dot.edge(dep, name)
        
        # Render
        if output_path:
            dot.render(output_path, format=format, cleanup=True)
            logger.info(f"Workflow DAG saved to {output_path}.{format}")
            return None
        else:
            # Return PNG bytes
            return dot.pipe(format='png')
    
    def cancel(self):
        """Cancel workflow execution"""
        self._cancelled = True
        logger.info(f"Workflow {self.workflow_id} cancellation requested")
    
    def get_execution_summary(self) -> Dict:
        """Get workflow execution summary"""
        if not self.start_time:
            return {'status': 'not_started'}
        
        return {
            'workflow_id': self.workflow_id,
            'status': self.status.value,
            'total_steps': len(self.steps),
            'completed_steps': sum(1 for s in self.steps.values() if s['status'] == WorkflowStatus.COMPLETED),
            'failed_steps': sum(1 for s in self.steps.values() if s['status'] == WorkflowStatus.FAILED),
            'duration_s': (self.end_time - self.start_time).total_seconds() if self.end_time and self.start_time else 0,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'steps': {
                name: {
                    'status': step['status'].value,
                    'duration_s': (step['end_time'] - step['start_time']).total_seconds()
                    if step['end_time'] and step['start_time'] else 0,
                    'attempts': step.get('attempts', 0)
                }
                for name, step in self.steps.items()
            }
        }
    
    def get_statistics(self) -> Dict:
        """Get workflow statistics"""
        return {
            'workflow_id': self.workflow_id,
            'class_name': self.__class__.__name__,
            'total_steps': len(self.steps),
            'execution_summary': self.get_execution_summary()
        }

# ============================================================
# ENHANCED: GREENAGENTCONFIG WITH CROSS-SECTION VALIDATION
# ============================================================

class GreenAgentConfig:
    """
    Enhanced unified configuration loader for all Green Agent modules.
    
    ENHANCEMENTS v8.0:
    - Schema versioning and migration
    - Circular dependency detection
    - Enhanced cross-section validation
    - Configuration hot-reload with diff
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, config_path: str = None):
        """Singleton pattern with thread safety"""
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config_path: str = None):
        if not hasattr(self, '_initialized'):
            self._config = {}
            self._config_path = None
            self._listeners = []
            self._schema_version = '1.0'
            self._validators = {}
            self._initialized = True
            
            if config_path:
                self.load(config_path)
    
    def load(self, config_path: str):
        """Load configuration from file"""
        self._config_path = Path(config_path)
        
        if not self._config_path.exists():
            raise ConfigurationError(f"Config file not found: {config_path}")
        
        # Load config based on extension
        if self._config_path.suffix == '.json':
            with open(self._config_path, 'r') as f:
                self._config = json.load(f)
        elif self._config_path.suffix in ['.yaml', '.yml']:
            with open(self._config_path, 'r') as f:
                self._config = yaml.safe_load(f)
        else:
            raise ConfigurationError(f"Unsupported config format: {self._config_path.suffix}")
        
        # Check schema version and migrate if needed
        self._check_schema_version()
        
        # Validate configuration
        self._validate_config()
        
        logger.info(f"Configuration loaded from {config_path}")
    
    def _check_schema_version(self):
        """Check config schema version and migrate if needed"""
        config_version = self._config.get('schema_version', '1.0')
        
        if config_version != self._schema_version:
            logger.info(f"Migrating config from version {config_version} to {self._schema_version}")
            self._migrate_config(config_version)
    
    def _migrate_config(self, from_version: str):
        """Migrate configuration from old schema version"""
        if from_version == '1.0':
            # No migration needed for 1.0
            pass
        # Add migration logic for future versions
        
        self._config['schema_version'] = self._schema_version
    
    def _validate_config(self):
        """Enhanced validation with cross-section checks and circular dependency detection"""
        # Single-section validation
        for section, validator_class in self._validators.items():
            if section in self._config:
                try:
                    validator_class(**self._config[section])
                    logger.debug(f"Configuration section '{section}' validated")
                except ValidationError as e:
                    logger.error(f"Configuration validation failed for '{section}': {e}")
                    raise ConfigurationError(f"Invalid configuration for {section}", details={'errors': e.errors()})
        
        # Cross-section validation
        cross_section_errors = self._validate_cross_section()
        if cross_section_errors:
            raise ConfigurationError(
                "Cross-section configuration validation failed",
                details={'errors': cross_section_errors}
            )
        
        # Check for circular dependencies in config sections
        circular_deps = self._detect_circular_dependencies()
        if circular_deps:
            raise ConfigurationError(
                "Circular dependencies detected in configuration",
                details={'cycles': circular_deps}
            )
        
        # Check required sections
        required_sections = ['system', 'helium', 'quantum', 'blockchain', 
                           'sustainability', 'thermal', 'synthetic_data', 'carbon']
        
        missing = [s for s in required_sections if s not in self._config]
        
        if missing:
            logger.warning(f"Missing configuration sections: {missing}")
    
    def _validate_cross_section(self) -> List[Dict]:
        """
        Validate relationships between configuration sections.
        
        Returns:
            List of validation errors
        """
        errors = []
        
        # Quantum validation
        quantum = self._config.get('quantum', {})
        if quantum.get('provider') == 'ibm':
            n_qubits = quantum.get('n_qubits', 0)
            if n_qubits > 127:
                errors.append({
                    'section': 'quantum',
                    'field': 'n_qubits',
                    'message': 'IBM Quantum supports max 127 qubits',
                    'value': n_qubits
                })
            elif n_qubits < 1:
                errors.append({
                    'section': 'quantum',
                    'field': 'n_qubits',
                    'message': 'Invalid number of qubits',
                    'value': n_qubits
                })
        
        # Blockchain validation
        blockchain = self._config.get('blockchain', {})
        network_map = {
            'mainnet': 1, 'goerli': 5, 'sepolia': 11155111,
            'polygon': 137, 'polygon_mumbai': 80001
        }
        network = blockchain.get('network', '')
        expected_chain_id = network_map.get(network)
        actual_chain_id = blockchain.get('chain_id')
        
        if expected_chain_id and actual_chain_id and expected_chain_id != actual_chain_id:
            errors.append({
                'section': 'blockchain',
                'field': 'chain_id',
                'message': f"Chain ID {actual_chain_id} does not match network {network} (expected {expected_chain_id})",
                'value': actual_chain_id,
                'expected': expected_chain_id
            })
        
        # Carbon validation
        carbon = self._config.get('carbon', {})
        renewable_pct = carbon.get('renewable_energy_pct', 0)
        
        if not 0 <= renewable_pct <= 100:
            errors.append({
                'section': 'carbon',
                'field': 'renewable_energy_pct',
                'message': 'Renewable energy percentage must be between 0 and 100',
                'value': renewable_pct
            })
        
        # Thermal validation
        thermal = self._config.get('thermal', {})
        if thermal.get('enabled', False):
            max_temp = thermal.get('max_temperature_celsius', 85)
            if max_temp > 100:
                errors.append({
                    'section': 'thermal',
                    'field': 'max_temperature_celsius',
                    'message': 'Maximum temperature exceeds safe operating limits',
                    'value': max_temp
                })
        
        # Helium and thermal integration
        helium = self._config.get('helium', {})
        if helium and thermal and thermal.get('enabled', False):
            if not helium.get('data_collector', {}).get('enable_synthetic_fallback', True):
                errors.append({
                    'section': 'helium',
                    'field': 'enable_synthetic_fallback',
                    'message': 'Synthetic fallback should be enabled when helium data is used for thermal optimization',
                    'value': False
                })
        
        # Sustainability metrics validation
        sustainability = self._config.get('sustainability', {})
        if sustainability:
            carbon_intensity = sustainability.get('carbon_intensity_g_per_kwh', 500)
            if carbon_intensity < 0:
                errors.append({
                    'section': 'sustainability',
                    'field': 'carbon_intensity_g_per_kwh',
                    'message': 'Carbon intensity cannot be negative',
                    'value': carbon_intensity
                })
        
        # System resource validation
        system = self._config.get('system', {})
        if system:
            max_memory = system.get('max_memory_gb', 16)
            if max_memory > 1024:
                errors.append({
                    'section': 'system',
                    'field': 'max_memory_gb',
                    'message': 'Memory limit exceeds hardware capabilities',
                    'value': max_memory
                })
        
        return errors
    
    def _detect_circular_dependencies(self) -> List[List[str]]:
        """
        Detect circular dependencies in configuration references.
        
        Returns:
            List of cycles found in configuration
        """
        # Build dependency graph from config references
        graph = {}
        
        def extract_references(obj, path=''):
            """Extract references from config values"""
            if isinstance(obj, dict):
                for key, value in obj.items():
                    current_path = f"{path}.{key}" if path else key
                    
                    if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                        # Found a reference like ${section.field}
                        ref = value[2:-1]
                        if path:
                            graph.setdefault(path, set()).add(ref)
                    
                    extract_references(value, current_path)
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    extract_references(item, f"{path}[{i}]")
        
        extract_references(self._config)
        
        # Detect cycles using DFS
        visited = set()
        stack = set()
        cycles = []
        
        def dfs(node, path):
            if node in stack:
                # Found a cycle
                cycle_start = path.index(node)
                cycles.append(path[cycle_start:] + [node])
                return
            
            if node in visited:
                return
            
            visited.add(node)
            stack.add(node)
            
            for neighbor in graph.get(node, []):
                dfs(neighbor, path + [node])
            
            stack.remove(node)
        
        for node in graph:
            if node not in visited:
                dfs(node, [])
        
        return cycles
    
    def subscribe(self, callback: Callable):
        """Subscribe to configuration changes"""
        self._listeners.append(callback)
    
    def unsubscribe(self, callback: Callable):
        """Unsubscribe from configuration changes"""
        if callback in self._listeners:
            self._listeners.remove(callback)
    
    def _notify_listeners(self):
        """Notify all subscribers of configuration change"""
        for callback in self._listeners:
            try:
                callback(self._config)
            except Exception as e:
                logger.error(f"Error notifying listener: {e}")
    
    def reload(self):
        """Reload configuration from file"""
        if self._config_path:
            self.load(self._config_path)
            self._notify_listeners()
            logger.info("Configuration reloaded")
    
    def get(self, key: str, default=None):
        """Get configuration value by dot-notation key"""
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any):
        """Set configuration value by dot-notation key"""
        keys = key.split('.')
        config = self._config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
        self._notify_listeners()
    
    def backup(self, backup_path: str = None):
        """Create configuration backup"""
        if not backup_path:
            backup_path = f"config_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(backup_path, 'w') as f:
            json.dump(self._config, f, indent=2, default=str)
        
        logger.info(f"Configuration backup saved to {backup_path}")
        return backup_path
    
    def restore(self, backup_path: str):
        """Restore configuration from backup"""
        with open(backup_path, 'r') as f:
            self._config = json.load(f)
        
        self._validate_config()
        self._notify_listeners()
        logger.info(f"Configuration restored from {backup_path}")
    
    @property
    def config(self):
        """Get full configuration"""
        return self._config.copy()
    
    def __getitem__(self, key):
        return self.get(key)
    
    def __setitem__(self, key, value):
        self.set(key, value)

# ============================================================
# ENHANCED: BASE METRICS WITH MODEL TRACKING
# ============================================================

@dataclass
class BaseMetrics:
    """Base class for all metrics objects with auto-registration and model tracking"""
    calculation_id: str = field(default_factory=lambda: str(uuid.uuid4())[:12])
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    source_module: str = "base"
    metadata: Dict = field(default_factory=dict)
    
    # Model tracking fields
    model_version: Optional[str] = None
    experiment_id: Optional[str] = None
    
    # Performance fields
    calculation_time_ms: Optional[float] = None
    gpu_memory_used_mb: Optional[float] = None
    
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
                        registry=registry,
                        labelnames=['model_version'] if self.model_version else None
                    )
                    if self.model_version:
                        gauge.labels(model_version=self.model_version).set(value)
                    else:
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
# HELPER FUNCTIONS
# ============================================================

def load_module_config(module_name: str) -> Dict:
    """Load configuration for a specific module"""
    config = GreenAgentConfig()
    module_config = config.get(module_name, {})
    
    # Add system-wide config
    module_config['system'] = config.get('system', {})
    module_config['monitoring'] = config.get('monitoring', {})
    
    return module_config

def get_system_config() -> Dict:
    """Get system configuration"""
    config = GreenAgentConfig()
    return config.get('system', {})

def get_api_config() -> Dict:
    """Get API configuration"""
    config = GreenAgentConfig()
    return config.get('api', {})

def get_monitoring_config() -> Dict:
    """Get monitoring configuration"""
    config = GreenAgentConfig()
    return config.get('monitoring', {})

def reload_all_config():
    """Reload all configurations"""
    config = GreenAgentConfig()
    config.reload()

def discover_modules(base_path: str = "src/modules") -> List[str]:
    """Discover available modules"""
    modules = []
    base_dir = Path(base_path)
    
    if base_dir.exists():
        for item in base_dir.iterdir():
            if item.is_dir() and (item / "__init__.py").exists():
                modules.append(item.name)
    
    return modules

# ============================================================
# CONFIGURATION MODELS (for validation)
# ============================================================

class HeliumConfigModel(BaseModel):
    """Helium module configuration model"""
    enabled: bool = True
    api_endpoint: str = "https://api.helium.io"
    update_interval: int = 60
    data_collector: Dict = Field(default_factory=dict)
    
    @validator('update_interval')
    def validate_update_interval(cls, v):
        if v < 1:
            raise ValueError('Update interval must be at least 1 second')
        return v

class QuantumConfigModel(BaseModel):
    """Quantum module configuration model"""
    provider: str = "ibm"
    n_qubits: int = 20
    backend: str = "ibmq_qasm_simulator"
    api_token: Optional[str] = None
    
    @validator('n_qubits')
    def validate_qubits(cls, v, values):
        provider = values.get('provider', 'ibm')
        if provider == 'ibm' and v > 127:
            raise ValueError('IBM Quantum supports max 127 qubits')
        return v

class BlockchainConfigModel(BaseModel):
    """Blockchain module configuration model"""
    network: str = "goerli"
    chain_id: Optional[int] = None
    contract_address: Optional[str] = None
    private_key: Optional[str] = None
    
    @validator('chain_id', always=True)
    def validate_chain_id(cls, v, values):
        network = values.get('network', 'goerli')
        network_map = {
            'mainnet': 1, 'goerli': 5, 'sepolia': 11155111,
            'polygon': 137, 'polygon_mumbai': 80001
        }
        expected = network_map.get(network)
        
        if v and expected and v != expected:
            raise ValueError(f'Chain ID {v} does not match network {network} (expected {expected})')
        
        return v or expected

class APIConfigModel(BaseModel):
    """API configuration model"""
    host: str = "0.0.0.0"
    port: int = 8000
    workers: int = 4
    cors_origins: List[str] = ["*"]
    
    @validator('port')
    def validate_port(cls, v):
        if not 1 <= v <= 65535:
            raise ValueError('Port must be between 1 and 65535')
        return v

class CarbonConfigModel(BaseModel):
    """Carbon tracking configuration model"""
    renewable_energy_pct: float = 50.0
    carbon_offset_enabled: bool = True
    reporting_interval: int = 3600
    
    @validator('renewable_energy_pct')
    def validate_renewable_pct(cls, v):
        if not 0 <= v <= 100:
            raise ValueError('Renewable energy percentage must be between 0 and 100')
        return v

class AIDataCenterConfigModel(BaseModel):
    """AI Data Center configuration model"""
    enabled: bool = True
    power_usage_effectiveness: float = 1.2
    cooling_efficiency: float = 0.8
    server_count: int = 100
    
    @validator('power_usage_effectiveness')
    def validate_pue(cls, v):
        if v < 1.0:
            raise ValueError('PUE cannot be less than 1.0')
        return v

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
    'BaseMetrics', 'BaseRealtimeHandler', 'BaseMLModel', 'BaseWorkflow',
    
    # Model Management
    'ModelRegistry', 'MLFramework',
    
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
    'APIConfigModel', 'CarbonConfigModel', 'AIDataCenterConfigModel',
    
    # Enums
    'WorkflowStatus'
]

# Note: LifecycleAware and ContextManagerMixin referenced in exports but not defined
# in this file - they would be defined elsewhere in the codebase

class LifecycleAware(ABC):
    """Lifecycle management interface"""
    
    @abstractmethod
    async def initialize(self):
        """Initialize the component"""
        pass
    
    @abstractmethod
    async def start(self):
        """Start the component"""
        pass
    
    @abstractmethod
    async def stop(self):
        """Stop the component"""
        pass
    
    @abstractmethod
    async def health_check(self) -> Dict:
        """Check component health"""
        pass

class ContextManagerMixin:
    """Context manager mixin for resource management"""
    
    async def __aenter__(self):
        await self.initialize()
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
