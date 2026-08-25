"""
Enhanced Photosynthetic Harvester v9.1.0
Complete implementation with architectural improvements:
- Interface-based components (Dependency Inversion)
- Central event bus for decoupled communication
- Global circuit breaker for external services
- JSON-based persistence with schema versioning
- Grouped configuration (sub-configs)
- Trace IDs for structured logging
- WebSocket rate limiting and TLS support
- Multi‑Objective Pareto Decision (MOPD) with NSGA‑II optimizer
"""

import asyncio
import logging
import json
import hashlib
import os
import math
import random
import time
import uuid
from typing import Dict, Any, List, Optional, Tuple, Union, Set, Callable, Awaitable, Protocol
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from collections import deque, defaultdict
import numpy as np
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import weakref
import inspect
import yaml

# Third-party imports
try:
    import tensorflow as tf
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    import jwt
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    import websockets
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False

try:
    from pydantic import BaseModel, Field, validator, root_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# Local imports (with fallback)
try:
    from .eco_atp_currency import EcoATPTokenManager, EcoATPSource
    TOKEN_MANAGER_AVAILABLE = True
except ImportError:
    TOKEN_MANAGER_AVAILABLE = False

try:
    from .proton_gradient_fields import GradientFieldManager
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False

# Use structlog for structured logging if available, else standard logging
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

# ============================================================================
# Custom Exception Hierarchy
# ============================================================================
class HarvesterError(Exception):
    """Base exception for all harvester-related errors."""
    pass

class ConfigError(HarvesterError):
    """Configuration validation error."""
    pass

class PersistenceError(HarvesterError):
    """Persistence backend error."""
    pass

class GeneticOptimizerError(HarvesterError):
    """Genetic optimizer error."""
    pass

class CompetitionError(HarvesterError):
    """Child competition error."""
    pass

class SwarmError(HarvesterError):
    """Swarm coordination error."""
    pass

class WebSocketError(HarvesterError):
    """WebSocket server error."""
    pass

class CircuitBreakerOpenError(HarvesterError):
    """Circuit breaker is open."""
    pass

# ============================================================================
# Event Bus (Decoupled Communication)
# ============================================================================
class EventBus:
    """
    Simple in-memory event bus for internal communication.
    Components can subscribe to events and publish them.
    """
    def __init__(self):
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._lock = asyncio.Lock()

    def subscribe(self, event_type: str, callback: Callable):
        self._subscribers[event_type].append(callback)

    async def publish(self, event_type: str, data: Any):
        async with self._lock:
            callbacks = self._subscribers.get(event_type, [])
        for cb in callbacks:
            asyncio.create_task(cb(data))

# ============================================================================
# Trace Context for Observability
# ============================================================================
class TraceContext:
    """Holds trace ID for request correlation."""
    def __init__(self, trace_id: Optional[str] = None):
        self.trace_id = trace_id or str(uuid.uuid4())

    def get_logger(self, base_logger):
        """Return a logger with trace_id bound."""
        return base_logger.bind(trace_id=self.trace_id)

# ============================================================================
# Circuit Breaker Pattern (Global)
# ============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    """
    Circuit breaker for external service calls to prevent cascading failures.
    """
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: float = 30.0,
                 half_open_attempts: int = 3):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_attempts = half_open_attempts
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None
        self._half_open_attempt_count = 0
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            if self._state == CircuitBreakerState.OPEN:
                if (datetime.now(timezone.utc) - self._last_failure_time).total_seconds() > self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    self._half_open_attempt_count = 0
                    logger.info(f"Circuit breaker {self.name} entering HALF_OPEN")
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} is OPEN")
            elif self._state == CircuitBreakerState.HALF_OPEN:
                if self._half_open_attempt_count >= self.half_open_attempts:
                    self._state = CircuitBreakerState.OPEN
                    self._last_failure_time = datetime.now(timezone.utc)
                    raise CircuitBreakerOpenError(f"Circuit breaker {self.name} half-open attempts exceeded")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self._state == CircuitBreakerState.HALF_OPEN:
                    self._state = CircuitBreakerState.CLOSED
                    self._failure_count = 0
                    logger.info(f"Circuit breaker {self.name} recovered to CLOSED")
                else:
                    self._failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self._failure_count += 1
                self._last_failure_time = datetime.now(timezone.utc)
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker {self.name} opened after {self._failure_count} failures")
                elif self._state == CircuitBreakerState.HALF_OPEN:
                    self._half_open_attempt_count += 1
            raise e

    @property
    def state(self) -> CircuitBreakerState:
        return self._state

# Global circuit breaker registry
class GlobalCircuitBreaker:
    """Singleton registry for circuit breakers."""
    _instance = None
    _breakers: Dict[str, CircuitBreaker] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_or_create(self, name: str, **kwargs) -> CircuitBreaker:
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(name, **kwargs)
        return self._breakers[name]

# ============================================================================
# Centralized Task Manager
# ============================================================================
class TaskManager:
    """Manages background tasks with restart and exponential backoff."""
    def __init__(self, event_bus: Optional[EventBus] = None):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._task_coroutines: Dict[str, Callable[[], Awaitable[None]]] = {}
        self.event_bus = event_bus or EventBus()

    def start_task(self, name: str, coro_func: Callable[[], Awaitable[None]], *args, **kwargs):
        async def wrapper():
            backoff = 1
            max_backoff = 300
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("Task crashed", name=name, error=str(e), exc_info=True)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
        task = asyncio.create_task(wrapper(), name=name)
        async with self._lock:
            self.tasks[name] = task
        return task

    async def stop_all(self):
        self.shutdown_event.set()
        async with self._lock:
            for task in self.tasks.values():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()
        logger.info("All background tasks stopped")

    def register_task(self, name: str, coro_func: Callable[[], Awaitable[None]], *args, **kwargs):
        self._task_coroutines[name] = (coro_func, args, kwargs)

    def start_registered_tasks(self):
        for name, (coro_func, args, kwargs) in self._task_coroutines.items():
            self.start_task(name, coro_func, *args, **kwargs)
        self._task_coroutines.clear()

# ============================================================================
# Configuration (Grouped Pydantic Models)
# ============================================================================
if PYDANTIC_AVAILABLE:
    class PigmentConfig(BaseModel):
        default_repair_rate: float = Field(0.01, ge=0.001, le=0.1)
        damage_threshold: float = Field(0.8, ge=0.5, le=1.0)
        photoinhibition_rate: float = Field(0.001, ge=0.0001, le=0.01)
        safe_excitation_level: float = Field(0.7, ge=0.5, le=0.95)
        lstm_sequence_length: int = Field(20, ge=5)
        lstm_epochs: int = Field(5, ge=1)
        lstm_batch_size: int = Field(16, ge=1)
        lstm_model_dir: str = "./lstm_models"
        fallback_model: str = "moving_average"  # moving_average, arima, linear
        arima_order: Tuple[int, int, int] = (1, 1, 1)

    class ReactionCenterConfig(BaseModel):
        base_quantum_efficiency: float = Field(0.85, ge=0.3, le=0.98)
        min_efficiency: float = Field(0.3, ge=0.1, le=0.5)
        max_efficiency: float = Field(0.98, ge=0.9, le=1.0)
        demand_modulation_enabled: bool = True
        token_abundance_threshold: float = 50000
        token_scarcity_threshold: float = 5000
        demand_response_factor: float = Field(0.5, ge=0.1, le=1.0)
        repair_rate: float = Field(0.005, ge=0.001, le=0.02)

    class HealthConfig(BaseModel):
        efficiency_warning_threshold: float = 0.6
        efficiency_critical_threshold: float = 0.3
        damage_warning_threshold: float = 0.4
        damage_critical_threshold: float = 0.7
        harvest_rate_min: float = 0.1
        prediction_accuracy_min: float = 0.7
        max_healing_attempts: int = Field(3, ge=1)
        healing_cooldown: int = Field(300, ge=10)

    class GeneticConfig(BaseModel):
        population_size: int = Field(20, ge=5)
        mutation_rate: float = Field(0.2, ge=0.0, le=1.0)
        crossover_rate: float = Field(0.7, ge=0.0, le=1.0)
        generations: int = Field(10, ge=1)
        tournament_size: int = Field(3, ge=2)
        evolution_interval: int = Field(86400, ge=3600)
        simulation_cycles: int = Field(50, ge=10)

    class ChildConfig(BaseModel):
        max_children: int = Field(10, ge=1)
        competition_interval: int = Field(3600, ge=60)
        replacement_threshold: float = Field(0.3, ge=0.1, le=0.5)
        performance_window: int = Field(100, ge=10)

    class SwarmConfig(BaseModel):
        update_interval: int = Field(120, ge=10)
        redis_url: Optional[str] = None

    class WebSocketConfig(BaseModel):
        enable: bool = False
        host: str = "0.0.0.0"
        port: int = Field(8765, ge=1024, le=65535)
        auth_token: Optional[str] = None
        use_jwt: bool = False
        jwt_secret: Optional[str] = None
        rate_limit_per_minute: int = Field(60, ge=1)
        tls_enabled: bool = False
        tls_cert: Optional[str] = None
        tls_key: Optional[str] = None

    class PersistenceConfig(BaseModel):
        enable: bool = True
        backend: str = "memory"  # redis, file, memory
        retention_days: int = Field(30, ge=1)
        checkpoint_interval: int = Field(300, ge=10)
        redis_url: Optional[str] = None
        base_dir: str = "./harvester_data"

    class MOPDConfig(BaseModel):
        """Configuration for Multi‑Objective Pareto Decision (MOPD)."""
        enabled: bool = Field(True, description="Enable multi‑objective optimization")
        objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'energy_output': 0.4,
                'pigment_health': 0.3,
                'longterm_efficiency': 0.2,
                'resource_usage': 0.1,
            },
            description="Weights for scalarising Pareto front (must sum to 1)"
        )
        grid_resolution: int = Field(5, description="Number of discrete points for sampling (unused)")

        @validator('objective_weights')
        def check_weights(cls, v):
            if abs(sum(v.values()) - 1.0) > 1e-6:
                raise ValueError("objective_weights must sum to 1")
            return v

    class HarvesterConfig(BaseModel):
        harvester_id: str = "primary"
        latitude: float = Field(0.0, ge=-90, le=90)
        longitude: float = Field(0.0, ge=-180, le=180)
        enable_prometheus: bool = False
        circuit_breaker_failure_threshold: int = Field(5, ge=1)
        circuit_breaker_recovery_timeout: float = Field(30.0, ge=5.0)
        circuit_breaker_half_open_attempts: int = Field(3, ge=1)

        pigment: PigmentConfig = Field(default_factory=PigmentConfig)
        reaction_center: ReactionCenterConfig = Field(default_factory=ReactionCenterConfig)
        health: HealthConfig = Field(default_factory=HealthConfig)
        genetic: GeneticConfig = Field(default_factory=GeneticConfig)
        child: ChildConfig = Field(default_factory=ChildConfig)
        swarm: SwarmConfig = Field(default_factory=SwarmConfig)
        websocket: WebSocketConfig = Field(default_factory=WebSocketConfig)
        persistence: PersistenceConfig = Field(default_factory=PersistenceConfig)
        mopd: MOPDConfig = Field(default_factory=MOPDConfig)

        class Config:
            env_prefix = "HARVESTER_"

        @validator('latitude')
        def validate_latitude(cls, v):
            if not -90 <= v <= 90:
                raise ValueError('latitude must be between -90 and 90')
            return v

        @validator('longitude')
        def validate_longitude(cls, v):
            if not -180 <= v <= 180:
                raise ValueError('longitude must be between -180 and 180')
            return v

        @root_validator
        def validate_websocket_auth(cls, values):
            ws = values.get('websocket')
            if ws and ws.enable:
                if ws.use_jwt and not ws.jwt_secret:
                    raise ValueError('JWT secret required when use_jwt is True')
                if not ws.use_jwt and not ws.auth_token:
                    raise ValueError('Either auth token or JWT must be set when WebSocket is enabled')
            return values

        @classmethod
        def from_yaml(cls, path: str) -> 'HarvesterConfig':
            with open(path, 'r') as f:
                data = yaml.safe_load(f)
            return cls(**data)

        @classmethod
        def from_json(cls, path: str) -> 'HarvesterConfig':
            with open(path, 'r') as f:
                data = json.load(f)
            return cls(**data)
else:
    # Fallback dataclass with flat fields (simplified)
    @dataclass
    class PigmentConfig:
        default_repair_rate: float = 0.01
        damage_threshold: float = 0.8
        photoinhibition_rate: float = 0.001
        safe_excitation_level: float = 0.7
        lstm_sequence_length: int = 20
        lstm_epochs: int = 5
        lstm_batch_size: int = 16
        lstm_model_dir: str = "./lstm_models"
        fallback_model: str = "moving_average"
        arima_order: Tuple[int, int, int] = (1, 1, 1)

    @dataclass
    class ReactionCenterConfig:
        base_quantum_efficiency: float = 0.85
        min_efficiency: float = 0.3
        max_efficiency: float = 0.98
        demand_modulation_enabled: bool = True
        token_abundance_threshold: float = 50000
        token_scarcity_threshold: float = 5000
        demand_response_factor: float = 0.5
        repair_rate: float = 0.005

    @dataclass
    class HealthConfig:
        efficiency_warning_threshold: float = 0.6
        efficiency_critical_threshold: float = 0.3
        damage_warning_threshold: float = 0.4
        damage_critical_threshold: float = 0.7
        harvest_rate_min: float = 0.1
        prediction_accuracy_min: float = 0.7
        max_healing_attempts: int = 3
        healing_cooldown: int = 300

    @dataclass
    class GeneticConfig:
        population_size: int = 20
        mutation_rate: float = 0.2
        crossover_rate: float = 0.7
        generations: int = 10
        tournament_size: int = 3
        evolution_interval: int = 86400
        simulation_cycles: int = 50

    @dataclass
    class ChildConfig:
        max_children: int = 10
        competition_interval: int = 3600
        replacement_threshold: float = 0.3
        performance_window: int = 100

    @dataclass
    class SwarmConfig:
        update_interval: int = 120
        redis_url: Optional[str] = None

    @dataclass
    class WebSocketConfig:
        enable: bool = False
        host: str = "0.0.0.0"
        port: int = 8765
        auth_token: Optional[str] = None
        use_jwt: bool = False
        jwt_secret: Optional[str] = None
        rate_limit_per_minute: int = 60
        tls_enabled: bool = False
        tls_cert: Optional[str] = None
        tls_key: Optional[str] = None

    @dataclass
    class PersistenceConfig:
        enable: bool = True
        backend: str = "memory"
        retention_days: int = 30
        checkpoint_interval: int = 300
        redis_url: Optional[str] = None
        base_dir: str = "./harvester_data"

    @dataclass
    class MOPDConfig:
        enabled: bool = True
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'energy_output': 0.4,
            'pigment_health': 0.3,
            'longterm_efficiency': 0.2,
            'resource_usage': 0.1,
        })
        grid_resolution: int = 5

    @dataclass
    class HarvesterConfig:
        harvester_id: str = "primary"
        latitude: float = 0.0
        longitude: float = 0.0
        enable_prometheus: bool = False
        circuit_breaker_failure_threshold: int = 5
        circuit_breaker_recovery_timeout: float = 30.0
        circuit_breaker_half_open_attempts: int = 3
        pigment: PigmentConfig = field(default_factory=PigmentConfig)
        reaction_center: ReactionCenterConfig = field(default_factory=ReactionCenterConfig)
        health: HealthConfig = field(default_factory=HealthConfig)
        genetic: GeneticConfig = field(default_factory=GeneticConfig)
        child: ChildConfig = field(default_factory=ChildConfig)
        swarm: SwarmConfig = field(default_factory=SwarmConfig)
        websocket: WebSocketConfig = field(default_factory=WebSocketConfig)
        persistence: PersistenceConfig = field(default_factory=PersistenceConfig)
        mopd: MOPDConfig = field(default_factory=MOPDConfig)

# ============================================================================
# Interface Definitions (Dependency Inversion)
# ============================================================================
class IPigmentArray(Protocol):
    async def sense_environment(self, environmental_data: Dict[str, float]) -> Dict[str, float]: ...
    async def get_predictions(self) -> Dict[str, Dict[str, Any]]: ...
    def get_pigment_health_summary(self) -> Dict[str, float]: ...
    def get_circadian_summary(self) -> Dict[str, float]: ...
    async def stop(self): ...

class IReactionCenter(Protocol):
    async def harvest_cycle(self, excitations: Dict[str, float]) -> Dict[str, Any]: ...
    def get_efficiency_stats(self) -> Dict[str, Any]: ...
    async def stop(self): ...

class IHealthMonitor(Protocol):
    def collect_metrics(self, harvester_state: Dict[str, Any]) -> Dict[str, Any]: ...
    def get_metrics(self) -> Dict[str, Any]: ...
    def get_recommendations(self) -> List[Dict[str, Any]]: ...

class ISelfHealer(Protocol):
    async def apply_healing(self, issue_type: str) -> bool: ...

class IPersistence(Protocol):
    async def save_state(self, state: Dict[str, Any]) -> bool: ...
    async def load_state(self) -> Optional[Dict[str, Any]]: ...
    async def save_checkpoint(self, checkpoint: Dict[str, Any]) -> bool: ...
    async def load_latest_checkpoint(self) -> Optional[Tuple[str, Dict[str, Any]]]: ...
    async def delete_old_checkpoints(self, retention_days: int): ...

# ============================================================================
# Pigment Health and Data Structures
# ============================================================================
@dataclass
class PigmentHealth:
    pigment_name: str
    health: float = 1.0
    damage: float = 0.0
    recovery_rate: float = 0.01
    last_repair: Optional[datetime] = None
    excitation_count: int = 0
    overexposure_events: int = 0

    def apply_damage(self, amount: float):
        self.damage = min(1.0, self.damage + amount)
        self.health = max(0.0, 1.0 - self.damage)

    def repair(self, rate: Optional[float] = None):
        rate = rate or self.recovery_rate
        self.damage = max(0.0, self.damage - rate)
        self.health = min(1.0, self.health + rate)
        self.last_repair = datetime.now(timezone.utc)

class HarvestingMode(Enum):
    FULL = "full"
    MODULATED = "modulated"
    CONSERVATIVE = "conservative"
    OFF = "off"

# ============================================================================
# LSTM Persistence
# ============================================================================
class LSTMPersistence:
    def __init__(self, model_dir: str):
        self.model_dir = model_dir
        os.makedirs(model_dir, exist_ok=True)

    def save_model(self, pigment_name: str, model: 'tf.keras.Model'):
        if not TENSORFLOW_AVAILABLE:
            return
        path = os.path.join(self.model_dir, f"{pigment_name}.keras")
        model.save(path)
        logger.info("LSTM model saved", pigment=pigment_name, path=path)

    def load_model(self, pigment_name: str) -> Optional['tf.keras.Model']:
        if not TENSORFLOW_AVAILABLE:
            return None
        path = os.path.join(self.model_dir, f"{pigment_name}.keras")
        if os.path.exists(path):
            try:
                model = tf.keras.models.load_model(path)
                logger.info("LSTM model loaded", pigment=pigment_name, path=path)
                return model
            except Exception as e:
                logger.error("Failed to load LSTM model", pigment=pigment_name, error=str(e))
        return None

# ============================================================================
# Fallback Prediction Models
# ============================================================================
class FallbackPredictor:
    def __init__(self, model_type: str = "moving_average", window_size: int = 20, arima_order: Tuple[int, int, int] = (1, 1, 1)):
        self.model_type = model_type
        self.window_size = window_size
        self.history = deque(maxlen=window_size)
        self.arima_order = arima_order

    def update(self, value: float):
        self.history.append(value)

    def predict(self, steps: int = 1) -> List[float]:
        if not self.history:
            return [0.0] * steps
        if self.model_type == "moving_average":
            avg = sum(self.history) / len(self.history)
            return [avg] * steps
        elif self.model_type == "linear":
            x = np.arange(len(self.history))
            y = np.array(self.history)
            if len(x) < 2:
                return [y[-1]] * steps
            coeffs = np.polyfit(x, y, 1)
            preds = []
            for i in range(1, steps+1):
                preds.append(coeffs[0] * (len(self.history) - 1 + i) + coeffs[1])
            return preds
        elif self.model_type == "arima":
            if len(self.history) < 3:
                return [self.history[-1]] * steps
            diff = [self.history[i] - self.history[i-1] for i in range(1, len(self.history))]
            if len(diff) > 1:
                ar1 = np.corrcoef(diff[:-1], diff[1:])[0,1]
                if np.isnan(ar1):
                    ar1 = 0.0
            else:
                ar1 = 0.0
            last_diff = diff[-1] if diff else 0
            next_diff = ar1 * last_diff
            pred = self.history[-1] + next_diff
            return [pred] * steps
        else:
            return [self.history[-1]] * steps

# ============================================================================
# Advanced Circadian Model
# ============================================================================
class AdvancedCircadianModel:
    def __init__(self, latitude: float = 0.0, longitude: float = 0.0):
        self.latitude = latitude
        self.longitude = longitude

    def get_solar_elevation(self, dt: Optional[datetime] = None) -> float:
        if dt is None:
            dt = datetime.now(timezone.utc)
        hour = dt.hour + dt.minute/60.0
        elevation = math.sin(math.pi * (hour - 6) / 12)
        return max(0, elevation)

    def get_multiplier(self, pigment: Dict[str, Any]) -> float:
        peak_hours = pigment.get('circadian_peak_hours', list(range(24)))
        now = datetime.now(timezone.utc)
        hour = now.hour
        if hour in peak_hours:
            return 1.0
        distance = min(abs(h - hour) for h in peak_hours)
        return max(0.2, 1.0 - distance / 12.0)

# ============================================================================
# Environmental Anomaly Detector
# ============================================================================
class EnvironmentalAnomalyDetector:
    def __init__(self, window_size: int = 100, std_threshold: float = 3.0):
        self.history = defaultdict(lambda: deque(maxlen=window_size))
        self.std_threshold = std_threshold

    def update(self, data: Dict[str, float]):
        for key, value in data.items():
            self.history[key].append(value)

    def detect(self, data: Dict[str, float]) -> Dict[str, bool]:
        anomalies = {}
        for key, value in data.items():
            if key in self.history and len(self.history[key]) > 10:
                mean = np.mean(self.history[key])
                std = np.std(self.history[key])
                if std == 0:
                    anomalies[key] = False
                else:
                    z_score = (value - mean) / std
                    anomalies[key] = abs(z_score) > self.std_threshold
            else:
                anomalies[key] = False
        return anomalies

# ============================================================================
# Enhanced Pigment Array (implements IPigmentArray)
# ============================================================================
class EnhancedPigmentArray(IPigmentArray):
    def __init__(self, config: HarvesterConfig, task_manager: TaskManager, event_bus: EventBus):
        self.config = config
        self.task_manager = task_manager
        self.event_bus = event_bus
        # Pigment definitions (unchanged)
        self.pigments = {
            'chlorophyll_a': {
                'target': 'renewable_availability',
                'base_sensitivity': 1.0,
                'sensitivity': 1.0,
                'response_time_ms': 100,
                'saturation_threshold': 0.9,
                'noise_floor': 0.05,
                'photoinhibition_rate': config.pigment.photoinhibition_rate,
                'safe_excitation_level': config.pigment.safe_excitation_level,
                'repair_rate': config.pigment.default_repair_rate,
                'circadian_peak_hours': [10, 11, 12, 13, 14],
                'specialization': 'solar',
                'energy_conversion_factor': 0.01,
                'critical_threshold': 0.85
            },
            'chlorophyll_b': {
                'target': 'carbon_intensity',
                'base_sensitivity': 0.8,
                'sensitivity': 0.8,
                'response_time_ms': 200,
                'saturation_threshold': 0.7,
                'noise_floor': 0.03,
                'photoinhibition_rate': 0.0005,
                'safe_excitation_level': 0.8,
                'repair_rate': config.pigment.default_repair_rate * 1.5,
                'circadian_peak_hours': list(range(24)),
                'specialization': 'carbon',
                'energy_conversion_factor': 0.001,
                'critical_threshold': 0.75
            },
            'carotenoids': {
                'target': 'waste_heat',
                'base_sensitivity': 0.6,
                'sensitivity': 0.6,
                'response_time_ms': 500,
                'saturation_threshold': 0.8,
                'noise_floor': 0.1,
                'photoinhibition_rate': 0.0002,
                'safe_excitation_level': 0.9,
                'repair_rate': config.pigment.default_repair_rate * 2.0,
                'circadian_peak_hours': list(range(24)),
                'specialization': 'thermal',
                'energy_conversion_factor': 0.01,
                'critical_threshold': 0.9
            },
            'phycobilins': {
                'target': 'edge_availability',
                'base_sensitivity': 0.7,
                'sensitivity': 0.7,
                'response_time_ms': 300,
                'saturation_threshold': 0.6,
                'noise_floor': 0.08,
                'photoinhibition_rate': 0.0003,
                'safe_excitation_level': 0.85,
                'repair_rate': config.pigment.default_repair_rate * 1.2,
                'circadian_peak_hours': list(range(24)),
                'specialization': 'edge',
                'energy_conversion_factor': 0.005,
                'critical_threshold': 0.8
            },
            'xanthophylls': {
                'target': 'system_overload',
                'base_sensitivity': 0.9,
                'sensitivity': 0.9,
                'response_time_ms': 50,
                'saturation_threshold': 1.0,
                'noise_floor': 0.01,
                'photoinhibition_rate': 0.0001,
                'safe_excitation_level': 0.95,
                'repair_rate': config.pigment.default_repair_rate * 2.5,
                'circadian_peak_hours': list(range(24)),
                'specialization': 'protection',
                'energy_conversion_factor': 0.02,
                'critical_threshold': 0.95
            }
        }
        self._pigment_names = list(self.pigments.keys())
        self._targets = np.array([self.pigments[p]['target'] for p in self._pigment_names])
        self._sensitivities = np.array([self.pigments[p]['sensitivity'] for p in self._pigment_names])
        self._safe_levels = np.array([self.pigments[p]['safe_excitation_level'] for p in self._pigment_names])
        self._saturation_thresholds = np.array([self.pigments[p]['saturation_threshold'] for p in self._pigment_names])
        self._noise_floors = np.array([self.pigments[p]['noise_floor'] for p in self._pigment_names])

        self.pigment_health: Dict[str, PigmentHealth] = {
            name: PigmentHealth(pigment_name=name, recovery_rate=self.pigments[name]['repair_rate'])
            for name in self._pigment_names
        }
        self._health_lock = asyncio.Lock()
        self.excitation_history: Dict[str, deque] = {
            name: deque(maxlen=500) for name in self._pigment_names
        }
        self._history_lock = asyncio.Lock()
        self.circadian_model = AdvancedCircadianModel(config.latitude, config.longitude)
        self.prediction_models: Dict[str, Dict[str, Any]] = {}
        self.lstm_predictors = {} if TENSORFLOW_AVAILABLE else {}
        self.lstm_persistence = LSTMPersistence(config.pigment.lstm_model_dir) if TENSORFLOW_AVAILABLE else None
        self.fallback_predictors = {
            name: FallbackPredictor(model_type=config.pigment.fallback_model,
                                   window_size=config.pigment.lstm_sequence_length,
                                   arima_order=config.pigment.arima_order if config.pigment.fallback_model == "arima" else None)
            for name in self._pigment_names
        }
        self.anomaly_detector = EnvironmentalAnomalyDetector()
        self.task_manager.start_task("pigment_repair", self._repair_loop)
        self.task_manager.start_task("pigment_adaptation", self._adaptation_loop)
        self.task_manager.start_task("pigment_anomaly", self._anomaly_detection_loop)
        self._thread_pool = ThreadPoolExecutor(max_workers=4)
        logger.info("Enhanced Pigment Array initialized", pigments=len(self.pigments))

    async def _repair_loop(self):
        while True:
            try:
                async with self._health_lock:
                    for health in self.pigment_health.values():
                        if health.damage > 0:
                            health.repair()
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Repair loop error", error=str(e))
                await asyncio.sleep(60)

    async def _adaptation_loop(self):
        while True:
            try:
                async with self._history_lock:
                    for name, hist in self.excitation_history.items():
                        if len(hist) < 10:
                            continue
                        avg_excitation = np.mean(hist)
                        target = self.pigments[name]['safe_excitation_level']
                        if avg_excitation > target * 1.2:
                            self.pigments[name]['sensitivity'] *= 0.95
                        elif avg_excitation < target * 0.8:
                            self.pigments[name]['sensitivity'] *= 1.05
                        self.pigments[name]['sensitivity'] = np.clip(
                            self.pigments[name]['sensitivity'],
                            0.5 * self.pigments[name]['base_sensitivity'],
                            2.0 * self.pigments[name]['base_sensitivity']
                        )
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Adaptation loop error", error=str(e))
                await asyncio.sleep(300)

    async def _anomaly_detection_loop(self):
        while True:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Anomaly detection loop error", error=str(e))
                await asyncio.sleep(3600)

    async def sense_environment(self, environmental_data: Dict[str, float]) -> Dict[str, float]:
        self.anomaly_detector.update(environmental_data)
        anomalies = self.anomaly_detector.detect(environmental_data)
        circadian_multipliers = {}
        for name, pigment in self.pigments.items():
            circadian_multipliers[name] = self.circadian_model.get_multiplier(pigment)

        excitations = {}
        async with self._health_lock:
            for name in self._pigment_names:
                pigment = self.pigments[name]
                target_key = pigment['target']
                raw_value = environmental_data.get(target_key, 0.0)
                sensitivity = pigment['sensitivity']
                circadian = circadian_multipliers[name]
                health = self.pigment_health[name].health

                excitation = raw_value * sensitivity * circadian * health
                excitation = np.clip(excitation, 0.0, pigment['saturation_threshold'])
                if random.random() < pigment['noise_floor']:
                    excitation += random.uniform(-0.05, 0.05)
                excitation = max(0.0, excitation)

                async with self._history_lock:
                    self.excitation_history[name].append(excitation)

                if excitation > pigment['safe_excitation_level']:
                    damage = (excitation - pigment['safe_excitation_level']) * pigment['photoinhibition_rate']
                    self.pigment_health[name].apply_damage(damage)
                    self.pigment_health[name].overexposure_events += 1

                excitations[name] = excitation

        for name, val in excitations.items():
            self.fallback_predictors[name].update(val)

        return excitations

    async def get_predictions(self) -> Dict[str, Dict[str, Any]]:
        predictions = {}
        for name in self._pigment_names:
            pred = {}
            if name in self.lstm_predictors and TENSORFLOW_AVAILABLE:
                try:
                    model = self.lstm_predictors[name]
                    async with self._history_lock:
                        hist = list(self.excitation_history[name])
                    if len(hist) >= self.config.pigment.lstm_sequence_length:
                        seq = np.array(hist[-self.config.pigment.lstm_sequence_length:]).reshape(1, -1, 1)
                        pred['medium_term_300s'] = float(model.predict(seq, verbose=0)[0][0])
                        pred['confidence'] = 0.9
                    else:
                        pred['medium_term_300s'] = self.fallback_predictors[name].predict(1)[0]
                        pred['confidence'] = 0.5
                except Exception as e:
                    logger.warning("LSTM prediction failed", pigment=name, error=str(e))
                    pred['medium_term_300s'] = self.fallback_predictors[name].predict(1)[0]
                    pred['confidence'] = 0.5
            else:
                pred['medium_term_300s'] = self.fallback_predictors[name].predict(1)[0]
                pred['confidence'] = 0.5
            predictions[name] = pred
        return predictions

    def get_pigment_health_summary(self) -> Dict[str, float]:
        summary = {}
        async with self._health_lock:
            for name, health in self.pigment_health.items():
                summary[name] = health.health
        return summary

    def get_circadian_summary(self) -> Dict[str, float]:
        return {name: self.circadian_model.get_multiplier(pigment) for name, pigment in self.pigments.items()}

    async def stop(self):
        pass

# ============================================================================
# Enhanced Reaction Center (implements IReactionCenter)
# ============================================================================
class EnhancedReactionCenter(IReactionCenter):
    def __init__(self, config: HarvesterConfig, task_manager: TaskManager,
                 token_manager=None, gradient_manager=None, event_bus: Optional[EventBus] = None):
        self.config = config
        self.task_manager = task_manager
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        self.event_bus = event_bus
        self.base_quantum_efficiency = config.reaction_center.base_quantum_efficiency
        self.current_efficiency = config.reaction_center.base_quantum_efficiency
        self.min_efficiency = config.reaction_center.min_efficiency
        self.max_efficiency = config.reaction_center.max_efficiency
        self.demand_modulation_enabled = config.reaction_center.demand_modulation_enabled
        self.token_abundance_threshold = config.reaction_center.token_abundance_threshold
        self.token_scarcity_threshold = config.reaction_center.token_scarcity_threshold
        self.demand_response_factor = config.reaction_center.demand_response_factor
        self.repair_rate = config.reaction_center.repair_rate
        self.damage_threshold = config.health.damage_threshold
        self.cumulative_damage = 0.0
        self.conversion_history = deque(maxlen=2000)
        self.efficiency_history = deque(maxlen=100)
        self.performance_metrics = {'peak_efficiency': config.reaction_center.base_quantum_efficiency,
                                   'avg_conversion_rate': 0.0, 'total_conversions': 0}
        self._lock = asyncio.Lock()
        self.task_manager.start_task("rc_maintenance", self._maintenance_loop)
        self.task_manager.start_task("rc_performance", self._performance_loop)
        logger.info("Enhanced Reaction Center initialized")

    async def _maintenance_loop(self):
        while True:
            try:
                async with self._lock:
                    if self.cumulative_damage > 0:
                        repair = min(self.cumulative_damage, self.repair_rate)
                        self.cumulative_damage -= repair
                        self.current_efficiency = self.base_quantum_efficiency * (1 - self.cumulative_damage)
                        self.current_efficiency = np.clip(self.current_efficiency, self.min_efficiency, self.max_efficiency)
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Maintenance loop error", error=str(e))
                await asyncio.sleep(60)

    async def _performance_loop(self):
        while True:
            try:
                async with self._lock:
                    if self.conversion_history:
                        self.performance_metrics['avg_conversion_rate'] = sum(self.conversion_history) / len(self.conversion_history)
                        self.performance_metrics['peak_efficiency'] = max(self.performance_metrics['peak_efficiency'],
                                                                          self.current_efficiency)
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Performance loop error", error=str(e))
                await asyncio.sleep(300)

    async def harvest_cycle(self, excitations: Dict[str, float]) -> Dict[str, Any]:
        async with self._lock:
            demand_factor = 1.0
            if self.demand_modulation_enabled and self.token_manager:
                summary = self.token_manager.get_account_summary(None)
                if summary:
                    total_tokens = summary.get('total_supply', 0)
                    if total_tokens > self.token_abundance_threshold:
                        demand_factor = 1.0 - self.demand_response_factor * 0.5
                    elif total_tokens < self.token_scarcity_threshold:
                        demand_factor = 1.0 + self.demand_response_factor
                if self.gradient_manager:
                    gradient_intensity = self.gradient_manager.get_intensity()
                    demand_factor *= (1 + 0.1 * gradient_intensity)

            total_excitation = sum(excitations.values())
            efficiency = self.current_efficiency * demand_factor
            efficiency = np.clip(efficiency, self.min_efficiency, self.max_efficiency)

            eco_atp_generated = total_excitation * efficiency * 0.1
            self.total_conversions += eco_atp_generated
            self.conversion_history.append(eco_atp_generated)
            self.efficiency_history.append(efficiency)

            if efficiency > 0.9:
                self.cumulative_damage += 0.001
            elif efficiency < 0.3:
                self.cumulative_damage += 0.005

            self.performance_metrics['total_conversions'] = self.total_conversions

            if self.token_manager and hasattr(self.token_manager, 'credit'):
                self.token_manager.credit(self.account_id, eco_atp_generated)

            # Publish event
            if self.event_bus:
                await self.event_bus.publish("harvest_completed", {
                    "eco_atp_generated": eco_atp_generated,
                    "efficiency": efficiency,
                    "demand_factor": demand_factor,
                    "total_excitation": total_excitation
                })

            return {
                'eco_atp_generated': eco_atp_generated,
                'efficiency': efficiency,
                'demand_factor': demand_factor,
                'total_excitation': total_excitation
            }

    def get_efficiency_stats(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                'current_efficiency': self.current_efficiency,
                'base_efficiency': self.base_quantum_efficiency,
                'cumulative_damage': self.cumulative_damage,
                'avg_conversion_rate': self.performance_metrics['avg_conversion_rate'],
                'peak_efficiency': self.performance_metrics['peak_efficiency'],
                'total_conversions': self.performance_metrics['total_conversions']
            }

    async def stop(self):
        pass

# ============================================================================
# HealthMonitor (implements IHealthMonitor)
# ============================================================================
class HealthMonitor(IHealthMonitor):
    def __init__(self, config: HarvesterConfig, harvester_id: str, event_bus: Optional[EventBus] = None):
        self.config = config
        self.harvester_id = harvester_id
        self.event_bus = event_bus
        self.metrics: Dict[str, Any] = {}
        self.recommendations: List[Dict[str, Any]] = []
        self.alert_history = deque(maxlen=100)
        self.thresholds = {
            'efficiency_warning': config.health.efficiency_warning_threshold,
            'efficiency_critical': config.health.efficiency_critical_threshold,
            'damage_warning': config.health.damage_warning_threshold,
            'damage_critical': config.health.damage_critical_threshold,
            'harvest_rate_min': config.health.harvest_rate_min,
            'prediction_accuracy_min': config.health.prediction_accuracy_min
        }
        if config.enable_prometheus and PROMETHEUS_AVAILABLE:
            self.prometheus_metrics = {
                'harvesting_rate': Gauge('harvester_rate', 'Harvesting rate'),
                'pigment_health': Gauge('pigment_health', 'Pigment health', ['pigment']),
                'mode_transitions': Counter('mode_transitions', 'Mode transitions'),
                'prediction_accuracy': Histogram('prediction_accuracy', 'Prediction accuracy')
            }
        else:
            self.prometheus_metrics = None
        logger.info("HealthMonitor initialized")

    def collect_metrics(self, harvester_state: Dict[str, Any]) -> Dict[str, Any]:
        self.metrics['timestamp'] = datetime.now(timezone.utc).isoformat()
        self.metrics['harvester_id'] = self.harvester_id
        self.metrics['total_harvested'] = harvester_state.get('total_harvested', 0)
        self.metrics['harvest_cycles'] = harvester_state.get('harvest_cycles', 0)
        self.metrics['efficiency'] = harvester_state.get('efficiency', 0)
        self.metrics['mode'] = harvester_state.get('mode', 'unknown')
        pigment_health = harvester_state.get('pigment_health', {})
        self.metrics['pigment_health'] = pigment_health
        overall_health = np.mean(list(pigment_health.values())) if pigment_health else 1.0
        self.metrics['overall_health'] = overall_health
        predictions = harvester_state.get('predictions', {})
        confidences = [p.get('confidence', 0.5) for p in predictions.values()]
        avg_confidence = np.mean(confidences) if confidences else 0.5
        self.metrics['prediction_confidence'] = avg_confidence

        if self.prometheus_metrics:
            self.prometheus_metrics['harvesting_rate'].set(self.metrics.get('total_harvested', 0))
            for pigment, health in pigment_health.items():
                self.prometheus_metrics['pigment_health'].labels(pigment=pigment).set(health)
            self.prometheus_metrics['mode_transitions'].inc()
            self.prometheus_metrics['prediction_accuracy'].observe(avg_confidence)

        self.recommendations = self._generate_recommendations(harvester_state)

        if self.event_bus and self.recommendations:
            asyncio.create_task(self.event_bus.publish("health_recommendation", self.recommendations))

        return self.metrics.copy()

    def _generate_recommendations(self, harvester_state: Dict[str, Any]) -> List[Dict[str, Any]]:
        recs = []
        efficiency = harvester_state.get('efficiency', 1.0)
        if efficiency < self.thresholds['efficiency_warning']:
            recs.append({'type': 'warning', 'message': 'Efficiency below warning threshold', 'severity': 'medium'})
        if efficiency < self.thresholds['efficiency_critical']:
            recs.append({'type': 'critical', 'message': 'Efficiency critical, immediate action needed', 'severity': 'high'})
        overall_health = self.metrics.get('overall_health', 1.0)
        if overall_health < self.thresholds['damage_warning']:
            recs.append({'type': 'warning', 'message': 'Pigment health below warning threshold', 'severity': 'medium'})
        if overall_health < self.thresholds['damage_critical']:
            recs.append({'type': 'critical', 'message': 'Pigment health critical, initiate healing', 'severity': 'high'})
        return recs

    def get_metrics(self) -> Dict[str, Any]:
        return self.metrics.copy()

    def get_recommendations(self) -> List[Dict[str, Any]]:
        return self.recommendations.copy()

# ============================================================================
# SelfHealer (implements ISelfHealer, uses event bus)
# ============================================================================
class SelfHealer(ISelfHealer):
    def __init__(self, harvester: 'EnhancedPhotosyntheticHarvester', config: HarvesterConfig,
                 event_bus: Optional[EventBus] = None):
        self.harvester = harvester
        self.config = config
        self.event_bus = event_bus
        self.healing_attempts: Dict[str, int] = {}
        self.max_attempts = config.health.max_healing_attempts
        self.cooldown_period = config.health.healing_cooldown
        self.healing_strategies = {
            'photoinhibition': self._apply_photoinhibition_healing,
            'prediction_drift': self._recalibrate_predictions,
            'gradient_stagnation': self._stimulate_gradients,
            'efficiency_collapse': self._restore_efficiency
        }
        logger.info("SelfHealer initialized")

    async def apply_healing(self, issue_type: str) -> bool:
        if issue_type not in self.healing_strategies:
            logger.warning("Unknown healing strategy", issue_type=issue_type)
            return False
        attempts = self.healing_attempts.get(issue_type, 0)
        if attempts >= self.max_attempts:
            logger.warning("Max healing attempts reached for", issue_type=issue_type)
            return False
        try:
            await self.healing_strategies[issue_type]()
            self.healing_attempts[issue_type] = attempts + 1
            logger.info("Healing applied", issue_type=issue_type, attempts=attempts+1)
            if self.event_bus:
                await self.event_bus.publish("healing_applied", {"issue_type": issue_type, "attempts": attempts+1})
            return True
        except Exception as e:
            logger.error("Healing failed", issue_type=issue_type, error=str(e))
            return False

    async def _apply_photoinhibition_healing(self):
        async with self.harvester.pigments._health_lock:
            for pigment, health in self.harvester.pigments.pigment_health.items():
                health.recovery_rate *= 1.5
                health.repair()
                self.harvester.pigments.pigments[pigment]['sensitivity'] *= 0.8
        async with self.harvester.reaction_center._lock:
            self.harvester.reaction_center.cumulative_damage *= 0.8
        logger.info("Photoinhibition healing applied")

    async def _recalibrate_predictions(self):
        for name in self.harvester.pigments._pigment_names:
            predictor = self.harvester.pigments.fallback_predictors[name]
            async with self.harvester.pigments._history_lock:
                hist = list(self.harvester.pigments.excitation_history[name])
            predictor.history.clear()
            for val in hist[-50:]:
                predictor.update(val)
        logger.info("Prediction recalibration applied")

    async def _stimulate_gradients(self):
        if self.harvester.gradient_manager:
            await self.harvester.gradient_manager.increase_intensity(0.2)
            logger.info("Gradient stimulation applied")
        else:
            logger.warning("No gradient manager available for stimulation")

    async def _restore_efficiency(self):
        async with self.harvester.reaction_center._lock:
            self.harvester.reaction_center.cumulative_damage = max(0, self.harvester.reaction_center.cumulative_damage - 0.1)
            self.harvester.reaction_center.current_efficiency = self.harvester.reaction_center.base_quantum_efficiency * (
                1 - self.harvester.reaction_center.cumulative_damage
            )
            self.harvester.reaction_center.current_efficiency = np.clip(
                self.harvester.reaction_center.current_efficiency,
                self.harvester.reaction_center.min_efficiency,
                self.harvester.reaction_center.max_efficiency
            )
        logger.info("Efficiency restoration applied")

# ============================================================================
# Persistence Backend (improved with JSON and schema versioning)
# ============================================================================
class PersistenceBackend:
    """Abstract base for persistence backends."""
    async def save(self, key: str, data: Any) -> bool:
        raise NotImplementedError
    async def load(self, key: str) -> Optional[Any]:
        raise NotImplementedError
    async def delete(self, key: str) -> bool:
        raise NotImplementedError

class MemoryBackend(PersistenceBackend):
    def __init__(self):
        self._store = {}
    async def save(self, key: str, data: Any) -> bool:
        self._store[key] = data
        return True
    async def load(self, key: str) -> Optional[Any]:
        return self._store.get(key)
    async def delete(self, key: str) -> bool:
        if key in self._store:
            del self._store[key]
            return True
        return False

class FileBackend(PersistenceBackend):
    def __init__(self, base_dir: str = "./harvester_data"):
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)
        self._cache = {}
        self._cache_lock = asyncio.Lock()

    def _get_path(self, key: str) -> str:
        return os.path.join(self.base_dir, f"{key}.json")

    async def save(self, key: str, data: Any) -> bool:
        path = self._get_path(key)
        try:
            serialized = {
                "version": "1.0",
                "data": data
            }
            with open(path, 'w') as f:
                json.dump(serialized, f, default=self._json_default)
            async with self._cache_lock:
                self._cache[key] = data
            return True
        except Exception as e:
            logger.error("File save failed", key=key, error=str(e))
            return False

    def _json_default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Enum):
            return obj.value
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        raise TypeError(f"Object of type {type(obj)} not JSON serializable")

    async def load(self, key: str) -> Optional[Any]:
        async with self._cache_lock:
            if key in self._cache:
                return self._cache[key]
        path = self._get_path(key)
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r') as f:
                serialized = json.load(f)
            version = serialized.get("version", "1.0")
            data = serialized["data"]
            async with self._cache_lock:
                self._cache[key] = data
            return data
        except Exception as e:
            logger.error("File load failed", key=key, error=str(e))
            return None

    async def delete(self, key: str) -> bool:
        path = self._get_path(key)
        if os.path.exists(path):
            try:
                os.remove(path)
                async with self._cache_lock:
                    if key in self._cache:
                        del self._cache[key]
                return True
            except Exception as e:
                logger.error("File delete failed", key=key, error=str(e))
                return False
        return False

class RedisBackend(PersistenceBackend):
    def __init__(self, redis_client):
        self.redis = redis_client
        self.circuit_breaker = GlobalCircuitBreaker().get_or_create("redis")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
           retry=retry_if_exception_type(redis.ConnectionError))
    async def save(self, key: str, data: Any) -> bool:
        async def _save():
            serialized = json.dumps(data, default=self._json_default)
            await self.redis.set(key, serialized)
            return True
        try:
            return await self.circuit_breaker.call(_save)
        except CircuitBreakerOpenError:
            logger.warning("Circuit breaker open, falling back to memory?")
            return False

    def _json_default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Enum):
            return obj.value
        if hasattr(obj, '__dict__'):
            return obj.__dict__
        raise TypeError(f"Object of type {type(obj)} not JSON serializable")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
           retry=retry_if_exception_type(redis.ConnectionError))
    async def load(self, key: str) -> Optional[Any]:
        async def _load():
            data = await self.redis.get(key)
            if data is None:
                return None
            return json.loads(data)
        try:
            return await self.circuit_breaker.call(_load)
        except CircuitBreakerOpenError:
            return None

    async def delete(self, key: str) -> bool:
        try:
            await self.redis.delete(key)
            return True
        except Exception as e:
            logger.error("Redis delete failed", key=key, error=str(e))
            return False

    async def delete_old_checkpoints(self, prefix: str, retention_days: int):
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        pattern = f"{prefix}:checkpoint:*"
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(cursor, match=pattern)
            for key in keys:
                parts = key.split(':')
                if len(parts) >= 3:
                    timestamp_str = parts[-1]
                    try:
                        timestamp = datetime.fromisoformat(timestamp_str)
                        if timestamp < cutoff:
                            await self.redis.delete(key)
                            logger.info("Deleted old Redis checkpoint", key=key)
                    except:
                        pass
            if cursor == 0:
                break

class PersistentHarvesterState(IPersistence):
    def __init__(self, harvester_id: str, config: HarvesterConfig):
        self.harvester_id = harvester_id
        self.config = config
        self.backend: PersistenceBackend
        if config.persistence.backend == "redis" and REDIS_AVAILABLE:
            redis_url = config.persistence.redis_url or "redis://localhost:6379"
            self.backend = RedisBackend(redis.from_url(redis_url))
        elif config.persistence.backend == "file":
            self.backend = FileBackend(config.persistence.base_dir)
        else:
            self.backend = MemoryBackend()
        self._lock = asyncio.Lock()
        logger.info("Persistence initialized", backend=config.persistence.backend)

    async def save_state(self, state: Dict[str, Any]) -> bool:
        key = f"{self.harvester_id}:state"
        async with self._lock:
            return await self.backend.save(key, state)

    async def load_state(self) -> Optional[Dict[str, Any]]:
        key = f"{self.harvester_id}:state"
        async with self._lock:
            return await self.backend.load(key)

    async def save_checkpoint(self, checkpoint: Dict[str, Any]) -> bool:
        timestamp = datetime.now(timezone.utc).isoformat()
        key = f"{self.harvester_id}:checkpoint:{timestamp}"
        async with self._lock:
            return await self.backend.save(key, checkpoint)

    async def load_latest_checkpoint(self) -> Optional[Tuple[str, Dict[str, Any]]]:
        key = f"{self.harvester_id}:checkpoint:latest"
        async with self._lock:
            data = await self.backend.load(key)
            if data:
                return (key, data)
        return None

    async def delete_old_checkpoints(self, retention_days: int):
        if isinstance(self.backend, FileBackend):
            cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
            for f in os.listdir(self.backend.base_dir):
                if f.startswith(f"{self.harvester_id}:checkpoint:"):
                    parts = f.split(':')
                    if len(parts) >= 3:
                        timestamp_str = parts[-1].replace('.json', '')
                        try:
                            timestamp = datetime.fromisoformat(timestamp_str)
                            if timestamp < cutoff:
                                os.remove(os.path.join(self.backend.base_dir, f))
                                logger.info("Deleted old checkpoint", file=f)
                        except:
                            pass
        elif isinstance(self.backend, RedisBackend):
            await self.backend.delete_old_checkpoints(self.harvester_id, retention_days)

# ============================================================================
# WebSocket Server (with rate limiting and TLS)
# ============================================================================
class HarvesterWebSocketServer:
    def __init__(self, config: HarvesterConfig):
        self.config = config
        self.host = config.websocket.host
        self.port = config.websocket.port
        self.auth_token = config.websocket.auth_token
        self.use_jwt = config.websocket.use_jwt
        self.jwt_secret = config.websocket.jwt_secret
        self.tls_enabled = config.websocket.tls_enabled
        self.tls_cert = config.websocket.tls_cert
        self.tls_key = config.websocket.tls_key
        self.rate_limit = config.websocket.rate_limit_per_minute
        self.connections: Set[websockets.WebSocketServerProtocol] = set()
        self.stream_interval = 1.0
        self.is_running = False
        self.server = None
        self._broadcast_queue = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._rate_limiter = defaultdict(lambda: deque(maxlen=self.rate_limit))
        self._rate_limit_lock = asyncio.Lock()
        if not WEBSOCKET_AVAILABLE:
            logger.warning("WebSocket support not available")

    async def start(self):
        if not WEBSOCKET_AVAILABLE:
            return
        try:
            ssl_context = None
            if self.tls_enabled:
                import ssl
                ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
                ssl_context.load_cert_chain(self.tls_cert, self.tls_key)
            self.server = await websockets.serve(self._handle_connection, self.host, self.port, ssl=ssl_context)
            self.is_running = True
            logger.info("WebSocket server started", host=self.host, port=self.port, tls=self.tls_enabled)
        except Exception as e:
            logger.error("Failed to start WebSocket server", error=str(e))

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.is_running = False
            async with self._lock:
                for ws in self.connections:
                    await ws.close(1000, "Server shutting down")
                self.connections.clear()
            logger.info("WebSocket server stopped")

    async def _handle_connection(self, websocket: websockets.WebSocketServerProtocol, path):
        client_ip = websocket.remote_address[0]
        async with self._rate_limit_lock:
            timestamps = self._rate_limiter[client_ip]
            now = time.time()
            while timestamps and now - timestamps[0] > 60:
                timestamps.popleft()
            if len(timestamps) >= self.rate_limit:
                await websocket.close(1008, "Rate limit exceeded")
                return
            timestamps.append(now)

        if self.auth_token or self.use_jwt:
            try:
                auth_msg = await asyncio.wait_for(websocket.recv(), timeout=5)
                if self.use_jwt:
                    if not self._verify_jwt(auth_msg):
                        await websocket.close(1008, "Authentication failed")
                        return
                else:
                    if auth_msg != self.auth_token:
                        await websocket.close(1008, "Authentication failed")
                        return
            except asyncio.TimeoutError:
                await websocket.close(1008, "Authentication timeout")
                return
            except Exception as e:
                logger.error("Auth error", error=str(e))
                await websocket.close(1008, "Authentication error")
                return
        async with self._lock:
            self.connections.add(websocket)
        try:
            async for message in websocket:
                await self._handle_message(websocket, message)
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            logger.error("WebSocket error", error=str(e))
        finally:
            async with self._lock:
                self.connections.remove(websocket)

    def _verify_jwt(self, token: str) -> bool:
        if not JWT_AVAILABLE:
            logger.warning("PyJWT not installed, using simple token comparison")
            return token == self.jwt_secret if self.jwt_secret else False
        if not self.jwt_secret:
            return False
        try:
            payload = jwt.decode(token, self.jwt_secret, algorithms=["HS256"])
            return True
        except jwt.ExpiredSignatureError:
            logger.warning("JWT expired")
            return False
        except jwt.InvalidTokenError:
            logger.warning("Invalid JWT")
            return False

    async def _handle_message(self, websocket, message: str):
        try:
            data = json.loads(message)
            msg_type = data.get('type')
            if msg_type == 'subscribe':
                pass
            elif msg_type == 'ping':
                await websocket.send(json.dumps({'type': 'pong'}))
            elif msg_type == 'control':
                action = data.get('action')
                if action == 'set_mode':
                    mode = data.get('mode')
                    if mode in ['full', 'modulated', 'conservative', 'off']:
                        await self._parent_harvester.set_mode(HarvestingMode(mode.upper()))
                        await websocket.send(json.dumps({'type': 'control_response', 'status': 'ok', 'action': action}))
                    else:
                        await websocket.send(json.dumps({'type': 'control_response', 'status': 'error', 'message': 'Invalid mode'}))
                elif action == 'trigger_healing':
                    issue = data.get('issue')
                    if issue:
                        success = await self._parent_harvester.self_healer.apply_healing(issue)
                        await websocket.send(json.dumps({'type': 'control_response', 'status': 'ok' if success else 'error'}))
                elif action == 'start_evolution':
                    await self._parent_harvester.genetic_optimizer.evolve()
                    await websocket.send(json.dumps({'type': 'control_response', 'status': 'ok', 'action': action}))
                else:
                    await websocket.send(json.dumps({'type': 'control_response', 'status': 'error', 'message': 'Unknown action'}))
        except Exception as e:
            logger.error("Error handling message", error=str(e))

    async def broadcast(self, data: Dict[str, Any]):
        if not self.connections:
            return
        message = json.dumps(data)
        async with self._lock:
            for ws in self.connections:
                try:
                    await ws.send(message)
                except Exception as e:
                    logger.error("Broadcast failed to client", error=str(e))

    async def broadcast_loop(self, harvester_stats_provider: Callable[[], Dict[str, Any]]):
        while self.is_running:
            try:
                stats = harvester_stats_provider()
                await self.broadcast(stats)
                await asyncio.sleep(self.stream_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Broadcast loop error", error=str(e))
                await asyncio.sleep(5)

# ============================================================================
# Enhanced Multi-Objective Genetic Optimizer (NSGA-II)
# ============================================================================
@dataclass
class MOPDPoint:
    individual: Dict[str, Any]
    energy_output: float
    pigment_health: float
    longterm_efficiency: float
    resource_usage: float
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPoint':
        return cls(**data)

class HarvesterGeneticOptimizer:
    def __init__(self, harvester: 'EnhancedPhotosyntheticHarvester', config: HarvesterConfig):
        self.harvester = harvester
        self.config = config
        self.population_size = config.genetic.population_size
        self.mutation_rate = config.genetic.mutation_rate
        self.crossover_rate = config.genetic.crossover_rate
        self.generations = config.genetic.generations
        self.tournament_size = config.genetic.tournament_size
        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self._lock = asyncio.Lock()
        self.param_bounds = {
            'conversion_factors': (0.001, 0.1),
            'sensitivity_multipliers': (0.5, 2.0),
            'repair_rates': (0.005, 0.05),
            'demand_response_factor': (0.1, 1.0)
        }
        self.recent_data = deque(maxlen=config.genetic.simulation_cycles * 2)
        # MOPD attributes
        self.pareto_front: List[MOPDPoint] = []
        self._eval_cache: Dict[Tuple[Any, ...], Dict[str, float]] = {}
        logger.info("Enhanced Multi-Objective Genetic Optimizer initialized (NSGA-II)")

    # ----------------------------------------------------------------------
    # Initialization
    # ----------------------------------------------------------------------
    def _initialize_individual(self) -> Dict:
        ind = {
            'conversion_factors': {},
            'sensitivity_multipliers': {},
            'repair_rates': {},
            'demand_response_factor': random.uniform(*self.param_bounds['demand_response_factor'])
        }
        pigments = self.harvester.pigments.pigments.keys()
        for p in pigments:
            ind['conversion_factors'][p] = random.uniform(*self.param_bounds['conversion_factors'])
            ind['sensitivity_multipliers'][p] = random.uniform(*self.param_bounds['sensitivity_multipliers'])
            ind['repair_rates'][p] = random.uniform(*self.param_bounds['repair_rates'])
        return ind

    def _initialize_population(self) -> List[Dict]:
        return [self._initialize_individual() for _ in range(self.population_size)]

    # ----------------------------------------------------------------------
    # Cache key
    # ----------------------------------------------------------------------
    def _individual_to_cache_key(self, individual: Dict) -> Tuple[Any, ...]:
        conv = tuple(sorted(individual['conversion_factors'].items()))
        sens = tuple(sorted(individual['sensitivity_multipliers'].items()))
        rep = tuple(sorted(individual['repair_rates'].items()))
        demand = individual['demand_response_factor']
        return (conv, sens, rep, demand)

    # ----------------------------------------------------------------------
    # Multi-objective evaluation
    # ----------------------------------------------------------------------
    async def _evaluate_individual_mo(self, individual: Dict) -> Dict[str, float]:
        key = self._individual_to_cache_key(individual)
        if key in self._eval_cache:
            return self._eval_cache[key]

        if not self.recent_data:
            objectives = {
                'energy_output': 0.0,
                'pigment_health': 1.0,
                'longterm_efficiency': 0.0,
                'resource_usage': 0.0
            }
        else:
            total_energy = 0.0
            total_damage = 0.0
            total_efficiency = 0.0
            total_repair_cost = 0.0
            cycles = 0
            for env_data in self.recent_data:
                total_excitation = 0.0
                for pigment_name, pigment in self.harvester.pigments.pigments.items():
                    target_key = pigment['target']
                    raw = env_data.get(target_key, 0.0)
                    sensitivity = pigment['base_sensitivity'] * individual['sensitivity_multipliers'][pigment_name]
                    conversion = individual['conversion_factors'][pigment_name]
                    excitation = raw * sensitivity
                    excitation = np.clip(excitation, 0, 1.0)
                    converted = excitation * conversion
                    total_excitation += converted

                efficiency = 0.85 * (1 - 0.01 * total_excitation)
                efficiency *= individual['demand_response_factor']
                efficiency = np.clip(efficiency, 0.1, 0.95)

                damage = 0.001 * total_excitation
                repair_cost = sum(individual['repair_rates'].values()) * 0.01

                health = 1.0 - damage + repair_cost * 0.5
                health = np.clip(health, 0.0, 1.0)

                total_energy += total_excitation * efficiency * health
                total_damage += damage
                total_efficiency += efficiency
                total_repair_cost += repair_cost
                cycles += 1

            avg_energy = total_energy / cycles if cycles > 0 else 0.0
            avg_damage = total_damage / cycles if cycles > 0 else 0.0
            avg_efficiency = total_efficiency / cycles if cycles > 0 else 0.0
            avg_repair_cost = total_repair_cost / cycles if cycles > 0 else 0.0

            objectives = {
                'energy_output': avg_energy,
                'pigment_health': 1.0 - avg_damage,
                'longterm_efficiency': avg_efficiency,
                'resource_usage': 1.0 - avg_repair_cost
            }

        self._eval_cache[key] = objectives
        return objectives

    # ----------------------------------------------------------------------
    # NSGA-II core methods
    # ----------------------------------------------------------------------
    def _fast_non_dominated_sort(self, population: List[Dict], objectives: Dict[Tuple[Any, ...], Dict[str, float]]) -> List[List[Dict]]:
        fronts = []
        domination_count = {k: 0 for k in objectives}
        dominated_solutions = {k: [] for k in objectives}

        for p_key, p_obj in objectives.items():
            for q_key, q_obj in objectives.items():
                if p_key == q_key:
                    continue
                if all(p_obj[k] >= q_obj[k] for k in p_obj) and any(p_obj[k] > q_obj[k] for k in p_obj):
                    dominated_solutions[p_key].append(q_key)
                elif all(q_obj[k] >= p_obj[k] for k in q_obj) and any(q_obj[k] > p_obj[k] for k in q_obj):
                    domination_count[p_key] += 1

            if domination_count[p_key] == 0:
                if not fronts:
                    fronts.append([])
                fronts[0].append(p_key)

        i = 0
        while i < len(fronts):
            next_front = []
            for p_key in fronts[i]:
                for q_key in dominated_solutions[p_key]:
                    domination_count[q_key] -= 1
                    if domination_count[q_key] == 0:
                        next_front.append(q_key)
            if next_front:
                fronts.append(next_front)
            i += 1

        key_to_ind = {self._individual_to_cache_key(ind): ind for ind in population}
        return [[key_to_ind[key] for key in front] for front in fronts]

    def _crowding_distance(self, front: List[Dict], objectives: Dict[Tuple[Any, ...], Dict[str, float]]) -> Dict[Tuple[Any, ...], float]:
        if not front:
            return {}
        distances = {self._individual_to_cache_key(ind): 0.0 for ind in front}
        obj_keys = list(next(iter(objectives.values())).keys())
        for obj in obj_keys:
            sorted_front = sorted(front, key=lambda ind: objectives[self._individual_to_cache_key(ind)][obj])
            distances[self._individual_to_cache_key(sorted_front[0])] = float('inf')
            distances[self._individual_to_cache_key(sorted_front[-1])] = float('inf')
            obj_min = objectives[self._individual_to_cache_key(sorted_front[0])][obj]
            obj_max = objectives[self._individual_to_cache_key(sorted_front[-1])][obj]
            if obj_max == obj_min:
                continue
            for i in range(1, len(sorted_front) - 1):
                key = self._individual_to_cache_key(sorted_front[i])
                prev_key = self._individual_to_cache_key(sorted_front[i-1])
                next_key = self._individual_to_cache_key(sorted_front[i+1])
                distances[key] += (objectives[next_key][obj] - objectives[prev_key][obj]) / (obj_max - obj_min)
        return distances

    def _tournament_selection(self, population: List[Dict], fronts: List[List[Dict]], crowding: Dict[Tuple[Any, ...], float]) -> Dict:
        ind1 = random.choice(population)
        ind2 = random.choice(population)
        rank1 = self._get_rank(ind1, fronts)
        rank2 = self._get_rank(ind2, fronts)
        if rank1 < rank2:
            return ind1
        elif rank2 < rank1:
            return ind2
        else:
            key1 = self._individual_to_cache_key(ind1)
            key2 = self._individual_to_cache_key(ind2)
            if crowding.get(key1, 0) > crowding.get(key2, 0):
                return ind1
            else:
                return ind2

    def _get_rank(self, individual: Dict, fronts: List[List[Dict]]) -> int:
        for i, front in enumerate(fronts):
            if individual in front:
                return i
        return len(fronts)

    def _sbx_crossover(self, parent1: Dict, parent2: Dict) -> Tuple[Dict, Dict]:
        child1, child2 = {}, {}
        # conversion_factors
        child1['conversion_factors'] = {}
        child2['conversion_factors'] = {}
        for p in parent1['conversion_factors']:
            if random.random() < 0.5:
                low, high = self.param_bounds['conversion_factors']
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1 / (20 + 1))
                else:
                    beta = (1 / (2 * (1 - u))) ** (1 / (20 + 1))
                val1 = 0.5 * ((1 + beta) * parent1['conversion_factors'][p] + (1 - beta) * parent2['conversion_factors'][p])
                val2 = 0.5 * ((1 - beta) * parent1['conversion_factors'][p] + (1 + beta) * parent2['conversion_factors'][p])
                child1['conversion_factors'][p] = max(low, min(high, val1))
                child2['conversion_factors'][p] = max(low, min(high, val2))
            else:
                child1['conversion_factors'][p] = parent1['conversion_factors'][p]
                child2['conversion_factors'][p] = parent2['conversion_factors'][p]

        for group, bounds in [('sensitivity_multipliers', self.param_bounds['sensitivity_multipliers']),
                              ('repair_rates', self.param_bounds['repair_rates'])]:
            child1[group] = {}
            child2[group] = {}
            for p in parent1[group]:
                if random.random() < 0.5:
                    low, high = bounds
                    u = random.random()
                    if u <= 0.5:
                        beta = (2 * u) ** (1 / (20 + 1))
                    else:
                        beta = (1 / (2 * (1 - u))) ** (1 / (20 + 1))
                    val1 = 0.5 * ((1 + beta) * parent1[group][p] + (1 - beta) * parent2[group][p])
                    val2 = 0.5 * ((1 - beta) * parent1[group][p] + (1 + beta) * parent2[group][p])
                    child1[group][p] = max(low, min(high, val1))
                    child2[group][p] = max(low, min(high, val2))
                else:
                    child1[group][p] = parent1[group][p]
                    child2[group][p] = parent2[group][p]

        low, high = self.param_bounds['demand_response_factor']
        if random.random() < 0.5:
            u = random.random()
            if u <= 0.5:
                beta = (2 * u) ** (1 / (20 + 1))
            else:
                beta = (1 / (2 * (1 - u))) ** (1 / (20 + 1))
            val1 = 0.5 * ((1 + beta) * parent1['demand_response_factor'] + (1 - beta) * parent2['demand_response_factor'])
            val2 = 0.5 * ((1 - beta) * parent1['demand_response_factor'] + (1 + beta) * parent2['demand_response_factor'])
            child1['demand_response_factor'] = max(low, min(high, val1))
            child2['demand_response_factor'] = max(low, min(high, val2))
        else:
            child1['demand_response_factor'] = parent1['demand_response_factor']
            child2['demand_response_factor'] = parent2['demand_response_factor']

        return child1, child2

    def _polynomial_mutation(self, individual: Dict) -> Dict:
        mutant = {}
        for group, bounds in [('conversion_factors', self.param_bounds['conversion_factors']),
                              ('sensitivity_multipliers', self.param_bounds['sensitivity_multipliers']),
                              ('repair_rates', self.param_bounds['repair_rates'])]:
            mutant[group] = {}
            for p, val in individual[group].items():
                if random.random() < self.mutation_rate:
                    low, high = bounds
                    u = random.random()
                    if u < 0.5:
                        delta = (2 * u) ** (1 / (20 + 1)) - 1
                    else:
                        delta = 1 - (2 * (1 - u)) ** (1 / (20 + 1))
                    new_val = val + delta * (high - low)
                    mutant[group][p] = max(low, min(high, new_val))
                else:
                    mutant[group][p] = val
        low, high = self.param_bounds['demand_response_factor']
        if random.random() < self.mutation_rate:
            u = random.random()
            if u < 0.5:
                delta = (2 * u) ** (1 / (20 + 1)) - 1
            else:
                delta = 1 - (2 * (1 - u)) ** (1 / (20 + 1))
            mutant['demand_response_factor'] = max(low, min(high, individual['demand_response_factor'] + delta * (high - low)))
        else:
            mutant['demand_response_factor'] = individual['demand_response_factor']
        return mutant

    # ----------------------------------------------------------------------
    # Dynamic objective weighting
    # ----------------------------------------------------------------------
    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = self.config.mopd.objective_weights.copy()
        health_summary = self.harvester.pigments.get_pigment_health_summary()
        avg_health = np.mean(list(health_summary.values())) if health_summary else 1.0
        if avg_health < 0.5:
            weights['pigment_health'] = min(0.6, weights['pigment_health'] * 1.5)
        total = sum(weights.values())
        return {k: v / total for k, v in weights.items()}

    # ----------------------------------------------------------------------
    # Main evolve (NSGA-II)
    # ----------------------------------------------------------------------
    async def evolve(self, generations: Optional[int] = None) -> Dict:
        async with self._lock:
            if generations is None:
                generations = self.generations

            population = self._initialize_population()
            objectives = {}
            eval_tasks = [self._evaluate_individual_mo(ind) for ind in population]
            eval_results = await asyncio.gather(*eval_tasks)
            for ind, objs in zip(population, eval_results):
                objectives[self._individual_to_cache_key(ind)] = objs

            self.pareto_front = []

            for gen in range(generations):
                # Create offspring
                offspring = []
                pop_objectives = {k: objectives[k] for k in objectives if k in [self._individual_to_cache_key(i) for i in population]}
                fronts = self._fast_non_dominated_sort(population, pop_objectives)
                crowding = {}
                for front in fronts:
                    front_crowding = self._crowding_distance(front, pop_objectives)
                    crowding.update(front_crowding)

                while len(offspring) < self.population_size:
                    parent1 = self._tournament_selection(population, fronts, crowding)
                    parent2 = self._tournament_selection(population, fronts, crowding)
                    if random.random() < self.crossover_rate:
                        child1, child2 = self._sbx_crossover(parent1, parent2)
                        child1 = self._polynomial_mutation(child1)
                        child2 = self._polynomial_mutation(child2)
                        offspring.extend([child1, child2])
                    else:
                        offspring.append(self._polynomial_mutation(parent1.copy()))
                offspring = offspring[:self.population_size]

                eval_tasks = [self._evaluate_individual_mo(ind) for ind in offspring]
                eval_results = await asyncio.gather(*eval_tasks)
                for ind, objs in zip(offspring, eval_results):
                    objectives[self._individual_to_cache_key(ind)] = objs

                combined = population + offspring
                unique_keys = {}
                for ind in combined:
                    unique_keys[self._individual_to_cache_key(ind)] = ind
                combined = list(unique_keys.values())

                combined_objectives = {self._individual_to_cache_key(ind): objectives[self._individual_to_cache_key(ind)] for ind in combined}
                fronts = self._fast_non_dominated_sort(combined, combined_objectives)

                new_population = []
                for front in fronts:
                    if len(new_population) + len(front) <= self.population_size:
                        new_population.extend(front)
                    else:
                        crowding = self._crowding_distance(front, combined_objectives)
                        sorted_front = sorted(front, key=lambda ind: crowding.get(self._individual_to_cache_key(ind), 0), reverse=True)
                        remaining = self.population_size - len(new_population)
                        new_population.extend(sorted_front[:remaining])
                        break
                population = new_population

                # Update Pareto front
                pop_objectives = {self._individual_to_cache_key(ind): objectives[self._individual_to_cache_key(ind)] for ind in population}
                fronts_pop = self._fast_non_dominated_sort(population, pop_objectives)
                if fronts_pop:
                    pareto_individuals = fronts_pop[0]
                    self.pareto_front = []
                    for ind in pareto_individuals:
                        objs = pop_objectives[self._individual_to_cache_key(ind)]
                        self.pareto_front.append(MOPDPoint(
                            individual=ind,
                            energy_output=objs['energy_output'],
                            pigment_health=objs['pigment_health'],
                            longterm_efficiency=objs['longterm_efficiency'],
                            resource_usage=objs['resource_usage']
                        ))
                logger.debug(f"Generation {gen+1}/{generations}: Pareto front size={len(self.pareto_front)}")

            # Select final best using dynamic weights
            weights = self._compute_dynamic_weights()
            if self.config.mopd.enabled and self.pareto_front:
                best_point = self._select_best_from_pareto(self.pareto_front, weights)
                if best_point:
                    self.best_individual = best_point.individual
                    self.best_fitness = best_point.scalarised_score
                    await self._apply_individual(best_point.individual)
                    logger.info(f"Applied best MOPD individual with scalarised score {self.best_fitness:.4f}")
            else:
                pop_objectives = {self._individual_to_cache_key(ind): objectives[self._individual_to_cache_key(ind)] for ind in population}
                best_ind = max(population, key=lambda ind: sum(weights[k] * pop_objectives[self._individual_to_cache_key(ind)][k] for k in weights))
                best_obj = pop_objectives[self._individual_to_cache_key(best_ind)]
                self.best_individual = best_ind
                self.best_fitness = sum(weights[k] * best_obj[k] for k in weights)
                await self._apply_individual(best_ind)
                logger.info(f"Applied best individual with scalarised fitness {self.best_fitness:.4f}")

            self.evolution_history.append({
                'timestamp': datetime.now(timezone.utc),
                'best_fitness': self.best_fitness,
                'pareto_front_size': len(self.pareto_front) if self.config.mopd.enabled else 0,
                'dynamic_weights': weights,
                'generation_count': generations
            })
            if self.harvester.config.persistence.enable:
                state = {
                    'best_individual': self.best_individual,
                    'best_fitness': self.best_fitness,
                    'pareto_front': [p.to_dict() for p in self.pareto_front] if self.config.mopd.enabled else []
                }
                await self.harvester.persistence.save_global_state('genetic_optimizer', state)
            return {
                'best_fitness': self.best_fitness,
                'best_individual': self.best_individual,
                'pareto_front': [p.to_dict() for p in self.pareto_front] if self.config.mopd.enabled else None,
                'dynamic_weights': weights
            }

    def _select_best_from_pareto(self, pareto_front: List[MOPDPoint], weights: Optional[Dict[str, float]] = None) -> Optional[MOPDPoint]:
        if not pareto_front:
            return None
        if weights is None:
            weights = self.config.mopd.objective_weights
        obj_keys = list(weights.keys())

        max_vals = {k: max(getattr(p, k) for p in pareto_front) for k in obj_keys}
        min_vals = {k: min(getattr(p, k) for p in pareto_front) for k in obj_keys}
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in obj_keys}

        best = None
        best_score = -float('inf')
        for point in pareto_front:
            score = 0.0
            for key in obj_keys:
                val = getattr(point, key)
                norm = (val - min_vals[key]) / ranges[key] if ranges[key] > 0 else 1.0
                score += weights.get(key, 0.0) * norm
            point.scalarised_score = score
            if score > best_score:
                best_score = score
                best = point
        return best

    def get_status(self) -> Dict[str, Any]:
        return {
            'best_fitness': self.best_fitness,
            'best_individual': self.best_individual,
            'evolution_history': self.evolution_history[-10:],
            'pareto_front_size': len(self.pareto_front) if self.config.mopd.enabled else 0,
            'cache_size': len(self._eval_cache)
        }

    async def _apply_individual(self, individual: Dict):
        async with self.harvester._state_lock:
            pigments = self.harvester.pigments.pigments
            for p in pigments:
                pigments[p]['energy_conversion_factor'] = individual['conversion_factors'][p]
                pigments[p]['sensitivity'] = individual['sensitivity_multipliers'][p] * pigments[p]['base_sensitivity']
                self.harvester.pigments.pigment_health[p].recovery_rate = individual['repair_rates'][p]
            self.harvester.config.reaction_center.demand_response_factor = individual['demand_response_factor']

# ============================================================================
# Competition Engine
# ============================================================================
class ChildHarvesterCompetition:
    def __init__(self, parent: 'EnhancedPhotosyntheticHarvester', config: HarvesterConfig):
        self.parent = parent
        self.config = config
        self.competition_interval = config.child.competition_interval
        self.replacement_threshold = config.child.replacement_threshold
        self.performance_window = config.child.performance_window
        self._lock = asyncio.Lock()
        logger.info("Child Harvester Competition initialized")

    async def run_competition(self):
        async with self._lock:
            children = list(self.parent.child_harvesters.values())
            if len(children) < 2:
                return
            performance = {}
            for child in children:
                stats = child.get_harvesting_stats()
                cycles = stats.get('harvest_cycles', 0)
                total = stats.get('total_harvested', 0)
                avg = total / max(cycles, 1)
                performance[child.harvester_id] = avg
            if not performance:
                return
            sorted_perf = sorted(performance.items(), key=lambda x: x[1])
            bottom_count = max(1, int(len(sorted_perf) * self.replacement_threshold))
            bottom = [cid for cid, _ in sorted_perf[:bottom_count]]
            top = [cid for cid, _ in sorted_perf[-bottom_count:]]
            if not top:
                return
            diversity_pool = []
            for child_id, child in self.parent.child_harvesters.items():
                if child_id not in bottom:
                    diversity_pool.append(child_id)
            if diversity_pool and len(bottom) == len(children):
                keep_id = random.choice(bottom)
                bottom = [bid for bid in bottom if bid != keep_id]
            for child_id in bottom:
                top_id = random.choice(top)
                top_child = self.parent.child_harvesters.get(top_id)
                if not top_child:
                    continue
                new_child = self.parent.spawn_child_with_config(top_child)
                if new_child:
                    for pigment_name, config in new_child.pigments.pigments.items():
                        if random.random() < 0.3:
                            config['sensitivity'] = config['base_sensitivity'] * random.uniform(0.8, 1.2)
                            new_child.pigments.pigment_health[pigment_name].recovery_rate *= random.uniform(0.9, 1.1)
                    self.parent.remove_child(child_id)
                    self.parent.child_harvesters[new_child.harvester_id] = new_child
                    logger.info("Replaced child", old=child_id, new=new_child.harvester_id)

    def get_stats(self) -> Dict[str, Any]:
        return {
            'competition_interval': self.competition_interval,
            'replacement_threshold': self.replacement_threshold,
            'performance_window': self.performance_window
        }

# ============================================================================
# Swarm Coordinator
# ============================================================================
class SwarmCoordinator:
    def __init__(self, parent: 'EnhancedPhotosyntheticHarvester', config: HarvesterConfig):
        self.parent = parent
        self.config = config
        self.shared_predictions: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self.redis_client = None
        self.pubsub = None
        self.channel = f"harvester_swarm_{self.parent.harvester_id}"
        if REDIS_AVAILABLE:
            try:
                redis_url = config.swarm.redis_url or "redis://localhost:6379"
                self.redis_client = redis.from_url(redis_url)
                self.pubsub = self.redis_client.pubsub()
                asyncio.create_task(self._listen())
            except:
                self.redis_client = None
        logger.info("Swarm Coordinator initialized")

    async def _listen(self):
        if not self.pubsub:
            return
        await self.pubsub.subscribe(self.channel)
        async for message in self.pubsub.listen():
            if message['type'] == 'message':
                try:
                    data = json.loads(message['data'])
                    async with self._lock:
                        for harvester_id, preds in data.items():
                            self.shared_predictions[harvester_id] = preds
                except Exception as e:
                    logger.error("Failed to process swarm message", error=str(e))

    async def share_predictions(self):
        async with self._lock:
            all_preds = {}
            parent_preds = await self.parent.pigments.get_predictions()
            all_preds[self.parent.harvester_id] = parent_preds
            for child_id, child in self.parent.child_harvesters.items():
                child_preds = await child.pigments.get_predictions()
                all_preds[child_id] = child_preds
            self.shared_predictions = all_preds
            high_count = 0
            total = 0
            for preds in all_preds.values():
                for p in preds.values():
                    total += 1
                    if p.get('medium_term_300s', 0) > 0.7:
                        high_count += 1
            if total > 0:
                ratio = high_count / total
                if ratio > 0.5:
                    self.parent.set_mode(HarvestingMode.FULL)
                elif ratio < 0.2:
                    self.parent.set_mode(HarvestingMode.CONSERVATIVE)
                else:
                    self.parent.set_mode(HarvestingMode.MODULATED)
            if self.redis_client:
                await self.redis_client.publish(self.channel, json.dumps(all_preds))

    def get_shared_predictions(self) -> Dict[str, Dict[str, Any]]:
        return self.shared_predictions.copy()

# ============================================================================
# Enhanced Photosynthetic Harvester (Main Class)
# ============================================================================
class EnhancedPhotosyntheticHarvester:
    def __init__(self, config: Optional[HarvesterConfig] = None,
                 token_manager: Optional[Any] = None,
                 gradient_manager: Optional[Any] = None):
        self.config = config or HarvesterConfig()
        self.harvester_id = self.config.harvester_id
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager

        # Event bus
        self.event_bus = EventBus()

        # Central task manager with event bus
        self._task_manager = TaskManager(event_bus=self.event_bus)

        # Sub-modules with dependency injection (interfaces)
        self.pigments = EnhancedPigmentArray(self.config, self._task_manager, self.event_bus)
        self.reaction_center = EnhancedReactionCenter(self.config, self._task_manager,
                                                     token_manager, gradient_manager, self.event_bus)
        self.health_monitor = HealthMonitor(self.config, self.harvester_id, self.event_bus)
        self.self_healer = SelfHealer(self, self.config, self.event_bus)
        self.persistence = PersistentHarvesterState(self.harvester_id, self.config)
        self.websocket_server = None
        if self.config.websocket.enable and WEBSOCKET_AVAILABLE:
            self.websocket_server = HarvesterWebSocketServer(self.config)
            self.websocket_server._parent_harvester = self
            self._task_manager.start_task("websocket_server", self.websocket_server.start)
            self._task_manager.start_task("websocket_broadcast", self._websocket_broadcast_loop)

        # Harvesting state
        self.mode = HarvestingMode.FULL
        self.total_harvested = 0.0
        self.harvesting_efficiency = 0.0
        self.peak_harvest_rate = 0.0
        self.harvest_cycles = 0
        self.account_id = f"photosynthetic_{self.harvester_id}"
        if token_manager:
            token_manager.create_account(self.account_id)
        self.predicted_peaks: Dict[str, datetime] = {}
        self.child_harvesters: Dict[str, 'EnhancedPhotosyntheticHarvester'] = {}
        self.is_child = self.harvester_id != "primary"
        self.performance_metrics = {
            'start_time': datetime.now(timezone.utc),
            'uptime': 0.0,
            'harvest_rate_avg': 0.0,
            'harvest_rate_peak': 0.0,
            'successful_cycles': 0,
            'failed_cycles': 0
        }

        # New components
        self.genetic_optimizer = HarvesterGeneticOptimizer(self, self.config)
        self.competition_engine = ChildHarvesterCompetition(self, self.config)
        self.swarm_coordinator = SwarmCoordinator(self, self.config)

        # Locks
        self._state_lock = asyncio.Lock()
        self._child_lock = asyncio.Lock()
        self._prediction_lock = asyncio.Lock()

        # Register and start background loops
        self._register_tasks()
        self._task_manager.start_registered_tasks()

        # Restore state
        if self.config.persistence.enable:
            asyncio.create_task(self._restore_state())

        logger.info("Enhanced Photosynthetic Harvester initialized", id=self.harvester_id)

    def _register_tasks(self):
        self._task_manager.register_task("predictive_window", self._predictive_window_loop)
        self._task_manager.register_task("metrics", self._metrics_loop)
        self._task_manager.register_task("genetic_evolution", self._genetic_evolution_loop)
        self._task_manager.register_task("competition", self._competition_loop)
        self._task_manager.register_task("swarm_coordination", self._swarm_coordination_loop)
        self._task_manager.register_task("checkpoint", self._checkpoint_loop)

    async def _predictive_window_loop(self):
        while True:
            try:
                predictions = await self.pigments.get_predictions()
                for pigment, pred in predictions.items():
                    if pred.get('medium_term_300s', 0) > 0.7:
                        peak_time = datetime.now(timezone.utc) + timedelta(seconds=300)
                        self.predicted_peaks[pigment] = peak_time
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Predictive window loop error", error=str(e))
                await asyncio.sleep(60)

    async def _metrics_loop(self):
        while True:
            try:
                stats = await self.get_harvesting_stats()
                self.health_monitor.collect_metrics(stats)
                recs = self.health_monitor.get_recommendations()
                for rec in recs:
                    if rec['severity'] == 'high':
                        issue_type = rec['type']
                        await self.self_healer.apply_healing(issue_type)
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Metrics loop error", error=str(e))
                await asyncio.sleep(30)

    async def _genetic_evolution_loop(self):
        while True:
            try:
                if self.harvest_cycles > 50 and not self.is_child:
                    logger.info("Starting genetic evolution...")
                    result = await self.genetic_optimizer.evolve(generations=self.config.genetic.generations)
                    if self.config.mopd.enabled:
                        logger.info("Evolution complete", 
                                    best_fitness=result['best_fitness'],
                                    pareto_front_size=len(result.get('pareto_front', [])),
                                    dynamic_weights=result.get('dynamic_weights', {}))
                    else:
                        logger.info("Evolution complete", best_fitness=result['best_fitness'])
                await asyncio.sleep(self.config.genetic.evolution_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Genetic evolution loop error", error=str(e))
                await asyncio.sleep(3600)

    async def _competition_loop(self):
        while True:
            try:
                if not self.is_child and len(self.child_harvesters) >= 2:
                    await self.competition_engine.run_competition()
                await asyncio.sleep(self.config.child.competition_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Competition loop error", error=str(e))
                await asyncio.sleep(300)

    async def _swarm_coordination_loop(self):
        while True:
            try:
                await self.swarm_coordinator.share_predictions()
                await asyncio.sleep(self.config.swarm.update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Swarm coordination error", error=str(e))
                await asyncio.sleep(300)

    async def _checkpoint_loop(self):
        while True:
            try:
                if self.config.persistence.enable:
                    await self._checkpoint()
                    if random.random() < 0.01:
                        await self.persistence.delete_old_checkpoints(self.config.persistence.retention_days)
                await asyncio.sleep(self.config.persistence.checkpoint_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Checkpoint loop error", error=str(e))
                await asyncio.sleep(300)

    async def _websocket_broadcast_loop(self):
        if not self.websocket_server:
            return
        while self.websocket_server.is_running:
            try:
                stats = await self.get_harvesting_stats()
                await self.websocket_server.broadcast(stats)
                await asyncio.sleep(self.websocket_server.stream_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("WebSocket broadcast error", error=str(e))
                await asyncio.sleep(5)

    async def _restore_state(self):
        if not self.config.persistence.enable:
            return
        state = await self.persistence.load_state()
        if state:
            async with self._state_lock:
                self.total_harvested = state.get('total_harvested', 0)
                self.harvest_cycles = state.get('harvest_cycles', 0)
                self.mode = HarvestingMode(state.get('mode', 'full'))
                pigment_health = state.get('pigment_health', {})
                for name, health_data in pigment_health.items():
                    if name in self.pigments.pigment_health:
                        self.pigments.pigment_health[name].health = health_data.get('health', 1.0)
                        self.pigments.pigment_health[name].damage = health_data.get('damage', 0.0)
                rc_state = state.get('reaction_center', {})
                self.reaction_center.cumulative_damage = rc_state.get('cumulative_damage', 0.0)
                self.reaction_center.current_efficiency = rc_state.get('current_efficiency', self.config.reaction_center.base_quantum_efficiency)
                self.peak_harvest_rate = state.get('peak_harvest_rate', 0.0)
                self.harvesting_efficiency = state.get('harvesting_efficiency', 0.0)
            logger.info("State restored", id=self.harvester_id)
        else:
            logger.info("No previous state found")

    async def _checkpoint(self):
        if not self.config.persistence.enable:
            return
        async with self._state_lock:
            state = {
                'harvester_id': self.harvester_id,
                'total_harvested': self.total_harvested,
                'harvest_cycles': self.harvest_cycles,
                'mode': self.mode.value,
                'peak_harvest_rate': self.peak_harvest_rate,
                'harvesting_efficiency': self.harvesting_efficiency,
                'pigment_health': {name: {'health': h.health, 'damage': h.damage}
                                   for name, h in self.pigments.pigment_health.items()},
                'reaction_center': {
                    'cumulative_damage': self.reaction_center.cumulative_damage,
                    'current_efficiency': self.reaction_center.current_efficiency
                },
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        await self.persistence.save_checkpoint(state)
        await self.persistence.save_state(state)

    async def harvest_cycle(self, environmental_data: Dict[str, float]) -> Dict[str, Any]:
        try:
            trace = TraceContext()
            cycle_logger = trace.get_logger(logger)
            cycle_logger.info("Starting harvest cycle", trace_id=trace.trace_id)

            excitations = await self.pigments.sense_environment(environmental_data)
            rc_result = await self.reaction_center.harvest_cycle(excitations)
            eco_atp = rc_result['eco_atp_generated']

            async with self._state_lock:
                self.total_harvested += eco_atp
                self.harvest_cycles += 1
                self.harvesting_efficiency = rc_result['efficiency']
                if eco_atp > self.peak_harvest_rate:
                    self.peak_harvest_rate = eco_atp
                self.performance_metrics['harvest_rate_avg'] = self.total_harvested / max(self.harvest_cycles, 1)
                self.performance_metrics['harvest_rate_peak'] = max(self.performance_metrics['harvest_rate_peak'],
                                                                    eco_atp)
                self.performance_metrics['successful_cycles'] += 1
                self.performance_metrics['uptime'] = (datetime.now(timezone.utc) - self.performance_metrics['start_time']).total_seconds()

            self.genetic_optimizer.recent_data.append(environmental_data.copy())

            stats = await self.get_harvesting_stats()
            self.health_monitor.collect_metrics(stats)
            recs = self.health_monitor.get_recommendations()
            for rec in recs:
                if rec['severity'] == 'high':
                    issue_type = rec['type']
                    await self.self_healer.apply_healing(issue_type)

            cycle_logger.info("Harvest cycle complete", eco_atp=eco_atp, efficiency=rc_result['efficiency'])
            return {
                'eco_atp_generated': eco_atp,
                'total_harvested': self.total_harvested,
                'dominant_signal': max(excitations, key=excitations.get),
                'recent_conversions': list(self.reaction_center.conversion_history)[-10:],
                'efficiency': rc_result['efficiency'],
                'mode': self.mode.value
            }
        except Exception as e:
            async with self._state_lock:
                self.performance_metrics['failed_cycles'] += 1
            logger.error("Harvest cycle failed", error=str(e), exc_info=True)
            raise

    async def spawn_child(self, specialization: str) -> Optional['EnhancedPhotosyntheticHarvester']:
        async with self._child_lock:
            if len(self.child_harvesters) >= self.config.child.max_children:
                logger.warning("Max children reached")
                return None
            child_id = f"{self.harvester_id}_child_{specialization}_{uuid.uuid4().hex[:8]}"
            child_config = self.config.copy(deep=True)
            child_config.harvester_id = child_id
            child_config.websocket.enable = False
            child = EnhancedPhotosyntheticHarvester(
                config=child_config,
                token_manager=self.token_manager,
                gradient_manager=self.gradient_manager
            )
            child.is_child = True
            for pigment_name, pigment_config in child.pigments.pigments.items():
                if pigment_config['specialization'] == specialization:
                    pigment_config['sensitivity'] *= 1.5
                else:
                    pigment_config['sensitivity'] *= 0.3
            self.child_harvesters[child_id] = child
            logger.info("Spawned child harvester", id=child_id, specialization=specialization)
            return child

    async def spawn_child_with_config(self, template: 'EnhancedPhotosyntheticHarvester') -> Optional['EnhancedPhotosyntheticHarvester']:
        async with self._child_lock:
            if len(self.child_harvesters) >= self.config.child.max_children:
                logger.warning("Max children reached")
                return None
            child_id = f"{self.harvester_id}_child_clone_{uuid.uuid4().hex[:8]}"
            child_config = template.config.copy(deep=True)
            child_config.harvester_id = child_id
            child_config.websocket.enable = False
            child = EnhancedPhotosyntheticHarvester(
                config=child_config,
                token_manager=self.token_manager,
                gradient_manager=self.gradient_manager
            )
            child.is_child = True
            for pigment_name in child.pigments.pigments:
                child.pigments.pigments[pigment_name]['sensitivity'] = template.pigments.pigments[pigment_name]['sensitivity']
                child.pigments.pigment_health[pigment_name].health = template.pigments.pigment_health[pigment_name].health
                child.pigments.pigment_health[pigment_name].damage = template.pigments.pigment_health[pigment_name].damage
            self.child_harvesters[child_id] = child
            logger.info("Spawned child from template", id=child_id)
            return child

    async def remove_child(self, child_id: str):
        async with self._child_lock:
            if child_id in self.child_harvesters:
                asyncio.create_task(self.child_harvesters[child_id].shutdown())
                del self.child_harvesters[child_id]
                logger.info("Removed child harvester", id=child_id)

    def set_mode(self, mode: HarvestingMode):
        async with self._state_lock:
            self.mode = mode
            logger.info("Mode changed", mode=mode.value)

    async def shutdown(self):
        logger.info("Shutting down harvester", id=self.harvester_id)
        await self._task_manager.stop_all()
        if self.websocket_server:
            await self.websocket_server.stop()
        async with self._child_lock:
            for child in self.child_harvesters.values():
                await child.shutdown()
            self.child_harvesters.clear()
        if self.config.persistence.enable:
            await self._checkpoint()
        logger.info("Harvester shutdown complete")

    async def get_harvesting_stats(self) -> Dict[str, Any]:
        async with self._state_lock:
            stats = {
                'harvester_id': self.harvester_id,
                'total_harvested': self.total_harvested,
                'harvest_cycles': self.harvest_cycles,
                'peak_harvest_rate': self.peak_harvest_rate,
                'mode': self.mode.value,
                'efficiency': self.reaction_center.current_efficiency,
                'account_balance': (self.token_manager.get_account_summary(self.account_id).get('balance', 0)
                                    if self.token_manager else 0),
                'pigment_health': self.pigments.get_pigment_health_summary(),
                'circadian': self.pigments.get_circadian_summary(),
                'predictions': await self.pigments.get_predictions(),
                'reaction_center': self.reaction_center.get_efficiency_stats(),
                'predicted_peaks': {k: v.isoformat() for k, v in self.predicted_peaks.items()},
                'child_harvesters': len(self.child_harvesters),
                'is_child': self.is_child,
                'performance_metrics': self.performance_metrics,
                'health_metrics': self.health_monitor.get_metrics(),
                'genetic_optimizer': self.genetic_optimizer.get_status(),
                'competition': self.competition_engine.get_stats(),
                'swarm': self.swarm_coordinator.get_shared_predictions(),
                'mopd_enabled': self.config.mopd.enabled,
                'pareto_front_size': len(self.genetic_optimizer.pareto_front) if self.config.mopd.enabled else 0
            }
            return stats

# ============================================================================
# Helper functions
# ============================================================================
def create_harvester(config: Union[Dict, HarvesterConfig] = None) -> EnhancedPhotosyntheticHarvester:
    if isinstance(config, dict):
        if PYDANTIC_AVAILABLE:
            config = HarvesterConfig(**config)
        else:
            config = HarvesterConfig(**config)
    return EnhancedPhotosyntheticHarvester(config=config)

async def example_usage():
    logging.basicConfig(level=logging.INFO)
    config = HarvesterConfig(persistence=HarvesterConfig.persistence_class(enable=False))
    harvester = EnhancedPhotosyntheticHarvester(config=config)
    env_data = {'renewable_availability': 0.8, 'carbon_intensity': 200, 'waste_heat': 0.3, 'edge_availability': 0.6, 'system_overload': 0.1}
    for _ in range(10):
        result = await harvester.harvest_cycle(env_data)
        print(f"Cycle: generated {result['eco_atp_generated']:.2f} Eco-ATP")
        await asyncio.sleep(1)
    stats = await harvester.get_harvesting_stats()
    print(f"Total harvested: {stats['total_harvested']:.2f}")
    await harvester.shutdown()

if __name__ == "__main__":
    asyncio.run(example_usage())
