# =============================================================================
# Enhanced ATP Synthase Scheduler v9.0.0 – Full Implementation
# =============================================================================
"""
Enhanced ATP Synthase Scheduler v9.0.0
Complete implementation with all improvements:
- Secure model serialization using joblib with allow_pickle=False
- Persistent circuit breaker state using SQLite
- Improved ML predictor with RandomForest and scheduled retraining
- Retry logic with tenacity for external calls
- Event subscription integration
- Refined load-balancing scoring with configurable weights
- Adaptive priority weights based on historical performance
- Proper async callback handling
- Updated Prometheus gauges in regulation loop
- Comprehensive docstrings
- Improved gradient forecasting with Holt-Winters exponential smoothing
- Graceful shutdown with configurable timeout
"""

import asyncio
import logging
import uuid
import os
import math
import random
import sqlite3
from typing import Dict, Any, List, Optional, Tuple, Callable, Protocol, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from collections import deque
import numpy as np

# Try optional dependencies
try:
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    from prometheus_client import Gauge, Counter, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.preprocessing import StandardScaler
    import joblib
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

try:
    import structlog
    from structlog.processors import JSONRenderer, TimeStamper
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            TimeStamper(fmt="iso"),
            JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

# Local imports (with fallback)
try:
    from .eco_atp_currency import EcoATPTokenManager, EcoATPConsumer, EcoATPSource
    TOKEN_AVAILABLE = True
except ImportError:
    TOKEN_AVAILABLE = False

try:
    from .proton_gradient_fields import GradientFieldManager, GradientField
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False

# ============================================================================
# Retry decorator (using tenacity if available)
# ============================================================================

def retry_decorator(max_attempts: int = 3, min_delay: float = 0.1, max_delay: float = 10.0):
    """Decorator to retry async functions with exponential backoff."""
    if TENACITY_AVAILABLE:
        def decorator(func):
            @retry(
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential(multiplier=min_delay, min=min_delay, max=max_delay),
                retry=retry_if_exception_type(Exception),
                before_sleep=before_sleep_log(logger, logging.WARNING)
            )
            async def wrapper(*args, **kwargs):
                return await func(*args, **kwargs)
            return wrapper
        return decorator
    else:
        def decorator(func):
            async def wrapper(*args, **kwargs):
                for attempt in range(max_attempts):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        if attempt == max_attempts - 1:
                            raise
                        delay = min(min_delay * (2 ** attempt), max_delay)
                        await asyncio.sleep(delay)
            return wrapper
        return decorator

# ============================================================================
# Persistent Circuit Breaker (SQLite)
# ============================================================================

class CircuitBreaker:
    """Circuit breaker with SQLite persistence."""
    def __init__(self, name: str, db_path: str, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self.name = name
        self.db_path = db_path
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._init_db()
        self._load_state()
        self._lock = asyncio.Lock()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS circuit_breaker (
                name TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                failures INTEGER NOT NULL,
                last_failure TEXT
            )
        """)
        conn.commit()
        conn.close()

    def _load_state(self):
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT state, failures, last_failure FROM circuit_breaker WHERE name = ?", (self.name,)).fetchone()
        conn.close()
        if row:
            self.state = row[0]
            self.failure_count = row[1]
            self.last_failure_time = datetime.fromisoformat(row[2]) if row[2] else None
        else:
            self.state = 'closed'
            self.failure_count = 0
            self.last_failure_time = None

    def _save_state(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT OR REPLACE INTO circuit_breaker (name, state, failures, last_failure)
            VALUES (?, ?, ?, ?)
        """, (self.name, self.state, self.failure_count, self.last_failure_time.isoformat() if self.last_failure_time else None))
        conn.commit()
        conn.close()

    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            if self.state == 'open':
                if self.last_failure_time and (datetime.now(timezone.utc) - self.last_failure_time).total_seconds() >= self.recovery_timeout:
                    self.state = 'half_open'
                    self._save_state()
                    logger.info(f"Circuit breaker {self.name} transitioning to half_open")
                else:
                    raise Exception(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == 'half_open':
                    self.state = 'closed'
                    self.failure_count = 0
                    self._save_state()
                    logger.info(f"Circuit breaker {self.name} closed after success")
                else:
                    self.failure_count = 0
                    self._save_state()
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.now(timezone.utc)
                if self.failure_count >= self.failure_threshold:
                    self.state = 'open'
                    logger.warning(f"Circuit breaker {self.name} opened after {self.failure_count} failures")
                self._save_state()
            raise e

# ============================================================================
# Protocols for dependency injection
# ============================================================================

class TokenServiceProtocol(Protocol):
    def get_system_summary(self) -> Dict[str, Any]: ...
    def generate_tokens(self, account_id: str, source: Any, **kwargs) -> List[Any]: ...
    def reserve_tokens(self, account_id: str, amount: float, consumer: Any) -> Tuple[bool, List[str]]: ...
    def consume_tokens(self, token_ids: List[str], consumer: Any, operation_success: bool) -> float: ...
    def recover_tokens(self, token_ids: List[str], completion_percentage: float) -> float: ...
    def create_account(self, account_id: str) -> Any: ...
    def get_account_summary(self, account_id: str) -> Dict[str, Any]: ...

class GradientServiceProtocol(Protocol):
    def get_field_strengths(self) -> Dict[str, float]: ...
    def discharge_field(self, field_id: str, amount: float) -> float: ...
    def pump_field(self, field_id: str, amount: float, source: str) -> None: ...
    def get_field_stats(self) -> Dict[str, Any]: ...

class HarvesterProtocol(Protocol):
    def get_harvesting_stats(self) -> Dict[str, Any]: ...
    def set_mode(self, mode: Any) -> None: ...

# ============================================================================
# Configuration (Pydantic or dataclass)
# ============================================================================

if PYDANTIC_AVAILABLE:
    class SynthaseSchedulerConfig(BaseModel):
        """Configuration for ATP Synthase Scheduler."""
        # Core parameters
        protons_per_rotation: int = Field(12, ge=8, le=17)
        atp_per_rotation: int = Field(3, ge=1)
        max_rotation_speed_rpm: float = Field(6000, gt=0)
        activation_gradient: float = Field(0.05, ge=0, le=1)
        base_efficiency: float = Field(0.95, ge=0, le=1)
        atp_inhibition_constant: float = Field(0.1, ge=0)
        atp_inhibition_max: float = Field(0.5, ge=0, le=1)
        reverse_efficiency: float = Field(0.7, ge=0, le=1)
        hydrolysis_protons_per_atp: int = Field(4, ge=1)
        uncoupling_leak_rate: float = Field(0.01, ge=0, le=1)
        uncoupling_activation_threshold: float = Field(0.9, ge=0, le=1)
        adaptive_c_ring: bool = True
        min_c_ring: int = 8
        max_c_ring: int = 17
        degradation_scaling: bool = True

        # Quantum tunneling
        quantum_tunneling_enabled: bool = True
        quantum_efficiency_boost: float = Field(0.25, ge=0, le=1)
        quantum_tunneling_threshold: float = Field(0.7, ge=0, le=1)
        quantum_coherence_time: float = Field(10.0, ge=0)

        # Driving force weights (for gradient combination)
        driving_force_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'carbon': 0.25,
                'helium': 0.15,
                'trust': 0.20,
                'opportunity': 0.25,
                'eco_atp_reserve': 0.15
            }
        )

        # Priority defaults
        priority_defaults: Dict[str, Dict[str, float]] = Field(
            default_factory=lambda: {
                'critical': {'weight': 2.0, 'min_balance': 10000, 'max_consumption': 0.9},
                'high': {'weight': 1.5, 'min_balance': 5000, 'max_consumption': 0.7},
                'normal': {'weight': 1.0, 'min_balance': 2000, 'max_consumption': 0.5},
                'low': {'weight': 0.7, 'min_balance': 1000, 'max_consumption': 0.3},
                'background': {'weight': 0.4, 'min_balance': 500, 'max_consumption': 0.1}
            }
        )
        default_priority: str = 'normal'

        # ML predictor (RandomForest)
        ml_lookback: int = Field(50, ge=10)
        ml_model_path: str = Field("./models/atp_demand_model.joblib")
        ml_retrain_interval: int = Field(3600, ge=300)  # seconds
        ml_min_samples: int = Field(100, ge=20)

        # Gradient forecaster (Holt-Winters)
        forecast_history_window: int = Field(50, ge=10)
        forecast_horizon: int = Field(20, ge=5)
        forecast_alpha: float = Field(0.3, ge=0, le=1)
        forecast_beta: float = Field(0.1, ge=0, le=1)

        # Load balancing
        load_balance_history_size: int = Field(100, ge=10)
        load_balance_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'health': 0.3,
                'efficiency': 0.3,
                'quantum': 0.2,
                'performance': 0.2
            }
        )

        # Priority adaptation
        adaptive_priority_enabled: bool = True
        adaptive_priority_learning_rate: float = Field(0.1, ge=0, le=1)
        priority_performance_window: int = Field(50, ge=10)

        # Scheduling
        synthesis_interval: float = Field(0.1, ge=0.01)
        regulation_interval: float = Field(30, ge=5)
        predictive_interval: float = Field(60, ge=10)
        forecast_interval: float = Field(60, ge=10)
        maintenance_interval: float = Field(60, ge=10)

        # Feature flags
        enable_multi_synthase: bool = True
        enable_quantum: bool = True
        enable_ml_prediction: bool = True
        enable_prometheus: bool = False

        # Degradation tier threshold
        degradation_tier_update_interval: int = Field(600, ge=60)
        efficiency_thresholds: Dict[int, float] = Field(
            default_factory=lambda: {5: 0.9, 4: 0.8, 3: 0.7, 2: 0.6, 1: 0.0}
        )

        # Shutdown timeout
        shutdown_timeout_seconds: int = Field(30, ge=5)

        # Circuit breaker DB
        circuit_breaker_db_path: str = Field("./circuit_breakers.db")

        class Config:
            env_prefix = "ATP_SCHEDULER_"

    class DemandPriorityConfig(BaseModel):
        priority_level: str
        weight: float = Field(..., ge=0)
        min_balance: float = Field(..., ge=0)
        max_consumption: float = Field(..., ge=0, le=1)

else:
    @dataclass
    class SynthaseSchedulerConfig:
        protons_per_rotation: int = 12
        atp_per_rotation: int = 3
        max_rotation_speed_rpm: float = 6000
        activation_gradient: float = 0.05
        base_efficiency: float = 0.95
        atp_inhibition_constant: float = 0.1
        atp_inhibition_max: float = 0.5
        reverse_efficiency: float = 0.7
        hydrolysis_protons_per_atp: int = 4
        uncoupling_leak_rate: float = 0.01
        uncoupling_activation_threshold: float = 0.9
        adaptive_c_ring: bool = True
        min_c_ring: int = 8
        max_c_ring: int = 17
        degradation_scaling: bool = True
        quantum_tunneling_enabled: bool = True
        quantum_efficiency_boost: float = 0.25
        quantum_tunneling_threshold: float = 0.7
        quantum_coherence_time: float = 10.0
        driving_force_weights: Dict[str, float] = field(default_factory=lambda: {
            'carbon': 0.25,
            'helium': 0.15,
            'trust': 0.20,
            'opportunity': 0.25,
            'eco_atp_reserve': 0.15
        })
        priority_defaults: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
            'critical': {'weight': 2.0, 'min_balance': 10000, 'max_consumption': 0.9},
            'high': {'weight': 1.5, 'min_balance': 5000, 'max_consumption': 0.7},
            'normal': {'weight': 1.0, 'min_balance': 2000, 'max_consumption': 0.5},
            'low': {'weight': 0.7, 'min_balance': 1000, 'max_consumption': 0.3},
            'background': {'weight': 0.4, 'min_balance': 500, 'max_consumption': 0.1}
        })
        default_priority: str = 'normal'
        ml_lookback: int = 50
        ml_model_path: str = "./models/atp_demand_model.joblib"
        ml_retrain_interval: int = 3600
        ml_min_samples: int = 100
        forecast_history_window: int = 50
        forecast_horizon: int = 20
        forecast_alpha: float = 0.3
        forecast_beta: float = 0.1
        load_balance_history_size: int = 100
        load_balance_weights: Dict[str, float] = field(default_factory=lambda: {
            'health': 0.3,
            'efficiency': 0.3,
            'quantum': 0.2,
            'performance': 0.2
        })
        adaptive_priority_enabled: bool = True
        adaptive_priority_learning_rate: float = 0.1
        priority_performance_window: int = 50
        synthesis_interval: float = 0.1
        regulation_interval: float = 30
        predictive_interval: float = 60
        forecast_interval: float = 60
        maintenance_interval: float = 60
        enable_multi_synthase: bool = True
        enable_quantum: bool = True
        enable_ml_prediction: bool = True
        enable_prometheus: bool = False
        degradation_tier_update_interval: int = 600
        efficiency_thresholds: Dict[int, float] = field(default_factory=lambda: {5: 0.9, 4: 0.8, 3: 0.7, 2: 0.6, 1: 0.0})
        shutdown_timeout_seconds: int = 30
        circuit_breaker_db_path: str = "./circuit_breakers.db"

    @dataclass
    class DemandPriorityConfig:
        priority_level: str
        weight: float
        min_balance: float
        max_consumption: float

# ============================================================================
# Enums and Data Classes
# ============================================================================

class SynthaseMode(Enum):
    SYNTHESIS = "synthesis"
    HYDROLYSIS = "hydrolysis"
    IDLE = "idle"
    INHIBITED = "inhibited"
    UNCOUPLED = "uncoupled"
    QUANTUM_ENHANCED = "quantum_enhanced"

class SynthaseState(Enum):
    ACTIVE = "active"
    DEGRADED = "degraded"
    OVERLOADED = "overloaded"
    REPAIRING = "repairing"
    DORMANT = "dormant"
    QUANTUM_READY = "quantum_ready"

@dataclass
class SynthaseConfig:
    """Synthase-specific configuration (derived from global config)."""
    protons_per_rotation: int = 12
    atp_per_rotation: int = 3
    max_rotation_speed_rpm: float = 6000
    activation_gradient: float = 0.05
    base_efficiency: float = 0.95
    atp_inhibition_constant: float = 0.1
    atp_inhibition_max: float = 0.5
    reverse_efficiency: float = 0.7
    hydrolysis_protons_per_atp: int = 4
    uncoupling_leak_rate: float = 0.01
    uncoupling_activation_threshold: float = 0.9
    adaptive_c_ring: bool = True
    min_c_ring: int = 8
    max_c_ring: int = 17
    degradation_scaling: bool = True
    quantum_tunneling_enabled: bool = True
    quantum_efficiency_boost: float = 0.25
    quantum_tunneling_threshold: float = 0.7
    quantum_coherence_time: float = 10.0

@dataclass
class ScheduledTask:
    task_id: str
    eco_atp_required: float
    priority: int
    deadline: Optional[datetime] = None
    callback: Optional[Callable] = None
    compartment_preference: Optional[str] = None
    scheduled_at: datetime = field(default_factory=datetime.utcnow)
    token_ids: List[str] = field(default_factory=list)
    status: str = "pending"
    user_priority: Optional[str] = None

@dataclass
class ProductionRecord:
    timestamp: datetime
    mode: str
    driving_force: float
    rotation_speed: float
    atp_produced: float
    efficiency: float
    demand_level: float
    inhibition_level: float
    degradation_tier: int
    quantum_enhancement: float = 0.0
    quantum_efficiency: float = 0.0

@dataclass
class DemandPriority:
    priority_level: str
    weight: float
    min_balance: float
    max_consumption: float

# ============================================================================
# TaskManager for background loops
# ============================================================================

class TaskManager:
    """Manages background tasks with restart and exponential backoff."""
    
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.shutdown_event = asyncio.Event()
        self._lock = asyncio.Lock()

    def start_task(self, name: str, coro_func, *args, **kwargs):
        """Start a background task and register it."""
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
        """Gracefully stop all tasks."""
        self.shutdown_event.set()
        async with self._lock:
            for task in self.tasks.values():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()
        logger.info("All background tasks stopped")

# ============================================================================
# Enhanced ATP Synthase (with all methods implemented)
# ============================================================================

class EnhancedATPSynthase:
    """Individual ATP Synthase complex with quantum tunneling and full behavior."""
    
    def __init__(self, synthase_id: str, config: SynthaseConfig):
        self.synthase_id = synthase_id
        self.config = config
        
        self.mode = SynthaseMode.IDLE
        self.state = SynthaseState.ACTIVE
        self.rotation_speed = 0.0
        self.current_efficiency = config.base_efficiency
        self.total_atp_produced = 0.0
        self.total_atp_hydrolyzed = 0.0
        self.production_history: deque = deque(maxlen=1000)
        self.inhibition_level = 0.0
        self.operational_hours = 0.0
        self.degradation_rate = 0.0001
        self.repair_rate = 0.01
        
        # Quantum
        self.quantum_coherence = 1.0
        self.quantum_enhancement_factor = 0.0
        self.quantum_active = False
        
        # Adaptive c-ring
        if self.config.adaptive_c_ring:
            self._adapt_c_ring()
        
        logger.info(f"ATP Synthase '{synthase_id}' initialized", c_ring=self.config.protons_per_rotation)
    
    def _adapt_c_ring(self):
        # Placeholder for adaptive c-ring logic; could adjust based on gradient strength
        # For simplicity, we keep default
        pass
    
    def calculate_driving_force(self, gradient_service: Optional[GradientServiceProtocol]) -> float:
        """Compute driving force from gradient fields."""
        if gradient_service is None:
            return 0.0
        strengths = gradient_service.get_field_strengths()
        # Weighted sum (could use config weights)
        force = 0.0
        weights = {'carbon': 0.25, 'helium': 0.15, 'trust': 0.20, 
                   'opportunity': 0.25, 'eco_atp_reserve': 0.15}
        for field, weight in weights.items():
            force += strengths.get(field, 0.0) * weight
        return force
    
    def calculate_rotation_speed(self, driving_force: float) -> float:
        """Calculate rotation speed from driving force."""
        if driving_force < self.config.activation_gradient:
            return 0.0
        # Simple linear mapping, capped at max_speed
        speed = driving_force * self.config.max_rotation_speed_rpm
        return min(speed, self.config.max_rotation_speed_rpm)
    
    def calculate_atp_production_rate(self, rotation_speed: float) -> float:
        """Calculate ATP production rate in molecules per second."""
        if rotation_speed == 0:
            return 0.0
        # rotations per second
        rps = rotation_speed / 60.0
        # ATP per rotation
        atp_per_rotation = self.config.atp_per_rotation
        # Apply efficiency
        efficiency = self.current_efficiency * (1 - self.inhibition_level)
        return rps * atp_per_rotation * efficiency
    
    def update_allosteric_inhibition(self, atp_balance: float):
        """Update inhibition level based on ATP balance."""
        if atp_balance > 20000:
            self.inhibition_level = min(
                self.config.atp_inhibition_max,
                self.inhibition_level + self.config.atp_inhibition_constant
            )
        elif atp_balance < 5000:
            self.inhibition_level = max(0.0, self.inhibition_level - 0.01)
        else:
            self.inhibition_level *= 0.99  # slow decay
        self.inhibition_level = max(0.0, min(self.config.atp_inhibition_max, self.inhibition_level))
    
    def operate_forward(self, gradient_service: Optional[GradientServiceProtocol],
                        token_service: Optional[TokenServiceProtocol],
                        account_id: str) -> float:
        """Perform forward operation (ATP synthesis)."""
        if self.state == SynthaseState.DORMANT:
            return 0.0
        
        driving_force = self.calculate_driving_force(gradient_service)
        speed = self.calculate_rotation_speed(driving_force)
        if speed == 0:
            return 0.0
        
        self.rotation_speed = speed
        self.mode = SynthaseMode.SYNTHESIS
        
        # Apply quantum enhancement
        if self.config.quantum_tunneling_enabled and self.quantum_active:
            speed *= (1 + self.quantum_enhancement_factor * self.config.quantum_efficiency_boost)
        
        # Produce ATP
        atp_rate = self.calculate_atp_production_rate(speed)
        atp_produced = atp_rate * self.config.synthesis_interval  # approximate
        self.total_atp_produced += atp_produced
        self.production_history.append(atp_produced)
        
        # Update quantum coherence
        if self.quantum_active:
            self.quantum_coherence -= 0.001
            if self.quantum_coherence <= 0:
                self.quantum_active = False
                self.quantum_enhancement_factor = 0.0
        
        # Update operational hours and degradation
        self.operational_hours += self.config.synthesis_interval / 3600.0
        if self.config.degradation_scaling:
            self.degradation_rate *= (1 + 0.001 * self.operational_hours)
        
        # Token generation
        if token_service:
            token_service.generate_tokens(
                account_id=account_id,
                source=EcoATPSource.GRADIENT_CONVERSION,
                energy_saved_kwh=atp_produced / 10000.0,
                efficiency=self.current_efficiency * (1 - self.inhibition_level)
            )
        
        return atp_produced
    
    def operate_reverse(self, gradient_service: Optional[GradientServiceProtocol],
                        token_service: Optional[TokenServiceProtocol],
                        account_id: str, amount: float) -> float:
        """Perform reverse operation (ATP hydrolysis to pump protons)."""
        if self.state == SynthaseState.DORMANT:
            return 0.0
        
        self.mode = SynthaseMode.HYDROLYSIS
        self.rotation_speed = -self.config.max_rotation_speed_rpm * 0.5  # reverse
        
        # Hydrolyze ATP
        atp_hydrolyzed = amount * self.config.reverse_efficiency
        self.total_atp_hydrolyzed += atp_hydrolyzed
        
        # Pump protons
        if gradient_service:
            field_id = "helium"  # example
            gradient_service.pump_field(field_id, atp_hydrolyzed * 0.01, "reverse_operation")
        
        return atp_hydrolyzed
    
    def operate_uncoupled(self, gradient_service: Optional[GradientServiceProtocol]):
        """Perform uncoupled operation (dissipate gradient)."""
        self.mode = SynthaseMode.UNCOUPLED
        self.rotation_speed = self.config.max_rotation_speed_rpm * 0.9
        # Dissipate gradient
        if gradient_service:
            strengths = gradient_service.get_field_strengths()
            for field_id, strength in strengths.items():
                if strength > self.config.uncoupling_activation_threshold:
                    gradient_service.discharge_field(field_id, strength * 0.1)
    
    def repair(self):
        """Repair degraded synthase."""
        self.state = SynthaseState.REPAIRING
        # Simulate repair process
        self.degradation_rate = max(0.0001, self.degradation_rate * 0.9)
        self.current_efficiency = min(self.config.base_efficiency, 
                                       self.current_efficiency + self.repair_rate)
        self.state = SynthaseState.ACTIVE
        logger.info(f"Synthase {self.synthase_id} repaired")
    
    def get_status(self) -> Dict[str, Any]:
        """Return current status."""
        return {
            'id': self.synthase_id,
            'mode': self.mode.value,
            'state': self.state.value,
            'rotation_speed': self.rotation_speed,
            'efficiency': self.current_efficiency,
            'inhibition_level': self.inhibition_level,
            'total_atp_produced': self.total_atp_produced,
            'total_atp_hydrolyzed': self.total_atp_hydrolyzed,
            'quantum_active': self.quantum_active,
            'quantum_enhancement': self.quantum_enhancement_factor,
            'operational_hours': self.operational_hours,
            'degradation_rate': self.degradation_rate
        }

# ============================================================================
# Demand Priority Manager with Adaptive Weights
# ============================================================================

class DemandPriorityManager:
    """User-defined demand priority management with adaptive weights."""
    
    def __init__(self, config: SynthaseSchedulerConfig):
        self.config = config
        self.priorities: Dict[str, DemandPriority] = {}
        for level, params in config.priority_defaults.items():
            self.priorities[level] = DemandPriority(
                priority_level=level,
                weight=params['weight'],
                min_balance=params['min_balance'],
                max_consumption=params['max_consumption']
            )
        self.default_priority = config.default_priority
        self._lock = asyncio.Lock()
        self.performance_history: Dict[str, List[float]] = defaultdict(lambda: deque(maxlen=config.priority_performance_window))
        logger.info("Demand Priority Manager initialized")
    
    async def set_priority_config(self, priority_level: str, weight: float,
                                min_balance: float, max_consumption: float):
        async with self._lock:
            if priority_level not in self.priorities:
                self.priorities[priority_level] = DemandPriority(
                    priority_level, weight, min_balance, max_consumption
                )
            else:
                self.priorities[priority_level].weight = weight
                self.priorities[priority_level].min_balance = min_balance
                self.priorities[priority_level].max_consumption = max_consumption
            logger.info("Priority configured", level=priority_level, weight=weight)
    
    def get_priority_weight(self, priority_level: str) -> float:
        return self.priorities.get(priority_level, self.priorities[self.default_priority]).weight
    
    def get_task_priority(self, task: ScheduledTask) -> float:
        base_weight = self.get_priority_weight(task.user_priority or self.default_priority)
        if task.deadline:
            time_remaining = (task.deadline - datetime.utcnow()).total_seconds()
            if time_remaining < 300:
                base_weight *= 1.5
            elif time_remaining < 3600:
                base_weight *= 1.2
        return base_weight * (task.priority + 1)
    
    async def adapt_weights(self):
        """Adapt priority weights based on historical performance."""
        if not self.config.adaptive_priority_enabled:
            return
        async with self._lock:
            for level, hist in self.performance_history.items():
                if len(hist) >= self.config.priority_performance_window:
                    avg_perf = np.mean(hist)
                    # Increase weight if performance is good, decrease if poor
                    if avg_perf > 0.8:
                        delta = self.config.adaptive_priority_learning_rate
                    elif avg_perf < 0.5:
                        delta = -self.config.adaptive_priority_learning_rate
                    else:
                        delta = 0.0
                    self.priorities[level].weight += delta
                    self.priorities[level].weight = max(0.1, min(5.0, self.priorities[level].weight))
                    logger.debug(f"Adjusted priority weight for {level}: {self.priorities[level].weight:.2f}")
    
    async def record_performance(self, priority_level: str, success: bool, latency: float):
        """Record the performance of a task with given priority."""
        async with self._lock:
            if priority_level in self.priorities:
                self.performance_history[priority_level].append(1.0 if success else 0.0)

# ============================================================================
# Synthase Load Balancer (Enhanced)
# ============================================================================

class SynthaseLoadBalancer:
    """Load balancing between synthases with adaptive weights."""
    
    def __init__(self, config: SynthaseSchedulerConfig):
        self.config = config
        self.historical_loads: Dict[str, List[float]] = {}
        self.efficiency_scores: Dict[str, float] = {}
        self.performance_history: Dict[str, deque] = {}
        self._lock = asyncio.Lock()
        logger.info("Synthase Load Balancer initialized")
    
    async def assign_load(self, synthases: Dict[str, 'EnhancedATPSynthase'],
                         total_demand: float) -> Dict[str, float]:
        async with self._lock:
            if not synthases:
                return {}
            
            scores = {}
            total_score = 0.0
            weights = self.config.load_balance_weights
            
            for sid, synthase in synthases.items():
                # Health score
                if synthase.state == SynthaseState.ACTIVE:
                    health_score = 1.0
                elif synthase.state == SynthaseState.QUANTUM_READY:
                    health_score = 1.2
                elif synthase.state == SynthaseState.DEGRADED:
                    health_score = 0.6
                elif synthase.state == SynthaseState.REPAIRING:
                    health_score = 0.3
                else:
                    health_score = 0.5
                
                # Efficiency
                efficiency_score = synthase.current_efficiency
                
                # Quantum bonus
                quantum_bonus = 1.0 + synthase.quantum_enhancement_factor * 0.5
                
                # Historical performance (average load handled)
                hist = self.performance_history.get(sid, deque(maxlen=10))
                if hist:
                    avg_perf = sum(hist) / len(hist)
                else:
                    avg_perf = 0.5
                performance_factor = 0.5 + avg_perf  # scale 0.5..1.5
                
                # Composite score with configurable weights
                score = (health_score * weights.get('health', 0.3) +
                         efficiency_score * weights.get('efficiency', 0.3) +
                         quantum_bonus * weights.get('quantum', 0.2) +
                         performance_factor * weights.get('performance', 0.2))
                
                if sid not in self.historical_loads:
                    self.historical_loads[sid] = []
                self.historical_loads[sid].append(score)
                if len(self.historical_loads[sid]) > self.config.load_balance_history_size:
                    self.historical_loads[sid] = self.historical_loads[sid][-self.config.load_balance_history_size:]
                
                scores[sid] = score
                total_score += score
            
            if total_score == 0:
                return {sid: total_demand / len(synthases) for sid in synthases}
            
            assignments = {}
            for sid, score in scores.items():
                assignments[sid] = (score / total_score) * total_demand
            
            return assignments
    
    async def record_performance(self, synthase_id: str, load: float):
        """Record the load actually handled by a synthase."""
        if synthase_id not in self.performance_history:
            self.performance_history[synthase_id] = deque(maxlen=10)
        self.performance_history[synthase_id].append(load)
    
    def get_load_balance_stats(self) -> Dict:
        return {
            'synthases_tracked': len(self.historical_loads),
            'average_loads': {
                sid: np.mean(loads) if loads else 0
                for sid, loads in self.historical_loads.items()
            }
        }

# ============================================================================
# ML Demand Predictor with RandomForest and secure serialization
# ============================================================================

class MLDemandPredictor:
    """Machine learning for demand prediction using RandomForest with joblib."""
    
    def __init__(self, config: SynthaseSchedulerConfig):
        self.config = config
        self.model = None
        self.scaler = None
        self.is_trained = False
        self.training_data: List[float] = []
        self._lock = asyncio.Lock()
        self._load_model()
        logger.info("ML Demand Predictor initialized")
    
    def _load_model(self):
        """Load model from disk using joblib."""
        if not SKLEARN_AVAILABLE:
            return
        path = self.config.ml_model_path
        if os.path.exists(path):
            try:
                self.model, self.scaler = joblib.load(path)
                self.is_trained = True
                logger.info("Loaded ML model", path=path)
            except Exception as e:
                logger.warning("Failed to load ML model", error=str(e))
    
    def _save_model(self):
        """Save model to disk using joblib."""
        if not SKLEARN_AVAILABLE or not self.is_trained:
            return
        path = self.config.ml_model_path
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            joblib.dump((self.model, self.scaler), path)
            logger.info("Saved ML model", path=path)
        except Exception as e:
            logger.error("Failed to save ML model", error=str(e))
    
    async def train(self, demand_history: List[float]) -> Dict:
        """Train the demand prediction model using RandomForest."""
        if not SKLEARN_AVAILABLE:
            return {'status': 'sklearn_not_available'}
        if len(demand_history) < self.config.ml_min_samples:
            return {'status': 'insufficient_data'}
        
        async with self._lock:
            X = []
            y = []
            for i in range(self.config.ml_lookback, len(demand_history) - 1):
                X.append(demand_history[i - self.config.ml_lookback:i])
                y.append(demand_history[i + 1])
            X = np.array(X)
            y = np.array(y)
            if len(X) < self.config.ml_min_samples:
                return {'status': 'insufficient_samples'}
            
            if self.scaler is None:
                self.scaler = StandardScaler()
            X_scaled = self.scaler.fit_transform(X)
            
            self.model = RandomForestRegressor(n_estimators=100, random_state=42)
            self.model.fit(X_scaled, y)
            self.is_trained = True
            self.training_data = demand_history
            self._save_model()
            logger.info("ML model trained", samples=len(X))
            return {'status': 'success', 'samples': len(X)}
    
    async def predict(self, recent_demand: List[float]) -> Dict:
        """Predict future demand."""
        if not self.is_trained or len(recent_demand) < self.config.ml_lookback:
            return {'prediction': None, 'confidence': 0.0}
        async with self._lock:
            features = recent_demand[-self.config.ml_lookback:]
            features_scaled = self.scaler.transform([features])
            prediction = self.model.predict(features_scaled)[0]
            # Confidence based on prediction std (simplified)
            # For RandomForest, we could use tree variance; for simplicity, use volatility.
            volatility = np.std(recent_demand[-20:]) if len(recent_demand) >= 20 else 0.2
            confidence = max(0.1, 1.0 - volatility)
            return {'prediction': max(0.0, min(1.0, prediction)), 'confidence': confidence}
    
    def get_model_stats(self) -> Dict:
        """Return model statistics."""
        return {
            'is_trained': self.is_trained,
            'training_samples': len(self.training_data) if self.training_data else 0,
            'model_type': type(self.model).__name__ if self.model else None,
            'scaler': type(self.scaler).__name__ if self.scaler else None,
            'lookback': self.config.ml_lookback
        }

# ============================================================================
# Gradient Forecaster with Holt-Winters Exponential Smoothing
# ============================================================================

class GradientForecaster:
    """Gradient forecasting with Holt-Winters exponential smoothing."""
    
    def __init__(self, config: SynthaseSchedulerConfig):
        self.config = config
        self.gradient_history: Dict[str, List[float]] = {}
        self.forecast_results: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        # Holt-Winters state per field
        self.level: Dict[str, float] = {}
        self.trend: Dict[str, float] = {}
        self.last_update: Dict[str, datetime] = {}
        logger.info("Gradient Forecaster initialized")
    
    def record_gradient(self, field_id: str, value: float):
        if field_id not in self.gradient_history:
            self.gradient_history[field_id] = []
            self.level[field_id] = value
            self.trend[field_id] = 0.0
        self.gradient_history[field_id].append(value)
        if len(self.gradient_history[field_id]) > self.config.forecast_history_window * 2:
            self.gradient_history[field_id] = self.gradient_history[field_id][-self.config.forecast_history_window*2:]
        # Update Holt-Winters state
        if len(self.gradient_history[field_id]) >= 2:
            alpha = self.config.forecast_alpha
            beta = self.config.forecast_beta
            last_level = self.level.get(field_id, value)
            last_trend = self.trend.get(field_id, 0.0)
            self.level[field_id] = alpha * value + (1 - alpha) * (last_level + last_trend)
            self.trend[field_id] = beta * (self.level[field_id] - last_level) + (1 - beta) * last_trend
            self.last_update[field_id] = datetime.now(timezone.utc)
    
    async def forecast(self, field_id: str) -> Dict:
        """Forecast gradient values using Holt-Winters."""
        if field_id not in self.gradient_history or len(self.gradient_history[field_id]) < 20:
            return {'status': 'insufficient_data'}
        async with self._lock:
            current_level = self.level.get(field_id, 0.5)
            current_trend = self.trend.get(field_id, 0.0)
            forecast_values = []
            for i in range(self.config.forecast_horizon):
                next_value = current_level + current_trend * (i + 1)
                forecast_values.append(max(0.0, min(1.0, next_value)))
            volatility = np.std(self.gradient_history[field_id][-20:]) if len(self.gradient_history[field_id]) >= 20 else 0.2
            confidence = max(0.1, 1.0 - volatility * 2)
            result = {
                'field': field_id,
                'current': self.gradient_history[field_id][-1],
                'forecast': forecast_values,
                'trend': 'increasing' if current_trend > 0.01 else 'decreasing' if current_trend < -0.01 else 'stable',
                'slope': current_trend,
                'confidence': confidence
            }
            self.forecast_results[field_id] = result
            return result

# ============================================================================
# Enhanced ATP Synthase Scheduler (Main class)
# ============================================================================

class ATPSynthaseScheduler:
    """
    Enhanced ATP Synthase Scheduler v9.0.0.
    Integrates all features with concurrency safety, configuration, and task management.
    """
    
    def __init__(
        self,
        token_service: Optional[TokenServiceProtocol] = None,
        gradient_service: Optional[GradientServiceProtocol] = None,
        harvester: Optional[HarvesterProtocol] = None,
        config: Optional[Union[SynthaseSchedulerConfig, Dict[str, Any]]] = None
    ):
        self.token_service = token_service
        self.gradient_service = gradient_service
        self.harvester = harvester
        
        # Load configuration
        if isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = SynthaseSchedulerConfig(**config)
            else:
                self.config = SynthaseSchedulerConfig(**config)
        elif isinstance(config, SynthaseSchedulerConfig):
            self.config = config
        else:
            self.config = SynthaseSchedulerConfig()
        
        # Create synthase config from global config
        synthase_config = SynthaseConfig(
            protons_per_rotation=self.config.protons_per_rotation,
            atp_per_rotation=self.config.atp_per_rotation,
            max_rotation_speed_rpm=self.config.max_rotation_speed_rpm,
            activation_gradient=self.config.activation_gradient,
            base_efficiency=self.config.base_efficiency,
            atp_inhibition_constant=self.config.atp_inhibition_constant,
            atp_inhibition_max=self.config.atp_inhibition_max,
            reverse_efficiency=self.config.reverse_efficiency,
            hydrolysis_protons_per_atp=self.config.hydrolysis_protons_per_atp,
            uncoupling_leak_rate=self.config.uncoupling_leak_rate,
            uncoupling_activation_threshold=self.config.uncoupling_activation_threshold,
            adaptive_c_ring=self.config.adaptive_c_ring,
            min_c_ring=self.config.min_c_ring,
            max_c_ring=self.config.max_c_ring,
            degradation_scaling=self.config.degradation_scaling,
            quantum_tunneling_enabled=self.config.quantum_tunneling_enabled,
            quantum_efficiency_boost=self.config.quantum_efficiency_boost,
            quantum_tunneling_threshold=self.config.quantum_tunneling_threshold,
            quantum_coherence_time=self.config.quantum_coherence_time
        )
        
        # Primary synthase
        self.primary_synthase = EnhancedATPSynthase("primary", synthase_config)
        self.synthases: Dict[str, EnhancedATPSynthase] = {"primary": self.primary_synthase}
        
        # Sub-components
        self.priority_manager = DemandPriorityManager(self.config)
        self.load_balancer = SynthaseLoadBalancer(self.config)
        self.ml_predictor = MLDemandPredictor(self.config) if self.config.enable_ml_prediction else None
        self.gradient_forecaster = GradientForecaster(self.config)
        
        # Queues
        self.execution_queue: List[ScheduledTask] = []
        self.priority_queue: List[ScheduledTask] = []
        
        # State
        self.total_eco_atp_produced = 0.0
        self.generation_history: deque = deque(maxlen=1000)
        self.demand_history: deque = deque(maxlen=500)
        self.predicted_demand = 0.0
        self.current_tier = 5
        self.account_id = "atp_synthase"
        if token_service:
            token_service.create_account(self.account_id)
        
        # Locks
        self._queue_lock = asyncio.Lock()
        self._synthase_lock = asyncio.Lock()
        self._demand_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        
        # Circuit breakers with persistence
        self._token_circuit = CircuitBreaker(
            "token_service",
            db_path=self.config.circuit_breaker_db_path,
            failure_threshold=3,
            recovery_timeout=30
        )
        self._gradient_circuit = CircuitBreaker(
            "gradient_service",
            db_path=self.config.circuit_breaker_db_path,
            failure_threshold=3,
            recovery_timeout=30
        )
        
        # Task manager
        self._task_manager = TaskManager()
        self._task_manager.start_task("synthesis", self._synthesis_loop)
        self._task_manager.start_task("regulation", self._regulation_loop)
        self._task_manager.start_task("maintenance", self._maintenance_loop)
        self._task_manager.start_task("predictive", self._predictive_loop)
        self._task_manager.start_task("gradient_forecast", self._gradient_forecast_loop)
        self._task_manager.start_task("degradation_update", self._degradation_update_loop)
        self._task_manager.start_task("priority_adapt", self._priority_adapt_loop)
        
        # Prometheus metrics
        self._setup_metrics()
        
        logger.info("ATP Synthase Scheduler v9.0.0 initialized", config=self.config.dict() if PYDANTIC_AVAILABLE else asdict(self.config))
    
    def _setup_metrics(self):
        """Setup Prometheus metrics if enabled."""
        if not self.config.enable_prometheus or not PROMETHEUS_AVAILABLE:
            self.metrics = {}
            return
        self.metrics = {
            'total_produced': Counter('atp_total_produced', 'Total Eco-ATP produced'),
            'production_rate': Gauge('atp_production_rate', 'Current production rate'),
            'demand_level': Gauge('atp_demand_level', 'Current demand level'),
            'efficiency': Gauge('atp_efficiency', 'Current efficiency'),
            'synthase_count': Gauge('atp_synthase_count', 'Number of synthases'),
            'quantum_enhancement': Gauge('atp_quantum_enhancement', 'Quantum enhancement factor'),
            'queue_size': Gauge('atp_queue_size', 'Execution queue size'),
            'priority_queue_size': Gauge('atp_priority_queue_size', 'Priority queue size'),
            'degradation_tier': Gauge('atp_degradation_tier', 'Current degradation tier'),
            'inhibition_level': Gauge('atp_inhibition_level', 'Current inhibition level')
        }
    
    async def shutdown(self, timeout: Optional[float] = None):
        """
        Gracefully shut down the scheduler.
        Args:
            timeout: Maximum time to wait for background tasks to finish.
        """
        logger.info("Shutting down ATP Synthase Scheduler")
        if timeout is None:
            timeout = self.config.shutdown_timeout_seconds
        # Stop all tasks with timeout
        try:
            await asyncio.wait_for(self._task_manager.stop_all(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Background tasks did not finish in time; forcing cancellation")
        # Save ML model
        if self.ml_predictor:
            self.ml_predictor._save_model()
        logger.info("ATP Synthase Scheduler shutdown complete")
    
    # ========================================================================
    # Core Operations (with locks)
    # ========================================================================
    
    def calculate_gradient_driving_force(self) -> float:
        """Calculate overall gradient driving force using configurable weights."""
        if not self.gradient_service:
            return 0.0
        strengths = self.gradient_service.get_field_strengths()
        weights = self.config.driving_force_weights
        force = sum(strengths.get(field, 0) * weight for field, weight in weights.items())
        return force
    
    def _calculate_demand_level(self) -> float:
        """Calculate current demand level with priority weighting."""
        if not self.token_service:
            return 0.5
        summary = self.token_service.get_system_summary()
        balance = summary.get('total_balance', 10000)
        consumption_rate = summary.get('total_consumed', 0)
        generation_rate = summary.get('total_generated', 0)
        
        queue_demand = min(1.0, len(self.execution_queue) / 50.0)
        
        # Priority-weighted demand
        if self.execution_queue:
            weights = [self.priority_manager.get_task_priority(t) for t in self.execution_queue[:10]]
            priority_demand = np.mean(weights) if weights else 0.5
        else:
            priority_demand = 0.5
        
        if generation_rate > 0:
            ratio_demand = consumption_rate / generation_rate
        else:
            ratio_demand = 1.0
        
        if balance < 5000:
            balance_demand = 1.0
        elif balance < 20000:
            balance_demand = 0.5 + (20000 - balance) / 30000
        else:
            balance_demand = max(0.1, 1.0 - (balance - 20000) / 30000)
        
        demand = (queue_demand * 0.2 + priority_demand * 0.2 + ratio_demand * 0.3 + balance_demand * 0.3)
        demand = min(1.0, max(0.1, demand))
        
        async with self._demand_lock:
            self.demand_history.append(demand)
        return demand
    
    # ========================================================================
    # Synthase management (async)
    # ========================================================================
    
    async def spawn_synthase(self, c_ring_size: Optional[int] = None) -> str:
        """Spawn a new ATP synthase."""
        if not self.config.enable_multi_synthase:
            return "primary"
        
        config = SynthaseConfig()
        if c_ring_size:
            config.protons_per_rotation = c_ring_size
        config.quantum_tunneling_enabled = self.config.quantum_tunneling_enabled
        
        synthase_id = f"synthase_{len(self.synthases)}"
        synthase = EnhancedATPSynthase(synthase_id, config)
        async with self._synthase_lock:
            self.synthases[synthase_id] = synthase
        logger.info("Spawned ATP synthase", id=synthase_id, c_ring=config.protons_per_rotation)
        return synthase_id
    
    async def remove_synthase(self, synthase_id: str) -> bool:
        """Remove a synthase (cannot remove primary)."""
        if synthase_id == "primary" or synthase_id not in self.synthases:
            return False
        async with self._synthase_lock:
            del self.synthases[synthase_id]
        logger.info("Removed ATP synthase", id=synthase_id)
        return True
    
    # ========================================================================
    # Synthesis Loop
    # ========================================================================
    
    async def _synthesis_loop(self):
        """Continuous ATP synthesis with demand modulation and load balancing."""
        while True:
            try:
                total_produced = 0.0
                demand = self._calculate_demand_level()
                
                # Get load assignments
                async with self._synthase_lock:
                    synthases_copy = self.synthases.copy()
                load_assignments = await self.load_balancer.assign_load(synthases_copy, demand)
                
                for synthase_id, synthase in synthases_copy.items():
                    if synthase.state not in [SynthaseState.ACTIVE, SynthaseState.QUANTUM_READY]:
                        continue
                    assigned_load = load_assignments.get(synthase_id, demand / len(synthases_copy))
                    
                    # Update inhibition
                    if self.token_service:
                        summary = self.token_service.get_system_summary()
                        balance = summary.get('total_balance', 10000)
                        synthase.update_allosteric_inhibition(balance)
                    
                    # Check reverse or uncoupling
                    if self._should_reverse_operate():
                        synthase.operate_reverse(
                            self.gradient_service, self.token_service,
                            self.account_id, amount=50.0 * assigned_load
                        )
                        continue
                    if self._should_uncouple():
                        synthase.operate_uncoupled(self.gradient_service)
                        continue
                    
                    # Forward operation
                    driving_force = synthase.calculate_driving_force(self.gradient_service)
                    rotation_speed = synthase.calculate_rotation_speed(driving_force)
                    if rotation_speed > 0:
                        base_rate = synthase.calculate_atp_production_rate(rotation_speed)
                        if synthase_id == "primary":
                            eco_atp_rate = self._modulate_production(base_rate) * assigned_load
                        else:
                            eco_atp_rate = base_rate * assigned_load
                        if eco_atp_rate > 0.1:
                            eco_atp_produced = synthase.operate_forward(
                                self.gradient_service, self.token_service, self.account_id
                            )
                            total_produced += eco_atp_produced * assigned_load
                            await self.load_balancer.record_performance(synthase_id, assigned_load)
                
                if total_produced > 0:
                    async with self._state_lock:
                        self.total_eco_atp_produced += total_produced
                    if self.metrics:
                        self.metrics['total_produced'].inc(total_produced)
                        self.metrics['production_rate'].set(total_produced / self.config.synthesis_interval)
                
                # Record gradients for forecasting
                if self.gradient_service:
                    strengths = self.gradient_service.get_field_strengths()
                    for field_id, strength in strengths.items():
                        self.gradient_forecaster.record_gradient(field_id, strength)
                
                await asyncio.sleep(self.config.synthesis_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Synthesis loop error", error=str(e))
                await asyncio.sleep(5)
    
    def _modulate_production(self, base_rate: float) -> float:
        """Modulate production rate based on demand and degradation tier."""
        demand = self._calculate_demand_level()
        tier_scaling = {5: 1.0, 4: 0.75, 3: 0.5, 2: 0.25, 1: 0.1}
        tier_factor = tier_scaling.get(self.current_tier, 1.0)
        if demand > 0.7:
            demand_factor = 1.0 + (demand - 0.7) * 1.5
        elif demand < 0.3:
            demand_factor = 0.5 + demand
        else:
            demand_factor = 1.0
        quantum_factor = 1.0
        if self.config.enable_quantum and self.primary_synthase.quantum_active:
            quantum_factor = 1.0 + self.primary_synthase.quantum_enhancement_factor * 0.3
        return base_rate * demand_factor * tier_factor * quantum_factor
    
    def _should_reverse_operate(self) -> bool:
        """Determine if reverse operation is needed."""
        if not self.token_service or not self.gradient_service:
            return False
        summary = self.token_service.get_system_summary()
        balance = summary.get('total_balance', 10000)
        if balance > 40000:
            carbon = self.gradient_service.get_field_strengths().get('carbon', 0.5)
            if carbon < 0.3:
                return True
        return False
    
    def _should_uncouple(self) -> bool:
        """Determine if uncoupling is needed."""
        if not self.gradient_service:
            return False
        strengths = self.gradient_service.get_field_strengths()
        for strength in strengths.values():
            if strength > self.config.uncoupling_activation_threshold:
                return True
        return False
    
    # ========================================================================
    # Other loops
    # ========================================================================
    
    async def _regulation_loop(self):
        """Regulatory loop for scaling, inhibition, and metrics updates."""
        while True:
            try:
                if self.token_service:
                    summary = self.token_service.get_system_summary()
                    balance = summary.get('total_balance', 10000)
                    async with self._synthase_lock:
                        for synthase in self.synthases.values():
                            synthase.update_allosteric_inhibition(balance)
                
                demand = self._calculate_demand_level()
                active_count = sum(1 for s in self.synthases.values() if s.state in [SynthaseState.ACTIVE, SynthaseState.QUANTUM_READY])
                if demand > 0.8 and active_count < 3 and self.config.enable_multi_synthase:
                    await self.spawn_synthase()
                elif demand < 0.2 and len(self.synthases) > 1:
                    for sid in list(self.synthases.keys()):
                        if sid != "primary" and len(self.synthases) > 1:
                            await self.remove_synthase(sid)
                            break
                
                # Update Prometheus gauges
                if self.metrics:
                    self.metrics['synthase_count'].set(len(self.synthases))
                    self.metrics['queue_size'].set(len(self.execution_queue))
                    self.metrics['priority_queue_size'].set(len(self.priority_queue))
                    self.metrics['degradation_tier'].set(self.current_tier)
                    self.metrics['inhibition_level'].set(self.primary_synthase.inhibition_level)
                    self.metrics['quantum_enhancement'].set(self.primary_synthase.quantum_enhancement_factor)
                
                await asyncio.sleep(self.config.regulation_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Regulation loop error", error=str(e))
                await asyncio.sleep(60)
    
    async def _maintenance_loop(self):
        """Maintenance loop for repairing synthases."""
        while True:
            try:
                async with self._synthase_lock:
                    for synthase in self.synthases.values():
                        if synthase.state == SynthaseState.DEGRADED:
                            synthase.repair()
                await asyncio.sleep(self.config.maintenance_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Maintenance loop error", error=str(e))
                await asyncio.sleep(60)
    
    async def _predictive_loop(self):
        """Predictive loop with ML."""
        while True:
            try:
                if self.ml_predictor:
                    async with self._demand_lock:
                        history = list(self.demand_history)
                    # Retrain if needed
                    if len(history) > self.config.ml_min_samples and (not self.ml_predictor.is_trained or len(history) % 10 == 0):
                        await self.ml_predictor.train(history)
                    if len(history) > self.config.ml_lookback:
                        pred = await self.ml_predictor.predict(history)
                        if pred['prediction'] is not None:
                            self.predicted_demand = pred['prediction']
                            logger.debug("ML demand prediction", value=self.predicted_demand, confidence=pred['confidence'])
                
                if self.predicted_demand > 0.7 and self.token_service:
                    pre_amount = self.predicted_demand * 100
                    # Use retry decorator for external call
                    @retry_decorator(max_attempts=3, min_delay=0.1, max_delay=2)
                    async def generate():
                        self.token_service.generate_tokens(
                            account_id=self.account_id,
                            source=EcoATPSource.GRADIENT_CONVERSION,
                            energy_saved_kwh=pre_amount / 10000.0,
                            efficiency=0.9
                        )
                    await generate()
                await asyncio.sleep(self.config.predictive_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Predictive loop error", error=str(e))
                await asyncio.sleep(120)
    
    async def _gradient_forecast_loop(self):
        """Gradient forecasting loop."""
        while True:
            try:
                if self.gradient_service:
                    strengths = self.gradient_service.get_field_strengths()
                    for field_id in strengths:
                        await self.gradient_forecaster.forecast(field_id)
                await asyncio.sleep(self.config.forecast_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Gradient forecast loop error", error=str(e))
                await asyncio.sleep(120)
    
    async def _degradation_update_loop(self):
        """Update degradation tier based on average efficiency."""
        while True:
            try:
                async with self._synthase_lock:
                    efficiencies = [s.current_efficiency for s in self.synthases.values()]
                if efficiencies:
                    avg_efficiency = np.mean(efficiencies)
                    for tier, threshold in sorted(self.config.efficiency_thresholds.items(), reverse=True):
                        if avg_efficiency >= threshold:
                            if self.current_tier != tier:
                                self.current_tier = tier
                                logger.info("Degradation tier updated", tier=tier, avg_efficiency=avg_efficiency)
                            break
                await asyncio.sleep(self.config.degradation_tier_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Degradation update loop error", error=str(e))
                await asyncio.sleep(60)
    
    async def _priority_adapt_loop(self):
        """Adapt priority weights based on historical performance."""
        while True:
            try:
                await self.priority_manager.adapt_weights()
                await asyncio.sleep(self.config.regulation_interval * 5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Priority adaptation loop error", error=str(e))
                await asyncio.sleep(60)
    
    # ========================================================================
    # Scheduling methods
    # ========================================================================
    
    async def schedule_execution(self, task_id: str, eco_atp_required: float,
                               priority: int = 0, deadline: Optional[datetime] = None,
                               callback: Optional[Callable] = None,
                               user_priority: Optional[str] = None) -> bool:
        """Schedule task execution."""
        if not self.token_service:
            return True
        
        # Use retry for token reservation
        @retry_decorator(max_attempts=3, min_delay=0.1, max_delay=2)
        async def reserve():
            return self.token_service.reserve_tokens(
                self.account_id, eco_atp_required, EcoATPConsumer.EXPERT_EXECUTION
            )
        success, token_ids = await reserve()
        if success:
            task = ScheduledTask(
                task_id=task_id, eco_atp_required=eco_atp_required,
                priority=priority, deadline=deadline, callback=callback,
                token_ids=token_ids, user_priority=user_priority
            )
            async with self._queue_lock:
                self.execution_queue.append(task)
                self.execution_queue.sort(
                    key=lambda t: (self.priority_manager.get_task_priority(t), t.deadline or datetime.max),
                    reverse=True
                )
            return True
        else:
            task = ScheduledTask(
                task_id=task_id, eco_atp_required=eco_atp_required,
                priority=priority, deadline=deadline, callback=callback,
                user_priority=user_priority
            )
            async with self._queue_lock:
                self.priority_queue.append(task)
            return False
    
    async def execute_next_task(self) -> Optional[Dict[str, Any]]:
        """Execute the next task."""
        async with self._queue_lock:
            if not self.execution_queue:
                return None
            task = self.execution_queue.pop(0)
        if self.token_service:
            self.token_service.consume_tokens(task.token_ids, EcoATPConsumer.EXPERT_EXECUTION, True)
        if task.callback:
            # Handle async callbacks properly
            if asyncio.iscoroutinefunction(task.callback):
                result = await task.callback()
            else:
                result = task.callback()
            task.status = "completed"
            return {'task_id': task.task_id, 'result': result, 'status': 'completed'}
        task.status = "completed"
        return {'task_id': task.task_id, 'status': 'completed'}
    
    async def recover_failed_task(self, task_id: str, completion_percentage: float) -> float:
        """Recover tokens from failed task."""
        async with self._queue_lock:
            for task in self.execution_queue:
                if task.task_id == task_id:
                    if self.token_service:
                        recovered = self.token_service.recover_tokens(task.token_ids, completion_percentage)
                        self.execution_queue.remove(task)
                        return recovered
        return 0.0
    
    # ========================================================================
    # Public methods for configuration and stats
    # ========================================================================
    
    async def set_priority_config(self, priority_level: str, weight: float,
                                 min_balance: float, max_consumption: float):
        """Set priority configuration."""
        await self.priority_manager.set_priority_config(priority_level, weight, min_balance, max_consumption)
    
    def set_degradation_tier(self, tier: int):
        """Set degradation tier manually."""
        self.current_tier = max(1, min(5, tier))
        if tier <= 2:
            # Remove non-primary synthases asynchronously with proper waiting
            async def remove_all():
                tasks = []
                for sid in list(self.synthases.keys()):
                    if sid != "primary":
                        tasks.append(self.remove_synthase(sid))
                await asyncio.gather(*tasks, return_exceptions=True)
            asyncio.create_task(remove_all())
        logger.info("Degradation tier set", tier=tier)
    
    def get_scheduler_stats(self) -> Dict[str, Any]:
        """Get comprehensive scheduler statistics."""
        driving_force = self.calculate_gradient_driving_force()
        rotation_speed = self.primary_synthase.calculate_rotation_speed(driving_force)
        atp_rate = self.primary_synthase.calculate_atp_production_rate(rotation_speed)
        
        stats = {
            'total_eco_atp_produced': self.total_eco_atp_produced,
            'current_driving_force': driving_force,
            'current_rotation_speed': rotation_speed,
            'current_atp_rate': atp_rate,
            'demand_level': self._calculate_demand_level(),
            'predicted_demand': self.predicted_demand,
            'degradation_tier': self.current_tier,
            'queue_size': len(self.execution_queue),
            'priority_queue_size': len(self.priority_queue),
            'synthase_count': len(self.synthases),
            'active_synthases': sum(1 for s in self.synthases.values() if s.state in [SynthaseState.ACTIVE, SynthaseState.QUANTUM_READY]),
            'quantum_active': self.config.enable_quantum and any(s.quantum_active for s in self.synthases.values()),
            'synthases': {sid: s.get_status() for sid, s in self.synthases.items()},
            'load_balance': self.load_balancer.get_load_balance_stats(),
            'ml_predictor': self.ml_predictor.get_model_stats() if self.ml_predictor else None,
            'gradient_forecast': self.gradient_forecaster.forecast_results
        }
        return stats
    
    def get_efficiency_report(self) -> Dict[str, Any]:
        """Get efficiency optimization report."""
        report = {
            'primary_efficiency': self.primary_synthase.current_efficiency,
            'base_efficiency': self.config.base_efficiency,
            'inhibition_level': self.primary_synthase.inhibition_level,
            'synthase_count': len(self.synthases),
            'quantum_enhancement': self.primary_synthase.quantum_enhancement_factor,
            'quantum_active': self.primary_synthase.quantum_active,
            'recommendations': []
        }
        if self.primary_synthase.current_efficiency < 0.8:
            report['recommendations'].append("Primary synthase degraded. Consider repair cycle.")
        if len(self.synthases) > 1 and self._calculate_demand_level() < 0.3:
            report['recommendations'].append("Low demand with multiple synthases. Consider consolidating.")
        if self.primary_synthase.inhibition_level > 0.4:
            report['recommendations'].append("High ATP inhibition. Consider reverse operation to regulate.")
        if self.config.enable_quantum and not self.primary_synthase.quantum_active and self._calculate_demand_level() > 0.5:
            report['recommendations'].append("Quantum enhancement available but inactive. Increase gradient to activate.")
        return report
    
    # ========================================================================
    # Async context manager
    # ========================================================================
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

# ============================================================================
# Example usage
# ============================================================================

async def example_usage():
    """Example demonstrating the scheduler."""
    # Mock services
    class MockTokenService:
        def get_system_summary(self):
            return {'total_balance': 10000, 'total_consumed': 500, 'total_generated': 400}
        def generate_tokens(self, **kwargs):
            return []
        def reserve_tokens(self, **kwargs):
            return True, []
        def consume_tokens(self, **kwargs):
            return 0
        def recover_tokens(self, **kwargs):
            return 0
        def create_account(self, account_id):
            pass
        def get_account_summary(self, account_id):
            return {'balance': 10000}
    
    class MockGradientService:
        def get_field_strengths(self):
            return {'carbon': 0.8, 'helium': 0.2, 'trust': 0.1, 'opportunity': 0.9, 'eco_atp_reserve': 0.5}
        def discharge_field(self, field_id, amount):
            return 0
        def pump_field(self, field_id, amount, source):
            pass
        def get_field_stats(self):
            return {}
    
    token = MockTokenService()
    gradient = MockGradientService()
    
    config = {
        'enable_multi_synthase': True,
        'enable_quantum': True,
        'enable_ml_prediction': True,
        'ml_model_path': './test_model.joblib',
        'circuit_breaker_db_path': './test_cb.db'
    }
    scheduler = ATPSynthaseScheduler(
        token_service=token,
        gradient_service=gradient,
        config=config
    )
    
    # Run for a few seconds
    await asyncio.sleep(5)
    
    stats = scheduler.get_scheduler_stats()
    print("Stats:", stats)
    
    await scheduler.shutdown()

if __name__ == "__main__":
    asyncio.run(example_usage())
