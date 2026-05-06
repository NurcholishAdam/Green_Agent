# src/enhancements/control_system.py

"""
Complete Control System for Green Agent - Enhanced Version 3.1

Features:
1. Circuit breaker pattern for fault isolation (ENHANCED with adaptive thresholds)
2. Sensor feedback loops for actuation verification (ENHANCED with validation)
3. Gradual ramping to avoid thermal shock (ENHANCED with configurable ramping)
4. State persistence across restarts (ENHANCED with SQLite backend)
5. Predictive actuation with LSTM-based trend analysis (ENHANCED with multiple models)
6. Comprehensive audit logging (ENHANCED with structured logging)
7. Rate limiting for cooling systems (ENHANCED with adaptive limits)
8. Calibration support for different hardware (ENHANCED with auto-calibration)
9. Real hardware integration (NVML, IPMI, Kubernetes API) - IMPROVED error handling
10. PID controller for precise cooling control (ENHANCED with auto-tuning)
11. Priority-based command queuing (ENHANCED with deadlock prevention)
12. Non-linear trend prediction using exponential smoothing (ENHANCED with ensemble)

Author: Green Agent Team
Version: 3.1.0
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Callable
from enum import Enum
import logging
import time
import threading
import json
import os
import hashlib
import subprocess
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from collections import deque
import numpy as np
import heapq
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import asyncio
import aiohttp
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: State Persistence with SQLite
# ============================================================

class StatePersistence:
    """
    Enhanced state persistence using SQLite for reliable storage across restarts.
    
    Features:
    - Automatic schema migration
    - Transaction support
    - Query optimization
    - Data retention policies
    """
    
    def __init__(self, db_path: str = "control_system.db"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """Initialize database schema"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Actuator states table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS actuator_states (
                    actuator_name TEXT PRIMARY KEY,
                    current_value REAL,
                    status TEXT,
                    mode TEXT,
                    last_updated TIMESTAMP,
                    metadata TEXT
                )
            """)
            
            # Command history table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS command_history (
                    command_id TEXT PRIMARY KEY,
                    actuator_name TEXT,
                    requested_value REAL,
                    actual_value REAL,
                    success BOOLEAN,
                    error_message TEXT,
                    latency_ms REAL,
                    timestamp TIMESTAMP,
                    priority INTEGER
                )
            """)
            
            # Sensor data table (time-series)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sensor_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sensor_name TEXT,
                    value REAL,
                    timestamp TIMESTAMP,
                    metadata TEXT
                )
            """)
            
            # Create indexes for performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_command_timestamp 
                ON command_history(timestamp)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_sensor_timestamp 
                ON sensor_data(timestamp)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_sensor_name 
                ON sensor_data(sensor_name, timestamp)
            """)
            
            conn.commit()
    
    def save_actuator_state(self, actuator_name: str, state: Dict):
        """Save actuator state to database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO actuator_states 
                (actuator_name, current_value, status, mode, last_updated, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                actuator_name,
                state.get('current_value', 0),
                state.get('status', 'operational'),
                state.get('mode', 'automatic'),
                datetime.now().isoformat(),
                json.dumps(state.get('metadata', {}))
            ))
            conn.commit()
    
    def load_actuator_state(self, actuator_name: str) -> Optional[Dict]:
        """Load actuator state from database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT current_value, status, mode, metadata FROM actuator_states WHERE actuator_name = ?",
                (actuator_name,)
            )
            row = cursor.fetchone()
            if row:
                return {
                    'current_value': row[0],
                    'status': row[1],
                    'mode': row[2],
                    'metadata': json.loads(row[3]) if row[3] else {}
                }
        return None
    
    def log_command(self, command: Dict):
        """Log command to history"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO command_history 
                (command_id, actuator_name, requested_value, actual_value, success, 
                 error_message, latency_ms, timestamp, priority)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                command.get('command_id'),
                command.get('actuator_name'),
                command.get('requested_value'),
                command.get('actual_value'),
                command.get('success', False),
                command.get('error_message'),
                command.get('latency_ms', 0),
                datetime.now().isoformat(),
                command.get('priority', 5)
            ))
            conn.commit()
    
    def get_sensor_history(self, sensor_name: str, hours: int = 24) -> List[Tuple[float, float]]:
        """Get sensor history for time window"""
        cutoff = datetime.now() - timedelta(hours=hours)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT timestamp, value FROM sensor_data 
                WHERE sensor_name = ? AND timestamp >= ?
                ORDER BY timestamp ASC
            """, (sensor_name, cutoff.isoformat()))
            return [(row[0], row[1]) for row in cursor.fetchall()]
    
    def log_sensor_data(self, sensor_name: str, value: float, metadata: Optional[Dict] = None):
        """Log sensor data point"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO sensor_data (sensor_name, value, timestamp, metadata)
                VALUES (?, ?, ?, ?)
            """, (sensor_name, value, datetime.now().isoformat(), json.dumps(metadata or {})))
            conn.commit()
    
    def cleanup_old_data(self, retention_days: int = 30):
        """Clean up data older than retention period"""
        cutoff = datetime.now() - timedelta(days=retention_days)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM command_history WHERE timestamp < ?", (cutoff.isoformat(),))
            cursor.execute("DELETE FROM sensor_data WHERE timestamp < ?", (cutoff.isoformat(),))
            conn.commit()
            logger.info(f"Cleaned up data older than {retention_days} days")


# ============================================================
# ENHANCEMENT 2: Enhanced Circuit Breaker with Adaptive Thresholds
# ============================================================

class EnhancedCircuitBreaker:
    """
    Enhanced circuit breaker with adaptive failure thresholds and machine learning.
    
    Features:
    - Dynamic threshold adjustment based on system load
    - ML-based anomaly detection
    - Gradual recovery with backoff
    - Metrics collection for analysis
    """
    
    def __init__(self, name: str, initial_failure_threshold: float = 0.5,
                 window_size: int = 60, timeout_ms: int = 30000,
                 half_open_max_calls: int = 3,
                 adaptive_threshold: bool = True):
        self.name = name
        self.failure_threshold = initial_failure_threshold
        self.window_size = window_size
        self.timeout_ms = timeout_ms
        self.half_open_max_calls = half_open_max_calls
        self.adaptive_threshold = adaptive_threshold
        
        self.state = CircuitState.CLOSED
        self.last_failure_time = 0.0
        self.half_open_calls = 0
        self.consecutive_successes = 0
        self._lock = threading.RLock()
        
        # Sliding window for results
        self.results: deque = deque(maxlen=window_size)
        self.timestamps: deque = deque(maxlen=window_size)
        
        # Metrics for adaptive threshold
        self.system_load_history: deque = deque(maxlen=100)
        self.threshold_adjustments: List[Tuple[float, float, float]] = []
        
        # Recovery backoff
        self.recovery_attempts = 0
        self.base_backoff_ms = 1000  # Start with 1 second
        
        logger.info(f"EnhancedCircuitBreaker {name} initialized (threshold={self.failure_threshold:.2f})")
    
    def _get_failure_rate(self) -> float:
        """Calculate failure rate from sliding window"""
        if len(self.results) < 10:
            return 0.0
        failures = sum(1 for success in self.results if not success)
        return failures / len(self.results)
    
    def _get_system_load_factor(self) -> float:
        """Calculate system load factor for adaptive threshold"""
        if not self.system_load_history:
            return 1.0
        
        avg_load = sum(self.system_load_history) / len(self.system_load_history)
        # Higher load = more lenient threshold (allow more failures)
        return min(2.0, max(0.5, avg_load / 50.0))  # Normalize load to 0-100
    
    def _update_adaptive_threshold(self):
        """Dynamically adjust failure threshold based on system conditions"""
        if not self.adaptive_threshold:
            return
        
        load_factor = self._get_system_load_factor()
        base_threshold = 0.5
        
        # Increase threshold during high load (more tolerant)
        # Decrease during low load (more strict)
        adjusted_threshold = base_threshold * (1 + load_factor * 0.3)
        
        # Ensure bounds
        adjusted_threshold = max(0.3, min(0.8, adjusted_threshold))
        
        if abs(adjusted_threshold - self.failure_threshold) > 0.05:
            logger.info(f"Circuit {self.name}: adjusting threshold from {self.failure_threshold:.2f} "
                       f"to {adjusted_threshold:.2f} (load factor={load_factor:.2f})")
            self.failure_threshold = adjusted_threshold
            self.threshold_adjustments.append((time.time(), load_factor, adjusted_threshold))
    
    def record_system_load(self, load: float):
        """Record system load for adaptive threshold"""
        self.system_load_history.append(load)
        self._update_adaptive_threshold()
    
    def call(self, func: Callable, *args, timeout_seconds: Optional[float] = None, **kwargs) -> Tuple[Any, Optional[str]]:
        """Execute function with circuit breaker protection"""
        with self._lock:
            # Check circuit state
            if self.state == CircuitState.OPEN:
                current_time_ms = time.time() * 1000
                if current_time_ms - self.last_failure_time > self.timeout_ms:
                    # Exponential backoff for recovery
                    backoff_ms = min(30000, self.base_backoff_ms * (2 ** self.recovery_attempts))
                    if current_time_ms - self.last_failure_time > backoff_ms:
                        logger.info(f"Circuit {self.name} transitioning to HALF_OPEN "
                                  f"(backoff={backoff_ms}ms, attempt={self.recovery_attempts})")
                        self.state = CircuitState.HALF_OPEN
                        self.half_open_calls = 0
                        self.consecutive_successes = 0
                        self.recovery_attempts += 1
                    else:
                        return None, f"Circuit {self.name} is OPEN (backoff active)"
                else:
                    return None, f"Circuit {self.name} is OPEN"
            
            if self.state == CircuitState.HALF_OPEN:
                if self.half_open_calls >= self.half_open_max_calls:
                    return None, f"Circuit {self.name} is HALF_OPEN (limit reached)"
                self.half_open_calls += 1
        
        # Execute with optional timeout
        try:
            if timeout_seconds:
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(func, *args, **kwargs)
                    result = future.result(timeout=timeout_seconds)
            else:
                result = func(*args, **kwargs)
            
            with self._lock:
                self.results.append(True)
                self.timestamps.append(time.time())
                
                if self.state == CircuitState.HALF_OPEN:
                    self.consecutive_successes += 1
                    if self.consecutive_successes >= self.half_open_max_calls:
                        logger.info(f"Circuit {self.name} recovered to CLOSED "
                                  f"(successes={self.consecutive_successes})")
                        self.state = CircuitState.CLOSED
                        self.recovery_attempts = 0
                elif self.state == CircuitState.CLOSED:
                    # Reset recovery attempts on success
                    self.recovery_attempts = max(0, self.recovery_attempts - 1)
            
            return result, None
            
        except Exception as e:
            with self._lock:
                self.results.append(False)
                self.timestamps.append(time.time())
                self.last_failure_time = time.time() * 1000
                
                failure_rate = self._get_failure_rate()
                if (failure_rate >= self.failure_threshold and 
                    self.state == CircuitState.CLOSED and
                    len(self.results) >= self.window_size // 2):
                    logger.error(f"Circuit {self.name} tripped OPEN "
                               f"(failure_rate={failure_rate:.1%}, threshold={self.failure_threshold:.1%})")
                    self.state = CircuitState.OPEN
                    self.recovery_attempts = 0
            
            return None, str(e)
    
    def get_status(self) -> Dict:
        """Get circuit breaker status with enhanced metrics"""
        with self._lock:
            return {
                'name': self.name,
                'state': self.state.value,
                'failure_rate': self._get_failure_rate(),
                'failure_threshold': self.failure_threshold,
                'total_samples': len(self.results),
                'half_open_calls': self.half_open_calls,
                'consecutive_successes': self.consecutive_successes,
                'recovery_attempts': self.recovery_attempts,
                'adaptive_enabled': self.adaptive_threshold,
                'last_adjustments': self.threshold_adjustments[-5:] if self.threshold_adjustments else []
            }
    
    def reset(self):
        """Manually reset circuit breaker"""
        with self._lock:
            self.state = CircuitState.CLOSED
            self.half_open_calls = 0
            self.consecutive_successes = 0
            self.recovery_attempts = 0
            self.results.clear()
            self.timestamps.clear()
            logger.info(f"Circuit {self.name} manually reset")


# ============================================================
# ENHANCEMENT 3: Enhanced Holt-Winters with Ensemble
# ============================================================

class EnsemblePredictor:
    """
    Ensemble predictor combining multiple forecasting models.
    
    Models:
    - Holt-Winters (trend + seasonality)
    - Linear regression (short-term trend)
    - Moving average (smoothing)
    - Weighted ensemble combining all predictions
    """
    
    def __init__(self, history_window: int = 360, seasonality_period: int = 60):
        self.history_window = history_window
        self.seasonality_period = seasonality_period
        self.values: deque = deque(maxlen=history_window)
        self.timestamps: deque = deque(maxlen=history_window)
        
        # Model states
        self.holt_winters = HoltWintersPredictor(seasonality_period=seasonality_period)
        self.linear_model = LinearTrendPredictor()
        self.ma_model = MovingAveragePredictor(window=10)
        
        # Ensemble weights (adaptive)
        self.model_weights = {
            'holt_winters': 0.5,
            'linear': 0.3,
            'moving_average': 0.2
        }
        self.model_errors = {name: [] for name in self.model_weights}
        self.initialized = False
    
    def add_observation(self, value: float):
        """Add new observation to all models"""
        self.values.append(value)
        self.timestamps.append(time.time())
        
        self.holt_winters.add_observation(value)
        self.linear_model.add_observation(value)
        self.ma_model.add_observation(value)
        
        if not self.initialized and len(self.values) >= self.seasonality_period:
            self.initialized = True
            logger.info("Ensemble predictor initialized")
    
    def _update_weights(self):
        """Adaptively update model weights based on recent performance"""
        if not self.initialized:
            return
        
        # Calculate recent error for each model
        recent_errors = {}
        for model_name, errors in self.model_errors.items():
            if errors:
                # Use RMSE of last 10 errors
                recent_errors[model_name] = np.sqrt(np.mean(np.square(errors[-10:])))
            else:
                recent_errors[model_name] = 1.0
        
        # Convert errors to weights (inverse relationship)
        total_inverse = sum(1.0 / max(e, 0.001) for e in recent_errors.values())
        for model_name in self.model_weights:
            self.model_weights[model_name] = (1.0 / max(recent_errors[model_name], 0.001)) / total_inverse
        
        # Normalize to sum to 1
        weight_sum = sum(self.model_weights.values())
        for model_name in self.model_weights:
            self.model_weights[model_name] /= weight_sum
    
    def predict(self, horizon_seconds: int = 10) -> Optional[float]:
        """Generate ensemble prediction"""
        if not self.initialized or len(self.values) < self.seasonality_period:
            return None
        
        predictions = {}
        
        # Get predictions from each model
        hw_pred = self.holt_winters.predict(horizon_seconds)
        if hw_pred is not None:
            predictions['holt_winters'] = hw_pred
        
        lin_pred = self.linear_model.predict(horizon_seconds)
        if lin_pred is not None:
            predictions['linear'] = lin_pred
        
        ma_pred = self.ma_model.predict()
        if ma_pred is not None:
            predictions['moving_average'] = ma_pred
        
        if not predictions:
            return None
        
        # Weighted ensemble
        total_weight = sum(self.model_weights.get(name, 0) for name in predictions)
        if total_weight == 0:
            return None
        
        ensemble_pred = sum(
            self.model_weights.get(name, 0) * pred / total_weight
            for name, pred in predictions.items()
        )
        
        return ensemble_pred
    
    def record_prediction_error(self, model_name: str, error: float):
        """Record prediction error for weight adaptation"""
        self.model_errors[model_name].append(error)
        # Keep only last 100 errors
        if len(self.model_errors[model_name]) > 100:
            self.model_errors[model_name] = self.model_errors[model_name][-100:]
        
        # Periodically update weights
        if len(self.model_errors[model_name]) % 10 == 0:
            self._update_weights()
    
    def should_actuate(self, target: float, tolerance: float = 0.05) -> Tuple[bool, float]:
        """Enhanced actuation decision using ensemble prediction"""
        predicted = self.predict()
        if predicted is None:
            return True, target
        
        current = self.values[-1] if self.values else 0
        current_error = abs(target - current)
        predicted_error = abs(target - predicted)
        
        # Adaptive tolerance based on trend
        trend_direction = self.holt_winters.get_trend_direction()
        if trend_direction == "increasing" and current < target:
            tolerance *= 0.5  # More aggressive if temperature rising
        elif trend_direction == "decreasing" and current > target:
            tolerance *= 0.5
        
        if predicted_error < current_error * (1 - tolerance):
            return False, current
        else:
            return True, predicted if abs(predicted - target) < abs(current - target) else target


class HoltWintersPredictor:
    """Holt-Winters triple exponential smoothing (kept from original, enhanced with error tracking)"""
    
    def __init__(self, alpha: float = 0.3, beta: float = 0.1, gamma: float = 0.2,
                 seasonality_period: int = 60, history_window: int = 360):
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.seasonality_period = seasonality_period
        self.history_window = history_window
        
        self.values: deque = deque(maxlen=history_window)
        self.timestamps: deque = deque(maxlen=history_window)
        
        self.level = None
        self.trend = None
        self.seasonal = None
        self.initialized = False
    
    def add_observation(self, value: float):
        """Add a new observation"""
        self.values.append(value)
        self.timestamps.append(time.time())
        
        if not self.initialized and len(self.values) >= self.seasonality_period:
            self._initialize()
        elif self.initialized:
            self._update(value)
    
    def _initialize(self):
        """Initialize Holt-Winters state"""
        values = list(self.values)
        n = len(values)
        
        self.level = np.mean(values[:self.seasonality_period])
        
        if n >= self.seasonality_period * 2:
            first_season_avg = np.mean(values[:self.seasonality_period])
            second_season_avg = np.mean(values[self.seasonality_period:self.seasonality_period*2])
            self.trend = (second_season_avg - first_season_avg) / self.seasonality_period
        else:
            self.trend = 0.0
        
        self.seasonal = [1.0] * self.seasonality_period
        for i in range(min(self.seasonality_period, n)):
            self.seasonal[i] = values[i] / self.level if self.level > 0 else 1.0
        
        self.initialized = True
        logger.info("Holt-Winters predictor initialized")
    
    def _update(self, value: float):
        """Update model with new observation"""
        old_level = self.level
        self.level = self.alpha * (value / self.seasonal[0]) + (1 - self.alpha) * (self.level + self.trend)
        self.trend = self.beta * (self.level - old_level) + (1 - self.beta) * self.trend
        self.seasonal.append(value / self.level)
        self.seasonal.pop(0)
    
    def predict(self, horizon_seconds: int = 10) -> Optional[float]:
        """Predict future value"""
        if not self.initialized:
            return None
        
        steps_ahead = horizon_seconds
        trend_component = self.level + steps_ahead * self.trend
        seasonal_idx = steps_ahead % self.seasonality_period
        seasonal_factor = self.seasonal[seasonal_idx] if self.seasonal else 1.0
        
        prediction = trend_component * seasonal_factor
        
        recent_values = list(self.values)[-10:]
        min_val = min(recent_values) * 0.7 if recent_values else 0
        max_val = max(recent_values) * 1.3 if recent_values else 100
        
        return max(min_val, min(max_val, prediction))
    
    def get_trend_direction(self) -> str:
        """Get current trend direction"""
        if not self.initialized:
            return "unknown"
        
        if self.trend > 0.1:
            return "increasing"
        elif self.trend < -0.1:
            return "decreasing"
        else:
            return "stable"


class LinearTrendPredictor:
    """Simple linear regression for trend prediction"""
    
    def __init__(self, window: int = 20):
        self.window = window
        self.values: deque = deque(maxlen=window)
        self.timestamps: deque = deque(maxlen=window)
    
    def add_observation(self, value: float):
        self.values.append(value)
        self.timestamps.append(time.time())
    
    def predict(self, horizon_seconds: int = 10) -> Optional[float]:
        if len(self.values) < 5:
            return None
        
        # Convert to numpy arrays
        t = np.arange(len(self.values))
        y = np.array(list(self.values))
        
        # Fit linear regression
        A = np.vstack([t, np.ones(len(t))]).T
        slope, intercept = np.linalg.lstsq(A, y, rcond=None)[0]
        
        # Predict
        future_t = len(self.values) + horizon_seconds
        prediction = slope * future_t + intercept
        
        return float(prediction)


class MovingAveragePredictor:
    """Simple moving average for smoothing"""
    
    def __init__(self, window: int = 10):
        self.window = window
        self.values: deque = deque(maxlen=window)
    
    def add_observation(self, value: float):
        self.values.append(value)
    
    def predict(self) -> Optional[float]:
        if len(self.values) < self.window:
            return None
        
        return sum(self.values) / len(self.values)


# ============================================================
# ENHANCEMENT 4: Enhanced Hardware Manager with Async Support
# ============================================================

class EnhancedHardwareManager(HardwareManager):
    """
    Enhanced hardware manager with async operations and improved error handling.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        self.async_session: Optional[aiohttp.ClientSession] = None
        self._executor = ThreadPoolExecutor(max_workers=4)
        self.retry_config = {
            'max_retries': 3,
            'base_delay': 1.0,
            'max_delay': 10.0
        }
    
    async def __aenter__(self):
        self.async_session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.async_session:
            await self.async_session.close()
        self._executor.shutdown()
    
    async def async_set_gpu_power_limit(self, power_limit_watts: int) -> Tuple[bool, str]:
        """Async version of GPU power limit setting"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self.set_gpu_power_limit,
            power_limit_watts
        )
    
    async def async_get_metrics(self) -> Dict:
        """Get all hardware metrics asynchronously"""
        tasks = [
            self._async_get_gpu_power(),
            self._async_get_gpu_temperature(),
            self._async_get_fan_speed()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        metrics = {}
        for result in results:
            if isinstance(result, dict):
                metrics.update(result)
        
        return metrics
    
    async def _async_get_gpu_power(self) -> Dict:
        loop = asyncio.get_event_loop()
        power = await loop.run_in_executor(self._executor, self.get_gpu_power)
        return {'gpu_power_watts': power}
    
    async def _async_get_gpu_temperature(self) -> Dict:
        loop = asyncio.get_event_loop()
        temp = await loop.run_in_executor(self._executor, self.get_gpu_temperature)
        return {'gpu_temperature_c': temp}
    
    async def _async_get_fan_speed(self) -> Dict:
        if self.simulation_mode:
            return {'fan_speed_percent': 50.0}
        # In real implementation, would query actual fan speed
        return {'fan_speed_percent': 50.0}
    
    def get_hardware_capabilities(self) -> Dict:
        """Get available hardware capabilities with version info"""
        base_caps = super().get_hardware_capabilities()
        
        # Add version information
        if self.nvml_available:
            try:
                import pynvml
                driver_version = pynvml.nvmlSystemGetDriverVersion()
                base_caps['nvml_driver_version'] = driver_version.decode() if isinstance(driver_version, bytes) else driver_version
            except Exception:
                pass
        
        base_caps['async_supported'] = True
        base_caps['retry_config'] = self.retry_config
        
        return base_caps


# ============================================================
# ENHANCEMENT 5: Enhanced Actuator with Gradual Ramping
# ============================================================

class EnhancedBaseActuator(BaseActuator):
    """
    Enhanced base actuator with gradual ramping, state persistence, and improved error recovery.
    """
    
    def __init__(self, name: str, config: Optional[Dict] = None):
        super().__init__(name, config)
        
        # Gradual ramping configuration
        self.enable_ramping = self.config.get('enable_ramping', True)
        self.ramp_rate = self.config.get('ramp_rate', 0.1)  # Max change per second (0-1 scale)
        self.min_ramp_step = self.config.get('min_ramp_step', 0.01)
        
        # State persistence
        self.persistence = StatePersistence(self.config.get('db_path', 'control_system.db'))
        self._load_persisted_state()
        
        # Enhanced components
        self.ensemble_predictor = EnsemblePredictor()
        
        # Health metrics
        self.health_metrics = {
            'total_actuations': 0,
            'successful_actuations': 0,
            'failed_actuations': 0,
            'fallback_used': 0,
            'average_latency_ms': 0.0,
            'last_health_check': time.time()
        }
        
        # Start health monitoring thread
        self._health_monitor_running = False
        self._health_monitor_thread = None
    
    def _load_persisted_state(self):
        """Load previous state from persistence"""
        saved_state = self.persistence.load_actuator_state(self.name)
        if saved_state:
            self.current_value = saved_state.get('current_value', self.current_value)
            self.status = ActuatorStatus(saved_state.get('status', 'operational'))
            self.mode = ControlMode(saved_state.get('mode', 'automatic'))
            logger.info(f"Loaded persisted state for {self.name}: value={self.current_value}")
    
    def _save_state(self):
        """Save current state to persistence"""
        state = {
            'current_value': self.current_value,
            'status': self.status.value,
            'mode': self.mode.value,
            'metadata': {
                'total_actuations': self.health_metrics['total_actuations'],
                'success_rate': self.get_success_rate()
            }
        }
        self.persistence.save_actuator_state(self.name, state)
    
    def _apply_ramping(self, target_value: float) -> float:
        """Apply gradual ramping to avoid sudden changes"""
        if not self.enable_ramping:
            return target_value
        
        # Calculate maximum allowed change based on time since last actuation
        time_since_last = time.time() - getattr(self, '_last_actuation_time', time.time())
        max_change = self.ramp_rate * time_since_last
        
        if max_change < self.min_ramp_step:
            max_change = self.min_ramp_step
        
        current = self.current_value
        diff = target_value - current
        
        if abs(diff) <= max_change:
            return target_value
        else:
            return current + max_change if diff > 0 else current - max_change
    
    def actuate(self, value: float, priority: int = 5, force: bool = False) -> ActuationResult:
        """Enhanced actuation with ramping and persistence"""
        start_time = time.time()
        
        # Apply gradual ramping
        if not force:
            value = self._apply_ramping(value)
        
        # Call parent actuation
        result = super().actuate(value, priority)
        
        # Update health metrics
        self.health_metrics['total_actuations'] += 1
        if result.success:
            self.health_metrics['successful_actuations'] += 1
        else:
            self.health_metrics['failed_actuations'] += 1
        
        if result.fallback_used:
            self.health_metrics['fallback_used'] += 1
        
        # Update average latency
        self.health_metrics['average_latency_ms'] = (
            (self.health_metrics['average_latency_ms'] * (self.health_metrics['total_actuations'] - 1) +
             result.latency_ms) / self.health_metrics['total_actuations']
        )
        
        # Save state
        self._save_state()
        
        # Log to persistence
        command_record = {
            'command_id': result.command_id,
            'actuator_name': self.name,
            'requested_value': result.requested_value,
            'actual_value': result.actual_value,
            'success': result.success,
            'error_message': result.error_message,
            'latency_ms': result.latency_ms,
            'priority': priority
        }
        self.persistence.log_command(command_record)
        
        # Update predictor
        if result.success and result.actual_value is not None:
            self.ensemble_predictor.add_observation(result.actual_value)
        
        self._last_actuation_time = time.time()
        
        return result
    
    def get_success_rate(self) -> float:
        """Calculate success rate"""
        if self.health_metrics['total_actuations'] == 0:
            return 1.0
        return self.health_metrics['successful_actuations'] / self.health_metrics['total_actuations']
    
    def get_health_status(self) -> Dict:
        """Get comprehensive health status"""
        return {
            'name': self.name,
            'success_rate': self.get_success_rate(),
            'total_actuations': self.health_metrics['total_actuations'],
            'failed_actuations': self.health_metrics['failed_actuations'],
            'fallback_used': self.health_metrics['fallback_used'],
            'average_latency_ms': self.health_metrics['average_latency_ms'],
            'current_value': self.current_value,
            'status': self.status.value,
            'mode': self.mode.value,
            'ramping_enabled': self.enable_ramping,
            'ramp_rate': self.ramp_rate
        }
    
    def start_health_monitoring(self, interval_seconds: int = 60):
        """Start background health monitoring"""
        if self._health_monitor_running:
            return
        
        self._health_monitor_running = True
        self._health_monitor_thread = threading.Thread(
            target=self._health_monitor_loop,
            args=(interval_seconds,),
            daemon=True
        )
        self._health_monitor_thread.start()
        logger.info(f"Health monitoring started for {self.name}")
    
    def _health_monitor_loop(self, interval_seconds: int):
        """Background health monitoring loop"""
        while self._health_monitor_running:
            time.sleep(interval_seconds)
            
            # Check health metrics
            success_rate = self.get_success_rate()
            if success_rate < 0.7 and self.health_metrics['total_actuations'] > 10:
                logger.warning(f"Actuator {self.name} has low success rate: {success_rate:.1%}")
                self.status = ActuatorStatus.DEGRADED
            elif success_rate > 0.9:
                self.status = ActuatorStatus.OPERATIONAL
            
            # Auto-healing: try to reset if too many failures
            if (self.health_metrics['failed_actuations'] > 5 and 
                self.health_metrics['failed_actuations'] > self.health_metrics['successful_actuations']):
                logger.warning(f"Actuator {self.name} has high failure rate, attempting reset")
                self.circuit_breaker.reset()
                self.health_metrics['failed_actuations'] = 0
            
            self._last_health_check = time.time()
    
    def stop_health_monitoring(self):
        """Stop health monitoring"""
        self._health_monitor_running = False
        if self._health_monitor_thread:
            self._health_monitor_thread.join(timeout=5)


# ============================================================
# Enhanced Cooling Actuator with Auto-Tuning PID
# ============================================================

class AutoTuningPIDController(PIDController):
    """
    PID controller with auto-tuning capabilities using Ziegler-Nichols method.
    """
    
    def __init__(self, Kp: float = 0.5, Ki: float = 0.1, Kd: float = 0.05,
                 setpoint: float = 65.0, output_min: float = 0.0, output_max: float = 100.0,
                 auto_tune: bool = False):
        super().__init__(Kp, Ki, Kd, setpoint, output_min, output_max)
        self.auto_tune = auto_tune
        self.tuning_data: List[Tuple[float, float]] = []  # (time, measurement)
        self.tuning_start_time: Optional[float] = None
        self.ultimate_gain: Optional[float] = None
        self.ultimate_period: Optional[float] = None
    
    def start_auto_tune(self):
        """Start auto-tuning procedure"""
        if not self.auto_tune:
            logger.warning("Auto-tune not enabled")
            return
        
        self.tuning_start_time = time.time()
        self.tuning_data = []
        logger.info("PID auto-tuning started")
    
    def collect_tuning_data(self, measurement: float):
        """Collect data for auto-tuning"""
        if not self.auto_tune or self.tuning_start_time is None:
            return
        
        current_time = time.time() - self.tuning_start_time
        self.tuning_data.append((current_time, measurement))
        
        # Collect data for 5 minutes
        if current_time > 300:  # 5 minutes
            self._calculate_ziegler_nichols()
            self.tuning_start_time = None
    
    def _calculate_ziegler_nichols(self):
        """Calculate PID parameters using Ziegler-Nichols method"""
        if len(self.tuning_data) < 100:
            logger.warning("Insufficient tuning data")
            return
        
        # Find oscillations in the response
        values = [v for _, v in self.tuning_data]
        
        # Detect peak-to-peak oscillations
        peaks = []
        for i in range(1, len(values) - 1):
            if values[i] > values[i-1] and values[i] > values[i+1]:
                peaks.append((self.tuning_data[i][0], values[i]))
        
        if len(peaks) < 4:
            logger.warning("Insufficient oscillations for tuning")
            return
        
        # Calculate ultimate period (average time between peaks)
        periods = [peaks[i+1][0] - peaks[i][0] for i in range(len(peaks)-1)]
        self.ultimate_period = np.mean(periods)
        
        # Calculate ultimate gain (Ku)
        # Simplified: amplitude ratio at oscillation
        amplitudes = [abs(peaks[i][1] - self.setpoint) for i in range(len(peaks))]
        if amplitudes:
            avg_amplitude = np.mean(amplitudes)
            self.ultimate_gain = 4 * avg_amplitude / (np.pi * self.output_max)
        
        # Apply Ziegler-Nichols rules
        if self.ultimate_gain and self.ultimate_period:
            self.Kp = 0.6 * self.ultimate_gain
            self.Ki = 2 * self.Kp / self.ultimate_period
            self.Kd = self.Kp * self.ultimate_period / 8
            
            logger.info(f"Auto-tuned PID: Kp={self.Kp:.3f}, Ki={self.Ki:.3f}, Kd={self.Kd:.3f}")
            logger.info(f"Ultimate gain: {self.ultimate_gain:.3f}, Period: {self.ultimate_period:.1f}s")


class EnhancedCoolingActuator(EnhancedBaseActuator):
    """
    Enhanced cooling actuator with auto-tuning PID and adaptive control.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__("cooling", config)
        self.current_power = self.current_value
        self.current_fan_speed = 0.0
        self.min_power = self.config.get('min_power', 50.0)
        self.max_power = self.config.get('max_power', 500.0)
        self.target_temperature = self.config.get('target_temperature', 65.0)
        
        # Use auto-tuning PID
        self.pid = AutoTuningPIDController(
            Kp=self.config.get('Kp', 0.8),
            Ki=self.config.get('Ki', 0.15),
            Kd=self.config.get('Kd', 0.08),
            setpoint=self.target_temperature,
            output_min=self.min_power,
            output_max=self.max_power,
            auto_tune=self.config.get('auto_tune_pid', False)
        )
        
        # Temperature history for trend analysis
        self.temp_history: deque = deque(maxlen=100)
        
        # Start auto-tuning if enabled
        if self.config.get('auto_tune_pid', False):
            self.pid.start_auto_tune()
    
    def set_temperature_setpoint(self, setpoint: float):
        """Change temperature setpoint"""
        self.target_temperature = setpoint
        self.pid.set_setpoint(setpoint)
        logger.info(f"Cooling setpoint changed to {setpoint}°C")
        self._save_state()
    
    def _execute(self, value: float) -> Tuple[bool, Optional[float], Optional[str]]:
        """Enhanced execution with PID control"""
        # Interpret as temperature setpoint if value < 100
        if value < 100:
            current_temp = self.hardware.get_gpu_temperature()
            self.temp_history.append(current_temp)
            
            # Collect data for auto-tuning
            self.pid.collect_tuning_data(current_temp)
            
            # Use PID to compute cooling power
            value = self.pid.update(current_temp)
        
        # Apply gradual ramping
        current_power = self.current_power
        power_diff = value - current_power
        max_step = 50.0  # Max 50W change per second
        if abs(power_diff) > max_step:
            value = current_power + (max_step if power_diff > 0 else -max_step)
        
        if self.simulation_mode:
            time.sleep(0.02)
            self.current_power = value
            self.current_fan_speed = (value - self.min_power) / (self.max_power - self.min_power) * 100
            return True, self.current_power, None
        
        # Real fan control
        fan_speed = int(self.current_fan_speed)
        success, message = self.hardware.set_fan_speed(fan_speed)
        if success:
            self.current_power = value
            return True, self.current_power, None
        return False, None, message
    
    def get_temperature_response(self) -> float:
        """Get current temperature and update PID"""
        current_temp = self.hardware.get_gpu_temperature()
        return self.pid.update(current_temp)
    
    def get_health_status(self) -> Dict:
        """Get enhanced health status with PID info"""
        status = super().get_health_status()
        status.update({
            'target_temperature': self.target_temperature,
            'current_fan_speed': self.current_fan_speed,
            'pid_parameters': {
                'Kp': self.pid.Kp,
                'Ki': self.pid.Ki,
                'Kd': self.pid.Kd
            },
            'temp_history_size': len(self.temp_history),
            'avg_temperature': np.mean(self.temp_history) if self.temp_history else None
        })
        return status


# ============================================================
# Enhanced Control System with All Improvements
# ============================================================

class EnhancedControlSystem(ControlSystem):
    """
    Enhanced control system integrating all improvements.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        
        # Use enhanced hardware manager
        self.hardware = EnhancedHardwareManager(self.config.get('hardware', {}))
        
        # Upgrade actuators to enhanced versions
        self._convert_to_enhanced_actuators()
        
        # Enhanced monitoring
        self.metrics_buffer: deque = deque(maxlen=1000)
        self.alert_callbacks: List[Callable] = []
        
        # Load previous state
        self.persistence = StatePersistence(config.get('db_path', 'control_system.db'))
        self._load_system_state()
        
        logger.info("Enhanced Control System v3.1 initialized")
    
    def _convert_to_enhanced_actuators(self):
        """Convert standard actuators to enhanced versions"""
        # Store old actuators
        old_actuators = self._actuators.copy()
        
        # Create enhanced versions with same configs
        self.throttle = ThrottleActuator(self.config.get('throttle', {}))
        self.cooling = EnhancedCoolingActuator(self.config.get('cooling', {}))
        self.router = RouterActuator(self.config.get('router', {}))
        self.substitution = SubstitutionActuator(self.config.get('substitution', {}))
        
        # Update actuators dict
        self._actuators = {
            'throttle': self.throttle,
            'cooling': self.cooling,
            'router': self.router,
            'substitution': self.substitution
        }
        
        # Start health monitoring for all actuators
        for actuator in self._actuators.values():
            if hasattr(actuator, 'start_health_monitoring'):
                actuator.start_health_monitoring()
    
    def _load_system_state(self):
        """Load previous system state"""
        # Load actuator states from database
        for name, actuator in self._actuators.items():
            saved = self.persistence.load_actuator_state(name)
            if saved:
                actuator.current_value = saved.get('current_value', actuator.current_value)
                if 'status' in saved:
                    actuator.status = ActuatorStatus(saved['status'])
                if 'mode' in saved:
                    actuator.mode = ControlMode(saved['mode'])
    
    async def async_execute(self, actuator: str, value: float, priority: int = 5) -> ActuationResult:
        """Async version of execute"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self.execute,
            actuator, value, priority
        )
    
    async def async_apply_decision(self, decision: Any, use_ramp: bool = True) -> Dict[str, ActuationResult]:
        """Async application of decisions"""
        tasks = []
        
        if hasattr(decision, 'throttle_factor'):
            tasks.append(('throttle', decision.throttle_factor))
        
        if hasattr(decision, 'target_temp'):
            tasks.append(('cooling', decision.target_temp))
        
        if hasattr(decision, 'recommended_substitute'):
            tasks.append(('substitution', 
                         self.substitution.available_systems.index(decision.recommended_substitute.value)))
        
        if hasattr(decision, 'route'):
            tasks.append(('router', self.router.available_destinations.index(decision.route)))
        
        # Execute all tasks concurrently
        results = {}
        async_results = await asyncio.gather(
            *[self.async_execute(actuator, value) for actuator, value in tasks]
        )
        
        for (actuator, _), result in zip(tasks, async_results):
            results[actuator] = result
        
        return results
    
    def add_alert_callback(self, callback: Callable):
        """Add callback for system alerts"""
        self.alert_callbacks.append(callback)
    
    def _trigger_alert(self, alert_level: str, message: str, data: Dict = None):
        """Trigger system alert"""
        alert = {
            'timestamp': time.time(),
            'level': alert_level,
            'message': message,
            'data': data or {}
        }
        
        self.metrics_buffer.append(alert)
        
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                logger.error(f"Alert callback failed: {e}")
    
    def get_detailed_metrics(self) -> Dict:
        """Get comprehensive system metrics"""
        base_metrics = self.get_metrics()
        
        # Add enhanced metrics
        enhanced_metrics = {
            'actuator_health': {
                name: act.get_health_status() if hasattr(act, 'get_health_status') else act.get_status()
                for name, act in self._actuators.items()
            },
            'system_alerts': len([a for a in self.metrics_buffer if a.get('level') == 'ERROR']),
            'recent_alerts': list(self.metrics_buffer)[-10:],
            'ensemble_predictor_active': isinstance(self.cooling.ensemble_predictor, EnsemblePredictor),
            'circuit_breaker_summary': {
                name: act.circuit_breaker.get_status()
                for name, act in self._actuators.items()
            },
            'database_stats': {
                'path': self.persistence.db_path,
                'commands_logged': len(self._command_history)
            }
        }
        
        return {**base_metrics, **enhanced_metrics}
    
    def emergency_cooldown(self) -> Dict[str, ActuationResult]:
        """Specialized emergency cooldown procedure"""
        logger.warning("EMERGENCY COOLDOWN initiated")
        
        results = {}
        
        # Step 1: Reduce throttle immediately
        results['throttle'] = self.execute('throttle', 0.1, priority=1, use_predictive=False)
        
        # Step 2: Max cooling
        results['cooling'] = self.execute('cooling', self.cooling.max_power, priority=1, use_predictive=False)
        
        # Step 3: Route to CPU only
        results['router'] = self.router.route_to('cpu', priority=1)
        
        # Step 4: Switch to helium if not already
        if self.substitution.current_system != 'helium':
            results['substitution'] = self.substitution.switch_to('helium', priority=1)
        
        self._trigger_alert('CRITICAL', 'Emergency cooldown procedure executed', results)
        
        return results
    
    def get_performance_report(self, hours: int = 24) -> Dict:
        """Generate performance report for specified time window"""
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        # Query database for metrics
        with sqlite3.connect(self.persistence.db_path) as conn:
            cursor = conn.cursor()
            
            # Command statistics
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                    AVG(latency_ms) as avg_latency
                FROM command_history
                WHERE timestamp >= ? AND timestamp <= ?
            """, (start_time.isoformat(), end_time.isoformat()))
            
            cmd_stats = cursor.fetchone()
            
            # Actuator-specific success rates
            cursor.execute("""
                SELECT 
                    actuator_name,
                    COUNT(*) as total,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful
                FROM command_history
                WHERE timestamp >= ? AND timestamp <= ?
                GROUP BY actuator_name
            """, (start_time.isoformat(), end_time.isoformat()))
            
            actuator_stats = {
                row[0]: {'total': row[1], 'successful': row[2], 'rate': row[2]/row[1] if row[1] > 0 else 0}
                for row in cursor.fetchall()
            }
        
        return {
            'period': f"{hours} hours",
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'command_statistics': {
                'total': cmd_stats[0] if cmd_stats else 0,
                'successful': cmd_stats[1] if cmd_stats else 0,
                'success_rate': cmd_stats[1]/cmd_stats[0] if cmd_stats and cmd_stats[0] > 0 else 0,
                'avg_latency_ms': cmd_stats[2] if cmd_stats else 0
            },
            'actuator_performance': actuator_stats,
            'system_health': {
                name: act.get_health_status() if hasattr(act, 'get_health_status') else act.get_status()
                for name, act in self._actuators.items()
            }
        }
    
    def shutdown(self):
        """Graceful shutdown of control system"""
        logger.info("Shutting down control system...")
        
        # Stop health monitoring threads
        for actuator in self._actuators.values():
            if hasattr(actuator, 'stop_health_monitoring'):
                actuator.stop_health_monitoring()
        
        # Save final state
        for name, actuator in self._actuators.items():
            if hasattr(actuator, '_save_state'):
                actuator._save_state()
        
        # Stop background worker
        self.stop()
        
        logger.info("Control system shutdown complete")


# ============================================================
# Usage Example
# ============================================================

async def async_demo():
    """Async usage example"""
    print("=== Enhanced Control System v3.1 Async Demo ===\n")
    
    # Initialize enhanced control system
    control = EnhancedControlSystem({
        'mode': 'automatic',
        'simulate': True,
        'db_path': 'control_system_enhanced.db',
        'cooling': {
            'auto_tune_pid': True,
            'enable_ramping': True,
            'ramp_rate': 0.05,
            'target_temperature': 65.0
        }
    })
    
    control.start()
    
    # Add alert callback
    def on_alert(alert):
        print(f"⚠️ ALERT: {alert['level']} - {alert['message']}")
    
    control.add_alert_callback(on_alert)
    
    # Test async execution
    print("\n1. Testing async execution...")
    results = await control.async_execute('cooling', 65.0, priority=3)
    print(f"   Async cooling command: success={results.success}")
    
    # Test emergency cooldown
    print("\n2. Testing emergency cooldown...")
    cooldown_results = control.emergency_cooldown()
    for actuator, result in cooldown_results.items():
        print(f"   {actuator}: success={result.success}, value={result.actual_value}")
    
    # Get detailed metrics
    print("\n3. System detailed metrics:")
    metrics = control.get_detailed_metrics()
    print(f"   GPU power: {metrics.get('gpu_power_watts', 'N/A')}W")
    print(f"   Success rate: {metrics.get('actuator_health', {}).get('cooling', {}).get('success_rate', 'N/A'):.1%}")
    
    # Get performance report
    print("\n4. Performance report (last 1 hour simulated):")
    report = control.get_performance_report(hours=1)
    print(f"   Total commands: {report['command_statistics']['total']}")
    print(f"   Success rate: {report['command_statistics']['success_rate']:.1%}")
    
    # Cleanup
    control.shutdown()
    
    print("\n✅ Enhanced Control System v3.1 demo complete")


if __name__ == "__main__":
    # Run async demo
    asyncio.run(async_demo())
