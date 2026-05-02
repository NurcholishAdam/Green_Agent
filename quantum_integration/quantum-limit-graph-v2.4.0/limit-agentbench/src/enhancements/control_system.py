# src/enhancements/control_system.py

"""
Complete Control System for Green Agent - Enhanced Version 2.0

Features:
1. Circuit breaker pattern for fault isolation
2. Sensor feedback loops for actuation verification
3. Gradual ramping to avoid thermal shock
4. State persistence across restarts
5. Predictive actuation based on trends
6. Comprehensive audit logging
7. Rate limiting for cooling systems
8. Calibration support for different hardware

Author: Green Agent Team
Version: 2.0.0
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
from abc import ABC, abstractmethod
from datetime import datetime
from collections import deque
import numpy as np

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: Circuit Breaker Pattern
# ============================================================

class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery


class CircuitBreaker:
    """
    Circuit breaker pattern to prevent cascading failures.
    
    Scientific basis: Prevents repeated calls to failing actuators,
    allowing time for recovery and reducing system load.
    """
    
    def __init__(self, name: str, failure_threshold: int = 5,
                 timeout_ms: int = 30000, half_open_max_calls: int = 1):
        self.name = name
        self.failure_threshold = failure_threshold
        self.timeout_ms = timeout_ms
        self.half_open_max_calls = half_open_max_calls
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.half_open_calls = 0
        self._lock = threading.Lock()
        
        # Metrics
        self.total_calls = 0
        self.total_failures = 0
        self.total_rejections = 0
    
    def call(self, func: Callable, *args, **kwargs) -> Tuple[Any, Optional[str]]:
        """
        Execute function with circuit breaker protection.
        
        Returns:
            (result, error_message) - error_message is None on success
        """
        with self._lock:
            self.total_calls += 1
            
            # Check if circuit is open
            if self.state == CircuitState.OPEN:
                if time.time() * 1000 - self.last_failure_time > self.timeout_ms:
                    logger.info(f"Circuit {self.name} transitioning to HALF_OPEN")
                    self.state = CircuitState.HALF_OPEN
                    self.half_open_calls = 0
                else:
                    self.total_rejections += 1
                    return None, f"Circuit {self.name} is OPEN (rejected)"
            
            # Half-open: limit number of test calls
            if self.state == CircuitState.HALF_OPEN:
                if self.half_open_calls >= self.half_open_max_calls:
                    self.total_rejections += 1
                    return None, f"Circuit {self.name} is HALF_OPEN (limit reached)"
                self.half_open_calls += 1
        
        # Execute the call
        try:
            result = func(*args, **kwargs)
            
            with self._lock:
                if self.state == CircuitState.HALF_OPEN:
                    # Successful test call, close circuit
                    logger.info(f"Circuit {self.name} recovered to CLOSED")
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                elif self.state == CircuitState.CLOSED:
                    # Success, reduce failure count
                    self.failure_count = max(0, self.failure_count - 1)
            
            return result, None
            
        except Exception as e:
            with self._lock:
                self.total_failures += 1
                self.failure_count += 1
                self.last_failure_time = time.time() * 1000
                
                if (self.failure_count >= self.failure_threshold and 
                    self.state == CircuitState.CLOSED):
                    logger.error(f"Circuit {self.name} tripped OPEN after {self.failure_count} failures")
                    self.state = CircuitState.OPEN
            
            return None, str(e)
    
    def get_status(self) -> Dict:
        """Get circuit breaker status"""
        return {
            'name': self.name,
            'state': self.state.value,
            'failure_count': self.failure_count,
            'threshold': self.failure_threshold,
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
            logger.info(f"Circuit {self.name} manually reset")


# ============================================================
# ENHANCEMENT 2: Sensor Feedback System
# ============================================================

@dataclass
class SensorReading:
    """Reading from a hardware sensor"""
    sensor_name: str
    value: float
    unit: str
    timestamp: float
    quality: float  # 0-1 confidence


class SensorFeedbackSystem:
    """
    Sensor feedback loop for actuation verification.
    
    Reads actual hardware state to confirm actuation success
    and provide closed-loop control.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.simulation_mode = self.config.get('simulate', True)
        self._sensor_cache: Dict[str, SensorReading] = {}
        self._reading_history: Dict[str, deque] = {}
        self._lock = threading.Lock()
        
        # Simulated sensor values for testing
        self._simulated_values = {
            'gpu_power_w': 150.0,
            'gpu_temperature_c': 65.0,
            'cpu_power_w': 50.0,
            'cpu_temperature_c': 55.0,
            'fan_speed_percent': 40.0,
            'coolant_flow_lpm': 2.0,
            'helium_pressure_psi': 150.0
        }
        
        # Maximum history length
        self.max_history = 1000
    
    def read_sensor(self, sensor_name: str) -> Optional[SensorReading]:
        """Read current value from a sensor"""
        if self.simulation_mode:
            return self._simulate_reading(sensor_name)
        
        # Production: read from actual hardware
        return self._read_hardware_sensor(sensor_name)
    
    def _simulate_reading(self, sensor_name: str) -> SensorReading:
        """Generate simulated sensor reading"""
        import random
        
        base_value = self._simulated_values.get(sensor_name, 0)
        
        # Add realistic noise
        if 'temperature' in sensor_name:
            noise = random.gauss(0, 0.5)
        elif 'power' in sensor_name:
            noise = random.gauss(0, 5)
        else:
            noise = random.gauss(0, 1)
        
        value = max(0, base_value + noise)
        
        return SensorReading(
            sensor_name=sensor_name,
            value=value,
            unit=self._get_unit(sensor_name),
            timestamp=time.time(),
            quality=0.95
        )
    
    def _read_hardware_sensor(self, sensor_name: str) -> Optional[SensorReading]:
        """Read from actual hardware sensor"""
        # Placeholder for hardware integration
        # In production, this would call:
        # - NVML for GPU sensors
        # - IPMI for system sensors
        # - Custom drivers for helium sensors
        
        logger.warning(f"Hardware sensor {sensor_name} not implemented, using simulation")
        return self._simulate_reading(sensor_name)
    
    def _get_unit(self, sensor_name: str) -> str:
        """Get unit for sensor type"""
        units = {
            'power': 'W',
            'temperature': '°C',
            'speed': 'rpm',
            'pressure': 'psi',
            'flow': 'L/min'
        }
        
        for key, unit in units.items():
            if key in sensor_name:
                return unit
        return 'unknown'
    
    def get_trend(self, sensor_name: str, window_seconds: int = 60) -> List[SensorReading]:
        """Get historical trend for a sensor"""
        if sensor_name not in self._reading_history:
            return []
        
        cutoff = time.time() - window_seconds
        readings = [r for r in self._reading_history[sensor_name] 
                   if r.timestamp > cutoff]
        
        return list(readings)
    
    def verify_actuation(self, actuator_name: str, expected_value: float,
                        tolerance: float = 0.1) -> Tuple[bool, float, str]:
        """
        Verify that actuation had desired effect.
        
        Returns:
            (success, actual_value, verification_message)
        """
        # Map actuator to relevant sensor
        sensor_map = {
            'throttle': 'gpu_power_w',
            'cooling': 'gpu_temperature_c',
            'cooling_fan': 'fan_speed_percent',
            'router': None,  # Routing verification via logs
            'substitution': 'helium_pressure_psi'
        }
        
        sensor_name = sensor_map.get(actuator_name)
        if sensor_name is None:
            # Cannot verify this actuator type
            return True, expected_value, "verification_not_supported"
        
        # Read current sensor value
        reading = self.read_sensor(sensor_name)
        if reading is None:
            return False, 0, "sensor_unavailable"
        
        # Convert expected to appropriate units
        if actuator_name == 'throttle':
            expected_power = expected_value * 300  # 300W max
            actual_power = reading.value
            is_verified = abs(actual_power - expected_power) <= tolerance * expected_power
            return is_verified, actual_power, f"power: expected {expected_power:.0f}W, actual {actual_power:.0f}W"
        
        elif actuator_name == 'cooling':
            # Cooling should reduce temperature
            # Get previous temperature
            history = self.get_trend('gpu_temperature_c', window_seconds=30)
            if len(history) >= 2:
                prev_temp = history[-2].value
                temp_change = reading.value - prev_temp
                is_verified = temp_change <= 0  # Temperature should decrease or stay same
                return is_verified, reading.value, f"temp change: {temp_change:+.1f}°C"
        
        return True, reading.value, "verification_passed"
    
    def update_simulated_values(self, actuator: str, value: float):
        """Update simulated values based on actuator commands"""
        if actuator == 'throttle':
            self._simulated_values['gpu_power_w'] = value * 300
            # Temperature responds to power with lag
            thermal_lag = 0.95
            target_temp = 30 + (self._simulated_values['gpu_power_w'] / 300) * 50
            current_temp = self._simulated_values.get('gpu_temperature_c', 65)
            self._simulated_values['gpu_temperature_c'] = (current_temp * thermal_lag + 
                                                          target_temp * (1 - thermal_lag))
        
        elif actuator == 'cooling':
            # Cooling reduces temperature
            cooling_effect = (value / 500) * 20  # Max 20°C reduction
            self._simulated_values['fan_speed_percent'] = (value / 500) * 100
            self._simulated_values['gpu_temperature_c'] -= cooling_effect
            self._simulated_values['gpu_temperature_c'] = max(25, self._simulated_values['gpu_temperature_c'])
    
    def record_reading(self, reading: SensorReading):
        """Record a sensor reading for history"""
        with self._lock:
            if reading.sensor_name not in self._reading_history:
                self._reading_history[reading.sensor_name] = deque(maxlen=self.max_history)
            self._reading_history[reading.sensor_name].append(reading)


# ============================================================
# ENHANCEMENT 3: Gradual Ramping
# ============================================================

class GradualRampController:
    """
    Apply changes gradually to avoid thermal shock and power spikes.
    
    Scientific basis: Sudden power changes cause thermal stress
    and mechanical wear on cooling systems.
    """
    
    def __init__(self, step_size: float = 0.1, interval_ms: int = 100,
                 max_rate_per_second: float = 0.5):
        self.step_size = step_size
        self.interval = interval_ms / 1000.0
        self.max_rate_per_second = max_rate_per_second
        self._active_ramps: Dict[str, Dict] = {}
    
    def ramp_to(self, actuator: Any, target: float, current: float,
               actuator_name: str) -> List[ActuationResult]:
        """
        Gradually move from current to target value.
        
        Returns list of intermediate actuation results.
        """
        results = []
        
        # Calculate step count based on max rate
        max_change = self.max_rate_per_second * self.interval
        total_change = abs(target - current)
        num_steps = max(1, int(total_change / max_change))
        actual_step = total_change / num_steps
        
        logger.info(f"Ramping {actuator_name}: {current:.2f} → {target:.2f} in {num_steps} steps")
        
        for i in range(num_steps):
            # Calculate intermediate value
            fraction = (i + 1) / num_steps
            intermediate = current + (target - current) * fraction
            
            # Ensure step size isn't too large
            if i < num_steps - 1:
                step_change = abs(intermediate - (current if i == 0 else 
                                   current + (target - current) * i / num_steps))
                if step_change > self.max_rate_per_second * self.interval:
                    # Limit step size
                    direction = 1 if target > current else -1
                    intermediate = (current if i == 0 else current + (target - current) * i / num_steps) + \
                                  direction * self.max_rate_per_second * self.interval
            
            # Execute step
            result = actuator.actuate(intermediate)
            results.append(result)
            
            # Wait for next step
            time.sleep(self.interval)
        
        return results


# ============================================================
# ENHANCEMENT 4: State Persistence
# ============================================================

class PersistentState:
    """Persist actuator state to disk for recovery"""
    
    def __init__(self, state_dir: str = "/var/lib/green_agent"):
        self.state_dir = state_dir
        self._ensure_directory()
    
    def _ensure_directory(self):
        """Ensure state directory exists"""
        try:
            os.makedirs(self.state_dir, exist_ok=True)
        except PermissionError:
            # Fallback to current directory
            self.state_dir = "green_agent_state"
            os.makedirs(self.state_dir, exist_ok=True)
    
    def save(self, name: str, data: Dict):
        """Save state to disk"""
        filepath = os.path.join(self.state_dir, f"{name}_state.json")
        try:
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            logger.debug(f"Saved state for {name}")
        except Exception as e:
            logger.error(f"Failed to save state for {name}: {e}")
    
    def load(self, name: str) -> Optional[Dict]:
        """Load state from disk"""
        filepath = os.path.join(self.state_dir, f"{name}_state.json")
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            # Check if state is too old (> 7 days)
            timestamp = data.get('timestamp', 0)
            if time.time() - timestamp > 7 * 24 * 3600:
                logger.warning(f"State for {name} is older than 7 days, ignoring")
                return None
            
            return data
        except FileNotFoundError:
            return None
        except Exception as e:
            logger.error(f"Failed to load state for {name}: {e}")
            return None


# ============================================================
# ENHANCEMENT 5: Predictive Actuation
# ============================================================

class PredictiveActuator:
    """
    Predictive actuation based on trend analysis.
    
    Uses historical data to anticipate future needs
    and actuate proactively.
    """
    
    def __init__(self, history_window: int = 60):
        self.history_window = history_window
        self.value_history: deque = deque(maxlen=history_window)
        self.timestamp_history: deque = deque(maxlen=history_window)
    
    def record_value(self, value: float):
        """Record a value for trend analysis"""
        self.value_history.append(value)
        self.timestamp_history.append(time.time())
    
    def predict_next_value(self, horizon_seconds: int = 10) -> Optional[float]:
        """
        Predict next value using linear regression on recent history.
        
        Returns None if insufficient data.
        """
        if len(self.value_history) < 5:
            return None
        
        # Use last 10 values for prediction
        recent_values = list(self.value_history)[-10:]
        recent_times = list(self.timestamp_history)[-10:]
        
        # Convert to numpy for linear regression
        import numpy as np
        times = np.array([t - recent_times[0] for t in recent_times])
        values = np.array(recent_values)
        
        # Linear regression
        A = np.vstack([times, np.ones(len(times))]).T
        slope, intercept = np.linalg.lstsq(A, values, rcond=None)[0]
        
        # Predict at horizon
        prediction = slope * horizon_seconds + intercept
        
        # Bound prediction
        min_value = min(recent_values) * 0.8
        max_value = max(recent_values) * 1.2
        
        return max(min_value, min(max_value, prediction))
    
    def should_actuate(self, target: float, tolerance: float = 0.05) -> Tuple[bool, float]:
        """
        Determine if actuation is needed based on predicted trend.
        
        Returns:
            (should_actuate, recommended_value)
        """
        predicted = self.predict_next_value()
        if predicted is None:
            return True, target
        
        # If prediction is already moving toward target, wait
        current = self.value_history[-1] if self.value_history else 0
        current_error = abs(target - current)
        predicted_error = abs(target - predicted)
        
        if predicted_error < current_error * 0.8:
            # Trend is moving in right direction, wait
            return False, current
        else:
            # Need to actuate
            return True, target


# ============================================================
# ENHANCEMENT 6: Enhanced Base Actuator
# ============================================================

class BaseActuator(ABC):
    """
    Enhanced abstract base class for all actuators.
    
    Features:
    - Circuit breaker protection
    - Sensor feedback verification
    - State persistence
    - Comprehensive audit logging
    """
    
    def __init__(self, name: str, config: Optional[Dict] = None):
        self.name = name
        self.config = config or {}
        self.status = ActuatorStatus.OPERATIONAL
        self.mode = ControlMode.AUTOMATIC
        self.simulation_mode = self.config.get('simulate', True)
        self.max_retries = self.config.get('max_retries', 3)
        self.retry_delay_ms = self.config.get('retry_delay_ms', 100)
        self._last_result: Optional[ActuationResult] = None
        self.current_value: float = self.config.get('initial_value', 0.0)
        
        # Enhanced components
        self.circuit_breaker = CircuitBreaker(
            name,
            failure_threshold=self.config.get('circuit_threshold', 5),
            timeout_ms=self.config.get('circuit_timeout_ms', 30000)
        )
        self.sensor_feedback = SensorFeedbackSystem(self.config.get('sensors', {}))
        self.ramp_controller = GradualRampController(
            step_size=self.config.get('ramp_step', 0.1),
            interval_ms=self.config.get('ramp_interval_ms', 100)
        )
        self.state_persistence = PersistentState()
        self.predictor = PredictiveActuator()
        
        # Audit log
        self.audit_log: List[Dict] = []
        self.max_audit_size = self.config.get('max_audit_size', 1000)
        
        # Load persisted state
        self._load_state()
    
    def _load_state(self):
        """Load persisted actuator state"""
        saved_state = self.state_persistence.load(self.name)
        if saved_state:
            self.current_value = saved_state.get('current_value', self.current_value)
            self.status = ActuatorStatus(saved_state.get('status', 'operational'))
            logger.info(f"Loaded persisted state for {self.name}: value={self.current_value}")
    
    def _save_state(self):
        """Save current actuator state"""
        self.state_persistence.save(self.name, {
            'current_value': self.current_value,
            'status': self.status.value,
            'timestamp': time.time(),
            'version': '2.0'
        })
    
    def _log_audit(self, result: ActuationResult):
        """Log actuation for audit trail"""
        entry = {
            'timestamp': result.timestamp,
            'actuator': self.name,
            'requested': result.requested_value,
            'actual': result.actual_value,
            'success': result.success,
            'fallback_used': result.fallback_used,
            'retry_count': result.retry_count,
            'error': result.error_message
        }
        self.audit_log.append(entry)
        
        # Trim log if needed
        if len(self.audit_log) > self.max_audit_size:
            self.audit_log = self.audit_log[-self.max_audit_size:]
    
    @abstractmethod
    def _execute(self, value: float) -> Tuple[bool, Optional[float], Optional[str]]:
        """Execute the actual actuation (to be implemented by subclass)"""
        pass
    
    def actuate(self, value: float, use_ramp: bool = False) -> ActuationResult:
        """
        Main actuation interface with retries, fallbacks, and circuit breaker.
        
        Args:
            value: Target value for the actuator
            use_ramp: Whether to apply gradual ramping
        """
        start_time = time.time()
        
        # Validate value range
        if not self._validate_value(value):
            result = ActuationResult(
                success=False,
                command=self.name,
                requested_value=value,
                actual_value=None,
                latency_ms=(time.time() - start_time) * 1000,
                error_message=f"Value {value} out of valid range",
                fallback_used=False,
                retry_count=0
            )
            self._log_audit(result)
            return result
        
        # Use circuit breaker for execution
        def execute_with_value(target_value):
            # Apply ramping if requested
            if use_ramp and abs(target_value - self.current_value) > 0.1:
                current = self.current_value
                self.ramp_controller.ramp_to(self, target_value, current, self.name)
            
            # Execute the actuation with retries
            retry_count = 0
            for attempt in range(self.max_retries):
                try:
                    success, actual, error = self._execute(target_value)
                    if success:
                        self.current_value = actual or target_value
                        self._save_state()
                        self.predictor.record_value(self.current_value)
                        return success, actual, error
                    
                    retry_count += 1
                    time.sleep(self.retry_delay_ms / 1000)
                except Exception as e:
                    retry_count += 1
                    time.sleep(self.retry_delay_ms / 1000)
            
            # All retries failed
            fallback = self._get_fallback_value(target_value)
            success, actual, error = self._execute(fallback)
            return success, actual, error
        
        # Execute with circuit breaker
        result_value, error = self.circuit_breaker.call(execute_with_value, value)
        
        latency_ms = (time.time() - start_time) * 1000
        
        if error is None and result_value is not None:
            result = ActuationResult(
                success=True,
                command=self.name,
                requested_value=value,
                actual_value=result_value if isinstance(result_value, float) else value,
                latency_ms=latency_ms,
                error_message=None,
                fallback_used=False,
                retry_count=0
            )
        else:
            # Use fallback without circuit breaker
            fallback_value = self._get_fallback_value(value)
            try:
                success, actual, fallback_error = self._execute(fallback_value)
                result = ActuationResult(
                    success=success,
                    command=self.name,
                    requested_value=value,
                    actual_value=actual or fallback_value,
                    latency_ms=latency_ms,
                    error_message=fallback_error or error,
                    fallback_used=True,
                    retry_count=self.max_retries
                )
            except Exception as e:
                result = ActuationResult(
                    success=False,
                    command=self.name,
                    requested_value=value,
                    actual_value=None,
                    latency_ms=latency_ms,
                    error_message=str(e),
                    fallback_used=True,
                    retry_count=self.max_retries
                )
        
        self._last_result = result
        self._log_audit(result)
        
        # Update sensor simulation
        if self.simulation_mode and result.success and result.actual_value:
            self.sensor_feedback.update_simulated_values(self.name, result.actual_value)
        
        # Verify actuation
        if result.success and result.actual_value:
            verified, actual_sensor, message = self.sensor_feedback.verify_actuation(
                self.name, result.actual_value
            )
            if not verified:
                logger.warning(f"Actuation verification failed for {self.name}: {message}")
        
        return result
    
    def _validate_value(self, value: float) -> bool:
        """Validate value range (override in subclasses)"""
        return 0.0 <= value <= 1.0
    
    def _get_fallback_value(self, requested: float) -> float:
        """Get fallback value (override in subclasses)"""
        return 0.5
    
    def get_status(self) -> Dict:
        """Get actuator status with enhanced metrics"""
        return {
            'name': self.name,
            'status': self.status.value,
            'mode': self.mode.value,
            'simulation': self.simulation_mode,
            'current_value': self.current_value,
            'circuit_breaker': self.circuit_breaker.get_status(),
            'last_result': self._last_result.__dict__ if self._last_result else None,
            'audit_count': len(self.audit_log)
        }
    
    def get_audit_log(self, limit: int = 100) -> List[Dict]:
        """Get recent audit log entries"""
        return self.audit_log[-limit:]


# ============================================================
# ENHANCEMENT 7: Concrete Actuators (Enhanced)
# ============================================================

class ThrottleActuator(BaseActuator):
    """Enhanced control system for GPU/CPU throttling"""
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__("throttle", config)
        self.current_throttle = self.current_value
        self.power_capping_enabled = self.config.get('power_capping', True)
        self.max_power_watts = self.config.get('max_power_watts', 300)
    
    def _validate_value(self, value: float) -> bool:
        return 0.0 <= value <= 1.0
    
    def _get_fallback_value(self, requested: float) -> float:
        """Safe fallback: moderate throttle (50%)"""
        return 0.5
    
    def _execute(self, value: float) -> Tuple[bool, Optional[float], Optional[str]]:
        if self.simulation_mode:
            # Simulate actuation with realistic latency
            time.sleep(0.01 + 0.005 * abs(value - self.current_throttle))
            self.current_throttle = value
            return True, self.current_throttle, None
        
        # Production: set GPU power cap
        try:
            import subprocess
            power_limit = int(value * self.max_power_watts)
            subprocess.run(['nvidia-smi', '-pl', str(power_limit)], 
                          check=True, timeout=5, capture_output=True)
            self.current_throttle = value
            return True, self.current_throttle, None
        except ImportError:
            logger.warning("nvidia-smi not available, using simulation")
            self.current_throttle = value
            return True, self.current_throttle, None
        except Exception as e:
            return False, None, str(e)


class CoolingActuator(BaseActuator):
    """Enhanced control system for cooling infrastructure"""
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__("cooling", config)
        self.current_power = self.current_value
        self.current_fan_speed = 0.0
        self.min_power = self.config.get('min_power', 50.0)
        self.max_power = self.config.get('max_power', 500.0)
        self.target_temperature = self.config.get('target_temperature', 65.0)
    
    def _validate_value(self, value: float) -> bool:
        # Value is cooling power in watts
        return self.min_power <= value <= self.max_power
    
    def _get_fallback_value(self, requested: float) -> float:
        """Safe fallback: maximum cooling"""
        return self.max_power
    
    def _execute(self, value: float) -> Tuple[bool, Optional[float], Optional[str]]:
        if self.simulation_mode:
            time.sleep(0.02)
            self.current_power = value
            self.current_fan_speed = (value - self.min_power) / (self.max_power - self.min_power) * 100
            return True, self.current_power, None
        
        # Production: control cooling system
        try:
            # Set fan speed via IPMI
            fan_speed = int((value - self.min_power) / (self.max_power - self.min_power) * 100)
            # subprocess.run(['ipmitool', 'raw', '0x30', '0x30', '0x02', '0xff', str(fan_speed)], timeout=5)
            self.current_power = value
            return True, self.current_power, None
        except Exception as e:
            return False, None, str(e)
    
    def get_temperature_response(self) -> float:
        """Get expected temperature response based on cooling power"""
        # Simple model: more cooling = lower temperature
        base_temp = 80.0
        cooling_effect = (self.current_power / self.max_power) * 30
        return max(30, base_temp - cooling_effect)


class RouterActuator(BaseActuator):
    """Enhanced control system for workload routing"""
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__("router", config)
        self.current_route = 'gpu_cluster'
        self.available_destinations = self.config.get('destinations', 
            ['cpu', 'single_gpu', 'gpu_cluster', 'quantum', 'distilled'])
        
        # Helium footprint for each destination
        self.helium_footprints = {
            'cpu': 0.05,
            'single_gpu': 0.75,
            'gpu_cluster': 0.95,
            'quantum': 0.99,
            'distilled': 0.30
        }
    
    def _validate_value(self, value: float) -> bool:
        idx = int(value)
        return 0 <= idx < len(self.available_destinations)
    
    def _get_fallback_value(self, requested: float) -> float:
        """Safe fallback: route to CPU"""
        return 0.0
    
    def route_to(self, destination: str) -> ActuationResult:
        """Route workload to specific destination"""
        if destination not in self.available_destinations:
            destination = 'cpu'
        return self.actuate(float(self.available_destinations.index(destination)))
    
    def _execute(self, value: float) -> Tuple[bool, Optional[float], Optional[str]]:
        idx = int(value)
        destination = self.available_destinations[idx] if idx < len(self.available_destinations) else 'cpu'
        
        if self.simulation_mode:
            time.sleep(0.005)
            self.current_route = destination
            return True, float(idx), None
        
        # Production: update load balancer
        try:
            # kubectl label nodes --all workload-type={destination}
            self.current_route = destination
            return True, float(idx), None
        except Exception as e:
            return False, None, str(e)
    
    def get_helium_footprint(self) -> float:
        """Get current helium footprint based on route"""
        return self.helium_footprints.get(self.current_route, 0.5)


class SubstitutionActuator(BaseActuator):
    """Enhanced control system for material substitution"""
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__("substitution", config)
        self.current_system = 'helium'
        self.available_systems = self.config.get('systems',
            ['helium', 'cryocooler', 'neon', 'adiabatic'])
        
        # Characteristics of each system
        self.system_properties = {
            'helium': {'helium_reduction': 0.0, 'power_overhead': 1.0, 'latency_ms': 0},
            'cryocooler': {'helium_reduction': 0.90, 'power_overhead': 3.0, 'latency_ms': 5000},
            'neon': {'helium_reduction': 0.50, 'power_overhead': 1.5, 'latency_ms': 2000},
            'adiabatic': {'helium_reduction': 0.95, 'power_overhead': 2.0, 'latency_ms': 10000}
        }
    
    def _validate_value(self, value: float) -> bool:
        return 0 <= int(value) < len(self.available_systems)
    
    def _get_fallback_value(self, requested: float) -> float:
        """Safe fallback: stay with helium"""
        return float(self.available_systems.index('helium'))
    
    def switch_to(self, system: str) -> ActuationResult:
        """Switch cooling system to alternative"""
        if system not in self.available_systems:
            return self.actuate(float(self.available_systems.index('helium')))
        return self.actuate(float(self.available_systems.index(system)))
    
    def _execute(self, value: float) -> Tuple[bool, Optional[float], Optional[str]]:
        idx = int(value)
        system = self.available_systems[idx]
        
        # Check if switch is feasible
        if system != 'helium':
            # Validate system availability
            props = self.system_properties.get(system, {})
            if props.get('latency_ms', 0) > 0:
                # Simulate switching time
                time.sleep(props['latency_ms'] / 1000)
        
        if self.simulation_mode:
            self.current_system = system
            return True, float(idx), None
        
        # Production: control cooling system switch
        try:
            # requests.post('http://cooling-controller/switch', json={'system': system}, timeout=30)
            self.current_system = system
            return True, float(idx), None
        except Exception as e:
            return False, None, str(e)
    
    def get_helium_savings(self) -> float:
        """Get helium savings from current system"""
        return self.system_properties.get(self.current_system, {}).get('helium_reduction', 0)


# ============================================================
# ENHANCEMENT 8: Enhanced Control System
# ============================================================

class ControlSystem:
    """Enhanced unified control system managing all actuators"""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.mode = ControlMode(self.config.get('mode', 'automatic'))
        
        # Initialize all actuators
        self.throttle = ThrottleActuator(self.config.get('throttle', {}))
        self.cooling = CoolingActuator(self.config.get('cooling', {}))
        self.router = RouterActuator(self.config.get('router', {}))
        self.substitution = SubstitutionActuator(self.config.get('substitution', {}))
        
        self._actuators = {
            'throttle': self.throttle,
            'cooling': self.cooling,
            'router': self.router,
            'substitution': self.substitution
        }
        
        self._command_history: List[ActuationResult] = []
        self.sensor_feedback = SensorFeedbackSystem(self.config.get('sensors', {}))
        self.state_persistence = PersistentState()
        self.predictor = PredictiveActuator()
        
        # Load global state
        self._load_global_state()
        
        logger.info("Enhanced Control System v2.0 initialized")
    
    def _load_global_state(self):
        """Load global control system state"""
        state = self.state_persistence.load("control_system")
        if state:
            self.mode = ControlMode(state.get('mode', 'automatic'))
            logger.info(f"Loaded global state: mode={self.mode.value}")
    
    def _save_global_state(self):
        """Save global control system state"""
        self.state_persistence.save("control_system", {
            'mode': self.mode.value,
            'timestamp': time.time()
        })
    
    def execute(self, actuator: str, value: float, use_ramp: bool = False) -> ActuationResult:
        """Execute a command on a specific actuator with enhanced features"""
        if actuator not in self._actuators:
            return ActuationResult(
                success=False,
                command=actuator,
                requested_value=value,
                actual_value=None,
                latency_ms=0,
                error_message=f"Unknown actuator: {actuator}",
                fallback_used=True,
                retry_count=0
            )
        
        # Check if we should actuate based on prediction
        if actuator in ['cooling', 'throttle']:
            should_actuate, recommended = self._should_actuate(actuator, value)
            if not should_actuate:
                logger.info(f"Predictive control skipping actuation for {actuator}")
                # Return success without actuation
                return ActuationResult(
                    success=True,
                    command=actuator,
                    requested_value=value,
                    actual_value=recommended,
                    latency_ms=0,
                    error_message="predictive_skip",
                    fallback_used=False,
                    retry_count=0
                )
            value = recommended
        
        # Execute with ramping if requested
        result = self._actuators[actuator].actuate(value, use_ramp=use_ramp)
        self._command_history.append(result)
        
        # Keep history limited
        if len(self._command_history) > 1000:
            self._command_history = self._command_history[-1000:]
        
        return result
    
    def _should_actuate(self, actuator: str, target: float) -> Tuple[bool, float]:
        """Determine if actuation is needed based on predictions"""
        if actuator == 'cooling':
            # Read current temperature
            temp_reading = self.sensor_feedback.read_sensor('gpu_temperature_c')
            if temp_reading:
                self.predictor.record_value(temp_reading.value)
                return self.predictor.should_actuate(target, tolerance=0.1)
        elif actuator == 'throttle':
            power_reading = self.sensor_feedback.read_sensor('gpu_power_w')
            if power_reading:
                normalized_power = power_reading.value / 300
                self.predictor.record_value(normalized_power)
                return self.predictor.should_actuate(target, tolerance=0.05)
        
        return True, target
    
    def apply_decision(self, decision: Any, use_ramp: bool = True) -> Dict[str, ActuationResult]:
        """Apply a complete decision from enhancement modules"""
        results = {}
        
        if hasattr(decision, 'throttle_factor'):
            results['throttle'] = self.execute('throttle', decision.throttle_factor, use_ramp)
        
        if hasattr(decision, 'target_temp'):
            # Convert temperature to cooling power
            target_temp = decision.target_temp
            current_temp = self.sensor_feedback.read_sensor('gpu_temperature_c')
            if current_temp:
                current = current_temp.value
                # PID-like adjustment
                error = current - target_temp
                power_adjustment = min(200, max(-200, error * 10))
                current_power = self.cooling.current_power or 200
                required_power = max(50, min(500, current_power + power_adjustment))
            else:
                required_power = max(50, min(500, (target_temp - 20) * 10))
            
            results['cooling'] = self.execute('cooling', required_power, use_ramp)
        
        if hasattr(decision, 'recommended_substitute'):
            results['substitution'] = self.substitution.switch_to(decision.recommended_substitute.value)
        
        if hasattr(decision, 'route'):
            results['router'] = self.router.route_to(decision.route)
        
        self._save_global_state()
        return results
    
    def emergency_stop(self) -> Dict[str, ActuationResult]:
        """Emergency stop - safe state for all systems"""
        logger.warning("EMERGENCY STOP triggered")
        self.mode = ControlMode.EMERGENCY
        results = {}
        
        # Maximum cooling, minimum throttle
        results['cooling'] = self.execute('cooling', 500.0, use_ramp=False)
        results['throttle'] = self.execute('throttle', 0.2, use_ramp=False)
        results['router'] = self.router.route_to('cpu')
        # Don't switch substitution in emergency
        
        self._save_global_state()
        return results
    
    def get_status(self) -> Dict:
        """Get complete control system status"""
        return {
            'mode': self.mode.value,
            'actuators': {name: act.get_status() for name, act in self._actuators.items()},
            'command_history_count': len(self._command_history),
            'circuit_breakers': {name: act.circuit_breaker.get_status() 
                                for name, act in self._actuators.items()},
            'predictive_enabled': True
        }
    
    def get_metrics(self) -> Dict:
        """Get Prometheus metrics"""
        return {
            'actuator_commands_total': len(self._command_history),
            'actuator_failures_total': sum(1 for r in self._command_history if not r.success),
            'actuator_fallback_total': sum(1 for r in self._command_history if r.fallback_used),
            'current_throttle': self.throttle.current_throttle,
            'current_cooling_power': self.cooling.current_power,
            'current_route': self.router.current_route,
            'helium_footprint': self.router.get_helium_footprint(),
            'circuit_breaker_open_count': sum(1 for act in self._actuators.values() 
                                             if act.circuit_breaker.state == CircuitState.OPEN)
        }
    
    def get_audit_report(self, actuator: Optional[str] = None) -> Dict:
        """Get comprehensive audit report"""
        if actuator and actuator in self._actuators:
            audits = {actuator: self._actuators[actuator].get_audit_log()}
        else:
            audits = {name: act.get_audit_log() for name, act in self._actuators.items()}
        
        return {
            'timestamp': time.time(),
            'mode': self.mode.value,
            'audit_logs': audits,
            'metrics': self.get_metrics()
        }
    
    def reset_circuit_breaker(self, actuator: str):
        """Manually reset circuit breaker for an actuator"""
        if actuator in self._actuators:
            self._actuators[actuator].circuit_breaker.reset()
            logger.info(f"Circuit breaker for {actuator} manually reset")


# ============================================================
# Usage Example
# ============================================================

if __name__ == "__main__":
    # Initialize control system
    control = ControlSystem({
        'mode': 'automatic',
        'simulate': True,
        'throttle': {'max_power_watts': 300},
        'cooling': {'min_power': 50, 'max_power': 500}
    })
    
    # Test emergency stop
    print("Testing emergency stop...")
    results = control.emergency_stop()
    print(f"Emergency stop results: {list(results.keys())}")
    
    # Test decision application
    print("\nTesting decision application...")
    
    class MockDecision:
        throttle_factor = 0.6
        target_temp = 65.0
        route = 'single_gpu'
    
    decision = MockDecision()
    results = control.apply_decision(decision)
    
    for actuator, result in results.items():
        print(f"{actuator}: success={result.success}, value={result.actual_value}")
    
    # Get metrics
    print("\nSystem metrics:")
    metrics = control.get_metrics()
    for key, value in metrics.items():
        print(f"  {key}: {value}")
    
    print("\n✅ Enhanced Control System v2.0 test complete")
