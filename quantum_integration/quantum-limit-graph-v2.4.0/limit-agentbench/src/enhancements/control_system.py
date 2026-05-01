# src/enhancements/control_system.py

"""
Complete control system for all enhancement actuators
Supports hardware throttling, cooling, routing, and substitution
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import logging
import time
import threading
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class ActuatorStatus(Enum):
    OPERATIONAL = "operational"
    DEGRADED = "degraded"
    FAILED = "failed"
    RECOVERING = "recovering"
    MAINTENANCE = "maintenance"


class ControlMode(Enum):
    AUTOMATIC = "automatic"
    MANUAL = "manual"
    SAFE = "safe"  # Minimally invasive
    TEST = "test"  # Dry run only
    EMERGENCY = "emergency"  # Force safe state


@dataclass
class ActuationResult:
    """Complete result of an actuation command"""
    success: bool
    command: str
    requested_value: float
    actual_value: Optional[float]
    latency_ms: float
    error_message: Optional[str]
    fallback_used: bool
    retry_count: int = 0
    timestamp: float = field(default_factory=time.time)


class BaseActuator(ABC):
    """Abstract base class for all actuators"""
    
    def __init__(self, name: str, config: Optional[Dict] = None):
        self.name = name
        self.config = config or {}
        self.status = ActuatorStatus.OPERATIONAL
        self.mode = ControlMode.AUTOMATIC
        self.simulation_mode = self.config.get('simulate', True)
        self.max_retries = self.config.get('max_retries', 3)
        self.retry_delay_ms = self.config.get('retry_delay_ms', 100)
        self._last_result: Optional[ActuationResult] = None
    
    @abstractmethod
    def _execute(self, value: float) -> Tuple[bool, Optional[float], Optional[str]]:
        """Execute the actual actuation (to be implemented by subclass)"""
        pass
    
    def actuate(self, value: float) -> ActuationResult:
        """Main actuation interface with retries and fallbacks"""
        start_time = time.time()
        retry_count = 0
        fallback_used = False
        
        # Validate value range
        if not self._validate_value(value):
            return ActuationResult(
                success=False,
                command=self.name,
                requested_value=value,
                actual_value=None,
                latency_ms=(time.time() - start_time) * 1000,
                error_message=f"Value {value} out of valid range",
                fallback_used=False,
                retry_count=0
            )
        
        # Check status
        if self.status == ActuatorStatus.FAILED:
            fallback_value = self._get_fallback_value(value)
            return self._actuate_with_fallback(value, fallback_value, start_time)
        
        # Retry loop
        for attempt in range(self.max_retries):
            try:
                success, actual, error = self._execute(value)
                if success:
                    self._last_result = ActuationResult(
                        success=True,
                        command=self.name,
                        requested_value=value,
                        actual_value=actual or value,
                        latency_ms=(time.time() - start_time) * 1000,
                        error_message=None,
                        fallback_used=fallback_used,
                        retry_count=retry_count
                    )
                    logger.info(f"Actuator {self.name}: set to {actual or value}")
                    return self._last_result
                
                retry_count += 1
                time.sleep(self.retry_delay_ms / 1000)
                
            except Exception as e:
                logger.warning(f"Actuator {self.name} attempt {attempt + 1} failed: {e}")
                retry_count += 1
                time.sleep(self.retry_delay_ms / 1000)
        
        # All retries failed, use fallback
        fallback_value = self._get_fallback_value(value)
        return self._actuate_with_fallback(value, fallback_value, start_time)
    
    def _actuate_with_fallback(self, requested: float, fallback: float, start_time: float) -> ActuationResult:
        """Execute fallback actuation"""
        try:
            success, actual, error = self._execute(fallback)
            return ActuationResult(
                success=success,
                command=self.name,
                requested_value=requested,
                actual_value=actual or fallback,
                latency_ms=(time.time() - start_time) * 1000,
                error_message=None if success else error,
                fallback_used=True,
                retry_count=self.max_retries
            )
        except Exception as e:
            return ActuationResult(
                success=False,
                command=self.name,
                requested_value=requested,
                actual_value=None,
                latency_ms=(time.time() - start_time) * 1000,
                error_message=str(e),
                fallback_used=True,
                retry_count=self.max_retries
            )
    
    def _validate_value(self, value: float) -> bool:
        """Validate value range (override in subclasses)"""
        return 0.0 <= value <= 1.0
    
    def _get_fallback_value(self, requested: float) -> float:
        """Get fallback value (override in subclasses)"""
        return 0.5
    
    def get_status(self) -> Dict:
        """Get actuator status"""
        return {
            'name': self.name,
            'status': self.status.value,
            'mode': self.mode.value,
            'simulation': self.simulation_mode,
            'last_result': self._last_result.__dict__ if self._last_result else None
        }


class ThrottleActuator(BaseActuator):
    """Control system for GPU/CPU throttling"""
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__("throttle", config)
        self.current_throttle = 1.0
        self.power_capping_enabled = self.config.get('power_capping', True)
    
    def _validate_value(self, value: float) -> bool:
        return 0.0 <= value <= 1.0
    
    def _get_fallback_value(self, requested: float) -> float:
        """Safe fallback: moderate throttle (50%)"""
        return 0.5
    
    def _execute(self, value: float) -> Tuple[bool, Optional[float], Optional[str]]:
        if self.simulation_mode:
            # Simulate actuation
            time.sleep(0.01)  # Simulate hardware latency
            self.current_throttle = value
            return True, self.current_throttle, None
        
        # Production implementation would write to hardware
        # e.g., nvidia-smi -pl {value * max_power}
        try:
            # Example: set GPU power cap
            import subprocess
            power_limit = int(value * 300)  # 300W max
            subprocess.run(['nvidia-smi', '-pl', str(power_limit)], check=True, timeout=5)
            self.current_throttle = value
            return True, self.current_throttle, None
        except Exception as e:
            return False, None, str(e)


class CoolingActuator(BaseActuator):
    """Control system for cooling infrastructure"""
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__("cooling", config)
        self.current_power = 0.0
        self.current_fan_speed = 0.0
        self.min_power = self.config.get('min_power', 50.0)
        self.max_power = self.config.get('max_power', 500.0)
    
    def _validate_value(self, value: float) -> bool:
        """Value is cooling power in watts (0-500)"""
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
        
        # Production: control actual cooling system
        try:
            # Example: set fan speed via IPMI
            # subprocess.run(['ipmitool', 'raw', '0x30', '0x30', '0x02', '0xff', str(int(fan_speed))])
            self.current_power = value
            return True, self.current_power, None
        except Exception as e:
            return False, None, str(e)


class RouterActuator(BaseActuator):
    """Control system for workload routing between hardware pools"""
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__("router", config)
        self.current_route = 'gpu_cluster'
        self.available_destinations = ['cpu', 'single_gpu', 'gpu_cluster', 'quantum', 'distilled']
    
    def _validate_value(self, value: float) -> bool:
        """Value is route index or name"""
        return True  # Will validate against available destinations
    
    def _get_fallback_value(self, requested: float) -> float:
        """Safe fallback: route to CPU"""
        return 0  # index of CPU
    
    def route_to(self, destination: str) -> ActuationResult:
        """Route workload to specific destination"""
        if destination not in self.available_destinations:
            destination = 'cpu'  # fallback
        return self.actuate(self.available_destinations.index(destination))
    
    def _execute(self, value: float) -> Tuple[bool, Optional[float], Optional[str]]:
        idx = int(value)
        destination = self.available_destinations[idx] if idx < len(self.available_destinations) else 'cpu'
        
        if self.simulation_mode:
            time.sleep(0.005)
            self.current_route = destination
            return True, float(idx), None
        
        # Production: update load balancer configuration
        try:
            # Example: update Kubernetes node selector
            # kubectl label nodes node-1 workload-type={destination}
            self.current_route = destination
            return True, float(idx), None
        except Exception as e:
            return False, None, str(e)


class SubstitutionActuator(BaseActuator):
    """Control system for material substitution (cooling alternatives)"""
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__("substitution", config)
        self.current_system = 'helium'
        self.available_systems = ['helium', 'cryocooler', 'neon', 'adiabatic']
    
    def _validate_value(self, value: float) -> bool:
        return 0 <= int(value) < len(self.available_systems)
    
    def _get_fallback_value(self, requested: float) -> float:
        """Safe fallback: stay with helium (what's already working)"""
        return float(self.available_systems.index('helium'))
    
    def switch_to(self, system: str) -> ActuationResult:
        """Switch cooling system to alternative"""
        if system not in self.available_systems:
            return self.actuate(float(self.available_systems.index('helium')))
        return self.actuate(float(self.available_systems.index(system)))
    
    def _execute(self, value: float) -> Tuple[bool, Optional[float], Optional[str]]:
        idx = int(value)
        system = self.available_systems[idx]
        
        if self.simulation_mode:
            time.sleep(0.1)  # Switching takes longer
            self.current_system = system
            return True, float(idx), None
        
        # Production: control cooling system switch
        try:
            # Example: send command to cooling controller
            # requests.post('http://cooling-controller/switch', json={'system': system})
            self.current_system = system
            return True, float(idx), None
        except Exception as e:
            return False, None, str(e)


class ControlSystem:
    """Unified control system managing all actuators"""
    
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
        logger.info("Control system initialized")
    
    def execute(self, actuator: str, value: float) -> ActuationResult:
        """Execute a command on a specific actuator"""
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
        
        result = self._actuators[actuator].actuate(value)
        self._command_history.append(result)
        
        # Keep history limited
        if len(self._command_history) > 1000:
            self._command_history = self._command_history[-1000:]
        
        return result
    
    def apply_decision(self, decision: Any) -> Dict[str, ActuationResult]:
        """Apply a complete decision from enhancement modules"""
        results = {}
        
        if hasattr(decision, 'throttle_factor'):
            results['throttle'] = self.execute('throttle', decision.throttle_factor)
        
        if hasattr(decision, 'target_temp'):
            # Convert temperature to cooling power
            required_power = max(50, min(500, (decision.target_temp - 20) * 10))
            results['cooling'] = self.execute('cooling', required_power)
        
        if hasattr(decision, 'recommended_substitute'):
            sub_value = self.substitution.available_systems.index(decision.recommended_substitute.value)
            results['substitution'] = self.execute('substitution', float(sub_value))
        
        return results
    
    def emergency_stop(self) -> Dict[str, ActuationResult]:
        """Emergency stop - safe state for all systems"""
        logger.warning("EMERGENCY STOP triggered")
        self.mode = ControlMode.EMERGENCY
        results = {}
        
        # Maximum cooling, minimum throttle
        results['cooling'] = self.execute('cooling', 500.0)
        results['throttle'] = self.execute('throttle', 0.2)
        results['router'] = self.execute('router', 0.0)  # Route to CPU
        # Don't switch substitution in emergency
        
        return results
    
    def get_status(self) -> Dict:
        """Get complete control system status"""
        return {
            'mode': self.mode.value,
            'actuators': {name: act.get_status() for name, act in self._actuators.items()},
            'command_history_count': len(self._command_history)
        }
    
    def get_metrics(self) -> Dict:
        """Get Prometheus metrics"""
        return {
            'actuator_commands_total': len(self._command_history),
            'actuator_failures_total': sum(1 for r in self._command_history if not r.success),
            'actuator_fallback_total': sum(1 for r in self._command_history if r.fallback_used),
            'current_throttle': self.throttle.current_throttle,
            'current_cooling_power': self.cooling.current_power
        }
