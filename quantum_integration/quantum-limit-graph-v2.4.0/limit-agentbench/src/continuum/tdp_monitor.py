# src/continuum/tdp_monitor.py

from typing import Dict, Optional, Callable, List
from dataclasses import dataclass, field
from enum import Enum
import asyncio
from datetime import datetime, timedelta
import psutil
import subprocess

class DeviceType(Enum):
    RASPBERRY_PI_5 = "raspberry_pi_5"
    NVIDIA_JETSON_ORIN = "nvidia_jetson_orin"
    INTEL_NUC_13_PRO = "intel_nuc_13_pro"
    UNKNOWN = "unknown"

@dataclass
class TDPThresholds:
    """TDP thresholds per device type"""
    RASPBERRY_PI_5: float = 12.0    # Watts
    NVIDIA_JETSON_ORIN: float = 60.0  # Watts (max mode)
    INTEL_NUC_13_PRO: float = 28.0   # Watts
    WARNING_BUFFER: float = 2.0      # Watts before threshold

@dataclass
class TDPReading:
    """Real-time TDP telemetry"""
    timestamp: datetime
    device_id: str
    device_type: DeviceType
    current_power_watts: float
    tdp_threshold_watts: float
    utilization_percent: float
    temperature_celsius: float
    thermal_throttling: bool
    predicted_breach_seconds: Optional[int] = None  # ETA to TDP breach

class TDPMonitor:
    """
    Real-time TDP telemetry and breach prediction
    
    Responsibilities:
    - Monitor device power consumption in real-time
    - Detect thermal throttling conditions
    - Predict TDP breaches 5-10 minutes ahead
    - Notify OffloadingDecisionEngine on threshold breaches
    """
    
    def __init__(
        self,
        device_id: str,
        device_type: DeviceType,
        sampling_interval_seconds: int = 5,
        prediction_horizon_seconds: int = 300,  # 5 minutes
        thresholds: Optional[TDPThresholds] = None
    ):
        self.device_id = device_id
        self.device_type = device_type
        self.sampling_interval = sampling_interval_seconds
        self.prediction_horizon = prediction_horizon_seconds
        self.thresholds = thresholds or TDPThresholds()
        
        self._callbacks: List[Callable[[TDPReading], None]] = []
        self._history: List[TDPReading] = []
        self._max_history = 1000  # Keep last 1000 readings (~83 minutes)
        self._running = False
        
    async def start(self):
        """Start TDP monitoring loop"""
        self._running = True
        asyncio.create_task(self._monitoring_loop())
        
    async def stop(self):
        """Stop TDP monitoring loop"""
        self._running = False
        
    def register_callback(self, callback: Callable[[TDPReading], None]):
        """Register callback for TDP breach notifications"""
        self._callbacks.append(callback)
        
    async def get_current_reading(self) -> TDPReading:
        """Get current TDP reading with breach prediction"""
        reading = await self._measure_power()
        reading.predicted_breach_seconds = self._predict_breach(reading)
        
        # Store in history
        self._history.append(reading)
        if len(self._history) > self._max_history:
            self._history.pop(0)
            
        # Notify callbacks if approaching breach
        if reading.predicted_breach_seconds is not None and \
           reading.predicted_breach_seconds < 60:  # Less than 1 minute
            for callback in self._callbacks:
                try:
                    callback(reading)
                except Exception as e:
                    logger.error(f"TDP callback failed: {e}")
                    
        return reading
        
    async def _measure_power(self) -> TDPReading:
        """Measure current power consumption based on device type"""
        if self.device_type == DeviceType.RASPBERRY_PI_5:
            power = await self._measure_rpi_power()
        elif self.device_type == DeviceType.NVIDIA_JETSON_ORIN:
            power = await self._measure_jetson_power()
        elif self.device_type == DeviceType.INTEL_NUC_13_PRO:
            power = await self._measure_nuc_power()
        else:
            power = await self._estimate_power_from_cpu()
            
        # Get temperature
        temperature = await self._measure_temperature()
        
        # Check thermal throttling
        throttling = await self._check_thermal_throttling()
        
        # Calculate utilization
        utilization = psutil.cpu_percent(interval=1)
        
        # Get TDP threshold
        tdp_threshold = getattr(self.thresholds, self.device_type.value.upper(), 28.0)
        
        return TDPReading(
            timestamp=datetime.now(),
            device_id=self.device_id,
            device_type=self.device_type,
            current_power_watts=power,
            tdp_threshold_watts=tdp_threshold,
            utilization_percent=utilization,
            temperature_celsius=temperature,
            thermal_throttling=throttling
        )
        
    async def _measure_rpi_power(self) -> float:
        """Measure Raspberry Pi 5 power via INA219 sensor"""
        try:
            # Read from INA219 power monitor HAT
            result = await asyncio.create_subprocess_exec(
                'i2cget', '-y', '1', '0x40', '0x02', 'w',
                stdout=asyncio.subprocess.PIPE
            )
            stdout, _ = await result.communicate()
            # Parse power value (implementation-specific)
            return float(stdout.decode().strip()) * 0.001  # Convert to Watts
        except Exception:
            return await self._estimate_power_from_cpu()
            
    async def _measure_jetson_power(self) -> float:
        """Measure NVIDIA Jetson Orin power via tegrastats"""
        try:
            result = await asyncio.create_subprocess_exec(
                'tegrastats', '--interval', '1000', '--limit', '1',
                stdout=asyncio.subprocess.PIPE
            )
            stdout, _ = await result.communicate()
            # Parse VDD_IN power from tegrastats output
            # Example: "VDD_IN: ... 5123mW ..."
            import re
            match = re.search(r'VDD_IN:.*?(\d+)mW', stdout.decode())
            if match:
                return int(match.group(1)) / 1000.0
        except Exception:
            return await self._estimate_power_from_cpu()
        return 15.0  # Default estimate
        
    async def _measure_nuc_power(self) -> float:
        """Measure Intel NUC 13 Pro power via RAPL"""
        try:
            # Read from RAPL (Running Average Power Limit)
            rapl_path = '/sys/class/powercap/intel-rapl/intel-rapl:0/energy_uj'
            with open(rapl_path, 'r') as f:
                energy_uj = int(f.read().strip())
                
            # Calculate power from energy difference
            if len(self._history) > 0:
                prev_reading = self._history[-1]
                time_delta = (datetime.now() - prev_reading.timestamp).total_seconds()
                if time_delta > 0:
                    # This is simplified - real implementation tracks cumulative energy
                    return prev_reading.current_power_watts  # Placeholder
                    
            return 20.0  # Default estimate
        except Exception:
            return await self._estimate_power_from_cpu()
            
    async def _estimate_power_from_cpu(self) -> float:
        """Estimate power from CPU utilization (fallback)"""
        cpu_percent = psutil.cpu_percent(interval=1)
        # Linear model: idle power + (max_power - idle_power) * utilization
        base_power = {
            DeviceType.RASPBERRY_PI_5: (3.0, 12.0),
            DeviceType.NVIDIA_JETSON_ORIN: (5.0, 60.0),
            DeviceType.INTEL_NUC_13_PRO: (8.0, 28.0),
        }.get(self.device_type, (10.0, 50.0))
        
        idle, max_power = base_power
        return idle + (max_power - idle) * (cpu_percent / 100.0)
        
    async def _measure_temperature(self) -> float:
        """Measure device temperature"""
        try:
            if self.device_type == DeviceType.RASPBERRY_PI_5:
                result = await asyncio.create_subprocess_exec(
                    'vcgencmd', 'measure_temp',
                    stdout=asyncio.subprocess.PIPE
                )
                stdout, _ = await result.communicate()
                # Parse "temp=45.2'C"
                import re
                match = re.search(r'temp=(\d+\.\d+)', stdout.decode())
                if match:
                    return float(match.group(1))
            elif self.device_type == DeviceType.NVIDIA_JETSON_ORIN:
                # Read from thermal zone
                with open('/sys/class/thermal/thermal_zone0/temp', 'r') as f:
                    return int(f.read().strip()) / 1000.0
            else:
                # Generic Linux
                for path in ['/sys/class/thermal/thermal_zone0/temp',
                            '/sys/class/hwmon/hwmon0/temp1_input']:
                    try:
                        with open(path, 'r') as f:
                            temp = int(f.read().strip())
                            if temp > 1000:  # Millidegrees
                                return temp / 1000.0
                            return float(temp)
                    except FileNotFoundError:
                        continue
        except Exception:
            pass
        return 25.0  # Default room temperature
        
    async def _check_thermal_throttling(self) -> bool:
        """Check if device is thermally throttling"""
        try:
            if self.device_type == DeviceType.RASPBERRY_PI_5:
                result = await asyncio.create_subprocess_exec(
                    'vcgencmd', 'get_throttled',
                    stdout=asyncio.subprocess.PIPE
                )
                stdout, _ = await result.communicate()
                # Parse throttled flags (bit 0 = throttled)
                import re
                match = re.search(r'0x([0-9a-fA-F]+)', stdout.decode())
                if match:
                    flags = int(match.group(1), 16)
                    return (flags & 0x1) != 0
        except Exception:
            pass
        return False
        
    def _predict_breach(self, reading: TDPReading) -> Optional[int]:
        """Predict time to TDP breach using linear regression on history"""
        if len(self._history) < 10:  # Need at least 10 data points
            return None
            
        # Get recent power trend
        recent = self._history[-10:]
        power_values = [r.current_power_watts for r in recent]
        time_values = list(range(len(power_values)))
        
        # Simple linear regression
        from statistics import mean
        x_mean = mean(time_values)
        y_mean = mean(power_values)
        
        numerator = sum((x - x_mean) * (y - y_mean) 
                       for x, y in zip(time_values, power_values))
        denominator = sum((x - x_mean) ** 2 for x in time_values)
        
        if denominator == 0:
            return None
            
        slope = numerator / denominator
        
        if slope <= 0:  # Power not increasing
            return None
            
        # Calculate time to breach
        remaining_power = reading.tdp_threshold_watts - reading.current_power_watts - \
                         self.thresholds.WARNING_BUFFER
                         
        if remaining_power <= 0:
            return 0  # Already at threshold
            
        # Time to breach in samples
        samples_to_breach = remaining_power / slope
        seconds_to_breach = int(samples_to_breach * self.sampling_interval)
        
        return max(0, min(seconds_to_breach, self.prediction_horizon))
        
    async def _monitoring_loop(self):
        """Background loop for periodic TDP measurements"""
        while self._running:
            try:
                await self.get_current_reading()
                await asyncio.sleep(self.sampling_interval)
            except Exception as e:
                logger.error(f"TDP monitoring loop error: {e}")
                await asyncio.sleep(10)  # Retry after 10 seconds
