# src/enhancements/thermal_optimizer.py

"""
Thermal-Aware Workload Scheduling for Green Agent
Scientific basis: Arrhenius equation for temperature-dependent leakage power

Reference: "Thermal-Aware Scheduling in Green Data Centers" (IEEE TPDS, 2023)
"""

import math
import numpy as np
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ThermalZone(Enum):
    """Thermal operating zones"""
    CRITICAL = "critical"      # > 85°C - immediate action needed
    WARNING = "warning"         # 75-85°C - throttle recommended
    NORMAL = "normal"           # 65-75°C - normal operation
    OPTIMAL = "optimal"         # 55-65°C - most efficient
    COOL = "cool"              # < 55°C - can increase load


@dataclass
class ThermalProfile:
    """Thermal profile for a workload"""
    current_temp_celsius: float
    target_temp_celsius: float
    cooling_power_watts: float
    leakage_power_watts: float
    thermal_time_constant_seconds: float
    zone: ThermalZone
    recommended_action: str


@dataclass
class ThermalDecision:
    """Decision output from thermal optimizer"""
    action: str  # 'cool', 'heat', 'maintain', 'throttle', 'emergency_shutdown'
    throttle_factor: float
    target_temp: float
    energy_savings_percent: float
    recovery_time_seconds: float
    reasoning: str


class ThermalAwareOptimizer:
    """
    Thermal-aware workload scheduler for energy optimization.
    
    Scientific basis: P_total = P_dynamic + P_leakage(T)
    P_leakage(T) = P_leakage(T0) * exp((E_a/k) * (1/T0 - 1/T))
    
    Where:
    - E_a = activation energy (0.3-1.2 eV for CMOS)
    - k = Boltzmann constant (8.617 × 10^-5 eV/K)
    - T = absolute temperature (Kelvin)
    """
    
    # Physical constants
    BOLTZMANN_EV = 8.617333262145e-5  # eV/K
    ROOM_TEMP_K = 298.15  # 25°C
    
    # Technology parameters (typical for 5nm GPUs)
    ACTIVATION_ENERGY_EV = 0.65  # eV
    LEAKAGE_POWER_AT_ROOM_W = 15.0  # Watts
    HYSTERESIS_TEMP_C = 3.0  # °C hysteresis to prevent cycling
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.current_temp = self.config.get('initial_temperature', 65.0)
        self.hardware_tdp = self.config.get('hardware_tdp_watts', 300.0)
        self.cooling_efficiency = self.config.get('cooling_efficiency', 0.35)  # COP
        self.temperature_log: List[Tuple[float, float]] = []  # (time, temp)
        
        # Thermal thresholds
        self.thresholds = {
            'optimal_max': 65.0,
            'normal_max': 75.0,
            'warning_max': 85.0,
            'critical_max': 95.0
        }
        
    def calculate_leakage_power(self, temp_celsius: float) -> float:
        """
        Calculate temperature-dependent leakage power using Arrhenius equation.
        
        P_leak(T) = P_leak(T0) * exp((E_a/k) * (1/T0 - 1/T))
        
        Args:
            temp_celsius: Current temperature in Celsius
            
        Returns:
            Leakage power in Watts
        """
        temp_k = temp_celsius + 273.15
        
        # Arrhenius factor
        arrhenius_factor = math.exp(
            (self.ACTIVATION_ENERGY_EV / self.BOLTZMANN_EV) * 
            (1/self.ROOM_TEMP_K - 1/temp_k)
        )
        
        leakage_power = self.LEAKAGE_POWER_AT_ROOM_W * arrhenius_factor
        
        # Cap at reasonable limits
        return min(leakage_power, self.hardware_tdp * 0.5)
    
    def calculate_cooling_power(self, temp_celsius: float, target_celsius: float) -> float:
        """
        Calculate cooling power required to reach target temperature.
        
        Cooling power = (ΔT * ThermalMass) / (COP * Time)
        
        Args:
            temp_celsius: Current temperature
            target_celsius: Desired temperature
            
        Returns:
            Cooling power in Watts
        """
        delta_temp = max(0, temp_celsius - target_celsius)
        
        # Estimated thermal mass (J/°C) - derived from hardware specs
        thermal_mass = self.hardware_tdp * 2.5  # Rough estimate
        
        # Cooling energy required
        cooling_energy_joules = delta_temp * thermal_mass
        
        # Convert to power (assuming 60-second cooling window)
        cooling_power_watts = cooling_energy_joules / 60.0
        
        # Account for cooling system efficiency (COP)
        electrical_power = cooling_power_watts / self.cooling_efficiency
        
        return max(0, electrical_power)
    
    def get_thermal_zone(self, temp_celsius: float) -> ThermalZone:
        """Determine thermal zone based on temperature"""
        if temp_celsius >= self.thresholds['critical_max']:
            return ThermalZone.CRITICAL
        elif temp_celsius >= self.thresholds['warning_max']:
            return ThermalZone.WARNING
        elif temp_celsius >= self.thresholds['normal_max']:
            return ThermalZone.NORMAL
        elif temp_celsius >= self.thresholds['optimal_max']:
            return ThermalZone.OPTIMAL
        else:
            return ThermalZone.COOL
    
    def find_optimal_operating_temp(self, workload_power_watts: float) -> float:
        """
        Find temperature that minimizes total power.
        
        Total power = DynamicPower + LeakagePower(T) + CoolingPower(T)
        """
        temperatures = np.arange(40, 85, 0.5)  # Search range
        
        min_total_power = float('inf')
        optimal_temp = 65.0
        
        for temp in temperatures:
            dynamic_power = workload_power_watts
            leakage_power = self.calculate_leakage_power(temp)
            
            # Cooling power needed to maintain temp
            # Assume ambient is 25°C
            cooling_power = self.calculate_cooling_power(temp, 25.0)
            
            total_power = dynamic_power + leakage_power + cooling_power
            
            if total_power < min_total_power:
                min_total_power = total_power
                optimal_temp = temp
        
        return optimal_temp
    
    def update_temperature(self, power_watts: float, time_delta_seconds: int = 10) -> float:
        """
        Update temperature based on thermal dynamics.
        
        Simple thermal model: dT/dt = (P_in - P_out) / C_th
        """
        # Thermal capacitance (J/°C) - typical for GPU servers
        thermal_capacitance = 500.0  # J/°C
        
        # Heat dissipation rate (W/°C above ambient)
        dissipation_coefficient = 10.0  # W/°C
        
        ambient_temp = 25.0
        
        # Temperature change
        dT = (power_watts - dissipation_coefficient * (self.current_temp - ambient_temp)) * time_delta_seconds / thermal_capacitance
        
        self.current_temp += dT
        
        # Clip to reasonable limits
        self.current_temp = max(ambient_temp, min(115.0, self.current_temp))
        
        # Log temperature
        import time
        self.temperature_log.append((time.time(), self.current_temp))
        
        # Keep only last 1000 entries
        if len(self.temperature_log) > 1000:
            self.temperature_log = self.temperature_log[-1000:]
        
        return self.current_temp
    
    def optimize_schedule(self, workload_profile, execution_decision) -> ThermalDecision:
        """
        Main optimization function for thermal-aware scheduling.
        
        Args:
            workload_profile: WorkloadProfile from Layer 0
            execution_decision: ExecutionDecision from Layer 3
            
        Returns:
            ThermalDecision with recommended action
        """
        # Get workload power estimate
        workload_power = self.estimate_workload_power(workload_profile, execution_decision)
        
        # Update thermal model
        self.update_temperature(workload_power)
        
        # Current thermal zone
        current_zone = self.get_thermal_zone(self.current_temp)
        
        # Find optimal operating temperature
        optimal_temp = self.find_optimal_operating_temp(workload_power)
        optimal_zone = self.get_thermal_zone(optimal_temp)
        
        # Calculate potential savings
        current_power = self.calculate_total_power(workload_power, self.current_temp)
        optimal_power = self.calculate_total_power(workload_power, optimal_temp)
        
        potential_savings = (current_power - optimal_power) / current_power * 100 if current_power > 0 else 0
        
        # Determine action based on thermal zone and hysteresis
        action, throttle_factor = self._determine_action(current_zone, self.current_temp, optimal_temp)
        
        # Calculate recovery time
        recovery_time = self._estimate_recovery_time(self.current_temp, optimal_temp, workload_power)
        
        reasoning = self._generate_reasoning(current_zone, optimal_zone, potential_savings, action)
        
        # Log decision
        logger.info(f"Thermal decision: {action} | Temp: {self.current_temp:.1f}°C → {optimal_temp:.1f}°C | Savings: {potential_savings:.1f}%")
        
        return ThermalDecision(
            action=action,
            throttle_factor=throttle_factor,
            target_temp=optimal_temp,
            energy_savings_percent=max(0, potential_savings),
            recovery_time_seconds=recovery_time,
            reasoning=reasoning
        )
    
    def estimate_workload_power(self, workload_profile, execution_decision) -> float:
        """Estimate power draw of workload in Watts"""
        # Base power from resource requirements
        gpu_count = getattr(workload_profile, 'gpu_count', 0)
        
        # Power per GPU (varies by utilization)
        power_per_gpu = 250.0  # Watts (typical for A100)
        
        # Adjust by power budget from decision core
        power_budget = execution_decision.power_budget if hasattr(execution_decision, 'power_budget') else 1.0
        
        workload_power = gpu_count * power_per_gpu * power_budget
        
        # Add CPU and memory power
        workload_power += 50.0  # CPU
        workload_power += 30.0  # Memory
        
        return min(workload_power, self.hardware_tdp * 1.2)
    
    def calculate_total_power(self, workload_power: float, temp_celsius: float) -> float:
        """Calculate total power including leakage and cooling"""
        leakage = self.calculate_leakage_power(temp_celsius)
        cooling = self.calculate_cooling_power(temp_celsius, 25.0)
        
        return workload_power + leakage + cooling
    
    def _determine_action(self, zone: ThermalZone, current_temp: float, optimal_temp: float) -> Tuple[str, float]:
        """Determine action based on thermal zone"""
        if zone == ThermalZone.CRITICAL:
            return 'emergency_shutdown', 0.0
        
        elif zone == ThermalZone.WARNING:
            # Calculate throttle factor based on temperature delta
            overshoot = current_temp - self.thresholds['warning_max']
            throttle_factor = max(0.3, 1.0 - overshoot / 20.0)
            return 'throttle', throttle_factor
        
        elif zone == ThermalZone.NORMAL:
            # Check hysteresis to prevent cycling
            if current_temp > optimal_temp + self.HYSTERESIS_TEMP_C:
                return 'cool', 0.9
            elif current_temp < optimal_temp - self.HYSTERESIS_TEMP_C:
                return 'heat', 1.1
            else:
                return 'maintain', 1.0
        
        elif zone == ThermalZone.OPTIMAL:
            return 'maintain', 1.0
        
        else:  # COOL
            if current_temp < optimal_temp - self.HYSTERESIS_TEMP_C:
                return 'heat', 1.05
            else:
                return 'maintain', 1.0
    
    def _estimate_recovery_time(self, current_temp: float, target_temp: float, power_watts: float) -> float:
        """Estimate time to reach target temperature in seconds"""
        delta_temp = abs(current_temp - target_temp)
        
        # Thermal time constant (seconds)
        thermal_mass = 500.0  # J/°C
        dissipation = 10.0  # W/°C
        time_constant = thermal_mass / dissipation
        
        # Exponential recovery
        recovery_time = time_constant * math.log(delta_temp / 1.0) if delta_temp > 1 else 0
        
        return max(0, min(300, recovery_time))
    
    def _generate_reasoning(self, current_zone: ThermalZone, optimal_zone: ThermalZone, 
                           savings: float, action: str) -> str:
        """Generate human-readable reasoning for decision"""
        reasons = []
        
        if savings > 10:
            reasons.append(f"potential {savings:.0f}% energy savings")
        
        if current_zone != optimal_zone:
            reasons.append(f"moving from {current_zone.value} to {optimal_zone.value} zone")
        
        if action == 'throttle':
            reasons.append("preventing critical overheating")
        elif action == 'emergency_shutdown':
            reasons.append("CRITICAL: immediate shutdown required")
        
        if reasons:
            return f"Thermal-optimized: {', '.join(reasons)}"
        else:
            return f"Maintaining thermal equilibrium at {current_zone.value} zone"
    
    def get_thermal_metrics(self) -> Dict:
        """Get current thermal metrics for Prometheus export"""
        return {
            'current_temperature_celsius': self.current_temp,
            'leakage_power_watts': self.calculate_leakage_power(self.current_temp),
            'cooling_power_watts': self.calculate_cooling_power(self.current_temp, 25.0),
            'thermal_zone': self.get_thermal_zone(self.current_temp).value,
            'energy_savings_percent': self._calculate_historical_savings()
        }
    
    def _calculate_historical_savings(self) -> float:
        """Calculate average energy savings from thermal optimization"""
        if len(self.temperature_log) < 2:
            return 0.0
        
        # Simplified calculation based on temperature history
        avg_temp = sum(t for _, t in self.temperature_log[-100:]) / min(100, len(self.temperature_log))
        baseline_temp = 70.0  # assumed baseline without optimization
        
        if avg_temp > baseline_temp:
            return 0.0
        
        # Approximate 1°C reduction = 2% leakage reduction
        savings = (baseline_temp - avg_temp) * 2
        
        return max(0, min(30, savings))
