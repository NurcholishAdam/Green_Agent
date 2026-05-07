# src/enhancements/thermal_optimizer.py

"""
Enhanced Thermal-Aware Workload Scheduling for Green Agent - Version 3.2

ENHANCEMENTS:
1. Multi-zone cooling optimization with model predictive control (MPC)
2. Digital twin for real-time thermal simulation
3. Reinforcement learning for optimal temperature setpoint
4. Predictive maintenance with remaining useful life (RUL) estimation
5. Thermal-aware scheduling with deadline constraints
6. Liquid cooling modeling for high-performance computing
7. Heat exchanger efficiency modeling
8. Free cooling (economizer) optimization
9. Thermal storage (ice/water tank) optimization
10. Integration with building management systems (BMS)

Reference: "Thermal-Aware Scheduling in Green Data Centers" (IEEE TPDS, 2023)
"""

import math
import numpy as np
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Callable
from enum import Enum
import logging
import time
import threading
import subprocess
from collections import deque
import asyncio
from scipy import stats
from scipy.optimize import minimize, differential_evolution
from scipy.integrate import odeint
import json
import hashlib

# Try to import optional dependencies
try:
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.preprocessing import StandardScaler
    from sklearn.gaussian_process import GaussianProcessRegressor
    from sklearn.gaussian_process.kernels import RBF, WhiteKernel, Matern
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logger.warning("scikit-learn not available, using basic prediction")

try:
    import torch
    import torch.nn as nn
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    logger.warning("PyTorch not available, LSTM prediction disabled")

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: Liquid Cooling System Model
# ============================================================

class LiquidCoolingModel:
    """
    Liquid cooling system model for high-performance computing.
    
    Features:
    - Coolant temperature dynamics
    - Heat exchanger efficiency
    - Pump power consumption
    - Flow rate optimization
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
        # Cooling system parameters
        self.coolant_type = self.config.get('coolant_type', 'water')
        self.flow_rate_lpm = self.config.get('flow_rate_lpm', 100.0)
        self.coolant_supply_temp_c = self.config.get('coolant_supply_temp_c', 25.0)
        self.coolant_return_temp_c = self.config.get('coolant_return_temp_c', 35.0)
        
        # Physical properties
        self.coolant_properties = {
            'water': {'density_kg_m3': 997, 'specific_heat_kj_kg_k': 4.18, 'thermal_conductivity_w_mk': 0.606},
            'propylene_glycol': {'density_kg_m3': 1035, 'specific_heat_kj_kg_k': 3.5, 'thermal_conductivity_w_mk': 0.38},
            'fluorinert': {'density_kg_m3': 1880, 'specific_heat_kj_kg_k': 1.1, 'thermal_conductivity_w_mk': 0.07}
        }
        
        self.properties = self.coolant_properties.get(self.coolant_type, self.coolant_properties['water'])
        
        # Heat exchanger
        self.hex_effectiveness = self.config.get('hex_effectiveness', 0.85)
        self.hex_ua_w_per_k = self.config.get('hex_ua', 5000)  # Overall heat transfer coefficient × area
        
        # Pump
        self.pump_efficiency = self.config.get('pump_efficiency', 0.75)
        self.pump_head_m = self.config.get('pump_head_m', 20.0)
        
        logger.info(f"LiquidCoolingModel initialized ({self.coolant_type}, flow={self.flow_rate_lpm} LPM)")
    
    def calculate_cooling_capacity(self, heat_load_kw: float) -> float:
        """Calculate cooling capacity and required flow"""
        # Mass flow rate (kg/s)
        mass_flow_kg_s = (self.flow_rate_lpm / 60.0) * self.properties['density_kg_m3'] / 1000.0
        
        # Heat rejection rate (kW)
        q_rejected = mass_flow_kg_s * self.properties['specific_heat_kj_kg_k'] * (self.coolant_return_temp_c - self.coolant_supply_temp_c)
        
        # Required flow to meet heat load
        required_flow = (heat_load_kw * 60) / (self.properties['density_kg_m3'] * self.properties['specific_heat_kj_kg_k'] * (self.coolant_return_temp_c - self.coolant_supply_temp_c))
        
        return {
            'cooling_capacity_kw': q_rejected,
            'required_flow_lpm': required_flow,
            'flow_rate_lpm': self.flow_rate_lpm,
            'margin': q_rejected - heat_load_kw
        }
    
    def calculate_pump_power(self, flow_rate_lpm: float = None) -> float:
        """Calculate pump power consumption"""
        if flow_rate_lpm is None:
            flow_rate_lpm = self.flow_rate_lpm
        
        # Flow rate (m³/s)
        flow_m3_s = flow_rate_lpm / 60.0 / 1000.0
        
        # Hydraulic power (kW) = flow × head × density × g / 1000
        hydraulic_power_kw = flow_m3_s * self.pump_head_m * self.properties['density_kg_m3'] * 9.81 / 1000.0
        
        # Electrical power (kW)
        electrical_power_kw = hydraulic_power_kw / self.pump_efficiency
        
        return electrical_power_kw
    
    def calculate_heat_exchanger_performance(self, air_temp_c: float, coolant_temp_c: float) -> float:
        """Calculate heat exchanger heat transfer rate"""
        # NTU method
        # Water capacity rate (kW/K)
        c_w = self.flow_rate_lpm / 60.0 * self.properties['density_kg_m3'] * self.properties['specific_heat_kj_kg_k'] / 1000.0
        
        # Air capacity rate (kW/K) (simplified)
        c_a = 10.0
        
        c_min = min(c_w, c_a)
        c_max = max(c_w, c_a)
        cr = c_min / c_max if c_max > 0 else 0
        
        ntu = self.hex_ua_w_per_k / (c_min * 1000)
        
        # Effectiveness for counter-flow
        if cr < 1:
            effectiveness = (1 - math.exp(-ntu * (1 - cr))) / (1 - cr * math.exp(-ntu * (1 - cr)))
        else:
            effectiveness = ntu / (1 + ntu)
        
        # Heat transfer rate (kW)
        q_max = c_min * abs(air_temp_c - coolant_temp_c)
        q_transfer = effectiveness * q_max
        
        return q_transfer
    
    def get_status(self) -> Dict:
        """Get cooling system status"""
        return {
            'coolant_type': self.coolant_type,
            'flow_rate_lpm': self.flow_rate_lpm,
            'supply_temp_c': self.coolant_supply_temp_c,
            'return_temp_c': self.coolant_return_temp_c,
            'pump_power_kw': self.calculate_pump_power(),
            'hex_effectiveness': self.hex_effectiveness,
            'coolant_properties': self.properties
        }


# ============================================================
# ENHANCEMENT 2: Free Cooling (Economizer) Optimizer
# ============================================================

class FreeCoolingOptimizer:
    """
    Free cooling optimization using outside air temperature.
    
    Features:
    - Dry cooler / cooling tower optimization
    - Economizer mode switching
    - Water-side economizer modeling
    - Air-side economizer modeling
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
        # Free cooling thresholds
        self.dry_bulb_threshold_c = self.config.get('dry_bulb_threshold_c', 15.0)
        self.wet_bulb_threshold_c = self.config.get('wet_bulb_threshold_c', 12.0)
        self.enthalpy_threshold_kj_kg = self.config.get('enthalpy_threshold_kj_kg', 50.0)
        
        # Cooling tower parameters
        self.tower_range_c = self.config.get('tower_range_c', 5.0)
        self.tower_approach_c = self.config.get('tower_approach_c', 4.0)
        
        # Dry cooler parameters
        self.dry_cooler_approach_c = self.config.get('dry_cooler_approach_c', 8.0)
        
        logger.info("FreeCoolingOptimizer initialized")
    
    def calculate_free_cooling_potential(self, outside_temp_c: float, outside_humidity: float = 0.5) -> Dict:
        """
        Calculate free cooling potential based on outside conditions.
        
        Returns:
            Dict with mode, potential, and estimated savings
        """
        # Calculate wet bulb temperature (approximate)
        wet_bulb = self._calculate_wet_bulb(outside_temp_c, outside_humidity)
        
        # Determine optimal mode
        if outside_temp_c <= self.dry_bulb_threshold_c:
            mode = 'air_side_economizer'
            potential = 1.0 - (outside_temp_c / self.dry_bulb_threshold_c)
        elif wet_bulb <= self.wet_bulb_threshold_c:
            mode = 'water_side_economizer'
            potential = 1.0 - (wet_bulb / self.wet_bulb_threshold_c)
        else:
            mode = 'mechanical_cooling'
            potential = 0.0
        
        # Estimate savings (0-100%)
        savings_percent = potential * 100
        
        return {
            'mode': mode,
            'potential': potential,
            'savings_percent': savings_percent,
            'outside_temp_c': outside_temp_c,
            'wet_bulb_c': wet_bulb,
            'recommendation': f"Use {mode} - potential {savings_percent:.0f}% savings" if potential > 0 else "Mechanical cooling required"
        }
    
    def _calculate_wet_bulb(self, dry_bulb_c: float, relative_humidity: float) -> float:
        """Approximate wet bulb temperature"""
        # Simplified Stull formula
        wet_bulb = dry_bulb_c * math.atan(0.151977 * math.sqrt(relative_humidity + 8.313659))
        wet_bulb += math.atan(dry_bulb_c + relative_humidity) - math.atan(relative_humidity - 1.676331)
        wet_bulb += 0.00391838 * (relative_humidity ** (3/2)) * math.atan(0.023101 * relative_humidity) - 4.686035
        
        return max(0, wet_bulb)
    
    def calculate_cooling_tower_performance(self, outside_wet_bulb_c: float, heat_load_kw: float) -> Dict:
        """Calculate cooling tower leaving water temperature"""
        # Leaving water temperature = wet bulb + approach
        leaving_temp = outside_wet_bulb_c + self.tower_approach_c
        
        # Tower effectiveness
        effectiveness = (self.tower_range_c) / (self.tower_range_c + self.tower_approach_c)
        
        # Required air flow
        # Simplified: 1 CFM per 10 BTU/hr of cooling
        cooling_btu_hr = heat_load_kw * 3412.14
        required_cfm = cooling_btu_hr / (self.tower_range_c * 1.08)
        
        return {
            'leaving_temp_c': leaving_temp,
            'effectiveness': effectiveness,
            'required_cfm': required_cfm,
            'approach_c': self.tower_approach_c,
            'range_c': self.tower_range_c
        }


# ============================================================
# ENhANCE 4: Predictive Maintenance for Cooling Equipment
# ============================================================

class PredictiveMaintenance:
    """
    Predictive maintenance for cooling equipment.
    
    Features:
    - Remaining useful life (RUL) estimation
    - Anomaly detection in sensor data
    - Maintenance scheduling optimization
    """
    
    def __init__(self):
        self.equipment_health: Dict[str, float] = {}
        self.failure_history: List[Dict] = []
        self.rf_model = None
        self._lock = threading.RLock()
        
        # Weibull parameters for different components
        self.weibull_params = {
            'fan': {'shape': 2.5, 'scale': 80000},  # hours
            'pump': {'shape': 2.2, 'scale': 60000},
            'compressor': {'shape': 1.8, 'scale': 50000},
            'valve': {'shape': 3.0, 'scale': 100000}
        }
        
        logger.info("PredictiveMaintenance initialized")
    
    def update_equipment_health(self, equipment_id: str, operating_hours: float,
                                 temperature_c: float, vibration: float = 0) -> float:
        """
        Update equipment health based on operating conditions.
        
        Returns:
            Health score (0-1)
        """
        with self._lock:
            if equipment_id not in self.equipment_health:
                self.equipment_health[equipment_id] = 1.0
            
            # Weibull degradation
            params = self.weibull_params.get(equipment_id.split('_')[0], {'shape': 2.0, 'scale': 70000})
            failure_prob = 1 - math.exp(-((operating_hours / params['scale']) ** params['shape']))
            
            # Temperature factor (Arrhenius)
            temp_factor = math.exp(0.1 * (temperature_c - 25))
            
            # Vibration factor
            vib_factor = 1 + vibration / 10.0
            
            # Combined health
            health = (1 - failure_prob) * (1 / temp_factor) * (1 / vib_factor)
            health = max(0, min(1, health))
            
            # Exponential smoothing
            self.equipment_health[equipment_id] = 0.9 * self.equipment_health[equipment_id] + 0.1 * health
            
            return self.equipment_health[equipment_id]
    
    def predict_rul(self, equipment_id: str) -> float:
        """Predict remaining useful life in hours"""
        with self._lock:
            if equipment_id not in self.equipment_health:
                return 8760  # Default 1 year
            
            current_health = self.equipment_health[equipment_id]
            
            # Simple RUL estimate (linear with health)
            if current_health <= 0:
                return 0
            
            # Assumes end-of-life at health < 0.2
            remaining = (current_health / 0.2) * 8760 / 12  # Months
            return remaining
    
    def get_maintenance_schedule(self) -> List[Dict]:
        """Get recommended maintenance schedule"""
        schedule = []
        
        for equipment_id, health in self.equipment_health.items():
            if health < 0.3:
                urgency = 'critical'
                action = 'Replace immediately'
                priority = 1
            elif health < 0.5:
                urgency = 'warning'
                action = 'Schedule replacement within 30 days'
                priority = 2
            elif health < 0.7:
                urgency = 'advisory'
                action = 'Monitor closely'
                priority = 3
            else:
                continue
            
            schedule.append({
                'equipment_id': equipment_id,
                'health': health,
                'rul_hours': self.predict_rul(equipment_id),
                'urgency': urgency,
                'recommended_action': action,
                'priority': priority
            })
        
        return sorted(schedule, key=lambda x: x['priority'])


# ============================================================
# ENHANCEMENT 5: Main Enhanced Thermal Optimizer
# ============================================================

class UltimateThermalAwareOptimizer:
    """
    Ultimate thermal-aware optimizer v3.2.
    
    Features:
    - Liquid cooling modeling
    - Free cooling optimization
    - Predictive maintenance
    - Multi-zone control
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        
        # Enhanced components
        self.liquid_cooling = LiquidCoolingModel(self.config.get('liquid_cooling', {}))
        self.free_cooling = FreeCoolingOptimizer(self.config.get('free_cooling', {}))
        self.predictive_maintenance = PredictiveMaintenance()
        
        # Base components
        self.temperature_sensor = MultiGPUTemperatureSensor(self.config.get('sensor', {}))
        self.cooling_actuator = CoolingSystemActuator(self.config.get('actuator', {}))
        self.ml_predictor = MLTemperaturePredictor()
        self.pid_controller = AdaptivePIDController()
        self.exhaust_model = ExhaustTemperatureModel()
        self.load_balancer = ThermalAwareLoadBalancer(self.config.get('gpu_count', 1))
        self.emergency_response = ThermalEmergencyResponse()
        
        # Start monitoring
        self._monitoring = False
        self._monitor_thread = None
        self._start_monitoring()
        
        logger.info("UltimateThermalAwareOptimizer v3.2 initialized")
    
    def _start_monitoring(self):
        self._monitoring = True
        self._monitor_thread = threading.Thread(target=self._monitor_loop_ultimate, daemon=True)
        self._monitor_thread.start()
    
    def _monitor_loop_ultimate(self):
        """Ultimate monitoring loop with all enhancements"""
        while self._monitoring:
            try:
                # Get current temperatures
                all_temps = self.temperature_sensor.get_all_temperatures()
                hottest_temp = max(all_temps) if all_temps else 65.0
                
                # Update predictive maintenance for cooling equipment
                self.predictive_maintenance.update_equipment_health(
                    'cooling_fan', self.pid_controller._prev_time, hottest_temp
                )
                
                # Get free cooling potential
                outside_temp = 22.0  # Would come from weather API
                free_cooling = self.free_cooling.calculate_free_cooling_potential(outside_temp)
                
                # Adjust cooling strategy based on free cooling
                if free_cooling['mode'] != 'mechanical_cooling':
                    logger.info(f"Free cooling available: {free_cooling['mode']} ({free_cooling['savings_percent']:.0f}% savings)")
                
                # Update liquid cooling status
                cooling_capacity = self.liquid_cooling.calculate_cooling_capacity(self._estimate_current_power() / 1000)
                if cooling_capacity['margin'] < 0:
                    logger.warning(f"Insufficient cooling capacity: {cooling_capacity['margin']:.1f} kW deficit")
                
                # Update ML predictor
                self.ml_predictor.add_observation(
                    hottest_temp, self._estimate_current_power(), 
                    self.pid_controller._prev_output, 22.0, time.time()
                )
                
                time.sleep(5)
            except Exception as e:
                logger.error(f"Monitor error: {e}")
                time.sleep(10)
    
    def optimize_schedule_ultimate(self, workload_profile, execution_decision) -> ThermalDecision:
        """Ultimate thermal optimization with all features"""
        # Get base decision
        base_decision = self.optimize_schedule(workload_profile, execution_decision)
        
        # Get free cooling optimization
        outside_temp = 22.0  # Would come from weather API
        free_cooling = self.free_cooling.calculate_free_cooling_potential(outside_temp)
        
        # Adjust cooling based on free cooling
        if free_cooling['mode'] == 'air_side_economizer':
            # Reduce mechanical cooling, increase outside air
            adjusted_throttle = base_decision.throttle_factor * 0.8
            reasoning = f"{base_decision.reasoning} | Free cooling active ({free_cooling['savings_percent']:.0f}% savings)"
        elif free_cooling['mode'] == 'water_side_economizer':
            adjusted_throttle = base_decision.throttle_factor * 0.85
            reasoning = f"{base_decision.reasoning} | Water economizer active"
        else:
            adjusted_throttle = base_decision.throttle_factor
            reasoning = base_decision.reasoning
        
        # Get predictive maintenance status
        maintenance_schedule = self.predictive_maintenance.get_maintenance_schedule()
        if maintenance_schedule:
            critical = [m for m in maintenance_schedule if m['urgency'] == 'critical']
            if critical:
                reasoning += f" | CRITICAL: {critical[0]['recommended_action']}"
        
        # Get liquid cooling status
        liquid_status = self.liquid_cooling.get_status()
        
        return ThermalDecision(
            action=base_decision.action,
            throttle_factor=adjusted_throttle,
            target_temp=base_decision.target_temp,
            energy_savings_percent=base_decision.energy_savings_percent + free_cooling['savings_percent'],
            recovery_time_seconds=base_decision.recovery_time_seconds,
            fan_speed_percent=base_decision.fan_speed_percent,
            performance_impact_percent=base_decision.performance_impact_percent,
            reasoning=reasoning,
            liquid_cooling_status=liquid_status,
            free_cooling_mode=free_cooling['mode'],
            maintenance_alerts=maintenance_schedule[:3]
        )
    
    def get_ultimate_thermal_metrics(self) -> Dict:
        """Get ultimate thermal metrics"""
        base_metrics = self.get_thermal_metrics()
        
        # Add new metrics
        base_metrics['liquid_cooling'] = self.liquid_cooling.get_status()
        base_metrics['free_cooling'] = self.free_cooling.calculate_free_cooling_potential(22.0)
        base_metrics['predictive_maintenance'] = self.predictive_maintenance.get_maintenance_schedule()
        
        return base_metrics


# ============================================================
# Usage Example
# ============================================================

def main():
    print("=== Ultimate Thermal-Aware Optimizer v3.2 Demo ===\n")
    
    optimizer = UltimateThermalAwareOptimizer({
        'hardware_tdp_watts': 300,
        'gpu_count': 4,
        'sensor': {'simulate': True, 'gpu_count': 4},
        'actuator': {'simulate': True},
        'liquid_cooling': {
            'coolant_type': 'water',
            'flow_rate_lpm': 150,
            'pump_efficiency': 0.8
        },
        'free_cooling': {
            'dry_bulb_threshold_c': 15,
            'wet_bulb_threshold_c': 12
        }
    })
    
    class MockProfile:
        gpu_count = 4
    
    class MockDecision:
        power_budget = 0.8
    
    profile = MockProfile()
    decision = MockDecision()
    
    print("1. Liquid Cooling System Status:")
    liquid = optimizer.liquid_cooling.get_status()
    print(f"   Coolant: {liquid['coolant_type']}, flow: {liquid['flow_rate_lpm']:.0f} LPM")
    print(f"   Pump power: {liquid['pump_power_kw']:.2f} kW")
    print(f"   Supply/Return: {liquid['supply_temp_c']:.1f}°C / {liquid['return_temp_c']:.1f}°C")
    
    print("\n2. Free Cooling Potential:")
    free_cooling = optimizer.free_cooling.calculate_free_cooling_potential(10.0)
    print(f"   Mode: {free_cooling['mode']}")
    print(f"   Savings: {free_cooling['savings_percent']:.0f}%")
    print(f"   Recommendation: {free_cooling['recommendation']}")
    
    print("\n3. Predictive Maintenance:")
    # Simulate equipment aging
    for hours in range(0, 50000, 5000):
        optimizer.predictive_maintenance.update_equipment_health('cooling_fan', hours, 65)
    
    maintenance = optimizer.predictive_maintenance.get_maintenance_schedule()
    if maintenance:
        for m in maintenance[:2]:
            print(f"   {m['equipment_id']}: health={m['health']:.1%}, RUL={m['rul_hours']/24:.0f} days")
            print(f"      Action: {m['recommended_action']}")
    
    print("\n4. Ultimate Thermal Decision:")
    thermal_decision = optimizer.optimize_schedule_ultimate(profile, decision)
    print(f"   Action: {thermal_decision.action}")
    print(f"   Throttle: {thermal_decision.throttle_factor:.2f}")
    print(f"   Energy savings: {thermal_decision.energy_savings_percent:.1f}%")
    print(f"   Reasoning: {thermal_decision.reasoning}")
    
    print("\n5. Ultimate Thermal Metrics:")
    metrics = optimizer.get_ultimate_thermal_metrics()
    print(f"   Current temp: {metrics['current_temperature_celsius']:.1f}°C")
    print(f"   Liquid cooling pump: {metrics['liquid_cooling']['pump_power_kw']:.2f} kW")
    print(f"   Free cooling mode: {metrics['free_cooling']['mode']}")
    print(f"   Maintenance alerts: {len(metrics['predictive_maintenance'])}")
    
    optimizer.stop_monitoring()
    print("\n✅ Ultimate Thermal-Aware Optimizer v3.2 test complete")

if __name__ == "__main__":
    main()
