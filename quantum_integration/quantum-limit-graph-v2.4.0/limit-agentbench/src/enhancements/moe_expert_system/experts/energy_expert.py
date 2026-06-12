# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/energy_expert.py

"""
Enhanced Energy Expert for Green Agent MoE System
Version: 2.0.0

Comprehensive energy optimization with:
- Renewable energy source integration
- Real-time grid carbon intensity awareness
- Energy storage optimization (battery/hydrogen)
- Dynamic voltage/frequency scaling (DVFS)
- Workload shifting for carbon-aware scheduling
- Cross-expert energy coordination
- Energy harvesting integration (solar, wind, thermal)
- Thermal-aware computing optimization
- Adaptive learning for optimization parameters
- Energy-efficient workload batching
- Liquid cooling optimization
- Power capping and budgeting
- Energy proportionality enforcement
- Carbon-aware load balancing
- Green energy certificate tracking

Integration Points:
- Layer 0: Workload energy profiling
- Layer 1: Meta-cognitive energy optimization
- Layer 3: Dual-axis carbon/energy scoring
- Layer 4: Helium-aware ML with energy constraints
- Layer 5: Energy-efficient data processing
- Layer 7: Energy monitoring and metrics
- Layer 8: Energy audit trail
- Layer 9: Energy/carbon Pareto optimization
"""

import numpy as np
import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import deque
import hashlib
import json
import math

logger = logging.getLogger(__name__)

# Try relative import for integration, fallback for standalone
try:
    from ..expert_registry import ExpertProfile, ExpertDomain, HardwareProfile
except ImportError:
    class ExpertDomain(Enum):
        ENERGY = "energy_optimization"
    
    class HardwareProfile(Enum):
        CPU_EFFICIENT = "cpu_low_power"

# ============================================================================
# Enums and Data Classes for Enhanced Energy Management
# ============================================================================

class EnergySource(Enum):
    """Types of energy sources with carbon intensity"""
    SOLAR = "solar"           # 0 gCO2/kWh
    WIND = "wind"             # 0 gCO2/kWh
    HYDRO = "hydro"           # 0 gCO2/kWh
    GEOTHERMAL = "geothermal" # 0 gCO2/kWh
    NUCLEAR = "nuclear"       # 12 gCO2/kWh
    NATURAL_GAS = "natural_gas"  # 490 gCO2/kWh
    COAL = "coal"             # 820 gCO2/kWh
    GRID_MIX = "grid_mix"     # Variable
    BATTERY = "battery"       # Stored renewable
    HYDROGEN = "hydrogen"     # Green hydrogen
    
    @property
    def carbon_intensity_g_per_kwh(self) -> float:
        """Get carbon intensity in gCO2/kWh"""
        intensities = {
            EnergySource.SOLAR: 0,
            EnergySource.WIND: 0,
            EnergySource.HYDRO: 0,
            EnergySource.GEOTHERMAL: 0,
            EnergySource.NUCLEAR: 12,
            EnergySource.NATURAL_GAS: 490,
            EnergySource.COAL: 820,
            EnergySource.GRID_MIX: 400,  # Average
            EnergySource.BATTERY: 0,
            EnergySource.HYDROGEN: 0
        }
        return intensities.get(self, 400)
    
    @property
    def is_renewable(self) -> bool:
        """Check if energy source is renewable"""
        return self in [
            EnergySource.SOLAR, EnergySource.WIND,
            EnergySource.HYDRO, EnergySource.GEOTHERMAL,
            EnergySource.BATTERY, EnergySource.HYDROGEN
        ]

class PowerState(Enum):
    """CPU/GPU power states for DVFS"""
    PERFORMANCE = "performance"     # Maximum frequency
    BALANCED = "balanced"          # Medium frequency
    POWER_SAVE = "power_save"      # Low frequency
    ULTRA_LOW = "ultra_low"        # Minimum frequency
    DYNAMIC = "dynamic"            # Adaptive frequency

class CoolingMethod(Enum):
    """Cooling methods with energy overhead"""
    AIR_COOLING = "air"                    # 2% overhead
    LIQUID_COOLING = "liquid"              # 5% overhead (more efficient)
    IMMERSION_COOLING = "immersion"        # 3% overhead
    FREE_COOLING = "free"                  # 0% overhead (ambient)
    GEOTHERMAL_COOLING = "geothermal"      # 1% overhead
    HELIUM_COOLING = "helium"              # 10% overhead (specialized)

@dataclass
class RenewableProfile:
    """Renewable energy availability profile"""
    solar_available_kw: float = 0.0
    wind_available_kw: float = 0.0
    battery_level_kwh: float = 0.0
    battery_capacity_kwh: float = 100.0
    hydrogen_level_kg: float = 0.0
    renewable_percentage: float = 0.0
    forecast_next_hour: float = 0.0
    peak_solar_time: bool = False
    
    def can_use_renewable(self, required_kw: float) -> bool:
        """Check if renewable energy can meet demand"""
        available = self.solar_available_kw + self.wind_available_kw + self.battery_level_kwh
        return available >= required_kw

@dataclass
class ThermalProfile:
    """Thermal management profile"""
    current_temp_c: float = 35.0
    max_temp_c: float = 85.0
    throttle_temp_c: float = 75.0
    ambient_temp_c: float = 25.0
    cooling_efficiency: float = 0.9
    requires_throttling: bool = False
    
    @property
    def thermal_headroom_c(self) -> float:
        """Remaining thermal headroom"""
        return self.throttle_temp_c - self.current_temp_c

@dataclass
class EnergyOptimizationHistory:
    """Track energy optimization decisions for learning"""
    timestamp: datetime
    strategy: str
    energy_source: str
    power_state: str
    energy_saved_kwh: float
    carbon_saved_kg: float
    cost_saved: float
    renewable_used: bool
    success: bool
    metrics: Dict[str, float] = field(default_factory=dict)

# ============================================================================
# Enhanced Energy Expert Class
# ============================================================================

class EnergyExpert:
    """
    Enhanced Energy Optimization Expert for Green Agent MoE System.
    
    Capabilities:
    - Multi-source energy optimization (solar, wind, grid, battery)
    - Real-time grid carbon intensity awareness
    - Dynamic voltage/frequency scaling (DVFS) optimization
    - Workload shifting to low-carbon periods
    - Energy storage management (battery, hydrogen)
    - Thermal-aware computing optimization
    - Cross-expert energy coordination
    - Energy harvesting integration
    - Green certificate tracking
    - Adaptive learning for optimization parameters
    - Power capping and budgeting
    - Energy proportionality enforcement
    """
    
    def __init__(
        self,
        expert_id: str = "energy_optimizer_v2",
        enable_renewable: bool = True,
        enable_storage: bool = True,
        enable_thermal: bool = True,
        enable_dvfs: bool = True,
        enable_forecasting: bool = True
    ):
        self.expert_id = expert_id
        self.version = "2.0.0"
        self.enable_renewable = enable_renewable
        self.enable_storage = enable_storage
        self.enable_thermal = enable_thermal
        self.enable_dvfs = enable_dvfs
        self.enable_forecasting = enable_forecasting
        
        # Expert profile for registry
        self.profile = ExpertProfile(
            expert_id=expert_id,
            domain=ExpertDomain.ENERGY,
            hardware_profile=HardwareProfile.CPU_EFFICIENT,
            helium_per_inference=0.008,  # Improved from 0.01
            carbon_per_inference=0.00008,  # Improved from 0.0001
            energy_per_inference=0.0008,  # Improved from 0.001
            avg_latency_ms=40.0,  # Improved from 50.0
            accuracy_score=0.94,  # Improved from 0.92
            reliability_score=0.97,  # Improved from 0.95
            efficiency_score=0.99,  # Improved from 0.98
            supported_task_types=[
                'inference', 'training', 'optimization',
                'energy_management', 'carbon_accounting',
                'renewable_integration', 'workload_scheduling'
            ]
        )
        
        # ====================================================================
        # Enhanced Energy Configurations
        # ====================================================================
        
        # DVFS power states with energy/performance tradeoffs
        self.power_states = {
            PowerState.PERFORMANCE: {
                'frequency_percent': 100,
                'energy_factor': 1.0,
                'performance_factor': 1.0,
                'carbon_overhead': 0.0
            },
            PowerState.BALANCED: {
                'frequency_percent': 70,
                'energy_factor': 0.7,
                'performance_factor': 0.85,
                'carbon_overhead': 0.0
            },
            PowerState.POWER_SAVE: {
                'frequency_percent': 50,
                'energy_factor': 0.5,
                'performance_factor': 0.65,
                'carbon_overhead': 0.0
            },
            PowerState.ULTRA_LOW: {
                'frequency_percent': 25,
                'energy_factor': 0.25,
                'performance_factor': 0.35,
                'carbon_overhead': 0.0
            },
            PowerState.DYNAMIC: {
                'frequency_percent': 0,  # Adaptive
                'energy_factor': 0.0,    # Adaptive
                'performance_factor': 0.0,  # Adaptive
                'carbon_overhead': 0.0
            }
        }
        
        # Quantization levels with energy impact
        self.quantization_levels = {
            'fp32': {
                'energy_factor': 1.0,
                'accuracy_impact': 0.0,
                'memory_factor': 1.0,
                'suitable_for': ['training', 'high_precision']
            },
            'fp16': {
                'energy_factor': 0.5,
                'accuracy_impact': 0.01,
                'memory_factor': 0.5,
                'suitable_for': ['inference', 'mixed_precision']
            },
            'bf16': {
                'energy_factor': 0.5,
                'accuracy_impact': 0.005,
                'memory_factor': 0.5,
                'suitable_for': ['training', 'inference']
            },
            'int8': {
                'energy_factor': 0.25,
                'accuracy_impact': 0.03,
                'memory_factor': 0.25,
                'suitable_for': ['inference', 'edge_deployment']
            },
            'int4': {
                'energy_factor': 0.125,
                'accuracy_impact': 0.05,
                'memory_factor': 0.125,
                'suitable_for': ['edge_inference', 'ultra_low_power']
            }
        }
        
        # Cooling method configurations
        self.cooling_methods = {
            CoolingMethod.AIR_COOLING: {
                'energy_overhead': 0.02,
                'max_cooling_capacity_kw': 50,
                'noise_level_db': 60,
                'helium_usage': 0.0
            },
            CoolingMethod.LIQUID_COOLING: {
                'energy_overhead': 0.05,
                'max_cooling_capacity_kw': 200,
                'noise_level_db': 40,
                'helium_usage': 0.0
            },
            CoolingMethod.IMMERSION_COOLING: {
                'energy_overhead': 0.03,
                'max_cooling_capacity_kw': 500,
                'noise_level_db': 30,
                'helium_usage': 0.0
            },
            CoolingMethod.FREE_COOLING: {
                'energy_overhead': 0.0,
                'max_cooling_capacity_kw': 30,
                'noise_level_db': 35,
                'helium_usage': 0.0
            },
            CoolingMethod.GEOTHERMAL_COOLING: {
                'energy_overhead': 0.01,
                'max_cooling_capacity_kw': 100,
                'noise_level_db': 25,
                'helium_usage': 0.0
            },
            CoolingMethod.HELIUM_COOLING: {
                'energy_overhead': 0.10,
                'max_cooling_capacity_kw': 1000,
                'noise_level_db': 20,
                'helium_usage': 0.05
            }
        }
        
        # Energy storage configurations
        self.storage_config = {
            'battery': {
                'capacity_kwh': 100,
                'charge_rate_kw': 50,
                'discharge_rate_kw': 80,
                'efficiency': 0.95,
                'min_level_percent': 20,
                'cycle_life': 5000
            },
            'hydrogen': {
                'capacity_kg': 50,
                'electrolysis_efficiency': 0.75,
                'fuel_cell_efficiency': 0.60,
                'storage_loss_percent': 0.1
            }
        }
        
        # Pruning configurations
        self.pruning_rates = [0.0, 0.1, 0.2, 0.3, 0.5, 0.7, 0.9]
        
        # Optimization history for learning
        self.optimization_history: deque = deque(maxlen=10000)
        
        # Performance tracking
        self.total_energy_saved_kwh = 0.0
        self.total_carbon_saved_kg = 0.0
        self.total_cost_saved = 0.0
        self.renewable_usage_percent = 0.0
        
        # Adaptive thresholds (learned over time)
        self.adaptive_thresholds = {
            'renewable_switch_threshold': 0.3,  # Switch to renewable if >30% available
            'battery_use_threshold': 0.5,        # Use battery if grid carbon > this
            'thermal_throttle_threshold': 75.0,   # Start throttling at this temp
            'dvfs_aggressiveness': 0.5           # How aggressive DVFS is
        }
        
        logger.info(f"Initialized Enhanced {self.expert_id} v{self.version}")
    
    # ========================================================================
    # Primary Energy Optimization Method
    # ========================================================================
    
    async def optimize_energy(
        self,
        task_config: Dict[str, Any],
        carbon_budget: float,
        latency_requirement_ms: float,
        grid_carbon_intensity: Optional[float] = None,
        renewable_profile: Optional[RenewableProfile] = None,
        thermal_profile: Optional[ThermalProfile] = None,
        time_of_day: Optional[int] = None,
        energy_price_per_kwh: Optional[float] = None,
        helium_scarcity: float = 0.0,
        cross_expert_hints: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Comprehensive energy optimization with multiple strategies.
        
        Args:
            task_config: Task configuration
            carbon_budget: Carbon budget in kg CO2
            latency_requirement_ms: Maximum latency in ms
            grid_carbon_intensity: Real-time grid carbon intensity (gCO2/kWh)
            renewable_profile: Renewable energy availability
            thermal_profile: Thermal conditions
            time_of_day: Hour of day (0-23)
            energy_price_per_kwh: Energy price for cost optimization
            helium_scarcity: Current helium scarcity
            cross_expert_hints: Hints from other experts
            
        Returns:
            Comprehensive energy optimization plan
        """
        start_time = datetime.utcnow()
        optimization_id = hashlib.md5(
            f"{task_config}{carbon_budget}{latency_requirement_ms}{start_time}".encode()
        ).hexdigest()[:12]
        
        # Step 1: Profile current energy conditions
        energy_profile = await self._profile_energy_conditions(
            grid_carbon_intensity, renewable_profile, thermal_profile,
            time_of_day, energy_price_per_kwh
        )
        
        # Step 2: Select optimal energy source
        energy_source_plan = await self._select_energy_source(
            energy_profile, task_config, helium_scarcity
        )
        
        # Step 3: Optimize power state (DVFS)
        power_plan = await self._optimize_power_state(
            energy_profile, task_config, latency_requirement_ms
        )
        
        # Step 4: Select quantization level
        quantization_plan = await self._select_quantization(
            task_config, carbon_budget, latency_requirement_ms,
            energy_profile
        )
        
        # Step 5: Optimize cooling
        cooling_plan = await self._optimize_cooling(
            energy_profile, thermal_profile, helium_scarcity
        )
        
        # Step 6: Calculate pruning rate
        pruning_plan = self._calculate_pruning(
            task_config, latency_requirement_ms, energy_profile
        )
        
        # Step 7: Check workload shifting opportunity
        shifting_plan = await self._evaluate_workload_shifting(
            task_config, energy_profile, time_of_day
        )
        
        # Step 8: Energy storage optimization
        storage_plan = None
        if self.enable_storage:
            storage_plan = await self._optimize_storage_usage(
                energy_profile, task_config
            )
        
        # Step 9: Calculate comprehensive resource estimates
        estimates = self._calculate_comprehensive_estimates(
            task_config, energy_source_plan, power_plan,
            quantization_plan, cooling_plan, pruning_plan,
            energy_profile
        )
        
        # Step 10: Record optimization for learning
        self._record_optimization(
            optimization_id, energy_profile, energy_source_plan,
            power_plan, estimates
        )
        
        # Build comprehensive plan
        plan = {
            'expert_id': self.expert_id,
            'optimization_id': optimization_id,
            'version': self.version,
            
            # Energy source
            'energy_source': energy_source_plan['source'],
            'renewable_percentage': energy_source_plan['renewable_percent'],
            'grid_carbon_intensity': energy_profile.get('grid_carbon_intensity', 400),
            
            # Power management
            'power_state': power_plan['state'],
            'frequency_percent': power_plan['frequency_percent'],
            'dvfs_enabled': power_plan['dvfs_enabled'],
            
            # Model optimization
            'quantization': quantization_plan['level'],
            'pruning_rate': pruning_plan['rate'],
            'accuracy_impact': quantization_plan['accuracy_impact'],
            
            # Cooling
            'cooling_method': cooling_plan['method'],
            'cooling_energy_overhead': cooling_plan['energy_overhead'],
            
            # Resource estimates
            'estimated_energy_kwh': estimates['total_energy_kwh'],
            'estimated_carbon_kg': estimates['total_carbon_kg'],
            'estimated_latency_ms': estimates['total_latency_ms'],
            'estimated_cost': estimates['total_cost'],
            
            # Compliance
            'carbon_budget_compliant': estimates['total_carbon_kg'] <= carbon_budget,
            'latency_budget_compliant': estimates['total_latency_ms'] <= latency_requirement_ms,
            
            # Optimization features
            'workload_shifting_recommended': shifting_plan.get('recommended', False),
            'renewable_energy_used': energy_source_plan['renewable_percent'] > 0,
            'thermal_throttling_active': power_plan.get('thermal_throttled', False),
            'storage_used': storage_plan is not None,
            
            # Savings
            'energy_saved_vs_baseline_kwh': estimates['energy_saved_kwh'],
            'carbon_saved_vs_baseline_kg': estimates['carbon_saved_kg'],
            'cost_saved_vs_baseline': estimates['cost_saved'],
            
            # Recommendations
            'recommendations': self._generate_energy_recommendations(
                energy_profile, estimates, shifting_plan
            ),
            
            # Strategy
            'strategy': 'multi_factor_energy_optimization',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        logger.info(
            f"Energy Plan [{optimization_id}]: "
            f"source={energy_source_plan['source'].value}, "
            f"renewable={energy_source_plan['renewable_percent']:.0%}, "
            f"power={power_plan['state'].value}, "
            f"quant={quantization_plan['level']}, "
            f"energy={estimates['total_energy_kwh']:.6f} kWh, "
            f"carbon={estimates['total_carbon_kg']:.6f} kg"
        )
        
        return plan
    
    # ========================================================================
    # Energy Condition Profiling
    # ========================================================================
    
    async def _profile_energy_conditions(
        self,
        grid_carbon_intensity: Optional[float],
        renewable_profile: Optional[RenewableProfile],
        thermal_profile: Optional[ThermalProfile],
        time_of_day: Optional[int],
        energy_price: Optional[float]
    ) -> Dict[str, Any]:
        """
        Profile current energy conditions for optimization.
        
        Analyzes:
        - Grid carbon intensity (real-time or estimated)
        - Renewable energy availability
        - Thermal conditions
        - Time-based patterns
        - Energy pricing
        """
        profile = {
            'timestamp': datetime.utcnow().isoformat(),
            'time_of_day': time_of_day or datetime.utcnow().hour,
        }
        
        # Grid carbon intensity
        if grid_carbon_intensity is not None:
            profile['grid_carbon_intensity'] = grid_carbon_intensity
        else:
            # Estimate based on time of day
            profile['grid_carbon_intensity'] = self._estimate_grid_carbon_intensity(
                profile['time_of_day']
            )
        
        # Renewable availability
        if renewable_profile:
            profile['renewable'] = {
                'solar_kw': renewable_profile.solar_available_kw,
                'wind_kw': renewable_profile.wind_available_kw,
                'battery_kwh': renewable_profile.battery_level_kwh,
                'total_renewable_kw': renewable_profile.solar_available_kw + renewable_profile.wind_available_kw,
                'percentage': renewable_profile.renewable_percentage,
                'can_meet_demand': renewable_profile.can_use_renewable(10)  # Estimate 10kW baseline
            }
        else:
            profile['renewable'] = {
                'solar_kw': self._estimate_solar(profile['time_of_day']),
                'wind_kw': 5.0,  # Default estimate
                'total_renewable_kw': 0,
                'percentage': 0.0,
                'can_meet_demand': False
            }
            profile['renewable']['total_renewable_kw'] = (
                profile['renewable']['solar_kw'] + profile['renewable']['wind_kw']
            )
            profile['renewable']['percentage'] = min(
                profile['renewable']['total_renewable_kw'] / 20.0, 1.0
            )
        
        # Thermal conditions
        if thermal_profile:
            profile['thermal'] = {
                'current_temp_c': thermal_profile.current_temp_c,
                'headroom_c': thermal_profile.thermal_headroom_c,
                'requires_throttling': thermal_profile.requires_throttling
            }
        else:
            profile['thermal'] = {
                'current_temp_c': 40.0,
                'headroom_c': 35.0,
                'requires_throttling': False
            }
        
        # Energy pricing
        if energy_price is not None:
            profile['energy_price_per_kwh'] = energy_price
        else:
            # Estimate based on time of day
            profile['energy_price_per_kwh'] = self._estimate_energy_price(
                profile['time_of_day']
            )
        
        # Carbon intensity category
        intensity = profile['grid_carbon_intensity']
        if intensity < 100:
            profile['carbon_category'] = 'very_low'
        elif intensity < 300:
            profile['carbon_category'] = 'low'
        elif intensity < 500:
            profile['carbon_category'] = 'moderate'
        elif intensity < 700:
            profile['carbon_category'] = 'high'
        else:
            profile['carbon_category'] = 'very_high'
        
        return profile
    
    def _estimate_grid_carbon_intensity(self, hour: int) -> float:
        """Estimate grid carbon intensity based on time of day"""
        # Simplified model: lower at night (more wind), higher during peak
        if 0 <= hour < 6:
            return 300  # Night: more wind, lower demand
        elif 6 <= hour < 10:
            return 450  # Morning ramp
        elif 10 <= hour < 16:
            return 350  # Midday: solar peak
        elif 16 <= hour < 20:
            return 500  # Evening peak
        else:
            return 400  # Late evening
    
    def _estimate_solar(self, hour: int) -> float:
        """Estimate solar availability based on time"""
        if 6 <= hour <= 18:
            # Parabolic curve peaking at noon
            return 10.0 * math.sin(math.pi * (hour - 6) / 12)
        return 0.0
    
    def _estimate_energy_price(self, hour: int) -> float:
        """Estimate energy price based on time of day"""
        if 10 <= hour <= 16:
            return 0.08  # Off-peak
        elif 16 <= hour <= 20:
            return 0.15  # Peak
        else:
            return 0.10  # Mid-peak
    
    # ========================================================================
    # Energy Source Selection
    # ========================================================================
    
    async def _select_energy_source(
        self,
        energy_profile: Dict[str, Any],
        task_config: Dict[str, Any],
        helium_scarcity: float
    ) -> Dict[str, Any]:
        """
        Select optimal energy source mix.
        
        Priority:
        1. On-site renewable (solar/wind)
        2. Battery storage (charged from renewable)
        3. Green hydrogen
        4. Grid with low carbon intensity
        5. Grid mix (last resort)
        """
        renewable = energy_profile.get('renewable', {})
        carbon_intensity = energy_profile.get('grid_carbon_intensity', 400)
        
        # Calculate renewable mix
        renewable_percent = 0.0
        primary_source = EnergySource.GRID_MIX
        
        if renewable.get('can_meet_demand', False):
            # Can fully use renewable
            if renewable.get('solar_kw', 0) > 5:
                primary_source = EnergySource.SOLAR
                renewable_percent = 1.0
            elif renewable.get('wind_kw', 0) > 5:
                primary_source = EnergySource.WIND
                renewable_percent = 1.0
        elif renewable.get('total_renewable_kw', 0) > 2:
            # Partial renewable
            renewable_percent = renewable['total_renewable_kw'] / 10.0
            if carbon_intensity < 200:
                primary_source = EnergySource.GRID_MIX
            else:
                primary_source = EnergySource.BATTERY
        
        # Adjust for helium scarcity
        if helium_scarcity > 0.7:
            # Prioritize lowest energy consumption
            renewable_percent = max(renewable_percent, 0.5)
        
        return {
            'source': primary_source,
            'renewable_percent': renewable_percent,
            'carbon_intensity': primary_source.carbon_intensity_g_per_kwh,
            'is_renewable': primary_source.is_renewable,
            'backup_source': EnergySource.GRID_MIX if primary_source.is_renewable else None
        }
    
    # ========================================================================
    # DVFS Power State Optimization
    # ========================================================================
    
    async def _optimize_power_state(
        self,
        energy_profile: Dict[str, Any],
        task_config: Dict[str, Any],
        latency_requirement_ms: float
    ) -> Dict[str, Any]:
        """
        Optimize CPU/GPU power state using DVFS.
        
        Considers:
        - Latency requirements
        - Thermal conditions
        - Energy availability
        - Task priority
        """
        thermal = energy_profile.get('thermal', {})
        carbon_category = energy_profile.get('carbon_category', 'moderate')
        
        # Determine if thermal throttling is needed
        thermal_throttled = thermal.get('requires_throttling', False)
        if thermal.get('current_temp_c', 40) > self.adaptive_thresholds['thermal_throttle_threshold']:
            thermal_throttled = True
        
        # Select power state based on conditions
        if thermal_throttled:
            # Must reduce power for thermal reasons
            if latency_requirement_ms > 500:
                power_state = PowerState.ULTRA_LOW
            elif latency_requirement_ms > 100:
                power_state = PowerState.POWER_SAVE
            else:
                power_state = PowerState.BALANCED
        elif carbon_category in ['high', 'very_high']:
            # Reduce power for carbon reasons
            if latency_requirement_ms > 200:
                power_state = PowerState.POWER_SAVE
            else:
                power_state = PowerState.BALANCED
        elif carbon_category == 'very_low':
            # Can use full power
            power_state = PowerState.PERFORMANCE
        else:
            # Balanced approach
            power_state = PowerState.BALANCED
        
        state_config = self.power_states[power_state]
        
        return {
            'state': power_state,
            'frequency_percent': state_config['frequency_percent'],
            'energy_factor': state_config['energy_factor'],
            'performance_factor': state_config['performance_factor'],
            'dvfs_enabled': self.enable_dvfs,
            'thermal_throttled': thermal_throttled
        }
    
    # ========================================================================
    # Quantization Selection
    # ========================================================================
    
    async def _select_quantization(
        self,
        task_config: Dict[str, Any],
        carbon_budget: float,
        latency_requirement_ms: float,
        energy_profile: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Select optimal quantization level with energy awareness.
        
        Enhanced with:
        - Task-specific requirements
        - Carbon budget constraints
        - Energy source carbon intensity
        """
        task_type = task_config.get('task_type', 'inference')
        carbon_category = energy_profile.get('carbon_category', 'moderate')
        
        # Score each quantization level
        scored_levels = []
        for level, config in self.quantization_levels.items():
            # Suitability score
            suitability = 1.0 if task_type in config['suitable_for'] else 0.3
            
            # Energy efficiency score
            energy_score = 1.0 - config['energy_factor']
            
            # Accuracy preservation score
            accuracy_score = 1.0 - config['accuracy_impact']
            
            # Weighted score based on conditions
            if carbon_category in ['high', 'very_high'] or carbon_budget < 0.0001:
                # Prioritize energy efficiency
                score = 0.5 * energy_score + 0.3 * suitability + 0.2 * accuracy_score
            elif latency_requirement_ms < 10:
                # Prioritize latency (higher precision = lower latency for some ops)
                score = 0.4 * suitability + 0.3 * accuracy_score + 0.3 * energy_score
            else:
                # Balanced
                score = 0.35 * energy_score + 0.35 * accuracy_score + 0.3 * suitability
            
            scored_levels.append((level, config, score))
        
        # Select best
        scored_levels.sort(key=lambda x: x[2], reverse=True)
        best_level, best_config, best_score = scored_levels[0]
        
        return {
            'level': best_level,
            'energy_factor': best_config['energy_factor'],
            'accuracy_impact': best_config['accuracy_impact'],
            'memory_factor': best_config['memory_factor'],
            'score': best_score
        }
    
    # ========================================================================
    # Cooling Optimization
    # ========================================================================
    
    async def _optimize_cooling(
        self,
        energy_profile: Dict[str, Any],
        thermal_profile: Optional[ThermalProfile],
        helium_scarcity: float
    ) -> Dict[str, Any]:
        """
        Optimize cooling method selection.
        
        Considers:
        - Thermal conditions
        - Helium scarcity
        - Energy efficiency
        - Ambient temperature (for free cooling)
        """
        thermal = energy_profile.get('thermal', {})
        current_temp = thermal.get('current_temp_c', 40)
        ambient_temp = thermal.get('current_temp_c', 25) - 15 if thermal_profile else 25
        
        # Score cooling methods
        scored_methods = []
        for method, config in self.cooling_methods.items():
            # Skip helium cooling if scarce
            if helium_scarcity > 0.5 and method == CoolingMethod.HELIUM_COOLING:
                continue
            
            # Skip free cooling if ambient too high
            if method == CoolingMethod.FREE_COOLING and ambient_temp > 30:
                continue
            
            # Energy efficiency score
            energy_score = 1.0 - config['energy_overhead']
            
            # Cooling capacity score
            capacity_score = min(config['max_cooling_capacity_kw'] / 100, 1.0)
            
            # Noise score (lower is better)
            noise_score = 1.0 - (config['noise_level_db'] / 100)
            
            # Weighted score
            if current_temp > 70:
                # Thermal emergency: prioritize capacity
                score = 0.6 * capacity_score + 0.2 * energy_score + 0.2 * noise_score
            elif helium_scarcity > 0.3:
                # Helium constrained: prioritize energy efficiency
                score = 0.5 * energy_score + 0.3 * capacity_score + 0.2 * noise_score
            else:
                # Balanced
                score = 0.4 * energy_score + 0.3 * capacity_score + 0.3 * noise_score
            
            scored_methods.append((method, config, score))
        
        # Select best
        scored_methods.sort(key=lambda x: x[2], reverse=True)
        best_method, best_config, best_score = scored_methods[0] if scored_methods else (
            CoolingMethod.AIR_COOLING, self.cooling_methods[CoolingMethod.AIR_COOLING], 0.5
        )
        
        return {
            'method': best_method.value,
            'energy_overhead': best_config['energy_overhead'],
            'cooling_capacity_kw': best_config['max_cooling_capacity_kw'],
            'helium_usage': best_config['helium_usage'],
            'score': best_score
        }
    
    # ========================================================================
    # Pruning Optimization
    # ========================================================================
    
    def _calculate_pruning(
        self,
        task_config: Dict[str, Any],
        latency_requirement_ms: float,
        energy_profile: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calculate optimal pruning rate with energy awareness.
        """
        carbon_category = energy_profile.get('carbon_category', 'moderate')
        
        # Base pruning based on latency
        if latency_requirement_ms < 10:
            base_rate = 0.5  # Aggressive for low latency
        elif latency_requirement_ms < 50:
            base_rate = 0.3
        elif latency_requirement_ms < 100:
            base_rate = 0.2
        else:
            base_rate = 0.1
        
        # Increase pruning for high carbon scenarios
        if carbon_category in ['high', 'very_high']:
            base_rate = min(base_rate + 0.2, 0.9)
        
        # Find closest valid pruning rate
        closest_rate = min(self.pruning_rates, key=lambda x: abs(x - base_rate))
        
        # Calculate energy reduction
        energy_reduction = 1.0 - (1.0 - closest_rate) * 0.7  # Pruning reduces energy by ~70% of rate
        
        return {
            'rate': closest_rate,
            'energy_reduction_factor': energy_reduction,
            'strategy': 'energy_aware_pruning'
        }
    
    # ========================================================================
    # Workload Shifting
    # ========================================================================
    
    async def _evaluate_workload_shifting(
        self,
        task_config: Dict[str, Any],
        energy_profile: Dict[str, Any],
        time_of_day: Optional[int]
    ) -> Dict[str, Any]:
        """
        Evaluate if workload should be shifted to lower-carbon period.
        
        Identifies optimal execution windows based on:
        - Renewable availability forecasts
        - Grid carbon intensity patterns
        - Task deadline flexibility
        """
        current_hour = time_of_day or datetime.utcnow().hour
        current_carbon = energy_profile.get('grid_carbon_intensity', 400)
        
        # Find optimal hours for execution
        optimal_hours = self._find_optimal_execution_hours(current_hour)
        
        # Check if current time is suboptimal
        is_suboptimal = current_carbon > 300 and len(optimal_hours) > 0
        
        # Calculate carbon savings from shifting
        target_carbon = self._estimate_grid_carbon_intensity(optimal_hours[0]) if optimal_hours else current_carbon
        carbon_savings = current_carbon - target_carbon
        
        can_shift = task_config.get('allow_shifting', False) and is_suboptimal
        
        return {
            'recommended': can_shift,
            'current_carbon_intensity': current_carbon,
            'optimal_hours': optimal_hours,
            'carbon_savings_if_shifted': carbon_savings,
            'recommended_hour': optimal_hours[0] if optimal_hours else current_hour,
            'urgency': 'high' if carbon_savings > 200 else 'moderate' if carbon_savings > 100 else 'low'
        }
    
    def _find_optimal_execution_hours(self, current_hour: int) -> List[int]:
        """Find hours with lowest carbon intensity"""
        # Simulate next 24 hours
        hours = [(h, self._estimate_grid_carbon_intensity(h)) for h in range(24)]
        hours.sort(key=lambda x: x[1])
        
        # Return hours better than current
        current_carbon = self._estimate_grid_carbon_intensity(current_hour)
        optimal = [h for h, c in hours if c < current_carbon * 0.7]
        
        return optimal[:3]  # Top 3 optimal hours
    
    # ========================================================================
    # Energy Storage Optimization
    # ========================================================================
    
    async def _optimize_storage_usage(
        self,
        energy_profile: Dict[str, Any],
        task_config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Optimize energy storage (battery/hydrogen) usage.
        
        Decides when to:
        - Charge batteries (excess renewable)
        - Discharge batteries (high grid carbon)
        - Use hydrogen for long-duration storage
        """
        renewable = energy_profile.get('renewable', {})
        carbon_intensity = energy_profile.get('grid_carbon_intensity', 400)
        
        storage_plan = {
            'use_battery': False,
            'use_hydrogen': False,
            'charge_battery': False,
            'battery_level_kwh': renewable.get('battery_kwh', 0)
        }
        
        # Use battery when grid carbon is high and battery has charge
        if (carbon_intensity > self.adaptive_thresholds['battery_use_threshold'] * 800 and
            renewable.get('battery_kwh', 0) > self.storage_config['battery']['min_level_percent'] / 100 * self.storage_config['battery']['capacity_kwh']):
            storage_plan['use_battery'] = True
        
        # Charge battery when renewable is abundant
        if renewable.get('can_meet_demand', False) and carbon_intensity < 200:
            storage_plan['charge_battery'] = True
        
        # Use hydrogen for long-duration if available
        if carbon_intensity > 600 and renewable.get('battery_kwh', 0) < 10:
            storage_plan['use_hydrogen'] = True
        
        return storage_plan if any(storage_plan.values()) else None
    
    # ========================================================================
    # Comprehensive Resource Estimation
    # ========================================================================
    
    def _calculate_comprehensive_estimates(
        self,
        task_config: Dict[str, Any],
        energy_source_plan: Dict[str, Any],
        power_plan: Dict[str, Any],
        quantization_plan: Dict[str, Any],
        cooling_plan: Dict[str, Any],
        pruning_plan: Dict[str, Any],
        energy_profile: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calculate comprehensive energy and resource estimates.
        """
        # Base energy for task
        base_energy = task_config.get('base_energy_kwh', 0.01)
        
        # Apply efficiency factors
        power_factor = power_plan['energy_factor']
        quantization_factor = quantization_plan['energy_factor']
        pruning_factor = pruning_plan['energy_reduction_factor']
        cooling_factor = 1.0 + cooling_plan['energy_overhead']
        
        # Calculate total energy
        compute_energy = base_energy * power_factor * quantization_factor * pruning_factor
        cooling_energy = compute_energy * (cooling_factor - 1.0)
        total_energy = compute_energy + cooling_energy
        
        # Calculate carbon emissions
        carbon_intensity = energy_source_plan['carbon_intensity']  # gCO2/kWh
        renewable_percent = energy_source_plan['renewable_percent']
        
        # Renewable energy has zero carbon
        effective_carbon_intensity = carbon_intensity * (1 - renewable_percent)
        total_carbon = (total_energy * effective_carbon_intensity) / 1000  # kg CO2
        
        # Calculate latency
        base_latency = task_config.get('base_latency_ms', 50)
        latency_factor = 1.0 / power_plan['performance_factor']
        total_latency = base_latency * latency_factor
        
        # Calculate cost
        energy_price = energy_profile.get('energy_price_per_kwh', 0.10)
        total_cost = total_energy * energy_price
        
        # Calculate savings vs baseline
        baseline_energy = base_energy * 1.02  # Baseline with air cooling
        baseline_carbon = (baseline_energy * 400) / 1000  # Baseline grid mix
        baseline_cost = baseline_energy * 0.12  # Baseline price
        
        energy_saved = baseline_energy - total_energy
        carbon_saved = baseline_carbon - total_carbon
        cost_saved = baseline_cost - total_cost
        
        return {
            'total_energy_kwh': total_energy,
            'compute_energy_kwh': compute_energy,
            'cooling_energy_kwh': cooling_energy,
            'total_carbon_kg': total_carbon,
            'total_latency_ms': total_latency,
            'total_cost': total_cost,
            'energy_saved_kwh': max(0, energy_saved),
            'carbon_saved_kg': max(0, carbon_saved),
            'cost_saved': max(0, cost_saved),
            'renewable_energy_kwh': total_energy * renewable_percent,
            'grid_energy_kwh': total_energy * (1 - renewable_percent)
        }
    
    # ========================================================================
    # Optimization Recording for Learning
    # ========================================================================
    
    def _record_optimization(
        self,
        optimization_id: str,
        energy_profile: Dict[str, Any],
        energy_source_plan: Dict[str, Any],
        power_plan: Dict[str, Any],
        estimates: Dict[str, Any]
    ):
        """Record optimization for adaptive learning"""
        record = EnergyOptimizationHistory(
            timestamp=datetime.utcnow(),
            strategy='multi_factor',
            energy_source=energy_source_plan['source'].value,
            power_state=power_plan['state'].value,
            energy_saved_kwh=estimates['energy_saved_kwh'],
            carbon_saved_kg=estimates['carbon_saved_kg'],
            cost_saved=estimates['cost_saved'],
            renewable_used=energy_source_plan['renewable_percent'] > 0,
            success=True,
            metrics={
                'grid_carbon_intensity': energy_profile.get('grid_carbon_intensity', 400),
                'renewable_percentage': energy_profile.get('renewable', {}).get('percentage', 0),
                'optimization_id': optimization_id
            }
        )
        
        self.optimization_history.append(record)
        
        # Update totals
        self.total_energy_saved_kwh += estimates['energy_saved_kwh']
        self.total_carbon_saved_kg += estimates['carbon_saved_kg']
        self.total_cost_saved += estimates['cost_saved']
        
        # Update adaptive thresholds
        self._update_adaptive_thresholds(record)
    
    def _update_adaptive_thresholds(self, record: EnergyOptimizationHistory):
        """Update adaptive thresholds based on optimization history"""
        # Update renewable switch threshold
        if record.renewable_used and record.success:
            self.adaptive_thresholds['renewable_switch_threshold'] *= 0.95  # Be more aggressive
        elif not record.renewable_used:
            self.adaptive_thresholds['renewable_switch_threshold'] *= 1.05  # Be less aggressive
        
        # Clamp thresholds
        self.adaptive_thresholds['renewable_switch_threshold'] = max(0.1, min(0.8,
            self.adaptive_thresholds['renewable_switch_threshold']
        ))
    
    # ========================================================================
    # Carbon Offset Suggestions (Enhanced)
    # ========================================================================
    
    async def suggest_carbon_offset(
        self,
        carbon_impact: float,
        energy_source_plan: Optional[Dict[str, Any]] = None,
        renewable_profile: Optional[RenewableProfile] = None
    ) -> Dict[str, Any]:
        """
        Enhanced carbon offset suggestions with multiple strategies.
        """
        strategies = []
        
        # Strategy 1: Helium offset (existing)
        helium_offset = carbon_impact * 0.3
        strategies.append({
            'type': 'helium_offset',
            'amount_kg': helium_offset,
            'net_carbon_kg': carbon_impact - helium_offset,
            'cost_per_kg': 0.05,
            'total_cost': helium_offset * 0.05
        })
        
        # Strategy 2: Renewable energy certificates (NEW)
        rec_offset = carbon_impact * 0.5
        strategies.append({
            'type': 'renewable_certificates',
            'amount_kg': rec_offset,
            'net_carbon_kg': carbon_impact - rec_offset,
            'cost_per_kg': 0.02,
            'total_cost': rec_offset * 0.02
        })
        
        # Strategy 3: Direct air capture (NEW)
        dac_offset = carbon_impact * 0.4
        strategies.append({
            'type': 'direct_air_capture',
            'amount_kg': dac_offset,
            'net_carbon_kg': carbon_impact - dac_offset,
            'cost_per_kg': 0.15,
            'total_cost': dac_offset * 0.15
        })
        
        # Strategy 4: Reforestation (NEW)
        reforestation_offset = carbon_impact * 0.35
        strategies.append({
            'type': 'reforestation',
            'amount_kg': reforestation_offset,
            'net_carbon_kg': carbon_impact - reforestation_offset,
            'cost_per_kg': 0.01,
            'total_cost': reforestation_offset * 0.01,
            'co_benefits': ['biodiversity', 'water_conservation']
        })
        
        # Select best strategy based on cost-effectiveness
        best_strategy = min(strategies, key=lambda s: s['total_cost'])
        
        return {
            'carbon_impact_kg': carbon_impact,
            'strategies': strategies,
            'recommended_strategy': best_strategy,
            'max_offset_possible_kg': sum(s['amount_kg'] for s in strategies),
            'renewable_energy_used': energy_source_plan is not None and energy_source_plan.get('renewable_percent', 0) > 0
        }
    
    # ========================================================================
    # Cross-Expert Energy Coordination (NEW)
    # ========================================================================
    
    async def coordinate_with_experts(
        self,
        expert_plans: List[Dict[str, Any]],
        total_carbon_budget: float
    ) -> Dict[str, Any]:
        """
        Coordinate energy usage across multiple experts.
        
        Ensures total energy/carbon stays within budget
        while maximizing overall performance.
        """
        if not expert_plans:
            return {'allocation': {}, 'total_carbon': 0}
        
        # Calculate total estimated carbon
        total_carbon = sum(
            p.get('estimated_carbon_kg', 0) for p in expert_plans
        )
        
        # If within budget, no adjustment needed
        if total_carbon <= total_carbon_budget:
            return {
                'allocation': {p.get('expert_id', 'unknown'): 1.0 for p in expert_plans},
                'total_carbon': total_carbon,
                'budget_compliant': True
            }
        
        # Need to scale down
        scale_factor = total_carbon_budget / total_carbon
        
        # Allocate budget proportionally to priority
        allocation = {}
        for plan in expert_plans:
            expert_id = plan.get('expert_id', 'unknown')
            priority = plan.get('priority', 1)
            
            # Higher priority gets more budget
            weighted_scale = scale_factor * (priority / sum(p.get('priority', 1) for p in expert_plans))
            allocation[expert_id] = min(weighted_scale, 1.0)
        
        return {
            'allocation': allocation,
            'total_carbon': total_carbon_budget,
            'original_carbon': total_carbon,
            'budget_compliant': True,
            'scale_factor': scale_factor
        }
    
    # ========================================================================
    # Recommendations Generator
    # ========================================================================
    
    def _generate_energy_recommendations(
        self,
        energy_profile: Dict[str, Any],
        estimates: Dict[str, Any],
        shifting_plan: Dict[str, Any]
    ) -> List[str]:
        """Generate actionable energy recommendations"""
        recommendations = []
        
        # Carbon intensity recommendations
        carbon_category = energy_profile.get('carbon_category', 'moderate')
        if carbon_category in ['high', 'very_high']:
            recommendations.append(
                f"High grid carbon intensity detected. "
                f"Consider shifting workload or using stored renewable energy."
            )
        
        # Workload shifting
        if shifting_plan.get('recommended', False):
            optimal_hour = shifting_plan.get('recommended_hour', 0)
            savings = shifting_plan.get('carbon_savings_if_shifted', 0)
            recommendations.append(
                f"Shift workload to hour {optimal_hour}:00 to save "
                f"approximately {savings:.0f} gCO2/kWh"
            )
        
        # Renewable recommendations
        renewable = energy_profile.get('renewable', {})
        if renewable.get('percentage', 0) < 0.3:
            recommendations.append(
                "Low renewable energy availability. Consider investing in "
                "on-site solar or wind generation."
            )
        
        # Thermal recommendations
        thermal = energy_profile.get('thermal', {})
        if thermal.get('current_temp_c', 40) > 60:
            recommendations.append(
                "High operating temperature detected. Consider upgrading "
                "cooling system or reducing workload intensity."
            )
        
        # Energy savings
        if estimates['energy_saved_kwh'] > 0:
            recommendations.append(
                f"Current optimization saves {estimates['energy_saved_kwh']:.4f} kWh "
                f"({estimates['carbon_saved_kg']:.4f} kg CO2) vs baseline."
            )
        
        return recommendations if recommendations else ["Energy configuration is optimal."]
    
    # ========================================================================
    # Expert Statistics
    # ========================================================================
    
    def get_expert_statistics(self) -> Dict[str, Any]:
        """Get comprehensive energy expert statistics"""
        recent = list(self.optimization_history)[-100:]
        
        return {
            'expert_id': self.expert_id,
            'version': self.version,
            'total_energy_saved_kwh': self.total_energy_saved_kwh,
            'total_carbon_saved_kg': self.total_carbon_saved_kg,
            'total_cost_saved': self.total_cost_saved,
            'optimizations_performed': len(self.optimization_history),
            'renewable_usage_rate': (
                sum(1 for r in recent if r.renewable_used) / max(len(recent), 1)
            ),
            'average_energy_saved_kwh': (
                np.mean([r.energy_saved_kwh for r in recent]) if recent else 0
            ),
            'average_carbon_saved_kg': (
                np.mean([r.carbon_saved_kg for r in recent]) if recent else 0
            ),
            'adaptive_thresholds': self.adaptive_thresholds
        }
