# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/energy_expert.py
# Enhanced with complete bio-inspired integration - Metabolic Energy Producer v4.0.0

"""
Enhanced Energy Expert v4.0.0 - Metabolic Energy Producer (Primary Producer/Autotroph)

Complete bio-inspired integration with:
- Gradient-based energy source selection (carbon gradient drives source priority)
- ATP-driven DVFS power states (energy availability controls frequency)
- Token-cost quantization selection (Eco-ATP efficient precision)
- Compartment thermal state for cooling (health-based temperature)
- Harvester-aligned workload shifting (photosynthetic opportunity timing)
- Biomass-backed energy storage (storage tiers as energy reserves)
- Harvester-based renewable forecasting (excitation as prediction)
- Token-generating carbon offsets (Eco-ATP from carbon savings)
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import deque
import math
import hashlib
import json

logger = logging.getLogger(__name__)

# ============================================================================
# Try importing bio-inspired modules
# ============================================================================

try:
    from enhancements.bio_inspired.eco_atp_currency import (
        EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer,
        TokenState, EcoATPToken, EcoATPAccount
    )
    from enhancements.bio_inspired.proton_gradient_fields import (
        GradientFieldManager, GradientField
    )
    from enhancements.bio_inspired.atp_synthase_scheduler import (
        ATPSynthaseScheduler, SynthaseConfig
    )
    from enhancements.bio_inspired.chromatophore_compartments import (
        CompartmentManager, ChromatophoreCompartment, CompartmentState,
        MembranePermeability
    )
    from enhancements.bio_inspired.biomass_storage import (
        BiomassStorage, StorageTier, GuaranteeLevel
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (
        PhotosyntheticHarvester
    )
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired modules loaded for Energy Expert")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired modules not available: {str(e)} - using standard energy optimization")

# Try importing from expert registry
try:
    from ..expert_registry import ExpertProfile, ExpertDomain, HardwareProfile
except ImportError:
    class ExpertDomain(Enum):
        ENERGY = "energy_optimization"
    class HardwareProfile(Enum):
        CPU_EFFICIENT = "cpu_low_power"

# ============================================================================
# Enums and Data Classes
# ============================================================================

class EnergySource(Enum):
    """Types of energy sources with carbon intensity"""
    SOLAR = "solar"
    WIND = "wind"
    HYDRO = "hydro"
    GEOTHERMAL = "geothermal"
    NUCLEAR = "nuclear"
    NATURAL_GAS = "natural_gas"
    COAL = "coal"
    GRID_MIX = "grid_mix"
    BATTERY = "battery"
    HYDROGEN = "hydrogen"
    GRADIENT_DRIVEN = "gradient_driven"  # BIO-INSPIRED
    
    @property
    def carbon_intensity_g_per_kwh(self) -> float:
        intensities = {
            EnergySource.SOLAR: 0, EnergySource.WIND: 0, EnergySource.HYDRO: 0,
            EnergySource.GEOTHERMAL: 0, EnergySource.NUCLEAR: 12,
            EnergySource.NATURAL_GAS: 490, EnergySource.COAL: 820,
            EnergySource.GRID_MIX: 400, EnergySource.BATTERY: 0,
            EnergySource.HYDROGEN: 0, EnergySource.GRADIENT_DRIVEN: 200
        }
        return intensities.get(self, 400)
    
    @property
    def is_renewable(self) -> bool:
        return self in [EnergySource.SOLAR, EnergySource.WIND, EnergySource.HYDRO,
                       EnergySource.GEOTHERMAL, EnergySource.BATTERY, EnergySource.HYDROGEN]

class PowerState(Enum):
    """CPU/GPU power states for DVFS"""
    PERFORMANCE = "performance"
    BALANCED = "balanced"
    POWER_SAVE = "power_save"
    ULTRA_LOW = "ultra_low"
    DYNAMIC = "dynamic"
    ATP_DRIVEN = "atp_driven"  # BIO-INSPIRED

class CoolingMethod(Enum):
    """Cooling methods with energy overhead"""
    AIR_COOLING = "air"
    LIQUID_COOLING = "liquid"
    IMMERSION_COOLING = "immersion"
    FREE_COOLING = "free"
    GEOTHERMAL_COOLING = "geothermal"
    HELIUM_COOLING = "helium"
    COMPARTMENT_AWARE = "compartment_aware"  # BIO-INSPIRED

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
    
    # BIO-INSPIRED
    harvester_contribution_kw: float = 0.0
    biomass_reserve_kwh: float = 0.0
    
    def can_use_renewable(self, required_kw: float) -> bool:
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
    
    # BIO-INSPIRED
    compartment_health: float = 0.7
    
    @property
    def thermal_headroom_c(self) -> float:
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
    
    # BIO-INSPIRED
    ecoatp_generated: float = 0.0
    gradient_level: float = 0.5

# ============================================================================
# Enhanced Energy Expert with Complete Bio-Inspired Integration
# ============================================================================

class EnergyExpert:
    """
    Enhanced Energy Expert v4.0.0 - Metabolic Energy Producer (Primary Producer/Autotroph)
    
    Complete bio-inspired integration:
    - Gradient-based energy source selection
    - ATP-driven DVFS power states
    - Token-cost quantization selection
    - Compartment thermal state for cooling
    - Harvester-aligned workload shifting
    - Biomass-backed energy storage
    - Harvester-based renewable forecasting
    - Token-generating carbon offsets
    """
    
    def __init__(
        self,
        expert_id: str = "energy_optimizer_v4",
        enable_renewable: bool = True,
        enable_storage: bool = True,
        enable_thermal: bool = True,
        enable_dvfs: bool = True,
        enable_forecasting: bool = True,
        enable_bio_integration: bool = True,
        grid_api_key: Optional[str] = None
    ):
        self.expert_id = expert_id
        self.version = "4.0.0"
        self.enable_renewable = enable_renewable
        self.enable_storage = enable_storage
        self.enable_thermal = enable_thermal
        self.enable_dvfs = enable_dvfs
        self.enable_forecasting = enable_forecasting
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        
        # BIO-INSPIRED: Module references (injected)
        self.token_manager: Optional[EcoATPTokenManager] = None
        self.gradient_manager: Optional[GradientFieldManager] = None
        self.scheduler: Optional[ATPSynthaseScheduler] = None
        self.compartment_manager: Optional[CompartmentManager] = None
        self.biomass_storage: Optional[BiomassStorage] = None
        self.harvester: Optional[PhotosyntheticHarvester] = None
        
        # Expert profile for registry
        self.profile = ExpertProfile(
            expert_id=expert_id,
            domain=ExpertDomain.ENERGY,
            hardware_profile=HardwareProfile.CPU_EFFICIENT,
            helium_per_inference=0.008,
            carbon_per_inference=0.00008,
            energy_per_inference=0.0008,
            avg_latency_ms=40.0,
            accuracy_score=0.94,
            reliability_score=0.97,
            efficiency_score=0.99,
            supported_task_types=[
                'inference', 'training', 'optimization',
                'energy_management', 'carbon_accounting',
                'renewable_integration', 'workload_scheduling'
            ]
        )
        
        # DVFS power states
        self.power_states = {
            PowerState.PERFORMANCE: {'frequency_percent': 100, 'energy_factor': 1.0, 'performance_factor': 1.0},
            PowerState.BALANCED: {'frequency_percent': 70, 'energy_factor': 0.7, 'performance_factor': 0.85},
            PowerState.POWER_SAVE: {'frequency_percent': 50, 'energy_factor': 0.5, 'performance_factor': 0.65},
            PowerState.ULTRA_LOW: {'frequency_percent': 25, 'energy_factor': 0.25, 'performance_factor': 0.35},
            PowerState.DYNAMIC: {'frequency_percent': 0, 'energy_factor': 0.0, 'performance_factor': 0.0},
            PowerState.ATP_DRIVEN: {'frequency_percent': 0, 'energy_factor': 0.0, 'performance_factor': 0.0}
        }
        
        # Quantization levels
        self.quantization_levels = {
            'fp32': {'energy_factor': 1.0, 'accuracy_impact': 0.0, 'memory_factor': 1.0, 'ecoatp_cost': 10.0},
            'fp16': {'energy_factor': 0.5, 'accuracy_impact': 0.01, 'memory_factor': 0.5, 'ecoatp_cost': 5.0},
            'bf16': {'energy_factor': 0.5, 'accuracy_impact': 0.005, 'memory_factor': 0.5, 'ecoatp_cost': 5.0},
            'int8': {'energy_factor': 0.25, 'accuracy_impact': 0.03, 'memory_factor': 0.25, 'ecoatp_cost': 2.0},
            'int4': {'energy_factor': 0.125, 'accuracy_impact': 0.05, 'memory_factor': 0.125, 'ecoatp_cost': 1.0}
        }
        
        # Cooling methods
        self.cooling_methods = {
            CoolingMethod.AIR_COOLING: {'energy_overhead': 0.02, 'max_cooling_capacity_kw': 50, 'helium_usage': 0.0},
            CoolingMethod.LIQUID_COOLING: {'energy_overhead': 0.05, 'max_cooling_capacity_kw': 200, 'helium_usage': 0.0},
            CoolingMethod.IMMERSION_COOLING: {'energy_overhead': 0.03, 'max_cooling_capacity_kw': 500, 'helium_usage': 0.0},
            CoolingMethod.FREE_COOLING: {'energy_overhead': 0.0, 'max_cooling_capacity_kw': 30, 'helium_usage': 0.0},
            CoolingMethod.GEOTHERMAL_COOLING: {'energy_overhead': 0.01, 'max_cooling_capacity_kw': 100, 'helium_usage': 0.0},
            CoolingMethod.HELIUM_COOLING: {'energy_overhead': 0.10, 'max_cooling_capacity_kw': 1000, 'helium_usage': 0.05},
            CoolingMethod.COMPARTMENT_AWARE: {'energy_overhead': 0.0, 'max_cooling_capacity_kw': 0, 'helium_usage': 0.0}
        }
        
        # Optimization history
        self.optimization_history: deque = deque(maxlen=10000)
        
        # Performance tracking
        self.total_energy_saved_kwh = 0.0
        self.total_carbon_saved_kg = 0.0
        self.total_cost_saved = 0.0
        self.total_ecoatp_generated = 0.0  # BIO-INSPIRED
        
        # Adaptive thresholds
        self.adaptive_thresholds = {
            'renewable_switch_threshold': 0.3,
            'battery_use_threshold': 0.5,
            'thermal_throttle_threshold': 75.0,
            'dvfs_aggressiveness': 0.5
        }
        
        logger.info(f"Enhanced Energy Expert v{self.version} initialized: bio_integration={self.enable_bio_integration}")
    
    # ========================================================================
    # Bio-Inspired Module Injection
    # ========================================================================
    
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        """
        Inject bio-inspired modules for energy optimization.
        
        Connects energy expert to real bio-inspired systems.
        """
        if bio_core:
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.scheduler = getattr(bio_core, 'scheduler', None)
            self.compartment_manager = getattr(bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(bio_core, 'biomass_storage', None)
            self.harvester = getattr(bio_core, 'harvester', None)
        else:
            self.token_manager = kwargs.get('token_manager')
            self.gradient_manager = kwargs.get('gradient_manager')
            self.scheduler = kwargs.get('scheduler')
            self.compartment_manager = kwargs.get('compartment_manager')
            self.biomass_storage = kwargs.get('biomass_storage')
            self.harvester = kwargs.get('harvester')
        
        injections = {
            'token_manager': self.token_manager is not None,
            'gradient_manager': self.gradient_manager is not None,
            'scheduler': self.scheduler is not None,
            'compartment_manager': self.compartment_manager is not None,
            'biomass_storage': self.biomass_storage is not None,
            'harvester': self.harvester is not None
        }
        logger.info(f"Bio-inspired injections into Energy Expert: {injections}")
        
        if any(injections.values()):
            self.enable_bio_integration = True
    
    # ========================================================================
    # Bio-Inspired Data Access Methods
    # ========================================================================
    
    def _get_gradient_energy_source(self) -> EnergySource:
        """
        Select energy source based on carbon gradient.
        
        High carbon gradient = use stored/battery energy.
        Low carbon gradient = grid is clean enough.
        """
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength > 0.7:
                return EnergySource.BATTERY  # Use stored energy in high carbon stress
            elif carbon and carbon.gradient_strength > 0.4:
                return EnergySource.GRADIENT_DRIVEN  # Hybrid approach
            elif carbon and carbon.gradient_strength < 0.3:
                return EnergySource.GRID_MIX  # Grid is clean enough
        
        return EnergySource.SOLAR  # Default renewable
    
    def _get_atp_driven_dvfs(self) -> PowerState:
        """
        Select DVFS power state based on ATP availability.
        
        More ATP = higher performance allowed.
        """
        if self.scheduler:
            driving_force = self.scheduler.calculate_gradient_driving_force()
            rotation_speed = self.scheduler.calculate_rotation_speed(driving_force)
            ecoatp_rate = self.scheduler.calculate_atp_production_rate(rotation_speed)
            
            if ecoatp_rate > 100:
                return PowerState.PERFORMANCE
            elif ecoatp_rate > 50:
                return PowerState.BALANCED
            elif ecoatp_rate > 20:
                return PowerState.POWER_SAVE
            else:
                return PowerState.ULTRA_LOW
        
        return PowerState.ATP_DRIVEN
    
    def _get_token_efficient_quantization(self, task_type: str = 'inference') -> str:
        """
        Select quantization based on token availability.
        
        Fewer tokens = more aggressive quantization.
        """
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            balance = summary.get('total_balance', 500)
            
            if balance < 100:
                return 'int4'  # Most efficient when tokens scarce
            elif balance < 300:
                return 'int8'  # Balanced
            else:
                return 'fp16' if task_type == 'training' else 'int8'
        
        return 'int8'
    
    def _get_compartment_thermal_state(self) -> ThermalProfile:
        """
        Get thermal state from compartment health.
        
        Healthier compartments run cooler.
        """
        if self.compartment_manager:
            compartment = self.compartment_manager.find_best_compartment('energy')
            if compartment:
                health = compartment.health_score
                temp = 35.0 + (1.0 - health) * 40.0  # 35°C to 75°C based on health
                return ThermalProfile(
                    current_temp_c=temp,
                    compartment_health=health,
                    requires_throttling=health < 0.3
                )
        
        return ThermalProfile(current_temp_c=40.0, compartment_health=0.7)
    
    def _get_harvester_shift_timing(self) -> Optional[float]:
        """
        Get optimal workload shift timing from photosynthetic harvester.
        
        Returns seconds to wait before optimal execution window.
        """
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            recent = stats.get('recent_conversions', [])
            if recent:
                avg_energy = np.mean([c.get('convertible_energy', 0.5) for c in recent[-10:]])
                if avg_energy > 0.6:
                    return 0.0  # Shift now - good harvesting conditions
                elif avg_energy > 0.4:
                    return 1800.0  # Wait 30 minutes
                else:
                    return 7200.0  # Wait 2 hours for better conditions
        return None
    
    def _get_biomass_energy_reserve(self) -> float:
        """
        Get energy reserve from biomass storage.
        
        Returns kWh equivalent stored in biomass.
        """
        if self.biomass_storage:
            stats = self.biomass_storage.get_storage_stats()
            total_stored = stats.get('total_stored', 0)
            # Convert stored tasks to energy equivalent
            return float(total_stored) * 0.01  # 100 tasks ≈ 1 kWh
        return 0.0
    
    def _get_harvester_renewable_forecast(self) -> Dict[str, float]:
        """
        Get renewable energy forecast from harvester excitation levels.
        
        Maps photosynthetic activity to renewable predictions.
        """
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            total = stats.get('total_harvested', 0)
            recent = stats.get('recent_conversions', [])
            avg_energy = np.mean([c.get('convertible_energy', 0.5) for c in recent[-10:]]) if recent else 0.5
            
            return {
                'solar_kw': total * 0.6 * avg_energy,
                'wind_kw': total * 0.4 * (1.0 - avg_energy),
                'total_renewable_kw': total * avg_energy,
                'confidence': avg_energy
            }
        
        return {'solar_kw': 5.0, 'wind_kw': 3.0, 'total_renewable_kw': 8.0, 'confidence': 0.5}
    
    def _generate_offset_tokens(self, carbon_kg: float) -> float:
        """
        Generate Eco-ATP tokens from carbon offsets.
        
        Carbon savings are converted to Eco-ATP currency.
        """
        if self.token_manager:
            tokens = self.token_manager.generate_tokens(
                account_id='energy_expert_offsets',
                source=EcoATPSource.CARBON_OFFSET,
                carbon_saved_kg=carbon_kg,
                num_tokens=int(carbon_kg * 100)
            )
            if tokens:
                total = sum(t.value for t in tokens)
                self.total_ecoatp_generated += total
                return total
        return 0.0
    
    def _get_gradient_levels(self) -> Dict[str, float]:
        """Get all gradient levels for optimization"""
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}
    
    # ========================================================================
    # Primary Energy Optimization Method (Enhanced with Bio-Inspired)
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
        cross_expert_hints: Optional[Dict[str, Any]] = None,
        ecoatp_budget: Optional[float] = None  # BIO-INSPIRED
    ) -> Dict[str, Any]:
        """
        Comprehensive energy optimization with bio-inspired strategies.
        """
        start_time = datetime.utcnow()
        optimization_id = hashlib.md5(
            f"{task_config}{carbon_budget}{latency_requirement_ms}{start_time}".encode()
        ).hexdigest()[:12]
        
        # BIO-INSPIRED: Get real gradient levels
        gradient_levels = self._get_gradient_levels() if self.enable_bio_integration else {}
        
        # Step 1: BIO-INSPIRED - Select energy source based on gradient
        if self.enable_bio_integration:
            energy_source = self._get_gradient_energy_source()
        else:
            energy_source = EnergySource.SOLAR if renewable_profile and renewable_profile.can_use_renewable(10) else EnergySource.GRID_MIX
        
        # Step 2: BIO-INSPIRED - Select power state based on ATP
        if self.enable_bio_integration:
            power_state = self._get_atp_driven_dvfs()
        else:
            if latency_requirement_ms < 10:
                power_state = PowerState.PERFORMANCE
            elif latency_requirement_ms < 100:
                power_state = PowerState.BALANCED
            else:
                power_state = PowerState.POWER_SAVE
        
        power_config = self.power_states[power_state]
        
        # Step 3: BIO-INSPIRED - Select quantization based on tokens
        if self.enable_bio_integration:
            quant_level = self._get_token_efficient_quantization(task_config.get('task_type', 'inference'))
        else:
            quant_level = 'int8' if carbon_budget < 0.01 else 'fp16'
        
        quant_config = self.quantization_levels[quant_level]
        
        # Step 4: BIO-INSPIRED - Get thermal state from compartment
        if self.enable_bio_integration:
            thermal = self._get_compartment_thermal_state()
        else:
            thermal = thermal_profile or ThermalProfile()
        
        # Step 5: BIO-INSPIRED - Get workload shift timing from harvester
        shift_timing = None
        if self.enable_bio_integration:
            shift_timing = self._get_harvester_shift_timing()
        
        # Step 6: BIO-INSPIRED - Get renewable forecast from harvester
        if self.enable_bio_integration:
            harvester_forecast = self._get_harvester_renewable_forecast()
            if renewable_profile is None:
                renewable_profile = RenewableProfile(
                    solar_available_kw=harvester_forecast.get('solar_kw', 5.0),
                    wind_available_kw=harvester_forecast.get('wind_kw', 3.0),
                    harvester_contribution_kw=harvester_forecast.get('total_renewable_kw', 8.0)
                )
        
        # Step 7: Calculate resource estimates
        energy_factor = power_config['energy_factor'] * quant_config['energy_factor']
        base_energy = task_config.get('base_energy_kwh', 0.01)
        estimated_energy = base_energy * energy_factor
        
        carbon_intensity = energy_source.carbon_intensity_g_per_kwh
        estimated_carbon = estimated_energy * carbon_intensity / 1000
        
        # BIO-INSPIRED: Calculate Eco-ATP cost
        ecoatp_cost = estimated_energy * 1000  # 1 kWh = 1000 Eco-ATP
        
        # BIO-INSPIRED: Check Eco-ATP budget
        if ecoatp_budget and ecoatp_cost > ecoatp_budget and self.enable_bio_integration:
            # Downgrade to more efficient settings
            power_state = PowerState.POWER_SAVE
            quant_level = 'int8'
            energy_factor = self.power_states[power_state]['energy_factor'] * self.quantization_levels[quant_level]['energy_factor']
            estimated_energy = base_energy * energy_factor
            ecoatp_cost = estimated_energy * 1000
        
        # BIO-INSPIRED: Generate offset tokens
        ecoatp_generated = 0.0
        if self.enable_bio_integration and estimated_carbon > 0:
            ecoatp_generated = self._generate_offset_tokens(estimated_carbon)
        
        # Build comprehensive plan
        plan = {
            'expert_id': self.expert_id,
            'optimization_id': optimization_id,
            'version': self.version,
            
            # Energy source
            'energy_source': energy_source.value,
            'renewable_percentage': 100 if energy_source.is_renewable else 25,
            
            # Power management
            'power_state': power_state.value,
            'frequency_percent': power_config['frequency_percent'],
            
            # Model optimization
            'quantization': quant_level,
            'accuracy_impact': quant_config['accuracy_impact'],
            
            # Resource estimates
            'estimated_energy_kwh': estimated_energy,
            'estimated_carbon_kg': estimated_carbon,
            'estimated_ecoatp_cost': ecoatp_cost,  # BIO-INSPIRED
            'estimated_latency_ms': 40.0,
            
            # Compliance
            'carbon_budget_compliant': estimated_carbon <= carbon_budget,
            
            # BIO-INSPIRED features
            'bio_integration_active': self.enable_bio_integration,
            'gradient_levels': gradient_levels,
            'ecoatp_generated': ecoatp_generated,
            'harvester_forecast': self._get_harvester_renewable_forecast() if self.enable_bio_integration else {},
            'biomass_reserve_kwh': self._get_biomass_energy_reserve() if self.enable_bio_integration else 0.0,
            'shift_timing_seconds': shift_timing,
            
            # Recommendations
            'recommendations': self._generate_bio_recommendations(
                gradient_levels, ecoatp_generated, shift_timing, self.enable_bio_integration
            ),
            
            # Strategy
            'strategy': 'bio_optimized' if self.enable_bio_integration else 'standard',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Record optimization
        history_entry = EnergyOptimizationHistory(
            timestamp=start_time,
            strategy=plan['strategy'],
            energy_source=energy_source.value,
            power_state=power_state.value,
            energy_saved_kwh=max(0, base_energy - estimated_energy),
            carbon_saved_kg=max(0, base_energy * 400 / 1000 - estimated_carbon),
            cost_saved=0.0,
            renewable_used=energy_source.is_renewable,
            success=True,
            ecoatp_generated=ecoatp_generated,
            gradient_level=gradient_levels.get('carbon', 0.5)
        )
        
        self.optimization_history.append(history_entry)
        
        # Update totals
        self.total_energy_saved_kwh += history_entry.energy_saved_kwh
        self.total_carbon_saved_kg += history_entry.carbon_saved_kg
        self.total_ecoatp_generated += ecoatp_generated
        
        logger.info(
            f"Energy Plan [{optimization_id}]: source={energy_source.value}, "
            f"power={power_state.value}, quant={quant_level}, "
            f"carbon={estimated_carbon:.6f}kg, ecoatp={ecoatp_generated:.1f}, "
            f"bio={self.enable_bio_integration}"
        )
        
        return plan
    
    def _generate_bio_recommendations(
        self, gradient_levels: Dict[str, float], ecoatp_generated: float,
        shift_timing: Optional[float], bio_active: bool
    ) -> List[str]:
        """Generate bio-inspired recommendations"""
        recommendations = []
        
        if bio_active:
            carbon = gradient_levels.get('carbon', 0.5)
            if carbon > 0.7:
                recommendations.append(f"High carbon gradient ({carbon:.2f}) - switched to battery/stored energy.")
            elif carbon < 0.3:
                recommendations.append(f"Low carbon gradient ({carbon:.2f}) - grid energy is clean.")
            
            if ecoatp_generated > 0:
                recommendations.append(f"Generated {ecoatp_generated:.1f} Eco-ATP from carbon offsets.")
            
            if shift_timing and shift_timing > 0:
                recommendations.append(f"Optimal workload shift in {shift_timing:.0f}s for better harvesting.")
        
        if not recommendations:
            recommendations.append("Energy configuration is optimal for current conditions.")
        
        return recommendations
    
    # ========================================================================
    # Enhanced Carbon Offset with Token Generation
    # ========================================================================
    
    async def suggest_carbon_offset(
        self,
        carbon_impact: float,
        energy_source_plan: Optional[Dict[str, Any]] = None,
        renewable_profile: Optional[RenewableProfile] = None
    ) -> Dict[str, Any]:
        """Enhanced carbon offset suggestions with token generation"""
        strategies = [
            {'type': 'helium_offset', 'amount_kg': carbon_impact * 0.3, 'net_carbon_kg': carbon_impact * 0.7,
             'cost_per_kg': 0.05, 'total_cost': carbon_impact * 0.3 * 0.05},
            {'type': 'renewable_certificates', 'amount_kg': carbon_impact * 0.5, 'net_carbon_kg': carbon_impact * 0.5,
             'cost_per_kg': 0.02, 'total_cost': carbon_impact * 0.5 * 0.02},
            {'type': 'direct_air_capture', 'amount_kg': carbon_impact * 0.4, 'net_carbon_kg': carbon_impact * 0.6,
             'cost_per_kg': 0.15, 'total_cost': carbon_impact * 0.4 * 0.15},
            {'type': 'reforestation', 'amount_kg': carbon_impact * 0.35, 'net_carbon_kg': carbon_impact * 0.65,
             'cost_per_kg': 0.01, 'total_cost': carbon_impact * 0.35 * 0.01,
             'co_benefits': ['biodiversity', 'water_conservation']}
        ]
        
        best_strategy = min(strategies, key=lambda s: s['total_cost'])
        
        # BIO-INSPIRED: Generate Eco-ATP tokens from the offset
        ecoatp_generated = 0.0
        if self.enable_bio_integration:
            ecoatp_generated = self._generate_offset_tokens(carbon_impact)
        
        return {
            'carbon_impact_kg': carbon_impact,
            'strategies': strategies,
            'recommended_strategy': best_strategy,
            'max_offset_possible_kg': sum(s['amount_kg'] for s in strategies),
            'ecoatp_generated': ecoatp_generated,  # BIO-INSPIRED
            'bio_integration_active': self.enable_bio_integration,
            'renewable_energy_used': energy_source_plan is not None and energy_source_plan.get('renewable_percentage', 0) > 0
        }
    
    # ========================================================================
    # Cross-Expert Energy Coordination with Bio-Inspired Awareness
    # ========================================================================
    
    async def coordinate_with_experts(
        self, expert_plans: List[Dict[str, Any]], total_carbon_budget: float
    ) -> Dict[str, Any]:
        """Coordinate energy usage across experts with bio-inspired awareness"""
        if not expert_plans:
            return {'allocation': {}, 'total_carbon': 0}
        
        total_carbon = sum(p.get('estimated_carbon_kg', 0) for p in expert_plans)
        
        if total_carbon <= total_carbon_budget:
            return {
                'allocation': {p.get('expert_id', 'unknown'): 1.0 for p in expert_plans},
                'total_carbon': total_carbon,
                'budget_compliant': True,
                'bio_active': self.enable_bio_integration,
                'gradient_levels': self._get_gradient_levels() if self.enable_bio_integration else {}
            }
        
        scale_factor = total_carbon_budget / total_carbon
        
        # BIO-INSPIRED: Prioritize experts with higher gradient alignment
        if self.enable_bio_integration and self.gradient_manager:
            trust = self.gradient_manager.fields.get('trust')
            if trust:
                allocation = {}
                for plan in expert_plans:
                    expert_id = plan.get('expert_id', 'unknown')
                    allocation[expert_id] = min(scale_factor * (0.5 + 0.5 * trust.gradient_strength), 1.0)
                return {
                    'allocation': allocation,
                    'total_carbon': total_carbon_budget,
                    'budget_compliant': True,
                    'bio_active': True,
                    'trust_based': True
                }
        
        allocation = {p.get('expert_id', 'unknown'): scale_factor for p in expert_plans}
        return {'allocation': allocation, 'total_carbon': total_carbon_budget, 'budget_compliant': True}
    
    # ========================================================================
    # Enhanced Statistics
    # ========================================================================
    
    def get_expert_statistics(self) -> Dict[str, Any]:
        """Get comprehensive energy expert statistics with bio-inspired metrics"""
        recent = list(self.optimization_history)[-100:]
        
        stats = {
            'expert_id': self.expert_id,
            'version': self.version,
            'total_energy_saved_kwh': self.total_energy_saved_kwh,
            'total_carbon_saved_kg': self.total_carbon_saved_kg,
            'total_cost_saved': self.total_cost_saved,
            'total_ecoatp_generated': self.total_ecoatp_generated,
            'bio_integration_active': self.enable_bio_integration,
            'bio_modules_available': BIO_INSPIRED_AVAILABLE,
            'optimizations_performed': len(self.optimization_history),
            'renewable_usage_rate': sum(1 for r in recent if r.renewable_used) / max(len(recent), 1) if recent else 0,
            'average_energy_saved_kwh': np.mean([r.energy_saved_kwh for r in recent]) if recent else 0,
            'average_carbon_saved_kg': np.mean([r.carbon_saved_kg for r in recent]) if recent else 0,
            'average_ecoatp_generated': np.mean([r.ecoatp_generated for r in recent]) if recent else 0,
            'adaptive_thresholds': self.adaptive_thresholds
        }
        
        # BIO-INSPIRED: Add gradient and harvester data
        if self.enable_bio_integration:
            stats['bio_metrics'] = {
                'gradient_levels': self._get_gradient_levels(),
                'harvester_forecast': self._get_harvester_renewable_forecast(),
                'biomass_reserve_kwh': self._get_biomass_energy_reserve(),
                'compartment_health': self._get_compartment_thermal_state().compartment_health,
                'atp_power_state': self._get_atp_driven_dvfs().value,
                'token_efficient_quantization': self._get_token_efficient_quantization()
            }
        
        return stats
