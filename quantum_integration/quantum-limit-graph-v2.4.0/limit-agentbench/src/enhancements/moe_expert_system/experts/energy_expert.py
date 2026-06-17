# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/energy_expert.py
# Complete enhanced file with natural language explanations and decision analysis

"""
Enhanced Energy Expert v5.0.0 - Complete Metabolic Energy Producer
With Natural Language Explanations, Renewable Forecast Analysis, and Decision Explanations
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
# Bio-Inspired Import Check
# ============================================================================
try:
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager, EcoATPSource, EcoATPConsumer
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    from enhancements.bio_inspired.atp_synthase_scheduler import ATPSynthaseScheduler
    from enhancements.bio_inspired.chromatophore_compartments import CompartmentManager
    from enhancements.bio_inspired.biomass_storage import BiomassStorage
    from enhancements.bio_inspired.photosynthetic_harvester import PhotosyntheticHarvester
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False

try:
    from ..expert_registry import ExpertProfile, ExpertDomain, HardwareProfile
except ImportError:
    class ExpertDomain(Enum): ENERGY = "energy_optimization"
    class HardwareProfile(Enum): CPU_EFFICIENT = "cpu_low_power"

# ============================================================================
# Enums and Data Classes
# ============================================================================
class EnergySource(Enum):
    SOLAR = "solar"; WIND = "wind"; HYDRO = "hydro"; GEOTHERMAL = "geothermal"
    NUCLEAR = "nuclear"; NATURAL_GAS = "natural_gas"; COAL = "coal"
    GRID_MIX = "grid_mix"; BATTERY = "battery"; HYDROGEN = "hydrogen"; GRADIENT_DRIVEN = "gradient_driven"
    
    @property
    def carbon_intensity_g_per_kwh(self) -> float:
        intensities = {EnergySource.SOLAR: 0, EnergySource.WIND: 0, EnergySource.HYDRO: 0,
                      EnergySource.GEOTHERMAL: 0, EnergySource.NUCLEAR: 12, EnergySource.NATURAL_GAS: 490,
                      EnergySource.COAL: 820, EnergySource.GRID_MIX: 400, EnergySource.BATTERY: 0,
                      EnergySource.HYDROGEN: 0, EnergySource.GRADIENT_DRIVEN: 200}
        return intensities.get(self, 400)
    
    @property
    def is_renewable(self) -> bool:
        return self in [EnergySource.SOLAR, EnergySource.WIND, EnergySource.HYDRO,
                       EnergySource.GEOTHERMAL, EnergySource.BATTERY, EnergySource.HYDROGEN]

class PowerState(Enum):
    PERFORMANCE = "performance"; BALANCED = "balanced"; POWER_SAVE = "power_save"
    ULTRA_LOW = "ultra_low"; DYNAMIC = "dynamic"; ATP_DRIVEN = "atp_driven"

class CoolingMethod(Enum):
    AIR_COOLING = "air"; LIQUID_COOLING = "liquid"; IMMERSION_COOLING = "immersion"
    FREE_COOLING = "free"; GEOTHERMAL_COOLING = "geothermal"; HELIUM_COOLING = "helium"
    COMPARTMENT_AWARE = "compartment_aware"

@dataclass
class RenewableProfile:
    solar_available_kw: float = 0.0; wind_available_kw: float = 0.0
    battery_level_kwh: float = 0.0; battery_capacity_kwh: float = 100.0
    hydrogen_level_kg: float = 0.0; renewable_percentage: float = 0.0
    forecast_next_hour: float = 0.0; peak_solar_time: bool = False
    harvester_contribution_kw: float = 0.0; biomass_reserve_kwh: float = 0.0
    
    def can_use_renewable(self, required_kw: float) -> bool:
        return (self.solar_available_kw + self.wind_available_kw + self.battery_level_kwh) >= required_kw

@dataclass
class ThermalProfile:
    current_temp_c: float = 35.0; max_temp_c: float = 85.0; throttle_temp_c: float = 75.0
    ambient_temp_c: float = 25.0; cooling_efficiency: float = 0.9
    requires_throttling: bool = False; compartment_health: float = 0.7
    
    @property
    def thermal_headroom_c(self) -> float: return self.throttle_temp_c - self.current_temp_c

@dataclass
class EnergyOptimizationHistory:
    timestamp: datetime; strategy: str; energy_source: str; power_state: str
    energy_saved_kwh: float; carbon_saved_kg: float; cost_saved: float
    renewable_used: bool; success: bool
    metrics: Dict[str, float] = field(default_factory=dict)
    ecoatp_generated: float = 0.0; gradient_level: float = 0.5

# ============================================================================
# Enhanced Energy Expert
# ============================================================================
class EnergyExpert:
    """Enhanced Energy Expert v5.0.0 with Natural Language Explanations"""
    
    def __init__(self, expert_id: str = "energy_optimizer_v5", enable_renewable: bool = True,
                 enable_storage: bool = True, enable_thermal: bool = True, enable_dvfs: bool = True,
                 enable_forecasting: bool = True, enable_bio_integration: bool = True):
        self.expert_id = expert_id; self.version = "5.0.0"
        self.enable_renewable = enable_renewable; self.enable_storage = enable_storage
        self.enable_thermal = enable_thermal; self.enable_dvfs = enable_dvfs
        self.enable_forecasting = enable_forecasting
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        
        self.token_manager = None; self.gradient_manager = None; self.scheduler = None
        self.compartment_manager = None; self.biomass_storage = None; self.harvester = None
        
        self.profile = ExpertProfile(expert_id=expert_id, domain=ExpertDomain.ENERGY, hardware_profile=HardwareProfile.CPU_EFFICIENT,
                                     helium_per_inference=0.008, carbon_per_inference=0.00008, energy_per_inference=0.0008,
                                     avg_latency_ms=40.0, accuracy_score=0.94, reliability_score=0.97, efficiency_score=0.99,
                                     supported_task_types=['inference', 'training', 'optimization', 'energy_management'])
        
        self.power_states = {
            PowerState.PERFORMANCE: {'frequency_percent': 100, 'energy_factor': 1.0, 'performance_factor': 1.0},
            PowerState.BALANCED: {'frequency_percent': 70, 'energy_factor': 0.7, 'performance_factor': 0.85},
            PowerState.POWER_SAVE: {'frequency_percent': 50, 'energy_factor': 0.5, 'performance_factor': 0.65},
            PowerState.ULTRA_LOW: {'frequency_percent': 25, 'energy_factor': 0.25, 'performance_factor': 0.35},
            PowerState.DYNAMIC: {'frequency_percent': 0, 'energy_factor': 0.0, 'performance_factor': 0.0},
            PowerState.ATP_DRIVEN: {'frequency_percent': 0, 'energy_factor': 0.0, 'performance_factor': 0.0}
        }
        
        self.quantization_levels = {
            'fp32': {'energy_factor': 1.0, 'accuracy_impact': 0.0, 'ecoatp_cost': 10},
            'fp16': {'energy_factor': 0.5, 'accuracy_impact': 0.01, 'ecoatp_cost': 5},
            'bf16': {'energy_factor': 0.5, 'accuracy_impact': 0.005, 'ecoatp_cost': 5},
            'int8': {'energy_factor': 0.25, 'accuracy_impact': 0.03, 'ecoatp_cost': 2},
            'int4': {'energy_factor': 0.125, 'accuracy_impact': 0.05, 'ecoatp_cost': 1}
        }
        
        self.cooling_methods = {
            CoolingMethod.AIR_COOLING: {'energy_overhead': 0.02, 'max_cooling_capacity_kw': 50, 'helium_usage': 0.0},
            CoolingMethod.LIQUID_COOLING: {'energy_overhead': 0.05, 'max_cooling_capacity_kw': 200, 'helium_usage': 0.0},
            CoolingMethod.IMMERSION_COOLING: {'energy_overhead': 0.03, 'max_cooling_capacity_kw': 500, 'helium_usage': 0.0},
            CoolingMethod.FREE_COOLING: {'energy_overhead': 0.0, 'max_cooling_capacity_kw': 30, 'helium_usage': 0.0},
            CoolingMethod.GEOTHERMAL_COOLING: {'energy_overhead': 0.01, 'max_cooling_capacity_kw': 100, 'helium_usage': 0.0},
            CoolingMethod.HELIUM_COOLING: {'energy_overhead': 0.10, 'max_cooling_capacity_kw': 1000, 'helium_usage': 0.05},
            CoolingMethod.COMPARTMENT_AWARE: {'energy_overhead': 0.0, 'max_cooling_capacity_kw': 0, 'helium_usage': 0.0}
        }
        
        self.optimization_history: deque = deque(maxlen=10000)
        self.total_energy_saved_kwh = 0.0; self.total_carbon_saved_kg = 0.0
        self.total_cost_saved = 0.0; self.total_ecoatp_generated = 0.0
        self.adaptive_thresholds = {'renewable_switch_threshold': 0.3, 'battery_use_threshold': 0.5,
                                    'thermal_throttle_threshold': 75.0, 'dvfs_aggressiveness': 0.5}
        
        logger.info(f"Energy Expert v{self.version} initialized")
    
    def inject_bio_core(self, bio_core=None, **kwargs):
        if bio_core:
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.scheduler = getattr(bio_core, 'scheduler', None)
            self.compartment_manager = getattr(bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(bio_core, 'biomass_storage', None)
            self.harvester = getattr(bio_core, 'harvester', None)
        else:
            self.token_manager = kwargs.get('token_manager'); self.gradient_manager = kwargs.get('gradient_manager')
            self.scheduler = kwargs.get('scheduler'); self.compartment_manager = kwargs.get('compartment_manager')
            self.biomass_storage = kwargs.get('biomass_storage'); self.harvester = kwargs.get('harvester')
        if any([self.token_manager, self.gradient_manager, self.compartment_manager]):
            self.enable_bio_integration = True
    
    # ========================================================================
    # Bio-Inspired Data Access
    # ========================================================================
    def _get_gradient_energy_source(self) -> EnergySource:
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength > 0.7: return EnergySource.BATTERY
            elif carbon and carbon.gradient_strength < 0.3: return EnergySource.GRID_MIX
        return EnergySource.SOLAR
    
    def _get_atp_driven_dvfs(self) -> PowerState:
        if self.scheduler:
            df = self.scheduler.calculate_gradient_driving_force()
            rs = self.scheduler.calculate_rotation_speed(df)
            rate = self.scheduler.calculate_atp_production_rate(rs)
            if rate > 100: return PowerState.PERFORMANCE
            elif rate > 50: return PowerState.BALANCED
            elif rate > 20: return PowerState.POWER_SAVE
            else: return PowerState.ULTRA_LOW
        return PowerState.ATP_DRIVEN
    
    def _get_token_efficient_quantization(self, task_type: str = 'inference') -> str:
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            balance = summary.get('total_balance', 500)
            if balance < 100: return 'int4'
            elif balance < 300: return 'int8'
            else: return 'fp16' if task_type == 'training' else 'int8'
        return 'int8'
    
    def _get_compartment_thermal_state(self) -> ThermalProfile:
        if self.compartment_manager:
            compartment = self.compartment_manager.find_best_compartment('energy')
            if compartment:
                health = compartment.health_score
                temp = 35.0 + (1.0 - health) * 40.0
                return ThermalProfile(current_temp_c=temp, compartment_health=health, requires_throttling=health < 0.3)
        return ThermalProfile(current_temp_c=40.0, compartment_health=0.7)
    
    def _get_harvester_renewable_forecast(self) -> Dict[str, float]:
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            total = stats.get('total_harvested', 0)
            recent = stats.get('recent_conversions', [])
            avg = np.mean([c.get('convertible_energy', 0.5) for c in recent[-10:]]) if recent else 0.5
            return {'solar_kw': total * 0.6 * avg, 'wind_kw': total * 0.4 * avg,
                    'total_renewable_kw': total * avg, 'confidence': avg}
        return {'solar_kw': 5.0, 'wind_kw': 3.0, 'total_renewable_kw': 8.0, 'confidence': 0.5}
    
    def _generate_offset_tokens(self, carbon_kg: float) -> float:
        if self.token_manager:
            tokens = self.token_manager.generate_tokens(account_id='energy_expert_offsets', source=EcoATPSource.CARBON_OFFSET,
                                                        carbon_saved_kg=carbon_kg, num_tokens=int(carbon_kg * 100))
            if tokens:
                total = sum(t.value for t in tokens)
                self.total_ecoatp_generated += total
                return total
        return 0.0
    
    def _get_gradient_levels(self) -> Dict[str, float]:
        if self.gradient_manager: return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}
    
    # ========================================================================
    # Primary Optimization
    # ========================================================================
    async def optimize_energy(self, task_config: Dict[str, Any], carbon_budget: float,
                             latency_requirement_ms: float, grid_carbon_intensity: Optional[float] = None,
                             renewable_profile: Optional[RenewableProfile] = None,
                             thermal_profile: Optional[ThermalProfile] = None,
                             time_of_day: Optional[int] = None, energy_price_per_kwh: Optional[float] = None,
                             helium_scarcity: float = 0.0, cross_expert_hints: Optional[Dict[str, Any]] = None,
                             ecoatp_budget: Optional[float] = None) -> Dict[str, Any]:
        start_time = datetime.utcnow()
        optimization_id = hashlib.md5(f"{task_config}{carbon_budget}{latency_requirement_ms}{start_time}".encode()).hexdigest()[:12]
        
        gradient_levels = self._get_gradient_levels() if self.enable_bio_integration else {}
        
        energy_source = self._get_gradient_energy_source() if self.enable_bio_integration else (
            EnergySource.SOLAR if renewable_profile and renewable_profile.can_use_renewable(10) else EnergySource.GRID_MIX)
        
        power_state = self._get_atp_driven_dvfs() if self.enable_bio_integration else (
            PowerState.PERFORMANCE if latency_requirement_ms < 10 else PowerState.BALANCED if latency_requirement_ms < 100 else PowerState.POWER_SAVE)
        power_config = self.power_states[power_state]
        
        quant_level = self._get_token_efficient_quantization(task_config.get('task_type', 'inference')) if self.enable_bio_integration else (
            'int8' if carbon_budget < 0.01 else 'fp16')
        quant_config = self.quantization_levels[quant_level]
        
        thermal = self._get_compartment_thermal_state() if self.enable_bio_integration else (thermal_profile or ThermalProfile())
        
        energy_factor = power_config['energy_factor'] * quant_config['energy_factor']
        base_energy = task_config.get('base_energy_kwh', 0.01)
        estimated_energy = base_energy * energy_factor
        estimated_carbon = estimated_energy * energy_source.carbon_intensity_g_per_kwh / 1000
        
        ecoatp_cost = estimated_energy * 1000
        if ecoatp_budget and ecoatp_cost > ecoatp_budget and self.enable_bio_integration:
            power_state = PowerState.POWER_SAVE; quant_level = 'int8'
            energy_factor = self.power_states[power_state]['energy_factor'] * self.quantization_levels[quant_level]['energy_factor']
            estimated_energy = base_energy * energy_factor
            ecoatp_cost = estimated_energy * 1000
        
        ecoatp_generated = self._generate_offset_tokens(estimated_carbon) if self.enable_bio_integration and estimated_carbon > 0 else 0.0
        
        plan = {
            'expert_id': self.expert_id, 'optimization_id': optimization_id, 'version': self.version,
            'energy_source': energy_source.value, 'renewable_percentage': 100 if energy_source.is_renewable else 25,
            'power_state': power_state.value, 'frequency_percent': power_config['frequency_percent'],
            'quantization': quant_level, 'accuracy_impact': quant_config['accuracy_impact'],
            'estimated_energy_kwh': estimated_energy, 'estimated_carbon_kg': estimated_carbon,
            'estimated_ecoatp_cost': ecoatp_cost, 'estimated_latency_ms': 40.0,
            'carbon_budget_compliant': estimated_carbon <= carbon_budget,
            'bio_integration_active': self.enable_bio_integration,
            'gradient_levels': gradient_levels, 'ecoatp_generated': ecoatp_generated,
            'harvester_forecast': self._get_harvester_renewable_forecast() if self.enable_bio_integration else {},
            'recommendations': self._generate_recommendations(gradient_levels, ecoatp_generated),
            'strategy': 'bio_optimized' if self.enable_bio_integration else 'standard',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        history = EnergyOptimizationHistory(timestamp=start_time, strategy=plan['strategy'],
                                           energy_source=energy_source.value, power_state=power_state.value,
                                           energy_saved_kwh=max(0, base_energy - estimated_energy),
                                           carbon_saved_kg=max(0, base_energy * 400 / 1000 - estimated_carbon),
                                           cost_saved=0.0, renewable_used=energy_source.is_renewable,
                                           success=True, ecoatp_generated=ecoatp_generated,
                                           gradient_level=gradient_levels.get('carbon', 0.5))
        self.optimization_history.append(history)
        self.total_energy_saved_kwh += history.energy_saved_kwh
        self.total_carbon_saved_kg += history.carbon_saved_kg
        self.total_ecoatp_generated += ecoatp_generated
        
        return plan
    
    def _generate_recommendations(self, gradient_levels: Dict[str, float], ecoatp_generated: float) -> List[str]:
        recs = []
        carbon = gradient_levels.get('carbon', 0.5)
        if carbon > 0.7: recs.append(f"High carbon gradient ({carbon:.2f}) - using stored energy.")
        elif carbon < 0.3: recs.append(f"Low carbon gradient ({carbon:.2f}) - grid energy is clean.")
        if ecoatp_generated > 0: recs.append(f"Generated {ecoatp_generated:.1f} Eco-ATP from carbon offsets.")
        return recs if recs else ["Energy configuration is optimal."]
    
    # ========================================================================
    # Natural Language Explanations
    # ========================================================================
    def explain_energy_decision(self, optimization_result: Dict[str, Any]) -> Dict[str, Any]:
        plan = optimization_result
        energy_source = plan.get('energy_source', 'unknown')
        power_state = plan.get('power_state', 'unknown')
        quantization = plan.get('quantization', 'unknown')
        carbon_kg = plan.get('estimated_carbon_kg', 0)
        ecoatp_generated = plan.get('ecoatp_generated', 0)
        
        if plan.get('bio_integration_active'):
            if energy_source in ['solar', 'wind', 'battery']:
                executive = f"Selected {energy_source} energy with {power_state} power and {quantization} quantization. Carbon: {carbon_kg:.6f}kg."
            else:
                executive = f"Using {energy_source} energy (gradient-driven). Generated {ecoatp_generated:.1f} Eco-ATP from offsets."
        else:
            executive = f"Standard optimization: {power_state} power, {quantization} precision. Carbon: {carbon_kg:.6f}kg."
        
        technical = [f"Energy source: {energy_source}", f"Power state: {power_state}",
                    f"Quantization: {quantization}", f"Estimated carbon: {carbon_kg:.6f} kg CO2",
                    f"Estimated energy: {plan.get('estimated_energy_kwh', 0):.6f} kWh",
                    f"Eco-ATP cost: {plan.get('estimated_ecoatp_cost', 0):.1f}",
                    f"Eco-ATP generated: {ecoatp_generated:.1f}"]
        
        if plan.get('bio_integration_active'):
            gradients = plan.get('gradient_levels', {})
            technical.append(f"Carbon gradient: {gradients.get('carbon', 0):.2f}")
        
        if energy_source == 'battery':
            counterfactual = "If carbon gradient were below 0.3, grid energy would be preferred."
        elif quantization == 'int8':
            counterfactual = "If token balance were above 500, fp16 precision would be used for 1% accuracy improvement."
        else:
            counterfactual = "If carbon budget were tighter, quantization would downgrade to int4 for 50% energy reduction."
        
        confidence = 0.85
        if plan.get('bio_integration_active'):
            harvester_conf = plan.get('harvester_forecast', {}).get('confidence', 0.5)
            confidence = 0.7 + harvester_conf * 0.2
        
        return {'decision_type': 'energy_optimization', 'executive_summary': executive,
                'technical_details': technical, 'counterfactual': counterfactual,
                'confidence': confidence, 'timestamp': datetime.utcnow().isoformat()}
    
    def explain_renewable_forecast(self) -> Dict[str, Any]:
        if not self.enable_bio_integration: return {'error': 'Bio-integration not enabled'}
        forecast = self._get_harvester_renewable_forecast()
        gradient_levels = self._get_gradient_levels()
        confidence = forecast.get('confidence', 0.5)
        solar = forecast.get('solar_kw', 0); wind = forecast.get('wind_kw', 0)
        total = forecast.get('total_renewable_kw', 0)
        
        if confidence > 0.7 and total > 10: outlook = "EXCELLENT: Strong renewable energy expected."
        elif confidence > 0.5 and total > 5: outlook = "GOOD: Adequate renewable energy available."
        elif confidence > 0.3: outlook = "FAIR: Limited renewable energy."
        else: outlook = "POOR: Minimal renewable energy."
        
        explanation = {'outlook': outlook, 'forecast': {'solar_kw': f"{solar:.1f}", 'wind_kw': f"{wind:.1f}",
                       'total_kw': f"{total:.1f}", 'confidence': f"{confidence:.0%}"},
                       'gradient_context': {'carbon': f"{gradient_levels.get('carbon', 0):.2f}"},
                       'recommendations': []}
        
        if solar > wind and solar > 3: explanation['recommendations'].append("Solar dominant. Schedule tasks during daylight.")
        if wind > solar and wind > 3: explanation['recommendations'].append("Wind dominant. Night-time processing optimal.")
        if confidence < 0.4: explanation['recommendations'].append("Low confidence. Maintain battery reserves above 50%.")
        if total < 5: explanation['recommendations'].append("Insufficient renewable. Activate conservation measures.")
        return explanation
    
    def get_decision_explanation(self, optimization_id: str) -> Optional[Dict[str, Any]]:
        for entry in self.optimization_history:
            plan = entry.plan if hasattr(entry, 'plan') else entry.get('plan', {})
            if plan.get('optimization_id') == optimization_id:
                return self.explain_energy_decision(plan)
        return None
    
    async def suggest_carbon_offset(self, carbon_impact: float, energy_source_plan: Optional[Dict[str, Any]] = None,
                                   renewable_profile: Optional[RenewableProfile] = None) -> Dict[str, Any]:
        strategies = [
            {'type': 'helium_offset', 'amount_kg': carbon_impact * 0.3, 'cost_per_kg': 0.05},
            {'type': 'renewable_certificates', 'amount_kg': carbon_impact * 0.5, 'cost_per_kg': 0.02},
            {'type': 'direct_air_capture', 'amount_kg': carbon_impact * 0.4, 'cost_per_kg': 0.15},
            {'type': 'reforestation', 'amount_kg': carbon_impact * 0.35, 'cost_per_kg': 0.01}
        ]
        best = min(strategies, key=lambda s: s['cost_per_kg'] * s['amount_kg'])
        ecoatp_generated = self._generate_offset_tokens(carbon_impact) if self.enable_bio_integration else 0.0
        return {'carbon_impact_kg': carbon_impact, 'strategies': strategies, 'recommended_strategy': best,
                'ecoatp_generated': ecoatp_generated, 'bio_integration_active': self.enable_bio_integration}
    
    def get_expert_statistics(self) -> Dict[str, Any]:
        recent = list(self.optimization_history)[-100:]
        stats = {
            'expert_id': self.expert_id, 'version': self.version,
            'total_energy_saved_kwh': self.total_energy_saved_kwh,
            'total_carbon_saved_kg': self.total_carbon_saved_kg,
            'total_ecoatp_generated': self.total_ecoatp_generated,
            'bio_integration_active': self.enable_bio_integration,
            'optimizations_performed': len(self.optimization_history),
            'renewable_usage_rate': sum(1 for r in recent if r.renewable_used) / max(len(recent), 1) if recent else 0,
            'average_ecoatp_generated': np.mean([r.ecoatp_generated for r in recent]) if recent else 0
        }
        if self.enable_bio_integration:
            stats['bio_metrics'] = {'gradient_levels': self._get_gradient_levels(),
                                    'harvester_forecast': self._get_harvester_renewable_forecast(),
                                    'atp_power_state': self._get_atp_driven_dvfs().value,
                                    'token_quantization': self._get_token_efficient_quantization()}
        return stats
