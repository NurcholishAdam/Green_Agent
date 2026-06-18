# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/photosynthetic_harvester.py
# Complete enhanced file v5.0.0 with all improvements

"""
Enhanced Photosynthetic Harvester v5.0.0
Complete implementation with demand-responsive harvesting, photoinhibition protection,
predictive windows, circadian rhythm, multi-harvester scaling, and direct gradient coupling.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
from collections import deque
import math

logger = logging.getLogger(__name__)

# ============================================================================
# Try importing dependencies
# ============================================================================
try:
    from .eco_atp_currency import EcoATPTokenManager, EcoATPSource
    TOKEN_MANAGER_AVAILABLE = True
except ImportError:
    TOKEN_MANAGER_AVAILABLE = False

try:
    from .proton_gradient_fields import GradientFieldManager
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False

# ============================================================================
# Enums and Data Classes
# ============================================================================

class PigmentState(Enum):
    """Pigment operational states"""
    ACTIVE = "active"
    PHOTOINHIBITED = "photoinhibited"
    REPAIRING = "repairing"
    QUIESCENT = "quiescent"
    DAMAGED = "damaged"

class HarvestingMode(Enum):
    """Harvesting operational modes"""
    FULL = "full"
    MODULATED = "modulated"
    CONSERVATIVE = "conservative"
    MINIMAL = "minimal"
    DORMANT = "dormant"

@dataclass
class PigmentHealth:
    """Health tracking for individual pigments"""
    pigment_name: str
    state: PigmentState = PigmentState.ACTIVE
    efficiency: float = 1.0
    damage_accumulation: float = 0.0
    repair_progress: float = 0.0
    total_excitations: int = 0
    total_conversions: int = 0
    last_repair: datetime = field(default_factory=datetime.utcnow)
    protection_level: float = 0.0  # Non-photochemical quenching level

@dataclass
class CircadianProfile:
    """Circadian rhythm profile for harvesting efficiency"""
    hour_efficiency: Dict[int, float] = field(default_factory=lambda: {
        0: 0.1, 1: 0.05, 2: 0.05, 3: 0.05, 4: 0.1, 5: 0.2,
        6: 0.4, 7: 0.6, 8: 0.8, 9: 0.9, 10: 0.95, 11: 1.0,
        12: 1.0, 13: 0.95, 14: 0.9, 15: 0.85, 16: 0.75,
        17: 0.6, 18: 0.4, 19: 0.25, 20: 0.15, 21: 0.1,
        22: 0.1, 23: 0.1
    })
    learned_patterns: Dict[int, float] = field(default_factory=dict)
    last_updated: datetime = field(default_factory=datetime.utcnow)

@dataclass
class ExcitationRecord:
    """Record of excitation events for predictive modeling"""
    timestamp: datetime
    pigment_name: str
    excitation_level: float
    converted_energy: float
    environmental_context: Dict[str, float]

# ============================================================================
# Enhanced Pigment Array
# ============================================================================

class EnhancedPigmentArray:
    """
    Enhanced multi-spectral sensor array with adaptive sensitivity,
    photoinhibition protection, and health tracking.
    """
    
    def __init__(self):
        # Pigment definitions with enhanced properties
        self.pigments = {
            'chlorophyll_a': {
                'target': 'renewable_availability',
                'sensitivity': 1.0,
                'base_sensitivity': 1.0,
                'response_time_ms': 100,
                'saturation_threshold': 0.9,
                'noise_floor': 0.05,
                'photoinhibition_rate': 0.001,  # Damage per unit excitation above safe level
                'safe_excitation_level': 0.7,    # Maximum safe continuous excitation
                'repair_rate': 0.01,             # Recovery per second when repairing
                'circadian_peak_hours': [10, 11, 12, 13, 14],  # Peak solar hours
                'specialization': 'solar'
            },
            'chlorophyll_b': {
                'target': 'carbon_intensity',
                'sensitivity': 0.8,
                'base_sensitivity': 0.8,
                'response_time_ms': 200,
                'saturation_threshold': 0.7,
                'noise_floor': 0.03,
                'photoinhibition_rate': 0.0005,
                'safe_excitation_level': 0.8,
                'repair_rate': 0.015,
                'circadian_peak_hours': list(range(24)),  # Always active
                'specialization': 'carbon'
            },
            'carotenoids': {
                'target': 'waste_heat',
                'sensitivity': 0.6,
                'base_sensitivity': 0.6,
                'response_time_ms': 500,
                'saturation_threshold': 0.8,
                'noise_floor': 0.1,
                'photoinhibition_rate': 0.0002,
                'safe_excitation_level': 0.9,
                'repair_rate': 0.02,
                'circadian_peak_hours': list(range(24)),
                'specialization': 'thermal'
            },
            'phycobilins': {
                'target': 'edge_availability',
                'sensitivity': 0.7,
                'base_sensitivity': 0.7,
                'response_time_ms': 300,
                'saturation_threshold': 0.6,
                'noise_floor': 0.08,
                'photoinhibition_rate': 0.0003,
                'safe_excitation_level': 0.85,
                'repair_rate': 0.012,
                'circadian_peak_hours': list(range(24)),
                'specialization': 'edge'
            },
            'xanthophylls': {
                'target': 'system_overload',
                'sensitivity': 0.9,
                'base_sensitivity': 0.9,
                'response_time_ms': 50,
                'saturation_threshold': 1.0,
                'noise_floor': 0.01,
                'photoinhibition_rate': 0.0001,
                'safe_excitation_level': 0.95,
                'repair_rate': 0.025,
                'circadian_peak_hours': list(range(24)),
                'specialization': 'protection'
            }
        }
        
        # Pigment health tracking
        self.pigment_health: Dict[str, PigmentHealth] = {
            name: PigmentHealth(pigment_name=name)
            for name in self.pigments
        }
        
        # Excitation history for predictive modeling
        self.excitation_history: Dict[str, deque] = {
            name: deque(maxlen=200) for name in self.pigments
        }
        
        # Circadian profile
        self.circadian_profile = CircadianProfile()
        
        # Predictive models
        self.prediction_models: Dict[str, Dict[str, float]] = {}
        
        # Start repair and adaptation loops
        asyncio.create_task(self._repair_loop())
        asyncio.create_task(self._adaptation_loop())
        
        logger.info(f"Enhanced Pigment Array initialized with {len(self.pigments)} pigment types")
    
    def sense_environment(self, environmental_data: Dict[str, float]) -> Dict[str, float]:
        """
        Enhanced environmental sensing with adaptive sensitivity,
        circadian modulation, and photoinhibition protection.
        """
        excitations = {}
        current_hour = datetime.utcnow().hour
        
        for pigment_name, pigment_config in self.pigments.items():
            target = pigment_config['target']
            
            if target not in environmental_data:
                excitations[pigment_name] = 0.0
                continue
            
            raw_value = environmental_data[target]
            health = self.pigment_health[pigment_name]
            
            # Skip damaged pigments
            if health.state == PigmentState.DAMAGED:
                excitations[pigment_name] = 0.0
                continue
            
            # Apply circadian modulation
            circadian_factor = self._get_circadian_factor(pigment_name, current_hour)
            
            # Apply health-based efficiency
            efficiency_factor = health.efficiency
            
            # Apply protection level (non-photochemical quenching)
            protection_factor = 1.0 - health.protection_level
            
            # Invert carbon intensity (lower = better)
            if target == 'carbon_intensity':
                raw_value = 1.0 - min(raw_value / 1000.0, 1.0)
            
            # Calculate effective sensitivity
            effective_sensitivity = (
                pigment_config['sensitivity'] *
                circadian_factor *
                efficiency_factor *
                protection_factor
            )
            
            # Calculate excitation
            excitation = raw_value * effective_sensitivity
            
            # Apply noise floor
            if excitation < pigment_config['noise_floor']:
                excitation = 0.0
            
            # Check saturation
            if excitation > pigment_config['saturation_threshold']:
                excitation = pigment_config['saturation_threshold']
            
            excitations[pigment_name] = excitation
            
            # Track photoinhibition
            self._track_photoinhibition(pigment_name, excitation, pigment_config)
            
            # Record for predictive modeling
            self.excitation_history[pigment_name].append(
                ExcitationRecord(
                    timestamp=datetime.utcnow(),
                    pigment_name=pigment_name,
                    excitation_level=excitation,
                    converted_energy=0.0,  # Will be set by reaction center
                    environmental_context=environmental_data
                )
            )
            
            # Update health tracking
            health.total_excitations += 1
        
        # Update predictive models
        if len(self.excitation_history.get('chlorophyll_a', [])) % 50 == 0:
            self._update_predictive_models()
        
        return excitations
    
    def _get_circadian_factor(self, pigment_name: str, hour: int) -> float:
        """Get circadian modulation factor for pigment at given hour"""
        config = self.pigments[pigment_name]
        
        # Use learned patterns if available, otherwise use default profile
        if pigment_name in self.circadian_profile.learned_patterns:
            profile = self.circadian_profile.learned_patterns.get(pigment_name, {})
            if hour in profile:
                return profile[hour]
        
        # Use default circadian profile
        return self.circadian_profile.hour_efficiency.get(hour, 0.5)
    
    def _track_photoinhibition(self, pigment_name: str, excitation: float, config: Dict):
        """Track and apply photoinhibition effects"""
        health = self.pigment_health[pigment_name]
        safe_level = config['safe_excitation_level']
        
        if excitation > safe_level:
            # Calculate damage
            excess = excitation - safe_level
            damage = excess * config['photoinhibition_rate']
            health.damage_accumulation += damage
            
            # Apply efficiency reduction
            health.efficiency = max(0.1, 1.0 - health.damage_accumulation)
            
            # Activate protection (non-photochemical quenching)
            health.protection_level = min(0.9, health.damage_accumulation * 2)
            
            # Check if photoinhibited
            if health.damage_accumulation > 0.3 and health.state == PigmentState.ACTIVE:
                health.state = PigmentState.PHOTOINHIBITED
                logger.warning(f"{pigment_name} photoinhibited (damage: {health.damage_accumulation:.2f})")
            
            # Check if damaged
            if health.damage_accumulation > 0.7 and health.state != PigmentState.DAMAGED:
                health.state = PigmentState.DAMAGED
                logger.error(f"{pigment_name} DAMAGED (damage: {health.damage_accumulation:.2f})")
        else:
            # Gradual recovery when below safe level
            if health.damage_accumulation > 0:
                health.damage_accumulation = max(0, health.damage_accumulation - 0.001)
                health.efficiency = max(0.1, 1.0 - health.damage_accumulation)
                health.protection_level = max(0, health.protection_level - 0.005)
    
    async def _repair_loop(self):
        """Background repair loop for photoinhibited pigments"""
        while True:
            try:
                for name, health in self.pigment_health.items():
                    if health.state in [PigmentState.PHOTOINHIBITED, PigmentState.REPAIRING]:
                        # Apply repair
                        config = self.pigments[name]
                        health.repair_progress += config['repair_rate']
                        
                        if health.repair_progress >= 1.0:
                            # Repair complete
                            health.state = PigmentState.ACTIVE
                            health.damage_accumulation = max(0, health.damage_accumulation - 0.2)
                            health.efficiency = 1.0 - health.damage_accumulation
                            health.repair_progress = 0.0
                            health.protection_level = 0.0
                            health.last_repair = datetime.utcnow()
                            logger.info(f"{name} repaired and reactivated")
                        else:
                            health.state = PigmentState.REPAIRING
                
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Repair loop error: {str(e)}")
                await asyncio.sleep(30)
    
    async def _adaptation_loop(self):
        """Background adaptation loop for learning circadian patterns"""
        while True:
            try:
                # Learn efficiency patterns by hour
                for pigment_name in self.pigments:
                    history = list(self.excitation_history.get(pigment_name, []))
                    if len(history) < 100:
                        continue
                    
                    # Group by hour and calculate average excitation
                    hour_excitations = {}
                    for record in history[-500:]:
                        hour = record.timestamp.hour
                        if hour not in hour_excitations:
                            hour_excitations[hour] = []
                        hour_excitations[hour].append(record.excitation_level)
                    
                    # Update learned patterns
                    if pigment_name not in self.circadian_profile.learned_patterns:
                        self.circadian_profile.learned_patterns[pigment_name] = {}
                    
                    for hour, excitations in hour_excitations.items():
                        if len(excitations) >= 5:
                            avg = np.mean(excitations)
                            # Blend with existing
                            old = self.circadian_profile.learned_patterns[pigment_name].get(hour, avg)
                            self.circadian_profile.learned_patterns[pigment_name][hour] = old * 0.7 + avg * 0.3
                
                self.circadian_profile.last_updated = datetime.utcnow()
                await asyncio.sleep(3600)  # Update every hour
                
            except Exception as e:
                logger.error(f"Adaptation loop error: {str(e)}")
                await asyncio.sleep(3600)
    
    def _update_predictive_models(self):
        """Update predictive models for each pigment"""
        for pigment_name in self.pigments:
            history = list(self.excitation_history.get(pigment_name, []))
            if len(history) < 20:
                continue
            
            values = [h.excitation_level for h in history[-50:]]
            
            # Simple exponential smoothing
            alpha = 0.3
            level = values[0]
            trend = 0
            
            for i in range(1, len(values)):
                new_level = alpha * values[i] + (1 - alpha) * (level + trend)
                new_trend = 0.1 * (new_level - level) + 0.9 * trend
                level, trend = new_level, new_trend
            
            self.prediction_models[pigment_name] = {
                'level': level,
                'trend': trend,
                'last_updated': datetime.utcnow()
            }
    
    def predict_excitation(self, pigment_name: str, horizon_seconds: float) -> float:
        """Predict future excitation for a pigment"""
        if pigment_name not in self.prediction_models:
            return 0.5
        
        model = self.prediction_models[pigment_name]
        predicted = model['level'] + model['trend'] * horizon_seconds
        return max(0, min(1, predicted))
    
    def get_antenna_amplification(self, excitations: Dict[str, float]) -> Dict[str, float]:
        """
        Enhanced cooperative amplification with adaptive factors.
        
        Learns which pigment pairs provide the most reliable amplification.
        """
        amplified = excitations.copy()
        
        # Dynamic amplification pairs (all combinations)
        pigment_names = list(excitations.keys())
        
        for i, pigment_a in enumerate(pigment_names):
            for pigment_b in pigment_names[i+1:]:
                if pigment_a in amplified and pigment_b in amplified:
                    # Calculate correlation-based amplification
                    correlation = self._calculate_correlation(pigment_a, pigment_b)
                    amp_factor = 1.0 + (amplified[pigment_a] * amplified[pigment_b] * 0.3 * correlation)
                    amplified[pigment_a] *= amp_factor
                    amplified[pigment_b] *= amp_factor
        
        # Clamp to saturation thresholds
        for name in amplified:
            if name in self.pigments:
                amplified[name] = min(amplified[name], self.pigments[name]['saturation_threshold'])
        
        return amplified
    
    def _calculate_correlation(self, pigment_a: str, pigment_b: str) -> float:
        """Calculate correlation between two pigments' excitation histories"""
        history_a = [h.excitation_level for h in list(self.excitation_history.get(pigment_a, []))[-50:]]
        history_b = [h.excitation_level for h in list(self.excitation_history.get(pigment_b, []))[-50:]]
        
        if len(history_a) < 10 or len(history_b) < 10:
            return 0.5  # Default correlation
        
        min_len = min(len(history_a), len(history_b))
        correlation = np.corrcoef(history_a[:min_len], history_b[:min_len])[0, 1]
        return max(0, correlation) if not np.isnan(correlation) else 0.5
    
    def get_dominant_signal(self, excitations: Dict[str, float]) -> Tuple[str, float]:
        """Get the dominant environmental signal"""
        if not excitations:
            return 'none', 0.0
        return max(excitations.items(), key=lambda x: x[1])
    
    def get_pigment_health_summary(self) -> Dict[str, Any]:
        """Get health summary for all pigments"""
        return {
            name: {
                'state': health.state.value,
                'efficiency': health.efficiency,
                'damage': health.damage_accumulation,
                'repair_progress': health.repair_progress,
                'protection_level': health.protection_level,
                'total_excitations': health.total_excitations
            }
            for name, health in self.pigment_health.items()
        }
    
    def get_circadian_summary(self) -> Dict[str, Any]:
        """Get circadian rhythm summary"""
        return {
            'default_profile': self.circadian_profile.hour_efficiency,
            'learned_patterns': self.circadian_profile.learned_patterns,
            'last_updated': self.circadian_profile.last_updated.isoformat()
        }
    
    def get_predictions(self) -> Dict[str, Dict[str, Any]]:
        """Get predictions for all pigments"""
        predictions = {}
        for name in self.pigments:
            short = self.predict_excitation(name, 60)
            medium = self.predict_excitation(name, 300)
            predictions[name] = {
                'short_term_60s': short,
                'medium_term_300s': medium,
                'trend': 'rising' if medium > short else 'falling' if medium < short else 'stable'
            }
        return predictions

# ============================================================================
# Enhanced Reaction Center
# ============================================================================

class EnhancedReactionCenter:
    """
    Enhanced reaction center with demand-responsive conversion,
    direct gradient coupling, and efficiency tracking.
    """
    
    def __init__(self, token_manager=None, gradient_manager=None):
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        
        # Conversion efficiency (dynamic)
        self.base_quantum_efficiency = 0.85
        self.current_efficiency = 0.85
        self.min_efficiency = 0.3
        
        # Energy thresholds
        self.activation_threshold = 0.1
        self.saturation_point = 0.9
        
        # Demand-responsive parameters
        self.demand_modulation_enabled = True
        self.token_abundance_threshold = 50000  # Reduce above this
        self.token_scarcity_threshold = 5000    # Increase below this
        
        # Photoinhibition tracking
        self.cumulative_damage = 0.0
        self.repair_rate = 0.005
        
        # Conversion history
        self.conversion_history: deque = deque(maxlen=1000)
        
        # Start maintenance
        asyncio.create_task(self._maintenance_loop())
        
        logger.info(f"Enhanced Reaction Center initialized: base_efficiency={self.base_quantum_efficiency}")
    
    def modulate_efficiency(self) -> float:
        """
        Modulate conversion efficiency based on system demand.
        
        Reduces efficiency when tokens are abundant (prevent inflation).
        Increases efficiency when tokens are scarce (maximize production).
        """
        if not self.demand_modulation_enabled or not self.token_manager:
            return self.base_quantum_efficiency
        
        summary = self.token_manager.get_system_summary()
        balance = summary.get('total_balance', 10000)
        
        if balance > self.token_abundance_threshold:
            # Too many tokens - reduce efficiency
            excess_ratio = (balance - self.token_abundance_threshold) / self.token_abundance_threshold
            efficiency = self.base_quantum_efficiency * (1.0 / (1.0 + excess_ratio))
            efficiency = max(self.min_efficiency, efficiency)
        elif balance < self.token_scarcity_threshold:
            # Too few tokens - increase efficiency
            scarcity_ratio = (self.token_scarcity_threshold - balance) / self.token_scarcity_threshold
            efficiency = self.base_quantum_efficiency * (1.0 + scarcity_ratio * 0.5)
            efficiency = min(0.98, efficiency)
        else:
            # Balanced - use base efficiency
            efficiency = self.base_quantum_efficiency
        
        # Apply photoinhibition damage
        efficiency *= (1.0 - self.cumulative_damage * 0.5)
        
        self.current_efficiency = max(self.min_efficiency, min(0.98, efficiency))
        return self.current_efficiency
    
    def convert_excitation(self, excitations: Dict[str, float], account_id: str) -> float:
        """
        Enhanced conversion with demand modulation and direct gradient coupling.
        
        Returns amount of Eco-ATP generated.
        """
        total_excitation = sum(excitations.values())
        
        # Check activation threshold
        if total_excitation < self.activation_threshold:
            return 0.0
        
        # Clamp to saturation
        effective_excitation = min(total_excitation, self.saturation_point)
        
        # Modulate efficiency based on demand
        efficiency = self.modulate_efficiency()
        
        # Apply efficiency
        convertible_energy = effective_excitation * efficiency
        
        # Track photoinhibition from high excitation
        if effective_excitation > 0.8:
            self.cumulative_damage += 0.0005
        elif effective_excitation < 0.3:
            self.cumulative_damage = max(0, self.cumulative_damage - 0.0001)
        
        # Calculate carbon/helium savings
        carbon_saved = 0.0
        helium_saved = 0.0
        energy_saved = 0.0
        
        if 'chlorophyll_a' in excitations and excitations['chlorophyll_a'] > 0:
            energy_saved = excitations['chlorophyll_a'] * 0.01
        if 'chlorophyll_b' in excitations and excitations['chlorophyll_b'] > 0:
            carbon_saved = excitations['chlorophyll_b'] * 0.001
        if 'carotenoids' in excitations and excitations['carotenoids'] > 0:
            helium_saved = excitations['carotenoids'] * 0.01
        
        # Generate Eco-ATP tokens
        total_generated = 0.0
        if self.token_manager:
            tokens = self.token_manager.generate_tokens(
                account_id=account_id,
                source=EcoATPSource.RENEWABLE_ENERGY if 'chlorophyll_a' in excitations else EcoATPSource.EFFICIENCY_GAIN,
                carbon_saved_kg=carbon_saved,
                helium_saved_units=helium_saved,
                energy_saved_kwh=energy_saved,
                efficiency=efficiency
            )
            if tokens:
                total_generated = sum(t.value for t in tokens)
        
        # DIRECT GRADIENT COUPLING: Pump gradient fields
        if self.gradient_manager:
            # Pump opportunity gradient
            if total_generated > 0:
                self.gradient_manager.pump_field('opportunity', total_generated / 1000.0, source='harvester')
            
            # Pump carbon gradient when detecting low carbon
            if 'chlorophyll_b' in excitations and excitations['chlorophyll_b'] > 0.5:
                self.gradient_manager.pump_field('carbon', -0.01, source='harvester_carbon_detection')
        
        # Record conversion
        self.conversion_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'total_excitation': total_excitation,
            'effective_excitation': effective_excitation,
            'efficiency': efficiency,
            'convertible_energy': convertible_energy,
            'eco_atp_generated': total_generated,
            'carbon_saved': carbon_saved,
            'helium_saved': helium_saved,
            'energy_saved': energy_saved,
            'cumulative_damage': self.cumulative_damage
        })
        
        if total_generated > 0:
            logger.debug(
                f"Reaction Center: {total_excitation:.3f} excitation → "
                f"{total_generated:.1f} Eco-ATP (efficiency: {efficiency:.2f})"
            )
        
        return total_generated
    
    async def _maintenance_loop(self):
        """Background maintenance for reaction center repair"""
        while True:
            try:
                # Gradual repair of cumulative damage
                if self.cumulative_damage > 0:
                    self.cumulative_damage = max(0, self.cumulative_damage - self.repair_rate)
                
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Reaction center maintenance error: {str(e)}")
                await asyncio.sleep(300)
    
    def get_efficiency_stats(self) -> Dict[str, Any]:
        """Get efficiency statistics"""
        recent = list(self.conversion_history)[-50:]
        return {
            'current_efficiency': self.current_efficiency,
            'base_efficiency': self.base_quantum_efficiency,
            'cumulative_damage': self.cumulative_damage,
            'demand_modulation': self.demand_modulation_enabled,
            'average_efficiency': np.mean([c['efficiency'] for c in recent]) if recent else 0,
            'total_conversions': len(self.conversion_history)
        }

# ============================================================================
# Enhanced Photosynthetic Harvester
# ============================================================================

class EnhancedPhotosyntheticHarvester:
    """
    Enhanced Photosynthetic Harvester v5.0.0
    
    Complete implementation with:
    - Demand-responsive harvesting
    - Photoinhibition protection and repair
    - Predictive harvesting windows
    - Circadian rhythm integration
    - Multi-harvester scaling support
    - Direct gradient field coupling
    - ATP synthase feedback
    """
    
    def __init__(self, token_manager=None, gradient_manager=None, harvester_id: str = "primary"):
        self.harvester_id = harvester_id
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        
        # Enhanced sub-modules
        self.pigments = EnhancedPigmentArray()
        self.reaction_center = EnhancedReactionCenter(token_manager, gradient_manager)
        
        # Harvesting mode
        self.mode = HarvestingMode.FULL
        
        # Harvesting statistics
        self.total_harvested = 0.0
        self.harvesting_efficiency = 0.0
        self.peak_harvest_rate = 0.0
        self.harvest_cycles = 0
        
        # Account for harvested energy
        self.account_id = f"photosynthetic_{harvester_id}"
        if token_manager:
            token_manager.create_account(self.account_id)
        
        # Predictive window tracking
        self.predicted_peaks: Dict[str, datetime] = {}
        
        # Child harvesters (multi-harvester scaling)
        self.child_harvesters: Dict[str, 'EnhancedPhotosyntheticHarvester'] = {}
        self.is_child = harvester_id != "primary"
        
        # Start maintenance
        asyncio.create_task(self._predictive_window_loop())
        
        logger.info(f"Enhanced Photosynthetic Harvester '{harvester_id}' initialized")
    
    def set_mode(self, mode: HarvestingMode):
        """Set harvesting operational mode"""
        self.mode = mode
        
        # Adjust efficiencies based on mode
        mode_efficiencies = {
            HarvestingMode.FULL: 1.0,
            HarvestingMode.MODULATED: 0.8,
            HarvestingMode.CONSERVATIVE: 0.5,
            HarvestingMode.MINIMAL: 0.2,
            HarvestingMode.DORMANT: 0.0
        }
        
        self.reaction_center.current_efficiency = (
            self.reaction_center.base_quantum_efficiency * mode_efficiencies.get(mode, 1.0)
        )
        
        logger.info(f"Harvester '{self.harvester_id}' mode: {mode.value}")
    
    def set_mode_from_degradation_tier(self, tier: int):
        """Set harvesting mode based on degradation tier"""
        tier_mode_map = {
            5: HarvestingMode.FULL,
            4: HarvestingMode.MODULATED,
            3: HarvestingMode.CONSERVATIVE,
            2: HarvestingMode.MINIMAL,
            1: HarvestingMode.DORMANT
        }
        mode = tier_mode_map.get(tier, HarvestingMode.MODULATED)
        self.set_mode(mode)
    
    async def harvest_cycle(self, environmental_data: Dict[str, float]) -> Dict[str, Any]:
        """
        Enhanced harvesting cycle with all improvements.
        """
        # Skip harvesting if dormant
        if self.mode == HarvestingMode.DORMANT:
            return {
                'harvester_id': self.harvester_id,
                'eco_atp_generated': 0.0,
                'mode': 'dormant',
                'reason': 'System in survival mode'
            }
        
        # Step 1: Sense environment with adaptive pigments
        raw_excitations = self.pigments.sense_environment(environmental_data)
        
        # Step 2: Amplify signals with learned correlations
        amplified_excitations = self.pigments.get_antenna_amplification(raw_excitations)
        
        # Step 3: Convert to Eco-ATP with demand modulation
        eco_atp_generated = self.reaction_center.convert_excitation(
            amplified_excitations, self.account_id
        )
        
        # Update statistics
        self.total_harvested += eco_atp_generated
        self.harvest_cycles += 1
        
        if eco_atp_generated > self.peak_harvest_rate:
            self.peak_harvest_rate = eco_atp_generated
        
        # Get dominant signal
        dominant_name, dominant_value = self.pigments.get_dominant_signal(amplified_excitations)
        
        # Delegate to child harvesters if needed
        child_results = {}
        for child_id, child in self.child_harvesters.items():
            child_result = await child.harvest_cycle(environmental_data)
            child_results[child_id] = child_result
        
        result = {
            'harvester_id': self.harvester_id,
            'timestamp': datetime.utcnow().isoformat(),
            'mode': self.mode.value,
            'raw_excitations': raw_excitations,
            'amplified_excitations': amplified_excitations,
            'dominant_signal': dominant_name,
            'dominant_value': dominant_value,
            'eco_atp_generated': eco_atp_generated,
            'total_harvested': self.total_harvested,
            'efficiency': self.reaction_center.current_efficiency,
            'pigment_health': self.pigments.get_pigment_health_summary(),
            'predictions': self.pigments.get_predictions(),
            'child_results': child_results if child_results else None,
            'account_balance': self.token_manager.get_account_summary(self.account_id).get('balance', 0) if self.token_manager else 0
        }
        
        if eco_atp_generated > 0:
            logger.debug(
                f"Harvest cycle: {eco_atp_generated:.1f} Eco-ATP "
                f"(dominant: {dominant_name} @ {dominant_value:.3f}, "
                f"efficiency: {self.reaction_center.current_efficiency:.2f})"
            )
        
        return result
    
    def spawn_child(self, specialization: str) -> 'EnhancedPhotosyntheticHarvester':
        """
        Spawn a child harvester specialized in a particular pigment type.
        
        Enables multi-harvester scaling for high-demand scenarios.
        """
        child_id = f"{self.harvester_id}_child_{specialization}_{len(self.child_harvesters)}"
        
        child = EnhancedPhotosyntheticHarvester(
            token_manager=self.token_manager,
            gradient_manager=self.gradient_manager,
            harvester_id=child_id
        )
        child.is_child = True
        
        # Specialize the child's pigments
        for pigment_name, pigment_config in child.pigments.pigments.items():
            if pigment_config['specialization'] != specialization:
                # Reduce sensitivity for non-specialized pigments
                pigment_config['sensitivity'] *= 0.3
            else:
                # Increase sensitivity for specialized pigment
                pigment_config['sensitivity'] *= 1.5
        
        self.child_harvesters[child_id] = child
        
        logger.info(f"Spawned child harvester '{child_id}' specialized in {specialization}")
        return child
    
    def remove_child(self, child_id: str) -> bool:
        """Remove a child harvester"""
        if child_id in self.child_harvesters:
            del self.child_harvesters[child_id]
            logger.info(f"Removed child harvester '{child_id}'")
            return True
        return False
    
    async def _predictive_window_loop(self):
        """Background loop for predictive harvesting window optimization"""
        while True:
            try:
                # Predict next peak for each pigment
                for pigment_name in self.pigments.pigments:
                    prediction = self.pigments.predict_excitation(pigment_name, 300)
                    if prediction > 0.7:
                        peak_time = datetime.utcnow() + timedelta(seconds=300)
                        self.predicted_peaks[pigment_name] = peak_time
                
                # Adjust mode based on predictions
                now = datetime.utcnow()
                imminent_peaks = sum(1 for peak_time in self.predicted_peaks.values() 
                                   if 0 < (peak_time - now).total_seconds() < 600)
                
                if imminent_peaks >= 2 and self.mode in [HarvestingMode.CONSERVATIVE, HarvestingMode.MINIMAL]:
                    self.set_mode(HarvestingMode.MODULATED)
                    logger.info("Preparing for predicted harvesting peak")
                
                await asyncio.sleep(120)
                
            except Exception as e:
                logger.error(f"Predictive window error: {str(e)}")
                await asyncio.sleep(300)
    
    def get_harvesting_stats(self) -> Dict[str, Any]:
        """Get comprehensive harvesting statistics"""
        stats = {
            'harvester_id': self.harvester_id,
            'total_harvested': self.total_harvested,
            'harvest_cycles': self.harvest_cycles,
            'peak_harvest_rate': self.peak_harvest_rate,
            'mode': self.mode.value,
            'efficiency': self.reaction_center.current_efficiency,
            'account_balance': self.token_manager.get_account_summary(self.account_id).get('balance', 0) if self.token_manager else 0,
            'pigment_health': self.pigments.get_pigment_health_summary(),
            'circadian': self.pigments.get_circadian_summary(),
            'predictions': self.pigments.get_predictions(),
            'reaction_center': self.reaction_center.get_efficiency_stats(),
            'predicted_peaks': {k: v.isoformat() for k, v in self.predicted_peaks.items()},
            'child_harvesters': len(self.child_harvesters),
            'is_child': self.is_child
        }
        
        # Add recent conversions
        recent = list(self.reaction_center.conversion_history)[-10:]
        stats['recent_conversions'] = recent
        
        return stats
    
    def get_circadian_report(self) -> Dict[str, Any]:
        """Get circadian rhythm optimization report"""
        profile = self.pigments.circadian_profile
        
        # Find optimal harvesting hours
        hour_efficiencies = profile.hour_efficiency.copy()
        
        # Merge learned patterns
        for pigment, patterns in profile.learned_patterns.items():
            for hour, efficiency in patterns.items():
                if hour in hour_efficiencies:
                    hour_efficiencies[hour] = (hour_efficiencies[hour] + efficiency) / 2
        
        optimal_hours = sorted(hour_efficiencies.items(), key=lambda x: x[1], reverse=True)[:6]
        
        return {
            'optimal_hours': [{'hour': h, 'efficiency': eff} for h, eff in optimal_hours],
            'current_hour_efficiency': hour_efficiencies.get(datetime.utcnow().hour, 0.5),
            'learned_patterns_count': sum(len(p) for p in profile.learned_patterns.values()),
            'recommendations': [
                f"Peak harvesting at {h}:00 ({eff:.0%} efficiency)" for h, eff in optimal_hours[:3]
            ]
        }

# ============================================================================
# Legacy Compatibility
# ============================================================================

class PhotosyntheticHarvester(EnhancedPhotosyntheticHarvester):
    """
    Legacy Photosynthetic Harvester for backward compatibility.
    Maintains original interface while using enhanced functionality.
    """
    
    def __init__(self, token_manager=None):
        super().__init__(token_manager=token_manager, harvester_id="primary")
        logger.info("Photosynthetic Harvester initialized (legacy compatibility mode)")
    
    async def harvest_cycle(self, environmental_data: Dict[str, float]) -> Dict[str, Any]:
        """Legacy harvest cycle (simplified interface)"""
        result = await super().harvest_cycle(environmental_data)
        
        # Return simplified result for backward compatibility
        return {
            'eco_atp_generated': result['eco_atp_generated'],
            'total_harvested': result['total_harvested'],
            'dominant_signal': result['dominant_signal'],
            'recent_conversions': result.get('recent_conversions', [])
        }
