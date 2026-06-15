# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/photosynthetic_harvester.py

"""
Photosynthetic Environmental Harvester
Version: 1.0.0

Detects environmental opportunities and converts them into Eco-ATP.
Inspired by photosynthetic light-harvesting complexes.

Biological Analogy: Photosystem I & II
- Pigment arrays detect different wavelengths (opportunity types)
- Antenna complexes amplify weak signals
- Reaction centers convert excitation into chemical energy
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
# Pigment Arrays - Opportunity Sensors
# ============================================================================

class PigmentArray:
    """
    Multi-spectral sensor array for environmental opportunities.
    
    Each "pigment" detects a specific type of opportunity.
    """
    
    def __init__(self):
        # Different pigment types detect different opportunities
        self.pigments = {
            'chlorophyll_a': {  # 680nm - Renewable energy peaks
                'target': 'renewable_availability',
                'sensitivity': 1.0,
                'response_time_ms': 100,
                'saturation_threshold': 0.9,
                'noise_floor': 0.05
            },
            'chlorophyll_b': {  # 640nm - Low carbon intensity
                'target': 'carbon_intensity',
                'sensitivity': 0.8,
                'response_time_ms': 200,
                'saturation_threshold': 0.7,
                'noise_floor': 0.03
            },
            'carotenoids': {  # 450-550nm - Waste heat recovery
                'target': 'waste_heat',
                'sensitivity': 0.6,
                'response_time_ms': 500,
                'saturation_threshold': 0.8,
                'noise_floor': 0.1
            },
            'phycobilins': {  # 550-650nm - Edge computing opportunities
                'target': 'edge_availability',
                'sensitivity': 0.7,
                'response_time_ms': 300,
                'saturation_threshold': 0.6,
                'noise_floor': 0.08
            },
            'xanthophylls': {  # Protection pigments - Overload detection
                'target': 'system_overload',
                'sensitivity': 0.9,
                'response_time_ms': 50,
                'saturation_threshold': 1.0,
                'noise_floor': 0.01
            }
        }
        
        # Excitation history
        self.excitation_history: Dict[str, deque] = {
            name: deque(maxlen=100) for name in self.pigments
        }
        
        logger.info(f"Pigment Array initialized with {len(self.pigments)} sensor types")
    
    def sense_environment(
        self,
        environmental_data: Dict[str, float]
    ) -> Dict[str, float]:
        """
        Sense environmental opportunities.
        
        Returns excitation levels for each pigment.
        """
        excitations = {}
        
        for pigment_name, pigment_config in self.pigments.items():
            target = pigment_config['target']
            
            if target in environmental_data:
                raw_value = environmental_data[target]
                
                # Apply sensitivity and noise filtering
                sensitivity = pigment_config['sensitivity']
                noise_floor = pigment_config['noise_floor']
                
                # Invert carbon intensity (lower = better opportunity)
                if target == 'carbon_intensity':
                    raw_value = 1.0 - min(raw_value / 1000.0, 1.0)
                
                # Calculate excitation
                excitation = raw_value * sensitivity
                
                # Apply noise floor
                if excitation < noise_floor:
                    excitation = 0.0
                
                # Check saturation
                if excitation > pigment_config['saturation_threshold']:
                    excitation = pigment_config['saturation_threshold']
                
                excitations[pigment_name] = excitation
                
                # Record history
                self.excitation_history[pigment_name].append({
                    'value': excitation,
                    'timestamp': datetime.utcnow()
                })
            else:
                excitations[pigment_name] = 0.0
        
        return excitations
    
    def get_antenna_amplification(
        self,
        excitations: Dict[str, float]
    ) -> Dict[str, float]:
        """
        Amplify weak signals through cooperative antenna effects.
        
        Adjacent pigments can amplify each other's signals.
        """
        amplified = excitations.copy()
        
        # Cooperative amplification pairs
        pairs = [
            ('chlorophyll_a', 'chlorophyll_b'),  # Renewable + Low carbon
            ('chlorophyll_a', 'carotenoids'),     # Renewable + Waste heat
            ('phycobilins', 'carotenoids'),       # Edge + Waste heat
        ]
        
        for pigment_a, pigment_b in pairs:
            if pigment_a in amplified and pigment_b in amplified:
                # Mutual amplification
                amp_factor = 1.0 + (amplified[pigment_a] * amplified[pigment_b] * 0.3)
                amplified[pigment_a] *= amp_factor
                amplified[pigment_b] *= amp_factor
        
        # Clamp to saturation
        for name in amplified:
            if name in self.pigments:
                amplified[name] = min(
                    amplified[name],
                    self.pigments[name]['saturation_threshold']
                )
        
        return amplified
    
    def get_dominant_signal(
        self,
        excitations: Dict[str, float]
    ) -> Tuple[str, float]:
        """Get the dominant environmental signal"""
        if not excitations:
            return 'none', 0.0
        
        dominant = max(excitations.items(), key=lambda x: x[1])
        return dominant


# ============================================================================
# Reaction Centers - Signal Conversion
# ============================================================================

class ReactionCenter:
    """
    Converts environmental excitation into actionable Eco-ATP.
    
    Biological analogy: Photosystem reaction centers
    - PSII: Splits water, generates proton gradient
    - PSI: Generates reducing power (NADPH)
    """
    
    def __init__(self, token_manager):
        self.token_manager = token_manager
        
        # Conversion efficiency
        self.quantum_efficiency = 0.85  # 85% of photons converted
        
        # Energy thresholds
        self.activation_threshold = 0.1  # Minimum excitation to trigger
        self.saturation_point = 0.9  # Maximum useful excitation
        
        # Conversion history
        self.conversion_history: deque = deque(maxlen=1000)
        
        logger.info(f"Reaction Center initialized: efficiency={self.quantum_efficiency}")
    
    def convert_excitation(
        self,
        excitations: Dict[str, float],
        account_id: str
    ) -> float:
        """
        Convert environmental excitation into Eco-ATP.
        
        Returns amount of Eco-ATP generated.
        """
        total_excitation = sum(excitations.values())
        
        # Check activation threshold
        if total_excitation < self.activation_threshold:
            return 0.0
        
        # Clamp to saturation
        effective_excitation = min(total_excitation, self.saturation_point)
        
        # Apply quantum efficiency
        convertible_energy = effective_excitation * self.quantum_efficiency
        
        # Calculate carbon/helium savings
        carbon_saved = 0.0
        helium_saved = 0.0
        energy_saved = 0.0
        
        if 'chlorophyll_a' in excitations and excitations['chlorophyll_a'] > 0:
            # Renewable energy opportunity
            energy_saved = excitations['chlorophyll_a'] * 0.01  # kWh
        
        if 'chlorophyll_b' in excitations and excitations['chlorophyll_b'] > 0:
            # Low carbon opportunity
            carbon_saved = excitations['chlorophyll_b'] * 0.001  # kg CO2
        
        if 'carotenoids' in excitations and excitations['carotenoids'] > 0:
            # Waste heat recovery
            helium_saved = excitations['carotenoids'] * 0.01  # helium units
        
        # Generate Eco-ATP tokens
        tokens = self.token_manager.generate_tokens(
            account_id=account_id,
            source=EcoATPSource.RENEWABLE_ENERGY if 'chlorophyll_a' in excitations else EcoATPSource.EFFICIENCY_GAIN,
            carbon_saved_kg=carbon_saved,
            helium_saved_units=helium_saved,
            energy_saved_kwh=energy_saved,
            efficiency=self.quantum_efficiency
        )
        
        total_generated = sum(t.value for t in tokens) if tokens else 0.0
        
        # Record conversion
        self.conversion_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'total_excitation': total_excitation,
            'effective_excitation': effective_excitation,
            'convertible_energy': convertible_energy,
            'eco_atp_generated': total_generated,
            'carbon_saved': carbon_saved,
            'helium_saved': helium_saved,
            'energy_saved': energy_saved
        })
        
        if total_generated > 0:
            logger.info(
                f"Reaction Center: {total_excitation:.3f} excitation → "
                f"{total_generated:.1f} Eco-ATP generated"
            )
        
        return total_generated


# ============================================================================
# Photosynthetic Harvester
# ============================================================================

class PhotosyntheticHarvester:
    """
    Complete photosynthetic harvesting system.
    
    Combines pigment arrays, antenna amplification, and reaction centers
    to convert environmental opportunities into Eco-ATP.
    """
    
    def __init__(self, token_manager):
        self.token_manager = token_manager
        self.pigments = PigmentArray()
        self.reaction_center = ReactionCenter(token_manager)
        
        # Harvesting statistics
        self.total_harvested = 0.0
        self.harvesting_efficiency = 0.0
        self.peak_harvest_rate = 0.0
        
        # Account for harvested energy
        self.account_id = "photosynthetic_harvester"
        self.token_manager.create_account(self.account_id)
        
        logger.info("Photosynthetic Harvester initialized")
    
    async def harvest_cycle(
        self,
        environmental_data: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        Execute one harvesting cycle.
        
        1. Sense environment with pigment arrays
        2. Amplify weak signals through antenna effects
        3. Convert excitation to Eco-ATP in reaction center
        
        Returns harvesting results.
        """
        # Step 1: Sense environment
        raw_excitations = self.pigments.sense_environment(environmental_data)
        
        # Step 2: Amplify signals
        amplified_excitations = self.pigments.get_antenna_amplification(raw_excitations)
        
        # Step 3: Convert to Eco-ATP
        eco_atp_generated = self.reaction_center.convert_excitation(
            amplified_excitations,
            self.account_id
        )
        
        # Update statistics
        self.total_harvested += eco_atp_generated
        
        # Get dominant signal
        dominant_name, dominant_value = self.pigments.get_dominant_signal(
            amplified_excitations
        )
        
        result = {
            'timestamp': datetime.utcnow().isoformat(),
            'raw_excitations': raw_excitations,
            'amplified_excitations': amplified_excitations,
            'dominant_signal': dominant_name,
            'dominant_value': dominant_value,
            'eco_atp_generated': eco_atp_generated,
            'total_harvested': self.total_harvested,
            'account_balance': self.token_manager.get_account_summary(self.account_id).get('balance', 0)
        }
        
        if eco_atp_generated > 0:
            logger.debug(
                f"Harvest cycle: {eco_atp_generated:.1f} Eco-ATP "
                f"(dominant: {dominant_name} @ {dominant_value:.3f})"
            )
        
        return result
    
    def get_harvesting_stats(self) -> Dict[str, Any]:
        """Get harvesting statistics"""
        return {
            'total_harvested': self.total_harvested,
            'account_balance': self.token_manager.get_account_summary(self.account_id).get('balance', 0),
            'recent_conversions': list(self.reaction_center.conversion_history)[-10:],
            'pigment_health': {
                name: {
                    'recent_avg': np.mean([e['value'] for e in list(history)[-20:]]) if history else 0
                }
                for name, history in self.pigments.excitation_history.items()
            }
        }
