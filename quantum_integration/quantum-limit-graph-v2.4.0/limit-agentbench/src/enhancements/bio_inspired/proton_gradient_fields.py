# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/proton_gradient_fields.py

"""
Proton Gradient Fields System
Version: 1.0.0

Distributed potential fields that accumulate environmental signals.
Inspired by proton gradients across biological membranes.

Biological Analogy: Electron Transport Chain
- Complex I-IV pump protons across membrane
- Creates electrochemical gradient
- Gradient drives ATP synthase
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
# Gradient Field Types
# ============================================================================

@dataclass
class GradientField:
    """A single gradient field representing accumulated potential"""
    field_id: str
    field_type: str  # carbon, helium, trust, opportunity
    current_value: float = 0.0
    baseline: float = 0.0
    max_value: float = 100.0
    leakage_rate: float = 0.05  # 5% per minute natural leakage
    pumping_rate: float = 0.0  # Current rate of gradient building
    last_updated: datetime = field(default_factory=datetime.utcnow)
    history: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    def pump_protons(self, amount: float, efficiency: float = 1.0):
        """Add to gradient (pump protons)"""
        effective_amount = amount * efficiency
        self.current_value = min(self.max_value, self.current_value + effective_amount)
        self.pumping_rate = amount
        self.last_updated = datetime.utcnow()
        
        self.history.append({
            'action': 'pump',
            'amount': effective_amount,
            'new_value': self.current_value,
            'timestamp': datetime.utcnow().isoformat()
        })
    
    def leak_protons(self, time_elapsed_minutes: float):
        """Natural leakage over time"""
        leakage = self.current_value * (1 - math.exp(-self.leakage_rate * time_elapsed_minutes))
        self.current_value = max(0, self.current_value - leakage)
        
        if leakage > 0.01:
            self.history.append({
                'action': 'leak',
                'amount': leakage,
                'new_value': self.current_value,
                'timestamp': datetime.utcnow().isoformat()
            })
    
    def discharge(self, amount: float) -> float:
        """Discharge gradient to perform work"""
        actual_discharge = min(amount, self.current_value)
        self.current_value -= actual_discharge
        
        self.history.append({
            'action': 'discharge',
            'amount': actual_discharge,
            'new_value': self.current_value,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        return actual_discharge
    
    @property
    def gradient_strength(self) -> float:
        """Normalized gradient strength (0-1)"""
        return self.current_value / self.max_value if self.max_value > 0 else 0.0
    
    @property
    def is_above_threshold(self, threshold: float = 0.1) -> bool:
        return self.gradient_strength > threshold

# ============================================================================
# Gradient Field Manager
# ============================================================================

class GradientFieldManager:
    """
    Manages multiple proton gradient fields.
    
    Coordinates pumping, leakage, and discharge across fields.
    """
    
    def __init__(self):
        # Core gradient fields
        self.fields: Dict[str, GradientField] = {
            'carbon': GradientField(
                field_id='carbon',
                field_type='carbon',
                max_value=100.0,
                leakage_rate=0.03  # Slower leakage for carbon (long-term concern)
            ),
            'helium': GradientField(
                field_id='helium',
                field_type='helium',
                max_value=100.0,
                leakage_rate=0.08  # Faster leakage for helium (volatile resource)
            ),
            'trust': GradientField(
                field_id='trust',
                field_type='trust',
                max_value=50.0,
                leakage_rate=0.10  # Fast leakage for trust (recency matters)
            ),
            'opportunity': GradientField(
                field_id='opportunity',
                field_type='opportunity',
                max_value=200.0,
                leakage_rate=0.15  # Fastest leakage (opportunities are transient)
            ),
            'eco_atp_reserve': GradientField(
                field_id='eco_atp_reserve',
                field_type='reserve',
                max_value=500.0,
                leakage_rate=0.02  # Very slow leakage (energy storage)
            )
        }
        
        # Coupling between gradients
        self.coupling_matrix = {
            ('carbon', 'helium'): 0.2,    # Weak coupling
            ('carbon', 'opportunity'): 0.6,  # Strong coupling
            ('trust', 'carbon'): 0.4,     # Moderate coupling
            ('helium', 'opportunity'): 0.3, # Weak coupling
        }
        
        # Pumping history
        self.pumping_history: deque = deque(maxlen=10000)
        
        # Start leakage loop
        asyncio.create_task(self._leakage_loop())
        
        logger.info(f"Gradient Field Manager initialized with {len(self.fields)} fields")
    
    def pump_field(
        self,
        field_id: str,
        amount: float,
        source: str = "unknown",
        efficiency: float = 1.0
    ):
        """Pump protons into a gradient field"""
        if field_id not in self.fields:
            return
        
        self.fields[field_id].pump_protons(amount, efficiency)
        
        # Apply coupling effects
        self._apply_coupling(field_id, amount)
        
        self.pumping_history.append({
            'field_id': field_id,
            'amount': amount,
            'source': source,
            'timestamp': datetime.utcnow().isoformat()
        })
    
    def _apply_coupling(self, source_field: str, amount: float):
        """Apply cross-gradient coupling effects"""
        for (field_a, field_b), coupling_strength in self.coupling_matrix.items():
            if source_field == field_a and field_b in self.fields:
                coupled_amount = amount * coupling_strength
                self.fields[field_b].pump_protons(coupled_amount, 0.8)
            elif source_field == field_b and field_a in self.fields:
                coupled_amount = amount * coupling_strength
                self.fields[field_a].pump_protons(coupled_amount, 0.8)
    
    def discharge_field(
        self,
        field_id: str,
        amount: float
    ) -> float:
        """Discharge gradient to perform work"""
        if field_id not in self.fields:
            return 0.0
        
        return self.fields[field_id].discharge(amount)
    
    async def _leakage_loop(self):
        """Background leakage loop"""
        while True:
            try:
                for field in self.fields.values():
                    field.leak_protons(1.0)  # 1 minute leakage
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Leakage loop error: {str(e)}")
                await asyncio.sleep(60)
    
    def get_field_strengths(self) -> Dict[str, float]:
        """Get current strength of all fields"""
        return {
            field_id: field.gradient_strength
            for field_id, field in self.fields.items()
        }
    
    def get_dominant_field(self) -> Tuple[str, float]:
        """Get the field with highest gradient"""
        strengths = self.get_field_strengths()
        if not strengths:
            return 'none', 0.0
        
        dominant = max(strengths.items(), key=lambda x: x[1])
        return dominant
    
    def get_total_potential(self) -> float:
        """Get total potential energy across all fields"""
        return sum(f.current_value for f in self.fields.values())
    
    def get_field_stats(self) -> Dict[str, Any]:
        """Get comprehensive field statistics"""
        return {
            field_id: {
                'current_value': field.current_value,
                'gradient_strength': field.gradient_strength,
                'pumping_rate': field.pumping_rate,
                'leakage_rate': field.leakage_rate,
                'above_threshold': field.is_above_threshold(),
                'recent_activity': list(field.history)[-5:]
            }
            for field_id, field in self.fields.items()
        }
