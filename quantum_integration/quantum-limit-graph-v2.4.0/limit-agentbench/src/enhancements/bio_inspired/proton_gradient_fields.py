# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/proton_gradient_fields.py
# Enhanced with logarithmic scaling, overflow buffers, and homeostasis

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
# Enhanced Gradient Field with Logarithmic Scaling
# ============================================================================

@dataclass
class GradientField:
    """Gradient field with logarithmic scaling and overflow buffer"""
    field_id: str
    field_type: str
    current_value: float = 0.0
    baseline: float = 0.0
    max_value: float = 100.0
    leakage_rate: float = 0.05
    pumping_rate: float = 0.0
    last_updated: datetime = field(default_factory=datetime.utcnow)
    history: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    # ========================================================================
    # FIX 2: Logarithmic Scaling with Overflow Buffer
    # ========================================================================
    use_log_scale: bool = True
    log_base: float = 10.0
    overflow_buffer: float = 0.0
    overflow_decay_rate: float = 0.01
    
    def pump_protons(self, amount: float, efficiency: float = 1.0):
        """Enhanced pumping with logarithmic scaling and overflow"""
        effective_amount = amount * efficiency
        
        if self.use_log_scale:
            if self.current_value > 0:
                log_current = math.log(self.current_value + 1, self.log_base)
                log_amount = math.log(effective_amount + 1, self.log_base)
                new_log = log_current + log_amount * 0.1
                new_value = math.pow(self.log_base, new_log) - 1
            else:
                new_value = effective_amount
            
            if new_value > self.max_value:
                self.overflow_buffer += (new_value - self.max_value)
                self.current_value = self.max_value
            else:
                self.current_value = new_value
        else:
            if self.current_value + effective_amount > self.max_value:
                self.overflow_buffer += (self.current_value + effective_amount - self.max_value)
                self.current_value = self.max_value
            else:
                self.current_value += effective_amount
        
        self.pumping_rate = amount
        self.last_updated = datetime.utcnow()
        self._release_overflow()
        
        self.history.append({
            'action': 'pump', 'amount': effective_amount,
            'new_value': self.current_value, 'overflow': self.overflow_buffer,
            'scale': 'log' if self.use_log_scale else 'linear',
            'timestamp': datetime.utcnow().isoformat()
        })
    
    def _release_overflow(self):
        """Slowly release overflow buffer back into main gradient"""
        if self.overflow_buffer > 0 and self.current_value < self.max_value * 0.9:
            release = min(
                self.overflow_buffer * self.overflow_decay_rate,
                (self.max_value - self.current_value) * 0.1
            )
            self.overflow_buffer -= release
            self.current_value += release
    
    def leak_protons(self, time_elapsed_minutes: float):
        """Natural leakage over time"""
        leakage = self.current_value * (1 - math.exp(-self.leakage_rate * time_elapsed_minutes))
        self.current_value = max(0, self.current_value - leakage)
        if leakage > 0.01:
            self.history.append({
                'action': 'leak', 'amount': leakage,
                'new_value': self.current_value,
                'timestamp': datetime.utcnow().isoformat()
            })
    
    def discharge(self, amount: float) -> float:
        """Discharge gradient to perform work"""
        actual_discharge = min(amount, self.current_value)
        self.current_value -= actual_discharge
        self.history.append({
            'action': 'discharge', 'amount': actual_discharge,
            'new_value': self.current_value,
            'timestamp': datetime.utcnow().isoformat()
        })
        return actual_discharge
    
    @property
    def gradient_strength(self) -> float:
        return self.current_value / self.max_value if self.max_value > 0 else 0.0
    
    @property
    def effective_strength(self) -> float:
        """Effective strength considering overflow buffer"""
        base_strength = self.gradient_strength
        overflow_factor = min(1.0, self.overflow_buffer / self.max_value)
        return min(1.0, base_strength + overflow_factor * 0.3)
    
    @property
    def is_above_threshold(self, threshold: float = 0.1) -> bool:
        return self.effective_strength > threshold
    
    def get_detailed_state(self) -> Dict[str, Any]:
        return {
            'current_value': self.current_value,
            'max_value': self.max_value,
            'gradient_strength': self.gradient_strength,
            'effective_strength': self.effective_strength,
            'overflow_buffer': self.overflow_buffer,
            'is_saturated': self.current_value >= self.max_value,
            'scale': 'logarithmic' if self.use_log_scale else 'linear',
            'pumping_rate': self.pumping_rate,
            'leakage_rate': self.leakage_rate
        }

# ============================================================================
# Enhanced Gradient Field Manager with Homeostasis
# ============================================================================

class GradientFieldManager:
    """
    Enhanced Gradient Field Manager with:
    - Logarithmic scaling for better signal differentiation
    - Overflow buffers for saturation prevention
    - Homeostasis mechanisms to prevent runaway feedback
    - Routing diversity enforcement
    """
    
    def __init__(self):
        self.fields: Dict[str, GradientField] = {
            'carbon': GradientField(field_id='carbon', field_type='carbon', 
                                    max_value=100.0, leakage_rate=0.03),
            'helium': GradientField(field_id='helium', field_type='helium',
                                    max_value=100.0, leakage_rate=0.08),
            'trust': GradientField(field_id='trust', field_type='trust',
                                   max_value=50.0, leakage_rate=0.10),
            'opportunity': GradientField(field_id='opportunity', field_type='opportunity',
                                        max_value=200.0, leakage_rate=0.15),
            'eco_atp_reserve': GradientField(field_id='eco_atp_reserve', field_type='reserve',
                                            max_value=500.0, leakage_rate=0.02)
        }
        
        # Coupling matrix
        self.coupling_matrix = {
            ('carbon', 'helium'): 0.2, ('carbon', 'opportunity'): 0.6,
            ('trust', 'carbon'): 0.4, ('helium', 'opportunity'): 0.3
        }
        
        self.pumping_history: deque = deque(maxlen=10000)
        
        # ====================================================================
        # FIX 3: Homeostasis Mechanisms
        # ====================================================================
        self.homeostasis_enabled = True
        self.homeostasis_target = 0.5
        self.homeostasis_strength = 0.1
        self.routing_diversity: Dict[str, float] = {}
        self.diversity_threshold = 0.3
        
        asyncio.create_task(self._leakage_loop())
        asyncio.create_task(self._homeostasis_loop())
        
        logger.info("Enhanced Gradient Field Manager initialized with all fixes")
    
    def pump_field(self, field_id: str, amount: float, source: str = "unknown", efficiency: float = 1.0):
        """Pump protons into a gradient field"""
        if field_id not in self.fields:
            return
        self.fields[field_id].pump_protons(amount, efficiency)
        self._apply_coupling(field_id, amount)
        self.pumping_history.append({
            'field_id': field_id, 'amount': amount, 'source': source,
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
    
    def discharge_field(self, field_id: str, amount: float) -> float:
        if field_id not in self.fields:
            return 0.0
        return self.fields[field_id].discharge(amount)
    
    async def _leakage_loop(self):
        """Background leakage loop"""
        while True:
            try:
                for field in self.fields.values():
                    field.leak_protons(1.0)
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Leakage loop error: {str(e)}")
                await asyncio.sleep(60)
    
    # ========================================================================
    # FIX 3: Homeostasis Loop
    # ========================================================================
    
    async def _homeostasis_loop(self):
        """Maintain gradient homeostasis to prevent runaway feedback"""
        while True:
            try:
                if not self.homeostasis_enabled:
                    await asyncio.sleep(30)
                    continue
                
                for field_id, field in self.fields.items():
                    effective_strength = field.effective_strength
                    
                    if effective_strength > 0.85:
                        original_leakage = field.leakage_rate
                        field.leakage_rate = min(0.5, original_leakage * 3)
                        logger.info(
                            f"Homeostasis: Dampening {field_id} gradient "
                            f"(strength={effective_strength:.2f}, "
                            f"leakage={original_leakage:.3f}→{field.leakage_rate:.3f})"
                        )
                        await asyncio.sleep(60)
                        field.leakage_rate = original_leakage
                    
                    elif effective_strength < 0.15:
                        original_leakage = field.leakage_rate
                        field.leakage_rate = max(0.01, original_leakage * 0.3)
                        logger.info(
                            f"Homeostasis: Stimulating {field_id} gradient "
                            f"(strength={effective_strength:.2f})"
                        )
                        await asyncio.sleep(60)
                        field.leakage_rate = original_leakage
                
                self._enforce_routing_diversity()
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Homeostasis error: {str(e)}")
                await asyncio.sleep(60)
    
    def _enforce_routing_diversity(self):
        """Ensure no single expert monopolizes routing"""
        if not self.routing_diversity:
            return
        
        total_routes = sum(self.routing_diversity.values())
        if total_routes == 0:
            return
        
        shares = {eid: count / total_routes for eid, count in self.routing_diversity.items()}
        
        if shares:
            dominant_expert = max(shares, key=shares.get)
            dominant_share = shares[dominant_expert]
            
            if dominant_share > 0.5:
                trust_field = self.fields.get('trust')
                if trust_field:
                    penalty = (dominant_share - 0.5) * 0.1
                    trust_field.current_value = max(0.1, trust_field.current_value - penalty)
                    logger.info(
                        f"Diversity enforcement: {dominant_expert} share={dominant_share:.1%}, "
                        f"trust penalty={penalty:.3f}"
                    )
    
    def record_routing_decision(self, expert_id: str):
        """Record routing for diversity tracking"""
        self.routing_diversity[expert_id] = self.routing_diversity.get(expert_id, 0) + 1
        for eid in list(self.routing_diversity.keys()):
            self.routing_diversity[eid] *= 0.99
    
    def get_field_strengths(self) -> Dict[str, float]:
        """Get effective field strengths (including overflow)"""
        return {field_id: field.effective_strength for field_id, field in self.fields.items()}
    
    def get_field_stats(self) -> Dict[str, Any]:
        """Get comprehensive field statistics"""
        return {
            field_id: field.get_detailed_state()
            for field_id, field in self.fields.items()
        }
    
    def get_total_potential(self) -> float:
        return sum(f.current_value for f in self.fields.values())
    
    def get_dominant_field(self) -> Tuple[str, float]:
        strengths = self.get_field_strengths()
        if not strengths:
            return 'none', 0.0
        dominant = max(strengths.items(), key=lambda x: x[1])
        return dominant
