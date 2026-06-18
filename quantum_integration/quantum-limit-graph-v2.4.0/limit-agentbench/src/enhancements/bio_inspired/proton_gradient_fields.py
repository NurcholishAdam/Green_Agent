# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/proton_gradient_fields.py
# Complete enhanced file with HierarchicalGradientManager, protocol, and forecasting

"""
Enhanced Proton Gradient Fields v5.0.0
Complete implementation with hierarchical management, forecasting, and protocol support
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Protocol
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
from collections import deque
import math

logger = logging.getLogger(__name__)

# ============================================================================
# Protocol Definition
# ============================================================================

class GradientServiceProtocol(Protocol):
    """Explicit contract for gradient management services"""
    def get_field_strengths(self) -> Dict[str, float]: ...
    def pump_field(self, field_id: str, amount: float, source: str) -> None: ...
    def discharge_field(self, field_id: str, amount: float) -> float: ...
    def get_dominant_field(self) -> Tuple[str, float]: ...
    def get_field_stats(self) -> Dict[str, Any]: ...
    def find_root_cause(self, anomaly_field: str, max_depth: int) -> Dict[str, Any]: ...
    def explain_gradient_state(self, field_id: str) -> Dict[str, Any]: ...
    def forecast(self, field_id: str, horizon_seconds: float) -> Dict[str, Any]: ...
    def get_forecast_summary(self) -> Dict[str, Any]: ...

# ============================================================================
# Enhanced Gradient Field
# ============================================================================

@dataclass
class GradientField:
    field_id: str
    field_type: str
    current_value: float = 0.0
    baseline: float = 0.0
    max_value: float = 100.0
    leakage_rate: float = 0.05
    pumping_rate: float = 0.0
    last_updated: datetime = field(default_factory=datetime.utcnow)
    history: deque = field(default_factory=lambda: deque(maxlen=1000))
    use_log_scale: bool = True
    log_base: float = 10.0
    overflow_buffer: float = 0.0
    overflow_decay_rate: float = 0.01
    
    def pump_protons(self, amount: float, efficiency: float = 1.0):
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
        self.history.append({'action': 'pump', 'amount': effective_amount, 'new_value': self.current_value,
                            'overflow': self.overflow_buffer, 'timestamp': datetime.utcnow().isoformat()})
    
    def _release_overflow(self):
        if self.overflow_buffer > 0 and self.current_value < self.max_value * 0.9:
            release = min(self.overflow_buffer * self.overflow_decay_rate, (self.max_value - self.current_value) * 0.1)
            self.overflow_buffer -= release
            self.current_value += release
    
    def leak_protons(self, time_elapsed_minutes: float):
        leakage = self.current_value * (1 - math.exp(-self.leakage_rate * time_elapsed_minutes))
        self.current_value = max(0, self.current_value - leakage)
    
    def discharge(self, amount: float) -> float:
        actual_discharge = min(amount, self.current_value)
        self.current_value -= actual_discharge
        return actual_discharge
    
    @property
    def gradient_strength(self) -> float:
        return self.current_value / self.max_value if self.max_value > 0 else 0.0
    
    @property
    def effective_strength(self) -> float:
        base_strength = self.gradient_strength
        overflow_factor = min(1.0, self.overflow_buffer / self.max_value)
        return min(1.0, base_strength + overflow_factor * 0.3)
    
    def get_detailed_state(self) -> Dict[str, Any]:
        return {'current_value': self.current_value, 'max_value': self.max_value,
                'gradient_strength': self.gradient_strength, 'effective_strength': self.effective_strength,
                'overflow_buffer': self.overflow_buffer, 'is_saturated': self.current_value >= self.max_value,
                'pumping_rate': self.pumping_rate, 'leakage_rate': self.leakage_rate}

# ============================================================================
# Hierarchical Gradient Manager
# ============================================================================

class HierarchicalGradientManager:
    """Enhanced gradient manager with forecasting and hierarchical organization"""
    
    def __init__(self):
        self.fields: Dict[str, GradientField] = {
            'carbon': GradientField('carbon', 'carbon', max_value=100.0, leakage_rate=0.03),
            'helium': GradientField('helium', 'helium', max_value=100.0, leakage_rate=0.08),
            'trust': GradientField('trust', 'trust', max_value=50.0, leakage_rate=0.10),
            'opportunity': GradientField('opportunity', 'opportunity', max_value=200.0, leakage_rate=0.15),
            'eco_atp_reserve': GradientField('eco_atp_reserve', 'reserve', max_value=500.0, leakage_rate=0.02)
        }
        
        self.coupling_matrix = {('carbon', 'helium'): 0.2, ('carbon', 'opportunity'): 0.6,
                               ('trust', 'carbon'): 0.4, ('helium', 'opportunity'): 0.3}
        self.pumping_history: deque = deque(maxlen=10000)
        
        self.homeostasis_enabled = True
        self.homeostasis_target = 0.5
        self.homeostasis_strength = 0.1
        self.routing_diversity: Dict[str, float] = {}
        
        self.causal_graph: Dict[str, List[Tuple[str, float]]] = {}
        self._initialize_causal_graph()
        
        # Forecasting
        self.models: Dict[str, Dict[str, float]] = {}
        self.forecast_history: Dict[str, deque] = {}
        self.short_term_horizon = 60
        self.medium_term_horizon = 300
        self.long_term_horizon = 1800
        self.warning_threshold = 0.7
        self.critical_threshold = 0.85
        
        asyncio.create_task(self._leakage_loop())
        asyncio.create_task(self._homeostasis_loop())
        asyncio.create_task(self._forecasting_loop())
        
        logger.info("Hierarchical Gradient Manager initialized with forecasting")
    
    def _initialize_causal_graph(self):
        self.causal_graph = {
            'carbon': [('token_balance', -0.6), ('expert_activation', -0.4)],
            'token_balance': [('expert_activation', 0.7), ('trust', 0.5)],
            'trust': [('expert_activation', 0.6), ('token_balance', 0.3)],
            'opportunity': [('token_balance', 0.4), ('expert_activation', 0.3)]
        }
    
    def pump_field(self, field_id: str, amount: float, source: str = "unknown", efficiency: float = 1.0):
        if field_id not in self.fields:
            return
        self.fields[field_id].pump_protons(amount, efficiency)
        self._apply_coupling(field_id, amount)
        self.pumping_history.append({'field_id': field_id, 'amount': amount, 'source': source, 'timestamp': datetime.utcnow().isoformat()})
    
    def _apply_coupling(self, source_field: str, amount: float):
        for (a, b), strength in self.coupling_matrix.items():
            if source_field == a and b in self.fields:
                self.fields[b].pump_protons(amount * strength, 0.8)
            elif source_field == b and a in self.fields:
                self.fields[a].pump_protons(amount * strength, 0.8)
    
    def discharge_field(self, field_id: str, amount: float) -> float:
        if field_id not in self.fields:
            return 0.0
        return self.fields[field_id].discharge(amount)
    
    def get_field_strengths(self) -> Dict[str, float]:
        return {fid: f.effective_strength for fid, f in self.fields.items()}
    
    def get_field_stats(self) -> Dict[str, Any]:
        return {fid: f.get_detailed_state() for fid, f in self.fields.items()}
    
    def get_dominant_field(self) -> Tuple[str, float]:
        strengths = self.get_field_strengths()
        if not strengths:
            return 'none', 0.0
        return max(strengths.items(), key=lambda x: x[1])
    
    def get_total_potential(self) -> float:
        return sum(f.current_value for f in self.fields.values())
    
    async def _leakage_loop(self):
        while True:
            try:
                for field in self.fields.values():
                    field.leak_protons(1.0)
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Leakage error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _homeostasis_loop(self):
        while True:
            try:
                if not self.homeostasis_enabled:
                    await asyncio.sleep(30)
                    continue
                for field_id, field in self.fields.items():
                    eff = field.effective_strength
                    if eff > 0.85:
                        field.leakage_rate = min(0.5, field.leakage_rate * 3)
                        await asyncio.sleep(60)
                        field.leakage_rate = max(0.01, field.leakage_rate / 3)
                    elif eff < 0.15:
                        field.leakage_rate = max(0.01, field.leakage_rate * 0.3)
                        await asyncio.sleep(60)
                        field.leakage_rate = min(0.5, field.leakage_rate / 0.3)
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Homeostasis error: {str(e)}")
                await asyncio.sleep(60)
    
    # ========================================================================
    # Forecasting Methods
    # ========================================================================
    
    def record_measurement(self, field_id: str, value: float):
        if field_id not in self.forecast_history:
            self.forecast_history[field_id] = deque(maxlen=200)
        self.forecast_history[field_id].append({'value': value, 'timestamp': datetime.utcnow()})
        if len(self.forecast_history[field_id]) >= 10:
            self._update_model(field_id)
    
    def _update_model(self, field_id: str):
        history = list(self.forecast_history.get(field_id, []))
        if len(history) < 10:
            return
        values = [h['value'] for h in history]
        alpha, beta = 0.3, 0.1
        level, trend = values[0], 0
        for i in range(1, len(values)):
            new_level = alpha * values[i] + (1 - alpha) * (level + trend)
            new_trend = beta * (new_level - level) + (1 - beta) * trend
            level, trend = new_level, new_trend
        self.models[field_id] = {'level': level, 'trend': trend, 'last_updated': datetime.utcnow(), 'data_points': len(values)}
    
    def forecast(self, field_id: str, horizon_seconds: float) -> Dict[str, Any]:
        if field_id not in self.models:
            return {'current': 0.5, 'predicted': 0.5, 'trend': 'stable', 'confidence': 0.3}
        model = self.models[field_id]
        history = list(self.forecast_history.get(field_id, []))
        if not history:
            return {'current': 0.5, 'predicted': 0.5, 'trend': 'stable', 'confidence': 0.3}
        current = history[-1]['value']
        predicted = max(0, min(1, model['level'] + model['trend'] * horizon_seconds))
        variance = np.var([h['value'] for h in history[-20:]]) if len(history) >= 20 else 0.01
        confidence = min(1.0, len(history) / 50) * (1.0 / (1.0 + variance * 10))
        trend = 'rising' if model['trend'] > 0.001 else 'falling' if model['trend'] < -0.001 else 'stable'
        if predicted > self.critical_threshold and trend == 'rising':
            rec = f"CRITICAL: {field_id} predicted to exceed critical threshold."
        elif predicted > self.warning_threshold and trend == 'rising':
            rec = f"WARNING: {field_id} predicted to enter warning zone."
        else:
            rec = f"MONITOR: {field_id} stable."
        return {'field_id': field_id, 'current': current, 'predicted': predicted, 'horizon_seconds': horizon_seconds,
                'trend': trend, 'confidence': confidence, 'recommendation': rec}
    
    def get_forecast_summary(self) -> Dict[str, Any]:
        summary = {}
        for field_id in self.fields:
            short = self.forecast(field_id, self.short_term_horizon)
            medium = self.forecast(field_id, self.medium_term_horizon)
            summary[field_id] = {'current': short['current'], 'short_term': short['predicted'],
                                'medium_term': medium['predicted'], 'trend': short['trend'],
                                'confidence': short['confidence'], 'recommendation': short['recommendation']}
        return summary
    
    async def _forecasting_loop(self):
        while True:
            try:
                for field_id in self.fields:
                    field = self.fields[field_id]
                    self.record_measurement(field_id, field.effective_strength)
                    forecast = self.forecast(field_id, self.short_term_horizon)
                    if forecast['trend'] == 'rising' and forecast['predicted'] > self.warning_threshold:
                        field.leakage_rate = min(0.3, field.leakage_rate * 1.5)
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Forecasting error: {str(e)}")
                await asyncio.sleep(60)
    
    # ========================================================================
    # Causal Inference
    # ========================================================================
    
    def find_root_cause(self, anomaly_field: str, max_depth: int = 3) -> Dict[str, Any]:
        root_causes, visited = [], set()
        def trace(field, depth, path):
            if depth >= max_depth or field in visited: return
            visited.add(field)
            causes = [(cf, s) for cf, targets in self.causal_graph.items() for t, s in targets if t == field and s > 0.3]
            if not causes:
                root_causes.append({'field': field, 'path': path + [field], 'depth': depth,
                                   'value': self.fields[field].effective_strength if field in self.fields else 0})
            else:
                for cf, _ in causes:
                    trace(cf, depth + 1, path + [field])
        trace(anomaly_field, 0, [])
        root_causes.sort(key=lambda x: x['depth'], reverse=True)
        return {'anomaly_field': anomaly_field, 'root_causes': root_causes,
                'primary_root_cause': root_causes[0] if root_causes else None,
                'causal_chain': ' → '.join(root_causes[0]['path']) if root_causes else 'unknown'}
    
    def explain_gradient_state(self, field_id: str) -> Dict[str, Any]:
        field = self.fields.get(field_id)
        if not field:
            return {'error': f'Field {field_id} not found'}
        state = field.get_detailed_state()
        eff = state['effective_strength']
        if eff > 0.85: health = f"CRITICAL: {field_id} gradient at critically high level."
        elif eff > 0.6: health = f"WARNING: {field_id} gradient elevated."
        elif eff < 0.15: health = f"LOW: {field_id} gradient at minimal level."
        else: health = f"NORMAL: {field_id} gradient within optimal range."
        return {'field_id': field_id, 'health_assessment': health, 'current_value': state['current_value'],
                'effective_strength': eff, 'overflow_buffer': state['overflow_buffer'],
                'is_saturated': state['is_saturated']}
