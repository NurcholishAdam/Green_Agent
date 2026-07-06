# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/atp_synthase_scheduler.py
# Complete enhanced file v6.0.0 with all improvements

"""
Enhanced ATP Synthase Scheduler v6.0.0
Complete implementation with demand-responsive production, bidirectional operation,
allosteric feedback inhibition, multi-synthase scaling, degradation awareness,
predictive scheduling, uncoupling mechanism, quantum tunneling effects (NEW),
user-defined demand priorities (NEW), load balancing between synthases (NEW),
machine learning for demand prediction (NEW), and gradient forecasting (NEW).
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
from collections import deque
import math
import uuid
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

# ============================================================================
# Try importing dependencies
# ============================================================================
try:
    from .eco_atp_currency import EcoATPTokenManager, EcoATPConsumer, EcoATPSource
    TOKEN_AVAILABLE = True
except ImportError:
    TOKEN_AVAILABLE = False

try:
    from .proton_gradient_fields import GradientFieldManager, GradientField
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False

# ============================================================================
# Enums and Data Classes
# ============================================================================

class SynthaseMode(Enum):
    """ATP Synthase operational modes"""
    SYNTHESIS = "synthesis"        # Forward: protons → ATP
    HYDROLYSIS = "hydrolysis"      # Reverse: ATP → protons
    IDLE = "idle"                  # No activity
    INHIBITED = "inhibited"        # Allosterically inhibited
    UNCOUPLED = "uncoupled"        # Proton leak without ATP production
    QUANTUM_ENHANCED = "quantum_enhanced"  # NEW: Quantum tunneling mode

class SynthaseState(Enum):
    """Synthase health states"""
    ACTIVE = "active"
    DEGRADED = "degraded"
    OVERLOADED = "overloaded"
    REPAIRING = "repairing"
    DORMANT = "dormant"
    QUANTUM_READY = "quantum_ready"  # NEW

@dataclass
class SynthaseConfig:
    """ATP Synthase configuration with adaptive parameters"""
    # Core parameters
    protons_per_rotation: int = 12      # c-ring size
    atp_per_rotation: int = 3           # ATP produced per full rotation
    max_rotation_speed_rpm: float = 6000
    activation_gradient: float = 0.05
    base_efficiency: float = 0.95
    
    # Allosteric regulation
    atp_inhibition_constant: float = 0.1  # Ki for ATP binding
    atp_inhibition_max: float = 0.5       # Maximum inhibition (50% reduction)
    
    # Bidirectional operation
    reverse_efficiency: float = 0.7       # Efficiency in reverse mode
    hydrolysis_protons_per_atp: int = 4   # Protons pumped per ATP hydrolyzed
    
    # Uncoupling
    uncoupling_leak_rate: float = 0.01    # Proton leak rate when uncoupled
    uncoupling_activation_threshold: float = 0.9  # Gradient level to trigger uncoupling
    
    # Adaptive parameters
    adaptive_c_ring: bool = True          # Allow c-ring size adaptation
    min_c_ring: int = 8
    max_c_ring: int = 17
    
    # Degradation awareness
    degradation_scaling: bool = True
    
    # NEW: Quantum tunneling
    quantum_tunneling_enabled: bool = True
    quantum_efficiency_boost: float = 0.25
    quantum_tunneling_threshold: float = 0.7  # Gradient threshold for quantum effects
    quantum_coherence_time: float = 10.0  # Seconds

@dataclass
class ScheduledTask:
    """Task scheduled for execution with enhanced metadata"""
    task_id: str
    eco_atp_required: float
    priority: int
    deadline: Optional[datetime] = None
    callback: Optional[Callable] = None
    compartment_preference: Optional[str] = None
    scheduled_at: datetime = field(default_factory=datetime.utcnow)
    token_ids: List[str] = field(default_factory=list)
    status: str = "pending"
    # NEW: User-defined priority
    user_priority: Optional[str] = None  # critical, high, normal, low, background

@dataclass
class ProductionRecord:
    """Record of ATP production for analysis"""
    timestamp: datetime
    mode: str
    driving_force: float
    rotation_speed: float
    atp_produced: float
    efficiency: float
    demand_level: float
    inhibition_level: float
    degradation_tier: int
    # NEW: Quantum metrics
    quantum_enhancement: float = 0.0
    quantum_efficiency: float = 0.0

@dataclass
class DemandPriority:
    """User-defined demand priority configuration"""
    priority_level: str
    weight: float
    min_balance: float
    max_consumption: float
    
# ============================================================================
# Enhanced ATP Synthase with Quantum Tunneling (NEW)
# ============================================================================

class EnhancedATPSynthase:
    """
    Individual ATP Synthase complex with full regulatory mechanisms and quantum tunneling.
    
    Supports:
    - Forward synthesis (protons → ATP)
    - Reverse hydrolysis (ATP → protons)
    - Allosteric inhibition by ATP
    - Uncoupling for gradient regulation
    - Adaptive c-ring sizing
    - Health tracking and degradation
    - Quantum tunneling for enhanced efficiency (NEW)
    """
    
    def __init__(self, synthase_id: str, config: Optional[SynthaseConfig] = None):
        self.synthase_id = synthase_id
        self.config = config or SynthaseConfig()
        
        # Operational state
        self.mode = SynthaseMode.IDLE
        self.state = SynthaseState.ACTIVE
        self.rotation_speed = 0.0
        self.current_efficiency = self.config.base_efficiency
        
        # Production tracking
        self.total_atp_produced = 0.0
        self.total_atp_hydrolyzed = 0.0
        self.production_history: deque = deque(maxlen=1000)
        
        # Allosteric regulation
        self.inhibition_level = 0.0
        
        # Health tracking
        self.operational_hours = 0.0
        self.degradation_rate = 0.0001
        self.repair_rate = 0.01
        
        # NEW: Quantum tunneling
        self.quantum_coherence = 1.0
        self.quantum_enhancement_factor = 0.0
        self.quantum_active = False
        
        # Adaptive parameters
        if self.config.adaptive_c_ring:
            self._adapt_c_ring()
        
        logger.info(f"ATP Synthase '{synthase_id}' initialized: c-ring={self.config.protons_per_rotation}, quantum={self.config.quantum_tunneling_enabled}")
    
    def _adapt_c_ring(self):
        """Adapt c-ring size based on expected operating conditions"""
        self.config.protons_per_rotation = 12
    
    # ========================================================================
    # Quantum Tunneling Methods (NEW)
    # ========================================================================
    
    def calculate_quantum_enhancement(self, driving_force: float) -> float:
        """Calculate quantum tunneling enhancement factor"""
        if not self.config.quantum_tunneling_enabled:
            return 0.0
        
        if driving_force < self.config.quantum_tunneling_threshold:
            return 0.0
        
        # Quantum tunneling probability increases with gradient
        # and is modulated by coherence time
        base_probability = (driving_force - self.config.quantum_tunneling_threshold) / 0.3
        coherence_factor = min(1.0, self.quantum_coherence / self.config.quantum_coherence_time)
        
        quantum_factor = base_probability * coherence_factor * self.config.quantum_efficiency_boost
        
        # Update state
        if quantum_factor > 0.1:
            self.quantum_active = True
            self.state = SynthaseState.QUANTUM_READY
            self.mode = SynthaseMode.QUANTUM_ENHANCED
        else:
            self.quantum_active = False
        
        return min(0.5, quantum_factor)
    
    def calculate_driving_force(self, gradient_manager) -> float:
        """Calculate proton motive force with quantum enhancement"""
        if not gradient_manager:
            return 0.0
        
        field_strengths = gradient_manager.get_field_strengths()
        
        weights = {
            'carbon': 0.25,
            'helium': 0.15,
            'trust': 0.20,
            'opportunity': 0.25,
            'eco_atp_reserve': 0.15
        }
        
        driving_force = sum(field_strengths.get(field, 0) * weight for field, weight in weights.items())
        
        # Apply quantum enhancement
        quantum_boost = self.calculate_quantum_enhancement(driving_force)
        if quantum_boost > 0:
            driving_force *= (1.0 + quantum_boost)
            self.quantum_enhancement_factor = quantum_boost
        
        return driving_force
    
    def calculate_rotation_speed(self, driving_force: float) -> float:
        """Calculate rotation speed with quantum-enhanced kinetics"""
        if self.mode == SynthaseMode.IDLE or self.mode == SynthaseMode.INHIBITED:
            return 0.0
        
        if driving_force < self.config.activation_gradient:
            return 0.0
        
        vmax = self.config.max_rotation_speed_rpm * self.current_efficiency
        km = 0.3
        
        speed = vmax * driving_force / (km + driving_force)
        
        # Apply quantum enhancement to rotation speed
        if self.quantum_active and self.quantum_enhancement_factor > 0:
            speed *= (1.0 + self.quantum_enhancement_factor * 0.5)
        
        # Apply allosteric inhibition
        if self.inhibition_level > 0:
            speed *= (1.0 - self.inhibition_level)
        
        return speed
    
    def calculate_atp_production_rate(self, rotation_speed: float) -> float:
        """Calculate ATP production rate with quantum efficiency"""
        if rotation_speed <= 0:
            return 0.0
        
        rps = rotation_speed / 60.0
        atp_per_second = rps * self.config.atp_per_rotation
        effective_atp = atp_per_second * self.current_efficiency
        eco_atp_per_second = effective_atp * 10.0
        
        # Apply quantum efficiency boost
        if self.quantum_active and self.quantum_enhancement_factor > 0:
            eco_atp_per_second *= (1.0 + self.quantum_enhancement_factor * 0.3)
        
        return eco_atp_per_second
    
    def calculate_proton_consumption(self, atp_produced: float) -> float:
        """Calculate protons consumed with quantum efficiency"""
        base_consumption = atp_produced * (self.config.protons_per_rotation / self.config.atp_per_rotation) / 10.0
        
        # Reduce proton consumption when quantum active
        if self.quantum_active and self.quantum_enhancement_factor > 0:
            base_consumption *= (1.0 - self.quantum_enhancement_factor * 0.2)
        
        return base_consumption
    
    def update_allosteric_inhibition(self, token_balance: float, token_capacity: float = 50000.0):
        """Update allosteric inhibition with quantum awareness"""
        if token_capacity == 0:
            self.inhibition_level = 0.0
            return
        
        atp_ratio = token_balance / token_capacity
        
        # Quantum reduces inhibition sensitivity
        inhibition_reduction = 1.0
        if self.quantum_active:
            inhibition_reduction = 1.0 - self.quantum_enhancement_factor * 0.3
        
        if atp_ratio > 0.8:
            self.inhibition_level = min(
                self.config.atp_inhibition_max,
                (atp_ratio - 0.8) / 0.2 * self.config.atp_inhibition_max * inhibition_reduction
            )
            if self.mode == SynthaseMode.SYNTHESIS and self.inhibition_level > 0.4:
                self.mode = SynthaseMode.INHIBITED
        elif atp_ratio < 0.3:
            self.inhibition_level = max(0.0, self.inhibition_level - 0.1)
            if self.mode == SynthaseMode.INHIBITED and self.inhibition_level < 0.2:
                self.mode = SynthaseMode.SYNTHESIS
        else:
            target_inhibition = (atp_ratio - 0.3) / 0.5 * self.config.atp_inhibition_max * inhibition_reduction
            self.inhibition_level += (target_inhibition - self.inhibition_level) * 0.1
    
    def set_mode(self, mode: SynthaseMode):
        """Set operational mode"""
        old_mode = self.mode
        self.mode = mode
        if old_mode != mode:
            logger.debug(f"Synthase {self.synthase_id}: {old_mode.value} → {mode.value}")
    
    def operate_forward(self, gradient_manager, token_manager, account_id: str) -> float:
        """Forward operation: protons → ATP with quantum enhancement"""
        self.set_mode(SynthaseMode.SYNTHESIS)
        
        driving_force = self.calculate_driving_force(gradient_manager)
        self.rotation_speed = self.calculate_rotation_speed(driving_force)
        
        if self.rotation_speed <= 0:
            self.set_mode(SynthaseMode.IDLE)
            return 0.0
        
        eco_atp_rate = self.calculate_atp_production_rate(self.rotation_speed)
        eco_atp_produced = eco_atp_rate
        
        protons_consumed = self.calculate_proton_consumption(eco_atp_produced)
        if gradient_manager:
            field_strengths = gradient_manager.get_field_strengths()
            total_strength = sum(field_strengths.values())
            if total_strength > 0:
                for field_id, strength in field_strengths.items():
                    proportion = strength / total_strength
                    gradient_manager.discharge_field(field_id, protons_consumed * proportion)
        
        if token_manager:
            tokens = token_manager.generate_tokens(
                account_id=account_id,
                source=EcoATPSource.GRADIENT_CONVERSION,
                energy_saved_kwh=eco_atp_produced / 10000.0,
                efficiency=self.current_efficiency
            )
            if tokens:
                self.total_atp_produced += sum(t.value for t in tokens)
        
        self.operational_hours += 1.0 / 3600.0
        if self.operational_hours > 100:
            self.current_efficiency = max(0.5, self.config.base_efficiency - 
                                         self.degradation_rate * self.operational_hours)
        
        self.production_history.append(ProductionRecord(
            timestamp=datetime.utcnow(), mode='synthesis', driving_force=driving_force,
            rotation_speed=self.rotation_speed, atp_produced=eco_atp_produced,
            efficiency=self.current_efficiency, demand_level=0.5,
            inhibition_level=self.inhibition_level, degradation_tier=5,
            quantum_enhancement=self.quantum_enhancement_factor,
            quantum_efficiency=self.current_efficiency * (1.0 + self.quantum_enhancement_factor * 0.3)
        ))
        
        if self.quantum_active:
            logger.debug(f"Synthase {self.synthase_id}: quantum-enhanced production: {eco_atp_produced:.2f} ATP")
        
        return eco_atp_produced
    
    def operate_reverse(self, gradient_manager, token_manager, account_id: str, amount: float) -> float:
        """Reverse operation: ATP → protons with quantum efficiency"""
        self.set_mode(SynthaseMode.HYDROLYSIS)
        
        if token_manager:
            success, token_ids = token_manager.reserve_tokens(
                account_id=account_id, amount=amount, consumer=EcoATPConsumer.MAINTENANCE
            )
            if success:
                token_manager.consume_tokens(token_ids, EcoATPConsumer.MAINTENANCE)
                self.total_atp_hydrolyzed += amount
        
        atp_units = amount / 10.0
        protons_pumped = atp_units * self.config.hydrolysis_protons_per_atp * self.config.reverse_efficiency
        
        # Quantum enhancement for reverse operation
        if self.quantum_active and self.quantum_enhancement_factor > 0:
            protons_pumped *= (1.0 + self.quantum_enhancement_factor * 0.1)
        
        if gradient_manager:
            gradient_manager.pump_field('carbon', protons_pumped * 0.3, source=f'synthase_{self.synthase_id}_reverse')
            gradient_manager.pump_field('eco_atp_reserve', protons_pumped * 0.7, source=f'synthase_{self.synthase_id}_reverse')
        
        self.production_history.append(ProductionRecord(
            timestamp=datetime.utcnow(), mode='hydrolysis', driving_force=0,
            rotation_speed=-self.rotation_speed, atp_produced=-amount,
            efficiency=self.config.reverse_efficiency, demand_level=0,
            inhibition_level=0, degradation_tier=5,
            quantum_enhancement=self.quantum_enhancement_factor,
            quantum_efficiency=self.config.reverse_efficiency * (1.0 + self.quantum_enhancement_factor * 0.1)
        ))
        
        logger.info(f"Synthase {self.synthase_id}: hydrolyzed {amount:.1f} Eco-ATP, pumped {protons_pumped:.1f} protons")
        return protons_pumped
    
    def operate_uncoupled(self, gradient_manager) -> float:
        """Uncoupling operation: proton leak without ATP production"""
        self.set_mode(SynthaseMode.UNCOUPLED)
        
        if not gradient_manager:
            return 0.0
        
        field_strengths = gradient_manager.get_field_strengths()
        total_discharged = 0.0
        
        for field_id, strength in field_strengths.items():
            if strength > self.config.uncoupling_activation_threshold:
                discharge_amount = strength * self.config.uncoupling_leak_rate
                gradient_manager.discharge_field(field_id, discharge_amount)
                total_discharged += discharge_amount
        
        if total_discharged > 0:
            logger.info(f"Synthase {self.synthase_id}: uncoupled {total_discharged:.2f} gradient units")
        
        return total_discharged
    
    def repair(self):
        """Repair degradation and restore efficiency"""
        self.state = SynthaseState.REPAIRING
        self.current_efficiency = min(
            self.config.base_efficiency,
            self.current_efficiency + self.repair_rate
        )
        
        if self.current_efficiency >= self.config.base_efficiency * 0.95:
            self.state = SynthaseState.ACTIVE
            self.current_efficiency = self.config.base_efficiency
            logger.info(f"Synthase {self.synthase_id}: repair complete")
    
    def get_status(self) -> Dict[str, Any]:
        """Get synthase status with quantum metrics"""
        return {
            'synthase_id': self.synthase_id,
            'mode': self.mode.value,
            'state': self.state.value,
            'rotation_speed': self.rotation_speed,
            'efficiency': self.current_efficiency,
            'inhibition_level': self.inhibition_level,
            'total_atp_produced': self.total_atp_produced,
            'total_atp_hydrolyzed': self.total_atp_hydrolyzed,
            'operational_hours': self.operational_hours,
            'c_ring_size': self.config.protons_per_rotation,
            'quantum_active': self.quantum_active,
            'quantum_enhancement_factor': self.quantum_enhancement_factor,
            'quantum_coherence': self.quantum_coherence
        }

# ============================================================================
# Demand Priority Manager (NEW)
# ============================================================================

class DemandPriorityManager:
    """
    User-defined demand priority management.
    
    Features:
    - Priority-based demand scaling
    - Configurable priority levels
    - Dynamic priority adjustment
    """
    
    def __init__(self):
        self.priorities: Dict[str, DemandPriority] = {
            'critical': DemandPriority('critical', 2.0, 10000, 0.9),
            'high': DemandPriority('high', 1.5, 5000, 0.7),
            'normal': DemandPriority('normal', 1.0, 2000, 0.5),
            'low': DemandPriority('low', 0.7, 1000, 0.3),
            'background': DemandPriority('background', 0.4, 500, 0.1)
        }
        self.default_priority = 'normal'
        
        logger.info("Demand Priority Manager initialized")
    
    def set_priority_config(self, priority_level: str, weight: float, 
                           min_balance: float, max_consumption: float):
        """Set configuration for a priority level"""
        if priority_level not in self.priorities:
            self.priorities[priority_level] = DemandPriority(
                priority_level, weight, min_balance, max_consumption
            )
        else:
            self.priorities[priority_level].weight = weight
            self.priorities[priority_level].min_balance = min_balance
            self.priorities[priority_level].max_consumption = max_consumption
        
        logger.info(f"Priority '{priority_level}' configured: weight={weight}, min_balance={min_balance}")
    
    def get_priority_weight(self, priority_level: str) -> float:
        """Get weight for a priority level"""
        return self.priorities.get(priority_level, self.priorities[self.default_priority]).weight
    
    def get_task_priority(self, task: ScheduledTask) -> float:
        """Calculate effective priority for a task"""
        base_weight = self.get_priority_weight(task.user_priority or self.default_priority)
        
        # Adjust for deadline urgency
        if task.deadline:
            time_remaining = (task.deadline - datetime.utcnow()).total_seconds()
            if time_remaining < 300:  # 5 minutes
                base_weight *= 1.5
            elif time_remaining < 3600:  # 1 hour
                base_weight *= 1.2
        
        return base_weight * (task.priority + 1)

# ============================================================================
# Load Balancer for Multi-Synthase (NEW)
# ============================================================================

class SynthaseLoadBalancer:
    """
    Load balancing between multiple synthases.
    
    Features:
    - Dynamic load distribution
    - Health-aware routing
    - Efficiency optimization
    """
    
    def __init__(self):
        self.historical_loads: Dict[str, List[float]] = {}
        self.efficiency_scores: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        
        logger.info("Synthase Load Balancer initialized")
    
    async def assign_load(self, synthases: Dict[str, EnhancedATPSynthase], 
                         total_demand: float) -> Dict[str, float]:
        """Assign load across synthases based on health and efficiency"""
        async with self._lock:
            if not synthases:
                return {}
            
            # Calculate score for each synthase
            scores = {}
            total_score = 0.0
            
            for sid, synthase in synthases.items():
                # Health score: higher is better
                health_score = 1.0
                if synthase.state == SynthaseState.ACTIVE:
                    health_score = 1.0
                elif synthase.state == SynthaseState.QUANTUM_READY:
                    health_score = 1.2  # Quantum-ready synthases get bonus
                elif synthase.state == SynthaseState.DEGRADED:
                    health_score = 0.6
                elif synthase.state == SynthaseState.REPAIRING:
                    health_score = 0.3
                else:
                    health_score = 0.5
                
                # Efficiency score
                efficiency_score = synthase.current_efficiency
                
                # Quantum bonus
                quantum_bonus = 1.0 + synthase.quantum_enhancement_factor * 0.5
                
                # Composite score
                score = health_score * efficiency_score * quantum_bonus
                
                # Track historical load
                if sid not in self.historical_loads:
                    self.historical_loads[sid] = []
                self.historical_loads[sid].append(score)
                if len(self.historical_loads[sid]) > 100:
                    self.historical_loads[sid] = self.historical_loads[sid][-100:]
                
                scores[sid] = score
                total_score += score
            
            if total_score == 0:
                return {sid: total_demand / len(synthases) for sid in synthases}
            
            # Assign load proportionally to scores
            assignments = {}
            for sid, score in scores.items():
                assignments[sid] = (score / total_score) * total_demand
            
            return assignments
    
    def update_efficiency_score(self, synthase_id: str, efficiency: float):
        """Update efficiency score for a synthase"""
        self.efficiency_scores[synthase_id] = efficiency
    
    def get_load_balance_stats(self) -> Dict:
        """Get load balancing statistics"""
        return {
            'synthases_tracked': len(self.historical_loads),
            'average_loads': {
                sid: np.mean(loads) if loads else 0
                for sid, loads in self.historical_loads.items()
            },
            'efficiency_scores': self.efficiency_scores
        }

# ============================================================================
# ML-Based Demand Predictor (NEW)
# ============================================================================

class MLDemandPredictor:
    """
    Machine learning for demand prediction.
    
    Features:
    - Linear regression for demand forecasting
    - Feature engineering
    - Confidence scoring
    """
    
    def __init__(self, lookback: int = 50):
        self.model = LinearRegression()
        self.scaler = StandardScaler()
        self.lookback = lookback
        self.is_trained = False
        self.training_data: List[float] = []
        self._lock = asyncio.Lock()
        
        logger.info("ML Demand Predictor initialized")
    
    async def train(self, demand_history: List[float]):
        """Train the demand prediction model"""
        if len(demand_history) < self.lookback + 10:
            return {'status': 'insufficient_data'}
        
        async with self._lock:
            # Prepare features: previous N demand values
            X = []
            y = []
            
            for i in range(self.lookback, len(demand_history) - 1):
                features = demand_history[i - self.lookback:i]
                X.append(features)
                y.append(demand_history[i + 1])
            
            X = np.array(X)
            y = np.array(y)
            
            if len(X) < 10:
                return {'status': 'insufficient_samples'}
            
            # Scale features
            X_scaled = self.scaler.fit_transform(X)
            
            # Train model
            self.model.fit(X_scaled, y)
            self.is_trained = True
            self.training_data = demand_history
            
            logger.info(f"ML Demand Predictor trained on {len(X)} samples")
            return {'status': 'success', 'samples': len(X)}
    
    async def predict(self, recent_demand: List[float]) -> Dict:
        """Predict future demand"""
        if not self.is_trained or len(recent_demand) < self.lookback:
            return {'prediction': None, 'confidence': 0.0}
        
        async with self._lock:
            features = recent_demand[-self.lookback:]
            features_scaled = self.scaler.transform([features])
            
            prediction = self.model.predict(features_scaled)[0]
            
            # Confidence based on recent volatility
            volatility = np.std(recent_demand[-20:]) if len(recent_demand) >= 20 else 0.2
            confidence = max(0.1, 1.0 - volatility)
            
            return {
                'prediction': max(0.0, min(1.0, prediction)),
                'confidence': confidence,
                'timestamp': datetime.utcnow().isoformat()
            }
    
    def get_model_stats(self) -> Dict:
        """Get model statistics"""
        return {
            'is_trained': self.is_trained,
            'training_samples': len(self.training_data),
            'lookback': self.lookback,
            'coefficients': self.model.coef_.tolist() if self.is_trained else None,
            'intercept': self.model.intercept_ if self.is_trained else None
        }

# ============================================================================
# Gradient Forecaster (NEW)
# ============================================================================

class GradientForecaster:
    """
    Gradient forecasting for proactive production optimization.
    
    Features:
    - Gradient trend analysis
    - Future gradient prediction
    - Proactive production adjustment
    """
    
    def __init__(self, history_window: int = 50):
        self.gradient_history: Dict[str, List[float]] = {}
        self.forecast_results: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self.history_window = history_window
        
        logger.info("Gradient Forecaster initialized")
    
    def record_gradient(self, field_id: str, value: float):
        """Record gradient value for forecasting"""
        if field_id not in self.gradient_history:
            self.gradient_history[field_id] = []
        
        self.gradient_history[field_id].append(value)
        if len(self.gradient_history[field_id]) > self.history_window * 2:
            self.gradient_history[field_id] = self.gradient_history[field_id][-self.history_window*2:]
    
    async def forecast(self, field_id: str, horizon_steps: int = 10) -> Dict:
        """Forecast gradient values for a field"""
        if field_id not in self.gradient_history or len(self.gradient_history[field_id]) < 20:
            return {'status': 'insufficient_data'}
        
        async with self._lock:
            history = self.gradient_history[field_id][-self.history_window:]
            
            # Simple trend-based forecast
            x = np.arange(len(history))
            y = np.array(history)
            slope = np.polyfit(x, y, 1)[0]
            intercept = np.polyfit(x, y, 1)[1]
            
            # Generate forecast
            forecast_values = []
            for i in range(horizon_steps):
                next_value = slope * (len(history) + i) + intercept
                forecast_values.append(max(0.0, min(1.0, next_value)))
            
            # Confidence based on data stability
            volatility = np.std(history[-20:]) if len(history) >= 20 else 0.2
            confidence = max(0.1, 1.0 - volatility * 2)
            
            result = {
                'field': field_id,
                'current': history[-1] if history else 0.5,
                'forecast': forecast_values,
                'trend': 'increasing' if slope > 0.01 else 'decreasing' if slope < -0.01 else 'stable',
                'slope': slope,
                'confidence': confidence,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            self.forecast_results[field_id] = result
            return result
    
    def get_forecast_summary(self) -> Dict:
        """Get summary of all gradient forecasts"""
        return {
            'fields_forecasted': list(self.forecast_results.keys()),
            'recent_forecasts': {
                fid: {
                    'current': f['current'],
                    'forecast': f['forecast'][:5] if f.get('forecast') else [],
                    'trend': f.get('trend', 'stable'),
                    'confidence': f.get('confidence', 0.5)
                }
                for fid, f in self.forecast_results.items()
            },
            'timestamp': datetime.utcnow().isoformat()
        }

# ============================================================================
# Enhanced ATP Synthase Scheduler
# ============================================================================

class ATPSynthaseScheduler:
    """
    Enhanced ATP Synthase Scheduler v6.0.0
    
    Complete implementation with:
    - Demand-responsive production
    - Bidirectional operation (synthesis + hydrolysis)
    - Allosteric feedback inhibition
    - Multi-synthase scaling
    - Degradation-aware production
    - Predictive scheduling
    - Uncoupling mechanism
    - Harvester feedback loop
    - Quantum tunneling effects (NEW)
    - User-defined demand priorities (NEW)
    - Load balancing between synthases (NEW)
    - Machine learning for demand prediction (NEW)
    - Gradient forecasting (NEW)
    """
    
    def __init__(
        self, token_manager=None, gradient_manager=None,
        config: Optional[SynthaseConfig] = None,
        enable_multi_synthase: bool = True,
        enable_quantum: bool = True,
        enable_ml_prediction: bool = True
    ):
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        self.config = config or SynthaseConfig()
        self.enable_multi_synthase = enable_multi_synthase
        self.enable_quantum = enable_quantum
        self.enable_ml_prediction = enable_ml_prediction
        
        # Primary synthase with quantum support
        self.primary_synthase = EnhancedATPSynthase("primary", self.config)
        
        # Child synthases
        self.synthases: Dict[str, EnhancedATPSynthase] = {
            "primary": self.primary_synthase
        }
        
        # NEW: Priority manager
        self.priority_manager = DemandPriorityManager()
        
        # NEW: Load balancer
        self.load_balancer = SynthaseLoadBalancer()
        
        # NEW: ML predictor
        self.ml_predictor = MLDemandPredictor() if enable_ml_prediction else None
        
        # NEW: Gradient forecaster
        self.gradient_forecaster = GradientForecaster()
        
        # Scheduling queues
        self.execution_queue: List[ScheduledTask] = []
        self.priority_queue: List[ScheduledTask] = []
        
        # Production tracking
        self.total_eco_atp_produced = 0.0
        self.generation_history: deque = deque(maxlen=1000)
        
        # Demand tracking
        self.demand_history: deque = deque(maxlen=500)
        self.predicted_demand = 0.0
        
        # Degradation tier
        self.current_tier = 5
        
        # Account for scheduler
        self.account_id = "atp_synthase"
        if token_manager:
            token_manager.create_account(self.account_id)
        
        # Harvester reference (injected)
        self.harvester = None
        
        # Start operational loops
        asyncio.create_task(self._synthesis_loop())
        asyncio.create_task(self._regulation_loop())
        asyncio.create_task(self._maintenance_loop())
        asyncio.create_task(self._predictive_loop())
        asyncio.create_task(self._gradient_forecast_loop())
        
        logger.info(
            f"ATP Synthase Scheduler v6.0.0 initialized: "
            f"multi_synthase={enable_multi_synthase}, "
            f"quantum={enable_quantum}, "
            f"ml_prediction={enable_ml_prediction}, "
            f"c-ring={self.config.protons_per_rotation}"
        )
    
    # ========================================================================
    # Harvester Injection
    # ========================================================================
    
    def inject_harvester(self, harvester):
        """Inject photosynthetic harvester for demand signaling"""
        self.harvester = harvester
        logger.info("Photosynthetic harvester injected into ATP Synthase Scheduler")
    
    # ========================================================================
    # Core Operations
    # ========================================================================
    
    def calculate_gradient_driving_force(self) -> float:
        """Calculate overall gradient driving force"""
        return self.primary_synthase.calculate_driving_force(self.gradient_manager)
    
    def calculate_rotation_speed(self, driving_force: float) -> float:
        """Calculate rotation speed for primary synthase"""
        return self.primary_synthase.calculate_rotation_speed(driving_force)
    
    def calculate_atp_production_rate(self, rotation_speed: float) -> float:
        """Calculate ATP production rate"""
        return self.primary_synthase.calculate_atp_production_rate(rotation_speed)
    
    # ========================================================================
    # Demand-Responsive Production (Enhanced)
    # ========================================================================
    
    def _calculate_demand_level(self) -> float:
        """Calculate current demand level with priority weighting"""
        if not self.token_manager:
            return 0.5
        
        summary = self.token_manager.get_system_summary()
        balance = summary.get('total_balance', 10000)
        consumption_rate = summary.get('total_consumed', 0)
        generation_rate = summary.get('total_generated', 0)
        
        # Queue demand
        queue_demand = min(1.0, len(self.execution_queue) / 50.0)
        
        # Priority-weighted demand
        if self.execution_queue:
            priority_weights = [
                self.priority_manager.get_task_priority(task)
                for task in self.execution_queue[:10]
            ]
            priority_demand = np.mean(priority_weights) if priority_weights else 0.5
        else:
            priority_demand = 0.5
        
        # Ratio demand
        if generation_rate > 0:
            ratio_demand = consumption_rate / generation_rate
        else:
            ratio_demand = 1.0
        
        # Balance demand
        if balance < 5000:
            balance_demand = 1.0
        elif balance < 20000:
            balance_demand = 0.5 + (20000 - balance) / 30000
        else:
            balance_demand = max(0.1, 1.0 - (balance - 20000) / 30000)
        
        # Combined demand
        demand = (
            queue_demand * 0.2 +
            priority_demand * 0.2 +
            ratio_demand * 0.3 +
            balance_demand * 0.3
        )
        
        self.demand_history.append(demand)
        return min(1.0, max(0.1, demand))
    
    def _modulate_production(self, base_rate: float) -> float:
        """Modulate production rate based on demand and priorities"""
        demand = self._calculate_demand_level()
        
        # Tier-based scaling
        tier_scaling = {
            5: 1.0, 4: 0.75, 3: 0.5, 2: 0.25, 1: 0.1
        }
        tier_factor = tier_scaling.get(self.current_tier, 1.0)
        
        # Demand-based modulation
        if demand > 0.7:
            demand_factor = 1.0 + (demand - 0.7) * 1.5
        elif demand < 0.3:
            demand_factor = 0.5 + demand
        else:
            demand_factor = 1.0
        
        # Quantum boost for production
        quantum_factor = 1.0
        if self.enable_quantum and self.primary_synthase.quantum_active:
            quantum_factor = 1.0 + self.primary_synthase.quantum_enhancement_factor * 0.3
        
        return base_rate * demand_factor * tier_factor * quantum_factor
    
    # ========================================================================
    # Multi-Synthase Management (Enhanced with Load Balancing)
    # ========================================================================
    
    def spawn_synthase(self, c_ring_size: Optional[int] = None) -> str:
        """Spawn a new ATP synthase for scaling"""
        if not self.enable_multi_synthase:
            return "primary"
        
        config = SynthaseConfig()
        if c_ring_size:
            config.protons_per_rotation = c_ring_size
        config.quantum_tunneling_enabled = self.enable_quantum
        
        synthase_id = f"synthase_{len(self.synthases)}"
        synthase = EnhancedATPSynthase(synthase_id, config)
        self.synthases[synthase_id] = synthase
        
        logger.info(f"Spawned ATP synthase '{synthase_id}' (c-ring={config.protons_per_rotation}, quantum={self.enable_quantum})")
        return synthase_id
    
    def remove_synthase(self, synthase_id: str) -> bool:
        """Remove a synthase (cannot remove primary)"""
        if synthase_id == "primary" or synthase_id not in self.synthases:
            return False
        
        del self.synthases[synthase_id]
        logger.info(f"Removed ATP synthase '{synthase_id}'")
        return True
    
    # ========================================================================
    # Main Synthesis Loop (Enhanced)
    # ========================================================================
    
    async def _synthesis_loop(self):
        """Continuous ATP synthesis with demand modulation and load balancing"""
        while True:
            try:
                total_produced = 0.0
                demand = self._calculate_demand_level()
                
                # Get load assignments from load balancer
                load_assignments = await self.load_balancer.assign_load(
                    self.synthases, demand
                )
                
                for synthase_id, synthase in self.synthases.items():
                    if synthase.state not in [SynthaseState.ACTIVE, SynthaseState.QUANTUM_READY]:
                        continue
                    
                    # Get assigned load for this synthase
                    assigned_load = load_assignments.get(synthase_id, demand / len(self.synthases))
                    
                    # Update allosteric inhibition with quantum awareness
                    if self.token_manager:
                        summary = self.token_manager.get_system_summary()
                        balance = summary.get('total_balance', 10000)
                        synthase.update_allosteric_inhibition(balance)
                    
                    # Check for reverse operation
                    if self._should_reverse_operate():
                        protons_pumped = synthase.operate_reverse(
                            self.gradient_manager, self.token_manager,
                            self.account_id, amount=50.0 * assigned_load
                        )
                        continue
                    
                    # Check for uncoupling
                    if self._should_uncouple():
                        synthase.operate_uncoupled(self.gradient_manager)
                        continue
                    
                    # Normal forward operation
                    driving_force = synthase.calculate_driving_force(self.gradient_manager)
                    rotation_speed = synthase.calculate_rotation_speed(driving_force)
                    
                    if rotation_speed > 0:
                        base_rate = synthase.calculate_atp_production_rate(rotation_speed)
                        
                        # Apply demand modulation with assigned load
                        if synthase_id == "primary":
                            eco_atp_rate = self._modulate_production(base_rate) * assigned_load
                        else:
                            eco_atp_rate = base_rate * assigned_load
                        
                        if eco_atp_rate > 0.1:
                            eco_atp_produced = synthase.operate_forward(
                                self.gradient_manager, self.token_manager, self.account_id
                            )
                            total_produced += eco_atp_produced * assigned_load
                
                if total_produced > 0:
                    self.total_eco_atp_produced += total_produced
                    
                    # Signal harvester if demand is high
                    if self.harvester and demand > 0.7:
                        pass
                
                # Record gradient for forecasting
                if self.gradient_manager:
                    for field_id, strength in self.gradient_manager.get_field_strengths().items():
                        self.gradient_forecaster.record_gradient(field_id, strength)
                
                interval = 0.1 if total_produced > 0 else 1.0
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Synthesis loop error: {str(e)}")
                await asyncio.sleep(5)
    
    def _should_reverse_operate(self) -> bool:
        """Determine if synthase should operate in reverse"""
        if not self.token_manager:
            return False
        
        summary = self.token_manager.get_system_summary()
        balance = summary.get('total_balance', 10000)
        
        if balance > 40000 and self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.effective_strength < 0.3:
                return True
        
        return False
    
    def _should_uncouple(self) -> bool:
        """Determine if synthase should uncouple"""
        if not self.gradient_manager:
            return False
        
        for field in self.gradient_manager.fields.values():
            if field.effective_strength > self.config.uncoupling_activation_threshold:
                return True
        
        return False
    
    # ========================================================================
    # Regulation Loop (Enhanced)
    # ========================================================================
    
    async def _regulation_loop(self):
        """Regulatory loop for allosteric control and scaling"""
        while True:
            try:
                # Update inhibition for all synthases
                if self.token_manager:
                    summary = self.token_manager.get_system_summary()
                    balance = summary.get('total_balance', 10000)
                    for synthase in self.synthases.values():
                        synthase.update_allosteric_inhibition(balance)
                
                # Scale synthases based on demand and quantum readiness
                demand = self._calculate_demand_level()
                active_count = sum(1 for s in self.synthases.values() if s.state in [SynthaseState.ACTIVE, SynthaseState.QUANTUM_READY])
                
                # Spawn new synthase if demand is high and quantum enabled
                if demand > 0.8 and active_count < 3 and self.enable_multi_synthase:
                    self.spawn_synthase()
                    logger.info(f"Auto-scaled to {len(self.synthases)} synthases (demand: {demand:.2f})")
                
                # Remove excess synthases if demand is low
                elif demand < 0.2 and len(self.synthases) > 1:
                    for sid in list(self.synthases.keys()):
                        if sid != "primary" and len(self.synthases) > 1:
                            self.remove_synthase(sid)
                            break
                
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Regulation loop error: {str(e)}")
                await asyncio.sleep(60)
    
    # ========================================================================
    # Predictive Loop (Enhanced with ML)
    # ========================================================================
    
    async def _predictive_loop(self):
        """Predictive scheduling loop with ML"""
        while True:
            try:
                # Train ML model if enabled
                if self.enable_ml_prediction and self.ml_predictor:
                    if len(self.demand_history) > 50:
                        await self.ml_predictor.train(list(self.demand_history))
                
                # Predict future demand
                if self.enable_ml_prediction and self.ml_predictor and len(self.demand_history) > 30:
                    prediction_result = await self.ml_predictor.predict(list(self.demand_history))
                    if prediction_result['prediction'] is not None:
                        self.predicted_demand = prediction_result['prediction']
                        confidence = prediction_result['confidence']
                        logger.debug(f"ML demand prediction: {self.predicted_demand:.2f} (confidence: {confidence:.2f})")
                
                # Pre-allocate tokens for predicted demand
                if self.predicted_demand > 0.7 and self.token_manager:
                    pre_amount = self.predicted_demand * 100
                    self.token_manager.generate_tokens(
                        account_id=self.account_id,
                        source=EcoATPSource.GRADIENT_CONVERSION,
                        energy_saved_kwh=pre_amount / 10000.0,
                        efficiency=0.9
                    )
                
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Predictive loop error: {str(e)}")
                await asyncio.sleep(120)
    
    # ========================================================================
    # Gradient Forecast Loop (NEW)
    # ========================================================================
    
    async def _gradient_forecast_loop(self):
        """Gradient forecasting for proactive production"""
        while True:
            try:
                if self.gradient_manager:
                    for field_id in ['carbon', 'helium', 'trust', 'opportunity', 'eco_atp_reserve']:
                        # Record current gradient
                        field = self.gradient_manager.fields.get(field_id)
                        if field:
                            self.gradient_forecaster.record_gradient(field_id, field.effective_strength)
                        
                        # Generate forecast
                        await self.gradient_forecaster.forecast(field_id, horizon_steps=20)
                
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Gradient forecast loop error: {str(e)}")
                await asyncio.sleep(120)
    
    # ========================================================================
    # Scheduling Methods (Enhanced)
    # ========================================================================
    
    def schedule_execution(self, task_id: str, eco_atp_required: float,
                          priority: int = 0, deadline: Optional[datetime] = None,
                          callback: Optional[Callable] = None,
                          user_priority: Optional[str] = None) -> bool:
        """Schedule task execution with priority awareness"""
        if not self.token_manager:
            return True
        
        success, token_ids = self.token_manager.reserve_tokens(
            self.account_id, eco_atp_required, EcoATPConsumer.EXPERT_EXECUTION
        )
        
        if success:
            task = ScheduledTask(
                task_id=task_id, eco_atp_required=eco_atp_required,
                priority=priority, deadline=deadline, callback=callback,
                token_ids=token_ids, user_priority=user_priority
            )
            self.execution_queue.append(task)
            
            # Re-sort queue by effective priority
            self.execution_queue.sort(
                key=lambda t: (self.priority_manager.get_task_priority(t), t.deadline or datetime.max),
                reverse=True
            )
            return True
        
        task = ScheduledTask(
            task_id=task_id, eco_atp_required=eco_atp_required,
            priority=priority, deadline=deadline, callback=callback,
            user_priority=user_priority
        )
        self.priority_queue.append(task)
        return False
    
    def execute_next_task(self) -> Optional[Dict[str, Any]]:
        """Execute the next task with priority awareness"""
        if not self.execution_queue:
            return None
        
        # Queue is already sorted by effective priority
        task = self.execution_queue.pop(0)
        
        if self.token_manager:
            self.token_manager.consume_tokens(task.token_ids, EcoATPConsumer.EXPERT_EXECUTION, True)
        
        if task.callback:
            result = task.callback()
            task.status = "completed"
            return {'task_id': task.task_id, 'result': result, 'status': 'completed'}
        
        task.status = "completed"
        return {'task_id': task.task_id, 'status': 'completed'}
    
    def recover_failed_task(self, task_id: str, completion_percentage: float) -> float:
        """Recover tokens from failed task"""
        for task in self.execution_queue:
            if task.task_id == task_id:
                if self.token_manager:
                    recovered = self.token_manager.recover_tokens(task.token_ids, completion_percentage)
                    self.execution_queue.remove(task)
                    return recovered
        return 0.0
    
    # ========================================================================
    # Degradation Integration
    # ========================================================================
    
    def set_degradation_tier(self, tier: int):
        """Set degradation tier for production scaling"""
        self.current_tier = max(1, min(5, tier))
        
        if tier <= 2:
            for sid in list(self.synthases.keys()):
                if sid != "primary":
                    self.synthases[sid].state = SynthaseState.DORMANT
                    self.remove_synthase(sid)
        
        logger.info(f"ATP Synthase degradation tier set to {tier}")
    
    # ========================================================================
    # Priority Management (NEW)
    # ========================================================================
    
    def set_priority_config(self, priority_level: str, weight: float,
                           min_balance: float, max_consumption: float):
        """Set priority configuration"""
        self.priority_manager.set_priority_config(
            priority_level, weight, min_balance, max_consumption
        )
    
    def get_priority_stats(self) -> Dict:
        """Get priority statistics"""
        return {
            'priorities': {
                level: {
                    'weight': p.weight,
                    'min_balance': p.min_balance,
                    'max_consumption': p.max_consumption
                }
                for level, p in self.priority_manager.priorities.items()
            },
            'default_priority': self.priority_manager.default_priority
        }
    
    # ========================================================================
    # Statistics (Enhanced)
    # ========================================================================
    
    def get_scheduler_stats(self) -> Dict[str, Any]:
        """Get comprehensive scheduler statistics"""
        driving_force = self.calculate_gradient_driving_force()
        rotation_speed = self.calculate_rotation_speed(driving_force)
        atp_rate = self.calculate_atp_production_rate(rotation_speed)
        
        stats = {
            'total_eco_atp_produced': self.total_eco_atp_produced,
            'current_driving_force': driving_force,
            'current_rotation_speed': rotation_speed,
            'current_atp_rate': atp_rate,
            'demand_level': self._calculate_demand_level(),
            'predicted_demand': self.predicted_demand,
            'degradation_tier': self.current_tier,
            'queue_size': len(self.execution_queue),
            'priority_queue_size': len(self.priority_queue),
            'synthase_count': len(self.synthases),
            'active_synthases': sum(1 for s in self.synthases.values() if s.state in [SynthaseState.ACTIVE, SynthaseState.QUANTUM_READY]),
            'quantum_active': self.enable_quantum and any(s.quantum_active for s in self.synthases.values()),
            'synthases': {
                sid: s.get_status() for sid, s in self.synthases.items()
            },
            'recent_production': [
                {
                    'timestamp': r.timestamp.isoformat(),
                    'mode': r.mode,
                    'atp_produced': r.atp_produced,
                    'efficiency': r.efficiency,
                    'quantum_enhancement': r.quantum_enhancement
                }
                for r in list(self.primary_synthase.production_history)[-10:]
            ],
            'load_balance': self.load_balancer.get_load_balance_stats(),
            'ml_predictor': self.ml_predictor.get_model_stats() if self.ml_predictor else None,
            'gradient_forecast': self.gradient_forecaster.get_forecast_summary()
        }
        
        return stats
    
    def get_efficiency_report(self) -> Dict[str, Any]:
        """Get efficiency optimization report"""
        report = {
            'primary_efficiency': self.primary_synthase.current_efficiency,
            'base_efficiency': self.config.base_efficiency,
            'degradation': self.primary_synthase.operational_hours * self.primary_synthase.degradation_rate,
            'inhibition_level': self.primary_synthase.inhibition_level,
            'synthase_count': len(self.synthases),
            'quantum_enhancement': self.primary_synthase.quantum_enhancement_factor,
            'quantum_active': self.primary_synthase.quantum_active,
            'recommendations': []
        }
        
        if self.primary_synthase.current_efficiency < 0.8:
            report['recommendations'].append("Primary synthase degraded. Consider repair cycle.")
        
        if len(self.synthases) > 1 and self._calculate_demand_level() < 0.3:
            report['recommendations'].append("Low demand with multiple synthases. Consider consolidating.")
        
        if self.primary_synthase.inhibition_level > 0.4:
            report['recommendations'].append("High ATP inhibition. Consider reverse operation to regulate.")
        
        if self.enable_quantum and not self.primary_synthase.quantum_active and self._calculate_demand_level() > 0.5:
            report['recommendations'].append("Quantum enhancement available but inactive. Increase gradient to activate.")
        
        return report
