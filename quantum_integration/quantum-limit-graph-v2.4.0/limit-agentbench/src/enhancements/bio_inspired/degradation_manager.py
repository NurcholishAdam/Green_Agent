# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/degradation_manager.py
# Complete enhanced file v6.0.0 with all improvements

"""
Enhanced Degradation Manager v6.0.0
Complete implementation with predictive degradation, gradual transitions,
hysteresis, weighted health scoring, trend-based rules, chaos analytics,
ML-based health prediction (NEW), anomaly detection for sudden changes (NEW),
chaos injection for specific components (NEW), user-defined transition speeds (NEW),
and predictive recovery validation (NEW).
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import deque
import hashlib
import json
from sklearn.ensemble import RandomForestRegressor, IsolationForest
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

# ============================================================================
# Try importing dependencies
# ============================================================================
try:
    from .proton_gradient_fields import GradientFieldManager
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False

# ============================================================================
# Enums and Data Classes
# ============================================================================

class OperationalTier(Enum):
    """5-tier operational readiness levels"""
    TIER_5_FULL = 5
    TIER_4_REDUCED = 4
    TIER_3_CONSERVATIVE = 3
    TIER_2_CRITICAL = 2
    TIER_1_SURVIVAL = 1

class TransitionType(Enum):
    """Types of tier transitions"""
    DEGRADATION = "degradation"
    RECOVERY = "recovery"
    PREEMPTIVE = "preemptive"
    CHAOS_INDUCED = "chaos_induced"
    MANUAL = "manual"
    ANOMALY_INDUCED = "anomaly_induced"  # NEW

class TransitionSpeed(Enum):  # NEW
    """User-defined transition speeds"""
    INSTANT = "instant"      # Immediate transition
    FAST = "fast"           # 5-second transition
    NORMAL = "normal"       # 15-second transition
    SLOW = "slow"           # 60-second transition
    GRACEFUL = "graceful"   # 120-second transition

@dataclass
class DegradationRule:
    """Enhanced degradation rule with hysteresis and trend awareness"""
    rule_id: str
    metric: str
    enter_threshold: float          # Threshold to enter this tier
    exit_threshold: float           # Threshold to exit this tier (hysteresis)
    comparison: str                 # 'above' or 'below'
    target_tier: OperationalTier
    cooldown_seconds: float = 60.0
    description: str = ""
    weight: float = 1.0             # Weight in composite health score
    trend_sensitive: bool = False    # Whether to consider metric trend
    trend_window: int = 10          # Samples for trend calculation
    trend_threshold: float = 0.0    # Trend threshold for preemptive action
    anomaly_sensitive: bool = False  # NEW: Whether to consider anomalies

@dataclass
class TransitionRecord:
    """Record of a tier transition"""
    transition_id: str
    timestamp: datetime
    transition_type: TransitionType
    from_tier: OperationalTier
    to_tier: OperationalTier
    trigger_metric: str
    trigger_value: float
    trigger_threshold: float
    health_scores: Dict[str, float]
    duration_in_previous_tier: float
    was_preemptive: bool = False
    was_anomaly: bool = False  # NEW
    transition_speed: TransitionSpeed = TransitionSpeed.NORMAL  # NEW

@dataclass
class HealthScore:
    """Weighted composite health score with ML prediction"""
    timestamp: datetime
    overall_score: float
    component_scores: Dict[str, float]
    trend: str                      # 'improving', 'degrading', 'stable'
    predicted_tier: Optional[OperationalTier] = None
    time_to_next_tier: Optional[float] = None
    confidence: float = 0.7
    # NEW: ML prediction
    ml_predicted_score: Optional[float] = None
    ml_confidence: float = 0.0
    anomaly_score: float = 0.0      # NEW: Anomaly detection score
    is_anomalous: bool = False      # NEW: Whether current state is anomalous

@dataclass
class ChaosExperimentResult:
    """Enhanced chaos experiment result with analytics"""
    experiment_id: str
    experiment_name: str
    intensity: float
    start_time: datetime
    end_time: datetime
    recovery_time_seconds: float
    tier_impact: int
    safety_breached: bool
    metrics_before: Dict[str, float]
    metrics_after: Dict[str, float]
    resilience_score: float
    recommendations: List[str]
    lessons_learned: List[str]
    # NEW: Component-specific impact
    component_impacts: Dict[str, float] = field(default_factory=dict)

# ============================================================================
# ML-Based Health Predictor (NEW)
# ============================================================================

class MLHealthPredictor:
    """
    Machine learning-based health prediction using historical patterns.
    
    Features:
    - Random Forest regression for health prediction
    - Pattern recognition from historical data
    - Confidence scoring
    - Online learning
    """
    
    def __init__(self):
        self.model = RandomForestRegressor(n_estimators=50, random_state=42)
        self.scaler = StandardScaler()
        self.is_trained = False
        self.training_samples = 0
        self.history: List[Dict] = []
        self._lock = asyncio.Lock()
        
        logger.info("ML Health Predictor initialized")
    
    def add_training_data(self, health_data: Dict[str, float]):
        """Add data point for training"""
        self.history.append({
            'timestamp': datetime.utcnow(),
            **health_data
        })
        if len(self.history) > 1000:
            self.history = self.history[-1000:]
    
    async def train(self):
        """Train the ML model"""
        if len(self.history) < 50:
            return {'status': 'insufficient_data', 'samples': len(self.history)}
        
        async with self._lock:
            # Prepare features
            X = []
            y = []
            
            for i in range(10, len(self.history) - 1):
                features = []
                for j in range(10):
                    data = self.history[i - j]
                    features.extend([
                        data.get('health_score', 0.5),
                        data.get('token_balance', 500) / 1000,
                        data.get('carbon_gradient', 0.5),
                        data.get('compartment_health', 0.5),
                        data.get('harvester_activity', 0.5),
                        data.get('error_rate', 0.01)
                    ])
                X.append(features)
                y.append(self.history[i + 1].get('health_score', 0.5))
            
            if len(X) < 20:
                return {'status': 'insufficient_training_data', 'samples': len(X)}
            
            X = np.array(X)
            y = np.array(y)
            X_scaled = self.scaler.fit_transform(X)
            
            self.model.fit(X_scaled, y)
            self.is_trained = True
            self.training_samples = len(X)
            
            logger.info(f"ML Health Predictor trained on {len(X)} samples")
            return {'status': 'success', 'samples': len(X)}
    
    async def predict(self, current_metrics: Dict[str, float]) -> Dict[str, Any]:
        """Predict future health using ML"""
        if not self.is_trained:
            return {'predicted_score': None, 'confidence': 0.0}
        
        async with self._lock:
            # Prepare features from current metrics
            features = [
                current_metrics.get('health_score', 0.5),
                current_metrics.get('token_balance', 500) / 1000,
                current_metrics.get('carbon_gradient', 0.5),
                current_metrics.get('compartment_health', 0.5),
                current_metrics.get('harvester_activity', 0.5),
                current_metrics.get('error_rate', 0.01)
            ]
            
            # Add recent history for context
            recent = self.history[-10:] if len(self.history) >= 10 else self.history
            for data in recent:
                features.extend([
                    data.get('health_score', 0.5),
                    data.get('token_balance', 500) / 1000,
                    data.get('carbon_gradient', 0.5),
                    data.get('compartment_health', 0.5),
                    data.get('harvester_activity', 0.5),
                    data.get('error_rate', 0.01)
                ])
            
            # Ensure correct feature count
            features = features[:self.model.n_features_in_]
            while len(features) < self.model.n_features_in_:
                features.append(0.5)
            
            features_array = np.array([features])
            features_scaled = self.scaler.transform(features_array)
            
            prediction = self.model.predict(features_scaled)[0]
            confidence = min(0.9, self.training_samples / 100)
            
            return {
                'predicted_score': max(0.0, min(1.0, prediction)),
                'confidence': confidence,
                'timestamp': datetime.utcnow().isoformat()
            }

# ============================================================================
# Anomaly Detection System (NEW)
# ============================================================================

class AnomalyDetectionSystem:
    """
    Anomaly detection for sudden metric changes.
    
    Features:
    - Isolation Forest for outlier detection
    - Z-score based anomaly detection
    - Threshold-based alerting
    - Anomaly history tracking
    """
    
    def __init__(self):
        self.model = IsolationForest(contamination=0.1, random_state=42)
        self.scaler = StandardScaler()
        self.is_trained = False
        self.metric_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.anomaly_history: deque = deque(maxlen=100)
        self._lock = asyncio.Lock()
        self.zscore_threshold = 3.0
        
        logger.info("Anomaly Detection System initialized")
    
    def add_metric(self, metric_name: str, value: float):
        """Add metric value for anomaly detection"""
        self.metric_history[metric_name].append({
            'value': value,
            'timestamp': datetime.utcnow()
        })
    
    async def detect_anomalies(self, metrics: Dict[str, float]) -> Dict[str, Any]:
        """Detect anomalies in metrics"""
        async with self._lock:
            anomalies = []
            anomaly_scores = {}
            
            # Z-score based detection
            for metric_name, value in metrics.items():
                if metric_name in self.metric_history:
                    history = list(self.metric_history[metric_name])
                    if len(history) >= 10:
                        values = [h['value'] for h in history[-20:]]
                        mean = np.mean(values)
                        std = np.std(values)
                        
                        if std > 0:
                            zscore = abs(value - mean) / std
                            if zscore > self.zscore_threshold:
                                anomalies.append({
                                    'metric': metric_name,
                                    'value': value,
                                    'mean': mean,
                                    'zscore': zscore,
                                    'timestamp': datetime.utcnow().isoformat()
                                })
                                anomaly_scores[metric_name] = min(1.0, zscore / 5.0)
            
            # Train Isolation Forest if enough data
            if not self.is_trained and len(self.metric_history) > 50:
                await self._train_isolation_forest()
            
            # Use Isolation Forest for additional detection
            if self.is_trained:
                # Prepare features for current metrics
                features = []
                for metric in ['token_balance', 'carbon_gradient', 'compartment_health', 
                              'harvester_activity', 'error_rate']:
                    if metric in metrics:
                        features.append(metrics[metric])
                    else:
                        features.append(0.5)
                
                features_array = np.array([features])
                features_scaled = self.scaler.transform(features_array)
                prediction = self.model.predict(features_scaled)[0]
                
                if prediction == -1:
                    anomalies.append({
                        'metric': 'composite',
                        'value': 'outlier',
                        'type': 'isolation_forest',
                        'timestamp': datetime.utcnow().isoformat()
                    })
                    anomaly_scores['composite'] = 0.8
            
            return {
                'anomalies': anomalies,
                'anomaly_scores': anomaly_scores,
                'is_anomalous': len(anomalies) > 0,
                'timestamp': datetime.utcnow().isoformat()
            }
    
    async def _train_isolation_forest(self):
        """Train Isolation Forest model"""
        if self.is_trained:
            return
        
        # Prepare training data
        X = []
        for i in range(50):  # Use last 50 data points
            features = []
            for metric in ['token_balance', 'carbon_gradient', 'compartment_health', 
                          'harvester_activity', 'error_rate']:
                if metric in self.metric_history:
                    history = list(self.metric_history[metric])
                    if len(history) > i:
                        features.append(history[-(i+1)]['value'])
                    else:
                        features.append(0.5)
                else:
                    features.append(0.5)
            X.append(features)
        
        if len(X) < 20:
            return
        
        X = np.array(X)
        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled)
        self.is_trained = True
        
        logger.info("Isolation Forest trained for anomaly detection")

# ============================================================================
# Chaos Injection System (NEW)
# ============================================================================

class ChaosInjectionSystem:
    """
    Chaos injection for specific components.
    
    Features:
    - Targeted component injection
    - Configurable intensity
    - Safety bounds per component
    - Component-specific recovery
    """
    
    def __init__(self):
        self.component_health: Dict[str, float] = {}
        self.injection_history: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        
        # Component injection configurations
        self.component_configs = {
            'token_manager': {'max_intensity': 0.8, 'recovery_time': 30},
            'gradient_manager': {'max_intensity': 0.7, 'recovery_time': 45},
            'compartment_manager': {'max_intensity': 0.9, 'recovery_time': 60},
            'biomass_storage': {'max_intensity': 0.6, 'recovery_time': 20},
            'harvester': {'max_intensity': 0.5, 'recovery_time': 15},
            'atp_synthase': {'max_intensity': 0.7, 'recovery_time': 40}
        }
        
        logger.info("Chaos Injection System initialized")
    
    async def inject_component_chaos(self, component: str, intensity: float) -> Dict[str, Any]:
        """Inject chaos into a specific component"""
        if component not in self.component_configs:
            return {'status': 'error', 'message': f'Unknown component: {component}'}
        
        config = self.component_configs[component]
        if intensity > config['max_intensity']:
            return {'status': 'error', 'message': f'Intensity exceeds max ({config["max_intensity"]})'}
        
        async with self._lock:
            # Record health before injection
            health_before = self.component_health.get(component, 0.5)
            
            # Apply injection (simulated)
            health_impact = intensity * 0.5
            new_health = max(0.0, health_before - health_impact)
            self.component_health[component] = new_health
            
            self.injection_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'component': component,
                'intensity': intensity,
                'health_before': health_before,
                'health_after': new_health,
                'recovery_time': config['recovery_time']
            })
            
            logger.warning(f"CHAOS INJECTION: {component} at {intensity:.0%} (health: {health_before:.2f} → {new_health:.2f})")
            
            return {
                'status': 'success',
                'component': component,
                'intensity': intensity,
                'health_before': health_before,
                'health_after': new_health,
                'estimated_recovery': config['recovery_time']
            }
    
    async def recover_component(self, component: str) -> Dict[str, Any]:
        """Recover a component from chaos injection"""
        if component not in self.component_configs:
            return {'status': 'error', 'message': f'Unknown component: {component}'}
        
        async with self._lock:
            recovery_amount = 0.5
            current_health = self.component_health.get(component, 0.5)
            new_health = min(1.0, current_health + recovery_amount)
            self.component_health[component] = new_health
            
            logger.info(f"Component recovery: {component} (health: {current_health:.2f} → {new_health:.2f})")
            
            return {
                'status': 'success',
                'component': component,
                'recovery_amount': recovery_amount,
                'health_after': new_health
            }
    
    def get_component_health(self) -> Dict[str, float]:
        """Get health status for all components"""
        return self.component_health.copy()
    
    def get_injection_stats(self) -> Dict[str, Any]:
        """Get chaos injection statistics"""
        recent = list(self.injection_history)[-50:]
        
        if not recent:
            return {'status': 'no_injections'}
        
        component_counts = defaultdict(int)
        for entry in recent:
            component_counts[entry['component']] += 1
        
        return {
            'total_injections': len(self.injection_history),
            'recent_injections': len(recent),
            'component_distribution': dict(component_counts),
            'average_intensity': np.mean([e['intensity'] for e in recent]),
            'average_health_impact': np.mean([e['health_before'] - e['health_after'] for e in recent])
        }

# ============================================================================
# Enhanced Degradation Manager
# ============================================================================

class DegradationManager:
    """
    Enhanced Degradation Manager v6.0.0
    
    New Features:
    - ML-based health prediction
    - Anomaly detection for sudden metric changes
    - Chaos injection for specific components
    - User-defined transition speeds
    - Predictive recovery validation
    """
    
    def __init__(self, event_bus=None):
        self.event_bus = event_bus
        
        # Current operational state
        self.current_tier = OperationalTier.TIER_5_FULL
        self.previous_tier = OperationalTier.TIER_5_FULL
        
        # Transition tracking
        self.tier_history: List[TransitionRecord] = []
        self.last_transition_time = datetime.utcnow()
        self.transition_cooldown = timedelta(seconds=30)
        self.transition_in_progress = False
        self.gradual_transition_remaining = 0.0
        
        # Health metrics storage
        self.metrics_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.health_scores: deque = deque(maxlen=100)
        
        # Tier-specific policies with gradual application
        self.tier_policies = self._initialize_policies()
        self.current_policy = self.tier_policies[OperationalTier.TIER_5_FULL]
        self.target_policy = None
        self.policy_transition_progress = 1.0
        
        # Degradation and recovery rules with hysteresis
        self.rules = self._initialize_rules()
        
        # Callbacks
        self.tier_change_callbacks: List[Callable] = []
        
        # NEW: ML Health Predictor
        self.ml_predictor = MLHealthPredictor()
        
        # NEW: Anomaly Detection System
        self.anomaly_detector = AnomalyDetectionSystem()
        
        # NEW: Chaos Injection System
        self.chaos_injector = ChaosInjectionSystem()
        
        # Predictive degradation
        self.prediction_enabled = True
        self.prediction_horizon_seconds = 60.0
        self.predicted_tier: Optional[OperationalTier] = None
        self.time_to_predicted_tier: Optional[float] = None
        self.prediction_history: deque = deque(maxlen=100)
        
        # NEW: User-defined transition speeds
        self.transition_speed = TransitionSpeed.NORMAL
        self.transition_speed_map = {
            TransitionSpeed.INSTANT: 0.0,
            TransitionSpeed.FAST: 5.0,
            TransitionSpeed.NORMAL: 15.0,
            TransitionSpeed.SLOW: 60.0,
            TransitionSpeed.GRACEFUL: 120.0
        }
        
        # NEW: Recovery validation
        self.recovery_validation_enabled = True
        self.recovery_validation_period = timedelta(seconds=60)
        self.recovery_validation_metrics: Dict[str, List[float]] = defaultdict(list)
        self.recovering_from_tier: Optional[OperationalTier] = None
        
        # Chaos engineering
        self.chaos_experiments: Dict[str, Dict[str, Any]] = {}
        self.chaos_history: deque = deque(maxlen=500)
        self.chaos_active = False
        self.chaos_safety_enabled = True
        self.chaos_schedule_enabled = True
        self.chaos_schedule_interval_hours = 6
        self._initialize_chaos_experiments()
        
        # Start background tasks
        asyncio.create_task(self._monitoring_loop())
        asyncio.create_task(self._predictive_loop())
        asyncio.create_task(self._chaos_scheduler_loop())
        asyncio.create_task(self._gradual_transition_loop())
        asyncio.create_task(self._anomaly_monitoring_loop())  # NEW
        
        logger.info(f"Enhanced Degradation Manager v6.0.0 initialized at {self.current_tier.name}")
    
    def _initialize_policies(self) -> Dict[OperationalTier, Dict[str, Any]]:
        """Initialize operational policies"""
        return {
            OperationalTier.TIER_5_FULL: {
                'expert_activation': 'all',
                'token_allocation': 'generous',
                'exploration_rate': 0.2,
                'cache_ttl_seconds': 120,
                'max_parallel_tasks': 100,
                'quality_threshold': 0.9,
                'biomass_storage': 'all_tiers',
                'gradient_sensitivity': 'high',
                'atp_production': 1.0,
                'harvesting_rate': 1.0
            },
            OperationalTier.TIER_4_REDUCED: {
                'expert_activation': 'all',
                'token_allocation': 'moderate',
                'exploration_rate': 0.1,
                'cache_ttl_seconds': 90,
                'max_parallel_tasks': 75,
                'quality_threshold': 0.85,
                'biomass_storage': 'hot_warm_only',
                'gradient_sensitivity': 'moderate',
                'atp_production': 0.75,
                'harvesting_rate': 0.8
            },
            OperationalTier.TIER_3_CONSERVATIVE: {
                'expert_activation': 'essential_only',
                'token_allocation': 'conservative',
                'exploration_rate': 0.05,
                'cache_ttl_seconds': 60,
                'max_parallel_tasks': 40,
                'quality_threshold': 0.8,
                'biomass_storage': 'hot_only',
                'gradient_sensitivity': 'low',
                'atp_production': 0.5,
                'harvesting_rate': 0.5
            },
            OperationalTier.TIER_2_CRITICAL: {
                'expert_activation': 'critical_only',
                'token_allocation': 'minimal',
                'exploration_rate': 0.0,
                'cache_ttl_seconds': 30,
                'max_parallel_tasks': 15,
                'quality_threshold': 0.7,
                'biomass_storage': 'emergency_only',
                'gradient_sensitivity': 'minimal',
                'atp_production': 0.25,
                'harvesting_rate': 0.2
            },
            OperationalTier.TIER_1_SURVIVAL: {
                'expert_activation': 'survival_only',
                'token_allocation': 'emergency',
                'exploration_rate': 0.0,
                'cache_ttl_seconds': 10,
                'max_parallel_tasks': 5,
                'quality_threshold': 0.5,
                'biomass_storage': 'none',
                'gradient_sensitivity': 'none',
                'atp_production': 0.1,
                'harvesting_rate': 0.0
            }
        }
    
    def _initialize_rules(self) -> List[DegradationRule]:
        """Initialize rules with hysteresis and anomaly sensitivity"""
        return [
            # Degradation rules
            DegradationRule('R1', 'token_balance', 100, 150, 'below',
                           OperationalTier.TIER_4_REDUCED, 60,
                           'Token balance below threshold', 0.30, True, 10, -5.0, True),
            DegradationRule('R2', 'token_balance', 30, 80, 'below',
                           OperationalTier.TIER_3_CONSERVATIVE, 30,
                           'Token balance critically low', 0.25, True, 10, -3.0, True),
            DegradationRule('R3', 'carbon_gradient', 0.85, 0.75, 'above',
                           OperationalTier.TIER_3_CONSERVATIVE, 120,
                           'High carbon gradient', 0.25, True, 10, 0.02, True),
            DegradationRule('R4', 'compartment_health', 0.3, 0.5, 'below',
                           OperationalTier.TIER_2_CRITICAL, 60,
                           'Low compartment health', 0.20, True, 10, -0.05, True),
            DegradationRule('R5', 'token_balance', 10, 25, 'below',
                           OperationalTier.TIER_1_SURVIVAL, 30,
                           'Emergency token level', 0.30, True, 5, -2.0, True),
            
            # Recovery rules
            DegradationRule('R6', 'token_balance', 200, 150, 'above',
                           OperationalTier.TIER_4_REDUCED, 120,
                           'Token recovery sufficient', 0.30, True, 10, 5.0, False),
            DegradationRule('R7', 'token_balance', 500, 400, 'above',
                           OperationalTier.TIER_5_FULL, 300,
                           'Full token recovery', 0.30, True, 10, 10.0, False),
            DegradationRule('R8', 'compartment_health', 0.6, 0.5, 'above',
                           OperationalTier.TIER_3_CONSERVATIVE, 180,
                           'Health recovery sufficient', 0.20, True, 10, 0.05, False),
            DegradationRule('R9', 'carbon_gradient', 0.4, 0.5, 'below',
                           OperationalTier.TIER_4_REDUCED, 180,
                           'Carbon improvement', 0.25, True, 10, -0.02, False),
        ]
    
    def _initialize_chaos_experiments(self):
        """Initialize chaos experiments"""
        self.chaos_experiments = {
            'token_drought': {
                'name': 'Token Drought',
                'description': 'Reduce token generation to test system response',
                'intensity_levels': [0.7, 0.5, 0.3, 0.1],
                'duration_per_level': 60,
                'safety_bounds': {'min_token_balance': 10, 'max_tier_drop': 2},
                'rollback_trigger': 'token_balance < 10',
                'metrics_collected': ['token_consumption_rate', 'expert_activation_count', 'degradation_tier'],
                'analytics': {'run_count': 0, 'avg_recovery_time': 0, 'avg_resilience_score': 0}
            },
            'gradient_flood': {
                'name': 'Gradient Flood',
                'description': 'Artificially pump all gradients to maximum',
                'intensity_levels': [0.6, 0.8, 0.95],
                'duration_per_level': 45,
                'safety_bounds': {'max_overflow_buffer': 50, 'max_tier_drop': 3},
                'rollback_trigger': 'any_gradient > 0.98 for 30s',
                'metrics_collected': ['gradient_saturation_time', 'homeostasis_response_time', 'recovery_time'],
                'analytics': {'run_count': 0, 'avg_recovery_time': 0, 'avg_resilience_score': 0}
            },
            'compartment_cascade': {
                'name': 'Compartment Cascade Failure',
                'description': 'Kill compartments in rapid succession',
                'intensity_levels': [0.1, 0.3, 0.5],
                'duration_per_level': 30,
                'safety_bounds': {'min_viable_compartments': 2, 'max_tier_drop': 3},
                'rollback_trigger': 'viable_compartments < 2',
                'metrics_collected': ['compartment_recovery_time', 'task_redistribution_time', 'survival_count'],
                'analytics': {'run_count': 0, 'avg_recovery_time': 0, 'avg_resilience_score': 0}
            },
            'bio_core_partition': {
                'name': 'Bio-Core Network Partition',
                'description': 'Simulate bio-core unavailability',
                'intensity_levels': [15, 30, 60],
                'duration_per_level': 0,
                'safety_bounds': {'max_degraded_operations': 100, 'max_tier_drop': 2},
                'rollback_trigger': 'degraded_operations > 100',
                'metrics_collected': ['buffer_hit_rate', 'degraded_operation_count', 'recovery_sync_time'],
                'analytics': {'run_count': 0, 'avg_recovery_time': 0, 'avg_resilience_score': 0}
            }
        }
    
    # ========================================================================
    # Callbacks and Event Publishing
    # ========================================================================
    
    def register_callback(self, callback: Callable):
        """Register callback for tier changes"""
        self.tier_change_callbacks.append(callback)
    
    def _publish_transition_event(self, transition: TransitionRecord):
        """Publish tier transition event"""
        if self.event_bus:
            self.event_bus.publish('degradation_tier_change', {
                'transition_id': transition.transition_id,
                'from_tier': transition.from_tier.value,
                'to_tier': transition.to_tier.value,
                'transition_type': transition.transition_type.value,
                'trigger_metric': transition.trigger_metric,
                'was_preemptive': transition.was_preemptive,
                'was_anomaly': transition.was_anomaly,
                'transition_speed': transition.transition_speed.value,
                'timestamp': transition.timestamp.isoformat()
            })
    
    # ========================================================================
    # Health Metrics Collection (Enhanced)
    # ========================================================================
    
    def update_metrics(self, **kwargs):
        """Update health metrics and train ML model"""
        for key, value in kwargs.items():
            setattr(self, f'_{key}', value)
            self.metrics_history[key].append({
                'value': value,
                'timestamp': datetime.utcnow()
            })
            # Add to anomaly detection
            self.anomaly_detector.add_metric(key, value)
        
        # Add to ML training data
        self.ml_predictor.add_training_data(kwargs)
        asyncio.create_task(self.ml_predictor.train())
    
    def _collect_health_metrics(self) -> Dict[str, float]:
        """Collect current health metrics"""
        return {
            'token_balance': getattr(self, '_token_balance', 500),
            'carbon_gradient': getattr(self, '_carbon_gradient', 0.5),
            'compartment_health': getattr(self, '_compartment_health', 0.8),
            'harvester_activity': getattr(self, '_harvester_activity', 0.6),
            'error_rate': getattr(self, '_error_rate', 0.01),
            'queue_depth': getattr(self, '_queue_depth', 10)
        }
    
    # ========================================================================
    # Weighted Health Scoring with ML Prediction (Enhanced)
    # ========================================================================
    
    def calculate_health_score(self) -> HealthScore:
        """Calculate weighted composite health score with ML prediction"""
        metrics = self._collect_health_metrics()
        
        # Normalize each metric to 0-1 score (1 = healthy)
        scores = {
            'token_balance': min(1.0, metrics['token_balance'] / 500.0),
            'carbon_gradient': 1.0 - metrics['carbon_gradient'],
            'compartment_health': metrics['compartment_health'],
            'harvester_activity': metrics['harvester_activity'],
            'error_rate': 1.0 - min(1.0, metrics['error_rate'] * 10),
            'queue_depth': 1.0 - min(1.0, metrics['queue_depth'] / 100.0)
        }
        
        # Weighted combination
        weights = {
            'token_balance': 0.30,
            'carbon_gradient': 0.25,
            'compartment_health': 0.20,
            'harvester_activity': 0.15,
            'error_rate': 0.10
        }
        
        overall = sum(scores[k] * weights.get(k, 0.1) for k in scores)
        
        # Get ML prediction
        ml_prediction = asyncio.run(self.ml_predictor.predict(metrics))
        ml_predicted_score = ml_prediction.get('predicted_score')
        ml_confidence = ml_prediction.get('confidence', 0)
        
        # Detect anomalies
        anomaly_result = asyncio.run(self.anomaly_detector.detect_anomalies(metrics))
        is_anomalous = anomaly_result.get('is_anomalous', False)
        anomaly_score = max(anomaly_result.get('anomaly_scores', {}).values()) if anomaly_result.get('anomaly_scores') else 0.0
        
        # Determine trend
        trend = self._calculate_health_trend()
        
        # Predict next tier
        predicted_tier = self._predict_tier_from_score(overall)
        
        score = HealthScore(
            timestamp=datetime.utcnow(),
            overall_score=overall,
            component_scores=scores,
            trend=trend,
            predicted_tier=predicted_tier,
            confidence=0.7 + 0.2 * (len(self.health_scores) / 100),
            ml_predicted_score=ml_predicted_score,
            ml_confidence=ml_confidence,
            anomaly_score=anomaly_score,
            is_anomalous=is_anomalous
        )
        
        self.health_scores.append(score)
        
        return score
    
    def _calculate_health_trend(self) -> str:
        """Calculate health score trend"""
        if len(self.health_scores) < 5:
            return 'stable'
        
        recent = [s.overall_score for s in list(self.health_scores)[-10:]]
        slope = np.polyfit(range(len(recent)), recent, 1)[0]
        
        if slope > 0.01:
            return 'improving'
        elif slope < -0.01:
            return 'degrading'
        return 'stable'
    
    def _predict_tier_from_score(self, score: float) -> Optional[OperationalTier]:
        """Predict operational tier from health score"""
        if score > 0.8:
            return OperationalTier.TIER_5_FULL
        elif score > 0.6:
            return OperationalTier.TIER_4_REDUCED
        elif score > 0.4:
            return OperationalTier.TIER_3_CONSERVATIVE
        elif score > 0.2:
            return OperationalTier.TIER_2_CRITICAL
        else:
            return OperationalTier.TIER_1_SURVIVAL
    
    # ========================================================================
    # Anomaly Monitoring Loop (NEW)
    # ========================================================================
    
    async def _anomaly_monitoring_loop(self):
        """Background loop for anomaly detection"""
        while True:
            try:
                metrics = self._collect_health_metrics()
                anomaly_result = await self.anomaly_detector.detect_anomalies(metrics)
                
                if anomaly_result['is_anomalous']:
                    logger.warning(f"ANOMALY DETECTED: {anomaly_result['anomalies']}")
                    
                    # Trigger preemptive degradation if anomaly is severe
                    max_anomaly_score = max(anomaly_result['anomaly_scores'].values()) if anomaly_result['anomaly_scores'] else 0
                    if max_anomaly_score > 0.7:
                        # Degrade one tier
                        target_tier = OperationalTier(max(1, self.current_tier.value - 1))
                        await self._transition_to(
                            target_tier, metrics, TransitionType.ANOMALY_INDUCED,
                            'anomaly_detected', max_anomaly_score, 0.7,
                            was_anomaly=True
                        )
                
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Anomaly monitoring error: {str(e)}")
                await asyncio.sleep(60)
    
    # ========================================================================
    # Predictive Degradation Loop (Enhanced)
    # ========================================================================
    
    async def _predictive_loop(self):
        """Predictive degradation loop with ML and anomaly awareness"""
        while True:
            try:
                if not self.prediction_enabled:
                    await asyncio.sleep(30)
                    continue
                
                metrics = self._collect_health_metrics()
                health_score = self.calculate_health_score()
                
                # Use ML prediction
                if health_score.ml_predicted_score and health_score.ml_confidence > 0.5:
                    ml_prediction = health_score.ml_predicted_score
                    if ml_prediction < 0.3 and self.current_tier.value > 2:
                        logger.warning(f"ML PREDICTION: Health predicted to drop to {ml_prediction:.2f}")
                        
                        target_tier = self._predict_tier_from_score(ml_prediction)
                        if target_tier and target_tier.value < self.current_tier.value:
                            await self._transition_to(
                                target_tier, metrics, TransitionType.PREEMPTIVE,
                                'ml_prediction', ml_prediction, 0.3,
                                was_preemptive=True
                            )
                
                # Check each rule for predictive triggering
                earliest_transition = None
                earliest_time = float('inf')
                
                for rule in self.rules:
                    if rule.trend_sensitive:
                        is_trending, seconds = self._is_trending_toward_threshold(
                            rule.metric, rule.enter_threshold, rule.comparison, rule.trend_window
                        )
                        
                        if is_trending and seconds < earliest_time:
                            trend = self._calculate_metric_trend(rule.metric, rule.trend_window)
                            
                            if (rule.comparison == 'below' and trend < rule.trend_threshold) or \
                               (rule.comparison == 'above' and trend > rule.trend_threshold):
                                earliest_transition = rule
                                earliest_time = seconds
                
                # Trigger preemptive transition if imminent
                if earliest_transition and earliest_time < self.prediction_horizon_seconds:
                    if self.current_tier.value > earliest_transition.target_tier.value:
                        logger.warning(
                            f"PREDICTIVE DEGRADATION: {earliest_transition.metric} trending toward "
                            f"threshold in {earliest_time:.0f}s. Pre-emptively transitioning to "
                            f"{earliest_transition.target_tier.name}"
                        )
                        
                        await self._transition_to(
                            earliest_transition.target_tier,
                            metrics,
                            TransitionType.PREEMPTIVE,
                            earliest_transition.metric,
                            metrics.get(earliest_transition.metric, 0),
                            earliest_transition.enter_threshold,
                            was_preemptive=True
                        )
                
                # Store prediction
                self.predicted_tier = earliest_transition.target_tier if earliest_transition else None
                self.time_to_predicted_tier = earliest_time if earliest_time != float('inf') else None
                
                self.prediction_history.append({
                    'timestamp': datetime.utcnow().isoformat(),
                    'predicted_tier': self.predicted_tier.value if self.predicted_tier else None,
                    'time_to_transition': self.time_to_predicted_tier,
                    'trigger_metric': earliest_transition.metric if earliest_transition else None,
                    'ml_prediction': health_score.ml_predicted_score,
                    'anomaly_score': health_score.anomaly_score
                })
                
                await asyncio.sleep(15)
                
            except Exception as e:
                logger.error(f"Predictive loop error: {str(e)}")
                await asyncio.sleep(30)
    
    def _calculate_metric_trend(self, metric: str, window: int = 10) -> float:
        """Calculate trend (rate of change) for a metric"""
        history = list(self.metrics_history.get(metric, []))
        
        if len(history) < window:
            return 0.0
        
        values = [h['value'] for h in history[-window:]]
        if len(values) < 2:
            return 0.0
        
        slope = np.polyfit(range(len(values)), values, 1)[0]
        return slope
    
    def _is_trending_toward_threshold(self, metric: str, threshold: float, 
                                      comparison: str, window: int = 10) -> Tuple[bool, float]:
        """Check if metric is trending toward a threshold"""
        current = self._collect_health_metrics().get(metric, 0)
        trend = self._calculate_metric_trend(metric, window)
        
        if trend == 0:
            return False, float('inf')
        
        if comparison == 'below':
            if current <= threshold:
                return True, 0.0
            if trend < 0:
                seconds = (threshold - current) / trend
                return seconds > 0 and seconds < 300, max(0, seconds)
        elif comparison == 'above':
            if current >= threshold:
                return True, 0.0
            if trend > 0:
                seconds = (threshold - current) / trend
                return seconds > 0 and seconds < 300, max(0, seconds)
        
        return False, float('inf')
    
    # ========================================================================
    # Gradual Transition Loop (Enhanced)
    # ========================================================================
    
    async def _gradual_transition_loop(self):
        """Smoothly apply policy changes during transitions with user-defined speed"""
        while True:
            try:
                if self.transition_in_progress and self.target_policy:
                    # Get transition speed
                    speed_seconds = self.transition_speed_map.get(self.transition_speed, 15.0)
                    
                    if speed_seconds == 0:
                        # Instant transition
                        self.policy_transition_progress = 1.0
                    else:
                        # Increment gradually
                        increment = 0.1 / (speed_seconds / 10)
                        self.policy_transition_progress = min(1.0, 
                            self.policy_transition_progress + increment)
                    
                    # Interpolate between current and target policies
                    interpolated = {}
                    for key in self.current_policy:
                        if isinstance(self.current_policy[key], (int, float)):
                            current_val = self.current_policy[key]
                            target_val = self.target_policy[key]
                            interpolated[key] = current_val + (target_val - current_val) * self.policy_transition_progress
                        else:
                            interpolated[key] = self.target_policy[key] if self.policy_transition_progress > 0.5 else self.current_policy[key]
                    
                    self.current_policy = interpolated
                    
                    if self.policy_transition_progress >= 1.0:
                        self.transition_in_progress = False
                        self.target_policy = None
                        self.policy_transition_progress = 1.0
                        logger.debug(f"Gradual transition complete ({self.transition_speed.value})")
                
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Gradual transition error: {str(e)}")
                await asyncio.sleep(5)
    
    # ========================================================================
    # Main Monitoring Loop (Enhanced)
    # ========================================================================
    
    async def _monitoring_loop(self):
        """Enhanced monitoring with weighted scoring and anomaly awareness"""
        while True:
            try:
                # Calculate weighted health score with ML
                health_score = self.calculate_health_score()
                
                metrics = self._collect_health_metrics()
                new_tier = self._evaluate_tier_weighted(metrics)
                
                # Recovery validation
                if self.recovering_from_tier and self.recovery_validation_enabled:
                    if self._validate_recovery(self.recovering_from_tier):
                        self.recovering_from_tier = None
                        logger.info("Recovery validated")
                
                # Check if transition needed
                if new_tier != self.current_tier and not self.transition_in_progress:
                    if self._can_transition():
                        trigger_rule = self._find_triggering_rule(metrics, new_tier)
                        
                        transition_type = TransitionType.DEGRADATION if new_tier.value < self.current_tier.value else TransitionType.RECOVERY
                        
                        await self._transition_to(
                            new_tier, metrics, transition_type,
                            trigger_rule.metric if trigger_rule else 'health_score',
                            metrics.get(trigger_rule.metric if trigger_rule else 'token_balance', 0),
                            trigger_rule.enter_threshold if trigger_rule else 0
                        )
                        
                        if transition_type == TransitionType.RECOVERY:
                            self.recovering_from_tier = new_tier
                            self.recovery_validation_metrics.clear()
                
                # Collect recovery validation metrics
                if self.recovering_from_tier:
                    for key, value in metrics.items():
                        self.recovery_validation_metrics[key].append(value)
                
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Monitoring loop error: {str(e)}")
                await asyncio.sleep(30)
    
    def _evaluate_tier_weighted(self, metrics: Dict[str, float]) -> OperationalTier:
        """Evaluate tier using weighted scoring"""
        scores = {tier: 0.0 for tier in OperationalTier}
        
        for rule in self.rules:
            metric_value = metrics.get(rule.metric, 0)
            
            # Check if rule is triggered
            triggered = False
            if rule.comparison == 'above' and metric_value > rule.enter_threshold:
                triggered = True
            elif rule.comparison == 'below' and metric_value < rule.enter_threshold:
                triggered = True
            
            if triggered:
                scores[rule.target_tier] += rule.weight
        
        # Select tier with highest weighted score
        best_tier = max(scores, key=scores.get)
        
        # If no rules triggered, stay at current tier
        if scores[best_tier] == 0:
            return self.current_tier
        
        # Ensure smooth degradation (one tier at a time)
        if best_tier.value < self.current_tier.value - 1:
            best_tier = OperationalTier(self.current_tier.value - 1)
        elif best_tier.value > self.current_tier.value + 1:
            best_tier = OperationalTier(self.current_tier.value + 1)
        
        return best_tier
    
    def _find_triggering_rule(self, metrics: Dict[str, float], target_tier: OperationalTier) -> Optional[DegradationRule]:
        """Find which rule triggered the transition"""
        for rule in self.rules:
            if rule.target_tier == target_tier:
                metric_value = metrics.get(rule.metric, 0)
                
                if rule.comparison == 'above' and metric_value > rule.enter_threshold:
                    return rule
                elif rule.comparison == 'below' and metric_value < rule.enter_threshold:
                    return rule
        return None
    
    def _can_transition(self) -> bool:
        """Check if enough time has passed since last transition"""
        elapsed = datetime.utcnow() - self.last_transition_time
        return elapsed > self.transition_cooldown
    
    # ========================================================================
    # Recovery Validation with Predictive Health (Enhanced)
    # ========================================================================
    
    def _validate_recovery(self, target_tier: OperationalTier) -> bool:
        """Validate that recovery is stable before confirming"""
        if len(self.recovery_validation_metrics.get('token_balance', [])) < 10:
            return False
        
        # Check that metrics are stable in recovery range
        for rule in self.rules:
            if rule.target_tier == target_tier:
                values = self.recovery_validation_metrics.get(rule.metric, [])
                if len(values) >= 5:
                    recent_avg = np.mean(values[-5:])
                    
                    if rule.comparison == 'above' and recent_avg <= rule.exit_threshold:
                        return False
                    elif rule.comparison == 'below' and recent_avg >= rule.exit_threshold:
                        return False
        
        # Check predictive recovery validation
        current_metrics = self._collect_health_metrics()
        ml_prediction = asyncio.run(self.ml_predictor.predict(current_metrics))
        if ml_prediction.get('predicted_score', 0) < 0.4:
            return False
        
        return True
    
    # ========================================================================
    # Transition Execution (Enhanced)
    # ========================================================================
    
    async def _transition_to(self, new_tier: OperationalTier, metrics: Dict[str, float],
                            transition_type: TransitionType, trigger_metric: str,
                            trigger_value: float, trigger_threshold: float,
                            was_preemptive: bool = False, was_anomaly: bool = False):
        """Execute tier transition with user-defined speed"""
        old_tier = self.current_tier
        self.previous_tier = old_tier
        self.current_tier = new_tier
        self.last_transition_time = datetime.utcnow()
        
        # Calculate duration in previous tier
        duration = 0
        if self.tier_history:
            duration = (datetime.utcnow() - self.tier_history[-1].timestamp).total_seconds()
        
        # Create transition record
        transition = TransitionRecord(
            transition_id=f"trans_{datetime.utcnow().timestamp()}_{hashlib.md5(str(metrics).encode()).hexdigest()[:6]}",
            timestamp=datetime.utcnow(),
            transition_type=transition_type,
            from_tier=old_tier,
            to_tier=new_tier,
            trigger_metric=trigger_metric,
            trigger_value=trigger_value,
            trigger_threshold=trigger_threshold,
            health_scores=self.calculate_health_score().component_scores,
            duration_in_previous_tier=duration,
            was_preemptive=was_preemptive,
            was_anomaly=was_anomaly,
            transition_speed=self.transition_speed
        )
        
        self.tier_history.append(transition)
        
        # Start gradual policy transition
        self.target_policy = self.tier_policies[new_tier]
        self.policy_transition_progress = 0.0
        self.transition_in_progress = True
        
        # Publish event
        self._publish_transition_event(transition)
        
        # Notify callbacks
        for callback in self.tier_change_callbacks:
            try:
                await callback(old_tier, new_tier, self.current_policy)
            except Exception as e:
                logger.error(f"Tier change callback error: {str(e)}")
        
        logger.warning(
            f"TIER CHANGE [{transition_type.value}]: {old_tier.name} → {new_tier.name} "
            f"(trigger: {trigger_metric}={trigger_value:.2f}, "
            f"preemptive: {was_preemptive}, anomaly: {was_anomaly}, "
            f"speed: {self.transition_speed.value})"
        )
    
    # ========================================================================
    # Set Transition Speed (NEW)
    # ========================================================================
    
    def set_transition_speed(self, speed: TransitionSpeed):
        """Set user-defined transition speed"""
        self.transition_speed = speed
        logger.info(f"Transition speed set to {speed.value}")
    
    def get_transition_speed_options(self) -> Dict[str, float]:
        """Get available transition speed options"""
        return {s.value: self.transition_speed_map[s] for s in TransitionSpeed}
    
    # ========================================================================
    # Chaos Injection API (NEW)
    # ========================================================================
    
    async def inject_component_chaos(self, component: str, intensity: float) -> Dict[str, Any]:
        """Inject chaos into a specific component"""
        if self.current_tier.value < 4:
            return {'status': 'error', 'message': 'System not healthy enough for chaos injection'}
        
        result = await self.chaos_injector.inject_component_chaos(component, intensity)
        
        # If injection caused significant degradation, adjust tier
        if result.get('health_after', 0.5) < 0.2:
            target_tier = OperationalTier(max(1, self.current_tier.value - 1))
            await self._transition_to(
                target_tier,
                self._collect_health_metrics(),
                TransitionType.CHAOS_INDUCED,
                f'chaos_{component}',
                result['intensity'],
                0.5
            )
        
        return result
    
    async def recover_component(self, component: str) -> Dict[str, Any]:
        """Recover a component from chaos injection"""
        return await self.chaos_injector.recover_component(component)
    
    # ========================================================================
    # Chaos Engineering (Enhanced)
    # ========================================================================
    
    async def _chaos_scheduler_loop(self):
        """Automated chaos scheduling"""
        while True:
            try:
                if (self.chaos_schedule_enabled and 
                    self.current_tier == OperationalTier.TIER_5_FULL and 
                    not self.chaos_active):
                    
                    experiment_id = min(
                        self.chaos_experiments.keys(),
                        key=lambda eid: self.chaos_experiments[eid]['analytics']['run_count']
                    )
                    
                    experiment = self.chaos_experiments[experiment_id]
                    await self._run_chaos_experiment(experiment_id, experiment, intensity_index=0)
                
                await asyncio.sleep(self.chaos_schedule_interval_hours * 3600)
                
            except Exception as e:
                logger.error(f"Chaos scheduler error: {str(e)}")
                await asyncio.sleep(300)
    
    async def _run_chaos_experiment(self, experiment_id: str, experiment: Dict[str, Any],
                                   intensity_index: int = 0) -> Optional[ChaosExperimentResult]:
        """Execute chaos experiment with enhanced analytics"""
        if intensity_index >= len(experiment['intensity_levels']):
            return None
        
        intensity = experiment['intensity_levels'][intensity_index]
        
        logger.warning(f"CHAOS: {experiment['name']} Intensity {intensity:.0%} (Level {intensity_index + 1})")
        
        self.chaos_active = True
        start_time = datetime.utcnow()
        metrics_before = self._collect_chaos_metrics(experiment['metrics_collected'])
        safety_breached = False
        
        try:
            # Apply chaos (simulated)
            await asyncio.sleep(experiment['duration_per_level'])
            
            # Check safety bounds
            if self._check_chaos_safety(experiment['safety_bounds']):
                logger.warning(f"CHAOS ROLLBACK: Safety bounds exceeded")
                safety_breached = True
                await self._rollback_chaos(experiment_id)
            
            end_time = datetime.utcnow()
            metrics_after = self._collect_chaos_metrics(experiment['metrics_collected'])
            recovery_time = (end_time - start_time).total_seconds()
            
            tier_impact = self.previous_tier.value - self.current_tier.value if hasattr(self, 'previous_tier') else 0
            resilience_score = max(0, 100 - recovery_time - tier_impact * 10 - (20 if safety_breached else 0))
            
            # Track component impacts
            component_impacts = {}
            for component in self.chaos_injector.component_configs.keys():
                health = self.chaos_injector.component_health.get(component, 0.5)
                component_impacts[component] = health
            
            lessons = self._generate_chaos_lessons(experiment_id, recovery_time, safety_breached)
            
            result = ChaosExperimentResult(
                experiment_id=experiment_id,
                experiment_name=experiment['name'],
                intensity=intensity,
                start_time=start_time,
                end_time=end_time,
                recovery_time_seconds=recovery_time,
                tier_impact=tier_impact,
                safety_breached=safety_breached,
                metrics_before=metrics_before,
                metrics_after=metrics_after,
                resilience_score=resilience_score,
                recommendations=self._generate_chaos_recommendations([result]) if 'result' in locals() else [],
                lessons_learned=lessons,
                component_impacts=component_impacts
            )
            
            self.chaos_history.append(result)
            
            analytics = experiment['analytics']
            analytics['run_count'] += 1
            n = analytics['run_count']
            analytics['avg_recovery_time'] = (analytics['avg_recovery_time'] * (n - 1) + recovery_time) / n
            analytics['avg_resilience_score'] = (analytics['avg_resilience_score'] * (n - 1) + resilience_score) / n
            
            logger.info(f"CHAOS COMPLETE: {experiment['name']} - Recovery: {recovery_time:.1f}s, Score: {resilience_score:.0f}")
            
            if not safety_breached and intensity_index + 1 < len(experiment['intensity_levels']):
                await asyncio.sleep(120)
                return await self._run_chaos_experiment(experiment_id, experiment, intensity_index + 1)
            
            return result
            
        except Exception as e:
            logger.error(f"Chaos experiment failed: {str(e)}")
            await self._rollback_chaos(experiment_id)
            return None
        finally:
            self.chaos_active = False
    
    def _check_chaos_safety(self, safety_bounds: Dict[str, Any]) -> bool:
        """Check if safety bounds have been exceeded"""
        metrics = self._collect_health_metrics()
        
        for bound, value in safety_bounds.items():
            if bound == 'min_token_balance' and metrics.get('token_balance', 500) < value:
                return True
            elif bound == 'max_tier_drop':
                tier_drop = OperationalTier.TIER_5_FULL.value - self.current_tier.value
                if tier_drop > value:
                    return True
            elif bound == 'min_viable_compartments' and metrics.get('compartment_health', 0) < value:
                return True
        
        return False
    
    async def _rollback_chaos(self, experiment_id: str):
        """Rollback chaos experiment"""
        logger.warning(f"Rolling back chaos: {experiment_id}")
        self.chaos_active = False
        
        if self.current_tier.value < 4:
            await self._transition_to(
                OperationalTier.TIER_4_REDUCED,
                self._collect_health_metrics(),
                TransitionType.CHAOS_INDUCED,
                'chaos_rollback', 0, 0
            )
    
    def _collect_chaos_metrics(self, metric_names: List[str]) -> Dict[str, float]:
        """Collect metrics for chaos experiment"""
        health = self._collect_health_metrics()
        return {name: health.get(name, 0) for name in metric_names}
    
    def _generate_chaos_lessons(self, experiment_id: str, recovery_time: float, 
                                safety_breached: bool) -> List[str]:
        """Generate lessons learned from chaos experiment"""
        lessons = []
        
        if safety_breached:
            lessons.append(f"Safety bounds breached in {experiment_id}. Review thresholds.")
        
        if recovery_time > 60:
            lessons.append(f"Slow recovery ({recovery_time:.0f}s) in {experiment_id}. Optimize recovery paths.")
        elif recovery_time < 10:
            lessons.append(f"Fast recovery ({recovery_time:.0f}s) in {experiment_id}. System is resilient.")
        
        if not lessons:
            lessons.append(f"Experiment {experiment_id} completed successfully within parameters.")
        
        return lessons
    
    def _generate_chaos_recommendations(self, experiments: List[ChaosExperimentResult]) -> List[str]:
        """Generate recommendations from chaos results"""
        recommendations = []
        
        avg_recovery = np.mean([e.recovery_time_seconds for e in experiments]) if experiments else 0
        if avg_recovery > 30:
            recommendations.append(f"Improve recovery time (avg: {avg_recovery:.1f}s)")
        
        safety_breaches = sum(1 for e in experiments if e.safety_breached)
        if safety_breaches > 0:
            recommendations.append(f"Review safety bounds ({safety_breaches} breaches)")
        
        if not recommendations:
            recommendations.append("System demonstrates good resilience.")
        
        return recommendations
    
    # ========================================================================
    # Public API Methods (Enhanced)
    # ========================================================================
    
    def get_current_policy(self) -> Dict[str, Any]:
        """Get current operational policy"""
        return self.current_policy.copy()
    
    def get_tier_status(self) -> Dict[str, Any]:
        """Get comprehensive tier status with ML predictions"""
        health_score = self.calculate_health_score()
        
        return {
            'current_tier': self.current_tier.value,
            'current_tier_name': self.current_tier.name,
            'previous_tier': self.previous_tier.value,
            'policy': self.get_current_policy(),
            'health_score': health_score.overall_score,
            'health_trend': health_score.trend,
            'component_scores': health_score.component_scores,
            'predicted_tier': self.predicted_tier.value if self.predicted_tier else None,
            'time_to_predicted_tier': self.time_to_predicted_tier,
            'ml_predicted_score': health_score.ml_predicted_score,
            'ml_confidence': health_score.ml_confidence,
            'anomaly_score': health_score.anomaly_score,
            'is_anomalous': health_score.is_anomalous,
            'transition_speed': self.transition_speed.value,
            'last_transition': self.last_transition_time.isoformat(),
            'transitions_today': len([
                t for t in self.tier_history
                if t.timestamp.date() == datetime.utcnow().date()
            ]),
            'recent_transitions': [
                {
                    'from': t.from_tier.value,
                    'to': t.to_tier.value,
                    'type': t.transition_type.value,
                    'trigger': t.trigger_metric,
                    'preemptive': t.was_preemptive,
                    'anomaly': t.was_anomaly,
                    'speed': t.transition_speed.value,
                    'timestamp': t.timestamp.isoformat()
                }
                for t in self.tier_history[-10:]
            ]
        }
    
    def should_execute(self, operation_type: str) -> bool:
        """Check if operation type is allowed in current tier"""
        policy = self.get_current_policy()
        
        operation_map = {
            'expert_execution': lambda p: p['expert_activation'] in ['all', 'essential_only', 'critical_only', 'survival_only'],
            'exploration': lambda p: p['exploration_rate'] > 0,
            'biomass_storage': lambda p: p['biomass_storage'] != 'none',
            'caching': lambda p: p['cache_ttl_seconds'] > 0,
            'harvesting': lambda p: p['harvesting_rate'] > 0
        }
        
        checker = operation_map.get(operation_type)
        if checker:
            return checker(policy)
        return True
    
    def get_chaos_report(self) -> Dict[str, Any]:
        """Get chaos engineering report"""
        recent = list(self.chaos_history)[-20:]
        
        if not recent:
            return {'status': 'No experiments run', 'experiments': []}
        
        avg_recovery = np.mean([e.recovery_time_seconds for e in recent])
        avg_resilience = np.mean([e.resilience_score for e in recent])
        safety_breaches = sum(1 for e in recent if e.safety_breached)
        
        return {
            'total_experiments': len(self.chaos_history),
            'recent_experiments': [
                {
                    'name': e.experiment_name,
                    'intensity': e.intensity,
                    'recovery_time': e.recovery_time_seconds,
                    'resilience_score': e.resilience_score,
                    'safety_breached': e.safety_breached,
                    'component_impacts': e.component_impacts,
                    'timestamp': e.start_time.isoformat()
                }
                for e in recent[-5:]
            ],
            'average_recovery_time': avg_recovery,
            'average_resilience_score': avg_resilience,
            'safety_breach_rate': safety_breaches / max(len(recent), 1),
            'resilience_grade': 'A' if avg_resilience > 90 else 'B' if avg_resilience > 70 else 'C' if avg_resilience > 50 else 'D',
            'experiment_analytics': {
                eid: exp['analytics'] for eid, exp in self.chaos_experiments.items()
            },
            'chaos_injection_stats': self.chaos_injector.get_injection_stats(),
            'lessons_learned': list(set(
                lesson for e in recent for lesson in e.lessons_learned
            ))[-10:]
        }
    
    def get_prediction_report(self) -> Dict[str, Any]:
        """Get predictive degradation report"""
        return {
            'predicted_tier': self.predicted_tier.value if self.predicted_tier else None,
            'time_to_transition': self.time_to_predicted_tier,
            'health_score': self.calculate_health_score().overall_score,
            'health_trend': self._calculate_health_trend(),
            'ml_prediction': self.calculate_health_score().ml_predicted_score,
            'ml_confidence': self.calculate_health_score().ml_confidence,
            'anomaly_score': self.calculate_health_score().anomaly_score,
            'recent_predictions': list(self.prediction_history)[-20:]
        }
    
    def run_chaos_experiment_manual(self, experiment_id: str) -> Dict[str, Any]:
        """Manually trigger a chaos experiment"""
        if experiment_id not in self.chaos_experiments:
            return {'error': f'Unknown experiment: {experiment_id}'}
        
        if self.current_tier.value < 4:
            return {'error': 'System not healthy enough for chaos testing'}
        
        experiment = self.chaos_experiments[experiment_id]
        asyncio.create_task(self._run_chaos_experiment(experiment_id, experiment))
        
        return {
            'status': 'started',
            'experiment': experiment_id,
            'name': experiment['name'],
            'estimated_duration': experiment['duration_per_level'] * len(experiment['intensity_levels'])
        }
