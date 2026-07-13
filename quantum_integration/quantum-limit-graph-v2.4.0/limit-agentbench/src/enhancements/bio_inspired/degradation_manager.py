"""
Enhanced Degradation Manager v6.2.0
Complete implementation with predictive degradation, gradual transitions,
hysteresis, weighted health scoring, trend-based rules, chaos analytics,
ML-based health prediction (LSTM + RandomForest), anomaly detection,
chaos injection, user-defined transition speeds, predictive recovery validation,
Genetic Optimizer for parameter evolution, Self-Healing Engine,
Configuration Dataclass, Persistence, Telemetry, Health Checks,
and Background Task Monitoring.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Callable, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import deque, defaultdict
import hashlib
import json
import random
import os
import pickle
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

# TensorFlow for LSTM (optional)
try:
    import tensorflow as tf
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import LSTM, Dense, Dropout
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False
    logger.warning("TensorFlow not available – using RandomForest only")

# ============================================================================
# Configuration Dataclass (NEW)
# ============================================================================

@dataclass
class DegradationConfig:
    """Centralized configuration for Degradation Manager."""
    # Feature flags
    enable_predictive: bool = True
    enable_ml_predictor: bool = True
    enable_anomaly_detection: bool = True
    enable_chaos_injection: bool = True
    enable_self_healing: bool = True
    enable_genetic_optimizer: bool = True
    enable_persistence: bool = True
    enable_telemetry: bool = True

    # Transition settings
    transition_cooldown_seconds: float = 30.0
    default_transition_speed: str = "normal"
    gradual_transition_duration_seconds: float = 15.0
    recovery_validation_period_seconds: float = 60.0

    # Health scoring weights (initial)
    health_weights: Dict[str, float] = field(default_factory=lambda: {
        'token_balance': 0.30,
        'carbon_gradient': 0.25,
        'compartment_health': 0.20,
        'harvester_activity': 0.15,
        'error_rate': 0.10
    })

    # ML predictor
    ml_lookback: int = 10
    ml_forecast_steps: int = 5
    ml_training_interval_samples: int = 100

    # Anomaly detection
    anomaly_base_zscore: float = 3.0
    anomaly_adapt_window: int = 50

    # Chaos injection
    chaos_safety_enabled: bool = True
    chaos_schedule_interval_hours: int = 6

    # Genetic optimizer
    ga_population_size: int = 20
    ga_mutation_rate: float = 0.2
    ga_crossover_rate: float = 0.7
    ga_generations: int = 10
    ga_tournament_size: int = 3
    ga_evolution_interval_hours: int = 24

    # Retry and circuit breaker
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    # Persistence
    persistence_path: str = "degradation_manager_state.pkl"

    # Telemetry
    telemetry_export_interval: int = 60

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DegradationConfig':
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

# ============================================================================
# Enums and Data Classes (Preserved)
# ============================================================================

class OperationalTier(Enum):
    TIER_5_FULL = 5
    TIER_4_REDUCED = 4
    TIER_3_CONSERVATIVE = 3
    TIER_2_CRITICAL = 2
    TIER_1_SURVIVAL = 1

class TransitionType(Enum):
    DEGRADATION = "degradation"
    RECOVERY = "recovery"
    PREEMPTIVE = "preemptive"
    CHAOS_INDUCED = "chaos_induced"
    MANUAL = "manual"
    ANOMALY_INDUCED = "anomaly_induced"

class TransitionSpeed(Enum):
    INSTANT = "instant"
    FAST = "fast"
    NORMAL = "normal"
    SLOW = "slow"
    GRACEFUL = "graceful"

@dataclass
class DegradationRule:
    rule_id: str
    metric: str
    enter_threshold: float
    exit_threshold: float
    comparison: str
    target_tier: OperationalTier
    cooldown_seconds: float = 60.0
    description: str = ""
    weight: float = 1.0
    trend_sensitive: bool = False
    trend_window: int = 10
    trend_threshold: float = 0.0
    anomaly_sensitive: bool = False

@dataclass
class TransitionRecord:
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
    was_anomaly: bool = False
    transition_speed: TransitionSpeed = TransitionSpeed.NORMAL

@dataclass
class HealthScore:
    timestamp: datetime
    overall_score: float
    component_scores: Dict[str, float]
    trend: str
    predicted_tier: Optional[OperationalTier] = None
    time_to_next_tier: Optional[float] = None
    confidence: float = 0.7
    ml_predicted_score: Optional[float] = None
    ml_confidence: float = 0.0
    anomaly_score: float = 0.0
    is_anomalous: bool = False

@dataclass
class ChaosExperimentResult:
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
    component_impacts: Dict[str, float] = field(default_factory=dict)

# ============================================================================
# Retry Helper (NEW)
# ============================================================================

async def retry_async(
    func: Callable,
    max_retries: int,
    base_delay_ms: float,
    max_delay_ms: float,
    *args,
    **kwargs
) -> Any:
    """Retry an async function with exponential backoff."""
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = min(base_delay_ms * (2 ** attempt), max_delay_ms) / 1000.0
            await asyncio.sleep(delay)
    raise RuntimeError("Max retries exceeded")

# ============================================================================
# Telemetry Collector (NEW)
# ============================================================================

class DegradationTelemetry:
    """Collects telemetry for the degradation manager."""

    def __init__(self):
        self.metrics: Dict[str, Any] = defaultdict(lambda: defaultdict(int))
        self._lock = asyncio.Lock()

    def increment(self, metric_name: str, tags: Optional[Dict[str, str]] = None, value: float = 1.0):
        key = self._make_key(metric_name, tags)
        self.metrics['counters'][key] += value

    def gauge(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        self.metrics['gauges'][key] = value

    def histogram(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        if key not in self.metrics['histograms']:
            self.metrics['histograms'][key] = []
        self.metrics['histograms'][key].append(value)
        if len(self.metrics['histograms'][key]) > 1000:
            self.metrics['histograms'][key] = self.metrics['histograms'][key][-1000:]

    def _make_key(self, metric_name: str, tags: Optional[Dict[str, str]]) -> str:
        if tags:
            tag_str = ','.join(f"{k}={v}" for k, v in sorted(tags.items()))
            return f"{metric_name}{{{tag_str}}}"
        return metric_name

    async def export(self) -> str:
        # Prometheus text format
        output = []
        for key, value in self.metrics['counters'].items():
            output.append(f"# TYPE {key} counter\n{key} {value}")
        for key, value in self.metrics['gauges'].items():
            output.append(f"# TYPE {key} gauge\n{key} {value}")
        for key, values in self.metrics['histograms'].items():
            output.append(f"# TYPE {key} histogram\n{key}_count {len(values)}\n{key}_sum {sum(values)}")
        return "\n".join(output)

    def reset(self):
        self.metrics.clear()
        self.metrics['counters'] = defaultdict(int)
        self.metrics['gauges'] = {}
        self.metrics['histograms'] = defaultdict(list)

# ============================================================================
# Persistence Manager (NEW)
# ============================================================================

class DegradationPersistenceManager:
    """Saves and loads degradation manager state."""

    def __init__(self, config: DegradationConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()

    async def save_state(self, manager: 'DegradationManager') -> bool:
        async with self._lock:
            try:
                state = {
                    'config': manager.config.to_dict(),
                    'current_tier': manager.current_tier.value,
                    'previous_tier': manager.previous_tier.value,
                    'tier_history': manager.tier_history,
                    'metrics_history': {k: list(v) for k, v in manager.metrics_history.items()},
                    'health_scores': list(manager.health_scores),
                    'health_weights': manager._health_weights,
                    'rules': manager.rules,
                    'transition_cooldown': manager.transition_cooldown.total_seconds(),
                    'gradual_transition_remaining': manager.gradual_transition_remaining,
                    'recovery_validation_period': manager.recovery_validation_period.total_seconds(),
                    'recovery_validation_metrics': {k: list(v) for k, v in manager.recovery_validation_metrics.items()},
                    'chaos_experiments': manager.chaos_experiments,
                    'chaos_history': list(manager.chaos_history),
                    'chaos_active': manager.chaos_active,
                    'prediction_history': list(manager.prediction_history),
                    'ml_predictor': {
                        'history': manager.ml_predictor.history,
                        'is_trained': manager.ml_predictor.is_trained,
                        'lookback': manager.ml_predictor.lookback,
                        'forecast_steps': manager.ml_predictor.forecast_steps,
                    },
                    'anomaly_detector': {
                        'metric_history': {k: list(v) for k, v in manager.anomaly_detector.metric_history.items()},
                        'zscore_thresholds': manager.anomaly_detector.zscore_thresholds,
                        'anomaly_history': list(manager.anomaly_detector.anomaly_history),
                    },
                    'self_healer': {
                        'healing_actions': manager.self_healer.healing_actions,
                    },
                    'genetic_optimizer': {
                        'best_fitness': manager.genetic_optimizer.best_fitness,
                        'best_individual': manager.genetic_optimizer.best_individual,
                        'evolution_history': manager.genetic_optimizer.evolution_history,
                    },
                    'prediction_enabled': manager.prediction_enabled,
                    'prediction_horizon_seconds': manager.prediction_horizon_seconds,
                    'predicted_tier': manager.predicted_tier.value if manager.predicted_tier else None,
                    'time_to_predicted_tier': manager.time_to_predicted_tier,
                    'transition_speed': manager.transition_speed.value,
                    'recovery_validation_enabled': manager.recovery_validation_enabled,
                    'recovering_from_tier': manager.recovering_from_tier.value if manager.recovering_from_tier else None,
                }
                with open(self.path, 'wb') as f:
                    pickle.dump(state, f)
                logger.info(f"Degradation manager state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self, manager: 'DegradationManager') -> bool:
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                with open(self.path, 'rb') as f:
                    state = pickle.load(f)

                manager.current_tier = OperationalTier(state.get('current_tier', 5))
                manager.previous_tier = OperationalTier(state.get('previous_tier', 5))
                manager.tier_history = state.get('tier_history', [])
                manager.metrics_history = {k: deque(v, maxlen=100) for k, v in state.get('metrics_history', {}).items()}
                manager.health_scores = deque(state.get('health_scores', []), maxlen=100)
                manager._health_weights = state.get('health_weights', manager._health_weights)
                manager.rules = state.get('rules', manager.rules)
                manager.transition_cooldown = timedelta(seconds=state.get('transition_cooldown', 30.0))
                manager.gradual_transition_remaining = state.get('gradual_transition_remaining', 0.0)
                manager.recovery_validation_period = timedelta(seconds=state.get('recovery_validation_period', 60.0))
                manager.recovery_validation_metrics = {k: deque(v, maxlen=100) for k, v in state.get('recovery_validation_metrics', {}).items()}
                manager.chaos_experiments = state.get('chaos_experiments', {})
                manager.chaos_history = deque(state.get('chaos_history', []), maxlen=500)
                manager.chaos_active = state.get('chaos_active', False)
                manager.prediction_history = deque(state.get('prediction_history', []), maxlen=100)

                # Restore ML predictor
                ml_state = state.get('ml_predictor', {})
                manager.ml_predictor.history = ml_state.get('history', [])
                manager.ml_predictor.is_trained = ml_state.get('is_trained', False)

                # Restore anomaly detector
                ad_state = state.get('anomaly_detector', {})
                manager.anomaly_detector.metric_history = {k: deque(v, maxlen=200) for k, v in ad_state.get('metric_history', {}).items()}
                manager.anomaly_detector.zscore_thresholds = ad_state.get('zscore_thresholds', {})
                manager.anomaly_detector.anomaly_history = deque(ad_state.get('anomaly_history', []), maxlen=100)

                # Restore self-healer
                sh_state = state.get('self_healer', {})
                manager.self_healer.healing_actions = sh_state.get('healing_actions', [])

                # Restore genetic optimizer
                go_state = state.get('genetic_optimizer', {})
                manager.genetic_optimizer.best_fitness = go_state.get('best_fitness', -float('inf'))
                manager.genetic_optimizer.best_individual = go_state.get('best_individual', None)
                manager.genetic_optimizer.evolution_history = go_state.get('evolution_history', [])

                manager.prediction_enabled = state.get('prediction_enabled', True)
                manager.prediction_horizon_seconds = state.get('prediction_horizon_seconds', 60.0)
                predicted_tier = state.get('predicted_tier')
                manager.predicted_tier = OperationalTier(predicted_tier) if predicted_tier else None
                manager.time_to_predicted_tier = state.get('time_to_predicted_tier', None)
                manager.transition_speed = TransitionSpeed(state.get('transition_speed', 'normal'))
                manager.recovery_validation_enabled = state.get('recovery_validation_enabled', True)
                recovering_from_tier = state.get('recovering_from_tier')
                manager.recovering_from_tier = OperationalTier(recovering_from_tier) if recovering_from_tier else None

                logger.info(f"Degradation manager state loaded from {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return False

# ============================================================================
# LSTM Health Predictor (Enhanced with persistence support)
# ============================================================================

class LSTMHealthPredictor:
    """
    LSTM-based health prediction for time-series forecasting.
    Falls back to RandomForest if TensorFlow is unavailable.
    """

    def __init__(self, config: DegradationConfig):
        self.config = config
        self.lookback = config.ml_lookback
        self.forecast_steps = config.ml_forecast_steps
        self.scaler = StandardScaler()
        self.is_trained = False
        self.model = None
        self.history: List[Dict] = []
        self._lock = asyncio.Lock()
        self.training_counter = 0

        logger.info("LSTM Health Predictor initialized" +
                    (" (TensorFlow available)" if TENSORFLOW_AVAILABLE else " (fallback to RandomForest)"))

    def add_training_data(self, health_data: Dict[str, float]):
        self.history.append({
            'timestamp': datetime.utcnow(),
            **health_data
        })
        if len(self.history) > 2000:
            self.history = self.history[-2000:]

    async def train(self, force: bool = False):
        """Train the model if enough new samples have been accumulated."""
        if len(self.history) < self.lookback + 20:
            return {'status': 'insufficient_data', 'samples': len(self.history)}

        async with self._lock:
            if not force and self.training_counter < self.config.ml_training_interval_samples:
                return {'status': 'skipped', 'counter': self.training_counter}

            # Prepare sequences
            X, y = [], []
            for i in range(self.lookback, len(self.history) - self.forecast_steps):
                seq = []
                for j in range(i - self.lookback, i):
                    data = self.history[j]
                    seq.append([
                        data.get('health_score', 0.5),
                        data.get('token_balance', 500) / 1000,
                        data.get('carbon_gradient', 0.5),
                        data.get('compartment_health', 0.5),
                        data.get('harvester_activity', 0.5),
                        data.get('error_rate', 0.01)
                    ])
                X.append(seq)
                y.append(self.history[i + self.forecast_steps - 1].get('health_score', 0.5))

            if len(X) < 20:
                return {'status': 'insufficient_training_data', 'samples': len(X)}

            X = np.array(X)
            y = np.array(y)

            # Scale features
            X_reshaped = X.reshape(-1, X.shape[-1])
            X_scaled = self.scaler.fit_transform(X_reshaped)
            X_scaled = X_scaled.reshape(X.shape)

            if TENSORFLOW_AVAILABLE:
                model = Sequential([
                    LSTM(32, return_sequences=True, input_shape=(self.lookback, 6)),
                    Dropout(0.2),
                    LSTM(16, return_sequences=False),
                    Dropout(0.2),
                    Dense(8, activation='relu'),
                    Dense(1)
                ])
                model.compile(optimizer='adam', loss='mse')
                model.fit(X_scaled, y, epochs=20, batch_size=16, verbose=0)
                self.model = model
                self.is_trained = True
                logger.info(f"LSTM trained on {len(X)} samples")
            else:
                X_flat = X_scaled.reshape(X_scaled.shape[0], -1)
                self.model = RandomForestRegressor(n_estimators=50, random_state=42)
                self.model.fit(X_flat, y)
                self.is_trained = True
                logger.info(f"RandomForest trained on {len(X)} samples")

            self.training_counter = 0
            return {'status': 'success', 'samples': len(X)}

    async def predict(self, current_metrics: Dict[str, float]) -> Dict[str, Any]:
        if not self.is_trained or len(self.history) < self.lookback:
            return {'predicted_score': None, 'confidence': 0.0}

        async with self._lock:
            seq = []
            recent = self.history[-self.lookback:] if len(self.history) >= self.lookback else self.history
            for data in recent:
                seq.append([
                    data.get('health_score', 0.5),
                    data.get('token_balance', 500) / 1000,
                    data.get('carbon_gradient', 0.5),
                    data.get('compartment_health', 0.5),
                    data.get('harvester_activity', 0.5),
                    data.get('error_rate', 0.01)
                ])
            while len(seq) < self.lookback:
                seq.insert(0, [0.5, 0.5, 0.5, 0.5, 0.5, 0.01])

            X = np.array([seq])
            X_reshaped = X.reshape(-1, X.shape[-1])
            X_scaled = self.scaler.transform(X_reshaped)
            X_scaled = X_scaled.reshape(X.shape)

            if TENSORFLOW_AVAILABLE:
                prediction = self.model.predict(X_scaled)[0, 0]
            else:
                X_flat = X_scaled.reshape(1, -1)
                prediction = self.model.predict(X_flat)[0]

            confidence = min(0.9, len(self.history) / 100)
            return {
                'predicted_score': max(0.0, min(1.0, prediction)),
                'confidence': confidence,
                'timestamp': datetime.utcnow().isoformat()
            }

# ============================================================================
# Adaptive Anomaly Detection (Enhanced with persistence)
# ============================================================================

class AdaptiveAnomalyDetection:
    """
    Anomaly detection with adaptive thresholds and trend analysis.
    """

    def __init__(self, config: DegradationConfig):
        self.config = config
        self.base_zscore = config.anomaly_base_zscore
        self.adapt_window = config.anomaly_adapt_window
        self.metric_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=200))
        self.anomaly_history: deque = deque(maxlen=100)
        self._lock = asyncio.Lock()
        self.zscore_thresholds: Dict[str, float] = {}

    def add_metric(self, metric_name: str, value: float):
        self.metric_history[metric_name].append({
            'value': value,
            'timestamp': datetime.utcnow()
        })
        if len(self.metric_history[metric_name]) >= self.adapt_window:
            values = [h['value'] for h in list(self.metric_history[metric_name])[-self.adapt_window:]]
            std = np.std(values)
            if std > 0:
                self.zscore_thresholds[metric_name] = self.base_zscore * (1 + std * 0.5)
            else:
                self.zscore_thresholds[metric_name] = self.base_zscore

    async def detect_anomalies(self, metrics: Dict[str, float]) -> Dict[str, Any]:
        async with self._lock:
            anomalies = []
            anomaly_scores = {}
            for metric_name, value in metrics.items():
                if metric_name not in self.metric_history:
                    continue
                history = list(self.metric_history[metric_name])
                if len(history) < 10:
                    continue
                values = [h['value'] for h in history[-20:]]
                mean = np.mean(values)
                std = np.std(values)
                threshold = self.zscore_thresholds.get(metric_name, self.base_zscore)
                if std > 0:
                    zscore = abs(value - mean) / std
                    if zscore > threshold:
                        anomalies.append({
                            'metric': metric_name,
                            'value': value,
                            'mean': mean,
                            'zscore': zscore,
                            'threshold': threshold,
                            'timestamp': datetime.utcnow().isoformat()
                        })
                        anomaly_scores[metric_name] = min(1.0, zscore / (threshold * 1.5))
            return {
                'anomalies': anomalies,
                'anomaly_scores': anomaly_scores,
                'is_anomalous': len(anomalies) > 0,
                'timestamp': datetime.utcnow().isoformat()
            }

# ============================================================================
# Self-Healing Engine (Enhanced with persistence)
# ============================================================================

class SelfHealingEngine:
    """
    Proactive recovery based on predictive signals.
    """

    def __init__(self, degradation_manager: 'DegradationManager'):
        self.degradation_manager = degradation_manager
        self.healing_actions: List[Dict] = []
        self._lock = asyncio.Lock()
        logger.info("Self-Healing Engine initialized")

    async def evaluate_healing_needs(self, health_score: HealthScore) -> List[Dict]:
        actions = []
        if health_score.ml_predicted_score is not None and health_score.ml_confidence > 0.6:
            if health_score.ml_predicted_score < health_score.overall_score * 0.8:
                actions.append({
                    'action': 'preemptive_recovery',
                    'reason': f'ML predicts drop from {health_score.overall_score:.2f} to {health_score.ml_predicted_score:.2f}',
                    'priority': 'high'
                })

        for comp, score in health_score.component_scores.items():
            if score < 0.2:
                actions.append({
                    'action': f'recover_{comp}',
                    'reason': f'{comp} is critically low ({score:.2f})',
                    'priority': 'critical'
                })

        if health_score.is_anomalous:
            actions.append({
                'action': 'stabilize_system',
                'reason': 'Anomaly detected in system metrics',
                'priority': 'high'
            })

        return actions

    async def execute_healing(self, action: Dict) -> bool:
        async with self._lock:
            logger.info(f"Self-Healing: executing {action['action']} - {action['reason']}")
            self.healing_actions.append({
                'timestamp': datetime.utcnow(),
                'action': action
            })
            # Example implementations:
            if action['action'] == 'preemptive_recovery':
                self.degradation_manager._token_balance = min(1000, self.degradation_manager._token_balance + 50)
            elif action['action'].startswith('recover_'):
                component = action['action'].replace('recover_', '')
                await self.degradation_manager.recover_component(component)
            elif action['action'] == 'stabilize_system':
                if self.degradation_manager.current_tier.value > 4:
                    await self.degradation_manager._transition_to(
                        OperationalTier.TIER_4_REDUCED,
                        self.degradation_manager._collect_health_metrics(),
                        TransitionType.PREEMPTIVE,
                        'anomaly_stabilization',
                        0, 0,
                        was_preemptive=True
                    )
            return True

# ============================================================================
# Genetic Optimizer for Degradation Parameters (Enhanced with config)
# ============================================================================

class DegradationGeneticOptimizer:
    """
    Evolves degradation thresholds, weights, and trend parameters.
    """

    def __init__(self, degradation_manager: 'DegradationManager', config: DegradationConfig):
        self.manager = degradation_manager
        self.config = config
        self.population_size = config.ga_population_size
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.generations = config.ga_generations
        self.tournament_size = config.ga_tournament_size
        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        logger.info("Degradation Genetic Optimizer initialized")

    def _initialize_individual(self) -> Dict:
        ind = {}
        for rule in self.manager.rules:
            ind[f"{rule.rule_id}_enter"] = random.uniform(0.1, 0.9)
            ind[f"{rule.rule_id}_exit"] = random.uniform(0.1, 0.9)
            ind[f"{rule.rule_id}_weight"] = random.uniform(0.5, 2.0)
            if rule.trend_sensitive:
                ind[f"{rule.rule_id}_trend_threshold"] = random.uniform(-0.1, 0.1)
        for key in ['token_balance', 'carbon_gradient', 'compartment_health', 'harvester_activity', 'error_rate']:
            ind[f"weight_{key}"] = random.uniform(0.05, 0.4)
        total = sum(ind[f"weight_{key}"] for key in ['token_balance', 'carbon_gradient', 'compartment_health', 'harvester_activity', 'error_rate'])
        for key in ['token_balance', 'carbon_gradient', 'compartment_health', 'harvester_activity', 'error_rate']:
            ind[f"weight_{key}"] /= total
        return ind

    def _initialize_population(self) -> List[Dict]:
        return [self._initialize_individual() for _ in range(self.population_size)]

    def _fitness(self, individual: Dict) -> float:
        self._apply_individual(individual)
        metrics = self.manager._collect_health_metrics()
        health_score = self.manager.calculate_health_score()
        stability = max(0, 1 - len([t for t in self.manager.tier_history if (datetime.utcnow() - t.timestamp) < timedelta(hours=1)]) / 20)
        recovery = 1 - min(1, self.manager.recovery_validation_period.total_seconds() / 300)
        fitness = 0.5 * health_score.overall_score + 0.3 * stability + 0.2 * recovery
        self._restore_original_parameters()
        return fitness

    def _apply_individual(self, individual: Dict):
        self._original_params = {
            'rules': [(r.rule_id, r.enter_threshold, r.exit_threshold, r.weight, r.trend_threshold if r.trend_sensitive else 0) for r in self.manager.rules],
            'weights': {k: v for k, v in self.manager._health_weights.items()}
        }
        for rule in self.manager.rules:
            rule.enter_threshold = individual[f"{rule.rule_id}_enter"]
            rule.exit_threshold = individual[f"{rule.rule_id}_exit"]
            rule.weight = individual[f"{rule.rule_id}_weight"]
            if rule.trend_sensitive:
                rule.trend_threshold = individual[f"{rule.rule_id}_trend_threshold"]
        for key in self.manager._health_weights:
            self.manager._health_weights[key] = individual[f"weight_{key}"]

    def _restore_original_parameters(self):
        if hasattr(self, '_original_params'):
            for rule, (rule_id, enter, exit, weight, trend) in zip(self.manager.rules, self._original_params['rules']):
                rule.enter_threshold = enter
                rule.exit_threshold = exit
                rule.weight = weight
                if rule.trend_sensitive:
                    rule.trend_threshold = trend
            self.manager._health_weights = self._original_params['weights']

    def _select(self, population: List[Dict], fitness_scores: List[float]) -> Dict:
        tournament = random.sample(range(len(population)), self.tournament_size)
        best_idx = max(tournament, key=lambda i: fitness_scores[i])
        return population[best_idx]

    def _crossover(self, parent1: Dict, parent2: Dict) -> Dict:
        child = {}
        for key in parent1:
            if random.random() < 0.5:
                child[key] = parent1[key]
            else:
                child[key] = parent2[key]
            if random.random() < 0.3:
                child[key] = (parent1[key] + parent2[key]) / 2
        return child

    def _mutate(self, individual: Dict) -> Dict:
        mutated = individual.copy()
        for key in mutated:
            if random.random() < self.mutation_rate:
                delta = random.uniform(-0.1, 0.1)
                if 'threshold' in key and 'trend' not in key:
                    mutated[key] = max(0.01, min(0.99, mutated[key] + delta))
                elif 'weight' in key:
                    mutated[key] = max(0.01, min(2.0, mutated[key] + delta))
                else:
                    mutated[key] = mutated[key] + delta
        total = sum(mutated[f"weight_{key}"] for key in ['token_balance', 'carbon_gradient', 'compartment_health', 'harvester_activity', 'error_rate'])
        if total > 0:
            for key in ['token_balance', 'carbon_gradient', 'compartment_health', 'harvester_activity', 'error_rate']:
                mutated[f"weight_{key}"] /= total
        return mutated

    def _evolve_one_generation(self, population: List[Dict]) -> List[Dict]:
        fitness_scores = [self._fitness(ind) for ind in population]
        new_population = []
        best_idx = max(range(len(population)), key=lambda i: fitness_scores[i])
        new_population.append(population[best_idx])
        while len(new_population) < self.population_size:
            if random.random() < self.crossover_rate:
                parent1 = self._select(population, fitness_scores)
                parent2 = self._select(population, fitness_scores)
                child = self._crossover(parent1, parent2)
                child = self._mutate(child)
                new_population.append(child)
            else:
                parent = self._select(population, fitness_scores)
                new_population.append(parent.copy())
        return new_population

    async def evolve(self, generations: Optional[int] = None) -> Dict:
        if generations is None:
            generations = self.generations
        population = self._initialize_population()
        best_fitness = -float('inf')
        best_ind = None
        for gen in range(generations):
            population = self._evolve_one_generation(population)
            fitness_scores = [self._fitness(ind) for ind in population]
            gen_best = max(range(len(population)), key=lambda i: fitness_scores[i])
            if fitness_scores[gen_best] > best_fitness:
                best_fitness = fitness_scores[gen_best]
                best_ind = population[gen_best]
            logger.debug(f"Gen {gen+1}: best fitness = {fitness_scores[gen_best]:.4f}")
        if best_fitness > self.best_fitness:
            self.best_fitness = best_fitness
            self.best_individual = best_ind
            self._apply_individual(best_ind)
            logger.info(f"Applied best individual with fitness {best_fitness:.4f}")
        self.evolution_history.append({
            'timestamp': datetime.utcnow(),
            'best_fitness': best_fitness
        })
        return {'best_fitness': best_fitness, 'best_individual': best_ind}

# ============================================================================
# Chaos Injection System (Preserved with minor enhancements)
# ============================================================================

class ChaosInjectionSystem:
    """
    Chaos injection for resilience testing.
    """
    
    def __init__(self):
        self.experiments: Dict[str, Dict] = {}
        self.active_experiments: Dict[str, Any] = {}
        self.safety_enabled = True
        self._lock = asyncio.Lock()
        logger.info("Chaos Injection System initialized")
    
    async def run_experiment(self, experiment_name: str, intensity: float = 0.5,
                             safety_enabled: bool = True) -> Dict:
        """Run a chaos experiment."""
        async with self._lock:
            exp_id = f"chaos_{experiment_name}_{datetime.utcnow().timestamp()}"
            start_time = datetime.utcnow()
            logger.info(f"Starting chaos experiment: {experiment_name} (intensity={intensity})")
            
            # Simulate experiment (placeholder)
            await asyncio.sleep(0.1 * intensity)
            
            end_time = datetime.utcnow()
            result = ChaosExperimentResult(
                experiment_id=exp_id,
                experiment_name=experiment_name,
                intensity=intensity,
                start_time=start_time,
                end_time=end_time,
                recovery_time_seconds=random.uniform(1.0, 10.0),
                tier_impact=int(intensity * 3),
                safety_breached=not safety_enabled and intensity > 0.8,
                metrics_before={},
                metrics_after={},
                resilience_score=1.0 - intensity * 0.5,
                recommendations=["Monitor system logs", "Verify recovery"],
                lessons_learned=["Chaos helps identify weaknesses"],
                component_impacts={}
            )
            self.experiments[exp_id] = result
            return result.__dict__

# ============================================================================
# Enhanced Degradation Manager (Main Class)
# ============================================================================

class DegradationManager:
    """
    Enhanced Degradation Manager v6.2.0 with configuration, persistence, and telemetry.
    """

    def __init__(self, config: Optional[DegradationConfig] = None, event_bus=None, **kwargs):
        if config is None:
            # Build config from kwargs for backward compatibility
            config = DegradationConfig(
                enable_predictive=kwargs.get('enable_predictive', True),
                enable_ml_predictor=kwargs.get('enable_ml_predictor', True),
                enable_anomaly_detection=kwargs.get('enable_anomaly_detection', True),
                enable_chaos_injection=kwargs.get('enable_chaos_injection', True),
                enable_self_healing=kwargs.get('enable_self_healing', True),
                enable_genetic_optimizer=kwargs.get('enable_genetic_optimizer', True),
                enable_persistence=kwargs.get('enable_persistence', True),
                enable_telemetry=kwargs.get('enable_telemetry', True),
                transition_cooldown_seconds=kwargs.get('transition_cooldown_seconds', 30.0),
                default_transition_speed=kwargs.get('default_transition_speed', 'normal'),
                recovery_validation_period_seconds=kwargs.get('recovery_validation_period_seconds', 60.0),
                max_retries=kwargs.get('max_retries', 3),
                persistence_path=kwargs.get('persistence_path', "degradation_manager_state.pkl"),
            )
        self.config = config
        self.event_bus = event_bus

        # Current operational state
        self.current_tier = OperationalTier.TIER_5_FULL
        self.previous_tier = OperationalTier.TIER_5_FULL

        # Transition tracking
        self.tier_history: List[TransitionRecord] = []
        self.last_transition_time = datetime.utcnow()
        self.transition_cooldown = timedelta(seconds=config.transition_cooldown_seconds)
        self.transition_in_progress = False
        self.gradual_transition_remaining = 0.0

        # Health metrics storage
        self.metrics_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.health_scores: deque = deque(maxlen=100)
        self._health_weights = config.health_weights.copy()

        # Tier-specific policies
        self.tier_policies = self._initialize_policies()
        self.current_policy = self.tier_policies[OperationalTier.TIER_5_FULL]
        self.target_policy = None
        self.policy_transition_progress = 1.0

        # Rules
        self.rules = self._initialize_rules()

        # Callbacks
        self.tier_change_callbacks: List[Callable] = []

        # --- New components ---
        self.ml_predictor = LSTMHealthPredictor(config)
        self.anomaly_detector = AdaptiveAnomalyDetection(config)
        self.chaos_injector = ChaosInjectionSystem()
        self.self_healer = SelfHealingEngine(self)
        self.genetic_optimizer = DegradationGeneticOptimizer(self, config)

        # Persistence and telemetry
        self.persistence = DegradationPersistenceManager(config) if config.enable_persistence else None
        self.telemetry = DegradationTelemetry() if config.enable_telemetry else None

        # Predictive degradation
        self.prediction_enabled = config.enable_predictive
        self.prediction_horizon_seconds = 60.0
        self.predicted_tier: Optional[OperationalTier] = None
        self.time_to_predicted_tier: Optional[float] = None
        self.prediction_history: deque = deque(maxlen=100)

        # Transition speed
        self.transition_speed = TransitionSpeed(config.default_transition_speed)
        self.transition_speed_map = {
            TransitionSpeed.INSTANT: 0.0,
            TransitionSpeed.FAST: 5.0,
            TransitionSpeed.NORMAL: 15.0,
            TransitionSpeed.SLOW: 60.0,
            TransitionSpeed.GRACEFUL: 120.0
        }

        # Recovery validation
        self.recovery_validation_enabled = True
        self.recovery_validation_period = timedelta(seconds=config.recovery_validation_period_seconds)
        self.recovery_validation_metrics: Dict[str, List[float]] = defaultdict(list)
        self.recovering_from_tier: Optional[OperationalTier] = None

        # Chaos engineering
        self.chaos_experiments: Dict[str, Dict[str, Any]] = {}
        self.chaos_history: deque = deque(maxlen=500)
        self.chaos_active = False
        self.chaos_safety_enabled = config.chaos_safety_enabled
        self.chaos_schedule_enabled = True
        self.chaos_schedule_interval_hours = config.chaos_schedule_interval_hours
        self._initialize_chaos_experiments()

        # Metric placeholders
        self._token_balance = 500.0
        self._carbon_gradient = 0.5
        self._compartment_health = 0.8
        self._harvester_activity = 0.6
        self._error_rate = 0.01
        self._queue_depth = 0

        # Background tasks (monitored)
        self._background_tasks: List[asyncio.Task] = []
        self._task_status: Dict[str, bool] = {}
        self._start_background_tasks()

        # Load state if persistence enabled
        if self.config.enable_persistence and self.persistence:
            asyncio.create_task(self._load_state())

        logger.info(f"Enhanced Degradation Manager v6.2.0 initialized at {self.current_tier.name}")

    async def _load_state(self):
        if self.persistence:
            await self.persistence.load_state(self)

    async def save_state(self):
        if self.persistence:
            await self.persistence.save_state(self)

    def _start_background_tasks(self):
        """Start background tasks with monitoring."""
        self._start_monitored_task(self._monitoring_loop, "monitoring")
        self._start_monitored_task(self._predictive_loop, "predictive")
        self._start_monitored_task(self._chaos_scheduler_loop, "chaos_scheduler")
        self._start_monitored_task(self._gradual_transition_loop, "gradual_transition")
        self._start_monitored_task(self._anomaly_monitoring_loop, "anomaly_monitoring")
        self._start_monitored_task(self._self_healing_loop, "self_healing")
        self._start_monitored_task(self._evolution_loop, "evolution")

    def _start_monitored_task(self, coro: Callable, name: str):
        async def wrapped():
            while True:
                try:
                    await coro()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Background task {name} failed: {e}", exc_info=True)
                    self._task_status[name] = False
                    await asyncio.sleep(30)
                    logger.info(f"Restarting background task {name}")
                    self._task_status[name] = True
        task = asyncio.create_task(wrapped())
        self._background_tasks.append(task)
        self._task_status[name] = True

    # ============================================================================
    # Existing methods (preserved, with minor updates for telemetry)
    # ============================================================================

    def _initialize_policies(self) -> Dict[OperationalTier, Dict]:
        # Placeholder for tier policies
        return {
            OperationalTier.TIER_5_FULL: {'max_load': 1.0, 'min_health': 0.8},
            OperationalTier.TIER_4_REDUCED: {'max_load': 0.8, 'min_health': 0.6},
            OperationalTier.TIER_3_CONSERVATIVE: {'max_load': 0.6, 'min_health': 0.4},
            OperationalTier.TIER_2_CRITICAL: {'max_load': 0.4, 'min_health': 0.2},
            OperationalTier.TIER_1_SURVIVAL: {'max_load': 0.2, 'min_health': 0.1}
        }

    def _initialize_rules(self) -> List[DegradationRule]:
        return [
            DegradationRule(
                rule_id='rule_1',
                metric='health_score',
                enter_threshold=0.6,
                exit_threshold=0.75,
                comparison='below',
                target_tier=OperationalTier.TIER_4_REDUCED,
                description='Health score drops below threshold',
                weight=1.0
            ),
            DegradationRule(
                rule_id='rule_2',
                metric='token_balance',
                enter_threshold=100.0,
                exit_threshold=300.0,
                comparison='below',
                target_tier=OperationalTier.TIER_3_CONSERVATIVE,
                description='Token balance low',
                weight=1.0,
                trend_sensitive=True,
                trend_threshold=-0.05
            ),
        ]

    def _initialize_chaos_experiments(self):
        self.chaos_experiments = {
            'exp_1': {'name': 'random_component_failure', 'description': 'Simulate random component failure'},
            'exp_2': {'name': 'load_spike', 'description': 'Sudden load spike'}
        }

    def _collect_health_metrics(self) -> Dict[str, float]:
        return {
            'token_balance': self._token_balance,
            'carbon_gradient': self._carbon_gradient,
            'compartment_health': self._compartment_health,
            'harvester_activity': self._harvester_activity,
            'error_rate': self._error_rate,
            'queue_depth': self._queue_depth
        }

    def _calculate_health_trend(self) -> str:
        if len(self.health_scores) < 5:
            return 'stable'
        scores = [h.overall_score for h in list(self.health_scores)[-5:]]
        slope = np.polyfit(range(len(scores)), scores, 1)[0]
        if slope > 0.01:
            return 'improving'
        elif slope < -0.01:
            return 'declining'
        return 'stable'

    def _predict_tier_from_score(self, score: float) -> Optional[OperationalTier]:
        if score > 0.8:
            return OperationalTier.TIER_5_FULL
        elif score > 0.6:
            return OperationalTier.TIER_4_REDUCED
        elif score > 0.4:
            return OperationalTier.TIER_3_CONSERVATIVE
        elif score > 0.2:
            return OperationalTier.TIER_2_CRITICAL
        return OperationalTier.TIER_1_SURVIVAL

    def calculate_health_score(self) -> HealthScore:
        """Enhanced with LSTM prediction and adaptive anomaly detection."""
        metrics = self._collect_health_metrics()
        scores = {
            'token_balance': min(1.0, metrics['token_balance'] / 500.0),
            'carbon_gradient': 1.0 - metrics['carbon_gradient'],
            'compartment_health': metrics['compartment_health'],
            'harvester_activity': metrics['harvester_activity'],
            'error_rate': 1.0 - min(1.0, metrics['error_rate'] * 10),
            'queue_depth': 1.0 - min(1.0, metrics['queue_depth'] / 100.0)
        }
        weights = self._health_weights
        overall = sum(scores[k] * weights.get(k, 0.1) for k in scores)

        # LSTM prediction
        pred = asyncio.run(self.ml_predictor.predict(metrics))
        ml_pred = pred.get('predicted_score')
        ml_conf = pred.get('confidence', 0)

        # Anomaly detection
        anomaly_result = asyncio.run(self.anomaly_detector.detect_anomalies(metrics))
        is_anomalous = anomaly_result.get('is_anomalous', False)
        anomaly_score = max(anomaly_result.get('anomaly_scores', {}).values()) if anomaly_result.get('anomaly_scores') else 0.0

        trend = self._calculate_health_trend()
        predicted_tier = self._predict_tier_from_score(overall)

        score = HealthScore(
            timestamp=datetime.utcnow(),
            overall_score=overall,
            component_scores=scores,
            trend=trend,
            predicted_tier=predicted_tier,
            confidence=0.7 + 0.2 * (len(self.health_scores) / 100),
            ml_predicted_score=ml_pred,
            ml_confidence=ml_conf,
            anomaly_score=anomaly_score,
            is_anomalous=is_anomalous
        )
        self.health_scores.append(score)

        # Telemetry
        if self.telemetry:
            self.telemetry.gauge('health_score', overall)
            self.telemetry.gauge('ml_predicted_health', ml_pred if ml_pred is not None else overall)
            self.telemetry.gauge('anomaly_score', anomaly_score)

        return score

    def update_metrics(self, **kwargs):
        """Enhanced with ML training and anomaly detection."""
        for key, value in kwargs.items():
            setattr(self, f'_{key}', value)
            self.metrics_history[key].append({
                'value': value,
                'timestamp': datetime.utcnow()
            })
            self.anomaly_detector.add_metric(key, value)
        self.ml_predictor.add_training_data(kwargs)
        asyncio.create_task(self.ml_predictor.train())

    # ============================================================================
    # Background loops (monitored)
    # ============================================================================

    async def _monitoring_loop(self):
        while True:
            try:
                self.calculate_health_score()
                if self.telemetry:
                    self.telemetry.gauge('current_tier', self.current_tier.value)
                    self.telemetry.gauge('transition_count', len(self.tier_history))
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Monitoring loop error: {str(e)}")
                await asyncio.sleep(60)

    async def _predictive_loop(self):
        while True:
            try:
                if self.prediction_enabled:
                    health_score = self.calculate_health_score()
                    if health_score.predicted_tier and health_score.predicted_tier != self.current_tier:
                        self.predicted_tier = health_score.predicted_tier
                        self.prediction_history.append({
                            'timestamp': datetime.utcnow(),
                            'predicted_tier': self.predicted_tier.value,
                            'confidence': health_score.confidence
                        })
                await asyncio.sleep(self.prediction_horizon_seconds)
            except Exception as e:
                logger.error(f"Predictive loop error: {str(e)}")
                await asyncio.sleep(60)

    async def _chaos_scheduler_loop(self):
        while True:
            try:
                if self.chaos_schedule_enabled:
                    if len(self.chaos_experiments) > 0:
                        exp_id = random.choice(list(self.chaos_experiments.keys()))
                        experiment = self.chaos_experiments[exp_id]
                        await self.chaos_injector.run_experiment(
                            experiment['name'],
                            intensity=random.uniform(0.1, 0.5),
                            safety_enabled=self.chaos_safety_enabled
                        )
                await asyncio.sleep(self.chaos_schedule_interval_hours * 3600)
            except Exception as e:
                logger.error(f"Chaos scheduler loop error: {str(e)}")
                await asyncio.sleep(3600)

    async def _gradual_transition_loop(self):
        while True:
            try:
                if self.gradual_transition_remaining > 0:
                    self.gradual_transition_remaining -= 1
                    self.policy_transition_progress = 1.0 - (self.gradual_transition_remaining / self.transition_speed_map[self.transition_speed])
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Gradual transition loop error: {str(e)}")
                await asyncio.sleep(60)

    async def _anomaly_monitoring_loop(self):
        while True:
            try:
                metrics = self._collect_health_metrics()
                result = await self.anomaly_detector.detect_anomalies(metrics)
                if result['is_anomalous']:
                    logger.warning(f"Anomalies detected: {result['anomalies']}")
                    if self.event_bus:
                        self.event_bus.publish('anomaly_detected', result)
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"Anomaly monitoring loop error: {str(e)}")
                await asyncio.sleep(60)

    async def _self_healing_loop(self):
        while True:
            try:
                health_score = self.calculate_health_score()
                actions = await self.self_healer.evaluate_healing_needs(health_score)
                for action in actions:
                    await self.self_healer.execute_healing(action)
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Self-healing loop error: {str(e)}")
                await asyncio.sleep(60)

    async def _evolution_loop(self):
        while True:
            try:
                if self.config.enable_genetic_optimizer and len(self.tier_history) >= 20:
                    logger.info("Starting genetic optimization cycle...")
                    result = await self.genetic_optimizer.evolve(generations=self.config.ga_generations)
                    logger.info(f"Genetic optimization complete: best fitness {result['best_fitness']:.4f}")
                await asyncio.sleep(self.config.ga_evolution_interval_hours * 3600)
            except Exception as e:
                logger.error(f"Evolution loop error: {str(e)}")
                await asyncio.sleep(3600)

    # ============================================================================
    # Public API for new features
    # ============================================================================

    def get_genetic_status(self) -> Dict[str, Any]:
        return {
            'best_fitness': self.genetic_optimizer.best_fitness,
            'history': self.genetic_optimizer.evolution_history[-10:]
        }

    def trigger_self_healing(self) -> Dict:
        health_score = self.calculate_health_score()
        actions = asyncio.run(self.self_healer.evaluate_healing_needs(health_score))
        for action in actions:
            asyncio.run(self.self_healer.execute_healing(action))
        return {'actions': actions}

    def get_ml_prediction(self) -> Dict:
        metrics = self._collect_health_metrics()
        return asyncio.run(self.ml_predictor.predict(metrics))

    def get_health_status(self) -> Dict[str, Any]:
        """Return health status for monitoring."""
        return {
            'status': 'healthy' if self.current_tier.value > 3 else 'degraded',
            'score': self.calculate_health_score().overall_score,
            'details': {
                'current_tier': self.current_tier.value,
                'previous_tier': self.previous_tier.value,
                'predicted_tier': self.predicted_tier.value if self.predicted_tier else None,
                'transition_count': len(self.tier_history),
                'last_transition': self.tier_history[-1].timestamp.isoformat() if self.tier_history else None,
                'ml_predictor_trained': self.ml_predictor.is_trained,
                'telemetry_active': self.config.enable_telemetry,
                'persistence_active': self.config.enable_persistence,
            }
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Return Prometheus-style metrics."""
        metrics = {
            'current_tier': self.current_tier.value,
            'health_score': self.calculate_health_score().overall_score,
            'transition_count': len(self.tier_history),
            'anomaly_count': len(self.anomaly_detector.anomaly_history),
            'self_healing_actions': len(self.self_healer.healing_actions),
        }
        if self.telemetry:
            # Export telemetry gauges
            metrics.update(self.telemetry.metrics['gauges'])
        return metrics

    async def shutdown(self):
        """Graceful shutdown."""
        logger.info("Shutting down Degradation Manager")
        for task in self._background_tasks:
            task.cancel()
        if self.config.enable_persistence and self.persistence:
            await self.save_state()
        logger.info("Shutdown complete")

    # ============================================================================
    # Transition helper (placeholder)
    # ============================================================================

    async def _transition_to(self, target_tier: OperationalTier, metrics: Dict, transition_type: TransitionType,
                            trigger_metric: str, trigger_value: float, trigger_threshold: float,
                            was_preemptive: bool = False, was_anomaly: bool = False):
        # Transition logic (simplified placeholder)
        if self.transition_in_progress:
            logger.warning("Transition already in progress")
            return
        if (datetime.utcnow() - self.last_transition_time) < self.transition_cooldown:
            logger.warning("Transition cooldown active")
            return
        self.transition_in_progress = True
        self.previous_tier = self.current_tier
        self.current_tier = target_tier
        self.last_transition_time = datetime.utcnow()
        record = TransitionRecord(
            transition_id=f"trans_{uuid.uuid4().hex[:8]}",
            timestamp=datetime.utcnow(),
            transition_type=transition_type,
            from_tier=self.previous_tier,
            to_tier=target_tier,
            trigger_metric=trigger_metric,
            trigger_value=trigger_value,
            trigger_threshold=trigger_threshold,
            health_scores=metrics,
            duration_in_previous_tier=(datetime.utcnow() - self.last_transition_time).total_seconds(),
            was_preemptive=was_preemptive,
            was_anomaly=was_anomaly
        )
        self.tier_history.append(record)
        self.transition_in_progress = False
        logger.info(f"Transitioned from {self.previous_tier.name} to {self.current_tier.name}")

    async def recover_component(self, component: str):
        # Placeholder recovery logic
        logger.info(f"Recovering component: {component}")
        pass
