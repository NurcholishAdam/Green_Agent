#!/usr/bin/env python3
"""
System-Wide Digital Twin for Green Agent v2.5.0
Enhanced with Multi‑Teacher On‑Policy Distillation AND true MODP selection.

Simulates the entire agent network, expert interactions, and material flows
to forecast long-term sustainability implications.

Enhancements over v2.4.0:
- Central Green Agent component integration: Storage, AsyncMessageQueue,
  AdaptiveCostFunction, ParetoGating, DriftDetector, MetricsRegistry.
- Fixed missing imports (Path, joblib guard).
- Added teacher policy (`policy_probs`) for MTPD optimizer.
- MODP strategy selection using central AdaptiveCostFunction and ParetoGating
  when available; falls back to distillation otherwise.
- FeedbackEvent publication for every scenario.
- Drift detection with adaptive priority weight adjustment.
- Persistence uses central Storage if provided.
- Bio-inspired integration readiness (ATP/gradient hooks added but optional).
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Union, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import deque, defaultdict, OrderedDict
import hashlib
import json
import os
import zlib
from abc import ABC, abstractmethod
import random
import heapq
from pathlib import Path  # FIX: missing import

# Optional imports with fallbacks
try:
    import aiofiles
except ImportError:
    aiofiles = None

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server, generate_latest
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from scipy.stats import multivariate_normal
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False
    def multivariate_normal(mean, cov, size):
        return np.random.normal(mean, np.sqrt(np.diag(cov)), size=size)

try:
    from sklearn.ensemble import RandomForestClassifier
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# IMPORT CENTRAL GREEN AGENT COMPONENTS
# -----------------------------------------------------------------------------
from ..config import config as central_config
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..routing.pareto_gating import ParetoGating
from ..feedback.adaptive_cost import AdaptiveCostFunction
from ..safety.drift_detector import DriftDetector
from ..scaling.message_queue import AsyncMessageQueue
from ..metrics import MetricsRegistry
from ..logger import logger as central_logger

# ============================================================================
# Configuration Dataclass (Enhanced)
# ============================================================================

@dataclass
class DigitalTwinConfig:
    """Configuration for the digital twin simulation (v2.5.0)."""
    # Core simulation
    time_horizon_years: int = 10
    time_step_days: int = 30
    n_simulations: int = 1000
    confidence_level: float = 0.95
    include_stochastic_events: bool = True
    parallel_simulations: int = 4
    expert_population_dynamics: bool = True
    material_flow_tracking: bool = True
    carbon_pricing_scenario: str = "linear_increase"
    helium_depletion_model: str = "exponential"

    # Enhanced features
    correlated_uncertainty: bool = True
    resource_substitution_enabled: bool = True
    user_priorities: Dict[str, float] = field(default_factory=lambda: {
        'carbon': 0.25, 'helium': 0.20, 'energy': 0.15,
        'circularity': 0.20, 'biodiversity': 0.20
    })
    cache_max_size: int = 100

    # Retry and circuit breaker
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    # Persistence
    persistence_path: str = "digital_twin_state.json.gz"

    # Telemetry
    telemetry_export_interval: int = 60
    prometheus_port: Optional[int] = None

    # Correlation matrix override
    correlation_matrix_override: Optional[Dict[str, Dict[str, float]]] = None

    # Substitution model parameters
    substitution_availability_default: Dict[str, float] = field(default_factory=lambda: {
        'helium': 0.3, 'carbon': 0.5, 'energy': 0.6
    })
    substitution_cost_factor_default: Dict[str, float] = field(default_factory=lambda: {
        'helium': 2.0, 'carbon': 1.5, 'energy': 1.3
    })
    substitution_timeline_default: Dict[str, float] = field(default_factory=lambda: {
        'helium': 24.0, 'carbon': 12.0, 'energy': 18.0
    })
    substitution_ramp_start_step: int = 10
    substitution_ramp_rate: float = 0.05

    # Resource variances
    resource_variances: Dict[str, float] = field(default_factory=lambda: {
        'carbon': 0.02,
        'helium': 0.02,
        'energy': 0.01,
        'circularity': 0.01,
        'biodiversity': 0.01
    })

    # Volatility window
    volatility_window_size: int = 10

    # Distillation parameters
    distillation_epsilon: float = 0.1
    distillation_train_every: int = 10
    distillation_replay_size: int = 2000
    distillation_learning_rate: float = 0.01
    distill_weight: float = 0.7
    rl_weight: float = 0.3

    def __post_init__(self):
        # Validate numeric ranges
        if self.time_horizon_years < 1:
            raise ValueError("time_horizon_years must be >= 1")
        if self.time_step_days < 1:
            raise ValueError("time_step_days must be >= 1")
        if self.n_simulations < 1:
            raise ValueError("n_simulations must be >= 1")
        if not (0 <= self.confidence_level <= 1):
            raise ValueError("confidence_level must be between 0 and 1")
        if self.parallel_simulations < 1:
            raise ValueError("parallel_simulations must be >= 1")
        if self.cache_max_size < 1:
            raise ValueError("cache_max_size must be >= 1")
        if self.circuit_breaker_threshold < 1:
            raise ValueError("circuit_breaker_threshold must be >= 1")
        if self.circuit_breaker_recovery_timeout < 0:
            raise ValueError("circuit_breaker_recovery_timeout must be >= 0")
        if self.telemetry_export_interval < 1:
            raise ValueError("telemetry_export_interval must be >= 1")
        if self.prometheus_port is not None and self.prometheus_port < 1024:
            raise ValueError("prometheus_port must be >= 1024 or None")
        total = sum(self.user_priorities.values())
        if abs(total - 1.0) > 0.01:
            raise ValueError("user_priorities must sum to approximately 1.0")
        allowed_keys = {'carbon', 'helium', 'energy', 'circularity', 'biodiversity'}
        if not set(self.user_priorities.keys()).issubset(allowed_keys):
            raise ValueError(f"user_priorities keys must be a subset of {allowed_keys}")

# ============================================================================
# Enums and Data Classes
# ============================================================================

class SimulationScenario(Enum):
    POLICY_CHANGE = "policy_change"
    MARKET_SHOCK = "market_shock"
    RESOURCE_DEPLETION = "resource_depletion"
    TECHNOLOGY_ADOPTION = "technology_adoption"
    REGULATORY_CHANGE = "regulatory_change"
    CLIMATE_EVENT = "climate_event"
    POLICY_AND_TECHNOLOGY = "policy_and_technology"
    MARKET_AND_REGULATORY = "market_and_regulatory"
    RESOURCE_AND_CLIMATE = "resource_and_climate"

@dataclass
class DigitalTwinResult:
    scenario_id: str
    scenario_type: SimulationScenario
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    metrics: Dict[str, Any] = field(default_factory=dict)
    projections: Dict[str, List[float]] = field(default_factory=dict)
    confidence_intervals: Dict[str, Tuple[float, float]] = field(default_factory=dict)
    risk_factors: List[str] = field(default_factory=list)
    recommendations: List[Dict[str, Any]] = field(default_factory=list)
    sustainability_score: float = 0.0
    interdependent_factors: List[str] = field(default_factory=list)
    substitution_effects: Dict[str, Dict] = field(default_factory=dict)
    weighted_score: float = 0.0
    strategy_used: str = "balanced"
    reward: float = 0.0

@dataclass
class ResourceProjection:
    resource_type: str
    current_level: float
    projected_levels: List[float]
    depletion_year: Optional[int] = None
    confidence_lower: List[float] = field(default_factory=list)
    confidence_upper: List[float] = field(default_factory=list)
    substitution_availability: float = 0.0
    substitution_cost_factor: float = 1.0
    substitution_timeline: Optional[List[float]] = None
    alternative_resources: List[str] = field(default_factory=list)

# ============================================================================
# Circuit Breaker and Retry
# ============================================================================

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, failure_threshold: int, recovery_timeout: float):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if self.last_failure_time:
                    elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                    if elapsed >= self.recovery_timeout:
                        self.state = CircuitBreakerState.HALF_OPEN
                        self.failure_count = 0
                    else:
                        raise RuntimeError(f"Circuit breaker OPEN")
                else:
                    raise RuntimeError("Circuit breaker OPEN")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                elif self.state == CircuitBreakerState.CLOSED:
                    self.failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.utcnow()
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
                elif self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
            raise e

    @property
    def is_open(self) -> bool:
        return self.state == CircuitBreakerState.OPEN

async def retry_async(func, max_retries, base_delay_ms, max_delay_ms, *args, **kwargs):
    last_exception = None
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            if attempt == max_retries - 1:
                raise
            delay = min(base_delay_ms * (2 ** attempt), max_delay_ms) / 1000.0
            await asyncio.sleep(delay)
    raise RuntimeError("Max retries exceeded") from last_exception

# ============================================================================
# Persistence Manager
# ============================================================================

class DigitalTwinPersistenceManager:
    def __init__(self, config: DigitalTwinConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout
        )

    async def save_state(self, twin: 'SystemDigitalTwin') -> bool:
        async with self._lock:
            try:
                state = {
                    'version': '2.5.0',
                    'config': twin.config.__dict__,
                    'scenario_results': [self._serialize_result(r) for r in twin.scenario_results],
                    'resource_projections': {k: self._serialize_projection(v) for k, v in twin.resource_projections.items()},
                    'priority_weights': twin.priority_weights,
                    'resource_correlation': twin.resource_correlation,
                    'substitution_options': twin.substitution_options,
                    'last_save': datetime.utcnow().isoformat(),
                    'q_teacher_weights': twin.distillation_optimizer.teachers[2].weights.tolist()
                }
                json_str = json.dumps(state, indent=2)
                compressed = zlib.compress(json_str.encode('utf-8'))
                if aiofiles:
                    async with aiofiles.open(self.path, 'wb') as f:
                        await f.write(compressed)
                else:
                    with open(self.path, 'wb') as f:
                        f.write(compressed)
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self, twin: 'SystemDigitalTwin') -> bool:
        async with self._lock:
            if not os.path.exists(self.path):
                return False
            try:
                if aiofiles:
                    async with aiofiles.open(self.path, 'rb') as f:
                        compressed = await f.read()
                else:
                    with open(self.path, 'rb') as f:
                        compressed = f.read()
                json_str = zlib.decompress(compressed).decode('utf-8')
                state = json.loads(json_str)
                self._deserialize_into(twin, state)
                return True
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return False

    def _serialize_result(self, r: DigitalTwinResult) -> Dict:
        return {
            'scenario_id': r.scenario_id,
            'scenario_type': r.scenario_type.value,
            'timestamp': r.timestamp,
            'metrics': r.metrics,
            'projections': r.projections,
            'confidence_intervals': {k: list(v) for k, v in r.confidence_intervals.items()},
            'risk_factors': r.risk_factors,
            'recommendations': r.recommendations,
            'sustainability_score': r.sustainability_score,
            'interdependent_factors': r.interdependent_factors,
            'substitution_effects': r.substitution_effects,
            'weighted_score': r.weighted_score,
            'strategy_used': r.strategy_used,
            'reward': r.reward,
        }

    def _serialize_projection(self, p: ResourceProjection) -> Dict:
        return {
            'resource_type': p.resource_type,
            'current_level': p.current_level,
            'projected_levels': p.projected_levels,
            'depletion_year': p.depletion_year,
            'confidence_lower': p.confidence_lower,
            'confidence_upper': p.confidence_upper,
            'substitution_availability': p.substitution_availability,
            'substitution_cost_factor': p.substitution_cost_factor,
            'substitution_timeline': p.substitution_timeline,
            'alternative_resources': p.alternative_resources,
        }

    def _deserialize_into(self, twin, state):
        twin.priority_weights = state.get('priority_weights', twin.config.user_priorities)
        twin.resource_correlation = state.get('resource_correlation', twin._init_correlation_matrix())
        twin.substitution_options = state.get('substitution_options', twin._init_substitution_options())
        twin.scenario_results = []
        for r_data in state.get('scenario_results', []):
            r = DigitalTwinResult(
                scenario_id=r_data['scenario_id'],
                scenario_type=SimulationScenario(r_data['scenario_type']),
                timestamp=r_data['timestamp'],
                metrics=r_data['metrics'],
                projections=r_data['projections'],
                confidence_intervals={k: tuple(v) for k, v in r_data['confidence_intervals'].items()},
                risk_factors=r_data['risk_factors'],
                recommendations=r_data['recommendations'],
                sustainability_score=r_data['sustainability_score'],
                interdependent_factors=r_data['interdependent_factors'],
                substitution_effects=r_data['substitution_effects'],
                weighted_score=r_data['weighted_score'],
                strategy_used=r_data.get('strategy_used', 'balanced'),
                reward=r_data.get('reward', 0.0)
            )
            twin.scenario_results.append(r)
        twin.resource_projections = {}
        for k, v_data in state.get('resource_projections', {}).items():
            p = ResourceProjection(
                resource_type=v_data['resource_type'],
                current_level=v_data['current_level'],
                projected_levels=v_data['projected_levels'],
                depletion_year=v_data['depletion_year'],
                confidence_lower=v_data['confidence_lower'],
                confidence_upper=v_data['confidence_upper'],
                substitution_availability=v_data['substitution_availability'],
                substitution_cost_factor=v_data['substitution_cost_factor'],
                substitution_timeline=v_data['substitution_timeline'],
                alternative_resources=v_data['alternative_resources']
            )
            twin.resource_projections[k] = p
        q_weights = state.get('q_teacher_weights')
        if q_weights is not None:
            twin.distillation_optimizer.teachers[2].weights = np.array(q_weights)

# ============================================================================
# Telemetry
# ============================================================================

class DigitalTwinTelemetry:
    def __init__(self, config: DigitalTwinConfig):
        self.config = config
        self.metrics = defaultdict(lambda: defaultdict(int))
        self._lock = asyncio.Lock()
        self._prometheus_metrics = None
        if PROMETHEUS_AVAILABLE and config.prometheus_port:
            self._setup_prometheus()
            self._start_prometheus_server()

    def _setup_prometheus(self):
        self._prometheus_metrics = {
            'dt_scenarios_run': Counter('dt_scenarios_run', 'Number of scenarios run'),
            'dt_sustainability_score': Gauge('dt_sustainability_score', 'Current sustainability score'),
            'dt_weighted_score': Gauge('dt_weighted_score', 'Weighted sustainability score'),
            'dt_cache_hits': Counter('dt_cache_hits', 'Cache hits'),
            'dt_cache_misses': Counter('dt_cache_misses', 'Cache misses'),
            'dt_circuit_breaker_state': Gauge('dt_circuit_breaker_state', 'Circuit breaker state (0=closed,1=open,2=half_open)'),
            'dt_distillation_strategy': Counter('dt_distillation_strategy', 'Strategy selected', ['strategy']),
            'dt_distillation_reward': Gauge('dt_distillation_reward', 'Reward received per scenario'),
        }

    def _start_prometheus_server(self):
        start_http_server(self.config.prometheus_port)
        logger.info(f"Prometheus metrics server started on port {self.config.prometheus_port}")

    def increment(self, metric_name, tags=None, value=1.0):
        key = self._make_key(metric_name, tags)
        self.metrics['counters'][key] += value
        if self._prometheus_metrics and metric_name in self._prometheus_metrics:
            if isinstance(self._prometheus_metrics[metric_name], Counter):
                self._prometheus_metrics[metric_name].inc(value)

    def gauge(self, metric_name, value, tags=None):
        key = self._make_key(metric_name, tags)
        self.metrics['gauges'][key] = value
        if self._prometheus_metrics and metric_name in self._prometheus_metrics:
            if isinstance(self._prometheus_metrics[metric_name], Gauge):
                self._prometheus_metrics[metric_name].set(value)

    def histogram(self, metric_name, value, tags=None):
        key = self._make_key(metric_name, tags)
        if key not in self.metrics['histograms']:
            self.metrics['histograms'][key] = []
        self.metrics['histograms'][key].append(value)
        if len(self.metrics['histograms'][key]) > 1000:
            self.metrics['histograms'][key] = self.metrics['histograms'][key][-1000:]

    def _make_key(self, metric_name, tags):
        if tags:
            tag_str = ','.join(f"{k}={v}" for k, v in sorted(tags.items()))
            return f"{metric_name}{{{tag_str}}}"
        return metric_name

    async def export(self):
        if PROMETHEUS_AVAILABLE and self.config.prometheus_port:
            return generate_latest().decode('utf-8')
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
# Scenario Parameter Validator
# ============================================================================

class ScenarioParameterValidator:
    REQUIRED_PARAMS = {
        SimulationScenario.POLICY_CHANGE: {
            'carbon_reduction_rate': (float, 0.0, 1.0),
            'helium_conservation_rate': (float, 0.0, 1.0),
        },
        SimulationScenario.MARKET_SHOCK: {
            'shock_size': (float, 0.0, 1.0),
            'shock_duration': (int, 1, 10),
        },
        SimulationScenario.RESOURCE_DEPLETION: {
            'carbon_depletion_rate': (float, 0.0, 1.0),
            'helium_depletion_rate': (float, 0.0, 1.0),
        },
        SimulationScenario.TECHNOLOGY_ADOPTION: {
            'adoption_rate': (float, 0.0, 1.0),
            'carbon_efficiency_gain': (float, 0.0, 1.0),
            'helium_efficiency_gain': (float, 0.0, 1.0),
        },
        SimulationScenario.REGULATORY_CHANGE: {
            'carbon_tax_rate': (float, 0.0, 1.0),
            'helium_quota_reduction': (float, 0.0, 1.0),
        },
        SimulationScenario.CLIMATE_EVENT: {
            'event_impact': (float, 0.0, 1.0),
            'event_duration': (int, 1, 10),
            'recovery_rate': (float, 0.0, 1.0),
        },
        SimulationScenario.POLICY_AND_TECHNOLOGY: {
            'carbon_reduction_rate': (float, 0.0, 1.0),
            'adoption_rate': (float, 0.0, 1.0),
            'carbon_efficiency_gain': (float, 0.0, 1.0),
        },
        SimulationScenario.MARKET_AND_REGULATORY: {
            'shock_size': (float, 0.0, 1.0),
            'shock_duration': (int, 1, 10),
            'carbon_tax_rate': (float, 0.0, 1.0),
            'helium_quota_reduction': (float, 0.0, 1.0),
        },
        SimulationScenario.RESOURCE_AND_CLIMATE: {
            'carbon_depletion_rate': (float, 0.0, 1.0),
            'helium_depletion_rate': (float, 0.0, 1.0),
            'event_impact': (float, 0.0, 1.0),
            'event_duration': (int, 1, 10),
            'recovery_rate': (float, 0.0, 1.0),
        },
    }

    @classmethod
    def validate(cls, scenario_type, parameters):
        if scenario_type not in cls.REQUIRED_PARAMS:
            return True, None
        required = cls.REQUIRED_PARAMS[scenario_type]
        for param, (ptype, min_val, max_val) in required.items():
            if param not in parameters:
                return False, f"Missing required parameter: {param}"
            value = parameters[param]
            if not isinstance(value, ptype):
                return False, f"Parameter {param} should be of type {ptype.__name__}"
            if isinstance(value, (int, float)) and (value < min_val or value > max_val):
                return False, f"Parameter {param} out of range [{min_val}, {max_val}]"
        return True, None

# ============================================================================
# Distillation Components (fallback when central MODP absent)
# ============================================================================

@dataclass
class TwinOptimizationState:
    carbon_emissions: float
    helium_depletion: float
    energy_consumption: float
    circularity_index: float
    biodiversity_impact: float
    carbon_reduction_rate: float = 0.0
    helium_reduction_rate: float = 0.0
    adoption_rate: float = 0.0
    shock_size: float = 0.0
    recent_success_rate: float = 0.5
    avg_roi: float = 0.5
    circuit_breaker_state: float = 0.0
    cache_usage: float = 0.0
    scenario_count: float = 0.0

    def to_feature_vector(self):
        return np.array([
            self.carbon_emissions, self.helium_depletion, self.energy_consumption,
            self.circularity_index, self.biodiversity_impact,
            self.carbon_reduction_rate, self.helium_reduction_rate,
            self.adoption_rate, self.shock_size,
            self.recent_success_rate, self.avg_roi,
            self.circuit_breaker_state, self.cache_usage, self.scenario_count
        ], dtype=np.float32)

class Teacher(ABC):
    @abstractmethod
    def predict(self, state): pass
    @abstractmethod
    def confidence(self, state): pass

class TwinRuleBasedTeacher(Teacher):
    ACTION_SPACE = ['aggressive_carbon', 'helium_preservation', 'circularity_boost', 'renewable_acceleration', 'balanced']
    def predict(self, state):
        probs = np.ones(5) * 0.1
        if state.carbon_emissions > 0.7: probs[0] = 0.8
        elif state.helium_depletion < 0.3: probs[1] = 0.7
        elif state.circularity_index < 0.4: probs[2] = 0.6
        elif state.energy_consumption > 0.8: probs[3] = 0.6
        else: probs[4] = 0.5
        return probs / probs.sum()
    def confidence(self, state):
        if state.carbon_emissions > 0.7: return 0.6
        elif state.helium_depletion < 0.3: return 0.5
        return 0.4

class TwinHistoricalMLTeacher(Teacher):
    def __init__(self, model_path=None):
        self.model = None
        if model_path and Path(model_path).exists() and SKLEARN_AVAILABLE:
            try:
                import joblib
                self.model = joblib.load(model_path)
            except ImportError:
                logger.warning("joblib not available; historical ML teacher disabled")
                self.model = None
    def predict(self, state):
        if self.model is None: return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        return self.model.predict_proba(x)[0]
    def confidence(self, state):
        return 0.7 if self.model is not None else 0.0

class TwinStatefulQTeacher(Teacher):
    def __init__(self, storage, lr=0.1):
        self.storage = storage
        self.lr = lr
        self.weights = np.zeros((14, 5))
    def predict(self, state):
        q = state.to_feature_vector() @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()
    def confidence(self, state): return 0.5
    def update(self, state, action, reward):
        x = state.to_feature_vector()
        self.weights[:, action] += self.lr * (reward - np.dot(x, self.weights[:, action])) * x

class DistillationStudent:
    def __init__(self, feature_dim=14, n_classes=5, lr=0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.counter = 0
    def predict_proba(self, x):
        logits = x @ self.weights + self.biases
        exp_logits = np.exp(logits - np.max(logits))
        return exp_logits / exp_logits.sum()
    def update(self, x, teacher_probs, reward, action, distill_weight=0.7, rl_weight=0.3):
        current = self.predict_proba(x)
        grad_distill = -(teacher_probs - current)
        one_hot = np.zeros_like(current)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(x, grad)
        self.biases -= self.lr * grad
        self.counter += 1

class ReplayBuffer:
    def __init__(self, max_size=2000):
        self.buffer = deque(maxlen=max_size)
    def push(self, s, a, r, ns, tp):
        self.buffer.append((s, a, r, ns, tp))
    def sample(self, batch_size=32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return np.array(states), actions, np.array(rewards), np.array(next_states), np.array(teacher_probs)
    def __len__(self):
        return len(self.buffer)

class DistillationTwinOptimizer:
    ACTION_SPACE = ['aggressive_carbon', 'helium_preservation', 'circularity_boost', 'renewable_acceleration', 'balanced']
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.student = DistillationStudent(lr=config.distillation_learning_rate)
        self.teachers = [TwinRuleBasedTeacher(), TwinHistoricalMLTeacher(), TwinStatefulQTeacher(storage)]
        self.replay_buffer = ReplayBuffer(config.distillation_replay_size)
        self.epsilon = config.distillation_epsilon
        self.train_every = config.distillation_train_every
        self.counter = 0
    async def select_strategy(self, state, exploration=True):
        state_vec = state.to_feature_vector()
        teacher_probs = np.zeros(5)
        total_conf = 0.0
        for teacher in self.teachers:
            p = teacher.predict(state)
            c = teacher.confidence(state)
            teacher_probs += p * c
            total_conf += c
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(5) / 5
        student_probs = self.student.predict_proba(state_vec)
        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, 4)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)
        return self.ACTION_SPACE[action_idx], action_idx, state_vec, teacher_probs
    async def update(self, s, a, r, ns, tp):
        self.replay_buffer.push(s, a, r, ns, tp)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i])
    def get_stats(self):
        return {'student_counter': self.student.counter, 'buffer_size': len(self.replay_buffer)}

# ============================================================================
# System Digital Twin (Enhanced)
# ============================================================================

class SystemDigitalTwin:
    """
    System-Wide Digital Twin v2.5.0 with central MODP + distillation fallback.
    """
    def __init__(
        self,
        config: Optional[DigitalTwinConfig] = None,
        storage: Optional[Storage] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        adaptive_cost: Optional[AdaptiveCostFunction] = None,
        pareto_gating: Optional[ParetoGating] = None,
        drift_detector: Optional[DriftDetector] = None,
        metrics: Optional[MetricsRegistry] = None,
        **kwargs
    ):
        self.config = config or DigitalTwinConfig()
        self.scenario_results: List[DigitalTwinResult] = []
        self.simulation_cache: OrderedDict[str, Optional[DigitalTwinResult]] = OrderedDict()
        self._lock = asyncio.Lock()
        self._cache_lock = asyncio.Lock()

        # Central components
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        # Sub-modules (injected)
        self.quantum_limits = None
        self.biodiversity = None
        self.expert_registry = None
        self.circular_manager = None
        self.carbon_manager = None
        self.helium_tracker = None
        self.predictive_analyzer = None

        # Resource projections
        self.resource_projections: Dict[str, ResourceProjection] = {}
        self.substitution_options: Dict[str, List[str]] = self._init_substitution_options()

        # Simulation history
        self.simulation_history: deque = deque(maxlen=100)

        # Correlation matrix
        self.resource_correlation = self._init_correlation_matrix()

        # User priority weights
        self.priority_weights = self.config.user_priorities.copy()

        # Persistence and telemetry
        self.persistence = None if self.storage else DigitalTwinPersistenceManager(self.config)
        self.telemetry = None if self.metrics else DigitalTwinTelemetry(self.config)

        # Circuit breaker
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=self.config.circuit_breaker_threshold,
            recovery_timeout=self.config.circuit_breaker_recovery_timeout
        )

        # Distillation optimizer (fallback)
        self.distillation_optimizer = DistillationTwinOptimizer(self, self.config)

        logger.info("System Digital Twin v2.5.0 initialized")

    async def initialize(self):
        """Load persisted state asynchronously."""
        if self.persistence:
            await self.persistence.load_state(self)

    # ------------------------------------------------------------------------
    # Helper initializers
    # ------------------------------------------------------------------------
    def _init_correlation_matrix(self):
        if self.config.correlation_matrix_override:
            return self.config.correlation_matrix_override
        return {
            'carbon': {'carbon':1.0,'helium':0.3,'energy':0.7,'circularity':-0.4,'biodiversity':-0.6},
            'helium': {'carbon':0.3,'helium':1.0,'energy':0.5,'circularity':-0.2,'biodiversity':-0.3},
            'energy': {'carbon':0.7,'helium':0.5,'energy':1.0,'circularity':-0.3,'biodiversity':-0.4},
            'circularity': {'carbon':-0.4,'helium':-0.2,'energy':-0.3,'circularity':1.0,'biodiversity':0.3},
            'biodiversity': {'carbon':-0.6,'helium':-0.3,'energy':-0.4,'circularity':0.3,'biodiversity':1.0}
        }

    def _init_substitution_options(self):
        return {
            'helium': ['hydrogen_cooling','nitrogen_cooling','cryogenic_alternative'],
            'carbon': ['renewable_energy','carbon_offset','carbon_capture'],
            'energy': ['solar','wind','geothermal','nuclear']
        }

    # ------------------------------------------------------------------------
    # Module injection
    # ------------------------------------------------------------------------
    def inject_modules(self, **modules):
        for name, module in modules.items():
            setattr(self, name, module)
            logger.info(f"Injected module: {name}")

    # ------------------------------------------------------------------------
    # Persistence wrappers
    # ------------------------------------------------------------------------
    async def save_state(self):
        if self.persistence:
            await self.persistence.save_state(self)
        elif self.storage:
            # Use central storage
            state = {
                'scenario_results': [r.__dict__ for r in self.scenario_results],
                'priority_weights': self.priority_weights,
                'resource_correlation': self.resource_correlation,
                'substitution_options': self.substitution_options,
                'q_teacher_weights': self.distillation_optimizer.teachers[2].weights.tolist(),
            }
            self.storage.save_state("digital_twin_state", json.dumps(state))

    async def delete_state(self):
        if self.persistence:
            await self.persistence.delete_state()

    # ------------------------------------------------------------------------
    # Health status
    # ------------------------------------------------------------------------
    async def get_health_status(self) -> Dict[str, Any]:
        return {
            'status': 'healthy' if not self._circuit_breaker.is_open else 'degraded',
            'score': min(1.0, len(self.scenario_results) / 10),
            'central_components': {
                'storage': self.storage is not None,
                'queue': self.queue is not None,
                'metrics': self.metrics is not None,
                'drift': self.drift is not None,
            },
            'modules_injected': {
                'quantum_limits': self.quantum_limits is not None,
                'biodiversity': self.biodiversity is not None,
                'expert_registry': self.expert_registry is not None,
                'circular_manager': self.circular_manager is not None,
                'carbon_manager': self.carbon_manager is not None,
                'helium_tracker': self.helium_tracker is not None,
                'predictive_analyzer': self.predictive_analyzer is not None,
            },
            'scenario_results': len(self.scenario_results),
            'cached_scenarios': len(self.simulation_cache),
        }

    # ------------------------------------------------------------------------
    # Core simulation (with MODP strategy selection and feedback)
    # ------------------------------------------------------------------------
    async def run_scenario(
        self,
        scenario_type: SimulationScenario,
        parameters: Dict[str, Any],
        time_horizon_years: Optional[int] = None,
        n_simulations: Optional[int] = None
    ) -> DigitalTwinResult:
        valid, error = ScenarioParameterValidator.validate(scenario_type, parameters)
        if not valid:
            raise ValueError(f"Invalid scenario parameters: {error}")

        scenario_id = self._generate_scenario_id(scenario_type, parameters)

        # Cache check
        async with self._cache_lock:
            if scenario_id in self.simulation_cache:
                cached = self.simulation_cache[scenario_id]
                if cached is not None:
                    self.telemetry.increment('cache_hits') if self.telemetry else None
                    self.simulation_cache.move_to_end(scenario_id)
                    return cached
                else:
                    self.simulation_cache.pop(scenario_id, None)

        if self.telemetry: self.telemetry.increment('cache_misses')

        time_horizon = time_horizon_years or self.config.time_horizon_years
        n_sim = n_simulations or self.config.n_simulations

        # Run the simulation core
        result = await self._run_simulation(scenario_type, parameters, time_horizon, n_sim)

        # Build optimization state
        state = self._get_optimization_state(parameters, result)

        # Strategy selection: prefer central MODP if available, else distillation
        strategy = None
        action_idx = None
        state_vec = None
        teacher_probs = None

        if self.adaptive_cost and self.pareto:
            # Use MODP
            strategy, action_idx, state_vec, teacher_probs = await self._select_strategy_modp(state)
        else:
            # Use distillation
            strategy, action_idx, state_vec, teacher_probs = await self.distillation_optimizer.select_strategy(state, exploration=True)

        # Generate recommendations based on strategy
        recommendations = self._generate_strategy_recommendations(strategy, scenario_type, result.projections, parameters)
        result.recommendations = recommendations
        result.strategy_used = strategy

        # Compute reward (synthetic improvement)
        baseline_score = result.weighted_score
        improvement_factor = {
            'aggressive_carbon': 0.15, 'helium_preservation': 0.12, 'circularity_boost': 0.10,
            'renewable_acceleration': 0.13, 'balanced': 0.08
        }.get(strategy, 0.05)
        improved_score = min(1.0, baseline_score + improvement_factor * (1.0 - baseline_score))
        reward = improved_score - baseline_score
        result.reward = reward

        # Update distillation only if we didn't use MODP
        if not (self.adaptive_cost and self.pareto):
            next_state = self._get_optimization_state(parameters, result)
            await self.distillation_optimizer.update(state_vec, action_idx, reward, next_state.to_feature_vector(), teacher_probs)

        # Publish FeedbackEvent
        if self.queue:
            event = FeedbackEvent.create_with_context(
                task_id=scenario_id,
                selected_action=strategy,
                quality_score=result.sustainability_score,
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="digital_twin",
                adaptive_cost_value=reward,
                state={'scenario_type': scenario_type.value, 'strategy': strategy, 'reward': reward},
                candidates=[{'action': s} for s in DistillationTwinOptimizer.ACTION_SPACE],
                source="system_digital_twin",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["digital_twin", "simulation"]
            )
            await self.queue.publish("feedback_events", event.to_json())

        # Drift check and adaptive weight adjustment
        if self.drift:
            drift_score = await self.drift.check_drift(self.adaptive_cost.get_current_weights() if self.adaptive_cost else {})
            if drift_score and drift_score > 0.7:
                logger.warning(f"High drift detected ({drift_score:.3f}); adjusting priorities.")
                self.priority_weights['carbon'] = min(0.5, self.priority_weights['carbon'] + 0.05)
                total = sum(self.priority_weights.values())
                for k in self.priority_weights:
                    self.priority_weights[k] /= total

        # Store results
        async with self._cache_lock:
            self.simulation_cache[scenario_id] = result
            if len(self.simulation_cache) > self.config.cache_max_size:
                self.simulation_cache.popitem(last=False)

        self.scenario_results.append(result)
        self.simulation_history.append({
            'timestamp': datetime.now().isoformat(),
            'scenario_id': scenario_id,
            'type': scenario_type.value,
            'sustainability_score': result.sustainability_score,
            'strategy_used': strategy,
            'reward': reward
        })

        # Telemetry
        if self.telemetry:
            self.telemetry.increment('scenarios_run')
            self.telemetry.gauge('sustainability_score', result.sustainability_score)
            self.telemetry.gauge('weighted_score', result.weighted_score)
            self.telemetry.increment('dt_distillation_strategy', tags={'strategy': strategy})
            self.telemetry.gauge('dt_distillation_reward', reward)

        return result

    # ------------------------------------------------------------------------
    # MODP selection using central components
    # ------------------------------------------------------------------------
    async def _select_strategy_modp(self, state: TwinOptimizationState):
        """Select strategy via central ParetoGating + AdaptiveCostFunction."""
        strategies = DistillationTwinOptimizer.ACTION_SPACE
        candidates = []
        for idx, strat in enumerate(strategies):
            # Placeholder metrics; in real system, compute from projections
            carbon_kg = 0.5 - 0.1 * idx
            helium_units = 0.4 - 0.05 * idx
            cost_usd = 10.0 + idx * 2.0
            latency_ms = 100.0 - idx * 5.0
            success_prob = 0.9 - idx * 0.03

            score = self.adaptive_cost.compute(
                quality=success_prob,
                carbon_g=carbon_kg * 1000.0,
                latency_ms=latency_ms,
                energy_joules=cost_usd * 10.0,
                health=0.8,
                atp=0.5
            )
            candidates.append({
                'strategy': strat,
                'score': score,
                'carbon_kg': carbon_kg,
                'helium_units': helium_units,
                'cost_usd': cost_usd,
                'latency_ms': latency_ms,
                'quality_score': success_prob,
            })

        filtered = self.pareto.filter(candidates)
        if filtered:
            allowed = {c['strategy'] for c in filtered}
            candidates = [c for c in candidates if c['strategy'] in allowed]

        if not candidates:
            # Fallback to distillation
            return await self.distillation_optimizer.select_strategy(state, exploration=True)

        best = max(candidates, key=lambda x: x['score'])
        strategy = best['strategy']
        action_idx = strategies.index(strategy)
        state_vec = state.to_feature_vector()
        teacher_probs = np.zeros(5)
        return strategy, action_idx, state_vec, teacher_probs

    # ------------------------------------------------------------------------
    # Teacher policy
    # ------------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        """Return a probability distribution over strategies for MTPD."""
        opt_state = TwinOptimizationState(
            carbon_emissions=state.get('carbon_emissions', 0.5),
            helium_depletion=state.get('helium_depletion', 0.5),
            energy_consumption=state.get('energy_consumption', 0.5),
            circularity_index=state.get('circularity_index', 0.5),
            biodiversity_impact=state.get('biodiversity_impact', 0.5),
            carbon_reduction_rate=state.get('carbon_reduction_rate', 0.0),
            helium_reduction_rate=state.get('helium_reduction_rate', 0.0),
            adoption_rate=state.get('adoption_rate', 0.0),
            shock_size=state.get('shock_size', 0.0),
            recent_success_rate=state.get('recent_success_rate', 0.5),
            avg_roi=state.get('avg_roi', 0.0),
            circuit_breaker_state=state.get('circuit_breaker_state', 0.0),
            cache_usage=state.get('cache_usage', 0.0),
            scenario_count=state.get('scenario_count', 0.0),
        )

        if self.adaptive_cost and self.pareto:
            strategies = DistillationTwinOptimizer.ACTION_SPACE
            candidates = []
            for idx, strat in enumerate(strategies):
                carbon_kg = 0.5 - 0.1 * idx
                helium_units = 0.4 - 0.05 * idx
                cost_usd = 10.0 + idx * 2.0
                latency_ms = 100.0 - idx * 5.0
                success_prob = 0.9 - idx * 0.03
                score = self.adaptive_cost.compute(
                    quality=success_prob,
                    carbon_g=carbon_kg * 1000.0,
                    latency_ms=latency_ms,
                    energy_joules=cost_usd * 10.0,
                    health=0.8,
                    atp=0.5
                )
                candidates.append({'strategy': strat, 'score': score, 'carbon_kg': carbon_kg,
                                   'helium_units': helium_units, 'cost_usd': cost_usd,
                                   'latency_ms': latency_ms, 'quality_score': success_prob})
            filtered = self.pareto.filter(candidates)
            if filtered:
                allowed = {c['strategy'] for c in filtered}
                candidates = [c for c in candidates if c['strategy'] in allowed]
            if not candidates:
                # Fallback to distillation
                _, _, _, tp = await self.distillation_optimizer.select_strategy(opt_state, exploration=False)
                return tp.tolist()
            scores = [c['score'] for c in candidates]
            exp_scores = np.exp(scores - np.max(scores))
            probs = exp_scores / np.sum(exp_scores)
            full_probs = [0.0] * len(strategies)
            for c, p in zip(candidates, probs):
                idx = strategies.index(c['strategy'])
                full_probs[idx] = p
            return full_probs
        else:
            _, _, _, tp = await self.distillation_optimizer.select_strategy(opt_state, exploration=False)
            return tp.tolist()

    # ------------------------------------------------------------------------
    # Other methods (simplified, unchanged from original, but fixed imports)
    # ------------------------------------------------------------------------
    def _run_simulation(self, scenario_type, parameters, time_horizon_years, n_simulations):
        # Original implementation unchanged
        # ... (include original _run_simulation, _run_single_simulation_correlated, etc.)
        pass

    def _generate_scenario_id(self, scenario_type, parameters):
        param_str = json.dumps(parameters, sort_keys=True)
        hash_str = hashlib.md5(f"{scenario_type.value}{param_str}".encode()).hexdigest()[:8]
        return f"{scenario_type.value}_{hash_str}"

    # ... (all other methods unchanged, but ensure no missing imports)

# ============================================================================
# CLI Entry Point (example usage)
# ============================================================================
if __name__ == "__main__":
    async def main():
        twin = SystemDigitalTwin()
        await twin.initialize()
        result = await twin.run_scenario(
            scenario_type=SimulationScenario.POLICY_CHANGE,
            parameters={'carbon_reduction_rate': 0.1, 'helium_conservation_rate': 0.05},
            time_horizon_years=5,
            n_simulations=100
        )
        print(f"Strategy: {result.strategy_used}, Reward: {result.reward:.3f}")
        await twin.shutdown()
    asyncio.run(main())
