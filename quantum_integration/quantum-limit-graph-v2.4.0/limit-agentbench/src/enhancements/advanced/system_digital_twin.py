#!/usr/bin/env python3
"""
System-Wide Digital Twin for Green Agent v2.4.0
Enhanced with Multi‑Teacher On‑Policy Distillation for Adaptive Recommendations

Simulates the entire agent network, expert interactions, and material flows
to forecast long-term sustainability implications.

Enhanced Features (v2.4.0):
- Multi‑Teacher Distillation replaces static recommendation heuristics.
- State‑aware strategy selection for recommendation generation.
- Online learning from scenario outcomes.
- Teachers: rule‑based, historical ML, stateful Q.
- Student: linear softmax with distillation + REINFORCE.
- Experience replay and mini‑batch updates.
- Reward based on sustainability improvement.
- All previous features (v2.3.0) retained.
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
    # Fallback: use independent normal distributions
    def multivariate_normal(mean, cov, size):
        # Generate independent samples (ignoring correlation)
        if size == 1:
            return np.random.normal(mean, np.sqrt(np.diag(cov)))
        else:
            return np.random.normal(mean, np.sqrt(np.diag(cov)), size=size)

try:
    from sklearn.ensemble import RandomForestClassifier
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration Dataclass (Enhanced)
# ============================================================================

@dataclass
class DigitalTwinConfig:
    """Configuration for the digital twin simulation (v2.4.0)."""
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
    cache_max_size: int = 100  # LRU cache size
    
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
    prometheus_port: Optional[int] = None  # if set, start HTTP server
    
    # Correlation matrix override (optional)
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

    # Resource variances for correlated uncertainty
    resource_variances: Dict[str, float] = field(default_factory=lambda: {
        'carbon': 0.02,
        'helium': 0.02,
        'energy': 0.01,
        'circularity': 0.01,
        'biodiversity': 0.01
    })

    # Volatility window for risk detection
    volatility_window_size: int = 10

    # NEW: Distillation parameters
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
        # Validate user priorities sum to ~1.0 and keys are known
        total = sum(self.user_priorities.values())
        if abs(total - 1.0) > 0.01:
            raise ValueError("user_priorities must sum to approximately 1.0")
        allowed_keys = {'carbon', 'helium', 'energy', 'circularity', 'biodiversity'}
        if not set(self.user_priorities.keys()).issubset(allowed_keys):
            raise ValueError(f"user_priorities keys must be a subset of {allowed_keys}")

# ============================================================================
# Enums and Data Classes (Enhanced)
# ============================================================================

class SimulationScenario(Enum):
    """Types of simulation scenarios."""
    POLICY_CHANGE = "policy_change"
    MARKET_SHOCK = "market_shock"
    RESOURCE_DEPLETION = "resource_depletion"
    TECHNOLOGY_ADOPTION = "technology_adoption"
    REGULATORY_CHANGE = "regulatory_change"
    CLIMATE_EVENT = "climate_event"
    # Interdependent scenarios
    POLICY_AND_TECHNOLOGY = "policy_and_technology"
    MARKET_AND_REGULATORY = "market_and_regulatory"
    RESOURCE_AND_CLIMATE = "resource_and_climate"

@dataclass
class DigitalTwinResult:
    """Result of a digital twin simulation (enhanced)."""
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
    # NEW: strategy used and reward
    strategy_used: str = "balanced"
    reward: float = 0.0

@dataclass
class ResourceProjection:
    """Projection for a specific resource with substitution modeling."""
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
# Circuit Breaker (with half-open state)
# ============================================================================

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    """Circuit breaker with half-open state for external calls."""
    def __init__(self, failure_threshold: int, recovery_timeout: float):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute the given async function with circuit breaker protection."""
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if self.last_failure_time:
                    elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                    if elapsed >= self.recovery_timeout:
                        self.state = CircuitBreakerState.HALF_OPEN
                        self.failure_count = 0
                        logger.info("Circuit breaker entered HALF_OPEN state")
                    else:
                        raise RuntimeError(f"Circuit breaker OPEN (recovery in {self.recovery_timeout - elapsed:.1f}s)")
                else:
                    raise RuntimeError("Circuit breaker OPEN (no failure time)")

        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    logger.info("Circuit breaker closed after successful half-open call")
                elif self.state == CircuitBreakerState.CLOSED:
                    self.failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.utcnow()
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker opened due to failure in half-open state: {e}")
                elif self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
            raise e

    @property
    def is_open(self) -> bool:
        return self.state == CircuitBreakerState.OPEN

    async def reset(self):
        async with self._lock:
            self.state = CircuitBreakerState.CLOSED
            self.failure_count = 0
            self.last_failure_time = None
            logger.info("Circuit breaker manually reset")

# ============================================================================
# Retry Helper (Enhanced)
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
# Persistence Manager (JSON + zlib + async I/O)
# ============================================================================

class DigitalTwinPersistenceManager:
    """Manages persistence of digital twin state using JSON + compression."""

    def __init__(self, config: DigitalTwinConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=config.circuit_breaker_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout
        )
        logger.info(f"DigitalTwinPersistenceManager initialized (path={self.path})")

    async def save_state(self, twin: 'SystemDigitalTwin') -> bool:
        """Save the twin state to disk."""
        async with self._lock:
            try:
                state = {
                    'version': '2.4.0',
                    'config': twin.config.__dict__,
                    'scenario_results': [
                        {
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
                        for r in twin.scenario_results
                    ],
                    'resource_projections': {
                        k: {
                            'resource_type': v.resource_type,
                            'current_level': v.current_level,
                            'projected_levels': v.projected_levels,
                            'depletion_year': v.depletion_year,
                            'confidence_lower': v.confidence_lower,
                            'confidence_upper': v.confidence_upper,
                            'substitution_availability': v.substitution_availability,
                            'substitution_cost_factor': v.substitution_cost_factor,
                            'substitution_timeline': v.substitution_timeline,
                            'alternative_resources': v.alternative_resources,
                        }
                        for k, v in twin.resource_projections.items()
                    },
                    'priority_weights': twin.priority_weights,
                    'resource_correlation': twin.resource_correlation,
                    'substitution_options': twin.substitution_options,
                    'last_save': datetime.utcnow().isoformat(),
                    # NEW: distillation state (Q-teacher weights)
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
                logger.info(f"Digital twin state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self, twin: 'SystemDigitalTwin') -> bool:
        """Load the twin state from disk."""
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
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

                # Version check
                version = state.get('version', '1.0.0')
                if version != '2.4.0':
                    logger.warning(f"State version mismatch: {version} != 2.4.0; attempting to load anyway")

                # Restore simple attributes
                twin.priority_weights = state.get('priority_weights', twin.config.user_priorities)
                twin.resource_correlation = state.get('resource_correlation', twin._init_correlation_matrix())
                twin.substitution_options = state.get('substitution_options', twin._init_substitution_options())

                # Restore scenario results
                for r_data in state.get('scenario_results', []):
                    result = DigitalTwinResult(
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
                    twin.scenario_results.append(result)

                # Restore resource projections
                for k, v_data in state.get('resource_projections', {}).items():
                    proj = ResourceProjection(
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
                    twin.resource_projections[k] = proj

                # Restore Q-teacher weights (if present)
                q_weights = state.get('q_teacher_weights')
                if q_weights is not None:
                    # The StatefulQTeacher is the third teacher (index 2)
                    twin.distillation_optimizer.teachers[2].weights = np.array(q_weights)

                logger.info(f"Digital twin state loaded from {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return False

    async def delete_state(self):
        async with self._lock:
            if os.path.exists(self.path):
                if aiofiles:
                    await aiofiles.os.remove(self.path)
                else:
                    os.remove(self.path)
                logger.info(f"Persistence file {self.path} deleted")
                return True
            return False

# ============================================================================
# Telemetry Collector (Prometheus-ready)
# ============================================================================

class DigitalTwinTelemetry:
    """Collects telemetry for the digital twin, with Prometheus integration."""

    def __init__(self, config: DigitalTwinConfig):
        self.config = config
        self.metrics: Dict[str, Any] = defaultdict(lambda: defaultdict(int))
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
            # Distillation metrics
            'dt_distillation_strategy': Counter('dt_distillation_strategy', 'Strategy selected', ['strategy']),
            'dt_distillation_reward': Gauge('dt_distillation_reward', 'Reward received per scenario'),
        }

    def _start_prometheus_server(self):
        start_http_server(self.config.prometheus_port)
        logger.info(f"Prometheus metrics server started on port {self.config.prometheus_port}")

    def increment(self, metric_name: str, tags: Optional[Dict[str, str]] = None, value: float = 1.0):
        key = self._make_key(metric_name, tags)
        self.metrics['counters'][key] += value
        if self._prometheus_metrics and metric_name in self._prometheus_metrics:
            if isinstance(self._prometheus_metrics[metric_name], Counter):
                self._prometheus_metrics[metric_name].inc(value)

    def gauge(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None):
        key = self._make_key(metric_name, tags)
        self.metrics['gauges'][key] = value
        if self._prometheus_metrics and metric_name in self._prometheus_metrics:
            if isinstance(self._prometheus_metrics[metric_name], Gauge):
                self._prometheus_metrics[metric_name].set(value)

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
        if PROMETHEUS_AVAILABLE and self.config.prometheus_port:
            return generate_latest().decode('utf-8')
        # Fallback text format
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
# Scenario Parameter Validator (Enhanced)
# ============================================================================

class ScenarioParameterValidator:
    """Validates scenario parameters for each scenario type."""

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
    def validate(cls, scenario_type: SimulationScenario, parameters: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate parameters for a given scenario type."""
        if scenario_type not in cls.REQUIRED_PARAMS:
            return True, None  # No validation defined

        required = cls.REQUIRED_PARAMS[scenario_type]
        for param, (param_type, min_val, max_val) in required.items():
            if param not in parameters:
                return False, f"Missing required parameter: {param}"
            value = parameters[param]
            if not isinstance(value, param_type):
                return False, f"Parameter {param} should be of type {param_type.__name__}"
            if isinstance(value, (int, float)):
                if value < min_val or value > max_val:
                    return False, f"Parameter {param} out of range [{min_val}, {max_val}]"
        return True, None

# ============================================================================
# NEW: Distillation Components for Recommendation Strategy Selection
# ============================================================================

@dataclass
class TwinOptimizationState:
    """State for the distillation agent."""
    # Sustainability metrics (normalized)
    carbon_emissions: float
    helium_depletion: float
    energy_consumption: float
    circularity_index: float
    biodiversity_impact: float
    # Scenario parameters (normalized)
    carbon_reduction_rate: float = 0.0
    helium_reduction_rate: float = 0.0
    adoption_rate: float = 0.0
    shock_size: float = 0.0
    # Historical performance
    recent_success_rate: float = 0.5
    avg_roi: float = 0.5
    # System health
    circuit_breaker_state: float = 0.0  # 0=closed, 1=half, 2=open
    cache_usage: float = 0.0
    scenario_count: float = 0.0

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 14‑dim numeric feature vector."""
        features = [
            self.carbon_emissions,
            self.helium_depletion,
            self.energy_consumption,
            self.circularity_index,
            self.biodiversity_impact,
            self.carbon_reduction_rate,
            self.helium_reduction_rate,
            self.adoption_rate,
            self.shock_size,
            self.recent_success_rate,
            self.avg_roi,
            self.circuit_breaker_state,
            self.cache_usage,
            self.scenario_count,
        ]
        return np.array(features, dtype=np.float32)


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: TwinOptimizationState) -> np.ndarray:
        """Return probability vector over 5 strategies."""
        pass

    @abstractmethod
    def confidence(self, state: TwinOptimizationState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class TwinRuleBasedTeacher(Teacher):
    """Rule‑based expert: uses heuristics to select strategy."""
    ACTION_SPACE = ['aggressive_carbon', 'helium_preservation', 'circularity_boost', 'renewable_acceleration', 'balanced']

    def predict(self, state: TwinOptimizationState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if state.carbon_emissions > 0.7:
            probs[0] = 0.8   # aggressive_carbon
        elif state.helium_depletion < 0.3:
            probs[1] = 0.7   # helium_preservation
        elif state.circularity_index < 0.4:
            probs[2] = 0.6   # circularity_boost
        elif state.energy_consumption > 0.8:
            probs[3] = 0.6   # renewable_acceleration
        else:
            probs[4] = 0.5   # balanced
        return probs / probs.sum()

    def confidence(self, state: TwinOptimizationState) -> float:
        if state.carbon_emissions > 0.7:
            return 0.6
        elif state.helium_depletion < 0.3:
            return 0.5
        return 0.4


class TwinHistoricalMLTeacher(Teacher):
    """Offline trained classifier on historical optimal actions."""
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        if model_path and Path(model_path).exists() and SKLEARN_AVAILABLE:
            import joblib
            self.model = joblib.load(model_path)

    def predict(self, state: TwinOptimizationState) -> np.ndarray:
        if self.model is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: TwinOptimizationState) -> float:
        return 0.7 if self.model is not None else 0.0


class TwinStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, storage: 'SystemDigitalTwin', lr: float = 0.1):
        self.storage = storage
        self.lr = lr
        self.weights = np.zeros((14, 5))  # 14 features, 5 actions
        self._load_state()

    def _load_state(self):
        # The storage is the digital twin itself; we use a simple in‑memory state.
        # In a real system, we'd load from persistence.
        pass  # weights are loaded via persistence manager

    def _save_state(self):
        # We don't need to save here; persistence manager handles it.
        pass

    def predict(self, state: TwinOptimizationState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: TwinOptimizationState) -> float:
        return 0.5

    def update(self, state: TwinOptimizationState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        # No need to save here; will be saved at the end of scenario.


class DistillationStudent:
    """Linear softmax student updated via distillation + policy gradient."""
    def __init__(self, feature_dim: int = 14, n_classes: int = 5, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray) -> np.ndarray:
        logits = state_vector @ self.weights + self.biases
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
        current_probs = self.predict_proba(state_vector)
        logits = state_vector @ self.weights + self.biases

        # Distillation gradient (KL divergence)
        grad_distill = -(teacher_probs - current_probs)

        # Policy gradient (REINFORCE)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)

        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1


class ReplayBuffer:
    def __init__(self, max_size: int = 2000):
        self.buffer = deque(maxlen=max_size)

    def push(self, state_vec: np.ndarray, action: int, reward: float,
             next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))

    def sample(self, batch_size: int = 32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), actions, np.array(rewards),
                np.array(next_states), np.array(teacher_probs))

    def __len__(self):
        return len(self.buffer)


class DistillationTwinOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for recommendation strategy selection.
    """
    ACTION_SPACE = ['aggressive_carbon', 'helium_preservation', 'circularity_boost', 'renewable_acceleration', 'balanced']

    def __init__(self, storage: 'SystemDigitalTwin', config: DigitalTwinConfig):
        self.storage = storage
        self.config = config
        self.student = DistillationStudent(lr=config.distillation_learning_rate)
        self.teachers: List[Teacher] = [
            TwinRuleBasedTeacher(),
            TwinHistoricalMLTeacher(),  # optionally load model
            TwinStatefulQTeacher(storage)
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.distillation_replay_size)
        self.epsilon = config.distillation_epsilon
        self.train_every = config.distillation_train_every
        self.counter = 0

    async def select_strategy(self, state: TwinOptimizationState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()

        # Ensemble teachers
        teacher_probs = np.zeros(5)
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(state)
            conf = teacher.confidence(state)
            teacher_probs += prob * conf
            total_conf += conf
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

    async def update(self, state_vec: np.ndarray, action_idx: int, reward: float,
                     next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1

        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i])

        # Update StatefulQTeacher if we have the original state (done in main loop)
        # We'll update it separately in the main loop.

    def get_stats(self) -> Dict:
        return {
            'student_counter': self.student.counter,
            'buffer_size': len(self.replay_buffer),
            'weights_norm': float(np.linalg.norm(self.student.weights))
        }


# ============================================================================
# System Digital Twin (Enhanced)
# ============================================================================

class SystemDigitalTwin:
    """
    System-Wide Digital Twin v2.4.0 with Multi‑Teacher Distillation.
    """

    def __init__(self, config: Optional[DigitalTwinConfig] = None):
        self.config = config or DigitalTwinConfig()
        self.scenario_results: List[DigitalTwinResult] = []
        self.simulation_cache: OrderedDict[str, Optional[DigitalTwinResult]] = OrderedDict()
        self._lock = asyncio.Lock()
        self._cache_lock = asyncio.Lock()

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
        self.persistence = DigitalTwinPersistenceManager(self.config)
        self.telemetry = DigitalTwinTelemetry(self.config)

        # Circuit breaker
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=self.config.circuit_breaker_threshold,
            recovery_timeout=self.config.circuit_breaker_recovery_timeout
        )

        # NEW: Distillation optimizer
        self.distillation_optimizer = DistillationTwinOptimizer(self, self.config)

        # No background loading in __init__; use initialize() instead.
        logger.info("System Digital Twin v2.4.0 initialized (call initialize() to load state)")

    async def initialize(self):
        """Load persisted state asynchronously."""
        if self.persistence:
            await self.persistence.load_state(self)
            logger.info("Digital twin state loaded")

    def _init_correlation_matrix(self) -> Dict[str, Dict[str, float]]:
        """Initialize correlation matrix, possibly from config override."""
        if self.config.correlation_matrix_override:
            return self.config.correlation_matrix_override
        return {
            'carbon': {'carbon': 1.0, 'helium': 0.3, 'energy': 0.7, 'circularity': -0.4, 'biodiversity': -0.6},
            'helium': {'carbon': 0.3, 'helium': 1.0, 'energy': 0.5, 'circularity': -0.2, 'biodiversity': -0.3},
            'energy': {'carbon': 0.7, 'helium': 0.5, 'energy': 1.0, 'circularity': -0.3, 'biodiversity': -0.4},
            'circularity': {'carbon': -0.4, 'helium': -0.2, 'energy': -0.3, 'circularity': 1.0, 'biodiversity': 0.3},
            'biodiversity': {'carbon': -0.6, 'helium': -0.3, 'energy': -0.4, 'circularity': 0.3, 'biodiversity': 1.0}
        }

    def _init_substitution_options(self) -> Dict[str, List[str]]:
        return {
            'helium': ['hydrogen_cooling', 'nitrogen_cooling', 'cryogenic_alternative'],
            'carbon': ['renewable_energy', 'carbon_offset', 'carbon_capture'],
            'energy': ['solar', 'wind', 'geothermal', 'nuclear']
        }

    async def save_state(self):
        if self.persistence:
            await self.persistence.save_state(self)

    async def delete_state(self):
        if self.persistence:
            await self.persistence.delete_state()

    async def get_health_status(self) -> Dict[str, Any]:
        """Report health of the digital twin."""
        return {
            'status': 'healthy' if not self._circuit_breaker.is_open else 'degraded',
            'score': min(1.0, (len(self.scenario_results) / 10) if self.scenario_results else 0.5),
            'details': {
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
                'persistence_enabled': self.persistence is not None,
                'telemetry_active': True,
                'circuit_open': self._circuit_breaker.is_open,
            }
        }

    # ========================================================================
    # Module Injection
    # ========================================================================

    def inject_modules(self, **modules):
        """Inject required system modules."""
        for name, module in modules.items():
            setattr(self, name, module)
            logger.info(f"Injected module: {name}")

    # ========================================================================
    # Core Simulation Methods (Enhanced)
    # ========================================================================

    async def run_scenario(
        self,
        scenario_type: SimulationScenario,
        parameters: Dict[str, Any],
        time_horizon_years: Optional[int] = None,
        n_simulations: Optional[int] = None
    ) -> DigitalTwinResult:
        """
        Run a simulation scenario on the digital twin, using distillation to select
        the recommendation strategy and learn from the outcome.
        """
        # Validate parameters
        valid, error = ScenarioParameterValidator.validate(scenario_type, parameters)
        if not valid:
            raise ValueError(f"Invalid scenario parameters: {error}")

        async with self._lock:
            scenario_id = self._generate_scenario_id(scenario_type, parameters)

            # Check cache (with LRU)
            async with self._cache_lock:
                if scenario_id in self.simulation_cache:
                    cached = self.simulation_cache[scenario_id]
                    if cached is not None:
                        self.telemetry.increment('cache_hits')
                        logger.info(f"Returning cached simulation for {scenario_id}")
                        self.simulation_cache.move_to_end(scenario_id)
                        return cached
                    else:
                        self.simulation_cache.pop(scenario_id, None)

            self.telemetry.increment('cache_misses')

            time_horizon = time_horizon_years or self.config.time_horizon_years
            n_sim = n_simulations or self.config.n_simulations

            # Run the simulation (unchanged)
            result = await self._run_simulation(
                scenario_type, parameters, time_horizon, n_sim
            )

            # --- NEW: Distillation strategy selection and recommendation generation ---
            # Build state
            state = self._get_optimization_state(parameters, result)
            # Select strategy
            strategy, action_idx, state_vec, teacher_probs = await self.distillation_optimizer.select_strategy(state, exploration=True)

            # Generate recommendations according to strategy
            recommendations = self._generate_strategy_recommendations(strategy, scenario_type, result.projections, parameters)
            result.recommendations = recommendations
            result.strategy_used = strategy

            # Compute reward: improvement in weighted sustainability score
            # We simulate a "post‑recommendation" score by applying a synthetic improvement factor.
            baseline_score = result.weighted_score
            improvement_factor = {
                'aggressive_carbon': 0.15,
                'helium_preservation': 0.12,
                'circularity_boost': 0.10,
                'renewable_acceleration': 0.13,
                'balanced': 0.08,
            }.get(strategy, 0.05)
            improved_score = min(1.0, baseline_score + improvement_factor * (1.0 - baseline_score))
            reward = improved_score - baseline_score  # reward is the improvement

            # Update distillation agent
            next_state = self._get_optimization_state(parameters, result)
            await self.distillation_optimizer.update(state_vec, action_idx, reward, next_state.to_feature_vector(), teacher_probs)

            # Store result with reward info
            result.reward = reward

            # Store in cache and history
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
            self.telemetry.increment('scenarios_run')
            self.telemetry.gauge('sustainability_score', result.sustainability_score)
            self.telemetry.gauge('weighted_score', result.weighted_score)
            self.telemetry.increment('dt_distillation_strategy', tags={'strategy': strategy})
            self.telemetry.gauge('dt_distillation_reward', reward)

            logger.info(f"Completed scenario: {scenario_id}, strategy={strategy}, reward={reward:.3f}")
            return result

    # ------------------------------------------------------------------------
    # Build optimization state from scenario parameters and result
    # ------------------------------------------------------------------------
    def _get_optimization_state(self, parameters: Dict, result: DigitalTwinResult) -> TwinOptimizationState:
        # Extract normalized metrics from result
        projections = result.projections
        carbon_val = projections.get('carbon_emissions', [0.5])[-1]
        helium_val = projections.get('helium_depletion', [0.5])[-1]
        energy_val = projections.get('energy_consumption', [0.5])[-1]
        circularity_val = projections.get('circularity_index', [0.5])[-1]
        biodiversity_val = projections.get('biodiversity_impact', [0.5])[-1]

        # Normalize to [0,1] roughly
        carbon_emissions_norm = min(1.0, carbon_val / 2.0)
        helium_depletion_norm = min(1.0, helium_val)
        energy_consumption_norm = min(1.0, energy_val / 2.0)
        circularity_index_norm = circularity_val
        biodiversity_impact_norm = max(0, min(1.0, biodiversity_val))

        # Extract scenario parameters
        carbon_reduction = parameters.get('carbon_reduction_rate', 0.0)
        helium_reduction = parameters.get('helium_reduction_rate', 0.0)
        adoption_rate = parameters.get('adoption_rate', 0.0)
        shock_size = parameters.get('shock_size', 0.0)

        # Historical stats (from recent results)
        if len(self.scenario_results) > 0:
            recent = self.scenario_results[-10:]
            success_rate = np.mean([1.0 for r in recent if r.reward > 0.0])
            avg_roi = np.mean([r.reward for r in recent]) if recent else 0.0
        else:
            success_rate = 0.5
            avg_roi = 0.0

        # System health
        circuit_state = 0.0 if not self._circuit_breaker.is_open else (0.5 if self._circuit_breaker.state == CircuitBreakerState.HALF_OPEN else 1.0)
        cache_usage = len(self.simulation_cache) / self.config.cache_max_size
        scenario_count = len(self.scenario_results) / 100.0

        return TwinOptimizationState(
            carbon_emissions=carbon_emissions_norm,
            helium_depletion=helium_depletion_norm,
            energy_consumption=energy_consumption_norm,
            circularity_index=circularity_index_norm,
            biodiversity_impact=biodiversity_impact_norm,
            carbon_reduction_rate=carbon_reduction,
            helium_reduction_rate=helium_reduction,
            adoption_rate=adoption_rate,
            shock_size=shock_size,
            recent_success_rate=success_rate,
            avg_roi=avg_roi,
            circuit_breaker_state=circuit_state,
            cache_usage=cache_usage,
            scenario_count=scenario_count
        )

    # ------------------------------------------------------------------------
    # Generate recommendations based on selected strategy
    # ------------------------------------------------------------------------
    def _generate_strategy_recommendations(
        self,
        strategy: str,
        scenario_type: SimulationScenario,
        projections: Dict,
        parameters: Dict
    ) -> List[Dict[str, Any]]:
        """Generate a list of recommendations according to the chosen strategy."""
        base_recs = self._generate_cost_benefit_recommendations(scenario_type, projections, parameters)

        # Filter or augment based on strategy
        if strategy == 'aggressive_carbon':
            rec = self._create_recommendation(
                action="Deep Carbon Reduction",
                description="Implement aggressive carbon capture and renewable transition",
                estimated_cost=70.0,
                estimated_benefit=0.5,
                time_horizon_months=24,
                risk_level="high",
                prerequisites=["Carbon capture technology", "Renewable infrastructure"],
                confidence=0.6
            )
            base_recs.insert(0, rec)
        elif strategy == 'helium_preservation':
            rec = self._create_recommendation(
                action="Helium Recycling & Substitution",
                description="Deploy helium recovery systems and research alternatives",
                estimated_cost=60.0,
                estimated_benefit=0.45,
                time_horizon_months=18,
                risk_level="medium",
                prerequisites=["Helium recovery system", "Alternative cooling research"],
                confidence=0.7
            )
            base_recs.insert(0, rec)
        elif strategy == 'circularity_boost':
            rec = self._create_recommendation(
                action="Circular Economy Overhaul",
                description="Maximize material recycling and closed-loop systems",
                estimated_cost=50.0,
                estimated_benefit=0.4,
                time_horizon_months=24,
                risk_level="medium",
                prerequisites=["Circularity audit", "Recycling partnerships"],
                confidence=0.75
            )
            base_recs.insert(0, rec)
        elif strategy == 'renewable_acceleration':
            rec = self._create_recommendation(
                action="Accelerated Renewable Adoption",
                description="Rapid deployment of solar, wind, and storage",
                estimated_cost=80.0,
                estimated_benefit=0.55,
                time_horizon_months=24,
                risk_level="low",
                prerequisites=["Renewable vendor contracts", "Grid integration"],
                confidence=0.8
            )
            base_recs.insert(0, rec)
        # Balanced: no extra recs

        # Keep only top 5 by ROI
        base_recs.sort(key=lambda x: x.get('roi', 0), reverse=True)
        return base_recs[:5]

    # ------------------------------------------------------------------------
    # Original recommendation generation (used as base)
    # ------------------------------------------------------------------------
    def _generate_cost_benefit_recommendations(
        self,
        scenario_type: SimulationScenario,
        projections: Dict,
        parameters: Dict
    ) -> List[Dict[str, Any]]:
        recommendations = []

        # Carbon recommendations
        if 'carbon_emissions' in projections and projections['carbon_emissions']:
            trend = projections['carbon_emissions'][-1] - projections['carbon_emissions'][0]
            if trend > 0:
                recommendations.append(self._create_recommendation(
                    action="Reduce Carbon Emissions",
                    description="Implement aggressive carbon reduction strategies",
                    estimated_cost=50.0,
                    estimated_benefit=0.3,
                    time_horizon_months=12,
                    risk_level="medium",
                    prerequisites=["Carbon budget approval", "Expert review"],
                    confidence=0.75
                ))
                recommendations.append(self._create_recommendation(
                    action="Adopt Renewable Energy",
                    description="Increase renewable energy adoption to 50%",
                    estimated_cost=30.0,
                    estimated_benefit=0.2,
                    time_horizon_months=18,
                    risk_level="low",
                    prerequisites=["Renewable vendor selection", "Infrastructure upgrade"],
                    confidence=0.85
                ))
            elif trend < -0.1:
                recommendations.append(self._create_recommendation(
                    action="Maintain Carbon Momentum",
                    description="Continue successful carbon reduction strategies",
                    estimated_cost=10.0,
                    estimated_benefit=0.15,
                    time_horizon_months=6,
                    risk_level="low",
                    confidence=0.9
                ))

        # Helium recommendations
        if 'helium_depletion' in projections and projections['helium_depletion']:
            if projections['helium_depletion'][-1] < 0.3:
                recommendations.append(self._create_recommendation(
                    action="CRITICAL: Helium Conservation",
                    description="Implement immediate helium recovery and substitution",
                    estimated_cost=80.0,
                    estimated_benefit=0.5,
                    time_horizon_months=6,
                    risk_level="high",
                    prerequisites=["Helium recovery system", "Substitution research"],
                    confidence=0.7
                ))
            elif projections['helium_depletion'][-1] < 0.5:
                recommendations.append(self._create_recommendation(
                    action="Optimize Helium Usage",
                    description="Improve helium efficiency in quantum cooling",
                    estimated_cost=25.0,
                    estimated_benefit=0.2,
                    time_horizon_months=9,
                    risk_level="medium",
                    prerequisites=["Helium audit", "Efficiency review"],
                    confidence=0.8
                ))

        # Circularity recommendations
        if 'circularity_index' in projections and projections['circularity_index']:
            if projections['circularity_index'][-1] < 0.5:
                recommendations.append(self._create_recommendation(
                    action="Improve Circularity",
                    description="Enhance material recovery and recycling",
                    estimated_cost=40.0,
                    estimated_benefit=0.25,
                    time_horizon_months=15,
                    risk_level="medium",
                    prerequisites=["Circularity audit", "Recycling infrastructure"],
                    confidence=0.75
                ))

        # Scenario-specific recommendations
        if scenario_type == SimulationScenario.POLICY_CHANGE:
            if parameters.get('carbon_reduction_rate', 0) > 0.05:
                recommendations.append(self._create_recommendation(
                    action="Increase Policy Ambition",
                    description="Consider more aggressive carbon reduction targets",
                    estimated_cost=15.0,
                    estimated_benefit=0.1,
                    time_horizon_months=3,
                    risk_level="low",
                    confidence=0.85
                ))

        if scenario_type == SimulationScenario.RESOURCE_DEPLETION:
            recommendations.append(self._create_recommendation(
                action="Resource Diversification",
                description="Diversify resource portfolio to reduce dependency",
                estimated_cost=60.0,
                estimated_benefit=0.35,
                time_horizon_months=24,
                risk_level="medium",
                prerequisites=["Resource audit", "Alternative identification"],
                confidence=0.7
            ))

        # Substitution recommendations
        if self.config.resource_substitution_enabled:
            for resource, alternatives in self.substitution_options.items():
                if resource in self.resource_projections:
                    proj = self.resource_projections[resource]
                    if proj.substitution_availability > 0.3:
                        recommendations.append(self._create_recommendation(
                            action=f"Substitute {resource.capitalize()}",
                            description=f"Transition to {', '.join(alternatives[:2])} as alternatives",
                            estimated_cost=45.0,
                            estimated_benefit=0.3,
                            time_horizon_months=18,
                            risk_level="medium",
                            prerequisites=[f"{resource} substitution study", "Alternative validation"],
                            confidence=0.7
                        ))

        # Sort by ROI
        recommendations.sort(key=lambda x: x.get('roi', 0), reverse=True)
        return recommendations

    def _create_recommendation(
        self,
        action: str,
        description: str,
        estimated_cost: float,
        estimated_benefit: float,
        time_horizon_months: int,
        risk_level: str,
        prerequisites: List[str] = None,
        confidence: float = 0.7
    ) -> Dict[str, Any]:
        roi = estimated_benefit / max(estimated_cost, 0.01)
        return {
            'action': action,
            'description': description,
            'estimated_cost': estimated_cost,
            'estimated_benefit': estimated_benefit,
            'roi': roi,
            'time_horizon_months': time_horizon_months,
            'risk_level': risk_level,
            'prerequisites': prerequisites or [],
            'confidence': confidence,
            'cost_benefit_ratio': f"1:{roi:.2f}"
        }

    # ========================================================================
    # Real Data Access Methods (with Retry and Circuit Breaker)
    # ========================================================================

    async def _get_current_carbon(self) -> float:
        """Get current carbon intensity from the carbon manager."""
        if self.carbon_manager:
            if hasattr(self.carbon_manager, 'get_current_intensity'):
                intensity = await retry_async(
                    self.carbon_manager.get_current_intensity,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms
                )
                return intensity / 1000
            elif hasattr(self.carbon_manager, 'carbon_intensity'):
                return self.carbon_manager.carbon_intensity / 1000
        raise RuntimeError("Carbon manager not available or no method to get intensity")

    async def _get_current_carbon_with_fallback(self) -> float:
        """Get carbon with fallback and circuit breaker."""
        try:
            return await self._circuit_breaker.call(self._get_current_carbon)
        except Exception as e:
            logger.warning(f"Failed to get carbon intensity, using fallback: {e}")
            return 0.5

    async def _get_current_helium(self) -> float:
        """Get current helium position from the helium tracker."""
        if self.helium_tracker:
            if hasattr(self.helium_tracker, 'get_helium_position'):
                position = await retry_async(
                    self.helium_tracker.get_helium_position,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms
                )
                if position:
                    return position.get('total_usage_l', 0) / position.get('budget_l', 100)
        raise RuntimeError("Helium tracker not available")

    async def _get_current_helium_with_fallback(self) -> float:
        try:
            return await self._circuit_breaker.call(self._get_current_helium)
        except Exception as e:
            logger.warning(f"Failed to get helium position, using fallback: {e}")
            return 0.5

    async def _get_current_energy(self) -> float:
        """Get current energy consumption from the expert registry."""
        if self.expert_registry:
            experts = await retry_async(
                self.expert_registry.get_all_active_experts,
                self.config.max_retries,
                self.config.retry_base_delay_ms,
                self.config.retry_max_delay_ms
            )
            total_energy = sum(
                getattr(e, 'energy_per_inference', 0.001) 
                for e in experts[:10]
            )
            return min(1.0, total_energy * 100)
        raise RuntimeError("Expert registry not available")

    async def _get_current_energy_with_fallback(self) -> float:
        try:
            return await self._circuit_breaker.call(self._get_current_energy)
        except Exception as e:
            logger.warning(f"Failed to get energy consumption, using fallback: {e}")
            return 0.5

    async def _get_current_expert_count(self) -> float:
        """Get current expert count from the expert registry."""
        if self.expert_registry:
            experts = await retry_async(
                self.expert_registry.get_all_active_experts,
                self.config.max_retries,
                self.config.retry_base_delay_ms,
                self.config.retry_max_delay_ms
            )
            return len(experts)
        raise RuntimeError("Expert registry not available")

    async def _get_current_expert_count_with_fallback(self) -> float:
        try:
            return await self._circuit_breaker.call(self._get_current_expert_count)
        except Exception as e:
            logger.warning(f"Failed to get expert count, using fallback: {e}")
            return 10

    async def _get_current_circularity(self) -> float:
        """Get current circularity score from the circular manager."""
        if self.circular_manager:
            if hasattr(self.circular_manager, 'get_circularity_report'):
                report = await retry_async(
                    self.circular_manager.get_circularity_report,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms
                )
                if report:
                    return report.get('circularity_score', 0.5)
        raise RuntimeError("Circular manager not available")

    async def _get_current_circularity_with_fallback(self) -> float:
        try:
            return await self._circuit_breaker.call(self._get_current_circularity)
        except Exception as e:
            logger.warning(f"Failed to get circularity, using fallback: {e}")
            return 0.5

    # ========================================================================
    # Analysis Methods (Enhanced)
    # ========================================================================

    def _calculate_depletion_year(self, projection: List[float]) -> Optional[int]:
        if len(projection) < 2:
            return None

        for i, value in enumerate(projection):
            if value <= 0.0:
                years_from_now = i * self.config.time_step_days / 365.0
                return int(datetime.now().year + years_from_now)

        trend = (projection[-1] - projection[0]) / len(projection)
        if trend < 0:
            steps_to_zero = int(projection[-1] / -trend)
            years_from_now = (len(projection) + steps_to_zero) * self.config.time_step_days / 365.0
            return int(datetime.now().year + years_from_now)

        return None

    def _calculate_sustainability_score(self, projections: Dict) -> float:
        scores = []
        if 'carbon_emissions' in projections and projections['carbon_emissions']:
            carbon_end = projections['carbon_emissions'][-1]
            carbon_start = projections['carbon_emissions'][0]
            carbon_score = 1.0 - min(1.0, (carbon_end - carbon_start) / max(carbon_start, 0.1))
            scores.append(carbon_score)
        if 'helium_depletion' in projections and projections['helium_depletion']:
            helium_end = projections['helium_depletion'][-1]
            helium_start = projections['helium_depletion'][0]
            helium_score = min(1.0, helium_end / max(helium_start, 0.1))
            scores.append(helium_score)
        if 'circularity_index' in projections and projections['circularity_index']:
            circ_end = projections['circularity_index'][-1]
            circ_start = projections['circularity_index'][0]
            circ_score = min(1.0, circ_end / max(circ_start, 0.1))
            scores.append(circ_score)
        if 'biodiversity_impact' in projections and projections['biodiversity_impact']:
            bio_end = projections['biodiversity_impact'][-1]
            bio_score = max(0, min(1.0, bio_end))
            scores.append(bio_score)
        return np.mean(scores) if scores else 0.5

    def _calculate_weighted_sustainability_score(self, projections: Dict) -> float:
        if not self.priority_weights:
            return self._calculate_sustainability_score(projections)

        weighted_scores = []
        total_weight = 0.0

        for key, weight in self.priority_weights.items():
            if key == 'carbon' and 'carbon_emissions' in projections:
                carbon_end = projections['carbon_emissions'][-1]
                carbon_start = projections['carbon_emissions'][0]
                score = 1.0 - min(1.0, (carbon_end - carbon_start) / max(carbon_start, 0.1))
                weighted_scores.append(score * weight)
                total_weight += weight
            elif key == 'helium' and 'helium_depletion' in projections:
                helium_end = projections['helium_depletion'][-1]
                helium_start = projections['helium_depletion'][0]
                score = min(1.0, helium_end / max(helium_start, 0.1))
                weighted_scores.append(score * weight)
                total_weight += weight
            elif key == 'energy' and 'energy_consumption' in projections:
                energy_end = projections['energy_consumption'][-1]
                energy_start = projections['energy_consumption'][0]
                score = 1.0 - min(1.0, (energy_end - energy_start) / max(energy_start, 0.1))
                weighted_scores.append(score * weight)
                total_weight += weight
            elif key == 'circularity' and 'circularity_index' in projections:
                circ_end = projections['circularity_index'][-1]
                circ_start = projections['circularity_index'][0]
                score = min(1.0, circ_end / max(circ_start, 0.1))
                weighted_scores.append(score * weight)
                total_weight += weight
            elif key == 'biodiversity' and 'biodiversity_impact' in projections:
                bio_end = projections['biodiversity_impact'][-1]
                score = max(0, min(1.0, bio_end))
                weighted_scores.append(score * weight)
                total_weight += weight

        return sum(weighted_scores) / max(total_weight, 0.001)

    # ========================================================================
    # Risk Factors (unchanged)
    # ========================================================================

    def _identify_risk_factors(self, projections: Dict) -> List[str]:
        risks = []

        for key, values in projections.items():
            if values and len(values) > 1:
                trend = values[-1] - values[0]
                if trend < -0.1:
                    if 'carbon' in key or 'emissions' in key:
                        risks.append(f"Increasing {key} - carbon risk")
                    elif 'helium' in key or 'depletion' in key:
                        risks.append(f"Declining {key} - helium scarcity risk")

        for key, values in projections.items():
            if values and len(values) > self.config.volatility_window_size:
                recent = values[-self.config.volatility_window_size:]
                volatility = np.std(recent)
                if volatility > 0.1:
                    risks.append(f"High volatility in {key}")

        for key, proj in self.resource_projections.items():
            if proj.depletion_year and proj.depletion_year < datetime.now().year + 5:
                risks.append(f"{key} depletion risk within 5 years")

        return risks

    # ========================================================================
    # Interdependent Factors (unchanged)
    # ========================================================================

    def _get_interdependent_factors(self, scenario_type: SimulationScenario, parameters: Dict) -> List[str]:
        factors = []
        if scenario_type == SimulationScenario.POLICY_AND_TECHNOLOGY:
            if 'carbon_reduction_rate' in parameters:
                factors.append('carbon_policy')
            if 'adoption_rate' in parameters:
                factors.append('technology_adoption')
            if 'carbon_efficiency_gain' in parameters:
                factors.append('carbon_efficiency')
        elif scenario_type == SimulationScenario.MARKET_AND_REGULATORY:
            if 'shock_size' in parameters:
                factors.append('market_shock')
            if 'carbon_tax_rate' in parameters:
                factors.append('carbon_regulation')
            if 'helium_quota_reduction' in parameters:
                factors.append('helium_regulation')
        elif scenario_type == SimulationScenario.RESOURCE_AND_CLIMATE:
            if 'carbon_depletion_rate' in parameters:
                factors.append('carbon_depletion')
            if 'helium_depletion_rate' in parameters:
                factors.append('helium_depletion')
            if 'event_impact' in parameters:
                factors.append('climate_event')
        return factors

    # ========================================================================
    # _apply_interdependent_scenario (unchanged)
    # ========================================================================

    def _apply_interdependent_scenario(
        self,
        scenario_type: SimulationScenario,
        parameters: Dict,
        step: int
    ) -> Tuple[float, float, float]:
        """Apply interdependent scenario effects."""
        carbon_effect = 1.0
        helium_effect = 1.0
        energy_effect = 1.0

        if scenario_type == SimulationScenario.POLICY_AND_TECHNOLOGY:
            carbon_reduction = parameters.get('carbon_reduction_rate', 0.05)
            adoption_rate = parameters.get('adoption_rate', 0.1)
            efficiency_gain = parameters.get('carbon_efficiency_gain', 0.3)

            tech_factor = 1 - np.exp(-adoption_rate * step)
            carbon_effect = 1.0 - (carbon_reduction * (1 + tech_factor * 0.5)) * (step / 10)
            helium_effect = 1.0 - (carbon_reduction * 0.3 * (1 + tech_factor * 0.3)) * (step / 10)
            energy_effect = 1.0 - efficiency_gain * tech_factor * 0.5

        elif scenario_type == SimulationScenario.MARKET_AND_REGULATORY:
            shock_size = parameters.get('shock_size', 0.3)
            shock_duration = parameters.get('shock_duration', 5)
            tax_rate = parameters.get('carbon_tax_rate', 0.1)
            quota_reduction = parameters.get('helium_quota_reduction', 0.05)

            if step < shock_duration:
                shock_factor = 1.0 + (1.0 - step / shock_duration) * shock_size
                carbon_effect = shock_factor
                helium_effect = shock_factor * 0.5
                energy_effect = shock_factor * 0.3

            carbon_effect *= (1.0 + tax_rate * (step / 10))
            helium_effect *= (1.0 - quota_reduction * (step / 10))
            energy_effect *= (1.0 + tax_rate * 0.5 * (step / 10))

        elif scenario_type == SimulationScenario.RESOURCE_AND_CLIMATE:
            carbon_depletion = parameters.get('carbon_depletion_rate', 0.02)
            helium_depletion = parameters.get('helium_depletion_rate', 0.03)
            event_impact = parameters.get('event_impact', 0.2)
            event_duration = parameters.get('event_duration', 3)
            recovery_rate = parameters.get('recovery_rate', 0.1)

            carbon_effect = 1.0 - carbon_depletion * step
            helium_effect = max(0.1, 1.0 - helium_depletion * step)

            if step < event_duration:
                carbon_effect *= (1.0 + event_impact)
                helium_effect *= (1.0 + event_impact * 0.7)
                energy_effect *= (1.0 + event_impact * 0.5)
            else:
                recovery = np.exp(-recovery_rate * (step - event_duration))
                carbon_effect *= (1.0 + event_impact * recovery * 0.5)
                helium_effect *= (1.0 + event_impact * 0.7 * recovery * 0.5)
                energy_effect *= (1.0 + event_impact * 0.5 * recovery * 0.5)

        return carbon_effect, helium_effect, energy_effect

    # ========================================================================
    # _run_simulation (unchanged)
    # ========================================================================

    async def _run_simulation(
        self,
        scenario_type: SimulationScenario,
        parameters: Dict[str, Any],
        time_horizon_years: int,
        n_simulations: int
    ) -> DigitalTwinResult:
        n_steps = int(time_horizon_years * 365 / self.config.time_step_days)
        timestamps = [
            datetime.now() + timedelta(days=i * self.config.time_step_days)
            for i in range(n_steps)
        ]

        # Run Monte Carlo simulations in parallel using asyncio.gather with return_exceptions
        tasks = []
        for sim_idx in range(n_simulations):
            tasks.append(self._run_single_simulation_correlated(
                scenario_type, parameters, timestamps, sim_idx, n_simulations
            ))
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out failed simulations and log errors
        all_simulations = []
        for r in results:
            if isinstance(r, Exception):
                logger.error(f"Simulation failed: {r}")
            else:
                all_simulations.append(r)

        if not all_simulations:
            raise RuntimeError("All simulations failed")

        # Aggregate results
        projections = {
            'carbon_emissions': [],
            'helium_depletion': [],
            'energy_consumption': [],
            'expert_population': [],
            'circularity_index': [],
            'biodiversity_impact': []
        }

        for key in projections.keys():
            values = [sim[key] for sim in all_simulations]
            projections[key] = np.mean(values, axis=0).tolist()

            if self.config.confidence_level < 1.0:
                lower = np.percentile(values, (1 - self.config.confidence_level) / 2 * 100, axis=0)
                upper = np.percentile(values, (1 + self.config.confidence_level) / 2 * 100, axis=0)
            else:
                lower = [0.0] * len(projections[key])
                upper = [0.0] * len(projections[key])

            # Build resource projections
            substitution = self._get_substitution_effects(key)
            alternative_resources = self.substitution_options.get(key, [])

            self.resource_projections[key] = ResourceProjection(
                resource_type=key,
                current_level=projections[key][0] if projections[key] else 0,
                projected_levels=projections[key],
                confidence_lower=lower.tolist() if hasattr(lower, 'tolist') else lower,
                confidence_upper=upper.tolist() if hasattr(upper, 'tolist') else upper,
                depletion_year=self._calculate_depletion_year(projections[key]),
                substitution_availability=substitution.get('availability', 0.0),
                substitution_cost_factor=substitution.get('cost_factor', 1.0),
                alternative_resources=alternative_resources
            )

        # Calculate weighted and unweighted sustainability scores
        weighted_score = self._calculate_weighted_sustainability_score(projections)
        sustainability_score = self._calculate_sustainability_score(projections)

        # Generate cost-benefit recommendations (base)
        recommendations = self._generate_cost_benefit_recommendations(
            scenario_type, projections, parameters
        )

        # Identify risk factors
        risk_factors = self._identify_risk_factors(projections)

        # Interdependent factors
        interdependent_factors = self._get_interdependent_factors(scenario_type, parameters)

        # Substitution effects
        substitution_effects = self._get_substitution_effects_all()

        # Return a result without strategy/reward (will be filled later)
        result = DigitalTwinResult(
            scenario_id=self._generate_scenario_id(scenario_type, parameters),
            scenario_type=scenario_type,
            metrics={
                'time_horizon_years': time_horizon_years,
                'n_simulations': n_simulations,
                'n_steps': n_steps,
                'correlated_uncertainty': self.config.correlated_uncertainty,
                'resource_substitution': self.config.resource_substitution_enabled
            },
            projections=projections,
            confidence_intervals={
                key: (proj.confidence_lower[-1] if proj.confidence_lower else 0,
                      proj.confidence_upper[-1] if proj.confidence_upper else 0)
                for key, proj in self.resource_projections.items()
                if key in self.resource_projections
            },
            risk_factors=risk_factors,
            recommendations=recommendations,
            sustainability_score=sustainability_score,
            interdependent_factors=interdependent_factors,
            substitution_effects=substitution_effects,
            weighted_score=weighted_score
        )
        return result

    # ========================================================================
    # _run_single_simulation_correlated (unchanged)
    # ========================================================================

    async def _run_single_simulation_correlated(
        self,
        scenario_type: SimulationScenario,
        parameters: Dict[str, Any],
        timestamps: List[datetime],
        sim_idx: int,
        total_sims: int
    ) -> Dict[str, List[float]]:
        """Run a single simulation with correlated uncertainty."""
        # Get current state with retry protection and circuit breaker
        current_carbon = await self._get_current_carbon_with_fallback()
        current_helium = await self._get_current_helium_with_fallback()
        current_energy = await self._get_current_energy_with_fallback()
        current_experts = await self._get_current_expert_count_with_fallback()
        current_circularity = await self._get_current_circularity_with_fallback()

        # Generate correlated noise
        if self.config.correlated_uncertainty and HAS_SCIPY:
            resources = ['carbon', 'helium', 'energy', 'circularity', 'biodiversity']
            mean = np.zeros(len(resources))
            cov_matrix = self._build_covariance_matrix(resources)

            # Generate correlated noise
            noise_samples = multivariate_normal.rvs(mean, cov_matrix, size=len(timestamps))
            carbon_noise = noise_samples[:, 0]
            helium_noise = noise_samples[:, 1]
            energy_noise = noise_samples[:, 2]
            circularity_noise = noise_samples[:, 3]
            biodiversity_noise = noise_samples[:, 4]
        else:
            # Independent noise
            carbon_noise = np.random.normal(0, 0.02, len(timestamps))
            helium_noise = np.random.normal(0, 0.02, len(timestamps))
            energy_noise = np.random.normal(0, 0.01, len(timestamps))
            circularity_noise = np.random.normal(0, 0.01, len(timestamps))
            biodiversity_noise = np.random.normal(0, 0.01, len(timestamps))

        carbon_emissions = []
        helium_depletion = []
        energy_consumption = []
        expert_population = []
        circularity_index = []
        biodiversity_impact = []

        for i, timestamp in enumerate(timestamps):
            # Apply scenario effects with interdependence
            carbon_effect, helium_effect, energy_effect = self._apply_interdependent_scenario(
                scenario_type, parameters, i
            )

            # Apply substitution effects (resource-specific)
            substitution_factors = {}
            if self.config.resource_substitution_enabled:
                for resource in ['carbon', 'helium', 'energy']:
                    substitution_factors[resource] = self._apply_substitution_effects(
                        resource, i, parameters
                    )

            # Update state with correlated noise
            noise_factor_carbon = 1.0 + carbon_noise[i] * 0.1
            noise_factor_helium = 1.0 + helium_noise[i] * 0.1
            noise_factor_energy = 1.0 + energy_noise[i] * 0.05

            carbon_val = current_carbon * carbon_effect * noise_factor_carbon
            if 'carbon' in substitution_factors:
                carbon_val *= substitution_factors['carbon']
            helium_val = current_helium * helium_effect * noise_factor_helium
            if 'helium' in substitution_factors:
                helium_val *= substitution_factors['helium']
            energy_val = current_energy * energy_effect * noise_factor_energy
            if 'energy' in substitution_factors:
                energy_val *= substitution_factors['energy']

            carbon_emissions.append(carbon_val)
            helium_depletion.append(helium_val)
            energy_consumption.append(energy_val)
            expert_population.append(
                current_experts * (1 + np.random.normal(0, 0.005)) * noise_factor_carbon
            )
            circularity_index.append(
                current_circularity * (1 + circularity_noise[i] * 0.05)
            )
            biodiversity_impact.append(
                1.0 - (carbon_val / 1000) * 0.1 + biodiversity_noise[i] * 0.02
            )

        return {
            'carbon_emissions': carbon_emissions,
            'helium_depletion': helium_depletion,
            'energy_consumption': energy_consumption,
            'expert_population': expert_population,
            'circularity_index': circularity_index,
            'biodiversity_impact': biodiversity_impact
        }

    def _build_covariance_matrix(self, resources: List[str]) -> np.ndarray:
        """Build covariance matrix from correlation matrix and configurable variances."""
        n = len(resources)
        corr_matrix = np.zeros((n, n))

        for i, res_i in enumerate(resources):
            for j, res_j in enumerate(resources):
                corr_matrix[i, j] = self.resource_correlation.get(res_i, {}).get(res_j, 0.0)

        # Use configurable variances
        variances = [self.config.resource_variances.get(res, 0.02) for res in resources]
        std_matrix = np.diag(np.sqrt(variances))
        cov_matrix = std_matrix @ corr_matrix @ std_matrix

        return cov_matrix

    # ========================================================================
    # Substitution Methods (unchanged)
    # ========================================================================

    def _get_substitution_effects(self, resource_type: str) -> Dict[str, float]:
        """Get substitution effects for a resource."""
        default = {
            'availability': self.config.substitution_availability_default.get(resource_type, 0.0),
            'cost_factor': self.config.substitution_cost_factor_default.get(resource_type, 1.0),
            'timeline': self.config.substitution_timeline_default.get(resource_type, 12.0)
        }
        return default

    def _apply_substitution_effects(self, resource: str, step: int, parameters: Dict) -> float:
        """
        Apply resource‑specific substitution effects based on step and parameters.
        Returns a multiplier (1.0 = no substitution, lower = more substitution).
        """
        # Get resource‑specific substitution availability and cost factor
        subst_availability = self.config.substitution_availability_default.get(resource, 0.0)
        subst_cost_factor = self.config.substitution_cost_factor_default.get(resource, 1.0)
        # In this simple model, substitution reduces resource usage proportionally to availability
        # and cost factor. Higher availability and lower cost mean more substitution.
        # We also consider a ramp-up based on step.
        ramp_start = self.config.substitution_ramp_start_step
        ramp_rate = self.config.substitution_ramp_rate

        if step < ramp_start:
            return 1.0

        # Ramp factor: increases from 0 to 1 over time
        ramp_progress = min(1.0, ramp_rate * (step - ramp_start))
        # Substitution effect: reduce usage by (availability * ramp_progress * cost_factor)
        # but cap at 0.5 to avoid unrealistic extreme reductions
        reduction = min(0.5, subst_availability * ramp_progress * subst_cost_factor)
        return 1.0 - reduction

    def _get_substitution_effects_all(self) -> Dict[str, Dict]:
        """Get substitution effects for all resources."""
        effects = {}
        for resource in ['carbon', 'helium', 'energy', 'circularity', 'biodiversity']:
            effects[resource] = self._get_substitution_effects(resource)
        return effects

    # ========================================================================
    # Utility Methods
    # ========================================================================

    def _generate_scenario_id(self, scenario_type: SimulationScenario, parameters: Dict) -> str:
        param_str = json.dumps(parameters, sort_keys=True)
        hash_str = hashlib.md5(f"{scenario_type.value}{param_str}".encode()).hexdigest()[:8]
        return f"{scenario_type.value}_{hash_str}"

    # ========================================================================
    # Statistics and Export
    # ========================================================================

    def get_simulation_stats(self) -> Dict[str, Any]:
        return {
            'total_scenarios': len(self.scenario_results),
            'cached_scenarios': len(self.simulation_cache),
            'resource_projections': len(self.resource_projections),
            'correlated_uncertainty_enabled': self.config.correlated_uncertainty,
            'resource_substitution_enabled': self.config.resource_substitution_enabled,
            'recent_results': [
                {
                    'scenario_id': r.scenario_id,
                    'type': r.scenario_type.value,
                    'sustainability_score': r.sustainability_score,
                    'weighted_score': r.weighted_score,
                    'strategy_used': r.strategy_used,
                    'reward': r.reward,
                    'recommendations': [
                        {'action': rec.get('action'), 'roi': rec.get('roi', 0)}
                        for rec in r.recommendations[:2]
                    ]
                }
                for r in self.scenario_results[-5:]
            ] if self.scenario_results else []
        }

    def update_user_priorities(self, new_priorities: Dict[str, float]):
        """Update user priority weights with validation."""
        total = sum(new_priorities.values())
        if abs(total - 1.0) > 0.01:
            raise ValueError("new_priorities must sum to approximately 1.0")
        allowed_keys = {'carbon', 'helium', 'energy', 'circularity', 'biodiversity'}
        if not set(new_priorities.keys()).issubset(allowed_keys):
            raise ValueError(f"new_priorities keys must be a subset of {allowed_keys}")
        self.priority_weights.update(new_priorities)
        logger.info(f"User priorities updated: {self.priority_weights}")

    async def export_projections(self) -> Dict[str, Any]:
        return {
            'timestamp': datetime.now().isoformat(),
            'config': {
                'time_horizon_years': self.config.time_horizon_years,
                'correlated_uncertainty': self.config.correlated_uncertainty,
                'resource_substitution': self.config.resource_substitution_enabled,
                'user_priorities': self.priority_weights
            },
            'projections': {
                key: {
                    'current': proj.current_level,
                    'projected': proj.projected_levels[-10:],
                    'depletion_year': proj.depletion_year,
                    'confidence_lower': proj.confidence_lower[-10:] if proj.confidence_lower else [],
                    'confidence_upper': proj.confidence_upper[-10:] if proj.confidence_upper else [],
                    'substitution_availability': proj.substitution_availability,
                    'substitution_cost_factor': proj.substitution_cost_factor,
                    'alternative_resources': proj.alternative_resources
                }
                for key, proj in self.resource_projections.items()
            },
            'user_priorities': self.priority_weights,
            'resource_correlation': self.resource_correlation
        }

    async def get_telemetry_export(self) -> str:
        return await self.telemetry.export()

    async def shutdown(self):
        """Graceful shutdown."""
        logger.info("Shutting down System Digital Twin v2.4.0")
        await self.save_state()
        logger.info("Shutdown complete")


# ============================================================================
# CLI Entry Point (example usage)
# ============================================================================

async def main():
    # Example: create digital twin, run a scenario, and print results
    twin = SystemDigitalTwin()
    await twin.initialize()

    # Inject some modules (stubs for demo)
    class MockCarbonManager:
        async def get_current_intensity(self):
            return 400.0
    class MockHeliumTracker:
        async def get_helium_position(self):
            return {'total_usage_l': 5, 'budget_l': 100}
    class MockExpertRegistry:
        async def get_all_active_experts(self):
            return [type('Expert', (), {'energy_per_inference': 0.001})() for _ in range(10)]
    class MockCircularManager:
        async def get_circularity_report(self):
            return {'circularity_score': 0.6}

    twin.inject_modules(
        carbon_manager=MockCarbonManager(),
        helium_tracker=MockHeliumTracker(),
        expert_registry=MockExpertRegistry(),
        circular_manager=MockCircularManager()
    )

    # Run a scenario
    scenario_params = {
        'carbon_reduction_rate': 0.1,
        'helium_conservation_rate': 0.05
    }
    result = await twin.run_scenario(
        scenario_type=SimulationScenario.POLICY_CHANGE,
        parameters=scenario_params,
        time_horizon_years=5,
        n_simulations=100
    )

    print(f"Scenario: {result.scenario_id}")
    print(f"Sustainability score: {result.sustainability_score:.3f}")
    print(f"Weighted score: {result.weighted_score:.3f}")
    print(f"Strategy used: {result.strategy_used}")
    print(f"Reward: {result.reward:.3f}")
    print("Top recommendations:")
    for rec in result.recommendations[:3]:
        print(f"  - {rec['action']} (ROI: {rec['roi']:.2f})")

    await twin.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
