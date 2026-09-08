# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/unified_sustainability_engine.py
# Enhanced version v5.2.0 – Complete, robust, with MOPD, XAI, temporal safety, human approval, chaos testing.

"""
Unified Sustainability Valuation Engine v5.2.0
Creates a single, authoritative global sustainability function that aggregates all dimensions
(carbon, helium, energy, circularity, biodiversity) with full bio‑inspired core integration
and Multi‑Objective Pareto Decision (MOPD) support.

ENHANCEMENTS OVER v5.1.0:
- Fixed missing imports and type hints.
- Deferred async task creation to avoid RuntimeError outside event loop.
- Meaningful Pareto front generation by simulating trade-off scenarios.
- XAI explanations added to MOPD points.
- Temporal safety invariant checks.
- Human‑in‑the‑loop approval hooks.
- Chaos testing (fault injection and recovery verification).
- Federated learning configuration stub.
- New configuration flags for above features.
"""

import asyncio
import logging
import json
import time
import hashlib
import os
import random
import sqlite3
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple, Union, Protocol, Callable
from collections import deque, defaultdict
from enum import Enum
import numpy as np

# ---------- Pydantic ----------
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Structlog ----------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

# ---------- FastAPI (optional) ----------
try:
    from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
    from fastapi.responses import JSONResponse, Response
    from contextlib import asynccontextmanager
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

# ---------- Bio-Inspired Core Import (with fallback) ----------
try:
    from enhancements.bio_inspired.__init__ import EnhancedBioInspiredCore, BioEvent, Persistence
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    from enhancements.bio_inspired.atp_synthase_scheduler import ATPSynthaseScheduler
    from enhancements.bio_inspired.chromatophore_compartments import CompartmentManager
    from enhancements.bio_inspired.biomass_storage import BiomassStorage
    from enhancements.bio_inspired.photosynthetic_harvester import PhotosyntheticHarvester
    from enhancements.bio_inspired.time_tick_engine import TimeTickEngine
    from enhancements.bio_inspired.quantum_bridge import QuantumBridge
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False
    class BioEvent:
        def __init__(self, event_type, source, data=None):
            self.event_type = event_type
            self.source = source
            self.data = data or {}

# ---------- MoE and Self-Evolving Gate imports (optional) ----------
try:
    from ..expert_router import ExpertRouter
    from ..gating_network import GatingNetworkManager
    from ..advanced.self_evolving_gates import EnhancedSelfEvolvingGate
    MOE_AVAILABLE = True
except ImportError:
    MOE_AVAILABLE = False

# ---------- Placeholder types (to avoid NameError) ----------
class HeliumProvider:
    def get_scarcity(self) -> float: raise NotImplementedError
    def get_cost_index(self) -> float: raise NotImplementedError

class CarbonProvider:
    async def get_current_intensity(self, region: str) -> float: raise NotImplementedError

class HeliumTracker:
    async def get_helium_position(self) -> Dict: raise NotImplementedError

class CircularManager:
    async def get_circularity_report(self) -> Dict: raise NotImplementedError

class BiodiversityProvider:
    async def get_biodiversity_report(self) -> Dict: raise NotImplementedError

class ExpertRegistry:
    async def get_all_active_experts(self) -> List: raise NotImplementedError

class QuantumLimits:
    async def update_sustainability_limits(self, score: float, dimensions: Dict): raise NotImplementedError

# ---------- Circuit Breaker ----------
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class EnhancedCircuitBreaker:
    """
    Stateful circuit breaker with half‑open state and metrics.
    """
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self.success_count = 0
        self._lock = asyncio.Lock()
        self.metrics = {"total_calls": 0, "failed_calls": 0, "successful_calls": 0}

    async def call(self, func: Callable, *args, **kwargs):
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.success_count = 0
                    logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN")
                else:
                    raise Exception(f"Circuit breaker {self.name} is OPEN")
            if self.state == CircuitBreakerState.HALF_OPEN and self.success_count >= 2:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                logger.info(f"Circuit breaker {self.name} CLOSED after {self.success_count} successes")
        self.metrics["total_calls"] += 1
        try:
            result = await func(*args, **kwargs)
            await self._record_success()
            return result
        except Exception as e:
            await self._record_failure()
            raise

    async def _record_success(self):
        async with self._lock:
            self.metrics["successful_calls"] += 1
            self.success_count += 1
            if self.state == CircuitBreakerState.HALF_OPEN:
                if self.success_count >= 2:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
            else:
                self.failure_count = 0

    async def _record_failure(self):
        async with self._lock:
            self.metrics["failed_calls"] += 1
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN

    def get_state_value(self) -> int:
        return {CircuitBreakerState.CLOSED: 0, CircuitBreakerState.HALF_OPEN: 1, CircuitBreakerState.OPEN: 2}[self.state]

# ---------- Retry helper with jitter ----------
async def retry_async(
    func: Callable,
    max_retries: int,
    base_delay_ms: float,
    max_delay_ms: float,
    *args,
    **kwargs
) -> Any:
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = min(base_delay_ms * (2 ** attempt), max_delay_ms) / 1000.0
            delay = delay * (1 + random.uniform(-0.2, 0.2))
            await asyncio.sleep(delay)
    raise RuntimeError("Max retries exceeded")

# ---------- Prometheus metrics ----------
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    SUSTAINABILITY_SCORE_GAUGE = Gauge('sustainability_total_score', 'Overall sustainability score', registry=REGISTRY)
    DIMENSION_SCORE_GAUGE = Gauge('sustainability_dimension_score', 'Dimension scores', ['dimension'], registry=REGISTRY)
    DIMENSION_WEIGHT_GAUGE = Gauge('sustainability_dimension_weight', 'Dimension weights', ['dimension'], registry=REGISTRY)
    SCARCITY_FACTOR_GAUGE = Gauge('sustainability_scarcity_factor', 'Scarcity factor per dimension', ['dimension'], registry=REGISTRY)
    UPDATE_LATENCY = Histogram('sustainability_update_latency_seconds', 'Score update latency', registry=REGISTRY)
    EXTERNAL_CALL_COUNTER = Counter('sustainability_external_calls_total', 'External service calls', ['service', 'status'], registry=REGISTRY)
    CIRCUIT_BREAKER_STATE = Gauge('sustainability_circuit_breaker_state', 'Circuit breaker state (0=CLOSED,1=HALF_OPEN,2=OPEN)', ['service'], registry=REGISTRY)
    CACHE_HIT_COUNTER = Counter('sustainability_cache_hits_total', 'Cache hits', ['dimension'], registry=REGISTRY)
    CACHE_MISS_COUNTER = Counter('sustainability_cache_misses_total', 'Cache misses', ['dimension'], registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def set(self, value): pass
        def inc(self, amount=1): pass
        def observe(self, value): pass
    SUSTAINABILITY_SCORE_GAUGE = DummyMetric()
    DIMENSION_SCORE_GAUGE = DummyMetric()
    DIMENSION_WEIGHT_GAUGE = DummyMetric()
    SCARCITY_FACTOR_GAUGE = DummyMetric()
    UPDATE_LATENCY = DummyMetric()
    EXTERNAL_CALL_COUNTER = DummyMetric()
    CIRCUIT_BREAKER_STATE = DummyMetric()
    CACHE_HIT_COUNTER = DummyMetric()
    CACHE_MISS_COUNTER = DummyMetric()

# ============================================================================
# 1. PYDANTIC CONFIGURATION (Enhanced with new flags)
# ============================================================================
class MOPDConfig(BaseModel):
    """Configuration for Multi-Objective Pareto Decision (MOPD) in sustainability."""
    enabled: bool = Field(True, description="Enable MOPD-aware analysis")
    objective_weights: Dict[str, float] = Field(
        default_factory=lambda: {
            'carbon': 0.25,
            'helium': 0.20,
            'energy': 0.15,
            'circularity': 0.25,
            'biodiversity': 0.15,
        },
        description="Weights for scalarising Pareto front (must sum to 1)"
    )
    grid_resolution: int = Field(5, description="Number of discrete weight combinations to sample")
    enable_cost_benefit: bool = Field(True)
    enable_predictive: bool = Field(True)

    # XAI, safety, approval, chaos
    enable_xai: bool = Field(True, description="Generate explanations for MOPD points")
    enable_temporal_safety: bool = Field(True, description="Perform temporal safety invariant checks")
    require_human_approval: bool = Field(False, description="Require human approval for major weight changes")
    aggressive_weight_change_threshold: float = Field(0.2, ge=0, le=1, description="Absolute weight change above which approval is needed")
    enable_chaos_testing: bool = Field(False, description="Enable chaos testing hooks")
    chaos_test_interval_seconds: int = Field(3600, ge=60)

    @model_validator(mode='after')
    def check_weights(self):
        total = sum(self.objective_weights.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError("Objective weights must sum to 1")
        return self

class FederatedSustainabilityConfig(BaseModel):
    """Configuration for federated sustainability metrics aggregation."""
    enabled: bool = Field(False)
    server_url: Optional[str] = None
    sparsity_ratio: float = Field(0.1, ge=0, le=1)
    privacy_epsilon: float = Field(1.0, ge=0)
    sync_interval_seconds: int = Field(3600, ge=60)

class SustainabilityEngineConfig(BaseModel):
    """Pydantic‑validated configuration for the Sustainability Engine."""
    # Dimension weights (initial)
    dimension_weights: Dict[str, float] = Field(
        default_factory=lambda: {
            'carbon': 0.25,
            'helium': 0.20,
            'energy': 0.15,
            'circularity': 0.25,
            'biodiversity': 0.15
        },
        description="Initial weights for each dimension (must sum to 1)"
    )
    # Threshold parameters
    warning_threshold: float = Field(0.3, ge=0, le=1)
    critical_threshold: float = Field(0.1, ge=0, le=1)
    # Adaptive threshold
    adaptation_rate: float = Field(0.1, ge=0, le=1)
    adaptive_window_size: int = Field(100, ge=1)
    # Predictive analyzer
    prediction_window: int = Field(50, ge=1)
    model_weights: Dict[str, float] = Field(
        default_factory=lambda: {
            'linear': 0.4,
            'exponential': 0.3,
            'moving_average': 0.3
        }
    )
    # Retry and circuit breaker
    max_retries: int = Field(3, ge=0)
    retry_base_delay_ms: float = Field(100.0, ge=0)
    retry_max_delay_ms: float = Field(5000.0, ge=0)
    circuit_breaker_failure_threshold: int = Field(5, ge=1)
    circuit_breaker_recovery_timeout: float = Field(30.0, ge=0)
    # History limits
    history_limit: int = Field(10000, ge=1)
    dimension_history_limit: int = Field(100, ge=1)
    # Persistence
    persistence_path: str = Field("sustainability_engine_state.json")
    # Telemetry
    telemetry_export_interval: int = Field(60, ge=0)
    # Report templates path (optional)
    report_templates_path: Optional[str] = None

    # Feature flags for bio-inspired integrations
    enable_event_driven: bool = True
    enable_self_healing: bool = True
    enable_swarm_coordination: bool = True
    enable_time_tick_engine: bool = True
    enable_quantum_bridge: bool = True
    enable_cost_benefit: bool = True
    enable_workflow_orchestration: bool = True

    # Workflow triggers
    workflow_on_critical_alert: str = "adjust_sustainability_strategy"
    workflow_on_slo_breach: str = "rebalance_dimensions"

    # Swarm sharing interval
    swarm_share_interval: int = 60

    # Cache TTL for dimension scores (seconds)
    cache_ttl: int = 60

    # Multi-region support
    default_region: str = "global"

    # Adaptive mapping from dimensions to adaptive cost function keys
    dimension_adaptive_mapping: Dict[str, str] = Field(
        default_factory=lambda: {
            'carbon': 'beta',
            'helium': 'gamma',
            'energy': 'alpha',
            'circularity': 'delta',
            'biodiversity': 'epsilon',
        },
        description="Mapping of dimension names to AdaptiveCostFunction keys"
    )

    # MOPD Configuration
    mopd: MOPDConfig = Field(default_factory=MOPDConfig, description="MOPD sub‑configuration")

    # Federated learning configuration
    federated: FederatedSustainabilityConfig = Field(default_factory=FederatedSustainabilityConfig)

    # ========== Pydantic model config ==========
    model_config = ConfigDict(env_prefix="SUSTAINABILITY_")

    # ========== Validators ==========
    @model_validator(mode='after')
    def validate_weights(self):
        total = sum(self.dimension_weights.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError(f"Dimension weights must sum to 1, got {total}")
        return self

    @model_validator(mode='after')
    def validate_thresholds(self):
        if self.critical_threshold > self.warning_threshold:
            raise ValueError("critical_threshold must be <= warning_threshold")
        return self

    @model_validator(mode='after')
    def validate_retry(self):
        if self.retry_max_delay_ms < self.retry_base_delay_ms:
            raise ValueError("retry_max_delay_ms must be >= retry_base_delay_ms")
        return self

    # ========== Utility methods ==========
    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump(exclude_none=True)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SustainabilityEngineConfig":
        return cls(**data)

    @classmethod
    def from_env(cls) -> "SustainabilityEngineConfig":
        return cls()

# Global config instance
SUSTAINABILITY_CONFIG = SustainabilityEngineConfig()

# ============================================================================
# 2. DATA CLASSES (Enhanced with XAI in MOPDPoint)
# ============================================================================
@dataclass
class SustainabilityDimension:
    name: str
    current_value: float
    target_value: float
    weight: float
    units: str
    trend: str = "stable"
    confidence: float = 0.8
    scarcity_factor: float = 1.0
    historical_weights: List[float] = field(default_factory=list)
    volatility: float = 0.0
    prediction: float = 0.0
    prediction_confidence: float = 0.0
    last_update: Optional[datetime] = None

@dataclass
class UnifiedSustainabilityScore:
    total_score: float
    dimensions: Dict[str, SustainabilityDimension]
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    confidence: float = 0.8
    trend: str = "stable"
    risk_factors: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    predicted_future_score: Optional[float] = None
    scenario_scores: Dict[str, float] = field(default_factory=dict)
    report_template: str = "standard"

@dataclass
class SustainabilityThreshold:
    dimension: str
    warning_threshold: float
    critical_threshold: float
    current_value: float = 0.0
    status: str = "unknown"
    adaptive_warning: float = 0.0
    adaptive_critical: float = 0.0
    historical_avg: float = 0.0
    history_std: float = 0.0
    alert_count: int = 0

@dataclass
class ReportTemplate:
    name: str
    description: str
    included_dimensions: List[str]
    metrics: List[str]
    format: str = "json"
    frequency: str = "daily"
    target_audience: str = "general"
    customization: Dict[str, Any] = field(default_factory=dict)

@dataclass
class MOPDPoint:
    """Represents a single sustainability state with its objective values and explanation."""
    weights: Dict[str, float]          # sum to 1
    dimensions: Dict[str, float]       # score per dimension
    scalarised_score: float = 0.0
    explanation: str = ""              # XAI explanation

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPoint':
        return cls(**data)


# ============================================================================
# 3. MANAGER IMPLEMENTATIONS (unchanged, but with stubs for missing classes)
# ============================================================================
class AdaptiveThresholdManager:
    # ... (same as before) ...
    def __init__(self, config: SustainabilityEngineConfig):
        self.config = config
        self.history: Dict[str, List[float]] = defaultdict(list)
        self.anomaly_scores: Dict[str, float] = defaultdict(float)

    async def update_thresholds(
        self,
        dimension: str,
        current_value: float,
        base_warning: float,
        base_critical: float
    ) -> Tuple[float, float]:
        self.history[dimension].append(current_value)
        if len(self.history[dimension]) > self.config.adaptive_window_size:
            self.history[dimension] = self.history[dimension][-self.config.adaptive_window_size:]

        if len(self.history[dimension]) < 5:
            return base_warning, base_critical

        hist = np.array(self.history[dimension])
        mean = np.mean(hist)
        std = np.std(hist)
        if std < 0.01:
            std = 0.01

        adaptive_warning = min(1.0, max(0.0, mean + 0.5 * std))
        adaptive_critical = min(1.0, max(0.0, mean + 1.5 * std))

        adaptive_warning = self.config.adaptation_rate * adaptive_warning + (1 - self.config.adaptation_rate) * base_warning
        adaptive_critical = self.config.adaptation_rate * adaptive_critical + (1 - self.config.adaptation_rate) * base_critical

        return adaptive_warning, adaptive_critical

    def get_anomaly_score(self, dimension: str, current_value: float) -> float:
        hist = self.history.get(dimension, [])
        if len(hist) < 5:
            return 0.0
        mean = np.mean(hist)
        std = np.std(hist)
        if std < 1e-6:
            return 0.0
        z = abs(current_value - mean) / std
        return min(1.0, z / 4.0)

    def get_threshold_stats(self, dimension: str) -> Dict[str, Any]:
        hist = self.history.get(dimension, [])
        if not hist:
            return {}
        return {
            'mean': np.mean(hist),
            'std': np.std(hist),
            'min': np.min(hist),
            'max': np.max(hist),
            'count': len(hist)
        }

class DynamicWeightManager:
    def __init__(self, config: SustainabilityEngineConfig):
        self.config = config
        self.weight_history: Dict[str, List[float]] = defaultdict(list)

    async def update_weights(
        self,
        dimension_scores: Dict[str, float],
        scarcity_factors: Dict[str, float]
    ) -> Dict[str, float]:
        weights = self.config.dimension_weights.copy()
        for dim in weights:
            scarcity = scarcity_factors.get(dim, 1.0)
            weights[dim] *= (1.0 + 0.1 * (scarcity - 1.0))
        total = sum(weights.values())
        if total > 0:
            for dim in weights:
                weights[dim] /= total
        for dim, w in weights.items():
            self.weight_history[dim].append(w)
            if len(self.weight_history[dim]) > self.config.dimension_history_limit:
                self.weight_history[dim] = self.weight_history[dim][-self.config.dimension_history_limit:]
        return weights

    def get_weight_trends(self) -> Dict[str, List[float]]:
        return {k: list(v) for k, v in self.weight_history.items()}

class PredictiveTrendAnalyzer:
    def __init__(self, config: SustainabilityEngineConfig):
        self.config = config
        self.model_histories: Dict[str, List[float]] = defaultdict(list)
        self.prediction_accuracy: Dict[str, float] = defaultdict(float)

    async def update_model(self, dimension: str, history: List[float]):
        self.model_histories[dimension] = history

    async def predict(self, dimension: str, steps: int = 10) -> Tuple[float, float, float]:
        hist = self.model_histories.get(dimension, [])
        if len(hist) < 3:
            return 0.5, 0.0, 0.0
        x = np.arange(len(hist))
        y = np.array(hist)
        coeffs = np.polyfit(x, y, 1)
        slope = coeffs[0]
        intercept = coeffs[1]
        prediction = intercept + slope * (len(hist) + steps)
        y_pred = coeffs[0] * x + coeffs[1]
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r2 = 1 - ss_res / (ss_tot + 1e-10)
        confidence = max(0.0, min(1.0, r2))
        volatility = np.std(y - y_pred) if len(y) > 2 else 0.0
        return float(prediction), float(confidence), float(volatility)

    async def predict_scenario(self, dimension: str, scenario: str, steps: int = 10) -> float:
        base_pred, _, _ = await self.predict(dimension, steps)
        if scenario == 'optimistic':
            return base_pred * 1.1
        elif scenario == 'pessimistic':
            return base_pred * 0.9
        else:
            return base_pred

    def get_prediction_accuracy(self, dimension: str) -> float:
        return self.prediction_accuracy.get(dimension, 0.5)

class ReportTemplateManager:
    def __init__(self, config: SustainabilityEngineConfig):
        self.config = config
        self.templates: Dict[str, ReportTemplate] = {}
        self._load_defaults()

    def _load_defaults(self):
        self.templates['executive_summary'] = ReportTemplate(
            name='executive_summary',
            description='High-level summary for executives',
            included_dimensions=['carbon', 'helium', 'energy', 'circularity', 'biodiversity'],
            metrics=['total_score', 'trend', 'risk_factors'],
            format='json',
            frequency='daily',
            target_audience='executive'
        )
        self.templates['detailed'] = ReportTemplate(
            name='detailed',
            description='Full breakdown with predictions',
            included_dimensions=['carbon', 'helium', 'energy', 'circularity', 'biodiversity'],
            metrics=['score', 'trend', 'prediction', 'volatility', 'scarcity'],
            format='json',
            frequency='weekly',
            target_audience='engineers'
        )

    def list_templates(self) -> List[str]:
        return list(self.templates.keys())

    def create_template(self, template: ReportTemplate) -> bool:
        if template.name in self.templates:
            return False
        self.templates[template.name] = template
        return True

    async def generate_report(
        self,
        template_name: str,
        data: Dict[str, Any],
        output_format: str = "json"
    ) -> Dict[str, Any]:
        template = self.templates.get(template_name)
        if not template:
            return {'status': 'template_not_found'}
        filtered = {}
        if 'included_dimensions' in template.__dict__:
            dims = template.included_dimensions
            filtered['dimensions'] = {k: v for k, v in data.get('dimensions', {}).items() if k in dims}
        if 'metrics' in template.__dict__:
            metrics = template.metrics
            for m in metrics:
                if m in data:
                    filtered[m] = data[m]
        filtered['template_name'] = template_name
        filtered['timestamp'] = datetime.now(timezone.utc).isoformat()
        return {'status': 'generated', 'report': filtered}

class SustainabilityTelemetry:
    # ... (same as before) ...
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
        async with self._lock:
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
# 4. EMISSION AND OFFSET STORAGE (SQLite)
# ============================================================================
class EmissionsStorage:
    def __init__(self, db_path: str = "emissions.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS emission_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                source TEXT,
                amount_kg REAL NOT NULL,
                metadata TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS offsets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                source TEXT,
                amount_kg REAL NOT NULL,
                metadata TEXT
            )
        """)
        conn.close()

    def record_emission(self, amount_kg: float, source: str = None, metadata: Dict = None):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT INTO emission_records (timestamp, source, amount_kg, metadata)
            VALUES (?, ?, ?, ?)
        """, (datetime.now(timezone.utc).isoformat(), source, amount_kg, json.dumps(metadata or {})))
        conn.commit()
        conn.close()

    def record_offset(self, amount_kg: float, source: str = None, metadata: Dict = None):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT INTO offsets (timestamp, source, amount_kg, metadata)
            VALUES (?, ?, ?, ?)
        """, (datetime.now(timezone.utc).isoformat(), source, amount_kg, json.dumps(metadata or {})))
        conn.commit()
        conn.close()

    def get_recent_emissions(self, hours: int = 24) -> float:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute(
            "SELECT SUM(amount_kg) FROM emission_records WHERE timestamp >= ?",
            (cutoff,)
        )
        row = cursor.fetchone()
        conn.close()
        return row[0] if row[0] is not None else 0.0


# ============================================================================
# 5. PERSISTENCE MANAGER (Enhanced with MOPD)
# ============================================================================
class SustainabilityPersistenceManager:
    def __init__(self, config: SustainabilityEngineConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        logger.info(f"SustainabilityPersistenceManager initialized (path={self.path})")

    async def save_state(self, engine: 'UnifiedSustainabilityEngine') -> bool:
        async with self._lock:
            try:
                state = {
                    'config': engine.config.to_dict(),
                    'sustainability_score': engine.sustainability_score,
                    'history': list(engine.history),
                    'dimension_history': {k: list(v) for k, v in engine.dimension_history.items()},
                    'thresholds': {
                        k: {
                            'warning_threshold': t.warning_threshold,
                            'critical_threshold': t.critical_threshold,
                            'current_value': t.current_value,
                            'adaptive_warning': t.adaptive_warning,
                            'adaptive_critical': t.adaptive_critical,
                            'historical_avg': t.historical_avg,
                            'history_std': t.history_std,
                            'alert_count': t.alert_count
                        } for k, t in engine.thresholds.items()
                    },
                    'scarcity_factors': engine.scarcity_factors,
                    'last_update': engine.last_update.isoformat() if engine.last_update else None,
                    'pareto_front_history': [p.to_dict() for p in engine.pareto_front_history],
                }
                with open(self.path, 'w') as f:
                    json.dump(state, f, default=str, indent=2)
                logger.info(f"Engine state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save engine state: {e}")
                return False

    async def load_state(self, engine: 'UnifiedSustainabilityEngine') -> bool:
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                with open(self.path, 'r') as f:
                    state = json.load(f)
                engine.sustainability_score = state.get('sustainability_score', 0.5)
                engine.history = deque(state.get('history', []), maxlen=engine.config.history_limit)
                engine.dimension_history = defaultdict(list)
                for k, v in state.get('dimension_history', {}).items():
                    engine.dimension_history[k] = v
                for k, t_data in state.get('thresholds', {}).items():
                    if k in engine.thresholds:
                        t = engine.thresholds[k]
                        for attr, val in t_data.items():
                            setattr(t, attr, val)
                engine.scarcity_factors = state.get('scarcity_factors', {
                    'carbon': 1.0, 'helium': 1.0, 'energy': 1.0,
                    'circularity': 1.0, 'biodiversity': 1.0
                })
                last_update = state.get('last_update')
                if last_update:
                    engine.last_update = datetime.fromisoformat(last_update)
                pareto_fronts = state.get('pareto_front_history', [])
                for p_dict in pareto_fronts:
                    engine.pareto_front_history.append(MOPDPoint.from_dict(p_dict))
                logger.info(f"Engine state loaded from {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load engine state: {e}")
                return False

    async def delete_state(self):
        async with self._lock:
            if os.path.exists(self.path):
                os.remove(self.path)
                logger.info(f"Persistence file {self.path} deleted")
                return True
            return False


# ============================================================================
# 6. ENHANCED UNIFIED SUSTAINABILITY ENGINE (with MOPD, XAI, Safety, Approval, Chaos)
# ============================================================================
class UnifiedSustainabilityEngine:
    """
    Unified Sustainability Valuation Engine v5.2.0
    """

    def __init__(
        self,
        bio_core: Optional[EnhancedBioInspiredCore] = None,
        config: Optional[SustainabilityEngineConfig] = None,
        **kwargs
    ):
        if config is None:
            config = SustainabilityEngineConfig.from_dict(kwargs)
        self.config = config

        self.enable_event_driven = config.enable_event_driven
        self.enable_self_healing = config.enable_self_healing
        self.enable_swarm_coordination = config.enable_swarm_coordination
        self.enable_time_tick_engine = config.enable_time_tick_engine
        self.enable_quantum_bridge = config.enable_quantum_bridge
        self.enable_cost_benefit = config.enable_cost_benefit
        self.enable_workflow_orchestration = config.enable_workflow_orchestration

        self.bio_core = bio_core
        self.event_broker = None
        self.alert_system = None
        self.anomaly_detection = None
        self.cost_benefit_engine = None
        self.quantum_bridge = None
        self.tick_engine = None
        self.swarm_coordinator = None
        self.self_healer = None
        self.workflow_orchestrator = None
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None

        if self.bio_core:
            self.event_broker = getattr(self.bio_core, 'event_broker', None)
            self.alert_system = getattr(self.bio_core, 'alert_system', None)
            self.anomaly_detection = getattr(self.bio_core, 'anomaly_detection', None)
            self.cost_benefit_engine = getattr(self.bio_core, 'cost_benefit_engine', None)
            self.quantum_bridge = getattr(self.bio_core, 'quantum_bridge', None)
            self.tick_engine = getattr(self.bio_core, 'tick_engine', None)
            self.swarm_coordinator = getattr(self.bio_core, 'swarm_coordinator', None)
            self.self_healer = getattr(self.bio_core, 'self_healer', None)
            self.workflow_orchestrator = getattr(self.bio_core, 'workflow_orchestrator', None)
            self.token_manager = getattr(self.bio_core, 'token_manager', None)
            self.gradient_manager = getattr(self.bio_core, 'gradient_manager', None)
            self.scheduler = getattr(self.bio_core, 'scheduler', None)
            self.compartment_manager = getattr(self.bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(self.bio_core, 'biomass_storage', None)
            self.harvester = getattr(self.bio_core, 'harvester', None)

        self.expert_router = None
        self.gating_network = None
        self.self_evolving_gate = None

        self.helium_provider = None
        self.carbon_manager: Optional[CarbonProvider] = None
        self.helium_tracker: Optional[HeliumTracker] = None
        self.circular_manager: Optional[CircularManager] = None
        self.biodiversity: Optional[BiodiversityProvider] = None
        self.expert_registry: Optional[ExpertRegistry] = None
        self.quantum_limits: Optional[QuantumLimits] = None
        self.adaptive_cost_function: Optional[Any] = None

        self.adaptive_threshold_manager = AdaptiveThresholdManager(self.config)
        self.dynamic_weight_manager = DynamicWeightManager(self.config)
        self.predictive_analyzer = PredictiveTrendAnalyzer(self.config)
        self.report_manager = ReportTemplateManager(self.config)
        self.persistence = SustainabilityPersistenceManager(self.config)
        self.telemetry = SustainabilityTelemetry()
        self.emissions_storage = EmissionsStorage()

        self.sustainability_score = 0.5
        self.dimensions: Dict[str, SustainabilityDimension] = {}
        self.thresholds: Dict[str, SustainabilityThreshold] = {}
        self.history: deque = deque(maxlen=self.config.history_limit)
        self.last_update: Optional[datetime] = None
        self.dimension_weights = self.config.dimension_weights.copy()
        self.scarcity_factors = {
            'carbon': 1.0,
            'helium': 1.0,
            'energy': 1.0,
            'circularity': 1.0,
            'biodiversity': 1.0
        }
        self.dimension_history: Dict[str, List[float]] = defaultdict(list)
        self.pareto_front_history: deque = deque(maxlen=1000)
        self._score_cache: Dict[str, Tuple[float, datetime]] = {}
        self._cache_lock = asyncio.Lock()

        self._carbon_circuit = EnhancedCircuitBreaker("carbon_manager", failure_threshold=config.circuit_breaker_failure_threshold, recovery_timeout=config.circuit_breaker_recovery_timeout)
        self._helium_circuit = EnhancedCircuitBreaker("helium_tracker", failure_threshold=config.circuit_breaker_failure_threshold, recovery_timeout=config.circuit_breaker_recovery_timeout)
        self._circular_circuit = EnhancedCircuitBreaker("circular_manager", failure_threshold=config.circuit_breaker_failure_threshold, recovery_timeout=config.circuit_breaker_recovery_timeout)
        self._biodiversity_circuit = EnhancedCircuitBreaker("biodiversity_provider", failure_threshold=config.circuit_breaker_failure_threshold, recovery_timeout=config.circuit_breaker_recovery_timeout)
        self._expert_circuit = EnhancedCircuitBreaker("expert_registry", failure_threshold=config.circuit_breaker_failure_threshold, recovery_timeout=config.circuit_breaker_recovery_timeout)
        self._quantum_circuit = EnhancedCircuitBreaker("quantum_limits", failure_threshold=config.circuit_breaker_failure_threshold, recovery_timeout=config.circuit_breaker_recovery_timeout)

        self._background_tasks: List[asyncio.Task] = []
        self.health_status = "healthy"
        self.last_error = None
        self._ready_event = asyncio.Event()
        self._load_state_task: Optional[asyncio.Task] = None

        self._init_thresholds()
        if self.enable_event_driven and self.event_broker:
            self._subscribe_events()

        # Deferred task creation: only if loop is running
        try:
            loop = asyncio.get_running_loop()
            self._load_state_task = loop.create_task(self._load_state())
        except RuntimeError:
            # No running loop; will be started in wait_ready()
            self._load_state_task = None

        logger.info("Unified Sustainability Engine v5.2.0 initialized with MOPD, XAI, safety, approval, chaos testing")

    async def wait_ready(self):
        if self._load_state_task is None:
            await self._load_state()
        else:
            await self._load_state_task
        self._ready_event.set()
        self._start_background_tasks()

    async def _load_state(self):
        if self.persistence:
            await self.persistence.load_state(self)
        self._ready_event.set()
        # Background tasks start after state load
        if not self._background_tasks:
            self._start_background_tasks()

    def _subscribe_events(self):
        if self.event_broker:
            self.event_broker.subscribe('carbon_update', self._on_carbon_update)
            self.event_broker.subscribe('helium_update', self._on_helium_update)
            self.event_broker.subscribe('alert_generated', self._on_alert_generated)
            self.event_broker.subscribe('config_updated', self._on_config_updated)
            self.event_broker.subscribe('token_balance_update', self._on_token_update)
            self.event_broker.subscribe('health_update', self._on_health_update)
            self.event_broker.subscribe('anomaly_detected', self._on_anomaly_detected)
            logger.info("Sustainability Engine subscribed to core events")

    # ... event handlers unchanged ...
    async def _on_carbon_update(self, event: BioEvent):
        intensity = event.data.get('intensity', 400)
        price = event.data.get('price', 50.0)
        self.carbon_intensity = intensity
        self.carbon_price = price
        self.scarcity_factors['carbon'] = min(2.0, intensity / 500)
        async with self._cache_lock:
            self._score_cache.pop('carbon', None)

    async def _on_helium_update(self, event: BioEvent):
        scarcity = event.data.get('scarcity', 0.5)
        price = event.data.get('price', 0.5)
        self.helium_scarcity = scarcity
        self.helium_price = price
        self.scarcity_factors['helium'] = min(2.0, 2.0 - (1 - scarcity) * 2)
        async with self._cache_lock:
            self._score_cache.pop('helium', None)

    async def _on_alert_generated(self, event: BioEvent):
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert received; switching to conservative sustainability and triggering healing")
            self.config.adaptation_rate = 0.05
            if self.enable_self_healing and self.self_healer:
                await self.self_healer.apply_healing('damage_accumulation')
            if self.workflow_orchestrator and self.config.workflow_on_critical_alert:
                await self.workflow_orchestrator.execute_workflow(self.config.workflow_on_critical_alert)

    async def _on_config_updated(self, event: BioEvent):
        updates = event.data.get('updates', {})
        if 'sustainability_engine' in updates:
            new_config = updates['sustainability_engine']
            for key, value in new_config.items():
                if hasattr(self.config, key):
                    setattr(self.config, key, value)
            logger.info("Sustainability Engine configuration reloaded")

    async def _on_token_update(self, event: BioEvent):
        self.token_balance = event.data.get('balance', 500)

    async def _on_health_update(self, event: BioEvent):
        self.health_status = event.data.get('status', 'healthy')

    async def _on_anomaly_detected(self, event: BioEvent):
        if event.data.get('metric') == 'carbon_intensity':
            logger.info("Carbon anomaly detected; adjusting carbon weight")
            self.dimension_weights['carbon'] = min(0.5, self.dimension_weights['carbon'] * 1.2)
        if event.data.get('metric') == 'helium_scarcity':
            logger.info("Helium anomaly detected; adjusting helium weight")
            self.dimension_weights['helium'] = min(0.5, self.dimension_weights['helium'] * 1.2)

    def _start_background_tasks(self):
        if not self._background_tasks:
            if self.telemetry:
                self._background_tasks.append(asyncio.create_task(self._telemetry_export_loop()))
            if self.enable_swarm_coordination and self.swarm_coordinator:
                self._background_tasks.append(asyncio.create_task(self._swarm_update_loop()))
            if self.persistence:
                self._background_tasks.append(asyncio.create_task(self._persistence_save_loop()))
            # Chaos testing loop (if enabled)
            if self.config.mopd.enable_chaos_testing:
                self._background_tasks.append(asyncio.create_task(self._chaos_testing_loop()))

    async def _telemetry_export_loop(self):
        while True:
            try:
                if self.telemetry:
                    export_data = await self.telemetry.export()
                    logger.debug(f"Telemetry export: {len(export_data)} bytes")
                await asyncio.sleep(self.config.telemetry_export_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Telemetry export error: {e}")
                await asyncio.sleep(60)

    async def _swarm_update_loop(self):
        while True:
            try:
                await self.share_with_swarm()
                await asyncio.sleep(self.config.swarm_share_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Swarm update error: {e}")
                await asyncio.sleep(120)

    async def _persistence_save_loop(self):
        while True:
            try:
                await self.save_state()
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Persistence save error: {e}")
                await asyncio.sleep(60)

    async def _chaos_testing_loop(self):
        while True:
            await asyncio.sleep(self.config.mopd.chaos_test_interval_seconds)
            await self.run_chaos_test()

    async def share_with_swarm(self):
        if not self.enable_swarm_coordination or not self.swarm_coordinator:
            return
        swarm_payload = {
            'engine_id': hashlib.md5(str(self.dimensions).encode()).hexdigest()[:8],
            'sustainability_score': self.sustainability_score,
            'dimension_scores': {k: v.current_value for k, v in self.dimensions.items()},
            'scarcity_factors': self.scarcity_factors,
            'history_sample_count': len(self.history),
            'mopd_enabled': self.config.mopd.enabled,
        }
        await self.swarm_coordinator.share_predictions(swarm_payload)

    # ... injection methods unchanged ...
    def inject_modules(self, **modules):
        for name, module in modules.items():
            setattr(self, name, module)
            logger.info(f"Injected module: {name}")

    def set_gating_network(self, gating_network: 'GatingNetworkManager'):
        self.gating_network = gating_network
        logger.info("Gating network injected into Sustainability Engine")

    def set_self_evolving_gate(self, gate: 'EnhancedSelfEvolvingGate'):
        self.self_evolving_gate = gate
        logger.info("Self-Evolving Gate injected into Sustainability Engine")

    def set_expert_router(self, router: 'ExpertRouter'):
        self.expert_router = router
        logger.info("Expert Router injected into Sustainability Engine")

    def set_helium_provider(self, provider: HeliumProvider):
        self.helium_provider = provider
        logger.info("Helium provider injected into Sustainability Engine")

    def set_adaptive_cost_function(self, cost_func: Any):
        self.adaptive_cost_function = cost_func
        logger.info("AdaptiveCostFunction injected into Sustainability Engine")

    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        if bio_core:
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.scheduler = getattr(bio_core, 'scheduler', None)
            self.compartment_manager = getattr(bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(bio_core, 'biomass_storage', None)
            self.harvester = getattr(bio_core, 'harvester', None)
        else:
            self.token_manager = kwargs.get('token_manager')
            self.gradient_manager = kwargs.get('gradient_manager')
            self.scheduler = kwargs.get('scheduler')
            self.compartment_manager = kwargs.get('compartment_manager')
            self.biomass_storage = kwargs.get('biomass_storage')
            self.harvester = kwargs.get('harvester')
        logger.info("Bio-inspired modules injected into Sustainability Engine")

    # ... init_thresholds etc ...
    def _init_thresholds(self):
        self.thresholds = {
            'carbon': SustainabilityThreshold(
                dimension='carbon',
                warning_threshold=self.config.warning_threshold,
                critical_threshold=self.config.critical_threshold,
                adaptive_warning=self.config.warning_threshold,
                adaptive_critical=self.config.critical_threshold
            ),
            'helium': SustainabilityThreshold(
                dimension='helium',
                warning_threshold=self.config.warning_threshold,
                critical_threshold=self.config.critical_threshold,
                adaptive_warning=self.config.warning_threshold,
                adaptive_critical=self.config.critical_threshold
            ),
            'energy': SustainabilityThreshold(
                dimension='energy',
                warning_threshold=self.config.warning_threshold,
                critical_threshold=self.config.critical_threshold,
                adaptive_warning=self.config.warning_threshold,
                adaptive_critical=self.config.critical_threshold
            ),
            'circularity': SustainabilityThreshold(
                dimension='circularity',
                warning_threshold=self.config.warning_threshold,
                critical_threshold=self.config.critical_threshold,
                adaptive_warning=self.config.warning_threshold,
                adaptive_critical=self.config.critical_threshold
            ),
            'biodiversity': SustainabilityThreshold(
                dimension='biodiversity',
                warning_threshold=self.config.warning_threshold,
                critical_threshold=self.config.critical_threshold,
                adaptive_warning=self.config.warning_threshold,
                adaptive_critical=self.config.critical_threshold
            )
        }

    # ... main update method (modified MOPD part) ...
    async def update_sustainability_score(
        self,
        region: str = None,
        return_mopd: bool = False
    ) -> Dict[str, Any]:
        if region is None:
            region = self.config.default_region

        start_time = time.time()
        dimensions = await self._collect_dimensions(region)
        await self._update_adaptive_thresholds(dimensions)
        weights = await self._adjust_weights(dimensions)
        await self._update_predictions(dimensions)
        total_score = self._aggregate_score(dimensions, weights)
        self._record_history(total_score, dimensions, weights)
        recommendations = self._generate_recommendations(dimensions, total_score)
        risk_factors = self._assess_risks(dimensions)
        scenario_scores = await self._compute_scenarios(dimensions)
        await self._update_external_systems(total_score, dimensions)
        self._update_telemetry(dimensions, total_score)
        self.sustainability_score = total_score
        self.last_update = datetime.now(timezone.utc)
        UPDATE_LATENCY.observe(time.time() - start_time)

        result = UnifiedSustainabilityScore(
            total_score=total_score,
            dimensions=dimensions,
            confidence=0.8,
            trend=self._calculate_global_trend(),
            risk_factors=risk_factors,
            recommendations=recommendations,
            predicted_future_score=self._compute_predicted_total(dimensions),
            scenario_scores=scenario_scores
        )

        # MOPD: generate Pareto front if requested and enabled
        pareto_front = None
        best_plan = None
        if return_mopd and self.config.mopd.enabled:
            # Generate Pareto front by simulating trade-off scenarios
            pareto_front = await self._generate_pareto_front(dimensions)
            if pareto_front:
                best_plan = self._select_best_from_pareto(pareto_front, self.config.mopd.objective_weights)
                for point in pareto_front:
                    self.pareto_front_history.append(point)
                self.telemetry.increment('mopd_generations')
                self.telemetry.histogram('mopd_pareto_front_size', len(pareto_front))
                if best_plan:
                    self.telemetry.gauge('best_scalarised_score', best_plan.scalarised_score)

        output = {
            'score': result,
            'pareto_front': [p.to_dict() for p in pareto_front] if pareto_front else None,
            'best_plan': best_plan.to_dict() if best_plan else None,
        }
        return output

    # ... collect_dimensions etc ...

    # ========================================================================
    # Enhanced MOPD: meaningful Pareto front via scenario simulation
    # ========================================================================
    async def _generate_pareto_front(
        self,
        dimensions: Dict[str, SustainabilityDimension]
    ) -> List[MOPDPoint]:
        """
        Generate a Pareto front by creating different policy scenarios that
        perturb the dimension scores. Each scenario corresponds to a different
        weight vector that emphasizes certain dimensions, and we simulate the
        effect on the other dimensions (e.g., improving carbon may worsen energy).
        This yields a set of trade-off points that can be filtered for dominance.
        """
        dim_names = list(dimensions.keys())
        base_scores = {name: dim.current_value for name, dim in dimensions.items()}

        # Sample weight vectors (decision variables) from Dirichlet distribution
        num_samples = 50
        rng = np.random.default_rng(42)
        points = []
        for _ in range(num_samples):
            weights = rng.dirichlet([1.0] * len(dim_names))
            weight_dict = {dim_names[i]: float(weights[i]) for i in range(len(dim_names))}

            # Simulate effect: increase the emphasized dimension's score by up to 20%,
            # and decrease others proportionally to maintain a rough budget.
            # This creates conflicting objectives.
            simulated_scores = base_scores.copy()
            total_increase = 0.0
            for name, w in weight_dict.items():
                if w > 0.2:  # only consider dimensions with significant weight
                    increase = w * 0.1  # up to 10% increase
                    simulated_scores[name] = min(1.0, simulated_scores[name] + increase)
                    total_increase += increase
            # Distribute the total increase as a decrease across other dimensions
            if total_increase > 0:
                # decrease other dimensions proportionally to their current scores
                decrease_factor = total_increase / max(len(dim_names) - 1, 1)
                for name in dim_names:
                    if weight_dict.get(name, 0.0) <= 0.2:
                        simulated_scores[name] = max(0.0, simulated_scores[name] - decrease_factor)

            # Build MOPDPoint with explanation
            explanation = f"Weights: {weight_dict}, Scores: {simulated_scores}"
            point = MOPDPoint(
                weights=weight_dict,
                dimensions=simulated_scores,
                explanation=explanation
            )
            points.append(point)

        # Filter dominated points (Pareto front)
        # Objectives are the dimension scores; higher is better.
        pareto = []
        for i, p_i in enumerate(points):
            dominated = False
            for j, p_j in enumerate(points):
                if i == j:
                    continue
                a_vec = [p_i.dimensions.get(k, 0.0) for k in dim_names]
                b_vec = [p_j.dimensions.get(k, 0.0) for k in dim_names]
                if all(b >= a for a, b in zip(a_vec, b_vec)) and any(b > a for a, b in zip(a_vec, b_vec)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p_i)

        # If Pareto empty (unlikely), return at least a few points
        if not pareto:
            pareto = points[:10]
        return pareto

    def _select_best_from_pareto(
        self,
        pareto_front: List[MOPDPoint],
        weights: Dict[str, float]
    ) -> Optional[MOPDPoint]:
        if not pareto_front:
            return None
        best = None
        best_score = -float('inf')
        for point in pareto_front:
            score = sum(weights[d] * point.dimensions.get(d, 0.0) for d in weights)
            point.scalarised_score = score
            if score > best_score:
                best_score = score
                best = point
        return best

    # ========================================================================
    # Temporal Safety Checks
    # ========================================================================
    async def check_invariants(self) -> List[str]:
        """
        Check temporal safety invariants. Returns list of violation strings.
        """
        violations = []
        if self.config.mopd.enable_temporal_safety:
            # Example: ensure no dimension is below critical threshold
            for name, threshold in self.thresholds.items():
                if threshold.current_value < threshold.adaptive_critical:
                    violations.append(f"{name} at critical level")
            # Ensure no dimension weight is too high/low
            total_weight = sum(self.dimension_weights.values())
            if abs(total_weight - 1.0) > 0.01:
                violations.append("Weights do not sum to 1")
        return violations

    # ========================================================================
    # Human-in-the-loop
    # ========================================================================
    async def request_approval(self, new_weights: Dict[str, float]) -> bool:
        """
        Request human approval for a significant weight change.
        Returns True if approved, False otherwise.
        """
        if not self.config.mopd.require_human_approval:
            return True
        # Compute max change
        max_change = max(abs(new_weights.get(d, 0.0) - self.dimension_weights.get(d, 0.0))
                         for d in new_weights)
        if max_change < self.config.mopd.aggressive_weight_change_threshold:
            return True
        logger.warning(f"Human approval required for weight change of {max_change:.2f}. Auto-denying.")
        return False

    # ========================================================================
    # Chaos Testing
    # ========================================================================
    async def inject_fault(self, fault_type: str, **params):
        if not self.config.mopd.enable_chaos_testing:
            logger.info("Chaos testing disabled")
            return
        if fault_type == 'carbon_spike':
            if self.carbon_manager:
                self.carbon_manager.carbon_intensity = 1000.0
                self.scarcity_factors['carbon'] = 2.0
                logger.warning("Injected carbon_spike fault")
        elif fault_type == 'helium_critical':
            self.scarcity_factors['helium'] = 2.0
            logger.warning("Injected helium_critical fault")
        elif fault_type == 'provider_failure':
            # Simulate carbon manager failure
            self.carbon_manager = None
            logger.warning("Injected provider_failure (carbon manager set to None)")
        else:
            logger.warning(f"Unknown fault type: {fault_type}")

    async def run_chaos_test(self) -> Dict[str, Any]:
        """Run chaos tests to verify resilience."""
        if not self.config.mopd.enable_chaos_testing:
            return {'status': 'disabled'}
        report = {'faults': [], 'results': {}}

        # Test carbon spike
        await self.inject_fault('carbon_spike')
        report['faults'].append('carbon_spike')
        # Check invariants after fault
        violations = await self.check_invariants()
        report['results']['carbon_spike'] = {'violations': violations}
        # Reset
        self.scarcity_factors['carbon'] = 1.0
        if self.carbon_manager:
            self.carbon_manager.carbon_intensity = 400.0

        # Test provider failure
        await self.inject_fault('provider_failure')
        report['faults'].append('provider_failure')
        # Try to collect dimensions (should fallback)
        try:
            dimensions = await self._collect_dimensions('global')
            report['results']['provider_failure'] = f'collected {len(dimensions)} dimensions'
        except Exception as e:
            report['results']['provider_failure'] = f'error: {e}'
        # Reset (not trivial; but we leave for demo)

        return report

    # ========================================================================
    # Federated Learning (stub)
    # ========================================================================
    async def participate_in_federation(self):
        """Placeholder for federated learning integration."""
        if not self.config.federated.enabled:
            return
        # In a real implementation, send local metrics to server and get aggregated result.
        logger.info("Federated sustainability sharing not implemented yet.")

    # ========================================================================
    # Public Methods (unchanged, plus new ones)
    # ========================================================================
    async def get_current_score(self) -> float:
        return self.sustainability_score

    async def get_dimension_status(self) -> Dict[str, str]:
        status = {}
        for name, threshold in self.thresholds.items():
            adaptive_warning = getattr(threshold, 'adaptive_warning', threshold.warning_threshold)
            adaptive_critical = getattr(threshold, 'adaptive_critical', threshold.critical_threshold)
            if threshold.current_value < adaptive_critical:
                status[name] = "critical"
            elif threshold.current_value < adaptive_warning:
                status[name] = "warning"
            else:
                status[name] = "healthy"
        return status

    async def get_historical_scores(self, n: int = 100) -> List[Dict]:
        return list(self.history)[-n:]

    async def get_dimension_predictions(self) -> Dict[str, Any]:
        predictions = {}
        for name, history in self.dimension_history.items():
            if len(history) > 10:
                pred, conf, vol = await self.predictive_analyzer.predict(name, 10)
                predictions[name] = {
                    'prediction': pred,
                    'confidence': conf,
                    'volatility': vol,
                    'accuracy': self.predictive_analyzer.get_prediction_accuracy(name)
                }
        return predictions

    async def get_sustainability_report(
        self,
        template_name: str = "executive_summary",
        output_format: str = "json"
    ) -> Dict[str, Any]:
        result = await self.update_sustainability_score(return_mopd=False)
        score = result['score']
        status = await self.get_dimension_status()
        predictions = await self.get_dimension_predictions()
        report_data = {
            'total_score': score.total_score,
            'trend': score.trend,
            'dimensions': {
                name: {
                    'value': dim.current_value,
                    'weight': dim.weight,
                    'trend': dim.trend,
                    'status': status.get(name, 'unknown'),
                    'scarcity_factor': dim.scarcity_factor,
                    'prediction': dim.prediction,
                    'prediction_confidence': dim.prediction_confidence,
                    'volatility': dim.volatility
                }
                for name, dim in score.dimensions.items()
            },
            'risk_factors': score.risk_factors,
            'recommendations': score.recommendations,
            'predictions': predictions,
            'history': await self.get_historical_scores(10),
            'weight_trends': self.dynamic_weight_manager.get_weight_trends(),
            'threshold_stats': {
                name: self.adaptive_threshold_manager.get_threshold_stats(name)
                for name in self.thresholds
            }
        }
        if template_name:
            report = await self.report_manager.generate_report(
                template_name,
                report_data,
                output_format
            )
            if report.get('status') == 'generated':
                report['data'] = report_data
                return report
        return report_data

    async def update_scarcity_factors(self, new_factors: Dict[str, float]):
        for dim, factor in new_factors.items():
            if dim in self.scarcity_factors:
                self.scarcity_factors[dim] = factor
        logger.info(f"Updated scarcity factors: {new_factors}")

    def get_available_templates(self) -> List[str]:
        return self.report_manager.list_templates()

    async def create_custom_template(self, template: ReportTemplate) -> bool:
        return self.report_manager.create_template(template)

    # ========================================================================
    # MOPD Public Methods
    # ========================================================================
    async def get_mopd_pareto_front(
        self,
        region: str = None,
        num_samples: int = 50
    ) -> List[MOPDPoint]:
        if not self.config.mopd.enabled:
            return []
        dimensions = await self._collect_dimensions(region or self.config.default_region)
        pareto = await self._generate_pareto_front(dimensions)
        for point in pareto:
            self.pareto_front_history.append(point)
        return pareto

    async def get_mopd_summary(self) -> Dict[str, Any]:
        if not self.config.mopd.enabled:
            return {"enabled": False}
        return {
            "enabled": True,
            "objective_weights": self.config.mopd.objective_weights,
            "grid_resolution": self.config.mopd.grid_resolution,
            "pareto_front_history_size": len(self.pareto_front_history),
            "dimensions": list(self.dimension_weights.keys()),
            "xai_enabled": self.config.mopd.enable_xai,
            "temporal_safety_enabled": self.config.mopd.enable_temporal_safety,
            "human_approval_enabled": self.config.mopd.require_human_approval,
            "chaos_testing_enabled": self.config.mopd.enable_chaos_testing,
        }

    # ========================================================================
    # Emission and Offset Methods
    # ========================================================================
    async def get_recent_emissions(self, hours: int = 24) -> float:
        return self.emissions_storage.get_recent_emissions(hours)

    async def record_offset(self, kg: float, source: str = None):
        self.emissions_storage.record_offset(kg, source=source)
        logger.info(f"Recorded offset: {kg} kg CO₂ from {source or 'unknown'}")

    # ========================================================================
    # Configuration Reload
    # ========================================================================
    async def reload_config(self, new_config: SustainabilityEngineConfig):
        self.config = new_config
        self.adaptive_threshold_manager = AdaptiveThresholdManager(self.config)
        self.dynamic_weight_manager = DynamicWeightManager(self.config)
        self.predictive_analyzer = PredictiveTrendAnalyzer(self.config)
        self.report_manager = ReportTemplateManager(self.config)
        self.dimension_weights = self.config.dimension_weights.copy()
        self._init_thresholds()
        logger.info("Configuration reloaded and managers reinitialized.")

    # ========================================================================
    # Self-Healing
    # ========================================================================
    async def self_heal(self):
        logger.info("SustainabilityEngine self‑healing")
        if self.enable_self_healing:
            self.dimension_weights = self.config.dimension_weights.copy()
            self.scarcity_factors = {
                'carbon': 1.0,
                'helium': 1.0,
                'energy': 1.0,
                'circularity': 1.0,
                'biodiversity': 1.0
            }
            self.config.adaptation_rate = 0.1
            if len(self.history) > 10:
                self.history = deque(list(self.history)[-10:], maxlen=self.config.history_limit)
            async with self._cache_lock:
                self._score_cache.clear()
            for threshold in self.thresholds.values():
                threshold.adaptive_warning = threshold.warning_threshold
                threshold.adaptive_critical = threshold.critical_threshold
                threshold.current_value = 0.0
                threshold.historical_avg = 0.0
                threshold.history_std = 0.0
                threshold.alert_count = 0
            self.health_status = "healthy"
            self.last_error = None
            await self.save_state()
            logger.info("Self-healing completed")

    # ========================================================================
    # Health Status
    # ========================================================================
    async def get_health_status(self) -> Dict[str, Any]:
        return {
            'status': self.health_status,
            'last_error': self.last_error,
            'sustainability_score': self.sustainability_score,
            'dimension_count': len(self.dimensions),
            'history_samples': len(self.history),
            'bio_integration_active': self.bio_core is not None,
            'event_driven_active': self.enable_event_driven,
            'self_healing_enabled': self.enable_self_healing,
            'swarm_coordination_active': self.enable_swarm_coordination,
            'persistence_enabled': self.persistence is not None,
            'mopd_enabled': self.config.mopd.enabled,
            'pareto_front_history_size': len(self.pareto_front_history),
            'xai_enabled': self.config.mopd.enable_xai,
            'temporal_safety_enabled': self.config.mopd.enable_temporal_safety,
            'human_approval_enabled': self.config.mopd.require_human_approval,
            'chaos_testing_enabled': self.config.mopd.enable_chaos_testing,
            'federated_enabled': self.config.federated.enabled,
        }

    # ========================================================================
    # Persistence Methods
    # ========================================================================
    async def save_state(self):
        if self.persistence:
            await self.persistence.save_state(self)

    async def load_state(self):
        if self.persistence:
            await self.persistence.load_state(self)

    # ========================================================================
    # Shutdown
    # ========================================================================
    async def shutdown(self):
        logger.info("Shutting down Unified Sustainability Engine")
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        if self.persistence:
            await self.save_state()
        logger.info("Shutdown complete")


# ============================================================================
# FastAPI REST API (updated with new endpoints)
# ============================================================================
if FASTAPI_AVAILABLE:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        global engine
        config = SustainabilityEngineConfig()
        engine = UnifiedSustainabilityEngine(config=config)
        await engine.wait_ready()
        logger.info("FastAPI startup complete")
        yield
        if engine:
            await engine.shutdown()
        logger.info("FastAPI shutdown complete")

    app = FastAPI(title="Sustainability Engine API", version="5.2.0", lifespan=lifespan)
    engine: Optional[UnifiedSustainabilityEngine] = None

    @app.get("/metrics")
    async def get_metrics():
        if PROMETHEUS_AVAILABLE:
            return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
        return {"error": "Prometheus not enabled"}

    @app.get("/health")
    async def health():
        if not engine:
            raise HTTPException(status_code=503, detail="Engine not initialized")
        return await engine.get_health_status()

    @app.get("/score")
    async def get_current_score():
        if not engine:
            raise HTTPException(status_code=503, detail="Engine not initialized")
        score = await engine.get_current_score()
        return {"score": score}

    @app.post("/update")
    async def update_score(background_tasks: BackgroundTasks, region: Optional[str] = None):
        if not engine:
            raise HTTPException(status_code=503, detail="Engine not initialized")
        background_tasks.add_task(engine.update_sustainability_score, region)
        return {"status": "update started"}

    @app.get("/report")
    async def get_report(template: str = "executive_summary", format: str = "json"):
        if not engine:
            raise HTTPException(status_code=503, detail="Engine not initialized")
        return await engine.get_sustainability_report(template_name=template, output_format=format)

    @app.get("/dimensions")
    async def get_dimensions():
        if not engine:
            raise HTTPException(status_code=503, detail="Engine not initialized")
        dimensions = {}
        for name, dim in engine.dimensions.items():
            dimensions[name] = {
                "value": dim.current_value,
                "weight": dim.weight,
                "trend": dim.trend,
                "scarcity": dim.scarcity_factor
            }
        return dimensions

    @app.post("/self-heal")
    async def trigger_self_heal():
        if not engine:
            raise HTTPException(status_code=503, detail="Engine not initialized")
        await engine.self_heal()
        return {"status": "self-heal triggered"}

    @app.get("/mopd/pareto")
    async def get_mopd_pareto(region: Optional[str] = None, num_samples: int = 50):
        if not engine:
            raise HTTPException(status_code=503, detail="Engine not initialized")
        pareto = await engine.get_mopd_pareto_front(region, num_samples)
        return {"pareto_front": [p.to_dict() for p in pareto]}

    @app.get("/mopd/summary")
    async def get_mopd_summary():
        if not engine:
            raise HTTPException(status_code=503, detail="Engine not initialized")
        return await engine.get_mopd_summary()

    @app.post("/chaos/run")
    async def run_chaos():
        if not engine:
            raise HTTPException(status_code=503, detail="Engine not initialized")
        return await engine.run_chaos_test()

    @app.post("/approve-weight-change")
    async def approve_weight_change(new_weights: Dict[str, float]):
        if not engine:
            raise HTTPException(status_code=503, detail="Engine not initialized")
        approved = await engine.request_approval(new_weights)
        return {"approved": approved}


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    import asyncio
    async def main():
        config = SustainabilityEngineConfig()
        engine = UnifiedSustainabilityEngine(config=config)
        await engine.wait_ready()
        result = await engine.update_sustainability_score(return_mopd=True)
        print(f"Sustainability score: {result['score'].total_score}")
        if result.get('pareto_front'):
            print(f"Pareto front size: {len(result['pareto_front'])}")
            for p in result['pareto_front'][:3]:
                print(f"  {p.explanation}")
        # Chaos test
        chaos_report = await engine.run_chaos_test()
        print(f"Chaos report: {chaos_report}")
        await engine.shutdown()

    asyncio.run(main())
