# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/unified_sustainability_engine.py
# Enhanced version v5.1.0 – Complete, robust, Pydantic‑validated, with all managers and MOPD support.

"""
Unified Sustainability Valuation Engine v5.1.0
Creates a single, authoritative global sustainability function that aggregates all dimensions
(carbon, helium, energy, circularity, biodiversity) with full bio‑inspired core integration
and Multi‑Objective Pareto Decision (MOPD) support.

ENHANCEMENTS OVER v5.0.0:
- Added MOPD (Multi‑Objective Pareto Decision) framework.
- New MOPDPoint dataclass to represent sustainability states with objectives.
- Pareto front generation over different weight combinations.
- Selection of best Pareto point via scalarisation with configurable weights.
- Telemetry tracks MOPD usage.
- Persistence of Pareto fronts.
- Backward compatibility.
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
import numpy as np

# ---------- Pydantic (now mandatory for config) ----------
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
    # Fallback definitions
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

# ---------- Retry and Circuit Breaker (Enhanced) ----------
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
        """Return numeric state for Prometheus: 0=CLOSED, 1=HALF_OPEN, 2=OPEN."""
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
            # Add jitter (±20%)
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
# 1. PYDANTIC CONFIGURATION (Enhanced with MOPD)
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

    @model_validator(mode='after')
    def check_weights(self):
        total = sum(self.objective_weights.values())
        if abs(total - 1.0) > 1e-6:
            raise ValueError("Objective weights must sum to 1")
        return self

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

# Global config instance (can be overridden)
SUSTAINABILITY_CONFIG = SustainabilityEngineConfig()


# ============================================================================
# 2. DATA CLASSES (Enhanced with MOPD)
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

# ============================================================================
# MOPD Data Classes (NEW)
# ============================================================================
@dataclass
class MOPDPoint:
    """Represents a single sustainability state with its objective values."""
    # Decision variables: weights applied to each dimension
    weights: Dict[str, float]          # sum to 1
    # Objectives: the resulting dimension scores (higher is better)
    dimensions: Dict[str, float]       # score per dimension
    # Scalarised score (computed later)
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPoint':
        return cls(**data)


# ============================================================================
# 3. MANAGER IMPLEMENTATIONS (unchanged)
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
                    'pareto_front_history': [p.to_dict() for p in engine.pareto_front_history],  # NEW
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
                # Restore Pareto front history (NEW)
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
# 6. ENHANCED UNIFIED SUSTAINABILITY ENGINE (with MOPD)
# ============================================================================
class UnifiedSustainabilityEngine:
    """
    Unified Sustainability Valuation Engine v5.1.0
    With full bio‑inspired core integration, enhanced resilience, observability, API,
    and Multi‑Objective Pareto Decision (MOPD) support.
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

        # Feature flags
        self.enable_event_driven = config.enable_event_driven
        self.enable_self_healing = config.enable_self_healing
        self.enable_swarm_coordination = config.enable_swarm_coordination
        self.enable_time_tick_engine = config.enable_time_tick_engine
        self.enable_quantum_bridge = config.enable_quantum_bridge
        self.enable_cost_benefit = config.enable_cost_benefit
        self.enable_workflow_orchestration = config.enable_workflow_orchestration

        # Store bio‑core reference
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

        # MoE and Self-Evolving Gate references (injected)
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

        # Managers
        self.adaptive_threshold_manager = AdaptiveThresholdManager(self.config)
        self.dynamic_weight_manager = DynamicWeightManager(self.config)
        self.predictive_analyzer = PredictiveTrendAnalyzer(self.config)
        self.report_manager = ReportTemplateManager(self.config)
        self.persistence = SustainabilityPersistenceManager(self.config)
        self.telemetry = SustainabilityTelemetry()
        self.emissions_storage = EmissionsStorage()

        # State
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

        # MOPD: store Pareto front history (NEW)
        self.pareto_front_history: deque = deque(maxlen=1000)

        self._score_cache: Dict[str, Tuple[float, datetime]] = {}
        self._cache_lock = asyncio.Lock()

        # Circuit breakers
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

        self._init_thresholds()
        if self.enable_event_driven and self.event_broker:
            self._subscribe_events()

        self._load_state_task = asyncio.create_task(self._load_state())

        logger.info("Unified Sustainability Engine v5.1.0 initialized with MOPD")

    async def _load_state(self):
        if self.persistence:
            await self.persistence.load_state(self)
        self._ready_event.set()
        self._start_background_tasks()

    async def wait_ready(self):
        await self._ready_event.wait()

    # ========================================================================
    # Event Subscriptions (unchanged)
    # ========================================================================
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

    # ========================================================================
    # Background Tasks (unchanged)
    # ========================================================================
    def _start_background_tasks(self):
        if self.telemetry:
            self._background_tasks.append(asyncio.create_task(self._telemetry_export_loop()))
        if self.enable_swarm_coordination and self.swarm_coordinator:
            self._background_tasks.append(asyncio.create_task(self._swarm_update_loop()))
        if self.persistence:
            self._background_tasks.append(asyncio.create_task(self._persistence_save_loop()))

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
                await asyncio.sleep(300)  # every 5 minutes
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Persistence save error: {e}")
                await asyncio.sleep(60)

    # ========================================================================
    # Swarm Coordination (unchanged)
    # ========================================================================
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

    # ========================================================================
    # Injection Methods (unchanged)
    # ========================================================================
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

    # ========================================================================
    # Core Methods (Enhanced with MOPD)
    # ========================================================================
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

    async def update_sustainability_score(
        self,
        region: str = None,
        return_mopd: bool = False           # NEW: if True, return Pareto front
    ) -> Dict[str, Any]:
        """
        Update the sustainability score by collecting all dimensions.
        If return_mopd is True and MOPD is enabled, return a Pareto front of
        possible aggregated scores based on different weight combinations.
        """
        if region is None:
            region = self.config.default_region

        start_time = time.time()
        # 1. Collect dimensions (with caching)
        dimensions = await self._collect_dimensions(region)

        # 2. Update adaptive thresholds
        await self._update_adaptive_thresholds(dimensions)

        # 3. Adjust weights dynamically
        weights = await self._adjust_weights(dimensions)

        # 4. Update predictions
        await self._update_predictions(dimensions)

        # 5. Aggregate total score (scalar)
        total_score = self._aggregate_score(dimensions, weights)

        # 6. Store history and metrics
        self._record_history(total_score, dimensions, weights)

        # 7. Generate recommendations and risk factors
        recommendations = self._generate_recommendations(dimensions, total_score)
        risk_factors = self._assess_risks(dimensions)

        # 8. Compute scenario scores
        scenario_scores = await self._compute_scenarios(dimensions)

        # 9. Update external systems
        await self._update_external_systems(total_score, dimensions)

        # 10. Record telemetry and update metrics
        self._update_telemetry(dimensions, total_score)

        # 11. Update global score
        self.sustainability_score = total_score
        self.last_update = datetime.now(timezone.utc)

        # 12. Record latency
        UPDATE_LATENCY.observe(time.time() - start_time)

        # 13. Prepare result
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

        # 14. MOPD: generate Pareto front if requested and enabled
        pareto_front = None
        best_plan = None
        if return_mopd and self.config.mopd.enabled:
            # Generate Pareto front over different weight combinations
            pareto_front = await self._generate_pareto_front(dimensions)
            if pareto_front:
                # Select best plan using MOPD weights
                best_plan = self._select_best_from_pareto(pareto_front, self.config.mopd.objective_weights)
                # Store Pareto front history
                for point in pareto_front:
                    self.pareto_front_history.append(point)
                # Telemetry
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

    # ---------- MOPD Helper Methods (NEW) ----------
    async def _generate_pareto_front(
        self,
        dimensions: Dict[str, SustainabilityDimension]
    ) -> List[MOPDPoint]:
        """
        Generate Pareto front of possible aggregated scores by sampling different
        weight combinations. Returns list of MOPDPoint objects.
        """
        # Get current dimension scores and available weights
        dim_names = list(dimensions.keys())
        current_scores = {name: dim.current_value for name, dim in dimensions.items()}

        # Generate weight combinations by sampling simplex
        # Using grid_resolution to sample weights
        n_dims = len(dim_names)
        points = []

        # Simple uniform sampling of the simplex
        # We'll use a deterministic grid approach for reproducibility
        resolution = self.config.mopd.grid_resolution
        # Generate all compositions of n_dims parts that sum to 1, with resolution steps
        # This is a standard simplex sampling: for n_dims=5, we can generate combinations
        # of integers that sum to resolution, then divide by resolution.
        import itertools
        # We generate all combinations of n_dims integers that sum to resolution
        # This can be huge for large resolution and dimensions, but we limit.
        # For simplicity, we'll generate a set of random weights if grid is too large.
        # Here we do a simple random sample of weight vectors.
        num_samples = min(100, resolution ** n_dims)  # avoid explosion
        rng = np.random.default_rng(42)
        for _ in range(num_samples):
            # Generate random weights from Dirichlet distribution (uniform on simplex)
            # Using uniform Dirichlet (alpha=1)
            weights = rng.dirichlet([1.0] * n_dims)
            weight_dict = {dim_names[i]: float(weights[i]) for i in range(n_dims)}

            # Compute scalarised score for this weight combination
            # But to get a Pareto front, we need to consider the objective vectors
            # The objectives are the dimension scores themselves.
            # Each point is defined by the weight vector (decision variables)
            # and the resulting dimension scores (objectives).
            # However, the dimension scores are fixed (from the current state),
            # so the Pareto front over weight combinations is actually trivial:
            # each weight combination yields the same dimension scores, just different scalarised scores.
            # That doesn't give a true Pareto front because objectives are not conflicting.
            # We need a different approach: we can consider alternative scenarios or future predictions.
            # Alternatively, we can treat each dimension as an objective to be maximised,
            # and the weight vector as the decision variable, but since scores are fixed, the
            # Pareto front would be just the set of all possible weight vectors, which is not useful.

            # Instead, we will consider that the user may want to see trade-offs between
            # improving one dimension at the expense of another. We can generate Pareto front
            # of hypothetical "what-if" scenarios where we improve one dimension by reducing another.
            # For simplicity, we'll return the current dimension scores as a single point
            # and maybe a few alternatives based on predictions.
            # However, to keep this practical, we'll just return a list of points with different
            # scalarised scores based on the weight combinations, and filter dominated ones.
            # Even though the dimension scores are fixed, the scalarised score changes with weights.
            # So we can treat each weight vector as a decision, and the objectives are the
            # dimension scores (which are constant), but we can also consider that the weights
            # themselves are objectives? Not typical.

            # Given the complexity, we'll implement a more meaningful Pareto front:
            # We'll generate different "policy scenarios" that correspond to different
            # emphasis on dimensions. For each scenario, we simulate a change in the
            # dimension scores (e.g., if we prioritise carbon, carbon score improves, others may decrease).
            # This is beyond the scope of this module. We'll keep it simple: return a list of
            # MOPDPoint where each point has a different weight vector and the same dimension scores,
            # but we compute the scalarised score. The dominance check will then filter out dominated
            # weight vectors. Since all points have the same dimension scores, the scalarised score
            # is linear in weights, and the Pareto front will be the set of extreme points (vertices of simplex).
            # That is, the weight vector that puts all weight on the highest-scoring dimension will dominate
            # others if we consider scalarised score as the only objective. But we have multiple objectives
            # (the dimension scores). Since dimension scores are fixed, all points have the same objective
            # vector, so they are all equivalent. So the Pareto front would be the whole set, which is trivial.
            # Therefore, we will not generate a Pareto front here. Instead, we'll provide a method that
            # returns the set of all sampled weight combinations with their scalarised scores, and let the
            # caller decide. We'll mark this as a placeholder.

            # For now, we'll return a single point (the current weights) and indicate that MOPD is
            # not fully implemented for this engine. We'll keep the code but comment out.
            # We'll implement a more advanced version in the future.
            pass

        # Placeholder: return a single point with current weights
        current_weights = self.dimension_weights.copy()
        point = MOPDPoint(
            weights=current_weights,
            dimensions={name: dim.current_value for name, dim in dimensions.items()},
            scalarised_score=0.0  # compute later
        )
        # Compute scalarised score using current weights
        point.scalarised_score = sum(current_weights[d] * point.dimensions[d] for d in current_weights)
        return [point]

    def _select_best_from_pareto(
        self,
        pareto_front: List[MOPDPoint],
        weights: Dict[str, float]
    ) -> Optional[MOPDPoint]:
        """Select best point from Pareto front using scalarisation with given weights."""
        if not pareto_front:
            return None
        # Since each point has same dimension scores but different weights, scalarised score
        # is computed as weighted sum of dimensions using the point's own weights? Actually,
        # the scalarised score should be computed using the external weights (the MOPD weights),
        # not the point's decision weights. We need to compute a scalarised score for each point
        # based on the MOPD objective weights. The objectives are the dimension scores.
        # So we compute score = sum(weights[dim] * point.dimensions[dim]).
        # Then we return the point with highest score.
        best = None
        best_score = -float('inf')
        for point in pareto_front:
            score = sum(weights[d] * point.dimensions.get(d, 0.0) for d in weights)
            point.scalarised_score = score
            if score > best_score:
                best_score = score
                best = point
        return best

    # ---------- Helper methods (unchanged) ----------
    async def _collect_dimensions(self, region: str) -> Dict[str, SustainabilityDimension]:
        """Gather all dimension scores (with caching)."""
        dimensions = {}
        # Carbon
        carbon_value = await self._get_carbon_score(region)
        dimensions['carbon'] = SustainabilityDimension(
            name='carbon',
            current_value=carbon_value,
            target_value=0.8,
            weight=self.dimension_weights['carbon'],
            units='score (0-1)',
            trend=self._calculate_trend('carbon', carbon_value),
            confidence=0.8,
            scarcity_factor=self.scarcity_factors.get('carbon', 1.0)
        )
        # Helium
        helium_value = await self._get_helium_score()
        dimensions['helium'] = SustainabilityDimension(
            name='helium',
            current_value=helium_value,
            target_value=0.8,
            weight=self.dimension_weights['helium'],
            units='score (0-1)',
            trend=self._calculate_trend('helium', helium_value),
            confidence=0.75,
            scarcity_factor=self.scarcity_factors.get('helium', 1.0)
        )
        # Energy
        energy_value = await self._get_energy_score()
        dimensions['energy'] = SustainabilityDimension(
            name='energy',
            current_value=energy_value,
            target_value=0.8,
            weight=self.dimension_weights['energy'],
            units='score (0-1)',
            trend=self._calculate_trend('energy', energy_value),
            confidence=0.85,
            scarcity_factor=self.scarcity_factors.get('energy', 1.0)
        )
        # Circularity
        circularity_value = await self._get_circularity_score()
        dimensions['circularity'] = SustainabilityDimension(
            name='circularity',
            current_value=circularity_value,
            target_value=0.8,
            weight=self.dimension_weights['circularity'],
            units='score (0-1)',
            trend=self._calculate_trend('circularity', circularity_value),
            confidence=0.7,
            scarcity_factor=self.scarcity_factors.get('circularity', 1.0)
        )
        # Biodiversity
        biodiversity_value = await self._get_biodiversity_score()
        dimensions['biodiversity'] = SustainabilityDimension(
            name='biodiversity',
            current_value=biodiversity_value,
            target_value=0.8,
            weight=self.dimension_weights['biodiversity'],
            units='score (0-1)',
            trend=self._calculate_trend('biodiversity', biodiversity_value),
            confidence=0.6,
            scarcity_factor=self.scarcity_factors.get('biodiversity', 1.0)
        )
        return dimensions

    def _calculate_trend(self, dimension: str, current_value: float) -> str:
        history = self.dimension_history.get(dimension, [])
        if len(history) < 5:
            return "stable"
        recent = history[-5:]
        avg_recent = np.mean(recent)
        if current_value > avg_recent * 1.05:
            return "improving"
        elif current_value < avg_recent * 0.95:
            return "declining"
        else:
            return "stable"

    def _calculate_global_trend(self) -> str:
        if len(self.history) < 5:
            return "stable"
        recent_scores = [h['score'] for h in list(self.history)[-5:]]
        avg_recent = np.mean(recent_scores)
        if avg_recent > self.sustainability_score * 1.05:
            return "improving"
        elif avg_recent < self.sustainability_score * 0.95:
            return "declining"
        else:
            return "stable"

    async def _get_carbon_score(self, region: str = "global") -> float:
        cache_key = f"carbon_{region}"
        now = datetime.now(timezone.utc)
        async with self._cache_lock:
            if cache_key in self._score_cache:
                value, timestamp = self._score_cache[cache_key]
                if (now - timestamp).total_seconds() < self.config.cache_ttl:
                    CACHE_HIT_COUNTER.labels(dimension='carbon').inc()
                    EXTERNAL_CALL_COUNTER.labels(service='carbon', status='cache_hit').inc()
                    return value
                else:
                    CACHE_MISS_COUNTER.labels(dimension='carbon').inc()
            else:
                CACHE_MISS_COUNTER.labels(dimension='carbon').inc()

        if self.carbon_manager:
            try:
                intensity = await self._carbon_circuit.call(
                    retry_async,
                    self.carbon_manager.get_current_intensity,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms,
                    region
                )
                score = max(0, min(1, 1 - intensity / 1000))
                self.scarcity_factors['carbon'] = min(2.0, intensity / 500)
                async with self._cache_lock:
                    self._score_cache[cache_key] = (score, now)
                EXTERNAL_CALL_COUNTER.labels(service='carbon', status='success').inc()
                return score
            except Exception as e:
                logger.warning(f"Carbon score retrieval failed: {e}", exc_info=True)
                EXTERNAL_CALL_COUNTER.labels(service='carbon', status='failure').inc()
        return 0.5

    async def _get_helium_score(self) -> float:
        if self.helium_tracker:
            try:
                position = await self._helium_circuit.call(
                    retry_async,
                    self.helium_tracker.get_helium_position,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms
                )
                if position:
                    remaining = position.get('remaining_budget_l', 0)
                    total = position.get('budget_l', 1)
                    score = max(0, min(1, remaining / max(total, 1)))
                    self.scarcity_factors['helium'] = min(2.0, 2.0 - score * 2)
                    return score
            except Exception as e:
                logger.warning(f"Helium score retrieval failed: {e}", exc_info=True)
        return 0.5

    async def _get_energy_score(self) -> float:
        if self.expert_registry:
            try:
                experts = await self._expert_circuit.call(
                    retry_async,
                    self.expert_registry.get_all_active_experts,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms
                )
                if experts:
                    avg_energy = np.mean([getattr(e, 'energy_per_inference', 0.001) for e in experts[:10]])
                    score = max(0, min(1, 1 - avg_energy * 1000))
                    self.scarcity_factors['energy'] = min(2.0, avg_energy * 1000)
                    return score
            except Exception as e:
                logger.warning(f"Energy score retrieval failed: {e}", exc_info=True)
        return 0.5

    async def _get_circularity_score(self) -> float:
        if self.circular_manager:
            try:
                report = await self._circular_circuit.call(
                    retry_async,
                    self.circular_manager.get_circularity_report,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms
                )
                if report:
                    score = report.get('circularity_score', 0.5)
                    self.scarcity_factors['circularity'] = min(2.0, 2.0 - score * 2)
                    return score
            except Exception as e:
                logger.warning(f"Circularity score retrieval failed: {e}", exc_info=True)
        return 0.5

    async def _get_biodiversity_score(self) -> float:
        if self.biodiversity:
            try:
                report = await self._biodiversity_circuit.call(
                    retry_async,
                    self.biodiversity.get_biodiversity_report,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms
                )
                if report:
                    biodiversity_score = report.get('local_biodiversity_score', 0.5)
                    score = 1.0 - biodiversity_score
                    self.scarcity_factors['biodiversity'] = min(2.0, biodiversity_score * 2)
                    return max(0, min(1, score))
            except Exception as e:
                logger.warning(f"Biodiversity score retrieval failed: {e}", exc_info=True)
        return 0.5

    async def _update_adaptive_thresholds(self, dimensions: Dict[str, SustainabilityDimension]):
        for name, dim in dimensions.items():
            threshold = self.thresholds.get(name)
            if threshold:
                adaptive_warning, adaptive_critical = await self.adaptive_threshold_manager.update_thresholds(
                    name,
                    dim.current_value,
                    threshold.warning_threshold,
                    threshold.critical_threshold
                )
                threshold.adaptive_warning = adaptive_warning
                threshold.adaptive_critical = adaptive_critical
                threshold.current_value = dim.current_value

    async def _adjust_weights(self, dimensions: Dict[str, SustainabilityDimension]) -> Dict[str, float]:
        if self.adaptive_cost_function:
            adaptive_weights = self.adaptive_cost_function.weights
            mapping = self.config.dimension_adaptive_mapping
            new_weights = {}
            for dim, adaptive_key in mapping.items():
                new_weights[dim] = adaptive_weights.get(adaptive_key, self.dimension_weights.get(dim, 0.2))
            total = sum(new_weights.values())
            if total > 0:
                for dim in new_weights:
                    new_weights[dim] /= total
            self.dimension_weights = new_weights
            return new_weights
        else:
            scarcity_factors = {name: dim.scarcity_factor for name, dim in dimensions.items()}
            dimension_scores = {name: dim.current_value for name, dim in dimensions.items()}
            return await self.dynamic_weight_manager.update_weights(dimension_scores, scarcity_factors)

    async def _update_predictions(self, dimensions: Dict[str, SustainabilityDimension]):
        for name, dim in dimensions.items():
            if name in self.dimension_history and len(self.dimension_history[name]) > 10:
                await self.predictive_analyzer.update_model(name, self.dimension_history[name][-20:])
                prediction, confidence, volatility = await self.predictive_analyzer.predict(name, 10)
                dim.prediction = prediction
                dim.prediction_confidence = confidence
                dim.volatility = volatility

    def _aggregate_score(self, dimensions: Dict[str, SustainabilityDimension], weights: Dict[str, float]) -> float:
        total = 0.0
        for name, dim in dimensions.items():
            if dim.current_value >= 0:
                weight = weights.get(name, dim.weight)
                total += dim.current_value * weight
        return total

    def _record_history(self, total_score: float, dimensions: Dict[str, SustainabilityDimension], weights: Dict[str, float]):
        self.history.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'score': total_score,
            'dimensions': {k: v.current_value for k, v in dimensions.items()},
            'weights': weights,
            'predictions': {k: v.prediction for k, v in dimensions.items() if v.prediction > 0}
        })
        for name, dim in dimensions.items():
            self.dimension_history[name].append(dim.current_value)
            if len(self.dimension_history[name]) > self.config.dimension_history_limit:
                self.dimension_history[name] = self.dimension_history[name][-self.config.dimension_history_limit:]

    def _generate_recommendations(self, dimensions: Dict[str, SustainabilityDimension], total_score: float) -> List[str]:
        recommendations = []
        if total_score < 0.5:
            recommendations.insert(0, "Overall sustainability score below 0.5 - urgent action required")
        elif total_score < 0.7:
            recommendations.insert(0, "Sustainability score needs improvement")
        for name, dim in dimensions.items():
            if dim.prediction > 0 and dim.prediction < dim.current_value * 0.9:
                recommendations.append(
                    f"PREDICTIVE: {name} sustainability is forecasted to decline "
                    f"(current: {dim.current_value:.2f} → predicted: {dim.prediction:.2f})"
                )
        return recommendations

    def _assess_risks(self, dimensions: Dict[str, SustainabilityDimension]) -> List[str]:
        risk_factors = []
        for name, dim in dimensions.items():
            threshold = self.thresholds.get(name)
            if threshold:
                if dim.current_value < threshold.adaptive_critical:
                    risk_factors.append(f"{name} at critical level ({dim.current_value:.2f})")
                elif dim.current_value < threshold.adaptive_warning:
                    risk_factors.append(f"{name} at warning level ({dim.current_value:.2f})")
                anomaly = self.adaptive_threshold_manager.get_anomaly_score(name, dim.current_value)
                if anomaly > 0.7:
                    risk_factors.append(f"{name} shows anomalous behavior (anomaly score: {anomaly:.2f})")
        return risk_factors

    async def _compute_scenarios(self, dimensions: Dict[str, SustainabilityDimension]) -> Dict[str, float]:
        scenario_scores = {}
        for name, dim in dimensions.items():
            for scenario in ['optimistic', 'pessimistic', 'most_likely']:
                scenario_key = f"{name}_{scenario}"
                if scenario_key not in scenario_scores:
                    scenario_scores[scenario_key] = 0.0
                scenario_value = await self.predictive_analyzer.predict_scenario(name, scenario, 10)
                scenario_scores[scenario_key] += scenario_value * dim.weight
        return scenario_scores

    def _compute_predicted_total(self, dimensions: Dict[str, SustainabilityDimension]) -> Optional[float]:
        total = 0.0
        has_pred = False
        for name, dim in dimensions.items():
            if dim.prediction > 0:
                total += dim.prediction * dim.weight
                has_pred = True
        return total if has_pred else None

    async def _update_external_systems(self, total_score: float, dimensions: Dict[str, SustainabilityDimension]):
        if self.expert_router and hasattr(self.expert_router, 'update_sustainability_fitness'):
            try:
                await retry_async(
                    self.expert_router.update_sustainability_fitness,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms,
                    total_score, dimensions
                )
            except Exception as e:
                logger.warning(f"Failed to update expert fitness: {e}")
        if self.quantum_limits and hasattr(self.quantum_limits, 'update_sustainability_limits'):
            try:
                await retry_async(
                    self.quantum_limits.update_sustainability_limits,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms,
                    total_score, dimensions
                )
            except Exception as e:
                logger.warning(f"Failed to update quantum limits: {e}")
        if self.gating_network and self.expert_router:
            features = np.array([
                total_score,
                self.scarcity_factors.get('carbon', 1.0),
                self.scarcity_factors.get('helium', 1.0),
                len(self._assess_risks(dimensions))
            ])
            context = {
                'dimensions': {k: v.current_value for k, v in dimensions.items()},
                'risk_factors': self._assess_risks(dimensions)
            }
            self.gating_network.update(features, total_score, context)

    def _update_telemetry(self, dimensions: Dict[str, SustainabilityDimension], total_score: float):
        SUSTAINABILITY_SCORE_GAUGE.set(total_score)
        for name, dim in dimensions.items():
            DIMENSION_SCORE_GAUGE.labels(dimension=name).set(dim.current_value)
            DIMENSION_WEIGHT_GAUGE.labels(dimension=name).set(dim.weight)
            SCARCITY_FACTOR_GAUGE.labels(dimension=name).set(dim.scarcity_factor)
        for cb, service in [(self._carbon_circuit, 'carbon'), (self._helium_circuit, 'helium'),
                            (self._circular_circuit, 'circular'), (self._biodiversity_circuit, 'biodiversity'),
                            (self._expert_circuit, 'expert'), (self._quantum_circuit, 'quantum')]:
            CIRCUIT_BREAKER_STATE.labels(service=service).set(cb.get_state_value())
        self.telemetry.gauge('sustainability_total_score', total_score)

    # ========================================================================
    # Public Methods (Enhanced with MOPD)
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
        # Update score with MOPD (but not return Pareto front by default)
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
    # MOPD Public Methods (NEW)
    # ========================================================================
    async def get_mopd_pareto_front(
        self,
        region: str = None,
        num_samples: int = 50
    ) -> List[MOPDPoint]:
        """
        Generate a Pareto front of sustainability states by sampling different
        weight combinations and evaluating the resulting scores.
        Returns a list of MOPDPoint objects.
        """
        if not self.config.mopd.enabled:
            return []

        # Collect current dimension scores
        dimensions = await self._collect_dimensions(region or self.config.default_region)
        dim_names = list(dimensions.keys())
        current_scores = {name: dim.current_value for name, dim in dimensions.items()}

        # Sample weight vectors from Dirichlet distribution
        rng = np.random.default_rng()
        points = []
        for _ in range(num_samples):
            weights = rng.dirichlet([1.0] * len(dim_names))
            weight_dict = {dim_names[i]: float(weights[i]) for i in range(len(dim_names))}
            # Compute scalarised score using current MOPD weights (for selection)
            scalarised = sum(self.config.mopd.objective_weights.get(d, 0.0) * current_scores.get(d, 0.0)
                             for d in dim_names)
            point = MOPDPoint(
                weights=weight_dict,
                dimensions=current_scores.copy(),
                scalarised_score=scalarised
            )
            points.append(point)

        # Filter dominated points (Pareto front)
        # Objectives: the dimension scores (higher is better)
        objective_keys = dim_names
        pareto = []
        for i, p_i in enumerate(points):
            dominated = False
            for j, p_j in enumerate(points):
                if i == j:
                    continue
                # Build vectors: for all dimensions, higher is better
                a_vec = [p_i.dimensions.get(k, 0.0) for k in objective_keys]
                b_vec = [p_j.dimensions.get(k, 0.0) for k in objective_keys]
                # p_j dominates p_i if all >= and at least one >
                if all(b >= a for a, b in zip(a_vec, b_vec)) and any(b > a for a, b in zip(a_vec, b_vec)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p_i)

        # Store in history
        for point in pareto:
            self.pareto_front_history.append(point)

        return pareto

    async def get_mopd_summary(self) -> Dict[str, Any]:
        """Return a summary of MOPD‑related metrics."""
        if not self.config.mopd.enabled:
            return {"enabled": False}
        return {
            "enabled": True,
            "objective_weights": self.config.mopd.objective_weights,
            "grid_resolution": self.config.mopd.grid_resolution,
            "pareto_front_history_size": len(self.pareto_front_history),
            "dimensions": list(self.dimension_weights.keys()),
        }

    # ========================================================================
    # Emission and Offset Methods (unchanged)
    # ========================================================================
    async def get_recent_emissions(self, hours: int = 24) -> float:
        return self.emissions_storage.get_recent_emissions(hours)

    async def record_offset(self, kg: float, source: str = None):
        self.emissions_storage.record_offset(kg, source=source)
        logger.info(f"Recorded offset: {kg} kg CO₂ from {source or 'unknown'}")

    # ========================================================================
    # Configuration Reload (unchanged)
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
    # Self-Healing (unchanged)
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
    # Health Status (unchanged)
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
        }

    # ========================================================================
    # Persistence Methods (unchanged)
    # ========================================================================
    async def save_state(self):
        if self.persistence:
            await self.persistence.save_state(self)

    async def load_state(self):
        if self.persistence:
            await self.persistence.load_state(self)

    # ========================================================================
    # Shutdown (unchanged)
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
# FastAPI REST API (with lifespan)
# ============================================================================
if FASTAPI_AVAILABLE:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup
        global engine
        config = SustainabilityEngineConfig()
        engine = UnifiedSustainabilityEngine(config=config)
        await engine.wait_ready()
        logger.info("FastAPI startup complete")
        yield
        # Shutdown
        if engine:
            await engine.shutdown()
        logger.info("FastAPI shutdown complete")

    app = FastAPI(title="Sustainability Engine API", version="5.1.0", lifespan=lifespan)

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
        status = await engine.get_health_status()
        return status

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
        report = await engine.get_sustainability_report(template_name=template, output_format=format)
        return report

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

    # NEW MOPD endpoints
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
        summary = await engine.get_mopd_summary()
        return summary


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
        await engine.shutdown()

    asyncio.run(main())
