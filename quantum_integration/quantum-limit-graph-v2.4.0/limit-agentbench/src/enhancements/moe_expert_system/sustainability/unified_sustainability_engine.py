# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/unified_sustainability_engine.py
# Enhanced version v3.0.0 – Full integration with bio‑inspired core, event‑driven, circuit breakers, self‑healing, and deep MoE/SEG integration

"""
Unified Sustainability Valuation Engine v3.0.0
Creates a single, authoritative global sustainability function that aggregates all dimensions
(carbon, helium, energy, circularity, biodiversity) with full bio‑inspired core integration.

New Features:
- Event-driven integration via core EventBroker (carbon, helium, alerts, config)
- Circuit breakers for all external services
- Self-healing and reactive alert handling
- Configuration reload via events
- Swarm coordination via SwarmCoordinator
- Integration with TimeTickEngine and QuantumBridge
- Integration with CostBenefitEngine and PredictiveAlertSystem
- Workflow orchestration triggers on threshold breaches
- Deep MoE and Self-Evolving Gate integration with rich context
- Enhanced telemetry and health monitoring
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Union, Protocol, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
import numpy as np
from collections import deque, defaultdict
import json
import hashlib
import os
import pickle
import zlib
import aiohttp
import random

logger = logging.getLogger(__name__)

# ============================================================================
# Bio-Inspired Core Import (with fallback)
# ============================================================================
try:
    from enhancements.bio_inspired.__init__ import EnhancedBioInspiredCore, BioEvent, CircuitBreaker, Persistence
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

    class CircuitBreaker:
        def __init__(self, name, failure_threshold=3, recovery_timeout=30.0):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self._state = "closed"
            self._failure_count = 0
            self._last_failure_time = None
            self._lock = asyncio.Lock()
        async def call(self, func, *args, **kwargs):
            return await func(*args, **kwargs)

# ============================================================================
# MoE and Self-Evolving Gate imports (optional)
# ============================================================================
try:
    from ..expert_router import ExpertRouter
    from ..gating_network import GatingNetworkManager
    from ..advanced.self_evolving_gates import EnhancedSelfEvolvingGate
    MOE_AVAILABLE = True
except ImportError:
    MOE_AVAILABLE = False
    logger.warning("MoE Expert Router or Self-Evolving Gates not available - sustainability engine will operate standalone")

# ============================================================================
# Helium Provider Interface (unchanged)
# ============================================================================
class HeliumProvider:
    def get_scarcity(self) -> float: raise NotImplementedError
    def get_cost_index(self) -> float: raise NotImplementedError
    def get_efficiency(self) -> float: raise NotImplementedError

# ============================================================================
# Configuration Dataclass (Enhanced)
# ============================================================================

@dataclass
class SustainabilityEngineConfig:
    """Centralized configuration for the Sustainability Engine."""
    # Dimension weights (initial)
    dimension_weights: Dict[str, float] = field(default_factory=lambda: {
        'carbon': 0.25,
        'helium': 0.20,
        'energy': 0.15,
        'circularity': 0.25,
        'biodiversity': 0.15
    })
    # Threshold parameters
    warning_threshold: float = 0.3
    critical_threshold: float = 0.1
    # Adaptive threshold
    adaptation_rate: float = 0.1
    adaptive_window_size: int = 100
    # Predictive analyzer
    prediction_window: int = 50
    model_weights: Dict[str, float] = field(default_factory=lambda: {
        'linear': 0.4,
        'exponential': 0.3,
        'moving_average': 0.3
    })
    # Retry and circuit breaker
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    # History limits
    history_limit: int = 10000
    dimension_history_limit: int = 100
    # Persistence
    persistence_path: str = "sustainability_engine_state.pkl"
    # Telemetry
    telemetry_export_interval: int = 60
    # Report templates path (optional)
    report_templates_path: Optional[str] = None

    # NEW: Feature flags for bio-inspired integrations
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

# ============================================================================
# Protocol Interfaces for External Modules (unchanged)
# ============================================================================
class CarbonProvider(Protocol):
    async def get_current_intensity(self) -> float: ...

class HeliumTracker(Protocol):
    async def get_helium_position(self) -> Dict[str, Any]: ...
    async def get_stats(self) -> Dict[str, Any]: ...

class CircularManager(Protocol):
    async def get_circularity_report(self) -> Dict[str, Any]: ...

class BiodiversityProvider(Protocol):
    async def get_biodiversity_report(self) -> Dict[str, Any]: ...

class ExpertRegistry(Protocol):
    async def get_all_active_experts(self) -> List[Any]: ...

class QuantumLimits(Protocol):
    async def update_sustainability_limits(self, score: float, dimensions: Dict) -> None: ...

# ============================================================================
# Data Classes (unchanged)
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
# Adaptive Threshold Manager (unchanged)
# ============================================================================
class AdaptiveThresholdManager:
    def __init__(self, config: SustainabilityEngineConfig):
        self.config = config
        self.window_size = config.adaptive_window_size
        self.adaptation_rate = config.adaptation_rate
        self.historical_values: Dict[str, deque] = {}
        self.threshold_history: Dict[str, List[Dict]] = defaultdict(list)
        self._lock = asyncio.Lock()
        logger.info("Adaptive Threshold Manager initialized")

    async def update_thresholds(self, dimension: str, current_value: float, base_warning: float, base_critical: float) -> Tuple[float, float]:
        async with self._lock:
            if dimension not in self.historical_values:
                self.historical_values[dimension] = deque(maxlen=self.window_size)
            self.historical_values[dimension].append(current_value)
            if len(self.historical_values[dimension]) < 10:
                return base_warning, base_critical
            values = list(self.historical_values[dimension])
            mean = np.mean(values)
            std = np.std(values)
            if mean < base_warning * 0.5:
                adjustment = 1.0 - self.adaptation_rate
            elif mean > base_warning * 1.2:
                adjustment = 1.0 + self.adaptation_rate
            else:
                adjustment = 1.0
            adaptive_warning = min(1.0, base_warning * adjustment)
            adaptive_critical = min(0.5, base_critical * adjustment)
            self.threshold_history[dimension].append({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'current_value': current_value,
                'mean': mean,
                'std': std,
                'adaptive_warning': adaptive_warning,
                'adaptive_critical': adaptive_critical,
                'adjustment': adjustment
            })
            return adaptive_warning, adaptive_critical

    def get_anomaly_score(self, dimension: str, value: float) -> float:
        if dimension not in self.historical_values or len(self.historical_values[dimension]) < 10:
            return 0.0
        values = list(self.historical_values[dimension])
        mean = np.mean(values)
        std = np.std(values)
        if std < 0.001:
            return 0.0
        z_score = abs(value - mean) / std
        return min(1.0, z_score / 5.0)

    def get_threshold_stats(self, dimension: str) -> Dict[str, Any]:
        if dimension not in self.historical_values:
            return {'status': 'insufficient_data'}
        values = list(self.historical_values[dimension])
        return {
            'sample_count': len(values),
            'mean': np.mean(values),
            'std': np.std(values),
            'min': min(values),
            'max': max(values),
            'percentile_25': np.percentile(values, 25),
            'percentile_75': np.percentile(values, 75),
            'trend': 'improving' if values[-1] > values[0] * 1.05 else 'declining' if values[-1] < values[0] * 0.95 else 'stable'
        }

# ============================================================================
# Dynamic Weight Manager (unchanged)
# ============================================================================
class DynamicWeightManager:
    def __init__(self, config: SustainabilityEngineConfig):
        self.config = config
        self.base_weights = config.dimension_weights.copy()
        self.current_weights = config.dimension_weights.copy()
        self.weight_history: Dict[str, List[float]] = defaultdict(list)
        self.scarcity_factors: Dict[str, float] = {dim: 1.0 for dim in config.dimension_weights}
        self._lock = asyncio.Lock()
        self.normalization_factor = 1.0 / sum(self.base_weights.values())
        logger.info("Dynamic Weight Manager initialized")

    async def update_weights(self, dimension_scores: Dict[str, float], scarcity_factors: Dict[str, float]) -> Dict[str, float]:
        async with self._lock:
            for dim, factor in scarcity_factors.items():
                if dim in self.scarcity_factors:
                    self.scarcity_factors[dim] = factor
            adjusted_weights = {}
            total_adjusted = 0.0
            for dim, base_weight in self.base_weights.items():
                scarcity = self.scarcity_factors.get(dim, 1.0)
                performance = dimension_scores.get(dim, 0.5)
                weight_factor = (scarcity * 0.7 + (1.0 - performance) * 0.3)
                adjusted_weight = base_weight * weight_factor
                adjusted_weights[dim] = adjusted_weight
                total_adjusted += adjusted_weight
            if total_adjusted > 0:
                for dim in adjusted_weights:
                    adjusted_weights[dim] /= total_adjusted
                    self.weight_history[dim].append(adjusted_weights[dim])
            self.current_weights = adjusted_weights
            return adjusted_weights

    def get_current_weights(self) -> Dict[str, float]:
        return self.current_weights.copy()

    def get_weight_trends(self) -> Dict[str, Any]:
        trends = {}
        for dim, history in self.weight_history.items():
            if len(history) > 5:
                slope = np.polyfit(range(len(history[-10:])), history[-10:], 1)[0] if len(history) >= 10 else 0
                trends[dim] = {
                    'current': history[-1] if history else self.base_weights.get(dim, 0),
                    'trend': 'increasing' if slope > 0.01 else 'decreasing' if slope < -0.01 else 'stable',
                    'slope': slope,
                    'history_length': len(history)
                }
            else:
                trends[dim] = {'current': self.base_weights.get(dim, 0), 'trend': 'stable'}
        return trends

# ============================================================================
# Predictive Trend Analyzer (unchanged)
# ============================================================================
class PredictiveTrendAnalyzer:
    def __init__(self, config: SustainabilityEngineConfig):
        self.config = config
        self.window_size = config.prediction_window
        self.historical_data: Dict[str, List[float]] = {}
        self.predictions: Dict[str, List[float]] = {}
        self.model_weights = config.model_weights.copy()
        self.model_performance: Dict[str, List[float]] = {'linear': [], 'exponential': [], 'moving_average': []}
        self._lock = asyncio.Lock()
        logger.info("Predictive Trend Analyzer initialized")

    async def update_model(self, dimension: str, values: List[float]):
        async with self._lock:
            if dimension not in self.historical_data:
                self.historical_data[dimension] = []
            self.historical_data[dimension].extend(values)
            if len(self.historical_data[dimension]) > self.window_size:
                self.historical_data[dimension] = self.historical_data[dimension][-self.window_size:]

    async def predict(self, dimension: str, horizon_steps: int = 10) -> Tuple[float, float, float]:
        if dimension not in self.historical_data or len(self.historical_data[dimension]) < 10:
            return 0.5, 0.0, 0.0
        async with self._lock:
            data = self.historical_data[dimension]
            n = len(data)
            x = np.array(range(n))
            y = np.array(data)
            linear_coeffs = np.polyfit(x, y, 1)
            linear_pred = linear_coeffs[0] * (n + horizon_steps - 1) + linear_coeffs[1]
            alpha = 0.3
            exp_forecast = data[-1]
            for _ in range(horizon_steps):
                exp_forecast = alpha * data[-1] + (1 - alpha) * exp_forecast
            window = min(10, n)
            ma_forecast = np.mean(data[-window:])
            ensemble_pred = (
                self.model_weights['linear'] * linear_pred +
                self.model_weights['exponential'] * exp_forecast +
                self.model_weights['moving_average'] * ma_forecast
            )
            ensemble_pred = max(0.0, min(1.0, ensemble_pred))
            preds = [linear_pred, exp_forecast, ma_forecast]
            variance = np.var(preds)
            confidence = max(0.0, min(1.0, 1.0 - variance * 10))
            volatility = np.std(data[-10:]) if len(data) >= 10 else 0.0
            if dimension not in self.predictions:
                self.predictions[dimension] = []
            self.predictions[dimension].append(ensemble_pred)
            return ensemble_pred, confidence, volatility

    async def predict_scenario(self, dimension: str, scenario_type: str, horizon_steps: int = 10) -> float:
        if dimension not in self.historical_data or len(self.historical_data[dimension]) < 10:
            return 0.5
        data = self.historical_data[dimension]
        current = data[-1]
        if scenario_type == 'optimistic':
            improvement = 0.1 * (1 + horizon_steps / 20)
            return min(1.0, current + improvement)
        elif scenario_type == 'pessimistic':
            decline = 0.1 * (1 + horizon_steps / 20)
            return max(0.0, current - decline)
        else:
            pred, _, _ = await self.predict(dimension, horizon_steps)
            return pred

    def get_prediction_accuracy(self, dimension: str) -> float:
        if dimension not in self.predictions or len(self.predictions[dimension]) < 5:
            return 0.0
        predictions = self.predictions[dimension]
        actual = self.historical_data[dimension][-len(predictions):]
        errors = [abs(p - a) / max(a, 0.01) for p, a in zip(predictions, actual)]
        accuracy = 1.0 - min(1.0, np.mean(errors))
        return accuracy

# ============================================================================
# Report Template Manager (unchanged)
# ============================================================================
class ReportTemplateManager:
    def __init__(self, config: SustainabilityEngineConfig):
        self.config = config
        self.templates: Dict[str, ReportTemplate] = {}
        self.generated_reports: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        self._init_default_templates()
        logger.info("Report Template Manager initialized")

    def _init_default_templates(self):
        default_templates = [
            ReportTemplate(
                name="executive_summary",
                description="High-level sustainability overview for executives",
                included_dimensions=["carbon", "helium", "energy", "circularity", "biodiversity"],
                metrics=["total_score", "trend", "risk_factors"],
                format="json",
                frequency="daily",
                target_audience="executives",
                customization={"show_confidence": True, "show_recommendations": True}
            ),
            ReportTemplate(
                name="technical_detailed",
                description="Detailed sustainability metrics for technical teams",
                included_dimensions=["carbon", "helium", "energy", "circularity", "biodiversity"],
                metrics=["all"],
                format="json",
                frequency="daily",
                target_audience="engineers",
                customization={"show_all_metrics": True, "include_raw_data": True}
            ),
            ReportTemplate(
                name="compliance",
                description="Compliance-focused sustainability report",
                included_dimensions=["carbon", "energy", "circularity"],
                metrics=["total_score", "trend", "threshold_status"],
                format="json",
                frequency="weekly",
                target_audience="compliance_officers",
                customization={"threshold_emphasis": True, "include_audit_trail": True}
            ),
            ReportTemplate(
                name="sustainability_summary",
                description="Summary of sustainability performance for stakeholders",
                included_dimensions=["carbon", "helium", "circularity"],
                metrics=["total_score", "trend", "recommendations"],
                format="json",
                frequency="weekly",
                target_audience="investors",
                customization={"show_roi_metrics": True, "include_benchmarks": True}
            )
        ]
        for template in default_templates:
            self.templates[template.name] = template

    def create_template(self, template: ReportTemplate) -> bool:
        if template.name in self.templates:
            logger.warning(f"Template {template.name} already exists")
            return False
        self.templates[template.name] = template
        logger.info(f"Created report template: {template.name}")
        return True

    def get_template(self, name: str) -> Optional[ReportTemplate]:
        return self.templates.get(name)

    def list_templates(self) -> List[str]:
        return list(self.templates.keys())

    async def generate_report(self, template_name: str, data: Dict[str, Any], output_format: str = "json") -> Dict[str, Any]:
        template = self.get_template(template_name)
        if not template:
            return {'status': 'error', 'message': f'Template {template_name} not found'}
        filtered_data = {}
        for dim in template.included_dimensions:
            if dim in data:
                filtered_data[dim] = data[dim]
        report = {
            'template': template_name,
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'target_audience': template.target_audience,
            'dimensions': filtered_data,
            'customization': template.customization,
            'status': 'generated'
        }
        if 'total_score' in data:
            report['total_score'] = data['total_score']
        if template.customization.get('show_recommendations', False) and 'recommendations' in data:
            report['recommendations'] = data['recommendations']
        report_id = hashlib.md5(f"{template_name}_{datetime.now(timezone.utc).timestamp()}".encode()).hexdigest()[:12]
        self.generated_reports[report_id] = report
        if output_format == 'html':
            report['rendered'] = self._render_html(report)
        elif output_format == 'pdf':
            report['rendered'] = self._render_pdf(report)
        elif output_format == 'csv':
            report['rendered'] = self._render_csv(report)
        return report

    def _render_html(self, report: Dict) -> str:
        html = f"<html><body><h1>Sustainability Report: {report['template']}</h1>"
        html += f"<p>Generated at: {report['generated_at']}</p>"
        html += f"<h2>Total Score: {report.get('total_score', 'N/A')}</h2>"
        html += "<h3>Dimensions:</h3><ul>"
        for dim, value in report.get('dimensions', {}).items():
            html += f"<li>{dim}: {value}</li>"
        html += "</ul>"
        if 'recommendations' in report:
            html += "<h3>Recommendations:</h3><ul>"
            for rec in report['recommendations']:
                html += f"<li>{rec}</li>"
            html += "</ul>"
        html += "</body></html>"
        return html

    def _render_pdf(self, report: Dict) -> str:
        return "PDF generation not implemented"

    def _render_csv(self, report: Dict) -> str:
        import csv
        import io
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Dimension', 'Value'])
        for dim, value in report.get('dimensions', {}).items():
            writer.writerow([dim, value])
        if 'total_score' in report:
            writer.writerow(['Total Score', report['total_score']])
        return output.getvalue()

# ============================================================================
# Retry Helper (unchanged)
# ============================================================================
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
            await asyncio.sleep(delay)
    raise RuntimeError("Max retries exceeded")

# ============================================================================
# Persistence Manager (unchanged)
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
                    'config': engine.config,
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
                    'last_update': engine.last_update.isoformat() if engine.last_update else None
                }
                serialized = pickle.dumps(state)
                compressed = zlib.compress(serialized)
                with open(self.path, 'wb') as f:
                    f.write(compressed)
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
                with open(self.path, 'rb') as f:
                    compressed = f.read()
                serialized = zlib.decompress(compressed)
                state = pickle.loads(serialized)
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
# Telemetry Collector (unchanged)
# ============================================================================
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
# Enhanced Unified Sustainability Engine (Main Class) – v3.0.0
# ============================================================================

class UnifiedSustainabilityEngine:
    """
    Unified Sustainability Valuation Engine v3.0.0
    With full bio‑inspired core integration.
    """

    def __init__(
        self,
        bio_core: Optional[EnhancedBioInspiredCore] = None,
        config: Optional[SustainabilityEngineConfig] = None,
        **kwargs
    ):
        """
        Initialize the sustainability engine.

        Args:
            bio_core: Reference to the bio‑inspired core for event subscriptions.
            config: Configuration dataclass (preferred).
            **kwargs: Legacy arguments for backward compatibility.
        """
        if config is None:
            config = SustainabilityEngineConfig(
                enable_event_driven=kwargs.get('enable_event_driven', True),
                enable_self_healing=kwargs.get('enable_self_healing', True),
                enable_swarm_coordination=kwargs.get('enable_swarm_coordination', True),
                enable_time_tick_engine=kwargs.get('enable_time_tick_engine', True),
                enable_quantum_bridge=kwargs.get('enable_quantum_bridge', True),
                enable_cost_benefit=kwargs.get('enable_cost_benefit', True),
                enable_workflow_orchestration=kwargs.get('enable_workflow_orchestration', True),
                max_retries=kwargs.get('max_retries', 3),
                retry_base_delay_ms=kwargs.get('retry_base_delay_ms', 100.0),
                retry_max_delay_ms=kwargs.get('retry_max_delay_ms', 5000.0),
                circuit_breaker_failure_threshold=kwargs.get('circuit_breaker_failure_threshold', 5),
                circuit_breaker_recovery_timeout=kwargs.get('circuit_breaker_recovery_timeout', 30.0),
                persistence_path=kwargs.get('persistence_path', 'sustainability_engine_state.pkl'),
                co_evolution_interval=kwargs.get('co_evolution_interval', 300)
            )
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

        # Extract core sub‑modules if available
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

        # Helium provider (injected)
        self.helium_provider = None

        # External modules (will be injected)
        self.carbon_manager: Optional[CarbonProvider] = None
        self.helium_tracker: Optional[HeliumTracker] = None
        self.circular_manager: Optional[CircularManager] = None
        self.biodiversity: Optional[BiodiversityProvider] = None
        self.expert_registry: Optional[ExpertRegistry] = None
        self.quantum_limits: Optional[QuantumLimits] = None

        # Managers
        self.adaptive_threshold_manager = AdaptiveThresholdManager(self.config)
        self.dynamic_weight_manager = DynamicWeightManager(self.config)
        self.predictive_analyzer = PredictiveTrendAnalyzer(self.config)
        self.report_manager = ReportTemplateManager(self.config)
        self.persistence = SustainabilityPersistenceManager(self.config)
        self.telemetry = SustainabilityTelemetry()

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

        # Circuit breakers for external services
        self._carbon_circuit = CircuitBreaker("carbon_manager")
        self._helium_circuit = CircuitBreaker("helium_tracker")
        self._circular_circuit = CircuitBreaker("circular_manager")
        self._biodiversity_circuit = CircuitBreaker("biodiversity_provider")
        self._expert_circuit = CircuitBreaker("expert_registry")
        self._quantum_circuit = CircuitBreaker("quantum_limits")

        # Health status
        self.health_status = "healthy"
        self.last_error = None

        # Initialize thresholds
        self._init_thresholds()

        # Subscribe to core events if enabled
        if self.enable_event_driven and self.event_broker:
            self._subscribe_events()

        # Start background tasks
        self._start_background_tasks()

        # Load state if persistence available
        if self.persistence:
            asyncio.create_task(self._load_state())

        logger.info("Unified Sustainability Engine v3.0.0 initialized")

    # ========================================================================
    # Event Subscriptions
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
        # Update scarcity factor
        self.scarcity_factors['carbon'] = min(2.0, intensity / 500)
        # Trigger a new score update if needed

    async def _on_helium_update(self, event: BioEvent):
        scarcity = event.data.get('scarcity', 0.5)
        price = event.data.get('price', 0.5)
        self.helium_scarcity = scarcity
        self.helium_price = price
        self.scarcity_factors['helium'] = min(2.0, 2.0 - (1 - scarcity) * 2)
        # Update helium threshold if helium_tracker is used

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
            # Increase carbon weight temporarily
            self.dimension_weights['carbon'] = min(0.5, self.dimension_weights['carbon'] * 1.2)
        if event.data.get('metric') == 'helium_scarcity':
            logger.info("Helium anomaly detected; adjusting helium weight")
            self.dimension_weights['helium'] = min(0.5, self.dimension_weights['helium'] * 1.2)

    # ========================================================================
    # Background Tasks
    # ========================================================================
    def _start_background_tasks(self):
        if self.enable_telemetry:
            asyncio.create_task(self._telemetry_export_loop())
        if self.enable_swarm_coordination and self.swarm_coordinator:
            asyncio.create_task(self._swarm_update_loop())
        if self.enable_persistence:
            asyncio.create_task(self._persistence_save_loop())

    async def _telemetry_export_loop(self):
        while True:
            try:
                if self.telemetry:
                    export_data = await self.telemetry.export()
                    logger.debug(f"Telemetry export: {len(export_data)} bytes")
                await asyncio.sleep(self.config.telemetry_export_interval)
            except Exception as e:
                logger.error(f"Telemetry export error: {e}")
                await asyncio.sleep(60)

    async def _swarm_update_loop(self):
        while True:
            try:
                await self.share_with_swarm()
                await asyncio.sleep(self.config.swarm_share_interval)
            except Exception as e:
                logger.error(f"Swarm update error: {e}")
                await asyncio.sleep(120)

    async def _persistence_save_loop(self):
        while True:
            try:
                await self.save_state()
                await asyncio.sleep(300)  # every 5 minutes
            except Exception as e:
                logger.error(f"Persistence save error: {e}")
                await asyncio.sleep(60)

    # ========================================================================
    # Swarm Coordination
    # ========================================================================
    async def share_with_swarm(self):
        if not self.enable_swarm_coordination or not self.swarm_coordinator:
            return
        swarm_payload = {
            'engine_id': hashlib.md5(str(self.dimensions).encode()).hexdigest()[:8],
            'sustainability_score': self.sustainability_score,
            'dimension_scores': {k: v.current_value for k, v in self.dimensions.items()},
            'scarcity_factors': self.scarcity_factors,
            'history_sample_count': len(self.history)
        }
        await self.swarm_coordinator.share_predictions(swarm_payload)

    # ========================================================================
    # Injection Methods
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
    # Core Methods (Enhanced)
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

    async def update_sustainability_score(self) -> UnifiedSustainabilityScore:
        dimensions = {}
        risk_factors = []
        recommendations = []
        dimension_scores = {}

        # Carbon dimension
        carbon_value = await self._get_carbon_score()
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
        dimension_scores['carbon'] = carbon_value

        # Helium dimension
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
        dimension_scores['helium'] = helium_value

        # Energy dimension
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
        dimension_scores['energy'] = energy_value

        # Circularity dimension
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
        dimension_scores['circularity'] = circularity_value

        # Biodiversity dimension
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
        dimension_scores['biodiversity'] = biodiversity_value

        # Update dimension history for prediction
        for name, dim in dimensions.items():
            self.dimension_history[name].append(dim.current_value)
            if len(self.dimension_history[name]) > self.config.dimension_history_limit:
                self.dimension_history[name] = self.dimension_history[name][-self.config.dimension_history_limit:]

        # Update adaptive thresholds
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

                if dim.current_value < adaptive_critical:
                    threshold.status = "critical"
                    risk_factors.append(f"{name} at critical level ({dim.current_value:.2f}) - threshold: {adaptive_critical:.2f}")
                    recommendations.append(f"CRITICAL: Address {name} sustainability immediately")
                elif dim.current_value < adaptive_warning:
                    threshold.status = "warning"
                    risk_factors.append(f"{name} at warning level ({dim.current_value:.2f}) - threshold: {adaptive_warning:.2f}")
                    recommendations.append(f"WARNING: Monitor {name} sustainability")
                else:
                    threshold.status = "healthy"

                anomaly_score = self.adaptive_threshold_manager.get_anomaly_score(name, dim.current_value)
                if anomaly_score > 0.7:
                    risk_factors.append(f"{name} shows anomalous behavior (anomaly score: {anomaly_score:.2f})")

        # Dynamic weight adjustment
        scarcity_factors = {name: dim.scarcity_factor for name, dim in dimensions.items()}
        updated_weights = await self.dynamic_weight_manager.update_weights(dimension_scores, scarcity_factors)

        for name, weight in updated_weights.items():
            if name in dimensions:
                dimensions[name].weight = weight
                dimensions[name].historical_weights.append(weight)

        # Predictive analytics
        for name, dim in dimensions.items():
            if name in self.dimension_history and len(self.dimension_history[name]) > 10:
                await self.predictive_analyzer.update_model(name, self.dimension_history[name][-20:])
                prediction, confidence, volatility = await self.predictive_analyzer.predict(name, 10)
                dim.prediction = prediction
                dim.prediction_confidence = confidence
                dim.volatility = volatility

        # Calculate total score with dynamic weights
        total_score = 0.0
        for name, dim in dimensions.items():
            if dim.current_value >= 0:
                total_score += dim.current_value * dim.weight

        # Predicted future score and scenario analysis
        predicted_total = 0.0
        scenario_scores = {}
        for name, dim in dimensions.items():
            if dim.prediction > 0:
                predicted_total += dim.prediction * dim.weight
                for scenario in ['optimistic', 'pessimistic', 'most_likely']:
                    scenario_key = f"{name}_{scenario}"
                    if scenario_key not in scenario_scores:
                        scenario_scores[scenario_key] = 0.0
                    scenario_value = await self.predictive_analyzer.predict_scenario(name, scenario, 10)
                    scenario_scores[scenario_key] += scenario_value * dim.weight

        self.sustainability_score = total_score

        # Store in history
        self.history.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'score': total_score,
            'dimensions': {k: v.current_value for k, v in dimensions.items()},
            'weights': {k: v.weight for k, v in dimensions.items()},
            'predictions': {k: v.prediction for k, v in dimensions.items() if v.prediction > 0}
        })

        # Global recommendations
        if total_score < 0.5:
            recommendations.insert(0, "Overall sustainability score below 0.5 - urgent action required")
        elif total_score < 0.7:
            recommendations.insert(0, "Sustainability score needs improvement")

        # Predictive recommendations
        for name, dim in dimensions.items():
            if dim.prediction > 0 and dim.prediction < dim.current_value * 0.9:
                recommendations.append(
                    f"PREDICTIVE: {name} sustainability is forecasted to decline "
                    f"(current: {dim.current_value:.2f} → predicted: {dim.prediction:.2f})"
                )

        # Update external systems
        if self.expert_registry:
            await self._update_expert_fitness(total_score, dimensions)
        if self.quantum_limits:
            await self._update_quantum_limits(total_score, dimensions)

        # Pass to gating network if available
        if self.gating_network and self.expert_router:
            features = np.array([
                total_score,
                self.scarcity_factors.get('carbon', 1.0),
                self.scarcity_factors.get('helium', 1.0),
                len(risk_factors)
            ])
            reward = total_score
            context = {
                'dimensions': {k: v.current_value for k, v in dimensions.items()},
                'risk_factors': risk_factors
            }
            self.gating_network.update(features, reward, context)

        # Pass to self-evolving gate if available
        if self.self_evolving_gate:
            self.self_evolving_gate.adapt(
                state=torch.tensor([total_score, self.scarcity_factors['carbon']]),
                chosen_expert=0,  # dummy
                reward=total_score,
                environmental_feedback={'risk_factors': risk_factors},
                quantum_mode=False
            )

        # Trigger workflow if needed
        if total_score < 0.4 and self.workflow_orchestrator:
            await self.workflow_orchestrator.execute_workflow(self.config.workflow_on_slo_breach)

        # Telemetry
        self.telemetry.gauge('sustainability_total_score', total_score)
        for name, dim in dimensions.items():
            self.telemetry.gauge(f'dimension_{name}_score', dim.current_value)
            self.telemetry.gauge(f'dimension_{name}_weight', dim.weight)

        return UnifiedSustainabilityScore(
            total_score=total_score,
            dimensions=dimensions,
            confidence=0.8,
            trend=self._calculate_global_trend(),
            risk_factors=risk_factors,
            recommendations=recommendations,
            predicted_future_score=predicted_total if predicted_total > 0 else None,
            scenario_scores=scenario_scores
        )

    # ========================================================================
    # Dimension Score Methods (Enhanced with circuit breakers)
    # ========================================================================
    async def _get_carbon_score(self) -> float:
        if self.carbon_manager:
            try:
                intensity = await self._carbon_circuit.call(
                    retry_async,
                    self.carbon_manager.get_current_intensity,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms
                )
                score = max(0, min(1, 1 - intensity / 1000))
                self.scarcity_factors['carbon'] = min(2.0, intensity / 500)
                return score
            except Exception as e:
                logger.warning(f"Carbon score retrieval failed: {e}")
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
                logger.warning(f"Helium score retrieval failed: {e}")
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
                logger.warning(f"Energy score retrieval failed: {e}")
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
                logger.warning(f"Circularity score retrieval failed: {e}")
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
                logger.warning(f"Biodiversity score retrieval failed: {e}")
        return 0.5

    # ========================================================================
    # Trend Analysis Methods (unchanged)
    # ========================================================================
    def _calculate_trend(self, dimension: str, current_value: float) -> str:
        history = list(self.history)[-20:]
        if not history:
            return "stable"
        values = [h['dimensions'].get(dimension, current_value) for h in history]
        if len(values) > 5:
            trend = np.polyfit(range(len(values)), values, 1)[0]
            if trend > 0.01:
                return "improving"
            elif trend < -0.01:
                return "declining"
        return "stable"

    def _calculate_global_trend(self) -> str:
        history = list(self.history)[-20:]
        if not history:
            return "stable"
        scores = [h['score'] for h in history]
        if len(scores) > 5:
            trend = np.polyfit(range(len(scores)), scores, 1)[0]
            if trend > 0.005:
                return "improving"
            elif trend < -0.005:
                return "declining"
        return "stable"

    # ========================================================================
    # Integration Methods (Enhanced)
    # ========================================================================
    async def _update_expert_fitness(self, score: float, dimensions: Dict):
        if hasattr(self.expert_registry, 'update_sustainability_fitness'):
            try:
                await retry_async(
                    self.expert_registry.update_sustainability_fitness,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms,
                    score, dimensions
                )
            except Exception as e:
                logger.warning(f"Failed to update expert fitness: {e}")

    async def _update_quantum_limits(self, score: float, dimensions: Dict):
        if hasattr(self.quantum_limits, 'update_sustainability_limits'):
            try:
                await retry_async(
                    self.quantum_limits.update_sustainability_limits,
                    self.config.max_retries,
                    self.config.retry_base_delay_ms,
                    self.config.retry_max_delay_ms,
                    score, dimensions
                )
            except Exception as e:
                logger.warning(f"Failed to update quantum limits: {e}")

    # ========================================================================
    # Public Methods (Enhanced)
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
        score = await self.update_sustainability_score()
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
    # Self-Healing
    # ========================================================================
    async def self_heal(self):
        logger.info("SustainabilityEngine self‑healing")
        if self.enable_self_healing:
            # Reset weights to config defaults
            self.dimension_weights = self.config.dimension_weights.copy()
            self.scarcity_factors = {
                'carbon': 1.0,
                'helium': 1.0,
                'energy': 1.0,
                'circularity': 1.0,
                'biodiversity': 1.0
            }
            self.config.adaptation_rate = 0.1
            # Clear stale history (keep last 10)
            if len(self.history) > 10:
                self.history = deque(list(self.history)[-10:], maxlen=self.config.history_limit)
            # Reset health status
            self.health_status = "healthy"
            self.last_error = None
            # Save state
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
        if self.persistence:
            await self.save_state()
        logger.info("Shutdown complete")
