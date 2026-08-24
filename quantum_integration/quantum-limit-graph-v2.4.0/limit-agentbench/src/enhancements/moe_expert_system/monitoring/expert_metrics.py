#!/usr/bin/env python3
"""
Enhanced Expert Metrics Collector v8.2.0 - Complete Green Agent Implementation
with full bio‑inspired core integration and Multi‑Objective Pareto Decision (MOPD) support.

ENHANCEMENTS OVER v8.1.0:
- Central Green Agent component integration: Storage, AsyncMessageQueue, AdaptiveCostFunction,
  ParetoGating, DriftDetector, MetricsRegistry.
- Safe async task creation (no RuntimeError outside event loop).
- Implemented teacher policy (`policy_probs`) for MTPD optimizer.
- FeedbackEvent publication for routing and execution.
- Drift detection with adaptive threshold adjustment.
- Deep bio‑inspired integration: ATP spend/earn, carbon/helium gradient pumping.
- Fixed persistence to properly serialize/deserialize dataclasses.
- Improved optional dependency handling (sklearn).
"""

import asyncio
import logging
import json
import os
import time
import math
import random
import hashlib
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple, Set, Callable, Union, Deque
from collections import defaultdict, deque
import numpy as np
import aiohttp
import zlib

# Attempt to import torch (optional)
try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

logger = logging.getLogger(__name__)

# ============================================================================
# Central Green Agent Components
# ============================================================================
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
# Optional scikit-learn imports (guarded)
# ============================================================================
try:
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    from sklearn.linear_model import SGDRegressor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    StandardScaler = None
    IsolationForest = None
    SGDRegressor = None
    logger.warning("scikit-learn not available; using fallback methods")

# ============================================================================
# Bio-Inspired Core Import (with fallback)
# ============================================================================
try:
    from enhancements.bio_inspired.__init__ import EnhancedBioInspiredCore, BioEvent, CircuitBreaker, Persistence
    from enhancements.bio_inspired.eco_atp_currency import (
        EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer,
        TokenState, EcoATPToken, EcoATPAccount
    )
    from enhancements.bio_inspired.proton_gradient_fields import (
        GradientFieldManager, GradientField
    )
    from enhancements.bio_inspired.atp_synthase_scheduler import (
        ATPSynthaseScheduler, SynthaseConfig
    )
    from enhancements.bio_inspired.chromatophore_compartments import (
        CompartmentManager, ChromatophoreCompartment, CompartmentState,
        MembranePermeability
    )
    from enhancements.bio_inspired.biomass_storage import (
        BiomassStorage, StorageTier, GuaranteeLevel
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (
        PhotosyntheticHarvester
    )
    from enhancements.bio_inspired.time_tick_engine import TimeTickEngine
    from enhancements.bio_inspired.quantum_bridge import QuantumBridge
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired core modules loaded for Expert Metrics")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired core modules not available: {str(e)} - using standard metrics")
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
    logger.warning("MoE Expert Router or Self-Evolving Gates not available - metrics collector will operate standalone")

# ============================================================================
# Helium Provider Interface
# ============================================================================
class HeliumProvider:
    def get_scarcity(self) -> float: raise NotImplementedError
    def get_cost_index(self) -> float: raise NotImplementedError
    def get_efficiency(self) -> float: raise NotImplementedError

# ============================================================================
# Enums and Data Classes (Enhanced with MOPD)
# ============================================================================
class MetricSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"

class MetricType(Enum):
    LATENCY = "latency"
    SUCCESS_RATE = "success_rate"
    CARBON = "carbon"
    HELIUM = "helium"
    TOKEN = "token"
    HEALTH = "health"

class AnomalyType(Enum):
    LATENCY_SPIKE = "latency_spike"
    ERROR_RATE = "error_rate"
    CARBON_SURGE = "carbon_surge"
    TOKEN_DRAIN = "token_drain"
    GRADIENT_DROP = "gradient_drop"

class SLOStatus(Enum):
    COMPLIANT = "compliant"
    AT_RISK = "at_risk"
    BREACHED = "breached"

@dataclass
class MetricThreshold:
    metric_name: str
    warning_threshold: float
    critical_threshold: float
    comparison: str = "greater_than"
    gradient_modulated: bool = False
    cooldown_seconds: float = 300.0

@dataclass
class ServiceLevelObjective:
    slo_id: str
    metric_name: str
    target_value: float
    target_percentile: float = 99.0
    evaluation_window_hours: float = 24.0
    min_samples: int = 30
    current_value: float = 0.0
    status: SLOStatus = SLOStatus.COMPLIANT
    predicted_violation_probability: float = 0.0
    next_predicted_violation: Optional[datetime] = None

@dataclass
class AnomalyEvent:
    anomaly_type: AnomalyType
    severity: MetricSeverity
    expert_id: str
    expected_value: float
    actual_value: float
    timestamp: datetime
    description: str = ""

@dataclass
class MetricSample:
    timestamp: datetime
    expert_id: str
    metric_name: str
    value: float
    tags: Dict[str, str] = field(default_factory=dict)

@dataclass
class CostAttribution:
    expert_id: str
    total_cost: float
    carbon_cost: float
    helium_cost: float
    token_cost: float
    timestamp: datetime

@dataclass
class PredictiveMetricForecast:
    metric_name: str
    forecast_value: float
    confidence_interval: Tuple[float, float]
    timestamp: datetime
    horizon_seconds: int

# ============================================================================
# MOPD Data Classes (NEW)
# ============================================================================
@dataclass
class MOPDPoint:
    """Represents a single point in the multi‑objective space."""
    expert_id: str
    timestamp: datetime
    carbon_kg: float
    helium_units: float
    ecoatp_cost: float
    latency_ms: float
    success_probability: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPoint':
        # Convert timestamp string if needed
        if isinstance(data.get('timestamp'), str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)

@dataclass
class MOPDConfig:
    """Configuration for MOPD analysis."""
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        'carbon': 0.3,
        'helium': 0.2,
        'cost': 0.2,
        'latency': 0.15,
        'success': 0.15,
    })
    grid_resolution: int = 5
    enable_cost_benefit: bool = True
    enable_predictive: bool = True
    enable_quantum: bool = True

# ============================================================================
# Enhanced Configuration with MOPD Sub‑Config
# ============================================================================
@dataclass
class AnomalyDetectionConfig:
    enabled: bool = True
    ml_enabled: bool = True
    contamination: float = 0.1
    n_estimators: int = 100
    window_size: int = 100
    update_interval_seconds: int = 300

@dataclass
class SLOConfig:
    enabled: bool = True
    slo_definitions: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {
        'latency_slo': {'metric_name': 'expert_latency_ms', 'target_value': 100.0, 'target_percentile': 99.0, 'evaluation_window_hours': 24.0},
        'availability_slo': {'metric_name': 'expert_success_rate', 'target_value': 0.999, 'target_percentile': 99.9, 'evaluation_window_hours': 24.0},
        'carbon_slo': {'metric_name': 'carbon_per_inference', 'target_value': 0.0005, 'target_percentile': 95.0, 'evaluation_window_hours': 24.0},
        'token_efficiency_slo': {'metric_name': 'token_efficiency', 'target_value': 0.8, 'target_percentile': 90.0, 'evaluation_window_hours': 24.0},
        'sustainability_slo': {'metric_name': 'sustainability_score', 'target_value': 0.7, 'target_percentile': 95.0, 'evaluation_window_hours': 24.0}
    })
    evaluation_interval_seconds: int = 60

@dataclass
class CarbonConfig:
    enabled: bool = True
    region: str = "us-east"
    update_interval_seconds: int = 300
    max_retries: int = 3
    circuit_breaker_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    api_key_env: str = "ELECTRICITYMAP_API_KEY"

@dataclass
class FederatedConfig:
    enabled: bool = True
    server_url: Optional[str] = None
    sparsity_ratio: float = 0.1
    privacy_epsilon: float = 1.0
    sync_interval_seconds: int = 3600
    max_retries: int = 3

@dataclass
class TelemetryConfig:
    enabled: bool = True
    export_interval_seconds: int = 60
    exporter_type: str = "prometheus"

@dataclass
class PersistenceConfig:
    enabled: bool = True
    path: str = "metrics_state.json"
    save_interval_seconds: int = 300
    retention_hours: float = 24.0

@dataclass
class SelfHealingConfig:
    enabled: bool = True
    auto_heal_on_critical: bool = True

@dataclass
class ExpertMetricsConfig:
    enable_bio_integration: bool = True
    enable_event_driven: bool = True
    enable_swarm_coordination: bool = True
    enable_cross_domain: bool = True
    enable_human_ai: bool = True
    enable_sustainability_scoring: bool = True
    enable_cost_benefit: bool = True
    enable_mopd: bool = True

    anomaly_detection: AnomalyDetectionConfig = field(default_factory=AnomalyDetectionConfig)
    slo: SLOConfig = field(default_factory=SLOConfig)
    carbon: CarbonConfig = field(default_factory=CarbonConfig)
    federated: FederatedConfig = field(default_factory=FederatedConfig)
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)
    persistence: PersistenceConfig = field(default_factory=PersistenceConfig)
    self_healing: SelfHealingConfig = field(default_factory=SelfHealingConfig)
    mopd: MOPDConfig = field(default_factory=MOPDConfig)

    thresholds: Dict[str, MetricThreshold] = field(default_factory=lambda: {
        'latency_p95': MetricThreshold(metric_name='latency_p95', warning_threshold=100.0, critical_threshold=500.0, comparison='greater_than', gradient_modulated=True, cooldown_seconds=300.0),
        'error_rate': MetricThreshold(metric_name='error_rate', warning_threshold=0.05, critical_threshold=0.10, comparison='greater_than', gradient_modulated=True),
        'carbon_per_inference': MetricThreshold(metric_name='carbon_per_inference', warning_threshold=0.0005, critical_threshold=0.001, comparison='greater_than', gradient_modulated=True),
        'token_balance': MetricThreshold(metric_name='token_balance', warning_threshold=200.0, critical_threshold=50.0, comparison='less_than', gradient_modulated=True),
        'gradient_health': MetricThreshold(metric_name='gradient_health', warning_threshold=0.3, critical_threshold=0.1, comparison='less_than', gradient_modulated=True),
        'biomass_level': MetricThreshold(metric_name='biomass_level', warning_threshold=8000.0, critical_threshold=9500.0, comparison='greater_than', gradient_modulated=True),
        'sustainability_score': MetricThreshold(metric_name='sustainability_score', warning_threshold=0.7, critical_threshold=0.4, comparison='less_than', gradient_modulated=True)
    })

    workflow_on_slo_breach: str = "adjust_slo_targets"
    workflow_on_critical_alert: str = "rebalance_experts"

    token_exchange_rate: float = 1000.0
    swarm_share_interval_seconds: int = 60

# ============================================================================
# Carbon Intensity Manager
# ============================================================================
class CarbonIntensityManager:
    def __init__(self, config: CarbonConfig):
        self.config = config
        self.endpoint = "https://api.electricitymap.org/v3/carbon-intensity"
        self.region = config.region
        self.carbon_intensity = 0.0
        self.last_update: Optional[datetime] = None
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self.cache: Dict[str, Dict] = {}
        self.historical_intensities: Deque[float] = deque(maxlen=1000)
        self.api_key = os.getenv(config.api_key_env, '')
        self._circuit = CircuitBreaker(
            "carbon_api",
            failure_threshold=config.circuit_breaker_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def update_carbon_intensity(self, region: Optional[str] = None) -> Dict:
        if region is not None:
            self.region = region

        async def _fetch():
            cache_key = f"{self.region}_{datetime.now(timezone.utc).hour}"
            if (self.last_update and
                (datetime.now(timezone.utc) - self.last_update).seconds < self.config.update_interval_seconds and
                cache_key in self.cache):
                return self.cache[cache_key]

            for attempt in range(self.config.max_retries):
                try:
                    session = await self._get_session()
                    url = f"{self.endpoint}/latest?zone={self.region}"
                    headers = {'auth-token': self.api_key} if self.api_key else {}
                    async with session.get(url, headers=headers, timeout=10) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            self.carbon_intensity = data.get('carbonIntensity', 400)
                            self.last_update = datetime.now(timezone.utc)
                            result = {
                                'intensity': self.carbon_intensity,
                                'region': self.region,
                                'timestamp': self.last_update.isoformat()
                            }
                            self.cache[cache_key] = result
                            self.historical_intensities.append(self.carbon_intensity)
                            return result
                        else:
                            logger.warning(f"Carbon API returned {resp.status}, attempt {attempt+1}")
                except Exception as e:
                    logger.error(f"Carbon API error: {e}, attempt {attempt+1}")
                await asyncio.sleep(2 ** attempt)

            fallback_intensities = {'us-east': 420, 'us-west': 350, 'eu': 280, 'asia': 500}
            intensity = fallback_intensities.get(self.region, 400)
            self.carbon_intensity = intensity
            self.last_update = datetime.now(timezone.utc)
            result = {'intensity': intensity, 'region': self.region, 'timestamp': self.last_update.isoformat(), 'is_fallback': True}
            return result

        return await self._circuit.call(_fetch)

    async def get_current_intensity(self) -> float:
        if self.last_update is None or (datetime.now(timezone.utc) - self.last_update).seconds > self.config.update_interval_seconds:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Predictive Metrics Analyzer (with fixed sklearn imports)
# ============================================================================
class PredictiveMetricsAnalyzer:
    def __init__(self, config: ExpertMetricsConfig, history_window: int = 100):
        self.config = config
        self.history_window = history_window
        self.metric_history: Deque[Dict] = deque(maxlen=history_window)
        self.forecast_history: Deque[Dict] = deque(maxlen=50)
        self.scaler = StandardScaler() if SKLEARN_AVAILABLE else None
        self.model = None
        self.is_trained = False
        self.violation_model = None
        self.slo_violation_history: Deque[Dict] = deque(maxlen=1000)
        self._ml_available = SKLEARN_AVAILABLE
        self._lock = asyncio.Lock()
        self._init_models()

    def _init_models(self):
        if SKLEARN_AVAILABLE:
            try:
                self.model = SGDRegressor(
                    learning_rate='constant',
                    eta0=0.01,
                    penalty='l2',
                    alpha=0.0001,
                    max_iter=1,
                    random_state=42,
                    warm_start=True
                )
                self.violation_model = SGDRegressor(
                    learning_rate='constant',
                    eta0=0.01,
                    penalty='l2',
                    alpha=0.0001,
                    max_iter=1,
                    random_state=42,
                    warm_start=True
                )
            except Exception as e:
                logger.warning(f"Failed to initialize sklearn models: {e}")
                self._ml_available = False
        else:
            self._ml_available = False

    def update_history(self, metric_data: Dict):
        self.metric_history.append({
            'timestamp': datetime.now(timezone.utc),
            'success_rate': metric_data.get('success_rate', 0.8),
            'avg_latency_ms': metric_data.get('avg_latency_ms', 100),
            'carbon_intensity': metric_data.get('carbon_intensity', 400),
            'token_efficiency': metric_data.get('token_efficiency', 0.5),
            'health_score': metric_data.get('health_score', 0.5),
            'slo_compliant': metric_data.get('slo_compliant', 1.0)
        })

    async def train_forecast_model(self):
        if not self._ml_available or self.model is None:
            return {'status': 'ml_not_available'}
        if len(self.metric_history) < 10:
            return {'status': 'insufficient_data'}
        async with self._lock:
            X, y = [], []
            history_list = list(self.metric_history)
            for i in range(len(history_list) - 5):
                features = []
                for j in range(5):
                    data = history_list[i + j]
                    features.extend([
                        data['success_rate'],
                        data['avg_latency_ms'] / 1000,
                        data['carbon_intensity'] / 100,
                        data['token_efficiency'],
                        data['health_score'],
                        data.get('slo_compliant', 1.0)
                    ])
                X.append(features)
                y.append(history_list[i + 5]['health_score'])
            X = np.array(X)
            y = np.array(y)
            if self.scaler.mean_ is None:
                X_scaled = self.scaler.fit_transform(X)
            else:
                X_scaled = self.scaler.transform(X)
            for _ in range(3):
                self.model.partial_fit(X_scaled, y)
            self.is_trained = True
            try:
                from sklearn.metrics import r2_score
                pred = self.model.predict(X_scaled)
                r2 = r2_score(y, pred) if len(y) > 5 else 0.0
            except:
                r2 = 0.0
            return {'status': 'success', 'r2': r2, 'samples': len(X)}

    async def predict_slo_violation(self, features: Dict[str, float]) -> float:
        if not self._ml_available or self.violation_model is None:
            return 0.5
        try:
            X = np.array([[
                features.get('success_rate', 0.8),
                features.get('avg_latency_ms', 100) / 1000,
                features.get('carbon_intensity', 400) / 100,
                features.get('token_efficiency', 0.5),
                features.get('health_score', 0.5)
            ]])
            if self.scaler.mean_ is not None:
                X_scaled = self.scaler.transform(X)
            else:
                X_scaled = X
            prob = self.violation_model.predict(X_scaled)[0]
            prob = max(0.0, min(1.0, prob))
            self.slo_violation_history.append({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'probability': prob,
                'features': features
            })
            return prob
        except Exception as e:
            logger.warning(f"SLO violation prediction failed: {e}")
            return 0.5

# ============================================================================
# ML Anomaly Detector
# ============================================================================
class MLAnomalyDetector:
    def __init__(self, config: AnomalyDetectionConfig):
        self.config = config
        if SKLEARN_AVAILABLE:
            self.model = IsolationForest(contamination=config.contamination, n_estimators=config.n_estimators, random_state=42)
            self.scaler = StandardScaler()
            self.is_trained = False
        else:
            self.model = None
            self.scaler = None
            self.is_trained = False
        self.training_window: List[List[float]] = []
        self.window_size = config.window_size
        self._lock = asyncio.Lock()

    async def add_sample(self, metrics: Dict[str, float]):
        feature_vector = [
            metrics.get('success_rate', 0.5),
            metrics.get('latency_ms', 100) / 1000,
            metrics.get('carbon_per_inference', 0.001) * 1000,
            metrics.get('helium_per_inference', 0.01),
            metrics.get('token_efficiency', 0.5),
            metrics.get('health_score', 0.5),
            metrics.get('gradient_level', 0.5)
        ]
        async with self._lock:
            self.training_window.append(feature_vector)
            if len(self.training_window) >= self.window_size:
                await self._retrain()

    async def _retrain(self):
        if len(self.training_window) < 10 or self.model is None:
            return
        X = np.array(self.training_window)
        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled)
        self.is_trained = True
        self.training_window = self.training_window[-self.window_size:]
        logger.debug(f"ML Anomaly Detector retrained on {len(self.training_window)} samples")

    async def detect_anomaly(self, metrics: Dict[str, float]) -> Tuple[bool, float, str]:
        if not self.is_trained or self.model is None:
            return False, 0.0, "Model not trained"
        feature_vector = [
            metrics.get('success_rate', 0.5),
            metrics.get('latency_ms', 100) / 1000,
            metrics.get('carbon_per_inference', 0.001) * 1000,
            metrics.get('helium_per_inference', 0.01),
            metrics.get('token_efficiency', 0.5),
            metrics.get('health_score', 0.5),
            metrics.get('gradient_level', 0.5)
        ]
        X = np.array([feature_vector])
        X_scaled = self.scaler.transform(X)
        prediction = self.model.predict(X_scaled)[0]
        is_anomaly = prediction == -1
        decision = self.model.decision_function(X_scaled)[0]
        confidence = abs(decision) / (abs(decision) + 1)
        description = "ML-detected anomaly"
        if is_anomaly:
            if decision < -0.5:
                description = "Severe anomaly detected (high deviation)"
            elif decision < -0.2:
                description = "Moderate anomaly detected"
            else:
                description = "Slight anomaly detected"
        return is_anomaly, confidence, description

# ============================================================================
# SLOTracker
# ============================================================================
class SLOTracker:
    def __init__(self, config: SLOConfig):
        self.config = config
        self.slos: Dict[str, ServiceLevelObjective] = {}
        self.metric_samples: Dict[str, List[float]] = defaultdict(list)
        self.violation_history: Dict[str, List[datetime]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self.alpha = 0.3
        self.last_value: Dict[str, float] = {}
        self.trend: Dict[str, float] = {}

    def define_slo(self, slo_id: str, metric_name: str, target_value: float,
                   target_percentile: float = 99.0, evaluation_window_hours: float = 24.0) -> bool:
        if slo_id in self.slos:
            return False
        self.slos[slo_id] = ServiceLevelObjective(
            slo_id=slo_id, metric_name=metric_name, target_value=target_value,
            target_percentile=target_percentile, evaluation_window_hours=evaluation_window_hours
        )
        return True

    def record_metric(self, slo_id: str, value: float):
        if slo_id not in self.slos:
            return
        self.metric_samples[slo_id].append(value)
        if len(self.metric_samples[slo_id]) > 10000:
            self.metric_samples[slo_id] = self.metric_samples[slo_id][-10000:]

    async def evaluate_slos(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            results = {}
            for slo_id, slo in self.slos.items():
                samples = self.metric_samples.get(slo_id, [])
                if len(samples) < slo.min_samples:
                    results[slo_id] = {'status': 'insufficient_data', 'samples': len(samples)}
                    continue
                current = np.percentile(samples, slo.target_percentile)
                slo.current_value = current
                if current <= slo.target_value:
                    status = SLOStatus.COMPLIANT
                elif current <= slo.target_value * 1.2:
                    status = SLOStatus.AT_RISK
                else:
                    status = SLOStatus.BREACHED
                slo.status = status

                if slo_id not in self.last_value:
                    self.last_value[slo_id] = current
                    self.trend[slo_id] = 0.0
                else:
                    prev = self.last_value[slo_id]
                    self.last_value[slo_id] = self.alpha * current + (1 - self.alpha) * prev
                    if len(samples) > 5:
                        recent = samples[-5:]
                        x = np.arange(len(recent))
                        slope = np.polyfit(x, recent, 1)[0]
                        self.trend[slo_id] = 0.5 * slope + 0.5 * self.trend.get(slo_id, 0.0)

                forecast = self.last_value[slo_id] + self.trend.get(slo_id, 0.0) * 1
                if forecast > slo.target_value * 1.2:
                    violation_prob = 0.8
                elif forecast > slo.target_value * 1.05:
                    violation_prob = 0.4
                else:
                    violation_prob = 0.1
                slo.predicted_violation_probability = violation_prob

                if violation_prob > 0.3:
                    if self.trend.get(slo_id, 0.0) > 0:
                        time_to_breach = (slo.target_value * 1.05 - current) / (self.trend.get(slo_id, 0.0) * 10)
                        slo.next_predicted_violation = datetime.now(timezone.utc) + timedelta(seconds=max(30, min(3600, time_to_breach)))
                    else:
                        slo.next_predicted_violation = datetime.now(timezone.utc) + timedelta(hours=1)
                else:
                    slo.next_predicted_violation = None

                if status == SLOStatus.BREACHED:
                    self.violation_history[slo_id].append(datetime.now(timezone.utc))

                results[slo_id] = {
                    'status': status.value,
                    'current_value': current,
                    'target_value': slo.target_value,
                    'violation_probability': violation_prob,
                    'next_predicted_violation': slo.next_predicted_violation.isoformat() if slo.next_predicted_violation else None,
                    'samples': len(samples),
                    'violations': len(self.violation_history.get(slo_id, []))
                }
            return results

# ============================================================================
# Cost Attribution Engine
# ============================================================================
class CostAttributionEngine:
    def __init__(self):
        self.attributions: List[CostAttribution] = []
        self._lock = asyncio.Lock()

    async def record(self, expert_id: str, total_cost: float, carbon_cost: float,
                     helium_cost: float, token_cost: float):
        async with self._lock:
            attribution = CostAttribution(
                expert_id=expert_id,
                total_cost=total_cost,
                carbon_cost=carbon_cost,
                helium_cost=helium_cost,
                token_cost=token_cost,
                timestamp=datetime.now(timezone.utc)
            )
            self.attributions.append(attribution)
            if len(self.attributions) > 10000:
                self.attributions = self.attributions[-10000:]

    async def get_summary(self, expert_id: Optional[str] = None) -> Dict:
        async with self._lock:
            if expert_id:
                filtered = [a for a in self.attributions if a.expert_id == expert_id]
            else:
                filtered = self.attributions
            if not filtered:
                return {}
            return {
                'total_cost': sum(a.total_cost for a in filtered),
                'carbon_cost': sum(a.carbon_cost for a in filtered),
                'helium_cost': sum(a.helium_cost for a in filtered),
                'token_cost': sum(a.token_cost for a in filtered),
                'count': len(filtered)
            }

# ============================================================================
# Telemetry Exporter
# ============================================================================
class TelemetryExporter:
    def __init__(self, config: TelemetryConfig):
        self.config = config
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
# Persistence Manager (improved dataclass serialization)
# ============================================================================
class MetricsPersistenceManager:
    def __init__(self, config: PersistenceConfig):
        self.config = config
        self.path = config.path
        self._lock = asyncio.Lock()
        self._version = 2

    async def save_state(self, state: Dict[str, Any]) -> bool:
        async with self._lock:
            try:
                payload = {
                    'version': self._version,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'data': state
                }
                serializable = self._make_serializable(payload)
                with open(self.path, 'w') as f:
                    json.dump(serializable, f, indent=2)
                return True
            except Exception as e:
                logger.error(f"Failed to save metrics state: {e}")
                return False

    async def load_state(self) -> Optional[Dict]:
        async with self._lock:
            if not os.path.exists(self.path):
                return None
            try:
                with open(self.path, 'r') as f:
                    payload = json.load(f)
                state = payload.get('data', {})
                state = self._deserialize(state)
                return state
            except Exception as e:
                logger.error(f"Failed to load metrics state: {e}")
                return None

    def _make_serializable(self, obj: Any) -> Any:
        if hasattr(obj, 'to_dict'):
            return self._make_serializable(obj.to_dict())
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(v) for v in obj]
        elif isinstance(obj, tuple):
            return [self._make_serializable(v) for v in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, (deque, set)):
            return self._make_serializable(list(obj))
        elif hasattr(obj, '__dict__'):
            return self._make_serializable(obj.__dict__)
        else:
            return obj

    def _deserialize(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: self._deserialize(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._deserialize(v) for v in obj]
        elif isinstance(obj, str):
            try:
                return datetime.fromisoformat(obj)
            except ValueError:
                return obj
        else:
            return obj

# ============================================================================
# Federated Metrics Aggregator
# ============================================================================
class FederatedMetricsAggregator:
    def __init__(self, config: FederatedConfig):
        self.config = config
        self.server_url = config.server_url
        self.round = 0
        self.local_metrics = {}
        self.global_metrics = {}
        self.participants = []
        self.contribution_scores = {}
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self._circuit = CircuitBreaker("federated_server", failure_threshold=3, recovery_timeout=30.0)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session

    def _add_differential_privacy(self, metrics: Dict) -> Dict:
        epsilon = self.config.privacy_epsilon
        if epsilon <= 0:
            return metrics
        private = {}
        sensitivity = 1.0
        noise_scale = (2 * sensitivity) / epsilon
        for key, value in metrics.items():
            if isinstance(value, (int, float)):
                noise = np.random.normal(0, noise_scale * 0.001)
                private[key] = value + noise
            else:
                private[key] = value
        return private

    def _compress_metrics(self, metrics: Dict) -> Dict:
        sparsity = self.config.sparsity_ratio
        if sparsity == 1.0:
            return metrics
        numeric_metrics = {k: v for k, v in metrics.items() if isinstance(v, (int, float))}
        if not numeric_metrics:
            return metrics
        sorted_items = sorted(numeric_metrics.items(), key=lambda x: abs(x[1]), reverse=True)
        k = max(1, int(len(sorted_items) * sparsity))
        kept_keys = {item[0] for item in sorted_items[:k]}
        return {k: v for k, v in metrics.items() if k in kept_keys or not isinstance(v, (int, float))}

    async def send_local_metrics(self, participant_id: str, metrics: Dict, performance: float = 1.0) -> Dict:
        if not self.server_url:
            return {'status': 'local'}

        async def _send():
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        private = self._add_differential_privacy(metrics)
                        compressed = self._compress_metrics(private)
                        update_data = {
                            'participant_id': participant_id,
                            'round': self.round,
                            'metrics': compressed,
                            'performance': performance,
                            'privacy_epsilon': self.config.privacy_epsilon,
                            'sparsity_ratio': self.config.sparsity_ratio,
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        }
                        async with session.post(
                            f"{self.server_url}/federated/metrics",
                            json=update_data,
                            timeout=30
                        ) as resp:
                            if resp.status == 200:
                                result = await resp.json()
                                self.round += 1
                                self.contribution_scores[participant_id] = performance
                                return result
                            else:
                                logger.warning(f"Federated metrics send failed (attempt {attempt+1}): {resp.status}")
                except Exception as e:
                    logger.error(f"Federated metrics send error (attempt {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)
            return {'status': 'failed'}
        return await self._circuit.call(_send)

    async def get_global_metrics(self) -> Optional[Dict]:
        if not self.server_url:
            return self.global_metrics

        async def _fetch():
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        async with session.get(
                            f"{self.server_url}/federated/metrics/global",
                            timeout=30
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                self.global_metrics = data.get('metrics', {})
                                self.participants = data.get('participants', [])
                                return self.global_metrics
                            else:
                                logger.warning(f"Global metrics fetch failed (attempt {attempt+1}): {resp.status}")
                except Exception as e:
                    logger.error(f"Global metrics fetch error (attempt {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)
            return None
        return await self._circuit.call(_fetch)

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Metrics Storage (Enhanced with MOPD points)
# ============================================================================
class MetricsStorage:
    def __init__(self, retention_hours: float = 24.0):
        self.retention_hours = retention_hours
        self.expert_usage: Dict[str, int] = defaultdict(int)
        self.expert_success: Dict[str, int] = defaultdict(int)
        self.expert_failures: Dict[str, int] = defaultdict(int)
        self.expert_latency: Dict[str, Deque[Dict]] = defaultdict(lambda: deque(maxlen=10000))
        self.expert_energy: Dict[str, float] = defaultdict(float)
        self.expert_carbon: Dict[str, float] = defaultdict(float)
        self.expert_helium: Dict[str, float] = defaultdict(float)
        self.expert_ecoatp: Dict[str, float] = defaultdict(float)
        self.routing_decisions: Deque[Dict] = deque(maxlen=10000)
        self.routing_latency: Deque[float] = deque(maxlen=10000)
        self.pareto_points: Deque[Dict] = deque(maxlen=10000)
        self.mopd_points: Deque[MOPDPoint] = deque(maxlen=10000)
        self.bio_metrics_history: Deque[Dict] = deque(maxlen=10000)
        self.health_scores: Dict[str, float] = {}
        self.correlation_map: Dict[str, List[str]] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def record_routing(self, routing_decisions: List[Tuple[int, float]], execution_time: float,
                             success: bool, correlation_id: Optional[str] = None):
        async with self._lock:
            for expert_idx, _ in routing_decisions:
                self.expert_usage[expert_idx] += 1
                if success:
                    self.expert_success[expert_idx] += 1
                else:
                    self.expert_failures[expert_idx] += 1
            self.routing_latency.append(execution_time)
            self.routing_decisions.append({
                'decisions': routing_decisions,
                'execution_time': execution_time,
                'success': success,
                'timestamp': datetime.now(timezone.utc),
                'correlation_id': correlation_id
            })
            if correlation_id:
                self.correlation_map[correlation_id].append('routing')

    async def record_expert_execution(self, expert_id: str, execution_time: float,
                                      energy_kwh: float, carbon_kg: float, helium_units: float,
                                      success: bool, correlation_id: Optional[str] = None,
                                      metadata: Optional[Dict[str, Any]] = None):
        async with self._lock:
            self.expert_latency[expert_id].append({
                'value': execution_time,
                'timestamp': datetime.now(timezone.utc)
            })
            self.expert_energy[expert_id] += energy_kwh
            self.expert_carbon[expert_id] += carbon_kg
            self.expert_helium[expert_id] += helium_units
            if success:
                self.expert_success[expert_id] += 1
            else:
                self.expert_failures[expert_id] += 1
            mopd_point = MOPDPoint(
                expert_id=expert_id,
                timestamp=datetime.now(timezone.utc),
                carbon_kg=carbon_kg,
                helium_units=helium_units,
                ecoatp_cost=energy_kwh * 1000,
                latency_ms=execution_time,
                success_probability=1.0 if success else 0.0,
                metadata=metadata or {}
            )
            self.mopd_points.append(mopd_point)
            self.pareto_points.append({
                'expert_id': expert_id,
                'energy': energy_kwh,
                'time': execution_time,
                'helium': helium_units,
                'carbon': carbon_kg,
                'ecoatp': self.expert_ecoatp.get(expert_id, 0),
                'timestamp': datetime.now(timezone.utc)
            })
            self._prune_stale()

    def _prune_stale(self):
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.retention_hours)
        for expert_id in list(self.expert_latency.keys()):
            dq = self.expert_latency[expert_id]
            while dq and dq[0]['timestamp'] < cutoff:
                dq.popleft()

    async def compute_pareto_front(
        self,
        objective_names: List[str] = None,
        constraints: Dict[str, Tuple[float, float]] = None,
        max_points: int = 50
    ) -> List[MOPDPoint]:
        async with self._lock:
            if not self.mopd_points:
                return []
            if objective_names is None:
                objective_names = ['carbon_kg', 'helium_units', 'ecoatp_cost', 'latency_ms', 'success_probability']
            points = list(self.mopd_points)
            if constraints:
                filtered = []
                for p in points:
                    ok = True
                    for key, (low, high) in constraints.items():
                        val = getattr(p, key, None)
                        if val is None or not (low <= val <= high):
                            ok = False
                            break
                    if ok:
                        filtered.append(p)
                points = filtered

            if len(points) < 2:
                return points

            pareto = []
            for i, p_i in enumerate(points):
                dominated = False
                for j, p_j in enumerate(points):
                    if i == j:
                        continue
                    a_vec = []
                    b_vec = []
                    for key in objective_names:
                        val_i = getattr(p_i, key)
                        val_j = getattr(p_j, key)
                        if key == 'success_probability':
                            a_vec.append(-val_i)
                            b_vec.append(-val_j)
                        else:
                            a_vec.append(val_i)
                            b_vec.append(val_j)
                    if all(b <= a for a, b in zip(a_vec, b_vec)) and any(b < a for a, b in zip(a_vec, b_vec)):
                        dominated = True
                        break
                if not dominated:
                    pareto.append(p_i)
            pareto.sort(key=lambda p: p.timestamp, reverse=True)
            return pareto[:max_points]

    async def get_expert_usage(self) -> Dict[str, int]:
        async with self._lock:
            return dict(self.expert_usage)

    async def get_expert_success_rate(self) -> Dict[str, float]:
        async with self._lock:
            rates = {}
            for expert_id in self.expert_usage:
                total = self.expert_usage[expert_id]
                success = self.expert_success[expert_id]
                rates[expert_id] = success / total if total > 0 else 0.0
            return rates

    async def get_expert_latency_stats(self) -> Dict[str, Dict[str, float]]:
        async with self._lock:
            stats = {}
            for expert_id, dq in self.expert_latency.items():
                if dq:
                    values = [item['value'] for item in dq]
                    stats[expert_id] = {
                        'avg_ms': np.mean(values),
                        'p95_ms': np.percentile(values, 95),
                        'p99_ms': np.percentile(values, 99),
                        'min_ms': np.min(values),
                        'max_ms': np.max(values),
                        'count': len(values)
                    }
            return stats

    async def get_resource_consumption(self) -> Dict[str, Dict[str, float]]:
        async with self._lock:
            return {
                'energy': dict(self.expert_energy),
                'carbon': dict(self.expert_carbon),
                'helium': dict(self.expert_helium),
                'ecoatp': dict(self.expert_ecoatp)
            }

    async def get_pareto_frontier(self) -> List[Dict]:
        async with self._lock:
            return list(self.pareto_points)

    async def get_health_scores(self) -> Dict[str, float]:
        async with self._lock:
            return dict(self.health_scores)

    async def set_health_score(self, expert_id: str, score: float):
        async with self._lock:
            self.health_scores[expert_id] = score

# ============================================================================
# Metrics Analyzer
# ============================================================================
class MetricsAnalyzer:
    def __init__(self, config: ExpertMetricsConfig, storage: MetricsStorage):
        self.config = config
        self.storage = storage
        self.slo_tracker = SLOTracker(config.slo) if config.slo.enabled else None
        self.ml_anomaly_detector = MLAnomalyDetector(config.anomaly_detection) if config.anomaly_detection.ml_enabled else None
        self.predictive_analyzer = PredictiveMetricsAnalyzer(config) if config.anomaly_detection.enabled else None
        self._lock = asyncio.Lock()
        self.anomaly_events: Deque[AnomalyEvent] = deque(maxlen=1000)
        self.predictions: Dict[str, Dict] = {}

    async def analyze_routing(self, routing_decisions, execution_time, success, correlation_id=None):
        await self.storage.record_routing(routing_decisions, execution_time, success, correlation_id)
        if self.slo_tracker:
            await self.slo_tracker.record_metric('latency_slo', execution_time)
            rates = await self.storage.get_expert_success_rate()
            avg_success = np.mean(list(rates.values())) if rates else 0.0
            await self.slo_tracker.record_metric('availability_slo', avg_success)

    async def analyze_execution(self, expert_id, execution_time, energy_kwh, carbon_kg, helium_units, success, correlation_id=None, metadata=None):
        await self.storage.record_expert_execution(expert_id, execution_time, energy_kwh, carbon_kg, helium_units, success, correlation_id, metadata)
        if self.ml_anomaly_detector:
            metrics = {
                'success_rate': (await self.storage.get_expert_success_rate()).get(expert_id, 0.5),
                'latency_ms': execution_time,
                'carbon_per_inference': carbon_kg,
                'helium_per_inference': helium_units,
                'token_efficiency': 0.5,
                'health_score': (await self.storage.get_health_scores()).get(expert_id, 0.5),
                'gradient_level': 0.5
            }
            await self.ml_anomaly_detector.add_sample(metrics)
            is_anomaly, confidence, desc = await self.ml_anomaly_detector.detect_anomaly(metrics)
            if is_anomaly:
                await self._record_anomaly(expert_id, AnomalyType.ERROR_RATE, 0.5, 0.2, confidence, desc)

    async def _record_anomaly(self, expert_id, anomaly_type, expected, actual, severity, description):
        event = AnomalyEvent(anomaly_type=anomaly_type, severity=severity, expert_id=expert_id,
                             expected_value=expected, actual_value=actual,
                             timestamp=datetime.now(timezone.utc), description=description)
        self.anomaly_events.append(event)
        logger.warning(f"Anomaly recorded: {event}")

    async def get_anomaly_events(self) -> List[AnomalyEvent]:
        return list(self.anomaly_events)

# ============================================================================
# Metrics Reporter
# ============================================================================
class MetricsReporter:
    def __init__(self, config: ExpertMetricsConfig, storage: MetricsStorage, analyzer: MetricsAnalyzer):
        self.config = config
        self.storage = storage
        self.analyzer = analyzer
        self.telemetry = TelemetryExporter(config.telemetry) if config.telemetry.enabled else None
        self.persistence = MetricsPersistenceManager(config.persistence) if config.persistence.enabled else None
        self.human_ai_support = None
        self.cross_domain_transfer = None

    async def generate_summary(self) -> Dict[str, Any]:
        summary = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'expert_usage': await self.storage.get_expert_usage(),
            'success_rates': await self.storage.get_expert_success_rate(),
            'latency_stats': await self.storage.get_expert_latency_stats(),
            'resource_consumption': await self.storage.get_resource_consumption(),
            'pareto_frontier_size': len(await self.storage.get_pareto_frontier()),
            'total_routes': len(self.storage.routing_decisions),
            'avg_routing_latency_ms': np.mean(self.storage.routing_latency) if self.storage.routing_latency else 0.0,
            'health_scores': await self.storage.get_health_scores(),
        }
        if self.analyzer.slo_tracker:
            summary['slo_status'] = await self.analyzer.slo_tracker.evaluate_slos()
        if self.config.enable_mopd:
            pareto = await self.storage.compute_pareto_front(max_points=20)
            summary['mopd_pareto_front'] = [p.to_dict() for p in pareto]
        return summary

    async def export_telemetry(self):
        if self.telemetry:
            data = await self.telemetry.export()
            logger.debug(f"Telemetry export: {len(data)} bytes")

    async def save_state(self):
        if self.persistence:
            state = {
                'expert_usage': await self.storage.get_expert_usage(),
                'expert_success': dict(self.storage.expert_success),
                'expert_failures': dict(self.storage.expert_failures),
                'expert_energy': dict(self.storage.expert_energy),
                'expert_carbon': dict(self.storage.expert_carbon),
                'expert_helium': dict(self.storage.expert_helium),
                'expert_ecoatp': dict(self.storage.expert_ecoatp),
                'health_scores': await self.storage.get_health_scores(),
                'mopd_points': [p.to_dict() for p in self.storage.mopd_points],
            }
            await self.persistence.save_state(state)

    async def load_state(self):
        if self.persistence:
            state = await self.persistence.load_state()
            if state:
                mopd_points_dict = state.get('mopd_points', [])
                for p_dict in mopd_points_dict:
                    self.storage.mopd_points.append(MOPDPoint.from_dict(p_dict))
                health_scores = state.get('health_scores', {})
                async with self.storage._lock:
                    self.storage.health_scores.update(health_scores)

# ============================================================================
# Human-AI Collaborative Support (stub)
# ============================================================================
class HumanAICollaborativeSupport:
    def __init__(self):
        self.dashboard_data = {'metrics': deque(maxlen=1000), 'alerts': deque(maxlen=1000), 'insights': deque(maxlen=1000)}
        self._lock = asyncio.Lock()

    async def get_dashboard_data(self):
        async with self._lock:
            return {'recent_insights': list(self.dashboard_data['insights'])[-10:]}

# ============================================================================
# Cross-Domain Transfer (stub)
# ============================================================================
class MetricsCrossDomainTransfer:
    def transfer_knowledge(self, source, target, knowledge_type, data):
        pass

# ============================================================================
# Main ExpertMetricsCollector (Enhanced)
# ============================================================================
class ExpertMetricsCollector:
    """
    Enhanced Expert Metrics Collector v8.2.0 - Full Green Agent Integration with MOPD.
    """

    def __init__(
        self,
        bio_core: Optional[EnhancedBioInspiredCore] = None,
        config: Optional[ExpertMetricsConfig] = None,
        storage: Optional[Storage] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        adaptive_cost: Optional[AdaptiveCostFunction] = None,
        pareto_gating: Optional[ParetoGating] = None,
        drift_detector: Optional[DriftDetector] = None,
        metrics: Optional[MetricsRegistry] = None,
        **kwargs
    ):
        if config is None:
            config = ExpertMetricsConfig(**kwargs)
        self.config = config

        # Central components
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        # Bio-core
        self.bio_core = bio_core
        self.event_broker = None
        self.alert_system = None
        self.self_healer = None
        self.workflow_orchestrator = None
        self.token_manager = None
        self.gradient_manager = None
        self.swarm_coordinator = None
        if bio_core:
            self.event_broker = getattr(bio_core, 'event_broker', None)
            self.alert_system = getattr(bio_core, 'alert_system', None)
            self.self_healer = getattr(bio_core, 'self_healer', None)
            self.workflow_orchestrator = getattr(bio_core, 'workflow_orchestrator', None)
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.swarm_coordinator = getattr(bio_core, 'swarm_coordinator', None)

        # Sub-modules
        self.storage_metrics = MetricsStorage(retention_hours=config.persistence.retention_hours)
        self.analyzer = MetricsAnalyzer(config, self.storage_metrics)
        self.reporter = MetricsReporter(config, self.storage_metrics, self.analyzer)
        self.carbon_manager = CarbonIntensityManager(config.carbon) if config.carbon.enabled else None
        self.federated_aggregator = FederatedMetricsAggregator(config.federated) if config.federated.enabled else None

        # MoE injections
        self.expert_router = None
        self.gating_network = None
        self.self_evolving_gate = None
        self.helium_provider = None

        # Sustainability
        self.sustainability_score = 0.0
        self.total_carbon_savings_kg = 0.0
        self.total_helium_savings_l = 0.0

        # Alerts
        self.active_alerts = {}
        self.alert_history = deque(maxlen=5000)
        self.alert_cooldowns = {}

        # Event queue
        self._event_queue = asyncio.Queue()
        self._event_consumer_task = None

        # Background tasks
        self._background_tasks = []
        self.health_status = "healthy"
        self.last_error = None
        self._lock = asyncio.Lock()

        if self.config.enable_event_driven and self.event_broker:
            self._subscribe_events()

        # Safe task creation
        self._load_state_task = self._create_task(self._load_persisted_state())
        self._start_background_tasks()

        logger.info(
            f"Enhanced Expert Metrics Collector v8.2.0 initialized: "
            f"bio_integration={self.config.enable_bio_integration}, "
            f"mopd={self.config.enable_mopd}, "
            f"central_storage={storage is not None}, central_queue={message_queue is not None}"
        )

    def _create_task(self, coro):
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            logger.warning("No running event loop; background task not started.")
            return None

    def _subscribe_events(self):
        if self.event_broker:
            self.event_broker.subscribe('carbon_update', self._enqueue_event)
            self.event_broker.subscribe('helium_update', self._enqueue_event)
            self.event_broker.subscribe('alert_generated', self._enqueue_event)
            self.event_broker.subscribe('config_updated', self._enqueue_event)
            self.event_broker.subscribe('token_balance_update', self._enqueue_event)
            self.event_broker.subscribe('health_update', self._enqueue_event)
            self.event_broker.subscribe('anomaly_detected', self._enqueue_event)

    async def _enqueue_event(self, event):
        await self._event_queue.put(event)

    async def _event_consumer(self):
        while True:
            try:
                event = await self._event_queue.get()
                await self._handle_event(event)
                self._event_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Event consumer error: {e}")

    async def _handle_event(self, event):
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            try:
                await handler(event)
            except Exception as e:
                logger.error(f"Error handling event {event.event_type}: {e}")

    async def _on_carbon_update(self, event):
        if self.carbon_manager:
            self.carbon_manager.carbon_intensity = event.data.get('intensity', 400)

    async def _on_helium_update(self, event):
        pass

    async def _on_alert_generated(self, event):
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert received; triggering self-healing")
            if self.config.self_healing.enabled and self.self_healer:
                await self.self_healer.apply_healing('damage_accumulation')

    async def _on_config_updated(self, event):
        pass

    async def _on_token_update(self, event):
        pass

    async def _on_health_update(self, event):
        self.health_status = event.data.get('status', 'healthy')

    async def _on_anomaly_detected(self, event):
        pass

    def _start_background_tasks(self):
        if self.config.enable_event_driven:
            self._event_consumer_task = self._create_task(self._event_consumer())
            if self._event_consumer_task:
                self._background_tasks.append(self._event_consumer_task)
        if self.config.carbon.enabled:
            t = self._create_task(self._carbon_update_loop())
            if t: self._background_tasks.append(t)
        if self.config.federated.enabled:
            t = self._create_task(self._federated_sync_loop())
            if t: self._background_tasks.append(t)
        if self.config.telemetry.enabled:
            t = self._create_task(self._telemetry_export_loop())
            if t: self._background_tasks.append(t)
        if self.config.persistence.enabled:
            t = self._create_task(self._persistence_save_loop())
            if t: self._background_tasks.append(t)

    async def _carbon_update_loop(self):
        while True:
            try:
                if self.carbon_manager:
                    await self.carbon_manager.update_carbon_intensity()
                await asyncio.sleep(self.config.carbon.update_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update error: {e}")
                await asyncio.sleep(60)

    async def _federated_sync_loop(self):
        while True:
            try:
                if self.federated_aggregator:
                    summary = await self.reporter.generate_summary()
                    metrics = {
                        'avg_success_rate': np.mean(list(summary.get('success_rates', {}).values())),
                        'sustainability_score': self.sustainability_score,
                    }
                    await self.federated_aggregator.send_local_metrics(f"metrics_{id(self)}", metrics, self.sustainability_score)
                    await self.federated_aggregator.get_global_metrics()
                await asyncio.sleep(self.config.federated.sync_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Federated sync error: {e}")
                await asyncio.sleep(300)

    async def _telemetry_export_loop(self):
        while True:
            try:
                await self.reporter.export_telemetry()
                await asyncio.sleep(self.config.telemetry.export_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Telemetry export error: {e}")
                await asyncio.sleep(60)

    async def _persistence_save_loop(self):
        while True:
            try:
                await self.reporter.save_state()
                await asyncio.sleep(self.config.persistence.save_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Persistence save error: {e}")
                await asyncio.sleep(60)

    # ========================================================================
    # Public API – Routing and Execution Recording
    # ========================================================================
    async def record_routing(self, routing_decisions, gating_context, execution_time, success, correlation_id=None):
        await self.analyzer.analyze_routing(routing_decisions, execution_time, success, correlation_id)

        # Update gating network if available
        if self.gating_network and self.expert_router:
            features = np.array([len(routing_decisions), execution_time, self.sustainability_score])
            reward = 1.0 if success else 0.0
            context = {'success': success, 'execution_time_ms': execution_time}
            self.gating_network.update(features, reward, context)

        if self.self_evolving_gate and TORCH_AVAILABLE:
            state_tensor = torch.tensor([execution_time, self.sustainability_score], dtype=torch.float32)
            chosen = routing_decisions[0][0] if routing_decisions else 0
            self.self_evolving_gate.adapt(state=state_tensor, chosen_expert=chosen,
                                          reward=1.0 if success else 0.0,
                                          environmental_feedback={'success': success},
                                          quantum_mode=False)

        # Publish FeedbackEvent
        if self.queue:
            event = FeedbackEvent.create_with_context(
                task_id=correlation_id or f"routing_{uuid.uuid4().hex[:8]}",
                selected_action=f"route_{routing_decisions[0][0] if routing_decisions else 'none'}",
                quality_score=1.0 if success else 0.0,
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="metrics_routing",
                adaptive_cost_value=0.0,
                state={'routing_decisions': routing_decisions, 'success': success},
                candidates=[{'action': f"expert_{idx}"} for idx, _ in routing_decisions],
                source="expert_metrics_collector",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["metrics", "routing"]
            )
            await self.queue.publish("feedback_events", event.to_json())

    async def record_expert_execution(self, expert_id, execution_time, energy_kwh, carbon_kg, helium_units,
                                      success, correlation_id=None, metadata=None):
        # Bio-inspired: spend ATP before execution (estimate)
        if self.token_manager and energy_kwh > 0:
            ecoatp_cost = energy_kwh * self.config.token_exchange_rate
            await self.token_manager.spend(f"expert_{expert_id}", ecoatp_cost)
            if success:
                await self.token_manager.earn(f"expert_{expert_id}", ecoatp_cost * 0.5)

        await self.analyzer.analyze_execution(expert_id, execution_time, energy_kwh, carbon_kg, helium_units,
                                              success, correlation_id, metadata)

        # Pump gradients
        if self.gradient_manager:
            delta = 0.05 if success else -0.1
            await self.gradient_manager.pump_field('trust', delta, source=f"expert_{expert_id}")
            if carbon_kg > 0.001:
                await self.gradient_manager.pump_field('carbon', 0.05, source=f"expert_{expert_id}")
            if helium_units > 0:
                await self.gradient_manager.pump_field('helium', 0.05, source=f"expert_{expert_id}")

        await self._update_sustainability_score()

        # Publish FeedbackEvent
        if self.queue:
            event = FeedbackEvent.create_with_context(
                task_id=correlation_id or f"exec_{expert_id}_{uuid.uuid4().hex[:8]}",
                selected_action=expert_id,
                quality_score=1.0 if success else 0.0,
                energy_joules=energy_kwh * 3.6e6,
                carbon_g=carbon_kg * 1000.0,
                feedback_type="metrics_execution",
                adaptive_cost_value=0.0,
                state={'expert_id': expert_id, 'success': success},
                candidates=[{'action': expert_id}],
                source="expert_metrics_collector",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["metrics", "execution"]
            )
            await self.queue.publish("feedback_events", event.to_json())

        # Drift check
        if self.drift:
            drift_score = await self.drift.check_drift(self.adaptive_cost.get_current_weights() if self.adaptive_cost else {})
            if drift_score and drift_score > 0.7:
                logger.warning(f"High drift detected ({drift_score:.3f}); adjusting thresholds.")
                if 'carbon_per_inference' in self.config.thresholds:
                    self.config.thresholds['carbon_per_inference'].warning_threshold *= 0.95
                    self.config.thresholds['carbon_per_inference'].critical_threshold *= 0.95

    # ========================================================================
    # MOPD Public Methods
    # ========================================================================
    async def get_mopd_pareto_front(self, objective_names=None, constraints=None, max_points=50):
        if not self.config.enable_mopd:
            return []
        return await self.storage_metrics.compute_pareto_front(objective_names, constraints, max_points)

    async def get_mopd_summary(self):
        if not self.config.enable_mopd:
            return {'enabled': False}
        pareto = await self.storage_metrics.compute_pareto_front(max_points=20)
        return {
            'enabled': True,
            'pareto_front_size': len(pareto),
            'total_mopd_points': len(self.storage_metrics.mopd_points),
            'objective_weights': self.config.mopd.objective_weights,
            'grid_resolution': self.config.mopd.grid_resolution,
            'sample_pareto': [p.to_dict() for p in pareto[:5]]
        }

    # ========================================================================
    # Teacher Policy
    # ========================================================================
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        if not self.config.enable_mopd:
            usage = await self.storage_metrics.get_expert_usage()
            experts = list(usage.keys())
            return [1.0 / len(experts)] * len(experts) if experts else []

        pareto_points = await self.storage_metrics.compute_pareto_front(
            objective_names=['carbon_kg', 'helium_units', 'ecoatp_cost', 'latency_ms', 'success_probability'],
            max_points=50
        )
        if not pareto_points:
            usage = await self.storage_metrics.get_expert_usage()
            experts = list(usage.keys())
            return [1.0 / len(experts)] * len(experts) if experts else []

        scored = []
        for point in pareto_points:
            if self.adaptive_cost:
                cost = self.adaptive_cost.compute(
                    quality=point.success_probability,
                    carbon_g=point.carbon_kg * 1000.0,
                    latency_ms=point.latency_ms,
                    energy_joules=point.ecoatp_cost / self.config.token_exchange_rate if self.config.token_exchange_rate else point.ecoatp_cost,
                    health=0.8,
                    atp=point.ecoatp_cost / self.config.token_exchange_rate if self.config.token_exchange_rate else 0.5
                )
            else:
                weights = self.config.mopd.objective_weights
                cost = (weights['carbon'] * (1 - point.carbon_kg / 10.0) +
                        weights['helium'] * (1 - point.helium_units / 0.1) +
                        weights['cost'] * (1 - point.ecoatp_cost / 1000.0) +
                        weights['latency'] * (1 - point.latency_ms / 1000.0) +
                        weights['success'] * point.success_probability)
            scored.append((point.expert_id, cost))

        expert_scores = defaultdict(list)
        for expert_id, cost in scored:
            expert_scores[expert_id].append(cost)
        expert_avg = {eid: np.mean(scores) for eid, scores in expert_scores.items()}
        if not expert_avg:
            return []

        experts = list(expert_avg.keys())
        scores = np.array([expert_avg[eid] for eid in experts])
        exp_scores = np.exp(scores - np.max(scores))
        probs = exp_scores / np.sum(exp_scores)
        return probs.tolist()

    # ========================================================================
    # Sustainability and Helpers
    # ========================================================================
    async def _update_sustainability_score(self):
        health_scores = await self.storage_metrics.get_health_scores()
        avg_health = np.mean(list(health_scores.values())) if health_scores else 0.5
        token_eff = self._get_token_efficiency()
        carbon_factor = 1.0 - (self.carbon_manager.carbon_intensity / 800) if self.carbon_manager else 0.5
        success_rates = await self.storage_metrics.get_expert_success_rate()
        avg_success = np.mean(list(success_rates.values())) if success_rates else 0.5
        self.sustainability_score = min(1.0, max(0.0, avg_health * 0.25 + token_eff * 0.2 + carbon_factor * 0.25 + avg_success * 0.3))

    def _get_token_efficiency(self):
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            return summary.get('system_efficiency', 0.5)
        return 0.5

    # ========================================================================
    # Swarm, Self-Healing, Queries, Injection, Shutdown
    # ========================================================================
    async def share_with_swarm(self):
        if not self.config.enable_swarm_coordination or not self.swarm_coordinator:
            return
        payload = {
            'collector_id': id(self),
            'sustainability_score': self.sustainability_score,
            'mopd_enabled': self.config.enable_mopd,
        }
        await self.swarm_coordinator.share_predictions(payload)

    async def self_heal(self):
        logger.info("ExpertMetricsCollector self-healing")
        if self.config.self_healing.enabled:
            self.config.thresholds = ExpertMetricsConfig().thresholds
            if self.analyzer.slo_tracker:
                self.analyzer.slo_tracker.slos.clear()
                for slo_id, params in self.config.slo.slo_definitions.items():
                    self.analyzer.slo_tracker.define_slo(slo_id, **params)
            self.active_alerts.clear()
            self.health_status = "healthy"
            self.last_error = None

    async def get_expert_usage(self):
        return await self.storage_metrics.get_expert_usage()

    async def get_expert_success_rate(self):
        return await self.storage_metrics.get_expert_success_rate()

    async def get_expert_latency_stats(self):
        return await self.storage_metrics.get_expert_latency_stats()

    async def get_resource_consumption(self):
        return await self.storage_metrics.get_resource_consumption()

    async def get_pareto_frontier(self):
        return await self.storage_metrics.get_pareto_frontier()

    async def get_health_scores(self):
        return await self.storage_metrics.get_health_scores()

    async def get_alerts(self, acknowledged=None, severity=None, limit=50):
        alerts = [a for a in self.active_alerts.values()]
        if acknowledged is not None:
            alerts = [a for a in alerts if a.get('acknowledged') == acknowledged]
        if severity:
            alerts = [a for a in alerts if a.get('severity') == severity.value]
        return alerts[:limit]

    async def acknowledge_alert(self, alert_id):
        async with self._lock:
            if alert_id in self.active_alerts:
                self.active_alerts[alert_id]['acknowledged'] = True
                return True
            return False

    async def get_predictions(self):
        return self.analyzer.predictions

    async def get_slo_status(self):
        if self.analyzer.slo_tracker:
            return await self.analyzer.slo_tracker.evaluate_slos()
        return {}

    async def get_metrics_summary(self):
        return await self.reporter.generate_summary()

    async def get_health_status(self):
        return {
            'status': self.health_status,
            'last_error': self.last_error,
            'active_alerts': len([a for a in self.active_alerts.values() if not a.get('acknowledged')]),
            'sustainability_score': self.sustainability_score,
            'bio_integration_active': self.config.enable_bio_integration,
            'mopd_enabled': self.config.enable_mopd,
        }

    def set_gating_network(self, gating_network):
        self.gating_network = gating_network

    def set_self_evolving_gate(self, gate):
        self.self_evolving_gate = gate

    def set_expert_router(self, router):
        self.expert_router = router

    def set_helium_provider(self, provider):
        self.helium_provider = provider

    def inject_bio_core(self, bio_core):
        self.bio_core = bio_core
        if bio_core:
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.event_broker = getattr(bio_core, 'event_broker', None)
            self.self_healer = getattr(bio_core, 'self_healer', None)
            self.swarm_coordinator = getattr(bio_core, 'swarm_coordinator', None)

    async def _load_persisted_state(self):
        await self.reporter.load_state()

    async def shutdown(self):
        logger.info("Shutting down Expert Metrics Collector")
        for task in self._background_tasks:
            if task:
                task.cancel()
        await asyncio.gather(*[t for t in self._background_tasks if t], return_exceptions=True)
        if self.config.persistence.enabled:
            await self.reporter.save_state()
        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated_aggregator:
            await self.federated_aggregator.close()
        logger.info("Shutdown complete")
