# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/monitoring/expert_metrics.py
# Enhanced with complete bio-inspired integration - Metabolic Observatory v4.0.0

"""
Enhanced Expert Metrics Collector v4.0.0 - Metabolic Observatory

Complete bio-inspired integration with:
- Gradient field metrics (trust, carbon, helium as Prometheus metrics)
- Token economy observability (Eco-ATP balance, efficiency, consumption)
- Biomass storage metrics (total stored, tier distribution, collateral)
- Harvester vitality metrics (photosynthetic activity, excitation levels)
- Compartment health metrics (membrane permeability, population counts)
- Closed-loop feedback (metrics → gradient pumping → routing adaptation)
- Token-aware SLO tracking (Eco-ATP cost as SLO dimension)
- Gradient-modulated alerting (dynamic thresholds based on gradient state)
- Unified bio-inspired dashboard (all metabolic metrics in one view)
- Metabolic Pareto frontier (energy × tokens × time optimization)
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import time
import threading
import json
import hashlib
import math

logger = logging.getLogger(__name__)

# ============================================================================
# Try importing bio-inspired modules
# ============================================================================

try:
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
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired modules loaded for Expert Metrics")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired modules not available: {str(e)} - using standard metrics")

# ============================================================================
# Enums and Data Classes (Enhanced with Bio-Inspired)
# ============================================================================

class MetricSeverity(Enum):
    """Metric alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"

class MetricType(Enum):
    """Types of metrics"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"
    TREND = "trend"
    GRADIENT = "gradient"       # BIO-INSPIRED
    TOKEN = "token"             # BIO-INSPIRED
    BIOMASS = "biomass"         # BIO-INSPIRED
    HARVESTER = "harvester"     # BIO-INSPIRED
    COMPARTMENT = "compartment" # BIO-INSPIRED

class AnomalyType(Enum):
    """Types of anomalies detected"""
    SPIKE = "spike"
    DIP = "dip"
    TREND_CHANGE = "trend_change"
    LEVEL_SHIFT = "level_shift"
    VARIANCE_CHANGE = "variance_change"
    OUTLIER = "outlier"
    GRADIENT_ANOMALY = "gradient_anomaly"      # BIO-INSPIRED
    TOKEN_EXHAUSTION = "token_exhaustion"       # BIO-INSPIRED
    BIOMASS_OVERFLOW = "biomass_overflow"       # BIO-INSPIRED

class SLOStatus(Enum):
    """Service Level Objective status"""
    COMPLIANT = "compliant"
    AT_RISK = "at_risk"
    BREACHED = "breached"
    UNKNOWN = "unknown"

@dataclass
class MetricThreshold:
    """Threshold configuration for alerting with bio-inspired modulation"""
    metric_name: str
    warning_threshold: float
    critical_threshold: float
    comparison: str = "greater_than"
    duration_seconds: float = 60.0
    cooldown_seconds: float = 300.0
    gradient_modulated: bool = False  # BIO-INSPIRED
    
    def is_exceeded(self, value: float, gradient_modulation: float = 1.0) -> Tuple[bool, 'MetricSeverity']:
        effective_warning = self.warning_threshold * gradient_modulation
        effective_critical = self.critical_threshold * gradient_modulation
        
        if self.comparison == "greater_than":
            if value >= effective_critical:
                return True, MetricSeverity.CRITICAL
            elif value >= effective_warning:
                return True, MetricSeverity.WARNING
        elif self.comparison == "less_than":
            if value <= effective_critical:
                return True, MetricSeverity.CRITICAL
            elif value <= effective_warning:
                return True, MetricSeverity.WARNING
        return False, MetricSeverity.INFO

@dataclass
class ServiceLevelObjective:
    """Service Level Objective definition with token awareness"""
    slo_id: str
    metric_name: str
    target_value: float
    target_percentile: float = 99.0
    evaluation_window_hours: float = 24.0
    min_samples: int = 100
    current_value: float = 0.0
    status: SLOStatus = SLOStatus.UNKNOWN
    error_budget_remaining: float = 1.0
    last_evaluated: datetime = field(default_factory=datetime.utcnow)
    token_cost_per_violation: float = 10.0  # BIO-INSPIRED

@dataclass
class AnomalyEvent:
    """Anomaly detection event with bio-inspired context"""
    event_id: str
    metric_name: str
    anomaly_type: AnomalyType
    detected_at: datetime
    expected_value: float
    actual_value: float
    deviation_std: float
    severity: MetricSeverity
    expert_id: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    gradient_level: float = 0.5  # BIO-INSPIRED

@dataclass
class MetricSample:
    """Individual metric sample with bio-inspired metadata"""
    timestamp: datetime
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    trace_id: Optional[str] = None
    expert_id: Optional[str] = None
    token_cost: float = 0.0  # BIO-INSPIRED
    gradient_level: float = 0.5  # BIO-INSPIRED

@dataclass
class CostAttribution:
    """Carbon/helium/token cost attribution"""
    expert_id: str
    time_period: str
    total_carbon_kg: float = 0.0
    total_helium_units: float = 0.0
    total_energy_kwh: float = 0.0
    total_ecoatp_cost: float = 0.0  # BIO-INSPIRED
    cost_per_operation: float = 0.0
    carbon_efficiency_score: float = 0.0
    helium_efficiency_score: float = 0.0
    token_efficiency_score: float = 0.0  # BIO-INSPIRED
    trend: str = "stable"

# ============================================================================
# Anomaly Detector with Bio-Inspired Integration
# ============================================================================

class AnomalyDetector:
    """Statistical anomaly detection for metrics with bio-inspired awareness"""
    
    def __init__(self, zscore_threshold: float = 3.0, iqr_multiplier: float = 1.5,
                 window_size: int = 100, min_samples: int = 30):
        self.zscore_threshold = zscore_threshold
        self.iqr_multiplier = iqr_multiplier
        self.window_size = window_size
        self.min_samples = min_samples
        self.metric_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=window_size))
        self.detection_history: deque = deque(maxlen=1000)
        self.gradient_manager: Optional[GradientFieldManager] = None  # BIO-INSPIRED
    
    def inject_gradient_manager(self, gradient_manager):
        """Inject gradient manager for anomaly context"""
        self.gradient_manager = gradient_manager
    
    def add_sample(self, metric_name: str, value: float):
        self.metric_history[metric_name].append({'value': value, 'timestamp': datetime.utcnow()})
    
    def detect_anomalies(self, metric_name: str, current_value: float,
                        expert_id: Optional[str] = None) -> List[AnomalyEvent]:
        """Detect anomalies with gradient-modulated thresholds"""
        anomalies = []
        history = self.metric_history.get(metric_name)
        if not history or len(history) < self.min_samples:
            return anomalies
        
        values = np.array([h['value'] for h in history])
        
        # BIO-INSPIRED: Get gradient modulation for thresholds
        gradient_mod = 1.0
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength > 0.7:
                gradient_mod = 0.7  # More sensitive in high carbon stress
        
        # Z-score detection with gradient modulation
        zscore_anomaly = self._zscore_detect(metric_name, current_value, values, gradient_mod)
        if zscore_anomaly:
            anomalies.append(zscore_anomaly)
        
        # IQR detection
        iqr_anomaly = self._iqr_detect(metric_name, current_value, values)
        if iqr_anomaly:
            anomalies.append(iqr_anomaly)
        
        # BIO-INSPIRED: Token exhaustion detection
        if 'token' in metric_name.lower() and current_value < 10.0:
            anomalies.append(AnomalyEvent(
                event_id=f"token_exhaustion_{datetime.utcnow().timestamp()}",
                metric_name=metric_name,
                anomaly_type=AnomalyType.TOKEN_EXHAUSTION,
                detected_at=datetime.utcnow(),
                expected_value=100.0, actual_value=current_value,
                deviation_std=5.0, severity=MetricSeverity.CRITICAL,
                expert_id=expert_id
            ))
        
        for anomaly in anomalies:
            anomaly.expert_id = expert_id
            if self.gradient_manager:
                carbon = self.gradient_manager.fields.get('carbon')
                anomaly.gradient_level = carbon.gradient_strength if carbon else 0.5
        
        for anomaly in anomalies:
            self.detection_history.append(anomaly)
        
        return anomalies
    
    def _zscore_detect(self, metric_name: str, current_value: float,
                      values: np.ndarray, gradient_mod: float = 1.0) -> Optional[AnomalyEvent]:
        mean = np.mean(values)
        std = np.std(values)
        if std == 0:
            return None
        zscore = abs(current_value - mean) / std
        effective_threshold = self.zscore_threshold * gradient_mod
        
        if zscore > effective_threshold:
            return AnomalyEvent(
                event_id=f"anomaly_{datetime.utcnow().timestamp()}_{metric_name}",
                metric_name=metric_name,
                anomaly_type=AnomalyType.OUTLIER if current_value > mean else AnomalyType.DIP,
                detected_at=datetime.utcnow(),
                expected_value=mean, actual_value=current_value,
                deviation_std=zscore,
                severity=MetricSeverity.CRITICAL if zscore > effective_threshold * 1.5 else MetricSeverity.WARNING
            )
        return None
    
    def _iqr_detect(self, metric_name: str, current_value: float,
                   values: np.ndarray) -> Optional[AnomalyEvent]:
        q1, q3 = np.percentile(values, [25, 75])
        iqr = q3 - q1
        lower_bound = q1 - self.iqr_multiplier * iqr
        upper_bound = q3 + self.iqr_multiplier * iqr
        
        if current_value < lower_bound or current_value > upper_bound:
            deviation = (current_value - np.median(values)) / (iqr + 1e-8)
            return AnomalyEvent(
                event_id=f"anomaly_iqr_{datetime.utcnow().timestamp()}_{metric_name}",
                metric_name=metric_name,
                anomaly_type=AnomalyType.OUTLIER,
                detected_at=datetime.utcnow(),
                expected_value=np.median(values), actual_value=current_value,
                deviation_std=abs(deviation),
                severity=MetricSeverity.WARNING
            )
        return None
    
    def get_detection_stats(self) -> Dict[str, Any]:
        return {
            'total_detections': len(self.detection_history),
            'recent_detections': [
                {'metric': d.metric_name, 'type': d.anomaly_type.value,
                 'severity': d.severity.value, 'deviation': d.deviation_std,
                 'gradient_level': d.gradient_level,
                 'timestamp': d.detected_at.isoformat()}
                for d in list(self.detection_history)[-20:]
            ]
        }

# ============================================================================
# Enhanced Expert Metrics Collector with Complete Bio-Inspired Integration
# ============================================================================

class ExpertMetricsCollector:
    """
    Enhanced Expert Metrics Collector v4.0.0 - Metabolic Observatory
    
    Complete bio-inspired integration:
    - Gradient field metrics export
    - Token economy observability
    - Biomass storage metrics
    - Harvester vitality metrics
    - Compartment health metrics
    - Closed-loop feedback (metrics → gradients)
    - Token-aware SLO tracking
    - Gradient-modulated alerting
    - Unified bio-inspired dashboard
    - Metabolic Pareto frontier
    """
    
    def __init__(
        self,
        enable_anomaly_detection: bool = True,
        enable_slo_tracking: bool = True,
        enable_cost_attribution: bool = True,
        enable_alerting: bool = True,
        enable_predictive: bool = True,
        enable_bio_integration: bool = True,
        retention_hours: float = 24.0
    ):
        # Feature flags
        self.enable_anomaly_detection = enable_anomaly_detection
        self.enable_slo_tracking = enable_slo_tracking
        self.enable_cost_attribution = enable_cost_attribution
        self.enable_alerting = enable_alerting
        self.enable_predictive = enable_predictive
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        self.retention_hours = retention_hours
        
        # BIO-INSPIRED: Module references (injected)
        self.token_manager: Optional[EcoATPTokenManager] = None
        self.gradient_manager: Optional[GradientFieldManager] = None
        self.scheduler: Optional[ATPSynthaseScheduler] = None
        self.compartment_manager: Optional[CompartmentManager] = None
        self.biomass_storage: Optional[BiomassStorage] = None
        self.harvester: Optional[PhotosyntheticHarvester] = None
        
        # Sub-modules
        self.anomaly_detector = AnomalyDetector() if enable_anomaly_detection else None
        self.slo_tracker = SLOTracker() if enable_slo_tracking else None
        self.cost_engine = CostAttributionEngine() if enable_cost_attribution else None
        
        # Expert usage metrics
        self.expert_usage: Dict[str, int] = defaultdict(int)
        self.expert_success: Dict[str, int] = defaultdict(int)
        self.expert_failures: Dict[str, int] = defaultdict(int)
        self.expert_latency: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        
        # Routing metrics
        self.routing_decisions: deque = deque(maxlen=10000)
        self.routing_latency: deque = deque(maxlen=10000)
        
        # Resource metrics
        self.expert_energy: Dict[str, float] = defaultdict(float)
        self.expert_carbon: Dict[str, float] = defaultdict(float)
        self.expert_helium: Dict[str, float] = defaultdict(float)
        self.expert_ecoatp: Dict[str, float] = defaultdict(float)  # BIO-INSPIRED
        
        # Pareto frontier data
        self.pareto_points: deque = deque(maxlen=10000)
        
        # Alert management
        self.active_alerts: Dict[str, Dict[str, Any]] = {}
        self.alert_history: deque = deque(maxlen=5000)
        self.alert_cooldowns: Dict[str, datetime] = {}
        
        # Thresholds with gradient modulation
        self.thresholds: Dict[str, MetricThreshold] = {}
        self._initialize_thresholds()
        
        # SLO definitions
        if self.slo_tracker:
            self._initialize_slos()
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Health scores
        self.health_scores: Dict[str, float] = {}
        
        # Predictive models
        self.predictions: Dict[str, Dict[str, Any]] = {}
        
        # Correlation tracking
        self.correlation_map: Dict[str, List[str]] = defaultdict(list)
        
        # BIO-INSPIRED: Bio metrics history
        self.bio_metrics_history: deque = deque(maxlen=10000)
        
        logger.info(
            f"Enhanced Expert Metrics Collector v4.0.0 initialized: "
            f"bio_integration={self.enable_bio_integration}, "
            f"bio_available={BIO_INSPIRED_AVAILABLE}"
        )
    
    def _initialize_thresholds(self):
        """Initialize metric thresholds with gradient modulation"""
        self.thresholds = {
            'latency_p95': MetricThreshold('latency_p95', 100.0, 500.0, 'greater_than', gradient_modulated=True),
            'error_rate': MetricThreshold('error_rate', 0.05, 0.10, 'greater_than', gradient_modulated=True),
            'carbon_per_inference': MetricThreshold('carbon_per_inference', 0.0005, 0.001, 'greater_than', gradient_modulated=True),
            'token_balance': MetricThreshold('token_balance', 200.0, 50.0, 'less_than', gradient_modulated=True),
            'gradient_health': MetricThreshold('gradient_health', 0.3, 0.1, 'less_than', gradient_modulated=True),
            'biomass_level': MetricThreshold('biomass_level', 8000.0, 9500.0, 'greater_than', gradient_modulated=True)
        }
    
    def _initialize_slos(self):
        """Initialize SLOs with token costs"""
        self.slo_tracker.define_slo('latency_slo', 'expert_latency_ms', target_value=100.0, target_percentile=99.0)
        self.slo_tracker.define_slo('availability_slo', 'expert_success_rate', target_value=0.999, target_percentile=99.9)
        self.slo_tracker.define_slo('carbon_slo', 'carbon_per_inference', target_value=0.0005, target_percentile=95.0)
        self.slo_tracker.define_slo('token_efficiency_slo', 'token_efficiency', target_value=0.8, target_percentile=90.0)
    
    # ========================================================================
    # Bio-Inspired Module Injection
    # ========================================================================
    
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        """
        Inject bio-inspired modules for metrics correlation.
        
        Connects metrics to real bio-inspired systems for closed-loop feedback.
        """
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
        
        # Inject gradient manager into anomaly detector
        if self.anomaly_detector and self.gradient_manager:
            self.anomaly_detector.inject_gradient_manager(self.gradient_manager)
        
        injections = {
            'token_manager': self.token_manager is not None,
            'gradient_manager': self.gradient_manager is not None,
            'scheduler': self.scheduler is not None,
            'compartment_manager': self.compartment_manager is not None,
            'biomass_storage': self.biomass_storage is not None,
            'harvester': self.harvester is not None
        }
        logger.info(f"Bio-inspired injections into Expert Metrics: {injections}")
        
        if any(injections.values()):
            self.enable_bio_integration = True
    
    # ========================================================================
    # Bio-Inspired Data Access Methods
    # ========================================================================
    
    def _get_gradient_modulation(self) -> float:
        """Get gradient-based modulation for thresholds"""
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength > 0.7:
                return 0.7  # More sensitive thresholds in high carbon
        return 1.0
    
    def _pump_trust_gradient(self, expert_id: str, success: bool):
        """Pump trust gradient based on expert performance"""
        if self.gradient_manager:
            delta = 0.05 if success else -0.1
            self.gradient_manager.pump_field('trust', delta, source=f"expert_{expert_id}")
    
    def _record_token_consumption(self, expert_id: str, energy_kwh: float, success: bool):
        """Record token consumption for expert execution"""
        if self.token_manager:
            ecoatp_cost = energy_kwh * 1000
            self.expert_ecoatp[expert_id] += ecoatp_cost
            
            if hasattr(self.token_manager, 'consume_tokens'):
                try:
                    self.token_manager.consume_tokens(
                        token_ids=[f"expert_{expert_id}"],
                        consumer=EcoATPConsumer.EXPERT_EXECUTION,
                        operation_success=success
                    )
                except Exception:
                    pass
    
    def _get_bio_metrics(self) -> Dict[str, Any]:
        """Collect all bio-inspired metrics"""
        metrics = {'timestamp': datetime.utcnow().isoformat()}
        
        if self.gradient_manager:
            metrics['gradients'] = self.gradient_manager.get_field_strengths()
        
        if self.token_manager:
            metrics['token_economy'] = self.token_manager.get_system_summary()
        
        if self.biomass_storage:
            metrics['biomass'] = self.biomass_storage.get_storage_stats()
        
        if self.harvester:
            metrics['harvester'] = self.harvester.get_harvesting_stats()
        
        if self.compartment_manager:
            metrics['compartments'] = {
                'total': len(self.compartment_manager.compartments),
                'viable': sum(1 for c in self.compartment_manager.compartments.values() if c.is_viable)
            }
        
        if self.scheduler:
            metrics['atp_synthase'] = self.scheduler.get_scheduler_stats()
        
        return metrics
    
    # ========================================================================
    # Enhanced Metric Recording with Bio-Inspired Feedback
    # ========================================================================
    
    def record_routing(
        self, routing_decisions: List[Tuple[int, float]], gating_context: Any,
        execution_time: float, success: bool, correlation_id: Optional[str] = None
    ):
        """Record routing decision with bio-inspired feedback"""
        with self._lock:
            for expert_idx, weight in routing_decisions:
                self.expert_usage[expert_idx] = self.expert_usage.get(expert_idx, 0) + 1
                if success:
                    self.expert_success[expert_idx] = self.expert_success.get(expert_idx, 0) + 1
                else:
                    self.expert_failures[expert_idx] = self.expert_failures.get(expert_idx, 0) + 1
            
            self.routing_latency.append(execution_time)
            
            decision_record = {
                'decisions': routing_decisions, 'context': str(gating_context)[:200],
                'execution_time': execution_time, 'success': success,
                'timestamp': datetime.utcnow(), 'correlation_id': correlation_id
            }
            self.routing_decisions.append(decision_record)
            
            if correlation_id:
                self.correlation_map[correlation_id].append('routing')
    
    def record_expert_execution(
        self, expert_id: str, execution_time: float, energy_kwh: float,
        carbon_kg: float, helium_units: float, success: bool,
        correlation_id: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None
    ):
        """Record expert execution with bio-inspired feedback loop"""
        with self._lock:
            # Latency tracking
            self.expert_latency[expert_id].append({'value': execution_time, 'timestamp': datetime.utcnow()})
            
            # Resource tracking
            self.expert_energy[expert_id] += energy_kwh
            self.expert_carbon[expert_id] += carbon_kg
            self.expert_helium[expert_id] += helium_units
            
            # BIO-INSPIRED: Record token consumption
            if self.enable_bio_integration:
                self._record_token_consumption(expert_id, energy_kwh, success)
            
            # Success tracking
            if success:
                self.expert_success[expert_id] = self.expert_success.get(expert_id, 0) + 1
            else:
                self.expert_failures[expert_id] = self.expert_failures.get(expert_id, 0) + 1
            
            # BIO-INSPIRED: Pump trust gradient based on performance
            if self.enable_bio_integration:
                self._pump_trust_gradient(expert_id, success)
            
            # BIO-INSPIRED: Record Pareto point with token dimension
            self.pareto_points.append({
                'expert_id': expert_id, 'energy': energy_kwh, 'time': execution_time,
                'helium': helium_units, 'carbon': carbon_kg,
                'ecoatp': self.expert_ecoatp.get(expert_id, 0),  # BIO-INSPIRED
                'timestamp': datetime.utcnow()
            })
            
            # Anomaly detection with gradient awareness
            if self.enable_anomaly_detection:
                anomalies = self.anomaly_detector.detect_anomalies(
                    f"{expert_id}_latency", execution_time, expert_id
                )
                for anomaly in anomalies:
                    self._process_anomaly(anomaly)
            
            # BIO-INSPIRED: Record bio metrics periodically
            if self.enable_bio_integration and len(self.bio_metrics_history) % 100 == 0:
                self.bio_metrics_history.append(self._get_bio_metrics())
            
            # SLO tracking
            if self.enable_slo_tracking:
                self.slo_tracker.record_metric('latency_slo', execution_time)
                self.slo_tracker.record_metric('carbon_slo', carbon_kg)
                if self.enable_bio_integration:
                    self.slo_tracker.record_metric('token_efficiency_slo', 
                        self.expert_ecoatp.get(expert_id, 0) / max(self.expert_usage.get(expert_id, 1), 1))
            
            # Cost attribution
            if self.enable_cost_attribution:
                self.cost_engine.record_cost(expert_id, carbon_kg, helium_units, energy_kwh)
            
            # Correlation tracking
            if correlation_id:
                self.correlation_map[correlation_id].append(f'expert_{expert_id}')
            
            # BIO-INSPIRED: Gradient-modulated threshold checking
            if self.enable_alerting:
                self._check_bio_thresholds(expert_id, execution_time, success)
            
            # Health score update
            self._update_health_score(expert_id)
            
            # Predictive analytics
            if self.enable_predictive:
                self._update_predictions(expert_id)
    
    def _process_anomaly(self, anomaly: AnomalyEvent):
        """Process detected anomaly with bio-inspired context"""
        logger.warning(f"Anomaly detected: {anomaly.metric_name} - {anomaly.anomaly_type.value} "
                      f"(severity={anomaly.severity.value}, gradient={anomaly.gradient_level:.2f})")
        
        if anomaly.severity in [MetricSeverity.CRITICAL, MetricSeverity.EMERGENCY]:
            self._create_alert(
                f"anomaly_{anomaly.event_id}", anomaly.metric_name,
                f"Anomaly: {anomaly.anomaly_type.value}. Expected={anomaly.expected_value:.2f}, "
                f"Actual={anomaly.actual_value:.2f}, Gradient={anomaly.gradient_level:.2f}",
                anomaly.severity
            )
    
    def _check_bio_thresholds(self, expert_id: str, execution_time: float, success: bool):
        """Check thresholds with gradient modulation"""
        gradient_mod = self._get_gradient_modulation() if self.enable_bio_integration else 1.0
        
        if 'latency_p95' in self.thresholds:
            p95 = self.get_expert_latency_stats().get(expert_id, {}).get('p95_ms', 0)
            exceeded, severity = self.thresholds['latency_p95'].is_exceeded(p95, gradient_mod)
            if exceeded:
                self._create_alert(f"latency_{expert_id}", 'latency_p95',
                    f"Expert {expert_id} P95 latency {p95:.1f}ms exceeded threshold", severity)
        
        # BIO-INSPIRED: Check token balance
        if 'token_balance' in self.thresholds and self.enable_bio_integration:
            if self.token_manager:
                summary = self.token_manager.get_system_summary()
                balance = summary.get('total_balance', 500)
                exceeded, severity = self.thresholds['token_balance'].is_exceeded(balance, gradient_mod)
                if exceeded:
                    self._create_alert('token_balance', 'token_balance',
                        f"System token balance {balance:.0f} below threshold", severity)
    
    def _create_alert(self, alert_id: str, metric_name: str, message: str, severity: MetricSeverity):
        """Create alert with cooldown management"""
        if alert_id in self.alert_cooldowns:
            last_alert = self.alert_cooldowns[alert_id]
            threshold = self.thresholds.get(metric_name)
            if threshold:
                cooldown = threshold.cooldown_seconds
                if (datetime.utcnow() - last_alert).total_seconds() < cooldown:
                    return
        
        alert = {
            'alert_id': alert_id, 'metric': metric_name, 'message': message,
            'severity': severity.value, 'timestamp': datetime.utcnow().isoformat(),
            'acknowledged': False,
            'gradient_level': self._get_gradient_modulation() if self.enable_bio_integration else 1.0
        }
        
        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)
        self.alert_cooldowns[alert_id] = datetime.utcnow()
        
        log_level = logging.CRITICAL if severity == MetricSeverity.CRITICAL else logging.WARNING
        logger.log(log_level, f"ALERT: {message}")
    
    def _update_health_score(self, expert_id: str):
        """Update expert health score with bio-inspired factors"""
        success_rate = self.get_expert_success_rate().get(expert_id, 0.5)
        latency_stats = self.get_expert_latency_stats().get(expert_id, {})
        p95 = latency_stats.get('p95_ms', 100)
        latency_score = 1.0 / (1.0 + p95 / 100)
        
        total_carbon = self.expert_carbon.get(expert_id, 0)
        total_usage = max(self.expert_usage.get(expert_id, 1), 1)
        carbon_score = 1.0 / (1.0 + total_carbon / total_usage * 10000)
        
        # BIO-INSPIRED: Token efficiency factor
        token_score = 0.5
        if self.enable_bio_integration:
            ecoatp = self.expert_ecoatp.get(expert_id, 0)
            token_score = 1.0 / (1.0 + ecoatp / max(total_usage, 1) / 100)
        
        health = 0.35 * success_rate + 0.25 * latency_score + 0.25 * carbon_score + 0.15 * token_score
        self.health_scores[expert_id] = health
    
    def _update_predictions(self, expert_id: str):
        """Update predictive analytics"""
        latencies = list(self.expert_latency.get(expert_id, []))
        if len(latencies) < 10:
            return
        
        values = [l['value'] if isinstance(l, dict) else l for l in latencies[-50:]]
        x = np.arange(len(values))
        y = np.array(values)
        
        try:
            slope, intercept = np.polyfit(x, y, 1)
            prediction = intercept + slope * (len(values) + 10)
            
            self.predictions[expert_id] = {
                'predicted_latency_ms': max(0, prediction),
                'trend': 'increasing' if slope > 0.01 else 'decreasing' if slope < -0.01 else 'stable',
                'confidence': 0.7 if len(values) > 30 else 0.4,
                'updated_at': datetime.utcnow().isoformat(),
                'token_cost_trend': 'increasing' if slope > 0 else 'stable'
            }
        except Exception:
            pass
    
    # ========================================================================
    # Metric Queries
    # ========================================================================
    
    def get_expert_usage(self) -> Dict[int, float]:
        total_usage = sum(self.expert_usage.values())
        if total_usage == 0:
            return {}
        return {expert: count / total_usage for expert, count in self.expert_usage.items()}
    
    def get_expert_success_rate(self) -> Dict[int, float]:
        rates = {}
        for expert_id in set(list(self.expert_success.keys()) + list(self.expert_failures.keys())):
            successes = self.expert_success.get(expert_id, 0)
            failures = self.expert_failures.get(expert_id, 0)
            total = successes + failures
            rates[expert_id] = successes / total if total > 0 else 0.5
        return rates
    
    def get_expert_latency_stats(self) -> Dict[str, Dict[str, float]]:
        stats = {}
        for expert_id, latencies in self.expert_latency.items():
            values = [l['value'] if isinstance(l, dict) else l for l in latencies]
            if values:
                arr = np.array(values)
                stats[expert_id] = {
                    'avg_ms': float(np.mean(arr)), 'p50_ms': float(np.median(arr)),
                    'p95_ms': float(np.percentile(arr, 95)), 'p99_ms': float(np.percentile(arr, 99)),
                    'min_ms': float(np.min(arr)), 'max_ms': float(np.max(arr)),
                    'std_ms': float(np.std(arr)), 'samples': len(values)
                }
        return stats
    
    def get_resource_consumption(self) -> Dict[str, Dict[str, float]]:
        consumption = {}
        for expert_id in set(list(self.expert_energy.keys()) + list(self.expert_carbon.keys())):
            consumption[expert_id] = {
                'total_energy_kwh': self.expert_energy.get(expert_id, 0.0),
                'total_carbon_kg': self.expert_carbon.get(expert_id, 0.0),
                'total_helium_units': self.expert_helium.get(expert_id, 0.0),
                'total_ecoatp': self.expert_ecoatp.get(expert_id, 0.0),
                'carbon_per_use_kg': self.expert_carbon.get(expert_id, 0.0) / max(self.expert_usage.get(expert_id, 1), 1),
                'ecoatp_per_use': self.expert_ecoatp.get(expert_id, 0.0) / max(self.expert_usage.get(expert_id, 1), 1)
            }
        return consumption
    
    def get_pareto_frontier(self) -> List[Dict]:
        if not self.pareto_points:
            return []
        recent = list(self.pareto_points)[-1000:]
        pareto_optimal = []
        for i, point in enumerate(recent):
            dominated = False
            for j, other in enumerate(recent):
                if i != j:
                    if (other['energy'] <= point['energy'] and other['time'] <= point['time'] and
                        other['helium'] <= point['helium'] and other.get('ecoatp', 0) <= point.get('ecoatp', 0) and
                        (other['energy'] < point['energy'] or other['time'] < point['time'] or
                         other['helium'] < point['helium'] or other.get('ecoatp', 0) < point.get('ecoatp', 0))):
                        dominated = True
                        break
            if not dominated:
                pareto_optimal.append(point)
        return pareto_optimal
    
    # ========================================================================
    # Health and Status
    # ========================================================================
    
    def get_health_scores(self) -> Dict[str, float]:
        return self.health_scores.copy()
    
    def get_alerts(self, acknowledged: Optional[bool] = None,
                   severity: Optional[MetricSeverity] = None, limit: int = 50) -> List[Dict[str, Any]]:
        alerts = list(self.alert_history)
        if acknowledged is not None:
            alerts = [a for a in alerts if a.get('acknowledged') == acknowledged]
        if severity:
            alerts = [a for a in alerts if a.get('severity') == severity.value]
        return alerts[-limit:]
    
    def acknowledge_alert(self, alert_id: str) -> bool:
        if alert_id in self.active_alerts:
            self.active_alerts[alert_id]['acknowledged'] = True
            return True
        return False
    
    def get_predictions(self) -> Dict[str, Dict[str, Any]]:
        return self.predictions.copy()
    
    def get_slo_status(self) -> Dict[str, Dict[str, Any]]:
        if self.slo_tracker:
            return self.slo_tracker.evaluate_slos()
        return {}
    
    # ========================================================================
    # Metrics Summary with Bio-Inspired Data
    # ========================================================================
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary with bio-inspired data"""
        summary = {
            'timestamp': datetime.utcnow().isoformat(),
            'expert_usage': self.get_expert_usage(),
            'success_rates': self.get_expert_success_rate(),
            'latency_stats': self.get_expert_latency_stats(),
            'resource_consumption': self.get_resource_consumption(),
            'pareto_frontier_size': len(self.get_pareto_frontier()),
            'total_routes': len(self.routing_decisions),
            'avg_routing_latency_ms': np.mean(list(self.routing_latency)) if self.routing_latency else 0,
            'health_scores': self.get_health_scores(),
            'active_alerts': len([a for a in self.active_alerts.values() if not a.get('acknowledged')]),
            'total_alerts': len(self.alert_history),
            'bio_integration_active': self.enable_bio_integration,
            'bio_modules_available': BIO_INSPIRED_AVAILABLE
        }
        
        if self.slo_tracker:
            summary['slo_status'] = self.slo_tracker.evaluate_slos()
        
        if self.anomaly_detector:
            summary['anomaly_stats'] = self.anomaly_detector.get_detection_stats()
        
        if self.enable_predictive:
            summary['predictions'] = self.get_predictions()
        
        # BIO-INSPIRED: Add bio metrics
        if self.enable_bio_integration:
            summary['bio_metrics'] = self._get_bio_metrics()
            summary['gradient_modulation'] = self._get_gradient_modulation()
        
        return summary
    
    def get_expert_performance_report(self, expert_id: str) -> Dict[str, Any]:
        """Get detailed performance report with bio-inspired metrics"""
        latency_stats = self.get_expert_latency_stats().get(expert_id, {})
        success_rate = self.get_expert_success_rate().get(expert_id, 0)
        health = self.health_scores.get(expert_id, 0)
        usage = self.get_expert_usage().get(expert_id, 0)
        
        return {
            'expert_id': expert_id,
            'usage_rate': usage, 'success_rate': success_rate, 'health_score': health,
            'latency': latency_stats,
            'cost_attribution': {
                'carbon_kg': self.expert_carbon.get(expert_id, 0),
                'helium_units': self.expert_helium.get(expert_id, 0),
                'energy_kwh': self.expert_energy.get(expert_id, 0),
                'ecoatp': self.expert_ecoatp.get(expert_id, 0)
            },
            'predictions': self.predictions.get(expert_id, {}),
            'total_executions': self.expert_usage.get(expert_id, 0)
        }
    
    # ========================================================================
    # Enhanced Export Methods with Bio-Inspired Metrics
    # ========================================================================
    
    def to_prometheus_format(self) -> str:
        """Export metrics in Prometheus format with bio-inspired data"""
        lines = []
        timestamp_ms = int(time.time() * 1000)
        
        # Expert usage
        for expert_id, usage in self.get_expert_usage().items():
            lines.append(f'moe_expert_usage{{expert="{expert_id}"}} {usage} {timestamp_ms}')
        
        # Success rates
        for expert_id, rate in self.get_expert_success_rate().items():
            lines.append(f'moe_expert_success_rate{{expert="{expert_id}"}} {rate} {timestamp_ms}')
        
        # Latency stats
        for expert_id, stats in self.get_expert_latency_stats().items():
            lines.append(f'moe_expert_latency_avg{{expert="{expert_id}"}} {stats["avg_ms"]} {timestamp_ms}')
            lines.append(f'moe_expert_latency_p95{{expert="{expert_id}"}} {stats["p95_ms"]} {timestamp_ms}')
        
        # Resource consumption
        for expert_id, resources in self.get_resource_consumption().items():
            lines.append(f'moe_expert_energy_kwh{{expert="{expert_id}"}} {resources["total_energy_kwh"]} {timestamp_ms}')
            lines.append(f'moe_expert_carbon_kg{{expert="{expert_id}"}} {resources["total_carbon_kg"]} {timestamp_ms}')
            lines.append(f'moe_expert_helium_units{{expert="{expert_id}"}} {resources["total_helium_units"]} {timestamp_ms}')
            lines.append(f'moe_expert_ecoatp{{expert="{expert_id}"}} {resources["total_ecoatp"]} {timestamp_ms}')
        
        # Health scores
        for expert_id, health in self.get_health_scores().items():
            lines.append(f'moe_expert_health{{expert="{expert_id}"}} {health} {timestamp_ms}')
        
        # Routing metrics
        lines.append(f'moe_routing_total {len(self.routing_decisions)} {timestamp_ms}')
        if self.routing_latency:
            lines.append(f'moe_routing_latency_avg {np.mean(list(self.routing_latency))} {timestamp_ms}')
        
        # BIO-INSPIRED: Gradient metrics
        if self.enable_bio_integration and self.gradient_manager:
            for field_id, field in self.gradient_manager.fields.items():
                lines.append(f'green_agent_gradient{{field="{field_id}"}} {field.gradient_strength} {timestamp_ms}')
                lines.append(f'green_agent_gradient_value{{field="{field_id}"}} {field.current_value} {timestamp_ms}')
        
        # BIO-INSPIRED: Token economy metrics
        if self.enable_bio_integration and self.token_manager:
            summary = self.token_manager.get_system_summary()
            lines.append(f'green_agent_ecoatp_balance {summary.get("total_balance", 0)} {timestamp_ms}')
            lines.append(f'green_agent_ecoatp_generated {summary.get("total_generated", 0)} {timestamp_ms}')
            lines.append(f'green_agent_ecoatp_consumed {summary.get("total_consumed", 0)} {timestamp_ms}')
            lines.append(f'green_agent_ecoatp_efficiency {summary.get("system_efficiency", 0)} {timestamp_ms}')
        
        # BIO-INSPIRED: Biomass metrics
        if self.enable_bio_integration and self.biomass_storage:
            stats = self.biomass_storage.get_storage_stats()
            lines.append(f'green_agent_biomass_total {stats.get("total_stored", 0)} {timestamp_ms}')
            tiers = stats.get('tiers', {})
            for tier, count in tiers.items():
                lines.append(f'green_agent_biomass_tier{{tier="{tier}"}} {count} {timestamp_ms}')
        
        # BIO-INSPIRED: Harvester metrics
        if self.enable_bio_integration and self.harvester:
            harvester_stats = self.harvester.get_harvesting_stats()
            lines.append(f'green_agent_harvester_total {harvester_stats.get("total_harvested", 0)} {timestamp_ms}')
        
        # BIO-INSPIRED: Compartment metrics
        if self.enable_bio_integration and self.compartment_manager:
            viable = sum(1 for c in self.compartment_manager.compartments.values() if c.is_viable)
            total = len(self.compartment_manager.compartments)
            lines.append(f'green_agent_compartments_viable {viable} {timestamp_ms}')
            lines.append(f'green_agent_compartments_total {total} {timestamp_ms}')
        
        # Alert count
        active_alerts = len([a for a in self.active_alerts.values() if not a.get('acknowledged')])
        lines.append(f'moe_active_alerts {active_alerts} {timestamp_ms}')
        
        return '\n'.join(lines)
    
    def to_json_format(self) -> str:
        """Export metrics in JSON format with bio-inspired data"""
        return json.dumps(self.get_metrics_summary(), indent=2, default=str)
    
    def to_grafana_format(self) -> List[Dict[str, Any]]:
        """Export metrics in Grafana-compatible format with bio-inspired panels"""
        panels = []
        timestamp_ms = int(time.time() * 1000)
        
        # Expert Usage Panel
        usage_data = []
        for expert_id, usage in self.get_expert_usage().items():
            usage_data.append({'target': f'expert_{expert_id}', 'datapoints': [[usage, timestamp_ms]]})
        panels.append({'title': 'Expert Usage', 'type': 'piechart', 'data': usage_data})
        
        # Expert Latency Panel
        latency_data = []
        for expert_id, stats in self.get_expert_latency_stats().items():
            latency_data.append({
                'target': f'expert_{expert_id}',
                'datapoints': [[stats['p50_ms'], timestamp_ms], [stats['p95_ms'], timestamp_ms]]
            })
        panels.append({'title': 'Expert Latency', 'type': 'graph', 'data': latency_data})
        
        # BIO-INSPIRED: Gradient Panel
        if self.enable_bio_integration and self.gradient_manager:
            gradient_data = []
            for field_id, field in self.gradient_manager.fields.items():
                gradient_data.append({
                    'target': f'gradient_{field_id}',
                    'datapoints': [[field.gradient_strength, timestamp_ms]]
                })
            panels.append({'title': 'Gradient Fields', 'type': 'gauge', 'data': gradient_data})
        
        # BIO-INSPIRED: Token Economy Panel
        if self.enable_bio_integration and self.token_manager:
            summary = self.token_manager.get_system_summary()
            token_data = [
                {'target': 'balance', 'datapoints': [[summary.get('total_balance', 0), timestamp_ms]]},
                {'target': 'generated', 'datapoints': [[summary.get('total_generated', 0), timestamp_ms]]},
                {'target': 'consumed', 'datapoints': [[summary.get('total_consumed', 0), timestamp_ms]]}
            ]
            panels.append({'title': 'Token Economy', 'type': 'graph', 'data': token_data})
        
        # BIO-INSPIRED: Biomass Panel
        if self.enable_bio_integration and self.biomass_storage:
            stats = self.biomass_storage.get_storage_stats()
            biomass_data = []
            for tier, count in stats.get('tiers', {}).items():
                biomass_data.append({'target': tier, 'datapoints': [[count, timestamp_ms]]})
            panels.append({'title': 'Biomass Storage', 'type': 'bargauge', 'data': biomass_data})
        
        return panels
    
    # ========================================================================
    # Maintenance Methods
    # ========================================================================
    
    def reset_metrics(self):
        """Reset all metrics"""
        with self._lock:
            self.expert_usage.clear()
            self.expert_success.clear()
            self.expert_failures.clear()
            self.expert_latency.clear()
            self.routing_decisions.clear()
            self.routing_latency.clear()
            self.expert_energy.clear()
            self.expert_carbon.clear()
            self.expert_helium.clear()
            self.expert_ecoatp.clear()
            self.pareto_points.clear()
            self.health_scores.clear()
            self.predictions.clear()
            self.bio_metrics_history.clear()
            logger.info("All metrics reset")
    
    def cleanup_old_data(self, max_age_hours: float = 24.0):
        """Clean up data older than specified age"""
        cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
        with self._lock:
            while self.routing_decisions and self.routing_decisions[0]['timestamp'] < cutoff:
                self.routing_decisions.popleft()
            while self.pareto_points and self.pareto_points[0]['timestamp'] < cutoff:
                self.pareto_points.popleft()
            while self.alert_history and datetime.fromisoformat(self.alert_history[0]['timestamp']) < cutoff:
                self.alert_history.popleft()
        logger.info(f"Cleaned up metrics older than {max_age_hours}h")


# ============================================================================
# Legacy Compatibility Classes (preserved)
# ============================================================================

class SLOTracker:
    """Service Level Objective tracking (preserved from original)"""
    def __init__(self):
        self.slos: Dict[str, ServiceLevelObjective] = {}
        self.slo_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
    
    def define_slo(self, slo_id: str, metric_name: str, target_value: float,
                   target_percentile: float = 99.0, evaluation_window_hours: float = 24.0):
        self.slos[slo_id] = ServiceLevelObjective(
            slo_id=slo_id, metric_name=metric_name, target_value=target_value,
            target_percentile=target_percentile, evaluation_window_hours=evaluation_window_hours
        )
    
    def record_metric(self, slo_id: str, value: float):
        if slo_id in self.slos:
            self.slo_history[slo_id].append({'value': value, 'timestamp': datetime.utcnow()})
    
    def evaluate_slos(self) -> Dict[str, Dict[str, Any]]:
        results = {}
        for slo_id, slo in self.slos.items():
            history = list(self.slo_history[slo_id])
            if len(history) < slo.min_samples:
                slo.status = SLOStatus.UNKNOWN
                results[slo_id] = {'status': 'unknown', 'reason': 'insufficient_data'}
                continue
            
            cutoff = datetime.utcnow() - timedelta(hours=slo.evaluation_window_hours)
            recent = [h for h in history if h['timestamp'] > cutoff]
            
            if len(recent) < slo.min_samples:
                slo.status = SLOStatus.UNKNOWN
                results[slo_id] = {'status': 'unknown', 'reason': 'insufficient_recent_data'}
                continue
            
            values = [h['value'] for h in recent]
            actual_percentile = np.percentile(values, slo.target_percentile)
            slo.current_value = actual_percentile
            
            if actual_percentile <= slo.target_value:
                slo.status = SLOStatus.COMPLIANT
            else:
                compliant_count = sum(1 for v in values if v <= slo.target_value)
                slo.error_budget_remaining = max(0, compliant_count / len(recent))
                slo.status = SLOStatus.BREACHED if slo.error_budget_remaining < 0.5 else SLOStatus.AT_RISK
            
            slo.last_evaluated = datetime.utcnow()
            results[slo_id] = {
                'slo_id': slo_id, 'metric': slo.metric_name, 'target': slo.target_value,
                'actual_percentile': actual_percentile, 'status': slo.status.value,
                'error_budget_remaining': slo.error_budget_remaining
            }
        return results


class CostAttributionEngine:
    """Cost attribution engine (preserved from original)"""
    def __init__(self):
        self.cost_records: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
    
    def record_cost(self, expert_id: str, carbon_kg: float, helium_units: float, energy_kwh: float):
        self.cost_records[expert_id].append({
            'carbon_kg': carbon_kg, 'helium_units': helium_units,
            'energy_kwh': energy_kwh, 'timestamp': datetime.utcnow()
        })
    
    def get_expert_cost_attribution(self, expert_id: str, time_period_hours: float = 24.0) -> CostAttribution:
        records = list(self.cost_records[expert_id])
        if not records:
            return CostAttribution(expert_id=expert_id, time_period=f"{time_period_hours}h")
        
        cutoff = datetime.utcnow() - timedelta(hours=time_period_hours)
        recent = [r for r in records if r['timestamp'] > cutoff]
        if not recent:
            recent = records[-100:]
        
        total_carbon = sum(r['carbon_kg'] for r in recent)
        total_helium = sum(r['helium_units'] for r in recent)
        total_energy = sum(r['energy_kwh'] for r in recent)
        
        return CostAttribution(
            expert_id=expert_id, time_period=f"{time_period_hours}h",
            total_carbon_kg=total_carbon, total_helium_units=total_helium,
            total_energy_kwh=total_energy,
            carbon_efficiency_score=1.0 / (1.0 + total_carbon * 100),
            helium_efficiency_score=1.0 / (1.0 + total_helium * 10)
        )
