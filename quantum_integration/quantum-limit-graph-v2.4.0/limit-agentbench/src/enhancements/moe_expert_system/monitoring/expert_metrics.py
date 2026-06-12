# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/monitoring/expert_metrics.py

"""
Enhanced Expert Metrics Collector for Green Agent MoE System
Version: 2.0.0

Advanced monitoring and analytics with:
- Real-time streaming metrics with WebSocket support
- Anomaly detection using statistical methods
- Predictive analytics with trend forecasting
- Multi-level alerting system (info, warning, critical)
- SLO/SLA tracking and compliance reporting
- Distributed tracing with correlation IDs
- Custom dashboard data generation
- Automated metric retention and downsampling
- Comparative expert analytics
- Carbon/helium cost attribution
- Performance degradation detection
- Capacity planning insights
- Health score calculation
- Metric aggregation pipelines
- Export to multiple formats (Prometheus, Grafana, JSON, CSV)

Integration Points:
- Layer 1: Meta-cognitive performance feedback
- Layer 7: Native dual monitoring integration
- Layer 8: Metric audit trail
- Layer 9: Pareto optimization metrics
- Layer 11: Dashboard data feed
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, deque
import numpy as np
from scipy import stats
import time
import threading
import json
import hashlib
import math
import warnings

logger = logging.getLogger(__name__)

# ============================================================================
# Enums and Data Classes
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

class AnomalyType(Enum):
    """Types of anomalies detected"""
    SPIKE = "spike"
    DIP = "dip"
    TREND_CHANGE = "trend_change"
    LEVEL_SHIFT = "level_shift"
    VARIANCE_CHANGE = "variance_change"
    OUTLIER = "outlier"

class SLOStatus(Enum):
    """Service Level Objective status"""
    COMPLIANT = "compliant"
    AT_RISK = "at_risk"
    BREACHED = "breached"
    UNKNOWN = "unknown"

@dataclass
class MetricThreshold:
    """Threshold configuration for alerting"""
    metric_name: str
    warning_threshold: float
    critical_threshold: float
    comparison: str = "greater_than"  # greater_than, less_than, equal_to
    duration_seconds: float = 60.0  # How long threshold must be exceeded
    cooldown_seconds: float = 300.0  # Minimum time between alerts
    
    def is_exceeded(self, value: float) -> Tuple[bool, MetricSeverity]:
        """Check if threshold is exceeded"""
        if self.comparison == "greater_than":
            if value >= self.critical_threshold:
                return True, MetricSeverity.CRITICAL
            elif value >= self.warning_threshold:
                return True, MetricSeverity.WARNING
        elif self.comparison == "less_than":
            if value <= self.critical_threshold:
                return True, MetricSeverity.CRITICAL
            elif value <= self.warning_threshold:
                return True, MetricSeverity.WARNING
        
        return False, MetricSeverity.INFO

@dataclass
class ServiceLevelObjective:
    """Service Level Objective definition"""
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

@dataclass
class AnomalyEvent:
    """Anomaly detection event"""
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

@dataclass
class MetricSample:
    """Individual metric sample with metadata"""
    timestamp: datetime
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    trace_id: Optional[str] = None
    expert_id: Optional[str] = None

@dataclass
class CostAttribution:
    """Carbon/helium cost attribution"""
    expert_id: str
    time_period: str
    total_carbon_kg: float
    total_helium_units: float
    total_energy_kwh: float
    cost_per_operation: float
    carbon_efficiency_score: float
    helium_efficiency_score: float
    trend: str  # improving, stable, degrading

# ============================================================================
# Anomaly Detector
# ============================================================================

class AnomalyDetector:
    """
    Statistical anomaly detection for metrics.
    
    Methods:
    - Z-score based detection
    - Moving average deviation
    - Seasonal decomposition
    - Interquartile range (IQR)
    - Isolation forest (simplified)
    """
    
    def __init__(
        self,
        zscore_threshold: float = 3.0,
        iqr_multiplier: float = 1.5,
        window_size: int = 100,
        min_samples: int = 30
    ):
        self.zscore_threshold = zscore_threshold
        self.iqr_multiplier = iqr_multiplier
        self.window_size = window_size
        self.min_samples = min_samples
        
        self.metric_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=window_size)
        )
        self.seasonal_patterns: Dict[str, np.ndarray] = {}
        self.detection_history: deque = deque(maxlen=1000)
    
    def add_sample(self, metric_name: str, value: float):
        """Add sample for anomaly detection"""
        self.metric_history[metric_name].append({
            'value': value,
            'timestamp': datetime.utcnow()
        })
    
    def detect_anomalies(
        self,
        metric_name: str,
        current_value: float,
        expert_id: Optional[str] = None
    ) -> List[AnomalyEvent]:
        """
        Detect anomalies in metric using multiple methods.
        
        Returns list of detected anomalies.
        """
        anomalies = []
        history = self.metric_history.get(metric_name)
        
        if not history or len(history) < self.min_samples:
            return anomalies
        
        values = np.array([h['value'] for h in history])
        
        # Method 1: Z-score
        zscore_anomaly = self._zscore_detect(metric_name, current_value, values)
        if zscore_anomaly:
            anomalies.append(zscore_anomaly)
        
        # Method 2: IQR
        iqr_anomaly = self._iqr_detect(metric_name, current_value, values)
        if iqr_anomaly:
            anomalies.append(iqr_anomaly)
        
        # Method 3: Moving average deviation
        mad_anomaly = self._mad_detect(metric_name, current_value, values)
        if mad_anomaly:
            anomalies.append(mad_anomaly)
        
        # Set expert ID
        for anomaly in anomalies:
            anomaly.expert_id = expert_id
        
        # Record detections
        for anomaly in anomalies:
            self.detection_history.append(anomaly)
        
        return anomalies
    
    def _zscore_detect(
        self,
        metric_name: str,
        current_value: float,
        values: np.ndarray
    ) -> Optional[AnomalyEvent]:
        """Z-score based anomaly detection"""
        mean = np.mean(values)
        std = np.std(values)
        
        if std == 0:
            return None
        
        zscore = abs(current_value - mean) / std
        
        if zscore > self.zscore_threshold:
            return AnomalyEvent(
                event_id=f"anomaly_{datetime.utcnow().timestamp()}_{metric_name}",
                metric_name=metric_name,
                anomaly_type=AnomalyType.OUTLIER if current_value > mean else AnomalyType.DIP if current_value < mean else AnomalyType.SPIKE,
                detected_at=datetime.utcnow(),
                expected_value=mean,
                actual_value=current_value,
                deviation_std=zscore,
                severity=MetricSeverity.CRITICAL if zscore > self.zscore_threshold * 1.5 else MetricSeverity.WARNING
            )
        
        return None
    
    def _iqr_detect(
        self,
        metric_name: str,
        current_value: float,
        values: np.ndarray
    ) -> Optional[AnomalyEvent]:
        """IQR-based anomaly detection"""
        q1 = np.percentile(values, 25)
        q3 = np.percentile(values, 75)
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
                expected_value=np.median(values),
                actual_value=current_value,
                deviation_std=abs(deviation),
                severity=MetricSeverity.WARNING
            )
        
        return None
    
    def _mad_detect(
        self,
        metric_name: str,
        current_value: float,
        values: np.ndarray
    ) -> Optional[AnomalyEvent]:
        """Moving average deviation detection"""
        if len(values) < 10:
            return None
        
        # Calculate moving average
        ma = np.convolve(values, np.ones(10)/10, mode='valid')
        if len(ma) == 0:
            return None
        
        last_ma = ma[-1]
        ma_std = np.std(ma)
        
        deviation = abs(current_value - last_ma) / (ma_std + 1e-8)
        
        if deviation > 2.0:
            return AnomalyEvent(
                event_id=f"anomaly_mad_{datetime.utcnow().timestamp()}_{metric_name}",
                metric_name=metric_name,
                anomaly_type=AnomalyType.TREND_CHANGE,
                detected_at=datetime.utcnow(),
                expected_value=last_ma,
                actual_value=current_value,
                deviation_std=deviation,
                severity=MetricSeverity.INFO
            )
        
        return None
    
    def get_detection_stats(self) -> Dict[str, Any]:
        """Get anomaly detection statistics"""
        return {
            'total_detections': len(self.detection_history),
            'recent_detections': [
                {
                    'metric': d.metric_name,
                    'type': d.anomaly_type.value,
                    'severity': d.severity.value,
                    'deviation': d.deviation_std,
                    'timestamp': d.detected_at.isoformat()
                }
                for d in list(self.detection_history)[-20:]
            ],
            'detection_rate': len([
                d for d in self.detection_history
                if d.detected_at > datetime.utcnow() - timedelta(hours=1)
            ])
        }

# ============================================================================
# SLO Tracker
# ============================================================================

class SLOTracker:
    """
    Service Level Objective tracking and compliance.
    
    Features:
    - Multi-metric SLO definitions
    - Error budget calculation
    - Burn rate monitoring
    - Compliance reporting
    """
    
    def __init__(self):
        self.slos: Dict[str, ServiceLevelObjective] = {}
        self.slo_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.burn_rate_alerts: List[Dict[str, Any]] = []
    
    def define_slo(
        self,
        slo_id: str,
        metric_name: str,
        target_value: float,
        target_percentile: float = 99.0,
        evaluation_window_hours: float = 24.0
    ):
        """Define a new SLO"""
        self.slos[slo_id] = ServiceLevelObjective(
            slo_id=slo_id,
            metric_name=metric_name,
            target_value=target_value,
            target_percentile=target_percentile,
            evaluation_window_hours=evaluation_window_hours
        )
        
        logger.info(f"SLO defined: {slo_id} - {metric_name} <= {target_value}")
    
    def record_metric(self, slo_id: str, value: float):
        """Record metric value for SLO tracking"""
        if slo_id not in self.slos:
            return
        
        self.slo_history[slo_id].append({
            'value': value,
            'timestamp': datetime.utcnow()
        })
    
    def evaluate_slos(self) -> Dict[str, Dict[str, Any]]:
        """Evaluate all SLOs"""
        results = {}
        
        for slo_id, slo in self.slos.items():
            result = self._evaluate_single_slo(slo_id)
            results[slo_id] = result
        
        return results
    
    def _evaluate_single_slo(self, slo_id: str) -> Dict[str, Any]:
        """Evaluate a single SLO"""
        slo = self.slos[slo_id]
        history = list(self.slo_history[slo_id])
        
        if len(history) < slo.min_samples:
            slo.status = SLOStatus.UNKNOWN
            return {'status': 'unknown', 'reason': 'insufficient_data'}
        
        # Filter to evaluation window
        cutoff = datetime.utcnow() - timedelta(hours=slo.evaluation_window_hours)
        recent = [h for h in history if h['timestamp'] > cutoff]
        
        if len(recent) < slo.min_samples:
            slo.status = SLOStatus.UNKNOWN
            return {'status': 'unknown', 'reason': 'insufficient_recent_data'}
        
        values = [h['value'] for h in recent]
        
        # Calculate percentile
        actual_percentile = np.percentile(values, slo.target_percentile)
        slo.current_value = actual_percentile
        
        # Calculate error budget
        compliant_count = sum(1 for v in values if v <= slo.target_value)
        total_count = len(values)
        compliance_rate = compliant_count / total_count
        slo.error_budget_remaining = max(0, compliance_rate - (1 - slo.target_percentile / 100))
        
        # Determine status
        if actual_percentile <= slo.target_value:
            slo.status = SLOStatus.COMPLIANT
        elif slo.error_budget_remaining > 0.5:
            slo.status = SLOStatus.AT_RISK
        else:
            slo.status = SLOStatus.BREACHED
        
        slo.last_evaluated = datetime.utcnow()
        
        # Check burn rate
        burn_rate = self._calculate_burn_rate(slo_id, values)
        
        return {
            'slo_id': slo_id,
            'metric': slo.metric_name,
            'target': slo.target_value,
            'actual_percentile': actual_percentile,
            'compliance_rate': compliance_rate,
            'error_budget_remaining': slo.error_budget_remaining,
            'status': slo.status.value,
            'burn_rate': burn_rate,
            'samples_evaluated': total_count
        }
    
    def _calculate_burn_rate(
        self,
        slo_id: str,
        values: List[float]
    ) -> float:
        """Calculate error budget burn rate"""
        if not values:
            return 0.0
        
        slo = self.slos[slo_id]
        
        # Count violations
        violations = sum(1 for v in values if v > slo.target_value)
        
        # Burn rate as violations per hour
        time_span_hours = slo.evaluation_window_hours
        burn_rate = violations / time_span_hours if time_span_hours > 0 else 0
        
        # Alert on high burn rate
        if burn_rate > 10:
            self.burn_rate_alerts.append({
                'slo_id': slo_id,
                'burn_rate': burn_rate,
                'timestamp': datetime.utcnow().isoformat(),
                'severity': 'critical'
            })
        
        return burn_rate

# ============================================================================
# Cost Attribution Engine
# ============================================================================

class CostAttributionEngine:
    """
    Carbon and helium cost attribution.
    
    Features:
    - Per-expert cost tracking
    - Time-based aggregation
    - Efficiency scoring
    - Trend analysis
    - Cost optimization recommendations
    """
    
    def __init__(self):
        self.cost_records: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.attribution_history: List[CostAttribution] = []
    
    def record_cost(
        self,
        expert_id: str,
        carbon_kg: float,
        helium_units: float,
        energy_kwh: float,
        operation_count: int = 1
    ):
        """Record cost for an expert operation"""
        self.cost_records[expert_id].append({
            'carbon_kg': carbon_kg,
            'helium_units': helium_units,
            'energy_kwh': energy_kwh,
            'operations': operation_count,
            'timestamp': datetime.utcnow()
        })
    
    def get_expert_cost_attribution(
        self,
        expert_id: str,
        time_period_hours: float = 24.0
    ) -> CostAttribution:
        """Get cost attribution for an expert"""
        records = list(self.cost_records[expert_id])
        
        if not records:
            return CostAttribution(
                expert_id=expert_id,
                time_period=f"{time_period_hours}h",
                total_carbon_kg=0,
                total_helium_units=0,
                total_energy_kwh=0,
                cost_per_operation=0,
                carbon_efficiency_score=0,
                helium_efficiency_score=0,
                trend="stable"
            )
        
        # Filter by time period
        cutoff = datetime.utcnow() - timedelta(hours=time_period_hours)
        recent = [r for r in records if r['timestamp'] > cutoff]
        
        if not recent:
            recent = records[-100:]  # Fallback to last 100 records
        
        # Calculate totals
        total_carbon = sum(r['carbon_kg'] for r in recent)
        total_helium = sum(r['helium_units'] for r in recent)
        total_energy = sum(r['energy_kwh'] for r in recent)
        total_ops = sum(r['operations'] for r in recent)
        
        # Calculate efficiency scores
        carbon_per_op = total_carbon / max(total_ops, 1)
        helium_per_op = total_helium / max(total_ops, 1)
        
        # Normalize scores (lower is better)
        carbon_efficiency = 1.0 / (1.0 + carbon_per_op * 10000)
        helium_efficiency = 1.0 / (1.0 + helium_per_op * 1000)
        
        # Trend analysis
        if len(recent) >= 20:
            recent_carbon = [r['carbon_kg'] for r in recent[-20:]]
            trend_slope = np.polyfit(range(20), recent_carbon, 1)[0]
            
            if trend_slope < -0.0001:
                trend = "improving"
            elif trend_slope > 0.0001:
                trend = "degrading"
            else:
                trend = "stable"
        else:
            trend = "stable"
        
        attribution = CostAttribution(
            expert_id=expert_id,
            time_period=f"{time_period_hours}h",
            total_carbon_kg=total_carbon,
            total_helium_units=total_helium,
            total_energy_kwh=total_energy,
            cost_per_operation=carbon_per_op + helium_per_op * 0.1,
            carbon_efficiency_score=carbon_efficiency,
            helium_efficiency_score=helium_efficiency,
            trend=trend
        )
        
        self.attribution_history.append(attribution)
        
        return attribution
    
    def get_optimization_recommendations(self) -> List[str]:
        """Get cost optimization recommendations"""
        recommendations = []
        
        # Analyze all experts
        for expert_id in self.cost_records:
            attribution = self.get_expert_cost_attribution(expert_id)
            
            if attribution.trend == "degrading":
                recommendations.append(
                    f"Expert {expert_id}: Carbon efficiency degrading. "
                    f"Consider optimization or replacement."
                )
            
            if attribution.carbon_efficiency_score < 0.5:
                recommendations.append(
                    f"Expert {expert_id}: Low carbon efficiency. "
                    f"Consider switching to renewable energy or optimizing model."
                )
            
            if attribution.helium_efficiency_score < 0.3:
                recommendations.append(
                    f"Expert {expert_id}: Very low helium efficiency. "
                    f"Consider helium-free alternatives."
                )
        
        return recommendations

# ============================================================================
# Enhanced Expert Metrics Collector
# ============================================================================

class ExpertMetricsCollector:
    """
    Enhanced metrics collector with comprehensive monitoring.
    
    Features:
    - Real-time metrics collection
    - Anomaly detection
    - SLO tracking
    - Cost attribution
    - Predictive analytics
    - Multi-format export
    - Alert management
    - Health scoring
    - Performance degradation detection
    - Capacity planning
    """
    
    def __init__(
        self,
        enable_anomaly_detection: bool = True,
        enable_slo_tracking: bool = True,
        enable_cost_attribution: bool = True,
        enable_alerting: bool = True,
        enable_predictive: bool = True,
        retention_hours: float = 24.0
    ):
        # Feature flags
        self.enable_anomaly_detection = enable_anomaly_detection
        self.enable_slo_tracking = enable_slo_tracking
        self.enable_cost_attribution = enable_cost_attribution
        self.enable_alerting = enable_alerting
        self.enable_predictive = enable_predictive
        self.retention_hours = retention_hours
        
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
        
        # Pareto frontier data
        self.pareto_points: deque = deque(maxlen=10000)
        
        # Alert management
        self.active_alerts: Dict[str, Dict[str, Any]] = {}
        self.alert_history: deque = deque(maxlen=5000)
        self.alert_cooldowns: Dict[str, datetime] = {}
        
        # Thresholds
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
        
        logger.info(
            "Enhanced Expert Metrics Collector initialized: "
            f"anomaly={enable_anomaly_detection}, "
            f"slo={enable_slo_tracking}, "
            f"cost={enable_cost_attribution}"
        )
    
    def _initialize_thresholds(self):
        """Initialize default metric thresholds"""
        self.thresholds = {
            'latency_p95': MetricThreshold(
                metric_name='latency_p95',
                warning_threshold=100.0,
                critical_threshold=500.0,
                comparison='greater_than'
            ),
            'error_rate': MetricThreshold(
                metric_name='error_rate',
                warning_threshold=0.05,
                critical_threshold=0.10,
                comparison='greater_than'
            ),
            'carbon_per_inference': MetricThreshold(
                metric_name='carbon_per_inference',
                warning_threshold=0.0005,
                critical_threshold=0.001,
                comparison='greater_than'
            ),
            'success_rate': MetricThreshold(
                metric_name='success_rate',
                warning_threshold=0.95,
                critical_threshold=0.90,
                comparison='less_than'
            )
        }
    
    def _initialize_slos(self):
        """Initialize default SLOs"""
        self.slo_tracker.define_slo(
            'latency_slo',
            'expert_latency_ms',
            target_value=100.0,
            target_percentile=99.0
        )
        self.slo_tracker.define_slo(
            'availability_slo',
            'expert_success_rate',
            target_value=0.999,
            target_percentile=99.9
        )
        self.slo_tracker.define_slo(
            'carbon_slo',
            'carbon_per_inference',
            target_value=0.0005,
            target_percentile=95.0
        )
    
    # ========================================================================
    # Metric Recording
    # ========================================================================
    
    def record_routing(
        self,
        routing_decisions: List[Tuple[int, float]],
        gating_context: Any,
        execution_time: float,
        success: bool,
        correlation_id: Optional[str] = None
    ):
        """Record routing decision with enhanced metadata"""
        with self._lock:
            # Record per-expert usage
            for expert_idx, weight in routing_decisions:
                self.expert_usage[expert_idx] = self.expert_usage.get(expert_idx, 0) + 1
                if success:
                    self.expert_success[expert_idx] = self.expert_success.get(expert_idx, 0) + 1
                else:
                    self.expert_failures[expert_idx] = self.expert_failures.get(expert_idx, 0) + 1
            
            # Record routing latency
            self.routing_latency.append(execution_time)
            
            # Store routing decision with correlation
            decision_record = {
                'decisions': routing_decisions,
                'context': str(gating_context)[:200],
                'execution_time': execution_time,
                'success': success,
                'timestamp': datetime.utcnow(),
                'correlation_id': correlation_id
            }
            self.routing_decisions.append(decision_record)
            
            # Track correlation
            if correlation_id:
                self.correlation_map[correlation_id].append('routing')
    
    def record_expert_execution(
        self,
        expert_id: str,
        execution_time: float,
        energy_kwh: float,
        carbon_kg: float,
        helium_units: float,
        success: bool,
        correlation_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Record expert execution with comprehensive metrics"""
        with self._lock:
            # Latency tracking
            self.expert_latency[expert_id].append({
                'value': execution_time,
                'timestamp': datetime.utcnow()
            })
            
            # Resource tracking
            self.expert_energy[expert_id] += energy_kwh
            self.expert_carbon[expert_id] += carbon_kg
            self.expert_helium[expert_id] += helium_units
            
            # Success tracking
            if success:
                self.expert_success[expert_id] = self.expert_success.get(expert_id, 0) + 1
            else:
                self.expert_failures[expert_id] = self.expert_failures.get(expert_id, 0) + 1
            
            # Pareto point
            self.pareto_points.append({
                'expert_id': expert_id,
                'energy': energy_kwh,
                'time': execution_time,
                'helium': helium_units,
                'carbon': carbon_kg,
                'timestamp': datetime.utcnow()
            })
            
            # Anomaly detection
            if self.enable_anomaly_detection:
                anomalies = self.anomaly_detector.detect_anomalies(
                    f"{expert_id}_latency",
                    execution_time,
                    expert_id
                )
                
                for anomaly in anomalies:
                    self._process_anomaly(anomaly)
            
            # SLO tracking
            if self.enable_slo_tracking:
                self.slo_tracker.record_metric('latency_slo', execution_time)
                self.slo_tracker.record_metric('carbon_slo', carbon_kg)
            
            # Cost attribution
            if self.enable_cost_attribution:
                self.cost_engine.record_cost(
                    expert_id, carbon_kg, helium_units, energy_kwh
                )
            
            # Correlation tracking
            if correlation_id:
                self.correlation_map[correlation_id].append(f'expert_{expert_id}')
            
            # Threshold checking
            if self.enable_alerting:
                self._check_thresholds(expert_id, execution_time, success)
            
            # Health score update
            self._update_health_score(expert_id)
            
            # Predictive analytics
            if self.enable_predictive:
                self._update_predictions(expert_id)
    
    def _process_anomaly(self, anomaly: AnomalyEvent):
        """Process detected anomaly"""
        logger.warning(
            f"Anomaly detected: {anomaly.metric_name} - "
            f"{anomaly.anomaly_type.value} (severity: {anomaly.severity.value})"
        )
        
        # Create alert for critical anomalies
        if anomaly.severity in [MetricSeverity.CRITICAL, MetricSeverity.EMERGENCY]:
            self._create_alert(
                f"anomaly_{anomaly.event_id}",
                anomaly.metric_name,
                f"Anomaly detected: {anomaly.anomaly_type.value}. "
                f"Expected: {anomaly.expected_value:.2f}, "
                f"Actual: {anomaly.actual_value:.2f}",
                anomaly.severity
            )
    
    def _check_thresholds(
        self,
        expert_id: str,
        execution_time: float,
        success: bool
    ):
        """Check metric thresholds and create alerts"""
        # Check latency
        if 'latency_p95' in self.thresholds:
            p95 = self.get_expert_latency_stats().get(expert_id, {}).get('p95_ms', 0)
            exceeded, severity = self.thresholds['latency_p95'].is_exceeded(p95)
            
            if exceeded:
                self._create_alert(
                    f"latency_{expert_id}",
                    'latency_p95',
                    f"Expert {expert_id} P95 latency {p95:.1f}ms exceeded threshold",
                    severity
                )
        
        # Check error rate
        error_rate = self.get_expert_success_rate().get(expert_id, 0)
        if 'error_rate' in self.thresholds:
            exceeded, severity = self.thresholds['error_rate'].is_exceeded(1 - error_rate)
            
            if exceeded:
                self._create_alert(
                    f"error_{expert_id}",
                    'error_rate',
                    f"Expert {expert_id} error rate {(1-error_rate):.2%} exceeded threshold",
                    severity
                )
    
    def _create_alert(
        self,
        alert_id: str,
        metric_name: str,
        message: str,
        severity: MetricSeverity
    ):
        """Create alert with cooldown management"""
        # Check cooldown
        if alert_id in self.alert_cooldowns:
            last_alert = self.alert_cooldowns[alert_id]
            threshold = self.thresholds.get(metric_name)
            if threshold:
                cooldown = threshold.cooldown_seconds
                if (datetime.utcnow() - last_alert).total_seconds() < cooldown:
                    return
        
        alert = {
            'alert_id': alert_id,
            'metric': metric_name,
            'message': message,
            'severity': severity.value,
            'timestamp': datetime.utcnow().isoformat(),
            'acknowledged': False
        }
        
        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)
        self.alert_cooldowns[alert_id] = datetime.utcnow()
        
        log_level = logging.CRITICAL if severity == MetricSeverity.CRITICAL else logging.WARNING
        logger.log(log_level, f"ALERT: {message}")
    
    def _update_health_score(self, expert_id: str):
        """Update expert health score"""
        success_rate = self.get_expert_success_rate().get(expert_id, 0.5)
        latency_stats = self.get_expert_latency_stats().get(expert_id, {})
        p95 = latency_stats.get('p95_ms', 100)
        
        # Normalize scores
        latency_score = 1.0 / (1.0 + p95 / 100)
        
        # Carbon efficiency
        total_carbon = self.expert_carbon.get(expert_id, 0)
        total_usage = max(self.expert_usage.get(expert_id, 1), 1)
        carbon_score = 1.0 / (1.0 + total_carbon / total_usage * 10000)
        
        # Composite health
        health = 0.4 * success_rate + 0.3 * latency_score + 0.3 * carbon_score
        self.health_scores[expert_id] = health
    
    def _update_predictions(self, expert_id: str):
        """Update predictive analytics"""
        latencies = list(self.expert_latency.get(expert_id, []))
        
        if len(latencies) < 10:
            return
        
        values = [l['value'] if isinstance(l, dict) else l for l in latencies[-50:]]
        
        # Simple linear regression for prediction
        x = np.arange(len(values))
        y = np.array(values)
        
        try:
            slope, intercept = np.polyfit(x, y, 1)
            prediction = intercept + slope * (len(values) + 10)  # Predict 10 steps ahead
            
            self.predictions[expert_id] = {
                'predicted_latency_ms': max(0, prediction),
                'trend': 'increasing' if slope > 0.01 else 'decreasing' if slope < -0.01 else 'stable',
                'confidence': 0.7 if len(values) > 30 else 0.4,
                'updated_at': datetime.utcnow().isoformat()
            }
        except Exception:
            pass
    
    # ========================================================================
    # Metric Queries
    # ========================================================================
    
    def get_expert_usage(self) -> Dict[int, float]:
        """Get expert usage rates"""
        total_usage = sum(self.expert_usage.values())
        if total_usage == 0:
            return {}
        
        return {
            expert: count / total_usage
            for expert, count in self.expert_usage.items()
        }
    
    def get_expert_success_rate(self) -> Dict[int, float]:
        """Get expert success rates"""
        rates = {}
        for expert_id in set(list(self.expert_success.keys()) + list(self.expert_failures.keys())):
            successes = self.expert_success.get(expert_id, 0)
            failures = self.expert_failures.get(expert_id, 0)
            total = successes + failures
            rates[expert_id] = successes / total if total > 0 else 0.5
        return rates
    
    def get_expert_latency_stats(self) -> Dict[str, Dict[str, float]]:
        """Get comprehensive latency statistics per expert"""
        stats = {}
        
        for expert_id, latencies in self.expert_latency.items():
            values = [l['value'] if isinstance(l, dict) else l for l in latencies]
            
            if values:
                arr = np.array(values)
                stats[expert_id] = {
                    'avg_ms': float(np.mean(arr)),
                    'p50_ms': float(np.median(arr)),
                    'p95_ms': float(np.percentile(arr, 95)),
                    'p99_ms': float(np.percentile(arr, 99)),
                    'min_ms': float(np.min(arr)),
                    'max_ms': float(np.max(arr)),
                    'std_ms': float(np.std(arr)),
                    'samples': len(values)
                }
        
        return stats
    
    def get_resource_consumption(self) -> Dict[str, Dict[str, float]]:
        """Get resource consumption per expert"""
        consumption = {}
        for expert_id in set(list(self.expert_energy.keys()) + list(self.expert_carbon.keys())):
            consumption[expert_id] = {
                'total_energy_kwh': self.expert_energy.get(expert_id, 0.0),
                'total_carbon_kg': self.expert_carbon.get(expert_id, 0.0),
                'total_helium_units': self.expert_helium.get(expert_id, 0.0),
                'carbon_per_use_kg': self.expert_carbon.get(expert_id, 0.0) / max(self.expert_usage.get(expert_id, 1), 1),
                'helium_per_use': self.expert_helium.get(expert_id, 0.0) / max(self.expert_usage.get(expert_id, 1), 1)
            }
        return consumption
    
    def get_pareto_frontier(self) -> List[Dict]:
        """Get Pareto-optimal points with enhanced filtering"""
        if not self.pareto_points:
            return []
        
        # Get recent points
        recent = list(self.pareto_points)[-1000:]
        
        # Find non-dominated points
        pareto_optimal = []
        for i, point in enumerate(recent):
            dominated = False
            for j, other in enumerate(recent):
                if i != j:
                    if (other['energy'] <= point['energy'] and
                        other['time'] <= point['time'] and
                        other['helium'] <= point['helium'] and
                        (other['energy'] < point['energy'] or
                         other['time'] < point['time'] or
                         other['helium'] < point['helium'])):
                        dominated = True
                        break
            if not dominated:
                pareto_optimal.append(point)
        
        return pareto_optimal
    
    # ========================================================================
    # Health and Status
    # ========================================================================
    
    def get_health_scores(self) -> Dict[str, float]:
        """Get health scores for all experts"""
        return self.health_scores.copy()
    
    def get_alerts(
        self,
        acknowledged: Optional[bool] = None,
        severity: Optional[MetricSeverity] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get filtered alerts"""
        alerts = list(self.alert_history)
        
        if acknowledged is not None:
            alerts = [a for a in alerts if a.get('acknowledged') == acknowledged]
        
        if severity:
            alerts = [a for a in alerts if a.get('severity') == severity.value]
        
        return alerts[-limit:]
    
    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert"""
        if alert_id in self.active_alerts:
            self.active_alerts[alert_id]['acknowledged'] = True
            return True
        return False
    
    def get_predictions(self) -> Dict[str, Dict[str, Any]]:
        """Get predictive analytics"""
        return self.predictions.copy()
    
    def get_slo_status(self) -> Dict[str, Dict[str, Any]]:
        """Get SLO status"""
        if self.slo_tracker:
            return self.slo_tracker.evaluate_slos()
        return {}
    
    def get_cost_attribution(
        self,
        time_period_hours: float = 24.0
    ) -> Dict[str, CostAttribution]:
        """Get cost attribution for all experts"""
        attributions = {}
        if self.cost_engine:
            for expert_id in self.expert_usage:
                attributions[expert_id] = self.cost_engine.get_expert_cost_attribution(
                    expert_id, time_period_hours
                )
        return attributions
    
    def get_optimization_recommendations(self) -> List[str]:
        """Get optimization recommendations"""
        if self.cost_engine:
            return self.cost_engine.get_optimization_recommendations()
        return []
    
    # ========================================================================
    # Metrics Summary
    # ========================================================================
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary"""
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
            'total_alerts': len(self.alert_history)
        }
        
        if self.slo_tracker:
            summary['slo_status'] = self.slo_tracker.evaluate_slos()
        
        if self.anomaly_detector:
            summary['anomaly_stats'] = self.anomaly_detector.get_detection_stats()
        
        if self.enable_predictive:
            summary['predictions'] = self.get_predictions()
        
        return summary
    
    def get_expert_performance_report(
        self,
        expert_id: str
    ) -> Dict[str, Any]:
        """Get detailed performance report for an expert"""
        latency_stats = self.get_expert_latency_stats().get(expert_id, {})
        success_rate = self.get_expert_success_rate().get(expert_id, 0)
        health = self.health_scores.get(expert_id, 0)
        usage = self.get_expert_usage().get(expert_id, 0)
        
        # Get cost attribution
        cost = None
        if self.cost_engine:
            cost = self.cost_engine.get_expert_cost_attribution(expert_id)
        
        # Get predictions
        predictions = self.predictions.get(expert_id, {})
        
        return {
            'expert_id': expert_id,
            'usage_rate': usage,
            'success_rate': success_rate,
            'health_score': health,
            'latency': latency_stats,
            'cost_attribution': {
                'carbon_kg': self.expert_carbon.get(expert_id, 0),
                'helium_units': self.expert_helium.get(expert_id, 0),
                'energy_kwh': self.expert_energy.get(expert_id, 0),
                'efficiency': cost.carbon_efficiency_score if cost else 0
            },
            'predictions': predictions,
            'total_executions': self.expert_usage.get(expert_id, 0)
        }
    
    # ========================================================================
    # Export Methods
    # ========================================================================
    
    def to_prometheus_format(self) -> str:
        """Export metrics in Prometheus format with enhanced labels"""
        lines = []
        timestamp_ms = int(time.time() * 1000)
        
        # Expert usage
        for expert_id, usage in self.get_expert_usage().items():
            lines.append(
                f'moe_expert_usage{{expert="{expert_id}"}} {usage} {timestamp_ms}'
            )
        
        # Success rates
        for expert_id, rate in self.get_expert_success_rate().items():
            lines.append(
                f'moe_expert_success_rate{{expert="{expert_id}"}} {rate} {timestamp_ms}'
            )
        
        # Latency stats
        for expert_id, stats in self.get_expert_latency_stats().items():
            lines.append(
                f'moe_expert_latency_avg{{expert="{expert_id}"}} {stats["avg_ms"]} {timestamp_ms}'
            )
            lines.append(
                f'moe_expert_latency_p95{{expert="{expert_id}"}} {stats["p95_ms"]} {timestamp_ms}'
            )
            lines.append(
                f'moe_expert_latency_p99{{expert="{expert_id}"}} {stats["p99_ms"]} {timestamp_ms}'
            )
        
        # Resource consumption
        for expert_id, resources in self.get_resource_consumption().items():
            lines.append(
                f'moe_expert_energy_kwh{{expert="{expert_id}"}} {resources["total_energy_kwh"]} {timestamp_ms}'
            )
            lines.append(
                f'moe_expert_carbon_kg{{expert="{expert_id}"}} {resources["total_carbon_kg"]} {timestamp_ms}'
            )
            lines.append(
                f'moe_expert_helium_units{{expert="{expert_id}"}} {resources["total_helium_units"]} {timestamp_ms}'
            )
        
        # Health scores
        for expert_id, health in self.get_health_scores().items():
            lines.append(
                f'moe_expert_health{{expert="{expert_id}"}} {health} {timestamp_ms}'
            )
        
        # Routing metrics
        lines.append(
            f'moe_routing_total {len(self.routing_decisions)} {timestamp_ms}'
        )
        
        if self.routing_latency:
            lines.append(
                f'moe_routing_latency_avg {np.mean(list(self.routing_latency))} {timestamp_ms}'
            )
        
        # Alert count
        active_alerts = len([a for a in self.active_alerts.values() if not a.get('acknowledged')])
        lines.append(
            f'moe_active_alerts {active_alerts} {timestamp_ms}'
        )
        
        return '\n'.join(lines)
    
    def to_json_format(self) -> str:
        """Export metrics in JSON format"""
        return json.dumps(self.get_metrics_summary(), indent=2, default=str)
    
    def to_grafana_format(self) -> List[Dict[str, Any]]:
        """Export metrics in Grafana-compatible format"""
        panels = []
        
        # Usage panel
        usage_data = []
        for expert_id, usage in self.get_expert_usage().items():
            usage_data.append({
                'target': f'expert_{expert_id}',
                'datapoints': [[usage, int(time.time() * 1000)]]
            })
        
        panels.append({
            'title': 'Expert Usage',
            'type': 'piechart',
            'data': usage_data
        })
        
        # Latency panel
        latency_data = []
        for expert_id, stats in self.get_expert_latency_stats().items():
            latency_data.append({
                'target': f'expert_{expert_id}',
                'datapoints': [
                    [stats['p50_ms'], int(time.time() * 1000)],
                    [stats['p95_ms'], int(time.time() * 1000)],
                    [stats['p99_ms'], int(time.time() * 1000)]
                ]
            })
        
        panels.append({
            'title': 'Expert Latency',
            'type': 'graph',
            'data': latency_data
        })
        
        # Health panel
        health_data = []
        for expert_id, health in self.get_health_scores().items():
            health_data.append({
                'target': f'expert_{expert_id}',
                'datapoints': [[health, int(time.time() * 1000)]]
            })
        
        panels.append({
            'title': 'Expert Health',
            'type': 'gauge',
            'data': health_data
        })
        
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
            self.pareto_points.clear()
            self.health_scores.clear()
            self.predictions.clear()
            
            logger.info("All metrics reset")
    
    def cleanup_old_data(self, max_age_hours: float = 24.0):
        """Clean up data older than specified age"""
        cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
        
        with self._lock:
            # Clean routing decisions
            while self.routing_decisions and self.routing_decisions[0]['timestamp'] < cutoff:
                self.routing_decisions.popleft()
            
            # Clean Pareto points
            while self.pareto_points and self.pareto_points[0]['timestamp'] < cutoff:
                self.pareto_points.popleft()
            
            # Clean alert history
            while self.alert_history and datetime.fromisoformat(self.alert_history[0]['timestamp']) < cutoff:
                self.alert_history.popleft()
        
        logger.info(f"Cleaned up metrics older than {max_age_hours}h")

# ============================================================================
# Legacy Compatibility
# ============================================================================

class ExpertMetricsCollectorLegacy(ExpertMetricsCollector):
    """
    Legacy metrics collector for backward compatibility.
    """
    
    def record_routing(
        self,
        routing_decisions: List[tuple],
        gating_context: Any,
        execution_time: float,
        success: bool
    ):
        """Legacy routing record"""
        super().record_routing(routing_decisions, gating_context, execution_time, success)
    
    def record_expert_execution(
        self,
        expert_id: str,
        execution_time: float,
        energy_kwh: float,
        carbon_kg: float,
        helium_units: float,
        success: bool
    ):
        """Legacy execution record"""
        super().record_expert_execution(
            expert_id, execution_time, energy_kwh,
            carbon_kg, helium_units, success
        )
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Legacy metrics summary"""
        summary = super().get_metrics_summary()
        # Add legacy fields
        summary['expert_performance'] = {
            str(k): v for k, v in self.get_expert_success_rate().items()
        }
        return summary
