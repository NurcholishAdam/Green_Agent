# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/monitoring/expert_metrics.py
"""
Enhanced Expert Metrics Collector v5.0.0 - Complete Green Agent Implementation

Complete bio-inspired integration with:
- Federated Reflexive Learning with distributed metrics aggregation
- User-Adaptive Reflexivity with dynamic configuration
- Real-time Carbon Intensity Integration with API support
- Cross-Domain Knowledge Transfer with correlation analysis
- Human-AI Collaborative Reflection with decision explanations
- Predictive Reflexivity with ensemble forecasting
- Sustainability Score with multi-metric aggregation
- Enhanced Carbon/Helium Awareness with real-time tracking
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
import aiohttp
import os

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
# Carbon Intensity Integration Module
# ============================================================================

class CarbonIntensityManager:
    """Real-time carbon intensity integration with API support"""
    
    def __init__(self, endpoint: str = "https://api.electricitymap.org/v3/carbon-intensity"):
        self.endpoint = endpoint
        self.carbon_intensity = 0.0
        self.region = "us-east"
        self.last_update = None
        self._lock = asyncio.Lock()
        self._session = None
        self.update_interval = 300
        self.cache = {}
        self.historical_intensities = deque(maxlen=1000)
        self.api_key = os.getenv('ELECTRICITYMAP_API_KEY', '')
    
    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def update_carbon_intensity(self, region: str = "us-east") -> Dict:
        async with self._lock:
            session = await self._get_session()
            try:
                url = f"{self.endpoint}/latest?zone={region}"
                headers = {'auth-token': self.api_key} if self.api_key else {}
                async with session.get(url, headers=headers, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.carbon_intensity = data.get('carbonIntensity', 400)
                        self.region = region
                        self.last_update = datetime.now()
                        self.cache[region] = {'intensity': self.carbon_intensity, 'timestamp': self.last_update}
                        self.historical_intensities.append(self.carbon_intensity)
                    else:
                        self.carbon_intensity = self._get_fallback_intensity(region)
                        self.last_update = datetime.now()
            except Exception as e:
                logger.error(f"Carbon intensity fetch error: {e}")
                self.carbon_intensity = self._get_fallback_intensity(region)
                self.last_update = datetime.now()
            return {'intensity': self.carbon_intensity, 'region': self.region,
                    'timestamp': self.last_update.isoformat() if self.last_update else None}
    
    def _get_fallback_intensity(self, region: str) -> float:
        fallback_values = {'us-east': 420, 'us-west': 350, 'eu': 280, 'asia': 500, 'default': 400}
        return fallback_values.get(region, 400)
    
    async def get_current_intensity(self) -> float:
        if self.last_update is None or (datetime.now() - self.last_update).seconds > self.update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Predictive Reflexivity Module
# ============================================================================

class PredictiveMetricsAnalyzer:
    """Predictive reflexivity with ensemble forecasting for metrics"""
    
    def __init__(self, history_window: int = 100):
        self.history_window = history_window
        self.metric_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.models = {}
        self.scaler = None
        self.is_trained = False
        
        try:
            from sklearn.preprocessing import StandardScaler
            from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
            from sklearn.metrics import r2_score
            self.scaler = StandardScaler()
            self.models['random_forest'] = RandomForestRegressor(n_estimators=100, random_state=42)
            self.models['gradient_boosting'] = GradientBoostingRegressor(n_estimators=100, random_state=42)
            self._ml_available = True
        except ImportError:
            self._ml_available = False
    
    def update_history(self, metric_data: Dict):
        self.metric_history.append({
            'timestamp': datetime.utcnow(),
            'success_rate': metric_data.get('success_rate', 0.8),
            'avg_latency_ms': metric_data.get('avg_latency_ms', 100),
            'carbon_intensity': metric_data.get('carbon_intensity', 400),
            'token_efficiency': metric_data.get('token_efficiency', 0.5),
            'health_score': metric_data.get('health_score', 0.5)
        })
    
    async def train_forecast_model(self):
        if not self._ml_available or len(self.metric_history) < 10:
            return {'status': 'insufficient_data'}
        
        X, y = [], []
        history_list = list(self.metric_history)
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([data['success_rate'], data['avg_latency_ms'] / 1000,
                               data['carbon_intensity'] / 100, data['token_efficiency'],
                               data['health_score']])
            X.append(features)
            y.append(history_list[i + 5]['health_score'])
        
        X = np.array(X); y = np.array(y)
        X_scaled = self.scaler.fit_transform(X)
        results = {}
        for name, model in self.models.items():
            if model is not None:
                model.fit(X_scaled, y)
                predictions = model.predict(X_scaled)
                from sklearn.metrics import r2_score
                results[name] = r2_score(y, predictions)
        self.is_trained = True
        return {'status': 'success', 'results': results}
    
    async def predict_metric_trend(self) -> Dict:
        if not self.is_trained or len(self.metric_history) < 10:
            return {'predicted_health': 0.5, 'confidence': 0.0, 'trend': 'insufficient_data'}
        
        recent = list(self.metric_history)[-5:]
        features = []
        for data in recent:
            features.extend([data['success_rate'], data['avg_latency_ms'] / 1000,
                           data['carbon_intensity'] / 100, data['token_efficiency'],
                           data['health_score']])
        features = np.array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        predictions = []
        for name, model in self.models.items():
            if model is not None:
                predictions.append(model.predict(features_scaled)[0])
        if not predictions:
            return {'predicted_health': 0.5, 'confidence': 0.0, 'trend': 'no_models'}
        prediction = np.mean(predictions)
        confidence = min(0.9, np.std(predictions) / 0.2) if len(predictions) > 1 else 0.5
        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "improving" if prediction > recent_forecasts[-1] else "declining" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"
        self.forecast_history.append({'prediction': prediction, 'trend': trend})
        return {'predicted_health': prediction, 'confidence': confidence, 'trend': trend,
                'recommended_actions': self._generate_actions(prediction)}
    
    def _generate_actions(self, prediction: float) -> List[str]:
        actions = []
        if prediction < 0.4:
            actions.append("Increase token allocation for critical experts")
            actions.append("Optimize carbon-aware scheduling")
        elif prediction < 0.6:
            actions.append("Enhance gradient health monitoring")
            actions.append("Improve compartment health")
        return actions or ["Metric trends are on track"]

# ============================================================================
# Cross-Domain Knowledge Transfer Module
# ============================================================================

class MetricsCrossDomainTransfer:
    """Cross-domain knowledge transfer for metrics"""
    
    def __init__(self):
        self.knowledge_base: Dict[str, Dict[str, Dict]] = {}
        self.transfer_logs = deque(maxlen=1000)
        self.domain_mappings = {
            'metrics→energy': {
                'efficiency_strategies': ['token-based', 'gradient-driven'],
                'resource_allocation': ['dynamic', 'adaptive']
            },
            'metrics→carbon': {
                'optimization_strategies': ['load-shifting', 'efficiency-first']
            },
            'metrics→helium': {
                'scarcity_strategies': ['efficiency-first', 'conservation']
            }
        }
    
    def transfer_knowledge(self, source_domain: str, target_domain: str, 
                          knowledge_type: str, data: Dict[str, Any]) -> Dict:
        key = f"{source_domain}→{target_domain}"
        if key not in self.knowledge_base:
            self.knowledge_base[key] = {}
        if knowledge_type not in self.knowledge_base[key]:
            self.knowledge_base[key][knowledge_type] = {'data': data, 'transfer_count': 1,
                'effectiveness_score': 0.5, 'last_used': datetime.utcnow()}
        else:
            existing = self.knowledge_base[key][knowledge_type]
            existing['data'].update(data); existing['transfer_count'] += 1
            existing['last_used'] = datetime.utcnow()
        self.transfer_logs.append({'timestamp': datetime.utcnow(), 'source': source_domain,
                                   'target': target_domain, 'type': knowledge_type})
        return self.knowledge_base[key][knowledge_type]
    
    def get_transfer_statistics(self) -> Dict:
        total_transfers = len(self.transfer_logs)
        domain_pairs = {}
        for log in self.transfer_logs:
            key = f"{log['source']}→{log['target']}"
            domain_pairs[key] = domain_pairs.get(key, 0) + 1
        return {'total_transfers': total_transfers, 'domain_pairs': domain_pairs,
                'knowledge_types': list(self.knowledge_base.keys())}

# ============================================================================
# Federated Metrics Aggregator Module
# ============================================================================

class FederatedMetricsAggregator:
    """Federated Reflexive Learning for distributed metrics aggregation"""
    
    def __init__(self, server_url: Optional[str] = None):
        self.server_url = server_url
        self.round = 0
        self.local_metrics = {}
        self.global_metrics = {}
        self.participants = []
        self.contribution_scores = {}
        self._lock = asyncio.Lock()
        self._session = None
    
    async def _get_session(self):
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def send_local_metrics(self, participant_id: str, metrics: Dict, performance: float = 1.0) -> Dict:
        """Send local metrics to federated server"""
        if not self.server_url:
            return {'status': 'local'}
        
        async with self._lock:
            session = await self._get_session()
            try:
                update_data = {
                    'participant_id': participant_id,
                    'round': self.round,
                    'metrics': metrics,
                    'performance': performance,
                    'timestamp': datetime.utcnow().isoformat()
                }
                async with session.post(
                    f"{self.server_url}/federated/metrics",
                    json=update_data,
                    timeout=30
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.round += 1
                        self.contribution_scores[participant_id] = performance
                        return result
                    return {'status': 'failed'}
            except Exception as e:
                logger.error(f"Federated metrics send error: {e}")
                return {'status': 'error'}
    
    async def get_global_metrics(self) -> Optional[Dict]:
        """Get aggregated metrics from federated server"""
        if not self.server_url:
            return self.global_metrics
        
        async with self._lock:
            session = await self._get_session()
            try:
                async with session.get(
                    f"{self.server_url}/federated/metrics/global",
                    timeout=30
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.global_metrics = data.get('metrics', {})
                        self.participants = data.get('participants', [])
                        return self.global_metrics
            except Exception as e:
                logger.error(f"Global metrics fetch error: {e}")
                return None
    
    def aggregate_metrics(self, peer_metrics: List[Dict], weights: Dict[str, float] = None) -> Dict:
        """Aggregate metrics from peers with weighted averaging"""
        if not peer_metrics:
            return {}
        
        aggregated = {}
        if weights is None:
            weights = {i: 1.0 for i in range(len(peer_metrics))}
        
        for metric_key in peer_metrics[0].keys():
            if isinstance(peer_metrics[0][metric_key], (int, float)):
                total = 0.0
                total_weight = 0.0
                for i, peer in enumerate(peer_metrics):
                    if metric_key in peer:
                        total += peer[metric_key] * weights.get(i, 1.0)
                        total_weight += weights.get(i, 1.0)
                aggregated[metric_key] = total / max(total_weight, 0.001)
            else:
                # For non-numeric metrics, take the most common value
                values = [peer.get(metric_key) for peer in peer_metrics if metric_key in peer]
                if values:
                    aggregated[metric_key] = max(set(values), key=values.count)
        
        return aggregated
    
    def get_federated_stats(self) -> Dict:
        return {
            'round': self.round,
            'participants': len(self.participants),
            'has_global_metrics': bool(self.global_metrics),
            'contribution_scores': self.contribution_scores
        }
    
    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Human-AI Collaborative Decision Support Module
# ============================================================================

class HumanAICollaborativeSupport:
    """Human-AI collaborative reflection with decision explanations"""
    
    def __init__(self):
        self.decision_history = deque(maxlen=1000)
        self.explanation_cache = {}
        self.feedback_history = deque(maxlen=500)
        self._lock = asyncio.Lock()
    
    def generate_explanation(self, expert_id: str, metrics: Dict[str, Any], 
                            anomalies: List[Any]) -> Dict[str, Any]:
        """Generate human-readable explanation for metric state"""
        explanation = {
            'expert_id': expert_id,
            'timestamp': datetime.utcnow().isoformat(),
            'summary': '',
            'details': [],
            'anomalies': [],
            'recommendations': []
        }
        
        # Generate summary
        success_rate = metrics.get('success_rate', 0.5)
        if success_rate > 0.9:
            summary = f"Expert {expert_id} is performing excellently with {success_rate:.1%} success rate."
        elif success_rate > 0.7:
            summary = f"Expert {expert_id} is performing well with {success_rate:.1%} success rate."
        elif success_rate > 0.5:
            summary = f"Expert {expert_id} has moderate performance with {success_rate:.1%} success rate."
        else:
            summary = f"Expert {expert_id} performance needs attention with {success_rate:.1%} success rate."
        explanation['summary'] = summary
        
        # Add latency details
        latency = metrics.get('avg_latency_ms', 0)
        if latency > 100:
            explanation['details'].append(f"High latency detected: {latency:.1f}ms (threshold: 100ms)")
            explanation['recommendations'].append("Consider scaling resources or optimizing execution path")
        
        # Add carbon details
        carbon = metrics.get('carbon_per_inference', 0)
        if carbon > 0.001:
            explanation['details'].append(f"High carbon footprint: {carbon:.6f} kg CO2 per inference")
            explanation['recommendations'].append("Optimize for carbon efficiency or use renewable energy")
        
        # Add token details
        token_cost = metrics.get('token_cost', 0)
        if token_cost > 50:
            explanation['details'].append(f"High Eco-ATP cost: {token_cost:.1f} tokens per execution")
            explanation['recommendations'].append("Consider token-efficient alternatives or batching")
        
        # Process anomalies
        for anomaly in anomalies:
            explanation['anomalies'].append({
                'type': anomaly.anomaly_type.value if hasattr(anomaly, 'anomaly_type') else 'unknown',
                'severity': anomaly.severity.value if hasattr(anomaly, 'severity') else 'info',
                'description': f"Anomaly detected: expected {anomaly.expected_value:.2f}, actual {anomaly.actual_value:.2f}"
            })
        
        # Add sustainability recommendations
        if metrics.get('sustainability_score', 0.5) < 0.6:
            explanation['recommendations'].append("Improve sustainability score through optimization")
        
        self.explanation_cache[f"{expert_id}_{datetime.utcnow().timestamp()}"] = explanation
        return explanation
    
    def process_feedback(self, expert_id: str, feedback: Dict) -> Dict:
        """Process human feedback on expert performance"""
        feedback_entry = {
            'expert_id': expert_id,
            'timestamp': datetime.utcnow().isoformat(),
            'feedback': feedback
        }
        self.feedback_history.append(feedback_entry)
        
        # Generate reflection based on feedback
        reflection = {
            'acknowledgment': f"Feedback received for expert {expert_id}",
            'actions': [],
            'insights': []
        }
        
        if feedback.get('concern') == 'performance':
            reflection['actions'].append("Review performance metrics and optimize execution")
            reflection['insights'].append("Performance optimization may require resource scaling")
        
        if feedback.get('concern') == 'sustainability':
            reflection['actions'].append("Implement sustainability improvements")
            reflection['insights'].append("Focus on carbon and helium efficiency")
        
        return reflection
    
    def get_decision_insights(self, expert_id: str, hours: int = 24) -> Dict:
        """Get aggregated decision insights for an expert"""
        recent_explanations = [
            exp for exp in self.explanation_cache.values()
            if exp.get('expert_id') == expert_id and 
            datetime.fromisoformat(exp['timestamp']) > datetime.utcnow() - timedelta(hours=hours)
        ]
        
        return {
            'expert_id': expert_id,
            'total_decisions': len(recent_explanations),
            'recent_summary': recent_explanations[-1].get('summary', 'No recent decisions') if recent_explanations else 'No decisions',
            'feedback_count': len([f for f in self.feedback_history if f.get('expert_id') == expert_id]),
            'recommendations': [r for exp in recent_explanations for r in exp.get('recommendations', [])][:5]
        }

# ============================================================================
# Enums and Data Classes (Enhanced with Bio-Inspired)
# ============================================================================

class MetricSeverity(Enum):
    INFO = "info"; WARNING = "warning"; CRITICAL = "critical"; EMERGENCY = "emergency"

class MetricType(Enum):
    COUNTER = "counter"; GAUGE = "gauge"; HISTOGRAM = "histogram"; SUMMARY = "summary"
    TREND = "trend"; GRADIENT = "gradient"; TOKEN = "token"; BIOMASS = "biomass"
    HARVESTER = "harvester"; COMPARTMENT = "compartment"

class AnomalyType(Enum):
    SPIKE = "spike"; DIP = "dip"; TREND_CHANGE = "trend_change"; LEVEL_SHIFT = "level_shift"
    VARIANCE_CHANGE = "variance_change"; OUTLIER = "outlier"; GRADIENT_ANOMALY = "gradient_anomaly"
    TOKEN_EXHAUSTION = "token_exhaustion"; BIOMASS_OVERFLOW = "biomass_overflow"

class SLOStatus(Enum):
    COMPLIANT = "compliant"; AT_RISK = "at_risk"; BREACHED = "breached"; UNKNOWN = "unknown"

@dataclass
class MetricThreshold:
    metric_name: str
    warning_threshold: float
    critical_threshold: float
    comparison: str = "greater_than"
    duration_seconds: float = 60.0
    cooldown_seconds: float = 300.0
    gradient_modulated: bool = False
    sustainability_impact: float = 0.0
    
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
    token_cost_per_violation: float = 10.0
    sustainability_weight: float = 0.5

@dataclass
class AnomalyEvent:
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
    gradient_level: float = 0.5
    sustainability_impact: float = 0.0

@dataclass
class MetricSample:
    timestamp: datetime
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    trace_id: Optional[str] = None
    expert_id: Optional[str] = None
    token_cost: float = 0.0
    gradient_level: float = 0.5
    sustainability_score: float = 0.0

@dataclass
class CostAttribution:
    expert_id: str
    time_period: str
    total_carbon_kg: float = 0.0
    total_helium_units: float = 0.0
    total_energy_kwh: float = 0.0
    total_ecoatp_cost: float = 0.0
    cost_per_operation: float = 0.0
    carbon_efficiency_score: float = 0.0
    helium_efficiency_score: float = 0.0
    token_efficiency_score: float = 0.0
    trend: str = "stable"
    sustainability_score: float = 0.0

@dataclass
class PredictiveMetricForecast:
    timestamp: datetime = field(default_factory=datetime.utcnow)
    predicted_health: float = 0.0
    predicted_success_rate: float = 0.0
    predicted_latency_ms: float = 0.0
    confidence: float = 0.0
    trend: str = "stable"
    recommended_actions: List[str] = field(default_factory=list)

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
        self.gradient_manager: Optional[GradientFieldManager] = None
    
    def inject_gradient_manager(self, gradient_manager):
        self.gradient_manager = gradient_manager
    
    def add_sample(self, metric_name: str, value: float):
        self.metric_history[metric_name].append({'value': value, 'timestamp': datetime.utcnow()})
    
    def detect_anomalies(self, metric_name: str, current_value: float,
                        expert_id: Optional[str] = None) -> List[AnomalyEvent]:
        anomalies = []
        history = self.metric_history.get(metric_name)
        if not history or len(history) < self.min_samples:
            return anomalies
        
        values = np.array([h['value'] for h in history])
        
        gradient_mod = 1.0
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength > 0.7:
                gradient_mod = 0.7
        
        zscore_anomaly = self._zscore_detect(metric_name, current_value, values, gradient_mod)
        if zscore_anomaly:
            anomalies.append(zscore_anomaly)
        
        iqr_anomaly = self._iqr_detect(metric_name, current_value, values)
        if iqr_anomaly:
            anomalies.append(iqr_anomaly)
        
        if 'token' in metric_name.lower() and current_value < 10.0:
            anomalies.append(AnomalyEvent(
                event_id=f"token_exhaustion_{datetime.utcnow().timestamp()}",
                metric_name=metric_name,
                anomaly_type=AnomalyType.TOKEN_EXHAUSTION,
                detected_at=datetime.utcnow(),
                expected_value=100.0, actual_value=current_value,
                deviation_std=5.0, severity=MetricSeverity.CRITICAL,
                expert_id=expert_id,
                sustainability_impact=0.9
            ))
        
        for anomaly in anomalies:
            anomaly.expert_id = expert_id
            if self.gradient_manager:
                carbon = self.gradient_manager.fields.get('carbon')
                anomaly.gradient_level = carbon.gradient_strength if carbon else 0.5
            anomaly.sustainability_impact = 1.0 - min(1.0, anomaly.deviation_std / 10)
        
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
                 'sustainability_impact': d.sustainability_impact,
                 'timestamp': d.detected_at.isoformat()}
                for d in list(self.detection_history)[-20:]
            ]
        }

# ============================================================================
# Enhanced Expert Metrics Collector
# ============================================================================

class ExpertMetricsCollector:
    """
    Enhanced Expert Metrics Collector v5.0.0 - Complete Green Agent Implementation
    """
    
    def __init__(
        self,
        enable_anomaly_detection: bool = True,
        enable_slo_tracking: bool = True,
        enable_cost_attribution: bool = True,
        enable_alerting: bool = True,
        enable_predictive: bool = True,
        enable_bio_integration: bool = True,
        enable_carbon_intensity: bool = True,
        enable_federated: bool = True,
        enable_cross_domain: bool = True,
        enable_human_ai: bool = True,
        enable_sustainability_scoring: bool = True,
        retention_hours: float = 24.0
    ):
        # Feature flags
        self.enable_anomaly_detection = enable_anomaly_detection
        self.enable_slo_tracking = enable_slo_tracking
        self.enable_cost_attribution = enable_cost_attribution
        self.enable_alerting = enable_alerting
        self.enable_predictive = enable_predictive
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        self.enable_carbon_intensity = enable_carbon_intensity
        self.enable_federated = enable_federated
        self.enable_cross_domain = enable_cross_domain
        self.enable_human_ai = enable_human_ai
        self.enable_sustainability_scoring = enable_sustainability_scoring
        self.retention_hours = retention_hours
        
        # Bio-inspired modules
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        
        # New modules
        self.carbon_manager = CarbonIntensityManager()
        self.predictive_analyzer = PredictiveMetricsAnalyzer()
        self.cross_domain_transfer = MetricsCrossDomainTransfer()
        self.federated_aggregator = FederatedMetricsAggregator()
        self.human_ai_support = HumanAICollaborativeSupport()
        
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
        self.expert_ecoatp: Dict[str, float] = defaultdict(float)
        
        # Pareto frontier data
        self.pareto_points: deque = deque(maxlen=10000)
        
        # Sustainability tracking
        self.total_carbon_savings_kg = 0.0
        self.total_helium_savings_l = 0.0
        self.sustainability_score = 0.0
        
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
        
        # Bio metrics history
        self.bio_metrics_history: deque = deque(maxlen=10000)
        
        # Start background tasks
        self._start_background_tasks()
        
        logger.info(
            f"Enhanced Expert Metrics Collector v5.0.0 initialized: "
            f"bio_integration={self.enable_bio_integration}, "
            f"carbon_intensity={self.enable_carbon_intensity}, "
            f"predictive={self.enable_predictive}, "
            f"federated={self.enable_federated}"
        )
    
    def _start_background_tasks(self):
        if self.enable_carbon_intensity:
            asyncio.create_task(self._carbon_update_loop())
        if self.enable_predictive:
            asyncio.create_task(self._predictive_update_loop())
        if self.enable_federated:
            asyncio.create_task(self._federated_sync_loop())
    
    async def _carbon_update_loop(self):
        while True:
            try:
                await self.carbon_manager.update_carbon_intensity()
                await asyncio.sleep(self.carbon_manager.update_interval)
            except Exception as e:
                logger.error(f"Carbon update error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _predictive_update_loop(self):
        while True:
            try:
                summary = self.get_metrics_summary()
                self.predictive_analyzer.update_history({
                    'success_rate': summary.get('success_rates', {}).values() and np.mean(list(summary['success_rates'].values())) or 0.5,
                    'avg_latency_ms': np.mean([s.get('avg_ms', 0) for s in summary.get('latency_stats', {}).values()]) if summary.get('latency_stats') else 100,
                    'carbon_intensity': await self.carbon_manager.get_current_intensity() if self.enable_carbon_intensity else 400,
                    'token_efficiency': self._get_token_efficiency(),
                    'health_score': np.mean(list(self.health_scores.values())) if self.health_scores else 0.5
                })
                await self.predictive_analyzer.train_forecast_model()
                await asyncio.sleep(300)
            except Exception as e:
                logger.error(f"Predictive update error: {str(e)}")
                await asyncio.sleep(60)
    
    async def _federated_sync_loop(self):
        while True:
            try:
                if self.enable_federated:
                    summary = self.get_metrics_summary()
                    await self.federated_aggregator.send_local_metrics(
                        f"metrics_{self._get_instance_id()}",
                        summary,
                        performance=self.sustainability_score
                    )
                    await self.federated_aggregator.get_global_metrics()
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error(f"Federated sync error: {str(e)}")
                await asyncio.sleep(300)
    
    def _get_instance_id(self) -> str:
        return hashlib.md5(f"{datetime.utcnow()}_{id(self)}".encode()).hexdigest()[:8]
    
    def _get_token_efficiency(self) -> float:
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            return summary.get('system_efficiency', 0.5)
        return 0.5
    
    def _initialize_thresholds(self):
        self.thresholds = {
            'latency_p95': MetricThreshold('latency_p95', 100.0, 500.0, 'greater_than', gradient_modulated=True),
            'error_rate': MetricThreshold('error_rate', 0.05, 0.10, 'greater_than', gradient_modulated=True),
            'carbon_per_inference': MetricThreshold('carbon_per_inference', 0.0005, 0.001, 'greater_than', gradient_modulated=True),
            'token_balance': MetricThreshold('token_balance', 200.0, 50.0, 'less_than', gradient_modulated=True),
            'gradient_health': MetricThreshold('gradient_health', 0.3, 0.1, 'less_than', gradient_modulated=True),
            'biomass_level': MetricThreshold('biomass_level', 8000.0, 9500.0, 'greater_than', gradient_modulated=True),
            'sustainability_score': MetricThreshold('sustainability_score', 0.7, 0.4, 'less_than', gradient_modulated=True)
        }
    
    def _initialize_slos(self):
        self.slo_tracker.define_slo('latency_slo', 'expert_latency_ms', target_value=100.0, target_percentile=99.0)
        self.slo_tracker.define_slo('availability_slo', 'expert_success_rate', target_value=0.999, target_percentile=99.9)
        self.slo_tracker.define_slo('carbon_slo', 'carbon_per_inference', target_value=0.0005, target_percentile=95.0)
        self.slo_tracker.define_slo('token_efficiency_slo', 'token_efficiency', target_value=0.8, target_percentile=90.0)
        self.slo_tracker.define_slo('sustainability_slo', 'sustainability_score', target_value=0.7, target_percentile=95.0)
    
    # ========================================================================
    # Bio-Inspired Module Injection
    # ========================================================================
    
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
    # Bio-Inspired Methods
    # ========================================================================
    
    def _get_gradient_modulation(self) -> float:
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength > 0.7:
                return 0.7
        return 1.0
    
    def _pump_trust_gradient(self, expert_id: str, success: bool):
        if self.gradient_manager:
            delta = 0.05 if success else -0.1
            self.gradient_manager.pump_field('trust', delta, source=f"expert_{expert_id}")
    
    def _record_token_consumption(self, expert_id: str, energy_kwh: float, success: bool):
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
    
    def _calculate_sustainability_score(self) -> float:
        """Calculate overall sustainability score"""
        if not self.health_scores:
            return 0.5
        
        avg_health = np.mean(list(self.health_scores.values()))
        token_efficiency = self._get_token_efficiency()
        
        if self.enable_carbon_intensity:
            carbon_intensity = self.carbon_manager.carbon_intensity
            carbon_factor = 1.0 - (carbon_intensity / 800)
        else:
            carbon_factor = 0.5
        
        success_rates = self.get_expert_success_rate()
        avg_success = np.mean(list(success_rates.values())) if success_rates else 0.5
        
        score = (avg_health * 0.25 + token_efficiency * 0.2 + carbon_factor * 0.25 + avg_success * 0.3)
        return min(1.0, max(0.0, score))
    
    # ========================================================================
    # Enhanced Metric Recording
    # ========================================================================
    
    def record_routing(
        self, routing_decisions: List[Tuple[int, float]], gating_context: Any,
        execution_time: float, success: bool, correlation_id: Optional[str] = None
    ):
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
        with self._lock:
            # Latency tracking
            self.expert_latency[expert_id].append({'value': execution_time, 'timestamp': datetime.utcnow()})
            
            # Resource tracking
            self.expert_energy[expert_id] += energy_kwh
            self.expert_carbon[expert_id] += carbon_kg
            self.expert_helium[expert_id] += helium_units
            
            # Token consumption
            if self.enable_bio_integration:
                self._record_token_consumption(expert_id, energy_kwh, success)
            
            # Success tracking
            if success:
                self.expert_success[expert_id] = self.expert_success.get(expert_id, 0) + 1
            else:
                self.expert_failures[expert_id] = self.expert_failures.get(expert_id, 0) + 1
            
            # Pump trust gradient
            if self.enable_bio_integration:
                self._pump_trust_gradient(expert_id, success)
            
            # Pareto point
            self.pareto_points.append({
                'expert_id': expert_id, 'energy': energy_kwh, 'time': execution_time,
                'helium': helium_units, 'carbon': carbon_kg,
                'ecoatp': self.expert_ecoatp.get(expert_id, 0),
                'timestamp': datetime.utcnow()
            })
            
            # Anomaly detection
            if self.enable_anomaly_detection:
                anomalies = self.anomaly_detector.detect_anomalies(
                    f"{expert_id}_latency", execution_time, expert_id
                )
                for anomaly in anomalies:
                    self._process_anomaly(anomaly)
            
            # Bio metrics
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
            
            # Threshold checking
            if self.enable_alerting:
                self._check_bio_thresholds(expert_id, execution_time, success)
            
            # Health score update
            self._update_health_score(expert_id)
            
            # Predictive analytics
            if self.enable_predictive:
                self._update_predictions(expert_id)
            
            # Cross-domain knowledge transfer
            if self.enable_cross_domain:
                self.cross_domain_transfer.transfer_knowledge(
                    'metrics', 'energy',
                    'efficiency_strategies',
                    {'expert_id': expert_id, 'energy': energy_kwh, 'carbon': carbon_kg}
                )
            
            # Human-AI collaboration
            if self.enable_human_ai:
                self.human_ai_support.generate_explanation(
                    expert_id,
                    {'success_rate': self.get_expert_success_rate().get(expert_id, 0.5),
                     'avg_latency_ms': execution_time,
                     'carbon_per_inference': carbon_kg,
                     'token_cost': self.expert_ecoatp.get(expert_id, 0) / max(self.expert_usage.get(expert_id, 1), 1)},
                    []
                )
            
            # Update sustainability
            if self.enable_sustainability_scoring:
                self.sustainability_score = self._calculate_sustainability_score()
                self.total_carbon_savings_kg += max(0, 0.001 - carbon_kg) if carbon_kg < 0.001 else 0
    
    def _process_anomaly(self, anomaly: AnomalyEvent):
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
        gradient_mod = self._get_gradient_modulation() if self.enable_bio_integration else 1.0
        
        if 'latency_p95' in self.thresholds:
            p95 = self.get_expert_latency_stats().get(expert_id, {}).get('p95_ms', 0)
            exceeded, severity = self.thresholds['latency_p95'].is_exceeded(p95, gradient_mod)
            if exceeded:
                self._create_alert(f"latency_{expert_id}", 'latency_p95',
                    f"Expert {expert_id} P95 latency {p95:.1f}ms exceeded threshold", severity)
        
        if 'token_balance' in self.thresholds and self.enable_bio_integration:
            if self.token_manager:
                summary = self.token_manager.get_system_summary()
                balance = summary.get('total_balance', 500)
                exceeded, severity = self.thresholds['token_balance'].is_exceeded(balance, gradient_mod)
                if exceeded:
                    self._create_alert('token_balance', 'token_balance',
                        f"System token balance {balance:.0f} below threshold", severity)
        
        if 'sustainability_score' in self.thresholds:
            exceeded, severity = self.thresholds['sustainability_score'].is_exceeded(
                self.sustainability_score, gradient_mod)
            if exceeded:
                self._create_alert('sustainability_score', 'sustainability_score',
                    f"Sustainability score {self.sustainability_score:.2f} below threshold", severity)
    
    def _create_alert(self, alert_id: str, metric_name: str, message: str, severity: MetricSeverity):
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
            'gradient_level': self._get_gradient_modulation() if self.enable_bio_integration else 1.0,
            'sustainability_impact': self.sustainability_score
        }
        
        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)
        self.alert_cooldowns[alert_id] = datetime.utcnow()
        
        log_level = logging.CRITICAL if severity == MetricSeverity.CRITICAL else logging.WARNING
        logger.log(log_level, f"ALERT: {message}")
    
    def _update_health_score(self, expert_id: str):
        success_rate = self.get_expert_success_rate().get(expert_id, 0.5)
        latency_stats = self.get_expert_latency_stats().get(expert_id, {})
        p95 = latency_stats.get('p95_ms', 100)
        latency_score = 1.0 / (1.0 + p95 / 100)
        
        total_carbon = self.expert_carbon.get(expert_id, 0)
        total_usage = max(self.expert_usage.get(expert_id, 1), 1)
        carbon_score = 1.0 / (1.0 + total_carbon / total_usage * 10000)
        
        token_score = 0.5
        if self.enable_bio_integration:
            ecoatp = self.expert_ecoatp.get(expert_id, 0)
            token_score = 1.0 / (1.0 + ecoatp / max(total_usage, 1) / 100)
        
        health = 0.35 * success_rate + 0.25 * latency_score + 0.25 * carbon_score + 0.15 * token_score
        self.health_scores[expert_id] = health
    
    def _update_predictions(self, expert_id: str):
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
            'bio_modules_available': BIO_INSPIRED_AVAILABLE,
            'carbon_intensity_active': self.enable_carbon_intensity,
            'federated_active': self.enable_federated,
            'cross_domain_active': self.enable_cross_domain,
            'human_ai_active': self.enable_human_ai,
            'sustainability_scoring_active': self.enable_sustainability_scoring,
            'sustainability_score': self.sustainability_score,
            'total_carbon_savings_kg': self.total_carbon_savings_kg
        }
        
        if self.slo_tracker:
            summary['slo_status'] = self.slo_tracker.evaluate_slos()
        
        if self.anomaly_detector:
            summary['anomaly_stats'] = self.anomaly_detector.get_detection_stats()
        
        if self.enable_predictive:
            summary['predictions'] = self.get_predictions()
        
        if self.enable_federated:
            summary['federated_stats'] = self.federated_aggregator.get_federated_stats()
        
        if self.enable_human_ai:
            summary['human_ai_insights'] = self.human_ai_support.get_decision_insights('all', 24)
        
        if self.enable_bio_integration:
            summary['bio_metrics'] = self._get_bio_metrics()
            summary['gradient_modulation'] = self._get_gradient_modulation()
        
        if self.enable_cross_domain:
            summary['cross_domain_stats'] = self.cross_domain_transfer.get_transfer_statistics()
        
        return summary
    
    def get_expert_performance_report(self, expert_id: str) -> Dict[str, Any]:
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
            'total_executions': self.expert_usage.get(expert_id, 0),
            'sustainability_contribution': self.sustainability_score
        }
    
    def get_sustainability_report(self) -> Dict[str, Any]:
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'sustainability_score': self.sustainability_score,
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'total_helium_savings_l': self.total_helium_savings_l,
            'bio_integration_active': self.enable_bio_integration,
            'predictive_forecast': asyncio.run(self.predictive_analyzer.predict_metric_trend()) if self.enable_predictive else {},
            'recommendations': self._generate_sustainability_recommendations()
        }
    
    def _generate_sustainability_recommendations(self) -> List[str]:
        recommendations = []
        if self.sustainability_score < 0.5:
            recommendations.append("Increase token efficiency for better sustainability")
            recommendations.append("Optimize carbon-aware expert scheduling")
        if self.total_carbon_savings_kg < 10:
            recommendations.append("Implement more aggressive carbon reduction strategies")
        if self.enable_bio_integration and self._get_gradient_modulation() < 0.5:
            recommendations.append("Improve gradient health through better trust management")
        if self.enable_federated and len(self.federated_aggregator.participants) < 2:
            recommendations.append("Increase federated participation for better metrics aggregation")
        return recommendations or ["Metrics sustainability is on track"]
    
    # ========================================================================
    # Export Methods
    # ========================================================================
    
    def to_prometheus_format(self) -> str:
        lines = []
        timestamp_ms = int(time.time() * 1000)
        
        for expert_id, usage in self.get_expert_usage().items():
            lines.append(f'moe_expert_usage{{expert="{expert_id}"}} {usage} {timestamp_ms}')
        
        for expert_id, rate in self.get_expert_success_rate().items():
            lines.append(f'moe_expert_success_rate{{expert="{expert_id}"}} {rate} {timestamp_ms}')
        
        for expert_id, stats in self.get_expert_latency_stats().items():
            lines.append(f'moe_expert_latency_avg{{expert="{expert_id}"}} {stats["avg_ms"]} {timestamp_ms}')
            lines.append(f'moe_expert_latency_p95{{expert="{expert_id}"}} {stats["p95_ms"]} {timestamp_ms}')
        
        for expert_id, resources in self.get_resource_consumption().items():
            lines.append(f'moe_expert_energy_kwh{{expert="{expert_id}"}} {resources["total_energy_kwh"]} {timestamp_ms}')
            lines.append(f'moe_expert_carbon_kg{{expert="{expert_id}"}} {resources["total_carbon_kg"]} {timestamp_ms}')
            lines.append(f'moe_expert_helium_units{{expert="{expert_id}"}} {resources["total_helium_units"]} {timestamp_ms}')
            lines.append(f'moe_expert_ecoatp{{expert="{expert_id}"}} {resources["total_ecoatp"]} {timestamp_ms}')
        
        for expert_id, health in self.get_health_scores().items():
            lines.append(f'moe_expert_health{{expert="{expert_id}"}} {health} {timestamp_ms}')
        
        lines.append(f'moe_routing_total {len(self.routing_decisions)} {timestamp_ms}')
        if self.routing_latency:
            lines.append(f'moe_routing_latency_avg {np.mean(list(self.routing_latency))} {timestamp_ms}')
        
        # Sustainability metrics
        lines.append(f'green_agent_sustainability_score {self.sustainability_score} {timestamp_ms}')
        lines.append(f'green_agent_carbon_savings_kg {self.total_carbon_savings_kg} {timestamp_ms}')
        
        if self.enable_bio_integration and self.gradient_manager:
            for field_id, field in self.gradient_manager.fields.items():
                lines.append(f'green_agent_gradient{{field="{field_id}"}} {field.gradient_strength} {timestamp_ms}')
        
        if self.enable_bio_integration and self.token_manager:
            summary = self.token_manager.get_system_summary()
            lines.append(f'green_agent_ecoatp_balance {summary.get("total_balance", 0)} {timestamp_ms}')
            lines.append(f'green_agent_ecoatp_efficiency {summary.get("system_efficiency", 0)} {timestamp_ms}')
        
        active_alerts = len([a for a in self.active_alerts.values() if not a.get('acknowledged')])
        lines.append(f'moe_active_alerts {active_alerts} {timestamp_ms}')
        
        return '\n'.join(lines)
    
    def to_json_format(self) -> str:
        return json.dumps(self.get_metrics_summary(), indent=2, default=str)
    
    def to_grafana_format(self) -> List[Dict[str, Any]]:
        panels = []
        timestamp_ms = int(time.time() * 1000)
        
        usage_data = []
        for expert_id, usage in self.get_expert_usage().items():
            usage_data.append({'target': f'expert_{expert_id}', 'datapoints': [[usage, timestamp_ms]]})
        panels.append({'title': 'Expert Usage', 'type': 'piechart', 'data': usage_data})
        
        latency_data = []
        for expert_id, stats in self.get_expert_latency_stats().items():
            latency_data.append({
                'target': f'expert_{expert_id}',
                'datapoints': [[stats['p50_ms'], timestamp_ms], [stats['p95_ms'], timestamp_ms]]
            })
        panels.append({'title': 'Expert Latency', 'type': 'graph', 'data': latency_data})
        
        if self.enable_bio_integration and self.gradient_manager:
            gradient_data = []
            for field_id, field in self.gradient_manager.fields.items():
                gradient_data.append({
                    'target': f'gradient_{field_id}',
                    'datapoints': [[field.gradient_strength, timestamp_ms]]
                })
            panels.append({'title': 'Gradient Fields', 'type': 'gauge', 'data': gradient_data})
        
        if self.enable_bio_integration and self.token_manager:
            summary = self.token_manager.get_system_summary()
            token_data = [
                {'target': 'balance', 'datapoints': [[summary.get('total_balance', 0), timestamp_ms]]},
                {'target': 'generated', 'datapoints': [[summary.get('total_generated', 0), timestamp_ms]]},
                {'target': 'consumed', 'datapoints': [[summary.get('total_consumed', 0), timestamp_ms]]}
            ]
            panels.append({'title': 'Token Economy', 'type': 'graph', 'data': token_data})
        
        if self.enable_bio_integration and self.biomass_storage:
            stats = self.biomass_storage.get_storage_stats()
            biomass_data = []
            for tier, count in stats.get('tiers', {}).items():
                biomass_data.append({'target': tier, 'datapoints': [[count, timestamp_ms]]})
            panels.append({'title': 'Biomass Storage', 'type': 'bargauge', 'data': biomass_data})
        
        # Sustainability panel
        panels.append({
            'title': 'Sustainability Score',
            'type': 'stat',
            'data': [{'target': 'score', 'datapoints': [[self.sustainability_score, timestamp_ms]]}]
        })
        
        return panels
    
    # ========================================================================
    # Maintenance Methods
    # ========================================================================
    
    def reset_metrics(self):
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
        cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
        with self._lock:
            while self.routing_decisions and self.routing_decisions[0]['timestamp'] < cutoff:
                self.routing_decisions.popleft()
            while self.pareto_points and self.pareto_points[0]['timestamp'] < cutoff:
                self.pareto_points.popleft()
            while self.alert_history and datetime.fromisoformat(self.alert_history[0]['timestamp']) < cutoff:
                self.alert_history.popleft()
        logger.info(f"Cleaned up metrics older than {max_age_hours}h")
    
    async def shutdown(self):
        logger.info("Shutting down Expert Metrics Collector")
        await self.carbon_manager.close()
        await self.federated_aggregator.close()
        logger.info("Shutdown complete")


# ============================================================================
# Legacy Compatibility Classes
# ============================================================================

class SLOTracker:
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
            helium_efficiency_score=1.0 / (1.0 + total_helium * 10),
            sustainability_score=0.5
        )
