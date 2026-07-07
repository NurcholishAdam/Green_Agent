# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/bio_inspired/__init__.py
# Complete enhanced file with protocol-based DI, economic reporting, consolidated initialization,
# event-driven communication (NEW), predictive alerts (NEW), cost-benefit analysis (NEW),
# workflow orchestration (NEW), and anomaly detection (NEW)

"""
Bio-Inspired Green Agent v6.0.0
Complete implementation with protocol-based DI, supply management, economic reporting,
event-driven communication, predictive alerts, cost-benefit analysis, workflow orchestration,
and anomaly detection
"""

from .quantum_bridge import QuantumBridge
from .time_tick_engine import TimeTickEngine
from .helium_environment_translator import HeliumEnvironmentTranslator  # (the one I gave earlier)
import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Protocol, Callable, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
from collections import defaultdict, deque
import hashlib
import json

logger = logging.getLogger(__name__)

# ============================================================================
# Service Protocols (Enhanced)
# ============================================================================

class TokenServiceProtocol(Protocol):
    def get_system_summary(self) -> Dict[str, Any]: ...
    def get_account_summary(self, account_id: str) -> Dict[str, Any]: ...
    def reserve_tokens(self, account_id: str, amount: float, consumer: Any, tenant_id: str, priority: int) -> Tuple[bool, List[str]]: ...
    def generate_tokens(self, account_id: str, source: Any, **kwargs) -> List[Any]: ...
    def consume_tokens(self, token_ids: List[str], consumer: Any, operation_success: bool) -> float: ...
    def recover_tokens(self, token_ids: List[str], completion_percentage: float) -> float: ...
    def create_account(self, account_id: str) -> Any: ...

class GradientServiceProtocol(Protocol):
    def get_field_strengths(self) -> Dict[str, float]: ...
    def pump_field(self, field_id: str, amount: float, source: str) -> None: ...
    def discharge_field(self, field_id: str, amount: float) -> float: ...
    def get_dominant_field(self) -> Tuple[str, float]: ...
    def get_field_stats(self) -> Dict[str, Any]: ...
    def find_root_cause(self, anomaly_field: str, max_depth: int) -> Dict[str, Any]: ...
    def explain_gradient_state(self, field_id: str) -> Dict[str, Any]: ...
    def forecast(self, field_id: str, horizon_seconds: float) -> Dict[str, Any]: ...
    def get_forecast_summary(self) -> Dict[str, Any]: ...

class CompartmentServiceProtocol(Protocol):
    def find_best_compartment(self, expert_type: str, task_complexity: float) -> Any: ...
    def get_ecosystem_stats(self) -> Dict[str, Any]: ...
    def create_compartment(self, expert_type: str, expert_instance: Any, resources: Any, parent_id: str) -> Any: ...
    def decommission_compartment(self, compartment_id: str) -> Dict[str, Any]: ...

class BiomassServiceProtocol(Protocol):
    def store_task(self, task_data: Dict[str, Any], ecoatp_cost: float, guarantee: Any, deadline: Any, initial_tier: Any) -> Tuple[bool, Optional[str]]: ...
    def retrieve_task(self, token_id: str) -> Tuple[Optional[Dict[str, Any]], float]: ...
    def get_storage_stats(self) -> Dict[str, Any]: ...
    def simulate_storage_scenario(self, scenario: Dict[str, Any]) -> Dict[str, Any]: ...

# ============================================================================
# Event System (NEW)
# ============================================================================

@dataclass
class BioEvent:
    """Event for event-driven communication"""
    event_type: str
    source: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    data: Dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    priority: int = 0

class EventBroker:
    """
    Event-driven communication between bio-inspired services.
    
    Features:
    - Publish/subscribe pattern
    - Event filtering
    - Priority queuing
    - Event correlation
    """
    
    def __init__(self):
        self.subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self.event_history: deque = deque(maxlen=10000)
        self.event_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._lock = asyncio.Lock()
        self._running = True
        self._processor_task = None
        
        logger.info("Event Broker initialized")
    
    def subscribe(self, event_type: str, callback: Callable):
        """Subscribe to an event type"""
        self.subscribers[event_type].append(callback)
        logger.debug(f"Subscribed to {event_type}")
    
    def unsubscribe(self, event_type: str, callback: Callable):
        """Unsubscribe from an event type"""
        if event_type in self.subscribers:
            self.subscribers[event_type].remove(callback)
    
    async def publish(self, event: BioEvent):
        """Publish an event"""
        async with self._lock:
            await self.event_queue.put((event.priority, event))
            self.event_history.append(event)
            logger.debug(f"Event published: {event.event_type} from {event.source}")
    
    async def _process_events(self):
        """Process events from the queue"""
        while self._running:
            try:
                priority, event = await self.event_queue.get()
                
                if event.event_type in self.subscribers:
                    for callback in self.subscribers[event.event_type]:
                        try:
                            if asyncio.iscoroutinefunction(callback):
                                await callback(event)
                            else:
                                callback(event)
                        except Exception as e:
                            logger.error(f"Event callback error: {str(e)}")
                
                self.event_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Event processing error: {str(e)}")
    
    def start_processing(self):
        """Start the event processing loop"""
        if self._processor_task is None:
            self._processor_task = asyncio.create_task(self._process_events())
    
    def stop_processing(self):
        """Stop the event processing loop"""
        self._running = False
        if self._processor_task:
            self._processor_task.cancel()
    
    def get_event_history(self, event_type: Optional[str] = None, limit: int = 100) -> List[BioEvent]:
        """Get event history"""
        if event_type:
            return [e for e in self.event_history if e.event_type == event_type][-limit:]
        return list(self.event_history)[-limit:]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get event broker statistics"""
        return {
            'total_events': len(self.event_history),
            'subscribers': {k: len(v) for k, v in self.subscribers.items()},
            'queue_size': self.event_queue.qsize(),
            'is_running': self._running
        }

# ============================================================================
# Predictive Alert System (NEW)
# ============================================================================

@dataclass
class PredictiveAlert:
    """Predictive alert for bio-inspired system"""
    alert_id: str
    severity: str  # info, warning, critical
    category: str  # token, gradient, compartment, biomass, harvester
    message: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    predicted_time: Optional[datetime] = None
    confidence: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    acknowledged: bool = False
    resolved: bool = False

class PredictiveAlertSystem:
    """
    Predictive alert system for bio-inspired ecosystem.
    
    Features:
    - Pattern detection in metrics
    - Threshold-based alerting
    - Predictive alert generation
    - Alert escalation
    """
    
    def __init__(self, event_broker: Optional[EventBroker] = None):
        self.event_broker = event_broker
        self.alerts: List[PredictiveAlert] = []
        self.alert_history: deque = deque(maxlen=1000)
        self.thresholds = {
            'token_balance': {'warning': 200, 'critical': 50},
            'gradient_carbon': {'warning': 0.7, 'critical': 0.9},
            'gradient_helium': {'warning': 0.7, 'critical': 0.9},
            'compartment_health': {'warning': 0.4, 'critical': 0.2},
            'biomass_utilization': {'warning': 0.7, 'critical': 0.9}
        }
        self._lock = asyncio.Lock()
        
        # Register with event broker if provided
        if self.event_broker:
            self.event_broker.subscribe('token_balance_update', self._on_token_update)
            self.event_broker.subscribe('gradient_update', self._on_gradient_update)
        
        logger.info("Predictive Alert System initialized")
    
    async def _on_token_update(self, event: BioEvent):
        """Handle token balance update events"""
        balance = event.data.get('balance', 500)
        await self._check_threshold('token_balance', balance, event.data)
    
    async def _on_gradient_update(self, event: BioEvent):
        """Handle gradient update events"""
        field = event.data.get('field', 'carbon')
        strength = event.data.get('strength', 0.5)
        threshold_key = f'gradient_{field}'
        if threshold_key in self.thresholds:
            await self._check_threshold(threshold_key, strength, event.data)
    
    async def _check_threshold(self, metric: str, value: float, metadata: Dict):
        """Check if a threshold has been exceeded"""
        if metric not in self.thresholds:
            return
        
        thresholds = self.thresholds[metric]
        severity = None
        
        if value <= thresholds.get('critical', 0):
            severity = 'critical'
        elif value <= thresholds.get('warning', 0):
            severity = 'warning'
        
        if severity:
            alert_id = hashlib.md5(
                f"{metric}_{value}_{datetime.utcnow().timestamp()}".encode()
            ).hexdigest()[:12]
            
            alert = PredictiveAlert(
                alert_id=alert_id,
                severity=severity,
                category=metric,
                message=f"{metric} at {value:.3f} (threshold: {severity})",
                predicted_time=datetime.utcnow() + timedelta(minutes=5),
                confidence=0.7,
                metadata=metadata
            )
            
            async with self._lock:
                self.alerts.append(alert)
                self.alert_history.append(alert)
            
            # Publish alert event
            if self.event_broker:
                await self.event_broker.publish(BioEvent(
                    event_type='alert_generated',
                    source='predictive_alert_system',
                    data={'alert': alert.__dict__}
                ))
            
            logger.warning(f"Alert generated: {alert.message}")
    
    async def generate_predictive_alerts(self, metrics: Dict[str, float]) -> List[PredictiveAlert]:
        """Generate predictive alerts from metrics"""
        alerts = []
        
        # Token balance prediction
        if 'token_balance' in metrics and 'token_trend' in metrics:
            balance = metrics['token_balance']
            trend = metrics['token_trend']
            
            if trend < -0.1 and balance < 500:
                predicted_balance = balance + trend * 60  # 1 hour projection
                if predicted_balance < 100:
                    alert = PredictiveAlert(
                        alert_id=hashlib.md5(f"predict_token_{datetime.utcnow().timestamp()}".encode()).hexdigest()[:12],
                        severity='warning',
                        category='token',
                        message=f"Token balance predicted to drop below 100 in 1 hour",
                        predicted_time=datetime.utcnow() + timedelta(hours=1),
                        confidence=0.6,
                        metadata={'current': balance, 'trend': trend, 'predicted': predicted_balance}
                    )
                    alerts.append(alert)
        
        # Gradient prediction
        if 'gradient_carbon' in metrics and 'gradient_carbon_trend' in metrics:
            carbon = metrics['gradient_carbon']
            trend = metrics['gradient_carbon_trend']
            
            if trend > 0.1 and carbon > 0.5:
                predicted_carbon = carbon + trend * 60
                if predicted_carbon > 0.9:
                    alert = PredictiveAlert(
                        alert_id=hashlib.md5(f"predict_carbon_{datetime.utcnow().timestamp()}".encode()).hexdigest()[:12],
                        severity='critical',
                        category='gradient',
                        message=f"Carbon gradient predicted to reach critical level in 1 hour",
                        predicted_time=datetime.utcnow() + timedelta(hours=1),
                        confidence=0.65,
                        metadata={'current': carbon, 'trend': trend, 'predicted': predicted_carbon}
                    )
                    alerts.append(alert)
        
        return alerts
    
    def get_active_alerts(self, severity: Optional[str] = None) -> List[PredictiveAlert]:
        """Get active alerts"""
        alerts = [a for a in self.alerts if not a.resolved]
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
        return alerts
    
    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert"""
        for alert in self.alerts:
            if alert.alert_id == alert_id:
                alert.acknowledged = True
                return True
        return False
    
    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert"""
        for alert in self.alerts:
            if alert.alert_id == alert_id:
                alert.resolved = True
                return True
        return False
    
    def get_alert_stats(self) -> Dict[str, Any]:
        """Get alert statistics"""
        return {
            'total_alerts': len(self.alert_history),
            'active_alerts': len([a for a in self.alerts if not a.resolved]),
            'acknowledged': len([a for a in self.alerts if a.acknowledged]),
            'by_severity': {
                'critical': len([a for a in self.alerts if a.severity == 'critical']),
                'warning': len([a for a in self.alerts if a.severity == 'warning']),
                'info': len([a for a in self.alerts if a.severity == 'info'])
            },
            'by_category': {
                'token': len([a for a in self.alerts if a.category == 'token']),
                'gradient': len([a for a in self.alerts if a.category == 'gradient']),
                'compartment': len([a for a in self.alerts if a.category == 'compartment']),
                'biomass': len([a for a in self.alerts if a.category == 'biomass']),
                'harvester': len([a for a in self.alerts if a.category == 'harvester'])
            }
        }

# ============================================================================
# Cost-Benefit Analysis Engine (NEW)
# ============================================================================

@dataclass
class CostBenefitAnalysis:
    """Cost-benefit analysis result"""
    analysis_id: str
    scenario: str
    total_cost: float  # in Eco-ATP
    total_benefit: float  # in sustainability score
    net_value: float
    roi: float
    payback_period_hours: float
    recommendations: List[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.utcnow)

class CostBenefitEngine:
    """
    Cost-benefit analysis for bio-inspired ecosystem.
    
    Features:
    - Scenario analysis
    - ROI calculation
    - Payback period estimation
    - Comparative analysis
    """
    
    def __init__(self, token_manager=None):
        self.token_manager = token_manager
        self.analyses: List[CostBenefitAnalysis] = []
        self._lock = asyncio.Lock()
        
        # Cost models for different operations
        self.cost_models = {
            'token_generation': {'base_cost': 1.0, 'variable_cost': 0.1},
            'gradient_pumping': {'base_cost': 2.0, 'variable_cost': 0.2},
            'compartment_creation': {'base_cost': 5.0, 'variable_cost': 0.5},
            'biomass_storage': {'base_cost': 0.5, 'variable_cost': 0.05},
            'harvester_operation': {'base_cost': 0.3, 'variable_cost': 0.03}
        }
        
        # Benefit models
        self.benefit_models = {
            'token_generation': {'base_benefit': 1.0, 'variable_benefit': 0.2},
            'gradient_pumping': {'base_benefit': 1.5, 'variable_benefit': 0.3},
            'compartment_creation': {'base_benefit': 3.0, 'variable_benefit': 0.5},
            'biomass_storage': {'base_benefit': 0.8, 'variable_benefit': 0.1},
            'harvester_operation': {'base_benefit': 1.2, 'variable_benefit': 0.25}
        }
        
        logger.info("Cost-Benefit Engine initialized")
    
    async def analyze_scenario(
        self,
        scenario: str,
        parameters: Dict[str, Any]
    ) -> CostBenefitAnalysis:
        """Analyze a scenario with cost-benefit calculation"""
        async with self._lock:
            total_cost = 0.0
            total_benefit = 0.0
            
            for operation, amount in parameters.items():
                if operation in self.cost_models:
                    model = self.cost_models[operation]
                    cost = model['base_cost'] + model['variable_cost'] * amount
                    total_cost += cost
                
                if operation in self.benefit_models:
                    model = self.benefit_models[operation]
                    benefit = model['base_benefit'] + model['variable_benefit'] * amount
                    total_benefit += benefit
            
            net_value = total_benefit - total_cost
            roi = (total_benefit / max(total_cost, 0.001)) - 1
            
            # Estimate payback period (hours)
            if net_value > 0 and total_benefit > 0:
                payback_period = total_cost / (total_benefit / 24)  # Assume 24-hour benefit window
            else:
                payback_period = float('inf')
            
            analysis = CostBenefitAnalysis(
                analysis_id=hashlib.md5(f"{scenario}_{datetime.utcnow().timestamp()}".encode()).hexdigest()[:12],
                scenario=scenario,
                total_cost=total_cost,
                total_benefit=total_benefit,
                net_value=net_value,
                roi=roi,
                payback_period_hours=payback_period,
                recommendations=self._generate_recommendations(total_cost, total_benefit, roi)
            )
            
            self.analyses.append(analysis)
            return analysis
    
    def _generate_recommendations(self, cost: float, benefit: float, roi: float) -> List[str]:
        """Generate recommendations based on analysis"""
        recommendations = []
        
        if cost > benefit:
            recommendations.append("Reduce costs or increase benefits")
            if cost > 10:
                recommendations.append("Consider optimizing resource allocation")
        else:
            recommendations.append("Current configuration is cost-effective")
        
        if roi < 0.5:
            recommendations.append("Improve return on investment")
        
        return recommendations or ["Scenario is economically viable"]
    
    def get_best_scenario(self, scenarios: List[str]) -> Optional[str]:
        """Get the best scenario based on ROI"""
        best_analysis = None
        best_roi = -float('inf')
        
        for analysis in self.analyses:
            if analysis.scenario in scenarios:
                if analysis.roi > best_roi:
                    best_roi = analysis.roi
                    best_analysis = analysis
        
        return best_analysis.scenario if best_analysis else None
    
    def get_analysis_stats(self) -> Dict[str, Any]:
        """Get analysis statistics"""
        if not self.analyses:
            return {'total_analyses': 0}
        
        rois = [a.roi for a in self.analyses]
        return {
            'total_analyses': len(self.analyses),
            'average_roi': np.mean(rois),
            'max_roi': max(rois),
            'min_roi': min(rois),
            'best_scenario': max(self.analyses, key=lambda a: a.roi).scenario,
            'recent_analyses': [
                {'scenario': a.scenario, 'roi': a.roi, 'net_value': a.net_value}
                for a in self.analyses[-5:]
            ]
        }

# ============================================================================
# Workflow Orchestrator (NEW)
# ============================================================================

@dataclass
class WorkflowStep:
    """Step in a workflow"""
    step_id: str
    name: str
    service: str  # token, gradient, compartment, biomass, harvester
    action: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)
    retry_count: int = 0
    max_retries: int = 3
    timeout_seconds: float = 30.0
    status: str = 'pending'  # pending, running, completed, failed

class WorkflowOrchestrator:
    """
    Workflow orchestration across bio-inspired services.
    
    Features:
    - Multi-step workflow definition
    - Dependency management
    - Retry logic
    - Timeout handling
    - Status tracking
    """
    
    def __init__(self, bio_core):
        self.bio_core = bio_core
        self.workflows: Dict[str, List[WorkflowStep]] = {}
        self.workflow_status: Dict[str, str] = {}
        self._lock = asyncio.Lock()
        
        logger.info("Workflow Orchestrator initialized")
    
    def define_workflow(self, workflow_id: str, steps: List[Dict[str, Any]]):
        """Define a workflow with steps"""
        workflow_steps = []
        for i, step in enumerate(steps):
            workflow_steps.append(WorkflowStep(
                step_id=f"{workflow_id}_step_{i}",
                name=step.get('name', f'Step {i}'),
                service=step['service'],
                action=step['action'],
                parameters=step.get('parameters', {}),
                depends_on=step.get('depends_on', [])
            ))
        
        self.workflows[workflow_id] = workflow_steps
        self.workflow_status[workflow_id] = 'pending'
        
        logger.info(f"Workflow defined: {workflow_id} with {len(workflow_steps)} steps")
    
    async def execute_workflow(self, workflow_id: str) -> Dict[str, Any]:
        """Execute a workflow"""
        if workflow_id not in self.workflows:
            return {'status': 'error', 'message': 'Workflow not found'}
        
        steps = self.workflows[workflow_id]
        results = {}
        self.workflow_status[workflow_id] = 'running'
        
        for step in steps:
            # Check dependencies
            deps_met = all(
                results.get(dep, {}).get('status') == 'success'
                for dep in step.depends_on
            )
            
            if not deps_met:
                step.status = 'failed'
                self.workflow_status[workflow_id] = 'failed'
                return {
                    'status': 'failed',
                    'step': step.name,
                    'reason': 'Dependencies not met',
                    'results': results
                }
            
            # Execute step with retries
            for attempt in range(step.max_retries):
                step.status = 'running'
                try:
                    result = await self._execute_step(step)
                    step.status = 'completed'
                    results[step.name] = {'status': 'success', 'result': result}
                    break
                except asyncio.TimeoutError:
                    step.retry_count += 1
                    if step.retry_count >= step.max_retries:
                        step.status = 'failed'
                        self.workflow_status[workflow_id] = 'failed'
                        results[step.name] = {'status': 'failed', 'error': 'Timeout'}
                        break
                except Exception as e:
                    step.retry_count += 1
                    if step.retry_count >= step.max_retries:
                        step.status = 'failed'
                        self.workflow_status[workflow_id] = 'failed'
                        results[step.name] = {'status': 'failed', 'error': str(e)}
                        break
        
        if all(step.status == 'completed' for step in steps):
            self.workflow_status[workflow_id] = 'completed'
        elif self.workflow_status[workflow_id] != 'failed':
            self.workflow_status[workflow_id] = 'partial'
        
        return {
            'status': self.workflow_status[workflow_id],
            'results': results,
            'steps': [{'name': s.name, 'status': s.status} for s in steps]
        }
    
    async def _execute_step(self, step: WorkflowStep) -> Dict[str, Any]:
        """Execute a single workflow step"""
        service = getattr(self.bio_core, step.service, None)
        if not service:
            raise ValueError(f"Service {step.service} not found")
        
        action = getattr(service, step.action, None)
        if not action:
            raise ValueError(f"Action {step.action} not found on {step.service}")
        
        # Execute with timeout
        try:
            if asyncio.iscoroutinefunction(action):
                result = await asyncio.wait_for(
                    action(**step.parameters),
                    timeout=step.timeout_seconds
                )
            else:
                result = action(**step.parameters)
            
            return {'success': True, 'result': result}
        except asyncio.TimeoutError:
            raise
        except Exception as e:
            raise
    
    def get_workflow_status(self, workflow_id: str) -> Dict[str, Any]:
        """Get workflow status"""
        if workflow_id not in self.workflows:
            return {'status': 'not_found'}
        
        steps = self.workflows[workflow_id]
        return {
            'workflow_id': workflow_id,
            'status': self.workflow_status.get(workflow_id, 'pending'),
            'steps': [
                {
                    'name': s.name,
                    'status': s.status,
                    'retries': s.retry_count,
                    'max_retries': s.max_retries
                }
                for s in steps
            ]
        }
    
    def get_workflow_stats(self) -> Dict[str, Any]:
        """Get workflow statistics"""
        return {
            'total_workflows': len(self.workflows),
            'workflow_status': self.workflow_status,
            'completed': sum(1 for s in self.workflow_status.values() if s == 'completed'),
            'running': sum(1 for s in self.workflow_status.values() if s == 'running'),
            'failed': sum(1 for s in self.workflow_status.values() if s == 'failed'),
            'pending': sum(1 for s in self.workflow_status.values() if s == 'pending')
        }

# ============================================================================
# Anomaly Detection System (NEW)
# ============================================================================

@dataclass
class AnomalyDetectionResult:
    """Anomaly detection result"""
    metric: str
    value: float
    expected_range: Tuple[float, float]
    deviation: float
    severity: str  # info, warning, critical
    timestamp: datetime = field(default_factory=datetime.utcnow)
    confidence: float = 0.0

class AnomalyDetectionSystem:
    """
    Anomaly detection in bio-inspired ecosystem metrics.
    
    Features:
    - Statistical anomaly detection
    - Trend-based anomaly detection
    - Multi-metric correlation
    - Automated alerting
    """
    
    def __init__(self, event_broker: Optional[EventBroker] = None):
        self.event_broker = event_broker
        self.metric_history: Dict[str, List[float]] = defaultdict(lambda: deque(maxlen=1000))
        self.anomalies: List[AnomalyDetectionResult] = []
        self._lock = asyncio.Lock()
        self.zscore_threshold = 3.0
        self.trend_threshold = 0.2
        
        logger.info("Anomaly Detection System initialized")
    
    def record_metric(self, metric: str, value: float):
        """Record a metric value"""
        self.metric_history[metric].append(value)
    
    async def detect_anomalies(self, metric: str, value: float) -> Optional[AnomalyDetectionResult]:
        """Detect anomalies in a metric"""
        if metric not in self.metric_history or len(self.metric_history[metric]) < 10:
            return None
        
        history = list(self.metric_history[metric])[-50:]
        mean = np.mean(history)
        std = np.std(history)
        
        if std == 0:
            return None
        
        zscore = abs(value - mean) / std
        
        if zscore > self.zscore_threshold:
            severity = 'critical' if zscore > self.zscore_threshold * 1.5 else 'warning'
            anomaly = AnomalyDetectionResult(
                metric=metric,
                value=value,
                expected_range=(mean - 2*std, mean + 2*std),
                deviation=zscore,
                severity=severity,
                confidence=min(0.9, zscore / (self.zscore_threshold * 2))
            )
            
            async with self._lock:
                self.anomalies.append(anomaly)
            
            # Publish anomaly event
            if self.event_broker:
                await self.event_broker.publish(BioEvent(
                    event_type='anomaly_detected',
                    source='anomaly_detection_system',
                    data={'anomaly': anomaly.__dict__}
                ))
            
            logger.warning(f"Anomaly detected: {metric} = {value:.3f} (z-score: {zscore:.2f})")
            return anomaly
        
        return None
    
    async def detect_trend_anomaly(self, metric: str) -> Optional[AnomalyDetectionResult]:
        """Detect trend-based anomalies"""
        if metric not in self.metric_history or len(self.metric_history[metric]) < 20:
            return None
        
        history = list(self.metric_history[metric])[-20:]
        
        # Calculate trend
        x = np.arange(len(history))
        slope = np.polyfit(x, history, 1)[0]
        
        # Check if trend is significant
        if abs(slope) > self.trend_threshold:
            # Calculate expected range
            mean = np.mean(history)
            std = np.std(history)
            expected = mean + slope * len(history)
            
            anomaly = AnomalyDetectionResult(
                metric=metric,
                value=history[-1],
                expected_range=(expected - std, expected + std),
                deviation=abs(slope) / self.trend_threshold,
                severity='warning' if abs(slope) > self.trend_threshold * 1.5 else 'info',
                confidence=min(0.8, abs(slope) / (self.trend_threshold * 2))
            )
            
            async with self._lock:
                self.anomalies.append(anomaly)
            
            return anomaly
        
        return None
    
    def get_recent_anomalies(self, limit: int = 20) -> List[AnomalyDetectionResult]:
        """Get recent anomalies"""
        return self.anomalies[-limit:] if self.anomalies else []
    
    def get_anomaly_stats(self) -> Dict[str, Any]:
        """Get anomaly statistics"""
        return {
            'total_anomalies': len(self.anomalies),
            'by_severity': {
                'critical': len([a for a in self.anomalies if a.severity == 'critical']),
                'warning': len([a for a in self.anomalies if a.severity == 'warning']),
                'info': len([a for a in self.anomalies if a.severity == 'info'])
            },
            'by_metric': {
                metric: len([a for a in self.anomalies if a.metric == metric])
                for metric in set(a.metric for a in self.anomalies)
            },
            'recent_anomalies': [
                {'metric': a.metric, 'value': a.value, 'severity': a.severity, 'deviation': a.deviation}
                for a in self.anomalies[-5:]
            ]
        }

# ============================================================================
# Module Availability Checks
# ============================================================================

BIO_INSPIRED_AVAILABLE = True

try:
    from .eco_atp_currency import (
        EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer,
        TokenSupplyManager, PredictiveTokenAllocator, TokenServiceProtocol as TSP
    )
except ImportError:
    BIO_INSPIRED_AVAILABLE = False

try:
    from .proton_gradient_fields import HierarchicalGradientManager, GradientServiceProtocol as GSP
except ImportError:
    pass

try:
    from .atp_synthase_scheduler import ATPSynthaseScheduler
except ImportError:
    pass

try:
    from .chromatophore_compartments import HierarchicalCompartmentManager
except ImportError:
    pass

try:
    from .biomass_storage import BiomassStorage
except ImportError:
    pass

try:
    from .photosynthetic_harvester import PhotosyntheticHarvester
except ImportError:
    pass

# ============================================================================
# Enhanced Bio-Inspired Core (v6.0.0)
# ============================================================================

class EnhancedBioInspiredCore:
    """
    Enhanced Bio-Inspired Core v6.0.0 with event-driven communication,
    predictive alerts, cost-benefit analysis, workflow orchestration,
    and anomaly detection.
    """
    
    def __init__(self, enable_enhancements: bool = True):
        # Initialize exchange rate
        self.exchange_rate = DynamicExchangeRate()
        
        # Initialize core modules
        self._token_manager = EcoATPTokenManager(self.exchange_rate)
        self._gradient_manager = HierarchicalGradientManager()
        self._scheduler = ATPSynthaseScheduler(self._token_manager, self._gradient_manager)
        self._compartment_manager = HierarchicalCompartmentManager(self._token_manager)
        self._biomass_storage = BiomassStorage(self._token_manager)
        self._harvester = PhotosyntheticHarvester(self._token_manager)
        
        # Supply management and pre-allocation
        if enable_enhancements:
            self._supply_manager = TokenSupplyManager(self._token_manager)
            self._token_allocator = PredictiveTokenAllocator(self._token_manager)
        
        # Knowledge transfer
        from .knowledge_transfer import KnowledgeTransferManager
        self._knowledge_transfer = KnowledgeTransferManager()
        
        # Degradation management
        from .degradation_manager import DegradationManager
        self._degradation_manager = DegradationManager()
        
        # NEW: Event system
        self._event_broker = EventBroker()
        self._event_broker.start_processing()
        
        # NEW: Predictive alert system
        self._alert_system = PredictiveAlertSystem(self._event_broker)
        
        # NEW: Cost-benefit engine
        self._cost_benefit_engine = CostBenefitEngine(self._token_manager)
        
        # NEW: Workflow orchestrator
        self._workflow_orchestrator = WorkflowOrchestrator(self)
        
        # NEW: Anomaly detection system
        self._anomaly_detection = AnomalyDetectionSystem(self._event_broker)
        
        # API
        from .api import BioInspiredAPI
        self._api = BioInspiredAPI(self)
        
        # Wire degradation manager
        self._degradation_manager.update_metrics(
            token_balance=self._token_manager.get_system_summary().get('total_balance', 500)
        )
        self._degradation_manager.register_callback(self._on_tier_change)
        
        # Subscribe to events
        self._event_broker.subscribe('token_balance_update', self._on_token_balance_update)
        self._event_broker.subscribe('gradient_update', self._on_gradient_update_event)
        
        # Start monitoring
        asyncio.create_task(self._enhanced_monitoring_loop())
        asyncio.create_task(self._anomaly_detection_loop())
        
        logger.info("Enhanced Bio-Inspired Core v6.0.0 initialized with protocol-based DI and all enhancements")
    
    # ========================================================================
    # Event Handlers
    # ========================================================================
    
    async def _on_token_balance_update(self, event: BioEvent):
        """Handle token balance update events"""
        balance = event.data.get('balance', 500)
        # Record for anomaly detection
        self._anomaly_detection.record_metric('token_balance', balance)
        # Check for anomalies
        await self._anomaly_detection.detect_anomalies('token_balance', balance)
    
    async def _on_gradient_update_event(self, event: BioEvent):
        """Handle gradient update events"""
        field = event.data.get('field', 'carbon')
        strength = event.data.get('strength', 0.5)
        self._anomaly_detection.record_metric(f'gradient_{field}', strength)
        await self._anomaly_detection.detect_anomalies(f'gradient_{field}', strength)
    
    async def _on_tier_change(self, old_tier, new_tier, policies):
        logger.warning(f"Tier change: {old_tier.name} → {new_tier.name}")
        # Publish tier change event
        await self._event_broker.publish(BioEvent(
            event_type='tier_change',
            source='degradation_manager',
            data={'old_tier': old_tier.name, 'new_tier': new_tier.name}
        ))
    
    # ========================================================================
    # Enhanced Monitoring Loop
    # ========================================================================
    
    async def _enhanced_monitoring_loop(self):
        while True:
            try:
                summary = self._token_manager.get_system_summary()
                gradients = self._gradient_manager.get_field_strengths()
                
                # Update degradation manager
                self._degradation_manager.update_metrics(
                    token_balance=summary.get('total_balance', 500),
                    carbon_gradient=gradients.get('carbon', 0.5),
                    compartment_health=self._get_avg_compartment_health(),
                    harvester_activity=self._harvester.total_harvested if self._harvester else 0
                )
                
                # Record gradient measurements
                for field_id, strength in gradients.items():
                    self._gradient_manager.record_measurement(field_id, strength)
                    
                    # Publish gradient update event
                    await self._event_broker.publish(BioEvent(
                        event_type='gradient_update',
                        source='monitoring',
                        data={'field': field_id, 'strength': strength}
                    ))
                
                # Record token balance
                await self._event_broker.publish(BioEvent(
                    event_type='token_balance_update',
                    source='monitoring',
                    data={'balance': summary.get('total_balance', 500)}
                ))
                
                # Generate predictive alerts
                metrics = {
                    'token_balance': summary.get('total_balance', 500),
                    'gradient_carbon': gradients.get('carbon', 0.5),
                    'gradient_carbon_trend': self._gradient_manager.get_field_stats().get('carbon', {}).get('trend', 0)
                }
                alerts = await self._alert_system.generate_predictive_alerts(metrics)
                for alert in alerts:
                    logger.warning(f"Predictive alert: {alert.message}")
                
                await asyncio.sleep(15)
            except Exception as e:
                logger.error(f"Monitoring error: {str(e)}")
                await asyncio.sleep(30)
    
    async def _anomaly_detection_loop(self):
        """Background loop for anomaly detection"""
        while True:
            try:
                # Check for trend anomalies in key metrics
                for metric in ['token_balance', 'gradient_carbon', 'gradient_helium']:
                    await self._anomaly_detection.detect_trend_anomaly(metric)
                
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Anomaly detection loop error: {str(e)}")
                await asyncio.sleep(120)
    
    def _get_avg_compartment_health(self) -> float:
        if not self._compartment_manager:
            return 0.5
        compartments = self._compartment_manager.compartments
        if not compartments:
            return 0.5
        return np.mean([c.health_score for c in compartments.values()])
    
    # ========================================================================
    # Protocol-Compliant Service Accessors
    # ========================================================================
    
    @property
    def token_service(self) -> TokenServiceProtocol:
        return self._token_manager
    
    @property
    def gradient_service(self) -> GradientServiceProtocol:
        return self._gradient_manager
    
    @property
    def compartment_service(self) -> CompartmentServiceProtocol:
        return self._compartment_manager
    
    @property
    def biomass_service(self) -> BiomassServiceProtocol:
        return self._biomass_storage
    
    # Legacy accessors
    @property
    def token_manager(self): return self._token_manager
    @property
    def gradient_manager(self): return self._gradient_manager
    @property
    def scheduler(self): return self._scheduler
    @property
    def compartment_manager(self): return self._compartment_manager
    @property
    def biomass_storage(self): return self._biomass_storage
    @property
    def harvester(self): return self._harvester
    @property
    def supply_manager(self): return self._supply_manager if hasattr(self, '_supply_manager') else None
    @property
    def token_allocator(self): return self._token_allocator if hasattr(self, '_token_allocator') else None
    @property
    def knowledge_transfer(self): return self._knowledge_transfer
    @property
    def degradation_manager(self): return self._degradation_manager
    @property
    def api(self): return self._api
    
    # NEW accessors
    @property
    def event_broker(self): return self._event_broker
    @property
    def alert_system(self): return self._alert_system
    @property
    def cost_benefit_engine(self): return self._cost_benefit_engine
    @property
    def workflow_orchestrator(self): return self._workflow_orchestrator
    @property
    def anomaly_detection(self): return self._anomaly_detection
    
    # ========================================================================
    # Enhanced System Status and Reporting
    # ========================================================================
    
    def get_system_status(self) -> Dict[str, Any]:
        status = {
            'token_economy': self._token_manager.get_system_summary(),
            'gradients': self._gradient_manager.get_field_stats(),
            'gradient_forecasts': self._gradient_manager.get_forecast_summary(),
            'scheduler': self._scheduler.get_scheduler_stats() if self._scheduler else {},
            'compartments': self._compartment_manager.get_ecosystem_stats() if self._compartment_manager else {},
            'biomass': self._biomass_storage.get_storage_stats() if self._biomass_storage else {},
            'harvester': self._harvester.get_harvesting_stats() if self._harvester else {},
            'degradation': self._degradation_manager.get_tier_status() if hasattr(self, '_degradation_manager') else {},
            'knowledge': self._knowledge_transfer.get_knowledge_summary() if hasattr(self, '_knowledge_transfer') else {},
            # NEW
            'event_broker': self._event_broker.get_stats(),
            'alerts': self._alert_system.get_alert_stats(),
            'anomalies': self._anomaly_detection.get_anomaly_stats(),
            'workflows': self._workflow_orchestrator.get_workflow_stats(),
            'cost_benefit': self._cost_benefit_engine.get_analysis_stats()
        }
        
        if hasattr(self, '_supply_manager'):
            status['token_economy']['supply_management'] = self._supply_manager.get_economic_indicators()
        if hasattr(self, '_token_allocator'):
            status['token_economy']['pre_allocation'] = self._token_allocator.get_cache_stats()
        
        return status
    
    def get_economic_report(self) -> Dict[str, Any]:
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'token_economy': self._token_manager.get_system_summary()
        }
        
        if hasattr(self, '_supply_manager'):
            report['supply_management'] = self._supply_manager.get_economic_indicators()
        if hasattr(self, '_token_allocator'):
            report['pre_allocation'] = self._token_allocator.get_cache_stats()
        
        # NEW: Add cost-benefit analysis
        report['cost_benefit'] = self._cost_benefit_engine.get_analysis_stats()
        
        indicators = report.get('supply_management', {})
        utilization = indicators.get('utilization', 0.5)
        inflation = indicators.get('inflation_pressure', 0)
        
        if 0.6 < utilization < 0.9 and abs(inflation) < 0.2:
            report['health'] = 'healthy'
        elif utilization < 0.4:
            report['health'] = 'deflationary'
        elif utilization > 0.95:
            report['health'] = 'inflationary'
        else:
            report['health'] = 'stable'
        
        recs = []
        if utilization < 0.4:
            recs.append("Economy under-utilized. Increase task throughput.")
        if utilization > 0.95:
            recs.append("Economy over-heating. Add capacity or reduce load.")
        if inflation > 0.3:
            recs.append("High inflation pressure. Token burning recommended.")
        
        # Add cost-benefit recommendations
        if report.get('cost_benefit', {}).get('total_analyses', 0) > 0:
            best_scenario = self._cost_benefit_engine.get_best_scenario(
                ['token_generation', 'gradient_pumping', 'compartment_creation']
            )
            if best_scenario:
                recs.append(f"Best cost-benefit scenario: {best_scenario}")
        
        report['recommendations'] = recs if recs else ["Economy is healthy."]
        
        return report
    
    def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        ecoatp_required = task.get('complexity', 0.5) * 10
        
        if hasattr(self, '_token_allocator'):
            success, _ = self._token_allocator.get_tokens('task_processor', ecoatp_required)
            if success:
                self._token_allocator.record_demand('task_processor', ecoatp_required)
        else:
            success, _ = self._token_manager.reserve_tokens('task_processor', ecoatp_required, EcoATPConsumer.EXPERT_EXECUTION)
        
        if not success:
            if self._biomass_storage:
                stored, token_id = self._biomass_storage.store_task(task_data=task, ecoatp_cost=ecoatp_required)
                return {'success': True, 'status': 'stored', 'biomass_token': token_id}
            return {'success': False, 'reason': 'Insufficient tokens'}
        
        return {'success': True, 'task_id': task.get('task_id', 'unknown')}
    
    # ========================================================================
    # NEW: Workflow Management
    # ========================================================================
    
    def define_standard_workflows(self):
        """Define standard bio-inspired workflows"""
        # Token generation and distribution workflow
        self._workflow_orchestrator.define_workflow(
            'token_generation',
            [
                {'name': 'Harvest Energy', 'service': 'harvester', 'action': 'harvest_energy', 'parameters': {'amount': 10}},
                {'name': 'Generate Tokens', 'service': 'token_manager', 'action': 'generate_tokens', 
                 'parameters': {'account_id': 'system', 'source': 'harvest', 'num_tokens': 10}},
                {'name': 'Distribute Tokens', 'service': 'token_manager', 'action': 'reserve_tokens',
                 'parameters': {'account_id': 'system', 'amount': 5, 'consumer': 'workflow'}},
                {'name': 'Update Gradients', 'service': 'gradient_manager', 'action': 'pump_field',
                 'parameters': {'field_id': 'opportunity', 'amount': 0.1, 'source': 'token_generation'}}
            ]
        )
        
        # Compartment provisioning workflow
        self._workflow_orchestrator.define_workflow(
            'compartment_provisioning',
            [
                {'name': 'Check Resources', 'service': 'compartment_manager', 'action': 'find_best_compartment',
                 'parameters': {'expert_type': 'data', 'task_complexity': 0.5}},
                {'name': 'Allocate Tokens', 'service': 'token_manager', 'action': 'reserve_tokens',
                 'parameters': {'account_id': 'system', 'amount': 10, 'consumer': 'compartment'}},
                {'name': 'Create Compartment', 'service': 'compartment_manager', 'action': 'create_compartment',
                 'parameters': {'expert_type': 'data', 'expert_instance': None, 'resources': {'tokens': 10}}},
                {'name': 'Update Biomass', 'service': 'biomass_storage', 'action': 'store_task',
                 'parameters': {'task_data': {'type': 'compartment_creation'}, 'ecoatp_cost': 2}}
            ]
        )
        
        logger.info("Standard workflows defined")
    
    async def run_workflow(self, workflow_id: str) -> Dict[str, Any]:
        """Run a workflow"""
        return await self._workflow_orchestrator.execute_workflow(workflow_id)
    
    # ========================================================================
    # NEW: Cost-Benefit Analysis
    # ========================================================================
    
    async def analyze_cost_benefit(self, scenario: str, parameters: Dict[str, Any]) -> CostBenefitAnalysis:
        """Run a cost-benefit analysis"""
        return await self._cost_benefit_engine.analyze_scenario(scenario, parameters)
    
    # ========================================================================
    # NEW: Alert Management
    # ========================================================================
    
    def get_active_alerts(self, severity: Optional[str] = None) -> List[PredictiveAlert]:
        """Get active alerts"""
        return self._alert_system.get_active_alerts(severity)
    
    def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert"""
        return self._alert_system.acknowledge_alert(alert_id)
    
    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert"""
        return self._alert_system.resolve_alert(alert_id)
    
    # ========================================================================
    # NEW: Anomaly Detection
    # ========================================================================
    
    def get_recent_anomalies(self, limit: int = 20) -> List[AnomalyDetectionResult]:
        """Get recent anomalies"""
        return self._anomaly_detection.get_recent_anomalies(limit)
    
    # ========================================================================
    # Shutdown
    # ========================================================================
    
    async def shutdown(self):
        """Graceful shutdown of all components"""
        logger.info("Shutting down Enhanced Bio-Inspired Core")
        
        # Stop event processing
        self._event_broker.stop_processing()
        
        # Close any pending tasks
        if self._token_manager:
            await self._token_manager.close()
        if self._gradient_manager:
            await self._gradient_manager.close()
        
        logger.info("Shutdown complete")

# ============================================================================
# Convenience Functions
# ============================================================================

def create_metabolic_ecosystem(enable_bio: bool = True) -> EnhancedBioInspiredCore:
    return EnhancedBioInspiredCore(enable_enhancements=enable_bio)

def create_minimal_ecosystem() -> EnhancedBioInspiredCore:
    return EnhancedBioInspiredCore(enable_enhancements=False)

class EnhancedBioInspiredCore:
    def __init__(self, enable_enhancements: bool = True, 
                 csv_path: str = "helium_timeseries_realistic_2020_2026.csv",
                 quantum_graph=None):   # <-- pass your quantum graph object here
        # ... existing initialization ...
        
        # NEW: Quantum Bridge
        self._quantum_bridge = QuantumBridge(self._gradient_manager, quantum_graph)
        
        # NEW: TimeTickEngine (will use the translator and harvester)
        self._translator = HeliumEnvironmentTranslator(csv_path)  # or pass the translator instance
        self._tick_engine = TimeTickEngine(
            csv_path=csv_path,
            harvester=self._harvester,
            translator_class=HeliumEnvironmentTranslator   # pass class, not instance
        )
        
        # Optionally start the simulation in a background task
        if enable_enhancements:
            asyncio.create_task(self._run_simulation_loop())
        
        logger.info("EnhancedBioInspiredCore now includes QuantumBridge and TimeTickEngine")
    
    async def _run_simulation_loop(self):
        """Run the tick engine in the background (example: run once)."""
        await self._tick_engine.run_simulation(
            tick_interval_seconds=0.1,
            post_tick_callback=self._on_tick
        )
    
    async def _on_tick(self, idx: int, row: pd.Series, harvest_result: Dict[str, Any]):
        """Callback after each tick: update quantum graph with latest gradients."""
        # Push current gradients to the quantum graph
        self._quantum_bridge.apply_to_quantum_graph()
        # Optionally log or alert
