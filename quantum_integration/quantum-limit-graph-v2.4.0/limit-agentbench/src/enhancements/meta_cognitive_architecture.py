# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/meta_cognitive_architecture.py
# Enhanced to consume expert_metrics.py analytics

"""
Enhanced Meta-Cognitive Architecture with Expert Metrics Integration
Version: 2.0.0

Now consumes real-time analytics from expert_metrics.py for:
- Anomaly-driven reflection triggers
- SLO-aware strategy adaptation
- Health-score-based routing preference updates
- Predictive analytics integration
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import deque
import numpy as np

logger = logging.getLogger(__name__)

# ============================================================================
# Metrics Bridge Interface
# ============================================================================

class MetricsBridge:
    """
    Bridge between meta-cognitive architecture and expert metrics.
    
    Enables meta-cognition to consume real-time analytics from expert_metrics.py.
    """
    
    def __init__(self):
        self.metrics_collector = None  # Will be injected
        self.anomaly_callbacks: List[Callable] = []
        self.slo_callbacks: List[Callable] = []
        self.health_callbacks: List[Callable] = []
        self.prediction_callbacks: List[Callable] = []
        
        # Cached metrics state
        self.last_health_scores: Dict[str, float] = {}
        self.last_slo_status: Dict[str, Any] = {}
        self.last_anomalies: List[Dict] = []
        self.last_predictions: Dict[str, Any] = {}
        
        # Polling interval
        self.poll_interval_seconds = 5.0
        
        logger.info("MetricsBridge initialized for meta-cognitive integration")
    
    def inject_metrics_collector(self, collector: Any):
        """Inject the expert metrics collector"""
        self.metrics_collector = collector
        logger.info("Metrics collector injected into meta-cognitive bridge")
    
    def on_anomaly_detected(self, callback: Callable):
        """Register callback for anomaly events"""
        self.anomaly_callbacks.append(callback)
    
    def on_slo_breach(self, callback: Callable):
        """Register callback for SLO breaches"""
        self.slo_callbacks.append(callback)
    
    def on_health_change(self, callback: Callable):
        """Register callback for health score changes"""
        self.health_callbacks.append(callback)
    
    async def poll_metrics(self):
        """Poll latest metrics from collector"""
        if not self.metrics_collector:
            return
        
        try:
            # Get health scores
            health_scores = self.metrics_collector.get_health_scores()
            
            # Detect health changes
            for expert_id, score in health_scores.items():
                old_score = self.last_health_scores.get(expert_id, score)
                if abs(score - old_score) > 0.1:  # Significant change
                    for callback in self.health_callbacks:
                        await callback(expert_id, old_score, score)
            
            self.last_health_scores = health_scores
            
            # Get SLO status
            if hasattr(self.metrics_collector, 'get_slo_status'):
                slo_status = self.metrics_collector.get_slo_status()
                
                for slo_id, status in slo_status.items():
                    old_status = self.last_slo_status.get(slo_id, {})
                    if status.get('status') != old_status.get('status'):
                        if status.get('status') == 'breached':
                            for callback in self.slo_callbacks:
                                await callback(slo_id, status)
                
                self.last_slo_status = slo_status
            
            # Get anomalies
            if hasattr(self.metrics_collector, 'anomaly_detector'):
                anomaly_stats = self.metrics_collector.anomaly_detector.get_detection_stats()
                recent_anomalies = anomaly_stats.get('recent_detections', [])
                
                new_anomalies = [
                    a for a in recent_anomalies
                    if a not in self.last_anomalies
                ]
                
                for anomaly in new_anomalies:
                    for callback in self.anomaly_callbacks:
                        await callback(anomaly)
                
                self.last_anomalies = recent_anomalies
            
            # Get predictions
            if hasattr(self.metrics_collector, 'get_predictions'):
                predictions = self.metrics_collector.get_predictions()
                self.last_predictions = predictions
                
                for expert_id, pred in predictions.items():
                    if pred.get('trend') == 'degrading':
                        for callback in self.prediction_callbacks:
                            await callback(expert_id, pred)
            
        except Exception as e:
            logger.error(f"Metrics polling error: {str(e)}")
    
    def get_expert_health(self, expert_id: str) -> float:
        """Get current health score for expert"""
        return self.last_health_scores.get(expert_id, 0.5)
    
    def get_slo_compliance(self) -> Dict[str, str]:
        """Get SLO compliance status"""
        return {
            slo_id: status.get('status', 'unknown')
            for slo_id, status in self.last_slo_status.items()
        }
    
    def get_predictions(self) -> Dict[str, Any]:
        """Get predictive analytics"""
        return self.last_predictions.copy()


# ============================================================================
# Enhanced Meta-Cognitive State
# ============================================================================

@dataclass
class EnhancedMetaCognitiveState:
    """Enhanced meta-cognitive state with metrics awareness"""
    
    # Core state
    confidence: float = 0.5
    uncertainty: float = 0.5
    learning_progress: float = 0.0
    
    # Budget tracking
    carbon_budget_remaining: float = 1.0
    helium_budget_remaining: float = 1.0
    latency_budget_ms: float = 1000.0
    
    # Performance tracking
    historical_success_rate: float = 0.9
    recent_rewards: List[float] = field(default_factory=list)
    
    # Metrics-aware state (NEW)
    expert_health_scores: Dict[str, float] = field(default_factory=dict)
    active_anomalies: List[Dict] = field(default_factory=list)
    slo_compliance: Dict[str, str] = field(default_factory=dict)
    degradation_warnings: List[str] = field(default_factory=list)
    predicted_degradation: Dict[str, Any] = field(default_factory=dict)
    
    # Reflection state
    reflection_notes: List[str] = field(default_factory=list)
    last_reflection_time: Optional[datetime] = None
    reflection_count: int = 0
    
    # Strategy adaptation
    preferred_experts: List[str] = field(default_factory=list)
    avoided_experts: List[str] = field(default_factory=list)
    strategy_effectiveness: Dict[str, float] = field(default_factory=dict)
    
    def add_reflection(self, note: str):
        """Add reflection note with timestamp"""
        self.reflection_notes.append(
            f"[{datetime.utcnow().isoformat()}] {note}"
        )
        self.reflection_count += 1
        self.last_reflection_time = datetime.utcnow()
        
        # Keep only last 100 reflections
        if len(self.reflection_notes) > 100:
            self.reflection_notes = self.reflection_notes[-100:]
    
    def update_from_metrics(self, bridge: MetricsBridge):
        """Update state from metrics bridge"""
        self.expert_health_scores = bridge.last_health_scores.copy()
        self.slo_compliance = bridge.get_slo_compliance()
        self.predicted_degradation = bridge.get_predictions()
        
        # Check for degradation warnings
        self.degradation_warnings = []
        for expert_id, health in self.expert_health_scores.items():
            if health < 0.3:
                self.degradation_warnings.append(
                    f"Expert {expert_id} critically degraded (health: {health:.2f})"
                )
            elif health < 0.5:
                self.degradation_warnings.append(
                    f"Expert {expert_id} showing degradation (health: {health:.2f})"
                )
        
        # Check SLO compliance
        for slo_id, status in self.slo_compliance.items():
            if status == 'breached':
                self.degradation_warnings.append(f"SLO {slo_id} breached")
            elif status == 'at_risk':
                self.degradation_warnings.append(f"SLO {slo_id} at risk")


# ============================================================================
# Enhanced Meta-Cognitive Architecture
# ============================================================================

class EnhancedMetaCognitiveArchitecture:
    """
    Enhanced Meta-Cognitive Architecture with full metrics integration.
    
    Features:
    - Real-time anomaly-driven reflection
    - SLO-aware strategy adaptation
    - Health-score-based expert preference updates
    - Predictive degradation prevention
    - Metrics-bridged decision making
    """
    
    def __init__(
        self,
        metrics_collector: Optional[Any] = None,
        enable_metrics_integration: bool = True,
        reflection_threshold: float = 0.3,
        adaptation_rate: float = 0.1
    ):
        self.enable_metrics_integration = enable_metrics_integration
        self.reflection_threshold = reflection_threshold
        self.adaptation_rate = adaptation_rate
        
        # Initialize metrics bridge
        self.metrics_bridge = MetricsBridge()
        
        # Inject metrics collector if provided
        if metrics_collector:
            self.metrics_bridge.inject_metrics_collector(metrics_collector)
        
        # Initialize state
        self.state = EnhancedMetaCognitiveState()
        
        # Register callbacks
        self._register_metrics_callbacks()
        
        # Reflection triggers
        self.reflection_triggers = {
            'anomaly_detected': self._reflect_on_anomaly,
            'slo_breached': self._reflect_on_slo_breach,
            'health_degraded': self._reflect_on_health_change,
            'prediction_warning': self._reflect_on_prediction,
            'performance_drop': self._reflect_on_performance,
            'budget_low': self._reflect_on_budget
        }
        
        # Performance history for trend detection
        self.performance_window: deque = deque(maxlen=100)
        
        # Start background tasks
        self._start_background_tasks()
        
        logger.info(
            "Enhanced Meta-Cognitive Architecture initialized: "
            f"metrics_integration={enable_metrics_integration}"
        )
    
    def _register_metrics_callbacks(self):
        """Register callbacks for metrics events"""
        if not self.enable_metrics_integration:
            return
        
        self.metrics_bridge.on_anomaly_detected(self._on_anomaly_detected)
        self.metrics_bridge.on_slo_breach(self._on_slo_breached)
        self.metrics_bridge.on_health_change(self._on_health_changed)
        self.metrics_bridge.on_health_change(self._on_prediction_warning)
    
    def _start_background_tasks(self):
        """Start background polling tasks"""
        if self.enable_metrics_integration:
            asyncio.create_task(self._metrics_polling_loop())
        
        asyncio.create_task(self._reflection_loop())
    
    async def _metrics_polling_loop(self):
        """Poll metrics bridge periodically"""
        while True:
            try:
                await self.metrics_bridge.poll_metrics()
                self.state.update_from_metrics(self.metrics_bridge)
                await asyncio.sleep(self.metrics_bridge.poll_interval_seconds)
            except Exception as e:
                logger.error(f"Metrics polling error: {str(e)}")
                await asyncio.sleep(30)
    
    async def _reflection_loop(self):
        """Periodic reflection loop"""
        while True:
            try:
                # Check if reflection is needed
                if self._should_reflect():
                    await self._trigger_reflection()
                
                await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"Reflection loop error: {str(e)}")
                await asyncio.sleep(30)
    
    def _should_reflect(self) -> bool:
        """Determine if reflection should be triggered"""
        # Reflect if there are active anomalies
        if self.state.active_anomalies:
            return True
        
        # Reflect if SLOs are breached
        if 'breached' in self.state.slo_compliance.values():
            return True
        
        # Reflect if degradation warnings exist
        if self.state.degradation_warnings:
            return True
        
        # Reflect if performance dropped significantly
        if self.performance_window:
            recent = list(self.performance_window)[-10:]
            if recent and np.mean(recent) < self.reflection_threshold:
                return True
        
        # Reflect periodically (every 5 minutes)
        if self.state.last_reflection_time:
            elapsed = (datetime.utcnow() - self.state.last_reflection_time).total_seconds()
            if elapsed > 300:
                return True
        
        return False
    
    async def _trigger_reflection(self):
        """Trigger comprehensive reflection"""
        self.state.add_reflection("Automated reflection triggered")
        
        # Check each trigger condition
        if self.state.active_anomalies:
            await self.reflection_triggers['anomaly_detected']()
        
        breached_slos = [
            slo_id for slo_id, status in self.state.slo_compliance.items()
            if status == 'breached'
        ]
        if breached_slos:
            await self.reflection_triggers['slo_breached'](breached_slos)
        
        if self.state.degradation_warnings:
            await self.reflection_triggers['health_degraded'](
                self.state.degradation_warnings
            )
        
        if self.state.predicted_degradation:
            degrading = [
                eid for eid, pred in self.state.predicted_degradation.items()
                if pred.get('trend') == 'degrading'
            ]
            if degrading:
                await self.reflection_triggers['prediction_warning'](degrading)
        
        # Check budgets
        if self.state.carbon_budget_remaining < 0.1:
            await self.reflection_triggers['budget_low']('carbon')
        if self.state.helium_budget_remaining < 0.1:
            await self.reflection_triggers['budget_low']('helium')
        
        logger.info(
            f"Reflection complete: {self.state.reflection_count} total reflections"
        )
    
    # ========================================================================
    # Metrics Event Handlers
    # ========================================================================
    
    async def _on_anomaly_detected(self, anomaly: Dict[str, Any]):
        """Handle detected anomaly"""
        self.state.active_anomalies.append(anomaly)
        
        # Keep only recent anomalies
        if len(self.state.active_anomalies) > 50:
            self.state.active_anomalies = self.state.active_anomalies[-50:]
        
        logger.warning(
            f"Anomaly detected: {anomaly.get('metric')} - "
            f"{anomaly.get('type')} (severity: {anomaly.get('severity')})"
        )
        
        # Immediate action for critical anomalies
        if anomaly.get('severity') == 'critical':
            await self._take_immediate_action(anomaly)
    
    async def _on_slo_breached(self, slo_id: str, status: Dict[str, Any]):
        """Handle SLO breach"""
        logger.critical(f"SLO breached: {slo_id} - {status}")
        
        # Update state
        self.state.slo_compliance[slo_id] = 'breached'
        self.state.add_reflection(f"SLO {slo_id} breached: {status}")
    
    async def _on_health_changed(
        self,
        expert_id: str,
        old_score: float,
        new_score: float
    ):
        """Handle health score change"""
        if new_score < old_score:
            direction = "decreased"
            if new_score < 0.3:
                severity = "CRITICAL"
            elif new_score < 0.5:
                severity = "WARNING"
            else:
                severity = "INFO"
        else:
            direction = "increased"
            severity = "INFO"
        
        logger.log(
            logging.WARNING if severity != "INFO" else logging.INFO,
            f"Expert {expert_id} health {direction}: "
            f"{old_score:.2f} -> {new_score:.2f} [{severity}]"
        )
        
        # Update expert preferences based on health
        if new_score < 0.3:
            if expert_id not in self.state.avoided_experts:
                self.state.avoided_experts.append(expert_id)
                self.state.add_reflection(
                    f"Added {expert_id} to avoided experts (health: {new_score:.2f})"
                )
        elif new_score > 0.7 and expert_id in self.state.avoided_experts:
            self.state.avoided_experts.remove(expert_id)
            self.state.add_reflection(
                f"Removed {expert_id} from avoided experts (health: {new_score:.2f})"
            )
    
    async def _on_prediction_warning(self, expert_id: str, prediction: Dict[str, Any]):
        """Handle prediction warning"""
        logger.warning(
            f"Predicted degradation for expert {expert_id}: "
            f"trend={prediction.get('trend')}, "
            f"confidence={prediction.get('confidence', 0):.2f}"
        )
        
        self.state.add_reflection(
            f"Predicted degradation for {expert_id}: {prediction.get('trend')}"
        )
    
    # ========================================================================
    # Reflection Handlers
    # ========================================================================
    
    async def _reflect_on_anomaly(self):
        """Reflect on detected anomalies"""
        anomalies = self.state.active_anomalies[-5:]  # Last 5 anomalies
        
        # Group by metric
        by_metric = {}
        for a in anomalies:
            metric = a.get('metric', 'unknown')
            if metric not in by_metric:
                by_metric[metric] = []
            by_metric[metric].append(a)
        
        for metric, metric_anomalies in by_metric.items():
            if len(metric_anomalies) >= 3:
                self.state.add_reflection(
                    f"Pattern detected: {len(metric_anomalies)} anomalies in {metric}. "
                    f"Consider adjusting {metric}-related parameters."
                )
                
                # Update strategy effectiveness
                current_strategy = self._infer_current_strategy()
                if current_strategy:
                    self.state.strategy_effectiveness[current_strategy] = max(
                        0, self.state.strategy_effectiveness.get(current_strategy, 0.5) - 0.1
                    )
    
    async def _reflect_on_slo_breach(self, breached_slos: List[str]):
        """Reflect on SLO breaches"""
        for slo_id in breached_slos:
            self.state.add_reflection(
                f"SLO {slo_id} breached. Reviewing routing strategy..."
            )
            
            # Increase exploration to find better routing
            self.state.confidence = max(0.1, self.state.confidence - 0.1)
            self.state.uncertainty = min(0.9, self.state.uncertainty + 0.1)
    
    async def _reflect_on_health_change(self, warnings: List[str]):
        """Reflect on health degradation"""
        for warning in warnings[:3]:  # Top 3 warnings
            self.state.add_reflection(f"Health concern: {warning}")
        
        # Adjust routing preferences
        if len(warnings) >= 3:
            self.state.add_reflection(
                "Multiple health warnings detected. Increasing routing exploration."
            )
            self.state.confidence = max(0.1, self.state.confidence - 0.05)
    
    async def _reflect_on_prediction(self, degrading_experts: List[str]):
        """Reflect on predictive warnings"""
        for expert_id in degrading_experts:
            self.state.add_reflection(
                f"Proactive: {expert_id} predicted to degrade. "
                f"Preparing alternative experts."
            )
            
            # Pre-warm alternative experts
            if expert_id not in self.state.preferred_experts:
                self.state.preferred_experts = [
                    e for e in self.state.preferred_experts if e != expert_id
                ]
    
    async def _reflect_on_performance(self):
        """Reflect on performance drop"""
        recent = list(self.performance_window)[-20:]
        if recent:
            avg = np.mean(recent)
            self.state.add_reflection(
                f"Performance drop detected: avg reward={avg:.3f}. "
                f"Consider strategy adjustment."
            )
    
    async def _reflect_on_budget(self, budget_type: str):
        """Reflect on low budget"""
        self.state.add_reflection(
            f"Low {budget_type} budget remaining: "
            f"{getattr(self.state, f'{budget_type}_budget_remaining', 0):.3f}. "
            f"Switching to conservative mode."
        )
        
        # Reduce confidence, increase caution
        self.state.confidence = max(0.1, self.state.confidence - 0.2)
    
    # ========================================================================
    # Action Methods
    # ========================================================================
    
    async def _take_immediate_action(self, anomaly: Dict[str, Any]):
        """Take immediate action for critical anomalies"""
        expert_id = anomaly.get('expert_id')
        if expert_id and expert_id not in self.state.avoided_experts:
            self.state.avoided_experts.append(expert_id)
            self.state.add_reflection(
                f"IMMEDIATE: Avoiding expert {expert_id} due to critical anomaly"
            )
    
    def _infer_current_strategy(self) -> Optional[str]:
        """Infer current routing strategy from state"""
        if self.state.confidence < 0.3:
            return "exploratory"
        elif self.state.uncertainty > 0.7:
            return "cautious"
        elif len(self.state.avoided_experts) > 2:
            return "restricted"
        else:
            return "standard"
    
    # ========================================================================
    # Public API Methods
    # ========================================================================
    
    def get_state(self, task_id: Optional[str] = None) -> EnhancedMetaCognitiveState:
        """Get current meta-cognitive state"""
        # Update from metrics before returning
        if self.enable_metrics_integration:
            self.state.update_from_metrics(self.metrics_bridge)
        
        return self.state
    
    def record_outcome(
        self,
        task_id: str,
        success: bool,
        reward: float,
        expert_used: str,
        carbon_kg: float,
        helium_units: float,
        latency_ms: float
    ):
        """Record task outcome for learning"""
        # Update budgets
        self.state.carbon_budget_remaining = max(0, self.state.carbon_budget_remaining - carbon_kg)
        self.state.helium_budget_remaining = max(0, self.state.helium_budget_remaining - helium_units)
        
        # Update performance window
        self.performance_window.append(reward)
        
        # Update recent rewards
        self.state.recent_rewards.append(reward)
        if len(self.state.recent_rewards) > 100:
            self.state.recent_rewards = self.state.recent_rewards[-100:]
        
        # Update success rate
        alpha = 0.1
        self.state.historical_success_rate = (
            self.state.historical_success_rate * (1 - alpha) +
            (1.0 if success else 0.0) * alpha
        )
        
        # Update strategy effectiveness
        strategy = self._infer_current_strategy()
        if strategy:
            old_effectiveness = self.state.strategy_effectiveness.get(strategy, 0.5)
            self.state.strategy_effectiveness[strategy] = (
                old_effectiveness * (1 - alpha) + reward * alpha
            )
        
        # Update expert preferences
        if success and reward > 0.7:
            if expert_used not in self.state.preferred_experts:
                self.state.preferred_experts.append(expert_used)
        elif not success and expert_used not in self.state.avoided_experts:
            self.state.avoided_experts.append(expert_used)
        
        # Record to metrics collector for SLO tracking
        if self.metrics_bridge.metrics_collector:
            if hasattr(self.metrics_bridge.metrics_collector, 'slo_tracker'):
                self.metrics_bridge.metrics_collector.slo_tracker.record_metric(
                    'latency_slo', latency_ms
                )
    
    def get_routing_guidance(self) -> Dict[str, Any]:
        """Get routing guidance based on meta-cognitive state"""
        return {
            'confidence': self.state.confidence,
            'uncertainty': self.state.uncertainty,
            'preferred_experts': self.state.preferred_experts,
            'avoided_experts': self.state.avoided_experts,
            'health_scores': self.state.expert_health_scores,
            'degradation_warnings': self.state.degradation_warnings,
            'slo_compliance': self.state.slo_compliance,
            'strategy_effectiveness': self.state.strategy_effectiveness,
            'recommended_strategy': self._infer_current_strategy(),
            'exploration_rate': 1.0 - self.state.confidence
        }
    
    def inject_metrics_collector(self, collector: Any):
        """Inject metrics collector for integration"""
        self.metrics_bridge.inject_metrics_collector(collector)
        logger.info("Metrics collector injected into meta-cognitive architecture")


# ============================================================================
# Legacy Compatibility
# ============================================================================

class MetaCognitiveArchitecture(EnhancedMetaCognitiveArchitecture):
    """
    Legacy meta-cognitive architecture for backward compatibility.
    """
    
    def __init__(self):
        super().__init__(enable_metrics_integration=False)
        logger.info("Meta-Cognitive Architecture initialized (legacy mode)")
    
    def get_state(self) -> Dict[str, Any]:
        """Legacy state getter"""
        state = super().get_state()
        return {
            'carbon_budget_remaining': state.carbon_budget_remaining,
            'helium_budget_remaining': state.helium_budget_remaining,
            'latency_budget_ms': state.latency_budget_ms,
            'historical_success_rate': state.historical_success_rate,
            'preferred_experts': state.preferred_experts,
            'avoided_experts': state.avoided_experts
        }
