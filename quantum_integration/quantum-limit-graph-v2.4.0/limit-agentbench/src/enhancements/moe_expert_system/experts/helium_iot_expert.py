# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/helium_iot_expert.py
# Enhanced version v3.0 – Full integration with bio‑inspired core, predictive, economic, and swarm‑aware

"""
Enhanced Helium IoT Expert v3.0
Integrates real-time helium data, IoT telemetry, predictive analytics,
multi-objective trade-off options, explainable recommendations,
and self-evolving tunable thresholds, with full integration into
the bio‑inspired ecosystem (event broker, circuit breakers,
predictive alerts, anomaly detection, cost‑benefit, quantum bridge,
time tick engine, and swarm coordination).
"""

import asyncio
import logging
import json
import os
import uuid
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from collections import deque
import numpy as np

# Try optional dependencies
try:
    from pydantic import BaseModel, Field
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

# Local imports from bio‑inspired core (with fallback)
try:
    from ...bio_inspired.__init__ import EnhancedBioInspiredCore, BioEvent, CircuitBreaker, Persistence
    CORE_AVAILABLE = True
except ImportError:
    CORE_AVAILABLE = False
    # Fallback definitions
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
            # Simple pass-through
            return await func(*args, **kwargs)

    class BioEvent:
        def __init__(self, event_type, source, data=None):
            self.event_type = event_type
            self.source = source
            self.data = data or {}

# Try optional imports from other experts
try:
    from ...advanced.cross_region_federation import HeliumProvider
    from ...integration.layer_integrator import CarbonIntensityManager
    from ...advanced.self_evolving_gates import EnhancedSelfEvolvingGate
    MANAGERS_AVAILABLE = True
except ImportError:
    MANAGERS_AVAILABLE = False
    logger.warning("Helium IoT Expert: external managers not available - using fallback data")

# Base expert
try:
    from .base_expert import BaseExpert
except ImportError:
    # Fallback BaseExpert
    class BaseExpert:
        pass

# ============================================================================
# Configuration (Pydantic or dataclass)
# ============================================================================
if PYDANTIC_AVAILABLE:
    class HeliumIoTExpertConfig(BaseModel):
        """Configuration for Helium IoT Expert."""
        expert_id: str = Field(default_factory=lambda: f"helium_iot_{uuid.uuid4().hex[:8]}")
        enable_persistence: bool = True
        persistence_path: str = "./helium_iot_expert.json"
        enable_predictive_alerts: bool = True
        enable_anomaly_detection: bool = True
        enable_cost_benefit: bool = True
        enable_quantum_bridge: bool = True
        enable_time_tick_engine: bool = True
        enable_swarm_coordination: bool = True
        enable_self_healing: bool = True

        # Thresholds (can be evolved)
        thresholds: Dict[str, float] = Field(default_factory=lambda: {
            'helium_scarcity_high': 0.6,
            'helium_scarcity_critical': 0.8,
            'network_latency_high': 100.0,
            'battery_low_threshold': 0.2,
            'sampling_rate_high': 10.0,
            'sampling_rate_low': 5.0,
            'sampling_rate_critical': 2.0,
        })

        # Multi‑objective weights (if used)
        objective_weights: Dict[str, float] = Field(default_factory=lambda: {
            'helium_savings': 0.4,
            'data_quality': 0.3,
            'latency': 0.2,
            'cost': 0.1,
        })

        class Config:
            env_prefix = "HELIUM_IOT_EXPERT_"
else:
    @dataclass
    class HeliumIoTExpertConfig:
        expert_id: str = field(default_factory=lambda: f"helium_iot_{uuid.uuid4().hex[:8]}")
        enable_persistence: bool = True
        persistence_path: str = "./helium_iot_expert.json"
        enable_predictive_alerts: bool = True
        enable_anomaly_detection: bool = True
        enable_cost_benefit: bool = True
        enable_quantum_bridge: bool = True
        enable_time_tick_engine: bool = True
        enable_swarm_coordination: bool = True
        enable_self_healing: bool = True
        thresholds: Dict[str, float] = field(default_factory=lambda: {
            'helium_scarcity_high': 0.6,
            'helium_scarcity_critical': 0.8,
            'network_latency_high': 100.0,
            'battery_low_threshold': 0.2,
            'sampling_rate_high': 10.0,
            'sampling_rate_low': 5.0,
            'sampling_rate_critical': 2.0,
        })
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'helium_savings': 0.4,
            'data_quality': 0.3,
            'latency': 0.2,
            'cost': 0.1,
        })

# ============================================================================
# Persistence for this expert
# ============================================================================
class HeliumIoTExpertPersistence:
    """Simple file‑based persistence for thresholds and history."""
    def __init__(self, path: str):
        self.path = path
        self.data = {
            'thresholds': {},
            'history': [],
            'last_forecast': None,
            'last_recommendation': None,
        }
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, 'r') as f:
                    self.data = json.load(f)
            except Exception as e:
                logger.error(f"Failed to load persistence: {e}")

    def _save(self):
        try:
            with open(self.path, 'w') as f:
                json.dump(self.data, f, default=str)
        except Exception as e:
            logger.error(f"Failed to save persistence: {e}")

    def get_thresholds(self) -> Dict[str, float]:
        return self.data.get('thresholds', {})

    def set_thresholds(self, thresholds: Dict[str, float]):
        self.data['thresholds'] = thresholds
        self._save()

    def add_history(self, entry: Dict[str, Any]):
        self.data['history'].append(entry)
        if len(self.data['history']) > 1000:
            self.data['history'] = self.data['history'][-1000:]
        self._save()

    def get_history(self, limit: int = 100) -> List[Dict]:
        return self.data['history'][-limit:]

    def set_last_forecast(self, forecast: Dict):
        self.data['last_forecast'] = forecast
        self._save()

    def get_last_forecast(self) -> Optional[Dict]:
        return self.data.get('last_forecast')

    def set_last_recommendation(self, rec: Dict):
        self.data['last_recommendation'] = rec
        self._save()

    def get_last_recommendation(self) -> Optional[Dict]:
        return self.data.get('last_recommendation')

# ============================================================================
# Helium IoT Expert (Enhanced)
# ============================================================================
class HeliumIoTExpert(BaseExpert):
    """
    Helium IoT Expert v3.0
    Provides recommendations for IoT sampling rate, aggregation strategies,
    gateway selection, and power management, using real-time helium data,
    network metrics, predictive analytics, and multi-objective trade-offs,
    with full integration into the bio‑inspired ecosystem.
    """

    def __init__(
        self,
        bio_core: Optional[Any] = None,   # EnhancedBioInspiredCore instance
        config: Optional[Union[HeliumIoTExpertConfig, Dict[str, Any]]] = None,
        expert_id: Optional[str] = None,
    ):
        super().__init__()
        # Load config
        if isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = HeliumIoTExpertConfig(**config)
            else:
                self.config = HeliumIoTExpertConfig(**config)
        elif isinstance(config, HeliumIoTExpertConfig):
            self.config = config
        else:
            self.config = HeliumIoTExpertConfig()

        if expert_id:
            self.config.expert_id = expert_id

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

        # Circuit breakers for external providers
        self._helium_circuit = CircuitBreaker("helium_provider")
        self._carbon_circuit = CircuitBreaker("carbon_manager")

        # Persistence
        self.persistence = None
        if self.config.enable_persistence:
            self.persistence = HeliumIoTExpertPersistence(self.config.persistence_path)

        # Load thresholds from persistence if available
        if self.persistence:
            stored_thresholds = self.persistence.get_thresholds()
            if stored_thresholds:
                self.config.thresholds.update(stored_thresholds)

        # Internal state
        self.thresholds = self.config.thresholds.copy()
        self._last_context = {}
        self.correlation_id = str(uuid.uuid4())
        self.health_status = "healthy"
        self.last_error = None

        # External managers (optional)
        self.helium_provider = None
        self.carbon_manager = None
        self.predictive_analyzer = None
        self.self_evolving_gate = None
        self.cross_domain_transfer = None

        # Subscribe to events if bio‑core available
        if self.event_broker:
            self._subscribe_events()

        logger.info(f"HeliumIoTExpert initialized with ID {self.config.expert_id}, correlation_id={self.correlation_id}")

    # ========================================================================
    # Event Subscriptions
    # ========================================================================

    def _subscribe_events(self):
        """Subscribe to core events for state updates."""
        if self.event_broker:
            self.event_broker.subscribe('helium_update', self._on_helium_update)
            self.event_broker.subscribe('alert_generated', self._on_alert_generated)
            self.event_broker.subscribe('anomaly_detected', self._on_anomaly_detected)
            self.event_broker.subscribe('token_balance_update', self._on_token_update)
            self.event_broker.subscribe('config_updated', self._on_config_updated)
            logger.info("HeliumIoTExpert subscribed to core events")

    async def _on_helium_update(self, event: BioEvent):
        """Update helium scarcity from event."""
        self._last_context['helium_scarcity'] = event.data.get('scarcity', 0.5)
        self._last_context['helium_cost_index'] = event.data.get('cost', 1.0)

    async def _on_alert_generated(self, event: BioEvent):
        """React to critical alerts by adjusting thresholds."""
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert received; adjusting helium thresholds")
            # Temporarily reduce thresholds to be more conservative
            self.thresholds['helium_scarcity_high'] *= 0.8
            self.thresholds['helium_scarcity_critical'] *= 0.8
            # Trigger healing if available
            if self.self_healer:
                await self.self_healer.apply_healing('damage_accumulation')

    async def _on_anomaly_detected(self, event: BioEvent):
        """React to anomalies by adjusting thresholds."""
        if event.data.get('metric') == 'helium_scarcity':
            logger.info("Helium anomaly detected; adjusting thresholds")
            self.thresholds['helium_scarcity_high'] += 0.1
            self.thresholds['helium_scarcity_critical'] += 0.1

    async def _on_token_update(self, event: BioEvent):
        """Update token balance (might influence cost decisions)."""
        self._last_context['token_balance'] = event.data.get('balance', 500)

    async def _on_config_updated(self, event: BioEvent):
        """Reload configuration if changed."""
        updates = event.data.get('updates', {})
        if 'helium_iot_expert' in updates:
            new_config = updates['helium_iot_expert']
            if 'thresholds' in new_config:
                self.thresholds.update(new_config['thresholds'])
                if self.persistence:
                    self.persistence.set_thresholds(self.thresholds)
            logger.info("Configuration reloaded", updates=new_config)

    # ========================================================================
    # Dependency Injection (unchanged)
    # ========================================================================

    def set_helium_provider(self, provider):
        self.helium_provider = provider

    def set_carbon_manager(self, manager):
        self.carbon_manager = manager

    def set_predictive_analyzer(self, analyzer):
        self.predictive_analyzer = analyzer

    def set_self_evolving_gate(self, gate):
        self.self_evolving_gate = gate

    def set_cross_domain_transfer(self, transfer):
        self.cross_domain_transfer = transfer

    # ========================================================================
    # Threshold Management (with persistence)
    # ========================================================================

    def get_thresholds(self) -> Dict[str, float]:
        return self.thresholds

    def set_thresholds(self, thresholds: Dict[str, float]):
        self.thresholds.update(thresholds)
        if self.persistence:
            self.persistence.set_thresholds(self.thresholds)
        logger.info(f"Thresholds updated: {self.thresholds}")

    # ========================================================================
    # Health Check
    # ========================================================================

    def get_health_status(self) -> Dict[str, Any]:
        return {
            'expert_id': self.config.expert_id,
            'status': self.health_status,
            'last_error': self.last_error,
            'thresholds': self.thresholds,
            'persistence_enabled': self.config.enable_persistence,
        }

    # ========================================================================
    # Core Propose Method (Async)
    # ========================================================================

    async def propose(self, context: dict) -> dict:
        """
        Generate IoT recommendations based on real-time and predictive data.
        Returns a dict with:
          - 'recommendations': single preferred action set
          - 'options': list of trade-off options (for router to choose)
          - 'explanation': natural‑language description
        """
        self._last_context.update(context)

        # 1. Gather data using circuit breakers
        helium_data = await self._get_helium_data()
        network_data = self._get_network_data()
        device_data = self._get_device_data()

        # 2. Apply predictive forecast if available
        if self.predictive_analyzer:
            forecast = await self._get_predictive_forecast()
            if forecast:
                # Adjust helium scarcity based on forecast trend
                if forecast.get('trend') == 'increasing':
                    helium_data['scarcity'] *= 1.2
                    helium_data['scarcity'] = min(1.0, helium_data['scarcity'])
                elif forecast.get('trend') == 'decreasing':
                    helium_data['scarcity'] *= 0.9
                    helium_data['scarcity'] = max(0.0, helium_data['scarcity'])
                # Store forecast for later
                if self.persistence:
                    self.persistence.set_last_forecast(forecast)

        # 3. Adjust thresholds based on predictive alerts and anomaly detection
        if self.config.enable_predictive_alerts and self.alert_system:
            alerts = await self.alert_system.get_active_alerts()
            # If there are critical alerts about helium, reduce thresholds further
            critical_helium_alerts = [a for a in alerts if a.category == 'helium' and a.severity == 'critical']
            if critical_helium_alerts:
                self.thresholds['helium_scarcity_high'] = min(0.5, self.thresholds['helium_scarcity_high'])
                self.thresholds['helium_scarcity_critical'] = min(0.7, self.thresholds['helium_scarcity_critical'])

        # 4. Use QuantumBridge to get QUBO penalties for helium
        q_penalty_helium = 0.5
        if self.config.enable_quantum_bridge and self.quantum_bridge:
            try:
                q_params = self.quantum_bridge.get_qubo_parameters()
                q_penalty_helium = q_params.get('penalty_helium_shortage', 0.5)
                # If penalty high, increase sensitivity
                if q_penalty_helium > 0.7:
                    helium_data['scarcity'] *= 1.1
            except Exception as e:
                logger.warning(f"QuantumBridge error: {e}")

        # 5. Use TimeTickEngine forecast if available
        if self.config.enable_time_tick_engine and self.tick_engine:
            if hasattr(self.tick_engine, 'get_helium_forecast'):
                forecast = self.tick_engine.get_helium_forecast(4)  # next 4 hours
                if forecast and len(forecast) > 3:
                    avg_future = np.mean(forecast)
                    if avg_future < 0.3:
                        # Anticipate scarcity
                        helium_data['scarcity'] = max(helium_data['scarcity'], 0.8)

        # 6. Build the primary recommendation
        primary = self._build_recommendation(
            helium_scarcity=helium_data['scarcity'],
            helium_cost=helium_data['cost'],
            network_latency=network_data['latency'],
            battery_level=device_data['battery']
        )

        # 7. Build alternative trade‑off options with cost‑benefit analysis
        options = await self._build_tradeoff_options(
            helium_scarcity=helium_data['scarcity'],
            helium_cost=helium_data['cost'],
            network_latency=network_data['latency'],
            battery_level=device_data['battery']
        )

        # 8. Generate explanation
        explanation = self._generate_explanation(
            primary, helium_data, network_data, device_data
        )

        # 9. Swarm coordination – share insights
        if self.config.enable_swarm_coordination and self.swarm_coordinator:
            swarm_payload = {
                'expert_id': self.config.expert_id,
                'recommendation': primary,
                'helium_scarcity': helium_data['scarcity'],
                'thresholds': self.thresholds,
            }
            await self.swarm_coordinator.share_predictions(swarm_payload)

        # 10. Cross‑domain knowledge transfer
        if self.cross_domain_transfer:
            self.cross_domain_transfer.transfer_knowledge(
                'helium_iot',
                'energy',
                'efficiency_patterns',
                {'helium_scarcity': helium_data['scarcity']}
            )

        # 11. Persist history
        if self.persistence:
            self.persistence.add_history({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'helium_scarcity': helium_data['scarcity'],
                'recommendation': primary,
                'options': options,
            })
            self.persistence.set_last_recommendation(primary)

        # 12. Trigger workflow if needed (e.g., reduce sampling)
        if self.workflow_orchestrator and primary.get('sampling_rate_hz', 0) < 5:
            # Trigger a workflow to adjust device settings
            await self.workflow_orchestrator.execute_workflow('adjust_iot_sampling')

        # 13. Update health status
        self.health_status = "healthy"
        self.last_error = None

        return {
            'recommendations': primary,
            'options': options,
            'explanation': explanation
        }

    # ========================================================================
    # Data Gathering Helpers (with circuit breakers)
    # ========================================================================

    async def _get_helium_data(self) -> Dict[str, float]:
        """Fetch helium scarcity and cost from provider or context."""
        if self.helium_provider:
            try:
                scarcity = await self._helium_circuit.call(self.helium_provider.get_scarcity)
                cost = await self._helium_circuit.call(self.helium_provider.get_cost_index)
                return {'scarcity': scarcity, 'cost': cost}
            except Exception as e:
                logger.error(f"Helium provider error: {e}")
                self.health_status = "degraded"
                self.last_error = str(e)

        ctx_scarcity = self._last_context.get('helium_scarcity', 0.5)
        ctx_cost = self._last_context.get('helium_cost_index', 1.0)
        return {'scarcity': ctx_scarcity, 'cost': ctx_cost}

    def _get_network_data(self) -> Dict[str, float]:
        return {
            'latency': self._last_context.get('network_latency_ms', 50.0),
            'bandwidth': self._last_context.get('bandwidth_mbps', 100.0)
        }

    def _get_device_data(self) -> Dict[str, float]:
        return {
            'battery': self._last_context.get('battery_level', 0.8),
            'data_quality': self._last_context.get('data_quality', 0.9)
        }

    async def _get_predictive_forecast(self) -> Optional[Dict]:
        if self.predictive_analyzer:
            try:
                if hasattr(self.predictive_analyzer, 'predict_helium_trend'):
                    return await self.predictive_analyzer.predict_helium_trend()
                elif hasattr(self.predictive_analyzer, 'predict_evolution_trend'):
                    return await self.predictive_analyzer.predict_evolution_trend()
            except Exception as e:
                logger.error(f"Predictive analyzer error: {e}")
        return None

    # ========================================================================
    # Recommendation Builders (enhanced with cost‑benefit)
    # ========================================================================

    def _build_recommendation(
        self,
        helium_scarcity: float,
        helium_cost: float,
        network_latency: float,
        battery_level: float
    ) -> Dict[str, Any]:
        """Build the single preferred recommendation using current thresholds."""
        rec = {}

        # Sampling rate
        if helium_scarcity > self.thresholds['helium_scarcity_critical']:
            rec['sampling_rate_hz'] = self.thresholds['sampling_rate_critical']
            rec['power_saving_mode'] = True
        elif helium_scarcity > self.thresholds['helium_scarcity_high']:
            rec['sampling_rate_hz'] = self.thresholds['sampling_rate_low']
            rec['power_saving_mode'] = False
        else:
            rec['sampling_rate_hz'] = self.thresholds['sampling_rate_high']
            rec['power_saving_mode'] = False

        # Aggregation strategy
        if helium_scarcity > self.thresholds['helium_scarcity_high']:
            rec['aggregation_strategy'] = 'compressed'
        else:
            rec['aggregation_strategy'] = 'adaptive'

        # Gateway preference (based on latency and carbon)
        if network_latency > self.thresholds['network_latency_high']:
            rec['preferred_gateways'] = ['gateway_nearby']
        else:
            rec['preferred_gateways'] = []

        # Battery‑aware sampling override
        if battery_level < self.thresholds['battery_low_threshold']:
            rec['sampling_rate_hz'] = min(rec['sampling_rate_hz'], 2.0)
            rec['power_saving_mode'] = True

        # Apply cost‑benefit if available
        if self.config.enable_cost_benefit and self.cost_benefit_engine:
            # Simulate a cost‑benefit analysis for the chosen action
            # We could call the engine to validate the choice
            pass

        return rec

    async def _build_tradeoff_options(
        self,
        helium_scarcity: float,
        helium_cost: float,
        network_latency: float,
        battery_level: float
    ) -> List[Dict[str, Any]]:
        """Generate multiple alternative actions with estimated trade‑offs."""
        options = []

        # Option A: Reduce sampling rate
        if helium_scarcity > 0.4:
            option = {
                'action': 'reduce_sampling_rate',
                'estimated_helium_savings_l': helium_scarcity * 0.1,
                'estimated_data_quality_loss': 0.05,
                'priority': 'high'
            }
            if self.config.enable_cost_benefit and self.cost_benefit_engine:
                # Evaluate ROI
                params = {'helium_savings': option['estimated_helium_savings_l']}
                analysis = await self.cost_benefit_engine.analyze_scenario('reduce_sampling', params)
                option['roi'] = analysis.roi
                option['net_value'] = analysis.net_value
            options.append(option)

        # Option B: Switch to compressed aggregation
        if helium_scarcity > 0.3:
            option = {
                'action': 'enable_compressed_aggregation',
                'estimated_bandwidth_save': 0.3,
                'estimated_latency_increase': 5.0,
                'priority': 'medium'
            }
            if self.config.enable_cost_benefit and self.cost_benefit_engine:
                params = {'bandwidth_save': option['estimated_bandwidth_save']}
                analysis = await self.cost_benefit_engine.analyze_scenario('compressed_aggregation', params)
                option['roi'] = analysis.roi
                option['net_value'] = analysis.net_value
            options.append(option)

        # Option C: Use closer gateways
        if network_latency > 80:
            option = {
                'action': 'use_closer_gateways',
                'estimated_latency_reduction': 20.0,
                'estimated_cost_increase': 0.1,
                'priority': 'low'
            }
            if self.config.enable_cost_benefit and self.cost_benefit_engine:
                params = {'latency_reduction': option['estimated_latency_reduction']}
                analysis = await self.cost_benefit_engine.analyze_scenario('closer_gateways', params)
                option['roi'] = analysis.roi
                option['net_value'] = analysis.net_value
            options.append(option)

        # Option D: Enable power‑saving mode
        if battery_level < 0.3:
            option = {
                'action': 'enable_power_saving',
                'estimated_battery_extension_hours': 24.0,
                'estimated_data_quality_loss': 0.1,
                'priority': 'medium'
            }
            if self.config.enable_cost_benefit and self.cost_benefit_engine:
                params = {'battery_extension': option['estimated_battery_extension_hours']}
                analysis = await self.cost_benefit_engine.analyze_scenario('power_saving', params)
                option['roi'] = analysis.roi
                option['net_value'] = analysis.net_value
            options.append(option)

        return options

    # ========================================================================
    # Explainability (enhanced with core context)
    # ========================================================================

    def _generate_explanation(
        self,
        recommendation: Dict[str, Any],
        helium_data: Dict[str, float],
        network_data: Dict[str, float],
        device_data: Dict[str, float]
    ) -> str:
        """Generate a human‑readable explanation for the recommendations."""
        parts = []

        helium_scarcity = helium_data.get('scarcity', 0.5)
        if helium_scarcity > self.thresholds['helium_scarcity_critical']:
            parts.append(f"Helium scarcity is critical ({helium_scarcity:.2f}), "
                         f"so we reduced sampling rate to {recommendation['sampling_rate_hz']:.1f} Hz "
                         f"and enabled power‑saving mode.")
        elif helium_scarcity > self.thresholds['helium_scarcity_high']:
            parts.append(f"Helium scarcity is high ({helium_scarcity:.2f}), "
                         f"so we reduced sampling rate to {recommendation['sampling_rate_hz']:.1f} Hz "
                         f"and switched to compressed aggregation.")
        else:
            parts.append(f"Helium scarcity is moderate ({helium_scarcity:.2f}), "
                         f"maintaining standard sampling rate.")

        if device_data.get('battery', 0.8) < self.thresholds['battery_low_threshold']:
            parts.append(f"Battery level is low ({device_data['battery']:.0%}), "
                         f"so we further reduced sampling to conserve energy.")

        if not parts:
            parts.append("IoT metrics are within acceptable ranges. "
                         "Current recommendations maintain optimal performance.")

        # Include core context if available
        if self.bio_core:
            parts.append("Decisions are informed by real‑time ecosystem analytics.")

        return " ".join(parts)

    # ========================================================================
    # Legacy Compatibility (for router)
    # ========================================================================

    def propose(self, context: dict) -> dict:
        """
        Synchronous version for compatibility with the router.
        It wraps the async propose and runs it in an event loop.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(self.propose(context))
        finally:
            loop.close()
        return result

    async def propose_async(self, context: dict) -> dict:
        return await self.propose(context)

    # ========================================================================
    # Self‑Healing and Shutdown
    # ========================================================================

    async def self_heal(self):
        """Trigger self‑healing routines."""
        logger.info("HeliumIoTExpert self‑healing")
        # Reset thresholds to defaults if corrupted
        if self.config.enable_self_healing:
            self.thresholds = self.config.thresholds.copy()
            if self.persistence:
                self.persistence.set_thresholds(self.thresholds)
            self.health_status = "healthy"
            self.last_error = None

    async def shutdown(self):
        """Graceful shutdown."""
        logger.info(f"Shutting down HeliumIoTExpert {self.config.expert_id}")
        if self.persistence:
            self.persistence._save()  # ensure persistence
