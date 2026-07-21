# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/sustainability_expert.py
# Enhanced version v3.0 – Full integration with bio‑inspired core, event‑driven, circuit breakers, persistence, cost‑benefit, quantum bridge, time tick engine, swarm, self‑healing, and config reload

"""
Enhanced Sustainability Expert v3.0
Full integration with bio‑inspired core, event‑driven, circuit breakers, persistence,
cost‑benefit, QuantumBridge, TimeTickEngine, swarm coordination, self‑healing,
and config reload.
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
import pickle

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

# ============================================================================
# BaseExpert Import
# ============================================================================
from .base_expert import BaseExpert

# ============================================================================
# Bio‑Inspired Core Import (with fallback)
# ============================================================================
try:
    from enhancements.bio_inspired.__init__ import EnhancedBioInspiredCore, BioEvent, CircuitBreaker, Persistence
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager, EcoATPConsumer, EcoATPSource
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    from enhancements.bio_inspired.atp_synthase_scheduler import ATPSynthaseScheduler
    from enhancements.bio_inspired.chromatophore_compartments import CompartmentManager, MembranePermeability, CompartmentState
    from enhancements.bio_inspired.biomass_storage import BiomassStorage, StorageTier, GuaranteeLevel
    from enhancements.bio_inspired.photosynthetic_harvester import PhotosyntheticHarvester
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False
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
            return await func(*args, **kwargs)

    class BioEvent:
        def __init__(self, event_type, source, data=None):
            self.event_type = event_type
            self.source = source
            self.data = data or {}

# ============================================================================
# Configuration (Pydantic or dataclass)
# ============================================================================
if PYDANTIC_AVAILABLE:
    class SustainabilityExpertConfig(BaseModel):
        """Configuration for Sustainability Expert."""
        expert_id: str = Field(default_factory=lambda: f"sustainability_{uuid.uuid4().hex[:8]}")
        enable_persistence: bool = True
        persistence_path: str = "./sustainability_expert.json"
        enable_predictive_alerts: bool = True
        enable_anomaly_detection: bool = True
        enable_cost_benefit: bool = True
        enable_quantum_bridge: bool = True
        enable_time_tick_engine: bool = True
        enable_swarm_coordination: bool = True
        enable_self_healing: bool = True

        # Thresholds (can be evolved)
        thresholds: Dict[str, float] = Field(default_factory=lambda: {
            'carbon_high_threshold': 500.0,
            'helium_scarcity_threshold': 0.6,
            'carbon_price_threshold': 80.0,
            'renewable_share_high': 0.8,
            'renewable_share_low': 0.4,
        })

        # Multi‑objective weights (if used)
        objective_weights: Dict[str, float] = Field(default_factory=lambda: {
            'carbon_savings': 0.4,
            'helium_savings': 0.3,
            'cost': 0.2,
            'latency': 0.1,
        })

        class Config:
            env_prefix = "SUSTAINABILITY_EXPERT_"
else:
    @dataclass
    class SustainabilityExpertConfig:
        expert_id: str = field(default_factory=lambda: f"sustainability_{uuid.uuid4().hex[:8]}")
        enable_persistence: bool = True
        persistence_path: str = "./sustainability_expert.json"
        enable_predictive_alerts: bool = True
        enable_anomaly_detection: bool = True
        enable_cost_benefit: bool = True
        enable_quantum_bridge: bool = True
        enable_time_tick_engine: bool = True
        enable_swarm_coordination: bool = True
        enable_self_healing: bool = True
        thresholds: Dict[str, float] = field(default_factory=lambda: {
            'carbon_high_threshold': 500.0,
            'helium_scarcity_threshold': 0.6,
            'carbon_price_threshold': 80.0,
            'renewable_share_high': 0.8,
            'renewable_share_low': 0.4,
        })
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'carbon_savings': 0.4,
            'helium_savings': 0.3,
            'cost': 0.2,
            'latency': 0.1,
        })

# ============================================================================
# Persistence for this expert
# ============================================================================
class SustainabilityExpertPersistence:
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
# Sustainability Expert (Main Class) – Enhanced v3.0
# ============================================================================
class SustainabilityExpert(BaseExpert):
    """
    Enhanced Sustainability Expert v3.0
    Provides recommendations for data center selection, carbon budget, helium conservation,
    renewable energy share, and carbon offsets, using real-time data, predictive analytics,
    multi-objective trade-offs, and full integration with the bio‑inspired ecosystem.
    """

    def __init__(
        self,
        bio_core: Optional[Any] = None,   # EnhancedBioInspiredCore instance
        config: Optional[Union[SustainabilityExpertConfig, Dict[str, Any]]] = None,
        expert_id: Optional[str] = None,
    ):
        super().__init__()
        # Load config
        if isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = SustainabilityExpertConfig(**config)
            else:
                self.config = SustainabilityExpertConfig(**config)
        elif isinstance(config, SustainabilityExpertConfig):
            self.config = config
        else:
            self.config = SustainabilityExpertConfig()

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
        self.token_manager = None
        self.gradient_manager = None

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

        # Circuit breakers for external providers
        self._carbon_circuit = CircuitBreaker("carbon_manager")
        self._helium_circuit = CircuitBreaker("helium_provider")
        self._pricing_circuit = CircuitBreaker("pricing_manager")

        # Persistence
        self.persistence = None
        if self.config.enable_persistence:
            self.persistence = SustainabilityExpertPersistence(self.config.persistence_path)

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
        self.carbon_manager = None
        self.helium_provider = None
        self.pricing_manager = None
        self.predictive_analyzer = None
        self.self_evolving_gate = None
        self.cross_domain_transfer = None

        # Subscribe to events if bio‑core available
        if self.event_broker:
            self._subscribe_events()

        logger.info(f"SustainabilityExpert initialized with ID {self.config.expert_id}, correlation_id={self.correlation_id}")

    # ========================================================================
    # Event Subscriptions
    # ========================================================================

    def _subscribe_events(self):
        """Subscribe to core events for state updates."""
        if self.event_broker:
            self.event_broker.subscribe('carbon_update', self._on_carbon_update)
            self.event_broker.subscribe('helium_update', self._on_helium_update)
            self.event_broker.subscribe('alert_generated', self._on_alert_generated)
            self.event_broker.subscribe('anomaly_detected', self._on_anomaly_detected)
            self.event_broker.subscribe('token_balance_update', self._on_token_update)
            self.event_broker.subscribe('config_updated', self._on_config_updated)
            self.event_broker.subscribe('health_update', self._on_health_update)
            logger.info("SustainabilityExpert subscribed to core events")

    async def _on_carbon_update(self, event: BioEvent):
        """Update carbon intensity from event."""
        self._last_context['carbon_intensity'] = event.data.get('intensity', 0.5)
        self._last_context['carbon_price'] = event.data.get('price', 50.0)

    async def _on_helium_update(self, event: BioEvent):
        """Update helium scarcity from event."""
        self._last_context['helium_scarcity'] = event.data.get('scarcity', 0.5)
        self._last_context['helium_price'] = event.data.get('price', 0.5)

    async def _on_alert_generated(self, event: BioEvent):
        """React to critical alerts by adjusting thresholds."""
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert received; adjusting sustainability thresholds")
            # Reduce thresholds to be more conservative
            self.thresholds['carbon_high_threshold'] *= 0.9
            self.thresholds['helium_scarcity_threshold'] *= 0.9
            if self.self_healer:
                await self.self_healer.apply_healing('damage_accumulation')

    async def _on_anomaly_detected(self, event: BioEvent):
        """React to anomalies by adjusting thresholds."""
        if event.data.get('metric') == 'carbon_intensity':
            logger.info("Carbon anomaly detected; adjusting thresholds")
            self.thresholds['carbon_high_threshold'] += 10.0

    async def _on_token_update(self, event: BioEvent):
        """Update token balance (might influence cost decisions)."""
        self._last_context['token_balance'] = event.data.get('balance', 500)

    async def _on_config_updated(self, event: BioEvent):
        """Reload configuration if changed."""
        updates = event.data.get('updates', {})
        if 'sustainability_expert' in updates:
            new_config = updates['sustainability_expert']
            if 'thresholds' in new_config:
                self.thresholds.update(new_config['thresholds'])
                if self.persistence:
                    self.persistence.set_thresholds(self.thresholds)
            logger.info("Configuration reloaded", updates=new_config)

    async def _on_health_update(self, event: BioEvent):
        """Update health score from core."""
        self.health_status = event.data.get('status', 'healthy')

    # ========================================================================
    # Dependency Injection (unchanged)
    # ========================================================================

    def set_carbon_manager(self, manager):
        self.carbon_manager = manager

    def set_helium_provider(self, provider):
        self.helium_provider = provider

    def set_pricing_manager(self, manager):
        self.pricing_manager = manager

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
        Generate sustainability recommendations based on real-time and predictive data.
        Returns a dict with:
          - 'recommendations': single preferred action set
          - 'options': list of trade-off options (for router to choose)
          - 'explanation': natural‑language description
        """
        self._last_context.update(context)

        # 1. Gather data using circuit breakers
        carbon_data = await self._get_carbon_data()
        helium_data = await self._get_helium_data()
        price_data = await self._get_price_data()

        # 2. Apply predictive forecast if available
        if self.predictive_analyzer:
            forecast = await self._get_predictive_forecast()
            if forecast:
                if forecast.get('trend') == 'increasing':
                    carbon_data['intensity'] *= 1.2
                    carbon_data['intensity'] = min(1000, carbon_data['intensity'])
                elif forecast.get('trend') == 'decreasing':
                    carbon_data['intensity'] *= 0.9
                    carbon_data['intensity'] = max(0, carbon_data['intensity'])
                if self.persistence:
                    self.persistence.set_last_forecast(forecast)

        # 3. Adjust thresholds based on predictive alerts and anomaly detection
        if self.config.enable_predictive_alerts and self.alert_system:
            alerts = await self.alert_system.get_active_alerts()
            critical_carbon_alerts = [a for a in alerts if a.category == 'carbon' and a.severity == 'critical']
            if critical_carbon_alerts:
                self.thresholds['carbon_high_threshold'] = min(450, self.thresholds['carbon_high_threshold'])
                self.thresholds['carbon_price_threshold'] = min(60, self.thresholds['carbon_price_threshold'])

        # 4. Use QuantumBridge to get QUBO penalties for carbon and helium
        q_penalty_carbon = 0.5
        q_penalty_helium = 0.5
        if self.config.enable_quantum_bridge and self.quantum_bridge:
            try:
                q_params = self.quantum_bridge.get_qubo_parameters()
                q_penalty_carbon = q_params.get('penalty_carbon', 0.5)
                q_penalty_helium = q_params.get('penalty_helium_shortage', 0.5)
                if q_penalty_carbon > 0.7:
                    carbon_data['intensity'] *= 1.1
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
                        helium_data['scarcity'] = max(helium_data['scarcity'], 0.8)

        # 6. Build the primary recommendation
        primary = self._build_recommendation(
            carbon_intensity=carbon_data['intensity'],
            helium_scarcity=helium_data['scarcity'],
            carbon_price=price_data['carbon_price'],
            helium_price=price_data['helium_price']
        )

        # 7. Build alternative trade‑off options with cost‑benefit analysis
        options = await self._build_tradeoff_options(
            carbon_intensity=carbon_data['intensity'],
            helium_scarcity=helium_data['scarcity'],
            carbon_price=price_data['carbon_price'],
            helium_price=price_data['helium_price']
        )

        # 8. Generate explanation
        explanation = self._generate_explanation(
            primary, carbon_data, helium_data, price_data
        )

        # 9. Swarm coordination – share insights
        if self.config.enable_swarm_coordination and self.swarm_coordinator:
            swarm_payload = {
                'expert_id': self.config.expert_id,
                'recommendation': primary,
                'carbon_intensity': carbon_data['intensity'],
                'helium_scarcity': helium_data['scarcity'],
                'thresholds': self.thresholds,
            }
            await self.swarm_coordinator.share_predictions(swarm_payload)

        # 10. Cross‑domain knowledge transfer
        if self.cross_domain_transfer:
            self.cross_domain_transfer.transfer_knowledge(
                'sustainability',
                'energy',
                'efficiency_patterns',
                {'carbon_intensity': carbon_data['intensity'],
                 'helium_scarcity': helium_data['scarcity']}
            )

        # 11. Persist history
        if self.persistence:
            self.persistence.add_history({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'carbon_intensity': carbon_data['intensity'],
                'helium_scarcity': helium_data['scarcity'],
                'recommendation': primary,
                'options': options,
            })
            self.persistence.set_last_recommendation(primary)

        # 12. Trigger workflow if needed (e.g., shift data center)
        if self.workflow_orchestrator and primary.get('preferred_data_center') != 'us-east':
            # Trigger a workflow to migrate workloads
            await self.workflow_orchestrator.execute_workflow('migrate_data_center')

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

    async def _get_carbon_data(self) -> Dict[str, float]:
        """Fetch carbon intensity and price from manager or context."""
        if self.carbon_manager:
            try:
                intensity = await self._carbon_circuit.call(self.carbon_manager.get_current_intensity)
                price = await self._carbon_circuit.call(self.carbon_manager.get_current_price)
                return {'intensity': intensity, 'price': price}
            except Exception as e:
                logger.error(f"Carbon manager error: {e}")
                self.health_status = "degraded"
                self.last_error = str(e)

        ctx_intensity = self._last_context.get('carbon_intensity', 0.5)
        intensity = ctx_intensity * 800.0 if ctx_intensity <= 1.0 else ctx_intensity
        price = self._last_context.get('carbon_price', 50.0)
        return {'intensity': intensity, 'price': price}

    async def _get_helium_data(self) -> Dict[str, float]:
        """Fetch helium scarcity and price from provider or context."""
        if self.helium_provider:
            try:
                scarcity = await self._helium_circuit.call(self.helium_provider.get_scarcity)
                cost = await self._helium_circuit.call(self.helium_provider.get_cost_index)
                return {'scarcity': scarcity, 'price': cost * 0.5}
            except Exception as e:
                logger.error(f"Helium provider error: {e}")
                self.health_status = "degraded"
                self.last_error = str(e)

        ctx_scarcity = self._last_context.get('helium_scarcity', 0.5)
        ctx_price = self._last_context.get('helium_price', 0.5)
        return {'scarcity': ctx_scarcity, 'price': ctx_price}

    async def _get_price_data(self) -> Dict[str, float]:
        """Fetch carbon/helium prices from pricing manager."""
        if self.pricing_manager:
            try:
                prices = await self._pricing_circuit.call(self.pricing_manager.get_current_prices)
                return {
                    'carbon_price': prices.get('carbon_price_usd_per_ton', 50.0),
                    'helium_price': prices.get('helium_price_usd_per_l', 0.5)
                }
            except Exception as e:
                logger.error(f"Pricing manager error: {e}")
                self.health_status = "degraded"
                self.last_error = str(e)

        return {
            'carbon_price': self._last_context.get('carbon_price', 50.0),
            'helium_price': self._last_context.get('helium_price', 0.5)
        }

    async def _get_predictive_forecast(self) -> Optional[Dict]:
        if self.predictive_analyzer:
            try:
                if hasattr(self.predictive_analyzer, 'predict_carbon_trend'):
                    return await self.predictive_analyzer.predict_carbon_trend()
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
        carbon_intensity: float,
        helium_scarcity: float,
        carbon_price: float,
        helium_price: float
    ) -> Dict[str, Any]:
        rec = {}

        # Data center
        if carbon_intensity > self.thresholds['carbon_high_threshold']:
            rec['preferred_data_center'] = 'us-west'
            rec['carbon_budget_kg'] = 5.0
        else:
            rec['preferred_data_center'] = 'us-east'
            rec['carbon_budget_kg'] = 10.0

        # Helium recovery
        if helium_scarcity > self.thresholds['helium_scarcity_threshold']:
            rec['helium_recovery'] = True
            rec['cooling_method'] = 'alternative'
        else:
            rec['helium_recovery'] = False
            rec['cooling_method'] = 'standard'

        # Carbon offsets
        if carbon_price > self.thresholds['carbon_price_threshold']:
            rec['carbon_offset'] = True
            rec['offset_amount_kg'] = rec['carbon_budget_kg'] * 0.5
        else:
            rec['carbon_offset'] = False
            rec['offset_amount_kg'] = 0.0

        # Renewable share
        if carbon_intensity < 300:
            rec['renewable_share'] = self.thresholds['renewable_share_high']
        else:
            rec['renewable_share'] = self.thresholds['renewable_share_low']

        # Token incentive
        carbon_savings = (400 - carbon_intensity) / 400 if carbon_intensity < 400 else 0
        if carbon_savings > 0.1:
            rec['token_stake_recommendation'] = carbon_savings * 100

        return rec

    async def _build_tradeoff_options(
        self,
        carbon_intensity: float,
        helium_scarcity: float,
        carbon_price: float,
        helium_price: float
    ) -> List[Dict[str, Any]]:
        options = []

        # Option A: Shift to low‑carbon region
        if carbon_intensity > 400:
            option = {
                'action': 'shift_to_low_carbon_region',
                'estimated_carbon_savings_kg': (carbon_intensity - 300) * 0.01,
                'estimated_cost_increase_usd': 5.0,
                'accuracy_impact': -0.02,
                'priority': 'high'
            }
            if self.config.enable_cost_benefit and self.cost_benefit_engine:
                params = {'carbon_savings': option['estimated_carbon_savings_kg']}
                analysis = await self.cost_benefit_engine.analyze_scenario('shift_low_carbon', params)
                option['roi'] = analysis.roi
                option['net_value'] = analysis.net_value
            options.append(option)

        # Option B: Enable helium recovery
        if helium_scarcity > 0.4:
            option = {
                'action': 'enable_helium_recovery',
                'estimated_helium_savings_l': helium_scarcity * 0.5,
                'estimated_latency_increase_ms': 10.0,
                'accuracy_impact': 0.0,
                'priority': 'medium'
            }
            if self.config.enable_cost_benefit and self.cost_benefit_engine:
                params = {'helium_savings': option['estimated_helium_savings_l']}
                analysis = await self.cost_benefit_engine.analyze_scenario('helium_recovery', params)
                option['roi'] = analysis.roi
                option['net_value'] = analysis.net_value
            options.append(option)

        # Option C: Purchase carbon offsets
        if carbon_price > 50:
            option = {
                'action': 'purchase_carbon_offsets',
                'estimated_carbon_savings_kg': 10.0,
                'estimated_cost_usd': carbon_price * 0.1,
                'accuracy_impact': 0.0,
                'priority': 'low'
            }
            if self.config.enable_cost_benefit and self.cost_benefit_engine:
                params = {'carbon_savings': option['estimated_carbon_savings_kg']}
                analysis = await self.cost_benefit_engine.analyze_scenario('carbon_offsets', params)
                option['roi'] = analysis.roi
                option['net_value'] = analysis.net_value
            options.append(option)

        # Option D: Increase renewable share
        option = {
            'action': 'increase_renewable_share',
            'estimated_renewable_share': 0.9,
            'estimated_scheduling_complexity': 0.3,
            'accuracy_impact': -0.01,
            'priority': 'medium'
        }
        if self.config.enable_cost_benefit and self.cost_benefit_engine:
            params = {'renewable_share': option['estimated_renewable_share']}
            analysis = await self.cost_benefit_engine.analyze_scenario('increase_renewable', params)
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
        carbon_data: Dict[str, float],
        helium_data: Dict[str, float],
        price_data: Dict[str, float]
    ) -> str:
        parts = []

        carbon_intensity = carbon_data.get('intensity', 400)
        if carbon_intensity > self.thresholds['carbon_high_threshold']:
            parts.append(f"Carbon intensity is high ({carbon_intensity:.0f} g/kWh), "
                         f"so we shifted workload to a lower‑carbon region.")
        else:
            parts.append(f"Carbon intensity is moderate ({carbon_intensity:.0f} g/kWh), "
                         f"keeping workload in the primary region.")

        helium_scarcity = helium_data.get('scarcity', 0.5)
        if helium_scarcity > self.thresholds['helium_scarcity_threshold']:
            parts.append(f"Helium scarcity is high ({helium_scarcity:.2f}), "
                         f"so we enabled helium recovery and alternative cooling.")

        carbon_price = price_data.get('carbon_price', 50.0)
        if carbon_price > self.thresholds['carbon_price_threshold']:
            parts.append(f"Carbon price is high (${carbon_price:.2f}/ton), "
                         f"so we recommend purchasing carbon offsets.")

        if self.bio_core:
            parts.append("Decisions are informed by real‑time ecosystem analytics.")

        if not parts:
            parts.append("Sustainability metrics are within acceptable ranges. "
                         "Current recommendations maintain optimal efficiency.")

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
        logger.info("SustainabilityExpert self‑healing")
        if self.config.enable_self_healing:
            self.thresholds = self.config.thresholds.copy()
            if self.persistence:
                self.persistence.set_thresholds(self.thresholds)
            self.health_status = "healthy"
            self.last_error = None

    async def shutdown(self):
        """Graceful shutdown."""
        logger.info(f"Shutting down SustainabilityExpert {self.config.expert_id}")
        if self.persistence:
            self.persistence._save()
