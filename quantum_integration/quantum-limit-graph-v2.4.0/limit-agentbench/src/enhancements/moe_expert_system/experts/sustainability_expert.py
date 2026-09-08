#!/usr/bin/env python3
# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/sustainability_expert.py
# Version 3.4.0 – Enhanced Green Agent MODP Integration with central components
#
# ENHANCEMENTS OVER v3.3.0:
# 1. Fixed critical bug: cross_domain_transfer not initialized (now a fallback class).
# 2. Accept optional central carbon/helium/pricing managers in constructor.
# 3. Added async get_metrics method.
# 4. Fixed _select_best_solution to actually apply Pareto filtering.
# 5. Standardized carbon intensity conversion (normalized 0-1 => g/kWh by *800).
# 6. Made policy_probs context-aware (uses last context metrics).
# 7. Added human-in-the-loop flag for critical recommendations.
# 8. Added temporal safety cooldown for drastic actions.
# 9. Safe normalization of objective weights.
# 10. Added explanation field to each Pareto solution.
# 11. Minor cleanup and error handling.

import asyncio
import json
import os
import uuid
import time
from typing import Dict, Any, List, Optional, Union, Callable, Awaitable, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from collections import deque
import numpy as np

# -----------------------------------------------------------------------------
# IMPORT CENTRAL GREEN AGENT COMPONENTS
# -----------------------------------------------------------------------------
from ..config import config as central_config
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..routing.pareto_gating import ParetoGating
from ..feedback.adaptive_cost import AdaptiveCostFunction
from ..safety.drift_detector import DriftDetector
from ..scaling.message_queue import AsyncMessageQueue
from ..metrics import MetricsRegistry
from ..logger import logger

# Optional: central circuit breaker and rate limiter
try:
    from ..scaling.circuit_breaker import EnhancedCircuitBreaker
    from ..scaling.rate_limiter import EnhancedRateLimiter
    CENTRAL_CIRCUIT_BREAKER_AVAILABLE = True
except ImportError:
    class EnhancedCircuitBreaker:
        def __init__(self, name, failure_threshold=5, recovery_timeout=30.0):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self.failure_count = 0
            self.last_failure_time = None
            self.state = "closed"
            self._lock = asyncio.Lock()
        async def call(self, func, *args, **kwargs):
            async with self._lock:
                if self.state == "open":
                    if self.last_failure_time and (datetime.now(timezone.utc) - self.last_failure_time).total_seconds() > self.recovery_timeout:
                        self.state = "half-open"
                    else:
                        raise RuntimeError(f"Circuit breaker {self.name} is open")
            try:
                result = await func(*args, **kwargs)
                async with self._lock:
                    self.state = "closed"
                    self.failure_count = 0
                return result
            except Exception as e:
                async with self._lock:
                    self.failure_count += 1
                    self.last_failure_time = datetime.now(timezone.utc)
                    if self.failure_count >= self.failure_threshold:
                        self.state = "open"
                raise e
    CENTRAL_CIRCUIT_BREAKER_AVAILABLE = False

# -----------------------------------------------------------------------------
# Optional: base expert
# -----------------------------------------------------------------------------
try:
    from .base_expert import BaseExpert
    BASE_EXPERT_AVAILABLE = True
except ImportError:
    class BaseExpert:
        def __init__(self):
            self.expert_name = "sustainability_expert"
            self.supported_task_types = ["propose", "apply_recommendation", "get_thresholds", "set_thresholds"]
            self.health_status = "healthy"
        async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
            raise NotImplementedError()
        def get_capabilities(self) -> Dict[str, Any]:
            return {'name': self.expert_name, 'supported_tasks': self.supported_task_types, 'health': self.health_status}
        def get_metrics(self) -> Dict[str, Any]:
            return {}

# -----------------------------------------------------------------------------
# Optional: bio-inspired core
# -----------------------------------------------------------------------------
try:
    from ...bio_inspired.__init__ import EnhancedBioInspiredCore, BioEvent
    CORE_AVAILABLE = True
except ImportError:
    CORE_AVAILABLE = False
    class BioEvent:
        def __init__(self, event_type, source, data=None):
            self.event_type = event_type
            self.source = source
            self.data = data or {}

# -----------------------------------------------------------------------------
# Optional: pydantic
# -----------------------------------------------------------------------------
try:
    from pydantic import BaseModel, Field
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ============================================================================
# Configuration
# ============================================================================
if PYDANTIC_AVAILABLE:
    class SustainabilityExpertConfig(BaseModel):
        expert_id: str = Field(default_factory=lambda: f"sustainability_{uuid.uuid4().hex[:8]}")
        enable_persistence: bool = True
        enable_predictive_alerts: bool = True
        enable_anomaly_detection: bool = True
        enable_cost_benefit: bool = True
        enable_quantum_bridge: bool = True
        enable_time_tick_engine: bool = True
        enable_swarm_coordination: bool = True
        enable_self_healing: bool = True
        enable_feedback_loop: bool = True
        enable_metrics: bool = True
        human_approval_required: bool = True  # NEW

        thresholds: Dict[str, float] = Field(default_factory=lambda: {
            'carbon_high_threshold': float(os.getenv('SUSTAINABILITY_CARBON_HIGH', '500.0')),
            'helium_scarcity_threshold': float(os.getenv('SUSTAINABILITY_HELIUM_SCARCITY', '0.6')),
            'carbon_price_threshold': float(os.getenv('SUSTAINABILITY_CARBON_PRICE', '80.0')),
            'renewable_share_high': float(os.getenv('SUSTAINABILITY_RENEWABLE_HIGH', '0.8')),
            'renewable_share_low': float(os.getenv('SUSTAINABILITY_RENEWABLE_LOW', '0.4')),
        })

        objective_weights: Dict[str, float] = Field(default_factory=lambda: {
            'carbon_savings': 0.4,
            'helium_savings': 0.3,
            'cost': 0.2,
            'latency': 0.1,
        })

        pareto_grid_resolution: int = 5
else:
    @dataclass
    class SustainabilityExpertConfig:
        expert_id: str = field(default_factory=lambda: f"sustainability_{uuid.uuid4().hex[:8]}")
        enable_persistence: bool = True
        enable_predictive_alerts: bool = True
        enable_anomaly_detection: bool = True
        enable_cost_benefit: bool = True
        enable_quantum_bridge: bool = True
        enable_time_tick_engine: bool = True
        enable_swarm_coordination: bool = True
        enable_self_healing: bool = True
        enable_feedback_loop: bool = True
        enable_metrics: bool = True
        human_approval_required: bool = True  # NEW
        thresholds: Dict[str, float] = field(default_factory=lambda: {
            'carbon_high_threshold': float(os.getenv('SUSTAINABILITY_CARBON_HIGH', '500.0')),
            'helium_scarcity_threshold': float(os.getenv('SUSTAINABILITY_HELIUM_SCARCITY', '0.6')),
            'carbon_price_threshold': float(os.getenv('SUSTAINABILITY_CARBON_PRICE', '80.0')),
            'renewable_share_high': float(os.getenv('SUSTAINABILITY_RENEWABLE_HIGH', '0.8')),
            'renewable_share_low': float(os.getenv('SUSTAINABILITY_RENEWABLE_LOW', '0.4')),
        })
        objective_weights: Dict[str, float] = field(default_factory=lambda: {
            'carbon_savings': 0.4,
            'helium_savings': 0.3,
            'cost': 0.2,
            'latency': 0.1,
        })
        pareto_grid_resolution: int = 5

# ============================================================================
# Supporting simulated managers (unchanged from v3.2)
# ============================================================================
class CarbonIntensityManager:
    def __init__(self, mean=400.0, volatility=10.0, min_val=100.0, max_val=800.0):
        self._intensity = mean
        self._price = 50.0
        self._mean = mean
        self._volatility = volatility
        self._min = min_val
        self._max = max_val
        self._lock = asyncio.Lock()

    async def get_current_intensity(self) -> float:
        async with self._lock:
            change = -0.1 * (self._intensity - self._mean) + np.random.normal(0, self._volatility)
            self._intensity = max(self._min, min(self._max, self._intensity + change))
            return self._intensity

    async def get_current_price(self) -> float:
        async with self._lock:
            self._price = 50.0 + (self._intensity - self._mean) * 0.1
            return self._price

class HeliumProvider:
    def __init__(self, mean_scarcity=0.5, volatility=0.02):
        self._scarcity = mean_scarcity
        self._cost = 1.0
        self._mean = mean_scarcity
        self._volatility = volatility
        self._lock = asyncio.Lock()

    async def get_scarcity(self) -> float:
        async with self._lock:
            change = -0.1 * (self._scarcity - self._mean) + np.random.normal(0, self._volatility)
            self._scarcity = max(0.0, min(1.0, self._scarcity + change))
            return self._scarcity

    async def get_cost_index(self) -> float:
        async with self._lock:
            self._cost = 1.0 + self._scarcity * 0.5
            return self._cost

class PricingManager:
    def __init__(self):
        self._carbon_price = 50.0
        self._helium_price = 0.5
        self._lock = asyncio.Lock()

    async def get_current_prices(self) -> Dict[str, float]:
        async with self._lock:
            self._carbon_price = 50.0 + np.random.normal(0, 5)
            self._helium_price = 0.5 + np.random.normal(0, 0.05)
            return {
                'carbon_price_usd_per_ton': max(10.0, self._carbon_price),
                'helium_price_usd_per_l': max(0.1, self._helium_price)
            }

class PredictiveAnalyzer:
    def __init__(self, window=10):
        self._carbon_history = deque(maxlen=window)
        self._helium_history = deque(maxlen=window)
        self._lock = asyncio.Lock()

    async def record(self, carbon_intensity: float, helium_scarcity: float):
        async with self._lock:
            self._carbon_history.append(carbon_intensity)
            self._helium_history.append(helium_scarcity)

    async def predict_carbon_trend(self) -> Dict[str, Any]:
        async with self._lock:
            if len(self._carbon_history) < 3:
                return {'trend': 'stable', 'confidence': 0.5}
            recent = list(self._carbon_history)
            slope = (recent[-1] - recent[0]) / len(recent)
            if slope > 5:
                trend = 'increasing'
            elif slope < -5:
                trend = 'decreasing'
            else:
                trend = 'stable'
            return {'trend': trend, 'confidence': min(1.0, 0.5 + 0.1 * len(self._carbon_history))}

class CostBenefitEngine:
    async def analyze_scenario(self, action_type: str, params: Dict[str, float]) -> Dict[str, float]:
        if action_type == 'shift_low_carbon':
            carbon_savings = params.get('carbon_savings', 0)
            cost_increase = 5.0
            roi = (carbon_savings * 0.05) / cost_increase
            net_value = carbon_savings * 0.05 - cost_increase
        elif action_type == 'helium_recovery':
            helium_savings = params.get('helium_savings', 0)
            cost = 2.0
            roi = (helium_savings * 1.0) / cost
            net_value = helium_savings * 1.0 - cost
        elif action_type == 'carbon_offsets':
            carbon_savings = params.get('carbon_savings', 0)
            cost = params.get('cost_usd', 0)
            roi = carbon_savings * 0.02 / cost if cost > 0 else 0
            net_value = carbon_savings * 0.02 - cost
        elif action_type == 'increase_renewable':
            renewable_share = params.get('renewable_share', 0.9)
            cost = 3.0
            roi = (renewable_share * 0.1) / cost
            net_value = renewable_share * 0.1 - cost
        else:
            roi = 0.0
            net_value = 0.0
        return {'roi': roi, 'net_value': net_value}

# ============================================================================
# Simple CrossDomainTransfer fallback (if no external class provided)
# ============================================================================
class SimpleCrossDomainTransfer:
    def __init__(self):
        self.transfer_logs = deque(maxlen=100)

    def transfer_knowledge(self, source_domain: str, target_domain: str, knowledge_type: str, data: Dict[str, Any]):
        self.transfer_logs.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'source': source_domain,
            'target': target_domain,
            'type': knowledge_type,
            'data': data
        })

# ============================================================================
# SustainabilityExpert (Main Class) – Enhanced v3.4
# ============================================================================
class SustainabilityExpert(BaseExpert):
    """
    Enhanced Sustainability Expert v3.4
    """

    def __init__(
        self,
        storage: Storage,
        message_queue: AsyncMessageQueue,
        adaptive_cost: AdaptiveCostFunction,
        pareto_gating: ParetoGating,
        drift_detector: DriftDetector,
        metrics: MetricsRegistry,
        bio_core: Optional[Any] = None,
        config: Optional[Union[SustainabilityExpertConfig, Dict[str, Any]]] = None,
        expert_id: Optional[str] = None,
        carbon_manager: Optional[Any] = None,   # NEW: central carbon manager
        helium_manager: Optional[Any] = None,   # NEW: central helium manager
        pricing_manager: Optional[Any] = None,  # NEW: central pricing manager
    ):
        super().__init__()

        # Central components
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        # Config
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

        self.expert_id = self.config.expert_id
        self.version = "3.4.0"
        self.expert_name = "sustainability_expert"
        self.supported_task_types = ["propose", "apply_recommendation", "get_thresholds", "set_thresholds"]

        # Bio-core reference
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
        self.compartment_manager = None
        self.biomass_storage = None

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
            self.compartment_manager = getattr(self.bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(self.bio_core, 'biomass_storage', None)

        # Circuit breakers (central or fallback)
        self._carbon_circuit = EnhancedCircuitBreaker("carbon_manager")
        self._helium_circuit = EnhancedCircuitBreaker("helium_provider")
        self._pricing_circuit = EnhancedCircuitBreaker("pricing_manager")

        # Use provided central managers if available, else simulated ones
        self.carbon_manager = carbon_manager if carbon_manager is not None else CarbonIntensityManager()
        self.helium_provider = helium_manager if helium_manager is not None else HeliumProvider()
        self.pricing_manager = pricing_manager if pricing_manager is not None else PricingManager()

        # Internal state
        self.thresholds = self.config.thresholds.copy()
        self._last_context: Dict[str, Any] = {}
        self.correlation_id = str(uuid.uuid4())
        self.health_status = "healthy"
        self.last_error: Optional[str] = None

        # Predictive analyzer and cost-benefit
        self.predictive_analyzer = PredictiveAnalyzer()
        if not self.cost_benefit_engine:
            self.cost_benefit_engine = CostBenefitEngine()

        # Cross-domain transfer: use fallback if not provided
        self.cross_domain_transfer = getattr(bio_core, 'cross_domain_transfer', None) if bio_core else None
        if not self.cross_domain_transfer:
            self.cross_domain_transfer = SimpleCrossDomainTransfer()

        # Temporal safety: track last drastic action time
        self._last_drastic_action_time: Optional[datetime] = None
        self._drastic_action_cooldown_seconds = 300  # 5 minutes

        # Load thresholds from central storage (safe)
        self._load_thresholds_task = self._create_task(self._load_thresholds())

        # Subscribe to events
        if self.event_broker:
            self._subscribe_events()

        logger.info(f"SustainabilityExpert v{self.version} initialized with ID {self.expert_id}")

    def _create_task(self, coro):
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            logger.warning("No running event loop; background task not started.")
            return None

    async def _load_thresholds(self):
        """Load thresholds from central storage."""
        try:
            data = self.storage.get_state("sustainability_expert_thresholds")
            if data:
                stored = json.loads(data)
                self.thresholds.update(stored)
                logger.info("Loaded thresholds from central storage")
        except Exception as e:
            logger.error(f"Failed to load thresholds: {e}")

    async def _save_thresholds(self):
        """Save thresholds to central storage."""
        try:
            self.storage.save_state("sustainability_expert_thresholds", json.dumps(self.thresholds))
        except Exception as e:
            logger.error(f"Failed to save thresholds: {e}")

    def _subscribe_events(self):
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
        self._last_context['carbon_intensity'] = event.data.get('intensity', 0.5)
        self._last_context['carbon_price'] = event.data.get('price', 50.0)

    async def _on_helium_update(self, event: BioEvent):
        self._last_context['helium_scarcity'] = event.data.get('scarcity', 0.5)
        self._last_context['helium_price'] = event.data.get('price', 0.5)

    async def _on_alert_generated(self, event: BioEvent):
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert received; adjusting sustainability thresholds")
            self.thresholds['carbon_high_threshold'] *= 0.9
            self.thresholds['helium_scarcity_threshold'] *= 0.9
            if self.self_healer:
                await self.self_healer.apply_healing('damage_accumulation')
            await self._adjust_weights_for_alert(event.data)

    async def _adjust_weights_for_alert(self, alert_data: Dict):
        if alert_data.get('category') == 'carbon':
            self.config.objective_weights['carbon_savings'] = min(1.0, self.config.objective_weights['carbon_savings'] + 0.1)
            self._normalize_weights()
        elif alert_data.get('category') == 'helium':
            self.config.objective_weights['helium_savings'] = min(1.0, self.config.objective_weights['helium_savings'] + 0.1)
            self._normalize_weights()

    def _normalize_weights(self):
        total = sum(self.config.objective_weights.values())
        if total > 0:
            for k in self.config.objective_weights:
                self.config.objective_weights[k] /= total
        else:
            # Fallback to equal weights
            n = len(self.config.objective_weights)
            for k in self.config.objective_weights:
                self.config.objective_weights[k] = 1.0 / n

    async def _on_anomaly_detected(self, event: BioEvent):
        if event.data.get('metric') == 'carbon_intensity':
            logger.info("Carbon anomaly detected; adjusting thresholds")
            self.thresholds['carbon_high_threshold'] += 10.0

    async def _on_token_update(self, event: BioEvent):
        self._last_context['token_balance'] = event.data.get('balance', 500)

    async def _on_config_updated(self, event: BioEvent):
        updates = event.data.get('updates', {})
        if 'sustainability_expert' in updates:
            new_config = updates['sustainability_expert']
            if 'thresholds' in new_config:
                self.thresholds.update(new_config['thresholds'])
                await self._save_thresholds()
            if 'objective_weights' in new_config:
                self.config.objective_weights.update(new_config['objective_weights'])
            logger.info("Configuration reloaded", updates=new_config)

    async def _on_health_update(self, event: BioEvent):
        self.health_status = event.data.get('status', 'healthy')

    # ========================================================================
    # Teacher Interface for MOPD (context-aware)
    # ========================================================================
    async def policy_probs(self, state: Dict) -> List[float]:
        """
        Return a probability distribution over sustainability strategies.
        Uses current context to derive realistic metrics.
        """
        strategies = ['shift_low_carbon', 'helium_recovery', 'carbon_offsets', 'increase_renewable']
        candidates = []
        carbon_intensity = self._last_context.get('carbon_intensity', 400.0)
        helium_scarcity = self._last_context.get('helium_scarcity', 0.5)
        carbon_price = self._last_context.get('carbon_price', 50.0)

        for strategy in strategies:
            if strategy == 'shift_low_carbon':
                quality = 0.8
                carbon_g = carbon_intensity * 0.05  # proxy
                latency_ms = 30.0
                energy_joules = 50.0
            elif strategy == 'helium_recovery':
                quality = 0.7
                carbon_g = 5.0
                latency_ms = 80.0
                energy_joules = 40.0
                # higher quality if helium scarce
                quality += helium_scarcity * 0.2
            elif strategy == 'carbon_offsets':
                quality = 0.6
                carbon_g = 50.0
                latency_ms = 10.0
                energy_joules = 20.0
                # higher quality if carbon price high
                quality += (carbon_price / 100.0) * 0.3
            elif strategy == 'increase_renewable':
                quality = 0.75
                carbon_g = 10.0
                latency_ms = 20.0
                energy_joules = 30.0
                # higher quality if renewable share low (from context)
                renewable_share = self._last_context.get('renewable_share', 0.5)
                quality += (1.0 - renewable_share) * 0.2
            else:
                quality = 0.5
                carbon_g = 10.0
                latency_ms = 50.0
                energy_joules = 30.0

            cost = self.adaptive_cost.compute(
                quality=min(1.0, quality),
                carbon_g=carbon_g,
                latency_ms=latency_ms,
                energy_joules=energy_joules,
                health=self.health_status == 'healthy',
                atp=0.5
            )
            candidates.append({
                'strategy': strategy,
                'score': cost,
                'carbon_g': carbon_g,
                'latency_ms': latency_ms,
                'energy_joules': energy_joules,
                'quality_score': min(1.0, quality)
            })

        if self.pareto:
            filtered = self.pareto.filter(candidates)
            if filtered:
                allowed = {c['strategy'] for c in filtered}
                candidates = [c for c in candidates if c['strategy'] in allowed]

        scores = [c['score'] for c in candidates]
        if scores:
            exp_scores = np.exp(scores - np.max(scores))
            probs = exp_scores / np.sum(exp_scores)
            full_probs = [0.0] * len(strategies)
            for c, p in zip(candidates, probs):
                idx = strategies.index(c['strategy'])
                full_probs[idx] = p
            return full_probs
        return [0.25] * 4

    # ========================================================================
    # Core Propose Method
    # ========================================================================
    async def propose_async(self, context: dict) -> dict:
        start_time = time.time()
        self._last_context.update(context)

        try:
            carbon_data = await self._get_carbon_data()
            helium_data = await self._get_helium_data()
            price_data = await self._get_price_data()

            if self.predictive_analyzer:
                await self.predictive_analyzer.record(carbon_data['intensity'], helium_data['scarcity'])

            forecast = await self._get_predictive_forecast()
            if forecast:
                if forecast.get('trend') == 'increasing':
                    carbon_data['intensity'] *= 1.2
                    carbon_data['intensity'] = min(1000, carbon_data['intensity'])
                elif forecast.get('trend') == 'decreasing':
                    carbon_data['intensity'] *= 0.9
                    carbon_data['intensity'] = max(0, carbon_data['intensity'])

            q_penalty_carbon = 0.5
            q_penalty_helium = 0.5
            if self.config.enable_quantum_bridge and self.quantum_bridge:
                try:
                    q_params = self.quantum_bridge.get_qubo_parameters()
                    q_penalty_carbon = q_params.get('penalty_carbon', 0.5)
                    q_penalty_helium = q_params.get('penalty_helium_shortage', 0.5)
                except Exception as e:
                    logger.warning(f"QuantumBridge error: {e}")

            if self.config.enable_time_tick_engine and self.tick_engine:
                if hasattr(self.tick_engine, 'get_helium_forecast'):
                    tick_forecast = self.tick_engine.get_helium_forecast(4)
                    if tick_forecast and len(tick_forecast) > 3:
                        avg_future = np.mean(tick_forecast)
                        if avg_future < 0.3:
                            helium_data['scarcity'] = max(helium_data['scarcity'], 0.8)

            # Generate Pareto front
            pareto_front = await self._generate_pareto_front(
                carbon_intensity=carbon_data['intensity'],
                helium_scarcity=helium_data['scarcity'],
                carbon_price=price_data['carbon_price'],
                helium_price=price_data['helium_price']
            )

            # Select best using adaptive cost + central Pareto
            best_solution = await self._select_best_solution(pareto_front)

            explanation = self._generate_explanation(
                best_solution, carbon_data, helium_data, price_data, pareto_front
            )

            # Human-in-the-loop flag
            requires_approval = False
            if (carbon_data['intensity'] > self.thresholds['carbon_high_threshold'] * 1.5 or
                helium_data['scarcity'] > self.thresholds['helium_scarcity_threshold'] * 1.5):
                requires_approval = True
                # Temporal safety cooldown
                now = datetime.now(timezone.utc)
                if self._last_drastic_action_time and (now - self._last_drastic_action_time).total_seconds() < self._drastic_action_cooldown_seconds:
                    logger.warning("Drastic action within cooldown; requiring human approval.")
                else:
                    self._last_drastic_action_time = now

            # Swarm coordination
            if self.config.enable_swarm_coordination and self.swarm_coordinator:
                swarm_payload = {
                    'expert_id': self.expert_id,
                    'recommendation': best_solution,
                    'carbon_intensity': carbon_data['intensity'],
                    'helium_scarcity': helium_data['scarcity'],
                    'thresholds': self.thresholds,
                    'requires_approval': requires_approval,
                }
                await self.swarm_coordinator.share_predictions(swarm_payload)

            # Cross-domain knowledge transfer
            if self.cross_domain_transfer:
                self.cross_domain_transfer.transfer_knowledge(
                    'sustainability', 'energy', 'efficiency_patterns',
                    {'carbon_intensity': carbon_data['intensity'], 'helium_scarcity': helium_data['scarcity']}
                )

            # Persist state via central storage
            self.storage.save_state(
                "sustainability_expert_last_recommendation",
                json.dumps(best_solution)
            )

            # Bio-inspired integration
            if self.token_manager:
                atp_cost = 0.05
                await self.token_manager.spend(self.expert_id, atp_cost)
                if best_solution.get('carbon_savings', 0) > 0:
                    await self.token_manager.earn(self.expert_id, atp_cost * 2)
            if self.gradient_manager:
                trust_delta = 0.03 if self.health_status == "healthy" else -0.04
                self.gradient_manager.pump_field('trust', trust_delta, source="sustainability_propose")
                if carbon_data['intensity'] > self.thresholds['carbon_high_threshold']:
                    self.gradient_manager.pump_field('carbon', 0.1, source="sustainability_propose")
                if helium_data['scarcity'] > self.thresholds['helium_scarcity_threshold']:
                    self.gradient_manager.pump_field('helium', 0.1, source="sustainability_propose")

            # Update health and metrics
            self.health_status = "healthy"
            self.last_error = None
            self.metrics.increment("sustainability_propose")
            self.metrics.observe("sustainability_propose_latency", (time.time() - start_time) * 1000)

            # Publish FeedbackEvent
            event = FeedbackEvent.create_with_context(
                task_id=f"sustainability_propose_{uuid.uuid4().hex[:8]}",
                selected_action="propose",
                quality_score=best_solution.get('carbon_savings', 0) / 100.0,
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="sustainability",
                adaptive_cost_value=0.0,
                state={'context': context, 'requires_approval': requires_approval},
                candidates=[{'action': 'propose'}],
                source="sustainability_expert",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["sustainability", "proposal"]
            )
            await self.queue.publish("feedback_events", event.to_json())

            # Check drift
            await self._check_drift()

            return {
                'recommendations': best_solution,
                'options': pareto_front,
                'explanation': explanation,
                'requires_approval': requires_approval
            }

        except Exception as e:
            logger.error(f"Error in propose_async: {e}", exc_info=True)
            self.health_status = "degraded"
            self.last_error = str(e)
            self.metrics.increment("sustainability_propose_error")
            fallback = await self._get_fallback_recommendation()
            return {
                'recommendations': fallback,
                'options': [],
                'explanation': f"Due to an error ({e}), the last known good recommendation has been applied.",
                'requires_approval': False
            }

    async def _get_fallback_recommendation(self) -> Dict[str, Any]:
        try:
            data = self.storage.get_state("sustainability_expert_last_recommendation")
            if data:
                return json.loads(data)
        except Exception:
            pass
        return {
            'preferred_data_center': 'us-east',
            'carbon_budget_kg': 10.0,
            'helium_recovery': False,
            'cooling_method': 'standard',
            'carbon_offset': False,
            'offset_amount_kg': 0.0,
            'renewable_share': 0.5,
            'token_stake_recommendation': 0.0,
        }

    # ========================================================================
    # Data Gathering Helpers (standardize carbon units)
    # ========================================================================
    async def _get_carbon_data(self) -> Dict[str, float]:
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
        if ctx_intensity <= 10:  # assume normalized 0-1 or low value
            intensity = ctx_intensity * 800.0
        else:
            intensity = ctx_intensity
        price = self._last_context.get('carbon_price', 50.0)
        return {'intensity': intensity, 'price': price}

    async def _get_helium_data(self) -> Dict[str, float]:
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
                return await self.predictive_analyzer.predict_carbon_trend()
            except Exception as e:
                logger.error(f"Predictive analyzer error: {e}")
        return None

    # ========================================================================
    # Pareto Front Generation (with explanations)
    # ========================================================================
    async def _generate_pareto_front(
        self,
        carbon_intensity: float,
        helium_scarcity: float,
        carbon_price: float,
        helium_price: float
    ) -> List[Dict[str, Any]]:
        feasible_actions = []
        data_centers = ['us-east', 'us-west']
        helium_recovery_options = [False, True]
        carbon_offset_options = [False, True]
        low = self.thresholds['renewable_share_low']
        high = self.thresholds['renewable_share_high']
        renewable_shares = np.linspace(low, high, self.config.pareto_grid_resolution)

        for dc in data_centers:
            for hr in helium_recovery_options:
                for co in carbon_offset_options:
                    for rs in renewable_shares:
                        action = {
                            'preferred_data_center': dc,
                            'helium_recovery': hr,
                            'carbon_offset': co,
                            'renewable_share': float(rs),
                        }
                        obj = self._compute_objectives(action, carbon_intensity, helium_scarcity, carbon_price, helium_price)
                        action.update(obj)
                        # Add explanation
                        reasons = []
                        if dc == 'us-west' and carbon_intensity > 400:
                            reasons.append("Shift to lower-carbon region")
                        if hr:
                            reasons.append("Enable helium recovery")
                        if co:
                            reasons.append("Purchase carbon offsets")
                        if rs > 0.5:
                            reasons.append(f"Increase renewable share to {rs:.0%}")
                        action['explanation'] = "; ".join(reasons) if reasons else "Baseline configuration"
                        feasible_actions.append(action)

        # Filter dominated solutions (true Pareto front)
        pareto = []
        for i, a in enumerate(feasible_actions):
            dominated = False
            for j, b in enumerate(feasible_actions):
                if i == j:
                    continue
                a_vec = (a['carbon_savings'], a['helium_savings'], -a['cost'], -a['latency'])
                b_vec = (b['carbon_savings'], b['helium_savings'], -b['cost'], -b['latency'])
                if all(b_vec[k] >= a_vec[k] for k in range(4)) and any(b_vec[k] > a_vec[k] for k in range(4)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(a)

        # Add ROI/net_value
        for sol in pareto:
            if sol['preferred_data_center'] == 'us-west' and carbon_intensity > 400:
                action_type = 'shift_low_carbon'
                params = {'carbon_savings': sol['carbon_savings']}
            elif sol['helium_recovery']:
                action_type = 'helium_recovery'
                params = {'helium_savings': sol['helium_savings']}
            elif sol['carbon_offset']:
                action_type = 'carbon_offsets'
                params = {'carbon_savings': sol['carbon_savings'], 'cost_usd': sol['cost']}
            else:
                action_type = 'increase_renewable'
                params = {'renewable_share': sol['renewable_share']}
            cb = await self.cost_benefit_engine.analyze_scenario(action_type, params)
            sol['roi'] = cb['roi']
            sol['net_value'] = cb['net_value']

        return pareto

    def _compute_objectives(self, action: Dict, carbon_intensity: float, helium_scarcity: float,
                            carbon_price: float, helium_price: float) -> Dict[str, float]:
        carbon_savings = 0.0
        helium_savings = 0.0
        cost = 0.0
        latency = 0.0

        if action['preferred_data_center'] == 'us-west' and carbon_intensity > 400:
            carbon_savings = (carbon_intensity - 300) * 0.01
        if action['carbon_offset']:
            carbon_savings += 10.0

        if action['helium_recovery']:
            helium_savings = helium_scarcity * 0.5

        cost = 0.0
        if action['preferred_data_center'] == 'us-west':
            cost += 5.0
        if action['helium_recovery']:
            cost += 2.0
        if action['carbon_offset']:
            cost += carbon_price * 0.1
        if action['renewable_share'] > 0.5:
            cost += (action['renewable_share'] - 0.5) * 2.0

        latency = 0.0
        if action['preferred_data_center'] == 'us-west':
            latency += 5.0
        if action['helium_recovery']:
            latency += 10.0

        return {
            'carbon_savings': carbon_savings,
            'helium_savings': helium_savings,
            'cost': cost,
            'latency': latency,
        }

    async def _select_best_solution(self, pareto_front: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not pareto_front:
            return await self._get_fallback_recommendation()

        # Prepare candidates for central ParetoGating
        candidates = []
        for idx, sol in enumerate(pareto_front):
            candidates.append({
                'id': idx,
                'quality_score': sol.get('carbon_savings', 0) / 100.0,
                'carbon_g': sol.get('carbon_savings', 0) * 1000.0,
                'latency_ms': sol.get('latency', 0),
                'energy_joules': sol.get('cost', 0) * 10.0,
                'sol': sol
            })

        # Apply central Pareto filter (if available)
        if self.pareto:
            filtered = self.pareto.filter(candidates)
            if filtered:
                allowed_ids = {c['id'] for c in filtered}
                candidates = [c for c in candidates if c['id'] in allowed_ids]

        # If after filtering no candidates, use original
        if not candidates:
            candidates = [{'id': i, 'quality_score': sol.get('carbon_savings', 0) / 100.0,
                           'carbon_g': sol.get('carbon_savings', 0) * 1000.0,
                           'latency_ms': sol.get('latency', 0),
                           'energy_joules': sol.get('cost', 0) * 10.0,
                           'sol': sol} for i, sol in enumerate(pareto_front)]

        # Score remaining with adaptive cost
        scored = []
        for c in candidates:
            cost = self.adaptive_cost.compute(
                quality=c['quality_score'],
                carbon_g=c['carbon_g'],
                latency_ms=c['latency_ms'],
                energy_joules=c['energy_joules'],
                health=self.health_status == 'healthy',
                atp=0.5
            )
            c['adaptive_cost'] = cost
            scored.append((cost, c['sol']))

        scored.sort(reverse=True, key=lambda x: x[0])
        best = scored[0][1]
        # Add adaptive cost to the solution for reference
        best['adaptive_cost'] = scored[0][0]
        return best

    def _generate_explanation(
        self,
        recommendation: Dict[str, Any],
        carbon_data: Dict[str, float],
        helium_data: Dict[str, float],
        price_data: Dict[str, float],
        pareto_front: List[Dict[str, Any]] = None
    ) -> str:
        parts = []
        carbon_intensity = carbon_data.get('intensity', 400)
        if carbon_intensity > self.thresholds['carbon_high_threshold']:
            parts.append(f"Carbon intensity is high ({carbon_intensity:.0f} g/kWh), so we shifted workload to a lower-carbon region.")
        else:
            parts.append(f"Carbon intensity is moderate ({carbon_intensity:.0f} g/kWh).")
        helium_scarcity = helium_data.get('scarcity', 0.5)
        if helium_scarcity > self.thresholds['helium_scarcity_threshold']:
            parts.append(f"Helium scarcity is high ({helium_scarcity:.2f}), so we enabled helium recovery.")
        carbon_price = price_data.get('carbon_price', 50.0)
        if carbon_price > self.thresholds['carbon_price_threshold']:
            parts.append(f"Carbon price is high (${carbon_price:.2f}/ton), so we recommend purchasing carbon offsets.")
        if recommendation.get('explanation'):
            parts.append(f"Chosen action: {recommendation['explanation']}")
        if pareto_front:
            parts.append(f"Selected from {len(pareto_front)} Pareto-optimal solutions.")
        return " ".join(parts) if parts else "Sustainability metrics are within acceptable ranges."

    # ========================================================================
    # Action Execution with Feedback Loop
    # ========================================================================
    async def apply_recommendation(self, recommendation: Dict[str, Any]) -> bool:
        logger.info(f"Applying recommendation: {recommendation}")
        success = True
        actual_carbon_savings = recommendation.get('carbon_savings', 0.0) * (0.8 + 0.4 * np.random.rand())
        actual_cost = recommendation.get('cost', 0.0) * (0.9 + 0.2 * np.random.rand())

        if self.config.enable_feedback_loop:
            feedback = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'action': recommendation,
                'actual_carbon_savings': actual_carbon_savings,
                'actual_cost': actual_cost,
                'success': success
            }
            self.storage.save_state(
                "sustainability_expert_feedback",
                json.dumps(feedback)
            )
            await self._adapt_from_feedback(feedback)

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"sustainability_apply_{uuid.uuid4().hex[:8]}",
            selected_action="apply_recommendation",
            quality_score=1.0 if success else 0.0,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="sustainability",
            adaptive_cost_value=0.0,
            state={'recommendation': recommendation},
            candidates=[{'action': 'apply'}],
            source="sustainability_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["sustainability", "apply"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        await self._check_drift()
        return success

    async def _adapt_from_feedback(self, feedback: Dict[str, Any]):
        if not feedback['success']:
            self.thresholds['carbon_high_threshold'] *= 0.95
            self.thresholds['helium_scarcity_threshold'] *= 0.95
        else:
            expected_savings = feedback.get('actual_carbon_savings', 0) * 1.2
            if feedback['actual_carbon_savings'] > expected_savings:
                self.thresholds['carbon_high_threshold'] *= 1.02
                self.thresholds['helium_scarcity_threshold'] *= 1.02
        self.thresholds['carbon_high_threshold'] = max(200, min(800, self.thresholds['carbon_high_threshold']))
        self.thresholds['helium_scarcity_threshold'] = max(0.2, min(1.0, self.thresholds['helium_scarcity_threshold']))
        await self._save_thresholds()

    async def _check_drift(self):
        if self.drift:
            try:
                drift_score = await self.drift.check_drift(self.adaptive_cost.get_current_weights())
                if drift_score and drift_score > 0.7:
                    logger.warning(f"High drift detected ({drift_score:.3f}); adjusting thresholds.")
                    self.thresholds['carbon_high_threshold'] *= 0.95
                    self.thresholds['helium_scarcity_threshold'] *= 0.95
                    await self._save_thresholds()
            except Exception as e:
                logger.warning(f"Drift check failed: {e}")

    # ========================================================================
    # New: Metrics (async)
    # ========================================================================
    async def get_metrics(self) -> Dict[str, Any]:
        return {
            'expert_id': self.expert_id,
            'status': self.health_status,
            'last_error': self.last_error,
            'thresholds': self.thresholds,
            'objective_weights': self.config.objective_weights,
            'version': self.version,
        }

    # ========================================================================
    # Health Check & Shutdown
    # ========================================================================
    async def get_health_status(self) -> Dict[str, Any]:
        return {
            'expert_id': self.expert_id,
            'status': self.health_status,
            'last_error': self.last_error,
            'thresholds': self.thresholds,
            'persistence_enabled': self.config.enable_persistence,
        }

    async def shutdown(self):
        logger.info(f"Shutting down SustainabilityExpert {self.expert_id}")
        await self._save_thresholds()
        logger.info("SustainabilityExpert shutdown complete")
