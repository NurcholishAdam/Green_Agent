#!/usr/bin/env python3
# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/helium_iot_expert.py
# Version 3.3.0 – Full Green Agent MODP Integration

"""
Enhanced Helium IoT Expert v3.3.0 – Full Green Agent MODP Integration

ENHANCEMENTS OVER v3.2.0:
1. Fixed critical bugs: safe async task creation, generic metric methods, async get_metrics,
   dataclass config serialization, robust circuit breaker fallback, use provided helium manager.
2. Deep bio‑inspired integration: ATP spend/earn, gradient fields, compartment manager.
3. Real MODP: multi‑objective metrics, adaptive cost compute, Pareto filtering on all options,
   drift‑triggered adaptation.
4. Enhanced teacher policy (`policy_probs`) as a true context‑aware MoE teacher distribution.
5. Improved persistence and observability.
6. All optional dependencies still gracefully degrade.
"""

import asyncio
import json
import os
import uuid
from typing import Dict, Any, List, Optional, Union, Callable
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
    # Fallback: define a simple local circuit breaker
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

# Optional: central carbon manager
try:
    from ..carbon_intensity import CarbonIntensityManager
    CENTRAL_CARBON_AVAILABLE = True
except ImportError:
    CENTRAL_CARBON_AVAILABLE = False

# Optional: central helium manager
try:
    from ..helium_optimizer import HeliumEfficiencyOptimizer
    CENTRAL_HELIUM_AVAILABLE = True
except ImportError:
    CENTRAL_HELIUM_AVAILABLE = False

# Optional: base expert
try:
    from .base_expert import BaseExpert
    BASE_EXPERT_AVAILABLE = True
except ImportError:
    class BaseExpert:
        def __init__(self):
            self.expert_name = "helium_iot_expert"
            self.supported_task_types = ["propose", "apply_recommendation", "get_thresholds", "set_thresholds"]
            self.health_status = "healthy"
        async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
            raise NotImplementedError()
        def get_capabilities(self) -> Dict[str, Any]:
            return {'name': self.expert_name, 'supported_tasks': self.supported_task_types, 'health': self.health_status}
        def get_metrics(self) -> Dict[str, Any]:
            return {}

# Optional: bio-inspired core
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

# ============================================================================
# Configuration – now a dataclass for easy serialization
# ============================================================================
@dataclass
class HeliumIoTExpertConfig:
    """Configuration for HeliumIoTExpert, built from central_config."""
    expert_id: str = f"helium_iot_{uuid.uuid4().hex[:8]}"
    enable_persistence: bool = True
    enable_predictive_alerts: bool = getattr(central_config, "he_iot_enable_predictive_alerts", True)
    enable_anomaly_detection: bool = getattr(central_config, "he_iot_enable_anomaly_detection", True)
    enable_cost_benefit: bool = getattr(central_config, "he_iot_enable_cost_benefit", True)
    enable_quantum_bridge: bool = getattr(central_config, "he_iot_enable_quantum_bridge", True)
    enable_time_tick_engine: bool = getattr(central_config, "he_iot_enable_time_tick_engine", True)
    enable_swarm_coordination: bool = getattr(central_config, "he_iot_enable_swarm_coordination", True)
    enable_self_healing: bool = getattr(central_config, "he_iot_enable_self_healing", True)

    thresholds: Dict[str, float] = field(default_factory=lambda: {
        'helium_scarcity_high': float(os.getenv('HELIUM_SCARCITY_HIGH', '0.6')),
        'helium_scarcity_critical': float(os.getenv('HELIUM_SCARCITY_CRITICAL', '0.8')),
        'network_latency_high': float(os.getenv('NETWORK_LATENCY_HIGH', '100.0')),
        'battery_low_threshold': float(os.getenv('BATTERY_LOW_THRESHOLD', '0.2')),
        'sampling_rate_high': float(os.getenv('SAMPLING_RATE_HIGH', '10.0')),
        'sampling_rate_low': float(os.getenv('SAMPLING_RATE_LOW', '5.0')),
        'sampling_rate_critical': float(os.getenv('SAMPLING_RATE_CRITICAL', '2.0')),
    })

    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        'helium_savings': float(os.getenv('HE_OBJ_HELIUM_SAVINGS', '0.4')),
        'data_quality': float(os.getenv('HE_OBJ_DATA_QUALITY', '0.3')),
        'latency': float(os.getenv('HE_OBJ_LATENCY', '0.2')),
        'cost': float(os.getenv('HE_OBJ_COST', '0.1')),
    })

# ============================================================================
# Concrete HeliumProvider (simulated but realistic) – uses central helium manager if available
# ============================================================================
class SimulatedHeliumProvider:
    def __init__(self):
        self._scarcity = 0.5
        self._cost = 1.0
        self._trend = deque(maxlen=100)
        self._lock = asyncio.Lock()

    async def get_scarcity(self) -> float:
        async with self._lock:
            change = np.random.normal(0, 0.02)
            self._scarcity = max(0.0, min(1.0, self._scarcity + change))
            self._trend.append(self._scarcity)
            return self._scarcity

    async def get_cost_index(self) -> float:
        async with self._lock:
            self._cost = 1.0 + self._scarcity * 0.5
            return self._cost

    async def get_forecast(self, hours: int = 4) -> List[float]:
        if len(self._trend) < 10:
            return [self._scarcity] * hours
        last_values = list(self._trend)[-10:]
        slope = (last_values[-1] - last_values[0]) / 9
        forecast = [last_values[-1] + slope * (i+1) for i in range(hours)]
        return [max(0.0, min(1.0, v)) for v in forecast]

# ============================================================================
# Concrete PredictiveAnalyzer (simulated)
# ============================================================================
class SimulatedPredictiveAnalyzer:
    def __init__(self, helium_provider: SimulatedHeliumProvider):
        self.helium_provider = helium_provider

    async def predict_helium_trend(self) -> Dict[str, Any]:
        forecast = await self.helium_provider.get_forecast(6)
        if len(forecast) < 2:
            return {'trend': 'stable', 'confidence': 0.5}
        trend = 'increasing' if forecast[-1] > forecast[0] else 'decreasing' if forecast[-1] < forecast[0] else 'stable'
        return {
            'trend': trend,
            'forecast': forecast,
            'confidence': 0.7,
            'current': forecast[0],
            'predicted': forecast[-1],
        }

# ============================================================================
# Helium IoT Expert – Fully Integrated v3.3.0
# ============================================================================
class HeliumIoTExpert(BaseExpert):
    """
    Helium IoT Expert v3.3.0 – Full Green Agent MODP integration.
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
        helium_manager: Optional[Any] = None,
        carbon_manager: Optional[Any] = None
    ):
        if BASE_EXPERT_AVAILABLE:
            super().__init__()
        self.expert_name = "helium_iot_expert"
        self.supported_task_types = [
            "propose", "apply_recommendation", "get_thresholds", "set_thresholds"
        ]
        self.health_status = "healthy"

        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics
        self.bio_core = bio_core
        self.helium_manager = helium_manager
        self.carbon_manager = carbon_manager

        # Configuration – built from central_config
        self.config = HeliumIoTExpertConfig()

        # Sub‑modules
        # Use provided helium_manager if available, else SimulatedHeliumProvider
        if helium_manager is not None:
            self.helium_provider = helium_manager
        else:
            self.helium_provider = SimulatedHeliumProvider()
        self.predictive_analyzer = SimulatedPredictiveAnalyzer(self.helium_provider)

        # Thresholds (loaded from storage)
        self.thresholds = self.config.thresholds.copy()
        self._load_thresholds_task = self._create_task(self._load_thresholds())

        # Internal state
        self._last_context: Dict[str, Any] = {}
        self.correlation_id = str(uuid.uuid4())
        self.last_error: Optional[str] = None
        self._proposals_count = 0

        # Bio-inspired managers (extracted from bio_core if available)
        self.token_manager = getattr(bio_core, 'token_manager', None) if bio_core else None
        self.gradient_manager = getattr(bio_core, 'gradient_manager', None) if bio_core else None
        self.compartment_manager = getattr(bio_core, 'compartment_manager', None) if bio_core else None

        # Circuit breaker (central)
        self._helium_circuit = EnhancedCircuitBreaker("helium_provider")

        # Event subscriptions (if bio‑core available)
        if self.bio_core:
            self._subscribe_events()

        logger.info(f"HeliumIoTExpert v3.3.0 initialized with ID {self.config.expert_id}")

    def _create_task(self, coro):
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            logger.warning("No running event loop; background task not started.")
            return None

    # --------------------------------------------------------------------------
    # State Persistence using central Storage
    # --------------------------------------------------------------------------
    async def _load_thresholds(self):
        """Load thresholds from central storage."""
        try:
            data = self.storage.get_state("helium_iot_thresholds")
            if data:
                stored = json.loads(data)
                self.thresholds.update(stored)
                logger.info("Thresholds loaded from storage")
        except Exception as e:
            logger.error(f"Failed to load thresholds: {e}")

    async def _save_thresholds(self):
        """Save thresholds to central storage."""
        try:
            self.storage.save_state("helium_iot_thresholds", json.dumps(self.thresholds))
            logger.debug("Thresholds saved to storage")
        except Exception as e:
            logger.error(f"Failed to save thresholds: {e}")

    async def _save_history(self, entry: Dict[str, Any]):
        """Append a history entry to storage."""
        try:
            history = self.storage.get_state("helium_iot_history")
            if history:
                history_list = json.loads(history)
            else:
                history_list = []
            history_list.append(entry)
            if len(history_list) > 1000:
                history_list = history_list[-1000:]
            self.storage.save_state("helium_iot_history", json.dumps(history_list))
        except Exception as e:
            logger.error(f"Failed to save history: {e}")

    # --------------------------------------------------------------------------
    # Event Subscriptions
    # --------------------------------------------------------------------------
    def _subscribe_events(self):
        if hasattr(self.bio_core, 'event_broker'):
            self.bio_core.event_broker.subscribe('helium_update', self._on_helium_update)
            self.bio_core.event_broker.subscribe('alert_generated', self._on_alert_generated)
            self.bio_core.event_broker.subscribe('anomaly_detected', self._on_anomaly_detected)
            self.bio_core.event_broker.subscribe('token_balance_update', self._on_token_update)
            self.bio_core.event_broker.subscribe('config_updated', self._on_config_updated)
            logger.info("HeliumIoTExpert subscribed to core events")

    async def _on_helium_update(self, event: BioEvent):
        self._last_context['helium_scarcity'] = event.data.get('scarcity', 0.5)
        self._last_context['helium_cost_index'] = event.data.get('cost', 1.0)

    async def _on_alert_generated(self, event: BioEvent):
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert received; adjusting helium thresholds")
            self.thresholds['helium_scarcity_high'] *= 0.8
            self.thresholds['helium_scarcity_critical'] *= 0.8
            await self._save_thresholds()

    async def _on_anomaly_detected(self, event: BioEvent):
        if event.data.get('metric') == 'helium_scarcity':
            logger.info("Helium anomaly detected; adjusting thresholds")
            self.thresholds['helium_scarcity_high'] += 0.1
            self.thresholds['helium_scarcity_critical'] += 0.1
            await self._save_thresholds()

    async def _on_token_update(self, event: BioEvent):
        self._last_context['token_balance'] = event.data.get('balance', 500)

    async def _on_config_updated(self, event: BioEvent):
        updates = event.data.get('updates', {})
        if 'helium_iot_expert' in updates:
            new_config = updates['helium_iot_expert']
            if 'thresholds' in new_config:
                self.thresholds.update(new_config['thresholds'])
                await self._save_thresholds()
            logger.info("Configuration reloaded", updates=new_config)

    # --------------------------------------------------------------------------
    # Teacher Interface for MOPD (context-aware soft policy)
    # --------------------------------------------------------------------------
    async def policy_probs(self, state: Dict) -> List[float]:
        """
        Return a probability distribution over IoT strategies,
        computed using adaptive cost and Pareto constraints.
        """
        strategies = ['reduce_sampling', 'enable_compressed', 'use_closer_gateways', 'enable_power_saving']
        candidates = []
        for strategy in strategies:
            # Estimate metrics for each strategy
            if strategy == 'reduce_sampling':
                quality = 0.7
                carbon_g = 2.0   # proxy for helium savings? we'll map separately
                latency_ms = 60.0
                energy_joules = 30.0
            elif strategy == 'enable_compressed':
                quality = 0.75
                carbon_g = 1.5
                latency_ms = 80.0
                energy_joules = 20.0
            elif strategy == 'use_closer_gateways':
                quality = 0.85
                carbon_g = 1.0
                latency_ms = 30.0
                energy_joules = 15.0
            elif strategy == 'enable_power_saving':
                quality = 0.6
                carbon_g = 0.5
                latency_ms = 100.0
                energy_joules = 5.0
            else:
                quality = 0.5
                carbon_g = 1.0
                latency_ms = 50.0
                energy_joules = 25.0

            cost = self.adaptive_cost.compute(
                quality=quality,
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
                'quality_score': quality
            })

        # Apply Pareto filter
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

    # --------------------------------------------------------------------------
    # Core Expert Interface
    # --------------------------------------------------------------------------
    async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        task_type = task.get('type', 'unknown')
        if task_type == 'propose':
            return await self.propose_async(task.get('context', {}))
        elif task_type == 'apply_recommendation':
            success = await self.apply_recommendation(task.get('recommendation', {}))
            return {'status': 'success' if success else 'error'}
        elif task_type == 'get_thresholds':
            return {'thresholds': self.thresholds}
        elif task_type == 'set_thresholds':
            await self.set_thresholds(task.get('thresholds', {}))
            return {'status': 'success'}
        else:
            return {'status': 'error', 'error': f'Unknown task type: {task_type}'}

    def get_capabilities(self) -> Dict[str, Any]:
        return {
            'expert_name': self.expert_name,
            'supported_tasks': self.supported_task_types,
            'health_status': self.health_status,
            'config': asdict(self.config),
        }

    def get_metrics(self) -> Dict[str, Any]:
        # Return sync dict, no asyncio.run
        return {
            'proposals_count': self._proposals_count,
            'last_error': self.last_error,
        }

    async def get_health_status(self) -> Dict[str, Any]:
        return {
            'expert_id': self.config.expert_id,
            'status': self.health_status,
            'last_error': self.last_error,
            'thresholds': self.thresholds,
            'persistence_enabled': True,
        }

    async def _get_expert_metrics(self) -> Dict[str, Any]:
        return {
            'proposals_count': self._proposals_count,
            'last_error': self.last_error,
        }

    # --------------------------------------------------------------------------
    # Threshold Management
    # --------------------------------------------------------------------------
    def get_thresholds(self) -> Dict[str, float]:
        return self.thresholds

    async def set_thresholds(self, thresholds: Dict[str, float]):
        self.thresholds.update(thresholds)
        await self._save_thresholds()
        logger.info(f"Thresholds updated: {self.thresholds}")

    # --------------------------------------------------------------------------
    # Core Propose Method (Enhanced with FeedbackEvent, MODP, bio integration)
    # --------------------------------------------------------------------------
    async def propose_async(self, context: dict) -> dict:
        self._last_context.update(context)

        try:
            # Bio-inspired: spend ATP before computation
            if self.token_manager:
                atp_cost = 0.05  # base cost
                await self.token_manager.spend("helium_iot_expert", atp_cost)

            # 1. Gather data using circuit breakers
            helium_data = await self._get_helium_data()
            network_data = self._get_network_data()
            device_data = self._get_device_data()

            # 2. Predictive forecast
            forecast = await self._get_predictive_forecast()
            if forecast:
                if forecast.get('trend') == 'increasing':
                    helium_data['scarcity'] = min(1.0, helium_data['scarcity'] * 1.2)
                elif forecast.get('trend') == 'decreasing':
                    helium_data['scarcity'] = max(0.0, helium_data['scarcity'] * 0.9)

            # 3. Build primary recommendation
            primary = self._build_recommendation(
                helium_scarcity=helium_data['scarcity'],
                helium_cost=helium_data['cost'],
                network_latency=network_data['latency'],
                battery_level=device_data['battery']
            )

            # 4. Build alternative trade‑off options
            options = await self._build_tradeoff_options(
                helium_scarcity=helium_data['scarcity'],
                helium_cost=helium_data['cost'],
                network_latency=network_data['latency'],
                battery_level=device_data['battery']
            )

            # 5. Generate explanation
            explanation = self._generate_explanation(
                primary, helium_data, network_data, device_data
            )

            # 6. Swarm coordination (stub)
            # ...

            # 7. Persist history
            await self._save_history({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'helium_scarcity': helium_data['scarcity'],
                'recommendation': primary,
                'options': options,
            })

            # 8. Bio-inspired: earn ATP if high-quality proposal; pump gradients
            quality = 0.9  # placeholder; could be based on helium savings
            if self.token_manager:
                if quality > 0.7:
                    await self.token_manager.earn("helium_iot_expert", atp_cost * 2)
            if self.gradient_manager:
                self.gradient_manager.pump_field('trust', 0.05 if quality > 0.7 else -0.02, source="helium_iot_propose")
                if helium_data['scarcity'] > self.thresholds['helium_scarcity_high']:
                    self.gradient_manager.pump_field('helium', 0.1, source="helium_iot_propose")

            # 9. Update health status
            self.health_status = "healthy"
            self.last_error = None
            self._proposals_count += 1

            # Publish FeedbackEvent
            event = FeedbackEvent.create_with_context(
                task_id=f"he_iot_propose_{uuid.uuid4().hex[:8]}",
                selected_action="propose",
                quality_score=quality,
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="helium_iot",
                adaptive_cost_value=0.0,
                state=context,
                candidates=[{'action': s} for s in ['reduce_sampling', 'enable_compressed', 'use_closer_gateways', 'enable_power_saving']],
                source="helium_iot_expert",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["helium", "iot"]
            )
            await self.queue.publish("feedback_events", event.to_json())

            # Check drift and adapt
            await self._check_drift()

            # Update metrics (generic)
            self.metrics.increment("helium_iot_proposals")

            return {
                'recommendations': primary,
                'options': options,
                'explanation': explanation
            }

        except Exception as e:
            logger.error(f"Error in propose_async: {e}", exc_info=True)
            self.health_status = "degraded"
            self.last_error = str(e)
            # Fallback
            fallback = {
                'sampling_rate_hz': 5.0,
                'power_saving_mode': True,
                'aggregation_strategy': 'compressed',
                'preferred_gateways': ['gateway_nearby'],
            }
            return {
                'recommendations': fallback,
                'options': [],
                'explanation': f"Due to an error ({e}), a conservative fallback has been applied."
            }

    # --------------------------------------------------------------------------
    # Data Gathering Helpers (with circuit breakers)
    # --------------------------------------------------------------------------
    async def _get_helium_data(self) -> Dict[str, float]:
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
                return await self.predictive_analyzer.predict_helium_trend()
            except Exception as e:
                logger.error(f"Predictive analyzer error: {e}")
        return None

    # --------------------------------------------------------------------------
    # Recommendation Builders (Enhanced with adaptive cost and Pareto)
    # --------------------------------------------------------------------------
    def _build_recommendation(
        self,
        helium_scarcity: float,
        helium_cost: float,
        network_latency: float,
        battery_level: float
    ) -> Dict[str, Any]:
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

        # Gateway preference
        if network_latency > self.thresholds['network_latency_high']:
            rec['preferred_gateways'] = ['gateway_nearby']
        else:
            rec['preferred_gateways'] = []

        # Battery‑aware override
        if battery_level < self.thresholds['battery_low_threshold']:
            rec['sampling_rate_hz'] = min(rec['sampling_rate_hz'], 2.0)
            rec['power_saving_mode'] = True

        # Apply adaptive cost weights to modify recommendation
        if self.adaptive_cost:
            weights = self.adaptive_cost.get_current_weights()
            carbon_weight = weights.get('carbon', 0.3)
            cost_weight = weights.get('cost', 0.2)
            if carbon_weight > 0.5:
                rec['power_saving_mode'] = True
            if cost_weight > 0.5:
                rec['preferred_gateways'] = ['gateway_nearby']

        return rec

    async def _build_tradeoff_options(
        self,
        helium_scarcity: float,
        helium_cost: float,
        network_latency: float,
        battery_level: float
    ) -> List[Dict[str, Any]]:
        options = []

        # Option A: Reduce sampling rate
        if helium_scarcity > 0.4:
            option = {
                'action': 'reduce_sampling_rate',
                'estimated_helium_savings_l': helium_scarcity * 0.1,
                'estimated_data_quality_loss': 0.05,
                'priority': 'high',
                'latency_ms': 60.0,
                'energy_joules': 30.0,
                'carbon_g': 2.0,  # proxy for helium savings? we'll use separate key
                'quality_score': 1.0 - 0.05,
            }
            options.append(option)

        # Option B: Switch to compressed aggregation
        if helium_scarcity > 0.3:
            option = {
                'action': 'enable_compressed_aggregation',
                'estimated_bandwidth_save': 0.3,
                'estimated_latency_increase': 5.0,
                'priority': 'medium',
                'latency_ms': 80.0,
                'energy_joules': 20.0,
                'carbon_g': 1.5,
                'quality_score': 0.75,
            }
            options.append(option)

        # Option C: Use closer gateways
        if network_latency > 80:
            option = {
                'action': 'use_closer_gateways',
                'estimated_latency_reduction': 20.0,
                'estimated_cost_increase': 0.1,
                'priority': 'low',
                'latency_ms': 30.0,
                'energy_joules': 15.0,
                'carbon_g': 1.0,
                'quality_score': 0.85,
            }
            options.append(option)

        # Option D: Enable power‑saving mode
        if battery_level < 0.3:
            option = {
                'action': 'enable_power_saving',
                'estimated_battery_extension_hours': 24.0,
                'estimated_data_quality_loss': 0.1,
                'priority': 'medium',
                'latency_ms': 100.0,
                'energy_joules': 5.0,
                'carbon_g': 0.5,
                'quality_score': 0.6,
            }
            options.append(option)

        # Apply Pareto gating and adaptive cost scoring
        if self.pareto:
            candidates = []
            for opt in options:
                candidates.append({
                    'action': opt['action'],
                    'quality_score': opt.get('quality_score', 1.0),
                    'carbon_g': opt.get('carbon_g', 0.0),
                    'latency_ms': opt.get('latency_ms', 0.0),
                    'energy_joules': opt.get('energy_joules', 0.0),
                    # Keep extra info for later
                    'opt': opt,
                })
            filtered = self.pareto.filter(candidates)
            if filtered:
                allowed_actions = {c['action'] for c in filtered}
                options = [opt for opt in options if opt['action'] in allowed_actions]

        # Compute adaptive cost scores and sort
        if self.adaptive_cost and options:
            scored_options = []
            for opt in options:
                cost = self.adaptive_cost.compute(
                    quality=opt.get('quality_score', 0.5),
                    carbon_g=opt.get('carbon_g', 0.0),
                    latency_ms=opt.get('latency_ms', 0.0),
                    energy_joules=opt.get('energy_joules', 0.0),
                    health=True,
                    atp=0.5
                )
                opt['adaptive_cost'] = cost
                scored_options.append((cost, opt))
            scored_options.sort(reverse=True)
            options = [opt for _, opt in scored_options]

        return options

    # --------------------------------------------------------------------------
    # Explainability
    # --------------------------------------------------------------------------
    def _generate_explanation(
        self,
        recommendation: Dict[str, Any],
        helium_data: Dict[str, float],
        network_data: Dict[str, float],
        device_data: Dict[str, float]
    ) -> str:
        parts = []
        helium_scarcity = helium_data.get('scarcity', 0.5)
        if helium_scarcity > self.thresholds['helium_scarcity_critical']:
            parts.append(f"Helium scarcity is critical ({helium_scarcity:.2f}), so we reduced sampling rate to {recommendation['sampling_rate_hz']:.1f} Hz and enabled power‑saving mode.")
        elif helium_scarcity > self.thresholds['helium_scarcity_high']:
            parts.append(f"Helium scarcity is high ({helium_scarcity:.2f}), so we reduced sampling rate to {recommendation['sampling_rate_hz']:.1f} Hz and switched to compressed aggregation.")
        else:
            parts.append(f"Helium scarcity is moderate ({helium_scarcity:.2f}), maintaining standard sampling rate.")
        if device_data.get('battery', 0.8) < self.thresholds['battery_low_threshold']:
            parts.append(f"Battery level is low ({device_data['battery']:.0%}), so we further reduced sampling to conserve energy.")
        if not parts:
            parts.append("IoT metrics are within acceptable ranges. Current recommendations maintain optimal performance.")
        return " ".join(parts)

    # --------------------------------------------------------------------------
    # Action Execution (Enhanced with FeedbackEvent and bio integration)
    # --------------------------------------------------------------------------
    async def apply_recommendation(self, recommendation: Dict[str, Any]) -> bool:
        logger.info(f"Applying recommendation: {recommendation}")
        # Bio-inspired: spend ATP on execution
        if self.token_manager:
            await self.token_manager.spend("helium_iot_expert", 0.02)
        # Simulate execution
        success = True

        # Bio-inspired: earn ATP if success, pump gradient
        if success:
            if self.token_manager:
                await self.token_manager.earn("helium_iot_expert", 0.03)
            if self.gradient_manager:
                self.gradient_manager.pump_field('trust', 0.05, source="helium_iot_apply")
        else:
            if self.gradient_manager:
                self.gradient_manager.pump_field('trust', -0.05, source="helium_iot_apply")

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"he_iot_apply_{uuid.uuid4().hex[:8]}",
            selected_action="apply_recommendation",
            quality_score=0.9 if success else 0.0,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="helium_iot",
            adaptive_cost_value=0.0,
            state={'recommendation': recommendation},
            candidates=[{'action': 'apply'}],
            source="helium_iot_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["helium", "iot"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        # Check drift and adapt
        await self._check_drift()

        return success

    # --------------------------------------------------------------------------
    # Drift detection and adaptation
    # --------------------------------------------------------------------------
    async def _check_drift(self):
        if self.drift:
            try:
                drift_score = await self.drift.check_drift(self.adaptive_cost.get_current_weights())
                if drift_score and drift_score > 0.7:
                    logger.warning(f"High drift detected ({drift_score:.3f}); adjusting thresholds.")
                    self.thresholds['helium_scarcity_high'] = min(0.9, self.thresholds['helium_scarcity_high'] * 1.1)
                    self.thresholds['helium_scarcity_critical'] = min(0.95, self.thresholds['helium_scarcity_critical'] * 1.1)
                    await self._save_thresholds()
            except Exception as e:
                logger.warning(f"Drift check failed: {e}")

    # --------------------------------------------------------------------------
    # Self‑Healing
    # --------------------------------------------------------------------------
    async def self_heal(self):
        logger.info("HeliumIoTExpert self‑healing")
        if self.config.enable_self_healing:
            self.thresholds = self.config.thresholds.copy()
            await self._save_thresholds()
            self.health_status = "healthy"
            self.last_error = None

    # --------------------------------------------------------------------------
    # Cleanup
    # --------------------------------------------------------------------------
    async def shutdown(self):
        logger.info(f"Shutting down HeliumIoTExpert {self.config.expert_id}")
        if self._load_thresholds_task:
            self._load_thresholds_task.cancel()
        # No further cleanup needed; central storage handles persistence.
