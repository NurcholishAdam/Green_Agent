# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/helium_iot_expert.py
"""
Enhanced Helium IoT Expert v2.0
Integrates real-time helium data, IoT telemetry, predictive analytics,
multi-objective trade-off options, explainable recommendations,
and self-evolving tunable thresholds.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional
from .base_expert import BaseExpert

logger = logging.getLogger(__name__)

# ============================================================================
# (Optional) Import external managers if available
# ============================================================================
try:
    from ...advanced.cross_region_federation import HeliumProvider
    from ...integration.layer_integrator import CarbonIntensityManager
    from ...advanced.self_evolving_gates import EnhancedSelfEvolvingGate
    MANAGERS_AVAILABLE = True
except ImportError:
    MANAGERS_AVAILABLE = False
    logger.warning("Helium IoT Expert: external managers not available - using fallback data")


class HeliumIoTExpert(BaseExpert):
    """
    Helium IoT Expert v2.0
    Provides recommendations for IoT sampling rate, aggregation strategies,
    gateway selection, and power management, using real-time helium data,
    network metrics, predictive analytics, and multi-objective trade-offs.
    """

    # Tunable thresholds (can be evolved by self_evolving_gates)
    DEFAULT_THRESHOLDS = {
        'helium_scarcity_high': 0.6,        # 0‑1 – above this, reduce sampling
        'helium_scarcity_critical': 0.8,    # 0‑1 – above this, enable power‑saving mode
        'network_latency_high': 100.0,      # ms – above this, prefer closer gateways
        'battery_low_threshold': 0.2,       # 0‑1 – below this, reduce sampling
        'sampling_rate_high': 10.0,         # Hz – when helium abundant
        'sampling_rate_low': 5.0,           # Hz – when helium scarce
        'sampling_rate_critical': 2.0       # Hz – when helium critical
    }

    def __init__(self):
        super().__init__()
        self.thresholds = self.DEFAULT_THRESHOLDS.copy()
        self.helium_provider = None
        self.carbon_manager = None
        self.predictive_analyzer = None
        self.self_evolving_gate = None
        self.cross_domain_transfer = None

        # Store the last context for explanations
        self._last_context = {}

    # ========================================================================
    # Dependency Injection
    # ========================================================================

    def set_helium_provider(self, provider):
        """Inject a HeliumProvider instance."""
        self.helium_provider = provider

    def set_carbon_manager(self, manager):
        """Inject a CarbonIntensityManager instance (for carbon‑aware gateways)."""
        self.carbon_manager = manager

    def set_predictive_analyzer(self, analyzer):
        """Inject a PredictiveEvolutionAnalyzer or similar."""
        self.predictive_analyzer = analyzer

    def set_self_evolving_gate(self, gate):
        """Inject a self_evolving_gate to allow threshold evolution."""
        self.self_evolving_gate = gate

    def set_cross_domain_transfer(self, transfer):
        """Inject a cross-domain knowledge transfer module."""
        self.cross_domain_transfer = transfer

    # ========================================================================
    # Threshold Management for Self‑Evolving Gates
    # ========================================================================

    def get_thresholds(self) -> Dict[str, float]:
        """Return current thresholds for evolution."""
        return self.thresholds

    def set_thresholds(self, thresholds: Dict[str, float]):
        """Update thresholds (called by self_evolving_gate)."""
        self.thresholds.update(thresholds)
        logger.info(f"HeliumIoTExpert thresholds updated: {self.thresholds}")

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
        self._last_context = context

        # 1. Gather real‑time data (use injected providers or fallback)
        helium_data = self._get_helium_data()
        network_data = self._get_network_data()
        device_data = self._get_device_data()

        # 2. Apply predictive forecast if available
        if self.predictive_analyzer:
            forecast = await self._get_predictive_forecast()
            if forecast:
                # Adjust helium scarcity based on forecast trend
                if forecast.get('trend') == 'increasing':
                    helium_data['scarcity'] *= 1.2
                elif forecast.get('trend') == 'decreasing':
                    helium_data['scarcity'] *= 0.9

        # 3. Build the primary recommendation using current thresholds
        primary = self._build_recommendation(
            helium_scarcity=helium_data['scarcity'],
            helium_cost=helium_data['cost'],
            network_latency=network_data['latency'],
            battery_level=device_data['battery']
        )

        # 4. Build alternative trade‑off options (multi‑objective)
        options = self._build_tradeoff_options(
            helium_scarcity=helium_data['scarcity'],
            helium_cost=helium_data['cost'],
            network_latency=network_data['latency'],
            battery_level=device_data['battery']
        )

        # 5. Generate natural‑language explanation
        explanation = self._generate_explanation(
            primary, helium_data, network_data, device_data
        )

        # 6. (Optional) Learn from cross‑domain knowledge
        if self.cross_domain_transfer:
            self.cross_domain_transfer.transfer_knowledge(
                'helium_iot',
                'energy',
                'efficiency_patterns',
                {'helium_scarcity': helium_data['scarcity']}
            )

        return {
            'recommendations': primary,
            'options': options,
            'explanation': explanation
        }

    # ========================================================================
    # Data Gathering Helpers
    # ========================================================================

    def _get_helium_data(self) -> Dict[str, float]:
        """Fetch helium scarcity and cost from provider or context."""
        if self.helium_provider:
            try:
                scarcity = self.helium_provider.get_scarcity()
                cost = self.helium_provider.get_cost_index()
                return {'scarcity': scarcity, 'cost': cost}
            except Exception as e:
                logger.error(f"Helium provider error: {e}")

        ctx_scarcity = self._last_context.get('helium_scarcity', 0.5)
        ctx_cost = self._last_context.get('helium_cost_index', 1.0)
        return {'scarcity': ctx_scarcity, 'cost': ctx_cost}

    def _get_network_data(self) -> Dict[str, float]:
        """Fetch network latency and bandwidth from context."""
        return {
            'latency': self._last_context.get('network_latency_ms', 50.0),
            'bandwidth': self._last_context.get('bandwidth_mbps', 100.0)
        }

    def _get_device_data(self) -> Dict[str, float]:
        """Fetch device battery and data quality from context."""
        return {
            'battery': self._last_context.get('battery_level', 0.8),
            'data_quality': self._last_context.get('data_quality', 0.9)
        }

    async def _get_predictive_forecast(self) -> Optional[Dict]:
        """Get forecast from predictive analyzer if available."""
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
    # Recommendation Builders
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
            rec['preferred_gateways'] = ['gateway_nearby']  # simplified
        else:
            rec['preferred_gateways'] = []

        # Battery‑aware sampling override
        if battery_level < self.thresholds['battery_low_threshold']:
            rec['sampling_rate_hz'] = min(rec['sampling_rate_hz'], 2.0)
            rec['power_saving_mode'] = True

        return rec

    def _build_tradeoff_options(
        self,
        helium_scarcity: float,
        helium_cost: float,
        network_latency: float,
        battery_level: float
    ) -> List[Dict[str, Any]]:
        """Generate multiple alternative actions with estimated trade‑offs."""
        options = []

        # Option A: Reduce sampling rate (saves helium, reduces data quality)
        if helium_scarcity > 0.4:
            options.append({
                'action': 'reduce_sampling_rate',
                'estimated_helium_savings_l': helium_scarcity * 0.1,
                'estimated_data_quality_loss': 0.05,
                'priority': 'high'
            })

        # Option B: Switch to compressed aggregation (saves bandwidth, may increase latency)
        if helium_scarcity > 0.3:
            options.append({
                'action': 'enable_compressed_aggregation',
                'estimated_bandwidth_save': 0.3,
                'estimated_latency_increase': 5.0,
                'priority': 'medium'
            })

        # Option C: Use closer gateways (reduces latency, may increase cost)
        if network_latency > 80:
            options.append({
                'action': 'use_closer_gateways',
                'estimated_latency_reduction': 20.0,
                'estimated_cost_increase': 0.1,
                'priority': 'low'
            })

        # Option D: Enable power‑saving mode (extends battery, reduces sampling)
        if battery_level < 0.3:
            options.append({
                'action': 'enable_power_saving',
                'estimated_battery_extension_hours': 24.0,
                'estimated_data_quality_loss': 0.1,
                'priority': 'medium'
            })

        return options

    # ========================================================================
    # Explainability
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

    # Ensure the async method is available for the router if it supports async
    async def propose_async(self, context: dict) -> dict:
        return await self.propose(context)
