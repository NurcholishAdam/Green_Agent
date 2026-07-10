# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/fl_energy_expert.py
"""
Enhanced FL Energy Expert v2.0
Integrates real-time carbon/helium data, federated orchestrator metrics,
predictive analytics, multi-objective trade-off options,
explainable recommendations, and self-evolving tunable thresholds.
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
    from ...integration.layer_integrator import CarbonIntensityManager
    from ...advanced.cross_region_federation import HeliumProvider, EconomicPricingManager
    from ...advanced.federated_experts import EnhancedFederatedOrchestrator
    from ...advanced.self_evolving_gates import EnhancedSelfEvolvingGate
    MANAGERS_AVAILABLE = True
except ImportError:
    MANAGERS_AVAILABLE = False
    logger.warning("FL Energy Expert: external managers not available - using fallback data")


class FLEnergyExpert(BaseExpert):
    """
    FL Energy Expert v2.0
    Provides recommendations for federated learning round frequency, client selection,
    compression levels, model pruning, and token incentives, using real-time data,
    predictive analytics, and multi-objective trade-offs.
    """

    # Tunable thresholds (can be evolved by self_evolving_gates)
    DEFAULT_THRESHOLDS = {
        'carbon_high_threshold': 500.0,      # g/kWh – above this, reduce round frequency
        'helium_scarcity_threshold': 0.6,    # 0‑1 – above this, increase compression
        'accuracy_loss_limit': 0.05,         # maximum acceptable accuracy loss
        'energy_save_target': 0.2,           # target energy savings ratio
        'round_frequency_high': 1.0,         # when carbon < threshold
        'round_frequency_low': 0.5           # when carbon >= threshold
    }

    def __init__(self):
        super().__init__()
        self.thresholds = self.DEFAULT_THRESHOLDS.copy()
        self.carbon_manager = None
        self.helium_provider = None
        self.pricing_manager = None
        self.federated_orchestrator = None
        self.predictive_analyzer = None
        self.self_evolving_gate = None
        self.cross_domain_transfer = None

        # Store the last context for explanations
        self._last_context = {}

    # ========================================================================
    # Dependency Injection
    # ========================================================================

    def set_carbon_manager(self, manager):
        """Inject a CarbonIntensityManager instance."""
        self.carbon_manager = manager

    def set_helium_provider(self, provider):
        """Inject a HeliumProvider instance."""
        self.helium_provider = provider

    def set_pricing_manager(self, manager):
        """Inject an EconomicPricingManager instance."""
        self.pricing_manager = manager

    def set_federated_orchestrator(self, orchestrator):
        """Inject a federated orchestrator to get FL metrics."""
        self.federated_orchestrator = orchestrator

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
        logger.info(f"FLEnergyExpert thresholds updated: {self.thresholds}")

    # ========================================================================
    # Core Propose Method (Async)
    # ========================================================================

    async def propose(self, context: dict) -> dict:
        """
        Generate FL energy recommendations based on real-time and predictive data.
        Returns a dict with:
          - 'recommendations': single preferred action set
          - 'options': list of trade-off options (for router to choose)
          - 'explanation': natural‑language description
        """
        self._last_context = context

        # 1. Gather real‑time data (use injected managers or fallback)
        carbon_data = await self._get_carbon_data()
        helium_data = self._get_helium_data()
        price_data = await self._get_price_data()
        fl_metrics = self._get_fl_metrics()

        # 2. Apply predictive forecast if available
        if self.predictive_analyzer:
            forecast = await self._get_predictive_forecast()
            if forecast:
                # Adjust carbon intensity based on forecast trend
                if forecast.get('trend') == 'increasing':
                    carbon_data['intensity'] *= 1.2
                elif forecast.get('trend') == 'decreasing':
                    carbon_data['intensity'] *= 0.9

        # 3. Build the primary recommendation using current thresholds
        primary = self._build_recommendation(
            carbon_intensity=carbon_data['intensity'],
            helium_scarcity=helium_data['scarcity'],
            carbon_price=price_data['carbon_price'],
            helium_price=price_data['helium_price'],
            fl_metrics=fl_metrics
        )

        # 4. Build alternative trade‑off options (multi‑objective)
        options = self._build_tradeoff_options(
            carbon_intensity=carbon_data['intensity'],
            helium_scarcity=helium_data['scarcity'],
            carbon_price=price_data['carbon_price'],
            helium_price=price_data['helium_price'],
            fl_metrics=fl_metrics
        )

        # 5. Generate natural‑language explanation
        explanation = self._generate_explanation(
            primary, carbon_data, helium_data, price_data, fl_metrics
        )

        # 6. (Optional) Learn from cross‑domain knowledge
        if self.cross_domain_transfer:
            self.cross_domain_transfer.transfer_knowledge(
                'fl_energy',
                'energy',
                'efficiency_patterns',
                {'carbon_intensity': carbon_data['intensity'],
                 'helium_scarcity': helium_data['scarcity']}
            )

        return {
            'recommendations': primary,
            'options': options,
            'explanation': explanation
        }

    # ========================================================================
    # Data Gathering Helpers
    # ========================================================================

    async def _get_carbon_data(self) -> Dict[str, float]:
        """Fetch carbon intensity and price from manager or context."""
        if self.carbon_manager:
            try:
                intensity = await self.carbon_manager.get_current_intensity()
                price = await self.carbon_manager.get_current_price()
                return {'intensity': intensity, 'price': price}
            except Exception as e:
                logger.error(f"Carbon manager error: {e}")

        # Fallback to context or defaults
        ctx_intensity = self._last_context.get('carbon_intensity', 0.5)
        # Convert normalized [0,1] to g/kWh (assuming 0.5 → 400)
        intensity = ctx_intensity * 800.0 if ctx_intensity <= 1.0 else ctx_intensity
        price = self._last_context.get('carbon_price', 50.0)
        return {'intensity': intensity, 'price': price}

    def _get_helium_data(self) -> Dict[str, float]:
        """Fetch helium scarcity and price from provider or context."""
        if self.helium_provider:
            try:
                scarcity = self.helium_provider.get_scarcity()
                cost = self.helium_provider.get_cost_index()
                # price from provider if available, else fallback
                return {'scarcity': scarcity, 'price': cost * 0.5}
            except Exception as e:
                logger.error(f"Helium provider error: {e}")

        ctx_scarcity = self._last_context.get('helium_scarcity', 0.5)
        ctx_price = self._last_context.get('helium_price', 0.5)
        return {'scarcity': ctx_scarcity, 'price': ctx_price}

    async def _get_price_data(self) -> Dict[str, float]:
        """Fetch carbon/helium prices from pricing manager."""
        if self.pricing_manager:
            try:
                prices = await self.pricing_manager.get_current_prices()
                return {
                    'carbon_price': prices.get('carbon_price_usd_per_ton', 50.0),
                    'helium_price': prices.get('helium_price_usd_per_l', 0.5)
                }
            except Exception as e:
                logger.error(f"Pricing manager error: {e}")

        # Fallback from context
        return {
            'carbon_price': self._last_context.get('carbon_price', 50.0),
            'helium_price': self._last_context.get('helium_price', 0.5)
        }

    def _get_fl_metrics(self) -> Dict[str, Any]:
        """Fetch current FL round metrics from orchestrator."""
        if self.federated_orchestrator:
            try:
                if hasattr(self.federated_orchestrator, 'get_federation_stats'):
                    stats = self.federated_orchestrator.get_federation_stats()
                    return {
                        'round_number': stats.get('total_rounds', 0),
                        'participants': stats.get('total_participants', 0),
                        'sustainability_score': stats.get('sustainability_score', 0.5),
                        'carbon_savings_kg': stats.get('total_carbon_savings_kg', 0),
                    }
            except Exception as e:
                logger.error(f"Federated orchestrator error: {e}")
        return {
            'round_number': self._last_context.get('round_number', 0),
            'participants': self._last_context.get('participants', 0),
            'sustainability_score': self._last_context.get('sustainability_score', 0.5),
            'carbon_savings_kg': self._last_context.get('carbon_savings_kg', 0),
        }

    async def _get_predictive_forecast(self) -> Optional[Dict]:
        """Get forecast from predictive analyzer if available."""
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
    # Recommendation Builders
    # ========================================================================

    def _build_recommendation(
        self,
        carbon_intensity: float,
        helium_scarcity: float,
        carbon_price: float,
        helium_price: float,
        fl_metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build the single preferred recommendation using current thresholds."""
        rec = {}

        # Round frequency
        if carbon_intensity > self.thresholds['carbon_high_threshold']:
            rec['round_frequency_hz'] = self.thresholds['round_frequency_low']
            rec['client_selection'] = 'energy_aware'
        else:
            rec['round_frequency_hz'] = self.thresholds['round_frequency_high']
            rec['client_selection'] = 'standard'

        # Compression level
        if helium_scarcity > self.thresholds['helium_scarcity_threshold']:
            rec['compression_level'] = 'high'
            rec['model_pruning_ratio'] = 0.3
        else:
            rec['compression_level'] = 'medium'
            rec['model_pruning_ratio'] = 0.1

        # Token incentive (if carbon savings are high)
        if fl_metrics.get('carbon_savings_kg', 0) > 10:
            rec['token_incentive_factor'] = 1.5

        # Preferred aggregation region
        if carbon_intensity > 400:
            rec['preferred_aggregation_region'] = 'us-west'
        else:
            rec['preferred_aggregation_region'] = 'us-east'

        return rec

    def _build_tradeoff_options(
        self,
        carbon_intensity: float,
        helium_scarcity: float,
        carbon_price: float,
        helium_price: float,
        fl_metrics: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Generate multiple alternative actions with estimated trade‑offs."""
        options = []

        # Option A: Reduce round frequency (saves energy, may delay convergence)
        if carbon_intensity > 400:
            options.append({
                'action': 'reduce_round_frequency',
                'estimated_energy_save': 0.3,
                'estimated_accuracy_loss': 0.02,
                'priority': 'high'
            })

        # Option B: Increase compression (saves communication, may reduce accuracy)
        if helium_scarcity > 0.4:
            options.append({
                'action': 'increase_compression',
                'estimated_energy_save': 0.2,
                'estimated_accuracy_loss': 0.05,
                'priority': 'medium'
            })

        # Option C: Switch to renewable‑rich region (reduces carbon, may increase latency)
        options.append({
            'action': 'switch_to_renewable_region',
            'estimated_carbon_savings': 0.3,
            'estimated_latency_increase': 20.0,
            'priority': 'low'
        })

        # Option D: Increase token incentives (may improve participation, energy cost)
        if fl_metrics.get('participants', 0) < 10:
            options.append({
                'action': 'increase_token_incentives',
                'estimated_participation_increase': 0.3,
                'estimated_token_cost': 100.0,
                'priority': 'medium'
            })

        return options

    # ========================================================================
    # Explainability
    # ========================================================================

    def _generate_explanation(
        self,
        recommendation: Dict[str, Any],
        carbon_data: Dict[str, float],
        helium_data: Dict[str, float],
        price_data: Dict[str, float],
        fl_metrics: Dict[str, Any]
    ) -> str:
        """Generate a human‑readable explanation for the recommendations."""
        parts = []

        carbon_intensity = carbon_data.get('intensity', 400)
        if carbon_intensity > self.thresholds['carbon_high_threshold']:
            parts.append(f"Carbon intensity is high ({carbon_intensity:.0f} g/kWh), "
                         f"so we reduced round frequency and enabled energy‑aware client selection.")
        else:
            parts.append(f"Carbon intensity is moderate ({carbon_intensity:.0f} g/kWh), "
                         f"maintaining standard round frequency.")

        helium_scarcity = helium_data.get('scarcity', 0.5)
        if helium_scarcity > self.thresholds['helium_scarcity_threshold']:
            parts.append(f"Helium scarcity is high ({helium_scarcity:.2f}), "
                         f"so we increased compression and applied model pruning.")

        if fl_metrics.get('carbon_savings_kg', 0) > 10:
            parts.append(f"FL has already saved {fl_metrics['carbon_savings_kg']:.1f} kg of carbon, "
                         f"so we increased token incentives to encourage further participation.")

        if not parts:
            parts.append("FL energy metrics are within acceptable ranges. "
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

    # Ensure the async method is available for the router if it supports async
    async def propose_async(self, context: dict) -> dict:
        return await self.propose(context)
