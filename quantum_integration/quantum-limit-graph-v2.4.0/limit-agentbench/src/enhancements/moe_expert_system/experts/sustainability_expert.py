# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/sustainability_expert.py
"""
Enhanced Sustainability Expert v2.0
Integrates real-time carbon/helium data, predictive analytics,
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
    from ...integration.layer_integrator import CarbonIntensityManager
    from ...advanced.cross_region_federation import HeliumProvider, EconomicPricingManager
    from ...advanced.self_evolving_gates import EnhancedSelfEvolvingGate
    MANAGERS_AVAILABLE = True
except ImportError:
    MANAGERS_AVAILABLE = False
    logger.warning("Sustainability Expert: external managers not available - using fallback data")


class SustainabilityExpert(BaseExpert):
    """
    Sustainability Expert v2.0
    Provides recommendations for data center selection, carbon budget, helium conservation,
    renewable energy share, and carbon offsets, using real-time data, predictive analytics,
    and multi-objective trade-offs.
    """

    # Tunable thresholds (can be evolved by self_evolving_gates)
    DEFAULT_THRESHOLDS = {
        'carbon_high_threshold': 500.0,      # g/kWh – above this, shift to low‑carbon region
        'helium_scarcity_threshold': 0.6,    # 0‑1 – above this, enable helium recovery
        'carbon_price_threshold': 80.0,      # USD/ton – above this, recommend offsets
        'renewable_share_high': 0.8,         # when carbon < 300
        'renewable_share_low': 0.4           # when carbon >= 300
    }

    def __init__(self):
        super().__init__()
        self.thresholds = self.DEFAULT_THRESHOLDS.copy()
        self.carbon_manager = None
        self.helium_provider = None
        self.pricing_manager = None
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
        logger.info(f"SustainabilityExpert thresholds updated: {self.thresholds}")

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
        self._last_context = context

        # 1. Gather real‑time data (use injected managers or fallback)
        carbon_data = await self._get_carbon_data()
        helium_data = self._get_helium_data()
        price_data = await self._get_price_data()

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
            helium_price=price_data['helium_price']
        )

        # 4. Build alternative trade‑off options (multi‑objective)
        options = self._build_tradeoff_options(
            carbon_intensity=carbon_data['intensity'],
            helium_scarcity=helium_data['scarcity'],
            carbon_price=price_data['carbon_price'],
            helium_price=price_data['helium_price']
        )

        # 5. Generate natural‑language explanation
        explanation = self._generate_explanation(
            primary, carbon_data, helium_data, price_data
        )

        # 6. (Optional) Learn from cross‑domain knowledge
        if self.cross_domain_transfer:
            self.cross_domain_transfer.transfer_knowledge(
                'sustainability',
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
        helium_price: float
    ) -> Dict[str, Any]:
        """Build the single preferred recommendation using current thresholds."""
        rec = {}

        # Data center
        if carbon_intensity > self.thresholds['carbon_high_threshold']:
            rec['preferred_data_center'] = 'us-west'  # typically lower carbon
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

        # Token incentive (if carbon savings are high)
        carbon_savings = (400 - carbon_intensity) / 400 if carbon_intensity < 400 else 0
        if carbon_savings > 0.1:
            rec['token_stake_recommendation'] = carbon_savings * 100  # in Eco-ATP

        return rec

    def _build_tradeoff_options(
        self,
        carbon_intensity: float,
        helium_scarcity: float,
        carbon_price: float,
        helium_price: float
    ) -> List[Dict[str, Any]]:
        """Generate multiple alternative actions with estimated trade‑offs."""
        options = []

        # Option A: Shift to low‑carbon region (higher cost, lower carbon)
        if carbon_intensity > 400:
            options.append({
                'action': 'shift_to_low_carbon_region',
                'estimated_carbon_savings_kg': (carbon_intensity - 300) * 0.01,
                'estimated_cost_increase_usd': 5.0,
                'accuracy_impact': -0.02,  # slight accuracy loss
                'priority': 'high'
            })

        # Option B: Enable helium recovery (saves helium, may increase latency)
        if helium_scarcity > 0.4:
            options.append({
                'action': 'enable_helium_recovery',
                'estimated_helium_savings_l': helium_scarcity * 0.5,
                'estimated_latency_increase_ms': 10.0,
                'accuracy_impact': 0.0,
                'priority': 'medium'
            })

        # Option C: Purchase carbon offsets (no operational change)
        if carbon_price > 50:
            options.append({
                'action': 'purchase_carbon_offsets',
                'estimated_carbon_savings_kg': 10.0,
                'estimated_cost_usd': carbon_price * 0.1,
                'accuracy_impact': 0.0,
                'priority': 'low'
            })

        # Option D: Increase renewable share (may require scheduling changes)
        options.append({
            'action': 'increase_renewable_share',
            'estimated_renewable_share': 0.9,
            'estimated_scheduling_complexity': 0.3,
            'accuracy_impact': -0.01,
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
        price_data: Dict[str, float]
    ) -> str:
        """Generate a human‑readable explanation for the recommendations."""
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

    # Ensure the async method is available for the router if it supports async
    async def propose_async(self, context: dict) -> dict:
        return await self.propose(context)
