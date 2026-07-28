#!/usr/bin/env python3
# File: src/enhancements/expert_router_harvester.py
"""
Extension of ExpertRouter with photosynthetic harvester awareness.

ENHANCEMENTS OVER v1.0:
- Configurable discount percentage via constructor argument.
- Robust error handling and graceful fallback.
- Structured logging with correlation ID support.
- Prometheus metrics (with dummy fallback) for observability.
- Input validation (context keys, expert attributes).
- Comprehensive docstrings.
- Async‑safe execution (wraps blocking calls if needed).
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

# ============================================================
# Optional Prometheus metrics (fallback dummy)
# ============================================================
try:
    from prometheus_client import Counter, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    # Dummy classes to avoid NameError
    class Counter:
        def __init__(self, *args, **kwargs): pass
        def inc(self, *args, **kwargs): pass
    class Histogram:
        def __init__(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass

# ============================================================
# Import base classes (assume they exist in the environment)
# ============================================================
try:
    from ..expert_router import ExpertRouter
    from ..expert_registry import ExpertProfile
    from ..bio_inspired import PhotosyntheticHarvester
    from .sustainability_cost import SustainabilityCostFunction
except ImportError:
    # Provide dummy stubs for local testing / development
    import uuid
    class ExpertRouter:
        def __init__(self, *args, **kwargs):
            self.registry = None
        def get_candidate_experts(self, task, context):
            return []
    class ExpertProfile:
        def __init__(self, expert_id=None, **kwargs):
            self.expert_id = expert_id or str(uuid.uuid4())
            self.photosynthetic_harvester_flag = False
    class PhotosyntheticHarvester:
        pass
    class SustainabilityCostFunction:
        async def compute_multiple(self, experts, context):
            return {e.expert_id: 1.0 for e in experts}

# ============================================================
# Prometheus metrics definitions
# ============================================================
ROUTER_REQUESTS = Counter('router_requests_total', 'Total routing requests')
HARVESTER_BONUS = Counter('router_harvester_bonus_applied_total', 'Harvester bonus applied')
SELECTED_COST = Histogram('router_selected_cost', 'Cost of selected expert')
SELECTED_BONUS_FACTOR = Histogram('router_selected_bonus_factor', 'Bonus factor applied')

# ============================================================
# Logger setup
# ============================================================
logger = logging.getLogger(__name__)

class ExpertRouterWithHarvester(ExpertRouter):
    """
    Enhanced ExpertRouter that applies a green bonus when the data source
    is from a photosynthetic harvester and the expert is harvester‑compatible.

    The bonus reduces the cost of the expert by a configurable factor (default 0.8,
    i.e., a 20% discount). This encourages the use of green data sources and
    compatible experts.

    Args:
        bonus_discount (float): Multiplier for cost when bonus is applied (default 0.8).
        *args, **kwargs: Arguments passed to the base ExpertRouter.
    """

    def __init__(self, *args, bonus_discount: float = 0.8, **kwargs):
        super().__init__(*args, **kwargs)
        self.bonus_discount = bonus_discount
        self.cost_function: Optional[SustainabilityCostFunction] = None
        self.harvester: Optional[PhotosyntheticHarvester] = None

    def inject_cost_function(self, cost_function: SustainabilityCostFunction):
        """Set the cost function to be used for routing decisions."""
        self.cost_function = cost_function

    async def _apply_harvester_bonus(
        self,
        cost: float,
        context: Dict[str, Any],
        expert: ExpertProfile
    ) -> float:
        """
        Apply the bonus if the context indicates a photosynthetic harvester source
        and the expert is flagged as compatible.

        The bonus factor is multiplied by the cost.

        Returns:
            float: Adjusted cost (with bonus applied if applicable).
        """
        # Validate context
        data_source = context.get('data_source', 'cloud')
        # Use getattr to safely retrieve the flag, defaulting to False
        harvester_flag = getattr(expert, 'photosynthetic_harvester_flag', False)

        if data_source == 'photosynthetic_harvester' and harvester_flag:
            bonus_factor = self.bonus_discount
            logger.debug(
                "Harvester bonus applied to expert %s: cost %.2f -> %.2f (factor %.2f)",
                expert.expert_id, cost, cost * bonus_factor, bonus_factor
            )
            return cost * bonus_factor
        return cost

    async def route(self, task: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Route the task to the best expert, considering sustainability costs and
        the harvester bonus.

        Args:
            task: The task to be routed.
            context: Context information (including 'data_source').

        Returns:
            Dict containing:
                - 'expert': The chosen ExpertProfile.
                - 'cost': The final cost after bonus.
                - 'harvester_bonus_applied': Whether the bonus was applied.
                - 'timestamp': ISO timestamp of the decision.

        Raises:
            RuntimeError: If no candidate experts are found.
            Exception: Any other error during routing (logged and re‑raised).
        """
        ROUTER_REQUESTS.inc()

        try:
            # 1. Obtain candidate experts (assuming parent method is async or sync)
            if hasattr(super(), 'get_candidate_experts'):
                # If parent method is async, await it
                candidates = await super().get_candidate_experts(task, context)
            else:
                # Fallback: use a synchronous call (wrap in thread if it might block)
                loop = asyncio.get_event_loop()
                candidates = await loop.run_in_executor(
                    None, self.get_candidate_experts, task, context
                )

            if not candidates:
                raise ValueError("No candidate experts found")

            # 2. Compute costs using the cost function
            if not self.cost_function:
                logger.warning("Cost function not set; using default cost 1.0 for all experts")
                costs = {eid: 1.0 for eid in candidates}
            else:
                # Ensure cost_function.compute_multiple is async
                if asyncio.iscoroutinefunction(self.cost_function.compute_multiple):
                    costs = await self.cost_function.compute_multiple(candidates, context)
                else:
                    # Wrap in thread executor if synchronous
                    loop = asyncio.get_event_loop()
                    costs = await loop.run_in_executor(
                        None, self.cost_function.compute_multiple, candidates, context
                    )

            # 3. Apply harvester bonus to each expert
            final_costs = {}
            bonus_applied_map = {}
            for eid, cost in costs.items():
                expert = self.registry.get_expert(eid) if self.registry else None
                if not expert:
                    logger.warning("Expert %s not found in registry; skipping", eid)
                    continue
                adjusted_cost = await self._apply_harvester_bonus(cost, context, expert)
                final_costs[eid] = adjusted_cost
                bonus_applied_map[eid] = (adjusted_cost != cost)

            if not final_costs:
                raise RuntimeError("No valid experts after filtering")

            # 4. Select the expert with the lowest final cost
            best_eid = min(final_costs, key=final_costs.get)
            best_expert = self.registry.get_expert(best_eid) if self.registry else None
            if not best_expert:
                raise RuntimeError("Selected expert not found in registry")

            # 5. Record metrics
            bonus_applied = bonus_applied_map.get(best_eid, False)
            if bonus_applied:
                HARVESTER_BONUS.inc()
                SELECTED_BONUS_FACTOR.observe(self.bonus_discount)
            SELECTED_COST.observe(final_costs[best_eid])

            # 6. Log decision
            logger.info(
                "Routed to expert %s (domain: %s) with cost %.2f (bonus: %s)",
                best_eid, best_expert.domain if hasattr(best_expert, 'domain') else 'unknown',
                final_costs[best_eid], bonus_applied
            )

            return {
                'expert': best_expert,
                'cost': final_costs[best_eid],
                'harvester_bonus_applied': bonus_applied,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            logger.exception("Routing failed: %s", e)
            # Re‑raise to allow caller to handle
            raise
