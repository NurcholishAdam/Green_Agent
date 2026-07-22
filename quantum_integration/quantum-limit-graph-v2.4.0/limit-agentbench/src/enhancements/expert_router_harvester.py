# File: src/enhancements/expert_router_harvester.py
"""
Extension of ExpertRouter with photosynthetic harvester awareness.
"""

from typing import Dict, Any, List
from ..expert_router import ExpertRouter
from ..expert_registry import ExpertProfile
from ..bio_inspired import PhotosyntheticHarvester
from .sustainability_cost import SustainabilityCostFunction

class ExpertRouterWithHarvester(ExpertRouter):
    """
    Enhanced ExpertRouter that applies a green bonus when the data source
    is from a photosynthetic harvester and the expert is harvester‑compatible.
    """

    def __init__(self, *args, harvester: PhotosyntheticHarvester = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.harvester = harvester
        self.cost_function: SustainabilityCostFunction = None

    def inject_cost_function(self, cost_function: SustainabilityCostFunction):
        self.cost_function = cost_function

    async def _apply_harvester_bonus(
        self,
        cost: float,
        context: Dict[str, Any],
        expert: ExpertProfile
    ) -> float:
        """
        Reduce cost if:
        - Data source is from a photosynthetic harvester (context['data_source'] == 'photosynthetic_harvester').
        - Expert is harvester‑compatible (expert.photosynthetic_harvester_flag == True).
        """
        data_source = context.get('data_source', 'cloud')
        if data_source == 'photosynthetic_harvester' and expert.photosynthetic_harvester_flag:
            # Apply a 20% discount
            bonus = 0.8
            logger.debug(f"Harvester bonus applied to expert {expert.expert_id}, cost {cost:.2f} -> {cost*bonus:.2f}")
            return cost * bonus
        return cost

    async def route(self, task: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        # 1. Get candidate experts (use existing logic)
        candidates = self.get_candidate_experts(task, context)  # assume this method exists

        # 2. Compute costs using SustainabilityCostFunction
        if not self.cost_function:
            raise RuntimeError("Cost function not injected")
        costs = await self.cost_function.compute_multiple(candidates, context)

        # 3. Apply harvester bonus
        final_costs = {}
        for eid, cost in costs.items():
            expert = self.registry.get_expert(eid)  # assume registry is available
            final_costs[eid] = await self._apply_harvester_bonus(cost, context, expert)

        # 4. Select expert with lowest final cost
        best_eid = min(final_costs, key=final_costs.get)
        best_expert = self.registry.get_expert(best_eid)

        # 5. Record decision and return
        return {
            'expert': best_expert,
            'cost': final_costs[best_eid],
            'harvester_bonus_applied': (
                context.get('data_source') == 'photosynthetic_harvester' and
                best_expert.photosynthetic_harvester_flag
            ),
            'timestamp': datetime.now().isoformat()
        }
