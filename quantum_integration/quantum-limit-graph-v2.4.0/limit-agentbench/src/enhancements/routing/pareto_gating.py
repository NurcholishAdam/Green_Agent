"""
Pareto gating module to filter infeasible actions before optimization.
"""
from typing import List, Dict, Any, Optional
import numpy as np

class ParetoGating:
    """Ensures hard constraints are met and returns Pareto-optimal options."""

    def __init__(self):
        self.constraints = {
            "quality": config.PARETO_QUALITY_MIN,
            "latency_ms": config.PARETO_LATENCY_MAX,
            "carbon_g": config.PARETO_CARBON_MAX
        }

    def filter(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Input: list of candidates, each dict must contain:
            'quality_score', 'latency_ms', 'carbon_g', 'energy_joules'
        Output: list of Pareto-optimal candidates that satisfy constraints.
        """
        # 1. Hard constraint filtering
        feasible = []
        for c in candidates:
            quality = c.get('quality_score', 1.0)
            latency = c.get('latency_ms', 0.0)
            carbon = c.get('carbon_g', 0.0)

            if (quality >= self.constraints['quality'] and
                latency <= self.constraints['latency_ms'] and
                carbon <= self.constraints['carbon_g']):
                feasible.append(c)

        if not feasible:
            return []

        # 2. Pareto dominance check
        # Objective vector: [quality (maximize), latency (minimize), carbon (minimize), energy (minimize)]
        pareto = []
        for i, c1 in enumerate(feasible):
            dominated = False
            for j, c2 in enumerate(feasible):
                if i == j:
                    continue
                # check if c2 dominates c1
                if (c2['quality_score'] >= c1['quality_score'] and
                    c2['latency_ms'] <= c1['latency_ms'] and
                    c2['carbon_g'] <= c1['carbon_g'] and
                    c2['energy_joules'] <= c1['energy_joules'] and
                    (c2['quality_score'] > c1['quality_score'] or
                     c2['latency_ms'] < c1['latency_ms'] or
                     c2['carbon_g'] < c1['carbon_g'] or
                     c2['energy_joules'] < c1['energy_joules'])):
                    dominated = True
                    break
            if not dominated:
                pareto.append(c1)

        return pareto
