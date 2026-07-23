# File: src/enhancements/feedback_collector.py
"""
Feedback Collector – gathers post‑routing metrics and feeds them to the adaptive cost function.
"""

import asyncio
import logging
from typing import Dict, Any, Optional

from .adaptive_cost_function import AdaptiveCostFunction
from ..expert_registry import ExpertRegistry

logger = logging.getLogger(__name__)

class FeedbackCollector:
    """
    Collects post‑routing metrics and feeds them to the AdaptiveCostFunction.
    """

    def __init__(
        self,
        cost_function: AdaptiveCostFunction,
        registry: ExpertRegistry,
    ):
        self.cost_function = cost_function
        self.registry = registry

    async def record(
        self,
        request_id: str,
        expert_id: str,
        node_id: str,
        actual_energy_joules: float,
        actual_carbon_kg: float,
        actual_helium_units: float,
        actual_latency_ms: float,
        actual_accuracy: float,
    ) -> None:
        """
        Record actual metrics after a routing decision.
        This method is called by the router after task execution.
        """
        context = {
            'request_id': request_id,
            'expert_id': expert_id,
            'node_id': node_id,
        }
        metrics = {
            'energy_joules': actual_energy_joules,
            'carbon_kg': actual_carbon_kg,
            'helium_units': actual_helium_units,
            'latency_ms': actual_latency_ms,
            'accuracy': actual_accuracy,
            # material_index could be added if measured
        }
        await self.cost_function.record_feedback(context, metrics)
        logger.debug(f"Feedback recorded for request {request_id}")

    async def get_adaptation_status(self) -> Dict[str, Any]:
        """Return current weight values and recent MAE."""
        errors = list(self.cost_function.prediction_errors)
        mae = np.mean(np.abs(errors)) if errors else 0.0
        return {
            'weights': self.cost_function.weights,
            'mae': mae,
            'samples': len(errors),
            'learning_rate': self.cost_function.learning_rate,
        }
