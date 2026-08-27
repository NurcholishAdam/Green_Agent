# src/enhancements/modp/flexgen_modp_planner.py
"""
Enhanced MODP planner for temporal scheduling of FlexGen workloads.
Decides when to run (now or defer) and on which node, based on carbon forecasts.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, timedelta

from ..data_integration.carbon_intensity import CarbonIntensityFetcher
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..schemas.node_descriptor import NodeDescriptor
from ..gpu_optimization.flexgen_policy import FlexGenPolicy
from ..gpu_optimization.reward import compute_reward
from ..async_message_queue import AsyncMessageQueue
from ..schemas.feedback_event import FeedbackEvent
from ..logger import logger


class FlexGenMODPPlanner:
    """
    Simple lookahead planner for deciding execution time and node.
    Uses carbon forecasts to shift workloads to low‑carbon periods.
    """

    def __init__(
        self,
        carbon_forecaster: Optional[CarbonIntensityFetcher] = None,
        horizon: int = 6,  # hours
        discount_factor: float = 0.9,
        defer_penalty: float = 0.05,   # per hour delay
        move_penalty: float = 0.1,     # cost of moving to different node
        message_queue: Optional[AsyncMessageQueue] = None,
    ):
        self.carbon_forecaster = carbon_forecaster
        self.horizon = horizon
        self.discount = discount_factor
        self.defer_penalty = defer_penalty
        self.move_penalty = move_penalty
        self.message_queue = message_queue

    async def get_carbon_forecast(self) -> List[float]:
        """
        Fetch predicted carbon intensity for the next `horizon` hours.
        Returns list of gCO2/kWh values.
        """
        if self.carbon_forecaster:
            forecast = await self.carbon_forecaster.forecast_carbon_prices(hours=self.horizon)
            if forecast.get('status') == 'success':
                return forecast['predictions']
        # Fallback: constant intensity
        return [400] * self.horizon

    async def plan(
        self,
        workload: WorkloadDescriptor,
        node: NodeDescriptor,
        current_policy: FlexGenPolicy,
        queue_length: int,
        current_carbon: float,
        available_nodes: Optional[List[NodeDescriptor]] = None,
    ) -> Tuple[str, int, Optional[str]]:
        """
        Decide action: 'run_now', 'defer', or 'move_node'.
        Returns (action, delay_hours, node_id_for_move).
        """
        forecast = await self.get_carbon_forecast()

        # Check if we can defer
        if workload.deadline and (workload.deadline - datetime.utcnow()).total_seconds() > 3600:
            # Find best future hour with lower carbon
            min_carbon = min(forecast)
            min_idx = forecast.index(min_carbon)
            if min_carbon < current_carbon - 50:
                # Defer to that hour
                return ("defer", min_idx, None)

        # Check if moving to another node is beneficial
        if available_nodes:
            best_node = None
            best_node_carbon = current_carbon
            for cand_node in available_nodes:
                # Use cand_node's region_carbon_intensity as proxy
                cand_carbon = cand_node.region_carbon_intensity * 1000  # approximate
                if cand_carbon < best_node_carbon - 50:
                    best_node = cand_node.id
                    best_node_carbon = cand_carbon
            if best_node:
                return ("move_node", 0, best_node)

        # Otherwise run now
        return ("run_now", 0, None)

    async def publish_decision(self, workload, action, delay, node_id=None, reward_estimate=0.0):
        """
        Publish the MODP decision as a FeedbackEvent.
        """
        if not self.message_queue:
            return
        event = FeedbackEvent(
            source="modp_flexgen_planner",
            feedback_type="routing",
            task_id=workload.task_id or "unknown",
            context={"action": action, "delay_hours": delay, "target_node": node_id},
            action={"selected_action": action, "selected_rank": 1, "confidence_score": 0.5},
            performance={"quality_score": 0.9, "latency_ms": 0, "energy_joules": 0,
                         "carbon_g": 0, "helium_cost": 0, "duration_ms": 0},
            adaptive_cost_value=reward_estimate,
            tags=["modp", "scheduling", "carbon_aware"],
        )
        await self.message_queue.publish("modp_events", event.to_json())
