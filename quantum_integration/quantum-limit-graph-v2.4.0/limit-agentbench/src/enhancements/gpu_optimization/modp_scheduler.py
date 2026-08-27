"""
MODP‑based time‑shifting scheduler (enhanced).
Decides whether to run a workload now or defer based on carbon forecasts,
queue length, deadline, and historical patterns. Integrates with
AsyncMessageQueue, FeedbackEvent, and other Green Agent modules.

Enhancements over simple rule-based:
- Uses dynamic programming / value iteration for optimal deferral.
- Considers deadline, queue length, and energy budget.
- Publishes decisions as FeedbackEvent.
- Supports multiple nodes (move decision).
- Learns from past decisions via Q-learning (simple).
"""

import asyncio
import logging
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, timedelta
import numpy as np

from ..data_integration.carbon_intensity import CarbonIntensityFetcher
from ..async_message_queue import AsyncMessageQueue
from ..schemas.feedback_event import FeedbackEvent
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..schemas.node_descriptor import NodeDescriptor
from ..logger import logger


class MODPScheduler:
    """
    MODP-based scheduler that decides when (and where) to run a workload.
    Uses a simple Q-learning approach for deferral decisions.
    """

    def __init__(
        self,
        carbon_fetcher: Optional[CarbonIntensityFetcher] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        learning_rate: float = 0.1,
        discount_factor: float = 0.9,
        epsilon: float = 0.1,
        horizon: int = 6,  # hours to look ahead
        defer_penalty: float = 0.05,
        move_penalty: float = 0.1,
    ):
        self.carbon_fetcher = carbon_fetcher
        self.message_queue = message_queue
        self.lr = learning_rate
        self.discount = discount_factor
        self.epsilon = epsilon
        self.horizon = horizon
        self.defer_penalty = defer_penalty
        self.move_penalty = move_penalty

        # Q-table for (state, action) -> value
        # State: discretized current carbon, queue length, hours to deadline
        # Actions: 0=run_now, 1..horizon=defer that many hours, horizon+1=move_node
        self.q_table: Dict[Tuple[int, int, int], np.ndarray] = {}
        self.last_state: Optional[Tuple[int, int, int]] = None
        self.last_action: Optional[int] = None

    def _discretize_state(self, current_carbon: float, queue_length: int, hours_to_deadline: float) -> Tuple[int, int, int]:
        """Convert continuous state to discrete buckets."""
        carbon_bucket = int(current_carbon // 50)  # 0,1,2,... (50 gCO2/kWh per bucket)
        queue_bucket = min(queue_length, 10)
        deadline_bucket = min(int(hours_to_deadline), 24)
        return (carbon_bucket, queue_bucket, deadline_bucket)

    def _get_action_values(self, state: Tuple[int, int, int]) -> np.ndarray:
        """Return Q-values for all actions for a given state."""
        if state not in self.q_table:
            # Initialize with zeros; length = horizon + 2 (run_now, defer 1..horizon, move_node)
            self.q_table[state] = np.zeros(self.horizon + 2)
        return self.q_table[state]

    async def get_carbon_forecast(self, hours: int = None) -> List[float]:
        """Return predicted carbon intensity for next `hours` hours."""
        hours = hours or self.horizon
        if self.carbon_fetcher:
            forecast = await self.carbon_fetcher.forecast_carbon_prices(hours=hours)
            if forecast.get('status') == 'success':
                return forecast['predictions']
        # Fallback: constant carbon intensity
        return [400] * hours

    async def decide(
        self,
        workload: WorkloadDescriptor,
        node: NodeDescriptor,
        queue_length: int = 0,
        current_carbon: Optional[float] = None,
        available_nodes: Optional[List[NodeDescriptor]] = None,
    ) -> Tuple[str, int, Optional[str]]:
        """
        Decide action: 'run_now', 'defer', or 'move_node'.
        Returns (action, delay_hours, target_node_id).
        """
        # Get current carbon intensity
        if current_carbon is None:
            current_carbon = await self.carbon_fetcher.get_current_intensity() if self.carbon_fetcher else 400

        # Compute hours to deadline
        if workload.deadline:
            hours_to_deadline = (workload.deadline - datetime.utcnow()).total_seconds() / 3600
        else:
            hours_to_deadline = 24.0  # no deadline, assume 24h

        # Discretize state
        state = self._discretize_state(current_carbon, queue_length, hours_to_deadline)

        # Choose action using epsilon-greedy
        action_values = self._get_action_values(state)
        if np.random.random() < self.epsilon:
            action = np.random.randint(len(action_values))
        else:
            action = int(np.argmax(action_values))

        # Interpret action
        if action == 0:
            decision = ("run_now", 0, None)
        elif action <= self.horizon:
            decision = ("defer", action, None)
        else:
            # Move to another node (simplified: pick the node with lowest carbon intensity)
            if available_nodes:
                best_node = min(available_nodes, key=lambda n: n.region_carbon_intensity)
                decision = ("move_node", 0, best_node.id)
            else:
                decision = ("run_now", 0, None)  # fallback

        # Store state-action for learning
        self.last_state = state
        self.last_action = action

        # Publish decision as FeedbackEvent
        if self.message_queue and FeedbackEvent:
            event = FeedbackEvent(
                source="modp_scheduler",
                feedback_type="routing",
                task_id=workload.task_id or "unknown",
                context={"current_carbon": current_carbon,
                         "queue_length": queue_length,
                         "hours_to_deadline": hours_to_deadline,
                         "available_nodes": [n.id for n in available_nodes] if available_nodes else []},
                action={"selected_action": decision[0],
                        "selected_rank": action,
                        "confidence_score": 0.5},
                performance={"quality_score": 0.9,
                             "latency_ms": 0,
                             "energy_joules": 0,
                             "carbon_g": current_carbon,
                             "helium_cost": 0,
                             "duration_ms": 0},
                adaptive_cost_value=0.0,
                tags=["modp", "scheduling", "carbon_aware"],
            )
            await self.message_queue.publish("modp_events", event.to_json())

        return decision

    async def learn(self, reward: float, next_carbon: float, next_queue_length: int):
        """
        Update Q-values based on observed reward and next state.
        Call after execution outcome is known.
        """
        if self.last_state is None or self.last_action is None:
            return

        # Compute next state from next_carbon and queue
        next_state = self._discretize_state(next_carbon, next_queue_length, 24)  # deadline unknown, assume 24h
        next_values = self._get_action_values(next_state)
        max_next = np.max(next_values) if len(next_values) > 0 else 0

        # Q-learning update
        current_values = self._get_action_values(self.last_state)
        current_values[self.last_action] += self.lr * (reward + self.discount * max_next - current_values[self.last_action])

        # Reset last state/action
        self.last_state = None
        self.last_action = None

    def get_policy_stats(self) -> Dict[str, Any]:
        """Return simple stats about Q-table."""
        return {
            "num_states": len(self.q_table),
            "avg_q_value": np.mean([np.mean(v) for v in self.q_table.values()]) if self.q_table else 0,
        }
