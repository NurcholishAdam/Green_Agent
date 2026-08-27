# src/enhancements/modp/flexgen_modp_planner.py
"""
Enhanced MODP planner for temporal scheduling of FlexGen workloads.
Decides when to run (now or defer) and on which node, based on carbon forecasts.
Uses Q-learning for adaptive deferral decisions, supports moving workloads to
lower-carbon nodes, and publishes decisions via FeedbackEvent.

Improvements over the basic lookahead:
- Discretized state (carbon intensity, queue length, hours to deadline).
- Q-learning with epsilon-greedy exploration.
- Reward includes execution cost and deferral/move penalties.
- Persistence of Q-table to JSON.
- Integration with AsyncMessageQueue and FeedbackEvent.
"""

import asyncio
import logging
import json
import os
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, timedelta
import numpy as np

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
    MODP-based planner that decides when and where to execute a workload.
    Uses a simple Q-learning approach for temporal scheduling decisions.
    """

    def __init__(
        self,
        carbon_forecaster: Optional[CarbonIntensityFetcher] = None,
        horizon: int = 6,                 # hours to look ahead
        discount_factor: float = 0.9,
        learning_rate: float = 0.1,
        epsilon: float = 0.1,
        epsilon_decay: float = 0.999,
        defer_penalty: float = 0.05,      # per hour delay penalty
        move_penalty: float = 0.1,        # penalty for moving nodes
        message_queue: Optional[AsyncMessageQueue] = None,
        q_table_path: str = "modp_q_table.json",
    ):
        self.carbon_forecaster = carbon_forecaster
        self.horizon = horizon
        self.discount = discount_factor
        self.lr = learning_rate
        self.epsilon = epsilon
        self.epsilon_decay = epsilon_decay
        self.defer_penalty = defer_penalty
        self.move_penalty = move_penalty
        self.message_queue = message_queue
        self.q_table_path = q_table_path

        # Q-table: dict key = (carbon_bucket, queue_bucket, deadline_bucket) -> np.array
        # Actions: 0 = run_now, 1..horizon = defer that many hours, horizon+1 = move_node
        self.q_table: Dict[Tuple[int, int, int], np.ndarray] = {}
        self.last_state: Optional[Tuple[int, int, int]] = None
        self.last_action: Optional[int] = None
        self.last_decision: Optional[Tuple[str, int, Optional[str]]] = None

        # Load Q-table if exists
        self._load_q_table()

    def _discretize_state(self, current_carbon: float, queue_length: int, hours_to_deadline: float) -> Tuple[int, int, int]:
        """Convert continuous state to discrete buckets."""
        carbon_bucket = int(current_carbon // 50)   # 50 gCO2/kWh per bucket
        queue_bucket = min(queue_length, 10)
        deadline_bucket = min(int(hours_to_deadline), 24)
        return (carbon_bucket, queue_bucket, deadline_bucket)

    def _get_action_values(self, state: Tuple[int, int, int]) -> np.ndarray:
        """Return Q-values for all actions for a given state."""
        if state not in self.q_table:
            # Initialize with small random values
            self.q_table[state] = np.random.randn(self.horizon + 2) * 0.01
        return self.q_table[state]

    async def get_carbon_forecast(self, hours: Optional[int] = None) -> List[float]:
        """Return predicted carbon intensity for next `hours` hours."""
        hours = hours or self.horizon
        if self.carbon_forecaster:
            forecast = await self.carbon_forecaster.forecast_carbon_prices(hours=hours)
            if forecast.get('status') == 'success':
                return forecast['predictions']
        # Fallback: constant intensity
        return [400] * hours

    async def plan(
        self,
        workload: WorkloadDescriptor,
        node: NodeDescriptor,
        current_policy: FlexGenPolicy,
        queue_length: int = 0,
        current_carbon: Optional[float] = None,
        available_nodes: Optional[List[NodeDescriptor]] = None,
    ) -> Tuple[str, int, Optional[str]]:
        """
        Decide action: 'run_now', 'defer', or 'move_node'.
        Returns (action, delay_hours, node_id_for_move).
        """
        # Get current carbon intensity
        if current_carbon is None:
            current_carbon = await self.carbon_forecaster.get_current_intensity() if self.carbon_forecaster else 400

        # Compute hours to deadline
        if workload.deadline:
            hours_to_deadline = (workload.deadline - datetime.utcnow()).total_seconds() / 3600
            hours_to_deadline = max(0, hours_to_deadline)
        else:
            hours_to_deadline = 24.0  # assume 24h if no deadline

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
            # Move to another node (pick the one with lowest carbon intensity)
            if available_nodes:
                best_node = min(available_nodes, key=lambda n: n.region_carbon_intensity)
                decision = ("move_node", 0, best_node.id)
            else:
                decision = ("run_now", 0, None)  # fallback if no nodes to move to

        # Store state-action for learning
        self.last_state = state
        self.last_action = action
        self.last_decision = decision

        # Calculate reward estimate for publishing (not actual reward)
        reward_estimate = 0.0
        if decision[0] == "run_now":
            # Estimate reward using current policy (could be rough)
            # For now, just use 0.5
            reward_estimate = 0.5
        elif decision[0] == "defer":
            reward_estimate = -self.defer_penalty * decision[1]
        elif decision[0] == "move_node":
            reward_estimate = -self.move_penalty

        # Publish decision as FeedbackEvent
        await self.publish_decision(workload, decision[0], decision[1], decision[2], reward_estimate)

        # Decay epsilon
        self.epsilon = max(0.01, self.epsilon * self.epsilon_decay)

        return decision

    async def learn(self, reward: float, next_carbon: float, next_queue_length: int):
        """
        Update Q-values based on observed reward and next state.
        Call after execution outcome is known.
        """
        if self.last_state is None or self.last_action is None:
            return

        # Compute next state
        next_state = self._discretize_state(next_carbon, next_queue_length, 24)  # deadline unknown, assume 24h
        next_values = self._get_action_values(next_state)
        max_next = np.max(next_values) if len(next_values) > 0 else 0

        # Q-learning update
        current_values = self._get_action_values(self.last_state)
        current_values[self.last_action] += self.lr * (
            reward + self.discount * max_next - current_values[self.last_action]
        )

        # Reset last state/action
        self.last_state = None
        self.last_action = None

        # Save Q-table periodically
        if np.random.random() < 0.1:  # save ~10% of the time
            self._save_q_table()

    async def publish_decision(self, workload: WorkloadDescriptor, action: str, delay: int,
                               node_id: Optional[str] = None, reward_estimate: float = 0.0):
        """
        Publish the MODP decision as a FeedbackEvent.
        """
        if not self.message_queue or FeedbackEvent is None:
            return
        event = FeedbackEvent(
            source="modp_flexgen_planner",
            feedback_type="routing",
            task_id=workload.task_id or "unknown",
            context={
                "action": action,
                "delay_hours": delay,
                "target_node": node_id,
                "epsilon": self.epsilon,
            },
            action={"selected_action": action, "selected_rank": 1, "confidence_score": 0.5},
            performance={"quality_score": 0.9, "latency_ms": 0, "energy_joules": 0,
                         "carbon_g": 0, "helium_cost": 0, "duration_ms": 0},
            adaptive_cost_value=reward_estimate,
            tags=["modp", "scheduling", "carbon_aware"],
        )
        await self.message_queue.publish("modp_events", event.to_json())

    def _save_q_table(self) -> None:
        """Save Q-table to JSON."""
        if not self.q_table_path:
            return
        try:
            # Convert tuple keys to strings for JSON
            serializable = {str(k): v.tolist() for k, v in self.q_table.items()}
            with open(self.q_table_path, 'w') as f:
                json.dump(serializable, f)
            logger.info(f"MODP Q-table saved to {self.q_table_path}")
        except Exception as e:
            logger.warning(f"Failed to save Q-table: {e}")

    def _load_q_table(self) -> None:
        """Load Q-table from JSON if file exists."""
        if not self.q_table_path or not os.path.exists(self.q_table_path):
            return
        try:
            with open(self.q_table_path, 'r') as f:
                serialized = json.load(f)
            # Convert string keys back to tuples, lists back to arrays
            self.q_table = {
                tuple(map(int, k.strip('()').split(','))): np.array(v)
                for k, v in serialized.items()
            }
            logger.info(f"MODP Q-table loaded from {self.q_table_path}")
        except Exception as e:
            logger.warning(f"Failed to load Q-table: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Return planner statistics."""
        return {
            "num_states": len(self.q_table),
            "epsilon": self.epsilon,
            "horizon": self.horizon,
        }
