#!/usr/bin/env python3
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
- Thread‑safe with asyncio.Lock.
- Optimistic initialization and proper next‑state handling.
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
        optimistic_init: float = 1.0,
        save_every: int = 10,
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
        self.optimistic_init = optimistic_init
        self.save_every = save_every
        self._update_counter = 0

        # Q-table: dict key = (carbon_bucket, queue_bucket, deadline_bucket) -> np.array
        # Actions: 0 = run_now, 1..horizon = defer that many hours, horizon+1 = move_node
        self.q_table: Dict[Tuple[int, int, int], np.ndarray] = {}
        self.last_state: Optional[Tuple[int, int, int]] = None
        self.last_action: Optional[int] = None
        self.last_decision: Optional[Tuple[str, int, Optional[str]]] = None
        self.last_hours_to_deadline: Optional[float] = None

        # Async lock for thread safety
        self._lock = asyncio.Lock()

        # Load Q-table if exists
        self._load_q_table()

    def _discretize_state(self, current_carbon: float, queue_length: int, hours_to_deadline: float) -> Tuple[int, int, int]:
        """Convert continuous state to discrete buckets."""
        carbon_bucket = int(current_carbon // 50)   # 50 gCO2/kWh per bucket
        queue_bucket = min(queue_length, 10)
        deadline_bucket = min(int(hours_to_deadline), 24)
        return (carbon_bucket, queue_bucket, deadline_bucket)

    def _get_action_values(self, state: Tuple[int, int, int]) -> np.ndarray:
        """Return Q-values for all actions for a given state, initializing if needed."""
        async with self._lock:
            if state not in self.q_table:
                # Optimistic initialization to encourage exploration
                self.q_table[state] = np.full(self.horizon + 2, self.optimistic_init, dtype=float)
            return self.q_table[state]

    async def get_carbon_forecast(self, hours: Optional[int] = None) -> List[float]:
        """Return predicted carbon intensity for next `hours` hours."""
        hours = hours or self.horizon
        if self.carbon_forecaster:
            try:
                forecast = await self.carbon_forecaster.forecast_carbon_prices(hours=hours)
                if isinstance(forecast, dict) and forecast.get('status') == 'success':
                    return forecast['predictions']
                elif isinstance(forecast, list):
                    return forecast
            except Exception as e:
                logger.warning(f"Carbon forecast failed: {e}")
        # Fallback: constant intensity
        return [400.0] * hours

    async def plan(
        self,
        workload: WorkloadDescriptor,
        node: NodeDescriptor,
        current_policy: Optional[FlexGenPolicy] = None,
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
            if self.carbon_forecaster:
                try:
                    current_carbon = await self.carbon_forecaster.get_current_intensity()
                except Exception as e:
                    logger.warning(f"Failed to get current carbon: {e}")
                    current_carbon = 400.0
            else:
                current_carbon = 400.0

        # Compute hours to deadline
        if workload.deadline:
            hours_to_deadline = (workload.deadline - datetime.utcnow()).total_seconds() / 3600
            hours_to_deadline = max(0, hours_to_deadline)
        else:
            hours_to_deadline = 24.0  # assume 24h if no deadline

        # Discretize state
        state = self._discretize_state(current_carbon, queue_length, hours_to_deadline)

        # Choose action using epsilon-greedy
        async with self._lock:
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
            # Move to another node: pick feasible one with lowest carbon intensity
            if available_nodes:
                feasible_nodes = [n for n in available_nodes if self._is_node_feasible(n, current_policy)]
                if feasible_nodes:
                    best_node = min(feasible_nodes, key=lambda n: n.region_carbon_intensity)
                    decision = ("move_node", 0, best_node.id)
                else:
                    decision = ("run_now", 0, None)  # fallback if no feasible nodes
            else:
                decision = ("run_now", 0, None)  # fallback if no nodes to move to

        # Store state-action for learning
        async with self._lock:
            self.last_state = state
            self.last_action = action
            self.last_decision = decision
            self.last_hours_to_deadline = hours_to_deadline

        # Calculate reward estimate for publishing (not actual reward)
        reward_estimate = 0.0
        if decision[0] == "run_now":
            reward_estimate = 0.5
        elif decision[0] == "defer":
            reward_estimate = -self.defer_penalty * decision[1]
        elif decision[0] == "move_node":
            reward_estimate = -self.move_penalty

        # Publish decision as FeedbackEvent
        await self.publish_decision(workload, decision[0], decision[1], decision[2], reward_estimate)

        # Decay epsilon
        async with self._lock:
            self.epsilon = max(0.01, self.epsilon * self.epsilon_decay)

        return decision

    async def learn(self, reward: float, next_carbon: float, next_queue_length: int):
        """
        Update Q-values based on observed reward and next state.
        Call after execution outcome is known.
        """
        async with self._lock:
            if self.last_state is None or self.last_action is None:
                return

            # Compute next state using the stored deadline decremented by the delay
            if self.last_decision and self.last_decision[0] == "defer":
                delay = self.last_decision[1]
                next_deadline = max(0, self.last_hours_to_deadline - delay)
            else:
                next_deadline = 24.0  # unknown, assume 24h

            next_state = self._discretize_state(next_carbon, next_queue_length, next_deadline)
            next_values = self._get_action_values(next_state)
            max_next = np.max(next_values) if len(next_values) > 0 else 0

            current_values = self._get_action_values(self.last_state)
            current_values[self.last_action] += self.lr * (
                reward + self.discount * max_next - current_values[self.last_action]
            )

            # Reset last state/action
            self.last_state = None
            self.last_action = None
            self.last_decision = None
            self.last_hours_to_deadline = None

            # Increment update counter and save if needed
            self._update_counter += 1
            if self._update_counter % self.save_every == 0:
                self._save_q_table_locked()

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

    def _is_node_feasible(self, node: NodeDescriptor, policy: Optional[FlexGenPolicy]) -> bool:
        """
        Check if the node has enough GPU memory for the given policy.
        This is a simplified feasibility check.
        """
        if policy is None:
            return True
        required_memory_gb = policy.gpu_batch_size * policy.block_size * 0.1  # rough estimation
        available_memory_gb = node.metadata.get("gpu_memory_gb", 16.0)
        return required_memory_gb <= available_memory_gb

    def _save_q_table(self) -> None:
        """Save Q-table to JSON (non‑lock, used externally)."""
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

    def _save_q_table_locked(self):
        """Save Q-table assuming the lock is already held."""
        self._save_q_table()  # but we must ensure we are inside async with self._lock

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
