#!/usr/bin/env python3
"""
Enhanced MODP‑based time‑shifting scheduler (v2.0).
Decides whether to run a workload now, defer, or move to another node.
Uses carbon forecasts, queue length, deadlines, and Q‑learning to optimize.

Improvements:
- Forecast‑aware action selection (looks at predicted carbon for deferrals).
- State includes current carbon, queue, deadline, and forecast mean.
- Optimistic initialization and epsilon decay for better exploration.
- Persistence of Q‑table to JSON.
- Asynchronous lock for thread safety.
- Better move‑node selection based on forecast.
- Reward computation helper.
"""

import asyncio
import logging
import json
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, timedelta
import numpy as np
from pathlib import Path

from ..data_integration.carbon_intensity import CarbonIntensityFetcher
from ..async_message_queue import AsyncMessageQueue
from ..schemas.feedback_event import FeedbackEvent
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..schemas.node_descriptor import NodeDescriptor
from ..logger import logger


class MODPScheduler:
    """
    MODP-based scheduler that decides when (and where) to run a workload.
    Uses a simple Q-learning approach for deferral decisions, enhanced with
    carbon forecasts and node selection.
    """

    def __init__(
        self,
        carbon_fetcher: Optional[CarbonIntensityFetcher] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        learning_rate: float = 0.1,
        discount_factor: float = 0.9,
        epsilon: float = 0.1,
        epsilon_decay: float = 0.995,
        min_epsilon: float = 0.01,
        horizon: int = 6,
        defer_penalty: float = 0.05,
        move_penalty: float = 0.1,
        q_table_path: Optional[Path] = None,
        optimistic_init: float = 1.0,
    ):
        self.carbon_fetcher = carbon_fetcher
        self.message_queue = message_queue
        self.lr = learning_rate
        self.discount = discount_factor
        self.epsilon = epsilon
        self.epsilon_decay = epsilon_decay
        self.min_epsilon = min_epsilon
        self.horizon = horizon
        self.defer_penalty = defer_penalty
        self.move_penalty = move_penalty
        self.q_table_path = q_table_path or Path("./modp_q_table.json")
        self.optimistic_init = optimistic_init

        # Q-table: dict of state tuple -> numpy array (action values)
        self.q_table: Dict[Tuple[int, int, int, int], np.ndarray] = {}
        self.last_state: Optional[Tuple[int, int, int, int]] = None
        self.last_action: Optional[int] = None
        self._lock = asyncio.Lock()

        # Load Q-table if exists
        self._load_q_table()

    def _discretize_state(
        self,
        current_carbon: float,
        queue_length: int,
        hours_to_deadline: float,
        forecast_mean: float,
    ) -> Tuple[int, int, int, int]:
        """Convert continuous state to discrete buckets."""
        carbon_bucket = int(current_carbon // 50)          # 50 gCO2/kWh per bucket
        queue_bucket = min(int(queue_length), 10)         # cap at 10
        deadline_bucket = min(int(hours_to_deadline), 24)  # cap at 24
        forecast_bucket = int(forecast_mean // 50)         # 50 gCO2/kWh per bucket
        return (carbon_bucket, queue_bucket, deadline_bucket, forecast_bucket)

    def _get_action_values(self, state: Tuple[int, int, int, int]) -> np.ndarray:
        """Return Q-values for all actions for a given state, initializing if needed."""
        if state not in self.q_table:
            # Actions: run_now (0), defer 1..horizon (1..horizon), move_node (horizon+1)
            self.q_table[state] = np.full(self.horizon + 2, self.optimistic_init, dtype=float)
        return self.q_table[state]

    async def get_carbon_forecast(self, hours: Optional[int] = None) -> List[float]:
        """Return predicted carbon intensity for next `hours` hours."""
        hours = hours or self.horizon
        if self.carbon_fetcher:
            try:
                forecast = await self.carbon_fetcher.forecast_carbon_prices(hours=hours)
                if isinstance(forecast, dict) and forecast.get('status') == 'success':
                    predictions = forecast.get('predictions', [])
                    if predictions:
                        return predictions
                elif isinstance(forecast, list):
                    return forecast
            except Exception as e:
                logger.warning(f"Carbon forecast failed: {e}")
        # Fallback: constant carbon intensity
        return [400.0] * hours

    async def get_current_carbon(self) -> float:
        """Get current carbon intensity, with fallback."""
        if self.carbon_fetcher:
            try:
                return await self.carbon_fetcher.get_current_intensity()
            except Exception as e:
                logger.warning(f"Current carbon fetch failed: {e}")
        return 400.0

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
        # Get current carbon and forecast
        if current_carbon is None:
            current_carbon = await self.get_current_carbon()

        forecast = await self.get_carbon_forecast(self.horizon)
        forecast_mean = float(np.mean(forecast)) if forecast else current_carbon

        # Hours to deadline
        if workload.deadline:
            hours_to_deadline = max(0.0, (workload.deadline - datetime.utcnow()).total_seconds() / 3600)
        else:
            hours_to_deadline = 24.0

        # Discretize state
        state = self._discretize_state(current_carbon, queue_length, hours_to_deadline, forecast_mean)

        async with self._lock:
            action_values = self._get_action_values(state)

            # Epsilon-greedy
            if np.random.random() < self.epsilon:
                action = np.random.randint(len(action_values))
            else:
                # For defer actions, adjust Q-values with forecast penalty
                adjusted_values = action_values.copy()
                for i in range(1, self.horizon + 1):
                    # Penalize based on forecast at that hour (if available)
                    if i <= len(forecast):
                        future_carbon = forecast[i - 1]
                        # Lower is better; we subtract a small term to favor lower carbon later
                        adjusted_values[i] -= self.defer_penalty * (future_carbon / 100.0)
                    else:
                        adjusted_values[i] -= self.defer_penalty
                # Move penalty
                adjusted_values[self.horizon + 1] -= self.move_penalty
                action = int(np.argmax(adjusted_values))

            # Interpret action
            if action == 0:
                decision = ("run_now", 0, None)
            elif 1 <= action <= self.horizon:
                decision = ("defer", action, None)
            else:  # move_node
                if available_nodes:
                    # Select node with lowest forecasted carbon at future time (use current intensity)
                    # For simplicity, use region_carbon_intensity as proxy
                    best_node = min(available_nodes, key=lambda n: n.region_carbon_intensity)
                    decision = ("move_node", 0, best_node.id)
                else:
                    decision = ("run_now", 0, None)  # fallback

            # Store for learning
            self.last_state = state
            self.last_action = action

            # Decay epsilon
            self.epsilon = max(self.min_epsilon, self.epsilon * self.epsilon_decay)

        # Publish decision as FeedbackEvent
        if self.message_queue and FeedbackEvent:
            event = FeedbackEvent(
                source="modp_scheduler",
                feedback_type="routing",
                task_id=workload.task_id or "unknown",
                context={
                    "current_carbon": current_carbon,
                    "forecast_mean": forecast_mean,
                    "queue_length": queue_length,
                    "hours_to_deadline": hours_to_deadline,
                    "available_nodes": [n.id for n in available_nodes] if available_nodes else [],
                },
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

    async def learn(
        self,
        reward: float,
        next_carbon: float,
        next_queue_length: int,
        next_forecast_mean: Optional[float] = None,
    ):
        """Update Q-values based on observed reward and next state."""
        if self.last_state is None or self.last_action is None:
            return

        if next_forecast_mean is None:
            next_forecast_mean = next_carbon  # fallback

        next_state = self._discretize_state(
            next_carbon, next_queue_length, 24, next_forecast_mean
        )

        async with self._lock:
            next_values = self._get_action_values(next_state)
            max_next = np.max(next_values) if len(next_values) > 0 else 0

            current_values = self._get_action_values(self.last_state)
            current_values[self.last_action] += self.lr * (
                reward + self.discount * max_next - current_values[self.last_action]
            )

        # Reset last state/action
        self.last_state = None
        self.last_action = None

        # Save Q-table periodically (could be optimized)
        await self._save_q_table()

    def get_policy_stats(self) -> Dict[str, Any]:
        """Return simple stats about Q-table."""
        async_lock = asyncio.Lock()  # Not needed for sync method; we'll just compute
        num_states = len(self.q_table)
        avg_q = np.mean([np.mean(v) for v in self.q_table.values()]) if self.q_table else 0.0
        return {
            "num_states": num_states,
            "avg_q_value": avg_q,
            "epsilon": self.epsilon,
        }

    async def _save_q_table(self):
        """Persist Q-table to JSON."""
        try:
            # Convert keys (tuples) to strings for JSON
            data = {str(k): v.tolist() for k, v in self.q_table.items()}
            with open(self.q_table_path, 'w') as f:
                json.dump(data, f, indent=2)
            logger.debug(f"Q-table saved to {self.q_table_path}")
        except Exception as e:
            logger.error(f"Failed to save Q-table: {e}")

    def _load_q_table(self):
        """Load Q-table from JSON if exists."""
        if not self.q_table_path.exists():
            return
        try:
            with open(self.q_table_path, 'r') as f:
                data = json.load(f)
            for key_str, values in data.items():
                # Convert string key back to tuple of ints
                key = tuple(map(int, key_str.strip('()').split(', ')))
                self.q_table[key] = np.array(values, dtype=float)
            logger.info(f"Loaded Q-table with {len(self.q_table)} states from {self.q_table_path}")
        except Exception as e:
            logger.error(f"Failed to load Q-table: {e}")

    async def compute_reward(
        self,
        decision: str,
        delay_hours: int,
        carbon_at_execution: float,
        queue_length: int,
        success: bool = True,
    ) -> float:
        """Compute reward for a decision."""
        if not success:
            return -1.0
        base_reward = 1.0
        if decision == "defer":
            # Penalty for delaying, but reward if carbon was lower
            base_reward -= self.defer_penalty * delay_hours
            # Bonus if carbon intensity is low
            if carbon_at_execution < 200:
                base_reward += 0.2
        elif decision == "move_node":
            base_reward -= self.move_penalty
            if carbon_at_execution < 200:
                base_reward += 0.2
        # Penalize long queue
        base_reward -= 0.01 * min(queue_length, 10)
        return max(0.0, min(1.0, base_reward))
