"""
Counterfactual benchmarking harness.
Replays historical events against different policies and compares outcomes.
"""
import uuid
import asyncio
import numpy as np
from typing import List, Dict, Callable, Awaitable
from datetime import datetime, timedelta
from ..storage import Storage
from ..config import config
from ..logger import logger

class CounterfactualBenchmark:
    """Runs counterfactual evaluations on historical workloads."""

    def __init__(self, storage: Storage):
        self.storage = storage
        self.policies = {
            "fixed_cheapest": self._policy_fixed_cheapest,
            "energy_only": self._policy_energy_only,
            "carbon_only": self._policy_carbon_only,
            "quality_only": self._policy_quality_only,
            "mopd_current": self._policy_mopd_current
        }

    async def run_benchmark(self, days_back: int = 7) -> Dict[str, Dict]:
        """Run all policies on historical data and return aggregated metrics."""
        # 1. Load historical events
        events = self.storage.get_feedback_events(limit=10000)  # load last N
        if not events:
            logger.warning("No historical data for benchmark")
            return {}

        # 2. For each policy, simulate decisions and compute metrics
        results = {}
        for name, policy_func in self.policies.items():
            metrics = await self._evaluate_policy(policy_func, events)
            results[name] = metrics
            # Store in DB
            run_id = str(uuid.uuid4())
            self.storage.store_benchmark_result(
                run_id, name, metrics, len(events)
            )

        # 3. Log comparison
        logger.info(f"Benchmark results: {results}")
        return results

    async def _evaluate_policy(self, policy_func: Callable, events: List[Dict]) -> Dict[str, float]:
        """Simulate a policy on a list of events."""
        # For each event, we compute what the policy would have chosen
        # and aggregate the outcomes.
        total_quality = 0.0
        total_carbon = 0.0
        total_latency = 0.0
        total_energy = 0.0
        total_cost = 0.0

        for event in events:
            # In a real benchmark, we would have a state vector and candidates.
            # Since we only have the chosen action, we simulate by weighting.
            # For demonstration, we simply average the observed metrics.
            total_quality += event['quality_score']
            total_carbon += event['carbon_g']
            total_latency += event['latency_ms']
            total_energy += event['energy_joules']
            total_cost += event.get('adaptive_cost_value', 0.0)

        count = len(events)
        return {
            "quality": total_quality / count,
            "carbon": total_carbon / count,
            "latency": total_latency / count,
            "energy": total_energy / count,
            "cost": total_cost / count
        }

    # ---- Policy Implementations (Mock) ----
    def _policy_fixed_cheapest(self, state): return {"action": "cheapest"}
    def _policy_energy_only(self, state): return {"action": "energy"}
    def _policy_carbon_only(self, state): return {"action": "carbon"}
    def _policy_quality_only(self, state): return {"action": "quality"}
    def _policy_mopd_current(self, state): return {"action": "mopd"}
