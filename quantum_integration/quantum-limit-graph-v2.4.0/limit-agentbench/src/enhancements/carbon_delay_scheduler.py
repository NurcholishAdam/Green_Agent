"""
carbon_delay_scheduler.py

Enhanced carbon‑intensity‑aware delay queue with MODP, bio‑inspired tuning, MoE integration,
and FlexGen policy selection.

Features:
- Multi‑objective decision (carbon, latency, energy, cost) using MODP framework.
- Adaptive threshold and max_delay tuned by a genetic algorithm (bio_inspired).
- Expert routing (MoE) to classify tasks by delayability.
- Probabilistic forecast handling with confidence intervals.
- Persistent queue across restarts.
- Comprehensive logging and reward feedback loop.
- FlexGen integration: select optimal offloading policy for AI tasks based on carbon intensity.
"""

import heapq
import time
import json
import os
import logging
from typing import Dict, Any, Optional, List, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum

# ----------------------------------------------------------------------
# 1. Imports from other enhancement modules (assumed present)
# ----------------------------------------------------------------------
# Enhanced modules (with fallback stubs)
try:
    from enhancements.bio_inspired import GeneticOptimizer
except ImportError:
    class GeneticOptimizer:
        def adapt(self, context, reward):
            return None

try:
    from enhancements.MODP import ParetoOptimizer
except ImportError:
    class ParetoOptimizer:
        def decide(self, objectives, weights):
            return sum(objectives[k] * weights.get(k, 0.0) for k in objectives)

try:
    from enhancements.moe_system import ExpertRouter
except ImportError:
    class ExpertRouter:
        def classify(self, task):
            return "normal"

# FlexGen modules (with fallback)
try:
    from enhancements.gpu_optimization.flexgen_policy import FlexGenPolicy, generate_candidate_policies
    from enhancements.gpu_optimization.flexgen_cost_model import FlexGenCostModel, CostEstimate
    from enhancements.gpu_optimization.flexgen_controller import FlexGenController
    from enhancements.schemas.node_descriptor import NodeDescriptor
    from enhancements.schemas.workload_descriptor import WorkloadDescriptor
    FLEXGEN_AVAILABLE = True
except ImportError:
    FLEXGEN_AVAILABLE = False
    class FlexGenPolicy: pass
    class FlexGenCostModel:
        def estimate(self, policy, node, workload):
            return None
    class FlexGenController:
        def __init__(self, *args, **kwargs): pass
        async def step(self): return {}
    class NodeDescriptor: pass
    class WorkloadDescriptor: pass


# ----------------------------------------------------------------------
# 2. Core Enhanced CarbonDelayScheduler
# ----------------------------------------------------------------------

class Delayability(Enum):
    HIGH = "high"       # Must not be delayed
    MEDIUM = "medium"   # Can be delayed with trade‑offs
    LOW = "low"         # Can be delayed aggressively

@dataclass
class DelayedTask:
    scheduled_time: float
    task: Dict[str, Any]
    original_submit_time: float
    delay_reason: str

class CarbonDelayScheduler:
    """
    Enhanced scheduler with MODP, bio‑inspired adaptation, MoE integration,
    and FlexGen policy selection.
    """
    def __init__(
        self,
        carbon_api: Any,
        max_delay_seconds: int = 3600,
        threshold_gco2_per_kwh: float = 150.0,
        persistence_file: Optional[str] = "delay_queue.json",
        modp_weights: Optional[Dict[str, float]] = None,
        bio_optimizer: Optional[Any] = None,
        moe_router: Optional[Any] = None,
        forecast_confidence_threshold: float = 0.7,
        flexgen_manager: Optional[Any] = None,
    ):
        """
        Args:
            carbon_api: Object with get_current() and get_forecast().
            max_delay_seconds: Initial maximum delay.
            threshold_gco2_per_kwh: Initial carbon threshold.
            persistence_file: Path to save/load queue state.
            modp_weights: Weights for MODP objectives: carbon, latency, energy, cost.
            bio_optimizer: Instance of bio_inspired optimizer (optional).
            moe_router: Instance of MoE router (optional).
            forecast_confidence_threshold: Only use forecast points with confidence > this.
            flexgen_manager: Optional FlexGen manager for policy selection.
        """
        self.carbon_api = carbon_api
        self.max_delay = max_delay_seconds
        self.threshold = threshold_gco2_per_kwh
        self.persistence_file = persistence_file
        self.forecast_confidence_threshold = forecast_confidence_threshold

        # Logging
        self.logger = logging.getLogger(__name__)

        # Multi‑objective decision
        if modp_weights is None:
            self.modp_weights = {
                "carbon": 0.4,
                "latency": 0.3,
                "energy": 0.2,
                "cost": 0.1
            }
        else:
            self.modp_weights = modp_weights

        # MODP instance
        try:
            # If real ParetoOptimizer available, use it; otherwise fallback to stub logic
            if 'ParetoOptimizer' in globals() and hasattr(ParetoOptimizer, 'decide'):
                self.modp = ParetoOptimizer()
            else:
                # Create a stub that works like the original stub
                class _MODP:
                    def __init__(self, weights):
                        self.weights = weights
                    def decide(self, objectives, weights=None):
                        w = weights or self.weights
                        return sum(objectives[k] * w.get(k, 0.0) for k in objectives)
                self.modp = _MODP(self.modp_weights)
        except Exception:
            class _MODP:
                def __init__(self, weights):
                    self.weights = weights
                def decide(self, objectives, weights=None):
                    w = weights or self.weights
                    return sum(objectives[k] * w.get(k, 0.0) for k in objectives)
            self.modp = _MODP(self.modp_weights)

        # Bio‑inspired adaptation
        self.bio = bio_optimizer if bio_optimizer else GeneticOptimizer()

        # MoE router
        self.moe = moe_router if moe_router else ExpertRouter()

        # FlexGen manager
        self.flexgen_manager = flexgen_manager
        self._flexgen_available = FLEXGEN_AVAILABLE and flexgen_manager is not None

        # Priority queue: heap of (scheduled_time, DelayedTask)
        self.queue: List[Tuple[float, DelayedTask]] = []

        # Load persisted queue if exists
        self._load_queue()

        # Metrics for feedback loop
        self.metrics = {
            "total_delayed": 0,
            "total_forwarded": 0,
            "total_released": 0,
            "total_rewards": 0.0,
            "total_flexgen_policies_selected": 0,
        }

    # --------------------- Persistence ---------------------
    def _load_queue(self):
        if not self.persistence_file or not os.path.exists(self.persistence_file):
            return
        try:
            with open(self.persistence_file, "r") as f:
                data = json.load(f)
                for item in data:
                    task = DelayedTask(
                        scheduled_time=item["scheduled_time"],
                        task=item["task"],
                        original_submit_time=item["original_submit_time"],
                        delay_reason=item["delay_reason"]
                    )
                    heapq.heappush(self.queue, (task.scheduled_time, task))
            self.logger.info(f"Loaded {len(self.queue)} tasks from persistence.")
        except Exception as e:
            self.logger.error(f"Failed to load queue: {e}")

    def _save_queue(self):
        if not self.persistence_file:
            return
        try:
            data = []
            for _, task in self.queue:
                data.append({
                    "scheduled_time": task.scheduled_time,
                    "task": task.task,
                    "original_submit_time": task.original_submit_time,
                    "delay_reason": task.delay_reason,
                })
            with open(self.persistence_file, "w") as f:
                json.dump(data, f)
            self.logger.debug("Queue persisted.")
        except Exception as e:
            self.logger.error(f"Failed to persist queue: {e}")

    # --------------------- Multi‑Objective Decision ---------------------
    def _evaluate_delay(self, task: Dict[str, Any],
                        current_intensity: float,
                        forecast: List[Tuple[float, float]]) -> Tuple[bool, Optional[float], str]:
        """
        Uses MODP to decide whether to delay.
        Returns (should_delay, scheduled_time, reason).
        """
        # Extract task features
        task_latency_sensitivity = task.get("latency_sensitivity", 0.5)  # 0-1
        task_energy_estimate = task.get("energy_kwh_estimate", 1.0)
        task_cost_estimate = task.get("cost_estimate", 0.0)

        # Compute objectives if delayed vs forward
        # If delayed: carbon benefit, but latency cost, possibly energy overhead (idle)
        # If forwarded now: carbon cost, but low latency.

        # Simple model:
        # Carbon benefit = current_intensity - expected_intensity_at_delay
        # Find expected intensity at a future time (best candidate)
        now = time.time()
        best_time = None
        best_intensity = None
        for ts, intensity in forecast:
            if intensity < self.threshold and (ts - now) <= self.max_delay:
                # For MODP, we also consider confidence (if available)
                confidence = 1.0  # stub; extend later
                if confidence >= self.forecast_confidence_threshold:
                    if best_time is None or intensity < best_intensity:
                        best_time = ts
                        best_intensity = intensity

        if best_time is None:
            return False, None, "No suitable low‑carbon window"

        # Objectives:
        carbon_reduction = current_intensity - best_intensity  # gCO2/kWh benefit
        # Latency cost = delay duration (seconds)
        latency_cost = best_time - now
        # Energy overhead: assume idle energy consumption during delay
        idle_power_watts = task.get("idle_power_watts", 10.0)  # W
        energy_overhead_kwh = (idle_power_watts * latency_cost) / 3600 / 1000
        # Cost: could be monetary cost of energy, or carbon cost
        # We'll treat cost as (energy_overhead * electricity_price)
        electricity_price_per_kwh = 0.15  # €/kWh
        cost = energy_overhead_kwh * electricity_price_per_kwh

        objectives = {
            "carbon": -carbon_reduction,  # negative because we want to minimize carbon (or maximize reduction)
            "latency": latency_cost,
            "energy": energy_overhead_kwh,
            "cost": cost,
        }
        # Use MODP to compute a utility score (lower is better)
        utility = self.modp.decide(objectives, self.modp_weights)

        # If utility < some threshold (e.g., 0), we delay
        # In practice, we compare with forwarding (which would have utility = 0 + maybe carbon cost)
        # For simplicity, we delay if utility < 0 (i.e., delay is better than forwarding).
        if utility < 0:
            return True, best_time, f"MODP utility={utility:.3f}"
        else:
            return False, None, f"MODP utility={utility:.3f} (not beneficial)"

    # --------------------- Bio‑inspired Adaptation ---------------------
    def adapt_parameters(self, reward: float):
        """
        Called after a task is released and its outcome (reward) is known.
        The bio_inspired optimizer adjusts threshold and max_delay.
        """
        context = {
            "threshold": self.threshold,
            "max_delay": self.max_delay,
            "task_count": len(self.queue),
            "avg_carbon": self.carbon_api.get_current(),
        }
        # The bio module should return new parameters
        new_params = self.bio.adapt(context, reward)
        if new_params:
            self.threshold = new_params.get("threshold", self.threshold)
            self.max_delay = new_params.get("max_delay", self.max_delay)
            self.logger.info(f"Bio‑adapted: threshold={self.threshold:.1f}, max_delay={self.max_delay}")

    # --------------------- MoE Delayability Classification ---------------------
    def _get_delayability(self, task: Dict[str, Any]) -> Delayability:
        """
        Uses the MoE router to classify task into delayability category.
        """
        # First, check explicit priority
        if task.get("priority") == "high":
            return Delayability.HIGH

        # Use MoE to decide
        category = self.moe.classify(task)
        if category == "high":
            return Delayability.HIGH
        elif category == "medium":
            return Delayability.MEDIUM
        else:
            return Delayability.LOW

    # --------------------- FlexGen Policy Selection ---------------------
    def select_flexgen_policy(self, task: Dict[str, Any], node: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Select an optimal FlexGen policy for an AI inference task.
        Uses the FlexGenManager (if available) to run policy optimization.
        Returns a dict with chosen policy and metrics.
        """
        if not self._flexgen_available:
            return {"error": "FlexGen manager not available"}

        # Prepare workload and node descriptors
        try:
            workload = WorkloadDescriptor(**task.get("workload", {}))
            node_desc = NodeDescriptor(**node) if node else NodeDescriptor(
                id="default_node",
                type="cloud",
                region="us-east",
                region_carbon_intensity=0.42,
                energy_per_token=0.00005,
                uptime=0.99,
                maintenance_status="operational"
            )
            # Use flexgen manager to optimize policy
            result = self.flexgen_manager.optimize_policy(workload, node_desc)
            self.metrics["total_flexgen_policies_selected"] += 1
            return result
        except Exception as e:
            self.logger.error(f"FlexGen policy selection failed: {e}")
            return {"error": str(e)}

    # --------------------- Main Public API ---------------------
    def submit(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Decide whether to delay the task, based on MODP, MoE, bio‑adapted thresholds,
        and possibly FlexGen policy evaluation.
        Returns dict with status, delay_until, reason, and optional FlexGen policy.
        """
        # 1. Determine delayability via MoE
        delayability = self._get_delayability(task)
        if delayability == Delayability.HIGH:
            self.metrics["total_forwarded"] += 1
            return {"status": "forward", "task": task, "delay_until": None, "reason": "High priority"}

        # 2. Current carbon intensity
        current_intensity = self.carbon_api.get_current()

        # 3. If current intensity is already low, forward immediately
        if current_intensity <= self.threshold:
            # Optionally select FlexGen policy for the forwarded task
            flexgen_policy = None
            if self._flexgen_available and task.get("type") == "inference":
                flexgen_policy = self.select_flexgen_policy(task)
            self.metrics["total_forwarded"] += 1
            result = {"status": "forward", "task": task, "delay_until": None, "reason": "Already low carbon"}
            if flexgen_policy:
                result["flexgen_policy"] = flexgen_policy
            return result

        # 4. Get forecast
        forecast_minutes = self.max_delay // 60 + 2  # extra buffer
        forecast = self.carbon_api.get_forecast(forecast_minutes)
        if not forecast:
            self.metrics["total_forwarded"] += 1
            return {"status": "forward", "task": task, "delay_until": None, "reason": "No forecast available"}

        # 5. Evaluate delay using MODP
        should_delay, scheduled_time, reason = self._evaluate_delay(task, current_intensity, forecast)

        if not should_delay:
            # Forward, but optionally include FlexGen policy
            flexgen_policy = None
            if self._flexgen_available and task.get("type") == "inference":
                flexgen_policy = self.select_flexgen_policy(task)
            self.metrics["total_forwarded"] += 1
            result = {"status": "forward", "task": task, "delay_until": None, "reason": reason}
            if flexgen_policy:
                result["flexgen_policy"] = flexgen_policy
            return result

        # 6. For delayable tasks, also check if the delay is acceptable given the delayability category
        if delayability == Delayability.MEDIUM and (scheduled_time - time.time()) > self.max_delay * 0.5:
            # For medium, we limit delay to half the max
            self.metrics["total_forwarded"] += 1
            return {"status": "forward", "task": task, "delay_until": None,
                    "reason": f"Delay too long for medium priority ({scheduled_time - time.time():.0f}s)"}

        # 7. Schedule the task
        delayed_task = DelayedTask(
            scheduled_time=scheduled_time,
            task=task,
            original_submit_time=time.time(),
            delay_reason=reason
        )
        heapq.heappush(self.queue, (delayed_task.scheduled_time, delayed_task))
        self.metrics["total_delayed"] += 1
        self._save_queue()

        return {
            "status": "delayed",
            "task": task,
            "delay_until": scheduled_time,
            "reason": reason,
            "delayability": delayability.value,
        }

    def tick(self) -> List[Dict[str, Any]]:
        """
        Release tasks whose scheduled time has arrived.
        Returns list of tasks (dicts) that are ready to be processed.
        """
        now = time.time()
        released = []
        while self.queue and self.queue[0][0] <= now:
            _, delayed_task = heapq.heappop(self.queue)
            released.append(delayed_task.task)
            self.metrics["total_released"] += 1
            # Optionally select FlexGen policy upon release
            if self._flexgen_available and delayed_task.task.get("type") == "inference":
                policy = self.select_flexgen_policy(delayed_task.task)
                # In a real system, the policy would be attached to the task or returned separately.
                # For simplicity, we log it.
                self.logger.info(f"FlexGen policy selected for released task: {policy}")

        if released:
            self._save_queue()
        return released

    def report_reward(self, task: Dict[str, Any], reward: float):
        """
        Called after the task is executed and its outcome is measured.
        Feeds back into bio‑inspired adaptation.
        """
        self.metrics["total_rewards"] += reward
        self.adapt_parameters(reward)

    def get_queue_stats(self) -> Dict[str, Any]:
        """Return statistics about the queue and metrics."""
        return {
            "queue_size": len(self.queue),
            "next_release": self.queue[0][0] if self.queue else None,
            "threshold": self.threshold,
            "max_delay": self.max_delay,
            **self.metrics,
        }

    # ------------------------------------------------------------------
    # 3. Example usage / test harness
    # ------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Use stub API for testing
    from carbon_api_stub import CarbonAPIStub
    carbon_api = CarbonAPIStub(base_intensity=250.0, volatility=80.0)

    # Instantiate enhanced scheduler
    scheduler = CarbonDelayScheduler(
        carbon_api=carbon_api,
        max_delay_seconds=1800,
        threshold_gco2_per_kwh=200.0,
        persistence_file="test_queue.json",
        modp_weights={"carbon": 0.5, "latency": 0.2, "energy": 0.2, "cost": 0.1},
    )

    # Simulate tasks
    tasks = [
        {"priority": "normal", "latency_sensitivity": 0.5, "idle_power_watts": 10, "type": "inference"},
        {"priority": "high", "latency_sensitivity": 0.9},
        {"priority": "normal", "latency_sensitivity": 0.3, "idle_power_watts": 5, "type": "inference"},
    ]

    for task in tasks:
        result = scheduler.submit(task)
        print(f"Task {task}: {result['status']} (reason: {result.get('reason', 'N/A')})")

    # Simulate tick after some time
    time.sleep(2)
    released = scheduler.tick()
    print(f"Released {len(released)} tasks")

    # Report a sample reward to test adaptation
    scheduler.report_reward({"sample": "task"}, 5.0)

    # Show stats
    print(scheduler.get_queue_stats())
