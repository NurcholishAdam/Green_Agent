"""
carbon_delay_scheduler.py

Enhanced carbon‑intensity‑aware delay queue with MODP, bio‑inspired tuning, MoE integration,
FlexGen policy selection, and NEW additions:
- Causal Reinforcement Learning via CausalBandit.
- Temporal Logic Safety Monitor.
- Explainable AI (XAI) for every decision.
- Federated Learning Coordinator (stub).
- Multi-Agent Coordinator (basic).
- Chaos Monkey for resilience testing.
- Human-in-the-Loop with Active Learning.

All previous features retained.
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

# NEW: CausalBandit for policy selection
class CausalBandit:
    """
    A simple causal bandit that maintains estimates of average treatment effects
    for each scheduling action (delay vs. forward). Used instead of or alongside
    bio-inspired optimization.
    """
    def __init__(self, actions: List[str] = ["delay", "forward"], initial_value: float = 0.0):
        self.actions = actions
        self.q_values = {a: initial_value for a in actions}
        self.counts = {a: 0 for a in actions}
        self.causal_effects = {a: 0.0 for a in actions}
        self.trials = 0
        self.context_history = []
        self.reward_history = []
        self.action_history = []

    def select_action(self, context: Dict) -> str:
        # epsilon-greedy with causal effect preference
        epsilon = 0.1
        if random.random() < epsilon:
            return random.choice(self.actions)
        # Use causal effect if enough data, else Q-values
        if self.trials >= 10 and any(self.causal_effects.values()):
            return max(self.causal_effects, key=self.causal_effects.get)
        return max(self.q_values, key=self.q_values.get)

    def update(self, context: Dict, action: str, reward: float):
        self.trials += 1
        self.counts[action] += 1
        self.q_values[action] += (reward - self.q_values[action]) / self.counts[action]
        self.context_history.append(context)
        self.reward_history.append(reward)
        self.action_history.append(action)
        # Estimate causal effect: average reward for this action
        rewards = [r for a, r in zip(self.action_history, self.reward_history) if a == action]
        self.causal_effects[action] = sum(rewards) / len(rewards) if rewards else 0.0

# NEW: Safety Monitor (Temporal Logic-like)
class SafetyMonitor:
    def __init__(self, max_delay_high_priority: float = 300.0):  # 5 minutes
        self.rules = {
            "max_delay_high_priority": lambda task, delay: task.get("priority") == "high" and delay > max_delay_high_priority,
            "no_delay_without_forecast": lambda task, delay, forecast: forecast is None and delay > 0,
        }

    def check(self, task: Dict, delay: float, forecast: Optional[List[Tuple[float, float]]] = None) -> bool:
        """
        Returns True if safe, False if violation.
        """
        if self.rules["max_delay_high_priority"](task, delay):
            return False
        if self.rules["no_delay_without_forecast"](task, delay, forecast):
            return False
        return True

# NEW: XAI Explainer
class XAIExplainer:
    def explain_delay(self, task: Dict, current_intensity: float, best_intensity: float,
                      delay_seconds: float, utility: float, reason: str) -> str:
        parts = []
        if delay_seconds > 0:
            parts.append(f"Delaying task for {delay_seconds:.0f}s.")
            parts.append(f"Carbon intensity drops from {current_intensity:.1f} to {best_intensity:.1f} gCO2/kWh.")
        else:
            parts.append("Forwarding task immediately.")
            if reason:
                parts.append(f"Reason: {reason}")
        parts.append(f"Utility score: {utility:.3f}")
        return " ".join(parts)

# NEW: Federated Learning Coordinator (stub)
class FederatedCoordinator:
    def __init__(self):
        self.participants = {}

    def register(self, participant_id: str, model_update: Dict):
        self.participants[participant_id] = model_update

    def aggregate(self) -> Dict:
        if not self.participants:
            return {}
        # Average all model updates
        keys = set()
        for p in self.participants.values():
            keys.update(p.keys())
        avg = {}
        for key in keys:
            vals = [p.get(key, 0.0) for p in self.participants.values()]
            avg[key] = sum(vals) / len(vals)
        return avg

# NEW: Multi-Agent Coordinator (basic)
class MultiAgentCoordinator:
    def __init__(self, num_agents: int = 3):
        self.agents = [f"agent_{i}" for i in range(num_agents)]
        self.responsibilities = {agent: [] for agent in self.agents}

    def assign_task(self, task: Dict) -> str:
        # Simple round-robin assignment
        agent = self.agents[hash(task.get("id", str(time.time()))) % len(self.agents)]
        self.responsibilities[agent].append(task.get("id"))
        return agent

# NEW: Chaos Monkey
class ChaosMonkey:
    def __init__(self, failure_probability: float = 0.1, enabled: bool = True):
        self.failure_probability = failure_probability
        self.enabled = enabled

    def maybe_fail(self):
        if self.enabled and random.random() < self.failure_probability:
            raise Exception("Simulated chaos failure")

# NEW: Human Review Manager
class HumanReviewManager:
    def __init__(self):
        self.pending_reviews = {}

    def request_review(self, task: Dict, decision: Dict) -> str:
        review_id = str(uuid.uuid4()) if 'uuid' in globals() else str(time.time())
        self.pending_reviews[review_id] = {"task": task, "decision": decision, "status": "pending"}
        return review_id

    def approve(self, review_id: str):
        if review_id in self.pending_reviews:
            self.pending_reviews[review_id]["status"] = "approved"

    def reject(self, review_id: str):
        if review_id in self.pending_reviews:
            self.pending_reviews[review_id]["status"] = "rejected"

    def get_pending(self) -> List[str]:
        return [rid for rid, data in self.pending_reviews.items() if data["status"] == "pending"]


class CarbonDelayScheduler:
    """
    Enhanced scheduler with MODP, bio‑inspired adaptation, MoE integration,
    FlexGen policy selection, and now also:
    - Causal Bandit for action selection.
    - Safety Monitor.
    - XAI Explanations.
    - Federated Learning (optional).
    - Multi-Agent Coordination (basic).
    - Chaos Monkey.
    - Human-in-the-Loop.

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
        causal_bandit: Optional[CausalBandit] = None,
        safety_monitor: Optional[SafetyMonitor] = None,
        xai_explainer: Optional[XAIExplainer] = None,
        federated_coordinator: Optional[FederatedCoordinator] = None,
        multi_agent_coordinator: Optional[MultiAgentCoordinator] = None,
        chaos_monkey: Optional[ChaosMonkey] = None,
        human_review: Optional[HumanReviewManager] = None,
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
            causal_bandit: Optional causal bandit for scheduling decisions.
            safety_monitor: Optional safety monitor.
            xai_explainer: Optional XAI explainer.
            federated_coordinator: Optional federated learning coordinator.
            multi_agent_coordinator: Optional multi-agent coordinator.
            chaos_monkey: Optional chaos monkey for resilience testing.
            human_review: Optional human review manager.
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
            if 'ParetoOptimizer' in globals() and hasattr(ParetoOptimizer, 'decide'):
                self.modp = ParetoOptimizer()
            else:
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

        # Causal bandit (if provided, else default)
        self.causal_bandit = causal_bandit if causal_bandit else CausalBandit()

        # MoE router
        self.moe = moe_router if moe_router else ExpertRouter()

        # Safety monitor
        self.safety_monitor = safety_monitor if safety_monitor else SafetyMonitor()

        # XAI explainer
        self.xai = xai_explainer if xai_explainer else XAIExplainer()

        # Federated coordinator (optional)
        self.federated = federated_coordinator

        # Multi-agent coordinator (optional)
        self.multi_agent = multi_agent_coordinator

        # Chaos monkey
        self.chaos_monkey = chaos_monkey if chaos_monkey else ChaosMonkey(enabled=False)

        # Human review manager
        self.human_review = human_review if human_review else HumanReviewManager()

        # FlexGen manager
        self.flexgen_manager = flexgen_manager
        self._flexgen_available = FLEXGEN_AVAILABLE and flexgen_manager is not None

        # Priority queue
        self.queue: List[Tuple[float, DelayedTask]] = []

        # Load persisted queue if exists
        self._load_queue()

        # Metrics
        self.metrics = {
            "total_delayed": 0,
            "total_forwarded": 0,
            "total_released": 0,
            "total_rewards": 0.0,
            "total_flexgen_policies_selected": 0,
            "total_causal_bandit_actions": 0,
            "total_safety_violations": 0,
            "total_reviews_requested": 0,
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
        task_latency_sensitivity = task.get("latency_sensitivity", 0.5)
        task_energy_estimate = task.get("energy_kwh_estimate", 1.0)
        task_cost_estimate = task.get("cost_estimate", 0.0)

        # Simulate chaos
        self.chaos_monkey.maybe_fail()

        # Find best low-carbon window
        now = time.time()
        best_time = None
        best_intensity = None
        for ts, intensity in forecast:
            if intensity < self.threshold and (ts - now) <= self.max_delay:
                confidence = 1.0  # stub; could be from forecast
                if confidence >= self.forecast_confidence_threshold:
                    if best_time is None or intensity < best_intensity:
                        best_time = ts
                        best_intensity = intensity

        if best_time is None:
            return False, None, "No suitable low‑carbon window"

        # Objectives
        carbon_reduction = current_intensity - best_intensity
        latency_cost = best_time - now
        idle_power_watts = task.get("idle_power_watts", 10.0)
        energy_overhead_kwh = (idle_power_watts * latency_cost) / 3600 / 1000
        electricity_price_per_kwh = 0.15
        cost = energy_overhead_kwh * electricity_price_per_kwh

        objectives = {
            "carbon": -carbon_reduction,
            "latency": latency_cost,
            "energy": energy_overhead_kwh,
            "cost": cost,
        }
        utility = self.modp.decide(objectives, self.modp_weights)

        if utility < 0:
            return True, best_time, f"MODP utility={utility:.3f}"
        else:
            return False, None, f"MODP utility={utility:.3f} (not beneficial)"

    # --------------------- Bio‑inspired Adaptation ---------------------
    def adapt_parameters(self, reward: float):
        context = {
            "threshold": self.threshold,
            "max_delay": self.max_delay,
            "task_count": len(self.queue),
            "avg_carbon": self.carbon_api.get_current(),
        }
        new_params = self.bio.adapt(context, reward)
        if new_params:
            self.threshold = new_params.get("threshold", self.threshold)
            self.max_delay = new_params.get("max_delay", self.max_delay)
            self.logger.info(f"Bio‑adapted: threshold={self.threshold:.1f}, max_delay={self.max_delay}")

    # --------------------- MoE Delayability Classification ---------------------
    def _get_delayability(self, task: Dict[str, Any]) -> Delayability:
        if task.get("priority") == "high":
            return Delayability.HIGH

        category = self.moe.classify(task)
        if category == "high":
            return Delayability.HIGH
        elif category == "medium":
            return Delayability.MEDIUM
        else:
            return Delayability.LOW

    # --------------------- FlexGen Policy Selection ---------------------
    def select_flexgen_policy(self, task: Dict[str, Any], node: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not self._flexgen_available:
            return {"error": "FlexGen manager not available"}
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
            result = self.flexgen_manager.optimize_policy(workload, node_desc)
            self.metrics["total_flexgen_policies_selected"] += 1
            return result
        except Exception as e:
            self.logger.error(f"FlexGen policy selection failed: {e}")
            return {"error": str(e)}

    # --------------------- Main Public API ---------------------
    def submit(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Decide whether to delay the task, based on MODP, MoE, causal bandit,
        safety monitor, and possibly FlexGen policy evaluation.
        Returns dict with status, delay_until, reason, explanation, and optional FlexGen policy.
        """
        # 1. Determine delayability via MoE
        delayability = self._get_delayability(task)
        if delayability == Delayability.HIGH:
            self.metrics["total_forwarded"] += 1
            explanation = self.xai.explain_delay(task, 0, 0, 0, 0, "High priority")
            return {"status": "forward", "task": task, "delay_until": None, "reason": "High priority",
                    "explanation": explanation}

        # 2. Current carbon intensity
        current_intensity = self.carbon_api.get_current()

        # 3. If current intensity is already low, forward immediately
        if current_intensity <= self.threshold:
            flexgen_policy = None
            if self._flexgen_available and task.get("type") == "inference":
                flexgen_policy = self.select_flexgen_policy(task)
            self.metrics["total_forwarded"] += 1
            explanation = self.xai.explain_delay(task, current_intensity, current_intensity, 0, 0, "Already low carbon")
            result = {"status": "forward", "task": task, "delay_until": None, "reason": "Already low carbon",
                      "explanation": explanation}
            if flexgen_policy:
                result["flexgen_policy"] = flexgen_policy
            return result

        # 4. Get forecast
        forecast_minutes = self.max_delay // 60 + 2
        forecast = self.carbon_api.get_forecast(forecast_minutes)
        if not forecast:
            self.metrics["total_forwarded"] += 1
            explanation = self.xai.explain_delay(task, current_intensity, current_intensity, 0, 0, "No forecast available")
            return {"status": "forward", "task": task, "delay_until": None, "reason": "No forecast available",
                    "explanation": explanation}

        # 5. Evaluate delay using MODP
        should_delay, scheduled_time, reason = self._evaluate_delay(task, current_intensity, forecast)

        # 6. Use causal bandit to decide final action (override if bandit says forward)
        #    (We treat bandit as a meta-decision on whether to trust MODP's delay suggestion)
        context = {
            "current_intensity": current_intensity,
            "forecast_available": len(forecast) > 0,
            "delayability": delayability.value,
            "task_type": task.get("type", "unknown"),
        }
        bandit_action = self.causal_bandit.select_action(context)
        self.metrics["total_causal_bandit_actions"] += 1

        if should_delay and bandit_action == "forward":
            # Bandit suggests not delaying, override
            should_delay = False
            reason = "Causal bandit suggests forward"
        elif not should_delay and bandit_action == "delay":
            # Bandit suggests delaying, but only if MODP also found a window
            if scheduled_time is not None:
                should_delay = True
                reason = "Causal bandit suggests delay"

        # 7. Safety monitor check
        delay_seconds = scheduled_time - time.time() if should_delay else 0
        if should_delay and not self.safety_monitor.check(task, delay_seconds, forecast):
            self.metrics["total_safety_violations"] += 1
            should_delay = False
            reason = "Safety monitor violation"
            scheduled_time = None

        # 8. If medium and delay too long, forward
        if should_delay and delayability == Delayability.MEDIUM and delay_seconds > self.max_delay * 0.5:
            should_delay = False
            reason = f"Delay too long for medium priority ({delay_seconds:.0f}s)"
            scheduled_time = None

        # 9. Final decision
        if should_delay:
            delayed_task = DelayedTask(
                scheduled_time=scheduled_time,
                task=task,
                original_submit_time=time.time(),
                delay_reason=reason
            )
            heapq.heappush(self.queue, (delayed_task.scheduled_time, delayed_task))
            self.metrics["total_delayed"] += 1
            self._save_queue()
            explanation = self.xai.explain_delay(
                task, current_intensity, scheduled_time and self._get_forecast_intensity_at(forecast, scheduled_time),
                delay_seconds, 0.0, reason
            )
            result = {
                "status": "delayed",
                "task": task,
                "delay_until": scheduled_time,
                "reason": reason,
                "delayability": delayability.value,
                "explanation": explanation,
            }
            # Human review for long delays
            if delay_seconds > self.max_delay * 0.8:
                review_id = self.human_review.request_review(task, result)
                self.metrics["total_reviews_requested"] += 1
                result["review_id"] = review_id
                result["review_pending"] = True
            return result
        else:
            flexgen_policy = None
            if self._flexgen_available and task.get("type") == "inference":
                flexgen_policy = self.select_flexgen_policy(task)
            self.metrics["total_forwarded"] += 1
            explanation = self.xai.explain_delay(task, current_intensity, current_intensity, 0, 0, reason)
            result = {"status": "forward", "task": task, "delay_until": None, "reason": reason,
                      "explanation": explanation}
            if flexgen_policy:
                result["flexgen_policy"] = flexgen_policy
            return result

    def _get_forecast_intensity_at(self, forecast: List[Tuple[float, float]], timestamp: float) -> Optional[float]:
        """Helper to find forecast intensity at a specific timestamp (linear interpolation)."""
        if not forecast:
            return None
        # Find bracketing points
        prev = None
        for ts, intensity in forecast:
            if ts <= timestamp:
                prev = (ts, intensity)
            else:
                if prev is not None:
                    # Interpolate
                    t0, i0 = prev
                    t1, i1 = ts, intensity
                    if t1 == t0:
                        return i0
                    ratio = (timestamp - t0) / (t1 - t0)
                    return i0 + ratio * (i1 - i0)
                else:
                    return intensity
        return prev[1] if prev else None

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
            if self._flexgen_available and delayed_task.task.get("type") == "inference":
                policy = self.select_flexgen_policy(delayed_task.task)
                self.logger.info(f"FlexGen policy selected for released task: {policy}")

        if released:
            self._save_queue()
        return released

    def report_reward(self, task: Dict[str, Any], reward: float, action: Optional[str] = None):
        """
        Called after the task is executed and its outcome is measured.
        Feeds back into bio‑inspired adaptation and causal bandit.
        action: 'delay' or 'forward' (if known)
        """
        self.metrics["total_rewards"] += reward
        self.adapt_parameters(reward)
        if action:
            context = {
                "current_intensity": self.carbon_api.get_current(),
                "task_type": task.get("type", "unknown"),
            }
            self.causal_bandit.update(context, action, reward)

    def federated_update(self, model_update: Dict):
        if self.federated:
            self.federated.register(self, model_update)

    def aggregate_federated(self) -> Dict:
        if self.federated:
            return self.federated.aggregate()
        return {}

    def get_queue_stats(self) -> Dict[str, Any]:
        return {
            "queue_size": len(self.queue),
            "next_release": self.queue[0][0] if self.queue else None,
            "threshold": self.threshold,
            "max_delay": self.max_delay,
            **self.metrics,
        }


# ----------------------------------------------------------------------
# 3. Example usage / test harness
# ----------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Use stub API for testing
    from carbon_api_stub import CarbonAPIStub
    carbon_api = CarbonAPIStub(base_intensity=250.0, volatility=80.0)

    # Instantiate enhanced scheduler with new features
    scheduler = CarbonDelayScheduler(
        carbon_api=carbon_api,
        max_delay_seconds=1800,
        threshold_gco2_per_kwh=200.0,
        persistence_file="test_queue.json",
        modp_weights={"carbon": 0.5, "latency": 0.2, "energy": 0.2, "cost": 0.1},
        causal_bandit=CausalBandit(),
        safety_monitor=SafetyMonitor(),
        xai_explainer=XAIExplainer(),
        federated_coordinator=FederatedCoordinator(),
        multi_agent_coordinator=MultiAgentCoordinator(),
        chaos_monkey=ChaosMonkey(enabled=False),
        human_review=HumanReviewManager(),
    )

    # Simulate tasks
    tasks = [
        {"id": "task1", "priority": "normal", "latency_sensitivity": 0.5, "idle_power_watts": 10, "type": "inference"},
        {"id": "task2", "priority": "high", "latency_sensitivity": 0.9},
        {"id": "task3", "priority": "normal", "latency_sensitivity": 0.3, "idle_power_watts": 5, "type": "inference"},
    ]

    for task in tasks:
        result = scheduler.submit(task)
        print(f"Task {task['id']}: {result['status']} (reason: {result.get('reason', 'N/A')})")
        if 'explanation' in result:
            print(f"  Explanation: {result['explanation']}")

    # Simulate tick after some time
    time.sleep(2)
    released = scheduler.tick()
    print(f"Released {len(released)} tasks")

    # Report a sample reward to test adaptation and causal bandit
    scheduler.report_reward({"id": "sample"}, 5.0, action="delay")

    # Show stats
    print(scheduler.get_queue_stats())
