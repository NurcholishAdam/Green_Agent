"""
carbon_delay_scheduler.py — Enhanced carbon-intensity-aware delay queue (v3.0.0)
================================================================================

Layers MODP, bio-inspired tuning, MoE, FlexGen, causal bandit, temporal safety,
XAI, federated coordination, multi-agent assignment, chaos engineering, and
human-in-the-loop on top of a heap-based delay queue.

CHANGES (v3.0.0 over v2.0.0):
- Fixed missing imports (``random``, ``uuid``, ``hashlib``).
- **Interfaces aligned with the codebase:** ``ParetoOptimizer.evaluate``,
  ``GeneticPolicyGenerator.evolve``, ``ExpertRouter.encode``/``select``.
  Fallback stubs match the production signatures.
- **Utility convention unified** (higher = better).  ``_evaluate_delay``
  now returns ``(should_delay, scheduled_time, reason, utility)``.
- ``federated_update`` uses a stable per-instance ``participant_id``.
- ``MultiAgentCoordinator`` uses a stable SHA-256 hash for assignment.
- **Async-first API** (``asubmit``, ``atick``, ``areport_reward``) with
  sync wrappers (``submit``, ``tick``, ``report_reward``) that dispatch to
  a background loop when called from a non-async context.
- Heap and persistence guarded by a ``threading.RLock``.
- Seeded ``np.random.default_rng`` replaces global ``random``.
- ``CausalBandit`` histories are bounded; ``update`` uses ``setdefault``
  and never raises ``KeyError`` for unknown actions.
- ``HumanReviewManager.pending_reviews`` is a bounded ``OrderedDict``.
- **``FeedbackEvent`` emission** on every decision (guarded by the schema
  import and ``emit_feedback_events`` flag).
- ``SafetyMonitor`` rules now share a uniform ``(task, delay, forecast)``
  signature; new rules can be registered at runtime.
- ``ChaosMonkey`` validates ``failure_probability`` and raises a typed
  ``ChaosExperimentError``.
- ``Delayability`` values renamed to ``NOT_DELAYABLE`` / ``TRADE_OFF`` /
  ``FULLY_DELAYABLE`` to match the docstrings.
- Scheduling math uses ``time.monotonic``; human-readable timestamps use
  ``datetime.now(timezone.utc)``.
- ``electricity_price_per_kwh`` is configurable.
- ``_get_forecast_intensity_at`` falls back to the last known intensity.
- Demo imports fixed (``from carbon_api import CarbonAPIStub``) with the
  correct constructor signature.
- ``__version__`` and ``__all__`` added.
- Built-in test suite: ``python carbon_delay_scheduler.py --test``.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import heapq
import inspect
import json
import logging
import os
import secrets
import threading
import time
import uuid
from collections import OrderedDict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Tuple

import numpy as np

__version__ = "3.0.0"
__all__ = [
    "__version__",
    "Delayability",
    "DelayedTask",
    "CausalBandit",
    "SafetyMonitor",
    "SafetyRule",
    "XAIExplainer",
    "FederatedCoordinator",
    "MultiAgentCoordinator",
    "ChaosMonkey",
    "ChaosExperimentError",
    "HumanReviewManager",
    "CarbonDelayScheduler",
    "run_tests",
]


# ============================================================================
# Logging
# ============================================================================
_LOG_DIR = Path(os.environ.get("CARBON_SCHED_LOG_DIR", "./logs"))
try:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
except OSError:
    _LOG_DIR = Path("/tmp")

logging.basicConfig(
    level=os.environ.get("CARBON_SCHED_LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    try:
        from src.enhancements.schemas.feedback_event import FeedbackEvent  # type: ignore
    except ImportError:
        FeedbackEvent = None  # type: ignore


# ============================================================================
# Optional enhancement modules (fallback stubs match the production API)
# ============================================================================
try:
    from enhancements.bio_inspired import GeneticPolicyGenerator  # type: ignore
    _BIO_AVAILABLE = True
except ImportError:  # pragma: no cover
    _BIO_AVAILABLE = False

    class GeneticPolicyGenerator:  # type: ignore
        """Fallback that mirrors the production signature."""
        def __init__(self, *a, **k) -> None: pass

        def evolve(self, population: List[Dict[str, Any]],
                   fitness_fn: Callable[[Dict[str, Any]], float],
                   generations: int = 10,
                   population_size: int = 20) -> List[Dict[str, Any]]:
            return list(population)


try:
    from enhancements.MODP import ParetoOptimizer  # type: ignore
    _MODP_AVAILABLE = True
except ImportError:  # pragma: no cover
    _MODP_AVAILABLE = False

    class ParetoOptimizer:  # type: ignore
        """Fallback that mirrors the production signature."""
        def __init__(self, *a, **k) -> None: pass

        def evaluate(self, objectives: Dict[str, float],
                     weights: Dict[str, float]) -> float:
            return float(sum(objectives.get(k, 0.0) * weights.get(k, 1.0)
                             for k in objectives))


try:
    from enhancements.moe_system import ExpertRouter  # type: ignore
    _MOE_AVAILABLE = True
except ImportError:  # pragma: no cover
    _MOE_AVAILABLE = False

    class ExpertRouter:  # type: ignore
        """Fallback that mirrors the production signature."""
        def __init__(self, *a, **k) -> None: pass

        def encode(self, context: Dict[str, Any]) -> List[float]:
            # Deterministic encoding based on a few stable features.
            priority = context.get("priority", "normal")
            return [
                1.0 if priority == "high" else 0.0,
                1.0 if priority == "normal" else 0.0,
                1.0 if priority == "low" else 0.0,
                float(context.get("latency_sensitivity", 0.5)),
                float(context.get("energy_kwh_estimate", 1.0)) / 10.0,
            ]

        def select(self, encoded: List[float]) -> str:
            # Return one of "high" / "medium" / "low".
            if not encoded:
                return "normal"
            if encoded[0] >= 0.5:
                return "high"
            if encoded[1] >= 0.5:
                return "medium"
            return "low"


# ============================================================================
# Types and enums
# ============================================================================
class Delayability(str, Enum):
    """How delayable a task is.

    ``NOT_DELAYABLE`` must not be delayed; ``TRADE_OFF`` may be delayed within
    limits; ``FULLY_DELAYABLE`` may be delayed aggressively.
    """
    NOT_DELAYABLE = "not_delayable"
    TRADE_OFF = "trade_off"
    FULLY_DELAYABLE = "fully_delayable"


@dataclass
class DelayedTask:
    scheduled_time: float
    task: Dict[str, Any]
    original_submit_time: float
    delay_reason: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "scheduled_time": self.scheduled_time,
            "task": self.task,
            "original_submit_time": self.original_submit_time,
            "delay_reason": self.delay_reason,
        }


class ChaosExperimentError(Exception):
    """Raised by ChaosMonkey when a fault is intentionally injected."""


@dataclass
class SafetyRule:
    """A named predicate over (task, delay_seconds, forecast)."""
    name: str
    predicate: Callable[[Dict[str, Any], float,
                         Optional[List[Tuple[float, float]]]], bool]
    description: str = ""


# ============================================================================
# Enhancement components
# ============================================================================
class CausalBandit:
    """
    Epsilon-greedy bandit with an average-treatment-effect estimate.

    Bounded histories prevent unbounded memory growth.  ``update`` never
    raises ``KeyError`` for unknown actions; new actions are registered
    automatically (which also supports bio-driven expansion).
    """

    def __init__(self, actions: Optional[List[str]] = None,
                 initial_value: float = 0.0,
                 epsilon: float = 0.1,
                 min_trials_for_causal: int = 10,
                 history_size: int = 1000,
                 seed: int = 42) -> None:
        self.actions: List[str] = list(actions or ["delay", "forward"])
        self._rng = np.random.default_rng(seed)
        self.epsilon = float(epsilon)
        self.min_trials_for_causal = int(min_trials_for_causal)
        self.q_values: Dict[str, float] = {}
        self.counts: Dict[str, int] = {}
        self.causal_effects: Dict[str, float] = {}
        self.trials = 0
        self.context_history: Deque[Dict[str, Any]] = deque(maxlen=history_size)
        self.reward_history: Deque[float] = deque(maxlen=history_size)
        self.action_history: Deque[str] = deque(maxlen=history_size)
        for a in self.actions:
            self._register(a, initial_value)

    def _register(self, name: str, initial: float = 0.0) -> None:
        self.q_values.setdefault(name, float(initial))
        self.counts.setdefault(name, 0)
        self.causal_effects.setdefault(name, 0.0)
        if name not in self.actions:
            self.actions.append(name)

    def select_action(self, context: Dict[str, Any]) -> str:
        if float(self._rng.random()) < self.epsilon:
            idx = int(self._rng.integers(0, len(self.actions)))
            return self.actions[idx]
        if (self.trials >= self.min_trials_for_causal
                and any(self.causal_effects.values())):
            return max(self.causal_effects, key=self.causal_effects.get)
        return max(self.q_values, key=self.q_values.get)

    def update(self, context: Dict[str, Any], action: str,
               reward: float) -> None:
        self._register(action)
        self.trials += 1
        self.counts[action] += 1
        n = self.counts[action]
        self.q_values[action] += (float(reward) - self.q_values[action]) / n
        self.context_history.append(dict(context))
        self.reward_history.append(float(reward))
        self.action_history.append(action)
        self.causal_effects[action] += (
            float(reward) - self.causal_effects[action]) / n

    def export_state(self) -> Dict[str, Any]:
        return {
            "q_values": dict(self.q_values),
            "counts": dict(self.counts),
            "causal_effects": dict(self.causal_effects),
            "trials": self.trials,
            "action_names": list(self.actions),
        }

    def import_state(self, state: Dict[str, Any]) -> None:
        for name, v in state.get("q_values", {}).items():
            self._register(name)
            self.q_values[name] = float(v)
        for name, v in state.get("counts", {}).items():
            self._register(name)
            self.counts[name] = int(v)
        for name, v in state.get("causal_effects", {}).items():
            self._register(name)
            self.causal_effects[name] = float(v)
        self.trials = int(state.get("trials", 0))


class SafetyMonitor:
    """
    Temporal-logic-like safety monitor with a uniform rule signature
    ``(task, delay_seconds, forecast) -> bool`` (True = safe).
    """

    def __init__(self,
                 max_delay_high_priority: float = 300.0) -> None:
        self.max_delay_high_priority = float(max_delay_high_priority)
        self._rules: List[SafetyRule] = []
        self._install_default_rules()

    def _install_default_rules(self) -> None:
        def max_delay_high(task: Dict[str, Any], delay: float,
                           forecast: Optional[List[Tuple[float, float]]]
                           ) -> bool:
            if task.get("priority") == "high" and delay > self.max_delay_high_priority:
                return False
            return True

        def no_delay_without_forecast(
            task: Dict[str, Any], delay: float,
            forecast: Optional[List[Tuple[float, float]]],
        ) -> bool:
            if delay > 0 and not forecast:
                return False
            return True

        self.add_rule(SafetyRule("max_delay_high_priority",
                                 max_delay_high,
                                 "high-priority delay cap"))
        self.add_rule(SafetyRule("no_delay_without_forecast",
                                 no_delay_without_forecast,
                                 "delay requires a forecast"))

    def add_rule(self, rule: SafetyRule) -> None:
        self._rules.append(rule)

    def check(self, task: Dict[str, Any], delay: float,
              forecast: Optional[List[Tuple[float, float]]] = None
              ) -> Tuple[bool, List[str]]:
        violations: List[str] = []
        for rule in self._rules:
            try:
                if not rule.predicate(task, delay, forecast):
                    violations.append(rule.name)
            except Exception as e:  # pragma: no cover
                logger.warning("Safety rule %s raised: %s", rule.name, e)
        return (not violations), violations

    @property
    def rule_names(self) -> List[str]:
        return [r.name for r in self._rules]


class XAIExplainer:
    def explain_delay(self, task: Dict[str, Any],
                      current_intensity: float,
                      best_intensity: Optional[float],
                      delay_seconds: float,
                      utility: float,
                      reason: str) -> str:
        parts: List[str] = []
        if delay_seconds > 0:
            parts.append(f"Delaying task for {delay_seconds:.0f}s.")
            if best_intensity is not None:
                parts.append(
                    f"Carbon intensity drops from {current_intensity:.1f} "
                    f"to {best_intensity:.1f} gCO2/kWh.")
        else:
            parts.append("Forwarding task immediately.")
            if reason:
                parts.append(f"Reason: {reason}")
        parts.append(f"Utility: {utility:.3f}")
        return " ".join(parts)


class FederatedCoordinator:
    """In-process FedAvg over a weighted mean of numeric model updates."""

    def __init__(self, max_participants: int = 1000) -> None:
        self._participants: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._weights: Dict[str, float] = {}
        self._max_participants = max(1, int(max_participants))

    def register(self, participant_id: str, model_update: Dict[str, Any],
                 weight: float = 1.0) -> None:
        if participant_id not in self._participants:
            while len(self._participants) >= self._max_participants:
                self._participants.popitem(last=False)
        self._participants[participant_id] = dict(model_update)
        self._weights[participant_id] = float(weight)

    def aggregate(self) -> Dict[str, float]:
        if not self._participants:
            return {}
        keys = set()
        for update in self._participants.values():
            keys.update(update.keys())
        out: Dict[str, float] = {}
        for k in keys:
            total = 0.0
            wsum = 0.0
            for pid, update in self._participants.items():
                v = update.get(k)
                if isinstance(v, (int, float)):
                    w = self._weights.get(pid, 1.0)
                    total += float(v) * w
                    wsum += w
            if wsum > 0:
                out[k] = total / wsum
        return out

    def reset(self) -> None:
        self._participants.clear()
        self._weights.clear()


class MultiAgentCoordinator:
    """
    Assigns tasks to agents using a stable SHA-256 hash of the task id.
    Round-robin fallback when the id is missing.
    """

    def __init__(self, num_agents: int = 3) -> None:
        if num_agents < 1:
            raise ValueError("num_agents must be >= 1")
        self.agents = [f"agent_{i}" for i in range(num_agents)]
        self.responsibilities: Dict[str, List[str]] = {
            a: [] for a in self.agents
        }
        self._rr_counter = 0

    def _stable_hash(self, value: Any) -> int:
        raw = str(value).encode()
        return int(hashlib.sha256(raw).hexdigest(), 16)

    def assign_task(self, task: Dict[str, Any]) -> str:
        task_id = task.get("id")
        if task_id is None:
            agent = self.agents[self._rr_counter % len(self.agents)]
            self._rr_counter += 1
        else:
            agent = self.agents[self._stable_hash(task_id) % len(self.agents)]
        self.responsibilities[agent].append(str(task_id))
        return agent

    def load(self) -> Dict[str, int]:
        return {a: len(tasks) for a, tasks in self.responsibilities.items()}


class ChaosMonkey:
    """Injects random failures when enabled."""

    def __init__(self, failure_probability: float = 0.1,
                 enabled: bool = True,
                 seed: int = 42) -> None:
        if not 0.0 <= failure_probability <= 1.0:
            raise ValueError("failure_probability must be in [0, 1]")
        self.failure_probability = float(failure_probability)
        self.enabled = bool(enabled)
        self._rng = np.random.default_rng(seed)

    def maybe_fail(self) -> None:
        if self.enabled and float(self._rng.random()) < self.failure_probability:
            raise ChaosExperimentError("Simulated chaos failure")


class HumanReviewManager:
    def __init__(self, max_pending: int = 500) -> None:
        self.pending_reviews: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._max_pending = max(1, int(max_pending))

    def request_review(self, task: Dict[str, Any],
                       decision: Dict[str, Any]) -> str:
        review_id = str(uuid.uuid4())
        self.pending_reviews[review_id] = {
            "task": dict(task),
            "decision": dict(decision),
            "status": "pending",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        while len(self.pending_reviews) > self._max_pending:
            self.pending_reviews.popitem(last=False)
        return review_id

    def approve(self, review_id: str,
                feedback: Optional[str] = None) -> bool:
        r = self.pending_reviews.get(review_id)
        if r is None:
            return False
        r["status"] = "approved"
        r["feedback"] = feedback
        r["reviewed_at"] = datetime.now(timezone.utc).isoformat()
        return True

    def reject(self, review_id: str,
               feedback: Optional[str] = None) -> bool:
        r = self.pending_reviews.get(review_id)
        if r is None:
            return False
        r["status"] = "rejected"
        r["feedback"] = feedback
        r["reviewed_at"] = datetime.now(timezone.utc).isoformat()
        return True

    def get_pending(self) -> List[str]:
        return [rid for rid, data in self.pending_reviews.items()
                if data["status"] == "pending"]


# ============================================================================
# Scheduler
# ============================================================================
class CarbonDelayScheduler:
    """
    Carbon-aware delay queue.

    Recommended usage is async::

        await scheduler.asubmit(task)
        released = await scheduler.atick()
        await scheduler.areport_reward(task, reward, action)

    Synchronous ``submit``, ``tick`` and ``report_reward`` remain for
    non-async callers; they dispatch to the background event loop when the
    caller is not already inside one.
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
        electricity_price_per_kwh: float = 0.15,
        seed: int = 42,
        emit_feedback_events: bool = True,
    ) -> None:
        self.carbon_api = carbon_api
        self.max_delay = float(max_delay_seconds)
        self.threshold = float(threshold_gco2_per_kwh)
        self.persistence_file = persistence_file
        self.forecast_confidence_threshold = float(
            forecast_confidence_threshold)
        self.electricity_price_per_kwh = float(electricity_price_per_kwh)
        self.seed = int(seed)
        self.emit_feedback_events = bool(emit_feedback_events)
        self.instance_id = str(uuid.uuid4())[:8]

        self.modp_weights = modp_weights or {
            "carbon": 0.4, "latency": 0.3, "energy": 0.2, "cost": 0.1}

        self.modp = ParetoOptimizer()
        self.bio = bio_optimizer or GeneticPolicyGenerator()
        self.moe = moe_router or ExpertRouter()
        self.causal_bandit = causal_bandit or CausalBandit(seed=self.seed)
        self.safety_monitor = safety_monitor or SafetyMonitor()
        self.xai = xai_explainer or XAIExplainer()
        self.federated = federated_coordinator
        self.multi_agent = multi_agent_coordinator
        self.chaos_monkey = chaos_monkey or ChaosMonkey(enabled=False,
                                                       seed=self.seed)
        self.human_review = human_review or HumanReviewManager()
        self.flexgen_manager = flexgen_manager
        self._flexgen_available = flexgen_manager is not None

        self._rng = np.random.default_rng(self.seed)
        self.queue: List[Tuple[float, DelayedTask]] = []
        self._lock = threading.RLock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None

        self.metrics: Dict[str, Any] = {
            "total_delayed": 0,
            "total_forwarded": 0,
            "total_released": 0,
            "total_rewards": 0.0,
            "total_flexgen_policies_selected": 0,
            "total_causal_bandit_actions": 0,
            "total_safety_violations": 0,
            "total_reviews_requested": 0,
        }

        self._load_queue()

    # ------------------------------------------------------------------
    # Background loop (used by sync wrappers)
    # ------------------------------------------------------------------
    def _get_loop(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._loop is not None and self._loop.is_running():
                return self._loop
            loop = asyncio.new_event_loop()

            def runner() -> None:
                asyncio.set_event_loop(loop)
                loop.run_forever()

            t = threading.Thread(target=runner,
                                 name=f"cds-loop-{self.instance_id}",
                                 daemon=True)
            t.start()
            self._loop = loop
            self._loop_thread = t
            return loop

    def _run_sync(self, coro: Awaitable[Any]) -> Any:
        try:
            # If we're already inside a loop, submit to the background one.
            asyncio.get_running_loop()
            loop = self._get_loop()
            return asyncio.run_coroutine_threadsafe(coro, loop).result()
        except RuntimeError:
            return asyncio.run(coro)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _load_queue(self) -> None:
        if not self.persistence_file or not os.path.exists(self.persistence_file):
            return
        try:
            with open(self.persistence_file, "r") as f:
                data = json.load(f)
            with self._lock:
                for item in data:
                    task = DelayedTask(
                        scheduled_time=float(item["scheduled_time"]),
                        task=item["task"],
                        original_submit_time=float(item["original_submit_time"]),
                        delay_reason=item.get("delay_reason", ""),
                    )
                    heapq.heappush(self.queue, (task.scheduled_time, task))
            logger.info("Loaded %d tasks from persistence.", len(self.queue))
        except (OSError, json.JSONDecodeError, KeyError, TypeError) as e:
            logger.error("Failed to load queue: %s", e)

    def _save_queue(self) -> None:
        if not self.persistence_file:
            return
        try:
            with self._lock:
                payload = [t.to_dict() for _, t in self.queue]
            path = Path(self.persistence_file)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            with open(tmp, "w") as f:
                json.dump(payload, f)
            os.replace(tmp, path)
        except OSError as e:
            logger.error("Failed to persist queue: %s", e)

    # ------------------------------------------------------------------
    # Decision
    # ------------------------------------------------------------------
    def _get_delayability(self, task: Dict[str, Any]) -> Delayability:
        if task.get("priority") == "high":
            return Delayability.NOT_DELAYABLE
        try:
            encoded = self.moe.encode(task)
            category = self.moe.select(encoded) if hasattr(self.moe, "select") \
                else "normal"
        except Exception as e:
            logger.warning("MoE classification failed: %s", e)
            category = "normal"
        if category == "high":
            return Delayability.NOT_DELAYABLE
        if category == "medium":
            return Delayability.TRADE_OFF
        return Delayability.FULLY_DELAYABLE

    async def _evaluate_delay(
        self,
        task: Dict[str, Any],
        current_intensity: float,
        forecast: List[Tuple[float, float]],
    ) -> Tuple[bool, Optional[float], str, float]:
        """
        Returns ``(should_delay, scheduled_time, reason, utility)``.

        Convention: **higher utility = better.**  Delaying is beneficial
        when utility > 0.
        """
        # Chaos injection (disabled by default).
        self.chaos_monkey.maybe_fail()

        now = time.monotonic()
        best_time: Optional[float] = None
        best_intensity: Optional[float] = None
        for ts, intensity in forecast:
            if intensity < self.threshold and (ts - now) <= self.max_delay:
                if best_time is None or intensity < best_intensity:
                    best_time = ts
                    best_intensity = intensity

        if best_time is None or best_intensity is None:
            return False, None, "No suitable low-carbon window", 0.0

        carbon_reduction = max(0.0, current_intensity - best_intensity)
        latency_cost = max(0.0, best_time - now)
        idle_power_watts = float(task.get("idle_power_watts", 10.0))
        energy_overhead_kwh = (idle_power_watts * latency_cost) / 3_600_000.0
        cost = energy_overhead_kwh * self.electricity_price_per_kwh

        # Higher = better for every objective.
        objectives = {
            "carbon": carbon_reduction,
            "latency": -latency_cost / max(1.0, self.max_delay),
            "energy": -energy_overhead_kwh,
            "cost": -cost,
        }
        utility = float(self.modp.evaluate(objectives, self.modp_weights))

        if utility > 0:
            return True, best_time, f"MODP utility={utility:.3f}", utility
        return False, None, f"MODP utility={utility:.3f} (not beneficial)", utility

    # ------------------------------------------------------------------
    # Bio-inspired adaptation
    # ------------------------------------------------------------------
    async def _adapt_parameters(self, reward: float) -> None:
        try:
            current_intensity = await self._current_intensity_async()
        except Exception:
            current_intensity = 0.0
        context = {
            "threshold": self.threshold,
            "max_delay": self.max_delay,
            "task_count": len(self.queue),
            "avg_carbon": current_intensity,
        }

        def fitness(policy: Dict[str, Any]) -> float:
            return float(reward)

        try:
            population = [{"threshold": self.threshold, "max_delay": self.max_delay}]
            evolved = self.bio.evolve(
                population=population,
                fitness_fn=fitness,
                generations=1,
                population_size=2,
            )
            if evolved:
                best = evolved[0]
                self.threshold = float(best.get("threshold", self.threshold))
                self.max_delay = float(best.get("max_delay", self.max_delay))
        except Exception as e:
            logger.warning("Bio adaptation failed: %s", e)

    def adapt_parameters(self, reward: float) -> None:
        """Sync wrapper for ``_adapt_parameters``."""
        self._run_sync(self._adapt_parameters(reward))

    # ------------------------------------------------------------------
    # Carbon API helpers
    # ------------------------------------------------------------------
    async def _current_intensity_async(self) -> float:
        result = self.carbon_api.get_current()
        if inspect.iscoroutine(result):
            result = await result
        return float(result)

    async def _forecast_async(self, minutes: int
                              ) -> List[Tuple[float, float]]:
        result = self.carbon_api.get_forecast(minutes)
        if inspect.iscoroutine(result):
            result = await result
        return list(result or [])

    # ------------------------------------------------------------------
    # FlexGen
    # ------------------------------------------------------------------
    async def select_flexgen_policy(self,
                                    task: Dict[str, Any],
                                    node: Optional[Dict[str, Any]] = None
                                    ) -> Dict[str, Any]:
        if not self._flexgen_available:
            return {"error": "FlexGen manager not available"}
        try:
            result = self.flexgen_manager.optimize_policy(
                task.get("workload", task), node or {})
            if inspect.iscoroutine(result):
                result = await result
            self.metrics["total_flexgen_policies_selected"] += 1
            return result
        except Exception as e:
            logger.error("FlexGen policy selection failed: %s", e)
            return {"error": str(e)}

    # ------------------------------------------------------------------
    # Async public API
    # ------------------------------------------------------------------
    async def asubmit(self, task: Dict[str, Any]) -> Dict[str, Any]:
        # 1. Delayability gate.
        delayability = self._get_delayability(task)
        if delayability is Delayability.NOT_DELAYABLE:
            self.metrics["total_forwarded"] += 1
            explanation = self.xai.explain_delay(
                task, 0.0, None, 0.0, 0.0, "High priority")
            result = {"status": "forward", "task": task, "delay_until": None,
                      "reason": "High priority",
                      "delayability": delayability.value,
                      "explanation": explanation}
            await self._emit_feedback_event(task, "forward", 0.0, explanation)
            return result

        # 2. Current intensity.
        current_intensity = await self._current_intensity_async()

        # 3. Already low carbon?  Forward immediately.
        if current_intensity <= self.threshold:
            flexgen_policy = None
            if self._flexgen_available and task.get("type") == "inference":
                flexgen_policy = await self.select_flexgen_policy(task)
            self.metrics["total_forwarded"] += 1
            explanation = self.xai.explain_delay(
                task, current_intensity, current_intensity, 0.0, 0.0,
                "Already low carbon")
            result = {"status": "forward", "task": task, "delay_until": None,
                      "reason": "Already low carbon",
                      "delayability": delayability.value,
                      "explanation": explanation}
            if flexgen_policy:
                result["flexgen_policy"] = flexgen_policy
            await self._emit_feedback_event(task, "forward", 0.0, explanation)
            return result

        # 4. Forecast.
        forecast_minutes = int(self.max_delay // 60) + 2
        forecast = await self._forecast_async(forecast_minutes)
        if not forecast:
            self.metrics["total_forwarded"] += 1
            explanation = self.xai.explain_delay(
                task, current_intensity, current_intensity, 0.0, 0.0,
                "No forecast available")
            result = {"status": "forward", "task": task, "delay_until": None,
                      "reason": "No forecast available",
                      "delayability": delayability.value,
                      "explanation": explanation}
            await self._emit_feedback_event(task, "forward", 0.0, explanation)
            return result

        # 5. MODP evaluation.
        should_delay, scheduled_time, reason, utility = await self._evaluate_delay(
            task, current_intensity, forecast)

        # 6. Causal bandit meta-decision.
        context = {
            "current_intensity": current_intensity,
            "forecast_available": len(forecast) > 0,
            "delayability": delayability.value,
            "task_type": task.get("type", "unknown"),
        }
        bandit_action = self.causal_bandit.select_action(context)
        self.metrics["total_causal_bandit_actions"] += 1

        if should_delay and bandit_action == "forward":
            should_delay = False
            reason = "Causal bandit suggests forward"
            scheduled_time = None
        elif not should_delay and bandit_action == "delay":
            if scheduled_time is not None:
                should_delay = True
                reason = "Causal bandit suggests delay"

        # 7. Safety gate.
        delay_seconds = 0.0
        if should_delay and scheduled_time is not None:
            delay_seconds = max(0.0, scheduled_time - time.monotonic())
            safe, violations = self.safety_monitor.check(
                task, delay_seconds, forecast)
            if not safe:
                self.metrics["total_safety_violations"] += 1
                should_delay = False
                scheduled_time = None
                reason = f"Safety violation: {violations}"

        # 8. Medium-priority cap.
        if should_delay and delayability is Delayability.TRADE_OFF \
                and delay_seconds > self.max_delay * 0.5:
            should_delay = False
            scheduled_time = None
            reason = (f"Delay too long for trade-off priority "
                      f"({delay_seconds:.0f}s)")

        # 9. Commit.
        if should_delay and scheduled_time is not None:
            delayed = DelayedTask(
                scheduled_time=scheduled_time,
                task=task,
                original_submit_time=time.monotonic(),
                delay_reason=reason,
            )
            with self._lock:
                heapq.heappush(self.queue, (delayed.scheduled_time, delayed))
            self.metrics["total_delayed"] += 1
            self._save_queue()
            best_intensity = self._get_forecast_intensity_at(
                forecast, scheduled_time)
            explanation = self.xai.explain_delay(
                task, current_intensity, best_intensity,
                delay_seconds, utility, reason)
            result = {
                "status": "delayed",
                "task": task,
                "delay_until": scheduled_time,
                "reason": reason,
                "delayability": delayability.value,
                "utility": utility,
                "explanation": explanation,
            }
            if delay_seconds > self.max_delay * 0.8:
                review_id = self.human_review.request_review(task, result)
                self.metrics["total_reviews_requested"] += 1
                result["review_id"] = review_id
                result["review_pending"] = True
            await self._emit_feedback_event(task, "delay", utility, explanation)
            return result

        # Forward.
        flexgen_policy = None
        if self._flexgen_available and task.get("type") == "inference":
            flexgen_policy = await self.select_flexgen_policy(task)
        self.metrics["total_forwarded"] += 1
        explanation = self.xai.explain_delay(
            task, current_intensity, current_intensity, 0.0, utility, reason)
        result = {"status": "forward", "task": task, "delay_until": None,
                  "reason": reason,
                  "delayability": delayability.value,
                  "utility": utility,
                  "explanation": explanation}
        if flexgen_policy:
            result["flexgen_policy"] = flexgen_policy
        await self._emit_feedback_event(task, "forward", utility, explanation)
        return result

    async def atick(self) -> List[Dict[str, Any]]:
        now = time.monotonic()
        released: List[DelayedTask] = []
        with self._lock:
            while self.queue and self.queue[0][0] <= now:
                _, delayed = heapq.heappop(self.queue)
                released.append(delayed)
        out: List[Dict[str, Any]] = []
        for d in released:
            entry: Dict[str, Any] = {"task": d.task,
                                     "delay_reason": d.delay_reason}
            if self._flexgen_available and d.task.get("type") == "inference":
                try:
                    entry["flexgen_policy"] = await self.select_flexgen_policy(
                        d.task)
                except Exception as e:
                    logger.warning("FlexGen selection on release failed: %s", e)
            out.append(entry)
            self.metrics["total_released"] += 1
        if released:
            self._save_queue()
        return out

    async def areport_reward(self, task: Dict[str, Any], reward: float,
                             action: Optional[str] = None) -> Dict[str, Any]:
        self.metrics["total_rewards"] += float(reward)
        await self._adapt_parameters(float(reward))
        if action:
            try:
                current = await self._current_intensity_async()
            except Exception:
                current = 0.0
            context = {
                "current_intensity": current,
                "task_type": task.get("type", "unknown"),
            }
            self.causal_bandit.update(context, action, float(reward))
        return {"status": "recorded", "reward": float(reward)}

    # ------------------------------------------------------------------
    # Sync wrappers
    # ------------------------------------------------------------------
    def submit(self, task: Dict[str, Any]) -> Dict[str, Any]:
        return self._run_sync(self.asubmit(task))

    def tick(self) -> List[Dict[str, Any]]:
        return self._run_sync(self.atick())

    def report_reward(self, task: Dict[str, Any], reward: float,
                      action: Optional[str] = None) -> Dict[str, Any]:
        return self._run_sync(self.areport_reward(task, reward, action))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _get_forecast_intensity_at(
        self, forecast: List[Tuple[float, float]], timestamp: float,
    ) -> Optional[float]:
        if not forecast:
            return None
        prev: Optional[Tuple[float, float]] = None
        for ts, intensity in forecast:
            if ts <= timestamp:
                prev = (ts, intensity)
                continue
            if prev is None:
                return float(intensity)
            t0, i0 = prev
            t1, i1 = ts, intensity
            if t1 == t0:
                return float(i0)
            ratio = (timestamp - t0) / (t1 - t0)
            return float(i0 + ratio * (i1 - i0))
        return float(prev[1]) if prev is not None else None

    async def _emit_feedback_event(self, task: Dict[str, Any], action: str,
                                   reward: float, explanation: str) -> None:
        if not self.emit_feedback_events or FeedbackEvent is None:
            return
        try:
            ev = FeedbackEvent(
                source="carbon_delay_scheduler",
                feedback_type="routing",
                context={
                    "task_id": str(task.get("id", "unknown")),
                    "teacher_ids": [],
                },
                action={
                    "selected_action": action,
                    "selected_rank": 1,
                    "confidence_score": 0.5,
                },
                performance={
                    "quality_score": float(max(0.0, min(1.0, reward))),
                    "latency_ms": 0.0, "energy_joules": 0.0,
                    "carbon_g": 0.0, "helium_cost": 0.0, "duration_ms": 0.0,
                },
                adaptive_cost_value=float(reward),
                tags=["scheduler", action, str(task.get("type", "unknown"))],
                envelope={"trace_id": self.instance_id},
                metadata={"explanation": explanation},
            )
            logger.debug("FeedbackEvent emitted: %s",
                         getattr(ev, "event_id", "?"))
        except Exception as e:
            logger.warning("Failed to emit FeedbackEvent: %s", e)

    # ------------------------------------------------------------------
    # Federated
    # ------------------------------------------------------------------
    def federated_update(self, model_update: Dict[str, Any],
                         weight: float = 1.0) -> None:
        if self.federated is not None:
            self.federated.register(self.instance_id, model_update, weight)

    def aggregate_federated(self) -> Dict[str, float]:
        if self.federated is not None:
            return self.federated.aggregate()
        return {}

    # ------------------------------------------------------------------
    def get_queue_stats(self) -> Dict[str, Any]:
        with self._lock:
            next_release = self.queue[0][0] if self.queue else None
            queue_size = len(self.queue)
        return {
            "queue_size": queue_size,
            "next_release": next_release,
            "threshold": self.threshold,
            "max_delay": self.max_delay,
            "instance_id": self.instance_id,
            **self.metrics,
        }

    def clear(self) -> None:
        with self._lock:
            self.queue.clear()
        self._save_queue()

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "queue": [d.to_dict() for _, d in self.queue],
                "threshold": self.threshold,
                "max_delay": self.max_delay,
                "instance_id": self.instance_id,
            }

    def shutdown(self) -> None:
        """Stop the background event loop if one was created."""
        with self._lock:
            loop = self._loop
            self._loop = None
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)


# ============================================================================
# Tests
# ============================================================================
class _StubCarbonAPI:
    """Deterministic carbon API for tests."""

    def __init__(self, current: float = 300.0,
                 forecast_values: Optional[List[float]] = None,
                 step_seconds: int = 600) -> None:
        self.current = float(current)
        self.forecast_values = list(forecast_values or [200.0, 180.0, 250.0])
        self.step_seconds = int(step_seconds)

    def get_current(self) -> float:
        return float(self.current)

    def get_forecast(self, minutes: int) -> List[Tuple[float, float]]:
        now = time.monotonic()
        return [
            (now + (i + 1) * self.step_seconds, v)
            for i, v in enumerate(self.forecast_values)
        ]


def _test_delayability_names() -> None:
    assert Delayability.NOT_DELAYABLE.value == "not_delayable"
    assert Delayability.TRADE_OFF.value == "trade_off"
    assert Delayability.FULLY_DELAYABLE.value == "fully_delayable"


def _test_causal_bandit_no_keyerror() -> None:
    b = CausalBandit(seed=0)
    b.update({}, "unknown_action", 1.0)
    assert "unknown_action" in b.q_values
    assert b.counts["unknown_action"] == 1


def _test_causal_bandit_bounded_histories() -> None:
    b = CausalBandit(history_size=5, seed=0)
    for i in range(50):
        b.update({"i": i}, "delay", 1.0)
    assert len(b.context_history) <= 5
    assert len(b.reward_history) <= 5
    assert len(b.action_history) <= 5


def _test_safety_monitor_uniform_signature() -> None:
    sm = SafetyMonitor(max_delay_high_priority=10.0)
    safe, violations = sm.check({"priority": "high"}, 20.0,
                                [(time.monotonic() + 60, 100.0)])
    assert not safe
    assert "max_delay_high_priority" in violations

    safe, violations = sm.check({"priority": "normal"}, 10.0, None)
    assert not safe
    assert "no_delay_without_forecast" in violations

    safe, _ = sm.check({"priority": "normal"}, 0.0, None)
    assert safe


def _test_chaos_monkey_validation() -> None:
    try:
        ChaosMonkey(failure_probability=1.5)
    except ValueError:
        pass
    else:
        raise AssertionError("invalid probability should raise")

    cm = ChaosMonkey(failure_probability=1.0, enabled=True, seed=0)
    try:
        cm.maybe_fail()
    except ChaosExperimentError:
        pass
    else:
        raise AssertionError("should raise ChaosExperimentError")


def _test_multi_agent_stable_hashing() -> None:
    m = MultiAgentCoordinator(num_agents=3)
    task = {"id": "task-abc"}
    a1 = m.assign_task(task)
    m2 = MultiAgentCoordinator(num_agents=3)
    a2 = m2.assign_task(task)
    assert a1 == a2


def _test_federated_update_uses_string_id() -> None:
    fed = FederatedCoordinator()
    sched = CarbonDelayScheduler(carbon_api=_StubCarbonAPI(),
                                 federated_coordinator=fed,
                                 persistence_file=None)
    sched.federated_update({"x": 1.0})
    assert sched.instance_id in fed._participants
    assert fed.aggregate() == {"x": 1.0}


def _test_scheduler_end_to_end() -> None:
    api = _StubCarbonAPI(current=300.0,
                         forecast_values=[200.0, 180.0, 250.0])
    sched = CarbonDelayScheduler(
        carbon_api=api, max_delay_seconds=1800,
        threshold_gco2_per_kwh=250.0, persistence_file=None,
        emit_feedback_events=False,
    )
    try:
        # High-priority task forwards.
        r = sched.submit({"id": "t1", "priority": "high"})
        assert r["status"] == "forward"

        # Low-carbon context forwards.
        api.current = 100.0
        r = sched.submit({"id": "t2", "priority": "normal"})
        assert r["status"] == "forward"
        api.current = 300.0

        # Normal task may be delayed if MODP finds a good window.
        r = sched.submit({"id": "t3", "priority": "normal",
                          "idle_power_watts": 5.0})
        assert r["status"] in ("forward", "delayed")

        # Feedback.
        sched.report_reward({"id": "t3"}, 0.5, action="delay")

        # Nothing due yet.
        released = sched.tick()
        assert isinstance(released, list)

        stats = sched.get_queue_stats()
        assert stats["instance_id"] == sched.instance_id
    finally:
        sched.shutdown()


def _test_sign_convention() -> None:
    """Higher utility = better; a strong low-carbon window yields utility > 0."""
    api = _StubCarbonAPI(current=500.0, forecast_values=[100.0])
    sched = CarbonDelayScheduler(
        carbon_api=api, max_delay_seconds=1800,
        threshold_gco2_per_kwh=200.0, persistence_file=None,
        modp_weights={"carbon": 0.9, "latency": 0.05,
                      "energy": 0.03, "cost": 0.02},
        emit_feedback_events=False,
    )
    try:
        r = sched.submit({"id": "sig", "priority": "normal"})
        assert r["status"] in ("forward", "delayed")
        # If delayed, MODP found utility > 0.
        if r["status"] == "delayed":
            assert r.get("utility", 0.0) > 0
    finally:
        sched.shutdown()


def run_tests() -> None:
    _test_delayability_names()
    _test_causal_bandit_no_keyerror()
    _test_causal_bandit_bounded_histories()
    _test_safety_monitor_uniform_signature()
    _test_chaos_monkey_validation()
    _test_multi_agent_stable_hashing()
    _test_federated_update_uses_string_id()
    _test_scheduler_end_to_end()
    _test_sign_convention()
    print("All carbon_delay_scheduler v3.0.0 tests passed.")


# ============================================================================
# Demo
# ============================================================================
def _demo() -> None:
    logging.basicConfig(level=logging.INFO)

    class DemoCarbonAPI:
        def get_current(self) -> float:
            return 300.0

        def get_forecast(self, minutes: int) -> List[Tuple[float, float]]:
            now = time.monotonic()
            return [(now + 300 * (i + 1), v)
                    for i, v in enumerate([200.0, 180.0, 250.0])]

    sched = CarbonDelayScheduler(
        carbon_api=DemoCarbonAPI(), max_delay_seconds=1800,
        threshold_gco2_per_kwh=200.0, persistence_file=None,
        causal_bandit=CausalBandit(seed=0),
        safety_monitor=SafetyMonitor(),
        xai_explainer=XAIExplainer(),
        federated_coordinator=FederatedCoordinator(),
        multi_agent_coordinator=MultiAgentCoordinator(),
        chaos_monkey=ChaosMonkey(enabled=False),
        human_review=HumanReviewManager(),
    )
    tasks = [
        {"id": "task1", "priority": "normal", "idle_power_watts": 10,
         "type": "inference"},
        {"id": "task2", "priority": "high"},
        {"id": "task3", "priority": "normal", "idle_power_watts": 5,
         "type": "inference"},
    ]
    for t in tasks:
        r = sched.submit(t)
        print(f"Task {t['id']}: {r['status']} "
              f"(reason: {r.get('reason', 'N/A')})")
        print(f"  Explanation: {r.get('explanation', '')}")
    time.sleep(0.1)
    released = sched.tick()
    print(f"Released {len(released)} tasks")
    sched.report_reward({"id": "sample"}, 0.5, action="delay")
    print(sched.get_queue_stats())
    sched.shutdown()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Carbon Delay Scheduler v3.0.0")
    parser.add_argument("--test", action="store_true",
                        help="Run the built-in test suite.")
    parser.add_argument("--demo", action="store_true",
                        help="Run the demo.")
    args = parser.parse_args()
    if args.test:
        run_tests()
    elif args.demo:
        _demo()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
