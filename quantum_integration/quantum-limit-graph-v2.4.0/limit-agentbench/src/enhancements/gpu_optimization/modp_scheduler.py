#!/usr/bin/env python3
"""
Enhanced MODP-based time-shifting scheduler (v2.5.0).
======================================================
Decides whether to run a workload now, defer, or move to another node.
Uses carbon forecasts, queue length, deadlines, and Q-learning to optimize.

FIXES OVER v2.0:
- Relative imports wrapped in try/except with fallbacks.
- `self.last_state` / `self.last_action` kept for backward compatibility,
  but a per-task context store allows concurrent decide/learn pairs to be
  associated correctly. `learn()` accepts optional `state`, `action`,
  `next_hours_to_deadline`, and `task_id` keyword args.
- Q-table serialization uses JSON lists for keys (robust against whitespace,
  trailing commas, and non-canonical tuple reprs). Legacy stringified-tuple
  keys are still read for backward compatibility.
- Bucket dimensions are capped on both ends (no unbounded state growth).
- Q-table writes are debounced and run in a worker thread with an atomic
  tmp+rename. `flush()` and `aclose()` are provided for shutdown.
- `epsilon`, `horizon`, and a format version are persisted with the Q-table.
- `learn()` uses the actual `hours_to_deadline` captured at decision time
  (was hardcoded to 24).
- `move_node` only selects a node whose carbon is meaningfully better than
  the current node; nodes with missing `region_carbon_intensity` are filtered.
- Reward range is consistent: `[-1.0, 1.0]`.
- Dead code removed (`async_lock` in `get_policy_stats`, unconditional queue
  penalty in `compute_reward`).
- `datetime.now(timezone.utc)` used; naive deadlines are coerced.
- Per-instance RNG (`np.random.default_rng(seed)`) — no global state touched.
- `FeedbackEvent` publishing is fire-and-forget and its errors are logged
  locally so a broken queue cannot break the decision.
- Constructor arguments validated.
- Optional Prometheus metrics.
- `aclose()` / `flush()` / `__aenter__` / `__aexit__` added.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple
from collections import deque

import numpy as np

# ---------- Prometheus (optional) ----------
try:
    from prometheus_client import Counter as _PromCounter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False

# ---------- structlog / stdlib logger ----------
try:
    import structlog
    _STRUCTLOG_LOGGER = structlog.get_logger(__name__)
    _STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    _STRUCTLOG_LOGGER = logging.getLogger(__name__)
    _STRUCTLOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)


# ---------- Preserved module-level logger ----------
try:
    from ..logger import logger  # type: ignore
except ImportError:  # pragma: no cover
    logger = logging.getLogger(__name__)


def log_event(level: str, message: str, **kwargs: Any) -> None:
    """Logging shim that works with structlog or stdlib logging."""
    if _STRUCTLOG_AVAILABLE:
        getattr(_STRUCTLOG_LOGGER, level)(message, **kwargs)
    else:
        if kwargs:
            extra = " ".join(f"{k}={v!r}" for k, v in kwargs.items())
            message = f"{message} | {extra}"
        getattr(logger, level)(message)


# ---------- Relative imports with fallbacks ----------
try:
    from ..data_integration.carbon_intensity import CarbonIntensityFetcher  # type: ignore
except ImportError:  # pragma: no cover
    CarbonIntensityFetcher = Any  # type: ignore

try:
    from ..async_message_queue import AsyncMessageQueue  # type: ignore
except ImportError:  # pragma: no cover
    AsyncMessageQueue = Any  # type: ignore

try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:  # pragma: no cover
    FeedbackEvent = Any  # type: ignore

try:
    from ..schemas.workload_descriptor import WorkloadDescriptor  # type: ignore
except ImportError:  # pragma: no cover
    WorkloadDescriptor = Any  # type: ignore

try:
    from ..schemas.node_descriptor import NodeDescriptor  # type: ignore
except ImportError:  # pragma: no cover
    NodeDescriptor = Any  # type: ignore


# ---------- Prometheus metrics (module scope: single registration) ----------
if PROMETHEUS_AVAILABLE:
    _M_DECISIONS = _PromCounter(
        "modp_decisions_total",
        "MODP decisions",
        ["action"],
    )
    _M_DEFER_HOURS = Histogram(
        "modp_defer_hours",
        "Deferral duration in hours",
    )
    _M_REWARD = Histogram(
        "modp_reward",
        "Rewards fed into the learner",
    )
    _M_EPSILON = Gauge(
        "modp_epsilon",
        "Current exploration rate",
    )
    _M_Q_TABLE_SIZE = Gauge(
        "modp_q_table_size",
        "Number of states in the Q-table",
    )
    _M_PUBLISH_FAILURES = _PromCounter(
        "modp_publish_failures_total",
        "Failed FeedbackEvent publishes",
    )
    _M_SAVE_FAILURES = _PromCounter(
        "modp_save_failures_total",
        "Failed Q-table saves",
    )
else:  # pragma: no cover
    _M_DECISIONS = _M_DEFER_HOURS = _M_REWARD = _M_EPSILON = None
    _M_Q_TABLE_SIZE = _M_PUBLISH_FAILURES = _M_SAVE_FAILURES = None


# ==============================================================================
# Helpers
# ==============================================================================
# Bucket caps: keep the state space bounded.
_CARBON_BUCKET_WIDTH = 50      # gCO2eq/kWh per bucket
_CARBON_BUCKET_CAP = 30        # 30 * 50 = 1500 gCO2eq/kWh upper bound
_QUEUE_BUCKET_CAP = 10
_DEADLINE_BUCKET_CAP = 24
_FORECAST_BUCKET_CAP = 30


def _as_float(
    value: Any,
    default: float,
    name: Optional[str] = None,
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        log_event("warning", f"Invalid {name or 'value'}: {value!r}; using {default}")
        return default
    if not math.isfinite(v):
        log_event("warning", f"Non-finite {name or 'value'}: {v}; using {default}")
        return default
    if min_val is not None and v < min_val:
        return min_val
    if max_val is not None and v > max_val:
        return max_val
    return v


def _as_int(
    value: Any,
    default: int,
    name: Optional[str] = None,
    min_val: Optional[int] = None,
    max_val: Optional[int] = None,
) -> int:
    try:
        v = int(value)
    except (TypeError, ValueError):
        log_event("warning", f"Invalid {name or 'value'}: {value!r}; using {default}")
        return default
    if min_val is not None and v < min_val:
        return min_val
    if max_val is not None and v > max_val:
        return max_val
    return v


def _parse_legacy_state_key(raw: str) -> Optional[Tuple[int, ...]]:
    """Parse a legacy stringified tuple like '(1, 2, 3, 4)' into a tuple of ints."""
    if not raw:
        return None
    s = raw.strip()
    if s.startswith("(") and s.endswith(")"):
        s = s[1:-1]
    if not s:
        return ()
    parts = [p.strip() for p in s.split(",") if p.strip() != ""]
    try:
        return tuple(int(p) for p in parts)
    except (TypeError, ValueError):
        return None


def _node_carbon(node: Any) -> Optional[float]:
    """Read region_carbon_intensity from a node, or None if unavailable."""
    try:
        v = getattr(node, "region_carbon_intensity", None)
        if v is None:
            return None
        f = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return f


# ==============================================================================
# Context record for a decision (used to fix the last_state/last_action race)
# ==============================================================================
@dataclass
class _DecisionContext:
    state: Tuple[int, int, int, int]
    action: int
    hours_to_deadline_at_decision: float
    created_at_monotonic: float


# ==============================================================================
# MODPScheduler
# ==============================================================================
class MODPScheduler:
    """
    MODP-based scheduler that decides when (and where) to run a workload.
    Uses a simple Q-learning approach for deferral decisions, enhanced with
    carbon forecasts and node selection.

    Public attributes preserved from v2.0:
      * ``q_table`` (Dict[state, np.ndarray])
      * ``epsilon``
      * ``last_state``, ``last_action``
      * ``carbon_fetcher``, ``message_queue``, ``q_table_path``
    """

    # Bucket caps (class-level so tests can override)
    CARBON_BUCKET_WIDTH: int = _CARBON_BUCKET_WIDTH
    CARBON_BUCKET_CAP: int = _CARBON_BUCKET_CAP
    QUEUE_BUCKET_CAP: int = _QUEUE_BUCKET_CAP
    DEADLINE_BUCKET_CAP: int = _DEADLINE_BUCKET_CAP
    FORECAST_BUCKET_CAP: int = _FORECAST_BUCKET_CAP

    Q_TABLE_FORMAT_VERSION: int = 2

    def __init__(
        self,
        carbon_fetcher: Optional[Any] = None,
        message_queue: Optional[Any] = None,
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
        *,
        seed: Optional[int] = None,
        save_interval_sec: float = 5.0,
        publish_events: bool = True,
        enable_prometheus: bool = True,
    ):
        # --- Validate / coerce parameters ---
        self.carbon_fetcher = carbon_fetcher
        self.message_queue = message_queue
        self.lr = _as_float(learning_rate, 0.1, "learning_rate", 0.0, 1.0)
        self.discount = _as_float(discount_factor, 0.9, "discount_factor", 0.0, 0.999)
        self.epsilon = _as_float(epsilon, 0.1, "epsilon", 0.0, 1.0)
        self.epsilon_decay = _as_float(epsilon_decay, 0.995, "epsilon_decay", 0.0, 1.0)
        self.min_epsilon = _as_float(min_epsilon, 0.01, "min_epsilon", 0.0, 1.0)
        if self.min_epsilon > self.epsilon:
            log_event(
                "warning",
                f"min_epsilon ({self.min_epsilon}) > epsilon ({self.epsilon}); "
                "clamping min_epsilon to epsilon",
            )
            self.min_epsilon = self.epsilon

        self.horizon = _as_int(horizon, 6, "horizon", min_val=1, max_val=48)
        self.defer_penalty = _as_float(defer_penalty, 0.05, "defer_penalty", 0.0)
        self.move_penalty = _as_float(move_penalty, 0.1, "move_penalty", 0.0)
        self.optimistic_init = _as_float(optimistic_init, 1.0, "optimistic_init")

        self.q_table_path: Path = (
            Path(q_table_path) if q_table_path else Path("./modp_q_table.json")
        )

        self._save_interval_sec = _as_float(save_interval_sec, 5.0, "save_interval_sec", 0.0)
        self._publish_events = bool(publish_events)
        self._enable_prometheus = bool(enable_prometheus) and PROMETHEUS_AVAILABLE

        # --- Per-instance RNG ---
        self._seed = seed if seed is not None else int.from_bytes(os.urandom(4), "little")
        self._rng = np.random.default_rng(self._seed)

        # --- Q-table and learning state (public) ---
        # Actions: 0=run_now, 1..horizon=defer k hours, horizon+1=move_node.
        self.q_table: Dict[Tuple[int, int, int, int], np.ndarray] = {}
        self.last_state: Optional[Tuple[int, int, int, int]] = None
        self.last_action: Optional[int] = None

        # --- Concurrency ---
        self._lock = asyncio.Lock()
        # Per-task context store: task_id -> _DecisionContext.
        self._decision_contexts: Dict[str, _DecisionContext] = {}
        # FIFO of task_ids for legacy learn() calls without task_id.
        self._decision_fifo: Deque[str] = deque(maxlen=1024)
        self._anon_counter = 0

        # --- Persistence bookkeeping ---
        self._dirty = False
        self._last_save_monotonic: float = 0.0
        self._pending_publish_tasks: set = set()
        self._closed = False
        self._loaded = False

        # --- Load Q-table eagerly (cheap) ---
        self._load_q_table()

    # ------------------------------------------------------------------
    # Public properties / helpers
    # ------------------------------------------------------------------
    @property
    def _num_actions(self) -> int:
        return self.horizon + 2

    def _discretize_state(
        self,
        current_carbon: float,
        queue_length: int,
        hours_to_deadline: float,
        forecast_mean: float,
    ) -> Tuple[int, int, int, int]:
        """Convert continuous state to discrete buckets (all bounded)."""
        # Coerce and clamp each dimension.
        try:
            c = float(current_carbon)
        except (TypeError, ValueError):
            c = 0.0
        if not math.isfinite(c):
            c = 0.0

        try:
            f = float(forecast_mean)
        except (TypeError, ValueError):
            f = c
        if not math.isfinite(f):
            f = c

        carbon_bucket = int(max(0.0, c) // self.CARBON_BUCKET_WIDTH)
        carbon_bucket = min(carbon_bucket, self.CARBON_BUCKET_CAP)

        queue_bucket = _as_int(queue_length, 0, "queue_length", min_val=0)
        queue_bucket = min(queue_bucket, self.QUEUE_BUCKET_CAP)

        deadline_bucket = int(max(0.0, float(hours_to_deadline)))
        deadline_bucket = min(deadline_bucket, self.DEADLINE_BUCKET_CAP)

        forecast_bucket = int(max(0.0, f) // self.CARBON_BUCKET_WIDTH)
        forecast_bucket = min(forecast_bucket, self.FORECAST_BUCKET_CAP)

        return (carbon_bucket, queue_bucket, deadline_bucket, forecast_bucket)

    def _get_action_values(self, state: Tuple[int, int, int, int]) -> np.ndarray:
        """Return Q-values for all actions for a given state, initializing if needed."""
        arr = self.q_table.get(state)
        if arr is None or arr.shape[0] != self._num_actions:
            self.q_table[state] = np.full(
                self._num_actions, self.optimistic_init, dtype=float,
            )
        return self.q_table[state]

    # ------------------------------------------------------------------
    # Carbon lookups
    # ------------------------------------------------------------------
    async def get_carbon_forecast(self, hours: Optional[int] = None) -> List[float]:
        """Return predicted carbon intensity for the next `hours` hours."""
        hours = _as_int(hours if hours is not None else self.horizon,
                        self.horizon, "hours", min_val=1, max_val=168)

        if self.carbon_fetcher is not None:
            try:
                # Preferred: async forecast method
                fn = getattr(self.carbon_fetcher, "forecast_carbon_prices", None)
                if fn is not None:
                    forecast = fn(hours=hours)
                    if asyncio.iscoroutine(forecast):
                        forecast = await forecast
                    if isinstance(forecast, dict) and forecast.get("status") == "success":
                        predictions = forecast.get("predictions", [])
                        if predictions:
                            return [float(x) for x in predictions[:hours]]
                    elif isinstance(forecast, list) and forecast:
                        return [float(x) for x in forecast[:hours]]
            except Exception as exc:
                log_event("warning", f"Carbon forecast failed: {exc}")

        # Fallback: constant carbon intensity.
        return [400.0] * hours

    async def get_current_carbon(self) -> float:
        """Get current carbon intensity, with fallback."""
        if self.carbon_fetcher is not None:
            try:
                fn = getattr(self.carbon_fetcher, "get_current_intensity", None)
                if fn is not None:
                    value = fn()
                    if asyncio.iscoroutine(value):
                        value = await value
                    f = float(value)
                    if math.isfinite(f) and f >= 0:
                        return f
            except Exception as exc:
                log_event("warning", f"Current carbon fetch failed: {exc}")
        return 400.0

    # ------------------------------------------------------------------
    # Decision
    # ------------------------------------------------------------------
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
        if self._closed:
            log_event("warning", "decide() called after close(); returning run_now.")
            return ("run_now", 0, None)

        # --- Current carbon ---
        if current_carbon is None:
            current_carbon = await self.get_current_carbon()
        current_carbon = _as_float(current_carbon, 400.0, "current_carbon", min_val=0.0)

        # --- Forecast ---
        try:
            forecast = await self.get_carbon_forecast(self.horizon)
        except Exception as exc:
            log_event("warning", f"Forecast lookup failed: {exc}")
            forecast = [current_carbon] * self.horizon

        forecast_mean = float(np.mean(forecast)) if forecast else current_carbon

        # --- Deadline ---
        hours_to_deadline = self._hours_to_deadline(workload)

        # --- Discretize ---
        state = self._discretize_state(
            current_carbon, queue_length, hours_to_deadline, forecast_mean,
        )

        async with self._lock:
            action_values = self._get_action_values(state)

            # Epsilon-greedy
            if self._rng.random() < self.epsilon:
                action = int(self._rng.integers(self._num_actions))
            else:
                adjusted = action_values.copy()
                for i in range(1, self.horizon + 1):
                    if i <= len(forecast):
                        future_carbon = float(forecast[i - 1])
                        adjusted[i] -= self.defer_penalty * (future_carbon / 100.0)
                    else:
                        adjusted[i] -= self.defer_penalty
                adjusted[self.horizon + 1] -= self.move_penalty
                action = int(np.argmax(adjusted))

            # Interpret action
            if action == 0:
                decision: Tuple[str, int, Optional[str]] = ("run_now", 0, None)
            elif 1 <= action <= self.horizon:
                decision = ("defer", action, None)
            else:
                # move_node
                target_id = self._select_best_node(available_nodes, current_carbon)
                if target_id is not None:
                    decision = ("move_node", 0, target_id)
                else:
                    # No better node available; fall back to run_now.
                    decision = ("run_now", 0, None)

            # Backward-compatible instance state.
            self.last_state = state
            self.last_action = action

            # Concurrency-safe per-task context.
            task_id = self._task_id_for(workload)
            if task_id is not None:
                self._decision_contexts[task_id] = _DecisionContext(
                    state=state,
                    action=action,
                    hours_to_deadline_at_decision=hours_to_deadline,
                    created_at_monotonic=time.monotonic(),
                )
                self._decision_fifo.append(task_id)

            # Epsilon decay.
            self.epsilon = max(
                self.min_epsilon, self.epsilon * self.epsilon_decay,
            )

        # --- Metrics ---
        if self._enable_prometheus:
            try:
                if _M_DECISIONS is not None:
                    _M_DECISIONS.labels(action=decision[0]).inc()
                if decision[0] == "defer" and _M_DEFER_HOURS is not None:
                    _M_DEFER_HOURS.observe(float(decision[1]))
                if _M_EPSILON is not None:
                    _M_EPSILON.set(self.epsilon)
                if _M_Q_TABLE_SIZE is not None:
                    _M_Q_TABLE_SIZE.set(len(self.q_table))
            except Exception:
                pass

        # --- Publish (fire-and-forget) ---
        self._spawn_publish(
            workload=workload,
            decision=decision,
            action_idx=action,
            state=state,
            current_carbon=current_carbon,
            forecast_mean=forecast_mean,
            queue_length=queue_length,
            hours_to_deadline=hours_to_deadline,
            available_nodes=available_nodes,
        )

        return decision

    def _hours_to_deadline(self, workload: Any) -> float:
        deadline = getattr(workload, "deadline", None)
        if deadline is None:
            return 24.0
        try:
            if isinstance(deadline, datetime):
                if deadline.tzinfo is None:
                    deadline = deadline.replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                return max(0.0, (deadline - now).total_seconds() / 3600.0)
        except Exception as exc:
            log_event("warning", f"Deadline computation failed: {exc}")
        return 24.0

    def _select_best_node(
        self,
        available_nodes: Optional[List[Any]],
        current_carbon: float,
    ) -> Optional[str]:
        """Return the id of a meaningfully better node, or None."""
        if not available_nodes:
            return None
        threshold = current_carbon * 0.9
        candidates: List[Tuple[float, str]] = []
        for node in available_nodes:
            c = _node_carbon(node)
            if c is None:
                continue
            if c < threshold:
                node_id = getattr(node, "id", None)
                if node_id is not None:
                    candidates.append((c, str(node_id)))
        if not candidates:
            return None
        candidates.sort(key=lambda t: t[0])
        return candidates[0][1]

    def _task_id_for(self, workload: Any) -> Optional[str]:
        tid = getattr(workload, "task_id", None)
        if tid:
            return str(tid)
        # Fall back to a per-decision anonymous key.
        self._anon_counter += 1
        return f"_anon_{self._anon_counter}"

    # ------------------------------------------------------------------
    # Learning
    # ------------------------------------------------------------------
    async def learn(
        self,
        reward: float,
        next_carbon: float,
        next_queue_length: int,
        next_forecast_mean: Optional[float] = None,
        *,
        state: Optional[Tuple[int, int, int, int]] = None,
        action: Optional[int] = None,
        next_hours_to_deadline: Optional[float] = None,
        task_id: Optional[str] = None,
    ):
        """
        Update Q-values based on observed reward and next state.

        Concurrency-safe variants: pass `state` and `action` directly (usually
        captured from the tuple returned by `decide_and_get_context`), or pass
        `task_id` to retrieve the context stored by `decide()`.
        """
        if self._closed:
            return

        # Resolve the (state, action) pair.
        ctx: Optional[_DecisionContext] = None
        if state is not None and action is not None:
            hours_at_decision = (
                float(next_hours_to_deadline)
                if next_hours_to_deadline is not None
                else 24.0
            )
            ctx = _DecisionContext(
                state=state,
                action=int(action),
                hours_to_deadline_at_decision=hours_at_decision,
                created_at_monotonic=time.monotonic(),
            )
        elif task_id is not None and task_id in self._decision_contexts:
            ctx = self._decision_contexts.pop(task_id)
            # Drop the FIFO entry as well.
            try:
                self._decision_fifo.remove(task_id)
            except ValueError:
                pass
        elif self._decision_fifo:
            # Legacy path: FIFO of pending decisions.
            oldest = self._decision_fifo.popleft()
            ctx = self._decision_contexts.pop(oldest, None)
        elif self.last_state is not None and self.last_action is not None:
            # Fully legacy path: use the most recent instance state.
            ctx = _DecisionContext(
                state=self.last_state,
                action=int(self.last_action),
                hours_to_deadline_at_decision=24.0,
                created_at_monotonic=time.monotonic(),
            )

        if ctx is None:
            return

        if not (0 <= ctx.action < self._num_actions):
            log_event("warning", f"Invalid action index {ctx.action}; skipping update")
            return

        reward = _as_float(reward, 0.0, "reward")
        next_carbon = _as_float(next_carbon, 400.0, "next_carbon", min_val=0.0)
        next_queue_length = _as_int(next_queue_length, 0, "next_queue_length", min_val=0)
        if next_forecast_mean is None:
            next_forecast_mean = next_carbon
        next_forecast_mean = _as_float(next_forecast_mean, next_carbon, "next_forecast_mean")

        # Prefer the caller-provided next deadline, else reuse the one captured
        # at decision time (best available without a workload reference).
        hours_next = (
            float(next_hours_to_deadline)
            if next_hours_to_deadline is not None
            else ctx.hours_to_deadline_at_decision
        )

        next_state = self._discretize_state(
            next_carbon, next_queue_length, hours_next, next_forecast_mean,
        )

        async with self._lock:
            next_values = self._get_action_values(next_state)
            max_next = float(np.max(next_values)) if next_values.size > 0 else 0.0

            current_values = self._get_action_values(ctx.state)
            td_target = reward + self.discount * max_next
            current_values[ctx.action] += self.lr * (td_target - current_values[ctx.action])

        # Reset legacy state.
        self.last_state = None
        self.last_action = None

        if self._enable_prometheus and _M_REWARD is not None:
            try:
                _M_REWARD.observe(reward)
            except Exception:
                pass

        # Persist (debounced).
        self._dirty = True
        await self._maybe_save_q_table()

    # ------------------------------------------------------------------
    # Reward helper
    # ------------------------------------------------------------------
    async def compute_reward(
        self,
        decision: str,
        delay_hours: int,
        carbon_at_execution: float,
        queue_length: int,
        success: bool = True,
    ) -> float:
        """Compute a reward in [-1.0, 1.0] for a decision."""
        if not success:
            return -1.0

        base_reward = 1.0
        delay_hours = max(0, int(delay_hours))
        try:
            carbon_at_execution = float(carbon_at_execution)
        except (TypeError, ValueError):
            carbon_at_execution = 400.0
        queue_length = max(0, int(queue_length))

        if decision == "defer":
            base_reward -= self.defer_penalty * delay_hours
            if carbon_at_execution < 200:
                base_reward += 0.2
            # Only deferral/move reinforce the queue penalty.
            base_reward -= 0.01 * min(queue_length, 10)
        elif decision == "move_node":
            base_reward -= self.move_penalty
            if carbon_at_execution < 200:
                base_reward += 0.2
            base_reward -= 0.01 * min(queue_length, 10)
        else:  # run_now
            # Running immediately keeps the queue shorter; no queue penalty.
            pass

        return max(-1.0, min(1.0, base_reward))

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------
    def get_policy_stats(self) -> Dict[str, Any]:
        """Return simple stats about the Q-table."""
        num_states = len(self.q_table)
        if num_states > 0:
            means = [float(np.mean(v)) for v in self.q_table.values()]
            maxes = [float(np.max(v)) for v in self.q_table.values()]
            mins = [float(np.min(v)) for v in self.q_table.values()]
            avg_q = float(np.mean(means))
            max_q = float(np.max(maxes))
            min_q = float(np.min(mins))
        else:
            avg_q = 0.0
            max_q = 0.0
            min_q = 0.0
        return {
            "num_states": num_states,
            "avg_q_value": avg_q,
            "max_q_value": max_q,
            "min_q_value": min_q,
            "epsilon": self.epsilon,
            "horizon": self.horizon,
            "num_actions": self._num_actions,
            "pending_decisions": len(self._decision_fifo),
            "last_save_monotonic": self._last_save_monotonic,
            "dirty": self._dirty,
        }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    async def _maybe_save_q_table(self) -> None:
        if not self._dirty:
            return
        if time.monotonic() - self._last_save_monotonic < self._save_interval_sec:
            return
        await self.flush()

    async def flush(self) -> None:
        """Force-persist the Q-table and learning state."""
        if not self._dirty and self.q_table_path.exists():
            return
        try:
            await asyncio.to_thread(self._write_q_table_sync)
            self._last_save_monotonic = time.monotonic()
            self._dirty = False
        except Exception as exc:
            log_event("error", f"Failed to save Q-table: {exc}")
            if self._enable_prometheus and _M_SAVE_FAILURES is not None:
                try:
                    _M_SAVE_FAILURES.inc()
                except Exception:
                    pass

    def _write_q_table_sync(self) -> None:
        path = self.q_table_path
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": self.Q_TABLE_FORMAT_VERSION,
            "horizon": self.horizon,
            "epsilon": self.epsilon,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "states": [
                {"key": list(k), "values": v.tolist()}
                for k, v in self.q_table.items()
            ],
        }
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, path)

    def _load_q_table(self) -> None:
        if not self.q_table_path.exists():
            return
        try:
            with open(self.q_table_path, "r") as f:
                data = json.load(f)
        except Exception as exc:
            log_event("error", f"Failed to read Q-table: {exc}")
            return

        try:
            if isinstance(data, dict) and data.get("version") == self.Q_TABLE_FORMAT_VERSION:
                # New format.
                file_horizon = int(data.get("horizon", self.horizon))
                if file_horizon != self.horizon:
                    log_event(
                        "warning",
                        f"Q-table horizon mismatch: file={file_horizon}, "
                        f"scheduler={self.horizon}; states will be skipped.",
                    )
                expected = file_horizon + 2
                for entry in data.get("states", []):
                    try:
                        key = tuple(int(x) for x in entry["key"])
                        values = np.asarray(entry["values"], dtype=float)
                    except Exception:
                        continue
                    if values.shape[0] != expected:
                        continue
                    self.q_table[key] = values
                saved_eps = data.get("epsilon")
                if saved_eps is not None:
                    self.epsilon = max(
                        self.min_epsilon,
                        min(1.0, float(saved_eps)),
                    )
                log_event(
                    "info",
                    f"Loaded Q-table with {len(self.q_table)} states "
                    f"(epsilon={self.epsilon:.4f}) from {self.q_table_path}",
                )
                return

            # Legacy format: { "(0, 1, 2, 3)": [q0, q1, ...], ... }
            if isinstance(data, dict):
                skipped = 0
                for key_str, values in data.items():
                    if key_str in {"version", "horizon", "epsilon", "saved_at", "states"}:
                        continue
                    key = _parse_legacy_state_key(key_str)
                    if key is None or len(key) != 4:
                        skipped += 1
                        continue
                    arr = np.asarray(values, dtype=float)
                    if arr.shape[0] != self._num_actions:
                        skipped += 1
                        continue
                    self.q_table[key] = arr
                log_event(
                    "info",
                    f"Loaded legacy Q-table with {len(self.q_table)} states "
                    f"(skipped {skipped}) from {self.q_table_path}",
                )
        except Exception as exc:
            log_event("error", f"Failed to parse Q-table: {exc}")

    # ------------------------------------------------------------------
    # Feedback publishing
    # ------------------------------------------------------------------
    def _spawn_publish(
        self,
        workload: Any,
        decision: Tuple[str, int, Optional[str]],
        action_idx: int,
        state: Tuple[int, int, int, int],
        current_carbon: float,
        forecast_mean: float,
        queue_length: int,
        hours_to_deadline: float,
        available_nodes: Optional[List[Any]],
    ) -> None:
        if not self._publish_events or self.message_queue is None or FeedbackEvent is Any:
            return
        try:
            task = asyncio.create_task(
                self._publish(
                    workload=workload,
                    decision=decision,
                    action_idx=action_idx,
                    state=state,
                    current_carbon=current_carbon,
                    forecast_mean=forecast_mean,
                    queue_length=queue_length,
                    hours_to_deadline=hours_to_deadline,
                    available_nodes=available_nodes,
                )
            )
        except RuntimeError:
            # No running loop: skip publishing rather than blocking.
            return
        self._pending_publish_tasks.add(task)
        task.add_done_callback(self._pending_publish_tasks.discard)

    async def _publish(
        self,
        workload: Any,
        decision: Tuple[str, int, Optional[str]],
        action_idx: int,
        state: Tuple[int, int, int, int],
        current_carbon: float,
        forecast_mean: float,
        queue_length: int,
        hours_to_deadline: float,
        available_nodes: Optional[List[Any]],
    ) -> None:
        try:
            task_id = getattr(workload, "task_id", None)
            available_ids = [
                getattr(n, "id", None) for n in (available_nodes or [])
            ]
            try:
                reward = float(await self.compute_reward(
                    decision=decision[0],
                    delay_hours=decision[1],
                    carbon_at_execution=current_carbon,
                    queue_length=queue_length,
                    success=True,
                ))
            except Exception:
                reward = 0.0

            event = FeedbackEvent(
                source="modp_scheduler",
                feedback_type="routing",
                task_id=task_id,
                context={
                    "current_carbon": float(current_carbon),
                    "forecast_mean": float(forecast_mean),
                    "queue_length": int(queue_length),
                    "hours_to_deadline": float(hours_to_deadline),
                    "state": list(state),
                    "available_nodes": [x for x in available_ids if x is not None],
                },
                action={
                    "selected_action": decision[0],
                    "selected_rank": int(action_idx),
                    "confidence_score": 0.5,
                },
                performance={
                    "quality_score": 0.9,
                    "latency_ms": 0.0,
                    "energy_joules": 0.0,
                    "carbon_g": float(current_carbon),
                    "helium_cost": 0.0,
                    "duration_ms": 0.0,
                },
                adaptive_cost_value=reward,
                tags=["modp", "scheduling", "carbon_aware"],
            )
            payload = (
                event.to_json()
                if hasattr(event, "to_json") and callable(event.to_json)
                else json.dumps(
                    event.to_dict() if hasattr(event, "to_dict") else {},
                    default=str,
                )
            )
            await self.message_queue.publish("modp_events", payload)
        except Exception as exc:
            log_event("warning", f"Failed to publish MODP decision event: {exc}")
            if self._enable_prometheus and _M_PUBLISH_FAILURES is not None:
                try:
                    _M_PUBLISH_FAILURES.inc()
                except Exception:
                    pass

    async def drain_publishes(self) -> None:
        """Await any in-flight publish tasks."""
        if not self._pending_publish_tasks:
            return
        pending = list(self._pending_publish_tasks)
        await asyncio.gather(*pending, return_exceptions=True)
        self._pending_publish_tasks.clear()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def flush_and_close(self) -> None:
        """Alias kept for symmetry with other modules."""
        await self.aclose()

    async def aclose(self) -> None:
        """Flush the Q-table and drain in-flight publishes."""
        if self._closed:
            return
        self._closed = True
        await self.drain_publishes()
        self._dirty = True
        try:
            await asyncio.to_thread(self._write_q_table_sync)
            self._dirty = False
            self._last_save_monotonic = time.monotonic()
        except Exception as exc:
            log_event("error", f"Failed to save Q-table on close: {exc}")

    async def __aenter__(self) -> "MODPScheduler":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()


# ==============================================================================
# Example usage
# ==============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    class _FakeWorkload:
        task_id = "task-1"
        deadline = None

    class _FakeNode:
        def __init__(self, id_: str, carbon: float):
            self.id = id_
            self.region_carbon_intensity = carbon

    async def _demo() -> None:
        scheduler = MODPScheduler(seed=42)
        try:
            workload = _FakeWorkload()
            node = _FakeNode("node-a", 400.0)
            alt_nodes = [
                _FakeNode("node-a", 400.0),
                _FakeNode("node-b", 150.0),
                _FakeNode("node-c", 380.0),
            ]
            for _ in range(3):
                action, delay, target = await scheduler.decide(
                    workload, node, queue_length=2, available_nodes=alt_nodes,
                )
                print(f"Decision: {action}, delay={delay}, target={target}")
                await scheduler.learn(
                    reward=0.7,
                    next_carbon=300.0,
                    next_queue_length=1,
                    task_id=workload.task_id,
                )
            print("Stats:", scheduler.get_policy_stats())
        finally:
            await scheduler.aclose()

    asyncio.run(_demo())
