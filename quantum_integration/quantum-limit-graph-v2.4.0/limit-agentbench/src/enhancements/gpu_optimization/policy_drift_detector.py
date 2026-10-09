#!/usr/bin/env python3
"""
Enhanced drift detection for selected FlexGen policies.
=========================================================
Monitors the distribution of chosen policies and triggers rollback if drift
occurs.

FIXES OVER v2.0:
- Guarded every relative import; added log_event shim.
- Fixed moving-average mode so baseline_policy is not overwritten by the
  most recent policy (rollback now targets a safe policy).
- Persistence is debounced, threaded, atomic, and versioned.
- Lock is released during I/O.
- Publisher startup is race-free.
- `_trigger_drift_callback` uses the owner's loop instead of asyncio.run.
- Events are drained before stop_publisher cancels the task.
- Callback fires once per drift episode; resets when drift resolves.
- Distance metric validates vector lengths.
- baseline_policy stored as a deep copy.
- Constructor arguments validated (threshold, ewma_alpha, baseline_window,
  consecutive_drift_threshold, drift_cooldown_seconds, history_size).
- asyncio.Queue created lazily on first use.
- add_policy returns a status dict.
- get_history() added.
- Optional Prometheus metrics.
- aclose() / flush() / __aenter__ / __aexit__ added.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import math
import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional

# ---------- Prometheus (optional) ----------
try:
    from prometheus_client import Counter as _PromCounter, Gauge
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


# ---------- Guarded relative imports ----------
try:
    from ..async_message_queue import AsyncMessageQueue  # type: ignore
except ImportError:  # pragma: no cover
    AsyncMessageQueue = None  # type: ignore

try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:  # pragma: no cover
    FeedbackEvent = None  # type: ignore


# ---------- Prometheus metrics (module scope: single registration) ----------
if PROMETHEUS_AVAILABLE:
    _M_OBSERVATIONS = _PromCounter(
        "policy_drift_observations_total",
        "Policies observed by the drift detector",
    )
    _M_EVENTS = _PromCounter(
        "policy_drift_events_total",
        "Drift events detected (EWMA above threshold)",
    )
    _M_ALERTS = _PromCounter(
        "policy_drift_alerts_total",
        "Drift alerts published",
    )
    _M_CALLBACKS = _PromCounter(
        "policy_drift_callbacks_total",
        "Persistent-drift callbacks triggered",
    )
    _M_PUBLISH_FAILURES = _PromCounter(
        "policy_drift_publish_failures_total",
        "Drift alert publish failures",
    )
    _M_SAVE_FAILURES = _PromCounter(
        "policy_drift_save_failures_total",
        "State save failures",
    )
    _M_LOAD_FAILURES = _PromCounter(
        "policy_drift_load_failures_total",
        "State load failures",
    )
    _M_EWMA = Gauge(
        "policy_drift_ewma",
        "Current EWMA of the drift distance",
    )
    _M_COUNTER = Gauge(
        "policy_drift_consecutive",
        "Current consecutive-drift counter",
    )
else:  # pragma: no cover
    _M_OBSERVATIONS = _M_EVENTS = _M_ALERTS = _M_CALLBACKS = None
    _M_PUBLISH_FAILURES = _M_SAVE_FAILURES = _M_LOAD_FAILURES = None
    _M_EWMA = _M_COUNTER = None


# ==============================================================================
# Helpers
# ==============================================================================
_VALID_METRICS = ("euclidean", "manhattan", "cosine")
_VALID_BASELINE_MODES = ("fixed", "moving_average")


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
) -> int:
    try:
        v = int(value)
    except (TypeError, ValueError):
        log_event("warning", f"Invalid {name or 'value'}: {value!r}; using {default}")
        return default
    if min_val is not None and v < min_val:
        return min_val
    return v


# ==============================================================================
# PolicyDriftDetector
# ==============================================================================
class PolicyDriftDetector:
    """
    Specialized drift detector for FlexGen policy selection.
    Monitors chosen policies over time and detects when they deviate significantly
    from a safe baseline.
    """

    STATE_FORMAT_VERSION: int = 2

    def __init__(
        self,
        threshold: float = 0.3,
        history_size: int = 100,
        baseline_policy: Optional[Dict[str, Any]] = None,
        message_queue: Optional[Any] = None,
        persistence_path: Optional[str] = "policy_drift_state.pkl",
        metric: str = "euclidean",
        ewma_alpha: float = 0.3,
        consecutive_drift_threshold: int = 3,
        baseline_update_mode: str = "fixed",
        baseline_window: int = 10,
        drift_cooldown_seconds: float = 60.0,
        on_drift_callback: Optional[Callable[[Dict[str, Any]], Any]] = None,
        *,
        seed: Optional[int] = None,
        save_interval_seconds: float = 5.0,
        publish_events: bool = True,
        enable_prometheus: bool = True,
    ):
        # ---- Validate inputs ----
        if metric not in _VALID_METRICS:
            raise ValueError(f"Unsupported metric: {metric}; expected one of {_VALID_METRICS}")
        if baseline_update_mode not in _VALID_BASELINE_MODES:
            raise ValueError(
                f"Unsupported baseline_update_mode: {baseline_update_mode}; "
                f"expected one of {_VALID_BASELINE_MODES}"
            )

        self.threshold = _as_float(threshold, 0.3, "threshold", min_val=0.0)
        self.history_size = _as_int(history_size, 100, "history_size", min_val=1)
        self.history: Deque[Dict[str, Any]] = deque(maxlen=self.history_size)

        self.baseline_vector: Optional[List[float]] = None
        self.baseline_policy: Optional[Dict[str, Any]] = (
            copy.deepcopy(baseline_policy) if baseline_policy else None
        )
        if self.baseline_policy:
            self.baseline_vector = self._policy_to_vector(self.baseline_policy)

        self.message_queue = message_queue
        self.persistence_path = persistence_path
        self.metric = metric
        self.ewma_alpha = _as_float(ewma_alpha, 0.3, "ewma_alpha", min_val=0.0001, max_val=1.0)
        self._ewma_distance: Optional[float] = None
        self._drift_counter = 0
        self._consecutive_drift_threshold = _as_int(
            consecutive_drift_threshold, 3, "consecutive_drift_threshold", min_val=1,
        )
        self._last_drift_time: Optional[float] = None
        self._last_alert_time: Optional[float] = None

        self.baseline_update_mode = baseline_update_mode
        self.baseline_window = _as_int(baseline_window, 10, "baseline_window", min_val=1)
        self.drift_cooldown_seconds = _as_float(
            drift_cooldown_seconds, 60.0, "drift_cooldown_seconds", min_val=0.0,
        )
        self.on_drift_callback = on_drift_callback

        # Track whether the callback has fired for the current drift episode.
        self._callback_fired = False

        # Rollback target: separate from baseline_policy in moving_average mode.
        self._rollback_policy: Optional[Dict[str, Any]] = copy.deepcopy(
            baseline_policy
        ) if baseline_policy else None

        # ---- Threading ----
        self._lock = threading.Lock()

        # ---- Async publisher (created lazily) ----
        self._event_queue: Optional[asyncio.Queue] = None
        self._publisher_task: Optional[asyncio.Task] = None
        self._publisher_lock = asyncio.Lock()
        self._owner_loop: Optional[asyncio.AbstractEventLoop] = None

        # ---- Configuration ----
        self._publish_events = bool(publish_events)
        self._enable_prometheus = bool(enable_prometheus) and PROMETHEUS_AVAILABLE
        self._save_interval_seconds = _as_float(
            save_interval_seconds, 5.0, "save_interval_seconds", min_val=0.0,
        )
        self._last_save_at: float = 0.0
        self._dirty = False
        self._closed = False

        # Deterministic not required here, but kept for forward compatibility.
        self._seed = seed

        # ---- Load persisted state ----
        # We do the load synchronously for backward compatibility but shield the
        # heavy path in _load_state (small file).
        if self.persistence_path and Path(self.persistence_path).exists():
            self._load_state()

    # ------------------------------------------------------------------
    # Vector encoding
    # ------------------------------------------------------------------
    def _policy_to_vector(self, policy: Dict[str, Any]) -> List[float]:
        """Convert policy dict to fixed-length numeric vector."""
        return [
            _as_float(policy.get("gpu_batch_size", 1), 1.0, "gpu_batch_size") / 8.0,
            _as_float(policy.get("block_size", 16), 16.0, "block_size") / 64.0,
            1.0 if policy.get("weight_device") == "gpu" else 0.0,
            1.0 if policy.get("weight_device") == "cpu" else 0.0,
            1.0 if policy.get("weight_device") == "disk" else 0.0,
            1.0 if policy.get("activation_device") == "gpu" else 0.0,
            1.0 if policy.get("activation_device") == "cpu" else 0.0,
            1.0 if policy.get("kv_cache_device") == "gpu" else 0.0,
            1.0 if policy.get("kv_cache_device") == "cpu" else 0.0,
            1.0 if policy.get("kv_cache_device") == "disk" else 0.0,
            _as_float(policy.get("weight_bits", 16), 16.0, "weight_bits") / 16.0,
            _as_float(policy.get("kv_cache_bits", 16), 16.0, "kv_cache_bits") / 16.0,
            1.0 if policy.get("cpu_attention", False) else 0.0,
            1.0 if policy.get("overlap_io_compute", True) else 0.0,
        ]

    def _distance(self, a: List[float], b: List[float]) -> float:
        """Compute distance between two vectors using the configured metric."""
        if len(a) != len(b):
            log_event(
                "warning",
                "Vector length mismatch",
                a_len=len(a), b_len=len(b),
            )
            # Pad the shorter one with zeros to avoid silent truncation.
            n = max(len(a), len(b))
            a = list(a) + [0.0] * (n - len(a))
            b = list(b) + [0.0] * (n - len(b))

        if self.metric == "manhattan":
            return float(sum(abs(x - y) for x, y in zip(a, b)))
        if self.metric == "cosine":
            dot = sum(x * y for x, y in zip(a, b))
            norm_a = math.sqrt(sum(x * x for x in a))
            norm_b = math.sqrt(sum(x * x for x in b))
            if norm_a == 0.0 or norm_b == 0.0:
                return 1.0
            cos_sim = dot / (norm_a * norm_b)
            # Clamp to [0, 2] to guard against floating-point overshoot.
            return float(max(0.0, min(2.0, 1.0 - cos_sim)))
        # euclidean
        return float(math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b))))

    # ------------------------------------------------------------------
    # Core policy processing (assumes self._lock is held)
    # ------------------------------------------------------------------
    def _process_policy(
        self, policy_dict: Dict[str, Any], reward: Optional[float]
    ) -> Dict[str, Any]:
        """Record a policy, update the EWMA distance, and return status."""
        vec = self._policy_to_vector(policy_dict)
        entry = {
            "vector": vec,
            "reward": float(reward) if reward is not None else None,
            "timestamp": time.time(),
            "policy": copy.deepcopy(policy_dict),
        }
        self.history.append(entry)

        # First observation establishes the baseline.
        if self.baseline_vector is None:
            self.baseline_vector = list(vec)
            self.baseline_policy = copy.deepcopy(policy_dict)
            self._rollback_policy = copy.deepcopy(policy_dict)
            self._ewma_distance = 0.0
            self._drift_counter = 0
            self._callback_fired = False
            log_event("info", "Baseline policy set from first observed policy.")
            self._dirty = True
            return {
                "drift": False,
                "ewma": 0.0,
                "counter": 0,
                "alert": False,
            }

        # Moving-average mode: update the baseline vector from recent history
        # but keep `baseline_policy` and `_rollback_policy` stable, so that
        # rollback actually returns a safe policy.
        if self.baseline_update_mode == "moving_average":
            window = list(self.history)[-self.baseline_window :]
            if window:
                vectors = [h["vector"] for h in window]
                n = len(vectors)
                # Simple mean of each dimension.
                avg_vec = [sum(col) / n for col in zip(*vectors)]
                self.baseline_vector = avg_vec
                # Pick the highest-reward entry in the window as the rollback
                # target (rewards may be None; fall back to the most recent).
                best = None
                best_reward = -float("inf")
                for h in window:
                    r = h.get("reward")
                    if r is not None and r > best_reward:
                        best_reward = r
                        best = h
                if best is not None:
                    self._rollback_policy = copy.deepcopy(best["policy"])
                else:
                    self._rollback_policy = copy.deepcopy(window[-1]["policy"])

        dist = self._distance(vec, self.baseline_vector)

        # EWMA update.
        if self._ewma_distance is None:
            self._ewma_distance = dist
        else:
            self._ewma_distance = (
                self.ewma_alpha * dist + (1.0 - self.ewma_alpha) * self._ewma_distance
            )

        drift_detected = self._ewma_distance > self.threshold
        alert_published = False
        callback_fired = False

        if drift_detected:
            self._drift_counter += 1
            self._last_drift_time = time.time()
            log_event(
                "warning",
                f"Policy drift detected: distance={dist:.3f} "
                f"ewma={self._ewma_distance:.3f} counter={self._drift_counter}",
            )
            if self._enable_prometheus and _M_EVENTS is not None:
                try:
                    _M_EVENTS.inc()
                except Exception:
                    pass

            # Alert with cooldown.
            if (
                self._last_alert_time is None
                or (time.time() - self._last_alert_time) >= self.drift_cooldown_seconds
            ):
                self._publish_drift_alert(dist, self._ewma_distance)
                self._last_alert_time = time.time()
                alert_published = True

            # Persistent-drift callback (fires once per episode).
            if (
                self._drift_counter >= self._consecutive_drift_threshold
                and not self._callback_fired
            ):
                self._callback_fired = True
                callback_fired = True
                self._trigger_drift_callback()
        else:
            if self._drift_counter > 0:
                log_event("info", f"Drift resolved: ewma={self._ewma_distance:.3f}")
            self._drift_counter = 0
            self._callback_fired = False

        if self._enable_prometheus:
            try:
                if _M_EWMA is not None and self._ewma_distance is not None:
                    _M_EWMA.set(float(self._ewma_distance))
                if _M_COUNTER is not None:
                    _M_COUNTER.set(int(self._drift_counter))
            except Exception:
                pass

        self._dirty = True
        # Debounced save; caller flushes on shutdown via aclose().
        self._maybe_save_locked()

        return {
            "drift": bool(drift_detected),
            "ewma": float(self._ewma_distance) if self._ewma_distance is not None else 0.0,
            "counter": int(self._drift_counter),
            "alert": bool(alert_published),
            "callback": bool(callback_fired),
        }

    # ------------------------------------------------------------------
    # Public API: add policies
    # ------------------------------------------------------------------
    def add_policy(
        self, policy_dict: Dict[str, Any], reward: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Record a chosen policy along with optional reward.
        Thread-safe. Returns a status dict.
        """
        if self._closed:
            return {"drift": False, "ewma": 0.0, "counter": 0, "closed": True}
        with self._lock:
            status = self._process_policy(policy_dict, reward)
        if self._enable_prometheus and _M_OBSERVATIONS is not None:
            try:
                _M_OBSERVATIONS.inc()
            except Exception:
                pass
        return status

    async def add_policy_async(
        self, policy_dict: Dict[str, Any], reward: Optional[float] = None
    ) -> Dict[str, Any]:
        """Async wrapper for add_policy; runs the CPU-bound work in a thread."""
        return await asyncio.to_thread(self.add_policy, policy_dict, reward)

    # ------------------------------------------------------------------
    # Publisher lifecycle
    # ------------------------------------------------------------------
    def _publish_drift_alert(self, distance: float, ewma: float) -> None:
        """Enqueue a drift alert event for publishing (non-blocking)."""
        if not self._publish_events:
            return
        if self.message_queue is None or FeedbackEvent is None:
            return
        try:
            event = FeedbackEvent(
                source="policy_drift_detector",
                feedback_type="telemetry",
                task_id="drift_monitor",
                context={
                    "distance": float(distance),
                    "ewma": float(ewma),
                    "threshold": float(self.threshold),
                },
                action={
                    "selected_action": "alert",
                    "selected_rank": 0,
                    "confidence_score": 0.9,
                },
                performance={
                    "quality_score": 0.5,
                    "latency_ms": 0.0,
                    "energy_joules": 0.0,
                    "carbon_g": 0.0,
                    "helium_cost": 0.0,
                    "duration_ms": 0.0,
                },
                adaptive_cost_value=0.0,
                tags=["drift", "policy_selection", "alert"],
            )
        except Exception as exc:
            log_event("warning", f"Failed to build drift FeedbackEvent: {exc}")
            return

        q = self._ensure_queue()
        try:
            q.put_nowait(event)
        except Exception as exc:
            log_event("warning", f"Failed to enqueue drift event: {exc}")
            return

        # Best-effort publisher startup.
        self._ensure_publisher_started()

    def _ensure_queue(self) -> asyncio.Queue:
        """Create the event queue on first use (binds to the current loop)."""
        if self._event_queue is None:
            self._event_queue = asyncio.Queue()
        return self._event_queue

    def _ensure_publisher_started(self) -> None:
        """Start the async publisher task if not already running."""
        if not self._publish_events:
            return
        if self.message_queue is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            log_event(
                "warning",
                "No running event loop; drift alert queued but not published. "
                "Call start_publisher() from an async context.",
            )
            return

        # Remember the loop we were started on so callbacks can be scheduled
        # on it later.
        self._owner_loop = loop

        if self._publisher_task is not None and not self._publisher_task.done():
            return
        # Race-safe creation: schedule under the loop.
        try:
            self._publisher_task = loop.create_task(self._publish_loop())
        except Exception as exc:
            log_event("warning", f"Failed to start publisher: {exc}")

    async def _publish_loop(self) -> None:
        """Continuously publish events from the queue."""
        q = self._ensure_queue()
        while True:
            event = await q.get()
            if event is None:
                # Sentinel for graceful shutdown.
                q.task_done()
                return
            try:
                if self.message_queue is not None:
                    payload = (
                        event.to_json()
                        if hasattr(event, "to_json") and callable(event.to_json)
                        else json.dumps(
                            event.to_dict() if hasattr(event, "to_dict") else {},
                            default=str,
                        )
                    )
                    await self.message_queue.publish("drift_events", payload)
                    if self._enable_prometheus and _M_ALERTS is not None:
                        try:
                            _M_ALERTS.inc()
                        except Exception:
                            pass
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log_event("error", f"Failed to publish drift event: {exc}")
                if self._enable_prometheus and _M_PUBLISH_FAILURES is not None:
                    try:
                        _M_PUBLISH_FAILURES.inc()
                    except Exception:
                        pass
            finally:
                q.task_done()

    async def start_publisher(self) -> None:
        """Explicitly start the publisher task (if not already running)."""
        self._ensure_publisher_started()

    async def stop_publisher(self, drain: bool = True, timeout: float = 5.0) -> None:
        """
        Stop the publisher task.

        If `drain` is True, first flush pending events (up to `timeout`
        seconds), then send a sentinel and await the loop's exit.
        """
        q = self._event_queue
        if q is not None and drain:
            try:
                await asyncio.wait_for(q.join(), timeout=timeout)
            except asyncio.TimeoutError:
                log_event("warning", "Timed out draining drift event queue")
            except Exception as exc:
                log_event("warning", f"Error while draining queue: {exc}")

        task = self._publisher_task
        if task is None:
            return
        if q is not None:
            try:
                q.put_nowait(None)
            except Exception:
                pass
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
        self._publisher_task = None

    # ------------------------------------------------------------------
    # Drift callback
    # ------------------------------------------------------------------
    def _trigger_drift_callback(self) -> None:
        """Invoke the drift callback. Handles sync and async callbacks."""
        if not self.on_drift_callback:
            return
        if self._enable_prometheus and _M_CALLBACKS is not None:
            try:
                _M_CALLBACKS.inc()
            except Exception:
                pass
        try:
            payload = copy.deepcopy(self._rollback_policy or self.baseline_policy)
            result = self.on_drift_callback(payload)
            if asyncio.iscoroutine(result):
                # Prefer the owner loop; fall back to the current running loop.
                loop = self._owner_loop
                if loop is None or loop.is_closed():
                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        loop = None
                if loop is not None and not loop.is_closed():
                    try:
                        loop.call_soon_threadsafe(
                            lambda: loop.create_task(result)
                        )
                        return
                    except Exception as exc:
                        log_event("warning", f"Failed to schedule drift callback: {exc}")
                # No loop available: run synchronously on a throwaway loop.
                log_event(
                    "warning",
                    "No loop available for async drift callback; running synchronously.",
                )
                asyncio.run(result)
        except Exception as exc:
            log_event("error", f"Drift callback failed: {exc}")

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------
    def detect_drift(self) -> bool:
        """Return True if persistent drift is detected."""
        with self._lock:
            return self._drift_counter >= self._consecutive_drift_threshold

    def get_baseline_policy(self) -> Optional[Dict[str, Any]]:
        """Return a copy of the safe baseline policy."""
        with self._lock:
            return copy.deepcopy(self.baseline_policy) if self.baseline_policy else None

    def set_baseline(self, policy_dict: Dict[str, Any]) -> None:
        """Manually set a new baseline policy."""
        with self._lock:
            self.baseline_vector = self._policy_to_vector(policy_dict)
            self.baseline_policy = copy.deepcopy(policy_dict)
            self._rollback_policy = copy.deepcopy(policy_dict)
            self._ewma_distance = 0.0
            self._drift_counter = 0
            self._callback_fired = False
            log_event("info", "Baseline policy manually updated.")
            self._dirty = True
            self._maybe_save_locked(force=True)

    def rollback_to_baseline(self) -> Dict[str, Any]:
        """Return a copy of the safe policy for rollback (caller applies it)."""
        with self._lock:
            target = self._rollback_policy or self.baseline_policy
            if target is None:
                raise ValueError("No baseline policy available.")
            log_event("info", "Rollback requested; returning safe policy.")
            return copy.deepcopy(target)

    def reset_state(self) -> None:
        """Reset drift state but keep baseline."""
        with self._lock:
            self.history.clear()
            self._ewma_distance = 0.0
            self._drift_counter = 0
            self._last_drift_time = None
            self._last_alert_time = None
            self._callback_fired = False
            log_event("info", "Drift state reset (baseline preserved).")
            self._dirty = True
            self._maybe_save_locked(force=True)

    def get_stats(self) -> Dict[str, Any]:
        """Return current drift statistics."""
        with self._lock:
            return {
                "history_size": len(self.history),
                "ewma_distance": self._ewma_distance,
                "threshold": self.threshold,
                "drift_counter": self._drift_counter,
                "last_drift_time": self._last_drift_time,
                "last_alert_time": self._last_alert_time,
                "baseline_update_mode": self.baseline_update_mode,
                "metric": self.metric,
                "closed": self._closed,
                "publisher_running": (
                    self._publisher_task is not None
                    and not self._publisher_task.done()
                ),
            }

    def get_history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return a snapshot of the policy history."""
        with self._lock:
            snapshot = list(self.history)
        if limit is None or limit <= 0:
            return snapshot
        return snapshot[-limit:]

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _maybe_save_locked(self, force: bool = False) -> None:
        """Debounced save; caller must hold the lock. Runs the write in a thread."""
        if not self.persistence_path:
            return
        now = time.monotonic()
        if not force and (now - self._last_save_at) < self._save_interval_seconds:
            return
        # Snapshot under lock; write outside the lock.
        snapshot = self._build_state_snapshot_locked()
        self._last_save_at = now
        self._dirty = False
        try:
            t = threading.Thread(
                target=self._write_state_sync,
                args=(snapshot,),
                daemon=True,
            )
            t.start()
        except Exception as exc:
            log_event("warning", f"Failed to spawn state writer: {exc}")

    def _build_state_snapshot_locked(self) -> Dict[str, Any]:
        return {
            "version": self.STATE_FORMAT_VERSION,
            "baseline_vector": list(self.baseline_vector) if self.baseline_vector else None,
            "baseline_policy": copy.deepcopy(self.baseline_policy),
            "rollback_policy": copy.deepcopy(self._rollback_policy),
            "ewma_distance": self._ewma_distance,
            "drift_counter": self._drift_counter,
            "last_drift_time": self._last_drift_time,
            "last_alert_time": self._last_alert_time,
            "callback_fired": self._callback_fired,
            "history": list(self.history)[-100:],
        }

    def _write_state_sync(self, snapshot: Dict[str, Any]) -> None:
        path = self.persistence_path
        if not path:
            return
        try:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(p.suffix + ".tmp")
            with open(tmp, "wb") as f:
                pickle.dump(snapshot, f, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp, p)
        except Exception as exc:
            log_event("warning", f"Failed to save drift state: {exc}")
            if self._enable_prometheus and _M_SAVE_FAILURES is not None:
                try:
                    _M_SAVE_FAILURES.inc()
                except Exception:
                    pass

    def _load_state(self) -> None:
        if not self.persistence_path:
            return
        p = Path(self.persistence_path)
        if not p.exists():
            return
        try:
            with open(p, "rb") as f:
                state = pickle.load(f)
        except Exception as exc:
            log_event("warning", f"Failed to load drift state: {exc}")
            if self._enable_prometheus and _M_LOAD_FAILURES is not None:
                try:
                    _M_LOAD_FAILURES.inc()
                except Exception:
                    pass
            return

        try:
            version = state.get("version", 1)
            if version > self.STATE_FORMAT_VERSION:
                log_event(
                    "warning",
                    f"State version {version} > supported "
                    f"{self.STATE_FORMAT_VERSION}; ignoring file.",
                )
                return
            self.baseline_vector = state.get("baseline_vector")
            self.baseline_policy = state.get("baseline_policy")
            self._rollback_policy = state.get("rollback_policy") or self.baseline_policy
            self._ewma_distance = state.get("ewma_distance")
            self._drift_counter = int(state.get("drift_counter", 0) or 0)
            self._last_drift_time = state.get("last_drift_time")
            self._last_alert_time = state.get("last_alert_time")
            self._callback_fired = bool(state.get("callback_fired", False))
            hist = state.get("history", []) or []
            self.history = deque(hist, maxlen=self.history_size)
            log_event("info", "Drift state loaded from disk.")
        except Exception as exc:
            log_event("warning", f"Failed to parse drift state: {exc}")

    async def flush(self) -> None:
        """Force-persist the current state."""
        with self._lock:
            self._dirty = True
            self._maybe_save_locked(force=True)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def aclose(self) -> None:
        """Drain the publisher, flush state, mark closed."""
        if self._closed:
            return
        self._closed = True
        try:
            await self.stop_publisher(drain=True)
        except Exception as exc:
            log_event("warning", f"Failed to stop publisher: {exc}")
        try:
            await self.flush()
        except Exception as exc:
            log_event("warning", f"Failed to flush drift state: {exc}")

    async def __aenter__(self) -> "PolicyDriftDetector":
        await self.start_publisher()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()
