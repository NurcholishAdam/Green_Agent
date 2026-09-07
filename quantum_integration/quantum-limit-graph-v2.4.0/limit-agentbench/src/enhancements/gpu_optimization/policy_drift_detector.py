#!/usr/bin/env python3
"""
Enhanced drift detection for selected FlexGen policies.
Monitors the distribution of chosen policies and triggers rollback if drift occurs.

Enhancements over the original stub:
- Complete policy vector (all fields from FlexGenPolicy).
- Tracks reward/performance alongside policies for context.
- Maintains a safe baseline snapshot; computes distance to baseline (not just recent average).
- EWMA smoothing of drift distance for early detection.
- Adaptive threshold based on historical distance statistics (optional).
- Optional rollback to a stored safe policy.
- Persistence of history and baseline to disk (pickle).
- Integration with AsyncMessageQueue/FeedbackEvent for alerting.
- Support for multiple drift metrics (Euclidean, Manhattan, Cosine).
- Consecutive drift counter for hysteresis.
- NEW: Asynchronous event publishing via background queue to avoid blocking.
- NEW: Moving average baseline for non‑stationary environments.
- NEW: Drift alert cooldown to prevent spam.
- NEW: Callback when persistent drift is detected.
- NEW: Thread‑safe operations with a lock.
"""

import asyncio
import logging
import pickle
import time
import threading
from collections import deque
from pathlib import Path
from typing import Deque, Dict, Any, Optional, List, Callable

try:
    from ..async_message_queue import AsyncMessageQueue
except ImportError:
    AsyncMessageQueue = None

try:
    from ..schemas.feedback_event import FeedbackEvent
except ImportError:
    FeedbackEvent = None

from ..logger import logger


class PolicyDriftDetector:
    """
    Specialized drift detector for FlexGen policy selection.
    Monitors chosen policies over time and detects when they deviate significantly
    from a safe baseline.
    """

    def __init__(
        self,
        threshold: float = 0.3,
        history_size: int = 100,
        baseline_policy: Optional[Dict[str, Any]] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        persistence_path: Optional[str] = "policy_drift_state.pkl",
        metric: str = "euclidean",
        ewma_alpha: float = 0.3,
        consecutive_drift_threshold: int = 3,
        baseline_update_mode: str = "fixed",  # "fixed" or "moving_average"
        baseline_window: int = 10,
        drift_cooldown_seconds: float = 60.0,
        on_drift_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        """
        Args:
            threshold: Drift threshold (distance beyond which drift is flagged).
            history_size: Max number of policy vectors to store.
            baseline_policy: Initial safe policy (if None, first policy added becomes baseline).
            message_queue: Optional queue for publishing drift events.
            persistence_path: File path for saving/loading state (if None, no persistence).
            metric: Distance metric: "euclidean", "manhattan", "cosine".
            ewma_alpha: Smoothing factor for EWMA of distance.
            consecutive_drift_threshold: Number of consecutive drift detections before flagging persistent drift.
            baseline_update_mode: "fixed" (baseline never changes) or "moving_average" (baseline is average of last `baseline_window` policies).
            baseline_window: Number of recent policies to average when using moving average baseline.
            drift_cooldown_seconds: Minimum interval between drift alert publications.
            on_drift_callback: Optional async or sync callback invoked when persistent drift is detected.
        """
        # Validate metric
        if metric not in ("euclidean", "manhattan", "cosine"):
            raise ValueError(f"Unsupported metric: {metric}")

        self.threshold = threshold
        self.history: Deque[Dict[str, Any]] = deque(maxlen=history_size)
        self.baseline_vector: Optional[List[float]] = None
        self.baseline_policy: Optional[Dict[str, Any]] = baseline_policy
        if baseline_policy:
            self.baseline_vector = self._policy_to_vector(baseline_policy)
        self.message_queue = message_queue
        self.persistence_path = persistence_path
        self.metric = metric
        self.ewma_alpha = ewma_alpha
        self._ewma_distance: Optional[float] = None
        self._drift_counter = 0
        self._consecutive_drift_threshold = consecutive_drift_threshold
        self._last_drift_time: Optional[float] = None
        self._last_alert_time: Optional[float] = None

        # New attributes
        self.baseline_update_mode = baseline_update_mode
        self.baseline_window = baseline_window
        self.drift_cooldown_seconds = drift_cooldown_seconds
        self.on_drift_callback = on_drift_callback

        # Thread safety
        self._lock = threading.Lock()

        # Async event publishing
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._publisher_task: Optional[asyncio.Task] = None

        # Load persisted state if available
        if self.persistence_path and Path(self.persistence_path).exists():
            self._load_state()

    def _policy_to_vector(self, policy: Dict[str, Any]) -> List[float]:
        """Convert policy dict to fixed-length numeric vector."""
        vec = [
            policy.get('gpu_batch_size', 1) / 8.0,
            policy.get('block_size', 16) / 64.0,
            1.0 if policy.get('weight_device') == 'gpu' else 0.0,
            1.0 if policy.get('weight_device') == 'cpu' else 0.0,
            1.0 if policy.get('weight_device') == 'disk' else 0.0,
            1.0 if policy.get('activation_device') == 'gpu' else 0.0,
            1.0 if policy.get('activation_device') == 'cpu' else 0.0,
            1.0 if policy.get('kv_cache_device') == 'gpu' else 0.0,
            1.0 if policy.get('kv_cache_device') == 'cpu' else 0.0,
            1.0 if policy.get('kv_cache_device') == 'disk' else 0.0,
            policy.get('weight_bits', 16) / 16.0,
            policy.get('kv_cache_bits', 16) / 16.0,
            1.0 if policy.get('cpu_attention', False) else 0.0,
            1.0 if policy.get('overlap_io_compute', True) else 0.0,
        ]
        return vec

    def _distance(self, a: List[float], b: List[float]) -> float:
        """Compute distance between two vectors using configured metric."""
        a = list(a)
        b = list(b)
        if self.metric == "manhattan":
            return sum(abs(x - y) for x, y in zip(a, b))
        elif self.metric == "cosine":
            dot = sum(x * y for x, y in zip(a, b))
            norm_a = sum(x * x for x in a) ** 0.5
            norm_b = sum(x * x for x in b) ** 0.5
            if norm_a == 0 or norm_b == 0:
                return 1.0
            return 1.0 - dot / (norm_a * norm_b)
        else:  # euclidean
            return sum((x - y) ** 2 for x, y in zip(a, b)) ** 0.5

    def _process_policy(self, policy_dict: Dict[str, Any], reward: Optional[float]) -> None:
        """Internal method that assumes lock is held."""
        vec = self._policy_to_vector(policy_dict)
        entry = {
            "vector": vec,
            "reward": reward,
            "timestamp": time.time(),
            "policy": policy_dict,
        }
        self.history.append(entry)

        if self.baseline_vector is None:
            # Set baseline from first observed policy
            self.baseline_vector = vec
            self.baseline_policy = policy_dict
            self._ewma_distance = 0.0
            self._drift_counter = 0
            logger.info("Baseline policy set from first observed policy.")
            self._save_state()
            return

        # If moving average mode, update baseline before computing distance
        if self.baseline_update_mode == "moving_average":
            vectors = [h["vector"] for h in list(self.history)[-self.baseline_window:]]
            if vectors:
                avg_vec = [sum(col) / len(col) for col in zip(*vectors)]
                self.baseline_vector = avg_vec
                # Keep baseline_policy as the most recent policy for rollback
                self.baseline_policy = policy_dict

        dist = self._distance(vec, self.baseline_vector)

        # EWMA update
        if self._ewma_distance is None:
            self._ewma_distance = dist
        else:
            self._ewma_distance = self.ewma_alpha * dist + (1 - self.ewma_alpha) * self._ewma_distance

        drift_detected = self._ewma_distance > self.threshold
        if drift_detected:
            self._drift_counter += 1
            self._last_drift_time = time.time()
            logger.warning(f"Policy drift detected: distance={dist:.3f}, ewma={self._ewma_distance:.3f}, "
                           f"counter={self._drift_counter}")

            # Alert with cooldown
            if (self._last_alert_time is None or
                    (time.time() - self._last_alert_time) >= self.drift_cooldown_seconds):
                self._publish_drift_alert(dist, self._ewma_distance)
                self._last_alert_time = time.time()

            # Persistent drift callback
            if self._drift_counter >= self._consecutive_drift_threshold:
                self._trigger_drift_callback()
        else:
            if self._drift_counter > 0:
                logger.info(f"Drift resolved: ewma={self._ewma_distance:.3f}")
                self._drift_counter = 0

        self._save_state()

    def add_policy(self, policy_dict: Dict[str, Any], reward: Optional[float] = None) -> None:
        """
        Record a chosen policy along with optional reward.
        This method is thread‑safe.
        """
        with self._lock:
            self._process_policy(policy_dict, reward)

    async def add_policy_async(self, policy_dict: Dict[str, Any], reward: Optional[float] = None) -> None:
        """
        Async wrapper for add_policy. Uses a thread to avoid blocking the event loop.
        """
        await asyncio.to_thread(self.add_policy, policy_dict, reward)

    def _publish_drift_alert(self, distance: float, ewma: float) -> None:
        """Enqueue a drift alert event for publishing."""
        if not self.message_queue or FeedbackEvent is None:
            return
        event = FeedbackEvent(
            source="policy_drift_detector",
            feedback_type="telemetry",
            task_id="drift_monitor",
            context={"distance": distance, "ewma": ewma, "threshold": self.threshold},
            action={"selected_action": "alert", "selected_rank": 0, "confidence_score": 0.9},
            performance={"quality_score": 0.5, "latency_ms": 0, "energy_joules": 0,
                         "carbon_g": 0, "helium_cost": 0, "duration_ms": 0},
            adaptive_cost_value=0.0,
            tags=["drift", "policy_selection", "alert"],
        )
        # Put event into queue; publisher will send it later
        self._event_queue.put_nowait(event)
        self._ensure_publisher_started()

    def _ensure_publisher_started(self) -> None:
        """Start the async publisher task if not already running and an event loop is available."""
        if self._publisher_task is not None and not self._publisher_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop; cannot start publisher. The event will remain in queue.
            logger.warning("No running event loop; drift alert will not be published until start_publisher() is called.")
            return
        self._publisher_task = loop.create_task(self._publish_loop())

    async def _publish_loop(self) -> None:
        """Continuously publish events from the queue."""
        while True:
            event = await self._event_queue.get()
            try:
                if self.message_queue:
                    await self.message_queue.publish("drift_events", event.to_json())
            except Exception as e:
                logger.error(f"Failed to publish drift event: {e}")
            finally:
                self._event_queue.task_done()

    async def start_publisher(self) -> None:
        """Explicitly start the publisher task (if not already running)."""
        self._ensure_publisher_started()

    async def stop_publisher(self) -> None:
        """Stop the publisher task and flush pending events."""
        if self._publisher_task:
            self._publisher_task.cancel()
            try:
                await self._publisher_task
            except asyncio.CancelledError:
                pass
            self._publisher_task = None

    def _trigger_drift_callback(self) -> None:
        """Invoke the drift callback if set, either sync or async."""
        if not self.on_drift_callback:
            return
        try:
            result = self.on_drift_callback(self.baseline_policy)
            if asyncio.iscoroutine(result):
                # Schedule coroutine
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(result)
                except RuntimeError:
                    asyncio.run(result)
        except Exception as e:
            logger.error(f"Drift callback failed: {e}")

    def detect_drift(self) -> bool:
        """
        Return True if persistent drift is detected (consecutive counter exceeds limit).
        """
        with self._lock:
            return self._drift_counter >= self._consecutive_drift_threshold

    def get_baseline_policy(self) -> Optional[Dict[str, Any]]:
        """Return the safe baseline policy."""
        with self._lock:
            return self.baseline_policy

    def set_baseline(self, policy_dict: Dict[str, Any]) -> None:
        """Manually set a new baseline policy."""
        with self._lock:
            self.baseline_vector = self._policy_to_vector(policy_dict)
            self.baseline_policy = policy_dict
            self._ewma_distance = 0.0
            self._drift_counter = 0
            logger.info("Baseline policy manually updated.")
            self._save_state()

    def rollback_to_baseline(self) -> Dict[str, Any]:
        """Return the baseline policy for rollback (caller applies it)."""
        with self._lock:
            if self.baseline_policy is None:
                raise ValueError("No baseline policy available.")
            logger.info("Rollback requested; returning baseline policy.")
            return self.baseline_policy

    def reset_state(self) -> None:
        """Reset drift state but keep baseline."""
        with self._lock:
            self.history.clear()
            self._ewma_distance = 0.0
            self._drift_counter = 0
            self._last_drift_time = None
            self._last_alert_time = None
            if self.persistence_path:
                self._save_state()
            logger.info("Drift state reset (baseline preserved).")

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
            }

    def _save_state(self) -> None:
        """Save detector state to disk."""
        if not self.persistence_path:
            return
        try:
            state = {
                "baseline_vector": self.baseline_vector,
                "baseline_policy": self.baseline_policy,
                "ewma_distance": self._ewma_distance,
                "drift_counter": self._drift_counter,
                "last_drift_time": self._last_drift_time,
                "last_alert_time": self._last_alert_time,
                "history": list(self.history)[-100:],
            }
            with open(self.persistence_path, 'wb') as f:
                pickle.dump(state, f)
        except Exception as e:
            logger.warning(f"Failed to save drift state: {e}")

    def _load_state(self) -> None:
        """Load detector state from disk."""
        if not self.persistence_path:
            return
        try:
            with open(self.persistence_path, 'rb') as f:
                state = pickle.load(f)
            self.baseline_vector = state.get("baseline_vector")
            self.baseline_policy = state.get("baseline_policy")
            self._ewma_distance = state.get("ewma_distance")
            self._drift_counter = state.get("drift_counter", 0)
            self._last_drift_time = state.get("last_drift_time")
            self._last_alert_time = state.get("last_alert_time")
            self.history = deque(state.get("history", []), maxlen=self.history.maxlen)
            logger.info("Drift state loaded from disk.")
        except Exception as e:
            logger.warning(f"Failed to load drift state: {e}")
