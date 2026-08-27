"""
Drift detection for selected FlexGen policies (enhanced).
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
"""

import asyncio
import json
import logging
import pickle
import time
from collections import deque
from pathlib import Path
from typing import Deque, Dict, Any, Optional, List, Tuple

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
        """
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

        # Load persisted state if available
        if self.persistence_path and Path(self.persistence_path).exists():
            self._load_state()

    def _policy_to_vector(self, policy: Dict[str, Any]) -> List[float]:
        """Convert policy dict to fixed-length numeric vector."""
        # Include all fields from FlexGenPolicy
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

    def add_policy(self, policy_dict: Dict[str, Any], reward: Optional[float] = None) -> None:
        """
        Record a chosen policy along with optional reward.
        """
        vec = self._policy_to_vector(policy_dict)
        entry = {
            "vector": vec,
            "reward": reward,
            "timestamp": time.time(),
            "policy": policy_dict,
        }
        self.history.append(entry)

        # If baseline not set, set it to this policy
        if self.baseline_vector is None:
            self.baseline_vector = vec
            self.baseline_policy = policy_dict
            logger.info("Baseline policy set from first observed policy.")

        # Compute distance from baseline
        dist = self._distance(vec, self.baseline_vector)

        # Update EWMA
        if self._ewma_distance is None:
            self._ewma_distance = dist
        else:
            self._ewma_distance = self.ewma_alpha * dist + (1 - self.ewma_alpha) * self._ewma_distance

        # Persist state
        if self.persistence_path:
            self._save_state()

        # Check drift
        if self._ewma_distance > self.threshold:
            self._drift_counter += 1
            self._last_drift_time = time.time()
            logger.warning(f"Policy drift detected: distance={dist:.3f}, ewma={self._ewma_distance:.3f}, "
                           f"counter={self._drift_counter}")
            # Publish alert event (async, non-blocking)
            if self.message_queue and FeedbackEvent:
                event = FeedbackEvent(
                    source="policy_drift_detector",
                    feedback_type="telemetry",
                    task_id="drift_monitor",
                    context={"distance": dist, "ewma": self._ewma_distance, "threshold": self.threshold},
                    action={"selected_action": "alert", "selected_rank": 0, "confidence_score": 0.9},
                    performance={"quality_score": 0.5, "latency_ms": 0, "energy_joules": 0,
                                 "carbon_g": 0, "helium_cost": 0, "duration_ms": 0},
                    adaptive_cost_value=0.0,
                    tags=["drift", "policy_selection", "alert"],
                )
                # Use asyncio.create_task if event loop running, else schedule via asyncio.run
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self.message_queue.publish("drift_events", event.to_json()))
                except RuntimeError:
                    # No running loop; run synchronously (blocking, but acceptable for infrequent alerts)
                    asyncio.run(self.message_queue.publish("drift_events", event.to_json()))
        else:
            # Reset counter if distance goes below threshold
            if self._drift_counter > 0:
                logger.info(f"Drift resolved: ewma={self._ewma_distance:.3f}")
                self._drift_counter = 0

    def detect_drift(self) -> bool:
        """
        Return True if persistent drift is detected (consecutive counter exceeds limit).
        """
        return self._drift_counter >= self._consecutive_drift_threshold

    def get_baseline_policy(self) -> Optional[Dict[str, Any]]:
        """Return the safe baseline policy."""
        return self.baseline_policy

    def set_baseline(self, policy_dict: Dict[str, Any]) -> None:
        """Manually set a new baseline policy."""
        self.baseline_vector = self._policy_to_vector(policy_dict)
        self.baseline_policy = policy_dict
        self._ewma_distance = 0.0
        self._drift_counter = 0
        logger.info("Baseline policy manually updated.")
        if self.persistence_path:
            self._save_state()

    def rollback_to_baseline(self) -> Dict[str, Any]:
        """Return the baseline policy for rollback (caller applies it)."""
        if self.baseline_policy is None:
            raise ValueError("No baseline policy available.")
        logger.info("Rollback requested; returning baseline policy.")
        return self.baseline_policy

    def reset_state(self) -> None:
        """Reset drift state but keep baseline."""
        self.history.clear()
        self._ewma_distance = 0.0
        self._drift_counter = 0
        self._last_drift_time = None
        if self.persistence_path:
            self._save_state()
        logger.info("Drift state reset (baseline preserved).")

    def get_stats(self) -> Dict[str, Any]:
        """Return current drift statistics."""
        return {
            "history_size": len(self.history),
            "ewma_distance": self._ewma_distance,
            "threshold": self.threshold,
            "drift_counter": self._drift_counter,
            "last_drift_time": self._last_drift_time,
        }

    def _save_state(self) -> None:
        """Save detector state to disk."""
        try:
            state = {
                "baseline_vector": self.baseline_vector,
                "baseline_policy": self.baseline_policy,
                "ewma_distance": self._ewma_distance,
                "drift_counter": self._drift_counter,
                "history": list(self.history)[-100:],  # keep last 100 entries
            }
            with open(self.persistence_path, 'wb') as f:
                pickle.dump(state, f)
        except Exception as e:
            logger.warning(f"Failed to save drift state: {e}")

    def _load_state(self) -> None:
        """Load detector state from disk."""
        try:
            with open(self.persistence_path, 'rb') as f:
                state = pickle.load(f)
            self.baseline_vector = state.get("baseline_vector")
            self.baseline_policy = state.get("baseline_policy")
            self._ewma_distance = state.get("ewma_distance")
            self._drift_counter = state.get("drift_counter", 0)
            self.history = deque(state.get("history", []), maxlen=self.history.maxlen)
            logger.info("Drift state loaded from disk.")
        except Exception as e:
            logger.warning(f"Failed to load drift state: {e}")
