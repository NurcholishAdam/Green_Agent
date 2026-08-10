"""
Drift Detection for Adaptive Cost Function
===========================================
Detects sudden shifts in cost weights and triggers rollback to a safe snapshot.
Enhanced with configurable intervals, hysteresis, drift event logging, and metrics.
"""
import hashlib
import time
import pickle
from typing import Dict, Optional, List, Any

from ..storage import Storage
from ..config import config
from ..logger import logger

# Optional Prometheus metric
try:
    from prometheus_client import Counter
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


class DriftDetector:
    """
    Detects policy drift and manages rollback checkpoints.

    Features:
        - Periodic snapshots (configurable interval).
        - Drift detection using Euclidean distance between current and last snapshot weights.
        - Hysteresis: requires N consecutive drift detections before rollback.
        - Persistent log of drift events in SQLite.
        - Manual snapshot trigger.
        - Rollback restores online weights and resets detection state.
    """

    def __init__(self, storage: Storage, adaptive_cost, metrics_registry=None):
        """
        Args:
            storage: Storage instance for snapshots and event logs.
            adaptive_cost: AdaptiveCostFunction instance (must have online.weights).
            metrics_registry: Optional MetricsRegistry for Prometheus counters.
        """
        self.storage = storage
        self.adaptive_cost = adaptive_cost
        self.threshold = config.DRIFT_THRESHOLD
        self.rollback_enabled = config.ROLLBACK_ENABLED
        self.snapshot_interval = config.DRIFT_SNAPSHOT_INTERVAL or 3600  # seconds
        self.hysteresis_count = config.DRIFT_HYSTERESIS_COUNT or 1  # consecutive detections before rollback
        self.last_snapshot_time = 0
        self._drift_counter = 0  # consecutive drift detections

        # Prometheus metric (if available)
        self.drift_counter_metric = None
        if PROMETHEUS_AVAILABLE and metrics_registry:
            self.drift_counter_metric = Counter(
                'green_agent_drift_detections_total',
                'Total number of drift detections',
                registry=metrics_registry.registry if hasattr(metrics_registry, 'registry') else None
            )

        # Attempt to load last snapshot time from storage (if saved)
        self._load_last_snapshot_time()

    def _load_last_snapshot_time(self):
        """Load the timestamp of the last snapshot from storage (if saved)."""
        try:
            last_snap = self.storage.get_last_snapshot()
            if last_snap:
                self.last_snapshot_time = last_snap.get('timestamp', 0)
        except Exception as e:
            logger.warning(f"Failed to load last snapshot time: {e}")

    async def check_drift(self, current_weights: Dict[str, float]) -> None:
        """
        Compare current weights with the last snapshot.
        If drift is detected (distance > threshold), increment counter.
        If counter reaches hysteresis_count, trigger rollback.
        Periodically take new snapshots.
        """
        # 1. Load last snapshot (if exists)
        last_snap = self.storage.get_last_snapshot()
        if not last_snap:
            # No snapshot yet – take one now
            await self._take_snapshot(current_weights, "initial")
            return

        # 2. Compute distance
        prev_weights = self._deserialize_weights(last_snap["online_weights"])
        dist = self._distance(current_weights, prev_weights)

        # 3. Check if snapshot interval elapsed – take new snapshot if due
        if time.time() - self.last_snapshot_time > self.snapshot_interval:
            await self._take_snapshot(current_weights, "periodic")

        # 4. Drift detection
        if dist > self.threshold:
            self._drift_counter += 1
            logger.warning(f"Drift detected! Distance: {dist:.4f} (threshold {self.threshold})")
            if self.drift_counter_metric:
                self.drift_counter_metric.inc()

            # Log drift event
            await self._log_drift_event(dist, self.threshold, self._drift_counter, rollback_triggered=False)

            if self._drift_counter >= self.hysteresis_count:
                logger.warning(f"Drift persisted for {self._drift_counter} consecutive detections. Triggering rollback.")
                if self.rollback_enabled:
                    await self._rollback_to_snapshot(last_snap)
                    # Reset counter after rollback
                    self._drift_counter = 0
                    # Log that rollback occurred
                    await self._log_drift_event(dist, self.threshold, self._drift_counter, rollback_triggered=True)
                else:
                    logger.error("Drift detected but rollback disabled. Manual intervention required.")
                    # Optionally alert via other channels
        else:
            # Reset counter if no drift
            if self._drift_counter > 0:
                logger.info(f"Drift resolved. Distance {dist:.4f} below threshold.")
                self._drift_counter = 0

        # 5. Optionally update last snapshot time if we took a new snapshot (already updated in _take_snapshot)

    async def _take_snapshot(self, weights: Dict[str, float], reason: str) -> None:
        """
        Save current weights as a snapshot.
        This also updates the last_snapshot_time.
        """
        snapshot_id = hashlib.sha256(f"{time.time()}{weights}".encode()).hexdigest()[:16]
        # Serialize weights to bytes
        online_bytes = pickle.dumps(weights)
        offline_bytes = pickle.dumps({})  # placeholder for offline weights (could extend)
        cost_score = sum(weights.values())  # or any other metric
        self.storage.save_drift_snapshot(
            snapshot_id,
            online_bytes,
            offline_bytes,
            cost_score,
            reason
        )
        self.last_snapshot_time = time.time()
        logger.info(f"Snapshot taken: {snapshot_id} (reason: {reason})")

    async def _rollback_to_snapshot(self, snapshot: Dict) -> None:
        """
        Restore weights from snapshot and reset state.
        """
        try:
            online_weights = self._deserialize_weights(snapshot["online_weights"])
            # Restore the online manager's weights
            for k, v in online_weights.items():
                if k in self.adaptive_cost.online.weights:
                    self.adaptive_cost.online.weights[k] = v
            # Persist the restored weights
            self.adaptive_cost.online._save_state()
            # Reset the last snapshot time to now to avoid immediate re‑detection
            self.last_snapshot_time = time.time()
            # Clear drift counter
            self._drift_counter = 0
            logger.info(f"Rolled back to snapshot {snapshot['snapshot_id']}")
        except Exception as e:
            logger.error(f"Rollback failed: {e}")

    async def _log_drift_event(self, distance: float, threshold: float, consecutive: int, rollback_triggered: bool) -> None:
        """
        Persist a drift event to the drift_events table.
        """
        try:
            self.storage.log_drift_event({
                "timestamp": time.time(),
                "distance": distance,
                "threshold": threshold,
                "consecutive": consecutive,
                "rollback_triggered": rollback_triggered,
            })
        except Exception as e:
            logger.warning(f"Failed to log drift event: {e}")

    def _distance(self, a: Dict[str, float], b: Dict[str, float]) -> float:
        """
        Compute Euclidean distance between two weight vectors.
        Only considers keys present in both; missing keys default to 0.
        """
        all_keys = set(a.keys()) | set(b.keys())
        total = 0.0
        for k in all_keys:
            diff = a.get(k, 0.0) - b.get(k, 0.0)
            total += diff * diff
        return total ** 0.5

    def _deserialize_weights(self, hex_str: str) -> Dict:
        """Convert hex-encoded pickle back to a dict."""
        return pickle.loads(bytes.fromhex(hex_str))

    # --------------------------------------------------------------------------
    # Public API for external control
    # --------------------------------------------------------------------------
    async def force_snapshot(self) -> None:
        """Manually trigger a snapshot with current weights."""
        current_weights = self.adaptive_cost.get_current_weights()
        await self._take_snapshot(current_weights, "manual")

    async def force_rollback(self) -> bool:
        """
        Manually trigger a rollback to the last snapshot.
        Returns True if successful, False if no snapshot.
        """
        last_snap = self.storage.get_last_snapshot()
        if not last_snap:
            logger.warning("No snapshot available for rollback.")
            return False
        await self._rollback_to_snapshot(last_snap)
        return True

    def get_drift_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Retrieve recent drift events from storage."""
        return self.storage.get_drift_events(limit=limit)

    def reset_state(self) -> None:
        """Reset the internal drift counter and last snapshot time (does not clear snapshots)."""
        self._drift_counter = 0
        self.last_snapshot_time = 0
        logger.info("Drift detector state reset.")
