"""
Drift Detection for Adaptive Cost Function
===========================================
Detects sudden shifts in cost weights and triggers rollback to a safe snapshot.
Enhanced with configurable intervals, hysteresis, drift event logging, and metrics.

Enhancements implemented:
- Multiple distance metrics (Euclidean, Manhattan, Cosine, Relative)
- Adaptive threshold based on rolling distance history
- EWMA smoothing for early detection of gradual drift
- Weighted distance to prioritize important dimensions
- JSON serialization for portability and safety
- In-memory caching of last snapshot for performance
- Additional configuration options (metric, alpha, history size, weights)
"""
import hashlib
import time
import json
from typing import Dict, Optional, List, Any
from collections import deque
import numpy as np

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
        - Drift detection using configurable distance metrics.
        - Adaptive threshold or static threshold.
        - EWMA smoothing for early drift detection.
        - Hysteresis: requires N consecutive drift detections before rollback.
        - Persistent log of drift events in SQLite.
        - Manual snapshot trigger.
        - Rollback restores online weights and resets detection state.
    """

    def __init__(
        self,
        storage: Storage,
        adaptive_cost,
        metrics_registry=None,
        metric: Optional[str] = None,
        use_adaptive_threshold: Optional[bool] = None,
        ewma_alpha: Optional[float] = None,
        weight_importance: Optional[Dict[str, float]] = None,
        distance_history_size: Optional[int] = None,
    ):
        """
        Args:
            storage: Storage instance for snapshots and event logs.
            adaptive_cost: AdaptiveCostFunction instance (must have online.weights).
            metrics_registry: Optional MetricsRegistry for Prometheus counters.
            metric: Distance metric to use ('euclidean', 'manhattan', 'cosine', 'relative').
                    Defaults to config.DRIFT_METRIC or 'euclidean'.
            use_adaptive_threshold: If True, threshold is computed from rolling distance history.
                    Defaults to config.DRIFT_USE_ADAPTIVE_THRESHOLD or False.
            ewma_alpha: Smoothing factor for EWMA (0 < alpha <= 1). Larger alpha gives more weight to recent distances.
                    Defaults to config.DRIFT_EWMA_ALPHA or 0.3.
            weight_importance: Optional dict mapping weight keys to importance multipliers for weighted distance.
                    Defaults to config.DRIFT_WEIGHT_IMPORTANCE or {} (all equal).
            distance_history_size: Number of recent distances to keep for adaptive threshold.
                    Defaults to config.DRIFT_HISTORY_SIZE or 100.
        """
        self.storage = storage
        self.adaptive_cost = adaptive_cost
        self.threshold = config.DRIFT_THRESHOLD
        self.rollback_enabled = config.ROLLBACK_ENABLED
        self.snapshot_interval = config.DRIFT_SNAPSHOT_INTERVAL or 3600  # seconds
        self.hysteresis_count = config.DRIFT_HYSTERESIS_COUNT or 1  # consecutive detections before rollback
        self.last_snapshot_time = 0
        self._drift_counter = 0  # consecutive drift detections

        # New configuration options
        self.metric = metric or getattr(config, 'DRIFT_METRIC', 'euclidean')
        self.use_adaptive_threshold = use_adaptive_threshold or getattr(config, 'DRIFT_USE_ADAPTIVE_THRESHOLD', False)
        self.ewma_alpha = ewma_alpha if ewma_alpha is not None else getattr(config, 'DRIFT_EWMA_ALPHA', 0.3)
        self.weight_importance = weight_importance or getattr(config, 'DRIFT_WEIGHT_IMPORTANCE', {})
        self.distance_history_size = distance_history_size or getattr(config, 'DRIFT_HISTORY_SIZE', 100)
        self.distance_history = deque(maxlen=self.distance_history_size)
        self._ewma_distance = 0.0
        self._last_snapshot_cache = None  # Cache for last snapshot

        # Validate metric
        valid_metrics = {'euclidean', 'manhattan', 'cosine', 'relative'}
        if self.metric not in valid_metrics:
            logger.warning(f"Invalid metric '{self.metric}', falling back to 'euclidean'.")
            self.metric = 'euclidean'

        # Prometheus metric (if available)
        self.drift_counter_metric = None
        if PROMETHEUS_AVAILABLE and metrics_registry:
            self.drift_counter_metric = Counter(
                'green_agent_drift_detections_total',
                'Total number of drift detections',
                registry=metrics_registry.registry if hasattr(metrics_registry, 'registry') else None
            )

        # Attempt to load last snapshot time and cache from storage
        self._load_last_snapshot_time()

    def _load_last_snapshot_time(self):
        """Load the timestamp of the last snapshot from storage and cache it."""
        try:
            last_snap = self.storage.get_last_snapshot()
            if last_snap:
                self.last_snapshot_time = last_snap.get('timestamp', 0)
                self._last_snapshot_cache = last_snap
        except Exception as e:
            logger.warning(f"Failed to load last snapshot time: {e}")

    def _get_last_snapshot(self) -> Optional[Dict]:
        """Return cached last snapshot, or fetch from storage if cache is empty."""
        if self._last_snapshot_cache is None:
            self._last_snapshot_cache = self.storage.get_last_snapshot()
            if self._last_snapshot_cache:
                self.last_snapshot_time = self._last_snapshot_cache.get('timestamp', 0)
        return self._last_snapshot_cache

    async def check_drift(self, current_weights: Dict[str, float]) -> None:
        """
        Compare current weights with the last snapshot.
        If drift is detected (distance > threshold), increment counter.
        If counter reaches hysteresis_count, trigger rollback.
        Periodically take new snapshots.
        """
        # 1. Load last snapshot (cached)
        last_snap = self._get_last_snapshot()
        if not last_snap:
            # No snapshot yet – take one now
            await self._take_snapshot(current_weights, "initial")
            return

        # 2. Compute distance using configured metric and weight importance
        prev_weights = self._deserialize_weights(last_snap["online_weights"])
        dist = self._distance(current_weights, prev_weights, self.metric, self.weight_importance)

        # Update EWMA
        self._ewma_distance = self.ewma_alpha * dist + (1 - self.ewma_alpha) * self._ewma_distance

        # Determine effective threshold (adaptive or static)
        effective_threshold = self.threshold
        if self.use_adaptive_threshold:
            effective_threshold = self._compute_adaptive_threshold()

        # 3. Check if snapshot interval elapsed – take new snapshot if due
        if time.time() - self.last_snapshot_time > self.snapshot_interval:
            await self._take_snapshot(current_weights, "periodic")

        # 4. Drift detection using EWMA distance (or raw distance if alpha=1)
        drift_metric = self._ewma_distance if self.ewma_alpha < 1.0 else dist
        if drift_metric > effective_threshold:
            self._drift_counter += 1
            logger.warning(f"Drift detected! Distance: {drift_metric:.4f} (threshold {effective_threshold:.4f})")
            if self.drift_counter_metric:
                self.drift_counter_metric.inc()

            # Log drift event
            await self._log_drift_event(
                drift_metric, effective_threshold, self._drift_counter, rollback_triggered=False
            )

            if self._drift_counter >= self.hysteresis_count:
                logger.warning(f"Drift persisted for {self._drift_counter} consecutive detections. Triggering rollback.")
                if self.rollback_enabled:
                    await self._rollback_to_snapshot(last_snap)
                    self._drift_counter = 0
                    await self._log_drift_event(
                        drift_metric, effective_threshold, self._drift_counter, rollback_triggered=True
                    )
                else:
                    logger.error("Drift detected but rollback disabled. Manual intervention required.")
        else:
            # Reset counter if no drift
            if self._drift_counter > 0:
                logger.info(f"Drift resolved. Distance {drift_metric:.4f} below threshold {effective_threshold:.4f}.")
                self._drift_counter = 0
            # Only add to history when no drift (to avoid contaminating adaptive threshold)
            self.distance_history.append(dist)

    def _compute_adaptive_threshold(self) -> float:
        """
        Compute threshold based on mean + k*std of recent distances.
        If insufficient history, return static threshold.
        """
        if len(self.distance_history) < 10:  # need minimum samples
            return self.threshold
        arr = np.array(self.distance_history)
        mean = arr.mean()
        std = arr.std()
        k = getattr(config, 'DRIFT_ADAPTIVE_K', 3.0)  # number of std deviations
        return mean + k * std

    async def _take_snapshot(self, weights: Dict[str, float], reason: str) -> None:
        """
        Save current weights as a snapshot.
        This also updates the last_snapshot_time and cache.
        """
        snapshot_id = hashlib.sha256(f"{time.time()}{weights}".encode()).hexdigest()[:16]
        # Serialize weights to JSON string (safe and portable)
        online_json = self._serialize_weights(weights)
        offline_json = self._serialize_weights({})  # placeholder for offline weights
        cost_score = sum(weights.values())  # or any other metric
        self.storage.save_drift_snapshot(
            snapshot_id,
            online_json,
            offline_json,
            cost_score,
            reason
        )
        self.last_snapshot_time = time.time()
        # Update cache with the new snapshot
        self._last_snapshot_cache = {
            "snapshot_id": snapshot_id,
            "online_weights": online_json,
            "offline_weights": offline_json,
            "timestamp": self.last_snapshot_time,
            "reason": reason,
            "cost_score": cost_score,
        }
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
            # Clear drift counter and EWMA
            self._drift_counter = 0
            self._ewma_distance = 0.0
            # Update cache to reflect restored state
            self._last_snapshot_cache = snapshot
            logger.info(f"Rolled back to snapshot {snapshot['snapshot_id']}")
        except Exception as e:
            logger.error(f"Rollback failed: {e}")

    async def _log_drift_event(
        self, distance: float, threshold: float, consecutive: int, rollback_triggered: bool
    ) -> None:
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

    def _distance(
        self,
        a: Dict[str, float],
        b: Dict[str, float],
        metric: str = "euclidean",
        importance: Optional[Dict[str, float]] = None,
    ) -> float:
        """
        Compute distance between two weight vectors using the specified metric.
        importance: optional dict mapping keys to multiplier (default 1.0).
        Missing keys are treated as 0.
        """
        all_keys = set(a.keys()) | set(b.keys())
        if not all_keys:
            return 0.0

        imp = importance or {}
        # Helper to get importance for a key
        def get_imp(k):
            return imp.get(k, 1.0)

        if metric == "euclidean":
            total = 0.0
            for k in all_keys:
                diff = (a.get(k, 0.0) - b.get(k, 0.0)) * get_imp(k)
                total += diff * diff
            return total ** 0.5
        elif metric == "manhattan":
            total = 0.0
            for k in all_keys:
                total += abs(a.get(k, 0.0) - b.get(k, 0.0)) * get_imp(k)
            return total
        elif metric == "cosine":
            dot = sum(a.get(k, 0.0) * b.get(k, 0.0) * get_imp(k) for k in all_keys)
            norm_a = sum((a.get(k, 0.0) * get_imp(k)) ** 2 for k in all_keys) ** 0.5
            norm_b = sum((b.get(k, 0.0) * get_imp(k)) ** 2 for k in all_keys) ** 0.5
            if norm_a == 0 or norm_b == 0:
                return 1.0  # maximum dissimilarity
            return 1.0 - dot / (norm_a * norm_b)
        elif metric == "relative":
            total = 0.0
            count = 0
            for k in all_keys:
                va = a.get(k, 0.0)
                vb = b.get(k, 0.0)
                if va == 0 and vb == 0:
                    continue
                denom = max(abs(va), abs(vb))
                total += (abs(va - vb) / denom) * get_imp(k)
                count += 1
            return total / count if count > 0 else 0.0
        else:
            # fallback to euclidean
            return self._distance(a, b, "euclidean", importance)

    def _serialize_weights(self, weights: Dict[str, float]) -> str:
        """Convert weight dict to JSON string."""
        return json.dumps(weights)

    def _deserialize_weights(self, json_str: str) -> Dict:
        """Convert JSON string back to dict."""
        return json.loads(json_str)

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
        last_snap = self._get_last_snapshot()
        if not last_snap:
            logger.warning("No snapshot available for rollback.")
            return False
        await self._rollback_to_snapshot(last_snap)
        return True

    def get_drift_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Retrieve recent drift events from storage."""
        return self.storage.get_drift_events(limit=limit)

    def reset_state(self) -> None:
        """Reset the internal drift counter, EWMA, and last snapshot time (does not clear snapshots)."""
        self._drift_counter = 0
        self._ewma_distance = 0.0
        self.last_snapshot_time = 0
        self.distance_history.clear()
        logger.info("Drift detector state reset.")
