"""
Drift detection for adaptive cost function.
Detects sudden shifts in cost weights and triggers rollback to a safe snapshot.
"""
import hashlib
import time
from typing import Dict, Optional
from ..storage import Storage
from ..config import config
from ..logger import logger

class DriftDetector:
    """Detects policy drift and manages rollback checkpoints."""

    def __init__(self, storage: Storage, adaptive_cost):
        self.storage = storage
        self.adaptive_cost = adaptive_cost
        self.threshold = config.DRIFT_THRESHOLD
        self.rollback_enabled = config.ROLLBACK_ENABLED
        self.last_snapshot_time = 0
        self.snapshot_interval = 3600  # take snapshot every hour

    async def check_drift(self, current_weights: Dict[str, float]):
        """Compare current weights with previous snapshot."""
        # 1. Check if it's time to take a new snapshot
        if time.time() - self.last_snapshot_time > self.snapshot_interval:
            await self._take_snapshot(current_weights, "periodic")
            return

        # 2. Load last snapshot
        last_snap = self.storage.get_last_snapshot()
        if not last_snap:
            return

        # 3. Compute Euclidean distance between weight vectors
        prev_weights = self._deserialize_weights(last_snap["online_weights"])
        dist = sum((current_weights[k] - prev_weights.get(k, 0)) ** 2 for k in current_weights) ** 0.5

        if dist > self.threshold:
            logger.warning(f"Drift detected! Distance: {dist:.4f} > threshold {self.threshold}")
            if self.rollback_enabled:
                await self._rollback_to_snapshot(last_snap)
            else:
                # Just log and alert
                logger.error("Drift detected but rollback disabled. Manual intervention required.")

    async def _take_snapshot(self, weights: Dict[str, float], reason: str):
        """Save current weights as a snapshot."""
        snapshot_id = hashlib.sha256(f"{time.time()}{weights}".encode()).hexdigest()[:16]
        # Serialize weights to bytes
        import pickle
        online_bytes = pickle.dumps(weights)
        offline_bytes = pickle.dumps({})  # placeholder for offline weights
        self.storage.save_drift_snapshot(
            snapshot_id, online_bytes, offline_bytes,
            sum(weights.values()), reason
        )
        self.last_snapshot_time = time.time()
        logger.info(f"Snapshot taken: {snapshot_id}")

    async def _rollback_to_snapshot(self, snapshot: Dict):
        """Restore weights from snapshot."""
        import pickle
        online_weights = pickle.loads(bytes.fromhex(snapshot["online_weights"]))
        # Restore the online manager's weights
        for k, v in online_weights.items():
            if k in self.adaptive_cost.online.weights:
                self.adaptive_cost.online.weights[k] = v
        logger.info(f"Rolled back to snapshot {snapshot['snapshot_id']}")

    def _deserialize_weights(self, hex_str: str) -> Dict:
        import pickle
        return pickle.loads(bytes.fromhex(hex_str))
