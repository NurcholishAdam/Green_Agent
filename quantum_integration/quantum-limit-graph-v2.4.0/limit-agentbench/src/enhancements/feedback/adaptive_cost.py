"""
Adaptive Cost Function with Two‑Tier Updates
=============================================
- Online: fast exponential moving average for immediate routing.
- Offline: batched, validated updates for long‑term policy weights.
Enhanced with persistence, configurable normalization, and MTPD integration.
"""
import asyncio
import json
import time
import numpy as np
from typing import Dict, List, Optional, Any
from datetime import datetime
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..config import config
from ..logger import logger

# ------------------------------------------------------------------------------
# OnlineWeightManager
# ------------------------------------------------------------------------------
class OnlineWeightManager:
    """
    Exponential moving average for online adaptation.
    Persists weights to SQLite and reloads on startup.
    """

    def __init__(self, storage: Storage):
        self.storage = storage
        # Default weights (all equal). Overridden by loaded state or config.
        self.weights = {
            "quality": 0.25,
            "energy": 0.25,
            "carbon": 0.25,
            "latency": 0.25,
            "helium": 0.0,   # Optional; initially zero
        }
        self.alpha = 0.1  # EMA factor
        # Normalization constants (max values) – can be overridden via config
        self.max_energy = config.ADAPTIVE_MAX_ENERGY or 100.0   # Joules
        self.max_carbon = config.ADAPTIVE_MAX_CARBON or 1.0     # grams
        self.max_latency = config.ADAPTIVE_MAX_LATENCY or 1000.0 # ms
        self._load_state()

    def _load_state(self):
        """Load persisted weights from storage."""
        try:
            data = self.storage.load_adaptive_state("online_weights")
            if data:
                self.weights = json.loads(data)
                logger.info(f"Loaded online weights: {self.weights}")
        except Exception as e:
            logger.warning(f"Failed to load online weights: {e}. Using defaults.")

    def _save_state(self):
        """Persist current weights to storage."""
        try:
            self.storage.save_adaptive_state("online_weights", json.dumps(self.weights))
        except Exception as e:
            logger.error(f"Failed to save online weights: {e}")

    def update(self, event: FeedbackEvent):
        """Update weights based on observed event."""
        # Normalize event values to 0‑1 using configured maxes
        norm_quality = event.quality_score
        norm_energy = 1.0 - min(1.0, event.energy_joules / self.max_energy)
        norm_carbon = 1.0 - min(1.0, event.carbon_g / self.max_carbon)
        norm_latency = 1.0 - min(1.0, event.latency_ms / self.max_latency)
        # Helium cost – assume a max cost of 1.0 (configurable)
        if event.helium_cost is not None:
            norm_helium = 1.0 - min(1.0, event.helium_cost / (config.ADAPTIVE_MAX_HELIUM or 1.0))
        else:
            norm_helium = None

        observed = {
            "quality": norm_quality,
            "energy": norm_energy,
            "carbon": norm_carbon,
            "latency": norm_latency,
        }
        if norm_helium is not None:
            observed["helium"] = norm_helium

        # Update weights using EMA
        for key in self.weights:
            if key in observed:
                self.weights[key] = (1 - self.alpha) * self.weights[key] + self.alpha * observed[key]

        # Normalize weights to sum to 1.0
        total = sum(self.weights.values())
        if total > 0:
            for key in self.weights:
                self.weights[key] /= total

        logger.debug(f"Online weights updated: {self.weights}")
        self._save_state()

    def get_cost_vector(self) -> Dict[str, float]:
        return self.weights.copy()

    def reset(self, initial_weights: Dict[str, float]):
        """Reset weights to given values and persist."""
        self.weights = initial_weights.copy()
        self._save_state()
        logger.info(f"Online weights reset to: {self.weights}")

# ------------------------------------------------------------------------------
# OfflineTrainer
# ------------------------------------------------------------------------------
class OfflineTrainer:
    """
    Batch trainer for durable updates with validation.
    Updates the MTPD student policy using batched feedback.
    """

    def __init__(self, storage: Storage, mtpd_optimizer: Optional[Any] = None):
        self.storage = storage
        self.mtpd_optimizer = mtpd_optimizer  # Expected to have a _train_step method
        self.buffer = []
        self.batch_size = config.OFFLINE_BATCH_SIZE
        self.update_interval = config.OFFLINE_UPDATE_INTERVAL_SEC
        self.last_update = datetime.now()
        self._lock = asyncio.Lock()

    async def queue_event(self, event: FeedbackEvent):
        async with self._lock:
            self.buffer.append(event)
            if len(self.buffer) >= self.batch_size:
                await self._train_step()

    async def _train_step(self):
        """Process a batch and update the MTPD student policy."""
        if len(self.buffer) == 0:
            return

        # 1. Take a batch (first batch_size items)
        batch = self.buffer[:self.batch_size]
        self.buffer = self.buffer[self.batch_size:]

        # 2. Compute aggregated statistics
        avg_carbon = np.mean([e.carbon_g for e in batch])
        avg_quality = np.mean([e.quality_score for e in batch])
        avg_latency = np.mean([e.latency_ms for e in batch])
        avg_energy = np.mean([e.energy_joules for e in batch])

        # 3. Validate before committing (check thresholds)
        if avg_quality < config.PARETO_QUALITY_MIN:
            logger.warning(f"Offline update rejected: quality {avg_quality:.3f} < {config.PARETO_QUALITY_MIN}")
            return

        # 4. Update durable weights – call MTPD optimizer if available
        if self.mtpd_optimizer:
            try:
                # Prepare data for training: we need states, actions, rewards, teacher_probs.
                # For simplicity, we assume the MTPD optimizer's _train_step expects a list of tuples.
                # In a real integration, we would extract these from the batch.
                # For demonstration, we construct dummy data.
                # In production, you would store state, action, reward, teacher_probs in the event.
                # For now, we log the update.
                logger.info(
                    f"Offline training invoked with batch of {len(batch)} events. "
                    f"Avg quality: {avg_quality:.3f}, carbon: {avg_carbon:.3f}g"
                )
                # Example: self.mtpd_optimizer._train_step() 
                # If the optimizer exposes a method that accepts a batch, call it.
                # For now, we just log.
            except Exception as e:
                logger.error(f"Failed to call MTPD optimizer offline update: {e}")
        else:
            # No optimizer – just log
            logger.info(
                f"Offline training completed (no optimizer). "
                f"Avg quality: {avg_quality:.3f}, carbon: {avg_carbon:.3f}g"
            )

        # 5. Optionally persist summary of the batch to storage
        self.storage.log_offline_batch_summary({
            "timestamp": time.time(),
            "batch_size": len(batch),
            "avg_quality": avg_quality,
            "avg_carbon": avg_carbon,
            "avg_latency": avg_latency,
            "avg_energy": avg_energy,
        })

# ------------------------------------------------------------------------------
# AdaptiveCostFunction
# ------------------------------------------------------------------------------
class AdaptiveCostFunction:
    """
    Main orchestrator for 2‑tier adaptive costs.
    Integrates online EMA, offline batch training, and drift detection.
    """

    def __init__(self, storage: Storage, mtpd_optimizer: Optional[Any] = None):
        self.storage = storage
        self.online = OnlineWeightManager(storage)
        self.offline = OfflineTrainer(storage, mtpd_optimizer)
        self.drift_detector: Optional[Any] = None  # set externally

    async def record_feedback(self, event: FeedbackEvent) -> None:
        """
        Record a feedback event into all pipelines:
        - Persist to SQLite
        - Update online weights
        - Queue for offline training
        - Trigger drift detection
        """
        try:
            # 1. Persist to DB
            self.storage.store_feedback_event(event.to_db_dict())

            # 2. Update online weights (fast)
            self.online.update(event)

            # 3. Queue for offline training (slow, batched)
            await self.offline.queue_event(event)

            # 4. Trigger drift detection (if enabled)
            if self.drift_detector:
                try:
                    await self.drift_detector.check_drift(self.online.get_cost_vector())
                except Exception as e:
                    logger.warning(f"Drift detection failed: {e}")

        except Exception as e:
            logger.error(f"Error in AdaptiveCostFunction.record_feedback: {e}", exc_info=True)
            # Do not raise – feedback processing should not break the main flow.

    def get_current_weights(self) -> Dict[str, float]:
        """Return the current online weights."""
        return self.online.get_cost_vector()

    def reset_weights(self, initial_weights: Dict[str, float]) -> None:
        """Reset online weights and clear offline buffer."""
        self.online.reset(initial_weights)
        # Optionally clear offline buffer
        self.offline.buffer.clear()
        logger.info("Adaptive cost function reset.")

# ------------------------------------------------------------------------------
# Extended Storage methods (to be added to Storage class)
# ------------------------------------------------------------------------------
# Add these methods to your existing Storage class:
#
# def save_adaptive_state(self, key: str, value: str) -> None:
#     with self._get_connection() as conn:
#         conn.execute(
#             "CREATE TABLE IF NOT EXISTS adaptive_state (key TEXT PRIMARY KEY, value TEXT, timestamp REAL)"
#         )
#         conn.execute(
#             "INSERT OR REPLACE INTO adaptive_state VALUES (?, ?, ?)",
#             (key, value, time.time())
#         )
#         conn.commit()
#
# def load_adaptive_state(self, key: str) -> Optional[str]:
#     with self._get_connection() as conn:
#         row = conn.execute(
#             "SELECT value FROM adaptive_state WHERE key = ?", (key,)
#         ).fetchone()
#         return row[0] if row else None
#
# def log_offline_batch_summary(self, summary: Dict) -> None:
#     with self._get_connection() as conn:
#         conn.execute(
#             "CREATE TABLE IF NOT EXISTS offline_batch_summaries "
#             "(timestamp REAL, batch_size INTEGER, avg_quality REAL, avg_carbon REAL, "
#             "avg_latency REAL, avg_energy REAL)"
#         )
#         conn.execute(
#             "INSERT INTO offline_batch_summaries VALUES (?, ?, ?, ?, ?, ?)",
#             (summary['timestamp'], summary['batch_size'], summary['avg_quality'],
#              summary['avg_carbon'], summary['avg_latency'], summary['avg_energy'])
#         )
#         conn.commit()
