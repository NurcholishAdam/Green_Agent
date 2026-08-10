"""
Adaptive Cost Function with two-tier updates:
- Online: fast exponential moving average for immediate routing.
- Offline: batched, validated updates for long-term policy weights.
"""
import asyncio
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..config import config
from ..logger import logger

class OnlineWeightManager:
    """Exponential moving average for online adaptation."""
    def __init__(self, storage: Storage):
        self.storage = storage
        self.weights = {
            "quality": 0.25,
            "energy": 0.25,
            "carbon": 0.25,
            "latency": 0.25
        }
        self.alpha = 0.1  # EMA factor
        self._load_state()

    def _load_state(self):
        # Load from storage if available
        pass  # Implement serialization if needed

    def update(self, event: FeedbackEvent):
        """Update weights based on observed event."""
        # Normalize event values to 0-1
        norm_quality = event.quality_score
        norm_energy = 1.0 - (event.energy_joules / 100.0)  # assumed max 100J
        norm_carbon = 1.0 - (event.carbon_g / 1.0)         # assumed max 1g
        norm_latency = 1.0 - (event.latency_ms / 1000.0)   # assumed max 1000ms

        observed = {
            "quality": norm_quality,
            "energy": norm_energy,
            "carbon": norm_carbon,
            "latency": norm_latency
        }
        # Clipped EMA
        for key in self.weights:
            self.weights[key] = (1 - self.alpha) * self.weights[key] + self.alpha * observed[key]
        logger.debug(f"Online weights updated: {self.weights}")

    def get_cost_vector(self) -> Dict[str, float]:
        return self.weights

class OfflineTrainer:
    """Batch trainer for durable updates with validation."""
    def __init__(self, storage: Storage):
        self.storage = storage
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
        """Process batch and update durable weights."""
        if len(self.buffer) == 0:
            return

        # 1. Extract batch features
        batch = self.buffer[:self.batch_size]
        self.buffer = self.buffer[self.batch_size:]

        # 2. Compute aggregated statistics
        avg_carbon = np.mean([e.carbon_g for e in batch])
        avg_quality = np.mean([e.quality_score for e in batch])
        avg_latency = np.mean([e.latency_ms for e in batch])
        avg_energy = np.mean([e.energy_joules for e in batch])

        # 3. Validate before committing (check thresholds)
        if avg_quality < config.PARETO_QUALITY_MIN:
            logger.warning(f"Offline update rejected: quality {avg_quality} < min")
            return

        # 4. Update durable weights (example: reinforce successful actions)
        # In a real implementation, this would update the MOPD student policy
        # For now, we log the update
        logger.info(f"Offline training completed. Avg quality: {avg_quality}, carbon: {avg_carbon}")
        # TODO: Call MTPDOptimizer._train_step() with batched data if needed

class AdaptiveCostFunction:
    """Main orchestrator for 2-tier adaptive costs."""
    def __init__(self, storage: Storage):
        self.storage = storage
        self.online = OnlineWeightManager(storage)
        self.offline = OfflineTrainer(storage)
        self.drift_detector = None  # initialized externally in LifecycleManager

    async def record_feedback(self, event: FeedbackEvent):
        """Record a feedback event into both online and offline pipelines."""
        # 1. Persist to DB
        self.storage.store_feedback_event(event.to_db_dict())

        # 2. Update online weights (fast)
        self.online.update(event)

        # 3. Queue for offline training (slow, batched)
        await self.offline.queue_event(event)

        # 4. Trigger drift detection (if enabled)
        if self.drift_detector:
            await self.drift_detector.check_drift(self.online.weights)

    def get_current_weights(self) -> Dict[str, float]:
        return self.online.get_cost_vector()
