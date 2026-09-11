"""
federated_green_learning.py
Cross-deployment weight sharing for MOPD.
"""
from __future__ import annotations
import asyncio
from typing import Any, Dict, List, Optional


class FederatedGreenAggregator:
    """Real byte-wise averaging across federated instances."""

    def __init__(self, storage, instance_id: str, share_interval: int = 3600):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.rounds = 0

    async def share_weights(self, model_id: str, weights: bytes) -> None:
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id, model_id, weights,
                float(len(weights)), self.rounds)
        except Exception:
            pass

    async def pull_aggregated_weights(self, model_id: str) -> Optional[bytes]:
        rows = await asyncio.to_thread(self.storage.get_federated_weights, model_id)
        if not rows:
            return None
        blobs = [r["weights"] for r in rows if r.get("weights")]
        if not blobs:
            return None
        n = min(len(b) for b in blobs)
        avg = bytearray(n)
        for i in range(n):
            avg[i] = int(sum(b[i] for b in blobs) / len(blobs)) & 0xFF
        self.rounds += 1
        return bytes(avg)

    async def apply_aggregated_weights(self, model_id: str, current: bytes) -> bytes:
        agg = await self.pull_aggregated_weights(model_id)
        if agg is None:
            return current
        n = min(len(current), len(agg))
        return bytes([(current[i] + agg[i]) // 2 for i in range(n)])
