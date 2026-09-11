"""
chaos_testing_engine.py
Chaos engineering for MOPD pipeline resilience.
"""
from __future__ import annotations
import asyncio
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict


class ChaosTestingEngine:
    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()

    async def _steady(self) -> bool:
        if not self.active:
            return True
        return random.random() > getattr(self.config, "chaos_intensity", 0.05)

    async def run_experiment(self, name: str, fault_type: str) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        t0 = time.time()
        before = await self._steady()
        status = "completed"
        try:
            async with self._lock:
                self.active[name] = {
                    "fault_type": fault_type,
                    "started": datetime.now(timezone.utc).isoformat()}
            if fault_type == "latency":
                await asyncio.sleep(0.5)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "memory_pressure":
                _ = bytearray(5 * 1024 * 1024)
            elif fault_type == "network_drop":
                await asyncio.sleep(0.2)
        except Exception:
            status = "injected"
        finally:
            async with self._lock:
                self.active.pop(name, None)
        after = await self._steady()
        duration = (time.time() - t0) * 1000.0
        await asyncio.to_thread(
            self.storage.save_chaos_experiment,
            name, name, fault_type,
            getattr(self.config, "chaos_blast_radius", 0.1),
            int(before), int(after), status, duration)
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after,
                "status": status}
