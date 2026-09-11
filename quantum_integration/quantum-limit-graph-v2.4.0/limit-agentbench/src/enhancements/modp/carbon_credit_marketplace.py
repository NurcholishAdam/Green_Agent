"""
carbon_credit_marketplace.py
External carbon market and REC integration for MOPD.
"""
from __future__ import annotations
import asyncio
import random
import uuid
from typing import Any, Dict, Optional


class CarbonMarketIntegrator:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.last_price = 25.0

    async def _fetch_price(self) -> float:
        return max(5.0, self.last_price + random.gauss(0, 1.5))

    async def update_price(self) -> float:
        try:
            price = await self._fetch_price()
        except Exception:
            price = self.last_price
        self.last_price = price
        await asyncio.to_thread(self.storage.save_credit_price, price)
        return price

    async def purchase_rec(self, mwh: float, price_per_mwh: float = 5.0,
                           source: str = "wind") -> float:
        cost = mwh * price_per_mwh
        await asyncio.to_thread(self.storage.save_rec, mwh, price_per_mwh, source)
        return cost

    async def net_zero_schedule(self, workload_kwh: float,
                                intensity: float) -> Dict[str, Any]:
        price = await self.update_price()
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price
        action = "defer" if intensity > 0.3 else \
                 ("run_offset" if offset_cost < 0.5 else "run")
        await asyncio.to_thread(
            self.storage.save_net_zero_match,
            uuid.uuid4().hex[:8], workload_kwh, intensity, action,
            carbon_kg, offset_cost, price)
        return {"action": action, "carbon_kg": carbon_kg,
                "offset_cost_usd": offset_cost, "credit_price_usd": price,
                "rec_balance_mwh": await asyncio.to_thread(
                    self.storage.get_rec_balance)}
