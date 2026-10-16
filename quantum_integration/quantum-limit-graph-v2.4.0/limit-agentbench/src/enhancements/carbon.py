"""
carbon.py — Carbon markets, RECs, and the unified carbon manager (Enhancement 8).

Single source of truth for carbon intensity, price, and REC inventory.
"""

from __future__ import annotations

import logging
import math
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol, Sequence, runtime_checkable

try:
    import aiohttp
    from aiohttp import ClientTimeout, ClientSession
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False


@runtime_checkable
class ICarbonManager(Protocol):
    async def get_current_intensity(self) -> float: ...
    async def close(self): ...
    async def health_check(self) -> Dict: ...


class CarbonMarketClient:
    """
    Carbon price model with optional API hook.
    Price peaks in the evening and bottoms before dawn.
    """

    def __init__(self, base_price: float = 0.05, sensitivity: float = 0.0005,
                 session: Optional[Any] = None,
                 api_url: Optional[str] = None,
                 api_key: Optional[str] = None):
        self.base_price = base_price
        self.sensitivity = sensitivity
        self._session = session
        self._api_url = api_url
        self._api_key = api_key
        self._cache: Dict[tuple, float] = {}

    def _tod_factor(self, hour_of_day: Optional[int]) -> float:
        if hour_of_day is None:
            return 1.0
        return 1.0 + 0.3 * math.sin((hour_of_day - 6) / 24.0 * 2 * math.pi)

    def _key(self, ci: float, hour: Optional[int]) -> tuple:
        return (int(ci), -1 if hour is None else int(hour))

    async def price_async(self, carbon_intensity: float,
                          hour_of_day: Optional[int] = None) -> float:
        key = self._key(carbon_intensity, hour_of_day)
        if key in self._cache:
            return self._cache[key]
        value: Optional[float] = None
        if self._api_url and self._session is not None:
            try:
                headers = {"Authorization": f"Bearer {self._api_key}"} if self._api_key else {}
                async with self._session.get(self._api_url, headers=headers,
                                             params={"intensity": carbon_intensity}) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        value = float(data.get("price_usd_per_kg", 0.0))
            except Exception as e:
                logging.getLogger(__name__).warning(f"Carbon market API failed: {e}")
        if value is None:
            value = self.base_price + self.sensitivity * carbon_intensity * self._tod_factor(hour_of_day)
        self._cache[key] = value
        return value

    def price(self, carbon_intensity: float, hour_of_day: Optional[int] = None) -> float:
        key = self._key(carbon_intensity, hour_of_day)
        if key in self._cache:
            return self._cache[key]
        value = self.base_price + self.sensitivity * carbon_intensity * self._tod_factor(hour_of_day)
        self._cache[key] = value
        return value


@dataclass
class RECRecord:
    kwh: float
    issued_at: float
    source: str = "solar"
    expires_at: Optional[float] = None


class RECInventory:
    """FIFO consumption with expiry."""

    def __init__(self, grid_kg_co2_per_kwh: float = 0.4, default_ttl_days: float = 365.0):
        self.grid_factor = grid_kg_co2_per_kwh
        self.default_ttl_days = default_ttl_days
        self._records: List[RECRecord] = []

    def _prune(self) -> None:
        now = time.time()
        self._records = [r for r in self._records if r.expires_at is None or r.expires_at > now]

    def add(self, kwh: float, source: str = "solar", expires_at: Optional[float] = None) -> None:
        if expires_at is None:
            expires_at = time.time() + self.default_ttl_days * 86400.0
        self._records.append(RECRecord(kwh=kwh, issued_at=time.time(),
                                       source=source, expires_at=expires_at))

    def total_kwh(self) -> float:
        self._prune()
        return sum(r.kwh for r in self._records)

    def consume(self, kwh: float) -> float:
        self._prune()
        remaining = float(kwh)
        offset = 0.0
        for r in self._records:
            if remaining <= 0:
                break
            take = min(r.kwh, remaining)
            r.kwh -= take
            remaining -= take
            offset += take * self.grid_factor
        self._records = [r for r in self._records if r.kwh > 1e-9]
        return offset

    def snapshot(self) -> Dict[str, Any]:
        self._prune()
        return {
            "total_kwh": self.total_kwh(),
            "n_records": len(self._records),
            "sources": sorted({r.source for r in self._records}),
        }


class CarbonIntensityManager(ICarbonManager):
    """
    Single source of truth for carbon intensity, price, and RECs.

    - If `api_key` is provided, fetches from ElectricityMaps with caching.
    - Otherwise, uses a synthetic diurnal model.
    """

    def __init__(self, api_key: Optional[str] = None, region: str = "global",
                 base_price: float = 0.05, sensitivity: float = 0.0005,
                 rec_default_kwh: float = 0.0, cache_seconds: int = 300):
        self._current = 400.0
        self._last_fetch = 0.0
        self._cache_seconds = cache_seconds
        self._session: Optional[Any] = None
        self._api_key = api_key
        self._region = region
        self.market = CarbonMarketClient(base_price=base_price, sensitivity=sensitivity)
        self.recs = RECInventory()
        if rec_default_kwh > 0:
            self.recs.add(rec_default_kwh)

    async def _ensure_session(self) -> None:
        if not AIOHTTP_AVAILABLE:
            return
        if self._session is None:
            self._session = ClientSession(timeout=ClientTimeout(total=10))

    async def _fetch_electricity_maps(self) -> Optional[float]:
        if not self._api_key or not AIOHTTP_AVAILABLE:
            return None
        await self._ensure_session()
        url = "https://api.electricitymap.org/v3/carbon-intensity/latest"
        try:
            async with self._session.get(url, params={"zone": self._region},
                                         headers={"auth-token": self._api_key}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return float(data.get("carbonIntensity", self._current))
        except Exception as e:
            logging.getLogger(__name__).warning(f"ElectricityMaps fetch failed: {e}")
        return None

    def _synthetic(self) -> float:
        hour = datetime.now().hour
        diurnal = 100 * math.sin(2 * math.pi * (hour - 14) / 24)
        return max(50.0, 400.0 + diurnal + random.uniform(-20, 20))

    async def get_current_intensity(self) -> float:
        now = time.time()
        if now - self._last_fetch < self._cache_seconds:
            return self._current
        fetched = await self._fetch_electricity_maps()
        self._current = fetched if fetched is not None else self._synthetic()
        self._last_fetch = now
        return self._current

    async def get_current_price(self, hour_of_day: Optional[int] = None) -> float:
        intensity = await self.get_current_intensity()
        return self.market.price(intensity, hour_of_day)

    def consume_rec(self, kwh: float) -> float:
        return self.recs.consume(kwh)

    def rec_kwh(self) -> float:
        return self.recs.total_kwh()

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def health_check(self) -> Dict:
        return {
            "status": "ok",
            "intensity": self._current,
            "carbon_price": self.market.price(self._current),
            "rec_kwh": self.recs.total_kwh(),
            "source": "api" if self._api_key else "synthetic",
        }
