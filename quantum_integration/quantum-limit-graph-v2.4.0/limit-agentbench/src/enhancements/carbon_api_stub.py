"""
carbon_api.py — Enhanced Carbon Intensity API module (v3.0.0)
==============================================================

Provides:
- ``CarbonAPIStub`` — deterministic, configurable simulation.
- ``RealCarbonAPI`` — ElectricityMap client with LRU+TTL cache, backoff,
  and last-known-value fallback.
- ``ChaosCarbonAPI`` — resilience-testing wrapper.
- ``CarbonIntensityFetcher`` — async adapter for FlexGen/MODP with:
  ``get_current_intensity``, ``forecast_carbon_prices``, ``is_safe``,
  ``run_safety_check``, ``purchase_offsets``, ``update_feedback``.
- ``CarbonOffsetBroker`` — policy-checked offset purchase.
- ``FederatedCarbonAggregator`` — weighted/median aggregation.

CHANGES (v3.0.0 over v2.0.0):
- **Units unified.** All public surfaces return **kg CO₂/kWh** by default.
  The raw stub and ElectricityMap values remain gCO₂/kWh internally; a
  configurable ``unit`` (``"kg_per_kwh"`` or ``"g_per_kwh"``) controls
  what callers see.
- ``CarbonOffsetBroker.purchase_offsets`` now requires an intensity
  argument and honours ``should_offset``.
- ``run_safety_check`` computes the real interval from the forecast
  timestamps instead of assuming 10 minutes.
- ``RealCarbonAPI`` returns the last successful value on failure, keeps an
  LRU+TTL cache, and applies exponential backoff with jitter on 429/5xx.
- ``update_feedback`` persists real API feedback to a local JSONL store.
- ``FeedbackEvent`` is emitted on safety violations, offset purchases, and
  forecast errors when the schema is importable.
- ``get_carbon_api`` filters kwargs by constructor signature.
- ``CarbonConfig`` dataclass centralizes all parameters.
- Stub feedback history is bounded with ``deque(maxlen=10)``.
- ``__version__`` and ``__all__`` added.
- Built-in test suite at the bottom.

Usage:
    Set ``CARBON_API_MODE=stub|real``.
    Optionally set ``CARBON_API_KEY``, ``CARBON_API_REGION``,
    ``CARBON_API_CACHE_TTL``, ``CARBON_API_UNIT``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import random
import time
import uuid
from abc import ABC, abstractmethod
from collections import OrderedDict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    NUMPY_AVAILABLE = False
    np = None  # type: ignore

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None  # type: ignore
    logging.warning("requests not installed; RealCarbonAPI will not work.")

try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    try:
        from src.enhancements.schemas.feedback_event import FeedbackEvent  # type: ignore
    except ImportError:
        FeedbackEvent = None  # type: ignore

logger = logging.getLogger(__name__)

__version__ = "3.0.0"
__all__ = [
    "__version__",
    "CarbonConfig",
    "CarbonAPI",
    "CarbonAPIStub",
    "RealCarbonAPI",
    "ChaosCarbonAPI",
    "CarbonOffsetBroker",
    "FederatedCarbonAggregator",
    "CarbonIntensityFetcher",
    "get_carbon_api",
]


# ============================================================================
# Unit helpers
# ============================================================================
KG_PER_KWH = "kg_per_kwh"
G_PER_KWH = "g_per_kwh"


def _to_kg(value_g: float) -> float:
    return float(value_g) / 1000.0


def _from_kg(value_kg: float) -> float:
    return float(value_kg) * 1000.0


def _convert(value_g: float, unit: str) -> float:
    if unit == G_PER_KWH:
        return float(value_g)
    if unit == KG_PER_KWH:
        return _to_kg(value_g)
    raise ValueError(f"Unknown unit: {unit!r}")


def _unconvert(value: float, unit: str) -> float:
    if unit == G_PER_KWH:
        return float(value)
    if unit == KG_PER_KWH:
        return _from_kg(value)
    raise ValueError(f"Unknown unit: {unit!r}")


# ============================================================================
# Config
# ============================================================================
@dataclass
class CarbonConfig:
    mode: str = "stub"
    region: str = "DE"
    api_key: Optional[str] = None
    cache_ttl_seconds: int = 300
    cache_max_entries: int = 256
    safety_threshold_g: float = 500.0
    unit: str = KG_PER_KWH
    seed: int = 42
    timeout_seconds: float = 10.0
    max_retries: int = 3
    retry_backoff_base: float = 0.5
    feedback_store_path: Optional[str] = None
    emit_feedback_events: bool = True

    def __post_init__(self) -> None:
        if self.unit not in (KG_PER_KWH, G_PER_KWH):
            raise ValueError(f"unit must be one of {KG_PER_KWH!r}, {G_PER_KWH!r}")

    @classmethod
    def from_env(cls) -> "CarbonConfig":
        return cls(
            mode=os.environ.get("CARBON_API_MODE", "stub"),
            region=os.environ.get("CARBON_API_REGION", "DE"),
            api_key=os.environ.get("CARBON_API_KEY"),
            cache_ttl_seconds=int(os.environ.get("CARBON_API_CACHE_TTL", 300)),
            cache_max_entries=int(os.environ.get("CARBON_API_CACHE_MAX", 256)),
            safety_threshold_g=float(os.environ.get("CARBON_API_SAFETY_THRESHOLD", 500.0)),
            unit=os.environ.get("CARBON_API_UNIT", KG_PER_KWH),
            seed=int(os.environ.get("CARBON_API_SEED", 42)),
            timeout_seconds=float(os.environ.get("CARBON_API_TIMEOUT", 10.0)),
            max_retries=int(os.environ.get("CARBON_API_MAX_RETRIES", 3)),
            feedback_store_path=os.environ.get("CARBON_API_FEEDBACK_PATH"),
        )


# ============================================================================
# Abstract base
# ============================================================================
class CarbonAPI(ABC):
    """
    Interface for carbon intensity providers.

    All implementations return **raw gCO₂/kWh** from ``_get_current_raw`` and
    ``_get_forecast_raw``; the public methods convert to the configured unit.
    """

    def __init__(self, unit: str = KG_PER_KWH) -> None:
        self.unit = unit

    @abstractmethod
    def _get_current_raw(self) -> float:
        """Return current carbon intensity in gCO₂/kWh (raw)."""

    @abstractmethod
    def _get_forecast_raw(self, minutes: int = 60) -> List[Tuple[float, float]]:
        """Return list of (timestamp, intensity) in gCO₂/kWh (raw)."""

    def get_current(self) -> float:
        return _convert(self._get_current_raw(), self.unit)

    def get_forecast(self, minutes: int = 60) -> List[Tuple[float, float]]:
        return [(_ts, _convert(v, self.unit))
                for _ts, v in self._get_forecast_raw(minutes)]

    # Optional: identity for XAI
    def describe(self) -> Dict[str, Any]:
        return {
            "class": type(self).__name__,
            "unit": self.unit,
        }


# ============================================================================
# Stub
# ============================================================================
class CarbonAPIStub(CarbonAPI):
    """
    Simulated carbon intensity with daily + seasonal cycles and noise.

    Deterministic per-instance via a seeded ``random.Random`` (allows
    reproducible tests).  Feedback history is bounded with a deque.
    """

    def __init__(
        self,
        base_intensity: float = 200.0,
        daily_amplitude: float = 80.0,
        seasonal_amplitude: float = 30.0,
        noise_std: float = 5.0,
        start_time: Optional[float] = None,
        seed: int = 42,
        unit: str = KG_PER_KWH,
    ) -> None:
        super().__init__(unit=unit)
        self.base_g = float(base_intensity)
        self.daily_amp = float(daily_amplitude)
        self.seasonal_amp = float(seasonal_amplitude)
        self.noise_std = float(noise_std)
        self._start_time = float(start_time if start_time is not None else time.time())
        self._rng = random.Random(seed)
        self._recent_feedback: Deque[float] = deque(maxlen=10)

    # ------------------------------------------------------------------
    def _simulate_g(self, timestamp: float) -> float:
        hours = (timestamp - self._start_time) / 3600.0
        if NUMPY_AVAILABLE:
            daily = self.daily_amp * 0.5 * (1 + math.sin(2 * math.pi * hours / 24))
            seasonal = self.seasonal_amp * 0.5 * (
                1 + math.sin(2 * math.pi * hours / (365 * 24)))
        else:
            daily = self.daily_amp * 0.5 * (1 + math.sin(2 * math.pi * hours / 24))
            seasonal = self.seasonal_amp * 0.5 * (
                1 + math.sin(2 * math.pi * hours / (365 * 24)))
        noise = self._rng.gauss(0, self.noise_std)
        return max(50.0, self.base_g + daily + seasonal + noise)

    def _get_current_raw(self) -> float:
        return self._simulate_g(time.time())

    def _get_forecast_raw(self, minutes: int = 60) -> List[Tuple[float, float]]:
        now = time.time()
        step_minutes = 10
        out: List[Tuple[float, float]] = []
        for i in range(0, minutes, step_minutes):
            ts = now + i * 60
            out.append((ts, self._simulate_g(ts)))
        return out

    # ------------------------------------------------------------------
    def update_from_feedback(self, actual_intensity_g: float) -> None:
        """Adapt baseline parameters to real observations (in gCO₂/kWh)."""
        self._recent_feedback.append(float(actual_intensity_g))
        if self._recent_feedback:
            self.base_g = sum(self._recent_feedback) / len(self._recent_feedback)
            self.noise_std = max(1.0, self.noise_std * 0.95)

    def describe(self) -> Dict[str, Any]:
        return {
            "class": type(self).__name__,
            "unit": self.unit,
            "base_g": self.base_g,
            "daily_amp": self.daily_amp,
            "seasonal_amp": self.seasonal_amp,
            "noise_std": self.noise_std,
            "recent_feedback": list(self._recent_feedback),
        }


# ============================================================================
# Real API
# ============================================================================
class _LRUCache:
    """Tiny LRU+TTL cache for JSON payloads."""

    def __init__(self, max_entries: int = 256, ttl_seconds: float = 300.0) -> None:
        self.max_entries = max(1, int(max_entries))
        self.ttl = float(ttl_seconds)
        self._data: "OrderedDict[Any, Tuple[float, Dict[str, Any]]]" = OrderedDict()

    def get(self, key: Any, *, allow_stale: bool = False
            ) -> Optional[Tuple[Dict[str, Any], float]]:
        entry = self._data.get(key)
        if entry is None:
            return None
        ts, value = entry
        age = time.monotonic() - ts
        if age > self.ttl and not allow_stale:
            return None
        # Refresh recency on hit
        self._data.move_to_end(key)
        return value, age

    def set(self, key: Any, value: Dict[str, Any]) -> None:
        self._data[key] = (time.monotonic(), value)
        self._data.move_to_end(key)
        while len(self._data) > self.max_entries:
            self._data.popitem(last=False)

    def __len__(self) -> int:
        return len(self._data)


class RealCarbonAPI(CarbonAPI):
    """
    ElectricityMap client with LRU+TTL cache, exponential backoff + jitter,
    and last-known-value fallback.
    """

    BASE_URL = "https://api.electricitymap.org/v3"
    DEFAULT_INTENSITY_G = 200.0

    def __init__(
        self,
        api_key: Optional[str] = None,
        region: Optional[str] = None,
        cache_ttl: int = 300,
        cache_max_entries: int = 256,
        timeout_seconds: float = 10.0,
        max_retries: int = 3,
        retry_backoff_base: float = 0.5,
        seed: int = 42,
        unit: str = KG_PER_KWH,
    ) -> None:
        super().__init__(unit=unit)
        if requests is None:
            raise ImportError(
                "requests library is required for RealCarbonAPI.")
        self.api_key = api_key or os.environ.get("CARBON_API_KEY")
        if not self.api_key:
            raise ValueError("API key is required. Set CARBON_API_KEY env var.")
        self.region = region or os.environ.get("CARBON_API_REGION", "DE")
        self.timeout = float(timeout_seconds)
        self.max_retries = max(1, int(max_retries))
        self.retry_backoff_base = float(retry_backoff_base)
        self._rng = random.Random(seed)
        self._cache = _LRUCache(cache_max_entries, cache_ttl)
        self._last_success_g: Optional[float] = None
        self._last_forecast_g: Optional[List[Tuple[float, float]]] = None

    # ------------------------------------------------------------------
    def _api_call(self, endpoint: str,
                  params: Optional[Dict[str, Any]] = None,
                  ) -> Optional[Dict[str, Any]]:
        cache_key = (endpoint, tuple(sorted((params or {}).items())))
        hit = self._cache.get(cache_key)
        if hit is not None:
            value, _age = hit
            return value

        url = f"{self.BASE_URL}/{endpoint}"
        headers = {"auth-token": self.api_key}
        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                resp = requests.get(
                    url, headers=headers, params=params, timeout=self.timeout)
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise requests.exceptions.HTTPError(
                        f"status={resp.status_code}")
                resp.raise_for_status()
                data = resp.json()
                self._cache.set(cache_key, data)
                return data
            except requests.exceptions.RequestException as e:
                last_exc = e
                # Exponential backoff with jitter
                backoff = self.retry_backoff_base * (2 ** attempt)
                backoff *= (0.5 + self._rng.random())  # jitter
                logger.warning("API request attempt %d/%d failed: %s "
                               "(backoff %.2fs)",
                               attempt + 1, self.max_retries, e, backoff)
                if attempt + 1 < self.max_retries:
                    time.sleep(backoff)

        # All attempts failed — return stale cache if available.
        stale = self._cache.get(cache_key, allow_stale=True)
        if stale is not None:
            value, age = stale
            logger.warning("Using stale cache (age=%.1fs) after failure: %s",
                           age, last_exc)
            return value
        logger.error("API call failed and no cache available: %s", last_exc)
        return None

    # ------------------------------------------------------------------
    def _get_current_raw(self) -> float:
        data = self._api_call("carbon-intensity/latest",
                              {"zone": self.region})
        if data and "carbonIntensity" in data:
            value = float(data["carbonIntensity"])
            self._last_success_g = value
            return value
        if self._last_success_g is not None:
            logger.warning("Falling back to last-known intensity (%.1f g).",
                           self._last_success_g)
            return self._last_success_g
        logger.warning("No data available; returning default %.1f g.",
                       self.DEFAULT_INTENSITY_G)
        return self.DEFAULT_INTENSITY_G

    def _get_forecast_raw(self, minutes: int = 60) -> List[Tuple[float, float]]:
        data = self._api_call("carbon-intensity/forecast",
                              {"zone": self.region})
        if not data or "forecast" not in data:
            if self._last_forecast_g is not None:
                logger.warning("Falling back to last-known forecast.")
                return list(self._last_forecast_g)
            logger.warning("Forecast API failed; using stub fallback.")
            stub = CarbonAPIStub(seed=42)
            return stub._get_forecast_raw(minutes)

        now = time.time()
        horizon = now + minutes * 60
        out: List[Tuple[float, float]] = []
        for entry in data["forecast"]:
            try:
                dt = datetime.fromisoformat(
                    str(entry["datetime"]).replace("Z", "+00:00"))
                ts = dt.timestamp()
                intensity = float(entry["carbonIntensity"])
            except (KeyError, ValueError, TypeError):
                continue
            if ts <= horizon:
                out.append((ts, intensity))
            else:
                break
        if out:
            self._last_forecast_g = out
        return out

    def describe(self) -> Dict[str, Any]:
        return {
            "class": type(self).__name__,
            "unit": self.unit,
            "region": self.region,
            "cache_size": len(self._cache),
            "last_success_g": self._last_success_g,
        }


# ============================================================================
# Chaos wrapper
# ============================================================================
class ChaosCarbonAPI(CarbonAPI):
    """Injects random failures and delays into a wrapped CarbonAPI."""

    def __init__(
        self,
        base_api: CarbonAPI,
        failure_probability: float = 0.1,
        delay_mean: float = 0.5,
        delay_std: float = 0.2,
        seed: Optional[int] = None,
        unit: Optional[str] = None,
    ) -> None:
        super().__init__(unit=unit or base_api.unit)
        self.api = base_api
        self.failure_prob = float(failure_probability)
        self.delay_mean = float(delay_mean)
        self.delay_std = float(delay_std)
        self._rng = random.Random(seed if seed is not None else 42)

    def _maybe_fail_or_delay(self) -> None:
        # Decide first, then delay only on success.
        will_fail = self._rng.random() < self.failure_prob
        if not will_fail and self.delay_mean > 0:
            time.sleep(max(0.0, self._rng.gauss(self.delay_mean, self.delay_std)))
        if will_fail:
            raise RuntimeError("Simulated carbon API failure")

    def _get_current_raw(self) -> float:
        self._maybe_fail_or_delay()
        return self.api._get_current_raw()

    def _get_forecast_raw(self, minutes: int = 60) -> List[Tuple[float, float]]:
        self._maybe_fail_or_delay()
        return self.api._get_forecast_raw(minutes)


# ============================================================================
# Factory
# ============================================================================
def _filter_kwargs(cls: Any, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    import inspect
    sig = inspect.signature(cls.__init__)
    allowed = set(sig.parameters.keys()) - {"self"}
    return {k: v for k, v in kwargs.items() if k in allowed}


def get_carbon_api(mode: Optional[str] = None,
                   config: Optional[CarbonConfig] = None,
                   **kwargs: Any) -> CarbonAPI:
    cfg = config or CarbonConfig.from_env()
    resolved_mode = (mode or cfg.mode or "stub").lower()
    merged = {
        "region": cfg.region,
        "api_key": cfg.api_key,
        "cache_ttl": cfg.cache_ttl_seconds,
        "cache_max_entries": cfg.cache_max_entries,
        "timeout_seconds": cfg.timeout_seconds,
        "max_retries": cfg.max_retries,
        "retry_backoff_base": cfg.retry_backoff_base,
        "seed": cfg.seed,
        "unit": cfg.unit,
    }
    merged.update(kwargs)
    if resolved_mode == "real":
        return RealCarbonAPI(**_filter_kwargs(RealCarbonAPI, merged))
    return CarbonAPIStub(**_filter_kwargs(CarbonAPIStub, merged))


# ============================================================================
# Offset broker
# ============================================================================
class CarbonOffsetBroker:
    """
    Policy-checked carbon offset purchases.

    ``purchase_offsets`` requires the intensity that motivated the purchase
    and refuses to buy when the policy says no.  Use ``force=True`` to
    bypass the policy.
    """

    def __init__(
        self,
        threshold_intensity_g: float = 400.0,
        cost_per_kg_co2: float = 0.10,
        api_key: Optional[str] = None,
        emit_events: bool = True,
    ) -> None:
        self.threshold_g = float(threshold_intensity_g)
        self.cost_per_kg = float(cost_per_kg_co2)
        self.api_key = api_key or os.environ.get("CARBON_OFFSET_API_KEY")
        self.emit_events = emit_events
        self.total_offset_kg = 0.0
        self.total_cost_usd = 0.0
        self._purchases: Deque[Dict[str, Any]] = deque(maxlen=1000)

    def should_offset(self, current_intensity_g: float) -> bool:
        return float(current_intensity_g) > self.threshold_g

    def purchase_offsets(
        self,
        co2_kg: float,
        current_intensity_g: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        if co2_kg <= 0:
            return {"status": "no_action",
                    "reason": "co2_kg must be positive",
                    "offset_kg": 0.0, "cost_usd": 0.0}

        if not force:
            if current_intensity_g is None:
                return {
                    "status": "no_action",
                    "reason": "current_intensity_g is required unless force=True",
                    "offset_kg": 0.0,
                    "cost_usd": 0.0,
                }
            if not self.should_offset(current_intensity_g):
                return {
                    "status": "no_action",
                    "reason": "intensity below threshold",
                    "offset_kg": 0.0,
                    "cost_usd": 0.0,
                }

        cost = co2_kg * self.cost_per_kg
        self.total_offset_kg += co2_kg
        self.total_cost_usd += cost
        receipt = {
            "status": "success",
            "offset_kg": float(co2_kg),
            "cost_usd": float(cost),
            "timestamp": time.time(),
            "provider": "mock_offset_provider",
            "metadata": dict(metadata or {}),
            "receipt_id": str(uuid.uuid4()),
        }
        self._purchases.append(receipt)
        if self.emit_events:
            _emit_feedback_event(
                tag="carbon_offset_purchase",
                action="purchase",
                reward=1.0,
                metadata={"offset_kg": co2_kg, "cost_usd": cost},
            )
        return receipt

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_offset_kg": self.total_offset_kg,
            "total_cost_usd": self.total_cost_usd,
            "purchase_count": len(self._purchases),
            "threshold_g": self.threshold_g,
            "cost_per_kg": self.cost_per_kg,
        }


# ============================================================================
# Federated aggregator
# ============================================================================
class FederatedCarbonAggregator:
    """
    Aggregates readings from multiple deployments using a weighted average
    with median-based outlier rejection.
    """

    def __init__(self, max_history: int = 20) -> None:
        self._data: Dict[str, Deque[float]] = {}
        self._weights: Dict[str, float] = {}
        self._max_history = int(max_history)
        self.last_aggregation_time: Optional[float] = None

    def add_participant_data(
        self,
        participant_id: str,
        intensities: Iterable[float],
        weight: float = 1.0,
    ) -> None:
        bucket = self._data.setdefault(
            participant_id, deque(maxlen=self._max_history))
        for v in intensities:
            try:
                bucket.append(float(v))
            except (TypeError, ValueError):
                continue
        self._weights[participant_id] = float(weight)

    def reset(self) -> None:
        self._data.clear()
        self._weights.clear()
        self.last_aggregation_time = None

    def aggregate(self) -> Dict[str, Any]:
        latest: List[Tuple[str, float]] = []
        for pid, values in self._data.items():
            if values:
                latest.append((pid, values[-1]))
        if not latest:
            return {"status": "no_data", "average": None, "count": 0}

        values = [v for _pid, v in latest]
        median = float(np.median(values)) if NUMPY_AVAILABLE else _median(values)

        # Reject values more than 50% away from the median.
        kept = [(pid, v) for pid, v in latest
                if median == 0 or abs(v - median) <= 0.5 * abs(median) or
                abs(v - median) <= 50.0]
        if not kept:
            kept = latest

        weighted_sum = 0.0
        weight_sum = 0.0
        for pid, v in kept:
            w = self._weights.get(pid, 1.0)
            weighted_sum += v * w
            weight_sum += w

        average = weighted_sum / weight_sum if weight_sum > 0 else median
        self.last_aggregation_time = time.time()
        return {
            "status": "success",
            "average": float(average),
            "median": float(median),
            "count": len(kept),
            "rejected": len(latest) - len(kept),
            "timestamp": self.last_aggregation_time,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "data": {k: list(v) for k, v in self._data.items()},
            "weights": dict(self._weights),
            "last_aggregation_time": self.last_aggregation_time,
        }

    def from_dict(self, payload: Dict[str, Any]) -> None:
        self._data.clear()
        self._weights.clear()
        for k, v in payload.get("data", {}).items():
            self._data[k] = deque((float(x) for x in v),
                                  maxlen=self._max_history)
        for k, w in payload.get("weights", {}).items():
            self._weights[k] = float(w)
        self.last_aggregation_time = payload.get("last_aggregation_time")


def _median(values: List[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    if n % 2:
        return s[n // 2]
    return (s[n // 2 - 1] + s[n // 2]) / 2.0


# ============================================================================
# FeedbackEvent helper
# ============================================================================
def _emit_feedback_event(tag: str, action: str, reward: float,
                         metadata: Optional[Dict[str, Any]] = None) -> None:
    if FeedbackEvent is None:
        return
    try:
        ev = FeedbackEvent(
            source="carbon_api",
            feedback_type="carbon",
            context={"task_id": tag, "teacher_ids": []},
            action={"selected_action": action, "selected_rank": 1,
                    "confidence_score": 0.5},
            performance={
                "quality_score": float(max(0.0, min(1.0, reward))),
                "latency_ms": 0.0, "energy_joules": 0.0,
                "carbon_g": float((metadata or {}).get("carbon_g", 0.0)),
                "helium_cost": 0.0, "duration_ms": 0.0,
            },
            adaptive_cost_value=float(reward),
            tags=[tag],
            envelope={"trace_id": str(uuid.uuid4())},
            metadata=dict(metadata or {}),
        )
        logger.debug("FeedbackEvent emitted: %s",
                     getattr(ev, "event_id", "?"))
    except Exception as e:
        logger.warning("Failed to emit FeedbackEvent: %s", e)


# ============================================================================
# Feedback store for real APIs
# ============================================================================
class _FeedbackStore:
    """Append-only JSONL store for real-API feedback."""

    def __init__(self, path: Optional[str] = None) -> None:
        self.path = Path(path) if path else None
        if self.path is not None:
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
            except OSError:
                pass

    def append(self, record: Dict[str, Any]) -> None:
        if self.path is None:
            return
        try:
            with open(self.path, "a") as f:
                f.write(json.dumps(record, default=str) + "\n")
        except OSError as e:
            logger.warning("Failed to persist feedback: %s", e)


# ============================================================================
# CarbonIntensityFetcher
# ============================================================================
class CarbonIntensityFetcher:
    """
    Async adapter for FlexGen/MODP.

    Public API:
        await get_current_intensity() -> float (in configured unit)
        await forecast_carbon_prices(hours) -> dict
        await is_safe(threshold=None) -> bool
        await run_safety_check(max_consecutive_minutes, threshold) -> dict
        await purchase_offsets(co2_kg, current_intensity=None) -> dict
        update_feedback(actual_intensity) -> dict
    """

    def __init__(
        self,
        api: Optional[CarbonAPI] = None,
        config: Optional[CarbonConfig] = None,
        mode: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        self.config = config or CarbonConfig.from_env()
        self.api = api or get_carbon_api(mode=mode, config=self.config, **kwargs)
        self.offset_broker = CarbonOffsetBroker(
            threshold_intensity_g=self.config.safety_threshold_g,
            emit_events=self.config.emit_feedback_events,
        )
        self.federated = FederatedCarbonAggregator()
        self._feedback_store = _FeedbackStore(self.config.feedback_store_path)
        self._last_intensity: Optional[float] = None

    # ------------------------------------------------------------------
    async def get_current_intensity(self) -> float:
        try:
            value = await asyncio.to_thread(self.api.get_current)
            self._last_intensity = value
            return value
        except Exception as e:
            logger.warning("get_current_intensity failed: %s", e)
            if self._last_intensity is not None:
                return self._last_intensity
            # Convert the raw fallback to the configured unit.
            return _convert(RealCarbonAPI.DEFAULT_INTENSITY_G, self.config.unit)

    # ------------------------------------------------------------------
    async def forecast_carbon_prices(self, hours: int = 24) -> Dict[str, Any]:
        minutes = max(1, hours * 60)
        try:
            forecast = await asyncio.to_thread(
                self.api.get_forecast, minutes)
        except Exception as e:
            _emit_feedback_event(
                tag="carbon_forecast_error",
                action="forecast_error",
                reward=0.0,
                metadata={"error": str(e)},
            )
            return {
                "status": "error",
                "reason": str(e),
                "predictions": [],
                "timestamps": [],
                "metadata": {"source": type(self.api).__name__},
            }

        if not forecast:
            return {
                "status": "insufficient_data",
                "predictions": [],
                "timestamps": [],
                "metadata": {"source": type(self.api).__name__},
            }

        # Bucket into hourly averages.
        hourly: Dict[int, List[float]] = {}
        for ts, intensity in forecast:
            hour_bucket = int(ts // 3600) * 3600
            hourly.setdefault(hour_bucket, []).append(float(intensity))
        timestamps = sorted(hourly.keys())
        predictions = [sum(hourly[ts]) / len(hourly[ts]) for ts in timestamps]

        # Determine source identity
        source_name = type(self.api).__name__
        source = "stub" if isinstance(self.api, CarbonAPIStub) else "real"
        region = getattr(self.api, "region", "unknown")
        metadata = {
            "source": source,
            "source_class": source_name,
            "region": region,
            "forecast_points": len(predictions),
            "interval_minutes": _infer_interval_minutes(forecast),
            "unit": self.config.unit,
        }
        return {
            "status": "success",
            "predictions": predictions,
            "timestamps": timestamps,
            "region": region,
            "source": source,
            "metadata": metadata,
        }

    # ------------------------------------------------------------------
    async def is_safe(self, threshold: Optional[float] = None) -> bool:
        intensity = await self.get_current_intensity()
        # Threshold is provided in the configured unit or in g?
        # Contract: threshold is in the SAME unit as the returned intensity.
        if threshold is None:
            threshold = _convert(self.config.safety_threshold_g, self.config.unit)
        safe = intensity <= threshold
        if not safe:
            _emit_feedback_event(
                tag="carbon_safety_violation",
                action="safety_violation",
                reward=0.0,
                metadata={"intensity": intensity, "threshold": threshold,
                          "unit": self.config.unit},
            )
        return safe

    # ------------------------------------------------------------------
    async def run_safety_check(
        self,
        max_consecutive_minutes: int = 30,
        threshold: Optional[float] = None,
    ) -> Dict[str, Any]:
        if threshold is None:
            threshold = _convert(self.config.safety_threshold_g, self.config.unit)
        try:
            forecast = await asyncio.to_thread(
                self.api.get_forecast, max_consecutive_minutes)
        except Exception as e:
            return {"safe": False, "reason": f"forecast error: {e}"}
        if not forecast:
            return {"safe": False, "reason": "no forecast data"}

        interval_minutes = _infer_interval_minutes(forecast)
        consecutive_high = 0.0
        peak = 0.0
        for _ts, intensity in forecast:
            peak = max(peak, float(intensity))
            if intensity > threshold:
                consecutive_high += interval_minutes
            else:
                consecutive_high = 0.0
            if consecutive_high >= max_consecutive_minutes:
                _emit_feedback_event(
                    tag="carbon_temporal_safety_violation",
                    action="temporal_safety_violation",
                    reward=0.0,
                    metadata={"threshold": threshold,
                              "consecutive_minutes": consecutive_high,
                              "peak": peak,
                              "unit": self.config.unit},
                )
                return {
                    "safe": False,
                    "reason": f"intensity above {threshold:.4f} for "
                              f"{consecutive_high:.1f} minutes",
                    "peak": peak,
                    "interval_minutes": interval_minutes,
                }
        return {
            "safe": True,
            "peak": peak,
            "interval_minutes": interval_minutes,
        }

    # ------------------------------------------------------------------
    async def purchase_offsets(
        self,
        co2_kg: float,
        current_intensity: Optional[float] = None,
        force: bool = False,
    ) -> Dict[str, Any]:
        if current_intensity is None:
            current_intensity = await self.get_current_intensity()
        # Broker expects the intensity in g for its threshold comparison.
        intensity_g = _unconvert(current_intensity, self.config.unit)
        return await asyncio.to_thread(
            self.offset_broker.purchase_offsets,
            co2_kg, intensity_g, {"unit": self.config.unit}, force,
        )

    # ------------------------------------------------------------------
    def update_feedback(self, actual_intensity: float,
                        region: Optional[str] = None) -> Dict[str, Any]:
        """
        Submit an observed intensity (in the configured unit).

        For stubs, this adapts the simulation.  For real APIs, this
        persists the observation to the feedback store for offline use.
        """
        raw_g = _unconvert(actual_intensity, self.config.unit)
        if isinstance(self.api, CarbonAPIStub):
            self.api.update_from_feedback(raw_g)
            return {"status": "applied", "target": "stub"}

        record = {
            "timestamp": time.time(),
            "iso_time": datetime.now(timezone.utc).isoformat(),
            "intensity_g": raw_g,
            "region": region or getattr(self.api, "region", "unknown"),
            "source": type(self.api).__name__,
        }
        self._feedback_store.append(record)
        return {"status": "persisted", "target": "real",
                "store": str(self._feedback_store.path)
                if self._feedback_store.path else None}

    # ------------------------------------------------------------------
    def describe(self) -> Dict[str, Any]:
        return {
            "unit": self.config.unit,
            "api": self.api.describe(),
            "offset_broker": self.offset_broker.get_stats(),
        }


def _infer_interval_minutes(forecast: List[Tuple[float, float]]) -> float:
    if len(forecast) < 2:
        return 10.0
    deltas = [(forecast[i + 1][0] - forecast[i][0]) / 60.0
              for i in range(len(forecast) - 1)]
    return max(0.1, sum(deltas) / len(deltas))


# ============================================================================
# Demo
# ============================================================================
async def _demo() -> None:
    logging.basicConfig(level=logging.INFO)
    cfg = CarbonConfig(mode="stub", unit=KG_PER_KWH)
    api = get_carbon_api(config=cfg)
    print(f"Current intensity (stub): {api.get_current():.6f} kgCO2/kWh")
    print("Forecast (stub) first 5 entries:")
    for ts, intensity in api.get_forecast(30)[:5]:
        print(f"  {datetime.fromtimestamp(ts)}: {intensity:.6f}")

    fetcher = CarbonIntensityFetcher(api=api, config=cfg)
    current = await fetcher.get_current_intensity()
    print(f"Async current intensity: {current:.6f}")

    forecast = await fetcher.forecast_carbon_prices(hours=6)
    print(f"Forecast status: {forecast['status']}, "
          f"source: {forecast['metadata']['source']}, "
          f"interval: {forecast['metadata']['interval_minutes']:.1f} min")
    if forecast["predictions"]:
        print(f"First 3 hourly predictions: "
              f"{[round(p, 6) for p in forecast['predictions'][:3]]}")

    safe = await fetcher.is_safe()
    print(f"Safety check: {'safe' if safe else 'unsafe'}")

    temporal = await fetcher.run_safety_check(
        max_consecutive_minutes=20)
    print(f"Temporal safety check: {temporal}")

    receipt = await fetcher.purchase_offsets(co2_kg=2.5)
    print(f"Offset receipt: {receipt['status']} (offset={receipt['offset_kg']} kg)")

    fetcher.update_feedback(0.21)  # 0.21 kg/kWh
    print("Feedback applied to stub.")

    chaos = ChaosCarbonAPI(api, failure_probability=0.3,
                           delay_mean=0.05, seed=0)
    chaos_fetcher = CarbonIntensityFetcher(api=chaos, config=cfg)
    try:
        v = await chaos_fetcher.get_current_intensity()
        print(f"Chaos API returned {v:.6f}")
    except Exception as e:
        print(f"Chaos API failed: {e}")

    fed = FederatedCarbonAggregator()
    fed.add_participant_data("dep1", [300, 310, 320], weight=1.0)
    fed.add_participant_data("dep2", [280, 290, 295], weight=1.0)
    print(f"Federated: {fed.aggregate()}")


# ============================================================================
# Tests
# ============================================================================
def _test_unit_conversion() -> None:
    assert abs(_to_kg(400.0) - 0.4) < 1e-9
    assert abs(_from_kg(0.4) - 400.0) < 1e-9


def _test_stub_units() -> None:
    stub_g = CarbonAPIStub(unit=G_PER_KWH, seed=0)
    stub_kg = CarbonAPIStub(unit=KG_PER_KWH, seed=0)
    v_g = stub_g.get_current()
    v_kg = stub_kg.get_current()
    assert 50.0 <= v_g <= 800.0
    assert abs(v_kg - v_g / 1000.0) < 1e-6


def _test_stub_feedback_adapts() -> None:
    stub = CarbonAPIStub(base_intensity=200.0, noise_std=50.0,
                         seed=0, unit=G_PER_KWH)
    for _ in range(10):
        stub.update_from_feedback(500.0)
    assert stub.base_g > 400.0
    assert stub.noise_std < 50.0


def _test_offset_broker_policy() -> None:
    broker = CarbonOffsetBroker(threshold_intensity_g=400.0)
    # Below threshold: no action
    r1 = broker.purchase_offsets(1.0, current_intensity_g=100.0)
    assert r1["status"] == "no_action"
    # Above threshold: purchase
    r2 = broker.purchase_offsets(2.0, current_intensity_g=500.0)
    assert r2["status"] == "success"
    assert broker.total_offset_kg == 2.0
    # Missing intensity is rejected
    r3 = broker.purchase_offsets(1.0, current_intensity_g=None)
    assert r3["status"] == "no_action"
    # Force bypasses policy
    r4 = broker.purchase_offsets(1.0, current_intensity_g=100.0, force=True)
    assert r4["status"] == "success"


def _test_federated_weighted() -> None:
    fed = FederatedCarbonAggregator()
    fed.add_participant_data("a", [100.0], weight=3.0)
    fed.add_participant_data("b", [200.0], weight=1.0)
    agg = fed.aggregate()
    assert agg["status"] == "success"
    # Weighted mean = (100*3 + 200*1)/4 = 125
    assert abs(agg["average"] - 125.0) < 1e-6


def _test_federated_outlier_rejection() -> None:
    fed = FederatedCarbonAggregator()
    fed.add_participant_data("a", [300.0])
    fed.add_participant_data("b", [305.0])
    fed.add_participant_data("c", [10000.0])  # outlier
    agg = fed.aggregate()
    assert agg["rejected"] >= 1
    assert agg["average"] < 500.0


def _test_infer_interval() -> None:
    forecast = [(t * 600.0, 100.0) for t in range(5)]  # 10-min spacing
    assert abs(_infer_interval_minutes(forecast) - 10.0) < 1e-6


def _test_forecast_error_propagation() -> None:
    class BrokenAPI(CarbonAPI):
        def __init__(self) -> None:
            super().__init__(unit=KG_PER_KWH)
        def _get_current_raw(self) -> float: return 0.0
        def _get_forecast_raw(self, minutes: int = 60):
            raise RuntimeError("boom")

    async def run() -> None:
        fetcher = CarbonIntensityFetcher(api=BrokenAPI())
        result = await fetcher.forecast_carbon_prices(hours=1)
        assert result["status"] == "error"
        assert "boom" in result["reason"]

    asyncio.run(run())


def _test_factory_kwargs_filtering() -> None:
    # Passing an incompatible kwarg must not raise.
    api = get_carbon_api(mode="stub", region="FR", unit=KG_PER_KWH)
    assert isinstance(api, CarbonAPIStub)


def _test_last_known_value_fallback() -> None:
    class Intermittent(CarbonAPI):
        def __init__(self) -> None:
            super().__init__(unit=KG_PER_KWH)
            self.calls = 0
        def _get_current_raw(self) -> float:
            self.calls += 1
            if self.calls > 1:
                raise RuntimeError("nope")
            return 300.0
        def _get_forecast_raw(self, minutes: int = 60):
            return [(time.time(), 300.0)]

    async def run() -> None:
        fetcher = CarbonIntensityFetcher(api=Intermittent())
        v1 = await fetcher.get_current_intensity()
        assert abs(v1 - 0.3) < 1e-9
        v2 = await fetcher.get_current_intensity()
        assert abs(v2 - 0.3) < 1e-9  # cached last-known

    asyncio.run(run())


def _test_safety_threshold_in_unit() -> None:
    class ConstAPI(CarbonAPI):
        def __init__(self, value_g: float, unit: str) -> None:
            super().__init__(unit=unit)
            self.value_g = value_g
        def _get_current_raw(self) -> float:
            return self.value_g
        def _get_forecast_raw(self, minutes: int = 60):
            return [(time.time() + i * 60, self.value_g) for i in range(10)]

    async def run() -> None:
        cfg = CarbonConfig(unit=KG_PER_KWH, safety_threshold_g=500.0)
        api = ConstAPI(400.0, KG_PER_KWH)  # 0.4 kg/kWh
        fetcher = CarbonIntensityFetcher(api=api, config=cfg)
        assert await fetcher.is_safe() is True
        api_hi = ConstAPI(800.0, KG_PER_KWH)  # 0.8 kg/kWh
        fetcher_hi = CarbonIntensityFetcher(api=api_hi, config=cfg)
        assert await fetcher_hi.is_safe() is False

    asyncio.run(run())


def run_tests() -> None:
    _test_unit_conversion()
    _test_stub_units()
    _test_stub_feedback_adapts()
    _test_offset_broker_policy()
    _test_federated_weighted()
    _test_federated_outlier_rejection()
    _test_infer_interval()
    _test_forecast_error_propagation()
    _test_factory_kwargs_filtering()
    _test_last_known_value_fallback()
    _test_safety_threshold_in_unit()
    print("All carbon_api v3.0.0 tests passed.")


# ============================================================================
# Entry point
# ============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="Carbon Intensity API v3.0.0")
    parser.add_argument("--test", action="store_true",
                        help="Run the built-in test suite.")
    parser.add_argument("--demo", action="store_true",
                        help="Run the async demo.")
    args = parser.parse_args()
    if args.test:
        run_tests()
    elif args.demo:
        asyncio.run(_demo())
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
