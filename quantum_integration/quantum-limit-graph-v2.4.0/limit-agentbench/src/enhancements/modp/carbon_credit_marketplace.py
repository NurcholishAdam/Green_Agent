"""
carbon_credit_marketplace.py
External carbon market and REC integration for MOPD.
"""
from __future__ import annotations

import asyncio
import logging
import math
import random
import time
import uuid
from collections import deque
from typing import Any, Deque, Dict, FrozenSet, Optional, Tuple

# ---------- structlog / stdlib logger ----------
try:
    import structlog
    _STRUCTLOG_LOGGER = structlog.get_logger(__name__)
    _STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    _STRUCTLOG_LOGGER = logging.getLogger(__name__)
    _STRUCTLOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)

# ---------- Preserved module-level logger ----------
logger = logging.getLogger(__name__)


def log_event(level: str, message: str, **kwargs: Any) -> None:
    """Structured-logging shim (structlog or stdlib)."""
    if _STRUCTLOG_AVAILABLE:
        getattr(_STRUCTLOG_LOGGER, level)(message, **kwargs)
    else:
        if kwargs:
            extra = " ".join(f"{k}={v!r}" for k, v in kwargs.items())
            message = f"{message} | {extra}"
        getattr(logger, level)(message)


# ---------- Prometheus (optional) ----------
try:
    from prometheus_client import Counter, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False


if PROMETHEUS_AVAILABLE:
    _M_PRICE = Gauge("carbon_market_price_usd", "Current credit price (USD)")
    _M_PRICE_MIN = Gauge("carbon_market_price_min_usd", "Min observed price (USD)")
    _M_PRICE_MAX = Gauge("carbon_market_price_max_usd", "Max observed price (USD)")
    _M_PURCHASES = Counter(
        "carbon_market_rec_purchases_total", "REC purchases", ["source"],
    )
    _M_REC_BALANCE = Gauge("carbon_market_rec_balance_mwh", "REC balance (MWh)")
    _M_DECISIONS = Counter(
        "carbon_market_schedule_decisions_total", "Schedule decisions", ["action"],
    )
    _M_STORAGE_FAILURES = Counter(
        "carbon_market_storage_failures_total", "Storage call failures", ["method"],
    )
else:
    class _Dummy:
        def labels(self, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass

    _M_PRICE = _M_PRICE_MIN = _M_PRICE_MAX = _Dummy()
    _M_PURCHASES = _M_REC_BALANCE = _M_DECISIONS = _M_STORAGE_FAILURES = _Dummy()


def _cfg_get(config: Any, key: str, default: Any = None) -> Any:
    if config is None:
        return default
    if isinstance(config, dict):
        return config.get(key, default)
    if hasattr(config, "model_dump"):
        try:
            return config.model_dump().get(key, default)
        except Exception:
            pass
    if hasattr(config, "dict") and callable(getattr(config, "dict")):
        try:
            return config.dict().get(key, default)
        except Exception:
            pass
    return getattr(config, key, default)


def _as_nonneg_float(value: Any, default: float = 0.0,
                     name: str = "value") -> float:
    """Coerce to a finite, non-negative float, warning on bad input."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        log_event("warning", f"Invalid {name}: {value!r}; using {default}")
        return default
    if not math.isfinite(v):
        log_event("warning", f"Non-finite {name}: {v}; using {default}")
        return default
    if v < 0.0:
        log_event("warning", f"Negative {name}: {v}; clamping to 0")
        return 0.0
    return v


class CarbonMarketIntegrator:
    """External carbon market and REC integration for MOPD."""

    # ---- Class constants (units documented) ----
    DEFER_INTENSITY_THRESHOLD: float = 0.3   # kgCO2/kWh
    RUN_OFFSET_COST_THRESHOLD: float = 0.5   # USD
    MIN_PRICE_USD: float = 5.0
    MAX_PRICE_USD: float = 200.0
    DEFAULT_PRICE_USD: float = 25.0
    DEFAULT_PRICE_PER_MWH_USD: float = 5.0
    ALLOWED_REC_SOURCES: FrozenSet[str] = frozenset({
        "wind", "solar", "hydro", "nuclear", "geothermal", "biomass",
    })
    PRICE_HISTORY_MAX: int = 200
    IDEMPOTENCY_CACHE_MAX: int = 512

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage

        # Public attribute preserved.
        self.last_price: float = float(
            _cfg_get(config, "carbon_default_price_usd", self.DEFAULT_PRICE_USD)
            or self.DEFAULT_PRICE_USD
        )

        # Thresholds / defaults (configurable, falling back to class constants).
        self.defer_threshold: float = float(
            _cfg_get(config, "carbon_defer_intensity_threshold",
                     self.DEFER_INTENSITY_THRESHOLD)
            or self.DEFER_INTENSITY_THRESHOLD
        )
        self.offset_threshold: float = float(
            _cfg_get(config, "carbon_offset_cost_threshold",
                     self.RUN_OFFSET_COST_THRESHOLD)
            or self.RUN_OFFSET_COST_THRESHOLD
        )
        self.min_price: float = float(
            _cfg_get(config, "carbon_min_price_usd", self.MIN_PRICE_USD)
            or self.MIN_PRICE_USD
        )
        self.max_price: float = float(
            _cfg_get(config, "carbon_max_price_usd", self.MAX_PRICE_USD)
            or self.MAX_PRICE_USD
        )

        # In-memory state (source of truth; storage is best-effort durable).
        self._rec_balance_mwh: float = 0.0
        self._price_history: Deque[Tuple[float, float]] = deque(
            maxlen=self.PRICE_HISTORY_MAX,
        )
        self._purchase_cache: Dict[str, float] = {}
        self._purchase_order: Deque[str] = deque()
        self._closed: bool = False
        self._storage_available: bool = storage is not None

        # Metrics init.
        try:
            _M_PRICE.set(self.last_price)
            _M_REC_BALANCE.set(self._rec_balance_mwh)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Storage helper (soft-fail)
    # ------------------------------------------------------------------
    async def _safe_storage(self, method_name: str, *args: Any,
                            **kwargs: Any) -> Any:
        """Call a storage method, swallowing exceptions and counting them."""
        if self.storage is None:
            return None
        try:
            method = getattr(self.storage, method_name, None)
        except Exception:
            method = None
        if method is None:
            return None
        try:
            return await asyncio.to_thread(method, *args, **kwargs)
        except Exception as exc:
            log_event("warning",
                      f"Storage call {method_name} failed: {exc}")
            try:
                _M_STORAGE_FAILURES.labels(method=method_name).inc()
            except Exception:
                pass
            return None

    # ------------------------------------------------------------------
    # Price
    # ------------------------------------------------------------------
    async def _fetch_price(self) -> float:
        """
        Simulate fetching the current market price.

        Async to preserve the original interface. Uses a Gaussian random
        walk around `last_price`, clamped to [min_price, max_price].
        """
        drift = random.gauss(0.0, 1.5)
        candidate = self.last_price + drift
        return max(self.min_price, min(self.max_price, candidate))

    async def update_price(self) -> float:
        """Fetch a new price, persist it (best effort), and update state."""
        try:
            price = await self._fetch_price()
        except Exception as exc:
            log_event("warning", f"Price fetch failed: {exc}")
            price = self.last_price

        # Clamp defensively in case a subclass overrides _fetch_price.
        try:
            price = max(self.min_price, min(self.max_price, float(price)))
        except (TypeError, ValueError):
            price = self.last_price

        # Persist first, then mutate in-memory state. The in-memory value is
        # the source of truth; a failed save does not roll it back.
        await self._safe_storage("save_credit_price", price)

        self.last_price = price
        try:
            self._price_history.append((time.time(), price))
        except Exception:
            pass
        try:
            _M_PRICE.set(price)
            if self._price_history:
                prices = [p for _, p in self._price_history]
                _M_PRICE_MIN.set(min(prices))
                _M_PRICE_MAX.set(max(prices))
        except Exception:
            pass
        return price

    # ------------------------------------------------------------------
    # REC purchases
    # ------------------------------------------------------------------
    async def purchase_rec(
        self,
        mwh: float,
        price_per_mwh: float = 5.0,
        source: str = "wind",
        *,
        request_id: Optional[str] = None,
        use_market_price: bool = False,
    ) -> float:
        """
        Purchase `mwh` megawatt-hours of RECs.

        Idempotent when `request_id` is provided: replaying the same request
        returns the cached cost without recording a duplicate purchase.

        Note: the in-memory balance is authoritative for `get_rec_balance`.
        Call `refresh_rec_balance()` to sync from storage on startup.
        """
        if self._closed:
            raise RuntimeError("CarbonMarketIntegrator is closed")

        mwh = _as_nonneg_float(mwh, 0.0, name="mwh")
        if mwh == 0.0:
            return 0.0

        if use_market_price:
            price_per_mwh = float(self.last_price)
        price_per_mwh = _as_nonneg_float(
            price_per_mwh, self.DEFAULT_PRICE_PER_MWH_USD, name="price_per_mwh",
        )

        # Normalise source.
        src = (source or "").strip().lower()
        if not src:
            src = "unspecified"
        if src not in self.ALLOWED_REC_SOURCES:
            # Do not reject; log for auditability and record verbatim.
            log_event("warning", f"Unknown REC source {source!r}; recording as-is")

        # Idempotency.
        if request_id is not None:
            cached = self._purchase_cache.get(request_id)
            if cached is not None:
                return cached

        cost = mwh * price_per_mwh

        # Update in-memory state first so it stays consistent even if the
        # storage write fails.
        self._rec_balance_mwh += mwh

        # Persist (best effort).
        await self._safe_storage("save_rec", mwh, price_per_mwh, src)

        try:
            _M_PURCHASES.labels(source=src).inc()
            _M_REC_BALANCE.set(self._rec_balance_mwh)
        except Exception:
            pass

        # Bounded idempotency cache.
        if request_id is not None:
            self._purchase_cache[request_id] = cost
            self._purchase_order.append(request_id)
            while len(self._purchase_order) > self.IDEMPOTENCY_CACHE_MAX:
                old = self._purchase_order.popleft()
                self._purchase_cache.pop(old, None)

        return cost

    def get_rec_balance(self) -> float:
        """Return the in-memory REC balance (MWh)."""
        return float(self._rec_balance_mwh)

    async def refresh_rec_balance(self) -> float:
        """Sync the in-memory REC balance from storage (best effort)."""
        balance = await self._safe_storage("get_rec_balance")
        try:
            if balance is not None:
                self._rec_balance_mwh = float(balance)
                try:
                    _M_REC_BALANCE.set(self._rec_balance_mwh)
                except Exception:
                    pass
        except (TypeError, ValueError):
            pass
        return self._rec_balance_mwh

    # ------------------------------------------------------------------
    # Scheduling
    # ------------------------------------------------------------------
    async def net_zero_schedule(
        self,
        workload_kwh: float,
        intensity: float,
        *,
        price: Optional[float] = None,
        commit: bool = True,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Decide how to run a workload given the grid carbon intensity.

        `intensity` is expected in kgCO2/kWh. `price` is the credit price to
        use for the offset decision; when omitted, the last observed price is
        used WITHOUT refreshing it — price refresh is a separate concern
        (see `update_price`).

        Set `commit=False` to compute the decision without persisting a
        match record (see also `quote()`).
        """
        if self._closed:
            raise RuntimeError("CarbonMarketIntegrator is closed")

        workload_kwh = _as_nonneg_float(
            workload_kwh, 0.0, name="workload_kwh")
        intensity = _as_nonneg_float(intensity, 0.0, name="intensity")

        if price is None:
            price = float(self.last_price)
        else:
            price = _as_nonneg_float(price, float(self.last_price), name="price")

        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price

        if intensity > self.defer_threshold:
            action = "defer"
        elif offset_cost < self.offset_threshold:
            action = "run_offset"
        else:
            action = "run"

        req_id = request_id or uuid.uuid4().hex[:8]

        if commit:
            await self._safe_storage(
                "save_net_zero_match",
                req_id, workload_kwh, intensity, action,
                carbon_kg, offset_cost, price,
            )

        try:
            _M_DECISIONS.labels(action=action).inc()
        except Exception:
            pass

        return {
            "action": action,
            "carbon_kg": carbon_kg,
            "offset_cost_usd": offset_cost,
            "credit_price_usd": price,
            "rec_balance_mwh": float(self._rec_balance_mwh),
            "request_id": req_id,
        }

    async def quote(
        self,
        workload_kwh: float,
        intensity: float,
        *,
        price: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Like `net_zero_schedule` but does not commit a match."""
        return await self.net_zero_schedule(
            workload_kwh, intensity, price=price, commit=False,
        )

    # ------------------------------------------------------------------
    # Lifecycle / diagnostics
    # ------------------------------------------------------------------
    async def close(self) -> None:
        self._closed = True

    async def __aenter__(self) -> "CarbonMarketIntegrator":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    def stats(self) -> Dict[str, Any]:
        return {
            "last_price_usd": self.last_price,
            "min_price_usd": self.min_price,
            "max_price_usd": self.max_price,
            "rec_balance_mwh": self._rec_balance_mwh,
            "price_history_len": len(self._price_history),
            "purchases_cached": len(self._purchase_cache),
            "closed": self._closed,
            "storage_available": self._storage_available,
        }
