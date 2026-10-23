"""
carbon_forecast.py — Enhanced v18.0.0
======================================

Carbon intensity forecast provider for the Green Agent.

v18.0.0 is a **correctness- and honesty-focused** release on top of v17.0.0.

Correctness & reliability
-------------------------
* **CircuitBreaker HALF_OPEN** now permits only *one* probe in flight
  (the previous version could cascade failures under concurrency).
* **Retry with exponential backoff** is actually implemented and used
  around the HTTP call (v17 promised it in the docstring but never did).
* **Sync API prefers the last known API value** over a random walk.
  v17's ``current_intensity()`` could never reach the network at all —
  this is now documented, and the sync path returns the last API value
  (or cached derived value) when one exists.
* **``_fetch_from_api`` raises** on missing ``carbonIntensity`` instead of
  silently defaulting to ``400`` (which made bad data look valid).
* **Unified timezone-aware datetimes** everywhere (``datetime.now(
  timezone.utc)``); legacy naive datetimes are migrated on load.
* **Per-region history deques** and a **bounded cache**.
* **Thread/async-safe mutable state** via a single ``threading.RLock``
  and a separate ``asyncio.Lock`` for the aiohttp session.
* **``Orchestrator.start()`` is idempotent**, and ``shutdown()`` resets
  the shutdown event so ``start()`` can be called again.
* **``asyncio.get_running_loop()``** (3.12 safe).
* **``inspect.isawaitable``** replaces ``asyncio.iscoroutinefunction``
  (fixes ``functools.partial`` + double-call bug).
* **Std=0 guard** in ``SimpleCausalGraphLearner``.

Observability & ergonomics
--------------------------
* ``ForecastBatch.to_dict()`` / ``from_dict()`` for persistence & interop.
* ``current_intensity_async`` returns ``(value, ForecastSource)`` —
  callers can distinguish API / cache / history / fallback.
* ``stats()`` exposes ``hook_failures_total``, per-hook failure counts,
  ``last_api_success``, ``last_api_error``, ``circuit_state``.
* ``fail_mode`` config: ``"silent" | "log" | "raise"``.
* ``ForecastPoint`` / ``ForecastBatch`` **docstrings are honest**:
  the "ten enhancements" below are **simplified reference
  implementations**, not production-grade ML.
* Async context managers on ``CarbonForecast`` and
  ``CarbonForecastOrchestratorV18``.

Backward compatibility
----------------------
* The v1.0 sync API (``current_intensity``, ``forecast_next_hours``) is
  preserved, with the corrected semantics noted above.
* The v17 async API (``current_intensity_async``,
  ``forecast_next_hours_async``) keeps its signature. Note that
  ``current_intensity_async`` now returns a ``(float, ForecastSource)``
  tuple — pass ``legacy_float=True`` to get the raw float.
* ``QuantumDistillationEngine``, ``CausalGraphLearner``,
  ``CausalPolicyAdapter``, ``FederatedGreenAggregator``,
  ``MultiAgentCoordinator``, ``TemporalLogicVerifier``,
  ``XAIDecisionExplainer``, ``AdaptivePrecisionSwitcher``,
  ``CarbonMarketIntegrator``, ``ChaosTestingEngine``, and
  ``ActiveUserPreferenceLearner`` keep their class names (and the
  ``Simple*`` aliases are added). Their docstrings now explicitly say
  *simplified reference implementation*.
* ``CarbonForecastOrchestratorV17`` is aliased to
  ``CarbonForecastOrchestratorV18``.

Self-contained: Python stdlib + optional numpy / sklearn / torch / aiohttp.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import math
import random
import re as _re
import statistics
import threading
import time
import uuid
from collections import OrderedDict, defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

# --------------------------------------------------------------------------- #
# Optional dependencies
# --------------------------------------------------------------------------- #
try:
    import numpy as np  # type: ignore
    NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    NUMPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression  # type: ignore
    SKLEARN_AVAILABLE = True
except ImportError:  # pragma: no cover
    SKLEARN_AVAILABLE = False

try:
    import torch  # type: ignore
    TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover
    TORCH_AVAILABLE = False

try:
    import aiohttp  # type: ignore
    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    AIOHTTP_AVAILABLE = False


logger = logging.getLogger(__name__)

_MEMORY_SENTINEL = ":memory:"
_VALID_FAIL_MODES = ("silent", "log", "raise")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_timestamp(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return _ensure_aware(raw)
    if isinstance(raw, str):
        s = raw.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return _ensure_aware(datetime.fromisoformat(s))
        except ValueError:
            return _utcnow()
    return _utcnow()


def _json_safe(obj: Any) -> Any:
    """Make *obj* JSON-serialisable (inf/nan → None, datetimes → ISO)."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, datetime):
        return _ensure_aware(obj).isoformat()
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(v) for v in obj]
    return obj


def _cfg_get(config: Any, key: str, default: Any = None) -> Any:
    """Read a key from a dict / pydantic-v1 / pydantic-v2 / object config."""
    if config is None:
        return default
    if isinstance(config, Mapping):
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


def _validate_config(config: Any) -> None:
    """Validate configuration; raise ``ValueError`` on obvious mistakes."""
    if config is None:
        return

    ttl = _cfg_get(config, "carbon_cache_ttl", 300)
    if isinstance(ttl, bool) or not isinstance(ttl, int) or ttl <= 0:
        raise ValueError(f"carbon_cache_ttl must be a positive int, got {ttl!r}")

    fail_mode = _cfg_get(config, "fail_mode", "log")
    if fail_mode not in _VALID_FAIL_MODES:
        raise ValueError(
            f"fail_mode must be one of {_VALID_FAIL_MODES}, got {fail_mode!r}"
        )

    region = _cfg_get(config, "carbon_region", "global")
    if not isinstance(region, str) or not region.strip():
        raise ValueError(f"carbon_region must be a non-empty string, got {region!r}")

    threshold = _cfg_get(config, "circuit_failure_threshold", 3)
    if isinstance(threshold, bool) or not isinstance(threshold, int) or threshold <= 0:
        raise ValueError(
            f"circuit_failure_threshold must be a positive int, got {threshold!r}"
        )

    recovery = _cfg_get(config, "circuit_recovery_timeout", 60.0)
    if not isinstance(recovery, (int, float)) or recovery <= 0:
        raise ValueError(
            f"circuit_recovery_timeout must be > 0, got {recovery!r}"
        )


async def _retry_with_backoff(
    fn: Callable[[], Any],
    *,
    attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 4.0,
    exceptions: Tuple[type, ...] = (Exception,),
) -> Any:
    """Retry an awaitable-producing callable with exponential backoff + jitter."""
    last_exc: Optional[BaseException] = None
    for i in range(attempts):
        try:
            result = fn()
            if inspect.isawaitable(result):
                result = await result
            return result
        except exceptions as exc:  # type: ignore[misc]
            last_exc = exc
            if i == attempts - 1:
                break
            delay = min(max_delay, base_delay * (2 ** i))
            delay += random.uniform(0.0, base_delay * 0.1)
            logger.debug(
                "Retry %d/%d after %.3fs: %s", i + 1, attempts, delay, exc
            )
            await asyncio.sleep(delay)
    assert last_exc is not None
    raise last_exc


# --------------------------------------------------------------------------- #
# Data structures
# --------------------------------------------------------------------------- #
class ForecastSource(Enum):
    """Where a forecast value came from (critical for data provenance)."""

    API = "api"
    CACHE = "cache"
    HISTORY = "history"
    LAST_API = "last_api"
    RANDOM_WALK = "random_walk"
    FALLBACK = "fallback"


@dataclass
class ForecastPoint:
    """A single timestamped carbon-intensity point."""

    timestamp: str
    intensity: float
    source: str
    region: str = "global"
    confidence: float = 0.5

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ForecastPoint":
        return cls(
            timestamp=str(data.get("timestamp", "")),
            intensity=float(data.get("intensity", 0.0)),
            source=str(data.get("source", ForecastSource.FALLBACK.value)),
            region=str(data.get("region", "global")),
            confidence=float(data.get("confidence", 0.5)),
        )


@dataclass
class ForecastBatch:
    """A collection of forecast points with metadata."""

    batch_id: str
    region: str
    generated_at: str
    horizon_hours: int
    points: List[ForecastPoint] = field(default_factory=list)
    confidence: float = 0.5
    xai_explanation: Optional[Dict[str, Any]] = None
    temporal_violations: List[str] = field(default_factory=list)
    duration_ms: float = 0.0
    source: str = ForecastSource.FALLBACK.value

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(
            {
                "batch_id": self.batch_id,
                "region": self.region,
                "generated_at": self.generated_at,
                "horizon_hours": self.horizon_hours,
                "points": [p.to_dict() for p in self.points],
                "confidence": self.confidence,
                "xai_explanation": self.xai_explanation,
                "temporal_violations": self.temporal_violations,
                "duration_ms": self.duration_ms,
                "source": self.source,
            }
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ForecastBatch":
        raw_points = data.get("points", []) or []
        points = [
            ForecastPoint.from_dict(p) if isinstance(p, Mapping) else p
            for p in raw_points
        ]
        return cls(
            batch_id=str(data.get("batch_id", "")),
            region=str(data.get("region", "global")),
            generated_at=str(data.get("generated_at", "")),
            horizon_hours=int(data.get("horizon_hours", 0)),
            points=points,
            confidence=float(data.get("confidence", 0.5) or 0.0),
            xai_explanation=data.get("xai_explanation"),
            temporal_violations=list(data.get("temporal_violations", []) or []),
            duration_ms=float(data.get("duration_ms", 0.0) or 0.0),
            source=str(data.get("source", ForecastSource.FALLBACK.value)),
        )


# --------------------------------------------------------------------------- #
# Circuit breaker (fixed: single-probe HALF_OPEN)
# --------------------------------------------------------------------------- #
class CircuitBreaker:
    """
    Thread-safe circuit breaker for external API calls.

    * ``CLOSED``   → all calls pass.
    * ``OPEN``     → all calls fail fast until ``recovery_timeout`` elapses.
    * ``HALF_OPEN`` → exactly **one** probe is allowed through; success
      returns to ``CLOSED``, failure returns to ``OPEN``.
    """

    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_timeout: float = 60.0,
        name: str = "default",
    ) -> None:
        if failure_threshold <= 0:
            raise ValueError("failure_threshold must be > 0")
        if recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be > 0")
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self._failures = 0
        self._last_failure_time: Optional[datetime] = None
        self._state = "CLOSED"
        self._probe_in_flight = False
        self._lock = threading.RLock()

    def _acquire_slot(self) -> None:
        """Return if the caller may proceed; raise if OPEN / probe busy."""
        with self._lock:
            if self._state == "CLOSED":
                return
            if self._state == "OPEN":
                elapsed = (
                    (_utcnow() - self._last_failure_time).total_seconds()
                    if self._last_failure_time
                    else self.recovery_timeout + 1.0
                )
                if elapsed <= self.recovery_timeout:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")
                # Recovery window elapsed — try to enter HALF_OPEN.
                self._state = "HALF_OPEN"
            if self._state == "HALF_OPEN":
                if self._probe_in_flight:
                    raise RuntimeError(
                        f"Circuit breaker {self.name} is OPEN (probe in flight)"
                    )
                self._probe_in_flight = True

    def _on_success(self) -> None:
        with self._lock:
            self._state = "CLOSED"
            self._failures = 0
            self._probe_in_flight = False

    def _on_failure(self) -> None:
        with self._lock:
            self._failures += 1
            self._last_failure_time = _utcnow()
            self._probe_in_flight = False
            if self._failures >= self.failure_threshold or self._state == "HALF_OPEN":
                self._state = "OPEN"

    async def call(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        self._acquire_slot()
        try:
            result = fn(*args, **kwargs)
            if inspect.isawaitable(result):
                result = await result
            self._on_success()
            return result
        except Exception:
            self._on_failure()
            raise

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    @property
    def failure_count(self) -> int:
        with self._lock:
            return self._failures


# --------------------------------------------------------------------------- #
# Storage protocol + in-memory implementation
# --------------------------------------------------------------------------- #
class StorageProtocol:
    """Duck-typed storage protocol. Any object with these methods works."""

    def save_carbon_intensity(
        self, region: str, intensity: float, timestamp: Optional[str] = None
    ) -> None: ...
    def get_carbon_intensity(self, region: str) -> Optional[float]: ...
    def save_teacher_superposition(self, *a: Any, **kw: Any) -> None: ...
    def save_causal_edge(
        self, source: str, target: str, weight: float, confidence: float
    ) -> None: ...
    def save_causal_experiment(self, *a: Any, **kw: Any) -> None: ...
    def save_federated_weights(self, *a: Any, **kw: Any) -> None: ...
    def get_federated_weights(self, model_id: str) -> List[Dict[str, Any]]: ...
    def save_precision_switch(self, *a: Any, **kw: Any) -> None: ...
    def save_credit_price(self, price_usd: float, **kw: Any) -> None: ...
    def save_rec(self, mwh: float, price_per_mwh: float, source: str, **kw: Any) -> None: ...
    def save_net_zero_match(self, *a: Any, **kw: Any) -> None: ...
    def save_chaos_experiment(self, *a: Any, **kw: Any) -> None: ...
    def save_temporal_trace(self, state: Mapping[str, Any], context: Any = None) -> None: ...
    def save_xai_explanation(self, *a: Any, **kw: Any) -> None: ...
    def save_user_preference(self, user_id: str, weights: Mapping[str, float]) -> None: ...
    def get_user_preference(self, user_id: str) -> Optional[Dict[str, float]]: ...


class InMemoryStorage:
    """
    Bounded in-memory storage for demos and tests.

    Every ``save_*`` list is capped at ``max_entries``; oldest entries are
    evicted first. Persistence of a real deployment should be supplied by
    the caller.
    """

    def __init__(self, max_entries: int = 10_000) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries must be > 0")
        self.max_entries = max_entries
        self._data: Dict[str, Deque[Any]] = defaultdict(
            lambda: deque(maxlen=max_entries)
        )
        self._prefs: Dict[str, Dict[str, float]] = {}

    # ------------------------------------------------------------------
    def _record(self, bucket: str, entry: Any) -> None:
        self._data[bucket].append(entry)

    def save_carbon_intensity(
        self, region: str, intensity: float, timestamp: Optional[str] = None
    ) -> None:
        self._record(
            "carbon_intensity",
            {
                "region": region,
                "intensity": float(intensity),
                "ts": timestamp or _utcnow().isoformat(),
            },
        )

    def get_carbon_intensity(self, region: str) -> Optional[float]:
        for entry in reversed(self._data["carbon_intensity"]):
            if entry["region"] == region:
                return entry["intensity"]
        return None

    def save_teacher_superposition(self, *a: Any, **kw: Any) -> None:
        self._record("teacher_superpositions", a)

    def save_causal_edge(
        self, source: str, target: str, weight: float, confidence: float
    ) -> None:
        self._record(
            "causal_graph",
            {"source": source, "target": target, "weight": float(weight)},
        )

    def save_causal_experiment(
        self,
        exp_id: str,
        treatment: str,
        outcome: str,
        ate: float,
        samples: int,
        method: str = "",
    ) -> None:
        self._record(
            "causal_experiments",
            {"exp_id": exp_id, "ate": float(ate), "method": method},
        )

    def save_federated_weights(
        self,
        instance_id: str,
        model_id: str,
        weights: bytes,
        weight_norm: float = 0.0,
        round_id: int = 0,
    ) -> None:
        self._record(
            "federated_weights",
            {
                "instance_id": instance_id,
                "model_id": model_id,
                "weights": weights,
                "round_id": round_id,
            },
        )

    def get_federated_weights(self, model_id: str) -> List[Dict[str, Any]]:
        return [r for r in self._data["federated_weights"] if r["model_id"] == model_id]

    def save_precision_switch(self, *a: Any, **kw: Any) -> None:
        self._record("precision_history", a)

    def save_credit_price(self, price_usd: float, **kw: Any) -> None:
        self._record("carbon_credit_prices", {"price_usd": float(price_usd)})

    def save_rec(self, mwh: float, price_per_mwh: float, source: str, **kw: Any) -> None:
        self._record(
            "rec_ledger",
            {"mwh": float(mwh), "price_per_mwh": float(price_per_mwh), "source": source},
        )

    def get_rec_balance(self) -> float:
        return sum(r["mwh"] for r in self._data["rec_ledger"])

    def save_net_zero_match(self, *a: Any, **kw: Any) -> None:
        self._record("net_zero_matches", a)

    def save_chaos_experiment(self, *a: Any, **kw: Any) -> None:
        self._record("chaos_experiments", a)

    def save_temporal_trace(
        self, state: Mapping[str, Any], context: Any = None
    ) -> None:
        self._record("temporal_trace", {"state": dict(state), "context": context})

    def save_temporal_rule(self, *a: Any, **kw: Any) -> None:
        self._record("temporal_rules", a)

    def save_temporal_violation(self, *a: Any, **kw: Any) -> None:
        self._record("temporal_violations", a)

    def save_xai_explanation(
        self,
        explanation_id: str,
        decision_id: str,
        method: str,
        label: str,
        features: Any,
        attributions: Mapping[str, float],
        nl: str,
    ) -> None:
        self._record(
            "xai_explanations",
            {
                "explanation_id": explanation_id,
                "decision_id": decision_id,
                "method": method,
                "label": label,
                "attributions": dict(attributions),
                "nl": nl,
            },
        )

    def save_user_preference(self, user_id: str, weights: Mapping[str, float]) -> None:
        self._prefs[user_id] = dict(weights)

    def get_user_preference(self, user_id: str) -> Optional[Dict[str, float]]:
        return self._prefs.get(user_id)

    def reset(self) -> None:
        self._data.clear()
        self._prefs.clear()


# --------------------------------------------------------------------------- #
# CarbonForecast (the target of this enhancement)
# --------------------------------------------------------------------------- #
class CarbonForecast:
    """
    Carbon intensity forecast provider for the Green Agent.

    v18.0.0 features:
      * Region-aware, per-region history and bounded cache.
      * Async HTTP client (aiohttp) with retry + circuit breaker.
      * Sync API that **prefers the last API value** over a random walk.
      * Structured ``stats()`` including hook failures and API diagnostics.
      * ``fail_mode`` for enhancement hooks ("silent" | "log" | "raise").
      * Backward compatible with both the v1.0 sync API and the v17 async
        API (the async API gains a provenance tuple; use
        ``legacy_float=True`` for the raw value).
    """

    REGION_BASELINES: Dict[str, Tuple[float, float]] = {
        "US-CAL-CISO": (150, 400),
        "US-NE-ISNE": (200, 450),
        "US-MIDA-PJM": (300, 550),
        "EU-DE": (200, 500),
        "EU-FR": (50, 300),
        "EU-GB": (150, 450),
        "ID": (400, 700),
        "SG": (350, 600),
        "global": (300, 600),
    }

    def __init__(
        self,
        config: Optional[Any] = None,
        storage: Optional[Any] = None,
        region: str = "global",
        cache_ttl: int = 300,
        *,
        fail_mode: Optional[str] = None,
        cache_max_entries: int = 256,
        history_max_entries: int = 2000,
        api_retry_attempts: int = 3,
        api_retry_base_delay: float = 0.5,
        # Enhancement hooks (all optional)
        causal_rl: Optional["SimpleCausalPolicyAdapter"] = None,
        temporal: Optional["SimpleTemporalLogicVerifier"] = None,
        xai: Optional["SimpleXAIDecisionExplainer"] = None,
        hitl: Optional["SimpleActiveUserPreferenceLearner"] = None,
        carbon_market: Optional["SimpleCarbonMarketIntegrator"] = None,
        chaos: Optional["SimpleChaosTestingEngine"] = None,
        federated: Optional["SimpleFederatedGreenAggregator"] = None,
        multi_agent: Optional["SimpleMultiAgentCoordinator"] = None,
        precision: Optional["SimpleAdaptivePrecisionSwitcher"] = None,
        registry: Optional[Any] = None,
    ) -> None:
        _validate_config(config)

        self.config = config
        self.storage = storage
        self.region = region or _cfg_get(config, "carbon_region", "global")
        self.cache_ttl = int(_cfg_get(config, "carbon_cache_ttl", cache_ttl))
        self.api_url = _cfg_get(config, "carbon_market_api_url", None)
        self.api_key = _cfg_get(config, "electricity_maps_api_key", None)

        self._fail_mode = (
            fail_mode
            or _cfg_get(config, "fail_mode", "log")
            or "log"
        )
        if self._fail_mode not in _VALID_FAIL_MODES:
            raise ValueError(
                f"fail_mode must be one of {_VALID_FAIL_MODES}, got {self._fail_mode!r}"
            )

        self._api_retry_attempts = max(1, int(api_retry_attempts))
        self._api_retry_base_delay = max(0.0, float(api_retry_base_delay))

        # Enhancement hooks
        self.causal_rl = causal_rl
        self.temporal = temporal
        self.xai = xai
        self.hitl = hitl
        self.carbon_market = carbon_market
        self.chaos = chaos
        self.federated = federated
        self.multi_agent = multi_agent
        self.precision = precision
        self.registry = registry

        # Bounded cache: (timestamp, value, source)
        self._cache: "OrderedDict[str, Tuple[datetime, float, ForecastSource]]" = (
            OrderedDict()
        )
        self._cache_max = max(1, int(cache_max_entries))

        # Per-region history
        self._history_max = max(1, int(history_max_entries))
        self._history: Dict[str, Deque[Tuple[datetime, float]]] = defaultdict(
            lambda: deque(maxlen=self._history_max)
        )

        # Last successful API value (per region) — used by the sync path
        self._last_api_value: Dict[str, Tuple[datetime, float]] = {}

        # Forecast history (unchanged shape for backward compat)
        self.forecast_history: Deque[ForecastBatch] = deque(maxlen=200)

        # aiohttp session (created under a lock)
        self._session: Optional["aiohttp.ClientSession"] = None
        self._session_lock = asyncio.Lock()

        # Circuit breaker (configurable)
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=int(
                _cfg_get(config, "circuit_failure_threshold", 3)
            ),
            recovery_timeout=float(
                _cfg_get(config, "circuit_recovery_timeout", 60.0)
            ),
            name=f"carbon_forecast_{self.region}",
        )

        # Stats
        self._stats: Dict[str, Any] = {
            "api_calls": 0,
            "api_successes": 0,
            "api_failures": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "fallbacks": 0,
            "circuit_openings": 0,
            "total_forecasts": 0,
            "hook_failures_total": 0,
            "last_api_success": None,
            "last_api_error": None,
        }
        self._stats_lock = threading.RLock()

        # Single lock protecting cache / history / stats
        self._state_lock = threading.RLock()

        logger.info(
            "CarbonForecast v18.0.0 initialized (region=%s, ttl=%ds, fail_mode=%s)",
            self.region,
            self.cache_ttl,
            self._fail_mode,
        )
        if not AIOHTTP_AVAILABLE:
            logger.warning("aiohttp not available; API path disabled")
        if not self.api_url:
            logger.info("No API URL configured; all reads will use fallback data")

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    @property
    def fail_mode(self) -> str:
        return self._fail_mode

    @property
    def circuit_state(self) -> str:
        return self._circuit_breaker.state

    @property
    def last_api_error(self) -> Optional[str]:
        with self._stats_lock:
            return self._stats.get("last_api_error")

    def history(self, region: Optional[str] = None) -> List[Tuple[datetime, float]]:
        """Return a snapshot of the history deque for ``region`` (default current)."""
        region = region or self.region
        with self._state_lock:
            return list(self._history.get(region, ()))

    # ------------------------------------------------------------------
    # Hook safety helper
    # ------------------------------------------------------------------
    async def _run_hook(self, name: str, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        Run an enhancement hook honouring ``fail_mode``.

        * ``silent`` → swallow, return None.
        * ``log``    → log a warning, return None.
        * ``raise``  → re-raise.
        All failures are counted in ``stats()['hook_failures_total']``.
        """
        try:
            result = fn(*args, **kwargs)
            if inspect.isawaitable(result):
                result = await result
            return result
        except Exception as e:
            key = f"hook_failures_{name}"
            with self._stats_lock:
                self._stats[key] = self._stats.get(key, 0) + 1
                self._stats["hook_failures_total"] += 1
            if self._fail_mode == "raise":
                raise
            if self._fail_mode == "log":
                logger.warning("Hook %r failed: %s", name, e)
            return None

    # ------------------------------------------------------------------
    # Cache helpers (must be called under _state_lock or from a single thread)
    # ------------------------------------------------------------------
    def _cache_get(
        self, key: str
    ) -> Optional[Tuple[datetime, float, ForecastSource]]:
        with self._state_lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            self._cache.move_to_end(key)
            return entry

    def _cache_put(
        self,
        key: str,
        ts: datetime,
        value: float,
        source: ForecastSource,
    ) -> None:
        with self._state_lock:
            self._cache[key] = (ts, value, source)
            self._cache.move_to_end(key)
            while len(self._cache) > self._cache_max:
                self._cache.popitem(last=False)

    def _record_history(self, region: str, ts: datetime, value: float) -> None:
        with self._state_lock:
            self._history[region].append((_ensure_aware(ts), float(value)))

    def _bump(self, key: str, delta: int = 1) -> None:
        with self._stats_lock:
            self._stats[key] = self._stats.get(key, 0) + delta

    # ------------------------------------------------------------------
    # Backward-compatible sync API (v1.0)
    # ------------------------------------------------------------------
    def current_intensity(self) -> float:
        """
        Return the current carbon intensity (sync API, v1.0 compatible).

        v18.0.0 preferred order:

        1. Fresh cache entry for this region.
        2. Last successful **API** value (so operators get real data
           even when the caller cannot await).
        3. Recent history rolling walk.
        4. Region baseline.
        """
        key = f"current_{self.region}"
        now = _utcnow()

        cached = self._cache_get(key)
        if cached is not None:
            ts, value, _source = cached
            if (now - _ensure_aware(ts)).total_seconds() < self.cache_ttl:
                self._bump("cache_hits")
                return value

        self._bump("cache_misses")

        # Prefer last known API value
        with self._state_lock:
            last_api = self._last_api_value.get(self.region)

        baseline = self.REGION_BASELINES.get(self.region, (300, 600))

        if last_api is not None:
            ts, api_value = last_api
            age_s = (now - _ensure_aware(ts)).total_seconds()
            # Accept API values up to 30 minutes old (sync path only).
            if age_s <= max(self.cache_ttl, 30 * 60):
                self._cache_put(key, now, api_value, ForecastSource.LAST_API)
                self._record_history(self.region, now, api_value)
                self._bump("fallbacks")  # still not a live API call
                return api_value

        # Random walk from history
        with self._state_lock:
            hist = self._history.get(self.region)

        if hist:
            last = hist[-1][1]
            value = max(baseline[0], min(baseline[1], last + random.gauss(0, 15)))
            source = ForecastSource.RANDOM_WALK
        else:
            value = random.uniform(*baseline)
            source = ForecastSource.FALLBACK

        self._cache_put(key, now, value, source)
        self._record_history(self.region, now, value)
        self._bump("fallbacks")
        return value

    def forecast_next_hours(self, hours: int = 4) -> Dict[int, float]:
        """
        Return forecast intensities for the next N hours (sync API).

        Uses a random walk seeded from ``current_intensity()``,
        clamped to the region's typical range.
        """
        if isinstance(hours, bool) or not isinstance(hours, int) or hours <= 0:
            raise ValueError("hours must be a positive int")
        baseline = self.REGION_BASELINES.get(self.region, (150, 500))
        current = self.current_intensity()
        out: Dict[int, float] = {}
        value = current
        for h in range(1, hours + 1):
            value = max(
                baseline[0], min(baseline[1], value + random.gauss(0, 10))
            )
            out[h] = value
        return out

    # ------------------------------------------------------------------
    # Async API (v18.0)
    # ------------------------------------------------------------------
    async def current_intensity_async(
        self,
        bypass_cache: bool = False,
        *,
        legacy_float: bool = False,
    ) -> Union[float, Tuple[float, ForecastSource]]:
        """
        Async current intensity with explicit provenance.

        Returns
        -------
        (value, source) : Tuple[float, ForecastSource]
            Or, when ``legacy_float=True``, just the ``float`` value
            (matching the v17 signature).
        """
        key = f"current_{self.region}"
        now = _utcnow()

        # 0. Occasional chaos injection (best-effort, non-fatal)
        if self.chaos is not None and random.random() < 0.05:
            await self._run_hook(
                "chaos",
                self.chaos.run_experiment,
                f"forecast_{uuid.uuid4().hex[:6]}",
                "latency",
            )

        # 1. Cache
        if not bypass_cache:
            cached = self._cache_get(key)
            if cached is not None:
                ts, value, source = cached
                if (now - _ensure_aware(ts)).total_seconds() < self.cache_ttl:
                    self._bump("cache_hits")
                    return (value, ForecastSource.CACHE) if not legacy_float else value

        self._bump("cache_misses")

        # 2. API with retry + circuit breaker
        if AIOHTTP_AVAILABLE and self.api_url:
            try:
                self._bump("api_calls")
                value = await self._circuit_breaker.call(
                    lambda: _retry_with_backoff(
                        self._fetch_from_api,
                        attempts=self._api_retry_attempts,
                        base_delay=self._api_retry_base_delay,
                    )
                )
                self._bump("api_successes")
                with self._stats_lock:
                    self._stats["last_api_success"] = now.isoformat()
                self._cache_put(key, now, value, ForecastSource.API)
                self._record_history(self.region, now, value)
                with self._state_lock:
                    self._last_api_value[self.region] = (now, value)
                return (value, ForecastSource.API) if not legacy_float else value
            except Exception as e:
                self._bump("api_failures")
                with self._stats_lock:
                    self._stats["last_api_error"] = f"{type(e).__name__}: {e}"
                if self._circuit_breaker.state == "OPEN":
                    self._bump("circuit_openings")
                if self._fail_mode == "log":
                    logger.debug("API fetch failed: %s", e)

        # 3. Fallback
        with self._state_lock:
            hist = self._history.get(self.region)
            last_api = self._last_api_value.get(self.region)

        baseline = self.REGION_BASELINES.get(self.region, (300, 600))

        if last_api is not None:
            ts, api_value = last_api
            age_s = (now - _ensure_aware(ts)).total_seconds()
            if age_s <= max(self.cache_ttl, 30 * 60):
                self._bump("fallbacks")
                self._cache_put(key, now, api_value, ForecastSource.LAST_API)
                self._record_history(self.region, now, api_value)
                return (
                    (api_value, ForecastSource.LAST_API)
                    if not legacy_float
                    else api_value
                )

        if hist:
            last = hist[-1][1]
            value = max(baseline[0], min(baseline[1], last + random.gauss(0, 15)))
            source = ForecastSource.RANDOM_WALK
        else:
            value = random.uniform(*baseline)
            source = ForecastSource.FALLBACK

        self._bump("fallbacks")
        self._cache_put(key, now, value, source)
        self._record_history(self.region, now, value)
        return (value, source) if not legacy_float else value

    async def forecast_next_hours_async(
        self,
        hours: int = 4,
        region: Optional[str] = None,
        include_xai: bool = False,
    ) -> ForecastBatch:
        """
        Async forecast with timestamped points and provenance.

        Steps:

        1. Chaos injection (best-effort).
        2. Multi-agent bid (best-effort).
        3. Carbon-market context (best-effort).
        4. Base curve (from ``current_intensity_async``).
        5. XAI explanation (optional).
        6. Temporal verification.
        7. Federated sharing.
        8. Persist anchor to storage.
        9. Multi-agent reward.
        10. HITL on unusual spikes.
        11. Registry (if present).
        """
        if isinstance(hours, bool) or not isinstance(hours, int) or hours <= 0:
            raise ValueError("hours must be a positive int")

        t0 = time.perf_counter()
        region = region or self.region
        batch_id = uuid.uuid4().hex[:8]
        now = _utcnow()

        # 1. Chaos
        if self.chaos is not None and random.random() < 0.05:
            await self._run_hook(
                "chaos",
                self.chaos.run_experiment,
                f"forecast_batch_{batch_id}",
                "latency",
            )

        # 2. Multi-agent bid
        agent_id: Optional[str] = None
        if self.multi_agent is not None:
            result = await self._run_hook(
                "multi_agent_bid",
                self.multi_agent.bid,
                {"name": f"forecast_{region}", "preferred_role": "reporter"},
            )
            if result:
                agent_id = result[0]

        # 3. Carbon market context
        if self.carbon_market is not None:
            await self._run_hook(
                "carbon_market_schedule",
                self.carbon_market.net_zero_schedule,
                workload_kwh=0.01,
                intensity=0.3,
            )

        # 4. Base curve
        baseline = self.REGION_BASELINES.get(region, (150, 500))
        try:
            current, anchor_source = await self.current_intensity_async(region=region)  # type: ignore[call-arg]
        except TypeError:
            # older signature compatibility (region kwarg not supported)
            current, anchor_source = await self.current_intensity_async()
        except Exception as e:  # pragma: no cover — defensive
            current = random.uniform(*baseline)
            anchor_source = ForecastSource.FALLBACK
            self._bump("fallbacks")
            if self._fail_mode == "log":
                logger.warning("Forecast anchor failed: %s", e)

        points: List[ForecastPoint] = []
        value = current
        for h in range(1, hours + 1):
            value = max(
                baseline[0], min(baseline[1], value + random.gauss(0, 10))
            )
            ts = (now + timedelta(hours=h)).isoformat()
            confidence = max(0.3, 1.0 - (h * 0.05))
            points.append(
                ForecastPoint(
                    timestamp=ts,
                    intensity=value,
                    source=anchor_source.value,
                    region=region,
                    confidence=confidence,
                )
            )

        batch = ForecastBatch(
            batch_id=batch_id,
            region=region,
            generated_at=now.isoformat(),
            horizon_hours=hours,
            points=points,
            confidence=statistics.mean(p.confidence for p in points),
            source=anchor_source.value,
        )

        # 5. XAI
        if include_xai and self.xai is not None and NUMPY_AVAILABLE:
            async def _xai_call():
                feats = np.array(
                    [
                        current,
                        float(hours),
                        batch.confidence,
                        float(len(self._history.get(region, ()))),
                    ]
                )

                def _score(x):
                    return float(np.dot(x, [0.5, 0.2, 0.2, 0.1]))

                return await self.xai.explain(  # type: ignore[union-attr]
                    decision_id=f"fc_{batch_id}",
                    label=f"forecast:{region}",
                    features=feats,
                    names=[
                        "current_intensity",
                        "horizon_hours",
                        "confidence",
                        "history_size",
                    ],
                    model_fn=_score,
                    include_counterfactual=False,
                )

            explanation = await self._run_hook("xai", _xai_call)
            if explanation:
                batch.xai_explanation = explanation

        # 6. Temporal verification
        if self.temporal is not None:
            async def _temporal_call():
                await self.temporal.push_state(  # type: ignore[union-attr]
                    {
                        "carbon_intensity": current,
                        "forecast_horizon": hours,
                        "quality": batch.confidence,
                    }
                )
                return await self.temporal.verify()  # type: ignore[union-attr]

            verify = await self._run_hook("temporal", _temporal_call)
            if verify:
                batch.temporal_violations = [
                    k for k, ok in verify.items() if not ok
                ]

        # 7. Federated sharing
        if self.federated is not None:
            async def _federated_call():
                blob = json.dumps(
                    {
                        "region": region,
                        "horizon": hours,
                        "current": current,
                        "confidence": batch.confidence,
                    }
                ).encode()
                await self.federated.share_weights(  # type: ignore[union-attr]
                    f"carbon_forecast_{region}", blob
                )

            await self._run_hook("federated", _federated_call)

        # 8. Persist anchor
        if self.storage is not None:
            await self._run_hook(
                "storage_anchor",
                asyncio.to_thread,
                self.storage.save_carbon_intensity,
                region,
                current,
                now.isoformat(),
            )

        # 9. Multi-agent reward
        if self.multi_agent is not None and agent_id:
            await self._run_hook(
                "multi_agent_reward",
                self.multi_agent.reward,
                agent_id,
                0.8,
            )

        # 10. HITL on unusual spikes
        if self.hitl is not None and current > baseline[1] * 1.2:
            await self._run_hook(
                "hitl",
                self.hitl.query_user_if_needed,
                "forecast_user",
                [
                    {
                        "solution_id": "acknowledge",
                        "quality_score": 0.9,
                        "carbon_g": current,
                    },
                    {
                        "solution_id": "alert",
                        "quality_score": 0.89,
                        "carbon_g": current,
                    },
                ],
                timeout=1.0,
            )

        # 11. Registry
        if self.registry is not None and hasattr(self.registry, "aregister"):
            summary = {
                "region": region,
                "horizon": hours,
                "mean_intensity": statistics.mean(p.intensity for p in points),
                "confidence": batch.confidence,
            }
            await self._run_hook(
                "registry",
                self.registry.aregister,
                "carbon_forecast",
                summary,
                explain=False,
                require_hitl=False,
                defer_if_high_carbon=False,
            )

        batch.duration_ms = (time.perf_counter() - t0) * 1000.0
        self._bump("total_forecasts")

        with self._state_lock:
            self.forecast_history.append(batch)

        return batch

    # ------------------------------------------------------------------
    # HTTP client
    # ------------------------------------------------------------------
    async def _get_session(self) -> Optional["aiohttp.ClientSession"]:
        """Lazily create (thread-safe) the aiohttp session."""
        if not AIOHTTP_AVAILABLE:
            return None
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()
            return self._session

    async def _fetch_from_api(self) -> float:
        """Fetch current intensity from an ElectricityMaps-style API."""
        session = await self._get_session()
        if session is None:
            raise RuntimeError("aiohttp not available")
        if not self.api_url:
            raise RuntimeError("api_url not configured")

        url = f"{self.api_url}/carbon-intensity/latest?zone={self.region}"
        headers = {"auth-token": self.api_key} if self.api_key else {}
        async with session.get(url, headers=headers, timeout=8) as response:
            if response.status != 200:
                raise RuntimeError(f"API returned {response.status}")
            data = await response.json()
            if "carbonIntensity" not in data:
                raise RuntimeError(
                    "API response missing 'carbonIntensity' field; "
                    f"got keys: {sorted(data.keys())}"
                )
            return float(data["carbonIntensity"])

    # ------------------------------------------------------------------
    # Drift detection
    # ------------------------------------------------------------------
    def detect_drift(
        self, window: int = 50, z_threshold: float = 2.5
    ) -> Optional[Dict[str, Any]]:
        """Detect drift in the recent carbon intensity history (current region)."""
        if window <= 1:
            raise ValueError("window must be > 1")

        with self._state_lock:
            hist = list(self._history.get(self.region, ()))

        if len(hist) < window:
            return None

        recent = [v for _, v in hist[-window:]]
        mean = statistics.mean(recent)
        std = statistics.pstdev(recent) or 1e-6
        z = abs(recent[-1] - mean) / std
        return {
            "drift": z > z_threshold,
            "z": z,
            "mean": mean,
            "current": recent[-1],
            "std": std,
            "region": self.region,
        }

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------
    def stats(self) -> Dict[str, Any]:
        with self._stats_lock:
            snapshot = dict(self._stats)
        snapshot["circuit_state"] = self._circuit_breaker.state
        snapshot["circuit_failures"] = self._circuit_breaker.failure_count
        with self._state_lock:
            snapshot["history_size"] = len(self._history.get(self.region, ()))
            snapshot["cache_size"] = len(self._cache)
        snapshot["region"] = self.region
        snapshot["fail_mode"] = self._fail_mode
        return snapshot

    def set_region(self, region: str) -> None:
        if not isinstance(region, str) or not region.strip():
            raise ValueError("region must be a non-empty string")
        self.region = region
        logger.info("CarbonForecast region set to %s", region)

    def region_list(self) -> List[str]:
        return sorted(self.REGION_BASELINES.keys())

    def _inject_history(
        self, region: str, points: Sequence[Tuple[datetime, float]]
    ) -> None:
        """
        Test helper: inject points into the history for ``region``.

        Prefer this over touching internal state directly.
        """
        with self._state_lock:
            for ts, value in points:
                self._history[region].append((_ensure_aware(ts), float(value)))

    async def close(self) -> None:
        async with self._session_lock:
            if self._session is not None and not self._session.closed:
                await self._session.close()
            self._session = None

    async def __aenter__(self) -> "CarbonForecast":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


# --------------------------------------------------------------------------- #
# ENHANCEMENT 1 — SIMPLIFIED quantum-distillation
# --------------------------------------------------------------------------- #
class QuantumDistillationEngine:
    """
    **Simplified reference implementation** of teacher→student distillation.

    The "quantum" framing in the docstring refers to:
      * teacher superposition via √p (Hellinger) averaging,
      * a softmax temperature parameter.

    This is *not* a quantum information-theoretic algorithm — it is a
    demonstration of the *integration shape* (teachers → student policy).
    """

    def __init__(
        self,
        temperature: float = 2.0,
        alpha: float = 0.5,
        n_actions: int = 5,
        learning_rate: float = 0.1,
    ) -> None:
        self.temperature = temperature
        self.alpha = alpha
        self.n_actions = n_actions
        self.learning_rate = learning_rate
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy = [1.0 / n_actions] * n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def register_teacher(self, name: str, policy: List[float]) -> None:
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[name] = [p / s for p in policy]

    def _softmax(self, x: Sequence[float], temp: float) -> List[float]:
        m = max(x)
        exps = [math.exp((v - m) / max(temp, 1e-6)) for v in x]
        s = sum(exps) or 1.0
        return [e / s for e in exps]

    def _superpose(self) -> List[float]:
        if not self.teachers:
            return list(self.student_policy)
        n = self.n_actions
        accum = [0.0] * n
        for pol in self.teachers.values():
            for i in range(min(n, len(pol))):
                accum[i] += math.sqrt(max(pol[i], 1e-9))
        accum = [a / len(self.teachers) for a in accum]
        sq = [a * a for a in accum]
        s = sum(sq) or 1.0
        return [x / s for x in sq]

    async def step(
        self, storage: Any = None, student_id: str = "carbon_student"
    ) -> Dict[str, Any]:
        target = self._softmax(self._superpose(), self.temperature)
        new = []
        for s, t in zip(self.student_policy, target):
            eps = max(1e-12, min(self.student_policy) * 0.1)
            grad = -(t / max(s, eps))
            new.append(max(1e-3, s - self.learning_rate * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]
        entry = {
            "target": target,
            "student": list(self.student_policy),
            "ts": _utcnow().isoformat(),
        }
        self.history.append(entry)
        if storage is not None:
            for tid, pol in self.teachers.items():
                try:
                    await asyncio.to_thread(
                        storage.save_teacher_superposition,
                        student_id,
                        tid,
                        pol[0] if pol else 0.0,
                        self.temperature,
                        0.0,
                        0.0,
                    )
                except Exception:
                    pass
        return entry

    def get_policy(self) -> List[float]:
        return list(self.student_policy)


SimpleQuantumDistillationEngine = QuantumDistillationEngine


# --------------------------------------------------------------------------- #
# ENHANCEMENT 2 — SIMPLIFIED causal RL
# --------------------------------------------------------------------------- #
class CausalGraphLearner:
    """
    **Simplified reference implementation**: correlation-based causal graph
    approximation.

    This is *not* true causal inference (no do-calculus, no backdoor
    adjustment, no instrumental variables). Edges are thresholded
    correlations with variance-based direction inference.
    """

    def __init__(self, storage: Any = None) -> None:
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []

    async def learn(
        self,
        samples: Sequence[Mapping[str, float]],
        variables: Sequence[str],
        threshold: float = 0.25,
    ) -> Dict[str, Any]:
        self.variables = list(variables)
        if len(samples) < 5 or not NUMPY_AVAILABLE:
            # Fallback: random sparse graph
            self.graph.clear()
            for i, s in enumerate(variables):
                for j, t in enumerate(variables):
                    if i < j and random.random() < 0.25:
                        w = random.uniform(0.1, 0.9)
                        self.graph[s][t] = {"weight": w, "confidence": w}
            return self.summary()

        X = np.array(
            [[s[v] for v in variables] for s in samples], dtype=float
        )
        if X.shape[0] < 2:
            return self.summary()

        # Guard against zero-variance columns (the v17 crash)
        std = X.std(0)
        safe_std = np.where(std > 1e-9, std, 1.0)
        X = (X - X.mean(0)) / safe_std

        corr = np.corrcoef(X, rowvar=False)
        # corr may be a scalar if only one variable; guard:
        if corr.ndim == 0:
            return self.summary()

        self.graph.clear()
        for i in range(len(variables)):
            for j in range(len(variables)):
                if i == j:
                    continue
                c = abs(float(corr[i, j]))
                if c > threshold:
                    vi = float(X[:, i].var())
                    vj = float(X[:, j].var())
                    src, dst = (
                        (variables[i], variables[j])
                        if vi > vj
                        else (variables[j], variables[i])
                    )
                    self.graph[src][dst] = {
                        "weight": float(corr[i, j]),
                        "confidence": c,
                    }
                    if self.storage is not None:
                        try:
                            await asyncio.to_thread(
                                self.storage.save_causal_edge,
                                src,
                                dst,
                                float(corr[i, j]),
                                c,
                            )
                        except Exception:
                            pass
        return self.summary()

    def parents(self, node: str) -> List[str]:
        return [s for s, e in self.graph.items() if node in e]

    def summary(self) -> Dict[str, Any]:
        return {
            "nodes": len(self.variables),
            "edges": sum(len(v) for v in self.graph.values()),
            "variables": list(self.variables),
        }


class CausalPolicyAdapter:
    """
    **Simplified reference implementation** of a causal-RL policy adapter.

    Uses Q-learning-style value updates over a fixed action set.
    """

    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(
        self, config: Any, storage: Any, graph: CausalGraphLearner
    ) -> None:
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values: Dict[str, float] = defaultdict(float)
        self.counts: Dict[str, int] = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = float(_cfg_get(config, "causal_exploration_rate", 0.1))

    async def choose_action(self, state: Any) -> str:
        if random.random() < self.epsilon:
            return random.choice(self.ACTIONS)
        return max(self.ACTIONS, key=lambda a: self.values.get(a, 0.0))

    async def update(
        self, action: str, reward: float, state: Any
    ) -> None:
        if action not in self.ACTIONS:
            action = self.ACTIONS[0]
        self.counts[action] += 1
        n = self.counts[action]
        self.values[action] += (reward - self.values[action]) / n
        vals = [self.values.get(a, 0.0) for a in self.ACTIONS]
        m = max(vals)
        exps = [math.exp((v - m) / 0.5) for v in vals]
        s = sum(exps) or 1.0
        self.policy = [e / s for e in exps]

    async def estimate_ate(
        self, treatment: str, outcome: str, samples: int = 100
    ) -> float:
        w = self.graph.graph.get(treatment, {}).get(outcome, {}).get(
            "weight", 0.0
        )
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_causal_experiment,
                    f"exp_{uuid.uuid4().hex[:8]}",
                    treatment,
                    outcome,
                    w,
                    samples,
                )
            except Exception:
                pass
        return w

    def get_policy(self) -> List[float]:
        return list(self.policy)


SimpleCausalGraphLearner = CausalGraphLearner
SimpleCausalPolicyAdapter = CausalPolicyAdapter


# --------------------------------------------------------------------------- #
# ENHANCEMENT 3 — SIMPLIFIED federated green learning
# --------------------------------------------------------------------------- #
class FederatedGreenAggregator:
    """
    **Simplified reference implementation** of federated averaging for
    byte blobs.

    The v17 aggregator averages byte-by-byte across clients — for a
    byte-string "model" that is coherent, but it is *not* a substitute
    for parameter-level federated averaging on a real network.
    """

    def __init__(
        self,
        storage: Any,
        instance_id: str,
        share_interval: float = 3600.0,
    ) -> None:
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.rounds = 0

    async def share_weights(
        self, model_id: str, weights: bytes
    ) -> None:
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id,
                model_id,
                weights,
                float(len(weights)),
                self.rounds,
            )
        except Exception:
            pass

    async def pull_aggregated_weights(
        self, model_id: str
    ) -> Optional[bytes]:
        try:
            rows = await asyncio.to_thread(
                self.storage.get_federated_weights, model_id
            )
        except Exception:
            return None
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

    async def apply_aggregated_weights(
        self, model_id: str, current: bytes
    ) -> bytes:
        agg = await self.pull_aggregated_weights(model_id)
        if agg is None:
            return current
        n = min(len(current), len(agg))
        return bytes((current[i] + agg[i]) // 2 for i in range(n))


SimpleFederatedGreenAggregator = FederatedGreenAggregator


# --------------------------------------------------------------------------- #
# ENHANCEMENT 4 — SIMPLIFIED multi-agent coordination
# --------------------------------------------------------------------------- #
class _Agent:
    ROLES = ["orchestrator", "validator", "optimizer", "reporter", "negotiator"]

    def __init__(self, agent_id: str) -> None:
        self.id = agent_id
        self.role = "validator"
        self.reputation = 0.5
        self.utilities: Dict[str, float] = {
            r: random.uniform(0.3, 0.7) for r in self.ROLES
        }
        self.completed = 0


class MultiAgentCoordinator:
    """
    **Simplified reference implementation**: fixed-size agent pool with
    role specialisation by utility argmax. Not a negotiation framework.
    """

    def __init__(self, config: Any, storage: Any) -> None:
        self.config = config
        self.storage = storage
        count = int(_cfg_get(config, "agent_count", 5))
        if count <= 0:
            raise ValueError("agent_count must be > 0")
        self.agents: Dict[str, _Agent] = {
            f"agent_{i:02d}": _Agent(f"agent_{i:02d}") for i in range(count)
        }
        self.bus: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()

    async def _specialise(self) -> None:
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])

    async def broadcast(
        self, topic: str, sender: str, payload: Any
    ) -> None:
        try:
            self.bus.put_nowait(
                {"topic": topic, "sender": sender, "payload": payload}
            )
        except asyncio.QueueFull:
            pass

    async def bid(self, task: Mapping[str, Any]) -> Tuple[str, float]:
        preferred = task.get("preferred_role", "orchestrator")
        best_id: Optional[str] = None
        best_score = -1.0
        async with self._lock:
            for aid, a in self.agents.items():
                bonus = 1.0 if a.role == preferred else 0.6
                score = a.utilities[a.role] * bonus + 0.3 * a.reputation
                score += random.uniform(-0.02, 0.02)
                if score > best_score:
                    best_score, best_id = score, aid
            if best_id:
                self.agents[best_id].completed += 1
        return best_id or next(iter(self.agents)), best_score

    async def reward(self, agent_id: str, reward: float) -> None:
        async with self._lock:
            if agent_id in self.agents:
                a = self.agents[agent_id]
                n = max(1, a.completed)
                a.reputation = max(0.0, min(1.0, a.reputation + reward / n))
                a.utilities[a.role] = min(1.0, a.utilities[a.role] + 0.05 * reward)

    def get_policy(self) -> List[float]:
        counts: Dict[str, int] = defaultdict(int)
        for a in self.agents.values():
            counts[a.role] += 1
        total = max(1, sum(counts.values()))
        out = [0.0] * 5
        for role, c in counts.items():
            w = c / total
            for i in range(5):
                out[i] += w * 0.2
        s = sum(out) or 1.0
        return [x / s for x in out]

    async def step(self) -> Dict[str, Any]:
        await self._specialise()
        processed = 0
        while not self.bus.empty():
            try:
                self.bus.get_nowait()
                processed += 1
            except asyncio.QueueEmpty:
                break
        return {
            "roles": {a.id: a.role for a in self.agents.values()},
            "role_distribution": self.get_policy(),
            "processed_messages": processed,
        }


SimpleMultiAgentCoordinator = MultiAgentCoordinator


# --------------------------------------------------------------------------- #
# ENHANCEMENT 5 — SIMPLIFIED temporal logic
# --------------------------------------------------------------------------- #
_ATOMIC_RE = _re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$"
)


class TemporalRule:
    """Atomic temporal rule (``G`` / ``F`` / ``NEVER`` + a comparison)."""

    def __init__(
        self,
        rule_id: str,
        operator: str,
        conditions: List[Callable[[Mapping[str, Any]], bool]],
        window: float = 0.0,
        description: str = "",
        severity: str = "warning",
    ) -> None:
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window = window
        self.description = description or rule_id
        self.severity = severity
        self.violations = 0
        self.last_violation: Optional[datetime] = None

    def evaluate(self, trace: Deque[Tuple[datetime, Dict[str, Any]]]) -> bool:
        """Return ``True`` iff the rule is **violated**."""
        if not trace:
            return False

        if self.window > 0 and trace:
            cutoff = trace[-1][0] - timedelta(seconds=self.window)
            while trace and trace[0][0] < cutoff:
                trace.popleft()

        if self.operator == "always":
            return any(not self.conditions[0](s) for _, s in trace)
        if self.operator == "eventually":
            return not any(self.conditions[0](s) for _, s in trace)
        if self.operator == "never":
            return any(self.conditions[0](s) for _, s in trace)
        return False


class TemporalLogicVerifier:
    """
    **Simplified reference implementation**: bounded ``G`` / ``F`` /
    ``NEVER`` over atomic comparisons. Full LTL (``U``, ``&&``, ``→``)
    is out of scope.
    """

    def __init__(self, storage: Any, config: Any) -> None:
        self.storage = storage
        self.config = config
        maxlen = int(_cfg_get(config, "temporal_max_trace", 2000))
        self.trace: Deque[Tuple[datetime, Dict[str, Any]]] = deque(maxlen=maxlen)
        self.rules: Dict[str, TemporalRule] = {}
        self.approval_cb: Optional[Callable[..., Any]] = None
        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            self._install(formula)

    def _install(self, formula: str) -> None:
        f = formula.strip()
        if f.startswith("G "):
            self._add_atomic(f[2:].strip().strip("()"), "always")
        elif f.startswith("F "):
            self._add_atomic(f[2:].strip().strip("()"), "eventually")
        elif f.startswith("NEVER "):
            self._add_atomic(f[6:].strip().strip("()"), "never")

    def _add_atomic(self, expr: str, op: str) -> None:
        m = _ATOMIC_RE.match(expr)
        if not m:
            logger.warning("Ignoring malformed temporal formula: %r", expr)
            return
        var, cmp, val = m.group(1), m.group(2), float(m.group(3))

        def cond(state: Mapping[str, Any], v: str = var, c: str = cmp, x: float = val) -> bool:
            try:
                sv = float(state.get(v, 0.0))
            except Exception:
                return True
            return {
                ">=": sv >= x,
                "<=": sv <= x,
                "==": sv == x,
                "!=": sv != x,
                ">": sv > x,
                "<": sv < x,
            }[c]

        rid = f"{op}:{expr}"
        self.rules[rid] = TemporalRule(
            rid, op, [cond], description=expr, severity="warning"
        )

    def set_approval_callback(self, cb: Callable[..., Any]) -> None:
        self.approval_cb = cb

    async def push_state(self, state: Mapping[str, Any]) -> None:
        self.trace.append((_utcnow(), dict(state)))
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_temporal_trace, state
                )
            except Exception:
                pass

    async def verify(self) -> Dict[str, bool]:
        result: Dict[str, bool] = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            violated = rule.evaluate(copy)
            result[rid] = not violated
            if violated:
                rule.violations += 1
                rule.last_violation = _utcnow()
                if rule.severity == "critical" and self.approval_cb:
                    try:
                        approved = self.approval_cb(rid, self.trace[-1][1])
                        if inspect.isawaitable(approved):
                            await approved
                    except Exception:
                        pass
        return result


SimpleTemporalLogicVerifier = TemporalLogicVerifier


# --------------------------------------------------------------------------- #
# ENHANCEMENT 6 — SIMPLIFIED XAI (honest naming)
# --------------------------------------------------------------------------- #
class XAIDecisionExplainer:
    """
    **Simplified reference implementation** of local feature attribution.

    Methods:

    * ``"permutation"`` → **permutation importance** relative to a
      zero-vector baseline. Not SHAP.
    * ``"local_linear"`` → **local linear surrogate** with a Gaussian
      kernel. Not LIME (no binary interpretable representation).

    Both methods provide *directional sensitivity* attribution, not
    Shapley values.
    """

    def __init__(self, config: Any, storage: Any) -> None:
        self.config = config
        self.storage = storage
        self.method = _cfg_get(config, "xai_method", "permutation")
        # Backward compat: map the old v17 method names
        if self.method == "kernel_shap":
            self.method = "permutation"
        elif self.method == "lime":
            self.method = "local_linear"
        if self.method not in ("permutation", "local_linear"):
            raise ValueError(
                f"xai_method must be 'permutation' or 'local_linear', "
                f"got {self.method!r}"
            )
        self.depth = int(_cfg_get(config, "xai_depth", 5))
        self.global_attributions: Deque[Dict[str, float]] = deque(maxlen=1000)

    def _permutation_importance(
        self,
        f: Callable[[Any], float],
        x: Any,
        names: Sequence[str],
        n: int = 32,
    ) -> Dict[str, float]:
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
        base = np.zeros_like(x)
        contrib = np.zeros(len(x))
        for _ in range(n):
            perm = list(range(len(x)))
            random.shuffle(perm)
            prev = base.copy()
            for i in perm:
                cur = prev.copy()
                cur[i] = x[i]
                try:
                    delta = float(f(cur.reshape(1, -1))) - float(
                        f(prev.reshape(1, -1))
                    )
                except Exception:
                    delta = 0.0
                contrib[i] += delta
                prev = cur
        contrib /= max(1, n)
        return dict(zip(names, contrib.tolist()))

    def _local_linear(
        self,
        f: Callable[[Any], float],
        x: Any,
        names: Sequence[str],
        n: int = 100,
    ) -> Dict[str, float]:
        if not (SKLEARN_AVAILABLE and NUMPY_AVAILABLE):
            return {k: random.uniform(-1, 1) for k in names}
        X = np.tile(x, (n, 1)) + np.random.normal(0, 0.1, (n, len(x)))
        try:
            y = np.array([float(f(r.reshape(1, -1))) for r in X])
        except Exception:
            return {k: 0.0 for k in names}
        w = np.exp(-np.sum((X - x) ** 2, axis=1) / 0.02)
        try:
            m = LinearRegression().fit(X, y, sample_weight=w)
            return dict(zip(names, m.coef_.tolist()))
        except Exception:
            return {k: 0.0 for k in names}

    def _nl(self, decision: str, attrs: Mapping[str, float]) -> str:
        top = sorted(
            attrs.items(), key=lambda kv: abs(kv[1]), reverse=True
        )[: self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(
        self,
        decision_id: str,
        label: str,
        features: Any,
        names: Sequence[str],
        model_fn: Callable[[Any], float],
        include_counterfactual: bool = False,
        include_anchors: bool = False,
    ) -> Dict[str, Any]:
        if not NUMPY_AVAILABLE:
            attrs = {k: 0.0 for k in names}
        else:
            x = np.asarray(features, dtype=float)
            if self.method == "local_linear":
                attrs = self._local_linear(model_fn, x, names)
            else:
                attrs = self._permutation_importance(model_fn, x, names)
        nl = self._nl(label, attrs)
        result = {
            "decision_id": decision_id,
            "method": self.method,
            "attributions": attrs,
            "explanation": nl,
            "counterfactual": None,
            "anchors": None,
        }
        self.global_attributions.append(dict(attrs))
        if self.storage is not None:
            try:
                features_json = _json_safe(
                    list(features) if not isinstance(features, Mapping) else features
                )
                await asyncio.to_thread(
                    self.storage.save_xai_explanation,
                    decision_id,
                    decision_id,
                    self.method,
                    label,
                    features_json,
                    attrs,
                    nl,
                )
            except Exception:
                pass
        return result

    def global_feature_importance(self) -> Dict[str, float]:
        if not self.global_attributions:
            return {}
        agg: Dict[str, List[float]] = defaultdict(list)
        for d in self.global_attributions:
            for k, v in d.items():
                agg[k].append(abs(v))
        return {k: sum(v) / len(v) for k, v in agg.items()}


SimpleXAIDecisionExplainer = XAIDecisionExplainer


# --------------------------------------------------------------------------- #
# ENHANCEMENT 7 — SIMPLIFIED adaptive precision switcher
# --------------------------------------------------------------------------- #
class AdaptivePrecisionSwitcher:
    """
    **Simplified reference implementation** of precision selection by
    relative energy cost. The switch decision is a simple accuracy-drop
    threshold.
    """

    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55, "fp16": 0.5, "int8": 0.3}

    def __init__(self, config: Any, storage: Any) -> None:
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def _probe(self) -> Dict[str, Any]:
        info = {"cuda": False, "bf16": False, "device": "cpu"}
        if TORCH_AVAILABLE:
            try:
                info["cuda"] = bool(torch.cuda.is_available())
                if info["cuda"]:
                    info["device"] = torch.cuda.get_device_name(0)
                    info["bf16"] = bool(torch.cuda.is_bf16_supported())
            except Exception:
                pass
        return info

    def select_precision(self) -> str:
        hw = self._probe()
        cands = list(
            _cfg_get(
                self.config,
                "precision_levels",
                ["fp32", "fp16", "bf16", "int8"],
            )
        )
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        if not cands:
            return "fp32"
        return min(cands, key=lambda c: self.ENERGY.get(c, 1.0))

    async def switch_to(self, target: str, reason: str = "policy") -> bool:
        if target == self.current or target not in self.ENERGY:
            return False
        old = self.current
        self.current = target
        saved = max(
            0.0, self.ENERGY.get(old, 1.0) - self.ENERGY.get(target, 1.0)
        )
        self.saved_wh += saved
        self.history.append(
            {"from": old, "to": target, "reason": reason, "saved_wh": saved}
        )
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_precision_switch,
                    old,
                    target,
                    reason,
                    saved,
                    0.0,
                )
            except Exception:
                pass
        return True

    async def auto_switch(
        self, recent_acc: float, baseline_acc: float
    ) -> None:
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = float(
            _cfg_get(self.config, "precision_switch_threshold", 0.02)
        )
        if drop > thresh:
            await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
        elif drop < thresh / 2:
            await self.switch_to(
                self.select_precision(), reason="headroom"
            )


SimpleAdaptivePrecisionSwitcher = AdaptivePrecisionSwitcher


# --------------------------------------------------------------------------- #
# ENHANCEMENT 8 — SIMPLIFIED carbon markets / REC
# --------------------------------------------------------------------------- #
class CarbonMarketIntegrator:
    """
    **Simplified reference implementation**: random-walk price with a
    threshold-based ``defer / run_offset / run`` policy. Not a real
    market model.
    """

    def __init__(self, config: Any, storage: Any) -> None:
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
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_credit_price, price
                )
            except Exception:
                pass
        return price

    async def purchase_rec(
        self, mwh: float, price_per_mwh: float = 5.0, source: str = "wind"
    ) -> float:
        cost = mwh * price_per_mwh
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_rec, mwh, price_per_mwh, source
                )
            except Exception:
                pass
        return cost

    async def net_zero_schedule(
        self, workload_kwh: float, intensity: float
    ) -> Dict[str, Any]:
        price = await self.update_price()
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price
        action = (
            "defer"
            if intensity > 0.3
            else ("run_offset" if offset_cost < 0.5 else "run")
        )
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_net_zero_match,
                    uuid.uuid4().hex[:8],
                    workload_kwh,
                    intensity,
                    action,
                    carbon_kg,
                    offset_cost,
                    price,
                )
            except Exception:
                pass
        return {
            "action": action,
            "carbon_kg": carbon_kg,
            "offset_cost_usd": offset_cost,
            "credit_price_usd": price,
        }


SimpleCarbonMarketIntegrator = CarbonMarketIntegrator


# --------------------------------------------------------------------------- #
# ENHANCEMENT 9 — SIMPLIFIED chaos testing
# --------------------------------------------------------------------------- #
class ChaosTestingEngine:
    """
    **Simplified reference implementation**: single-shot fault injection
    with steady-state probes. Not a chaos mesh.
    """

    FAULT_TYPES = [
        "latency",
        "exception",
        "data_corruption",
        "memory_pressure",
        "network_drop",
    ]

    def __init__(self, config: Any, storage: Any) -> None:
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def _steady(self) -> bool:
        if not self.active:
            return True
        return random.random() > float(
            _cfg_get(self.config, "chaos_intensity", 0.05)
        )

    async def run_experiment(
        self, name: str, fault_type: str
    ) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        t0 = time.perf_counter()
        before = await self._steady()
        status = "completed"
        try:
            async with self._lock:
                self.active[name] = {"fault_type": fault_type}
            if fault_type == "latency":
                await asyncio.sleep(0.1)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "memory_pressure":
                _ = bytearray(1024)
            elif fault_type == "network_drop":
                await asyncio.sleep(0.05)
            elif fault_type == "data_corruption":
                await asyncio.sleep(0)
        except Exception:
            status = "injected"
        finally:
            async with self._lock:
                self.active.pop(name, None)
        after = await self._steady()
        duration = (time.perf_counter() - t0) * 1000.0
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_chaos_experiment,
                    name,
                    name,
                    fault_type,
                    float(_cfg_get(self.config, "chaos_blast_radius", 0.1)),
                    int(before),
                    int(after),
                    status,
                    duration,
                )
            except Exception:
                pass
        return {
            "name": name,
            "fault_type": fault_type,
            "steady_before": before,
            "steady_after": after,
            "status": status,
            "duration_ms": duration,
        }


SimpleChaosTestingEngine = ChaosTestingEngine


# --------------------------------------------------------------------------- #
# ENHANCEMENT 10 — SIMPLIFIED HITL active learning
# --------------------------------------------------------------------------- #
class ActiveUserPreferenceLearner:
    """
    **Simplified reference implementation**: queue-based preference
    elicitation with a timeout default. Not an active learning loop.
    """

    def __init__(self, storage: Any, dashboard: Any = None) -> None:
        self.storage = storage
        self.dashboard = dashboard
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=100)

    async def submit_response(self, user_id: str, chosen_id: str) -> None:
        try:
            self._responses.put_nowait(
                {"user_id": user_id, "chosen": chosen_id}
            )
        except asyncio.QueueFull:
            pass

    async def query_user_if_needed(
        self,
        user_id: str,
        candidates: Sequence[Mapping[str, Any]],
        timeout: float = 3.0,
    ) -> Optional[str]:
        if len(candidates) < 2:
            return None
        try:
            msg = await asyncio.wait_for(
                self._responses.get(), timeout=timeout
            )
            return msg.get("chosen")
        except asyncio.TimeoutError:
            return candidates[0].get("solution_id")

    async def record_choice(
        self,
        user_id: str,
        solution_id: str,
        metrics: Optional[Mapping[str, float]] = None,
    ) -> None:
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k in ("quality_score", "carbon_g", "cost_usd", "latency_ms"):
                if k in metrics:
                    v = float(metrics[k])
                    if k == "quality_score":
                        prefs[k] = prefs.get(k, 0.25) + v * 0.01
                    else:
                        prefs[k] = prefs.get(k, 0.25) + 1.0 / (v + 1e-6) * 0.01
            s = sum(prefs.values()) or 1.0
            prefs = {k: v / s for k, v in prefs.items()}
            self.preferences[user_id] = prefs
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_user_preference, user_id, prefs
                )
            except Exception:
                pass


SimpleActiveUserPreferenceLearner = ActiveUserPreferenceLearner


# --------------------------------------------------------------------------- #
# Unified orchestrator
# --------------------------------------------------------------------------- #
class CarbonForecastOrchestratorV18:
    """
    Unified entry point: wires the enhanced ``CarbonForecast`` with all ten
    **simplified reference** enhancements and runs nine background loops.
    """

    def __init__(
        self, storage: Any, config: Any, dashboard: Any = None
    ) -> None:
        _validate_config(config)
        self.storage = storage
        self.config = config
        self.instance_id = uuid.uuid4().hex[:8]

        # Ten enhancements (simplified reference implementations)
        self.quantum = QuantumDistillationEngine()
        self.causal_graph = CausalGraphLearner(storage)
        self.causal_rl = CausalPolicyAdapter(
            config, storage, self.causal_graph
        )
        self.federated = FederatedGreenAggregator(storage, self.instance_id)
        self.multi_agent = MultiAgentCoordinator(config, storage)
        self.temporal = TemporalLogicVerifier(storage, config)
        self.xai = XAIDecisionExplainer(config, storage)
        self.precision = AdaptivePrecisionSwitcher(config, storage)
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.chaos = ChaosTestingEngine(config, storage)
        self.hitl = ActiveUserPreferenceLearner(storage, dashboard=dashboard)

        # Enhanced CarbonForecast
        self.forecast = CarbonForecast(
            config=config,
            storage=storage,
            region=_cfg_get(config, "carbon_region", "global"),
            causal_rl=self.causal_rl,
            temporal=self.temporal,
            xai=self.xai,
            hitl=self.hitl,
            carbon_market=self.carbon_market,
            chaos=self.chaos,
            federated=self.federated,
            multi_agent=self.multi_agent,
            precision=self.precision,
            registry=None,
            fail_mode=_cfg_get(config, "fail_mode", "log"),
        )

        # Lifecycle
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: Set["asyncio.Task[Any]"] = set()
        self._lifecycle_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    async def get_forecast(
        self, hours: int = 4, include_xai: bool = False
    ) -> ForecastBatch:
        return await self.forecast.forecast_next_hours_async(
            hours=hours, include_xai=include_xai
        )

    # ------------------------------------------------------------------
    async def start(self) -> None:
        """Start the background loops (idempotent)."""
        async with self._lifecycle_lock:
            if self._running:
                logger.debug("Orchestrator already running; ignoring start()")
                return
            # Reset the shutdown event so a previous shutdown() doesn't
            # immediately kill new loops.
            self._shutdown_event.clear()
            self._running = True

            loop = asyncio.get_running_loop()
            factories: List[Tuple[str, Callable[[], Any]]] = [
                ("causal_rl", self._causal_rl_loop),
                ("federated", self._federated_loop),
                ("multi_agent", self._multi_agent_loop),
                ("temporal", self._temporal_loop),
                ("xai", self._xai_loop),
                ("precision", self._precision_loop),
                ("carbon_market", self._carbon_market_loop),
                ("chaos", self._chaos_loop),
                ("distillation", self._distillation_loop),
            ]
            for name, factory in factories:
                t = loop.create_task(factory(), name=f"cf_{name}")
                self._background_tasks.add(t)
                t.add_done_callback(self._background_tasks.discard)
            logger.info(
                "CarbonForecastOrchestratorV18 started (%d loops)",
                len(self._background_tasks),
            )

    async def shutdown(self) -> None:
        """Stop all background loops and close the forecast session."""
        async with self._lifecycle_lock:
            self._shutdown_event.set()
            self._running = False
            tasks = list(self._background_tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        with self._lifecycle_lock:  # wait for the callbacks to drain
            self._background_tasks.clear()
        try:
            await self.forecast.close()
        except Exception:
            pass
        logger.info("CarbonForecastOrchestratorV18 shut down")

    async def __aenter__(self) -> "CarbonForecastOrchestratorV18":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ------------------------------------------------------------------
    async def _causal_rl_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=900)
                return
            except asyncio.TimeoutError:
                pass
            try:
                samples = [
                    {
                        "carbon_intensity": random.uniform(150, 700),
                        "quality": random.uniform(0.5, 1.0),
                        "cost": random.uniform(0.1, 0.9),
                        "latency": random.uniform(0.1, 0.9),
                    }
                    for _ in range(20)
                ]
                await self.causal_graph.learn(
                    samples,
                    ["carbon_intensity", "quality", "cost", "latency"],
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("causal_rl_loop iteration failed: %s", e)

    async def _federated_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=3600)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self.federated.pull_aggregated_weights(
                    f"carbon_forecast_{self.forecast.region}"
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("federated_loop iteration failed: %s", e)

    async def _multi_agent_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=600)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self.multi_agent.step()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("multi_agent_loop iteration failed: %s", e)

    async def _temporal_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=300)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self.temporal.verify()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("temporal_loop iteration failed: %s", e)

    async def _xai_loop(self) -> None:
        if not NUMPY_AVAILABLE:
            return
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=300)
                return
            except asyncio.TimeoutError:
                pass
            try:
                feats = np.array([0.9, 0.4, 0.5, 0.4])
                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="forecast_health",
                    features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=lambda x: float(
                        np.dot(x, [0.4, -0.3, -0.2, -0.1])
                    ),
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("xai_loop iteration failed: %s", e)

    async def _precision_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=300)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self.precision.auto_switch(0.9, 0.92)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("precision_loop iteration failed: %s", e)

    async def _carbon_market_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=3600)
                return
            except asyncio.TimeoutError:
                pass
            try:
                await self.carbon_market.update_price()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("carbon_market_loop iteration failed: %s", e)

    async def _chaos_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=1800)
                return
            except asyncio.TimeoutError:
                pass
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    f"auto_{uuid.uuid4().hex[:6]}", fault
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("chaos_loop iteration failed: %s", e)

    async def _distillation_loop(self) -> None:
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(self._shutdown_event.wait(), timeout=300)
                return
            except asyncio.TimeoutError:
                pass
            try:
                self.quantum.register_teacher(
                    "causal", self.causal_rl.get_policy()
                )
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy()
                )
                await self.quantum.step(self.storage, "carbon_student")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("distillation_loop iteration failed: %s", e)

    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        drift = self.forecast.detect_drift()
        stats = self.forecast.stats()
        return {
            "version": "18.0.0",
            "instance_id": self.instance_id,
            "running": self._running,
            "region": self.forecast.region,
            "forecast_stats": stats,
            "history_size": stats.get("history_size", 0),
            "drift": drift,
            "precision": self.precision.current,
            "carbon_price": self.carbon_market.last_price,
            "agents": {
                a.id: a.role for a in self.multi_agent.agents.values()
            },
            "federated_rounds": self.federated.rounds,
            "circuit_state": self.forecast.circuit_state,
            "last_api_error": stats.get("last_api_error"),
            "last_api_success": stats.get("last_api_success"),
            "hook_failures_total": stats.get("hook_failures_total", 0),
            "fail_mode": self.forecast.fail_mode,
        }


# Backward compatibility alias
CarbonForecastOrchestratorV17 = CarbonForecastOrchestratorV18


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    # Types
    "ForecastSource",
    "ForecastPoint",
    "ForecastBatch",
    # Infrastructure
    "CircuitBreaker",
    "StorageProtocol",
    "InMemoryStorage",
    # Main
    "CarbonForecast",
    "CarbonForecastOrchestratorV18",
    "CarbonForecastOrchestratorV17",
    # Enhancements (simplified reference implementations)
    "QuantumDistillationEngine",
    "CausalGraphLearner",
    "CausalPolicyAdapter",
    "FederatedGreenAggregator",
    "MultiAgentCoordinator",
    "TemporalLogicVerifier",
    "XAIDecisionExplainer",
    "AdaptivePrecisionSwitcher",
    "CarbonMarketIntegrator",
    "ChaosTestingEngine",
    "ActiveUserPreferenceLearner",
    # Simple aliases
    "SimpleQuantumDistillationEngine",
    "SimpleCausalGraphLearner",
    "SimpleCausalPolicyAdapter",
    "SimpleFederatedGreenAggregator",
    "SimpleMultiAgentCoordinator",
    "SimpleTemporalLogicVerifier",
    "SimpleXAIDecisionExplainer",
    "SimpleAdaptivePrecisionSwitcher",
    "SimpleCarbonMarketIntegrator",
    "SimpleChaosTestingEngine",
    "SimpleActiveUserPreferenceLearner",
]


# --------------------------------------------------------------------------- #
# Demo
# --------------------------------------------------------------------------- #
async def _demo() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    config = {
        "carbon_region": "US-CAL-CISO",
        "carbon_cache_ttl": 60,
        "causal_exploration_rate": 0.1,
        "agent_count": 5,
        "temporal_max_trace": 500,
        "temporal_formulas": [
            "G (carbon_intensity >= 0)",
            "G (carbon_intensity <= 900)",
        ],
        "xai_method": "permutation",
        "xai_depth": 5,
        "precision_levels": ["fp32", "fp16", "bf16", "int8"],
        "precision_switch_threshold": 0.02,
        "chaos_intensity": 0.0,
        "chaos_blast_radius": 0.5,
        "fail_mode": "log",
    }

    storage = InMemoryStorage(max_entries=5000)

    async with CarbonForecastOrchestratorV18(storage, config) as orch:
        print("=" * 80)
        print("carbon_forecast.py v18.0.0 — Demo")
        print("=" * 80)

        # 1. Sync backward-compatible API
        print("\n=== Backward-compatible sync API ===")
        ci = orch.forecast.current_intensity()
        print(f"  current_intensity(): {ci:.2f} gCO2/kWh")
        fc = orch.forecast.forecast_next_hours(4)
        print("  forecast_next_hours(4):")
        for h, v in fc.items():
            print(f"    +{h}h: {v:.2f} gCO2/kWh")

        # 2. Async forecast with provenance + XAI
        print("\n=== Async forecast with provenance + XAI ===")
        value, source = await orch.forecast.current_intensity_async()
        print(f"  current_intensity_async(): {value:.2f} (source={source.value})")

        batch = await orch.get_forecast(hours=6, include_xai=True)
        print(f"  Batch: {batch.batch_id} (source={batch.source})")
        print(f"  Region: {batch.region}")
        print(f"  Confidence: {batch.confidence:.2f}")
        print(f"  Duration: {batch.duration_ms:.2f} ms")
        print("  Points:")
        for p in batch.points[:3]:
            print(
                f"    {p.timestamp[:19]}  {p.intensity:.2f}  "
                f"({p.source}, conf={p.confidence:.2f})"
            )
        if batch.xai_explanation:
            print("  XAI top-3:")
            for k, v in list(batch.xai_explanation["attributions"].items())[:3]:
                print(f"    {k}: {v:+.4f}")
        if batch.temporal_violations:
            print(f"  Temporal violations: {batch.temporal_violations}")

        # 3. Batch serialization round-trip
        print("\n=== ForecastBatch serialization round-trip ===")
        payload = json.dumps(batch.to_dict(), default=str)
        restored = ForecastBatch.from_dict(json.loads(payload))
        assert restored.batch_id == batch.batch_id
        assert restored.region == batch.region
        assert len(restored.points) == len(batch.points)
        print(f"  Round-trip OK ({len(restored.points)} points)")

        # 4. Multi-region comparison
        print("\n=== Multi-region comparison ===")
        for region in ["US-CAL-CISO", "EU-FR", "ID", "SG"]:
            orch.forecast.set_region(region)
            v, src = await orch.forecast.current_intensity_async()
            print(f"  {region:15s} current={v:.1f} gCO2/kWh (source={src.value})")
        orch.forecast.set_region("US-CAL-CISO")

        # 5. Cache behaviour
        print("\n=== Cache behaviour ===")
        await orch.forecast.current_intensity_async(bypass_cache=True)
        t0 = time.time()
        await orch.forecast.current_intensity_async()
        t1 = time.time() - t0
        t0 = time.time()
        _, src2 = await orch.forecast.current_intensity_async()
        t2 = time.time() - t0
        print(f"  First call:  {t1*1000:.2f} ms (cache miss)")
        print(f"  Second call: {t2*1000:.2f} ms (cache hit, source={src2.value})")
        stats = orch.forecast.stats()
        print(f"  Cache hits: {stats['cache_hits']}")
        print(f"  Cache misses: {stats['cache_misses']}")
        print(f"  Cache size: {stats['cache_size']}")

        # 6. Drift detection (via test helper)
        print("\n=== Drift detection ===")
        orch.forecast._inject_history(
            "US-CAL-CISO",
            [(datetime.now(timezone.utc), 400.0 + random.gauss(0, 5)) for _ in range(30)]
            + [(datetime.now(timezone.utc), 900.0)],
        )
        drift = orch.forecast.detect_drift()
        if drift:
            print(f"  Drift detected: {drift['drift']}")
            print(f"  Z-score: {drift['z']:.2f}")
            print(f"  Mean: {drift['mean']:.2f}, current: {drift['current']:.2f}")

        # 7. Health check
        print("\n=== Health check ===")
        print(json.dumps(await orch.health_check(), indent=2, default=str))

        # 8. Circuit breaker
        print("\n=== Circuit breaker ===")
        breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1, name="demo")

        async def _fail():
            raise RuntimeError("boom")

        for i in range(3):
            try:
                await breaker.call(_fail)
            except RuntimeError as e:
                print(f"  attempt {i+1}: {e}")
        print(f"  breaker state: {breaker.state}")
        await asyncio.sleep(0.15)

        async def _ok():
            return 42

        result = await breaker.call(_ok)
        print(f"  after recovery window: {result}, state={breaker.state}")

        # 9. Retry helper
        print("\n=== Retry with backoff ===")
        attempts = {"n": 0}

        async def _flaky():
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError(f"flaky #{attempts['n']}")
            return "success"

        r = await _retry_with_backoff(
            _flaky, attempts=5, base_delay=0.01, max_delay=0.05
        )
        print(f"  Result: {r} after {attempts['n']} attempts")

        # 10. Start/stop idempotency
        print("\n=== start()/shutdown() idempotency ===")
        await orch.start()  # Already running
        await orch.start()  # Should be a no-op
        print("  start() called twice without error")
        await orch.shutdown()
        await orch.start()
        print("  shutdown() then start() works")
        await orch.shutdown()

    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
