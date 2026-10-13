# material_lca_v2_4_0.py
# Version: 2.5.0
"""
Enhanced Material Index Integration with Hardware Life-Cycle Databases v2.5.0
=============================================================================

Fetches accurate embodied carbon and rare-earth content from public LCA
databases and integrates adaptive weight selection via Multi-Teacher
On-Policy Distillation, plus NSGA-II for global weight refinement.

FIXES OVER v2.4.1:
- FastAPI `/metrics` route now imports `Response`.
- Pydantic v1/v2 compatibility via compat helpers (field_validator / model_dump).
- Timezone-aware datetimes throughout (`datetime.now(timezone.utc)`).
- `MoEGatingNetwork.add_training_sample` uses reward with a running baseline.
- Cache-save no longer leaks tasks; serialized with a lock and single-flight.
- `WeightHistoricalMLTeacher` / `WeightStatefulQTeacher` accept the caller's
  config instead of loading from `LCAConfig()` defaults.
- `DistillationStudent.predict_proba` no longer reshapes weights at runtime.
- `AdaptiveMaterialCostFunction.compute_cost` scopes `selected_expert` safely.
- `run_moea_update.evaluate` depends on the candidate weights (real Pareto).
- MOEA optimizer is reused across ticks; population seeded from last front.
- Per-request `_last_*` state is cleared at the start of `compute_cost`.
- Cache loading is per-entry; writes are atomic (`.tmp` + `os.replace`).
- `retry_decorator` fallback matches the Tenacity behaviour.
- Unit tests moved out of the production module.
- FastAPI app guarded by a factory; `on_event` replaced with `lifespan`.
- Prometheus metrics for MoE, MOEA, RLHF, MODP, LIMIT.
- `AdaptiveMaterialCostFunction` gained `aclose()` / `__aenter__` / `__aexit__`.
"""

from __future__ import annotations

import asyncio
import copy
import csv
import hashlib
import json
import logging
import os
import time
import uuid
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import numpy as np

# ---------- Pydantic (v1/v2 compat) ----------
from pydantic import BaseModel, Field

try:
    from pydantic import field_validator as _pyd_field_validator
    _PYDANTIC_V2 = True
except ImportError:  # pragma: no cover
    from pydantic import validator as _pyd_field_validator  # type: ignore
    _PYDANTIC_V2 = False


def _make_validator(field_name: str):
    """Return a decorator that works under Pydantic v1 and v2."""
    if _PYDANTIC_V2:
        def deco(fn):
            return _pyd_field_validator(field_name)(classmethod(fn))
        return deco
    def deco(fn):
        return _pyd_field_validator(field_name, allow_reuse=True)(fn)  # type: ignore
    return deco


def _pydantic_dump(obj: Any) -> Dict[str, Any]:
    if hasattr(obj, "model_dump"):
        return obj.model_dump()
    return obj.dict()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ---------- Tenacity ----------
try:
    from tenacity import (
        retry, stop_after_attempt, wait_exponential,
        retry_if_exception_type, before_sleep_log,
    )
    TENACITY_AVAILABLE = True
except ImportError:  # pragma: no cover
    TENACITY_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import (
        Counter, Gauge, Histogram, CollectorRegistry,
        generate_latest, CONTENT_TYPE_LATEST,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False

# ---------- aiohttp / aiofiles ----------
try:
    import aiohttp
    _AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    aiohttp = None  # type: ignore
    _AIOHTTP_AVAILABLE = False

try:
    import aiofiles
    _AIOFILES_AVAILABLE = True
except ImportError:  # pragma: no cover
    aiofiles = None  # type: ignore
    _AIOFILES_AVAILABLE = False

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
    if _STRUCTLOG_AVAILABLE:
        getattr(_STRUCTLOG_LOGGER, level)(message, **kwargs)
    else:
        if kwargs:
            extra = " ".join(f"{k}={v!r}" for k, v in kwargs.items())
            message = f"{message} | {extra}"
        getattr(logger, level)(message)


# ---------- scikit-learn ----------
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:  # pragma: no cover
    SKLEARN_ML = False

# ---------- FastAPI (optional) ----------
try:
    from fastapi import FastAPI, HTTPException, Depends
    from fastapi.responses import JSONResponse, Response
    FASTAPI_AVAILABLE = True
except ImportError:  # pragma: no cover
    FastAPI = None  # type: ignore
    HTTPException = None  # type: ignore
    Depends = None  # type: ignore
    JSONResponse = None  # type: ignore
    Response = None  # type: ignore
    FASTAPI_AVAILABLE = False


# ============================================================================
# 1. CONFIGURATION
# ============================================================================
class LCAConfig(BaseModel):
    source: str = Field("mock", description="'mock', 'ecoinvent', 'openlca', 'cache_only'")
    api_url: str = Field("https://api.example.com/lca")
    api_key: Optional[str] = None
    cache_dir: str = "./lca_cache"
    cache_ttl: int = 86400 * 7
    max_retry_attempts: int = 3
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 30
    default_embodied_carbon: float = 50.0
    default_rare_earth_fraction: float = 0.001
    default_material_index: float = 1.0
    variant_fields: List[str] = Field(default_factory=lambda: ["memory", "revision"])

    distillation_epsilon: float = Field(0.1, ge=0, le=1)
    distillation_train_every: int = Field(10, ge=1)
    distillation_replay_size: int = Field(2000, ge=10)
    distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
    distill_weight: float = Field(0.7, ge=0, le=1)
    rl_weight: float = Field(0.3, ge=0, le=1)

    moea_enabled: bool = Field(True)
    moea_interval_seconds: int = Field(300, ge=60)
    moea_population_size: int = Field(20, ge=5)
    moea_generations: int = Field(5, ge=1)
    moea_mutation_rate: float = Field(0.2, ge=0.0, le=1.0)
    moea_crossover_rate: float = Field(0.8, ge=0.0, le=1.0)
    moea_tournament_size: int = Field(3, ge=2)
    moea_objective_weights: Optional[Dict[str, float]] = None
    moea_dynamic_weights: bool = True

    enable_limit_graph: bool = Field(True)
    enable_modp: bool = Field(True)
    enable_rlhf: bool = Field(True)
    enable_moe: bool = Field(True)
    moe_expert_count: int = Field(3, ge=2)

    q_weights_path: str = Field("./lca_q_weights.json")
    interaction_logs_path: str = Field("./lca_interactions.csv")
    historical_model_path: str = Field("./lca_historical_model.pkl")
    moea_pareto_path: str = Field("./lca_moea_pareto.json")
    limit_graph_path: str = Field("./lca_limit_graph.json")

    @_make_validator("source")
    def _validate_source(cls, v):
        allowed = {"mock", "ecoinvent", "openlca", "cache_only"}
        if v not in allowed:
            raise ValueError(f"source must be one of {allowed}")
        return v


# ============================================================================
# 2. DATA STRUCTURES
# ============================================================================
class MaterialFootprint(BaseModel):
    hardware_model: str
    variant: Optional[str] = None
    embodied_carbon_kg: float
    rare_earth_kg: float
    total_mass_kg: float
    material_index: float
    water_usage_l: float = 0.0
    energy_mj: float = 0.0
    manufacturing_energy_mj: float = 0.0
    transport_emissions_kg_co2: float = 0.0
    source: str = "mock"
    timestamp: datetime = Field(default_factory=_now_utc)

    @_make_validator("material_index")
    def _validate_material_index(cls, v):
        if v < 0:
            raise ValueError("material_index must be non-negative")
        return v

    def cache_key(self) -> str:
        return cache_key_for(self.hardware_model, self.variant)


def cache_key_for(hardware_model: str, variant: Optional[str]) -> str:
    return f"{hardware_model}:{variant}" if variant else hardware_model


# ============================================================================
# 3. CIRCUIT BREAKER
# ============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        async with self._lock:
            now = _now_utc()
            if self.state == CircuitBreakerState.OPEN:
                if (
                    self.last_failure_time
                    and (now - self.last_failure_time).total_seconds() >= self.recovery_timeout
                ):
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.failure_count = 0
                    log_event("info", f"Circuit breaker {self.name} entering HALF_OPEN")
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")

        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    log_event("info", f"Circuit breaker {self.name} closed after success")
                else:
                    self.failure_count = 0
            return result
        except Exception as exc:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = _now_utc()
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    log_event(
                        "warning",
                        f"Circuit breaker {self.name} opened "
                        f"after {self.failure_count} failures",
                    )
            raise


# ============================================================================
# 4. TASK MANAGER
# ============================================================================
class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()
        self.shutdown_event = asyncio.Event()

    def is_running(self, name: str) -> bool:
        task = self.tasks.get(name)
        return task is not None and not task.done()

    def start_task(
        self,
        name: str,
        coro_func: Callable[[], Awaitable[None]],
        *args: Any,
        **kwargs: Any,
    ) -> asyncio.Task:
        async def wrapper():
            backoff = 1
            max_backoff = 300
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                    # A successful return means the task is done; exit the loop.
                    break
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    log_event(
                        "error",
                        f"Task '{name}' crashed: {exc}",
                        exc_info=True,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)

        task = asyncio.create_task(wrapper(), name=name)
        # Overwrite the handle if a same-named task exists and is done.
        old = self.tasks.get(name)
        if old is not None and not old.done():
            log_event(
                "warning",
                f"Task '{name}' is already running; not starting another.",
            )
            task.cancel()
            return old
        self.tasks[name] = task
        return task

    async def stop_all(self):
        self.shutdown_event.set()
        async with self._lock:
            for task in self.tasks.values():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()
        log_event("info", "All background tasks stopped")


# ============================================================================
# 5. RETRY DECORATOR
# ============================================================================
def retry_decorator(attempts: int = 3, min_wait: int = 2, max_wait: int = 10):
    if TENACITY_AVAILABLE:
        # Use a stdlib logger for Tenacity's before_sleep_log — it expects a
        # logger whose .warning() accepts positional args, which structlog does not.
        stdlib_logger = logging.getLogger(__name__)
        return retry(
            stop=stop_after_attempt(attempts),
            wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
            retry=retry_if_exception_type(
                (aiohttp.ClientError, asyncio.TimeoutError)
                if _AIOHTTP_AVAILABLE else (asyncio.TimeoutError,)
            ),
            before_sleep=before_sleep_log(stdlib_logger, logging.WARNING),
        )
    def decorator(func):
        async def wrapper(*args, **kwargs):
            last_exc: Optional[BaseException] = None
            for attempt in range(attempts):
                try:
                    return await func(*args, **kwargs)
                except (asyncio.TimeoutError, Exception) as exc:
                    # Only retry on the same exceptions as the Tenacity path.
                    if _AIOHTTP_AVAILABLE and not isinstance(
                        exc, (aiohttp.ClientError, asyncio.TimeoutError)
                    ):
                        raise
                    last_exc = exc
                    if attempt == attempts - 1:
                        raise
                    wait = min_wait * (2 ** attempt)
                    wait = min(wait, max_wait)
                    await asyncio.sleep(wait)
            if last_exc is not None:
                raise last_exc
        return wrapper
    return decorator


# ============================================================================
# 6. PROMETHEUS METRICS
# ============================================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    LCA_API_CALLS = Counter(
        "lca_api_calls_total", "LCA API calls", ["source", "status"], registry=REGISTRY,
    )
    LCA_API_LATENCY = Histogram(
        "lca_api_latency_seconds", "LCA API call latency", registry=REGISTRY,
    )
    CACHE_HITS = Counter("lca_cache_hits_total", "Cache hits", registry=REGISTRY)
    CACHE_MISSES = Counter("lca_cache_misses_total", "Cache misses", registry=REGISTRY)
    FOOTPRINT_FETCHED = Counter(
        "lca_footprints_fetched_total", "Footprints fetched", ["source"], registry=REGISTRY,
    )
    SIMULATION_RUNS = Counter(
        "lca_simulation_runs_total", "DigitalTwin simulations run", registry=REGISTRY,
    )
    DISTILLATION_UPDATES = Counter(
        "lca_distillation_updates_total", "Distillation student updates", registry=REGISTRY,
    )
    MOE_ROUTING = Counter(
        "lca_moe_routing_total", "MoE routing decisions", ["expert"], registry=REGISTRY,
    )
    MOEA_RUNS = Counter("lca_moea_runs_total", "MOEA runs", registry=REGISTRY)
    MOEA_PARETO = Gauge(
        "lca_moea_pareto_size", "MOEA Pareto front size", registry=REGISTRY,
    )
    RLHF_PAIRS = Counter("lca_rlhf_pairs_total", "RLHF preference pairs", registry=REGISTRY)
    MODP_STATES = Counter("lca_modp_states_total", "MODP states recorded", registry=REGISTRY)
    LIMIT_NODES = Gauge(
        "lca_limit_graph_nodes_total", "LIMIT graph node count", registry=REGISTRY,
    )
else:
    class _Dummy:
        def labels(self, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass
    LCA_API_CALLS = LCA_API_LATENCY = CACHE_HITS = CACHE_MISSES = _Dummy()
    FOOTPRINT_FETCHED = SIMULATION_RUNS = DISTILLATION_UPDATES = _Dummy()
    MOE_ROUTING = MOEA_RUNS = MOEA_PARETO = RLHF_PAIRS = _Dummy()
    MODP_STATES = LIMIT_NODES = _Dummy()


# ============================================================================
# 7. LCA API CLIENT
# ============================================================================
class LCAClient:
    def __init__(self, config: Optional[LCAConfig] = None):
        self.config = config or LCAConfig()
        self.cache: Dict[str, MaterialFootprint] = {}
        self._session: Optional[Any] = None
        self._lock = asyncio.Lock()
        self._save_lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker(
            "lca_api",
            failure_threshold=self.config.circuit_breaker_threshold,
            recovery_timeout=self.config.circuit_breaker_timeout,
        )
        self._task_manager = TaskManager()
        self._cache_loaded = False
        self._load_cache_sync()

    # ---- cache persistence ----
    def _cache_path(self) -> str:
        os.makedirs(self.config.cache_dir, exist_ok=True)
        return os.path.join(self.config.cache_dir, "lca_cache.json")

    def _load_cache_sync(self) -> None:
        path = self._cache_path()
        if not os.path.exists(path):
            log_event("info", "No LCA cache found, starting fresh")
            return
        try:
            with open(path, "r") as f:
                data = json.load(f)
        except Exception as exc:
            log_event("warning", f"Failed to load LCA cache file: {exc}")
            return

        loaded = 0
        skipped = 0
        for key, fp_dict in data.items():
            try:
                self.cache[key] = MaterialFootprint(**fp_dict)
                loaded += 1
            except Exception as exc:
                skipped += 1
                log_event("warning", f"Skipping corrupt cache entry '{key}': {exc}")
        self._cache_loaded = True
        log_event("info", f"Loaded LCA cache (loaded={loaded}, skipped={skipped})")

    async def _save_cache_async(self) -> None:
        path = self._cache_path()
        data = {key: _pydantic_dump(fp) for key, fp in self.cache.items()}
        async with self._save_lock:
            try:
                payload = json.dumps(data, indent=2, default=str)
                tmp = path + ".tmp"
                if _AIOFILES_AVAILABLE:
                    async with aiofiles.open(tmp, "w") as f:
                        await f.write(payload)
                else:
                    with open(tmp, "w") as f:
                        f.write(payload)
                os.replace(tmp, path)
                log_event("debug", "Cache saved")
            except Exception as exc:
                log_event("warning", f"Failed to save LCA cache: {exc}")

    def _schedule_cache_save(self) -> None:
        # Single-flight: don't spawn a new task if one is already running.
        if self._task_manager.is_running("lca_cache_save"):
            return
        try:
            self._task_manager.start_task("lca_cache_save", self._save_cache_async)
        except RuntimeError:
            # No running loop; run inline. Best-effort.
            log_event(
                "warning",
                "No event loop; LCA cache save skipped.",
            )

    # ---- session ----
    async def _get_session(self) -> Any:
        if not _AIOHTTP_AVAILABLE:
            raise RuntimeError("aiohttp not installed")
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    # ---- API ----
    @retry_decorator(attempts=3)
    async def _fetch_from_api(
        self, hardware_model: str, variant: Optional[str] = None,
    ) -> Optional[MaterialFootprint]:
        source = self.config.source
        if source == "mock":
            return self._generate_mock_footprint(hardware_model, variant)
        if source in ("ecoinvent", "openlca"):
            session = await self._get_session()
            url = f"{self.config.api_url}/footprint"
            params: Dict[str, Any] = {"model": hardware_model, "source": source}
            if variant:
                params["variant"] = variant
            headers: Dict[str, str] = {}
            if self.config.api_key:
                headers["Authorization"] = f"Bearer {self.config.api_key}"
            start = time.monotonic()
            async with session.get(url, params=params, headers=headers, timeout=30) as resp:
                LCA_API_LATENCY.observe(time.monotonic() - start)
                if resp.status != 200:
                    raise aiohttp.ClientError(f"API returned {resp.status}")
                data = await resp.json()
                return MaterialFootprint(
                    hardware_model=data["hardware_model"],
                    variant=data.get("variant"),
                    embodied_carbon_kg=data["embodied_carbon_kg"],
                    rare_earth_kg=data["rare_earth_kg"],
                    total_mass_kg=data["total_mass_kg"],
                    material_index=data["material_index"],
                    water_usage_l=data.get("water_usage_l", 0.0),
                    energy_mj=data.get("energy_mj", 0.0),
                    manufacturing_energy_mj=data.get("manufacturing_energy_mj", 0.0),
                    transport_emissions_kg_co2=data.get("transport_emissions_kg_co2", 0.0),
                    source=source,
                )
        return None

    def _generate_mock_footprint(
        self, hardware_model: str, variant: Optional[str] = None,
    ) -> MaterialFootprint:
        base_hash = int(hashlib.sha256(hardware_model.encode()).hexdigest(), 16) % 1000
        variant_hash = (
            int(hashlib.sha256((variant or "").encode()).hexdigest(), 16) % 100
            if variant else 0
        )
        mass_kg = 2.0 + (base_hash % 10) * 0.5
        carbon = 20.0 + (base_hash % 80) * 0.5
        rare_earth = 0.002 + (base_hash % 5) * 0.001
        material_index = carbon / 50.0 + rare_earth * 100
        return MaterialFootprint(
            hardware_model=hardware_model,
            variant=variant,
            embodied_carbon_kg=carbon,
            rare_earth_kg=rare_earth,
            total_mass_kg=mass_kg,
            material_index=material_index,
            water_usage_l=5.0 + (base_hash % 20),
            energy_mj=10.0 + (base_hash % 40),
            manufacturing_energy_mj=20.0 + (base_hash % 30),
            transport_emissions_kg_co2=0.5 + (base_hash % 5) * 0.1,
            source="mock",
        )

    async def get_footprint(
        self,
        hardware_model: str,
        variant: Optional[str] = None,
        force_refresh: bool = False,
    ) -> MaterialFootprint:
        cache_key = cache_key_for(hardware_model, variant)

        async with self._lock:
            if not force_refresh and cache_key in self.cache:
                cached = self.cache[cache_key]
                ts = cached.timestamp
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                age = (_now_utc() - ts).total_seconds()
                if age < self.config.cache_ttl:
                    CACHE_HITS.inc()
                    return cached
                CACHE_MISSES.inc()

            if self.config.source == "cache_only":
                raise ValueError(
                    f"Footprint for {cache_key} not found in cache and cache_only is set"
                )

            try:
                fp = await self._circuit_breaker.call(
                    self._fetch_from_api, hardware_model, variant,
                )
                if fp is None:
                    raise ValueError("API returned no data")
                LCA_API_CALLS.labels(source=self.config.source, status="success").inc()
                FOOTPRINT_FETCHED.labels(source=fp.source).inc()
                self.cache[cache_key] = fp
                self._schedule_cache_save()
                return fp
            except Exception as exc:
                log_event(
                    "error",
                    f"API fetch failed ({hardware_model}/{variant}): {exc}",
                )
                LCA_API_CALLS.labels(source=self.config.source, status="failed").inc()
                if cache_key in self.cache:
                    log_event(
                        "warning",
                        f"Returning expired cache entry as fallback for {cache_key}",
                    )
                    return self.cache[cache_key]
                fallback = MaterialFootprint(
                    hardware_model=hardware_model,
                    variant=variant,
                    embodied_carbon_kg=self.config.default_embodied_carbon,
                    rare_earth_kg=self.config.default_rare_earth_fraction,
                    total_mass_kg=1.0,
                    material_index=self.config.default_material_index,
                    source="default",
                )
                self.cache[cache_key] = fallback
                self._schedule_cache_save()
                return fallback

    async def get_footprints_batch(
        self, hardware_models: List[Tuple[str, Optional[str]]],
    ) -> Dict[str, MaterialFootprint]:
        tasks = [self.get_footprint(m, v) for m, v in hardware_models]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        footprints: Dict[str, MaterialFootprint] = {}
        for (model, variant), result in zip(hardware_models, results):
            key = cache_key_for(model, variant)
            if isinstance(result, Exception):
                log_event("error", f"Batch fetch failed for {key}: {result}")
                footprints[key] = MaterialFootprint(
                    hardware_model=model,
                    variant=variant,
                    embodied_carbon_kg=self.config.default_embodied_carbon,
                    rare_earth_kg=self.config.default_rare_earth_fraction,
                    total_mass_kg=1.0,
                    material_index=self.config.default_material_index,
                    source="default",
                )
            else:
                footprints[key] = result
        return footprints

    async def update_footprint(
        self,
        hardware_model: str,
        variant: Optional[str],
        footprint: MaterialFootprint,
    ) -> None:
        key = cache_key_for(hardware_model, variant)
        async with self._lock:
            self.cache[key] = footprint
            self._schedule_cache_save()

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        await self._task_manager.stop_all()
        # Flush the cache one last time.
        try:
            await self._save_cache_async()
        except Exception as exc:
            log_event("warning", f"Final cache save failed: {exc}")


# ============================================================================
# 8. DIGITALTWIN SIMULATOR
# ============================================================================
class DigitalTwinMaterialSimulator:
    def __init__(self, lca_client: Optional[LCAClient] = None):
        self.lca_client = lca_client or LCAClient()

    async def simulate_refresh_cycle(
        self,
        hardware_model: str,
        variant: Optional[str] = None,
        quantity: int = 1,
        lifetime_years: float = 5,
        refresh_interval_years: float = 3,
        years_to_simulate: int = 10,
        operational_energy_per_year_joules: float = 0.0,
        recycling_rate: float = 0.5,
        transport_distance_km: float = 1000.0,
    ) -> Dict[str, Any]:
        SIMULATION_RUNS.inc()
        footprint = await self.lca_client.get_footprint(hardware_model, variant)

        num_cycles = int(years_to_simulate / refresh_interval_years) + 1
        total_embodied_carbon = 0.0
        total_rare_earth = 0.0
        total_mass = 0.0
        total_manufacturing_energy = 0.0
        total_transport_emissions = 0.0
        operational_carbon_total = 0.0
        timeline: List[Dict[str, Any]] = []

        for cycle in range(num_cycles):
            year = cycle * refresh_interval_years
            if year > years_to_simulate:
                break
            total_embodied_carbon += footprint.embodied_carbon_kg * quantity
            total_rare_earth += footprint.rare_earth_kg * quantity
            total_mass += footprint.total_mass_kg * quantity
            total_manufacturing_energy += footprint.manufacturing_energy_mj * quantity
            total_transport_emissions += footprint.transport_emissions_kg_co2 * quantity

            operational_carbon = operational_energy_per_year_joules / 3.6e6 * 0.2
            operational_carbon_total += operational_carbon * refresh_interval_years

            timeline.append({
                "year": year,
                "carbon_kg": total_embodied_carbon + operational_carbon_total,
                "rare_earth_kg": total_rare_earth,
                "mass_kg": total_mass,
                "manufacturing_energy_mj": total_manufacturing_energy,
                "transport_emissions_kg": total_transport_emissions,
            })

        avg_carbon_per_kg = (
            footprint.embodied_carbon_kg / footprint.total_mass_kg
            if footprint.total_mass_kg > 0 else 0.0
        )
        recycled_carbon_saved = total_mass * recycling_rate * avg_carbon_per_kg
        total_carbon_with_recycling = (
            total_embodied_carbon + operational_carbon_total - recycled_carbon_saved
        )

        return {
            "hardware_model": hardware_model,
            "variant": variant,
            "quantity": quantity,
            "years_to_simulate": years_to_simulate,
            "refresh_interval_years": refresh_interval_years,
            "total_carbon_kg": total_carbon_with_recycling,
            "total_rare_earth_kg": total_rare_earth,
            "total_mass_kg": total_mass,
            "total_manufacturing_energy_mj": total_manufacturing_energy,
            "total_transport_emissions_kg": total_transport_emissions,
            "operational_carbon_kg": operational_carbon_total,
            "recycling_savings_kg": recycled_carbon_saved,
            "avg_carbon_per_year": (
                total_carbon_with_recycling / years_to_simulate
                if years_to_simulate > 0 else 0.0
            ),
            "timeline": timeline,
        }

    async def compare_refresh_strategies(
        self,
        hardware_model: str,
        variant: Optional[str] = None,
        quantity: int = 1,
        strategies: Optional[List[Dict[str, float]]] = None,
        years_to_simulate: int = 10,
        operational_energy_per_year_joules: float = 0.0,
    ) -> List[Dict[str, Any]]:
        if strategies is None:
            strategies = [
                {"interval": 3, "lifetime": 3},
                {"interval": 5, "lifetime": 5},
                {"interval": 7, "lifetime": 7},
            ]
        results = []
        for strat in strategies:
            interval = strat.get("interval", 3)
            sim = await self.simulate_refresh_cycle(
                hardware_model=hardware_model,
                variant=variant,
                quantity=quantity,
                lifetime_years=strat.get("lifetime", interval),
                refresh_interval_years=interval,
                years_to_simulate=years_to_simulate,
                operational_energy_per_joules=operational_energy_per_year_joules,
            )
            results.append(sim)
        return results


# ============================================================================
# 9. DISTILLATION COMPONENTS
# ============================================================================
FEATURE_DIM = 11
NUM_STRATEGIES = 5


@dataclass
class CostState:
    embodied_carbon_kg: float
    rare_earth_kg: float
    water_usage_l: float
    material_index: float
    operational_energy_joules: float
    lifetime_years: float
    quantity: int
    carbon_intensity: float
    recycling_rate: float
    avg_cost_trend: float
    avg_user_rating: float

    def to_feature_vector(self) -> np.ndarray:
        features = [
            min(self.embodied_carbon_kg / 100.0, 1.0),
            min(self.rare_earth_kg / 0.01, 1.0),
            min(self.water_usage_l / 50.0, 1.0),
            min(self.material_index / 2.0, 1.0),
            min(self.operational_energy_joules / 1e7, 1.0),
            min(self.lifetime_years / 10.0, 1.0),
            min(self.quantity / 10.0, 1.0),
            min(self.carbon_intensity / 1000.0, 1.0),
            self.recycling_rate,
            self.avg_cost_trend,
            self.avg_user_rating,
        ]
        return np.array(features, dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: CostState) -> np.ndarray:
        ...

    @abstractmethod
    def confidence(self, state: CostState) -> float:
        ...


STRATEGIES: List[str] = [
    "balanced", "carbon_focus", "rare_earth_focus",
    "operational_focus", "water_focus",
]


class WeightRuleBasedTeacher(Teacher):
    STRATEGIES = STRATEGIES

    def predict(self, state: CostState) -> np.ndarray:
        probs = np.ones(NUM_STRATEGIES) * 0.1
        if state.carbon_intensity > 500:
            probs[1] = 0.8
        elif state.rare_earth_kg > 0.005:
            probs[2] = 0.7
        elif state.operational_energy_joules > 5e6:
            probs[3] = 0.7
        elif state.water_usage_l > 20:
            probs[4] = 0.6
        else:
            probs[0] = 0.6
        total = probs.sum()
        return probs / total if total > 0 else np.ones(NUM_STRATEGIES) / NUM_STRATEGIES

    def confidence(self, state: CostState) -> float:
        return 0.6 if state.carbon_intensity > 500 else 0.4


class WeightHistoricalMLTeacher(Teacher):
    def __init__(self, model_path: Optional[Path] = None):
        self.model = None
        self.label_encoder = None
        self.model_path: Optional[Path] = Path(model_path) if model_path else None
        if self.model_path and self.model_path.exists() and SKLEARN_ML:
            try:
                with open(self.model_path, "rb") as f:
                    self.model, self.label_encoder = pickle.load(f)
                log_event("info", f"Loaded historical ML model from {self.model_path}")
            except Exception as exc:
                log_event("error", f"Failed to load historical model: {exc}")
                self.model = None

    def predict(self, state: CostState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(NUM_STRATEGIES) / NUM_STRATEGIES
        x = state.to_feature_vector().reshape(1, -1)
        try:
            probs = self.model.predict_proba(x)[0]
        except Exception:
            return np.ones(NUM_STRATEGIES) / NUM_STRATEGIES
        # Reorder into a fixed STRATEGIES order.
        out = np.zeros(NUM_STRATEGIES)
        for i, cls in enumerate(self.label_encoder.classes_):
            if 0 <= int(cls) < NUM_STRATEGIES:
                out[int(cls)] = probs[i]
        s = out.sum()
        return out / s if s > 0 else np.ones(NUM_STRATEGIES) / NUM_STRATEGIES

    def confidence(self, state: CostState) -> float:
        return 0.7 if self.model is not None else 0.0


class WeightStatefulQTeacher(Teacher):
    def __init__(self, lr: float = 0.1, weights_path: Optional[Path] = None):
        self.lr = lr
        self.weights = np.zeros((FEATURE_DIM, NUM_STRATEGIES))
        self.weights_path: Optional[Path] = Path(weights_path) if weights_path else None
        if self.weights_path and self.weights_path.exists():
            self._load_state()

    def _load_state(self) -> None:
        if self.weights_path is None or not self.weights_path.exists():
            return
        try:
            with open(self.weights_path, "r") as f:
                data = json.load(f)
            arr = np.asarray(data, dtype=float)
            if arr.shape == self.weights.shape:
                self.weights = arr
                log_event("info", f"Loaded Q-teacher weights from {self.weights_path}")
            else:
                log_event("warning", f"Q-teacher shape mismatch: {arr.shape}")
        except Exception as exc:
            log_event("error", f"Failed to load Q-weights: {exc}")

    def _save_state(self) -> None:
        if self.weights_path is None:
            return
        try:
            self.weights_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.weights_path.with_suffix(self.weights_path.suffix + ".tmp")
            with open(tmp, "w") as f:
                json.dump(self.weights.tolist(), f)
            os.replace(tmp, self.weights_path)
        except Exception as exc:
            log_event("warning", f"Failed to save Q-weights: {exc}")

    def predict(self, state: CostState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: CostState) -> float:
        return 0.5

    def update(self, state: CostState, action: int, reward: float) -> None:
        x = state.to_feature_vector()
        q_current = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q_current) * x


class DistillationStudent:
    def __init__(self, feature_dim: int = FEATURE_DIM, n_classes: int = NUM_STRATEGIES, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray, num_classes: Optional[int] = None) -> np.ndarray:
        # num_classes is accepted for backward compatibility but ignored.
        logits = state_vector @ self.weights + self.biases
        max_logit = float(np.max(logits))
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(
        self,
        state_vector: np.ndarray,
        teacher_probs: np.ndarray,
        reward: float,
        action: int,
        distill_weight: float = 0.7,
        rl_weight: float = 0.3,
    ) -> None:
        if teacher_probs.shape[0] != self.n_classes:
            return
        current_probs = self.predict_proba(state_vector)
        grad_distill = -(teacher_probs - current_probs)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1


class ReplayBuffer:
    def __init__(self, max_size: int = 2000):
        self.buffer: deque = deque(maxlen=max_size)

    def push(
        self,
        state_vec: np.ndarray,
        action: int,
        reward: float,
        next_state_vec: np.ndarray,
        teacher_probs: np.ndarray,
    ) -> None:
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))

    def sample(self, batch_size: int = 32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (
            np.array(states),
            list(actions),
            np.array(rewards),
            np.array(next_states),
            np.array(teacher_probs),
        )

    def __len__(self) -> int:
        return len(self.buffer)


class DistillationWeightOptimizer:
    STRATEGIES = STRATEGIES

    def __init__(
        self,
        config: Dict[str, Any],
        historical_model_path: Optional[Path] = None,
        q_weights_path: Optional[Path] = None,
    ):
        self.config = config
        self.student = DistillationStudent(
            lr=config.get("distillation_learning_rate", 0.01),
        )
        self.q_teacher = WeightStatefulQTeacher(weights_path=q_weights_path)
        self.teachers: List[Teacher] = [
            WeightRuleBasedTeacher(),
            WeightHistoricalMLTeacher(model_path=historical_model_path),
            self.q_teacher,
        ]
        self.replay_buffer = ReplayBuffer(
            max_size=config.get("distillation_replay_size", 2000),
        )
        self.epsilon = config.get("distillation_epsilon", 0.1)
        self.train_every = config.get("distillation_train_every", 10)
        self.counter = 0

    def _aggregate_teacher_probs(self, state: CostState) -> np.ndarray:
        n = NUM_STRATEGIES
        teacher_probs = np.zeros(n)
        total_conf = 0.0
        for teacher in self.teachers:
            try:
                prob = teacher.predict(state)
                conf = teacher.confidence(state)
            except Exception:
                continue
            if len(prob) != n:
                continue
            teacher_probs += prob * conf
            total_conf += conf
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(n) / n
        return teacher_probs

    async def select_strategy(
        self, state: CostState, exploration: bool = True,
    ) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        teacher_probs = self._aggregate_teacher_probs(state)
        student_probs = self.student.predict_proba(state_vec)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, NUM_STRATEGIES - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = int(np.argmax(combined))

        return STRATEGIES[action_idx], action_idx, state_vec, teacher_probs

    async def update(
        self,
        state_vec: np.ndarray,
        action_idx: int,
        reward: float,
        next_state_vec: np.ndarray,
        teacher_probs: np.ndarray,
    ) -> None:
        # Online Q-teacher update.
        try:
            q_current = float(np.dot(state_vec, self.q_teacher.weights[:, action_idx]))
            self.q_teacher.weights[:, action_idx] += self.q_teacher.lr * (
                reward - q_current
            ) * state_vec
        except Exception:
            pass

        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(
                    states[i], teacher_probs_batch[i], rewards[i], actions[i],
                    distill_weight=float(self.config.get("distill_weight", 0.7)),
                    rl_weight=float(self.config.get("rl_weight", 0.3)),
                )
            try:
                DISTILLATION_UPDATES.inc()
            except Exception:
                pass

    def get_stats(self) -> Dict[str, Any]:
        return {
            "student_counter": self.student.counter,
            "buffer_size": len(self.replay_buffer),
            "weights_norm": float(np.linalg.norm(self.student.weights)),
        }


# ============================================================================
# 10. NSGA-II WEIGHT OPTIMIZER
# ============================================================================
@dataclass
class MOPDWeightVector:
    vector_id: str
    weights: Dict[str, float]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "vector_id": self.vector_id,
            "weights": self.weights,
            "objectives": self.objectives,
            "scalarised_score": self.scalarised_score,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDWeightVector":
        allowed = {"vector_id", "weights", "objectives", "scalarised_score"}
        return cls(**{k: v for k, v in data.items() if k in allowed})


def _select_best_from_pareto(
    pareto: List[MOPDWeightVector], weights: Dict[str, float],
) -> Optional[MOPDWeightVector]:
    if not pareto:
        return None
    keys = list(weights.keys())
    max_vals = {k: max(p.objectives.get(k, 0.0) for p in pareto) for k in keys}
    min_vals = {k: min(p.objectives.get(k, 0.0) for p in pareto) for k in keys}
    ranges = {
        k: (max_vals[k] - min_vals[k]) if max_vals[k] != min_vals[k] else 1.0
        for k in keys
    }
    best: Optional[MOPDWeightVector] = None
    best_score = -float("inf")
    for p in pareto:
        score = 0.0
        for k in keys:
            v = p.objectives.get(k, 0.0)
            norm = (v - min_vals[k]) / ranges[k] if ranges[k] > 0 else 1.0
            score += weights.get(k, 0.0) * norm
        p.scalarised_score = score
        if score > best_score:
            best_score = score
            best = p
    return best


class NSGAIIWeightOptimizer:
    OBJECTIVE_KEYS = ["carbon", "rare_earth", "water", "operational"]

    def __init__(
        self,
        evaluate_func: Callable[[Dict[str, float]], Awaitable[Dict[str, float]]],
        population_size: int = 20,
        generations: int = 10,
        mutation_rate: float = 0.2,
        crossover_rate: float = 0.8,
        tournament_size: int = 3,
        objective_weights: Optional[Dict[str, float]] = None,
        dynamic_weights: bool = True,
    ):
        self.evaluate_func = evaluate_func
        self.population_size = max(4, population_size)
        self.generations = max(1, generations)
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = max(2, tournament_size)
        self.objective_weights = objective_weights or {
            "carbon": 0.35, "rare_earth": 0.25, "water": 0.2, "operational": 0.2,
        }
        self.dynamic_weights = dynamic_weights

        self.best_individual: Optional[Dict[str, float]] = None
        self.best_fitness = -float("inf")
        self.evolution_history: List[Dict[str, Any]] = []
        self.pareto_front: List[MOPDWeightVector] = []
        self._eval_cache: Dict[Tuple[Tuple[str, float], ...], Dict[str, float]] = {}
        self._all_points: List[MOPDWeightVector] = []

    # ---- genetic operators ----
    def _random_individual(self) -> Dict[str, float]:
        keys = self.OBJECTIVE_KEYS
        weights = {k: random.random() for k in keys}
        total = sum(weights.values())
        return {k: v / total for k, v in weights.items()} if total > 0 else weights

    def _crossover(self, p1: Dict[str, float], p2: Dict[str, float]) -> Dict[str, float]:
        child: Dict[str, float] = {}
        for key in p1:
            if random.random() < 0.5:
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1 / 21)
                else:
                    beta = (1 / (2 * (1 - u))) ** (1 / 21)
                child[key] = max(0.0, min(1.0, 0.5 * ((1 + beta) * p1[key] + (1 - beta) * p2[key])))
            else:
                child[key] = p1[key] if random.random() < 0.5 else p2[key]
        total = sum(child.values())
        return {k: v / total for k, v in child.items()} if total > 0 else child

    def _mutate(self, ind: Dict[str, float]) -> Dict[str, float]:
        mutant = ind.copy()
        for key in mutant:
            if random.random() < self.mutation_rate:
                u = random.random()
                if u < 0.5:
                    delta = (2 * u) ** (1 / 21) - 1
                else:
                    delta = 1 - (2 * (1 - u)) ** (1 / 21)
                mutant[key] = max(0.0, min(1.0, mutant[key] + delta))
        total = sum(mutant.values())
        return {k: v / total for k, v in mutant.items()} if total > 0 else mutant

    # ---- NSGA-II core ----
    @staticmethod
    def _dominates(a: MOPDWeightVector, b: MOPDWeightVector, keys: List[str]) -> bool:
        at_least = all(a.objectives.get(k, 0.0) >= b.objectives.get(k, 0.0) for k in keys)
        strictly = any(a.objectives.get(k, 0.0) > b.objectives.get(k, 0.0) for k in keys)
        return at_least and strictly

    def _fast_non_dominated_sort(
        self, points: List[MOPDWeightVector],
    ) -> List[List[MOPDWeightVector]]:
        if not points:
            return []
        keys = list(self.objective_weights.keys())
        fronts: List[List[MOPDWeightVector]] = [[]]
        dom_count: Dict[int, int] = {id(p): 0 for p in points}
        dominated_by: Dict[int, List[MOPDWeightVector]] = {id(p): [] for p in points}

        for p in points:
            for q in points:
                if p is q:
                    continue
                if self._dominates(p, q, keys):
                    dominated_by[id(p)].append(q)
                elif self._dominates(q, p, keys):
                    dom_count[id(p)] += 1
            if dom_count[id(p)] == 0:
                fronts[0].append(p)

        i = 0
        while i < len(fronts) and fronts[i]:
            nxt: List[MOPDWeightVector] = []
            for p in fronts[i]:
                for q in dominated_by[id(p)]:
                    dom_count[id(q)] -= 1
                    if dom_count[id(q)] == 0:
                        nxt.append(q)
            i += 1
            fronts.append(nxt)
        return [f for f in fronts if f]

    def _crowding_distance(self, front: List[MOPDWeightVector]) -> Dict[int, float]:
        if not front:
            return {}
        keys = list(self.objective_weights.keys())
        dist: Dict[int, float] = {id(p): 0.0 for p in front}
        for k in keys:
            sorted_front = sorted(front, key=lambda x: x.objectives.get(k, 0.0))
            dist[id(sorted_front[0])] = float("inf")
            dist[id(sorted_front[-1])] = float("inf")
            lo = sorted_front[0].objectives.get(k, 0.0)
            hi = sorted_front[-1].objectives.get(k, 0.0)
            span = hi - lo
            if span <= 0:
                continue
            for i in range(1, len(sorted_front) - 1):
                dist[id(sorted_front[i])] += (
                    sorted_front[i + 1].objectives.get(k, 0.0)
                    - sorted_front[i - 1].objectives.get(k, 0.0)
                ) / span
        return dist

    def _tournament_selection(
        self,
        population: List[Dict[str, float]],
        rank: Dict[int, int],
        crowding: Dict[int, float],
        ind_to_point: Dict[Tuple[Tuple[str, float], ...], MOPDWeightVector],
    ) -> Dict[str, float]:
        candidates = random.sample(population, min(self.tournament_size, len(population)))
        best = candidates[0]
        best_rank = float("inf")
        best_cd = -float("inf")
        for cand in candidates:
            key = tuple(sorted(cand.items()))
            point = ind_to_point.get(key)
            if point is None:
                continue
            r = rank.get(id(point), float("inf"))
            cd = crowding.get(id(point), 0.0)
            if r < best_rank or (r == best_rank and cd > best_cd):
                best = cand
                best_rank = r
                best_cd = cd
        return best

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = dict(self.objective_weights)
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        for k in weights:
            vals = [p.objectives.get(k, 0.0) for p in self.pareto_front]
            if not vals:
                continue
            avg = float(np.mean(vals))
            mx = float(np.max(vals))
            if mx > 0 and avg < 0.5 * mx:
                weights[k] = min(0.6, weights[k] * 1.5)
        total = sum(weights.values())
        return {k: v / total for k, v in weights.items()} if total > 0 else weights

    def _select_best_from_pareto(
        self, pareto: List[MOPDWeightVector], weights: Dict[str, float],
    ) -> Optional[MOPDWeightVector]:
        return _select_best_from_pareto(pareto, weights)

    async def _evaluate(self, weights: Dict[str, float]) -> Dict[str, float]:
        key = tuple(sorted(weights.items()))
        if key in self._eval_cache:
            return self._eval_cache[key]
        obj = await self.evaluate_func(weights)
        self._eval_cache[key] = obj
        return obj

    async def evolve(self) -> List[MOPDWeightVector]:
        if self.evaluate_func is None:
            raise ValueError("evaluate_func not set")

        # Seed with the previous Pareto front if available.
        population: List[Dict[str, float]] = [dict(p.weights) for p in self.pareto_front]
        while len(population) < self.population_size:
            population.append(self._random_individual())
        population = population[: self.population_size]

        points: List[MOPDWeightVector] = []
        for w in population:
            obj = await self._evaluate(w)
            points.append(MOPDWeightVector(
                vector_id=str(uuid.uuid4()),
                weights=w,
                objectives=obj,
            ))

        for gen in range(self.generations):
            fronts = self._fast_non_dominated_sort(points)
            rank: Dict[int, int] = {}
            crowding: Dict[int, float] = {}
            for i, front in enumerate(fronts):
                cd = self._crowding_distance(front)
                for p in front:
                    rank[id(p)] = i
                    crowding[id(p)] = cd.get(id(p), 0.0)

            ind_to_point = {
                tuple(sorted(p.weights.items())): p for p in points
            }

            offspring: List[MOPDWeightVector] = []
            while len(offspring) < self.population_size:
                p1 = self._tournament_selection(population, rank, crowding, ind_to_point)
                p2 = self._tournament_selection(population, rank, crowding, ind_to_point)
                child = (
                    self._crossover(p1, p2)
                    if random.random() < self.crossover_rate else dict(p1)
                )
                child = self._mutate(child)
                obj = await self._evaluate(child)
                offspring.append(MOPDWeightVector(
                    vector_id=str(uuid.uuid4()),
                    weights=child,
                    objectives=obj,
                ))

            combined = points + offspring
            fronts = self._fast_non_dominated_sort(combined)
            next_points: List[MOPDWeightVector] = []
            for front in fronts:
                if len(next_points) + len(front) <= self.population_size:
                    next_points.extend(front)
                else:
                    cd = self._crowding_distance(front)
                    sorted_front = sorted(front, key=lambda p: cd.get(id(p), 0.0), reverse=True)
                    next_points.extend(sorted_front[: self.population_size - len(next_points)])
                    break
            points = next_points
            population = [dict(p.weights) for p in points]

            self.evolution_history.append({
                "generation": gen,
                "pareto_size": len(fronts[0]) if fronts else 0,
                "population_size": len(points),
            })

        fronts = self._fast_non_dominated_sort(points)
        self.pareto_front = fronts[0] if fronts else points
        self._all_points = points

        best = self._select_best_from_pareto(
            self.pareto_front, self._compute_dynamic_weights(),
        )
        if best:
            self.best_individual = dict(best.weights)
            self.best_fitness = best.scalarised_score
        return self.pareto_front


# ============================================================================
# 11. LIMIT GRAPH / MODP / RLHF / MoE
# ============================================================================
class LimitGraphManager:
    def __init__(self, storage: Optional[Any] = None):
        self.storage = storage
        self.graphs: Dict[str, Dict[str, Any]] = {}

    def create_graph(self, graph_id: str, description: str, configuration: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, "save_limit_graph_metadata"):
            self.storage.save_limit_graph_metadata(graph_id, description, configuration)
        else:
            self.graphs[graph_id] = {
                "description": description, "configuration": configuration,
                "nodes": {}, "edges": {},
            }

    def add_node(self, graph_id: str, node_id: str, node_type: Optional[str], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, "save_limit_graph_node"):
            self.storage.save_limit_graph_node(node_id, graph_id, node_type, attributes)
        else:
            self.graphs.setdefault(graph_id, {"nodes": {}, "edges": {}})
            self.graphs[graph_id]["nodes"][node_id] = {
                "node_type": node_type, "attributes": attributes,
            }

    def add_edge(self, graph_id: str, edge_id: str, source: str, target: str,
                 weight: Optional[float], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, "save_limit_graph_edge"):
            self.storage.save_limit_graph_edge(edge_id, graph_id, source, target, weight, attributes)
        else:
            self.graphs.setdefault(graph_id, {"nodes": {}, "edges": {}})
            self.graphs[graph_id]["edges"][edge_id] = {
                "source": source, "target": target,
                "weight": weight, "attributes": attributes,
            }

    def get_nodes(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, "get_limit_graph_nodes"):
            return self.storage.get_limit_graph_nodes(graph_id)
        return list(self.graphs.get(graph_id, {}).get("nodes", {}).values())

    def get_edges(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, "get_limit_graph_edges"):
            return self.storage.get_limit_graph_edges(graph_id)
        return list(self.graphs.get(graph_id, {}).get("edges", {}).values())

    def get_metadata(self, graph_id: str) -> Optional[Dict]:
        if self.storage and hasattr(self.storage, "get_limit_graph_metadata"):
            return self.storage.get_limit_graph_metadata(graph_id)
        return self.graphs.get(graph_id, {})


class MODPOptimizer:
    def __init__(self, storage: Optional[Any] = None):
        self.storage = storage
        self.states: Dict[str, List[Dict[str, Any]]] = {}

    def add_state(self, state_id: str, problem_id: str, state_attributes: Dict[str, Any],
                  objective_values: Dict[str, float], stage: int) -> None:
        if self.storage and hasattr(self.storage, "save_modp_state"):
            self.storage.save_modp_state(state_id, problem_id, state_attributes, objective_values, stage)
        else:
            self.states.setdefault(problem_id, []).append({
                "state_id": state_id, "state_attributes": state_attributes,
                "objective_values": objective_values, "stage": stage,
            })

    def add_policy(self, policy_id: str, problem_id: str, state_id: str,
                   action: str, expected_objectives: Dict[str, float]) -> None:
        if self.storage and hasattr(self.storage, "save_modp_policy"):
            self.storage.save_modp_policy(policy_id, problem_id, state_id, action, expected_objectives)

    def get_states(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, "get_modp_states"):
            return self.storage.get_modp_states(problem_id)
        return self.states.get(problem_id, [])

    def get_policies(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, "get_modp_policies"):
            return self.storage.get_modp_policies(problem_id)
        return []


class RLHFTrainer:
    def __init__(self, storage: Optional[Any] = None):
        self.storage = storage
        self.pairs: List[Dict[str, Any]] = []

    def record_pair(self, pair_id: str, prompt: str, chosen: str, rejected: str,
                    reward_diff: float, metadata: Optional[Dict] = None) -> None:
        if self.storage and hasattr(self.storage, "save_preference_pair"):
            self.storage.save_preference_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)
        else:
            self.pairs.append({
                "pair_id": pair_id, "prompt": prompt, "chosen": chosen,
                "rejected": rejected, "reward_diff": reward_diff, "metadata": metadata,
            })
        try:
            RLHF_PAIRS.inc()
        except Exception:
            pass

    def get_pairs(self, limit: int = 100) -> List[Dict]:
        if self.storage and hasattr(self.storage, "get_preference_pairs"):
            return self.storage.get_preference_pairs(limit)
        return self.pairs[-limit:]

    def train_reward_model(self) -> None:
        pairs = self.get_pairs()
        if len(pairs) < 5:
            log_event("info", "Not enough preference pairs for RLHF training.")
            return
        log_event("info", f"Training reward model on {len(pairs)} preference pairs...")


class MoEGatingNetwork:
    def __init__(self, storage: Optional[Any] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.expert_names = self.config.get("expert_names", ["online", "offline", "rule_based"])
        self.num_experts = len(self.expert_names)
        self.gating_weights = np.random.randn(self.num_experts, 5) * 0.01
        self.gating_lr = float(self.config.get("gating_lr", 0.05))
        self._reward_baseline = 0.5

    def _encode_state(self, metrics: Dict[str, float]) -> np.ndarray:
        features = [
            metrics.get("carbon", 0.5),
            metrics.get("rare_earth", 0.5),
            metrics.get("water", 0.5),
            metrics.get("operational", 0.5),
            metrics.get("material_index", 0.5),
        ]
        return np.array(features, dtype=np.float32)

    async def select_expert(self, metrics: Dict[str, float]) -> Tuple[str, np.ndarray]:
        x = self._encode_state(metrics)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs = probs / probs.sum()
        expert_idx = int(np.argmax(probs))
        selected = self.expert_names[expert_idx]
        if self.storage and hasattr(self.storage, "log_routing_decision"):
            try:
                sample_id = hashlib.sha256(str(metrics).encode()).hexdigest()[:16]
                self.storage.log_routing_decision(
                    str(uuid.uuid4()), sample_id, selected, float(probs[expert_idx]),
                )
            except Exception:
                pass
        try:
            MOE_ROUTING.labels(expert=selected).inc()
        except Exception:
            pass
        return selected, probs

    async def add_training_sample(
        self, metrics: Dict[str, float], selected_expert: str, reward: float,
    ) -> None:
        if selected_expert not in self.expert_names:
            return
        x = self._encode_state(metrics)
        expert_idx = self.expert_names.index(selected_expert)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs = probs / probs.sum()
        # REINFORCE with a running baseline.
        self._reward_baseline = 0.95 * self._reward_baseline + 0.05 * float(reward)
        advantage = float(reward) - self._reward_baseline
        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        grad = advantage * (probs - target)[:, None] * x[None, :]
        self.gating_weights -= self.gating_lr * grad


# ============================================================================
# 12. ADAPTIVE MATERIAL COST FUNCTION
# ============================================================================
@dataclass
class _RequestContext:
    """Per-call state; replaces the previous `self._last_*` attributes."""
    state_vec: np.ndarray
    action_idx: Optional[int]
    teacher_probs: Optional[np.ndarray]
    moe_metrics: Optional[Dict[str, float]] = None
    selected_expert: Optional[str] = None


class AdaptiveMaterialCostFunction:
    STRATEGY_KEYS = ["carbon", "rare_earth", "water", "operational"]

    def __init__(
        self,
        lca_client: LCAClient,
        config: Optional[LCAConfig] = None,
        storage: Optional[Any] = None,
    ):
        self.lca_client = lca_client
        self.config = config or LCAConfig()
        self.storage = storage

        historical_path = Path(self.config.historical_model_path)
        q_weights_path = Path(self.config.q_weights_path)
        self.weight_optimizer = DistillationWeightOptimizer(
            config={
                "distillation_epsilon": self.config.distillation_epsilon,
                "distillation_train_every": self.config.distillation_train_every,
                "distillation_replay_size": self.config.distillation_replay_size,
                "distillation_learning_rate": self.config.distillation_learning_rate,
                "distill_weight": self.config.distill_weight,
                "rl_weight": self.config.rl_weight,
            },
            historical_model_path=historical_path,
            q_weights_path=q_weights_path,
        )
        self.interaction_log: List[Dict[str, Any]] = []

        # Legacy attributes kept for backward compatibility. Not used internally.
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

        # MOEA
        self.moea_optimizer: Optional[NSGAIIWeightOptimizer] = None
        self.global_best_weights: Optional[Dict[str, float]] = None
        self.pareto_front: List[MOPDWeightVector] = []
        self._moea_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

        # v2.4.0 components
        self.limit_graph_manager = (
            LimitGraphManager(storage) if self.config.enable_limit_graph else None
        )
        self.modp_solver = (
            MODPOptimizer(storage) if self.config.enable_modp else None
        )
        self.rlhf_trainer = (
            RLHFTrainer(storage) if self.config.enable_rlhf else None
        )
        self.moe_gating = (
            MoEGatingNetwork(
                storage, {"expert_names": ["online", "offline", "rule_based"]},
            ) if self.config.enable_moe else None
        )

        if self.limit_graph_manager:
            if not self.limit_graph_manager.get_metadata("weight_vectors"):
                self.limit_graph_manager.create_graph(
                    "weight_vectors", "Weight Vector Relationships", {},
                )
            for src in ("online", "offline", "rule_based"):
                self.limit_graph_manager.add_node(
                    "weight_vectors", f"source_{src}", src, {"type": "source"},
                )

    # ---- lifecycle ----
    async def start_background_tasks(self) -> None:
        if self._moea_task is None or self._moea_task.done():
            if self.config.moea_enabled:
                self._moea_task = asyncio.create_task(self._moea_loop())

    async def aclose(self) -> None:
        if self._moea_task:
            self._moea_task.cancel()
            try:
                await self._moea_task
            except (asyncio.CancelledError, Exception):
                pass
            self._moea_task = None
        # Persist Q-weights.
        try:
            self.weight_optimizer.q_teacher._save_state()
        except Exception:
            pass

    async def __aenter__(self) -> "AdaptiveMaterialCostFunction":
        await self.start_background_tasks()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def _moea_loop(self) -> None:
        interval = self.config.moea_interval_seconds
        while True:
            try:
                await asyncio.sleep(interval)
                await self.run_moea_update()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log_event("error", f"MOEA loop error: {exc}")
                await asyncio.sleep(60)

    # ---- MOEA ----
    async def _evaluate_weights(self, weights: Dict[str, float]) -> Dict[str, float]:
        """
        Evaluate a candidate weight vector.

        The candidate defines a preference over a fixed portfolio of hardware
        models. We rank the portfolio by the weighted score and compute the
        mean benefit of the top half. Different candidates select different
        subsets, producing a genuine trade-off.
        """
        portfolio: List[Tuple[str, Optional[str]]] = [
            ("NVIDIA A100", "24GB"),
            ("NVIDIA A100", "40GB"),
            ("NVIDIA H100", None),
            ("NVIDIA V100", None),
            ("AMD MI250", None),
        ]
        benefits: List[Dict[str, float]] = []
        for model, variant in portfolio:
            try:
                fp = await self.lca_client.get_footprint(model, variant)
            except Exception:
                continue
            benefits.append({
                "carbon": 1.0 - min(fp.embodied_carbon_kg / 100.0, 1.0),
                "rare_earth": 1.0 - min(fp.rare_earth_kg / 0.01, 1.0),
                "water": 1.0 - min(fp.water_usage_l / 50.0, 1.0),
                "operational": 1.0 - min(fp.energy_mj / 100.0, 1.0),
            })
        if not benefits:
            return {k: 0.0 for k in weights}

        keys = list(weights.keys())
        scores = [
            sum(float(weights.get(k, 0.0)) * b.get(k, 0.0) for k in keys)
            for b in benefits
        ]
        top_k = max(1, len(benefits) // 2)
        top_idx = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_k]
        selected = [benefits[i] for i in top_idx]
        return {
            k: float(np.mean([b.get(k, 0.0) for b in selected]))
            for k in keys
        }

    async def run_moea_update(self) -> List[MOPDWeightVector]:
        if self.moea_optimizer is None:
            self.moea_optimizer = NSGAIIWeightOptimizer(
                evaluate_func=self._evaluate_weights,
                population_size=self.config.moea_population_size,
                generations=self.config.moea_generations,
                mutation_rate=self.config.moea_mutation_rate,
                crossover_rate=self.config.moea_crossover_rate,
                tournament_size=self.config.moea_tournament_size,
                objective_weights=self.config.moea_objective_weights,
                dynamic_weights=self.config.moea_dynamic_weights,
            )
        pareto = await self.moea_optimizer.evolve()
        self.pareto_front = pareto
        try:
            MOEA_RUNS.inc()
            MOEA_PARETO.set(len(pareto))
        except Exception:
            pass

        if pareto:
            weights = self.moea_optimizer._compute_dynamic_weights()
            best = self.moea_optimizer._select_best_from_pareto(pareto, weights)
            if best:
                self.global_best_weights = best.weights
                log_event("info", f"MOEA selected best weights: {best.weights}")
                if self.modp_solver:
                    self.modp_solver.add_state(
                        state_id=f"moea_best_{best.vector_id}",
                        problem_id="material_weight_optimization",
                        state_attributes={"weights": best.weights},
                        objective_values=best.objectives,
                        stage=1,
                    )
                    MODP_STATES.inc()
                if self.limit_graph_manager:
                    self.limit_graph_manager.add_node(
                        "weight_vectors",
                        f"vector_{best.vector_id}",
                        "best_weight_vector",
                        {"weights": best.weights, "objectives": best.objectives},
                    )

        # Persist Pareto front.
        try:
            path = Path(self.config.moea_pareto_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            with open(tmp, "w") as f:
                json.dump([p.to_dict() for p in pareto], f, indent=2)
            os.replace(tmp, path)
        except Exception as exc:
            log_event("warning", f"Failed to persist MOEA Pareto front: {exc}")

        return pareto

    # ---- cost ----
    async def compute_cost(
        self,
        hardware_model: str,
        variant: Optional[str] = None,
        operational_energy_joules: float = 0.0,
        lifetime_years: float = 5.0,
        quantity: int = 1,
        carbon_intensity: float = 400.0,
        recycling_rate: float = 0.5,
        context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[float, Dict[str, Any]]:
        footprint = await self.lca_client.get_footprint(hardware_model, variant)

        state = CostState(
            embodied_carbon_kg=footprint.embodied_carbon_kg,
            rare_earth_kg=footprint.rare_earth_kg,
            water_usage_l=footprint.water_usage_l,
            material_index=footprint.material_index,
            operational_energy_joules=operational_energy_joules,
            lifetime_years=lifetime_years,
            quantity=quantity,
            carbon_intensity=carbon_intensity,
            recycling_rate=recycling_rate,
            avg_cost_trend=0.0,
            avg_user_rating=0.0,
        )

        selected_expert: Optional[str] = None
        strategy: str
        weights: Dict[str, float]
        state_vec: np.ndarray
        teacher_probs: np.ndarray
        action_idx: Optional[int] = None

        if self.moe_gating:
            metrics = {
                "carbon": footprint.embodied_carbon_kg,
                "rare_earth": footprint.rare_earth_kg,
                "water": footprint.water_usage_l,
                "operational": operational_energy_joules,
                "material_index": footprint.material_index,
            }
            selected_expert, _ = await self.moe_gating.select_expert(metrics)
            if selected_expert == "offline" and self.global_best_weights:
                weights = dict(self.global_best_weights)
            elif selected_expert == "rule_based":
                weights = self._strategy_to_weights("balanced")
            else:
                weights = self._strategy_to_weights("balanced")
            strategy = selected_expert
            state_vec = state.to_feature_vector()
            teacher_probs = np.ones(NUM_STRATEGIES) / NUM_STRATEGIES
        else:
            strategy, action_idx, state_vec, teacher_probs = (
                await self.weight_optimizer.select_strategy(state, exploration=True)
            )
            weights = self._strategy_to_weights(strategy)

        # Blend with the global best when not already using it.
        if self.global_best_weights is not None and selected_expert != "offline":
            for k in self.STRATEGY_KEYS:
                weights[k] = (
                    0.8 * float(self.global_best_weights.get(k, weights[k]))
                    + 0.2 * weights[k]
                )

        carbon_score = min(footprint.embodied_carbon_kg / 100.0, 1.0)
        rare_earth_score = min(footprint.rare_earth_kg / 0.01, 1.0)
        water_score = min(footprint.water_usage_l / 50.0, 1.0)
        operational_carbon = operational_energy_joules / 3.6e6 * (carbon_intensity / 1000)
        operational_score = min(operational_carbon / 10.0, 1.0)

        total_cost = (
            weights["carbon"] * carbon_score
            + weights["rare_earth"] * rare_earth_score
            + weights["water"] * water_score
            + weights["operational"] * operational_score
        )

        # Store per-request context on the instance for `record_outcome` to use.
        # This is legacy behaviour; a future refactor can return it to the caller.
        async with self._lock:
            self._ctx = _RequestContext(
                state_vec=state_vec,
                action_idx=action_idx,
                teacher_probs=teacher_probs,
                moe_metrics=(
                    {
                        "carbon": footprint.embodied_carbon_kg,
                        "rare_earth": footprint.rare_earth_kg,
                        "water": footprint.water_usage_l,
                        "operational": operational_energy_joules,
                        "material_index": footprint.material_index,
                    } if self.moe_gating else None
                ),
                selected_expert=selected_expert,
            )
            # Legacy attributes.
            self.last_state_vec = state_vec
            self.last_action_idx = action_idx
            self.last_teacher_probs = teacher_probs

        metadata = {
            "strategy": strategy,
            "weights": weights,
            "footprint": _pydantic_dump(footprint),
            "moea_blended": self.global_best_weights is not None,
            "moe_used": self.moe_gating is not None,
        }
        return total_cost, metadata

    def _strategy_to_weights(self, strategy: str) -> Dict[str, float]:
        mapping = {
            "balanced": {"carbon": 0.3, "rare_earth": 0.3, "water": 0.2, "operational": 0.2},
            "carbon_focus": {"carbon": 0.6, "rare_earth": 0.15, "water": 0.15, "operational": 0.1},
            "rare_earth_focus": {"carbon": 0.15, "rare_earth": 0.6, "water": 0.15, "operational": 0.1},
            "operational_focus": {"carbon": 0.2, "rare_earth": 0.2, "water": 0.1, "operational": 0.5},
            "water_focus": {"carbon": 0.2, "rare_earth": 0.2, "water": 0.5, "operational": 0.1},
        }
        return dict(mapping.get(strategy, mapping["balanced"]))

    async def record_outcome(
        self,
        cost: float,
        carbon_savings_kg: float,
        user_rating: Optional[float] = None,
    ) -> None:
        if user_rating is not None:
            reward = 0.5 * min(1.0, carbon_savings_kg / 10.0) + 0.3 * user_rating + 0.2 * (1 - cost)
        else:
            reward = 0.5 * min(1.0, carbon_savings_kg / 10.0) + 0.2 * (1 - cost)
        reward = max(0.0, min(1.0, reward))

        entry = {
            "timestamp": _now_utc().isoformat(),
            "cost": cost,
            "carbon_savings": carbon_savings_kg,
            "user_rating": user_rating,
            "reward": reward,
        }
        self.interaction_log.append(entry)
        if len(self.interaction_log) > 10_000:
            self.interaction_log = self.interaction_log[-5_000:]

        # Best-effort CSV append; never let a write error kill the outcome.
        try:
            path = Path(self.config.interaction_logs_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            file_exists = path.exists() and path.stat().st_size > 0
            with open(path, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(entry.keys()))
                if not file_exists:
                    writer.writeheader()
                writer.writerow(entry)
        except Exception as exc:
            log_event("warning", f"Failed to append interaction log: {exc}")

        ctx = getattr(self, "_ctx", None)

        # Distillation update.
        if ctx is not None and ctx.state_vec is not None and ctx.action_idx is not None:
            await self.weight_optimizer.update(
                ctx.state_vec,
                ctx.action_idx,
                reward,
                ctx.state_vec,
                ctx.teacher_probs,
            )

        # MoE gating update.
        if (
            self.moe_gating is not None
            and ctx is not None
            and ctx.moe_metrics is not None
            and ctx.selected_expert is not None
        ):
            await self.moe_gating.add_training_sample(
                ctx.moe_metrics, ctx.selected_expert, reward,
            )

        # RLHF sample.
        if self.rlhf_trainer and random.random() < 0.05:
            chosen = (
                ctx.selected_expert if ctx is not None and ctx.selected_expert
                else "online"
            )
            rejected = random.choice(
                [s for s in ("online", "offline", "rule_based") if s != chosen]
                or ["rule_based"]
            )
            self.rlhf_trainer.record_pair(
                pair_id=str(uuid.uuid4()),
                prompt="Which weight source produced better cost?",
                chosen=chosen,
                rejected=rejected,
                reward_diff=reward,
                metadata={"cost": cost, "carbon_savings": carbon_savings_kg},
            )

        # MODP state.
        if self.modp_solver:
            self.modp_solver.add_state(
                state_id=str(uuid.uuid4()),
                problem_id="material_cost",
                state_attributes={
                    "cost": cost,
                    "carbon_savings": carbon_savings_kg,
                    "user_rating": user_rating,
                },
                objective_values={
                    "carbon": carbon_savings_kg,
                    "cost": 1.0 - cost,
                    "satisfaction": reward,
                },
                stage=0,
            )
            MODP_STATES.inc()

        # LIMIT graph.
        if self.limit_graph_manager:
            self.limit_graph_manager.add_node(
                "weight_vectors",
                f"outcome_{uuid.uuid4()}",
                "cost_outcome",
                {"cost": cost, "carbon_savings": carbon_savings_kg, "reward": reward},
            )

    async def material_index(self, hardware_model: str, variant: Optional[str] = None) -> float:
        footprint = await self.lca_client.get_footprint(hardware_model, variant)
        return footprint.material_index

    async def get_limit_graph(self, graph_id: str = "weight_vectors") -> Dict:
        if self.limit_graph_manager:
            return {
                "metadata": self.limit_graph_manager.get_metadata(graph_id),
                "nodes": self.limit_graph_manager.get_nodes(graph_id),
                "edges": self.limit_graph_manager.get_edges(graph_id),
            }
        return {}

    async def get_moe_experts(self) -> List[str]:
        if self.moe_gating:
            return list(self.moe_gating.expert_names)
        return []

    async def get_rlhf_pairs(self, limit: int = 100) -> List[Dict]:
        if self.rlhf_trainer:
            return self.rlhf_trainer.get_pairs(limit)
        return []

    async def record_rlhf_pair(
        self, pair_id: str, prompt: str, chosen: str, rejected: str,
        reward_diff: float, metadata: Optional[Dict] = None,
    ) -> None:
        if self.rlhf_trainer:
            self.rlhf_trainer.record_pair(
                pair_id, prompt, chosen, rejected, reward_diff, metadata,
            )


# ============================================================================
# 13. INTEGRATION WITH PREDICTIVE MAINTENANCE
# ============================================================================
class MaterialAwarePredictiveMaintenance:
    def __init__(self, lca_client: LCAClient, pm_engine: Any):
        self.lca_client = lca_client
        self.pm_engine = pm_engine

    async def register_node(
        self, node_id: str, hardware_model: str,
        variant: Optional[str] = None, initial_flops: float = 1e12,
    ) -> None:
        footprint = await self.lca_client.get_footprint(hardware_model, variant)
        log_event(
            "info",
            f"Registered {node_id} ({hardware_model}/{variant})",
            material_index=footprint.material_index,
        )
        if hasattr(self.pm_engine, "update_node"):
            try:
                await self.pm_engine.update_node(node_id, initial_flops, 0.0)
            except Exception as exc:
                log_event("warning", f"pm_engine.update_node failed: {exc}")


# ============================================================================
# 14. FASTAPI (guarded)
# ============================================================================
if FASTAPI_AVAILABLE:
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _lifespan(app: "FastAPI"):
        app.lca_client = LCAClient(LCAConfig())
        app.cost_function = AdaptiveMaterialCostFunction(app.lca_client)
        await app.cost_function.start_background_tasks()
        log_event("info", "Material LCA API started")
        try:
            yield
        finally:
            try:
                await app.cost_function.aclose()
            except Exception:
                pass
            try:
                await app.lca_client.close()
            except Exception:
                pass
            log_event("info", "Material LCA API shut down")

    app = FastAPI(title="Material LCA API", version="2.5.0", lifespan=_lifespan)

    async def get_lca_client() -> LCAClient:
        if not hasattr(app, "lca_client"):
            app.lca_client = LCAClient(LCAConfig())
        return app.lca_client

    async def get_cost_function(
        lca_client: LCAClient = Depends(get_lca_client),
    ) -> AdaptiveMaterialCostFunction:
        return AdaptiveMaterialCostFunction(lca_client)

    async def get_simulator(
        lca_client: LCAClient = Depends(get_lca_client),
    ) -> DigitalTwinMaterialSimulator:
        return DigitalTwinMaterialSimulator(lca_client)

    @app.get("/footprint/{hardware_model}")
    async def get_footprint(
        hardware_model: str,
        variant: Optional[str] = None,
        force_refresh: bool = False,
        lca_client: LCAClient = Depends(get_lca_client),
    ):
        fp = await lca_client.get_footprint(hardware_model, variant, force_refresh)
        return _pydantic_dump(fp)

    @app.post("/footprint/batch")
    async def get_footprints_batch(
        models: List[Dict[str, str]],
        force_refresh: bool = False,
        lca_client: LCAClient = Depends(get_lca_client),
    ):
        tuples = [(m["hardware_model"], m.get("variant")) for m in models]
        footprints = await lca_client.get_footprints_batch(tuples)
        return {k: _pydantic_dump(v) for k, v in footprints.items()}

    @app.post("/simulate")
    async def simulate_refresh(
        hardware_model: str,
        variant: Optional[str] = None,
        quantity: int = 1,
        lifetime_years: float = 5,
        refresh_interval_years: float = 3,
        years_to_simulate: int = 10,
        operational_energy_per_year_joules: float = 0.0,
        recycling_rate: float = 0.5,
        transport_distance_km: float = 1000.0,
        simulator: DigitalTwinMaterialSimulator = Depends(get_simulator),
    ):
        return await simulator.simulate_refresh_cycle(
            hardware_model=hardware_model,
            variant=variant,
            quantity=quantity,
            lifetime_years=lifetime_years,
            refresh_interval_years=refresh_interval_years,
            years_to_simulate=years_to_simulate,
            operational_energy_per_year_joules=operational_energy_per_year_joules,
            recycling_rate=recycling_rate,
            transport_distance_km=transport_distance_km,
        )

    @app.post("/cost/{hardware_model}")
    async def compute_cost(
        hardware_model: str,
        variant: Optional[str] = None,
        operational_energy_joules: float = 0.0,
        lifetime_years: float = 5.0,
        quantity: int = 1,
        carbon_intensity: float = 400.0,
        recycling_rate: float = 0.5,
        cost_func: AdaptiveMaterialCostFunction = Depends(get_cost_function),
    ):
        cost, metadata = await cost_func.compute_cost(
            hardware_model, variant, operational_energy_joules,
            lifetime_years, quantity, carbon_intensity, recycling_rate,
        )
        return {"cost": cost, "metadata": metadata}

    @app.get("/metrics")
    async def get_metrics():
        if PROMETHEUS_AVAILABLE:
            return Response(
                content=generate_latest(REGISTRY),
                media_type=CONTENT_TYPE_LATEST,
            )
        return {"error": "Prometheus not enabled"}


# ============================================================================
# 15. FACTORY
# ============================================================================
def create_material_lca_integration(
    node_registry: Optional[Any] = None,
    pm_engine: Optional[Any] = None,
    config: Optional[LCAConfig] = None,
    storage: Optional[Any] = None,
):
    config = config or LCAConfig()
    lca_client = LCAClient(config)
    simulator = DigitalTwinMaterialSimulator(lca_client)
    cost_function = AdaptiveMaterialCostFunction(lca_client, config, storage)

    if node_registry:
        original_register = node_registry.register_node

        async def patched_register(node_id, hardware_model, variant=None, **kwargs):
            descriptor = original_register(node_id, hardware_model, **kwargs)
            footprint = await lca_client.get_footprint(hardware_model, variant)
            if hasattr(descriptor, "material_index"):
                descriptor.material_index = footprint.material_index
            if hasattr(descriptor, "material_footprint"):
                descriptor.material_footprint = footprint
            return descriptor

        node_registry.register_node = patched_register

    pm_integration = None
    if pm_engine:
        pm_integration = MaterialAwarePredictiveMaintenance(lca_client, pm_engine)

    return {
        "lca_client": lca_client,
        "simulator": simulator,
        "cost_function": cost_function,
        "node_registry": node_registry,
        "pm_integration": pm_integration,
    }


# ============================================================================
# 16. HISTORICAL MODEL TRAINING
# ============================================================================
def train_historical_model(
    log_path: Path = Path("./lca_interactions.csv"),
    model_path: Path = Path("./lca_historical_model.pkl"),
) -> None:
    if not log_path.exists():
        log_event("warning", f"Interaction logs not found at {log_path}. No model trained.")
        return
    try:
        import pandas as pd
    except ImportError:
        log_event("error", "pandas is required for historical training.")
        return
    df_logs = pd.read_csv(log_path)
    if len(df_logs) < 10:
        log_event("warning", "Not enough logs to train historical model (need >= 10).")
        return
    if not SKLEARN_ML:
        log_event("error", "scikit-learn is required for historical training.")
        return

    if "state_vector" not in df_logs.columns or "action" not in df_logs.columns:
        log_event(
            "warning",
            "Logs are missing `state_vector` / `action` columns; nothing to train on.",
        )
        return

    def _parse_vec(s: Any) -> np.ndarray:
        if isinstance(s, str):
            return np.array(
                [float(x) for x in s.split(",") if x != ""], dtype=np.float32,
            )
        return np.asarray(s, dtype=np.float32)

    try:
        X = np.stack([_parse_vec(s) for s in df_logs["state_vector"].values])
    except Exception as exc:
        log_event("error", f"Failed to parse state vectors: {exc}")
        return

    y = df_logs["action"].values
    le = LabelEncoder()
    y_enc = le.fit_transform(y)
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X, y_enc)

    model_path.parent.mkdir(parents=True, exist_ok=True)
    with open(model_path, "wb") as f:
        pickle.dump((clf, le), f)
    log_event("info", f"Historical model saved to {model_path}")


# ============================================================================
# 17. EXAMPLE USAGE
# ============================================================================
async def main():
    config = LCAConfig(
        moea_enabled=True,
        moea_interval_seconds=10,
        enable_limit_graph=True,
        enable_modp=True,
        enable_rlhf=True,
        enable_moe=True,
    )
    lca_client = LCAClient(config)
    cost_func = AdaptiveMaterialCostFunction(lca_client, config)
    try:
        await cost_func.start_background_tasks()

        cost, metadata = await cost_func.compute_cost(
            hardware_model="NVIDIA A100",
            variant="24GB",
            operational_energy_joules=1e6,
            lifetime_years=5,
            quantity=10,
            carbon_intensity=500,
            recycling_rate=0.6,
        )
        print(f"Cost: {cost}")
        print(f"Metadata: {metadata}")

        await cost_func.run_moea_update()
        print("MOEA best weights:", cost_func.global_best_weights)

        await cost_func.record_outcome(
            cost=0.3, carbon_savings_kg=5, user_rating=0.9,
        )

        limit_graph = await cost_func.get_limit_graph()
        print("Limit graph nodes:", len(limit_graph.get("nodes", [])))
        print("MoE experts:", await cost_func.get_moe_experts())
        print("Distillation stats:", cost_func.weight_optimizer.get_stats())
    finally:
        await cost_func.aclose()
        await lca_client.close()


if __name__ == "__main__":
    import asyncio as _asyncio
    _asyncio.run(main())
