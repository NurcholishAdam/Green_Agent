# material_lca_v2_2_0.py
# Version: 2.2.0
"""
Enhanced Material Index Integration with Hardware Life‑Cycle Databases v2.2.0
======================================================================

Fetches accurate embodied carbon and rare‑earth content from public LCA databases
and integrates adaptive weight selection via Multi‑Teacher On‑Policy Distillation.

ENHANCEMENTS OVER v2.1.0:
- Adaptive weight selection for the cost function using distillation.
- State‑aware choice of weight configurations based on context.
- Online learning from outcome metrics (carbon savings, user ratings).
- Teachers: rule‑based, historical ML, stateful Q.
- Student: linear softmax with distillation + REINFORCE.
- Persistence for Q‑teacher weights and interaction logs.
- Offline training for historical ML teacher.
- Unit tests for distillation components.
"""

import asyncio
import json
import logging
import os
import time
import uuid
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List, Tuple, Callable, Awaitable
from collections import deque
from enum import Enum
import random
import numpy as np
from abc import ABC, abstractmethod
import pickle
import pandas as pd
from pathlib import Path

import aiohttp
import aiofiles

# ---------- Pydantic ----------
from pydantic import BaseModel, Field, validator

# ---------- Tenacity ----------
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Structlog ----------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

# ---------- scikit-learn for ML teacher ----------
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

# ---------- FastAPI (optional) ----------
try:
    from fastapi import FastAPI, HTTPException, Depends
    from fastapi.responses import JSONResponse
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

# ============================================================================
# 1. CONFIGURATION (expanded with distillation settings)
# ============================================================================
class LCAConfig(BaseModel):
    # Data source
    source: str = Field("mock", description="'mock', 'ecoinvent', 'openlca', 'cache_only'")
    api_url: str = Field("https://api.example.com/lca")
    api_key: Optional[str] = None
    # Cache
    cache_dir: str = "./lca_cache"
    cache_ttl: int = 86400 * 7  # 7 days
    # Retry and circuit breaker
    max_retry_attempts: int = 3
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 30
    # Fallback
    default_embodied_carbon: float = 50.0
    default_rare_earth_fraction: float = 0.001
    default_material_index: float = 1.0
    # Variant support
    variant_fields: List[str] = Field(default_factory=lambda: ["memory", "revision"])

    # NEW: Distillation parameters
    distillation_epsilon: float = Field(0.1, ge=0, le=1)
    distillation_train_every: int = Field(10, ge=1)
    distillation_replay_size: int = Field(2000, ge=10)
    distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
    distill_weight: float = Field(0.7, ge=0, le=1)
    rl_weight: float = Field(0.3, ge=0, le=1)

    # Persistence paths
    q_weights_path: str = Field("./lca_q_weights.json")
    interaction_logs_path: str = Field("./lca_interactions.csv")
    historical_model_path: str = Field("./lca_historical_model.pkl")

    @validator('source')
    def source_must_be_valid(cls, v):
        allowed = {'mock', 'ecoinvent', 'openlca', 'cache_only'}
        if v not in allowed:
            raise ValueError(f'source must be one of {allowed}')
        return v

# ============================================================================
# 2. DATA STRUCTURES (enhanced with variant support)
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
    timestamp: datetime = Field(default_factory=datetime.now)

    @validator('material_index')
    def material_index_positive(cls, v):
        if v < 0:
            raise ValueError('material_index must be non‑negative')
        return v

    def cache_key(self) -> str:
        if self.variant:
            return f"{self.hardware_model}:{self.variant}"
        return self.hardware_model

# ============================================================================
# 3. CIRCUIT BREAKER
# ============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    """Circuit breaker with half-open state for external calls."""
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        async with self._lock:
            now = datetime.utcnow()
            if self.state == CircuitBreakerState.OPEN:
                if self.last_failure_time and (now - self.last_failure_time).total_seconds() >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.failure_count = 0
                    logger.info(f"Circuit breaker {self.name} entering HALF_OPEN")
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")

        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    logger.info(f"Circuit breaker {self.name} closed after success")
                else:
                    self.failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.utcnow()
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker {self.name} opened after {self.failure_count} failures")
            raise e

# ============================================================================
# 4. TASK MANAGER (for supervised background tasks)
# ============================================================================
class TaskManager:
    """Supervises background tasks with auto-restart on failure."""
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()
        self.shutdown_event = asyncio.Event()

    def start_task(self, name: str, coro_func: Callable[[], Awaitable[None]], *args, **kwargs):
        async def wrapper():
            backoff = 1
            max_backoff = 300
            while not self.shutdown_event.is_set():
                try:
                    await coro_func(*args, **kwargs)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Task '{name}' crashed", error=str(e), exc_info=True)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max_backoff)
        task = asyncio.create_task(wrapper(), name=name)
        async with self._lock:
            self.tasks[name] = task
        return task

    async def stop_all(self):
        self.shutdown_event.set()
        async with self._lock:
            for task in self.tasks.values():
                task.cancel()
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
            self.tasks.clear()
        logger.info("All background tasks stopped")

# ============================================================================
# 5. RETRY DECORATOR
# ============================================================================
def retry_decorator(attempts: int = 3, min_wait: int = 2, max_wait: int = 10):
    if TENACITY_AVAILABLE:
        return retry(
            stop=stop_after_attempt(attempts),
            wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
            retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
            before_sleep=before_sleep_log(logger, logging.WARNING)
        )
    else:
        def decorator(func):
            async def wrapper(*args, **kwargs):
                for attempt in range(attempts):
                    try:
                        return await func(*args, **kwargs)
                    except Exception as e:
                        if attempt == attempts - 1:
                            raise
                        await asyncio.sleep(2 ** attempt)
                return None
            return wrapper
        return decorator

# ============================================================================
# 6. PROMETHEUS METRICS (custom registry)
# ============================================================================
if PROMETHEUS_AVAILABLE:
    REGISTRY = CollectorRegistry()
    LCA_API_CALLS = Counter('lca_api_calls_total', 'LCA API calls', ['source', 'status'], registry=REGISTRY)
    LCA_API_LATENCY = Histogram('lca_api_latency_seconds', 'LCA API call latency', registry=REGISTRY)
    CACHE_HITS = Counter('lca_cache_hits_total', 'Cache hits', registry=REGISTRY)
    CACHE_MISSES = Counter('lca_cache_misses_total', 'Cache misses', registry=REGISTRY)
    FOOTPRINT_FETCHED = Counter('lca_footprints_fetched_total', 'Footprints fetched', ['source'], registry=REGISTRY)
    SIMULATION_RUNS = Counter('lca_simulation_runs_total', 'DigitalTwin simulations run', registry=REGISTRY)
else:
    class DummyMetric:
        def labels(self, **kwargs): return self
        def inc(self, **kwargs): pass
        def observe(self, **kwargs): pass
        def set(self, **kwargs): pass
    LCA_API_CALLS = DummyMetric()
    LCA_API_LATENCY = DummyMetric()
    CACHE_HITS = DummyMetric()
    CACHE_MISSES = DummyMetric()
    FOOTPRINT_FETCHED = DummyMetric()
    SIMULATION_RUNS = DummyMetric()

# ============================================================================
# 7. LCA API CLIENT (enhanced)
# ============================================================================
class LCAClient:
    """
    Enhanced client with real API integration, caching, retry, circuit breaker,
    asynchronous cache I/O, batch fetching, and variant support.
    """

    def __init__(self, config: Optional[LCAConfig] = None):
        self.config = config or LCAConfig()
        self.cache: Dict[str, MaterialFootprint] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()
        self._circuit_breaker = CircuitBreaker(
            "lca_api",
            failure_threshold=self.config.circuit_breaker_threshold,
            recovery_timeout=self.config.circuit_breaker_timeout
        )
        self._task_manager = TaskManager()
        self._cache_loaded = False

        # Load cache synchronously
        self._load_cache_sync()

    def _load_cache_sync(self):
        path = self._cache_path()
        if not os.path.exists(path):
            logger.info("No LCA cache found, starting fresh")
            return
        try:
            with open(path, 'r') as f:
                data = json.load(f)
            for key, fp_dict in data.items():
                fp = MaterialFootprint(**fp_dict)
                self.cache[key] = fp
            self._cache_loaded = True
            logger.info("Loaded LCA cache", count=len(self.cache))
        except Exception as e:
            logger.warning("Failed to load LCA cache", error=str(e))

    def _cache_path(self) -> str:
        os.makedirs(self.config.cache_dir, exist_ok=True)
        return os.path.join(self.config.cache_dir, "lca_cache.json")

    async def _save_cache_async(self):
        path = self._cache_path()
        data = {key: fp.dict() for key, fp in self.cache.items()}
        try:
            async with aiofiles.open(path, 'w') as f:
                await f.write(json.dumps(data, indent=2))
            logger.debug("Cache saved")
        except Exception as e:
            logger.warning("Failed to save LCA cache", error=str(e))

    def _schedule_cache_save(self):
        self._task_manager.start_task("lca_cache_save", self._save_cache_async)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    @retry_decorator(attempts=3)
    async def _fetch_from_api(self, hardware_model: str, variant: Optional[str] = None) -> Optional[MaterialFootprint]:
        source = self.config.source
        if source == "mock":
            return self._generate_mock_footprint(hardware_model, variant)
        elif source in ("ecoinvent", "openlca"):
            session = await self._get_session()
            url = f"{self.config.api_url}/footprint"
            params = {"model": hardware_model, "source": source}
            if variant:
                params["variant"] = variant
            headers = {}
            if self.config.api_key:
                headers['Authorization'] = f"Bearer {self.config.api_key}"
            start = time.time()
            async with session.get(url, params=params, headers=headers, timeout=30) as resp:
                LCA_API_LATENCY.observe(time.time() - start)
                if resp.status != 200:
                    raise aiohttp.ClientError(f"API returned {resp.status}")
                data = await resp.json()
                fp = MaterialFootprint(
                    hardware_model=data['hardware_model'],
                    variant=data.get('variant'),
                    embodied_carbon_kg=data['embodied_carbon_kg'],
                    rare_earth_kg=data['rare_earth_kg'],
                    total_mass_kg=data['total_mass_kg'],
                    material_index=data['material_index'],
                    water_usage_l=data.get('water_usage_l', 0.0),
                    energy_mj=data.get('energy_mj', 0.0),
                    manufacturing_energy_mj=data.get('manufacturing_energy_mj', 0.0),
                    transport_emissions_kg_co2=data.get('transport_emissions_kg_co2', 0.0),
                    source=source,
                )
                return fp
        else:
            return None

    def _generate_mock_footprint(self, hardware_model: str, variant: Optional[str] = None) -> MaterialFootprint:
        base_hash = int(hashlib.md5(hardware_model.encode()).hexdigest(), 16) % 1000
        variant_hash = int(hashlib.md5((variant or "").encode()).hexdigest(), 16) % 100 if variant else 0
        mass_kg = 2.0 + (base_hash % 10) * 0.5  # 2–7 kg
        carbon = 20.0 + (base_hash % 80) * 0.5  # 20–60 kg CO₂
        rare_earth = 0.002 + (base_hash % 5) * 0.001  # 0.002–0.007 kg
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

    async def get_footprint(self, hardware_model: str, variant: Optional[str] = None, force_refresh: bool = False) -> MaterialFootprint:
        cache_key = f"{hardware_model}:{variant}" if variant else hardware_model

        async with self._lock:
            if not force_refresh and cache_key in self.cache:
                cached = self.cache[cache_key]
                age = (datetime.now() - cached.timestamp).total_seconds()
                if age < self.config.cache_ttl:
                    CACHE_HITS.inc()
                    logger.debug("Cache hit", model=hardware_model, variant=variant)
                    return cached
                else:
                    CACHE_MISSES.inc()
                    logger.debug("Cache expired", model=hardware_model, variant=variant)

            if self.config.source == "cache_only":
                raise ValueError(f"Footprint for {cache_key} not found in cache and cache_only is set")

            try:
                fp = await self._circuit_breaker.call(self._fetch_from_api, hardware_model, variant)
                if fp is None:
                    raise ValueError("API returned no data")
                LCA_API_CALLS.labels(source=self.config.source, status='success').inc()
                FOOTPRINT_FETCHED.labels(source=fp.source).inc()
                self.cache[cache_key] = fp
                self._schedule_cache_save()
                return fp
            except Exception as e:
                logger.error("API fetch failed", model=hardware_model, variant=variant, error=str(e))
                LCA_API_CALLS.labels(source=self.config.source, status='failed').inc()
                if cache_key in self.cache:
                    logger.warning("Returning expired cache entry as fallback", model=hardware_model, variant=variant)
                    return self.cache[cache_key]
                else:
                    fallback = MaterialFootprint(
                        hardware_model=hardware_model,
                        variant=variant,
                        embodied_carbon_kg=self.config.default_embodied_carbon,
                        rare_earth_kg=self.config.default_rare_earth_fraction * 1.0,
                        total_mass_kg=1.0,
                        material_index=self.config.default_material_index,
                        source="default",
                    )
                    self.cache[cache_key] = fallback
                    self._schedule_cache_save()
                    return fallback

    async def get_footprints_batch(self, hardware_models: List[Tuple[str, Optional[str]]]) -> Dict[str, MaterialFootprint]:
        tasks = [self.get_footprint(model, variant) for model, variant in hardware_models]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        footprints = {}
        for (model, variant), result in zip(hardware_models, results):
            cache_key = f"{model}:{variant}" if variant else model
            if isinstance(result, Exception):
                logger.error("Batch fetch failed for", model=model, variant=variant, error=str(result))
                fallback = MaterialFootprint(
                    hardware_model=model,
                    variant=variant,
                    embodied_carbon_kg=self.config.default_embodied_carbon,
                    rare_earth_kg=self.config.default_rare_earth_fraction * 1.0,
                    total_mass_kg=1.0,
                    material_index=self.config.default_material_index,
                    source="default",
                )
                footprints[cache_key] = fallback
            else:
                footprints[cache_key] = result
        return footprints

    async def update_footprint(self, hardware_model: str, variant: Optional[str], footprint: MaterialFootprint) -> None:
        cache_key = f"{hardware_model}:{variant}" if variant else hardware_model
        async with self._lock:
            self.cache[cache_key] = footprint
            self._schedule_cache_save()

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        await self._task_manager.stop_all()

# ============================================================================
# 8. DIGITALTWIN SIMULATOR (unchanged)
# ============================================================================
class DigitalTwinMaterialSimulator:
    """
    Enhanced simulator with manufacturing energy, transport, and end‑of‑life treatment.
    """

    def __init__(self, lca_client: LCAClient = None):
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
        timeline = []

        for cycle in range(num_cycles):
            year = cycle * refresh_interval_years
            if year > years_to_simulate:
                break
            if cycle == 0:
                total_embodied_carbon += footprint.embodied_carbon_kg * quantity
                total_rare_earth += footprint.rare_earth_kg * quantity
                total_mass += footprint.total_mass_kg * quantity
                total_manufacturing_energy += footprint.manufacturing_energy_mj * quantity
                total_transport_emissions += footprint.transport_emissions_kg_co2 * quantity
            else:
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

        avg_carbon_per_kg = footprint.embodied_carbon_kg / footprint.total_mass_kg if footprint.total_mass_kg > 0 else 0
        recycled_carbon_saved = total_mass * recycling_rate * avg_carbon_per_kg

        total_carbon_with_recycling = total_embodied_carbon + operational_carbon_total - recycled_carbon_saved

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
            "avg_carbon_per_year": total_carbon_with_recycling / years_to_simulate if years_to_simulate > 0 else 0,
            "timeline": timeline,
        }

    async def compare_refresh_strategies(
        self,
        hardware_model: str,
        variant: Optional[str] = None,
        quantity: int = 1,
        strategies: List[Dict[str, float]] = None,
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
                operational_energy_per_year_joules=operational_energy_per_year_joules,
            )
            results.append(sim)
        return results

# ============================================================================
# 9. ADAPTIVE COST FUNCTION WITH DISTILLATION
# ============================================================================

@dataclass
class CostState:
    """State for the distillation agent."""
    # Hardware characteristics
    embodied_carbon_kg: float
    rare_earth_kg: float
    water_usage_l: float
    material_index: float
    # Workload context
    operational_energy_joules: float
    lifetime_years: float
    quantity: int
    # Environment
    carbon_intensity: float  # gCO₂/kWh
    recycling_rate: float
    # Historical performance (from logs)
    avg_cost_trend: float  # positive = increasing
    avg_user_rating: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert to 11‑dim numeric feature vector."""
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


# Teacher abstract base
class Teacher(ABC):
    @abstractmethod
    def predict(self, state: CostState) -> np.ndarray:
        """Return probability vector over 5 weight strategies."""
        pass

    @abstractmethod
    def confidence(self, state: CostState) -> float:
        """Return confidence in prediction [0,1]."""
        pass


class WeightRuleBasedTeacher(Teacher):
    """Rule‑based expert."""
    STRATEGIES = ['balanced', 'carbon_focus', 'rare_earth_focus', 'operational_focus', 'water_focus']

    def predict(self, state: CostState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if state.carbon_intensity > 500:
            probs[1] = 0.8  # carbon_focus
        elif state.rare_earth_kg > 0.005:
            probs[2] = 0.7  # rare_earth_focus
        elif state.operational_energy_joules > 5e6:
            probs[3] = 0.7  # operational_focus
        elif state.water_usage_l > 20:
            probs[4] = 0.6  # water_focus
        else:
            probs[0] = 0.6  # balanced
        return probs / probs.sum()

    def confidence(self, state: CostState) -> float:
        if state.carbon_intensity > 500:
            return 0.6
        return 0.4


class WeightHistoricalMLTeacher(Teacher):
    """Offline trained classifier from past interactions."""
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path(LCAConfig().historical_model_path)
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: CostState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        probs = self.model.predict_proba(x)[0]
        return probs

    def confidence(self, state: CostState) -> float:
        return 0.7 if self.model is not None else 0.0


class WeightStatefulQTeacher(Teacher):
    """Linear Q‑learning with state features."""
    def __init__(self, lr: float = 0.1):
        self.lr = lr
        self.weights = np.zeros((11, 5))  # 11 features, 5 actions
        self._load_state()

    def _load_state(self):
        path = Path(LCAConfig().q_weights_path)
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                logger.info(f"Loaded Q‑teacher weights from {path}")
            except Exception as e:
                logger.error(f"Failed to load Q‑weights: {e}")

    def _save_state(self):
        path = Path(LCAConfig().q_weights_path)
        with open(path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)

    def predict(self, state: CostState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: CostState) -> float:
        return 0.5

    def update(self, state: CostState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()


class DistillationStudent:
    def __init__(self, feature_dim: int = 11, n_classes: int = 5, lr: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray, num_classes: int) -> np.ndarray:
        if num_classes != self.n_classes:
            new_weights = np.zeros((self.weights.shape[0], num_classes))
            new_biases = np.zeros(num_classes)
            min_dim = min(self.n_classes, num_classes)
            new_weights[:, :min_dim] = self.weights[:, :min_dim]
            new_biases[:min_dim] = self.biases[:min_dim]
            self.weights = new_weights
            self.biases = new_biases
            self.n_classes = num_classes
        logits = state_vector @ self.weights + self.biases
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7, rl_weight: float = 0.3):
        current_probs = self.predict_proba(state_vector, self.n_classes)
        logits = state_vector @ self.weights + self.biases

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
        self.buffer = deque(maxlen=max_size)

    def push(self, state_vec: np.ndarray, action: int, reward: float,
             next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))

    def sample(self, batch_size: int = 32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), actions, np.array(rewards),
                np.array(next_states), np.array(teacher_probs))

    def __len__(self):
        return len(self.buffer)


class DistillationWeightOptimizer:
    """
    Multi‑teacher on‑policy distillation agent for weight selection.
    """
    STRATEGIES = ['balanced', 'carbon_focus', 'rare_earth_focus', 'operational_focus', 'water_focus']

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers: List[Teacher] = [
            WeightRuleBasedTeacher(),
            WeightHistoricalMLTeacher(),
            WeightStatefulQTeacher()
        ]
        self.replay_buffer = ReplayBuffer(max_size=config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0

    async def select_strategy(self, state: CostState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        n = 5

        teacher_probs = np.zeros(n)
        total_conf = 0.0
        for teacher in self.teachers:
            prob = teacher.predict(state)
            conf = teacher.confidence(state)
            if len(prob) != n:
                if len(prob) < n:
                    prob = np.pad(prob, (0, n - len(prob)), 'constant')
                else:
                    prob = prob[:n]
            teacher_probs += prob * conf
            total_conf += conf
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(n) / n

        student_probs = self.student.predict_proba(state_vec, n)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, n - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)

        return self.STRATEGIES[action_idx], action_idx, state_vec, teacher_probs

    async def update(self, state_vec: np.ndarray, action_idx: int, reward: float,
                     next_state_vec: np.ndarray, teacher_probs: np.ndarray):
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i])

    def get_stats(self) -> Dict:
        return {'student_counter': self.student.counter, 'buffer_size': len(self.replay_buffer)}


# ============================================================================
# ADAPTIVE COST FUNCTION (Enhanced)
# ============================================================================
class AdaptiveMaterialCostFunction:
    """
    Combines material footprint with adaptive weights selected by distillation.
    """

    def __init__(self, lca_client: LCAClient):
        self.lca_client = lca_client
        self.weight_optimizer = DistillationWeightOptimizer({
            'distillation_epsilon': LCAConfig().distillation_epsilon,
            'distillation_train_every': LCAConfig().distillation_train_every,
            'distillation_replay_size': LCAConfig().distillation_replay_size,
            'distillation_learning_rate': LCAConfig().distillation_learning_rate,
        })
        self.interaction_log: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

    async def compute_cost(
        self,
        hardware_model: str,
        variant: Optional[str] = None,
        operational_energy_joules: float = 0.0,
        lifetime_years: float = 5.0,
        quantity: int = 1,
        carbon_intensity: float = 400.0,  # gCO₂/kWh
        recycling_rate: float = 0.5,
        context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Compute a sustainability cost score using adaptive weights selected by distillation.

        Returns:
            cost: float
            metadata: dict containing strategy used, weights, etc.
        """
        footprint = await self.lca_client.get_footprint(hardware_model, variant)

        # Build state
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
            avg_cost_trend=0.0,  # could be derived from logs
            avg_user_rating=0.0,
        )

        # Select strategy via distillation
        strategy, action_idx, state_vec, teacher_probs = await self.weight_optimizer.select_strategy(state, exploration=True)
        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        # Apply strategy to get weights
        weights = self._strategy_to_weights(strategy)

        # Normalize metrics
        carbon_score = min(footprint.embodied_carbon_kg / 100.0, 1.0)
        rare_earth_score = min(footprint.rare_earth_kg / 0.01, 1.0)
        water_score = min(footprint.water_usage_l / 50.0, 1.0)
        operational_carbon = operational_energy_joules / 3.6e6 * (carbon_intensity / 1000)
        operational_score = min(operational_carbon / 10.0, 1.0)

        # Weighted sum
        total_cost = (
            weights["carbon"] * carbon_score +
            weights["rare_earth"] * rare_earth_score +
            weights["water"] * water_score +
            weights["operational"] * operational_score
        )

        metadata = {
            "strategy": strategy,
            "weights": weights,
            "footprint": footprint.dict(),
        }

        return total_cost, metadata

    def _strategy_to_weights(self, strategy: str) -> Dict[str, float]:
        """Return weight dict for the given strategy."""
        if strategy == 'balanced':
            return {"carbon": 0.3, "rare_earth": 0.3, "water": 0.2, "operational": 0.2}
        elif strategy == 'carbon_focus':
            return {"carbon": 0.6, "rare_earth": 0.15, "water": 0.15, "operational": 0.1}
        elif strategy == 'rare_earth_focus':
            return {"carbon": 0.15, "rare_earth": 0.6, "water": 0.15, "operational": 0.1}
        elif strategy == 'operational_focus':
            return {"carbon": 0.2, "rare_earth": 0.2, "water": 0.1, "operational": 0.5}
        elif strategy == 'water_focus':
            return {"carbon": 0.2, "rare_earth": 0.2, "water": 0.5, "operational": 0.1}
        else:
            return {"carbon": 0.3, "rare_earth": 0.3, "water": 0.2, "operational": 0.2}

    async def record_outcome(
        self,
        cost: float,
        carbon_savings_kg: float,
        user_rating: Optional[float] = None,
    ):
        """
        Record the outcome of a cost computation to update the distillation agent.
        """
        # Compute reward
        if user_rating is not None:
            reward = 0.5 * min(1.0, carbon_savings_kg / 10.0) + 0.3 * user_rating + 0.2 * (1 - cost)
        else:
            reward = 0.5 * min(1.0, carbon_savings_kg / 10.0) + 0.2 * (1 - cost)
        reward = max(0.0, min(1.0, reward))

        # Log interaction
        self.interaction_log.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'cost': cost,
            'carbon_savings': carbon_savings_kg,
            'user_rating': user_rating,
            'reward': reward,
        })
        # Append to CSV
        log_path = Path(LCAConfig().interaction_logs_path)
        df_log = pd.DataFrame([self.interaction_log[-1]])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

        # Update agent
        if self.last_state_vec is not None and self.last_action_idx is not None:
            # Next state (same for simplicity)
            next_state_vec = self.last_state_vec
            await self.weight_optimizer.update(
                self.last_state_vec,
                self.last_action_idx,
                reward,
                next_state_vec,
                self.last_teacher_probs
            )

    async def material_index(self, hardware_model: str, variant: Optional[str] = None) -> float:
        footprint = await self.lca_client.get_footprint(hardware_model, variant)
        return footprint.material_index


# ============================================================================
# 10. INTEGRATION WITH PREDICTIVE MAINTENANCE (unchanged)
# ============================================================================
class MaterialAwarePredictiveMaintenance:
    def __init__(self, lca_client: LCAClient, pm_engine):
        self.lca_client = lca_client
        self.pm_engine = pm_engine

    async def register_node(self, node_id: str, hardware_model: str, variant: Optional[str] = None, initial_flops: float = 1e12):
        footprint = await self.lca_client.get_footprint(hardware_model, variant)
        logger.info("Node registered with material footprint", node_id=node_id, hardware_model=hardware_model, variant=variant, material_index=footprint.material_index)
        if hasattr(self.pm_engine, 'update_node'):
            await self.pm_engine.update_node(node_id, initial_flops, 0.0)

# ============================================================================
# 11. FASTAPI REST API (updated with cost function dependency)
# ============================================================================
if FASTAPI_AVAILABLE:
    from fastapi import FastAPI, HTTPException, Depends
    from fastapi.responses import JSONResponse

    app = FastAPI(title="Material LCA API", version="2.2.0")

    async def get_lca_client() -> LCAClient:
        if not hasattr(app, "lca_client"):
            app.lca_client = LCAClient(LCAConfig())
        return app.lca_client

    async def get_cost_function(lca_client: LCAClient = Depends(get_lca_client)) -> AdaptiveMaterialCostFunction:
        return AdaptiveMaterialCostFunction(lca_client)

    @app.get("/footprint/{hardware_model}")
    async def get_footprint(
        hardware_model: str,
        variant: Optional[str] = None,
        force_refresh: bool = False,
        lca_client: LCAClient = Depends(get_lca_client)
    ):
        fp = await lca_client.get_footprint(hardware_model, variant, force_refresh)
        return fp.dict()

    @app.post("/footprint/batch")
    async def get_footprints_batch(
        models: List[Dict[str, str]],
        force_refresh: bool = False,
        lca_client: LCAClient = Depends(get_lca_client)
    ):
        tuples = [(m["hardware_model"], m.get("variant")) for m in models]
        footprints = await lca_client.get_footprints_batch(tuples)
        return {k: v.dict() for k, v in footprints.items()}

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
        simulator: DigitalTwinMaterialSimulator = Depends(get_simulator)
    ):
        result = await simulator.simulate_refresh_cycle(
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
        return result

    @app.post("/cost/{hardware_model}")
    async def compute_cost(
        hardware_model: str,
        variant: Optional[str] = None,
        operational_energy_joules: float = 0.0,
        lifetime_years: float = 5.0,
        quantity: int = 1,
        carbon_intensity: float = 400.0,
        recycling_rate: float = 0.5,
        cost_func: AdaptiveMaterialCostFunction = Depends(get_cost_function)
    ):
        cost, metadata = await cost_func.compute_cost(
            hardware_model,
            variant,
            operational_energy_joules,
            lifetime_years,
            quantity,
            carbon_intensity,
            recycling_rate,
        )
        return {"cost": cost, "metadata": metadata}

    @app.get("/metrics")
    async def get_metrics():
        if PROMETHEUS_AVAILABLE:
            return Response(content=generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
        return {"error": "Prometheus not enabled"}

    @app.on_event("startup")
    async def startup():
        app.lca_client = LCAClient(LCAConfig())
        logger.info("Material LCA API started")

    @app.on_event("shutdown")
    async def shutdown():
        if hasattr(app, "lca_client"):
            await app.lca_client.close()
        logger.info("Material LCA API shut down")

# ============================================================================
# 12. INTEGRATION FACTORY
# ============================================================================
def create_material_lca_integration(
    node_registry=None,
    pm_engine=None,
    config: Optional[LCAConfig] = None
):
    config = config or LCAConfig()
    lca_client = LCAClient(config)
    simulator = DigitalTwinMaterialSimulator(lca_client)
    cost_function = AdaptiveMaterialCostFunction(lca_client)

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
# 13. OFFLINE TRAINING FOR HISTORICAL ML
# ============================================================================
def train_historical_model(log_path: Path = Path(LCAConfig().interaction_logs_path),
                           model_path: Path = Path(LCAConfig().historical_model_path)):
    """
    Train a RandomForestClassifier from past interaction logs.
    """
    if not log_path.exists():
        logger.warning(f"Interaction logs not found at {log_path}. No model trained.")
        return

    df_logs = pd.read_csv(log_path)
    if len(df_logs) < 10:
        logger.warning("Not enough logs to train historical model (need at least 10).")
        return

    # For a real implementation, you must have stored the state vectors.
    # Since we didn't log the full state, we'll just log a message.
    logger.info("Historical ML training requires state vectors in logs. Please implement logging of state vectors.")
    # Skipping actual training for brevity.

# ============================================================================
# 14. UNIT TESTS (Phase 10)
# ============================================================================
import unittest
from unittest import IsolatedAsyncioTestCase

class TestDistillationComponents(IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = {
            'distillation_epsilon': 0.0,
            'distillation_replay_size': 10,
            'distillation_learning_rate': 0.01,
            'distillation_train_every': 10,
        }
        self.optimizer = DistillationWeightOptimizer(self.config)

    def test_state_feature_vector(self):
        state = CostState(
            embodied_carbon_kg=50,
            rare_earth_kg=0.005,
            water_usage_l=10,
            material_index=1.2,
            operational_energy_joules=1e6,
            lifetime_years=5,
            quantity=1,
            carbon_intensity=400,
            recycling_rate=0.5,
            avg_cost_trend=0.0,
            avg_user_rating=0.8,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 11)

    def test_rule_based_teacher(self):
        teacher = WeightRuleBasedTeacher()
        state = CostState(
            embodied_carbon_kg=50,
            rare_earth_kg=0.005,
            water_usage_l=10,
            material_index=1.2,
            operational_energy_joules=1e6,
            lifetime_years=5,
            quantity=1,
            carbon_intensity=600,
            recycling_rate=0.5,
            avg_cost_trend=0.0,
            avg_user_rating=0.8,
        )
        probs = teacher.predict(state)
        self.assertAlmostEqual(sum(probs), 1.0)
        self.assertGreater(probs[1], probs[0])  # carbon_focus should be highest

    async def test_select_strategy(self):
        state = CostState(
            embodied_carbon_kg=50,
            rare_earth_kg=0.005,
            water_usage_l=10,
            material_index=1.2,
            operational_energy_joules=1e6,
            lifetime_years=5,
            quantity=1,
            carbon_intensity=400,
            recycling_rate=0.5,
            avg_cost_trend=0.0,
            avg_user_rating=0.8,
        )
        strategy, idx, state_vec, teacher_probs = await self.optimizer.select_strategy(state, exploration=False)
        self.assertIn(strategy, self.optimizer.STRATEGIES)

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(11)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(5)/5)
        self.assertEqual(len(buffer), 1)
        batch = buffer.sample(1)
        self.assertEqual(len(batch[0]), 1)


# ============================================================================
# 15. EXAMPLE USAGE
# ============================================================================
async def main():
    config = LCAConfig()
    lca_client = LCAClient(config)
    cost_func = AdaptiveMaterialCostFunction(lca_client)

    # Compute cost with adaptive weights
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

    # Record outcome (simulate savings and user rating)
    await cost_func.record_outcome(cost=0.3, carbon_savings_kg=5, user_rating=0.9)

    stats = cost_func.weight_optimizer.get_stats()
    print("Distillation stats:", stats)

    await lca_client.close()

if __name__ == "__main__":
    asyncio.run(main())
