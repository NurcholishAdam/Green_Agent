# material_lca_v2_3_0.py
# Version: 2.4.0
"""
Enhanced Material Index Integration with Hardware Life‑Cycle Databases v2.4.0
======================================================================

Fetches accurate embodied carbon and rare‑earth content from public LCA databases
and integrates adaptive weight selection via Multi‑Teacher On‑Policy Distillation,
plus Multi‑Objective Evolutionary Optimization (NSGA‑II) for global weight refinement.

ENHANCEMENTS OVER v2.2.0:
- Added NSGA‑II optimizer to evolve continuous weight vectors (carbon, rare_earth, water, operational).
- Maintains a Pareto front of non‑dominated weight vectors.
- MODP‑based selection of best weight vector using dynamic objective weights.
- Background task for periodic MOEA evolution.
- Blending of MOEA global weights with online distillation strategy.
- New configuration parameters for MOEA.
- Persistence of evolved Pareto front.

NEW IN v2.4.0:
- Added LIMIT Graph manager for weight vector relationships.
- Added MODP solver wrapper for storing decision states/policies.
- Added RLHF trainer for human preference collection.
- Added MoE gating network to blend online distillation and offline MOEA weights.
- New configuration flags for each component.
- Integrated with central Storage (optional) for persistence.

All previous features (distillation, caching, circuit breaker, digital twin, etc.) retained.
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
import copy

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
# 1. CONFIGURATION (expanded with new component flags)
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

    # Distillation parameters
    distillation_epsilon: float = Field(0.1, ge=0, le=1)
    distillation_train_every: int = Field(10, ge=1)
    distillation_replay_size: int = Field(2000, ge=10)
    distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
    distill_weight: float = Field(0.7, ge=0, le=1)
    rl_weight: float = Field(0.3, ge=0, le=1)

    # MOEA parameters
    moea_enabled: bool = Field(True)
    moea_interval_seconds: int = Field(300, ge=60)
    moea_population_size: int = Field(20, ge=5)
    moea_generations: int = Field(5, ge=1)
    moea_mutation_rate: float = Field(0.2, ge=0.0, le=1.0)
    moea_crossover_rate: float = Field(0.8, ge=0.0, le=1.0)
    moea_tournament_size: int = Field(3, ge=2)
    moea_objective_weights: Optional[Dict[str, float]] = None
    moea_dynamic_weights: bool = True

    # NEW v2.4.0 flags
    enable_limit_graph: bool = Field(True, description="Enable LIMIT Graph manager")
    enable_modp: bool = Field(True, description="Enable MODP solver")
    enable_rlhf: bool = Field(True, description="Enable RLHF trainer")
    enable_moe: bool = Field(True, description="Enable MoE gating")
    moe_expert_count: int = Field(3, ge=2, description="Number of MoE experts")

    # Persistence paths
    q_weights_path: str = Field("./lca_q_weights.json")
    interaction_logs_path: str = Field("./lca_interactions.csv")
    historical_model_path: str = Field("./lca_historical_model.pkl")
    moea_pareto_path: str = Field("./lca_moea_pareto.json")
    limit_graph_path: str = Field("./lca_limit_graph.json", description="LIMIT Graph persistence")

    @validator('source')
    def source_must_be_valid(cls, v):
        allowed = {'mock', 'ecoinvent', 'openlca', 'cache_only'}
        if v not in allowed:
            raise ValueError(f'source must be one of {allowed}')
        return v

# ============================================================================
# 2. DATA STRUCTURES (unchanged)
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
# 3. CIRCUIT BREAKER (unchanged)
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
# 4. TASK MANAGER (unchanged)
# ============================================================================
class TaskManager:
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
# 5. RETRY DECORATOR (unchanged)
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
# 6. PROMETHEUS METRICS (unchanged)
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
# 7. LCA API CLIENT (unchanged)
# ============================================================================
class LCAClient:
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

    async def get_footprint(self, hardware_model: str, variant: Optional[str] = None, force_refresh: bool = False) -> MaterialFootprint:
        cache_key = f"{hardware_model}:{variant}" if variant else hardware_model

        async with self._lock:
            if not force_refresh and cache_key in self.cache:
                cached = self.cache[cache_key]
                age = (datetime.now() - cached.timestamp).total_seconds()
                if age < self.config.cache_ttl:
                    CACHE_HITS.inc()
                    return cached
                else:
                    CACHE_MISSES.inc()

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
# 9. DISTILLATION COMPONENTS (unchanged)
# ============================================================================
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
        pass

    @abstractmethod
    def confidence(self, state: CostState) -> float:
        pass


class WeightRuleBasedTeacher(Teacher):
    STRATEGIES = ['balanced', 'carbon_focus', 'rare_earth_focus', 'operational_focus', 'water_focus']

    def predict(self, state: CostState) -> np.ndarray:
        probs = np.ones(5) * 0.1
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
        return probs / probs.sum()

    def confidence(self, state: CostState) -> float:
        if state.carbon_intensity > 500:
            return 0.6
        return 0.4


class WeightHistoricalMLTeacher(Teacher):
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
    def __init__(self, lr: float = 0.1):
        self.lr = lr
        self.weights = np.zeros((11, 5))
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
# NEW: Multi‑Objective Weight Optimizer (NSGA‑II) (existing, retained)
# ============================================================================
@dataclass
class MOPDWeightVector:
    vector_id: str
    weights: Dict[str, float]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'vector_id': self.vector_id,
            'weights': self.weights,
            'objectives': self.objectives,
            'scalarised_score': self.scalarised_score,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDWeightVector':
        return cls(**data)


class NSGAIIWeightOptimizer:
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
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {
            'carbon': 0.35,
            'rare_earth': 0.25,
            'water': 0.2,
            'operational': 0.2,
        }
        self.dynamic_weights = dynamic_weights

        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front: List[MOPDWeightVector] = []
        self._eval_cache: Dict[Tuple[float, ...], Dict[str, float]] = {}

    def _random_individual(self) -> Dict[str, float]:
        keys = ['carbon', 'rare_earth', 'water', 'operational']
        weights = {k: random.random() for k in keys}
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    def _crossover(self, p1: Dict, p2: Dict) -> Dict:
        child = {}
        for key in p1:
            if random.random() < 0.5:
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1 / (20 + 1))
                else:
                    beta = (1 / (2 * (1 - u))) ** (1 / (20 + 1))
                child[key] = max(0.0, min(1.0, 0.5 * ((1 + beta) * p1[key] + (1 - beta) * p2[key])))
            else:
                child[key] = p1[key] if random.random() < 0.5 else p2[key]
        total = sum(child.values())
        if total > 0:
            child = {k: v / total for k, v in child.items()}
        return child

    def _mutate(self, ind: Dict) -> Dict:
        mutant = ind.copy()
        for key in mutant:
            if random.random() < self.mutation_rate:
                u = random.random()
                if u < 0.5:
                    delta = (2 * u) ** (1 / (20 + 1)) - 1
                else:
                    delta = 1 - (2 * (1 - u)) ** (1 / (20 + 1))
                mutant[key] = mutant[key] + delta
                mutant[key] = max(0.0, min(1.0, mutant[key]))
        total = sum(mutant.values())
        if total > 0:
            mutant = {k: v / total for k, v in mutant.items()}
        return mutant

    def _fast_non_dominated_sort(self, points: List[MOPDWeightVector]) -> List[List[MOPDWeightVector]]:
        fronts = []
        domination_count = {id(p): 0 for p in points}
        dominated_solutions = {id(p): [] for p in points}

        for i, p in enumerate(points):
            p_obj = p.objectives
            for j, q in enumerate(points):
                if i == j:
                    continue
                q_obj = q.objectives
                if all(p_obj[k] >= q_obj[k] for k in p_obj) and any(p_obj[k] > q_obj[k] for k in p_obj):
                    dominated_solutions[id(p)].append(q)
                elif all(q_obj[k] >= p_obj[k] for k in q_obj) and any(q_obj[k] > p_obj[k] for k in q_obj):
                    domination_count[id(p)] += 1

            if domination_count[id(p)] == 0:
                if not fronts:
                    fronts.append([])
                fronts[0].append(p)

        i = 0
        while i < len(fronts):
            next_front = []
            for p in fronts[i]:
                for q in dominated_solutions[id(p)]:
                    domination_count[id(q)] -= 1
                    if domination_count[id(q)] == 0:
                        next_front.append(q)
            if next_front:
                fronts.append(next_front)
            i += 1
        return fronts

    def _crowding_distance(self, front: List[MOPDWeightVector]) -> Dict[int, float]:
        if not front:
            return {}
        distances = {id(p): 0.0 for p in front}
        objective_keys = list(front[0].objectives.keys())
        for obj in objective_keys:
            sorted_front = sorted(front, key=lambda x: x.objectives[obj])
            distances[id(sorted_front[0])] = float('inf')
            distances[id(sorted_front[-1])] = float('inf')
            obj_min = sorted_front[0].objectives[obj]
            obj_max = sorted_front[-1].objectives[obj]
            if obj_max == obj_min:
                continue
            for i in range(1, len(sorted_front) - 1):
                distances[id(sorted_front[i])] += (sorted_front[i+1].objectives[obj] - sorted_front[i-1].objectives[obj]) / (obj_max - obj_min)
        return distances

    def _tournament_selection(self, population: List[Dict], fronts: List[List[MOPDWeightVector]],
                              crowding: Dict[int, float]) -> Dict:
        candidates = random.sample(population, self.tournament_size)
        ind_to_point = {}
        for ind, point in zip(population, self._all_points):
            ind_to_point[id(ind)] = point

        best = candidates[0]
        best_rank = float('inf')
        best_crowding = -float('inf')
        for cand in candidates:
            point = ind_to_point.get(id(cand))
            if not point:
                continue
            rank = len(fronts)
            for fi, front in enumerate(fronts):
                if point in front:
                    rank = fi
                    break
            cd = crowding.get(id(point), 0)
            if rank < best_rank or (rank == best_rank and cd > best_crowding):
                best = cand
                best_rank = rank
                best_crowding = cd
        return best

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = self.objective_weights.copy()
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        obj_keys = list(weights.keys())
        avg = {k: np.mean([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        max_val = {k: np.max([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        for k in obj_keys:
            if max_val[k] > 0 and avg[k] < 0.5 * max_val[k]:
                weights[k] = min(0.6, weights.get(k, 0.0) * 1.5)
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    def _select_best_from_pareto(self, pareto: List[MOPDWeightVector], weights: Dict[str, float]) -> Optional[MOPDWeightVector]:
        if not pareto:
            return None
        obj_keys = list(weights.keys())
        max_vals = {k: max(p.objectives[k] for p in pareto) for k in obj_keys}
        min_vals = {k: min(p.objectives[k] for p in pareto) for k in obj_keys}
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in obj_keys}

        best = None
        best_score = -float('inf')
        for p in pareto:
            score = 0.0
            for k in obj_keys:
                val = p.objectives[k]
                norm = (val - min_vals[k]) / ranges[k] if ranges[k] > 0 else 1.0
                score += weights.get(k, 0.0) * norm
            p.scalarised_score = score
            if score > best_score:
                best_score = score
                best = p
        return best

    async def evolve(self) -> List[MOPDWeightVector]:
        population = [self._random_individual() for _ in range(self.population_size)]
        points = []
        eval_tasks = [self.evaluate_func(ind) for ind in population]
        eval_results = await asyncio.gather(*eval_tasks)
        for ind, obj in zip(population, eval_results):
            point = MOPDWeightVector(vector_id=str(uuid.uuid4()), weights=ind, objectives=obj)
            points.append(point)
            self._eval_cache[tuple(sorted(ind.items()))] = obj

        self._all_points = points
        for gen in range(self.generations):
            fronts = self._fast_non_dominated_sort(points)
            crowding = {}
            for front in fronts:
                front_crowding = self._crowding_distance(front)
                crowding.update(front_crowding)

            offspring = []
            while len(offspring) < self.population_size:
                parent1 = self._tournament_selection(population, fronts, crowding)
                parent2 = self._tournament_selection(population, fronts, crowding)
                if random.random() < self.crossover_rate:
                    child = self._crossover(parent1, parent2)
                else:
                    child = copy.deepcopy(parent1)
                child = self._mutate(child)
                offspring.append(child)

            child_tasks = [self.evaluate_func(ind) for ind in offspring]
            child_results = await asyncio.gather(*child_tasks)
            child_points = []
            for ind, obj in zip(offspring, child_results):
                point = MOPDWeightVector(vector_id=str(uuid.uuid4()), weights=ind, objectives=obj)
                child_points.append(point)
                self._eval_cache[tuple(sorted(ind.items()))] = obj

            combined_inds = population + offspring
            combined_points = points + child_points
            unique_pairs = {}
            for ind, p in zip(combined_inds, combined_points):
                key = tuple(sorted(ind.items()))
                unique_pairs[key] = (ind, p)
            population = [v[0] for v in unique_pairs.values()]
            points = [v[1] for v in unique_pairs.values()]
            self._all_points = points

            fronts = self._fast_non_dominated_sort(points)
            new_population = []
            new_points = []
            for front in fronts:
                if len(new_population) + len(front) <= self.population_size:
                    for p in front:
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_population.append(ind)
                                new_points.append(p)
                                break
                else:
                    crowding = self._crowding_distance(front)
                    sorted_front = sorted(front, key=lambda x: crowding.get(id(x), 0), reverse=True)
                    for p in sorted_front:
                        if len(new_population) >= self.population_size:
                            break
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_population.append(ind)
                                new_points.append(p)
                                break
            population = new_population[:self.population_size]
            points = new_points[:self.population_size]
            self._all_points = points

            fronts = self._fast_non_dominated_sort(points)
            if fronts:
                self.pareto_front = fronts[0]
            logger.info(f"Generation {gen+1}/{self.generations}: Pareto front size={len(self.pareto_front)}")

        weights = self._compute_dynamic_weights()
        best = self._select_best_from_pareto(self.pareto_front, weights)
        if best:
            self.best_individual = best.weights
            self.best_fitness = best.scalarised_score
        return self.pareto_front


# ============================================================================
# NEW v2.4.0: LIMIT Graph Manager
# ============================================================================
class LimitGraphManager:
    """
    Manages a graph of weight vector relationships for LIMIT.
    Nodes are weight vectors or updates, edges represent dependencies or improvements.
    """
    def __init__(self, storage: Optional[Any] = None):
        self.storage = storage
        self.graphs = {}

    def create_graph(self, graph_id: str, description: str, configuration: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_metadata'):
            self.storage.save_limit_graph_metadata(graph_id, description, configuration)
        else:
            self.graphs[graph_id] = {'description': description, 'configuration': configuration, 'nodes': {}, 'edges': {}}

    def add_node(self, graph_id: str, node_id: str, node_type: Optional[str], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_node'):
            self.storage.save_limit_graph_node(node_id, graph_id, node_type, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['nodes'][node_id] = {'node_type': node_type, 'attributes': attributes}

    def add_edge(self, graph_id: str, edge_id: str, source: str, target: str,
                 weight: Optional[float], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_edge'):
            self.storage.save_limit_graph_edge(edge_id, graph_id, source, target, weight, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['edges'][edge_id] = {'source': source, 'target': target, 'weight': weight, 'attributes': attributes}

    def get_nodes(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_nodes'):
            return self.storage.get_limit_graph_nodes(graph_id)
        return list(self.graphs.get(graph_id, {}).get('nodes', {}).values())

    def get_edges(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_edges'):
            return self.storage.get_limit_graph_edges(graph_id)
        return list(self.graphs.get(graph_id, {}).get('edges', {}).values())

    def get_metadata(self, graph_id: str) -> Optional[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_metadata'):
            return self.storage.get_limit_graph_metadata(graph_id)
        return self.graphs.get(graph_id, {})


# ============================================================================
# NEW v2.4.0: MODP Optimizer (wrapper)
# ============================================================================
class MODPOptimizer:
    """
    Multi‑Objective Dynamic Programming solver that stores decision states/policies.
    Used for persisting Pareto front points and selected weight vectors.
    """
    def __init__(self, storage: Optional[Any] = None):
        self.storage = storage
        self.states = {}

    def add_state(self, state_id: str, problem_id: str, state_attributes: Dict[str, Any],
                  objective_values: Dict[str, float], stage: int) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_state'):
            self.storage.save_modp_state(state_id, problem_id, state_attributes, objective_values, stage)
        else:
            if problem_id not in self.states:
                self.states[problem_id] = []
            self.states[problem_id].append({
                'state_id': state_id, 'state_attributes': state_attributes,
                'objective_values': objective_values, 'stage': stage
            })

    def add_policy(self, policy_id: str, problem_id: str, state_id: str,
                   action: str, expected_objectives: Dict[str, float]) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_policy'):
            self.storage.save_modp_policy(policy_id, problem_id, state_id, action, expected_objectives)

    def get_states(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_states'):
            return self.storage.get_modp_states(problem_id)
        return self.states.get(problem_id, [])

    def get_policies(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_policies'):
            return self.storage.get_modp_policies(problem_id)
        return []


# ============================================================================
# NEW v2.4.0: RLHF Trainer
# ============================================================================
class RLHFTrainer:
    """
    Collects human preference pairs for weight vector choices.
    """
    def __init__(self, storage: Optional[Any] = None):
        self.storage = storage
        self.pairs = []

    def record_pair(self, pair_id: str, prompt: str, chosen: str, rejected: str,
                    reward_diff: float, metadata: Optional[Dict] = None) -> None:
        if self.storage and hasattr(self.storage, 'save_preference_pair'):
            self.storage.save_preference_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)
        else:
            self.pairs.append({
                'pair_id': pair_id, 'prompt': prompt, 'chosen': chosen,
                'rejected': rejected, 'reward_diff': reward_diff, 'metadata': metadata
            })

    def get_pairs(self, limit: int = 100) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_preference_pairs'):
            return self.storage.get_preference_pairs(limit)
        return self.pairs[-limit:]

    def train_reward_model(self):
        pairs = self.get_pairs()
        if len(pairs) < 5:
            logger.info("Not enough preference pairs for RLHF training.")
            return
        logger.info(f"Training reward model on {len(pairs)} preference pairs...")


# ============================================================================
# NEW v2.4.0: MoE Gating Network for Weight Blending
# ============================================================================
class MoEGatingNetwork:
    """
    Mixture-of-Experts gating that blends online distillation and offline MOEA weights.
    The gating network learns to select the best source for the current context.
    """
    def __init__(self, storage: Optional[Any] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.expert_names = self.config.get('expert_names', ['online', 'offline', 'rule_based'])
        self.num_experts = len(self.expert_names)
        # Gating input: 5 features representing normalized metrics
        self.gating_weights = np.random.randn(self.num_experts, 5)
        self._training_samples = []

    def _encode_state(self, metrics: Dict[str, float]) -> np.ndarray:
        features = [
            metrics.get('carbon', 0.5),
            metrics.get('rare_earth', 0.5),
            metrics.get('water', 0.5),
            metrics.get('operational', 0.5),
            metrics.get('material_index', 0.5),
        ]
        return np.array(features, dtype=np.float32)

    async def select_expert(self, metrics: Dict[str, float]) -> Tuple[str, np.ndarray]:
        x = self._encode_state(metrics)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        expert_idx = np.argmax(probs)
        selected = self.expert_names[expert_idx]
        if self.storage and hasattr(self.storage, 'log_routing_decision'):
            sample_id = hashlib.sha256(str(metrics).encode()).hexdigest()[:16]
            self.storage.log_routing_decision(str(uuid.uuid4()), sample_id, selected, float(probs[expert_idx]))
        return selected, probs

    async def add_training_sample(self, metrics: Dict[str, float], selected_expert: str, reward: float):
        x = self._encode_state(metrics)
        expert_idx = self.expert_names.index(selected_expert)
        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        grad = (probs - target)[:, None] * x[None, :]
        self.gating_weights -= 0.1 * grad


# ============================================================================
# ADAPTIVE COST FUNCTION (Enhanced with new components)
# ============================================================================
class AdaptiveMaterialCostFunction:
    def __init__(self, lca_client: LCAClient, config: Optional[LCAConfig] = None, storage: Optional[Any] = None):
        self.lca_client = lca_client
        self.config = config or LCAConfig()
        self.storage = storage
        self.weight_optimizer = DistillationWeightOptimizer({
            'distillation_epsilon': self.config.distillation_epsilon,
            'distillation_train_every': self.config.distillation_train_every,
            'distillation_replay_size': self.config.distillation_replay_size,
            'distillation_learning_rate': self.config.distillation_learning_rate,
        })
        self.interaction_log: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None

        # MOEA globals
        self.moea_optimizer: Optional[NSGAIIWeightOptimizer] = None
        self.global_best_weights: Optional[Dict[str, float]] = None
        self.pareto_front: List[MOPDWeightVector] = []
        self._moea_task: Optional[asyncio.Task] = None

        # NEW v2.4.0 components
        self.limit_graph_manager = LimitGraphManager(storage) if getattr(self.config, 'enable_limit_graph', True) else None
        self.modp_solver = MODPOptimizer(storage) if getattr(self.config, 'enable_modp', True) else None
        self.rlhf_trainer = RLHFTrainer(storage) if getattr(self.config, 'enable_rlhf', True) else None
        self.moe_gating = MoEGatingNetwork(
            storage,
            {'expert_names': ['online', 'offline', 'rule_based']}
        ) if getattr(self.config, 'enable_moe', True) else None

        # Initialize LIMIT graph if enabled
        if self.limit_graph_manager:
            if not self.limit_graph_manager.get_metadata("weight_vectors"):
                self.limit_graph_manager.create_graph("weight_vectors", "Weight Vector Relationships", {})
            # Add source nodes
            for src in ['online', 'offline', 'rule_based']:
                self.limit_graph_manager.add_node(
                    "weight_vectors",
                    f"source_{src}",
                    src,
                    {"type": "source"}
                )

        # Start MOEA background task if enabled
        if getattr(self.config, 'moea_enabled', True):
            self._moea_task = asyncio.create_task(self._moea_loop())

    async def _moea_loop(self):
        interval = getattr(self.config, 'moea_interval_seconds', 300)
        while True:
            try:
                await asyncio.sleep(interval)
                await self.run_moea_update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"MOEA loop error: {e}")
                await asyncio.sleep(60)

    async def run_moea_update(self) -> List[MOPDWeightVector]:
        if not hasattr(self, 'lca_client'):
            return []

        async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
            # Use a representative hardware model for evaluation.
            footprint = await self.lca_client.get_footprint("NVIDIA A100", None)
            carbon_benefit = 1.0 - min(footprint.embodied_carbon_kg / 100.0, 1.0)
            rare_earth_benefit = 1.0 - min(footprint.rare_earth_kg / 0.01, 1.0)
            water_benefit = 1.0 - min(footprint.water_usage_l / 50.0, 1.0)
            operational_benefit = 1.0 - min(0.5 / 10.0, 1.0)

            return {
                'carbon': carbon_benefit,
                'rare_earth': rare_earth_benefit,
                'water': water_benefit,
                'operational': operational_benefit,
            }

        self.moea_optimizer = NSGAIIWeightOptimizer(
            evaluate_func=evaluate,
            population_size=getattr(self.config, 'moea_population_size', 20),
            generations=getattr(self.config, 'moea_generations', 5),
            mutation_rate=getattr(self.config, 'moea_mutation_rate', 0.2),
            crossover_rate=getattr(self.config, 'moea_crossover_rate', 0.8),
            tournament_size=getattr(self.config, 'moea_tournament_size', 3),
            objective_weights=getattr(self.config, 'moea_objective_weights', None),
            dynamic_weights=getattr(self.config, 'moea_dynamic_weights', True),
        )

        pareto = await self.moea_optimizer.evolve()
        self.pareto_front = pareto
        if pareto:
            weights = self.moea_optimizer._compute_dynamic_weights()
            best = self.moea_optimizer._select_best_from_pareto(pareto, weights)
            if best:
                self.global_best_weights = best.weights
                logger.info(f"MOEA selected best weights: {best.weights}")

                # Store in MODP and LIMIT graph
                if self.modp_solver:
                    self.modp_solver.add_state(
                        state_id=f"moea_best_{best.vector_id}",
                        problem_id="material_weight_optimization",
                        state_attributes={'weights': best.weights},
                        objective_values=best.objectives,
                        stage=1
                    )
                if self.limit_graph_manager:
                    self.limit_graph_manager.add_node(
                        "weight_vectors",
                        f"vector_{best.vector_id}",
                        "best_weight_vector",
                        {'weights': best.weights, 'objectives': best.objectives}
                    )
        return pareto

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

        # Strategy selection via distillation or MoE
        if self.moe_gating:
            # Build metrics for gating
            metrics = {
                'carbon': footprint.embodied_carbon_kg,
                'rare_earth': footprint.rare_earth_kg,
                'water': footprint.water_usage_l,
                'operational': operational_energy_joules,
                'material_index': footprint.material_index,
            }
            selected_expert, _ = await self.moe_gating.select_expert(metrics)
            # Map expert to weights
            if selected_expert == 'offline' and self.global_best_weights:
                weights = self.global_best_weights
            else:
                # Fallback to rule-based default or balanced
                weights = self._strategy_to_weights('balanced')
            strategy = selected_expert
            state_vec = state.to_feature_vector()
            teacher_probs = np.ones(5) / 5
        else:
            strategy, action_idx, state_vec, teacher_probs = await self.weight_optimizer.select_strategy(state, exploration=True)
            weights = self._strategy_to_weights(strategy)
            self.last_state_vec = state_vec
            self.last_action_idx = action_idx
            self.last_teacher_probs = teacher_probs

        # Blend with global best if available (unless already offline)
        if self.global_best_weights is not None and not (self.moe_gating and selected_expert == 'offline'):
            for k in weights:
                weights[k] = 0.8 * self.global_best_weights[k] + 0.2 * weights[k]

        carbon_score = min(footprint.embodied_carbon_kg / 100.0, 1.0)
        rare_earth_score = min(footprint.rare_earth_kg / 0.01, 1.0)
        water_score = min(footprint.water_usage_l / 50.0, 1.0)
        operational_carbon = operational_energy_joules / 3.6e6 * (carbon_intensity / 1000)
        operational_score = min(operational_carbon / 10.0, 1.0)

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
            "moea_blended": self.global_best_weights is not None,
            "moe_used": self.moe_gating is not None,
        }

        # Optional: update MoE with outcome later (called by record_outcome)
        if self.moe_gating:
            self._last_moe_metrics = metrics
            self._last_selected_expert = selected_expert

        return total_cost, metadata

    def _strategy_to_weights(self, strategy: str) -> Dict[str, float]:
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
        if user_rating is not None:
            reward = 0.5 * min(1.0, carbon_savings_kg / 10.0) + 0.3 * user_rating + 0.2 * (1 - cost)
        else:
            reward = 0.5 * min(1.0, carbon_savings_kg / 10.0) + 0.2 * (1 - cost)
        reward = max(0.0, min(1.0, reward))

        self.interaction_log.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'cost': cost,
            'carbon_savings': carbon_savings_kg,
            'user_rating': user_rating,
            'reward': reward,
        })
        log_path = Path(self.config.interaction_logs_path)
        df_log = pd.DataFrame([self.interaction_log[-1]])
        if log_path.exists():
            df_log.to_csv(log_path, mode='a', header=False, index=False)
        else:
            df_log.to_csv(log_path, index=False)

        # Update distillation if we used it
        if self.last_state_vec is not None and self.last_action_idx is not None:
            next_state_vec = self.last_state_vec
            await self.weight_optimizer.update(
                self.last_state_vec,
                self.last_action_idx,
                reward,
                next_state_vec,
                self.last_teacher_probs
            )

        # Update MoE gating if used
        if self.moe_gating and hasattr(self, '_last_moe_metrics') and hasattr(self, '_last_selected_expert'):
            await self.moe_gating.add_training_sample(
                self._last_moe_metrics,
                self._last_selected_expert,
                reward
            )

        # RLHF: occasionally record preference pair
        if self.rlhf_trainer and random.random() < 0.05:
            chosen_strategy = getattr(self, '_last_selected_expert', 'online')
            rejected_strategy = random.choice(['online', 'offline', 'rule_based'])
            if rejected_strategy != chosen_strategy:
                self.rlhf_trainer.record_pair(
                    pair_id=str(uuid.uuid4()),
                    prompt="Which weight source produced better cost?",
                    chosen=chosen_strategy,
                    rejected=rejected_strategy,
                    reward_diff=reward,
                    metadata={"cost": cost, "carbon_savings": carbon_savings_kg}
                )

        # MODP: record state and policy
        if self.modp_solver:
            self.modp_solver.add_state(
                state_id=str(uuid.uuid4()),
                problem_id="material_cost",
                state_attributes={'cost': cost, 'carbon_savings': carbon_savings_kg, 'user_rating': user_rating},
                objective_values={'carbon': carbon_savings_kg, 'cost': 1.0 - cost, 'satisfaction': reward},
                stage=0
            )

        # LIMIT Graph: add node for this outcome
        if self.limit_graph_manager:
            self.limit_graph_manager.add_node(
                "weight_vectors",
                f"outcome_{uuid.uuid4()}",
                "cost_outcome",
                {'cost': cost, 'carbon_savings': carbon_savings_kg, 'reward': reward}
            )

    async def material_index(self, hardware_model: str, variant: Optional[str] = None) -> float:
        footprint = await self.lca_client.get_footprint(hardware_model, variant)
        return footprint.material_index

    # ---------- New public methods for enhancements ----------
    async def get_limit_graph(self, graph_id: str = "weight_vectors") -> Dict:
        if self.limit_graph_manager:
            return {
                'metadata': self.limit_graph_manager.get_metadata(graph_id),
                'nodes': self.limit_graph_manager.get_nodes(graph_id),
                'edges': self.limit_graph_manager.get_edges(graph_id),
            }
        return {}

    async def get_moe_experts(self) -> List[str]:
        if self.moe_gating:
            return self.moe_gating.expert_names
        return []

    async def get_rlhf_pairs(self, limit: int = 100) -> List[Dict]:
        if self.rlhf_trainer:
            return self.rlhf_trainer.get_pairs(limit)
        return []

    async def record_rlhf_pair(self, pair_id, prompt, chosen, rejected, reward_diff, metadata=None):
        if self.rlhf_trainer:
            self.rlhf_trainer.record_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)


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
# 11. FASTAPI REST API (now includes cost function with new components)
# ============================================================================
if FASTAPI_AVAILABLE:
    from fastapi import FastAPI, HTTPException, Depends
    from fastapi.responses import JSONResponse

    app = FastAPI(title="Material LCA API", version="2.4.0")

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
        simulator: DigitalTwinMaterialSimulator = Depends(lambda: DigitalTwinMaterialSimulator(app.lca_client))
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
        app.cost_function = AdaptiveMaterialCostFunction(app.lca_client)
        logger.info("Material LCA API started")

    @app.on_event("shutdown")
    async def shutdown():
        if hasattr(app, "lca_client"):
            await app.lca_client.close()
        if hasattr(app, "cost_function") and app.cost_function._moea_task:
            app.cost_function._moea_task.cancel()
            await asyncio.gather(app.cost_function._moea_task, return_exceptions=True)
        logger.info("Material LCA API shut down")

# ============================================================================
# 12. INTEGRATION FACTORY (now includes storage)
# ============================================================================
def create_material_lca_integration(
    node_registry=None,
    pm_engine=None,
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
# 13. OFFLINE TRAINING FOR HISTORICAL ML (unchanged)
# ============================================================================
def train_historical_model(log_path: Path = Path(LCAConfig().interaction_logs_path),
                           model_path: Path = Path(LCAConfig().historical_model_path)):
    if not log_path.exists():
        logger.warning(f"Interaction logs not found at {log_path}. No model trained.")
        return

    df_logs = pd.read_csv(log_path)
    if len(df_logs) < 10:
        logger.warning("Not enough logs to train historical model (need at least 10).")
        return

    logger.info("Historical ML training requires state vectors in logs. Please implement logging of state vectors.")

# ============================================================================
# 14. UNIT TESTS (Phase 10) - extended
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
        self.assertGreater(probs[1], probs[0])

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


class TestMOEA(IsolatedAsyncioTestCase):
    async def test_moea_evolve(self):
        async def dummy_evaluate(weights):
            return {'carbon': random.random(), 'rare_earth': random.random(),
                    'water': random.random(), 'operational': random.random()}
        opt = NSGAIIWeightOptimizer(evaluate_func=dummy_evaluate,
                                    population_size=10, generations=2)
        pareto = await opt.evolve()
        self.assertGreater(len(pareto), 0)


class TestNewComponents(IsolatedAsyncioTestCase):
    async def test_limit_graph(self):
        mgr = LimitGraphManager()
        graph_id = "test_graph"
        mgr.create_graph(graph_id, "Test", {})
        mgr.add_node(graph_id, "n1", "type1", {"key": "val"})
        nodes = mgr.get_nodes(graph_id)
        self.assertEqual(len(nodes), 1)

    async def test_modp(self):
        opt = MODPOptimizer()
        opt.add_state("s1", "p1", {"a":1}, {"obj":0.5}, 0)
        states = opt.get_states("p1")
        self.assertEqual(len(states), 1)

    async def test_rlhf(self):
        trainer = RLHFTrainer()
        trainer.record_pair("p1", "prompt", "chosen", "rejected", 0.5)
        pairs = trainer.get_pairs()
        self.assertEqual(len(pairs), 1)

    async def test_moe(self):
        gating = MoEGatingNetwork(config={"expert_names": ["online", "offline"]})
        metrics = {'carbon': 0.5, 'rare_earth': 0.5, 'water': 0.5, 'operational': 0.5, 'material_index': 0.5}
        selected, probs = await gating.select_expert(metrics)
        self.assertIn(selected, ["online", "offline"])


# ============================================================================
# 15. EXAMPLE USAGE (unchanged, but now includes new components)
# ============================================================================
async def main():
    config = LCAConfig(moea_enabled=True, moea_interval_seconds=10,
                       enable_limit_graph=True, enable_modp=True,
                       enable_rlhf=True, enable_moe=True)
    lca_client = LCAClient(config)
    cost_func = AdaptiveMaterialCostFunction(lca_client, config)

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

    # Wait for MOEA to run at least once (or trigger manually)
    await cost_func.run_moea_update()
    print("MOEA best weights:", cost_func.global_best_weights)

    # Record outcome
    await cost_func.record_outcome(cost=0.3, carbon_savings_kg=5, user_rating=0.9)

    # Access new components
    limit_graph = await cost_func.get_limit_graph()
    print("Limit graph nodes:", len(limit_graph.get('nodes', [])))
    print("MoE experts:", await cost_func.get_moe_experts())

    stats = cost_func.weight_optimizer.get_stats()
    print("Distillation stats:", stats)

    await lca_client.close()

if __name__ == "__main__":
    asyncio.run(main())
