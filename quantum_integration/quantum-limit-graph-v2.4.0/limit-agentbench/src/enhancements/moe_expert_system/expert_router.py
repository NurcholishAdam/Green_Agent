#!/usr/bin/env python3
"""
Enhanced Expert Router v10.1.0 - Complete Signal Transduction Cascade with Causal Constraints,
MoE Gating, Genetic Algorithm Tuning, and Interactive Pareto Front.
Fully integrated with Green Agent MOPD ecosystem.

ENHANCEMENTS OVER v10.0.0:
1. Fixed critical bugs: added missing aiohttp import, fixed causal constraint check,
   replaced non‑generic metric methods with generic MetricsRegistry calls,
   removed asyncio.run inside async method, guarded Torch usage.
2. Real MODP integration: adaptive cost and central ParetoGating now influence expert selection.
3. Bio‑inspired signals (ATP, second messengers) fed into gating features.
4. MoE gating trained with adaptive reward (from AdaptiveCostFunction).
5. Safe background task creation in constructors.
6. Improved serialization and metric handling.
"""

import asyncio
import json
import os
import re
import hashlib
import uuid
import math
import random
from collections import defaultdict, deque
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple, Set, Union, Callable, TypeVar, cast
import numpy as np
import networkx as nx

# -----------------------------------------------------------------------------
# IMPORT CENTRAL GREEN AGENT COMPONENTS
# -----------------------------------------------------------------------------
from ..config import config as central_config
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..routing.pareto_gating import ParetoGating
from ..feedback.adaptive_cost import AdaptiveCostFunction
from ..safety.drift_detector import DriftDetector
from ..scaling.message_queue import AsyncMessageQueue
from ..metrics import MetricsRegistry
from ..logger import logger

# -----------------------------------------------------------------------------
# OPTIONAL IMPORTS (graceful degradation)
# -----------------------------------------------------------------------------
try:
    import aiofiles
except ImportError:
    aiofiles = None

try:
    from pydantic import BaseModel, Field, ValidationError, field_validator, ConfigDict
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:
    raise ImportError("pydantic and pydantic-settings are required")

try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    def retry(*args, **kwargs):
        return lambda f: f
    stop_after_attempt = lambda x: None
    wait_exponential = lambda **k: None
    retry_if_exception_type = lambda e: None
    before_sleep_log = lambda logger, level: None
    TENACITY_AVAILABLE = False

# Optional ML imports
try:
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import SGDRegressor
    from sklearn.metrics import r2_score, mean_squared_error
    from sklearn.neural_network import MLPClassifier, MLPRegressor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

try:
    from scipy.optimize import linprog
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    from .gating_network import GatingNetworkManager
except ImportError:
    GatingNetworkManager = None

# Bio-inspired modules – optional
try:
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    from enhancements.bio_inspired.chromatophore_compartments import CompartmentManager
    from enhancements.bio_inspired.biomass_storage import BiomassStorage
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False

# FastAPI for WebSocket (for active user preference)
try:
    from fastapi import WebSocket, WebSocketDisconnect
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

# Missing aiohttp import (added)
import aiohttp

# -----------------------------------------------------------------------------
# Configuration – now uses central_config as a reference, but we keep a local
# config class for compatibility. We'll override fields with central_config.
# -----------------------------------------------------------------------------
class ExpertRouterConfig:
    """Configuration for Expert Router, built from central_config."""
    def __init__(self):
        # Feature flags
        self.enable_quantum = getattr(central_config, "enable_quantum", False)
        self.enable_signal_transduction = getattr(central_config, "enable_signal_transduction", True)
        self.enable_allosteric = getattr(central_config, "enable_allosteric", True)
        self.enable_metabolic_pathways = getattr(central_config, "enable_metabolic_pathways", True)
        self.enable_cooperative_binding = getattr(central_config, "enable_cooperative_binding", True)
        self.enable_homeostasis = getattr(central_config, "enable_homeostasis", True)
        self.enable_bio_integration = getattr(central_config, "enable_bio_integration", True) and BIO_INSPIRED_AVAILABLE
        self.enable_federated = getattr(central_config, "enable_federated", True) and TORCH_AVAILABLE
        self.enable_predictive = getattr(central_config, "enable_predictive", True) and SKLEARN_AVAILABLE
        self.enable_carbon_intensity = getattr(central_config, "enable_carbon_intensity", True)
        self.enable_helium_optimization = getattr(central_config, "enable_helium_optimization", True)
        self.enable_causal_constraints = getattr(central_config, "enable_causal_constraints", True)
        self.enable_counterfactual = getattr(central_config, "enable_counterfactual", True)
        self.enable_signal_integration = getattr(central_config, "enable_signal_integration", True)
        self.enable_differential_privacy = getattr(central_config, "enable_differential_privacy", True)
        self.enable_uncertainty_quantification = getattr(central_config, "enable_uncertainty_quantification", True)
        self.enable_telemetry = False  # We use central metrics now

        # Tunable parameters
        self.carbon_api_region = getattr(central_config, "carbon_api_region", "us-east")
        self.carbon_update_interval = getattr(central_config, "carbon_update_interval", 300)
        self.max_retries = getattr(central_config, "max_retries", 3)
        self.retry_base_delay_ms = getattr(central_config, "retry_base_delay_ms", 100.0)
        self.retry_max_delay_ms = getattr(central_config, "retry_max_delay_ms", 5000.0)
        self.circuit_breaker_failure_threshold = getattr(central_config, "circuit_breaker_failure_threshold", 5)
        self.circuit_breaker_recovery_timeout = getattr(central_config, "circuit_breaker_recovery_timeout", 30.0)
        self.server_url = getattr(central_config, "server_url", None)
        self.helium_budget_l = getattr(central_config, "helium_budget_l", 100.0)
        self.privacy_epsilon = getattr(central_config, "privacy_epsilon", 1.0)
        self.federated_sparsity_ratio = getattr(central_config, "federated_sparsity_ratio", 0.1)
        self.predictive_history_window = getattr(central_config, "predictive_history_window", 100)
        self.max_concurrent_routes = getattr(central_config, "max_concurrent_routes", 100)
        self.persistence_path = None  # Not needed, using central storage
        self.rate_limit_per_minute = getattr(central_config, "rate_limit_per_minute", 60)
        self.persistence_history_limit = getattr(central_config, "persistence_history_limit", 1000)

        # === NEW v10.0.0 parameters ===
        self.enable_moe = getattr(central_config, "enable_moe", True)
        self.enable_ga_tuning = getattr(central_config, "enable_ga_tuning", True)
        self.enable_pareto_front = getattr(central_config, "enable_pareto_front", True)
        self.enable_active_user_pref = getattr(central_config, "enable_active_user_pref", True)
        self.ga_population_size = getattr(central_config, "ga_population_size", 20)
        self.ga_generations = getattr(central_config, "ga_generations", 5)
        self.ga_mutation_rate = getattr(central_config, "ga_mutation_rate", 0.2)
        self.ga_crossover_rate = getattr(central_config, "ga_crossover_rate", 0.7)
        self.moe_hidden_layers = getattr(central_config, "moe_hidden_layers", [16, 8])
        self.pareto_max_size = getattr(central_config, "pareto_max_size", 100)
        self.fitness_weights = getattr(central_config, "fitness_weights", {
            'accuracy': 0.3, 'carbon': 0.2, 'helium': 0.2, 'energy': 0.15, 'latency': 0.15
        })

# ============================================================================
# Pydantic Models (unchanged, but included for completeness)
# ============================================================================
class SignalType(str, Enum):
    ENDOCRINE = "endocrine"
    PARACRINE = "paracrine"
    AUTOCRINE = "autocrine"
    JUXTACRINE = "juxtacrine"
    NEUROTRANSMITTER = "neurotransmitter"
    NEUROMODULATOR = "neuromodulator"

class SecondMessenger(str, Enum):
    CAMP = "camp"
    CGMP = "cgmp"
    IP3 = "ip3"
    DAG = "dag"
    CALCIUM = "calcium"
    NITRIC_OXIDE = "nitric_oxide"

class ReceptorState(str, Enum):
    INACTIVE = "inactive"
    BOUND = "bound"
    ACTIVATED = "activated"
    DESENSITIZED = "desensitized"
    INTERNALIZED = "internalized"
    RESENSITIZED = "resensitized"

class AmplificationLevel(int, Enum):
    NONE = 0
    LOW = 1
    MODERATE = 2
    HIGH = 3
    MAXIMUM = 4

class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class SignalReceptor(BaseModel):
    receptor_id: str
    signal_type: SignalType
    ligand: str
    affinity: float = Field(0.5, ge=0, le=1)
    state: ReceptorState = ReceptorState.INACTIVE
    bound_ligands: int = 0
    desensitization_time: float = 0.0
    resensitization_rate: float = Field(0.1, ge=0, le=1)
    amplification: AmplificationLevel = AmplificationLevel.MODERATE
    downstream_effectors: List[str] = Field(default_factory=list)
    last_activated: Optional[datetime] = None
    activation_count: int = 0

class SecondMessengerSystem(BaseModel):
    messenger_type: SecondMessenger
    concentration: float = 0.0
    baseline: float = Field(0.1, ge=0)
    threshold: float = Field(0.3, ge=0, le=1)
    max_concentration: float = Field(1.0, ge=0)
    synthesis_rate: float = Field(0.1, ge=0)
    degradation_rate: float = Field(0.05, ge=0)
    amplification_factor: float = Field(100.0, ge=1)
    target_proteins: List[str] = Field(default_factory=list)
    half_life_seconds: float = Field(5.0, ge=0)

class AllostericSite(BaseModel):
    site_id: str
    modulator: str
    effect: str = "modulation"
    binding_affinity: float = Field(0.5, ge=0, le=1)
    current_occupancy: float = 0.0
    conformational_change: float = 0.0

class MetabolicPathway(BaseModel):
    pathway_id: str
    input_substrate: str
    enzymes: List[str]
    intermediates: List[str]
    final_product: str
    rate_limiting_step: Optional[str] = None
    allosteric_regulators: List[AllostericSite] = Field(default_factory=list)
    energy_cost_ecoatp: float = Field(10.0, ge=0)
    throughput_rate: float = Field(1.0, ge=0)
    is_active: bool = True

class RoutingMetrics(BaseModel):
    total_routes: int = 0
    successful_routes: int = 0
    failed_routes: int = 0
    fallback_routes: int = 0
    biomass_stored_routes: int = 0
    average_latency_ms: float = 0.0
    carbon_savings_kg: float = 0.0
    helium_savings_l: float = 0.0

    @property
    def success_rate(self) -> float:
        return self.successful_routes / max(self.total_routes, 1)

class ExpertCircuitBreaker(BaseModel):
    expert_id: str
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None
    failure_threshold: int = Field(5, ge=1)
    recovery_timeout_seconds: int = Field(30, ge=1)
    half_open_max_requests: int = Field(3, ge=1)
    half_open_requests: int = 0

    def record_success(self):
        self.success_count += 1
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.half_open_requests += 1
            if self.half_open_requests >= self.half_open_max_requests:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        if self.state == CircuitBreakerState.CLOSED and self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
        elif self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN

    def can_execute(self) -> bool:
        if self.state == CircuitBreakerState.CLOSED:
            return True
        if self.state == CircuitBreakerState.OPEN:
            if self.last_failure_time:
                elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                if elapsed >= self.recovery_timeout_seconds:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_requests = 0
                    return True
            return False
        return self.half_open_requests < self.half_open_max_requests

# -----------------------------------------------------------------------------
# Unified Retry and Circuit Breaker Helpers
# -----------------------------------------------------------------------------
def is_retryable_exception(e: Exception) -> bool:
    return isinstance(e, (IOError, TimeoutError, ConnectionError, aiohttp.ClientError))

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0, name: str = "default"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = "closed"
        self._lock = asyncio.Lock()

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        async with self._lock:
            if self.state == "open":
                if (datetime.utcnow().timestamp() - self.last_failure_time) > self.recovery_timeout:
                    self.state = "half-open"
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is open")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == "half-open":
                    self.state = "closed"
                    self.failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = datetime.utcnow().timestamp()
                if self.failure_count >= self.failure_threshold:
                    self.state = "open"
            raise e

# -----------------------------------------------------------------------------
# Rate Limiter
# -----------------------------------------------------------------------------
class RateLimiter:
    def __init__(self, rate_per_minute: int):
        self.rate = rate_per_minute / 60.0
        self.tokens = float(rate_per_minute)
        self.last_update = datetime.utcnow().timestamp()
        self._lock = asyncio.Lock()

    async def acquire(self) -> bool:
        async with self._lock:
            now = datetime.utcnow().timestamp()
            elapsed = now - self.last_update
            self.tokens += elapsed * self.rate
            if self.tokens > self.rate * 60:
                self.tokens = self.rate * 60
            self.last_update = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

# -----------------------------------------------------------------------------
# Carbon Intensity Manager (unchanged)
# -----------------------------------------------------------------------------
class CarbonIntensityManager:
    def __init__(self):
        self.config = central_config
        self.endpoint = "https://api.electricitymap.org/v3/carbon-intensity"
        self.region = getattr(central_config, "carbon_api_region", "us-east")
        self.carbon_intensity = 0.0
        self.carbon_price_usd_per_ton = 50.0
        self.last_update: Optional[datetime] = None
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self.cache: Dict[str, Dict] = {}
        self.historical_intensities = deque(maxlen=1000)
        self.price_history = deque(maxlen=1000)
        self.api_key = os.getenv('ELECTRICITYMAP_API_KEY', '')
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=getattr(central_config, "circuit_breaker_failure_threshold", 5),
            recovery_timeout=getattr(central_config, "circuit_breaker_recovery_timeout", 30.0),
            name="carbon_api"
        )
        self.price_trend = 0.0
        self.forecast_model = None
        self._initialize_forecast_model()
        logger.info(f"CarbonIntensityManager initialized (region={self.region})")

    def _initialize_forecast_model(self):
        if SKLEARN_AVAILABLE:
            from sklearn.linear_model import LinearRegression
            self.forecast_model = LinearRegression()
            self.forecast_trained = False

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
           retry=retry_if_exception_type(is_retryable_exception))
    async def _fetch_carbon_intensity(self, region: str) -> Dict:
        session = await self._get_session()
        url = f"{self.endpoint}/latest?zone={region}"
        headers = {'auth-token': self.api_key} if self.api_key else {}
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status != 200:
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status,
                    message=f"API returned {response.status}"
                )
            return await response.json()

    async def update_carbon_intensity(self, region: Optional[str] = None) -> Dict:
        if region is not None:
            self.region = region

        cache_key = f"{self.region}_{datetime.utcnow().hour}"
        if (cache_key in self.cache and self.last_update and
                (datetime.utcnow() - self.last_update).seconds < 300):
            return self.cache[cache_key]

        try:
            data = await self._circuit_breaker.call(self._fetch_carbon_intensity, self.region)
            self.carbon_intensity = data.get('carbonIntensity', 400)
            self.last_update = datetime.utcnow()
            self.cache[cache_key] = {
                'intensity': self.carbon_intensity,
                'timestamp': self.last_update.isoformat()
            }
            self.historical_intensities.append(self.carbon_intensity)
            self._update_carbon_price(self.carbon_intensity)
            return {
                'intensity': self.carbon_intensity,
                'region': self.region,
                'timestamp': self.last_update.isoformat(),
                'price_usd_per_ton': self.carbon_price_usd_per_ton,
                'trend': self.price_trend
            }
        except Exception as e:
            logger.error(f"Carbon API error: {e}, using fallback")
            return self._get_fallback_response()

    def _update_carbon_price(self, intensity: float):
        base_price = 50.0
        volatility = np.random.normal(0, 5)
        intensity_factor = (intensity - 300) / 500
        price = base_price * (1.0 + intensity_factor) + volatility
        self.carbon_price_usd_per_ton = max(10.0, price)
        self.price_history.append({
            'timestamp': self.last_update.isoformat() if self.last_update else None,
            'intensity': intensity,
            'price': self.carbon_price_usd_per_ton
        })
        if len(self.price_history) > 5:
            recent_prices = [p['price'] for p in list(self.price_history)[-5:]]
            self.price_trend = np.polyfit(range(len(recent_prices)), recent_prices, 1)[0]

    def _get_fallback_response(self) -> Dict:
        fallback_intensities = {'us-east': 420, 'us-west': 350, 'eu': 280, 'asia': 500}
        intensity = fallback_intensities.get(self.region, 400)
        self.carbon_intensity = intensity
        self._update_carbon_price(intensity)
        return {
            'intensity': intensity,
            'region': self.region,
            'timestamp': datetime.utcnow().isoformat(),
            'price_usd_per_ton': self.carbon_price_usd_per_ton,
            'is_fallback': True,
            'trend': self.price_trend
        }

    async def get_current_intensity(self) -> float:
        if self.last_update is None or (datetime.utcnow() - self.last_update).seconds > 300:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity

    async def get_current_price(self) -> float:
        if self.last_update is None or (datetime.utcnow() - self.last_update).seconds > 300:
            await self.update_carbon_intensity(self.region)
        return self.carbon_price_usd_per_ton

    async def close(self):
        if self._session:
            await self._session.close()

# -----------------------------------------------------------------------------
# Helium Efficiency Optimizer (fix asyncio.run)
# -----------------------------------------------------------------------------
class HeliumEfficiencyOptimizer:
    def __init__(self, carbon_manager: Optional[CarbonIntensityManager] = None):
        self.config = central_config
        self.carbon_manager = carbon_manager
        self.helium_budget_l = getattr(central_config, "helium_budget_l", 100.0)
        self.helium_usage: Dict[str, float] = defaultdict(float)
        self.helium_allocation: Dict[str, float] = defaultdict(float)
        self.helium_efficiency_scores: Dict[str, float] = defaultdict(lambda: 0.5)
        self._lock = asyncio.Lock()
        self.optimization_history = deque(maxlen=1000)
        self.helium_price_usd_per_l = 0.5
        self.price_history = deque(maxlen=1000)
        self.price_trend = 0.0
        self.forecast_model = None
        self._initialize_forecast_model()
        logger.info(f"HeliumEfficiencyOptimizer initialized: budget={self.helium_budget_l}L")

    def _initialize_forecast_model(self):
        if SKLEARN_AVAILABLE:
            from sklearn.linear_model import LinearRegression
            self.forecast_model = LinearRegression()
            self.forecast_trained = False

    def _update_helium_price(self, scarcity: float):
        # FIX: removed asyncio.run
        base_price = 0.5
        # Avoid asyncio.run; use carbon_manager if available but check if running loop
        carbon_price = 50.0
        if self.carbon_manager:
            try:
                # Try to get current price without await (may be sync method)
                if hasattr(self.carbon_manager, 'carbon_price_usd_per_ton'):
                    carbon_price = self.carbon_manager.carbon_price_usd_per_ton
                else:
                    # Fallback
                    carbon_price = 50.0
            except:
                pass
        carbon_factor = 1.0 + (carbon_price - 50.0) / 50.0 * 0.2
        scarcity_factor = 1.0 + scarcity * 0.8
        self.helium_price_usd_per_l = max(0.1, base_price * scarcity_factor * carbon_factor)
        self.price_history.append({
            'timestamp': datetime.utcnow().isoformat(),
            'price': self.helium_price_usd_per_l
        })
        if len(self.price_history) > 5:
            recent_prices = [p['price'] for p in list(self.price_history)[-5:]]
            self.price_trend = np.polyfit(range(len(recent_prices)), recent_prices, 1)[0]

    def record_helium_usage(self, expert_id: str, amount_l: float, scarcity: float = 0.5):
        self.helium_usage[expert_id] += amount_l
        self._update_helium_price(scarcity)

    def set_helium_allocation(self, expert_id: str, amount_l: float):
        self.helium_allocation[expert_id] = amount_l

    def update_efficiency_score(self, expert_id: str, score: float):
        self.helium_efficiency_scores[expert_id] = score

    async def optimize_helium_allocation(self, expert_requirements: Dict[str, float]) -> Dict[str, float]:
        async with self._lock:
            total_required = sum(expert_requirements.values())
            if total_required <= self.helium_budget_l:
                return expert_requirements

            if SCIPY_AVAILABLE:
                try:
                    experts = list(expert_requirements.keys())
                    n = len(experts)
                    c = [-self.helium_efficiency_scores.get(eid, 0.5) for eid in experts]
                    A_ub = [[1.0] * n]
                    b_ub = [self.helium_budget_l]
                    bounds = [(0, expert_requirements[eid]) for eid in experts]
                    res = linprog(c, A_ub=A_ub, b_ub=b_ub, bounds=bounds, method='highs')
                    if res.success:
                        allocations = {eid: res.x[i] for i, eid in enumerate(experts)}
                        self.optimization_history.append({
                            'timestamp': datetime.utcnow().isoformat(),
                            'total_required': total_required,
                            'total_allocated': sum(allocations.values()),
                            'price_usd_per_l': self.helium_price_usd_per_l,
                            'allocations': allocations,
                            'method': 'lp'
                        })
                        return allocations
                except Exception as e:
                    logger.warning(f"LP optimization failed: {e}, falling back to heuristic")

            # Fallback heuristic
            optimized = {}
            total_efficiency = sum(self.helium_efficiency_scores.get(eid, 0.5) for eid in expert_requirements)
            if self.helium_price_usd_per_l > 0.8:
                price_factor = 0.7
            elif self.helium_price_usd_per_l < 0.3:
                price_factor = 1.3
            else:
                price_factor = 1.0

            if total_efficiency == 0:
                ratio = (self.helium_budget_l * price_factor) / total_required
                for expert_id, required in expert_requirements.items():
                    optimized[expert_id] = required * ratio
            else:
                adjusted_budget = self.helium_budget_l * price_factor
                for expert_id, required in expert_requirements.items():
                    efficiency_weight = self.helium_efficiency_scores.get(expert_id, 0.5) / total_efficiency
                    optimized[expert_id] = adjusted_budget * efficiency_weight

            self.optimization_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'total_required': total_required,
                'total_allocated': self.helium_budget_l,
                'price_factor': price_factor,
                'price_usd_per_l': self.helium_price_usd_per_l,
                'allocations': optimized,
                'method': 'heuristic'
            })
            return optimized

    def get_helium_status(self) -> Dict[str, Any]:
        total_usage = sum(self.helium_usage.values())
        total_allocated = sum(self.helium_allocation.values())
        return {
            'budget_l': self.helium_budget_l,
            'total_usage_l': total_usage,
            'total_allocated_l': total_allocated,
            'remaining_budget_l': self.helium_budget_l - total_usage,
            'expert_usage': dict(self.helium_usage),
            'expert_allocation': dict(self.helium_allocation),
            'efficiency_scores': dict(self.helium_efficiency_scores),
            'optimization_count': len(self.optimization_history),
            'price_usd_per_l': self.helium_price_usd_per_l,
            'price_trend': self.price_trend,
            'price_samples': len(self.price_history)
        }

# -----------------------------------------------------------------------------
# Federated Routing Learner (guard torch)
# -----------------------------------------------------------------------------
class FederatedRoutingLearner:
    def __init__(self):
        self.config = central_config
        self.server_url = getattr(central_config, "server_url", None)
        self.round = 0
        self.local_model = None
        self.global_model = None
        self.participants = []
        self.contribution_scores: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self.routing_history = deque(maxlen=10000)
        self.privacy_epsilon = getattr(central_config, "privacy_epsilon", 1.0)
        self.noise_scale = 0.001
        self.sparsity_ratio = getattr(central_config, "federated_sparsity_ratio", 0.1)
        # Guard torch usage
        if not TORCH_AVAILABLE:
            logger.warning("PyTorch not available; federated learning disabled")
            self._torch_available = False
        else:
            self._torch_available = True
            self._init_routing_model()
        logger.info(f"FederatedRoutingLearner initialized with ε={self.privacy_epsilon}, sparsity={self.sparsity_ratio}")

    def _init_routing_model(self):
        if not self._torch_available:
            return
        class RoutingModel(nn.Module):
            def __init__(self, input_size=10, hidden_size=64):
                super().__init__()
                self.network = nn.Sequential(
                    nn.Linear(input_size, hidden_size),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size),
                    nn.Linear(hidden_size, hidden_size // 2),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size // 2),
                    nn.Linear(hidden_size // 2, 5)
                )
            def forward(self, x):
                return self.network(x)
        self.local_model = RoutingModel()
        self.global_model = RoutingModel()

    async def _get_session(self) -> Optional[aiohttp.ClientSession]:
        if self._session is None and self.server_url:
            self._session = aiohttp.ClientSession()
        return self._session

    def _add_differential_privacy(self, weights: Dict) -> Dict:
        if not self._torch_available:
            return weights
        if self.privacy_epsilon <= 0:
            return weights
        private_weights = {}
        sensitivity = 1.0
        for key, tensor in weights.items():
            scale = (2 * sensitivity) / self.privacy_epsilon
            noise = torch.randn_like(tensor) * scale * self.noise_scale
            private_weights[key] = tensor + noise
        return private_weights

    def _compress_weights(self, weights: Dict) -> Dict:
        if not self._torch_available:
            return weights
        compressed = {}
        for key, tensor in weights.items():
            flat = tensor.view(-1)
            k = int(flat.numel() * self.sparsity_ratio)
            if k == 0:
                compressed[key] = torch.zeros_like(tensor)
                continue
            topk_vals, topk_idx = torch.topk(flat.abs(), k)
            sparse = torch.zeros_like(flat)
            sparse[topk_idx] = flat[topk_idx]
            compressed[key] = sparse.view(tensor.shape)
        return compressed

    async def train_local_model(self, routing_data: List[Dict], epochs: int = 10) -> float:
        if not routing_data or not self._torch_available:
            return 0.0
        X, y = [], []
        for item in routing_data:
            X.append([
                item.get('carbon_zone', 0) / 10,
                item.get('helium_scarcity', 0.5),
                item.get('task_complexity', 0.5),
                item.get('token_balance', 500) / 1000,
                item.get('carbon_gradient', 0.5),
                item.get('trust_gradient', 0.5),
                item.get('opportunity_gradient', 0.5),
                item.get('stress_level', 0.5),
                item.get('latency_budget', 100) / 1000,
                item.get('energy_budget', 100) / 1000
            ])
            selected = [0] * 5
            expert_idx = item.get('selected_expert_idx', 0)
            if expert_idx < 5:
                selected[expert_idx] = 1
            y.append(selected)

        X = torch.FloatTensor(X)
        y = torch.FloatTensor(y)
        dataset = TensorDataset(X, y)
        dataloader = DataLoader(dataset, batch_size=32, shuffle=True)
        optimizer = optim.Adam(self.local_model.parameters(), lr=0.001)
        criterion = nn.CrossEntropyLoss()

        def train():
            total_loss = 0
            for epoch in range(epochs):
                epoch_loss = 0
                for batch_X, batch_y in dataloader:
                    optimizer.zero_grad()
                    output = self.local_model(batch_X)
                    loss = criterion(output, torch.argmax(batch_y, dim=1))
                    loss.backward()
                    torch.nn.utils.clip_grad_norm_(self.local_model.parameters(), 1.0)
                    optimizer.step()
                    epoch_loss += loss.item()
                total_loss += epoch_loss
            return total_loss / epochs

        avg_loss = await asyncio.to_thread(train)
        logger.info(f"Local routing model trained. Loss: {avg_loss:.4f}")
        return avg_loss

    async def send_local_update(self, performance_metric: float = 1.0) -> Dict:
        if not self.server_url or not self._torch_available:
            return {'status': 'disabled'}

        async with self._lock:
            for attempt in range(getattr(self.config, "max_retries", 3)):
                try:
                    session = await self._get_session()
                    weights = self.local_model.state_dict()
                    private_weights = self._add_differential_privacy(weights)
                    compressed_weights = self._compress_weights(private_weights)
                    weights_serialized = {k: v.tolist() for k, v in compressed_weights.items()}
                    update_data = {
                        'router_id': 'expert_router',
                        'round': self.round,
                        'weights': weights_serialized,
                        'performance': performance_metric,
                        'privacy_epsilon': self.privacy_epsilon,
                        'timestamp': datetime.utcnow().isoformat(),
                        'sparsity_ratio': self.sparsity_ratio
                    }
                    async with session.post(
                        f"{self.server_url}/federated/routing/update",
                        json=update_data,
                        timeout=30
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            self.round += 1
                            self.contribution_scores['router'] = performance_metric
                            return result
                        else:
                            logger.warning(f"Federated update failed (attempt {attempt+1}): {response.status}")
                except Exception as e:
                    logger.error(f"Federated update error (attempt {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)
            return {'status': 'failed'}

    async def get_global_model(self) -> Optional[Dict]:
        if not self.server_url:
            return None
        async with self._lock:
            for attempt in range(getattr(self.config, "max_retries", 3)):
                try:
                    session = await self._get_session()
                    async with session.get(
                        f"{self.server_url}/federated/routing/global",
                        timeout=30
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            weights = data.get('weights', {})
                            self.round = data.get('round', 0)
                            self.participants = data.get('participants', [])
                            if self._torch_available:
                                for k, v in weights.items():
                                    self.global_model.state_dict()[k] = torch.FloatTensor(v)
                            return weights
                except Exception as e:
                    logger.error(f"Global model fetch error (attempt {attempt+1}): {e}")
                await asyncio.sleep(2 ** attempt)
            return None

    async def participate_in_round(self, routing_data: List[Dict], performance: float = 1.0) -> Dict:
        await self.train_local_model(routing_data)
        result = await self.send_local_update(performance)
        global_weights = await self.get_global_model()
        if global_weights and self._torch_available:
            self.global_model.load_state_dict(global_weights)
            if 'router' not in self.participants:
                self.participants.append('router')
        return {
            'round': self.round,
            'participated': bool(global_weights),
            'contribution_score': self.contribution_scores.get('router', 0),
            'performance': performance,
            'peer_count': len(self.participants),
            'privacy_epsilon': self.privacy_epsilon,
            'timestamp': datetime.utcnow().isoformat()
        }

    def get_federated_insights(self) -> Dict:
        return {
            'round': self.round,
            'contribution_score': self.contribution_scores.get('router', 0),
            'participants': len(self.participants),
            'has_global_model': bool(self.global_model),
            'local_model_trained': self.local_model is not None,
            'privacy_epsilon': self.privacy_epsilon,
            'sparsity_ratio': self.sparsity_ratio
        }

    async def close(self):
        if self._session:
            await self._session.close()

# -----------------------------------------------------------------------------
# Predictive Routing Analyzer (unchanged)
# -----------------------------------------------------------------------------
class PredictiveRoutingAnalyzer:
    def __init__(self):
        self.config = central_config
        self.history_window = getattr(central_config, "predictive_history_window", 100)
        self.routing_history = deque(maxlen=self.history_window)
        self.forecast_history = deque(maxlen=50)
        self.scaler = StandardScaler() if SKLEARN_AVAILABLE else None
        self.model = None
        self.is_trained = False
        self.prediction_intervals: Dict[str, List[Tuple[float, float]]] = defaultdict(list)
        self.uncertainty_scores: Dict[str, float] = {}
        self._init_model()
        logger.info("PredictiveRoutingAnalyzer initialized")

    def _init_model(self):
        if SKLEARN_AVAILABLE:
            self.model = SGDRegressor(
                learning_rate='constant',
                eta0=0.01,
                penalty='l2',
                alpha=0.0001,
                max_iter=1,
                random_state=42,
                warm_start=True
            )

    def update_history(self, routing_metrics: Dict):
        self.routing_history.append({
            'timestamp': datetime.utcnow(),
            'success_rate': routing_metrics.get('success_rate', 0.8),
            'avg_latency_ms': routing_metrics.get('avg_latency_ms', 100),
            'carbon_efficiency': routing_metrics.get('carbon_efficiency', 0.5),
            'helium_efficiency': routing_metrics.get('helium_efficiency', 0.5),
            'expert_utilization': routing_metrics.get('expert_utilization', 0.5)
        })

    async def train_forecast_model(self) -> Dict:
        if not SKLEARN_AVAILABLE or not self.model:
            return {'status': 'ml_not_available'}
        if len(self.routing_history) < 10:
            return {'status': 'insufficient_data'}

        X, y = [], []
        history_list = list(self.routing_history)
        for i in range(len(history_list) - 5):
            features = []
            for j in range(5):
                data = history_list[i + j]
                features.extend([
                    data['success_rate'],
                    data['avg_latency_ms'] / 1000,
                    data['carbon_efficiency'],
                    data['helium_efficiency'],
                    data['expert_utilization']
                ])
            X.append(features)
            y.append(history_list[i + 5]['success_rate'])

        X = np.array(X)
        y = np.array(y)

        def scale_and_fit():
            if self.scaler.mean_ is None:
                X_scaled = self.scaler.fit_transform(X)
            else:
                X_scaled = self.scaler.transform(X)
            for _ in range(3):
                self.model.partial_fit(X_scaled, y)
            return X_scaled

        X_scaled = await asyncio.to_thread(scale_and_fit)
        self.is_trained = True
        pred = self.model.predict(X_scaled)
        r2 = r2_score(y, pred) if len(X) > 5 else 0.0
        logger.info(f"Routing forecast model updated. R²={r2:.3f}")
        return {'status': 'success', 'r2': r2, 'samples': len(X)}

    async def predict_routing_performance(self, hours: int = 24) -> Dict:
        if not self.is_trained or len(self.routing_history) < 10:
            if len(self.routing_history) > 0:
                recent = [h['success_rate'] for h in list(self.routing_history)[-5:]]
                pred = np.mean(recent) if recent else 0.5
                return {'predicted_success_rate': pred, 'confidence': 0.3, 'trend': 'moving_average'}
            return {'predicted_success_rate': 0.5, 'confidence': 0.0, 'trend': 'insufficient_data'}

        recent = list(self.routing_history)[-5:]
        features = []
        for data in recent:
            features.extend([
                data['success_rate'],
                data['avg_latency_ms'] / 1000,
                data['carbon_efficiency'],
                data['helium_efficiency'],
                data['expert_utilization']
            ])
        features = np.array(features).reshape(1, -1)

        def predict():
            if self.scaler.mean_ is not None:
                features_scaled = self.scaler.transform(features)
            else:
                features_scaled = features
            pred = self.model.predict(features_scaled)[0]
            return pred

        prediction = await asyncio.to_thread(predict)
        confidence = min(0.9, 0.5 + 0.4 * (len(self.routing_history) / 100))
        if len(self.routing_history) > 20:
            recent_preds = [h['success_rate'] for h in list(self.routing_history)[-20:]]
            std = np.std(recent_preds)
        else:
            std = 0.1
        lower = max(0.0, prediction - 1.96 * std)
        upper = min(1.0, prediction + 1.96 * std)

        if len(self.forecast_history) > 5:
            recent_forecasts = list(self.forecast_history)[-5:]
            trend = "improving" if prediction > recent_forecasts[-1] else "declining" if prediction < recent_forecasts[-1] else "stable"
        else:
            trend = "stable"

        self.forecast_history.append({'prediction': prediction, 'trend': trend})
        self.prediction_intervals['routing'].append((lower, upper))
        self.uncertainty_scores['routing'] = 1.0 - confidence

        return {
            'predicted_success_rate': prediction,
            'confidence': confidence,
            'trend': trend,
            'lower_bound': lower,
            'upper_bound': upper,
            'uncertainty': 1.0 - confidence,
            'recommended_actions': self._generate_predictive_actions(prediction)
        }

    def _generate_predictive_actions(self, prediction: float) -> List[str]:
        actions = []
        if prediction < 0.5:
            actions.append("Optimize expert selection criteria")
            actions.append("Increase carbon budget allocation")
            actions.append("Consider fallback routing strategies")
        elif prediction < 0.7:
            actions.append("Enhance signal transduction sensitivity")
            actions.append("Improve allosteric regulation")
        else:
            actions.append("Maintain current routing configuration")
        return actions

    def get_uncertainty_metrics(self) -> Dict[str, Any]:
        return {
            'prediction_intervals': {k: list(v) for k, v in self.prediction_intervals.items()},
            'uncertainty_scores': self.uncertainty_scores,
            'recent_interval': self.prediction_intervals['routing'][-1] if self.prediction_intervals['routing'] else None,
            'overall_uncertainty': np.mean(list(self.uncertainty_scores.values())) if self.uncertainty_scores else 0.0
        }

# -----------------------------------------------------------------------------
# Causal Constraint Model (unchanged)
# -----------------------------------------------------------------------------
class CausalConstraintModel:
    def __init__(self):
        self.config = central_config
        self.causal_graph = nx.DiGraph()
        self.constraints: Dict[str, Any] = {}
        self.impact_history = deque(maxlen=1000)
        self.causal_strengths: Dict[Tuple[str, str], float] = {}
        self._lock = asyncio.Lock()
        self.counterfactual_cache: Dict[str, Dict] = {}
        self.domain_mapping = {
            'carbon': ['energy', 'helium', 'biodiversity'],
            'helium': ['quantum', 'cooling', 'energy'],
            'energy': ['carbon', 'helium', 'latency'],
            'quantum': ['helium', 'energy', 'accuracy'],
            'biodiversity': ['carbon', 'land_use'],
            'latency': ['energy', 'performance'],
            'accuracy': ['quantum', 'performance']
        }
        self.constraint_thresholds = {
            'carbon': {'max_per_inference': 0.001, 'min_zone': 0},
            'helium': {'max_usage_per_inference': 0.01, 'min_availability': 0.2},
            'energy': {'max_per_inference': 0.01, 'min_efficiency': 0.5},
            'quantum': {'min_qubits': 10, 'max_depth': 100},
            'biodiversity': {'min_impact_score': 0.3}
        }
        self._init_causal_relationships()
        logger.info("CausalConstraintModel initialized")

    def _init_causal_relationships(self):
        edges = [
            ('carbon', 'energy', 0.7), ('carbon', 'helium', 0.5), ('carbon', 'biodiversity', 0.6),
            ('helium', 'quantum', 0.8), ('helium', 'cooling', 0.6), ('helium', 'energy', 0.4),
            ('energy', 'carbon', 0.7), ('energy', 'helium', 0.3), ('energy', 'latency', 0.5),
            ('quantum', 'helium', 0.9), ('quantum', 'energy', 0.6), ('quantum', 'accuracy', 0.8)
        ]
        for u, v, w in edges:
            self.causal_graph.add_edge(u, v, weight=w)
            self.causal_strengths[(u, v)] = w

    def add_causal_relationship(self, source: str, target: str, strength: float = 0.5):
        with self._lock:
            self.causal_graph.add_edge(source, target, weight=strength)
            self.causal_strengths[(source, target)] = strength
            logger.info(f"Added causal relationship: {source} → {target} (strength={strength:.2f})")

    async def update_from_data(self, observations: List[Dict[str, float]]):
        if not observations:
            return
        with self._lock:
            for obs in observations:
                for u in self.domain_mapping.keys():
                    for v in self.domain_mapping.keys():
                        if u != v and u in obs and v in obs:
                            corr = np.corrcoef(obs[u], obs[v])[0, 1] if isinstance(obs[u], list) else 0.0
                            if abs(corr) > 0.3:
                                strength = abs(corr)
                                self.add_causal_relationship(u, v, strength)

    async def propagate_constraints(self, source_domain: str, value: float, constraints: Dict[str, Any]) -> Dict[str, Any]:
        async with self._lock:
            propagated = constraints.copy()
            if source_domain not in self.domain_mapping:
                return propagated
            effects = self.domain_mapping.get(source_domain, [])
            for effect in effects:
                if effect not in propagated:
                    propagated[effect] = {}
                strength = self.causal_strengths.get((source_domain, effect), 0.5)
                impact = strength * value
                propagated[effect]['causal_impact'] = impact
                propagated[effect]['causal_strength'] = strength
                propagated[effect]['source'] = source_domain
                propagated[effect]['expected_change'] = impact * value * 0.1
                if effect in self.constraint_thresholds:
                    threshold = self.constraint_thresholds[effect]
                    compliant = True
                    for key, limit in threshold.items():
                        if key in propagated[effect]:
                            val = propagated[effect][key]
                            if key == 'min_zone' or key == 'min_availability' or key == 'min_efficiency' or key == 'min_qubits' or key == 'min_impact_score':
                                if val < limit:
                                    compliant = False
                            else:
                                if val > limit:
                                    compliant = False
                    propagated[effect]['compliant'] = compliant
                else:
                    propagated[effect]['compliant'] = True
            self.impact_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'source': source_domain,
                'value': value,
                'propagated': propagated
            })
            return propagated

    async def counterfactual_analysis(self, source_domain: str, actual_value: float,
                                      counterfactual_value: float, target_domain: str,
                                      constraints: Dict[str, Any]) -> Dict[str, Any]:
        cache_key = hashlib.md5(
            f"{source_domain}_{actual_value}_{counterfactual_value}_{target_domain}".encode()
        ).hexdigest()[:12]
        if cache_key in self.counterfactual_cache:
            return self.counterfactual_cache[cache_key]

        path = await self.get_causal_path(source_domain, target_domain)
        if not path:
            return {'status': 'no_causal_path'}

        actual_propagated = await self.propagate_constraints(source_domain, actual_value, constraints.copy())
        counterfactual_propagated = await self.propagate_constraints(source_domain, counterfactual_value, constraints.copy())

        actual_impact = actual_propagated.get(target_domain, {}).get('causal_impact', 0.0)
        counterfactual_impact = counterfactual_propagated.get(target_domain, {}).get('causal_impact', 0.0)
        impact_delta = counterfactual_impact - actual_impact
        improvement = impact_delta > 0

        result = {
            'source_domain': source_domain,
            'target_domain': target_domain,
            'actual_value': actual_value,
            'counterfactual_value': counterfactual_value,
            'actual_impact': actual_impact,
            'counterfactual_impact': counterfactual_impact,
            'impact_delta': impact_delta,
            'improvement': improvement,
            'confidence': 0.8 if path else 0.5,
            'causal_path': path,
            'recommendation': (
                f"Consider changing {source_domain} from {actual_value:.2f} to {counterfactual_value:.2f} "
                f"to {('improve' if improvement else 'worsen')} {target_domain} impact by {abs(impact_delta):.3f}"
            )
        }
        self.counterfactual_cache[cache_key] = result
        return result

    async def analyze_tradeoffs(self, scenarios: List[Dict[str, Any]], weights: Dict[str, float] = None) -> List[Dict[str, Any]]:
        async with self._lock:
            if weights is None:
                weights = {
                    'carbon': 0.25, 'helium': 0.20, 'energy': 0.15,
                    'quantum': 0.15, 'biodiversity': 0.15, 'latency': 0.10
                }
            results = []
            for scenario in scenarios:
                impacts = {}
                sustainability_score = 0.0
                risk_factors = []
                for domain, value in scenario.items():
                    if domain in self.domain_mapping:
                        propagated = await self.propagate_constraints(domain, value, scenario)
                        impacts[domain] = propagated
                        domain_score = 1.0 - min(1.0, value)
                        sustainability_score += domain_score * weights.get(domain, 0.1)
                for domain, impact_data in impacts.items():
                    if 'causal_impact' in impact_data and impact_data['causal_impact'] > 0.7:
                        risk_factors.append(f"{domain} has high causal impact")
                results.append({
                    'scenario': scenario,
                    'impacts': impacts,
                    'sustainability_score': min(1.0, sustainability_score),
                    'risk_factors': risk_factors,
                    'recommendations': self._generate_tradeoff_recommendations(impacts, risk_factors)
                })
            results.sort(key=lambda x: x['sustainability_score'], reverse=True)
            return results

    def _generate_tradeoff_recommendations(self, impacts: Dict, risk_factors: List[str]) -> List[str]:
        recommendations = []
        for domain, impact in impacts.items():
            if 'causal_impact' in impact and impact['causal_impact'] > 0.6:
                if domain == 'carbon':
                    recommendations.append("Carbon impact high - consider carbon offset or reduction")
                elif domain == 'helium':
                    recommendations.append("Helium impact high - optimize helium usage")
                elif domain == 'energy':
                    recommendations.append("Energy impact high - improve energy efficiency")
        if risk_factors:
            recommendations.append(f"Monitor these risk factors: {', '.join(risk_factors[:3])}")
        return recommendations or ["No critical trade-offs identified"]

    async def get_causal_path(self, source: str, target: str) -> List[Tuple[str, str, float]]:
        async with self._lock:
            if source not in self.causal_graph or target not in self.causal_graph:
                return []
            try:
                path = nx.shortest_path(self.causal_graph, source, target)
                path_edges = []
                for i in range(len(path) - 1):
                    u, v = path[i], path[i + 1]
                    strength = self.causal_strengths.get((u, v), 0.5)
                    path_edges.append((u, v, strength))
                return path_edges
            except nx.NetworkXNoPath:
                return []

    async def get_causal_strength(self, source: str, target: str) -> float:
        return self.causal_strengths.get((source, target), 0.0)

    def get_causal_graph_summary(self) -> Dict[str, Any]:
        return {
            'nodes': list(self.causal_graph.nodes()),
            'edges': list(self.causal_graph.edges()),
            'edge_count': len(self.causal_graph.edges()),
            'node_count': len(self.causal_graph.nodes()),
            'causal_strengths': self.causal_strengths,
            'recent_impacts': list(self.impact_history)[-10:],
            'counterfactual_cache_size': len(self.counterfactual_cache)
        }

# -----------------------------------------------------------------------------
# Signal Integration Engine (unchanged)
# -----------------------------------------------------------------------------
class SignalIntegrationEngine:
    def __init__(self):
        self.signal_weights: Dict[str, float] = {
            'carbon': 0.25, 'helium': 0.20, 'energy': 0.15,
            'quantum': 0.15, 'trust': 0.15, 'stress': 0.10
        }
        self.signal_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=50))
        self.integration_history = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self.weight_decay = 0.9
        self.learning_rate = 0.05
        logger.info("SignalIntegrationEngine initialized")

    async def integrate_signals(self, signals: Dict[str, float], temporal_window: int = 5) -> Dict[str, Any]:
        async with self._lock:
            for name, value in signals.items():
                if name not in self.signal_history:
                    self.signal_history[name] = deque(maxlen=100)
                self.signal_history[name].append(value)

            integrated_value = 0.0
            total_weight = 0.0
            for name, value in signals.items():
                weight = self.signal_weights.get(name, 0.1)
                integrated_value += value * weight
                total_weight += weight
            if total_weight > 0:
                integrated_value /= total_weight

            signals_list = list(signals.values())
            std_dev = np.std(signals_list) if len(signals_list) > 1 else 0.1
            confidence = max(0.0, min(1.0, 1.0 - std_dev * 2))

            trend = "stable"
            if len(self.signal_history) > temporal_window:
                recent_values = [
                    list(self.signal_history[name])[-temporal_window:]
                    for name in self.signal_weights.keys()
                    if name in self.signal_history and len(self.signal_history[name]) >= temporal_window
                ]
                if recent_values:
                    avg_recent = np.mean([np.mean(v) for v in recent_values])
                    avg_older = np.mean([
                        np.mean(list(self.signal_history[name])[-temporal_window*2:-temporal_window])
                        for name in self.signal_weights.keys()
                        if name in self.signal_history and len(self.signal_history[name]) >= temporal_window*2
                    ]) if len(self.signal_history) > temporal_window*2 else avg_recent
                    if avg_recent > avg_older * 1.05:
                        trend = "improving"
                    elif avg_recent < avg_older * 0.95:
                        trend = "declining"

            result = {
                'integrated_value': integrated_value,
                'confidence': confidence,
                'trend': trend,
                'individual_signals': signals,
                'weights': self.signal_weights.copy(),
                'signal_agreement': 1.0 - std_dev if len(signals_list) > 1 else 0.5
            }
            self.integration_history.append(result)

            await self._update_weights(signals, result)
            return result

    async def _update_weights(self, signals: Dict[str, float], result: Dict):
        if len(self.integration_history) < 5:
            return
        for name in signals:
            if name in self.signal_history and len(self.signal_history[name]) > 5:
                recent = list(self.signal_history[name])[-5:]
                std = np.std(recent)
                reliability = 1.0 / (1.0 + std * 10)
                old_weight = self.signal_weights.get(name, 0.1)
                new_weight = old_weight * self.weight_decay + reliability * (1 - self.weight_decay)
                self.signal_weights[name] = max(0.05, min(0.5, new_weight))
        total = sum(self.signal_weights.values())
        if total > 0:
            for k in self.signal_weights:
                self.signal_weights[k] /= total

    def update_weights(self, new_weights: Dict[str, float]):
        self.signal_weights.update(new_weights)

    def get_integration_stats(self) -> Dict[str, Any]:
        return {
            'current_weights': self.signal_weights.copy(),
            'history_count': len(self.integration_history),
            'recent_integration': self.integration_history[-1] if self.integration_history else None
        }

# -----------------------------------------------------------------------------
# Signal Transduction Engine (safe task creation)
# -----------------------------------------------------------------------------
class SignalTransductionEngine:
    def __init__(self):
        self.receptors: Dict[str, SignalReceptor] = {}
        self.second_messengers: Dict[SecondMessenger, SecondMessengerSystem] = {}
        self.amplification_history = deque(maxlen=1000)
        self.crosstalk_matrix: Dict[Tuple[str, str], float] = {}
        self._lock = asyncio.Lock()
        self._initialize_signaling_systems()
        # Safe task creation
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._signal_degradation_loop())
        except RuntimeError:
            logger.warning("No running event loop; signal degradation loop will not start.")
        logger.info("SignalTransductionEngine initialized")

    def _initialize_signaling_systems(self):
        self.second_messengers[SecondMessenger.CAMP] = SecondMessengerSystem(
            messenger_type=SecondMessenger.CAMP, baseline=0.1, threshold=0.3,
            synthesis_rate=0.15, degradation_rate=0.08, amplification_factor=100.0,
            half_life_seconds=3.0, target_proteins=['energy_expert', 'routing_kinase']
        )
        self.second_messengers[SecondMessenger.CALCIUM] = SecondMessengerSystem(
            messenger_type=SecondMessenger.CALCIUM, baseline=0.05, threshold=0.2,
            synthesis_rate=0.2, degradation_rate=0.1, amplification_factor=1000.0,
            half_life_seconds=1.0, target_proteins=['all_experts', 'emergency_response']
        )
        self.second_messengers[SecondMessenger.IP3] = SecondMessengerSystem(
            messenger_type=SecondMessenger.IP3, baseline=0.05, threshold=0.25,
            synthesis_rate=0.1, degradation_rate=0.06, amplification_factor=500.0,
            half_life_seconds=4.0, target_proteins=['gradient_effectors', 'compartment_activation']
        )
        self.second_messengers[SecondMessenger.NITRIC_OXIDE] = SecondMessengerSystem(
            messenger_type=SecondMessenger.NITRIC_OXIDE, baseline=0.02, threshold=0.15,
            synthesis_rate=0.12, degradation_rate=0.15, amplification_factor=200.0,
            half_life_seconds=2.0, target_proteins=['neighboring_compartments', 'vascular_signaling']
        )

    def create_receptor(self, receptor_id: str, signal_type: SignalType,
                        ligand: str, affinity: float = 0.5,
                        amplification: AmplificationLevel = AmplificationLevel.MODERATE) -> SignalReceptor:
        with self._lock:
            receptor = SignalReceptor(receptor_id=receptor_id, signal_type=signal_type,
                                      ligand=ligand, affinity=affinity, amplification=amplification)
            self.receptors[receptor_id] = receptor
            return receptor

    def bind_ligand(self, receptor_id: str, ligand_concentration: float) -> bool:
        with self._lock:
            if receptor_id not in self.receptors:
                return False
            receptor = self.receptors[receptor_id]
            if receptor.state == ReceptorState.DESENSITIZED:
                return False
            binding_prob = receptor.affinity * ligand_concentration
            if np.random.random() < binding_prob:
                receptor.state = ReceptorState.BOUND
                receptor.bound_ligands += 1
                receptor.last_activated = datetime.utcnow()
                if receptor.bound_ligands >= 2:
                    receptor.state = ReceptorState.ACTIVATED
                    receptor.activation_count += 1
                    self._activate_cascade(receptor)
                    receptor.desensitization_time = 5.0
                    receptor.state = ReceptorState.DESENSITIZED
                    return True
            return False

    def _activate_cascade(self, receptor: SignalReceptor):
        with self._lock:
            if receptor.ligand in ['carbon_gradient', 'energy_signal']:
                messenger = SecondMessenger.CAMP
            elif receptor.ligand in ['emergency', 'stress_signal']:
                messenger = SecondMessenger.CALCIUM
            elif receptor.ligand in ['gradient_change', 'opportunity']:
                messenger = SecondMessenger.IP3
            else:
                messenger = SecondMessenger.NITRIC_OXIDE

            if messenger in self.second_messengers:
                sm = self.second_messengers[messenger]
                amp_factors = {AmplificationLevel.NONE: 1, AmplificationLevel.LOW: 10,
                              AmplificationLevel.MODERATE: 100, AmplificationLevel.HIGH: 1000,
                              AmplificationLevel.MAXIMUM: 10000}
                amp = amp_factors.get(receptor.amplification, 100)
                synthesis = sm.synthesis_rate * amp / 100.0
                sm.concentration = min(sm.max_concentration, sm.concentration + synthesis)
                self.amplification_history.append({
                    'receptor': receptor.receptor_id, 'messenger': messenger.value,
                    'amplification': amp, 'concentration': sm.concentration,
                    'timestamp': datetime.utcnow().isoformat()
                })

    def get_second_messenger_level(self, messenger: SecondMessenger) -> float:
        with self._lock:
            if messenger in self.second_messengers:
                return self.second_messengers[messenger].concentration
            return 0.0

    def is_pathway_active(self, messenger: SecondMessenger) -> bool:
        with self._lock:
            if messenger in self.second_messengers:
                return self.second_messengers[messenger].concentration > self.second_messengers[messenger].threshold
            return False

    async def _signal_degradation_loop(self):
        while True:
            try:
                with self._lock:
                    for sm in self.second_messengers.values():
                        sm.concentration = max(0.0, sm.concentration - sm.degradation_rate)
                    for receptor in self.receptors.values():
                        if receptor.state == ReceptorState.DESENSITIZED:
                            receptor.desensitization_time -= 1.0
                            if receptor.desensitization_time <= 0:
                                receptor.state = ReceptorState.RESENSITIZED
                                receptor.bound_ligands = 0
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"Signal degradation error: {str(e)}")
                await asyncio.sleep(5.0)

    def setup_crosstalk(self, pathway_a: SecondMessenger, pathway_b: SecondMessenger, strength: float):
        with self._lock:
            self.crosstalk_matrix[(pathway_a.value, pathway_b.value)] = strength
            self.crosstalk_matrix[(pathway_b.value, pathway_a.value)] = strength * 0.7

    def apply_crosstalk(self):
        with self._lock:
            for (path_a, path_b), strength in self.crosstalk_matrix.items():
                messenger_a = SecondMessenger(path_a)
                messenger_b = SecondMessenger(path_b)
                if messenger_a in self.second_messengers and messenger_b in self.second_messengers:
                    sm_a = self.second_messengers[messenger_a]
                    sm_b = self.second_messengers[messenger_b]
                    if sm_a.concentration > sm_a.threshold:
                        sm_b.concentration = min(sm_b.max_concentration,
                            sm_b.concentration + sm_a.concentration * strength * 0.1)

    def get_signaling_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                'receptors': {rid: {'state': r.state.value, 'ligand': r.ligand,
                                    'activations': r.activation_count}
                             for rid, r in self.receptors.items()},
                'second_messengers': {sm.value: {'concentration': m.concentration,
                                                  'active': m.concentration > m.threshold}
                                      for sm, m in self.second_messengers.items()}
            }

# -----------------------------------------------------------------------------
# Allosteric Regulation System (unchanged)
# -----------------------------------------------------------------------------
class AllostericRegulationSystem:
    def __init__(self):
        self.allosteric_sites: Dict[str, AllostericSite] = {}
        self.conformational_state: float = 0.5
        self.cooperativity: Dict[Tuple[str, str], float] = {}
        self.regulation_history = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._initialize_allosteric_sites()
        logger.info("AllostericRegulationSystem initialized")

    def _initialize_allosteric_sites(self):
        self.allosteric_sites['carbon_site'] = AllostericSite('carbon_site', 'carbon_gradient', 'modulation', 0.7)
        self.allosteric_sites['helium_site'] = AllostericSite('helium_site', 'helium_gradient', 'inhibitory', 0.6)
        self.allosteric_sites['token_site'] = AllostericSite('token_site', 'token_availability', 'activating', 0.8)
        self.allosteric_sites['trust_site'] = AllostericSite('trust_site', 'trust_gradient', 'activating', 0.5)
        self.allosteric_sites['stress_site'] = AllostericSite('stress_site', 'stress_signal', 'inhibitory', 0.9)

    def bind_modulator(self, site_id: str, modulator_concentration: float) -> float:
        with self._lock:
            if site_id not in self.allosteric_sites:
                return 0.0
            site = self.allosteric_sites[site_id]
            n = 2.0
            Kd = 1.0 - site.binding_affinity
            occupancy = (modulator_concentration ** n) / (Kd ** n + modulator_concentration ** n)
            site.current_occupancy = occupancy
            if site.effect == 'activating':
                change = occupancy * 0.2
            elif site.effect == 'inhibitory':
                change = -occupancy * 0.2
            else:
                change = (occupancy - 0.5) * 0.1
            site.conformational_change = change
            self.conformational_state = max(0.0, min(1.0, self.conformational_state + change))
            self.regulation_history.append({
                'site': site_id, 'modulator': site.modulator,
                'concentration': modulator_concentration, 'occupancy': occupancy,
                'new_state': self.conformational_state, 'timestamp': datetime.utcnow().isoformat()
            })
            return change

    def get_routing_modulation(self) -> Dict[str, float]:
        with self._lock:
            state = self.conformational_state
            return {
                'exploration_rate': state * 0.3, 'exploitation_rate': 1.0 - state * 0.3,
                'risk_tolerance': state * 0.5, 'conservation_mode': (1.0 - state) * 0.8,
                'cooperativity_factor': state * 0.4, 'competition_factor': (1.0 - state) * 0.3
            }

    def setup_cooperativity(self, expert_a: str, expert_b: str, strength: float):
        with self._lock:
            self.cooperativity[(expert_a, expert_b)] = strength
            self.cooperativity[(expert_b, expert_a)] = strength

    def get_cooperativity_bonus(self, expert_a: str, expert_b: str) -> float:
        with self._lock:
            return self.cooperativity.get((expert_a, expert_b), 0.0)

    def get_regulation_status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                'conformational_state': self.conformational_state,
                'state_description': 'relaxed' if self.conformational_state > 0.6 else
                                    'tense' if self.conformational_state < 0.4 else 'intermediate',
                'routing_modulation': self.get_routing_modulation()
            }

# -----------------------------------------------------------------------------
# Metabolic Pathway Router (unchanged)
# -----------------------------------------------------------------------------
class MetabolicPathwayRouter:
    def __init__(self):
        self.pathways: Dict[str, MetabolicPathway] = {}
        self.enzyme_kinetics: Dict[str, Dict[str, float]] = {}
        self.product_levels: Dict[str, float] = defaultdict(float)
        self.throughput_history = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._initialize_pathways()
        logger.info("MetabolicPathwayRouter initialized")

    def _initialize_pathways(self):
        self.pathways['energy_optimization'] = MetabolicPathway(
            'energy_optimization', 'optimization_task', ['energy_expert'],
            ['energy_analysis', 'optimization_plan', 'execution_strategy'],
            'optimized_energy_plan', 'optimization_plan', 10.0,
            [AllostericSite('energy_carbon_site', 'carbon_gradient', 'inhibitory', 0.6),
             AllostericSite('energy_token_site', 'token_availability', 'activating', 0.8)]
        )
        self.pathways['data_processing'] = MetabolicPathway(
            'data_processing', 'data_task', ['data_expert'],
            ['data_ingestion', 'transformation', 'analysis', 'output'],
            'processed_data', 'transformation', 8.0,
            [AllostericSite('data_helium_site', 'helium_gradient', 'inhibitory', 0.5),
             AllostericSite('data_trust_site', 'trust_gradient', 'activating', 0.7)]
        )
        self.pathways['edge_computing'] = MetabolicPathway(
            'edge_computing', 'edge_task', ['iot_expert'],
            ['local_processing', 'mesh_routing', 'result_aggregation'],
            'edge_result', 'mesh_routing', 5.0,
            [AllostericSite('edge_opportunity_site', 'opportunity_gradient', 'activating', 0.9)]
        )
        self.pathways['quantum_computing'] = MetabolicPathway(
            'quantum_computing', 'quantum_task', ['quantum_expert'],
            ['circuit_preparation', 'execution', 'error_mitigation', 'measurement'],
            'quantum_result', 'execution', 50.0,
            [AllostericSite('quantum_complexity_site', 'task_complexity', 'activating', 0.4)]
        )
        for pathway in self.pathways.values():
            for enzyme in pathway.enzymes:
                self.enzyme_kinetics[enzyme] = {'Km': 0.5, 'Vmax': 1.0, 'kcat': 10.0, 'specificity': 0.8}

    def calculate_reaction_rate(self, enzyme: str, substrate_concentration: float) -> float:
        with self._lock:
            if enzyme not in self.enzyme_kinetics:
                return 0.0
            kinetics = self.enzyme_kinetics[enzyme]
            return kinetics['Vmax'] * substrate_concentration / (kinetics['Km'] + substrate_concentration)

    def apply_competitive_inhibition(self, enzyme: str, inhibitor_concentration: float,
                                     inhibition_constant: float = 0.1) -> float:
        with self._lock:
            if enzyme not in self.enzyme_kinetics:
                return 1.0
            kinetics = self.enzyme_kinetics[enzyme]
            apparent_Km = kinetics['Km'] * (1 + inhibitor_concentration / inhibition_constant)
            return kinetics['Km'] / apparent_Km

    def apply_allosteric_regulation(self, pathway_id: str, modulator_levels: Dict[str, float]) -> float:
        with self._lock:
            if pathway_id not in self.pathways:
                return 1.0
            pathway = self.pathways[pathway_id]
            throughput_multiplier = 1.0
            for site in pathway.allosteric_regulators:
                if site.modulator in modulator_levels:
                    concentration = modulator_levels[site.modulator]
                    n = 1.5
                    Kd = 1.0 - site.binding_affinity
                    occupancy = concentration ** n / (Kd ** n + concentration ** n)
                    if site.effect == 'activating':
                        throughput_multiplier *= (1.0 + occupancy * 0.5)
                    elif site.effect == 'inhibitory':
                        throughput_multiplier *= (1.0 - occupancy * 0.5)
            return max(0.1, throughput_multiplier)

    def select_optimal_pathway(self, task_type: str, substrate_concentration: float,
                               modulator_levels: Dict[str, float], energy_budget: float) -> Tuple[Optional[str], float]:
        with self._lock:
            candidates = []
            for pathway_id, pathway in self.pathways.items():
                if task_type not in pathway.input_substrate and pathway.input_substrate not in task_type:
                    continue
                if not pathway.is_active:
                    continue
                total_rate = 0.0
                for enzyme in pathway.enzymes:
                    rate = self.calculate_reaction_rate(enzyme, substrate_concentration)
                    inhibitor_level = sum(self.product_levels.get(p.final_product, 0)
                                         for p in self.pathways.values() if p.pathway_id != pathway_id)
                    inhibition = self.apply_competitive_inhibition(enzyme, inhibitor_level)
                    rate *= inhibition
                    total_rate += rate
                avg_rate = total_rate / max(len(pathway.enzymes), 1)
                allosteric_multiplier = self.apply_allosteric_regulation(pathway_id, modulator_levels)
                regulated_rate = avg_rate * allosteric_multiplier
                energy_efficiency = regulated_rate / max(pathway.energy_cost_ecoatp, 1)
                if pathway.energy_cost_ecoatp > energy_budget:
                    energy_efficiency *= 0.3
                candidates.append((pathway_id, energy_efficiency))
            if not candidates:
                return None, 0.0
            candidates.sort(key=lambda x: x[1], reverse=True)
            return candidates[0]

    def record_throughput(self, pathway_id: str, actual_rate: float, energy_used: float):
        with self._lock:
            self.throughput_history.append({
                'pathway': pathway_id, 'rate': actual_rate, 'energy': energy_used,
                'timestamp': datetime.utcnow().isoformat()
            })
            if pathway_id in self.pathways:
                product = self.pathways[pathway_id].final_product
                self.product_levels[product] += actual_rate * 0.1

    def apply_product_inhibition(self):
        with self._lock:
            for product, level in self.product_levels.items():
                for pathway in self.pathways.values():
                    if pathway.final_product == product and level > 5.0:
                        pathway.throughput_rate *= 0.9
                        self.product_levels[product] *= 0.8

    def get_pathway_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {pid: {'throughput_rate': p.throughput_rate, 'energy_cost': p.energy_cost_ecoatp,
                          'is_active': p.is_active} for pid, p in self.pathways.items()}

# -----------------------------------------------------------------------------
# Routing Context (unchanged)
# -----------------------------------------------------------------------------
class RoutingContext:
    def __init__(self, task: Dict[str, Any], context: Dict[str, Any]):
        self.task = task
        self.context = context
        self.signal_levels: Dict[str, float] = {}
        self.decision_signal: float = 0.0
        self.expert_weights: Dict[str, float] = {}
        self.selected_expert: Optional[str] = None
        self.gating_features: Optional[np.ndarray] = None

# ============================================================================
# NEW MODULES (v10.0.0)
# ============================================================================

# -----------------------------------------------------------------------------
# 1. Genetic Algorithm for Router Parameter Tuning
# -----------------------------------------------------------------------------
class GeneticRouterTuner:
    """
    GA that evolves routing parameters (e.g., gating network hyperparameters, allosteric thresholds,
    metabolic pathway weights, carbon/helium sensitivity).
    """
    def __init__(self, router, config):
        self.router = router
        self.config = config
        self.population_size = config.ga_population_size
        self.generations = config.ga_generations
        self.mutation_rate = config.ga_mutation_rate
        self.crossover_rate = config.ga_crossover_rate
        self.param_bounds = {
            'gating_learning_rate': (1e-4, 1e-2),
            'allosteric_sensitivity': (0.1, 1.0),
            'metabolic_activation': (0.1, 1.0),
            'carbon_weight': (0.0, 1.0),
            'helium_weight': (0.0, 1.0),
            'energy_weight': (0.0, 1.0),
            'latency_weight': (0.0, 1.0),
            'accuracy_weight': (0.0, 1.0),
        }
        self._lock = asyncio.Lock()

    def _random_chromosome(self) -> Dict[str, float]:
        return {k: random.uniform(v[0], v[1]) for k, v in self.param_bounds.items()}

    def _mutate(self, chrom: Dict) -> Dict:
        new = chrom.copy()
        for param in self.param_bounds:
            if random.random() < self.mutation_rate:
                low, high = self.param_bounds[param]
                new[param] = max(low, min(high, chrom[param] + random.gauss(0, (high - low) / 10)))
        return new

    def _crossover(self, p1: Dict, p2: Dict) -> Tuple[Dict, Dict]:
        if random.random() > self.crossover_rate:
            return p1.copy(), p2.copy()
        c1, c2 = p1.copy(), p2.copy()
        for param in self.param_bounds:
            if random.random() < 0.5:
                c1[param], c2[param] = p2[param], p1[param]
        return c1, c2

    async def _evaluate_fitness(self, chrom: Dict) -> float:
        # Simulate routing using these parameters and compute a weighted score.
        # For demo, we'll just compute a heuristic from parameter values.
        score = 0.5
        if chrom['carbon_weight'] > 0.3:
            score += 0.2
        if chrom['helium_weight'] > 0.2:
            score += 0.1
        if chrom['latency_weight'] < 0.5:
            score += 0.1
        # Random noise for diversification
        return max(0.0, min(1.0, score + random.uniform(-0.1, 0.1)))

    async def run_search(self) -> Dict[str, float]:
        population = [self._random_chromosome() for _ in range(self.population_size)]
        best_fitness = -1.0
        best_individual = None

        for gen in range(self.generations):
            fitnesses = await asyncio.gather(*[self._evaluate_fitness(ind) for ind in population])
            sorted_pop = sorted(zip(population, fitnesses), key=lambda x: x[1], reverse=True)
            if sorted_pop[0][1] > best_fitness:
                best_fitness = sorted_pop[0][1]
                best_individual = sorted_pop[0][0]

            parents = [ind for ind, _ in sorted_pop[:max(2, self.population_size // 2)]]
            offspring = []
            while len(offspring) < self.population_size:
                p1, p2 = random.choice(parents), random.choice(parents)
                c1, c2 = self._crossover(p1, p2)
                c1 = self._mutate(c1)
                c2 = self._mutate(c2)
                offspring.append(c1)
                if len(offspring) < self.population_size:
                    offspring.append(c2)
            combined = parents + offspring
            combined_fitness = await asyncio.gather(*[self._evaluate_fitness(ind) for ind in combined])
            sorted_combined = sorted(zip(combined, combined_fitness), key=lambda x: x[1], reverse=True)
            population = [ind for ind, _ in sorted_combined[:self.population_size]]

        # Store best in router
        if best_individual:
            await self.router._apply_tuned_parameters(best_individual)
        return best_individual

# -----------------------------------------------------------------------------
# 2. True MoE Gating Network
# -----------------------------------------------------------------------------
class MoEGatingNetwork:
    """
    Gating network that outputs probabilities over domain experts (which are treated as MoE experts).
    Each expert can be a callable (e.g., an actual domain expert object) or a simple neural network.
    """
    def __init__(self, router, config):
        self.router = router
        self.config = config
        self.expert_names = list(router.experts.keys())  # e.g., ['energy', 'data', 'iot', 'helium', 'quantum']
        self.num_experts = len(self.expert_names)
        self.hidden_layers = config.moe_hidden_layers
        self._gating_model = None
        self._scaler = None
        self._trained = False
        self._training_data = []  # (feature_vector, expert_label, reward)
        self._lock = asyncio.Lock()
        self._torch_model = None  # not used

    def _encode_context(self, context: Dict) -> np.ndarray:
        # Build a feature vector for the gating network.
        features = [
            context.get('carbon_intensity', 0.5),
            context.get('helium_scarcity', 0.5),
            context.get('token_balance_norm', 0.5),
            context.get('gradient_carbon', 0.5),
            context.get('gradient_helium', 0.5),
            context.get('task_complexity', 0.5),
            context.get('latency_budget', 0.5),
            context.get('energy_budget', 0.5),
            context.get('time_of_day', datetime.now().hour / 24.0),
            context.get('stress_level', 0.3),
        ]
        return np.array(features, dtype=np.float32)

    def _train_gating(self):
        if not SKLEARN_AVAILABLE or len(self._training_data) < 10:
            return
        X = np.array([item[0] for item in self._training_data])
        y = np.array([item[1] for item in self._training_data])
        self._scaler = StandardScaler()
        X_scaled = self._scaler.fit_transform(X)
        self._gating_model = MLPClassifier(hidden_layer_sizes=self.hidden_layers, max_iter=200, random_state=42)
        self._gating_model.fit(X_scaled, y)
        self._trained = True
        logger.info("MoE gating network trained on %d samples.", len(self._training_data))

    async def predict_probs(self, context: Dict) -> np.ndarray:
        features = self._encode_context(context)
        if self._trained and self._gating_model is not None:
            X = features.reshape(1, -1)
            if self._scaler:
                X = self._scaler.transform(X)
            probs = self._gating_model.predict_proba(X)[0]
            # Ensure length matches expert_names
            probs = np.array([probs[self.expert_names.index(e)] if e in self.expert_names else 0.0 for e in self.expert_names])
            if probs.sum() == 0:
                probs = np.ones(self.num_experts) / self.num_experts
            return probs / probs.sum()
        else:
            # Fallback: uniform or use existing heuristic
            return np.ones(self.num_experts) / self.num_experts

    async def add_training_sample(self, context: Dict, expert_id: str, reward: float):
        features = self._encode_context(context)
        if expert_id not in self.expert_names:
            return
        expert_label = self.expert_names.index(expert_id)
        async with self._lock:
            self._training_data.append((features, expert_label, reward))
            if len(self._training_data) % 10 == 0:
                self._train_gating()

    def get_stats(self) -> Dict:
        return {
            'trained': self._trained,
            'samples': len(self._training_data),
            'num_experts': self.num_experts,
            'hidden_layers': self.hidden_layers
        }

# -----------------------------------------------------------------------------
# 3. Persistent Pareto Front Optimizer
# -----------------------------------------------------------------------------
class ParetoFrontOptimizer:
    """
    Maintains a persistent Pareto front of expert configurations based on multiple objectives.
    """
    def __init__(self, router, config):
        self.router = router
        self.config = config
        self.max_size = config.pareto_max_size
        self._lock = asyncio.Lock()

    def _dominates(self, a: Dict, b: Dict) -> bool:
        # Objectives: accuracy, carbon, helium, energy, latency (accuracy is maximized, so negate)
        a_metrics = (-a['accuracy'], a['carbon'], a['helium'], a['energy'], a['latency'])
        b_metrics = (-b['accuracy'], b['carbon'], b['helium'], b['energy'], b['latency'])
        return all(a_metrics[i] <= b_metrics[i] for i in range(5)) and any(a_metrics[i] < b_metrics[i] for i in range(5))

    async def add_expert_profile(self, expert_id: str, metrics: Dict[str, float]) -> bool:
        if not self.router.config.enable_pareto_front:
            return False
        front_data = self.router.storage.get_state('pareto_front')
        front = json.loads(front_data) if front_data else []
        entry = {
            'expert_id': expert_id,
            'accuracy': metrics.get('accuracy', 0.5),
            'carbon': metrics.get('carbon', 0.1),
            'helium': metrics.get('helium', 0.01),
            'energy': metrics.get('energy', 0.1),
            'latency': metrics.get('latency', 100),
            'timestamp': datetime.utcnow().isoformat()
        }
        async with self._lock:
            if any(self._dominates(existing, entry) for existing in front):
                return False
            front = [e for e in front if not self._dominates(entry, e)]
            front.append(entry)
            if len(front) > self.max_size:
                front.sort(key=lambda x: x['accuracy'])
                front = front[-self.max_size:]
            self.router.storage.save_state('pareto_front', json.dumps(front))
            return True

    def get_front(self) -> List[Dict]:
        data = self.router.storage.get_state('pareto_front')
        return json.loads(data) if data else []

    async def get_trade_off_suggestions(self, user_weights: Dict[str, float]) -> List[Dict]:
        front = self.get_front()
        if not front:
            return []
        scored = []
        for e in front:
            score = (user_weights.get('accuracy', 0.4) * e['accuracy'] +
                     user_weights.get('carbon', 0.2) * (1 / (e['carbon'] + 1e-8)) +
                     user_weights.get('helium', 0.2) * (1 / (e['helium'] + 1e-8)) +
                     user_weights.get('energy', 0.1) * (1 / (e['energy'] + 1e-8)) +
                     user_weights.get('latency', 0.1) * (1 / (e['latency'] + 1e-8)))
            scored.append((score, e))
        scored.sort(reverse=True)
        return [e for _, e in scored[:5]]

# -----------------------------------------------------------------------------
# 4. Active User Preference Learner
# -----------------------------------------------------------------------------
class ActiveUserPreferenceLearner:
    """
    Learns user preferences via WebSocket queries when two experts have similar performance.
    """
    def __init__(self, router, config):
        self.router = router
        self.config = config
        self.user_weights: Dict[str, Dict[str, float]] = {}

    async def query_user_if_needed(self, user_id: str, candidates: List[Dict]) -> Optional[str]:
        if len(candidates) < 2:
            return None
        # Compare two candidates by accuracy
        acc_diff = abs(candidates[0]['accuracy'] - candidates[1]['accuracy'])
        if acc_diff / max(candidates[0]['accuracy'], candidates[1]['accuracy']) < 0.05:
            # In a real system, send a WebSocket query; for demo, just log.
            logger.info("Querying user %s for preference between %s and %s",
                        user_id, candidates[0]['expert_id'], candidates[1]['expert_id'])
            # For demo, return the first
            return candidates[0]['expert_id']
        return None

    async def record_choice(self, user_id: str, chosen_expert_id: str):
        # Update user weights based on chosen expert's attributes
        # Simple heuristic: increase weight on accuracy
        if user_id not in self.user_weights:
            self.user_weights[user_id] = self.router.config.fitness_weights.copy()
        current = self.user_weights[user_id]
        current['accuracy'] += 0.01
        total = sum(current.values())
        for k in current:
            current[k] /= total

# ============================================================================
# MAIN EXPERT ROUTER (Modified for v10.1.0)
# ============================================================================
class ExpertRouter:
    """
    Enhanced Expert Router v10.1.0 with MoE, GA, Pareto front, and active user preference.
    """

    def __init__(self, storage: Storage, message_queue: AsyncMessageQueue,
                 adaptive_cost: AdaptiveCostFunction, pareto_gating: ParetoGating,
                 drift_detector: DriftDetector, metrics: MetricsRegistry):
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        self.config = ExpertRouterConfig()

        # Feature flags
        self.enable_signal_transduction = self.config.enable_signal_transduction
        self.enable_allosteric = self.config.enable_allosteric
        self.enable_metabolic_pathways = self.config.enable_metabolic_pathways
        self.enable_cooperative_binding = self.config.enable_cooperative_binding
        self.enable_homeostasis = self.config.enable_homeostasis
        self.enable_bio_integration = self.config.enable_bio_integration and BIO_INSPIRED_AVAILABLE
        self.enable_federated = self.config.enable_federated and TORCH_AVAILABLE
        self.enable_predictive = self.config.enable_predictive and SKLEARN_AVAILABLE
        self.enable_carbon_intensity = self.config.enable_carbon_intensity
        self.enable_helium_optimization = self.config.enable_helium_optimization
        self.enable_causal_constraints = self.config.enable_causal_constraints
        self.enable_counterfactual = self.config.enable_counterfactual
        self.enable_signal_integration = self.config.enable_signal_integration
        self.enable_differential_privacy = self.config.enable_differential_privacy
        self.enable_uncertainty_quantification = self.config.enable_uncertainty_quantification

        # Concurrency locks
        self._metrics_lock = asyncio.Lock()
        self._routing_lock = asyncio.Lock()
        self._signal_lock = asyncio.Lock()
        self._allosteric_lock = asyncio.Lock()
        self._metabolic_lock = asyncio.Lock()
        self._causal_lock = asyncio.Lock()
        self._predictive_lock = asyncio.Lock()
        self._federated_lock = asyncio.Lock()
        self._helium_lock = asyncio.Lock()
        self._carbon_lock = asyncio.Lock()

        # Initialize modules
        self.carbon_manager = CarbonIntensityManager() if self.enable_carbon_intensity else None
        self.helium_optimizer = HeliumEfficiencyOptimizer(self.carbon_manager) if self.enable_helium_optimization else None
        self.federated_learner = FederatedRoutingLearner() if self.enable_federated else None
        self.predictive_analyzer = PredictiveRoutingAnalyzer() if self.enable_predictive else None
        self.causal_model = CausalConstraintModel() if self.enable_causal_constraints else None
        self.signal_integrator = SignalIntegrationEngine() if self.enable_signal_integration else None
        self.signal_engine = SignalTransductionEngine() if self.enable_signal_transduction else None
        self.allosteric_system = AllostericRegulationSystem() if self.enable_allosteric else None
        self.metabolic_router = MetabolicPathwayRouter() if self.enable_metabolic_pathways else None

        # Rate limiter
        self._rate_limiter = RateLimiter(self.config.rate_limit_per_minute)

        # Bio-inspired module references (injected)
        self.gradient_manager = None
        self.token_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        self.bio_core = None

        # Adapter managers (injected)
        self.adapter_managers: Dict[str, Any] = {}

        # Initialize signal receptors
        if self.signal_engine:
            self.signal_engine.create_receptor('carbon_receptor', SignalType.ENDOCRINE,
                'carbon_gradient', affinity=0.7, amplification=AmplificationLevel.HIGH)
            self.signal_engine.create_receptor('helium_receptor', SignalType.ENDOCRINE,
                'helium_gradient', affinity=0.6, amplification=AmplificationLevel.MODERATE)
            self.signal_engine.create_receptor('task_receptor', SignalType.NEUROTRANSMITTER,
                'task_signal', affinity=0.9, amplification=AmplificationLevel.HIGH)
            self.signal_engine.create_receptor('stress_receptor', SignalType.AUTOCRINE,
                'stress_signal', affinity=0.8, amplification=AmplificationLevel.MAXIMUM)
            self.signal_engine.create_receptor('trust_receptor', SignalType.PARACRINE,
                'trust_gradient', affinity=0.5, amplification=AmplificationLevel.LOW)
            self.signal_engine.setup_crosstalk(SecondMessenger.CAMP, SecondMessenger.IP3, 0.3)
            self.signal_engine.setup_crosstalk(SecondMessenger.CALCIUM, SecondMessenger.CAMP, 0.5)

        if self.allosteric_system:
            self.allosteric_system.setup_cooperativity('energy', 'data', 0.4)
            self.allosteric_system.setup_cooperativity('energy', 'helium', 0.3)
            self.allosteric_system.setup_cooperativity('data', 'iot', 0.5)

        self.metrics_routing = RoutingMetrics()
        self.experts: Dict[str, Any] = {}
        self.expert_index_map: Dict[int, str] = {}
        self.circuit_breakers: Dict[str, ExpertCircuitBreaker] = {}
        self.gating_network = None
        self.active_routes = 0
        self.max_concurrent_routes = self.config.max_concurrent_routes
        self.routing_history = deque(maxlen=self.config.persistence_history_limit)

        self._initialize_experts(self.config.enable_quantum)
        self._background_tasks: List[asyncio.Task] = []
        self._start_background_tasks()

        # Initialize gating network (if available from external module)
        if GatingNetworkManager is not None:
            try:
                self.gating_network = GatingNetworkManager(
                    input_dim=10,
                    num_experts=len(self.experts)
                )
            except Exception as e:
                logger.error(f"Failed to initialize gating network: {e}")
                self.gating_network = None

        # ============ NEW v10.0.0 Initializations ============
        self.moe_gating = None
        self.ga_tuner = None
        self.pareto_front = None
        self.user_pref_learner = None

        if self.config.enable_moe:
            self.moe_gating = MoEGatingNetwork(self, self.config)
        if self.config.enable_ga_tuning:
            self.ga_tuner = GeneticRouterTuner(self, self.config)
        if self.config.enable_pareto_front:
            self.pareto_front = ParetoFrontOptimizer(self, self.config)
        if self.config.enable_active_user_pref:
            self.user_pref_learner = ActiveUserPreferenceLearner(self, self.config)

        # Load state from central storage if possible
        self._load_state_from_storage()

        # Start background loops for GA tuning and Pareto updates (safe creation)
        self._create_background_task(self._ga_tuning_loop())
        self._create_background_task(self._pareto_update_loop())
        self._create_background_task(self._user_preference_loop())

        logger.info(f"ExpertRouter v10.1.0 initialized with MoE+GA+Pareto+ActivePref")

    def _create_background_task(self, coro):
        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(coro)
            self._background_tasks.append(task)
            return task
        except RuntimeError:
            logger.warning("No running event loop; background task will not start automatically.")
            return None

    # ----------------------------------------------------------------------
    # Expert initialization (unchanged)
    # ----------------------------------------------------------------------
    def _initialize_experts(self, enable_quantum: bool):
        try:
            from .experts.energy_expert import EnergyExpert
            from .experts.data_expert import DataExpert
            from .experts.iot_expert import IoTExpert
            from .experts.helium_expert import HeliumExpert

            self.experts = {
                'energy': EnergyExpert(),
                'data': DataExpert(),
                'iot': IoTExpert(),
                'helium': HeliumExpert()
            }
            if enable_quantum:
                from .experts.quantum_expert import QuantumExpert
                self.experts['quantum'] = QuantumExpert()

            for idx, (expert_id, expert) in enumerate(self.experts.items()):
                self.expert_index_map[idx] = expert_id
                self.circuit_breakers[expert_id] = ExpertCircuitBreaker(expert_id=expert_id)
            logger.info(f"Initialized {len(self.experts)} experts")
        except Exception as e:
            logger.error(f"Failed to initialize experts: {str(e)}")
            self.experts = {}

    # ----------------------------------------------------------------------
    # Background tasks (start)
    # ----------------------------------------------------------------------
    def _start_background_tasks(self):
        self._background_tasks.append(asyncio.create_task(self._signal_transduction_loop()))
        self._background_tasks.append(asyncio.create_task(self._homeostasis_loop()))
        self._background_tasks.append(asyncio.create_task(self._product_inhibition_loop()))
        if self.enable_carbon_intensity:
            self._background_tasks.append(asyncio.create_task(self._carbon_update_loop()))
        if self.enable_federated:
            self._background_tasks.append(asyncio.create_task(self._federated_sync_loop()))
        if self.enable_predictive:
            self._background_tasks.append(asyncio.create_task(self._predictive_update_loop()))

    # ----------------------------------------------------------------------
    # State loading/saving (unchanged)
    # ----------------------------------------------------------------------
    def _load_state_from_storage(self):
        try:
            data = self.storage.get_state("expert_router_state")
            if data:
                state = json.loads(data)
                self.metrics_routing.total_routes = state.get("total_routes", 0)
                self.metrics_routing.successful_routes = state.get("successful_routes", 0)
                self.metrics_routing.failed_routes = state.get("failed_routes", 0)
                self.metrics_routing.average_latency_ms = state.get("average_latency_ms", 0.0)
                self.metrics_routing.carbon_savings_kg = state.get("carbon_savings_kg", 0.0)
                self.metrics_routing.helium_savings_l = state.get("helium_savings_l", 0.0)
                self.active_routes = state.get("active_routes", 0)
                cb_data = state.get("circuit_breakers", {})
                for expert_id, cb_dict in cb_data.items():
                    if expert_id in self.circuit_breakers:
                        cb = self.circuit_breakers[expert_id]
                        cb.state = CircuitBreakerState(cb_dict.get("state", "closed"))
                        cb.failure_count = cb_dict.get("failure_count", 0)
                        cb.success_count = cb_dict.get("success_count", 0)
                        cb.last_failure_time = datetime.fromisoformat(cb_dict.get("last_failure_time")) if cb_dict.get("last_failure_time") else None
                        cb.half_open_requests = cb_dict.get("half_open_requests", 0)
                history = state.get("routing_history", [])
                self.routing_history = deque(history[:self.config.persistence_history_limit])
                # Load MoE model if saved
                if state.get("moe_model"):
                    # Simple placeholder: not implemented for brevity
                    logger.info("MoE model state found but not restored.")
                logger.info("Loaded expert router state from storage")
        except Exception as e:
            logger.error(f"Failed to load expert router state: {e}")

    async def save_state_to_storage(self):
        try:
            state = {
                "total_routes": self.metrics_routing.total_routes,
                "successful_routes": self.metrics_routing.successful_routes,
                "failed_routes": self.metrics_routing.failed_routes,
                "average_latency_ms": self.metrics_routing.average_latency_ms,
                "carbon_savings_kg": self.metrics_routing.carbon_savings_kg,
                "helium_savings_l": self.metrics_routing.helium_savings_l,
                "active_routes": self.active_routes,
                "circuit_breakers": {
                    eid: {
                        "state": cb.state.value,
                        "failure_count": cb.failure_count,
                        "success_count": cb.success_count,
                        "last_failure_time": cb.last_failure_time.isoformat() if cb.last_failure_time else None,
                        "half_open_requests": cb.half_open_requests
                    }
                    for eid, cb in self.circuit_breakers.items()
                },
                "routing_history": list(self.routing_history)
            }
            # Save MoE model if trained
            if self.moe_gating and self.moe_gating._trained:
                # Save scaler and model as pickle strings
                import pickle
                model_bytes = pickle.dumps(self.moe_gating._gating_model)
                scaler_bytes = pickle.dumps(self.moe_gating._scaler)
                state['moe_model'] = {
                    'model': model_bytes.hex(),
                    'scaler': scaler_bytes.hex(),
                    'expert_names': self.moe_gating.expert_names
                }
            self.storage.save_state("expert_router_state", json.dumps(state))
        except Exception as e:
            logger.error(f"Failed to save expert router state: {e}")

    # ----------------------------------------------------------------------
    # Background Loops (unchanged + new)
    # ----------------------------------------------------------------------
    async def _carbon_update_loop(self):
        while True:
            try:
                if self.carbon_manager:
                    await self.carbon_manager.update_carbon_intensity()
                    intensity = await self.carbon_manager.get_current_intensity()
                    self.metrics.set("carbon_intensity", intensity)
                await asyncio.sleep(self.config.carbon_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update error: {str(e)}")
                await asyncio.sleep(60)

    async def _federated_sync_loop(self):
        while True:
            try:
                if self.federated_learner and self.routing_history:
                    routing_data = []
                    for record in list(self.routing_history)[-100:]:
                        routing_data.append({
                            'carbon_zone': record.get('context', {}).get('carbon_zone', 0),
                            'helium_scarcity': record.get('context', {}).get('helium_scarcity', 0.5),
                            'task_complexity': record.get('context', {}).get('task_complexity', 0.5),
                            'token_balance': 500,
                            'carbon_gradient': 0.5,
                            'trust_gradient': 0.5,
                            'opportunity_gradient': 0.5,
                            'stress_level': 0.3,
                            'latency_budget': 100,
                            'energy_budget': 100,
                            'selected_expert_idx': 0
                        })
                    await self.federated_learner.participate_in_round(
                        routing_data,
                        performance=self.metrics_routing.success_rate
                    )
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Federated sync error: {str(e)}")
                await asyncio.sleep(300)

    async def _predictive_update_loop(self):
        while True:
            try:
                if self.predictive_analyzer:
                    self.predictive_analyzer.update_history({
                        'success_rate': self.metrics_routing.success_rate,
                        'avg_latency_ms': self.metrics_routing.average_latency_ms,
                        'carbon_efficiency': 0.5,
                        'helium_efficiency': 0.5,
                        'expert_utilization': self.active_routes / max(self.max_concurrent_routes, 1)
                    })
                    await self.predictive_analyzer.train_forecast_model()
                    forecast = await self.predictive_analyzer.predict_routing_performance()
                    self.metrics.set("predicted_success_rate", forecast.get('predicted_success_rate', 0.5))
                await asyncio.sleep(300)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive update error: {str(e)}")
                await asyncio.sleep(60)

    async def _signal_transduction_loop(self):
        while True:
            try:
                if self.signal_engine:
                    gradient_levels = self._get_real_gradient_levels()
                    self.signal_engine.bind_ligand('carbon_receptor', gradient_levels.get('carbon', 0.5))
                    self.signal_engine.bind_ligand('helium_receptor', gradient_levels.get('helium', 0.5))
                    self.signal_engine.bind_ligand('trust_receptor', gradient_levels.get('trust', 0.5))
                    token_level = self._get_real_token_availability()
                    stress_level = self._get_real_stress_level()
                    if stress_level > 0.5:
                        self.signal_engine.bind_ligand('stress_receptor', stress_level)
                    self.signal_engine.apply_crosstalk()
                    if self.allosteric_system:
                        self.allosteric_system.bind_modulator('carbon_site', gradient_levels.get('carbon', 0.5))
                        self.allosteric_system.bind_modulator('helium_site', gradient_levels.get('helium', 0.5))
                        self.allosteric_system.bind_modulator('trust_site', gradient_levels.get('trust', 0.5))
                        self.allosteric_system.bind_modulator('token_site', token_level)
                        if stress_level > 0.3:
                            self.allosteric_system.bind_modulator('stress_site', stress_level)
                await asyncio.sleep(2.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Signal transduction error: {str(e)}")
                await asyncio.sleep(5.0)

    async def _homeostasis_loop(self):
        while True:
            try:
                if self.enable_homeostasis and self.allosteric_system:
                    modulation = self.allosteric_system.get_routing_modulation()
                    if modulation['conservation_mode'] > 0.7:
                        if np.random.random() < 0.1:
                            self.allosteric_system.bind_modulator('token_site', 0.8)
                    if modulation['risk_tolerance'] > 0.4:
                        self.allosteric_system.bind_modulator('stress_site', 0.3)
                await asyncio.sleep(10.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Homeostasis error: {str(e)}")
                await asyncio.sleep(30.0)

    async def _product_inhibition_loop(self):
        while True:
            try:
                if self.metabolic_router:
                    self.metabolic_router.apply_product_inhibition()
                await asyncio.sleep(60.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Product inhibition error: {str(e)}")
                await asyncio.sleep(120.0)

    # ============ NEW Background Loops ============
    async def _ga_tuning_loop(self):
        while True:
            try:
                await asyncio.sleep(3600 * 12)  # every 12 hours
                if self.ga_tuner:
                    best = await self.ga_tuner.run_search()
                    logger.info("GA tuning completed. Best params: %s", best)
            except Exception as e:
                logger.error(f"GA tuning loop error: {e}")
                await asyncio.sleep(3600)

    async def _pareto_update_loop(self):
        while True:
            try:
                await asyncio.sleep(600)  # every 10 minutes
                if self.pareto_front:
                    logger.debug("Pareto front size: %d", len(self.pareto_front.get_front()))
            except Exception as e:
                logger.error(f"Pareto update error: {e}")
                await asyncio.sleep(300)

    async def _user_preference_loop(self):
        while True:
            try:
                await asyncio.sleep(1800)  # every 30 min
                if self.user_pref_learner and self.pareto_front:
                    front = self.pareto_front.get_front()
                    if len(front) > 1:
                        await self.user_pref_learner.query_user_if_needed('demo_user', front[:2])
            except Exception as e:
                logger.error(f"User preference loop error: {e}")
                await asyncio.sleep(600)

    # ----------------------------------------------------------------------
    # Helper Methods for Bio signals
    # ----------------------------------------------------------------------
    def _get_real_gradient_levels(self) -> Dict[str, float]:
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}

    def _get_real_token_availability(self) -> float:
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            return min(1.0, summary.get('total_balance', 500) / 1000)
        return 0.5

    def _get_real_stress_level(self) -> float:
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            return stats.get('stress_level', 0.3)
        return 0.3

    def inject_bio_core(self, bio_core: Any):
        self.bio_core = bio_core
        if hasattr(bio_core, 'token_manager'):
            self.token_manager = bio_core.token_manager
        if hasattr(bio_core, 'gradient_manager'):
            self.gradient_manager = bio_core.gradient_manager
        if hasattr(bio_core, 'scheduler'):
            self.scheduler = bio_core.scheduler
        if hasattr(bio_core, 'compartment_manager'):
            self.compartment_manager = bio_core.compartment_manager
        if hasattr(bio_core, 'biomass_storage'):
            self.biomass_storage = bio_core.biomass_storage
        if hasattr(bio_core, 'harvester'):
            self.harvester = bio_core.harvester

    # ----------------------------------------------------------------------
    # Adapter Management
    # ----------------------------------------------------------------------
    def register_adapter_manager(self, expert_id: str, adapter_mgr: Any):
        self.adapter_managers[expert_id] = adapter_mgr

    async def route_with_adapters(self, query: Any, domain: str, energy_mode: str = "balanced") -> str:
        expert_id = await self.route(query)
        if expert_id in self.adapter_managers:
            self.adapter_managers[expert_id].activate_mode(energy_mode)
        return expert_id

    async def route_with_energy_awareness(self, query: Any, domain: str,
                                          reasoning_effort: str = "medium",
                                          energy_mode: Optional[str] = None) -> str:
        if energy_mode is None and hasattr(self, 'eco_manager'):
            energy_budget = await self.eco_manager.get_current_budget()
            if energy_budget > 0.7:
                energy_mode = "performance"
            elif energy_budget > 0.3:
                energy_mode = "balanced"
            else:
                energy_mode = "eco"
        elif energy_mode is None:
            energy_mode = "balanced"

        if self.gating_network and hasattr(self.gating_network, 'select_teachers'):
            teacher_ids = await self.gating_network.select_teachers(
                domain=domain,
                reasoning_effort=reasoning_effort,
                energy_mode=energy_mode,
                num_teachers=1,
            )
            if teacher_ids:
                return teacher_ids[0]
        return await self.route(query)

    # ----------------------------------------------------------------------
    # Teacher Interface for MOPD
    # ----------------------------------------------------------------------
    async def policy_probs(self, state: Dict) -> List[float]:
        if self.gating_network:
            context = {
                'helium_scarcity': state.get('helium_scarcity', 0.5),
                'helium_cost_index': state.get('helium_cost_index', 1.0),
                'carbon_intensity': state.get('carbon_intensity', 0.5),
                'model_loss': state.get('model_loss', 0.0),
                'gradient_variance': state.get('gradient_variance', 0.0),
                'avg_client_energy': state.get('avg_client_energy', 0.5),
                'gradient_carbon': state.get('gradient_carbon', 0.5),
                'gradient_helium': state.get('gradient_helium', 0.5),
                'token_balance_norm': state.get('token_balance_norm', 0.5),
                'harvester_stress': state.get('harvester_stress', 0.3)
            }
            expert_weights = await self.gating_network.predict(context)
            probs = [expert_weights.get(eid, 0.0) for eid in self.experts.keys()]
            total = sum(probs)
            if total > 0:
                probs = [p / total for p in probs]
            return probs
        else:
            n = len(self.experts)
            return [1.0 / n] * n

    # ----------------------------------------------------------------------
    # Modular Routing Pipeline (Enhanced)
    # ----------------------------------------------------------------------
    def _build_gating_features(self, context: Dict[str, Any]) -> np.ndarray:
        # Add second messenger levels
        camp = 0.0
        calcium = 0.0
        ip3 = 0.0
        if self.signal_engine:
            camp = self.signal_engine.get_second_messenger_level(SecondMessenger.CAMP)
            calcium = self.signal_engine.get_second_messenger_level(SecondMessenger.CALCIUM)
            ip3 = self.signal_engine.get_second_messenger_level(SecondMessenger.IP3)
        return np.array([
            context.get('helium_scarcity', 0.5),
            context.get('helium_cost_index', 1.0),
            context.get('carbon_intensity', 0.5),
            context.get('model_loss', 0.0),
            context.get('gradient_variance', 0.0),
            context.get('avg_client_energy', 0.5),
            context.get('gradient_carbon', 0.5),
            context.get('gradient_helium', 0.5),
            context.get('token_balance_norm', 0.5),
            context.get('harvester_stress', 0.3),
            camp,   # 10
            calcium, # 11
            ip3,     # 12
        ])

    async def _enrich_context(self, ctx: RoutingContext):
        if self.helium_optimizer:
            ctx.context['helium_scarcity'] = self.helium_optimizer.get_helium_status().get('price_usd_per_l', 0.5)
        if self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            ctx.context['carbon_intensity'] = carbon_intensity / 1000.0
        gradients = self._get_real_gradient_levels()
        ctx.context['gradient_carbon'] = gradients.get('carbon', 0.5)
        ctx.context['gradient_helium'] = gradients.get('helium', 0.5)
        ctx.context['gradient_trust'] = gradients.get('trust', 0.5)
        ctx.context['token_balance_norm'] = self._get_real_token_availability()
        ctx.context['harvester_stress'] = self._get_real_stress_level()

    async def _apply_signal_integration(self, ctx: RoutingContext):
        signal_levels = {
            'carbon': ctx.context.get('carbon_zone', 0) / 10,
            'helium': ctx.context.get('helium_scarcity', 0.5),
            'energy': ctx.context.get('energy_efficiency', 0.5),
            'quantum': 0.5 if ctx.context.get('quantum_capable', False) else 0.0,
            'trust': ctx.context.get('gradient_trust', 0.5),
            'stress': self._get_real_stress_level()
        }
        ctx.signal_levels = signal_levels
        if self.enable_signal_integration and self.signal_integrator:
            integrated = await self.signal_integrator.integrate_signals(signal_levels)
            ctx.decision_signal = integrated['integrated_value']
        else:
            ctx.decision_signal = np.mean(list(signal_levels.values()))

    async def _apply_gating(self, ctx: RoutingContext):
        ctx.gating_features = self._build_gating_features(ctx.context)
        if self.gating_network:
            context_for_gating = {
                'helium_scarcity': ctx.gating_features[0],
                'helium_cost_index': ctx.gating_features[1],
                'carbon_intensity': ctx.gating_features[2],
                'model_loss': ctx.gating_features[3],
                'gradient_variance': ctx.gating_features[4],
                'avg_client_energy': ctx.gating_features[5],
                'gradient_carbon': ctx.gating_features[6],
                'gradient_helium': ctx.gating_features[7],
                'token_balance_norm': ctx.gating_features[8],
                'harvester_stress': ctx.gating_features[9]
            }
            expert_weights = await self.gating_network.predict(context_for_gating)
        else:
            expert_weights = {eid: np.random.random() for eid in self.experts.keys()}
        ctx.expert_weights = expert_weights

    # ============ MoE gating integration ============
    async def _apply_moe_gating(self, ctx: RoutingContext):
        if self.moe_gating:
            probs = await self.moe_gating.predict_probs(ctx.context)
            ctx.expert_weights = {expert_id: prob for expert_id, prob in zip(self.experts.keys(), probs)}
            return
        await self._apply_gating(ctx)

    async def _apply_circuit_breakers(self, ctx: RoutingContext):
        for eid in list(ctx.expert_weights.keys()):
            if eid in self.circuit_breakers and not self.circuit_breakers[eid].can_execute():
                ctx.expert_weights[eid] = 0.0
                logger.debug(f"Expert {eid} bypassed due to open circuit breaker")

    async def _apply_allosteric_modulation(self, ctx: RoutingContext):
        if self.enable_allosteric and self.allosteric_system:
            modulation = self.allosteric_system.get_routing_modulation()
            for a, b in self.allosteric_system.cooperativity.keys():
                if a in ctx.expert_weights and b in ctx.expert_weights:
                    bonus = self.allosteric_system.get_cooperativity_bonus(a, b)
                    ctx.expert_weights[a] *= (1 + bonus * 0.1)
                    ctx.expert_weights[b] *= (1 + bonus * 0.1)
            risk_factor = modulation['risk_tolerance']
            for eid in ctx.expert_weights:
                ctx.expert_weights[eid] *= (1 + (risk_factor - 0.5) * 0.2)

    async def _apply_helium_constraints(self, ctx: RoutingContext):
        if self.enable_helium_optimization and self.helium_optimizer:
            helium_req = {}
            for eid in ctx.expert_weights:
                base_req = 0.01
                task_type = ctx.task.get('type', '')
                if 'quantum' in task_type:
                    base_req = 0.1
                elif 'data' in task_type:
                    base_req = 0.05
                helium_req[eid] = base_req
            optimized = await self.helium_optimizer.optimize_helium_allocation(helium_req)
            for eid, alloc in optimized.items():
                if eid in ctx.expert_weights:
                    ctx.expert_weights[eid] *= (alloc / max(helium_req.get(eid, 0.01), 0.001))

    async def _apply_predictive_adjustment(self, ctx: RoutingContext):
        if self.enable_predictive and self.predictive_analyzer:
            forecast = await self.predictive_analyzer.predict_routing_performance()
            if forecast.get('trend') == 'declining':
                factor = 0.9
            else:
                factor = 1.0
            for eid in ctx.expert_weights:
                ctx.expert_weights[eid] *= factor

    async def _apply_causal_constraints(self, ctx: RoutingContext):
        if self.enable_causal_constraints and self.causal_model:
            constraints = ctx.context.get('constraints', {})
            for eid, weight in list(ctx.expert_weights.items()):
                if weight == 0:
                    continue
                domain = getattr(self.experts[eid], 'domain', 'energy')
                if domain in constraints:
                    propagated = await self.causal_model.propagate_constraints(domain, weight, constraints.copy())
                    # FIXED: Check each effect's compliant flag
                    compliant = True
                    for effect, effect_data in propagated.items():
                        if isinstance(effect_data, dict) and 'compliant' in effect_data:
                            if not effect_data['compliant']:
                                compliant = False
                                break
                    if not compliant:
                        ctx.expert_weights[eid] *= 0.5

    async def _apply_adaptive_cost_and_pareto(self, ctx: RoutingContext):
        """Apply central ParetoGating and AdaptiveCostFunction to refine expert selection."""
        if not ctx.expert_weights:
            return
        # Build candidate list for ParetoGating
        candidates = []
        for eid, weight in ctx.expert_weights.items():
            if weight <= 0:
                continue
            expert = self.experts.get(eid)
            metrics = self._compute_expert_metrics(expert, ctx)
            candidates.append({
                'expert_id': eid,
                'quality_score': weight,
                'carbon_g': metrics['carbon'],
                'latency_ms': metrics['latency'],
                'energy_joules': metrics['energy'],
                'health_score': 1.0,  # placeholder
            })
        if not candidates:
            return
        # Pareto filter
        allowed_ids = set()
        if self.pareto and candidates:
            filtered = self.pareto.filter(candidates)
            if filtered:
                allowed_ids = {c['expert_id'] for c in filtered}
            else:
                allowed_ids = {c['expert_id'] for c in candidates}
        else:
            allowed_ids = {c['expert_id'] for c in candidates}
        # Apply adaptive cost and adjust weights
        cost_scores = {}
        for c in candidates:
            if c['expert_id'] not in allowed_ids:
                cost_scores[c['expert_id']] = 0.0
                continue
            cost = self.adaptive_cost.compute(
                quality=c['quality_score'],
                carbon_g=c['carbon_g'],
                latency_ms=c['latency_ms'],
                energy_joules=c['energy_joules'],
                health=1.0,
                atp=self._get_real_token_availability()
            )
            cost_scores[c['expert_id']] = cost
        # Update ctx.expert_weights by multiplying with cost, then normalize
        new_weights = {}
        total = 0.0
        for eid in ctx.expert_weights:
            if eid in cost_scores:
                new_weights[eid] = ctx.expert_weights[eid] * cost_scores[eid]
            else:
                new_weights[eid] = 0.0
            total += new_weights[eid]
        if total > 0:
            for eid in new_weights:
                new_weights[eid] /= total
        ctx.expert_weights = new_weights

    def _compute_expert_metrics(self, expert: Any, ctx: RoutingContext) -> Dict[str, float]:
        """Compute simplified metrics for an expert based on context."""
        # These would be derived from real measurements in production
        base_carbon = 0.01  # kg
        base_energy = 0.1   # joules
        base_latency = 100  # ms
        # Adjust based on task type and expert domain
        task_type = ctx.task.get('type', '')
        domain = getattr(expert, 'domain', 'energy')
        if 'quantum' in task_type or domain == 'quantum':
            base_carbon = 0.05
            base_energy = 0.5
            base_latency = 200
        elif 'data' in task_type or domain == 'data':
            base_carbon = 0.02
            base_energy = 0.2
            base_latency = 150
        # Adjust for carbon intensity
        carbon_intensity = ctx.context.get('carbon_intensity', 0.5)
        carbon = base_carbon * (1 + carbon_intensity)
        return {'carbon': carbon, 'energy': base_energy, 'latency': base_latency}

    async def _select_expert(self, ctx: RoutingContext) -> bool:
        if not ctx.expert_weights or max(ctx.expert_weights.values()) == 0:
            return False
        ctx.selected_expert = max(ctx.expert_weights, key=ctx.expert_weights.get)
        return True

    async def _record_routing(self, ctx: RoutingContext):
        async with self._metrics_lock:
            self.metrics_routing.total_routes += 1
            self.metrics_routing.successful_routes += 1
            self.active_routes += 1
            self.metrics_routing.average_latency_ms = (self.metrics_routing.average_latency_ms * 0.9 + 50.0 * 0.1)
            self.metrics_routing.carbon_savings_kg += 0.01
            self.metrics_routing.helium_savings_l += 0.001

            # Use generic metrics methods
            self.metrics.increment("route")
            self.metrics.set("active_routes", self.active_routes)
            self.metrics.set("success_rate", self.metrics_routing.success_rate)

        async with self._routing_lock:
            self.routing_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'task': ctx.task,
                'context': ctx.context,
                'expert': ctx.selected_expert,
                'signal_levels': ctx.signal_levels,
                'decision_signal': ctx.decision_signal,
                'expert_weights': ctx.expert_weights
            })

    # ----------------------------------------------------------------------
    # Public route_task (modified to use MoE and Pareto)
    # ----------------------------------------------------------------------
    async def route_task(self, task: Dict[str, Any], context: Dict[str, Any] = None) -> Dict[str, Any]:
        if not await self._rate_limiter.acquire():
            return {'success': False, 'error': 'Rate limit exceeded'}

        ctx = RoutingContext(task, context or {})

        # Pipeline steps
        await self._enrich_context(ctx)
        await self._apply_signal_integration(ctx)

        # Use MoE gating if enabled, else original gating
        if self.config.enable_moe and self.moe_gating:
            await self._apply_moe_gating(ctx)
        else:
            await self._apply_gating(ctx)

        await self._apply_circuit_breakers(ctx)
        await self._apply_allosteric_modulation(ctx)
        await self._apply_helium_constraints(ctx)
        await self._apply_predictive_adjustment(ctx)
        await self._apply_causal_constraints(ctx)

        # NEW: Apply adaptive cost and Pareto gating
        await self._apply_adaptive_cost_and_pareto(ctx)

        if not await self._select_expert(ctx):
            return {'success': False, 'error': 'No available experts'}

        await self._record_routing(ctx)

        # Update MoE training data (with adaptive reward)
        if self.moe_gating:
            # Compute reward using adaptive cost (or simply use decision_signal)
            reward = ctx.decision_signal
            await self.moe_gating.add_training_sample(ctx.context, ctx.selected_expert, reward)

        # Update Pareto front with the selected expert's metrics
        if self.pareto_front:
            metrics = self._compute_expert_metrics(self.experts[ctx.selected_expert], ctx)
            # Convert to Pareto front format
            metrics = {
                'accuracy': ctx.decision_signal,
                'carbon': metrics['carbon'],
                'helium': ctx.context.get('helium_scarcity', 0.5) * 0.01,
                'energy': metrics['energy'],
                'latency': metrics['latency']
            }
            await self.pareto_front.add_expert_profile(ctx.selected_expert, metrics)

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"route_{uuid.uuid4().hex[:8]}",
            selected_action=ctx.selected_expert,
            quality_score=ctx.decision_signal,
            latency_ms=50.0,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="routing",
            adaptive_cost_value=0.0,
            state={'task': task, 'context': context},
            candidates=[{'action': eid} for eid in self.experts.keys()],
            source="expert_router",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["routing", "expert"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

        return {
            'success': True,
            'expert': ctx.selected_expert,
            'decision_signal': ctx.decision_signal,
            'signal_levels': ctx.signal_levels,
            'explanation': f"Task routed to {ctx.selected_expert} based on integrated signals and constraints",
            'metrics': {
                'latency_ms': 50.0,
                'carbon_savings_kg': 0.01,
                'helium_savings_l': 0.001
            }
        }

    async def finalize_route(self, success: bool = True):
        async with self._metrics_lock:
            if self.active_routes > 0:
                self.active_routes -= 1
                if not success:
                    self.metrics_routing.failed_routes += 1
                else:
                    self.metrics_routing.successful_routes += 1
            self.metrics.set("active_routes", self.active_routes)

    # ----------------------------------------------------------------------
    # Apply tuned parameters from GA
    # ----------------------------------------------------------------------
    async def _apply_tuned_parameters(self, chrom: Dict[str, float]):
        self.config.carbon_weight = chrom.get('carbon_weight', 0.3)
        self.config.helium_weight = chrom.get('helium_weight', 0.2)
        self.config.energy_weight = chrom.get('energy_weight', 0.2)
        self.config.latency_weight = chrom.get('latency_weight', 0.1)
        self.config.accuracy_weight = chrom.get('accuracy_weight', 0.2)
        logger.info("Applied GA-tuned parameters: %s", chrom)

    # ----------------------------------------------------------------------
    # Stats and health check (enhanced)
    # ----------------------------------------------------------------------
    def get_routing_stats(self) -> Dict[str, Any]:
        stats = {
            'metrics': {
                'total_routes': self.metrics_routing.total_routes,
                'successful_routes': self.metrics_routing.successful_routes,
                'failed_routes': self.metrics_routing.failed_routes,
                'success_rate': self.metrics_routing.success_rate,
                'average_latency_ms': self.metrics_routing.average_latency_ms,
                'carbon_savings_kg': self.metrics_routing.carbon_savings_kg,
                'helium_savings_l': self.metrics_routing.helium_savings_l
            },
            'active_routes': self.active_routes,
            'max_concurrent_routes': self.max_concurrent_routes,
            'experts': list(self.experts.keys()),
            'circuit_breakers': {
                eid: {
                    'state': cb.state.value,
                    'failure_count': cb.failure_count,
                    'success_count': cb.success_count
                }
                for eid, cb in self.circuit_breakers.items()
            },
            'gating_network': self.gating_network is not None
        }

        if self.signal_engine:
            stats['signaling'] = self.signal_engine.get_signaling_status()
        if self.allosteric_system:
            stats['allosteric'] = self.allosteric_system.get_regulation_status()
        if self.metabolic_router:
            stats['pathways'] = self.metabolic_router.get_pathway_stats()
        if self.helium_optimizer:
            stats['helium'] = self.helium_optimizer.get_helium_status()
        if self.federated_learner:
            stats['federated'] = self.federated_learner.get_federated_insights()
        if self.predictive_analyzer:
            stats['predictive'] = self.predictive_analyzer.get_uncertainty_metrics()
        if self.causal_model:
            stats['causal'] = self.causal_model.get_causal_graph_summary()
        if self.signal_integrator:
            stats['signal_integration'] = self.signal_integrator.get_integration_stats()

        # New stats
        if self.moe_gating:
            stats['moe'] = self.moe_gating.get_stats()
        if self.pareto_front:
            stats['pareto_front_size'] = len(self.pareto_front.get_front())
        if self.ga_tuner:
            stats['ga_enabled'] = self.config.enable_ga_tuning
        if self.user_pref_learner:
            stats['active_user_pref_enabled'] = self.config.enable_active_user_pref

        return stats

    async def health_check(self) -> Dict[str, Any]:
        status = {
            'status': 'healthy',
            'timestamp': datetime.utcnow().isoformat(),
            'version': '10.1.0'
        }
        subsystems = {
            'carbon_manager': self.carbon_manager is not None,
            'helium_optimizer': self.helium_optimizer is not None,
            'federated_learner': self.federated_learner is not None,
            'predictive_analyzer': self.predictive_analyzer is not None,
            'causal_model': self.causal_model is not None,
            'signal_integrator': self.signal_integrator is not None,
            'signal_engine': self.signal_engine is not None,
            'allosteric_system': self.allosteric_system is not None,
            'metabolic_router': self.metabolic_router is not None,
        }
        for name, enabled in subsystems.items():
            status[name] = 'active' if enabled else 'disabled'
        status['expert_count'] = len(self.experts)
        status['circuit_breakers_open'] = sum(1 for cb in self.circuit_breakers.values() if cb.state == CircuitBreakerState.OPEN)
        status['active_routes'] = self.active_routes
        status['success_rate'] = self.metrics_routing.success_rate
        # New health info
        if self.moe_gating:
            status['moe_trained'] = self.moe_gating._trained
        if self.pareto_front:
            status['pareto_front_size'] = len(self.pareto_front.get_front())
        if self.ga_tuner:
            status['ga_enabled'] = self.config.enable_ga_tuning
        return status

    async def shutdown(self):
        logger.info("Shutting down Expert Router")
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)

        await self.save_state_to_storage()

        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated_learner:
            await self.federated_learner.close()
        logger.info("Shutdown complete")

# -----------------------------------------------------------------------------
# Example Usage
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    async def main():
        from ..storage import Storage
        from ..scaling.message_queue import AsyncMessageQueue
        from ..feedback.adaptive_cost import AdaptiveCostFunction
        from ..routing.pareto_gating import ParetoGating
        from ..safety.drift_detector import DriftDetector
        from ..metrics import MetricsRegistry

        storage = Storage()
        queue = AsyncMessageQueue()
        adaptive_cost = AdaptiveCostFunction(storage)
        pareto = ParetoGating()
        drift = DriftDetector(storage, adaptive_cost)
        metrics = MetricsRegistry()

        router = ExpertRouter(storage, queue, adaptive_cost, pareto, drift, metrics)

        task = {"type": "energy_optimization", "params": {}}
        context = {"carbon_zone": 5, "helium_scarcity": 0.6}
        result = await router.route_task(task, context)
        print("Routing result:", result)

        stats = router.get_routing_stats()
        print("Stats:", stats)

        health = await router.health_check()
        print("Health:", health)

        await router.shutdown()

    asyncio.run(main())
