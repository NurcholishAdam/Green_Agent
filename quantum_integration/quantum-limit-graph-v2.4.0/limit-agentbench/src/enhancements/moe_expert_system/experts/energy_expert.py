#!/usr/bin/env python3
# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/energy_expert.py
# Version 3.4.0 – Enhanced Green Agent MODP Integration
#
# ENHANCEMENTS OVER v3.3.0:
# 1. Fixed task_counts mapping bug.
# 2. Lightweight health check (no side effects).
# 3. Corrected energy field definitions: separated idle, CPU, memory, network, storage.
# 4. Added XAI explanation to routing and estimation decisions.
# 5. Added temporal safety tracking for route/strategy selections (cooldown).
# 6. Improved state loading robustness for new dataclass fields.
# 7. Minor cleanup and error handling.

import asyncio
import json
import os
import hashlib
import uuid
import time
from typing import Dict, Any, List, Optional, Tuple, Union, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from enum import Enum
import numpy as np
import pandas as pd
import pickle
from pathlib import Path

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

# Optional: aiohttp (guarded)
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    logger.warning("aiohttp not available; URL-based carbon fetch will fail gracefully")

# Optional: central circuit breaker and rate limiter
try:
    from ..scaling.circuit_breaker import EnhancedCircuitBreaker
    from ..scaling.rate_limiter import EnhancedRateLimiter
    CENTRAL_CIRCUIT_BREAKER_AVAILABLE = True
except ImportError:
    # Fallback: define a simple local circuit breaker
    class EnhancedCircuitBreaker:
        def __init__(self, name, failure_threshold=5, recovery_timeout=30.0):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self.failure_count = 0
            self.last_failure_time = None
            self.state = "closed"
            self._lock = asyncio.Lock()
        async def call(self, func, *args, **kwargs):
            async with self._lock:
                if self.state == "open":
                    if self.last_failure_time and (datetime.now(timezone.utc) - self.last_failure_time).total_seconds() > self.recovery_timeout:
                        self.state = "half-open"
                    else:
                        raise RuntimeError(f"Circuit breaker {self.name} is open")
            try:
                result = await func(*args, **kwargs)
                async with self._lock:
                    self.state = "closed"
                    self.failure_count = 0
                return result
            except Exception as e:
                async with self._lock:
                    self.failure_count += 1
                    self.last_failure_time = datetime.now(timezone.utc)
                    if self.failure_count >= self.failure_threshold:
                        self.state = "open"
                raise e
    CENTRAL_CIRCUIT_BREAKER_AVAILABLE = False

# Optional: central carbon manager
try:
    from ..carbon_intensity import CarbonIntensityManager
    CENTRAL_CARBON_AVAILABLE = True
except ImportError:
    CENTRAL_CARBON_AVAILABLE = False

# Optional: central helium manager
try:
    from ..helium_optimizer import HeliumEfficiencyOptimizer
    CENTRAL_HELIUM_AVAILABLE = True
except ImportError:
    CENTRAL_HELIUM_AVAILABLE = False

# Optional: base expert
try:
    from .base_expert import BaseExpert
    BASE_EXPERT_AVAILABLE = True
except ImportError:
    class BaseExpert:
        def __init__(self):
            self.expert_name = "energy_expert"
            self.supported_task_types = ["energy_estimate", "carbon_profile", "helium_analysis", "sustainability_recommend", "energy_route", "forecast"]
            self.health_status = "healthy"
        async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
            raise NotImplementedError()
        def get_capabilities(self) -> Dict[str, Any]:
            return {'name': self.expert_name, 'supported_tasks': self.supported_task_types, 'health': self.health_status}
        def get_metrics(self) -> Dict[str, Any]:
            return {}

# Optional: bio-inspired modules
try:
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager, EcoATPConsumer
    TOKEN_AVAILABLE = True
except ImportError:
    TOKEN_AVAILABLE = False
try:
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    GRADIENT_AVAILABLE = True
except ImportError:
    GRADIENT_AVAILABLE = False
try:
    from enhancements.bio_inspired.atp_synthase_scheduler import ATPSynthaseScheduler
    ATP_AVAILABLE = True
except ImportError:
    ATP_AVAILABLE = False
try:
    from enhancements.bio_inspired.time_tick_engine import TimeTickEngine
    TICK_ENGINE_AVAILABLE = True
except ImportError:
    TICK_ENGINE_AVAILABLE = False
try:
    from enhancements.bio_inspired.quantum_bridge import QuantumBridge
    QUANTUM_BRIDGE_AVAILABLE = True
except ImportError:
    QUANTUM_BRIDGE_AVAILABLE = False

# ============================================================================
# Configuration – dataclass for serialization
# ============================================================================
@dataclass
class EnergyExpertConfig:
    """Configuration for EnergyExpert, built from central_config."""
    enable_energy_estimation: bool = getattr(central_config, "energy_enable_estimation", True)
    enable_carbon_tracking: bool = getattr(central_config, "energy_enable_carbon_tracking", True)
    enable_helium_analysis: bool = getattr(central_config, "energy_enable_helium_analysis", True)
    enable_forecasting: bool = getattr(central_config, "energy_enable_forecasting", True)
    enable_telemetry: bool = True
    enable_persistence: bool = True
    enable_real_time_carbon: bool = getattr(central_config, "energy_enable_real_time_carbon", True)
    enable_real_time_helium: bool = getattr(central_config, "energy_enable_real_time_helium", False)

    cpu_power_watt: float = getattr(central_config, "energy_cpu_power_watt", 50.0)
    memory_power_per_gb: float = getattr(central_config, "energy_memory_power_per_gb", 0.5)
    network_power_per_mbps: float = getattr(central_config, "energy_network_power_per_mbps", 0.01)
    storage_power_per_gb: float = getattr(central_config, "energy_storage_power_per_gb", 0.001)
    idle_power_watt: float = getattr(central_config, "energy_idle_power_watt", 10.0)
    power_utilization_factor: float = getattr(central_config, "energy_power_utilization_factor", 0.7)
    default_carbon_intensity_g_per_kwh: float = getattr(central_config, "default_carbon_intensity_g_per_kwh", 100.0)
    carbon_api_url: str = getattr(central_config, "carbon_api_url", "https://api.electricitymap.org/v3/carbon-intensity/latest")
    carbon_api_key: str = os.getenv('ELECTRICITYMAP_API_KEY', '')
    helium_scarcity_factor: float = getattr(central_config, "energy_helium_scarcity_factor", 1.0)
    helium_recovery_efficiency: float = getattr(central_config, "energy_helium_recovery_efficiency", 0.7)
    helium_cost_per_liter_usd: float = getattr(central_config, "energy_helium_cost_per_liter_usd", 0.5)
    energy_efficiency_threshold: float = getattr(central_config, "energy_efficiency_threshold", 0.7)
    carbon_budget_per_task_g: float = getattr(central_config, "energy_carbon_budget_per_task_g", 10.0)
    helium_budget_per_task_ml: float = getattr(central_config, "energy_helium_budget_per_task_ml", 5.0)
    forecast_window_hours: int = getattr(central_config, "energy_forecast_window_hours", 24)
    circuit_breaker_failure_threshold: int = getattr(central_config, "circuit_breaker_failure_threshold", 5)
    circuit_breaker_recovery_timeout: float = getattr(central_config, "circuit_breaker_recovery_timeout", 30.0)
    carbon_cache_ttl_seconds: int = getattr(central_config, "energy_carbon_cache_ttl_seconds", 300)
    helium_cache_ttl_seconds: int = getattr(central_config, "energy_helium_cache_ttl_seconds", 300)

    def __post_init__(self):
        if self.cpu_power_watt <= 0:
            self.cpu_power_watt = 50.0

# ============================================================================
# Enums for Energy Operations (unchanged)
# ============================================================================
class EnergySourceType(Enum):
    RENEWABLE = "renewable"
    FOSSIL_FUEL = "fossil_fuel"
    NUCLEAR = "nuclear"
    MIXED = "mixed"
    UNKNOWN = "unknown"

class SustainabilityStrategy(Enum):
    CONSERVATIVE = "conservative"
    BALANCED = "balanced"
    PERFORMANCE = "performance"
    RENEWABLE_ONLY = "renewable_only"

# ============================================================================
# Energy Profiling Results – updated dataclass with extra fields
# ============================================================================
@dataclass
class EnergyProfile:
    task_id: str
    estimated_duration_seconds: float
    estimated_idle_energy_kwh: float = 0.0          # NEW
    estimated_cpu_energy_kwh: float = 0.0           # CPU active only
    estimated_memory_energy_kwh: float = 0.0
    estimated_network_energy_kwh: float = 0.0
    estimated_storage_energy_kwh: float = 0.0       # NEW
    estimated_total_energy_kwh: float = 0.0
    carbon_intensity_g_per_kwh: float = 0.0
    estimated_carbon_g: float = 0.0
    estimated_helium_ml: float = 0.0
    energy_efficiency_score: float = 0.0
    sustainability_score: float = 0.0
    recommended_strategy: str = "balanced"
    region: str = "us-east"
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    explanation: Dict[str, Any] = field(default_factory=dict)  # NEW for XAI
    def to_dict(self) -> Dict[str, Any]: return asdict(self)

@dataclass
class HeliumAnalysis:
    available_ml: float
    required_ml: float
    scarcity_factor: float
    recovery_potential_ml: float
    can_proceed: bool
    recommendation: str
    timestamp: str
    def to_dict(self) -> Dict[str, Any]: return asdict(self)

@dataclass
class CarbonFootprint:
    baseline_carbon_g: float
    offset_strategy: Optional[str]
    offset_carbon_g: float
    net_carbon_g: float
    cost_usd: float
    roi_factor: float
    timestamp: str
    def to_dict(self) -> Dict[str, Any]: return asdict(self)

@dataclass
class EnergyExpertMetrics:
    operation_name: str
    start_time: float
    end_time: Optional[float] = None
    tasks_analyzed: int = 0
    total_energy_kwh: float = 0.0
    total_carbon_kg: float = 0.0
    total_helium_ml: float = 0.0
    success: bool = True
    error_message: Optional[str] = None
    def duration_seconds(self) -> float:
        if self.end_time: return self.end_time - self.start_time
        return 0.0
    def to_dict(self) -> Dict[str, Any]: return asdict(self)

# ============================================================================
# Energy Expert Implementation – Fully Integrated v3.4.0
# ============================================================================
class EnergyExpert(BaseExpert):
    """
    Energy Expert v3.4.0 – MoE Expert for Energy, Carbon & Helium Profiling
    Full Green Agent MODP integration.
    """

    def __init__(
        self,
        storage: Storage,
        message_queue: AsyncMessageQueue,
        adaptive_cost: AdaptiveCostFunction,
        pareto_gating: ParetoGating,
        drift_detector: DriftDetector,
        metrics: MetricsRegistry,
        carbon_manager: Optional[Any] = None,
        helium_optimizer: Optional[Any] = None,
        token_manager: Optional[Any] = None,
        gradient_manager: Optional[Any] = None,
        scheduler: Optional[Any] = None,
        tick_engine: Optional[Any] = None,
        quantum_bridge: Optional[Any] = None
    ):
        super().__init__()
        self.expert_name = "energy_expert"
        self.supported_task_types = [
            "energy_estimate", "carbon_profile", "helium_analysis",
            "sustainability_recommend", "energy_route", "forecast"
        ]
        self.health_status = "healthy"

        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics
        self.carbon_manager = carbon_manager
        self.helium_optimizer = helium_optimizer
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        self.scheduler = scheduler
        self.tick_engine = tick_engine
        self.quantum_bridge = quantum_bridge

        self.config = EnergyExpertConfig()

        self.energy_profiles: Dict[str, EnergyProfile] = {}
        self.carbon_footprints: Dict[str, CarbonFootprint] = {}
        self.helium_analyses: Dict[str, HeliumAnalysis] = {}
        self.metrics_history: List[EnergyExpertMetrics] = []
        self.tasks_handled = 0
        self.total_latency = 0.0
        self.task_energy_cache: Dict[str, float] = {}

        # Fixed task counts mapping
        self.task_counts = {'estimate': 0, 'carbon': 0, 'helium': 0, 'recommend': 0, 'route': 0, 'forecast': 0}

        # Temporal safety: track recent route/strategy selections
        self.recent_route_selections = deque(maxlen=10)
        self.recent_strategy_selections = deque(maxlen=10)

        self._carbon_cache: Dict[str, Tuple[float, datetime]] = {}
        self._helium_cache: Dict[str, Tuple[float, datetime]] = {}
        self._cache_lock = asyncio.Lock()

        self._circuit_breaker = EnhancedCircuitBreaker(
            "energy_external",
            failure_threshold=self.config.circuit_breaker_failure_threshold,
            recovery_timeout=self.config.circuit_breaker_recovery_timeout
        )

        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        self._background_tasks: List[asyncio.Task] = []
        self._running = True
        if self.config.enable_real_time_carbon:
            self._start_background_tasks()

        # Safe async state loading
        self._load_state_task = self._create_task(self._load_state())

        logger.info(f"EnergyExpert v3.4.0 initialized.")

    def _create_task(self, coro):
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            logger.warning("No running event loop; background task not started.")
            return None

    # ==========================================================================
    # State Persistence using central Storage
    # ==========================================================================
    async def _load_state(self):
        try:
            data = self.storage.get_state("energy_expert_state")
            if data:
                state = json.loads(data)
                self.tasks_handled = state.get('tasks_handled', 0)
                self.total_latency = state.get('total_latency', 0.0)
                # Use fixed keys
                saved_counts = state.get('task_counts', {})
                for k in self.task_counts.keys():
                    self.task_counts[k] = saved_counts.get(k, 0)
                for metrics_dict in state.get('metrics_history', []):
                    metrics = EnergyExpertMetrics(**metrics_dict)
                    self.metrics_history.append(metrics)
                for k, v in state.get('carbon_footprints', {}).items():
                    self.carbon_footprints[k] = CarbonFootprint(**v)
                for k, v in state.get('helium_analyses', {}).items():
                    self.helium_analyses[k] = HeliumAnalysis(**v)
                for task_id, profile_dict in state.get('energy_profiles', {}).items():
                    # Ensure new fields have defaults
                    profile_dict.setdefault('estimated_idle_energy_kwh', 0.0)
                    profile_dict.setdefault('estimated_storage_energy_kwh', 0.0)
                    profile_dict.setdefault('explanation', {})
                    profile = EnergyProfile(**profile_dict)
                    self.energy_profiles[task_id] = profile
                logger.info("EnergyExpert state loaded from central storage")
        except Exception as e:
            logger.error(f"Failed to load energy expert state: {e}")

    async def _save_state(self):
        try:
            state = {
                'tasks_handled': self.tasks_handled,
                'total_latency': self.total_latency,
                'task_counts': self.task_counts,
                'metrics_history': [m.to_dict() for m in self.metrics_history[-1000:]],
                'carbon_footprints': {k: v.to_dict() for k, v in self.carbon_footprints.items()},
                'helium_analyses': {k: v.to_dict() for k, v in self.helium_analyses.items()},
                'energy_profiles': {k: v.to_dict() for k, v in self.energy_profiles.items()},
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            self.storage.save_state("energy_expert_state", json.dumps(state))
            logger.info("EnergyExpert state saved to central storage")
        except Exception as e:
            logger.error(f"Failed to save energy expert state: {e}")

    # ==========================================================================
    # Teacher Interface for MOPD (context-aware soft policy)
    # ==========================================================================
    async def policy_probs(self, state: Dict) -> List[float]:
        """
        Return a probability distribution over energy-handling strategies,
        computed using adaptive cost and Pareto constraints.
        """
        strategies = ['estimate', 'carbon', 'helium', 'recommend', 'route', 'forecast']
        candidates = []
        for strategy in strategies:
            if strategy == 'estimate':
                carbon_g = 5.0
                latency_ms = 50.0
                energy_joules = 100.0
                quality = 0.85
            elif strategy == 'carbon':
                carbon_g = 10.0
                latency_ms = 30.0
                energy_joules = 50.0
                quality = 0.8
            elif strategy == 'helium':
                carbon_g = 2.0
                latency_ms = 40.0
                energy_joules = 30.0
                quality = 0.75
            elif strategy == 'recommend':
                carbon_g = 1.0
                latency_ms = 20.0
                energy_joules = 20.0
                quality = 0.9
            elif strategy == 'route':
                carbon_g = 3.0
                latency_ms = 25.0
                energy_joules = 25.0
                quality = 0.8
            elif strategy == 'forecast':
                carbon_g = 1.5
                latency_ms = 60.0
                energy_joules = 40.0
                quality = 0.7
            else:
                carbon_g = 5.0
                latency_ms = 50.0
                energy_joules = 50.0
                quality = 0.5

            cost = self.adaptive_cost.compute(
                quality=quality,
                carbon_g=carbon_g,
                latency_ms=latency_ms,
                energy_joules=energy_joules,
                health=self.health_status == 'healthy',
                atp=0.5
            )
            candidates.append({
                'strategy': strategy,
                'score': cost,
                'carbon_g': carbon_g,
                'latency_ms': latency_ms,
                'energy_joules': energy_joules,
                'quality_score': quality
            })

        if self.pareto:
            filtered = self.pareto.filter(candidates)
            if filtered:
                allowed = {c['strategy'] for c in filtered}
                candidates = [c for c in candidates if c['strategy'] in allowed]

        scores = [c['score'] for c in candidates]
        if scores:
            exp_scores = np.exp(scores - np.max(scores))
            probs = exp_scores / np.sum(exp_scores)
            full_probs = [0.0] * len(strategies)
            for c, p in zip(candidates, probs):
                idx = strategies.index(c['strategy'])
                full_probs[idx] = p
            return full_probs
        return [1/6] * 6

    # ==========================================================================
    # Background Tasks
    # ==========================================================================
    def _start_background_tasks(self):
        if self.config.enable_real_time_carbon:
            task = self._create_task(self._periodic_carbon_update())
            if task:
                self._background_tasks.append(task)
                logger.info("Started background carbon update task")

    async def _periodic_carbon_update(self):
        while self._running:
            try:
                if self.carbon_manager:
                    await self._update_carbon_from_manager()
                else:
                    await self._fetch_carbon_intensity()
                await asyncio.sleep(self.config.carbon_cache_ttl_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Periodic carbon update error: {e}")
                await asyncio.sleep(60)

    async def _update_carbon_from_manager(self):
        """Update carbon intensity from central manager using available method."""
        if hasattr(self.carbon_manager, 'get_current_intensity'):
            intensity = await self.carbon_manager.get_current_intensity()
            async with self._cache_lock:
                self._carbon_cache["global"] = (intensity, datetime.now(timezone.utc))
        elif hasattr(self.carbon_manager, 'update'):
            result = await self.carbon_manager.update()
            intensity = result.get('intensity', 400) if isinstance(result, dict) else 400
            async with self._cache_lock:
                self._carbon_cache["global"] = (intensity, datetime.now(timezone.utc))
        else:
            logger.debug("Carbon manager has no update method; using default.")

    async def _get_session(self) -> aiohttp.ClientSession:
        if not AIOHTTP_AVAILABLE:
            raise RuntimeError("aiohttp not installed")
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()
            return self._session

    async def _fetch_carbon_intensity(self, region: str = "us-east") -> float:
        if not self.config.enable_real_time_carbon:
            return self.config.default_carbon_intensity_g_per_kwh

        async with self._cache_lock:
            if region in self._carbon_cache:
                value, timestamp = self._carbon_cache[region]
                if (datetime.now(timezone.utc) - timestamp).total_seconds() < self.config.carbon_cache_ttl_seconds:
                    return value

        if not AIOHTTP_AVAILABLE:
            logger.warning("aiohttp not available; using default carbon intensity.")
            return self.config.default_carbon_intensity_g_per_kwh

        async def _fetch():
            session = await self._get_session()
            url = f"{self.config.carbon_api_url}?zone={region}"
            headers = {}
            if self.config.carbon_api_key:
                headers['auth-token'] = self.config.carbon_api_key
            async with session.get(url, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    intensity = data.get('data', {}).get('carbonIntensity', 400)
                    return intensity
                else:
                    raise aiohttp.ClientError(f"Carbon API returned {resp.status}")

        try:
            intensity = await self._circuit_breaker.call(_fetch)
            async with self._cache_lock:
                self._carbon_cache[region] = (intensity, datetime.now(timezone.utc))
            logger.debug(f"Fetched carbon intensity for {region}: {intensity} g/kWh")
            return intensity
        except Exception as e:
            logger.warning(f"Failed to fetch carbon intensity, using default: {e}")
            return self.config.default_carbon_intensity_g_per_kwh

    async def _get_helium_scarcity(self) -> float:
        if self.helium_optimizer:
            try:
                status = await self.helium_optimizer.get_helium_status()
                return status.get('price', 0.5) * 2
            except:
                pass
        return self.config.helium_scarcity_factor

    # ==========================================================================
    # Core Expert Interface
    # ==========================================================================
    async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        task_type = task.get('type', 'unknown')
        task_id = task.get('correlation_id', str(uuid.uuid4()))

        start_ts = asyncio.get_event_loop().time()
        logger.info(f"EnergyExpert handling task: {task_type} (ID: {task_id})")

        try:
            if task_type == 'energy_estimate':
                result = await self.estimate_task_energy(task)
            elif task_type == 'carbon_profile':
                result = await self.profile_carbon_footprint(task)
            elif task_type == 'helium_analysis':
                result = await self.analyze_helium_impact(task)
            elif task_type == 'sustainability_recommend':
                result = await self.recommend_strategy(task)
            elif task_type == 'energy_route':
                result = await self.route_by_energy(task)
            elif task_type == 'forecast':
                result = await self.forecast_energy(task)
            else:
                result = {'status': 'error', 'error': f"Unknown task type: {task_type}"}

            end_ts = asyncio.get_event_loop().time()
            latency = end_ts - start_ts
            self.tasks_handled += 1
            self.total_latency += latency

            # Fixed task count mapping
            task_map = {
                'energy_estimate': 'estimate',
                'carbon_profile': 'carbon',
                'helium_analysis': 'helium',
                'sustainability_recommend': 'recommend',
                'energy_route': 'route',
                'forecast': 'forecast'
            }
            if task_type in task_map:
                key = task_map[task_type]
                self.task_counts[key] = self.task_counts.get(key, 0) + 1

            # Generic metric recording
            self.metrics.increment("energy_task", 1)
            self.metrics.observe("energy_latency", latency)

            result['correlation_id'] = task_id
            result['latency_seconds'] = latency
            logger.info(f"EnergyExpert completed {task_type}: latency={latency:.3f}s")

            return result

        except Exception as e:
            logger.error(f"EnergyExpert error on {task_type}: {e}", exc_info=True)
            self.metrics.increment("energy_task_error", 1)
            return {'status': 'error', 'error': str(e), 'correlation_id': task_id}

    # ==========================================================================
    # Core Energy Operations (Enhanced with FeedbackEvent and MODP)
    # ==========================================================================
    async def estimate_task_energy(self, task: Dict[str, Any]) -> Dict[str, Any]:
        payload = task.get('payload', {})
        task_id = task.get('correlation_id', str(uuid.uuid4()))
        region = payload.get('region', 'us-east')

        start_ts = asyncio.get_event_loop().time()

        cpu_seconds = payload.get('cpu_seconds', 1.0)
        memory_gb = payload.get('memory_gb', 0.5)
        network_mbps = payload.get('network_mbps', 1.0)
        storage_gb = payload.get('storage_gb', 0.0)
        duration_seconds = payload.get('duration_seconds', cpu_seconds)

        # Idle energy (constant power during duration)
        idle_energy_kwh = (duration_seconds * self.config.idle_power_watt) / 3600.0 / 1000.0

        # Active CPU energy (only CPU, not including memory/network/storage)
        cpu_energy_kwh = (cpu_seconds * self.config.cpu_power_watt) / 3600.0 / 1000.0 * self.config.power_utilization_factor

        # Memory energy
        memory_energy_kwh = (duration_seconds * memory_gb * self.config.memory_power_per_gb) / 3600.0 / 1000.0

        # Network energy
        network_energy_kwh = (network_mbps * duration_seconds * self.config.network_power_per_mbps) / 3600.0 / 1000.0

        # Storage energy
        storage_energy_kwh = (storage_gb * self.config.storage_power_per_gb) / 3600.0 / 1000.0

        # Total energy
        total_energy_kwh = idle_energy_kwh + cpu_energy_kwh + memory_energy_kwh + network_energy_kwh + storage_energy_kwh

        # Carbon intensity
        if self.carbon_manager:
            if hasattr(self.carbon_manager, 'get_current_intensity'):
                carbon_intensity = await self.carbon_manager.get_current_intensity()
            else:
                carbon_intensity = await self._fetch_carbon_intensity(region)
        else:
            carbon_intensity = await self._fetch_carbon_intensity(region)

        carbon_g = total_energy_kwh * carbon_intensity * 1000.0
        helium_ml = total_energy_kwh * 100.0

        efficiency_score = max(0.0, min(1.0, 1.0 - (total_energy_kwh / 0.1)))
        sustainability_score = (
            0.4 * efficiency_score +
            0.3 * max(0.0, 1.0 - (carbon_g / 100.0)) +
            0.3 * max(0.0, 1.0 - (helium_ml / 100.0))
        )
        if sustainability_score > 0.8:
            recommended_strategy = "performance"
        elif sustainability_score > 0.5:
            recommended_strategy = "balanced"
        else:
            recommended_strategy = "conservative"

        # XAI explanation
        explanation = {
            'energy_breakdown_kwh': {
                'idle': idle_energy_kwh,
                'cpu': cpu_energy_kwh,
                'memory': memory_energy_kwh,
                'network': network_energy_kwh,
                'storage': storage_energy_kwh
            },
            'carbon_intensity_g_per_kwh': carbon_intensity,
            'efficiency_score': efficiency_score,
            'sustainability_score': sustainability_score,
            'reasoning': f"Total energy {total_energy_kwh:.6f} kWh, carbon {carbon_g:.2f} g, helium {helium_ml:.2f} ml. Efficiency based on 0.1 kWh threshold."
        }

        profile = EnergyProfile(
            task_id=task_id,
            estimated_duration_seconds=duration_seconds,
            estimated_idle_energy_kwh=idle_energy_kwh,
            estimated_cpu_energy_kwh=cpu_energy_kwh,
            estimated_memory_energy_kwh=memory_energy_kwh,
            estimated_network_energy_kwh=network_energy_kwh,
            estimated_storage_energy_kwh=storage_energy_kwh,
            estimated_total_energy_kwh=total_energy_kwh,
            carbon_intensity_g_per_kwh=carbon_intensity,
            estimated_carbon_g=carbon_g,
            estimated_helium_ml=helium_ml,
            energy_efficiency_score=efficiency_score,
            sustainability_score=sustainability_score,
            recommended_strategy=recommended_strategy,
            region=region,
            timestamp=datetime.now(timezone.utc).isoformat(),
            explanation=explanation,
        )

        self.energy_profiles[task_id] = profile
        self.task_energy_cache[task_id] = total_energy_kwh

        end_ts = asyncio.get_event_loop().time()
        metrics = EnergyExpertMetrics(
            operation_name="estimate_task_energy",
            start_time=start_ts,
            end_time=end_ts,
            tasks_analyzed=1,
            total_energy_kwh=total_energy_kwh,
            total_carbon_kg=carbon_g / 1000.0,
        )
        self.metrics_history.append(metrics)

        # Generic metric updates
        self.metrics.set("energy_consumption", total_energy_kwh)
        self.metrics.set("carbon_footprint", carbon_g / 1000.0)

        # Bio-inspired integration
        await self._bio_spend_earn(total_energy_kwh, carbon_g, sustainability_score, "energy_estimate")

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"energy_estimate_{task_id}",
            selected_action="estimate",
            quality_score=sustainability_score,
            energy_joules=total_energy_kwh * 3.6e6,
            carbon_g=carbon_g,
            feedback_type="energy",
            adaptive_cost_value=0.0,
            state={'task_id': task_id, 'region': region, 'explanation': explanation},
            candidates=[{'action': 'estimate', 'carbon', 'helium', 'recommend', 'route', 'forecast'}],
            source="energy_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["energy", "estimate"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        # Check drift
        await self._check_drift()

        return {'status': 'success', 'task_id': task_id, 'profile': profile.to_dict()}

    async def profile_carbon_footprint(self, task: Dict[str, Any]) -> Dict[str, Any]:
        payload = task.get('payload', {})
        task_id = task.get('correlation_id', str(uuid.uuid4()))

        baseline_carbon_g = payload.get('baseline_carbon_g', 50.0)
        offset_strategy = payload.get('offset_strategy', 'purchase_offset')

        offset_carbon_g = 0.0
        if offset_strategy == 'renewable_swap':
            offset_carbon_g = baseline_carbon_g * 0.8
        elif offset_strategy == 'purchase_offset':
            offset_carbon_g = baseline_carbon_g * 0.5

        net_carbon_g = baseline_carbon_g - offset_carbon_g
        cost_usd = net_carbon_g * 0.00001 if offset_strategy else 0.0
        roi_factor = baseline_carbon_g / max(net_carbon_g, 0.1)

        footprint = CarbonFootprint(
            baseline_carbon_g=baseline_carbon_g,
            offset_strategy=offset_strategy,
            offset_carbon_g=offset_carbon_g,
            net_carbon_g=net_carbon_g,
            cost_usd=cost_usd,
            roi_factor=roi_factor,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

        self.carbon_footprints[task_id] = footprint

        # Bio-inspired integration: ATP spend/earn
        await self._bio_spend_earn(0.0, net_carbon_g, 0.8 if net_carbon_g < baseline_carbon_g else 0.5, "carbon_profile")

        event = FeedbackEvent.create_with_context(
            task_id=f"energy_carbon_{task_id}",
            selected_action="carbon_profile",
            quality_score=1.0 if net_carbon_g < baseline_carbon_g else 0.5,
            energy_joules=0.0,
            carbon_g=net_carbon_g,
            feedback_type="energy",
            adaptive_cost_value=0.0,
            state={'task_id': task_id, 'strategy': offset_strategy},
            candidates=[{'action': 'estimate', 'carbon', 'helium', 'recommend', 'route', 'forecast'}],
            source="energy_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["energy", "carbon"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        await self._check_drift()

        return {'status': 'success', 'task_id': task_id, 'footprint': footprint.to_dict()}

    async def analyze_helium_impact(self, task: Dict[str, Any]) -> Dict[str, Any]:
        payload = task.get('payload', {})
        task_id = task.get('correlation_id', str(uuid.uuid4()))

        required_ml = payload.get('required_ml', 5.0)
        scarcity = await self._get_helium_scarcity()

        available_ml = 1000.0 / scarcity
        recovery_potential_ml = required_ml * self.config.helium_recovery_efficiency
        can_proceed = available_ml >= required_ml

        if can_proceed:
            recommendation = "Sufficient helium available; proceed normally"
        else:
            recommendation = "Low helium; enable recovery or defer non-critical tasks"

        analysis = HeliumAnalysis(
            available_ml=available_ml,
            required_ml=required_ml,
            scarcity_factor=scarcity,
            recovery_potential_ml=recovery_potential_ml,
            can_proceed=can_proceed,
            recommendation=recommendation,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

        self.helium_analyses[task_id] = analysis

        # Bio-inspired integration
        await self._bio_spend_earn(0.0, 0.0, 1.0 if can_proceed else 0.3, "helium_analysis")

        event = FeedbackEvent.create_with_context(
            task_id=f"energy_helium_{task_id}",
            selected_action="helium_analysis",
            quality_score=1.0 if can_proceed else 0.3,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="energy",
            adaptive_cost_value=0.0,
            state={'task_id': task_id, 'required_ml': required_ml},
            candidates=[{'action': 'estimate', 'carbon', 'helium', 'recommend', 'route', 'forecast'}],
            source="energy_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["energy", "helium"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        await self._check_drift()

        return {'status': 'success', 'task_id': task_id, 'analysis': analysis.to_dict()}

    async def recommend_strategy(self, task: Dict[str, Any]) -> Dict[str, Any]:
        payload = task.get('payload', {})
        system_load = payload.get('system_load', 0.5)
        energy_budget = payload.get('energy_budget', 100.0)
        carbon_budget = payload.get('carbon_budget', 1000.0)
        helium_availability = payload.get('helium_availability', 0.7)

        strategies = ['conservative', 'balanced', 'performance', 'renewable_only']
        candidates = []
        for strategy in strategies:
            if strategy == 'conservative':
                quality = 0.9
                carbon_g = 5.0
                latency_ms = 80.0
                energy_joules = 50.0
            elif strategy == 'balanced':
                quality = 0.8
                carbon_g = 10.0
                latency_ms = 50.0
                energy_joules = 70.0
            elif strategy == 'performance':
                quality = 0.6
                carbon_g = 20.0
                latency_ms = 30.0
                energy_joules = 100.0
            elif strategy == 'renewable_only':
                quality = 0.7
                carbon_g = 2.0
                latency_ms = 90.0
                energy_joules = 40.0
            else:
                quality = 0.5
                carbon_g = 10.0
                latency_ms = 60.0
                energy_joules = 60.0

            cost = self.adaptive_cost.compute(
                quality=quality,
                carbon_g=carbon_g,
                latency_ms=latency_ms,
                energy_joules=energy_joules,
                health=True,
                atp=0.5
            )
            candidates.append({
                'strategy': strategy,
                'score': cost,
                'carbon_g': carbon_g,
                'latency_ms': latency_ms,
                'energy_joules': energy_joules,
                'quality_score': quality
            })

        if self.pareto:
            filtered = self.pareto.filter(candidates)
            if filtered:
                allowed = {c['strategy'] for c in filtered}
                candidates = [c for c in candidates if c['strategy'] in allowed]

        if not candidates:
            strategy = 'balanced'
            reason = "No candidates passed Pareto filter; using balanced default"
            explanation = {'reason': reason}
        else:
            best = max(candidates, key=lambda x: x['score'])
            strategy = best['strategy']
            reason = f"Selected by adaptive cost and Pareto optimization"
            explanation = {
                'reason': reason,
                'score': best['score'],
                'quality_score': best['quality_score'],
                'carbon_g': best['carbon_g'],
                'energy_joules': best['energy_joules'],
                'latency_ms': best['latency_ms']
            }

        # Temporal safety: avoid frequent repetition of the same strategy
        now = datetime.now(timezone.utc)
        self.recent_strategy_selections.append((strategy, now))
        if len(self.recent_strategy_selections) >= 3:
            recent = [s for s, t in self.recent_strategy_selections if (now - t).total_seconds() < 300]
            if len(recent) >= 3 and all(s == strategy for s in recent):
                logger.warning(f"Strategy {strategy} selected frequently; consider adding variety.")
                explanation['warning'] = "Strategy selected frequently"

        # Bio-inspired integration
        await self._bio_spend_earn(0.0, 0.0, 0.9 if strategy in ['balanced', 'conservative'] else 0.7, "recommend_strategy")

        event = FeedbackEvent.create_with_context(
            task_id=f"energy_recommend_{uuid.uuid4().hex[:8]}",
            selected_action="recommend",
            quality_score=0.9 if strategy in ['balanced', 'conservative'] else 0.7,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="energy",
            adaptive_cost_value=0.0,
            state={'system_load': system_load, 'strategy': strategy, 'explanation': explanation},
            candidates=[{'action': 'estimate', 'carbon', 'helium', 'recommend', 'route', 'forecast'}],
            source="energy_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["energy", "recommend"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        await self._check_drift()

        return {
            'status': 'success',
            'recommended_strategy': strategy,
            'reason': reason,
            'explanation': explanation,
            'details': {
                'system_load': system_load,
                'energy_budget_remaining': energy_budget,
                'carbon_budget_remaining': carbon_budget,
                'helium_availability': helium_availability,
            },
        }

    async def route_by_energy(self, task: Dict[str, Any]) -> Dict[str, Any]:
        payload = task.get('payload', {})
        energy_kwh = payload.get('energy_kwh', 0.1)
        carbon_g = payload.get('carbon_g', 50.0)

        routes = ['cpu_expert', 'optimization_expert', 'io_expert']
        candidates = []
        for route in routes:
            if route == 'cpu_expert':
                quality = 0.8
                carbon = carbon_g * 0.5
                latency = 60.0
                energy = energy_kwh * 0.6
            elif route == 'optimization_expert':
                quality = 0.7
                carbon = carbon_g
                latency = 120.0
                energy = energy_kwh
            else:  # io_expert
                quality = 0.6
                carbon = carbon_g * 0.3
                latency = 30.0
                energy = energy_kwh * 0.3

            cost = self.adaptive_cost.compute(
                quality=quality,
                carbon_g=carbon,
                latency_ms=latency,
                energy_joules=energy,
                health=True,
                atp=0.5
            )
            candidates.append({
                'expert': route,
                'score': cost,
                'carbon_g': carbon,
                'latency_ms': latency,
                'energy_joules': energy,
                'quality_score': quality
            })

        if self.pareto:
            filtered = self.pareto.filter(candidates)
            if filtered:
                allowed = {c['expert'] for c in filtered}
                candidates = [c for c in candidates if c['expert'] in allowed]

        if candidates:
            best = max(candidates, key=lambda x: x['score'])
            routing = {r: False for r in routes}
            routing[best['expert']] = True
            recommended = [best['expert']]
            explanation = {
                'reason': f"Chosen {best['expert']} due to highest adaptive cost after Pareto filtering.",
                'score': best['score'],
                'quality_score': best['quality_score'],
                'carbon_g': best['carbon_g'],
                'energy_joules': best['energy_joules'],
                'latency_ms': best['latency_ms']
            }
        else:
            routing = {'cpu_expert': True, 'optimization_expert': False, 'io_expert': False}
            recommended = ['cpu_expert']
            explanation = {'reason': "No candidates passed Pareto filter; using default cpu_expert"}

        # Temporal safety: avoid frequent repetition of the same route
        now = datetime.now(timezone.utc)
        self.recent_route_selections.append((recommended[0], now))
        if len(self.recent_route_selections) >= 3:
            recent = [r for r, t in self.recent_route_selections if (now - t).total_seconds() < 300]
            if len(recent) >= 3 and all(r == recommended[0] for r in recent):
                logger.warning(f"Route {recommended[0]} selected frequently; consider load balancing.")
                explanation['warning'] = "Route selected frequently"

        await self._bio_spend_earn(energy_kwh, carbon_g, 0.8, "route_by_energy")

        event = FeedbackEvent.create_with_context(
            task_id=f"energy_route_{uuid.uuid4().hex[:8]}",
            selected_action="route",
            quality_score=0.9,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="energy",
            adaptive_cost_value=0.0,
            state={'energy_kwh': energy_kwh, 'carbon_g': carbon_g, 'explanation': explanation},
            candidates=[{'action': 'estimate', 'carbon', 'helium', 'recommend', 'route', 'forecast'}],
            source="energy_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["energy", "route"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        await self._check_drift()

        return {
            'status': 'success',
            'routing': routing,
            'recommended_experts': recommended,
            'explanation': explanation
        }

    async def forecast_energy(self, task: Dict[str, Any]) -> Dict[str, Any]:
        payload = task.get('payload', {})
        current_load = payload.get('current_load', 0.5)
        forecast_hours = payload.get('forecast_hours', self.config.forecast_window_hours)

        if self.tick_engine:
            try:
                forecast = await self.tick_engine.get_energy_forecast(forecast_hours)
                event = FeedbackEvent.create_with_context(
                    task_id=f"energy_forecast_{uuid.uuid4().hex[:8]}",
                    selected_action="forecast",
                    quality_score=0.9,
                    energy_joules=0.0,
                    carbon_g=0.0,
                    feedback_type="energy",
                    adaptive_cost_value=0.0,
                    state={'horizon_hours': forecast_hours},
                    candidates=[{'action': 'estimate', 'carbon', 'helium', 'recommend', 'route', 'forecast'}],
                    source="energy_expert",
                    environment=getattr(central_config, "ENVIRONMENT", "production"),
                    tags=["energy", "forecast"]
                )
                await self.queue.publish("feedback_events", event.to_json())
                return {'status': 'success', 'forecast': forecast, 'horizon_hours': forecast_hours, 'source': 'tick_engine'}
            except Exception as e:
                logger.warning(f"TimeTickEngine forecast failed: {e}")

        forecast = []
        for hour in range(forecast_hours):
            variation = 0.1 * np.sin(hour / 6.0)
            load = current_load + variation
            energy_kwh = load * 50.0 / 1000.0
            forecast.append({
                'hour': hour,
                'predicted_load': max(0.0, min(1.0, load)),
                'predicted_energy_kwh': energy_kwh,
            })

        event = FeedbackEvent.create_with_context(
            task_id=f"energy_forecast_{uuid.uuid4().hex[:8]}",
            selected_action="forecast",
            quality_score=0.6,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="energy",
            adaptive_cost_value=0.0,
            state={'horizon_hours': forecast_hours},
            candidates=[{'action': 'estimate', 'carbon', 'helium', 'recommend', 'route', 'forecast'}],
            source="energy_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["energy", "forecast"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        return {'status': 'success', 'forecast': forecast, 'horizon_hours': forecast_hours, 'source': 'fallback'}

    # ==========================================================================
    # Bio-inspired helper methods
    # ==========================================================================
    async def _bio_spend_earn(self, energy_kwh: float, carbon_g: float, quality_score: float, operation_name: str):
        """Spend ATP before operation and earn based on quality."""
        if not self.token_manager:
            return
        try:
            atp_cost = max(0.01, energy_kwh * 0.1 + carbon_g * 0.001)
            await self.token_manager.spend("energy_expert", atp_cost)
            if quality_score > 0.7:
                await self.token_manager.earn("energy_expert", atp_cost * 2)
            if self.gradient_manager:
                trust_delta = 0.05 if quality_score > 0.7 else -0.05
                self.gradient_manager.pump_field('trust', trust_delta, source=f"energy_{operation_name}")
                if carbon_g > 50:
                    self.gradient_manager.pump_field('carbon', 0.1, source=f"energy_{operation_name}")
        except Exception as e:
            logger.debug(f"Bio integration failed: {e}")

    # ==========================================================================
    # Drift detection and adaptation
    # ==========================================================================
    async def _check_drift(self):
        if self.drift:
            try:
                drift_score = await self.drift.check_drift(self.adaptive_cost.get_current_weights())
                if drift_score and drift_score > 0.7:
                    logger.warning(f"High drift detected ({drift_score:.3f}) in EnergyExpert.")
                    self.config.energy_efficiency_threshold = min(0.9, self.config.energy_efficiency_threshold + 0.05)
                    self.config.carbon_budget_per_task_g *= 0.9
            except Exception as e:
                logger.warning(f"Drift check failed: {e}")

    # ==========================================================================
    # Expert Interface Methods
    # ==========================================================================
    def get_capabilities(self) -> Dict[str, Any]:
        return {
            'expert_name': self.expert_name,
            'supported_tasks': self.supported_task_types,
            'health_status': self.health_status,
            'avg_latency_seconds': self.total_latency / self.tasks_handled if self.tasks_handled > 0 else 0.0,
            'tasks_handled': self.tasks_handled,
            'config': asdict(self.config),
        }

    def get_metrics(self) -> Dict[str, Any]:
        total_carbon = sum(cf.net_carbon_g for cf in self.carbon_footprints.values()) / 1000.0
        total_energy = sum(ep.estimated_total_energy_kwh for ep in self.energy_profiles.values())
        total_helium = sum(ha.available_ml for ha in self.helium_analyses.values())
        failures = sum(1 for m in self.metrics_history if not m.success)
        return {
            'expert_name': self.expert_name,
            'tasks_handled': self.tasks_handled,
            'avg_latency_seconds': self.total_latency / self.tasks_handled if self.tasks_handled > 0 else 0.0,
            'total_carbon_kg': total_carbon,
            'total_energy_kwh': total_energy,
            'total_helium_ml': total_helium,
            'failure_rate': failures / len(self.metrics_history) if self.metrics_history else 0.0,
            'profiles_cached': len(self.energy_profiles),
            'task_counts': self.task_counts,
        }

    async def get_health_status(self) -> Dict[str, Any]:
        """
        Lightweight health check: just verify internal state, no side effects.
        """
        try:
            # Simple validation: check if necessary components exist and config is valid
            if self.config.cpu_power_watt <= 0:
                raise ValueError("Invalid cpu_power_watt")
            if self.config.idle_power_watt < 0:
                raise ValueError("Invalid idle_power_watt")
            # If carbon manager exists, verify it has expected method (without calling)
            if self.carbon_manager and not (hasattr(self.carbon_manager, 'get_current_intensity') or hasattr(self.carbon_manager, 'update')):
                logger.warning("Carbon manager missing update methods")
            self.health_status = "healthy"
            return {
                'status': 'healthy',
                'expert': self.expert_name,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'tasks_handled': self.tasks_handled,
                'last_error': None
            }
        except Exception as e:
            self.health_status = "unhealthy"
            logger.warning(f"EnergyExpert health check failed: {e}")
            return {
                'status': 'unhealthy',
                'expert': self.expert_name,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'error': str(e)
            }

    # ==========================================================================
    # Async Context Manager and Cleanup
    # ==========================================================================
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        self._running = False
        for task in self._background_tasks:
            if task:
                task.cancel()
        await asyncio.gather(*[t for t in self._background_tasks if t], return_exceptions=True)
        if self._session and not self._session.closed:
            await self._session.close()
        await self._save_state()
        logger.info("EnergyExpert closed")
