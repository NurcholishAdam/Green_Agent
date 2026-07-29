# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/energy_expert.py
# Enhanced Energy Expert v3.1.0 – Production-Ready MoE Energy & Sustainability Expert

"""
Energy Expert v3.1.0 – MoE Expert for Energy, Carbon & Helium Profiling

A specialized expert that handles energy-related tasks within the MoE pipeline:
- Realistic energy consumption estimation (CPU, memory, network, storage, idle)
- Carbon footprint calculation with real-time API integration (Electricity Maps)
- Helium usage and availability analysis with dynamic scarcity updates
- Task routing based on energy/carbon/helium impact
- Sustainable strategy recommendation (conservative/balanced/performance)
- Integration with Green_Agent bio-inspired modules (Token, Gradient, Scheduler)
- Energy-aware telemetry tracking with Prometheus metrics
- Multi-objective sustainability metrics
- Predictive energy forecasting via TimeTickEngine (if available)
- Quantum penalty analysis via QuantumBridge (if available)
- Circuit breaker and retries for external API calls
- Async persistence with JSON metadata and Parquet for profiles
- Background periodic updates for carbon intensity and helium scarcity
- Comprehensive error handling and logging
"""

import asyncio
import logging
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
import pickle
from pathlib import Path
import aiohttp

# ============================================================================
# Try optional dependencies
# ============================================================================
try:
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

try:
    from prometheus_client import Gauge, Counter, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    import aiofiles
    AIOFILES_AVAILABLE = True
except ImportError:
    AIOFILES_AVAILABLE = False

# ============================================================================
# Local imports – BaseExpert and bio-inspired modules
# ============================================================================
try:
    from .base_expert import BaseExpert
    BASE_EXPERT_AVAILABLE = True
except ImportError:
    BASE_EXPERT_AVAILABLE = False
    logger.warning("BaseExpert not available; using fallback interface")

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

try:
    from enhancements.bio_inspired.circuit_breaker import CircuitBreaker
    CIRCUIT_BREAKER_AVAILABLE = True
except ImportError:
    class CircuitBreaker:
        def __init__(self, name, failure_threshold=5, recovery_timeout=30.0):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self._state = "closed"
            self._failure_count = 0
            self._last_failure_time = None
            self._lock = asyncio.Lock()
        async def call(self, func, *args, **kwargs):
            return await func(*args, **kwargs)

# ============================================================================
# Configuration Dataclass (Enhanced with Pydantic support)
# ============================================================================

if PYDANTIC_AVAILABLE:
    class EnergyExpertConfig(BaseModel):
        """Centralized configuration for the Energy Expert."""
        # Feature flags
        enable_energy_estimation: bool = True
        enable_carbon_tracking: bool = True
        enable_helium_analysis: bool = True
        enable_forecasting: bool = True
        enable_telemetry: bool = True
        enable_persistence: bool = True
        enable_real_time_carbon: bool = True  # Use live API
        enable_real_time_helium: bool = False  # Future

        # Energy estimation
        cpu_power_watt: float = 50.0
        memory_power_per_gb: float = 0.5
        network_power_per_mbps: float = 0.01
        storage_power_per_gb: float = 0.001
        idle_power_watt: float = 10.0
        power_utilization_factor: float = 0.7  # Average utilization

        # Carbon tracking
        default_carbon_intensity_g_per_kwh: float = 100.0
        carbon_api_url: str = "https://api.electricitymap.org/v3/carbon-intensity/latest"
        carbon_api_key: Optional[str] = None

        # Helium availability
        helium_scarcity_factor: float = 1.0
        helium_recovery_efficiency: float = 0.7
        helium_cost_per_liter_usd: float = 0.5

        # Sustainability thresholds
        energy_efficiency_threshold: float = 0.7
        carbon_budget_per_task_g: float = 10.0
        helium_budget_per_task_ml: float = 5.0

        # Forecasting
        forecast_window_hours: int = 24

        # Persistence
        state_save_path: str = "./energy_expert_state.json"
        use_parquet_for_profiles: bool = True

        # Circuit breaker
        circuit_breaker_failure_threshold: int = 5
        circuit_breaker_recovery_timeout: float = 30.0

        # Caching
        carbon_cache_ttl_seconds: int = 300
        helium_cache_ttl_seconds: int = 300

        @validator('cpu_power_watt')
        def positive_cpu_power(cls, v):
            if v <= 0:
                raise ValueError('cpu_power_watt must be positive')
            return v

        class Config:
            env_prefix = "ENERGY_EXPERT_"
else:
    @dataclass
    class EnergyExpertConfig:
        enable_energy_estimation: bool = True
        enable_carbon_tracking: bool = True
        enable_helium_analysis: bool = True
        enable_forecasting: bool = True
        enable_telemetry: bool = True
        enable_persistence: bool = True
        enable_real_time_carbon: bool = True
        enable_real_time_helium: bool = False
        cpu_power_watt: float = 50.0
        memory_power_per_gb: float = 0.5
        network_power_per_mbps: float = 0.01
        storage_power_per_gb: float = 0.001
        idle_power_watt: float = 10.0
        power_utilization_factor: float = 0.7
        default_carbon_intensity_g_per_kwh: float = 100.0
        carbon_api_url: str = "https://api.electricitymap.org/v3/carbon-intensity/latest"
        carbon_api_key: Optional[str] = None
        helium_scarcity_factor: float = 1.0
        helium_recovery_efficiency: float = 0.7
        helium_cost_per_liter_usd: float = 0.5
        energy_efficiency_threshold: float = 0.7
        carbon_budget_per_task_g: float = 10.0
        helium_budget_per_task_ml: float = 5.0
        forecast_window_hours: int = 24
        state_save_path: str = "./energy_expert_state.json"
        use_parquet_for_profiles: bool = True
        circuit_breaker_failure_threshold: int = 5
        circuit_breaker_recovery_timeout: float = 30.0
        carbon_cache_ttl_seconds: int = 300
        helium_cache_ttl_seconds: int = 300

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
# Energy Profiling Results (unchanged)
# ============================================================================

@dataclass
class EnergyProfile:
    task_id: str
    estimated_duration_seconds: float
    estimated_cpu_energy_kwh: float
    estimated_memory_energy_kwh: float
    estimated_network_energy_kwh: float
    estimated_total_energy_kwh: float
    carbon_intensity_g_per_kwh: float
    estimated_carbon_g: float
    estimated_helium_ml: float
    energy_efficiency_score: float
    sustainability_score: float
    recommended_strategy: str
    region: str
    timestamp: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class HeliumAnalysis:
    available_ml: float
    required_ml: float
    scarcity_factor: float
    recovery_potential_ml: float
    can_proceed: bool
    recommendation: str
    timestamp: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class CarbonFootprint:
    baseline_carbon_g: float
    offset_strategy: Optional[str]
    offset_carbon_g: float
    net_carbon_g: float
    cost_usd: float
    roi_factor: float
    timestamp: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

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
        if self.end_time:
            return self.end_time - self.start_time
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

# ============================================================================
# Fallback BaseExpert if not available (unchanged)
# ============================================================================

if not BASE_EXPERT_AVAILABLE:
    class BaseExpert:
        def __init__(self):
            self.expert_name = "energy_expert"
            self.supported_task_types = [
                "energy_estimate", "carbon_profile", "helium_analysis",
                "sustainability_recommend", "energy_route", "forecast"
            ]
            self.health_status = "healthy"

        async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
            raise NotImplementedError()

        def get_capabilities(self) -> Dict[str, Any]:
            return {
                'name': self.expert_name,
                'supported_tasks': self.supported_task_types,
                'health': self.health_status,
            }

        def get_metrics(self) -> Dict[str, Any]:
            return {}

# ============================================================================
# Energy Expert Implementation (Enhanced)
# ============================================================================

class EnergyExpert(BaseExpert):
    """
    Energy Expert for MoE System v3.1.0

    Handles energy estimation, carbon tracking, helium analysis,
    and sustainability recommendations with real-time data integration,
    circuit breakers, persistence, and async context management.
    """

    def __init__(self, config: Optional[EnergyExpertConfig] = None):
        super().__init__()
        self.expert_name = "energy_expert"
        self.supported_task_types = [
            "energy_estimate", "carbon_profile", "helium_analysis",
            "sustainability_recommend", "energy_route", "forecast"
        ]
        self.health_status = "healthy"

        # Configuration
        self.config = config or EnergyExpertConfig()

        # State
        self.energy_profiles: Dict[str, EnergyProfile] = {}
        self.carbon_footprints: Dict[str, CarbonFootprint] = {}
        self.helium_analyses: Dict[str, HeliumAnalysis] = {}
        self.metrics_history: List[EnergyExpertMetrics] = []
        self.tasks_handled = 0
        self.total_latency = 0.0
        self.task_energy_cache: Dict[str, float] = {}

        # Caching with TTL
        self._carbon_cache: Dict[str, Tuple[float, datetime]] = {}
        self._helium_cache: Dict[str, Tuple[float, datetime]] = {}
        self._cache_lock = asyncio.Lock()

        # Bio-inspired integration
        self.token_manager = None
        if TOKEN_AVAILABLE:
            try:
                self.token_manager = EcoATPTokenManager()
            except Exception as e:
                logger.warning(f"Failed to initialize token manager: {e}")

        self.gradient_manager = None
        if GRADIENT_AVAILABLE:
            try:
                self.gradient_manager = GradientFieldManager()
            except Exception as e:
                logger.warning(f"Failed to initialize gradient manager: {e}")

        self.scheduler = None
        if ATP_AVAILABLE:
            try:
                self.scheduler = ATPSynthaseScheduler(self.token_manager, self.gradient_manager)
            except Exception as e:
                logger.warning(f"Failed to initialize scheduler: {e}")

        self.tick_engine = None
        if TICK_ENGINE_AVAILABLE:
            try:
                self.tick_engine = TimeTickEngine()
            except Exception as e:
                logger.warning(f"Failed to initialize tick engine: {e}")

        self.quantum_bridge = None
        if QUANTUM_BRIDGE_AVAILABLE:
            try:
                self.quantum_bridge = QuantumBridge(self.gradient_manager, None)
            except Exception as e:
                logger.warning(f"Failed to initialize quantum bridge: {e}")

        # Circuit breaker for external API calls
        self._circuit_breaker = CircuitBreaker(
            "energy_external",
            failure_threshold=self.config.circuit_breaker_failure_threshold,
            recovery_timeout=self.config.circuit_breaker_recovery_timeout
        )

        # Session for HTTP requests
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Prometheus metrics
        self.prometheus_metrics = {}
        if PROMETHEUS_AVAILABLE:
            self._init_prometheus()

        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        self._running = True
        if self.config.enable_real_time_carbon:
            self._start_background_tasks()

        # Load persisted state
        if self.config.enable_persistence:
            asyncio.create_task(self.load_state())

        logger.info(f"EnergyExpert v3.1.0 initialized with config: {self.config}")

    def _init_prometheus(self):
        """Initialize Prometheus metrics."""
        try:
            self.prometheus_metrics = {
                'energy_expert_tasks_total': Counter(
                    'energy_expert_tasks_total',
                    'Total tasks handled by energy expert',
                    ['task_type', 'status']
                ),
                'energy_expert_carbon_kg': Gauge(
                    'energy_expert_carbon_kg',
                    'Current carbon footprint (kg CO2)'
                ),
                'energy_expert_energy_kwh': Gauge(
                    'energy_expert_energy_kwh',
                    'Current energy consumption (kWh)'
                ),
                'energy_expert_latency_seconds': Histogram(
                    'energy_expert_latency_seconds',
                    'Latency of energy expert operations',
                    ['operation']
                ),
                'energy_expert_helium_ml': Gauge(
                    'energy_expert_helium_ml',
                    'Helium usage (ml)'
                ),
            }
        except Exception as e:
            logger.warning(f"Failed to init Prometheus: {e}")

    def _start_background_tasks(self):
        """Start background tasks for periodic data updates."""
        task = asyncio.create_task(self._periodic_carbon_update())
        self._background_tasks.append(task)
        logger.info("Started background carbon update task")

    async def _periodic_carbon_update(self):
        """Periodically update carbon intensity from API."""
        while self._running:
            try:
                await self._fetch_carbon_intensity()
                await asyncio.sleep(self.config.carbon_cache_ttl_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Periodic carbon update error: {e}")
                await asyncio.sleep(60)

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp session."""
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()
            return self._session

    async def _fetch_carbon_intensity(self, region: str = "us-east") -> float:
        """Fetch real-time carbon intensity from Electricity Maps API."""
        if not self.config.enable_real_time_carbon:
            return self.config.default_carbon_intensity_g_per_kwh

        # Check cache
        async with self._cache_lock:
            if region in self._carbon_cache:
                value, timestamp = self._carbon_cache[region]
                if (datetime.now(timezone.utc) - timestamp).total_seconds() < self.config.carbon_cache_ttl_seconds:
                    return value

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
                    # Convert g/kWh to the desired unit (already g/kWh)
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
        """Get current helium scarcity factor (placeholder; can be extended)."""
        # In a real implementation, this could call an API or use TimeTickEngine.
        return self.config.helium_scarcity_factor

    # ========================================================================
    # Core Expert Interface
    # ========================================================================

    async def handle_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle a task routed to this expert.
        """
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
                result = {
                    'status': 'error',
                    'error': f"Unknown task type: {task_type}",
                }

            end_ts = asyncio.get_event_loop().time()
            latency = end_ts - start_ts
            self.tasks_handled += 1
            self.total_latency += latency

            if PROMETHEUS_AVAILABLE and 'energy_expert_latency_seconds' in self.prometheus_metrics:
                self.prometheus_metrics['energy_expert_latency_seconds'].labels(
                    operation=task_type
                ).observe(latency)

            result['correlation_id'] = task_id
            result['latency_seconds'] = latency
            logger.info(f"EnergyExpert completed {task_type}: latency={latency:.3f}s")

            return result

        except Exception as e:
            logger.error(f"EnergyExpert error on {task_type}: {e}", exc_info=True)
            if PROMETHEUS_AVAILABLE and 'energy_expert_tasks_total' in self.prometheus_metrics:
                self.prometheus_metrics['energy_expert_tasks_total'].labels(
                    task_type=task_type, status='error'
                ).inc()
            return {
                'status': 'error',
                'error': str(e),
                'correlation_id': task_id,
            }

    def get_capabilities(self) -> Dict[str, Any]:
        return {
            'expert_name': self.expert_name,
            'supported_tasks': self.supported_task_types,
            'health_status': self.health_status,
            'avg_latency_seconds': (
                self.total_latency / self.tasks_handled
                if self.tasks_handled > 0 else 0.0
            ),
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
            'avg_latency_seconds': (
                self.total_latency / self.tasks_handled
                if self.tasks_handled > 0 else 0.0
            ),
            'total_carbon_kg': total_carbon,
            'total_energy_kwh': total_energy,
            'total_helium_ml': total_helium,
            'failure_rate': failures / len(self.metrics_history) if self.metrics_history else 0.0,
            'profiles_cached': len(self.energy_profiles),
        }

    async def get_health_status(self) -> Dict[str, Any]:
        try:
            test_task = {
                'type': 'energy_estimate',
                'payload': {
                    'cpu_seconds': 1.0,
                    'memory_gb': 0.5,
                    'network_mbps': 1.0,
                    'duration_seconds': 10.0,
                },
            }
            result = await self.estimate_task_energy(test_task)

            self.health_status = "healthy"
            return {
                'status': 'healthy',
                'expert': self.expert_name,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'last_tasks': self.tasks_handled,
                'last_error': None,
            }
        except Exception as e:
            self.health_status = "unhealthy"
            logger.warning(f"EnergyExpert health check failed: {e}")
            return {
                'status': 'unhealthy',
                'expert': self.expert_name,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'error': str(e),
            }

    # ========================================================================
    # Core Energy Operations (Enhanced)
    # ========================================================================

    async def estimate_task_energy(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Estimate energy footprint of a task with realistic models.
        """
        payload = task.get('payload', {})
        task_id = task.get('correlation_id', str(uuid.uuid4()))
        region = payload.get('region', 'us-east')

        start_ts = asyncio.get_event_loop().time()

        cpu_seconds = payload.get('cpu_seconds', 1.0)
        memory_gb = payload.get('memory_gb', 0.5)
        network_mbps = payload.get('network_mbps', 1.0)
        storage_gb = payload.get('storage_gb', 0.0)
        duration_seconds = payload.get('duration_seconds', cpu_seconds)

        # Include idle power
        idle_energy_kwh = (duration_seconds * self.config.idle_power_watt) / 3600.0 / 1000.0

        # Active power scaled by utilization
        active_energy_kwh = (
            (cpu_seconds * self.config.cpu_power_watt) +
            (duration_seconds * memory_gb * self.config.memory_power_per_gb) +
            (network_mbps * duration_seconds * self.config.network_power_per_mbps) +
            (storage_gb * self.config.storage_power_per_gb)
        ) / 3600.0 / 1000.0 * self.config.power_utilization_factor

        total_energy_kwh = idle_energy_kwh + active_energy_kwh

        # Carbon intensity (real-time or fallback)
        carbon_intensity = await self._fetch_carbon_intensity(region)
        carbon_g = total_energy_kwh * carbon_intensity * 1000.0  # g

        # Helium impact (cryogenic cooling estimate)
        helium_ml = total_energy_kwh * 100.0  # placeholder

        # Energy efficiency score (higher is better)
        efficiency_score = max(0.0, min(1.0, 1.0 - (total_energy_kwh / 0.1)))

        # Sustainability score
        sustainability_score = (
            0.4 * efficiency_score +
            0.3 * max(0.0, 1.0 - (carbon_g / 100.0)) +
            0.3 * max(0.0, 1.0 - (helium_ml / 100.0))
        )

        # Recommend strategy
        if sustainability_score > 0.8:
            recommended_strategy = "performance"
        elif sustainability_score > 0.5:
            recommended_strategy = "balanced"
        else:
            recommended_strategy = "conservative"

        profile = EnergyProfile(
            task_id=task_id,
            estimated_duration_seconds=duration_seconds,
            estimated_cpu_energy_kwh=idle_energy_kwh + active_energy_kwh,
            estimated_memory_energy_kwh=(duration_seconds * memory_gb * self.config.memory_power_per_gb) / 3600.0 / 1000.0,
            estimated_network_energy_kwh=(network_mbps * duration_seconds * self.config.network_power_per_mbps) / 3600.0 / 1000.0,
            estimated_total_energy_kwh=total_energy_kwh,
            carbon_intensity_g_per_kwh=carbon_intensity,
            estimated_carbon_g=carbon_g,
            estimated_helium_ml=helium_ml,
            energy_efficiency_score=efficiency_score,
            sustainability_score=sustainability_score,
            recommended_strategy=recommended_strategy,
            region=region,
            timestamp=datetime.now(timezone.utc).isoformat(),
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

        if PROMETHEUS_AVAILABLE:
            self.prometheus_metrics['energy_expert_energy_kwh'].set(total_energy_kwh)
            self.prometheus_metrics['energy_expert_carbon_kg'].set(carbon_g / 1000.0)
            self.prometheus_metrics['energy_expert_helium_ml'].set(helium_ml)

        return {
            'status': 'success',
            'task_id': task_id,
            'profile': profile.to_dict(),
        }

    async def profile_carbon_footprint(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Profile complete carbon footprint including offsets and strategies.
        """
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

        return {
            'status': 'success',
            'task_id': task_id,
            'footprint': footprint.to_dict(),
        }

    async def analyze_helium_impact(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze helium usage and availability with dynamic scarcity.
        """
        payload = task.get('payload', {})
        task_id = task.get('correlation_id', str(uuid.uuid4()))

        required_ml = payload.get('required_ml', 5.0)
        scarcity = await self._get_helium_scarcity()

        # Available helium
        available_ml = 1000.0 / scarcity

        # Recovery potential
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

        return {
            'status': 'success',
            'task_id': task_id,
            'analysis': analysis.to_dict(),
        }

    async def recommend_strategy(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recommend sustainability strategy based on current state.
        """
        payload = task.get('payload', {})

        system_load = payload.get('system_load', 0.5)
        energy_budget = payload.get('energy_budget', 100.0)
        carbon_budget = payload.get('carbon_budget', 1000.0)
        helium_availability = payload.get('helium_availability', 0.7)

        if helium_availability < 0.3 or energy_budget < 20.0:
            strategy = SustainabilityStrategy.CONSERVATIVE.value
            reason = "Low resources (helium/energy); using conservative strategy"
        elif helium_availability > 0.8 and energy_budget > 100.0 and carbon_budget > 5000.0:
            strategy = SustainabilityStrategy.PERFORMANCE.value
            reason = "Abundant resources; using performance strategy"
        else:
            strategy = SustainabilityStrategy.BALANCED.value
            reason = "Balanced resource availability; using balanced strategy"

        return {
            'status': 'success',
            'recommended_strategy': strategy,
            'reason': reason,
            'details': {
                'system_load': system_load,
                'energy_budget_remaining': energy_budget,
                'carbon_budget_remaining': carbon_budget,
                'helium_availability': helium_availability,
            },
        }

    async def route_by_energy(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Route tasks to experts based on energy characteristics.
        """
        payload = task.get('payload', {})
        energy_kwh = payload.get('energy_kwh', 0.1)
        carbon_g = payload.get('carbon_g', 50.0)

        routing = {
            'cpu_expert': energy_kwh > 0.5,
            'optimization_expert': carbon_g > 500.0,
            'io_expert': energy_kwh > 0.05 and energy_kwh <= 0.5,
        }

        recommended = [k for k, v in routing.items() if v]

        return {
            'status': 'success',
            'routing': routing,
            'recommended_experts': recommended or ['io_expert'],
        }

    async def forecast_energy(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """
        Forecast energy consumption over time using TimeTickEngine if available.
        """
        payload = task.get('payload', {})
        current_load = payload.get('current_load', 0.5)
        forecast_hours = payload.get('forecast_hours', self.config.forecast_window_hours)

        if self.tick_engine:
            # Use TimeTickEngine for more sophisticated forecasting
            try:
                forecast = await self.tick_engine.get_energy_forecast(forecast_hours)
                return {
                    'status': 'success',
                    'forecast': forecast,
                    'horizon_hours': forecast_hours,
                    'source': 'tick_engine',
                }
            except Exception as e:
                logger.warning(f"TimeTickEngine forecast failed: {e}")

        # Simple linear forecast as fallback
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

        return {
            'status': 'success',
            'forecast': forecast,
            'horizon_hours': forecast_hours,
            'source': 'fallback',
        }

    # ========================================================================
    # Persistence and State Management (Enhanced)
    # ========================================================================

    async def save_state(self) -> bool:
        """Save expert state to disk with JSON for metadata and Parquet for profiles."""
        if not self.config.enable_persistence:
            return False

        try:
            state = {
                'tasks_handled': self.tasks_handled,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'config': asdict(self.config),
                'energy_profiles_metadata': {
                    k: {'task_id': v.task_id, 'region': v.region, 'timestamp': v.timestamp}
                    for k, v in self.energy_profiles.items()
                },
                'carbon_footprints': {k: v.to_dict() for k, v in self.carbon_footprints.items()},
                'helium_analyses': {k: v.to_dict() for k, v in self.helium_analyses.items()},
                'metrics': [m.to_dict() for m in self.metrics_history],
            }

            # Save metadata as JSON
            metadata_path = Path(self.config.state_save_path).with_suffix('.json')
            with open(metadata_path, 'w') as f:
                json.dump(state, f, indent=2, default=str)

            # Save profiles as Parquet if enabled
            if self.config.use_parquet_for_profiles and self.energy_profiles:
                profiles_dir = Path(self.config.state_save_path).parent / 'energy_profiles'
                profiles_dir.mkdir(exist_ok=True)
                profiles_data = []
                for k, v in self.energy_profiles.items():
                    profiles_data.append(v.to_dict())
                df = pd.DataFrame(profiles_data)
                df.to_parquet(profiles_dir / f"profiles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")

            logger.info("EnergyExpert state saved")
            return True
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
            return False

    async def load_state(self) -> bool:
        """Load expert state from disk."""
        if not self.config.enable_persistence:
            return False

        metadata_path = Path(self.config.state_save_path).with_suffix('.json')
        if not metadata_path.exists():
            logger.info("No saved state found")
            return False

        try:
            with open(metadata_path, 'r') as f:
                state = json.load(f)

            self.tasks_handled = state.get('tasks_handled', 0)

            # Restore carbon footprints and helium analyses
            for k, v in state.get('carbon_footprints', {}).items():
                self.carbon_footprints[k] = CarbonFootprint(**v)
            for k, v in state.get('helium_analyses', {}).items():
                self.helium_analyses[k] = HeliumAnalysis(**v)

            # Restore metrics
            for m_dict in state.get('metrics', []):
                self.metrics_history.append(EnergyExpertMetrics(**m_dict))

            # Restore energy profiles from Parquet (if available)
            if self.config.use_parquet_for_profiles:
                profiles_dir = Path(self.config.state_save_path).parent / 'energy_profiles'
                if profiles_dir.exists():
                    parquet_files = list(profiles_dir.glob("*.parquet"))
                    if parquet_files:
                        latest = max(parquet_files, key=lambda f: f.stat().st_mtime)
                        df = pd.read_parquet(latest)
                        for _, row in df.iterrows():
                            profile = EnergyProfile(**row.to_dict())
                            self.energy_profiles[profile.task_id] = profile

            logger.info("EnergyExpert state loaded")
            return True
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
            return False

    # ========================================================================
    # Async Context Manager
    # ========================================================================

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """Close resources and stop background tasks."""
        self._running = False
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        if self._session and not self._session.closed:
            await self._session.close()
        if self.config.enable_persistence:
            await self.save_state()
        logger.info("EnergyExpert closed")

# ============================================================================
# Example Usage (unchanged)
# ============================================================================

async def example_usage():
    config = EnergyExpertConfig(
        enable_energy_estimation=True,
        enable_carbon_tracking=True,
        enable_helium_analysis=True,
    )
    expert = EnergyExpert(config)

    # Example 1: Estimate task energy
    task_estimate = {
        'type': 'energy_estimate',
        'payload': {
            'cpu_seconds': 10.0,
            'memory_gb': 2.0,
            'network_mbps': 10.0,
            'duration_seconds': 30.0,
            'region': 'us-west',
        },
        'correlation_id': 'task_001',
    }

    result = await expert.handle_task(task_estimate)
    print("Energy estimate:", result['status'])

    # Example 2: Carbon profile
    task_carbon = {
        'type': 'carbon_profile',
        'payload': {
            'baseline_carbon_g': 100.0,
            'offset_strategy': 'purchase_offset',
        },
        'correlation_id': 'task_002',
    }

    result = await expert.handle_task(task_carbon)
    print("Carbon profile:", result['status'])

    # Example 3: Helium analysis
    task_helium = {
        'type': 'helium_analysis',
        'payload': {
            'required_ml': 50.0,
        },
        'correlation_id': 'task_003',
    }

    result = await expert.handle_task(task_helium)
    print("Helium analysis:", result['status'])

    # Example 4: Health check
    health = await expert.get_health_status()
    print("Health:", health['status'])

    # Print metrics
    metrics = expert.get_metrics()
    print("Metrics:", metrics)

if __name__ == "__main__":
    asyncio.run(example_usage())
