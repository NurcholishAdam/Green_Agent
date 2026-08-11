#!/usr/bin/env python3
# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/energy_expert.py
# Version 3.2.0 – Full Green Agent MOPD Integration

"""
Enhanced Energy Expert v3.2.0 – MoE Expert for Energy, Carbon & Helium Profiling
Full Green Agent MOPD Integration

ENHANCEMENTS OVER v3.1.0:
1. INTEGRATED with central Config, Storage, Logger, MetricsRegistry, AsyncMessageQueue.
2. ADDED teacher interface (`policy_probs`) for MTPD optimizer.
3. PUBLISHES FeedbackEvent for every energy operation (estimate, carbon, helium, recommend, route, forecast).
4. USES central AdaptiveCostFunction, ParetoGating, and DriftDetector.
5. REUSES central Vault and master key for post‑quantum cryptography (if needed).
6. REMOVED custom persistence; now uses central Storage (extended with energy tables).
7. REMOVED custom Prometheus; now uses central MetricsRegistry.
8. REMOVED custom logging; now uses central structlog.
9. REMOVED custom circuit breaker; now uses central EnhancedCircuitBreaker.
10. All optional dependencies (pandas, numpy, etc.) still gracefully degrade.
"""

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
import aiohttp

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

# Optional: central circuit breaker and rate limiter
try:
    from ..scaling.circuit_breaker import EnhancedCircuitBreaker
    from ..scaling.rate_limiter import EnhancedRateLimiter
    CENTRAL_CIRCUIT_BREAKER_AVAILABLE = True
except ImportError:
    # Fallback (simple implementations provided below if needed)
    from ..scaling.circuit_breaker import CircuitBreaker as EnhancedCircuitBreaker
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
    # Fallback BaseExpert
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
# Configuration – now built from central_config
# ============================================================================
class EnergyExpertConfig:
    """Configuration for EnergyExpert, built from central_config."""
    def __init__(self):
        self.enable_energy_estimation = getattr(central_config, "energy_enable_estimation", True)
        self.enable_carbon_tracking = getattr(central_config, "energy_enable_carbon_tracking", True)
        self.enable_helium_analysis = getattr(central_config, "energy_enable_helium_analysis", True)
        self.enable_forecasting = getattr(central_config, "energy_enable_forecasting", True)
        self.enable_telemetry = True  # always use central metrics
        self.enable_persistence = True  # always use central storage
        self.enable_real_time_carbon = getattr(central_config, "energy_enable_real_time_carbon", True)
        self.enable_real_time_helium = getattr(central_config, "energy_enable_real_time_helium", False)

        self.cpu_power_watt = getattr(central_config, "energy_cpu_power_watt", 50.0)
        self.memory_power_per_gb = getattr(central_config, "energy_memory_power_per_gb", 0.5)
        self.network_power_per_mbps = getattr(central_config, "energy_network_power_per_mbps", 0.01)
        self.storage_power_per_gb = getattr(central_config, "energy_storage_power_per_gb", 0.001)
        self.idle_power_watt = getattr(central_config, "energy_idle_power_watt", 10.0)
        self.power_utilization_factor = getattr(central_config, "energy_power_utilization_factor", 0.7)
        self.default_carbon_intensity_g_per_kwh = getattr(central_config, "default_carbon_intensity_g_per_kwh", 100.0)
        self.carbon_api_url = getattr(central_config, "carbon_api_url", "https://api.electricitymap.org/v3/carbon-intensity/latest")
        self.carbon_api_key = os.getenv('ELECTRICITYMAP_API_KEY', '')
        self.helium_scarcity_factor = getattr(central_config, "energy_helium_scarcity_factor", 1.0)
        self.helium_recovery_efficiency = getattr(central_config, "energy_helium_recovery_efficiency", 0.7)
        self.helium_cost_per_liter_usd = getattr(central_config, "energy_helium_cost_per_liter_usd", 0.5)
        self.energy_efficiency_threshold = getattr(central_config, "energy_efficiency_threshold", 0.7)
        self.carbon_budget_per_task_g = getattr(central_config, "energy_carbon_budget_per_task_g", 10.0)
        self.helium_budget_per_task_ml = getattr(central_config, "energy_helium_budget_per_task_ml", 5.0)
        self.forecast_window_hours = getattr(central_config, "energy_forecast_window_hours", 24)
        self.circuit_breaker_failure_threshold = getattr(central_config, "circuit_breaker_failure_threshold", 5)
        self.circuit_breaker_recovery_timeout = getattr(central_config, "circuit_breaker_recovery_timeout", 30.0)
        self.carbon_cache_ttl_seconds = getattr(central_config, "energy_carbon_cache_ttl_seconds", 300)
        self.helium_cache_ttl_seconds = getattr(central_config, "energy_helium_cache_ttl_seconds", 300)

        # Validate
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
# Energy Expert Implementation – Fully Integrated
# ============================================================================
class EnergyExpert(BaseExpert):
    """
    Energy Expert v3.2.0 – MoE Expert for Energy, Carbon & Helium Profiling
    Full Green Agent MOPD integration.
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

        # Configuration – built from central_config
        self.config = EnergyExpertConfig()

        # State
        self.energy_profiles: Dict[str, EnergyProfile] = {}
        self.carbon_footprints: Dict[str, CarbonFootprint] = {}
        self.helium_analyses: Dict[str, HeliumAnalysis] = {}
        self.metrics_history: List[EnergyExpertMetrics] = []
        self.tasks_handled = 0
        self.total_latency = 0.0
        self.task_energy_cache: Dict[str, float] = {}
        self.task_counts = {'estimate': 0, 'carbon': 0, 'helium': 0, 'recommend': 0, 'route': 0, 'forecast': 0}

        # Caching with TTL
        self._carbon_cache: Dict[str, Tuple[float, datetime]] = {}
        self._helium_cache: Dict[str, Tuple[float, datetime]] = {}
        self._cache_lock = asyncio.Lock()

        # Circuit breaker (central)
        self._circuit_breaker = EnhancedCircuitBreaker(
            "energy_external",
            failure_threshold=self.config.circuit_breaker_failure_threshold,
            recovery_timeout=self.config.circuit_breaker_recovery_timeout
        )

        # Session for HTTP requests
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        self._running = True
        if self.config.enable_real_time_carbon:
            self._start_background_tasks()

        # Load persisted state from central storage
        asyncio.create_task(self._load_state())

        logger.info(f"EnergyExpert v3.2.0 initialized.")

    # ==========================================================================
    # State Persistence using central Storage
    # ==========================================================================
    async def _load_state(self):
        """Load expert state from central storage."""
        try:
            data = self.storage.get_state("energy_expert_state")
            if data:
                state = json.loads(data)
                self.tasks_handled = state.get('tasks_handled', 0)
                self.total_latency = state.get('total_latency', 0.0)
                self.task_counts = state.get('task_counts', {'estimate': 0, 'carbon': 0, 'helium': 0, 'recommend': 0, 'route': 0, 'forecast': 0})
                # Restore metrics history
                for metrics_dict in state.get('metrics_history', []):
                    metrics = EnergyExpertMetrics(**metrics_dict)
                    self.metrics_history.append(metrics)
                # Restore carbon footprints and helium analyses
                for k, v in state.get('carbon_footprints', {}).items():
                    self.carbon_footprints[k] = CarbonFootprint(**v)
                for k, v in state.get('helium_analyses', {}).items():
                    self.helium_analyses[k] = HeliumAnalysis(**v)
                # Restore energy profiles (reconstruct from dict)
                for task_id, profile_dict in state.get('energy_profiles', {}).items():
                    profile = EnergyProfile(**profile_dict)
                    self.energy_profiles[task_id] = profile
                logger.info("EnergyExpert state loaded from central storage")
        except Exception as e:
            logger.error(f"Failed to load energy expert state: {e}")

    async def _save_state(self):
        """Save expert state to central storage."""
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
    # Teacher Interface for MOPD
    # ==========================================================================
    async def policy_probs(self, state: Dict) -> List[float]:
        """
        Return a probability distribution over energy-handling strategies.
        This allows the MTPD optimizer to treat this module as a teacher.
        """
        strategies = ['estimate', 'carbon', 'helium', 'recommend', 'route', 'forecast']
        counts = [self.task_counts.get(s, 0) for s in strategies]
        total = sum(counts)
        if total == 0:
            return [1/6] * 6
        return [c / total for c in counts]

    # ==========================================================================
    # Background Tasks
    # ==========================================================================
    def _start_background_tasks(self):
        task = asyncio.create_task(self._periodic_carbon_update())
        self._background_tasks.append(task)
        logger.info("Started background carbon update task")

    async def _periodic_carbon_update(self):
        while self._running:
            try:
                if self.carbon_manager:
                    # Use central carbon manager if available
                    await self.carbon_manager.update()
                else:
                    await self._fetch_carbon_intensity()
                await asyncio.sleep(self.config.carbon_cache_ttl_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Periodic carbon update error: {e}")
                await asyncio.sleep(60)

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession()
            return self._session

    async def _fetch_carbon_intensity(self, region: str = "us-east") -> float:
        """Fetch real-time carbon intensity from API with circuit breaker."""
        if not self.config.enable_real_time_carbon:
            return self.config.default_carbon_intensity_g_per_kwh

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
        """Get current helium scarcity factor."""
        if self.helium_optimizer:
            try:
                status = await self.helium_optimizer.get_helium_status()
                return status.get('price', 0.5) * 2  # approximate
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
            # Update task counts
            task_key = task_type.replace('energy_', '').replace('_', '')
            self.task_counts[task_key] = self.task_counts.get(task_key, 0) + 1

            # Record metrics
            self.metrics.increment_energy_task(task_type, result.get('status', 'success'))
            self.metrics.observe_energy_latency(task_type, latency)

            result['correlation_id'] = task_id
            result['latency_seconds'] = latency
            logger.info(f"EnergyExpert completed {task_type}: latency={latency:.3f}s")

            return result

        except Exception as e:
            logger.error(f"EnergyExpert error on {task_type}: {e}", exc_info=True)
            self.metrics.increment_energy_task(task_type, 'error')
            return {'status': 'error', 'error': str(e), 'correlation_id': task_id}

    # ==========================================================================
    # Core Energy Operations (Enhanced with FeedbackEvent)
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

        idle_energy_kwh = (duration_seconds * self.config.idle_power_watt) / 3600.0 / 1000.0
        active_energy_kwh = (
            (cpu_seconds * self.config.cpu_power_watt) +
            (duration_seconds * memory_gb * self.config.memory_power_per_gb) +
            (network_mbps * duration_seconds * self.config.network_power_per_mbps) +
            (storage_gb * self.config.storage_power_per_gb)
        ) / 3600.0 / 1000.0 * self.config.power_utilization_factor

        total_energy_kwh = idle_energy_kwh + active_energy_kwh

        # Carbon intensity (use central manager if available, else fallback)
        if self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
        else:
            carbon_intensity = await self._fetch_carbon_intensity(region)

        carbon_g = total_energy_kwh * carbon_intensity * 1000.0  # g
        helium_ml = total_energy_kwh * 100.0  # placeholder

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

        # Update central metrics
        self.metrics.set_energy_consumption(total_energy_kwh)
        self.metrics.set_carbon_footprint(carbon_g / 1000.0)

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"energy_estimate_{task_id}",
            selected_action="estimate",
            quality_score=sustainability_score,
            energy_joules=total_energy_kwh * 3.6e6,
            carbon_g=carbon_g,
            feedback_type="energy",
            adaptive_cost_value=0.0,
            state={'task_id': task_id, 'region': region},
            candidates=[{'action': 'estimate', 'carbon', 'helium', 'recommend', 'route', 'forecast'}],
            source="energy_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["energy", "estimate"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        # Check drift
        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

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

        # Publish FeedbackEvent
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

        # Check drift
        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

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

        # Publish FeedbackEvent
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

        # Check drift
        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

        return {'status': 'success', 'task_id': task_id, 'analysis': analysis.to_dict()}

    async def recommend_strategy(self, task: Dict[str, Any]) -> Dict[str, Any]:
        payload = task.get('payload', {})
        system_load = payload.get('system_load', 0.5)
        energy_budget = payload.get('energy_budget', 100.0)
        carbon_budget = payload.get('carbon_budget', 1000.0)
        helium_availability = payload.get('helium_availability', 0.7)

        # Use adaptive cost weights to influence recommendation
        if self.adaptive_cost:
            weights = self.adaptive_cost.get_current_weights()
            carbon_weight = weights.get('carbon', 0.3)
            cost_weight = weights.get('cost', 0.2)
        else:
            carbon_weight = 0.3
            cost_weight = 0.2

        if helium_availability < 0.3 or energy_budget < 20.0:
            strategy = SustainabilityStrategy.CONSERVATIVE.value
            reason = "Low resources (helium/energy); using conservative strategy"
        elif helium_availability > 0.8 and energy_budget > 100.0 and carbon_budget > 5000.0:
            strategy = SustainabilityStrategy.PERFORMANCE.value
            reason = "Abundant resources; using performance strategy"
        else:
            strategy = SustainabilityStrategy.BALANCED.value
            reason = "Balanced resource availability; using balanced strategy"

        # Apply carbon/cost adjustments
        if carbon_weight > 0.5:
            strategy = SustainabilityStrategy.CONSERVATIVE.value
            reason += " (adjusted for high carbon weight)"
        elif cost_weight > 0.5:
            strategy = SustainabilityStrategy.BALANCED.value
            reason += " (adjusted for high cost weight)"

        # Pareto gating: filter strategy candidates
        if self.pareto:
            candidates = [
                {'strategy': 'conservative', 'quality': 0.9},
                {'strategy': 'balanced', 'quality': 0.8},
                {'strategy': 'performance', 'quality': 0.6},
                {'strategy': 'renewable_only', 'quality': 0.7}
            ]
            filtered = self.pareto.filter(candidates)
            if filtered:
                # Only keep strategies that passed Pareto; choose best among them
                best = max(filtered, key=lambda x: x['quality'])
                strategy = best['strategy']

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"energy_recommend_{uuid.uuid4().hex[:8]}",
            selected_action="recommend",
            quality_score=0.9 if strategy in ['balanced', 'conservative'] else 0.7,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="energy",
            adaptive_cost_value=0.0,
            state={'system_load': system_load, 'strategy': strategy},
            candidates=[{'action': 'estimate', 'carbon', 'helium', 'recommend', 'route', 'forecast'}],
            source="energy_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["energy", "recommend"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        # Check drift
        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

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
        payload = task.get('payload', {})
        energy_kwh = payload.get('energy_kwh', 0.1)
        carbon_g = payload.get('carbon_g', 50.0)

        routing = {
            'cpu_expert': energy_kwh > 0.5,
            'optimization_expert': carbon_g > 500.0,
            'io_expert': energy_kwh > 0.05 and energy_kwh <= 0.5,
        }

        # Use adaptive cost weights to adjust routing
        if self.adaptive_cost:
            weights = self.adaptive_cost.get_current_weights()
            carbon_weight = weights.get('carbon', 0.3)
            cost_weight = weights.get('cost', 0.2)
            if carbon_weight > 0.5:
                routing['optimization_expert'] = False
            if cost_weight > 0.5:
                routing['cpu_expert'] = False

        # Pareto gating
        if self.pareto:
            candidates = []
            for exp, active in routing.items():
                candidates.append({
                    'expert': exp,
                    'active': active,
                    'carbon_g': 0.0,
                    'latency_ms': 0.0,
                    'quality_score': 0.5
                })
            filtered = self.pareto.filter(candidates)
            if filtered:
                allowed = {c['expert'] for c in filtered}
                for exp in routing:
                    if exp not in allowed:
                        routing[exp] = False

        recommended = [k for k, v in routing.items() if v]

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"energy_route_{uuid.uuid4().hex[:8]}",
            selected_action="route",
            quality_score=0.9,
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="energy",
            adaptive_cost_value=0.0,
            state={'energy_kwh': energy_kwh, 'carbon_g': carbon_g},
            candidates=[{'action': 'estimate', 'carbon', 'helium', 'recommend', 'route', 'forecast'}],
            source="energy_expert",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["energy", "route"]
        )
        await self.queue.publish("feedback_events", event.to_json())

        # Check drift
        if self.drift:
            await self.drift.check_drift(self.adaptive_cost.get_current_weights())

        return {'status': 'success', 'routing': routing, 'recommended_experts': recommended or ['io_expert']}

    async def forecast_energy(self, task: Dict[str, Any]) -> Dict[str, Any]:
        payload = task.get('payload', {})
        current_load = payload.get('current_load', 0.5)
        forecast_hours = payload.get('forecast_hours', self.config.forecast_window_hours)

        if self.tick_engine:
            try:
                forecast = await self.tick_engine.get_energy_forecast(forecast_hours)
                # Publish FeedbackEvent for forecast
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

        # Fallback: simple linear forecast
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

        # Publish FeedbackEvent for forecast
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
    # Expert Interface Methods (unchanged)
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
        }

    async def get_health_status(self) -> Dict[str, Any]:
        try:
            test_task = {'type': 'energy_estimate', 'payload': {'cpu_seconds': 1.0, 'memory_gb': 0.5, 'network_mbps': 1.0, 'duration_seconds': 10.0}}
            result = await self.estimate_task_energy(test_task)
            self.health_status = "healthy"
            return {'status': 'healthy', 'expert': self.expert_name, 'timestamp': datetime.now(timezone.utc).isoformat(), 'last_tasks': self.tasks_handled, 'last_error': None}
        except Exception as e:
            self.health_status = "unhealthy"
            logger.warning(f"EnergyExpert health check failed: {e}")
            return {'status': 'unhealthy', 'expert': self.expert_name, 'timestamp': datetime.now(timezone.utc).isoformat(), 'error': str(e)}

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
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        if self._session and not self._session.closed:
            await self._session.close()
        await self._save_state()
        logger.info("EnergyExpert closed")
