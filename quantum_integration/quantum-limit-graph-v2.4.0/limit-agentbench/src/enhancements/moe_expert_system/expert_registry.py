#!/usr/bin/env python3
"""
Enhanced Expert Registry v6.4.0 - Complete Bio-Inspired Genome Repository
Full Green Agent MOPD Integration

ENHANCEMENTS OVER v6.3.0:
1. INTEGRATED with central Config, Storage, Logger, MetricsRegistry, AsyncMessageQueue.
2. ADDED teacher interface (`policy_probs`) for MTPD optimizer.
3. PUBLISHES FeedbackEvent for every registration, deprecation, fitness update, natural selection, and reproduction.
4. USES central AdaptiveCostFunction, ParetoGating, and DriftDetector.
5. REMOVED custom persistence; now uses central Storage.
6. REMOVED custom Prometheus; now uses central MetricsRegistry.
7. REMOVED custom logging; now uses central structlog.
8. All optional dependencies (PyTorch, scikit-learn, etc.) still gracefully degrade.
"""

import asyncio
import json
import os
import re
import hashlib
import uuid
import math
import random
import zlib
from collections import defaultdict, deque
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple, Set, Union, Callable, TypeVar
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

# Optional dependencies
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
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
except ImportError:
    def retry(*args, **kwargs):
        return lambda f: f
    stop_after_attempt = lambda x: None
    wait_exponential = lambda **k: None
    retry_if_exception_type = lambda e: None

try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# Bio-inspired modules – optional import
try:
    from enhancements.bio_inspired.eco_atp_currency import (
        EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer,
        TokenState, EcoATPToken, EcoATPAccount
    )
    from enhancements.bio_inspired.proton_gradient_fields import (
        GradientFieldManager, GradientField
    )
    from enhancements.bio_inspired.chromatophore_compartments import (
        CompartmentManager, ChromatophoreCompartment, CompartmentState,
        MembranePermeability, CompartmentResource
    )
    from enhancements.bio_inspired.biomass_storage import (
        BiomassStorage, StorageTier, GuaranteeLevel, StoredTask, StorageToken
    )
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired modules loaded for Expert Registry correlation")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired modules not available: {str(e)}")

# Optional external modules for integration
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

# -----------------------------------------------------------------------------
# Configuration – now uses central_config as a reference.
# We keep a local config class for backward compatibility, but values are pulled
# from central_config with sensible defaults.
# -----------------------------------------------------------------------------
class ExpertRegistryConfig:
    """Configuration for ExpertRegistry, built from central_config."""
    def __init__(self):
        self.registry_id = getattr(central_config, "expert_registry_id", "default")
        self.enable_bio_correlation = getattr(central_config, "enable_bio_correlation", True) and BIO_INSPIRED_AVAILABLE
        self.enable_natural_selection = getattr(central_config, "enable_natural_selection", True)
        self.enable_fitness_tracking = getattr(central_config, "enable_fitness_tracking", True)
        self.enable_population_tracking = getattr(central_config, "enable_population_tracking", True)
        self.enable_sustainability_dashboard = getattr(central_config, "enable_sustainability_dashboard", True)
        self.enable_predictive_forecasting = getattr(central_config, "enable_predictive_forecasting", True)
        self.enable_cross_region_sync = getattr(central_config, "enable_cross_region_sync", True)
        self.enable_quantum_efficiency = getattr(central_config, "enable_quantum_efficiency", True)
        self.enable_reproductive_strategies = getattr(central_config, "enable_reproductive_strategies", True)
        self.enable_climate_integration = getattr(central_config, "enable_climate_integration", True)
        self.enable_persistence = True  # We always use central storage
        self.sync_retries = getattr(central_config, "sync_retries", 3)
        self.sync_retry_base_delay_ms = getattr(central_config, "sync_retry_base_delay_ms", 100.0)
        self.sync_retry_max_delay_ms = getattr(central_config, "sync_retry_max_delay_ms", 5000.0)
        self.circuit_breaker_threshold = getattr(central_config, "circuit_breaker_failure_threshold", 5)
        self.circuit_breaker_recovery_timeout = getattr(central_config, "circuit_breaker_recovery_timeout", 30.0)
        self.sync_interval = getattr(central_config, "sync_interval", 3600)
        self.bio_sync_interval = getattr(central_config, "bio_sync_interval", 300)
        self.fitness_weights = getattr(central_config, "fitness_weights", {
            'resource_efficiency': 0.20,
            'resilience_score': 0.15,
            'adaptation_speed': 0.10,
            'cooperation_score': 0.10,
            'ecoatp_efficiency': 0.10,
            'sustainability_score': 0.15,
            'quantum_efficiency': 0.10,
            'quantum_advantage': 0.05,
            'helium_savings': 0.05
        })
        self.natural_selection_percentile_low = getattr(central_config, "natural_selection_percentile_low", 20.0)
        self.natural_selection_percentile_high = getattr(central_config, "natural_selection_percentile_high", 80.0)
        self.reproductive_mutation_rate = getattr(central_config, "reproductive_mutation_rate", 0.1)
        self.reproductive_max_offspring = getattr(central_config, "reproductive_max_offspring", 3)
        self.climate_update_interval = getattr(central_config, "climate_update_interval", 3600)
        self.rate_limit_per_minute = getattr(central_config, "rate_limit_requests", 60)
        self.enable_tick_engine = getattr(central_config, "enable_tick_engine", False)
        self.enable_quantum_bridge = getattr(central_config, "enable_quantum_bridge", False)

        # Validate
        if abs(sum(self.fitness_weights.values()) - 1.0) > 0.01:
            raise ValueError("Fitness weights must sum to 1.0")
        if self.natural_selection_percentile_low >= self.natural_selection_percentile_high:
            raise ValueError("low percentile must be less than high percentile")

# -----------------------------------------------------------------------------
# Pydantic Models for Data Structures (unchanged)
# -----------------------------------------------------------------------------
class ExpertVersion(BaseModel):
    major: int = Field(ge=0)
    minor: int = Field(ge=0)
    patch: int = Field(ge=0)
    prerelease: Optional[str] = None
    build: Optional[str] = None

    def to_string(self) -> str:
        version = f"{self.major}.{self.minor}.{self.patch}"
        if self.prerelease:
            version += f"-{self.prerelease}"
        if self.build:
            version += f"+{self.build}"
        return version

    @classmethod
    def from_string(cls, version_str: str) -> 'ExpertVersion':
        try:
            base = version_str.split('-')[0].split('+')[0]
            parts = base.split('.')
            major = int(parts[0]) if len(parts) > 0 else 1
            minor = int(parts[1]) if len(parts) > 1 else 0
            patch = int(parts[2]) if len(parts) > 2 else 0
            return cls(major=major, minor=minor, patch=patch)
        except Exception:
            return cls(major=1, minor=0, patch=0)

    def is_compatible_with(self, other: 'ExpertVersion') -> bool:
        return self.major == other.major

    def is_newer_than(self, other: 'ExpertVersion') -> bool:
        if self.major != other.major:
            return self.major > other.major
        if self.minor != other.minor:
            return self.minor > other.minor
        return self.patch > other.patch

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ExpertVersion):
            return NotImplemented
        return (self.major == other.major and
                self.minor == other.minor and
                self.patch == other.patch and
                self.prerelease == other.prerelease and
                self.build == other.build)

    def __hash__(self) -> int:
        return hash(self.to_string())

class ExpertDomain(str, Enum):
    ENERGY = "energy_optimization"
    DATA = "data_engineering"
    IOT = "iot_edge_computing"
    QUANTUM = "quantum_computing"
    HELIUM = "helium_aware_computing"
    CARBON = "carbon_optimization"
    SECURITY = "security_computing"
    GENERAL = "general_purpose"

class HardwareProfile(str, Enum):
    CPU_EFFICIENT = "cpu_low_power"
    CPU_PERFORMANCE = "cpu_high_performance"
    GPU_ACCELERATED = "gpu_cuda"
    QUANTUM_BACKEND = "quantum_processor"
    EDGE_DEVICE = "edge_iot_device"
    HYBRID = "hybrid_cpu_gpu"

class ExpertLifecycleState(str, Enum):
    REGISTERED = "registered"
    VALIDATING = "validating"
    CERTIFIED = "certified"
    ACTIVE = "active"
    CANARY = "canary"
    DEPRECATED = "deprecated"
    DEGRADED = "degraded"
    MAINTENANCE = "maintenance"
    RETIRED = "retired"
    ARCHIVED = "archived"

    def is_available(self) -> bool:
        return self in [self.CERTIFIED, self.ACTIVE, self.CANARY]

    def is_usable(self) -> bool:
        return self in [self.CERTIFIED, self.ACTIVE, self.CANARY, self.DEPRECATED, self.DEGRADED]

    def to_compartment_state(self):
        if not BIO_INSPIRED_AVAILABLE:
            return None
        mapping = {
            ExpertLifecycleState.REGISTERED: CompartmentState.GENESIS,
            ExpertLifecycleState.VALIDATING: CompartmentState.MATURING,
            ExpertLifecycleState.CERTIFIED: CompartmentState.MATURING,
            ExpertLifecycleState.ACTIVE: CompartmentState.ACTIVE,
            ExpertLifecycleState.CANARY: CompartmentState.ACTIVE,
            ExpertLifecycleState.DEPRECATED: CompartmentState.SENESCENT,
            ExpertLifecycleState.DEGRADED: CompartmentState.STRESSED,
            ExpertLifecycleState.MAINTENANCE: CompartmentState.STRESSED,
            ExpertLifecycleState.RETIRED: CompartmentState.APOPTOTIC,
            ExpertLifecycleState.ARCHIVED: CompartmentState.DECOMMISSIONED
        }
        return mapping.get(self)

class CertificationLevel(str, Enum):
    NONE = "none"
    SELF_CERTIFIED = "self_certified"
    INTERNAL_AUDIT = "internal_audit"
    THIRD_PARTY = "third_party"
    ISO_COMPLIANT = "iso_compliant"

class ExpertDependency(BaseModel):
    dependency_id: str
    dependency_type: str
    version_requirement: str
    is_optional: bool = False
    is_runtime: bool = True
    description: str = ""

class ExpertCertification(BaseModel):
    certification_id: str
    level: CertificationLevel
    issued_by: str
    issued_at: datetime
    expires_at: Optional[datetime] = None
    validation_results: Dict[str, Any] = Field(default_factory=dict)
    is_valid: bool = True

class HealthMetrics(BaseModel):
    success_rate: float = Field(1.0, ge=0.0, le=1.0)
    avg_latency_ms: float = Field(0.0, ge=0.0)
    error_rate: float = Field(0.0, ge=0.0, le=1.0)
    carbon_efficiency: float = Field(1.0, ge=0.0, le=1.0)
    helium_efficiency: float = Field(1.0, ge=0.0, le=1.0)
    availability: float = Field(1.0, ge=0.0, le=1.0)
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
    degradation_score: float = Field(0.0, ge=0.0, le=1.0)
    sustainability_score: float = Field(0.0, ge=0.0, le=1.0)
    quantum_efficiency: float = Field(0.0, ge=0.0, le=1.0)
    quantum_advantage_score: float = Field(0.0, ge=0.0, le=1.0)

    def calculate_health_score(self) -> float:
        weights = {
            'success_rate': 0.25, 'availability': 0.20, 'error_rate': 0.15,
            'carbon_efficiency': 0.10, 'helium_efficiency': 0.10,
            'degradation_score': 0.05, 'quantum_efficiency': 0.10,
            'quantum_advantage_score': 0.05
        }
        score = (weights['success_rate'] * self.success_rate +
                 weights['availability'] * self.availability +
                 weights['error_rate'] * (1 - self.error_rate) +
                 weights['carbon_efficiency'] * self.carbon_efficiency +
                 weights['helium_efficiency'] * self.helium_efficiency +
                 weights['degradation_score'] * (1 - self.degradation_score) +
                 weights['quantum_efficiency'] * self.quantum_efficiency +
                 weights['quantum_advantage_score'] * self.quantum_advantage_score)
        heartbeat_age = (datetime.utcnow() - self.last_heartbeat).total_seconds()
        if heartbeat_age > 300:
            score *= 0.5
        return max(0.0, min(1.0, score))

    def calculate_sustainability_score(self) -> float:
        return (self.carbon_efficiency * 0.35 +
                self.helium_efficiency * 0.25 +
                (1 - self.error_rate) * 0.20 +
                self.quantum_efficiency * 0.10 +
                self.quantum_advantage_score * 0.10)

class ExpertLineage(BaseModel):
    lineage_id: str
    parent_expert_id: Optional[str] = None
    created_from: Optional[str] = None
    training_data_hash: Optional[str] = None
    training_duration_hours: float = 0.0
    training_carbon_kg: float = 0.0
    model_architecture: str = ""
    hyperparameters: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    fitness_history: List[float] = Field(default_factory=list)
    reproductive_offspring: List[str] = Field(default_factory=list)
    mutation_count: int = 0

class ExpertProfile(BaseModel):
    expert_id: str
    expert_name: str = ""
    version: ExpertVersion = Field(default_factory=lambda: ExpertVersion(major=1, minor=0, patch=0))
    domain: ExpertDomain = ExpertDomain.GENERAL
    hardware_profile: HardwareProfile = HardwareProfile.CPU_EFFICIENT
    lifecycle_state: ExpertLifecycleState = ExpertLifecycleState.REGISTERED
    registered_at: datetime = Field(default_factory=datetime.utcnow)
    activated_at: Optional[datetime] = None
    retired_at: Optional[datetime] = None
    replaces_expert: Optional[str] = None
    replaced_by: Optional[str] = None
    helium_per_inference: float = Field(0.0, ge=0.0)
    carbon_per_inference: float = Field(0.0, ge=0.0)
    energy_per_inference: float = Field(0.0, ge=0.0)
    avg_latency_ms: float = Field(0.0, ge=0.0)
    memory_usage_mb: float = Field(0.0, ge=0.0)
    accuracy_score: float = Field(0.0, ge=0.0, le=1.0)
    reliability_score: float = Field(0.0, ge=0.0, le=1.0)
    efficiency_score: float = Field(0.0, ge=0.0, le=1.0)
    security_score: float = Field(0.0, ge=0.0, le=1.0)
    min_carbon_zone: int = 0
    max_helium_scarcity: float = Field(1.0, ge=0.0, le=1.0)
    supported_task_types: List[str] = Field(default_factory=list)
    incompatible_with: List[str] = Field(default_factory=list)
    dependencies: List[ExpertDependency] = Field(default_factory=list)
    certifications: List[ExpertCertification] = Field(default_factory=list)
    health: HealthMetrics = Field(default_factory=HealthMetrics)
    lineage: Optional[ExpertLineage] = None
    is_remote: bool = False
    remote_endpoint: Optional[str] = None
    origin_region: str = "local"
    dynamic_weights: Dict[str, float] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    capabilities: List[str] = Field(default_factory=list)
    is_active: bool = True
    sustainability_score: float = Field(0.0, ge=0.0, le=1.0)
    quantum_capable: bool = False
    quantum_backend: Optional[str] = None
    quantum_qubits: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

    def compute_hash(self) -> str:
        profile_str = json.dumps(self.model_dump(exclude={'health': {'last_heartbeat'}}), sort_keys=True)
        return hashlib.sha256(profile_str.encode()).hexdigest()

    def is_compatible_with(self, other: 'ExpertProfile') -> bool:
        if other.expert_id in self.incompatible_with:
            return False
        if self.expert_id in other.incompatible_with:
            return False
        if self.expert_name == other.expert_name:
            return self.version.is_compatible_with(other.version)
        return True

    def get_certification_level(self) -> CertificationLevel:
        if not self.certifications:
            return CertificationLevel.NONE
        levels = [c.level for c in self.certifications if c.is_valid]
        if not levels:
            return CertificationLevel.NONE
        return max(levels, key=lambda l: list(CertificationLevel).index(l))

class FitnessScore(BaseModel):
    expert_id: str
    overall_fitness: float = Field(0.5, ge=0.0, le=1.0)
    resource_efficiency: float = Field(0.5, ge=0.0, le=1.0)
    adaptation_speed: float = Field(0.5, ge=0.0, le=1.0)
    cooperation_score: float = Field(0.5, ge=0.0, le=1.0)
    resilience_score: float = Field(0.5, ge=0.0, le=1.0)
    selection_coefficient: float = Field(0.0, ge=-1.0, le=1.0)
    reproductive_success: int = Field(0, ge=0)
    ecoatp_efficiency: float = Field(0.5, ge=0.0, le=1.0)
    sustainability_score: float = Field(0.5, ge=0.0, le=1.0)
    quantum_efficiency: float = Field(0.5, ge=0.0, le=1.0)
    quantum_advantage: float = Field(0.0, ge=0.0, le=1.0)
    helium_savings: float = Field(0.5, ge=0.0, le=1.0)

    def calculate_overall(self, weights: Dict[str, float] = None):
        if weights is None:
            weights = {
                'resource_efficiency': 0.20,
                'resilience_score': 0.15,
                'adaptation_speed': 0.10,
                'cooperation_score': 0.10,
                'ecoatp_efficiency': 0.10,
                'sustainability_score': 0.15,
                'quantum_efficiency': 0.10,
                'quantum_advantage': 0.05,
                'helium_savings': 0.05
            }
        self.overall_fitness = (
            self.resource_efficiency * weights['resource_efficiency'] +
            self.resilience_score * weights['resilience_score'] +
            self.adaptation_speed * weights['adaptation_speed'] +
            self.cooperation_score * weights['cooperation_score'] +
            self.ecoatp_efficiency * weights['ecoatp_efficiency'] +
            self.sustainability_score * weights['sustainability_score'] +
            self.quantum_efficiency * weights['quantum_efficiency'] +
            self.quantum_advantage * weights['quantum_advantage'] +
            self.helium_savings * weights['helium_savings']
        )

# -----------------------------------------------------------------------------
# Timed Cache (unchanged)
# -----------------------------------------------------------------------------
class TimedCache:
    def __init__(self, ttl_seconds: float = 30.0):
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._ttl = ttl_seconds
        self._lock = asyncio.Lock()

    async def get_or_compute(self, key: str, compute: Callable[[], Any]) -> Any:
        now = datetime.utcnow().timestamp()
        async with self._lock:
            if key in self._cache:
                value, timestamp = self._cache[key]
                if now - timestamp < self._ttl:
                    return value
            value = compute()
            self._cache[key] = (value, now)
            return value

    def invalidate(self, key: str):
        if key in self._cache:
            del self._cache[key]

# -----------------------------------------------------------------------------
# Circuit Breaker (unchanged)
# -----------------------------------------------------------------------------
class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = "closed"

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        if self.state == "open":
            if (datetime.utcnow().timestamp() - self.last_failure_time) > self.recovery_timeout:
                self.state = "half-open"
            else:
                raise RuntimeError("Circuit breaker is open")
        try:
            result = await func(*args, **kwargs)
            if self.state == "half-open":
                self.state = "closed"
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = datetime.utcnow().timestamp()
            if self.failure_count >= self.failure_threshold:
                self.state = "open"
            raise e

# -----------------------------------------------------------------------------
# Rate Limiter (unchanged)
# -----------------------------------------------------------------------------
class RateLimiter:
    def __init__(self, rate_per_minute: int):
        self.rate = rate_per_minute / 60.0
        self.tokens = rate_per_minute
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
# Bio Correlator (unchanged)
# -----------------------------------------------------------------------------
class BioCorrelator:
    def __init__(self, registry: 'ExpertRegistry'):
        self.registry = registry
        self._ecoatp_cache = TimedCache(ttl_seconds=30)
        self._gradient_cache = TimedCache(ttl_seconds=30)
        self._population_cache = TimedCache(ttl_seconds=60)
        self._token_circuit = CircuitBreaker()
        self._gradient_circuit = CircuitBreaker()
        self._compartment_circuit = CircuitBreaker()
        logger.info("BioCorrelator initialized")

    async def get_expert_ecoatp_efficiency(self, expert_id: str) -> float:
        if not self.registry.token_manager:
            return 0.5
        async def compute():
            account = self.registry.token_manager.get_account_summary(f"expert_{expert_id}")
            if account:
                return account.get('efficiency_rating', 0.5)
            return 0.5
        return await self._ecoatp_cache.get_or_compute(f"ecoatp_efficiency_{expert_id}", compute)

    async def get_expert_token_balance(self, expert_id: str) -> float:
        if not self.registry.token_manager:
            return 0.0
        account = self.registry.token_manager.get_account_summary(f"expert_{expert_id}")
        if account:
            return account.get('balance', 0)
        return 0.0

    async def get_gradient_strength(self, field_id: str) -> float:
        if not self.registry.gradient_manager:
            return 0.5
        async def compute():
            field = self.registry.gradient_manager.fields.get(field_id)
            if field:
                return field.gradient_strength
            return 0.5
        return await self._gradient_cache.get_or_compute(f"gradient_{field_id}", compute)

    def get_species_population(self, species_id: str) -> int:
        if self.registry.compartment_manager:
            return sum(1 for c in self.registry.compartment_manager.compartments.values()
                      if c.expert_type == species_id and c.is_viable)
        return len([e for e in self.registry._experts.values()
                   if hasattr(e, 'domain') and species_id in str(e.domain).lower()])

    def get_species_populations(self) -> Dict[str, int]:
        species = ['energy', 'data', 'iot', 'quantum', 'helium', 'general']
        return {s: self.get_species_population(s) for s in species}

    def get_total_compartment_population(self) -> int:
        if self.registry.compartment_manager:
            return len([c for c in self.registry.compartment_manager.compartments.values() if c.is_viable])
        return len([e for e in self.registry._experts.values() if e.lifecycle_state.is_available()])

    def get_species_id(self, profile: ExpertProfile) -> str:
        domain = profile.domain.value
        if 'energy' in domain.lower(): return 'energy'
        if 'data' in domain.lower(): return 'data'
        if 'iot' in domain.lower(): return 'iot'
        if 'quantum' in domain.lower(): return 'quantum'
        if 'helium' in domain.lower(): return 'helium'
        return 'general'

# -----------------------------------------------------------------------------
# Fitness Manager (Enhanced with adaptive cost)
# -----------------------------------------------------------------------------
class FitnessManager:
    def __init__(self, registry: 'ExpertRegistry'):
        self.registry = registry
        self._fitness_weights = registry.config.fitness_weights.copy()
        self._fitness_weight_history = deque(maxlen=100)
        self._lock = asyncio.Lock()
        logger.info("FitnessManager initialized")

    async def update_fitness_from_gradients(self):
        if not self.registry.enable_bio_correlation or not self.registry.gradient_manager:
            return
        async with self._lock:
            trust_strength = await self.registry.bio_correlator.get_gradient_strength('trust')
            carbon_strength = await self.registry.bio_correlator.get_gradient_strength('carbon')
            for expert_id, fitness in self.registry.fitness_scores.items():
                if expert_id not in self.registry._experts:
                    continue
                expert = self.registry._experts[expert_id]
                fitness.resilience_score = fitness.resilience_score * 0.7 + trust_strength * 0.3
                carbon_eff = 1.0 / (1.0 + expert.carbon_per_inference * 10000)
                fitness.resource_efficiency = fitness.resource_efficiency * 0.8 + carbon_eff * 0.2
                fitness.ecoatp_efficiency = await self.registry.bio_correlator.get_expert_ecoatp_efficiency(expert_id)
                if self.registry.compartment_manager:
                    compartment = self.registry.compartment_manager.find_best_compartment(self.registry.bio_correlator.get_species_id(expert))
                    if compartment:
                        fitness.cooperation_score = fitness.cooperation_score * 0.8 + compartment.health_score * 0.2
                fitness.quantum_efficiency = expert.health.quantum_efficiency
                fitness.quantum_advantage = self._calculate_quantum_advantage(expert)
                fitness.helium_savings = 1.0 - expert.helium_per_inference / max(expert.helium_per_inference, 1)
                fitness.sustainability_score = expert.health.calculate_sustainability_score()
                fitness.calculate_overall(self._fitness_weights)

    def _calculate_quantum_advantage(self, profile: ExpertProfile) -> float:
        if not profile.quantum_capable:
            return 0.0
        return min(1.0, (profile.quantum_qubits / 100) * 0.5 + profile.accuracy_score * 0.5)

    def update_fitness_weights(self, new_weights: Dict[str, float]):
        if abs(sum(new_weights.values()) - 1.0) > 0.01:
            raise ValueError("Fitness weights must sum to 1.0")
        self._fitness_weights = new_weights
        self._fitness_weight_history.append(new_weights)
        logger.info(f"Fitness weights updated: {new_weights}")

    async def trigger_natural_selection(self):
        if not self.registry.enable_natural_selection:
            return
        await self.update_fitness_from_gradients()
        fitnesses = [f.overall_fitness for f in self.registry.fitness_scores.values()]
        if not fitnesses:
            return

        low_pct = self.registry.config.natural_selection_percentile_low
        high_pct = self.registry.config.natural_selection_percentile_high
        threshold = np.percentile(fitnesses, low_pct)
        top_threshold = np.percentile(fitnesses, high_pct)

        deprecated_count = 0
        reproducer_count = 0

        async with self.registry._lock:
            for expert_id, fitness in list(self.registry.fitness_scores.items()):
                if expert_id not in self.registry._experts:
                    continue
                expert = self.registry._experts[expert_id]
                if (fitness.overall_fitness < threshold and
                    fitness.reproductive_success == 0 and
                    expert.lifecycle_state in [ExpertLifecycleState.ACTIVE, ExpertLifecycleState.CERTIFIED]):
                    self.registry.deprecate_expert(expert_id, reason="natural_selection_low_fitness")
                    deprecated_count += 1
                    if self.registry.biomass_storage:
                        self.registry.biomass_storage.store_task(
                            task_data={'expert_id': expert_id, 'knowledge': expert.model_dump()},
                            ecoatp_cost=1.0,
                            guarantee=GuaranteeLevel.BEST_EFFORT,
                            initial_tier=StorageTier.LIPID_DEPOT
                        )
                    self.registry.evolutionary_events.append({
                        'type': 'extinction',
                        'expert_id': expert_id,
                        'fitness': fitness.overall_fitness,
                        'quantum_efficiency': fitness.quantum_efficiency,
                        'reason': 'natural_selection',
                        'timestamp': datetime.utcnow().isoformat()
                    })
                    self.registry.extinction_count += 1
                elif fitness.overall_fitness > top_threshold and fitness.reproductive_success < self.registry.config.reproductive_max_offspring:
                    fitness.reproductive_success += 1
                    reproducer_count += 1

        self.registry._stats['total_natural_selections'] += 1
        self.registry._stats['last_selection'] = datetime.utcnow()
        if deprecated_count > 0 or reproducer_count > 0:
            logger.info(f"Natural selection: {deprecated_count} deprecated, {reproducer_count} marked for reproduction")

# -----------------------------------------------------------------------------
# Sustainability Dashboard (unchanged, but uses central metrics)
# -----------------------------------------------------------------------------
class RegistrySustainabilityDashboard:
    def __init__(self, registry: 'ExpertRegistry'):
        self.registry = registry
        self.history = deque(maxlen=1000)
        self._alert_history = deque(maxlen=100)
        self._cache: Dict[str, Any] = {}
        self._cache_ttl = 30
        self._last_cache_update: Optional[datetime] = None
        logger.info("RegistrySustainabilityDashboard initialized")

    def get_dashboard_status(self) -> Dict[str, Any]:
        now = datetime.utcnow()
        if (self._last_cache_update and
            (now - self._last_cache_update).total_seconds() < self._cache_ttl):
            return self._cache

        registry = self.registry
        active_experts = registry.get_all_active_experts()
        total_experts = len(registry._experts)

        avg_carbon = np.mean([e.health.carbon_efficiency for e in active_experts]) if active_experts else 0.5
        avg_helium = np.mean([e.health.helium_efficiency for e in active_experts]) if active_experts else 0.5
        avg_quantum = np.mean([e.health.quantum_efficiency for e in active_experts]) if active_experts else 0.0
        avg_sustainability = np.mean([e.sustainability_score for e in active_experts]) if active_experts else 0.5

        fitnesses = [f.overall_fitness for f in registry.fitness_scores.values()] if registry.fitness_scores else [0.5]

        status = {
            'timestamp': now.isoformat(),
            'total_experts': total_experts,
            'active_experts': len(active_experts),
            'avg_carbon_efficiency': avg_carbon,
            'avg_helium_efficiency': avg_helium,
            'avg_quantum_efficiency': avg_quantum,
            'avg_sustainability_score': avg_sustainability,
            'fitness_distribution': {
                'mean': np.mean(fitnesses),
                'median': np.median(fitnesses),
                'std': np.std(fitnesses),
                'min': np.min(fitnesses),
                'max': np.max(fitnesses)
            },
            'species_populations': registry._get_species_populations(),
            'evolutionary_events': len(registry.evolutionary_events),
            'is_healthy': all([avg_sustainability > 0.3, avg_carbon > 0.3, len(active_experts) > 2]),
            'predictive_alerts': self._generate_predictive_alerts(),
            'alert_count': len(self._alert_history)
        }

        self._cache = status
        self._last_cache_update = now
        return status

    def _generate_predictive_alerts(self) -> List[Dict[str, Any]]:
        registry = self.registry
        alerts = []

        for expert_id, fitness in registry.fitness_scores.items():
            if expert_id not in registry._experts:
                continue
            if fitness.overall_fitness < 0.2:
                alerts.append({
                    'level': 'critical',
                    'expert_id': expert_id,
                    'message': f"Expert {expert_id} at high risk of extinction (fitness: {fitness.overall_fitness:.2f})",
                    'timeframe_hours': 24,
                    'recommendation': 'Immediate intervention recommended'
                })
            elif fitness.overall_fitness < 0.3:
                alerts.append({
                    'level': 'warning',
                    'expert_id': expert_id,
                    'message': f"Expert {expert_id} showing declining fitness (fitness: {fitness.overall_fitness:.2f})",
                    'timeframe_hours': 72,
                    'recommendation': 'Monitor and consider intervention'
                })

            if fitness.quantum_efficiency < 0.2 and registry._experts[expert_id].quantum_capable:
                alerts.append({
                    'level': 'warning',
                    'expert_id': expert_id,
                    'message': f"Quantum expert {expert_id} has low quantum efficiency",
                    'timeframe_hours': 48,
                    'recommendation': 'Optimize quantum circuit parameters'
                })

        species_counts = registry._get_species_populations()
        for species, count in species_counts.items():
            if count == 0:
                alerts.append({
                    'level': 'critical',
                    'species': species,
                    'message': f"Species {species} has gone extinct",
                    'timeframe_hours': 0,
                    'recommendation': 'Consider introducing new experts in this domain'
                })
            elif count < 2:
                alerts.append({
                    'level': 'warning',
                    'species': species,
                    'message': f"Species {species} is critically endangered (population: {count})",
                    'timeframe_hours': 72,
                    'recommendation': 'Promote reproduction or introduce new experts'
                })

        self._alert_history.extend(alerts)
        return alerts

    def get_predictive_alerts(self, level: Optional[str] = None) -> List[Dict]:
        alerts = list(self._alert_history)
        if level:
            return [a for a in alerts if a.get('level') == level]
        return alerts

    def export_metrics(self) -> Dict[str, float]:
        status = self.get_dashboard_status()
        metrics = {
            'registry_total_experts': status['total_experts'],
            'registry_active_experts': status['active_experts'],
            'registry_avg_carbon_efficiency': status['avg_carbon_efficiency'],
            'registry_avg_helium_efficiency': status['avg_helium_efficiency'],
            'registry_avg_quantum_efficiency': status['avg_quantum_efficiency'],
            'registry_avg_sustainability_score': status['avg_sustainability_score'],
            'registry_fitness_mean': status['fitness_distribution']['mean'],
            'registry_fitness_median': status['fitness_distribution']['median'],
            'registry_fitness_std': status['fitness_distribution']['std'],
            'registry_is_healthy': 1.0 if status['is_healthy'] else 0.0,
            'registry_evolutionary_events': status['evolutionary_events'],
            'registry_alert_count': status['alert_count']
        }
        for species, count in status['species_populations'].items():
            metrics[f'registry_species_{species}'] = count
        return metrics

    def generate_report(self) -> Dict[str, Any]:
        status = self.get_dashboard_status()
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'dashboard': status,
            'predictive_alerts': self.get_predictive_alerts(),
            'recommendations': self._generate_recommendations(status),
            'metrics': self.export_metrics()
        }

    def _generate_recommendations(self, status: Dict) -> List[Dict]:
        recommendations = []
        if status['avg_sustainability_score'] < 0.4:
            recommendations.append({
                'priority': 'high',
                'category': 'sustainability',
                'message': 'Overall sustainability score is low',
                'actions': ['Review expert carbon/helium efficiency', 'Optimize resource usage']
            })
        if status['avg_carbon_efficiency'] < 0.4:
            recommendations.append({
                'priority': 'high',
                'category': 'carbon',
                'message': 'Carbon efficiency is below threshold',
                'actions': ['Filter experts by carbon efficiency', 'Deprecate high-carbon experts']
            })
        if status['avg_helium_efficiency'] < 0.4:
            recommendations.append({
                'priority': 'high',
                'category': 'helium',
                'message': 'Helium efficiency is below threshold',
                'actions': ['Filter experts by helium efficiency', 'Optimize helium usage']
            })
        if status['avg_quantum_efficiency'] < 0.3:
            recommendations.append({
                'priority': 'medium',
                'category': 'quantum',
                'message': 'Quantum efficiency is low',
                'actions': ['Optimize quantum circuit parameters', 'Consider quantum error mitigation']
            })
        return recommendations

# -----------------------------------------------------------------------------
# Predictive Evolution Forecaster (unchanged)
# -----------------------------------------------------------------------------
class PredictiveEvolutionForecaster:
    def __init__(self, registry: 'ExpertRegistry'):
        self.registry = registry
        self.forecast_history = deque(maxlen=1000)
        self._climate_models = {
            'carbon': {'current': 400, 'trend': 0.02, 'volatility': 0.05},
            'helium': {'current': 0.5, 'trend': 0.03, 'volatility': 0.08}
        }
        self._last_update = datetime.utcnow()
        logger.info("PredictiveEvolutionForecaster initialized")

    def update_climate_model(self, model_type: str, data: Dict[str, float]):
        if model_type in self._climate_models:
            self._climate_models[model_type].update(data)
            logger.info(f"Updated climate model for {model_type}")

    async def forecast_evolutionary_trend(self, hours: int = 24) -> Dict[str, Any]:
        registry = self.registry
        self._update_trends_from_history()

        carbon_proj = self._project_climate('carbon', hours)
        helium_proj = self._project_climate('helium', hours)

        if registry.config.enable_tick_engine and registry.tick_engine:
            try:
                tick_forecast = await registry.tick_engine.get_helium_forecast(hours // 1)
                if tick_forecast and len(tick_forecast) > 0:
                    helium_proj['current'] = tick_forecast[0] if isinstance(tick_forecast, list) else tick_forecast.get('scarcity', helium_proj['current'])
                    helium_proj['projected'] = np.mean(tick_forecast) if isinstance(tick_forecast, list) else tick_forecast.get('projected', helium_proj['projected'])
            except Exception as e:
                logger.warning(f"TimeTickEngine integration failed: {e}")

        if registry.config.enable_quantum_bridge and registry.quantum_bridge:
            try:
                q_params = registry.quantum_bridge.get_qubo_parameters()
                q_penalty = q_params.get('penalty_helium_shortage', 0.5)
                if q_penalty > 0.7:
                    helium_proj['projected'] = min(1.0, helium_proj['projected'] * 1.1)
            except Exception as e:
                logger.warning(f"QuantumBridge integration failed: {e}")

        fitness_history = []
        for expert_id, fitness in registry.fitness_scores.items():
            if expert_id in registry._experts:
                expert = registry._experts[expert_id]
                if expert.lineage and expert.lineage.fitness_history:
                    fitness_history.extend(expert.lineage.fitness_history)

        extinctions = self._forecast_extinctions(carbon_proj, helium_proj)
        speciation = self._forecast_speciation(carbon_proj, helium_proj)
        trajectory = self._calculate_fitness_trajectory(fitness_history)

        forecast = {
            'timestamp': datetime.utcnow().isoformat(),
            'forecast_horizon_hours': hours,
            'climate_projections': {'carbon': carbon_proj, 'helium': helium_proj},
            'predicted_extinctions': extinctions,
            'predicted_speciation': speciation,
            'fitness_trajectory': trajectory,
            'recommended_actions': self._generate_actions(extinctions, speciation, carbon_proj, helium_proj),
            'confidence': self._calculate_forecast_confidence()
        }
        self.forecast_history.append(forecast)
        return forecast

    def _update_trends_from_history(self):
        registry = self.registry
        if len(registry._performance_history) < 10:
            return
        efficiencies = []
        for expert_id, history in registry._performance_history.items():
            for entry in history[-20:]:
                if 'carbon_kg' in entry:
                    efficiencies.append(entry['carbon_kg'])
        if efficiencies:
            avg = np.mean(efficiencies[-10:]) if len(efficiencies) >= 10 else np.mean(efficiencies)
            carbon_trend = 0.02 * (1 - avg)
            self._climate_models['carbon']['trend'] = carbon_trend

    def _project_climate(self, model_type: str, hours: int) -> Dict[str, float]:
        model = self._climate_models.get(model_type, {'current': 0.5, 'trend': 0.0, 'volatility': 0.05})
        current = model.get('current', 0.5)
        trend = model.get('trend', 0.0)
        volatility = model.get('volatility', 0.05)
        projected = current * (1 + trend * hours / (24 * 365))
        projected += np.random.normal(0, volatility * hours / 24)
        return {
            'current': current,
            'projected': max(0.0, min(1.0, projected)),
            'trend': trend,
            'volatility': volatility,
            'hours': hours
        }

    def _forecast_extinctions(self, carbon_proj: Dict, helium_proj: Dict) -> Dict[str, Any]:
        registry = self.registry
        carbon_stress = carbon_proj['projected'] / 500
        helium_stress = helium_proj['projected']
        at_risk = []
        for expert_id, fitness in registry.fitness_scores.items():
            if expert_id not in registry._experts:
                continue
            climate_adjustment = 1.0 - (carbon_stress * 0.2 + helium_stress * 0.3)
            adjusted = fitness.overall_fitness * climate_adjustment
            if adjusted < 0.25:
                at_risk.append({
                    'expert_id': expert_id,
                    'current_fitness': fitness.overall_fitness,
                    'adjusted_fitness': adjusted,
                    'risk_level': 'high',
                    'climate_stress': {'carbon': carbon_stress, 'helium': helium_stress}
                })
            elif adjusted < 0.4:
                at_risk.append({
                    'expert_id': expert_id,
                    'current_fitness': fitness.overall_fitness,
                    'adjusted_fitness': adjusted,
                    'risk_level': 'medium',
                    'climate_stress': {'carbon': carbon_stress, 'helium': helium_stress}
                })
        return {
            'at_risk_count': len(at_risk),
            'at_risk_details': at_risk,
            'extinction_rate': len(at_risk) / max(len(registry._experts), 1),
            'carbon_stress': carbon_stress,
            'helium_stress': helium_stress
        }

    def _forecast_speciation(self, carbon_proj: Dict, helium_proj: Dict) -> Dict[str, Any]:
        registry = self.registry
        carbon_opp = max(0, 1.0 - carbon_proj['projected'] / 500)
        helium_opp = max(0, 1.0 - helium_proj['projected'])
        candidates = []
        for expert_id, fitness in registry.fitness_scores.items():
            if expert_id not in registry._experts:
                continue
            climate_bonus = (carbon_opp * 0.2 + helium_opp * 0.3)
            adjusted = fitness.overall_fitness + climate_bonus * 0.3
            if adjusted > 0.7:
                candidates.append({
                    'expert_id': expert_id,
                    'fitness': fitness.overall_fitness,
                    'adjusted_fitness': adjusted,
                    'speciation_potential': min(1.0, fitness.reproductive_success / 3 + climate_bonus),
                    'climate_opportunity': {'carbon': carbon_opp, 'helium': helium_opp}
                })
        return {
            'speciation_candidates': len(candidates),
            'candidate_details': candidates,
            'predicted_new_species': len([c for c in candidates if c['speciation_potential'] > 0.5]),
            'carbon_opportunity': carbon_opp,
            'helium_opportunity': helium_opp
        }

    def _calculate_fitness_trajectory(self, fitness_history: List[float]) -> Dict[str, Any]:
        if len(fitness_history) < 10:
            return {'trend': 'stable', 'confidence': 0.3, 'average': np.mean(fitness_history) if fitness_history else 0.5}
        x = np.arange(len(fitness_history))
        slope = np.polyfit(x, fitness_history, 1)[0]
        if slope > 0.01:
            trend = 'improving'
            confidence = min(0.9, 0.5 + abs(slope) * 10)
        elif slope < -0.01:
            trend = 'declining'
            confidence = min(0.9, 0.5 + abs(slope) * 10)
        else:
            trend = 'stable'
            confidence = 0.6
        predicted = np.mean(fitness_history[-10:]) + slope * 10
        return {
            'trend': trend,
            'confidence': confidence,
            'average': np.mean(fitness_history),
            'slope': slope,
            'predicted_fitness': max(0.0, min(1.0, predicted))
        }

    def _generate_actions(self, extinctions: Dict, speciation: Dict, carbon_proj: Dict, helium_proj: Dict) -> List[str]:
        actions = []
        if extinctions['at_risk_count'] > 0:
            actions.append(f"Review {extinctions['at_risk_count']} experts at risk of extinction")
            for risk in extinctions['at_risk_details'][:3]:
                actions.append(f"Consider intervention for {risk['expert_id']} (risk: {risk['risk_level']})")
        if carbon_proj['projected'] > 500:
            actions.append("Carbon stress increasing - prioritize carbon-efficient experts")
        if helium_proj['projected'] > 0.6:
            actions.append("Helium scarcity increasing - prioritize helium-efficient experts")
        if speciation['speciation_candidates'] > 0:
            actions.append(f"Encourage reproduction from {speciation['speciation_candidates']} high-fitness experts")
        return actions

    def _calculate_forecast_confidence(self) -> float:
        registry = self.registry
        if len(registry.fitness_scores) < 10:
            return 0.3
        elif len(registry.fitness_scores) < 30:
            return 0.5
        else:
            return min(0.9, 0.7 + 0.1 * len(registry.fitness_scores) / 50 * 0.7)

# -----------------------------------------------------------------------------
# CrossRegionRegistrySynchronizer (unchanged)
# -----------------------------------------------------------------------------
class CrossRegionRegistrySynchronizer:
    def __init__(self, registry: 'ExpertRegistry'):
        self.registry = registry
        self._session: Optional[aiohttp.ClientSession] = None
        self.sync_history = deque(maxlen=1000)
        self.voting_weights: Dict[str, float] = {}
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=registry.config.circuit_breaker_threshold,
            recovery_timeout=registry.config.circuit_breaker_recovery_timeout
        )
        logger.info("CrossRegionRegistrySynchronizer initialized with resilience")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def sync_with_remote_registry(
        self,
        registry_url: str,
        registry_id: str,
        sync_mode: str = 'pull',
        resolve_conflicts: bool = True
    ) -> Dict[str, Any]:
        result = {
            'timestamp': datetime.utcnow().isoformat(),
            'registry_id': registry_id,
            'sync_mode': sync_mode,
            'synced_experts': 0,
            'conflicts': [],
            'resolved_conflicts': [],
            'status': 'unknown'
        }

        try:
            result = await self._circuit_breaker.call(
                self._do_sync,
                registry_url, registry_id, sync_mode, resolve_conflicts
            )
        except Exception as e:
            logger.error(f"Sync failed after circuit breaker: {e}")
            result['status'] = f'error: {str(e)}'
        return result

    async def _do_sync(
        self,
        registry_url: str,
        registry_id: str,
        sync_mode: str,
        resolve_conflicts: bool
    ) -> Dict[str, Any]:
        result = {
            'timestamp': datetime.utcnow().isoformat(),
            'registry_id': registry_id,
            'sync_mode': sync_mode,
            'synced_experts': 0,
            'conflicts': [],
            'resolved_conflicts': [],
            'status': 'unknown'
        }

        for attempt in range(self.registry.config.sync_retries):
            try:
                session = await self._get_session()

                if sync_mode in ['pull', 'both']:
                    async with session.get(f"{registry_url}/api/experts", timeout=30) as response:
                        if response.status == 200:
                            remote_experts = await response.json()
                            synced, conflicts, resolved = await self._merge_remote_experts_with_voting(
                                remote_experts, registry_id, resolve_conflicts
                            )
                            result['synced_experts'] = synced
                            result['conflicts'] = conflicts
                            result['resolved_conflicts'] = resolved
                        else:
                            logger.warning(f"Sync pull failed: {response.status} (attempt {attempt+1})")
                            if attempt == self.registry.config.sync_retries - 1:
                                result['status'] = f'failed_pull_{response.status}'
                                return result
                            await asyncio.sleep(2 ** attempt * 0.1)

                if sync_mode in ['push', 'both']:
                    local_experts = self._serialize_local_experts()
                    async with session.post(
                        f"{registry_url}/api/experts/sync",
                        json={'experts': local_experts, 'registry_id': self.registry.registry_id},
                        timeout=30
                    ) as response:
                        if response.status != 200:
                            logger.warning(f"Sync push failed: {response.status} (attempt {attempt+1})")
                            if attempt == self.registry.config.sync_retries - 1:
                                result['push_status'] = f'failed_push_{response.status}'
                                return result
                            await asyncio.sleep(2 ** attempt * 0.1)

                result['status'] = 'success'
                self.registry._remote_registries[registry_id] = registry_url
                self.voting_weights[registry_id] = self.voting_weights.get(registry_id, 0.5)
                self.sync_history.append({
                    'timestamp': datetime.utcnow().isoformat(),
                    'registry_id': registry_id,
                    'synced_count': result['synced_experts'],
                    'conflicts': len(result['conflicts']),
                    'resolved': len(result['resolved_conflicts'])
                })
                return result

            except Exception as e:
                logger.error(f"Sync error (attempt {attempt+1}): {e}")
                if attempt == self.registry.config.sync_retries - 1:
                    result['status'] = f'error: {str(e)}'
                    return result
                await asyncio.sleep(2 ** attempt * 0.1)

        return result

    async def _merge_remote_experts_with_voting(
        self,
        remote_experts: List[Dict],
        registry_id: str,
        resolve_conflicts: bool
    ) -> Tuple[int, List[Dict], List[Dict]]:
        synced = 0
        conflicts = []
        resolved = []

        for remote_data in remote_experts:
            expert_id = remote_data.get('expert_id')
            if not expert_id:
                continue

            try:
                remote_profile = ExpertProfile.model_validate(remote_data)
            except ValidationError as e:
                logger.warning(f"Invalid remote expert data: {e}")
                continue

            remote_version = remote_profile.version

            if expert_id in self.registry._experts:
                local_expert = self.registry._experts[expert_id]
                local_version = local_expert.version

                if remote_version.is_newer_than(local_version):
                    conflict = {
                        'expert_id': expert_id,
                        'local_version': local_version.to_string(),
                        'remote_version': remote_version.to_string(),
                        'action': 'remote_newer'
                    }
                    conflicts.append(conflict)
                    if resolve_conflicts:
                        resolution = await self._resolve_conflict_with_voting(
                            expert_id, local_expert, remote_profile, registry_id
                        )
                        if resolution:
                            resolved.append(resolution)
                            synced += 1
                elif local_version.is_newer_than(remote_version):
                    conflicts.append({
                        'expert_id': expert_id,
                        'local_version': local_version.to_string(),
                        'remote_version': remote_version.to_string(),
                        'action': 'local_newer'
                    })
            else:
                success, msg = self.registry.register_expert(remote_profile, validate=False, auto_certify=False)
                if success:
                    synced += 1
                else:
                    logger.warning(f"Failed to register remote expert {expert_id}: {msg}")

        return synced, conflicts, resolved

    async def _resolve_conflict_with_voting(
        self,
        expert_id: str,
        local_expert: ExpertProfile,
        remote_expert: ExpertProfile,
        remote_registry_id: str
    ) -> Optional[Dict]:
        votes = []
        local_trust = self.voting_weights.get(self.registry.registry_id, 0.5)
        votes.append({
            'registry': self.registry.registry_id,
            'version': local_expert.version.to_string(),
            'trust': local_trust,
            'decision': 'local'
        })
        remote_trust = self.voting_weights.get(remote_registry_id, 0.5)
        votes.append({
            'registry': remote_registry_id,
            'version': remote_expert.version.to_string(),
            'trust': remote_trust,
            'decision': 'remote'
        })

        for other_id, other_url in self.registry._remote_registries.items():
            if other_id not in [self.registry.registry_id, remote_registry_id]:
                try:
                    session = await self._get_session()
                    async with session.get(f"{other_url}/api/experts/{expert_id}", timeout=10) as response:
                        if response.status == 200:
                            data = await response.json()
                            other_version = data.get('version', '1.0.0')
                            other_trust = self.voting_weights.get(other_id, 0.3)
                            votes.append({
                                'registry': other_id,
                                'version': other_version,
                                'trust': other_trust,
                                'decision': 'other'
                            })
                except Exception as e:
                    logger.warning(f"Failed to get vote from {other_id}: {e}")

        version_scores = {}
        for vote in votes:
            version = vote['version']
            weight = vote['trust']
            version_scores[version] = version_scores.get(version, 0) + weight

        if version_scores:
            winner_version = max(version_scores.items(), key=lambda x: x[1])[0]
            if winner_version != local_expert.version.to_string():
                self.registry._experts[expert_id].version = ExpertVersion.from_string(winner_version)
                logger.info(f"Conflict resolved: {expert_id} updated to {winner_version}")
                return {
                    'expert_id': expert_id,
                    'winner_version': winner_version,
                    'vote_scores': version_scores,
                    'votes': votes
                }
        return None

    def update_trust_weight(self, registry_id: str, success: bool):
        if registry_id not in self.voting_weights:
            self.voting_weights[registry_id] = 0.5
        if success:
            self.voting_weights[registry_id] = min(1.0, self.voting_weights[registry_id] + 0.05)
        else:
            self.voting_weights[registry_id] = max(0.0, self.voting_weights[registry_id] - 0.1)

    def _serialize_local_experts(self) -> List[Dict]:
        return [
            expert.model_dump()
            for expert in self.registry._experts.values()
            if expert.lifecycle_state.is_available()
        ][:100]

    def get_sync_status(self) -> Dict[str, Any]:
        return {
            'remote_registries': self.registry._remote_registries,
            'federated_experts': len(self.registry._federated_experts),
            'last_sync': list(self.sync_history)[-5:] if self.sync_history else [],
            'total_syncs': len(self.sync_history),
            'voting_weights': self.voting_weights,
            'conflict_resolutions': sum(1 for h in self.sync_history if h.get('resolved', 0) > 0),
            'circuit_open': self._circuit_breaker.state == "open"
        }

# -----------------------------------------------------------------------------
# Enhanced Expert Registry (Main Class) - v6.4.0
# -----------------------------------------------------------------------------
class ExpertRegistry:
    """
    Enhanced Expert Registry v6.4.0 - Complete Bio-Inspired Genome Repository
    Fully integrated with Green Agent MOPD ecosystem.
    """

    def __init__(
        self,
        storage: Storage,
        message_queue: AsyncMessageQueue,
        adaptive_cost: AdaptiveCostFunction,
        pareto_gating: ParetoGating,
        drift_detector: DriftDetector,
        metrics: MetricsRegistry
    ):
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        self.config = ExpertRegistryConfig()
        self.registry_id = self.config.registry_id

        # Feature flags
        self.enable_bio_correlation = self.config.enable_bio_correlation and BIO_INSPIRED_AVAILABLE
        self.enable_natural_selection = self.config.enable_natural_selection
        self.enable_fitness_tracking = self.config.enable_fitness_tracking
        self.enable_population_tracking = self.config.enable_population_tracking
        self.enable_sustainability_dashboard = self.config.enable_sustainability_dashboard
        self.enable_predictive_forecasting = self.config.enable_predictive_forecasting
        self.enable_cross_region_sync = self.config.enable_cross_region_sync
        self.enable_quantum_efficiency = self.config.enable_quantum_efficiency
        self.enable_reproductive_strategies = self.config.enable_reproductive_strategies
        self.enable_climate_integration = self.config.enable_climate_integration

        # External integrations
        self.tick_engine: Optional[Any] = None
        self.quantum_bridge: Optional[Any] = None

        # Core storage
        self._experts: Dict[str, ExpertProfile] = {}
        self._domain_index: Dict[ExpertDomain, Set[str]] = defaultdict(set)
        self._hardware_index: Dict[HardwareProfile, Set[str]] = defaultdict(set)
        self._lifecycle_index: Dict[ExpertLifecycleState, Set[str]] = defaultdict(set)
        self._tag_index: Dict[str, Set[str]] = defaultdict(set)
        self._capability_index: Dict[str, Set[str]] = defaultdict(set)
        self._task_type_index: Dict[str, Set[str]] = defaultdict(set)
        self._region_index: Dict[str, Set[str]] = defaultdict(set)
        self._version_family_index: Dict[str, List[str]] = defaultdict(list)

        # Fitness tracking
        self.fitness_scores: Dict[str, FitnessScore] = {}

        # Performance history
        self._performance_history: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        # Dependency graph
        self._dependency_graph = nx.DiGraph()

        # Federation
        self._remote_registries: Dict[str, str] = {}
        self._federated_experts: Dict[str, str] = {}

        # A/B testing
        self._ab_tests: Dict[str, Dict[str, Any]] = {}

        # Migration paths
        self._migration_paths: Dict[str, str] = {}

        # Evolutionary tracking
        self.evolutionary_events: deque = deque(maxlen=10000)
        self.speciation_count: int = 0
        self.extinction_count: int = 0
        self.total_generations: int = 0
        self.reproductive_events: int = 0

        # Statistics
        self._stats = {
            'total_registrations': 0,
            'total_deregistrations': 0,
            'total_natural_selections': 0,
            'last_selection': None
        }

        # Bio-inspired module references
        self.token_manager: Optional[EcoATPTokenManager] = None
        self.gradient_manager: Optional[GradientFieldManager] = None
        self.compartment_manager: Optional[CompartmentManager] = None
        self.biomass_storage: Optional[BiomassStorage] = None

        # Sub-managers
        self.bio_correlator: Optional[BioCorrelator] = None
        self.fitness_manager: Optional[FitnessManager] = None
        self.sustainability_dashboard: Optional[RegistrySustainabilityDashboard] = None
        self.predictive_forecaster: Optional[PredictiveEvolutionForecaster] = None
        self.cross_region_sync: Optional[CrossRegionRegistrySynchronizer] = None

        # Locks
        self._lock = asyncio.Lock()
        self._index_lock = asyncio.Lock()
        self._fitness_lock = asyncio.Lock()
        self._performance_lock = asyncio.Lock()

        # Rate limiter
        self._rate_limiter = RateLimiter(self.config.rate_limit_per_minute)

        # Initialization status
        self._initialization_lock = asyncio.Lock()
        self._init_task: Optional[asyncio.Task] = None
        self._ready = False
        self._init_exception: Optional[Exception] = None

        # Start async initialization
        self._init_task = asyncio.create_task(self._async_init())

        logger.info(f"Expert Registry v6.4.0 initialization started...")

    async def _async_init(self):
        """Async initialization of sub-managers and state loading."""
        try:
            # Initialize sub-managers
            if self.enable_sustainability_dashboard:
                self.sustainability_dashboard = RegistrySustainabilityDashboard(self)
            if self.enable_predictive_forecasting:
                self.predictive_forecaster = PredictiveEvolutionForecaster(self)
            if self.enable_cross_region_sync:
                self.cross_region_sync = CrossRegionRegistrySynchronizer(self)
            # Bio-correlator and fitness manager
            if self.enable_bio_correlation:
                self.bio_correlator = BioCorrelator(self)
            self.fitness_manager = FitnessManager(self)

            # Load state from central storage (if any)
            await self._load_state_from_storage()

            async with self._initialization_lock:
                self._ready = True
            logger.info("Expert Registry v6.4.0 initialization complete.")
        except Exception as e:
            logger.error(f"Initialization failed: {e}", exc_info=True)
            async with self._initialization_lock:
                self._init_exception = e
                self._ready = False
            raise

    async def wait_until_ready(self, timeout: Optional[float] = None) -> bool:
        """Wait until initialization is complete."""
        try:
            if self._init_task:
                await asyncio.wait_for(self._init_task, timeout=timeout)
        except asyncio.TimeoutError:
            logger.error("Initialization timed out")
            return False
        async with self._initialization_lock:
            if self._init_exception:
                raise self._init_exception
            return self._ready

    @property
    def is_ready(self) -> bool:
        return self._ready

    # ----------------------------------------------------------------------
    # State Persistence using central Storage
    # ----------------------------------------------------------------------
    async def _load_state_from_storage(self):
        """Load registry state from central storage."""
        try:
            data = self.storage.get_state("expert_registry_state")
            if data:
                state = json.loads(data)
                # Restore experts
                experts_data = state.get("experts", {})
                for expert_id, exp_data in experts_data.items():
                    profile = ExpertProfile.model_validate(exp_data)
                    self._experts[expert_id] = profile
                    self._update_indexes(profile)
                # Restore fitness scores
                fitness_data = state.get("fitness_scores", {})
                for expert_id, fs_data in fitness_data.items():
                    fs = FitnessScore.model_validate(fs_data)
                    self.fitness_scores[expert_id] = fs
                # Restore other state
                self.speciation_count = state.get("speciation_count", 0)
                self.extinction_count = state.get("extinction_count", 0)
                self.total_generations = state.get("total_generations", 0)
                self.reproductive_events = state.get("reproductive_events", 0)
                self._stats = state.get("stats", self._stats)
                # Indices can be rebuilt from experts
                logger.info("Loaded expert registry state from storage")
        except Exception as e:
            logger.error(f"Failed to load registry state: {e}")

    async def save_state(self):
        """Save registry state to central storage."""
        try:
            state = {
                "experts": {eid: exp.model_dump() for eid, exp in self._experts.items()},
                "fitness_scores": {eid: fs.model_dump() for eid, fs in self.fitness_scores.items()},
                "speciation_count": self.speciation_count,
                "extinction_count": self.extinction_count,
                "total_generations": self.total_generations,
                "reproductive_events": self.reproductive_events,
                "stats": self._stats,
            }
            self.storage.save_state("expert_registry_state", json.dumps(state))
            logger.info("Saved registry state to storage")
        except Exception as e:
            logger.error(f"Failed to save registry state: {e}")

    # ----------------------------------------------------------------------
    # External Module Injection
    # ----------------------------------------------------------------------
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        if bio_core:
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.compartment_manager = getattr(bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(bio_core, 'biomass_storage', None)
        else:
            self.token_manager = kwargs.get('token_manager')
            self.gradient_manager = kwargs.get('gradient_manager')
            self.compartment_manager = kwargs.get('compartment_manager')
            self.biomass_storage = kwargs.get('biomass_storage')
        if self.enable_bio_correlation and self.bio_correlator is None:
            self.bio_correlator = BioCorrelator(self)
        if self.enable_bio_correlation:
            logger.info("Bio-inspired modules injected into Expert Registry")

    def inject_tick_engine(self, tick_engine: Any):
        self.tick_engine = tick_engine
        logger.info("TimeTickEngine injected into Expert Registry")

    def inject_quantum_bridge(self, quantum_bridge: Any):
        self.quantum_bridge = quantum_bridge
        logger.info("QuantumBridge injected into Expert Registry")

    # ----------------------------------------------------------------------
    # Teacher Interface for MOPD
    # ----------------------------------------------------------------------
    async def policy_probs(self, state: Dict) -> List[float]:
        """
        Return a probability distribution over experts based on fitness scores.
        This allows the MTPD optimizer to treat this module as a teacher.
        """
        experts = self.get_all_active_experts()
        if not experts:
            return []
        # Use fitness scores as logits (with a softmax)
        logits = [self.fitness_scores.get(e.expert_id, FitnessScore(expert_id=e.expert_id)).overall_fitness for e in experts]
        # Avoid division by zero
        logits = np.array(logits)
        logits = np.exp(logits - np.max(logits))
        probs = (logits / np.sum(logits)).tolist()
        return probs

    # ----------------------------------------------------------------------
    # Expert Registration (Enhanced with FeedbackEvent and Pareto gating)
    # ----------------------------------------------------------------------
    async def register_expert(
        self,
        profile: ExpertProfile,
        validate: bool = True,
        auto_certify: bool = False,
        create_ecoatp_account: bool = True,
        register_compartment: bool = True
    ) -> Tuple[bool, str]:
        await self._ensure_ready()
        if not await self._rate_limiter.acquire():
            return False, "Rate limit exceeded, please try later"

        async with self._lock:
            if profile.expert_id in self._experts:
                existing = self._experts[profile.expert_id]
                if profile.version.is_newer_than(existing.version):
                    logger.info(f"Updating expert {profile.expert_id} from "
                               f"v{existing.version.to_string()} to v{profile.version.to_string()}")
                    existing.lifecycle_state = ExpertLifecycleState.ARCHIVED
                    profile.replaces_expert = existing.expert_id
                    self._migration_paths[existing.expert_id] = profile.expert_id
                else:
                    return False, f"Expert {profile.expert_id} already registered with newer version"

            if validate:
                is_valid, message = self._validate_profile(profile)
                if not is_valid:
                    return False, f"Validation failed: {message}"

            if auto_certify:
                profile.lifecycle_state = ExpertLifecycleState.CERTIFIED
            elif validate:
                profile.lifecycle_state = ExpertLifecycleState.VALIDATING
            else:
                profile.lifecycle_state = ExpertLifecycleState.REGISTERED

            profile.health.quantum_efficiency = self._calculate_quantum_efficiency(profile)
            profile.sustainability_score = profile.health.calculate_sustainability_score()

            self._experts[profile.expert_id] = profile
            self._update_indexes(profile)

            # Eco-ATP account
            if self.enable_bio_correlation and create_ecoatp_account and self.token_manager:
                account_id = f"expert_{profile.expert_id}"
                self.token_manager.create_account(account_id)
                initial_tokens = int(profile.efficiency_score * 100)
                if initial_tokens > 0:
                    self.token_manager.generate_tokens(
                        account_id=account_id,
                        source=EcoATPSource.EFFICIENCY_GAIN,
                        energy_saved_kwh=profile.efficiency_score * 0.001,
                        num_tokens=initial_tokens
                    )
                logger.info(f"Created Eco-ATP account for {profile.expert_id}: {initial_tokens} tokens")

            # Chromatophore compartment
            if self.enable_bio_correlation and register_compartment and self.compartment_manager:
                species = self.bio_correlator.get_species_id(profile)
                self.compartment_manager.create_compartment(
                    expert_type=species,
                    expert_instance=None
                )
                logger.info(f"Created chromatophore compartment for {profile.expert_id}")

            # Fitness score
            if self.enable_fitness_tracking:
                fitness = FitnessScore(
                    expert_id=profile.expert_id,
                    resource_efficiency=min(1.0, 1.0 / (1.0 + profile.carbon_per_inference * 10000)),
                    resilience_score=profile.reliability_score,
                    adaptation_speed=0.5,
                    cooperation_score=0.5,
                    ecoatp_efficiency=profile.efficiency_score,
                    sustainability_score=profile.sustainability_score,
                    quantum_efficiency=profile.health.quantum_efficiency,
                    quantum_advantage=self._calculate_quantum_advantage(profile),
                    helium_savings=1.0 - profile.helium_per_inference / max(profile.helium_per_inference, 1)
                )
                fitness.calculate_overall(self.config.fitness_weights)
                self.fitness_scores[profile.expert_id] = fitness

            self._update_dependency_graph(profile)
            self._version_family_index[profile.expert_name].append(profile.expert_id)
            self._stats['total_registrations'] += 1
            self.total_generations += 1

            self.evolutionary_events.append({
                'type': 'speciation' if not profile.replaces_expert else 'evolution',
                'expert_id': profile.expert_id,
                'species': self.bio_correlator.get_species_id(profile) if self.bio_correlator else 'general',
                'generation': self.total_generations,
                'quantum_capable': profile.quantum_capable,
                'timestamp': datetime.utcnow().isoformat()
            })
            self.speciation_count += 1

            logger.info(f"Registered expert: {profile.expert_id} v{profile.version.to_string()} "
                       f"(species: {self.bio_correlator.get_species_id(profile) if self.bio_correlator else 'general'}, "
                       f"quantum: {profile.quantum_capable}, "
                       f"generation: {self.total_generations})")

            # Publish FeedbackEvent
            event = FeedbackEvent.create_with_context(
                task_id=f"reg_{profile.expert_id}",
                selected_action="register",
                quality_score=fitness.overall_fitness if self.enable_fitness_tracking else 0.5,
                energy_joules=0.0,
                carbon_g=0.0,
                feedback_type="registry",
                adaptive_cost_value=0.0,
                state={'expert_id': profile.expert_id, 'action': 'register'},
                candidates=[{'action': 'register', 'deprecate', 'activate'}],
                source="expert_registry",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["registry", "expert"]
            )
            await self.queue.publish("feedback_events", event.to_json())

            # Check drift
            if self.drift:
                await self.drift.check_drift(self.adaptive_cost.get_current_weights())

            return True, f"Expert {profile.expert_id} registered successfully"

    def _validate_profile(self, profile: ExpertProfile) -> Tuple[bool, str]:
        errors = []
        if not profile.expert_id: errors.append("expert_id is required")
        if not profile.expert_name: errors.append("expert_name is required")
        if profile.version.major < 0: errors.append("Invalid version")
        for score_name, score_value in [('accuracy_score', profile.accuracy_score),
                                         ('reliability_score', profile.reliability_score),
                                         ('efficiency_score', profile.efficiency_score)]:
            if not (0.0 <= score_value <= 1.0): errors.append(f"{score_name} must be between 0 and 1")
        for metric_name, metric_value in [('helium_per_inference', profile.helium_per_inference),
                                           ('carbon_per_inference', profile.carbon_per_inference),
                                           ('energy_per_inference', profile.energy_per_inference)]:
            if metric_value < 0: errors.append(f"{metric_name} cannot be negative")
        for dep in profile.dependencies:
            if not dep.is_optional and dep.dependency_id not in self._experts:
                errors.append(f"Required dependency {dep.dependency_id} not registered")
            if dep.dependency_id in self._experts:
                dep_expert = self._experts[dep.dependency_id]
                if not self._check_version_compatibility(dep_expert.version, dep.version_requirement):
                    errors.append(f"Dependency {dep.dependency_id} version {dep_expert.version.to_string()} "
                                 f"does not satisfy requirement {dep.version_requirement}")
        for incompatible_id in profile.incompatible_with:
            if incompatible_id == profile.expert_id:
                errors.append("Cannot be incompatible with self")
        if profile.quantum_capable and profile.quantum_qubits < 1:
            errors.append("Quantum capable experts must have at least 1 qubit")
        if profile.quantum_capable and not profile.quantum_backend:
            errors.append("Quantum capable experts must specify a quantum backend")
        if errors: return False, "; ".join(errors)
        return True, "Profile valid"

    def _check_version_compatibility(self, version: ExpertVersion, requirement: str) -> bool:
        if requirement.startswith('>='):
            req_ver = ExpertVersion.from_string(requirement[2:])
            return version.is_newer_than(req_ver) or version == req_ver
        elif requirement.startswith('<='):
            req_ver = ExpertVersion.from_string(requirement[2:])
            return not version.is_newer_than(req_ver) or version == req_ver
        elif requirement.startswith('=='):
            req_ver = ExpertVersion.from_string(requirement[2:])
            return version == req_ver
        elif requirement.startswith('>'):
            req_ver = ExpertVersion.from_string(requirement[1:])
            return version.is_newer_than(req_ver)
        elif requirement.startswith('<'):
            req_ver = ExpertVersion.from_string(requirement[1:])
            return not version.is_newer_than(req_ver) and version != req_ver
        else:
            return version == ExpertVersion.from_string(requirement)

    def _calculate_quantum_efficiency(self, profile: ExpertProfile) -> float:
        if not profile.quantum_capable:
            return 0.0
        qubit_eff = min(1.0, profile.quantum_qubits / 50)
        accuracy_eff = profile.accuracy_score
        helium_eff = 1.0 / (1.0 + profile.helium_per_inference * 10)
        return (qubit_eff * 0.3 + accuracy_eff * 0.4 + helium_eff * 0.3)

    def _calculate_quantum_advantage(self, profile: ExpertProfile) -> float:
        if not profile.quantum_capable:
            return 0.0
        return min(1.0, (profile.quantum_qubits / 100) * 0.5 + profile.accuracy_score * 0.5)

    def _update_indexes(self, profile: ExpertProfile):
        async with self._index_lock:
            self._domain_index[profile.domain].add(profile.expert_id)
            self._hardware_index[profile.hardware_profile].add(profile.expert_id)
            self._lifecycle_index[profile.lifecycle_state].add(profile.expert_id)
            for tag in profile.tags: self._tag_index[tag].add(profile.expert_id)
            for cap in profile.capabilities: self._capability_index[cap].add(profile.expert_id)
            for tt in profile.supported_task_types: self._task_type_index[tt].add(profile.expert_id)
            self._region_index[profile.origin_region].add(profile.expert_id)

    def _update_dependency_graph(self, profile: ExpertProfile):
        self._dependency_graph.add_node(profile.expert_id, name=profile.expert_name,
                                        version=profile.version.to_string(),
                                        quantum=profile.quantum_capable)
        for dep in profile.dependencies:
            self._dependency_graph.add_edge(profile.expert_id, dep.dependency_id,
                                           optional=dep.is_optional,
                                           version_req=dep.version_requirement)

    # ----------------------------------------------------------------------
    # Filtering Methods (Enhanced with adaptive cost and Pareto gating)
    # ----------------------------------------------------------------------
    async def filter_by_ecoatp_efficiency(self, min_efficiency: float = 0.5, min_token_balance: float = 10.0) -> List[ExpertProfile]:
        await self._ensure_ready()
        if not await self._rate_limiter.acquire():
            return []
        if not self.enable_bio_correlation or not self.token_manager:
            return self.get_all_active_experts()
        efficient = []
        for expert_id, expert in self._experts.items():
            if not expert.lifecycle_state.is_available():
                continue
            efficiency = await self.bio_correlator.get_expert_ecoatp_efficiency(expert_id)
            balance = await self.bio_correlator.get_expert_token_balance(expert_id)
            if efficiency >= min_efficiency and balance >= min_token_balance:
                efficient.append(expert)
        return efficient

    def filter_by_health_and_fitness(self, min_health: float = 0.5, min_fitness: float = 0.4) -> List[ExpertProfile]:
        # Use adaptive cost weights to adjust thresholds
        if self.adaptive_cost:
            weights = self.adaptive_cost.get_current_weights()
            # Example: adjust min_fitness based on carbon/cost weights
            carbon_weight = weights.get('carbon', 0.3)
            cost_weight = weights.get('cost', 0.3)
            adjustment = (carbon_weight + cost_weight) * 0.1
            min_fitness = max(0.2, min(0.8, min_fitness + adjustment))
        qualified = []
        for expert_id, expert in self._experts.items():
            if not expert.lifecycle_state.is_available():
                continue
            health = expert.health.calculate_health_score()
            fitness = self.fitness_scores.get(expert_id, FitnessScore(expert_id=expert_id)).overall_fitness
            if health >= min_health and fitness >= min_fitness:
                qualified.append(expert)
        return qualified

    def filter_by_sustainability_score(self, min_sustainability: float = 0.5) -> List[ExpertProfile]:
        # Use Pareto gating to enforce constraints
        candidates = []
        for e in self._experts.values():
            if e.lifecycle_state.is_available() and e.sustainability_score >= min_sustainability:
                candidates.append({
                    'expert_id': e.expert_id,
                    'sustainability_score': e.sustainability_score,
                    'carbon_per_inference': e.carbon_per_inference,
                    'helium_per_inference': e.helium_per_inference,
                    'quality_score': e.accuracy_score
                })
        # Apply Pareto filter (assuming we have a list of candidates)
        if self.pareto:
            filtered = self.pareto.filter(candidates)
            if filtered:
                # Return only experts that passed Pareto
                allowed_ids = {c['expert_id'] for c in filtered}
                return [e for e in self._experts.values() if e.expert_id in allowed_ids]
        # Fallback: return all with sustainability >= threshold
        return [e for e in self._experts.values() if e.lifecycle_state.is_available() and e.sustainability_score >= min_sustainability]

    def filter_by_quantum_efficiency(self, min_quantum_efficiency: float = 0.3) -> List[ExpertProfile]:
        return [e for e in self._experts.values() if e.lifecycle_state.is_available() and e.health.quantum_efficiency >= min_quantum_efficiency]

    async def filter_by_gradient_alignment(self, carbon_threshold: float = 0.3, trust_threshold: float = 0.4) -> List[ExpertProfile]:
        await self._ensure_ready()
        if not await self._rate_limiter.acquire():
            return []
        if not self.enable_bio_correlation or not self.gradient_manager:
            return self.get_all_active_experts()
        carbon_strength = await self.bio_correlator.get_gradient_strength('carbon')
        trust_strength = await self.bio_correlator.get_gradient_strength('trust')
        if carbon_strength > carbon_threshold:
            return sorted([e for e in self.get_all_active_experts()], key=lambda e: e.carbon_per_inference)[:max(1, len(self._experts) // 2)]
        if trust_strength < trust_threshold:
            return sorted([e for e in self.get_all_active_experts()], key=lambda e: e.reliability_score, reverse=True)[:max(1, len(self._experts) // 2)]
        return self.get_all_active_experts()

    # ----------------------------------------------------------------------
    # Natural Selection (Forward to FitnessManager)
    # ----------------------------------------------------------------------
    async def trigger_natural_selection(self):
        await self._ensure_ready()
        if self.fitness_manager:
            await self.fitness_manager.trigger_natural_selection()

    # ----------------------------------------------------------------------
    # Reproductive Strategies (Forward to FitnessManager)
    # ----------------------------------------------------------------------
    async def _reproductive_strategy_loop(self):
        while True:
            try:
                if self.enable_reproductive_strategies and self.fitness_manager:
                    candidates = []
                    for expert_id, fitness in self.fitness_scores.items():
                        if expert_id not in self._experts:
                            continue
                        if (fitness.overall_fitness > 0.7 and
                            fitness.reproductive_success > 0 and
                            self._experts[expert_id].lifecycle_state.is_available()):
                            candidates.append((expert_id, fitness))
                    for expert_id, fitness in candidates[:5]:
                        await self._reproduce_expert(expert_id, fitness)
                await asyncio.sleep(3600)
            except Exception as e:
                logger.error(f"Reproductive strategy loop error: {e}")
                await asyncio.sleep(600)

    async def _reproduce_expert(self, expert_id: str, fitness: FitnessScore):
        parent = self._experts[expert_id]
        offspring_id = f"{expert_id}_offspring_{self.reproductive_events}"
        offspring_version = ExpertVersion(
            major=parent.version.major,
            minor=parent.version.minor,
            patch=parent.version.patch + 1
        )
        mutation_rate = self.config.reproductive_mutation_rate
        offspring_accuracy = min(1.0, parent.accuracy_score + np.random.normal(0, 0.05))
        offspring_efficiency = min(1.0, parent.efficiency_score + np.random.normal(0, 0.05))
        offspring_quantum_qubits = max(1, parent.quantum_qubits + np.random.randint(-2, 3))

        offspring = ExpertProfile(
            expert_id=offspring_id,
            expert_name=f"{parent.expert_name}_offspring",
            version=offspring_version,
            domain=parent.domain,
            hardware_profile=parent.hardware_profile,
            accuracy_score=offspring_accuracy,
            efficiency_score=offspring_efficiency,
            helium_per_inference=parent.helium_per_inference * (0.9 + np.random.random() * 0.2),
            carbon_per_inference=parent.carbon_per_inference * (0.9 + np.random.random() * 0.2),
            energy_per_inference=parent.energy_per_inference * (0.9 + np.random.random() * 0.2),
            quantum_capable=parent.quantum_capable,
            quantum_qubits=offspring_quantum_qubits,
            quantum_backend=parent.quantum_backend,
            sustainability_score=parent.sustainability_score,
            health=HealthMetrics(
                success_rate=parent.health.success_rate,
                quantum_efficiency=parent.health.quantum_efficiency * (0.9 + np.random.random() * 0.2)
            )
        )
        success, msg = await self.register_expert(offspring, validate=False, auto_certify=True)
        if success:
            if parent.lineage is None:
                parent.lineage = ExpertLineage(lineage_id=f"lineage_{parent.expert_id}", parent_expert_id=None)
            parent.lineage.reproductive_offspring.append(offspring_id)
            parent.lineage.mutation_count += 1
            fitness.reproductive_success += 1
            self.reproductive_events += 1
            logger.info(f"Reproduced expert {offspring_id} from {expert_id}")

    # ----------------------------------------------------------------------
    # Deprecation and Activation (with FeedbackEvent)
    # ----------------------------------------------------------------------
    def deprecate_expert(self, expert_id: str, replacement_id: Optional[str] = None, reason: str = "manual") -> Tuple[bool, str]:
        if expert_id not in self._experts:
            return False, f"Expert {expert_id} not found"
        profile = self._experts[expert_id]
        profile.lifecycle_state = ExpertLifecycleState.DEPRECATED
        profile.is_active = False
        if replacement_id and replacement_id in self._experts:
            profile.replaced_by = replacement_id
            self._migration_paths[expert_id] = replacement_id
        self._lifecycle_index[ExpertLifecycleState.DEPRECATED].add(expert_id)
        logger.info(f"Deprecated expert: {expert_id} (reason: {reason}, replacement: {replacement_id})")

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"deprecate_{expert_id}",
            selected_action="deprecate",
            quality_score=0.0,  # or fitness if available
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="registry",
            adaptive_cost_value=0.0,
            state={'expert_id': expert_id, 'reason': reason},
            candidates=[{'action': 'deprecate'}],
            source="expert_registry",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["registry", "expert"]
        )
        asyncio.create_task(self.queue.publish("feedback_events", event.to_json()))

        return True, f"Expert {expert_id} deprecated"

    def activate_expert(self, expert_id: str) -> Tuple[bool, str]:
        if expert_id not in self._experts:
            return False, f"Expert {expert_id} not found"
        profile = self._experts[expert_id]
        if not profile.lifecycle_state.is_available():
            return False, f"Expert {expert_id} is not in certifiable state"
        profile.lifecycle_state = ExpertLifecycleState.ACTIVE
        profile.activated_at = datetime.utcnow()
        profile.is_active = True
        self._lifecycle_index[ExpertLifecycleState.ACTIVE].add(expert_id)
        logger.info(f"Activated expert: {expert_id}")

        # Publish FeedbackEvent
        event = FeedbackEvent.create_with_context(
            task_id=f"activate_{expert_id}",
            selected_action="activate",
            quality_score=profile.health.calculate_health_score(),
            energy_joules=0.0,
            carbon_g=0.0,
            feedback_type="registry",
            adaptive_cost_value=0.0,
            state={'expert_id': expert_id},
            candidates=[{'action': 'activate'}],
            source="expert_registry",
            environment=getattr(central_config, "ENVIRONMENT", "production"),
            tags=["registry", "expert"]
        )
        asyncio.create_task(self.queue.publish("feedback_events", event.to_json()))

        return True, f"Expert {expert_id} activated"

    # ----------------------------------------------------------------------
    # Performance Tracking (Enhanced with FeedbackEvent)
    # ----------------------------------------------------------------------
    async def update_performance(self, expert_id: str, metrics: Dict[str, Any]):
        await self._ensure_ready()
        if not await self._rate_limiter.acquire():
            return

        if expert_id not in self._experts:
            return

        async with self._performance_lock:
            self._performance_history[expert_id].append({
                **metrics,
                'timestamp': datetime.utcnow().isoformat()
            })
            if len(self._performance_history[expert_id]) > 10000:
                self._performance_history[expert_id] = self._performance_history[expert_id][-10000:]

            expert = self._experts[expert_id]
            if 'success' in metrics:
                alpha = 0.1
                expert.health.success_rate = expert.health.success_rate * (1 - alpha) + (1.0 if metrics['success'] else 0.0) * alpha
            if 'latency_ms' in metrics:
                expert.health.avg_latency_ms = metrics['latency_ms']
            if 'carbon_kg' in metrics:
                expert.health.carbon_efficiency = 1.0 / (1.0 + metrics['carbon_kg'] * 1000)
            if 'helium_units' in metrics:
                expert.health.helium_efficiency = 1.0 / (1.0 + metrics['helium_units'] * 100)
            if 'quantum_accuracy' in metrics:
                expert.health.quantum_efficiency = metrics['quantum_accuracy']
            if 'quantum_advantage' in metrics:
                expert.health.quantum_advantage_score = metrics['quantum_advantage']
            expert.health.last_heartbeat = datetime.utcnow()
            expert.sustainability_score = expert.health.calculate_sustainability_score()

            if self.enable_fitness_tracking and expert_id in self.fitness_scores:
                fitness = self.fitness_scores[expert_id]
                if 'success' in metrics:
                    fitness.resilience_score = fitness.resilience_score * 0.8 + (1.0 if metrics['success'] else 0.0) * 0.2
                if 'carbon_kg' in metrics:
                    fitness.resource_efficiency = 1.0 / (1.0 + metrics['carbon_kg'] * 10000)
                if 'ecoatp_efficiency' in metrics:
                    fitness.ecoatp_efficiency = metrics['ecoatp_efficiency']
                if 'quantum_accuracy' in metrics:
                    fitness.quantum_efficiency = metrics['quantum_accuracy']
                fitness.sustainability_score = expert.sustainability_score
                fitness.calculate_overall(self.config.fitness_weights)

            if self.enable_bio_correlation and self.gradient_manager:
                trust_delta = 0.05 if metrics.get('success', False) else -0.1
                self.gradient_manager.pump_field('trust', trust_delta, source=f"expert_{expert_id}")

            health_score = expert.health.calculate_health_score()
            if health_score < 0.3 and expert.lifecycle_state == ExpertLifecycleState.ACTIVE:
                expert.lifecycle_state = ExpertLifecycleState.DEGRADED
                logger.warning(f"Expert {expert_id} auto-degraded (health: {health_score:.2f})")
            elif health_score > 0.7 and expert.lifecycle_state == ExpertLifecycleState.DEGRADED:
                expert.lifecycle_state = ExpertLifecycleState.ACTIVE
                logger.info(f"Expert {expert_id} auto-recovered (health: {health_score:.2f})")

            # Publish FeedbackEvent for performance update
            event = FeedbackEvent.create_with_context(
                task_id=f"perf_{expert_id}",
                selected_action="update_performance",
                quality_score=expert.health.calculate_health_score(),
                energy_joules=metrics.get('energy_joules', 0.0),
                carbon_g=metrics.get('carbon_kg', 0.0) * 1000,
                feedback_type="registry",
                adaptive_cost_value=0.0,
                state={'expert_id': expert_id, 'metrics': metrics},
                candidates=[{'action': 'update_performance'}],
                source="expert_registry",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["registry", "performance"]
            )
            await self.queue.publish("feedback_events", event.to_json())

            # Check drift
            if self.drift:
                await self.drift.check_drift(self.adaptive_cost.get_current_weights())

    # ----------------------------------------------------------------------
    # Dynamic Fitness Weights API
    # ----------------------------------------------------------------------
    def update_fitness_weights(self, new_weights: Dict[str, float]):
        if self.fitness_manager:
            self.fitness_manager.update_fitness_weights(new_weights)
        else:
            logger.warning("Fitness manager not available, cannot update weights")

    # ----------------------------------------------------------------------
    # Background Tasks
    # ----------------------------------------------------------------------
    async def _ensure_ready(self):
        if not self.is_ready:
            raise RuntimeError("Expert Registry not fully initialized. Call wait_until_ready() first.")

    # ----------------------------------------------------------------------
    # Statistics and Reporting (Enhanced with central metrics)
    # ----------------------------------------------------------------------
    def get_registry_stats(self) -> Dict[str, Any]:
        if not self.is_ready:
            return {'status': 'not_initialized'}
        total = len(self._experts)
        available = len(self.get_all_active_experts())
        stats = {
            'registry_id': self.registry_id,
            'total_experts': total,
            'available_experts': available,
            'degraded_experts': len(self._lifecycle_index.get(ExpertLifecycleState.DEGRADED, set())),
            'deprecated_experts': len(self._lifecycle_index.get(ExpertLifecycleState.DEPRECATED, set())),
            'domains': {domain.value: len(experts) for domain, experts in self._domain_index.items()},
            'hardware_distribution': {hw.value: len(experts) for hw, experts in self._hardware_index.items()},
            'lifecycle_distribution': {state.value: len(self._lifecycle_index.get(state, set())) for state in ExpertLifecycleState},
            'bio_correlation_enabled': self.enable_bio_correlation,
            'bio_modules_available': BIO_INSPIRED_AVAILABLE,
            'sustainability_score': np.mean([e.sustainability_score for e in self._experts.values()]) if self._experts else 0,
            'quantum_experts': sum(1 for e in self._experts.values() if e.quantum_capable),
            'avg_quantum_efficiency': np.mean([e.health.quantum_efficiency for e in self._experts.values() if e.quantum_capable]) if self._experts else 0,
            'evolution': {
                'total_generations': self.total_generations,
                'speciation_events': self.speciation_count,
                'extinction_events': self.extinction_count,
                'reproductive_events': self.reproductive_events,
                'natural_selections': self._stats['total_natural_selections'],
                'last_selection': self._stats['last_selection'].isoformat() if self._stats['last_selection'] else None,
                'average_fitness': np.mean([f.overall_fitness for f in self.fitness_scores.values()]) if self.fitness_scores else 0,
                'top_fitness': max([f.overall_fitness for f in self.fitness_scores.values()]) if self.fitness_scores else 0,
                'top_quantum_fitness': max([f.quantum_efficiency for f in self.fitness_scores.values()]) if self.fitness_scores else 0
            },
            'adaptive_fitness_weights': self.config.fitness_weights,
            'persistence_enabled': self.enable_persistence,
            'circuit_breaker_open': self.cross_region_sync._circuit_breaker.state == "open" if self.cross_region_sync else False,
            'tick_engine_integrated': self.tick_engine is not None,
            'quantum_bridge_integrated': self.quantum_bridge is not None,
        }
        if self.enable_population_tracking and self.bio_correlator:
            stats['species_populations'] = self.bio_correlator.get_species_populations()
        if self.enable_sustainability_dashboard and self.sustainability_dashboard:
            stats['dashboard'] = self.sustainability_dashboard.get_dashboard_status()
            stats['predictive_alerts'] = self.sustainability_dashboard.get_predictive_alerts()
        if self.enable_predictive_forecasting and self.predictive_forecaster:
            stats['forecast'] = self.predictive_forecaster.forecast_history[-1] if self.predictive_forecaster.forecast_history else None
        if self.enable_cross_region_sync and self.cross_region_sync:
            stats['sync'] = self.cross_region_sync.get_sync_status()

        # Update central metrics
        self.metrics.set_total_experts(total)
        self.metrics.set_active_experts(available)
        self.metrics.set_avg_sustainability_score(stats['sustainability_score'])
        self.metrics.set_avg_fitness(stats['evolution']['average_fitness'])

        return stats

    def get_all_active_experts(self) -> List[ExpertProfile]:
        return [e for e in self._experts.values() if e.lifecycle_state.is_available() and e.is_active]

    def get_export_metrics(self) -> Dict[str, float]:
        if self.enable_sustainability_dashboard and self.sustainability_dashboard:
            return self.sustainability_dashboard.export_metrics()
        return {}

    # ----------------------------------------------------------------------
    # Helper for species populations (internal)
    # ----------------------------------------------------------------------
    def _get_species_populations(self) -> Dict[str, int]:
        if self.bio_correlator:
            return self.bio_correlator.get_species_populations()
        species = ['energy', 'data', 'iot', 'quantum', 'helium', 'general']
        counts = {}
        for sp in species:
            counts[sp] = len([e for e in self._experts.values() if self.bio_correlator and self.bio_correlator.get_species_id(e) == sp])
        return counts

    # ----------------------------------------------------------------------
    # Shutdown
    # ----------------------------------------------------------------------
    async def shutdown(self):
        logger.info("Shutting down Expert Registry")
        await self.save_state()
        if self.cross_region_sync and self.cross_region_sync._session:
            await self.cross_region_sync._session.close()
        # Cancel background tasks
        tasks = [t for t in asyncio.all_tasks() if t.get_name().startswith("ExpertRegistry_")]
        for task in tasks:
            task.cancel()
        logger.info("Shutdown complete")

# -----------------------------------------------------------------------------
# Example Usage (if run directly)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    async def main():
        # In a real deployment, these would be provided by LifecycleManager.
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

        registry = ExpertRegistry(storage, queue, adaptive_cost, pareto, drift, metrics)
        await registry.wait_until_ready()

        # Register an expert
        profile = ExpertProfile(
            expert_id="expert_001",
            expert_name="EcoOptimizer",
            domain=ExpertDomain.ENERGY,
            accuracy_score=0.85,
            reliability_score=0.9,
            efficiency_score=0.8,
            quantum_capable=False
        )
        success, msg = await registry.register_expert(profile)
        print(f"Registration: {success}, {msg}")

        # Update performance
        await registry.update_performance("expert_001", {
            'success': True,
            'latency_ms': 10,
            'carbon_kg': 0.001
        })

        # Get stats
        stats = registry.get_registry_stats()
        print("Stats:", stats)

        await registry.shutdown()

    asyncio.run(main())
