"""
Enhanced Expert Registry v6.1.0 - Complete Bio-Inspired Genome Repository

Full correlation with bio-inspired modules:
- Eco-ATP efficiency filtering for expert selection
- Species population tracking from chromatophore compartments
- Natural selection based on multi-dimensional fitness scores
- Gradient-based fitness updates from proton gradient fields
- Biomass storage integration for expert performance history
- Token economy integration for expert resource accounting
- Compartment lifecycle ↔ Registry lifecycle bidirectional mapping
- Evolutionary lineage tracking across generations
- Unified Sustainability Dashboard
- Predictive Evolution Forecasting
- Cross-Region Registry Synchronization
- Quantum efficiency as fitness dimension
- Predictive alerts for upcoming extinctions
- External climate model integration
- Conflict resolution with voting mechanisms
- Reproductive strategies for high-fitness experts

New in v6.1.0:
- Configuration dataclass for centralized settings
- Persistence manager for save/load registry state
- Resilient cross-region sync with retry and circuit breaker
- Adaptive fitness weights based on historical performance
- Dynamic natural selection thresholds
- Prometheus-style export for sustainability dashboard
- Improved bio-inspired fallback strategies
- Version compatibility checks for dependencies
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
import hashlib
import json
import networkx as nx
from collections import defaultdict, deque
import uuid
import math
import copy
import aiohttp
import os
import random
import pickle
import zlib

logger = logging.getLogger(__name__)

# ============================================================================
# Bio-Inspired Module Imports
# ============================================================================

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

# ============================================================================
# Configuration Dataclass (NEW)
# ============================================================================

@dataclass
class ExpertRegistryConfig:
    """Centralized configuration for the Expert Registry."""
    # Feature flags
    enable_bio_correlation: bool = True
    enable_natural_selection: bool = True
    enable_fitness_tracking: bool = True
    enable_population_tracking: bool = True
    enable_sustainability_dashboard: bool = True
    enable_predictive_forecasting: bool = True
    enable_cross_region_sync: bool = True
    enable_quantum_efficiency: bool = True
    enable_reproductive_strategies: bool = True
    enable_climate_integration: bool = True
    enable_persistence: bool = True

    # Tunable parameters
    registry_id: str = "default"
    persistence_path: str = "registry_state.pkl"
    sync_retries: int = 3
    sync_retry_base_delay_ms: float = 100.0
    sync_retry_max_delay_ms: float = 5000.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    fitness_weights: Dict[str, float] = field(default_factory=lambda: {
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
    natural_selection_percentile_low: float = 20.0
    natural_selection_percentile_high: float = 80.0
    reproductive_mutation_rate: float = 0.1
    reproductive_max_offspring: int = 3
    climate_update_interval: int = 3600
    sync_interval: int = 3600
    bio_sync_interval: int = 300

    def __post_init__(self):
        # Ensure boolean flags
        for key, value in self.__dict__.items():
            if isinstance(value, bool):
                setattr(self, key, bool(value))

# ============================================================================
# Existing Enums and Data Classes (preserved)
# ============================================================================

class ExpertDomain(Enum):
    ENERGY = "energy_optimization"
    DATA = "data_engineering"
    IOT = "iot_edge_computing"
    QUANTUM = "quantum_computing"
    HELIUM = "helium_aware_computing"
    CARBON = "carbon_optimization"
    SECURITY = "security_computing"
    GENERAL = "general_purpose"

class HardwareProfile(Enum):
    CPU_EFFICIENT = "cpu_low_power"
    CPU_PERFORMANCE = "cpu_high_performance"
    GPU_ACCELERATED = "gpu_cuda"
    QUANTUM_BACKEND = "quantum_processor"
    EDGE_DEVICE = "edge_iot_device"
    HYBRID = "hybrid_cpu_gpu"

class ExpertLifecycleState(Enum):
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

class CertificationLevel(Enum):
    NONE = "none"
    SELF_CERTIFIED = "self_certified"
    INTERNAL_AUDIT = "internal_audit"
    THIRD_PARTY = "third_party"
    ISO_COMPLIANT = "iso_compliant"

# ============================================================================
# Data Classes (Enhanced)
# ============================================================================

@dataclass
class ExpertVersion:
    major: int
    minor: int
    patch: int
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
            parts = version_str.replace('-beta', '').split('.')
            return cls(major=int(parts[0]), minor=int(parts[1]) if len(parts) > 1 else 0,
                       patch=int(parts[2]) if len(parts) > 2 else 0)
        except Exception:
            return cls(major=1, minor=0, patch=0)

    def is_compatible_with(self, other: 'ExpertVersion') -> bool:
        return self.major == other.major

    def is_newer_than(self, other: 'ExpertVersion') -> bool:
        if self.major != other.major: return self.major > other.major
        if self.minor != other.minor: return self.minor > other.minor
        return self.patch > other.patch

@dataclass
class ExpertDependency:
    dependency_id: str
    dependency_type: str
    version_requirement: str
    is_optional: bool = False
    is_runtime: bool = True
    description: str = ""

@dataclass
class ExpertCertification:
    certification_id: str
    level: CertificationLevel
    issued_by: str
    issued_at: datetime
    expires_at: Optional[datetime] = None
    validation_results: Dict[str, Any] = field(default_factory=dict)
    is_valid: bool = True

@dataclass
class HealthMetrics:
    success_rate: float = 1.0
    avg_latency_ms: float = 0.0
    error_rate: float = 0.0
    carbon_efficiency: float = 1.0
    helium_efficiency: float = 1.0
    availability: float = 1.0
    last_heartbeat: datetime = field(default_factory=datetime.utcnow)
    degradation_score: float = 0.0
    sustainability_score: float = 0.0
    quantum_efficiency: float = 0.0
    quantum_advantage_score: float = 0.0

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
        if heartbeat_age > 300: score *= 0.5
        return max(0.0, min(1.0, score))

    def calculate_sustainability_score(self) -> float:
        return (self.carbon_efficiency * 0.35 +
                self.helium_efficiency * 0.25 +
                (1 - self.error_rate) * 0.20 +
                self.quantum_efficiency * 0.10 +
                self.quantum_advantage_score * 0.10)

@dataclass
class ExpertLineage:
    lineage_id: str
    parent_expert_id: Optional[str] = None
    created_from: Optional[str] = None
    training_data_hash: Optional[str] = None
    training_duration_hours: float = 0.0
    training_carbon_kg: float = 0.0
    model_architecture: str = ""
    hyperparameters: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    fitness_history: List[float] = field(default_factory=list)
    reproductive_offspring: List[str] = field(default_factory=list)
    mutation_count: int = 0

@dataclass
class ExpertProfile:
    expert_id: str
    expert_name: str = ""
    version: ExpertVersion = field(default_factory=lambda: ExpertVersion(1, 0, 0))
    domain: ExpertDomain = ExpertDomain.GENERAL
    hardware_profile: HardwareProfile = HardwareProfile.CPU_EFFICIENT
    lifecycle_state: ExpertLifecycleState = ExpertLifecycleState.REGISTERED
    registered_at: datetime = field(default_factory=datetime.utcnow)
    activated_at: Optional[datetime] = None
    retired_at: Optional[datetime] = None
    replaces_expert: Optional[str] = None
    replaced_by: Optional[str] = None
    helium_per_inference: float = 0.0
    carbon_per_inference: float = 0.0
    energy_per_inference: float = 0.0
    avg_latency_ms: float = 0.0
    memory_usage_mb: float = 0.0
    accuracy_score: float = 0.0
    reliability_score: float = 0.0
    efficiency_score: float = 0.0
    security_score: float = 0.0
    min_carbon_zone: int = 0
    max_helium_scarcity: float = 1.0
    supported_task_types: List[str] = field(default_factory=list)
    incompatible_with: List[str] = field(default_factory=list)
    dependencies: List[ExpertDependency] = field(default_factory=list)
    certifications: List[ExpertCertification] = field(default_factory=list)
    health: HealthMetrics = field(default_factory=HealthMetrics)
    lineage: Optional[ExpertLineage] = None
    is_remote: bool = False
    remote_endpoint: Optional[str] = None
    origin_region: str = "local"
    dynamic_weights: Dict[str, float] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    capabilities: List[str] = field(default_factory=list)
    is_active: bool = True
    sustainability_score: float = 0.0
    quantum_capable: bool = False
    quantum_backend: Optional[str] = None
    quantum_qubits: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'expert_id': self.expert_id, 'expert_name': self.expert_name,
            'version': self.version.to_string(), 'domain': self.domain.value,
            'hardware_profile': self.hardware_profile.value,
            'lifecycle_state': self.lifecycle_state.value,
            'helium_per_inference': self.helium_per_inference,
            'carbon_per_inference': self.carbon_per_inference,
            'energy_per_inference': self.energy_per_inference,
            'avg_latency_ms': self.avg_latency_ms,
            'accuracy_score': self.accuracy_score,
            'reliability_score': self.reliability_score,
            'efficiency_score': self.efficiency_score,
            'health_score': self.health.calculate_health_score(),
            'sustainability_score': self.sustainability_score,
            'quantum_capable': self.quantum_capable,
            'quantum_qubits': self.quantum_qubits,
            'is_active': self.is_active and self.lifecycle_state.is_available(),
            'tags': self.tags, 'capabilities': self.capabilities,
            'supports_task_types': self.supported_task_types,
            'origin_region': self.origin_region, 'is_remote': self.is_remote
        }

    def compute_hash(self) -> str:
        profile_str = json.dumps(self.to_dict(), sort_keys=True)
        return hashlib.sha256(profile_str.encode()).hexdigest()

    def is_compatible_with(self, other: 'ExpertProfile') -> bool:
        if other.expert_id in self.incompatible_with: return False
        if self.expert_id in other.incompatible_with: return False
        if self.expert_name == other.expert_name:
            return self.version.is_compatible_with(other.version)
        return True

    def get_certification_level(self) -> CertificationLevel:
        if not self.certifications: return CertificationLevel.NONE
        levels = [c.level for c in self.certifications if c.is_valid]
        if not levels: return CertificationLevel.NONE
        level_order = list(CertificationLevel)
        return max(levels, key=lambda l: level_order.index(l))

@dataclass
class FitnessScore:
    expert_id: str
    overall_fitness: float = 0.5
    resource_efficiency: float = 0.5
    adaptation_speed: float = 0.5
    cooperation_score: float = 0.5
    resilience_score: float = 0.5
    selection_coefficient: float = 0.0
    reproductive_success: int = 0
    ecoatp_efficiency: float = 0.5
    sustainability_score: float = 0.5
    quantum_efficiency: float = 0.5
    quantum_advantage: float = 0.0
    helium_savings: float = 0.5

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

# ============================================================================
# Persistence Manager (NEW)
# ============================================================================

class RegistryPersistenceManager:
    """
    Manages saving and loading of the registry state to/from disk.
    """

    def __init__(self, config: ExpertRegistryConfig):
        self.config = config
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        logger.info(f"RegistryPersistenceManager initialized (path={self.path})")

    async def save_state(self, registry: 'ExpertRegistry'):
        """Save the entire registry state to disk."""
        async with self._lock:
            try:
                state = {
                    'experts': {eid: expert for eid, expert in registry._experts.items()},
                    'fitness_scores': registry.fitness_scores,
                    'domain_index': registry._domain_index,
                    'hardware_index': registry._hardware_index,
                    'lifecycle_index': registry._lifecycle_index,
                    'tag_index': registry._tag_index,
                    'capability_index': registry._capability_index,
                    'task_type_index': registry._task_type_index,
                    'region_index': registry._region_index,
                    'version_family_index': registry._version_family_index,
                    'dependency_graph': registry._dependency_graph,
                    'remote_registries': registry._remote_registries,
                    'federated_experts': registry._federated_experts,
                    'ab_tests': registry._ab_tests,
                    'migration_paths': registry._migration_paths,
                    'evolutionary_events': list(registry.evolutionary_events),
                    'speciation_count': registry.speciation_count,
                    'extinction_count': registry.extinction_count,
                    'total_generations': registry.total_generations,
                    'reproductive_events': registry.reproductive_events,
                    'stats': registry._stats,
                    'performance_history': dict(registry._performance_history),
                    'config': registry.config,
                    'registry_id': registry.registry_id,
                    'bio_integration_active': registry.enable_bio_correlation,
                    # Optional: save bio module references? We'll only save IDs; modules are injected at load.
                }
                # Serialize and compress
                serialized = pickle.dumps(state)
                compressed = zlib.compress(serialized)
                with open(self.path, 'wb') as f:
                    f.write(compressed)
                logger.info(f"Registry state saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save registry state: {e}")
                return False

    async def load_state(self, registry: 'ExpertRegistry') -> bool:
        """Load registry state from disk and populate the registry."""
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return False
            try:
                with open(self.path, 'rb') as f:
                    compressed = f.read()
                serialized = zlib.decompress(compressed)
                state = pickle.loads(serialized)

                # Restore core data
                registry._experts = state['experts']
                registry.fitness_scores = state['fitness_scores']
                registry._domain_index = state['domain_index']
                registry._hardware_index = state['hardware_index']
                registry._lifecycle_index = state['lifecycle_index']
                registry._tag_index = state['tag_index']
                registry._capability_index = state['capability_index']
                registry._task_type_index = state['task_type_index']
                registry._region_index = state['region_index']
                registry._version_family_index = state['version_family_index']
                registry._dependency_graph = state['dependency_graph']
                registry._remote_registries = state['remote_registries']
                registry._federated_experts = state['federated_experts']
                registry._ab_tests = state['ab_tests']
                registry._migration_paths = state['migration_paths']
                registry.evolutionary_events = deque(state['evolutionary_events'], maxlen=10000)
                registry.speciation_count = state['speciation_count']
                registry.extinction_count = state['extinction_count']
                registry.total_generations = state['total_generations']
                registry.reproductive_events = state['reproductive_events']
                registry._stats = state['stats']
                registry._performance_history = defaultdict(list)
                for k, v in state['performance_history'].items():
                    registry._performance_history[k] = v
                # Config and registry_id are already set; no need to override.
                logger.info(f"Registry state loaded from {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to load registry state: {e}")
                return False

    async def delete_state(self):
        """Delete the persistence file."""
        async with self._lock:
            if os.path.exists(self.path):
                os.remove(self.path)
                logger.info(f"Persistence file {self.path} deleted")
                return True
            return False

# ============================================================================
# Sustainability Dashboard (Enhanced)
# ============================================================================

class RegistrySustainabilityDashboard:
    """
    Unified Sustainability Dashboard with Prometheus-style export.
    """

    def __init__(self, registry: 'ExpertRegistry'):
        self.registry = registry
        self.history = deque(maxlen=1000)
        self._alert_history = deque(maxlen=100)
        logger.info("RegistrySustainabilityDashboard initialized")

    def get_dashboard_status(self) -> Dict[str, Any]:
        registry = self.registry
        active_experts = registry.get_all_active_experts()
        total_experts = len(registry._experts)

        avg_carbon = np.mean([e.health.carbon_efficiency for e in active_experts]) if active_experts else 0.5
        avg_helium = np.mean([e.health.helium_efficiency for e in active_experts]) if active_experts else 0.5
        avg_quantum = np.mean([e.health.quantum_efficiency for e in active_experts]) if active_experts else 0.0
        avg_sustainability = np.mean([e.sustainability_score for e in active_experts]) if active_experts else 0.5

        fitnesses = [f.overall_fitness for f in registry.fitness_scores.values()] if registry.fitness_scores else [0.5]

        return {
            'timestamp': datetime.utcnow().isoformat(),
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

    def _generate_predictive_alerts(self) -> List[Dict[str, Any]]:
        registry = self.registry
        alerts = []

        # Check fitness
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

            # Quantum-specific
            if fitness.quantum_efficiency < 0.2 and registry._experts[expert_id].quantum_capable:
                alerts.append({
                    'level': 'warning',
                    'expert_id': expert_id,
                    'message': f"Quantum expert {expert_id} has low quantum efficiency",
                    'timeframe_hours': 48,
                    'recommendation': 'Optimize quantum circuit parameters'
                })

        # Species diversity
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
        """Export Prometheus‑style metrics."""
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
        # Species populations
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

# ============================================================================
# Predictive Evolution Forecaster (Enhanced)
# ============================================================================

class PredictiveEvolutionForecaster:
    """
    Predictive Evolution Forecasting with climate integration.
    """

    def __init__(self, registry: 'ExpertRegistry'):
        self.registry = registry
        self.forecast_history = deque(maxlen=1000)
        self._climate_models = {
            'carbon': {'current': 400, 'trend': 0.02, 'volatility': 0.05},
            'helium': {'current': 0.5, 'trend': 0.03, 'volatility': 0.08}
        }
        logger.info("PredictiveEvolutionForecaster initialized")

    def update_climate_model(self, model_type: str, data: Dict[str, float]):
        if model_type in self._climate_models:
            self._climate_models[model_type].update(data)
            logger.info(f"Updated climate model for {model_type}")

    async def forecast_evolutionary_trend(self, hours: int = 24) -> Dict[str, Any]:
        registry = self.registry
        carbon_proj = self._project_climate('carbon', hours)
        helium_proj = self._project_climate('helium', hours)

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

# ============================================================================
# Cross-Region Registry Synchronizer (Enhanced with Retry & Circuit Breaker)
# ============================================================================

class CrossRegionRegistrySynchronizer:
    """
    Cross-Region Registry Synchronization with retry and circuit breaker.
    """

    def __init__(self, registry: 'ExpertRegistry'):
        self.registry = registry
        self._session = None
        self.sync_history = deque(maxlen=1000)
        self.voting_weights: Dict[str, float] = {}
        self.failure_count = 0
        self.circuit_open = False
        self.circuit_open_until: Optional[datetime] = None
        logger.info("CrossRegionRegistrySynchronizer initialized with resilience")

    async def _get_session(self):
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

        # Circuit breaker check
        if self.circuit_open:
            if datetime.utcnow() < self.circuit_open_until:
                logger.warning("Circuit breaker open, skipping sync")
                result['status'] = 'circuit_open'
                return result
            else:
                self.circuit_open = False
                self.failure_count = 0
                logger.info("Circuit breaker reset for CrossRegionRegistrySynchronizer")

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
                            self.failure_count = 0  # reset on success
                        else:
                            logger.warning(f"Sync pull failed: {response.status} (attempt {attempt+1})")
                            if attempt == self.registry.config.sync_retries - 1:
                                self._record_failure()
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
                                self._record_failure()
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
                    self._record_failure()
                    result['status'] = f'error: {str(e)}'
                    return result
                await asyncio.sleep(2 ** attempt * 0.1)

        return result

    def _record_failure(self):
        self.failure_count += 1
        if self.failure_count >= self.registry.config.circuit_breaker_threshold:
            self.circuit_open = True
            self.circuit_open_until = datetime.utcnow() + timedelta(seconds=self.registry.config.circuit_breaker_recovery_timeout)
            logger.error("Circuit breaker opened for CrossRegionRegistrySynchronizer")

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
            remote_version = ExpertVersion.from_string(remote_data.get('version', '1.0.0'))

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
                            expert_id, local_expert, remote_data, registry_id
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
                try:
                    profile = self._create_profile_from_remote(remote_data, registry_id)
                    success, _ = self.registry.register_expert(profile, validate=False, auto_certify=False)
                    if success:
                        synced += 1
                except Exception as e:
                    logger.error(f"Failed to create expert from remote: {e}")

        return synced, conflicts, resolved

    async def _resolve_conflict_with_voting(
        self,
        expert_id: str,
        local_expert: ExpertProfile,
        remote_data: Dict,
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
            'version': remote_data.get('version', '1.0.0'),
            'trust': remote_trust,
            'decision': 'remote'
        })

        # Ask other registries
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

    def _create_profile_from_remote(self, remote_data: Dict, registry_id: str) -> ExpertProfile:
        domain_map = {
            'energy_optimization': ExpertDomain.ENERGY,
            'data_engineering': ExpertDomain.DATA,
            'iot_edge_computing': ExpertDomain.IOT,
            'quantum_computing': ExpertDomain.QUANTUM,
            'helium_aware_computing': ExpertDomain.HELIUM,
            'general_purpose': ExpertDomain.GENERAL
        }
        domain_str = remote_data.get('domain', 'general_purpose')
        domain = domain_map.get(domain_str, ExpertDomain.GENERAL)

        health = HealthMetrics(
            success_rate=remote_data.get('health_score', 0.9),
            carbon_efficiency=remote_data.get('carbon_efficiency', 0.5),
            helium_efficiency=remote_data.get('helium_efficiency', 0.5),
            quantum_efficiency=remote_data.get('quantum_efficiency', 0.0)
        )
        return ExpertProfile(
            expert_id=remote_data.get('expert_id', f"remote_{registry_id}_{uuid.uuid4().hex[:8]}"),
            expert_name=remote_data.get('expert_name', 'Unknown'),
            version=ExpertVersion.from_string(remote_data.get('version', '1.0.0')),
            domain=domain,
            hardware_profile=HardwareProfile(remote_data.get('hardware_profile', 'cpu_low_power')),
            helium_per_inference=remote_data.get('helium_per_inference', 0.0),
            carbon_per_inference=remote_data.get('carbon_per_inference', 0.0),
            energy_per_inference=remote_data.get('energy_per_inference', 0.0),
            accuracy_score=remote_data.get('accuracy_score', 0.5),
            reliability_score=remote_data.get('reliability_score', 0.5),
            efficiency_score=remote_data.get('efficiency_score', 0.5),
            is_remote=True,
            remote_endpoint=remote_data.get('remote_endpoint'),
            origin_region=remote_data.get('origin_region', registry_id),
            quantum_capable=remote_data.get('quantum_capable', False),
            quantum_qubits=remote_data.get('quantum_qubits', 0),
            health=health
        )

    def _serialize_local_experts(self) -> List[Dict]:
        return [
            expert.to_dict()
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
            'circuit_open': self.circuit_open
        }

# ============================================================================
# Enhanced Expert Registry (Main Class)
# ============================================================================

class ExpertRegistry:
    """
    Enhanced Expert Registry v6.1.0 - Complete Bio-Inspired Genome Repository
    """

    def __init__(self, config: Optional[ExpertRegistryConfig] = None, **kwargs):
        if config is None:
            # Build config from kwargs for backward compatibility
            config = ExpertRegistryConfig(
                enable_bio_correlation=kwargs.get('enable_bio_correlation', True),
                enable_natural_selection=kwargs.get('enable_natural_selection', True),
                enable_fitness_tracking=kwargs.get('enable_fitness_tracking', True),
                enable_population_tracking=kwargs.get('enable_population_tracking', True),
                enable_sustainability_dashboard=kwargs.get('enable_sustainability_dashboard', True),
                enable_predictive_forecasting=kwargs.get('enable_predictive_forecasting', True),
                enable_cross_region_sync=kwargs.get('enable_cross_region_sync', True),
                enable_quantum_efficiency=kwargs.get('enable_quantum_efficiency', True),
                enable_reproductive_strategies=kwargs.get('enable_reproductive_strategies', True),
                enable_climate_integration=kwargs.get('enable_climate_integration', True),
                enable_persistence=kwargs.get('enable_persistence', True),
                registry_id=kwargs.get('registry_id', 'default'),
                persistence_path=kwargs.get('persistence_path', 'registry_state.pkl'),
                sync_retries=kwargs.get('sync_retries', 3)
            )
        self.config = config
        self.registry_id = config.registry_id

        # Feature flags
        self.enable_bio_correlation = config.enable_bio_correlation and BIO_INSPIRED_AVAILABLE
        self.enable_natural_selection = config.enable_natural_selection and BIO_INSPIRED_AVAILABLE
        self.enable_fitness_tracking = config.enable_fitness_tracking
        self.enable_population_tracking = config.enable_population_tracking and BIO_INSPIRED_AVAILABLE
        self.enable_sustainability_dashboard = config.enable_sustainability_dashboard
        self.enable_predictive_forecasting = config.enable_predictive_forecasting
        self.enable_cross_region_sync = config.enable_cross_region_sync
        self.enable_quantum_efficiency = config.enable_quantum_efficiency
        self.enable_reproductive_strategies = config.enable_reproductive_strategies
        self.enable_climate_integration = config.enable_climate_integration
        self.enable_persistence = config.enable_persistence

        # Bio-inspired module references
        self.token_manager: Optional[EcoATPTokenManager] = None
        self.gradient_manager: Optional[GradientFieldManager] = None
        self.compartment_manager: Optional[CompartmentManager] = None
        self.biomass_storage: Optional[BiomassStorage] = None

        # New modules
        self.sustainability_dashboard = None
        self.predictive_forecaster = None
        self.cross_region_sync = None
        self.persistence_manager = None

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

        # Adaptive fitness weights (dynamic)
        self._fitness_weights = config.fitness_weights.copy()
        self._fitness_weight_history = deque(maxlen=100)

        # Initialize modules
        self._initialize_modules()

        # Load state if persistence enabled
        if self.enable_persistence:
            asyncio.create_task(self._load_state())

        # Start background tasks
        self._start_background_tasks()

        logger.info(
            f"Expert Registry v6.1.0 initialized: "
            f"bio_correlation={self.enable_bio_correlation}, "
            f"persistence={self.enable_persistence}, "
            f"quantum={self.enable_quantum_efficiency}, "
            f"reproductive={self.enable_reproductive_strategies}"
        )

    def _initialize_modules(self):
        if self.enable_sustainability_dashboard:
            self.sustainability_dashboard = RegistrySustainabilityDashboard(self)
        if self.enable_predictive_forecasting:
            self.predictive_forecaster = PredictiveEvolutionForecaster(self)
        if self.enable_cross_region_sync:
            self.cross_region_sync = CrossRegionRegistrySynchronizer(self)
        if self.enable_persistence:
            self.persistence_manager = RegistryPersistenceManager(self.config)

    def _start_background_tasks(self):
        asyncio.create_task(self._bio_correlation_loop())
        if self.enable_predictive_forecasting:
            asyncio.create_task(self._predictive_forecast_loop())
        if self.enable_cross_region_sync:
            asyncio.create_task(self._cross_region_sync_loop())
        if self.enable_reproductive_strategies:
            asyncio.create_task(self._reproductive_strategy_loop())
        if self.enable_persistence:
            asyncio.create_task(self._persistence_save_loop())

    # ============================================================================
    # Persistence Methods
    # ============================================================================

    async def _load_state(self):
        if self.persistence_manager:
            await self.persistence_manager.load_state(self)

    async def save_state(self):
        if self.persistence_manager:
            await self.persistence_manager.save_state(self)

    async def _persistence_save_loop(self):
        while True:
            try:
                if self.enable_persistence and self.persistence_manager:
                    # Save every 5 minutes
                    await asyncio.sleep(300)
                    await self.save_state()
            except Exception as e:
                logger.error(f"Persistence save loop error: {e}")
                await asyncio.sleep(60)

    # ============================================================================
    # Bio-Inspired Module Injection
    # ============================================================================

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
        injections = {k: v is not None for k, v in [
            ('token_manager', self.token_manager),
            ('gradient_manager', self.gradient_manager),
            ('compartment_manager', self.compartment_manager),
            ('biomass_storage', self.biomass_storage)
        ]}
        logger.info(f"Bio-inspired injections into Expert Registry: {injections}")
        if any(injections.values()):
            self.enable_bio_correlation = True

    # ============================================================================
    # Bio-Inspired Data Access Methods
    # ============================================================================

    def _get_expert_ecoatp_efficiency(self, expert_id: str) -> float:
        if self.token_manager:
            account = self.token_manager.get_account_summary(f"expert_{expert_id}")
            if account:
                return account.get('efficiency_rating', 0.5)
        return 0.5

    def _get_expert_token_balance(self, expert_id: str) -> float:
        if self.token_manager:
            account = self.token_manager.get_account_summary(f"expert_{expert_id}")
            if account:
                return account.get('balance', 0)
        return 0.0

    def _get_gradient_strength(self, field_id: str) -> float:
        if self.gradient_manager:
            field = self.gradient_manager.fields.get(field_id)
            if field:
                return field.gradient_strength
        return 0.5

    def _get_species_population(self, species_id: str) -> int:
        if self.compartment_manager:
            return sum(1 for c in self.compartment_manager.compartments.values()
                      if c.expert_type == species_id and c.is_viable)
        return len([e for e in self._experts.values()
                   if hasattr(e, 'domain') and species_id in str(e.domain).lower()])

    def _get_species_populations(self) -> Dict[str, int]:
        species = ['energy', 'data', 'iot', 'quantum', 'helium', 'general']
        return {s: self._get_species_population(s) for s in species}

    def _get_total_compartment_population(self) -> int:
        if self.compartment_manager:
            return len([c for c in self.compartment_manager.compartments.values() if c.is_viable])
        return len([e for e in self._experts.values() if e.lifecycle_state.is_available()])

    def _get_species_id(self, profile: ExpertProfile) -> str:
        domain = profile.domain.value if hasattr(profile.domain, 'value') else str(profile.domain)
        if 'energy' in domain.lower(): return 'energy'
        if 'data' in domain.lower(): return 'data'
        if 'iot' in domain.lower(): return 'iot'
        if 'quantum' in domain.lower(): return 'quantum'
        if 'helium' in domain.lower(): return 'helium'
        return 'general'

    # ============================================================================
    # Expert Registration (Enhanced)
    # ============================================================================

    def register_expert(
        self,
        profile: ExpertProfile,
        validate: bool = True,
        auto_certify: bool = False,
        create_ecoatp_account: bool = True,
        register_compartment: bool = True
    ) -> Tuple[bool, str]:
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
            species = self._get_species_id(profile)
            self.compartment_manager.create_compartment(
                expert_type=species,
                expert_instance=None
            )
            logger.info(f"Created chromatophore compartment for {profile.expert_id}")

        # Fitness score
        if self.enable_fitness_tracking:
            self.fitness_scores[profile.expert_id] = FitnessScore(
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
            self.fitness_scores[profile.expert_id].calculate_overall(self._fitness_weights)

        self._update_dependency_graph(profile)
        self._version_family_index[profile.expert_name].append(profile.expert_id)
        self._stats['total_registrations'] += 1
        self.total_generations += 1

        self.evolutionary_events.append({
            'type': 'speciation' if not profile.replaces_expert else 'evolution',
            'expert_id': profile.expert_id,
            'species': self._get_species_id(profile),
            'generation': self.total_generations,
            'quantum_capable': profile.quantum_capable,
            'timestamp': datetime.utcnow().isoformat()
        })
        self.speciation_count += 1

        logger.info(f"Registered expert: {profile.expert_id} v{profile.version.to_string()} "
                   f"(species: {self._get_species_id(profile)}, "
                   f"quantum: {profile.quantum_capable}, "
                   f"generation: {self.total_generations})")

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
        # Dependency version checks
        for dep in profile.dependencies:
            if not dep.is_optional and dep.dependency_id not in self._experts:
                errors.append(f"Required dependency {dep.dependency_id} not registered")
            # Version compatibility check
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
        """Check if version satisfies requirement (e.g., '>=2.0.0', '==1.3.0')."""
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
            # assume exact match
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

    # ============================================================================
    # Filtering Methods (Enhanced)
    # ============================================================================

    def filter_by_ecoatp_efficiency(self, min_efficiency: float = 0.5, min_token_balance: float = 10.0) -> List[ExpertProfile]:
        if not self.enable_bio_correlation or not self.token_manager:
            return self.get_all_active_experts()
        efficient = []
        for expert_id, expert in self._experts.items():
            if not expert.lifecycle_state.is_available():
                continue
            efficiency = self._get_expert_ecoatp_efficiency(expert_id)
            balance = self._get_expert_token_balance(expert_id)
            if efficiency >= min_efficiency and balance >= min_token_balance:
                efficient.append(expert)
        return efficient

    def filter_by_health_and_fitness(self, min_health: float = 0.5, min_fitness: float = 0.4) -> List[ExpertProfile]:
        qualified = []
        for expert_id, expert in self._experts.items():
            if not expert.lifecycle_state.is_available():
                continue
            health = expert.health.calculate_health_score()
            fitness = self.fitness_scores.get(expert_id, FitnessScore(expert_id)).overall_fitness
            if health >= min_health and fitness >= min_fitness:
                qualified.append(expert)
        return qualified

    def filter_by_sustainability_score(self, min_sustainability: float = 0.5) -> List[ExpertProfile]:
        return [e for e in self._experts.values() if e.lifecycle_state.is_available() and e.sustainability_score >= min_sustainability]

    def filter_by_quantum_efficiency(self, min_quantum_efficiency: float = 0.3) -> List[ExpertProfile]:
        return [e for e in self._experts.values() if e.lifecycle_state.is_available() and e.health.quantum_efficiency >= min_quantum_efficiency]

    def filter_by_gradient_alignment(self, carbon_threshold: float = 0.3, trust_threshold: float = 0.4) -> List[ExpertProfile]:
        if not self.enable_bio_correlation or not self.gradient_manager:
            return self.get_all_active_experts()
        carbon_strength = self._get_gradient_strength('carbon')
        trust_strength = self._get_gradient_strength('trust')
        if carbon_strength > carbon_threshold:
            return sorted([e for e in self.get_all_active_experts()], key=lambda e: e.carbon_per_inference)[:max(1, len(self._experts) // 2)]
        if trust_strength < trust_threshold:
            return sorted([e for e in self.get_all_active_experts()], key=lambda e: e.reliability_score, reverse=True)[:max(1, len(self._experts) // 2)]
        return self.get_all_active_experts()

    # ============================================================================
    # Natural Selection (Enhanced)
    # ============================================================================

    def update_fitness_from_gradients(self):
        if not self.enable_bio_correlation or not self.gradient_manager:
            return
        trust_strength = self._get_gradient_strength('trust')
        carbon_strength = self._get_gradient_strength('carbon')
        for expert_id, fitness in self.fitness_scores.items():
            if expert_id not in self._experts:
                continue
            expert = self._experts[expert_id]
            fitness.resilience_score = fitness.resilience_score * 0.7 + trust_strength * 0.3
            carbon_eff = 1.0 / (1.0 + expert.carbon_per_inference * 10000)
            fitness.resource_efficiency = fitness.resource_efficiency * 0.8 + carbon_eff * 0.2
            fitness.ecoatp_efficiency = self._get_expert_ecoatp_efficiency(expert_id)
            if self.compartment_manager:
                compartment = self.compartment_manager.find_best_compartment(self._get_species_id(expert))
                if compartment:
                    fitness.cooperation_score = fitness.cooperation_score * 0.8 + compartment.health_score * 0.2
            fitness.quantum_efficiency = expert.health.quantum_efficiency
            fitness.quantum_advantage = self._calculate_quantum_advantage(expert)
            fitness.helium_savings = 1.0 - expert.helium_per_inference / max(expert.helium_per_inference, 1)
            fitness.sustainability_score = expert.health.calculate_sustainability_score()
            fitness.calculate_overall(self._fitness_weights)

    def trigger_natural_selection(self):
        if not self.enable_natural_selection:
            return
        self.update_fitness_from_gradients()
        fitnesses = [f.overall_fitness for f in self.fitness_scores.values()]
        if not fitnesses:
            return

        # Dynamic thresholds using moving percentiles
        low_pct = self.config.natural_selection_percentile_low
        high_pct = self.config.natural_selection_percentile_high
        threshold = np.percentile(fitnesses, low_pct)
        top_threshold = np.percentile(fitnesses, high_pct)

        deprecated_count = 0
        reproducer_count = 0

        for expert_id, fitness in list(self.fitness_scores.items()):
            if expert_id not in self._experts:
                continue
            expert = self._experts[expert_id]
            if (fitness.overall_fitness < threshold and
                fitness.reproductive_success == 0 and
                expert.lifecycle_state in [ExpertLifecycleState.ACTIVE, ExpertLifecycleState.CERTIFIED]):
                self.deprecate_expert(expert_id, reason="natural_selection_low_fitness")
                deprecated_count += 1
                if self.biomass_storage:
                    self.biomass_storage.store_task(
                        task_data={'expert_id': expert_id, 'knowledge': expert.to_dict()},
                        ecoatp_cost=1.0,
                        guarantee=GuaranteeLevel.BEST_EFFORT,
                        initial_tier=StorageTier.LIPID_DEPOT
                    )
                self.evolutionary_events.append({
                    'type': 'extinction',
                    'expert_id': expert_id,
                    'fitness': fitness.overall_fitness,
                    'quantum_efficiency': fitness.quantum_efficiency,
                    'reason': 'natural_selection',
                    'timestamp': datetime.utcnow().isoformat()
                })
                self.extinction_count += 1
            elif fitness.overall_fitness > top_threshold and fitness.reproductive_success < self.config.reproductive_max_offspring:
                fitness.reproductive_success += 1
                reproducer_count += 1

        self._stats['total_natural_selections'] += 1
        self._stats['last_selection'] = datetime.utcnow()
        if deprecated_count > 0 or reproducer_count > 0:
            logger.info(f"Natural selection: {deprecated_count} deprecated, {reproducer_count} marked for reproduction")

    # ============================================================================
    # Reproductive Strategies (Enhanced)
    # ============================================================================

    async def _reproductive_strategy_loop(self):
        while True:
            try:
                if self.enable_reproductive_strategies:
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
        success, msg = self.register_expert(offspring, validate=False, auto_certify=True)
        if success:
            if parent.lineage is None:
                parent.lineage = ExpertLineage(lineage_id=f"lineage_{parent.expert_id}", parent_expert_id=None)
            parent.lineage.reproductive_offspring.append(offspring_id)
            parent.lineage.mutation_count += 1
            fitness.reproductive_success += 1
            self.reproductive_events += 1
            logger.info(f"Reproduced expert {offspring_id} from {expert_id}")

    # ============================================================================
    # Deprecation and Activation
    # ============================================================================

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
        return True, f"Expert {expert_id} activated"

    # ============================================================================
    # Performance Tracking (Enhanced)
    # ============================================================================

    def update_performance(self, expert_id: str, metrics: Dict[str, Any]):
        if expert_id not in self._experts:
            return
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
            fitness.calculate_overall(self._fitness_weights)

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

    # ============================================================================
    # Background Tasks
    # ============================================================================

    async def _bio_correlation_loop(self):
        while True:
            try:
                if self.enable_bio_correlation:
                    if self.gradient_manager:
                        self.update_fitness_from_gradients()
                    if self.enable_natural_selection:
                        self.trigger_natural_selection()
                    if self.compartment_manager and self.enable_population_tracking:
                        for species_id in ['energy', 'data', 'iot', 'quantum', 'helium']:
                            self._get_species_population(species_id)
                await asyncio.sleep(self.config.bio_sync_interval)
            except Exception as e:
                logger.error(f"Bio-correlation loop error: {e}")
                await asyncio.sleep(60)

    async def _predictive_forecast_loop(self):
        while True:
            try:
                if self.enable_predictive_forecasting and self.predictive_forecaster:
                    await self.predictive_forecaster.forecast_evolutionary_trend()
                await asyncio.sleep(1800)
            except Exception as e:
                logger.error(f"Predictive forecast loop error: {e}")
                await asyncio.sleep(300)

    async def _cross_region_sync_loop(self):
        while True:
            try:
                if self.enable_cross_region_sync and self.cross_region_sync:
                    for registry_id, registry_url in self._remote_registries.items():
                        await self.cross_region_sync.sync_with_remote_registry(
                            registry_url, registry_id, 'pull'
                        )
                await asyncio.sleep(self.config.sync_interval)
            except Exception as e:
                logger.error(f"Cross-region sync loop error: {e}")
                await asyncio.sleep(600)

    # ============================================================================
    # Statistics and Reporting (Enhanced)
    # ============================================================================

    def get_registry_stats(self) -> Dict[str, Any]:
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
            'adaptive_fitness_weights': self._fitness_weights,
            'persistence_enabled': self.enable_persistence,
            'circuit_breaker_open': self.cross_region_sync.circuit_open if self.cross_region_sync else False
        }
        if self.enable_population_tracking:
            stats['species_populations'] = self._get_species_populations()
        if self.enable_sustainability_dashboard and self.sustainability_dashboard:
            stats['dashboard'] = self.sustainability_dashboard.get_dashboard_status()
            stats['predictive_alerts'] = self.sustainability_dashboard.get_predictive_alerts()
        if self.enable_predictive_forecasting and self.predictive_forecaster:
            stats['forecast'] = self.predictive_forecaster.forecast_history[-1] if self.predictive_forecaster.forecast_history else None
        if self.enable_cross_region_sync and self.cross_region_sync:
            stats['sync'] = self.cross_region_sync.get_sync_status()
        return stats

    def get_all_active_experts(self) -> List[ExpertProfile]:
        return [e for e in self._experts.values() if e.lifecycle_state.is_available() and e.is_active]

    def get_export_metrics(self) -> Dict[str, float]:
        """Export Prometheus‑style metrics for monitoring."""
        if self.enable_sustainability_dashboard and self.sustainability_dashboard:
            return self.sustainability_dashboard.export_metrics()
        return {}

    async def shutdown(self):
        logger.info("Shutting down Expert Registry")
        if self.enable_persistence:
            await self.save_state()
        if self.cross_region_sync and self.cross_region_sync._session:
            await self.cross_region_sync._session.close()
        logger.info("Shutdown complete")
