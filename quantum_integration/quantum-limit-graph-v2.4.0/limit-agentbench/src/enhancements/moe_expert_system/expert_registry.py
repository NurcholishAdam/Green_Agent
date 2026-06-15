# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/expert_registry.py
# Enhanced with complete bio-inspired correlation - Genome Repository v4.0.0

"""
Enhanced Expert Registry v4.0.0 - Complete Bio-Inspired Genome Repository

Full correlation with bio-inspired modules:
- Eco-ATP efficiency filtering
- Species population tracking from compartments
- Natural selection based on fitness scores
- Gradient-based fitness updates
- Biomass storage integration for expert history
- Token economy integration for expert accounting
- Compartment lifecycle ↔ Registry lifecycle mapping
- Evolutionary lineage tracking
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set
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

logger = logging.getLogger(__name__)

# ============================================================================
# Try importing bio-inspired modules
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
# Existing Enums and Data Classes (preserved from original)
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
    
    def to_compartment_state(self) -> 'CompartmentState':
        """Map registry lifecycle to compartment state"""
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
    
    def calculate_health_score(self) -> float:
        weights = {'success_rate': 0.30, 'availability': 0.25, 'error_rate': 0.20,
                   'carbon_efficiency': 0.10, 'helium_efficiency': 0.10, 'degradation_score': 0.05}
        score = (weights['success_rate'] * self.success_rate +
                 weights['availability'] * self.availability +
                 weights['error_rate'] * (1 - self.error_rate) +
                 weights['carbon_efficiency'] * self.carbon_efficiency +
                 weights['helium_efficiency'] * self.helium_efficiency +
                 weights['degradation_score'] * (1 - self.degradation_score))
        heartbeat_age = (datetime.utcnow() - self.last_heartbeat).total_seconds()
        if heartbeat_age > 300: score *= 0.5
        return max(0.0, min(1.0, score))

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

# ============================================================================
# Fitness Score for Natural Selection
# ============================================================================

@dataclass
class FitnessScore:
    """Multi-dimensional fitness scoring for natural selection"""
    expert_id: str
    overall_fitness: float = 0.5
    resource_efficiency: float = 0.5
    adaptation_speed: float = 0.5
    cooperation_score: float = 0.5
    resilience_score: float = 0.5
    selection_coefficient: float = 0.0
    reproductive_success: int = 0
    ecoatp_efficiency: float = 0.5
    
    def calculate_overall(self):
        self.overall_fitness = (
            self.resource_efficiency * 0.30 +
            self.resilience_score * 0.25 +
            self.adaptation_speed * 0.20 +
            self.cooperation_score * 0.15 +
            self.ecoatp_efficiency * 0.10
        )

# ============================================================================
# Enhanced Expert Registry with Complete Bio-Inspired Correlation
# ============================================================================

class ExpertRegistry:
    """
    Enhanced Expert Registry v4.0.0 - Complete Bio-Inspired Genome Repository
    
    Full correlation with bio-inspired modules:
    - Eco-ATP efficiency filtering for expert selection
    - Species population tracking from chromatophore compartments
    - Natural selection based on multi-dimensional fitness scores
    - Gradient-based fitness updates from proton gradient fields
    - Biomass storage integration for expert performance history
    - Token economy integration for expert resource accounting
    - Compartment lifecycle ↔ Registry lifecycle bidirectional mapping
    - Evolutionary lineage tracking across generations
    """
    
    def __init__(
        self,
        registry_id: str = "default",
        enable_bio_correlation: bool = True,
        enable_natural_selection: bool = True,
        enable_fitness_tracking: bool = True,
        enable_population_tracking: bool = True
    ):
        self.registry_id = registry_id
        
        # Feature flags
        self.enable_bio_correlation = enable_bio_correlation and BIO_INSPIRED_AVAILABLE
        self.enable_natural_selection = enable_natural_selection and BIO_INSPIRED_AVAILABLE
        self.enable_fitness_tracking = enable_fitness_tracking
        self.enable_population_tracking = enable_population_tracking and BIO_INSPIRED_AVAILABLE
        
        # Bio-inspired module references (injected)
        self.token_manager: Optional[EcoATPTokenManager] = None
        self.gradient_manager: Optional[GradientFieldManager] = None
        self.compartment_manager: Optional[CompartmentManager] = None
        self.biomass_storage: Optional[BiomassStorage] = None
        
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
        
        # Statistics
        self._stats = {
            'total_registrations': 0,
            'total_deregistrations': 0,
            'total_natural_selections': 0,
            'last_selection': None
        }
        
        # Start background tasks
        if self.enable_bio_correlation:
            asyncio.create_task(self._bio_correlation_loop())
        
        logger.info(
            f"Expert Registry v4.0.0 initialized: "
            f"bio_correlation={self.enable_bio_correlation}, "
            f"natural_selection={self.enable_natural_selection}, "
            f"bio_available={BIO_INSPIRED_AVAILABLE}"
        )
    
    # ========================================================================
    # Bio-Inspired Module Injection
    # ========================================================================
    
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        """
        Inject bio-inspired modules for complete correlation.
        
        This connects the registry to actual bio-inspired systems.
        """
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
        
        injections = {
            'token_manager': self.token_manager is not None,
            'gradient_manager': self.gradient_manager is not None,
            'compartment_manager': self.compartment_manager is not None,
            'biomass_storage': self.biomass_storage is not None
        }
        logger.info(f"Bio-inspired injections into Expert Registry: {injections}")
        
        # Enable correlation if modules available
        if any(injections.values()):
            self.enable_bio_correlation = True
    
    # ========================================================================
    # Bio-Inspired Data Access Methods
    # ========================================================================
    
    def _get_expert_ecoatp_efficiency(self, expert_id: str) -> float:
        """Get Eco-ATP efficiency from token manager"""
        if self.token_manager:
            account = self.token_manager.get_account_summary(f"expert_{expert_id}")
            if account:
                return account.get('efficiency_rating', 0.5)
        return 0.5
    
    def _get_expert_token_balance(self, expert_id: str) -> float:
        """Get token balance from token manager"""
        if self.token_manager:
            account = self.token_manager.get_account_summary(f"expert_{expert_id}")
            if account:
                return account.get('balance', 0)
        return 0.0
    
    def _get_gradient_strength(self, field_id: str) -> float:
        """Get gradient strength from gradient manager"""
        if self.gradient_manager:
            return self.gradient_manager.fields.get(field_id, 
                GradientField(field_id, field_id)).gradient_strength
        return 0.5
    
    def _get_species_population(self, species_id: str) -> int:
        """Get species population from compartment manager"""
        if self.compartment_manager:
            return sum(1 for c in self.compartment_manager.compartments.values()
                      if c.expert_type == species_id and c.is_viable)
        # Fallback: count from registry
        return len([e for e in self._experts.values()
                   if hasattr(e, 'domain') and species_id in str(e.domain).lower()])
    
    def _get_total_compartment_population(self) -> int:
        """Get total compartment population"""
        if self.compartment_manager:
            return len([c for c in self.compartment_manager.compartments.values() if c.is_viable])
        return len([e for e in self._experts.values() if e.lifecycle_state.is_available()])
    
    def _get_species_id(self, profile: ExpertProfile) -> str:
        """Extract species ID from expert profile"""
        domain = profile.domain.value if hasattr(profile.domain, 'value') else str(profile.domain)
        if 'energy' in domain.lower(): return 'energy'
        if 'data' in domain.lower(): return 'data'
        if 'iot' in domain.lower(): return 'iot'
        if 'quantum' in domain.lower(): return 'quantum'
        if 'helium' in domain.lower(): return 'helium'
        return 'general'
    
    # ========================================================================
    # Expert Registration with Bio-Inspired Correlation
    # ========================================================================
    
    def register_expert(
        self,
        profile: ExpertProfile,
        validate: bool = True,
        auto_certify: bool = False,
        create_ecoatp_account: bool = True,
        register_compartment: bool = True
    ) -> Tuple[bool, str]:
        """
        Register expert with bio-inspired correlation.
        
        Creates Eco-ATP account and optionally a chromatophore compartment.
        """
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
        
        # Validate profile
        if validate:
            is_valid, message = self._validate_profile(profile)
            if not is_valid:
                return False, f"Validation failed: {message}"
        
        # Set lifecycle state
        if auto_certify:
            profile.lifecycle_state = ExpertLifecycleState.CERTIFIED
        elif validate:
            profile.lifecycle_state = ExpertLifecycleState.VALIDATING
        else:
            profile.lifecycle_state = ExpertLifecycleState.REGISTERED
        
        # Store expert
        self._experts[profile.expert_id] = profile
        self._update_indexes(profile)
        
        # BIO-INSPIRED: Create Eco-ATP account
        if self.enable_bio_correlation and create_ecoatp_account and self.token_manager:
            account_id = f"expert_{profile.expert_id}"
            self.token_manager.create_account(account_id)
            
            # Initial token endowment based on efficiency
            initial_tokens = int(profile.efficiency_score * 100)
            if initial_tokens > 0:
                self.token_manager.generate_tokens(
                    account_id=account_id,
                    source=EcoATPSource.EFFICIENCY_GAIN,
                    energy_saved_kwh=profile.efficiency_score * 0.001,
                    num_tokens=initial_tokens
                )
            logger.info(f"Created Eco-ATP account for {profile.expert_id}: {initial_tokens} tokens")
        
        # BIO-INSPIRED: Register with compartment manager
        if self.enable_bio_correlation and register_compartment and self.compartment_manager:
            species = self._get_species_id(profile)
            self.compartment_manager.create_compartment(
                expert_type=species,
                expert_instance=None  # Would be the actual expert instance
            )
            logger.info(f"Created chromatophore compartment for {profile.expert_id}")
        
        # Initialize fitness score
        if self.enable_fitness_tracking:
            self.fitness_scores[profile.expert_id] = FitnessScore(
                expert_id=profile.expert_id,
                resource_efficiency=min(1.0, 1.0 / (1.0 + profile.carbon_per_inference * 10000)),
                resilience_score=profile.reliability_score,
                adaptation_speed=0.5,
                cooperation_score=0.5,
                ecoatp_efficiency=profile.efficiency_score
            )
            self.fitness_scores[profile.expert_id].calculate_overall()
        
        # Update dependency graph
        self._update_dependency_graph(profile)
        self._version_family_index[profile.expert_name].append(profile.expert_id)
        self._stats['total_registrations'] += 1
        self.total_generations += 1
        
        # Record evolutionary event
        self.evolutionary_events.append({
            'type': 'speciation' if not profile.replaces_expert else 'evolution',
            'expert_id': profile.expert_id,
            'species': self._get_species_id(profile),
            'generation': self.total_generations,
            'timestamp': datetime.utcnow().isoformat()
        })
        self.speciation_count += 1
        
        logger.info(f"Registered expert: {profile.expert_id} v{profile.version.to_string()} "
                   f"(species: {self._get_species_id(profile)}, generation: {self.total_generations})")
        
        return True, f"Expert {profile.expert_id} registered successfully"
    
    def _validate_profile(self, profile: ExpertProfile) -> Tuple[bool, str]:
        """Validate expert profile completeness"""
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
        for incompatible_id in profile.incompatible_with:
            if incompatible_id == profile.expert_id:
                errors.append("Cannot be incompatible with self")
        if errors: return False, "; ".join(errors)
        return True, "Profile valid"
    
    def _update_indexes(self, profile: ExpertProfile):
        """Update all indexes for an expert"""
        self._domain_index[profile.domain].add(profile.expert_id)
        self._hardware_index[profile.hardware_profile].add(profile.expert_id)
        self._lifecycle_index[profile.lifecycle_state].add(profile.expert_id)
        for tag in profile.tags: self._tag_index[tag].add(profile.expert_id)
        for cap in profile.capabilities: self._capability_index[cap].add(profile.expert_id)
        for tt in profile.supported_task_types: self._task_type_index[tt].add(profile.expert_id)
        self._region_index[profile.origin_region].add(profile.expert_id)
    
    def _update_dependency_graph(self, profile: ExpertProfile):
        """Update dependency graph"""
        self._dependency_graph.add_node(profile.expert_id, name=profile.expert_name,
                                        version=profile.version.to_string())
        for dep in profile.dependencies:
            self._dependency_graph.add_edge(profile.expert_id, dep.dependency_id,
                                           optional=dep.is_optional,
                                           version_req=dep.version_requirement)
    
    # ========================================================================
    # Bio-Inspired Filtering Methods
    # ========================================================================
    
    def filter_by_ecoatp_efficiency(
        self,
        min_efficiency: float = 0.5,
        min_token_balance: float = 10.0
    ) -> List[ExpertProfile]:
        """
        Filter experts by Eco-ATP efficiency and token balance.
        
        Uses real data from token manager when available.
        """
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
        
        logger.debug(f"Eco-ATP filter: {len(efficient)}/{len(self._experts)} experts passed "
                    f"(min_efficiency={min_efficiency}, min_balance={min_token_balance})")
        
        return efficient
    
    def filter_by_health_and_fitness(
        self,
        min_health: float = 0.5,
        min_fitness: float = 0.4
    ) -> List[ExpertProfile]:
        """Filter experts by health score and fitness"""
        qualified = []
        for expert_id, expert in self._experts.items():
            if not expert.lifecycle_state.is_available():
                continue
            
            health = expert.health.calculate_health_score()
            fitness = self.fitness_scores.get(expert_id, FitnessScore(expert_id)).overall_fitness
            
            if health >= min_health and fitness >= min_fitness:
                qualified.append(expert)
        
        return qualified
    
    def filter_by_gradient_alignment(
        self,
        carbon_threshold: float = 0.3,
        trust_threshold: float = 0.4
    ) -> List[ExpertProfile]:
        """Filter experts aligned with current gradient conditions"""
        if not self.enable_bio_correlation or not self.gradient_manager:
            return self.get_all_active_experts()
        
        carbon_strength = self._get_gradient_strength('carbon')
        trust_strength = self._get_gradient_strength('trust')
        
        # In high carbon zones, prefer low-carbon experts
        if carbon_strength > carbon_threshold:
            return sorted(
                [e for e in self.get_all_active_experts()],
                key=lambda e: e.carbon_per_inference
            )[:max(1, len(self._experts) // 2)]
        
        # In low trust zones, prefer high-reliability experts
        if trust_strength < trust_threshold:
            return sorted(
                [e for e in self.get_all_active_experts()],
                key=lambda e: e.reliability_score,
                reverse=True
            )[:max(1, len(self._experts) // 2)]
        
        return self.get_all_active_experts()
    
    # ========================================================================
    # Natural Selection and Evolution
    # ========================================================================
    
    def update_fitness_from_gradients(self):
        """Update expert fitness scores based on gradient fields"""
        if not self.enable_bio_correlation or not self.gradient_manager:
            return
        
        trust_strength = self._get_gradient_strength('trust')
        carbon_strength = self._get_gradient_strength('carbon')
        
        for expert_id, fitness in self.fitness_scores.items():
            if expert_id not in self._experts:
                continue
            
            expert = self._experts[expert_id]
            
            # Update resilience from trust gradient
            fitness.resilience_score = (
                fitness.resilience_score * 0.7 + trust_strength * 0.3
            )
            
            # Update resource efficiency from carbon gradient
            carbon_efficiency = 1.0 / (1.0 + expert.carbon_per_inference * 10000)
            fitness.resource_efficiency = (
                fitness.resource_efficiency * 0.8 + carbon_efficiency * 0.2
            )
            
            # Update Eco-ATP efficiency
            fitness.ecoatp_efficiency = self._get_expert_ecoatp_efficiency(expert_id)
            
            # Update cooperation score from compartment health
            if self.compartment_manager:
                compartment = self.compartment_manager.find_best_compartment(
                    self._get_species_id(expert)
                )
                if compartment:
                    fitness.cooperation_score = (
                        fitness.cooperation_score * 0.8 + compartment.health_score * 0.2
                    )
            
            fitness.calculate_overall()
    
    def trigger_natural_selection(self):
        """
        Apply natural selection pressure.
        
        Experts with low fitness are deprecated.
        Experts with high fitness are marked for reproduction.
        """
        if not self.enable_natural_selection:
            return
        
        # Update fitness from gradients first
        self.update_fitness_from_gradients()
        
        # Calculate fitness threshold (bottom 20%)
        fitnesses = [f.overall_fitness for f in self.fitness_scores.values()]
        if not fitnesses:
            return
        
        threshold = np.percentile(fitnesses, 20)
        top_threshold = np.percentile(fitnesses, 80)
        
        deprecated_count = 0
        reproducer_count = 0
        
        for expert_id, fitness in list(self.fitness_scores.items()):
            if expert_id not in self._experts:
                continue
            
            expert = self._experts[expert_id]
            
            # Deprecate low-fitness experts
            if (fitness.overall_fitness < threshold and
                fitness.reproductive_success == 0 and
                expert.lifecycle_state in [ExpertLifecycleState.ACTIVE, ExpertLifecycleState.CERTIFIED]):
                
                self.deprecate_expert(expert_id, reason="natural_selection_low_fitness")
                deprecated_count += 1
                
                # Store knowledge before deprecation
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
                    'reason': 'natural_selection',
                    'timestamp': datetime.utcnow().isoformat()
                })
                self.extinction_count += 1
            
            # Mark high-fitness for reproduction
            elif fitness.overall_fitness > top_threshold and fitness.reproductive_success < 3:
                fitness.reproductive_success += 1
                reproducer_count += 1
        
        self._stats['total_natural_selections'] += 1
        self._stats['last_selection'] = datetime.utcnow()
        
        if deprecated_count > 0 or reproducer_count > 0:
            logger.info(f"Natural selection: {deprecated_count} deprecated, "
                       f"{reproducer_count} marked for reproduction "
                       f"(threshold={threshold:.3f}, population={len(fitnesses)})")
    
    def deprecate_expert(
        self,
        expert_id: str,
        replacement_id: Optional[str] = None,
        reason: str = "manual"
    ) -> Tuple[bool, str]:
        """Deprecate expert with reason tracking"""
        if expert_id not in self._experts:
            return False, f"Expert {expert_id} not found"
        
        profile = self._experts[expert_id]
        profile.lifecycle_state = ExpertLifecycleState.DEPRECATED
        profile.is_active = False
        
        if replacement_id and replacement_id in self._experts:
            profile.replaced_by = replacement_id
            self._migration_paths[expert_id] = replacement_id
        
        # Update lifecycle index
        self._lifecycle_index[ExpertLifecycleState.DEPRECATED].add(expert_id)
        
        logger.info(f"Deprecated expert: {expert_id} (reason: {reason}, replacement: {replacement_id})")
        return True, f"Expert {expert_id} deprecated"
    
    def activate_expert(self, expert_id: str) -> Tuple[bool, str]:
        """Activate expert for production use"""
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
    
    # ========================================================================
    # Performance Tracking with Bio-Inspired Integration
    # ========================================================================
    
    def update_performance(
        self,
        expert_id: str,
        metrics: Dict[str, Any]
    ):
        """Record expert performance with bio-inspired updates"""
        if expert_id not in self._experts:
            return
        
        # Add to performance history
        self._performance_history[expert_id].append({
            **metrics,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        # Keep last 10000 records
        if len(self._performance_history[expert_id]) > 10000:
            self._performance_history[expert_id] = self._performance_history[expert_id][-10000:]
        
        # Update health metrics
        expert = self._experts[expert_id]
        if 'success' in metrics:
            alpha = 0.1
            expert.health.success_rate = (
                expert.health.success_rate * (1 - alpha) +
                (1.0 if metrics['success'] else 0.0) * alpha
            )
        if 'latency_ms' in metrics:
            expert.health.avg_latency_ms = metrics['latency_ms']
        if 'carbon_kg' in metrics:
            expert.health.carbon_efficiency = 1.0 / (1.0 + metrics['carbon_kg'] * 1000)
        if 'helium_units' in metrics:
            expert.health.helium_efficiency = 1.0 / (1.0 + metrics['helium_units'] * 100)
        
        expert.health.last_heartbeat = datetime.utcnow()
        
        # BIO-INSPIRED: Update fitness score
        if self.enable_fitness_tracking and expert_id in self.fitness_scores:
            fitness = self.fitness_scores[expert_id]
            
            if 'success' in metrics:
                fitness.resilience_score = (
                    fitness.resilience_score * 0.8 +
                    (1.0 if metrics['success'] else 0.0) * 0.2
                )
            
            if 'carbon_kg' in metrics:
                fitness.resource_efficiency = 1.0 / (1.0 + metrics['carbon_kg'] * 10000)
            
            if 'ecoatp_efficiency' in metrics:
                fitness.ecoatp_efficiency = metrics['ecoatp_efficiency']
            
            fitness.calculate_overall()
        
        # BIO-INSPIRED: Pump trust gradient on success
        if self.enable_bio_correlation and self.gradient_manager:
            trust_delta = 0.05 if metrics.get('success', False) else -0.1
            self.gradient_manager.pump_field('trust', trust_delta, source=f"expert_{expert_id}")
        
        # Check health and auto-degrade if needed
        health_score = expert.health.calculate_health_score()
        if health_score < 0.3 and expert.lifecycle_state == ExpertLifecycleState.ACTIVE:
            expert.lifecycle_state = ExpertLifecycleState.DEGRADED
            logger.warning(f"Expert {expert_id} auto-degraded (health: {health_score:.2f})")
        elif health_score > 0.7 and expert.lifecycle_state == ExpertLifecycleState.DEGRADED:
            expert.lifecycle_state = ExpertLifecycleState.ACTIVE
            logger.info(f"Expert {expert_id} auto-recovered (health: {health_score:.2f})")
    
    # ========================================================================
    # Bio-Inspired Background Tasks
    # ========================================================================
    
    async def _bio_correlation_loop(self):
        """Background loop for bio-inspired correlation maintenance"""
        while True:
            try:
                if self.enable_bio_correlation:
                    # Update fitness from gradients
                    if self.gradient_manager:
                        self.update_fitness_from_gradients()
                    
                    # Trigger natural selection periodically
                    if self.enable_natural_selection:
                        self.trigger_natural_selection()
                    
                    # Update compartment population tracking
                    if self.compartment_manager and self.enable_population_tracking:
                        for species_id in ['energy', 'data', 'iot', 'quantum', 'helium']:
                            population = self._get_species_population(species_id)
                            logger.debug(f"Species {species_id} population: {population}")
                
                await asyncio.sleep(300)  # Every 5 minutes
                
            except Exception as e:
                logger.error(f"Bio-correlation loop error: {str(e)}")
                await asyncio.sleep(60)
    
    # ========================================================================
    # Enhanced Statistics and Reporting
    # ========================================================================
    
    def get_registry_stats(self) -> Dict[str, Any]:
        """Get comprehensive registry statistics with bio-inspired metrics"""
        total = len(self._experts)
        available = len(self.get_all_active_experts())
        
        stats = {
            'registry_id': self.registry_id,
            'total_experts': total,
            'available_experts': available,
            'degraded_experts': len(self._lifecycle_index.get(ExpertLifecycleState.DEGRADED, set())),
            'deprecated_experts': len(self._lifecycle_index.get(ExpertLifecycleState.DEPRECATED, set())),
            
            # Domain distribution
            'domains': {domain.value: len(experts) for domain, experts in self._domain_index.items()},
            'hardware_distribution': {hw.value: len(experts) for hw, experts in self._hardware_index.items()},
            'lifecycle_distribution': {state.value: len(self._lifecycle_index.get(state, set())) 
                                       for state in ExpertLifecycleState},
            
            # Bio-inspired metrics
            'bio_correlation_enabled': self.enable_bio_correlation,
            'bio_modules_available': BIO_INSPIRED_AVAILABLE,
            
            # Evolutionary metrics
            'evolution': {
                'total_generations': self.total_generations,
                'speciation_events': self.speciation_count,
                'extinction_events': self.extinction_count,
                'natural_selections': self._stats['total_natural_selections'],
                'last_selection': self._stats['last_selection'].isoformat() if self._stats['last_selection'] else None,
                'average_fitness': np.mean([f.overall_fitness for f in self.fitness_scores.values()]) if self.fitness_scores else 0,
                'top_fitness': max([f.overall_fitness for f in self.fitness_scores.values()]) if self.fitness_scores else 0
            }
        }
        
        # Population tracking
        if self.enable_population_tracking:
            stats['species_populations'] = {
                species: self._get_species_population(species)
                for species in ['energy', 'data', 'iot', 'quantum', 'helium']
            }
            stats['total_population'] = self._get_total_compartment_population()
        
        # Token economy
        if self.token_manager:
            stats['token_economy'] = self.token_manager.get_system_summary()
        
        # Gradient health
        if self.gradient_manager:
            stats['gradient_health'] = self.gradient_manager.get_field_strengths()
        
        # Fitness distribution
        if self.fitness_scores:
            fitnesses = [f.overall_fitness for f in self.fitness_scores.values()]
            stats['fitness_distribution'] = {
                'mean': np.mean(fitnesses),
                'median': np.median(fitnesses),
                'std': np.std(fitnesses),
                'min': np.min(fitnesses),
                'max': np.max(fitnesses),
                'q25': np.percentile(fitnesses, 25),
                'q75': np.percentile(fitnesses, 75)
            }
        
        return stats
    
    def get_expert_performance(self, expert_id: str) -> List[Dict]:
        """Get performance history for expert"""
        return self._performance_history.get(expert_id, [])
    
    def get_all_active_experts(self) -> List[ExpertProfile]:
        """Get all currently active experts"""
        return [e for e in self._experts.values()
                if e.is_active and e.lifecycle_state.is_available()]
    
    def get_expert(self, expert_id: str) -> Optional[ExpertProfile]:
        """Get expert by ID"""
        return self._experts.get(expert_id)
    
    def get_experts_by_domain(self, domain: ExpertDomain) -> List[ExpertProfile]:
        """Get experts by domain"""
        expert_ids = self._domain_index.get(domain, set())
        return [self._experts[eid] for eid in expert_ids if eid in self._experts]
    
    def get_experts_by_lifecycle(self, state: ExpertLifecycleState) -> List[ExpertProfile]:
        """Get experts by lifecycle state"""
        expert_ids = self._lifecycle_index.get(state, set())
        return [self._experts[eid] for eid in expert_ids if eid in self._experts]
    
    def get_fitness_score(self, expert_id: str) -> Optional[FitnessScore]:
        """Get fitness score for expert"""
        return self.fitness_scores.get(expert_id)
    
    def get_top_fitness_experts(self, n: int = 5) -> List[Tuple[str, float]]:
        """Get top N experts by fitness"""
        sorted_fitness = sorted(self.fitness_scores.items(),
                               key=lambda x: x[1].overall_fitness, reverse=True)
        return [(eid, f.overall_fitness) for eid, f in sorted_fitness[:n]]
    
    def get_ecosystem_health_report(self) -> Dict[str, Any]:
        """Generate comprehensive ecosystem health report"""
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'total_species': len(set(self._get_species_id(e) for e in self._experts.values())),
            'total_population': self._get_total_compartment_population(),
            'genetic_diversity': len(self._experts),
            'fitness_scores': {eid: f.overall_fitness for eid, f in self.fitness_scores.items()},
            'top_performers': self.get_top_fitness_experts(5),
            'gradient_health': self.gradient_manager.get_field_strengths() if self.gradient_manager else {},
            'token_economy': self.token_manager.get_system_summary() if self.token_manager else {},
            'biomass_reserves': self.biomass_storage.get_storage_stats() if self.biomass_storage else {},
            'evolutionary_events': list(self.evolutionary_events)[-10:]
        }
    
    def cleanup_deprecated(self, max_age_days: int = 90) -> int:
        """Clean up experts deprecated longer than max_age_days"""
        cutoff = datetime.utcnow() - timedelta(days=max_age_days)
        retired_count = 0
        
        for expert_id in list(self._experts.keys()):
            profile = self._experts[expert_id]
            if (profile.lifecycle_state == ExpertLifecycleState.DEPRECATED and
                profile.retired_at is None):
                if expert_id in self._migration_paths:
                    profile.lifecycle_state = ExpertLifecycleState.RETIRED
                    profile.retired_at = datetime.utcnow()
                    retired_count += 1
        
        self._stats['last_cleanup'] = datetime.utcnow()
        logger.info(f"Cleanup: retired {retired_count} deprecated experts")
        return retired_count
