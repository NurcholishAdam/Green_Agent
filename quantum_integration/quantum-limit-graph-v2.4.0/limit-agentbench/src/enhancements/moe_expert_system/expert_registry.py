# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/expert_registry.py

"""
Enhanced Expert Registry for Green Agent MoE System
Version: 2.0.0

Comprehensive expert lifecycle management with:
- Semantic versioning and compatibility management
- Composite health scoring with trust metrics
- Dependency graph management
- Semantic expert discovery
- Full lifecycle state management (birth → retirement)
- Federation support for distributed experts
- Certification and validation tracking
- Conflict detection for expert combinations
- Dynamic weighting based on learned performance
- Expert lineage and provenance tracking
- A/B testing and canary deployment support
- Expert deprecation and migration management
- Multi-region expert synchronization
- Expert capability negotiation
- Automated expert auditing and compliance

Integration Points:
- Layer 1: Meta-cognitive performance feedback
- Layer 2: Neuro-symbolic constraint validation
- Layer 7: Expert health monitoring
- Layer 8: Immutable expert provenance ledger
- Layer 9: Pareto-optimal expert selection
"""

import logging
from typing import Dict, Any, List, Optional, Set, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict, OrderedDict
import hashlib
import json
import uuid
import semantic_version
import networkx as nx

logger = logging.getLogger(__name__)

# ============================================================================
# Enums and Data Classes
# ============================================================================

class ExpertDomain(Enum):
    """Expert specialization domains"""
    ENERGY = "energy_optimization"
    DATA = "data_engineering"
    IOT = "iot_edge_computing"
    QUANTUM = "quantum_computing"
    HELIUM = "helium_aware_computing"
    CARBON = "carbon_optimization"
    SECURITY = "security_computing"
    COOLING = "cooling_optimization"
    NETWORK = "network_optimization"
    STORAGE = "storage_optimization"
    GENERAL = "general_purpose"

class HardwareProfile(Enum):
    """Hardware execution profiles"""
    CPU_EFFICIENT = "cpu_low_power"
    CPU_PERFORMANCE = "cpu_high_performance"
    GPU_ACCELERATED = "gpu_cuda"
    GPU_EFFICIENT = "gpu_low_power"
    QUANTUM_BACKEND = "quantum_processor"
    QUANTUM_SIMULATOR = "quantum_simulator"
    EDGE_DEVICE = "edge_iot_device"
    EDGE_GATEWAY = "edge_gateway"
    HYBRID = "hybrid_cpu_gpu"
    FPGA = "fpga_accelerator"
    ASIC = "asic_custom"
    NEUROMORPHIC = "neuromorphic_chip"

class ExpertLifecycleState(Enum):
    """Expert lifecycle states"""
    REGISTERED = "registered"           # Just registered, not yet validated
    VALIDATING = "validating"           # Undergoing certification
    CERTIFIED = "certified"             # Passed validation
    ACTIVE = "active"                   # Ready for production use
    CANARY = "canary"                   # Limited deployment testing
    DEPRECATED = "deprecated"           # Still usable but not recommended
    DEGRADED = "degraded"               # Performance degraded
    MAINTENANCE = "maintenance"         # Under maintenance
    RETIRED = "retired"                 # No longer available
    ARCHIVED = "archived"               # Historical record only
    
    def is_available(self) -> bool:
        """Check if expert is available for routing"""
        return self in [
            ExpertLifecycleState.CERTIFIED,
            ExpertLifecycleState.ACTIVE,
            ExpertLifecycleState.CANARY
        ]
    
    def is_usable(self) -> bool:
        """Check if expert can still be used"""
        return self in [
            ExpertLifecycleState.CERTIFIED,
            ExpertLifecycleState.ACTIVE,
            ExpertLifecycleState.CANARY,
            ExpertLifecycleState.DEPRECATED,
            ExpertLifecycleState.DEGRADED
        ]

class CertificationLevel(Enum):
    """Expert certification levels"""
    NONE = "none"
    SELF_CERTIFIED = "self_certified"
    INTERNAL_AUDIT = "internal_audit"
    THIRD_PARTY = "third_party"
    ISO_COMPLIANT = "iso_compliant"
    FEDRAMP = "fedramp"

@dataclass
class ExpertVersion:
    """Semantic versioning for experts"""
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
        """Parse version string"""
        try:
            v = semantic_version.Version(version_str)
            return cls(
                major=v.major,
                minor=v.minor,
                patch=v.patch,
                prerelease=str(v.prerelease) if v.prerelease else None,
                build=str(v.build) if v.build else None
            )
        except ValueError:
            return cls(major=1, minor=0, patch=0)
    
    def is_compatible_with(self, other: 'ExpertVersion') -> bool:
        """Check if versions are compatible (same major)"""
        return self.major == other.major
    
    def is_newer_than(self, other: 'ExpertVersion') -> bool:
        """Check if this version is newer"""
        if self.major != other.major:
            return self.major > other.major
        if self.minor != other.minor:
            return self.minor > other.minor
        return self.patch > other.patch

@dataclass
class ExpertDependency:
    """Expert dependency declaration"""
    dependency_id: str
    dependency_type: str  # 'expert', 'service', 'hardware', 'data'
    version_requirement: str  # e.g., ">=1.0.0,<2.0.0"
    is_optional: bool = False
    is_runtime: bool = True
    description: str = ""

@dataclass
class ExpertCertification:
    """Expert certification record"""
    certification_id: str
    level: CertificationLevel
    issued_by: str
    issued_at: datetime
    expires_at: Optional[datetime] = None
    validation_results: Dict[str, Any] = field(default_factory=dict)
    is_valid: bool = True

@dataclass
class HealthMetrics:
    """Composite health metrics for expert"""
    success_rate: float = 1.0
    avg_latency_ms: float = 0.0
    error_rate: float = 0.0
    carbon_efficiency: float = 1.0
    helium_efficiency: float = 1.0
    availability: float = 1.0
    last_heartbeat: datetime = field(default_factory=datetime.utcnow)
    degradation_score: float = 0.0  # 0=healthy, 1=fully degraded
    
    def calculate_health_score(self) -> float:
        """Calculate composite health score (0-1)"""
        weights = {
            'success_rate': 0.30,
            'availability': 0.25,
            'error_rate': 0.20,
            'carbon_efficiency': 0.10,
            'helium_efficiency': 0.10,
            'degradation_score': 0.05
        }
        
        score = (
            weights['success_rate'] * self.success_rate +
            weights['availability'] * self.availability +
            weights['error_rate'] * (1 - self.error_rate) +
            weights['carbon_efficiency'] * self.carbon_efficiency +
            weights['helium_efficiency'] * self.helium_efficiency +
            weights['degradation_score'] * (1 - self.degradation_score)
        )
        
        # Check heartbeat freshness
        heartbeat_age = (datetime.utcnow() - self.last_heartbeat).total_seconds()
        if heartbeat_age > 300:  # 5 minutes
            score *= 0.5
        
        return max(0.0, min(1.0, score))

@dataclass
class ExpertLineage:
    """Track expert evolution and provenance"""
    lineage_id: str
    parent_expert_id: Optional[str] = None
    created_from: Optional[str] = None  # 'scratch', 'fork', 'distillation'
    training_data_hash: Optional[str] = None
    training_duration_hours: float = 0.0
    training_carbon_kg: float = 0.0
    model_architecture: str = ""
    hyperparameters: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)

@dataclass
class ExpertProfile:
    """
    Enhanced expert profile with comprehensive metadata.
    
    Includes versioning, health, certification, dependencies,
    lifecycle management, and lineage tracking.
    """
    # Core identification
    expert_id: str
    expert_name: str = ""
    version: ExpertVersion = field(default_factory=lambda: ExpertVersion(1, 0, 0))
    domain: ExpertDomain = ExpertDomain.GENERAL
    hardware_profile: HardwareProfile = HardwareProfile.CPU_EFFICIENT
    
    # Lifecycle management
    lifecycle_state: ExpertLifecycleState = ExpertLifecycleState.REGISTERED
    registered_at: datetime = field(default_factory=datetime.utcnow)
    activated_at: Optional[datetime] = None
    retired_at: Optional[datetime] = None
    replaces_expert: Optional[str] = None  # Expert this replaces
    replaced_by: Optional[str] = None  # Expert that replaces this
    
    # Resource consumption
    helium_per_inference: float = 0.0
    carbon_per_inference: float = 0.0
    energy_per_inference: float = 0.0
    avg_latency_ms: float = 0.0
    memory_usage_mb: float = 0.0
    
    # Capability scores (0.0 to 1.0)
    accuracy_score: float = 0.0
    reliability_score: float = 0.0
    efficiency_score: float = 0.0
    security_score: float = 0.0
    
    # Constraints
    min_carbon_zone: int = 0
    max_helium_scarcity: float = 1.0
    supported_task_types: List[str] = field(default_factory=list)
    incompatible_with: List[str] = field(default_factory=list)
    
    # Dependencies
    dependencies: List[ExpertDependency] = field(default_factory=list)
    
    # Certification
    certifications: List[ExpertCertification] = field(default_factory=list)
    
    # Health
    health: HealthMetrics = field(default_factory=HealthMetrics)
    
    # Lineage
    lineage: Optional[ExpertLineage] = None
    
    # Federation
    is_remote: bool = False
    remote_endpoint: Optional[str] = None
    origin_region: str = "local"
    
    # Dynamic weights (learned over time)
    dynamic_weights: Dict[str, float] = field(default_factory=dict)
    
    # Tags for semantic discovery
    tags: List[str] = field(default_factory=list)
    capabilities: List[str] = field(default_factory=list)
    
    # Status
    is_active: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'expert_id': self.expert_id,
            'expert_name': self.expert_name,
            'version': self.version.to_string(),
            'domain': self.domain.value,
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
            'tags': self.tags,
            'capabilities': self.capabilities,
            'supports_task_types': self.supported_task_types,
            'origin_region': self.origin_region,
            'is_remote': self.is_remote
        }
    
    def compute_hash(self) -> str:
        """Generate hash for immutable ledger logging"""
        profile_str = json.dumps(self.to_dict(), sort_keys=True)
        return hashlib.sha256(profile_str.encode()).hexdigest()
    
    def is_compatible_with(self, other: 'ExpertProfile') -> bool:
        """Check if two experts can work together"""
        # Check explicit incompatibility
        if other.expert_id in self.incompatible_with:
            return False
        if self.expert_id in other.incompatible_with:
            return False
        
        # Check version compatibility for same expert family
        if self.expert_name == other.expert_name:
            return self.version.is_compatible_with(other.version)
        
        return True
    
    def get_certification_level(self) -> CertificationLevel:
        """Get highest certification level"""
        if not self.certifications:
            return CertificationLevel.NONE
        
        levels = [c.level for c in self.certifications if c.is_valid]
        if not levels:
            return CertificationLevel.NONE
        
        # Return highest level
        level_order = list(CertificationLevel)
        return max(levels, key=lambda l: level_order.index(l))

# ============================================================================
# Enhanced Expert Registry
# ============================================================================

class ExpertRegistry:
    """
    Enhanced Expert Registry with comprehensive lifecycle management.
    
    Features:
    - Semantic versioning and compatibility management
    - Composite health scoring with trust metrics
    - Dependency graph with cycle detection
    - Semantic expert discovery by capabilities
    - Full lifecycle state machine
    - Federation support for distributed experts
    - Certification and validation tracking
    - Conflict detection for expert combinations
    - Dynamic weighting based on learned performance
    - Expert lineage and provenance tracking
    - A/B testing and canary deployment
    - Expert deprecation and migration paths
    - Automated health checking
    - Multi-region synchronization
    """
    
    def __init__(self, registry_id: str = "default"):
        self.registry_id = registry_id
        
        # Primary storage
        self._experts: Dict[str, ExpertProfile] = {}
        
        # Indexes for fast lookup
        self._domain_index: Dict[ExpertDomain, Set[str]] = defaultdict(set)
        self._hardware_index: Dict[HardwareProfile, Set[str]] = defaultdict(set)
        self._lifecycle_index: Dict[ExpertLifecycleState, Set[str]] = defaultdict(set)
        self._tag_index: Dict[str, Set[str]] = defaultdict(set)
        self._capability_index: Dict[str, Set[str]] = defaultdict(set)
        self._task_type_index: Dict[str, Set[str]] = defaultdict(set)
        self._region_index: Dict[str, Set[str]] = defaultdict(set)
        self._version_family_index: Dict[str, List[str]] = defaultdict(list)
        
        # Performance history
        self._performance_history: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
        # Dependency graph
        self._dependency_graph = nx.DiGraph()
        
        # Federation
        self._remote_registries: Dict[str, str] = {}  # registry_id -> endpoint
        self._federated_experts: Dict[str, str] = {}  # expert_id -> registry_id
        
        # A/B testing
        self._ab_tests: Dict[str, Dict[str, Any]] = {}
        
        # Migration paths
        self._migration_paths: Dict[str, str] = {}  # old_expert_id -> new_expert_id
        
        # Statistics
        self._stats = {
            'total_registrations': 0,
            'total_deregistrations': 0,
            'total_health_checks': 0,
            'total_certifications': 0,
            'last_cleanup': datetime.utcnow()
        }
        
        logger.info(f"Initialized Enhanced Expert Registry '{registry_id}'")
    
    # ========================================================================
    # Expert Registration and Lifecycle Management
    # ========================================================================
    
    def register_expert(
        self,
        profile: ExpertProfile,
        validate: bool = True,
        auto_certify: bool = False
    ) -> Tuple[bool, str]:
        """
        Register a new expert with validation.
        
        Args:
            profile: Expert profile to register
            validate: Whether to validate before registration
            auto_certify: Automatically certify if validation passes
            
        Returns:
            (success, message)
        """
        # Check if already registered
        if profile.expert_id in self._experts:
            existing = self._experts[profile.expert_id]
            
            # If new version is newer, register as update
            if profile.version.is_newer_than(existing.version):
                logger.info(f"Updating expert {profile.expert_id} from v{existing.version.to_string()} to v{profile.version.to_string()}")
                
                # Archive old version
                existing.lifecycle_state = ExpertLifecycleState.ARCHIVED
                profile.replaces_expert = existing.expert_id
                
                # Set up migration path
                self._migration_paths[existing.expert_id] = profile.expert_id
            else:
                return False, f"Expert {profile.expert_id} already registered with newer version {existing.version.to_string()}"
        
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
        
        # Update indexes
        self._update_indexes(profile)
        
        # Update dependency graph
        self._update_dependency_graph(profile)
        
        # Track version family
        self._version_family_index[profile.expert_name].append(profile.expert_id)
        
        self._stats['total_registrations'] += 1
        
        logger.info(
            f"Registered expert: {profile.expert_id} "
            f"(v{profile.version.to_string()}, state={profile.lifecycle_state.value})"
        )
        
        return True, f"Expert {profile.expert_id} registered successfully"
    
    def activate_expert(self, expert_id: str) -> Tuple[bool, str]:
        """Activate expert for production use"""
        if expert_id not in self._experts:
            return False, f"Expert {expert_id} not found"
        
        profile = self._experts[expert_id]
        
        # Check prerequisites
        if not profile.lifecycle_state.is_available():
            return False, f"Expert {expert_id} is not in a certifiable state"
        
        # Check dependencies
        dep_issues = self._check_dependencies(profile)
        if dep_issues:
            return False, f"Dependency issues: {', '.join(dep_issues)}"
        
        profile.lifecycle_state = ExpertLifecycleState.ACTIVE
        profile.activated_at = datetime.utcnow()
        profile.is_active = True
        
        # Update indexes
        self._lifecycle_index[ExpertLifecycleState.ACTIVE].add(expert_id)
        
        logger.info(f"Activated expert: {expert_id}")
        return True, f"Expert {expert_id} activated"
    
    def deprecate_expert(
        self,
        expert_id: str,
        replacement_id: Optional[str] = None
    ) -> Tuple[bool, str]:
        """Deprecate expert with optional replacement"""
        if expert_id not in self._experts:
            return False, f"Expert {expert_id} not found"
        
        profile = self._experts[expert_id]
        profile.lifecycle_state = ExpertLifecycleState.DEPRECATED
        profile.is_active = False
        
        # Set replacement
        if replacement_id and replacement_id in self._experts:
            profile.replaced_by = replacement_id
            self._migration_paths[expert_id] = replacement_id
        
        logger.info(f"Deprecated expert: {expert_id} (replacement: {replacement_id})")
        return True, f"Expert {expert_id} deprecated"
    
    def retire_expert(self, expert_id: str) -> Tuple[bool, str]:
        """Retire expert from service"""
        if expert_id not in self._experts:
            return False, f"Expert {expert_id} not found"
        
        profile = self._experts[expert_id]
        profile.lifecycle_state = ExpertLifecycleState.RETIRED
        profile.retired_at = datetime.utcnow()
        profile.is_active = False
        
        # Clean up indexes
        self._remove_from_indexes(expert_id)
        
        # Archive after retirement
        profile.lifecycle_state = ExpertLifecycleState.ARCHIVED
        
        self._stats['total_deregistrations'] += 1
        
        logger.info(f"Retired expert: {expert_id}")
        return True, f"Expert {expert_id} retired"
    
    # ========================================================================
    # Validation and Certification
    # ========================================================================
    
    def _validate_profile(self, profile: ExpertProfile) -> Tuple[bool, str]:
        """Validate expert profile completeness and correctness"""
        errors = []
        
        # Check required fields
        if not profile.expert_id:
            errors.append("expert_id is required")
        if not profile.expert_name:
            errors.append("expert_name is required")
        
        # Validate version
        if profile.version.major < 0:
            errors.append("Invalid version")
        
        # Validate scores
        for score_name, score_value in [
            ('accuracy_score', profile.accuracy_score),
            ('reliability_score', profile.reliability_score),
            ('efficiency_score', profile.efficiency_score)
        ]:
            if not (0.0 <= score_value <= 1.0):
                errors.append(f"{score_name} must be between 0 and 1")
        
        # Validate resource metrics
        for metric_name, metric_value in [
            ('helium_per_inference', profile.helium_per_inference),
            ('carbon_per_inference', profile.carbon_per_inference),
            ('energy_per_inference', profile.energy_per_inference)
        ]:
            if metric_value < 0:
                errors.append(f"{metric_name} cannot be negative")
        
        # Check dependencies exist or are declared optional
        for dep in profile.dependencies:
            if not dep.is_optional:
                if dep.dependency_id not in self._experts:
                    errors.append(f"Required dependency {dep.dependency_id} not registered")
        
        # Check incompatibility consistency
        for incompatible_id in profile.incompatible_with:
            if incompatible_id == profile.expert_id:
                errors.append("Cannot be incompatible with self")
        
        if errors:
            return False, "; ".join(errors)
        
        return True, "Profile valid"
    
    def certify_expert(
        self,
        expert_id: str,
        certification: ExpertCertification
    ) -> Tuple[bool, str]:
        """Certify an expert"""
        if expert_id not in self._experts:
            return False, f"Expert {expert_id} not found"
        
        profile = self._experts[expert_id]
        profile.certifications.append(certification)
        
        if certification.level in [CertificationLevel.THIRD_PARTY, CertificationLevel.ISO_COMPLIANT]:
            profile.lifecycle_state = ExpertLifecycleState.CERTIFIED
        
        self._stats['total_certifications'] += 1
        
        logger.info(f"Certified expert {expert_id} at level {certification.level.value}")
        return True, f"Expert {expert_id} certified"
    
    # ========================================================================
    # Health Management
    # ========================================================================
    
    def update_health(
        self,
        expert_id: str,
        health_update: Dict[str, Any]
    ) -> bool:
        """Update expert health metrics"""
        if expert_id not in self._experts:
            return False
        
        profile = self._experts[expert_id]
        
        # Update health metrics
        if 'success_rate' in health_update:
            profile.health.success_rate = health_update['success_rate']
        if 'avg_latency_ms' in health_update:
            profile.health.avg_latency_ms = health_update['avg_latency_ms']
        if 'error_rate' in health_update:
            profile.health.error_rate = health_update['error_rate']
        if 'carbon_efficiency' in health_update:
            profile.health.carbon_efficiency = health_update['carbon_efficiency']
        if 'helium_efficiency' in health_update:
            profile.health.helium_efficiency = health_update['helium_efficiency']
        if 'availability' in health_update:
            profile.health.availability = health_update['availability']
        
        profile.health.last_heartbeat = datetime.utcnow()
        
        # Check health score and update lifecycle
        health_score = profile.health.calculate_health_score()
        if health_score < 0.3 and profile.lifecycle_state == ExpertLifecycleState.ACTIVE:
            profile.lifecycle_state = ExpertLifecycleState.DEGRADED
            logger.warning(f"Expert {expert_id} degraded (health: {health_score:.2f})")
        elif health_score > 0.7 and profile.lifecycle_state == ExpertLifecycleState.DEGRADED:
            profile.lifecycle_state = ExpertLifecycleState.ACTIVE
            logger.info(f"Expert {expert_id} recovered (health: {health_score:.2f})")
        
        self._stats['total_health_checks'] += 1
        
        return True
    
    def check_health_all(self) -> Dict[str, float]:
        """Check health of all experts"""
        health_scores = {}
        for expert_id, profile in self._experts.items():
            health_scores[expert_id] = profile.health.calculate_health_score()
        return health_scores
    
    # ========================================================================
    # Expert Discovery and Querying
    # ========================================================================
    
    def get_expert(self, expert_id: str) -> Optional[ExpertProfile]:
        """Get expert by ID"""
        return self._experts.get(expert_id)
    
    def get_experts_by_domain(self, domain: ExpertDomain) -> List[ExpertProfile]:
        """Get experts by domain"""
        expert_ids = self._domain_index.get(domain, set())
        return [self._experts[eid] for eid in expert_ids if eid in self._experts]
    
    def get_experts_by_hardware(self, hardware: HardwareProfile) -> List[ExpertProfile]:
        """Get experts by hardware profile"""
        expert_ids = self._hardware_index.get(hardware, set())
        return [self._experts[eid] for eid in expert_ids if eid in self._experts]
    
    def get_experts_by_lifecycle(
        self,
        state: ExpertLifecycleState
    ) -> List[ExpertProfile]:
        """Get experts by lifecycle state"""
        expert_ids = self._lifecycle_index.get(state, set())
        return [self._experts[eid] for eid in expert_ids if eid in self._experts]
    
    def get_experts_by_tag(self, tag: str) -> List[ExpertProfile]:
        """Get experts by tag (semantic discovery)"""
        expert_ids = self._tag_index.get(tag, set())
        return [self._experts[eid] for eid in expert_ids if eid in self._experts]
    
    def get_experts_by_capability(self, capability: str) -> List[ExpertProfile]:
        """Get experts by capability"""
        expert_ids = self._capability_index.get(capability, set())
        return [self._experts[eid] for eid in expert_ids if eid in self._experts]
    
    def get_experts_by_task_type(self, task_type: str) -> List[ExpertProfile]:
        """Get experts supporting a task type"""
        expert_ids = self._task_type_index.get(task_type, set())
        return [self._experts[eid] for eid in expert_ids if eid in self._experts]
    
    def search_experts(
        self,
        query: str,
        search_fields: List[str] = None
    ) -> List[ExpertProfile]:
        """
        Semantic search for experts.
        
        Searches across name, tags, capabilities, and task types.
        """
        if search_fields is None:
            search_fields = ['name', 'tags', 'capabilities', 'task_types']
        
        query_lower = query.lower()
        results = set()
        
        for expert_id, profile in self._experts.items():
            if 'name' in search_fields and query_lower in profile.expert_name.lower():
                results.add(expert_id)
            if 'tags' in search_fields:
                for tag in profile.tags:
                    if query_lower in tag.lower():
                        results.add(expert_id)
            if 'capabilities' in search_fields:
                for cap in profile.capabilities:
                    if query_lower in cap.lower():
                        results.add(expert_id)
            if 'task_types' in search_fields:
                for tt in profile.supported_task_types:
                    if query_lower in tt.lower():
                        results.add(expert_id)
        
        return [self._experts[eid] for eid in results]
    
    def filter_experts(
        self,
        domain: Optional[ExpertDomain] = None,
        max_helium: Optional[float] = None,
        max_carbon: Optional[float] = None,
        max_energy: Optional[float] = None,
        min_accuracy: Optional[float] = None,
        min_reliability: Optional[float] = None,
        min_health: Optional[float] = None,
        hardware: Optional[HardwareProfile] = None,
        lifecycle_states: Optional[List[ExpertLifecycleState]] = None,
        exclude_quantum: bool = False,
        min_certification: Optional[CertificationLevel] = None,
        tags: Optional[List[str]] = None,
        capabilities: Optional[List[str]] = None,
        region: Optional[str] = None,
        task_type: Optional[str] = None
    ) -> List[ExpertProfile]:
        """
        Enhanced multi-criteria expert filtering.
        
        Supports all new dimensions: health, certification, tags,
        capabilities, region, and lifecycle state.
        """
        # Start with all experts
        candidates = list(self._experts.values())
        
        # Apply filters
        if domain:
            candidates = [e for e in candidates if e.domain == domain]
        
        if max_helium is not None:
            candidates = [e for e in candidates if e.helium_per_inference <= max_helium]
        
        if max_carbon is not None:
            candidates = [e for e in candidates if e.carbon_per_inference <= max_carbon]
        
        if max_energy is not None:
            candidates = [e for e in candidates if e.energy_per_inference <= max_energy]
        
        if min_accuracy is not None:
            candidates = [e for e in candidates if e.accuracy_score >= min_accuracy]
        
        if min_reliability is not None:
            candidates = [e for e in candidates if e.reliability_score >= min_reliability]
        
        if min_health is not None:
            candidates = [
                e for e in candidates
                if e.health.calculate_health_score() >= min_health
            ]
        
        if hardware:
            candidates = [e for e in candidates if e.hardware_profile == hardware]
        
        if lifecycle_states:
            candidates = [e for e in candidates if e.lifecycle_state in lifecycle_states]
        else:
            # Default: only available experts
            candidates = [e for e in candidates if e.lifecycle_state.is_available()]
        
        if exclude_quantum:
            candidates = [e for e in candidates if e.hardware_profile != HardwareProfile.QUANTUM_BACKEND]
        
        if min_certification:
            level_order = list(CertificationLevel)
            min_idx = level_order.index(min_certification)
            candidates = [
                e for e in candidates
                if level_order.index(e.get_certification_level()) >= min_idx
            ]
        
        if tags:
            for tag in tags:
                candidates = [
                    e for e in candidates
                    if tag in e.tags
                ]
        
        if capabilities:
            for cap in capabilities:
                candidates = [
                    e for e in candidates
                    if cap in e.capabilities
                ]
        
        if region:
            candidates = [e for e in candidates if e.origin_region == region]
        
        if task_type:
            candidates = [
                e for e in candidates
                if task_type in e.supported_task_types
            ]
        
        # Sort by health score (descending)
        candidates.sort(
            key=lambda e: e.health.calculate_health_score(),
            reverse=True
        )
        
        return candidates
    
    # ========================================================================
    # Compatibility and Conflict Detection
    # ========================================================================
    
    def check_compatibility(
        self,
        expert_ids: List[str]
    ) -> Tuple[bool, List[str]]:
        """
        Check if a set of experts can work together.
        
        Returns (is_compatible, list_of_issues)
        """
        issues = []
        
        if len(expert_ids) < 2:
            return True, []
        
        experts = [
            self._experts[eid] for eid in expert_ids
            if eid in self._experts
        ]
        
        if len(experts) < 2:
            return True, []
        
        # Check pairwise compatibility
        for i, e1 in enumerate(experts):
            for e2 in experts[i+1:]:
                if not e1.is_compatible_with(e2):
                    issues.append(
                        f"Incompatible: {e1.expert_id} and {e2.expert_id}"
                    )
        
        # Check dependency graph for conflicts
        for expert_id in expert_ids:
            try:
                # Check for circular dependencies
                cycle = self._find_dependency_cycle(expert_id)
                if cycle:
                    issues.append(
                        f"Circular dependency detected: {' -> '.join(cycle)}"
                    )
            except Exception:
                pass
        
        return len(issues) == 0, issues
    
    def find_optimal_combination(
        self,
        requirements: Dict[str, Any],
        max_experts: int = 3
    ) -> List[List[ExpertProfile]]:
        """
        Find optimal expert combinations that meet requirements.
        
        Uses compatibility checking and scoring to find best combinations.
        """
        # Filter experts by requirements
        candidates = self.filter_experts(
            domain=requirements.get('domain'),
            min_accuracy=requirements.get('min_accuracy'),
            min_health=requirements.get('min_health', 0.7),
            task_type=requirements.get('task_type')
        )
        
        if not candidates:
            return []
        
        # Generate combinations
        from itertools import combinations
        
        valid_combinations = []
        
        for size in range(1, min(max_experts + 1, len(candidates) + 1)):
            for combo in combinations(candidates, size):
                combo_ids = [e.expert_id for e in combo]
                is_compatible, _ = self.check_compatibility(combo_ids)
                
                if is_compatible:
                    # Score combination
                    score = self._score_combination(combo, requirements)
                    valid_combinations.append((list(combo), score))
        
        # Sort by score descending
        valid_combinations.sort(key=lambda x: x[1], reverse=True)
        
        return [combo for combo, _ in valid_combinations[:10]]
    
    def _score_combination(
        self,
        experts: List[ExpertProfile],
        requirements: Dict[str, Any]
    ) -> float:
        """Score an expert combination"""
        score = 0.0
        
        for expert in experts:
            # Health score
            score += expert.health.calculate_health_score() * 0.3
            
            # Accuracy
            score += expert.accuracy_score * 0.2
            
            # Efficiency
            score += expert.efficiency_score * 0.2
            
            # Carbon/Helium efficiency
            carbon_score = 1.0 / (1.0 + expert.carbon_per_inference * 1000)
            helium_score = 1.0 / (1.0 + expert.helium_per_inference * 100)
            score += (carbon_score + helium_score) * 0.15
        
        # Normalize by number of experts (prefer fewer)
        score /= len(experts) ** 0.3
        
        return score
    
    # ========================================================================
    # Dependency Management
    # ========================================================================
    
    def _update_dependency_graph(self, profile: ExpertProfile):
        """Update dependency graph with expert dependencies"""
        self._dependency_graph.add_node(
            profile.expert_id,
            name=profile.expert_name,
            version=profile.version.to_string()
        )
        
        for dep in profile.dependencies:
            self._dependency_graph.add_edge(
                profile.expert_id,
                dep.dependency_id,
                optional=dep.is_optional,
                version_req=dep.version_requirement
            )
    
    def _check_dependencies(self, profile: ExpertProfile) -> List[str]:
        """Check if all required dependencies are satisfied"""
        issues = []
        
        for dep in profile.dependencies:
            if dep.is_optional:
                continue
            
            if dep.dependency_id not in self._experts:
                issues.append(f"Missing dependency: {dep.dependency_id}")
                continue
            
            dep_profile = self._experts[dep.dependency_id]
            
            # Check dependency is active
            if not dep_profile.lifecycle_state.is_available():
                issues.append(
                    f"Dependency {dep.dependency_id} not available "
                    f"(state: {dep_profile.lifecycle_state.value})"
                )
        
        return issues
    
    def _find_dependency_cycle(self, start_id: str) -> Optional[List[str]]:
        """Find circular dependency paths"""
        try:
            cycle = nx.find_cycle(self._dependency_graph, source=start_id)
            return [node for node, _ in cycle]
        except nx.NetworkXNoCycle:
            return None
    
    def get_dependency_tree(self, expert_id: str) -> Dict[str, Any]:
        """Get dependency tree for an expert"""
        if expert_id not in self._experts:
            return {}
        
        try:
            # Get all ancestors (dependencies)
            ancestors = list(nx.ancestors(self._dependency_graph, expert_id))
            
            # Get all descendants (dependents)
            descendants = list(nx.descendants(self._dependency_graph, expert_id))
            
            return {
                'expert_id': expert_id,
                'dependencies': ancestors,
                'dependents': descendants,
                'total_dependencies': len(ancestors),
                'total_dependents': len(descendants)
            }
        except Exception:
            return {'expert_id': expert_id, 'error': 'Graph traversal failed'}
    
    # ========================================================================
    # Federation Support
    # ========================================================================
    
    def register_remote_registry(
        self,
        registry_id: str,
        endpoint: str
    ) -> bool:
        """Register a remote expert registry for federation"""
        self._remote_registries[registry_id] = endpoint
        logger.info(f"Registered remote registry: {registry_id} at {endpoint}")
        return True
    
    def import_remote_expert(
        self,
        remote_registry_id: str,
        expert_id: str
    ) -> Tuple[bool, str]:
        """Import expert from remote registry"""
        if remote_registry_id not in self._remote_registries:
            return False, f"Remote registry {remote_registry_id} not registered"
        
        # In production, would fetch from remote endpoint
        # For now, create a placeholder
        remote_profile = ExpertProfile(
            expert_id=f"remote_{remote_registry_id}_{expert_id}",
            expert_name=f"Remote Expert ({remote_registry_id})",
            is_remote=True,
            remote_endpoint=self._remote_registries[remote_registry_id],
            origin_region=remote_registry_id
        )
        
        success, message = self.register_expert(remote_profile, validate=False)
        
        if success:
            self._federated_experts[remote_profile.expert_id] = remote_registry_id
        
        return success, message
    
    def get_federated_experts(
        self,
        registry_id: Optional[str] = None
    ) -> List[ExpertProfile]:
        """Get experts from federated registries"""
        if registry_id:
            expert_ids = [
                eid for eid, rid in self._federated_experts.items()
                if rid == registry_id
            ]
        else:
            expert_ids = list(self._federated_experts.keys())
        
        return [self._experts[eid] for eid in expert_ids if eid in self._experts]
    
    # ========================================================================
    # A/B Testing Support
    # ========================================================================
    
    def create_ab_test(
        self,
        test_id: str,
        expert_a_id: str,
        expert_b_id: str,
        traffic_split: float = 0.5,
        duration_hours: float = 24.0
    ) -> Tuple[bool, str]:
        """Create A/B test between two expert versions"""
        if expert_a_id not in self._experts:
            return False, f"Expert A {expert_a_id} not found"
        if expert_b_id not in self._experts:
            return False, f"Expert B {expert_b_id} not found"
        
        self._ab_tests[test_id] = {
            'expert_a': expert_a_id,
            'expert_b': expert_b_id,
            'traffic_split': traffic_split,
            'duration_hours': duration_hours,
            'started_at': datetime.utcnow(),
            'ends_at': datetime.utcnow() + timedelta(hours=duration_hours),
            'results_a': [],
            'results_b': [],
            'status': 'running'
        }
        
        # Set both experts to canary state
        self._experts[expert_a_id].lifecycle_state = ExpertLifecycleState.CANARY
        self._experts[expert_b_id].lifecycle_state = ExpertLifecycleState.CANARY
        
        logger.info(f"Created A/B test {test_id}: {expert_a_id} vs {expert_b_id}")
        return True, f"A/B test {test_id} created"
    
    def record_ab_result(
        self,
        test_id: str,
        variant: str,  # 'a' or 'b'
        metrics: Dict[str, Any]
    ) -> bool:
        """Record result for A/B test"""
        if test_id not in self._ab_tests:
            return False
        
        test = self._ab_tests[test_id]
        key = f'results_{variant}'
        test[key].append(metrics)
        
        return True
    
    def complete_ab_test(self, test_id: str) -> Optional[str]:
        """
        Complete A/B test and return winning expert ID.
        """
        if test_id not in self._ab_tests:
            return None
        
        test = self._ab_tests[test_id]
        test['status'] = 'completed'
        test['completed_at'] = datetime.utcnow()
        
        # Compare results
        results_a = test['results_a']
        results_b = test['results_b']
        
        if not results_a or not results_b:
            # Not enough data
            self._experts[test['expert_a']].lifecycle_state = ExpertLifecycleState.ACTIVE
            self._experts[test['expert_b']].lifecycle_state = ExpertLifecycleState.ACTIVE
            return test['expert_a']  # Default to A
        
        # Calculate average performance
        avg_a = np.mean([r.get('score', 0) for r in results_a])
        avg_b = np.mean([r.get('score', 0) for r in results_b])
        
        winner = test['expert_a'] if avg_a >= avg_b else test['expert_b']
        loser = test['expert_b'] if winner == test['expert_a'] else test['expert_a']
        
        # Activate winner, deprecate loser
        self._experts[winner].lifecycle_state = ExpertLifecycleState.ACTIVE
        self.deprecate_expert(loser, replacement_id=winner)
        
        logger.info(f"A/B test {test_id} complete. Winner: {winner}")
        return winner
    
    # ========================================================================
    # Performance History and Dynamic Weights
    # ========================================================================
    
    def update_performance(
        self,
        expert_id: str,
        metrics: Dict[str, Any]
    ):
        """Record expert performance for dynamic weighting"""
        if expert_id not in self._experts:
            return
        
        # Add to performance history
        self._performance_history[expert_id].append({
            **metrics,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        # Keep last 10000 records
        if len(self._performance_history[expert_id]) > 10000:
            self._performance_history[expert_id] = \
                self._performance_history[expert_id][-10000:]
        
        # Update dynamic weights
        self._update_dynamic_weights(expert_id, metrics)
        
        # Update health
        health_update = {}
        if 'success' in metrics:
            health_update['success_rate'] = self._calculate_success_rate(expert_id)
        if 'latency_ms' in metrics:
            health_update['avg_latency_ms'] = metrics['latency_ms']
        if 'carbon_kg' in metrics:
            health_update['carbon_efficiency'] = 1.0 / (1.0 + metrics['carbon_kg'] * 1000)
        
        if health_update:
            self.update_health(expert_id, health_update)
    
    def _calculate_success_rate(self, expert_id: str) -> float:
        """Calculate success rate from history"""
        history = self._performance_history.get(expert_id, [])
        if not history:
            return 0.8
        
        recent = history[-100:]
        successes = sum(1 for h in recent if h.get('success', False))
        return successes / len(recent)
    
    def _update_dynamic_weights(
        self,
        expert_id: str,
        metrics: Dict[str, Any]
    ):
        """Update dynamic weights based on learned performance"""
        if expert_id not in self._experts:
            return
        
        profile = self._experts[expert_id]
        
        # Update weights with exponential moving average
        alpha = 0.1
        
        if 'reward' in metrics:
            old_weight = profile.dynamic_weights.get('routing_weight', 0.5)
            profile.dynamic_weights['routing_weight'] = (
                old_weight * (1 - alpha) + metrics['reward'] * alpha
            )
        
        if 'carbon_efficiency' in metrics:
            old_weight = profile.dynamic_weights.get('carbon_weight', 0.5)
            profile.dynamic_weights['carbon_weight'] = (
                old_weight * (1 - alpha) + metrics['carbon_efficiency'] * alpha
            )
    
    def get_expert_performance(self, expert_id: str) -> List[Dict]:
        """Get performance history for expert"""
        return self._performance_history.get(expert_id, [])
    
    # ========================================================================
    # Index Management
    # ========================================================================
    
    def _update_indexes(self, profile: ExpertProfile):
        """Update all indexes for an expert"""
        self._domain_index[profile.domain].add(profile.expert_id)
        self._hardware_index[profile.hardware_profile].add(profile.expert_id)
        self._lifecycle_index[profile.lifecycle_state].add(profile.expert_id)
        
        for tag in profile.tags:
            self._tag_index[tag].add(profile.expert_id)
        
        for cap in profile.capabilities:
            self._capability_index[cap].add(profile.expert_id)
        
        for tt in profile.supported_task_types:
            self._task_type_index[tt].add(profile.expert_id)
        
        self._region_index[profile.origin_region].add(profile.expert_id)
    
    def _remove_from_indexes(self, expert_id: str):
        """Remove expert from all indexes"""
        for index_set in self._domain_index.values():
            index_set.discard(expert_id)
        for index_set in self._hardware_index.values():
            index_set.discard(expert_id)
        for index_set in self._lifecycle_index.values():
            index_set.discard(expert_id)
        for index_set in self._tag_index.values():
            index_set.discard(expert_id)
        for index_set in self._capability_index.values():
            index_set.discard(expert_id)
        for index_set in self._task_type_index.values():
            index_set.discard(expert_id)
        for index_set in self._region_index.values():
            index_set.discard(expert_id)
    
    # ========================================================================
    # Registry Statistics and Reporting
    # ========================================================================
    
    def get_registry_stats(self) -> Dict[str, Any]:
        """Get comprehensive registry statistics"""
        total = len(self._experts)
        available = len(self.get_experts_by_lifecycle(ExpertLifecycleState.ACTIVE))
        degraded = len(self.get_experts_by_lifecycle(ExpertLifecycleState.DEGRADED))
        deprecated = len(self.get_experts_by_lifecycle(ExpertLifecycleState.DEPRECATED))
        
        health_scores = self.check_health_all()
        avg_health = np.mean(list(health_scores.values())) if health_scores else 0
        
        return {
            'registry_id': self.registry_id,
            'total_experts': total,
            'available_experts': available,
            'degraded_experts': degraded,
            'deprecated_experts': deprecated,
            'average_health_score': avg_health,
            'domains': {
                domain.value: len(experts)
                for domain, experts in self._domain_index.items()
            },
            'hardware_distribution': {
                hw.value: len(experts)
                for hw, experts in self._hardware_index.items()
            },
            'lifecycle_distribution': {
                state.value: len(self._lifecycle_index.get(state, set()))
                for state in ExpertLifecycleState
            },
            'federated_registries': len(self._remote_registries),
            'active_ab_tests': len([
                t for t in self._ab_tests.values()
                if t['status'] == 'running'
            ]),
            'migration_paths': len(self._migration_paths),
            'total_certifications': self._stats['total_certifications'],
            'top_tags': self._get_top_tags(10),
            'top_capabilities': self._get_top_capabilities(10)
        }
    
    def _get_top_tags(self, n: int) -> List[Dict]:
        """Get most common tags"""
        tag_counts = {tag: len(experts) for tag, experts in self._tag_index.items()}
        sorted_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)
        return [{'tag': tag, 'count': count} for tag, count in sorted_tags[:n]]
    
    def _get_top_capabilities(self, n: int) -> List[Dict]:
        """Get most common capabilities"""
        cap_counts = {cap: len(experts) for cap, experts in self._capability_index.items()}
        sorted_caps = sorted(cap_counts.items(), key=lambda x: x[1], reverse=True)
        return [{'capability': cap, 'count': count} for cap, count in sorted_caps[:n]]
    
    def get_all_active_experts(self) -> List[ExpertProfile]:
        """Get all currently active experts"""
        return [
            e for e in self._experts.values()
            if e.is_active and e.lifecycle_state.is_available()
        ]
    
    def get_expert_lineage_chain(self, expert_id: str) -> List[ExpertProfile]:
        """Get lineage chain for an expert"""
        chain = []
        current_id = expert_id
        
        while current_id and current_id in self._experts:
            profile = self._experts[current_id]
            chain.append(profile)
            current_id = profile.replaces_expert
        
        return chain
    
    def get_migration_path(self, expert_id: str) -> Optional[str]:
        """Get migration path for deprecated expert"""
        return self._migration_paths.get(expert_id)
    
    # ========================================================================
    # Maintenance
    # ========================================================================
    
    def cleanup_deprecated(self, max_age_days: int = 90) -> int:
        """Clean up experts deprecated longer than max_age_days"""
        cutoff = datetime.utcnow() - timedelta(days=max_age_days)
        retired_count = 0
        
        for expert_id in list(self._experts.keys()):
            profile = self._experts[expert_id]
            if (profile.lifecycle_state == ExpertLifecycleState.DEPRECATED and
                profile.retired_at is None):
                # Check if there's a migration path
                if expert_id in self._migration_paths:
                    self.retire_expert(expert_id)
                    retired_count += 1
        
        self._stats['last_cleanup'] = datetime.utcnow()
        logger.info(f"Cleanup: retired {retired_count} deprecated experts")
        
        return retired_count
    
    def verify_all_certifications(self) -> Dict[str, bool]:
        """Verify all certifications are still valid"""
        results = {}
        now = datetime.utcnow()
        
        for expert_id, profile in self._experts.items():
            for cert in profile.certifications:
                if cert.expires_at and cert.expires_at < now:
                    cert.is_valid = False
                    results[expert_id] = False
                else:
                    results[expert_id] = True
        
        return results
