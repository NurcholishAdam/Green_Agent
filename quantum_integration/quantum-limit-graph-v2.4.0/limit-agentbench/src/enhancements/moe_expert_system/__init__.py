# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/__init__.py
# Enhanced with complete bio-inspired integration - Unified Metabolic Ecosystem v4.0.0

"""
Green Agent MoE Expert System v4.0.0 - Unified Metabolic Ecosystem

Complete integration with bio-inspired modules providing:
- Eco-ATP currency system for unified resource accounting
- Proton gradient fields for distributed potential accumulation
- ATP synthase scheduling for energy-driven task dispatching
- Chromatophore compartments for modular expert isolation
- Biomass storage for deferred computation queuing
- Photosynthetic harvesting for environmental opportunity detection

This module serves as the central nervous system connecting:
- Expert Registry (Genome Repository)
- Gating Network (Allosteric Enzyme System)
- Expert Router (Signal Transduction Cascade)
- All specialized experts (Metabolic Organs)
- Monitoring system (Metabolic Observatory)
"""

import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# ============================================================================
# Bio-Inspired Module Availability Check
# ============================================================================

BIO_INSPIRED_AVAILABLE = False
try:
    from enhancements.bio_inspired import (
        EcoATPTokenManager,
        DynamicExchangeRate,
        GradientFieldManager,
        ATPSynthaseScheduler,
        CompartmentManager,
        BiomassStorage,
        PhotosyntheticHarvester,
        BioInspiredGreenCore,
        BioInspiredMoEIntegrator
    )
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired modules available for MoE Expert System integration")
except ImportError as e:
    logger.info(f"Bio-inspired modules not available: {str(e)} - using standard MoE system")

# ============================================================================
# Core MoE Components
# ============================================================================

from .expert_registry import (
    ExpertRegistry,
    ExpertProfile,
    ExpertDomain,
    ExpertLifecycleState,
    ExpertVersion,
    HardwareProfile,
    HealthMetrics,
    ExpertCertification,
    CertificationLevel,
    FitnessScore
)

from .gating_network import (
    MoEGatingNetwork,
    GatingContext,
    EnhancedSparseMoEGate
)

from .expert_router import (
    ExpertRouter,
    RoutingMetrics,
    ExpertCircuitBreaker,
    CircuitBreakerState,
    SignalTransductionEngine,
    AllostericRegulationSystem,
    MetabolicPathwayRouter
)

# ============================================================================
# Specialized Experts (Metabolic Organs)
# ============================================================================

from .experts.energy_expert import (
    EnergyExpert,
    EnergySource,
    PowerState,
    CoolingMethod,
    RenewableProfile,
    ThermalProfile
)

from .experts.data_expert import (
    DataExpert,
    DataTier,
    DataQuality,
    DataQualityMetrics,
    DataLineage,
    DataStream,
    StreamingMode,
    PipelineStatus
)

from .experts.iot_expert import (
    IoTExpert,
    DeviceType,
    ConnectionType,
    EnergySource as IoTEnergySource,
    ProcessingMode,
    MeshRole,
    EdgeDevice,
    MeshNetwork
)

# ============================================================================
# Optional Experts
# ============================================================================

try:
    from .experts.quantum_expert import QuantumExpert
    QUANTUM_AVAILABLE = True
except ImportError:
    QUANTUM_AVAILABLE = False
    logger.info("Quantum Expert not available")

try:
    from .experts.helium_expert import HeliumExpert
    HELIUM_AVAILABLE = True
except ImportError:
    HELIUM_AVAILABLE = False
    logger.info("Helium Expert not available")

# ============================================================================
# Advanced Modules
# ============================================================================

try:
    from .advanced.self_evolving_gates import (
        EnhancedSelfEvolvingGate,
        SelfEvolvingGate
    )
    EVOLVING_GATES_AVAILABLE = True
except ImportError:
    EVOLVING_GATES_AVAILABLE = False

try:
    from .advanced.federated_experts import (
        EnhancedFederatedOrchestrator,
        FederatedExpert
    )
    FEDERATED_AVAILABLE = True
except ImportError:
    FEDERATED_AVAILABLE = False

try:
    from .advanced.cross_region_federation import (
        CrossRegionFederationOptimizer,
        Region,
        SyncMode,
        AggregationTier
    )
    CROSS_REGION_AVAILABLE = True
except ImportError:
    CROSS_REGION_AVAILABLE = False

# ============================================================================
# Integration Modules
# ============================================================================

from .integration.layer_integrator import (
    EnhancedLayerIntegrator,
    LayerIntegrator,
    LayerInfo,
    LayerStatus,
    IntegrationMode,
    CircuitState
)

from .integration.enhanced_work_integration import (
    EnhancedWorkIntegrator,
    EnhancedWorkContext,
    WorkState,
    WorkPriority,
    WorkSLA,
    SLALevel
)

from .integration.quantum_limit_integration import (
    QuantumLimitGraphIntegrator,
    QuantumBackend,
    QuantumAlgorithm,
    QuantumErrorMitigation,
    QuantumResource,
    QuantumCircuitJob,
    AdaptiveBoundary,
    QuantumNode
)

# ============================================================================
# Monitoring Module
# ============================================================================

from .monitoring.expert_metrics import (
    ExpertMetricsCollector,
    MetricSeverity,
    MetricType,
    AnomalyType,
    SLOStatus,
    MetricThreshold,
    ServiceLevelObjective,
    AnomalyEvent,
    CostAttribution
)

# ============================================================================
# Unified Metabolic Ecosystem - Main Entry Point
# ============================================================================

class UnifiedMetabolicEcosystem:
    """
    Unified Metabolic Ecosystem v4.0.0
    
    Complete integration of MoE Expert System with Bio-Inspired Architecture.
    
    This class wires together:
    - Expert Registry (Genome Repository)
    - Gating Network (Allosteric Enzyme)
    - Expert Router (Signal Transduction Cascade)
    - All Metabolic Experts (Energy, Data, IoT, Quantum, Helium)
    - Bio-Inspired Core (Eco-ATP, Gradients, ATP Synthase, Compartments, Biomass, Harvester)
    - Advanced Modules (Self-Evolving Gates, Federated Learning, Cross-Region)
    - Integration Layer (12-Layer Bridge, Work Orchestrator, Quantum Limits)
    - Monitoring (Metabolic Observatory)
    """
    
    def __init__(
        self,
        enable_quantum: bool = False,
        enable_helium: bool = False,
        enable_bio_inspired: bool = True,
        enable_evolving_gates: bool = True,
        enable_federated: bool = False,
        enable_cross_region: bool = False,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the unified metabolic ecosystem.
        
        Args:
            enable_quantum: Enable Quantum Expert
            enable_helium: Enable Helium Expert
            enable_bio_inspired: Enable bio-inspired architecture
            enable_evolving_gates: Enable self-evolving gates
            enable_federated: Enable federated learning
            enable_cross_region: Enable cross-region federation
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.initialization_status: Dict[str, bool] = {}
        
        logger.info("=" * 70)
        logger.info("Initializing Unified Metabolic Ecosystem v4.0.0")
        logger.info("=" * 70)
        
        # ====================================================================
        # Step 1: Initialize Bio-Inspired Core (if available)
        # ====================================================================
        self.bio_core = None
        self.bio_available = False
        
        if enable_bio_inspired and BIO_INSPIRED_AVAILABLE:
            try:
                self.bio_core = BioInspiredGreenCore()
                self.bio_available = True
                self.initialization_status['bio_inspired_core'] = True
                logger.info("[BIO] Bio-Inspired Core initialized successfully")
            except Exception as e:
                logger.error(f"[BIO] Failed to initialize Bio-Inspired Core: {str(e)}")
                self.initialization_status['bio_inspired_core'] = False
        else:
            logger.info("[BIO] Bio-inspired architecture disabled or not available")
            self.initialization_status['bio_inspired_core'] = False
        
        # ====================================================================
        # Step 2: Initialize Expert Registry (Genome Repository)
        # ====================================================================
        try:
            self.registry = ExpertRegistry(
                enable_genetics=self.bio_available,
                enable_evolution=self.bio_available,
                enable_ecosystem=self.bio_available
            )
            
            # Inject bio-core if available
            if self.bio_available:
                self.registry.inject_bio_core(self.bio_core)
            
            self.initialization_status['expert_registry'] = True
            logger.info("[REGISTRY] Expert Registry (Genome Repository) initialized")
        except Exception as e:
            logger.error(f"[REGISTRY] Failed to initialize Expert Registry: {str(e)}")
            self.initialization_status['expert_registry'] = False
            raise
        
        # ====================================================================
        # Step 3: Initialize Gating Network (Allosteric Enzyme)
        # ====================================================================
        try:
            self.gating_network = MoEGatingNetwork(
                num_experts=5 + (1 if enable_quantum else 0) + (1 if enable_helium else 0),
                enable_bio_integration=self.bio_available
            )
            
            # Inject bio-core if available
            if self.bio_available:
                self.gating_network.inject_bio_core(self.bio_core)
            
            self.initialization_status['gating_network'] = True
            logger.info("[GATING] Gating Network (Allosteric Enzyme) initialized")
        except Exception as e:
            logger.error(f"[GATING] Failed to initialize Gating Network: {str(e)}")
            self.initialization_status['gating_network'] = False
            raise
        
        # ====================================================================
        # Step 4: Initialize Expert Router (Signal Transduction Cascade)
        # ====================================================================
        try:
            self.router = ExpertRouter(
                enable_quantum=enable_quantum,
                enable_signal_transduction=self.bio_available,
                enable_allosteric=self.bio_available,
                enable_metabolic_pathways=self.bio_available
            )
            
            # Wire gating network to router
            self.router.gating_network = self.gating_network
            
            # Inject bio-core if available
            if self.bio_available:
                self.router.inject_bio_core(self.bio_core)
            
            self.initialization_status['expert_router'] = True
            logger.info("[ROUTER] Expert Router (Signal Transduction) initialized")
        except Exception as e:
            logger.error(f"[ROUTER] Failed to initialize Expert Router: {str(e)}")
            self.initialization_status['expert_router'] = False
            raise
        
        # ====================================================================
        # Step 5: Initialize Metabolic Experts
        # ====================================================================
        self.experts: Dict[str, Any] = {}
        
        # Energy Expert (Primary Producer)
        try:
            self.experts['energy'] = EnergyExpert(
                enable_bio_integration=self.bio_available
            )
            if self.bio_available:
                self.experts['energy'].inject_bio_core(self.bio_core)
            logger.info("[EXPERT] Energy Expert (Primary Producer) initialized")
        except Exception as e:
            logger.error(f"[EXPERT] Failed to initialize Energy Expert: {str(e)}")
        
        # Data Expert (Primary Consumer)
        try:
            self.experts['data'] = DataExpert(
                enable_bio_integration=self.bio_available
            )
            if self.bio_available:
                self.experts['data'].inject_bio_core(self.bio_core)
            logger.info("[EXPERT] Data Expert (Primary Consumer) initialized")
        except Exception as e:
            logger.error(f"[EXPERT] Failed to initialize Data Expert: {str(e)}")
        
        # IoT Expert (Decomposer)
        try:
            self.experts['iot'] = IoTExpert(
                enable_bio_integration=self.bio_available
            )
            if self.bio_available:
                self.experts['iot'].inject_bio_core(self.bio_core)
            logger.info("[EXPERT] IoT Expert (Decomposer) initialized")
        except Exception as e:
            logger.error(f"[EXPERT] Failed to initialize IoT Expert: {str(e)}")
        
        # Quantum Expert (Catalyst) - Optional
        if enable_quantum and QUANTUM_AVAILABLE:
            try:
                self.experts['quantum'] = QuantumExpert()
                logger.info("[EXPERT] Quantum Expert (Catalyst) initialized")
            except Exception as e:
                logger.error(f"[EXPERT] Failed to initialize Quantum Expert: {str(e)}")
        
        # Helium Expert (Homeostatic Regulator) - Optional
        if enable_helium and HELIUM_AVAILABLE:
            try:
                self.experts['helium'] = HeliumExpert()
                logger.info("[EXPERT] Helium Expert (Homeostatic Regulator) initialized")
            except Exception as e:
                logger.error(f"[EXPERT] Failed to initialize Helium Expert: {str(e)}")
        
        # Register all experts with registry
        for expert_id, expert in self.experts.items():
            try:
                if hasattr(expert, 'profile'):
                    self.registry.register_expert(expert.profile, validate=False, auto_certify=True)
                    logger.debug(f"[REGISTRY] Registered {expert_id} expert")
            except Exception as e:
                logger.warning(f"[REGISTRY] Failed to register {expert_id}: {str(e)}")
        
        self.initialization_status['experts'] = len(self.experts) > 0
        logger.info(f"[EXPERTS] {len(self.experts)} metabolic experts initialized")
        
        # ====================================================================
        # Step 6: Initialize Expert Index Mapping in Router
        # ====================================================================
        for idx, (expert_id, expert) in enumerate(self.experts.items()):
            self.router.expert_index_map[idx] = expert_id
            self.router.experts[expert_id] = expert
            self.router.circuit_breakers[expert_id] = ExpertCircuitBreaker(expert_id=expert_id)
        
        # Update gating network index map
        for idx, expert_id in self.router.expert_index_map.items():
            self.gating_network.expert_index_map[idx] = expert_id
        
        # ====================================================================
        # Step 7: Initialize Advanced Modules (Optional)
        # ====================================================================
        
        # Self-Evolving Gates
        self.evolving_gates = None
        if enable_evolving_gates and EVOLVING_GATES_AVAILABLE:
            try:
                self.evolving_gates = EnhancedSelfEvolvingGate(
                    input_dim=GatingContext().feature_dim,
                    num_experts=len(self.experts),
                    enable_bio_integration=self.bio_available
                )
                if self.bio_available:
                    self.evolving_gates.inject_bio_core(self.bio_core)
                self.initialization_status['evolving_gates'] = True
                logger.info("[EVOLVE] Self-Evolving Gates initialized")
            except Exception as e:
                logger.error(f"[EVOLVE] Failed to initialize Self-Evolving Gates: {str(e)}")
                self.initialization_status['evolving_gates'] = False
        
        # Federated Learning
        self.federated = None
        if enable_federated and FEDERATED_AVAILABLE:
            try:
                self.federated = EnhancedFederatedOrchestrator(
                    enable_bio_integration=self.bio_available
                )
                if self.bio_available:
                    self.federated.inject_bio_core(self.bio_core)
                self.initialization_status['federated'] = True
                logger.info("[FEDERATED] Federated Learning initialized")
            except Exception as e:
                logger.error(f"[FEDERATED] Failed to initialize Federated Learning: {str(e)}")
                self.initialization_status['federated'] = False
        
        # Cross-Region Federation
        self.cross_region = None
        if enable_cross_region and CROSS_REGION_AVAILABLE:
            try:
                self.cross_region = CrossRegionFederationOptimizer(
                    enable_bio_integration=self.bio_available
                )
                if self.bio_available:
                    self.cross_region.inject_bio_core(self.bio_core)
                self.initialization_status['cross_region'] = True
                logger.info("[CROSS-REGION] Cross-Region Federation initialized")
            except Exception as e:
                logger.error(f"[CROSS-REGION] Failed to initialize Cross-Region Federation: {str(e)}")
                self.initialization_status['cross_region'] = False
        
        # ====================================================================
        # Step 8: Initialize Integration Layer
        # ====================================================================
        
        # Layer Integrator (12-Layer Bridge)
        try:
            self.layer_integrator = EnhancedLayerIntegrator(
                enable_bio_integration=self.bio_available
            )
            if self.bio_available:
                self.layer_integrator.inject_bio_core(self.bio_core)
            self.initialization_status['layer_integrator'] = True
            logger.info("[LAYER] Layer Integrator (Neural Bridge) initialized")
        except Exception as e:
            logger.error(f"[LAYER] Failed to initialize Layer Integrator: {str(e)}")
            self.initialization_status['layer_integrator'] = False
        
        # Enhanced Work Integrator (Metabolic Work Orchestrator)
        try:
            self.work_integrator = EnhancedWorkIntegrator(
                expert_router=self.router,
                enable_bio_integration=self.bio_available
            )
            if self.bio_available:
                self.work_integrator.inject_bio_core(self.bio_core)
            self.initialization_status['work_integrator'] = True
            logger.info("[WORK] Work Integrator (Metabolic Orchestrator) initialized")
        except Exception as e:
            logger.error(f"[WORK] Failed to initialize Work Integrator: {str(e)}")
            self.initialization_status['work_integrator'] = False
        
        # Quantum Limit Integrator
        try:
            self.quantum_limits = QuantumLimitGraphIntegrator(
                enable_bio_integration=self.bio_available
            )
            if self.bio_available:
                self.quantum_limits.inject_bio_core(self.bio_core)
            self.initialization_status['quantum_limits'] = True
            logger.info("[QUANTUM] Quantum Limit Integrator initialized")
        except Exception as e:
            logger.error(f"[QUANTUM] Failed to initialize Quantum Limit Integrator: {str(e)}")
            self.initialization_status['quantum_limits'] = False
        
        # ====================================================================
        # Step 9: Initialize Monitoring (Metabolic Observatory)
        # ====================================================================
        try:
            self.metrics = ExpertMetricsCollector(
                enable_bio_integration=self.bio_available
            )
            if self.bio_available:
                self.metrics.inject_bio_core(self.bio_core)
            self.initialization_status['metrics'] = True
            logger.info("[METRICS] Expert Metrics (Metabolic Observatory) initialized")
        except Exception as e:
            logger.error(f"[METRICS] Failed to initialize Expert Metrics: {str(e)}")
            self.initialization_status['metrics'] = False
        
        # ====================================================================
        # Step 10: Wire Router Metrics
        # ====================================================================
        if hasattr(self.router, 'metrics_collector'):
            self.router.metrics_collector = self.metrics
        
        # ====================================================================
        # Final Status
        # ====================================================================
        logger.info("=" * 70)
        logger.info("Unified Metabolic Ecosystem Initialization Complete")
        logger.info(f"  Bio-Inspired: {self.bio_available}")
        logger.info(f"  Experts: {len(self.experts)}")
        logger.info(f"  Status: {sum(self.initialization_status.values())}/{len(self.initialization_status)} components")
        logger.info("=" * 70)
    
    # ========================================================================
    # Public API Methods
    # ========================================================================
    
    def get_ecosystem_status(self) -> Dict[str, Any]:
        """Get comprehensive ecosystem status"""
        status = {
            'ecosystem_version': '4.0.0',
            'bio_inspired_available': self.bio_available,
            'initialization_status': self.initialization_status,
            'expert_count': len(self.experts),
            'expert_types': list(self.experts.keys())
        }
        
        # Registry stats
        if hasattr(self, 'registry'):
            status['registry'] = self.registry.get_registry_stats()
        
        # Router stats
        if hasattr(self, 'router'):
            status['router'] = self.router.get_routing_stats()
        
        # Gating stats
        if hasattr(self, 'gating_network'):
            status['gating'] = self.gating_network.get_comprehensive_stats()
        
        # Bio-inspired stats
        if self.bio_available and self.bio_core:
            status['bio_system'] = self.bio_core.get_system_status()
        
        # Metrics stats
        if hasattr(self, 'metrics'):
            status['metrics'] = self.metrics.get_metrics_summary()
        
        return status
    
    def process_task(self, task: Dict[str, Any], pipeline_type: str = 'standard') -> Dict[str, Any]:
        """Process a task through the unified metabolic ecosystem"""
        if hasattr(self, 'work_integrator'):
            return self.work_integrator.process_work(task, pipeline_type)
        elif hasattr(self, 'router'):
            return self.router.route_and_execute(
                workload_profile=task,
                meta_cognitive_state={},
                dual_axis_context={}
            )
        else:
            return {'success': False, 'error': 'No work processor available'}
    
    def get_expert(self, expert_type: str) -> Optional[Any]:
        """Get expert by type"""
        return self.experts.get(expert_type)
    
    def register_expert(self, expert_type: str, expert_instance: Any):
        """Register a new expert dynamically"""
        self.experts[expert_type] = expert_instance
        idx = len(self.router.expert_index_map)
        self.router.expert_index_map[idx] = expert_type
        self.router.experts[expert_type] = expert_instance
        self.router.circuit_breakers[expert_type] = ExpertCircuitBreaker(expert_id=expert_type)
        self.gating_network.expert_index_map[idx] = expert_type
        
        if hasattr(expert_instance, 'profile'):
            self.registry.register_expert(expert_instance.profile, validate=False)
        
        logger.info(f"Dynamic expert registered: {expert_type}")
    
    def inject_external_module(self, module_name: str, module_instance: Any):
        """Inject external module into the ecosystem"""
        if module_name == 'token_manager':
            for expert in self.experts.values():
                if hasattr(expert, 'token_manager'):
                    expert.token_manager = module_instance
            if hasattr(self, 'router') and hasattr(self.router, 'token_manager'):
                self.router.token_manager = module_instance
        elif module_name == 'gradient_manager':
            for expert in self.experts.values():
                if hasattr(expert, 'gradient_manager'):
                    expert.gradient_manager = module_instance
        elif module_name == 'compartment_manager':
            for expert in self.experts.values():
                if hasattr(expert, 'compartment_manager'):
                    expert.compartment_manager = module_instance
        logger.info(f"External module injected: {module_name}")
    
    def shutdown(self):
        """Graceful shutdown of the ecosystem"""
        logger.info("Shutting down Unified Metabolic Ecosystem...")
        # Cleanup would go here
        logger.info("Ecosystem shutdown complete")


# ============================================================================
# Convenience Functions
# ============================================================================

def create_metabolic_ecosystem(
    enable_quantum: bool = False,
    enable_helium: bool = False,
    enable_bio: bool = True,
    enable_evolving: bool = True,
    enable_federated: bool = False,
    enable_cross_region: bool = False
) -> UnifiedMetabolicEcosystem:
    """
    Create a unified metabolic ecosystem with specified features.
    
    Args:
        enable_quantum: Enable Quantum Expert
        enable_helium: Enable Helium Expert
        enable_bio: Enable bio-inspired architecture
        enable_evolving: Enable self-evolving gates
        enable_federated: Enable federated learning
        enable_cross_region: Enable cross-region federation
        
    Returns:
        Configured UnifiedMetabolicEcosystem instance
    """
    return UnifiedMetabolicEcosystem(
        enable_quantum=enable_quantum,
        enable_helium=enable_helium,
        enable_bio_inspired=enable_bio,
        enable_evolving_gates=enable_evolving,
        enable_federated=enable_federated,
        enable_cross_region=enable_cross_region
    )


def create_minimal_ecosystem() -> UnifiedMetabolicEcosystem:
    """Create minimal ecosystem with core experts only"""
    return UnifiedMetabolicEcosystem(
        enable_quantum=False,
        enable_helium=False,
        enable_bio_inspired=False,
        enable_evolving_gates=False,
        enable_federated=False,
        enable_cross_region=False
    )


def create_full_ecosystem() -> UnifiedMetabolicEcosystem:
    """Create full ecosystem with all features enabled"""
    return UnifiedMetabolicEcosystem(
        enable_quantum=QUANTUM_AVAILABLE,
        enable_helium=HELIUM_AVAILABLE,
        enable_bio_inspired=BIO_INSPIRED_AVAILABLE,
        enable_evolving_gates=EVOLVING_GATES_AVAILABLE,
        enable_federated=FEDERATED_AVAILABLE,
        enable_cross_region=CROSS_REGION_AVAILABLE
    )


# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    # Ecosystem
    'UnifiedMetabolicEcosystem',
    'create_metabolic_ecosystem',
    'create_minimal_ecosystem',
    'create_full_ecosystem',
    
    # Core Components
    'ExpertRegistry',
    'ExpertProfile',
    'ExpertDomain',
    'ExpertLifecycleState',
    'ExpertVersion',
    'HardwareProfile',
    'HealthMetrics',
    'ExpertCertification',
    'CertificationLevel',
    'FitnessScore',
    
    # Gating
    'MoEGatingNetwork',
    'GatingContext',
    'EnhancedSparseMoEGate',
    
    # Router
    'ExpertRouter',
    'RoutingMetrics',
    'ExpertCircuitBreaker',
    'CircuitBreakerState',
    'SignalTransductionEngine',
    'AllostericRegulationSystem',
    'MetabolicPathwayRouter',
    
    # Experts
    'EnergyExpert',
    'EnergySource',
    'PowerState',
    'CoolingMethod',
    'RenewableProfile',
    'ThermalProfile',
    'DataExpert',
    'DataTier',
    'DataQuality',
    'DataQualityMetrics',
    'DataLineage',
    'DataStream',
    'StreamingMode',
    'PipelineStatus',
    'IoTExpert',
    'DeviceType',
    'ConnectionType',
    'ProcessingMode',
    'MeshRole',
    'EdgeDevice',
    'MeshNetwork',
    
    # Integration
    'EnhancedLayerIntegrator',
    'LayerIntegrator',
    'LayerInfo',
    'LayerStatus',
    'IntegrationMode',
    'CircuitState',
    'EnhancedWorkIntegrator',
    'EnhancedWorkContext',
    'WorkState',
    'WorkPriority',
    'WorkSLA',
    'SLALevel',
    'QuantumLimitGraphIntegrator',
    'QuantumBackend',
    'QuantumAlgorithm',
    'QuantumErrorMitigation',
    'QuantumResource',
    'QuantumCircuitJob',
    'AdaptiveBoundary',
    'QuantumNode',
    
    # Monitoring
    'ExpertMetricsCollector',
    'MetricSeverity',
    'MetricType',
    'AnomalyType',
    'SLOStatus',
    'MetricThreshold',
    'ServiceLevelObjective',
    'AnomalyEvent',
    'CostAttribution',
    
    # Advanced (conditionally available)
    'EnhancedSelfEvolvingGate',
    'SelfEvolvingGate',
    'EnhancedFederatedOrchestrator',
    'FederatedExpert',
    'CrossRegionFederationOptimizer',
    'Region',
    'SyncMode',
    'AggregationTier',
    
    # Status
    'BIO_INSPIRED_AVAILABLE',
    'QUANTUM_AVAILABLE',
    'HELIUM_AVAILABLE',
    'EVOLVING_GATES_AVAILABLE',
    'FEDERATED_AVAILABLE',
    'CROSS_REGION_AVAILABLE'
]


# ============================================================================
# Module Version
# ============================================================================

__version__ = "4.0.0"
__author__ = "Green Agent Team"
__description__ = "Unified Metabolic Ecosystem - Bio-Inspired MoE Expert System"
