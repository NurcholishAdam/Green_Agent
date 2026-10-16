"""
Green Agent enhancements package.

All ten enhancements live here as focused modules. Consumers should import
only the specific modules they need; heavy optional dependencies (torch,
pynvml, web3, etc.) are handled by the consumers, not by these modules.
"""

from .common_state import (
    STATE_VECTOR_FIELDS,
    STATE_VECTOR_DIM,
    state_vector_from_metrics,
    PROFILER_SCHEMA_VERSION,
    ACCELERATOR_SCHEMA_VERSION,
    PROFILER_SCHEMA_VERSION as SCHEMA_VERSION,
)

from .precision import (
    PrecisionLevel,
    PRECISION_COST,
    PrecisionController,
    HardwareAwareAdapter,
)

from .carbon import (
    CarbonMarketClient,
    RECRecord,
    RECInventory,
    CarbonIntensityManager,
    ICarbonManager,
)

from .chaos import (
    ChaosConfig,
    ChaosEngineer,
)

from .resilience import (
    CircuitBreakerState,
    CircuitBreaker,
    GlobalCircuitBreaker,
)

from .stl import (
    STLOperator,
    STLFormula,
    TemporalLogicMonitor,
    SafetyShield,
)

from .causal import (
    CausalTransition,
    CausalCounterfactualEstimator,
)

from .federated import (
    SimpleRDPAccountant,
    FederatedUpdate,
    FederatedAggregator,
)

from .multi_agent import (
    AgentRole,
    AgentBid,
    EmergentRoleRegistry,
    MultiAgentCoordinator,
)

from .xai import (
    Explanation,
    DecisionExplainer,
)

from .quantum import (
    QuantumInspiredTeacher,
    DistillationEnsemble,
)

from .hitl import (
    HITLRequest,
    UncertaintyEstimator,
    HumanInTheLoopGate,
    ActiveLearningSampler,
)

__all__ = [
    "STATE_VECTOR_FIELDS",
    "STATE_VECTOR_DIM",
    "state_vector_from_metrics",
    "PROFILER_SCHEMA_VERSION",
    "ACCELERATOR_SCHEMA_VERSION",
    "SCHEMA_VERSION",
    "PrecisionLevel",
    "PRECISION_COST",
    "PrecisionController",
    "HardwareAwareAdapter",
    "CarbonMarketClient",
    "RECRecord",
    "RECInventory",
    "CarbonIntensityManager",
    "ICarbonManager",
    "ChaosConfig",
    "ChaosEngineer",
    "CircuitBreakerState",
    "CircuitBreaker",
    "GlobalCircuitBreaker",
    "STLOperator",
    "STLFormula",
    "TemporalLogicMonitor",
    "SafetyShield",
    "CausalTransition",
    "CausalCounterfactualEstimator",
    "SimpleRDPAccountant",
    "FederatedUpdate",
    "FederatedAggregator",
    "AgentRole",
    "AgentBid",
    "EmergentRoleRegistry",
    "MultiAgentCoordinator",
    "Explanation",
    "DecisionExplainer",
    "QuantumInspiredTeacher",
    "DistillationEnsemble",
    "HITLRequest",
    "UncertaintyEstimator",
    "HumanInTheLoopGate",
    "ActiveLearningSampler",
]
