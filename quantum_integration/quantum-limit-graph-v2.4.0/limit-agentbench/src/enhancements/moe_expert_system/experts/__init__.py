#!/usr/bin/env python3
"""
MoE Expert System – Expert Module (Enhanced v2.4.0) with MOPD Support & Green Agent Hooks

This package provides the core experts used in the mixture‑of‑experts framework.
Each expert implements a specific optimization domain (energy, data, IoT, quantum, helium).
All experts now support Multi‑Objective Pareto Decision (MOPD) through a standardised interface.
Additionally, the base class now provides optional hooks for Explainable AI (XAI),
human‑in‑the‑loop approval, temporal safety checks, and resilience/chaos testing,
making it easier to integrate Green Agent enhancements consistently.

ENHANCEMENTS OVER v2.3.0:
1. Made `__init_subclass__` lenient (warns instead of raising) and sets default version/description.
2. Added optional async `get_metrics` method.
3. Improved `policy_probs` default to use `adaptive_cost` when available.
4. Added `explain_decision` method for simple XAI.
5. Added `request_approval` and `log_uncertainty` for human‑in‑the‑loop.
6. Added `check_invariants` for temporal safety verification.
7. Added `inject_fault` and `run_chaos_test` for resilience engineering.
8. Fixed `scalarise` to not mutate input solutions (returns scores separately).
9. Added `requires_human_approval` field to `MOPDProposal`.
10. Expanded `MOPDConfig` to include optional hooks for XAI and safety.
"""

import logging
import inspect
from typing import Dict, Type, Optional, Any, List, Callable, Awaitable, Union, Tuple
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import asyncio

__version__ = "2.4.0"

logger = logging.getLogger(__name__)

# ============================================================================
# MOPD Shared Configuration
# ============================================================================
@dataclass
class MOPDConfig:
    """
    Configuration for Multi‑Objective Pareto Decision (MOPD) across all experts.
    Experts can use this structure to initialise their own MOPD logic.
    """
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        'carbon_savings': 0.4,
        'helium_savings': 0.3,
        'cost': 0.2,
        'latency': 0.1,
    })
    pareto_grid_resolution: int = 5
    enable_cost_benefit: bool = True
    enable_predictive: bool = True
    enable_quantum: bool = True
    # Additional common parameters
    adaptive_cost: Optional[Any] = None          # Reference to central AdaptiveCostFunction
    pareto_gating: Optional[Any] = None          # Reference to central ParetoGating
    drift_detector: Optional[Any] = None         # Reference to central DriftDetector
    enable_xai: bool = True                     # Should expert generate explanations?
    enable_human_approval: bool = False         # Should critical decisions require approval?
    enable_temporal_safety: bool = True         # Should experts check temporal invariants?

# ============================================================================
# Standardised Proposal Result Type
# ============================================================================
@dataclass
class MOPDProposal:
    """
    Standard return type for `propose_async` method in MOPD‑aware experts.
    """
    recommendations: Dict[str, Any]               # single preferred action
    options: List[Dict[str, Any]]                 # list of trade‑off options (could be Pareto front)
    explanation: str                              # natural‑language description
    pareto_front: Optional[List[Dict[str, Any]]] = None  # full Pareto front if available
    requires_human_approval: bool = False         # NEW: flag for human‑in‑the‑loop

# ============================================================================
# Shared MOPD Utilities (can be used by any expert)
# ============================================================================
def is_dominated(solution_a: Dict[str, Any],
                 solution_b: Dict[str, Any],
                 objective_keys: List[str]) -> bool:
    """
    Return True if solution_b dominates solution_a.
    For minimization objectives (cost, latency), we negate the values.
    Assumes all objectives are defined in both solutions.
    """
    a_vec = []
    b_vec = []
    for key in objective_keys:
        if key in ['cost', 'latency']:
            a_vec.append(-solution_a.get(key, 0))
            b_vec.append(-solution_b.get(key, 0))
        else:
            a_vec.append(solution_a.get(key, 0))
            b_vec.append(solution_b.get(key, 0))
    return all(b >= a for a, b in zip(a_vec, b_vec)) and any(b > a for a, b in zip(a_vec, b_vec))

def filter_pareto_front(solutions: List[Dict[str, Any]],
                        objective_keys: List[str]) -> List[Dict[str, Any]]:
    """Return only non‑dominated solutions from the given list."""
    pareto = []
    for i, sol_i in enumerate(solutions):
        dominated = False
        for j, sol_j in enumerate(solutions):
            if i == j:
                continue
            if is_dominated(sol_i, sol_j, objective_keys):
                dominated = True
                break
        if not dominated:
            pareto.append(sol_i)
    return pareto

def scalarise(solutions: List[Dict[str, Any]],
              weights: Dict[str, float],
              objective_keys: List[str]) -> Tuple[List[Dict[str, Any]], List[float]]:
    """
    Compute scalarised scores for a list of solutions using weighted sum.
    Returns a tuple: (list_of_original_solutions, list_of_scores).
    The input solutions are NOT modified.
    """
    # Make shallow copies to avoid mutation
    sols = [sol.copy() for sol in solutions]

    # Normalise each objective across solutions (stored locally)
    norm_vals = {}
    for key in objective_keys:
        vals = [sol.get(key, 0) for sol in sols]
        min_val = min(vals)
        max_val = max(vals)
        range_val = max_val - min_val if max_val != min_val else 1
        norm_vals[key] = []
        for sol in sols:
            if key in ['cost', 'latency']:
                norm_val = (max_val - sol.get(key, 0)) / range_val
            else:
                norm_val = (sol.get(key, 0) - min_val) / range_val
            norm_vals[key].append(norm_val)

    scores = []
    for i in range(len(sols)):
        score = 0.0
        for key in objective_keys:
            weight = weights.get(key, 0.0)
            score += weight * norm_vals[key][i]
        scores.append(score)

    return sols, scores

# ============================================================================
# Base Expert Interface (Abstract Base Class) – Enhanced with MOPD and Green Hooks
# ============================================================================
class BaseExpert(ABC):
    """
    Abstract base for all MoE experts.
    All concrete experts must implement the methods defined here.
    MOPD‑aware methods are now part of the core interface, along with optional
    hooks for XAI, human‑in‑the‑loop, temporal safety, and resilience.

    ENHANCEMENTS v2.4.0:
    - __init_subclass__ lenient (warns if missing version/description, sets defaults).
    - Optional async get_metrics.
    - policy_probs default uses adaptive_cost if available.
    - explain_decision method for simple XAI.
    - request_approval and log_uncertainty for human‑in‑the‑loop.
    - check_invariants for temporal safety.
    - inject_fault and run_chaos_test for resilience.
    - MOPDProposal now includes requires_human_approval flag.
    """

    __expert_version__: str = "0.0.0"          # Override per expert
    __expert_description__: str = ""           # Override per expert

    def __init_subclass__(cls, **kwargs):
        """Set default version/description if missing, and warn."""
        super().__init_subclass__(**kwargs)
        if getattr(cls, '__expert_version__', '0.0.0') == '0.0.0':
            logger.warning(f"{cls.__name__} does not define __expert_version__. Setting to '0.0.0'.")
        if getattr(cls, '__expert_description__', '') == '':
            logger.warning(f"{cls.__name__} does not define __expert_description__. Setting to empty string.")
            # Optionally set a default description
            cls.__expert_description__ = "No description provided."

    def __init__(self,
                 storage: Optional[Any] = None,
                 message_queue: Optional[Any] = None,
                 adaptive_cost: Optional[Any] = None,
                 pareto_gating: Optional[Any] = None,
                 drift_detector: Optional[Any] = None,
                 metrics: Optional[Any] = None,
                 **kwargs):
        """
        Constructor accepts central Green Agent components.
        Subclasses should call super().__init__() and optionally set these attributes.
        """
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics
        # Bio-inspired core (if injected later, can be set via set_bio_core)
        self.bio_core = None
        self.token_manager = None
        self.gradient_manager = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        self.scheduler = None

    def set_bio_core(self, bio_core: Any):
        """Inject bio‑inspired core and extract managers."""
        self.bio_core = bio_core
        if bio_core:
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.compartment_manager = getattr(bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(bio_core, 'biomass_storage', None)
            self.harvester = getattr(bio_core, 'harvester', None)
            self.scheduler = getattr(bio_core, 'scheduler', None)

    # ===== Bio‑inspired helper hooks =====
    async def spend_atp(self, amount: float, consumer: str = "expert"):
        """Spend ATP tokens. Returns True if successful, False otherwise."""
        if self.token_manager:
            try:
                return await self.token_manager.spend(consumer, amount)
            except Exception as e:
                logger.debug(f"ATP spend failed: {e}")
        return False

    async def earn_atp(self, amount: float, source: str = "expert"):
        """Earn ATP tokens. Returns True if successful, False otherwise."""
        if self.token_manager:
            try:
                return await self.token_manager.earn(source, amount)
            except Exception as e:
                logger.debug(f"ATP earn failed: {e}")
        return False

    async def pump_gradient(self, field: str, delta: float, source: str = "expert"):
        """Pump a gradient field (e.g., 'trust', 'carbon', 'helium')."""
        if self.gradient_manager:
            try:
                return await self.gradient_manager.pump_field(field, delta, source=source)
            except Exception as e:
                logger.debug(f"Gradient pump failed: {e}")
        return False

    # ===== MOPD‑specific abstract methods =====
    @abstractmethod
    async def propose_async(self, context: dict) -> MOPDProposal:
        """
        Generate a recommendation based on the provided context.
        Subclasses should implement this async method.
        """
        pass

    @abstractmethod
    async def get_health_status(self) -> Dict[str, Any]:
        """
        Return health metrics of the expert (async).
        """
        pass

    @abstractmethod
    async def shutdown(self):
        """
        Gracefully shut down the expert and any background tasks (async).
        """
        pass

    @abstractmethod
    async def get_pareto_front(self, context: dict) -> List[Dict[str, Any]]:
        """
        Return a list of Pareto‑optimal solutions for the given context.
        """
        pass

    @abstractmethod
    def set_objective_weights(self, weights: Dict[str, float]) -> None:
        """
        Update the expert's objective weights for scalarisation.
        """
        pass

    @abstractmethod
    def get_objective_weights(self) -> Dict[str, float]:
        """
        Return the current objective weights.
        """
        pass

    @abstractmethod
    def get_mopd_config(self) -> MOPDConfig:
        """
        Return the MOPD configuration (thresholds, grid resolution, etc.).
        """
        pass

    # ===== Optional teacher policy for MTPD =====
    async def policy_probs(self, state: Dict) -> List[float]:
        """
        Return a probability distribution over strategies/actions.
        Default implementation uses a uniform distribution if no adaptive_cost,
        otherwise uses a simple scoring based on adaptive_cost for a generic set.
        Subclasses should override for context‑aware multi‑objective scoring.
        """
        if hasattr(self, 'supported_task_types') and self.supported_task_types:
            n = len(self.supported_task_types)
            if self.adaptive_cost is not None:
                # Compute a rough score for each action using adaptive cost
                scores = []
                for task_type in self.supported_task_types:
                    # Use a generic cost computation (subclasses may override)
                    cost = self.adaptive_cost.compute(
                        quality=0.5,
                        carbon_g=0.0,
                        latency_ms=0.0,
                        energy_joules=0.0,
                        health=True,
                        atp=0.5
                    )
                    scores.append(cost)
                if scores:
                    exp_scores = np.exp(scores - np.max(scores))
                    probs = exp_scores / np.sum(exp_scores)
                    return probs.tolist()
            return [1.0 / n] * n
        # Fallback
        return [0.5, 0.5]

    # ===== Optional XAI hook =====
    async def explain_decision(self, context: Dict, recommendation: MOPDProposal) -> str:
        """
        Generate a human‑readable explanation for a given recommendation.
        Default implementation returns the stored explanation if present,
        otherwise a generic message.
        """
        if recommendation and recommendation.explanation:
            return recommendation.explanation
        return "No explanation available."

    # ===== Optional human‑in‑the‑loop hooks =====
    async def request_approval(self, recommendation: MOPDProposal, context: Dict) -> bool:
        """
        Request human approval for a critical recommendation.
        Default implementation checks a configured flag (or returns False).
        Subclasses can override to integrate with an external approval system.
        """
        # If config indicates human approval is not required, return True (auto‑approve)
        mopd_cfg = self.get_mopd_config()
        if not getattr(mopd_cfg, 'enable_human_approval', False):
            return True
        # Otherwise, this is a placeholder; in a real system you'd wait for a response.
        logger.warning(f"Human approval requested for recommendation but no approval system set. Auto‑denying.")
        return False

    async def log_uncertainty(self, metric: str, value: float, context: Dict):
        """
        Log a metric indicating uncertainty (e.g., low confidence) for later active learning.
        Default implementation just logs; subclasses can store for querying.
        """
        logger.info(f"Uncertainty logged: metric={metric}, value={value}, context={context}")

    # ===== Optional temporal safety check =====
    async def check_invariants(self, state: Dict) -> List[str]:
        """
        Check temporal safety invariants against the current state.
        Returns a list of violation descriptions (empty if all ok).
        Subclasses should override with domain‑specific invariants.
        """
        return []

    # ===== Optional resilience / chaos testing hooks =====
    async def inject_fault(self, fault_type: str, **params):
        """
        Inject a fault (e.g., 'timeout', 'circuit_open') to test resilience.
        Default implementation does nothing; subclasses can override.
        """
        logger.warning(f"Fault injection '{fault_type}' not implemented for {self.__class__.__name__}.")

    async def run_chaos_test(self):
        """
        Run a series of fault injections and verify recovery.
        Default implementation returns a simple report.
        """
        logger.info(f"Running chaos test for {self.__class__.__name__}...")
        return {'status': 'not_implemented'}

    # ===== Optional lifecycle methods =====
    async def initialize(self):
        """Perform one‑time setup after instantiation."""
        pass

    async def get_capabilities(self) -> Dict[str, Any]:
        """
        Return the expert's capabilities for routing and gating (async).
        """
        health_status = await self.get_health_status()
        return {
            'name': self.__class__.__name__,
            'version': self.__expert_version__,
            'description': self.__expert_description__,
            'health_status': health_status,
            'mopd_weights': self.get_objective_weights(),
            'mopd_config': self.get_mopd_config(),
        }

    def get_capabilities_sync(self) -> Dict[str, Any]:
        """
        Synchronous version of get_capabilities (health status omitted or default).
        """
        return {
            'name': self.__class__.__name__,
            'version': self.__expert_version__,
            'description': self.__expert_description__,
            'mopd_weights': self.get_objective_weights(),
            'mopd_config': self.get_mopd_config(),
        }

    # ===== Optional async metrics (non‑abstract) =====
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Return basic metrics about the expert's operation.
        Subclasses should override with meaningful counters.
        Default returns empty dict.
        """
        return {}

    def get_version(self) -> str:
        """Return the expert's version."""
        return self.__expert_version__

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

# ============================================================================
# Registry (dynamic registration)
# ============================================================================
_EXPERT_REGISTRY: Dict[str, Type[BaseExpert]] = {}

def register_expert(name: str, expert_class: Type[BaseExpert]) -> None:
    """
    Register an expert class dynamically.

    Args:
        name: The unique name of the expert (e.g., 'EnergyExpert').
        expert_class: The class that implements BaseExpert.

    Raises:
        TypeError: If expert_class does not inherit from BaseExpert.
        ValueError: If the name is already registered.
    """
    if not issubclass(expert_class, BaseExpert):
        raise TypeError(f"{expert_class} must inherit from BaseExpert")
    if name in _EXPERT_REGISTRY:
        raise ValueError(f"Expert '{name}' is already registered.")
    # Check MOPD compliance (warn if methods are not overridden)
    for method in ['get_pareto_front', 'set_objective_weights', 'get_objective_weights', 'get_mopd_config', 'propose_async']:
        if not hasattr(expert_class, method) or getattr(expert_class, method) is BaseExpert.__dict__.get(method):
            logger.warning(f"Expert '{name}' does not implement MOPD method '{method}'. "
                           f"It may not fully support MOPD. Consider updating the expert.")
    _EXPERT_REGISTRY[name] = expert_class
    logger.info(f"Registered expert '{name}' (v{expert_class.__expert_version__})")

def get_expert(name: str) -> Type[BaseExpert]:
    """
    Retrieve an expert class by its registered name.
    """
    if name not in _EXPERT_REGISTRY:
        raise ValueError(f"Expert '{name}' is not registered.")
    return _EXPERT_REGISTRY[name]

def list_experts() -> List[str]:
    """Return a list of all registered expert names."""
    return list(_EXPERT_REGISTRY.keys())

def unregister_expert(name: str) -> None:
    """
    Unregister an expert.
    """
    if name not in _EXPERT_REGISTRY:
        raise ValueError(f"Expert '{name}' is not registered.")
    del _EXPERT_REGISTRY[name]
    logger.info(f"Unregistered expert '{name}'")

# ============================================================================
# Factory: instantiate an expert with bio_core injection and config
# ============================================================================
def create_expert(
    name: str,
    bio_core: Optional[Any] = None,
    config: Optional[Dict[str, Any]] = None,
    mopd_config: Optional[Union[Dict[str, Any], MOPDConfig]] = None,
    storage: Optional[Any] = None,
    message_queue: Optional[Any] = None,
    adaptive_cost: Optional[Any] = None,
    pareto_gating: Optional[Any] = None,
    drift_detector: Optional[Any] = None,
    metrics: Optional[Any] = None,
    **kwargs: Any,
) -> BaseExpert:
    """
    Create an instance of an expert, injecting central components and bio_core.
    """
    expert_class = get_expert(name)

    # Build argument dict from provided kwargs plus bio_core, config, mopd_config and central components
    init_kwargs = kwargs.copy()
    if bio_core is not None:
        init_kwargs['bio_core'] = bio_core
    if config is not None:
        init_kwargs['config'] = config
    if mopd_config is not None:
        if isinstance(mopd_config, dict):
            mopd_config = MOPDConfig(**mopd_config)
        init_kwargs['mopd_config'] = mopd_config
    # Central components are passed to base __init__ (if accepted)
    for key, val in [('storage', storage), ('message_queue', message_queue),
                     ('adaptive_cost', adaptive_cost), ('pareto_gating', pareto_gating),
                     ('drift_detector', drift_detector), ('metrics', metrics)]:
        if val is not None:
            init_kwargs[key] = val

    # Use inspect to see which parameters are accepted
    sig = inspect.signature(expert_class.__init__)
    params = sig.parameters
    filtered_kwargs = {}
    for k, v in init_kwargs.items():
        if k in params:
            filtered_kwargs[k] = v
        else:
            logger.debug(f"Argument '{k}' not accepted by {name}.__init__; ignoring.")

    # If constructor has **kwargs, we can pass all
    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
        filtered_kwargs = init_kwargs

    # Instantiate
    expert = expert_class(**filtered_kwargs)
    # Inject bio_core if the expert has a set_bio_core method (in case constructor didn't accept it)
    if bio_core is not None and hasattr(expert, 'set_bio_core'):
        expert.set_bio_core(bio_core)
    return expert

# ============================================================================
# Helper: shutdown multiple experts with MOPD cleanup
# ============================================================================
async def shutdown_all_experts(experts: List[BaseExpert]) -> None:
    """
    Gracefully shut down a list of experts.
    Also flushes metrics and closes persistence if defined.
    """
    for expert in experts:
        try:
            if hasattr(expert, 'flush_metrics') and callable(expert.flush_metrics):
                await expert.flush_metrics()
            if hasattr(expert, 'close_persistence') and callable(expert.close_persistence):
                await expert.close_persistence()
            await expert.shutdown()
        except Exception as e:
            logger.error(f"Error shutting down expert {expert.__class__.__name__}: {e}")

# ============================================================================
# Helper: retrieve capabilities from multiple experts (including MOPD info)
# ============================================================================
async def get_experts_capabilities(experts: List[BaseExpert]) -> List[Dict[str, Any]]:
    """
    Return a list of capabilities for all experts, including MOPD‑specific information.
    """
    caps = []
    for expert in experts:
        base = await expert.get_capabilities()
        caps.append(base)
    return caps

# ============================================================================
# Backward Compatibility: eager imports for direct access
# ============================================================================
_imported_experts: Dict[str, Type[BaseExpert]] = {}

try:
    from .energy_expert import EnergyExpert
    register_expert('EnergyExpert', EnergyExpert)
    _imported_experts['EnergyExpert'] = EnergyExpert
except ImportError as e:
    logger.warning(f"EnergyExpert could not be imported: {e}")
    EnergyExpert = None

try:
    from .data_expert import DataExpert
    register_expert('DataExpert', DataExpert)
    _imported_experts['DataExpert'] = DataExpert
except ImportError as e:
    logger.warning(f"DataExpert could not be imported: {e}")
    DataExpert = None

try:
    from .iot_expert import IoTExpert
    register_expert('IoTExpert', IoTExpert)
    _imported_experts['IoTExpert'] = IoTExpert
except ImportError as e:
    logger.warning(f"IoTExpert could not be imported: {e}")
    IoTExpert = None

try:
    from .quantum_expert import QuantumExpert
    register_expert('QuantumExpert', QuantumExpert)
    _imported_experts['QuantumExpert'] = QuantumExpert
except ImportError as e:
    logger.warning(f"QuantumExpert could not be imported: {e}")
    QuantumExpert = None

try:
    from .helium_expert import HeliumExpert
    register_expert('HeliumExpert', HeliumExpert)
    _imported_experts['HeliumExpert'] = HeliumExpert
except ImportError as e:
    logger.warning(f"HeliumExpert could not be imported: {e}")
    HeliumExpert = None

# ============================================================================
# __all__ – control what is exported with 'from ... import *'
# ============================================================================
__all__ = [
    'BaseExpert',
    'get_expert',
    'create_expert',
    'list_experts',
    'register_expert',
    'unregister_expert',
    'shutdown_all_experts',
    'get_experts_capabilities',
    'MOPDConfig',
    'MOPDProposal',
    'is_dominated',
    'filter_pareto_front',
    'scalarise',
    '__version__',
]

# Add successfully imported experts to __all__
for expert_name, expert_class in _imported_experts.items():
    globals()[expert_name] = expert_class
    __all__.append(expert_name)
