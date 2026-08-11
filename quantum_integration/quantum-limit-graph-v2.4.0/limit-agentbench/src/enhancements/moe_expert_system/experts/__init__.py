"""
MoE Expert System – Expert Module (Enhanced v2.2.0) with MOPD Support

This package provides the core experts used in the mixture‑of‑experts framework.
Each expert implements a specific optimization domain (energy, data, IoT, quantum, helium).
All experts now support Multi‑Objective Pareto Decision (MOPD) through a standardised interface.

Usage:
    from enhancements.moe_expert_system.experts import get_expert, create_expert, BaseExpert

    # Get the EnergyExpert class directly
    EnergyExpert = get_expert('EnergyExpert')

    # Or instantiate with configuration, bio_core, and MOPD config
    expert = create_expert('EnergyExpert', bio_core=my_core, config={...}, mopd_config={...})

    # Alternatively, import directly:
    from enhancements.moe_expert_system.experts import EnergyExpert

Available experts:
    - EnergyExpert    : Optimizes energy consumption with renewable, cooling, and federated learning.
    - DataExpert      : Handles data compression, caching, and efficient storage.
    - IoTExpert       : Manages IoT device energy and communication.
    - QuantumExpert   : Optimizes quantum circuit execution and resource allocation.
    - HeliumExpert    : Manages helium usage and recovery in cryogenic systems.
"""

import logging
import inspect
from typing import Dict, Type, Optional, Any, List, Callable, Awaitable, Union, Tuple
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import asyncio

__version__ = "2.2.0"

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
    # Additional common parameters can be added here

# ============================================================================
# Standardised Proposal Result Type
# ============================================================================
@dataclass
class MOPDProposal:
    """
    Standard return type for `propose` method in MOPD‑aware experts.
    """
    recommendations: Dict[str, Any]               # single preferred action
    options: List[Dict[str, Any]]                 # list of trade‑off options (could be Pareto front)
    explanation: str                              # natural‑language description
    pareto_front: Optional[List[Dict[str, Any]]] = None  # full Pareto front if available

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
        # If objective is to be minimized, negate (so that larger is better)
        if key in ['cost', 'latency']:
            a_vec.append(-solution_a.get(key, 0))
            b_vec.append(-solution_b.get(key, 0))
        else:
            a_vec.append(solution_a.get(key, 0))
            b_vec.append(solution_b.get(key, 0))
    # b dominates a if b is >= a in all and > in at least one
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
              objective_keys: List[str]) -> List[Tuple[Dict[str, Any], float]]:
    """
    Compute a scalarised score for each solution using weighted sum.
    Assumes objectives are already normalised (or will be normalised inside).
    Returns list of (solution, score).
    """
    # Normalise each objective across solutions (min‑max scaling)
    norm_solutions = []
    for key in objective_keys:
        vals = [sol.get(key, 0) for sol in solutions]
        min_val = min(vals)
        max_val = max(vals)
        range_val = max_val - min_val if max_val != min_val else 1
        for i, sol in enumerate(solutions):
            # For minimization, we invert
            if key in ['cost', 'latency']:
                norm_val = (max_val - sol.get(key, 0)) / range_val
            else:
                norm_val = (sol.get(key, 0) - min_val) / range_val
            sol[f'_norm_{key}'] = norm_val

    # Compute weighted sum
    scored = []
    for sol in solutions:
        score = 0.0
        for key in objective_keys:
            weight = weights.get(key, 0.0)
            score += weight * sol.get(f'_norm_{key}', 0)
        scored.append((sol, score))
    return scored

# ============================================================================
# Base Expert Interface (Abstract Base Class) – Enhanced with MOPD
# ============================================================================
class BaseExpert(ABC):
    """
    Abstract base for all MoE experts.
    All concrete experts must implement the methods defined here.
    MOPD‑aware methods are now part of the core interface.
    """
    __expert_version__: str = "0.0.0"          # Override per expert
    __expert_description__: str = ""           # Override per expert

    def __init_subclass__(cls, **kwargs):
        """Ensure subclasses define version and description."""
        if cls.__expert_version__ == "0.0.0":
            raise TypeError(f"{cls.__name__} must define __expert_version__")
        if cls.__expert_description__ == "":
            raise TypeError(f"{cls.__name__} must define __expert_description__")
        super().__init_subclass__(**kwargs)

    @abstractmethod
    async def propose(self, context: dict) -> MOPDProposal:
        """
        Generate a recommendation based on the provided context.

        Args:
            context: A dictionary containing relevant input data.

        Returns:
            A MOPDProposal containing:
                - recommendations: single preferred action set
                - options: list of trade‑off options
                - explanation: natural‑language description
                - pareto_front: (optional) full Pareto front
        """
        pass

    @abstractmethod
    def get_health_status(self) -> Dict[str, Any]:
        """
        Return health metrics of the expert.

        Returns:
            A dictionary with at least 'status' and optionally 'last_error',
            'thresholds', 'persistence_enabled', etc.
        """
        pass

    @abstractmethod
    async def shutdown(self):
        """
        Gracefully shut down the expert and any background tasks.
        """
        pass

    # ===== MOPD‑specific abstract methods =====
    @abstractmethod
    async def get_pareto_front(self, context: dict) -> List[Dict[str, Any]]:
        """
        Return a list of Pareto‑optimal solutions for the given context.
        This method is used when the expert is queried directly for the front.
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

    # ===== Optional lifecycle methods =====
    async def initialize(self):
        """Perform one‑time setup after instantiation."""
        pass

    def get_capabilities(self) -> Dict[str, Any]:
        """Return the expert's capabilities for routing and gating."""
        return {
            'name': self.__class__.__name__,
            'version': self.__expert_version__,
            'description': self.__expert_description__,
            'health_status': self.get_health_status(),
            'mopd_weights': self.get_objective_weights(),
            'mopd_config': self.get_mopd_config(),
        }

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
    for method in ['get_pareto_front', 'set_objective_weights', 'get_objective_weights', 'get_mopd_config']:
        if not hasattr(expert_class, method) or getattr(expert_class, method) is BaseExpert.__dict__.get(method):
            logger.warning(f"Expert '{name}' does not implement MOPD method '{method}'. "
                           f"It may not fully support MOPD. Consider updating the expert.")
    _EXPERT_REGISTRY[name] = expert_class
    logger.info(f"Registered expert '{name}' (v{expert_class.__expert_version__})")

def get_expert(name: str) -> Type[BaseExpert]:
    """
    Retrieve an expert class by its registered name.

    Args:
        name: The expert's class name (e.g., 'EnergyExpert').

    Returns:
        The expert class.

    Raises:
        ValueError: If the name is not registered.
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

    Args:
        name: The expert's name.

    Raises:
        ValueError: If the name is not registered.
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
    **kwargs: Any,
) -> BaseExpert:
    """
    Create an instance of an expert.

    Args:
        name: The expert's class name.
        bio_core: Optional reference to the bio‑inspired core for event subscriptions,
                  circuit breakers, etc.
        config: Optional configuration dictionary passed to the expert's constructor.
        mopd_config: Optional MOPD configuration. Can be a dict or MOPDConfig object.
        **kwargs: Additional keyword arguments to pass to the expert's constructor.

    Returns:
        An instance of the expert class.

    Raises:
        ValueError: If the name is not registered.
    """
    expert_class = get_expert(name)

    # Build argument dict from provided kwargs plus bio_core, config, mopd_config
    init_kwargs = kwargs.copy()
    if bio_core is not None:
        init_kwargs['bio_core'] = bio_core
    if config is not None:
        init_kwargs['config'] = config
    if mopd_config is not None:
        if isinstance(mopd_config, dict):
            mopd_config = MOPDConfig(**mopd_config)
        init_kwargs['mopd_config'] = mopd_config

    # Use inspect to see which parameters are accepted
    sig = inspect.signature(expert_class.__init__)
    params = sig.parameters
    # Filter out any kwargs that are not in the constructor signature
    filtered_kwargs = {}
    for k, v in init_kwargs.items():
        if k in params:
            filtered_kwargs[k] = v
        else:
            logger.debug(f"Argument '{k}' not accepted by {name}.__init__; ignoring.")

    # If the constructor has **kwargs, we can pass all
    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
        filtered_kwargs = init_kwargs

    # Instantiate
    return expert_class(**filtered_kwargs)

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
        base = expert.get_capabilities()
        # Add MOPD‑specific info (already in get_capabilities, but we can also include current weights)
        caps.append(base)
    return caps

# ============================================================================
# Backward Compatibility: eager imports for direct access
# ============================================================================
# We attempt to import each expert and register it if successful.
# If an import fails, the expert will not be available.
# We conditionally include only the ones that succeed in __all__.

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
# Only include experts that were successfully imported.
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
