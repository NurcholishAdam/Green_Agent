"""
MoE Expert System – Expert Module (Enhanced v2.1.0)

This package provides the core experts used in the mixture‑of‑experts framework.
Each expert implements a specific optimization domain (energy, data, IoT, quantum, helium).

Usage:
    from enhancements.moe_expert_system.experts import get_expert, create_expert, BaseExpert

    # Get the EnergyExpert class directly
    EnergyExpert = get_expert('EnergyExpert')

    # Or instantiate with configuration and optional bio_core
    expert = create_expert('EnergyExpert', bio_core=my_core, config={'enable_forecasting': True})

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
from typing import Dict, Type, Optional, Any, List, Callable, Awaitable
from abc import ABC, abstractmethod
import asyncio

__version__ = "2.1.0"

logger = logging.getLogger(__name__)

# ============================================================================
# Base Expert Interface (Abstract Base Class)
# ============================================================================
class BaseExpert(ABC):
    """
    Abstract base for all MoE experts.
    All concrete experts must implement the methods defined here.
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
    async def propose(self, context: dict) -> dict:
        """
        Generate a recommendation based on the provided context.

        Args:
            context: A dictionary containing relevant input data.

        Returns:
            A dictionary containing:
                - 'recommendations': single preferred action set
                - 'options': list of trade‑off options
                - 'explanation': natural‑language description
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

    async def initialize(self):
        """
        Perform one‑time setup after instantiation.
        This can be overridden by experts that need async initialization.
        """
        pass

    def get_capabilities(self) -> Dict[str, Any]:
        """
        Return the expert's capabilities for routing and gating.
        Default implementation returns a basic set.
        Experts should override to provide more details.
        """
        return {
            'name': self.__class__.__name__,
            'version': self.__expert_version__,
            'description': self.__expert_description__,
            'health_status': self.get_health_status(),
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
    """
    Return a list of all registered expert names.
    """
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
    **kwargs: Any,
) -> BaseExpert:
    """
    Create an instance of an expert.

    Args:
        name: The expert's class name.
        bio_core: Optional reference to the bio‑inspired core for event subscriptions,
                  circuit breakers, etc.
        config: Optional configuration dictionary passed to the expert's constructor.
        **kwargs: Additional keyword arguments to pass to the expert's constructor.

    Returns:
        An instance of the expert class.

    Raises:
        ValueError: If the name is not registered.
    """
    expert_class = get_expert(name)

    # Build argument dict from provided kwargs plus bio_core and config
    init_kwargs = kwargs.copy()
    if bio_core is not None:
        init_kwargs['bio_core'] = bio_core
    if config is not None:
        init_kwargs['config'] = config

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
# Helper: shutdown multiple experts
# ============================================================================
async def shutdown_all_experts(experts: List[BaseExpert]) -> None:
    """
    Gracefully shut down a list of experts.

    Args:
        experts: List of expert instances.
    """
    for expert in experts:
        try:
            await expert.shutdown()
        except Exception as e:
            logger.error(f"Error shutting down expert {expert.__class__.__name__}: {e}")

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
    '__version__',
]

# Add successfully imported experts to __all__
for expert_name, expert_class in _imported_experts.items():
    globals()[expert_name] = expert_class
    __all__.append(expert_name)
