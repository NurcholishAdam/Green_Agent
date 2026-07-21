"""
MoE Expert System – Expert Module (Enhanced v2.0)

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
from typing import Dict, Type, Optional, Any, List
from abc import ABC, abstractmethod

__version__ = "2.0.0"

logger = logging.getLogger(__name__)

# ============================================================================
# Base Expert Interface (Abstract Base Class)
# ============================================================================
class BaseExpert(ABC):
    """
    Abstract base for all MoE experts.
    All concrete experts must implement the methods defined here.
    """
    __expert_version__ = "0.0.0"          # Override per expert
    __expert_description__ = ""           # Override per expert

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
    """
    if not issubclass(expert_class, BaseExpert):
        raise TypeError(f"{expert_class} must inherit from BaseExpert")
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

# ============================================================================
# Factory: instantiate an expert with bio_core injection and config
# ============================================================================
def create_expert(
    name: str,
    bio_core: Optional[Any] = None,
    config: Optional[Dict[str, Any]] = None
) -> BaseExpert:
    """
    Create an instance of an expert.

    Args:
        name: The expert's class name.
        bio_core: Optional reference to the bio‑inspired core for event subscriptions,
                  circuit breakers, etc.
        config: Optional configuration dictionary passed to the expert's constructor.

    Returns:
        An instance of the expert class.

    Raises:
        ValueError: If the name is not registered.
    """
    expert_class = get_expert(name)

    # Detect if the constructor accepts bio_core and/or config
    sig = inspect.signature(expert_class.__init__)
    params = sig.parameters

    # Build argument dict
    kwargs = {}
    if 'bio_core' in params:
        kwargs['bio_core'] = bio_core
    if 'config' in params:
        kwargs['config'] = config

    # If the expert has a constructor with only self, instantiate without args
    if len(params) == 1:  # only self
        return expert_class()
    else:
        # Attempt to pass only the arguments that the constructor expects
        # We'll use the kwargs we built
        return expert_class(**{k: v for k, v in kwargs.items() if k in params})

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
# This maintains the old direct import style.

try:
    from .energy_expert import EnergyExpert
    register_expert('EnergyExpert', EnergyExpert)
except ImportError as e:
    logger.warning(f"EnergyExpert could not be imported: {e}")
    EnergyExpert = None

try:
    from .data_expert import DataExpert
    register_expert('DataExpert', DataExpert)
except ImportError as e:
    logger.warning(f"DataExpert could not be imported: {e}")
    DataExpert = None

try:
    from .iot_expert import IoTExpert
    register_expert('IoTExpert', IoTExpert)
except ImportError as e:
    logger.warning(f"IoTExpert could not be imported: {e}")
    IoTExpert = None

try:
    from .quantum_expert import QuantumExpert
    register_expert('QuantumExpert', QuantumExpert)
except ImportError as e:
    logger.warning(f"QuantumExpert could not be imported: {e}")
    QuantumExpert = None

try:
    from .helium_expert import HeliumExpert
    register_expert('HeliumExpert', HeliumExpert)
except ImportError as e:
    logger.warning(f"HeliumExpert could not be imported: {e}")
    HeliumExpert = None

# ============================================================================
# __all__ – control what is exported with 'from ... import *'
# ============================================================================
__all__ = [
    'BaseExpert',
    'EnergyExpert',
    'DataExpert',
    'IoTExpert',
    'QuantumExpert',
    'HeliumExpert',
    'get_expert',
    'create_expert',
    'list_experts',
    'register_expert',
    'shutdown_all_experts',
    '__version__',
]
