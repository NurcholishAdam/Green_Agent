"""
MoE Expert System – Expert Module

This package provides the core experts used in the mixture‑of‑experts framework.
Each expert implements a specific optimization domain (energy, data, IoT, quantum, helium).

Usage:
    from enhancements.moe_expert_system.experts import get_expert, create_expert, Expert

    # Get the EnergyExpert class directly
    EnergyExpert = get_expert('EnergyExpert')

    # Or instantiate with configuration
    expert = create_expert('EnergyExpert', config={'enable_forecasting': True})

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
import importlib
from typing import Dict, Type, Optional, Any

__version__ = "1.0.0"

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Registry of expert classes (lazily loaded)
# ----------------------------------------------------------------------
_EXPERT_REGISTRY: Dict[str, str] = {
    'EnergyExpert': 'enhancements.moe_expert_system.experts.energy_expert',
    'DataExpert':   'enhancements.moe_expert_system.experts.data_expert',
    'IoTExpert':    'enhancements.moe_expert_system.experts.iot_expert',
    'QuantumExpert':'enhancements.moe_expert_system.experts.quantum_expert',
    'HeliumExpert': 'enhancements.moe_expert_system.experts.helium_expert',
}

# ----------------------------------------------------------------------
# Public API: get expert class by name
# ----------------------------------------------------------------------
def get_expert(name: str) -> Type:
    """
    Retrieve an expert class by its registered name.

    Args:
        name: The expert's class name (e.g., 'EnergyExpert').

    Returns:
        The expert class.

    Raises:
        ValueError: If the name is not registered.
        ImportError: If the underlying module cannot be imported.
    """
    module_path = _EXPERT_REGISTRY.get(name)
    if module_path is None:
        raise ValueError(f"Expert '{name}' is not registered.")
    try:
        module = importlib.import_module(module_path)
        expert_class = getattr(module, name)
        return expert_class
    except ImportError as e:
        logger.error(f"Failed to import expert '{name}' from {module_path}: {e}")
        raise ImportError(f"Could not import expert '{name}'") from e

# ----------------------------------------------------------------------
# Factory: instantiate an expert with optional configuration
# ----------------------------------------------------------------------
def create_expert(name: str, config: Optional[Dict[str, Any]] = None) -> Any:
    """
    Create an instance of an expert.

    Args:
        name: The expert's class name.
        config: Optional configuration dictionary passed to the expert's constructor.

    Returns:
        An instance of the expert class.

    Raises:
        ValueError: If the name is not registered.
        ImportError: If the expert module cannot be imported.
    """
    expert_class = get_expert(name)
    if config is not None:
        return expert_class(config=config)
    else:
        return expert_class()

# ----------------------------------------------------------------------
# Lazy imports for direct access (optional)
# ----------------------------------------------------------------------
# To allow `from . import EnergyExpert` style imports, we keep the original
# eager imports for backwards compatibility, but we wrap them in a try/except.
try:
    from .energy_expert import EnergyExpert
except ImportError:
    logger.warning("EnergyExpert could not be imported (missing dependencies?)")
    EnergyExpert = None

try:
    from .data_expert import DataExpert
except ImportError:
    logger.warning("DataExpert could not be imported")
    DataExpert = None

try:
    from .iot_expert import IoTExpert
except ImportError:
    logger.warning("IoTExpert could not be imported")
    IoTExpert = None

try:
    from .quantum_expert import QuantumExpert
except ImportError:
    logger.warning("QuantumExpert could not be imported")
    QuantumExpert = None

try:
    from .helium_expert import HeliumExpert
except ImportError:
    logger.warning("HeliumExpert could not be imported")
    HeliumExpert = None

# ----------------------------------------------------------------------
# Re-export base Expert class if it exists
# ----------------------------------------------------------------------
try:
    from .base_expert import Expert
except ImportError:
    # If there is no base expert, we define a dummy for type hints.
    class Expert:
        """Base type for all experts (fallback if no base_expert module exists)."""
        pass

# ----------------------------------------------------------------------
# __all__ – control what is exported with 'from ... import *'
# ----------------------------------------------------------------------
__all__ = [
    'EnergyExpert',
    'DataExpert',
    'IoTExpert',
    'QuantumExpert',
    'HeliumExpert',
    'Expert',
    'get_expert',
    'create_expert',
    '__version__',
]
