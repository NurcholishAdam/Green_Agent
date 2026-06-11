# File: enhancements/moe_expert_system/__init__.py

"""
Mixture of Experts (MoE) Enhancement for Green Agent
Provides specialized expert routing within the 12-layer architecture
while maintaining dual-axis, neuro-symbolic, and meta-cognitive design.
"""

from .expert_registry import ExpertRegistry
from .gating_network import MoEGatingNetwork
from .expert_router import ExpertRouter
from .experts import (
    EnergyExpert,
    DataExpert,
    IoTExpert,
    QuantumExpert,
    HeliumExpert
)
from .monitoring.expert_metrics import ExpertMetricsCollector
from .integration.layer_integrator import LayerIntegrator

__version__ = "1.0.0"
__all__ = [
    'ExpertRegistry',
    'MoEGatingNetwork',
    'ExpertRouter',
    'EnergyExpert',
    'DataExpert',
    'IoTExpert',
    'QuantumExpert',
    'HeliumExpert',
    'ExpertMetricsCollector',
    'LayerIntegrator'
]
