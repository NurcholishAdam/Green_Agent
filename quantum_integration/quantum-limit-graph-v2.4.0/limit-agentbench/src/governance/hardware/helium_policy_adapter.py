"""
HeliumPolicyAdapter — translate hardware capabilities into permitted
execution policies.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

logger = logging.getLogger(__name__)


@dataclass
class HardwareCapability:
    """Detected capabilities of a target device."""
    device_id: str
    max_memory_mb: float
    supported_precisions: Set[str]
    max_thermal_c: Optional[float] = None
    has_tensor_cores: bool = False
    supports_int8: bool = False
    supports_int4: bool = False
    helium_availability: float = 1.0


@dataclass
class PermittedPolicy:
    """The execution policy permitted for a given capability."""
    device_id: str
    allowed_precisions: List[str]
    max_memory_mb: float
    max_thermal_c: Optional[float]
    requires_fallback: bool = False
    notes: List[str] = field(default_factory=list)


class HeliumPolicyAdapter:
    """
    Translate hardware capability into an execution policy.

    The policy:
        - Constrains precision to what the device supports.
        - Preserves a memory headroom.
        - Preserves a thermal headroom.
        - Flags when helium scarcity requires fallback.
    """

    MEMORY_HEADROOM = 0.15           # 15% margin
    THERMAL_HEADROOM_C = 5.0
    HELIUM_FALLBACK_THRESHOLD = 0.4  # below this, force fallback

    def adapt(
        self, capability: HardwareCapability,
    ) -> PermittedPolicy:
        allowed = sorted(capability.supported_precisions)
        memory = capability.max_memory_mb * (1 - self.MEMORY_HEADROOM)
        thermal = (
            capability.max_thermal_c - self.THERMAL_HEADROOM_C
            if capability.max_thermal_c is not None else None
        )
        fallback = capability.helium_availability < self.HELIUM_FALLBACK_THRESHOLD
        notes: List[str] = []
        if fallback:
            notes.append(
                "helium scarcity below threshold; force fallback"
            )
        if not capability.has_tensor_cores:
            notes.append("no tensor cores; FP16 may not accelerate")

        return PermittedPolicy(
            device_id=capability.device_id,
            allowed_precisions=allowed,
            max_memory_mb=memory,
            max_thermal_c=thermal,
            requires_fallback=fallback,
            notes=notes,
        )
