"""
HardwareCapabilityPolicy — decide whether a device may run a task.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .helium_policy_adapter import HardwareCapability, PermittedPolicy


@dataclass
class HardwareCapabilityPolicy:
    """Governance rule: deny devices that fall below capability floors."""
    min_memory_mb: float = 512.0
    min_helium_availability: float = 0.3
    require_tensor_cores: bool = False

    def evaluate(
        self, capability: HardwareCapability,
    ) -> tuple:
        reasons = []
        if capability.max_memory_mb < self.min_memory_mb:
            reasons.append(
                f"memory {capability.max_memory_mb}MB below floor "
                f"{self.min_memory_mb}MB"
            )
        if capability.helium_availability < self.min_helium_availability:
            reasons.append(
                f"helium availability {capability.helium_availability} "
                f"below floor {self.min_helium_availability}"
            )
        if self.require_tensor_cores and not capability.has_tensor_cores:
            reasons.append("device lacks tensor cores")
        return (len(reasons) == 0, reasons)
