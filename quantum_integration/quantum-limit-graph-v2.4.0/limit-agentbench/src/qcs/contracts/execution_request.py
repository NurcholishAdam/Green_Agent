"""
QuantumExecutionRequest — the budgeted, reversible quantum job spec.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional


class BackendPreference(str, Enum):
    AUTO = "auto"
    SIMULATOR = "simulator"
    HARDWARE = "hardware"


@dataclass
class QuantumExecutionRequest:
    """
    A budgeted, reversible quantum job request.

    Every constraint is explicit. If any budget is exceeded, the
    execution monitor triggers classical fallback.
    """
    request_id: str
    task_id: str
    algorithm: str
    backend_preference: str = BackendPreference.AUTO.value
    max_shots: int = 1000
    max_queue_seconds: float = 300.0
    max_cost: float = 0.0              # 0 means "no monetary budget"
    max_energy_kwh: Optional[float] = None
    min_quality: float = 0.8
    allow_classical_fallback: bool = True
    policy_version: str = "v5.0.0"
    encoding_hint: Optional[str] = None
    seed: Optional[int] = None
    requested_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["requested_at"] = self.requested_at.isoformat()
        return out

    def validate(self) -> list:
        errors: list = []
        if not self.request_id:
            errors.append("request_id must be non-empty")
        if not self.task_id:
            errors.append("task_id must be non-empty")
        if not self.algorithm:
            errors.append("algorithm must be non-empty")
        try:
            BackendPreference(self.backend_preference)
        except ValueError:
            errors.append(
                f"unknown backend_preference '{self.backend_preference}'"
            )
        if self.max_shots <= 0:
            errors.append("max_shots must be positive")
        if self.max_queue_seconds < 0:
            errors.append("max_queue_seconds must be non-negative")
        if not 0.0 <= self.min_quality <= 1.0:
            errors.append("min_quality must be in [0, 1]")
        return errors
