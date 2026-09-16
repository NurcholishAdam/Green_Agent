"""
QuantumExecutionResult — the immutable outcome of a quantum job.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


class ExecutionStatus(str, Enum):
    SUBMITTED = "submitted"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    FALLBACK = "fallback"
    CANCELLED = "cancelled"
    BUDGET_EXCEEDED = "budget_exceeded"


class FallbackReason(str, Enum):
    TIMEOUT = "timeout"
    BUDGET_EXCEEDED = "budget_exceeded"
    INVALID_OUTPUT = "invalid_output"
    HIGH_NOISE = "high_noise"
    BACKEND_UNAVAILABLE = "backend_unavailable"
    LOW_QUALITY = "low_quality"
    NONE = "none"


@dataclass
class QuantumExecutionResult:
    """
    Immutable outcome of a quantum execution request.

    Always carries the full provenance needed for reporting and audit.
    `fallback_result` is populated when the quantum route fails.
    """
    request_id: str
    task_id: str
    status: str

    # --- Backend & runtime ---
    backend: Optional[str] = None
    backend_kind: Optional[str] = None
    algorithm: Optional[str] = None
    shots_used: int = 0
    queue_seconds: float = 0.0
    runtime_seconds: float = 0.0

    # --- Quality & noise ---
    quality: Optional[float] = None
    fidelity: Optional[float] = None
    error_rate: Optional[float] = None
    uncertainty: float = 0.0

    # --- Resource accounting ---
    estimated_energy_kwh: float = 0.0
    estimated_co2e_kg: float = 0.0
    estimated_cost: float = 0.0

    # --- Provenance ---
    signature: Optional[str] = None
    circuit_version: Optional[str] = None
    seed: Optional[int] = None
    measurement_settings: Dict[str, Any] = field(default_factory=dict)
    raw_output: Any = None

    # --- Fallback ---
    fallback_used: bool = False
    fallback_reason: Optional[str] = None
    fallback_result: Any = None

    # --- Metadata ---
    completed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    audit_event_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["completed_at"] = self.completed_at.isoformat()
        return out

    def is_success(self) -> bool:
        return self.status == ExecutionStatus.COMPLETED.value

    def is_terminal_failure(self) -> bool:
        return self.status in (
            ExecutionStatus.FAILED.value,
            ExecutionStatus.BUDGET_EXCEEDED.value,
            ExecutionStatus.CANCELLED.value,
        )
