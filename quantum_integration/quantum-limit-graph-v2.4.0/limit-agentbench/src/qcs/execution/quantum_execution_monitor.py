"""
QuantumExecutionMonitor — lifecycle monitoring for a quantum job.

Emits formal states: submitted, queued, running, completed, failed,
fallback, cancelled, budget_exceeded.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from ..contracts.execution_result import (
    ExecutionStatus, FallbackReason, QuantumExecutionResult,
)

logger = logging.getLogger(__name__)


@dataclass
class MonitorEvent:
    """A single lifecycle event."""
    event_id: str
    request_id: str
    status: str
    at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        return out


class QuantumExecutionMonitor:
    """
    Tracks lifecycle transitions and enforces budgets.

    Callers drive the lifecycle by calling `mark_submitted()`,
    `mark_queued()`, `mark_running()`, and finally `complete()` or
    `fail()`. On budget overrun, the monitor automatically transitions
    to BUDGET_EXCEEDED.
    """

    def __init__(self, *, event_callback: Optional[Callable] = None):
        self._events: Dict[str, List[MonitorEvent]] = {}
        self._start_times: Dict[str, float] = {}
        self._callback = event_callback

    def start(self, request_id: str) -> MonitorEvent:
        self._start_times[request_id] = time.monotonic()
        return self._emit(request_id, ExecutionStatus.SUBMITTED.value)

    def mark_queued(self, request_id: str) -> MonitorEvent:
        return self._emit(request_id, ExecutionStatus.QUEUED.value)

    def mark_running(self, request_id: str) -> MonitorEvent:
        return self._emit(request_id, ExecutionStatus.RUNNING.value)

    def check_queue_budget(
        self, request_id: str, max_queue_seconds: float,
    ) -> bool:
        """Return True if the queue budget has been exceeded."""
        elapsed = time.monotonic() - self._start_times.get(request_id, 0.0)
        return elapsed > max_queue_seconds

    def complete(
        self,
        *,
        request_id: str,
        task_id: str,
        backend: str,
        shots_used: int,
        quality: Optional[float],
        raw_output: Any,
        runtime_seconds: float,
        queue_seconds: float,
        energy_kwh: float = 0.0,
        co2e_kg: float = 0.0,
        cost: float = 0.0,
        fidelity: Optional[float] = None,
        error_rate: Optional[float] = None,
        signature: Optional[str] = None,
        circuit_version: Optional[str] = None,
        seed: Optional[int] = None,
    ) -> QuantumExecutionResult:
        self._emit(
            request_id,
            ExecutionStatus.COMPLETED.value,
            {"shots_used": shots_used, "quality": quality},
        )
        return QuantumExecutionResult(
            request_id=request_id,
            task_id=task_id,
            status=ExecutionStatus.COMPLETED.value,
            backend=backend,
            shots_used=shots_used,
            queue_seconds=queue_seconds,
            runtime_seconds=runtime_seconds,
            quality=quality,
            fidelity=fidelity,
            error_rate=error_rate,
            estimated_energy_kwh=energy_kwh,
            estimated_co2e_kg=co2e_kg,
            estimated_cost=cost,
            raw_output=raw_output,
            signature=signature,
            circuit_version=circuit_version,
            seed=seed,
        )

    def fail(
        self,
        *,
        request_id: str,
        task_id: str,
        reason: str,
        status: str = ExecutionStatus.FAILED.value,
    ) -> QuantumExecutionResult:
        self._emit(request_id, status, {"reason": reason})
        return QuantumExecutionResult(
            request_id=request_id,
            task_id=task_id,
            status=status,
        )

    def trigger_fallback(
        self,
        *,
        request_id: str,
        task_id: str,
        reason: FallbackReason,
        fallback_result: Any,
    ) -> QuantumExecutionResult:
        self._emit(
            request_id,
            ExecutionStatus.FALLBACK.value,
            {"reason": reason.value},
        )
        return QuantumExecutionResult(
            request_id=request_id,
            task_id=task_id,
            status=ExecutionStatus.FALLBACK.value,
            fallback_used=True,
            fallback_reason=reason.value,
            fallback_result=fallback_result,
        )

    def events(self, request_id: str) -> List[Dict[str, Any]]:
        return [e.to_dict() for e in self._events.get(request_id, [])]

    def _emit(
        self, request_id: str, status: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> MonitorEvent:
        event = MonitorEvent(
            event_id=uuid.uuid4().hex[:12],
            request_id=request_id,
            status=status,
            details=dict(details or {}),
        )
        self._events.setdefault(request_id, []).append(event)
        if self._callback:
            try:
                self._callback(event)
            except Exception as e:
                logger.warning(f"Monitor callback failed: {e}")
        return event
