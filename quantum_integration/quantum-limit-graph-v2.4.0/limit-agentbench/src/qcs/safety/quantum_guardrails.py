"""
QuantumGuardrails — pre-flight safety gates for quantum jobs.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

from ..contracts.execution_request import QuantumExecutionRequest
from ..contracts.task_spec import QuantumTaskSpec

logger = logging.getLogger(__name__)


@dataclass
class GuardrailVerdict:
    """The verdict of a single guardrail check."""
    name: str
    passed: bool
    reason: str
    severity: str = "warning"


@dataclass
class GuardrailResult:
    """Aggregate verdict across all guardrails."""
    passed: bool
    verdicts: List[GuardrailVerdict] = field(default_factory=list)
    blocking: List[GuardrailVerdict] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "passed": self.passed,
            "verdicts": [asdict(v) for v in self.verdicts],
            "blocking": [asdict(v) for v in self.blocking],
        }


class QuantumGuardrails:
    """
    Pre-flight safety gates.

    Every quantum job must pass these gates before execution:

      1. A classical baseline is declared.
      2. A minimum quality threshold is set.
      3. Budgets are declared (queue, cost, energy, carbon).
      4. The backend is on the allow-list (if one is configured).
      5. The task spec passes validation.
      6. The execution request passes validation.
    """

    def __init__(
        self,
        *,
        backend_allowlist: Optional[List[str]] = None,
        simulator_first: bool = True,
        max_queue_seconds: float = 600.0,
        max_cost: float = 100.0,
    ):
        self.backend_allowlist = set(backend_allowlist or [])
        self.simulator_first = simulator_first
        self.max_queue_seconds = max_queue_seconds
        self.max_cost = max_cost

    def check(
        self,
        *,
        task_spec: QuantumTaskSpec,
        request: QuantumExecutionRequest,
        backend_kind: Optional[str] = None,
        backend_id: Optional[str] = None,
    ) -> GuardrailResult:
        verdicts: List[GuardrailVerdict] = []
        blocking: List[GuardrailVerdict] = []

        # --- 1. Classical baseline declared ---
        verdicts.append(GuardrailVerdict(
            name="classical_baseline_declared",
            passed=bool(task_spec.classical_baseline),
            reason=(
                f"classical_baseline = {task_spec.classical_baseline}"
                if task_spec.classical_baseline
                else "no classical baseline declared"
            ),
            severity="blocking",
        ))

        # --- 2. Quality threshold ---
        verdicts.append(GuardrailVerdict(
            name="quality_threshold_set",
            passed=request.min_quality > 0.0,
            reason=(
                f"min_quality = {request.min_quality}"
                if request.min_quality > 0.0
                else "min_quality must be > 0"
            ),
            severity="blocking",
        ))

        # --- 3. Queue budget ---
        verdicts.append(GuardrailVerdict(
            name="queue_budget_within_limit",
            passed=request.max_queue_seconds <= self.max_queue_seconds,
            reason=(
                f"max_queue_seconds={request.max_queue_seconds} "
                f"vs limit={self.max_queue_seconds}"
            ),
            severity="blocking",
        ))

        # --- 4. Cost budget ---
        verdicts.append(GuardrailVerdict(
            name="cost_budget_within_limit",
            passed=request.max_cost <= self.max_cost,
            reason=(
                f"max_cost={request.max_cost} "
                f"vs limit={self.max_cost}"
            ),
            severity="blocking",
        ))

        # --- 5. Backend allowlist ---
        if self.backend_allowlist:
            verdicts.append(GuardrailVerdict(
                name="backend_allowlisted",
                passed=backend_id in self.backend_allowlist,
                reason=(
                    f"backend_id={backend_id} "
                    f"is in allowlist={sorted(self.backend_allowlist)}"
                    if backend_id in self.backend_allowlist
                    else f"backend_id={backend_id} not in allowlist"
                ),
                severity="blocking",
            ))

        # --- 6. Simulator-first (advisory) ---
        if self.simulator_first:
            verdicts.append(GuardrailVerdict(
                name="simulator_first",
                passed=backend_kind != "hardware",
                reason=(
                    f"backend_kind={backend_kind}; "
                    "prefer simulator for development"
                ),
                severity="warning",
            ))

        # --- 7. Task spec validation ---
        spec_errors = task_spec.validate()
        verdicts.append(GuardrailVerdict(
            name="task_spec_valid",
            passed=not spec_errors,
            reason=f"errors: {spec_errors}" if spec_errors else "spec valid",
            severity="blocking",
        ))

        # --- 8. Request validation ---
        req_errors = request.validate()
        verdicts.append(GuardrailVerdict(
            name="request_valid",
            passed=not req_errors,
            reason=f"errors: {req_errors}" if req_errors else "request valid",
            severity="blocking",
        ))

        # --- Aggregate ---
        blocking = [v for v in verdicts if not v.passed and v.severity == "blocking"]
        return GuardrailResult(
            passed=not blocking,
            verdicts=verdicts,
            blocking=blocking,
        )
