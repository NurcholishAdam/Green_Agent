"""
ClassicalFallback — safe deterministic fallback for quantum jobs.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from ..contracts.execution_result import FallbackReason

logger = logging.getLogger(__name__)


@dataclass
class FallbackPlan:
    """The plan for falling back to classical execution."""
    reason: str
    classical_callable: Optional[Callable] = None
    description: str = ""
    estimated_cost: float = 0.0


class ClassicalFallback:
    """
    Manages deterministic classical fallback for quantum jobs.

    Callers register a classical callable per task type. When a quantum
    job times out, exceeds budget, or produces invalid output, the
    matching classical callable is invoked.
    """

    def __init__(self):
        self._plans: Dict[str, FallbackPlan] = {}

    def register(
        self,
        task_type: str,
        *,
        classical_callable: Callable,
        description: str = "",
        estimated_cost: float = 0.0,
    ) -> None:
        self._plans[task_type] = FallbackPlan(
            reason="registered",
            classical_callable=classical_callable,
            description=description,
            estimated_cost=estimated_cost,
        )

    def execute(
        self,
        task_type: str,
        *,
        task: Any,
        reason: FallbackReason,
    ) -> Dict[str, Any]:
        plan = self._plans.get(task_type)
        if plan is None or plan.classical_callable is None:
            logger.warning(
                f"No classical fallback registered for '{task_type}'"
            )
            return {
                "fallback_available": False,
                "reason": reason.value,
            }
        try:
            result = plan.classical_callable(task)
        except Exception as e:
            logger.error(f"Classical fallback failed: {e}")
            return {
                "fallback_available": True,
                "fallback_failed": True,
                "reason": reason.value,
                "error": str(e),
            }
        return {
            "fallback_available": True,
            "fallback_failed": False,
            "reason": reason.value,
            "result": result,
            "description": plan.description,
        }

    def has_fallback(self, task_type: str) -> bool:
        plan = self._plans.get(task_type)
        return plan is not None and plan.classical_callable is not None
