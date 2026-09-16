"""
InputValidator — validation for all QCS inputs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class ValidationResult:
    """Aggregated validation result."""
    valid: bool
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {"valid": self.valid, "errors": list(self.errors)}


class InputValidator:
    """
    Validates quantum task specs, execution requests, and backend
    profiles before they enter the routing and execution pipeline.
    """

    @staticmethod
    def validate_task_spec(task_spec: Any) -> ValidationResult:
        errors = task_spec.validate() if hasattr(task_spec, "validate") else []
        return ValidationResult(valid=not errors, errors=list(errors))

    @staticmethod
    def validate_request(request: Any) -> ValidationResult:
        errors = (
            request.validate() if hasattr(request, "validate") else []
        )
        return ValidationResult(valid=not errors, errors=list(errors))

    @staticmethod
    def validate_backend(profile: Any) -> ValidationResult:
        errors: List[str] = []
        if not getattr(profile, "backend_id", None):
            errors.append("backend_id must be non-empty")
        if getattr(profile, "num_qubits", 0) <= 0:
            errors.append("num_qubits must be positive")
        if getattr(profile, "max_shots_per_job", 0) <= 0:
            errors.append("max_shots_per_job must be positive")
        return ValidationResult(valid=not errors, errors=errors)

    @staticmethod
    def validate_payload(payload: Any) -> ValidationResult:
        if payload is None:
            return ValidationResult(valid=False, errors=["payload is None"])
        return ValidationResult(valid=True)
