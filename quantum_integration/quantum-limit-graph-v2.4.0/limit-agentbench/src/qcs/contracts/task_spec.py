"""
QuantumTaskSpec — the canonical description of a quantum problem.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


class ProblemFamily(str, Enum):
    """Recognized quantum problem families."""
    OPTIMIZATION = "optimization"        # QAOA, VQE, annealing
    SAMPLING = "sampling"                # boson sampling, Gibbs
    LINEAR_SYSTEM = "linear_system"      # HHL
    SIMULATION = "simulation"            # Hamiltonian simulation
    MACHINE_LEARNING = "ml"              # quantum kernels, QNNs
    CRYPTOGRAPHY = "cryptography"        # key distribution
    UNKNOWN = "unknown"


@dataclass
class QuantumTaskSpec:
    """
    What the caller wants to solve.

    This spec does not commit to a backend, algorithm, or execution
    mode — it describes the problem so the router can decide.
    """
    task_id: str
    problem_family: str
    problem_size: int                       # e.g. number of variables
    classical_baseline: Optional[str] = None
    classical_baseline_quality: Optional[float] = None
    min_quality: float = 0.0
    complexity_class: Optional[str] = None  # e.g. "NP-hard", "BQP"
    problem_metadata: Dict[str, Any] = field(default_factory=dict)
    submitted_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["submitted_at"] = self.submitted_at.isoformat()
        return out

    def validate(self) -> List[str]:
        """Return a list of validation errors (empty when valid)."""
        errors: List[str] = []
        if not self.task_id:
            errors.append("task_id must be non-empty")
        try:
            ProblemFamily(self.problem_family)
        except ValueError:
            errors.append(
                f"unknown problem_family '{self.problem_family}'"
            )
        if self.problem_size <= 0:
            errors.append("problem_size must be positive")
        if not 0.0 <= self.min_quality <= 1.0:
            errors.append("min_quality must be in [0, 1]")
        if self.classical_baseline is None:
            errors.append(
                "classical_baseline is required — QCS will not route "
                "without a comparison anchor"
            )
        return errors
