"""
QuantumEvidence — the structured route recommendation QCS returns.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


class RouteJustification(str, Enum):
    """Classification of the QCS route recommendation."""
    QUANTUM_FAVORED = "quantum_favored"
    DISTILLED_FAVORED = "distilled_favored"
    CLASSICAL_FAVORED = "classical_favored"
    INCONCLUSIVE = "inconclusive"
    BLOCKED = "blocked"


@dataclass
class QuantumEvidence:
    """
    Structured evidence returned by QCS for a quantum route.

    This is a **recommendation with evidence** — not a final decision.
    The orchestrator, governance, or policy layer decides whether to
    act on it.
    """
    task_id: str
    justification: str
    recommended_backend: Optional[str] = None
    recommended_algorithm: Optional[str] = None

    # --- Quality expectation ---
    expected_quality: Optional[float] = None
    classical_baseline_quality: Optional[float] = None
    expected_advantage: Optional[float] = None

    # --- Cost expectation ---
    expected_queue_seconds: float = 0.0
    expected_shots: int = 0
    expected_energy_kwh: float = 0.0
    expected_co2e_kg: float = 0.0
    expected_cost_usd: float = 0.0

    # --- Uncertainty ---
    quality_uncertainty: float = 0.0
    noise_estimate: Optional[float] = None

    # --- Rationale (XAI) ---
    rationale: List[str] = field(default_factory=list)
    rejected_alternatives: List[Dict[str, Any]] = field(default_factory=list)

    # --- Safety & governance ---
    safety_verdict: str = "not_required"
    guardrail_verdicts: List[Dict[str, Any]] = field(default_factory=list)
    requires_human_review: bool = False

    # --- Provenance ---
    policy_version: Optional[str] = None
    qcs_version: str = "0.1.0"
    generated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["generated_at"] = self.generated_at.isoformat()
        return out
