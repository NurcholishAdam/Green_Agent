"""
VerificationEvidence — the outcome of a formal verification run.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass
class VerificationEvidence:
    policy_version: str
    verdict: str                         # "passed" | "failed" | "not_required"
    properties_checked: List[str] = field(default_factory=list)
    properties_passed: List[str] = field(default_factory=list)
    properties_failed: List[str] = field(default_factory=list)
    counterexamples: List[Dict[str, Any]] = field(default_factory=list)
    coverage_pct: float = 0.0
    verifier: Optional[str] = None
    verifier_version: Optional[str] = None
    at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        return out
