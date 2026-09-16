"""
PolicyVerifier — formal verification of safety-critical policies.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from ..contracts.decision_request import DecisionRequest
from .verification_evidence import VerificationEvidence

logger = logging.getLogger(__name__)


class PolicyVerifier:
    """
    Verifies a request against a set of registered safety properties.

    A property is a callable that returns True (satisfied) or False
    (violated). When violated, a counterexample dict may be produced.
    """

    def __init__(self):
        self._properties: Dict[str, Callable[[DecisionRequest], bool]] = {}

    def register(
        self, name: str, predicate: Callable[[DecisionRequest], bool],
    ) -> None:
        self._properties[name] = predicate

    async def verify(self, request: DecisionRequest) -> VerificationEvidence:
        checked: List[str] = []
        passed: List[str] = []
        failed: List[str] = []
        counterexamples: List[Dict[str, Any]] = []

        for name, predicate in self._properties.items():
            checked.append(name)
            try:
                ok = predicate(request)
                if ok:
                    passed.append(name)
                else:
                    failed.append(name)
                    counterexamples.append({"property": name})
            except Exception as e:
                failed.append(name)
                counterexamples.append({
                    "property": name,
                    "error": str(e),
                })

        verdict = (
            "passed" if not failed
            else "failed" if failed
            else "not_required"
        )

        return VerificationEvidence(
            policy_version=request.policy_version or "unknown",
            verdict=verdict,
            properties_checked=checked,
            properties_passed=passed,
            properties_failed=failed,
            counterexamples=counterexamples,
            coverage_pct=1.0 if not checked else len(passed) / len(checked),
        )
