"""
CounterexampleHandler — decides what to do when verification fails.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class CounterexampleResponse:
    action: str                 # "deny" | "escalate" | "fallback"
    reason: str
    counterexamples: List[Dict[str, Any]]


class CounterexampleHandler:
    """Default policy: deny on any counterexample."""

    def handle(
        self, counterexamples: List[Dict[str, Any]],
    ) -> CounterexampleResponse:
        if not counterexamples:
            return CounterexampleResponse(
                action="allow",
                reason="no counterexamples",
                counterexamples=[],
            )
        return CounterexampleResponse(
            action="deny",
            reason=f"{len(counterexamples)} counterexample(s) present",
            counterexamples=counterexamples,
        )
