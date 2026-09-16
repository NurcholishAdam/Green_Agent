"""
TemporalLogic — minimal STL-style property combinators.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Any

from ..contracts.decision_request import DecisionRequest


@dataclass
class TemporalLogic:
    """Basic temporal logic combinators (Always / Eventually)."""

    @staticmethod
    def always(predicate: Callable[[DecisionRequest], bool], name: str = "always"):
        def _check(request: DecisionRequest) -> bool:
            return predicate(request)
        _check.__name__ = name
        return _check

    @staticmethod
    def implies(
        antecedent: Callable[[DecisionRequest], bool],
        consequent: Callable[[DecisionRequest], bool],
        name: str = "implies",
    ):
        def _check(request: DecisionRequest) -> bool:
            return (not antecedent(request)) or consequent(request)
        _check.__name__ = name
        return _check
