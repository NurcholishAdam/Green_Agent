"""
stl.py — Temporal logic monitor and safety shield (Enhancement 5).

Implements G (always), F (eventually), and U (until) over a rolling window.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple


class STLOperator(str, Enum):
    ALWAYS = "G"
    EVENTUALLY = "F"
    UNTIL = "U"


@dataclass
class STLFormula:
    name: str
    predicate: Callable[[Dict[str, Any]], bool]
    operator: STLOperator
    horizon: int = 10
    until_predicate: Optional[Callable[[Dict[str, Any]], bool]] = None


class TemporalLogicMonitor:
    """
    Runtime monitor with correct G / F / U semantics.

      G[0,h) phi : phi holds at every step in the window
      F[0,h) phi : phi holds at some step in the window
      phi U[0,h) psi : phi holds at every step until psi becomes true within h
    """

    def __init__(self, horizon: int = 10):
        self.horizon = horizon
        self._history: deque = deque(maxlen=max(horizon * 4, horizon))
        self.formulas: List[STLFormula] = []
        self._evaluations: deque = deque(maxlen=1024)

    def add_formula(self, f: STLFormula) -> None:
        self.formulas.append(f)

    def observe(self, record: Dict[str, Any]) -> None:
        self._history.append(record)

    def _window(self, f: STLFormula) -> List[Dict[str, Any]]:
        return list(self._history)[-f.horizon:]

    def verify(self) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        for f in self.formulas:
            window = self._window(f)
            if not window:
                results[f.name] = True
                continue
            if f.operator == STLOperator.ALWAYS:
                ok = all(f.predicate(r) for r in window)
            elif f.operator == STLOperator.EVENTUALLY:
                ok = any(f.predicate(r) for r in window)
            elif f.operator == STLOperator.UNTIL:
                until_pred = f.until_predicate or f.predicate
                idx_until = next((i for i, r in enumerate(window) if until_pred(r)), None)
                if idx_until is None:
                    ok = False
                else:
                    ok = all(f.predicate(window[i]) for i in range(idx_until))
            else:
                ok = True
            results[f.name] = ok
            self._evaluations.append({"formula": f.name, "ok": ok, "t": time.time()})
        return results

    def recent_evaluations(self, k: int = 32) -> List[Dict[str, Any]]:
        return list(self._evaluations)[-k:]


class SafetyShield:
    """Screens the current state against STL invariants."""

    def __init__(self, monitor: TemporalLogicMonitor):
        self.monitor = monitor
        self.violations: deque = deque(maxlen=1024)

    def screen(self) -> Tuple[bool, Dict[str, bool], Optional[str]]:
        verdict = self.monitor.verify()
        violated = [k for k, ok in verdict.items() if not ok]
        if not violated:
            return True, verdict, None
        fallback = "safe_default"
        self.violations.append({"t": time.time(), "violated": violated, "fallback": fallback})
        return False, verdict, fallback
