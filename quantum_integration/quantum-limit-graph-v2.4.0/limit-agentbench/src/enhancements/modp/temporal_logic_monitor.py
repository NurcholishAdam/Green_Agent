"""
temporal_logic_monitor.py
Temporal logic runtime verification for MOPD safety policies.
"""
from __future__ import annotations
import asyncio
import re
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple


_ATOMIC_RE = re.compile(r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


class TemporalRule:
    def __init__(self, rule_id: str, operator: str,
                 conditions: List[Callable[[Dict], bool]],
                 window: float = 0.0, description: str = "",
                 severity: str = "warning"):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window = window
        self.description = description or rule_id
        self.severity = severity
        self.violations = 0
        self.last_violation: Optional[datetime] = None

    def evaluate(self, trace: Deque[Tuple[datetime, Dict]]) -> bool:
        if not trace:
            return False
        if self.window > 0:
            cutoff = trace[-1][0] - timedelta(seconds=self.window)
            while trace and trace[0][0] < cutoff:
                trace.popleft()
        op = self.operator
        if op == "always":
            return any(not self.conditions[0](s) for _, s in trace)
        if op == "eventually":
            return not any(self.conditions[0](s) for _, s in trace)
        if op == "never":
            return any(self.conditions[0](s) for _, s in trace)
        return False


class TemporalLogicVerifier:
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict]] = deque(
            maxlen=getattr(config, "temporal_max_trace", 2000))
        self.approval_cb: Optional[Callable] = None
        for formula in getattr(config, "temporal_formulas", []) or []:
            self._install(formula)

    def _install(self, formula: str) -> None:
        f = formula.strip()
        if f.startswith("G "):
            inner = f[2:].strip().strip("()")
            self._add_atomic(inner, "always")
        elif f.startswith("F "):
            inner = f[2:].strip().strip("()")
            self._add_atomic(inner, "eventually")
        elif f.startswith("NEVER "):
            inner = f[6:].strip().strip("()")
            self._add_atomic(inner, "never")

    def _add_atomic(self, expr: str, op: str) -> None:
        m = _ATOMIC_RE.match(expr)
        if not m:
            return
        var, cmp, val = m.group(1), m.group(2), float(m.group(3))

        def cond(state, v=var, c=cmp, x=val):
            try:
                sv = float(state.get(v, 0.0))
            except Exception:
                return True
            return {">=": sv >= x, "<=": sv <= x, "==": sv == x,
                    "!=": sv != x, ">": sv > x, "<": sv < x}[c]

        rid = f"{op}:{expr}"
        self.rules[rid] = TemporalRule(rid, op, [cond],
                                        description=expr, severity="warning")
        try:
            asyncio.create_task(asyncio.to_thread(
                self.storage.save_temporal_rule,
                rid, expr, op, "warning", expr, 0.0, True))
        except Exception:
            pass

    def set_approval_callback(self, cb: Callable) -> None:
        self.approval_cb = cb

    async def push_state(self, state: Dict[str, Any]) -> None:
        self.trace.append((datetime.now(timezone.utc), dict(state)))
        await asyncio.to_thread(self.storage.save_temporal_trace, state)

    async def verify(self) -> Dict[str, bool]:
        result = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            violated = rule.evaluate(copy)
            result[rid] = not violated
            if violated:
                rule.violations += 1
                rule.last_violation = datetime.now(timezone.utc)
                await asyncio.to_thread(
                    self.storage.save_temporal_violation,
                    rid, rule.description, len(self.trace) - 1,
                    self.trace[-1][1] if self.trace else {},
                    rule.severity, None)
                if rule.severity == "critical" and self.approval_cb:
                    try:
                        approved = self.approval_cb(rid, self.trace[-1][1])
                        if asyncio.iscoroutine(approved):
                            await approved
                    except Exception:
                        pass
        return result
