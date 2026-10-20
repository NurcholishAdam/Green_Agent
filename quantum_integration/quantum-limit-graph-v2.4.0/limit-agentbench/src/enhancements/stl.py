"""
stl.py — Temporal logic monitor and safety shield (Enhancement 5, enhanced).

Implements past-time G (always), F (eventually), and U (until) over a
rolling window, plus a fail-closed SafetyShield.

Enhancements over the initial version
--------------------------------------
Bugs fixed
    B1.  Correct empty-window semantics:
            G(φ) on empty → True  (vacuously true)
            F(φ) on empty → False (nothing satisfied)
            φ U ψ on empty → False (nothing satisfied)
         Previously all three returned True, letting the monitor claim
         a "success" it had no evidence for.
    B2.  add_formula() now rejects formulas whose horizon exceeds the
         monitor's history capacity.  Previously a formula with horizon
         greater than the deque's maxlen would silently evaluate against
         a truncated window — a soundness violation.
    B3.  SafetyShield latches by default.  Once a violation fires, the
         shield stays tripped until reset() — a rolled-out violation no
         longer silently un-trips the shield.  Also fires an optional
         on_violation callback.

Design improvements
    D1.  verify_detailed() exposes per-formula ok | None + reason +
         observations + window_len + violated_at.
    D2.  Configurable fallback: str | Callable[[], Any].
    D3.  on_violation callback fires on every trip (not just the first).
    D4.  reset() on both monitor and shield; clear_violations() on shield.
    D5.  STLFormula.__post_init__ validates inputs.
    D6.  add_formula() returns self for chaining.
    D7.  Optional timestamps on observe().
    D8.  Single materialization of history per verify() — O(F) not O(F·H).
    D9.  Bounded-window U is explicitly documented.
    D10. on_evaluation callback on the monitor for metrics.

Migration notes
---------------
* verify() now maps None (insufficient data) → False (fail-closed).
* F/U on empty window return False (see B1).  Callers who relied on the
  old vacuous-true behaviour should add a startup gate before invoking
  the monitor, or use G formulas (which are still vacuously true).
* STLFormula now has an optional `min_observations` field (default 1).
  Set it higher to require N observations before evaluating.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union


# =============================================================================
# Operator / formula / detail
# =============================================================================
class STLOperator(str, Enum):
    ALWAYS = "G"
    EVENTUALLY = "F"
    UNTIL = "U"


@dataclass
class STLFormula:
    """
    A single past-time STL formula.

    predicate         — φ (the invariant / event / premise)
    until_predicate   — ψ (only used for U; defaults to predicate)
    horizon           — how many recent observations to evaluate against
    min_observations  — require at least this many observations before
                        returning a definitive verdict.  Below the threshold
                        the monitor returns ok=None (insufficient_data).
                        Default 1 (evaluate as soon as any observation
                        exists; empty window uses vacuous semantics).
    """

    name: str
    predicate: Callable[[Dict[str, Any]], bool]
    operator: STLOperator
    horizon: int = 10
    until_predicate: Optional[Callable[[Dict[str, Any]], bool]] = None
    min_observations: int = 1

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("STLFormula.name must be non-empty")
        if not callable(self.predicate):
            raise ValueError("STLFormula.predicate must be callable")
        if self.horizon <= 0:
            raise ValueError(
                f"STLFormula '{self.name}': horizon must be >= 1 "
                f"(got {self.horizon})")
        if self.min_observations < 0:
            raise ValueError(
                f"STLFormula '{self.name}': min_observations must be >= 0")
        if self.until_predicate is not None and not callable(self.until_predicate):
            raise ValueError(
                f"STLFormula '{self.name}': until_predicate must be callable "
                f"or None")

    def __repr__(self) -> str:
        return (f"STLFormula(name={self.name!r}, op={self.operator.value}, "
                f"horizon={self.horizon}, min_obs={self.min_observations})")


@dataclass(frozen=True)
class VerifyDetail:
    """
    Full result of evaluating one formula.

    ok              — True (holds), False (fails), or None (insufficient
                      data — the caller decides whether to treat as failure)
    reason          — human-readable explanation
    observations    — how many observations were in the window
    window_len      — the formula's declared horizon
    violated_at     — for G: index of the first failing observation;
                      for U: index of the first φ-failure before ψ;
                      for F: always None (F fails by exhaustion, not
                      at a specific index)
    """

    ok: Optional[bool]
    reason: str
    observations: int
    window_len: int
    violated_at: Optional[int] = None


# =============================================================================
# Monitor
# =============================================================================
class TemporalLogicMonitor:
    """
    Past-time LTL monitor over a rolling window.

    Semantics (past-time, bounded window of length h = formula.horizon):

        G[0,h) φ    φ holds at every observation in the window
        F[0,h) φ    φ holds at some observation in the window
        φ U[0,h) ψ  ∃ i < h: ψ(obs_i) ∧ ∀ j<i: φ(obs_j)

    Empty-window semantics (correct LTL):
        G(φ) on empty → True   (vacuously true)
        F(φ) on empty → False
        φ U ψ on empty → False

    Bounded-window caveat for U: an event older than h observations is not
    visible to the monitor.  If you need unbounded U, use a large horizon or
    accumulate a latch externally (SafetyShield already latches).

    Example:
        m = TemporalLogicMonitor(horizon=10)
        m.add_formula(STLFormula(
            name="quality_floor",
            predicate=lambda r: r["quality"] >= 0.5,
            operator=STLOperator.ALWAYS,
            horizon=5,
        ))
        m.observe({"quality": 0.8})
        assert m.verify()["quality_floor"] is True
    """

    def __init__(
        self,
        horizon: int = 10,
        *,
        capacity: Optional[int] = None,
        timestamped: bool = False,
        on_evaluation: Optional[Callable[[str, VerifyDetail], None]] = None,
    ) -> None:
        if horizon <= 0:
            raise ValueError("horizon must be >= 1")
        self.horizon = horizon
        self.capacity = capacity if capacity is not None else max(horizon * 4, 100)
        if self.capacity < horizon:
            raise ValueError(
                f"capacity ({self.capacity}) must be >= horizon ({horizon})")
        self.timestamped = timestamped
        self.on_evaluation = on_evaluation

        self._history: deque = deque(maxlen=self.capacity)
        self._timestamps: deque = deque(maxlen=self.capacity)
        self.formulas: List[STLFormula] = []
        self._evaluations: deque = deque(maxlen=1024)

    # ------------------------------------------------------------------ #
    # Registration
    # ------------------------------------------------------------------ #
    def add_formula(self, f: STLFormula) -> "TemporalLogicMonitor":
        """Register a formula.  Returns self for chaining."""
        if f.horizon > self.capacity:
            raise ValueError(
                f"Formula '{f.name}' has horizon {f.horizon}, which exceeds "
                f"the monitor's history capacity ({self.capacity}). "
                f"Construct the monitor with a larger capacity=, or reduce "
                f"the formula's horizon.")
        self.formulas.append(f)
        return self

    def get_formula(self, name: str) -> Optional[STLFormula]:
        return next((f for f in self.formulas if f.name == name), None)

    def get_formulas(self) -> List[STLFormula]:
        return list(self.formulas)

    def remove_formula(self, name: str) -> bool:
        before = len(self.formulas)
        self.formulas = [f for f in self.formulas if f.name != name]
        return len(self.formulas) != before

    # ------------------------------------------------------------------ #
    # Observation
    # ------------------------------------------------------------------ #
    def observe(
        self,
        record: Dict[str, Any],
        ts: Optional[float] = None,
    ) -> None:
        self._history.append(dict(record))
        if self.timestamped:
            self._timestamps.append(ts if ts is not None else time.time())

    def get_window(self, formula_name: str) -> List[Dict[str, Any]]:
        f = self.get_formula(formula_name)
        if f is None:
            return []
        return list(self._history)[-f.horizon:]

    # ------------------------------------------------------------------ #
    # Verification
    # ------------------------------------------------------------------ #
    def verify(self) -> Dict[str, bool]:
        """
        Fail-closed boolean verdict per formula.

        ok=True  → True
        ok=False → False
        ok=None  → False (insufficient data fails closed)
        """
        return {name: bool(d.ok)
                for name, d in self.verify_detailed().items()}

    def verify_detailed(self) -> Dict[str, VerifyDetail]:
        """Per-formula detailed verdict.  Single history materialization."""
        hist = list(self._history)
        results: Dict[str, VerifyDetail] = {}
        for f in self.formulas:
            detail = self._verify_one(f, hist)
            results[f.name] = detail
            self._evaluations.append({
                "formula": f.name,
                "ok": detail.ok,
                "reason": detail.reason,
                "observations": detail.observations,
                "t": time.time(),
            })
            if self.on_evaluation is not None:
                try:
                    self.on_evaluation(f.name, detail)
                except Exception:
                    pass
        return results

    def _verify_one(
        self, f: STLFormula, hist: List[Dict[str, Any]]
    ) -> VerifyDetail:
        window = hist[-f.horizon:]
        n = len(window)

        # --- Empty window: correct LTL vacuous semantics (B1) --------- #
        if n == 0:
            if f.operator == STLOperator.ALWAYS:
                return VerifyDetail(ok=True, reason="vacuous", observations=0,
                                    window_len=f.horizon)
            return VerifyDetail(ok=False, reason="no_data", observations=0,
                                window_len=f.horizon)

        # --- Insufficient data (opt-in) ------------------------------- #
        if n < f.min_observations:
            return VerifyDetail(ok=None, reason="insufficient_data",
                                observations=n, window_len=f.horizon)

        # --- Normal evaluation ---------------------------------------- #
        if f.operator == STLOperator.ALWAYS:
            for i, r in enumerate(window):
                try:
                    if not f.predicate(r):
                        return VerifyDetail(
                            ok=False, reason=f"violated_at_{i}",
                            observations=n, window_len=f.horizon,
                            violated_at=i)
                except Exception:
                    return VerifyDetail(
                        ok=False, reason=f"predicate_error_at_{i}",
                        observations=n, window_len=f.horizon,
                        violated_at=i)
            return VerifyDetail(ok=True, reason="all_hold",
                                observations=n, window_len=f.horizon)

        if f.operator == STLOperator.EVENTUALLY:
            for i, r in enumerate(window):
                try:
                    if f.predicate(r):
                        return VerifyDetail(
                            ok=True, reason=f"satisfied_at_{i}",
                            observations=n, window_len=f.horizon)
                except Exception:
                    continue
            return VerifyDetail(ok=False, reason="not_satisfied",
                                observations=n, window_len=f.horizon)

        if f.operator == STLOperator.UNTIL:
            until_pred = f.until_predicate or f.predicate
            for i, r in enumerate(window):
                try:
                    if until_pred(r):
                        return VerifyDetail(
                            ok=True, reason=f"satisfied_at_{i}",
                            observations=n, window_len=f.horizon)
                    if not f.predicate(r):
                        return VerifyDetail(
                            ok=False, reason=f"until_premise_violated_at_{i}",
                            observations=n, window_len=f.horizon,
                            violated_at=i)
                except Exception:
                    return VerifyDetail(
                        ok=False, reason=f"predicate_error_at_{i}",
                        observations=n, window_len=f.horizon,
                        violated_at=i)
            return VerifyDetail(ok=False, reason="until_never_satisfied",
                                observations=n, window_len=f.horizon)

        # Unreachable, but safe default.
        return VerifyDetail(ok=True, reason="unknown_operator",
                            observations=n, window_len=f.horizon)

    # ------------------------------------------------------------------ #
    # Introspection / lifecycle
    # ------------------------------------------------------------------ #
    def recent_evaluations(self, k: int = 32) -> List[Dict[str, Any]]:
        return list(self._evaluations)[-k:]

    def get_history(self, k: Optional[int] = None) -> List[Dict[str, Any]]:
        if k is None:
            return list(self._history)
        return list(self._history)[-k:]

    def reset(self) -> None:
        self._history.clear()
        self._timestamps.clear()
        self._evaluations.clear()

    def clear_formulas(self) -> None:
        self.formulas.clear()

    def __len__(self) -> int:
        return len(self._history)

    def __repr__(self) -> str:
        return (f"TemporalLogicMonitor(horizon={self.horizon}, "
                f"capacity={self.capacity}, formulas={len(self.formulas)}, "
                f"observations={len(self._history)})")


# =============================================================================
# Safety shield
# =============================================================================
class SafetyShield:
    """
    Fail-closed screen around a TemporalLogicMonitor.

    Behaviour:
      * Trips on any formula whose ok is False (confirmed violation) OR None
        (insufficient data — fail-closed).
      * Latches by default: once tripped, stays tripped until reset().
      * Fires on_violation for every trip (not just the first).
      * Returns the configured fallback (str or callable) when unsafe.
    """

    def __init__(
        self,
        monitor: TemporalLogicMonitor,
        fallback: Union[str, Callable[[], Any]] = "safe_default",
        *,
        latch: bool = True,
        on_violation: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self.monitor = monitor
        self.fallback = fallback
        self.latch = latch
        self.on_violation = on_violation

        self._latched = False
        self._latched_record: Optional[Dict[str, Any]] = None
        self.violations: deque = deque(maxlen=1024)

    # ------------------------------------------------------------------ #
    # Screening
    # ------------------------------------------------------------------ #
    def _fallback_value(self) -> Any:
        if callable(self.fallback):
            return self.fallback()
        return self.fallback

    def screen(self) -> Tuple[bool, Dict[str, bool], Any]:
        """
        Returns (safe, verdict, fallback_or_None).

        Backwards compatible with the original signature.
        """
        if self.latch and self._latched:
            record = self._latched_record or {}
            return False, dict(record.get("verdict", {})), self._fallback_value()

        detail = self.monitor.verify_detailed()
        verdict = {name: bool(d.ok) if d.ok is not None else False
                   for name, d in detail.items()}

        violated = [n for n, d in detail.items() if d.ok is False]
        uncertain = [n for n, d in detail.items() if d.ok is None]

        if not violated and not uncertain:
            return True, verdict, None

        record = {
            "ts": time.time(),
            "violated": violated,
            "uncertain": uncertain,
            "verdict": dict(verdict),
        }
        self.violations.append(record)
        if self.on_violation is not None:
            try:
                self.on_violation(record)
            except Exception:
                pass
        if self.latch:
            self._latched = True
            self._latched_record = record
        return False, verdict, self._fallback_value()

    def screen_detailed(
        self,
    ) -> Tuple[bool, Dict[str, VerifyDetail], Any]:
        """Like screen(), but returns VerifyDetail per formula."""
        if self.latch and self._latched:
            return False, {}, self._fallback_value()

        detail = self.monitor.verify_detailed()
        violated = [n for n, d in detail.items() if d.ok is False]
        uncertain = [n for n, d in detail.items() if d.ok is None]

        if not violated and not uncertain:
            return True, detail, None

        record = {
            "ts": time.time(),
            "violated": violated,
            "uncertain": uncertain,
            "detail": detail,
        }
        self.violations.append(record)
        if self.on_violation is not None:
            try:
                self.on_violation(record)
            except Exception:
                pass
        if self.latch:
            self._latched = True
            self._latched_record = record
        return False, detail, self._fallback_value()

    # ------------------------------------------------------------------ #
    # Introspection / lifecycle
    # ------------------------------------------------------------------ #
    def is_latched(self) -> bool:
        return self._latched

    def recent_violations(self, k: int = 16) -> List[Dict[str, Any]]:
        return list(self.violations)[-k:]

    def reset(self) -> None:
        """Clear the latch.  Does NOT clear the violation log."""
        self._latched = False
        self._latched_record = None

    def clear_violations(self) -> None:
        self.violations.clear()

    def reset_all(self) -> None:
        self.reset()
        self.clear_violations()
        self.monitor.reset()

    def __repr__(self) -> str:
        return (f"SafetyShield(latch={self.latch}, latched={self._latched}, "
                f"violations={len(self.violations)}, "
                f"fallback={'<callable>' if callable(self.fallback) else self.fallback!r})")


# =============================================================================
# Self-test (run with ``python stl.py``)
# =============================================================================
def _run_unit_tests() -> int:
    failures: List[str] = []

    def check(name: str, cond: bool, detail: str = "") -> None:
        status = "PASS" if cond else "FAIL"
        print(f"  [{status}] {name}" + (f" — {detail}" if detail and not cond else ""))
        if not cond:
            failures.append(name)

    print("=" * 72)
    print("stl.py self-test")
    print("=" * 72)

    def g_formula(name: str, fn, horizon: int = 5) -> STLFormula:
        return STLFormula(name=name, predicate=fn,
                          operator=STLOperator.ALWAYS, horizon=horizon)

    def f_formula(name: str, fn, horizon: int = 5) -> STLFormula:
        return STLFormula(name=name, predicate=fn,
                          operator=STLOperator.EVENTUALLY, horizon=horizon)

    # --- 1. Empty-window semantics (B1) ---------------------------------
    print("\n[1] Empty-window semantics (B1)")
    m_g = TemporalLogicMonitor(horizon=5)
    m_g.add_formula(g_formula("g", lambda r: r["x"] > 0))
    check("G on empty is True (vacuous)", m_g.verify()["g"] is True,
          f"got {m_g.verify()}")

    m_f = TemporalLogicMonitor(horizon=5)
    m_f.add_formula(f_formula("f", lambda r: r["x"] > 0))
    check("F on empty is False", m_f.verify()["f"] is False,
          f"got {m_f.verify()}")

    m_u = TemporalLogicMonitor(horizon=5)
    m_u.add_formula(STLFormula(
        name="u", predicate=lambda r: r["x"] > 0,
        operator=STLOperator.UNTIL,
        until_predicate=lambda r: r["x"] > 10, horizon=5))
    check("U on empty is False", m_u.verify()["u"] is False,
          f"got {m_u.verify()}")

    # --- 2. Formula horizon > capacity is rejected (B2) -----------------
    print("\n[2] Formula horizon > capacity rejected (B2)")
    m = TemporalLogicMonitor(horizon=5, capacity=10)
    rejected = False
    try:
        m.add_formula(STLFormula(
            name="too_far", predicate=lambda r: True,
            operator=STLOperator.ALWAYS, horizon=20))
    except ValueError:
        rejected = True
    check("ValueError raised", rejected)
    check("formula not registered", m.get_formula("too_far") is None)

    # --- 3. Latch (B3) --------------------------------------------------
    print("\n[3] SafetyShield latches on violation (B3)")
    m = TemporalLogicMonitor(horizon=3)
    m.add_formula(g_formula("g", lambda r: r["x"] > 0, horizon=3))
    shield = SafetyShield(m, latch=True)

    m.observe({"x": -1})     # violating observation
    safe, _, _ = shield.screen()
    check("shield tripped on violation", safe is False)
    check("shield latched", shield.is_latched() is True)

    # Even after we roll the violation out of the window...
    m.observe({"x": 1})
    m.observe({"x": 1})
    m.observe({"x": 1})
    m.observe({"x": 1})
    check("monitor now passes", m.verify()["g"] is True)
    safe, _, _ = shield.screen()
    check("shield still tripped (latched)", safe is False)

    # --- 4. Latched reset -----------------------------------------------
    print("\n[4] Latched reset restores safe")
    shield.reset()
    check("not latched", shield.is_latched() is False)
    safe, _, _ = shield.screen()
    check("shield passes after reset", safe is True)

    # --- 5. G/F on populated windows ------------------------------------
    print("\n[5] G/F on populated windows")
    m = TemporalLogicMonitor(horizon=3)
    m.add_formula(g_formula("g", lambda r: r["x"] > 0, horizon=3))
    m.add_formula(f_formula("f", lambda r: r["x"] > 5, horizon=3))
    for x in (1, 2, 3):
        m.observe({"x": x})
    v = m.verify()
    check("G holds for positive x", v["g"] is True)
    check("F does not hold (no x>5)", v["f"] is False)

    m.observe({"x": 10})
    m.observe({"x": 10})
    m.observe({"x": 10})
    v = m.verify()
    check("F holds after x=10 appears", v["f"] is True)

    # --- 6. U semantics -------------------------------------------------
    print("\n[6] U semantics")
    # Case A: φ holds, ψ becomes true → U holds.
    m = TemporalLogicMonitor(horizon=4)
    m.add_formula(STLFormula(
        name="u", predicate=lambda r: r["phi"],
        operator=STLOperator.UNTIL,
        until_predicate=lambda r: r["psi"], horizon=4))
    m.observe({"phi": True, "psi": False})
    m.observe({"phi": True, "psi": False})
    m.observe({"phi": True, "psi": True})
    check("U holds (φ until ψ)", m.verify()["u"] is True)

    # Case B: φ fails before ψ → U fails.
    m2 = TemporalLogicMonitor(horizon=4)
    m2.add_formula(STLFormula(
        name="u", predicate=lambda r: r["phi"],
        operator=STLOperator.UNTIL,
        until_predicate=lambda r: r["psi"], horizon=4))
    m2.observe({"phi": True, "psi": False})
    m2.observe({"phi": False, "psi": False})   # premise violated
    m2.observe({"phi": True, "psi": True})
    check("U fails (premise violated before ψ)", m2.verify()["u"] is False)

    # Case C: ψ never becomes true → U fails.
    m3 = TemporalLogicMonitor(horizon=4)
    m3.add_formula(STLFormula(
        name="u", predicate=lambda r: r["phi"],
        operator=STLOperator.UNTIL,
        until_predicate=lambda r: r["psi"], horizon=4))
    m3.observe({"phi": True, "psi": False})
    m3.observe({"phi": True, "psi": False})
    m3.observe({"phi": True, "psi": False})
    check("U fails (ψ never satisfied)", m3.verify()["u"] is False)

    # --- 7. Callable fallback -------------------------------------------
    print("\n[7] Callable fallback")
    m = TemporalLogicMonitor(horizon=2)
    m.add_formula(g_formula("g", lambda r: r["x"] > 0, horizon=2))
    state = {"safe": False}
    def enter_safe():
        state["safe"] = True
        return "SAFE_STATE"
    shield = SafetyShield(m, fallback=enter_safe, latch=False)
    m.observe({"x": -1})
    safe, _, fallback = shield.screen()
    check("safe is False", safe is False)
    check("fallback returned SAFE_STATE", fallback == "SAFE_STATE")
    check("fallback side-effect ran", state["safe"] is True)

    # --- 8. on_violation callback ---------------------------------------
    print("\n[8] on_violation callback fires")
    m = TemporalLogicMonitor(horizon=2)
    m.add_formula(g_formula("g", lambda r: r["x"] > 0, horizon=2))
    records: List[Dict[str, Any]] = []
    shield = SafetyShield(m, latch=False,
                          on_violation=lambda r: records.append(r))
    m.observe({"x": -1})
    shield.screen()
    m.observe({"x": -1})
    shield.screen()
    check("callback fired twice", len(records) == 2, f"got {len(records)}")
    check("record has violated list", "violated" in records[0])

    # --- 9. verify_detailed / insufficient data -------------------------
    print("\n[9] verify_detailed with min_observations")
    m = TemporalLogicMonitor(horizon=5)
    m.add_formula(STLFormula(
        name="f", predicate=lambda r: r["x"] > 0,
        operator=STLOperator.EVENTUALLY, horizon=5, min_observations=3))
    m.observe({"x": 1})
    detail = m.verify_detailed()["f"]
    check("ok is None (insufficient data)", detail.ok is None,
          f"got ok={detail.ok}")
    check("reason is insufficient_data", detail.reason == "insufficient_data")
    check("observations=1", detail.observations == 1)
    # verify() maps None → False (fail-closed).
    check("verify() fail-closed on None", m.verify()["f"] is False)

    # Add enough observations to hit the threshold.
    m.observe({"x": 1}); m.observe({"x": 1})
    detail = m.verify_detailed()["f"]
    check("ok is True after enough obs", detail.ok is True,
          f"got ok={detail.ok}")

    # --- 10. add_formula chaining + integration -------------------------
    print("\n[10] Chaining + full integration")
    m = TemporalLogicMonitor(horizon=3)
    result = (m.add_formula(g_formula("g1", lambda r: r["x"] > 0, horizon=3))
               .add_formula(g_formula("g2", lambda r: r["x"] < 10, horizon=3)))
    check("add_formula returns self", result is m)
    check("two formulas registered", len(m.get_formulas()) == 2)

    shield = SafetyShield(m, latch=True)
    for x in (1, 2, 3, 4, 5):
        m.observe({"x": x})
    safe, verdict, _ = shield.screen()
    check("shield passes with valid observations", safe is True,
          f"verdict={verdict}")
    check("both formulas passed",
          verdict["g1"] is True and verdict["g2"] is True)

    # --- summary ------------------------------------------------------- #
    print("\n" + "=" * 72)
    if failures:
        print(f"FAILED: {len(failures)} test(s)")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("ALL TESTS PASSED")
    return 0


def _cli() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="stl.py (enhanced)")
    parser.add_argument("--test", action="store_true",
                        help="Run embedded unit tests and exit")
    args = parser.parse_args()
    if args.test:
        return _run_unit_tests()
    print("Usage: python stl.py --test")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(_cli())
