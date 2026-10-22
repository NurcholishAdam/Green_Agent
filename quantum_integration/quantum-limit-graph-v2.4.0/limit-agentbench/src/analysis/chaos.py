#!/usr/bin/env python3
# =============================================================================
# FILE: src/analysis/resilience/chaos.py
# VERSION: 2.0.0
# =============================================================================
"""
Chaos engineering primitives for Green Agent — v2.0.0
=====================================================

Provides fault-injection hooks that emit structured `ChaosEvent`s for the
analysis layer's `ResilienceAnalyzer`. Implements the proposal's Priority #4
("verify that green optimization does not make the system fragile").

v1 API preserved exactly:
    metrics = inject_energy_spike(metrics, probability=0.1)
    # mutates metrics["energy"] *= 1.5 and tags metrics["chaos_event"]

v2 enhancements:
  [P0] Missing-field injection no longer creates phantom keys (was:
       `metrics["energy"] = 0.0` when `energy` was absent). Private
       `_CHAOS_RNG` used when `seed=None` — no longer perturbs the caller's
       global `random` state. `inject_multi_fault` now aggregates fired
       faults into `metrics["chaos_events"]` (list) instead of silently
       overwriting `chaos_event`. Negative metric values are rejected.
       `magnitude` validated: finite, `0 < m <= 100`.
  [P1] `_to_jsonable` used in `to_dict` (JSON-safe context/at); per-metric
       `METRIC_SEVERITY` thresholds (latency spike of 2× is more severe than
       energy spike of 2×); `inject_multi_fault` returns `(metrics, events)`;
       orthogonal sub-seeds via `(seed, i)` tuple; `chaos_session(run_id)`
       context manager; `is_chaos_enabled()` getter; warning when chaos is
       globally disabled but injection was requested; `event_id` included in
       `chaos_events_as_resilience_events` output for recovery correlation.
  [P2] `math.isfinite` in `_safe_float`; lazy `%s` logging; `__repr__` on
       `ChaosEvent`; warning on out-of-range `probability`; `context`
       populated on all events; `_CHAOS_EVICTED` counter; `timezone.utc`
       timestamps; `_self_test()` with 22 assertions.
  [P3] `ChaosRecovery` dataclass + `record_chaos_recovery()` API;
       `check_auto_recovery()` watcher for pending injections; `chaos_session`
       / `chaos_fault` context managers (pytest-friendly); `threading.Lock`
       around shared state; run-tagged session isolation.

**v2.0.0 behavioural changes:**
  - Missing-field injection is now a **no-op** (v1 created a phantom key with
    value 0.0). A non-fired `ChaosEvent` with `context.reason="field_missing"`
    is still recorded for audit.
  - `inject_multi_fault` returns `(metrics, List[ChaosEvent])` instead of
    `metrics` alone. Callers ignoring the return value still work.
  - `inject_multi_fault` additionally populates `metrics["chaos_events"]`
    (list of all fired kinds). `metrics["chaos_event"]` (singular) is still
    set to the last fired fault for backward compatibility.
  - Global `random` state is no longer perturbed when `seed=None`. Pass
    `seed=s` explicitly for determinism, or call `set_chaos_seed(s)` to seed
    the private RNG for an entire test session.
"""

from __future__ import annotations

import logging
import math
import random
import threading
import uuid
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import (Any, Deque, Dict, Iterator, List, Optional, Tuple)

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================
class FaultKind(Enum):
    """The kind of fault injected."""
    ENERGY_SPIKE = "energy_spike"
    CARBON_SPIKE = "carbon_spike"
    LATENCY_SPIKE = "latency_spike"
    MEMORY_PRESSURE = "memory_pressure"
    GRID_INTENSITY_SPIKE = "grid_intensity_spike"


class ChaosSeverity(Enum):
    """Severity classification of the injected fault."""
    MILD = "mild"           # < 1.5× typical
    MODERATE = "moderate"   # 1.5×–3× typical
    SEVERE = "severe"       # 3×–10× typical
    EXTREME = "extreme"     # >= 10× typical


# [P1-2] per-metric severity thresholds (magnitude-relative)
# A 2× latency spike is more disruptive than a 2× energy spike.
METRIC_SEVERITY: Dict[str, Dict[str, float]] = {
    "energy": {"moderate": 1.5, "severe": 3.0, "extreme": 10.0},
    "carbon": {"moderate": 1.5, "severe": 4.0, "extreme": 10.0},
    "latency": {"moderate": 1.3, "severe": 2.5, "extreme": 5.0},
    "memory": {"moderate": 1.5, "severe": 3.0, "extreme": 8.0},
    "grid_intensity": {"moderate": 1.4, "severe": 3.0, "extreme": 8.0},
}
_DEFAULT_SEVERITY_THRESHOLDS = {"moderate": 1.5, "severe": 3.0, "extreme": 10.0}


# [P0-5] magnitude bounds
MIN_MAGNITUDE: float = 1e-3
MAX_MAGNITUDE: float = 100.0


# =============================================================================
# [P1-1] JSON-safe serializer
# =============================================================================
def _to_jsonable(v: Any) -> Any:
    if isinstance(v, dict):
        return {str(k): _to_jsonable(val) for k, val in v.items()}
    if isinstance(v, (list, tuple, set, deque)):
        return [_to_jsonable(val) for val in v]
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, Enum):
        return v.value
    if is_dataclass(v) and not isinstance(v, type):
        return _to_jsonable(asdict(v))
    return v


# =============================================================================
# Structured ChaosEvent
# =============================================================================
@dataclass
class ChaosEvent:
    """
    Structured record of a single injected (or considered) fault.

    Downstream `ResilienceAnalyzer` consumes these to compute recovery time,
    degraded-mode rate, safety-violation counts, and carbon cost of recovery.
    """
    event_id: str
    scenario: str
    injected_fault: str
    magnitude: float
    severity: str
    at: datetime
    run_id: Optional[str] = None
    task_id: Optional[str] = None
    injected: bool = True
    field_modified: Optional[str] = None
    value_before: Optional[float] = None
    value_after: Optional[float] = None
    context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        # [P1-1] JSON-safe; uses _to_jsonable for context / at
        out = _to_jsonable(asdict(self))
        out["at"] = self.at.isoformat()
        return out

    def __repr__(self) -> str:  # [P2-3]
        return (
            f"ChaosEvent(id={self.event_id!r}, fault={self.injected_fault!r}, "
            f"mag={self.magnitude}, sev={self.severity!r}, "
            f"injected={self.injected}, field={self.field_modified!r})"
        )


# =============================================================================
# [P3-1] Recovery record — correlates against injection event
# =============================================================================
@dataclass
class ChaosRecovery:
    """Recovery observation, correlated to an injected `ChaosEvent` by id."""
    event_id: str
    recovered_at: datetime
    recovery_time_ms: float
    degraded_mode_entered: bool = False
    safety_violations: int = 0
    carbon_cost_of_recovery_kg: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        out = _to_jsonable(asdict(self))
        out["recovered_at"] = self.recovered_at.isoformat()
        return out

    def __repr__(self) -> str:
        return (
            f"ChaosRecovery(event_id={self.event_id!r}, "
            f"recovery_ms={self.recovery_time_ms:.1f}, "
            f"degraded={self.degraded_mode_entered})"
        )


# =============================================================================
# Global state (thread-safe) — [P2/P3]
# =============================================================================
_CHAOS_HISTORY: Deque[ChaosEvent] = deque(maxlen=4096)
_CHAOS_RECOVERIES: Deque[ChaosRecovery] = deque(maxlen=4096)
_PENDING_RECOVERY: Dict[str, ChaosEvent] = {}
_CHAOS_ENABLED: bool = True
_CHAOS_EVICTED: int = 0
_CHAOS_LOCK = threading.Lock()
# [P0-2] private RNG — isolate from caller's `random` state
_CHAOS_RNG: random.Random = random.Random()


# =============================================================================
# Public state management — [P1-7/P1-8/P2-8/P3-1]
# =============================================================================
def get_chaos_history() -> List[ChaosEvent]:
    """Return a snapshot of all recorded ChaosEvents."""
    with _CHAOS_LOCK:
        return list(_CHAOS_HISTORY)


def get_pending_recoveries() -> List[ChaosEvent]:
    """Return injected events not yet recovered."""
    with _CHAOS_LOCK:
        return list(_PENDING_RECOVERY.values())


def get_chaos_recoveries() -> List[ChaosRecovery]:
    """Return a snapshot of all recorded ChaosRecoverys."""
    with _CHAOS_LOCK:
        return list(_CHAOS_RECOVERIES)


def clear_chaos_history() -> None:
    """Reset chaos history and pending recoveries (useful for tests)."""
    global _CHAOS_EVICTED
    with _CHAOS_LOCK:
        _CHAOS_HISTORY.clear()
        _CHAOS_RECOVERIES.clear()
        _PENDING_RECOVERY.clear()
        _CHAOS_EVICTED = 0


def set_chaos_enabled(enabled: bool) -> None:
    """Global kill switch for chaos injection."""
    global _CHAOS_ENABLED
    with _CHAOS_LOCK:
        _CHAOS_ENABLED = bool(enabled)


def is_chaos_enabled() -> bool:  # [P1-8]
    """Return whether chaos injection is globally enabled."""
    with _CHAOS_LOCK:
        return _CHAOS_ENABLED


def set_chaos_seed(seed: int) -> None:  # [P0-2]
    """Seed the private chaos RNG. Useful for reproducible test suites."""
    with _CHAOS_LOCK:
        _CHAOS_RNG.seed(seed)


def get_chaos_statistics() -> Dict[str, Any]:
    """Aggregate statistics over the chaos history."""
    with _CHAOS_LOCK:
        history = list(_CHAOS_HISTORY)
        recoveries = list(_CHAOS_RECOVERIES)
        evicted = _CHAOS_EVICTED

    if not history:
        return {"total_events": 0, "evicted_events": evicted}

    injected = [e for e in history if e.injected]
    severities: Dict[str, int] = defaultdict(int)
    faults: Dict[str, int] = defaultdict(int)
    for e in injected:
        severities[e.severity] += 1
        faults[e.injected_fault] += 1

    avg_recovery_ms = (
        sum(r.recovery_time_ms for r in recoveries) / len(recoveries)
        if recoveries else 0.0
    )

    return {
        "total_events": len(history),
        "injected_events": len(injected),
        "injection_rate": len(injected) / len(history),
        "by_severity": dict(severities),
        "by_fault": dict(faults),
        "pending_recoveries": len(_PENDING_RECOVERY),
        "recorded_recoveries": len(recoveries),
        "avg_recovery_ms": avg_recovery_ms,
        "evicted_events": evicted,
    }


# =============================================================================
# Internal helpers — [P0-5, P1-2, P2-1]
# =============================================================================
def _safe_float(value: Any, default: float = 0.0) -> float:
    """[P2-1] Finite float or default. Uses math.isfinite for clarity."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    return v if math.isfinite(v) else default


def _classify_severity(magnitude: float, field: str) -> str:  # [P1-2]
    """Per-metric severity classification."""
    thresholds = METRIC_SEVERITY.get(field, _DEFAULT_SEVERITY_THRESHOLDS)
    if magnitude >= thresholds.get("extreme", _DEFAULT_SEVERITY_THRESHOLDS["extreme"]):
        return ChaosSeverity.EXTREME.value
    if magnitude >= thresholds.get("severe", _DEFAULT_SEVERITY_THRESHOLDS["severe"]):
        return ChaosSeverity.SEVERE.value
    if magnitude >= thresholds.get("moderate", _DEFAULT_SEVERITY_THRESHOLDS["moderate"]):
        return ChaosSeverity.MODERATE.value
    return ChaosSeverity.MILD.value


def _validate_magnitude(magnitude: float) -> float:  # [P0-5]
    try:
        m = float(magnitude)
    except (TypeError, ValueError):
        logger.warning("Invalid magnitude %r; defaulting to 1.5", magnitude)
        return 1.5
    if not math.isfinite(m) or m <= 0.0:
        logger.warning(
            "Magnitude %r outside (0, inf); defaulting to 1.5", magnitude)
        return 1.5
    if m > MAX_MAGNITUDE:
        logger.warning(
            "Magnitude %r > %s; clamping to %s",
            magnitude, MAX_MAGNITUDE, MAX_MAGNITUDE)
        m = MAX_MAGNITUDE
    if m < MIN_MAGNITUDE:
        m = MIN_MAGNITUDE
    return m


def _record_event(event: ChaosEvent) -> None:
    """[P2-8] Bounded append with evicted-count tracking."""
    global _CHAOS_EVICTED
    with _CHAOS_LOCK:
        if len(_CHAOS_HISTORY) == _CHAOS_HISTORY.maxlen:
            _CHAOS_EVICTED += 1
        _CHAOS_HISTORY.append(event)
        if event.injected and event.field_modified is not None:
            _PENDING_RECOVERY[event.event_id] = event


def _pick_rng(seed: Optional[int]) -> random.Random:  # [P0-2]
    """Return a private RNG. Uses module-level _CHAOS_RNG when seed is None
    (isolated from caller's `random` global state)."""
    if seed is not None:
        return random.Random(seed)
    with _CHAOS_LOCK:
        return _CHAOS_RNG


# =============================================================================
# Shared spike implementation
# =============================================================================
def _apply_spike(
    metrics: Dict[str, Any],
    field: str,
    magnitude: float,
    fault_kind: FaultKind,
    probability: float,
    seed: Optional[int],
    run_id: Optional[str],
    task_id: Optional[str],
    scenario: Optional[str],
    emit_event: bool,
) -> Dict[str, Any]:
    """Shared implementation for all spike injectors."""

    # --- Global kill switch [P1-9 warn when disabled] ---
    if not is_chaos_enabled():
        logger.warning(
            "Chaos globally disabled; suppressing injection "
            "(fault=%s, probability=%s)",
            fault_kind.value, probability)
        return metrics

    # --- Input validation ---
    if metrics is None or not isinstance(metrics, dict):
        logger.warning(
            "inject_%s: invalid metrics (%s); returning unchanged",
            fault_kind.value, type(metrics).__name__)
        return metrics if metrics is not None else {}

    # --- Probability validation [P2-4 warn when clamped] ---
    try:
        p = float(probability)
    except (TypeError, ValueError):
        p = 0.0
    if p < 0.0 or p > 1.0:
        logger.warning(
            "probability=%r outside [0, 1]; clamping to %.2f",
            probability, max(0.0, min(1.0, p)))
        p = max(0.0, min(1.0, p))

    # --- Magnitude validation [P0-5] ---
    magnitude = _validate_magnitude(magnitude)

    # --- [P0-1] Missing field: skip injection (no phantom keys) ---
    if field not in metrics:
        logger.debug(
            "inject_%s: field %r missing; skipping (no phantom keys)",
            fault_kind.value, field)
        if emit_event:
            _record_event(ChaosEvent(
                event_id=uuid.uuid4().hex[:12],
                scenario=scenario or fault_kind.value,
                injected_fault=fault_kind.value,
                magnitude=magnitude,
                severity=_classify_severity(magnitude, field),
                at=datetime.now(timezone.utc),
                run_id=run_id, task_id=task_id,
                injected=False,
                field_modified=field,
                context={"probability": p, "seed": seed,
                          "reason": "field_missing"},
            ))
        return metrics

    # --- Read current value ---
    value_before = _safe_float(metrics.get(field, 0.0))

    # --- [P0-4] Reject negative metric value ---
    if value_before < 0.0:
        logger.warning(
            "inject_%s: field %r already negative (%s); "
            "refusing to make it worse",
            fault_kind.value, field, value_before)
        if emit_event:
            _record_event(ChaosEvent(
                event_id=uuid.uuid4().hex[:12],
                scenario=scenario or fault_kind.value,
                injected_fault=fault_kind.value,
                magnitude=magnitude,
                severity=_classify_severity(magnitude, field),
                at=datetime.now(timezone.utc),
                run_id=run_id, task_id=task_id,
                injected=False,
                field_modified=field,
                value_before=value_before,
                context={"probability": p, "seed": seed,
                          "reason": "negative_value"},
            ))
        return metrics

    # --- Determine whether to inject ---
    rng = _pick_rng(seed)  # [P0-2]
    fired = rng.random() < p

    if not fired:
        if emit_event:
            _record_event(ChaosEvent(
                event_id=uuid.uuid4().hex[:12],
                scenario=scenario or fault_kind.value,
                injected_fault=fault_kind.value,
                magnitude=magnitude,
                severity=_classify_severity(magnitude, field),
                at=datetime.now(timezone.utc),
                run_id=run_id, task_id=task_id,
                injected=False,
                field_modified=field,
                value_before=value_before,
                context={"probability": p, "seed": seed},
            ))
        return metrics

    # --- Apply mutation (v1 semantics preserved) ---
    value_after = value_before * magnitude
    metrics[field] = value_after
    metrics["chaos_event"] = fault_kind.value           # v1 exact
    metrics["chaos_magnitude"] = magnitude              # v1 additive
    metrics["chaos_severity"] = _classify_severity(magnitude, field)
    metrics["chaos_at"] = datetime.now(timezone.utc).isoformat()
    if run_id is not None:
        metrics["chaos_run_id"] = run_id
    if task_id is not None:
        metrics["chaos_task_id"] = task_id
    # [P0-3] Accumulate fired faults in a list (never silently overwritten)
    metrics.setdefault("chaos_events", []).append(fault_kind.value)

    # --- Emit structured event ---
    if emit_event:
        _record_event(ChaosEvent(
            event_id=uuid.uuid4().hex[:12],
            scenario=scenario or fault_kind.value,
            injected_fault=fault_kind.value,
            magnitude=magnitude,
            severity=_classify_severity(magnitude, field),
            at=datetime.now(timezone.utc),
            run_id=run_id, task_id=task_id,
            injected=True,
            field_modified=field,
            value_before=value_before,
            value_after=value_after,
            context={"probability": p, "seed": seed},  # [P2-7] always present
        ))

    logger.debug(
        "Chaos injected: %s × %s on %r (%.4f → %.4f)",
        fault_kind.value, magnitude, field, value_before, value_after)
    return metrics


# =============================================================================
# v1 public API — preserved exactly
# =============================================================================
def inject_energy_spike(
    metrics: Dict[str, Any],
    probability: float = 0.1,
    *,
    magnitude: float = 1.5,
    seed: Optional[int] = None,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
    scenario: Optional[str] = None,
    emit_event: bool = True,
) -> Dict[str, Any]:
    """Inject an energy spike. v1-compatible; adds optional kwargs."""
    return _apply_spike(
        metrics=metrics,
        field="energy",
        magnitude=magnitude,
        fault_kind=FaultKind.ENERGY_SPIKE,
        probability=probability,
        seed=seed,
        run_id=run_id,
        task_id=task_id,
        scenario=scenario,
        emit_event=emit_event,
    )


def inject_carbon_spike(
    metrics: Dict[str, Any],
    probability: float = 0.1,
    *,
    magnitude: float = 1.5,
    seed: Optional[int] = None,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
    scenario: Optional[str] = None,
    emit_event: bool = True,
) -> Dict[str, Any]:
    """Inject a carbon spike (multiplies `metrics["carbon"]`)."""
    return _apply_spike(
        metrics=metrics,
        field="carbon",
        magnitude=magnitude,
        fault_kind=FaultKind.CARBON_SPIKE,
        probability=probability,
        seed=seed,
        run_id=run_id,
        task_id=task_id,
        scenario=scenario,
        emit_event=emit_event,
    )


def inject_latency_spike(
    metrics: Dict[str, Any],
    probability: float = 0.1,
    *,
    magnitude: float = 2.0,
    seed: Optional[int] = None,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
    scenario: Optional[str] = None,
    emit_event: bool = True,
) -> Dict[str, Any]:
    """Inject a latency spike (multiplies `metrics["latency"]`)."""
    return _apply_spike(
        metrics=metrics,
        field="latency",
        magnitude=magnitude,
        fault_kind=FaultKind.LATENCY_SPIKE,
        probability=probability,
        seed=seed,
        run_id=run_id,
        task_id=task_id,
        scenario=scenario,
        emit_event=emit_event,
    )


def inject_memory_pressure(
    metrics: Dict[str, Any],
    probability: float = 0.1,
    *,
    magnitude: float = 1.8,
    seed: Optional[int] = None,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
    scenario: Optional[str] = None,
    emit_event: bool = True,
) -> Dict[str, Any]:
    """Inject memory pressure (multiplies `metrics["memory"]`)."""
    return _apply_spike(
        metrics=metrics,
        field="memory",
        magnitude=magnitude,
        fault_kind=FaultKind.MEMORY_PRESSURE,
        probability=probability,
        seed=seed,
        run_id=run_id,
        task_id=task_id,
        scenario=scenario,
        emit_event=emit_event,
    )


def inject_grid_intensity_spike(
    metrics: Dict[str, Any],
    probability: float = 0.1,
    *,
    magnitude: float = 1.5,
    seed: Optional[int] = None,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
    scenario: Optional[str] = None,
    emit_event: bool = True,
) -> Dict[str, Any]:
    """Inject a grid-intensity spike (multiplies `metrics["grid_intensity"]`)."""
    return _apply_spike(
        metrics=metrics,
        field="grid_intensity",
        magnitude=magnitude,
        fault_kind=FaultKind.GRID_INTENSITY_SPIKE,
        probability=probability,
        seed=seed,
        run_id=run_id,
        task_id=task_id,
        scenario=scenario,
        emit_event=emit_event,
    )


# =============================================================================
# Multi-fault injector — [P1-3, P1-4, P1-5] returns (metrics, events)
# =============================================================================
def inject_multi_fault(
    metrics: Dict[str, Any],
    *,
    faults: Optional[Dict[str, float]] = None,
    seed: Optional[int] = None,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> Tuple[Dict[str, Any], List[ChaosEvent]]:
    """
    Inject multiple fault types in one call. Returns `(metrics, events)`
    where `events` are the newly-emitted `ChaosEvent`s from this call.

    `faults` maps fault kind (str) to probability. Examples:
        {"energy_spike": 0.2, "latency_spike": 0.1}

    **[P1-5]** If `faults` is None or empty, defaults to
    `{"energy_spike": 0.1}` for backward compatibility with v1 naive callers.
    Pass `faults={}` explicitly to disable injection.
    """
    if not faults:
        # [P1-5] documented default
        faults = {"energy_spike": 0.1}

    injectors = {
        FaultKind.ENERGY_SPIKE.value: inject_energy_spike,
        FaultKind.CARBON_SPIKE.value: inject_carbon_spike,
        FaultKind.LATENCY_SPIKE.value: inject_latency_spike,
        FaultKind.MEMORY_PRESSURE.value: inject_memory_pressure,
        FaultKind.GRID_INTENSITY_SPIKE.value: inject_grid_intensity_spike,
    }

    # [P1-3] capture new events only
    with _CHAOS_LOCK:
        before_ids = {e.event_id for e in _CHAOS_HISTORY}

    # [P1-4] orthogonal sub-seeds via (seed, i) tuple
    for i, (kind, prob) in enumerate(faults.items()):
        injector = injectors.get(kind)
        if injector is None:
            logger.warning("Unknown fault kind %r; skipping", kind)
            continue
        sub_seed = None if seed is None else hash((seed, i)) % (2 ** 31)
        injector(
            metrics, probability=prob,
            seed=sub_seed, run_id=run_id, task_id=task_id,
        )

    with _CHAOS_LOCK:
        new_events = [e for e in _CHAOS_HISTORY if e.event_id not in before_ids]
    return metrics, new_events


# =============================================================================
# [P3-1] Recovery recording API
# =============================================================================
def record_chaos_recovery(
    event_id: str,
    *,
    degraded_mode_entered: bool = False,
    safety_violations: int = 0,
    carbon_cost_of_recovery_kg: float = 0.0,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[ChaosRecovery]:
    """
    Record recovery for a previously-injected chaos event.

    Called by the caller when the affected metric returns to baseline.
    Returns the `ChaosRecovery` record, or `None` if `event_id` is unknown
    or already recovered.
    """
    with _CHAOS_LOCK:
        event = _PENDING_RECOVERY.pop(event_id, None)
        if event is None:
            logger.debug(
                "record_chaos_recovery: event %r not pending; ignoring",
                event_id)
            return None
        now = datetime.now(timezone.utc)
        delta_ms = (now - event.at).total_seconds() * 1000.0
        rec = ChaosRecovery(
            event_id=event_id,
            recovered_at=now,
            recovery_time_ms=max(0.0, delta_ms),
            degraded_mode_entered=bool(degraded_mode_entered),
            safety_violations=int(safety_violations),
            carbon_cost_of_recovery_kg=float(carbon_cost_of_recovery_kg),
            metadata=dict(metadata or {}),
        )
        _CHAOS_RECOVERIES.append(rec)
    logger.debug(
        "Chaos recovery recorded: %r after %.1f ms",
        event_id, rec.recovery_time_ms)
    return rec


# =============================================================================
# [P3-2] Auto-recovery watcher (opt-in)
# =============================================================================
def check_auto_recovery(
    metrics: Dict[str, Any],
    *,
    tolerance: float = 0.05,
    run_id: Optional[str] = None,
) -> List[ChaosRecovery]:
    """
    Scan `metrics` for injected fields that have returned to within
    `tolerance` of their pre-injection baseline. Records recoveries for
    any matches. Called periodically by the caller after observing system
    behavior.

    This is best-effort: only recovers metrics for `_PENDING_RECOVERY`
    entries whose `field_modified` is currently present in `metrics`.
    """
    if not isinstance(metrics, dict):
        return []
    recovered: List[ChaosRecovery] = []
    with _CHAOS_LOCK:
        pending_snapshot = list(_PENDING_RECOVERY.values())

    for event in pending_snapshot:
        if event.field_modified is None or event.value_before is None:
            continue
        if event.field_modified not in metrics:
            continue
        current = _safe_float(metrics.get(event.field_modified, 0.0))
        baseline = event.value_before
        if baseline == 0.0:
            if abs(current) <= tolerance:
                rec = record_chaos_recovery(event.event_id)
                if rec is not None:
                    recovered.append(rec)
        else:
            if abs(current - baseline) / abs(baseline) <= tolerance:
                rec = record_chaos_recovery(event.event_id)
                if rec is not None:
                    recovered.append(rec)
    return recovered


# =============================================================================
# [P1-12 / P3-3] Session context manager
# =============================================================================
@contextmanager
def chaos_session(run_id: str) -> Iterator[Dict[str, Any]]:
    """
    Isolated chaos context. Clears history and pending recoveries on entry
    (saving the prior state), yields a small dict with `run_id` and
    `events` accessors, then restores prior state on exit.

    Usage:
        with chaos_session("test-001") as sess:
            inject_energy_spike(m, probability=1.0)
            assert len(sess["new_events"]()) == 1
    """
    with _CHAOS_LOCK:
        saved_history = list(_CHAOS_HISTORY)
        saved_recoveries = list(_CHAOS_RECOVERIES)
        saved_pending = dict(_PENDING_RECOVERY)
        _CHAOS_HISTORY.clear()
        _CHAOS_RECOVERIES.clear()
        _PENDING_RECOVERY.clear()

    def _new_events() -> List[ChaosEvent]:
        with _CHAOS_LOCK:
            return [e for e in _CHAOS_HISTORY if e.run_id == run_id]

    def _new_recoveries() -> List[ChaosRecovery]:
        with _CHAOS_LOCK:
            return [r for r in _CHAOS_RECOVERIES
                    if any(e.event_id == r.event_id and e.run_id == run_id
                            for e in _CHAOS_HISTORY)]

    try:
        yield {"run_id": run_id,
                "new_events": _new_events,
                "new_recoveries": _new_recoveries}
    finally:
        with _CHAOS_LOCK:
            _CHAOS_HISTORY.clear()
            _CHAOS_RECOVERIES.clear()
            _PENDING_RECOVERY.clear()
            _CHAOS_HISTORY.extend(saved_history)
            _CHAOS_RECOVERIES.extend(saved_recoveries)
            _PENDING_RECOVERY.update(saved_pending)


@contextmanager
def chaos_fault(
    fault_kind: str,
    *,
    magnitude: float = 1.5,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Deterministic single-fault scope for tests. Sets probability=1.0 and
    seed=0 so the fault always fires predictably.

    Usage:
        with chaos_fault("energy_spike", magnitude=3.0) as ctx:
            ctx["apply"](m)
            assert m["energy"] == 300.0
    """
    injectors = {
        FaultKind.ENERGY_SPIKE.value: inject_energy_spike,
        FaultKind.CARBON_SPIKE.value: inject_carbon_spike,
        FaultKind.LATENCY_SPIKE.value: inject_latency_spike,
        FaultKind.MEMORY_PRESSURE.value: inject_memory_pressure,
        FaultKind.GRID_INTENSITY_SPIKE.value: inject_grid_intensity_spike,
    }
    injector = injectors.get(fault_kind)
    if injector is None:
        raise ValueError(f"Unknown fault kind: {fault_kind!r}")

    def _apply(metrics: Dict[str, Any]) -> Dict[str, Any]:
        return injector(
            metrics, probability=1.0, magnitude=magnitude,
            seed=0, run_id=run_id, task_id=task_id,
        )

    yield {"fault_kind": fault_kind, "apply": _apply}


# =============================================================================
# ResilienceAnalyzer bridge — [P1-6] includes event_id
# =============================================================================
def chaos_events_as_resilience_events(
    events: Optional[List[ChaosEvent]] = None,
) -> List[Dict[str, Any]]:
    """
    Convert ChaosEvents into dicts compatible with ResilienceAnalyzer.
    Joins against `ChaosRecovery` records when available, populating
    recovery fields. `event_id` is included for external correlation.
    """
    events = events if events is not None else get_chaos_history()
    recoveries_by_id = {r.event_id: r for r in get_chaos_recoveries()}

    out: List[Dict[str, Any]] = []
    for e in events:
        if not e.injected:
            continue
        recovery = recoveries_by_id.get(e.event_id)
        out.append({
            "event_id": e.event_id,  # [P1-6]
            "scenario": e.scenario,
            "injected_fault": e.injected_fault,
            "severity": e.severity,
            "magnitude": e.magnitude,
            "at": e.at.isoformat(),
            "recovery_time_ms": (recovery.recovery_time_ms if recovery else 0.0),
            "degraded_mode_entered": (recovery.degraded_mode_entered
                                      if recovery else False),
            "safety_violations": (recovery.safety_violations
                                  if recovery else 0),
            "carbon_cost_of_recovery_kg": (recovery.carbon_cost_of_recovery_kg
                                            if recovery else 0.0),
            "recovered": recovery is not None,
        })
    return out


# =============================================================================
# Self test — [P2-10]
# =============================================================================
def _self_test() -> int:
    failures = 0

    def _check(name: str, fn) -> None:
        nonlocal failures
        try:
            fn()
            print(f"  ✓ {name}")
        except AssertionError as e:
            failures += 1
            print(f"  ✗ {name}: {e}")
        except Exception as e:
            failures += 1
            print(f"  ✗ {name}: {type(e).__name__}: {e}")

    print("=" * 72)
    print("Chaos v2.0.0 — self test")
    print("=" * 72)

    # Reset global state
    clear_chaos_history()
    set_chaos_enabled(True)

    # 1. v1 compatibility
    def _t_v1() -> None:
        m = {"energy": 100.0}
        out = inject_energy_spike(m, probability=1.0, seed=0)
        assert out["energy"] == 150.0
        assert out["chaos_event"] == "energy_spike"
    _check("v1: energy spike mutates in place + tags", _t_v1)

    # 2. [P0-1] Missing field: no phantom keys
    def _t_missing_field() -> None:
        clear_chaos_history()
        m = {"latency": 20.0}
        inject_energy_spike(m, probability=1.0, seed=0)
        assert "energy" not in m, m
        assert "chaos_event" not in m, m
        events = get_chaos_history()
        assert len(events) == 1
        assert events[0].injected is False
        assert events[0].context.get("reason") == "field_missing"
    _check("[P0-1] Missing field: no phantom keys", _t_missing_field)

    # 3. [P0-2] Global RNG untouched
    def _t_rng_isolation() -> None:
        random.seed(12345)
        before = random.getstate()
        m = {"energy": 100.0}
        for _ in range(10):
            inject_energy_spike(m, probability=0.5)  # no seed
        after = random.getstate()
        assert before == after, "chaos polluted global random state"
    _check("[P0-2] Global RNG untouched with seed=None", _t_rng_isolation)

    # 4. [P0-3] Multi-fault doesn't overwrite chaos_event
    def _t_multi_fault_tags() -> None:
        clear_chaos_history()
        m = {"energy": 100.0, "carbon": 0.05}
        inject_multi_fault(
            m, faults={"energy_spike": 1.0, "carbon_spike": 1.0}, seed=0)
        assert "chaos_events" in m
        assert "energy_spike" in m["chaos_events"]
        assert "carbon_spike" in m["chaos_events"]
        # v1 chaos_event is last-wins for backward compat
        assert m["chaos_event"] in ("energy_spike", "carbon_spike")
    _check("[P0-3] Multi-fault preserves all fired tags", _t_multi_fault_tags)

    # 5. [P0-4] Negative metric values rejected
    def _t_negative() -> None:
        clear_chaos_history()
        m = {"energy": -100.0}
        inject_energy_spike(m, probability=1.0, seed=0)
        assert m["energy"] == -100.0  # unchanged
        events = get_chaos_history()
        assert len(events) == 1
        assert events[0].injected is False
        assert events[0].context.get("reason") == "negative_value"
    _check("[P0-4] Negative values rejected", _t_negative)

    # 6. [P0-5] Magnitude validation
    def _t_magnitude() -> None:
        clear_chaos_history()
        # zero magnitude → 1.5 default
        m = {"energy": 100.0}
        inject_energy_spike(m, probability=1.0, magnitude=0.0, seed=0)
        assert m["energy"] == 150.0, m
        # negative magnitude → 1.5 default
        m2 = {"energy": 100.0}
        inject_energy_spike(m2, probability=1.0, magnitude=-3.0, seed=0)
        assert m2["energy"] == 150.0, m2
        # infinite magnitude → 1.5 default
        m3 = {"energy": 100.0}
        inject_energy_spike(m3, probability=1.0,
                              magnitude=float("inf"), seed=0)
        assert m3["energy"] == 150.0, m3
    _check("[P0-5] Invalid magnitudes default to 1.5", _t_magnitude)

    # 7. [P1-1] to_dict JSON-safe
    def _t_json_safe() -> None:
        clear_chaos_history()
        m = {"energy": 100.0}
        inject_energy_spike(m, probability=1.0, seed=0,
                              run_id="r1", scenario="energy-test")
        d = get_chaos_history()[-1].to_dict()
        import json
        json.dumps(d)  # must not raise
        assert isinstance(d["at"], str)
    _check("[P1-1] to_dict JSON-safe", _t_json_safe)

    # 8. [P1-2] Per-metric severity thresholds
    def _t_severity() -> None:
        # 2× energy → MODERATE (>= 1.5)
        assert _classify_severity(2.0, "energy") == ChaosSeverity.MODERATE.value
        # 2× latency → MODERATE (>= 1.3)
        assert _classify_severity(2.0, "latency") == ChaosSeverity.MODERATE.value
        # 2.6× latency → SEVERE (>= 2.5)
        assert _classify_severity(2.6, "latency") == ChaosSeverity.SEVERE.value
        # 2.6× energy → MODERATE (>= 1.5, < 3.0)
        assert _classify_severity(2.6, "energy") == ChaosSeverity.MODERATE.value
    _check("[P1-2] Per-metric severity thresholds", _t_severity)

    # 9. [P1-3] Multi-fault returns new events
    def _t_multi_returns() -> None:
        clear_chaos_history()
        m = {"energy": 100.0, "carbon": 0.05}
        _, events = inject_multi_fault(
            m, faults={"energy_spike": 1.0, "carbon_spike": 1.0}, seed=0)
        assert len(events) == 2, events
        assert all(e.injected for e in events)
    _check("[P1-3] Multi-fault returns new events", _t_multi_returns)

    # 10. [P1-4] Orthogonal sub-seeds
    def _t_orthogonal() -> None:
        clear_chaos_history()
        m1 = {"energy": 100.0, "carbon": 0.05}
        _, e1 = inject_multi_fault(
            m1, faults={"energy_spike": 0.5, "carbon_spike": 0.5}, seed=7)
        clear_chaos_history()
        m2 = {"energy": 100.0, "carbon": 0.05}
        _, e2 = inject_multi_fault(
            m2, faults={"energy_spike": 0.5, "carbon_spike": 0.5}, seed=7)
        # same seed → same events (kind + injected status)
        assert [(e.injected_fault, e.injected) for e in e1] == \
               [(e.injected_fault, e.injected) for e in e2]
    _check("[P1-4] Multi-fault deterministic with seed", _t_orthogonal)

    # 11. [P1-6] Resilience output includes event_id
    def _t_resilience_ids() -> None:
        clear_chaos_history()
        m = {"energy": 100.0}
        inject_energy_spike(m, probability=1.0, seed=0)
        events = chaos_events_as_resilience_events()
        assert len(events) == 1
        assert "event_id" in events[0]
        assert events[0]["recovered"] is False
    _check("[P1-6] Resilience output has event_id", _t_resilience_ids)

    # 12. [P1-7] chaos_session isolates history
    def _t_session() -> None:
        clear_chaos_history()
        m = {"energy": 100.0}
        inject_energy_spike(m, probability=1.0, seed=0)
        outer_count = len(get_chaos_history())
        with chaos_session("test-001") as sess:
            m2 = {"energy": 100.0}
            inject_energy_spike(m2, probability=1.0, seed=0, run_id="test-001")
            inner = sess["new_events"]()
            assert len(inner) == 1
        # outside: history restored (still contains original event)
        after_count = len(get_chaos_history())
        assert after_count == outer_count
    _check("[P1-7] chaos_session isolates history", _t_session)

    # 13. [P1-8] is_chaos_enabled getter
    def _t_getter() -> None:
        set_chaos_enabled(True)
        assert is_chaos_enabled() is True
        set_chaos_enabled(False)
        assert is_chaos_enabled() is False
        set_chaos_enabled(True)
    _check("[P1-8] is_chaos_enabled getter", _t_getter)

    # 14. [P1-9] Warn when chaos disabled
    def _t_disabled_warn() -> None:
        set_chaos_enabled(False)
        m = {"energy": 100.0}
        inject_energy_spike(m, probability=1.0, seed=0)
        assert m["energy"] == 100.0  # not injected
        set_chaos_enabled(True)
    _check("[P1-9] Disabled chaos suppresses injection", _t_disabled_warn)

    # 15. [P2-1] _safe_float handles inf/nan
    def _t_safe_float() -> None:
        assert _safe_float(None) == 0.0
        assert _safe_float("abc") == 0.0
        assert _safe_float(float("inf")) == 0.0
        assert _safe_float(float("nan")) == 0.0
        assert _safe_float(42) == 42.0
    _check("[P2-1] _safe_float rejects inf/nan", _t_safe_float)

    # 16. [P2-4] Probability clamping warns
    def _t_prob_clamp() -> None:
        m = {"energy": 100.0}
        # probability=2.0 → clamps to 1.0
        inject_energy_spike(m, probability=2.0, seed=0)
        assert m["energy"] == 150.0
        m2 = {"energy": 100.0}
        inject_energy_spike(m2, probability=-1.0, seed=0)
        assert m2["energy"] == 100.0
    _check("[P2-4] Probability clamped", _t_prob_clamp)

    # 17. [P2-7] context always present
    def _t_context() -> None:
        clear_chaos_history()
        m = {"energy": 100.0}
        inject_energy_spike(m, probability=0.0, seed=0)  # doesn't fire
        events = get_chaos_history()
        assert len(events) == 1
        assert events[0].context.get("probability") is not None
    _check("[P2-7] context always populated", _t_context)

    # 18. [P2-9] UTC timestamps
    def _t_utc() -> None:
        clear_chaos_history()
        m = {"energy": 100.0}
        inject_energy_spike(m, probability=1.0, seed=0)
        event = get_chaos_history()[-1]
        assert event.at.tzinfo is not None, "timestamp must be timezone-aware"
    _check("[P2-9] UTC timestamps", _t_utc)

    # 19. [P3-1] record_chaos_recovery
    def _t_recovery() -> None:
        clear_chaos_history()
        m = {"energy": 100.0}
        inject_energy_spike(m, probability=1.0, seed=0)
        event = get_chaos_history()[-1]
        assert event.injected
        rec = record_chaos_recovery(
            event.event_id, degraded_mode_entered=True, safety_violations=2,
            carbon_cost_of_recovery_kg=0.5)
        assert rec is not None
        assert rec.event_id == event.event_id
        assert rec.degraded_mode_entered is True
        assert rec.safety_violations == 2
        # second call returns None (already recovered)
        assert record_chaos_recovery(event.event_id) is None
    _check("[P3-1] record_chaos_recovery", _t_recovery)

    # 20. [P3-2] Auto-recovery watcher
    def _t_auto_recovery() -> None:
        clear_chaos_history()
        m = {"energy": 100.0}
        inject_energy_spike(m, probability=1.0, seed=0)
        assert m["energy"] == 150.0
        # Return metric to baseline
        m["energy"] = 100.0
        recovered = check_auto_recovery(m, tolerance=0.05)
        assert len(recovered) == 1
        assert recovered[0].recovery_time_ms >= 0.0
    _check("[P3-2] Auto-recovery watcher", _t_auto_recovery)

    # 21. [P3-3] chaos_fault context manager
    def _t_chaos_fault() -> None:
        clear_chaos_history()
        m = {"energy": 100.0}
        with chaos_fault("energy_spike", magnitude=3.0) as ctx:
            ctx["apply"](m)
        assert m["energy"] == 300.0
    _check("[P3-3] chaos_fault context manager", _t_chaos_fault)

    # 22. Resilience bridge joins recovery
    def _t_resilience_joined() -> None:
        clear_chaos_history()
        m = {"energy": 100.0}
        inject_energy_spike(m, probability=1.0, seed=0)
        event = get_chaos_history()[-1]
        record_chaos_recovery(
            event.event_id, degraded_mode_entered=True, safety_violations=1,
            carbon_cost_of_recovery_kg=0.25)
        events = chaos_events_as_resilience_events()
        assert len(events) == 1
        assert events[0]["recovered"] is True
        assert events[0]["degraded_mode_entered"] is True
        assert events[0]["safety_violations"] == 1
        assert events[0]["carbon_cost_of_recovery_kg"] == 0.25
    _check("[P3] Resilience output joins recovery", _t_resilience_joined)

    print("=" * 72)
    if failures == 0:
        print("✅ All tests passed")
    else:
        print(f"❌ {failures} test(s) failed")
    print("=" * 72)
    return failures


# =============================================================================
# Demo
# =============================================================================
def _demo() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    print("=" * 72)
    print("Chaos v2.0.0 — demo")
    print("=" * 72)

    clear_chaos_history()

    print("\n=== v1 compatibility ===")
    random.seed(42)
    for i in range(5):
        m = {"energy": 100.0}
        inject_energy_spike(m, probability=0.5, seed=i)
        print(f"  run {i}: energy={m['energy']:.1f}, "
              f"chaos={m.get('chaos_event')}")

    print("\n=== [P0-1] Missing field: no phantom keys ===")
    m = {"latency": 20.0}
    inject_energy_spike(m, probability=1.0, seed=0)
    print(f"  metric keys after: {sorted(m.keys())}")
    print(f"  energy present: {'energy' in m}")

    print("\n=== [P0-2] Global RNG untouched ===")
    random.seed(99)
    before = random.random()
    random.seed(99)
    m = {"energy": 100.0}
    for _ in range(20):
        inject_energy_spike(m, probability=0.5)  # no seed
    after = random.random()
    print(f"  random() after chaos: {after:.6f} "
          f"(should equal {before:.6f})")
    print(f"  isolated: {abs(after - before) < 1e-9}")

    print("\n=== [P0-3] Multi-fault aggregation ===")
    clear_chaos_history()
    m = {"energy": 100.0, "carbon": 0.05, "latency": 20.0}
    metrics, events = inject_multi_fault(
        m,
        faults={"energy_spike": 1.0, "carbon_spike": 1.0, "latency_spike": 1.0},
        seed=42, run_id="demo-1")
    print(f"  chaos_event (last):  {metrics.get('chaos_event')}")
    print(f"  chaos_events (all):  {metrics.get('chaos_events')}")
    print(f"  new events: {len(events)}")

    print("\n=== [P1-2] Per-metric severity ===")
    for field, mag in [("energy", 2.0), ("latency", 2.0),
                         ("latency", 3.0), ("energy", 5.0)]:
        sev = _classify_severity(mag, field)
        print(f"  {field:8s} × {mag}: {sev}")

    print("\n=== [P3-1/P3-2] Recovery ===")
    clear_chaos_history()
    m = {"energy": 100.0}
    inject_energy_spike(m, probability=1.0, seed=0, run_id="demo-2")
    event = get_chaos_history()[-1]
    print(f"  injected: energy {event.value_before} → {event.value_after}")
    # Simulate metric returning to baseline
    m["energy"] = 100.0
    recovered = check_auto_recovery(m, tolerance=0.05)
    if recovered:
        print(f"  recovered: {recovered[0].event_id} after "
              f"{recovered[0].recovery_time_ms:.1f}ms")

    print("\n=== Resilience analyzer bridge ===")
    for r in chaos_events_as_resilience_events():
        print(f"  {r['event_id'][:8]}… | {r['scenario']} | "
              f"{r['severity']} | recovered={r['recovered']} | "
              f"recovery_ms={r['recovery_time_ms']:.1f}")

    print("\n=== Statistics ===")
    import json
    print(json.dumps(get_chaos_statistics(), indent=2, default=str))


# =============================================================================
# Entry point
# =============================================================================
if __name__ == "__main__":
    import sys
    if os.environ.get("CHAOS_DEMO") == "1" or "demo" in sys.argv[1:]:
        _demo()
        sys.exit(0)
    sys.exit(_self_test())
