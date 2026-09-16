# src/analysis/resilience/chaos.py

"""
Chaos engineering primitives for Green Agent (Enhanced)
========================================================

Provides fault-injection hooks that emit structured ChaosEvents for the
analysis layer's ResilienceAnalyzer. Implements the proposal's Priority #4
("verify that green optimization does not make the system fragile").

Original API preserved:
    metrics = inject_energy_spike(metrics, probability=0.1)
    # mutates metrics["energy"] *= 1.5 and tags metrics["chaos_event"]

Enhanced API:
    metrics = inject_energy_spike(metrics, probability=0.1,
                                  magnitude=1.5, seed=None,
                                  emit_event=True)
    events = get_chaos_history()
    stats  = get_chaos_statistics()

Additional injectors:
    inject_carbon_spike(metrics, ...)
    inject_latency_spike(metrics, ...)
    inject_memory_pressure(metrics, ...)
    inject_grid_intensity_spike(metrics, ...)

ChaosEvent fields align with the analysis layer's ResilienceAnalyzer:
    scenario, injected_fault, magnitude, at, run_id, context
"""

from __future__ import annotations

import logging
import random
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional

from collections import deque

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
    MILD = "mild"          # < 1.5x
    MODERATE = "moderate"  # 1.5x - 3x
    SEVERE = "severe"      # 3x - 10x
    EXTREME = "extreme"    # >= 10x


# =============================================================================
# Structured ChaosEvent
# =============================================================================

@dataclass
class ChaosEvent:
    """
    Structured record of a single injected fault.

    Downstream ResilienceAnalyzer consumes these to compute recovery time,
    degraded-mode rate, safety-violation counts, and carbon cost of recovery.
    """
    event_id: str
    scenario: str            # human-readable scenario name
    injected_fault: str      # FaultKind value
    magnitude: float         # multiplier applied (e.g., 1.5)
    severity: str            # ChaosSeverity value
    at: datetime             # injection timestamp
    run_id: Optional[str] = None
    task_id: Optional[str] = None
    injected: bool = True    # False means the dice didn't fire
    field_modified: Optional[str] = None
    value_before: Optional[float] = None
    value_after: Optional[float] = None
    context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        return out


# =============================================================================
# Global chaos event history (module-level, opt-in)
# =============================================================================

_CHAOS_HISTORY: Deque[ChaosEvent] = deque(maxlen=4096)
_CHAOS_ENABLED: bool = True


def get_chaos_history() -> List[ChaosEvent]:
    """Return all recorded ChaosEvents."""
    return list(_CHAOS_HISTORY)


def clear_chaos_history() -> None:
    """Reset the chaos history (useful for tests)."""
    _CHAOS_HISTORY.clear()


def set_chaos_enabled(enabled: bool) -> None:
    """Global kill switch for chaos injection."""
    global _CHAOS_ENABLED
    _CHAOS_ENABLED = bool(enabled)


def get_chaos_statistics() -> Dict[str, Any]:
    """Aggregate statistics over the chaos history."""
    if not _CHAOS_HISTORY:
        return {"total_events": 0}
    injected = [e for e in _CHAOS_HISTORY if e.injected]
    severities: Dict[str, int] = {}
    faults: Dict[str, int] = {}
    for e in injected:
        severities[e.severity] = severities.get(e.severity, 0) + 1
        faults[e.injected_fault] = faults.get(e.injected_fault, 0) + 1
    return {
        "total_events": len(_CHAOS_HISTORY),
        "injected_events": len(injected),
        "injection_rate": len(injected) / len(_CHAOS_HISTORY),
        "by_severity": severities,
        "by_fault": faults,
    }


# =============================================================================
# Internal helpers
# =============================================================================

def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        v = float(value)
        return v if v == v and v not in (float("inf"), float("-inf")) else default
    except (TypeError, ValueError):
        return default


def _classify_severity(magnitude: float) -> str:
    if magnitude >= 10.0:
        return ChaosSeverity.EXTREME.value
    if magnitude >= 3.0:
        return ChaosSeverity.SEVERE.value
    if magnitude >= 1.5:
        return ChaosSeverity.MODERATE.value
    return ChaosSeverity.MILD.value


def _record_event(event: ChaosEvent) -> None:
    _CHAOS_HISTORY.append(event)


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
    """
    Shared implementation for all spike injectors.

    Preserves the original semantics: mutates `metrics[field]` in place and
    sets `metrics["chaos_event"]` when the dice fires. Emits a ChaosEvent.
    """
    # --- Global kill switch ---
    if not _CHAOS_ENABLED:
        return metrics

    # --- Input validation (original would crash on these) ---
    if metrics is None or not isinstance(metrics, dict):
        logger.warning(
            f"inject_{fault_kind.value}: invalid metrics "
            f"({type(metrics).__name__}); returning unchanged"
        )
        return metrics if metrics is not None else {}

    # --- Probability validation ---
    try:
        p = float(probability)
    except (TypeError, ValueError):
        p = 0.0
    p = max(0.0, min(1.0, p))

    # --- Determine whether to inject (with optional reproducibility) ---
    rng = random.Random(seed) if seed is not None else random
    fired = rng.random() < p

    if not fired:
        if emit_event:
            _record_event(ChaosEvent(
                event_id=uuid.uuid4().hex[:12],
                scenario=scenario or fault_kind.value,
                injected_fault=fault_kind.value,
                magnitude=magnitude,
                severity=_classify_severity(magnitude),
                at=datetime.now(),
                run_id=run_id,
                task_id=task_id,
                injected=False,
            ))
        return metrics

    # --- Read current value (original would KeyError on missing field) ---
    value_before = _safe_float(metrics.get(field, 0.0))
    value_after = value_before * magnitude

    # --- Apply the mutation (original behavior preserved) ---
    metrics[field] = value_after
    # Original tag preserved exactly for backward compatibility
    metrics["chaos_event"] = fault_kind.value
    # Extended tags (additive)
    metrics["chaos_magnitude"] = magnitude
    metrics["chaos_severity"] = _classify_severity(magnitude)
    metrics["chaos_at"] = datetime.now().isoformat()
    if run_id is not None:
        metrics["chaos_run_id"] = run_id
    if task_id is not None:
        metrics["chaos_task_id"] = task_id

    # --- Emit structured event ---
    if emit_event:
        _record_event(ChaosEvent(
            event_id=uuid.uuid4().hex[:12],
            scenario=scenario or fault_kind.value,
            injected_fault=fault_kind.value,
            magnitude=magnitude,
            severity=_classify_severity(magnitude),
            at=datetime.now(),
            run_id=run_id,
            task_id=task_id,
            injected=True,
            field_modified=field,
            value_before=value_before,
            value_after=value_after,
            context={"probability": p, "seed": seed},
        ))

    logger.debug(
        f"Chaos injected: {fault_kind.value} × {magnitude} "
        f"on '{field}' ({value_before:.4f} → {value_after:.4f})"
    )
    return metrics


# =============================================================================
# ORIGINAL FUNCTION — preserved exactly
# =============================================================================

def inject_energy_spike(
    metrics,
    probability=0.1,
    *,
    # --- New optional kwargs (all default to preserve original behavior) ---
    magnitude: float = 1.5,
    seed: Optional[int] = None,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
    scenario: Optional[str] = None,
    emit_event: bool = True,
):
    """
    Inject an energy spike into metrics with the given probability.

    Backward-compatible: `inject_energy_spike(metrics, probability)`
    behaves exactly as the original — mutates `metrics["energy"] *= 1.5`
    and sets `metrics["chaos_event"] = "energy_spike"`.

    Enhanced: `magnitude`, `seed`, `run_id`, `task_id`, and `scenario`
    can be supplied, and a structured ChaosEvent is recorded.
    """
    return _apply_spike(
        metrics=metrics,
        field="energy",
        magnitude=float(magnitude),
        fault_kind=FaultKind.ENERGY_SPIKE,
        probability=probability,
        seed=seed,
        run_id=run_id,
        task_id=task_id,
        scenario=scenario,
        emit_event=emit_event,
    )


# =============================================================================
# Additional injectors (same pattern)
# =============================================================================

def inject_carbon_spike(
    metrics,
    probability=0.1,
    *,
    magnitude: float = 1.5,
    seed: Optional[int] = None,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
    scenario: Optional[str] = None,
    emit_event: bool = True,
):
    """Inject a carbon spike (multiplies `metrics["carbon"]`)."""
    return _apply_spike(
        metrics=metrics,
        field="carbon",
        magnitude=float(magnitude),
        fault_kind=FaultKind.CARBON_SPIKE,
        probability=probability,
        seed=seed,
        run_id=run_id,
        task_id=task_id,
        scenario=scenario,
        emit_event=emit_event,
    )


def inject_latency_spike(
    metrics,
    probability=0.1,
    *,
    magnitude: float = 2.0,
    seed: Optional[int] = None,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
    scenario: Optional[str] = None,
    emit_event: bool = True,
):
    """Inject a latency spike (multiplies `metrics["latency"]`)."""
    return _apply_spike(
        metrics=metrics,
        field="latency",
        magnitude=float(magnitude),
        fault_kind=FaultKind.LATENCY_SPIKE,
        probability=probability,
        seed=seed,
        run_id=run_id,
        task_id=task_id,
        scenario=scenario,
        emit_event=emit_event,
    )


def inject_memory_pressure(
    metrics,
    probability=0.1,
    *,
    magnitude: float = 1.8,
    seed: Optional[int] = None,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
    scenario: Optional[str] = None,
    emit_event: bool = True,
):
    """Inject memory pressure (multiplies `metrics["memory"]`)."""
    return _apply_spike(
        metrics=metrics,
        field="memory",
        magnitude=float(magnitude),
        fault_kind=FaultKind.MEMORY_PRESSURE,
        probability=probability,
        seed=seed,
        run_id=run_id,
        task_id=task_id,
        scenario=scenario,
        emit_event=emit_event,
    )


def inject_grid_intensity_spike(
    metrics,
    probability=0.1,
    *,
    magnitude: float = 1.5,
    seed: Optional[int] = None,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
    scenario: Optional[str] = None,
    emit_event: bool = True,
):
    """
    Inject a grid-intensity spike (multiplies `metrics["grid_intensity"]`).

    Useful for testing whether the carbon estimator and eco-mode controller
    react correctly to a sudden grid dirtiness.
    """
    return _apply_spike(
        metrics=metrics,
        field="grid_intensity",
        magnitude=float(magnitude),
        fault_kind=FaultKind.GRID_INTENSITY_SPIKE,
        probability=probability,
        seed=seed,
        run_id=run_id,
        task_id=task_id,
        scenario=scenario,
        emit_event=emit_event,
    )


# =============================================================================
# Multi-scenario injector
# =============================================================================

def inject_multi_fault(
    metrics: Dict[str, Any],
    *,
    faults: Optional[Dict[str, float]] = None,
    seed: Optional[int] = None,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Inject multiple fault types in one call.

    `faults` maps fault kind (str) to probability, e.g.:
        {"energy_spike": 0.2, "latency_spike": 0.1}

    Defaults to a single energy spike at probability 0.1 for backward
    compatibility with a naive caller.
    """
    if not faults:
        return inject_energy_spike(
            metrics, probability=0.1,
            seed=seed, run_id=run_id, task_id=task_id,
        )

    injectors = {
        FaultKind.ENERGY_SPIKE.value: inject_energy_spike,
        FaultKind.CARBON_SPIKE.value: inject_carbon_spike,
        FaultKind.LATENCY_SPIKE.value: inject_latency_spike,
        FaultKind.MEMORY_PRESSURE.value: inject_memory_pressure,
        FaultKind.GRID_INTENSITY_SPIKE.value: inject_grid_intensity_spike,
    }
    # Deterministic seed per fault for reproducibility
    for i, (kind, prob) in enumerate(faults.items()):
        injector = injectors.get(kind)
        if injector is None:
            logger.warning(f"Unknown fault kind '{kind}'; skipping")
            continue
        sub_seed = None if seed is None else seed + i
        injector(
            metrics, probability=prob,
            seed=sub_seed, run_id=run_id, task_id=task_id,
        )
    return metrics


# =============================================================================
# ResilienceAnalyzer-friendly helpers
# =============================================================================

def chaos_events_as_resilience_events(
    events: Optional[List[ChaosEvent]] = None,
) -> List[Dict[str, Any]]:
    """
    Convert ChaosEvents into dicts compatible with the analysis layer's
    ResilienceAnalyzer expectations (scenario, injected_fault, recovery_time_ms,
    degraded_mode_entered, safety_violations, carbon_cost_of_recovery_kg).

    Recovery and degraded-mode fields are left at 0/False; the caller is
    expected to fill them after observing system behavior.
    """
    events = events if events is not None else get_chaos_history()
    out: List[Dict[str, Any]] = []
    for e in events:
        if not e.injected:
            continue
        out.append({
            "scenario": e.scenario,
            "injected_fault": e.injected_fault,
            "severity": e.severity,
            "magnitude": e.magnitude,
            "at": e.at.isoformat(),
            "recovery_time_ms": 0.0,          # to be filled by caller
            "degraded_mode_entered": False,   # to be filled by caller
            "safety_violations": 0,           # to be filled by caller
            "carbon_cost_of_recovery_kg": 0.0,# to be filled by caller
        })
    return out


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Original behavior (backward compatible) ---
    print("=== Original behavior ===")
    import random as _r
    _r.seed(42)
    for i in range(5):
        m = {"energy": 100.0}
        inject_energy_spike(m, probability=0.5)
        print(f"  run {i}: energy={m['energy']}, "
              f"chaos_event={m.get('chaos_event')}")

    # --- Enhanced behavior ---
    print("\n=== Enhanced (seeded, structured events) ===")
    clear_chaos_history()
    for i in range(5):
        m = {"energy": 100.0, "carbon": 0.05, "latency": 20.0}
        inject_multi_fault(
            m,
            faults={"energy_spike": 0.4, "latency_spike": 0.2},
            seed=42 + i,
            run_id="run-001",
            task_id=f"task-{i}",
        )
        print(f"  run {i}: energy={m['energy']:.2f}, "
              f"latency={m['latency']:.2f}, "
              f"chaos={m.get('chaos_event')}")

    # --- Reproducibility check ---
    print("\n=== Reproducibility (same seed → same result) ===")
    for seed in (7, 7, 8):
        m = {"energy": 100.0}
        inject_energy_spike(m, probability=0.5, seed=seed)
        print(f"  seed={seed}: energy={m['energy']}")

    # --- Statistics ---
    import json
    print("\n=== Chaos statistics ===")
    print(json.dumps(get_chaos_statistics(), indent=2))

    # --- ResilienceAnalyzer bridge ---
    print("\n=== Resilience events for analysis ===")
    for re in chaos_events_as_resilience_events():
        print(f"  {re['scenario']} | {re['severity']} | "
              f"magnitude={re['magnitude']}")

    # --- Missing-field safety (original would crash) ---
    print("\n=== Missing-field safety ===")
    m = {"latency": 20.0}  # no "energy"
    result = inject_energy_spike(m, probability=1.0)
    print(f"  No crash. energy={result.get('energy')} "
          f"(default 0.0 used)")
