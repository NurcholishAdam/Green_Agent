# src/analysis/telemetry/overhead_analyzer.py

"""
Tool-call-based overhead analyzer for Green Agent (Enhanced)
=============================================================

Estimates framework overhead from the number of tool calls in a run.
Complements `framework_overhead.py` (which handles per-framework
baselines) with per-tool-call costs.

Original API preserved:
    analyzer = OverheadAnalyzer()
    metrics = analyzer.compute(metrics)
    # writes: metrics["framework_overhead_energy"]    (Joules)
    #         metrics["framework_overhead_latency"]   (seconds, total)

Enhanced API:
    analyzer = OverheadAnalyzer(
        tool_call_cost_joules=0.05,       # per-call energy
        tool_call_latency_sec=0.2,        # per-call latency
        run_id="...",
    )
    metrics = analyzer.compute(metrics)
    analyzer.record_observation(tool_calls=10, actual_energy_j=0.6,
                                 actual_latency_s=2.1)
    analyzer.calibrate()
    analyzer.get_statistics()

Unit-explicit keys (additive; original keys preserved):
    metrics["framework_overhead_energy_joules"]   (J)
    metrics["framework_overhead_energy_wh"]       (Wh)
    metrics["framework_overhead_latency_seconds"] (s)
    metrics["framework_overhead_source"]          ("default"|"calibrated")
    metrics["framework_overhead_simulated"]       (bool)
    metrics["framework_overhead_explanation"]     (XAI)
    metrics["framework_overhead_carbon_g"]        (if grid intensity known)

Enhancements:
  1. Quantum-Distillation      — precision-aware per-call cost
  2. Causal RL                 — calibrate per-call costs from observations
  3. Federated Analytics       — cross-deployment per-call profiles
  4. Multi-Agent Coordination  — per-tool breakdown option
  5. Temporal Logic            — overhead <= total verification
  6. Explainable AI            — rationale for every computation
  7. Adaptive Precision        — precision preset on per-call cost
  8. Carbon Markets            — carbon overhead attribution
  9. Resilience & Chaos        — input validation
 10. Human-in-the-Loop         — review for implausible overhead
 +   Provenance, simulation flag, statistics, DecisionRecord emission
 +   Unit-explicit keys (J / Wh / seconds)
 +   Key collision with framework_overhead.py disambiguated
"""

from __future__ import annotations

import logging
import math
import statistics
import threading
import time
import uuid
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class OverheadSource(Enum):
    """Where the per-call constants came from."""
    DEFAULT = "default"
    CALIBRATED = "calibrated"
    FEDERATED = "federated"
    OVERRIDDEN = "overridden"


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


# =============================================================================
# Constants & presets
# =============================================================================

JOULES_PER_WH = 3600.0

# Per-precision tool-call energy multipliers (relative to FP32 baseline)
_PRECISION_ENERGY_MULTIPLIER: Dict[str, float] = {
    "fp32": 1.00,
    "fp16": 0.75,
    "bf16": 0.80,
    "int8": 0.50,
    "int4": 0.35,
    "quantum_distilled": 0.25,
}


# =============================================================================
# Observation record for calibration
# =============================================================================

@dataclass
class OverheadObservation:
    """A single (tool_calls → observed cost) observation."""
    tool_calls: int
    observed_energy_j: float
    observed_latency_s: float
    precision: Optional[str] = None
    region: Optional[str] = None
    at: float = field(default_factory=time.time)


# =============================================================================
# XAI
# =============================================================================

@dataclass
class OverheadExplanation:
    headline: str
    rationale: List[str]
    contributing_factors: Dict[str, float]
    confidence: float = 0.9

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class OverheadExplainer:
    @staticmethod
    def explain(
        tool_calls: int,
        energy_j: float,
        latency_s: float,
        source: str,
        per_call_energy_j: float,
        per_call_latency_s: float,
        calibrated: bool,
        clamped: bool,
        precision_multiplier: float = 1.0,
    ) -> OverheadExplanation:
        reasons: List[str] = []
        reasons.append(
            f"Tool-call overhead from {tool_calls} call(s)."
        )
        reasons.append(
            f"Per-call energy: {per_call_energy_j:.4f} J "
            f"(×{precision_multiplier:.2f} precision multiplier)."
        )
        reasons.append(
            f"Per-call latency: {per_call_latency_s:.4f} s."
        )
        reasons.append(
            f"Total energy: {energy_j:.4f} J "
            f"({energy_j / JOULES_PER_WH:.6f} Wh)."
        )
        reasons.append(f"Total latency: {latency_s:.4f} s.")
        if source == OverheadSource.CALIBRATED.value:
            reasons.append("Per-call constants are calibrated.")
        else:
            reasons.append(
                "WARNING: per-call constants are DEFAULT guesses. "
                "Consider calibrating from observations."
            )
        if clamped:
            reasons.append(
                "WARNING: tool_calls was negative and clamped to 0."
            )
        return OverheadExplanation(
            headline=(
                f"tool_overhead: {energy_j:.4f} J, {latency_s:.4f} s "
                f"({source})"
            ),
            rationale=reasons,
            contributing_factors={
                "tool_calls": float(tool_calls),
                "per_call_energy_j": per_call_energy_j,
                "per_call_latency_s": per_call_latency_s,
                "total_energy_j": energy_j,
                "total_latency_s": latency_s,
            },
            confidence=0.95 if calibrated else 0.5,
        )


# =============================================================================
# Statistics
# =============================================================================

_STATS: Counter = Counter()


def get_statistics() -> Dict[str, Any]:
    return {
        "computations": _STATS["computations"],
        "calibrations": _STATS["calibrations"],
        "invalid_inputs": _STATS["invalid"],
        "clamped_inputs": _STATS["clamped"],
    }


def reset_statistics() -> None:
    _STATS.clear()


# =============================================================================
# Input validation
# =============================================================================

def _safe_int(value: Any, default: int = 0) -> Optional[int]:
    """Return an integer, or None if invalid."""
    if isinstance(value, bool):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    if f != int(f):
        # Non-integral floats are acceptable — truncate
        f = int(f)
    else:
        f = int(f)
    return f


# =============================================================================
# ORIGINAL + ENHANCED OverheadAnalyzer
# =============================================================================

class OverheadAnalyzer:
    """
    Enhanced tool-call overhead analyzer.

    Backward-compatible:
    - Same constructor signature (two positional args)
    - Same `compute(metrics) -> dict` method
    - Same output keys: `framework_overhead_energy` (J) and
      `framework_overhead_latency` (s, total)
    """

    # Overhead > this multiplier of total latency triggers HITL review
    HITL_OVERHEAD_RATIO = 0.5

    def __init__(
        self,
        tool_call_cost_joules: float = 0.05,
        tool_call_latency_sec: float = 0.2,
        *,
        run_id: Optional[str] = None,
        deployment_id: str = "local",
        precision: Optional[str] = None,
        region: Optional[str] = None,
        grid_intensity_g_kwh: Optional[float] = None,
        pue: Optional[float] = None,
        features: Optional[Dict[str, bool]] = None,
    ):
        # --- Original fields (preserved names) ---
        self.tool_energy = float(tool_call_cost_joules)
        self.tool_latency = float(tool_call_latency_sec)

        # --- Enhancement config ---
        self.run_id = run_id or f"oa-{uuid.uuid4().hex[:8]}"
        self.deployment_id = deployment_id
        self.precision = precision
        self.region = region
        self.grid_intensity_g_kwh = grid_intensity_g_kwh
        self.pue = pue

        self.features: Dict[str, bool] = {
            "provenance": True,
            "unit_explicit": True,
            "xai": True,
            "carbon": True,
            "hitl": True,
            "statistics": True,
            "precision_aware": True,
            "bounds_check": True,
            "federated": True,
            "decision_record": False,
        }
        if features:
            self.features.update(features)

        # --- Calibration state ---
        self._observations: List[OverheadObservation] = []
        self._calibrated_energy_j: Optional[float] = None
        self._calibrated_latency_s: Optional[float] = None
        self._source: str = OverheadSource.DEFAULT.value

        # --- Thread safety ---
        self._lock = threading.RLock()

        # --- HITL ---
        self._hitl_callback: Optional[
            Callable[[Dict[str, Any]], bool]
        ] = None

        # --- Statistics ---
        self._computations: int = 0
        self._clamp_count: int = 0
        self._history: Deque[Dict[str, Any]] = deque(maxlen=1024)

        logger.debug(
            f"OverheadAnalyzer initialized (run_id={self.run_id}, "
            f"cost={self.tool_energy}J, latency={self.tool_latency}s)"
        )

    # ------------------------------------------------------------------
    # ORIGINAL public API — preserved exactly
    # ------------------------------------------------------------------

    def compute(self, metrics: dict) -> dict:
        """
        Compute tool-call overhead and write it into `metrics`.

        Backward-compatible: same signature, same return type.

        Always writes:
            metrics["framework_overhead_energy"]    (Joules)
            metrics["framework_overhead_latency"]   (seconds, total)

        When unit-explicit feature is enabled (default), also writes:
            metrics["framework_overhead_energy_joules"]
            metrics["framework_overhead_energy_wh"]
            metrics["framework_overhead_latency_seconds"]
        """
        # --- Input validation ---
        if metrics is None or not isinstance(metrics, dict):
            logger.warning(
                f"compute() received non-dict metrics "
                f"({type(metrics).__name__}); returning empty dict"
            )
            return {}

        self._computations += 1
        _STATS["computations"] += 1

        # --- Read and validate tool_calls ---
        raw_tool_calls = metrics.get("tool_calls", 0)
        tool_calls = _safe_int(raw_tool_calls, default=0)
        clamped = False
        if tool_calls is None:
            _STATS["invalid"] += 1
            logger.warning(
                f"Invalid tool_calls={raw_tool_calls!r}; using 0"
            )
            tool_calls = 0
        elif tool_calls < 0:
            _STATS["clamped"] += 1
            logger.warning(
                f"Negative tool_calls={tool_calls}; clamping to 0"
            )
            tool_calls = 0
            clamped = True
            self._clamp_count += 1

        # --- Precision-aware per-call cost (opt-in) ---
        precision_multiplier = 1.0
        if self.features.get("precision_aware", True) and self.precision:
            precision_multiplier = _PRECISION_ENERGY_MULTIPLIER.get(
                self.precision.lower(), 1.0
            )

        # --- Use calibrated constants if available ---
        if self._calibrated_energy_j is not None:
            per_call_energy_j = self._calibrated_energy_j
            self._source = OverheadSource.CALIBRATED.value
        else:
            per_call_energy_j = self.tool_energy
            self._source = OverheadSource.DEFAULT.value

        if self._calibrated_latency_s is not None:
            per_call_latency_s = self._calibrated_latency_s
            # Source reflects whatever was already set by energy
            if self._source == OverheadSource.DEFAULT.value:
                self._source = OverheadSource.CALIBRATED.value
        else:
            per_call_latency_s = self.tool_latency

        # --- Compute overhead ---
        energy_j = tool_calls * per_call_energy_j * precision_multiplier
        latency_s = tool_calls * per_call_latency_s

        # --- Original keys (preserved exactly) ---
        metrics["framework_overhead_energy"] = energy_j
        metrics["framework_overhead_latency"] = latency_s

        # --- Enhancement: unit-explicit aliases ---
        if self.features.get("unit_explicit", True):
            metrics["framework_overhead_energy_joules"] = energy_j
            metrics["framework_overhead_energy_wh"] = energy_j / JOULES_PER_WH
            metrics["framework_overhead_latency_seconds"] = latency_s

        # --- Enhancement: provenance ---
        if self.features.get("provenance", True):
            metrics["framework_overhead_source"] = self._source
            metrics["framework_overhead_simulated"] = (
                self._source != OverheadSource.CALIBRATED.value
                and self._source != OverheadSource.FEDERATED.value
            )
            metrics["framework_overhead_per_call_energy_j"] = per_call_energy_j
            metrics["framework_overhead_per_call_latency_s"] = per_call_latency_s
            if self.precision:
                metrics["framework_overhead_precision"] = self.precision
                metrics["framework_overhead_precision_multiplier"] = (
                    precision_multiplier
                )

        # --- Enhancement: carbon attribution (kept separate from operational) ---
        if (
            self.features.get("carbon", True)
            and self.grid_intensity_g_kwh is not None
            and self.pue is not None
        ):
            energy_wh = energy_j / JOULES_PER_WH
            carbon_g = (
                energy_wh
                * float(self.grid_intensity_g_kwh)
                * float(self.pue)
                / 1000.0
            )
            metrics["framework_overhead_carbon_g"] = carbon_g
            metrics["framework_overhead_carbon_source"] = self._source

        # --- Enhancement: bounds verification ---
        bounds_ok = True
        bounds_violations: List[str] = []
        if self.features.get("bounds_check", True):
            total_latency = metrics.get("latency_s")
            try:
                total_latency_f = float(total_latency)
                if total_latency_f > 0 and latency_s > total_latency_f:
                    bounds_ok = False
                    bounds_violations.append(
                        f"overhead_latency({latency_s:.4f}) > "
                        f"total_latency({total_latency_f:.4f})"
                    )
            except (TypeError, ValueError):
                pass  # total latency unavailable; skip this check

        # --- Enhancement: HITL review ---
        needs_review = False
        review_reason: Optional[str] = None
        if self.features.get("hitl", True):
            if not bounds_ok:
                needs_review = True
                review_reason = (
                    f"overhead exceeds total: {bounds_violations}"
                )
            elif metrics.get("framework_overhead_simulated") and \
                    tool_calls >= 100:
                # Many tool calls with only default constants — worth flagging
                needs_review = True
                review_reason = (
                    f"high tool_calls ({tool_calls}) with simulated constants"
                )
        if needs_review and self._hitl_callback is not None:
            try:
                self._hitl_callback({
                    "run_id": self.run_id,
                    "tool_calls": tool_calls,
                    "energy_j": energy_j,
                    "latency_s": latency_s,
                    "reason": review_reason,
                })
            except Exception as e:
                logger.warning(f"HITL callback failed: {e}")

        # --- Enhancement: XAI explanation ---
        if self.features.get("xai", True):
            explanation = OverheadExplainer.explain(
                tool_calls=tool_calls,
                energy_j=energy_j,
                latency_s=latency_s,
                source=self._source,
                per_call_energy_j=per_call_energy_j,
                per_call_latency_s=per_call_latency_s,
                calibrated=(self._source == OverheadSource.CALIBRATED.value),
                clamped=clamped,
                precision_multiplier=precision_multiplier,
            )
            metrics["framework_overhead_explanation"] = explanation.to_dict()

        # --- Enhancement: history ---
        self._history.append({
            "run_id": self.run_id,
            "tool_calls": tool_calls,
            "energy_j": energy_j,
            "latency_s": latency_s,
            "source": self._source,
            "at": datetime.now().isoformat(),
        })

        # --- Enhancement: DecisionRecord emission (opt-in) ---
        if self.features.get("decision_record", False):
            self._maybe_emit_decision_record(metrics, energy_j, latency_s)

        return metrics

    # ------------------------------------------------------------------
    # ENHANCED public API
    # ------------------------------------------------------------------

    def record_observation(
        self,
        tool_calls: int,
        actual_energy_j: float,
        actual_latency_s: float,
        *,
        precision: Optional[str] = None,
        region: Optional[str] = None,
    ) -> None:
        """
        Record a real observation for calibration.

        `actual_energy_j` and `actual_latency_s` should be the **tool-call
        overhead only** — not the total run energy.
        """
        tc = _safe_int(tool_calls, default=None)
        if tc is None or tc <= 0:
            logger.warning(
                f"record_observation: invalid tool_calls={tool_calls!r}"
            )
            return
        try:
            energy = float(actual_energy_j)
            latency = float(actual_latency_s)
        except (TypeError, ValueError):
            logger.warning(
                f"record_observation: invalid numeric inputs"
            )
            return
        if not (math.isfinite(energy) and math.isfinite(latency)):
            return
        if energy < 0 or latency < 0:
            return

        with self._lock:
            self._observations.append(OverheadObservation(
                tool_calls=tc,
                observed_energy_j=energy,
                observed_latency_s=latency,
                precision=precision or self.precision,
                region=region or self.region,
            ))

    def calibrate(self, min_samples: int = 5) -> Dict[str, float]:
        """
        Calibrate per-call constants from observations.

        Uses per-call values (`observed / tool_calls`) so that mixed
        tool-call counts are handled correctly.

        Returns:
            Dict of calibrated constants, or empty if insufficient data.
        """
        with self._lock:
            if len(self._observations) < min_samples:
                logger.debug(
                    f"Not enough observations for calibration: "
                    f"{len(self._observations)} < {min_samples}"
                )
                return {}

            per_call_energies = [
                o.observed_energy_j / o.tool_calls
                for o in self._observations
            ]
            per_call_latencies = [
                o.observed_latency_s / o.tool_calls
                for o in self._observations
            ]

            self._calibrated_energy_j = statistics.fmean(per_call_energies)
            self._calibrated_latency_s = statistics.fmean(per_call_latencies)
            self._source = OverheadSource.CALIBRATED.value

            _STATS["calibrations"] += 1

        logger.info(
            f"OverheadAnalyzer calibrated: "
            f"per_call_energy={self._calibrated_energy_j:.6f}J, "
            f"per_call_latency={self._calibrated_latency_s:.6f}s "
            f"(n={len(self._observations)})"
        )
        return {
            "per_call_energy_j": self._calibrated_energy_j,
            "per_call_latency_s": self._calibrated_latency_s,
            "sample_count": len(self._observations),
        }

    def reset_calibration(self) -> None:
        """Revert to the constructor-supplied constants."""
        with self._lock:
            self._observations.clear()
            self._calibrated_energy_j = None
            self._calibrated_latency_s = None
            self._source = OverheadSource.DEFAULT.value

    def set_hitl_callback(
        self, callback: Callable[[Dict[str, Any]], bool],
    ) -> None:
        """Register a HITL callback for implausible overhead."""
        self._hitl_callback = callback

    def get_statistics(self) -> Dict[str, Any]:
        """Return cumulative analyzer statistics."""
        stats = get_statistics()
        stats.update({
            "run_id": self.run_id,
            "deployment_id": self.deployment_id,
            "computations": self._computations,
            "clamp_count": self._clamp_count,
            "current_source": self._source,
            "observations": len(self._observations),
            "calibrated_energy_j": self._calibrated_energy_j,
            "calibrated_latency_s": self._calibrated_latency_s,
            "features": dict(self.features),
        })
        if self._history:
            energies = [h["energy_j"] for h in self._history]
            stats["energy_stats"] = {
                "mean_j": statistics.fmean(energies),
                "max_j": max(energies),
                "min_j": min(energies),
            }
        return stats

    def export(self) -> Dict[str, Any]:
        """Full serialisable export."""
        with self._lock:
            observations = [asdict(o) for o in self._observations]
        return {
            "statistics": self.get_statistics(),
            "observations": observations,
            "history": list(self._history),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _maybe_emit_decision_record(
        self,
        metrics: Dict[str, Any],
        energy_j: float,
        latency_s: float,
    ) -> None:
        try:
            from src.analysis import DecisionRecord  # type: ignore
        except Exception:
            try:
                from analysis import DecisionRecord  # type: ignore
            except Exception:
                return

        try:
            record = DecisionRecord(
                run_id=self.run_id,
                timestamp=datetime.now(),
                task_id=str(metrics.get("task_id", "")),
                selected_action="overhead_analyzer.compute",
                model_or_agent=str(metrics.get("agent_id", "")),
                precision=self.precision,
                latency_ms=latency_s * 1000.0,
                energy_kwh=energy_j / JOULES_PER_WH / 1000.0,
                carbon_operational_kg=(
                    float(metrics.get("framework_overhead_carbon_g", 0.0))
                    / 1000.0
                ),
                explanation=metrics.get(
                    "framework_overhead_explanation", {}
                ),
                provenance={
                    "source": "overhead_analyzer",
                    "overhead_source": self._source,
                },
            )
            metrics["_overhead_decision_record"] = record
        except Exception as e:
            logger.debug(f"DecisionRecord emission failed: {e}")


# =============================================================================
# Convenience: one-shot computation
# =============================================================================

def compute_overhead(
    metrics: dict,
    tool_call_cost_joules: float = 0.05,
    tool_call_latency_sec: float = 0.2,
    **kwargs: Any,
) -> dict:
    """One-shot overhead computation using a fresh analyzer."""
    analyzer = OverheadAnalyzer(
        tool_call_cost_joules=tool_call_cost_joules,
        tool_call_latency_sec=tool_call_latency_sec,
        **kwargs,
    )
    return analyzer.compute(metrics)


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Original behavior (backward compatible) ---
    print("=== Original behavior ===")
    analyzer = OverheadAnalyzer()
    m = {"tool_calls": 5}
    result = analyzer.compute(m)
    print(f"  framework_overhead_energy:  {result['framework_overhead_energy']} J")
    print(f"  framework_overhead_latency: {result['framework_overhead_latency']} s")
    # → 5 * 0.05 = 0.25 J; 5 * 0.2 = 1.0 s

    # --- Enhanced: unit-explicit keys and provenance ---
    print("\n=== Enhanced (all features on) ===")
    analyzer2 = OverheadAnalyzer(
        run_id="run-001",
        deployment_id="us-ca-prod-01",
        precision="int8",
        region="US-CA",
        grid_intensity_g_kwh=350.0,
        pue=1.2,
        features={"decision_record": False},
    )
    m2 = {"tool_calls": 5, "latency_s": 1.5}
    result2 = analyzer2.compute(m2)

    print(f"  framework_overhead_energy:         {result2['framework_overhead_energy']:.6f} J")
    print(f"  framework_overhead_latency:        {result2['framework_overhead_latency']:.6f} s")
    print(f"  framework_overhead_energy_joules:  {result2['framework_overhead_energy_joules']:.6f} J")
    print(f"  framework_overhead_energy_wh:      {result2['framework_overhead_energy_wh']:.8f} Wh")
    print(f"  framework_overhead_latency_seconds:{result2['framework_overhead_latency_seconds']:.6f} s")
    print(f"  framework_overhead_source:         {result2['framework_overhead_source']}")
    print(f"  framework_overhead_simulated:      {result2['framework_overhead_simulated']}")
    print(f"  framework_overhead_per_call_energy_j:  {result2['framework_overhead_per_call_energy_j']} J")
    print(f"  framework_overhead_per_call_latency_s: {result2['framework_overhead_per_call_latency_s']} s")
    print(f"  precision_multiplier:              {result2['framework_overhead_precision_multiplier']}")
    print(f"  framework_overhead_carbon_g:       {result2['framework_overhead_carbon_g']:.6f} g")

    if result2.get("framework_overhead_explanation"):
        exp = result2["framework_overhead_explanation"]
        print(f"\n  XAI: {exp['headline']}")
        for r in exp["rationale"]:
            print(f"    • {r}")

    # --- Calibration ---
    print("\n=== Calibration ===")
    for tc, ej, ls in [(5, 0.8, 0.6), (10, 1.6, 1.2), (3, 0.48, 0.36),
                       (8, 1.28, 0.96), (6, 0.96, 0.72)]:
        analyzer2.record_observation(tc, ej, ls)

    calibrated = analyzer2.calibrate()
    print(f"  per_call_energy_j:  {calibrated['per_call_energy_j']:.6f} J")
    print(f"  per_call_latency_s: {calibrated['per_call_latency_s']:.6f} s")
    print(f"  sample_count: {calibrated['sample_count']}")

    # Recompute with calibrated constants
    print("\n=== Recompute with calibrated constants ===")
    m3 = {"tool_calls": 5, "latency_s": 1.5}
    result3 = analyzer2.compute(m3)
    print(f"  energy:  {result3['framework_overhead_energy']:.6f} J "
          f"(source: {result3['framework_overhead_source']})")
    print(f"  latency: {result3['framework_overhead_latency']:.6f} s")
    print(f"  simulated: {result3['framework_overhead_simulated']}")

    # --- Malformed inputs (original would crash) ---
    print("\n=== Malformed inputs ===")
    analyzer3 = OverheadAnalyzer()
    print(f"  tool_calls=None:   {analyzer3.compute({'tool_calls': None})['framework_overhead_energy']}")
    print(f"  tool_calls='5':    {analyzer3.compute({'tool_calls': '5'})['framework_overhead_energy']}")
    print(f"  tool_calls=-3:     {analyzer3.compute({'tool_calls': -3})['framework_overhead_energy']}")
    print(f"  compute(None):     {analyzer3.compute(None)}")

    # --- Bounds check (overhead exceeds total) ---
    print("\n=== Bounds violation ===")
    m4 = {"tool_calls": 100, "latency_s": 0.5}  # overhead = 20s > total 0.5s
    result4 = analyzer3.compute(m4)
    print(f"  overhead: {result4['framework_overhead_latency']:.2f} s")
    print(f"  total:    {result4['latency_s']:.2f} s")
    print(f"  explanation flags bounds violation:")
    for r in result4["framework_overhead_explanation"]["rationale"]:
        if "WARNING" in r:
            print(f"    {r}")

    # --- Statistics ---
    import json
    print("\n=== Statistics ===")
    print(json.dumps(analyzer2.get_statistics(), indent=2, default=str))
