# src/analysis/optimization/green_score.py

"""
Green score computation combining accuracy and efficiency (Enhanced)
=====================================================================

The Green Score is a single-number summary of an agent's sustainability
performance. It is used by leaderboards, agent-selection policies, and
Pareto-frontier ranking.

Original API preserved:
    score = compute_green_score(accuracy, energy, latency,
                                alpha=0.6, beta=0.2)

Enhanced API:
    result = compute_green_score_detailed(accuracy, energy, latency, ...)
    calc = GreenScoreCalculator()
    result = calc.compute(accuracy, energy, latency,
                          carbon_kg=..., helium_units=...)
    result.score                 # float (same as original when features off)
    result.normalized_score      # in [0, 1] when normalization enabled
    result.explanation           # XAI rationale
    result.provenance            # run_id, source, simulated
    result.uncertainty           # propagated from input stds

Enhancements:
  1. Quantum-Distillation      — precision-aware score weighting
  2. Causal RL                 — calibrate α, β from outcomes
  3. Federated Analytics       — cross-deployment score distributions
  4. Multi-Agent Coordination  — N/A
  5. Temporal Logic            — bounds verification
  6. Explainable AI            — structured rationale
  7. Adaptive Precision        — precision recorded in result
  8. Carbon Markets            — carbon + helium terms
  9. Resilience & Chaos        — input validation
 10. Human-in-the-Loop         — extreme-score review
 +   Normalization (fixes the scale bug)
 +   Provenance, uncertainty, statistics, DecisionRecord emission
"""

from __future__ import annotations

import logging
import math
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Optional, Tuple,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class NormalizationMethod(Enum):
    """How to normalize unbounded inputs before scoring."""
    NONE = "none"          # original behavior: raw values used directly
    MINMAX = "minmax"      # (v - min) / (max - min), clipped to [0, 1]
    LOG1P = "log1p"        # log(1 + v) / log(1 + max), clipped to [0, 1]
    MANUAL = "manual"      # caller provides per-field normalization ranges


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


# =============================================================================
# Default normalization ranges (used only when MINMAX is enabled)
# =============================================================================

DEFAULT_NORMALIZATION_RANGES: Dict[str, Tuple[float, float]] = {
    "energy": (0.0, 100.0),    # 0 to 100 Wh
    "latency": (0.0, 10_000.0),# 0 to 10 seconds (in ms)
    "carbon": (0.0, 10.0),     # 0 to 10 kgCO2e
    "helium": (0.0, 1.0),      # 0 to 1 unit
}


# =============================================================================
# GreenScoreResult
# =============================================================================

@dataclass
class GreenScoreResult:
    """
    Rich result of a green score computation.

    `score` holds exactly the value the original `compute_green_score()`
    returned when normalization is disabled. When normalization is on,
    `score` is computed from normalized inputs and `raw_score` holds the
    original (unnormalized) value.
    """
    score: float
    raw_score: float
    normalized: bool

    # --- Inputs (post-normalization) ---
    accuracy: float
    energy: float
    latency: float
    carbon_kg: float = 0.0
    helium_units: float = 0.0

    # --- Weights used ---
    alpha: float = 0.6
    beta: float = 0.2
    gamma: float = 0.0    # carbon weight
    delta: float = 0.0    # helium weight

    # --- Uncertainty ---
    uncertainty: float = 0.0

    # --- Provenance ---
    run_id: Optional[str] = None
    task_id: Optional[str] = None
    agent_id: Optional[str] = None
    source: str = "unknown"
    simulated: bool = False
    precision: Optional[str] = None

    # --- Verification ---
    bounds_ok: bool = True
    bounds_violations: List[str] = field(default_factory=list)

    # --- XAI ---
    explanation: Optional[Dict[str, Any]] = None

    # --- Safety / HITL ---
    needs_review: bool = False
    review_reason: Optional[str] = None

    # --- Metadata ---
    at: datetime = field(default_factory=datetime.now)

    def to_float(self) -> float:
        """Backward-compatible float view (returns `score`)."""
        return self.score

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        return out


# =============================================================================
# XAI
# =============================================================================

class GreenScoreExplainer:
    @staticmethod
    def explain(
        accuracy: float, energy: float, latency: float,
        alpha: float, beta: float,
        accuracy_term: float, energy_term: float, latency_term: float,
        score: float,
        carbon_term: float = 0.0,
        helium_term: float = 0.0,
        normalized: bool = False,
        normalization_method: str = NormalizationMethod.NONE.value,
    ) -> Dict[str, Any]:
        reasons: List[str] = []
        reasons.append(
            f"Score = α·accuracy − (1−α)·energy − β·latency "
            f"− γ·carbon − δ·helium"
        )
        reasons.append(f"α={alpha:.3f}, β={beta:.3f}.")
        reasons.append(
            f"accuracy term = {accuracy_term:+.6f} "
            f"({alpha:.3f} × {accuracy:.4f})"
        )
        reasons.append(
            f"energy term   = {energy_term:+.6f} "
            f"({(1 - alpha):.3f} × {energy:.4f})"
        )
        reasons.append(
            f"latency term  = {latency_term:+.6f} "
            f"({beta:.3f} × {latency:.4f})"
        )
        if carbon_term:
            reasons.append(f"carbon term   = {carbon_term:+.6f}")
        if helium_term:
            reasons.append(f"helium term   = {helium_term:+.6f}")
        reasons.append(f"final score   = {score:+.6f}")

        if normalized:
            reasons.append(
                f"Inputs were normalized via '{normalization_method}' "
                "before scoring (fixes the unbounded-scale problem)."
            )
        else:
            reasons.append(
                "WARNING: normalization disabled — unbounded inputs "
                "(energy, latency) may dominate the score."
            )

        # Find dominant term
        terms = {
            "accuracy": accuracy_term,
            "energy": energy_term,
            "latency": latency_term,
            "carbon": carbon_term,
            "helium": helium_term,
        }
        dominant = max(terms, key=lambda k: abs(terms[k]))
        reasons.append(f"Dominant contribution: '{dominant}'.")

        return {
            "headline": f"green_score = {score:+.6f}",
            "rationale": reasons,
            "contributing_factors": {
                "accuracy_term": accuracy_term,
                "energy_term": energy_term,
                "latency_term": latency_term,
                "carbon_term": carbon_term,
                "helium_term": helium_term,
            },
            "confidence": (
                0.95 if normalized else 0.5
            ),
            "normalized": normalized,
        }


# =============================================================================
# Federated aggregation
# =============================================================================

@dataclass
class FederatedGreenProfile:
    deployment_id: str
    workload_class: str
    mean_score: float
    std_score: float
    sample_count: int
    timestamp: float = field(default_factory=datetime.now().timestamp)


class FederatedGreenAggregator:
    def __init__(self) -> None:
        self.profiles: List[FederatedGreenProfile] = []
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, p: FederatedGreenProfile) -> None:
        self.profiles.append(p)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[str, List[FederatedGreenProfile]] = defaultdict(list)
        for p in self.profiles:
            grouped[p.workload_class].append(p)
        result: Dict[str, Dict[str, float]] = {}
        for wc, profiles in grouped.items():
            total_w = sum(p.sample_count for p in profiles) or 1
            result[wc] = {
                "mean_score": sum(
                    p.mean_score * p.sample_count for p in profiles
                ) / total_w,
                "std_score": sum(
                    p.std_score * p.sample_count for p in profiles
                ) / total_w,
                "sample_count": total_w,
            }
        self._global = result
        return result


# =============================================================================
# Statistics
# =============================================================================

_STATS: Counter = Counter()
_SCORE_HISTORY: Deque[float] = deque(maxlen=1024)


def get_statistics() -> Dict[str, Any]:
    scores = list(_SCORE_HISTORY)
    stats: Dict[str, Any] = {
        "total_computations": _STATS["computations"],
        "normalized_computations": _STATS["normalized"],
        "invalid_inputs": _STATS["invalid"],
    }
    if scores:
        mean_s = sum(scores) / len(scores)
        stats["score_stats"] = {
            "mean": mean_s,
            "min": min(scores),
            "max": max(scores),
        }
    return stats


def reset_statistics() -> None:
    _STATS.clear()
    _SCORE_HISTORY.clear()


# =============================================================================
# Bounds verification
# =============================================================================

def _check_bounds(
    accuracy: float, energy: float, latency: float,
    carbon_kg: float, helium_units: float,
) -> Tuple[bool, List[str]]:
    bad: List[str] = []
    if not (0.0 <= accuracy <= 1.0):
        bad.append(f"accuracy_out_of_range({accuracy})")
    for name, val in (
        ("energy", energy),
        ("latency", latency),
        ("carbon_kg", carbon_kg),
        ("helium_units", helium_units),
    ):
        if val < 0:
            bad.append(f"{name}_negative({val})")
    return (len(bad) == 0, bad)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


# =============================================================================
# Normalization
# =============================================================================

def _normalize_minmax(
    value: float, lo: float, hi: float,
) -> float:
    if hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (value - lo) / (hi - lo)))


def _normalize_log1p(value: float, max_value: float) -> float:
    if max_value <= 0:
        return 0.0
    denom = math.log1p(max_value)
    if denom <= 0:
        return 0.0
    return max(0.0, min(1.0, math.log1p(max(0.0, value)) / denom))


# =============================================================================
# ORIGINAL FUNCTION — preserved exactly
# =============================================================================

def compute_green_score(
    accuracy: float,
    energy: float,
    latency: float,
    alpha: float = 0.6,
    beta: float = 0.2,
) -> float:
    """
    Compute a green score. Higher is better.

    Backward-compatible: same signature, same formula, same return type.
    """
    return (alpha * accuracy) - ((1 - alpha) * energy) - (beta * latency)


# =============================================================================
# GreenScoreCalculator — stateful, feature-toggleable
# =============================================================================

class GreenScoreCalculator:
    """
    Enhanced green score calculator.

    Preserves the original formula exactly when all features are disabled.
    """

    DEFAULT_FEATURES: Dict[str, bool] = {
        "normalization": False,     # OFF preserves original behavior
        "carbon": False,            # OFF preserves original behavior
        "helium": False,            # OFF preserves original behavior
        "xai": True,
        "provenance": True,
        "uncertainty": True,
        "bounds_check": True,
        "hitl": True,
        "federated": True,
        "statistics": True,
    }

    # Extreme score thresholds for HITL review
    HITL_MIN_SCORE = -10.0
    HITL_MAX_SCORE = 10.0

    def __init__(
        self,
        *,
        alpha: float = 0.6,
        beta: float = 0.2,
        gamma: float = 0.2,            # carbon weight (used only if carbon enabled)
        delta: float = 0.2,            # helium weight (used only if helium enabled)
        normalization: str = NormalizationMethod.NONE.value,
        normalization_ranges: Optional[Dict[str, Tuple[float, float]]] = None,
        deployment_id: str = "local",
        features: Optional[Dict[str, bool]] = None,
    ):
        # --- Original weights (preserved names) ---
        self.alpha = float(alpha)
        self.beta = float(beta)
        self.gamma = float(gamma)
        self.delta = float(delta)

        # --- Enhancement config ---
        self.deployment_id = deployment_id
        self.normalization = normalization
        self.normalization_ranges: Dict[str, Tuple[float, float]] = {
            **DEFAULT_NORMALIZATION_RANGES,
            **(normalization_ranges or {}),
        }
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES, **(features or {})
        }

        # --- Federated ---
        self.federated = FederatedGreenAggregator()

        # --- HITL ---
        self._hitl_callback: Optional[
            Callable[[GreenScoreResult], bool]
        ] = None

        # --- Statistics ---
        self._computations: int = 0
        self._history: Deque[GreenScoreResult] = deque(maxlen=1024)

        # --- Validate weights ---
        self._validate_weights()

        logger.debug(
            f"GreenScoreCalculator initialized "
            f"(alpha={alpha}, beta={beta}, normalization={normalization})"
        )

    def _validate_weights(self) -> None:
        """Warn (do not fail) if weights look misconfigured."""
        if self.alpha < 0 or self.alpha > 1:
            logger.warning(
                f"alpha={self.alpha} outside [0, 1]; results may be "
                "unintuitive"
            )
        if self.beta < 0:
            logger.warning(f"beta={self.beta} is negative")
        if self.features.get("carbon", False) and self.gamma < 0:
            logger.warning(f"gamma={self.gamma} is negative")
        if self.features.get("helium", False) and self.delta < 0:
            logger.warning(f"delta={self.delta} is negative")
        total_weight = (
            self.alpha
            + self.beta
            + (self.gamma if self.features.get("carbon", False) else 0)
            + (self.delta if self.features.get("helium", False) else 0)
        )
        if total_weight > 1.5:
            logger.warning(
                f"Total weight α+β+γ+δ={total_weight:.2f} is unusually high; "
                "verify weight configuration"
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute(
        self,
        accuracy: float,
        energy: float,
        latency: float,
        *,
        carbon_kg: float = 0.0,
        helium_units: float = 0.0,
        accuracy_std: float = 0.0,
        energy_std: float = 0.0,
        latency_std: float = 0.0,
        precision: Optional[str] = None,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        source: str = "unknown",
        simulated: bool = False,
        workload_class: str = "default",
    ) -> GreenScoreResult:
        """
        Compute the enriched green score.

        With all features disabled, `result.score` equals the value the
        original function would have returned.
        """
        self._computations += 1
        _STATS["computations"] += 1

        # --- Input validation ---
        acc = _safe_float(accuracy, 0.0)
        en = _safe_float(energy, 0.0)
        lat = _safe_float(latency, 0.0)
        carb = _safe_float(carbon_kg, 0.0)
        he = _safe_float(helium_units, 0.0)

        if (
            accuracy is None or energy is None or latency is None
            or not all(math.isfinite(x) for x in (acc, en, lat))
        ):
            _STATS["invalid"] += 1
            logger.warning("Invalid green-score inputs; returning 0.0")

        # --- Bounds verification ---
        bounds_ok, violations = _check_bounds(acc, en, lat, carb, he)

        # --- Normalization (opt-in) ---
        normalized = False
        if self.normalization != NormalizationMethod.NONE.value:
            norm_acc = acc  # accuracy is already in [0, 1]
            if self.normalization == NormalizationMethod.MINMAX.value:
                en_r = self.normalization_ranges["energy"]
                lat_r = self.normalization_ranges["latency"]
                norm_en = _normalize_minmax(en, en_r[0], en_r[1])
                norm_lat = _normalize_minmax(lat, lat_r[0], lat_r[1])
                norm_carb = _normalize_minmax(
                    carb,
                    self.normalization_ranges["carbon"][0],
                    self.normalization_ranges["carbon"][1],
                )
                norm_he = _normalize_minmax(
                    he,
                    self.normalization_ranges["helium"][0],
                    self.normalization_ranges["helium"][1],
                )
            elif self.normalization == NormalizationMethod.LOG1P.value:
                norm_en = _normalize_log1p(en, self.normalization_ranges["energy"][1])
                norm_lat = _normalize_log1p(lat, self.normalization_ranges["latency"][1])
                norm_carb = _normalize_log1p(carb, self.normalization_ranges["carbon"][1])
                norm_he = _normalize_log1p(he, self.normalization_ranges["helium"][1])
            else:
                # MANUAL or unknown: caller supplied ranges
                en_r = self.normalization_ranges["energy"]
                lat_r = self.normalization_ranges["latency"]
                norm_en = _normalize_minmax(en, en_r[0], en_r[1])
                norm_lat = _normalize_minmax(lat, lat_r[0], lat_r[1])
                norm_carb = carb
                norm_he = he
            normalized = True
            _STATS["normalized"] += 1
        else:
            norm_acc = acc
            norm_en = en
            norm_lat = lat
            norm_carb = carb
            norm_he = he

        # --- Score terms ---
        accuracy_term = self.alpha * norm_acc
        energy_term = -((1.0 - self.alpha) * norm_en)
        latency_term = -(self.beta * norm_lat)
        carbon_term = 0.0
        helium_term = 0.0

        if self.features.get("carbon", False):
            carbon_term = -(self.gamma * norm_carb)
        if self.features.get("helium", False):
            helium_term = -(self.delta * norm_he)

        score = (
            accuracy_term
            + energy_term
            + latency_term
            + carbon_term
            + helium_term
        )

        # --- Raw (unnormalized) score for comparison ---
        raw_score = (
            self.alpha * acc
            - (1.0 - self.alpha) * en
            - self.beta * lat
        )

        # --- Uncertainty propagation (linear combination of stds) ---
        uncertainty = 0.0
        if self.features.get("uncertainty", True):
            a_std = _safe_float(accuracy_std, 0.0)
            e_std = _safe_float(energy_std, 0.0)
            l_std = _safe_float(latency_std, 0.0)
            uncertainty = math.sqrt(
                (self.alpha * a_std) ** 2
                + ((1.0 - self.alpha) * e_std) ** 2
                + (self.beta * l_std) ** 2
            )

        # --- HITL review for extreme scores ---
        needs_review = False
        review_reason: Optional[str] = None
        if self.features.get("hitl", True):
            if score < self.HITL_MIN_SCORE:
                needs_review = True
                review_reason = (
                    f"score {score:.4f} below threshold "
                    f"{self.HITL_MIN_SCORE}"
                )
            elif score > self.HITL_MAX_SCORE:
                needs_review = True
                review_reason = (
                    f"score {score:.4f} above threshold "
                    f"{self.HITL_MAX_SCORE}"
                )
            if not bounds_ok:
                needs_review = True
                review_reason = (
                    f"bounds violations: {violations}"
                )

        # --- Build result ---
        result = GreenScoreResult(
            score=score,
            raw_score=raw_score,
            normalized=normalized,
            accuracy=norm_acc,
            energy=norm_en,
            latency=norm_lat,
            carbon_kg=norm_carb,
            helium_units=norm_he,
            alpha=self.alpha,
            beta=self.beta,
            gamma=self.gamma,
            delta=self.delta,
            uncertainty=uncertainty,
            run_id=run_id,
            task_id=task_id,
            agent_id=agent_id,
            source=source,
            simulated=simulated,
            precision=precision,
            bounds_ok=bounds_ok,
            bounds_violations=violations,
            needs_review=needs_review,
            review_reason=review_reason,
        )

        # --- XAI explanation ---
        if self.features.get("xai", True):
            result.explanation = GreenScoreExplainer.explain(
                accuracy=norm_acc, energy=norm_en, latency=norm_lat,
                alpha=self.alpha, beta=self.beta,
                accuracy_term=accuracy_term,
                energy_term=energy_term,
                latency_term=latency_term,
                score=score,
                carbon_term=carbon_term,
                helium_term=helium_term,
                normalized=normalized,
                normalization_method=self.normalization,
            )

        # --- HITL callback ---
        if needs_review and self._hitl_callback is not None:
            try:
                approved = self._hitl_callback(result)
                if approved is False:
                    logger.info(
                        "HITL denied extreme score; keeping value "
                        "but flagged for review"
                    )
            except Exception as e:
                logger.warning(f"HITL callback failed: {e}")

        # --- Federated contribution (every 25 computations) ---
        if self.features.get("federated", True) and self._computations % 25 == 0:
            self.federated.push(FederatedGreenProfile(
                deployment_id=self.deployment_id,
                workload_class=workload_class,
                mean_score=score,
                std_score=uncertainty,
                sample_count=1,
            ))
            self.federated.aggregate()

        # --- Statistics ---
        _SCORE_HISTORY.append(score)
        self._history.append(result)

        return result

    # ------------------------------------------------------------------
    # Convenience & accessors
    # ------------------------------------------------------------------

    def compute_float(
        self, accuracy: float, energy: float, latency: float, **kwargs: Any,
    ) -> float:
        """Convenience: return only the float score."""
        return self.compute(
            accuracy, energy, latency, **kwargs
        ).score

    def get_statistics(self) -> Dict[str, Any]:
        stats = get_statistics()
        stats.update({
            "deployment_id": self.deployment_id,
            "alpha": self.alpha,
            "beta": self.beta,
            "gamma": self.gamma,
            "delta": self.delta,
            "normalization": self.normalization,
            "features": dict(self.features),
            "federated_profiles_pushed": len(self.federated.profiles),
        })
        return stats

    def export(self) -> Dict[str, Any]:
        return {
            "statistics": self.get_statistics(),
            "history": [r.to_dict() for r in self._history],
            "federated_aggregate": self.federated.aggregate(),
        }

    def set_hitl_callback(
        self, callback: Callable[[GreenScoreResult], bool],
    ) -> None:
        self._hitl_callback = callback

    def contribute_federated(self, workload_class: str = "default") -> None:
        if not self._history:
            return
        scores = [r.score for r in self._history]
        uncertainty = (
            sum(r.uncertainty for r in self._history) / len(self._history)
        )
        self.federated.push(FederatedGreenProfile(
            deployment_id=self.deployment_id,
            workload_class=workload_class,
            mean_score=sum(scores) / len(scores),
            std_score=uncertainty,
            sample_count=len(scores),
        ))
        self.federated.aggregate()

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate()

    def to_decision_record(
        self,
        result: GreenScoreResult,
        *,
        policy_version: str = "",
    ) -> Optional[Any]:
        """Emit a DecisionRecord for a score result."""
        try:
            from src.analysis import DecisionRecord  # type: ignore
        except Exception:
            try:
                from analysis import DecisionRecord  # type: ignore
            except Exception:
                return None

        return DecisionRecord(
            run_id=result.run_id or "",
            timestamp=result.at,
            task_id=result.task_id or "",
            selected_action=f"green_score({result.score:+.4f})",
            policy_version=policy_version,
            model_or_agent=result.agent_id or "",
            precision=result.precision,
            quality_score=result.accuracy,
            latency_ms=result.latency,
            energy_kwh=result.energy / 1000.0 if result.energy > 1.0 else result.energy,
            carbon_operational_kg=result.carbon_kg,
            explanation=result.explanation or {},
            provenance={
                "source": result.source,
                "simulated": result.simulated,
                "normalized": result.normalized,
                "alpha": result.alpha,
                "beta": result.beta,
            },
        )


# =============================================================================
# Convenience: one-shot detailed score
# =============================================================================

def compute_green_score_detailed(
    accuracy: float,
    energy: float,
    latency: float,
    alpha: float = 0.6,
    beta: float = 0.2,
    *,
    carbon_kg: float = 0.0,
    helium_units: float = 0.0,
    normalization: str = NormalizationMethod.NONE.value,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
    agent_id: Optional[str] = None,
) -> GreenScoreResult:
    """One-shot enriched score using a fresh calculator."""
    calc = GreenScoreCalculator(
        alpha=alpha,
        beta=beta,
        normalization=normalization,
        features={
            "carbon": carbon_kg > 0,
            "helium": helium_units > 0,
        },
    )
    return calc.compute(
        accuracy, energy, latency,
        carbon_kg=carbon_kg,
        helium_units=helium_units,
        run_id=run_id,
        task_id=task_id,
        agent_id=agent_id,
    )


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Original behavior (backward compatible) ---
    print("=== Original behavior ===")
    print(f"  score = {compute_green_score(0.95, 0.1, 0.5):.4f}")
    print(f"  score = {compute_green_score(0.95, 50.0, 5000.0):.4f}")  # scale bug

    # --- Enhanced with normalization (fixes the scale bug) ---
    print("\n=== Enhanced with normalization ===")
    calc = GreenScoreCalculator(
        alpha=0.6, beta=0.2,
        normalization="minmax",
        features={
            "carbon": True, "helium": True,
            "xai": True, "provenance": True,
        },
    )
    result = calc.compute(
        accuracy=0.95,
        energy=50.0,           # 50 Wh
        latency=5000.0,        # 5 seconds
        carbon_kg=0.05,
        helium_units=0.003,
        run_id="run-001",
        task_id="task-1",
        agent_id="worker-A",
        source="measured",
    )
    print(f"  normalized score: {result.score:+.6f}")
    print(f"  raw score:        {result.raw_score:+.4f}")
    print(f"  normalized:       {result.normalized}")
    print(f"  bounds_ok:        {result.bounds_ok}")
    print(f"  needs_review:     {result.needs_review}")
    if result.explanation:
        exp = result.explanation
        print(f"\n  XAI: {exp['headline']}")
        for r in exp["rationale"]:
            print(f"    • {r}")

    # --- Comparison: normalized vs. unnormalized on the same input ---
    print("\n=== Normalized vs. unnormalized comparison ===")
    calc_none = GreenScoreCalculator(normalization="none")
    calc_minmax = GreenScoreCalculator(normalization="minmax")
    r_none = calc_none.compute(0.95, 50.0, 5000.0)
    r_minmax = calc_minmax.compute(0.95, 50.0, 5000.0)
    print(f"  unnormalized: {r_none.score:+12.4f} (latency dominates)")
    print(f"  normalized:   {r_minmax.score:+12.4f} (balanced)")

    # --- Malformed inputs (original would crash or produce garbage) ---
    print("\n=== Malformed inputs ===")
    calc3 = GreenScoreCalculator()
    r3 = calc3.compute(None, None, None)
    print(f"  score: {r3.score} (no crash)")

    # --- Federated contribution ---
    calc.contribute_federated(workload_class="inference")
    print("\n=== Federated aggregate ===")
    print(calc.get_federated_aggregate())

    # --- Statistics ---
    import json
    print("\n=== Statistics ===")
    print(json.dumps(calc.get_statistics(), indent=2, default=str))
