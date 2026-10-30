# metrics/efficiency_calculator.py

"""
Normalized Efficiency Calculator (v5.1)
=======================================

Calculates efficiency metrics normalized by task complexity to enable
fair comparison across tasks of different difficulties.

Fixes in v5.1 (see review)
--------------------------
  * P0 — ``None`` metrics no longer corrupt summaries / baselines. Every
         aggregate (``_mean``, ``benchmark_efficiency_baseline``,
         ``_generate_efficiency_recommendation``) filters ``None`` before
         computing, so a single zero-energy task can no longer turn the
         whole summary into ``nan`` or crash outlier detection.
  * P0 — ``calculate_accuracy_per_watt`` and
         ``calculate_throughput_per_watt`` reject negative ``energy_kwh``.
         Previously negative energy hit the floor branch and returned the
         maximum achievable score. All energy-taking methods are now
         consistent.
  * P0 — ``from_dict`` validates every ``ComparisonSample`` via
         ``__post_init__``, filters unknown keys, and enforces
         ``max_history`` on restore (previously silently overflowed).
  * P0 — ``from_dict`` reconstructs ``NormalizationConfig`` from the set
         of dataclass fields rather than hardcoding each field. Adding a
         field to the config no longer requires a manual edit here.
  * P0 — ``compare_across_complexities`` validates each result dict and
         raises ``EfficiencyCalculatorError`` on missing keys (previously
         leaked ``KeyError``).
  * P1 — ``_validate_finite`` rejects ``bool`` (Python's ``True`` is an
         ``int``; ``accuracy=True`` used to mean ``accuracy=1.0``).
  * P1 — ``calculate_latency_efficiency`` now checks ``0 <= accuracy <= 1``
         (consistent with ``calculate_accuracy_per_watt``) and returns
         ``None`` in *both* strict and non-strict modes when latency is
         below the floor. Previously non-strict mode floored the value
         and reported a finite number for an unmeasurable task.
  * P1 — ``benchmark_efficiency_baseline`` returns a consistent shape
         (``{metric: None}``) on empty input instead of ``{}``.
  * P1 — ``_record_sample`` is skipped for empty result sets (previously
         recorded a phantom ``num_agents=0, avg=0.0`` sample that dragged
         down every subsequent mean).
  * P1 — ``__repr__`` reads ``_history`` under the lock.
  * P1 — ``from_dict`` accepts an optional ``analyzer=`` keyword so
         callers can restore a custom analyzer (which is not serialized).
  * P1 — Robust ``sys.path`` fallback cleaned up in a ``finally`` block
         (previously left the path inserted for the process lifetime).
         Docstring updated to describe what actually happens.
  * P1 — Documented ``epsilon`` vs ``min_energy_kwh`` distinction and the
         ``*_efficiency_ceiling`` calibration range.
  * P2 — New helpers: ``__version__``, ``schema_version`` in
         ``to_dict()``, ``clear_history()``, ``best_agent()`` /
         ``worst_agent()``, ``history_since()`` / ``history_between()``,
         ``set_analyzer()``, ``by_agent`` aggregation in
         ``compare_across_complexities``, deterministic tie-break in
         rankings.
  * P3 — ``logger.info`` demoted to ``logger.debug`` on hot paths; type
         hints tightened (``Dict[str, Any]``); ``math.isfinite`` used
         consistently.

Backward-compatible aliases
---------------------------
    EfficiencyCalculator  →  NormalizedEfficiencyCalculator
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

__version__ = "5.1.0"


# --------------------------------------------------------------------------- #
# Robust import of the analysis module
# --------------------------------------------------------------------------- #
# When the package is importable normally, use it. Otherwise, temporarily
# extend ``sys.path`` so the sibling ``analysis/`` package can be found, and
# remove the extension in a ``finally`` block. The submodule stays cached in
# ``sys.modules`` after import, so subsequent imports still succeed without
# the path being present.
try:  # Preferred: package-relative import
    from analysis.complexity_analyzer import (
        ComplexityAnalyzer,
        TaskComplexity,
    )
except ImportError:  # pragma: no cover — fallback for script-style usage
    import sys

    _analysis_root = str(Path(__file__).resolve().parent.parent)
    _inserted = _analysis_root not in sys.path
    if _inserted:
        sys.path.insert(0, _analysis_root)
    try:
        from analysis.complexity_analyzer import (  # type: ignore
            ComplexityAnalyzer,
            TaskComplexity,
        )
    finally:
        if _inserted:
            try:
                sys.path.remove(_analysis_root)
            except ValueError:  # pragma: no cover — concurrent mutation
                pass


# --------------------------------------------------------------------------- #
# Errors & safe coercion helpers
# --------------------------------------------------------------------------- #
class EfficiencyCalculatorError(ValueError):
    """Raised for invalid inputs or configuration."""


def _safe_float(
    value: Any, *, name: str, default: float = 0.0,
) -> float:
    if value is None:
        return default
    try:
        v = float(value)
    except (TypeError, ValueError) as exc:
        raise EfficiencyCalculatorError(
            f"{name} must be numeric, got {value!r} "
            f"({type(value).__name__})"
        ) from exc
    if not math.isfinite(v):
        raise EfficiencyCalculatorError(
            f"{name} must be finite, got {value!r}"
        )
    return v


def _safe_optional_int(
    value: Any, *, name: str, default: Optional[int] = None,
    min_value: Optional[int] = None,
) -> Optional[int]:
    if value is None:
        return default
    if isinstance(value, bool):
        raise EfficiencyCalculatorError(
            f"{name} must be an integer, got bool {value!r}"
        )
    if isinstance(value, float):
        if not math.isfinite(value):
            raise EfficiencyCalculatorError(
                f"{name} must be finite, got {value!r}"
            )
        if not value.is_integer():
            raise EfficiencyCalculatorError(
                f"{name} must be an integer, got {value!r}"
            )
        result = int(value)
    else:
        try:
            result = int(value)
        except (TypeError, ValueError) as exc:
            raise EfficiencyCalculatorError(
                f"{name} must be an integer, got {value!r} "
                f"({type(value).__name__})"
            ) from exc
    if min_value is not None and result < min_value:
        raise EfficiencyCalculatorError(
            f"{name} must be >= {min_value}, got {result!r}"
        )
    return result


# --------------------------------------------------------------------------- #
# Config & samples
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class NormalizationConfig:
    """
    All tunable constants used for normalization / floors.

    Centralizing these avoids magic numbers scattered through the code and
    lets callers swap in domain-specific calibration without subclassing.

    Notes
    -----
    ``epsilon`` (set on the calculator, not here) guards *task existence*:
    an energy value at or below epsilon means "no meaningful compute
    happened". ``min_energy_kwh`` here is the floor for per-watt
    derivations. The two are separate concepts; epsilon is smaller.

    ``*_efficiency_ceiling`` values define the point at which a metric
    stops contributing to the composite score. The defaults
    (``energy_efficiency_ceiling=0.01``, ``carbon_efficiency_ceiling=0.002``)
    are calibrated for **small** workloads (≈1e-3..1e-2 kWh). For
    LLM-scale fine-tuning, override these to ≈1.0 and ≈0.4 respectively,
    or the energy/carbon components will always normalize to 0.
    """

    # Floors (guard against division by ~0)
    min_complexity_score: float = 0.01
    min_energy_kwh: float = 1e-6
    min_latency_ms: float = 1.0

    # Reference values used in composite normalization
    acc_per_watt_reference: float = 500.0
    energy_efficiency_ceiling: float = 0.01
    carbon_efficiency_ceiling: float = 0.002
    latency_efficiency_reference: float = 10.0

    # Recommendation thresholds (used by ``_generate_efficiency_recommendation``)
    poor_energy_efficiency_threshold: float = 0.01
    poor_accuracy_per_watt_threshold: float = 100.0
    poor_latency_efficiency_threshold: float = 1.0

    # Default weights for the composite score
    default_weights: Dict[str, float] = field(
        default_factory=lambda: {
            "accuracy_per_watt": 0.4,
            "energy_efficiency": 0.3,
            "carbon_efficiency": 0.2,
            "latency_efficiency": 0.1,
        }
    )

    def __post_init__(self) -> None:
        for name in (
            "min_complexity_score", "min_energy_kwh", "min_latency_ms",
        ):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise EfficiencyCalculatorError(
                    f"{name} must be finite and > 0 (got {value!r})."
                )
        total = sum(self.default_weights.values())
        if abs(total - 1.0) > 1e-6:
            raise EfficiencyCalculatorError(
                f"default_weights must sum to 1.0 (got {total:.6f})."
            )


@dataclass(frozen=True)
class ComparisonSample:
    """Immutable snapshot of one ``compare_across_complexities`` run."""

    timestamp: float
    num_agents: int
    best_agent: Optional[str]
    avg_composite_efficiency: float
    label: Optional[str] = None

    def __post_init__(self) -> None:
        if isinstance(self.timestamp, bool) or not isinstance(
            self.timestamp, (int, float)
        ):
            raise EfficiencyCalculatorError(
                f"timestamp must be numeric, got {self.timestamp!r}"
            )
        if not math.isfinite(float(self.timestamp)):
            raise EfficiencyCalculatorError(
                f"timestamp must be finite, got {self.timestamp!r}"
            )
        if isinstance(self.num_agents, bool) or not isinstance(
            self.num_agents, int
        ):
            raise EfficiencyCalculatorError(
                f"num_agents must be an integer, "
                f"got {self.num_agents!r}"
            )
        if self.num_agents < 0:
            raise EfficiencyCalculatorError(
                f"num_agents must be >= 0, got {self.num_agents!r}"
            )
        if isinstance(self.avg_composite_efficiency, bool) or not isinstance(
            self.avg_composite_efficiency, (int, float)
        ):
            raise EfficiencyCalculatorError(
                f"avg_composite_efficiency must be numeric, "
                f"got {self.avg_composite_efficiency!r}"
            )
        if not math.isfinite(float(self.avg_composite_efficiency)):
            raise EfficiencyCalculatorError(
                f"avg_composite_efficiency must be finite, "
                f"got {self.avg_composite_efficiency!r}"
            )
        if self.best_agent is not None and not isinstance(
            self.best_agent, str
        ):
            raise EfficiencyCalculatorError(
                f"best_agent must be str or None, "
                f"got {type(self.best_agent).__name__}"
            )
        if self.label is not None and not isinstance(self.label, str):
            raise EfficiencyCalculatorError(
                f"label must be str or None, "
                f"got {type(self.label).__name__}"
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Main calculator
# --------------------------------------------------------------------------- #
class NormalizedEfficiencyCalculator:
    """
    Calculate efficiency metrics normalized by task complexity.

    This enables fair comparison between agents across tasks of different
    complexity levels. Without normalization, simple tasks appear "efficient"
    and complex tasks appear "wasteful".

    Key Metrics
    -----------
    - Energy Efficiency   = Energy  / Task Complexity    (lower is better)
    - Carbon Efficiency   = Carbon  / Task Complexity    (lower is better)
    - Accuracy per Watt   = Accuracy / Energy            (higher is better)
    - Throughput per Watt = Tasks / Energy               (higher is better)
    - Latency Efficiency  = Accuracy / Latency(s)        (higher is better)

    Methods whose input is "below the floor" return ``None`` — callers
    (including the composite score) treat ``None`` as "component
    unavailable" and skip it.
    """

    #: Metrics aggregated in ``benchmark_efficiency_baseline``.
    _COMPARISON_METRICS: Tuple[str, ...] = (
        "energy_efficiency",
        "carbon_efficiency",
        "accuracy_per_watt",
        "latency_efficiency",
        "composite_efficiency",
    )

    _REQUIRED_RESULT_KEYS: Tuple[str, ...] = (
        "agent_id", "task_id", "accuracy",
        "energy_kwh", "carbon_kg", "latency_ms", "trace",
    )

    def __init__(
        self,
        *,
        config: Optional[NormalizationConfig] = None,
        epsilon: float = 1e-12,
        strict: bool = True,
        max_history: Optional[int] = 1000,
        analyzer: Optional[ComplexityAnalyzer] = None,
    ) -> None:
        if not math.isfinite(float(epsilon)) or epsilon <= 0:
            raise EfficiencyCalculatorError(
                f"epsilon must be finite and > 0, got {epsilon!r}."
            )
        if max_history is not None:
            if isinstance(max_history, bool) or not isinstance(
                max_history, int
            ):
                raise EfficiencyCalculatorError(
                    "max_history must be an int or None."
                )
            if max_history <= 0:
                raise EfficiencyCalculatorError(
                    "max_history must be > 0 or None."
                )

        self._config: NormalizationConfig = config or NormalizationConfig()
        self._epsilon: float = float(epsilon)
        self._strict: bool = bool(strict)
        self._max_history: Optional[int] = max_history

        self._lock = threading.RLock()
        self._complexity_analyzer: ComplexityAnalyzer = (
            analyzer if analyzer is not None else ComplexityAnalyzer()
        )
        self._history: List[ComparisonSample] = []

        logger.debug(
            "Initialized NormalizedEfficiencyCalculator "
            "(strict=%s, epsilon=%.1e, max_history=%s)",
            self._strict,
            self._epsilon,
            self._max_history,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> NormalizationConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def epsilon(self) -> float:
        return self._epsilon

    @property
    def max_history(self) -> Optional[int]:
        return self._max_history

    @property
    def history(self) -> List[ComparisonSample]:
        with self._lock:
            return list(self._history)

    @property
    def complexity_analyzer(self) -> ComplexityAnalyzer:
        return self._complexity_analyzer

    def set_analyzer(self, analyzer: ComplexityAnalyzer) -> None:
        """Swap the complexity analyzer at runtime."""
        if analyzer is None:
            raise EfficiencyCalculatorError("analyzer must not be None.")
        with self._lock:
            self._complexity_analyzer = analyzer

    # --------------------------------------------------------------- helpers
    def _resolve_complexity(
        self,
        trace: Optional[Dict],
        complexity: Optional[TaskComplexity],
    ) -> TaskComplexity:
        if complexity is not None:
            return complexity
        if trace is None:
            raise EfficiencyCalculatorError(
                "Either `trace` or `complexity` must be provided."
            )
        return self._complexity_analyzer.analyze_from_trace(trace)

    def _complexity_score(self, complexity: TaskComplexity) -> float:
        score = float(complexity.compute_composite_score())
        if score < self._config.min_complexity_score:
            logger.debug(
                "Very low complexity score %.6g; flooring to %.6g.",
                score,
                self._config.min_complexity_score,
            )
            score = self._config.min_complexity_score
        return score

    @staticmethod
    def _validate_finite(name: str, value: Any) -> None:
        if isinstance(value, bool):
            raise EfficiencyCalculatorError(
                f"{name} must be a finite number, got bool {value!r}."
            )
        if not isinstance(value, (int, float)) or not math.isfinite(
            float(value)
        ):
            raise EfficiencyCalculatorError(
                f"{name} must be a finite number, got {value!r}."
            )

    def _validate_result(
        self, result: Any, index: int,
    ) -> Dict[str, Any]:
        if not isinstance(result, dict):
            raise EfficiencyCalculatorError(
                f"results[{index}] must be a dict, "
                f"got {type(result).__name__}."
            )
        missing = [
            k for k in self._REQUIRED_RESULT_KEYS if k not in result
        ]
        if missing:
            raise EfficiencyCalculatorError(
                f"results[{index}] is missing required keys: {missing}."
            )
        return result

    # ---------------------------------------------------------- energy metrics
    def calculate_energy_efficiency(
        self,
        energy_kwh: float,
        trace: Optional[Dict] = None,
        complexity: Optional[TaskComplexity] = None,
    ) -> Optional[float]:
        """
        energy_efficiency = energy_kwh / task_complexity_score

        Lower is better. Returns ``None`` when energy is at or below
        epsilon (undefined / no-op task).
        """
        self._validate_finite("energy_kwh", energy_kwh)
        if energy_kwh < 0:
            raise EfficiencyCalculatorError(
                "energy_kwh must be non-negative."
            )

        if energy_kwh <= self._epsilon:
            msg = "energy_kwh is at/below epsilon; efficiency undefined."
            logger.warning(msg) if self._strict else logger.debug(msg)
            return None

        complexity_score = self._complexity_score(
            self._resolve_complexity(trace, complexity)
        )
        efficiency = energy_kwh / complexity_score
        logger.debug(
            "Energy efficiency=%.6f (energy=%.6g, complexity=%.6g)",
            efficiency,
            energy_kwh,
            complexity_score,
        )
        return efficiency

    def calculate_carbon_efficiency(
        self,
        carbon_kg: float,
        trace: Optional[Dict] = None,
        complexity: Optional[TaskComplexity] = None,
    ) -> Optional[float]:
        """carbon_efficiency = carbon_kg / task_complexity_score (lower is better)."""
        self._validate_finite("carbon_kg", carbon_kg)
        if carbon_kg < 0:
            raise EfficiencyCalculatorError(
                "carbon_kg must be non-negative."
            )

        if carbon_kg <= self._epsilon:
            msg = "carbon_kg is at/below epsilon; efficiency undefined."
            logger.warning(msg) if self._strict else logger.debug(msg)
            return None

        complexity_score = self._complexity_score(
            self._resolve_complexity(trace, complexity)
        )
        efficiency = carbon_kg / complexity_score
        logger.debug("Carbon efficiency=%.6f", efficiency)
        return efficiency

    # --------------------------------------------------------- per-watt metrics
    def calculate_accuracy_per_watt(
        self, accuracy: float, energy_kwh: float,
    ) -> Optional[float]:
        """accuracy_per_watt = accuracy / energy_kwh (higher is better)."""
        self._validate_finite("accuracy", accuracy)
        self._validate_finite("energy_kwh", energy_kwh)
        if not 0.0 <= accuracy <= 1.0:
            raise EfficiencyCalculatorError("accuracy must be in [0, 1].")
        if energy_kwh < 0:
            raise EfficiencyCalculatorError(
                "energy_kwh must be non-negative."
            )

        if energy_kwh < self._config.min_energy_kwh:
            msg = (
                "energy_kwh below floor %.3e; accuracy-per-watt undefined."
                % self._config.min_energy_kwh
            )
            logger.warning(msg) if self._strict else logger.debug(msg)
            return None

        efficiency = accuracy / energy_kwh
        logger.debug("Accuracy per watt=%.2f", efficiency)
        return efficiency

    def calculate_throughput_per_watt(
        self, tasks_completed: int, energy_kwh: float,
    ) -> Optional[float]:
        """throughput_per_watt = tasks_completed / energy_kwh (higher is better)."""
        if isinstance(tasks_completed, bool) or not isinstance(
            tasks_completed, int
        ) or tasks_completed < 0:
            raise EfficiencyCalculatorError(
                "tasks_completed must be a non-negative int."
            )
        self._validate_finite("energy_kwh", energy_kwh)
        if energy_kwh < 0:
            raise EfficiencyCalculatorError(
                "energy_kwh must be non-negative."
            )

        if energy_kwh < self._config.min_energy_kwh:
            msg = "energy_kwh below floor; throughput-per-watt undefined."
            logger.warning(msg) if self._strict else logger.debug(msg)
            return None

        throughput = tasks_completed / energy_kwh
        logger.debug("Throughput=%.2f tasks/Wh", throughput)
        return throughput

    def calculate_latency_efficiency(
        self, accuracy: float, latency_ms: float,
    ) -> Optional[float]:
        """latency_efficiency = accuracy / (latency_ms / 1000) (higher is better)."""
        self._validate_finite("accuracy", accuracy)
        self._validate_finite("latency_ms", latency_ms)
        if not 0.0 <= accuracy <= 1.0:
            raise EfficiencyCalculatorError("accuracy must be in [0, 1].")
        if latency_ms < 0:
            raise EfficiencyCalculatorError(
                "latency_ms must be non-negative."
            )

        if latency_ms < self._config.min_latency_ms:
            # A latency below the floor is unmeasurable. Returning a floored
            # finite value would misreport the efficiency (either inflating
            # or capping depending on the floor/input relationship), so we
            # return None in both strict and non-strict modes.
            msg = "latency_ms below floor; latency efficiency undefined."
            logger.warning(msg) if self._strict else logger.debug(msg)
            return None

        latency_seconds = latency_ms / 1000.0
        efficiency = accuracy / latency_seconds
        logger.debug("Latency efficiency=%.2f acc/s", efficiency)
        return efficiency

    # ------------------------------------------------------------ composite
    def calculate_composite_efficiency(
        self,
        accuracy: float,
        energy_kwh: float,
        carbon_kg: float,
        latency_ms: float,
        trace: Optional[Dict] = None,
        complexity: Optional[TaskComplexity] = None,
        weights: Optional[Dict[str, float]] = None,
    ) -> Optional[float]:
        """
        Combine multiple efficiency metrics into a single score in [0, 1].

        Missing components (``None``) are skipped and the remaining weights
        are re-normalized. Returns ``None`` if all components are undefined.
        """
        cfg = self._config
        w = dict(weights) if weights is not None else dict(
            cfg.default_weights
        )

        if set(w) != set(cfg.default_weights):
            raise EfficiencyCalculatorError(
                f"weights keys must be {sorted(cfg.default_weights)} "
                f"(got {sorted(w)})."
            )
        if any(v < 0 for v in w.values()):
            raise EfficiencyCalculatorError(
                "weights must be non-negative."
            )
        total_w = sum(w.values())
        if total_w <= 0:
            raise EfficiencyCalculatorError(
                "weights must sum to a positive value."
            )

        resolved_complexity = self._resolve_complexity(trace, complexity)

        acc_per_watt = self.calculate_accuracy_per_watt(
            accuracy, energy_kwh,
        )
        energy_eff = self.calculate_energy_efficiency(
            energy_kwh, complexity=resolved_complexity,
        )
        carbon_eff = self.calculate_carbon_efficiency(
            carbon_kg, complexity=resolved_complexity,
        )
        latency_eff = self.calculate_latency_efficiency(
            accuracy, latency_ms,
        )

        norm_acc_per_watt = (
            min(acc_per_watt / cfg.acc_per_watt_reference, 1.0)
            if acc_per_watt is not None else None
        )
        norm_energy_eff = (
            max(1.0 - energy_eff / cfg.energy_efficiency_ceiling, 0.0)
            if energy_eff is not None else None
        )
        norm_carbon_eff = (
            max(1.0 - carbon_eff / cfg.carbon_efficiency_ceiling, 0.0)
            if carbon_eff is not None else None
        )
        norm_latency_eff = (
            min(latency_eff / cfg.latency_efficiency_reference, 1.0)
            if latency_eff is not None else None
        )

        components = {
            "accuracy_per_watt": norm_acc_per_watt,
            "energy_efficiency": norm_energy_eff,
            "carbon_efficiency": norm_carbon_eff,
            "latency_efficiency": norm_latency_eff,
        }

        available = {k: v for k, v in components.items() if v is not None}
        if not available:
            logger.warning(
                "No composite components available; returning None."
            )
            return None

        available_weight = sum(w[k] for k in available)
        if available_weight <= 0:
            logger.warning(
                "All available components have zero weight; "
                "returning None."
            )
            return None

        composite = sum(
            (w[k] / available_weight) * v for k, v in available.items()
        )
        logger.debug(
            "Composite efficiency=%.3f (from %d components)",
            composite, len(available),
        )
        return composite

    # -------------------------------------------------------- comparison API
    def compare_across_complexities(
        self,
        results: Iterable[Dict],
        *,
        label: Optional[str] = None,
        record: bool = True,
    ) -> Dict[str, Any]:
        """
        Fair comparison across tasks of different complexity.

        Each entry in ``results`` must contain: ``agent_id``, ``task_id``,
        ``accuracy``, ``energy_kwh``, ``carbon_kg``, ``latency_ms``, ``trace``.

        Returns a dict with keys ``rankings``, ``summary``,
        ``complexity_distribution``, and (new in v5.1) ``by_agent``.
        """
        rankings: List[Dict[str, Any]] = []

        for index, result in enumerate(results):
            self._validate_result(result, index)
            complexity = self._complexity_analyzer.analyze_from_trace(
                result["trace"]
            )
            complexity_score = self._complexity_score(complexity)
            complexity_tier = (
                self._complexity_analyzer.categorize_complexity(complexity)
            )

            energy_eff = self.calculate_energy_efficiency(
                result["energy_kwh"], complexity=complexity,
            )
            carbon_eff = self.calculate_carbon_efficiency(
                result["carbon_kg"], complexity=complexity,
            )
            acc_per_watt = self.calculate_accuracy_per_watt(
                result["accuracy"], result["energy_kwh"],
            )
            latency_eff = self.calculate_latency_efficiency(
                result["accuracy"], result["latency_ms"],
            )
            composite_eff = self.calculate_composite_efficiency(
                result["accuracy"],
                result["energy_kwh"],
                result["carbon_kg"],
                result["latency_ms"],
                complexity=complexity,
            )

            rankings.append({
                "agent_id": result["agent_id"],
                "task_id": result["task_id"],
                # Raw
                "accuracy": float(result["accuracy"]),
                "energy_kwh": float(result["energy_kwh"]),
                "carbon_kg": float(result["carbon_kg"]),
                "latency_ms": float(result["latency_ms"]),
                # Complexity
                "task_complexity": float(complexity_score),
                "complexity_tier": complexity_tier,
                # Normalized (None is preserved for aggregation to skip)
                "energy_efficiency": energy_eff,
                "carbon_efficiency": carbon_eff,
                "accuracy_per_watt": acc_per_watt,
                "latency_efficiency": latency_eff,
                "composite_efficiency": (
                    float(composite_eff)
                    if composite_eff is not None else 0.0
                ),
            })

        # Deterministic sort: composite desc, then agent_id asc, then
        # task_id asc (mirrors GreenLeaderboard's tie-break policy).
        rankings.sort(
            key=lambda x: (
                -x["composite_efficiency"],
                x["agent_id"],
                x["task_id"],
            )
        )
        for i, ranking in enumerate(rankings):
            ranking["rank"] = i + 1

        summary: Dict[str, Any] = {}
        if rankings:
            def _mean(key: str) -> Optional[float]:
                values = [r[key] for r in rankings if r[key] is not None]
                return float(np.mean(values)) if values else None

            summary = {
                "total_agents": len(rankings),
                "avg_energy_efficiency": _mean("energy_efficiency"),
                "avg_carbon_efficiency": _mean("carbon_efficiency"),
                "avg_accuracy_per_watt": _mean("accuracy_per_watt"),
                "avg_latency_efficiency": _mean("latency_efficiency"),
                "avg_composite_efficiency": _mean("composite_efficiency"),
                "best_agent": rankings[0]["agent_id"],
            }

        tiers = [r["complexity_tier"] for r in rankings]
        complexity_distribution: Dict[str, int] = {}
        for tier in tiers:
            complexity_distribution[tier] = (
                complexity_distribution.get(tier, 0) + 1
            )

        # Per-agent aggregation (new in v5.1)
        by_agent: Dict[str, Dict[str, Any]] = {}
        for r in rankings:
            entry = by_agent.get(r["agent_id"])
            if entry is None:
                entry = {
                    "agent_id": r["agent_id"],
                    "task_count": 0,
                    "mean_composite": 0.0,
                    "best_composite": -1.0,
                    "best_task_id": None,
                }
                by_agent[r["agent_id"]] = entry
            entry["task_count"] += 1
            entry["mean_composite"] += r["composite_efficiency"]
            if r["composite_efficiency"] > entry["best_composite"]:
                entry["best_composite"] = r["composite_efficiency"]
                entry["best_task_id"] = r["task_id"]
        for entry in by_agent.values():
            if entry["task_count"] > 0:
                entry["mean_composite"] /= entry["task_count"]

        if record and rankings:
            self._record_sample(
                num_agents=len(rankings),
                best_agent=rankings[0]["agent_id"] if rankings else None,
                avg_composite_efficiency=summary.get(
                    "avg_composite_efficiency", 0.0
                ) or 0.0,
                label=label,
            )

        return {
            "rankings": rankings,
            "summary": summary,
            "complexity_distribution": complexity_distribution,
            "by_agent": by_agent,
        }

    def benchmark_efficiency_baseline(
        self, results: Iterable[Dict],
    ) -> Dict[str, Optional[Dict[str, float]]]:
        """
        Compute percentile baselines (min / p25 / p50 / p75 / p90 / p95 /
        max / mean / std) per metric.

        Returns a consistent-shape dict: every key in
        ``_COMPARISON_METRICS`` is present; the value is ``None`` when the
        metric had no finite samples.
        """
        comparison = self.compare_across_complexities(results, record=False)
        rankings = comparison["rankings"]

        baselines: Dict[str, Optional[Dict[str, float]]] = {}
        for metric in self._COMPARISON_METRICS:
            values = [
                r[metric] for r in rankings if r[metric] is not None
            ]
            if not values:
                baselines[metric] = None
                continue
            arr = np.asarray(values, dtype=float)
            baselines[metric] = {
                "min": float(arr.min()),
                "p25": float(np.percentile(arr, 25)),
                "p50": float(np.percentile(arr, 50)),
                "p75": float(np.percentile(arr, 75)),
                "p90": float(np.percentile(arr, 90)),
                "p95": float(np.percentile(arr, 95)),
                "max": float(arr.max()),
                "mean": float(arr.mean()),
                "std": float(arr.std()),
            }
        logger.debug(
            "Established baselines from %d results.", len(rankings),
        )
        return baselines

    def detect_efficiency_outliers(
        self, results: Iterable[Dict], threshold_std: float = 2.0,
    ) -> List[Dict[str, Any]]:
        """Flag agents with composite efficiency > threshold_std below the mean."""
        if not math.isfinite(float(threshold_std)) or threshold_std <= 0:
            raise EfficiencyCalculatorError(
                "threshold_std must be a finite positive number."
            )

        comparison = self.compare_across_complexities(results, record=False)
        rankings = comparison["rankings"]

        if len(rankings) < 3:
            logger.warning(
                "Too few results for outlier detection (need >= 3)."
            )
            return []

        efficiencies = [r["composite_efficiency"] for r in rankings]
        mean_eff = float(np.mean(efficiencies))
        std_eff = float(np.std(efficiencies))
        if std_eff <= self._epsilon:
            logger.debug(
                "Zero variance in composite efficiency; no outliers."
            )
            return []

        outliers: List[Dict[str, Any]] = []
        for ranking in rankings:
            z_score = (
                ranking["composite_efficiency"] - mean_eff
            ) / std_eff
            if z_score < -threshold_std:
                outliers.append({
                    "agent_id": ranking["agent_id"],
                    "task_id": ranking["task_id"],
                    "composite_efficiency": ranking[
                        "composite_efficiency"
                    ],
                    "z_score": z_score,
                    "deviation": (
                        f"{abs(z_score):.1f} std below mean"
                    ),
                    "recommendation": (
                        self._generate_efficiency_recommendation(ranking)
                    ),
                })
        logger.debug(
            "Detected %d efficiency outliers.", len(outliers),
        )
        return outliers

    def best_agent(
        self, results: Iterable[Dict],
    ) -> Optional[str]:
        """Return the agent with the highest composite efficiency."""
        comparison = self.compare_across_complexities(results, record=False)
        rankings = comparison["rankings"]
        return rankings[0]["agent_id"] if rankings else None

    def worst_agent(
        self, results: Iterable[Dict],
    ) -> Optional[str]:
        """Return the agent with the lowest composite efficiency."""
        comparison = self.compare_across_complexities(results, record=False)
        rankings = comparison["rankings"]
        return rankings[-1]["agent_id"] if rankings else None

    def _generate_efficiency_recommendation(
        self, ranking: Dict[str, Any],
    ) -> str:
        cfg = self._config
        issues: List[str] = []

        energy_eff = ranking.get("energy_efficiency")
        if (
            energy_eff is not None
            and energy_eff > cfg.poor_energy_efficiency_threshold
        ):
            issues.append(
                "High energy consumption relative to task complexity"
            )
        apw = ranking.get("accuracy_per_watt")
        if (
            apw is not None
            and apw < cfg.poor_accuracy_per_watt_threshold
        ):
            issues.append(
                "Low accuracy per watt - consider more efficient models"
            )
        latency_eff = ranking.get("latency_efficiency")
        if (
            latency_eff is not None
            and latency_eff < cfg.poor_latency_efficiency_threshold
        ):
            issues.append(
                "Slow execution - optimize inference speed"
            )

        if not issues:
            return "Overall efficiency is poor - review entire pipeline"
        return "; ".join(issues)

    # -------------------------------------------------------------- history
    def _record_sample(
        self,
        *,
        num_agents: int,
        best_agent: Optional[str],
        avg_composite_efficiency: float,
        label: Optional[str],
    ) -> None:
        sample = ComparisonSample(
            timestamp=time.time(),
            num_agents=num_agents,
            best_agent=best_agent,
            avg_composite_efficiency=float(avg_composite_efficiency),
            label=label,
        )
        with self._lock:
            self._history.append(sample)
            if (
                self._max_history is not None
                and len(self._history) > self._max_history
            ):
                del self._history[0]

    def history_since(
        self, since_epoch_seconds: float,
    ) -> List[ComparisonSample]:
        """Return samples with ``timestamp >= since_epoch_seconds``."""
        self._validate_finite("since_epoch_seconds", since_epoch_seconds)
        with self._lock:
            return [
                s for s in self._history
                if s.timestamp >= since_epoch_seconds
            ]

    def history_between(
        self, t0: float, t1: float,
    ) -> List[ComparisonSample]:
        """Return samples with ``t0 <= timestamp <= t1``."""
        self._validate_finite("t0", t0)
        self._validate_finite("t1", t1)
        if t1 < t0:
            raise EfficiencyCalculatorError(
                "t1 must be >= t0."
            )
        with self._lock:
            return [s for s in self._history if t0 <= s.timestamp <= t1]

    def clear_history(self) -> int:
        """Clear the history; return how many samples were removed."""
        with self._lock:
            n = len(self._history)
            self._history.clear()
        logger.debug("Cleared %d history sample(s).", n)
        return n

    def reset(self, *, clear_history: bool = False) -> None:
        with self._lock:
            if clear_history:
                self._history.clear()
        logger.debug(
            "Reset NormalizedEfficiencyCalculator "
            "(clear_history=%s)", clear_history,
        )

    # -------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "schema_version": __version__,
                "config": asdict(self._config),
                "epsilon": self._epsilon,
                "strict": self._strict,
                "max_history": self._max_history,
                "history": [s.to_dict() for s in self._history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Dict[str, Any],
        *,
        analyzer: Optional[ComplexityAnalyzer] = None,
    ) -> "NormalizedEfficiencyCalculator":
        """
        Restore a calculator from a serialized payload.

        Parameters
        ----------
        data : dict
            Payload produced by :meth:`to_dict`.
        analyzer : ComplexityAnalyzer, optional
            The analyzer is *not* serialized (it may carry state). Pass the
            same analyzer instance the source calculator used, or leave
            ``None`` to construct a fresh one. Mismatched analyzers will
            produce different complexity scores for the same traces.
        """
        if not isinstance(data, dict):
            raise EfficiencyCalculatorError(
                f"from_dict expects a dict, got {type(data).__name__}."
            )
        cfg_data = data.get("config", {}) or {}
        if not isinstance(cfg_data, dict):
            raise EfficiencyCalculatorError(
                f"config must be a dict, got {type(cfg_data).__name__}."
            )

        # Rebuild NormalizationConfig from the *set of dataclass fields*,
        # not a hardcoded list (fix P0 #6).
        known_fields = {f.name for f in fields(NormalizationConfig)}
        cfg_kwargs = {
            k: v for k, v in cfg_data.items() if k in known_fields
        }
        # Empty default_weights should fall back to the dataclass default.
        if not cfg_kwargs.get("default_weights"):
            cfg_kwargs.pop("default_weights", None)
        cfg = NormalizationConfig(**cfg_kwargs)

        epsilon = _safe_float(
            data.get("epsilon", 1e-12), name="epsilon", default=1e-12,
        )
        if epsilon <= 0:
            raise EfficiencyCalculatorError(
                "epsilon must be > 0."
            )
        max_history = _safe_optional_int(
            data.get("max_history", 1000),
            name="max_history", default=1000, min_value=1,
        )
        calc = cls(
            config=cfg,
            epsilon=epsilon,
            strict=bool(data.get("strict", True)),
            max_history=max_history,
            analyzer=analyzer,
        )

        # Restore history with validation + max_history enforcement
        # (fix P0 #4).
        raw_history = data.get("history", []) or []
        if not isinstance(raw_history, list):
            raise EfficiencyCalculatorError(
                "history must be a list."
            )
        if (
            max_history is not None
            and len(raw_history) > max_history
        ):
            logger.warning(
                "Restoring %d history entries into a max_history=%d "
                "buffer; keeping the most recent %d.",
                len(raw_history), max_history, max_history,
            )
            raw_history = raw_history[-max_history:]

        known_sample_fields = {f.name for f in fields(ComparisonSample)}
        with calc._lock:
            for entry in raw_history:
                if not isinstance(entry, dict):
                    raise EfficiencyCalculatorError(
                        f"history entry must be a dict, "
                        f"got {type(entry).__name__}."
                    )
                cleaned = {
                    k: v for k, v in entry.items()
                    if k in known_sample_fields
                }
                calc._history.append(ComparisonSample(**cleaned))
        return calc

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        analyzer: Optional[ComplexityAnalyzer] = None,
    ) -> "NormalizedEfficiencyCalculator":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise EfficiencyCalculatorError(
                f"Invalid JSON payload: {exc}"
            ) from exc
        if not isinstance(data, dict):
            raise EfficiencyCalculatorError(
                f"JSON payload must decode to an object, "
                f"got {type(data).__name__}."
            )
        return cls.from_dict(data, analyzer=analyzer)

    # ------------------------------------------------------------ context mgr
    def __enter__(self) -> "NormalizedEfficiencyCalculator":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped efficiency comparison.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - getattr(
            self, "_ctx_start", time.perf_counter()
        )
        if exc_type is not None:
            logger.debug(
                "Exiting scope after %.4fs due to %s.",
                elapsed, exc_type.__name__,
            )
            return
        logger.debug(
            "Scoped efficiency comparison completed in %.4fs.", elapsed,
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            n = len(self._history)
        return (
            "NormalizedEfficiencyCalculator("
            f"strict={self._strict}, epsilon={self._epsilon:.1e}, "
            f"max_history={self._max_history}, samples={n})"
        )


# Backward-compatible alias for callers using the shorter name.
EfficiencyCalculator = NormalizedEfficiencyCalculator


__all__ = [
    "ComparisonSample",
    "EfficiencyCalculator",
    "EfficiencyCalculatorError",
    "NormalizationConfig",
    "NormalizedEfficiencyCalculator",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m metrics.efficiency_calculator
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    sample_trace = {"steps": 10, "branches": 2, "tools_called": 3}

    calc = NormalizedEfficiencyCalculator(strict=False, max_history=5)

    results = [
        {
            "agent_id": "A", "task_id": "t1",
            "accuracy": 0.95, "energy_kwh": 0.01,
            "carbon_kg": 0.003, "latency_ms": 120.0,
            "trace": sample_trace,
        },
        {
            "agent_id": "B", "task_id": "t2",
            "accuracy": 0.90, "energy_kwh": 0.003,
            "carbon_kg": 0.001, "latency_ms": 90.0,
            "trace": sample_trace,
        },
    ]

    comparison = calc.compare_across_complexities(
        results, label="smoke",
    )
    print("Best agent:", comparison["summary"].get("best_agent"))
    print("Summary   :", comparison["summary"])
    print("Baselines :", calc.benchmark_efficiency_baseline(results))
    print("Outliers  :", calc.detect_efficiency_outliers(results))
    print("By agent  :", comparison["by_agent"])

    # --- P0 #1 regression: None metrics don't poison the summary ---
    mixed = [
        {"agent_id": "A", "task_id": "zero", "accuracy": 0.9,
         "energy_kwh": 0.0, "carbon_kg": 0.0, "latency_ms": 100.0,
         "trace": sample_trace},   # zero → None metrics
        {"agent_id": "B", "task_id": "real", "accuracy": 0.9,
         "energy_kwh": 0.01, "carbon_kg": 0.003, "latency_ms": 100.0,
         "trace": sample_trace},
    ]
    strict_calc = NormalizedEfficiencyCalculator(strict=True)
    mixed_summary = strict_calc.compare_across_complexities(
        mixed, record=False,
    )["summary"]
    for key in (
        "avg_energy_efficiency",
        "avg_carbon_efficiency",
        "avg_accuracy_per_watt",
        "avg_latency_efficiency",
    ):
        val = mixed_summary[key]
        assert val is None or not math.isnan(val), (
            f"{key} is nan ({val})"
        )
    print("None-filter : OK")

    # --- P0 #2 regression: negative energy rejected everywhere ---
    for fn, args in [
        (calc.calculate_accuracy_per_watt, (0.9, -1.0)),
        (calc.calculate_throughput_per_watt, (10, -1.0)),
        (calc.calculate_energy_efficiency, (-1.0,)),
        (calc.calculate_carbon_efficiency, (-0.001,)),
        (calc.calculate_latency_efficiency, (0.9, -1.0)),
    ]:
        try:
            fn(*args)
        except EfficiencyCalculatorError:
            pass
        else:
            raise AssertionError(
                f"{fn.__name__}{args} should have raised."
            )
    print("Negative   : OK (rejected everywhere)")

    # --- P0 #3 regression: recommendation on None-metric ranking ---
    outlier_calc = NormalizedEfficiencyCalculator(strict=True)
    outlier_mixed = [
        {"agent_id": "A", "task_id": "z", "accuracy": 0.9,
         "energy_kwh": 0.0, "carbon_kg": 0.0, "latency_ms": 100.0,
         "trace": sample_trace},
        {"agent_id": "B", "task_id": "r1", "accuracy": 0.9,
         "energy_kwh": 0.01, "carbon_kg": 0.003, "latency_ms": 100.0,
         "trace": sample_trace},
        {"agent_id": "C", "task_id": "r2", "accuracy": 0.9,
         "energy_kwh": 0.02, "carbon_kg": 0.006, "latency_ms": 100.0,
         "trace": sample_trace},
        {"agent_id": "D", "task_id": "r3", "accuracy": 0.9,
         "energy_kwh": 0.03, "carbon_kg": 0.009, "latency_ms": 100.0,
         "trace": sample_trace},
    ]
    # Must not raise even though agent A has all-None metrics.
    outlier_calc.detect_efficiency_outliers(outlier_mixed)
    print("Outliers   : OK (no crash on None metrics)")

    # --- P0 #4 regression: from_dict validates history ---
    payload = calc.to_json()
    restored = NormalizedEfficiencyCalculator.from_json(payload)
    assert restored.to_dict() == calc.to_dict()
    print("Round-trip : OK")

    # Bad history entry
    bad_payload = {
        "history": [{"timestamp": "not a number",
                     "num_agents": 1,
                     "best_agent": "A",
                     "avg_composite_efficiency": 0.5}],
        "config": {}, "epsilon": 1e-12, "strict": True,
        "max_history": 100,
    }
    try:
        NormalizedEfficiencyCalculator.from_dict(bad_payload)
    except EfficiencyCalculatorError as exc:
        print("Bad hist   :", exc)
    else:
        raise AssertionError(
            "String timestamp in history should have been rejected."
        )

    # max_history enforcement
    big_history = {
        "history": [
            {"timestamp": float(i), "num_agents": 1,
             "best_agent": "A", "avg_composite_efficiency": 0.5}
            for i in range(50)
        ],
        "config": {}, "epsilon": 1e-12, "strict": True,
        "max_history": 5,
    }
    trimmed = NormalizedEfficiencyCalculator.from_dict(big_history)
    assert len(trimmed.history) == 5
    assert trimmed.history[0].timestamp == 45.0
    print("History    : max_history enforced on restore")

    # --- P0 #5 regression: missing keys raise EfficiencyCalculatorError ---
    try:
        calc.compare_across_complexities([{"agent_id": "A"}])
    except EfficiencyCalculatorError as exc:
        print("Bad result :", exc)
    else:
        raise AssertionError("Missing keys should have raised.")

    # --- P1 #7 regression: bool rejected by _validate_finite ---
    try:
        calc.calculate_accuracy_per_watt(True, 0.01)
    except EfficiencyCalculatorError:
        print("bool acc   : rejected")
    else:
        raise AssertionError("bool accuracy should have been rejected.")

    # --- P1 #8/#9 regression: latency accuracy guard + None floor ---
    try:
        calc.calculate_latency_efficiency(5.0, 100.0)
    except EfficiencyCalculatorError:
        print("lat acc    : rejected (accuracy range check)")
    else:
        raise AssertionError("accuracy=5 should have been rejected.")

    below_floor = NormalizedEfficiencyCalculator(strict=False)
    result = below_floor.calculate_latency_efficiency(0.9, 0.1)
    assert result is None
    print("lat floor  : None in non-strict mode")

    # --- P1 #10: consistent baseline shape on empty input ---
    empty = calc.benchmark_efficiency_baseline([])
    assert set(empty) == set(NormalizedEfficiencyCalculator._COMPARISON_METRICS)
    assert all(v is None for v in empty.values())
    print("empty base : consistent shape")

    # --- P1 #11: no phantom sample on empty comparison ---
    before = len(calc.history)
    calc.compare_across_complexities([], record=True)
    assert len(calc.history) == before
    print("empty cmp  : no phantom history sample")

    # --- P2: by_agent aggregation ---
    multi = [
        {"agent_id": "A", "task_id": "t1", "accuracy": 0.9,
         "energy_kwh": 0.01, "carbon_kg": 0.003, "latency_ms": 100.0,
         "trace": sample_trace},
        {"agent_id": "A", "task_id": "t2", "accuracy": 0.85,
         "energy_kwh": 0.02, "carbon_kg": 0.006, "latency_ms": 150.0,
         "trace": sample_trace},
        {"agent_id": "B", "task_id": "t3", "accuracy": 0.9,
         "energy_kwh": 0.005, "carbon_kg": 0.0015, "latency_ms": 90.0,
         "trace": sample_trace},
    ]
    agg = calc.compare_across_complexities(multi, record=False)["by_agent"]
    assert agg["A"]["task_count"] == 2
    assert agg["B"]["task_count"] == 1
    print("by_agent   : OK", {k: v["task_count"] for k, v in agg.items()})

    # --- P2: history_since / history_between ---
    since = calc.history_since(0.0)
    assert len(since) == len(calc.history)
    between = calc.history_between(0.0, time.time() + 1)
    assert len(between) == len(calc.history)
    print("history    : query helpers OK")

    # --- P2: best/worst agent ---
    assert calc.best_agent(multi) is not None
    assert calc.worst_agent(multi) is not None
    print("best/worst : OK")

    # --- P2: clear_history ---
    n_cleared = calc.clear_history()
    assert n_cleared >= 0
    assert len(calc.history) == 0
    print(f"clear      : removed {n_cleared} sample(s)")

    # --- P2: set_analyzer ---
    custom = ComplexityAnalyzer()
    calc.set_analyzer(custom)
    assert calc.complexity_analyzer is custom
    print("set_anlyzr : OK")

    # --- Context manager ---
    with NormalizedEfficiencyCalculator(strict=False) as scoped:
        scoped.calculate_accuracy_per_watt(0.9, 0.0)  # returns None
    print("Context    : OK")

    print("\nSmoke test passed.")
