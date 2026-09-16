# metrics/efficiency_calculator.py

"""
Normalized Efficiency Calculator

Calculates efficiency metrics normalized by task complexity to enable
fair comparison across tasks of different difficulties.

Enhancements
------------
- Robust import of ``analysis.complexity_analyzer`` (no more global sys.path hack).
- All magic constants centralized in :class:`NormalizationConfig`.
- Thread-safe via ``RLock``.
- Strict / non-strict semantics for undefined results.
- Immutable ``ComparisonSample`` history with bounded ring-buffer.
- Serialization to / from ``dict`` and JSON.
- Context-manager support for scoped comparisons.
- Custom :class:`EfficiencyCalculatorError`.
- Lazy ``%s`` logging, ``__repr__``, type hints, and a smoke test under ``__main__``.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Robust import of the analysis module
# --------------------------------------------------------------------------- #
try:  # Preferred: package-relative import
    from analysis.complexity_analyzer import ComplexityAnalyzer, TaskComplexity
except ImportError:  # pragma: no cover — fallback for script-style usage
    import sys

    _ANALYSIS_ROOT = Path(__file__).resolve().parent.parent
    if str(_ANALYSIS_ROOT) not in sys.path:
        sys.path.insert(0, str(_ANALYSIS_ROOT))
    from analysis.complexity_analyzer import ComplexityAnalyzer, TaskComplexity  # type: ignore


# --------------------------------------------------------------------------- #
# Errors & data containers
# --------------------------------------------------------------------------- #
class EfficiencyCalculatorError(ValueError):
    """Raised for invalid inputs or configuration."""


@dataclass(frozen=True)
class NormalizationConfig:
    """
    All tunable constants used for normalization / floors.

    Centralizing these avoids magic numbers scattered through the code and
    lets callers swap in domain-specific calibration without subclassing.
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
        for name in ("min_complexity_score", "min_energy_kwh", "min_latency_ms"):
            value = getattr(self, name)
            if value <= 0:
                raise EfficiencyCalculatorError(f"{name} must be > 0 (got {value}).")
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
    - Energy Efficiency  = Energy  / Task Complexity
    - Carbon Efficiency  = Carbon  / Task Complexity
    - Accuracy per Watt  = Accuracy / Energy
    - Throughput per Watt = Tasks Completed / Energy
    """

    def __init__(
        self,
        *,
        config: Optional[NormalizationConfig] = None,
        epsilon: float = 1e-12,
        strict: bool = True,
        max_history: Optional[int] = 1000,
        analyzer: Optional[ComplexityAnalyzer] = None,
    ) -> None:
        if epsilon <= 0:
            raise EfficiencyCalculatorError("epsilon must be > 0.")
        if max_history is not None and max_history <= 0:
            raise EfficiencyCalculatorError("max_history must be > 0 or None.")

        self._config: NormalizationConfig = config or NormalizationConfig()
        self._epsilon: float = float(epsilon)
        self._strict: bool = bool(strict)
        self._max_history: Optional[int] = max_history

        self._lock = threading.RLock()
        self._complexity_analyzer: ComplexityAnalyzer = analyzer or ComplexityAnalyzer()
        self._history: List[ComparisonSample] = []

        logger.info(
            "Initialized NormalizedEfficiencyCalculator (strict=%s, epsilon=%.1e)",
            self._strict,
            self._epsilon,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> NormalizationConfig:
        return self._config

    @property
    def history(self) -> List[ComparisonSample]:
        with self._lock:
            return list(self._history)

    @property
    def complexity_analyzer(self) -> ComplexityAnalyzer:
        return self._complexity_analyzer

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
            logger.warning(
                "Very low complexity score %.6g; flooring to %.6g.",
                score,
                self._config.min_complexity_score,
            )
            score = self._config.min_complexity_score
        return score

    @staticmethod
    def _validate_finite(name: str, value: float) -> None:
        if not isinstance(value, (int, float)) or math.isnan(value) or math.isinf(value):
            raise EfficiencyCalculatorError(f"{name} must be a finite number.")

    # ---------------------------------------------------------- energy metrics
    def calculate_energy_efficiency(
        self,
        energy_kwh: float,
        trace: Optional[Dict] = None,
        complexity: Optional[TaskComplexity] = None,
    ) -> Optional[float]:
        """
        energy_efficiency = energy_kwh / task_complexity_score

        Lower is better. Returns ``None`` in strict mode if energy is below
        epsilon (undefined / no-op task).
        """
        self._validate_finite("energy_kwh", energy_kwh)
        if energy_kwh < 0:
            raise EfficiencyCalculatorError("energy_kwh must be non-negative.")

        if energy_kwh <= self._epsilon:
            msg = "energy_kwh is at/below epsilon; efficiency undefined."
            if self._strict:
                logger.warning(msg)
                return None
            logger.warning("%s Returning 0.0 (non-strict).", msg)
            return 0.0

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
            raise EfficiencyCalculatorError("carbon_kg must be non-negative.")

        if carbon_kg <= self._epsilon:
            msg = "carbon_kg is at/below epsilon; efficiency undefined."
            if self._strict:
                logger.warning(msg)
                return None
            logger.warning("%s Returning 0.0 (non-strict).", msg)
            return 0.0

        complexity_score = self._complexity_score(
            self._resolve_complexity(trace, complexity)
        )
        efficiency = carbon_kg / complexity_score
        logger.debug("Carbon efficiency=%.6f", efficiency)
        return efficiency

    # --------------------------------------------------------- per-watt metrics
    def calculate_accuracy_per_watt(
        self, accuracy: float, energy_kwh: float
    ) -> Optional[float]:
        """accuracy_per_watt = accuracy / energy_kwh (higher is better)."""
        self._validate_finite("accuracy", accuracy)
        self._validate_finite("energy_kwh", energy_kwh)
        if not 0.0 <= accuracy <= 1.0:
            raise EfficiencyCalculatorError("accuracy must be in [0, 1].")

        if energy_kwh < self._config.min_energy_kwh:
            msg = (
                "energy_kwh below floor %.3e; accuracy-per-watt undefined."
                % self._config.min_energy_kwh
            )
            if self._strict:
                logger.warning(msg)
                return None
            logger.warning("%s Using floor value.", msg)
            energy_kwh = self._config.min_energy_kwh

        efficiency = accuracy / energy_kwh
        logger.debug("Accuracy per watt=%.2f", efficiency)
        return efficiency

    def calculate_throughput_per_watt(
        self, tasks_completed: int, energy_kwh: float
    ) -> Optional[float]:
        """throughput_per_watt = tasks_completed / energy_kwh (higher is better)."""
        if not isinstance(tasks_completed, int) or tasks_completed < 0:
            raise EfficiencyCalculatorError("tasks_completed must be a non-negative int.")
        self._validate_finite("energy_kwh", energy_kwh)

        if energy_kwh < self._config.min_energy_kwh:
            msg = "energy_kwh below floor; throughput-per-watt undefined."
            if self._strict:
                logger.warning(msg)
                return None
            logger.warning("%s Using floor value.", msg)
            energy_kwh = self._config.min_energy_kwh

        throughput = tasks_completed / energy_kwh
        logger.debug("Throughput=%.2f tasks/Wh", throughput)
        return throughput

    def calculate_latency_efficiency(
        self, accuracy: float, latency_ms: float
    ) -> Optional[float]:
        """latency_efficiency = accuracy / (latency_ms / 1000) (higher is better)."""
        self._validate_finite("accuracy", accuracy)
        self._validate_finite("latency_ms", latency_ms)
        if latency_ms < 0:
            raise EfficiencyCalculatorError("latency_ms must be non-negative.")

        if latency_ms < self._config.min_latency_ms:
            msg = "latency_ms below floor; latency efficiency undefined."
            if self._strict:
                logger.warning(msg)
                return None
            logger.warning("%s Using floor value.", msg)
            latency_ms = self._config.min_latency_ms

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

        Missing components (from strict-mode ``None``) are skipped and the
        remaining weights are re-normalized. Returns ``None`` if all
        components are undefined.
        """
        cfg = self._config
        w = dict(weights) if weights is not None else dict(cfg.default_weights)

        if set(w) != set(cfg.default_weights):
            raise EfficiencyCalculatorError(
                f"weights keys must be {sorted(cfg.default_weights)} (got {sorted(w)})."
            )
        if any(v < 0 for v in w.values()):
            raise EfficiencyCalculatorError("weights must be non-negative.")
        total_w = sum(w.values())
        if total_w <= 0:
            raise EfficiencyCalculatorError("weights must sum to a positive value.")

        resolved_complexity = self._resolve_complexity(trace, complexity)

        acc_per_watt = self.calculate_accuracy_per_watt(accuracy, energy_kwh)
        energy_eff = self.calculate_energy_efficiency(
            energy_kwh, complexity=resolved_complexity
        )
        carbon_eff = self.calculate_carbon_efficiency(
            carbon_kg, complexity=resolved_complexity
        )
        latency_eff = self.calculate_latency_efficiency(accuracy, latency_ms)

        # Normalize each component to [0, 1]
        norm_acc_per_watt = (
            min(acc_per_watt / cfg.acc_per_watt_reference, 1.0)
            if acc_per_watt is not None
            else None
        )
        norm_energy_eff = (
            max(1.0 - energy_eff / cfg.energy_efficiency_ceiling, 0.0)
            if energy_eff is not None
            else None
        )
        norm_carbon_eff = (
            max(1.0 - carbon_eff / cfg.carbon_efficiency_ceiling, 0.0)
            if carbon_eff is not None
            else None
        )
        norm_latency_eff = (
            min(latency_eff / cfg.latency_efficiency_reference, 1.0)
            if latency_eff is not None
            else None
        )

        components = {
            "accuracy_per_watt": norm_acc_per_watt,
            "energy_efficiency": norm_energy_eff,
            "carbon_efficiency": norm_carbon_eff,
            "latency_efficiency": norm_latency_eff,
        }

        available = {k: v for k, v in components.items() if v is not None}
        if not available:
            logger.warning("No composite components available; returning None.")
            return None

        available_weight = sum(w[k] for k in available)
        if available_weight <= 0:
            logger.warning("All available components have zero weight; returning None.")
            return None

        composite = sum(
            (w[k] / available_weight) * v for k, v in available.items()
        )

        logger.info("Composite efficiency=%.3f (from %d components)", composite, len(available))
        return composite

    # -------------------------------------------------------- comparison API
    def compare_across_complexities(
        self,
        results: Iterable[Dict],
        *,
        label: Optional[str] = None,
        record: bool = True,
    ) -> Dict:
        """
        Fair comparison across tasks of different complexity.

        Parameters
        ----------
        results : iterable of dict
            Each entry must contain ``agent_id``, ``task_id``, ``accuracy``,
            ``energy_kwh``, ``carbon_kg``, ``latency_ms``, ``trace``.
        label : str, optional
            Human-readable label for the recorded history sample.
        record : bool
            If True (default), append a bounded :class:`ComparisonSample`.

        Returns
        -------
        dict with keys ``rankings``, ``summary``, ``complexity_distribution``.
        """
        rankings: List[Dict[str, Any]] = []

        for result in results:
            complexity = self._complexity_analyzer.analyze_from_trace(result["trace"])
            complexity_score = self._complexity_score(complexity)
            complexity_tier = self._complexity_analyzer.categorize_complexity(complexity)

            energy_eff = self.calculate_energy_efficiency(
                result["energy_kwh"], complexity=complexity
            )
            carbon_eff = self.calculate_carbon_efficiency(
                result["carbon_kg"], complexity=complexity
            )
            acc_per_watt = self.calculate_accuracy_per_watt(
                result["accuracy"], result["energy_kwh"]
            )
            latency_eff = self.calculate_latency_efficiency(
                result["accuracy"], result["latency_ms"]
            )
            composite_eff = self.calculate_composite_efficiency(
                result["accuracy"],
                result["energy_kwh"],
                result["carbon_kg"],
                result["latency_ms"],
                complexity=complexity,
            )

            rankings.append(
                {
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
                    # Normalized
                    "energy_efficiency": energy_eff,
                    "carbon_efficiency": carbon_eff,
                    "accuracy_per_watt": acc_per_watt,
                    "latency_efficiency": latency_eff,
                    "composite_efficiency": (
                        float(composite_eff) if composite_eff is not None else 0.0
                    ),
                }
            )

        rankings.sort(key=lambda x: x["composite_efficiency"], reverse=True)
        for i, ranking in enumerate(rankings):
            ranking["rank"] = i + 1

        summary: Dict[str, Any] = {}
        if rankings:
            def _mean(key: str) -> float:
                return float(np.mean([r[key] for r in rankings]))

            summary = {
                "total_agents": len(rankings),
                "avg_energy_efficiency": _mean("energy_efficiency"),
                "avg_carbon_efficiency": _mean("carbon_efficiency"),
                "avg_accuracy_per_watt": _mean("accuracy_per_watt"),
                "avg_composite_efficiency": _mean("composite_efficiency"),
                "best_agent": rankings[0]["agent_id"],
            }

        tiers = [r["complexity_tier"] for r in rankings]
        complexity_distribution = {tier: tiers.count(tier) for tier in set(tiers)}

        if record:
            self._record_sample(
                num_agents=len(rankings),
                best_agent=rankings[0]["agent_id"] if rankings else None,
                avg_composite_efficiency=summary.get("avg_composite_efficiency", 0.0),
                label=label,
            )

        return {
            "rankings": rankings,
            "summary": summary,
            "complexity_distribution": complexity_distribution,
        }

    def benchmark_efficiency_baseline(self, results: Iterable[Dict]) -> Dict:
        """
        Compute percentile baselines (p25/p50/p75/p90/p95) per metric.
        """
        comparison = self.compare_across_complexities(results, record=False)
        rankings = comparison["rankings"]
        if not rankings:
            return {}

        metrics = [
            "energy_efficiency",
            "carbon_efficiency",
            "accuracy_per_watt",
            "latency_efficiency",
            "composite_efficiency",
        ]
        baselines: Dict[str, Dict[str, float]] = {}
        for metric in metrics:
            values = [r[metric] for r in rankings]
            baselines[metric] = {
                "min": float(np.min(values)),
                "p25": float(np.percentile(values, 25)),
                "p50": float(np.percentile(values, 50)),
                "p75": float(np.percentile(values, 75)),
                "p90": float(np.percentile(values, 90)),
                "p95": float(np.percentile(values, 95)),
                "max": float(np.max(values)),
                "mean": float(np.mean(values)),
                "std": float(np.std(values)),
            }
        logger.info("Established baselines from %d results.", len(rankings))
        return baselines

    def detect_efficiency_outliers(
        self, results: Iterable[Dict], threshold_std: float = 2.0
    ) -> List[Dict]:
        """Flag agents with composite efficiency > threshold_std below the mean."""
        if threshold_std <= 0:
            raise EfficiencyCalculatorError("threshold_std must be > 0.")

        comparison = self.compare_across_complexities(results, record=False)
        rankings = comparison["rankings"]

        if len(rankings) < 3:
            logger.warning("Too few results for outlier detection (need >= 3).")
            return []

        efficiencies = [r["composite_efficiency"] for r in rankings]
        mean_eff = float(np.mean(efficiencies))
        std_eff = float(np.std(efficiencies))
        if std_eff <= self._epsilon:
            logger.info("Zero variance in composite efficiency; no outliers.")
            return []

        outliers: List[Dict[str, Any]] = []
        for ranking in rankings:
            z_score = (ranking["composite_efficiency"] - mean_eff) / std_eff
            if z_score < -threshold_std:
                outliers.append(
                    {
                        "agent_id": ranking["agent_id"],
                        "task_id": ranking["task_id"],
                        "composite_efficiency": ranking["composite_efficiency"],
                        "z_score": z_score,
                        "deviation": f"{abs(z_score):.1f} std below mean",
                        "recommendation": self._generate_efficiency_recommendation(
                            ranking
                        ),
                    }
                )
        logger.info("Detected %d efficiency outliers.", len(outliers))
        return outliers

    def _generate_efficiency_recommendation(self, ranking: Dict) -> str:
        cfg = self._config
        issues: List[str] = []

        if ranking["energy_efficiency"] > cfg.poor_energy_efficiency_threshold:
            issues.append("High energy consumption relative to task complexity")
        if ranking["accuracy_per_watt"] < cfg.poor_accuracy_per_watt_threshold:
            issues.append("Low accuracy per watt - consider more efficient models")
        if ranking["latency_efficiency"] < cfg.poor_latency_efficiency_threshold:
            issues.append("Slow execution - optimize inference speed")

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

    def reset(self, *, clear_history: bool = False) -> None:
        with self._lock:
            if clear_history:
                self._history.clear()
        logger.debug("Reset NormalizedEfficiencyCalculator (clear_history=%s)", clear_history)

    # -------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "epsilon": self._epsilon,
                "strict": self._strict,
                "max_history": self._max_history,
                "history": [s.to_dict() for s in self._history],
            }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NormalizedEfficiencyCalculator":
        cfg_data = data.get("config", {})
        cfg = NormalizationConfig(
            min_complexity_score=cfg_data.get("min_complexity_score", 0.01),
            min_energy_kwh=cfg_data.get("min_energy_kwh", 1e-6),
            min_latency_ms=cfg_data.get("min_latency_ms", 1.0),
            acc_per_watt_reference=cfg_data.get("acc_per_watt_reference", 500.0),
            energy_efficiency_ceiling=cfg_data.get("energy_efficiency_ceiling", 0.01),
            carbon_efficiency_ceiling=cfg_data.get("carbon_efficiency_ceiling", 0.002),
            latency_efficiency_reference=cfg_data.get("latency_efficiency_reference", 10.0),
            poor_energy_efficiency_threshold=cfg_data.get(
                "poor_energy_efficiency_threshold", 0.01
            ),
            poor_accuracy_per_watt_threshold=cfg_data.get(
                "poor_accuracy_per_watt_threshold", 100.0
            ),
            poor_latency_efficiency_threshold=cfg_data.get(
                "poor_latency_efficiency_threshold", 1.0
            ),
            default_weights=cfg_data.get("default_weights", None) or {
                "accuracy_per_watt": 0.4,
                "energy_efficiency": 0.3,
                "carbon_efficiency": 0.2,
                "latency_efficiency": 0.1,
            },
        )
        calc = cls(
            config=cfg,
            epsilon=float(data.get("epsilon", 1e-12)),
            strict=bool(data.get("strict", True)),
            max_history=data.get("max_history", 1000),
        )
        with calc._lock:
            for entry in data.get("history", []):
                calc._history.append(ComparisonSample(**entry))
        return calc

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "NormalizedEfficiencyCalculator":
        return cls.from_dict(json.loads(payload))

    # ------------------------------------------------------------ context mgr
    def __enter__(self) -> "NormalizedEfficiencyCalculator":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped efficiency comparison.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - getattr(self, "_ctx_start", time.perf_counter())
        if exc_type is not None:
            logger.debug(
                "Exiting scope after %.4fs due to %s.", elapsed, exc_type.__name__
            )
            return
        logger.info("Scoped efficiency comparison completed in %.4fs.", elapsed)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "NormalizedEfficiencyCalculator("
            f"strict={self._strict}, epsilon={self._epsilon:.1e}, "
            f"max_history={self._max_history}, samples={len(self._history)})"
        )


# Backward-compatible alias for callers using the shorter name.
EfficiencyCalculator = NormalizedEfficiencyCalculator


# --------------------------------------------------------------------------- #
# Local smoke test: python -m metrics.efficiency_calculator
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    # Minimal trace mock — adjust if ComplexityAnalyzer expects more fields.
    sample_trace = {"steps": 10, "branches": 2, "tools_called": 3}

    calc = NormalizedEfficiencyCalculator(strict=False, max_history=5)

    results = [
        {
            "agent_id": "A",
            "task_id": "t1",
            "accuracy": 0.95,
            "energy_kwh": 0.01,
            "carbon_kg": 0.003,
            "latency_ms": 120.0,
            "trace": sample_trace,
        },
        {
            "agent_id": "B",
            "task_id": "t2",
            "accuracy": 0.90,
            "energy_kwh": 0.003,
            "carbon_kg": 0.001,
            "latency_ms": 90.0,
            "trace": sample_trace,
        },
    ]

    try:
        comparison = calc.compare_across_complexities(results, label="smoke")
        print("Best agent:", comparison["summary"].get("best_agent"))
        print("Summary   :", comparison["summary"])
        print("Baselines :", calc.benchmark_efficiency_baseline(results))
        print("Outliers  :", calc.detect_efficiency_outliers(results))

        # Serialization round-trip
        payload = calc.to_json()
        restored = NormalizedEfficiencyCalculator.from_json(payload)
        assert restored.to_dict() == calc.to_dict()
        print("Serialization round-trip OK.")

        # Context manager
        with NormalizedEfficiencyCalculator(strict=False) as scoped:
            scoped.calculate_accuracy_per_watt(0.9, 0.0)  # non-strict branch
    except Exception as exc:
        logger.exception("Smoke test failed: %s", exc)
