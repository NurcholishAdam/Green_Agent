# src/benchmarking/benchmark_intelligence.py

"""
Benchmark Intelligence Layer
=============================

Multi-dimensional benchmarking that tracks accuracy, energy, carbon, cost,
and efficiency. Reveals eco-efficient models through comprehensive
performance analysis.

Location: src/benchmarking/benchmark_intelligence.py

Enhancements
------------
- Fixes the ``timedelta`` NameError in :meth:`BenchmarkIntelligence.get_efficiency_trends`.
- Thread-safe via ``RLock``.
- All magic numbers centralized in :class:`BenchmarkConfig`.
- Configurable efficiency weights and normalization ranges.
- Bounded results ring-buffer (``max_results``).
- Validation of every numeric input; strict / non-strict modes.
- Atomic file writes (temp + ``os.replace``) for the results file.
- Structured serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Context-manager support for scoped recording batches.
- Custom :class:`BenchmarkIntelligenceError`.
- Lazy ``%s`` logging, ``__repr__``, and a smoke test under ``__main__``.
"""

from __future__ import annotations

import json
import logging
import math
import os
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Union

import numpy as np

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class BenchmarkIntelligenceError(ValueError):
    """Raised for invalid inputs, configuration, or corrupted results files."""


# --------------------------------------------------------------------------- #
# Benchmark categories
# --------------------------------------------------------------------------- #
class BenchmarkCategory(Enum):
    """Benchmark categories."""

    TEXT_CLASSIFICATION = "text_classification"
    QUESTION_ANSWERING = "question_answering"
    TEXT_GENERATION = "text_generation"
    IMAGE_CLASSIFICATION = "image_classification"
    OBJECT_DETECTION = "object_detection"
    AGENT_SIMULATION = "agent_simulation"


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class BenchmarkConfig:
    """
    Tunable parameters for the Benchmark Intelligence layer.

    Centralizes weights, normalization ranges, and file settings so callers
    can calibrate without editing the class.
    """

    # Composite efficiency weights (must sum to 1.0).
    weight_accuracy: float = 0.40
    weight_energy: float = 0.30
    weight_carbon: float = 0.20
    weight_cost: float = 0.10

    # Reference ceilings used for normalization (metric / ceiling, capped at 1).
    energy_ceiling_kwh: float = 10.0
    carbon_ceiling_kgco2e: float = 5.0
    cost_ceiling_usd: float = 2.0

    # Bounded ring-buffer of in-memory results.
    max_results: Optional[int] = 10_000

    # Default field / filter settings.
    default_hardware: str = "V100"
    default_region: str = "US-CA"
    default_team: str = "default"

    # Default leaderboard / eco-champion settings.
    default_leaderboard_limit: int = 10
    default_eco_min_accuracy: float = 0.80
    default_eco_top_n: int = 5
    default_trends_window_days: int = 30

    def __post_init__(self) -> None:
        weight_sum = (
            self.weight_accuracy
            + self.weight_energy
            + self.weight_carbon
            + self.weight_cost
        )
        if abs(weight_sum - 1.0) > 1e-6:
            raise BenchmarkIntelligenceError(
                f"Efficiency weights must sum to 1.0 (got {weight_sum:.6f})."
            )
        for name in ("weight_accuracy", "weight_energy", "weight_carbon", "weight_cost"):
            if getattr(self, name) < 0:
                raise BenchmarkIntelligenceError(f"{name} must be >= 0.")
        for name in ("energy_ceiling_kwh", "carbon_ceiling_kgco2e", "cost_ceiling_usd"):
            if getattr(self, name) <= 0:
                raise BenchmarkIntelligenceError(f"{name} must be > 0.")
        if self.max_results is not None and self.max_results <= 0:
            raise BenchmarkIntelligenceError("max_results must be > 0 or None.")
        if self.default_leaderboard_limit <= 0:
            raise BenchmarkIntelligenceError("default_leaderboard_limit must be > 0.")
        if not 0.0 <= self.default_eco_min_accuracy <= 1.0:
            raise BenchmarkIntelligenceError(
                "default_eco_min_accuracy must be in [0, 1]."
            )
        if self.default_eco_top_n <= 0:
            raise BenchmarkIntelligenceError("default_eco_top_n must be > 0.")
        if self.default_trends_window_days <= 0:
            raise BenchmarkIntelligenceError(
                "default_trends_window_days must be > 0."
            )


# --------------------------------------------------------------------------- #
# Data containers (kept mutable for backward compatibility)
# --------------------------------------------------------------------------- #
@dataclass
class BenchmarkMetrics:
    """Complete benchmark metrics."""

    # Traditional metrics
    accuracy: float
    f1_score: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None

    # Performance metrics
    latency_ms: float = 0.0
    throughput_samples_per_sec: float = 0.0

    # Resource metrics
    energy_kwh: float = 0.0
    carbon_kgco2e: float = 0.0
    cost_usd: float = 0.0
    memory_gb: float = 0.0

    # Efficiency metrics
    energy_per_sample_wh: float = 0.0
    carbon_per_sample_gco2e: float = 0.0
    performance_per_watt: float = 0.0
    carbon_per_accuracy_point: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class BenchmarkResult:
    """Complete benchmark result."""

    benchmark_id: str
    timestamp: datetime
    model_name: str
    model_params: int
    dataset: str
    category: BenchmarkCategory
    metrics: BenchmarkMetrics
    hardware: str
    region: str
    team: str

    # Composite scores
    efficiency_score: float
    eco_efficiency_rank: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "timestamp": self.timestamp.isoformat(),
            "category": self.category.value,
            "metrics": self.metrics.to_dict(),
        }


@dataclass
class LeaderboardEntry:
    """Entry in the efficiency leaderboard."""

    rank: int
    model_name: str
    accuracy: float
    energy_kwh: float
    carbon_kgco2e: float
    cost_usd: float
    efficiency_score: float
    performance_per_watt: float
    carbon_per_accuracy: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Intelligence layer
# --------------------------------------------------------------------------- #
class BenchmarkIntelligence:
    """
    Intelligent benchmarking system.

    Tracks accuracy, latency, energy, carbon, cost, and composite efficiency.
    Generates multi-dimensional leaderboards, Pareto frontiers, efficiency
    trends, and eco-efficiency champions.

    Thread-safe, serializable, and bounded in memory. See :class:`BenchmarkConfig`
    for tunable weights and thresholds.
    """

    # Valid sort keys for ``get_leaderboard``.
    _VALID_SORT_KEYS = (
        "efficiency_score",
        "performance_per_watt",
        "carbon_per_accuracy",
        "accuracy",
        "energy_kwh",
        "carbon_kgco2e",
        "cost_usd",
        "latency_ms",
    )

    # Valid metrics for Pareto axis selection.
    _VALID_AXIS_METRICS = (
        "accuracy",
        "energy_kwh",
        "carbon_kgco2e",
        "cost_usd",
        "latency_ms",
        "performance_per_watt",
        "carbon_per_accuracy_point",
    )

    def __init__(
        self,
        results_path: Optional[Path] = None,
        *,
        config: Optional[BenchmarkConfig] = None,
        strict: bool = True,
        autosave: bool = True,
        auto_load: bool = True,
    ) -> None:
        """
        Parameters
        ----------
        results_path : Path, optional
            File used to persist results. Defaults to
            ``data/benchmark_results.json`` (matching legacy behavior).
        config : BenchmarkConfig, optional
            Weights and thresholds. Defaults to ``BenchmarkConfig()``.
        strict : bool, default True
            If True, malformed inputs raise :class:`BenchmarkIntelligenceError`.
            If False, they are logged and coerced to safe defaults.
        autosave : bool, default True
            If True, ``record_benchmark`` persists immediately.
        auto_load : bool, default True
            If True, loads existing results from ``results_path`` on init.
        """
        self.results_path: Path = Path(results_path) if results_path else Path(
            "data/benchmark_results.json"
        )
        self._config: BenchmarkConfig = config or BenchmarkConfig()
        self._strict: bool = bool(strict)
        self._autosave: bool = bool(autosave)

        self._lock = threading.RLock()
        self.results: List[BenchmarkResult] = []
        self._ctx_start: Optional[float] = None

        if auto_load:
            self._load_results()

        logger.info(
            "Benchmark Intelligence initialized with %d results (strict=%s).",
            len(self.results),
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> BenchmarkConfig:
        return self._config

    @property
    def results_count(self) -> int:
        with self._lock:
            return len(self.results)

    # -------------------------------------------------------------- loading
    def _load_results(self) -> None:
        """Load benchmark results from disk (missing file is not an error)."""
        if not self.results_path.exists():
            logger.debug("Results file %s not found; starting empty.", self.results_path)
            return

        try:
            with self.results_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as exc:
            logger.error("Corrupted results file %s: %s", self.results_path, exc)
            if self._strict:
                raise BenchmarkIntelligenceError(
                    f"Corrupted results file: {self.results_path}"
                ) from exc
            logger.warning("Starting with empty results (non-strict mode).")
            return
        except OSError as exc:
            logger.error("Could not read results file %s: %s", self.results_path, exc)
            if self._strict:
                raise BenchmarkIntelligenceError(
                    f"Could not read results file: {self.results_path}"
                ) from exc
            return

        if not isinstance(data, list):
            msg = (
                f"Results file root must be a list, got {type(data).__name__}."
            )
            if self._strict:
                raise BenchmarkIntelligenceError(msg)
            logger.warning(msg + " Starting with empty results.")
            return

        loaded: List[BenchmarkResult] = []
        for i, raw in enumerate(data):
            try:
                loaded.append(self._result_from_raw(raw))
            except (BenchmarkIntelligenceError, TypeError, ValueError, KeyError) as exc:
                if self._strict:
                    raise BenchmarkIntelligenceError(
                        f"Malformed result at index {i}: {exc}"
                    ) from exc
                logger.warning("Skipping malformed result at index %d: %s", i, exc)

        with self._lock:
            # Apply ring-buffer cap after loading.
            if (
                self._config.max_results is not None
                and len(loaded) > self._config.max_results
            ):
                loaded = loaded[-self._config.max_results:]
            self.results = loaded

    def _result_from_raw(self, raw: Mapping[str, Any]) -> BenchmarkResult:
        """Deserialize one raw dict into a :class:`BenchmarkResult`."""
        if not isinstance(raw, Mapping):
            raise BenchmarkIntelligenceError(
                f"result must be a Mapping, got {type(raw).__name__}."
            )

        metrics_raw = raw.get("metrics", {})
        if not isinstance(metrics_raw, Mapping):
            raise BenchmarkIntelligenceError("metrics must be a Mapping.")

        # Keep only fields that exist on BenchmarkMetrics to be forward-compatible.
        metric_fields = set(BenchmarkMetrics.__dataclass_fields__.keys())
        filtered_metrics = {k: v for k, v in metrics_raw.items() if k in metric_fields}

        ts_raw = raw.get("timestamp")
        if isinstance(ts_raw, str):
            timestamp = datetime.fromisoformat(ts_raw)
        elif isinstance(ts_raw, datetime):
            timestamp = ts_raw
        else:
            timestamp = datetime.now()

        cat_raw = raw.get("category")
        if isinstance(cat_raw, BenchmarkCategory):
            category = cat_raw
        else:
            category = BenchmarkCategory(str(cat_raw))

        return BenchmarkResult(
            benchmark_id=str(raw["benchmark_id"]),
            timestamp=timestamp,
            model_name=str(raw["model_name"]),
            model_params=int(raw.get("model_params", 0)),
            dataset=str(raw.get("dataset", "")),
            category=category,
            metrics=BenchmarkMetrics(**filtered_metrics),
            hardware=str(raw.get("hardware", self._config.default_hardware)),
            region=str(raw.get("region", self._config.default_region)),
            team=str(raw.get("team", self._config.default_team)),
            efficiency_score=float(raw.get("efficiency_score", 0.0)),
            eco_efficiency_rank=raw.get("eco_efficiency_rank"),
        )

    # -------------------------------------------------------------- saving
    def _save_results(self) -> None:
        """Atomic save: write to temp file, then ``os.replace``."""
        path = self.results_path
        path.parent.mkdir(parents=True, exist_ok=True)

        with self._lock:
            payload = [r.to_dict() for r in self.results]

        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2)
                os.replace(tmp_path, path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except OSError as exc:
            logger.error("Failed to save results to %s: %s", path, exc)
            if self._strict:
                raise BenchmarkIntelligenceError(
                    f"Failed to save results: {path}"
                ) from exc

    # --------------------------------------------------------- record_benchmark
    def record_benchmark(
        self,
        model_name: str,
        model_params: int,
        dataset: str,
        category: BenchmarkCategory,
        accuracy: float,
        energy_kwh: float,
        carbon_kgco2e: float,
        cost_usd: float,
        latency_ms: float,
        num_samples: int,
        hardware: str = "V100",
        region: str = "US-CA",
        team: str = "default",
        additional_metrics: Optional[Dict[str, float]] = None,
    ) -> BenchmarkResult:
        """
        Record a benchmark result.

        All numeric inputs are validated. Derived metrics (per-sample energy,
        carbon per accuracy point, throughput, etc.) are computed
        automatically. The composite efficiency score is computed from
        :class:`BenchmarkConfig` weights.

        See the original docstring for parameter meanings; signatures are
        unchanged.
        """
        # ---- Validation --------------------------------------------------
        if not isinstance(model_name, str) or not model_name:
            raise BenchmarkIntelligenceError("model_name must be a non-empty string.")
        if not isinstance(dataset, str) or not dataset:
            raise BenchmarkIntelligenceError("dataset must be a non-empty string.")
        if not isinstance(category, BenchmarkCategory):
            try:
                category = BenchmarkCategory(str(category))
            except ValueError as exc:
                raise BenchmarkIntelligenceError(
                    f"unknown category: {category!r}"
                ) from exc

        model_params = self._validate_int("model_params", model_params, low=0)
        num_samples = self._validate_int("num_samples", num_samples, low=1)
        accuracy = self._validate_number("accuracy", accuracy, low=0.0, high=1.0)
        energy_kwh = self._validate_number("energy_kwh", energy_kwh, low=0.0)
        carbon_kgco2e = self._validate_number(
            "carbon_kgco2e", carbon_kgco2e, low=0.0
        )
        cost_usd = self._validate_number("cost_usd", cost_usd, low=0.0)
        latency_ms = self._validate_number("latency_ms", latency_ms, low=0.0)

        # ---- Derived metrics --------------------------------------------
        energy_per_sample_wh = (energy_kwh * 1000.0) / num_samples
        carbon_per_sample_gco2e = (carbon_kgco2e * 1000.0) / num_samples
        performance_per_watt = accuracy / energy_kwh if energy_kwh > 0 else 0.0
        carbon_per_accuracy = (
            carbon_kgco2e / accuracy if accuracy > 0 else float("inf")
        )
        throughput = (1000.0 / latency_ms) if latency_ms > 0 else 0.0

        # ---- Build metrics (filter optional extras defensively) ---------
        optional_metrics = self._filter_additional_metrics(additional_metrics)
        metrics = BenchmarkMetrics(
            accuracy=accuracy,
            latency_ms=latency_ms,
            throughput_samples_per_sec=throughput,
            energy_kwh=energy_kwh,
            carbon_kgco2e=carbon_kgco2e,
            cost_usd=cost_usd,
            energy_per_sample_wh=energy_per_sample_wh,
            carbon_per_sample_gco2e=carbon_per_sample_gco2e,
            performance_per_watt=performance_per_watt,
            carbon_per_accuracy_point=carbon_per_accuracy,
            **optional_metrics,
        )

        # ---- Composite efficiency score ---------------------------------
        efficiency_score = self._calculate_efficiency_score(metrics)

        # ---- Assemble result --------------------------------------------
        with self._lock:
            next_index = len(self.results)

        result = BenchmarkResult(
            benchmark_id=(
                f"bench_{next_index}_"
                f"{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
            ),
            timestamp=datetime.now(),
            model_name=model_name,
            model_params=model_params,
            dataset=dataset,
            category=category,
            metrics=metrics,
            hardware=hardware,
            region=region,
            team=team,
            efficiency_score=efficiency_score,
        )

        with self._lock:
            self.results.append(result)
            if (
                self._config.max_results is not None
                and len(self.results) > self._config.max_results
            ):
                del self.results[0]

        if self._autosave:
            self._save_results()

        logger.info(
            "Recorded benchmark: %s on %s — accuracy=%.2f%%, energy=%.3f kWh, "
            "carbon=%.3f kgCO2e, efficiency=%.3f",
            model_name,
            dataset,
            accuracy * 100.0,
            energy_kwh,
            carbon_kgco2e,
            efficiency_score,
        )
        return result

    def _filter_additional_metrics(
        self, additional: Optional[Mapping[str, Any]]
    ) -> Dict[str, float]:
        """Validate and filter optional extras to known BenchamrkMetrics fields."""
        allowed = {"f1_score", "precision", "recall", "memory_gb"}
        if not additional:
            return {}
        if not isinstance(additional, Mapping):
            msg = f"additional_metrics must be a Mapping, got {type(additional).__name__}."
            if self._strict:
                raise BenchmarkIntelligenceError(msg)
            logger.warning("%s Ignoring.", msg)
            return {}

        out: Dict[str, float] = {}
        for key, value in additional.items():
            if key not in allowed:
                logger.debug("Ignoring unknown additional metric '%s'.", key)
                continue
            try:
                out[key] = float(value)
            except (TypeError, ValueError):
                if self._strict:
                    raise BenchmarkIntelligenceError(
                        f"additional_metrics['{key}'] must be numeric."
                    )
                logger.warning("Dropping non-numeric additional metric '%s'.", key)
        return out

    # ----------------------------------------------------------- validation
    @staticmethod
    def _validate_number(
        name: str,
        value: Any,
        *,
        low: float,
        high: Optional[float] = None,
    ) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise BenchmarkIntelligenceError(
                f"{name} must be numeric, got {type(value).__name__}."
            )
        fvalue = float(value)
        if math.isnan(fvalue) or math.isinf(fvalue):
            raise BenchmarkIntelligenceError(f"{name} must be finite, got {value!r}.")
        if fvalue < low:
            raise BenchmarkIntelligenceError(f"{name} must be >= {low}, got {fvalue}.")
        if high is not None and fvalue > high:
            raise BenchmarkIntelligenceError(f"{name} must be <= {high}, got {fvalue}.")
        return fvalue

    @staticmethod
    def _validate_int(name: str, value: Any, *, low: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise BenchmarkIntelligenceError(
                f"{name} must be an int, got {type(value).__name__}."
            )
        if value < low:
            raise BenchmarkIntelligenceError(f"{name} must be >= {low}, got {value}.")
        return value

    # ------------------------------------------------------ efficiency score
    def _calculate_efficiency_score(self, metrics: BenchmarkMetrics) -> float:
        """
        Composite efficiency score in [0, 1] (higher is better).

        Weights and normalization ceilings come from :class:`BenchmarkConfig`.
        """
        cfg = self._config
        accuracy_norm = metrics.accuracy  # already in [0, 1]
        energy_efficiency = 1.0 - min(1.0, metrics.energy_kwh / cfg.energy_ceiling_kwh)
        carbon_efficiency = 1.0 - min(
            1.0, metrics.carbon_kgco2e / cfg.carbon_ceiling_kgco2e
        )
        cost_efficiency = 1.0 - min(1.0, metrics.cost_usd / cfg.cost_ceiling_usd)

        return (
            cfg.weight_accuracy * accuracy_norm
            + cfg.weight_energy * energy_efficiency
            + cfg.weight_carbon * carbon_efficiency
            + cfg.weight_cost * cost_efficiency
        )

    # ---------------------------------------------------------- leaderboard
    def get_leaderboard(
        self,
        category: Optional[BenchmarkCategory] = None,
        sort_by: str = "efficiency_score",
        limit: Optional[int] = None,
    ) -> List[LeaderboardEntry]:
        """
        Return the ranked leaderboard.

        Parameters
        ----------
        category : BenchmarkCategory, optional
            Filter results by category.
        sort_by : str
            One of :attr:`_VALID_SORT_KEYS`.
        limit : int, optional
            Maximum entries to return. Defaults to
            ``config.default_leaderboard_limit``.
        """
        if sort_by not in self._VALID_SORT_KEYS:
            raise BenchmarkIntelligenceError(
                f"sort_by must be one of {self._VALID_SORT_KEYS}, got {sort_by!r}."
            )
        if limit is None:
            limit = self._config.default_leaderboard_limit
        if not isinstance(limit, int) or limit <= 0:
            raise BenchmarkIntelligenceError("limit must be a positive int.")

        with self._lock:
            filtered = list(self.results)
        if category is not None:
            if not isinstance(category, BenchmarkCategory):
                raise BenchmarkIntelligenceError(
                    f"category must be a BenchmarkCategory, got {type(category).__name__}."
                )
            filtered = [r for r in filtered if r.category == category]

        if not filtered:
            return []

        sorted_results = self._sort_results(filtered, sort_by)

        with self._lock:
            return [
                LeaderboardEntry(
                    rank=rank,
                    model_name=result.model_name,
                    accuracy=result.metrics.accuracy,
                    energy_kwh=result.metrics.energy_kwh,
                    carbon_kgco2e=result.metrics.carbon_kgco2e,
                    cost_usd=result.metrics.cost_usd,
                    efficiency_score=result.efficiency_score,
                    performance_per_watt=result.metrics.performance_per_watt,
                    carbon_per_accuracy=result.metrics.carbon_per_accuracy_point,
                )
                for rank, result in enumerate(sorted_results[:limit], 1)
            ]

    @staticmethod
    def _sort_results(
        results: List[BenchmarkResult], sort_by: str
    ) -> List[BenchmarkResult]:
        """Apply the sort key matching ``sort_by``."""
        if sort_by == "efficiency_score":
            return sorted(results, key=lambda r: r.efficiency_score, reverse=True)
        if sort_by == "performance_per_watt":
            return sorted(
                results, key=lambda r: r.metrics.performance_per_watt, reverse=True
            )
        if sort_by == "carbon_per_accuracy":
            return sorted(results, key=lambda r: r.metrics.carbon_per_accuracy_point)
        if sort_by == "accuracy":
            return sorted(results, key=lambda r: r.metrics.accuracy, reverse=True)
        if sort_by == "energy_kwh":
            return sorted(results, key=lambda r: r.metrics.energy_kwh)
        if sort_by == "carbon_kgco2e":
            return sorted(results, key=lambda r: r.metrics.carbon_kgco2e)
        if sort_by == "cost_usd":
            return sorted(results, key=lambda r: r.metrics.cost_usd)
        if sort_by == "latency_ms":
            return sorted(results, key=lambda r: r.metrics.latency_ms)
        return list(results)

    # ------------------------------------------------------ pareto frontier
    def get_pareto_frontier(
        self,
        category: Optional[BenchmarkCategory] = None,
        x_metric: str = "carbon_kgco2e",
        y_metric: str = "accuracy",
    ) -> List[BenchmarkResult]:
        """
        Compute the Pareto frontier for the given ``(x, y)`` metric pair.

        ``x_metric`` is minimized, ``y_metric`` is maximized. Raises if either
        axis name is not a valid metric.
        """
        for axis, name in (("x_metric", x_metric), ("y_metric", y_metric)):
            if name not in self._VALID_AXIS_METRICS:
                raise BenchmarkIntelligenceError(
                    f"{axis} must be one of {self._VALID_AXIS_METRICS}, got {name!r}."
                )
        if x_metric == y_metric:
            raise BenchmarkIntelligenceError(
                "x_metric and y_metric must be different."
            )

        with self._lock:
            filtered = list(self.results)
        if category is not None:
            if not isinstance(category, BenchmarkCategory):
                raise BenchmarkIntelligenceError(
                    f"category must be a BenchmarkCategory, got {type(category).__name__}."
                )
            filtered = [r for r in filtered if r.category == category]

        if len(filtered) < 2:
            return filtered

        def _get(result: BenchmarkResult, metric: str) -> float:
            m = result.metrics
            mapping = {
                "accuracy": m.accuracy,
                "energy_kwh": m.energy_kwh,
                "carbon_kgco2e": m.carbon_kgco2e,
                "cost_usd": m.cost_usd,
                "latency_ms": m.latency_ms,
                "performance_per_watt": m.performance_per_watt,
                "carbon_per_accuracy_point": m.carbon_per_accuracy_point,
            }
            return mapping[metric]

        pareto: List[BenchmarkResult] = []
        for candidate in filtered:
            xc = _get(candidate, x_metric)
            yc = _get(candidate, y_metric)
            dominated = False
            for other in filtered:
                if other is candidate:
                    continue
                xo = _get(other, x_metric)
                yo = _get(other, y_metric)
                # ``other`` dominates candidate iff it is <= on x and >= on y,
                # with strict improvement on at least one axis.
                if xo <= xc and yo >= yc and (xo < xc or yo > yc):
                    dominated = True
                    break
            if not dominated:
                pareto.append(candidate)

        pareto.sort(key=lambda r: _get(r, x_metric))
        return pareto

    # --------------------------------------------------------- trends
    def get_efficiency_trends(
        self,
        window_days: Optional[int] = None,
    ) -> Dict[str, List[Tuple[datetime, float]]]:
        """
        Return efficiency trends over the last ``window_days`` days.

        Default window comes from ``config.default_trends_window_days``.
        Note: the original implementation referenced ``timedelta`` without
        importing it at module scope — that bug is fixed here.
        """
        if window_days is None:
            window_days = self._config.default_trends_window_days
        if not isinstance(window_days, int) or window_days <= 0:
            raise BenchmarkIntelligenceError("window_days must be a positive int.")

        cutoff = datetime.now() - timedelta(days=window_days)
        with self._lock:
            recent = [r for r in self.results if r.timestamp >= cutoff]
        if not recent:
            return {}

        recent.sort(key=lambda r: r.timestamp)

        return {
            "efficiency_score": [(r.timestamp, r.efficiency_score) for r in recent],
            "energy_kwh": [(r.timestamp, r.metrics.energy_kwh) for r in recent],
            "carbon_kgco2e": [(r.timestamp, r.metrics.carbon_kgco2e) for r in recent],
            "performance_per_watt": [
                (r.timestamp, r.metrics.performance_per_watt) for r in recent
            ],
        }

    # --------------------------------------------------------- eco champions
    def get_eco_champions(
        self,
        category: Optional[BenchmarkCategory] = None,
        min_accuracy: Optional[float] = None,
        top_n: Optional[int] = None,
    ) -> List[BenchmarkResult]:
        """
        Return top eco-efficient models (high accuracy, low carbon).

        Parameters
        ----------
        category : BenchmarkCategory, optional
            Filter results by category.
        min_accuracy : float, optional
            Minimum accuracy threshold. Defaults to
            ``config.default_eco_min_accuracy``.
        top_n : int, optional
            Number of champions to return. Defaults to
            ``config.default_eco_top_n``.
        """
        if min_accuracy is None:
            min_accuracy = self._config.default_eco_min_accuracy
        if not 0.0 <= min_accuracy <= 1.0:
            raise BenchmarkIntelligenceError("min_accuracy must be in [0, 1].")
        if top_n is None:
            top_n = self._config.default_eco_top_n
        if not isinstance(top_n, int) or top_n <= 0:
            raise BenchmarkIntelligenceError("top_n must be a positive int.")

        with self._lock:
            filtered = list(self.results)
        if category is not None:
            if not isinstance(category, BenchmarkCategory):
                raise BenchmarkIntelligenceError(
                    f"category must be a BenchmarkCategory, got {type(category).__name__}."
                )
            filtered = [r for r in filtered if r.category == category]
        filtered = [r for r in filtered if r.metrics.accuracy >= min_accuracy]
        if not filtered:
            return []

        filtered.sort(key=lambda r: r.metrics.carbon_per_accuracy_point)
        return filtered[:top_n]

    # --------------------------------------------------------- statistics
    def get_statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the recorded results."""
        with self._lock:
            results = list(self.results)

        if not results:
            return {"num_benchmarks": 0}

        accuracies = [r.metrics.accuracy for r in results]
        energies = [r.metrics.energy_kwh for r in results]
        carbons = [r.metrics.carbon_kgco2e for r in results]
        costs = [r.metrics.cost_usd for r in results]
        effs = [r.efficiency_score for r in results]

        return {
            "num_benchmarks": len(results),
            "avg_accuracy": float(np.mean(accuracies)),
            "avg_energy_kwh": float(np.mean(energies)),
            "avg_carbon_kgco2e": float(np.mean(carbons)),
            "avg_cost_usd": float(np.mean(costs)),
            "total_energy_kwh": float(np.sum(energies)),
            "total_carbon_kgco2e": float(np.sum(carbons)),
            "total_cost_usd": float(np.sum(costs)),
            "best_efficiency_score": float(max(effs)),
            "median_efficiency_score": float(np.median(effs)),
            "p95_efficiency_score": float(np.percentile(effs, 95)),
            "categories": sorted({r.category.value for r in results}),
            "models": sorted({r.model_name for r in results}),
        }

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`get_statistics` (consistent with other modules)."""
        return self.get_statistics()

    # --------------------------------------------------------- lifecycle
    def reset(self, *, clear_results: bool = False) -> None:
        """Reset the in-memory state; optionally clear results (and persist)."""
        with self._lock:
            if clear_results:
                self.results.clear()
        if clear_results and self._autosave:
            self._save_results()
        logger.debug("BenchmarkIntelligence reset (clear_results=%s)", clear_results)

    def save(self) -> None:
        """Persist current results to disk (explicit save)."""
        self._save_results()

    def refresh(self) -> int:
        """Re-read results from disk. Returns the new result count."""
        self._load_results()
        with self._lock:
            return len(self.results)

    # ----------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "strict": self._strict,
                "autosave": self._autosave,
                "results_path": str(self.results_path),
                "results": [r.to_dict() for r in self.results],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        results_path: Optional[Union[str, Path]] = None,
        autosave: bool = False,
        auto_load: bool = False,
    ) -> "BenchmarkIntelligence":
        if not isinstance(data, Mapping):
            raise BenchmarkIntelligenceError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )

        cfg_data = dict(data.get("config", {}) or {})
        cfg = BenchmarkConfig(
            weight_accuracy=float(cfg_data.get("weight_accuracy", 0.40)),
            weight_energy=float(cfg_data.get("weight_energy", 0.30)),
            weight_carbon=float(cfg_data.get("weight_carbon", 0.20)),
            weight_cost=float(cfg_data.get("weight_cost", 0.10)),
            energy_ceiling_kwh=float(cfg_data.get("energy_ceiling_kwh", 10.0)),
            carbon_ceiling_kgco2e=float(cfg_data.get("carbon_ceiling_kgco2e", 5.0)),
            cost_ceiling_usd=float(cfg_data.get("cost_ceiling_usd", 2.0)),
            max_results=cfg_data.get("max_results", 10_000),
            default_hardware=str(cfg_data.get("default_hardware", "V100")),
            default_region=str(cfg_data.get("default_region", "US-CA")),
            default_team=str(cfg_data.get("default_team", "default")),
            default_leaderboard_limit=int(
                cfg_data.get("default_leaderboard_limit", 10)
            ),
            default_eco_min_accuracy=float(
                cfg_data.get("default_eco_min_accuracy", 0.80)
            ),
            default_eco_top_n=int(cfg_data.get("default_eco_top_n", 5)),
            default_trends_window_days=int(
                cfg_data.get("default_trends_window_days", 30)
            ),
        )

        resolved_path = results_path or data.get("results_path") or ":memory:"
        intel = cls(
            results_path=Path(resolved_path) if not isinstance(resolved_path, Path) else resolved_path,
            config=cfg,
            strict=bool(data.get("strict", True)),
            autosave=autosave,
            auto_load=auto_load,
        )
        with intel._lock:
            intel.results = [
                intel._result_from_raw(raw) for raw in data.get("results", [])
            ]
        return intel

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(
        cls, payload: str, *, results_path: Optional[Union[str, Path]] = None
    ) -> "BenchmarkIntelligence":
        try:
            return cls.from_dict(json.loads(payload), results_path=results_path)
        except json.JSONDecodeError as exc:
            raise BenchmarkIntelligenceError(f"Invalid JSON payload: {exc}") from exc

    # ------------------------------------------------------- context mgr
    def __enter__(self) -> "BenchmarkIntelligence":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped benchmark session.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "Benchmark session exited with %s after %.4fs; not saving.",
                exc_type.__name__,
                elapsed,
            )
            return
        if self._autosave:
            try:
                self._save_results()
            except BenchmarkIntelligenceError:
                logger.exception("Failed to persist benchmark results on context exit.")
        logger.info(
            "Benchmark session closed in %.4fs (%d result(s)).",
            elapsed,
            len(self.results),
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "BenchmarkIntelligence("
            f"results={len(self.results)}, "
            f"path={str(self.results_path)!r}, "
            f"strict={self._strict}, "
            f"autosave={self._autosave})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "BenchmarkCategory",
    "BenchmarkMetrics",
    "BenchmarkResult",
    "LeaderboardEntry",
    "BenchmarkIntelligence",
    "BenchmarkConfig",
    "BenchmarkIntelligenceError",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m benchmarking.benchmark_intelligence
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    import tempfile as _tf

    tmpdir = _tf.mkdtemp(prefix="benchmark_intelligence_smoke_")
    results_file = Path(tmpdir) / "benchmark_results.json"

    intelligence = BenchmarkIntelligence(results_path=results_file)

    models = [
        ("bert-base", 110_000_000, 0.92, 0.8, 0.32),
        ("bert-large", 340_000_000, 0.94, 2.5, 1.0),
        ("distilbert", 66_000_000, 0.90, 0.3, 0.12),
        ("roberta-base", 125_000_000, 0.93, 1.0, 0.40),
    ]

    for model_name, params, acc, energy, carbon in models:
        intelligence.record_benchmark(
            model_name=model_name,
            model_params=params,
            dataset="sst2",
            category=BenchmarkCategory.TEXT_CLASSIFICATION,
            accuracy=acc,
            energy_kwh=energy,
            carbon_kgco2e=carbon,
            cost_usd=energy * 0.20,
            latency_ms=50,
            num_samples=1000,
            team="nlp_research",
        )

    # ---- Leaderboard --------------------------------------------------
    print(f"\n{'=' * 80}")
    print("EFFICIENCY LEADERBOARD")
    print(f"{'=' * 80}")
    print(
        f"{'Rank':<6} {'Model':<20} {'Accuracy':<10} "
        f"{'Energy':<12} {'Carbon':<12} {'Efficiency':<12}"
    )
    print("-" * 80)
    for entry in intelligence.get_leaderboard(sort_by="efficiency_score", limit=10):
        print(
            f"{entry.rank:<6} {entry.model_name:<20} {entry.accuracy:<10.1%} "
            f"{entry.energy_kwh:<12.2f} {entry.carbon_kgco2e:<12.3f} "
            f"{entry.efficiency_score:<12.3f}"
        )

    # ---- Pareto frontier ---------------------------------------------
    print(f"\n{'=' * 80}")
    print("PARETO FRONTIER (Carbon vs Accuracy)")
    print(f"{'=' * 80}")
    for result in intelligence.get_pareto_frontier(
        x_metric="carbon_kgco2e", y_metric="accuracy"
    ):
        print(
            f"  • {result.model_name}: {result.metrics.accuracy:.1%} accuracy, "
            f"{result.metrics.carbon_kgco2e:.3f} kgCO2e"
        )

    # ---- Eco champions -----------------------------------------------
    print(f"\n{'=' * 80}")
    print("ECO-EFFICIENCY CHAMPIONS")
    print(f"{'=' * 80}")
    for result in intelligence.get_eco_champions(min_accuracy=0.90):
        print(
            f"  🏆 {result.model_name}: {result.metrics.accuracy:.1%} accuracy, "
            f"{result.metrics.carbon_per_accuracy_point:.3f} kgCO2e per accuracy point"
        )

    # ---- Statistics --------------------------------------------------
    stats = intelligence.get_statistics()
    print(f"\n{'=' * 80}")
    print("BENCHMARK STATISTICS")
    print(f"{'=' * 80}")
    print(f"Total Benchmarks    : {stats['num_benchmarks']}")
    print(f"Avg Accuracy        : {stats['avg_accuracy']:.1%}")
    print(f"Avg Energy          : {stats['avg_energy_kwh']:.2f} kWh")
    print(f"Avg Carbon          : {stats['avg_carbon_kgco2e']:.3f} kgCO2e")
    print(f"Best Efficiency     : {stats['best_efficiency_score']:.3f}")

    # ---- Trends (previously raised NameError) ------------------------
    trends = intelligence.get_efficiency_trends(window_days=1)
    print(f"\nTrend series        : {sorted(trends.keys())}")

    # ---- Serialization round-trip ------------------------------------
    payload = intelligence.to_json()
    restored = BenchmarkIntelligence.from_json(payload, results_path=":memory:")
    assert restored.to_dict() == intelligence.to_dict()
    print("Serialization round-trip OK.")

    # ---- Context manager ---------------------------------------------
    with BenchmarkIntelligence(
        results_path=Path(tmpdir) / "ctx.json", autosave=True
    ) as scoped:
        scoped.record_benchmark(
            model_name="mini",
            model_params=1_000_000,
            dataset="sst2",
            category=BenchmarkCategory.TEXT_CLASSIFICATION,
            accuracy=0.85,
            energy_kwh=0.05,
            carbon_kgco2e=0.02,
            cost_usd=0.01,
            latency_ms=20,
            num_samples=500,
        )
    print("Context-managed session OK.")

    # ---- Validation failures -----------------------------------------
    for bad in (
        dict(model_name="", model_params=1, dataset="d",
             category=BenchmarkCategory.TEXT_CLASSIFICATION,
             accuracy=0.9, energy_kwh=1.0, carbon_kgco2e=0.1,
             cost_usd=0.1, latency_ms=10, num_samples=100),
        dict(model_name="m", model_params=1, dataset="d",
             category=BenchmarkCategory.TEXT_CLASSIFICATION,
             accuracy=1.5, energy_kwh=1.0, carbon_kgco2e=0.1,
             cost_usd=0.1, latency_ms=10, num_samples=100),
        dict(model_name="m", model_params=1, dataset="d",
             category=BenchmarkCategory.TEXT_CLASSIFICATION,
             accuracy=0.9, energy_kwh=float("nan"), carbon_kgco2e=0.1,
             cost_usd=0.1, latency_ms=10, num_samples=100),
    ):
        try:
            intelligence.record_benchmark(**bad)
        except BenchmarkIntelligenceError as exc:
            print("Rejected as expected:", exc)
        else:  # pragma: no cover
            raise AssertionError(f"Expected rejection for {bad!r}")

    print("\nSmoke test passed.")
