# src/benchmarking/benchmark_intelligence.py

"""
Benchmark Intelligence Layer
=============================

Multi-dimensional benchmarking that tracks accuracy, energy, carbon, cost,
and efficiency. Reveals eco-efficient models through comprehensive
performance analysis.

Location: src/benchmarking/benchmark_intelligence.py

Enhancements (v2)
-----------------
- **Correct throughput / power / per-watt metrics** using ``num_samples`` and
  ``latency_ms`` (per-sample by default; batch-total optional).
- **JSON-safe serialization**: ``inf``/``nan`` never emitted; round-tripped
  through ``None`` and restored on load.
- **Concurrent-safe saves**: separate ``_save_lock`` serialises writers.
- **Atomic benchmark IDs**: generated inside the final lock, UUID-suffixed.
- **Crash-durable writes**: ``fsync`` before ``os.replace``.
- **``max_results`` applied on ``from_dict``**, not only on file load.
- **``strict=False`` honored in ``record_benchmark``** (clamp / drop instead
  of raise).
- **Bool guards** on all ``int``-typed public parameters.
- **``:memory:`` sentinel** disables persistence entirely.
- **Timezone-aware timestamps** (``datetime.now(timezone.utc)``).
- **Robust timestamp parsing** (trailing ``Z``, naive → UTC).
- **Field-relative efficiency scores** via ``recompute_efficiency_scores``.
- **O(n log n) Pareto sweep** with axis-direction inference.
- **``numpy`` removed** in favor of the ``statistics`` module.
- **``batch()`` contextmanager** for real transaction-style batching.
- **``compare_models`` and ``filter`` helpers**.
- ``_VALID_SORT_KEYS`` / ``_VALID_AXIS_METRICS`` aligned with metric surface.
- New ``accuracy_per_kwh`` (formerly mis-named ``performance_per_watt``),
  ``power_w``, ``cost_per_sample_usd``, ``latency_per_sample_ms``, and
  ``total_benchmark_time_ms`` metrics.
- Lazy ``%s`` logging, ``__repr__``, ``__all__``, and smoke test under
  ``__main__``.
"""

from __future__ import annotations

import contextlib
import json
import logging
import math
import os
import statistics
import tempfile
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

logger = logging.getLogger(__name__)


_MEMORY_SENTINEL = ":memory:"

#: Metrics where a *larger* value is better. Used by the Pareto sweep and
#: leaderboard ranking.
_HIGHER_IS_BETTER = frozenset(
    {
        "accuracy",
        "f1_score",
        "precision",
        "recall",
        "throughput_samples_per_sec",
        "performance_per_watt",
        "accuracy_per_kwh",
    }
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_aware(dt: datetime) -> datetime:
    """Treat naive datetimes as UTC (legacy data migration)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_timestamp(raw: Any) -> datetime:
    """Parse a timestamp, tolerating ``Z`` suffix and naive values."""
    if isinstance(raw, datetime):
        return _ensure_aware(raw)
    if isinstance(raw, str):
        s = raw.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return _ensure_aware(datetime.fromisoformat(s))
        except ValueError:
            return _utcnow()
    return _utcnow()


def _json_safe(obj: Any) -> Any:
    """Recursively replace ``inf``/``nan`` with ``None`` (RFC-8259 JSON)."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    return obj


def _percentile(data: List[float], p: float) -> float:
    """Linear-interpolated percentile (numpy-compatible for p in [0, 100])."""
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(s[int(k)])
    return s[int(f)] * (c - k) + s[int(c)] * (k - f)


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

    weight_accuracy: float = 0.40
    weight_energy: float = 0.30
    weight_carbon: float = 0.20
    weight_cost: float = 0.10

    energy_ceiling_kwh: float = 10.0
    carbon_ceiling_kgco2e: float = 5.0
    cost_ceiling_usd: float = 2.0

    max_results: Optional[int] = 10_000

    default_hardware: str = "V100"
    default_region: str = "US-CA"
    default_team: str = "default"

    default_leaderboard_limit: int = 10
    default_eco_min_accuracy: float = 0.80
    default_eco_top_n: int = 5
    default_trends_window_days: int = 30

    def __post_init__(self) -> None:
        # Coerce numeric fields (JSON round-trips may pass str / int).
        for name in (
            "weight_accuracy",
            "weight_energy",
            "weight_carbon",
            "weight_cost",
            "energy_ceiling_kwh",
            "carbon_ceiling_kgco2e",
            "cost_ceiling_usd",
            "default_eco_min_accuracy",
        ):
            try:
                object.__setattr__(self, name, float(getattr(self, name)))
            except (TypeError, ValueError) as exc:
                raise BenchmarkIntelligenceError(
                    f"{name} must be numeric, got {getattr(self, name)!r}."
                ) from exc

        for name in (
            "default_leaderboard_limit",
            "default_eco_top_n",
            "default_trends_window_days",
        ):
            raw = getattr(self, name)
            if isinstance(raw, bool) or not isinstance(raw, int):
                try:
                    coerced = int(raw)
                except (TypeError, ValueError) as exc:
                    raise BenchmarkIntelligenceError(
                        f"{name} must be int, got {raw!r}."
                    ) from exc
                object.__setattr__(self, name, coerced)

        if self.max_results is not None:
            if isinstance(self.max_results, bool) or not isinstance(
                self.max_results, int
            ):
                try:
                    coerced = int(self.max_results)
                except (TypeError, ValueError) as exc:
                    raise BenchmarkIntelligenceError(
                        f"max_results must be int or None, got "
                        f"{self.max_results!r}."
                    ) from exc
                object.__setattr__(self, "max_results", coerced)

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
        for name in (
            "weight_accuracy",
            "weight_energy",
            "weight_carbon",
            "weight_cost",
        ):
            if getattr(self, name) < 0:
                raise BenchmarkIntelligenceError(f"{name} must be >= 0.")
        for name in (
            "energy_ceiling_kwh",
            "carbon_ceiling_kgco2e",
            "cost_ceiling_usd",
        ):
            if getattr(self, name) <= 0:
                raise BenchmarkIntelligenceError(f"{name} must be > 0.")
        if self.max_results is not None and self.max_results <= 0:
            raise BenchmarkIntelligenceError("max_results must be > 0 or None.")
        if self.default_leaderboard_limit <= 0:
            raise BenchmarkIntelligenceError(
                "default_leaderboard_limit must be > 0."
            )
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
# Data containers
# --------------------------------------------------------------------------- #
@dataclass
class BenchmarkMetrics:
    """
    Complete benchmark metrics.

    Fields ``performance_per_watt`` and ``accuracy_per_kwh`` are distinct:

    * ``accuracy_per_kwh`` – accuracy / energy (legacy "performance per watt"
      semantics, correctly named).
    * ``performance_per_watt`` – accuracy / power (genuine per-watt metric,
      requires ``latency_ms`` and ``num_samples`` to estimate power).
    """

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

    # Extended metrics (v2)
    accuracy_per_kwh: float = 0.0
    power_w: float = 0.0
    cost_per_sample_usd: float = 0.0
    latency_per_sample_ms: float = 0.0
    total_benchmark_time_ms: float = 0.0

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

    efficiency_score: float
    eco_efficiency_rank: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        payload = {
            "benchmark_id": self.benchmark_id,
            "timestamp": _ensure_aware(self.timestamp).isoformat(),
            "model_name": self.model_name,
            "model_params": self.model_params,
            "dataset": self.dataset,
            "category": self.category.value,
            "metrics": self.metrics.to_dict(),
            "hardware": self.hardware,
            "region": self.region,
            "team": self.team,
            "efficiency_score": self.efficiency_score,
            "eco_efficiency_rank": self.eco_efficiency_rank,
        }
        return _json_safe(payload)


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
    accuracy_per_kwh: float = 0.0
    power_w: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


# --------------------------------------------------------------------------- #
# Intelligence layer
# --------------------------------------------------------------------------- #
class BenchmarkIntelligence:
    """
    Intelligent benchmarking system.

    Tracks accuracy, latency, energy, carbon, cost, and composite efficiency.
    Generates multi-dimensional leaderboards, Pareto frontiers, efficiency
    trends, and eco-efficiency champions.

    Thread-safe, serializable, and bounded in memory.
    """

    _VALID_SORT_KEYS: Tuple[str, ...] = (
        "efficiency_score",
        "performance_per_watt",
        "accuracy_per_kwh",
        "carbon_per_accuracy",
        "carbon_per_accuracy_point",
        "accuracy",
        "f1_score",
        "energy_kwh",
        "carbon_kgco2e",
        "cost_usd",
        "latency_ms",
        "throughput_samples_per_sec",
        "power_w",
    )

    _VALID_AXIS_METRICS: Tuple[str, ...] = (
        "accuracy",
        "f1_score",
        "precision",
        "recall",
        "energy_kwh",
        "carbon_kgco2e",
        "cost_usd",
        "latency_ms",
        "throughput_samples_per_sec",
        "performance_per_watt",
        "accuracy_per_kwh",
        "power_w",
        "carbon_per_accuracy_point",
    )

    def __init__(
        self,
        results_path: Optional[Union[str, Path]] = None,
        *,
        config: Optional[BenchmarkConfig] = None,
        strict: bool = True,
        autosave: bool = True,
        auto_load: bool = True,
    ) -> None:
        if results_path is not None and str(results_path).strip() == _MEMORY_SENTINEL:
            self._memory_only: bool = True
            self.results_path: Path = Path(_MEMORY_SENTINEL)
            autosave = False
            auto_load = False
        else:
            self._memory_only = False
            self.results_path = (
                Path(results_path)
                if results_path
                else Path("data/benchmark_results.json")
            )

        self._config: BenchmarkConfig = config or BenchmarkConfig()
        self._strict: bool = bool(strict)
        self._autosave: bool = bool(autosave)

        self._lock = threading.RLock()
        self._save_lock = threading.Lock()
        self.results: List[BenchmarkResult] = []

        # Scoped-session state
        self._ctx_start: Optional[float] = None
        self._ctx_prev_autosave: Optional[bool] = None

        if auto_load and not self._memory_only:
            self._load_results()

        logger.info(
            "Benchmark Intelligence initialized with %d results "
            "(strict=%s, memory=%s).",
            len(self.results),
            self._strict,
            self._memory_only,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> BenchmarkConfig:
        return self._config

    @property
    def results_count(self) -> int:
        with self._lock:
            return len(self.results)

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def autosave(self) -> bool:
        return self._autosave

    # -------------------------------------------------------------- loading
    def _load_results(self) -> None:
        """Load benchmark results from disk (missing file is not an error)."""
        if self._memory_only or not self.results_path.exists():
            logger.debug(
                "Results file %s not loaded (memory=%s, exists=%s).",
                self.results_path,
                self._memory_only,
                self.results_path.exists(),
            )
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
            logger.error(
                "Could not read results file %s: %s", self.results_path, exc
            )
            if self._strict:
                raise BenchmarkIntelligenceError(
                    f"Could not read results file: {self.results_path}"
                ) from exc
            return

        if not isinstance(data, list):
            msg = f"Results file root must be a list, got {type(data).__name__}."
            if self._strict:
                raise BenchmarkIntelligenceError(msg)
            logger.warning("%s Starting with empty results.", msg)
            return

        loaded: List[BenchmarkResult] = []
        for i, raw in enumerate(data):
            try:
                loaded.append(self._result_from_raw(raw))
            except (
                BenchmarkIntelligenceError,
                TypeError,
                ValueError,
                KeyError,
            ) as exc:
                if self._strict:
                    raise BenchmarkIntelligenceError(
                        f"Malformed result at index {i}: {exc}"
                    ) from exc
                logger.warning(
                    "Skipping malformed result at index %d: %s", i, exc
                )

        with self._lock:
            if (
                self._config.max_results is not None
                and len(loaded) > self._config.max_results
            ):
                loaded = loaded[-self._config.max_results :]
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

        metric_fields = set(BenchmarkMetrics.__dataclass_fields__.keys())
        filtered_metrics = {
            k: v for k, v in metrics_raw.items() if k in metric_fields
        }
        # Restore ``inf`` sentinels that were serialized as ``None``.
        for inf_field in ("carbon_per_accuracy_point",):
            if inf_field in filtered_metrics and filtered_metrics[inf_field] is None:
                filtered_metrics[inf_field] = float("inf")

        timestamp = _parse_timestamp(raw.get("timestamp"))

        cat_raw = raw.get("category")
        if isinstance(cat_raw, BenchmarkCategory):
            category = cat_raw
        else:
            try:
                category = BenchmarkCategory(str(cat_raw))
            except ValueError as exc:
                raise BenchmarkIntelligenceError(
                    f"unknown category: {cat_raw!r}"
                ) from exc

        return BenchmarkResult(
            benchmark_id=str(
                raw.get("benchmark_id") or f"bench_{uuid.uuid4().hex[:16]}"
            ),
            timestamp=timestamp,
            model_name=str(raw.get("model_name", "")),
            model_params=int(raw.get("model_params", 0)),
            dataset=str(raw.get("dataset", "")),
            category=category,
            metrics=BenchmarkMetrics(**filtered_metrics),
            hardware=str(raw.get("hardware", self._config.default_hardware)),
            region=str(raw.get("region", self._config.default_region)),
            team=str(raw.get("team", self._config.default_team)),
            efficiency_score=float(raw.get("efficiency_score", 0.0) or 0.0),
            eco_efficiency_rank=raw.get("eco_efficiency_rank"),
        )

    # -------------------------------------------------------------- saving
    def _save_results(self) -> None:
        """Atomic save with fsync + rename, serialised by ``_save_lock``."""
        if self._memory_only:
            return

        path = self.results_path
        path.parent.mkdir(parents=True, exist_ok=True)

        with self._save_lock:
            with self._lock:
                payload = [r.to_dict() for r in self.results]

            tmp_path: Optional[str] = None
            try:
                fd, tmp_path = tempfile.mkstemp(
                    prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
                )
                try:
                    with os.fdopen(fd, "w", encoding="utf-8") as f:
                        json.dump(payload, f, indent=2, allow_nan=False)
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(tmp_path, path)
                    tmp_path = None
                finally:
                    if tmp_path is not None:
                        try:
                            os.unlink(tmp_path)
                        except OSError:
                            pass
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
        hardware: Optional[str] = None,
        region: Optional[str] = None,
        team: Optional[str] = None,
        additional_metrics: Optional[Dict[str, float]] = None,
        *,
        latency_is_per_sample: bool = True,
    ) -> BenchmarkResult:
        """
        Record a benchmark result.

        Parameters
        ----------
        latency_ms : float
            When ``latency_is_per_sample=True`` (default), this is the
            **per-sample** latency; throughput is ``1000 / latency_ms``.
            When ``latency_is_per_sample=False``, it is the total benchmark
            time; throughput is ``num_samples * 1000 / latency_ms``.

        All numeric inputs are validated in strict mode; in non-strict mode
        they are clamped / dropped with a warning.
        """
        # ---- Validation --------------------------------------------------
        if not isinstance(model_name, str) or not model_name.strip():
            raise BenchmarkIntelligenceError(
                "model_name must be a non-empty string."
            )
        if not isinstance(dataset, str) or not dataset.strip():
            raise BenchmarkIntelligenceError(
                "dataset must be a non-empty string."
            )

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

        hardware = self._resolve_label(
            "hardware", hardware, self._config.default_hardware
        )
        region = self._resolve_label("region", region, self._config.default_region)
        team = self._resolve_label("team", team, self._config.default_team)

        # ---- Derived metrics --------------------------------------------
        if latency_is_per_sample:
            throughput = (1000.0 / latency_ms) if latency_ms > 0 else 0.0
            total_benchmark_time_ms = num_samples * latency_ms
        else:
            throughput = (
                (num_samples * 1000.0 / latency_ms) if latency_ms > 0 else 0.0
            )
            total_benchmark_time_ms = latency_ms

        energy_per_sample_wh = (energy_kwh * 1000.0) / num_samples
        carbon_per_sample_gco2e = (carbon_kgco2e * 1000.0) / num_samples
        cost_per_sample_usd = cost_usd / num_samples
        latency_per_sample_ms = total_benchmark_time_ms / num_samples

        total_time_h = total_benchmark_time_ms / 1000.0 / 3600.0
        power_w = (
            (energy_kwh * 1000.0) / total_time_h if total_time_h > 0 else 0.0
        )

        performance_per_watt = accuracy / power_w if power_w > 0 else 0.0
        accuracy_per_kwh = accuracy / energy_kwh if energy_kwh > 0 else 0.0
        carbon_per_accuracy = (
            carbon_kgco2e / accuracy if accuracy > 0 else float("inf")
        )

        # ---- Build metrics ----------------------------------------------
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
            accuracy_per_kwh=accuracy_per_kwh,
            power_w=power_w,
            cost_per_sample_usd=cost_per_sample_usd,
            latency_per_sample_ms=latency_per_sample_ms,
            total_benchmark_time_ms=total_benchmark_time_ms,
            **optional_metrics,
        )

        efficiency_score = self._calculate_efficiency_score(metrics)

        # ---- Assemble + append atomically -------------------------------
        with self._lock:
            next_index = len(self.results)
            result = BenchmarkResult(
                benchmark_id=(
                    f"bench_{next_index}_"
                    f"{_utcnow().strftime('%Y%m%d%H%M%S%f')}_"
                    f"{uuid.uuid4().hex[:8]}"
                ),
                timestamp=_utcnow(),
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
            "carbon=%.3f kgCO2e, power=%.1f W, efficiency=%.3f",
            model_name,
            dataset,
            accuracy * 100.0,
            energy_kwh,
            carbon_kgco2e,
            power_w,
            efficiency_score,
        )
        return result

    def _resolve_label(
        self, name: str, value: Optional[str], default: str
    ) -> str:
        if value is None:
            return default
        if not isinstance(value, str) or not value.strip():
            msg = f"{name} must be a non-empty string, got {value!r}."
            if self._strict:
                raise BenchmarkIntelligenceError(msg)
            logger.warning("Non-strict: %s; using default %r.", msg, default)
            return default
        return value

    def _filter_additional_metrics(
        self, additional: Optional[Mapping[str, Any]]
    ) -> Dict[str, float]:
        """Validate and filter optional extras to known BenchmarkMetrics fields."""
        allowed = {"f1_score", "precision", "recall", "memory_gb"}
        if not additional:
            return {}
        if not isinstance(additional, Mapping):
            msg = (
                f"additional_metrics must be a Mapping, got "
                f"{type(additional).__name__}."
            )
            if self._strict:
                raise BenchmarkIntelligenceError(msg)
            logger.warning("%s Ignoring.", msg)
            return {}

        out: Dict[str, float] = {}
        for key, value in additional.items():
            if key not in allowed:
                logger.debug("Ignoring unknown additional metric '%s'.", key)
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                if self._strict:
                    raise BenchmarkIntelligenceError(
                        f"additional_metrics['{key}'] must be numeric."
                    )
                logger.warning(
                    "Dropping non-numeric additional metric '%s'.", key
                )
                continue
            out[key] = float(value)
        return out

    # ----------------------------------------------------------- validation
    def _validate_number(
        self,
        name: str,
        value: Any,
        *,
        low: float,
        high: Optional[float] = None,
    ) -> float:
        try:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(
                    f"{name} must be numeric, got {type(value).__name__}."
                )
            fvalue = float(value)
            if math.isnan(fvalue) or math.isinf(fvalue):
                raise ValueError(f"{name} must be finite, got {value!r}.")
        except (TypeError, ValueError) as exc:
            if self._strict:
                raise BenchmarkIntelligenceError(str(exc)) from exc
            logger.warning(
                "Non-strict: %s; using %r.", exc, float(low)
            )
            return float(low)

        if fvalue < low:
            msg = f"{name} must be >= {low}, got {fvalue}."
            if self._strict:
                raise BenchmarkIntelligenceError(msg)
            logger.warning("Non-strict: %s; clamping to %r.", msg, low)
            return float(low)
        if high is not None and fvalue > high:
            msg = f"{name} must be <= {high}, got {fvalue}."
            if self._strict:
                raise BenchmarkIntelligenceError(msg)
            logger.warning("Non-strict: %s; clamping to %r.", msg, high)
            return float(high)
        return fvalue

    def _validate_int(self, name: str, value: Any, *, low: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            msg = (
                f"{name} must be an int, got {type(value).__name__}."
            )
            if self._strict:
                raise BenchmarkIntelligenceError(msg)
            logger.warning("Non-strict: %s; using %r.", msg, low)
            return low
        if value < low:
            msg = f"{name} must be >= {low}, got {value}."
            if self._strict:
                raise BenchmarkIntelligenceError(msg)
            logger.warning("Non-strict: %s; clamping to %r.", msg, low)
            return low
        return value

    # ------------------------------------------------------ efficiency score
    def _calculate_efficiency_score(self, metrics: BenchmarkMetrics) -> float:
        """
        Composite efficiency score in [0, 1] (higher is better).

        Uses ceiling-based normalization from :class:`BenchmarkConfig`.
        For field-relative scores, call :meth:`recompute_efficiency_scores`.
        """
        cfg = self._config
        accuracy_norm = max(0.0, min(1.0, metrics.accuracy))
        energy_efficiency = 1.0 - min(
            1.0, max(0.0, metrics.energy_kwh / cfg.energy_ceiling_kwh)
        )
        carbon_efficiency = 1.0 - min(
            1.0, max(0.0, metrics.carbon_kgco2e / cfg.carbon_ceiling_kgco2e)
        )
        cost_efficiency = 1.0 - min(
            1.0, max(0.0, metrics.cost_usd / cfg.cost_ceiling_usd)
        )

        return (
            cfg.weight_accuracy * accuracy_norm
            + cfg.weight_energy * energy_efficiency
            + cfg.weight_carbon * carbon_efficiency
            + cfg.weight_cost * cost_efficiency
        )

    def recompute_efficiency_scores(self, *, relative: bool = False) -> int:
        """
        Recompute ``efficiency_score`` for every stored result.

        With ``relative=True`` the normalization is field-relative (min/max
        over the current result set) instead of ceiling-based. Returns the
        number of results updated.
        """
        with self._lock:
            if not self.results:
                return 0
            cfg = self._config
            if not relative:
                for r in self.results:
                    r.efficiency_score = self._calculate_efficiency_score(r.metrics)
            else:
                energies = [r.metrics.energy_kwh for r in self.results]
                carbons = [r.metrics.carbon_kgco2e for r in self.results]
                costs = [r.metrics.cost_usd for r in self.results]
                e_min, e_max = min(energies), max(energies)
                c_min, c_max = min(carbons), max(carbons)
                m_min, m_max = min(costs), max(costs)
                for r in self.results:
                    acc_norm = max(0.0, min(1.0, r.metrics.accuracy))
                    e_eff = (
                        1.0
                        if e_max == e_min
                        else 1.0
                        - (r.metrics.energy_kwh - e_min) / (e_max - e_min)
                    )
                    c_eff = (
                        1.0
                        if c_max == c_min
                        else 1.0
                        - (r.metrics.carbon_kgco2e - c_min) / (c_max - c_min)
                    )
                    m_eff = (
                        1.0
                        if m_max == m_min
                        else 1.0 - (r.metrics.cost_usd - m_min) / (m_max - m_min)
                    )
                    r.efficiency_score = (
                        cfg.weight_accuracy * acc_norm
                        + cfg.weight_energy * e_eff
                        + cfg.weight_carbon * c_eff
                        + cfg.weight_cost * m_eff
                    )
            count = len(self.results)

        if self._autosave and not self._memory_only:
            self._save_results()
        return count

    # ---------------------------------------------------------- filtering
    def filter(
        self,
        *,
        category: Optional[BenchmarkCategory] = None,
        hardware: Optional[str] = None,
        region: Optional[str] = None,
        team: Optional[str] = None,
        dataset: Optional[str] = None,
        model_name: Optional[str] = None,
        since: Optional[datetime] = None,
    ) -> List[BenchmarkResult]:
        """Return a defensive copy of results matching all provided filters."""
        with self._lock:
            filtered = list(self.results)

        if category is not None:
            if not isinstance(category, BenchmarkCategory):
                raise BenchmarkIntelligenceError(
                    f"category must be BenchmarkCategory, got "
                    f"{type(category).__name__}."
                )
            filtered = [r for r in filtered if r.category == category]
        if hardware is not None:
            filtered = [r for r in filtered if r.hardware == hardware]
        if region is not None:
            filtered = [r for r in filtered if r.region == region]
        if team is not None:
            filtered = [r for r in filtered if r.team == team]
        if dataset is not None:
            filtered = [r for r in filtered if r.dataset == dataset]
        if model_name is not None:
            filtered = [r for r in filtered if r.model_name == model_name]
        if since is not None:
            since_aware = _ensure_aware(since)
            filtered = [
                r for r in filtered if _ensure_aware(r.timestamp) >= since_aware
            ]
        return filtered

    # ---------------------------------------------------------- leaderboard
    def get_leaderboard(
        self,
        category: Optional[BenchmarkCategory] = None,
        sort_by: str = "efficiency_score",
        limit: Optional[int] = None,
    ) -> List[LeaderboardEntry]:
        """Return the ranked leaderboard."""
        if sort_by not in self._VALID_SORT_KEYS:
            raise BenchmarkIntelligenceError(
                f"sort_by must be one of {self._VALID_SORT_KEYS}, got "
                f"{sort_by!r}."
            )
        if limit is None:
            limit = self._config.default_leaderboard_limit
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise BenchmarkIntelligenceError("limit must be a positive int.")

        with self._lock:
            filtered = list(self.results)
        if category is not None:
            if not isinstance(category, BenchmarkCategory):
                raise BenchmarkIntelligenceError(
                    f"category must be a BenchmarkCategory, got "
                    f"{type(category).__name__}."
                )
            filtered = [r for r in filtered if r.category == category]

        if not filtered:
            return []

        sorted_results = self._sort_results(filtered, sort_by)
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
                accuracy_per_kwh=result.metrics.accuracy_per_kwh,
                power_w=result.metrics.power_w,
            )
            for rank, result in enumerate(sorted_results[:limit], 1)
        ]

    @staticmethod
    def _metric_value(result: BenchmarkResult, metric: str) -> float:
        if metric == "efficiency_score":
            return result.efficiency_score
        m = result.metrics
        mapping = {
            "accuracy": m.accuracy,
            "f1_score": m.f1_score if m.f1_score is not None else 0.0,
            "precision": m.precision if m.precision is not None else 0.0,
            "recall": m.recall if m.recall is not None else 0.0,
            "latency_ms": m.latency_ms,
            "throughput_samples_per_sec": m.throughput_samples_per_sec,
            "energy_kwh": m.energy_kwh,
            "carbon_kgco2e": m.carbon_kgco2e,
            "cost_usd": m.cost_usd,
            "memory_gb": m.memory_gb,
            "energy_per_sample_wh": m.energy_per_sample_wh,
            "carbon_per_sample_gco2e": m.carbon_per_sample_gco2e,
            "performance_per_watt": m.performance_per_watt,
            "accuracy_per_kwh": m.accuracy_per_kwh,
            "carbon_per_accuracy_point": m.carbon_per_accuracy_point,
            "power_w": m.power_w,
        }
        return float(mapping[metric])

    @classmethod
    def _sort_results(
        cls, results: List[BenchmarkResult], sort_by: str
    ) -> List[BenchmarkResult]:
        # Normalise legacy alias
        if sort_by == "carbon_per_accuracy":
            sort_by = "carbon_per_accuracy_point"

        higher_is_better = (
            sort_by == "efficiency_score" or sort_by in _HIGHER_IS_BETTER
        )
        return sorted(
            results,
            key=lambda r: cls._metric_value(r, sort_by),
            reverse=higher_is_better,
        )

    # ------------------------------------------------------ pareto frontier
    def get_pareto_frontier(
        self,
        category: Optional[BenchmarkCategory] = None,
        x_metric: str = "carbon_kgco2e",
        y_metric: str = "accuracy",
    ) -> List[BenchmarkResult]:
        """
        Compute the Pareto frontier for ``(x_metric, y_metric)``.

        Direction per axis is inferred from the metric name
        (``_HIGHER_IS_BETTER``). Uses an O(n log n) sweep.
        """
        for axis, name in (("x_metric", x_metric), ("y_metric", y_metric)):
            if name not in self._VALID_AXIS_METRICS:
                raise BenchmarkIntelligenceError(
                    f"{axis} must be one of {self._VALID_AXIS_METRICS}, got "
                    f"{name!r}."
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
                    f"category must be a BenchmarkCategory, got "
                    f"{type(category).__name__}."
                )
            filtered = [r for r in filtered if r.category == category]

        if len(filtered) < 2:
            return filtered

        x_sign = -1.0 if x_metric in _HIGHER_IS_BETTER else 1.0
        y_sign = -1.0 if y_metric in _HIGHER_IS_BETTER else 1.0

        def _x(r: BenchmarkResult) -> float:
            return self._metric_value(r, x_metric)

        def _y(r: BenchmarkResult) -> float:
            return self._metric_value(r, y_metric)

        # Sort by "better-x" ascending, then by "better-y" ascending so that
        # within an x-tie the best-y candidate appears first.
        sorted_items = sorted(
            filtered, key=lambda r: (_x(r) * x_sign, _y(r) * y_sign)
        )

        pareto: List[BenchmarkResult] = []
        best_y_signed = float("inf")
        for candidate in sorted_items:
            y_signed = _y(candidate) * y_sign
            if y_signed < best_y_signed:
                pareto.append(candidate)
                best_y_signed = y_signed
        return pareto

    # --------------------------------------------------------- trends
    def get_efficiency_trends(
        self,
        window_days: Optional[int] = None,
    ) -> Dict[str, List[Tuple[datetime, float]]]:
        """Return efficiency trends over the last ``window_days`` days."""
        if window_days is None:
            window_days = self._config.default_trends_window_days
        if (
            isinstance(window_days, bool)
            or not isinstance(window_days, int)
            or window_days <= 0
        ):
            raise BenchmarkIntelligenceError(
                "window_days must be a positive int."
            )

        cutoff = _utcnow() - timedelta(days=window_days)
        with self._lock:
            recent = [
                r for r in self.results if _ensure_aware(r.timestamp) >= cutoff
            ]
        if not recent:
            return {}

        recent.sort(key=lambda r: _ensure_aware(r.timestamp))

        return {
            "efficiency_score": [
                (_ensure_aware(r.timestamp), r.efficiency_score) for r in recent
            ],
            "energy_kwh": [
                (_ensure_aware(r.timestamp), r.metrics.energy_kwh) for r in recent
            ],
            "carbon_kgco2e": [
                (_ensure_aware(r.timestamp), r.metrics.carbon_kgco2e)
                for r in recent
            ],
            "performance_per_watt": [
                (_ensure_aware(r.timestamp), r.metrics.performance_per_watt)
                for r in recent
            ],
        }

    # --------------------------------------------------------- eco champions
    def get_eco_champions(
        self,
        category: Optional[BenchmarkCategory] = None,
        min_accuracy: Optional[float] = None,
        top_n: Optional[int] = None,
    ) -> List[BenchmarkResult]:
        """Return top eco-efficient models (high accuracy, low carbon)."""
        if min_accuracy is None:
            min_accuracy = self._config.default_eco_min_accuracy
        if not 0.0 <= min_accuracy <= 1.0:
            raise BenchmarkIntelligenceError("min_accuracy must be in [0, 1].")
        if top_n is None:
            top_n = self._config.default_eco_top_n
        if isinstance(top_n, bool) or not isinstance(top_n, int) or top_n <= 0:
            raise BenchmarkIntelligenceError("top_n must be a positive int.")

        with self._lock:
            filtered = list(self.results)
        if category is not None:
            if not isinstance(category, BenchmarkCategory):
                raise BenchmarkIntelligenceError(
                    f"category must be a BenchmarkCategory, got "
                    f"{type(category).__name__}."
                )
            filtered = [r for r in filtered if r.category == category]
        filtered = [r for r in filtered if r.metrics.accuracy >= min_accuracy]
        if not filtered:
            return []

        filtered.sort(key=lambda r: r.metrics.carbon_per_accuracy_point)
        return filtered[:top_n]

    # --------------------------------------------------------- comparison
    def compare_models(
        self, model_a: str, model_b: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        Compare two models by their mean metrics across all benchmarks.

        Returns ``{"a": {...}, "b": {...}, "delta": {...}}`` where ``delta``
        is ``a - b`` for each numeric metric.
        """
        with self._lock:
            all_results = list(self.results)

        def _agg(name: str) -> Dict[str, float]:
            items = [r for r in all_results if r.model_name == name]
            if not items:
                raise BenchmarkIntelligenceError(f"No results for model: {name}")
            return {
                "accuracy": statistics.mean(r.metrics.accuracy for r in items),
                "energy_kwh": statistics.mean(
                    r.metrics.energy_kwh for r in items
                ),
                "carbon_kgco2e": statistics.mean(
                    r.metrics.carbon_kgco2e for r in items
                ),
                "cost_usd": statistics.mean(r.metrics.cost_usd for r in items),
                "latency_ms": statistics.mean(
                    r.metrics.latency_ms for r in items
                ),
                "efficiency_score": statistics.mean(
                    r.efficiency_score for r in items
                ),
                "power_w": statistics.mean(r.metrics.power_w for r in items),
            }

        a = _agg(model_a)
        b = _agg(model_b)
        delta = {k: a[k] - b[k] for k in a}
        return {"a": a, "b": b, "delta": delta}

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
            "avg_accuracy": float(statistics.mean(accuracies)),
            "std_accuracy": (
                float(statistics.pstdev(accuracies))
                if len(accuracies) > 1
                else 0.0
            ),
            "avg_energy_kwh": float(statistics.mean(energies)),
            "avg_carbon_kgco2e": float(statistics.mean(carbons)),
            "avg_cost_usd": float(statistics.mean(costs)),
            "total_energy_kwh": float(sum(energies)),
            "total_carbon_kgco2e": float(sum(carbons)),
            "total_cost_usd": float(sum(costs)),
            "best_efficiency_score": float(max(effs)),
            "median_efficiency_score": float(statistics.median(effs)),
            "p95_efficiency_score": float(_percentile(effs, 95)),
            "categories": sorted({r.category.value for r in results}),
            "models": sorted({r.model_name for r in results}),
        }

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`get_statistics`."""
        return self.get_statistics()

    # --------------------------------------------------------- lifecycle
    def reset(self, *, clear_results: bool = False) -> None:
        """Reset scoped state; optionally clear results (and persist)."""
        with self._lock:
            self._ctx_start = None
            self._ctx_prev_autosave = None
            if clear_results:
                self.results.clear()
        if clear_results and self._autosave and not self._memory_only:
            self._save_results()
        logger.debug(
            "BenchmarkIntelligence reset (clear_results=%s)", clear_results
        )

    def save(self) -> None:
        """Persist current results to disk (explicit save)."""
        self._save_results()

    def refresh(self) -> int:
        """Re-read results from disk. Returns the new result count."""
        if not self._autosave and not self._memory_only:
            with self._lock:
                pending = len(self.results)
            if pending:
                logger.warning(
                    "refresh() overwrites %d in-memory results (autosave=False).",
                    pending,
                )
        self._load_results()
        with self._lock:
            return len(self.results)

    # ----------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            payload = {
                "config": asdict(self._config),
                "strict": self._strict,
                "autosave": self._autosave,
                "memory_only": self._memory_only,
                "results_path": str(self.results_path),
                "results": [r.to_dict() for r in self.results],
            }
        return _json_safe(payload)

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
            weight_accuracy=cfg_data.get("weight_accuracy", 0.40),
            weight_energy=cfg_data.get("weight_energy", 0.30),
            weight_carbon=cfg_data.get("weight_carbon", 0.20),
            weight_cost=cfg_data.get("weight_cost", 0.10),
            energy_ceiling_kwh=cfg_data.get("energy_ceiling_kwh", 10.0),
            carbon_ceiling_kgco2e=cfg_data.get("carbon_ceiling_kgco2e", 5.0),
            cost_ceiling_usd=cfg_data.get("cost_ceiling_usd", 2.0),
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

        resolved_path: Union[str, Path] = (
            results_path or data.get("results_path") or _MEMORY_SENTINEL
        )
        intel = cls(
            results_path=resolved_path,
            config=cfg,
            strict=bool(data.get("strict", True)),
            autosave=autosave,
            auto_load=auto_load,
        )

        raw_results = data.get("results", [])
        if not isinstance(raw_results, list):
            raise BenchmarkIntelligenceError(
                f"results must be a list, got {type(raw_results).__name__}."
            )
        restored: List[BenchmarkResult] = [
            intel._result_from_raw(raw) for raw in raw_results
        ]
        with intel._lock:
            if (
                intel._config.max_results is not None
                and len(restored) > intel._config.max_results
            ):
                restored = restored[-intel._config.max_results :]
            intel.results = restored
        return intel

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("allow_nan", False)
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(
        cls, payload: str, *, results_path: Optional[Union[str, Path]] = None
    ) -> "BenchmarkIntelligence":
        try:
            return cls.from_dict(json.loads(payload), results_path=results_path)
        except json.JSONDecodeError as exc:
            raise BenchmarkIntelligenceError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    # ------------------------------------------------------- context mgr
    def __enter__(self) -> "BenchmarkIntelligence":
        """Timing-only scope (backward-compatible)."""
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
                "Benchmark session exited with %s after %.4fs.",
                exc_type.__name__,
                elapsed,
            )
            return
        logger.info(
            "Benchmark session closed in %.4fs (%d result(s)).",
            elapsed,
            len(self.results),
        )

    @contextlib.contextmanager
    def batch(self):
        """
        Transaction-style batching: suppresses autosave for the duration of
        the ``with`` block and writes once on clean exit.

        On exception, the in-memory results are kept but not persisted (the
        caller can still call :meth:`save` or :meth:`reset`).
        """
        self._ctx_prev_autosave = self._autosave
        self._autosave = False
        try:
            yield self
        finally:
            prev = self._ctx_prev_autosave
            self._ctx_prev_autosave = None
            self._autosave = bool(prev)
            if self._autosave and not self._memory_only:
                self._save_results()

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "BenchmarkIntelligence("
            f"results={self.results_count}, "
            f"path={str(self.results_path)!r}, "
            f"strict={self._strict}, "
            f"autosave={self._autosave}, "
            f"memory_only={self._memory_only})"
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

    with intelligence.batch():
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
    for entry in intelligence.get_leaderboard(
        sort_by="efficiency_score", limit=10
    ):
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
            f"{result.metrics.carbon_per_accuracy_point:.3f} kgCO2e per "
            f"accuracy point"
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

    # ---- Comparison --------------------------------------------------
    diff = intelligence.compare_models("bert-base", "distilbert")
    print(f"\nbert-base vs distilbert (delta = a - b):")
    for key, value in diff["delta"].items():
        print(f"  {key:<20}: {value:+.6f}")

    # ---- Trends (previously raised NameError) ------------------------
    trends = intelligence.get_efficiency_trends(window_days=1)
    print(f"\nTrend series        : {sorted(trends.keys())}")

    # ---- Field-relative recompute ------------------------------------
    n = intelligence.recompute_efficiency_scores(relative=True)
    print(f"Recomputed {n} efficiency scores (relative normalisation).")

    # ---- Serialization round-trip ------------------------------------
    payload = intelligence.to_json()
    assert "Infinity" not in payload and "NaN" not in payload, (
        "JSON must not contain inf/nan literals"
    )
    restored = BenchmarkIntelligence.from_json(payload, results_path=":memory:")
    assert restored.to_dict() == intelligence.to_dict()
    print("Serialization round-trip OK (no inf/nan leakage).")

    # ---- Memory-only mode --------------------------------------------
    memory_intel = BenchmarkIntelligence(results_path=":memory:")
    memory_intel.record_benchmark(
        model_name="tiny",
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
    assert memory_intel.results_count == 1
    print("Memory-only mode OK (no disk writes).")

    # ---- Batch + context manager -------------------------------------
    with BenchmarkIntelligence(
        results_path=Path(tmpdir) / "ctx.json", autosave=True
    ) as scoped:
        with scoped.batch():
            for i in range(3):
                scoped.record_benchmark(
                    model_name=f"mini-{i}",
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
    print("Scoped batch session OK.")

    # ---- Validation failures -----------------------------------------
    for bad in (
        dict(
            model_name="",
            model_params=1,
            dataset="d",
            category=BenchmarkCategory.TEXT_CLASSIFICATION,
            accuracy=0.9,
            energy_kwh=1.0,
            carbon_kgco2e=0.1,
            cost_usd=0.1,
            latency_ms=10,
            num_samples=100,
        ),
        dict(
            model_name="m",
            model_params=1,
            dataset="d",
            category=BenchmarkCategory.TEXT_CLASSIFICATION,
            accuracy=1.5,
            energy_kwh=1.0,
            carbon_kgco2e=0.1,
            cost_usd=0.1,
            latency_ms=10,
            num_samples=100,
        ),
        dict(
            model_name="m",
            model_params=1,
            dataset="d",
            category=BenchmarkCategory.TEXT_CLASSIFICATION,
            accuracy=0.9,
            energy_kwh=float("nan"),
            carbon_kgco2e=0.1,
            cost_usd=0.1,
            latency_ms=10,
            num_samples=100,
        ),
    ):
        try:
            intelligence.record_benchmark(**bad)
        except BenchmarkIntelligenceError as exc:
            print("Rejected as expected:", exc)
        else:  # pragma: no cover
            raise AssertionError(f"Expected rejection for {bad!r}")

    # ---- Non-strict coercion -----------------------------------------
    lenient = BenchmarkIntelligence(
        results_path=":memory:", strict=False, autosave=False
    )
    coerced = lenient.record_benchmark(
        model_name="m",
        model_params=1,
        dataset="d",
        category=BenchmarkCategory.TEXT_CLASSIFICATION,
        accuracy=1.5,  # clamped to 1.0
        energy_kwh=1.0,
        carbon_kgco2e=0.1,
        cost_usd=0.1,
        latency_ms=10,
        num_samples=100,
    )
    assert coerced.metrics.accuracy == 1.0
    print("Non-strict mode clamps accuracy=1.5 → 1.0.")

    print("\nSmoke test passed.")
