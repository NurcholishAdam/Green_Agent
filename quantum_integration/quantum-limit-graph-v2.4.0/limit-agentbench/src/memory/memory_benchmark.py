# src/memory/memory_benchmark.py

"""
Memory Benchmark
================

Implements the MVP's "Log a baseline without memory and compare task
quality, latency, tokens, energy, CO2e, and decision consistency."

Enhancements
------------
- ``MemoryBenchmarkConfig`` — frozen, validated: seeds, bootstrap samples,
  metric names.
- ``BenchmarkReport`` — frozen dataclass with per-metric deltas and a
  verdict.
- Deterministic seed.
- Custom ``MemoryBenchmarkError(ValueError)``.
- ``__main__`` smoke test with synthetic tasks and mocked runners.
"""

from __future__ import annotations

import json
import logging
import math
import random
import statistics
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

__version__ = "5.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MemoryBenchmarkError(ValueError):
    """Raised for invalid benchmark inputs or configuration."""


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MemoryBenchmarkConfig:
    """Tunable parameters for the benchmark harness."""

    seed: int = 42
    bootstrap_samples: int = 500
    confidence_level: float = 0.95
    min_task_count: int = 5
    max_task_count: int = 100_000
    metric_names: Tuple[str, ...] = (
        "quality", "latency_ms", "tokens", "energy_wh", "carbon_gco2e",
        "decision_consistency",
    )
    higher_is_better: Mapping[str, bool] = field(
        default_factory=lambda: {
            "quality": True,
            "latency_ms": False,
            "tokens": False,
            "energy_wh": False,
            "carbon_gco2e": False,
            "decision_consistency": True,
        }
    )

    def __post_init__(self) -> None:
        if not isinstance(self.seed, int) or self.seed < 0:
            raise MemoryBenchmarkError("seed must be a non-negative int.")
        if self.bootstrap_samples <= 0:
            raise MemoryBenchmarkError("bootstrap_samples must be > 0.")
        if not 0.0 < self.confidence_level < 1.0:
            raise MemoryBenchmarkError(
                "confidence_level must be in (0, 1)."
            )
        if self.min_task_count <= 0:
            raise MemoryBenchmarkError("min_task_count must be > 0.")
        if self.max_task_count <= self.min_task_count:
            raise MemoryBenchmarkError(
                "max_task_count must be > min_task_count."
            )
        if not self.metric_names:
            raise MemoryBenchmarkError("metric_names must be non-empty.")
        for metric in self.metric_names:
            if metric not in self.higher_is_better:
                raise MemoryBenchmarkError(
                    f"metric {metric!r} missing from higher_is_better."
                )

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["metric_names"] = list(self.metric_names)
        d["higher_is_better"] = dict(self.higher_is_better)
        return d


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class BenchmarkReport:
    """Frozen benchmark report."""

    tasks: int
    metrics: Mapping[str, Dict[str, float]]        # metric -> {baseline, memory, delta, ci_low, ci_high}
    verdict: str                                    # "memory_helps" | "memory_neutral" | "memory_hurts"
    seed: int
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "tasks": self.tasks,
            "metrics": {k: dict(v) for k, v in self.metrics.items()},
            "verdict": self.verdict,
            "seed": self.seed,
            "timestamp": self.timestamp.isoformat(),
        }

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kw)


# --------------------------------------------------------------------------- #
# Harness
# --------------------------------------------------------------------------- #
class MemoryBenchmark:
    """Paired benchmark: without-memory vs. with-memory."""

    def __init__(
        self,
        *,
        config: Optional[MemoryBenchmarkConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or MemoryBenchmarkConfig()
        self._strict = bool(strict)

    @property
    def config(self) -> MemoryBenchmarkConfig:
        return self._config

    def run_pair(
        self,
        *,
        tasks: Sequence[Any],
        without_memory: Callable[[Any], Mapping[str, float]],
        with_memory: Callable[[Any], Mapping[str, float]],
    ) -> BenchmarkReport:
        """Run the paired comparison on ``tasks``.

        Each runner must return a mapping of the metric names in
        ``config.metric_names`` to finite floats.
        """
        if not tasks:
            raise MemoryBenchmarkError("tasks must be a non-empty sequence.")
        if not callable(without_memory):
            raise MemoryBenchmarkError("without_memory must be callable.")
        if not callable(with_memory):
            raise MemoryBenchmarkError("with_memory must be callable.")
        if len(tasks) < self._config.min_task_count:
            raise MemoryBenchmarkError(
                f"at least {self._config.min_task_count} tasks required."
            )
        if len(tasks) > self._config.max_task_count:
            raise MemoryBenchmarkError(
                f"tasks exceeds max_task_count={self._config.max_task_count}."
            )

        rng = random.Random(self._config.seed)
        baseline_runs: Dict[str, List[float]] = {
            m: [] for m in self._config.metric_names
        }
        memory_runs: Dict[str, List[float]] = {
            m: [] for m in self._config.metric_names
        }

        for i, task in enumerate(tasks):
            b = self._run_runner(without_memory, task, name=f"baseline[{i}]")
            m = self._run_runner(with_memory, task, name=f"memory[{i}]")
            for metric in self._config.metric_names:
                if metric not in b or metric not in m:
                    raise MemoryBenchmarkError(
                        f"runner did not return metric {metric!r}."
                    )
                baseline_runs[metric].append(float(b[metric]))
                memory_runs[metric].append(float(m[metric]))

        metrics: Dict[str, Dict[str, float]] = {}
        votes: List[int] = []
        for metric in self._config.metric_names:
            baseline_vals = baseline_runs[metric]
            memory_vals = memory_runs[metric]
            delta = statistics.fmean(memory_vals) - statistics.fmean(baseline_vals)
            ci_low, ci_high = self._bootstrap_ci(
                memory_vals, baseline_vals, rng=rng,
            )
            metrics[metric] = {
                "baseline": statistics.fmean(baseline_vals),
                "memory": statistics.fmean(memory_vals),
                "delta": delta,
                "ci_low": ci_low,
                "ci_high": ci_high,
            }
            higher_is_better = self._config.higher_is_better[metric]
            # Vote only if the CI excludes zero.
            if ci_low > 0.0 and higher_is_better:
                votes.append(+1)
            elif ci_low > 0.0 and not higher_is_better:
                votes.append(-1)
            elif ci_high < 0.0 and higher_is_better:
                votes.append(-1)
            elif ci_high < 0.0 and not higher_is_better:
                votes.append(+1)

        score = sum(votes)
        if score > 0:
            verdict = "memory_helps"
        elif score < 0:
            verdict = "memory_hurts"
        else:
            verdict = "memory_neutral"

        return BenchmarkReport(
            tasks=len(tasks),
            metrics=metrics,
            verdict=verdict,
            seed=self._config.seed,
        )

    # ---------------------------------------------------------- helpers
    def _run_runner(
        self,
        runner: Callable[[Any], Mapping[str, float]],
        task: Any,
        *,
        name: str,
    ) -> Mapping[str, float]:
        try:
            result = runner(task)
        except Exception as exc:
            if self._strict:
                raise MemoryBenchmarkError(
                    f"{name} failed: {exc}"
                ) from exc
            logger.warning("%s failed: %s", name, exc)
            return {}
        if not isinstance(result, Mapping):
            raise MemoryBenchmarkError(
                f"{name} returned {type(result).__name__}, expected Mapping."
            )
        for k, v in result.items():
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise MemoryBenchmarkError(
                    f"{name}[{k!r}] must be numeric."
                )
            if math.isnan(float(v)) or math.isinf(float(v)):
                raise MemoryBenchmarkError(
                    f"{name}[{k!r}] must be finite."
                )
        return result

    def _bootstrap_ci(
        self,
        memory_vals: Sequence[float],
        baseline_vals: Sequence[float],
        *,
        rng: random.Random,
    ) -> Tuple[float, float]:
        n = len(memory_vals)
        deltas: List[float] = []
        for _ in range(self._config.bootstrap_samples):
            idx = [rng.randrange(n) for _ in range(n)]
            m = statistics.fmean(memory_vals[i] for i in idx)
            b = statistics.fmean(baseline_vals[i] for i in idx)
            deltas.append(m - b)
        deltas.sort()
        alpha = 1.0 - self._config.confidence_level
        lo = deltas[int(math.floor(alpha / 2 * len(deltas)))]
        hi = deltas[int(math.ceil((1 - alpha / 2) * len(deltas))) - 1]
        return lo, hi

    def __repr__(self) -> str:
        return (
            "MemoryBenchmark("
            f"seed={self._config.seed}, "
            f"samples={self._config.bootstrap_samples}, "
            f"strict={self._strict})"
        )


__all__ = [
    "BenchmarkReport",
    "MemoryBenchmark",
    "MemoryBenchmarkConfig",
    "MemoryBenchmarkError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.memory_benchmark
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    bench = MemoryBenchmark()
    print("repr       :", bench)

    tasks = list(range(20))

    def without_memory(task: Any) -> Dict[str, float]:
        rng = random.Random(task + 100)
        return {
            "quality": 0.80 + rng.random() * 0.05,
            "latency_ms": 400 + rng.random() * 60,
            "tokens": 1_800 + rng.randint(-100, 100),
            "energy_wh": 9.0 + rng.random() * 0.5,
            "carbon_gco2e": 4.2 + rng.random() * 0.3,
            "decision_consistency": 0.55 + rng.random() * 0.1,
        }

    def with_memory(task: Any) -> Dict[str, float]:
        rng = random.Random(task + 200)
        return {
            "quality": 0.84 + rng.random() * 0.05,     # improves
            "latency_ms": 380 + rng.random() * 50,     # improves
            "tokens": 1_400 + rng.randint(-100, 100),  # improves
            "energy_wh": 8.6 + rng.random() * 0.4,     # improves
            "carbon_gco2e": 4.0 + rng.random() * 0.25, # improves
            "decision_consistency": 0.72 + rng.random() * 0.08,  # improves
        }

    report = bench.run_pair(
        tasks=tasks,
        without_memory=without_memory,
        with_memory=with_memory,
    )
    print("verdict    :", report.verdict)
    for metric, stats in report.metrics.items():
        print(f"  {metric:<22} delta={stats['delta']:+.4g} "
              f"ci=[{stats['ci_low']:+.4g}, {stats['ci_high']:+.4g}]")

    # Validation.
    for bad in (
        lambda: bench.run_pair(tasks=[], without_memory=lambda t: {},
                               with_memory=lambda t: {}),
        lambda: bench.run_pair(tasks=[1, 2], without_memory=lambda t: {},
                               with_memory=lambda t: {}),
    ):
        try:
            bad()
        except MemoryBenchmarkError as exc:
            print("Rejected   :", exc)

    print("\nSmoke test passed.")
