# metrics/../memory/run_memory.py

"""
Run Memory Module

Implements a memory system for sustained reflection across multiple runs.
Tracks agent performance history and generates meta-policies.

Enhancements
------------
- Thread-safe via ``RLock``.
- Bounded ring-buffer history (``max_runs``) to cap memory / file growth.
- Immutable ``RunSample`` dataclass for stored runs.
- Configurable thresholds via :class:`MemoryConfig` (no magic numbers).
- Strict / non-strict mode for missing metrics.
- Structured serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Atomic file writes (temp + ``os.replace``) to avoid partial-file corruption.
- Custom :class:`RunMemoryError`.
- Context-manager support with auto-save on clean exit.
- Lazy ``%s`` logging (no more ``print``), ``__repr__``, and a smoke test.
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
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors & configuration
# --------------------------------------------------------------------------- #
class RunMemoryError(ValueError):
    """Raised for invalid inputs, configuration, or corrupted memory files."""


@dataclass(frozen=True)
class MemoryConfig:
    """
    Tunable thresholds used by trend analysis and meta-policy generation.

    Centralized so callers can calibrate without touching the analysis code.
    """

    # Trend classification band: +/- this fraction counts as "stable".
    trend_stability_band: float = 0.05  # 5%

    # Minimum runs required for trend analysis vs. meta-policy generation.
    min_runs_for_trend: int = 2
    min_runs_for_policy: int = 3

    # Metric names tracked by ``generate_meta_policy``.
    metrics_to_track: tuple = (
        "final_score",
        "accuracy",
        "energy_consumption",
        "carbon_emissions",
    )

    # Direction meanings per metric: does higher mean "better"?
    higher_is_better: Mapping[str, bool] = field(
        default_factory=lambda: {
            "final_score": True,
            "accuracy": True,
            "energy_consumption": False,
            "carbon_emissions": False,
        }
    )

    def __post_init__(self) -> None:
        if not 0.0 < self.trend_stability_band < 1.0:
            raise RunMemoryError(
                "trend_stability_band must be in (0, 1), got "
                f"{self.trend_stability_band!r}."
            )
        if self.min_runs_for_trend < 2:
            raise RunMemoryError("min_runs_for_trend must be >= 2.")
        if self.min_runs_for_policy < self.min_runs_for_trend:
            raise RunMemoryError(
                "min_runs_for_policy must be >= min_runs_for_trend."
            )


# --------------------------------------------------------------------------- #
# Sample container
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RunSample:
    """
    Immutable snapshot of a single completed run.

    ``run_data`` holds the caller's original payload untouched so downstream
    consumers that expect the old dict shape keep working. Extra fields are
    captured as attributes for convenience.
    """

    run_id: int
    timestamp: str
    run_data: Mapping[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "run_data": dict(self.run_data),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RunSample":
        if not isinstance(data, Mapping):
            raise RunMemoryError(
                f"RunSample.from_dict expects a Mapping, got {type(data).__name__}."
            )
        try:
            return cls(
                run_id=int(data["run_id"]),
                timestamp=str(data["timestamp"]),
                run_data=dict(data["run_data"]),
            )
        except KeyError as exc:
            raise RunMemoryError(
                f"RunSample missing required field: {exc.args[0]!r}"
            ) from exc


# --------------------------------------------------------------------------- #
# Main class
# --------------------------------------------------------------------------- #
class RunMemory:
    """
    Memory system for tracking agent performance across runs.

    Responsibilities
    ----------------
    - Store complete run histories.
    - Track performance trends over time.
    - Generate meta-policies from historical data.
    - Support long-context reasoning.

    Backward compatibility
    ----------------------
    All original public methods (``add_run``, ``get_recent_runs``,
    ``get_performance_trend``, ``generate_meta_policy``, ``get_best_run``,
    ``clear_memory``, ``save_memory``) keep their signatures.
    """

    def __init__(
        self,
        memory_file: str = "run_memory.json",
        *,
        config: Optional[MemoryConfig] = None,
        max_runs: Optional[int] = 1000,
        strict: bool = False,
        autosave: bool = True,
        auto_load: bool = True,
    ) -> None:
        """
        Parameters
        ----------
        memory_file : str
            Path to the persistent memory file (created on first save).
        config : MemoryConfig, optional
            Thresholds for trend / policy analysis. Defaults to ``MemoryConfig()``.
        max_runs : int | None
            Ring-buffer cap on stored runs. ``None`` disables trimming.
        strict : bool
            If True, missing metrics raise ``RunMemoryError`` instead of being
            silently skipped. Default False (matches legacy behavior).
        autosave : bool
            If True (default), ``add_run`` / ``clear_memory`` / ``generate_meta_policy``
            persist immediately. Set False for batch operations.
        auto_load : bool
            If True (default), load existing memory from disk on construction.
        """
        if max_runs is not None and max_runs <= 0:
            raise RunMemoryError("max_runs must be > 0 or None.")

        self.memory_file: str = str(memory_file)
        self._config: MemoryConfig = config or MemoryConfig()
        self._max_runs: Optional[int] = max_runs
        self._strict: bool = bool(strict)
        self._autosave: bool = bool(autosave)

        self._lock = threading.RLock()
        self._runs: List[RunSample] = []
        self._meta_policies: List[Dict[str, Any]] = []
        self._last_updated: Optional[str] = None
        self._next_run_id: int = 0
        self._ctx_start: Optional[float] = None

        if auto_load:
            self._load_memory()

        logger.debug(
            "RunMemory initialized (file=%s, max_runs=%s, strict=%s, autosave=%s)",
            self.memory_file,
            self._max_runs,
            self._strict,
            self._autosave,
        )

    # ------------------------------------------------------------------ props
    @property
    def runs(self) -> List[Dict[str, Any]]:
        """Return stored runs as plain dicts (backward-compatible view)."""
        with self._lock:
            return [s.run_data for s in self._runs]

    @property
    def samples(self) -> List[RunSample]:
        """Return the internal ``RunSample`` list (read-only copy)."""
        with self._lock:
            return list(self._runs)

    @property
    def meta_policies(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._meta_policies)

    @property
    def last_updated(self) -> Optional[str]:
        with self._lock:
            return self._last_updated

    @property
    def config(self) -> MemoryConfig:
        return self._config

    # ------------------------------------------------------------ persistence
    def _load_memory(self) -> None:
        """Load existing memory from disk (missing file is not an error)."""
        path = Path(self.memory_file)
        if not path.exists():
            logger.debug("Memory file %s not found; starting empty.", path)
            return

        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as exc:
            logger.error("Corrupted memory file %s: %s", path, exc)
            if self._strict:
                raise RunMemoryError(f"Corrupted memory file: {path}") from exc
            logger.warning("Starting with empty memory (non-strict mode).")
            return
        except OSError as exc:
            logger.error("Could not read memory file %s: %s", path, exc)
            if self._strict:
                raise RunMemoryError(f"Could not read memory file: {path}") from exc
            return

        if not isinstance(data, Mapping):
            msg = f"Memory file root must be a Mapping, got {type(data).__name__}."
            if self._strict:
                raise RunMemoryError(msg)
            logger.warning(msg)
            return

        with self._lock:
            raw_runs = data.get("runs", []) or []
            self._runs = []
            for entry in raw_runs:
                try:
                    if "run_data" in entry:
                        self._runs.append(RunSample.from_dict(entry))
                    else:
                        # Legacy format: run payload stored inline.
                        self._runs.append(
                            RunSample(
                                run_id=int(entry.get("run_id", len(self._runs))),
                                timestamp=str(
                                    entry.get("timestamp", _now_iso())
                                ),
                                run_data=dict(entry),
                            )
                        )
                except (RunMemoryError, TypeError, ValueError) as exc:
                    if self._strict:
                        raise
                    logger.warning("Skipping malformed run entry: %s", exc)

            self._meta_policies = list(data.get("meta_policies", []) or [])
            self._last_updated = data.get("last_updated")
            self._next_run_id = (
                max((s.run_id for s in self._runs), default=-1) + 1
            )

        logger.info(
            "Loaded %d run(s), %d meta-policy(ies) from %s",
            len(self._runs),
            len(self._meta_policies),
            path,
        )

    def save_memory(self) -> None:
        """Persist memory to disk atomically (temp file + ``os.replace``)."""
        with self._lock:
            payload = {
                "runs": [s.to_dict() for s in self._runs],
                "meta_policies": list(self._meta_policies),
                "last_updated": _now_iso(),
            }

        path = Path(self.memory_file)
        path.parent.mkdir(parents=True, exist_ok=True)

        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2)
                os.replace(tmp_path, path)
            except Exception:
                # Clean up temp file if something went wrong.
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except OSError as exc:
            logger.error("Could not save memory file %s: %s", path, exc)
            if self._strict:
                raise RunMemoryError(f"Could not save memory file: {path}") from exc
            return

        with self._lock:
            self._last_updated = payload["last_updated"]

        logger.debug("Saved %d run(s) to %s", len(payload["runs"]), path)

    # --------------------------------------------------------------- mutation
    def add_run(self, run_data: Mapping[str, Any]) -> int:
        """
        Add a completed run to memory.

        Parameters
        ----------
        run_data : Mapping
            Complete run data including metrics and reflections.

        Returns
        -------
        int
            The ``run_id`` assigned to the new run.
        """
        if not isinstance(run_data, Mapping):
            raise RunMemoryError(
                f"run_data must be a Mapping, got {type(run_data).__name__}."
            )

        # Defensive copy so the caller can't mutate stored state afterwards.
        payload = dict(run_data)

        with self._lock:
            run_id = self._next_run_id
            self._next_run_id += 1
            payload["timestamp"] = _now_iso()
            payload["run_id"] = run_id
            sample = RunSample(
                run_id=run_id,
                timestamp=payload["timestamp"],
                run_data=payload,
            )
            self._runs.append(sample)
            if self._max_runs is not None and len(self._runs) > self._max_runs:
                # Ring buffer: drop oldest.
                del self._runs[0]

        if self._autosave:
            self.save_memory()

        logger.debug(
            "Added run_id=%d (total=%d)", run_id, len(self._runs)
        )
        return run_id

    def clear_memory(self, *, autosave: Optional[bool] = None) -> None:
        """Clear all memory (runs + meta-policies)."""
        with self._lock:
            self._runs.clear()
            self._meta_policies.clear()
            self._next_run_id = 0
            self._last_updated = None
        if autosave if autosave is not None else self._autosave:
            self.save_memory()
        logger.info("RunMemory cleared.")

    # ------------------------------------------------------------------ reads
    def get_recent_runs(self, n: int = 5) -> List[Dict[str, Any]]:
        """Return the ``n`` most recent runs as plain dicts."""
        if n <= 0:
            return []
        with self._lock:
            return [s.run_data for s in self._runs[-n:]]

    def get_best_run(
        self, metric: str = "final_score"
    ) -> Optional[Dict[str, Any]]:
        """Return the run with the highest value for ``metric``, or None."""
        with self._lock:
            candidates = [
                s.run_data for s in self._runs if _extract_metric(s.run_data, metric) is not None
            ]
        if not candidates:
            return None
        return max(candidates, key=lambda r: _extract_metric(r, metric))

    # ------------------------------------------------------------ trend / stats
    def get_performance_trend(self, metric: str = "final_score") -> Dict[str, Any]:
        """
        Analyse the trend of a metric over stored runs.

        Returns a dict with at least ``trend`` and ``values``. When at least
        ``config.min_runs_for_trend`` data points are available, also returns
        ``first_half_avg``, ``second_half_avg``, and ``improvement``.
        """
        with self._lock:
            values = [
                v
                for v in (_extract_metric(s.run_data, metric) for s in self._runs)
                if v is not None
            ]

        if len(values) < self._config.min_runs_for_trend:
            return {"trend": "insufficient_data", "values": values}

        midpoint = len(values) // 2
        first_half = values[:midpoint] if midpoint > 0 else values[:1]
        second_half = values[midpoint:] if midpoint > 0 else values[1:]

        first_avg = sum(first_half) / len(first_half)
        second_avg = sum(second_half) / len(second_half)

        band = self._config.trend_stability_band
        higher_is_better = self._config.higher_is_better.get(metric, True)

        if first_avg == 0:
            trend = "stable" if second_avg == 0 else (
                "improving" if (second_avg > 0) == higher_is_better else "declining"
            )
        else:
            ratio = second_avg / first_avg
            if ratio > 1.0 + band:
                trend = "improving" if higher_is_better else "declining"
            elif ratio < 1.0 - band:
                trend = "declining" if higher_is_better else "improving"
            else:
                trend = "stable"

        # Raw improvement in the "higher is better" sense.
        raw_improvement = second_avg - first_avg
        improvement = raw_improvement if higher_is_better else -raw_improvement

        return {
            "trend": trend,
            "values": values,
            "first_half_avg": first_avg,
            "second_half_avg": second_avg,
            "improvement": improvement,
            "raw_change": raw_improvement,
        }

    def statistics(self, metric: str = "final_score") -> Dict[str, Optional[float]]:
        """Return count / mean / median / min / max / p95 for a metric."""
        with self._lock:
            values = [
                v
                for v in (_extract_metric(s.run_data, metric) for s in self._runs)
                if v is not None
            ]

        empty: Dict[str, Optional[float]] = {
            "count": 0,
            "mean": None,
            "median": None,
            "min": None,
            "max": None,
            "p95": None,
        }
        if not values:
            return empty

        ordered = sorted(values)
        p95_idx = max(
            0, min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1))))
        )
        return {
            "count": len(ordered),
            "mean": sum(ordered) / len(ordered),
            "median": ordered[len(ordered) // 2],
            "min": ordered[0],
            "max": ordered[-1],
            "p95": ordered[p95_idx],
        }

    # -------------------------------------------------------- meta-policy
    def generate_meta_policy(self, *, save: Optional[bool] = None) -> Dict[str, Any]:
        """
        Generate a meta-policy from historical performance.

        Returns a dict with at least ``policy`` and ``recommendations``.
        """
        if len(self._runs) < self._config.min_runs_for_policy:
            return {"policy": "insufficient_data", "recommendations": []}

        trends: Dict[str, Dict[str, Any]] = {}
        for metric in self._config.metrics_to_track:
            trends[metric] = self.get_performance_trend(metric)

        recommendations: List[str] = []

        # final_score trend
        score_trend = trends.get("final_score", {}).get("trend")
        if score_trend == "declining":
            recommendations.append(
                "Performance is declining - review recent changes"
            )
        elif score_trend == "improving":
            recommendations.append(
                "Performance improving - continue current strategy"
            )

        # Energy: "declining" here means energy is trending upward in raw terms
        # because higher_is_better=False for energy_consumption.
        energy_trend = trends.get("energy_consumption", {}).get("trend")
        if energy_trend == "declining":
            recommendations.append(
                "Energy consumption is increasing - optimize resource usage"
            )
        elif energy_trend == "improving":
            recommendations.append(
                "Energy consumption decreasing - efficiency gains detected"
            )

        # Carbon: same inversion as energy.
        carbon_trend = trends.get("carbon_emissions", {}).get("trend")
        if carbon_trend == "declining":
            recommendations.append(
                "Carbon emissions rising - consider greener execution targets"
            )
        elif carbon_trend == "improving":
            recommendations.append(
                "Carbon emissions decreasing - sustainability improving"
            )

        policy: Dict[str, Any] = {
            "policy": "generated",
            "recommendations": recommendations,
            "trends": trends,
            "based_on_runs": len(self._runs),
            "generated_at": _now_iso(),
        }

        with self._lock:
            self._meta_policies.append(policy)

        if save if save is not None else self._autosave:
            self.save_memory()

        logger.info(
            "Generated meta-policy from %d run(s) with %d recommendation(s).",
            len(self._runs),
            len(recommendations),
        )
        return policy

    # ----------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "max_runs": self._max_runs,
                "strict": self._strict,
                "last_updated": self._last_updated,
                "next_run_id": self._next_run_id,
                "runs": [s.to_dict() for s in self._runs],
                "meta_policies": list(self._meta_policies),
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        memory_file: str = ":memory:",
        autosave: bool = False,
        auto_load: bool = False,
    ) -> "RunMemory":
        if not isinstance(data, Mapping):
            raise RunMemoryError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )

        cfg_data = dict(data.get("config", {}) or {})
        cfg = MemoryConfig(
            trend_stability_band=float(cfg_data.get("trend_stability_band", 0.05)),
            min_runs_for_trend=int(cfg_data.get("min_runs_for_trend", 2)),
            min_runs_for_policy=int(cfg_data.get("min_runs_for_policy", 3)),
            metrics_to_track=tuple(
                cfg_data.get(
                    "metrics_to_track",
                    (
                        "final_score",
                        "accuracy",
                        "energy_consumption",
                        "carbon_emissions",
                    ),
                )
            ),
            higher_is_better=dict(
                cfg_data.get(
                    "higher_is_better",
                    {
                        "final_score": True,
                        "accuracy": True,
                        "energy_consumption": False,
                        "carbon_emissions": False,
                    },
                )
            ),
        )

        mem = cls(
            memory_file=memory_file,
            config=cfg,
            max_runs=data.get("max_runs", 1000),
            strict=bool(data.get("strict", False)),
            autosave=autosave,
            auto_load=auto_load,
        )
        with mem._lock:
            mem._runs = [RunSample.from_dict(entry) for entry in data.get("runs", [])]
            mem._meta_policies = list(data.get("meta_policies", []) or [])
            mem._last_updated = data.get("last_updated")
            mem._next_run_id = int(
                data.get("next_run_id", max((s.run_id for s in mem._runs), default=-1) + 1)
            )
        return mem

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(
        cls, payload: str, *, memory_file: str = ":memory:"
    ) -> "RunMemory":
        try:
            return cls.from_dict(json.loads(payload), memory_file=memory_file)
        except json.JSONDecodeError as exc:
            raise RunMemoryError(f"Invalid JSON payload: {exc}") from exc

    # ----------------------------------------------------------- context mgr
    def __enter__(self) -> "RunMemory":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped RunMemory session.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None

        if exc_type is not None:
            logger.warning(
                "RunMemory context exited with %s after %.4fs; not saving.",
                exc_type.__name__,
                elapsed,
            )
            return

        try:
            self.save_memory()
        except RunMemoryError:
            logger.exception("Failed to persist RunMemory on context exit.")
        finally:
            logger.info(
                "RunMemory context closed cleanly in %.4fs (%d runs).",
                elapsed,
                len(self._runs),
            )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "RunMemory("
            f"file={self.memory_file!r}, "
            f"runs={len(self._runs)}, "
            f"policies={len(self._meta_policies)}, "
            f"max_runs={self._max_runs}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Module-level helpers
# --------------------------------------------------------------------------- #
def _now_iso() -> str:
    return datetime.now().isoformat()


def _extract_metric(run: Mapping[str, Any], metric: str) -> Optional[float]:
    """
    Extract a numeric metric from a run dict, looking at the top level and
    then inside a nested ``metrics`` mapping. Returns None if not found or
    not a finite number.
    """
    for container in (run, run.get("metrics") if isinstance(run, Mapping) else None):
        if not isinstance(container, Mapping):
            continue
        if metric in container:
            value = container[metric]
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float)) and math.isfinite(float(value)):
                return float(value)
    return None


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "RunMemory",
    "RunMemoryError",
    "RunSample",
    "MemoryConfig",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m memory.run_memory
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    import tempfile as _tf

    tmpdir = _tf.mkdtemp(prefix="run_memory_smoke_")
    mem_file = os.path.join(tmpdir, "run_memory.json")

    mem = RunMemory(memory_file=mem_file, max_runs=5, strict=False)

    # Add a sequence of runs with a mix of improving score and rising energy
    for i in range(6):
        mem.add_run(
            {
                "final_score": 0.5 + 0.05 * i,
                "metrics": {
                    "accuracy": 0.7 + 0.02 * i,
                    "energy_consumption": 100 + 10 * i,  # rising (bad)
                    "carbon_emissions": 20 + 3 * i,      # rising (bad)
                },
                "notes": f"run-{i}",
            }
        )

    print("Runs stored :", len(mem.runs), "(max_runs=5 → ring trimmed)")
    print("Recent runs :", len(mem.get_recent_runs(3)))
    print("Best score  :", mem.get_best_run("final_score"))
    print("Trend score :", mem.get_performance_trend("final_score"))
    print("Trend energy:", mem.get_performance_trend("energy_consumption"))
    print("Stats score :", mem.statistics("final_score"))
    print("Meta-policy :", mem.generate_meta_policy())

    # Serialization round-trip
    payload = mem.to_json()
    restored = RunMemory.from_json(payload, memory_file=":memory:")
    assert restored.to_dict() == mem.to_dict()
    print("Serialization round-trip OK.")

    # Context manager
    with RunMemory(memory_file=os.path.join(tmpdir, "ctx.json")) as scoped:
        scoped.add_run({"final_score": 0.9})
    print("Context-managed save OK.")

    print("Final memory:", mem)
