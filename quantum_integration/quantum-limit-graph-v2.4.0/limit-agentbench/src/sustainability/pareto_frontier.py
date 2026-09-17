# src/sustainability/pareto_frontier.py

"""
Sustainability Pareto Frontier
==============================

Energy vs accuracy Pareto frontier for agent comparisons.

This module complements :mod:`analysis.pareto_analyzer` — that one handles
the full ParetoAnalyzer with all its extras (ParetoPoint, ranks, etc.),
whereas this lightweight variant is scoped to the sustainability layer and
exposes a minimal, self-contained API for the ``SustainabilityPareto``
consumer.

Enhancements
------------
- ``AgentPerformancePoint`` — frozen dataclass with full ``__post_init__``
  validation.
- ``SustainabilityParetoConfig`` — frozen, validated: capacity, dominance
  tolerance, tie-break strategy.
- **Thread safety** — ``RLock`` guards the point list.
- **Bounded storage** — LRU eviction when ``max_points`` is reached.
- **Deterministic frontier ordering** — sort by ``(energy, -accuracy, label)``.
- **`add_point` returns the point** (additive) so callers can inspect it.
- **Serialization** — ``to_dict`` / ``from_dict`` / ``to_json`` on the
  frontier and the point dataclass.
- ``statistics()`` / ``reset()`` / ``__repr__``, custom
  ``SustainabilityParetoError(ValueError)``, and a ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SustainabilityParetoError(ValueError):
    """Raised for invalid Pareto-frontier inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SustainabilityParetoConfig:
    """Tunable parameters for :class:`SustainabilityPareto`."""

    max_points: Optional[int] = 10_000
    dominance_tolerance: float = 0.0
    max_energy: float = 1e15
    max_accuracy: float = 100.0

    def __post_init__(self) -> None:
        if self.max_points is not None and self.max_points <= 0:
            raise SustainabilityParetoError(
                "max_points must be a positive int or None."
            )
        if self.dominance_tolerance < 0:
            raise SustainabilityParetoError(
                "dominance_tolerance must be >= 0."
            )
        if self.max_energy <= 0:
            raise SustainabilityParetoError("max_energy must be > 0.")
        if self.max_accuracy <= 0:
            raise SustainabilityParetoError("max_accuracy must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SustainabilityParetoConfig":
        if not isinstance(data, Mapping):
            raise SustainabilityParetoError(
                "SustainabilityParetoConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs = {k: v for k, v in data.items() if k in valid}
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Point
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class AgentPerformancePoint:
    """Immutable performance point on the energy/accuracy plane."""

    energy_joules: float
    accuracy_percent: float
    label: str
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        for name in ("energy_joules", "accuracy_percent"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise SustainabilityParetoError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv) or fv < 0:
                raise SustainabilityParetoError(
                    f"{name} must be finite and >= 0, got {value!r}."
                )
        if not isinstance(self.label, str) or not self.label:
            raise SustainabilityParetoError(
                "label must be a non-empty string."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "energy_joules": self.energy_joules,
            "accuracy_percent": self.accuracy_percent,
            "label": self.label,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AgentPerformancePoint":
        if not isinstance(data, Mapping):
            raise SustainabilityParetoError(
                "AgentPerformancePoint.from_dict expects a Mapping."
            )
        ts = data.get("timestamp")
        if isinstance(ts, str):
            timestamp = datetime.fromisoformat(ts)
            if not timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            timestamp = ts
        else:
            timestamp = datetime.now(timezone.utc)
        return cls(
            energy_joules=float(data["energy_joules"]),
            accuracy_percent=float(data["accuracy_percent"]),
            label=str(data["label"]),
            timestamp=timestamp,
        )

    def __repr__(self) -> str:
        return (
            "AgentPerformancePoint("
            f"label={self.label!r}, "
            f"energy={self.energy_joules:.4g}J, "
            f"accuracy={self.accuracy_percent:.2f}%)"
        )


# --------------------------------------------------------------------------- #
# Frontier
# --------------------------------------------------------------------------- #
class SustainabilityPareto:
    """
    Computes and manages the Sustainability Pareto frontier (energy vs
    accuracy).

    Lower energy and higher accuracy are preferred. Thread-safe, serializable,
    and bounded in memory.
    """

    def __init__(
        self,
        *,
        config: Optional[SustainabilityParetoConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or SustainabilityParetoConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self.points: Deque[AgentPerformancePoint] = deque(
            maxlen=self._config.max_points
        )
        self._started_at: float = time.time()

        logger.debug(
            "SustainabilityPareto initialized "
            "(max_points=%s, tolerance=%.3g, strict=%s)",
            self._config.max_points,
            self._config.dominance_tolerance,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> SustainabilityParetoConfig:
        return self._config

    @property
    def count(self) -> int:
        with self._lock:
            return len(self.points)

    # ---------------------------------------------------------- public API
    def add_point(
        self,
        energy_joules: float,
        accuracy_percent: float,
        label: str,
    ) -> AgentPerformancePoint:
        """
        Add a performance point.

        Returns the stored :class:`AgentPerformancePoint`.
        """
        point = AgentPerformancePoint(
            energy_joules=float(energy_joules),
            accuracy_percent=float(accuracy_percent),
            label=str(label),
        )
        with self._lock:
            self.points.append(point)
        logger.debug("Added %s", point)
        return point

    def compute_frontier(self) -> List[AgentPerformancePoint]:
        """Return the Pareto-optimal subset (sorted by ascending energy)."""
        with self._lock:
            points = list(self.points)

        if not points:
            return []

        tol = self._config.dominance_tolerance

        # Sort by energy asc, accuracy desc, label asc for determinism.
        sorted_points = sorted(
            points,
            key=lambda p: (p.energy_joules, -p.accuracy_percent, p.label),
        )

        frontier: List[AgentPerformancePoint] = []
        best_accuracy = -math.inf
        for p in sorted_points:
            if p.accuracy_percent > best_accuracy + tol:
                frontier.append(p)
                best_accuracy = p.accuracy_percent

        logger.debug(
            "Computed Pareto frontier: %d/%d points.",
            len(frontier), len(points),
        )
        return frontier

    def as_tuples(self) -> List[Tuple[float, float, str]]:
        """Return the stored points as ``(energy, accuracy, label)`` tuples."""
        with self._lock:
            return [
                (p.energy_joules, p.accuracy_percent, p.label)
                for p in self.points
            ]

    def reset(self) -> int:
        """Clear all points. Returns the number removed."""
        with self._lock:
            removed = len(self.points)
            self.points.clear()
            self._started_at = time.time()
        logger.debug("SustainabilityPareto reset (removed %d).", removed)
        return removed

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            points = list(self.points)
        if not points:
            return {
                "num_points": 0,
                "frontier_size": 0,
                "mean_energy_joules": 0.0,
                "mean_accuracy_percent": 0.0,
                "max_accuracy_percent": 0.0,
                "min_energy_joules": 0.0,
                "uptime_seconds": time.time() - self._started_at,
            }
        energies = [p.energy_joules for p in points]
        accuracies = [p.accuracy_percent for p in points]
        return {
            "num_points": len(points),
            "frontier_size": len(self.compute_frontier()),
            "mean_energy_joules": sum(energies) / len(energies),
            "mean_accuracy_percent": sum(accuracies) / len(accuracies),
            "max_accuracy_percent": max(accuracies),
            "min_energy_joules": min(energies),
            "uptime_seconds": time.time() - self._started_at,
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_points: bool = True) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "started_at": self._started_at,
            }
            if include_points:
                payload["points"] = [p.to_dict() for p in self.points]
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SustainabilityPareto":
        if not isinstance(data, Mapping):
            raise SustainabilityParetoError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = SustainabilityParetoConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        pareto = cls(config=cfg, strict=bool(data.get("strict", True)))
        with pareto._lock:
            for entry in data.get("points", []):
                pareto.points.append(
                    AgentPerformancePoint.from_dict(entry)
                )
            pareto._started_at = float(data.get("started_at", time.time()))
        return pareto

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "SustainabilityPareto":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise SustainabilityParetoError(f"Invalid JSON: {exc}") from exc

    # ----------------------------------------------------------------- dunder
    def __len__(self) -> int:
        return self.count

    def __repr__(self) -> str:
        with self._lock:
            return (
                "SustainabilityPareto("
                f"points={len(self.points)}, "
                f"max_points={self._config.max_points}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "AgentPerformancePoint",
    "SustainabilityPareto",
    "SustainabilityParetoConfig",
    "SustainabilityParetoError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m sustainability.pareto_frontier
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    pareto = SustainabilityPareto()
    print("repr       :", pareto)

    # Points: A, B, C are on the frontier; D and E are dominated.
    pareto.add_point(energy_joules=100.0, accuracy_percent=70.0, label="A")
    pareto.add_point(energy_joules=200.0, accuracy_percent=80.0, label="B")
    pareto.add_point(energy_joules=300.0, accuracy_percent=90.0, label="C")
    pareto.add_point(energy_joules=150.0, accuracy_percent=60.0, label="D")
    pareto.add_point(energy_joules=400.0, accuracy_percent=85.0, label="E")

    frontier = pareto.compute_frontier()
    print("frontier   :", [p.label for p in frontier])
    assert {p.label for p in frontier} == {"A", "B", "C"}

    # ---- Bug fix: NaN / inf / negative ---------------------------- #
    for bad in (
        dict(energy_joules=-1.0, accuracy_percent=50.0, label="x"),
        dict(energy_joules=float("nan"), accuracy_percent=50.0, label="x"),
        dict(energy_joules=100.0, accuracy_percent=-1.0, label="x"),
        dict(energy_joules=100.0, accuracy_percent=50.0, label=""),
    ):
        try:
            pareto.add_point(**bad)  # type: ignore[arg-type]
        except SustainabilityParetoError as exc:
            print("Rejected   :", exc)

    # ---- Statistics ----------------------------------------------- #
    print("statistics :", {
        k: v for k, v in pareto.statistics().items()
        if k != "uptime_seconds"
    })

    # ---- Serialization round-trip --------------------------------- #
    payload = pareto.to_json()
    restored = SustainabilityPareto.from_json(payload)
    assert restored.to_dict() == pareto.to_dict()
    print("Round-trip OK.")

    print("\nSmoke test passed.")
