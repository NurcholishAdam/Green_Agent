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

Storage model
-------------
Points are stored in a :class:`collections.deque` bounded by
``config.max_points``. When the deque is full, the **oldest inserted
point is evicted** (FIFO — a ring buffer). This is **not** an LRU cache:
there is no per-point access tracking. Eviction is silent except for the
``dropped_points`` counter in :meth:`statistics`.

Data-integrity policy
---------------------
This file **rejects** out-of-range values (above ``max_energy`` or
``max_accuracy``) and non-numeric values in both ``strict`` and
``strict=False`` modes. Pareto frontiers are comparison data — a clamped
or silently-zeroed point pollutes the frontier and produces a wrong
"best" for downstream consumers. The ``strict`` flag here controls only
**string/number coercion** (non-strict accepts numeric strings with a
warning); it does **not** relax range or finiteness checks.

Enhancements
------------
- ``SustainabilityParetoConfig`` — frozen, ``slots``, ``__repr__``; every
  field type-checked (``bool`` rejected for ``int`` fields, ``NaN``/``inf``
  rejected for floats); ``from_dict(..., strict=True)`` rejects unknown
  keys and wraps parse errors.
- **Renamed** ``dominance_tolerance`` → ``accuracy_gain_threshold`` — the
  old name implied "allow near-dominated points through", but the code
  did the opposite (require a minimum gain). This is a **breaking change**.
- ``AgentPerformancePoint`` — ``slots``; validates ``timestamp`` is a
  ``datetime``; ``from_dict`` wraps parse errors as
  ``SustainabilityParetoError`` and rejects missing required keys.
- **Dead config fields enforced** — ``max_energy`` / ``max_accuracy`` are
  now checked in ``add_point`` and ``from_dict``.
- **Strict-mode coercion fixed** — ``add_point`` validates before casting,
  so ``add_point(True, ...)`` raises instead of silently becoming
  ``energy=1.0``.
- **``to_json(include_points=...)`` now works** — the kwarg is routed to
  ``to_dict`` instead of being forwarded to ``json.dumps``.
- **Lifecycle counters** — ``total_added`` and ``dropped_points`` are
  decoupled from ``len(points)``.
- **Atomic ``statistics()``** — no re-lock, no re-sort of ``self.points``;
  every field read from one snapshot under one lock acquisition.
- ``add_points(points)`` — batch add with one lock acquisition.
- ``frontier_as_tuples()`` — like ``as_tuples()`` but ordered by the
  frontier.
- ``total_added`` / ``dropped_points`` properties.
- ``__eq__`` / ``__hash__`` on config and frontier (auto on the frozen
  point); ``__dir__``.
- ``__main__`` smoke test uses explicit ``_check()`` helpers so
  ``python -O`` does not silently pass.

Compatibility
-------------
- ``SustainabilityPareto(config=None, strict=True)`` unchanged.
- ``add_point(energy_joules, accuracy_percent, label)`` unchanged in
  return type; now raises on ``bool`` / out-of-range inputs in **both**
  strict and non-strict modes (previously silently coerced).
- ``compute_frontier()`` / ``as_tuples()`` / ``reset()`` unchanged.
- **Breaking**: ``SustainabilityParetoConfig(dominance_tolerance=...)``
  must become ``SustainabilityParetoConfig(accuracy_gain_threshold=...)``.
  The old name is gone.
- ``to_dict(include_points=True)`` **defaults to True** (unlike the rest
  of the tree, which defaults history/observations to False). This is
  because points are the frontier's primary state, not an audit trail.
- ``to_json`` signature is now ``to_json(*, include_points=True,
  **kwargs)`` — the kwarg routes correctly.
- ``SustainabilityParetoConfig.from_dict(...)`` now rejects unknown keys
  by default; pass ``strict=False`` to opt out.

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop it for older versions.
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
from typing import (
    Any, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple,
    TypedDict,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SustainabilityParetoError(ValueError):
    """Raised for invalid Pareto-frontier inputs or configuration."""


# --------------------------------------------------------------------------- #
# Typed statistics shape
# --------------------------------------------------------------------------- #
class SustainabilityParetoStatistics(TypedDict):
    """Return shape of :meth:`SustainabilityPareto.statistics`."""

    num_points: int
    total_added: int
    dropped_points: int
    frontier_size: int
    mean_energy_joules: float
    mean_accuracy_percent: float
    max_accuracy_percent: float
    min_energy_joules: float
    uptime_seconds: float


# --------------------------------------------------------------------------- #
# Numeric helpers
# --------------------------------------------------------------------------- #
def _require_finite_number(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SustainabilityParetoError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv):
        raise SustainabilityParetoError(
            f"{name} must be finite, got {value!r}."
        )
    return fv


def _require_positive_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SustainabilityParetoError(f"{name} must be an int.")
    if value <= 0:
        raise SustainabilityParetoError(f"{name} must be > 0.")
    return value


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class SustainabilityParetoConfig:
    """
    Tunable parameters for :class:`SustainabilityPareto`.

    ``accuracy_gain_threshold``
        A point enters the frontier only if its accuracy strictly exceeds
        the running frontier's best accuracy by more than this value.
        ``0.0`` gives the strict Pareto frontier. Larger values collapse
        near-flat segments into a single representative point. (This field
        was previously named ``dominance_tolerance``, whose semantics were
        the opposite of what the name suggested.)
    """

    max_points: Optional[int] = 10_000
    accuracy_gain_threshold: float = 0.0
    max_energy: float = 1e15
    max_accuracy: float = 100.0

    def __post_init__(self) -> None:
        # ---- max_points --------------------------------------------- #
        if self.max_points is not None:
            _require_positive_int("max_points", self.max_points)

        # ---- accuracy_gain_threshold -------------------------------- #
        fv = _require_finite_number(
            "accuracy_gain_threshold", self.accuracy_gain_threshold
        )
        if fv < 0:
            raise SustainabilityParetoError(
                "accuracy_gain_threshold must be >= 0."
            )

        # ---- max_energy --------------------------------------------- #
        fv = _require_finite_number("max_energy", self.max_energy)
        if fv <= 0:
            raise SustainabilityParetoError("max_energy must be > 0.")

        # ---- max_accuracy ------------------------------------------- #
        fv = _require_finite_number("max_accuracy", self.max_accuracy)
        if fv <= 0:
            raise SustainabilityParetoError("max_accuracy must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = True,
    ) -> "SustainabilityParetoConfig":
        if not isinstance(data, Mapping):
            raise SustainabilityParetoError(
                "SustainabilityParetoConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        if strict:
            unknown = [k for k in data if k not in valid]
            if unknown:
                raise SustainabilityParetoError(
                    f"unknown SustainabilityParetoConfig keys: {unknown}"
                )
        try:
            kwargs = {k: v for k, v in data.items() if k in valid}
            return cls(**kwargs)
        except SustainabilityParetoError:
            raise
        except (TypeError, ValueError) as exc:
            raise SustainabilityParetoError(
                f"invalid SustainabilityParetoConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "SustainabilityParetoConfig("
            f"max_points={self.max_points}, "
            f"gain_threshold={self.accuracy_gain_threshold:.3g}, "
            f"max_energy={self.max_energy:.3g}, "
            f"max_accuracy={self.max_accuracy:.3g})"
        )


# --------------------------------------------------------------------------- #
# Point
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
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
            if isinstance(value, bool) or not isinstance(
                value, (int, float)
            ):
                raise SustainabilityParetoError(f"{name} must be numeric.")
            fv = float(value)
            if not math.isfinite(fv) or fv < 0:
                raise SustainabilityParetoError(
                    f"{name} must be finite and >= 0, got {value!r}."
                )
        if not isinstance(self.label, str) or not self.label:
            raise SustainabilityParetoError(
                "label must be a non-empty string."
            )
        if not isinstance(self.timestamp, datetime):
            raise SustainabilityParetoError(
                f"timestamp must be a datetime, got "
                f"{type(self.timestamp).__name__}."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "energy_joules": float(self.energy_joules),
            "accuracy_percent": float(self.accuracy_percent),
            "label": self.label,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AgentPerformancePoint":
        if not isinstance(data, Mapping):
            raise SustainabilityParetoError(
                "AgentPerformancePoint.from_dict expects a Mapping."
            )
        try:
            ts = data.get("timestamp")
            if ts is None:
                timestamp = datetime.now(timezone.utc)
            elif isinstance(ts, str):
                timestamp = datetime.fromisoformat(ts)
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
            elif isinstance(ts, datetime):
                timestamp = ts
            else:
                raise SustainabilityParetoError(
                    f"timestamp must be str|datetime|None, "
                    f"got {type(ts).__name__}."
                )
            return cls(
                energy_joules=float(data["energy_joules"]),
                accuracy_percent=float(data["accuracy_percent"]),
                label=str(data["label"]),
                timestamp=timestamp,
            )
        except SustainabilityParetoError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise SustainabilityParetoError(
                f"invalid AgentPerformancePoint payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "AgentPerformancePoint("
            f"label={self.label!r}, "
            f"energy={self.energy_joules:.4g}J, "
            f"accuracy={self.accuracy_percent:.2f}%)"
        )


# --------------------------------------------------------------------------- #
# Point-input validation (used by add_point / add_points / from_dict)
# --------------------------------------------------------------------------- #
def _validate_point_number(
    name: str, value: Any, *, strict: bool
) -> float:
    """
    Validate a numeric point input and return ``float(value)``.

    Policy
    ------
    - Bools are rejected in **all** modes: ``True``/``False`` as a
      measurement is almost certainly a bug, and coercing it to 0/1
      corrupts the frontier.
    - Numeric ints/floats must be finite and ``>= 0``; otherwise rejected.
    - Numeric strings are accepted in non-strict mode with a warning;
      rejected in strict mode.
    - Any other type is rejected.
    """
    if isinstance(value, bool):
        raise SustainabilityParetoError(
            f"{name} must be numeric, got bool (True/False is not a "
            f"valid measurement)."
        )
    if isinstance(value, (int, float)):
        fv = float(value)
        if not math.isfinite(fv) or fv < 0:
            raise SustainabilityParetoError(
                f"{name} must be finite and >= 0, got {value!r}."
            )
        return fv
    if isinstance(value, str) and not strict:
        try:
            fv = float(value)
        except ValueError as exc:
            raise SustainabilityParetoError(
                f"{name} is not a numeric string: {value!r}."
            ) from exc
        if not math.isfinite(fv) or fv < 0:
            raise SustainabilityParetoError(
                f"{name} is not a valid numeric string: {value!r}."
            )
        logger.warning(
            "%s was a string (%r); coerced to %s.", name, value, fv,
        )
        return fv
    raise SustainabilityParetoError(
        f"{name} must be numeric, got {type(value).__name__}."
    )


def _validate_label(value: Any, *, strict: bool) -> str:
    """Validate a point label and return the string form."""
    if isinstance(value, str) and value:
        return value
    if strict:
        if not isinstance(value, str):
            raise SustainabilityParetoError(
                f"label must be a non-empty string, got "
                f"{type(value).__name__}."
            )
        raise SustainabilityParetoError("label must be non-empty.")
    if value is None:
        raise SustainabilityParetoError("label must not be None.")
    result = str(value)
    if not result:
        raise SustainabilityParetoError(
            "label is empty after coercion."
        )
    logger.warning(
        "label was %s; coerced to %r.",
        type(value).__name__, result,
    )
    return result


# --------------------------------------------------------------------------- #
# Frontier
# --------------------------------------------------------------------------- #
class SustainabilityPareto:
    """
    Computes and manages the Sustainability Pareto frontier (energy vs
    accuracy).

    Lower energy and higher accuracy are preferred. Thread-safe,
    serializable, and bounded in memory (FIFO eviction when
    ``max_points`` is reached).

    Data-integrity policy
    ---------------------
    :meth:`add_point` **rejects** out-of-range values in both strict and
    non-strict modes. See the module docstring for the rationale.

    ``_started_at`` lifecycle
    -------------------------
    Set at construction. :meth:`reset` resets it to ``time.time()``.
    :meth:`from_dict` restores the serialized value, so
    ``statistics()["uptime_seconds"]`` on a restored frontier reports the
    **lifetime of the serialized object**, not the current instance. Call
    :meth:`reset` after restoring if you want uptime relative to now.
    """

    def __init__(
        self,
        *,
        config: Optional[SustainabilityParetoConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None and not isinstance(
            config, SustainabilityParetoConfig
        ):
            raise SustainabilityParetoError(
                f"config must be a SustainabilityParetoConfig, got "
                f"{type(config).__name__}."
            )
        self._config = config or SustainabilityParetoConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self.points: Deque[AgentPerformancePoint] = deque(
            maxlen=self._config.max_points
        )
        self._started_at: float = time.time()

        # Lifecycle counters (cumulative; not reset by reset()).
        self._total_added: int = 0
        self._dropped_points: int = 0

        logger.debug(
            "SustainabilityPareto initialized "
            "(max_points=%s, gain_threshold=%.3g, strict=%s)",
            self._config.max_points,
            self._config.accuracy_gain_threshold,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> SustainabilityParetoConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def count(self) -> int:
        """
        Number of points currently buffered (bounded by ``max_points``).

        Not the lifetime total — use :attr:`total_added` for that.
        """
        with self._lock:
            return len(self.points)

    @property
    def total_added(self) -> int:
        """Total number of ``add_point`` calls over the object's lifetime."""
        with self._lock:
            return self._total_added

    @property
    def dropped_points(self) -> int:
        """Number of points evicted by the FIFO bound."""
        with self._lock:
            return self._dropped_points

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

        Raises
        ------
        SustainabilityParetoError
            If ``energy_joules`` / ``accuracy_percent`` is non-numeric,
            ``bool``, non-finite, negative, or exceeds
            ``config.max_energy`` / ``config.max_accuracy``; or if
            ``label`` is empty / non-string (in strict mode).
        """
        energy_fv = _validate_point_number(
            "energy_joules", energy_joules, strict=self._strict,
        )
        accuracy_fv = _validate_point_number(
            "accuracy_percent", accuracy_percent, strict=self._strict,
        )
        # Range checks — enforced in both modes (see class docstring).
        if energy_fv > self._config.max_energy:
            raise SustainabilityParetoError(
                f"energy_joules={energy_fv} exceeds "
                f"max_energy={self._config.max_energy}."
            )
        if accuracy_fv > self._config.max_accuracy:
            raise SustainabilityParetoError(
                f"accuracy_percent={accuracy_fv} exceeds "
                f"max_accuracy={self._config.max_accuracy}."
            )
        label_str = _validate_label(label, strict=self._strict)

        point = AgentPerformancePoint(
            energy_joules=energy_fv,
            accuracy_percent=accuracy_fv,
            label=label_str,
        )
        with self._lock:
            self._append_locked(point)
        logger.debug("Added %s", point)
        return point

    def add_points(
        self,
        points: Iterable[Tuple[float, float, str]],
    ) -> List[AgentPerformancePoint]:
        """
        Add multiple ``(energy, accuracy, label)`` tuples in one call.

        All entries are validated before any state is mutated, so a
        validation failure mid-batch leaves the frontier unchanged.
        Returns the stored :class:`AgentPerformancePoint` objects.
        """
        validated: List[AgentPerformancePoint] = []
        for pair in points:
            if not (isinstance(pair, tuple) and len(pair) == 3):
                raise SustainabilityParetoError(
                    "each entry must be a (energy, accuracy, label) tuple."
                )
            energy_fv = _validate_point_number(
                "energy_joules", pair[0], strict=self._strict,
            )
            accuracy_fv = _validate_point_number(
                "accuracy_percent", pair[1], strict=self._strict,
            )
            if energy_fv > self._config.max_energy:
                raise SustainabilityParetoError(
                    f"energy_joules={energy_fv} exceeds "
                    f"max_energy={self._config.max_energy}."
                )
            if accuracy_fv > self._config.max_accuracy:
                raise SustainabilityParetoError(
                    f"accuracy_percent={accuracy_fv} exceeds "
                    f"max_accuracy={self._config.max_accuracy}."
                )
            label_str = _validate_label(pair[2], strict=self._strict)
            validated.append(AgentPerformancePoint(
                energy_joules=energy_fv,
                accuracy_percent=accuracy_fv,
                label=label_str,
            ))
        with self._lock:
            for point in validated:
                self._append_locked(point)
        return validated

    def compute_frontier(self) -> List[AgentPerformancePoint]:
        """
        Return the Pareto-optimal subset.

        Ordering: ascending energy, then descending accuracy, then
        ascending label. Within equal energy, the higher-accuracy point is
        processed first — so a point is excluded only if a same-energy
        point has strictly higher accuracy, or a lower-energy point has at
        least its accuracy.
        """
        with self._lock:
            points = list(self.points)
            frontier = self._compute_frontier_locked(points)
        logger.debug(
            "Computed Pareto frontier: %d/%d points.",
            len(frontier), len(points),
        )
        return frontier

    def as_tuples(self) -> List[Tuple[float, float, str]]:
        """
        Return the stored points as ``(energy, accuracy, label)`` tuples
        in **insertion order** (FIFO). For frontier-ordered tuples, use
        :meth:`frontier_as_tuples`.
        """
        with self._lock:
            return [
                (p.energy_joules, p.accuracy_percent, p.label)
                for p in self.points
            ]

    def frontier_as_tuples(self) -> List[Tuple[float, float, str]]:
        """Return the frontier points as ``(energy, accuracy, label)``."""
        return [
            (p.energy_joules, p.accuracy_percent, p.label)
            for p in self.compute_frontier()
        ]

    def reset(self) -> int:
        """
        Clear all points. Returns the number of buffered entries removed.

        Lifecycle counters (``total_added``, ``dropped_points``) are
        **not** reset.
        """
        with self._lock:
            removed = len(self.points)
            self.points.clear()
            self._started_at = time.time()
        logger.debug(
            "SustainabilityPareto reset (removed %d).", removed,
        )
        return removed

    # ---------------------------------------------------------- internals
    def _append_locked(self, point: AgentPerformancePoint) -> None:
        """Append + bump counters. Caller must hold ``self._lock``."""
        if (
            self._config.max_points is not None
            and len(self.points) == self._config.max_points
        ):
            self._dropped_points += 1
        self._total_added += 1
        self.points.append(point)

    def _compute_frontier_locked(
        self, points: List[AgentPerformancePoint]
    ) -> List[AgentPerformancePoint]:
        """Compute the frontier from a snapshot. Caller must hold the lock."""
        if not points:
            return []
        tol = self._config.accuracy_gain_threshold
        sorted_points = sorted(
            points,
            key=lambda p: (
                p.energy_joules, -p.accuracy_percent, p.label,
            ),
        )
        frontier: List[AgentPerformancePoint] = []
        best_accuracy = -math.inf
        for p in sorted_points:
            if p.accuracy_percent > best_accuracy + tol:
                frontier.append(p)
                best_accuracy = p.accuracy_percent
        return frontier

    # ---------------------------------------------------------- statistics
    def statistics(self) -> SustainabilityParetoStatistics:
        """Return a JSON-safe, atomic snapshot of the frontier state."""
        with self._lock:
            points = list(self.points)
            started = self._started_at
            total = self._total_added
            dropped = self._dropped_points
            frontier = self._compute_frontier_locked(points)
            if not points:
                return {
                    "num_points": 0,
                    "total_added": total,
                    "dropped_points": dropped,
                    "frontier_size": 0,
                    "mean_energy_joules": 0.0,
                    "mean_accuracy_percent": 0.0,
                    "max_accuracy_percent": 0.0,
                    "min_energy_joules": 0.0,
                    "uptime_seconds": time.time() - started,
                }
            energies = [p.energy_joules for p in points]
            accuracies = [p.accuracy_percent for p in points]
            return {
                "num_points": len(points),
                "total_added": total,
                "dropped_points": dropped,
                "frontier_size": len(frontier),
                "mean_energy_joules": sum(energies) / len(energies),
                "mean_accuracy_percent": (
                    sum(accuracies) / len(accuracies)
                ),
                "max_accuracy_percent": max(accuracies),
                "min_energy_joules": min(energies),
                "uptime_seconds": time.time() - started,
            }

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_points: bool = True) -> Dict[str, Any]:
        """
        Serialize the frontier.

        ``include_points`` defaults to ``True`` here (unlike
        ``include_history`` / ``include_observations`` in the rest of the
        tree, which default to ``False``) because the points are the
        frontier's primary state, not an audit trail.
        """
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "started_at": self._started_at,
                "total_added": self._total_added,
                "dropped_points": self._dropped_points,
            }
            if include_points:
                payload["points"] = [
                    p.to_dict() for p in self.points
                ]
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SustainabilityPareto":
        if not isinstance(data, Mapping):
            raise SustainabilityParetoError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        try:
            cfg = SustainabilityParetoConfig.from_dict(
                dict(data.get("config") or {}), strict=True,
            )
            pareto = cls(
                config=cfg, strict=bool(data.get("strict", True)),
            )
            with pareto._lock:
                if "points" not in data:
                    logger.info(
                        "from_dict: payload has no 'points' key; "
                        "restoring empty frontier."
                    )
                for entry in data.get("points", []) or []:
                    point = AgentPerformancePoint.from_dict(entry)
                    if point.energy_joules > cfg.max_energy:
                        raise SustainabilityParetoError(
                            f"restored point energy={point.energy_joules} "
                            f"exceeds max_energy={cfg.max_energy}."
                        )
                    if point.accuracy_percent > cfg.max_accuracy:
                        raise SustainabilityParetoError(
                            f"restored point accuracy="
                            f"{point.accuracy_percent} exceeds "
                            f"max_accuracy={cfg.max_accuracy}."
                        )
                    pareto._append_locked(point)

                if data.get("started_at") is not None:
                    pareto._started_at = float(data["started_at"])
                if "total_added" in data:
                    pareto._total_added = int(data["total_added"])
                elif pareto.points:
                    pareto._total_added = len(pareto.points)
                if "dropped_points" in data:
                    pareto._dropped_points = int(
                        data["dropped_points"]
                    )
            return pareto
        except SustainabilityParetoError:
            raise
        except (TypeError, ValueError) as exc:
            raise SustainabilityParetoError(
                f"invalid SustainabilityPareto payload: {exc}"
            ) from exc

    def to_json(
        self, *, include_points: bool = True, **kwargs: Any
    ) -> str:
        # Payload is primitive-only; no default= needed.
        return json.dumps(
            self.to_dict(include_points=include_points), **kwargs
        )

    @classmethod
    def from_json(cls, payload: str) -> "SustainabilityPareto":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SustainabilityParetoError(
                f"Invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    # ----------------------------------------------------------------- dunder
    def __len__(self) -> int:
        """
        Number of points currently buffered (bounded by ``max_points``).

        Not the lifetime total — see :attr:`total_added`.
        """
        return self.count

    def __repr__(self) -> str:
        with self._lock:
            return (
                "SustainabilityPareto("
                f"points={len(self.points)}, "
                f"total_added={self._total_added}, "
                f"max_points={self._config.max_points}, "
                f"strict={self._strict})"
            )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SustainabilityPareto):
            return NotImplemented
        return self._config == other._config and self._strict == other._strict

    def __hash__(self) -> int:
        return hash((self._config, self._strict))


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "AgentPerformancePoint",
    "SustainabilityPareto",
    "SustainabilityParetoConfig",
    "SustainabilityParetoError",
    "SustainabilityParetoStatistics",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m sustainability.pareto_frontier
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    import sys

    passed = 0
    failed = 0

    def _check(name: str, cond: bool) -> None:
        global passed, failed
        if cond:
            passed += 1
            print(f"  ✅ {name}")
        else:
            failed += 1
            print(f"  ❌ {name}")

    # ---- 1. Basic frontier ---------------------------------------- #
    print("basic frontier:")
    pareto = SustainabilityPareto()
    print("  repr:", pareto)
    pareto.add_point(energy_joules=100.0, accuracy_percent=70.0, label="A")
    pareto.add_point(energy_joules=200.0, accuracy_percent=80.0, label="B")
    pareto.add_point(energy_joules=300.0, accuracy_percent=90.0, label="C")
    pareto.add_point(energy_joules=150.0, accuracy_percent=60.0, label="D")
    pareto.add_point(energy_joules=400.0, accuracy_percent=85.0, label="E")
    frontier = pareto.compute_frontier()
    _check(
        "frontier labels {A, B, C}",
        {p.label for p in frontier} == {"A", "B", "C"},
    )
    _check(
        "frontier ordered by energy",
        [p.label for p in frontier] == ["A", "B", "C"],
    )

    # ---- 2. add_point rejects bool / out-of-range ---------------- #
    print("add_point validation (both modes):")
    for strict_val in (True, False):
        p = SustainabilityPareto(strict=strict_val)
        # Bool energy
        try:
            p.add_point(True, 50.0, "x")  # type: ignore[arg-type]
            _check(
                f"strict={strict_val}: bool energy rejected", False,
            )
        except SustainabilityParetoError:
            _check(
                f"strict={strict_val}: bool energy rejected", True,
            )
        # NaN energy
        try:
            p.add_point(float("nan"), 50.0, "x")
            _check(
                f"strict={strict_val}: nan energy rejected", False,
            )
        except SustainabilityParetoError:
            _check(
                f"strict={strict_val}: nan energy rejected", True,
            )
        # Above max_energy
        try:
            p.add_point(1e20, 50.0, "x")
            _check(
                f"strict={strict_val}: above max_energy rejected", False,
            )
        except SustainabilityParetoError:
            _check(
                f"strict={strict_val}: above max_energy rejected", True,
            )
        # Above max_accuracy
        try:
            p.add_point(100.0, 500.0, "x")
            _check(
                f"strict={strict_val}: above max_accuracy rejected",
                False,
            )
        except SustainabilityParetoError:
            _check(
                f"strict={strict_val}: above max_accuracy rejected",
                True,
            )

    # ---- 3. String coercion in non-strict mode ------------------- #
    print("string coercion (non-strict):")
    lenient = SustainabilityPareto(strict=False)
    p = lenient.add_point("100", "50", "x")  # type: ignore[arg-type]
    _check("string energy coerced", p.energy_joules == 100.0)
    _check("string accuracy coerced", p.accuracy_percent == 50.0)
    # Strict mode rejects strings
    strict = SustainabilityPareto(strict=True)
    try:
        strict.add_point("100", 50, "x")  # type: ignore[arg-type]
        _check("strict rejects string energy", False)
    except SustainabilityParetoError:
        _check("strict rejects string energy", True)

    # ---- 4. Accuracy gain threshold ------------------------------ #
    print("accuracy_gain_threshold:")
    # tol=0: strict Pareto
    p0 = SustainabilityPareto(
        config=SustainabilityParetoConfig(
            accuracy_gain_threshold=0.0
        )
    )
    for e, a, l in [(100, 70, "A"), (150, 75, "B"), (200, 78, "C")]:
        p0.add_point(float(e), float(a), l)
    _check(
        "tol=0 keeps all",
        {p.label for p in p0.compute_frontier()}
        == {"A", "B", "C"},
    )
    # tol=10: requires >10% gain
    p10 = SustainabilityPareto(
        config=SustainabilityParetoConfig(
            accuracy_gain_threshold=10.0
        )
    )
    for e, a, l in [(100, 70, "A"), (150, 75, "B"), (200, 78, "C")]:
        p10.add_point(float(e), float(a), l)
    _check(
        "tol=10 collapses near-flat",
        {p.label for p in p10.compute_frontier()} == {"A"},
    )

    # ---- 5. FIFO eviction + counters ---------------------------- #
    print("FIFO eviction + counters:")
    p = SustainabilityPareto(
        config=SustainabilityParetoConfig(max_points=3)
    )
    for i in range(5):
        p.add_point(float(i * 10), float(i), f"pt{i}")
    _check("count capped at max_points", p.count == 3)
    _check("total_added == 5", p.total_added == 5)
    _check("dropped_points == 2", p.dropped_points == 2)
    _check(
        "invariant total == buffered + dropped",
        p.total_added == p.count + p.dropped_points,
    )
    _check(
        "oldest points evicted",
        [x.label for x in p.points] == ["pt2", "pt3", "pt4"],
    )

    # ---- 6. add_points batch ------------------------------------ #
    print("add_points batch:")
    p = SustainabilityPareto()
    added = p.add_points([
        (100.0, 70.0, "A"),
        (200.0, 80.0, "B"),
        (300.0, 90.0, "C"),
    ])
    _check("batch added 3", len(added) == 3)
    _check("count == 3", p.count == 3)
    # Batch rollback on bad value
    p2 = SustainabilityPareto()
    try:
        p2.add_points([
            (100.0, 70.0, "A"),
            (float("nan"), 80.0, "B"),
        ])
        _check("batch rejects bad value", False)
    except SustainabilityParetoError:
        _check("batch rejects bad value", True)
        _check("batch rollback", p2.count == 0)

    # ---- 7. Round-trip with points (bug fix) ------------------- #
    print("round-trip with points:")
    p = SustainabilityPareto()
    p.add_point(100.0, 70.0, "A")
    p.add_point(200.0, 80.0, "B")
    payload = p.to_json(include_points=True)
    restored = SustainabilityPareto.from_json(payload)
    _check(
        "points preserved",
        restored.as_tuples() == p.as_tuples(),
    )
    _check(
        "counters preserved",
        restored.total_added == p.total_added,
    )
    _check("eq works", restored == p)

    # include_points=False now works (was TypeError before)
    payload2 = p.to_json(include_points=False)
    _check(
        "include_points=False: no points key",
        "points" not in json.loads(payload2),
    )

    # ---- 8. from_dict error wrapping --------------------------- #
    print("from_dict errors:")
    try:
        SustainabilityPareto.from_dict(
            {"started_at": "not-a-number"}
        )
        _check("bad started_at rejected", False)
    except SustainabilityParetoError:
        _check("bad started_at rejected", True)

    try:
        SustainabilityPareto.from_dict(
            {"points": [{"label": "x"}]}  # missing fields
        )
        _check("missing point fields rejected", False)
    except SustainabilityParetoError:
        _check("missing point fields rejected", True)

    try:
        SustainabilityPareto.from_dict(
            {"points": [{
                "energy_joules": 1e20,
                "accuracy_percent": 50,
                "label": "x",
            }]}
        )
        _check("out-of-range restored point rejected", False)
    except SustainabilityParetoError:
        _check("out-of-range restored point rejected", True)

    # ---- 9. Config validation ---------------------------------- #
    print("config validation:")
    for bad_cfg in (
        dict(max_points=0),
        dict(max_points=-1),
        dict(max_points=True),               # bool
        dict(max_points=100.5),              # float
        dict(max_points="100"),              # string
        dict(accuracy_gain_threshold=-1.0),
        dict(accuracy_gain_threshold=float("nan")),
        dict(accuracy_gain_threshold=float("inf")),
        dict(max_energy=0),
        dict(max_energy=float("nan")),
        dict(max_accuracy=0),
        dict(max_accuracy=float("inf")),
    ):
        try:
            SustainabilityParetoConfig(**bad_cfg)  # type: ignore[arg-type]
            _check(f"reject cfg {list(bad_cfg)}", False)
        except SustainabilityParetoError:
            _check(f"reject cfg {list(bad_cfg)}", True)

    # ---- 10. Config.from_dict strict --------------------------- #
    print("config from_dict strict:")
    try:
        SustainabilityParetoConfig.from_dict({"typo": 1}, strict=True)
        _check("unknown key rejected", False)
    except SustainabilityParetoError:
        _check("unknown key rejected", True)
    cfg = SustainabilityParetoConfig.from_dict(
        {"typo": 1}, strict=False
    )
    _check(
        "unknown key accepted non-strict",
        cfg.max_points == 10_000,
    )

    # ---- 11. Timestamp validation ------------------------------ #
    print("timestamp validation:")
    try:
        AgentPerformancePoint(
            energy_joules=100.0,
            accuracy_percent=50.0,
            label="x",
            timestamp="not-a-datetime",  # type: ignore[arg-type]
        )
        _check("non-datetime timestamp rejected", False)
    except SustainabilityParetoError:
        _check("non-datetime timestamp rejected", True)

    # ---- 12. Atomic statistics -------------------------------- #
    print("statistics atomicity:")
    p = SustainabilityPareto()
    for i in range(3):
        p.add_point(float(i * 100), float(i * 10 + 50), f"pt{i}")
    stats = p.statistics()
    _check(
        "num_points matches",
        stats["num_points"] == p.count,
    )
    _check(
        "total_added matches",
        stats["total_added"] == p.total_added,
    )
    _check(
        "frontier_size matches",
        stats["frontier_size"] == len(p.compute_frontier()),
    )

    # ---- 13. frontier_as_tuples ------------------------------- #
    print("frontier_as_tuples:")
    p = SustainabilityPareto()
    p.add_point(100.0, 70.0, "A")
    p.add_point(200.0, 80.0, "B")
    p.add_point(150.0, 60.0, "Dominated")
    _check(
        "frontier_as_tuples matches compute_frontier",
        p.frontier_as_tuples()
        == [
            (pt.energy_joules, pt.accuracy_percent, pt.label)
            for pt in p.compute_frontier()
        ],
    )
    _check(
        "frontier excludes dominated point",
        all(t[2] != "Dominated" for t in p.frontier_as_tuples()),
    )

    # ---- 14. eq / hash ----------------------------------------- #
    print("eq/hash:")
    a = SustainabilityPareto(
        config=SustainabilityParetoConfig(max_points=100)
    )
    b = SustainabilityPareto(
        config=SustainabilityParetoConfig(max_points=100)
    )
    c = SustainabilityPareto(
        config=SustainabilityParetoConfig(max_points=200)
    )
    _check("equal configs", a == b)
    _check("hash equal", hash(a) == hash(b))
    _check("different configs", a != c)

    # ---- 15. dir ---------------------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))

    # ---- 16. Deferred: max_energy / max_accuracy now enforced -- #
    print("config fields enforced:")
    p = SustainabilityPareto(
        config=SustainabilityParetoConfig(
            max_energy=1000.0, max_accuracy=90.0,
        )
    )
    p.add_point(500.0, 80.0, "ok")
    try:
        p.add_point(2000.0, 80.0, "bad_energy")
        _check("max_energy enforced", False)
    except SustainabilityParetoError:
        _check("max_energy enforced", True)
    try:
        p.add_point(500.0, 95.0, "bad_accuracy")
        _check("max_accuracy enforced", False)
    except SustainabilityParetoError:
        _check("max_accuracy enforced", True)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
