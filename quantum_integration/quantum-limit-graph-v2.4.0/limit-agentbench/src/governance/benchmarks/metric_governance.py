"""
MetricGovernance — enforce metric definitions and units.

A small, thread-safe registry of approved metric definitions plus
validation helpers. Metrics must be registered before they can be
validated; each definition carries a unit, a comparison direction,
and optional numeric bounds.

Numeric values accepted by ``validate``/``check``:

* ``int`` / ``float``
* any other ``numbers.Real`` (e.g. ``numpy.float64``)
* ``bool`` is explicitly rejected (it is an ``int`` subclass)
* ``NaN`` / ``±Inf`` are rejected
"""

from __future__ import annotations

import math
import numbers
import threading
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import (
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Union,
)

__all__ = [
    "Direction",
    "ErrorCode",
    "MetricDefinition",
    "MetricViolation",
    "MetricValidationError",
    "MetricGovernance",
    "Number",
]


Number = Union[int, float]


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------
class Direction(str, Enum):
    """Whether lower or higher values of a metric are considered better."""

    MIN = "min"   # lower is better (e.g. latency)
    MAX = "max"   # higher is better (e.g. throughput)


class ErrorCode(str, Enum):
    """Machine-readable codes attached to every validation violation."""

    NOT_GOVERNED = "not_governed"
    NOT_NUMERIC = "not_numeric"
    NOT_FINITE = "not_finite"
    BELOW_MIN = "below_min"
    ABOVE_MAX = "above_max"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
class MetricViolation(NamedTuple):
    """A single validation finding, usable both as a message and as data."""

    metric: str
    code: ErrorCode
    message: str


@dataclass(frozen=True)
class MetricDefinition:
    """Immutable, self-validating description of a governed metric."""

    name: str
    unit: str
    direction: Direction
    description: str = ""
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    def __post_init__(self) -> None:
        # name / unit must be non-empty strings
        if not self.name or not self.name.strip():
            raise ValueError("MetricDefinition.name must be non-empty")
        if not self.unit or not self.unit.strip():
            raise ValueError("MetricDefinition.unit must be non-empty")

        # coerce plain strings to Direction (raises ValueError on bad input)
        if not isinstance(self.direction, Direction):
            object.__setattr__(self, "direction", Direction(self.direction))

        # bounds must be consistent
        if (
            self.min_value is not None
            and self.max_value is not None
            and self.min_value > self.max_value
        ):
            raise ValueError(
                f"MetricDefinition '{self.name}': min_value "
                f"({self.min_value}) must be <= max_value ({self.max_value})"
            )


class MetricValidationError(ValueError):
    """Aggregate error raised by :meth:`MetricGovernance.validate_or_raise`."""

    def __init__(self, violations: List[MetricViolation]) -> None:
        self.violations = list(violations)
        super().__init__("; ".join(v.message for v in self.violations))


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
class MetricGovernance:
    """Thread-safe registry of approved metric definitions."""

    def __init__(self) -> None:
        self._metrics: Dict[str, MetricDefinition] = {}
        self._lock = threading.RLock()

    # -- registration ------------------------------------------------------
    def register(
        self,
        definition: MetricDefinition,
        *,
        overwrite: bool = False,
    ) -> MetricDefinition:
        """Register a metric. Raises ``KeyError`` on duplicates unless
        ``overwrite=True``."""
        if not isinstance(definition, MetricDefinition):
            raise TypeError(
                f"expected MetricDefinition, got {type(definition).__name__}"
            )
        with self._lock:
            if definition.name in self._metrics and not overwrite:
                raise KeyError(
                    f"metric '{definition.name}' already registered "
                    f"(pass overwrite=True to replace)"
                )
            self._metrics[definition.name] = definition
        return definition

    def register_many(
        self,
        definitions: Iterable[MetricDefinition],
        *,
        overwrite: bool = False,
    ) -> None:
        for d in definitions:
            self.register(d, overwrite=overwrite)

    def unregister(self, name: str) -> bool:
        """Remove a metric. Returns True if it existed."""
        with self._lock:
            return self._metrics.pop(name, None) is not None

    # -- lookup ------------------------------------------------------------
    def __contains__(self, name: object) -> bool:
        return name in self._metrics

    def __len__(self) -> int:
        return len(self._metrics)

    def __iter__(self) -> Iterator[str]:
        with self._lock:
            return iter(list(self._metrics))

    def get(self, name: str) -> MetricDefinition:
        try:
            return self._metrics[name]
        except KeyError:
            raise KeyError(f"metric '{name}' not governed") from None

    def definitions(self) -> Mapping[str, Mapping[str, object]]:
        """Return a read-only snapshot of all registered definitions."""
        with self._lock:
            snapshot = {
                name: MappingProxyType(
                    {
                        "unit": d.unit,
                        "direction": d.direction.value,
                        "description": d.description,
                        "min_value": d.min_value,
                        "max_value": d.max_value,
                    }
                )
                for name, d in self._metrics.items()
            }
        return MappingProxyType(snapshot)

    # -- validation --------------------------------------------------------
    def check(self, metrics: Mapping[str, object]) -> List[MetricViolation]:
        """Return structured violations for ``metrics`` (empty list = valid)."""
        violations: List[MetricViolation] = []

        for name, value in metrics.items():
            defn = self._metrics.get(name)
            if defn is None:
                violations.append(
                    MetricViolation(
                        name,
                        ErrorCode.NOT_GOVERNED,
                        f"metric '{name}' not governed",
                    )
                )
                continue

            # Reject bool explicitly — isinstance(True, int) is True.
            if isinstance(value, bool) or not isinstance(value, numbers.Real):
                violations.append(
                    MetricViolation(
                        name,
                        ErrorCode.NOT_NUMERIC,
                        f"metric '{name}' value is not numeric "
                        f"(got {type(value).__name__})",
                    )
                )
                continue

            # Reject NaN / ±Inf. Huge ints overflow float conversion but are
            # always finite, so that case is treated as finite.
            try:
                finite = math.isfinite(value)  # type: ignore[arg-type]
            except (OverflowError, TypeError):
                finite = True
            if not finite:
                violations.append(
                    MetricViolation(
                        name,
                        ErrorCode.NOT_FINITE,
                        f"metric '{name}' value must be finite (got {value!r})",
                    )
                )
                continue

            if defn.min_value is not None and value < defn.min_value:
                violations.append(
                    MetricViolation(
                        name,
                        ErrorCode.BELOW_MIN,
                        f"metric '{name}' value {value} is below "
                        f"minimum {defn.min_value}",
                    )
                )
            if defn.max_value is not None and value > defn.max_value:
                violations.append(
                    MetricViolation(
                        name,
                        ErrorCode.ABOVE_MAX,
                        f"metric '{name}' value {value} is above "
                        f"maximum {defn.max_value}",
                    )
                )

        return violations

    def validate(self, metrics: Mapping[str, object]) -> List[str]:
        """Backwards-compatible wrapper — returns plain error strings."""
        return [v.message for v in self.check(metrics)]

    def is_valid(self, metrics: Mapping[str, object]) -> bool:
        return not self.check(metrics)

    def validate_or_raise(self, metrics: Mapping[str, object]) -> None:
        violations = self.check(metrics)
        if violations:
            raise MetricValidationError(violations)

    # -- comparison --------------------------------------------------------
    def is_better(self, name: str, a: Number, b: Number) -> bool:
        """Return True if ``a`` is 'better' than ``b`` for this metric."""
        defn = self.get(name)
        if defn.direction is Direction.MIN:
            return a < b
        return a > b
