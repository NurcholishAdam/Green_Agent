# metrics/quantum_efficiency.py

"""
Quantum Efficiency Metric

Energy-per-Bit inspired metric:

    E_eff = TaskCompletionRatio / QuantumEnergyConsumed

Enhancements
------------
- Epsilon-guarded division (no fragile ``== 0`` check).
- Thread-safe accumulation via ``RLock``.
- Strict / non-strict semantics for undefined efficiency.
- Immutable ``EfficiencySample`` history with bounded ring-buffer.
- Serialization to / from ``dict`` and JSON.
- Context-manager support for scoped measurement.
- Custom :class:`QuantumEfficiencyError`.
- Lazy ``%s`` logging, ``__repr__``, type hints, and a smoke test under ``__main__``.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors & data containers
# --------------------------------------------------------------------------- #
class QuantumEfficiencyError(ValueError):
    """Raised when metric inputs or configuration are invalid."""


@dataclass(frozen=True)
class EfficiencySample:
    """Immutable snapshot of one efficiency measurement."""

    timestamp: float
    energy_joules: float
    task_completion_ratio: float
    efficiency: float
    label: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Metric
# --------------------------------------------------------------------------- #
class QuantumEfficiencyMetric:
    """
    Computes Energy-per-Bit inspired metric:

        E_eff = TaskCompletionRatio / QuantumEnergyConsumed

    Features
    --------
    - Thread-safe accumulation via :class:`threading.RLock`.
    - Epsilon-guarded division (no fragile ``== 0`` checks).
    - Bounded history with statistical aggregation.
    - Serialization to / from ``dict`` and JSON.
    - Context-manager support for scoped runs.
    - Strict / non-strict modes for undefined efficiency.
    """

    def __init__(
        self,
        *,
        epsilon: float = 1e-12,
        max_history: Optional[int] = 1000,
        strict: bool = True,
    ) -> None:
        if epsilon <= 0:
            raise QuantumEfficiencyError("epsilon must be > 0.")
        if max_history is not None and max_history <= 0:
            raise QuantumEfficiencyError("max_history must be > 0 or None.")

        self._epsilon: float = float(epsilon)
        self._max_history: Optional[int] = max_history
        self._strict: bool = bool(strict)

        self._lock = threading.RLock()
        self._quantum_energy: float = 0.0
        self._task_completion_ratio: float = 0.0
        self._history: List[EfficiencySample] = []
        self._last_computed: Optional[float] = None
        self._ctx_start: Optional[float] = None

    # ------------------------------------------------------------------ props
    @property
    def quantum_energy(self) -> float:
        with self._lock:
            return self._quantum_energy

    @property
    def task_completion_ratio(self) -> float:
        with self._lock:
            return self._task_completion_ratio

    @property
    def last_computed(self) -> Optional[float]:
        with self._lock:
            return self._last_computed

    @property
    def history(self) -> List[EfficiencySample]:
        with self._lock:
            return list(self._history)

    # --------------------------------------------------------------- mutation
    def add_quantum_energy(
        self, energy_joules: float, *, label: Optional[str] = None
    ) -> None:
        """Accumulate quantum energy. Non-negative, finite values only."""
        self._validate_energy(energy_joules)
        with self._lock:
            self._quantum_energy += float(energy_joules)
            logger.debug(
                "Added %.6g J (total=%.6g J)", energy_joules, self._quantum_energy
            )
            if label is not None:
                self._maybe_snapshot(label=label)

    def add_quantum_energy_batch(self, values: Iterable[float]) -> float:
        """
        Atomically accumulate an iterable of energies.

        Returns the total delta added.
        """
        total = 0.0
        for v in values:
            self._validate_energy(v)
            total += float(v)
        with self._lock:
            self._quantum_energy += total
            logger.debug(
                "Batch-added %.6g J (total=%.6g J)", total, self._quantum_energy
            )
        return total

    def set_task_completion_ratio(self, ratio: float) -> None:
        """Set task completion ratio. Must lie in ``[0, 1]``."""
        if not isinstance(ratio, (int, float)) or math.isnan(ratio):
            raise QuantumEfficiencyError("ratio must be a real number.")
        if not 0.0 <= ratio <= 1.0:
            raise QuantumEfficiencyError(
                "Task completion ratio must be between 0 and 1."
            )
        with self._lock:
            self._task_completion_ratio = float(ratio)

    # ---------------------------------------------------------------- compute
    def compute(
        self, *, record: bool = True, label: Optional[str] = None
    ) -> Optional[float]:
        """
        Return ``E_eff``, or ``None`` when the denominator is at/below
        ``epsilon`` and strict mode is active. In non-strict mode, returns
        ``0.0`` instead of ``None``.
        """
        with self._lock:
            if self._quantum_energy <= self._epsilon:
                msg = (
                    "Quantum energy (%.3e J) at/below epsilon (%.3e J); "
                    "efficiency undefined."
                    % (self._quantum_energy, self._epsilon)
                )
                if self._strict:
                    logger.warning(msg)
                    return None
                logger.warning("%s Returning 0.0 (non-strict).", msg)
                self._last_computed = 0.0
                return 0.0

            efficiency = self._task_completion_ratio / self._quantum_energy
            self._last_computed = efficiency
            if record:
                self._record_sample(efficiency, label=label)
            logger.info("Quantum efficiency computed: %.6g", efficiency)
            return efficiency

    # ------------------------------------------------------------------ stats
    def statistics(self) -> Dict[str, Optional[float]]:
        """Return descriptive statistics over the recorded history."""
        with self._lock:
            values = [s.efficiency for s in self._history]

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

    # -------------------------------------------------------------- lifecycle
    def reset(self, *, clear_history: bool = False) -> None:
        """Reset accumulators; optionally clear history."""
        with self._lock:
            self._quantum_energy = 0.0
            self._task_completion_ratio = 0.0
            self._last_computed = None
            if clear_history:
                self._history.clear()
            logger.debug(
                "QuantumEfficiencyMetric reset (history cleared=%s)", clear_history
            )

    def __enter__(self) -> "QuantumEfficiencyMetric":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped quantum efficiency measurement.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None

        if exc_type is not None:
            logger.debug(
                "Skipping compute on context exit due to %s (after %.4fs).",
                exc_type.__name__,
                elapsed,
            )
            return
        try:
            self.compute(record=True, label="context")
        except Exception:  # pragma: no cover — never mask caller exception path
            logger.exception("Failed to compute efficiency on context exit")
        finally:
            logger.info("Scoped quantum efficiency measurement took %.4fs.", elapsed)

    # ----------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "quantum_energy": self._quantum_energy,
                "task_completion_ratio": self._task_completion_ratio,
                "last_computed": self._last_computed,
                "epsilon": self._epsilon,
                "max_history": self._max_history,
                "strict": self._strict,
                "history": [s.to_dict() for s in self._history],
            }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QuantumEfficiencyMetric":
        metric = cls(
            epsilon=float(data.get("epsilon", 1e-12)),
            max_history=data.get("max_history", 1000),
            strict=bool(data.get("strict", True)),
        )
        with metric._lock:
            metric._quantum_energy = float(data.get("quantum_energy", 0.0))
            metric._task_completion_ratio = float(
                data.get("task_completion_ratio", 0.0)
            )
            metric._last_computed = data.get("last_computed")
            for entry in data.get("history", []):
                metric._history.append(EfficiencySample(**entry))
        return metric

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "QuantumEfficiencyMetric":
        return cls.from_dict(json.loads(payload))

    # --------------------------------------------------------------- internals
    def _validate_energy(self, energy_joules: float) -> None:
        if (
            not isinstance(energy_joules, (int, float))
            or math.isnan(energy_joules)
            or math.isinf(energy_joules)
        ):
            raise QuantumEfficiencyError("Energy must be a finite number.")
        if energy_joules < 0:
            raise QuantumEfficiencyError("Energy must be non-negative.")

    def _record_sample(self, efficiency: float, *, label: Optional[str]) -> None:
        sample = EfficiencySample(
            timestamp=time.time(),
            energy_joules=self._quantum_energy,
            task_completion_ratio=self._task_completion_ratio,
            efficiency=efficiency,
            label=label,
        )
        self._history.append(sample)
        if self._max_history is not None and len(self._history) > self._max_history:
            # Bounded ring-buffer: drop oldest.
            del self._history[0]

    def _maybe_snapshot(self, *, label: Optional[str]) -> None:
        if self._quantum_energy > self._epsilon:
            self._record_sample(
                self._task_completion_ratio / self._quantum_energy,
                label=label,
            )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "QuantumEfficiencyMetric("
            f"energy={self._quantum_energy:.6g}J, "
            f"ratio={self._task_completion_ratio:.3f}, "
            f"last={self._last_computed})"
        )


# --------------------------------------------------------------------------- #
# Local smoke test: python -m metrics.quantum_efficiency
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    metric = QuantumEfficiencyMetric(epsilon=1e-9, max_history=10)
    metric.add_quantum_energy_batch([0.5, 0.25, 0.25])
    metric.set_task_completion_ratio(0.9)

    eff = metric.compute(label="run-1")
    print("Efficiency:", eff)
    print("Statistics:", metric.statistics())

    # Round-trip serialization
    payload = metric.to_json()
    restored = QuantumEfficiencyMetric.from_json(payload)
    print("Restored:", restored)
    assert restored.to_dict() == metric.to_dict()
    print("Serialization round-trip OK.")

    # Context manager (non-strict branch triggers 0.0 return)
    with QuantumEfficiencyMetric(strict=False) as ctx:
        ctx.add_quantum_energy(0.0)
        ctx.set_task_completion_ratio(0.5)
