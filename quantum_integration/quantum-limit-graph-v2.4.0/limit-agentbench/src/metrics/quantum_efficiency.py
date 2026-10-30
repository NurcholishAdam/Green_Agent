# metrics/quantum_efficiency.py

"""
Quantum Efficiency Metric (v5.1)
================================

Energy-per-Bit inspired metric::

    E_eff = TaskCompletionRatio / QuantumEnergyConsumed

Fixes in v5.1 (see review)
--------------------------
  * P0 — ``EfficiencySample.__post_init__`` validates every field.
         Previously ``from_dict`` could inject string timestamps, negative
         energies, out-of-range ratios, and NaN efficiencies into history,
         silently poisoning ``statistics()``.
  * P0 — ``from_dict`` trims the restored history to ``max_history``.
         Previously a 500-sample payload restored into a ``max_history=100``
         metric stayed at 500 forever (``_record_sample`` only drops one
         per append).
  * P0 — ``from_dict`` validates accumulators, ``last_computed``, and
         ``max_history`` through ``_safe_float`` / ``_safe_optional_int``.
         Previously accepted negative energies, NaN ratios, and string
         ``last_computed`` values.
  * P0 — ``_validate_energy`` and ``set_task_completion_ratio`` reject
         ``bool`` (``True`` was silently accepted as ``1.0``).
  * P1 — ``compute()`` returns ``None`` when energy is at/below epsilon
         **in both strict and non-strict modes**. The previous non-strict
         path returned ``0.0``, fabricating a value for an undefined
         measurement. (Behaviour change; see "Backward compatibility".)
  * P1 — ``compute()`` no longer writes ``_last_computed = 0.0`` on
         failure in non-strict mode. ``_last_computed`` reflects only the
         most recent successful computation.
  * P1 — New ``snapshot()`` method decouples sample recording from
         accumulation. ``add_quantum_energy(label=...)`` still works
         (backward compat) but now routes through ``snapshot()`` and
         logs a warning.
  * P1 — ``add_quantum_energy_batch`` validates all inputs before taking
         the lock and gains an optional ``label=`` for symmetry with
         ``add_quantum_energy``.
  * P1 — ``reset()`` clears ``_ctx_start`` so a ``with`` block that
         resets mid-scope does not leave a stale timer.
  * P1 — ``__repr__`` reads state under the lock.
  * P1 — ``_record_sample`` (now ``_record_sample_locked``) documents
         that the caller must hold the lock; the public ``snapshot()``
         acquires it.
  * P1 — ``set_task_completion_ratio`` uses ``math.isfinite`` for the
         same defensive check the energy validator already applied.
  * P1 — ``logger.info`` demoted to ``logger.debug`` on ``compute`` and
         ``__exit__`` (previously logged once per call — pathological in
         hot loops).
  * P2 — New API: ``__version__``, ``schema_version`` in ``to_dict()``,
         ``snapshot()``, ``last_sample``, ``mean_efficiency``,
         ``clear_history()``, ``history_since()``, ``history_between()``,
         ``add_quantum_energy_batch(..., label=)``.
  * P3 — ``p95`` uses nearest-rank (``ceil(0.95 * n) - 1``) matching the
         efficiency-calculator convention; ``from_dict`` accepts
         ``Mapping`` (not just ``Dict``); smoke test asserts every P0
         invariant.

Backward compatibility
----------------------
``compute()`` no longer returns ``0.0`` in non-strict mode. Callers that
relied on the old behaviour should adapt::

    efficiency = metric.compute() or 0.0

``add_quantum_energy(label=...)`` still records a sample after
accumulation (unchanged observable behaviour), but the intent is now
expressible explicitly via ``metric.snapshot(label=...)``. The ``label``
parameter on ``add_quantum_energy`` is marked deprecated and will be
removed in a future version.

``strict`` remains an accepted constructor flag but no longer affects
``compute()``. It is retained for signature compatibility.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field, fields
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

__version__ = "5.1.0"

__all__ = [
    "EfficiencySample",
    "QuantumEfficiencyError",
    "QuantumEfficiencyMetric",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Safe coercion helpers
# --------------------------------------------------------------------------- #

def _safe_float(
    value: Any, *, name: str, default: float = 0.0,
) -> float:
    if value is None:
        return default
    if isinstance(value, bool):
        raise QuantumEfficiencyError(
            f"{name} must be numeric, got bool {value!r}"
        )
    try:
        v = float(value)
    except (TypeError, ValueError) as exc:
        raise QuantumEfficiencyError(
            f"{name} must be numeric, got {value!r} "
            f"({type(value).__name__})"
        ) from exc
    if not math.isfinite(v):
        raise QuantumEfficiencyError(
            f"{name} must be finite, got {value!r}"
        )
    return v


def _safe_optional_int(
    value: Any,
    *,
    name: str,
    default: Optional[int] = None,
    min_value: Optional[int] = None,
) -> Optional[int]:
    if value is None:
        return default
    if isinstance(value, bool):
        raise QuantumEfficiencyError(
            f"{name} must be an integer, got bool {value!r}"
        )
    if isinstance(value, float):
        if not math.isfinite(value):
            raise QuantumEfficiencyError(
                f"{name} must be finite, got {value!r}"
            )
        if not value.is_integer():
            raise QuantumEfficiencyError(
                f"{name} must be an integer, got {value!r}"
            )
        result = int(value)
    else:
        try:
            result = int(value)
        except (TypeError, ValueError) as exc:
            raise QuantumEfficiencyError(
                f"{name} must be an integer, got {value!r} "
                f"({type(value).__name__})"
            ) from exc
    if min_value is not None and result < min_value:
        raise QuantumEfficiencyError(
            f"{name} must be >= {min_value}, got {result!r}"
        )
    return result


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

    def __post_init__(self) -> None:
        for name in (
            "timestamp", "energy_joules",
            "task_completion_ratio", "efficiency",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(
                value, (int, float)
            ):
                raise QuantumEfficiencyError(
                    f"{name} must be numeric, "
                    f"got {type(value).__name__}"
                )
            if not math.isfinite(float(value)):
                raise QuantumEfficiencyError(
                    f"{name} must be finite, got {value!r}"
                )
        if self.energy_joules < 0:
            raise QuantumEfficiencyError(
                f"energy_joules must be >= 0, "
                f"got {self.energy_joules!r}"
            )
        if not 0.0 <= self.task_completion_ratio <= 1.0:
            raise QuantumEfficiencyError(
                f"task_completion_ratio must be in [0, 1], "
                f"got {self.task_completion_ratio!r}"
            )
        if self.label is not None and not isinstance(self.label, str):
            raise QuantumEfficiencyError(
                f"label must be str or None, "
                f"got {type(self.label).__name__}"
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": __version__,
            **asdict(self),
        }


# --------------------------------------------------------------------------- #
# Metric
# --------------------------------------------------------------------------- #

class QuantumEfficiencyMetric:
    """
    Computes an Energy-per-Bit inspired metric::

        E_eff = TaskCompletionRatio / QuantumEnergyConsumed

    Features
    --------
    - Thread-safe accumulation via :class:`threading.RLock`.
    - Epsilon-guarded division (no fragile ``== 0`` checks).
    - Bounded history with statistical aggregation.
    - Serialization to / from ``dict`` and JSON.
    - Context-manager support for scoped runs.
    - Returns ``None`` for undefined efficiency (never fabricates ``0.0``).

    Notes
    -----
    The ``strict`` flag is retained for constructor-signature compatibility
    but does not affect :meth:`compute`. An efficiency whose denominator is
    at or below ``epsilon`` is *undefined*, not ``0.0``.
    """

    def __init__(
        self,
        *,
        epsilon: float = 1e-12,
        max_history: Optional[int] = 1000,
        strict: bool = True,
    ) -> None:
        if isinstance(epsilon, bool) or not isinstance(
            epsilon, (int, float)
        ):
            raise QuantumEfficiencyError(
                "epsilon must be numeric."
            )
        if not math.isfinite(float(epsilon)) or epsilon <= 0:
            raise QuantumEfficiencyError(
                f"epsilon must be finite and > 0, got {epsilon!r}."
            )
        if max_history is not None:
            if isinstance(max_history, bool) or not isinstance(
                max_history, int
            ):
                raise QuantumEfficiencyError(
                    "max_history must be an int or None."
                )
            if max_history <= 0:
                raise QuantumEfficiencyError(
                    "max_history must be > 0 or None."
                )

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
    def epsilon(self) -> float:
        return self._epsilon

    @property
    def max_history(self) -> Optional[int]:
        return self._max_history

    @property
    def strict(self) -> bool:
        """Retained for backward compatibility; no longer affects compute()."""
        return self._strict

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
    def last_sample(self) -> Optional[EfficiencySample]:
        """Most recent sample, or ``None`` if history is empty."""
        with self._lock:
            return self._history[-1] if self._history else None

    @property
    def history(self) -> List[EfficiencySample]:
        with self._lock:
            return list(self._history)

    @property
    def mean_efficiency(self) -> Optional[float]:
        """Convenience over ``statistics()["mean"]``."""
        with self._lock:
            values = [s.efficiency for s in self._history]
        if not values:
            return None
        return sum(values) / len(values)

    # --------------------------------------------------------------- mutation

    def add_quantum_energy(
        self, energy_joules: float, *, label: Optional[str] = None,
    ) -> None:
        """
        Accumulate quantum energy.

        Parameters
        ----------
        energy_joules : float
            Non-negative, finite energy to add.
        label : str, optional
            Deprecated. When provided, records a snapshot after
            accumulation (matching the pre-v5.1 behaviour). New callers
            should use :meth:`snapshot` explicitly.
        """
        self._validate_energy(energy_joules)
        with self._lock:
            self._quantum_energy += float(energy_joules)
            logger.debug(
                "Added %.6g J (total=%.6g J)",
                energy_joules, self._quantum_energy,
            )
        if label is not None:
            logger.debug(
                "add_quantum_energy(label=%r) is deprecated; call "
                "snapshot(label=...) explicitly.",
                label,
            )
            self.snapshot(label=label)

    def add_quantum_energy_batch(
        self,
        values: Iterable[float],
        *,
        label: Optional[str] = None,
    ) -> float:
        """
        Accumulate an iterable of energies atomically with respect to
        validation: either every value is valid (and the whole batch is
        committed under one lock acquisition) or none is committed.

        Parameters
        ----------
        values : Iterable[float]
            Each value must be a finite, non-negative number.
        label : str, optional
            When provided, records a snapshot after accumulation.

        Returns
        -------
        float
            The total delta added.
        """
        validated: List[float] = []
        for v in values:
            self._validate_energy(v)
            validated.append(float(v))
        total = sum(validated)
        with self._lock:
            self._quantum_energy += total
            logger.debug(
                "Batch-added %.6g J (total=%.6g J)",
                total, self._quantum_energy,
            )
        if label is not None:
            self.snapshot(label=label)
        return total

    def set_task_completion_ratio(self, ratio: float) -> None:
        """Set task completion ratio. Must lie in ``[0, 1]``."""
        if isinstance(ratio, bool) or not isinstance(ratio, (int, float)):
            raise QuantumEfficiencyError(
                f"ratio must be numeric, got {type(ratio).__name__}."
            )
        if not math.isfinite(float(ratio)):
            raise QuantumEfficiencyError(
                f"ratio must be finite, got {ratio!r}."
            )
        if not 0.0 <= ratio <= 1.0:
            raise QuantumEfficiencyError(
                "Task completion ratio must be between 0 and 1."
            )
        with self._lock:
            self._task_completion_ratio = float(ratio)

    # ---------------------------------------------------------------- compute

    def compute(
        self, *, record: bool = True, label: Optional[str] = None,
    ) -> Optional[float]:
        """
        Return ``E_eff``, or ``None`` when the denominator is at/below
        ``epsilon`` (undefined measurement).

        Never returns ``0.0`` for an undefined measurement.
        """
        with self._lock:
            efficiency = self._current_efficiency()
            if efficiency is None:
                logger.warning(
                    "Quantum energy (%.3e J) at/below epsilon (%.3e J); "
                    "efficiency undefined.",
                    self._quantum_energy, self._epsilon,
                )
                return None
            self._last_computed = efficiency
            if record:
                self._record_sample_locked(efficiency, label=label)
            logger.debug(
                "Quantum efficiency computed: %.6g", efficiency,
            )
            return efficiency

    def snapshot(
        self, *, label: Optional[str] = None,
    ) -> Optional[EfficiencySample]:
        """
        Record a sample of the current state.

        Returns the recorded sample, or ``None`` when energy is at/below
        epsilon (undefined efficiency).
        """
        with self._lock:
            efficiency = self._current_efficiency()
            if efficiency is None:
                logger.warning(
                    "Cannot snapshot: quantum energy (%.3e J) at/below "
                    "epsilon (%.3e J); efficiency undefined.",
                    self._quantum_energy, self._epsilon,
                )
                return None
            return self._record_sample_locked(
                efficiency, label=label,
            )

    # ------------------------------------------------------------------ stats

    def statistics(self) -> Dict[str, Any]:
        """Descriptive statistics over recorded history (consistent shape)."""
        with self._lock:
            values = [s.efficiency for s in self._history]

        empty: Dict[str, Any] = {
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
        n = len(ordered)
        # Nearest-rank percentile: robust and consistent across small n.
        p95_idx = min(n - 1, math.ceil(0.95 * n) - 1)
        return {
            "count": n,
            "mean": sum(ordered) / n,
            "median": ordered[n // 2],
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
            self._ctx_start = None
            if clear_history:
                self._history.clear()
            logger.debug(
                "QuantumEfficiencyMetric reset (history cleared=%s)",
                clear_history,
            )

    def clear_history(self) -> int:
        """Clear the history; return how many samples were removed."""
        with self._lock:
            n = len(self._history)
            self._history.clear()
        logger.debug("Cleared %d history sample(s).", n)
        return n

    def history_since(
        self, since_epoch_seconds: float,
    ) -> List[EfficiencySample]:
        """Samples with ``timestamp >= since_epoch_seconds``."""
        self._validate_finite("since_epoch_seconds", since_epoch_seconds)
        with self._lock:
            return [
                s for s in self._history
                if s.timestamp >= since_epoch_seconds
            ]

    def history_between(
        self, t0: float, t1: float,
    ) -> List[EfficiencySample]:
        """Samples with ``t0 <= timestamp <= t1``."""
        self._validate_finite("t0", t0)
        self._validate_finite("t1", t1)
        if t1 < t0:
            raise QuantumEfficiencyError("t1 must be >= t0.")
        with self._lock:
            return [
                s for s in self._history if t0 <= s.timestamp <= t1
            ]

    def __enter__(self) -> "QuantumEfficiencyMetric":
        self._ctx_start = time.perf_counter()
        logger.debug(
            "Entering scoped quantum efficiency measurement."
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start
            if self._ctx_start is not None
            else time.perf_counter()
        )
        self._ctx_start = None

        if exc_type is not None:
            logger.debug(
                "Skipping compute on context exit due to %s "
                "(after %.4fs).",
                exc_type.__name__, elapsed,
            )
            return
        try:
            self.compute(record=True, label="context")
        except Exception:  # pragma: no cover — never mask caller exception
            logger.exception(
                "Failed to compute efficiency on context exit"
            )
        finally:
            logger.debug(
                "Scoped quantum efficiency measurement took %.4fs.",
                elapsed,
            )

    # ----------------------------------------------------------- serialization

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "schema_version": __version__,
                "quantum_energy": self._quantum_energy,
                "task_completion_ratio": self._task_completion_ratio,
                "last_computed": self._last_computed,
                "epsilon": self._epsilon,
                "max_history": self._max_history,
                "strict": self._strict,
                "history": [s.to_dict() for s in self._history],
            }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "QuantumEfficiencyMetric":
        """
        Restore a metric from a serialized payload.

        Raises
        ------
        QuantumEfficiencyError
            On malformed input, non-finite numbers, or out-of-range
            accumulators. History entries are validated by
            :class:`EfficiencySample`; the restored history is trimmed to
            ``max_history`` (most recent samples kept).
        """
        if not isinstance(data, Mapping):
            raise QuantumEfficiencyError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        epsilon = _safe_float(
            data.get("epsilon", 1e-12),
            name="epsilon", default=1e-12,
        )
        if epsilon <= 0:
            raise QuantumEfficiencyError("epsilon must be > 0.")
        max_history = _safe_optional_int(
            data.get("max_history", 1000),
            name="max_history", default=1000, min_value=1,
        )
        metric = cls(
            epsilon=epsilon,
            max_history=max_history,
            strict=bool(data.get("strict", True)),
        )

        quantum_energy = _safe_float(
            data.get("quantum_energy", 0.0),
            name="quantum_energy", default=0.0,
        )
        if quantum_energy < 0:
            raise QuantumEfficiencyError(
                "quantum_energy must be >= 0."
            )
        ratio = _safe_float(
            data.get("task_completion_ratio", 0.0),
            name="task_completion_ratio", default=0.0,
        )
        if not 0.0 <= ratio <= 1.0:
            raise QuantumEfficiencyError(
                "task_completion_ratio must be in [0, 1]."
            )
        lc_raw = data.get("last_computed")
        last_computed = (
            None if lc_raw is None else _safe_float(
                lc_raw, name="last_computed",
            )
        )

        raw_history = data.get("history", []) or []
        if not isinstance(raw_history, list):
            raise QuantumEfficiencyError("history must be a list.")
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

        known_sample_fields = {f.name for f in fields(EfficiencySample)}

        with metric._lock:
            metric._quantum_energy = quantum_energy
            metric._task_completion_ratio = ratio
            metric._last_computed = last_computed
            for entry in raw_history:
                if not isinstance(entry, Mapping):
                    raise QuantumEfficiencyError(
                        f"history entry must be a Mapping, "
                        f"got {type(entry).__name__}."
                    )
                cleaned = {
                    k: v for k, v in entry.items()
                    if k in known_sample_fields
                }
                metric._history.append(
                    EfficiencySample(**cleaned)
                )
        return metric

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(
        cls, payload: str,
    ) -> "QuantumEfficiencyMetric":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise QuantumEfficiencyError(
                f"Invalid JSON payload: {exc}"
            ) from exc
        if not isinstance(data, Mapping):
            raise QuantumEfficiencyError(
                f"JSON payload must decode to an object, "
                f"got {type(data).__name__}."
            )
        return cls.from_dict(data)

    # --------------------------------------------------------------- internals

    def _current_efficiency(self) -> Optional[float]:
        """Efficiency from current state, or ``None`` if undefined.

        Caller must hold ``self._lock``.
        """
        if self._quantum_energy <= self._epsilon:
            return None
        return self._task_completion_ratio / self._quantum_energy

    def _validate_energy(self, energy_joules: Any) -> None:
        if isinstance(energy_joules, bool) or not isinstance(
            energy_joules, (int, float)
        ):
            raise QuantumEfficiencyError(
                f"Energy must be a finite number, "
                f"got {type(energy_joules).__name__}."
            )
        if not math.isfinite(float(energy_joules)):
            raise QuantumEfficiencyError(
                f"Energy must be a finite number, "
                f"got {energy_joules!r}."
            )
        if energy_joules < 0:
            raise QuantumEfficiencyError(
                "Energy must be non-negative."
            )

    def _validate_finite(self, name: str, value: Any) -> None:
        if isinstance(value, bool) or not isinstance(
            value, (int, float)
        ):
            raise QuantumEfficiencyError(
                f"{name} must be numeric, "
                f"got {type(value).__name__}."
            )
        if not math.isfinite(float(value)):
            raise QuantumEfficiencyError(
                f"{name} must be finite, got {value!r}."
            )

    def _record_sample_locked(
        self, efficiency: float, *, label: Optional[str],
    ) -> EfficiencySample:
        """Append a sample. Caller must hold ``self._lock``."""
        sample = EfficiencySample(
            timestamp=time.time(),
            energy_joules=self._quantum_energy,
            task_completion_ratio=self._task_completion_ratio,
            efficiency=efficiency,
            label=label,
        )
        self._history.append(sample)
        if (
            self._max_history is not None
            and len(self._history) > self._max_history
        ):
            del self._history[0]
        return sample

    # ----------------------------------------------------------------- dunder

    def __repr__(self) -> str:
        with self._lock:
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
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    # --- happy path ---
    metric = QuantumEfficiencyMetric(epsilon=1e-9, max_history=10)
    metric.add_quantum_energy_batch([0.5, 0.25, 0.25])
    metric.set_task_completion_ratio(0.9)

    eff = metric.compute(label="run-1")
    assert eff is not None
    print("Efficiency:", eff)
    print("Statistics:", metric.statistics())

    # --- round-trip ---
    payload = metric.to_json()
    restored = QuantumEfficiencyMetric.from_json(payload)
    assert restored.to_dict() == metric.to_dict()
    print("Serialization round-trip OK.")

    # --- P0 regression: EfficiencySample rejects bad values ---
    bad_samples = [
        dict(timestamp="2024-01-01", energy_joules=1.0,
             task_completion_ratio=0.5, efficiency=0.5),
        dict(timestamp=time.time(), energy_joules=-1.0,
             task_completion_ratio=0.5, efficiency=0.5),
        dict(timestamp=time.time(), energy_joules=1.0,
             task_completion_ratio=5.0, efficiency=0.5),
        dict(timestamp=time.time(), energy_joules=1.0,
             task_completion_ratio=0.5, efficiency=float("nan")),
        dict(timestamp=float("inf"), energy_joules=1.0,
             task_completion_ratio=0.5, efficiency=0.5),
    ]
    for bad in bad_samples:
        try:
            EfficiencySample(**bad)
        except QuantumEfficiencyError:
            pass
        else:
            raise AssertionError(
                f"EfficiencySample accepted bad input: {bad}"
            )
    print("Sample validation: OK")

    # --- P0 regression: from_dict rejects bad history ---
    bad_payload = {
        "history": [{"timestamp": "bad",
                     "energy_joules": 1.0,
                     "task_completion_ratio": 0.5,
                     "efficiency": 0.5}],
        "epsilon": 1e-12, "max_history": 100, "strict": True,
    }
    try:
        QuantumEfficiencyMetric.from_dict(bad_payload)
    except QuantumEfficiencyError as exc:
        print("Bad hist:", exc)
    else:
        raise AssertionError(
            "from_dict accepted invalid history entry."
        )

    # --- P0 regression: from_dict enforces max_history ---
    big_history = {
        "history": [
            {"timestamp": float(i), "energy_joules": 1.0,
             "task_completion_ratio": 0.5, "efficiency": 0.5}
            for i in range(50)
        ],
        "epsilon": 1e-12, "max_history": 5, "strict": True,
    }
    trimmed = QuantumEfficiencyMetric.from_dict(big_history)
    assert len(trimmed.history) == 5, (
        f"expected 5 samples, got {len(trimmed.history)}"
    )
    assert trimmed.history[-1].timestamp == 49.0
    print("from_dict max_history: enforced (len=5)")

    # --- P0 regression: from_dict rejects negative accumulators ---
    for bad_payload in (
        {"quantum_energy": -5.0},
        {"task_completion_ratio": 5.0},
        {"last_computed": "not a number"},
        {"max_history": "bogus"},
    ):
        try:
            QuantumEfficiencyMetric.from_dict(bad_payload)
        except QuantumEfficiencyError:
            pass
        else:
            raise AssertionError(
                f"from_dict accepted bad payload: {bad_payload}"
            )
    print("from_dict accumulator validation: OK")

    # --- P0 regression: bool rejected ---
    for bad_fn, args in (
        (metric.add_quantum_energy, (True,)),
        (metric.set_task_completion_ratio, (True,)),
        (metric.set_task_completion_ratio, (False,)),
    ):
        try:
            bad_fn(*args)
        except QuantumEfficiencyError:
            pass
        else:
            raise AssertionError(
                f"{bad_fn.__name__}{args} should have raised."
            )
    print("bool rejection: OK")

    # --- P1 regression: compute returns None in both modes ---
    for strict in (True, False):
        m = QuantumEfficiencyMetric(strict=strict)
        m.set_task_completion_ratio(0.9)
        result = m.compute()
        assert result is None, (
            f"strict={strict}: expected None, got {result!r}"
        )
        assert m.last_computed is None, (
            f"strict={strict}: _last_computed should be None, "
            f"got {m.last_computed!r}"
        )
    print("compute returns None uniformly: OK")

    # --- P1 regression: reset clears _ctx_start ---
    m = QuantumEfficiencyMetric()
    with m:
        m.add_quantum_energy(1.0)
        m.reset()
        assert m._ctx_start is None
    print("reset clears _ctx_start: OK")

    # --- P1: snapshot is decoupled and safe ---
    s_m = QuantumEfficiencyMetric()
    s_m.set_task_completion_ratio(0.9)
    assert s_m.snapshot() is None       # energy = 0 → undefined
    s_m.add_quantum_energy(1.0)
    snap = s_m.snapshot(label="explicit")
    assert snap is not None
    assert snap.label == "explicit"
    assert snap.efficiency == 0.9
    print("snapshot(): OK")

    # --- P1: add_quantum_energy(label=...) still works ---
    l_m = QuantumEfficiencyMetric()
    l_m.set_task_completion_ratio(0.5)
    l_m.add_quantum_energy(2.0, label="legacy")
    assert len(l_m.history) == 1
    assert l_m.history[0].label == "legacy"
    assert l_m.history[0].efficiency == 0.25
    print("legacy label= backward compat: OK")

    # --- P1: batch validation is all-or-nothing ---
    b_m = QuantumEfficiencyMetric()
    try:
        b_m.add_quantum_energy_batch([1.0, 2.0, -3.0, 4.0])
    except QuantumEfficiencyError:
        pass
    else:
        raise AssertionError(
            "batch with negative value should raise."
        )
    assert b_m.quantum_energy == 0.0, (
        f"batch mutated state on failure: {b_m.quantum_energy}"
    )
    total = b_m.add_quantum_energy_batch([1.0, 2.0, 3.0])
    assert total == 6.0
    assert b_m.quantum_energy == 6.0
    print("batch atomicity: OK")

    # --- P2: last_sample / mean_efficiency / history queries ---
    q_m = QuantumEfficiencyMetric()
    q_m.set_task_completion_ratio(0.8)
    q_m.add_quantum_energy(1.0)
    q_m.compute(label="one")
    q_m.add_quantum_energy(1.0)
    q_m.compute(label="two")
    assert q_m.last_sample.label == "two"
    assert q_m.mean_efficiency == (0.8 + 0.4) / 2
    assert len(q_m.history_since(0.0)) == 2
    assert len(q_m.history_between(0.0, time.time() + 1)) == 2
    assert q_m.clear_history() == 2
    print("history queries: OK")

    # --- P3: p95 nearest-rank ---
    p_m = QuantumEfficiencyMetric()
    for i in range(1, 21):
        p_m.set_task_completion_ratio(0.9)
        p_m.reset()
        p_m.add_quantum_energy(1.0)
        # efficiency = ratio / energy = 0.9; sample many similar values
    # Simpler: inject directly via from_dict
    injection = {
        "history": [
            {"timestamp": float(i), "energy_joules": 1.0,
             "task_completion_ratio": 0.5,
             "efficiency": float(i)}
            for i in range(1, 21)
        ],
        "epsilon": 1e-12, "max_history": 100, "strict": True,
    }
    injected = QuantumEfficiencyMetric.from_dict(injection)
    stats = injected.statistics()
    assert stats["p95"] == 19.0, (
        f"p95={stats['p95']}, expected 19 for n=20, values 1..20"
    )
    print("p95 nearest-rank: OK")

    # --- context manager (compute returns None on empty energy) ---
    with QuantumEfficiencyMetric() as ctx:
        ctx.set_task_completion_ratio(0.5)
        # no energy added → compute returns None on exit
    print("Context manager: OK")

    print("\nSmoke test passed.")
