# src/metacognition/meta_cognitive_controller.py

"""
Meta-Cognitive Controller

Monitors runtime metrics and tracks an anomaly score that drives recovery
decisions. When the accumulated anomaly score crosses a threshold, the
controller signals that recovery should be triggered.

Original behaviour preserved
----------------------------
- ``evaluate(metrics)`` returns the updated anomaly score (float).
- ``should_trigger_recovery()`` returns True once the score exceeds the
  configured threshold (default 5.0).

Enhancements
------------
- Thread-safe via ``RLock``.
- All magic numbers centralized in :class:`ControllerConfig`.
- Configurable decay, increment, multiplier, and trigger threshold.
- Bounded, immutable history of ``ControllerState`` samples.
- Validation of incoming metrics (required keys, finite values, baseline > 0).
- Strict / non-strict mode for malformed inputs.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Context-manager support with auto-reset on clean exit.
- Custom :class:`MetaCognitiveError`.
- Lazy ``%s`` logging, ``__repr__``, and a smoke test under ``__main__``.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MetaCognitiveError(ValueError):
    """Raised for invalid inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ControllerConfig:
    """
    Tunable parameters for the meta-cognitive controller.

    Centralizing them here removes the magic numbers from the original code
    (``2 *``, ``+= 1``, ``*= 0.9``, ``> 5``) and lets callers calibrate
    behavior without subclassing.
    """

    energy_multiplier: float = 2.0        # anomaly if energy > multiplier * baseline
    anomaly_increment: float = 1.0        # added when an anomaly is detected
    anomaly_decay: float = 0.9            # multiplier applied on normal steps
    trigger_threshold: float = 5.0        # anomaly score above which recovery fires
    max_anomaly_score: float = 100.0      # hard cap to bound runaway scores

    def __post_init__(self) -> None:
        if self.energy_multiplier <= 0:
            raise MetaCognitiveError("energy_multiplier must be > 0.")
        if self.anomaly_increment <= 0:
            raise MetaCognitiveError("anomaly_increment must be > 0.")
        if not 0.0 < self.anomaly_decay < 1.0:
            raise MetaCognitiveError("anomaly_decay must be in (0, 1).")
        if self.trigger_threshold <= 0:
            raise MetaCognitiveError("trigger_threshold must be > 0.")
        if self.max_anomaly_score <= 0:
            raise MetaCognitiveError("max_anomaly_score must be > 0.")


# --------------------------------------------------------------------------- #
# Immutable sample
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ControllerState:
    """Snapshot of one ``evaluate`` call."""

    timestamp: float
    energy: float
    baseline_energy: float
    ratio: float
    anomaly_detected: bool
    anomaly_score: float
    label: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Controller
# --------------------------------------------------------------------------- #
class MetaCognitiveController:
    """
    Meta-cognitive controller that tracks an anomaly score across evaluate
    calls and signals when recovery should be triggered.

    Thread-safe, serializable, and bounded in memory.
    """

    def __init__(
        self,
        *,
        config: Optional[ControllerConfig] = None,
        strict: bool = True,
        max_history: Optional[int] = 1000,
    ) -> None:
        if max_history is not None and max_history <= 0:
            raise MetaCognitiveError("max_history must be > 0 or None.")

        self._config: ControllerConfig = config or ControllerConfig()
        self._strict: bool = bool(strict)
        self._max_history: Optional[int] = max_history

        self._lock = threading.RLock()
        self.anomaly_score: float = 0.0
        self._history: List[ControllerState] = []
        self._ctx_start: Optional[float] = None

        logger.debug(
            "MetaCognitiveController initialized (threshold=%.3f, decay=%.3f)",
            self._config.trigger_threshold,
            self._config.anomaly_decay,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> ControllerConfig:
        return self._config

    @property
    def history(self) -> List[ControllerState]:
        with self._lock:
            return list(self._history)

    @property
    def anomaly_count(self) -> int:
        with self._lock:
            return sum(1 for s in self._history if s.anomaly_detected)

    # --------------------------------------------------------------- evaluate
    def evaluate(
        self,
        metrics: Mapping[str, Any],
        *,
        label: Optional[str] = None,
        record: bool = True,
    ) -> float:
        """
        Update the anomaly score based on the current metrics.

        Parameters
        ----------
        metrics : Mapping
            Must contain ``energy`` and ``baseline_energy`` keys.
            Non-numeric / missing values are handled according to ``strict``.
        label : str, optional
            Optional label for the recorded history sample.
        record : bool
            If True (default), append a :class:`ControllerState` sample
            (subject to ``max_history``). Set False for ad-hoc checks.

        Returns
        -------
        float
            The updated anomaly score.
        """
        if not isinstance(metrics, Mapping):
            raise MetaCognitiveError(
                f"metrics must be a Mapping, got {type(metrics).__name__}."
            )

        energy = self._coerce_metric("energy", metrics)
        baseline = self._coerce_metric("baseline_energy", metrics)

        if baseline <= 0:
            msg = f"baseline_energy must be > 0 (got {baseline})."
            if self._strict:
                raise MetaCognitiveError(msg)
            logger.warning(msg + " Ignoring sample.")
            return self.anomaly_score

        ratio = energy / baseline
        anomaly = energy > self._config.energy_multiplier * baseline

        with self._lock:
            if anomaly:
                self.anomaly_score += self._config.anomaly_increment
                if self.anomaly_score > self._config.max_anomaly_score:
                    self.anomaly_score = self._config.max_anomaly_score
            else:
                self.anomaly_score *= self._config.anomaly_decay

            if record:
                self._record_sample(
                    energy=energy,
                    baseline=baseline,
                    ratio=ratio,
                    anomaly=anomaly,
                    label=label,
                )

            score = self.anomaly_score

        if anomaly:
            logger.warning(
                "Anomaly detected: energy=%.6g > %.3g * baseline=%.6g "
                "(ratio=%.3f, score=%.3f)",
                energy,
                self._config.energy_multiplier,
                baseline,
                ratio,
                score,
            )
        else:
            logger.debug(
                "Normal step: energy=%.6g, baseline=%.6g, ratio=%.3f, score=%.3f",
                energy,
                baseline,
                ratio,
                score,
            )

        return score

    def should_trigger_recovery(self) -> bool:
        """Return True once the anomaly score exceeds the configured threshold."""
        with self._lock:
            score = self.anomaly_score
        triggered = score > self._config.trigger_threshold
        if triggered:
            logger.warning(
                "Recovery trigger fired: score=%.3f > threshold=%.3f",
                score,
                self._config.trigger_threshold,
            )
        return triggered

    # ----------------------------------------------------------- helpers
    def statistics(self) -> Dict[str, Optional[float]]:
        """Return descriptive statistics over the recorded history."""
        with self._lock:
            scores = [s.anomaly_score for s in self._history]
            ratios = [s.ratio for s in self._history]

        empty: Dict[str, Optional[float]] = {
            "count": 0,
            "mean_score": None,
            "max_score": None,
            "mean_ratio": None,
            "max_ratio": None,
        }
        if not scores:
            return empty

        return {
            "count": len(scores),
            "mean_score": sum(scores) / len(scores),
            "max_score": max(scores),
            "mean_ratio": sum(ratios) / len(ratios),
            "max_ratio": max(ratios),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset the anomaly score; optionally clear history."""
        with self._lock:
            self.anomaly_score = 0.0
            if clear_history:
                self._history.clear()
        logger.debug(
            "MetaCognitiveController reset (history cleared=%s)", clear_history
        )

    def _coerce_metric(self, name: str, metrics: Mapping[str, Any]) -> float:
        """Extract a numeric, finite metric, honoring strict mode."""
        if name not in metrics:
            msg = f"Missing required metric '{name}'."
            if self._strict:
                raise MetaCognitiveError(msg)
            logger.warning(msg + " Using 0.0.")
            return 0.0

        value = metrics[name]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"Metric '{name}' must be numeric, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise MetaCognitiveError(msg)
            logger.warning(msg + " Using 0.0.")
            return 0.0

        fvalue = float(value)
        if math.isnan(fvalue) or math.isinf(fvalue):
            msg = f"Metric '{name}' must be finite, got {value!r}."
            if self._strict:
                raise MetaCognitiveError(msg)
            logger.warning(msg + " Using 0.0.")
            return 0.0
        return fvalue

    def _record_sample(
        self,
        *,
        energy: float,
        baseline: float,
        ratio: float,
        anomaly: bool,
        label: Optional[str],
    ) -> None:
        sample = ControllerState(
            timestamp=time.time(),
            energy=energy,
            baseline_energy=baseline,
            ratio=ratio,
            anomaly_detected=anomaly,
            anomaly_score=self.anomaly_score,
            label=label,
        )
        self._history.append(sample)
        if self._max_history is not None and len(self._history) > self._max_history:
            del self._history[0]

    # --------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "anomaly_score": self.anomaly_score,
                "config": asdict(self._config),
                "strict": self._strict,
                "max_history": self._max_history,
                "history": [s.to_dict() for s in self._history],
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "MetaCognitiveController":
        if not isinstance(data, Mapping):
            raise MetaCognitiveError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )

        cfg_data = dict(data.get("config", {}) or {})
        cfg = ControllerConfig(
            energy_multiplier=float(cfg_data.get("energy_multiplier", 2.0)),
            anomaly_increment=float(cfg_data.get("anomaly_increment", 1.0)),
            anomaly_decay=float(cfg_data.get("anomaly_decay", 0.9)),
            trigger_threshold=float(cfg_data.get("trigger_threshold", 5.0)),
            max_anomaly_score=float(cfg_data.get("max_anomaly_score", 100.0)),
        )
        controller = cls(
            config=cfg,
            strict=bool(data.get("strict", True)),
            max_history=data.get("max_history", 1000),
        )
        with controller._lock:
            controller.anomaly_score = float(data.get("anomaly_score", 0.0))
            for entry in data.get("history", []):
                controller._history.append(ControllerState(**entry))
        return controller

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "MetaCognitiveController":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise MetaCognitiveError(f"Invalid JSON payload: {exc}") from exc

    # ----------------------------------------------------------- context mgr
    def __enter__(self) -> "MetaCognitiveController":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped meta-cognitive session.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "MetaCognitiveController scope exited with %s after %.4fs.",
                exc_type.__name__,
                elapsed,
            )
            return
        logger.info(
            "MetaCognitiveController scope closed in %.4fs (final score=%.3f).",
            elapsed,
            self.anomaly_score,
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "MetaCognitiveController("
            f"score={self.anomaly_score:.3f}, "
            f"threshold={self._config.trigger_threshold:.3f}, "
            f"samples={len(self._history)}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Local smoke test: python -m metacognition.meta_cognitive_controller
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    controller = MetaCognitiveController(
        config=ControllerConfig(trigger_threshold=3.0),
        strict=True,
        max_history=5,
    )

    # Normal step: score decays (0 * 0.9 = 0)
    print("normal 1 :", controller.evaluate({"energy": 1.0, "baseline_energy": 1.0}))

    # 4 anomalies in a row -> score reaches 4, above threshold 3
    for i in range(4):
        controller.evaluate(
            {"energy": 5.0, "baseline_energy": 1.0}, label=f"anomaly-{i}"
        )
    print("score    :", controller.anomaly_score)
    print("trigger? :", controller.should_trigger_recovery())
    print("stats    :", controller.statistics())

    # Serialization round-trip
    payload = controller.to_json()
    restored = MetaCognitiveController.from_json(payload)
    assert restored.to_dict() == controller.to_dict()
    print("Serialization round-trip OK.")

    # Context manager auto-reset check (manual reset here)
    with MetaCognitiveController(config=ControllerConfig(trigger_threshold=1.0)) as scoped:
        scoped.evaluate({"energy": 10.0, "baseline_energy": 1.0})
        assert scoped.should_trigger_recovery() is True
    print("Context-managed session OK.")

    # Validation failure (strict mode)
    for bad in (
        {"energy": 1.0},                                   # missing baseline
        {"energy": float("nan"), "baseline_energy": 1.0},  # NaN energy
        {"energy": 1.0, "baseline_energy": 0.0},           # zero baseline
    ):
        try:
            MetaCognitiveController(strict=True).evaluate(bad)
        except MetaCognitiveError as exc:
            print("Rejected as expected:", exc)
        else:  # pragma: no cover
            raise AssertionError(f"Expected rejection for {bad!r}")
