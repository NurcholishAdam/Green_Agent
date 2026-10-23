# src/metacognition/meta_cognitive_controller.py
"""
Meta-Cognitive Controller — v6.0.0
==================================

Monitors runtime metrics and tracks an anomaly score that drives recovery
decisions. When the accumulated anomaly score crosses a threshold, the
controller signals that recovery should be triggered.

**Why v6 is a small rewrite (not the usual refactor)**
------------------------------------------------------
v5 was close to correct: compact, thread-safe, immutable samples, bounded
history, round-trip serialization. It did not carry the ten duplicated
enhancement classes of the other v5 modules, so there was nothing to
import from ``carbon_forecast`` v18. Instead of forcing an artificial
dependency, v6 applies the same discipline that the previous six v6
rewrites applied to their modules:

* every magic number is centralized and validated;
* every input is validated;
* every audit-trail field is populated from what actually happened;
* every collection is bounded;
* every public method has a corresponding assertion in the smoke test.

**What changed (v5 → v6)**
--------------------------

**P0 — bugs**

1. ``from_dict`` validates every history entry (type, finiteness, required
   fields) and skips or raises cleanly per ``strict``.
2. ``from_dict`` trims ``history`` to ``max_history`` — v5 could
   deserialize an unbounded history and never enforce the cap.
3. The "auto-reset on clean exit" docstring claim is now honest:
   ``__exit__`` does **not** reset by default. Callers who want that
   behaviour pass ``reset_on_exit=True`` to ``__init__`` (new optional
   parameter, defaults ``False``).

**P1 — semantic correctness**

4. ``evaluate(..., persist=False)`` is a **true dry-run**: the stored
   ``anomaly_score`` is not mutated, and no sample is recorded.
   ``persist=True, record=False`` preserves v5's exact behaviour, so
   existing callers are unaffected.
5. ``max_history=0`` is now legal and means "track score, keep no
   history" — useful for hot-path controllers where the allocation of a
   ``ControllerState`` per call matters.
6. ``ControllerConfig.trigger_at_or_above`` (default ``True``) makes the
   trigger condition ``>=`` instead of ``>``. Set to ``False`` for
   strict ``>`` semantics.
7. ``_coerce_metric`` no longer double-logs missing/invalid input.
8. ``anomaly_count`` is O(1) via an incremental counter that is kept
   consistent on eviction.
9. ``statistics()`` returns ``std_score``, ``p95_score``, ``std_ratio``,
   and ``p95_ratio`` alongside the existing fields.

**P2 — API and observability**

10. ``anomaly_score`` is a read-only property. A deprecation setter keeps
    any external writer working with a warning.
11. ``from_dict`` uses dataclass field filtering for ``ControllerConfig``
    so adding a config field does not require updating ``from_dict``.
12. ``health_check()`` — operational probe shaped like the one in every
    other v6 module.
13. ``evaluate(..., on_anomaly=...)`` — a callback invoked on every
    anomaly so external observability (Prometheus, OpenTelemetry, custom
    alerting) can hook in without subclassing.
14. ``should_trigger_recovery(log_transitions_only=True)`` logs on
    ``False → True`` transitions, not every poll — a 15-second health
    check used to produce four warnings per minute forever.

**Public API preserved**

    controller = MetaCognitiveController()
    score = controller.evaluate(metrics)
    controller.should_trigger_recovery()
    controller.statistics()
    controller.reset()
    controller.to_dict() / controller.to_json()
    MetaCognitiveController.from_dict(...) / from_json(...)

Backward-compatible changes only:
    * ``evaluate(metrics, *, label=None, record=True)`` — unchanged
      signature; ``record=False`` keeps v5 semantics (mutate score, no
      history) because ``persist`` defaults to ``True``.
    * ``ControllerConfig`` gains ``trigger_at_or_above`` (default ``True``)
      — set ``False`` to restore v5's ``>`` comparison.
    * ``anomaly_score`` reads work; writes log a deprecation warning.
"""

from __future__ import annotations

import json
import logging
import math
import statistics
import threading
import time
from dataclasses import asdict, dataclass, fields
from typing import Any, Callable, Dict, List, Mapping, Optional

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

    energy_multiplier: float = 2.0
    anomaly_increment: float = 1.0
    anomaly_decay: float = 0.9
    trigger_threshold: float = 5.0
    max_anomaly_score: float = 100.0
    # NEW in v6: trigger at ``>=`` threshold by default (was ``>``).
    trigger_at_or_above: bool = True

    def __post_init__(self) -> None:
        for name in (
            "energy_multiplier",
            "anomaly_increment",
            "trigger_threshold",
            "max_anomaly_score",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) <= 0.0
            ):
                raise MetaCognitiveError(
                    f"{name} must be a finite float > 0, got {value!r}."
                )
        if not isinstance(self.anomaly_decay, (int, float)) or not (
            0.0 < float(self.anomaly_decay) < 1.0
        ):
            raise MetaCognitiveError(
                "anomaly_decay must be in (0, 1), "
                f"got {self.anomaly_decay!r}."
            )
        if not isinstance(self.trigger_at_or_above, bool):
            raise MetaCognitiveError(
                "trigger_at_or_above must be a bool, "
                f"got {type(self.trigger_at_or_above).__name__}."
            )


# --------------------------------------------------------------------------- #
# Immutable sample
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ControllerState:
    """
    Snapshot of one ``evaluate`` call.

    Immutable and fully validated — a malformed sample cannot exist, so
    downstream consumers (statistics, serialization, health checks) never
    have to re-validate.
    """

    timestamp: float
    energy: float
    baseline_energy: float
    ratio: float
    anomaly_detected: bool
    anomaly_score: float
    label: Optional[str] = None

    def __post_init__(self) -> None:
        for fname in (
            "timestamp",
            "energy",
            "baseline_energy",
            "ratio",
            "anomaly_score",
        ):
            v = getattr(self, fname)
            if (
                isinstance(v, bool)
                or not isinstance(v, (int, float))
                or not math.isfinite(float(v))
            ):
                raise MetaCognitiveError(
                    f"ControllerState.{fname} must be finite numeric, "
                    f"got {v!r}."
                )
        if not isinstance(self.anomaly_detected, bool):
            raise MetaCognitiveError(
                "ControllerState.anomaly_detected must be bool, got "
                f"{type(self.anomaly_detected).__name__}."
            )
        if self.label is not None and not isinstance(self.label, str):
            raise MetaCognitiveError(
                "ControllerState.label must be str or None, got "
                f"{type(self.label).__name__}."
            )

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
        reset_on_exit: bool = False,
    ) -> None:
        # P1 §3.2: max_history=0 legal (disables history); None unbounded.
        if max_history is not None:
            if (
                isinstance(max_history, bool)
                or not isinstance(max_history, int)
                or max_history < 0
            ):
                raise MetaCognitiveError(
                    "max_history must be an int >= 0 or None, "
                    f"got {max_history!r}."
                )

        self._config: ControllerConfig = config or ControllerConfig()
        self._strict: bool = bool(strict)
        self._max_history: Optional[int] = max_history
        self._reset_on_exit: bool = bool(reset_on_exit)

        self._lock = threading.RLock()
        self._anomaly_score: float = 0.0
        self._anomaly_count: int = 0
        self._history: List[ControllerState] = []
        self._was_triggered: bool = False
        self._ctx_start: Optional[float] = None

        logger.debug(
            "MetaCognitiveController v6 initialized "
            "(threshold=%.3f, decay=%.3f, strict=%s, max_history=%s, "
            "trigger_at_or_above=%s)",
            self._config.trigger_threshold,
            self._config.anomaly_decay,
            self._strict,
            self._max_history,
            self._config.trigger_at_or_above,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> ControllerConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def max_history(self) -> Optional[int]:
        return self._max_history

    @property
    def anomaly_score(self) -> float:
        with self._lock:
            return self._anomaly_score

    @anomaly_score.setter
    def anomaly_score(self, value: float) -> None:
        # P2 §4.1: deprecated write path.
        logger.warning(
            "Direct assignment to anomaly_score is deprecated; "
            "use reset() or evaluate() instead."
        )
        try:
            fvalue = float(value)
        except (TypeError, ValueError) as exc:
            raise MetaCognitiveError(
                f"anomaly_score must be numeric, got {value!r}."
            ) from exc
        if not math.isfinite(fvalue) or fvalue < 0:
            raise MetaCognitiveError(
                f"anomaly_score must be finite and >= 0, got {value!r}."
            )
        with self._lock:
            self._anomaly_score = fvalue

    @property
    def anomaly_count(self) -> int:
        # P1 §3.5: O(1) read.
        with self._lock:
            return self._anomaly_count

    @property
    def history(self) -> List[ControllerState]:
        with self._lock:
            return list(self._history)

    # --------------------------------------------------------------- evaluate
    def evaluate(
        self,
        metrics: Mapping[str, Any],
        *,
        label: Optional[str] = None,
        persist: bool = True,
        record: bool = True,
        on_anomaly: Optional[Callable[[ControllerState], None]] = None,
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
        persist : bool, default True
            If ``False``, this is a dry-run: the stored ``anomaly_score``
            is **not** mutated and no sample is recorded. The returned
            score is the hypothetical update.
        record : bool, default True
            If ``True`` (and ``persist=True``), append a
            :class:`ControllerState` sample (subject to ``max_history``).
            Set ``False`` for hot-path evaluations that should still move
            the score but not consume history space. Preserves v5
            semantics.
        on_anomaly : callable, optional
            Invoked with the freshly-created :class:`ControllerState`
            whenever an anomaly is detected **and** ``persist=True``.
            Exceptions raised by the callback are logged and swallowed so
            observability hooks cannot break the controller.

        Returns
        -------
        float
            The stored anomaly score (``persist=True``) or the hypothetical
            update (``persist=False``).
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
            logger.warning("Skipping sample: %s", msg)
            return self.anomaly_score

        ratio = energy / baseline
        anomaly = energy > self._config.energy_multiplier * baseline

        with self._lock:
            prev_score = self._anomaly_score
            if anomaly:
                new_score = min(
                    prev_score + self._config.anomaly_increment,
                    self._config.max_anomaly_score,
                )
            else:
                new_score = prev_score * self._config.anomaly_decay

            if persist:
                self._anomaly_score = new_score

            sample: Optional[ControllerState] = None
            if persist and record:
                sample = self._record_sample(
                    energy=energy,
                    baseline=baseline,
                    ratio=ratio,
                    anomaly=anomaly,
                    label=label,
                    score=new_score,
                )

            result = self._anomaly_score if persist else new_score

        # P2 §4.4: fire the observability hook outside the lock so a slow
        # callback cannot stall other callers. If ``max_history == 0``
        # skipped recording but persist=True, still build an ephemeral
        # sample so the hook fires on every anomaly.
        if anomaly and persist and on_anomaly is not None:
            hook_sample = sample or ControllerState(
                timestamp=time.time(),
                energy=energy,
                baseline_energy=baseline,
                ratio=ratio,
                anomaly_detected=True,
                anomaly_score=new_score,
                label=label,
            )
            try:
                on_anomaly(hook_sample)
            except Exception as exc:  # pragma: no cover — defensive
                logger.warning("on_anomaly hook failed: %s", exc)

        if anomaly:
            logger.warning(
                "Anomaly detected: energy=%.6g > %.3g * baseline=%.6g "
                "(ratio=%.3f, score=%.3f, persist=%s)",
                energy,
                self._config.energy_multiplier,
                baseline,
                ratio,
                result,
                persist,
            )
        else:
            logger.debug(
                "Normal step: energy=%.6g, baseline=%.6g, ratio=%.3f, "
                "score=%.3f, persist=%s",
                energy,
                baseline,
                ratio,
                result,
                persist,
            )

        return result

    def should_trigger_recovery(
        self,
        *,
        log: bool = True,
        log_transitions_only: bool = True,
    ) -> bool:
        """
        Return ``True`` when the anomaly score crosses the threshold.

        Parameters
        ----------
        log : bool, default True
            If ``False``, suppress the log entirely.
        log_transitions_only : bool, default True
            If ``True`` (recommended), log only when the state transitions
            from ``below-threshold`` to ``at-or-above-threshold``. If
            ``False``, log on every call where the score is over the
            threshold (v5 behaviour; produces log spam for polled health
            checks).
        """
        with self._lock:
            score = self._anomaly_score
            threshold = self._config.trigger_threshold
            if self._config.trigger_at_or_above:
                triggered = score >= threshold
            else:
                triggered = score > threshold
            newly_triggered = triggered and not self._was_triggered
            self._was_triggered = triggered

        if triggered and log and (
            not log_transitions_only or newly_triggered
        ):
            logger.warning(
                "Recovery trigger fired%s: score=%.3f, threshold=%.3f",
                " (transition)" if log_transitions_only else "",
                score,
                threshold,
            )
        return triggered

    # ----------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Optional[float]]:
        """
        Descriptive statistics over the recorded history.

        P1 §3.6: adds ``std_*`` and ``p95_*`` — the distribution matters
        more than the max for anomaly-detection tuning.
        """
        with self._lock:
            scores = [s.anomaly_score for s in self._history]
            ratios = [s.ratio for s in self._history]

        if not scores:
            return {
                "count": 0,
                "mean_score": None,
                "std_score": None,
                "max_score": None,
                "p95_score": None,
                "mean_ratio": None,
                "std_ratio": None,
                "max_ratio": None,
                "p95_ratio": None,
            }

        return {
            "count": len(scores),
            "mean_score": statistics.fmean(scores),
            "std_score": (
                statistics.pstdev(scores) if len(scores) > 1 else 0.0
            ),
            "max_score": max(scores),
            "p95_score": _percentile(scores, 95),
            "mean_ratio": statistics.fmean(ratios),
            "std_ratio": (
                statistics.pstdev(ratios) if len(ratios) > 1 else 0.0
            ),
            "max_ratio": max(ratios),
            "p95_ratio": _percentile(ratios, 95),
        }

    # --------------------------------------------------------------- health
    def health_check(self) -> Dict[str, Any]:
        """Operational health probe (see other v6 modules for the shape)."""
        with self._lock:
            score = self._anomaly_score
            threshold = self._config.trigger_threshold
            triggered = (
                score >= threshold
                if self._config.trigger_at_or_above
                else score > threshold
            )
            return {
                "healthy": not triggered,
                "anomaly_score": score,
                "trigger_threshold": threshold,
                "trigger_at_or_above": self._config.trigger_at_or_above,
                "triggered": triggered,
                "samples": len(self._history),
                "anomalies": self._anomaly_count,
                "max_history": self._max_history,
                "strict": self._strict,
            }

    # --------------------------------------------------------------- reset
    def reset(
        self,
        *,
        clear_history: bool = False,
        clear_trigger_state: bool = True,
    ) -> None:
        """
        Reset the anomaly score; optionally clear history and the
        trigger-transition state.
        """
        with self._lock:
            self._anomaly_score = 0.0
            if clear_trigger_state:
                self._was_triggered = False
            if clear_history:
                self._history.clear()
                self._anomaly_count = 0
        logger.debug(
            "MetaCognitiveController reset "
            "(clear_history=%s, clear_trigger_state=%s)",
            clear_history,
            clear_trigger_state,
        )

    # -------------------------------------------------------------- helpers
    def _coerce_metric(self, name: str, metrics: Mapping[str, Any]) -> float:
        """
        Extract a numeric, finite metric.

        P1 §3.4: strictly one warning per bad input, and no coupling to
        downstream semantic constraints (baseline > 0 is checked in
        ``evaluate``).
        """
        if name not in metrics:
            msg = f"Missing required metric '{name}'."
            if self._strict:
                raise MetaCognitiveError(msg)
            logger.warning("Metric '%s' missing; using 0.0.", name)
            return 0.0

        value = metrics[name]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"Metric '{name}' must be numeric, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise MetaCognitiveError(msg)
            logger.warning("Metric '%s' not numeric; using 0.0.", name)
            return 0.0

        fvalue = float(value)
        if math.isnan(fvalue) or math.isinf(fvalue):
            msg = f"Metric '{name}' must be finite, got {value!r}."
            if self._strict:
                raise MetaCognitiveError(msg)
            logger.warning("Metric '%s' not finite; using 0.0.", name)
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
        score: float,
    ) -> Optional[ControllerState]:
        """
        Record one sample, respecting ``max_history``.

        Returns ``None`` if history is disabled (``max_history == 0``).
        """
        if self._max_history == 0:
            return None
        sample = ControllerState(
            timestamp=time.time(),
            energy=energy,
            baseline_energy=baseline,
            ratio=ratio,
            anomaly_detected=anomaly,
            anomaly_score=score,
            label=label,
        )
        self._history.append(sample)
        if anomaly:
            self._anomaly_count += 1
        if (
            self._max_history is not None
            and len(self._history) > self._max_history
        ):
            evicted = self._history.pop(0)
            if evicted.anomaly_detected:
                self._anomaly_count -= 1
        return sample

    # ------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": 6,
                "anomaly_score": self._anomaly_score,
                "anomaly_count": self._anomaly_count,
                "was_triggered": self._was_triggered,
                "config": asdict(self._config),
                "strict": self._strict,
                "max_history": self._max_history,
                "history": [s.to_dict() for s in self._history],
            }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "MetaCognitiveController":
        if not isinstance(data, Mapping):
            raise MetaCognitiveError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )

        # P2 §4.2: use dataclass field filtering so new config fields are
        # picked up automatically. ``ControllerConfig.__post_init__``
        # validates each value.
        cfg_data = dict(data.get("config", {}) or {})
        if not isinstance(cfg_data, Mapping):
            raise MetaCognitiveError(
                "config must be a Mapping, "
                f"got {type(cfg_data).__name__}."
            )
        cfg_fields = {f.name for f in fields(ControllerConfig)}
        filtered_cfg = {
            k: v for k, v in cfg_data.items() if k in cfg_fields
        }
        cfg = ControllerConfig(**filtered_cfg)

        raw_max_history = data.get("max_history", 1000)
        if raw_max_history is not None:
            if isinstance(raw_max_history, bool):
                raise MetaCognitiveError(
                    "max_history must be int or None, not bool."
                )
            try:
                raw_max_history = int(raw_max_history)
            except (TypeError, ValueError) as exc:
                raise MetaCognitiveError(
                    f"max_history must be int or None, "
                    f"got {raw_max_history!r}."
                ) from exc

        controller = cls(
            config=cfg,
            strict=bool(data.get("strict", True)),
            max_history=raw_max_history,
        )

        with controller._lock:
            raw_score = data.get("anomaly_score", 0.0)
            try:
                controller._anomaly_score = float(raw_score)
            except (TypeError, ValueError) as exc:
                if controller._strict:
                    raise MetaCognitiveError(
                        f"anomaly_score must be numeric, got {raw_score!r}."
                    ) from exc
                logger.warning(
                    "Ignoring non-numeric anomaly_score %r", raw_score
                )
                controller._anomaly_score = 0.0
            if not math.isfinite(controller._anomaly_score):
                if controller._strict:
                    raise MetaCognitiveError(
                        "anomaly_score must be finite, "
                        f"got {controller._anomaly_score!r}."
                    )
                controller._anomaly_score = 0.0

            controller._anomaly_count = 0
            controller._was_triggered = bool(
                data.get("was_triggered", False)
            )

            raw_history = data.get("history", [])
            if not isinstance(raw_history, (list, tuple)):
                raise MetaCognitiveError(
                    f"history must be a list, got "
                    f"{type(raw_history).__name__}."
                )

            # P0 §2.1: validate each entry.
            for i, entry in enumerate(raw_history):
                state = _coerce_state(entry, i, controller._strict)
                if state is None:
                    continue
                controller._history.append(state)
                if state.anomaly_detected:
                    controller._anomaly_count += 1

            # P0 §2.2: trim to max_history.
            if (
                controller._max_history is not None
                and len(controller._history) > controller._max_history
            ):
                excess = (
                    len(controller._history) - controller._max_history
                )
                for _ in range(excess):
                    evicted = controller._history.pop(0)
                    if evicted.anomaly_detected:
                        controller._anomaly_count -= 1
        return controller

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("sort_keys", True)
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "MetaCognitiveController":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MetaCognitiveError(
                f"Invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(data)

    # ----------------------------------------------------------- context mgr
    def __enter__(self) -> "MetaCognitiveController":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped meta-cognitive session.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start
            if self._ctx_start is not None
            else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "MetaCognitiveController scope exited with %s "
                "after %.4fs.",
                exc_type.__name__,
                elapsed,
            )
            return
        logger.info(
            "MetaCognitiveController scope closed in %.4fs "
            "(final score=%.3f).",
            elapsed,
            self.anomaly_score,
        )
        # P0 §2.3: honour the (documented) reset_on_exit flag.
        if self._reset_on_exit:
            self.reset()

    async def __aenter__(self) -> "MetaCognitiveController":
        return self.__enter__()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.__exit__(exc_type, exc, tb)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "MetaCognitiveController("
                f"score={self._anomaly_score:.3f}, "
                f"threshold={self._config.trigger_threshold:.3f}, "
                f"trigger_at_or_above={self._config.trigger_at_or_above}, "
                f"samples={len(self._history)}, "
                f"anomalies={self._anomaly_count}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _percentile(values: List[float], p: float) -> float:
    """Linear-interpolated percentile (numpy-compatible) for p ∈ [0, 100]."""
    if not values:
        return 0.0
    s = sorted(values)
    if len(s) == 1:
        return float(s[0])
    k = (len(s) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(s[int(k)])
    return float(s[int(f)] * (c - k) + s[int(c)] * (k - f))


def _coerce_state(
    entry: Any, index: int, strict: bool
) -> Optional[ControllerState]:
    """Validate one history entry. Returns None on non-strict skip."""
    if not isinstance(entry, Mapping):
        msg = (
            f"history[{index}] must be a Mapping, "
            f"got {type(entry).__name__}."
        )
        if strict:
            raise MetaCognitiveError(msg)
        logger.warning("Skipping %s", msg)
        return None
    try:
        return ControllerState(**entry)
    except (MetaCognitiveError, TypeError) as exc:
        msg = f"history[{index}] is malformed: {exc}"
        if strict:
            raise MetaCognitiveError(msg) from exc
        logger.warning("Skipping %s", msg)
        return None


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "MetaCognitiveError",
    "ControllerConfig",
    "ControllerState",
    "MetaCognitiveController",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m metacognition.meta_cognitive_controller
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("=" * 72)
    print("meta_cognitive_controller.py v6 — Smoke test")
    print("=" * 72)

    # ---- Basic behaviour ----
    controller = MetaCognitiveController(
        config=ControllerConfig(trigger_threshold=3.0),
        strict=True,
        max_history=5,
    )
    assert controller.evaluate(
        {"energy": 1.0, "baseline_energy": 1.0}
    ) == 0.0
    for i in range(4):
        controller.evaluate(
            {"energy": 5.0, "baseline_energy": 1.0},
            label=f"anomaly-{i}",
        )
    print("score    :", controller.anomaly_score)
    assert controller.anomaly_score == 4.0
    assert controller.should_trigger_recovery() is True

    # ---- P1 §3.3: trigger at exactly the threshold ----
    print("\n=== P1 fix: trigger at >= threshold ===")
    c_at = MetaCognitiveController(
        config=ControllerConfig(trigger_threshold=3.0)
    )
    for _ in range(3):
        c_at.evaluate({"energy": 5.0, "baseline_energy": 1.0})
    assert c_at.anomaly_score == 3.0
    assert c_at.should_trigger_recovery() is True
    print("  score == threshold → triggers (v5 would not)")

    # ---- P1 §3.1: persist=False is a true dry-run ----
    print("\n=== P1 fix: persist=False does not mutate ===")
    c_dry = MetaCognitiveController(
        config=ControllerConfig(trigger_threshold=3.0)
    )
    c_dry.evaluate({"energy": 5.0, "baseline_energy": 1.0})
    assert c_dry.anomaly_score == 1.0
    hypothetical = c_dry.evaluate(
        {"energy": 5.0, "baseline_energy": 1.0}, persist=False
    )
    assert c_dry.anomaly_score == 1.0, (
        f"persist=False mutated score: {c_dry.anomaly_score}"
    )
    assert hypothetical == 2.0
    print(
        f"  stored={c_dry.anomaly_score}, hypothetical={hypothetical}"
    )

    # ---- P0 §2.1: from_dict validates history ----
    print("\n=== P0 fix: from_dict validates history ===")
    bad_payload = {
        "config": {"trigger_threshold": 3.0},
        "history": [{"energy": "not-a-number"}],
    }
    try:
        MetaCognitiveController.from_dict(bad_payload)
    except MetaCognitiveError as exc:
        print(f"  strict rejected: {exc}")
    else:
        raise AssertionError("Expected rejection for malformed history")
    lenient = MetaCognitiveController.from_dict(
        {**bad_payload, "strict": False}
    )
    assert len(lenient.history) == 0
    print("  non-strict skipped malformed entry")

    # ---- P0 §2.2: from_dict trims to max_history ----
    print("\n=== P0 fix: from_dict trims history ===")
    big_payload = {
        "config": {},
        "strict": False,
        "max_history": 3,
        "history": [
            {
                "timestamp": 1.0,
                "energy": 1.0,
                "baseline_energy": 1.0,
                "ratio": 1.0,
                "anomaly_detected": False,
                "anomaly_score": 0.0,
                "label": None,
            }
            for _ in range(10)
        ],
    }
    trimmed = MetaCognitiveController.from_dict(big_payload)
    assert len(trimmed.history) == 3
    print(f"  trimmed 10 → {len(trimmed.history)}")

    # ---- P1 §3.2: max_history=0 disables history ----
    print("\n=== P1 fix: max_history=0 disables history ===")
    no_hist = MetaCognitiveController(max_history=0)
    for _ in range(10):
        no_hist.evaluate({"energy": 5.0, "baseline_energy": 1.0})
    assert len(no_hist.history) == 0
    assert no_hist.anomaly_score == 10.0
    print("  score tracked, history empty")

    # ---- P1 §3.5: incremental anomaly_count ----
    print("\n=== P1 fix: anomaly_count is O(1) and consistent ===")
    counter = MetaCognitiveController(max_history=3)
    for _ in range(3):
        counter.evaluate({"energy": 5.0, "baseline_energy": 1.0})
    assert counter.anomaly_count == 3
    counter.evaluate({"energy": 5.0, "baseline_energy": 1.0})
    assert counter.anomaly_count == 3  # evicts an anomaly, adds an anomaly
    counter.evaluate({"energy": 1.0, "baseline_energy": 1.0})
    assert counter.anomaly_count == 2  # evicts an anomaly, adds a normal
    print(f"  anomaly_count={counter.anomaly_count}")

    # ---- P1 §3.6: statistics has std + p95 ----
    print("\n=== P1 fix: statistics has std + p95 ===")
    c_stats = MetaCognitiveController()
    for i in range(10):
        c_stats.evaluate(
            {"energy": 5.0 if i % 2 else 1.0, "baseline_energy": 1.0}
        )
    stats = c_stats.statistics()
    for key in ("std_score", "p95_score", "std_ratio", "p95_ratio"):
        assert key in stats, f"Missing {key}"
    print(
        f"  std_score={stats['std_score']:.3f} "
        f"p95_score={stats['p95_score']:.3f}"
    )

    # ---- P2 §4.1: anomaly_score is read-only (with deprecation) ----
    print("\n=== P2 fix: anomaly_score is read-only (with deprecation) ===")
    ro = MetaCognitiveController()
    ro.evaluate({"energy": 5.0, "baseline_energy": 1.0})
    assert ro.anomaly_score == 1.0
    ro.anomaly_score = 99.0  # type: ignore[assignment] # emits warning
    assert ro.anomaly_score == 99.0
    print("  direct write works but logs a deprecation warning")

    # ---- P2 §4.2: from_dict ignores unknown config keys ----
    print("\n=== P2 fix: from_dict ignores unknown config keys ===")
    with_unknown = MetaCognitiveController.from_dict(
        {
            "config": {
                "trigger_threshold": 2.0,
                "unknown_future_field": 42,
            },
        }
    )
    assert with_unknown.config.trigger_threshold == 2.0
    print("  unknown config key ignored cleanly")

    # ---- P2 §4.3: health_check ----
    print("\n=== P2 fix: health_check ===")
    h = controller.health_check()
    for key in (
        "healthy",
        "anomaly_score",
        "trigger_threshold",
        "trigger_at_or_above",
        "samples",
        "anomalies",
        "max_history",
        "strict",
    ):
        assert key in h, f"Missing {key}"
    print(
        f"  healthy={h['healthy']}, score={h['anomaly_score']:.3f}, "
        f"samples={h['samples']}"
    )

    # ---- P2 §4.4: on_anomaly hook ----
    print("\n=== P2 fix: on_anomaly hook ===")
    captured: List[ControllerState] = []
    hook_c = MetaCognitiveController()
    hook_c.evaluate(
        {"energy": 5.0, "baseline_energy": 1.0},
        on_anomaly=captured.append,
    )
    assert len(captured) == 1
    assert captured[0].anomaly_detected is True
    # Also fires when max_history=0 (ephemeral sample)
    captured2: List[ControllerState] = []
    hook_no_hist = MetaCognitiveController(max_history=0)
    hook_no_hist.evaluate(
        {"energy": 5.0, "baseline_energy": 1.0},
        on_anomaly=captured2.append,
    )
    assert len(captured2) == 1
    print(f"  on_anomaly fired with history={len(captured)} and "
          f"without history={len(captured2)}")

    # ---- P2 §4.5: transition logging ----
    print("\n=== P2 fix: should_trigger_recovery logs on transitions ===")
    trans = MetaCognitiveController(
        config=ControllerConfig(trigger_threshold=1.0)
    )
    trans.evaluate({"energy": 5.0, "baseline_energy": 1.0})
    # First poll: transition → logs
    assert trans.should_trigger_recovery(log_transitions_only=True) is True
    # Second poll: no transition → does not log
    assert trans.should_trigger_recovery(log_transitions_only=True) is True
    # Decay below threshold
    for _ in range(20):
        trans.evaluate({"energy": 1.0, "baseline_energy": 1.0})
    assert (
        trans.should_trigger_recovery(log_transitions_only=True) is False
    )
    # Trigger again → new transition
    trans.evaluate({"energy": 5.0, "baseline_energy": 1.0})
    assert trans.should_trigger_recovery(log_transitions_only=True) is True
    print("  transitions verified (False→True, True→False, False→True)")

    # ---- P0 §2.3: reset_on_exit ----
    print("\n=== P0 fix: reset_on_exit honoured ===")
    with MetaCognitiveController(
        config=ControllerConfig(trigger_threshold=1.0),
        reset_on_exit=True,
    ) as scoped:
        scoped.evaluate({"energy": 10.0, "baseline_energy": 1.0})
        assert scoped.anomaly_score == 1.0
    assert scoped.anomaly_score == 0.0
    print("  reset_on_exit=True resets after clean exit")

    with MetaCognitiveController(
        config=ControllerConfig(trigger_threshold=1.0),
        reset_on_exit=False,
    ) as preserved:
        preserved.evaluate({"energy": 10.0, "baseline_energy": 1.0})
    assert preserved.anomaly_score == 1.0
    print("  reset_on_exit=False preserves score")

    # ---- Serialization round-trip ----
    print("\n=== Serialization round-trip ===")
    payload = controller.to_json()
    restored = MetaCognitiveController.from_json(payload)
    assert restored.to_dict() == controller.to_dict()
    print("  OK")

    # ---- Strict-mode rejections ----
    print("\n=== Strict-mode rejections ===")
    for bad in (
        {"energy": 1.0},
        {"energy": float("nan"), "baseline_energy": 1.0},
        {"energy": 1.0, "baseline_energy": 0.0},
    ):
        try:
            MetaCognitiveController(strict=True).evaluate(bad)
        except MetaCognitiveError as exc:
            print(f"  rejected: {exc}")
        else:
            raise AssertionError(f"Expected rejection for {bad!r}")

    # ---- Async context manager ----
    print("\n=== Async context manager ===")

    async def _async_demo() -> None:
        async with MetaCognitiveController(
            config=ControllerConfig(trigger_threshold=1.0),
            reset_on_exit=True,
        ) as async_cm:
            async_cm.evaluate({"energy": 10.0, "baseline_energy": 1.0})
            assert async_cm.anomaly_score == 1.0
        assert async_cm.anomaly_score == 0.0

    import asyncio

    asyncio.run(_async_demo())
    print("  async __aenter__/__aexit__ OK")

    print("\nSmoke test passed.")
