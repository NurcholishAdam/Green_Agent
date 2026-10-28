# src/feedback/rlhf_engine.py

"""
RLHF Feedback Engine — v2.0.0
==============================

Main feedback engine combining all components:
- :class:`ReasoningTraceAnalyzer`
- :class:`SustainabilityImprovementSuggester`
- :func:`analyze_energy_metrics`
- Optional :class:`MetricSink` for emission

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``RLHFConfig`` rejects ``NaN`` / ``Inf`` weights. Previously
        ``weight_reasoning=float("nan")`` silently produced
        ``composite=1.0`` on every evaluation (``min(1.0, nan) == 1.0``).
  * 2.2 ``RLHFConfig`` rejects ``bool`` as a weight. ``True`` was silently
        coerced to ``1.0``.
  * 2.3 ``RLHFConfig`` rejects ``NaN`` / ``Inf`` for reference values
        (``energy_reference_kwh``, ``carbon_reference_kg``,
        ``latency_ceiling_seconds``). Previously ``float("nan")`` passed
        the ``<= 0`` check and made every ``_score_inverse`` return 1.0.
  * 2.4 ``_safe_float`` rejects negatives. Previously
        ``{"energy_kwh": -5.0}`` produced ``energy_score=1.0`` (because
        ``min(1.0, 1.0 + 2000) == 1.0``) — a perfect score for garbage.
  * 2.5 Empty / partial results are no longer scored as ``composite=0.6``.
        ``RLHFScore`` gains ``has_data`` / ``evaluated`` / ``coerced``;
        missing metrics contribute 0.0 (no evidence) instead of a
        fabricated perfect score. ``generate_feedback({})`` returns
        ``composite=0.0, has_data=False``.
  * 2.6 ``RLHFScore.__post_init__`` validates every field
        (numeric, finite, in [0, 1]).
  * 2.7 ``RLHFRecord.__post_init__`` + ``RLHFRecord.from_dict`` —
        both were previously missing (so the record could be constructed
        with ``suggestions_count=-1``, ``task_id=42``, or a score-less
        payload).
  * 2.8 ``RLHFScore.from_dict`` raises ``RLHFEngineError`` on invalid
        input (previously raw ``KeyError`` / ``TypeError`` / ``ValueError``).
  * 2.9 ``from_dict`` restores ``reasoning_config``, ``suggester_config``,
        and ``energy_config`` (previously only ``config`` — the other
        three were silently reset to defaults, even though ``to_dict``
        serialized them).
  * 2.10 ``from_dict`` delegates to ``RLHFConfig.from_legacy_dict`` so
         new config fields default correctly.

**P1 robustness / observability**
  * 3.1 ``latency_ms`` is passed to the suggester — the ``latency``
        suggestion category (v2.0.0 suggester) is now reachable.
  * 3.2 ``trace`` fallback (``result["trace"]`` when ``reasoning_trace``
        is absent) is logged at DEBUG so callers can see which key was
        used.
  * 3.3 Negative / extreme inputs are logged at WARNING.
  * 3.4 ``emit`` failure wraps as ``RLHFEngineError`` in strict mode
        (previously re-raised the raw exception).
  * 3.5 ``sink`` validated at construction (must expose callable
        ``.emit``).
  * 3.6 ``metrics_hook(event, fields)`` fires ``feedback_generated`` and
        ``sink_emit_failed`` events.
  * 3.7 ``statistics(window=N)`` for a consistent windowed view.
  * 3.8 ``close()`` (delegates to ``sink.close()``), ``__version__``,
        locked sync/async context managers.
  * 3.9 ``__getstate__`` / ``__setstate__`` — the engine is now picklable.
  * 3.10 ``from_dict`` warns on history truncation.
  * 3.11 Latency unit contract documented (``latency_ms`` = ms,
         ``latency`` = seconds).
  * 3.14 ``memory`` passed to the energy feedback only when present in
         ``result`` (so ``generate_feedback({})`` gets
         "No metrics available for evaluation" from the v2.0.0
         ``EnergyFeedback``).

**P2 polish**
  * ``__repr__`` includes ``last_composite`` when history is non-empty.
  * ``comparative_analysis`` reports ``latency_ms`` (was seconds).

Backward compatible: every v1.0.0 method signature and payload works
unchanged. ``RLHFScore`` and ``RLHFRecord`` gain new fields with
defaults that preserve the v1.0.0 shape for callers who construct them
directly.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field, fields
from typing import (
    Any, Callable, Deque, Dict, List, Mapping, Optional, Tuple,
)

from .energy_feedback import (
    EnergyFeedbackConfig,
    analyze_energy_metrics,
)
from .improvement_suggester import (
    SuggesterConfig,
    SustainabilityImprovementSuggester,
)
from .metric_sink import MetricSink, StdoutSink
from .reasoning_analyzer import (
    ReasoningConfig,
    ReasoningTraceAnalyzer,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Config restoration helper (P0 2.9 / 2.10)
# --------------------------------------------------------------------------- #
def _restore_config(config_cls: type, data: Optional[Mapping[str, Any]]) -> Any:
    """
    Restore a config dataclass from a serialized dict.

    Prefers ``from_legacy_dict`` (v2.0.0 module API); falls back to
    ``fields()`` introspection for v1.0.0 modules without it.
    """
    data = dict(data or {})
    from_legacy = getattr(config_cls, "from_legacy_dict", None)
    if callable(from_legacy):
        return from_legacy(data)
    try:
        valid = {f.name for f in fields(config_cls)}
        kwargs = {k: v for k, v in data.items() if k in valid}
        # Coerce list-typed fields (e.g. keyword tuples).
        for name in ("energy_keywords", "coherence_keywords"):
            if name in kwargs and isinstance(kwargs[name], list):
                kwargs[name] = tuple(kwargs[name])
        return config_cls(**kwargs)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Could not restore %s from payload (%s); using defaults.",
            config_cls.__name__, exc)
        return config_cls()


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class RLHFEngineError(ValueError):
    """Raised for invalid inputs, configuration, or feedback failures."""


# --------------------------------------------------------------------------- #
# Configuration (P0 2.1, 2.2, 2.3, 2.10)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RLHFConfig:
    """Tunable parameters for the RLHF feedback engine."""

    weight_reasoning: float = 0.4
    weight_energy: float = 0.3
    weight_carbon: float = 0.2
    weight_latency: float = 0.1

    energy_reference_kwh: float = 0.0025
    carbon_reference_kg: float = 0.001
    latency_ceiling_seconds: float = 5.0

    max_history: int = 1000

    def __post_init__(self) -> None:
        # P0 2.1 / 2.2 / 2.3: type + finite + range checks.
        for name in (
            "weight_reasoning", "weight_energy",
            "weight_carbon", "weight_latency",
            "energy_reference_kwh", "carbon_reference_kg",
            "latency_ceiling_seconds",
        ):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise RLHFEngineError(
                    f"{name} must be numeric, "
                    f"got {type(v).__name__}.")
            if math.isnan(v) or math.isinf(v):
                raise RLHFEngineError(
                    f"{name} must be finite, got {v!r}.")

        for name in (
            "weight_reasoning", "weight_energy",
            "weight_carbon", "weight_latency",
        ):
            if getattr(self, name) < 0:
                raise RLHFEngineError(f"{name} must be >= 0.")

        if self.energy_reference_kwh <= 0:
            raise RLHFEngineError(
                "energy_reference_kwh must be > 0.")
        if self.carbon_reference_kg <= 0:
            raise RLHFEngineError(
                "carbon_reference_kg must be > 0.")
        if self.latency_ceiling_seconds <= 0:
            raise RLHFEngineError(
                "latency_ceiling_seconds must be > 0.")

        if isinstance(self.max_history, bool) or \
                not isinstance(self.max_history, int) or \
                self.max_history <= 0:
            raise RLHFEngineError("max_history must be > 0.")

        total = (
            self.weight_reasoning + self.weight_energy
            + self.weight_carbon + self.weight_latency
        )
        if abs(total - 1.0) > 1e-6:
            raise RLHFEngineError(
                f"RLHF weights must sum to 1.0 "
                f"(got {total:.6f}).")

    @classmethod
    def from_legacy_dict(
        cls, cfg: Optional[Mapping[str, Any]]
    ) -> "RLHFConfig":
        """P0 2.10: build from a flat mapping; unknown keys ignored."""
        cfg = dict(cfg or {})
        valid = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in cfg.items() if k in valid})


# --------------------------------------------------------------------------- #
# Score & record (P0 2.5, 2.6, 2.7, 2.8)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RLHFScore:
    """Immutable composite RLHF score."""

    composite: float
    reasoning: float
    energy: float
    carbon: float
    latency: float

    # v2.0.0 additions (P0 2.5): distinguishes "0.0 because bad" from
    # "0.0 because we couldn't measure anything".
    has_data: bool = False
    evaluated: Tuple[str, ...] = ()
    coerced: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # P0 2.6: every score field is numeric, finite, in [0, 1].
        for name in (
            "composite", "reasoning", "energy", "carbon", "latency",
        ):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise RLHFEngineError(
                    f"{name} must be numeric, "
                    f"got {type(v).__name__}.")
            if math.isnan(v) or math.isinf(v):
                raise RLHFEngineError(
                    f"{name} must be finite, got {v!r}.")
            if not 0.0 <= v <= 1.0:
                raise RLHFEngineError(
                    f"{name} must be in [0, 1], got {v}.")

        if not isinstance(self.has_data, bool):
            raise RLHFEngineError(
                "has_data must be a bool.")

        for name in ("evaluated", "coerced"):
            val = getattr(self, name)
            if not isinstance(val, tuple):
                raise RLHFEngineError(
                    f"{name} must be a tuple, "
                    f"got {type(val).__name__}.")
            for i, item in enumerate(val):
                if not isinstance(item, str):
                    raise RLHFEngineError(
                        f"{name}[{i}] must be a string.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "composite": self.composite,
            "reasoning": self.reasoning,
            "energy": self.energy,
            "carbon": self.carbon,
            "latency": self.latency,
            "has_data": self.has_data,
            "evaluated": list(self.evaluated),
            "coerced": list(self.coerced),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RLHFScore":
        """P0 2.8: robust deserialization with ``RLHFEngineError``."""
        if not isinstance(data, Mapping):
            raise RLHFEngineError(
                f"RLHFScore.from_dict expects a Mapping, "
                f"got {type(data).__name__}.")

        def _f(key: str, default: Optional[float] = None) -> float:
            if key not in data:
                if default is None:
                    raise RLHFEngineError(
                        f"RLHFScore.from_dict: missing {key!r}.")
                return default
            v = data[key]
            if isinstance(v, bool) or \
                    not isinstance(v, (int, float)):
                raise RLHFEngineError(
                    f"RLHFScore.from_dict: {key} must be numeric, "
                    f"got {type(v).__name__}.")
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                raise RLHFEngineError(
                    f"RLHFScore.from_dict: {key} must be finite.")
            return f

        def _tuple(key: str) -> Tuple[str, ...]:
            raw = data.get(key, ())
            if not isinstance(raw, (list, tuple)):
                raise RLHFEngineError(
                    f"RLHFScore.from_dict: {key} must be a list or "
                    f"tuple, got {type(raw).__name__}.")
            for i, item in enumerate(raw):
                if not isinstance(item, str):
                    raise RLHFEngineError(
                        f"RLHFScore.from_dict: {key}[{i}] must be "
                        f"a string.")
            return tuple(raw)

        return cls(
            composite=_f("composite"),
            reasoning=_f("reasoning", 0.0),
            energy=_f("energy", 0.0),
            carbon=_f("carbon", 0.0),
            latency=_f("latency", 0.0),
            has_data=bool(data.get("has_data", False)),
            evaluated=_tuple("evaluated"),
            coerced=_tuple("coerced"),
        )

    def __repr__(self) -> str:
        return (
            "RLHFScore("
            f"composite={self.composite:.3f}, "
            f"reasoning={self.reasoning:.3f}, energy={self.energy:.3f}, "
            f"carbon={self.carbon:.3f}, latency={self.latency:.3f}, "
            f"has_data={self.has_data})"
        )


@dataclass(frozen=True)
class RLHFRecord:
    """Immutable record of one RLHF feedback evaluation."""

    task_id: Optional[str]
    region: Optional[str]
    score: RLHFScore
    suggestions_count: int
    timestamp: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        # P0 2.7: validate every field.
        if self.task_id is not None and (
            not isinstance(self.task_id, str) or not self.task_id
        ):
            raise RLHFEngineError(
                "task_id must be a non-empty string or None, "
                f"got {self.task_id!r}.")
        if self.region is not None and (
            not isinstance(self.region, str) or not self.region
        ):
            raise RLHFEngineError(
                "region must be a non-empty string or None, "
                f"got {self.region!r}.")
        if not isinstance(self.score, RLHFScore):
            raise RLHFEngineError(
                f"score must be an RLHFScore, "
                f"got {type(self.score).__name__}.")
        if isinstance(self.suggestions_count, bool) or \
                not isinstance(self.suggestions_count, int) or \
                self.suggestions_count < 0:
            raise RLHFEngineError(
                "suggestions_count must be a non-negative int, "
                f"got {self.suggestions_count!r}.")
        if isinstance(self.timestamp, bool) or \
                not isinstance(self.timestamp, (int, float)):
            raise RLHFEngineError(
                "timestamp must be numeric, "
                f"got {type(self.timestamp).__name__}.")
        if math.isnan(self.timestamp) or \
                math.isinf(self.timestamp):
            raise RLHFEngineError("timestamp must be finite.")
        if self.timestamp < 0:
            raise RLHFEngineError("timestamp must be >= 0.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "region": self.region,
            "score": self.score.to_dict(),
            "suggestions_count": self.suggestions_count,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RLHFRecord":
        """P0 2.7: symmetric with ``to_dict``."""
        if not isinstance(data, Mapping):
            raise RLHFEngineError(
                f"RLHFRecord.from_dict expects a Mapping, "
                f"got {type(data).__name__}.")
        if "score" not in data:
            raise RLHFEngineError(
                "RLHFRecord.from_dict: missing 'score'.")
        if not isinstance(data["score"], Mapping):
            raise RLHFEngineError(
                "RLHFRecord.from_dict: 'score' must be a Mapping.")

        ts_raw = data.get("timestamp", time.time())
        if isinstance(ts_raw, bool) or \
                not isinstance(ts_raw, (int, float)):
            raise RLHFEngineError(
                "RLHFRecord.from_dict: timestamp must be numeric.")
        ts = float(ts_raw)
        if math.isnan(ts) or math.isinf(ts):
            raise RLHFEngineError(
                "RLHFRecord.from_dict: timestamp must be finite.")

        count_raw = data.get("suggestions_count", 0)
        if isinstance(count_raw, bool) or \
                not isinstance(count_raw, int):
            raise RLHFEngineError(
                "RLHFRecord.from_dict: suggestions_count must be "
                "an int.")

        return cls(
            task_id=data.get("task_id"),
            region=data.get("region"),
            score=RLHFScore.from_dict(data["score"]),
            suggestions_count=count_raw,
            timestamp=ts,
        )

    def __repr__(self) -> str:
        return (
            "RLHFRecord("
            f"task_id={self.task_id!r}, region={self.region!r}, "
            f"score={self.score!r}, "
            f"suggestions_count={self.suggestions_count})"
        )


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #
class RLHFFeedbackEngine:
    """
    Main feedback engine combining all components.

    Thread-safe, serializable, picklable, bounded in memory.
    """

    def __init__(
        self,
        *,
        config: Optional[RLHFConfig] = None,
        reasoning_config: Optional[ReasoningConfig] = None,
        suggester_config: Optional[SuggesterConfig] = None,
        energy_config: Optional[EnergyFeedbackConfig] = None,
        sink: Optional[MetricSink] = None,
        strict: bool = True,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> None:
        # P1 3.5: validate sink at construction.
        if sink is not None and not callable(
            getattr(sink, "emit", None)
        ):
            raise RLHFEngineError(
                "sink must expose a callable .emit method, "
                f"got {type(sink).__name__}.")
        if metrics_hook is not None and not callable(metrics_hook):
            raise RLHFEngineError("metrics_hook must be callable.")

        self._config = config or RLHFConfig()
        self._strict = bool(strict)
        self._metrics_hook = metrics_hook

        self.reasoning_analyzer = ReasoningTraceAnalyzer(
            config=reasoning_config, strict=strict)
        self.suggester = SustainabilityImprovementSuggester(
            config=suggester_config, strict=strict)
        self._energy_config = energy_config or EnergyFeedbackConfig()
        self.sink = sink

        self._lock = threading.RLock()
        self._history: Deque[RLHFRecord] = deque(
            maxlen=self._config.max_history)
        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None

        logger.debug(
            "RLHFFeedbackEngine v%s initialized "
            "(sink=%s, strict=%s)",
            __version__,
            type(self.sink).__name__ if self.sink else None,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> RLHFConfig:
        return self._config

    @property
    def history(self) -> List[RLHFRecord]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- metrics
    def _emit_metric(
        self, event: str, fields_: Mapping[str, Any],
    ) -> None:
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields_))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised", exc_info=True)

    # ---------------------------------------------------------- helpers
    @staticmethod
    def _extract_metric(
        mapping: Mapping[str, Any], key: str, default: float,
    ) -> Tuple[float, bool]:
        """
        Return ``(value, was_valid)`` (P0 2.4).

        ``was_valid`` is True only when ``key`` is present, numeric (not
        bool), finite, and non-negative. Otherwise ``(default, False)``.
        """
        if key not in mapping:
            return default, False
        value = mapping[key]
        if isinstance(value, bool) or \
                not isinstance(value, (int, float)):
            return default, False
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default, False
        if f < 0:
            # P1 3.3: log the coercion so callers see the sign-flip.
            logger.warning(
                "Negative %r (%r) coerced to default %.4f.",
                key, value, default)
            return default, False
        return f, True

    @staticmethod
    def _safe_float(
        mapping: Mapping[str, Any],
        key: str,
        default: Optional[float],
    ) -> Optional[float]:
        """
        Backward-compatible float extraction (P0 2.4).

        Returns ``None`` only when both ``default`` is ``None`` AND the
        value is invalid. Otherwise returns ``default`` for invalid input.
        Now rejects negatives (previously accepted).
        """
        value = mapping.get(key, default)
        if value is None:
            return None
        if isinstance(value, bool) or \
                not isinstance(value, (int, float)):
            return default
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default
        if f < 0:  # P0 2.4
            return default
        return f

    @staticmethod
    def _score_inverse(value: float, reference: float) -> float:
        """``max(0, min(1, 1 - value/reference))`` for 'lower is better'."""
        if reference <= 0:
            return 0.0
        # Defensive: negative values clamp to 0 (already excluded by
        # _extract_metric, but belt-and-suspenders).
        if value < 0:
            value = 0.0
        return max(0.0, min(1.0, 1.0 - value / reference))

    @staticmethod
    def _extract_reasoning_trace(result: Mapping[str, Any]) -> Any:
        """P1 3.2: extract the reasoning trace with an explicit key."""
        if "reasoning_trace" in result:
            logger.debug("Using 'reasoning_trace' as trace.")
            return result["reasoning_trace"]
        if "trace" in result:
            logger.debug(
                "Using 'trace' as reasoning trace (fallback).")
            return result["trace"]
        return []

    # ---------------------------------------------------------- public API
    def generate_feedback(
        self,
        result: Mapping[str, Any],
        task: Optional[Mapping[str, Any]] = None,
        *,
        emit: bool = False,
    ) -> Dict[str, Any]:
        """Generate comprehensive RLHF feedback."""
        if not isinstance(result, Mapping):
            raise RLHFEngineError(
                f"result must be a Mapping, "
                f"got {type(result).__name__}.")
        if task is not None and not isinstance(task, Mapping):
            raise RLHFEngineError(
                f"task must be a Mapping or None, "
                f"got {type(task).__name__}.")
        task = task or {}

        # ---- Extract metrics (P0 2.4: track validation) ------------ #
        energy, energy_valid = self._extract_metric(
            result, "energy_kwh", 0.0)
        carbon, carbon_valid = self._extract_metric(
            result, "carbon_co2e_kg", 0.0)

        latency_ms, latency_valid = self._extract_metric(
            result, "latency_ms", 0.0)
        if not latency_valid:
            latency_secs, latency_valid = self._extract_metric(
                result, "latency", 0.0)
            latency_ms = latency_secs * 1000.0 if latency_valid else 0.0

        evaluated: List[str] = []
        coerced: List[str] = []
        for name, valid in (
            ("energy", energy_valid),
            ("carbon", carbon_valid),
            ("latency", latency_valid),
        ):
            if valid:
                evaluated.append(name)
            else:
                coerced.append(name)

        # ---- Reasoning analysis ------------------------------------ #
        trace = self._extract_reasoning_trace(result)
        reasoning = self.reasoning_analyzer.analyze(trace)
        reasoning_step_count = reasoning.get("step_count", 0)

        # ---- Improvement suggestions (P1 3.1: pass latency_ms) ----- #
        suggester_input: Dict[str, Any] = {}
        if energy_valid:
            suggester_input["energy_kwh"] = energy
        if carbon_valid:
            suggester_input["carbon_co2e_kg"] = carbon
        if latency_valid:
            suggester_input["latency_ms"] = latency_ms
        suggestions = self.suggester.generate(
            {}, suggester_input, record=True)

        # ---- Energy feedback (P1 3.14: only pass present metrics) -- #
        energy_metrics: Dict[str, Any] = {}
        if energy_valid:
            energy_metrics["energy"] = energy
        if latency_valid:
            energy_metrics["latency"] = latency_ms / 1000.0
        if "memory" in result:
            memory = result["memory"]
            if isinstance(memory, (int, float)) and \
                    not isinstance(memory, bool) and \
                    not math.isnan(float(memory)) and \
                    not math.isinf(float(memory)) and \
                    float(memory) >= 0:
                energy_metrics["memory"] = float(memory)
        energy_feedback = analyze_energy_metrics(
            energy_metrics,
            config=self._energy_config,
            strict=self._strict,
        )

        # ---- Composite RLHF score (P0 2.5) ------------------------- #
        reasoning_score = float(
            reasoning.get("composite_score", 0.0))
        cfg = self._config

        energy_score = (
            self._score_inverse(energy, cfg.energy_reference_kwh)
            if energy_valid else 0.0
        )
        carbon_score = (
            self._score_inverse(carbon, cfg.carbon_reference_kg)
            if carbon_valid else 0.0
        )
        latency_score = (
            self._score_inverse(
                latency_ms / 1000.0,
                cfg.latency_ceiling_seconds)
            if latency_valid else 0.0
        )

        composite = (
            cfg.weight_reasoning * reasoning_score
            + cfg.weight_energy * energy_score
            + cfg.weight_carbon * carbon_score
            + cfg.weight_latency * latency_score
        )
        composite = max(0.0, min(1.0, composite))

        has_data = bool(evaluated) or reasoning_step_count > 0

        score = RLHFScore(
            composite=composite,
            reasoning=reasoning_score,
            energy=energy_score,
            carbon=carbon_score,
            latency=latency_score,
            has_data=has_data,
            evaluated=tuple(evaluated),
            coerced=tuple(coerced),
        )

        # ---- Record history ---------------------------------------- #
        record = RLHFRecord(
            task_id=task.get("task_id"),
            region=task.get("region"),
            score=score,
            suggestions_count=len(suggestions),
        )
        with self._lock:
            self._history.append(record)

        # ---- Optional emission (P1 3.4) ---------------------------- #
        if emit and self.sink is not None:
            try:
                self.sink.emit(score.to_dict())
            except Exception as exc:  # noqa: BLE001
                logger.warning("Sink emit failed: %s", exc)
                self._emit_metric("sink_emit_failed", {
                    "error_type": type(exc).__name__,
                })
                if self._strict:
                    raise RLHFEngineError(
                        f"Sink emit failed: {exc}") from exc

        # ---- Metrics hook (P1 3.6) --------------------------------- #
        self._emit_metric("feedback_generated", {
            "composite": composite,
            "reasoning": reasoning_score,
            "energy": energy_score,
            "carbon": carbon_score,
            "latency": latency_score,
            "suggestions_count": len(suggestions),
            "has_data": has_data,
            "evaluated": list(evaluated),
            "coerced": list(coerced),
        })

        logger.info(
            "RLHF feedback generated: composite=%.3f "
            "(reasoning=%.3f, energy=%.3f, carbon=%.3f, latency=%.3f, "
            "has_data=%s)",
            score.composite,
            score.reasoning,
            score.energy,
            score.carbon,
            score.latency,
            has_data,
        )

        return {
            "rlhf_score": score.composite,
            "rlhf_score_breakdown": score.to_dict(),
            "has_data": has_data,
            "evaluated": list(evaluated),
            "coerced": list(coerced),
            "reasoning_analysis": reasoning,
            "improvement_suggestions": suggestions,
            "comparative_analysis": {
                "energy_vs_reference": (
                    energy / cfg.energy_reference_kwh
                    if energy_valid else None
                ),
                "carbon_vs_reference": (
                    carbon / cfg.carbon_reference_kg
                    if carbon_valid else None
                ),
                "latency_ms": latency_ms if latency_valid else None,
                "latency_vs_ceiling": (
                    latency_ms / 1000.0 / cfg.latency_ceiling_seconds
                    if latency_valid else None
                ),
            },
            "sustainability_insights": {
                "energy_feedback": energy_feedback.to_text(),
                "is_efficient": energy_feedback.is_efficient,
                "has_energy_data": energy_feedback.has_data,
                "suggestions_count": len(suggestions),
            },
            "logs": {
                "reasoning_trace": [
                    dict(s) if isinstance(s, Mapping) else s
                    for s in trace
                ],
                "execution_log": list(result.get("execution_log", [])),
                "energy_profile": list(
                    result.get("energy_profile", [])),
            },
        }

    async def generate_feedback_async(
        self,
        result: Mapping[str, Any],
        task: Optional[Mapping[str, Any]] = None,
        *,
        emit: bool = False,
    ) -> Dict[str, Any]:
        """Async wrapper that offloads the sync work to a thread."""
        return await asyncio.to_thread(
            self.generate_feedback, result, task, emit=emit)

    # ---------------------------------------------------------- statistics
    def statistics(
        self, window: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Return aggregate statistics (P1 3.7: windowed view)."""
        with self._lock:
            if window is not None:
                history = list(self._history)[-window:]
            else:
                history = list(self._history)

        if not history:
            return {
                "evaluations": 0,
                "evaluations_with_data": 0,
                "evaluations_empty": 0,
                "mean_composite": None,
                "mean_reasoning": None,
                "mean_energy": None,
                "mean_carbon": None,
                "mean_latency": None,
                "total_suggestions": 0,
            }

        with_data = [r for r in history if r.score.has_data]
        empties = [r for r in history if not r.score.has_data]

        def _mean_attr(items: List[RLHFRecord], attr: str) -> Optional[float]:
            if not items:
                return None
            return sum(
                getattr(r.score, attr) for r in items) / len(items)

        return {
            "evaluations": len(history),
            "evaluations_with_data": len(with_data),
            "evaluations_empty": len(empties),
            "mean_composite": _mean_attr(with_data, "composite"),
            "mean_reasoning": _mean_attr(with_data, "reasoning"),
            "mean_energy": _mean_attr(with_data, "energy"),
            "mean_carbon": _mean_attr(with_data, "carbon"),
            "mean_latency": _mean_attr(with_data, "latency"),
            "total_suggestions": sum(
                r.suggestions_count for r in history),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        with self._lock:
            if clear_history:
                self._history.clear()
            self._ctx_start = None
            self._async_ctx_start = None
        logger.debug(
            "RLHFFeedbackEngine reset (clear_history=%s)",
            clear_history)

    def close(self) -> None:
        """P1 3.8: release resources. Idempotent."""
        close_fn = getattr(self.sink, "close", None)
        if callable(close_fn):
            try:
                close_fn()
            except Exception:  # noqa: BLE001
                logger.warning(
                    "sink.close() failed", exc_info=True)
        logger.debug(
            "RLHFFeedbackEngine closed (%d evaluation(s)).",
            len(self._history))

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "config": asdict(self._config),
                "reasoning_config": asdict(
                    self.reasoning_analyzer.config),
                "suggester_config": asdict(self.suggester.config),
                "energy_config": asdict(self._energy_config),
                "strict": self._strict,
                "sink": (
                    type(self.sink).__name__
                    if self.sink else None
                ),
                "history": [r.to_dict() for r in self._history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        sink: Optional[MetricSink] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "RLHFFeedbackEngine":
        if not isinstance(data, Mapping):
            raise RLHFEngineError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}.")

        # P0 2.10: delegate to from_legacy_dict.
        cfg = RLHFConfig.from_legacy_dict(
            data.get("config") or {})

        # P0 2.9: restore all three sub-configs.
        reasoning_cfg = _restore_config(
            ReasoningConfig, data.get("reasoning_config"))
        suggester_cfg = _restore_config(
            SuggesterConfig, data.get("suggester_config"))
        energy_cfg = _restore_config(
            EnergyFeedbackConfig, data.get("energy_config"))

        engine = cls(
            config=cfg,
            reasoning_config=reasoning_cfg,
            suggester_config=suggester_cfg,
            energy_config=energy_cfg,
            sink=sink,
            strict=bool(data.get("strict", True)),
            metrics_hook=metrics_hook,
        )

        # P1 3.10: warn on history truncation.
        history_entries = list(data.get("history", []))
        if len(history_entries) > cfg.max_history:
            logger.warning(
                "Restoring %d history entries; only most recent "
                "%d kept (max_history).",
                len(history_entries), cfg.max_history)

        with engine._lock:
            for entry in history_entries:
                engine._history.append(
                    RLHFRecord.from_dict(entry))
        return engine

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        sink: Optional[MetricSink] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "RLHFFeedbackEngine":
        try:
            return cls.from_dict(
                json.loads(payload),
                sink=sink,
                metrics_hook=metrics_hook,
            )
        except json.JSONDecodeError as exc:
            raise RLHFEngineError(
                f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- pickling
    def __getstate__(self) -> Dict[str, Any]:
        # P1 3.9: RLock and the sink's lock (if any) are not picklable.
        state = self.__dict__.copy()
        state.pop("_lock", None)
        # Sink is optional; if unpicklable, drop it and warn.
        if state.get("sink") is not None:
            try:
                json.dumps(state["sink"].__dict__)
            except Exception:  # noqa: BLE001
                logger.info(
                    "Dropping unpicklable sink on pickle.")
                state["sink"] = None
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "RLHFFeedbackEngine":
        with self._lock:
            self._ctx_start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            start = (
                self._ctx_start
                if self._ctx_start is not None
                else time.perf_counter()
            )
            elapsed = time.perf_counter() - start
            self._ctx_start = None
            n = len(self._history)
        if exc_type is not None:
            logger.warning(
                "RLHFFeedbackEngine scope exited with %s after "
                "%.4fs.", exc_type.__name__, elapsed)
            return
        logger.info(
            "RLHFFeedbackEngine scope closed in %.4fs "
            "(%d evaluation(s)).", elapsed, n)

    async def __aenter__(self) -> "RLHFFeedbackEngine":
        with self._lock:
            self._async_ctx_start = time.perf_counter()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            start = (
                self._async_ctx_start
                if self._async_ctx_start is not None
                else time.perf_counter()
            )
            elapsed = time.perf_counter() - start
            self._async_ctx_start = None
            n = len(self._history)
        if exc_type is not None:
            logger.warning(
                "Async RLHFFeedbackEngine scope exited with %s "
                "after %.4fs.", exc_type.__name__, elapsed)
            return
        logger.info(
            "Async RLHFFeedbackEngine scope closed in %.4fs "
            "(%d evaluation(s)).", elapsed, n)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            last = (
                self._history[-1].score.composite
                if self._history else None
            )
            last_str = (
                f"{last:.3f}" if last is not None else "n/a")
            return (
                "RLHFFeedbackEngine("
                f"version={__version__!r}, "
                f"evaluations={len(self._history)}, "
                f"last_composite={last_str}, "
                f"sink={type(self.sink).__name__ if self.sink else None}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "__version__",
    "RLHFConfig",
    "RLHFEngineError",
    "RLHFFeedbackEngine",
    "RLHFRecord",
    "RLHFScore",
]


# --------------------------------------------------------------------------- #
# Self-tests
# --------------------------------------------------------------------------- #
def _run_selftests() -> None:
    """Run the module's self-tests."""
    import io
    import pickle
    from contextlib import redirect_stdout

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print(f"=== rlhf_engine v{__version__} self-tests ===\n")

    # ---------------------------------------------------------------- #
    # P0 2.1 — NaN/Inf weights rejected
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: NaN/Inf weights rejected ===")
    for bad in (float("nan"), float("inf"), float("-inf")):
        try:
            RLHFConfig(
                weight_reasoning=bad,
                weight_energy=0.4,
                weight_carbon=0.2,
                weight_latency=0.1,
            )
        except RLHFEngineError as exc:
            print(f"  ✓ weight_reasoning={bad!r} → RLHFEngineError")
        else:
            raise AssertionError(
                f"P0 2.1 regression: weight_reasoning={bad!r} "
                f"was accepted")

    # Contrast: valid weights still work.
    RLHFConfig()
    print(f"  ✓ valid default config accepted")

    # ---------------------------------------------------------------- #
    # P0 2.2 — bool rejected as weight
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: bool rejected as weight ===")
    try:
        RLHFConfig(weight_reasoning=True)  # type: ignore[arg-type]
    except RLHFEngineError:
        print(f"  ✓ weight_reasoning=True rejected")
    else:
        raise AssertionError(
            "P0 2.2 regression: weight_reasoning=True accepted")

    try:
        RLHFConfig(weight_reasoning=False)  # type: ignore[arg-type]
    except RLHFEngineError:
        print(f"  ✓ weight_reasoning=False rejected")

    # ---------------------------------------------------------------- #
    # P0 2.3 — NaN/Inf reference values rejected
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: NaN/Inf reference values rejected ===")
    for field_name, bad in (
        ("energy_reference_kwh", float("nan")),
        ("energy_reference_kwh", float("inf")),
        ("carbon_reference_kg", float("nan")),
        ("carbon_reference_kg", float("inf")),
        ("latency_ceiling_seconds", float("nan")),
        ("latency_ceiling_seconds", float("inf")),
    ):
        kwargs = {field_name: bad}
        try:
            RLHFConfig(**kwargs)
        except RLHFEngineError:
            print(f"  ✓ {field_name}={bad!r} rejected")
        else:
            raise AssertionError(
                f"P0 2.3 regression: {field_name}={bad!r} accepted")

    # ---------------------------------------------------------------- #
    # P0 2.4 — negative metrics coerced (not rewarded with 1.0)
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: negative metric coerced, not rewarded ===")
    engine = RLHFFeedbackEngine()

    neg = engine.generate_feedback({"energy_kwh": -5.0})
    energy_score = neg["rlhf_score_breakdown"]["energy"]
    assert energy_score == 0.0, (
        f"P0 2.4 regression: energy_kwh=-5.0 gave "
        f"energy_score={energy_score} (expected 0.0)")
    assert "energy" in neg["coerced"], (
        f"P0 2.4 regression: 'energy' not in coerced={neg['coerced']}")
    assert "energy" not in neg["evaluated"]
    print(f"  ✓ energy_kwh=-5.0 → energy_score=0.0 (coerced)")

    # Contrast: valid small energy → perfect component score.
    pos = engine.generate_feedback({"energy_kwh": 0.0})
    assert pos["rlhf_score_breakdown"]["energy"] == 1.0, (
        f"control: energy_kwh=0.0 should score 1.0, "
        f"got {pos['rlhf_score_breakdown']['energy']}")
    print(f"  ✓ energy_kwh=0.0 → energy_score=1.0 (valid)")

    # ---------------------------------------------------------------- #
    # P0 2.5 — empty metrics: has_data=False, composite=0.0
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: empty metrics not falsely passing ===")
    empty = engine.generate_feedback({})
    assert empty["has_data"] is False, (
        f"P0 2.5 regression: has_data={empty['has_data']}")
    assert empty["rlhf_score"] == 0.0, (
        f"P0 2.5 regression: composite={empty['rlhf_score']}, "
        f"expected 0.0")
    assert empty["evaluated"] == (), (
        f"P0 2.5 regression: evaluated={empty['evaluated']}")
    assert set(empty["coerced"]) == {"energy", "carbon", "latency"}, (
        f"P0 2.5 regression: coerced={empty['coerced']}")
    # The v2.0.0 EnergyFeedback reports the "no data" message.
    fb_text = empty["sustainability_insights"]["energy_feedback"]
    assert "No metrics available" in fb_text, (
        f"P0 2.5 regression: energy_feedback={fb_text!r}; "
        f"expected the 'No metrics available' message")
    print(f"  ✓ {{}} → has_data=False, composite=0.0, "
          f"energy_feedback={fb_text!r}")

    # Partial: only energy validated → composite reflects only energy.
    partial = engine.generate_feedback({"energy_kwh": 0.0025})
    # energy_score = 1 - 0.0025/0.0025 = 0.0
    assert partial["has_data"] is True
    assert "energy" in partial["evaluated"]
    assert "carbon" in partial["coerced"]
    assert "latency" in partial["coerced"]
    # composite = 0.4*0 + 0.3*0 + 0.2*0 + 0.1*0 = 0.0 (energy at reference)
    print(f"  ✓ partial: energy_kwh=0.0025 → has_data=True, "
          f"composite={partial['rlhf_score']}")

    # ---------------------------------------------------------------- #
    # P0 2.6 — RLHFScore structural validation
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: RLHFScore validation ===")

    try:
        RLHFScore(
            composite=2.0,  # > 1.0
            reasoning=0.5, energy=0.5, carbon=0.5, latency=0.5)
    except RLHFEngineError as exc:
        print(f"  ✓ composite=2.0 rejected: {exc}")
    else:
        raise AssertionError(
            "P0 2.6 regression: composite=2.0 accepted")

    try:
        RLHFScore(
            composite=0.5, reasoning=float("nan"),
            energy=0.5, carbon=0.5, latency=0.5)
    except RLHFEngineError:
        print(f"  ✓ NaN reasoning rejected")
    else:
        raise AssertionError(
            "P0 2.6 regression: NaN reasoning accepted")

    try:
        RLHFScore(
            composite=0.5, reasoning=-0.1,
            energy=0.5, carbon=0.5, latency=0.5)
    except RLHFEngineError:
        print(f"  ✓ negative reasoning rejected")
    else:
        raise AssertionError(
            "P0 2.6 regression: negative reasoning accepted")

    # Valid construction.
    valid = RLHFScore(
        composite=0.5, reasoning=0.5, energy=0.5,
        carbon=0.5, latency=0.5)
    print(f"  ✓ valid construction accepted")

    # ---------------------------------------------------------------- #
    # P0 2.7 — RLHFRecord validation + from_dict
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.7: RLHFRecord validation + from_dict ===")

    try:
        RLHFRecord(
            task_id=42,  # type: ignore[arg-type]
            region="eu-west",
            score=valid,
            suggestions_count=0,
        )
    except RLHFEngineError:
        print(f"  ✓ task_id=42 rejected")
    else:
        raise AssertionError(
            "P0 2.7 regression: non-string task_id accepted")

    try:
        RLHFRecord(
            task_id="t-1", region="eu-west",
            score=valid,
            suggestions_count=-1,
        )
    except RLHFEngineError:
        print(f"  ✓ suggestions_count=-1 rejected")
    else:
        raise AssertionError(
            "P0 2.7 regression: negative suggestions_count accepted")

    try:
        RLHFRecord(
            task_id="t-1", region="eu-west",
            score="not-a-score",  # type: ignore[arg-type]
            suggestions_count=0,
        )
    except RLHFEngineError:
        print(f"  ✓ non-RLHFScore rejected")
    else:
        raise AssertionError(
            "P0 2.7 regression: string score accepted")

    # Valid construction + round-trip.
    rec = RLHFRecord(
        task_id="t-1", region="eu-west",
        score=valid, suggestions_count=3,
    )
    rt = RLHFRecord.from_dict(rec.to_dict())
    assert rt == rec
    print(f"  ✓ RLHFRecord.from_dict round-trip OK")

    # ---------------------------------------------------------------- #
    # P0 2.8 — RLHFScore.from_dict raises RLHFEngineError
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.8: RLHFScore.from_dict robustness ===")

    try:
        RLHFScore.from_dict({})  # missing 'composite'
    except RLHFEngineError as exc:
        print(f"  ✓ missing composite → RLHFEngineError: {exc}")
    except KeyError as exc:
        raise AssertionError(
            f"P0 2.8 regression: raised KeyError instead of "
            f"RLHFEngineError: {exc}")
    else:
        raise AssertionError(
            "P0 2.8 regression: missing composite accepted")

    try:
        RLHFScore.from_dict({"composite": None})
    except RLHFEngineError:
        print(f"  ✓ composite=None → RLHFEngineError")
    except TypeError as exc:
        raise AssertionError(
            f"P0 2.8 regression: raised TypeError instead of "
            f"RLHFEngineError: {exc}")
    else:
        raise AssertionError(
            "P0 2.8 regression: composite=None accepted")

    try:
        RLHFScore.from_dict({
            "composite": "not-a-number",
            "reasoning": 0.5,
            "energy": 0.5,
            "carbon": 0.5,
            "latency": 0.5,
        })
    except RLHFEngineError:
        print(f"  ✓ composite='not-a-number' → RLHFEngineError")
    else:
        raise AssertionError(
            "P0 2.8 regression: non-numeric composite accepted")

    # Valid round-trip.
    original = RLHFScore(
        composite=0.5, reasoning=0.5, energy=0.5,
        carbon=0.5, latency=0.5, has_data=True,
        evaluated=("energy",), coerced=("carbon", "latency"))
    restored = RLHFScore.from_dict(original.to_dict())
    assert restored == original
    print(f"  ✓ valid round-trip preserves has_data/evaluated/coerced")

    # ---------------------------------------------------------------- #
    # P0 2.9 — from_dict restores all sub-configs
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.9: from_dict restores all sub-configs ===")

    custom_engine = RLHFFeedbackEngine(
        reasoning_config=ReasoningConfig(
            efficiency_ceiling_steps=5),
        suggester_config=SuggesterConfig(
            latency_threshold_ms=500.0),
        energy_config=EnergyFeedbackConfig(
            high_energy_threshold=0.5),
    )
    custom_engine.generate_feedback(
        {"energy_kwh": 0.001})

    payload = custom_engine.to_dict()
    assert payload["reasoning_config"]["efficiency_ceiling_steps"] == 5
    assert payload["suggester_config"]["latency_threshold_ms"] == 500.0
    assert payload["energy_config"]["high_energy_threshold"] == 0.5
    print(f"  ✓ to_dict includes all three sub-configs")

    restored_engine = RLHFFeedbackEngine.from_dict(payload)
    assert restored_engine.reasoning_analyzer.config.efficiency_ceiling_steps == 5, (
        f"P0 2.9 regression: reasoning_config.efficiency_ceiling_steps="
        f"{restored_engine.reasoning_analyzer.config.efficiency_ceiling_steps}")
    assert restored_engine.suggester.config.latency_threshold_ms == 500.0, (
        f"P0 2.9 regression: suggester_config.latency_threshold_ms="
        f"{restored_engine.suggester.config.latency_threshold_ms}")
    assert restored_engine._energy_config.high_energy_threshold == 0.5, (
        f"P0 2.9 regression: energy_config.high_energy_threshold="
        f"{restored_engine._energy_config.high_energy_threshold}")
    print(f"  ✓ from_dict restores reasoning/suggester/energy configs")

    # ---------------------------------------------------------------- #
    # P0 2.10 — from_dict uses declared defaults for omitted fields
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.10: from_dict uses declared defaults ===")

    legacy_payload = {
        "config": {"weight_reasoning": 0.5},
        "strict": True,
        "history": [],
    }
    legacy_engine = RLHFFeedbackEngine.from_dict(legacy_payload)
    assert legacy_engine.config.weight_reasoning == 0.5
    # Defaults for the other fields.
    assert legacy_engine.config.weight_energy == 0.3, (
        f"P0 2.10 regression: weight_energy="
        f"{legacy_engine.config.weight_energy}")
    assert legacy_engine.config.weight_carbon == 0.2
    assert legacy_engine.config.weight_latency == 0.1
    assert legacy_engine.config.energy_reference_kwh == 0.0025
    assert legacy_engine.config.carbon_reference_kg == 0.001
    assert legacy_engine.config.latency_ceiling_seconds == 5.0
    assert legacy_engine.config.max_history == 1000
    print(f"  ✓ weight_reasoning=0.5 (explicit); other fields use "
          f"declared defaults")

    # ---------------------------------------------------------------- #
    # P1 3.1 — latency suggestion fires
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: latency suggestion now fires ===")

    latency_engine = RLHFFeedbackEngine()
    result = latency_engine.generate_feedback({
        "energy_kwh": 0.001,
        "carbon_co2e_kg": 0.0005,
        "latency_ms": 5000.0,   # above default ceiling of 2000ms
    })
    # v2.0.0 suggester emits a latency suggestion when threshold is
    # exceeded.
    latency_suggestions = [
        s for s in result["improvement_suggestions"]
        if "atency" in s or "⏱" in s
    ]
    assert latency_suggestions, (
        f"P1 3.1 regression: no latency suggestion emitted; "
        f"got {result['improvement_suggestions']}")
    print(f"  ✓ latency_ms=5000 → latency suggestion fired")

    # ---------------------------------------------------------------- #
    # P1 3.4 — emit failure wraps as RLHFEngineError
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.4: emit failure wraps as RLHFEngineError ===")

    class _BrokenSink:
        def emit(self, metrics: Mapping[str, Any]) -> None:
            raise RuntimeError("simulated sink failure")

    strict_engine = RLHFFeedbackEngine(
        sink=_BrokenSink(), strict=True)  # type: ignore[arg-type]
    try:
        strict_engine.generate_feedback(
            {"energy_kwh": 0.001}, emit=True)
    except RLHFEngineError as exc:
        print(f"  ✓ strict: emit failure → RLHFEngineError: {exc}")
    except RuntimeError as exc:
        raise AssertionError(
            f"P1 3.4 regression: raw RuntimeError leaked "
            f"({exc}) instead of RLHFEngineError")
    else:
        raise AssertionError(
            "P1 3.4 regression: emit failure did not raise")

    lenient_engine = RLHFFeedbackEngine(
        sink=_BrokenSink(), strict=False)  # type: ignore[arg-type]
    out = lenient_engine.generate_feedback(
        {"energy_kwh": 0.001}, emit=True)
    assert out["rlhf_score"] is not None
    print(f"  ✓ lenient: emit failure swallowed, feedback still returned")

    # ---------------------------------------------------------------- #
    # P1 3.5 — sink validated at construction
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.5: sink validated at construction ===")

    try:
        RLHFFeedbackEngine(sink="not-a-sink")  # type: ignore[arg-type]
    except RLHFEngineError as exc:
        print(f"  ✓ sink='not-a-sink' rejected: {exc}")
    else:
        raise AssertionError(
            "P1 3.5 regression: invalid sink accepted")

    # ---------------------------------------------------------------- #
    # P1 3.6 — metrics_hook
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.6: metrics_hook ===")

    events: List[Tuple[str, Mapping[str, Any]]] = []
    hooked_engine = RLHFFeedbackEngine(
        metrics_hook=lambda ev, f: events.append((ev, f)))
    hooked_engine.generate_feedback({"energy_kwh": 0.001})
    event_names = [ev for ev, _ in events]
    assert "feedback_generated" in event_names
    generated = [
        (ev, f) for ev, f in events if ev == "feedback_generated"
    ]
    assert generated
    assert "composite" in generated[0][1]
    assert "has_data" in generated[0][1]
    print(f"  ✓ metrics_hook fired feedback_generated with "
          f"{len(generated[0][1])} fields")

    # ---------------------------------------------------------------- #
    # P1 3.7 — statistics(window=)
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.7: statistics(window=) ===")

    stats_engine = RLHFFeedbackEngine()
    stats_engine.generate_feedback({"energy_kwh": 0.001})
    stats_engine.generate_feedback({"energy_kwh": 0.002})
    stats_engine.generate_feedback({})  # empty
    stats_engine.generate_feedback({"energy_kwh": 0.003})

    stats = stats_engine.statistics()
    assert stats["evaluations"] == 4
    assert stats["evaluations_with_data"] == 3
    assert stats["evaluations_empty"] == 1
    print(f"  ✓ statistics: evaluations=4, with_data=3, empty=1")

    win = stats_engine.statistics(window=2)
    assert win["evaluations"] == 2
    print(f"  ✓ statistics(window=2): evaluations=2")

    # ---------------------------------------------------------------- #
    # P1 3.8 — close, context managers, __version__
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.8: close / context / version ===")

    stats_engine.close()
    stats_engine.close()
    print(f"  ✓ close() idempotent")

    with RLHFFeedbackEngine() as scoped:
        scoped.generate_feedback({"energy_kwh": 0.001})
    print(f"  ✓ sync context manager OK")

    async def _async_scope() -> None:
        async with RLHFFeedbackEngine() as scoped:
            scoped.generate_feedback({"energy_kwh": 0.001})

    asyncio.run(_async_scope())
    print(f"  ✓ async context manager OK")

    import sys
    mod = sys.modules[__name__]
    assert mod.__version__ == "2.0.0"
    print(f"  ✓ __version__ = {mod.__version__!r}")

    # ---------------------------------------------------------------- #
    # P1 3.9 — pickle round-trip
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.9: pickle round-trip ===")

    p_engine = RLHFFeedbackEngine()
    p_engine.generate_feedback({"energy_kwh": 0.001})
    blob = pickle.dumps(p_engine)
    restored_p = pickle.loads(blob)
    assert restored_p._lock is not None
    assert len(restored_p.history) == 1
    # Still usable.
    restored_p.generate_feedback({"energy_kwh": 0.002})
    assert len(restored_p.history) == 2
    print(f"  ✓ pickle round-trip preserves history; still usable")

    # ---------------------------------------------------------------- #
    # P1 3.10 — history truncation warning
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.10: history truncation warning ===")

    class _CaptureHandler(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.records: List[logging.LogRecord] = []

        def emit(self, record: logging.LogRecord) -> None:
            self.records.append(record)

    small_engine = RLHFFeedbackEngine(
        config=RLHFConfig(max_history=2))
    p = small_engine.to_dict()
    # Fabricate a payload with 5 entries.
    base_score = RLHFScore(
        composite=0.5, reasoning=0.5, energy=0.5,
        carbon=0.5, latency=0.5, has_data=True)
    base_record = RLHFRecord(
        task_id="t", region="r", score=base_score,
        suggestions_count=0)
    p["history"] = [base_record.to_dict() for _ in range(5)]

    handler = _CaptureHandler()
    logger.addHandler(handler)
    old_level = logger.level
    logger.setLevel(logging.WARNING)
    try:
        restored_small = RLHFFeedbackEngine.from_dict(p)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)

    assert len(restored_small.history) == 2
    warnings = [
        r for r in handler.records
        if r.levelno == logging.WARNING and "Restoring" in r.getMessage()
    ]
    assert warnings, (
        f"P1 3.10 regression: no truncation warning; "
        f"captured: {[r.getMessage() for r in handler.records]}")
    print(f"  ✓ 5 payload entries → 2 kept, warning logged")

    # ---------------------------------------------------------------- #
    # Bonus: valid efficient + inefficient flow
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: efficient vs inefficient ===")

    efficient_engine = RLHFFeedbackEngine()
    good = efficient_engine.generate_feedback({
        "energy_kwh": 0.001,
        "carbon_co2e_kg": 0.0005,
        "latency_ms": 500.0,
        "reasoning_trace": [
            {"thought": "Because the task is simple, use the "
                       "smallest model."},
            {"thought": "Therefore energy is minimized."},
            {"thought": "Thus we optimize for sustainability."},
        ],
    })
    assert good["has_data"] is True
    assert good["rlhf_score"] > 0.5, (
        f"efficient case composite={good['rlhf_score']}")
    print(f"  ✓ efficient: composite={good['rlhf_score']:.3f}")

    bad = efficient_engine.generate_feedback({
        "energy_kwh": 0.01,
        "carbon_co2e_kg": 0.005,
        "latency_ms": 8000.0,
        "reasoning_trace": [
            {"thought": f"step {i}"} for i in range(25)],
    })
    assert bad["has_data"] is True
    assert bad["rlhf_score"] < good["rlhf_score"], (
        f"bad composite={bad['rlhf_score']:.3f} should be < "
        f"good={good['rlhf_score']:.3f}")
    print(f"  ✓ inefficient: composite={bad['rlhf_score']:.3f} "
          f"< efficient={good['rlhf_score']:.3f}")

    # ---------------------------------------------------------------- #
    # Backward compat: v1.0.0 payload
    # ---------------------------------------------------------------- #
    print("\n=== Backward compat: v1.0.0 payload ===")

    v1_payload = {
        "config": {
            "weight_reasoning": 0.4,
            "weight_energy": 0.3,
            "weight_carbon": 0.2,
            "weight_latency": 0.1,
            "energy_reference_kwh": 0.0025,
            "carbon_reference_kg": 0.001,
            "latency_ceiling_seconds": 5.0,
            "max_history": 1000,
        },
        "strict": True,
        "history": [
            {
                "task_id": "t-1",
                "region": "eu-west",
                "score": {
                    "composite": 0.5,
                    "reasoning": 0.6,
                    "energy": 0.7,
                    "carbon": 0.8,
                    "latency": 0.9,
                    # has_data / evaluated / coerced absent — defaults
                },
                "suggestions_count": 2,
                "timestamp": 1700000000.0,
            }
        ],
    }
    restored_v1 = RLHFFeedbackEngine.from_dict(v1_payload)
    assert len(restored_v1.history) == 1
    r = restored_v1.history[0]
    assert r.score.composite == 0.5
    assert r.score.has_data is False  # v1.0.0 payload didn't carry it
    assert r.score.evaluated == ()
    assert r.score.coerced == ()
    print(f"  ✓ v1.0.0 payload loads; new fields default")

    # ---------------------------------------------------------------- #
    # Validation: bad inputs rejected
    # ---------------------------------------------------------------- #
    print("\n=== Validation: bad inputs rejected ===")

    for bad in (None, 42, "string"):
        try:
            efficient_engine.generate_feedback(bad)  # type: ignore[arg-type]
        except RLHFEngineError:
            print(f"  ✓ result={bad!r} rejected")
        else:
            raise AssertionError(
                f"Validation regression: result={bad!r} accepted")

    try:
        efficient_engine.generate_feedback(
            {"energy_kwh": 0.001},
            task="not-a-mapping")  # type: ignore[arg-type]
    except RLHFEngineError:
        print(f"  ✓ task='not-a-mapping' rejected")

    print("\n✓ All self-tests passed.")


if __name__ == "__main__":  # pragma: no cover
    _run_selftests()
