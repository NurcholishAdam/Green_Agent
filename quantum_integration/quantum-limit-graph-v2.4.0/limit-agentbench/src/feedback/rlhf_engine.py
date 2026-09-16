# src/feedback/rlhf_engine.py

"""
RLHF Feedback Engine
====================

Main feedback engine combining all components:
- :class:`ReasoningTraceAnalyzer` (imported — previously a ``NameError``)
- :class:`SustainabilityImprovementSuggester`
- :func:`generate_energy_feedback`
- Optional :class:`MetricSink` for emission

Enhancements
------------
- Fixed the missing imports of ``ReasoningTraceAnalyzer`` and
  ``SustainabilityImprovementSuggester`` (the original raised ``NameError``
  on every call to ``generate_feedback``).
- ``RLHFConfig`` with validated score weights and sink policy.
- ``RLHFScore`` and ``RLHFRecord`` frozen dataclasses with serialization.
- ``RLock``-guarded bounded history.
- Full validation of ``result`` / ``task``; strict / non-strict modes.
- Structured ``generate_feedback`` output matching the original schema.
- ``statistics()``, ``__repr__``, sync + async context managers, smoke test.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any, Awaitable, Deque, Dict, List, Mapping, Optional

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


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class RLHFEngineError(ValueError):
    """Raised for invalid inputs, configuration, or feedback failures."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RLHFConfig:
    """Tunable parameters for the RLHF feedback engine."""

    # Composite score weights (must sum to 1.0).
    weight_reasoning: float = 0.4
    weight_energy: float = 0.3
    weight_carbon: float = 0.2
    weight_latency: float = 0.1

    # Energy / carbon scoring reference values.
    energy_reference_kwh: float = 0.0025
    carbon_reference_kg: float = 0.001

    # Latency scoring ceiling (seconds).
    latency_ceiling_seconds: float = 5.0

    # History ring-buffer.
    max_history: int = 1000

    def __post_init__(self) -> None:
        total = (
            self.weight_reasoning + self.weight_energy
            + self.weight_carbon + self.weight_latency
        )
        if abs(total - 1.0) > 1e-6:
            raise RLHFEngineError(
                f"RLHF weights must sum to 1.0 (got {total:.6f})."
            )
        for name in ("weight_reasoning", "weight_energy",
                     "weight_carbon", "weight_latency"):
            if getattr(self, name) < 0:
                raise RLHFEngineError(f"{name} must be >= 0.")
        if self.energy_reference_kwh <= 0:
            raise RLHFEngineError("energy_reference_kwh must be > 0.")
        if self.carbon_reference_kg <= 0:
            raise RLHFEngineError("carbon_reference_kg must be > 0.")
        if self.latency_ceiling_seconds <= 0:
            raise RLHFEngineError("latency_ceiling_seconds must be > 0.")
        if self.max_history <= 0:
            raise RLHFEngineError("max_history must be > 0.")


# --------------------------------------------------------------------------- #
# Score & record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RLHFScore:
    """Immutable composite RLHF score."""

    composite: float
    reasoning: float
    energy: float
    carbon: float
    latency: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RLHFScore":
        return cls(
            composite=float(data["composite"]),
            reasoning=float(data.get("reasoning", 0.0)),
            energy=float(data.get("energy", 0.0)),
            carbon=float(data.get("carbon", 0.0)),
            latency=float(data.get("latency", 0.0)),
        )

    def __repr__(self) -> str:
        return (
            "RLHFScore("
            f"composite={self.composite:.3f}, "
            f"reasoning={self.reasoning:.3f}, energy={self.energy:.3f}, "
            f"carbon={self.carbon:.3f}, latency={self.latency:.3f})"
        )


@dataclass(frozen=True)
class RLHFRecord:
    """Immutable record of one RLHF feedback evaluation."""

    task_id: Optional[str]
    region: Optional[str]
    score: RLHFScore
    suggestions_count: int
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "region": self.region,
            "score": self.score.to_dict(),
            "suggestions_count": self.suggestions_count,
            "timestamp": self.timestamp,
        }


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #
class RLHFFeedbackEngine:
    """
    Main feedback engine combining all components.

    Thread-safe, serializable, and bounded in memory. The original
    ``generate_feedback(result, task)`` signature is preserved; new
    parameters are keyword-only.
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
    ) -> None:
        self._config = config or RLHFConfig()
        self._strict = bool(strict)

        # Components — imports fixed above.
        self.reasoning_analyzer = ReasoningTraceAnalyzer(
            config=reasoning_config, strict=strict
        )
        self.suggester = SustainabilityImprovementSuggester(
            config=suggester_config, strict=strict
        )
        self._energy_config = energy_config or EnergyFeedbackConfig()
        self.sink = sink  # optional

        self._lock = threading.RLock()
        self._history: Deque[RLHFRecord] = deque(
            maxlen=self._config.max_history
        )
        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None

        logger.debug(
            "RLHFFeedbackEngine initialized (sink=%s, strict=%s)",
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

    # ---------------------------------------------------------- public API
    def generate_feedback(
        self,
        result: Mapping[str, Any],
        task: Optional[Mapping[str, Any]] = None,
        *,
        emit: bool = False,
    ) -> Dict[str, Any]:
        """
        Generate comprehensive RLHF feedback.

        Parameters
        ----------
        result : Mapping
            Execution result. Recognized keys: ``energy_kwh``,
            ``carbon_co2e_kg``, ``latency`` (or ``latency_ms``),
            ``reasoning_trace`` (list of steps).
        task : Mapping, optional
            Task metadata. Recognized keys: ``task_id``, ``region``.
        emit : bool, default False
            If True and a sink is configured, emit the metrics through it.

        Returns
        -------
        dict
            ``{"rlhf_score", "reasoning_analysis", "improvement_suggestions",
            "comparative_analysis", "sustainability_insights", "logs"}``.
        """
        if not isinstance(result, Mapping):
            raise RLHFEngineError(
                f"result must be a Mapping, got {type(result).__name__}."
            )
        task = task or {}

        # ---- Extract metrics -----------------------------------------
        energy = self._safe_float(result, "energy_kwh", 0.0)
        carbon = self._safe_float(result, "carbon_co2e_kg", 0.0)
        latency_ms = self._safe_float(result, "latency_ms", None)  # type: ignore[arg-type]
        if latency_ms is None:
            latency_ms = self._safe_float(result, "latency", 0.0) * 1000.0

        # ---- Reasoning analysis --------------------------------------
        trace = result.get("reasoning_trace") or result.get("trace") or []
        reasoning = self.reasoning_analyzer.analyze(trace)

        # ---- Improvement suggestions ---------------------------------
        analysis_for_suggester = {
            "energy_kwh": energy,
            "carbon_co2e_kg": carbon,
        }
        suggestions = self.suggester.generate(analysis_for_suggester, analysis_for_suggester)

        # ---- Energy feedback -----------------------------------------
        energy_feedback = analyze_energy_metrics(
            {"energy": energy, "latency": latency_ms / 1000.0, "memory": result.get("memory", 0.0)},
            config=self._energy_config,
            strict=self._strict,
        )

        # ---- Composite RLHF score ------------------------------------
        reasoning_score = float(reasoning.get("composite_score", 0.0))
        energy_score = self._score_inverse(
            energy, self._config.energy_reference_kwh
        )
        carbon_score = self._score_inverse(
            carbon, self._config.carbon_reference_kg
        )
        latency_score = self._score_inverse(
            latency_ms / 1000.0, self._config.latency_ceiling_seconds
        )
        cfg = self._config
        composite = (
            cfg.weight_reasoning * reasoning_score
            + cfg.weight_energy * energy_score
            + cfg.weight_carbon * carbon_score
            + cfg.weight_latency * latency_score
        )

        score = RLHFScore(
            composite=max(0.0, min(1.0, composite)),
            reasoning=reasoning_score,
            energy=energy_score,
            carbon=carbon_score,
            latency=latency_score,
        )

        # ---- Record history ------------------------------------------
        record = RLHFRecord(
            task_id=task.get("task_id") if isinstance(task, Mapping) else None,
            region=task.get("region") if isinstance(task, Mapping) else None,
            score=score,
            suggestions_count=len(suggestions),
        )
        with self._lock:
            self._history.append(record)

        # ---- Optional emission ---------------------------------------
        if emit and self.sink is not None:
            try:
                self.sink.emit(score.to_dict())
            except Exception as exc:
                logger.warning("Sink emit failed: %s", exc)
                if self._strict:
                    raise

        logger.info(
            "RLHF feedback generated: composite=%.3f (reasoning=%.3f, "
            "energy=%.3f, carbon=%.3f, latency=%.3f)",
            score.composite,
            score.reasoning,
            score.energy,
            score.carbon,
            score.latency,
        )

        return {
            "rlhf_score": score.composite,
            "rlhf_score_breakdown": score.to_dict(),
            "reasoning_analysis": reasoning,
            "improvement_suggestions": suggestions,
            "comparative_analysis": {
                "energy_vs_reference": energy / self._config.energy_reference_kwh,
                "carbon_vs_reference": carbon / self._config.carbon_reference_kg,
                "latency_vs_ceiling": (
                    latency_ms / 1000.0 / self._config.latency_ceiling_seconds
                ),
            },
            "sustainability_insights": {
                "energy_feedback": energy_feedback.to_text(),
                "is_efficient": energy_feedback.is_efficient,
                "suggestions_count": len(suggestions),
            },
            "logs": {
                "reasoning_trace": [dict(s) if isinstance(s, Mapping) else s for s in trace],
                "execution_log": list(result.get("execution_log", [])),
                "energy_profile": list(result.get("energy_profile", [])),
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
            self.generate_feedback, result, task, emit=emit
        )

    # ---------------------------------------------------------- helpers
    @staticmethod
    def _safe_float(
        mapping: Mapping[str, Any], key: str, default: Optional[float]
    ) -> Optional[float]:
        value = mapping.get(key, default)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return default
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default
        return f

    @staticmethod
    def _score_inverse(value: float, reference: float) -> float:
        """Return ``max(0, 1 - value/reference)`` for a 'lower is better' metric."""
        if reference <= 0:
            return 0.0
        return max(0.0, min(1.0, 1.0 - value / reference))

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the RLHF history."""
        with self._lock:
            history = list(self._history)
        if not history:
            return {
                "evaluations": 0,
                "mean_composite": None,
                "mean_reasoning": None,
                "mean_energy": None,
                "mean_carbon": None,
                "mean_latency": None,
                "total_suggestions": 0,
            }
        return {
            "evaluations": len(history),
            "mean_composite": sum(r.score.composite for r in history) / len(history),
            "mean_reasoning": sum(r.score.reasoning for r in history) / len(history),
            "mean_energy": sum(r.score.energy for r in history) / len(history),
            "mean_carbon": sum(r.score.carbon for r in history) / len(history),
            "mean_latency": sum(r.score.latency for r in history) / len(history),
            "total_suggestions": sum(r.suggestions_count for r in history),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset internal state; optionally clear history."""
        with self._lock:
            if clear_history:
                self._history.clear()
        logger.debug("RLHFFeedbackEngine reset (clear_history=%s)", clear_history)

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "strict": self._strict,
                "sink": type(self.sink).__name__ if self.sink else None,
                "reasoning_config": asdict(self.reasoning_analyzer.config),
                "suggester_config": asdict(self.suggester.config),
                "history": [r.to_dict() for r in self._history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        sink: Optional[MetricSink] = None,
    ) -> "RLHFFeedbackEngine":
        if not isinstance(data, Mapping):
            raise RLHFEngineError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = RLHFConfig(
            weight_reasoning=float(cfg_data.get("weight_reasoning", 0.4)),
            weight_energy=float(cfg_data.get("weight_energy", 0.3)),
            weight_carbon=float(cfg_data.get("weight_carbon", 0.2)),
            weight_latency=float(cfg_data.get("weight_latency", 0.1)),
            energy_reference_kwh=float(
                cfg_data.get("energy_reference_kwh", 0.0025)
            ),
            carbon_reference_kg=float(
                cfg_data.get("carbon_reference_kg", 0.001)
            ),
            latency_ceiling_seconds=float(
                cfg_data.get("latency_ceiling_seconds", 5.0)
            ),
            max_history=int(cfg_data.get("max_history", 1000)),
        )
        engine = cls(config=cfg, sink=sink, strict=bool(data.get("strict", True)))
        with engine._lock:
            for entry in data.get("history", []):
                engine._history.append(
                    RLHFRecord(
                        task_id=entry.get("task_id"),
                        region=entry.get("region"),
                        score=RLHFScore.from_dict(entry["score"]),
                        suggestions_count=int(entry.get("suggestions_count", 0)),
                        timestamp=float(entry.get("timestamp", time.time())),
                    )
                )
        return engine

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls, payload: str, *, sink: Optional[MetricSink] = None
    ) -> "RLHFFeedbackEngine":
        try:
            return cls.from_dict(json.loads(payload), sink=sink)
        except json.JSONDecodeError as exc:
            raise RLHFEngineError(f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "RLHFFeedbackEngine":
        self._ctx_start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "RLHFFeedbackEngine scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info("RLHFFeedbackEngine scope closed in %.4fs.", elapsed)

    async def __aenter__(self) -> "RLHFFeedbackEngine":
        self._async_ctx_start = time.perf_counter()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._async_ctx_start
            if self._async_ctx_start is not None
            else time.perf_counter()
        )
        self._async_ctx_start = None
        if exc_type is not None:
            logger.warning(
                "Async RLHFFeedbackEngine scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info("Async RLHFFeedbackEngine scope closed in %.4fs.", elapsed)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "RLHFFeedbackEngine("
                f"evaluations={len(self._history)}, "
                f"sink={type(self.sink).__name__ if self.sink else None}, "
                f"strict={self._strict})"
            )


__all__ = [
    "RLHFConfig",
    "RLHFEngineError",
    "RLHFFeedbackEngine",
    "RLHFRecord",
    "RLHFScore",
]


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    engine = RLHFFeedbackEngine(sink=StdoutSink())
    print("repr       :", engine)

    # Efficient result.
    good = engine.generate_feedback(
        {
            "energy_kwh": 0.001,
            "carbon_co2e_kg": 0.0005,
            "latency_ms": 500.0,
            "reasoning_trace": [
                {"thought": "Because the task is simple, use the smallest model."},
                {"thought": "Therefore energy is minimized."},
                {"thought": "Thus we optimize for sustainability."},
            ],
        },
        {"task_id": "t-1", "region": "eu-west-1"},
    )
    print("Good score :", good["rlhf_score"])

    # Inefficient result.
    bad = engine.generate_feedback(
        {
            "energy_kwh": 0.01,
            "carbon_co2e_kg": 0.005,
            "latency_ms": 8000.0,
            "reasoning_trace": [{"thought": f"step {i}"} for i in range(25)],
        },
        {"task_id": "t-2", "region": "us-east-1"},
        emit=True,
    )
    print("Bad score  :", bad["rlhf_score"])
    print("Suggestions:", bad["improvement_suggestions"])

    # Statistics
    print("Stats      :", engine.statistics())

    # Serialization round-trip
    payload = engine.to_json()
    restored = RLHFFeedbackEngine.from_json(payload, sink=StdoutSink())
    assert restored.to_dict() == engine.to_dict()
    print("Round-trip OK.")

    # Async wrapper
    async def _async_main() -> None:
        async with RLHFFeedbackEngine() as scoped:
            result = await scoped.generate_feedback_async(
                {"energy_kwh": 0.001, "reasoning_trace": []}
            )
            print("Async score:", result["rlhf_score"])

    asyncio.run(_async_main())

    # Validation failure
    try:
        engine.generate_feedback(None)  # type: ignore[arg-type]
    except RLHFEngineError as exc:
        print("Rejected   :", exc)
