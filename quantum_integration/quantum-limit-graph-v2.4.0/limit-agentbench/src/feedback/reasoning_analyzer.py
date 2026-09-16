# src/feedback/reasoning_analyzer.py

"""
Reasoning Trace Analyzer
========================

Analyzes agent reasoning traces for quality metrics.

Enhancements
------------
- Implemented the previously missing ``analyze()`` method.
- Fixed the ``ZeroDivisionError`` in ``_assess_energy_awareness`` (was
  ``mentions / len(trace)`` with no empty-trace guard).
- ``ReasoningConfig`` — configurable keyword lists, weights, thresholds.
- ``RLock``-guarded bounded history.
- Full validation; strict / non-strict modes.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- ``statistics()``, ``__repr__``, ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any, Deque, Dict, List, Mapping, Optional, Sequence

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ReasoningError(ValueError):
    """Raised for invalid reasoning traces or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ReasoningConfig:
    """Tunable parameters for the reasoning analyzer."""

    energy_keywords: tuple = (
        "energy", "efficient", "optimize", "carbon", "sustainable",
        "efficiency", "green", "emissions",
    )
    coherence_keywords: tuple = (
        "because", "therefore", "thus", "hence", "consequently", "so",
    )
    max_history: int = 1000
    # Weights for the composite reasoning score.
    weight_coherence: float = 0.3
    weight_factuality: float = 0.4
    weight_efficiency: float = 0.2
    weight_energy_awareness: float = 0.1

    def __post_init__(self) -> None:
        if self.max_history <= 0:
            raise ReasoningError("max_history must be > 0.")
        if not self.energy_keywords:
            raise ReasoningError("energy_keywords must be non-empty.")
        total = (
            self.weight_coherence + self.weight_factuality
            + self.weight_efficiency + self.weight_energy_awareness
        )
        if abs(total - 1.0) > 1e-6:
            raise ReasoningError(
                f"reasoning weights must sum to 1.0 (got {total:.6f})."
            )


# --------------------------------------------------------------------------- #
# Analysis result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ReasoningAnalysis:
    """Immutable result of analyzing a reasoning trace."""

    coherence_score: float
    factuality_score: float
    efficiency_score: float
    energy_awareness: float
    composite_score: float
    step_breakdown: tuple
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "coherence_score": self.coherence_score,
            "factuality_score": self.factuality_score,
            "efficiency_score": self.efficiency_score,
            "energy_awareness": self.energy_awareness,
            "composite_score": self.composite_score,
            "step_breakdown": [dict(s) for s in self.step_breakdown],
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReasoningAnalysis":
        if not isinstance(data, Mapping):
            raise ReasoningError(
                f"ReasoningAnalysis.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        return cls(
            coherence_score=float(data.get("coherence_score", 0.0)),
            factuality_score=float(data.get("factuality_score", 0.0)),
            efficiency_score=float(data.get("efficiency_score", 0.0)),
            energy_awareness=float(data.get("energy_awareness", 0.0)),
            composite_score=float(data.get("composite_score", 0.0)),
            step_breakdown=tuple(
                dict(s) for s in data.get("step_breakdown", [])
            ),
            timestamp=float(data.get("timestamp", time.time())),
        )


# --------------------------------------------------------------------------- #
# Analyzer
# --------------------------------------------------------------------------- #
class ReasoningTraceAnalyzer:
    """
    Analyzes agent reasoning for quality metrics.

    Thread-safe, serializable, and bounded in memory. All original public
    methods are preserved; new parameters are keyword-only.
    """

    def __init__(
        self,
        *,
        config: Optional[ReasoningConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or ReasoningConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._history: Deque[ReasoningAnalysis] = deque(
            maxlen=self._config.max_history
        )

        logger.debug(
            "ReasoningTraceAnalyzer initialized (max_history=%d, strict=%s)",
            self._config.max_history,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> ReasoningConfig:
        return self._config

    @property
    def history(self) -> List[ReasoningAnalysis]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- public API
    def analyze(self, trace: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
        """
        Analyze a reasoning trace.

        Parameters
        ----------
        trace : sequence of Mapping
            Each step should contain a ``thought`` string (optional).

        Returns
        -------
        dict with keys:
            ``coherence_score``, ``factuality_score``, ``efficiency_score``,
            ``energy_awareness``, ``composite_score``, ``step_breakdown``.
        """
        if not isinstance(trace, (list, tuple)):
            msg = f"trace must be a list or tuple, got {type(trace).__name__}."
            if self._strict:
                raise ReasoningError(msg)
            logger.warning("%s Using empty trace.", msg)
            trace = []

        # Filter to mappings only.
        steps: List[Mapping[str, Any]] = []
        for i, s in enumerate(trace):
            if isinstance(s, Mapping):
                steps.append(s)
            else:
                if self._strict:
                    raise ReasoningError(
                        f"trace[{i}] must be a Mapping, got {type(s).__name__}."
                    )
                logger.debug("Skipping non-mapping step at index %d.", i)

        analysis = self._analyze_steps(steps)

        with self._lock:
            self._history.append(analysis)

        logger.debug(
            "Analyzed %d step(s): composite=%.3f (coherence=%.2f, "
            "factuality=%.2f, efficiency=%.2f, energy=%.2f)",
            len(steps),
            analysis.composite_score,
            analysis.coherence_score,
            analysis.factuality_score,
            analysis.efficiency_score,
            analysis.energy_awareness,
        )
        return analysis.to_dict()

    def _analyze_steps(
        self, steps: List[Mapping[str, Any]]
    ) -> ReasoningAnalysis:
        if not steps:
            return ReasoningAnalysis(
                coherence_score=0.0,
                factuality_score=0.0,
                efficiency_score=0.0,
                energy_awareness=0.0,
                composite_score=0.0,
                step_breakdown=tuple(),
            )

        energy_awareness = self._assess_energy_awareness(steps)
        coherence = self._assess_coherence(steps)
        factuality = self._assess_factuality(steps)
        efficiency = self._assess_efficiency(steps)

        cfg = self._config
        composite = (
            cfg.weight_coherence * coherence
            + cfg.weight_factuality * factuality
            + cfg.weight_efficiency * efficiency
            + cfg.weight_energy_awareness * energy_awareness
        )

        breakdown = tuple(
            {
                "index": i,
                "length": len(str(step.get("thought", ""))),
                "energy_aware": any(
                    kw in str(step.get("thought", "")).lower()
                    for kw in cfg.energy_keywords
                ),
            }
            for i, step in enumerate(steps)
        )

        return ReasoningAnalysis(
            coherence_score=coherence,
            factuality_score=factuality,
            efficiency_score=efficiency,
            energy_awareness=energy_awareness,
            composite_score=max(0.0, min(1.0, composite)),
            step_breakdown=breakdown,
        )

    # ---------------------------------------------------------- sub-scores
    def _assess_energy_awareness(
        self, trace: Sequence[Mapping[str, Any]]
    ) -> float:
        """
        Check whether the agent considered energy impact in its decisions.

        Fixed: previously divided by ``len(trace)`` without a zero-guard.
        """
        if not trace:
            return 0.0
        keywords = self._config.energy_keywords
        mentions = sum(
            1
            for step in trace
            if any(
                kw in str(step.get("thought", "")).lower()
                for kw in keywords
            )
        )
        return min(mentions / len(trace), 1.0)

    def _assess_coherence(self, trace: Sequence[Mapping[str, Any]]) -> float:
        """Fraction of steps containing a connective keyword."""
        if not trace:
            return 0.0
        keywords = self._config.coherence_keywords
        hits = sum(
            1
            for step in trace
            if any(
                kw in str(step.get("thought", "")).lower()
                for kw in keywords
            )
        )
        return min(hits / len(trace), 1.0)

    def _assess_factuality(self, trace: Sequence[Mapping[str, Any]]) -> float:
        """
        Placeholder factuality score: penalize empty thoughts.

        Real implementation would cross-check against a knowledge base.
        """
        if not trace:
            return 0.0
        non_empty = sum(
            1 for step in trace if str(step.get("thought", "")).strip()
        )
        return non_empty / len(trace)

    def _assess_efficiency(self, trace: Sequence[Mapping[str, Any]]) -> float:
        """
        Efficiency score = shorter traces are more efficient.

        Uses a soft ceiling of 20 steps: ``min(20/len, 1.0)``.
        """
        if not trace:
            return 0.0
        return min(20.0 / len(trace), 1.0)

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the analysis history."""
        with self._lock:
            history = list(self._history)
        if not history:
            return {
                "analyses": 0,
                "mean_composite": None,
                "mean_coherence": None,
                "mean_factuality": None,
                "mean_efficiency": None,
                "mean_energy_awareness": None,
            }
        return {
            "analyses": len(history),
            "mean_composite": sum(a.composite_score for a in history) / len(history),
            "mean_coherence": sum(a.coherence_score for a in history) / len(history),
            "mean_factuality": sum(a.factuality_score for a in history) / len(history),
            "mean_efficiency": sum(a.efficiency_score for a in history) / len(history),
            "mean_energy_awareness": sum(
                a.energy_awareness for a in history
            ) / len(history),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset internal state; optionally clear the analysis history."""
        with self._lock:
            if clear_history:
                self._history.clear()
        logger.debug("ReasoningTraceAnalyzer reset (clear_history=%s)", clear_history)

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "strict": self._strict,
                "history": [a.to_dict() for a in self._history],
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReasoningTraceAnalyzer":
        if not isinstance(data, Mapping):
            raise ReasoningError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = ReasoningConfig(
            energy_keywords=tuple(
                cfg_data.get(
                    "energy_keywords",
                    (
                        "energy", "efficient", "optimize",
                        "carbon", "sustainable",
                        "efficiency", "green", "emissions",
                    ),
                )
            ),
            coherence_keywords=tuple(
                cfg_data.get(
                    "coherence_keywords",
                    ("because", "therefore", "thus", "hence", "consequently", "so"),
                )
            ),
            max_history=int(cfg_data.get("max_history", 1000)),
            weight_coherence=float(cfg_data.get("weight_coherence", 0.3)),
            weight_factuality=float(cfg_data.get("weight_factuality", 0.4)),
            weight_efficiency=float(cfg_data.get("weight_efficiency", 0.2)),
            weight_energy_awareness=float(
                cfg_data.get("weight_energy_awareness", 0.1)
            ),
        )
        analyzer = cls(config=cfg, strict=bool(data.get("strict", True)))
        with analyzer._lock:
            for entry in data.get("history", []):
                analyzer._history.append(
                    ReasoningAnalysis.from_dict(entry)
                )
        return analyzer

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "ReasoningTraceAnalyzer":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise ReasoningError(f"Invalid JSON payload: {exc}") from exc

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "ReasoningTraceAnalyzer("
                f"analyses={len(self._history)}, "
                f"strict={self._strict})"
            )


__all__ = [
    "ReasoningAnalysis",
    "ReasoningConfig",
    "ReasoningError",
    "ReasoningTraceAnalyzer",
]


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    analyzer = ReasoningTraceAnalyzer()
    print("repr       :", analyzer)

    # Empty trace (previously raised ZeroDivisionError).
    empty = analyzer.analyze([])
    print("Empty      :", empty["composite_score"])

    # Short, energy-aware trace.
    good = analyzer.analyze([
        {"thought": "Because the task is simple, I will use the smallest model to save energy."},
        {"thought": "Therefore the carbon footprint is minimized."},
        {"thought": "Thus we optimize for sustainability."},
    ])
    print("Good trace :", {
        "coherence": good["coherence_score"],
        "energy": good["energy_awareness"],
        "composite": good["composite_score"],
    })

    # Long, non-energy-aware trace.
    bad = analyzer.analyze([{"thought": f"step {i}"} for i in range(25)])
    print("Bad trace  :", {
        "coherence": bad["coherence_score"],
        "energy": bad["energy_awareness"],
        "efficiency": bad["efficiency_score"],
    })

    print("Stats      :", analyzer.statistics())

    # Serialization round-trip.
    payload = analyzer.to_json()
    restored = ReasoningTraceAnalyzer.from_json(payload)
    assert restored.to_dict() == analyzer.to_dict()
    print("Round-trip OK.")

    # Validation failure.
    try:
        analyzer.analyze("not-a-list")  # type: ignore[arg-type]
    except ReasoningError as exc:
        print("Rejected   :", exc)
