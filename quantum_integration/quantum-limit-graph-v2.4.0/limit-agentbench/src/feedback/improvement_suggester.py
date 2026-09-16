# src/feedback/improvement_suggester.py

"""
Improvement Suggester
=====================

Generates actionable sustainability feedback for agents.

Enhancements
------------
- Fixed the critical bug where ``self.baseline`` and ``self.carbon_threshold``
  were referenced but never defined (previously ``AttributeError`` on every call).
- ``SuggesterConfig`` centralizes baseline, carbon threshold, and energy ratio.
- ``RLock``-guarded suggestion history with bounded ring-buffer.
- Full validation of ``analysis`` / ``result`` mappings.
- Custom :class:`SuggesterError`; strict / non-strict modes.
- Serialization (``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``).
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
from typing import Any, Deque, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SuggesterError(ValueError):
    """Raised for invalid inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SuggesterConfig:
    """Tunable parameters for the improvement suggester."""

    baseline_kwh: float = 0.0025             # baseline energy per task
    carbon_threshold_kg: float = 0.001        # carbon threshold per task
    energy_ratio_trigger: float = 1.5         # multiplier over baseline
    max_history: int = 1000

    def __post_init__(self) -> None:
        if self.baseline_kwh <= 0:
            raise SuggesterError("baseline_kwh must be > 0.")
        if self.carbon_threshold_kg < 0:
            raise SuggesterError("carbon_threshold_kg must be >= 0.")
        if self.energy_ratio_trigger <= 1.0:
            raise SuggesterError("energy_ratio_trigger must be > 1.0.")
        if self.max_history <= 0:
            raise SuggesterError("max_history must be > 0.")


# --------------------------------------------------------------------------- #
# Suggestion record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ImprovementSuggestion:
    """Immutable record of a generated suggestion."""

    category: str          # "energy" | "carbon" | "latency"
    text: str
    value: float
    threshold: float
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ImprovementSuggestion":
        if not isinstance(data, Mapping):
            raise SuggesterError(
                f"ImprovementSuggestion.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        return cls(
            category=str(data["category"]),
            text=str(data["text"]),
            value=float(data.get("value", 0.0)),
            threshold=float(data.get("threshold", 0.0)),
            timestamp=float(data.get("timestamp", time.time())),
        )


# --------------------------------------------------------------------------- #
# Suggester
# --------------------------------------------------------------------------- #
class SustainabilityImprovementSuggester:
    """
    Generates actionable sustainability feedback for agents.

    Thread-safe, serializable, and bounded in memory. The original
    ``generate(analysis, result)`` signature is preserved; new parameters
    are keyword-only.
    """

    def __init__(
        self,
        baseline: float = 0.0025,
        carbon_threshold: float = 0.001,
        *,
        config: Optional[SuggesterConfig] = None,
        strict: bool = True,
    ) -> None:
        """
        Parameters
        ----------
        baseline : float
            Baseline energy per task (kWh). Preserved for backward compatibility.
        carbon_threshold : float
            Carbon threshold per task (kg CO2e). Preserved for backward
            compatibility.
        config : SuggesterConfig, optional
            Typed configuration. Takes precedence over the two legacy params.
        strict : bool, default True
            If True, invalid inputs raise :class:`SuggesterError`.
        """
        if config is not None:
            self._config = config
        else:
            if not isinstance(baseline, (int, float)) or baseline <= 0:
                raise SuggesterError("baseline must be a positive number.")
            if not isinstance(carbon_threshold, (int, float)) or carbon_threshold < 0:
                raise SuggesterError(
                    "carbon_threshold must be a non-negative number."
                )
            self._config = SuggesterConfig(
                baseline_kwh=float(baseline),
                carbon_threshold_kg=float(carbon_threshold),
            )

        self._strict: bool = bool(strict)

        # Legacy attributes preserved — this is the bug fix: they now exist.
        self.baseline: float = self._config.baseline_kwh
        self.carbon_threshold: float = self._config.carbon_threshold_kg

        self._lock = threading.RLock()
        self._history: Deque[ImprovementSuggestion] = deque(
            maxlen=self._config.max_history
        )

        logger.debug(
            "SustainabilityImprovementSuggester initialized "
            "(baseline=%.4f kWh, carbon_threshold=%.4f kg, strict=%s)",
            self.baseline,
            self.carbon_threshold,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> SuggesterConfig:
        return self._config

    @property
    def history(self) -> List[ImprovementSuggestion]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- public API
    def generate(
        self,
        analysis: Mapping[str, Any],
        result: Mapping[str, Any],
        *,
        record: bool = True,
    ) -> List[str]:
        """
        Generate sustainability-focused improvement suggestions.

        Parameters
        ----------
        analysis : Mapping
            Analysis output (e.g. from :class:`ReasoningTraceAnalyzer`). Not
            required to contain specific fields; reserved for future use.
        result : Mapping
            Must contain ``energy_kwh`` and ``carbon_co2e_kg`` (numeric).

        Returns
        -------
        list[str]
            Human-readable suggestions (legacy format).
        """
        records = self.generate_structured(analysis, result, record=record)
        return [r.text for r in records]

    def generate_structured(
        self,
        analysis: Mapping[str, Any],
        result: Mapping[str, Any],
        *,
        record: bool = True,
    ) -> List[ImprovementSuggestion]:
        """Same as :meth:`generate` but returns structured records."""
        if not isinstance(result, Mapping):
            raise SuggesterError(
                f"result must be a Mapping, got {type(result).__name__}."
            )
        if analysis is not None and not isinstance(analysis, Mapping):
            msg = f"analysis must be a Mapping, got {type(analysis).__name__}."
            if self._strict:
                raise SuggesterError(msg)
            logger.warning("%s Ignoring.", msg)
            analysis = {}

        energy = self._coerce_number(result, "energy_kwh", default=0.0)
        carbon = self._coerce_number(result, "carbon_co2e_kg", default=0.0)

        suggestions: List[ImprovementSuggestion] = []

        # Energy efficiency suggestions.
        if energy > self.baseline * self._config.energy_ratio_trigger:
            text = (
                f"⚡ Energy usage {energy:.4f} kWh is "
                f"{(energy / self.baseline - 1) * 100:.0f}% above baseline "
                f"{self.baseline:.4f} kWh. Consider:\n"
                f"  • Reducing inference calls\n"
                f"  • Using smaller models for subtasks\n"
                f"  • Caching repeated computations"
            )
            suggestions.append(
                ImprovementSuggestion(
                    category="energy",
                    text=text,
                    value=energy,
                    threshold=self.baseline * self._config.energy_ratio_trigger,
                )
            )

        # Carbon optimization suggestions.
        if carbon > self.carbon_threshold:
            text = (
                f"🌱 Carbon emissions {carbon:.4f} kg CO2e exceed "
                f"threshold {self.carbon_threshold:.4f} kg. Try:\n"
                f"  • Scheduling tasks during low-carbon hours\n"
                f"  • Using more efficient hardware profiles\n"
                f"  • Optimizing model selection"
            )
            suggestions.append(
                ImprovementSuggestion(
                    category="carbon",
                    text=text,
                    value=carbon,
                    threshold=self.carbon_threshold,
                )
            )

        if record:
            with self._lock:
                for s in suggestions:
                    self._history.append(s)

        logger.debug(
            "Generated %d suggestion(s) (energy=%.4f, carbon=%.4f).",
            len(suggestions), energy, carbon,
        )
        return suggestions

    # ---------------------------------------------------------- helpers
    @staticmethod
    def _coerce_number(
        mapping: Mapping[str, Any], key: str, *, default: float
    ) -> float:
        value = mapping.get(key, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return default
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default
        return f

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the suggestion history."""
        with self._lock:
            history = list(self._history)
        if not history:
            return {
                "suggestions": 0,
                "by_category": {},
                "baseline_kwh": self.baseline,
                "carbon_threshold_kg": self.carbon_threshold,
            }
        by_category: Dict[str, int] = {}
        for s in history:
            by_category[s.category] = by_category.get(s.category, 0) + 1
        return {
            "suggestions": len(history),
            "by_category": by_category,
            "baseline_kwh": self.baseline,
            "carbon_threshold_kg": self.carbon_threshold,
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset internal state; optionally clear the suggestion history."""
        with self._lock:
            if clear_history:
                self._history.clear()
        logger.debug("Suggester reset (clear_history=%s)", clear_history)

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "strict": self._strict,
                "history": [s.to_dict() for s in self._history],
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SustainabilityImprovementSuggester":
        if not isinstance(data, Mapping):
            raise SuggesterError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = SuggesterConfig(
            baseline_kwh=float(cfg_data.get("baseline_kwh", 0.0025)),
            carbon_threshold_kg=float(cfg_data.get("carbon_threshold_kg", 0.001)),
            energy_ratio_trigger=float(cfg_data.get("energy_ratio_trigger", 1.5)),
            max_history=int(cfg_data.get("max_history", 1000)),
        )
        suggester = cls(config=cfg, strict=bool(data.get("strict", True)))
        with suggester._lock:
            for entry in data.get("history", []):
                suggester._history.append(
                    ImprovementSuggestion.from_dict(entry)
                )
        return suggester

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "SustainabilityImprovementSuggester":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise SuggesterError(f"Invalid JSON payload: {exc}") from exc

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "SustainabilityImprovementSuggester("
                f"baseline={self.baseline:.4f} kWh, "
                f"carbon_threshold={self.carbon_threshold:.4f} kg, "
                f"suggestions={len(self._history)})"
            )


__all__ = [
    "ImprovementSuggestion",
    "SuggesterConfig",
    "SuggesterError",
    "SustainabilityImprovementSuggester",
]


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    suggester = SustainabilityImprovementSuggester()
    print("repr       :", suggester)

    # Efficient result → no suggestions.
    s0 = suggester.generate({}, {"energy_kwh": 0.001, "carbon_co2e_kg": 0.0005})
    print("Efficient  :", s0)

    # High energy + high carbon → two suggestions.
    s1 = suggester.generate(
        {}, {"energy_kwh": 0.01, "carbon_co2e_kg": 0.005}
    )
    for s in s1:
        print("Suggestion :", s.splitlines()[0])

    print("Stats      :", suggester.statistics())

    # Serialization round-trip.
    payload = suggester.to_json()
    restored = SustainabilityImprovementSuggester.from_json(payload)
    assert restored.to_dict() == suggester.to_dict()
    print("Round-trip OK.")

    # Validation failure.
    try:
        suggester.generate({}, None)  # type: ignore[arg-type]
    except SuggesterError as exc:
        print("Rejected   :", exc)
