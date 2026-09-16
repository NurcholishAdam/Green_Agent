# src/feedback/energy_feedback.py

"""
Energy Feedback
===============

Human-readable feedback based on energy efficiency.

Enhancements
------------
- ``EnergyFeedbackConfig`` with validated thresholds (no more magic numbers).
- Structured :class:`EnergyFeedback` return value alongside the legacy string.
- Full validation of the input ``metrics`` mapping.
- Strict / non-strict modes; custom :class:`EnergyFeedbackError`.
- ``__repr__`` on the structured result; ``__main__`` smoke test.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class EnergyFeedbackError(ValueError):
    """Raised for invalid inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class EnergyFeedbackConfig:
    """Tunable thresholds for energy feedback generation."""

    high_energy_threshold: float = 0.1     # kWh
    high_latency_threshold: float = 2.0    # seconds
    high_memory_threshold: float = 1024.0  # MB

    def __post_init__(self) -> None:
        for name in (
            "high_energy_threshold",
            "high_latency_threshold",
            "high_memory_threshold",
        ):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or value < 0:
                raise EnergyFeedbackError(f"{name} must be >= 0 (got {value}).")


# --------------------------------------------------------------------------- #
# Structured result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class EnergyFeedback:
    """Structured energy feedback result."""

    messages: tuple
    is_efficient: bool
    energy: float
    latency: float
    memory: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "messages": list(self.messages),
            "is_efficient": self.is_efficient,
            "energy": self.energy,
            "latency": self.latency,
            "memory": self.memory,
        }

    def to_text(self) -> str:
        """Return the human-readable feedback string (legacy format)."""
        if not self.messages:
            return "Run is efficient and within green thresholds."
        return " ".join(self.messages)

    def __repr__(self) -> str:
        return (
            "EnergyFeedback("
            f"efficient={self.is_efficient}, "
            f"energy={self.energy:.4f}, latency={self.latency:.2f}, "
            f"memory={self.memory:.1f})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def _extract_number(
    metrics: Mapping[str, Any], key: str, default: float
) -> float:
    """Return ``metrics[key]`` as a finite float, or ``default``."""
    value = metrics.get(key, default)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        logger.debug("metrics['%s'] is not numeric; using default %.2f.", key, default)
        return default
    f = float(value)
    if math.isnan(f) or math.isinf(f):
        logger.debug("metrics['%s'] is non-finite; using default %.2f.", key, default)
        return default
    return f


def analyze_energy_metrics(
    metrics: Mapping[str, Any],
    *,
    config: Optional[EnergyFeedbackConfig] = None,
    strict: bool = True,
) -> EnergyFeedback:
    """
    Build a structured :class:`EnergyFeedback` from a metrics mapping.

    Parameters
    ----------
    metrics : Mapping
        Must contain (optionally) ``energy``, ``latency``, and ``memory``.
        Missing / non-numeric values default to ``0.0``.
    config : EnergyFeedbackConfig, optional
        Thresholds. Defaults to ``EnergyFeedbackConfig()``.
    strict : bool, default True
        If True, a non-Mapping ``metrics`` raises :class:`EnergyFeedbackError`.
    """
    cfg = config or EnergyFeedbackConfig()
    if not isinstance(metrics, Mapping):
        msg = f"metrics must be a Mapping, got {type(metrics).__name__}."
        if strict:
            raise EnergyFeedbackError(msg)
        logger.warning("%s Using empty mapping.", msg)
        metrics = {}

    energy = _extract_number(metrics, "energy", 0.0)
    latency = _extract_number(metrics, "latency", 0.0)
    memory = _extract_number(metrics, "memory", 0.0)

    messages: List[str] = []
    if energy > cfg.high_energy_threshold:
        messages.append("High energy usage detected.")
    if latency > cfg.high_latency_threshold:
        messages.append("Latency may impact user experience.")
    if memory > cfg.high_memory_threshold:
        messages.append("Large memory footprint observed.")

    return EnergyFeedback(
        messages=tuple(messages),
        is_efficient=not messages,
        energy=energy,
        latency=latency,
        memory=memory,
    )


def generate_energy_feedback(
    metrics: Mapping[str, Any],
    *,
    config: Optional[EnergyFeedbackConfig] = None,
    strict: bool = True,
) -> str:
    """
    Return a human-readable energy feedback string (backward-compatible).

    Parameters match :func:`analyze_energy_metrics`.
    """
    result = analyze_energy_metrics(metrics, config=config, strict=strict)
    text = result.to_text()
    logger.debug("Energy feedback: %s", text)
    return text


__all__ = [
    "EnergyFeedback",
    "EnergyFeedbackConfig",
    "EnergyFeedbackError",
    "analyze_energy_metrics",
    "generate_energy_feedback",
]


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    efficient = generate_energy_feedback({"energy": 0.05, "latency": 1.0, "memory": 512})
    print("Efficient  :", efficient)

    inefficient = generate_energy_feedback(
        {"energy": 0.5, "latency": 5.0, "memory": 2048}
    )
    print("Inefficient:", inefficient)

    structured = analyze_energy_metrics({"energy": 0.5, "latency": 5.0, "memory": 2048})
    print("Structured :", structured)
    print("Dict       :", structured.to_dict())

    # Non-strict mode tolerates garbage.
    print("Lenient    :", generate_energy_feedback(None, strict=False))  # type: ignore[arg-type]

    # Validation failure.
    try:
        generate_energy_feedback(None)  # type: ignore[arg-type]
    except EnergyFeedbackError as exc:
        print("Rejected   :", exc)
