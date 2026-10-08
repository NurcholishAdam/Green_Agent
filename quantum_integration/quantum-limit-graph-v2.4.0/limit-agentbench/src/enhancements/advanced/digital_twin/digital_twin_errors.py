# src/quantum_integration/digital_twin/digital_twin_errors.py

"""Structured error hierarchy for the digital-twin package."""

from __future__ import annotations


class DigitalTwinError(ValueError):
    """Base class for digital-twin problems."""


class DigitalTwinInputError(DigitalTwinError):
    """Invalid input to a public API."""


class DigitalTwinConfigError(DigitalTwinError):
    """Invalid configuration."""


class DigitalTwinPersistenceError(DigitalTwinError):
    """Disk I/O failure."""


class DigitalTwinSimulationError(DigitalTwinError):
    """Simulation failed or produced invalid output."""


class DigitalTwinCircuitOpenError(DigitalTwinError):
    """Circuit breaker is open."""


class DigitalTwinParseError(DigitalTwinError):
    """Failed to parse a payload from dict/JSON."""


__all__ = [
    "DigitalTwinError",
    "DigitalTwinInputError",
    "DigitalTwinConfigError",
    "DigitalTwinPersistenceError",
    "DigitalTwinSimulationError",
    "DigitalTwinCircuitOpenError",
    "DigitalTwinParseError",
]
