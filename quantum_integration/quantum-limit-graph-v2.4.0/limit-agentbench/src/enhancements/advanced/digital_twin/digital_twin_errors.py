# src/quantum_integration/digital_twin/digital_twin_errors.py

"""Structured error hierarchy for the digital-twin package.

Every error carries:

* an optional structured ``context`` mapping (for logging/metrics),
* a stable ``code`` class attribute (for machine consumers),
* standard Python semantic mixins (``ValueError``, ``OSError``,
  ``RuntimeError``, ``LookupError``, ``TimeoutError``,
  ``NotImplementedError``) so callers can catch by either the digital-twin
  hierarchy or the conventional built-in category.

Callers are encouraged to preserve the original cause when wrapping:

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise DigitalTwinParseError("invalid JSON payload") from exc

Example
-------
    try:
        twin.load(path)
    except DigitalTwinPersistenceError as exc:
        log.error("load failed", extra={"code": exc.code, **exc.context})

    try:
        twin.step()
    except DigitalTwinCircuitOpenError as exc:
        if exc.retry_after is not None:
            time.sleep(exc.retry_after)
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, Optional

__all__ = [
    "DigitalTwinError",
    "DigitalTwinInputError",
    "DigitalTwinConfigError",
    "DigitalTwinPersistenceError",
    "DigitalTwinSimulationError",
    "DigitalTwinCircuitOpenError",
    "DigitalTwinParseError",
    "DigitalTwinNotFoundError",
    "DigitalTwinTimeoutError",
    "DigitalTwinNotImplementedError",
]


# --------------------------------------------------------------------------- #
# Base
# --------------------------------------------------------------------------- #

class DigitalTwinError(Exception):
    """Base class for all digital-twin problems.

    Parameters
    ----------
    message:
        Human-readable description.
    **context:
        Arbitrary structured key/value pairs. They are stored on the
        ``context`` attribute and appended to ``str(exc)`` for readability.
        Values should be simple/serializable (str, int, float, bool, None,
        list, dict) so that pickling and structured logging work reliably.
    """

    code: ClassVar[str] = "digital_twin.error"

    def __init__(self, message: str = "", **context: Any) -> None:
        super().__init__(message)
        self.message: str = message
        self.context: Dict[str, Any] = dict(context)

    def __str__(self) -> str:
        if not self.context:
            return self.message
        kv = ", ".join(f"{k}={v!r}" for k, v in self.context.items())
        return f"{self.message} ({kv})" if self.message else kv

    def __repr__(self) -> str:
        cls = type(self).__name__
        return f"{cls}(code={self.code!r}, message={self.message!r}, context={self.context!r})"

    def __reduce__(self):
        # Preserve ``message`` and ``context`` across pickling
        # (multiprocessing, RPC, cached exception replay in tests).
        return (
            _rebuild_error,
            (type(self), self.message, self.context),
        )


def _rebuild_error(
    cls: type, message: str, context: Dict[str, Any],
) -> DigitalTwinError:
    """Module-level helper so instances pickle cleanly.

    ``DigitalTwinCircuitOpenError.__init__`` accepts ``retry_after`` as a
    keyword; because we always store it in ``context`` on construction,
    re-instantiating with ``**context`` reconstructs it correctly.
    """
    return cls(message, **context)


# --------------------------------------------------------------------------- #
# Input / config
# --------------------------------------------------------------------------- #

class DigitalTwinInputError(DigitalTwinError, ValueError):
    """Invalid input to a public API."""

    code = "digital_twin.input"


class DigitalTwinConfigError(DigitalTwinError, ValueError):
    """Invalid configuration."""

    code = "digital_twin.config"


class DigitalTwinParseError(DigitalTwinError, ValueError):
    """Failed to parse a payload from dict/JSON."""

    code = "digital_twin.parse"


# --------------------------------------------------------------------------- #
# Persistence / lookup
# --------------------------------------------------------------------------- #

class DigitalTwinPersistenceError(DigitalTwinError, OSError):
    """Disk I/O failure."""

    code = "digital_twin.persistence"


class DigitalTwinNotFoundError(DigitalTwinError, LookupError):
    """Requested resource does not exist."""

    code = "digital_twin.not_found"


# --------------------------------------------------------------------------- #
# Runtime / simulation
# --------------------------------------------------------------------------- #

class DigitalTwinSimulationError(DigitalTwinError, RuntimeError):
    """Simulation failed or produced invalid output."""

    code = "digital_twin.simulation"


class DigitalTwinTimeoutError(DigitalTwinSimulationError, TimeoutError):
    """Simulation or upstream call exceeded its deadline."""

    code = "digital_twin.timeout"


class DigitalTwinCircuitOpenError(DigitalTwinError, RuntimeError):
    """Circuit breaker is open.

    Parameters
    ----------
    message:
        Human-readable description.
    retry_after:
        Optional suggested delay (seconds) before retrying. Also stored in
        ``context`` so it survives pickling.
    **context:
        Additional structured key/value pairs.
    """

    code = "digital_twin.circuit_open"

    def __init__(
        self,
        message: str = "Circuit breaker is open.",
        *,
        retry_after: Optional[float] = None,
        **context: Any,
    ) -> None:
        super().__init__(message, retry_after=retry_after, **context)
        self.retry_after: Optional[float] = retry_after


class DigitalTwinNotImplementedError(DigitalTwinError, NotImplementedError):
    """Feature, backend, or action is not supported."""

    code = "digital_twin.not_implemented"
