# src/symbolic/__init__.py

"""
Symbolic Reasoning Module for Green Agent.

Neuro-symbolic oversight inspired by the FormalJudge paradigm.

Enhancements
------------
- Registry pattern (``SYMBOLIC_REGISTRY`` + ``register_symbolic_component`` /
  ``get_symbolic_component`` / ``list_symbolic_components``) consistent with
  the rest of the upgraded ``src/`` tree.
- Availability manifest (``SYMBOLIC_AVAILABILITY``).
- Optional import-time validation via ``GREEN_AGENT_VALIDATE_SYMBOLIC=1``.
- ``__version__`` constant.
"""

from __future__ import annotations

import logging
import os as _os
from typing import Any, Dict, Type

logger = logging.getLogger(__name__)

__version__ = "2.4.0"

# --------------------------------------------------------------------------- #
# Guarded sub-module imports
# --------------------------------------------------------------------------- #
SYMBOLIC_AVAILABILITY: Dict[str, bool] = {}

try:
    from .symbolic_reasoning_engine import (
        SymbolicReasoningEngine,
        SymbolicReasoningConfig,
        SymbolicReasoningError,
        SymbolicRule,
        ViolationTrace,
    )

    SYMBOLIC_AVAILABILITY["symbolic_reasoning_engine"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("symbolic_reasoning_engine unavailable: %s", exc)
    SymbolicReasoningEngine = None  # type: ignore[assignment,misc]
    SymbolicReasoningConfig = None  # type: ignore[assignment,misc]
    SymbolicReasoningError = None  # type: ignore[assignment,misc]
    SymbolicRule = None  # type: ignore[assignment,misc]
    ViolationTrace = None  # type: ignore[assignment,misc]
    SYMBOLIC_AVAILABILITY["symbolic_reasoning_engine"] = False

# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "__version__",
    "SymbolicReasoningEngine",
    "SymbolicReasoningConfig",
    "SymbolicReasoningError",
    "SymbolicRule",
    "ViolationTrace",
    "SYMBOLIC_REGISTRY",
    "SYMBOLIC_AVAILABILITY",
    "get_symbolic_component",
    "list_symbolic_components",
    "register_symbolic_component",
]

# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
SYMBOLIC_REGISTRY: Dict[str, Type[Any]] = {}


def _register_if_available(name: str, cls: Any) -> None:
    if cls is not None and isinstance(cls, type):
        SYMBOLIC_REGISTRY[name] = cls


_register_if_available("engine", SymbolicReasoningEngine)
_register_if_available("rule", SymbolicRule)
_register_if_available("violation", ViolationTrace)


def register_symbolic_component(name: str, cls: Type[Any]) -> None:
    """Register a symbolic component class under ``name``."""
    if not isinstance(name, str) or not name:
        raise ValueError("Component name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    existing = SYMBOLIC_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"Symbolic component '{name}' already registered to "
            f"{existing.__name__}."
        )
    SYMBOLIC_REGISTRY[name] = cls
    logger.debug("Registered symbolic component '%s' -> %s", name, cls.__name__)


def get_symbolic_component(name: str) -> Type[Any]:
    """Return the symbolic component class registered under ``name``."""
    try:
        return SYMBOLIC_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown symbolic component '{name}'. "
            f"Available: {sorted(SYMBOLIC_REGISTRY)}"
        ) from exc


def list_symbolic_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {n: c.__name__ for n, c in SYMBOLIC_REGISTRY.items()}


# --------------------------------------------------------------------------- #
# Optional import-time validation
# --------------------------------------------------------------------------- #
if _os.environ.get("GREEN_AGENT_VALIDATE_SYMBOLIC") == "1":
    _missing = [k for k, ok in SYMBOLIC_AVAILABILITY.items() if not ok]
    if _missing:
        logger.warning(
            "Symbolic sub-modules unavailable at import time: %s",
            sorted(_missing),
        )
    for _name, _cls in list(SYMBOLIC_REGISTRY.items()):
        try:
            if not callable(_cls):
                raise TypeError(f"{_cls!r} is not callable.")
            logger.debug(
                "Symbolic component '%s' (%s) OK.",
                _name, _cls.__name__,
            )
        except Exception:  # pragma: no cover — CI-only diagnostic
            logger.exception(
                "Symbolic component '%s' (%s) failed validation.",
                _name, _cls.__name__,
            )
            raise
