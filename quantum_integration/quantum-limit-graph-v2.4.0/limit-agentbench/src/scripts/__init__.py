# src/scripts/__init__.py

"""
Scripts for the Green Agent / LIMIT-AgentBench toolchain.

This package hosts standalone CLI utilities that operate on the Green Agent
runtime and its configuration files. Each script is importable as a module
and exposes a ``main(argv=None) -> int`` entry point that returns a process
exit code.

Available scripts
-----------------
- :mod:`scripts.validate_agentbeats` — validate an ``agentbeats.json`` config
  against the AgentBeats schema and verify the referenced Docker image is
  reachable.

Enhancements
------------
- Registry pattern (``SCRIPTS_REGISTRY`` + ``register_script`` /
  ``get_script`` / ``list_scripts``) consistent with the rest of the upgraded
  ``src/`` tree.
- Availability manifest (``SCRIPTS_AVAILABILITY``).
- Optional import-time validation via ``GREEN_AGENT_VALIDATE_SCRIPTS=1``.
- ``__version__`` constant.
"""

from __future__ import annotations

import logging
import os as _os
from typing import Any, Callable, Dict

logger = logging.getLogger(__name__)

__version__ = "2.4.0"

# --------------------------------------------------------------------------- #
# Guarded sub-module imports
# --------------------------------------------------------------------------- #
SCRIPTS_AVAILABILITY: Dict[str, bool] = {}

try:
    from .validate_agentbeats import (
        AgentBeatsValidationError,
        AgentBeatsValidator,
        AgentBeatsValidatorConfig,
        ValidationResult,
        main as validate_agentbeats_main,
    )

    SCRIPTS_AVAILABILITY["validate_agentbeats"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("validate_agentbeats unavailable: %s", exc)
    AgentBeatsValidationError = None  # type: ignore[assignment,misc]
    AgentBeatsValidator = None  # type: ignore[assignment,misc]
    AgentBeatsValidatorConfig = None  # type: ignore[assignment,misc]
    ValidationResult = None  # type: ignore[assignment,misc]
    validate_agentbeats_main = None  # type: ignore[assignment,misc]
    SCRIPTS_AVAILABILITY["validate_agentbeats"] = False


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "__version__",
    "AgentBeatsValidationError",
    "AgentBeatsValidator",
    "AgentBeatsValidatorConfig",
    "ValidationResult",
    "validate_agentbeats_main",
    "SCRIPTS_REGISTRY",
    "SCRIPTS_AVAILABILITY",
    "get_script",
    "list_scripts",
    "register_script",
]


# --------------------------------------------------------------------------- #
# Script registry
# --------------------------------------------------------------------------- #
# Maps a CLI script name to its ``main(argv) -> int`` entry point.
SCRIPTS_REGISTRY: Dict[str, Callable[..., int]] = {}


def _register_if_available(name: str, fn: Any) -> None:
    if callable(fn):
        SCRIPTS_REGISTRY[name] = fn


_register_if_available("validate_agentbeats", validate_agentbeats_main)


def register_script(name: str, entry_point: Callable[..., int]) -> None:
    """
    Register a script entry point under ``name``.

    Raises
    ------
    ValueError
        If ``name`` is empty or already registered to a different callable.
    TypeError
        If ``entry_point`` is not callable.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("Script name must be a non-empty string.")
    if not callable(entry_point):
        raise TypeError(f"entry_point must be callable, got {type(entry_point).__name__}.")
    existing = SCRIPTS_REGISTRY.get(name)
    if existing is not None and existing is not entry_point:
        raise ValueError(
            f"Script '{name}' already registered to "
            f"{getattr(existing, '__name__', existing)!r}."
        )
    SCRIPTS_REGISTRY[name] = entry_point
    logger.debug("Registered script '%s'.", name)


def get_script(name: str) -> Callable[..., int]:
    """Return the entry point registered under ``name``."""
    try:
        return SCRIPTS_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown script '{name}'. "
            f"Available: {sorted(SCRIPTS_REGISTRY)}"
        ) from exc


def list_scripts() -> Dict[str, str]:
    """Return a mapping of script name -> callable name."""
    return {
        n: getattr(fn, "__name__", repr(fn))
        for n, fn in SCRIPTS_REGISTRY.items()
    }


# --------------------------------------------------------------------------- #
# Optional import-time validation
# --------------------------------------------------------------------------- #
if _os.environ.get("GREEN_AGENT_VALIDATE_SCRIPTS") == "1":
    _missing = [k for k, ok in SCRIPTS_AVAILABILITY.items() if not ok]
    if _missing:
        logger.warning(
            "Scripts unavailable at import time: %s", sorted(_missing),
        )
    for _name, _fn in list(SCRIPTS_REGISTRY.items()):
        try:
            if not callable(_fn):
                raise TypeError(f"{_fn!r} is not callable.")
            logger.debug("Script '%s' (%s) OK.", _name, _fn.__name__)
        except Exception:  # pragma: no cover — CI-only diagnostic
            logger.exception("Script '%s' failed validation.", _name)
            raise
