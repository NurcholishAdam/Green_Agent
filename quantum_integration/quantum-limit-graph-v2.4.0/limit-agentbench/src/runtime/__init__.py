# src/runtime/__init__.py

"""
Runtime execution modules for different agent frameworks.

This package provides framework-specific runtime adapters that simulate
agent execution to measure accuracy, tool-call counts, conversation depth,
and related resource metrics. Each runtime exposes a uniform interface:

- ``init(config)``      — configure the runtime.
- ``run(query)``        — simulate execution and return a metrics dict.
- ``reduce_tool_calls()``  — reduce tool-call intensity (for throttling).
- ``shorten_context()``    — trim the context window (for token reduction).
- ``finalize()``           — release resources / finalize the session.

Enhancements
------------
- Registry pattern (``RUNTIME_REGISTRY`` + ``register_runtime_component`` /
  ``get_runtime_component`` / ``list_runtime_components``) consistent with
  the rest of the upgraded ``src/`` tree.
- Availability manifest (``RUNTIME_AVAILABILITY``).
- Optional import-time validation via ``GREEN_AGENT_VALIDATE_RUNTIME=1``.
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
RUNTIME_AVAILABILITY: Dict[str, bool] = {}

# ---- AutoGen runtime --------------------------------------------------------
try:
    from .autogen_runtime import (
        AutoGenRuntime,
        AutoGenRuntimeConfig,
        AutoGenRuntimeError,
        AutoGenRunResult,
    )

    RUNTIME_AVAILABILITY["autogen_runtime"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("autogen_runtime unavailable: %s", exc)
    AutoGenRuntime = None  # type: ignore[assignment,misc]
    AutoGenRuntimeConfig = None  # type: ignore[assignment,misc]
    AutoGenRuntimeError = None  # type: ignore[assignment,misc]
    AutoGenRunResult = None  # type: ignore[assignment,misc]
    RUNTIME_AVAILABILITY["autogen_runtime"] = False

# ---- LangChain runtime ------------------------------------------------------
try:
    from .langchain_runtime import (
        LangChainRuntime,
        LangChainRuntimeConfig,
        LangChainRuntimeError,
        LangChainRunResult,
    )

    RUNTIME_AVAILABILITY["langchain_runtime"] = True
except ImportError as exc:  # pragma: no cover — defensive
    logger.warning("langchain_runtime unavailable: %s", exc)
    LangChainRuntime = None  # type: ignore[assignment,misc]
    LangChainRuntimeConfig = None  # type: ignore[assignment,misc]
    LangChainRuntimeError = None  # type: ignore[assignment,misc]
    LangChainRunResult = None  # type: ignore[assignment,misc]
    RUNTIME_AVAILABILITY["langchain_runtime"] = False

# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "__version__",
    # AutoGen
    "AutoGenRuntime",
    "AutoGenRuntimeConfig",
    "AutoGenRuntimeError",
    "AutoGenRunResult",
    # LangChain
    "LangChainRuntime",
    "LangChainRuntimeConfig",
    "LangChainRuntimeError",
    "LangChainRunResult",
    # Registry
    "RUNTIME_REGISTRY",
    "RUNTIME_AVAILABILITY",
    "get_runtime_component",
    "list_runtime_components",
    "register_runtime_component",
]

# --------------------------------------------------------------------------- #
# Component registry
# --------------------------------------------------------------------------- #
RUNTIME_REGISTRY: Dict[str, Type[Any]] = {}


def _register_if_available(name: str, cls: Any) -> None:
    if cls is not None and isinstance(cls, type):
        RUNTIME_REGISTRY[name] = cls


_register_if_available("autogen", AutoGenRuntime)
_register_if_available("langchain", LangChainRuntime)

def register_runtime_component(name: str, cls: Type[Any]) -> None:
    """
    Register a runtime component class under ``name``.

    Raises
    ------
    ValueError
        If ``name`` is empty or already registered to a different class.
    TypeError
        If ``cls`` is not a class.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("Component name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    existing = RUNTIME_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"Runtime component '{name}' already registered to "
            f"{existing.__name__}."
        )
    RUNTIME_REGISTRY[name] = cls
    logger.debug("Registered runtime component '%s' -> %s", name, cls.__name__)

def get_runtime_component(name: str) -> Type[Any]:
    """Return the runtime component class registered under ``name``."""
    try:
        return RUNTIME_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown runtime component '{name}'. "
            f"Available: {sorted(RUNTIME_REGISTRY)}"
        ) from exc

def list_runtime_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {n: c.__name__ for n, c in RUNTIME_REGISTRY.items()}

# --------------------------------------------------------------------------- #
# Optional import-time validation
# --------------------------------------------------------------------------- #
if _os.environ.get("GREEN_AGENT_VALIDATE_RUNTIME") == "1":
    _missing = [k for k, ok in RUNTIME_AVAILABILITY.items() if not ok]
    if _missing:
        logger.warning(
            "Runtime sub-modules unavailable at import time: %s",
            sorted(_missing),
        )
    for _name, _cls in list(RUNTIME_REGISTRY.items()):
        try:
            if not callable(_cls):
                raise TypeError(f"{_cls!r} is not callable.")
            logger.debug(
                "Runtime component '%s' (%s) OK.",
                _name, _cls.__name__,
            )
        except Exception:  # pragma: no cover — CI-only diagnostic
            logger.exception(
                "Runtime component '%s' (%s) failed validation.",
                _name, _cls.__name__,
            )
            raise
