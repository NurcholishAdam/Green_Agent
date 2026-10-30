# src/interpretation/__init__.py

"""
Workload Interpretation & Helium Profiling (v5.1)
=================================================

Converts raw task JSON into structured :class:`WorkloadProfile` objects,
including helium-dependency analysis via :class:`HeliumProfile`.

Fixes in v5.1 (see review)
--------------------------
  * P0 — The env-var "validation" hook was a no-op: it logged the loop
         variable name and wrapped ``logger.info`` in a ``try`` that
         cannot fail. Replaced with ``test_smoke()``, which instantiates
         the interpreter, runs a canonical task, and round-trips the
         result through JSON. The env var still gates the *call*, but
         the call now actually validates.
  * P0 — The registry is no longer append-only. Added ``override=True``
         to ``register_interpretation_component``, a new
         ``unregister_interpretation_component``, and an
         ``override_interpretation_component`` context manager for
         test isolation.
  * P1 — ``get_interpretation_component`` raises ``InterpretationError``
         (was ``KeyError``), matching the rest of the package.
  * P1 — Added ``__version__``, ``MIN_*_VERSION`` constants, and a
         submodule version check.
  * P1 — Exported ``CarbonLevel`` (v5.1 API drift).
  * P2 — Added ``get_capabilities()``, ``health()``, ``test_smoke()``,
         ``__dir__()``.
  * P2 — ``register_interpretation_component`` accepts an optional
         ``expected_base`` for plugin type-checking.
  * P3 — Dropped the ``os as _os`` alias; removed the dead ``_cls``
         loop variable.

All components follow the conventions used across the upgraded ``src/``
tree: frozen config dataclasses, validation in ``__post_init__``, UTC
timestamps, serialization, strict / non-strict modes, custom ``*Error``
exceptions, ``statistics()``, ``__repr__``, and ``__main__`` smoke tests.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Type

__version__ = "5.1.0"

#: Oldest submodule versions this package can safely drive.
MIN_HELIUM_PROFILE_VERSION = "5.1.0"
MIN_WORKLOAD_INTERPRETER_VERSION = "5.1.0"

logger = logging.getLogger(__name__)


# =============================================================================
# Submodule imports + version diagnostics
# =============================================================================

from . import helium_profile as _helium_profile_module
from . import workload_interpreter as _workload_interpreter_module

from .helium_profile import (
    HardwareType,
    HeliumDependencyLevel,
    HeliumProfile,
    HeliumProfileConfig,
    HeliumProfileError,
)

from .workload_interpreter import (
    InterpretationConfig,
    InterpretationError,
    WorkloadInterpreter,
    WorkloadProfile,
)

# CarbonLevel was added in workload_interpreter v5.1. Tolerate the older
# submodule with a clear, actionable error at the use site.
try:
    from .workload_interpreter import CarbonLevel
    _HAS_CARBON_LEVEL = True
except ImportError:  # pragma: no cover — legacy submodule
    CarbonLevel = None  # type: ignore[assignment, misc]
    _HAS_CARBON_LEVEL = False
    logger.warning(
        "CarbonLevel not provided by workload_interpreter (v%s). "
        "Upgrade the submodule to v%s to enable.",
        getattr(_workload_interpreter_module, "__version__", "?"),
        MIN_WORKLOAD_INTERPRETER_VERSION,
    )


def _check_submodule_versions() -> None:
    """Raise if a submodule is older than this package supports."""
    try:
        from packaging.version import InvalidVersion, Version
    except ImportError:
        logger.debug("packaging not installed; skipping version check")
        return
    for mod, minimum, label in (
        (_helium_profile_module, MIN_HELIUM_PROFILE_VERSION,
         "helium_profile"),
        (_workload_interpreter_module, MIN_WORKLOAD_INTERPRETER_VERSION,
         "workload_interpreter"),
    ):
        v = getattr(mod, "__version__", None)
        if not v:
            logger.debug("%s has no __version__; skipping check", label)
            continue
        try:
            installed = Version(v)
            required = Version(minimum)
        except InvalidVersion:
            logger.warning(
                "%s has non-PEP-440 version %r; skipping check",
                label, v,
            )
            continue
        if installed < required:
            raise ImportError(
                f"src.interpretation v{__version__} requires "
                f"{label} >= {minimum}; found {v}."
            )


_check_submodule_versions()


# =============================================================================
# Registry
# =============================================================================

INTERPRETATION_REGISTRY: Dict[str, Type[Any]] = {
    "interpreter": WorkloadInterpreter,
    "helium": HeliumProfile,
    "workload": WorkloadProfile,
}

_MISSING = object()


def register_interpretation_component(
    name: str,
    cls: Type[Any],
    *,
    override: bool = False,
    expected_base: Optional[type] = None,
) -> None:
    """
    Register an interpretation component class under ``name``.

    Args:
        name:          Non-empty string key.
        cls:           A class.
        override:      When True, replace any existing registration. When
                       False (default), registering a *different* class
                       under an existing name raises ``ValueError``;
                       re-registering the *same* class is a no-op.
        expected_base: Optional base class; when provided, ``cls`` must
                       be a subclass.

    Raises:
        TypeError:  ``cls`` isn't a class, or ``expected_base`` isn't
                    matched.
        ValueError: ``name`` is empty, or a conflicting registration
                    exists without ``override``.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    if expected_base is not None and not issubclass(cls, expected_base):
        raise TypeError(
            f"'{name}' must be a subclass of {expected_base.__name__}, "
            f"got {cls.__name__}"
        )

    existing = INTERPRETATION_REGISTRY.get(name, _MISSING)
    if existing is not _MISSING and existing is not cls and not override:
        raise ValueError(
            f"Interpretation component '{name}' already registered to "
            f"{existing.__name__}. Pass override=True to replace."
        )
    INTERPRETATION_REGISTRY[name] = cls
    logger.debug(
        "Registered interpretation component '%s' -> %s",
        name, cls.__name__,
    )


def unregister_interpretation_component(name: str) -> None:
    """
    Remove the registration under ``name``.

    Raises:
        InterpretationError: no such component.
    """
    if name not in INTERPRETATION_REGISTRY:
        raise InterpretationError(
            f"Unknown interpretation component '{name}'. "
            f"Available: {sorted(INTERPRETATION_REGISTRY)}"
        )
    INTERPRETATION_REGISTRY.pop(name)
    logger.debug("Unregistered interpretation component '%s'", name)


@contextmanager
def override_interpretation_component(
    name: str, cls: Type[Any],
) -> Iterator[None]:
    """
    Temporarily override (or add) a registration, restoring the previous
    entry on exit. Intended for test isolation::

        with override_interpretation_component("interpreter", FakeInterp):
            ...  # registry contains FakeInterp
        # registry restored to prior state (or entry removed)
    """
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    prev = INTERPRETATION_REGISTRY.get(name, _MISSING)
    INTERPRETATION_REGISTRY[name] = cls
    try:
        yield
    finally:
        if prev is _MISSING:
            INTERPRETATION_REGISTRY.pop(name, None)
        else:
            INTERPRETATION_REGISTRY[name] = prev  # type: ignore[assignment]


def get_interpretation_component(name: str) -> Type[Any]:
    """
    Return the interpretation component class registered under ``name``.

    Raises:
        InterpretationError: no such component.
    """
    try:
        return INTERPRETATION_REGISTRY[name]
    except KeyError:
        raise InterpretationError(
            f"Unknown interpretation component '{name}'. "
            f"Available: {sorted(INTERPRETATION_REGISTRY)}"
        ) from None


def list_interpretation_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {n: c.__name__ for n, c in INTERPRETATION_REGISTRY.items()}


# =============================================================================
# Diagnostics
# =============================================================================

def get_capabilities() -> Dict[str, Any]:
    """Structured capability report suitable for logs / support tickets."""
    return {
        "package_version": __version__,
        "helium_profile_version": getattr(
            _helium_profile_module, "__version__", None,
        ),
        "workload_interpreter_version": getattr(
            _workload_interpreter_module, "__version__", None,
        ),
        "components": list_interpretation_components(),
        "component_count": len(INTERPRETATION_REGISTRY),
        "features": {
            "helium_analysis": True,
            "carbon_level": _HAS_CARBON_LEVEL,
            "validation_hook": True,
        },
        "min_versions": {
            "helium_profile": MIN_HELIUM_PROFILE_VERSION,
            "workload_interpreter": MIN_WORKLOAD_INTERPRETER_VERSION,
        },
    }


def health() -> Dict[str, Any]:
    """Compact health report; suitable for `/health` endpoints and CI."""
    caps = get_capabilities()
    return {
        "ok": caps["component_count"] >= 3 and caps["features"]["helium_analysis"],
        "package_version": caps["package_version"],
        "components": caps["component_count"],
        "helium_profile_version": caps["helium_profile_version"],
        "workload_interpreter_version": caps["workload_interpreter_version"],
        "carbon_level_available": caps["features"]["carbon_level"],
    }


def test_smoke() -> Dict[str, Any]:
    """
    End-to-end self-check: instantiate the interpreter, run a canonical
    task, round-trip the result through JSON. Never raises; returns a
    structured report.

    This is the real implementation of the env-var validation block that
    v5.0 shipped as a no-op.
    """
    results: Dict[str, Any] = {
        "ok": False,
        "package_version": __version__,
        "steps": [],
    }

    try:
        interp = WorkloadInterpreter(strict=False)
        results["steps"].append({"name": "instantiate", "ok": True})
    except Exception as exc:  # noqa: BLE001
        results["error"] = f"instantiate: {type(exc).__name__}: {exc}"
        return results

    try:
        task = {
            "task_id": "smoke",
            "hardware_requirements": {"gpu_count": 1, "cpu_cores": 4},
            "model_config": {"size_gb": 1.0},
        }
        profile = interp.analyze_task(task, record=False)
        results["steps"].append({"name": "analyze", "ok": True})
    except Exception as exc:  # noqa: BLE001
        results["error"] = f"analyze: {type(exc).__name__}: {exc}"
        return results

    try:
        restored = WorkloadProfile.from_json(profile.to_json())
        matches = restored.key() == profile.key()
        results["steps"].append({"name": "roundtrip", "ok": matches})
    except Exception as exc:  # noqa: BLE001
        results["steps"].append({
            "name": "roundtrip", "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        })

    results["ok"] = all(s.get("ok") for s in results["steps"])
    return results


def __dir__() -> List[str]:
    return sorted(__all__)


# =============================================================================
# Public API
# =============================================================================

__all__ = [
    # --- Core types ---
    "HardwareType",
    "HeliumDependencyLevel",
    "HeliumProfile",
    "HeliumProfileConfig",
    "HeliumProfileError",
    "InterpretationConfig",
    "InterpretationError",
    "WorkloadInterpreter",
    "WorkloadProfile",
    # --- Registry ---
    "INTERPRETATION_REGISTRY",
    "get_interpretation_component",
    "list_interpretation_components",
    "register_interpretation_component",
    "unregister_interpretation_component",
    "override_interpretation_component",
    # --- Diagnostics ---
    "get_capabilities",
    "health",
    "test_smoke",
    # --- Metadata ---
    "__version__",
]

# Re-export CarbonLevel only if the submodule provides it.
if _HAS_CARBON_LEVEL:
    __all__.append("CarbonLevel")


# =============================================================================
# Optional env-var gate for CI smoke test
# =============================================================================

if os.environ.get("GREEN_AGENT_VALIDATE_INTERPRETATION") == "1":
    _report = test_smoke()
    if not _report.get("ok"):
        raise RuntimeError(
            f"Interpretation package validation failed: {_report}"
        )
    logger.info(
        "Interpretation package validation passed "
        "(package v%s, %d steps)",
        __version__, len(_report.get("steps", [])),
    )
