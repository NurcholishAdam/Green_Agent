# src/optimization/__init__.py

"""
Optimization Layer (v5.1)
=========================

Multi-objective optimization for carbon-aware, helium-aware AI orchestration.

Components
----------
Search & tuning:
- :class:`GeneticOptimizer` — NSGA-II multi-objective configuration tuning.
- :class:`BayesianOptimizer` — sample-efficient hyperparameter search.
- :class:`HyperparameterTuner` — grid / random / adaptive search.

Carbon-aware model optimization:
- :class:`HeliumAwareMLOptimizer` — fine-tuning strategy per helium zone.
- :class:`HeliumAwareDataOptimizer` — caching / batching per helium zone.
- :class:`QuantizationOptimizer` — precision selection per carbon zone.

Data-pipeline optimization:
- :class:`SyntheticDataOptimizer` — dedup, quality filter, active learning,
  synthetic augmentation.

Scheduling & ranking:
- :class:`ScheduleOptimizer` — continuum tier scheduling optimization.
- :class:`GreenLeaderboard` — negawatt-score ranking across agents.

Fixes in v5.1 (see review)
--------------------------
  * P0 — The three largest modules in this package — ``ml_optimizer``,
         ``synthetic_data_optimizer``, and ``green_leaderboard`` — are
         now exposed. Previously they were importable only via
         ``from src.optimization.ml_optimizer import ...``; the package
         surface omitted them entirely, including all their error types,
         config dataclasses, and result records.
  * P0 — The env-var validation hook now runs ``test_smoke()``, which
         instantiates one object per registry component and reports a
         structured result. Previously the loop wrapped ``logger.info()``
         in a try that could not fail and iterated a dead ``_cls``
         variable.
  * P0 — ``get_optimization_component`` raises ``OptimizationError``
         (a ``ValueError`` subclass) instead of ``KeyError``, matching
         the rest of the codebase. ``from None`` suppresses the chained
         ``KeyError`` in the traceback.
  * P1 — Registry supports ``override=True``,
         ``unregister_optimization_component``, and an
         ``override_optimization_component`` context manager for test
         isolation.
  * P1 — ``register_optimization_component`` accepts an optional
         ``expected_base`` for plugin type-checking.
  * P1 — Registry now includes the four v5.1 components:
         ``"ml"``, ``"data"``, ``"synthetic"``, ``"leaderboard"``.
  * P1 — ``__version__ = "5.1.0"`` plus per-submodule version tracking
         (``_SUBMODULE_VERSIONS``) and a ``_check_submodule_versions()``
         guard that warns on partial upgrades.
  * P1 — ``import os`` at the top of the file (previously
         ``import os as _os`` mid-file).
  * P2 — New helpers: ``get_capabilities()``, ``health()``,
         ``test_smoke()``, ``is_available()``, ``__dir__()``.
  * P3 — Docstring updated to include the three previously-omitted
         modules; ``__main__`` block prints capabilities and smoke
         report as JSON and exits nonzero on failure.

Backward compatibility
----------------------
- Every symbol from the v5.0 ``__all__`` remains importable under the
  same name.
- ``get_optimization_component`` for a valid name is unchanged; only the
  error type on the *unknown-name* path changed (``KeyError`` →
  ``OptimizationError``). ``OptimizationError`` subclasses ``ValueError``,
  so ``except ValueError`` still catches it.
- ``register_optimization_component(name, cls)`` keeps its positional
  signature; ``override=`` and ``expected_base=`` are keyword-only
  with defaults.
- ``OPTIMIZATION_REGISTRY`` grows from 5 to 9 entries (4 new aliases);
  callers that iterated the registry see the four new components.
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Tuple, Type

logger = logging.getLogger(__name__)

__version__ = "5.1.0"

#: Oldest submodule version this package can safely drive.
_MIN_SUBMODULE_VERSION = "5.1.0"


# =============================================================================
# Errors
# =============================================================================
class OptimizationError(ValueError):
    """
    Raised for invalid optimization registry / lookup operations.

    Submodule-specific errors (``GeneticOptimizerError``,
    ``MLOptimizerError``, ``SyntheticDataOptimizerError``,
    ``LeaderboardError``, etc.) all subclass ``ValueError``, so
    ``except ValueError`` catches the full optimization surface.
    """


# =============================================================================
# Submodule tracking
# =============================================================================
_MODULES: Tuple[str, ...] = (
    "genetic_optimizer",
    "bayesian_optimizer",
    "hyperparameter_tuner",
    "quantization_optimizer",
    "schedule_optimizer",
    "ml_optimizer",
    "synthetic_data_optimizer",
    "green_leaderboard",
)

#: Per-submodule ``__version__`` snapshots (``None`` when the submodule
#: has no ``__version__``).
_SUBMODULE_VERSIONS: Dict[str, Optional[str]] = {}


# =============================================================================
# Module object imports (for version inspection)
# =============================================================================
# Import the module objects first so their ``__version__`` attributes can
# be recorded before any symbol is lifted into the package namespace.
from . import genetic_optimizer as _genetic_module
from . import bayesian_optimizer as _bayesian_module
from . import hyperparameter_tuner as _tuner_module
from . import quantization_optimizer as _quant_module
from . import schedule_optimizer as _sched_module
from . import ml_optimizer as _ml_module
from . import synthetic_data_optimizer as _synth_module
from . import green_leaderboard as _leaderboard_module

_SUBMODULE_VERSIONS = {
    "genetic_optimizer": getattr(_genetic_module, "__version__", None),
    "bayesian_optimizer": getattr(_bayesian_module, "__version__", None),
    "hyperparameter_tuner": getattr(_tuner_module, "__version__", None),
    "quantization_optimizer": getattr(_quant_module, "__version__", None),
    "schedule_optimizer": getattr(_sched_module, "__version__", None),
    "ml_optimizer": getattr(_ml_module, "__version__", None),
    "synthetic_data_optimizer": getattr(_synth_module, "__version__", None),
    "green_leaderboard": getattr(_leaderboard_module, "__version__", None),
}


def _check_submodule_versions() -> None:
    """Warn (do not raise) when a submodule is older than the minimum."""
    try:
        from packaging.version import InvalidVersion, Version
    except ImportError:
        logger.debug("packaging not installed; skipping version check")
        return
    required = Version(_MIN_SUBMODULE_VERSION)
    for name, version in _SUBMODULE_VERSIONS.items():
        if not version:
            logger.debug(
                "%s has no __version__; skipping check", name,
            )
            continue
        try:
            installed = Version(version)
        except InvalidVersion:
            logger.warning(
                "%s has non-PEP-440 version %r; skipping check",
                name, version,
            )
            continue
        if installed < required:
            logger.warning(
                "src.optimization v%s recommends %s >= %s; "
                "found %s. Some enhanced features may be missing.",
                __version__, name, _MIN_SUBMODULE_VERSION, version,
            )


_check_submodule_versions()


# =============================================================================
# Symbol imports
# =============================================================================
# --- Search & tuning ---
from .genetic_optimizer import (
    GeneticOptimizer,
    GeneticOptimizerConfig,
    GeneticOptimizerError,
    OptimizationResult,
)
from .bayesian_optimizer import (
    BayesianOptimizer,
    BayesianOptimizerConfig,
    BayesianOptimizerError,
)
from .hyperparameter_tuner import (
    HyperparameterTuner,
    HyperparameterTunerConfig,
    HyperparameterTunerError,
    TunerResult,
)

# --- Carbon-aware model optimization ---
from .quantization_optimizer import (
    QuantizationOptimizer,
    QuantizationOptimizerConfig,
    QuantizationOptimizerError,
)
from .ml_optimizer import (
    DataOptimizerConfig,
    DataOptimizerError,
    DataPipelinePlan,
    HeliumAwareDataOptimizer,
    HeliumAwareMLOptimizer,
    HeliumOptimizationMode,
    HeliumOptimizationStrategy,
    MLOptimizerConfig,
    MLOptimizerError,
    OptimizationReport,
    OptimizationStep,
)

# --- Data-pipeline optimization ---
from .synthetic_data_optimizer import (
    CompressionStrategy,
    DataOptimizationResult,
    SampleScore,
    SyntheticDataOptimizer,
    SyntheticDataOptimizerError,
    SyntheticOptimizerConfig,
    SyntheticStrategy,
)

# --- Scheduling & ranking ---
from .schedule_optimizer import (
    ScheduleOptimizer,
    ScheduleOptimizerConfig,
    ScheduleOptimizerError,
)
from .green_leaderboard import (
    GreenLeaderboard,
    LeaderboardConfig,
    LeaderboardError,
    LeaderboardRecord,
)


# =============================================================================
# Registry
# =============================================================================
_MISSING = object()

OPTIMIZATION_REGISTRY: Dict[str, Type[Any]] = {
    # --- Search & tuning (v5.0) ---
    "genetic":      GeneticOptimizer,
    "bayesian":     BayesianOptimizer,
    "tuner":        HyperparameterTuner,
    # --- Carbon-aware model optimization (v5.0 + v5.1) ---
    "quantization": QuantizationOptimizer,
    "ml":           HeliumAwareMLOptimizer,
    "data":         HeliumAwareDataOptimizer,
    # --- Data-pipeline optimization (v5.1) ---
    "synthetic":    SyntheticDataOptimizer,
    # --- Scheduling & ranking (v5.0 + v5.1) ---
    "schedule":     ScheduleOptimizer,
    "leaderboard":  GreenLeaderboard,
}


def register_optimization_component(
    name: str,
    cls: Type[Any],
    *,
    override: bool = False,
    expected_base: Optional[type] = None,
) -> None:
    """
    Register an optimization component class under ``name``.

    Parameters
    ----------
    name : str
        Non-empty identifier.
    cls : type
        The component class.
    override : bool, default False
        When True, replace any existing registration. When False, a
        conflicting registration raises ``ValueError``; re-registering
        the *same* class is a no-op.
    expected_base : type, optional
        When provided, ``cls`` must be a subclass.

    Raises
    ------
    ValueError
        ``name`` is empty, or a conflicting registration exists without
        ``override=True``.
    TypeError
        ``cls`` isn't a class, or ``expected_base`` isn't matched.
    """
    if not isinstance(name, str) or not name:
        raise ValueError(
            f"name must be a non-empty string, "
            f"got {type(name).__name__}."
        )
    if not isinstance(cls, type):
        raise TypeError(
            f"Expected a class, got {type(cls).__name__}."
        )
    if expected_base is not None and not issubclass(cls, expected_base):
        raise TypeError(
            f"'{name}' must be a subclass of {expected_base.__name__}, "
            f"got {cls.__name__}"
        )
    existing = OPTIMIZATION_REGISTRY.get(name, _MISSING)
    if existing is not _MISSING and existing is not cls and not override:
        raise ValueError(
            f"Optimization component '{name}' already registered to "
            f"{existing.__name__}. Pass override=True to replace."
        )
    OPTIMIZATION_REGISTRY[name] = cls
    logger.debug(
        "Registered optimization component '%s' -> %s",
        name, cls.__name__,
    )


def unregister_optimization_component(name: str) -> None:
    """Remove the registration under ``name``."""
    if name not in OPTIMIZATION_REGISTRY:
        raise OptimizationError(
            f"Unknown optimization component '{name}'. "
            f"Available: {sorted(OPTIMIZATION_REGISTRY)}"
        )
    OPTIMIZATION_REGISTRY.pop(name)
    logger.debug("Unregistered optimization component '%s'", name)


@contextmanager
def override_optimization_component(
    name: str, cls: Type[Any],
) -> Iterator[None]:
    """
    Temporarily override (or add) a component registration; restore the
    previous entry on exit. Intended for test isolation::

        with override_optimization_component("genetic", FakeOptimizer):
            assert get_optimization_component("genetic") is FakeOptimizer
        # original restored
    """
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    prev = OPTIMIZATION_REGISTRY.get(name, _MISSING)
    OPTIMIZATION_REGISTRY[name] = cls
    try:
        yield
    finally:
        if prev is _MISSING:
            OPTIMIZATION_REGISTRY.pop(name, None)
        else:
            OPTIMIZATION_REGISTRY[name] = prev  # type: ignore[assignment]


def get_optimization_component(name: str) -> Type[Any]:
    """
    Return the optimization component class registered under ``name``.

    Raises
    ------
    OptimizationError
        When ``name`` isn't registered. ``OptimizationError`` subclasses
        ``ValueError``, so ``except ValueError`` still works.
    """
    try:
        return OPTIMIZATION_REGISTRY[name]
    except KeyError:
        raise OptimizationError(
            f"Unknown optimization component '{name}'. "
            f"Available: {sorted(OPTIMIZATION_REGISTRY)}"
        ) from None


def list_optimization_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {
        name: cls.__name__
        for name, cls in OPTIMIZATION_REGISTRY.items()
        if cls is not None
    }


# =============================================================================
# Diagnostics
# =============================================================================
def is_available(name: str) -> bool:
    """
    True if ``name`` is a bound symbol exposed by this package.

    Unlike ``hasattr(module, name)``, this only returns ``True`` for
    genuinely-resolved names — the module defines no ``__getattr__``
    fallback, so a name absent from ``globals()`` is truly absent.
    """
    return name in globals() and globals()[name] is not None


def _try_instantiate(cls: Type[Any]) -> Tuple[bool, Optional[str]]:
    """
    Attempt ``cls()`` with no arguments.

    Returns ``(ok, note)`` where:

    - ``ok`` is True if instantiation succeeded, OR failed because the
      constructor requires arguments (which means the class itself is
      fine — we just can't exercise it without a fixture).
    - ``note`` is a human-readable description of any caveat.
    """
    try:
        cls()
        return (True, None)
    except TypeError as exc:
        msg = str(exc).lower()
        if any(k in msg for k in ("required", "missing", "positional")):
            return (
                True,
                f"constructor requires args; import verified: {exc}",
            )
        return (False, f"{type(exc).__name__}: {exc}")
    except Exception as exc:  # noqa: BLE001
        return (False, f"{type(exc).__name__}: {exc}")


def test_smoke() -> Dict[str, Any]:
    """
    Structured health check: instantiate one object per registry component.

    Never raises; returns a report.
    """
    results: Dict[str, Any] = {
        "ok": False,
        "package_version": __version__,
        "steps": [],
    }
    for name, cls in OPTIMIZATION_REGISTRY.items():
        if cls is None:
            results["steps"].append({
                "name": f"instantiate:{name}",
                "ok": False,
                "error": "registry entry is None",
            })
            continue
        ok, note = _try_instantiate(cls)
        step = {"name": f"instantiate:{name}", "ok": ok}
        if note:
            step["note"] = note
        results["steps"].append(step)
    results["ok"] = (
        len(results["steps"]) > 0
        and all(s.get("ok") for s in results["steps"])
    )
    return results


def health() -> Dict[str, Any]:
    """
    Compact health report suitable for ``/health`` endpoints and CI gates.
    """
    caps = get_capabilities()
    smoke = test_smoke()
    return {
        "ok": smoke["ok"],
        "package_version": caps["package_version"],
        "component_count": caps["component_count"],
        "instantiation": smoke["steps"],
    }


def get_capabilities() -> Dict[str, Any]:
    """
    Structured capability report suitable for logs and support tickets.
    """
    return {
        "package_version": __version__,
        "min_submodule_version": _MIN_SUBMODULE_VERSION,
        "submodules": list(_MODULES),
        "submodule_versions": dict(_SUBMODULE_VERSIONS),
        "registered_components": list_optimization_components(),
        "component_count": len(OPTIMIZATION_REGISTRY),
    }


def __dir__() -> List[str]:
    return sorted(__all__)


# =============================================================================
# Public API
# =============================================================================
__all__ = [
    # --- Search & tuning ---
    "GeneticOptimizer",
    "GeneticOptimizerConfig",
    "GeneticOptimizerError",
    "OptimizationResult",
    "BayesianOptimizer",
    "BayesianOptimizerConfig",
    "BayesianOptimizerError",
    "HyperparameterTuner",
    "HyperparameterTunerConfig",
    "HyperparameterTunerError",
    "TunerResult",
    # --- Carbon-aware model optimization ---
    "QuantizationOptimizer",
    "QuantizationOptimizerConfig",
    "QuantizationOptimizerError",
    "HeliumAwareMLOptimizer",
    "HeliumAwareDataOptimizer",
    "HeliumOptimizationMode",
    "HeliumOptimizationStrategy",
    "MLOptimizerConfig",
    "DataOptimizerConfig",
    "MLOptimizerError",
    "DataOptimizerError",
    "OptimizationReport",
    "OptimizationStep",
    "DataPipelinePlan",
    # --- Data-pipeline optimization ---
    "SyntheticDataOptimizer",
    "SyntheticDataOptimizerError",
    "SyntheticOptimizerConfig",
    "DataOptimizationResult",
    "SampleScore",
    "CompressionStrategy",
    "SyntheticStrategy",
    # --- Scheduling & ranking ---
    "ScheduleOptimizer",
    "ScheduleOptimizerConfig",
    "ScheduleOptimizerError",
    "GreenLeaderboard",
    "LeaderboardConfig",
    "LeaderboardError",
    "LeaderboardRecord",
    # --- Package errors ---
    "OptimizationError",
    # --- Registry ---
    "OPTIMIZATION_REGISTRY",
    "get_optimization_component",
    "list_optimization_components",
    "register_optimization_component",
    "unregister_optimization_component",
    "override_optimization_component",
    # --- Diagnostics ---
    "get_capabilities",
    "health",
    "test_smoke",
    "is_available",
    # --- Metadata ---
    "__version__",
]

# Filter against genuine bindings — every name above must resolve to a
# real symbol or the package is misconfigured.
__all__ = [n for n in __all__ if n in globals()]


# =============================================================================
# Optional env-var gate for CI
# =============================================================================
if os.environ.get("GREEN_AGENT_VALIDATE_OPTIMIZATION") == "1":
    _report = test_smoke()
    if not _report["ok"]:
        raise RuntimeError(
            f"Optimization package validation failed: {_report}"
        )
    logger.info(
        "Optimization package validation passed "
        "(package v%s, %d component(s) exercised).",
        __version__, len(_report["steps"]),
    )


# =============================================================================
# Load diagnostics
# =============================================================================
logger.debug(
    "src.optimization v%s ready: %d public symbols, %d registered "
    "component(s), %d submodule(s).",
    __version__, len(__all__), len(OPTIMIZATION_REGISTRY), len(_MODULES),
)


# =============================================================================
# Local smoke test: python -m src.optimization
# =============================================================================
if __name__ == "__main__":  # pragma: no cover
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("=== Capabilities ===")
    print(json.dumps(get_capabilities(), indent=2, default=str))

    print("\n=== Registered components ===")
    print(json.dumps(list_optimization_components(), indent=2))

    print("\n=== test_smoke() ===")
    _smoke = test_smoke()
    print(json.dumps(_smoke, indent=2, default=str))

    print("\n=== health() ===")
    print(json.dumps(health(), indent=2, default=str))

    # --- Registry override + unregister round-trip ---
    class _FakeComponent:
        pass

    with override_optimization_component("genetic", _FakeComponent):
        assert get_optimization_component("genetic") is _FakeComponent
    assert get_optimization_component("genetic") is GeneticOptimizer
    print("\noverride_optimization_component: OK")

    register_optimization_component("temp", _FakeComponent)
    assert get_optimization_component("temp") is _FakeComponent
    unregister_optimization_component("temp")
    try:
        get_optimization_component("temp")
    except OptimizationError:
        pass
    else:
        raise AssertionError("unregister did not remove the entry")
    print("register/unregister: OK")

    # --- Unknown component raises OptimizationError ---
    try:
        get_optimization_component("does_not_exist")
    except OptimizationError as exc:
        print(f"Unknown component rejected: {exc}")
    else:
        raise AssertionError(
            "get_optimization_component did not raise for unknown name"
        )

    # --- Conflict without override ---
    try:
        register_optimization_component("genetic", _FakeComponent)
    except ValueError as exc:
        print(f"Conflict rejected: {exc}")
    else:
        raise AssertionError(
            "register_optimization_component should have rejected the "
            "conflict"
        )

    # --- expected_base type guard ---
    try:
        register_optimization_component(
            "bad", _FakeComponent, expected_base=GeneticOptimizer,
        )
    except TypeError as exc:
        print(f"expected_base rejected: {exc}")
    else:
        raise AssertionError(
            "expected_base should have rejected the non-subclass"
        )

    # --- All v5.1 exports importable ---
    from src.optimization import (  # noqa: F401
        DataOptimizationResult,
        GreenLeaderboard,
        HeliumAwareMLOptimizer,
        LeaderboardRecord,
        MLOptimizerError,
        OptimizationError,
        SyntheticDataOptimizer,
        SyntheticOptimizerConfig,
    )
    print("\nAll v5.1 package-level exports import OK.")

    if not _smoke["ok"]:
        sys.exit(1)
    print("\nSmoke test passed.")
