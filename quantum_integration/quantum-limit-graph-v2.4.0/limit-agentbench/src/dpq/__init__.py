# src/dpq/__init__.py

"""
Dynamic Precision Quantization (DPQ) — v2.0.0
==============================================

Carbon-aware inference layer that dynamically adjusts model precision based on
grid carbon intensity. Coordinates:

- :class:`CarbonIntensityMonitor` — tracks carbon zones and predicts transitions.
- :class:`ModelConverter` — converts models between FP32 / FP16 / INT8 / INT4.
- :class:`PrecisionController` — maps zones to precision levels and triggers
  conversions.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``get_dpq_component("nonexistent")`` raises ``DPQError`` (subclass of
        ``ValueError``) instead of a bare ``KeyError``. Same for
        ``register_dpq_component``'s validation errors.
  * 2.2 ``DPQ_REGISTRY`` is a read-only ``MappingProxyType`` view. Direct
        assignment (``DPQ_REGISTRY["monitor"] = int``) raises ``TypeError``;
        the public ``register_dpq_component`` / ``unregister_dpq_component``
        API is the only mutation path.
  * 2.3 ``register_dpq_component`` validates the class contract
        (must expose callable ``statistics`` and ``to_dict``). Previously
        ``register_dpq_component("monitor", int)`` was accepted silently.
  * 2.5 Import-time validation actually instantiates ``CarbonIntensityMonitor``
        and ``ModelConverter`` (with safe constructor args) and calls
        ``.statistics()``. A broken class produces a descriptive
        ``ImportError`` listing every failure, not a silent ``logger.info``.

**P1 robustness / observability**
  * 2.6 ``register_dpq_component(..., override=True)`` allows replacing a
        registered component; ``unregister_dpq_component(name)`` removes one.
        Together they make the registry usable from tests without cross-test
        pollution.
  * 3.5 ``dpq.__version__`` is exported and matches the submodules' version.
  * 3.6 **Unified error taxonomy**: ``DPQError`` is defined here as a base
        class for the entire subsystem. ``CarbonMonitorError``,
        ``ModelConverterError``, ``PrecisionControllerError``, and
        ``DPQModelError`` are re-parented to inherit from it (in place, so
        existing ``except CarbonMonitorError`` clauses still work).
        ``except DPQError`` catches any DPQ error.
  * 3.9 Successful registration logs at ``INFO`` (was ``DEBUG``).
  * 3.10 ``GREEN_AGENT_VALIDATE_DPQ`` accepts ``"1"``, ``"true"``,
        ``"yes"`` (case-insensitive), not just ``"1"``.

**P2 polish**
  * 3.4 Removed the ``_os`` alias — plain ``import os``.
  * 3.7 ``get_dpq_component`` gains a typed return annotation.
  * ``_run_selftests()`` is exposed as a callable for CI / smoke-testing.

Backward compatible: every v1.0.0 import path, every exported symbol, and
every payload works unchanged. The `DPQ_REGISTRY` dict is still readable
via the same interface (``.items()``, ``.keys()``, ``.get()``, ``in``);
only direct mutation is now blocked.
"""

from __future__ import annotations

import logging
import os
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Tuple, Type

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Error base class (P0 2.1 / P1 3.6)
# --------------------------------------------------------------------------- #
class DPQError(ValueError):
    """
    Base class for every error raised by the DPQ subsystem.

    Submodules define their own subclasses (``CarbonMonitorError``,
    ``ModelConverterError``, ``PrecisionControllerError``, ``DPQModelError``)
    which inherit from this class. Callers can therefore catch ``DPQError``
    to handle any DPQ failure, or a specific subclass to handle one facet.
    """


# --------------------------------------------------------------------------- #
# Shared enums / dataclasses (single source of truth)
# --------------------------------------------------------------------------- #
from .models import (
    CarbonZone,
    ConversionResult,
    DPQConfig,
    DPQModelError,
    DPQMetrics,
    ModelPrecision,
    ModelVariant,
    PrecisionTransition,
    TransitionStatus,
    TransitionTrigger,
    new_transition_id,
)


# --------------------------------------------------------------------------- #
# Components
# --------------------------------------------------------------------------- #
from .carbon_intensity_monitor import (
    CarbonIntensityMonitor,
    CarbonIntensityUpdate,
    CarbonMonitorConfig,
    CarbonMonitorError,
)

from .model_converter import (
    ConverterConfig,
    ModelConverter,
    ModelConverterError,
)

from .precision_controller import (
    ControllerConfig,
    PrecisionController,
    PrecisionControllerError,
    PrecisionPolicy,
)


logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Re-parent submodule errors to inherit from DPQError (P1 3.6)
# --------------------------------------------------------------------------- #
# The submodule error classes were originally defined as `class X(ValueError)`.
# Re-parenting them in place (via `__bases__`) preserves their identity (so
# existing `except X:` clauses keep working) while making them children of
# `DPQError`, so `except DPQError:` catches them all.
def _reparent_errors() -> None:
    for cls in (
        CarbonMonitorError,
        ModelConverterError,
        PrecisionControllerError,
        DPQModelError,
    ):
        if DPQError in cls.__mro__:
            continue
        try:
            cls.__bases__ = (DPQError,)
            logger.debug(
                "Re-parented %s to inherit from DPQError.", cls.__name__)
        except TypeError as exc:  # pragma: no cover — defensive
            logger.warning(
                "Could not re-parent %s to inherit from DPQError: %s",
                cls.__name__, exc)


_reparent_errors()


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    # package metadata
    "__version__",
    # shared models
    "CarbonZone",
    "ConversionResult",
    "DPQConfig",
    "DPQError",
    "DPQModelError",
    "DPQMetrics",
    "ModelPrecision",
    "ModelVariant",
    "PrecisionTransition",
    "TransitionStatus",
    "TransitionTrigger",
    "new_transition_id",
    # carbon monitor
    "CarbonIntensityMonitor",
    "CarbonIntensityUpdate",
    "CarbonMonitorConfig",
    "CarbonMonitorError",
    # converter
    "ConverterConfig",
    "ModelConverter",
    "ModelConverterError",
    # controller
    "ControllerConfig",
    "PrecisionController",
    "PrecisionControllerError",
    "PrecisionPolicy",
    # registry
    "DPQ_REGISTRY",
    "get_dpq_component",
    "list_dpq_components",
    "register_dpq_component",
    "unregister_dpq_component",
]


# --------------------------------------------------------------------------- #
# Component registry (P0 2.2 — read-only public view)
# --------------------------------------------------------------------------- #
# Internal mutable dict — only mutated by the registry functions below.
_DPQ_REGISTRY: Dict[str, Type[Any]] = {
    "monitor": CarbonIntensityMonitor,
    "converter": ModelConverter,
    "controller": PrecisionController,
}

# Public read-only view. Direct assignment raises TypeError.
DPQ_REGISTRY: Mapping[str, Type[Any]] = MappingProxyType(_DPQ_REGISTRY)


# --------------------------------------------------------------------------- #
# Contract validation (P0 2.3)
# --------------------------------------------------------------------------- #
_REQUIRED_COMPONENT_METHODS: Tuple[str, ...] = ("statistics", "to_dict")


def _validate_component_class(name: str, cls: Type[Any]) -> None:
    """
    Ensure ``cls`` looks like a DPQ component.

    Every service class in the DPQ subsystem is expected to expose
    ``statistics()`` and ``to_dict()``. The check is intentionally
    lightweight (does not construct an instance) so it can run on the class
    alone.
    """
    if not isinstance(cls, type):
        raise DPQError(
            f"DPQ component {name!r} must be a class, "
            f"got {type(cls).__name__}.")
    for attr in _REQUIRED_COMPONENT_METHODS:
        if not callable(getattr(cls, attr, None)):
            raise DPQError(
                f"DPQ component {name!r} ({cls.__name__}) must expose a "
                f"callable {attr!r}.")


# --------------------------------------------------------------------------- #
# Registry functions (P0 2.1 / P1 2.6)
# --------------------------------------------------------------------------- #
def register_dpq_component(
    name: str,
    cls: Type[Any],
    *,
    override: bool = False,
) -> None:
    """
    Register a DPQ component class under ``name``.

    Parameters
    ----------
    name : str
        Non-empty identifier (e.g. ``"monitor"``).
    cls : type
        The class to register. Must expose callable ``statistics`` and
        ``to_dict``.
    override : bool, default False
        If True, replaces an existing registration. If False, re-registering
        a different class under the same name raises ``DPQError``.
    """
    if not isinstance(name, str) or not name:
        raise DPQError("name must be a non-empty string.")
    _validate_component_class(name, cls)

    existing = _DPQ_REGISTRY.get(name)
    if existing is not None and existing is not cls and not override:
        raise DPQError(
            f"DPQ component {name!r} already registered to "
            f"{existing.__name__}; pass override=True to replace.")

    _DPQ_REGISTRY[name] = cls
    logger.info(
        "Registered DPQ component %r -> %s%s",
        name, cls.__name__,
        " (override)" if existing is not None else "")


def unregister_dpq_component(name: str) -> bool:
    """
    Remove a DPQ component from the registry.

    Returns True if the component existed, False otherwise.
    """
    if name not in _DPQ_REGISTRY:
        return False
    del _DPQ_REGISTRY[name]
    logger.info("Unregistered DPQ component %r.", name)
    return True


def get_dpq_component(name: str) -> Type[Any]:
    """Return the DPQ component class registered under ``name``."""
    try:
        return _DPQ_REGISTRY[name]
    except KeyError as exc:
        raise DPQError(
            f"Unknown DPQ component {name!r}. "
            f"Available: {sorted(_DPQ_REGISTRY)}") from exc


def list_dpq_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {
        name: cls.__name__ for name, cls in _DPQ_REGISTRY.items()
    }


# --------------------------------------------------------------------------- #
# Import-time validation (P0 2.5)
# --------------------------------------------------------------------------- #
# Safe constructor arguments for each known component. A component that
# requires a live dependency that cannot be safely instantiated at import
# time (e.g. a network handle) should be listed with `None` and will be
# skipped during the smoke test.
_SMOKE_TEST_ARGS: Dict[str, Optional[Dict[str, Any]]] = {
    "monitor": {
        "carbon_forecaster_url": "http://test.invalid",
    },
    "converter": {},
    "controller": {
        "policy": PrecisionPolicy(),
    },
}


def _validate_all_components() -> None:
    """
    Validate every registered component.

    For each component:
      1. The contract check (``statistics`` and ``to_dict`` callable).
      2. If a smoke-test argument set is defined, instantiate and call
         ``.statistics()`` to prove the class is usable.

    Aggregates all failures into a single ``ImportError`` so operators see
    every problem at once (not just the first).
    """
    failures: List[str] = []
    for name, cls in _DPQ_REGISTRY.items():
        try:
            _validate_component_class(name, cls)
        except DPQError as exc:
            failures.append(f"{name} ({cls.__name__}): {exc}")
            continue

        smoke_args = _SMOKE_TEST_ARGS.get(name)
        if smoke_args is None:
            # Explicitly skipped — no safe way to construct here.
            logger.info(
                "DPQ component %r (%s) validated (contract only).",
                name, cls.__name__)
            continue

        try:
            instance = cls(**smoke_args)
            instance.statistics()  # smoke-call
            logger.info(
                "DPQ component %r (%s) validated (instantiated + "
                "statistics() called).", name, cls.__name__)
        except Exception as exc:  # noqa: BLE001
            failures.append(
                f"{name} ({cls.__name__}): "
                f"{type(exc).__name__}: {exc}")

    if failures:
        raise ImportError(
            "DPQ component validation failed:\n  "
            + "\n  ".join(failures))


def _coerce_bool_env(value: Optional[str]) -> bool:
    """Coerce an env-var string to bool (P1 3.10)."""
    if value is None:
        return False
    return value.strip().lower() in ("1", "true", "yes", "on", "t", "y")


if _coerce_bool_env(os.environ.get("GREEN_AGENT_VALIDATE_DPQ")):
    _validate_all_components()


# --------------------------------------------------------------------------- #
# Self-tests
# --------------------------------------------------------------------------- #
def _run_selftests() -> None:
    """
    Run package self-tests.

    Usage::

        python -c "import dpq; dpq._run_selftests()"

    The function prints ``✓`` lines for each passing test and raises
    ``AssertionError`` on the first failure.
    """
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )
    import asyncio
    import pickle  # noqa: F401 (used by context tests)

    print(f"=== dpq v{__version__} self-tests ===\n")

    # ------------------------------------------------------------------ #
    # P0 2.1 — get_dpq_component raises DPQError, not KeyError
    # ------------------------------------------------------------------ #
    print("=== P0 2.1: get_dpq_component raises DPQError ===")
    try:
        get_dpq_component("nonexistent-component-xyz")
    except DPQError as exc:
        print(f"  ✓ DPQError raised: {exc}")
    except KeyError as exc:
        raise AssertionError(
            f"P0 2.1 regression: got KeyError instead of DPQError: {exc}")
    else:
        raise AssertionError(
            "P0 2.1 regression: no exception raised")

    # Also check the register path raises DPQError.
    try:
        register_dpq_component("", int)  # empty name
    except DPQError:
        print("  ✓ register_dpq_component('') raises DPQError")
    except (ValueError, TypeError) as exc:
        raise AssertionError(
            f"P0 2.1 regression: register path raised "
            f"{type(exc).__name__}, expected DPQError")
    else:
        raise AssertionError(
            "P0 2.1 regression: empty name was accepted")

    # ------------------------------------------------------------------ #
    # P0 2.2 — DPQ_REGISTRY is read-only
    # ------------------------------------------------------------------ #
    print("\n=== P0 2.2: DPQ_REGISTRY is read-only ===")
    try:
        DPQ_REGISTRY["monitor"] = int  # type: ignore[index]
    except TypeError as exc:
        print(f"  ✓ direct assignment raises TypeError: {exc}")
    else:
        raise AssertionError(
            "P0 2.2 regression: DPQ_REGISTRY was directly mutable")

    # Reading still works.
    assert "monitor" in DPQ_REGISTRY
    assert DPQ_REGISTRY["monitor"] is CarbonIntensityMonitor
    assert DPQ_REGISTRY.get("nonexistent") is None
    assert set(DPQ_REGISTRY.keys()) == {
        "monitor", "converter", "controller",
    }
    print("  ✓ read access (get, in, keys) still works")

    # register_dpq_component still mutates the underlying dict.
    class _ScratchMonitor:
        def statistics(self): return {}
        def to_dict(self): return {}

    register_dpq_component("scratch", _ScratchMonitor)
    assert "scratch" in DPQ_REGISTRY
    assert DPQ_REGISTRY["scratch"] is _ScratchMonitor
    print("  ✓ register_dpq_component mutates the underlying dict")
    unregister_dpq_component("scratch")
    assert "scratch" not in DPQ_REGISTRY

    # ------------------------------------------------------------------ #
    # P0 2.3 — contract validation on register
    # ------------------------------------------------------------------ #
    print("\n=== P0 2.3: contract validation ===")
    # int has no statistics/to_dict → rejected.
    try:
        register_dpq_component("bad", int)  # type: ignore[arg-type]
    except DPQError as exc:
        print(f"  ✓ register_dpq_component('bad', int) raises "
              f"DPQError: {exc}")
    else:
        raise AssertionError(
            "P0 2.3 regression: int was accepted as a component")

    # Non-class → rejected.
    try:
        register_dpq_component("bad2", 42)  # type: ignore[arg-type]
    except DPQError:
        print("  ✓ non-class rejected")
    else:
        raise AssertionError(
            "P0 2.3 regression: int instance was accepted")

    # Proper class → accepted.
    class _FakeMonitor:
        def statistics(self): return {"ok": True}
        def to_dict(self): return {"ok": True}

    register_dpq_component("fake-mon", _FakeMonitor)
    assert DPQ_REGISTRY["fake-mon"] is _FakeMonitor
    print("  ✓ _FakeMonitor (has statistics + to_dict) accepted")

    # Cleanup.
    unregister_dpq_component("fake-mon")

    # ------------------------------------------------------------------ #
    # P0 2.5 — import-time validation actually instantiates
    # ------------------------------------------------------------------ #
    print("\n=== P0 2.5: _validate_all_components does real work ===")
    # The three defaults construct fine with _SMOKE_TEST_ARGS.
    _validate_all_components()
    print("  ✓ all default components validated (instantiated + "
          "statistics() called)")

    # A broken component causes a descriptive ImportError.
    class _BrokenComponent:
        """Intentionally missing statistics/to_dict."""
        pass

    register_dpq_component("broken", _BrokenComponent)
    try:
        _validate_all_components()
    except ImportError as exc:
        assert "broken" in str(exc).lower(), (
            f"P0 2.5 regression: failure message missing component "
            f"name: {exc}")
        print(f"  ✓ broken class → ImportError mentioning 'broken': "
              f"{str(exc)[:80]}...")
    else:
        raise AssertionError(
            "P0 2.5 regression: broken class did not fail validation")
    finally:
        unregister_dpq_component("broken")

    # ------------------------------------------------------------------ #
    # P1 2.6 — override + unregister
    # ------------------------------------------------------------------ #
    print("\n=== P1 2.6: override + unregister ===")

    class _MockMonitor:
        def statistics(self): return {"mock": True}
        def to_dict(self): return {"mock": True}

    # Without override → raises.
    try:
        register_dpq_component("monitor", _MockMonitor)
    except DPQError:
        print("  ✓ overriding without override=True raises DPQError")
    else:
        raise AssertionError(
            "P1 2.6 regression: override succeeded without override=True")

    original_monitor = DPQ_REGISTRY["monitor"]
    try:
        register_dpq_component(
            "monitor", _MockMonitor, override=True)
        assert DPQ_REGISTRY["monitor"] is _MockMonitor
        print("  ✓ override=True succeeded")
    finally:
        # Restore default (in try/finally so the registry is clean even
        # if the test fails midway).
        register_dpq_component(
            "monitor", original_monitor, override=True)
    assert DPQ_REGISTRY["monitor"] is CarbonIntensityMonitor
    print("  ✓ original restored via try/finally")

    # unregister returns True/False correctly.
    class _Scratch:
        def statistics(self): return {}
        def to_dict(self): return {}

    register_dpq_component("scratch-unreg", _Scratch)
    assert unregister_dpq_component("scratch-unreg") is True
    assert unregister_dpq_component("scratch-unreg") is False
    print("  ✓ unregister_dpq_component returns True then False")

    # ------------------------------------------------------------------ #
    # P1 3.6 — DPQError catches every submodule error
    # ------------------------------------------------------------------ #
    print("\n=== P1 3.6: except DPQError catches all submodule errors ===")

    # Each submodule error is a subclass of DPQError.
    for cls in (
        CarbonMonitorError,
        ModelConverterError,
        PrecisionControllerError,
        DPQModelError,
    ):
        assert issubclass(cls, DPQError), (
            f"P1 3.6 regression: {cls.__name__} does not inherit "
            f"from DPQError")
        assert issubclass(cls, ValueError), (
            f"Backward-compat regression: {cls.__name__} no longer "
            f"inherits from ValueError")
    print(f"  ✓ all 4 submodule errors subclass DPQError")

    # Constructing and raising each type is caught by `except DPQError`.
    for cls in (
        CarbonMonitorError,
        ModelConverterError,
        PrecisionControllerError,
        DPQModelError,
    ):
        try:
            raise cls("test")
        except DPQError:
            pass  # expected
        except Exception as exc:  # noqa: BLE001
            raise AssertionError(
                f"P1 3.6 regression: {cls.__name__} not caught by "
                f"except DPQError")
        # And still caught by `except ValueError` (backward compat).
        try:
            raise cls("test")
        except ValueError:
            pass

    print("  ✓ except DPQError catches each; except ValueError still works")

    # ------------------------------------------------------------------ #
    # P1 3.5 — __version__
    # ------------------------------------------------------------------ #
    print("\n=== P1 3.5: __version__ ===")
    import dpq as self_module
    assert hasattr(self_module, "__version__"), (
        "P1 3.5 regression: dpq.__version__ missing")
    assert self_module.__version__ == "2.0.0", (
        f"P1 3.5 regression: __version__={self_module.__version__!r}")
    # Check submodule version matches.
    from . import models as _models_mod
    assert _models_mod.__version__ == self_module.__version__, (
        f"Version mismatch: dpq.__version__={self_module.__version__!r}, "
        f"models.__version__={_models_mod.__version__!r}")
    print(f"  ✓ dpq.__version__ = {self_module.__version__!r} "
          f"(matches models.__version__)")

    # ------------------------------------------------------------------ #
    # Bonus: list_dpq_components shape, env var coercion
    # ------------------------------------------------------------------ #
    print("\n=== Bonus: registry reporting + env var coercion ===")
    listing = list_dpq_components()
    assert listing == {
        "monitor": "CarbonIntensityMonitor",
        "converter": "ModelConverter",
        "controller": "PrecisionController",
    }, listing
    print(f"  ✓ list_dpq_components: {listing}")

    # _coerce_bool_env accepts common spellings.
    for val, expected in [
        ("1", True), ("true", True), ("TRUE", True), ("Yes", True),
        ("on", True), ("0", False), ("false", False),
        ("", False), (None, False), ("garbage", False),
    ]:
        assert _coerce_bool_env(val) is expected, (
            f"_coerce_bool_env({val!r}) = {_coerce_bool_env(val)}, "
            f"expected {expected}")
    print("  ✓ _coerce_bool_env accepts 1/true/yes/on (any case)")

    # ------------------------------------------------------------------ #
    # Bonus: __all__ is complete and consistent
    # ------------------------------------------------------------------ #
    print("\n=== Bonus: __all__ consistency ===")
    for name in __all__:
        assert name in dir(self_module) or name == "__version__", (
            f"__all__ exports {name!r} but it's not defined in the "
            f"package namespace")
    # Reverse check: every public (non-underscore) top-level name is in __all__.
    for name in dir(self_module):
        if name.startswith("_"):
            continue
        if name in ("logging", "os", "MappingProxyType", "Any", "Dict",
                    "List", "Mapping", "Optional", "Tuple", "Type"):
            continue
        assert name in __all__, (
            f"public name {name!r} not listed in __all__")
    print(f"  ✓ __all__ has {len(__all__)} entries; every "
          f"public name accounted for")

    print("\n✓ All self-tests passed.")


# --------------------------------------------------------------------------- #
# Direct-execution support
# --------------------------------------------------------------------------- #
# Because this is a package __init__.py, running `python path/to/__init__.py`
# fails on relative imports. Tests must be triggered via:
#
#     python -c "import dpq; dpq._run_selftests()"
#
# The `if __name__ == "__main__"` block below is unreachable for `__init__.py`
# (relative imports fail first) but is preserved for consistency with the
# previous eleven DPQ / distributed files.
if __name__ == "__main__":  # pragma: no cover
    _run_selftests()
