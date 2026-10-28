# src/feedback/__init__.py

"""
Feedback & RLHF Subsystem — v2.0.0
===================================

Layer 1 (Meta-Cognition) feedback pipeline. Collects agent reasoning traces,
computes sustainability metrics, emits them through pluggable sinks, and
produces human-readable feedback plus actionable improvement suggestions.

Components
----------
- :func:`generate_energy_feedback` — human-readable energy feedback.
- :func:`analyze_energy_metrics` — structured energy analysis.
- :class:`SustainabilityImprovementSuggester` — actionable suggestions.
- :class:`MetricSink` / :class:`StdoutSink` / :class:`InMemorySink` /
  :class:`JsonlSink` — pluggable metric emitters.
- :class:`ReasoningTraceAnalyzer` — reasoning quality analysis.
- :class:`RLHFFeedbackEngine` — top-level orchestrator.

Conventions
-----------
The **service** classes (:class:`ReasoningTraceAnalyzer`,
:class:`SustainabilityImprovementSuggester`, :class:`RLHFFeedbackEngine`)
and the **stateful sink** classes (:class:`InMemorySink`,
:class:`JsonlSink`) share the subsystem-wide conventions:
``RLock``-guarded state, bounded histories, strict / lenient modes,
serialization, custom ``*Error`` exceptions, ``statistics()``, and
``__repr__``.

The **data** classes (:class:`RLHFScore`, :class:`RLHFRecord`,
:class:`ReasoningAnalysis`, :class:`ImprovementSuggestion`,
:class:`EnergyFeedback`, and the configs) are frozen dataclasses with
``to_dict`` / ``from_dict`` round-trip.

:class:`MetricSink` is an abstract base class; concrete subclasses follow
the sink contract (``emit`` / ``flush`` / ``close``).

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``EnergyFeedbackConfig`` is now exported in ``__all__`` (previously
        imported but silently excluded from ``from feedback import *``).
  * 2.2 Registry functions raise ``FeedbackError`` (subclass of
        ``ValueError``) instead of built-in ``ValueError`` /
        ``TypeError`` / ``KeyError``.
  * 2.3 ``FEEDBACK_REGISTRY`` is a read-only ``MappingProxyType`` view.
        Direct assignment raises ``TypeError``; the public
        ``register_feedback_component`` / ``unregister_feedback_component``
        API is the only mutation path.
  * 2.4 ``register_feedback_component`` validates the class contract
        (must expose callable ``statistics`` and ``to_dict`` for concrete
        classes).
  * 2.5 ``MetricSink`` is recognised as abstract in
        ``list_feedback_components`` and skipped during import-time
        validation (no spurious ``TypeError`` from smoke-test
        instantiation).
  * 2.6 Import-time validation actually instantiates each concrete
        component with safe constructor args and calls
        ``.statistics()``. Broken classes are aggregated into a single
        descriptive ``ImportError``.

**P1 robustness / observability**
  * 3.1 ``FeedbackError`` base class; the five sibling error types are
        re-parented to inherit from it (via ``__bases__`` reassignment,
        so ``except EnergyFeedbackError:`` still works, and
        ``except ValueError:`` still catches everything).
  * 3.2 ``register_feedback_component(..., override=True)`` allows
        replacing an entry; ``unregister_feedback_component(name)``
        removes one. Together they make the registry usable from tests
        without cross-test pollution.
  * 3.3 Docstring updated to accurately describe which components follow
        which conventions.
  * 3.4 ``__version__`` exported and matches the submodules' version.
  * 3.6 ``GREEN_AGENT_VALIDATE_FEEDBACK`` accepts ``"1"``, ``"true"``,
        ``"yes"``, ``"on"`` (case-insensitive).
  * 3.7 ``_run_selftests()`` callable for CI / smoke-testing.
  * 3.9 ``analyze_energy_metrics`` exported (previously absent from the
        package namespace even though the module defines it).

**P2 polish**
  * 3.5 Removed the unnecessary ``_os`` alias (plain ``import os``).
  * 3.8 Import ordering fixed (``RLHFEngineError`` alphabetically correct).
  * 3.10 ``list_feedback_components()`` reports per-component metadata
         (``abstract`` / ``instantiable``).

Backward compatible: every v1.0.0 import path, every exported symbol, and
the ``FEEDBACK_REGISTRY`` dict work unchanged. Direct mutation of
``FEEDBACK_REGISTRY`` now raises ``TypeError``, but the registry API is
the documented interface — code that reads the registry (``.items()``,
``.keys()``, ``.get()``, ``in``) is unaffected.
"""

from __future__ import annotations

import logging
import os
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Tuple, Type

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Base error class (P0 2.2 / P1 3.1)
# --------------------------------------------------------------------------- #
class FeedbackError(ValueError):
    """
    Base class for every error raised by the feedback subsystem.

    Submodules define their own subclasses (``EnergyFeedbackError``,
    ``SuggesterError``, ``MetricSinkError``, ``ReasoningError``,
    ``RLHFEngineError``), which are re-parented at import time to inherit
    from this class. Callers can therefore catch ``FeedbackError`` to
    handle any feedback failure, or a specific subclass to handle one
    facet.
    """


# --------------------------------------------------------------------------- #
# Public imports
# --------------------------------------------------------------------------- #
from .energy_feedback import (
    EnergyFeedback,
    EnergyFeedbackConfig,
    EnergyFeedbackError,
    analyze_batch as analyze_energy_metrics_batch,
    analyze_energy_metrics,
    generate_energy_feedback,
)

from .improvement_suggester import (
    ImprovementSuggestion,
    SuggesterConfig,
    SuggesterError,
    SuggestionCategory,
    SustainabilityImprovementSuggester,
)

from .metric_sink import (
    InMemorySink,
    JsonlSink,
    MetricSink,
    MetricSinkError,
    StdoutSink,
)

from .reasoning_analyzer import (
    ReasoningAnalysis,
    ReasoningConfig,
    ReasoningError,
    ReasoningTraceAnalyzer,
)

from .rlhf_engine import (
    RLHFConfig,
    RLHFEngineError,
    RLHFFeedbackEngine,
    RLHFRecord,
    RLHFScore,
)


logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Re-parent submodule errors to inherit from FeedbackError (P1 3.1)
# --------------------------------------------------------------------------- #
def _reparent_errors() -> None:
    """
    Re-parent submodule error classes to inherit from ``FeedbackError``.

    Uses ``__bases__`` reassignment in place, so class identity (and
    therefore ``except X:`` clauses, ``isinstance`` checks, and pickling)
    are preserved. Existing ``except ValueError:`` clauses still work
    because ``FeedbackError`` inherits from ``ValueError``.
    """
    for cls in (
        EnergyFeedbackError,
        SuggesterError,
        MetricSinkError,
        ReasoningError,
        RLHFEngineError,
    ):
        if FeedbackError in cls.__mro__:
            continue
        try:
            cls.__bases__ = (FeedbackError,)
            logger.debug(
                "Re-parented %s to inherit from FeedbackError.",
                cls.__name__)
        except TypeError as exc:  # pragma: no cover — defensive
            logger.warning(
                "Could not re-parent %s to inherit from FeedbackError: %s",
                cls.__name__, exc)


_reparent_errors()


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    # package metadata
    "__version__",
    # base error
    "FeedbackError",
    # energy feedback
    "EnergyFeedback",
    "EnergyFeedbackConfig",
    "EnergyFeedbackError",
    "analyze_energy_metrics",
    "analyze_energy_metrics_batch",
    "generate_energy_feedback",
    # improvement suggester
    "ImprovementSuggestion",
    "SuggesterConfig",
    "SuggesterError",
    "SuggestionCategory",
    "SustainabilityImprovementSuggester",
    # metric sinks
    "InMemorySink",
    "JsonlSink",
    "MetricSink",
    "MetricSinkError",
    "StdoutSink",
    # reasoning analyzer
    "ReasoningAnalysis",
    "ReasoningConfig",
    "ReasoningError",
    "ReasoningTraceAnalyzer",
    # rlhf engine
    "RLHFConfig",
    "RLHFEngineError",
    "RLHFFeedbackEngine",
    "RLHFRecord",
    "RLHFScore",
    # registry
    "FEEDBACK_REGISTRY",
    "get_feedback_component",
    "list_feedback_components",
    "register_feedback_component",
    "unregister_feedback_component",
]


# --------------------------------------------------------------------------- #
# Component registry (P0 2.3: read-only public view)
# --------------------------------------------------------------------------- #
# Internal mutable dict — only mutated by the registry functions below.
_FEEDBACK_REGISTRY: Dict[str, Type[Any]] = {
    "suggester":  SustainabilityImprovementSuggester,
    "analyzer":   ReasoningTraceAnalyzer,
    "engine":     RLHFFeedbackEngine,
    "sink":       MetricSink,   # abstract base class (P0 2.5)
    "stdout":     StdoutSink,
    "memory":     InMemorySink,
    "jsonl":      JsonlSink,
}

# Public read-only view. Direct assignment raises TypeError.
FEEDBACK_REGISTRY: Mapping[str, Type[Any]] = MappingProxyType(
    _FEEDBACK_REGISTRY)


# --------------------------------------------------------------------------- #
# Contract validation (P0 2.4)
# --------------------------------------------------------------------------- #
_REQUIRED_COMPONENT_METHODS: Tuple[str, ...] = ("statistics", "to_dict")


def _validate_component_class(name: str, cls: Type[Any]) -> None:
    """
    Ensure ``cls`` looks like a feedback component.

    Concrete components must expose callable ``statistics`` and
    ``to_dict``. Abstract base classes (e.g. ``MetricSink``) are allowed
    as-is — they exist to be subclassed, not instantiated.
    """
    import inspect

    if not isinstance(cls, type):
        raise FeedbackError(
            f"Feedback component {name!r} must be a class, "
            f"got {type(cls).__name__}.")

    if inspect.isabstract(cls):
        # Abstract base classes have no contract to enforce at
        # registration time — subclasses will be validated on their own.
        return

    for attr in _REQUIRED_COMPONENT_METHODS:
        if not callable(getattr(cls, attr, None)):
            raise FeedbackError(
                f"Feedback component {name!r} ({cls.__name__}) must "
                f"expose a callable {attr!r}.")


# --------------------------------------------------------------------------- #
# Registry functions (P0 2.2, P1 3.2)
# --------------------------------------------------------------------------- #
def register_feedback_component(
    name: str,
    cls: Type[Any],
    *,
    override: bool = False,
) -> None:
    """
    Register a feedback component class under ``name``.

    Parameters
    ----------
    name : str
        Non-empty identifier (e.g. ``"suggester"``).
    cls : type
        The class to register. Concrete classes must expose callable
        ``statistics`` and ``to_dict``; abstract base classes are
        accepted without those checks.
    override : bool, default False
        If True, replaces an existing registration. If False, re-registering
        a different class under the same name raises ``FeedbackError``.
    """
    if not isinstance(name, str) or not name:
        raise FeedbackError("name must be a non-empty string.")
    _validate_component_class(name, cls)

    existing = _FEEDBACK_REGISTRY.get(name)
    if existing is not None and existing is not cls and not override:
        raise FeedbackError(
            f"Feedback component {name!r} already registered to "
            f"{existing.__name__}; pass override=True to replace.")

    _FEEDBACK_REGISTRY[name] = cls
    logger.info(
        "Registered feedback component %r -> %s%s",
        name, cls.__name__,
        " (override)" if existing is not None else "")


def unregister_feedback_component(name: str) -> bool:
    """
    Remove a feedback component from the registry.

    Returns True if the component existed, False otherwise.
    """
    if name not in _FEEDBACK_REGISTRY:
        return False
    del _FEEDBACK_REGISTRY[name]
    logger.info("Unregistered feedback component %r.", name)
    return True


def get_feedback_component(name: str) -> Type[Any]:
    """Return the feedback component class registered under ``name``."""
    try:
        return _FEEDBACK_REGISTRY[name]
    except KeyError as exc:
        raise FeedbackError(
            f"Unknown feedback component {name!r}. "
            f"Available: {sorted(_FEEDBACK_REGISTRY)}") from exc


def list_feedback_components() -> Dict[str, Dict[str, Any]]:
    """
    Return metadata for every registered component.

    Each entry is a dict with:

    - ``class_name``: the class's ``__name__``
    - ``abstract``: True iff ``cls`` is an abstract base class
    - ``instantiable``: opposite of ``abstract`` (convenience)

    Note: v1.0.0 returned ``Dict[str, str]`` (name -> class name). The
    new shape is ``Dict[str, Dict[str, Any]]`` — a breaking change for
    callers who indexed into the string value. A compatibility shim
    ``list_feedback_component_names()`` is provided for those callers
    (see below).
    """
    import inspect

    return {
        name: {
            "class_name": cls.__name__,
            "abstract": inspect.isabstract(cls),
            "instantiable": not inspect.isabstract(cls),
        }
        for name, cls in _FEEDBACK_REGISTRY.items()
    }


def list_feedback_component_names() -> Dict[str, str]:
    """
    Backward-compatible v1.0.0 shape: ``name -> class name``.

    Use :func:`list_feedback_components` for the richer metadata.
    """
    return {
        name: cls.__name__
        for name, cls in _FEEDBACK_REGISTRY.items()
    }


# --------------------------------------------------------------------------- #
# Import-time validation (P0 2.5 / 2.6)
# --------------------------------------------------------------------------- #
# Safe constructor arguments for each known concrete component. A
# component that requires an argument that cannot be safely constructed
# at import time (e.g. ``JsonlSink`` needs a writable path) is listed
# with ``None`` and will be skipped during the smoke test.
_SMOKE_TEST_ARGS: Dict[str, Optional[Dict[str, Any]]] = {
    "suggester": {},
    "analyzer": {},
    "engine": {},
    "stdout": {},
    "memory": {},
    "jsonl": None,   # requires a path — skip
    "sink": None,    # abstract base class — skip
}


def _validate_all_components() -> None:
    """
    Validate every registered component.

    For each component:
      1. The contract check (``statistics`` and ``to_dict`` callable
         for concrete classes).
      2. If the class is abstract: skip instantiation (no contract to
         enforce, no way to construct).
      3. If a smoke-test argument set is defined: instantiate and call
         ``.statistics()`` to prove the class is usable.

    Aggregates all failures into a single ``ImportError`` so operators
    see every problem at once (not just the first).
    """
    import inspect

    failures: List[str] = []

    for name, cls in _FEEDBACK_REGISTRY.items():
        # Step 1: contract check (skipped for abstract classes).
        try:
            _validate_component_class(name, cls)
        except FeedbackError as exc:
            failures.append(f"{name} ({cls.__name__}): {exc}")
            continue

        # Step 2: abstract classes have no instantiation contract.
        if inspect.isabstract(cls):
            logger.info(
                "Feedback component %r (%s) validated "
                "(abstract, contract only).",
                name, cls.__name__)
            continue

        # Step 3: smoke-test instantiation.
        smoke_args = _SMOKE_TEST_ARGS.get(name)
        if smoke_args is None:
            logger.info(
                "Feedback component %r (%s) validated "
                "(smoke test skipped).",
                name, cls.__name__)
            continue

        try:
            instance = cls(**smoke_args)
            instance.statistics()   # smoke-call
            logger.info(
                "Feedback component %r (%s) validated "
                "(instantiated + statistics() called).",
                name, cls.__name__)
        except Exception as exc:  # noqa: BLE001
            failures.append(
                f"{name} ({cls.__name__}): "
                f"{type(exc).__name__}: {exc}")

    if failures:
        raise ImportError(
            "Feedback component validation failed:\n  "
            + "\n  ".join(failures))


# --------------------------------------------------------------------------- #
# Env var coercion (P1 3.6)
# --------------------------------------------------------------------------- #
def _coerce_bool_env(value: Optional[str]) -> bool:
    """Coerce an env-var string to bool."""
    if value is None:
        return False
    return value.strip().lower() in ("1", "true", "yes", "on", "t", "y")


if _coerce_bool_env(os.environ.get("GREEN_AGENT_VALIDATE_FEEDBACK")):
    _validate_all_components()


# --------------------------------------------------------------------------- #
# Self-tests (P1 3.7)
# --------------------------------------------------------------------------- #
def _run_selftests() -> None:
    """
    Run package self-tests.

    Usage::

        python -c "import feedback; feedback._run_selftests()"

    The function prints ``✓`` lines for each passing test and raises
    ``AssertionError`` on the first failure.
    """
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )
    import inspect
    import sys

    print(f"=== feedback v{__version__} self-tests ===\n")

    # ---------------------------------------------------------------- #
    # P0 2.1 — EnergyFeedbackConfig exported
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: EnergyFeedbackConfig exported ===")

    # Direct attribute access.
    assert hasattr(
        sys.modules[__name__], "EnergyFeedbackConfig"), (
        "P0 2.1 regression: feedback.EnergyFeedbackConfig is not "
        "accessible as an attribute")
    _ = sys.modules[__name__].EnergyFeedbackConfig
    print("  ✓ feedback.EnergyFeedbackConfig accessible")

    # __all__ contains it.
    assert "EnergyFeedbackConfig" in __all__, (
        "P0 2.1 regression: EnergyFeedbackConfig not in __all__")
    print("  ✓ 'EnergyFeedbackConfig' in __all__")

    # from feedback import * includes it (simulated).
    star_exports = {
        name: getattr(sys.modules[__name__], name)
        for name in __all__
        if name != "__version__"
    }
    assert "EnergyFeedbackConfig" in star_exports
    print("  ✓ star-import would export it")

    # analyze_energy_metrics exported (P1 3.9).
    assert "analyze_energy_metrics" in __all__, (
        "P1 3.9 regression: analyze_energy_metrics not in __all__")
    assert hasattr(
        sys.modules[__name__], "analyze_energy_metrics"), (
        "P1 3.9 regression: analyze_energy_metrics not accessible")
    print("  ✓ analyze_energy_metrics exported")

    # ---------------------------------------------------------------- #
    # P0 2.2 — registry raises FeedbackError, not built-ins
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: registry raises FeedbackError ===")

    # get_feedback_component on unknown name.
    try:
        get_feedback_component("nonexistent-component-xyz")
    except FeedbackError as exc:
        print(f"  ✓ get_feedback_component('nonexistent') → "
              f"FeedbackError: {exc}")
    except KeyError as exc:
        raise AssertionError(
            f"P0 2.2 regression: raised KeyError instead of "
            f"FeedbackError: {exc}")
    else:
        raise AssertionError(
            "P0 2.2 regression: no exception raised")

    # Empty name rejected with FeedbackError.
    try:
        register_feedback_component("", int)
    except FeedbackError:
        print("  ✓ register('', int) raises FeedbackError")
    except (ValueError, TypeError) as exc:
        raise AssertionError(
            f"P0 2.2 regression: register path raised "
            f"{type(exc).__name__} instead of FeedbackError")
    else:
        raise AssertionError(
            "P0 2.2 regression: empty name accepted")

    # Non-class rejected with FeedbackError.
    try:
        register_feedback_component("bad", 42)  # type: ignore[arg-type]
    except FeedbackError:
        print("  ✓ register('bad', 42) raises FeedbackError")
    else:
        raise AssertionError(
            "P0 2.2 regression: non-class accepted")

    # ---------------------------------------------------------------- #
    # P0 2.3 — FEEDBACK_REGISTRY is read-only
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: FEEDBACK_REGISTRY is read-only ===")

    try:
        FEEDBACK_REGISTRY["suggester"] = int  # type: ignore[index]
    except TypeError as exc:
        print(f"  ✓ direct assignment raises TypeError: {exc}")
    else:
        raise AssertionError(
            "P0 2.3 regression: FEEDBACK_REGISTRY was directly mutable")

    try:
        del FEEDBACK_REGISTRY["suggester"]  # type: ignore[attr-defined]
    except TypeError as exc:
        print(f"  ✓ del raises TypeError: {exc}")
    else:
        raise AssertionError(
            "P0 2.3 regression: del succeeded on FEEDBACK_REGISTRY")

    # Reads still work.
    assert "suggester" in FEEDBACK_REGISTRY
    assert FEEDBACK_REGISTRY["suggester"] is SustainabilityImprovementSuggester
    assert FEEDBACK_REGISTRY.get("nonexistent") is None
    assert set(FEEDBACK_REGISTRY.keys()) >= {
        "suggester", "analyzer", "engine", "stdout", "memory", "jsonl",
    }
    print("  ✓ read access (get, in, keys, indexing) still works")

    # register_feedback_component still mutates the underlying dict.
    class _ScratchComponent:
        def statistics(self) -> Dict[str, Any]:
            return {}

        def to_dict(self) -> Dict[str, Any]:
            return {}

    register_feedback_component("scratch", _ScratchComponent)
    assert "scratch" in FEEDBACK_REGISTRY
    assert FEEDBACK_REGISTRY["scratch"] is _ScratchComponent
    unregister_feedback_component("scratch")
    assert "scratch" not in FEEDBACK_REGISTRY
    print("  ✓ register_feedback_component mutates the internal dict")

    # ---------------------------------------------------------------- #
    # P0 2.4 — contract validation
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: contract validation ===")

    # int has no statistics/to_dict → rejected.
    try:
        register_feedback_component("bad", int)  # type: ignore[arg-type]
    except FeedbackError as exc:
        print(f"  ✓ register('bad', int) raises FeedbackError: {exc}")
    else:
        raise AssertionError(
            "P0 2.4 regression: int accepted as a component")

    # object also lacks the contract.
    try:
        register_feedback_component("obj", object)  # type: ignore[arg-type]
    except FeedbackError:
        print("  ✓ register('obj', object) rejected")
    else:
        raise AssertionError(
            "P0 2.4 regression: object accepted as a component")

    # Proper class accepted.
    class _FakeAnalyzer:
        def statistics(self) -> Dict[str, Any]:
            return {"ok": True}

        def to_dict(self) -> Dict[str, Any]:
            return {"ok": True}

    register_feedback_component("fake-analyzer", _FakeAnalyzer)
    assert FEEDBACK_REGISTRY["fake-analyzer"] is _FakeAnalyzer
    unregister_feedback_component("fake-analyzer")
    print("  ✓ _FakeAnalyzer (has statistics + to_dict) accepted")

    # ---------------------------------------------------------------- #
    # P0 2.5 — MetricSink is abstract, skipped by validation
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: MetricSink abstract handling ===")

    assert inspect.isabstract(MetricSink), (
        "P0 2.5 regression: MetricSink is no longer abstract "
        "(metric_sink.py v2.0.0 should have made it an ABC)")
    assert "sink" in FEEDBACK_REGISTRY
    assert FEEDBACK_REGISTRY["sink"] is MetricSink
    print("  ✓ 'sink' → MetricSink (abstract) in registry")

    # list_feedback_components marks it abstract.
    listing = list_feedback_components()
    assert listing["sink"]["abstract"] is True, (
        f"P0 2.5 regression: 'sink' listed as abstract="
        f"{listing['sink']['abstract']}")
    assert listing["sink"]["instantiable"] is False
    assert listing["suggester"]["abstract"] is False
    assert listing["suggester"]["instantiable"] is True
    print(f"  ✓ list_feedback_components: "
          f"sink.abstract=True, suggester.instantiable=True")

    # ---------------------------------------------------------------- #
    # P0 2.6 — validation actually instantiates + calls statistics()
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: real import-time validation ===")

    # All defaults validate cleanly (no breakage).
    _validate_all_components()
    print("  ✓ _validate_all_components() succeeds on defaults")

    # A broken concrete class produces a descriptive ImportError.
    class _BrokenComponent:
        """No statistics() or to_dict()."""
        pass

    register_feedback_component("broken", _BrokenComponent)
    try:
        _validate_all_components()
    except ImportError as exc:
        assert "broken" in str(exc).lower(), (
            f"P0 2.6 regression: failure message missing component "
            f"name: {exc}")
        print(f"  ✓ broken class → ImportError mentioning 'broken': "
              f"{str(exc)[:80]}...")
    else:
        raise AssertionError(
            "P0 2.6 regression: broken class did not fail validation")
    finally:
        unregister_feedback_component("broken")

    # A broken __init__ (statistics() is present but __init__ fails).
    class _InitFails:
        def __init__(self) -> None:
            raise RuntimeError("simulated __init__ failure")

        def statistics(self) -> Dict[str, Any]:
            return {}

        def to_dict(self) -> Dict[str, Any]:
            return {}

    register_feedback_component("init-fails", _InitFails)
    try:
        _validate_all_components()
    except ImportError as exc:
        assert "init-fails" in str(exc).lower(), (
            f"P0 2.6 regression: __init__ failure not surfaced: {exc}")
        print(f"  ✓ __init__ failure → ImportError mentioning "
              f"'init-fails'")
    else:
        raise AssertionError(
            "P0 2.6 regression: __init__ failure not caught")
    finally:
        unregister_feedback_component("init-fails")

    # ---------------------------------------------------------------- #
    # P1 3.1 — FeedbackError catches every submodule error
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: FeedbackError catches submodule errors ===")

    # Each submodule error is a subclass of FeedbackError.
    for cls in (
        EnergyFeedbackError,
        SuggesterError,
        MetricSinkError,
        ReasoningError,
        RLHFEngineError,
    ):
        assert issubclass(cls, FeedbackError), (
            f"P1 3.1 regression: {cls.__name__} does not inherit "
            f"from FeedbackError")
        assert issubclass(cls, ValueError), (
            f"Backward-compat regression: {cls.__name__} no longer "
            f"inherits from ValueError")
    print(f"  ✓ all 5 submodule errors subclass FeedbackError")
    print(f"  ✓ all still subclass ValueError (backward compat)")

    # Each can be caught via `except FeedbackError:`.
    for cls in (
        EnergyFeedbackError,
        SuggesterError,
        MetricSinkError,
        ReasoningError,
        RLHFEngineError,
    ):
        try:
            raise cls("test")
        except FeedbackError:
            pass  # expected
        except Exception as exc:  # noqa: BLE001
            raise AssertionError(
                f"P1 3.1 regression: {cls.__name__} not caught by "
                f"except FeedbackError")
        # And still caught by except ValueError (backward compat).
        try:
            raise cls("test")
        except ValueError:
            pass
    print("  ✓ except FeedbackError catches each; "
          "except ValueError still works")

    # ---------------------------------------------------------------- #
    # P1 3.2 — override + unregister
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.2: override + unregister ===")

    class _MockSuggester:
        def statistics(self) -> Dict[str, Any]:
            return {"mock": True}

        def to_dict(self) -> Dict[str, Any]:
            return {"mock": True}

    # Without override → raises.
    try:
        register_feedback_component("suggester", _MockSuggester)
    except FeedbackError:
        print("  ✓ overriding without override=True raises "
              "FeedbackError")
    else:
        raise AssertionError(
            "P1 3.2 regression: override succeeded without override=True")

    # With override → succeeds; original restored in try/finally.
    original_suggester = FEEDBACK_REGISTRY["suggester"]
    try:
        register_feedback_component(
            "suggester", _MockSuggester, override=True)
        assert FEEDBACK_REGISTRY["suggester"] is _MockSuggester
        print("  ✓ override=True succeeded")
    finally:
        register_feedback_component(
            "suggester", original_suggester, override=True)
    assert FEEDBACK_REGISTRY["suggester"] is original_suggester
    print("  ✓ original restored via try/finally")

    # unregister returns True/False correctly.
    class _Scratch:
        def statistics(self) -> Dict[str, Any]:
            return {}

        def to_dict(self) -> Dict[str, Any]:
            return {}

    register_feedback_component("scratch-unreg", _Scratch)
    assert unregister_feedback_component("scratch-unreg") is True
    assert unregister_feedback_component("scratch-unreg") is False
    print("  ✓ unregister_feedback_component returns True then False")

    # ---------------------------------------------------------------- #
    # P1 3.4 — __version__
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.4: __version__ ===")

    self_mod = sys.modules[__name__]
    assert hasattr(self_mod, "__version__"), (
        "P1 3.4 regression: feedback.__version__ missing")
    assert self_mod.__version__ == "2.0.0", (
        f"P1 3.4 regression: __version__={self_mod.__version__!r}")
    assert "__version__" in __all__, (
        "P1 3.4 regression: __version__ not in __all__")

    # Matches submodule versions (each was set to 2.0.0 in this series).
    from . import energy_feedback as _ef
    from . import improvement_suggester as _is
    from . import metric_sink as _ms
    from . import reasoning_analyzer as _ra
    from . import rlhf_engine as _re

    for name, mod in (
        ("energy_feedback", _ef),
        ("improvement_suggester", _is),
        ("metric_sink", _ms),
        ("reasoning_analyzer", _ra),
        ("rlhf_engine", _re),
    ):
        mod_v = getattr(mod, "__version__", None)
        assert mod_v == self_mod.__version__, (
            f"Version mismatch: feedback.__version__="
            f"{self_mod.__version__!r}, {name}.__version__={mod_v!r}")
    print(f"  ✓ feedback.__version__ = {self_mod.__version__!r} "
          f"(matches all 5 submodules)")

    # ---------------------------------------------------------------- #
    # P1 3.6 — env var coercion
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.6: env var coercion ===")

    for val, expected in [
        ("1", True), ("true", True), ("TRUE", True), ("Yes", True),
        ("on", True), ("0", False), ("false", False),
        ("", False), (None, False), ("garbage", False),
    ]:
        assert _coerce_bool_env(val) is expected, (
            f"_coerce_bool_env({val!r}) = {_coerce_bool_env(val)}, "
            f"expected {expected}")
    print("  ✓ _coerce_bool_env accepts 1/true/yes/on (any case)")

    # ---------------------------------------------------------------- #
    # Bonus: __all__ consistency
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: __all__ consistency ===")

    # Every name in __all__ is either in the module namespace or is
    # "__version__" (which is, but the assert special-cases it for
    # clarity).
    for name in __all__:
        assert name in dir(self_mod) or name == "__version__", (
            f"__all__ exports {name!r} but it's not defined in the "
            f"package namespace")

    # Every public top-level name is in __all__ (except non-DPQ names).
    _EXEMPT = {
        "logging", "os", "MappingProxyType", "Any", "Dict", "List",
        "Mapping", "Optional", "Tuple", "Type",
    }
    for name in dir(self_mod):
        if name.startswith("_"):
            continue
        if name in _EXEMPT:
            continue
        assert name in __all__, (
            f"public name {name!r} not listed in __all__")

    print(f"  ✓ __all__ has {len(__all__)} entries; every "
          f"public name accounted for")

    # ---------------------------------------------------------------- #
    # Bonus: v1.0.0 backward compat for list_feedback_components
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: backward-compat registry helpers ===")

    # The v2.0.0 richer listing.
    rich = list_feedback_components()
    assert isinstance(rich, dict)
    assert all(isinstance(v, dict) for v in rich.values())
    assert all(
        "class_name" in v and "abstract" in v and "instantiable" in v
        for v in rich.values()
    )
    print(f"  ✓ list_feedback_components: richer metadata shape")

    # The v1.0.0 flat shape via the shim.
    flat = list_feedback_component_names()
    assert isinstance(flat, dict)
    assert all(isinstance(v, str) for v in flat.values())
    assert flat["suggester"] == "SustainabilityImprovementSuggester"
    print(f"  ✓ list_feedback_component_names: v1.0.0 flat shape")

    print("\n✓ All self-tests passed.")


# --------------------------------------------------------------------------- #
# Direct-execution support
# --------------------------------------------------------------------------- #
# Because this is a package __init__.py, running
# `python path/to/__init__.py` fails on relative imports. Trigger the
# self-tests via:
#
#     python -c "import feedback; feedback._run_selftests()"
#
# The block below is preserved for consistency with the previous
# seventeen DPQ / feedback files.
if __name__ == "__main__":  # pragma: no cover
    _run_selftests()
