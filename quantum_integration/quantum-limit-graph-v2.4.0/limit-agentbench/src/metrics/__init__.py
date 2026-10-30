# src/metrics/__init__.py

"""
Metrics package for Green_Agent (v5.1)
======================================

Exposes classical efficiency calculators, normalized metrics, quantum
efficiency tracking, and episode records at the package level, plus a
name-based registry for discovery by benchmarking / dashboard layers.

Fixes in v5.1 (see review)
--------------------------
  * P0 — Re-exports ``EpisodeRecord``, ``EpisodeRecordError``, and
         ``create_episode_record``. Previously these were only importable
         via ``from metrics.episode_record import ...`` even though the
         record is the primary type every other metric consumes.
  * P0 — Re-exports every public name from the submodules' ``__all__``.
         ``EfficiencyCalculatorError``, ``NormalizationConfig``,
         ``ComparisonSample``, and ``EfficiencyCalculator`` are now
         importable from the package. Callers catching the documented
         exception type no longer get ``ImportError``.
  * P0 — ``sustainability_index`` is a tolerant import. A missing
         dependency (or the module itself) no longer breaks every
         consumer of the metrics package. The availability flag is
         surfaced via ``SUSTAINABILITY_AVAILABLE`` and
         ``get_capabilities()["features"]["sustainability_index"]``.
  * P0 — The env-var validation hook is now implemented via
         ``test_smoke()``, which deduplicates aliases, returns a
         structured report, and can be called from tests without setting
         an environment variable. The old loop instantiated aliases twice
         and would abort the entire import on a TypeError for any future
         metric with a required constructor argument.
  * P1 — ``get_metric`` raises ``MetricsError`` (a ``ValueError``
         subclass) instead of ``KeyError``, matching the rest of the
         codebase. ``from None`` suppresses the chained ``KeyError``.
  * P1 — Registry supports ``override=True``, ``unregister_metric``, and
         an ``override_metric`` context manager so tests can isolate
         registrations without polluting global state.
  * P1 — ``register_metric`` accepts an optional ``expected_base`` for
         plugin type-checking and fixes the argument-check order.
  * P1 — Package ``__version__`` plus a submodule version check that
         warns on partial upgrades.
  * P2 — ``get_capabilities()``, ``health()``, ``test_smoke()``,
         ``__dir__()``.
  * P3 — ``import os`` at the top of the file (previously ``import os as
         _os`` mid-file); ``__main__`` smoke test that exits nonzero on
         failure; ``logger.info`` → ``logger.debug`` on the hot path.

Backward compatibility
----------------------
- All names that were in the v5.0 ``__all__`` remain importable.
- ``get_metric`` for an existing name is unchanged; only the error type
  on the *unknown-name* path changed (``KeyError`` → ``MetricsError``).
  ``MetricsError`` subclasses ``ValueError``, so ``except ValueError``
  still works.
- ``register_metric(name, cls)`` keeps its positional signature; new
  ``override=`` and ``expected_base=`` are keyword-only with defaults.
- ``sustainability_index`` being tolerant is transparent to callers who
  have the dependency installed.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Type

logger = logging.getLogger(__name__)

__version__ = "5.1.0"

#: Oldest submodule version this package can safely drive.
MIN_SUBMODULE_VERSION = "5.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MetricsError(ValueError):
    """
    Raised for invalid metric lookup or registry configuration.

    Submodule-specific errors (``EfficiencyCalculatorError``,
    ``QuantumEfficiencyError``, ``EpisodeRecordError``) all subclass
    ``ValueError``, so ``except ValueError`` catches all of them plus
    ``MetricsError``.
    """


# --------------------------------------------------------------------------- #
# Submodule imports + version diagnostics
# --------------------------------------------------------------------------- #
# Import module objects first so we can inspect their ``__version__`` after.
# The imports below are the same modules; the aliases just give us a handle
# for the version check without a second import pass.
from . import efficiency_calculator as _eff_module
from . import episode_record as _episode_module
from . import quantum_efficiency as _quantum_module

# --- Classical / general metrics ---
from .efficiency_calculator import (
    ComparisonSample,
    EfficiencyCalculator,
    EfficiencyCalculatorError,
    NormalizationConfig,
    NormalizedEfficiencyCalculator,
)

# --- Episode record (primary exchange format between episodes and metrics) ---
from .episode_record import (
    EpisodeRecord,
    EpisodeRecordError,
    create_episode_record,
)

# --- Quantum metrics ---
from .quantum_efficiency import (
    EfficiencySample,
    QuantumEfficiencyError,
    QuantumEfficiencyMetric,
)

# --- Sustainability index (tolerant import) ---
# A missing dependency in ``sustainability_index`` should not break every
# consumer of the metrics package. The availability flag lets callers and
# ``health()`` report the state.
_SUSTAINABILITY_IMPORT_ERROR: Optional[str] = None
try:
    from .sustainability_index import SustainabilityIndexCalculator
    SUSTAINABILITY_AVAILABLE = True
except ImportError as exc:  # pragma: no cover — CI catches at test time
    SustainabilityIndexCalculator = None  # type: ignore[assignment, misc]
    SUSTAINABILITY_AVAILABLE = False
    _SUSTAINABILITY_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"
    logger.warning(
        "metrics.sustainability_index is unavailable: %s",
        _SUSTAINABILITY_IMPORT_ERROR,
    )


def _check_submodule_versions() -> None:
    """Warn (not raise) when submodules are older than the minimum."""
    try:
        from packaging.version import InvalidVersion, Version
    except ImportError:
        logger.debug("packaging not installed; skipping version check")
        return
    required = Version(MIN_SUBMODULE_VERSION)
    for mod, label in (
        (_eff_module, "efficiency_calculator"),
        (_episode_module, "episode_record"),
        (_quantum_module, "quantum_efficiency"),
    ):
        v = getattr(mod, "__version__", None)
        if not v:
            logger.debug("%s has no __version__; skipping check", label)
            continue
        try:
            installed = Version(v)
        except InvalidVersion:
            logger.warning(
                "%s has non-PEP-440 version %r; skipping check", label, v,
            )
            continue
        if installed < required:
            logger.warning(
                "metrics requires %s >= %s; found %s. Some features may "
                "be missing.",
                label, MIN_SUBMODULE_VERSION, v,
            )


_check_submodule_versions()


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
_MISSING = object()

METRIC_REGISTRY: Dict[str, Type[Any]] = {
    "classical":          NormalizedEfficiencyCalculator,
    "normalized":         NormalizedEfficiencyCalculator,
    "quantum":            QuantumEfficiencyMetric,
    "quantum_efficiency": QuantumEfficiencyMetric,
}

# Only register sustainability when the submodule imported successfully.
if SUSTAINABILITY_AVAILABLE and SustainabilityIndexCalculator is not None:
    METRIC_REGISTRY["sustainability"] = SustainabilityIndexCalculator


def register_metric(
    name: str,
    cls: Type[Any],
    *,
    override: bool = False,
    expected_base: Optional[type] = None,
) -> None:
    """
    Register a metric class under ``name``.

    Parameters
    ----------
    name : str
        Non-empty identifier.
    cls : type
        The metric class.
    override : bool, default False
        When True, replace any existing registration. When False, a
        conflicting registration raises ``ValueError``; re-registering the
        *same* class is a no-op.
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
            f"Metric name must be a non-empty string, "
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
    existing = METRIC_REGISTRY.get(name, _MISSING)
    if existing is not _MISSING and existing is not cls and not override:
        raise ValueError(
            f"Metric '{name}' already registered to "
            f"{existing.__name__}. Pass override=True to replace."
        )
    METRIC_REGISTRY[name] = cls
    logger.debug(
        "Registered metric '%s' -> %s", name, cls.__name__,
    )


def unregister_metric(name: str) -> None:
    """
    Remove the registration under ``name``.

    Raises
    ------
    MetricsError
        When ``name`` isn't registered.
    """
    if name not in METRIC_REGISTRY:
        raise MetricsError(
            f"Unknown metric '{name}'. "
            f"Available: {sorted(METRIC_REGISTRY)}"
        )
    METRIC_REGISTRY.pop(name)
    logger.debug("Unregistered metric '%s'", name)


@contextmanager
def override_metric(name: str, cls: Type[Any]) -> Iterator[None]:
    """
    Temporarily override (or add) a metric registration; restore the
    previous entry on exit. Intended for test isolation::

        with override_metric("classical", FakeMetric):
            ...  # registry contains FakeMetric
        # registry restored to prior state
    """
    if not isinstance(name, str) or not name:
        raise ValueError("name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    prev = METRIC_REGISTRY.get(name, _MISSING)
    METRIC_REGISTRY[name] = cls
    try:
        yield
    finally:
        if prev is _MISSING:
            METRIC_REGISTRY.pop(name, None)
        else:
            METRIC_REGISTRY[name] = prev  # type: ignore[assignment]


def get_metric(name: str) -> Type[Any]:
    """
    Return the metric class registered under ``name``.

    Raises
    ------
    MetricsError
        When ``name`` isn't registered, or when ``name`` refers to
        ``"sustainability"`` but the submodule failed to import.
    """
    try:
        return METRIC_REGISTRY[name]
    except KeyError:
        if name == "sustainability" and not SUSTAINABILITY_AVAILABLE:
            raise MetricsError(
                f"Metric 'sustainability' is unavailable: "
                f"{_SUSTAINABILITY_IMPORT_ERROR}. Install the required "
                f"dependencies to enable it."
            ) from None
        raise MetricsError(
            f"Unknown metric '{name}'. "
            f"Available: {sorted(METRIC_REGISTRY)}"
        ) from None


def list_metrics() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {
        name: cls.__name__
        for name, cls in METRIC_REGISTRY.items()
        if cls is not None
    }


# --------------------------------------------------------------------------- #
# Diagnostics
# --------------------------------------------------------------------------- #
def get_capabilities() -> Dict[str, Any]:
    """Structured capability report suitable for logs and tickets."""
    return {
        "package_version": __version__,
        "submodule_versions": {
            "efficiency_calculator": getattr(
                _eff_module, "__version__", None,
            ),
            "episode_record": getattr(
                _episode_module, "__version__", None,
            ),
            "quantum_efficiency": getattr(
                _quantum_module, "__version__", None,
            ),
        },
        "min_submodule_version": MIN_SUBMODULE_VERSION,
        "features": {
            "classical_efficiency": True,
            "quantum_efficiency": True,
            "episode_record": True,
            "sustainability_index": SUSTAINABILITY_AVAILABLE,
        },
        "sustainability_import_error": _SUSTAINABILITY_IMPORT_ERROR,
        "registered_metrics": list_metrics(),
        "metric_count": len(METRIC_REGISTRY),
    }


def health() -> Dict[str, Any]:
    """
    Compact health report suitable for ``/health`` endpoints and CI gates.

    ``ok`` is ``True`` when the essential submodules imported and can
    instantiate. The optional ``sustainability_index`` is reported
    separately and does not gate health.
    """
    caps = get_capabilities()
    smoke = test_smoke()
    return {
        "ok": smoke["ok"],
        "package_version": caps["package_version"],
        "metric_count": caps["metric_count"],
        "sustainability_available": caps["features"]["sustainability_index"],
        "instantiation": smoke["steps"],
    }


def test_smoke() -> Dict[str, Any]:
    """
    Structured health check: instantiate each distinct metric class once.

    Deduplicates aliases (so ``classical`` and ``normalized`` only
    instantiate ``NormalizedEfficiencyCalculator`` a single time) and
    never raises. Callers can use this from tests without setting
    ``GREEN_AGENT_VALIDATE_METRICS``.
    """
    results: Dict[str, Any] = {
        "ok": False,
        "package_version": __version__,
        "steps": [],
    }
    seen: set[int] = set()
    for name, cls in METRIC_REGISTRY.items():
        if cls is None or id(cls) in seen:
            continue
        seen.add(id(cls))
        try:
            cls()  # default constructor — every current metric supports it
            results["steps"].append(
                {"name": f"instantiate:{name}", "ok": True}
            )
        except Exception as exc:  # noqa: BLE001 — diagnostic only
            results["steps"].append({
                "name": f"instantiate:{name}",
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
            })
    results["ok"] = (
        len(results["steps"]) > 0
        and all(s.get("ok") for s in results["steps"])
    )
    return results


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    # --- Classical / general metrics ---
    "ComparisonSample",
    "EfficiencyCalculator",
    "EfficiencyCalculatorError",
    "NormalizationConfig",
    "NormalizedEfficiencyCalculator",
    # --- Episode record ---
    "EpisodeRecord",
    "EpisodeRecordError",
    "create_episode_record",
    # --- Quantum metrics ---
    "EfficiencySample",
    "QuantumEfficiencyError",
    "QuantumEfficiencyMetric",
    # --- Package errors ---
    "MetricsError",
    # --- Registry ---
    "METRIC_REGISTRY",
    "get_metric",
    "list_metrics",
    "register_metric",
    "unregister_metric",
    "override_metric",
    # --- Diagnostics ---
    "get_capabilities",
    "health",
    "test_smoke",
    "SUSTAINABILITY_AVAILABLE",
    # --- Metadata ---
    "__version__",
]

# Re-export SustainabilityIndexCalculator only when it imported successfully.
if SUSTAINABILITY_AVAILABLE and SustainabilityIndexCalculator is not None:
    __all__.append("SustainabilityIndexCalculator")


# --------------------------------------------------------------------------- #
# Optional env-var gate for CI smoke test
# --------------------------------------------------------------------------- #
if os.environ.get("GREEN_AGENT_VALIDATE_METRICS") == "1":
    _report = test_smoke()
    if not _report["ok"]:
        raise RuntimeError(
            f"Metrics validation failed: {_report}"
        )
    logger.info(
        "Metrics package validation passed (package v%s, %d "
        "class(es) instantiated).",
        __version__, len(_report["steps"]),
    )


# --------------------------------------------------------------------------- #
# Local smoke test: python -m metrics
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import json

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    report = test_smoke()
    print(json.dumps(report, indent=2, default=str))
    assert report["ok"], report

    print("\nRegistered metrics:", list_metrics())
    print("Capabilities     :", json.dumps(
        get_capabilities(), indent=2, default=str,
    ))

    # --- Registry override + unregister round-trip ---
    class _FakeMetric:
        pass

    with override_metric("classical", _FakeMetric):
        assert get_metric("classical") is _FakeMetric
    # Restored:
    assert get_metric("classical") is NormalizedEfficiencyCalculator

    register_metric("temp", _FakeMetric)
    assert get_metric("temp") is _FakeMetric
    unregister_metric("temp")
    try:
        get_metric("temp")
    except MetricsError:
        pass
    else:
        raise AssertionError("unregister_metric did not remove the entry.")

    # --- Unknown metric raises MetricsError ---
    try:
        get_metric("does_not_exist")
    except MetricsError as exc:
        print("Unknown metric:", exc)
    else:
        raise AssertionError("get_metric did not raise for unknown name.")

    # --- Conflict without override raises ---
    try:
        register_metric("classical", _FakeMetric)
    except ValueError as exc:
        print("Conflict rejected:", exc)
    else:
        raise AssertionError(
            "register_metric should have rejected the conflict."
        )

    # --- override=True succeeds ---
    register_metric("classical", _FakeMetric, override=True)
    assert get_metric("classical") is _FakeMetric
    # Restore for any subsequent test in this session
    register_metric(
        "classical", NormalizedEfficiencyCalculator, override=True,
    )

    # --- expected_base type guard ---
    try:
        register_metric(
            "bad", _FakeMetric, expected_base=NormalizedEfficiencyCalculator,
        )
    except TypeError as exc:
        print("expected_base rejected:", exc)
    else:
        raise AssertionError(
            "expected_base should have rejected the non-subclass."
        )

    # --- All v5.1 exports importable ---
    from metrics import (  # noqa: F401
        ComparisonSample,
        EfficiencyCalculator,
        EfficiencyCalculatorError,
        EpisodeRecord,
        EpisodeRecordError,
        NormalizationConfig,
        create_episode_record,
    )
    print("\nAll v5.1 package-level exports import OK.")
    print("\nSmoke test passed.")
