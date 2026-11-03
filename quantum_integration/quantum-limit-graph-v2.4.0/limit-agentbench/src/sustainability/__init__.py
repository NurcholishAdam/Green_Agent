# src/sustainability/__init__.py

"""
Sustainability modules for Green_Agent
======================================

Carbon-awareness and eco-mode control. Five sub-modules and two Protocols:

Components (instantiable)
-------------------------
- :class:`CarbonForecast` — moving-average / EWMA carbon intensity predictor.
- :class:`CarbonIntensityProvider` — current intensity + zone classification.
- :class:`EcoModeController` — adaptive throttling driven by carbon intensity.
- :class:`SustainabilityPareto` — energy vs. accuracy Pareto frontier.
- :class:`TemporalShifter` — defer work when the grid is forecast to be dirty.

Protocols (interfaces to implement)
-----------------------------------
- :class:`PolicyEngine` — required by :class:`EcoModeController`.
- :class:`ForecastProvider` — required by :class:`TemporalShifter`.
  Satisfied by the enhanced :class:`CarbonForecast`.

Pipeline composition
--------------------
The five components form a coherent pipeline::

    provider.update_manual(v)                  # raw reading
    forecast.update(v)                          # predictor sees the same reading
    forecast.predict_next_optional()            # -> Optional[float]
    shifter.should_delay()                      # uses forecast's prediction
    eco_mode.update(prediction)                 # parallel throttling decision
    pareto.add_point(energy, accuracy, label)   # record the outcome

The façade provides :func:`build_carbon_pipeline` to construct the first
four components pre-wired, and a :class:`CarbonPipeline` data holder with
two small convenience methods (``feed`` / ``decide``) that encode the
non-obvious "update the provider and the forecast with the same reading"
pattern. All five components share the same lifecycle: keyword-only
``config=`` and ``strict=`` arguments, ``from_dict(..., strict=True)``,
``to_dict(include_*=False)``, ``statistics()``, ``reset()``, ``__eq__``,
``__hash__``.

Enhancements
------------
- ``_MissingComponent`` stub — missing sub-modules raise a clear
  ``ImportError`` on first use instead of returning ``None``.
- **All enhanced sub-module exports re-exported** — including the two
  Protocols (``PolicyEngine``, ``ForecastProvider``), the enum
  ``GridZone``, and the six statistics TypedDicts.
- **``SustainabilityComponent`` Protocol** — the validation block
  now checks structural conformance, not just ``callable``.
- Read-only public views: ``SUSTAINABILITY_REGISTRY`` and
  ``SUSTAINABILITY_AVAILABILITY`` are ``MappingProxyType``; internal dicts
  are private.
- ``SustainabilityRegistryError(ValueError)`` — narrow error type for
  registry operations.
- **Pipeline composition** — :class:`CarbonPipeline` dataclass and
  :func:`build_carbon_pipeline` factory that wires the four stateful
  components together.
- ``create_sustainability_component(name, *, config=None, strict=True,
  **extra_kwargs)`` — validated factory matching ``create_runtime`` in
  the runtime façade.
- ``_guarded_import`` helper — five copy-pasted try/except blocks
  collapsed to five one-line calls.
- ``_registry_lock`` guards registration and lookup.
- ``health()`` — version + availability + registered components +
  registered Protocols + registered pipelines.
- ``__dir__`` for clean REPL discoverability.
- Optional import-time validation via
  ``GREEN_AGENT_VALIDATE_SUSTAINABILITY=1`` (now checks
  ``SustainabilityComponent`` conformance instead of just ``callable``).

Design decisions documented here (instead of in code comments)
--------------------------------------------------------------
- **Configs are not registered.** ``CarbonForecastConfig`` and friends
  are frozen dataclasses, not components. Use the components' ``config``
  arguments directly.
- **Polymorphic ``from_dict`` is intentionally omitted.** The five
  configs share field names (``max_history``, ``clean_threshold``,
  ``default_threshold``), so a "detect kind from config keys" heuristic
  would be fragile. Use each component's own ``from_dict`` instead.

Compatibility
-------------
- ``from sustainability import CarbonForecast`` and friends unchanged.
- ``SUSTAINABILITY_REGISTRY`` / ``SUSTAINABILITY_AVAILABILITY`` are now
  read-only. Use ``register_sustainability_component`` to add entries.
- ``register_sustainability_component`` / ``get_sustainability_component``
  now raise ``SustainabilityRegistryError`` (subclass of ``ValueError``);
  existing ``except ValueError:`` handlers continue to work. ``KeyError``
  is no longer raised from ``get_sustainability_component``.
- ``from sustainability import *`` no longer injects ``None`` for missing
  names — it injects a stub that raises ``ImportError`` on use.

Requires Python 3.8+. ``runtime_checkable`` Protocols work on 3.8+;
``slots=True`` on dataclasses requires 3.10+.
"""

from __future__ import annotations

import importlib
import inspect
import logging
import os
import threading
from dataclasses import dataclass
from types import MappingProxyType
from typing import (
    Any, Callable, Dict, List, Mapping, Optional, Protocol, Tuple, Type,
    Union, runtime_checkable,
)

logger = logging.getLogger(__name__)

__version__ = "2.4.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SustainabilityRegistryError(ValueError):
    """Raised for invalid sustainability-registry operations."""


# --------------------------------------------------------------------------- #
# Structural interfaces
# --------------------------------------------------------------------------- #
@runtime_checkable
class SustainabilityComponent(Protocol):
    """
    Structural interface shared by every sustainability component.

    Both ``CarbonForecast``, ``CarbonIntensityProvider``,
    ``EcoModeController``, ``SustainabilityPareto``, and
    ``TemporalShifter`` conform — each exposes ``to_dict`` and
    ``statistics``. Used by the registry to reject non-conforming classes
    at registration time. Signatures are not inspected.
    """

    def to_dict(self, **kwargs: Any) -> Mapping[str, Any]: ...
    def statistics(self) -> Mapping[str, Any]: ...


# --------------------------------------------------------------------------- #
# Missing-component stub
# --------------------------------------------------------------------------- #
class _MissingComponent:
    """
    Placeholder for an optional component whose sub-module failed to
    import.

    Raises :class:`ImportError` on first use instead of returning ``None``.
    Handles call, attribute access, subscript, iteration, and ``in``
    tests — the operations most likely to be attempted on a component
    that turned out to be unavailable.
    """

    __slots__ = ("_name", "_reason")

    def __init__(self, name: str, reason: BaseException) -> None:
        self._name = name
        self._reason = reason

    def _fail(self) -> None:
        raise ImportError(
            f"Sustainability component '{self._name}' is unavailable: "
            f"{self._reason}"
        ) from self._reason

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self._fail()

    def __getattr__(self, item: str) -> Any:
        self._fail()

    def __getitem__(self, key: Any) -> Any:
        self._fail()

    def __iter__(self):
        self._fail()

    def __contains__(self, item: Any) -> bool:
        self._fail()

    def __bool__(self) -> bool:
        self._fail()

    def __repr__(self) -> str:
        return f"<unavailable sustainability component {self._name!r}>"


# --------------------------------------------------------------------------- #
# Read-only public views
# --------------------------------------------------------------------------- #
_availability: Dict[str, bool] = {}
SUSTAINABILITY_AVAILABILITY: Mapping[str, bool] = MappingProxyType(
    _availability
)

_registry: Dict[str, Any] = {}
SUSTAINABILITY_REGISTRY: Mapping[str, Any] = MappingProxyType(_registry)
_registry_lock = threading.RLock()


# --------------------------------------------------------------------------- #
# Guarded loader
# --------------------------------------------------------------------------- #
def _load(module: str, names: Tuple[str, ...]) -> Dict[str, Any]:
    """
    Import ``names`` from ``.{module}``; return ``_MissingComponent``
    stubs on ``ImportError``.
    """
    try:
        mod = importlib.import_module(f".{module}", package=__package__)
        _availability[module] = True
        return {
            n: getattr(mod, n, _MissingComponent(n, AttributeError(n)))
            for n in names
        }
    except ImportError as exc:
        logger.info("%s unavailable: %s", module, exc)
        _availability[module] = False
        return {n: _MissingComponent(n, exc) for n in names}


# --------------------------------------------------------------------------- #
# Load all five sub-modules
# --------------------------------------------------------------------------- #
globals().update(_load(
    "carbon_forecast",
    (
        "CarbonForecast", "CarbonForecastConfig", "CarbonForecastError",
        "ForecastMethod", "CarbonForecastStatistics",
    ),
))

globals().update(_load(
    "carbon_intensity_provider",
    (
        "CarbonIntensityProvider", "CarbonIntensityProviderConfig",
        "CarbonIntensityProviderError", "GridZone",
        "CarbonIntensitySample", "CarbonIntensityStatistics",
    ),
))

globals().update(_load(
    "eco_mode_controller",
    (
        "EcoModeController", "EcoModeControllerConfig",
        "EcoModeControllerError", "EcoModeTransition",
        "EcoModeStatistics", "PolicyEngine",
    ),
))

globals().update(_load(
    "pareto_frontier",
    (
        "AgentPerformancePoint", "SustainabilityPareto",
        "SustainabilityParetoConfig", "SustainabilityParetoError",
        "SustainabilityParetoStatistics",
    ),
))

globals().update(_load(
    "temporal_shifter",
    (
        "TemporalShifter", "TemporalShifterConfig",
        "TemporalShifterError", "ShiftDecision",
        "ForecastProvider", "TemporalShifterStatistics",
    ),
))


# --------------------------------------------------------------------------- #
# Carbon pipeline
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CarbonPipeline:
    """
    Wiring of the four stateful sustainability components.

    Construct via :func:`build_carbon_pipeline`; do not instantiate
    directly (the factory validates that every required sub-module is
    available and wires the forecast into the shifter).

    Usage
    -----
    ::

        pipeline = build_carbon_pipeline()
        pipeline.feed(450.0)          # provider + forecast see the reading
        should_delay = pipeline.decide()   # shifter uses forecast's prediction
        if pipeline.eco_mode is not None:
            pipeline.eco_mode.update(450.0)
    """

    provider: Any
    forecast: Any
    shifter: Any
    eco_mode: Optional[Any] = None

    def feed(
        self, intensity: float, *, update_provider: bool = True
    ) -> None:
        """
        Push a new reading into the forecast (and, by default, the
        provider's audit trail).

        Both components see the same value — this is the standard wiring:
        the provider stores the observation, the forecast predicts from
        the same history.
        """
        if update_provider:
            self.provider.update_manual(intensity)
        self.forecast.update(intensity)

    def decide(self, *, threshold: Optional[float] = None) -> bool:
        """
        Ask the shifter whether to defer. Uses the forecast's prediction
        (via ``predict_next_optional`` when available, ``predict_next``
        otherwise).
        """
        return self.shifter.should_delay(threshold=threshold)

    def __repr__(self) -> str:
        return (
            "CarbonPipeline("
            f"provider={type(self.provider).__name__}, "
            f"forecast={type(self.forecast).__name__}, "
            f"shifter={type(self.shifter).__name__}, "
            f"eco_mode={type(self.eco_mode).__name__ if self.eco_mode else None})"
        )


def build_carbon_pipeline(
    *,
    provider_config: Optional[Any] = None,
    forecast_config: Optional[Any] = None,
    shifter_config: Optional[Any] = None,
    eco_mode_config: Optional[Any] = None,
    policy_engine: Optional[Any] = None,
    strict: bool = True,
) -> CarbonPipeline:
    """
    Build a :class:`CarbonPipeline` with the four components wired together.

    Parameters
    ----------
    provider_config, forecast_config, shifter_config, eco_mode_config :
        Typed configs for each component. ``None`` uses the component's
        default config.
    policy_engine : PolicyEngine, optional
        Required for the ``eco_mode`` component. When supplied,
        ``EcoModeController`` is included in the pipeline; otherwise it is
        ``None``. Must satisfy :class:`PolicyEngine` if provided.
    strict : bool, default True
        Forwarded to each component's ``__init__``.

    Raises
    ------
    SustainabilityRegistryError
        If a required sub-module is unavailable, or if ``eco_mode_config``
        is provided without ``policy_engine``.
    """
    for _name in (
        "carbon_forecast",
        "carbon_intensity_provider",
        "temporal_shifter",
    ):
        if not _availability.get(_name, False):
            raise SustainabilityRegistryError(
                f"build_carbon_pipeline requires '{_name}' to be "
                f"available."
            )

    eco_mode: Optional[Any] = None
    if eco_mode_config is not None or policy_engine is not None:
        if not _availability.get("eco_mode_controller", False):
            raise SustainabilityRegistryError(
                "build_carbon_pipeline: eco_mode requested but "
                "'eco_mode_controller' is unavailable."
            )
        if policy_engine is None:
            raise SustainabilityRegistryError(
                "build_carbon_pipeline: eco_mode_config requires a "
                "policy_engine."
            )
        if not isinstance(policy_engine, PolicyEngine):
            raise SustainabilityRegistryError(
                f"build_carbon_pipeline: policy_engine must satisfy "
                f"PolicyEngine, got {type(policy_engine).__name__}."
            )

    provider = CarbonIntensityProvider(
        config=provider_config, strict=strict,
    )
    forecast = CarbonForecast(config=forecast_config, strict=strict)
    shifter = TemporalShifter(
        config=shifter_config, forecast=forecast, strict=strict,
    )
    if policy_engine is not None:
        eco_mode = EcoModeController(
            policy_engine=policy_engine,
            config=eco_mode_config,
            strict=strict,
        )

    return CarbonPipeline(
        provider=provider,
        forecast=forecast,
        shifter=shifter,
        eco_mode=eco_mode,
    )


# --------------------------------------------------------------------------- #
# Registry API
# --------------------------------------------------------------------------- #
def _register_if_available(name: str, cls: Any) -> None:
    if isinstance(cls, _MissingComponent):
        logger.debug(
            "Sustainability component '%s' unavailable; skipping.",
            name,
        )
        return
    if not (isinstance(cls, type) and issubclass(
        cls, SustainabilityComponent
    )):
        logger.warning(
            "Default sustainability component '%s' (%r) does not satisfy "
            "SustainabilityComponent; not registering.",
            name, cls,
        )
        return
    with _registry_lock:
        _registry[name] = cls


_register_if_available("forecast", CarbonForecast)
_register_if_available("provider", CarbonIntensityProvider)
_register_if_available("eco_mode", EcoModeController)
_register_if_available("pareto", SustainabilityPareto)
_register_if_available("shifter", TemporalShifter)


def register_sustainability_component(
    name: str,
    cls: Type[Any],
    *,
    strict: bool = True,
) -> None:
    """
    Register a sustainability component class under ``name``.

    Parameters
    ----------
    name : str
        Registry key. Must be a non-empty string.
    cls : type
        Class to register.
    strict : bool, default True
        If True, raise :class:`SustainabilityRegistryError` when ``cls``
        does not satisfy :class:`SustainabilityComponent`.

    Raises
    ------
    SustainabilityRegistryError
        If ``name`` is empty, already registered to a different class, or
        (with ``strict=True``) ``cls`` does not satisfy
        :class:`SustainabilityComponent`.
    TypeError
        If ``cls`` is not a class.
    """
    if not isinstance(name, str) or not name:
        raise SustainabilityRegistryError(
            "Component name must be a non-empty string."
        )
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    if strict and not issubclass(cls, SustainabilityComponent):
        raise SustainabilityRegistryError(
            f"{cls.__name__} does not satisfy SustainabilityComponent "
            f"(missing to_dict / statistics)."
        )
    with _registry_lock:
        existing = _registry.get(name)
        if isinstance(existing, _MissingComponent):
            existing = None
        if existing is not None and existing is not cls:
            raise SustainabilityRegistryError(
                f"Sustainability component '{name}' already registered "
                f"to {getattr(existing, '__name__', existing)!r}."
            )
        _registry[name] = cls
        logger.debug(
            "Registered sustainability component '%s' -> %s",
            name, cls.__name__,
        )


def get_sustainability_component(name: str) -> Type[Any]:
    """
    Return the sustainability component class registered under ``name``.

    Raises
    ------
    SustainabilityRegistryError
        If ``name`` is not registered, or its sub-module failed to load.
    """
    with _registry_lock:
        try:
            cls = _registry[name]
        except KeyError as exc:
            raise SustainabilityRegistryError(
                f"Unknown sustainability component '{name}'. "
                f"Available: {sorted(_registry)}"
            ) from exc
    if isinstance(cls, _MissingComponent):
        raise SustainabilityRegistryError(
            f"Sustainability component '{name}' is registered but "
            f"unavailable."
        )
    return cls


def list_sustainability_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    with _registry_lock:
        return {
            n: getattr(c, "__name__", type(c).__name__)
            for n, c in _registry.items()
        }


# --------------------------------------------------------------------------- #
# Convenience factory
# --------------------------------------------------------------------------- #
def create_sustainability_component(
    name: str,
    *,
    config: Optional[Any] = None,
    strict: bool = True,
    **extra_kwargs: Any,
) -> Any:
    """
    Construct a sustainability component by registered name.

    Parameters
    ----------
    name : str
        Registry key (``"forecast"``, ``"provider"``, ``"eco_mode"``,
        ``"pareto"``, ``"shifter"``, or a custom key registered via
        :func:`register_sustainability_component`).
    config : component-specific config, optional
        Typed config. ``None`` uses the component's default config.
    strict : bool, default True
        Forwarded to the component's ``__init__``.
    **extra_kwargs : Any
        Forwarded to the component's ``__init__`` — use for
        component-specific required arguments such as
        ``EcoModeController(policy_engine=...)``.

    Raises
    ------
    SustainabilityRegistryError
        If ``name`` is not registered, its sub-module failed to load, or
        the resulting call raises ``TypeError``.
    """
    cls = get_sustainability_component(name)
    kwargs: Dict[str, Any] = {"strict": strict, **extra_kwargs}
    if config is not None:
        kwargs["config"] = config
    try:
        return cls(**kwargs)
    except TypeError as exc:
        raise SustainabilityRegistryError(
            f"cannot instantiate '{name}' with the supplied arguments: "
            f"{exc}"
        ) from exc


# --------------------------------------------------------------------------- #
# Health / diagnostics
# --------------------------------------------------------------------------- #
def health() -> Dict[str, Any]:
    """
    Return an aggregate summary suitable for operator dashboards and bug
    reports.
    """
    with _registry_lock:
        registered = sorted(_registry)
    return {
        "version": __version__,
        "submodules": dict(_availability),
        "registered_components": registered,
        "registered_protocols": [
            "SustainabilityComponent",
            "PolicyEngine",
            "ForecastProvider",
        ],
        "pipeline_factory": "build_carbon_pipeline",
    }


# --------------------------------------------------------------------------- #
# Optional import-time validation
# --------------------------------------------------------------------------- #
if os.environ.get("GREEN_AGENT_VALIDATE_SUSTAINABILITY") == "1":
    for _module, _ok in _availability.items():
        _level = logging.INFO if _ok else logging.WARNING
        logger.log(
            _level, "Sustainability submodule %-26s : %s",
            _module, "available" if _ok else "MISSING",
        )

    for _name, _cls in list(_registry.items()):
        if isinstance(_cls, _MissingComponent):
            logger.warning(
                "Sustainability component '%s' is unavailable: %s",
                _name, _cls._reason,
            )
            continue
        if not issubclass(_cls, SustainabilityComponent):
            logger.warning(
                "Sustainability component '%s' (%s) does not satisfy "
                "SustainabilityComponent.",
                _name, _cls.__name__,
            )
            continue
        if not (_cls.__doc__ or "").strip():
            logger.warning(
                "Sustainability component '%s' (%s) has no docstring.",
                _name, _cls.__name__,
            )
            continue
        try:
            sig = inspect.signature(_cls.__init__)
            logger.debug(
                "Sustainability component '%s' (%s) OK; __init__%s",
                _name, _cls.__name__, sig,
            )
        except (TypeError, ValueError) as exc:  # pragma: no cover — CI only
            logger.exception(
                "Sustainability component '%s' (%s) signature invalid: %s",
                _name, _cls.__name__, exc,
            )
            raise


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = (
    "__version__",
    # Errors / Protocols / stub
    "SustainabilityRegistryError",
    "SustainabilityComponent",
    "PolicyEngine",
    "ForecastProvider",
    # carbon_forecast
    "CarbonForecast",
    "CarbonForecastConfig",
    "CarbonForecastError",
    "CarbonForecastStatistics",
    "ForecastMethod",
    # carbon_intensity_provider
    "CarbonIntensityProvider",
    "CarbonIntensityProviderConfig",
    "CarbonIntensityProviderError",
    "CarbonIntensitySample",
    "CarbonIntensityStatistics",
    "GridZone",
    # eco_mode_controller
    "EcoModeController",
    "EcoModeControllerConfig",
    "EcoModeControllerError",
    "EcoModeStatistics",
    "EcoModeTransition",
    # pareto_frontier
    "AgentPerformancePoint",
    "SustainabilityPareto",
    "SustainabilityParetoConfig",
    "SustainabilityParetoError",
    "SustainabilityParetoStatistics",
    # temporal_shifter
    "ShiftDecision",
    "TemporalShifter",
    "TemporalShifterConfig",
    "TemporalShifterError",
    "TemporalShifterStatistics",
    # pipeline / factory
    "CarbonPipeline",
    "build_carbon_pipeline",
    "create_sustainability_component",
    # registry / manifest
    "SUSTAINABILITY_REGISTRY",
    "SUSTAINABILITY_AVAILABILITY",
    "get_sustainability_component",
    "list_sustainability_components",
    "register_sustainability_component",
    # health
    "health",
)


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m sustainability
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    import json
    import sys

    passed = 0
    failed = 0

    def _check(name: str, cond: bool) -> None:
        global passed, failed
        if cond:
            passed += 1
            print(f"  ✅ {name}")
        else:
            failed += 1
            print(f"  ❌ {name}")

    # ---- 1. Missing-component stub behaviour --------------------- #
    print("missing-component stub:")
    stub = _MissingComponent("x", ImportError("simulated"))
    for probe_name, probe in (
        ("call", lambda: stub()),
        ("attr", lambda: stub.some_attr),
        ("getitem", lambda: stub["k"]),
        ("iter", lambda: list(stub)),
        ("bool", lambda: bool(stub)),
    ):
        try:
            probe()
            _check(f"stub {probe_name}", False)
        except ImportError as exc:
            _check(
                f"stub {probe_name} raises ImportError",
                "unavailable" in str(exc),
            )

    # ---- 2. Registry & manifest are frozen ----------------------- #
    print("frozen public views:")
    try:
        SUSTAINABILITY_REGISTRY["hack"] = object  # type: ignore[index]
        _check("registry frozen", False)
    except TypeError:
        _check("registry frozen", True)
    try:
        SUSTAINABILITY_AVAILABILITY["hack"] = True  # type: ignore[index]
        _check("availability frozen", False)
    except TypeError:
        _check("availability frozen", True)

    # ---- 3. All enhanced exports present ------------------------- #
    print("enhanced re-exports:")
    for _name in (
        "PolicyEngine", "ForecastProvider",
        "GridZone",
        "CarbonForecastStatistics", "CarbonIntensitySample",
        "CarbonIntensityStatistics", "EcoModeStatistics",
        "SustainabilityParetoStatistics", "TemporalShifterStatistics",
    ):
        _check(
            f"{_name} exported",
            _name in __all__
            and getattr(sys.modules[__name__], _name, None) is not None,
        )

    # ---- 4. Protocol isinstance checks -------------------------- #
    print("Protocols work:")

    class _DummyEngine:
        def set_pruning_aggressiveness(self, v):
            pass

        def set_token_limit_ratio(self, v):
            pass

        def reset_defaults(self):
            pass

    _check(
        "dummy engine satisfies PolicyEngine",
        isinstance(_DummyEngine(), PolicyEngine),
    )

    _check(
        "CarbonForecast satisfies ForecastProvider",
        isinstance(CarbonForecast(), ForecastProvider),
    )

    # ---- 5. All five default components register ---------------- #
    print("default registration:")
    _check(
        "5 components registered",
        len(list_sustainability_components()) >= 5,
    )
    for _name in ("forecast", "provider", "eco_mode", "pareto", "shifter"):
        _check(f"'{_name}' registered", _name in SUSTAINABILITY_REGISTRY)

    # ---- 6. create_sustainability_component --------------------- #
    print("create_sustainability_component:")
    fc = create_sustainability_component("forecast")
    _check(
        "forecast constructed",
        isinstance(fc, CarbonForecast),
    )
    sh = create_sustainability_component("shifter", forecast=fc)
    _check(
        "shifter constructed with forecast collaborator",
        isinstance(sh, TemporalShifter),
    )
    engine = _DummyEngine()
    eco = create_sustainability_component(
        "eco_mode", policy_engine=engine,
    )
    _check(
        "eco_mode constructed with policy_engine",
        isinstance(eco, EcoModeController),
    )

    try:
        create_sustainability_component("nonexistent")
        _check("unknown name raises", False)
    except SustainabilityRegistryError:
        _check("unknown name raises", True)

    # ---- 7. build_carbon_pipeline ------------------------------- #
    print("build_carbon_pipeline:")
    pipeline = build_carbon_pipeline()
    _check(
        "pipeline is CarbonPipeline",
        isinstance(pipeline, CarbonPipeline),
    )
    _check("provider present", pipeline.provider is not None)
    _check("forecast present", pipeline.forecast is not None)
    _check("shifter present", pipeline.shifter is not None)
    _check("eco_mode absent by default", pipeline.eco_mode is None)

    # End-to-end: feed → predict → decide
    pipeline.feed(500.0)
    _check(
        "provider stored reading",
        pipeline.provider.get_current_intensity() == 500.0,
    )
    _check(
        "forecast stored reading",
        pipeline.forecast.sample_count == 1,
    )
    _check(
        "predict matches reading",
        pipeline.forecast.predict_next() == 500.0,
    )
    _check(
        "decide → True (500 > default_threshold 400)",
        pipeline.decide() is True,
    )

    # With eco_mode
    pipeline_eco = build_carbon_pipeline(policy_engine=_DummyEngine())
    _check(
        "eco_mode present when policy_engine supplied",
        pipeline_eco.eco_mode is not None,
    )

    # eco_mode_config without policy_engine → error
    try:
        build_carbon_pipeline(eco_mode_config=EcoModeControllerConfig())
        _check("eco_mode_config alone raises", False)
    except SustainabilityRegistryError:
        _check("eco_mode_config alone raises", True)

    # ---- 8. Pipeline round-trip via components ------------------ #
    print("pipeline component round-trip:")
    fc2 = CarbonForecast.from_dict(pipeline.forecast.to_dict())
    _check(
        "forecast to/from dict stable",
        fc2 == pipeline.forecast,
    )
    payload = json.dumps(pipeline.forecast.to_dict())
    fc3 = CarbonForecast.from_json(payload)
    _check(
        "forecast json round-trip",
        fc3.to_dict() == pipeline.forecast.to_dict(),
    )

    # ---- 9. health --------------------------------------------- #
    print("health:")
    h = health()
    _check("version", h["version"] == __version__)
    _check(
        "submodules present",
        isinstance(h["submodules"], dict),
    )
    _check(
        "registered components listed",
        isinstance(h["registered_components"], list)
        and len(h["registered_components"]) >= 5,
    )
    _check(
        "registered protocols listed",
        isinstance(h["registered_protocols"], list)
        and "PolicyEngine" in h["registered_protocols"]
        and "ForecastProvider" in h["registered_protocols"],
    )

    # ---- 10. Registry rejects non-conforming class -------------- #
    print("registry rejects non-conforming class:")
    class _NoToDict:
        def __init__(self):
            pass

    try:
        register_sustainability_component(
            "bad", _NoToDict, strict=True,
        )
        _check("non-conforming rejected", False)
    except SustainabilityRegistryError:
        _check("non-conforming rejected", True)

    # ...but accepted with strict=False
    register_sustainability_component("bad", _NoToDict, strict=False)
    _check(
        "non-conforming accepted with strict=False",
        "bad" in SUSTAINABILITY_REGISTRY,
    )

    # ---- 11. __dir__ ------------------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))

    # ---- 12. all five sub-modules available (in this env) ------ #
    print("availability:")
    for _sub in (
        "carbon_forecast", "carbon_intensity_provider",
        "eco_mode_controller", "pareto_frontier", "temporal_shifter",
    ):
        _check(
            f"{_sub} available",
            SUSTAINABILITY_AVAILABILITY.get(_sub) is True,
        )

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
