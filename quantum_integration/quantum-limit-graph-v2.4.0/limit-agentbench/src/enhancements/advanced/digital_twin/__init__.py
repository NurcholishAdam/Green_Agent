# src/quantum_integration/digital_twin/__init__.py

"""
System-Wide Digital Twin package for Green Agent
================================================

Modular rewrite of the v2.6.0 monolithic digital-twin script. Each concern
lives in its own module, mirroring the conventions of the ``memory`` package:

- ``SCHEMA_VERSION`` stamped on every persisted payload.
- Structured error hierarchy.
- Frozen config and record dataclasses, hashable, serializable.
- Loop-safe async primitives (see ``digital_twin_helpers._LazyLock``).
- Deterministic RNG plumbed through every stochastic component.
- ``TwinSubsystems`` container with reverse-order ``close(flush=True)``.
- ``build_default_twin()`` factory.
- ``assert_compatible_schema_versions`` / ``assert_compatible_vocabularies``.
- Friendly ``AttributeError`` / ``ImportError`` on access to names from
  submodules that failed to import.

Loading model
-------------
Submodules listed in :data:`_MODULE_SPECS` are imported eagerly at
package-import time. Names from successfully imported submodules are
copied into this module's globals. Names from failed submodules are
tracked in :data:`MEMORY_AVAILABILITY` (module level) and
:data:`_NAME_TO_MODULE` (name level), so ``__getattr__`` can raise a
helpful error on access.

Required submodules (:data:`ModuleSpec.required`) must import; if any
of them fail, the package import itself fails with a full report.

Compatibility checks
--------------------
``assert_compatible_schema_versions`` validates each loaded module
against a package-level ceiling; it does **not** require all modules to
share a single ``SCHEMA_VERSION``, because each module stamps its own
persisted payload and they legitimately evolve at different rates.
"""

from __future__ import annotations

import dataclasses
import importlib
import inspect
import logging
import sys
from dataclasses import dataclass
from types import MappingProxyType
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Tuple,
)

from .digital_twin_config import (
    DigitalTwinConfig,
    SCHEMA_VERSION,
)
from .digital_twin_errors import (
    DigitalTwinCircuitOpenError,
    DigitalTwinConfigError,
    DigitalTwinError,
    DigitalTwinInputError,
    DigitalTwinPersistenceError,
    DigitalTwinSimulationError,
)
from .digital_twin_helpers import (
    _LazyLock,
    _deep_freeze,
    _hashable,
    _is_finite_nonneg,
    _is_real_int,
    _iso_now,
    _parse_iso_datetime,
    _percentile,
    _to_plain,
)
from .digital_twin_schemas import (
    DigitalTwinResult,
    ResourceProjection,
    SimulationScenario,
)
from .digital_twin_validation import (
    ScenarioParameterValidator,
    ValidationResult,
)

logger = logging.getLogger(__name__)

__version__ = "7.0.0"

#: Highest ``SCHEMA_VERSION`` this package is known to be compatible with.
#: Bump when a new module adopts a newer schema version **and** every
#: consumer of that module has been updated.
_MAX_KNOWN_SCHEMA_VERSION: int = 2


# --------------------------------------------------------------------------- #
# Module specifications
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class ModuleSpec:
    """Specification for a lazily loadable submodule.

    Parameters
    ----------
    name:
        Dotted name relative to this package (e.g. ``"digital_twin_moe"``).
    required:
        When ``True``, a failed import aborts the package import.
    exports:
        Names the submodule is expected to expose. Missing names are
        tracked for a helpful error message.
    """

    name: str
    required: bool
    exports: Tuple[str, ...] = ()


_MODULE_SPECS: Tuple[ModuleSpec, ...] = (
    ModuleSpec(
        "digital_twin_resilience",
        True,
        ("CircuitBreaker", "CircuitBreakerState", "retry_async"),
    ),
    ModuleSpec(
        "digital_twin_telemetry",
        False,
        ("DigitalTwinTelemetry", "PROMETHEUS_AVAILABLE"),
    ),
    ModuleSpec(
        "digital_twin_persistence",
        False,
        ("DigitalTwinPersistenceManager",),
    ),
    ModuleSpec(
        "digital_twin_distillation",
        True,
        (
            "TwinOptimizationState",
            "Teacher",
            "TwinRuleBasedTeacher",
            "TwinHistoricalMLTeacher",
            "TwinStatefulQTeacher",
            "DistillationStudent",
            "ReplayBuffer",
            "DistillationTwinOptimizer",
            "ACTION_SPACE",
        ),
    ),
    ModuleSpec(
        "digital_twin_limit_graph",
        False,
        ("LimitGraphManager",),
    ),
    ModuleSpec(
        "digital_twin_modp",
        False,
        ("MODPOptimizer", "ParetoFront", "ParetoPoint"),
    ),
    ModuleSpec(
        "digital_twin_rlhf",
        False,
        ("RLHFTrainer", "RewardModel", "PreferencePair"),
    ),
    ModuleSpec(
        "digital_twin_pso",
        False,
        ("ParticleSwarmOptimizer", "PSOResult"),
    ),
    ModuleSpec(
        "digital_twin_moe",
        False,
        ("MoEGatingNetwork", "Expert", "MoEResult"),
    ),
    ModuleSpec(
        "system_digital_twin",
        True,
        ("SystemDigitalTwin",),
    ),
)


# --------------------------------------------------------------------------- #
# Module loading
# --------------------------------------------------------------------------- #

def _import_module(
    module_name: str, *, required: bool = False,
) -> Optional[Any]:
    """Import ``module_name`` relative to this package.

    Returns ``None`` on failure. Logs at ``warning`` when the module is
    required, ``info`` otherwise.
    """
    try:
        return importlib.import_module(f".{module_name}", package=__name__)
    except Exception as exc:  # noqa: BLE001 - defensive
        level = logging.WARNING if required else logging.INFO
        logger.log(
            level,
            "digital_twin: submodule %s failed to import (%s): %s",
            module_name, type(exc).__name__, exc,
        )
        return None


def _load_modules(
    specs: Tuple[ModuleSpec, ...],
) -> Tuple[
    Dict[str, Any],               # loaded modules
    Dict[str, Tuple[str, bool]],  # export name -> (module, available)
    Dict[str, bool],              # module name -> imported?
    Dict[str, Any],               # export name -> value
]:
    """Import each spec and collect bindings.

    Returns a four-tuple: loaded modules, per-name availability,
    per-module availability, and the bindings to copy into the package
    namespace.
    """
    loaded: Dict[str, Any] = {}
    name_to_module: Dict[str, Tuple[str, bool]] = {}
    availability: Dict[str, bool] = {}
    bindings: Dict[str, Any] = {}

    for spec in specs:
        module = _import_module(spec.name, required=spec.required)
        availability[spec.name] = module is not None
        if module is None:
            for export in spec.exports:
                name_to_module.setdefault(export, (spec.name, False))
            continue
        loaded[spec.name] = module
        for export in spec.exports:
            value = getattr(module, export, None)
            available = value is not None
            name_to_module.setdefault(export, (spec.name, available))
            if available:
                bindings.setdefault(export, value)
    return loaded, name_to_module, availability, bindings


# Track modules imported directly at the top of this file so that
# ``assert_compatible_schema_versions`` sees their versions.
_DIRECTLY_LOADED: Dict[str, Any] = {
    name: sys.modules[f"{__name__}.{name}"]
    for name in (
        "digital_twin_config",
        "digital_twin_schemas",
        "digital_twin_validation",
    )
    if f"{__name__}.{name}" in sys.modules
}

_LOADED_MODULES, _NAME_TO_MODULE, _MODULE_AVAILABILITY, _BINDINGS = (
    _load_modules(_MODULE_SPECS)
)
_LOADED_MODULES.update(_DIRECTLY_LOADED)
globals().update(_BINDINGS)

#: Mapping of submodule name to whether it was importable.
#: Read-only to prevent accidental mutation.
MEMORY_AVAILABILITY: MappingProxyType = MappingProxyType(
    dict(_MODULE_AVAILABILITY)
)

_MISSING_REQUIRED: Tuple[str, ...] = tuple(
    spec.name for spec in _MODULE_SPECS
    if spec.required and not _MODULE_AVAILABILITY.get(spec.name, False)
)
if _MISSING_REQUIRED:
    raise ImportError(
        "digital_twin package requires submodules that failed to import: "
        f"{list(_MISSING_REQUIRED)}. "
        "See MEMORY_AVAILABILITY and the preceding log entries."
    )

# Report optional-submodule status once, at import time.
_present = sorted(
    name for name, ok in _MODULE_AVAILABILITY.items() if ok
)
_missing = sorted(
    name for name, ok in _MODULE_AVAILABILITY.items() if not ok
)
if _missing:
    logger.info(
        "digital_twin: loaded=%s; optional missing=%s",
        _present, _missing,
    )


# --------------------------------------------------------------------------- #
# TwinSubsystems
# --------------------------------------------------------------------------- #

@dataclass
class TwinSubsystems:
    """Aggregator for every optional subsystem.

    Mirrors ``memory.Pipeline``: reverse-order ``close(flush=True)``,
    context-manager protocols, ``statistics()``.

    ``close()`` returns a mapping of component name to exception for any
    component whose ``close`` raised. Callers that ignore the return
    value see the same behavior as before.
    """

    limit_graph: Any = None
    modp: Any = None
    rlhf: Any = None
    pso: Any = None
    moe: Any = None
    distillation: Any = None
    telemetry: Any = None
    persistence: Any = None

    #: Close order. Reverse of construction order so that dependents are
    #: shut down before their dependencies. Class-level constant.
    _CLOSE_ORDER: ClassVar[Tuple[str, ...]] = (
        "moe", "pso", "rlhf", "modp", "limit_graph",
        "distillation", "telemetry", "persistence",
    )

    # -- introspection -------------------------------------------------- #

    def components(self) -> Dict[str, Any]:
        """Return ``{name: component}`` for non-``None`` fields."""
        return {
            f.name: getattr(self, f.name)
            for f in dataclasses.fields(self)
            if getattr(self, f.name) is not None
        }

    def missing_components(self) -> List[str]:
        """Return the field names whose components are ``None``."""
        return [
            f.name for f in dataclasses.fields(self)
            if getattr(self, f.name) is None
        ]

    def statistics(self) -> Dict[str, Any]:
        """Return a snapshot of the container's component layout."""
        components = self.components()
        return {
            "schema_version": SCHEMA_VERSION,
            "component_count": len(components),
            "components": sorted(components.keys()),
            "missing": self.missing_components(),
            "close_order": list(self._CLOSE_ORDER),
        }

    # -- lifecycle ------------------------------------------------------ #

    def close(self, *, flush: bool = True) -> Dict[str, Exception]:
        """Close every component in reverse order.

        Returns
        -------
        dict
            ``{component_name: exception}`` for any component whose
            ``close`` raised. Empty on full success.
        """
        failures: Dict[str, Exception] = {}
        for name in self._CLOSE_ORDER:
            comp = getattr(self, name, None)
            if comp is None:
                continue
            close = getattr(comp, "close", None)
            if not callable(close):
                continue
            try:
                sig = inspect.signature(close)
                if "flush" in sig.parameters:
                    close(flush=flush)
                else:
                    close()
            except Exception as exc:  # pragma: no cover - defensive
                failures[name] = exc
                logger.warning(
                    "subsystem close failed for %s: %s", name, exc,
                )
        return failures

    def __enter__(self) -> "TwinSubsystems":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        present = sorted(self.components().keys())
        return f"TwinSubsystems(components={present})"


# --------------------------------------------------------------------------- #
# Factory
# --------------------------------------------------------------------------- #

def build_default_twin(
    *,
    config: Optional[DigitalTwinConfig] = None,
    storage: Optional[Any] = None,
    message_queue: Optional[Any] = None,
    adaptive_cost: Optional[Any] = None,
    pareto_gating: Optional[Any] = None,
    drift_detector: Optional[Any] = None,
    metrics: Optional[Any] = None,
    **kwargs: Any,
) -> Any:
    """Build a :class:`SystemDigitalTwin` with sensible defaults.

    Raises
    ------
    ImportError
        If the ``system_digital_twin`` submodule failed to import. This
        is normally impossible because it is a required submodule; the
        guard exists so the failure mode is a clear ``ImportError``
        rather than a ``NameError``.
    DigitalTwinConfigError
        If ``config`` is not a :class:`DigitalTwinConfig` or ``None``.
    """
    if config is not None and not isinstance(config, DigitalTwinConfig):
        raise DigitalTwinConfigError(
            f"config must be a DigitalTwinConfig or None, got "
            f"{type(config).__name__}."
        )
    cls = globals().get("SystemDigitalTwin")
    if cls is None:
        raise ImportError(
            "cannot build default twin: system_digital_twin submodule "
            "failed to import. See MEMORY_AVAILABILITY."
        )
    return cls(
        config=config,
        storage=storage,
        message_queue=message_queue,
        adaptive_cost=adaptive_cost,
        pareto_gating=pareto_gating,
        drift_detector=drift_detector,
        metrics=metrics,
        **kwargs,
    )


# --------------------------------------------------------------------------- #
# Compatibility checks
# --------------------------------------------------------------------------- #

def assert_compatible_schema_versions() -> Dict[str, int]:
    """Validate that every loaded module's ``SCHEMA_VERSION`` is at or
    below the package's known ceiling.

    Modules are allowed to declare different versions; the only hard
    rule is that none may exceed :data:`_MAX_KNOWN_SCHEMA_VERSION`,
    which would mean the package cannot reason about the module's
    persisted payloads.

    Returns
    -------
    dict
        ``{module_name: declared_version}`` for every module that
        exposes a positive integer ``SCHEMA_VERSION``.

    Raises
    ------
    RuntimeError
        If any module declares a version newer than the ceiling.
    """
    versions: Dict[str, int] = {}
    for name, module in _LOADED_MODULES.items():
        v = getattr(module, "SCHEMA_VERSION", None)
        if not isinstance(v, int) or isinstance(v, bool) or v <= 0:
            continue
        if v > _MAX_KNOWN_SCHEMA_VERSION:
            raise RuntimeError(
                f"{name} declares schema_version {v}, newer than the "
                f"package's supported ceiling "
                f"{_MAX_KNOWN_SCHEMA_VERSION}. Upgrade the package "
                f"before mixing payloads from this module."
            )
        versions[name] = v
    return versions


def assert_compatible_vocabularies() -> Dict[str, Any]:
    """Collect shared vocabularies known to the package.

    Currently reports the canonical scenario vocabulary. Additional
    vocabularies can be appended as new shared enums are introduced.
    This function collects and returns; it does not raise for missing
    vocabularies, only for internal inconsistency.
    """
    scenarios = tuple(s.value for s in SimulationScenario)
    if not scenarios:
        raise RuntimeError(
            "SimulationScenario vocabulary is empty; schemas module "
            "may have failed to initialize correctly."
        )
    return {"scenarios": scenarios}


# --------------------------------------------------------------------------- #
# Lazy attribute access
# --------------------------------------------------------------------------- #

def __getattr__(name: str) -> Any:
    """Raise a helpful error for names that failed to load.

    Names that were never tracked are reported as ordinary missing
    attributes. Names tracked from a failed import (or a missing
    export) get a message pointing at the responsible module.
    """
    entry = _NAME_TO_MODULE.get(name)
    if entry is not None:
        module_name, available = entry
        if not available:
            raise AttributeError(
                f"digital_twin.{name} is unavailable: the "
                f"{module_name!r} submodule failed to import or did "
                f"not export this name. See MEMORY_AVAILABILITY."
            )
        # Defensive: the module loaded and exported the name, but it
        # was not bound into globals (should not happen).
        module = _LOADED_MODULES.get(module_name)
        if module is not None:
            value = getattr(module, name, None)
            if value is not None:
                return value
    raise AttributeError(
        f"module {__name__!r} has no attribute {name!r}"
    )


def require(name: str) -> Any:
    """Return a name that the caller knows must be present.

    Unlike ordinary attribute access, which raises ``AttributeError``,
    :func:`require` raises ``ImportError`` so callers can write a
    single handler for "something the package promised is missing."

    Raises
    ------
    ImportError
        If ``name`` is not a non-empty string, or if the name is not
        available (either never defined or its submodule failed to
        import).
    """
    if not isinstance(name, str) or not name:
        raise ImportError("require() expects a non-empty name.")
    # Trigger __getattr__ for tracked names.
    if name in globals():
        return globals()[name]
    module = sys.modules[__name__]
    try:
        return getattr(module, name)
    except AttributeError as exc:
        raise ImportError(str(exc)) from exc


def __dir__() -> List[str]:
    """Return names that are actually available at this moment."""
    return sorted(set(globals().keys()) | set(__all__))


# --------------------------------------------------------------------------- #
# Public surface
# --------------------------------------------------------------------------- #

__all__ = sorted(
    # Always-present names. These come from this module's direct imports
    # or the required submodules. ``SystemDigitalTwin`` is here because
    # its submodule is required; the package would have failed to
    # import otherwise.
    {
        "__version__",
        "SCHEMA_VERSION",
        "DigitalTwinConfig",
        "DigitalTwinResult",
        "DigitalTwinError",
        "DigitalTwinInputError",
        "DigitalTwinConfigError",
        "DigitalTwinPersistenceError",
        "DigitalTwinSimulationError",
        "DigitalTwinCircuitOpenError",
        "ResourceProjection",
        "ScenarioParameterValidator",
        "SimulationScenario",
        "TwinSubsystems",
        "ValidationResult",
        "MEMORY_AVAILABILITY",
        "assert_compatible_schema_versions",
        "assert_compatible_vocabularies",
        "build_default_twin",
        "require",
        "SystemDigitalTwin",
    }
    # Names actually bound from loaded submodules. Filtering on globals
    # keeps ``__all__`` honest if an optional submodule is missing.
    | {name for name in _NAME_TO_MODULE if name in globals()}
)
