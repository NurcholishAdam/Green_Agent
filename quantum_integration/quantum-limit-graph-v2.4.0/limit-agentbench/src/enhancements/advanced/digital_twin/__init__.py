# src/quantum_integration/digital_twin/__init__.py

"""
System-Wide Digital Twin package for Green Agent
================================================

Modular rewrite of the v2.6.0 monolithic digital-twin script. Each concern
lives in its own module, mirroring the conventions of the ``memory`` package:
- ``SCHEMA_VERSION`` stamped on every persisted payload.
- Structured error hierarchy.
- Frozen config and record dataclasses, hashable, serializable.
- Lazy ``asyncio.Lock`` to avoid the loop-binding bug.
- Deterministic RNG plumbed through every stochastic component.
- ``TwinSubsystems`` container with reverse-order ``close(flush=True)``.
- ``build_default_twin()`` factory.
- ``assert_compatible_schema_versions`` / ``assert_compatible_vocabularies``.
- ``AttributeError``-safe lazy loading with a ``require()`` helper.
"""

from __future__ import annotations

import importlib
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

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


# --------------------------------------------------------------------------- #
# Optional submodule specs. Each entry: (module_name, required, public_names)
# --------------------------------------------------------------------------- #
_MODULE_SPECS: Tuple[Tuple[str, bool, Tuple[str, ...]], ...] = (
    (
        "digital_twin_resilience",
        True,
        ("CircuitBreaker", "CircuitBreakerState", "retry_async"),
    ),
    (
        "digital_twin_telemetry",
        False,
        (
            "DigitalTwinTelemetry",
            "PROMETHEUS_AVAILABLE",
        ),
    ),
    (
        "digital_twin_persistence",
        False,
        ("DigitalTwinPersistenceManager",),
    ),
    (
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
    (
        "digital_twin_limit_graph",
        False,
        ("LimitGraphManager",),
    ),
    (
        "digital_twin_modp",
        False,
        ("MODPOptimizer", "ParetoFront", "ParetoPoint"),
    ),
    (
        "digital_twin_rlhf",
        False,
        ("RLHFTrainer", "RewardModel", "PreferencePair"),
    ),
    (
        "digital_twin_pso",
        False,
        ("ParticleSwarmOptimizer", "PSOResult"),
    ),
    (
        "digital_twin_moe",
        False,
        ("MoEGatingNetwork", "Expert", "MoEResult"),
    ),
    (
        "system_digital_twin",
        True,
        ("SystemDigitalTwin",),
    ),
)


def _import_module(module_name: str) -> Optional[Any]:
    try:
        return importlib.import_module(f".{module_name}", package=__name__)
    except Exception as exc:  # noqa: BLE001 - defensive
        logger.debug("Submodule %s failed to import: %s", module_name, exc)
        return None


MEMORY_AVAILABILITY: Dict[str, bool] = {}
_LOADED_MODULES: Dict[str, Any] = {}
_NAME_TO_MODULE: Dict[str, str] = {}
_RESERVED = {"SCHEMA_VERSION", "__version__", "TwinSubsystems"}

for _mod_name, _required, _names in _MODULE_SPECS:
    _mod = _import_module(_mod_name)
    MEMORY_AVAILABILITY[_mod_name] = _mod is not None
    if _mod is None:
        for _n in _names:
            _NAME_TO_MODULE.setdefault(_n, _mod_name)
        continue
    _LOADED_MODULES[_mod_name] = _mod
    for _n in _names:
        _v = getattr(_mod, _n, None)
        if _v is None:
            _NAME_TO_MODULE.setdefault(_n, _mod_name)
            continue
        if _n in _RESERVED:
            _NAME_TO_MODULE.setdefault(_n, _mod_name)
            continue
        globals().setdefault(_n, _v)
        _NAME_TO_MODULE.setdefault(_n, _mod_name)

del _mod_name, _required, _mod, _names, _n, _v


# --------------------------------------------------------------------------- #
# TwinSubsystems
# --------------------------------------------------------------------------- #
@dataclass
class TwinSubsystems:
    """Aggregator for every optional subsystem.

    Mirrors ``memory.Pipeline``: reverse-order ``close(flush=True)``,
    context-manager protocols, ``statistics()``.
    """

    limit_graph: Any = None
    modp: Any = None
    rlhf: Any = None
    pso: Any = None
    moe: Any = None
    distillation: Any = None
    telemetry: Any = None
    persistence: Any = None

    _CLOSE_ORDER: Tuple[str, ...] = (
        "moe", "pso", "rlhf", "modp", "limit_graph",
        "distillation", "telemetry", "persistence",
    )

    def components(self) -> Dict[str, Any]:
        return {
            name: getattr(self, name)
            for name in (
                "limit_graph", "modp", "rlhf", "pso", "moe",
                "distillation", "telemetry", "persistence",
            )
            if getattr(self, name, None) is not None
        }

    def missing_components(self) -> List[str]:
        return [
            name for name in (
                "limit_graph", "modp", "rlhf", "pso", "moe",
                "distillation", "telemetry", "persistence",
            )
            if getattr(self, name, None) is None
        ]

    def close(self, *, flush: bool = True) -> None:
        for name in self._CLOSE_ORDER:
            comp = getattr(self, name, None)
            if comp is None:
                continue
            close = getattr(comp, "close", None)
            if not callable(close):
                continue
            try:
                import inspect
                sig = inspect.signature(close)
                if "flush" in sig.parameters:
                    close(flush=flush)
                else:
                    close()
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning(
                    "subsystem close failed for %s: %s", name, exc,
                )

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
) -> "SystemDigitalTwin":
    """Build a :class:`SystemDigitalTwin` with sensible defaults."""
    if SystemDigitalTwin is None:  # type: ignore[truthy-bool]
        raise ImportError(
            "cannot build default twin: system_digital_twin submodule "
            "failed to import."
        )
    return SystemDigitalTwin(  # type: ignore[name-defined]
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
    versions: Dict[str, int] = {}
    for name, mod in _LOADED_MODULES.items():
        v = getattr(mod, "SCHEMA_VERSION", None)
        if isinstance(v, int) and not isinstance(v, bool) and v > 0:
            versions[name] = v
    distinct = set(versions.values())
    if len(distinct) > 1:
        raise RuntimeError(f"incompatible schema versions: {versions}")
    return versions


def assert_compatible_vocabularies() -> Dict[str, Any]:
    """Verify shared vocabularies agree (resource names, scenarios)."""
    collected: Dict[str, Any] = {}
    schemas = _LOADED_MODULES.get("digital_twin_schemas")
    if schemas is not None:
        collected["scenarios"] = tuple(
            s.value for s in getattr(schemas, "SimulationScenario")
        )
    return collected


# --------------------------------------------------------------------------- #
# Lazy loading
# --------------------------------------------------------------------------- #
def __getattr__(name: str) -> Any:
    if name in _NAME_TO_MODULE:
        source = _NAME_TO_MODULE[name]
        raise AttributeError(
            f"digital_twin.{name} is unavailable: the {source!r} "
            f"submodule failed to import. Check MEMORY_AVAILABILITY."
        )
    raise AttributeError(
        f"module {__name__!r} has no attribute {name!r}"
    )


def require(name: str) -> Any:
    import sys
    if not isinstance(name, str) or not name:
        raise ImportError("require() expects a non-empty name.")
    try:
        return getattr(sys.modules[__name__], name)
    except AttributeError as exc:
        raise ImportError(str(exc)) from exc


def __dir__() -> List[str]:
    base = set(globals().keys())
    base.update(__all__)
    return sorted(base)


__all__ = [
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
]
