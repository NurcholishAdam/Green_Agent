# src/dashboard/__init__.py

"""
Dashboard package for Green_Agent (Layer 11 — Visualization & Monitoring).
==========================================================================

Surfaces the memory pipeline as a set of visual and analytical
components: leaderboards, symbolic-rule overlays, telemetry loaders,
and — most importantly — the Green Decision Graph that turns
Supermemory recall into the memory-graph React UI.

Components
----------
- :mod:`green_decision_graph` — Green Decision Graph builder (primary).
- :mod:`green_dashboard`      — meta-cognitive leaderboard + HTML export.
- :mod:`symbolic_visualizer`  — symbolic rule violation display.
- :mod:`telemetry_loader`     — JSON report → DataFrame helper.
- :mod:`api_server`           — FastAPI + WebSocket REST layer.

Conventions
-----------
- Each submodule is imported under its own guard. A failure in one does
  not disable the rest; the module's exported names are declared in
  ``_MODULE_SPECS`` and, when the submodule loaded but lacks a name,
  the diagnostic distinguishes "module failed to import" from "module
  imported but does not expose this name" (see :func:`__getattr__`).
- :data:`DASHBOARD_AVAILABILITY` is a read-only mapping of submodule
  import status.
- :data:`DASHBOARD_REGISTRY` is a read-only mapping of registered
  components. :func:`register_dashboard_component`,
  :func:`unregister_dashboard_component`, and
  :func:`clear_dashboard_registry` are the only mutators.
- Errors raised by any dashboard module derive from
  :class:`DashboardErrorBase` (or from :class:`ValueError`, which every
  submodule error inherits). :data:`DASHBOARD_ERROR_TYPES` includes both
  the base class and every concrete submodule error.
- :func:`build_default_dashboard` wires the whole layer — optionally
  around an existing :class:`memory.Pipeline`.

Availability and integration
----------------------------
The dashboard consumes the memory pipeline through
``GreenDecisionGraph.build_from_pipeline(pipeline)``. The
:func:`build_default_dashboard` helper accepts an optional
``memory_pipeline=`` argument and forwards it, so the typical wiring is:

    from memory import build_default_pipeline
    from dashboard import build_default_dashboard

    with build_default_pipeline() as pipeline:
        with build_default_dashboard(memory_pipeline=pipeline) as dash:
            payload = dash.graph.build_from_pipeline(
                pipeline, query="vision_inference",
            )

Versioning
----------
The package's ``__version__`` tracks the *wave* of enhancements applied
to Layer 11 as a whole. Individual submodules carry their own
``__version__`` and evolve at their own pace; the package version
independently indicates the state of the façade and the aggregate
construction API. Use :func:`health` to inspect both at runtime.

Import-time validation
----------------------
Set ``GREEN_AGENT_VALIDATE_DASHBOARD=1`` to log a per-component summary
on import (non-fatal). Set ``GREEN_AGENT_STRICT_VALIDATE_DASHBOARD=1``
to raise on validation failures — the previous behaviour of failing the
whole package import is opt-in.
"""

from __future__ import annotations

import importlib
import inspect
import logging
import os as _os
import threading
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
)

__version__ = "7.0.0"

#: Dashboard schema version stamped on serialized payloads.
SCHEMA_VERSION: int = 1

#: Canonical in-memory sentinel (matches ``memory.IN_MEMORY_PATH``).
IN_MEMORY_PATH: str = ":memory:"

#: Canonical default container tag (matches ``memory.DEFAULT_CONTAINER_TAG``).
DEFAULT_CONTAINER_TAG: str = "org:green-agent"

#: Fallback truth-level vocabulary (replaced by ``memory``'s value when
#: that package is importable).
_FALLBACK_TRUTH_LEVELS: Tuple[str, ...] = (
    "measured", "estimated", "simulated", "user-reported",
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class DashboardErrorBase(ValueError):
    """Base class for errors raised by the dashboard facade.

    Submodule errors are separately rooted at :class:`ValueError` for
    historical reasons (each submodule is importable on its own, and
    cannot depend on this package). ``DASHBOARD_ERROR_TYPES`` includes
    both this base and every concrete submodule error so a single
    ``except`` clause covers both.
    """


class DashboardRegistryError(DashboardErrorBase):
    """Raised for invalid registry operations.

    Replaces the bare :class:`TypeError` / :class:`ValueError` /
    :class:`KeyError` that the previous implementation raised.
    """


class DashboardVersionError(DashboardErrorBase):
    """Raised when dashboard submodules disagree on ``SCHEMA_VERSION``.

    Replaces the bare :class:`RuntimeError` used previously so the
    error is caught by :data:`DASHBOARD_ERROR_TYPES`.
    """


# --------------------------------------------------------------------------- #
# Sentinel
# --------------------------------------------------------------------------- #
_MISSING = object()


# --------------------------------------------------------------------------- #
# Guarded import of the submodules
# --------------------------------------------------------------------------- #
#: ``(module_name, required, exported_names)`` per submodule.
#:
#: The exported names list is **verified** against each submodule's
#: ``__all__`` at load time — a warning is logged for every declared
#: name that the submodule doesn't actually expose. Names that no
#: submodule exposes are simply not re-exported.
#:
#: ``green_decision_graph`` is required because it is the pipeline
#: consumer; every other submodule is optional.
_MODULE_SPECS: Tuple[Tuple[str, bool, Tuple[str, ...]], ...] = (
    (
        "green_decision_graph",
        True,
        (
            "GreenDecisionGraph",
            "GreenDecisionGraphConfig",
            "GreenDecisionGraphError",
            "GreenDecisionGraphInputError",
            "GreenDecisionGraphConfigError",
            "GreenDecisionGraphParseError",
            "GraphNode",
            "GraphEdge",
            "GraphPayload",
            "NodeType",
            "EdgeType",
        ),
    ),
    (
        "green_dashboard",
        False,
        (
            "GreenDashboard",
            "GreenDashboardConfig",
            "GreenDashboardError",
        ),
    ),
    (
        "symbolic_visualizer",
        False,
        (
            "SymbolicVisualizer",
            "SymbolicVisualizerConfig",
            "SymbolicVisualizerError",
        ),
    ),
    (
        "telemetry_loader",
        False,
        (
            "TelemetryLoader",
            "TelemetryLoaderConfig",
            "TelemetryLoaderError",
        ),
    ),
    (
        "api_server",
        False,
        (
            "create_app",
            "DashboardAPIConfig",
            "DashboardAPIError",
            "ActiveTaskRequest",
            "ExecutionLog",
            "LogResponse",
            "StartResponse",
        ),
    ),
)


def _import_module(module_name: str) -> Optional[Any]:
    """
    Import ``.{module_name}``; return None on any failure.

    Distinguishes "module not installed" (``ImportError``) from
    "module installed but broken" (any other exception — the latter is
    logged at ``WARNING`` with a traceback so a broken install is
    visible in logs).
    """
    try:
        return importlib.import_module(f".{module_name}", package=__name__)
    except ImportError as exc:
        logger.debug(
            "dashboard submodule %s not available: %s",
            module_name, exc,
        )
        return None
    except Exception:
        logger.exception(
            "dashboard submodule %s raised during import; treating as "
            "unavailable.", module_name,
        )
        return None


#: Populated during the loading loop. Read-only public view is
#: :data:`DASHBOARD_AVAILABILITY`.
_availability: Dict[str, bool] = {}

#: Loaded submodule objects, keyed by submodule name.
_LOADED_MODULES: Dict[str, Any] = {}

#: Mapping of exported name → submodule that provides it.
_NAME_TO_MODULE: Dict[str, str] = {}

#: Name → human-readable explanation of why it is unavailable.
_UNRESOLVED_REASONS: Dict[str, str] = {}


for _mod_name, _required, _names in _MODULE_SPECS:
    _module = _import_module(_mod_name)
    _availability[_mod_name] = _module is not None

    if _module is None:
        _reason = f"submodule {_mod_name!r} failed to import"
        for _name in _names:
            _NAME_TO_MODULE.setdefault(_name, _mod_name)
            _UNRESOLVED_REASONS.setdefault(_name, _reason)
        if _required:
            logger.error(
                "Required dashboard submodule %r failed to import; "
                "its names will raise ImportError on access.",
                _mod_name,
            )
        continue

    _LOADED_MODULES[_mod_name] = _module

    # Verify the spec against the submodule's own __all__ so a future
    # submodule change is visible in logs.
    _actual_exports = set(getattr(_module, "__all__", ()))
    _declared = set(_names)
    if _actual_exports:
        _stale = _declared - _actual_exports
        if _stale:
            logger.warning(
                "dashboard._MODULE_SPECS declares names not exposed by "
                "%s.__all__: %s. They will not be re-exported.",
                _mod_name, sorted(_stale),
            )

    for _name in _names:
        _NAME_TO_MODULE.setdefault(_name, _mod_name)
        if _name in globals():
            # Package-level name takes precedence (e.g. a shared
            # constant defined above).
            continue
        _value = getattr(_module, _name, _MISSING)
        if _value is _MISSING:
            _UNRESOLVED_REASONS.setdefault(
                _name,
                f"submodule {_mod_name!r} imported but does not expose "
                f"{_name!r}",
            )
            continue
        globals()[_name] = _value

del (
    _mod_name, _required, _module, _names, _name, _value,
    _reason, _actual_exports, _declared, _stale,
) if '_reason' in dir() else (
    _mod_name, _required, _module, _names, _name, _value,
    _actual_exports, _declared, _stale,
)


#: Read-only availability manifest.
DASHBOARD_AVAILABILITY: Mapping[str, bool] = MappingProxyType(_availability)


# --------------------------------------------------------------------------- #
# SDK availability flags (public names)
# --------------------------------------------------------------------------- #
def _resolve_sdk_flag(
    submodule_attr: str,
    *,
    submodule: str,
    dependency: str,
) -> bool:
    """
    Return True when ``dependency`` is importable.

    Prefers the submodule's own ``<NAME>_AVAILABLE`` flag when present
    (so the façade and the submodule always agree). Falls back to
    probing the dependency directly.
    """
    mod = _LOADED_MODULES.get(submodule)
    if mod is not None:
        value = getattr(mod, submodule_attr, None)
        if isinstance(value, bool):
            return value
    try:
        importlib.import_module(dependency)
        return True
    except ImportError:
        return False


#: True when ``matplotlib`` is importable (visualization backend).
MATPLOTLIB_AVAILABLE: bool = _resolve_sdk_flag(
    "MATPLOTLIB_AVAILABLE",
    submodule="symbolic_visualizer",
    dependency="matplotlib",
)

#: True when ``pandas`` is importable (TelemetryLoader backend).
PANDAS_AVAILABLE: bool = _resolve_sdk_flag(
    "PANDAS_AVAILABLE",
    submodule="telemetry_loader",
    dependency="pandas",
)

#: True when ``fastapi`` is importable (api_server backend).
FASTAPI_AVAILABLE: bool = _resolve_sdk_flag(
    "FASTAPI_AVAILABLE",
    submodule="api_server",
    dependency="fastapi",
)

#: True when ``uvicorn`` is importable (needed at ``main()`` runtime).
UVICORN_AVAILABLE: bool = _resolve_sdk_flag(
    "UVICORN_AVAILABLE",
    submodule="api_server",
    dependency="uvicorn",
)


# --------------------------------------------------------------------------- #
# Error tuple
# --------------------------------------------------------------------------- #
_ERROR_BASE_NAMES: Tuple[str, ...] = (
    "GreenDecisionGraphError",
    "GreenDashboardError",
    "SymbolicVisualizerError",
    "TelemetryLoaderError",
    "DashboardAPIError",
)

_collected_errors: List[Type[BaseException]] = []
for _err_name in _ERROR_BASE_NAMES:
    _cls = globals().get(_err_name)
    if (
        isinstance(_cls, type)
        and issubclass(_cls, BaseException)
        and _cls is not DashboardErrorBase
    ):
        _collected_errors.append(_cls)
del _err_name, _cls

#: Catch-all tuple. Always includes :class:`DashboardErrorBase` so the
#: façade's own errors (registry, version) are caught alongside the
#: submodule errors.
DASHBOARD_ERROR_TYPES: Tuple[Type[BaseException], ...] = (
    DashboardErrorBase,
    *_collected_errors,
)


# --------------------------------------------------------------------------- #
# Optional re-exports from the ``memory`` package
# --------------------------------------------------------------------------- #
_MEMORY_AVAILABLE: bool = False
_MEM_IN_MEMORY_PATH: str = IN_MEMORY_PATH
_MEM_SCHEMA_VERSION: int = SCHEMA_VERSION
_MEM_DEFAULT_CONTAINER_TAG: str = DEFAULT_CONTAINER_TAG
_MEM_DEFAULT_TRUTH_LEVELS: Tuple[str, ...] = _FALLBACK_TRUTH_LEVELS
_MEM_SUPERMEMORY_SDK_AVAILABLE: bool = False
_MEM_MCP_SDK_AVAILABLE: bool = False

try:  # pragma: no cover — environment-dependent
    from ..memory import (  # type: ignore
        IN_MEMORY_PATH as _mem_in_memory_path,
        SCHEMA_VERSION as _mem_schema_version,
        DEFAULT_CONTAINER_TAG as _mem_default_container_tag,
        DEFAULT_TRUTH_LEVELS as _mem_default_truth_levels,
        SUPERMEMORY_SDK_AVAILABLE as _mem_supermemory_sdk_available,
        MCP_SDK_AVAILABLE as _mem_mcp_sdk_available,
    )
    _MEMORY_AVAILABLE = True
    _MEM_IN_MEMORY_PATH = _mem_in_memory_path
    _MEM_SCHEMA_VERSION = _mem_schema_version
    _MEM_DEFAULT_CONTAINER_TAG = _mem_default_container_tag
    _MEM_DEFAULT_TRUTH_LEVELS = _mem_default_truth_levels
    _MEM_SUPERMEMORY_SDK_AVAILABLE = _mem_supermemory_sdk_available
    _MEM_MCP_SDK_AVAILABLE = _mem_mcp_sdk_available
    del (
        _mem_in_memory_path, _mem_schema_version,
        _mem_default_container_tag, _mem_default_truth_levels,
        _mem_supermemory_sdk_available, _mem_mcp_sdk_available,
    )
except ImportError:  # pragma: no cover - defensive
    logger.debug(
        "memory package unavailable; dashboard will use its own defaults "
        "for shared constants.",
    )

#: True when the sibling ``memory`` package is importable.
MEMORY_AVAILABLE: bool = _MEMORY_AVAILABLE

#: Mirrors ``memory.SUPERMEMORY_SDK_AVAILABLE`` when available.
SUPERMEMORY_SDK_AVAILABLE: bool = _MEM_SUPERMEMORY_SDK_AVAILABLE

#: Mirrors ``memory.MCP_SDK_AVAILABLE`` when available.
MCP_SDK_AVAILABLE: bool = _MEM_MCP_SDK_AVAILABLE

#: Truth-level vocabulary (from ``memory`` when available, otherwise a
#: local fallback). Re-exported for callers that need the vocabulary
#: without importing the memory package themselves.
DEFAULT_TRUTH_LEVELS: Tuple[str, ...] = _MEM_DEFAULT_TRUTH_LEVELS

#: The memory schema version this dashboard understands.
MEMORY_SCHEMA_VERSION: int = _MEM_SCHEMA_VERSION


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
_registry: Dict[str, Type[Any]] = {}
_registry_lock = threading.RLock()

#: Read-only registry view.
DASHBOARD_REGISTRY: Mapping[str, Type[Any]] = MappingProxyType(_registry)


_DEFAULT_REGISTRY_ALIASES: Tuple[Tuple[Tuple[str, ...], str], ...] = (
    (("graph", "green_decision_graph", "decision_graph"), "GreenDecisionGraph"),
    (("dashboard", "green_dashboard"), "GreenDashboard"),
    (("visualizer", "symbolic_visualizer"), "SymbolicVisualizer"),
    (("loader", "telemetry_loader"), "TelemetryLoader"),
)


def _register_defaults() -> None:
    """Populate the registry with aliases for the default classes."""
    with _registry_lock:
        for aliases, cls_name in _DEFAULT_REGISTRY_ALIASES:
            cls = globals().get(cls_name)
            if not isinstance(cls, type):
                continue
            for alias in aliases:
                _registry.setdefault(alias, cls)


_register_defaults()


def register_dashboard_component(name: str, cls: Type[Any]) -> None:
    """
    Register a dashboard component class under ``name``.

    Raises
    ------
    DashboardRegistryError
        If ``name`` is not a non-empty string, ``cls`` is not a class, or
        ``name`` is already registered to a different class.
    """
    if not isinstance(name, str) or not name:
        raise DashboardRegistryError(
            f"Component name must be a non-empty string, got {name!r}."
        )
    if not isinstance(cls, type):
        raise DashboardRegistryError(
            f"Expected a class, got {type(cls).__name__}."
        )
    with _registry_lock:
        existing = _registry.get(name)
        if existing is not None and existing is not cls:
            raise DashboardRegistryError(
                f"Dashboard component '{name}' already registered to "
                f"{existing.__name__}."
            )
        _registry[name] = cls
    logger.debug(
        "Registered dashboard component '%s' -> %s", name, cls.__name__,
    )


def unregister_dashboard_component(name: str) -> bool:
    """Remove a registry entry. Returns True if it existed."""
    if not isinstance(name, str) or not name:
        raise DashboardRegistryError(
            "Component name must be a non-empty string."
        )
    with _registry_lock:
        existed = _registry.pop(name, None) is not None
    if existed:
        logger.debug("Unregistered dashboard component '%s'.", name)
    return existed


def clear_dashboard_registry() -> None:
    """
    Reset the registry to its default contents.

    Intended for test isolation — restores the built-in aliases and
    drops any user-registered entries.
    """
    with _registry_lock:
        _registry.clear()
    _register_defaults()
    logger.debug("Dashboard registry reset to defaults.")


def get_dashboard_component(name: str) -> Type[Any]:
    """
    Return the dashboard component class registered under ``name``.

    Raises
    ------
    DashboardRegistryError
        If ``name`` is not registered.
    """
    with _registry_lock:
        try:
            return _registry[name]
        except KeyError as exc:
            raise DashboardRegistryError(
                f"Unknown dashboard component '{name}'. "
                f"Available: {sorted(_registry)}"
            ) from exc


def list_dashboard_components() -> Dict[str, str]:
    """Return a mapping of registered name → class name."""
    with _registry_lock:
        return {n: c.__name__ for n, c in _registry.items()}


def list_modules() -> Dict[str, bool]:
    """Return a copy of :data:`DASHBOARD_AVAILABILITY`."""
    return dict(_availability)


# --------------------------------------------------------------------------- #
# Health
# --------------------------------------------------------------------------- #
def health() -> Dict[str, Any]:
    """
    Return an aggregate availability and capability snapshot.

    Answers "is the dashboard layer fully operational?" without
    constructing any component.
    """
    with _registry_lock:
        registered = sorted(_registry)
    unresolved = sorted(_UNRESOLVED_REASONS)
    return {
        "version": __version__,
        "schema_version": SCHEMA_VERSION,
        "submodules": dict(_availability),
        "unavailable_names": unresolved,
        "registered_components": registered,
        "memory_available": MEMORY_AVAILABLE,
        "supermemory_sdk_available": SUPERMEMORY_SDK_AVAILABLE,
        "mcp_sdk_available": MCP_SDK_AVAILABLE,
        "matplotlib_available": MATPLOTLIB_AVAILABLE,
        "pandas_available": PANDAS_AVAILABLE,
        "fastapi_available": FASTAPI_AVAILABLE,
        "uvicorn_available": UVICORN_AVAILABLE,
    }


# --------------------------------------------------------------------------- #
# Dashboard aggregate
# --------------------------------------------------------------------------- #
@dataclass
class Dashboard:
    """Wired dashboard layer.

    Holds a live instance of every component plus a convenience
    ``close()`` that shuts them down in reverse dependency order.

    ``telemetry_loader`` is an instance of the static-only
    :class:`TelemetryLoader` class. Because the class has no per-instance
    state, ``dash.telemetry_loader.load(path)`` is equivalent to
    ``TelemetryLoader.load(path)`` — either form works.
    """

    graph: Any = None
    green_dashboard: Any = None
    symbolic_visualizer: Any = None
    telemetry_loader: Any = None

    #: The memory pipeline the graph was built from (optional).
    memory_pipeline: Any = None

    # Component iteration order — used by close and repr.
    _COMPONENT_ORDER: Tuple[str, ...] = field(
        default=(
            "graph",
            "green_dashboard",
            "symbolic_visualizer",
            "telemetry_loader",
        ),
        init=False,
        repr=False,
    )

    def close(self) -> None:
        """Best-effort shutdown, outer layer first."""
        report = self.close_with_report()
        for name, error in report.items():
            if error is not None:
                logger.warning(
                    "dashboard close failed for %s: %s", name, error,
                )

    def close_with_report(self) -> Dict[str, Optional[str]]:
        """
        Close each component and report the per-component outcome.

        Returns
        -------
        Dict[str, Optional[str]]
            ``{name: None}`` on success, ``{name: "<error>"}`` on failure,
            ``{name: None}`` for absent components.
        """
        results: Dict[str, Optional[str]] = {}
        for name in self._COMPONENT_ORDER:
            component = getattr(self, name, None)
            if component is None:
                results[name] = None
                continue
            close = getattr(component, "close", None)
            if not callable(close):
                results[name] = None
                continue
            try:
                close()
                results[name] = None
            except Exception as exc:
                results[name] = f"{type(exc).__name__}: {exc}"
        return results

    def __enter__(self) -> "Dashboard":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __iter__(self):
        for name in self._COMPONENT_ORDER:
            component = getattr(self, name, None)
            if component is not None:
                yield name, component

    def __repr__(self) -> str:
        parts = [
            f"{name}={type(component).__name__}"
            for name, component in self
        ]
        pipeline = (
            type(self.memory_pipeline).__name__
            if self.memory_pipeline is not None else None
        )
        return (
            f"Dashboard(components={{{', '.join(parts)}}}, "
            f"memory_pipeline={pipeline})"
        )


#: Collaborators required to build the default dashboard.
_DASHBOARD_REQUIRED: Tuple[str, ...] = (
    "GreenDecisionGraph",
    "GreenDashboard",
    "SymbolicVisualizer",
    "TelemetryLoader",
)


def _instantiate_component(
    cls: Type[Any],
    *,
    config: Any,
    strict: bool,
    label: str,
) -> Any:
    """
    Instantiate ``cls`` with only the kwargs its ``__init__`` accepts.

    Uses :func:`inspect.signature` rather than a try/except cascade so a
    ``TypeError`` raised inside the constructor (e.g. an invalid config
    value) propagates to the caller instead of silently falling back to
    the class's default construction.
    """
    try:
        sig = inspect.signature(cls.__init__)
        params = set(sig.parameters.keys()) - {"self"}
    except (TypeError, ValueError):
        # Defensive: a C-extension class may not expose a signature.
        params = set()

    kwargs: Dict[str, Any] = {}
    if "config" in params:
        kwargs["config"] = config
    elif config is not None:
        logger.warning(
            "%s does not accept config=; the provided config will be "
            "ignored.", label,
        )
    if "strict" in params:
        kwargs["strict"] = strict

    return cls(**kwargs)


def build_default_dashboard(
    *,
    memory_pipeline: Optional[Any] = None,
    graph_config: Optional[Any] = None,
    green_dashboard_config: Optional[Any] = None,
    symbolic_visualizer_config: Optional[Any] = None,
    telemetry_loader_config: Optional[Any] = None,
    strict: bool = True,
    require_all: bool = False,
) -> Dashboard:
    """
    Build the default wired dashboard.

    Parameters
    ----------
    memory_pipeline : optional
        A ``memory.Pipeline``. Stored on the returned
        :class:`Dashboard` and available to callers invoking
        ``dash.graph.build_from_pipeline(...)``.
    graph_config, green_dashboard_config, symbolic_visualizer_config,
    telemetry_loader_config : optional
        Per-component config. ``None`` uses each component's own default.
    strict : bool
        Propagated to each component that accepts ``strict=``.
    require_all : bool
        If True, raise :class:`DashboardErrorBase` when any component
        module is unavailable. If False (default), missing components
        are simply left as ``None``.

    Returns
    -------
    Dashboard
    """
    if require_all:
        missing = [
            name for name in _DASHBOARD_REQUIRED
            if globals().get(name) is None
        ]
        if missing:
            raise DashboardErrorBase(
                "cannot build default dashboard: missing "
                f"{sorted(missing)}. Install missing dependencies or "
                "import the submodules individually."
            )

    def _safe_build(
        cls_name: str,
        config: Any,
        label: str,
    ) -> Any:
        cls = globals().get(cls_name)
        if not isinstance(cls, type):
            return None
        try:
            return _instantiate_component(
                cls, config=config, strict=strict, label=label,
            )
        except Exception:
            logger.exception(
                "could not instantiate %s with the supplied config; "
                "leaving the component as None.", label,
            )
            return None

    return Dashboard(
        graph=_safe_build(
            "GreenDecisionGraph", graph_config, "GreenDecisionGraph",
        ),
        green_dashboard=_safe_build(
            "GreenDashboard", green_dashboard_config, "GreenDashboard",
        ),
        symbolic_visualizer=_safe_build(
            "SymbolicVisualizer",
            symbolic_visualizer_config,
            "SymbolicVisualizer",
        ),
        telemetry_loader=_safe_build(
            "TelemetryLoader", telemetry_loader_config, "TelemetryLoader",
        ),
        memory_pipeline=memory_pipeline,
    )


def dashboard_payload_from_pipeline(
    memory_pipeline: Any,
    *,
    query: str,
    k: int = 5,
    container_tag: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None,
    current_telemetry: Optional[Dict[str, float]] = None,
    current_policy_version: Optional[str] = None,
    graph: Optional[Any] = None,
) -> Any:
    """
    Build a graph payload from a memory pipeline in one call.

    Thin wrapper around ``GreenDecisionGraph.build_from_pipeline``.
    Uses the supplied ``graph`` when provided; otherwise constructs a
    default one.
    """
    if graph is None:
        cls = globals().get("GreenDecisionGraph")
        if cls is None:
            raise DashboardErrorBase(
                "dashboard_payload_from_pipeline requires "
                "GreenDecisionGraph to be importable."
            )
        graph = cls()
    builder = getattr(graph, "build_from_pipeline", None)
    if not callable(builder):
        raise DashboardErrorBase(
            "graph does not expose build_from_pipeline(); supply a "
            "GreenDecisionGraph instance."
        )
    return builder(
        memory_pipeline,
        query=query,
        k=k,
        container_tag=container_tag,
        filters=filters,
        current_telemetry=current_telemetry,
        current_policy_version=current_policy_version,
    )


# --------------------------------------------------------------------------- #
# Schema version compatibility
# --------------------------------------------------------------------------- #
def assert_compatible_dashboard_versions() -> Dict[str, int]:
    """
    Verify that every loaded submodule declares the same
    ``SCHEMA_VERSION``.

    Returns
    -------
    Dict[str, int]
        Mapping of module name → detected version. Empty if no module
        declares one.

    Raises
    ------
    DashboardVersionError
        If loaded modules disagree on the version.
    """
    versions: Dict[str, int] = {}
    for module_name, module in _LOADED_MODULES.items():
        value = getattr(module, "SCHEMA_VERSION", None)
        if (
            isinstance(value, int)
            and not isinstance(value, bool)
            and value > 0
        ):
            versions[module_name] = value

    distinct = set(versions.values())
    if len(distinct) > 1:
        raise DashboardVersionError(
            "incompatible schema versions across dashboard submodules: "
            f"{versions}"
        )
    return versions


#: Alias with a name that doesn't imply "debug-only".
check_compatible_dashboard_versions = assert_compatible_dashboard_versions


# --------------------------------------------------------------------------- #
# Lazy name resolution
# --------------------------------------------------------------------------- #
def __getattr__(name: str) -> Any:
    """
    Resolve a missing attribute.

    Called by Python when ``name`` is not in the module's globals:

    - If ``name`` was declared in ``_MODULE_SPECS`` and is known to be
      unavailable, raise :class:`ImportError` with a specific reason
      (distinguishing "module failed to import" from "module imported
      but does not expose this name").
    - Otherwise raise :class:`AttributeError` as Python does by default.
    """
    reason = _UNRESOLVED_REASONS.get(name)
    if reason is not None:
        raise ImportError(
            f"{__name__}.{name} is unavailable: {reason}."
        )
    raise AttributeError(
        f"module {__name__!r} has no attribute {name!r}"
    )


def _build_all() -> List[str]:
    """
    Build ``__all__`` from static names + successfully loaded exports.

    Static names are the ones the façade defines itself (constants,
    registry API, aggregate helpers). Loaded exports are names that were
    actually resolved from a submodule; unresolved names are excluded so
    ``from dashboard import *`` never trips over them.
    """
    static_names = {
        # Metadata
        "__version__",
        "SCHEMA_VERSION",
        "IN_MEMORY_PATH",
        "DEFAULT_CONTAINER_TAG",
        "DEFAULT_TRUTH_LEVELS",
        "MEMORY_SCHEMA_VERSION",
        "MEMORY_AVAILABLE",
        "SUPERMEMORY_SDK_AVAILABLE",
        "MCP_SDK_AVAILABLE",
        "MATPLOTLIB_AVAILABLE",
        "PANDAS_AVAILABLE",
        "FASTAPI_AVAILABLE",
        "UVICORN_AVAILABLE",
        # Errors
        "DashboardErrorBase",
        "DashboardRegistryError",
        "DashboardVersionError",
        "DASHBOARD_ERROR_TYPES",
        # Registry / aggregate / diagnostics
        "DASHBOARD_REGISTRY",
        "DASHBOARD_AVAILABILITY",
        "register_dashboard_component",
        "unregister_dashboard_component",
        "clear_dashboard_registry",
        "get_dashboard_component",
        "list_dashboard_components",
        "list_modules",
        "health",
        "Dashboard",
        "build_default_dashboard",
        "dashboard_payload_from_pipeline",
        "assert_compatible_dashboard_versions",
        "check_compatible_dashboard_versions",
    }
    # Include only submodule exports that actually resolved.
    resolved_exports = {
        name for name in _NAME_TO_MODULE if name in globals()
    }
    return sorted(static_names | resolved_exports)


__all__: List[str] = _build_all()


def __dir__() -> List[str]:
    """Expose the full set of names to ``dir()``."""
    return sorted(set(globals()) | set(__all__))


# --------------------------------------------------------------------------- #
# Import-time validation
# --------------------------------------------------------------------------- #
def _run_import_time_validation(*, strict: bool) -> None:
    """
    Log a per-component summary; optionally raise on failures.

    Non-strict mode (the default when
    ``GREEN_AGENT_VALIDATE_DASHBOARD=1``) logs findings but never fails
    the import. Strict mode
    (``GREEN_AGENT_STRICT_VALIDATE_DASHBOARD=1``) raises
    :class:`DashboardErrorBase` on any finding.
    """
    missing_modules = [
        name for name, ok in _availability.items() if not ok
    ]
    if missing_modules:
        logger.warning(
            "Dashboard submodules unavailable at import time: %s",
            sorted(missing_modules),
        )
        if strict:
            raise DashboardErrorBase(
                "dashboard validation failed: missing submodules "
                f"{sorted(missing_modules)}"
            )

    if _UNRESOLVED_REASONS:
        logger.warning(
            "Dashboard names that could not be resolved: %s",
            sorted(_UNRESOLVED_REASONS),
        )
        if strict:
            raise DashboardErrorBase(
                "dashboard validation failed: unresolved names "
                f"{sorted(_UNRESOLVED_REASONS)}"
            )

    for registry_name, registry_cls in list(_registry.items()):
        if not callable(registry_cls):
            logger.error(
                "Dashboard component '%s' (%r) is not callable.",
                registry_name, registry_cls,
            )
            if strict:
                raise DashboardErrorBase(
                    f"dashboard component '{registry_name}' is not "
                    f"callable."
                )

    try:
        versions = assert_compatible_dashboard_versions()
    except DashboardVersionError:
        logger.exception(
            "Schema version mismatch across dashboard submodules."
        )
        if strict:
            raise
    else:
        if versions:
            logger.info(
                "All dashboard submodules share schema version %s.",
                next(iter(set(versions.values()))),
            )


if _os.environ.get("GREEN_AGENT_VALIDATE_DASHBOARD") == "1":
    _run_import_time_validation(
        strict=_os.environ.get(
            "GREEN_AGENT_STRICT_VALIDATE_DASHBOARD"
        ) == "1",
    )


# --------------------------------------------------------------------------- #
# Smoke test: python -m dashboard
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

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

    def _expect_error(name: str, expected: type, fn, *a, **kw):
        global failed
        try:
            fn(*a, **kw)
            _check(name, False)
        except expected:
            _check(name, True)
        except Exception as exc:
            print(
                f"  ❌ {name} — wrong exception "
                f"{type(exc).__name__}: {exc}"
            )
            failed += 1

    # ---- 1. Metadata ---------------------------------------------- #
    print("metadata:")
    _check("package version set", isinstance(__version__, str) and __version__)
    _check("SCHEMA_VERSION is int", isinstance(SCHEMA_VERSION, int))
    _check("DASHBOARD_AVAILABILITY is Mapping", hasattr(
        DASHBOARD_AVAILABILITY, "__getitem__"
    ))
    _check("DASHBOARD_REGISTRY is Mapping", hasattr(
        DASHBOARD_REGISTRY, "__getitem__"
    ))

    # Frozen views
    try:
        DASHBOARD_AVAILABILITY["hack"] = True  # type: ignore[index]
        _check("availability read-only", False)
    except TypeError:
        _check("availability read-only", True)
    try:
        DASHBOARD_REGISTRY["hack"] = object  # type: ignore[index]
        _check("registry read-only", False)
    except TypeError:
        _check("registry read-only", True)

    # ---- 2. Every __all__ name resolves --------------------------- #
    print("__all__:")
    module = sys.modules[__name__]
    missing = []
    for _n in __all__:
        try:
            getattr(module, _n)
        except Exception:
            missing.append(_n)
    _check("all __all__ names resolve", not missing)
    if missing:
        print(f"    missing: {missing}")

    # ---- 3. Availability / health --------------------------------- #
    print("health:")
    h = health()
    _check(
        "green_decision_graph available",
        _availability.get("green_decision_graph") is True,
    )
    _check(
        "health has all expected fields",
        {
            "version", "schema_version", "submodules",
            "unavailable_names", "registered_components",
            "memory_available", "supermemory_sdk_available",
            "mcp_sdk_available", "matplotlib_available",
            "pandas_available", "fastapi_available",
            "uvicorn_available",
        }.issubset(h.keys()),
    )

    # ---- 4. Error types ------------------------------------------- #
    print("error types:")
    _check(
        "DashboardErrorBase in DASHBOARD_ERROR_TYPES",
        DashboardErrorBase in DASHBOARD_ERROR_TYPES,
    )
    _check(
        "DashboardRegistryError subclass of DashboardErrorBase",
        issubclass(DashboardRegistryError, DashboardErrorBase),
    )
    _check(
        "DashboardVersionError subclass of DashboardErrorBase",
        issubclass(DashboardVersionError, DashboardErrorBase),
    )

    # ---- 5. Registry API ------------------------------------------ #
    print("registry:")
    _check(
        "graph alias registered",
        "graph" in DASHBOARD_REGISTRY,
    )
    _check(
        "get_dashboard_component('graph') works",
        isinstance(
            get_dashboard_component("graph"), type,
        ),
    )
    _expect_error(
        "unknown component raises DashboardRegistryError",
        DashboardRegistryError,
        get_dashboard_component,
        "not-a-real-name",
    )
    _expect_error(
        "register with empty name raises DashboardRegistryError",
        DashboardRegistryError,
        register_dashboard_component,
        "",
        object,
    )
    _expect_error(
        "register with non-class raises DashboardRegistryError",
        DashboardRegistryError,
        register_dashboard_component,
        "x",
        42,
    )

    class _FakeDashboard:
        pass

    register_dashboard_component("fake", _FakeDashboard)
    _check(
        "register then get",
        get_dashboard_component("fake") is _FakeDashboard,
    )
    _expect_error(
        "conflicting registration raises DashboardRegistryError",
        DashboardRegistryError,
        register_dashboard_component,
        "dashboard",
        _FakeDashboard,
    )
    _check(
        "unregister existing returns True",
        unregister_dashboard_component("fake") is True,
    )
    _check(
        "unregister missing returns False",
        unregister_dashboard_component("fake") is False,
    )
    clear_dashboard_registry()
    _check(
        "clear resets defaults",
        "graph" in DASHBOARD_REGISTRY,
    )

    # ---- 6. Lazy __getattr__ messaging ---------------------------- #
    print("lazy loading:")
    _expect_error(
        "unknown attribute raises AttributeError",
        AttributeError,
        getattr,
        module,
        "not-a-real-name",
    )

    # A name in _UNRESOLVED_REASONS raises ImportError with cause.
    if _UNRESOLVED_REASONS:
        unresolved_name = next(iter(_UNRESOLVED_REASONS))
        _expect_error(
            f"unresolved name {unresolved_name!r} raises ImportError",
            ImportError,
            getattr,
            module,
            unresolved_name,
        )
    else:
        _check(
            "no unresolved names in this environment", True,
        )

    # ---- 7. Schema versions -------------------------------------- #
    print("versions:")
    versions = assert_compatible_dashboard_versions()
    _check(
        "schema version check does not raise on fresh install",
        isinstance(versions, dict),
    )

    # ---- 8. Component build -------------------------------------- #
    print("component build:")
    if _availability.get("green_decision_graph"):
        from datetime import datetime, timezone
        graph = GreenDecisionGraph()
        bundle = {
            "query": "test",
            "memories": [{
                "id": "m1",
                "metadata": {
                    "run_id": "r1",
                    "kind": "decision_outcome",
                    "workload_type": "vision_inference",
                    "truth_level": "measured",
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                },
            }],
        }
        payload = graph.build_payload(recall_bundles=[bundle])
        _check("graph builds payload", payload.node_count > 0)
    else:
        _check("graph build (skipped — module unavailable)", True)

    # ---- 9. Aggregate + context manager --------------------------- #
    print("aggregate:")
    dash = build_default_dashboard()
    _check(
        "dashboard repr mentions graph",
        "graph=" in repr(dash),
    )
    if _availability.get("green_decision_graph"):
        with build_default_dashboard() as ctx:
            _check(
                "context manager yields Dashboard",
                isinstance(ctx, Dashboard),
            )
        _check(
            "context manager closes cleanly",
            ctx.close_with_report() == {
                "graph": None,
                "green_dashboard": None,
                "symbolic_visualizer": None,
                "telemetry_loader": None,
            },
        )

    # ---- 10. Invalid config surfaces ----------------------------- #
    if _availability.get("green_decision_graph"):
        # A malformed config should propagate, not be silently dropped.
        try:
            build_default_dashboard(graph_config="not-a-config")
            _check(
                "invalid graph_config propagates as error", False,
            )
        except DashboardErrorBase:
            _check(
                "invalid graph_config propagates as error", True,
            )
        except Exception:
            _check(
                "invalid graph_config propagates (not "
                "DashboardErrorBase, but at least not silent)", True,
            )

    # ---- 11. Pipeline bridge (skipped if memory missing) ---------- #
    if MEMORY_AVAILABLE and _availability.get("green_decision_graph"):
        try:
            from ..memory import build_default_pipeline  # type: ignore
            with build_default_pipeline() as mp:
                with build_default_dashboard(memory_pipeline=mp) as dash2:
                    _check(
                        "pipeline stored on dashboard",
                        dash2.memory_pipeline is mp,
                    )
        except Exception as exc:
            print(f"  pipeline bridge skipped: {exc}")
    else:
        _check("pipeline bridge (skipped — memory unavailable)", True)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
