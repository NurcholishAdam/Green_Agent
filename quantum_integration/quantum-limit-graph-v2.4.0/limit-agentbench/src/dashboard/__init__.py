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
- :mod:`api_server`           — FastAPI + WebSocket REST layer
                                (imported when available).

Conventions
-----------
- Each submodule is imported under its own guard; a failure in one does
  not disable the rest. Names declared by a failed submodule raise a
  descriptive :class:`ImportError` on access via :func:`__getattr__`.
- :data:`DASHBOARD_AVAILABILITY` reports per-module import status.
- :data:`DASHBOARD_REGISTRY` maps symbolic names to component classes.
- Errors raised by any dashboard module derive from
  :class:`DashboardErrorBase`; catching everything is done via
  :data:`DASHBOARD_ERROR_TYPES`.
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
"""

from __future__ import annotations

import importlib
import logging
import os as _os
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
)

__version__ = "6.0.0"

#: Dashboard schema version stamped on serialized payloads.
SCHEMA_VERSION: int = 1

#: Canonical in-memory sentinel (matches ``memory.IN_MEMORY_PATH``).
IN_MEMORY_PATH: str = ":memory:"

#: Canonical default container tag (matches ``memory.DEFAULT_CONTAINER_TAG``).
DEFAULT_CONTAINER_TAG: str = "org:green-agent"

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Error base
# --------------------------------------------------------------------------- #
class DashboardErrorBase(ValueError):
    """Conceptual base class for dashboard errors.

    Every dashboard submodule defines its own error hierarchy rooted at
    ``ValueError``. To catch every dashboard error in one clause use
    :data:`DASHBOARD_ERROR_TYPES`:

        try:
            ...
        except DASHBOARD_ERROR_TYPES as exc:
            ...

    ``DashboardErrorBase`` is provided for typing, documentation, and
    user-defined errors that want to opt into the package's convention.
    """


# --------------------------------------------------------------------------- #
# Per-module guarded imports
# --------------------------------------------------------------------------- #
#: ``(module_name, required, exported_names)`` per submodule.
#:
#: Ordering reflects the dependency graph: the graph builder first (it
#: is the pipeline consumer), then the analytical components, then the
#: HTTP layer.
_MODULE_SPECS: Tuple[Tuple[str, bool, Tuple[str, ...]], ...] = (
    (
        "green_decision_graph",
        False,
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
            "GreenDashboardInputError",
            "GreenDashboardConfigError",
            "GreenDashboardParseError",
        ),
    ),
    (
        "symbolic_visualizer",
        False,
        (
            "SymbolicVisualizer",
            "SymbolicVisualizerConfig",
            "SymbolicVisualizerError",
            "SymbolicVisualizerInputError",
            "SymbolicVisualizerConfigError",
        ),
    ),
    (
        "telemetry_loader",
        False,
        (
            "TelemetryLoader",
            "TelemetryLoaderConfig",
            "TelemetryLoaderError",
            "TelemetryLoaderInputError",
            "TelemetryLoaderConfigError",
            "TelemetryLoaderParseError",
        ),
    ),
    (
        "api_server",
        False,
        (
            "create_app",
            "serve",
            "APIServerConfig",
            "APIServerError",
        ),
    ),
)


def _import_module(module_name: str) -> Optional[Any]:
    """Import ``.{module_name}``; return None on any failure."""
    try:
        return importlib.import_module(f".{module_name}", package=__name__)
    except Exception as exc:  # noqa: BLE001 - defensive
        logger.debug(
            "Dashboard submodule %s could not be imported: %s",
            module_name, exc,
        )
        return None


#: Populated during the loading loop.
DASHBOARD_AVAILABILITY: Dict[str, bool] = {}
_LOADED_MODULES: Dict[str, Any] = {}
_NAME_TO_MODULE: Dict[str, str] = {}

#: Names that failed to resolve because their source module failed.
_UNRESOLVED: List[str] = []

for _mod_name, _required, _names in _MODULE_SPECS:
    _module = _import_module(_mod_name)
    DASHBOARD_AVAILABILITY[_mod_name] = _module is not None

    if _module is None:
        if _required:
            logger.error(
                "Required dashboard submodule %r failed to import; "
                "its names will raise ImportError on access.",
                _mod_name,
            )
        for _name in _names:
            _NAME_TO_MODULE.setdefault(_name, _mod_name)
        continue

    _LOADED_MODULES[_mod_name] = _module
    for _name in _names:
        if _name in globals():
            # Already defined at package level (e.g. a shared constant);
            # don't clobber it.
            _NAME_TO_MODULE.setdefault(_name, _mod_name)
            continue
        _value = getattr(_module, _name, None)
        if _value is None:
            _UNRESOLVED.append(_name)
            _NAME_TO_MODULE.setdefault(_name, _mod_name)
            continue
        globals()[_name] = _value
        _NAME_TO_MODULE.setdefault(_name, _mod_name)

del _mod_name, _required, _module, _names, _name, _value


# --------------------------------------------------------------------------- #
# SDK availability flags (public names)
# --------------------------------------------------------------------------- #
#: True when ``matplotlib`` is importable (SymbolicVisualizer backend).
MATPLOTLIB_AVAILABLE: bool = bool(
    getattr(
        _LOADED_MODULES.get("symbolic_visualizer"),
        "MATPLOTLIB_AVAILABLE",
        False,
    )
)

#: True when ``pandas`` is importable (TelemetryLoader backend).
PANDAS_AVAILABLE: bool = bool(
    getattr(
        _LOADED_MODULES.get("telemetry_loader"),
        "PANDAS_AVAILABLE",
        False,
    )
)

#: True when ``fastapi`` is importable (api_server backend).
FASTAPI_AVAILABLE: bool = bool(
    getattr(
        _LOADED_MODULES.get("api_server"),
        "FASTAPI_AVAILABLE",
        False,
    )
)


# --------------------------------------------------------------------------- #
# Error tuple
# --------------------------------------------------------------------------- #
_ERROR_BASE_NAMES: Tuple[str, ...] = (
    "GreenDecisionGraphError",
    "GreenDashboardError",
    "SymbolicVisualizerError",
    "TelemetryLoaderError",
)

_collected_errors: List[Type[BaseException]] = []
for _err_name in _ERROR_BASE_NAMES:
    _cls = globals().get(_err_name)
    if isinstance(_cls, type) and issubclass(_cls, BaseException):
        _collected_errors.append(_cls)
del _err_name, _cls

#: Tuple of every module-level error base, safe for ``except``.
DASHBOARD_ERROR_TYPES: Tuple[Type[BaseException], ...] = (
    tuple(_collected_errors) if _collected_errors else (DashboardErrorBase,)
)


# --------------------------------------------------------------------------- #
# Optional re-exports from the ``memory`` package
# --------------------------------------------------------------------------- #
_MEMORY_AVAILABLE: bool = False
_MEM_IN_MEMORY_PATH: str = IN_MEMORY_PATH
_MEM_SCHEMA_VERSION: int = SCHEMA_VERSION
_MEM_DEFAULT_CONTAINER_TAG: str = DEFAULT_CONTAINER_TAG
_MEM_DEFAULT_TRUTH_LEVELS: Tuple[str, ...] = (
    "measured", "estimated", "simulated", "user-reported",
)
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

#: Truth-level vocabulary (from ``memory`` when available).
DEFAULT_TRUTH_LEVELS: Tuple[str, ...] = _MEM_DEFAULT_TRUTH_LEVELS

#: The memory schema version this dashboard understands.
MEMORY_SCHEMA_VERSION: int = _MEM_SCHEMA_VERSION


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
DASHBOARD_REGISTRY: Dict[str, Type[Any]] = {}


def _register_defaults() -> None:
    """Populate ``DASHBOARD_REGISTRY`` with aliases for known classes."""
    pairs: Tuple[Tuple[Tuple[str, ...], str], ...] = (
        (
            ("graph", "green_decision_graph", "decision_graph"),
            "GreenDecisionGraph",
        ),
        (
            ("dashboard", "green_dashboard"),
            "GreenDashboard",
        ),
        (
            ("visualizer", "symbolic_visualizer"),
            "SymbolicVisualizer",
        ),
        (
            ("loader", "telemetry_loader"),
            "TelemetryLoader",
        ),
    )
    for aliases, cls_name in pairs:
        cls = globals().get(cls_name)
        if not isinstance(cls, type):
            continue
        for alias in aliases:
            DASHBOARD_REGISTRY.setdefault(alias, cls)


_register_defaults()


def register_dashboard_component(name: str, cls: Type[Any]) -> None:
    """Register a dashboard component class under ``name``.

    Raises
    ------
    TypeError
        If ``name`` is not a non-empty string or ``cls`` is not a class.
    ValueError
        If ``name`` is already registered to a different class.
    """
    if not isinstance(name, str) or not name:
        raise TypeError(
            "Component name must be a non-empty string, got "
            f"{name!r}."
        )
    if not isinstance(cls, type):
        raise TypeError(
            f"Expected a class, got {type(cls).__name__}."
        )
    existing = DASHBOARD_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"Dashboard component '{name}' already registered to "
            f"{existing.__name__}."
        )
    DASHBOARD_REGISTRY[name] = cls
    logger.debug(
        "Registered dashboard component '%s' -> %s", name, cls.__name__,
    )


def unregister_dashboard_component(name: str) -> bool:
    """Remove a registry entry. Returns True if it existed."""
    if not isinstance(name, str) or not name:
        raise TypeError(
            "Component name must be a non-empty string."
        )
    existed = DASHBOARD_REGISTRY.pop(name, None) is not None
    if existed:
        logger.debug("Unregistered dashboard component '%s'.", name)
    return existed


def clear_dashboard_registry() -> None:
    """Reset the registry to its default contents.

    Intended for test isolation — restores the built-in aliases and
    drops any user-registered entries.
    """
    DASHBOARD_REGISTRY.clear()
    _register_defaults()
    logger.debug("Dashboard registry reset to defaults.")


def get_dashboard_component(name: str) -> Type[Any]:
    """Return the dashboard component class registered under ``name``."""
    try:
        return DASHBOARD_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown dashboard component '{name}'. "
            f"Available: {sorted(DASHBOARD_REGISTRY)}"
        ) from exc


def list_dashboard_components() -> Dict[str, str]:
    """Return a mapping of registered name → class name."""
    return {n: c.__name__ for n, c in DASHBOARD_REGISTRY.items()}


def list_modules() -> Dict[str, bool]:
    """Return a copy of ``DASHBOARD_AVAILABILITY``."""
    return dict(DASHBOARD_AVAILABILITY)


# --------------------------------------------------------------------------- #
# Dashboard aggregate
# --------------------------------------------------------------------------- #
@dataclass
class Dashboard:
    """Wired dashboard layer.

    Holds a live instance of every component plus a convenience
    ``close()`` that shuts them down in reverse dependency order.
    """

    graph: Any = None
    green_dashboard: Any = None
    symbolic_visualizer: Any = None
    telemetry_loader: Any = None

    #: The memory pipeline the graph was built from (optional).
    memory_pipeline: Any = None

    # ------------------------------------------------------------------ #
    def close(self) -> None:
        """Best-effort shutdown, outer layer first."""
        order: Tuple[Any, ...] = (
            self.graph,
            self.telemetry_loader,
            self.symbolic_visualizer,
            self.green_dashboard,
        )
        for component in order:
            if component is None:
                continue
            close = getattr(component, "close", None)
            if callable(close):
                try:
                    close()
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning(
                        "dashboard close failed for %r: %s",
                        type(component).__name__, exc,
                    )

    def __enter__(self) -> "Dashboard":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __iter__(self):
        for name in (
            "graph", "green_dashboard", "symbolic_visualizer",
            "telemetry_loader",
        ):
            component = getattr(self, name, None)
            if component is not None:
                yield name, component

    def __repr__(self) -> str:
        present = [name for name, _ in self]
        return f"Dashboard(components={present})"


#: Collaborators required to build the default dashboard.
_DASHBOARD_REQUIRED: Tuple[str, ...] = (
    "GreenDecisionGraph",
    "GreenDashboard",
    "SymbolicVisualizer",
    "TelemetryLoader",
)


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
    """Build the default wired dashboard.

    Parameters
    ----------
    memory_pipeline : optional
        A ``memory.Pipeline``. When provided it is stored on the
        returned :class:`Dashboard` and used implicitly when callers
        invoke ``dash.graph.build_from_pipeline(...)``.
    graph_config, green_dashboard_config, symbolic_visualizer_config,
    telemetry_loader_config : optional
        Per-component config. ``None`` uses each component's own default.
    strict : bool
        Propagated to each component that accepts a ``strict=`` argument.
    require_all : bool
        If True, raise :class:`ImportError` when any component module is
        unavailable. If False (default), missing components are simply
        left as ``None``.

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
            raise ImportError(
                "cannot build default dashboard: missing "
                f"{sorted(missing)}. Install missing dependencies or "
                "import the submodules individually."
            )

    graph = None
    cls = globals().get("GreenDecisionGraph")
    if isinstance(cls, type):
        try:
            graph = cls(config=graph_config, strict=strict)
        except TypeError:
            graph = cls(config=graph_config)

    green_dashboard = None
    cls = globals().get("GreenDashboard")
    if isinstance(cls, type):
        try:
            green_dashboard = cls(
                config=green_dashboard_config, strict=strict,
            )
        except TypeError:
            try:
                green_dashboard = cls(config=green_dashboard_config)
            except TypeError:
                green_dashboard = cls()

    symbolic_visualizer = None
    cls = globals().get("SymbolicVisualizer")
    if isinstance(cls, type):
        try:
            symbolic_visualizer = cls(
                config=symbolic_visualizer_config, strict=strict,
            )
        except TypeError:
            try:
                symbolic_visualizer = cls(config=symbolic_visualizer_config)
            except TypeError:
                symbolic_visualizer = cls()

    telemetry_loader = None
    cls = globals().get("TelemetryLoader")
    if isinstance(cls, type):
        try:
            telemetry_loader = cls(
                config=telemetry_loader_config, strict=strict,
            )
        except TypeError:
            try:
                telemetry_loader = cls(config=telemetry_loader_config)
            except TypeError:
                telemetry_loader = cls()

    return Dashboard(
        graph=graph,
        green_dashboard=green_dashboard,
        symbolic_visualizer=symbolic_visualizer,
        telemetry_loader=telemetry_loader,
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
    """Build a graph payload from a memory pipeline in one call.

    Thin wrapper around ``GreenDecisionGraph.build_from_pipeline``.
    Uses the supplied ``graph`` when provided; otherwise constructs a
    default one.
    """
    if graph is None:
        cls = globals().get("GreenDecisionGraph")
        if cls is None:
            raise ImportError(
                "dashboard_payload_from_pipeline requires "
                "GreenDecisionGraph to be importable."
            )
        graph = cls()
    builder = getattr(graph, "build_from_pipeline", None)
    if not callable(builder):
        raise TypeError(
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
    """Verify that every loaded submodule declares the same ``SCHEMA_VERSION``.

    Returns
    -------
    Dict[str, int]
        Mapping of module name → detected version. Empty if no module
        declares one.

    Raises
    ------
    RuntimeError
        If loaded modules disagree on the version.
    """
    versions: Dict[str, int] = {}
    for module_name, module in _LOADED_MODULES.items():
        value = getattr(module, "SCHEMA_VERSION", None)
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            versions[module_name] = value

    distinct = set(versions.values())
    if len(distinct) > 1:
        raise RuntimeError(
            "incompatible schema versions across dashboard submodules: "
            f"{versions}"
        )
    return versions


# --------------------------------------------------------------------------- #
# Lazy loading
# --------------------------------------------------------------------------- #
def __getattr__(name: str) -> Any:
    """Raise helpful errors for names declared but not resolved.

    Called only when ``name`` is absent from the module's globals. Two
    cases:

    - The name is declared by a submodule that failed to import →
      raise :class:`ImportError` with a descriptive message.
    - The name is not declared anywhere → raise :class:`AttributeError`
      (Python's standard behavior).
    """
    if name in _NAME_TO_MODULE:
        source = _NAME_TO_MODULE[name]
        raise ImportError(
            f"{__name__}.{name} is unavailable: the "
            f"{source!r} submodule failed to import. Install the "
            f"missing dependency or fix the submodule, then retry."
        )
    raise AttributeError(
        f"module {__name__!r} has no attribute {name!r}"
    )


def __dir__() -> List[str]:
    """Expose ``__all__`` plus the standard module attributes to ``dir()``."""
    base = set(globals().keys())
    base.update(__all__)
    return sorted(base)


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    # metadata
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

    # error base + catch-all
    "DashboardErrorBase",
    "DASHBOARD_ERROR_TYPES",

    # green decision graph (primary component)
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

    # green dashboard
    "GreenDashboard",
    "GreenDashboardConfig",
    "GreenDashboardError",
    "GreenDashboardInputError",
    "GreenDashboardConfigError",
    "GreenDashboardParseError",

    # symbolic visualizer
    "SymbolicVisualizer",
    "SymbolicVisualizerConfig",
    "SymbolicVisualizerError",
    "SymbolicVisualizerInputError",
    "SymbolicVisualizerConfigError",

    # telemetry loader
    "TelemetryLoader",
    "TelemetryLoaderConfig",
    "TelemetryLoaderError",
    "TelemetryLoaderInputError",
    "TelemetryLoaderConfigError",
    "TelemetryLoaderParseError",

    # HTTP layer (optional)
    "create_app",
    "serve",
    "APIServerConfig",
    "APIServerError",

    # registry + aggregate
    "DASHBOARD_REGISTRY",
    "DASHBOARD_AVAILABILITY",
    "register_dashboard_component",
    "unregister_dashboard_component",
    "clear_dashboard_registry",
    "get_dashboard_component",
    "list_dashboard_components",
    "list_modules",
    "Dashboard",
    "build_default_dashboard",
    "dashboard_payload_from_pipeline",
    "assert_compatible_dashboard_versions",
]


# --------------------------------------------------------------------------- #
# Import-time validation
# --------------------------------------------------------------------------- #
if _os.environ.get("GREEN_AGENT_VALIDATE_DASHBOARD") == "1":
    _missing_modules = [
        name for name, ok in DASHBOARD_AVAILABILITY.items() if not ok
    ]
    if _missing_modules:
        logger.warning(
            "Dashboard submodules unavailable at import time: %s",
            sorted(_missing_modules),
        )
    for _registry_name, _registry_cls in list(DASHBOARD_REGISTRY.items()):
        try:
            if not callable(_registry_cls):
                raise TypeError(f"{_registry_cls!r} is not callable.")
            logger.debug(
                "Dashboard component '%s' (%s) OK.",
                _registry_name, getattr(_registry_cls, "__name__", "?"),
            )
        except Exception:  # pragma: no cover - CI-only diagnostic
            logger.exception(
                "Dashboard component '%s' failed validation.",
                _registry_name,
            )
            raise
    try:
        _versions = assert_compatible_dashboard_versions()
        if _versions:
            logger.info(
                "All dashboard submodules share schema version %s.",
                next(iter(set(_versions.values()))),
            )
    except RuntimeError:
        logger.exception(
            "Schema version mismatch across dashboard submodules."
        )
        raise
    del _missing_modules, _registry_name, _registry_cls, _versions


# --------------------------------------------------------------------------- #
# Smoke test: python -m dashboard
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print(f"dashboard package v{__version__}")
    print(f"SCHEMA_VERSION = {SCHEMA_VERSION}")
    print(f"MEMORY_SCHEMA_VERSION = {MEMORY_SCHEMA_VERSION}")
    print(f"IN_MEMORY_PATH = {IN_MEMORY_PATH!r}")
    print(f"DEFAULT_CONTAINER_TAG = {DEFAULT_CONTAINER_TAG!r}")
    print(f"MEMORY_AVAILABLE = {MEMORY_AVAILABLE}")
    print(f"SUPERMEMORY_SDK_AVAILABLE = {SUPERMEMORY_SDK_AVAILABLE}")
    print(f"MCP_SDK_AVAILABLE = {MCP_SDK_AVAILABLE}")
    print(f"MATPLOTLIB_AVAILABLE = {MATPLOTLIB_AVAILABLE}")
    print(f"PANDAS_AVAILABLE = {PANDAS_AVAILABLE}")
    print(f"FASTAPI_AVAILABLE = {FASTAPI_AVAILABLE}")
    print()

    # --------------------------------------------------- 1. Module availability
    print("Module availability:")
    for module, available in sorted(DASHBOARD_AVAILABILITY.items()):
        marker = "✓" if available else "✗"
        print(f"  {marker} {module}")
    assert DASHBOARD_AVAILABILITY["green_decision_graph"] is True

    # --------------------------------------------------- 2. Every __all__ name resolves
    print()
    print("Verifying every name in __all__ …")
    unresolved: List[str] = []
    for _name in __all__:
        try:
            getattr(__import__(__name__), _name)
        except ImportError as exc:
            logger.debug(
                "Unresolved (expected in minimal installs): %s", exc,
            )
            unresolved.append(_name)
        except AttributeError as exc:
            print(f"  MISSING: {_name} -> {exc}")
            raise
    if unresolved:
        print(f"  {len(unresolved)} name(s) unresolved:")
        for name in unresolved:
            print(f"    - {name}")
    else:
        print("  All names resolved.")

    # --------------------------------------------------- 3. Error catch-all
    print()
    print("Error catch-all:")
    print(f"  DashboardErrorBase: {DashboardErrorBase.__name__}")
    print(
        f"  DASHBOARD_ERROR_TYPES covers "
        f"{len(DASHBOARD_ERROR_TYPES)} class(es):"
    )
    for cls in DASHBOARD_ERROR_TYPES:
        print(f"    - {cls.__name__}")

    # --------------------------------------------------- 4. Registry
    print()
    print("Registry:")
    for name, cls_name in sorted(list_dashboard_components().items()):
        print(f"  {name:<24} -> {cls_name}")

    # --------------------------------------------------- 5. Registry mutators
    class _FakeDashboard:  # noqa: D401 - test stub
        pass

    register_dashboard_component("fake", _FakeDashboard)
    assert get_dashboard_component("fake") is _FakeDashboard
    try:
        register_dashboard_component("fake", _FakeDashboard)  # idempotent
    except Exception:
        raise AssertionError("idempotent re-registration should not raise")
    try:
        register_dashboard_component("dashboard", _FakeDashboard)  # conflict
    except ValueError as exc:
        print("conflict     :", exc)
    else:
        raise AssertionError("expected conflict error")
    assert unregister_dashboard_component("fake") is True
    assert unregister_dashboard_component("fake") is False
    clear_dashboard_registry()
    assert "graph" in DASHBOARD_REGISTRY
    print("registry ops : OK")

    # --------------------------------------------------- 6. Schema versions
    versions = assert_compatible_dashboard_versions()
    if versions:
        print(f"Schema versions: {versions}")
    else:
        print("Schema versions: (none declared by any submodule)")

    # --------------------------------------------------- 7. Lazy loading via __getattr__
    try:
        getattr(
            __import__(__name__), "NotARealName",  # type: ignore[attr-defined]
        )
    except AttributeError:
        print("lazy missing : AttributeError (correct)")

    # --------------------------------------------------- 8. Graph component
    if DASHBOARD_AVAILABILITY.get("green_decision_graph"):
        graph = GreenDecisionGraph()
        from datetime import datetime, timezone
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
        assert payload.node_count > 0
        assert payload.edge_count > 0
        print("graph build  :", payload.node_count, "nodes")
    else:
        print("graph build  : skipped (module unavailable)")

    # --------------------------------------------------- 9. Dashboard aggregate
    dash = build_default_dashboard()
    print(f"dashboard    : {dash!r}")

    # --------------------------------------------------- 10. Context manager
    with build_default_dashboard() as ctx:
        if ctx.graph is not None:
            ctx.graph.build_payload(recall_bundles=[])
    print("dash ctx     : OK (closed)")

    # --------------------------------------------------- 11. Pipeline bridge (skipped if memory missing)
    if MEMORY_AVAILABLE and DASHBOARD_AVAILABILITY.get("green_decision_graph"):
        try:
            from ..memory import build_default_pipeline  # type: ignore
            with build_default_pipeline() as mp:
                with build_default_dashboard(memory_pipeline=mp) as dash2:
                    assert dash2.memory_pipeline is mp
                    payload = dashboard_payload_from_pipeline(
                        mp, query="vision", graph=dash2.graph,
                    )
                    print("pipeline     :", payload.node_count, "nodes")
        except Exception as exc:
            print("pipeline     : skipped ->", exc)
    else:
        print("pipeline     : skipped (memory or graph unavailable)")

    # --------------------------------------------------- 12. Registry exception types
    try:
        if DASHBOARD_AVAILABILITY.get("green_decision_graph"):
            raise GreenDecisionGraphInputError("boom")  # type: ignore[misc]
        else:
            raise DashboardErrorBase("boom")
    except DASHBOARD_ERROR_TYPES as exc:
        print("error tuple  : OK ->", exc)

    print("\nSmoke test passed.")
