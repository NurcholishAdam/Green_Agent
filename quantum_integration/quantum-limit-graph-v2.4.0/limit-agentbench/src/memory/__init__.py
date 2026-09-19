# src/memory/__init__.py

"""
Memory package for Green_Agent
==============================

This package provides two complementary local backends, an optional
Supermemory integration layer, a policy store, a tiered cache, a
feedback-loop guard, a benchmark harness, and the MCP bridge that
surfaces the whole pipeline.

Core backends (always present)
------------------------------
- :class:`RunMemory`         — high-level, trend-aware memory across runs.
- :class:`EpisodicMemory`    — JSON-backed ring-buffered episode store.

Supermemory integration (guarded per module)
--------------------------------------------
- :mod:`supermemory_config`  — frozen ``SupermemoryConfig``.
- :mod:`supermemory_adapter` — bridge to the Supermemory service.
- :mod:`bounded_recall`      — top-k, filtered, token-bounded recall.
- :mod:`write_governor`      — capability-based write approval + audit.
- :mod:`feedback_loop_guard` — compare recalled evidence vs. telemetry.
- :mod:`memory_benchmark`    — paired without-memory vs. with-memory.
- :mod:`memory_schemas`      — canonical event schemas.
- :mod:`memory_tier`         — hot / warm / cold storage tiering.
- :mod:`policy_memory`       — versioned, governed policy store.
- :mod:`mcp_tools`           — MCP tool bridge over the whole pipeline.

Conventions
-----------
- Thread-safe accumulators (``RLock``).
- Bounded history / ring-buffers.
- Strict / non-strict handling of corrupt files and transport errors.
- Structured serialization on every public class.
- Reentrant context-manager support for scoped sessions.
- Custom ``ValueError`` subclasses for narrow exception handling.

Availability
------------
Each submodule is imported under its own guard. If a module fails to
import (missing optional dependency, corrupted install), the package
continues to work; the affected names raise a descriptive
``ImportError`` on access via :func:`__getattr__`, and their entry in
:data:`MEMORY_AVAILABILITY` is set to ``False``.

Catching errors
---------------
Every module-defined error class derives from ``ValueError``. To catch
any memory error in one clause, use :data:`MEMORY_ERROR_TYPES`:

    try:
        ...
    except MEMORY_ERROR_TYPES:
        ...

``MemoryErrorBase`` is the conceptual base for typing and documentation.
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

#: Canonical in-memory sentinel shared across backends.
IN_MEMORY_PATH: str = ":memory:"

#: Canonical schema version stamped on every persisted record.
SCHEMA_VERSION: int = 1

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Error base
# --------------------------------------------------------------------------- #
class MemoryErrorBase(ValueError):
    """Conceptual base class for memory errors.

    Every module-defined error in this package ultimately derives from
    ``ValueError``. Because each submodule defines its own hierarchy,
    catching *every* memory error should use :data:`MEMORY_ERROR_TYPES`
    rather than this class:

        try:
            ...
        except MEMORY_ERROR_TYPES as exc:
            ...

    ``MemoryErrorBase`` is provided for typing, documentation, and
    user-defined errors that want to opt into the package's convention.
    """


# --------------------------------------------------------------------------- #
# Per-module guarded imports
# --------------------------------------------------------------------------- #
#: ``(module_name, required, exported_names)`` per submodule.
#:
#: Ordering reflects the dependency graph: schemas and configs first,
#: the outer bridge last. A module's failure does not cascade unless a
#: later module depends on it — and even then, the failure is contained
#: to the affected module's names.
_MODULE_SPECS: Tuple[Tuple[str, bool, Tuple[str, ...]], ...] = (
    (
        "memory_schemas",
        True,
        (
            "DecisionRecord", "PolicyRecord", "IncidentRecord",
            "OutcomeRecord",
            "TruthLevel", "SeverityLevel", "RecordType",
            "MemorySchemaError", "MemorySchemaInputError",
            "MemorySchemaConfigError", "MemorySchemaParseError",
            "DEFAULT_CONTAINER_TAG", "DEFAULT_TRUTH_LEVELS",
        ),
    ),
    (
        "supermemory_config",
        False,
        (
            "SupermemoryConfig", "SupermemoryConfigError", "SupermemoryMode",
        ),
    ),
    (
        "supermemory_adapter",
        False,
        (
            "SupermemoryAdapter", "SupermemoryAdapterError",
            "SupermemoryClientError", "SupermemoryWriteError",
            "SupermemoryRecallError",
        ),
    ),
    (
        "bounded_recall",
        False,
        (
            "BoundedRecall", "BoundedRecallConfig", "BoundedRecallError",
            "BoundedRecallInputError", "BoundedRecallAdapterError",
            "BoundedRecallTimeoutError", "RecallBundle",
        ),
    ),
    (
        "write_governor",
        False,
        (
            "WriteGovernor", "WriteGovernorConfig", "WriteGovernorError",
            "ApprovalDecision",
        ),
    ),
    (
        "feedback_loop_guard",
        False,
        (
            "FeedbackLoopGuard", "FeedbackLoopGuardConfig",
            "FeedbackLoopGuardError", "FeedbackLoopGuardInputError",
            "FeedbackLoopGuardConfigError", "GuardVerdict",
        ),
    ),
    (
        "memory_benchmark",
        False,
        (
            "MemoryBenchmark", "MemoryBenchmarkConfig",
            "MemoryBenchmarkError", "MemoryBenchmarkInputError",
            "MemoryBenchmarkRunnerError", "MemoryBenchmarkConfigError",
            "BenchmarkReport", "MetricStats", "TaskResult",
        ),
    ),
    (
        "episodic_memory",
        True,
        (
            "EpisodicMemory", "EpisodicMemoryError",
            "EpisodicMemoryInputError", "EpisodicMemoryFileError",
            "EpisodicMemoryCorruptionError", "EpisodeEntry",
            "MEMORY_FILE", "DEFAULT_MAX_EPISODES", "DEFAULT_RECENT_N",
            "DEFAULT_WRITE_RETRIES", "DEFAULT_TTLS",
        ),
    ),
    (
        "memory_tier",
        False,
        (
            "MemoryTier", "MemoryTierConfig", "MemoryTierManager",
            "MemoryTierPolicy", "TieredRecord",
            "MemoryTierError", "MemoryTierInputError",
            "MemoryTierConfigError", "MemoryTierParseError",
            "DEFAULT_KIND_TTLS",
        ),
    ),
    (
        "policy_memory",
        False,
        (
            "PolicyMemory", "PolicyMemoryConfig", "PolicySnapshot",
            "PolicyMemoryError", "PolicyMemoryInputError",
            "PolicyMemoryConfigError", "PolicyMemoryGovernorError",
            "PolicyMemoryAdapterError", "PolicyMemoryParseError",
            "DEFAULT_POLICY_TTL", "DEFAULT_TTL_BY_KIND",
        ),
    ),
    (
        "run_memory",
        True,
        (
            "RunMemory", "MemoryConfig", "RunSample",
            "RunMemoryError", "RunMemoryInputError",
            "RunMemoryConfigError", "RunMemoryFileError",
            "RunMemoryCorruptionError", "RunMemoryParseError",
            "DEFAULT_MEMORY_FILE", "DEFAULT_METRICS", "DEFAULT_KIND",
            "DEFAULT_HIGHER_IS_BETTER", "DEFAULT_METRIC_ALIASES",
            "DEFAULT_TTL_SECONDS",
        ),
    ),
    (
        "mcp_tools",
        False,
        (
            "MemoryMCPBridge", "MemoryMCPConfig", "MemoryMCPError",
            "MemoryMCPInputError", "MemoryMCPConfigError",
            "MemoryMCPGovernorError", "MemoryMCPAdapterError",
            "MemoryMCPToolError", "MemoryMCPUnavailableError",
            "ToolCallRecord", "RESPONSE_SCHEMA_VERSION",
        ),
    ),
)


def _import_module(module_name: str) -> Optional[Any]:
    """Import ``.{module_name}``; return None on any failure."""
    try:
        return importlib.import_module(f".{module_name}", package=__name__)
    except Exception as exc:  # noqa: BLE001 - defensive
        logger.debug(
            "Submodule %s could not be imported: %s", module_name, exc,
        )
        return None


#: Populated during the loading loop.
MEMORY_AVAILABILITY: Dict[str, bool] = {}
_LOADED_MODULES: Dict[str, Any] = {}
_NAME_TO_MODULE: Dict[str, str] = {}

#: Names that failed to resolve because their source module failed.
_UNRESOLVED: List[str] = []

for _mod_name, _required, _names in _MODULE_SPECS:
    _module = _import_module(_mod_name)
    MEMORY_AVAILABILITY[_mod_name] = _module is not None

    if _module is None:
        if _required:
            logger.error(
                "Required memory submodule %r failed to import; "
                "its names will raise ImportError on access.",
                _mod_name,
            )
        for _name in _names:
            _NAME_TO_MODULE.setdefault(_name, _mod_name)
        continue

    _LOADED_MODULES[_mod_name] = _module
    for _name in _names:
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
#: True when the ``supermemory`` SDK is importable.
SUPERMEMORY_SDK_AVAILABLE: bool = bool(
    getattr(
        _LOADED_MODULES.get("supermemory_adapter"),
        "_SUPERMEMORY_AVAILABLE",
        False,
    )
)

#: True when the MCP SDK is importable.
MCP_SDK_AVAILABLE: bool = bool(
    getattr(
        _LOADED_MODULES.get("mcp_tools"),
        "_MCP_TOOL_AVAILABLE",
        False,
    )
)


# --------------------------------------------------------------------------- #
# Error tuple
# --------------------------------------------------------------------------- #
_ERROR_BASE_NAMES: Tuple[str, ...] = (
    "RunMemoryError",
    "EpisodicMemoryError",
    "SupermemoryConfigError",
    "SupermemoryAdapterError",
    "BoundedRecallError",
    "WriteGovernorError",
    "FeedbackLoopGuardError",
    "MemoryBenchmarkError",
    "MemorySchemaError",
    "MemoryTierError",
    "PolicyMemoryError",
    "MemoryMCPError",
)

_collected_errors: List[Type[BaseException]] = []
for _err_name in _ERROR_BASE_NAMES:
    _cls = globals().get(_err_name)
    if isinstance(_cls, type) and issubclass(_cls, BaseException):
        _collected_errors.append(_cls)
del _err_name, _cls

#: Tuple of every module-level error base, safe for ``except``.
#:
#: Guaranteed to be non-empty (falls back to ``MemoryErrorBase``).
MEMORY_ERROR_TYPES: Tuple[Type[BaseException], ...] = (
    tuple(_collected_errors) if _collected_errors else (MemoryErrorBase,)
)


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
MEMORY_REGISTRY: Dict[str, Type[Any]] = {}


def _register_defaults() -> None:
    """Populate ``MEMORY_REGISTRY`` with aliases for known classes."""
    pairs: Tuple[Tuple[Tuple[str, ...], str], ...] = (
        (("run", "runs", "run_memory"), "RunMemory"),
        (("episodic", "episodes", "episodic_memory"), "EpisodicMemory"),
        (("supermemory", "supermemory_adapter"), "SupermemoryAdapter"),
        (("recall", "bounded_recall"), "BoundedRecall"),
        (("governor", "write_governor"), "WriteGovernor"),
        (
            ("guard", "feedback_loop", "feedback_loop_guard"),
            "FeedbackLoopGuard",
        ),
        (("benchmark", "memory_benchmark"), "MemoryBenchmark"),
        (("tier", "tier_manager", "memory_tier"), "MemoryTierManager"),
        (("policy", "policy_memory"), "PolicyMemory"),
        (("mcp", "mcp_bridge", "mcp_tools"), "MemoryMCPBridge"),
    )
    for aliases, cls_name in pairs:
        cls = globals().get(cls_name)
        if not isinstance(cls, type):
            continue
        for alias in aliases:
            MEMORY_REGISTRY.setdefault(alias, cls)


_register_defaults()


def register_memory(name: str, cls: Type[Any]) -> None:
    """Register a memory backend class under ``name``.

    Raises
    ------
    TypeError
        If ``name`` is not a non-empty string or ``cls`` is not a class.
    ValueError
        If ``name`` is already registered to a different class.
    """
    if not isinstance(name, str) or not name:
        raise TypeError(
            "Memory name must be a non-empty string, got "
            f"{name!r}."
        )
    if not isinstance(cls, type):
        raise TypeError(
            f"Expected a class, got {type(cls).__name__}."
        )
    existing = MEMORY_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"Memory '{name}' already registered to "
            f"{existing.__name__}."
        )
    MEMORY_REGISTRY[name] = cls
    logger.debug("Registered memory '%s' -> %s", name, cls.__name__)


def unregister_memory(name: str) -> bool:
    """Remove a registry entry. Returns True if it existed."""
    if not isinstance(name, str) or not name:
        raise TypeError(
            "Memory name must be a non-empty string."
        )
    existed = MEMORY_REGISTRY.pop(name, None) is not None
    if existed:
        logger.debug("Unregistered memory '%s'.", name)
    return existed


def clear_registry() -> None:
    """Reset the registry to its default contents.

    Intended for test isolation — restores the built-in aliases and
    drops any user-registered entries.
    """
    MEMORY_REGISTRY.clear()
    _register_defaults()
    logger.debug("Memory registry reset to defaults.")


def get_memory_class(name: str) -> Type[Any]:
    """Return the memory class registered under ``name``."""
    try:
        return MEMORY_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown memory '{name}'. "
            f"Available: {sorted(MEMORY_REGISTRY)}"
        ) from exc


def list_memories() -> Dict[str, str]:
    """Return a mapping of registered name → class name."""
    return {name: cls.__name__ for name, cls in MEMORY_REGISTRY.items()}


def list_modules() -> Dict[str, bool]:
    """Return a copy of ``MEMORY_AVAILABILITY``."""
    return dict(MEMORY_AVAILABILITY)


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #
@dataclass
class Pipeline:
    """Wired memory pipeline.

    Holds a live instance of every collaborator plus a convenience
    ``close()`` that shuts them down in reverse dependency order.
    """

    adapter: Any = None
    recall: Any = None
    governor: Any = None
    guard: Any = None
    benchmark: Any = None
    policy_memory: Any = None
    episodic: Any = None
    tier_manager: Any = None
    run_memory: Any = None
    mcp_bridge: Any = None

    # ------------------------------------------------------------------ #
    def close(self) -> None:
        """Best-effort shutdown, outer layer first."""
        order: Tuple[Any, ...] = (
            self.mcp_bridge,
            self.run_memory,
            self.tier_manager,
            self.episodic,
            self.policy_memory,
            self.benchmark,
            self.guard,
            self.recall,
            self.adapter,
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
                        "pipeline close failed for %r: %s",
                        type(component).__name__, exc,
                    )

    def __enter__(self) -> "Pipeline":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        present = [
            name for name in (
                "adapter", "recall", "governor", "guard", "benchmark",
                "policy_memory", "episodic", "tier_manager",
                "run_memory", "mcp_bridge",
            )
            if getattr(self, name, None) is not None
        ]
        return f"Pipeline(components={present})"


_PIPELINE_REQUIRED: Tuple[str, ...] = (
    "SupermemoryAdapter",
    "BoundedRecall",
    "WriteGovernor",
    "FeedbackLoopGuard",
    "MemoryBenchmark",
    "PolicyMemory",
    "EpisodicMemory",
    "MemoryTierManager",
    "RunMemory",
    "MemoryMCPBridge",
)


def build_default_pipeline(
    *,
    supermemory_config: Optional[Any] = None,
    recall_config: Optional[Any] = None,
    guard_config: Optional[Any] = None,
    benchmark_config: Optional[Any] = None,
    policy_config: Optional[Any] = None,
    tier_config: Optional[Any] = None,
    run_config: Optional[Any] = None,
    mcp_config: Optional[Any] = None,
    episodic_file: str = IN_MEMORY_PATH,
    run_file: str = IN_MEMORY_PATH,
    governor_writer_id: str = "pipeline",
    governor_capabilities: Optional[Iterable[str]] = None,
    strict: bool = True,
) -> Pipeline:
    """Build the default wired memory pipeline.

    Constructs an adapter, governor, recall, guard, benchmark, policy
    store, episodic store, tier manager, run memory, and MCP bridge,
    wiring them into one another with sensible defaults.

    Parameters
    ----------
    supermemory_config, recall_config, guard_config, benchmark_config,
    policy_config, tier_config, run_config, mcp_config : optional
        Per-collaborator config. ``None`` uses each collaborator's own
        default.
    episodic_file, run_file : str
        Paths for the two local stores. Default ``":memory:"``.
    governor_writer_id : str
        Writer id registered with the governor.
    governor_capabilities : Iterable[str], optional
        Capabilities granted to that writer. Defaults to the union of
        the pipeline's write verbs.
    strict : bool
        Propagated to every collaborator's ``strict=`` argument.

    Returns
    -------
    Pipeline

    Raises
    ------
    ImportError
        If any required collaborator module is unavailable.
    """
    missing = [
        name for name in _PIPELINE_REQUIRED
        if globals().get(name) is None
    ]
    if missing:
        raise ImportError(
            "cannot build default pipeline: missing "
            f"{sorted(missing)}. Install missing dependencies or "
            "import the submodules individually."
        )

    adapter = SupermemoryAdapter(
        config=supermemory_config,
        strict=strict,
    )
    governor = WriteGovernor()
    caps = set(governor_capabilities or {
        "write:decision", "write:policy",
        "write:outcome", "write:incident",
    })
    register = getattr(governor, "register_writer", None)
    if callable(register):
        register(governor_writer_id, capabilities=caps)

    recall = BoundedRecall(adapter, config=recall_config, strict=strict)
    guard = FeedbackLoopGuard(config=guard_config, strict=strict)
    benchmark = MemoryBenchmark(config=benchmark_config, strict=strict)
    policy_memory = PolicyMemory(
        adapter=adapter,
        governor=governor,
        config=policy_config,
        strict=strict,
    )
    episodic = EpisodicMemory(memory_file=episodic_file, strict=strict)
    tier_manager = MemoryTierManager(
        config=tier_config,
        strict=strict,
        episodic=episodic,
        adapter=adapter,
    )
    run_memory = RunMemory(
        memory_file=run_file,
        config=run_config,
        strict=False,
        episodic=episodic,
        tier_manager=tier_manager,
        policy_memory=policy_memory,
    )
    mcp_bridge = MemoryMCPBridge(
        adapter=adapter,
        recall=recall,
        governor=governor,
        guard=guard,
        benchmark=benchmark,
        policy_memory=policy_memory,
        episodic=episodic,
        tier_manager=tier_manager,
        config=mcp_config,
        strict=strict,
    )

    return Pipeline(
        adapter=adapter,
        recall=recall,
        governor=governor,
        guard=guard,
        benchmark=benchmark,
        policy_memory=policy_memory,
        episodic=episodic,
        tier_manager=tier_manager,
        run_memory=run_memory,
        mcp_bridge=mcp_bridge,
    )


# --------------------------------------------------------------------------- #
# Schema version compatibility
# --------------------------------------------------------------------------- #
def assert_compatible_schema_versions() -> Dict[str, int]:
    """Verify that every loaded submodule declares the same ``SCHEMA_VERSION``.

    Returns
    -------
    Dict[str, int]
        Mapping of module name → detected version. Empty if no module
        declares one (older installs).

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
            "incompatible schema versions across memory submodules: "
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

    - The name is declared in ``__all__`` but its source submodule
      failed to import → raise ``ImportError`` with a descriptive
      message.
    - The name is not declared anywhere → raise ``AttributeError``
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
    "IN_MEMORY_PATH",
    "SCHEMA_VERSION",
    "SUPERMEMORY_SDK_AVAILABLE",
    "MCP_SDK_AVAILABLE",

    # error base + catch-all
    "MemoryErrorBase",
    "MEMORY_ERROR_TYPES",

    # core: run memory
    "RunMemory",
    "MemoryConfig",
    "RunSample",
    "RunMemoryError",
    "RunMemoryInputError",
    "RunMemoryConfigError",
    "RunMemoryFileError",
    "RunMemoryCorruptionError",
    "RunMemoryParseError",
    "DEFAULT_MEMORY_FILE",
    "DEFAULT_METRICS",
    "DEFAULT_KIND",
    "DEFAULT_HIGHER_IS_BETTER",
    "DEFAULT_METRIC_ALIASES",
    "DEFAULT_TTL_SECONDS",

    # core: episodic memory
    "EpisodicMemory",
    "EpisodeEntry",
    "EpisodicMemoryError",
    "EpisodicMemoryInputError",
    "EpisodicMemoryFileError",
    "EpisodicMemoryCorruptionError",
    "MEMORY_FILE",
    "DEFAULT_MAX_EPISODES",
    "DEFAULT_RECENT_N",
    "DEFAULT_WRITE_RETRIES",
    "DEFAULT_TTLS",

    # supermemory config
    "SupermemoryConfig",
    "SupermemoryConfigError",
    "SupermemoryMode",

    # adapter
    "SupermemoryAdapter",
    "SupermemoryAdapterError",
    "SupermemoryClientError",
    "SupermemoryWriteError",
    "SupermemoryRecallError",

    # recall
    "BoundedRecall",
    "BoundedRecallConfig",
    "BoundedRecallError",
    "BoundedRecallInputError",
    "BoundedRecallAdapterError",
    "BoundedRecallTimeoutError",
    "RecallBundle",

    # governor
    "WriteGovernor",
    "WriteGovernorConfig",
    "WriteGovernorError",
    "ApprovalDecision",

    # guard
    "FeedbackLoopGuard",
    "FeedbackLoopGuardConfig",
    "FeedbackLoopGuardError",
    "FeedbackLoopGuardInputError",
    "FeedbackLoopGuardConfigError",
    "GuardVerdict",

    # benchmark
    "MemoryBenchmark",
    "MemoryBenchmarkConfig",
    "MemoryBenchmarkError",
    "MemoryBenchmarkInputError",
    "MemoryBenchmarkRunnerError",
    "MemoryBenchmarkConfigError",
    "BenchmarkReport",
    "MetricStats",
    "TaskResult",

    # tier
    "MemoryTier",
    "MemoryTierConfig",
    "MemoryTierManager",
    "MemoryTierPolicy",
    "TieredRecord",
    "MemoryTierError",
    "MemoryTierInputError",
    "MemoryTierConfigError",
    "MemoryTierParseError",
    "DEFAULT_KIND_TTLS",

    # policy
    "PolicyMemory",
    "PolicyMemoryConfig",
    "PolicySnapshot",
    "PolicyMemoryError",
    "PolicyMemoryInputError",
    "PolicyMemoryConfigError",
    "PolicyMemoryGovernorError",
    "PolicyMemoryAdapterError",
    "PolicyMemoryParseError",
    "DEFAULT_POLICY_TTL",
    "DEFAULT_TTL_BY_KIND",

    # MCP bridge
    "MemoryMCPBridge",
    "MemoryMCPConfig",
    "MemoryMCPError",
    "MemoryMCPInputError",
    "MemoryMCPConfigError",
    "MemoryMCPGovernorError",
    "MemoryMCPAdapterError",
    "MemoryMCPToolError",
    "MemoryMCPUnavailableError",
    "ToolCallRecord",
    "RESPONSE_SCHEMA_VERSION",

    # schemas
    "DecisionRecord",
    "PolicyRecord",
    "IncidentRecord",
    "OutcomeRecord",
    "TruthLevel",
    "SeverityLevel",
    "RecordType",
    "MemorySchemaError",
    "MemorySchemaInputError",
    "MemorySchemaConfigError",
    "MemorySchemaParseError",
    "DEFAULT_CONTAINER_TAG",
    "DEFAULT_TRUTH_LEVELS",

    # registry + pipeline
    "MEMORY_REGISTRY",
    "MEMORY_AVAILABILITY",
    "register_memory",
    "unregister_memory",
    "clear_registry",
    "get_memory_class",
    "list_memories",
    "list_modules",
    "Pipeline",
    "build_default_pipeline",
    "assert_compatible_schema_versions",
]


# --------------------------------------------------------------------------- #
# Import-time validation
# --------------------------------------------------------------------------- #
if _os.environ.get("GREEN_AGENT_VALIDATE_MEMORY") == "1":
    _missing_modules = [
        name for name, ok in MEMORY_AVAILABILITY.items() if not ok
    ]
    if _missing_modules:
        logger.warning(
            "Memory submodules unavailable at import time: %s",
            sorted(_missing_modules),
        )
    for _name, _cls in list(MEMORY_REGISTRY.items()):
        try:
            if not callable(_cls):
                raise TypeError(f"{_cls!r} is not callable.")
            logger.debug(
                "Memory component '%s' (%s) OK.",
                _name, getattr(_cls, "__name__", "?"),
            )
        except Exception:  # pragma: no cover - CI-only diagnostic
            logger.exception(
                "Memory component '%s' failed validation.", _name,
            )
            raise
    try:
        _versions = assert_compatible_schema_versions()
        if _versions:
            logger.info(
                "All memory submodules share schema version %s.",
                next(iter(set(_versions.values()))),
            )
    except RuntimeError:
        logger.exception("Schema version mismatch across memory submodules.")
        raise
    del _missing_modules, _name, _cls, _versions


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print(f"memory package v{__version__}")
    print(f"IN_MEMORY_PATH = {IN_MEMORY_PATH!r}")
    print(f"SCHEMA_VERSION = {SCHEMA_VERSION}")
    print(f"SUPERMEMORY_SDK_AVAILABLE = {SUPERMEMORY_SDK_AVAILABLE}")
    print(f"MCP_SDK_AVAILABLE = {MCP_SDK_AVAILABLE}")
    print()

    # --------------------------------------------------- 1. Module availability
    print("Module availability:")
    for module, available in sorted(MEMORY_AVAILABILITY.items()):
        marker = "✓" if available else "✗"
        print(f"  {marker} {module}")
    assert MEMORY_AVAILABILITY["run_memory"] is True
    assert MEMORY_AVAILABILITY["episodic_memory"] is True
    assert MEMORY_AVAILABILITY["memory_schemas"] is True

    # --------------------------------------------------- 2. Every __all__ name resolves
    print()
    print("Verifying every name in __all__ …")
    unresolved: List[str] = []
    for _name in __all__:
        try:
            getattr(__import__(__name__), _name)
        except ImportError as exc:
            logger.debug("Unresolved (expected in minimal installs): %s", exc)
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
    print(f"  MemoryErrorBase: {MemoryErrorBase.__name__}")
    print(f"  MEMORY_ERROR_TYPES covers {len(MEMORY_ERROR_TYPES)} class(es):")
    for cls in MEMORY_ERROR_TYPES:
        print(f"    - {cls.__name__}")

    # --------------------------------------------------- 4. Registry
    print()
    print("Registry:")
    for name, cls_name in sorted(list_memories().items()):
        print(f"  {name:<24} -> {cls_name}")

    # --------------------------------------------------- 5. Registry mutators
    class _FakeMemory:  # noqa: D401 - test stub
        pass

    register_memory("fake", _FakeMemory)
    assert get_memory_class("fake") is _FakeMemory
    try:
        register_memory("fake", _FakeMemory)  # idempotent
    except Exception:
        raise AssertionError("idempotent re-registration should not raise")
    try:
        register_memory("run", _FakeMemory)  # conflict
    except ValueError as exc:
        print("conflict     :", exc)
    else:
        raise AssertionError("expected conflict error")
    assert unregister_memory("fake") is True
    assert unregister_memory("fake") is False
    clear_registry()
    assert "run" in MEMORY_REGISTRY
    print("registry ops : OK")

    # --------------------------------------------------- 6. Schema versions
    versions = assert_compatible_schema_versions()
    if versions:
        print(f"Schema versions: {versions}")
    else:
        print("Schema versions: (none declared by any submodule)")

    # --------------------------------------------------- 7. Lazy loading via __getattr__
    try:
        getattr(__import__(__name__), "NotARealName")  # type: ignore[attr-defined]
    except AttributeError:
        print("lazy missing : AttributeError (correct)")

    # --------------------------------------------------- 8. Core backends
    from .episodic_memory import IN_MEMORY_PATH as _EPISODIC_MEM
    assert _EPISODIC_MEM == IN_MEMORY_PATH

    ep = EpisodicMemory(memory_file=IN_MEMORY_PATH, autosave=False)
    ep.store({"content": "hello"}, kind="run", run_id="r1")
    assert ep.count == 1
    print("episodic     : OK")

    rm = RunMemory(memory_file=IN_MEMORY_PATH, autosave=False)
    rid = rm.add_run({"quality": 0.8, "energy_wh": 10.0})
    assert rid.startswith("run-")
    assert len(rm) == 1
    print("run memory   : OK")

    # --------------------------------------------------- 9. Pipeline (if available)
    if all(globals().get(n) is not None for n in _PIPELINE_REQUIRED):
        with build_default_pipeline() as pipeline:
            print(f"pipeline     : {pipeline!r}")
            assert pipeline.adapter is not None
            assert pipeline.recall is not None
            assert pipeline.guard is not None
            assert pipeline.mcp_bridge is not None
        print("pipeline ctx : OK (closed)")
    else:
        print("pipeline     : skipped (some submodules unavailable)")

    # --------------------------------------------------- 10. Registry exception types
    # Sanity check that common error classes are catchable via the tuple.
    try:
        raise RunMemoryInputError("boom")  # type: ignore[misc]
    except MEMORY_ERROR_TYPES as exc:
        assert isinstance(exc, RunMemoryError)
        print("error tuple  : OK ->", exc)

    print("\nSmoke test passed.")
