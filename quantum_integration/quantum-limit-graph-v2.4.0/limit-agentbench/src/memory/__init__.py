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
continues to work; the affected names raise ``AttributeError`` on access
via :func:`__getattr__` (so ``hasattr()`` returns ``False`` as expected),
and their entry in :data:`MEMORY_AVAILABILITY` is set to ``False``. Use
:func:`require` when you want an ``ImportError`` with a descriptive
message instead.

Catching errors
---------------
Every module-defined error class derives from ``ValueError``. To catch
any memory error in one clause, use :data:`MEMORY_ERROR_TYPES`:

    try:
        ...
    except MEMORY_ERROR_TYPES:
        ...

``MemoryErrorBase`` is the conceptual base for typing and documentation.

Pipeline
--------
:func:`build_default_pipeline` wires the whole stack together and
returns a :class:`Pipeline` whose ``close(flush=True)`` method shuts
every collaborator down in reverse dependency order — forwarding the
``flush`` flag to collaborators that accept it.
"""

from __future__ import annotations

import importlib
import inspect
import logging
import os as _os
import sys as _sys
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

__version__ = "6.1.0"

#: Canonical in-memory sentinel shared across backends.
IN_MEMORY_PATH: str = ":memory:"

#: Package schema version — derived from loaded submodules after import
#: completes. The pre-load value is used only if every submodule fails.
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
    rather than this class.

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
#:
#: ``__version__`` is intentionally NOT re-exported per module; the
#: package has its own ``__version__`` at the top of this file.
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
            "SCHEMA_VERSION",
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
            "SupermemoryRecallError", "SupermemoryParseError",
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
            "WriteGovernorInputError", "WriteGovernorConfigError",
            "WriteGovernorParseError", "ApprovalDecision",
        ),
    ),
    (
        "feedback_loop_guard",
        False,
        (
            "FeedbackLoopGuard", "FeedbackLoopGuardConfig",
            "FeedbackLoopGuardError", "FeedbackLoopGuardInputError",
            "FeedbackLoopGuardConfigError", "GuardVerdict",
            "REASON_CLAMPED_K", "REASON_CODES",
            "REASON_CONTAINER_MISMATCH", "REASON_CHECK_FAILED",
            "REASON_EXCESS_EVIDENCE", "REASON_INSUFFICIENT_EVIDENCE",
            "REASON_INTERNAL_ERROR", "REASON_POLICY_DRIFT",
            "REASON_STALE_EVIDENCE", "REASON_TELEMETRY_DRIFT",
            "REASON_TRUNCATED_EVIDENCE", "REASON_UNKNOWN_STALENESS",
            "REASON_UNTRUSTED_EVIDENCE",
        ),
    ),
    (
        "memory_benchmark",
        False,
        (
            "MemoryBenchmark", "MemoryBenchmarkConfig",
            "MemoryBenchmarkError", "MemoryBenchmarkInputError",
            "MemoryBenchmarkRunnerError", "MemoryBenchmarkConfigError",
            "MemoryBenchmarkParseError",
            "BenchmarkReport", "MetricStats", "MetricDirection", "TaskResult",
        ),
    ),
    (
        "episodic_memory",
        True,
        (
            "EpisodicMemory", "EpisodicMemoryError",
            "EpisodicMemoryInputError", "EpisodicMemoryFileError",
            "EpisodicMemoryCorruptionError", "EpisodeEntry",
            "MEMORY_FILE", "IN_MEMORY_PATH",
            "DEFAULT_MAX_EPISODES", "DEFAULT_RECENT_N",
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
            "DEFAULT_KIND_TTLS", "DEFAULT_TIER_TTLS",
            "DEFAULT_HOT_TTL_SECONDS", "DEFAULT_WARM_TTL_SECONDS",
            "DEFAULT_COLD_TTL_SECONDS",
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
            "MCP_SDK_AVAILABLE",
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

#: Names already present at the package level (SCHEMA_VERSION, etc.).
_PACKAGE_RESERVED: Tuple[str, ...] = (
    "SCHEMA_VERSION", "IN_MEMORY_PATH", "__version__",
    "MemoryErrorBase", "MEMORY_ERROR_TYPES", "MEMORY_REGISTRY",
    "MEMORY_AVAILABILITY",
)

for _mod_name, _required, _names in _MODULE_SPECS:
    _module = _import_module(_mod_name)
    MEMORY_AVAILABILITY[_mod_name] = _module is not None

    if _module is None:
        if _required:
            logger.error(
                "Required memory submodule %r failed to import; "
                "accessing its names raises AttributeError.",
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
        # First writer wins for shared constants; package-reserved names
        # are never overwritten.
        if _name in _PACKAGE_RESERVED:
            _NAME_TO_MODULE.setdefault(_name, _mod_name)
            continue
        globals().setdefault(_name, _value)
        _NAME_TO_MODULE.setdefault(_name, _mod_name)

del _mod_name, _required, _module, _names, _name, _value


# --------------------------------------------------------------------------- #
# Derived package SCHEMA_VERSION
# --------------------------------------------------------------------------- #
_detected_schema_versions: Dict[str, int] = {}
for _mod_name, _mod in _LOADED_MODULES.items():
    _v = getattr(_mod, "SCHEMA_VERSION", None)
    if isinstance(_v, int) and not isinstance(_v, bool) and _v > 0:
        _detected_schema_versions[_mod_name] = _v
if _detected_schema_versions:
    SCHEMA_VERSION = max(_detected_schema_versions.values())
del _mod_name, _mod, _v


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

#: True when the MCP SDK is importable. Prefers the public name first,
#: falls back to the legacy private alias.
MCP_SDK_AVAILABLE: bool = bool(
    getattr(_LOADED_MODULES.get("mcp_tools"), "MCP_SDK_AVAILABLE", None)
    or getattr(_LOADED_MODULES.get("mcp_tools"), "_MCP_TOOL_AVAILABLE", False)
)


# --------------------------------------------------------------------------- #
# Error tuple + subclasses
# --------------------------------------------------------------------------- #
_ERROR_BASE_NAMES: Tuple[str, ...] = (
    "RunMemoryError",
    "EpisodicMemoryError",
    "SupermemoryConfigError",
    "SupermemoryAdapterError",
    "SupermemoryParseError",
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
MEMORY_ERROR_TYPES: Tuple[Type[BaseException], ...] = (
    tuple(_collected_errors) if _collected_errors else (MemoryErrorBase,)
)

#: Error subclasses (input / config / parse / file / corruption / etc.).
#: Provided separately so callers can dispatch on "shape of failure"
#: without walking the class hierarchy themselves.
_ERROR_SUBCLASS_NAMES: Tuple[str, ...] = (
    "RunMemoryInputError", "RunMemoryConfigError",
    "RunMemoryFileError", "RunMemoryCorruptionError",
    "RunMemoryParseError",
    "EpisodicMemoryInputError", "EpisodicMemoryFileError",
    "EpisodicMemoryCorruptionError",
    "SupermemoryClientError", "SupermemoryWriteError",
    "SupermemoryRecallError",
    "BoundedRecallInputError", "BoundedRecallAdapterError",
    "BoundedRecallTimeoutError",
    "WriteGovernorInputError", "WriteGovernorConfigError",
    "WriteGovernorParseError",
    "FeedbackLoopGuardInputError", "FeedbackLoopGuardConfigError",
    "MemoryBenchmarkInputError", "MemoryBenchmarkRunnerError",
    "MemoryBenchmarkConfigError", "MemoryBenchmarkParseError",
    "MemorySchemaInputError", "MemorySchemaConfigError",
    "MemorySchemaParseError",
    "MemoryTierInputError", "MemoryTierConfigError",
    "MemoryTierParseError",
    "PolicyMemoryInputError", "PolicyMemoryConfigError",
    "PolicyMemoryGovernorError", "PolicyMemoryAdapterError",
    "PolicyMemoryParseError",
    "MemoryMCPInputError", "MemoryMCPConfigError",
    "MemoryMCPGovernorError", "MemoryMCPAdapterError",
    "MemoryMCPToolError", "MemoryMCPUnavailableError",
)

_collected_subclasses: List[Type[BaseException]] = []
for _err_name in _ERROR_SUBCLASS_NAMES:
    _cls = globals().get(_err_name)
    if isinstance(_cls, type) and issubclass(_cls, BaseException):
        _collected_subclasses.append(_cls)
del _err_name, _cls

#: Tuple of every module-level error subclass.
MEMORY_ERROR_SUBCLASSES: Tuple[Type[BaseException], ...] = tuple(
    _collected_subclasses
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
        (("config", "memory_config"), "MemoryConfig"),
        (("tier_config", "memory_tier_config"), "MemoryTierConfig"),
        (("policy_config", "policy_memory_config"), "PolicyMemoryConfig"),
        (("mcp_config",), "MemoryMCPConfig"),
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
    """Reset the registry to its default contents."""
    MEMORY_REGISTRY.clear()
    _register_defaults()
    logger.debug("Memory registry reset to defaults.")


def get_memory_class(name: str) -> Type[Any]:
    """Return the memory class registered under ``name``."""
    if not isinstance(name, str) or not name:
        raise TypeError("Memory name must be a non-empty string.")
    try:
        return MEMORY_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown memory '{name}'. "
            f"Available: {sorted(MEMORY_REGISTRY)}"
        ) from exc


def get_memory_instance(name: str, **kwargs: Any) -> Any:
    """Instantiate the memory class registered under ``name``.

    ``kwargs`` are passed verbatim to the constructor — the caller is
    responsible for matching the collaborator's signature.
    """
    cls = get_memory_class(name)
    return cls(**kwargs)


def list_memories() -> Dict[str, str]:
    """Return a mapping of registered name → class name."""
    return {name: cls.__name__ for name, cls in MEMORY_REGISTRY.items()}


def list_modules() -> Dict[str, bool]:
    """Return a copy of ``MEMORY_AVAILABILITY``."""
    return dict(MEMORY_AVAILABILITY)


def list_components() -> Dict[str, Dict[str, Any]]:
    """Return a per-submodule summary of what loaded."""
    out: Dict[str, Dict[str, Any]] = {}
    for name, ok in MEMORY_AVAILABILITY.items():
        mod = _LOADED_MODULES.get(name)
        entry: Dict[str, Any] = {"available": ok}
        if mod is not None:
            entry["version"] = getattr(mod, "__version__", None)
            entry["schema_version"] = getattr(mod, "SCHEMA_VERSION", None)
        out[name] = entry
    return out


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #
_COMPONENT_ORDER: Tuple[str, ...] = (
    "adapter", "recall", "governor", "guard", "benchmark",
    "policy_memory", "episodic", "tier_manager", "run_memory", "mcp_bridge",
)

#: Close order = reverse construction (outer layers first).
_CLOSE_ORDER: Tuple[str, ...] = (
    "mcp_bridge", "run_memory", "tier_manager", "episodic",
    "policy_memory", "benchmark", "guard", "recall", "adapter", "governor",
)


@dataclass
class Pipeline:
    """Wired memory pipeline.

    Holds a live instance of every collaborator plus:
    - ``close(flush=True)`` — shut down in reverse dependency order.
    - ``statistics()`` / ``snapshot()`` / ``reset()`` — aggregated views.
    - ``health()`` — a lightweight readiness probe.
    - ``components()`` — dict of name → collaborator (skips ``None``).
    - Sync + async context-manager protocols.
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
    def components(self) -> Dict[str, Any]:
        """Return a dict of present components (name → instance)."""
        return {
            name: getattr(self, name, None)
            for name in _COMPONENT_ORDER
            if getattr(self, name, None) is not None
        }

    def missing_components(self) -> List[str]:
        """Return the names of collaborators that are not wired."""
        return [
            name for name in _COMPONENT_ORDER
            if getattr(self, name, None) is None
        ]

    # ------------------------------------------------------------------ #
    def _call_close(self, component: Any, *, flush: bool) -> None:
        """Call ``component.close()``, forwarding ``flush`` if accepted."""
        close = getattr(component, "close", None)
        if not callable(close):
            return
        try:
            sig = inspect.signature(close)
            accepts_flush = "flush" in sig.parameters
        except (TypeError, ValueError):
            accepts_flush = False
        try:
            if accepts_flush:
                close(flush=flush)
            else:
                close()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "pipeline close failed for %r: %s",
                type(component).__name__, exc,
            )

    def close(self, *, flush: bool = True) -> None:
        """Best-effort shutdown, outer layer first.

        Parameters
        ----------
        flush : bool
            Forwarded as ``flush=`` to any collaborator whose ``close()``
            accepts the keyword. Defaults to True so a pipeline built
            with ``autosave=False`` still persists its state on shutdown.
        """
        for name in _CLOSE_ORDER:
            component = getattr(self, name, None)
            if component is None:
                continue
            self._call_close(component, flush=flush)

    def __enter__(self) -> "Pipeline":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    async def __aenter__(self) -> "Pipeline":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    def statistics(self) -> Dict[str, Any]:
        """Return aggregated statistics across every collaborator."""
        out: Dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "components": {},
            "missing": self.missing_components(),
        }
        for name in _COMPONENT_ORDER:
            component = getattr(self, name, None)
            if component is None:
                continue
            fn = getattr(component, "statistics", None)
            if not callable(fn):
                continue
            try:
                out["components"][name] = fn()
            except Exception as exc:  # pragma: no cover - defensive
                out["components"][name] = {
                    "error": f"{type(exc).__name__}: {exc}",
                }
        return out

    def snapshot(self) -> Dict[str, Any]:
        """Return a structured plain-dict snapshot of every collaborator."""
        out: Dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "components": {},
            "statistics": self.statistics(),
        }
        for name in _COMPONENT_ORDER:
            component = getattr(self, name, None)
            if component is None:
                continue
            fn = getattr(component, "snapshot", None)
            if not callable(fn):
                continue
            try:
                out["components"][name] = fn()
            except Exception as exc:  # pragma: no cover - defensive
                out["components"][name] = {
                    "error": f"{type(exc).__name__}: {exc}",
                }
        return out

    def reset(self, *, recursive: bool = True) -> Dict[str, int]:
        """Reset counters on every collaborator that supports it.

        Returns a mapping of component name → cleared count (or -1 for
        collaborators whose ``reset()`` doesn't return an int).
        """
        out: Dict[str, int] = {}
        if not recursive:
            return out
        for name in _COMPONENT_ORDER:
            component = getattr(self, name, None)
            if component is None:
                continue
            fn = getattr(component, "reset", None)
            if not callable(fn):
                continue
            try:
                result = fn()
                out[name] = int(result) if isinstance(result, int) else -1
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning(
                    "pipeline reset failed for %s: %s", name, exc,
                )
                out[name] = -1
        return out

    def health(self) -> Dict[str, Any]:
        """Lightweight readiness probe.

        Returns ``{"healthy": bool, "missing": [...], "client_available":
        bool | None}``. ``client_available`` is populated when an adapter
        is present.
        """
        missing = self.missing_components()
        adapter = self.adapter
        client_available: Optional[bool] = None
        if adapter is not None:
            try:
                client_available = bool(
                    getattr(adapter, "client_available", None)
                )
            except Exception:  # pragma: no cover - defensive
                client_available = None
        return {
            "healthy": not missing,
            "missing": missing,
            "client_available": client_available,
        }

    # ------------------------------------------------------------------ #
    @classmethod
    def from_config(
        cls,
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
        require_all: bool = True,
    ) -> "Pipeline":
        """Alias for :func:`build_default_pipeline`."""
        return build_default_pipeline(
            supermemory_config=supermemory_config,
            recall_config=recall_config,
            guard_config=guard_config,
            benchmark_config=benchmark_config,
            policy_config=policy_config,
            tier_config=tier_config,
            run_config=run_config,
            mcp_config=mcp_config,
            episodic_file=episodic_file,
            run_file=run_file,
            governor_writer_id=governor_writer_id,
            governor_capabilities=governor_capabilities,
            strict=strict,
            require_all=require_all,
        )

    @classmethod
    def from_components(
        cls,
        *,
        adapter: Any = None,
        recall: Any = None,
        governor: Any = None,
        guard: Any = None,
        benchmark: Any = None,
        policy_memory: Any = None,
        episodic: Any = None,
        tier_manager: Any = None,
        run_memory: Any = None,
        mcp_bridge: Any = None,
    ) -> "Pipeline":
        """Build a pipeline from explicit collaborators."""
        return cls(
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

    def __repr__(self) -> str:
        present = [
            name for name in _COMPONENT_ORDER
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
    require_all: bool = True,
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
    require_all : bool
        If True (default) a missing collaborator module raises
        ``ImportError``. If False, missing collaborators are left as
        ``None`` in the returned ``Pipeline``.

    Raises
    ------
    ImportError
        If ``require_all=True`` and any required collaborator module is
        unavailable.
    """
    missing = [
        name for name in _PIPELINE_REQUIRED
        if globals().get(name) is None
    ]
    if missing and require_all:
        raise ImportError(
            "cannot build default pipeline: missing "
            f"{sorted(missing)}. Install missing dependencies, "
            "import the submodules individually, or pass "
            "require_all=False to build a degraded pipeline."
        )

    # Adapter / governor always attempted when their classes are present.
    adapter: Any = None
    if SupermemoryAdapter is not None:      # noqa: F821 - guarded import
        adapter = SupermemoryAdapter(
            config=supermemory_config,
            strict=strict,
        )

    governor: Any = None
    if WriteGovernor is not None:           # noqa: F821 - guarded import
        governor = WriteGovernor()
        caps = set(governor_capabilities or {
            "write:decision", "write:policy",
            "write:outcome", "write:incident",
        })
        register = getattr(governor, "register_writer", None)
        if callable(register):
            register(governor_writer_id, capabilities=caps)

    recall: Any = None
    if (
        BoundedRecall is not None            # noqa: F821 - guarded import
        and adapter is not None
    ):
        recall = BoundedRecall(
            adapter, config=recall_config, strict=strict,
        )

    guard: Any = None
    if FeedbackLoopGuard is not None:       # noqa: F821 - guarded import
        guard = FeedbackLoopGuard(config=guard_config, strict=strict)

    benchmark: Any = None
    if MemoryBenchmark is not None:         # noqa: F821 - guarded import
        benchmark = MemoryBenchmark(
            config=benchmark_config, strict=strict,
        )

    policy_memory: Any = None
    if (
        PolicyMemory is not None             # noqa: F821 - guarded import
        and adapter is not None
        and governor is not None
    ):
        policy_memory = PolicyMemory(
            adapter=adapter,
            governor=governor,
            config=policy_config,
            strict=strict,
        )

    episodic: Any = None
    if EpisodicMemory is not None:          # noqa: F821 - guarded import
        episodic = EpisodicMemory(
            memory_file=episodic_file, strict=strict,
        )

    tier_manager: Any = None
    if MemoryTierManager is not None:       # noqa: F821 - guarded import
        tier_manager = MemoryTierManager(
            config=tier_config,
            strict=strict,
            episodic=episodic,
            adapter=adapter,
        )

    run_memory: Any = None
    if RunMemory is not None:               # noqa: F821 - guarded import
        run_memory = RunMemory(
            memory_file=run_file,
            config=run_config,
            strict=strict,
            episodic=episodic,
            tier_manager=tier_manager,
            policy_memory=policy_memory,
        )

    mcp_bridge: Any = None
    if (
        MemoryMCPBridge is not None          # noqa: F821 - guarded import
        and adapter is not None
        and recall is not None
    ):
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
# Schema / vocabulary compatibility
# --------------------------------------------------------------------------- #
def assert_compatible_schema_versions() -> Dict[str, int]:
    """Verify that every loaded submodule declares the same ``SCHEMA_VERSION``.

    Also checks ``RESPONSE_SCHEMA_VERSION`` when the string form is
    present (only ``mcp_tools`` currently exposes it).

    Returns
    -------
    Dict[str, int]
        Mapping of module name → detected integer version.

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

    # String response-envelope version check (informational).
    response_versions: Dict[str, str] = {}
    for module_name, module in _LOADED_MODULES.items():
        value = getattr(module, "RESPONSE_SCHEMA_VERSION", None)
        if isinstance(value, str) and value:
            response_versions[module_name] = value
    if len(set(response_versions.values())) > 1:
        logger.warning(
            "response schema versions differ across modules: %s",
            response_versions,
        )
    return versions


#: Shared vocabularies that must agree across submodules.
_SHARED_VOCABULARIES: Tuple[str, ...] = (
    "DEFAULT_TRUTH_LEVELS",
    "DEFAULT_CONTAINER_TAG",
    "DEFAULT_TTL_SECONDS",
    "DEFAULT_KIND_TTLS",
)


def assert_compatible_vocabularies() -> Dict[str, Dict[str, Any]]:
    """Verify that shared vocabularies agree across loaded submodules.

    Checks:
    - ``DEFAULT_TRUTH_LEVELS`` — must be identical sets everywhere it's
      defined.
    - ``DEFAULT_CONTAINER_TAG`` — must be a single string.
    - ``DEFAULT_TTL_SECONDS`` / ``DEFAULT_KIND_TTLS`` — must agree on
      shared keys when both are present.

    Returns
    -------
    Dict[str, Dict[str, Any]]
        Mapping of vocabulary name → {module: value}.

    Raises
    ------
    RuntimeError
        If vocabularies disagree.
    """
    collected: Dict[str, Dict[str, Any]] = {}

    for vocab in _SHARED_VOCABULARIES:
        per_module: Dict[str, Any] = {}
        for module_name, module in _LOADED_MODULES.items():
            value = getattr(module, vocab, None)
            if value is None:
                continue
            # Normalize mappings to sorted tuples for comparison.
            if isinstance(value, dict):
                per_module[module_name] = tuple(sorted(value.items()))
            else:
                per_module[module_name] = value
        if per_module:
            collected[vocab] = per_module

    # Truth levels: compare as sets.
    truth = collected.get("DEFAULT_TRUTH_LEVELS", {})
    truth_sets = {frozenset(v) for v in truth.values()}
    if len(truth_sets) > 1:
        raise RuntimeError(
            "DEFAULT_TRUTH_LEVELS disagree across modules: "
            f"{ {k: sorted(v) for k, v in truth.items()} }"
        )

    # Container tag: compare as scalars.
    tags = collected.get("DEFAULT_CONTAINER_TAG", {})
    tag_values = set(tags.values())
    if len(tag_values) > 1:
        raise RuntimeError(
            f"DEFAULT_CONTAINER_TAG disagree across modules: {tags}"
        )

    # TTL vocabularies: compare the shared key set.
    ttl_vocabs = (
        collected.get("DEFAULT_TTL_SECONDS", {}),
        collected.get("DEFAULT_KIND_TTLS", {}),
    )
    if all(ttl_vocabs):
        ttl_key_sets = [
            frozenset(dict(v).keys()) for v in ttl_vocabs[0].values()
        ] + [
            frozenset(dict(v).keys()) for v in ttl_vocabs[1].values()
        ]
        if len(set(ttl_key_sets)) > 1:
            logger.warning(
                "TTL vocabularies differ across modules: %s",
                ttl_key_sets,
            )
    return collected


# --------------------------------------------------------------------------- #
# Lazy loading + helpers
# --------------------------------------------------------------------------- #
def __getattr__(name: str) -> Any:
    """Raise helpful errors for names declared but not resolved.

    Called only when ``name`` is absent from the module's globals.
    Raises ``AttributeError`` (never ``ImportError``) so ``hasattr()``
    and ``getattr(obj, name, default)`` behave as expected:

    - The name is declared in ``__all__`` but its source submodule
      failed to import → ``AttributeError`` with a descriptive message.
    - The name is not declared anywhere → plain ``AttributeError``.

    For a version that raises ``ImportError``, use :func:`require`.
    """
    if name in _NAME_TO_MODULE:
        source = _NAME_TO_MODULE[name]
        raise AttributeError(
            f"{__name__}.{name} is unavailable: the {source!r} submodule "
            f"failed to import. Check MEMORY_AVAILABILITY[{source!r}], or "
            f"use memory.require({name!r}) to raise ImportError instead."
        )
    raise AttributeError(
        f"module {__name__!r} has no attribute {name!r}"
    )


def require(name: str) -> Any:
    """Return a declared name, or raise ``ImportError`` with context.

    Intended for callers who want "fail-fast with a clear message"
    semantics rather than ``hasattr()``-friendly behaviour.
    """
    if not isinstance(name, str) or not name:
        raise ImportError("require() expects a non-empty name.")
    try:
        return getattr(_sys.modules[__name__], name)
    except AttributeError as exc:
        raise ImportError(str(exc)) from exc


def describe() -> Dict[str, Any]:
    """Return a human-readable summary of the package's state."""
    return {
        "version": __version__,
        "schema_version": SCHEMA_VERSION,
        "supermemory_sdk_available": SUPERMEMORY_SDK_AVAILABLE,
        "mcp_sdk_available": MCP_SDK_AVAILABLE,
        "modules": list_components(),
        "registry_size": len(MEMORY_REGISTRY),
        "error_types": [c.__name__ for c in MEMORY_ERROR_TYPES],
        "unresolved": list(_UNRESOLVED),
    }


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

    # error base + catch-alls
    "MemoryErrorBase",
    "MEMORY_ERROR_TYPES",
    "MEMORY_ERROR_SUBCLASSES",

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
    "SupermemoryParseError",

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
    "WriteGovernorInputError",
    "WriteGovernorConfigError",
    "WriteGovernorParseError",
    "ApprovalDecision",

    # guard
    "FeedbackLoopGuard",
    "FeedbackLoopGuardConfig",
    "FeedbackLoopGuardError",
    "FeedbackLoopGuardInputError",
    "FeedbackLoopGuardConfigError",
    "GuardVerdict",
    "REASON_CLAMPED_K", "REASON_CODES",
    "REASON_CONTAINER_MISMATCH", "REASON_CHECK_FAILED",
    "REASON_EXCESS_EVIDENCE", "REASON_INSUFFICIENT_EVIDENCE",
    "REASON_INTERNAL_ERROR", "REASON_POLICY_DRIFT",
    "REASON_STALE_EVIDENCE", "REASON_TELEMETRY_DRIFT",
    "REASON_TRUNCATED_EVIDENCE", "REASON_UNKNOWN_STALENESS",
    "REASON_UNTRUSTED_EVIDENCE",

    # benchmark
    "MemoryBenchmark",
    "MemoryBenchmarkConfig",
    "MemoryBenchmarkError",
    "MemoryBenchmarkInputError",
    "MemoryBenchmarkRunnerError",
    "MemoryBenchmarkConfigError",
    "MemoryBenchmarkParseError",
    "BenchmarkReport",
    "MetricDirection",
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
    "DEFAULT_TIER_TTLS",
    "DEFAULT_HOT_TTL_SECONDS",
    "DEFAULT_WARM_TTL_SECONDS",
    "DEFAULT_COLD_TTL_SECONDS",

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

    # registry + pipeline + helpers
    "MEMORY_REGISTRY",
    "MEMORY_AVAILABILITY",
    "register_memory",
    "unregister_memory",
    "clear_registry",
    "get_memory_class",
    "get_memory_instance",
    "list_memories",
    "list_modules",
    "list_components",
    "Pipeline",
    "build_default_pipeline",
    "assert_compatible_schema_versions",
    "assert_compatible_vocabularies",
    "require",
    "describe",
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
    try:
        _vocabs = assert_compatible_vocabularies()
        if _vocabs:
            logger.info(
                "Shared vocabularies agree across memory submodules."
            )
    except RuntimeError:
        logger.exception("Vocabulary mismatch across memory submodules.")
        raise
    del _missing_modules, _name, _cls, _versions, _vocabs


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
            require(_name)
        except ImportError as exc:
            logger.debug("Unresolved (expected in minimal installs): %s", exc)
            unresolved.append(_name)
    if unresolved:
        print(f"  {len(unresolved)} name(s) unresolved:")
        for name in unresolved:
            print(f"    - {name}")
    else:
        print("  All names resolved.")

    # --------------------------------------------------- 3. Error catch-alls
    print()
    print("Error catch-alls:")
    print(f"  MemoryErrorBase: {MemoryErrorBase.__name__}")
    print(f"  MEMORY_ERROR_TYPES covers {len(MEMORY_ERROR_TYPES)} class(es)")
    print(f"  MEMORY_ERROR_SUBCLASSES covers "
          f"{len(MEMORY_ERROR_SUBCLASSES)} class(es)")

    # --------------------------------------------------- 4. hasattr() now works
    print()
    print("hasattr() semantics:")
    # Names that resolved should be present.
    assert hasattr(_sys.modules[__name__], "RunMemory") is True
    # Names that failed to resolve should not raise.
    assert hasattr(_sys.modules[__name__], "NotARealName") is False
    # Every declared name should have a stable hasattr() answer.
    for _name in __all__:
        _ = hasattr(_sys.modules[__name__], _name)
    print("  hasattr() is safe: OK")

    # --------------------------------------------------- 5. require() raises ImportError
    try:
        require("NotARealName")
    except ImportError as exc:
        print(f"  require(bad): ImportError -> {exc}")
    else:
        raise AssertionError("expected ImportError")

    # --------------------------------------------------- 6. Registry
    print()
    print("Registry (sample):")
    for name, cls_name in sorted(list_memories().items())[:6]:
        print(f"  {name:<24} -> {cls_name}")

    # --------------------------------------------------- 7. Registry mutators + factory
    class _FakeMemory:  # noqa: D401 - test stub
        def __init__(self, *, tag: str = "x") -> None:
            self.tag = tag

    register_memory("fake", _FakeMemory)
    assert get_memory_class("fake") is _FakeMemory
    inst = get_memory_instance("fake", tag="hello")
    assert isinstance(inst, _FakeMemory) and inst.tag == "hello"
    try:
        register_memory("fake", _FakeMemory)  # idempotent
    except Exception:
        raise AssertionError("idempotent re-registration should not raise")
    try:
        register_memory("run", _FakeMemory)  # conflict
    except ValueError as exc:
        print("  conflict     :", exc)
    else:
        raise AssertionError("expected conflict error")
    assert unregister_memory("fake") is True
    assert unregister_memory("fake") is False
    clear_registry()
    assert "run" in MEMORY_REGISTRY
    print("  registry ops : OK")

    # --------------------------------------------------- 8. Schema / vocab checks
    versions = assert_compatible_schema_versions()
    if versions:
        print(f"Schema versions: {versions}")
    else:
        print("Schema versions: (none declared by any submodule)")

    vocabs = assert_compatible_vocabularies()
    print(f"Shared vocabularies checked: {sorted(vocabs.keys())}")

    # --------------------------------------------------- 9. describe / list_components
    desc = describe()
    assert "modules" in desc and "version" in desc
    print(f"describe(): {sorted(desc.keys())}")
    comps = list_components()
    assert "run_memory" in comps
    print(f"list_components(): {len(comps)} modules")

    # --------------------------------------------------- 10. Core backends
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

    # --------------------------------------------------- 11. Pipeline (if available)
    if all(globals().get(n) is not None for n in _PIPELINE_REQUIRED):
        with build_default_pipeline(strict=False) as pipeline:
            print(f"pipeline     : {pipeline!r}")
            assert pipeline.adapter is not None
            assert pipeline.recall is not None
            assert pipeline.guard is not None
            assert pipeline.mcp_bridge is not None
            # health check
            health = pipeline.health()
            assert "healthy" in health
            print(f"pipeline health: {health}")
            # aggregated statistics
            stats = pipeline.statistics()
            assert stats["schema_version"] == SCHEMA_VERSION
            assert "components" in stats
            assert "adapter" in stats["components"]
            print(f"pipeline stats : keys={sorted(stats['components'].keys())}")
            # snapshot
            snap = pipeline.snapshot()
            assert "statistics" in snap
            print("pipeline snap  : OK")
            # reset(recursive=True)
            cleared = pipeline.reset(recursive=True)
            assert isinstance(cleared, dict)
            print(f"pipeline reset : {sorted(cleared.keys())}")
        print("pipeline ctx : OK (closed, flushed)")

        # from_components
        p2 = Pipeline.from_components(adapter=pipeline.adapter)
        assert p2.adapter is pipeline.adapter
        assert p2.recall is None
        assert p2.missing_components()
        print("Pipeline.from_components : OK")

        # from_config
        with Pipeline.from_config(strict=False) as p3:
            assert p3.adapter is not None
        print("Pipeline.from_config    : OK")
    else:
        print("pipeline     : skipped (some submodules unavailable)")

    # --------------------------------------------------- 12. require_all=False degradation
    # Simulate: don't require everything.
    with build_default_pipeline(
        strict=False, require_all=False,
    ) as degraded:
        # In a full install this is complete; in a partial install the
        # pipeline still constructs, with missing components as None.
        assert isinstance(degraded.missing_components(), list)
        print(f"degraded pipeline: {degraded!r}")

    # --------------------------------------------------- 13. Error tuple dispatch
    try:
        raise RunMemoryInputError("boom")  # type: ignore[misc]
    except MEMORY_ERROR_TYPES as exc:
        assert isinstance(exc, RunMemoryError)
        print("error tuple  : OK ->", exc)

    # MemoryErrorBase is the conceptual base.
    assert issubclass(RunMemoryError, MemoryErrorBase) is False  # sanity
    assert issubclass(MemoryErrorBase, ValueError) is True
    print("MemoryErrorBase: OK")

    # --------------------------------------------------- 14. Lazy loading via __getattr__
    try:
        getattr(_sys.modules[__name__], "NotARealName")
    except AttributeError:
        print("lazy missing : AttributeError (correct)")

    # --------------------------------------------------- 15. async context manager
    import asyncio as _asyncio

    async def _async_ctx():
        async with build_default_pipeline(strict=False) as p:
            assert p.adapter is not None
            return len(p.components())

    _n = _asyncio.run(_async_ctx())
    assert _n >= 1
    print("async ctx    : OK ->", _n, "components")

    print("\nSmoke test passed.")
