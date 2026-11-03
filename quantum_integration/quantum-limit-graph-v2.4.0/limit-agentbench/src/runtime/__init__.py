# src/runtime/__init__.py

"""
Runtime execution modules for different agent frameworks.

This package provides framework-specific runtime adapters that simulate
agent execution to measure accuracy, tool-call counts, conversation depth,
and related resource metrics. Each runtime exposes a uniform interface
(:class:`RuntimeProtocol`):

- ``init(config, ...)``    — configure the runtime.
- ``run(query)``           — simulate execution, return metrics.
- ``reduce_tool_calls()``  — reduce tool-call intensity (throttling).
- ``shorten_context()``    — trim the context window (token reduction).
- ``finalize()``           — release resources / finalize the session.

Enhancements
------------
- Registry pattern (``RUNTIME_REGISTRY`` + ``register_runtime_component`` /
  ``get_runtime_component`` / ``list_runtime_components``) consistent with
  the rest of the upgraded ``src/`` tree.
- ``RuntimeProtocol`` — structural check for runtime adapters; the
  registry now rejects objects that do not implement the interface.
- Read-only public views: ``RUNTIME_REGISTRY`` and ``RUNTIME_AVAILABILITY``
  are ``MappingProxyType``; internal dicts are private.
- ``_MissingComponent`` stub — missing sub-modules raise a clear
  ``ImportError`` on first use instead of returning ``None``.
- Polymorphic helpers: ``create_runtime(name, ...)`` and
  ``runtime_from_dict(payload)``.
- ``RuntimeRegistryError(ValueError)`` — narrow error type for registry
  operations.
- ``health()`` — aggregate availability + registry summary for dashboards
  and bug reports.
- ``_LOCK`` guards registration and lookup for concurrent plugin loading.
- ``__dir__`` for clean REPL discoverability.
- Optional import-time validation via ``GREEN_AGENT_VALIDATE_RUNTIME=1``
  (now actually meaningful — checks protocol conformance, not just
  ``callable``).
- Re-exports the new ``RunMetrics`` / ``RuntimeMetrics`` TypedDicts from
  both sub-modules.
"""

from __future__ import annotations

import importlib
import inspect
import logging
import os
import threading
from types import MappingProxyType
from typing import (
    Any, Callable, Dict, List, Mapping, Optional, Protocol, Tuple, Type,
    TypeVar, Union, runtime_checkable,
)

logger = logging.getLogger(__name__)

__version__ = "2.4.0"

_T = TypeVar("_T")


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class RuntimeRegistryError(ValueError):
    """Raised for invalid runtime registry operations."""


# --------------------------------------------------------------------------- #
# Runtime contract
# --------------------------------------------------------------------------- #
@runtime_checkable
class RuntimeProtocol(Protocol):
    """
    Structural interface implemented by every runtime adapter.

    Both :class:`AutoGenRuntime` and :class:`LangChainRuntime` conform.
    Third-party adapters registered via :func:`register_runtime_component`
    are validated against this protocol (method presence only).
    """

    def init(self, config: Any, **kwargs: Any) -> None: ...
    def run(self, query: Any) -> Mapping[str, Any]: ...
    def reduce_tool_calls(self) -> int: ...
    def shorten_context(self) -> int: ...
    def finalize(self) -> Dict[str, Any]: ...


# --------------------------------------------------------------------------- #
# Missing-component stub
# --------------------------------------------------------------------------- #
class _MissingComponent:
    """
    Placeholder for an optional component that failed to import.

    Raises :class:`ImportError` on first use instead of silently
    returning ``None``, so the caller sees the real cause.
    """

    def __init__(self, name: str, reason: BaseException) -> None:
        self._name = name
        self._reason = reason

    def _fail(self) -> None:
        raise ImportError(
            f"Runtime component '{self._name}' is unavailable: {self._reason}"
        ) from self._reason

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self._fail()

    def __getattr__(self, item: str) -> Any:
        self._fail()

    def __repr__(self) -> str:
        return f"<unavailable runtime component {self._name!r}>"


# --------------------------------------------------------------------------- #
# Availability manifest (read-only public view)
# --------------------------------------------------------------------------- #
_availability: Dict[str, bool] = {}
RUNTIME_AVAILABILITY: Mapping[str, bool] = MappingProxyType(_availability)


# --------------------------------------------------------------------------- #
# Registry (read-only public view)
# --------------------------------------------------------------------------- #
_registry: Dict[str, Union[Type[Any], _MissingComponent]] = {}
RUNTIME_REGISTRY: Mapping[str, Any] = MappingProxyType(_registry)
_registry_lock = threading.RLock()


# --------------------------------------------------------------------------- #
# Guarded sub-module loader
# --------------------------------------------------------------------------- #
def _load(module: str, names: Tuple[str, ...]) -> Dict[str, Any]:
    """Import ``names`` from ``.{module}``; return stubs on ImportError."""
    try:
        mod = importlib.import_module(f".{module}", package=__package__)
        _availability[module] = True
        return {n: getattr(mod, n) for n in names}
    except ImportError as exc:
        logger.info("%s unavailable: %s", module, exc)
        _availability[module] = False
        return {n: _MissingComponent(n, exc) for n in names}


# --------------------------------------------------------------------------- #
# Load every runtime sub-module
# --------------------------------------------------------------------------- #
globals().update(_load(
    "autogen_runtime",
    (
        "AutoGenRuntime", "AutoGenRuntimeConfig", "AutoGenRuntimeError",
        "AutoGenRunResult", "RunMetrics", "RuntimeMetrics",
    ),
))

# The two submodules both export ``RunMetrics`` / ``RuntimeMetrics`` —
# namespace them per framework to avoid a clobbering on ``globals().update``.
_autogen_metrics_types = globals()["RunMetrics"], globals()["RuntimeMetrics"]
globals().pop("RunMetrics", None)
globals().pop("RuntimeMetrics", None)

globals().update(_load(
    "langchain_runtime",
    (
        "LangChainRuntime", "LangChainRuntimeConfig", "LangChainRuntimeError",
        "LangChainRunResult", "RunMetrics", "RuntimeMetrics",
    ),
))
_langchain_metrics_types = globals()["RunMetrics"], globals()["RuntimeMetrics"]

# Re-publish under namespaced names.
AutoGenRunMetrics, AutoGenRuntimeMetrics = _autogen_metrics_types
LangChainRunMetrics, LangChainRuntimeMetrics = _langchain_metrics_types

# Convenience aliases: `RunMetrics` == LangChain's (per-run) TypedDict shape
# matches the AutoGen one field-for-field up to the extra `context_shortened`
# and tool-call semantic differences; callers should prefer the namespaced
# aliases. Kept for symmetry with the submodule `__all__`s.
RunMetrics = Union[AutoGenRunMetrics, LangChainRunMetrics]  # type: ignore[misc,assignment]
RuntimeMetrics = Union[  # type: ignore[misc,assignment]
    AutoGenRuntimeMetrics, LangChainRuntimeMetrics,
]


# --------------------------------------------------------------------------- #
# Registry API
# --------------------------------------------------------------------------- #
def _register_if_available(name: str, cls: Any) -> None:
    if isinstance(cls, _MissingComponent):
        logger.debug(
            "Runtime component '%s' unavailable; skipping registration.", name,
        )
        return
    if not (isinstance(cls, type) and issubclass(cls, RuntimeProtocol)):
        # This should never happen for the built-in runtimes — it means
        # the caller's environment has a mismatched autogen_runtime.py or
        # langchain_runtime.py. Warn loudly.
        logger.warning(
            "Default runtime '%s' (%r) does not satisfy RuntimeProtocol; "
            "not registering.",
            name, cls,
        )
        return
    with _registry_lock:
        _registry[name] = cls


_register_if_available("autogen", AutoGenRuntime)
_register_if_available("langchain", LangChainRuntime)


def register_runtime_component(
    name: str, cls: Type[Any], *, strict: bool = True
) -> None:
    """
    Register a runtime component class under ``name``.

    Parameters
    ----------
    name : str
        Registry key. Must be a non-empty string.
    cls : type
        Class to register. Must satisfy :class:`RuntimeProtocol` if
        ``strict=True`` (default).
    strict : bool, default True
        If True, raise :class:`RuntimeRegistryError` when ``cls`` does not
        satisfy :class:`RuntimeProtocol`.

    Raises
    ------
    RuntimeRegistryError
        If ``name`` is empty, already registered to a different class, or
        (with ``strict=True``) ``cls`` does not satisfy
        :class:`RuntimeProtocol`.
    TypeError
        If ``cls`` is not a class.
    """
    if not isinstance(name, str) or not name:
        raise RuntimeRegistryError("Component name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    if strict and not issubclass(cls, RuntimeProtocol):
        raise RuntimeRegistryError(
            f"{cls.__name__} does not satisfy RuntimeProtocol "
            f"(missing one of init / run / reduce_tool_calls / "
            f"shorten_context / finalize)."
        )
    with _registry_lock:
        existing = _registry.get(name)
        if isinstance(existing, _MissingComponent):
            existing = None
        if existing is not None and existing is not cls:
            raise RuntimeRegistryError(
                f"Runtime component '{name}' already registered to "
                f"{getattr(existing, '__name__', existing)!r}."
            )
        _registry[name] = cls
        logger.debug(
            "Registered runtime component '%s' -> %s", name, cls.__name__,
        )


def get_runtime_component(name: str) -> Type[Any]:
    """
    Return the runtime component class registered under ``name``.

    Raises
    ------
    RuntimeRegistryError
        If ``name`` is not registered.
    """
    with _registry_lock:
        try:
            cls = _registry[name]
        except KeyError as exc:
            raise RuntimeRegistryError(
                f"Unknown runtime component '{name}'. "
                f"Available: {sorted(k for k in _registry)}"
            ) from exc
    if isinstance(cls, _MissingComponent):
        # The component is registered but unavailable (its submodule
        # failed to import). Raise with the original cause.
        raise RuntimeRegistryError(
            f"Runtime component '{name}' is registered but unavailable."
        )
    return cls


def list_runtime_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    with _registry_lock:
        return {
            n: getattr(c, "__name__", type(c).__name__)
            for n, c in _registry.items()
        }


# --------------------------------------------------------------------------- #
# Convenience factories
# --------------------------------------------------------------------------- #
def create_runtime(
    name: str,
    *,
    config: Optional[Any] = None,
    strict: bool = True,
) -> RuntimeProtocol:
    """
    Construct a runtime adapter by registered name.

    Parameters
    ----------
    name : str
        Registry key (``"autogen"``, ``"langchain"``, or a custom one
        registered via :func:`register_runtime_component`).
    config : AutoGenRuntimeConfig | LangChainRuntimeConfig | None
        Typed configuration. If ``None``, the runtime uses its default.
    strict : bool, default True
        Forwarded to the runtime constructor.
    """
    cls = get_runtime_component(name)
    if config is not None:
        return cls(config=config, strict=strict)  # type: ignore[call-arg]
    return cls(strict=strict)  # type: ignore[call-arg]


# Unique config fields per runtime, used for polymorphic deserialization.
_RUNTIME_CONFIG_KEYS: Dict[str, frozenset[str]] = {
    "autogen":   frozenset({"base_tool_calls", "min_depth_increment", "min_tool_calls"}),
    "langchain": frozenset({"base_max_tools", "tools_per_run", "min_max_tools"}),
}


def _detect_runtime_kind(data: Mapping[str, Any]) -> str:
    # Explicit hint wins — future ``to_dict()`` versions can emit one.
    hint = data.get("runtime_type") if isinstance(data, Mapping) else None
    if isinstance(hint, str) and hint in _registry:
        return hint

    cfg = dict(data.get("config") or {})
    cfg_keys = set(cfg.keys())
    for kind, unique_keys in _RUNTIME_CONFIG_KEYS.items():
        if unique_keys & cfg_keys:
            if kind not in _registry:
                raise RuntimeRegistryError(
                    f"payload looks like a '{kind}' runtime but that "
                    f"component is not registered."
                )
            return kind
    raise RuntimeRegistryError(
        "Cannot detect runtime type from payload; expected one of "
        f"{sorted(_RUNTIME_CONFIG_KEYS)} or an explicit "
        f"'runtime_type' hint."
    )


def runtime_from_dict(data: Mapping[str, Any]) -> RuntimeProtocol:
    """
    Deserialize any registered runtime from its ``to_dict()`` output.

    Detection is by unique config field (or an explicit
    ``"runtime_type": "<name>"`` hint if present).
    """
    if not isinstance(data, Mapping):
        raise RuntimeRegistryError(
            f"runtime_from_dict expects a Mapping, got {type(data).__name__}."
        )
    kind = _detect_runtime_kind(data)
    cls = get_runtime_component(kind)
    if not hasattr(cls, "from_dict"):
        raise RuntimeRegistryError(
            f"runtime '{kind}' ({cls.__name__}) does not implement from_dict."
        )
    return cls.from_dict(data)  # type: ignore[attr-defined]


# --------------------------------------------------------------------------- #
# Health / diagnostics
# --------------------------------------------------------------------------- #
def health() -> Dict[str, Any]:
    """Return an aggregate summary suitable for operator dashboards."""
    with _registry_lock:
        registered = sorted(_registry)
    return {
        "version": __version__,
        "submodules": dict(_availability),
        "registered": registered,
        "numpy_available": _try_numpy_available(),
    }


def _try_numpy_available() -> bool:
    try:
        import numpy  # noqa: F401
        return True
    except ImportError:
        return False


# --------------------------------------------------------------------------- #
# Optional import-time validation (now meaningful)
# --------------------------------------------------------------------------- #
if os.environ.get("GREEN_AGENT_VALIDATE_RUNTIME") == "1":
    for _module, _ok in _availability.items():
        _level = logging.INFO if _ok else logging.WARNING
        logger.log(
            _level, "Runtime submodule %-22s : %s",
            _module, "available" if _ok else "MISSING",
        )

    for _name, _cls in list(_registry.items()):
        if isinstance(_cls, _MissingComponent):
            logger.warning(
                "Runtime component '%s' is unavailable: %s",
                _name, _cls._reason,
            )
            continue
        if not issubclass(_cls, RuntimeProtocol):
            logger.warning(
                "Runtime component '%s' (%s) does not satisfy "
                "RuntimeProtocol.",
                _name, _cls.__name__,
            )
            continue
        if not (_cls.__doc__ or "").strip():
            logger.warning(
                "Runtime component '%s' (%s) has no docstring.",
                _name, _cls.__name__,
            )
        else:
            logger.debug(
                "Runtime component '%s' (%s) OK.", _name, _cls.__name__,
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = (
    "__version__",
    # Errors
    "RuntimeRegistryError",
    # Protocol
    "RuntimeProtocol",
    # AutoGen
    "AutoGenRuntime",
    "AutoGenRuntimeConfig",
    "AutoGenRuntimeError",
    "AutoGenRunResult",
    "AutoGenRunMetrics",
    "AutoGenRuntimeMetrics",
    # LangChain
    "LangChainRuntime",
    "LangChainRuntimeConfig",
    "LangChainRuntimeError",
    "LangChainRunResult",
    "LangChainRunMetrics",
    "LangChainRuntimeMetrics",
    # Registry / manifest
    "RUNTIME_REGISTRY",
    "RUNTIME_AVAILABILITY",
    "get_runtime_component",
    "list_runtime_components",
    "register_runtime_component",
    # Convenience
    "create_runtime",
    "runtime_from_dict",
    "health",
)


def __dir__() -> List[str]:
    return sorted(__all__)
