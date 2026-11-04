# -*- coding: utf-8 -*-
"""
Multi-framework agent adapters with optional enhancements.
==========================================================

Exposes five adapters — one per supported framework — plus the base
class and the enhancement components they consume.

Adapters
--------
- :class:`BaseAgentAdapter` — abstract base; subclasses implement
  ``_execute`` and ``_get_agent_name``.
- :class:`LangChainAdapter` — LangChain / LangGraph / LCEL runnables.
- :class:`AutoGenAdapter` — Microsoft AutoGen agents.
- :class:`CrewAIAdapter` — CrewAI role-based agents.
- :class:`LimitGraphAdapter` — native LIMIT-GRAPH quantum agents.

Enhancement pipeline
--------------------
When ``use_enhancements=True`` **and** the enhancements package is
importable **and** (for adapters that cannot honour execution strategy
on their own) ``*_strategy_aware=True`` **and** a
``*_strategy_executor`` callable are supplied, the adapter delegates to
a distillation-optimizer pipeline that selects an execution strategy
per task. Otherwise the adapter runs in legacy mode.

``ENHANCEMENTS_AVAILABLE`` reports whether the enhancement schemas
(``WorkloadDescriptor``, ``NodeDescriptor``, ``ZeroTrustArchitecture``,
``FeedbackEvent``) are importable in this environment. When ``False``,
those names are :class:`_MissingComponent` stubs that raise
``ImportError`` on first use. The flag is a **single source of truth**
imported from ``base_adapter`` so it cannot diverge from what the
adapters themselves see.

Availability
------------
``INTEGRATIONS_AVAILABILITY`` maps sub-module name → bool:

- ``"base_adapter"``
- ``"langchain_adapter"``
- ``"autogen_adapter"``
- ``"crewai_adapter"``
- ``"limit_graph_adapter"``

A ``False`` entry means that adapter's class is a
:class:`_MissingComponent` stub; any attempt to instantiate it raises
``ImportError`` with the underlying cause chained.

Registry
--------
``INTEGRATIONS_REGISTRY`` maps a **short adapter name** → class:

- ``"langchain"``     → :class:`LangChainAdapter`
- ``"autogen"``       → :class:`AutoGenAdapter`
- ``"crewai"``        → :class:`CrewAIAdapter`
- ``"limit_graph"``   → :class:`LimitGraphAdapter`

Only concrete, instantiable adapters are registered. The abstract
:class:`BaseAgentAdapter` is exposed via its own import but is not
part of the registry (it cannot be instantiated).

:func:`create_agent_adapter` constructs an adapter by registered name,
forwarding ``config`` and the keyword-only ``strict`` flag.

Compatibility
-------------
- ``from src.integrations import LangChainAdapter`` and the other
  adapters — unchanged.
- **Behaviour change**: the four adapters that require optional
  frameworks are now import-guarded. A missing LangChain install no
  longer breaks ``import src.integrations``; the ``LangChainAdapter``
  name becomes a stub that raises ``ImportError`` on use instead.
- **Behaviour change**: ``TaskType``, ``Urgency``, ``NodeType``, and
  ``ZeroTrustArchitecture`` are unconditionally in ``__all__``. In
  non-enhanced environments they are ``_MissingComponent`` stubs that
  raise on use (previously ``TaskType`` / ``Urgency`` / ``NodeType``
  were either undefined or real classes depending on which import
  failed — an inconsistent public surface).
- ``ENHANCEMENTS_AVAILABLE``, ``INTEGRATIONS_AVAILABILITY``,
  ``INTEGRATIONS_REGISTRY``, ``AdapterConfigError``, ``AdapterError``,
  ``DistillationOptimizer``, ``ExecutionStrategy``,
  ``NormalizedFeatures``, ``create_agent_adapter``, ``health``, and
  ``__version__`` are new exports.
- ``__all__`` is unconditional; a caller using ``from src.integrations
  import *`` sees the same name list in every environment, with
  unavailable entries raising on use rather than being silently absent.

Requires Python 3.8+.
"""

from __future__ import annotations

import importlib
import logging
import threading
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Tuple


logger = logging.getLogger(__name__)

__version__ = "2.5.0"


# --------------------------------------------------------------------------- #
# Missing-component stub
# --------------------------------------------------------------------------- #
class _MissingComponent:
    """
    Placeholder for an adapter or enhancement schema that failed to
    import.

    Raises :class:`ImportError` on first use instead of returning
    ``None``. Supports call, attribute access, subscript, iteration,
    ``in``, and boolean evaluation so that any of the common ways a
    caller might probe the object fail loudly with the underlying
    cause chained.
    """

    __slots__ = ("_name", "_reason")

    def __init__(self, name: str, reason: BaseException) -> None:
        self._name = name
        self._reason = reason

    def _fail(self) -> None:
        raise ImportError(
            f"Integration component '{self._name}' is unavailable: "
            f"{self._reason}"
        ) from self._reason

    def __call__(self, *a: Any, **kw: Any) -> Any:
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
        return f"<unavailable integration component {self._name!r}>"


# --------------------------------------------------------------------------- #
# Availability manifest (public read-only view)
# --------------------------------------------------------------------------- #
_availability: Dict[str, bool] = {}
INTEGRATIONS_AVAILABILITY: Mapping[str, bool] = MappingProxyType(
    _availability
)


# --------------------------------------------------------------------------- #
# Guarded sub-module loader
# --------------------------------------------------------------------------- #
def _load(module: str, names: Tuple[str, ...]) -> Dict[str, Any]:
    """
    Import ``names`` from ``.{module}``.

    On module-level ``ImportError``: log at ``INFO``, mark the
    availability entry ``False``, and stub every requested name.

    On a name that is missing from an otherwise-importable module:
    mark the module ``True`` and stub only the missing name (this is a
    defensive branch; the current adapters export every name they
    declare).
    """
    try:
        mod = importlib.import_module(f".{module}", package=__package__)
    except ImportError as exc:
        logger.info("%s unavailable: %s", module, exc)
        _availability[module] = False
        return {
            name: _MissingComponent(name, exc) for name in names
        }

    _availability[module] = True
    out: Dict[str, Any] = {}
    for name in names:
        if hasattr(mod, name):
            out[name] = getattr(mod, name)
        else:
            reason = AttributeError(
                f"{name!r} not defined in {module}"
            )
            logger.warning(
                "%s does not export %r; substituting a stub.", module, name,
            )
            out[name] = _MissingComponent(name, reason)
    return out


# --------------------------------------------------------------------------- #
# Load base_adapter first (single source of truth for
# ENHANCEMENTS_AVAILABLE), then the four concrete adapters.
# --------------------------------------------------------------------------- #
globals().update(_load(
    "base_adapter",
    (
        "AdapterConfigError",
        "AdapterError",
        "BaseAgentAdapter",
        "DistillationOptimizer",
        "ENHANCEMENTS_AVAILABLE",
        "ExecutionStrategy",
        "NormalizedFeatures",
    ),
))

globals().update(_load(
    "langchain_adapter", ("LangChainAdapter",),
))
globals().update(_load(
    "autogen_adapter", ("AutoGenAdapter",),
))
globals().update(_load(
    "crewai_adapter", ("CrewAIAdapter",),
))
globals().update(_load(
    "limit_graph_adapter", ("LimitGraphAdapter",),
))


# --------------------------------------------------------------------------- #
# Enhancement schemas (independent of the adapters)
# --------------------------------------------------------------------------- #
# All names are shadowed uniformly on any import failure so that the
# public surface cannot end up with a mix of real classes and unbound
# names. This is the fix for the previous partial-binding bug where
# TaskType / Urgency / NodeType could remain bound to real classes even
# after an ImportError in a later line.
try:
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    from src.enhancements.schemas.workload_descriptor import (
        WorkloadDescriptor,
        TaskType,
        Urgency,
    )
    from src.enhancements.schemas.node_descriptor import (
        NodeDescriptor,
        NodeType,
    )
    from src.enhancements.zero_trust_architecture import (
        ZeroTrustArchitecture,
    )
except ImportError as _exc:  # pragma: no cover — defensive
    logger.info("Enhancement schemas unavailable: %s", _exc)
    FeedbackEvent = _MissingComponent("FeedbackEvent", _exc)
    WorkloadDescriptor = _MissingComponent("WorkloadDescriptor", _exc)
    TaskType = _MissingComponent("TaskType", _exc)
    Urgency = _MissingComponent("Urgency", _exc)
    NodeDescriptor = _MissingComponent("NodeDescriptor", _exc)
    NodeType = _MissingComponent("NodeType", _exc)
    ZeroTrustArchitecture = _MissingComponent(
        "ZeroTrustArchitecture", _exc,
    )


# --------------------------------------------------------------------------- #
# Registry of concrete, instantiable adapters
# --------------------------------------------------------------------------- #
_registry: Dict[str, Any] = {}
INTEGRATIONS_REGISTRY: Mapping[str, Any] = MappingProxyType(_registry)
_registry_lock = threading.RLock()


def _register_if_available(name: str, cls: Any) -> None:
    if isinstance(cls, _MissingComponent):
        logger.debug(
            "Adapter '%s' unavailable; not registering.", name,
        )
        return
    if not isinstance(cls, type):
        logger.warning(
            "Adapter '%s' (%r) is not a class; not registering.",
            name, cls,
        )
        return
    with _registry_lock:
        _registry[name] = cls


_register_if_available("langchain", LangChainAdapter)
_register_if_available("autogen", AutoGenAdapter)
_register_if_available("crewai", CrewAIAdapter)
_register_if_available("limit_graph", LimitGraphAdapter)


# --------------------------------------------------------------------------- #
# Registry API
# --------------------------------------------------------------------------- #
def register_agent_adapter(
    name: str, cls: type, *, strict: bool = True,
) -> None:
    """
    Register a custom adapter under ``name``.

    Parameters
    ----------
    name : str
        Non-empty registry key.
    cls : type
        Adapter class to register.
    strict : bool, default True
        If True, require ``cls`` to be a subclass of
        :class:`BaseAgentAdapter`. Set to False to register duck-typed
        adapters that only implement the ``run`` protocol.

    Raises
    ------
    AdapterConfigError
        If ``name`` is empty, ``cls`` is not a class, already registered
        to a different class, or (with ``strict=True``) not a subclass of
        :class:`BaseAgentAdapter`.
    """
    if not isinstance(name, str) or not name:
        raise AdapterConfigError(
            "Adapter name must be a non-empty string."
        )
    if not isinstance(cls, type):
        raise AdapterConfigError(
            f"Adapter must be a class, got {type(cls).__name__}."
        )
    if strict:
        base = BaseAgentAdapter
        if isinstance(base, _MissingComponent):
            base = None
        if base is not None and not issubclass(cls, base):
            raise AdapterConfigError(
                f"{cls.__name__} is not a subclass of BaseAgentAdapter."
            )
    with _registry_lock:
        existing = _registry.get(name)
        if existing is not None and existing is not cls:
            raise AdapterConfigError(
                f"Adapter '{name}' already registered to "
                f"{getattr(existing, '__name__', existing)!r}."
            )
        _registry[name] = cls


def get_agent_adapter(name: str) -> type:
    """
    Return the adapter class registered under ``name``.

    Raises
    ------
    AdapterConfigError
        If ``name`` is not registered, or the module failed to import.
    """
    with _registry_lock:
        try:
            cls = _registry[name]
        except KeyError as exc:
            raise AdapterConfigError(
                f"Unknown or unavailable adapter '{name}'. "
                f"Available: {sorted(_registry)}"
            ) from exc
    return cls


def list_agent_adapters() -> Dict[str, str]:
    """Return a mapping of registered adapter name -> class name."""
    with _registry_lock:
        return {
            n: getattr(c, "__name__", type(c).__name__)
            for n, c in _registry.items()
        }


# --------------------------------------------------------------------------- #
# Factory
# --------------------------------------------------------------------------- #
def create_agent_adapter(
    name: str,
    agent: Any,
    config: Optional[Dict[str, Any]] = None,
    *,
    strict: bool = False,
    **extra_kwargs: Any,
) -> BaseAgentAdapter:
    """
    Construct the adapter registered under ``name``.

    Parameters
    ----------
    name : str
        Registered adapter key (``"langchain"``, ``"autogen"``,
        ``"crewai"``, ``"limit_graph"``, or a custom name added via
        :func:`register_agent_adapter`).
    agent : Any
        The framework agent instance.
    config : dict, optional
        Adapter configuration (recognised keys depend on the adapter).
    strict : bool, default False
        Forwarded to the adapter's ``__init__`` as the keyword-only
        ``strict`` argument.
    **extra_kwargs : Any
        Additional keyword arguments forwarded verbatim to the adapter's
        ``__init__``. Reserved for adapter-specific construction needs;
        the four built-in adapters accept only ``agent``, ``config``,
        and ``strict``.

    Raises
    ------
    AdapterConfigError
        If ``name`` is not registered, the adapter module failed to
        import, or the resulting constructor raised ``TypeError``.
    """
    cls = get_agent_adapter(name)
    try:
        return cls(agent, config, strict=strict, **extra_kwargs)
    except TypeError as exc:
        raise AdapterConfigError(
            f"cannot instantiate '{name}' with the supplied arguments: "
            f"{exc}"
        ) from exc


# --------------------------------------------------------------------------- #
# Health
# --------------------------------------------------------------------------- #
def health() -> Dict[str, Any]:
    """
    Return an aggregate summary suitable for operator dashboards and
    bug reports.
    """
    with _registry_lock:
        registered = sorted(_registry)
    # Read the flag safely — if base_adapter failed to load, the value
    # is a _MissingComponent and any truth-test raises ImportError. That
    # is the correct behaviour (nothing in this package works), but
    # ``health()`` is often called from a diagnostic script that wants
    # a report rather than an exception. Coerce defensively.
    flag = ENHANCEMENTS_AVAILABLE
    try:
        enhancements_ok = bool(flag)
    except ImportError:
        enhancements_ok = False
    return {
        "version": __version__,
        "submodules": dict(_availability),
        "registered_adapters": registered,
        "enhancements_available": enhancements_ok,
    }


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    # Adapters
    "BaseAgentAdapter",
    "LangChainAdapter",
    "AutoGenAdapter",
    "CrewAIAdapter",
    "LimitGraphAdapter",
    # Errors / optimizer primitives
    "AdapterError",
    "AdapterConfigError",
    "DistillationOptimizer",
    "ExecutionStrategy",
    "NormalizedFeatures",
    # Enhancement schemas
    "FeedbackEvent",
    "WorkloadDescriptor",
    "TaskType",
    "Urgency",
    "NodeDescriptor",
    "NodeType",
    "ZeroTrustArchitecture",
    # Availability
    "ENHANCEMENTS_AVAILABLE",
    "INTEGRATIONS_AVAILABILITY",
    "INTEGRATIONS_REGISTRY",
    # Registry / factory / health
    "register_agent_adapter",
    "get_agent_adapter",
    "list_agent_adapters",
    "create_agent_adapter",
    "health",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m integrations
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

    def _raises(exc_type, fn) -> bool:
        try:
            fn()
        except exc_type:
            return True
        except Exception:
            return False
        return False

    # ---- 1. Module loads regardless of optional deps -------------- #
    print("imports:")
    _check("module imported", True)
    _check(
        "__version__ present",
        isinstance(__version__, str) and bool(__version__),
    )

    # ---- 2. __all__ is unconditional ---------------------------- #
    print("__all__ unconditional:")
    _check(
        "__all__ is a list",
        isinstance(__all__, list) and len(__all__) > 0,
    )
    _check(
        "enhancement names always present",
        all(
            n in __all__
            for n in (
                "FeedbackEvent", "WorkloadDescriptor",
                "TaskType", "Urgency",
                "NodeDescriptor", "NodeType",
                "ZeroTrustArchitecture",
                "ENHANCEMENTS_AVAILABLE",
            )
        ),
    )
    _check(
        "adapter names always present",
        all(
            n in __all__
            for n in (
                "BaseAgentAdapter", "LangChainAdapter",
                "AutoGenAdapter", "CrewAIAdapter",
                "LimitGraphAdapter",
            )
        ),
    )

    # ---- 3. __dir__ matches __all__ ----------------------------- #
    print("__dir__:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))

    # ---- 4. Availability manifest ------------------------------- #
    print("availability:")
    _check(
        "manifest is read-only",
        _raises(TypeError, lambda:
                INTEGRATIONS_AVAILABILITY.__setitem__("x", True)),  # type: ignore[attr-defined]
    )
    _check(
        "base_adapter always available",
        INTEGRATIONS_AVAILABILITY.get("base_adapter") is True,
    )
    for _mod in (
        "langchain_adapter", "autogen_adapter",
        "crewai_adapter", "limit_graph_adapter",
    ):
        _check(
            f"{_mod} availability recorded",
            _mod in INTEGRATIONS_AVAILABILITY,
        )

    # ---- 5. Enhancement names behave uniformly ------------------ #
    print("enhancement names:")
    _check(
        "FeedbackEvent is a class or a stub",
        isinstance(FeedbackEvent, type) or isinstance(
            FeedbackEvent, _MissingComponent
        ),
    )
    _check(
        "TaskType is defined",
        TaskType is not None,  # a class or _MissingComponent
    )
    _check(
        "NodeType is defined",
        NodeType is not None,
    )
    # If enhancements are unavailable, all names are stubs; using one
    # must raise ImportError (not silently be None).
    try:
        enh_ok = bool(ENHANCEMENTS_AVAILABLE)
    except ImportError:
        enh_ok = False
    if not enh_ok:
        _check(
            "unavailable FeedbackEvent raises on use",
            _raises(ImportError, lambda: FeedbackEvent()),  # type: ignore[operator]
        )
        _check(
            "unavailable TaskType raises on use",
            _raises(ImportError, lambda: bool(TaskType)),
        )
    else:
        print("  (enhancements available — skipping stub-use test)")

    # ---- 6. Registry ------------------------------------------- #
    print("registry:")
    _check(
        "registry is read-only",
        _raises(TypeError, lambda:
                INTEGRATIONS_REGISTRY.__setitem__("x", object)),  # type: ignore[attr-defined]
    )
    listed = list_agent_adapters()
    _check(
        "registry reflects availability",
        set(listed) == {
            name for name in (
                "langchain", "autogen", "crewai", "limit_graph",
            )
            if INTEGRATIONS_AVAILABILITY.get(
                f"{name}_adapter", False,
            )
        },
    )
    for name in listed:
        _check(
            f"get_agent_adapter({name!r}) returns the class",
            get_agent_adapter(name) is INTEGRATIONS_REGISTRY[name],
        )

    _check(
        "unknown name raises",
        _raises(AdapterConfigError, lambda:
                get_agent_adapter("nope")),
    )

    # ---- 7. Factory -------------------------------------------- #
    print("factory:")
    if "langchain" in listed:
        class _FakeChain:
            name = "fake"

            def invoke(self, x: Any) -> str:
                return "ok"

        adapter = create_agent_adapter(
            "langchain", _FakeChain(), {"use_enhancements": False},
        )
        _check(
            "factory constructs the adapter",
            type(adapter).__name__ == "LangChainAdapter",
        )
    else:
        print("  (langchain unavailable — skipping factory construction)")

    _check(
        "factory rejects unknown name",
        _raises(AdapterConfigError, lambda:
                create_agent_adapter("nope", object())),
    )

    # ---- 8. register_agent_adapter ----------------------------- #
    print("register:"):
    if not isinstance(BaseAgentAdapter, _MissingComponent):

        class _CustomAdapter(BaseAgentAdapter):
            def _execute(self, task_input, strategy="native"):
                return {"answer": "custom"}

            def _get_agent_name(self):
                return "custom"

        register_agent_adapter("custom", _CustomAdapter)
        _check(
            "custom adapter registered",
            "custom" in list_agent_adapters(),
        )
        _check(
            "get_agent_adapter returns custom",
            get_agent_adapter("custom") is _CustomAdapter,
        )

        _check(
            "re-registering same class is a no-op",
            (register_agent_adapter("custom", _CustomAdapter), True)[1],
        )

        class _OtherAdapter(BaseAgentAdapter):
            def _execute(self, task_input, strategy="native"):
                return {}

            def _get_agent_name(self):
                return "other"

        _check(
            "conflict raises",
            _raises(AdapterConfigError, lambda:
                    register_agent_adapter("custom", _OtherAdapter)),
        )
        _check(
            "empty name raises",
            _raises(AdapterConfigError, lambda:
                    register_agent_adapter("", _OtherAdapter)),
        )
        _check(
            "non-class raises",
            _raises(AdapterConfigError, lambda:
                    register_agent_adapter("x", 42)),  # type: ignore[arg-type]
        )

        class _NotAnAdapter:
            def _execute(self, task_input, strategy="native"):
                return {}

            def _get_agent_name(self):
                return "x"

        _check(
            "strict rejects non-subclass",
            _raises(AdapterConfigError, lambda:
                    register_agent_adapter(
                        "strict-test", _NotAnAdapter, strict=True,
                    )),
        )
        register_agent_adapter(
            "strict-test", _NotAnAdapter, strict=False,
        )
        _check(
            "strict=False accepts non-subclass",
            "strict-test" in list_agent_adapters(),
        )

    # ---- 9. health --------------------------------------------- #
    print("health:")
    h = health()
    _check("version present", h["version"] == __version__)
    _check(
        "submodules dict",
        isinstance(h["submodules"], dict)
        and "base_adapter" in h["submodules"],
    )
    _check(
        "registered adapters listed",
        isinstance(h["registered_adapters"], list),
    )
    _check(
        "enhancements_available is bool",
        isinstance(h["enhancements_available"], bool),
    )

    # ---- 10. Error types are exported -------------------------- #
    print("errors:")
    _check(
        "AdapterError is a class",
        isinstance(AdapterError, type),
    )
    _check(
        "AdapterConfigError subclasses AdapterError",
        issubclass(AdapterConfigError, AdapterError),
    )

    # ---- 11. Optimizer primitives exported -------------------- #
    print("optimizer primitives:")
    _check(
        "DistillationOptimizer exported",
        DistillationOptimizer is not None,
    )
    _check(
        "ExecutionStrategy exported",
        ExecutionStrategy is not None,
    )

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
