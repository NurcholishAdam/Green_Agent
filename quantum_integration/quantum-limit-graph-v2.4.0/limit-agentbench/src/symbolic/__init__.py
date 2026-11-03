# src/symbolic/__init__.py

"""
Symbolic Reasoning Module for Green Agent
=========================================

Neuro-symbolic oversight inspired by the FormalJudge paradigm. Exposes
the formal-rule engine that evaluates a policy against metrics.

Safety contract
---------------
The engine's condition language is restricted by design. A rule whose
condition violates any of the following raises :class:`SymbolicReasoningError`
at evaluation time (both strict and non-strict modes):

1. **Variable allowlist** — only names in
   ``SymbolicReasoningConfig.allowed_variables`` may appear in a
   condition. ``allowed_variables=("energy",)`` rejects
   ``carbon > 5``.

2. **Trivially-forced literals** — ``or True`` / ``and False`` /
   ``not <bool>`` / a bare ``True`` or ``False`` are rejected. A policy
   rule like ``carbon > 60 or True`` no longer silently fires on every
   step.

3. **AST allowlist** — ``Call``, ``Attribute``, ``Subscript``,
   comprehensions, f-strings, lambdas, and named expressions are all
   rejected by the AST visitor.

Unit conventions
----------------
``_normalize_metrics`` assumes ``cumulative.total_carbon_kg`` is in kg and
``cumulative.total_latency_ms`` is in ms; both are multiplied by
``config.carbon_unit_factor`` / ``config.latency_unit_factor`` (default
``1000``). If the upstream already emits grams or ms, set those factors to
``1.0`` — otherwise rules fire at 1/1000 of the intended threshold.

NaN policy
----------
``config.nan_policy`` ∈ ``{"coerce", "raise"}``. ``"coerce"`` (default;
backward-compatible) replaces non-finite metrics with ``0.0`` and logs a
``WARNING``. ``"raise"`` propagates :class:`SymbolicReasoningError` for
safety-critical deployments — a NaN silently treated as ``0`` can suppress
a rule that should have fired.

Missing engine
--------------
If :mod:`symbolic.symbolic_reasoning_engine` cannot be imported, every
exported name becomes a ``_MissingComponent`` stub whose first use raises
``ImportError`` chained to the original cause. The façade's ``health()``
reports ``symbolic_reasoning_engine: False`` for operators. **Do not**
wrap this in ``if SymbolicReasoningEngine is None: ...`` — the stub raises
on ``bool()``, so that pattern no longer silently disables the safety
check.

Registry semantics
------------------
``SYMBOLIC_REGISTRY`` holds **instantiable components** — classes with
``to_dict`` and ``statistics``. Only :class:`SymbolicReasoningEngine` is
registered by default. The data records :class:`SymbolicRule` and
:class:`ViolationTrace` are exported via ``__all__`` for direct use but
are not registry entries: their constructors require caller-supplied
data, so ``get_symbolic_component("rule")()`` would raise ``TypeError``.
Use the class directly instead.

Enhancements
------------
- ``_MissingComponent`` stub — missing sub-modules raise a clear
  ``ImportError`` on first use instead of returning ``None``.
- **``SymbolicReasoningStatistics`` re-exported** alongside the other
  enhanced sub-module exports.
- **Registry narrowed to instantiable components** — ``SymbolicRule``
  and ``ViolationTrace`` are exposed via ``__all__`` but not registered;
  adding them would cause ``TypeError`` on default construction.
- ``SymbolicComponent`` Protocol — structural check for the registry;
  ``_register_if_available`` and the import-time validation block both
  use it instead of ``callable``.
- Read-only public views: ``SYMBOLIC_REGISTRY`` and
  ``SYMBOLIC_AVAILABILITY`` are ``MappingProxyType``; internal dicts are
  private.
- ``SymbolicRegistryError(ValueError)`` — narrow error type; replaces
  the ``KeyError`` from ``get_symbolic_component`` and the ``ValueError``
  from ``register_symbolic_component``.
- ``create_symbolic_component(name, *, config=None, strict=True,
  no_policy=False)`` — validated factory with a ``no_policy`` shortcut
  for the common "engine without loading a policy file" case.
- ``YAML_AVAILABLE`` — public boolean reporting whether the optional
  ``yaml`` dependency is installed. Mirrors ``NUMPY_AVAILABLE`` in the
  enhanced ``rlhf/reward_shaper.py``.
- ``health()`` — version + availability + registered components + yaml
  status. Answers "is the safety engine fully operational?" without
  instantiating it.
- ``_registry_lock`` guards registration and lookup.
- ``__dir__`` for clean REPL discoverability.
- Optional import-time validation via ``GREEN_AGENT_VALIDATE_SYMBOLIC=1``
  (now checks ``SymbolicComponent`` conformance instead of just
  ``callable``).

Compatibility
-------------
- ``from symbolic import SymbolicReasoningEngine`` and the other five
  sub-module exports unchanged.
- ``SYMBOLIC_REGISTRY`` / ``SYMBOLIC_AVAILABILITY`` are now read-only.
  Anything doing ``SYMBOLIC_REGISTRY["x"] = ...`` must go through
  ``register_symbolic_component``.
- **``list_symbolic_components()`` returns ``{"engine": "..."}``**,
  not the previous ``{"engine", "rule", "violation"}``. The data records
  are still importable via ``from symbolic import SymbolicRule`` /
  ``ViolationTrace``; they're just not registry entries.
- ``get_symbolic_component`` now raises ``SymbolicRegistryError``
  (``ValueError`` subclass) instead of ``KeyError``. Callers doing
  ``except KeyError:`` must switch.
- ``register_symbolic_component`` now raises ``SymbolicRegistryError``
  instead of ``ValueError``; existing ``except ValueError:`` handlers
  continue to work.
- ``from symbolic import *`` no longer injects ``None`` for missing
  names — it injects a stub that raises ``ImportError`` on use.

Requires Python 3.8+. ``runtime_checkable`` Protocols work on 3.8+.
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
    Union, runtime_checkable,
)

logger = logging.getLogger(__name__)

__version__ = "2.4.0"


# --------------------------------------------------------------------------- #
# Optional yaml availability (public)
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — optional dependency
    import yaml as _yaml_probe  # noqa: F401

    YAML_AVAILABLE: bool = True
except ImportError:  # pragma: no cover
    YAML_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SymbolicRegistryError(ValueError):
    """Raised for invalid symbolic-registry operations."""


# --------------------------------------------------------------------------- #
# Structural component contract
# --------------------------------------------------------------------------- #
@runtime_checkable
class SymbolicComponent(Protocol):
    """
    Structural interface required of every registered symbolic component.

    The engine exposes ``to_dict`` and ``statistics``; custom engines
    registered via :func:`register_symbolic_component` must do the same.
    The data records (``SymbolicRule`` / ``ViolationTrace``) are **not**
    components in this sense — they have ``to_dict`` but not
    ``statistics``, and their constructors require caller-supplied data.
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
    For a safety subsystem this matters more than for the other façades:
    a caller who catches ``if engine is None: skip_checks()`` used to
    silently disable the safety layer. The stub raises on ``__bool__``,
    so that pattern fails loudly.
    """

    __slots__ = ("_name", "_reason")

    def __init__(self, name: str, reason: BaseException) -> None:
        self._name = name
        self._reason = reason

    def _fail(self) -> None:
        raise ImportError(
            f"Symbolic component '{self._name}' is unavailable: "
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
        return f"<unavailable symbolic component {self._name!r}>"


# --------------------------------------------------------------------------- #
# Read-only public views
# --------------------------------------------------------------------------- #
_availability: Dict[str, bool] = {}
SYMBOLIC_AVAILABILITY: Mapping[str, bool] = MappingProxyType(_availability)

_registry: Dict[str, Any] = {}
SYMBOLIC_REGISTRY: Mapping[str, Any] = MappingProxyType(_registry)
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
# Load the symbolic_reasoning_engine sub-module
# --------------------------------------------------------------------------- #
globals().update(_load(
    "symbolic_reasoning_engine",
    (
        "SymbolicReasoningEngine",
        "SymbolicReasoningConfig",
        "SymbolicReasoningError",
        "SymbolicReasoningStatistics",
        "SymbolicRule",
        "ViolationTrace",
    ),
))


# --------------------------------------------------------------------------- #
# Registry API
# --------------------------------------------------------------------------- #
def _register_if_available(name: str, cls: Any) -> None:
    if isinstance(cls, _MissingComponent):
        logger.debug(
            "Symbolic component '%s' unavailable; skipping registration.",
            name,
        )
        return
    if not (isinstance(cls, type) and issubclass(
        cls, SymbolicComponent
    )):
        logger.warning(
            "Default symbolic component '%s' (%r) does not satisfy "
            "SymbolicComponent; not registering.",
            name, cls,
        )
        return
    with _registry_lock:
        _registry[name] = cls


# Only the engine is instantiable-with-defaults. SymbolicRule and
# ViolationTrace are data records — exposed via __all__ but not
# registered (see module docstring, "Registry semantics").
_register_if_available("engine", SymbolicReasoningEngine)


def register_symbolic_component(
    name: str,
    cls: Type[Any],
    *,
    strict: bool = True,
) -> None:
    """
    Register a symbolic component class under ``name``.

    Parameters
    ----------
    name : str
        Registry key. Must be a non-empty string.
    cls : type
        Class to register.
    strict : bool, default True
        If True, raise :class:`SymbolicRegistryError` when ``cls`` does
        not satisfy :class:`SymbolicComponent`.

    Raises
    ------
    SymbolicRegistryError
        If ``name`` is empty, already registered to a different class, or
        (with ``strict=True``) ``cls`` does not satisfy
        :class:`SymbolicComponent`.
    TypeError
        If ``cls`` is not a class.
    """
    if not isinstance(name, str) or not name:
        raise SymbolicRegistryError(
            "Component name must be a non-empty string."
        )
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    if strict and not issubclass(cls, SymbolicComponent):
        raise SymbolicRegistryError(
            f"{cls.__name__} does not satisfy SymbolicComponent "
            f"(missing to_dict / statistics)."
        )
    with _registry_lock:
        existing = _registry.get(name)
        if isinstance(existing, _MissingComponent):
            existing = None
        if existing is not None and existing is not cls:
            raise SymbolicRegistryError(
                f"Symbolic component '{name}' already registered to "
                f"{getattr(existing, '__name__', existing)!r}."
            )
        _registry[name] = cls
        logger.debug(
            "Registered symbolic component '%s' -> %s",
            name, cls.__name__,
        )


def get_symbolic_component(name: str) -> Type[Any]:
    """
    Return the symbolic component class registered under ``name``.

    Raises
    ------
    SymbolicRegistryError
        If ``name`` is not registered, or its sub-module failed to load.
    """
    with _registry_lock:
        try:
            cls = _registry[name]
        except KeyError as exc:
            raise SymbolicRegistryError(
                f"Unknown symbolic component '{name}'. "
                f"Available: {sorted(_registry)}"
            ) from exc
    if isinstance(cls, _MissingComponent):
        raise SymbolicRegistryError(
            f"Symbolic component '{name}' is registered but unavailable."
        )
    return cls


def list_symbolic_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    with _registry_lock:
        return {
            n: getattr(c, "__name__", type(c).__name__)
            for n, c in _registry.items()
        }


# --------------------------------------------------------------------------- #
# Convenience factory
# --------------------------------------------------------------------------- #
def create_symbolic_component(
    name: str,
    *,
    config: Optional[Any] = None,
    strict: bool = True,
    no_policy: bool = False,
    **extra_kwargs: Any,
) -> Any:
    """
    Construct a symbolic component by registered name.

    Parameters
    ----------
    name : str
        Registry key (``"engine"`` by default).
    config : SymbolicReasoningConfig, optional
        Typed config. ``None`` uses the component's default config.
    strict : bool, default True
        Forwarded to the component's ``__init__``.
    no_policy : bool, default False
        Shortcut for ``name="engine"``: when True and ``config`` is
        ``None``, construct with ``SymbolicReasoningConfig(auto_load=False)``
        so the engine skips loading the policy file. Raises if both
        ``no_policy=True`` and an explicit ``config`` are supplied.
    **extra_kwargs : Any
        Forwarded to the component's ``__init__``.

    Raises
    ------
    SymbolicRegistryError
        If ``name`` is not registered, its sub-module failed to load, the
        factory's argument combination is invalid, or the resulting call
        raises ``TypeError``.
    """
    cls = get_symbolic_component(name)

    if no_policy:
        if config is not None:
            raise SymbolicRegistryError(
                "no_policy=True conflicts with an explicit config; "
                "set config=SymbolicReasoningConfig(auto_load=False) "
                "instead."
            )
        if name != "engine":
            raise SymbolicRegistryError(
                f"no_policy=True only applies to the 'engine' component, "
                f"got {name!r}."
            )
        config = SymbolicReasoningConfig(auto_load=False)

    kwargs: Dict[str, Any] = {"strict": strict, **extra_kwargs}
    if config is not None:
        kwargs["config"] = config
    try:
        return cls(**kwargs)
    except TypeError as exc:
        raise SymbolicRegistryError(
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

    Answers "is the safety engine fully operational?" without
    instantiating it.
    """
    with _registry_lock:
        registered = sorted(_registry)
    return {
        "version": __version__,
        "submodules": dict(_availability),
        "registered_components": registered,
        "yaml_available": YAML_AVAILABLE,
        "factory": "create_symbolic_component",
    }


# --------------------------------------------------------------------------- #
# Optional import-time validation
# --------------------------------------------------------------------------- #
if os.environ.get("GREEN_AGENT_VALIDATE_SYMBOLIC") == "1":
    for _module, _ok in _availability.items():
        _level = logging.INFO if _ok else logging.WARNING
        logger.log(
            _level, "Symbolic submodule %-26s : %s",
            _module, "available" if _ok else "MISSING",
        )

    if not YAML_AVAILABLE:
        logger.warning(
            "yaml not installed; the engine will fall back to default "
            "rules instead of loading the policy file."
        )

    for _name, _cls in list(_registry.items()):
        if isinstance(_cls, _MissingComponent):
            logger.warning(
                "Symbolic component '%s' is unavailable: %s",
                _name, _cls._reason,
            )
            continue
        if not issubclass(_cls, SymbolicComponent):
            logger.warning(
                "Symbolic component '%s' (%s) does not satisfy "
                "SymbolicComponent.",
                _name, _cls.__name__,
            )
            continue
        if not (_cls.__doc__ or "").strip():
            logger.warning(
                "Symbolic component '%s' (%s) has no docstring.",
                _name, _cls.__name__,
            )
            continue
        try:
            sig = inspect.signature(_cls.__init__)
            logger.debug(
                "Symbolic component '%s' (%s) OK; __init__%s",
                _name, _cls.__name__, sig,
            )
        except (TypeError, ValueError) as exc:  # pragma: no cover — CI only
            logger.exception(
                "Symbolic component '%s' (%s) signature invalid: %s",
                _name, _cls.__name__, exc,
            )
            raise


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = (
    "__version__",
    # Errors / Protocols / stub
    "SymbolicRegistryError",
    "SymbolicComponent",
    # engine and data records
    "SymbolicReasoningEngine",
    "SymbolicReasoningConfig",
    "SymbolicReasoningError",
    "SymbolicReasoningStatistics",
    "SymbolicRule",
    "ViolationTrace",
    # registry / manifest
    "SYMBOLIC_REGISTRY",
    "SYMBOLIC_AVAILABILITY",
    "get_symbolic_component",
    "list_symbolic_components",
    "register_symbolic_component",
    # factory / health
    "create_symbolic_component",
    "health",
    # environment
    "YAML_AVAILABLE",
)


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m symbolic
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

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
            _check(f"stub {probe_name} raises", False)
        except ImportError as exc:
            _check(
                f"stub {probe_name} raises ImportError",
                "unavailable" in str(exc),
            )

    # ---- 2. Frozen public views ---------------------------------- #
    print("frozen views:")
    try:
        SYMBOLIC_REGISTRY["hack"] = object  # type: ignore[index]
        _check("registry frozen", False)
    except TypeError:
        _check("registry frozen", True)
    try:
        SYMBOLIC_AVAILABILITY["hack"] = True  # type: ignore[index]
        _check("availability frozen", False)
    except TypeError:
        _check("availability frozen", True)

    # ---- 3. All exports present ---------------------------------- #
    print("exports:")
    for _name in (
        "SymbolicReasoningEngine", "SymbolicReasoningConfig",
        "SymbolicReasoningError", "SymbolicReasoningStatistics",
        "SymbolicRule", "ViolationTrace",
        "SymbolicRegistryError", "SymbolicComponent",
        "YAML_AVAILABLE",
        "create_symbolic_component", "health",
    ):
        _check(
            f"{_name} exported",
            _name in __all__
            and getattr(sys.modules[__name__], _name, None) is not None,
        )

    # ---- 4. Registry narrowed to engine only --------------------- #
    print("registry narrowing:")
    comps = list_symbolic_components()
    _check(
        "only engine registered",
        comps == {"engine": "SymbolicReasoningEngine"},
    )
    _check(
        "engine in registry",
        "engine" in SYMBOLIC_REGISTRY,
    )
    _check(
        "rule NOT in registry (data record)",
        "rule" not in SYMBOLIC_REGISTRY,
    )
    _check(
        "violation NOT in registry (data record)",
        "violation" not in SYMBOLIC_REGISTRY,
    )

    # Data records still importable
    _check(
        "SymbolicRule directly importable",
        SymbolicRule is not None,
    )
    _check(
        "ViolationTrace directly importable",
        ViolationTrace is not None,
    )

    # ---- 5. SymbolicComponent Protocol --------------------------- #
    print("SymbolicComponent Protocol:")
    _check(
        "engine satisfies SymbolicComponent",
        issubclass(SymbolicReasoningEngine, SymbolicComponent),
    )
    # Data records have to_dict but not statistics
    _check(
        "rule does NOT satisfy SymbolicComponent",
        not issubclass(SymbolicRule, SymbolicComponent),
    )
    _check(
        "violation does NOT satisfy SymbolicComponent",
        not issubclass(ViolationTrace, SymbolicComponent),
    )

    # ---- 6. Registry errors -------------------------------------- #
    print("registry errors:")
    try:
        get_symbolic_component("nope")
        _check("unknown name raises", False)
    except SymbolicRegistryError:
        _check("unknown name raises", True)

    try:
        get_symbolic_component("rule")
        _check("data record not registered", False)
    except SymbolicRegistryError:
        _check("data record not registered", True)

    # ---- 7. register_symbolic_component validation --------------- #
    print("register validation:")
    try:
        register_symbolic_component("", SymbolicReasoningEngine)
        _check("empty name rejected", False)
    except SymbolicRegistryError:
        _check("empty name rejected", True)

    try:
        register_symbolic_component("bad", 42)  # type: ignore[arg-type]
        _check("non-class rejected", False)
    except TypeError:
        _check("non-class rejected", True)

    # Non-conforming class rejected in strict mode
    class _NoStats:
        def to_dict(self, **kwargs):
            return {}

    try:
        register_symbolic_component(
            "no_stats", _NoStats, strict=True,
        )
        _check("non-conforming rejected (strict)", False)
    except SymbolicRegistryError:
        _check("non-conforming rejected (strict)", True)

    register_symbolic_component("no_stats", _NoStats, strict=False)
    _check(
        "non-conforming accepted with strict=False",
        "no_stats" in SYMBOLIC_REGISTRY,
    )

    # Idempotent re-registration of the same class
    register_symbolic_component("engine", SymbolicReasoningEngine)
    _check("re-registration of same class OK", True)

    # Different class under existing name
    try:
        register_symbolic_component("engine", _NoStats)
        _check("different class rejected", False)
    except SymbolicRegistryError:
        _check("different class rejected", True)

    # ---- 8. create_symbolic_component ---------------------------- #
    print("factory:")
    engine = create_symbolic_component("engine", no_policy=True)
    _check(
        "no_policy engine constructed",
        isinstance(engine, SymbolicReasoningEngine),
    )
    _check(
        "no_policy engine has no rules",
        len(engine.rules) == 0,
    )

    # no_policy=False uses default config (auto_load=True)
    engine2 = create_symbolic_component("engine")
    _check(
        "default engine constructed",
        isinstance(engine2, SymbolicReasoningEngine),
    )

    # no_policy + explicit config → error
    try:
        create_symbolic_component(
            "engine",
            config=SymbolicReasoningConfig(),
            no_policy=True,
        )
        _check("no_policy + config rejected", False)
    except SymbolicRegistryError:
        _check("no_policy + config rejected", True)

    # no_policy on non-engine → error
    try:
        create_symbolic_component("no_stats", no_policy=True)
        _check("no_policy on non-engine rejected", False)
    except SymbolicRegistryError:
        _check("no_policy on non-engine rejected", True)

    # ---- 9. End-to-end safety contract --------------------------- #
    print("safety contract end-to-end:")
    fresh = create_symbolic_component("engine", no_policy=True)
    fresh._load_default_rules()
    violations = fresh.evaluate_rules(
        {"energy": 10.0, "memory": 0}, step=1,
    )
    _check(
        "default rule triggers",
        len(violations) == 1 and violations[0].rule_id == "DEFAULT-001",
    )
    _check(
        "violation carries category",
        violations[0].category == "sustainability",
    )

    # Trivially-forced condition rejected
    fresh.rules.append(SymbolicRule(
        id="BAD-1",
        name="bad",
        category="test",
        priority="high",
        condition="energy > 5 or True",
        action="alert",
        explanation="bad",
    ))
    try:
        fresh.evaluate_rules({"energy": 10.0}, step=2)
        _check("trivially-forced rejected", False)
    except SymbolicReasoningError:
        _check("trivially-forced rejected", True)

    # ---- 10. Round-trip ----------------------------------------- #
    print("round-trip:")
    rt = create_symbolic_component("engine", no_policy=True)
    rt._load_default_rules()
    rt.evaluate_rules({"energy": 10.0, "memory": 0}, step=1)
    payload = rt.to_json(include_history=True)
    restored = SymbolicReasoningEngine.from_json(payload)
    _check(
        "rules preserved",
        len(restored.rules) == len(rt.rules),
    )
    _check(
        "violations preserved",
        len(restored.violation_history)
        == len(rt.violation_history),
    )
    _check(
        "category preserved",
        restored.violation_history[0].category
        == rt.violation_history[0].category,
    )

    # ---- 11. health --------------------------------------------- #
    print("health:")
    h = health()
    _check("version present", h["version"] == __version__)
    _check(
        "submodules dict",
        isinstance(h["submodules"], dict)
        and "symbolic_reasoning_engine" in h["submodules"],
    )
    _check(
        "yaml_available reported",
        "yaml_available" in h,
    )
    _check(
        "yaml_available matches public flag",
        h["yaml_available"] == YAML_AVAILABLE,
    )
    _check(
        "engine in registered",
        "engine" in h["registered_components"],
    )

    # ---- 12. __dir__ -------------------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
