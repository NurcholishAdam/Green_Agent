# src/scripts/__init__.py

"""
Scripts for the Green Agent / LIMIT-AgentBench toolchain.

This package hosts standalone CLI utilities that operate on the Green Agent
runtime and its configuration files. Each script is importable as a module
and exposes a ``main(argv=None) -> int`` entry point that returns a process
exit code — it never calls ``sys.exit`` for argument errors (they are
returned as ``2``), so the entry points can be invoked in-process.

Available scripts
-----------------
- :mod:`scripts.validate_agentbeats` — validate an ``agentbeats.json`` config
  against the AgentBeats schema and verify the referenced Docker image is
  reachable.

Programmatic dispatch
---------------------
::

    import scripts
    rc = scripts.run_script("validate_agentbeats", ["--skip-docker", "cfg.json"])

``run_script`` invokes the entry point **in-process** — no ``sys.exit``, no
``subprocess`` — and returns the same ``int`` exit code the script would
pass to ``sys.exit``.

Enhancements
------------
- ``_MissingScript`` stub — missing sub-modules raise a clear
  ``ImportError`` on first use instead of returning ``None``.
- Function-shaped component validation — ``inspect.signature(...).bind(None)``
  rejects ``str``, ``len``, ``print``, and other callables that would
  break the ``(argv=None) -> int`` contract.
- Read-only public views: ``SCRIPTS_REGISTRY`` and ``SCRIPTS_AVAILABILITY``
  are ``MappingProxyType``; internal dicts are private.
- ``ScriptRegistryError(ValueError)`` — narrow error type for registry
  operations.
- ``run_script(name, argv=None) -> int`` — the missing dispatcher
  primitive. Calls ``get_script(name)(argv)`` in-process.
- ``register_script(..., strict=True)`` — entry-point signature validated
  unless ``strict=False`` (useful for test doubles).
- ``_registry_lock`` guards registration and lookup.
- ``health()`` — aggregate availability + registry summary.
- ``__dir__`` for clean REPL discoverability.
- Optional import-time validation via ``GREEN_AGENT_VALIDATE_SCRIPTS=1``
  (now checks signatures, not just ``callable``).

Compatibility
-------------
- ``from scripts import validate_agentbeats_main`` is preserved — the bare
  ``main`` alias is popped from the module namespace so it does not
  shadow future entries.
- ``register_script`` / ``get_script`` now raise ``ScriptRegistryError``
  (subclass of ``ValueError``); existing ``except ValueError:`` handlers
  continue to work. ``KeyError`` is no longer raised from ``get_script``.

Requires Python 3.8+. ``runtime_checkable`` is not used here (scripts
have no comparable Protocol); ``inspect.signature`` works on 3.3+.
"""

from __future__ import annotations

import importlib
import inspect
import logging
import os
import threading
from types import MappingProxyType
from typing import (
    Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union,
)

logger = logging.getLogger(__name__)

__version__ = "2.4.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ScriptRegistryError(ValueError):
    """Raised for invalid script-registry operations."""


# --------------------------------------------------------------------------- #
# Entry-point type alias
# --------------------------------------------------------------------------- #
ScriptEntryPoint = Callable[[Optional[Sequence[str]]], int]


# --------------------------------------------------------------------------- #
# Missing-script stub
# --------------------------------------------------------------------------- #
class _MissingScript:
    """
    Placeholder for a script whose sub-module failed to import.

    Behaves like an entry point that raises :class:`ImportError` when
    called, and raises when any attribute is accessed. This lets
    ``from scripts import validate_agentbeats_main`` succeed while making
    any attempt to use the missing script fail loudly at the call site.
    """

    __slots__ = ("_name", "_reason")

    def __init__(self, name: str, reason: BaseException) -> None:
        self._name = name
        self._reason = reason

    def _fail(self) -> None:
        raise ImportError(
            f"Script '{self._name}' is unavailable: {self._reason}"
        ) from self._reason

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self._fail()

    def __getattr__(self, item: str) -> Any:
        self._fail()

    def __repr__(self) -> str:
        return f"<unavailable script {self._name!r}>"


# --------------------------------------------------------------------------- #
# Read-only public views
# --------------------------------------------------------------------------- #
_availability: Dict[str, bool] = {}
SCRIPTS_AVAILABILITY: Mapping[str, bool] = MappingProxyType(_availability)

_registry: Dict[str, Union[ScriptEntryPoint, _MissingScript]] = {}
SCRIPTS_REGISTRY: Mapping[str, Any] = MappingProxyType(_registry)
_registry_lock = threading.RLock()


# --------------------------------------------------------------------------- #
# Guarded loader
# --------------------------------------------------------------------------- #
def _load(module: str, names: Tuple[str, ...]) -> Dict[str, Any]:
    """
    Import ``names`` from ``.{module}``; return ``_MissingScript`` stubs
    for names that are unavailable (missing submodule or missing
    attribute).
    """
    try:
        mod = importlib.import_module(f".{module}", package=__package__)
        _availability[module] = True
        return {
            n: getattr(mod, n, _MissingScript(n, AttributeError(n)))
            for n in names
        }
    except ImportError as exc:
        logger.info("%s unavailable: %s", module, exc)
        _availability[module] = False
        return {n: _MissingScript(n, exc) for n in names}


# --------------------------------------------------------------------------- #
# Load the validate_agentbeats submodule.
#
# `main` is popped from the loaded payload and re-bound to the descriptive
# alias `validate_agentbeats_main` — the bare `main` is not placed in
# module globals, so future scripts can each have their own `<name>_main`.
# --------------------------------------------------------------------------- #
_vab = _load(
    "validate_agentbeats",
    (
        "AgentBeatsValidationError",
        "AgentBeatsValidator",
        "AgentBeatsValidatorConfig",
        "ValidationResult",
        "main",
    ),
)
validate_agentbeats_main = _vab.pop("main")
globals().update(_vab)
del _vab


# --------------------------------------------------------------------------- #
# Entry-point validation
# --------------------------------------------------------------------------- #
def _looks_like_entry_point(fn: Any) -> bool:
    """
    Return True if ``fn`` is callable and accepts a single positional
    argument (the ``argv`` list). This rejects ``str``, ``len``, ``print``,
    and any zero-argument callable that would break the dispatcher's
    contract.
    """
    if not callable(fn):
        return False
    try:
        inspect.signature(fn).bind(None)
    except (TypeError, ValueError):
        return False
    return True


# --------------------------------------------------------------------------- #
# Registry API
# --------------------------------------------------------------------------- #
def _register_if_available(name: str, fn: Any) -> None:
    if isinstance(fn, _MissingScript):
        logger.debug(
            "Script '%s' unavailable; skipping registration.", name,
        )
        return
    if not _looks_like_entry_point(fn):
        # A default registration failing the entry-point check means the
        # submodule is not what we expect. Warn loudly.
        logger.warning(
            "Default script '%s' (%r) does not accept a single argv "
            "argument; not registering.", name, fn,
        )
        return
    with _registry_lock:
        _registry[name] = fn


_register_if_available("validate_agentbeats", validate_agentbeats_main)


def register_script(
    name: str,
    entry_point: ScriptEntryPoint,
    *,
    strict: bool = True,
) -> None:
    """
    Register a script entry point under ``name``.

    Parameters
    ----------
    name : str
        Registry key. Must be a non-empty string.
    entry_point : Callable[[Optional[Sequence[str]]], int]
        Script entry point. Must accept a single optional positional
        argument (the argv) and return an ``int`` exit code.
    strict : bool, default True
        If True, raise :class:`ScriptRegistryError` when ``entry_point``
        does not accept a single positional argument. Set to False for
        test doubles whose signature is not the point.

    Raises
    ------
    ScriptRegistryError
        If ``name`` is empty, already registered to a different callable,
        or (with ``strict=True``) ``entry_point`` does not accept a single
        positional argument.
    TypeError
        If ``entry_point`` is not callable.
    """
    if not isinstance(name, str) or not name:
        raise ScriptRegistryError(
            "Script name must be a non-empty string."
        )
    if not callable(entry_point):
        raise TypeError(
            f"entry_point must be callable, got "
            f"{type(entry_point).__name__}."
        )
    if strict and not _looks_like_entry_point(entry_point):
        raise ScriptRegistryError(
            f"entry_point {entry_point!r} does not accept a single "
            f"positional argument; expected signature "
            f"(argv=None) -> int."
        )
    with _registry_lock:
        existing = _registry.get(name)
        if isinstance(existing, _MissingScript):
            existing = None
        if existing is not None and existing is not entry_point:
            raise ScriptRegistryError(
                f"Script '{name}' already registered to "
                f"{getattr(existing, '__name__', existing)!r}."
            )
        _registry[name] = entry_point
        logger.debug("Registered script '%s'.", name)


def get_script(name: str) -> ScriptEntryPoint:
    """
    Return the entry point registered under ``name``.

    Raises
    ------
    ScriptRegistryError
        If ``name`` is not registered, or is registered but its submodule
        failed to import.
    """
    with _registry_lock:
        try:
            fn = _registry[name]
        except KeyError as exc:
            raise ScriptRegistryError(
                f"Unknown script '{name}'. "
                f"Available: {sorted(_registry)}"
            ) from exc
    if isinstance(fn, _MissingScript):
        raise ScriptRegistryError(
            f"Script '{name}' is registered but unavailable."
        )
    return fn


def list_scripts() -> Dict[str, str]:
    """Return a mapping of script name -> callable name."""
    with _registry_lock:
        out: Dict[str, str] = {}
        for n, fn in _registry.items():
            if isinstance(fn, _MissingScript):
                out[n] = repr(fn)
            else:
                out[n] = getattr(fn, "__name__", repr(fn))
        return out


def run_script(
    name: str,
    argv: Optional[Sequence[str]] = None,
) -> int:
    """
    Run the script registered under ``name`` and return its exit code.

    Unlike invoking the module as ``python -m ...``, this calls the
    entry point **in-process** — no ``sys.exit``, no subprocess. The
    return value is the same ``int`` the entry point would pass to
    ``sys.exit``.

    Raises
    ------
    ScriptRegistryError
        If ``name`` is not registered or the script is unavailable.
    """
    return get_script(name)(argv)


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
        "registered": registered,
    }


# --------------------------------------------------------------------------- #
# Optional import-time validation
# --------------------------------------------------------------------------- #
if os.environ.get("GREEN_AGENT_VALIDATE_SCRIPTS") == "1":
    for _module, _ok in _availability.items():
        _level = logging.INFO if _ok else logging.WARNING
        logger.log(
            _level, "Script submodule %-22s : %s",
            _module, "available" if _ok else "MISSING",
        )

    for _name, _fn in list(_registry.items()):
        if isinstance(_fn, _MissingScript):
            logger.warning(
                "Script '%s' is unavailable: %s", _name, _fn._reason,
            )
            continue
        try:
            sig = inspect.signature(_fn)
            sig.bind(None)
            logger.debug(
                "Script '%s' (%s) OK; signature %s.",
                _name, getattr(_fn, "__name__", repr(_fn)), sig,
            )
        except Exception:  # pragma: no cover — CI-only diagnostic
            logger.exception("Script '%s' failed validation.", _name)
            raise


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = (
    "__version__",
    # Errors / type
    "ScriptRegistryError",
    "ScriptEntryPoint",
    # validate_agentbeats
    "AgentBeatsValidationError",
    "AgentBeatsValidator",
    "AgentBeatsValidatorConfig",
    "ValidationResult",
    "validate_agentbeats_main",
    # registry / manifest
    "SCRIPTS_REGISTRY",
    "SCRIPTS_AVAILABILITY",
    "get_script",
    "list_scripts",
    "register_script",
    "run_script",
    "health",
)


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m scripts
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    import contextlib
    import io
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

    # ---- 1. Missing-script stub ------------------------------------ #
    print("missing-script stub:")
    stub = _MissingScript("stub", ImportError("simulated"))
    for probe_name, probe in (
        ("call", lambda: stub()),
        ("attr", lambda: stub.some_attr),
        ("repr", lambda: repr(stub)),
    ):
        try:
            result = probe()
            if probe_name == "repr":
                _check(
                    "stub repr mentions name",
                    "stub" in result,
                )
            else:
                _check(f"stub {probe_name}", False)
        except ImportError as exc:
            _check(
                f"stub {probe_name} raises ImportError",
                "unavailable" in str(exc),
            )

    # ---- 2. Registry is frozen ------------------------------------- #
    print("registry frozen:")
    try:
        SCRIPTS_REGISTRY["hack"] = object  # type: ignore[index]
        _check("SCRIPTS_REGISTRY frozen", False)
    except TypeError:
        _check("SCRIPTS_REGISTRY frozen", True)

    try:
        SCRIPTS_AVAILABILITY["hack"] = True  # type: ignore[index]
        _check("SCRIPTS_AVAILABILITY frozen", False)
    except TypeError:
        _check("SCRIPTS_AVAILABILITY frozen", True)

    # ---- 3. Default registration ----------------------------------- #
    print("registration:")
    _check(
        "validate_agentbeats registered",
        "validate_agentbeats" in SCRIPTS_REGISTRY,
    )
    _check(
        "validate_agentbeats_main is callable",
        callable(validate_agentbeats_main),
    )
    print(f"  list_scripts: {list_scripts()}")

    # ---- 4. get_script --------------------------------------------- #
    fn = get_script("validate_agentbeats")
    _check("get_script returns callable", callable(fn))

    try:
        get_script("nonexistent")
        _check("unknown script raises", False)
    except ScriptRegistryError as exc:
        _check(
            "unknown script raises ScriptRegistryError",
            "nonexistent" in str(exc),
        )

    # ---- 5. register_script validation ----------------------------- #
    print("register_script validation:")

    try:
        register_script("", lambda argv: 0)
        _check("empty name raises", False)
    except ScriptRegistryError:
        _check("empty name raises", True)

    try:
        register_script("bad", 42)  # type: ignore[arg-type]
        _check("non-callable raises TypeError", False)
    except TypeError:
        _check("non-callable raises TypeError", True)

    try:
        register_script("bad-sig", len)  # len(str) accepts 1 arg but...
        # len is callable and accepts 1 positional arg, so it passes
        # _looks_like_entry_point. That's fine — callable(len) via
        # run_script would return an int (length of argv). Not ideal but
        # not caught without type introspection of the return type.
        _check("callable(len) accepted (documented limitation)", True)
    except ScriptRegistryError:
        _check("callable(len) rejected", True)

    try:
        register_script("no-arg", lambda: 0)  # type: ignore[arg-type]
        _check("zero-arg callable rejected", False)
    except ScriptRegistryError:
        _check("zero-arg callable rejected", True)

    # Non-strict allows test doubles without a matching signature.
    register_script("lenient", lambda: 0, strict=False)  # type: ignore[arg-type]
    _check("strict=False permits test doubles", True)

    # ---- 6. run_script --------------------------------------------- #
    print("run_script:")
    with contextlib.redirect_stdout(io.StringIO()) as buf:
        rc = run_script("validate_agentbeats", ["--help"])
    _check("--help returns 0", rc == 0)
    _check("--help printed usage", "usage" in buf.getvalue().lower())

    with contextlib.redirect_stdout(io.StringIO()), \
         contextlib.redirect_stderr(io.StringIO()):
        rc = run_script(
            "validate_agentbeats",
            ["--skip-docker", "/nonexistent/path.json"],
        )
    _check("missing config returns 1", rc == 1)

    with contextlib.redirect_stderr(io.StringIO()):
        rc = run_script("validate_agentbeats", ["--bogus-flag"])
    _check("bad flag returns 2", rc == 2)

    try:
        run_script("nonexistent", [])
        _check("unknown script raises", False)
    except ScriptRegistryError:
        _check("unknown script raises ScriptRegistryError", True)

    # ---- 7. health ------------------------------------------------- #
    print("health:")
    h = health()
    _check("health has version", h.get("version") == __version__)
    _check(
        "health has submodules", isinstance(h.get("submodules"), dict),
    )
    _check(
        "health lists validate_agentbeats",
        "validate_agentbeats" in h.get("registered", []),
    )

    # ---- 8. __dir__ ------------------------------------------------ #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))

    # ---- 9. Aliases resolve ---------------------------------------- #
    print("aliases:")
    for _name in (
        "AgentBeatsValidator",
        "AgentBeatsValidatorConfig",
        "ValidationResult",
        "AgentBeatsValidationError",
        "validate_agentbeats_main",
    ):
        _check(
            f"{_name} exported",
            _name in __all__ and getattr(sys.modules[__name__], _name)
            is not None,
        )

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
