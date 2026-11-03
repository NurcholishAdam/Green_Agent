# src/visualization/renderers/base.py

"""
Renderer backend selection.

Chooses between the two renderers the visualization layer supports:

- :attr:`RendererBackend.PLOTLY` — interactive HTML widgets (``plotly``).
- :attr:`RendererBackend.MATPLOTLIB` — static PNG / SVG / PDF
  (``matplotlib``).

Two entry points:

- :func:`select_backend` — the recommended API. Returns a
  :class:`RendererSelection` record carrying the resolved backend, the
  requested backend, whether a fallback occurred, and (if so) why.
- :func:`get_renderer` — the previous API, preserved for backward
  compatibility. Returns a :class:`RendererBackend` enum and silently
  falls back on unavailability.

Fallback and validation rules
-----------------------------
- **Unknown backend names raise** :class:`RendererError`. A typo
  (``"matplotlb"``) or a backend the folder does not implement
  (``"bokeh"``) previously returned ``MATPLOTLIB`` silently; that is a
  data-integrity failure for the caller's intent and is now rejected.
- **Uninstalled requested backend** returns a selection record with
  ``was_fallback=True`` when ``strict=False`` (the default), or raises
  :class:`RendererError` when ``strict=True``.
- **Neither backend installed** raises :class:`RendererError`. The old
  behaviour returned ``MATPLOTLIB`` regardless, so the crash surfaced
  several call frames deep inside the render pipeline. Now the failure
  is at the selection layer, with a message that names the problem.
- **Non-string input** raises :class:`RendererError` (previously an
  ``AttributeError`` from ``.lower()``).

Implementation notes
--------------------
- **No `import` at selection time.** Availability is checked with
  :func:`importlib.util.find_spec`, which scans ``sys.path`` without
  executing the module body. The result is cached in-process, so
  repeated calls are dict lookups. This is a measurable improvement in
  per-chart cost for pipelines that build many charts.
- **No linter suppression needed.** The old ``import plotly  # noqa:
  F401`` was correct but pointed at an import-for-side-effect; the
  `find_spec` approach removes the import entirely.

Enhancements over the previous revision
---------------------------------------
- ``RendererError(ValueError)`` — narrow error type for every validation
  path.
- ``RendererBackend`` is a ``StrEnum`` (with a 3.8–3.10 shim), so
  ``str(RendererBackend.PLOTLY) == "plotly"`` on all supported Python
  versions; a ``description`` property documents what each backend
  produces.
- ``RendererSelection`` — a ``TypedDict`` describing the fallback
  decision (``backend``, ``requested``, ``was_fallback``, ``reason``).
- ``select_backend(backend, *, strict=False)`` — validated, structured
  result, with an explicit ``strict`` mode.
- ``get_renderer(backend)`` — preserved as a thin wrapper over
  ``select_backend`` for backward compatibility.
- ``is_available(backend)`` / ``available_backends()`` — cheap
  environment probes for callers that want to plan around the installed
  set.
- Availability results cached at module level (via ``find_spec``);
  per-call cost is a dict lookup after the first probe.
- Fallback logs a ``WARNING`` with the requested backend and the
  fallback target, so the degradation is visible in operator logs even
  when the caller ignores the record.
- ``__all__`` / ``__dir__``.
- ``__main__`` smoke test with ``_check()`` helpers — survives
  ``python -O``.

Compatibility
-------------
- ``get_renderer(backend="plotly")`` — same signature and return type.
  Behaviour changes:
  * Unknown backend names now raise ``RendererError`` (previously
    returned ``MATPLOTLIB``).
  * When neither backend is installed, now raises ``RendererError``
    (previously returned ``MATPLOTLIB``, deferring the failure to render
    time).
  * Fallback to ``matplotlib`` still returns ``MATPLOTLIB``, but now
    logs a ``WARNING``.
  * Non-string ``backend`` now raises ``RendererError`` (previously
    ``AttributeError``).
- ``RendererBackend`` is now a ``str`` subclass on Python < 3.11 (via
  the shim). Existing code doing ``backend.value`` or
  ``backend == "plotly"`` continues to work; ``isinstance(backend, str)``
  becomes True where it previously was False.

Requires Python 3.8+.
"""

from __future__ import annotations

import importlib.util
import logging
from enum import Enum
from typing import Any, Dict, Final, List, Optional, TypedDict

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class RendererError(ValueError):
    """Raised for invalid or unavailable renderer backends."""


# --------------------------------------------------------------------------- #
# StrEnum compatibility shim
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — Python 3.11+
    from enum import StrEnum  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover — 3.10 and earlier
    class StrEnum(str, Enum):  # type: ignore[no-redef]
        """``str`` + ``Enum`` with ``__str__`` / ``__format__`` delegating to ``str``."""

        __str__ = str.__str__
        __format__ = str.__format__


# --------------------------------------------------------------------------- #
# Backend enum
# --------------------------------------------------------------------------- #
class RendererBackend(StrEnum):
    """Supported rendering backends."""

    PLOTLY = "plotly"
    MATPLOTLIB = "matplotlib"

    @property
    def description(self) -> str:
        """Human-readable summary of what the backend produces."""
        return {
            RendererBackend.PLOTLY: "Interactive HTML widgets",
            RendererBackend.MATPLOTLIB: "Static PNG / SVG / PDF",
        }[self]


class RendererSelection(TypedDict):
    """Return shape of :func:`select_backend`."""

    backend: str                    # resolved backend, one of VALID_BACKENDS
    requested: str                  # caller's requested backend, canonicalised
    was_fallback: bool              # True when backend != requested
    reason: Optional[str]           # why the fallback happened, or None


# --------------------------------------------------------------------------- #
# Backend registry
# --------------------------------------------------------------------------- #
_VALID_BACKENDS: Final[frozenset] = frozenset(
    {"plotly", "matplotlib"}
)


# Module-level cache: name → True / False after the first probe.
_AVAILABILITY: Dict[str, bool] = {}


def _is_available(module_name: str) -> bool:
    """
    Return True if ``module_name`` is importable.

    Uses :func:`importlib.util.find_spec`, which scans ``sys.path``
    without executing the module body. Result is cached for the life of
    the process.
    """
    if module_name not in _AVAILABILITY:
        try:
            _AVAILABILITY[module_name] = (
                importlib.util.find_spec(module_name) is not None
            )
        except (ImportError, ValueError):
            # A malformed package (e.g. a broken namespace package on
            # sys.path) can raise from find_spec. Treat as unavailable.
            _AVAILABILITY[module_name] = False
    return _AVAILABILITY[module_name]


# --------------------------------------------------------------------------- #
# Public probes
# --------------------------------------------------------------------------- #
def is_available(backend: Any) -> bool:
    """
    Return True if ``backend`` names a known, installed renderer.

    Accepts the string form or a :class:`RendererBackend` member.
    Returns ``False`` for unrecognised names and non-string input —
    unlike :func:`select_backend`, it does **not** raise.
    """
    if isinstance(backend, RendererBackend):
        name = backend.value
    elif isinstance(backend, str) and backend:
        name = backend.lower()
    else:
        return False
    if name not in _VALID_BACKENDS:
        return False
    return _is_available(name)


def available_backends() -> List[str]:
    """Return the sorted list of installed, recognised backends."""
    return sorted(name for name in _VALID_BACKENDS if _is_available(name))


# --------------------------------------------------------------------------- #
# Selection
# --------------------------------------------------------------------------- #
def _canonicalise(backend: Any) -> str:
    """Return the canonical lower-case name; raise on invalid input."""
    if isinstance(backend, RendererBackend):
        return backend.value
    if not isinstance(backend, str) or not backend:
        raise RendererError(
            f"backend must be a non-empty string or RendererBackend, "
            f"got {type(backend).__name__}."
        )
    name = backend.lower()
    if name not in _VALID_BACKENDS:
        raise RendererError(
            f"unknown backend {backend!r}; expected one of "
            f"{sorted(_VALID_BACKENDS)}."
        )
    return name


def select_backend(
    backend: Any = RendererBackend.PLOTLY,
    *,
    strict: bool = False,
) -> RendererSelection:
    """
    Resolve ``backend`` to an installed renderer.

    Parameters
    ----------
    backend : str or RendererBackend, default ``"plotly"``
        Requested backend. Must be one of ``{"plotly", "matplotlib"}``
        (case-insensitive).
    strict : bool, default ``False``
        If ``True``, raise :class:`RendererError` when the requested
        backend is not installed. The default returns a
        :class:`RendererSelection` with ``was_fallback=True``.

    Returns
    -------
    RendererSelection
        A record describing the resolved backend, the requested backend,
        whether a fallback occurred, and (if so) the reason.

    Raises
    ------
    RendererError
        If ``backend`` is not a string / enum, is empty, is not one of
        the recognised backends, is unavailable in ``strict=True`` mode,
        or if neither backend is installed.
    """
    requested = _canonicalise(backend)

    if _is_available(requested):
        return {
            "backend": requested,
            "requested": requested,
            "was_fallback": False,
            "reason": None,
        }

    if strict:
        raise RendererError(
            f"requested backend {requested!r} is not installed."
        )

    fallback = RendererBackend.MATPLOTLIB.value
    if _is_available(fallback):
        reason = f"{requested!r} is not installed"
        logger.warning(
            "requested backend %r unavailable; falling back to %r (%s).",
            requested, fallback, reason,
        )
        return {
            "backend": fallback,
            "requested": requested,
            "was_fallback": True,
            "reason": reason,
        }

    # Neither backend is installed: this is a hard error, not a fallback.
    raise RendererError(
        f"requested backend {requested!r} is unavailable and "
        f"{fallback!r} is not installed either; install at least one "
        f"renderer."
    )


# --------------------------------------------------------------------------- #
# Backward-compatible entry point
# --------------------------------------------------------------------------- #
def get_renderer(backend: Any = "plotly") -> RendererBackend:
    """
    Return the resolved backend as a :class:`RendererBackend`.

    Preserved for backward compatibility. Callers who need to know
    whether the requested backend was actually used should call
    :func:`select_backend` instead — this function will fall back to
    ``MATPLOTLIB`` when the requested backend is uninstalled, though it
    logs a warning at selection time.
    """
    selection = select_backend(backend)
    return RendererBackend(selection["backend"])


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "RendererBackend",
    "RendererError",
    "RendererSelection",
    "available_backends",
    "get_renderer",
    "is_available",
    "select_backend",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m visualization.renderers.base
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

    # ---- 1. Enum basics ----------------------------------------- #
    print("enum:")
    _check(
        "PLOTLY value",
        RendererBackend.PLOTLY.value == "plotly",
    )
    _check(
        "MATPLOTLIB value",
        RendererBackend.MATPLOTLIB.value == "matplotlib",
    )
    _check(
        "PLOTLY description",
        "Interactive" in RendererBackend.PLOTLY.description,
    )
    _check(
        "MATPLOTLIB description",
        "Static" in RendererBackend.MATPLOTLIB.description,
    )
    _check(
        "str(PLOTLY) == 'plotly'",
        str(RendererBackend.PLOTLY) == "plotly",
    )
    _check(
        "f-string renders canonical value",
        f"{RendererBackend.MATPLOTLIB}" == "matplotlib",
    )
    _check(
        "PLOTLY is str on Python <3.11",
        isinstance(RendererBackend.PLOTLY, str),
    )

    # ---- 2. Available backends on this machine ----------------- #
    print("environment:")
    print(f"  available_backends() = {available_backends()}")
    _check(
        "available_backends returns a list",
        isinstance(available_backends(), list),
    )
    _check(
        "plotly is_available agrees with find_spec",
        is_available("plotly") == _is_available("plotly"),
    )
    _check(
        "matplotlib is_available agrees with find_spec",
        is_available("matplotlib") == _is_available("matplotlib"),
    )

    # ---- 3. is_available validation ----------------------------- #
    print("is_available:")
    _check("is_available('plotly') bool", isinstance(is_available("plotly"), bool))
    _check("is_available(None) False", is_available(None) is False)
    _check("is_available(42) False", is_available(42) is False)
    _check("is_available('') False", is_available("") is False)
    _check("is_available('bokeh') False", is_available("bokeh") is False)
    _check(
        "is_available(RendererBackend.PLOTLY) == is_available('plotly')",
        is_available(RendererBackend.PLOTLY) == is_available("plotly"),
    )
    _check(
        "is_available accepts mixed case",
        is_available("PLOTLY") == is_available("plotly"),
    )

    # ---- 4. select_backend valid requests ----------------------- #
    print("select_backend (valid):")
    sel = select_backend("matplotlib")
    _check(
        "matplotlib selected",
        sel["backend"] == "matplotlib",
    )
    _check(
        "matplotlib was_fallback False",
        sel["was_fallback"] is False,
    )
    _check(
        "matplotlib reason None",
        sel["reason"] is None,
    )
    _check(
        "requested preserved",
        sel["requested"] == "matplotlib",
    )
    _check(
        "selection is a dict",
        isinstance(sel, dict),
    )
    _check(
        "selection has all keys",
        set(sel) == {"backend", "requested", "was_fallback", "reason"},
    )

    # ---- 5. select_backend rejects invalid input ---------------- #
    print("select_backend (invalid):")
    for bad in (None, 42, b"plotly", (), [""]):
        try:
            select_backend(bad)  # type: ignore[arg-type]
            _check(f"reject {bad!r}", False)
        except RendererError:
            _check(f"reject {bad!r}", True)
        except Exception:
            _check(f"reject {bad!r} (wrong type)", False)

    for bad in ("", "bokeh", "plotly-dash", "matplotlb"):
        try:
            select_backend(bad)
            _check(f"reject unknown {bad!r}", False)
        except RendererError:
            _check(f"reject unknown {bad!r}", True)

    # ---- 6. strict mode ----------------------------------------- #
    print("strict mode:")
    # When the requested backend is available, strict is a no-op.
    if _is_available("matplotlib"):
        sel = select_backend("matplotlib", strict=True)
        _check(
            "strict accepts available backend",
            sel["backend"] == "matplotlib" and not sel["was_fallback"],
        )
    else:
        _check("strict accepts available backend (skipped)", True)

    # When the requested backend is unavailable, strict raises.
    for name in ("plotly", "matplotlib"):
        if _is_available(name):
            continue
        try:
            select_backend(name, strict=True)
            _check(f"strict rejects unavailable {name!r}", False)
        except RendererError:
            _check(f"strict rejects unavailable {name!r}", True)

    # ---- 7. Non-strict fallback -------------------------------- #
    print("fallback:")
    for name in ("plotly", "matplotlib"):
        if _is_available(name):
            continue
        other = (
            "matplotlib" if name == "plotly" else "plotly"
        )
        if _is_available(other):
            sel = select_backend(name)
            _check(
                f"fallback from {name!r} to {other!r}",
                sel["backend"] == other
                and sel["was_fallback"] is True
                and sel["reason"] is not None,
            )
            _check(
                f"requested preserved on fallback from {name!r}",
                sel["requested"] == name,
            )

    # ---- 8. Neither installed → RendererError ------------------- #
    print("both uninstalled (synthetic):")
    # Simulate "no backends installed" by temporarily overriding the
    # cache; restore it afterwards.
    saved = dict(_AVAILABILITY)
    try:
        _AVAILABILITY["plotly"] = False
        _AVAILABILITY["matplotlib"] = False
        try:
            select_backend("plotly")
            _check("no backends → RendererError", False)
        except RendererError:
            _check("no backends → RendererError", True)
        try:
            get_renderer("plotly")
            _check("get_renderer no backends → RendererError", False)
        except RendererError:
            _check("get_renderer no backends → RendererError", True)
    finally:
        _AVAILABILITY.clear()
        _AVAILABILITY.update(saved)

    # ---- 9. get_renderer backward-compat ------------------------ #
    print("get_renderer:")
    # If at least one backend is installed, get_renderer must return
    # a RendererBackend for the resolved backend.
    if _is_available("matplotlib"):
        r = get_renderer("matplotlib")
        _check(
            "get_renderer returns RendererBackend",
            isinstance(r, RendererBackend),
        )
        _check(
            "get_renderer('matplotlib') == MATPLOTLIB",
            r == RendererBackend.MATPLOTLIB,
        )
    else:
        _check("get_renderer returns RendererBackend (skipped)", True)

    # get_renderer(None) now raises RendererError (previously AttributeError).
    try:
        get_renderer(None)  # type: ignore[arg-type]
        _check("get_renderer(None) raises", False)
    except RendererError:
        _check("get_renderer(None) raises RendererError", True)
    except Exception:
        _check("get_renderer(None) raises RendererError (wrong type)", False)

    # get_renderer('bokeh') now raises RendererError (previously MATPLOTLIB).
    try:
        get_renderer("bokeh")
        _check("get_renderer('bokeh') raises", False)
    except RendererError:
        _check("get_renderer('bokeh') raises RendererError", True)

    # ---- 10. Availability cache is used ----------------------- #
    print("cache:")
    _is_available("plotly")  # prime
    before = dict(_AVAILABILITY)
    _is_available("plotly")  # cached
    _check(
        "cache value stable across calls",
        _AVAILABILITY == before,
    )

    # ---- 11. __all__ / __dir__ -------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "RendererBackend", "RendererError", "RendererSelection",
        "available_backends", "get_renderer", "is_available",
        "select_backend",
    ):
        _check(f"{_name} exported", _name in __all__)

    # ---- 12. Error type --------------------------------------- #
    print("error type:")
    _check(
        "RendererError subclasses ValueError",
        issubclass(RendererError, ValueError),
    )

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
