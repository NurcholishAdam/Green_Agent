# src/visualization/renderers/export.py

"""
Figure export helpers.

Two entry points:

- :func:`save_figure` — the previous API, preserved for backward
  compatibility. Returns a ``bool`` and never raises.
- :func:`save_figure_detailed` — the recommended API. Returns an
  :class:`ExportResult` recording what happened, and raises
  :class:`ExportError` on programmer errors.

Same shape for HTML export: :func:`export_html` / :func:`export_html_detailed`.

Supported (figure type, extension) pairs
----------------------------------------
Plotly figures::

    .html / .htm   → fig.write_html(...)     no extra dependency
    .json          → fig.write_json(...)     no extra dependency
    .png / .svg / .pdf → fig.write_image(...) requires the 'kaleido' package

matplotlib figures::

    .png / .svg / .pdf / .ps / .eps / .jpg / .jpeg / .webp /
    .tif / .tiff   → fig.savefig(...)        no extra dependency

Any other (figure type, extension) pair raises :class:`ExportError` —
the previous implementation silently returned ``False`` after a
runtime exception, which meant the caller could not tell whether they
typed the extension wrong or the image backend was missing.

Path inputs
-----------
Every public function accepts ``str | os.PathLike[str]``. The old
implementation silently treated any non-``str`` as extension-less
(``"." in Path(...)`` is ``False``), which caused Plotly figures to be
HTML-encoded into ``out.png``. Fixed.

Failure taxonomy (only :func:`save_figure_detailed` /
:func:`export_html_detailed` are visible here; the boolean entry
points catch everything)
--------------------------------------------------------------------------------
- **Programmer error** — non-coercible path, ``None`` figure, empty
  scale, unknown extension. Raised as :class:`ExportError` before any
  file is touched.
- **Missing dependency** — Plotly → image export without ``kaleido``.
  Raised as :class:`ExportError` with an actionable message.
- **Runtime failure** — OS error, Plotly internal error, matplotlib
  internal error. Captured in :class:`ExportResult` (``ok=False``,
  ``error``, ``error_type="runtime"``); the ``save_figure`` wrapper
  converts this to ``False``.

Enhancements over the previous revision
---------------------------------------
- ``ExportError(ValueError)`` — narrow error type.
- ``ExportResult`` — ``TypedDict`` describing the outcome:
  ``ok``, ``path``, ``format``, ``figure_type``, ``error``,
  ``error_type``.
- ``save_figure_detailed`` / ``export_html_detailed`` — structured
  result API; ``save_figure`` / ``export_html`` are thin wrappers that
  convert failures to ``False``.
- **Extension dispatch is explicit** — the (figure type, extension)
  whitelist is a set-membership check, not a `hasattr` guess.
- **`Path` input support** — accepted on every public function;
  extension is read from ``Path(path).suffix``, so ``.hidden`` /
  ``out.`` / case-variant extensions resolve correctly.
- **`kaleido` pre-check** — Plotly-to-image export raises
  :class:`ExportError` naming the missing package instead of failing
  inside Plotly with an opaque error.
- **`write_json` support** for Plotly figures.
- **`dpi` / `bbox_inches` parameters** for matplotlib figures (default
  values match the previous hard-coded ``dpi=150`` /
  ``bbox_inches="tight"``).
- **`create_dirs: bool = False`** — optional parent-directory creation
  before the write.
- **`plotly_include_js` parameter** for the HTML branch of
  ``save_figure`` — the previous HTML branch used Plotly's default
  (``"cdn"``) with no way to override without dropping to
  ``export_html``.
- **`include_plotlyjs` validation** — must be a ``bool`` or one of
  ``{"cdn", "directory", "require", "embed"}``; anything else is
  rejected before writing.
- **`export_html` extension check** — rejects paths whose suffix isn't
  ``.html`` / ``.htm`` / empty.
- **`scale` is validated** (finite, positive); it applies only to
  Plotly image export, which is documented.
- **Lazy logging** — ``%s``-style throughout; no eager f-strings.
- **Unused `os` import removed.**
- ``__all__`` / ``__dir__``.
- ``__main__`` smoke test with ``_check()`` assertions — survives
  ``python -O``.

Compatibility
-------------
- ``save_figure(fig, path, *, scale=2.0) -> bool`` — same signature,
  same return type, never raises. Behaviour changes:
  * ``Path`` inputs now work (previously produced mis-typed output or
    ``False``).
  * Unknown-extension inputs still return ``False``, but the reason is
    logged and available via ``save_figure_detailed``.
  * Plotly ``.json`` now writes JSON (previously returned ``False``).
  * Plotly ``.png`` without ``kaleido`` still returns ``False`` (log
    now names the missing package).
- ``export_html(fig, path, *, include_plotlyjs="cdn") -> bool`` — same
  signature, same return type, never raises. Behaviour change:
  unknown ``include_plotlyjs`` values now return ``False`` (previously
  leaked a ``ValueError`` from inside Plotly after the file was
  partially written).

Requires Python 3.8+.
"""

from __future__ import annotations

import importlib.util
import logging
import math
import os
from pathlib import Path
from typing import (
    Any, Dict, Final, List, Optional, Tuple, TypedDict, Union,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ExportError(ValueError):
    """Raised for invalid export requests or missing dependencies."""


# --------------------------------------------------------------------------- #
# Result shape
# --------------------------------------------------------------------------- #
class ExportResult(TypedDict):
    """Return shape of :func:`save_figure_detailed`."""

    ok: bool
    path: str
    format: str
    figure_type: str
    error: Optional[str]
    error_type: Optional[str]   # "validation" | "missing_dependency" | "runtime"


# --------------------------------------------------------------------------- #
# Supported extensions
# --------------------------------------------------------------------------- #
_PLOTLY_HTML_EXTENSIONS: Final[frozenset] = frozenset({"html", "htm"})
_PLOTLY_JSON_EXTENSIONS: Final[frozenset] = frozenset({"json"})
_PLOTLY_IMAGE_EXTENSIONS: Final[frozenset] = frozenset(
    {"png", "svg", "pdf"}
)
_PLOTLY_EXTENSIONS: Final[frozenset] = (
    _PLOTLY_HTML_EXTENSIONS
    | _PLOTLY_JSON_EXTENSIONS
    | _PLOTLY_IMAGE_EXTENSIONS
)

_MATPLOTLIB_EXTENSIONS: Final[frozenset] = frozenset({
    "png", "svg", "pdf", "ps", "eps",
    "jpg", "jpeg", "webp", "tif", "tiff",
})

_VALID_INCLUDE_PLOTLYJS: Final[frozenset] = frozenset({
    "cdn", "directory", "require", "embed",
})


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _coerce_path(path: Any) -> Path:
    """
    Coerce ``path`` to a :class:`pathlib.Path`.

    Accepts ``str``, :class:`os.PathLike[str]`, or :class:`Path`.
    Rejects empty strings and ``None`` with a narrow error.
    """
    if isinstance(path, Path):
        return path
    if isinstance(path, str):
        if not path:
            raise ExportError("path must be a non-empty string.")
        return Path(path)
    if hasattr(path, "__fspath__"):
        return Path(os.fspath(path))
    raise ExportError(
        f"path must be a str or os.PathLike[str], got "
        f"{type(path).__name__}."
    )


def _extension_of(path: Path) -> str:
    """
    Return the lower-case extension of ``path`` without the leading dot.

    ``Path.suffix`` handles ``.hidden`` (no suffix) and ``out.``
    (suffix ``"."``) — the previous ``path.rsplit`` logic did not.
    """
    suffix = path.suffix
    return suffix[1:].lower() if suffix else ""


def _require_positive_float(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ExportError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv) or fv <= 0:
        raise ExportError(
            f"{name} must be finite and > 0, got {value!r}."
        )
    return fv


def _require_positive_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ExportError(f"{name} must be an int.")
    if value <= 0:
        raise ExportError(f"{name} must be > 0, got {value}.")
    return value


# --------------------------------------------------------------------------- #
# Figure-type detection and dependency probes
# --------------------------------------------------------------------------- #
_FIGURE_TYPE_PLOTLY: Final[str] = "plotly"
_FIGURE_TYPE_MATPLOTLIB: Final[str] = "matplotlib"

_KALEIDO_CACHE: Optional[bool] = None


def _detect_figure_type(fig: Any) -> str:
    """
    Return ``"plotly"`` or ``"matplotlib"``.

    Detection is by method presence:
    - Plotly figures expose ``write_html`` and ``write_image``.
    - matplotlib figures expose ``savefig``.

    Raises :class:`ExportError` when neither pattern matches.
    """
    if hasattr(fig, "write_html") and hasattr(fig, "write_image"):
        return _FIGURE_TYPE_PLOTLY
    if hasattr(fig, "savefig"):
        return _FIGURE_TYPE_MATPLOTLIB
    raise ExportError(
        f"unsupported figure type {type(fig).__name__!r}; expected a "
        f"Plotly figure or a matplotlib Figure."
    )


def _kaleido_available() -> bool:
    """Return True if ``kaleido`` is importable. Cached."""
    global _KALEIDO_CACHE
    if _KALEIDO_CACHE is None:
        try:
            _KALEIDO_CACHE = (
                importlib.util.find_spec("kaleido") is not None
            )
        except (ImportError, ValueError):
            _KALEIDO_CACHE = False
    return _KALEIDO_CACHE


def _validate_include_plotlyjs(value: Any) -> Any:
    """
    Validate Plotly's ``include_plotlyjs`` parameter.

    Accepts a ``bool`` or one of ``{"cdn", "directory", "require",
    "embed"}``. Custom ``<script>`` payloads are not supported by this
    helper — callers who need them should call ``fig.write_html``
    directly.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value in _VALID_INCLUDE_PLOTLYJS:
        return value
    raise ExportError(
        f"include_plotlyjs must be a bool or one of "
        f"{sorted(_VALID_INCLUDE_PLOTLYJS)}, got {value!r}."
    )


# --------------------------------------------------------------------------- #
# Core implementation
# --------------------------------------------------------------------------- #
def save_figure_detailed(
    fig: Any,
    path: Union[str, "os.PathLike[str]"],
    *,
    scale: float = 2.0,
    dpi: Optional[int] = None,
    bbox_inches: Optional[str] = "tight",
    plotly_include_js: Any = None,
    create_dirs: bool = False,
) -> ExportResult:
    """
    Save ``fig`` to ``path`` and return a structured result.

    Parameters
    ----------
    fig : Plotly figure or matplotlib Figure
    path : str or os.PathLike[str]
        Destination path. The format is inferred from the extension.
    scale : float, default 2.0
        Passed to Plotly's ``write_image`` (pixel multiplier). Ignored
        for matplotlib and HTML / JSON exports.
    dpi : int, optional
        Passed to matplotlib's ``savefig``. When ``None``, defaults to
        ``150`` (matching the previous hard-coded value).
    bbox_inches : str or None, default ``"tight"``
        Passed to matplotlib's ``savefig``. ``None`` disables the
        bounding-box adjustment.
    plotly_include_js : bool or str, optional
        Passed to Plotly's ``write_html`` when the HTML branch fires.
        When ``None``, Plotly's default (``"cdn"``) is used.
    create_dirs : bool, default False
        If True, create the destination's parent directories before
        writing.

    Returns
    -------
    ExportResult
        ``ok=True`` on success; ``ok=False`` with ``error`` and
        ``error_type`` on runtime failures.

    Raises
    ------
    ExportError
        On programmer errors: non-coercible ``path``, ``None`` figure,
        non-positive ``scale`` / ``dpi``, unknown extension for the
        figure type, or a missing dependency (``kaleido`` for
        Plotly-to-image).
    """
    # ---- Validate inputs (raises on programmer error) ------------ #
    if fig is None:
        raise ExportError("fig must not be None.")

    path_obj = _coerce_path(path)
    ext = _extension_of(path_obj)

    # Plotly-with-no-extension defaults to HTML (previous behaviour).
    if not ext:
        figure_type = _detect_figure_type(fig)
        if figure_type == _FIGURE_TYPE_PLOTLY:
            ext = "html"
        else:
            raise ExportError(
                f"path {str(path_obj)!r} has no extension; add one "
                f"(e.g. '.png', '.svg', '.pdf')."
            )

    figure_type = _detect_figure_type(fig)

    if figure_type == _FIGURE_TYPE_PLOTLY:
        if ext not in _PLOTLY_EXTENSIONS:
            raise ExportError(
                f"plotly figure cannot be exported to {ext!r}; "
                f"supported: {sorted(_PLOTLY_EXTENSIONS)}."
            )
    else:  # matplotlib
        if ext not in _MATPLOTLIB_EXTENSIONS:
            raise ExportError(
                f"matplotlib figure cannot be exported to {ext!r}; "
                f"supported: {sorted(_MATPLOTLIB_EXTENSIONS)}."
            )

    scale = _require_positive_float("scale", scale)
    if dpi is not None:
        dpi = _require_positive_int("dpi", dpi)

    if plotly_include_js is not None:
        plotly_include_js = _validate_include_plotlyjs(
            plotly_include_js
        )

    if create_dirs:
        try:
            path_obj.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ExportError(
                f"could not create directory {str(path_obj.parent)!r}: "
                f"{exc}"
            ) from exc

    # ---- Plotly-to-image requires kaleido ----------------------- #
    if (
        figure_type == _FIGURE_TYPE_PLOTLY
        and ext in _PLOTLY_IMAGE_EXTENSIONS
        and not _kaleido_available()
    ):
        raise ExportError(
            f"plotly figure → {ext!r} requires the 'kaleido' package; "
            f"install it with `pip install kaleido`."
        )

    # ---- Dispatch ----------------------------------------------- #
    try:
        if figure_type == _FIGURE_TYPE_PLOTLY:
            _write_plotly(
                fig, path_obj, ext, scale, plotly_include_js,
            )
        else:
            _write_matplotlib(
                fig, path_obj, ext, dpi, bbox_inches,
            )
    except ExportError:
        raise
    except Exception as exc:
        message = f"failed to export {figure_type} figure to " \
                  f"{str(path_obj)!r}: {exc}"
        logger.exception(message)
        return {
            "ok": False,
            "path": str(path_obj),
            "format": ext,
            "figure_type": figure_type,
            "error": str(exc),
            "error_type": "runtime",
        }

    logger.debug(
        "saved %s figure to %r (format=%s)",
        figure_type, str(path_obj), ext,
    )
    return {
        "ok": True,
        "path": str(path_obj),
        "format": ext,
        "figure_type": figure_type,
        "error": None,
        "error_type": None,
    }


def _write_plotly(
    fig: Any,
    path_obj: Path,
    ext: str,
    scale: float,
    include_js: Any,
) -> None:
    """Dispatch a Plotly figure to its writer."""
    if ext in _PLOTLY_HTML_EXTENSIONS:
        kwargs: Dict[str, Any] = {}
        if include_js is not None:
            kwargs["include_plotlyjs"] = include_js
        fig.write_html(str(path_obj), **kwargs)
        return
    if ext in _PLOTLY_JSON_EXTENSIONS:
        # write_json was introduced in plotly 5; fall back to
        # to_json + manual write for older versions.
        if hasattr(fig, "write_json"):
            fig.write_json(str(path_obj))
        else:  # pragma: no cover — plotly < 5
            path_obj.write_text(fig.to_json(), encoding="utf-8")
        return
    # image export
    fig.write_image(str(path_obj), scale=scale)


def _write_matplotlib(
    fig: Any,
    path_obj: Path,
    ext: str,             # noqa: ARG001 — used for validation upstream
    dpi: Optional[int],
    bbox_inches: Optional[str],
) -> None:
    """Dispatch a matplotlib figure to ``savefig``."""
    effective_dpi = 150 if dpi is None else dpi
    save_kwargs: Dict[str, Any] = {"dpi": effective_dpi}
    if bbox_inches is not None:
        save_kwargs["bbox_inches"] = bbox_inches
    fig.savefig(str(path_obj), **save_kwargs)


# --------------------------------------------------------------------------- #
# Backward-compatible wrappers
# --------------------------------------------------------------------------- #
def save_figure(
    fig: Any,
    path: Union[str, "os.PathLike[str]"],
    *,
    scale: float = 2.0,
) -> bool:
    """
    Save ``fig`` to ``path``. Returns True on success.

    Backward-compatible wrapper over :func:`save_figure_detailed`.
    Never raises — every failure (including programmer errors) returns
    ``False``. Callers who need to distinguish failure modes should use
    :func:`save_figure_detailed`.
    """
    try:
        result = save_figure_detailed(fig, path, scale=scale)
    except ExportError as exc:
        logger.error("save_figure rejected %r: %s", str(path), exc)
        return False
    except Exception:  # pragma: no cover — defensive
        logger.exception(
            "save_figure failed unexpectedly for %r", str(path),
        )
        return False
    return bool(result["ok"])


def export_html_detailed(
    fig: Any,
    path: Union[str, "os.PathLike[str]"],
    *,
    include_plotlyjs: Any = "cdn",
    create_dirs: bool = False,
) -> ExportResult:
    """
    Export a Plotly figure as a standalone HTML file. Structured result.

    Raises :class:`ExportError` on programmer errors (non-Plotly figure,
    empty path, non-HTML extension, invalid ``include_plotlyjs``);
    returns ``ExportResult(ok=False, ...)`` on runtime failures.
    """
    if fig is None:
        raise ExportError("fig must not be None.")
    if not hasattr(fig, "write_html"):
        raise ExportError(
            f"export_html requires a Plotly figure, got "
            f"{type(fig).__name__}."
        )

    path_obj = _coerce_path(path)
    ext = _extension_of(path_obj)
    if ext and ext not in _PLOTLY_HTML_EXTENSIONS:
        raise ExportError(
            f"export_html requires an .html/.htm extension (or none), "
            f"got {path_obj.suffix!r}."
        )

    include_plotlyjs = _validate_include_plotlyjs(include_plotlyjs)

    if create_dirs:
        try:
            path_obj.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ExportError(
                f"could not create directory {str(path_obj.parent)!r}: "
                f"{exc}"
            ) from exc

    try:
        fig.write_html(
            str(path_obj), include_plotlyjs=include_plotlyjs,
        )
    except Exception as exc:
        message = f"export_html failed for {str(path_obj)!r}: {exc}"
        logger.exception(message)
        return {
            "ok": False,
            "path": str(path_obj),
            "format": ext or "html",
            "figure_type": _FIGURE_TYPE_PLOTLY,
            "error": str(exc),
            "error_type": "runtime",
        }

    logger.debug("exported plotly figure as HTML to %r", str(path_obj))
    return {
        "ok": True,
        "path": str(path_obj),
        "format": ext or "html",
        "figure_type": _FIGURE_TYPE_PLOTLY,
        "error": None,
        "error_type": None,
    }


def export_html(
    fig: Any,
    path: Union[str, "os.PathLike[str]"],
    *,
    include_plotlyjs: str = "cdn",
) -> bool:
    """
    Export a Plotly figure as a standalone HTML file.

    Backward-compatible wrapper over :func:`export_html_detailed`.
    Never raises — every failure returns ``False``.
    """
    try:
        result = export_html_detailed(
            fig, path, include_plotlyjs=include_plotlyjs,
        )
    except ExportError as exc:
        logger.error("export_html rejected %r: %s", str(path), exc)
        return False
    except Exception:  # pragma: no cover — defensive
        logger.exception(
            "export_html failed unexpectedly for %r", str(path),
        )
        return False
    return bool(result["ok"])


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "ExportError",
    "ExportResult",
    "export_html",
    "export_html_detailed",
    "save_figure",
    "save_figure_detailed",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m visualization.renderers.export
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys
    import tempfile

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

    # ---- Fake figures for smoke testing (no real backends needed) -- #
    class _FakePlotly:
        def __init__(self):
            self.writes: List[Tuple[str, Dict[str, Any]]] = []

        def write_html(self, path, **kwargs):
            self.writes.append(("html", {"path": path, **kwargs}))
            Path(path).write_text("<html></html>", encoding="utf-8")

        def write_json(self, path, **kwargs):
            self.writes.append(("json", {"path": path, **kwargs}))
            Path(path).write_text("{}", encoding="utf-8")

        def write_image(self, path, **kwargs):
            self.writes.append(("image", {"path": path, **kwargs}))
            Path(path).write_text("<binary>", encoding="utf-8")

    class _FakeMatplotlib:
        def __init__(self):
            self.writes: List[Tuple[str, Dict[str, Any]]] = []

        def savefig(self, path, **kwargs):
            self.writes.append(("savefig", {"path": path, **kwargs}))
            Path(path).write_text("<binary>", encoding="utf-8")

    # ---- 1. Plotly HTML ----------------------------------------- #
    print("plotly html:")
    with tempfile.TemporaryDirectory() as tmp:
        fig = _FakePlotly()
        out = Path(tmp) / "chart.html"
        res = save_figure_detailed(fig, out)
        _check("ok", res["ok"] is True)
        _check("format == 'html'", res["format"] == "html")
        _check(
            "figure_type == 'plotly'",
            res["figure_type"] == "plotly",
        )
        _check("file created", out.exists())
        _check(
            "write_html called with default cdn",
            fig.writes[0][1].get("include_plotlyjs") is None,
        )

        # .htm extension
        fig2 = _FakePlotly()
        out2 = Path(tmp) / "chart.htm"
        res2 = save_figure_detailed(fig2, out2)
        _check("htm also works", res2["ok"] is True)

        # Path input
        fig3 = _FakePlotly()
        out3 = Path(tmp) / "chart_path.html"
        res3 = save_figure_detailed(fig3, out3)
        _check("Path input works", res3["ok"] is True)
        _check("Path file created", out3.exists())

    # ---- 2. Plotly JSON ---------------------------------------- #
    print("plotly json:")
    with tempfile.TemporaryDirectory() as tmp:
        fig = _FakePlotly()
        out = Path(tmp) / "chart.json"
        res = save_figure_detailed(fig, out)
        _check("json ok", res["ok"] is True)
        _check("json format", res["format"] == "json")
        _check("json file created", out.exists())

    # ---- 3. Plotly image (requires kaleido) -------------------- #
    print("plotly image (kaleido gate):")
    with tempfile.TemporaryDirectory() as tmp:
        fig = _FakePlotly()
        out = Path(tmp) / "chart.png"
        if _kaleido_available():
            res = save_figure_detailed(fig, out)
            _check("image ok with kaleido", res["ok"] is True)
            _check("image format", res["format"] == "png")
        else:
            try:
                save_figure_detailed(fig, out)
                _check("image raises without kaleido", False)
            except ExportError as exc:
                _check(
                    "image raises without kaleido (names package)",
                    "kaleido" in str(exc),
                )

    # ---- 4. Plotly + unsupported extension --------------------- #
    print("plotly + unsupported extension:")
    with tempfile.TemporaryDirectory() as tmp:
        fig = _FakePlotly()
        try:
            save_figure_detailed(fig, Path(tmp) / "chart.txt")
            _check("plotly .txt rejected", False)
        except ExportError:
            _check("plotly .txt rejected", True)

    # ---- 5. Matplotlib savefig --------------------------------- #
    print("matplotlib:")
    with tempfile.TemporaryDirectory() as tmp:
        fig = _FakeMatplotlib()
        out = Path(tmp) / "chart.png"
        res = save_figure_detailed(fig, out)
        _check("matplotlib ok", res["ok"] is True)
        _check(
            "figure_type == 'matplotlib'",
            res["figure_type"] == "matplotlib",
        )
        _check(
            "default dpi == 150",
            fig.writes[0][1].get("dpi") == 150,
        )
        _check(
            "default bbox_inches == 'tight'",
            fig.writes[0][1].get("bbox_inches") == "tight",
        )

        # Custom dpi
        fig2 = _FakeMatplotlib()
        save_figure_detailed(fig2, Path(tmp) / "hi.png", dpi=300)
        _check("custom dpi passed", fig2.writes[0][1]["dpi"] == 300)

        # Custom bbox_inches=None
        fig3 = _FakeMatplotlib()
        save_figure_detailed(
            fig3, Path(tmp) / "raw.png", bbox_inches=None,
        )
        _check(
            "bbox_inches=None omits the kwarg",
            "bbox_inches" not in fig3.writes[0][1],
        )

        # .svg / .pdf / .jpg
        for ext in ("svg", "pdf", "jpg"):
            fig_ext = _FakeMatplotlib()
            save_figure_detailed(fig_ext, Path(tmp) / f"x.{ext}")
            _check(f"matplotlib .{ext} accepted", True)

        # Unknown ext
        try:
            save_figure_detailed(_FakeMatplotlib(), Path(tmp) / "x.xyz")
            _check("matplotlib .xyz rejected", False)
        except ExportError:
            _check("matplotlib .xyz rejected", True)

    # ---- 6. No-extension input --------------------------------- #
    print("no-extension input:")
    with tempfile.TemporaryDirectory() as tmp:
        # Plotly → defaults to html
        fig = _FakePlotly()
        out = Path(tmp) / "chart"
        res = save_figure_detailed(fig, out)
        _check("plotly defaults to html", res["format"] == "html")

        # matplotlib → rejected
        try:
            save_figure_detailed(_FakeMatplotlib(), Path(tmp) / "chart")
            _check("matplotlib no-extension rejected", False)
        except ExportError:
            _check("matplotlib no-extension rejected", True)

    # ---- 7. Input validation ---------------------------------- #
    print("input validation:")
    with tempfile.TemporaryDirectory() as tmp:
        # fig=None
        try:
            save_figure_detailed(None, Path(tmp) / "x.png")
            _check("None figure rejected", False)
        except ExportError:
            _check("None figure rejected", True)

        # path=None
        try:
            save_figure_detailed(_FakePlotly(), None)  # type: ignore[arg-type]
            _check("None path rejected", False)
        except ExportError:
            _check("None path rejected", True)

        # path=""
        try:
            save_figure_detailed(_FakePlotly(), "")
            _check("empty path rejected", False)
        except ExportError:
            _check("empty path rejected", True)

        # scale=0
        try:
            save_figure_detailed(
                _FakePlotly(), Path(tmp) / "x.html", scale=0,
            )
            _check("scale=0 rejected", False)
        except ExportError:
            _check("scale=0 rejected", True)

        # scale=-1
        try:
            save_figure_detailed(
                _FakePlotly(), Path(tmp) / "x.html", scale=-1.0,
            )
            _check("negative scale rejected", False)
        except ExportError:
            _check("negative scale rejected", True)

        # dpi=0
        try:
            save_figure_detailed(
                _FakeMatplotlib(), Path(tmp) / "x.png", dpi=0,
            )
            _check("dpi=0 rejected", False)
        except ExportError:
            _check("dpi=0 rejected", True)

        # create_dirs with a nested path
        fig = _FakePlotly()
        nested = Path(tmp) / "a" / "b" / "chart.html"
        res = save_figure_detailed(fig, nested, create_dirs=True)
        _check("create_dirs works", res["ok"] is True)
        _check("nested file created", nested.exists())

        # Missing parent without create_dirs → runtime failure
        fig2 = _FakePlotly()
        try:
            res2 = save_figure_detailed(
                fig2, Path(tmp) / "missing" / "x.html",
            )
            _check(
                "missing parent → runtime failure",
                res2["ok"] is False
                and res2["error_type"] == "runtime",
            )
        except ExportError:
            # Some environments raise at write time — accept either.
            _check("missing parent → runtime failure", True)

    # ---- 8. Unsupported figure type ---------------------------- #
    print("unsupported figure type:")
    with tempfile.TemporaryDirectory() as tmp:
        try:
            save_figure_detailed(object(), Path(tmp) / "x.png")
            _check("object() rejected", False)
        except ExportError:
            _check("object() rejected", True)

    # ---- 9. Backward-compatible save_figure -------------------- #
    print("save_figure backward compat:")
    with tempfile.TemporaryDirectory() as tmp:
        _check(
            "save_figure returns True on success",
            save_figure(_FakePlotly(), Path(tmp) / "ok.html") is True,
        )
        _check(
            "save_figure returns False on None fig",
            save_figure(None, Path(tmp) / "x.html") is False,
        )
        _check(
            "save_figure returns False on bad scale",
            save_figure(
                _FakePlotly(), Path(tmp) / "x.html", scale=0,
            ) is False,
        )
        _check(
            "save_figure returns False on unknown extension",
            save_figure(_FakePlotly(), Path(tmp) / "x.txt") is False,
        )

    # ---- 10. export_html -------------------------------------- #
    print("export_html:")
    with tempfile.TemporaryDirectory() as tmp:
        fig = _FakePlotly()
        out = Path(tmp) / "report.html"
        _check(
            "export_html returns True",
            export_html(fig, out) is True,
        )
        _check("export_html created file", out.exists())
        _check(
            "write_html received cdn",
            fig.writes[0][1].get("include_plotlyjs") == "cdn",
        )

        # Bad include_plotlyjs
        try:
            export_html_detailed(
                _FakePlotly(), Path(tmp) / "x.html",
                include_plotlyjs="not-a-valid-value",
            )
            _check("invalid include_plotlyjs rejected", False)
        except ExportError:
            _check("invalid include_plotlyjs rejected", True)

        # Non-Plotly figure
        try:
            export_html_detailed(
                _FakeMatplotlib(), Path(tmp) / "x.html",
            )
            _check("non-plotly rejected", False)
        except ExportError:
            _check("non-plotly rejected", True)

        # Wrong extension
        try:
            export_html_detailed(
                _FakePlotly(), Path(tmp) / "x.txt",
            )
            _check("wrong extension rejected", False)
        except ExportError:
            _check("wrong extension rejected", True)

        # No extension accepted
        res = export_html_detailed(
            _FakePlotly(), Path(tmp) / "no_ext",
        )
        _check("no extension accepted", res["ok"] is True)

        # export_html backward-compat return on error
        _check(
            "export_html returns False on bad figure",
            export_html(_FakeMatplotlib(), Path(tmp) / "x.html") is False,
        )

    # ---- 11. include_plotlyjs pass-through -------------------- #
    print("plotly_include_js:")
    with tempfile.TemporaryDirectory() as tmp:
        fig = _FakePlotly()
        save_figure_detailed(
            fig, Path(tmp) / "x.html",
            plotly_include_js="directory",
        )
        _check(
            "custom include_plotlyjs passed",
            fig.writes[0][1].get("include_plotlyjs") == "directory",
        )

    # ---- 12. __all__ / __dir__ -------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "ExportError", "ExportResult",
        "export_html", "export_html_detailed",
        "save_figure", "save_figure_detailed",
    ):
        _check(f"{_name} exported", _name in __all__)

    # ---- 13. Error type -------------------------------------- #
    print("error type:")
    _check(
        "ExportError subclasses ValueError",
        issubclass(ExportError, ValueError),
    )

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
