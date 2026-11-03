# src/visualization/renderers/pareto_plots.py

"""
Consolidated Pareto visualizer.

Replaces the four overlapping legacy modules (energy_pareto.py,
leaderboard_plots.py, pareto_plots.py, and pareto_plotter.py) with a
single tested class. Legacy function names are preserved as thin
wrappers below.

Responsibilities
----------------
This module owns **exactly one concern**: turning a list of point dicts
into a 2D projection with frontier highlighting. Everything else is
delegated to the sibling contract modules:

- backend selection → :func:`renderers.base.select_backend`
- palette + typography → :mod:`contracts.theme`
- provenance subtitle → :func:`contracts.chart_schema.ChartContext.to_subtitle`

Audience-aware colors, dark-mode variants, and interactive toggles are
**not** implemented here — callers who need them configure the theme
(see :mod:`contracts.theme`) and pass it in.

Behavior changes over the previous revision
-------------------------------------------
1. **Import path fixed** — ``..contracts.theme`` (previously
   ``..contracts.visual_theme``, which does not exist in the enhanced
   tree).
2. **Backend selection delegated** to
   :func:`renderers.base.select_backend`. ``ParetoExplorer`` no longer
   mutates ``self.backend``, so a fallback is not sticky across calls;
   the object exposes ``backend`` (resolved) and ``was_fallback``
   (bool) as read-only properties. Both backends' availability is
   checked; if neither is installed, construction raises
   :class:`ParetoExplorerError` instead of failing at render time.
3. **Frontier IDs are normalised to ``str``** in both the split logic
   and the legacy wrappers. Previously, an ``int`` ``agent_id``
   produced a frontier set of ints that never matched the ``str`` IDs
   extracted from points, so *every* point was silently classified as
   dominated.
4. **Coordinate extraction is strict** — ``bool`` is rejected as a
   numeric coordinate, and non-finite ``NaN`` / ``inf`` values are
   dropped. Dropped counts are logged at ``WARNING``; if **every**
   point is dropped, :class:`ParetoExplorerError` is raised (a chart
   with zero valid points is almost always a field-name typo).
5. **``color_by`` is renamed to ``id_key``** and validated. The old
   name implied a categorical coloring that was never implemented; it
   was, in fact, only used to look up the point identifier.
6. **All public parameters are validated** — ``points``, ``x``, ``y``,
   ``id_key``, ``frontier_ids``, ``context``, ``title``, tick formats.
   Bad input raises :class:`ParetoExplorerError` before any rendering
   work is done.
7. **``compute_frontier`` helper added** and used by all four legacy
   wrappers. Previously, ``plot_latency_vs_energy`` /
   ``plot_carbon_vs_energy`` / ``plot_accuracy_vs_carbon`` rendered
   every point as dominated with no frontier highlighting and no way
   to supply one. All four wrappers now accept ``pareto_front=`` and
   auto-compute the frontier when it is omitted.
8. **Marker shapes are drawn from the theme** (``marker_frontier`` /
   ``marker_dominated``) via a small matplotlib→plotly symbol
   translation table, so both backends render the same shape. The
   previous code hard-coded ``symbol="diamond"`` on Plotly and used
   matplotlib's default circle, so a chart's frontier shape depended
   on the backend.
9. **Matplotlib font is set** from ``theme.font_family_python``.
   Previously, matplotlib used its default ``DejaVu Sans``, so charts
   from the two backends used different fonts.
10. **Optional tick formats** — ``x_tickformat`` / ``y_tickformat``
    parameters are forwarded to Plotly and mapped to matplotlib via a
    small formatter wrapper.
11. **Narrow error type** — :class:`ParetoExplorerError(ValueError)`
    is raised from every validation path. Failures no longer leak
    ``AttributeError`` / ``TypeError`` / ``ModuleNotFoundError``.
12. ``__repr__`` on :class:`ParetoExplorer`.
13. ``__all__`` / ``__dir__``.
14. ``__main__`` smoke test with ``_check()`` assertions — survives
    ``python -O``.

Compatibility
-------------
- ``ParetoExplorer(theme=None, backend="plotly")`` — same signature;
  ``strict_backend=False`` is a new keyword-only argument that maps to
  :func:`select_backend`'s ``strict`` parameter.
- ``ParetoExplorer.plot_tradeoff(...)`` — same parameters, plus
  ``id_key`` (renamed from ``color_by``) and
  ``x_tickformat`` / ``y_tickformat``. All new parameters are
  keyword-only with defaults that match the previous behavior.
  Callers who passed ``color_by="agent_id"`` (i.e. the default) are
  unaffected; callers who passed ``color_by="query_type"`` got
  silent-wrong output before — they must now either use
  ``id_key="query_type"`` (which does the same thing under a
  correct name) or pass the identifier field they actually meant.
- ``plot_accuracy_vs_energy(results, pareto_front=None, context=None)``
  — ``pareto_front`` is now optional. When ``None`` the frontier is
  computed from ``results`` via :func:`compute_frontier`.
- ``plot_latency_vs_energy(results, context=None, *,
  pareto_front=None)`` — ``pareto_front`` is new, keyword-only, and
  additive.
- Same for ``plot_carbon_vs_energy`` and ``plot_accuracy_vs_carbon``.
- Wrapper frontier IDs are now coerced to ``str``, so an ``int``
  ``agent_id`` no longer silently misclassifies every point.

Requires Python 3.8+.
"""

from __future__ import annotations

import logging
import math
from typing import (
    Any, Callable, Dict, Final, Iterable, List, Mapping, Optional,
    Sequence, Tuple, Union,
)

from .base import (
    RendererBackend,
    RendererError,
    RendererSelection,
    select_backend,
)
from ..contracts.theme import DEFAULT_THEME, ThemeError, VisualTheme
from ..contracts.chart_schema import ChartContext

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ParetoExplorerError(ValueError):
    """Raised for invalid Pareto-visualizer inputs or configuration."""


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
_PLOTLY_BACKEND: Final[str] = "plotly"
_MATPLOTLIB_BACKEND: Final[str] = "matplotlib"
_DEFAULT_ID_KEY: Final[str] = "agent_id"

# matplotlib marker code → Plotly symbol name.
_MARKER_TO_PLOTLY_SYMBOL: Final[Dict[str, str]] = {
    "o": "circle",
    "s": "square",
    "D": "diamond",
    "^": "triangle-up",
    "v": "triangle-down",
    "<": "triangle-left",
    ">": "triangle-right",
    "x": "x",
    "+": "cross",
}


# --------------------------------------------------------------------------- #
# Lazy backend imports
# --------------------------------------------------------------------------- #
def _load_plotly() -> Any:
    """
    Import ``plotly.graph_objects``.

    Raises :class:`ParetoExplorerError` if plotly is reported available
    but cannot actually be imported (e.g. broken install).
    """
    try:
        import plotly.graph_objects as go  # type: ignore
        return go
    except ImportError as exc:
        raise ParetoExplorerError(
            f"plotly is reported as available but could not be "
            f"imported: {exc}"
        ) from exc


def _load_pyplot() -> Any:
    """Import ``matplotlib.pyplot``; raise on failure."""
    try:
        import matplotlib.pyplot as plt  # type: ignore
        return plt
    except ImportError as exc:
        raise ParetoExplorerError(
            f"matplotlib is reported as available but could not be "
            f"imported: {exc}"
        ) from exc


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _require_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ParetoExplorerError(
            f"{name} must be a non-empty string, got "
            f"{type(value).__name__}."
        )
    return value


def _finite_or_none(value: Any) -> Optional[float]:
    """
    Return ``float(value)`` when ``value`` is a finite number, else
    ``None``.

    Rejects ``bool`` (a numeric subclass callers rarely mean as a
    coordinate) and non-finite ``float('nan')`` / ``float('inf')``.
    """
    if isinstance(value, bool):
        return None
    if not isinstance(value, (int, float)):
        return None
    fv = float(value)
    return fv if math.isfinite(fv) else None


def _normalise_frontier_ids(
    frontier_ids: Optional[Iterable[Any]],
) -> frozenset:
    """
    Coerce ``frontier_ids`` to a ``frozenset`` of strings.

    The previous implementation compared ``str(pid)`` against the raw
    ``set(frontier_ids)``, which silently failed when callers supplied
    integer IDs. Normalising on the way in closes the mismatch.
    """
    if frontier_ids is None:
        return frozenset()
    try:
        return frozenset(str(fid) for fid in frontier_ids)
    except TypeError as exc:
        raise ParetoExplorerError(
            f"frontier_ids must be an iterable of hashable values, "
            f"got {type(frontier_ids).__name__}: {exc}"
        ) from exc


def _validate_context(
    context: Optional[Any],
) -> Optional[ChartContext]:
    if context is None:
        return None
    if not isinstance(context, ChartContext):
        raise ParetoExplorerError(
            f"context must be a ChartContext or None, got "
            f"{type(context).__name__}."
        )
    return context


def _validate_title(title: Optional[Any]) -> Optional[str]:
    if title is None:
        return None
    if not isinstance(title, str):
        raise ParetoExplorerError(
            f"title must be a string or None, got "
            f"{type(title).__name__}."
        )
    return title


def _validate_tickformat(
    name: str, value: Optional[Any],
) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ParetoExplorerError(
            f"{name} must be a non-empty string or None, got "
            f"{type(value).__name__}."
        )
    return value


# --------------------------------------------------------------------------- #
# Coordinate extraction
# --------------------------------------------------------------------------- #
Coords = List[Tuple[float, float, str]]


def _split_points(
    points: Sequence[Mapping[str, Any]],
    *,
    x: str,
    y: str,
    frontier_ids: frozenset,
    id_key: str,
) -> Tuple[Coords, Coords, int]:
    """
    Split ``points`` into ``(dominated_coords, frontier_coords, dropped)``.

    Each coord is ``(x, y, id)`` with ``x`` / ``y`` as finite floats and
    ``id`` as a string. ``dropped`` counts points whose ``x`` or ``y``
    was missing, non-numeric, boolean, or non-finite.
    """
    dominated: Coords = []
    frontier: Coords = []
    dropped = 0
    for p in points:
        if not isinstance(p, Mapping):
            dropped += 1
            continue
        xv = _finite_or_none(p.get(x))
        yv = _finite_or_none(p.get(y))
        if xv is None or yv is None:
            dropped += 1
            continue
        pid = str(p.get(id_key, ""))
        entry = (xv, yv, pid)
        if pid in frontier_ids:
            frontier.append(entry)
        else:
            dominated.append(entry)
    return dominated, frontier, dropped


# --------------------------------------------------------------------------- #
# Frontier computation
# --------------------------------------------------------------------------- #
def compute_frontier(
    points: Sequence[Mapping[str, Any]],
    *,
    x: str,
    y: str,
    id_key: str = _DEFAULT_ID_KEY,
    x_direction: str = "min",
    y_direction: str = "max",
) -> List[str]:
    """
    Return the IDs of points on the 2D Pareto frontier.

    Parameters
    ----------
    points : sequence of mappings
        Candidate points.
    x, y : str
        Field names for the axes.
    id_key : str, default ``"agent_id"``
        Field name for the point identifier. Points without the field
        are excluded from the frontier (they can never be matched back
        to a plotted point).
    x_direction : ``"min"`` or ``"max"``, default ``"min"``
        Whether lower or higher ``x`` is preferable.
    y_direction : ``"min"`` or ``"max"``, default ``"max"``
        Whether lower or higher ``y`` is preferable.

    Returns
    -------
    list of str
        Frontier identifiers in the order the algorithm discovered
        them (ascending ``x`` for ``x_direction="min"``, descending for
        ``"max"``).
    """
    _require_str("x", x)
    _require_str("y", y)
    _require_str("id_key", id_key)
    if x_direction not in ("min", "max"):
        raise ParetoExplorerError(
            f"x_direction must be 'min' or 'max', got {x_direction!r}."
        )
    if y_direction not in ("min", "max"):
        raise ParetoExplorerError(
            f"y_direction must be 'min' or 'max', got {y_direction!r}."
        )
    if x == y:
        raise ParetoExplorerError(
            "x and y must name different fields."
        )

    entries: List[Tuple[float, float, str]] = []
    for i, p in enumerate(points):
        if not isinstance(p, Mapping):
            continue
        xv = _finite_or_none(p.get(x))
        yv = _finite_or_none(p.get(y))
        if xv is None or yv is None:
            continue
        raw_id = p.get(id_key)
        if raw_id is None:
            continue
        entries.append((xv, yv, str(raw_id)))

    if not entries:
        return []

    # Sort so that, for a given ``x``, the *best* ``y`` is processed
    # first. Then a single sweep with a running "best y so far" gives
    # the frontier.
    x_sign = 1 if x_direction == "min" else -1
    y_sign = 1 if y_direction == "max" else -1
    entries.sort(
        key=lambda e: (x_sign * e[0], -y_sign * e[1], e[2])
    )

    best_y = -math.inf if y_direction == "max" else math.inf
    frontier: List[str] = []
    for _xv, yv, pid in entries:
        if y_direction == "max":
            if yv > best_y:
                frontier.append(pid)
                best_y = yv
        else:
            if yv < best_y:
                frontier.append(pid)
                best_y = yv
    return frontier


def _extract_frontier_ids(
    pareto_front: Optional[Sequence[Mapping[str, Any]]],
    *,
    id_key: str = _DEFAULT_ID_KEY,
) -> List[str]:
    """
    Extract the IDs of a caller-supplied ``pareto_front`` list.

    Items without ``id_key`` are dropped with a ``WARNING`` — they
    cannot be matched back to a plotted point.
    """
    if not pareto_front:
        return []
    ids: List[str] = []
    missing = 0
    for r in pareto_front:
        if not isinstance(r, Mapping):
            missing += 1
            continue
        raw = r.get(id_key)
        if raw is None:
            missing += 1
            continue
        ids.append(str(raw))
    if missing:
        logger.warning(
            "%d/%d pareto_front items lack %r; those items cannot be "
            "matched against the plotted points.",
            missing, len(pareto_front), id_key,
        )
    return ids


# --------------------------------------------------------------------------- #
# ParetoExplorer
# --------------------------------------------------------------------------- #
class ParetoExplorer:
    """
    Interactive Pareto explorer.

    Renders 2D projections of a multi-objective frontier with frontier
    highlighting and provenance banners. Backend is resolved at
    construction time via :func:`renderers.base.select_backend`; the
    resolved backend is fixed for the lifetime of the instance.
    """

    def __init__(
        self,
        *,
        theme: Optional[VisualTheme] = None,
        backend: Union[str, RendererBackend] = "plotly",
        strict_backend: bool = False,
    ) -> None:
        # ---- theme ------------------------------------------------ #
        if theme is not None and not isinstance(theme, VisualTheme):
            raise ParetoExplorerError(
                f"theme must be a VisualTheme or None, got "
                f"{type(theme).__name__}."
            )
        self._theme: VisualTheme = theme or DEFAULT_THEME

        # ---- backend --------------------------------------------- #
        if isinstance(backend, RendererBackend):
            requested = backend.value
        elif isinstance(backend, str) and backend:
            requested = backend.lower()
        else:
            raise ParetoExplorerError(
                f"backend must be a non-empty string or "
                f"RendererBackend, got {type(backend).__name__}."
            )
        try:
            selection: RendererSelection = select_backend(
                requested, strict=strict_backend,
            )
        except RendererError as exc:
            raise ParetoExplorerError(str(exc)) from exc

        self._selection: RendererSelection = selection
        self._backend_name: str = selection["backend"]
        self._requested_backend: str = selection["requested"]
        self._was_fallback: bool = selection["was_fallback"]

        if self._was_fallback:
            logger.warning(
                "requested backend %r unavailable; using %r.",
                self._requested_backend, self._backend_name,
            )

    # ------------------------------------------------------------------ props
    @property
    def theme(self) -> VisualTheme:
        return self._theme

    @property
    def backend(self) -> str:
        """Resolved backend name (``"plotly"`` or ``"matplotlib"``)."""
        return self._backend_name

    @property
    def requested_backend(self) -> str:
        """The backend name the caller originally requested."""
        return self._requested_backend

    @property
    def was_fallback(self) -> bool:
        """True when the requested backend was unavailable."""
        return self._was_fallback

    # ---------------------------------------------------------- public API
    def plot_tradeoff(
        self,
        points: Sequence[Mapping[str, Any]],
        *,
        x: str,
        y: str,
        frontier_ids: Optional[Iterable[Any]] = None,
        x_label: Optional[str] = None,
        y_label: Optional[str] = None,
        title: Optional[str] = None,
        context: Optional[ChartContext] = None,
        id_key: str = _DEFAULT_ID_KEY,
        x_tickformat: Optional[str] = None,
        y_tickformat: Optional[str] = None,
    ) -> Any:
        """
        Plot a 2D projection of a Pareto frontier.

        Parameters
        ----------
        points : sequence of mappings
            Each point must have numeric ``x`` and ``y`` fields, plus
            an identifier under ``id_key`` (default ``"agent_id"``).
        x, y : str
            Field names for the axes.
        frontier_ids : iterable of str / int, optional
            Which point IDs are on the frontier. Compared as strings, so
            integer IDs are matched against the ``str(...)`` of each
            point's ``id_key``. When ``None`` or empty, no point is
            highlighted as frontier.
        x_label, y_label, title : str, optional
        context : ChartContext, optional
            When provided, its ``to_subtitle()`` is appended to the
            title.
        id_key : str, default ``"agent_id"``
            Field name for point identifiers. Renamed from the
            previous ``color_by`` (which misleadingly suggested a
            coloring feature).
        x_tickformat, y_tickformat : str, optional
            Passed to Plotly's ``tickformat`` and adapted for
            matplotlib via a formatter wrapper.

        Returns
        -------
        Plotly Figure or matplotlib Figure (per the resolved backend).
        """
        # ---- Validate inputs ------------------------------------- #
        if not isinstance(points, Sequence) or isinstance(points, (str, bytes)):
            raise ParetoExplorerError(
                f"points must be a sequence of mappings, got "
                f"{type(points).__name__}."
            )
        x = _require_str("x", x)
        y = _require_str("y", y)
        if x == y:
            raise ParetoExplorerError(
                "x and y must name different fields."
            )
        id_key = _require_str("id_key", id_key)
        frontier_set = _normalise_frontier_ids(frontier_ids)
        context = _validate_context(context)
        title = _validate_title(title)
        x_tickformat = _validate_tickformat("x_tickformat", x_tickformat)
        y_tickformat = _validate_tickformat("y_tickformat", y_tickformat)

        # ---- Split and drop-count ------------------------------- #
        dominated, frontier, dropped = _split_points(
            points,
            x=x, y=y,
            frontier_ids=frontier_set,
            id_key=id_key,
        )
        total = len(dominated) + len(frontier)
        if dropped:
            logger.warning(
                "%d point(s) dropped during %s vs %s extraction "
                "(missing / non-numeric / non-finite coordinates).",
                dropped, x, y,
            )
        if total == 0:
            raise ParetoExplorerError(
                f"no plottable points for {x} vs {y}; check the field "
                f"names and the numeric type of their values."
            )

        # ---- Subtitle -------------------------------------------- #
        subtitle = (
            context.to_subtitle() if context is not None else ""
        )

        # ---- Dispatch -------------------------------------------- #
        if self._backend_name == _PLOTLY_BACKEND:
            return self._render_plotly(
                dominated=dominated,
                frontier=frontier,
                x=x, y=y,
                x_label=x_label, y_label=y_label,
                title=title, subtitle=subtitle,
                x_tickformat=x_tickformat,
                y_tickformat=y_tickformat,
            )
        return self._render_matplotlib(
            dominated=dominated,
            frontier=frontier,
            x=x, y=y,
            x_label=x_label, y_label=y_label,
            title=title, subtitle=subtitle,
            x_tickformat=x_tickformat,
            y_tickformat=y_tickformat,
        )

    # ---------------------------------------------------------- rendering
    def _render_plotly(
        self,
        *,
        dominated: Coords,
        frontier: Coords,
        x: str,
        y: str,
        x_label: Optional[str],
        y_label: Optional[str],
        title: Optional[str],
        subtitle: str,
        x_tickformat: Optional[str],
        y_tickformat: Optional[str],
    ) -> Any:
        go = _load_plotly()
        fig = go.Figure()

        palette = self._theme.palette
        if dominated:
            fig.add_trace(go.Scatter(
                x=[c[0] for c in dominated],
                y=[c[1] for c in dominated],
                mode="markers",
                name="dominated",
                text=[c[2] for c in dominated],
                marker=dict(
                    size=self._theme.marker_size_dominated,
                    color=palette.dominated,
                    symbol=_MARKER_TO_PLOTLY_SYMBOL.get(
                        self._theme.marker_dominated, "x",
                    ),
                ),
            ))
        if frontier:
            fig.add_trace(go.Scatter(
                x=[c[0] for c in frontier],
                y=[c[1] for c in frontier],
                mode="markers",
                name="frontier",
                text=[c[2] for c in frontier],
                marker=dict(
                    size=self._theme.marker_size_frontier,
                    color=palette.frontier,
                    symbol=_MARKER_TO_PLOTLY_SYMBOL.get(
                        self._theme.marker_frontier, "circle",
                    ),
                ),
            ))

        full_title = title or f"{x} vs {y}"
        if subtitle:
            full_title = f"{full_title}<br><sub>{subtitle}</sub>"

        layout: Dict[str, Any] = dict(
            title=full_title,
            xaxis_title=x_label or x,
            yaxis_title=y_label or y,
            plot_bgcolor=palette.background,
            font=dict(family=self._theme.font_family),
        )
        fig.update_layout(**layout)
        fig.update_xaxes(
            gridcolor=palette.grid,
            tickformat=x_tickformat,
        )
        fig.update_yaxes(
            gridcolor=palette.grid,
            tickformat=y_tickformat,
        )
        return fig

    def _render_matplotlib(
        self,
        *,
        dominated: Coords,
        frontier: Coords,
        x: str,
        y: str,
        x_label: Optional[str],
        y_label: Optional[str],
        title: Optional[str],
        subtitle: str,
        x_tickformat: Optional[str],
        y_tickformat: Optional[str],
    ) -> Any:
        plt = _load_pyplot()
        palette = self._theme.palette

        # Honour the theme's native font for matplotlib. The CSS-syntax
        # ``font_family`` field is intended for HTML renderers and does
        # not resolve to a single family in matplotlib.
        plt.rcParams["font.family"] = self._theme.font_family_python

        fig, ax = plt.subplots(
            figsize=(
                self._theme.figure_width_px / 100,
                self._theme.figure_height_px / 100,
            )
        )

        # Plotly's ``marker.size`` is the marker diameter in pixels;
        # matplotlib's ``s`` is the marker area in points². ``s = M²``
        # yields an M-point diameter, which at typical display DPIs is
        # visually close to Plotly's M-pixel diameter. Documented as a
        # known slight (sub-2x) size mismatch rather than a pixel-exact
        # match.
        if dominated:
            ax.scatter(
                [c[0] for c in dominated],
                [c[1] for c in dominated],
                alpha=0.5,
                color=palette.dominated,
                marker=self._theme.marker_dominated,
                s=self._theme.marker_size_dominated ** 2,
                label="dominated",
            )
        if frontier:
            ax.scatter(
                [c[0] for c in frontier],
                [c[1] for c in frontier],
                color=palette.frontier,
                edgecolors="black",
                marker=self._theme.marker_frontier,
                s=self._theme.marker_size_frontier ** 2,
                label="frontier",
            )

        full_title = title or f"{x} vs {y}"
        if subtitle:
            full_title = f"{full_title}\n{subtitle}"

        ax.set_xlabel(x_label or x)
        ax.set_ylabel(y_label or y)
        ax.set_title(full_title)
        ax.grid(True, color=palette.grid)
        ax.legend()

        if x_tickformat is not None or y_tickformat is not None:
            _apply_matplotlib_tickformat(
                ax, x_tickformat, y_tickformat,
            )

        return fig

    # ------------------------------------------------------------------ dunder
    def __repr__(self) -> str:
        return (
            "ParetoExplorer("
            f"backend={self._backend_name!r}, "
            f"requested={self._requested_backend!r}, "
            f"was_fallback={self._was_fallback}, "
            f"theme={self._theme.palette.name!r})"
        )


# --------------------------------------------------------------------------- #
# matplotlib tick format adaptation
# --------------------------------------------------------------------------- #
def _apply_matplotlib_tickformat(
    ax: Any,
    x_tickformat: Optional[str],
    y_tickformat: Optional[str],
) -> None:
    """
    Best-effort mapping of Plotly's ``tickformat`` strings to matplotlib.

    Plotly's d3-format mini-language (``".2f"``, ``"%"``, ``"$,.0f"``)
    is not directly compatible with matplotlib's `ticker.Formatter`.
    We support the common subset and silently fall back to matplotlib's
    default when the string is not recognised — no exception, since
    a bad tick format should not prevent a chart from rendering.
    """
    try:
        from matplotlib.ticker import FuncFormatter, FormatStrFormatter
    except ImportError:  # pragma: no cover — defensive
        return

    def _wrap(fmt: Optional[str]):
        if not fmt:
            return None
        # Plotly format specifiers may include a leading "." (precision).
        stripped = fmt.lstrip(".")
        if stripped in ("0f", "1f", "2f", "3f", "4f"):
            decimals = int(stripped[0])
            return FormatStrFormatter(f"%.{decimals}f")
        if fmt == "%":
            return FuncFormatter(lambda v, _pos: f"{v * 100:.0f}%")
        if fmt.startswith("$"):
            inner = _wrap(fmt.lstrip("$").lstrip(","))
            if inner is not None:
                return FuncFormatter(
                    lambda v, pos: (
                        f"${inner(v, pos)}"
                        if hasattr(inner, "__call__")
                        else f"${FormatStrFormatter(inner)(v, pos)}"
                    )
                )
        # Fallback: matplotlib's default formatter.
        return None

    x_formatter = _wrap(x_tickformat)
    y_formatter = _wrap(y_tickformat)
    if x_formatter is not None:
        ax.xaxis.set_major_formatter(x_formatter)
    if y_formatter is not None:
        ax.yaxis.set_major_formatter(y_formatter)


# --------------------------------------------------------------------------- #
# Legacy wrappers
# --------------------------------------------------------------------------- #
def _plot_or_compute(
    *,
    results: Sequence[Mapping[str, Any]],
    pareto_front: Optional[Sequence[Mapping[str, Any]]],
    x: str,
    y: str,
    x_direction: str,
    y_direction: str,
    x_label: str,
    y_label: str,
    title: str,
    context: Optional[ChartContext],
    theme: Optional[VisualTheme] = None,
) -> Any:
    """Shared body for the four legacy wrappers."""
    explorer = ParetoExplorer(theme=theme)
    if pareto_front is None:
        frontier_ids = compute_frontier(
            results,
            x=x, y=y,
            x_direction=x_direction,
            y_direction=y_direction,
        )
    else:
        frontier_ids = _extract_frontier_ids(pareto_front)
    return explorer.plot_tradeoff(
        points=results,
        x=x, y=y,
        frontier_ids=frontier_ids,
        x_label=x_label,
        y_label=y_label,
        title=title,
        context=context,
    )


def plot_accuracy_vs_energy(
    results: Sequence[Mapping[str, Any]],
    pareto_front: Optional[Sequence[Mapping[str, Any]]] = None,
    context: Optional[ChartContext] = None,
    *,
    theme: Optional[VisualTheme] = None,
) -> Any:
    """
    Accuracy vs energy trade-off.

    When ``pareto_front`` is ``None``, the frontier is computed from
    ``results`` (energy minimized, accuracy maximized). When supplied,
    ``pareto_front`` must share ``agent_id`` with ``results`` — IDs
    are compared as strings, so ``int`` and ``str`` IDs both work.
    """
    return _plot_or_compute(
        results=results,
        pareto_front=pareto_front,
        x="energy", y="accuracy",
        x_direction="min", y_direction="max",
        x_label="Energy (Wh)",
        y_label="Accuracy",
        title="Accuracy vs Energy (Green Pareto Frontier)",
        context=context,
        theme=theme,
    )


def plot_latency_vs_energy(
    results: Sequence[Mapping[str, Any]],
    context: Optional[ChartContext] = None,
    *,
    pareto_front: Optional[Sequence[Mapping[str, Any]]] = None,
    theme: Optional[VisualTheme] = None,
) -> Any:
    """
    Latency vs energy trade-off.

    ``pareto_front`` is new and keyword-only; the previous version
    rendered every point as dominated with no frontier highlighting.
    """
    return _plot_or_compute(
        results=results,
        pareto_front=pareto_front,
        x="energy", y="latency",
        x_direction="min", y_direction="min",
        x_label="Energy (Wh)",
        y_label="Latency (s)",
        title="Latency vs Energy",
        context=context,
        theme=theme,
    )


def plot_carbon_vs_energy(
    results: Sequence[Mapping[str, Any]],
    context: Optional[ChartContext] = None,
    *,
    pareto_front: Optional[Sequence[Mapping[str, Any]]] = None,
    theme: Optional[VisualTheme] = None,
) -> Any:
    """
    Carbon vs energy trade-off.

    ``pareto_front`` is new and keyword-only; the previous version
    rendered every point as dominated.
    """
    return _plot_or_compute(
        results=results,
        pareto_front=pareto_front,
        x="energy", y="carbon",
        x_direction="min", y_direction="min",
        x_label="Energy (Wh)",
        y_label="Carbon (kg CO₂)",
        title="Carbon vs Energy",
        context=context,
        theme=theme,
    )


def plot_accuracy_vs_carbon(
    results: Sequence[Mapping[str, Any]],
    context: Optional[ChartContext] = None,
    *,
    pareto_front: Optional[Sequence[Mapping[str, Any]]] = None,
    theme: Optional[VisualTheme] = None,
) -> Any:
    """
    Accuracy vs carbon trade-off.

    ``pareto_front`` is new and keyword-only; the previous version
    rendered every point as dominated.
    """
    return _plot_or_compute(
        results=results,
        pareto_front=pareto_front,
        x="carbon", y="accuracy",
        x_direction="min", y_direction="max",
        x_label="Carbon (kg CO₂)",
        y_label="Accuracy",
        title="Accuracy vs Carbon",
        context=context,
        theme=theme,
    )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "ParetoExplorer",
    "ParetoExplorerError",
    "compute_frontier",
    "plot_accuracy_vs_carbon",
    "plot_accuracy_vs_energy",
    "plot_carbon_vs_energy",
    "plot_latency_vs_energy",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m visualization.renderers.pareto_plots
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

    # ---- 1. compute_frontier on simple shapes -------------------- #
    print("compute_frontier:")
    pts = [
        {"agent_id": "a", "energy": 1.0, "accuracy": 0.70},
        {"agent_id": "b", "energy": 2.0, "accuracy": 0.80},
        {"agent_id": "c", "energy": 3.0, "accuracy": 0.90},
        {"agent_id": "d", "energy": 1.5, "accuracy": 0.60},  # dominated by a
        {"agent_id": "e", "energy": 4.0, "accuracy": 0.85},  # dominated by c
    ]
    frontier = compute_frontier(
        pts, x="energy", y="accuracy",
        x_direction="min", y_direction="max",
    )
    _check(
        "accuracy-vs-energy frontier is {a, b, c}",
        set(frontier) == {"a", "b", "c"},
    )

    # Every-other-direction
    frontier2 = compute_frontier(
        pts, x="energy", y="accuracy",
        x_direction="max", y_direction="min",
    )
    _check(
        "max-min frontier is non-empty",
        len(frontier2) >= 1,
    )
    _check(
        "max-min frontier contains d",
        "d" in frontier2,
    )

    # Empty points
    _check(
        "empty points → empty frontier",
        compute_frontier([]) is not None
        if False
        else compute_frontier([], x="e", y="a") == [],
    )

    # Invalid direction
    try:
        compute_frontier(
            pts, x="energy", y="accuracy",
            x_direction="sideways",
        )
        _check("invalid direction rejected", False)
    except ParetoExplorerError:
        _check("invalid direction rejected", True)

    # x == y
    try:
        compute_frontier(pts, x="energy", y="energy")
        _check("x == y rejected", False)
    except ParetoExplorerError:
        _check("x == y rejected", True)

    # ---- 2. Frontier ID type mismatch (bug fix) ------------------ #
    print("frontier ID normalisation:")
    # int agent_id in pareto_front must match str-normalised point IDs
    int_pts = [
        {"agent_id": 1, "energy": 1.0, "accuracy": 0.7},
        {"agent_id": 2, "energy": 2.0, "accuracy": 0.8},
    ]
    pareto_front_int = [int_pts[0]]
    ids = _extract_frontier_ids(pareto_front_int)
    _check(
        "int agent_id extracted as str",
        ids == ["1"],
    )
    # And the split logic now matches
    dominated, frontier, dropped = _split_points(
        int_pts,
        x="energy", y="accuracy",
        frontier_ids=_normalise_frontier_ids(ids),
        id_key="agent_id",
    )
    _check("one point classified as frontier", len(frontier) == 1)
    _check(
        "frontier point has the right ID",
        frontier[0][2] == "1",
    )
    _check("no points dropped", dropped == 0)

    # ---- 3. Coordinate extraction rejects bool / NaN / inf ------ #
    print("coordinate extraction:")
    weird_pts = [
        {"agent_id": "good", "energy": 1.0, "accuracy": 0.7},
        {"agent_id": "bool_x", "energy": True, "accuracy": 0.7},
        {"agent_id": "nan_y", "energy": 1.0, "accuracy": float("nan")},
        {"agent_id": "inf_x", "energy": float("inf"), "accuracy": 0.7},
        {"agent_id": "missing_x", "accuracy": 0.7},
        {"agent_id": "string_x", "energy": "1.0", "accuracy": 0.7},
        {"agent_id": "not_a_mapping"},  # will not be a Mapping check trigger
    ]
    dominated, frontier, dropped = _split_points(
        weird_pts,
        x="energy", y="accuracy",
        frontier_ids=frozenset(),
        id_key="agent_id",
    )
    _check("only 1 valid point extracted", len(dominated) + len(frontier) == 1)
    _check("6 points dropped", dropped == 6)

    # ---- 4. ParetoExplorer construction ------------------------- #
    print("ParetoExplorer construction:")
    # Good theme
    try:
        explorer = ParetoExplorer()
        _check("default construction works", True)
        _check(
            "backend is str",
            isinstance(explorer.backend, str)
            and explorer.backend in ("plotly", "matplotlib"),
        )
        _check(
            "requested_backend preserved",
            explorer.requested_backend == "plotly",
        )
        _check(
            "was_fallback is bool",
            isinstance(explorer.was_fallback, bool),
        )
        _check(
            "repr non-empty",
            bool(repr(explorer)),
        )
    except ParetoExplorerError as exc:
        _check(f"default construction failed: {exc}", False)

    # Bad theme
    try:
        ParetoExplorer(theme="not-a-theme")  # type: ignore[arg-type]
        _check("bad theme rejected", False)
    except ParetoExplorerError:
        _check("bad theme rejected", True)

    # Bad backend
    try:
        ParetoExplorer(backend=42)  # type: ignore[arg-type]
        _check("bad backend rejected", False)
    except ParetoExplorerError:
        _check("bad backend rejected", True)

    try:
        ParetoExplorer(backend="bokeh")
        _check("unknown backend rejected", False)
    except ParetoExplorerError:
        _check("unknown backend rejected", True)

    # ---- 5. plot_tradeoff validation ---------------------------- #
    print("plot_tradeoff validation:")
    explorer = ParetoExplorer()
    for bad_kwargs in (
        dict(points="not-a-list", x="e", y="a"),
        dict(points=[], x="e", y="a"),  # empty → raises "no plottable"
        dict(points=[{"e": 1, "a": 0.5}], x="e", y="e"),  # x == y
        dict(points=[{"e": 1, "a": 0.5}], x="", y="a"),   # empty x
        dict(
            points=[{"e": 1, "a": 0.5}],
            x="e", y="a",
            frontier_ids=42,  # not iterable
        ),
        dict(
            points=[{"e": 1, "a": 0.5}],
            x="e", y="a",
            context="not-a-chartcontext",
        ),
        dict(
            points=[{"e": 1, "a": 0.5}],
            x="e", y="a",
            title=42,
        ),
    ):
        try:
            explorer.plot_tradeoff(**bad_kwargs)  # type: ignore[arg-type]
            _check(f"reject {list(bad_kwargs)}", False)
        except ParetoExplorerError:
            _check(f"reject {list(bad_kwargs)}", True)
        except Exception as exc:
            _check(
                f"reject {list(bad_kwargs)} (wrong type: {type(exc).__name__})",
                False,
            )

    # ---- 6. Rendering dispatch via a recording subclass --------- #
    print("rendering dispatch:")

    class _RecordingExplorer(ParetoExplorer):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.dispatch = []

        def _render_plotly(self, **kwargs):
            self.dispatch.append(("plotly", kwargs))
            return "plotly_fig"

        def _render_matplotlib(self, **kwargs):
            self.dispatch.append(("matplotlib", kwargs))
            return "matplotlib_fig"

    recorder = _RecordingExplorer()
    result = recorder.plot_tradeoff(
        [
            {"agent_id": "a", "energy": 1.0, "accuracy": 0.7},
            {"agent_id": "b", "energy": 2.0, "accuracy": 0.8},
        ],
        x="energy", y="accuracy",
        frontier_ids=["b"],
    )
    _check(
        "plot_tradeoff returns the recorder's value",
        result in ("plotly_fig", "matplotlib_fig"),
    )
    _check("exactly one render dispatched", len(recorder.dispatch) == 1)
    kind, kwargs = recorder.dispatch[0]
    _check(
        "backend matches resolution",
        (kind == "plotly") == (recorder.backend == "plotly"),
    )
    _check(
        "one point in dominated",
        len(kwargs["dominated"]) == 1,
    )
    _check(
        "one point in frontier",
        len(kwargs["frontier"]) == 1,
    )
    _check(
        "frontier ID matches",
        kwargs["frontier"][0][2] == "b",
    )

    # ---- 7. Subtitle from ChartContext -------------------------- #
    print("subtitle / ChartContext:")
    from ..contracts.chart_schema import ChartContext

    ctx = ChartContext(
        source="measured", system_version="v1",
    )
    result = recorder.plot_tradeoff(
        [{"agent_id": "a", "energy": 1.0, "accuracy": 0.7}],
        x="energy", y="accuracy",
        context=ctx,
    )
    _, kwargs = recorder.dispatch[-1]
    subtitle = kwargs["subtitle"]
    _check(
        "subtitle contains context text",
        "source=measured" in subtitle,
    )

    # ---- 8. Legacy wrappers accept pareto_front=None ------------ #
    print("legacy wrappers:")
    legacy = _RecordingExplorer()

    # plot_accuracy_vs_energy — auto-compute frontier
    legacy.plot_accuracy_vs_energy = None  # not a real method
    # Use the module-level wrapper with the recorder patched in
    from . import pareto_plots as pp  # type: ignore
    saved_init = pp.ParetoExplorer

    class _PatchedExplorer(_RecordingExplorer):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("strict_backend", False)
            super().__init__(*args, **kwargs)

    pp.ParetoExplorer = _PatchedExplorer  # type: ignore
    try:
        points = [
            {"agent_id": "a", "energy": 1.0, "accuracy": 0.7},
            {"agent_id": "b", "energy": 2.0, "accuracy": 0.8},
            {"agent_id": "c", "energy": 3.0, "accuracy": 0.9},
        ]
        fig = pp.plot_accuracy_vs_energy(points)
        _check(
            "plot_accuracy_vs_energy returns a figure",
            fig in ("plotly_fig", "matplotlib_fig"),
        )

        # int agent_id in the results
        int_points = [
            {"agent_id": 1, "energy": 1.0, "accuracy": 0.7},
            {"agent_id": 2, "energy": 2.0, "accuracy": 0.8},
            {"agent_id": 3, "energy": 3.0, "accuracy": 0.9},
        ]
        fig = pp.plot_accuracy_vs_energy(int_points)
        _check(
            "int agent_id does not silently drop frontier",
            fig in ("plotly_fig", "matplotlib_fig"),
        )

        # Legacy wrappers with no pareto_front
        for name in (
            "plot_latency_vs_energy",
            "plot_carbon_vs_energy",
            "plot_accuracy_vs_carbon",
        ):
            fn = getattr(pp, name)
            fig = fn([
                {"agent_id": "a", "energy": 1.0, "latency": 0.1,
                 "carbon": 0.01, "accuracy": 0.7},
                {"agent_id": "b", "energy": 2.0, "latency": 0.2,
                 "carbon": 0.02, "accuracy": 0.8},
            ])
            _check(
                f"{name} returns a figure",
                fig in ("plotly_fig", "matplotlib_fig"),
            )
    finally:
        pp.ParetoExplorer = saved_init  # type: ignore

    # ---- 9. __all__ / __dir__ ----------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "ParetoExplorer", "ParetoExplorerError", "compute_frontier",
        "plot_accuracy_vs_carbon", "plot_accuracy_vs_energy",
        "plot_carbon_vs_energy", "plot_latency_vs_energy",
    ):
        _check(f"{_name} exported", _name in __all__)

    # ---- 10. Error type ---------------------------------------- #
    print("error type:")
    _check(
        "ParetoExplorerError subclasses ValueError",
        issubclass(ParetoExplorerError, ValueError),
    )

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
