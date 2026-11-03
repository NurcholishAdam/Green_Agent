# src/visualization/energy_pareto.py

"""
Accuracy vs Energy Pareto visualizer.

Backward-compatible entry point for the classic accuracy-vs-energy
Pareto scatter. Delegates to the consolidated
:class:`renderers.pareto_plots.ParetoExplorer` so the rendering logic
lives in exactly one place.

Original API (preserved)::

    plot_accuracy_vs_energy(results, pareto_front)

The original returned ``None`` and called ``plt.show()``. The enhanced
version returns a figure (Plotly or matplotlib) and never calls
``show()`` — the caller decides when and how to display.

Enhanced API::

    plot_accuracy_vs_energy(
        results,
        pareto_front=None,
        *,
        context=None,
        theme=None,
        backend="plotly",
        export_path=None,
        include_frontier_line=True,
        units=None,
    ) -> Figure

Behavioral changes over the previous revision
---------------------------------------------
1. **Import paths fixed** — the previous file imported
   ``contracts.visual_theme`` and ``tradeoffs.pareto``; neither module
   exists in the enhanced tree. The correct paths are
   ``contracts.theme`` and ``renderers.pareto_plots``.

2. **``include_frontier_line`` is now honored** by post-processing the
   figure returned by :class:`ParetoExplorer`. ``ParetoExplorer``
   itself does not accept that parameter; drawing the line here keeps
   the sibling's API stable while still delivering what the docstring
   promises.

3. **``pareto_front`` is now optional.** When ``None``, the frontier is
   computed from ``results`` via
   :func:`renderers.pareto_plots.compute_frontier`. When an empty
   list is passed, no frontier is highlighted — the previous
   behavior when a caller intentionally wanted no frontier.

4. **Frontier ID extraction accepts ``int`` ``agent_id``.** The
   previous implementation only accepted ``str`` and synthesized
   positional IDs the explorer never matched against, silently
   classifying every point as "dominated".

5. **Mixed-timezone timestamps no longer crash** the context
   inference. Aware timestamps are preferred; naive ones are used only
   when no aware timestamps are present.

6. **Chaos-injected runs report ``trust=MEDIUM``, not ``HIGH``.** The
   observations are real, but the system was perturbed; claiming
   ``trust=high`` contradicted the "chaos-injected" note in the same
   subtitle.

7. **Export delegates to** :func:`renderers.export.save_figure_detailed`
   instead of re-implementing a version of it with stale bugs. ``Path``
   inputs, ``.hidden`` / ``out.`` extension edge cases, and Plotly
   image export without ``kaleido`` are all handled correctly.

8. **Empty-figure fallback uses the resolved backend**, not a
   duck-typed import. If neither backend is installed, the caller gets
   :class:`ParetoExplorerError` at construction time (matching
   :class:`ParetoExplorer`'s behavior).

9. **All inputs validated** — ``results``, ``pareto_front``, ``units``,
   ``export_path``, ``include_frontier_line``, ``context``,
   ``theme``. Failures raise :class:`ParetoExplorerError` (the same
   type the delegate raises) so a single ``except`` clause handles
   every error this function can produce.

10. **Honest docstring.** The previous "10 enhancements" list claimed
    six features this file never implemented (``route``,
    ``precision``, ``deployment_id``, reviewer metadata, a separate
    carbon plot, adaptive precision). Those claims have been removed.
    What remains is what the function actually does.

11. **Smoke test with ``_check()``** that survives ``python -O``.

Compatibility
-------------
- ``plot_accuracy_vs_energy(results, pareto_front)`` — works with the
  same positional arguments as before.
- ``plot_accuracy_vs_energy(results)`` — new: auto-computes the
  frontier. Previously this was a ``TypeError`` because
  ``pareto_front`` was required.
- ``plot_accuracy_vs_energy(results, [])`` — explicitly no frontier,
  matching the previous behavior when a caller passed an empty list.
- ``context``, ``theme``, ``backend``, ``export_path``,
  ``include_frontier_line``, ``units`` — all remain keyword-only
  arguments with the same meaning as documented in the previous
  revision.
- Return type is unchanged: a Plotly or matplotlib figure, never
  ``None``.

Requires Python 3.8+.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime
from pathlib import Path
from typing import (
    Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple,
    Union,
)

from .contracts.chart_schema import (
    ChartContext,
    DataSource,
    TrustLevel,
)
from .contracts.theme import DEFAULT_THEME, VisualTheme
from .renderers.export import ExportError, save_figure_detailed
from .renderers.pareto_plots import (
    ParetoExplorer,
    ParetoExplorerError,
    compute_frontier,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def plot_accuracy_vs_energy(
    results: Sequence[Mapping[str, Any]],
    pareto_front: Optional[Sequence[Mapping[str, Any]]] = None,
    *,
    context: Optional[ChartContext] = None,
    theme: Optional[VisualTheme] = None,
    backend: str = "plotly",
    export_path: Optional[Union[str, "Path"]] = None,
    include_frontier_line: bool = True,
    units: Optional[Mapping[str, str]] = None,
) -> Any:
    """
    Render the accuracy vs energy Pareto scatter.

    Parameters
    ----------
    results : sequence of mappings
        All evaluated points. Each mapping must carry a numeric
        ``energy`` and a numeric ``accuracy``; optional fields include
        ``agent_id`` (used as the point identifier),
        ``simulated`` / ``chaos_injected`` (used to infer provenance),
        ``source``, ``system_version``, ``policy_version``,
        ``run_id``, and ``timestamp``.
    pareto_front : sequence of mappings, optional
        The subset of ``results`` that is Pareto-optimal. When
        ``None``, the frontier is computed automatically from
        ``results`` (energy minimized, accuracy maximized). When an
        empty list is supplied, no frontier is highlighted.
    context : ChartContext, optional
        Describes the data source, units, baseline, and versions. When
        omitted, a context is inferred from the flags on ``results``
        (see :func:`_infer_context`).
    theme : VisualTheme, optional
        Defaults to :data:`contracts.theme.DEFAULT_THEME`.
    backend : ``"plotly"`` or ``"matplotlib"``, default ``"plotly"``
        Passed to :class:`ParetoExplorer`, which validates it via
        :func:`renderers.base.select_backend`.
    export_path : str or Path, optional
        When provided, the figure is written to this path. The format
        is inferred from the extension. Export failures are logged but
        not raised — a chart that has already rendered should not be
        lost to an export problem.
    include_frontier_line : bool, default ``True``
        When True and the frontier has at least two points, a line is
        drawn through them, ordered by ascending ``energy``.
    units : mapping of str → str, optional
        Overrides the default axis-unit labels
        (``{"energy": "Wh", "accuracy": "ratio"}``).

    Returns
    -------
    Plotly Figure or matplotlib Figure
        Never ``None``. When ``results`` is empty, an annotated empty
        figure is returned (with the same backend as the resolved
        ``backend`` argument).

    Raises
    ------
    ParetoExplorerError
        If any input is malformed (non-iterable ``results``, non-mapping
        entries that survive the dict filter and cause a downstream
        failure, invalid ``backend``, non-mapping ``units``, invalid
        ``export_path``, non-bool ``include_frontier_line``), or if
        neither renderer backend is installed.
    """
    # ---- Validate every input ---------------------------------- #
    results = _normalise_results(results)
    pareto_front = _normalise_frontier(pareto_front)

    if not isinstance(include_frontier_line, bool):
        raise ParetoExplorerError(
            f"include_frontier_line must be a bool, got "
            f"{type(include_frontier_line).__name__}."
        )

    if context is not None and not isinstance(context, ChartContext):
        raise ParetoExplorerError(
            f"context must be a ChartContext or None, got "
            f"{type(context).__name__}."
        )

    if theme is not None and not isinstance(theme, VisualTheme):
        raise ParetoExplorerError(
            f"theme must be a VisualTheme or None, got "
            f"{type(theme).__name__}."
        )
    effective_theme = theme or DEFAULT_THEME

    if units is not None:
        if not isinstance(units, Mapping):
            raise ParetoExplorerError(
                f"units must be a Mapping or None, got "
                f"{type(units).__name__}."
            )
        for k, v in units.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise ParetoExplorerError(
                    f"units must map str to str; got "
                    f"{k!r}: {v!r}."
                )

    if export_path is not None:
        if not isinstance(export_path, (str, Path)) or not export_path:
            raise ParetoExplorerError(
                f"export_path must be a non-empty str or Path, got "
                f"{export_path!r}."
            )

    # ---- Build the explorer (validates backend) --------------- #
    explorer = ParetoExplorer(
        theme=effective_theme, backend=backend,
    )

    # ---- Empty results → annotated empty figure --------------- #
    if not results:
        logger.warning(
            "plot_accuracy_vs_energy called with no results; "
            "returning an annotated empty figure.",
        )
        return _empty_figure(
            title="Accuracy vs Energy (no data)",
            context=context,
            theme=effective_theme,
            backend=explorer.backend,
        )

    # ---- Infer a ChartContext when none was supplied ---------- #
    effective_context = context or _infer_context(results, units)

    # ---- Resolve frontier IDs --------------------------------- #
    if pareto_front is None:
        frontier_ids: List[str] = compute_frontier(
            results, x="energy", y="accuracy",
        )
    elif not pareto_front:
        frontier_ids = []
    else:
        frontier_ids = _extract_frontier_ids(pareto_front)

    # ---- Render ---------------------------------------------- #
    x_label = (units or {}).get("energy", "Energy (Wh)")
    y_label = (units or {}).get("accuracy", "Accuracy")

    fig = explorer.plot_tradeoff(
        points=results,
        x="energy",
        y="accuracy",
        frontier_ids=frontier_ids,
        x_label=x_label,
        y_label=y_label,
        title="Accuracy vs Energy (Green Pareto Frontier)",
        context=effective_context,
    )

    # ---- Optional frontier line (post-processing) ------------ #
    if include_frontier_line and len(frontier_ids) >= 2:
        _add_frontier_line(
            fig,
            backend=explorer.backend,
            results=results,
            frontier_ids=frontier_ids,
            theme=effective_theme,
        )

    # ---- Optional export ------------------------------------- #
    if export_path is not None:
        _try_export(fig, export_path)

    return fig


# --------------------------------------------------------------------------- #
# Input normalisation
# --------------------------------------------------------------------------- #
def _normalise_results(
    results: Any,
) -> List[Mapping[str, Any]]:
    """Coerce ``results`` to a list of mappings, dropping non-mappings."""
    if isinstance(results, (str, bytes)):
        raise ParetoExplorerError(
            f"results must be a sequence of mappings, not a string; "
            f"got {type(results).__name__}."
        )
    if not isinstance(results, Iterable):
        raise ParetoExplorerError(
            f"results must be iterable, got {type(results).__name__}."
        )
    filtered = [r for r in results if isinstance(r, Mapping)]
    dropped = sum(1 for _ in results) if False else 0  # avoid double-consume
    if not filtered:
        return []
    return filtered


def _normalise_frontier(
    pareto_front: Any,
) -> Optional[List[Mapping[str, Any]]]:
    """
    Coerce ``pareto_front`` to a list of mappings.

    ``None`` is preserved (meaning "auto-compute"). An empty list is
    preserved (meaning "no frontier"). Non-mapping entries are
    filtered out with a warning.
    """
    if pareto_front is None:
        return None
    if isinstance(pareto_front, (str, bytes)):
        raise ParetoExplorerError(
            f"pareto_front must be a sequence of mappings, not a "
            f"string; got {type(pareto_front).__name__}."
        )
    if not isinstance(pareto_front, Iterable):
        raise ParetoExplorerError(
            f"pareto_front must be iterable, got "
            f"{type(pareto_front).__name__}."
        )
    entries = list(pareto_front)
    filtered = [r for r in entries if isinstance(r, Mapping)]
    dropped = len(entries) - len(filtered)
    if dropped:
        logger.warning(
            "%d/%d pareto_front entries are not mappings; dropping.",
            dropped, len(entries),
        )
    return filtered


# --------------------------------------------------------------------------- #
# Frontier ID extraction
# --------------------------------------------------------------------------- #
def _extract_frontier_ids(
    pareto_front: Sequence[Mapping[str, Any]],
    *,
    id_key: str = "agent_id",
) -> List[str]:
    """
    Extract the identifiers of the supplied frontier entries.

    Accepts both ``str`` and ``int`` ``agent_id`` (coerced to ``str``).
    Entries without ``id_key`` are dropped with a ``WARNING`` — they
    cannot be matched to a plotted point, so fabricating a positional
    ID (the previous behavior) silently misclassified every point.
    """
    ids: List[str] = []
    missing = 0
    for r in pareto_front:
        aid = r.get(id_key)
        if aid is None:
            missing += 1
            continue
        ids.append(str(aid))
    if missing:
        logger.warning(
            "%d/%d pareto_front entries lack %r; those entries cannot "
            "be highlighted.",
            missing, len(pareto_front), id_key,
        )
    return ids


# --------------------------------------------------------------------------- #
# Context inference
# --------------------------------------------------------------------------- #
def _infer_context(
    results: Sequence[Mapping[str, Any]],
    units: Optional[Mapping[str, str]],
) -> ChartContext:
    """
    Derive a :class:`ChartContext` from the flags present on the
    results.

    Simulated values, chaos-injected runs, and missing sources are all
    surfaced so a reader cannot mistake a synthetic plot for a measured
    one. Mixed-timezone timestamp sets are normalised so that a naive
    timestamp and an aware timestamp in the same result set do not
    crash ``min()`` / ``max()``.
    """
    # ---- Simulated / chaos detection ------------------------- #
    simulated = any(bool(r.get("simulated", False)) for r in results)
    chaos_injected = any(
        bool(r.get("chaos_injected") or r.get("chaos_event"))
        for r in results
    )

    # ---- Source inference ------------------------------------ #
    sources = {
        str(r.get("source", "")).lower()
        for r in results
    }
    sources.discard("")

    if simulated or "fallback" in sources or "simulated" in sources:
        source_kind = DataSource.SIMULATED.value
        trust = TrustLevel.LOW.value
    elif chaos_injected:
        # Observations are real, but the system was perturbed; do not
        # claim high trust for chaos-injected data.
        source_kind = DataSource.MEASURED.value
        trust = TrustLevel.MEDIUM.value
    elif sources == {"measured"} or sources == {"codecarbon"}:
        source_kind = DataSource.MEASURED.value
        trust = TrustLevel.HIGH.value
    elif "estimated" in sources:
        source_kind = DataSource.ESTIMATED.value
        trust = TrustLevel.MEDIUM.value
    else:
        source_kind = DataSource.UNKNOWN.value
        trust = TrustLevel.LOW.value

    # ---- Versions (first non-null wins) ---------------------- #
    system_version = next(
        (r.get("system_version") for r in results
         if r.get("system_version")),
        None,
    )
    policy_version = next(
        (r.get("policy_version") for r in results
         if r.get("policy_version")),
        None,
    )
    run_id = next(
        (r.get("run_id") for r in results if r.get("run_id")),
        None,
    )

    # ---- Time range (mixed-tz safe) -------------------------- #
    ts_start, ts_end = _time_range(results)

    # ---- Units ----------------------------------------------- #
    effective_units: Dict[str, str] = {
        "energy": "Wh",
        "accuracy": "ratio",
    }
    if units:
        effective_units.update(dict(units))

    # ---- Notes ----------------------------------------------- #
    notes_parts: List[str] = []
    if chaos_injected:
        notes_parts.append("chaos-injected")
    if simulated:
        notes_parts.append("contains simulated values")
    notes = "; ".join(notes_parts) if notes_parts else None

    return ChartContext(
        source=source_kind,
        trust_level=trust,
        system_version=system_version,
        policy_version=policy_version,
        run_id=run_id,
        time_range_start=ts_start,
        time_range_end=ts_end,
        units=effective_units,
        baseline=None,
        simulated=simulated,
        notes=notes,
    )


def _time_range(
    results: Sequence[Mapping[str, Any]],
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Return ``(min_timestamp, max_timestamp)`` across ``results``.

    Aware timestamps are preferred; when the result set mixes aware and
    naive timestamps, the naive ones are dropped with a ``WARNING``
    rather than crashing ``min()`` / ``max()`` with a ``TypeError``.
    """
    aware: List[datetime] = []
    naive: List[datetime] = []
    for r in results:
        ts = r.get("timestamp")
        if not isinstance(ts, datetime):
            continue
        if ts.tzinfo is not None:
            aware.append(ts)
        else:
            naive.append(ts)

    if aware and naive:
        logger.warning(
            "mixed timezone awareness in timestamps (%d aware, "
            "%d naive); using only the aware ones.",
            len(aware), len(naive),
        )
        chosen = aware
    else:
        chosen = aware or naive

    if not chosen:
        return None, None
    return min(chosen), max(chosen)


# --------------------------------------------------------------------------- #
# Frontier line post-processing
# --------------------------------------------------------------------------- #
def _frontier_coords(
    results: Sequence[Mapping[str, Any]],
    frontier_ids: Sequence[str],
    *,
    id_key: str = "agent_id",
    x_field: str = "energy",
    y_field: str = "accuracy",
) -> List[Tuple[float, float]]:
    """Return ``[(x, y)]`` for frontier points with finite coordinates."""
    id_set = frozenset(frontier_ids)
    coords: List[Tuple[float, float]] = []
    for r in results:
        aid = r.get(id_key)
        if aid is None or str(aid) not in id_set:
            continue
        xv = _finite_or_none(r.get(x_field))
        yv = _finite_or_none(r.get(y_field))
        if xv is None or yv is None:
            continue
        coords.append((xv, yv))
    return coords


def _finite_or_none(value: Any) -> Optional[float]:
    """Return ``float(value)`` when finite and non-boolean, else ``None``."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    fv = float(value)
    return fv if math.isfinite(fv) else None


def _add_frontier_line(
    fig: Any,
    *,
    backend: str,
    results: Sequence[Mapping[str, Any]],
    frontier_ids: Sequence[str],
    theme: VisualTheme,
) -> None:
    """
    Draw a line through the frontier points, ordered by ascending
    ``energy``.

    Mutates ``fig`` in place. Silent when there are fewer than two
    distinct frontier coordinates.
    """
    coords = _frontier_coords(results, frontier_ids)
    if len(coords) < 2:
        return
    coords_sorted = sorted(coords, key=lambda c: c[0])
    xs = [c[0] for c in coords_sorted]
    ys = [c[1] for c in coords_sorted]

    if backend == "plotly":
        import plotly.graph_objects as go  # type: ignore
        fig.add_trace(go.Scatter(
            x=xs, y=ys,
            mode="lines",
            name="frontier line",
            line=dict(
                color=theme.palette.frontier,
                width=theme.line_width,
            ),
            hoverinfo="skip",
            showlegend=False,
        ))
        return

    # matplotlib
    axes = getattr(fig, "axes", None)
    if not axes:
        return
    axes[0].plot(
        xs, ys,
        color=theme.palette.frontier,
        linewidth=theme.line_width,
        zorder=1,  # below the scatter markers
    )


# --------------------------------------------------------------------------- #
# Empty figure
# --------------------------------------------------------------------------- #
def _empty_figure(
    *,
    title: str,
    context: Optional[ChartContext],
    theme: VisualTheme,
    backend: str,
) -> Any:
    """
    Render an annotated empty figure on the resolved backend.

    The backend is provided by the caller (typically the resolved
    :attr:`ParetoExplorer.backend`), so this function never needs to
    probe for availability — it will only run when the backend is
    known to be installed.
    """
    subtitle = (
        context.to_subtitle() if context is not None else "no data"
    )

    if backend == "plotly":
        import plotly.graph_objects as go  # type: ignore
        fig = go.Figure()
        fig.add_annotation(
            text=f"{title}<br><sub>{subtitle}</sub>",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color="#888"),
        )
        fig.update_layout(
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            plot_bgcolor=theme.palette.background,
        )
        return fig

    import matplotlib.pyplot as plt  # type: ignore
    fig, ax = plt.subplots(
        figsize=(
            theme.figure_width_px / 100,
            theme.figure_height_px / 100,
        )
    )
    ax.text(
        0.5, 0.5, f"{title}\n{subtitle}",
        ha="center", va="center", color="#888",
    )
    ax.set_xticks([])
    ax.set_yticks([])
    return fig


# --------------------------------------------------------------------------- #
# Export
# --------------------------------------------------------------------------- #
def _try_export(
    fig: Any,
    export_path: Union[str, "Path"],
) -> None:
    """
    Best-effort export. Logs on failure; never raises.

    A chart that has already rendered must not be lost to an export
    failure — the caller asked for a figure, not for an exception.
    """
    try:
        result = save_figure_detailed(fig, export_path)
    except ExportError as exc:
        logger.warning(
            "export to %r rejected: %s",
            str(export_path), exc,
        )
        return
    except Exception:
        logger.exception(
            "export to %r failed unexpectedly", str(export_path),
        )
        return
    if not result["ok"]:
        logger.warning(
            "export to %r failed (%s): %s",
            str(export_path), result["error_type"], result["error"],
        )


# --------------------------------------------------------------------------- #
# Public API surface
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "ParetoExplorerError",
    "plot_accuracy_vs_energy",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m visualization.energy_pareto
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys
    from datetime import timezone
    from unittest.mock import MagicMock, patch

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

    # ---- 1. Frontier ID extraction (bug fix) ------------------- #
    print("frontier ID extraction:")
    ids = _extract_frontier_ids([{"agent_id": "A"}, {"agent_id": "B"}])
    _check("str agent_id → str IDs", ids == ["A", "B"])

    ids = _extract_frontier_ids([{"agent_id": 1}, {"agent_id": 2}])
    _check("int agent_id → str IDs", ids == ["1", "2"])

    ids = _extract_frontier_ids([{"agent_id": "A"}, {}])
    _check(
        "missing agent_id dropped (no positional synthesis)",
        ids == ["A"],
    )

    _check("empty frontier → empty list", _extract_frontier_ids([]) == [])

    # ---- 2. Context inference — simulated ---------------------- #
    print("context inference (simulated):")
    ctx = _infer_context(
        [
            {"energy": 1.0, "accuracy": 0.9, "simulated": True},
            {"energy": 2.0, "accuracy": 0.8},
        ],
        None,
    )
    _check("source == simulated", ctx.source == "simulated")
    _check("trust == low", ctx.trust_level == "low")
    _check("simulated flag set", ctx.simulated is True)
    _check(
        "subtitle mentions simulated",
        "simulated" in ctx.to_subtitle().lower(),
    )

    # ---- 3. Context inference — chaos ------------------------- #
    print("context inference (chaos):")
    ctx = _infer_context(
        [
            {"energy": 1.0, "accuracy": 0.9,
             "source": "measured", "chaos_injected": True},
        ],
        None,
    )
    _check("source == measured", ctx.source == "measured")
    _check(
        "trust == medium (not high) for chaos-injected",
        ctx.trust_level == "medium",
    )
    _check(
        "notes mention chaos",
        ctx.notes is not None and "chaos" in ctx.notes.lower(),
    )

    # ---- 4. Context inference — measured --------------------- #
    print("context inference (measured):")
    ctx = _infer_context(
        [
            {"energy": 1.0, "accuracy": 0.9, "source": "measured"},
            {"energy": 2.0, "accuracy": 0.8, "source": "measured"},
        ],
        None,
    )
    _check("source == measured", ctx.source == "measured")
    _check("trust == high", ctx.trust_level == "high")

    # ---- 5. Mixed-timezone timestamps ------------------------ #
    print("mixed-timezone timestamps:")
    naive = datetime(2026, 1, 1, 12, 0, 0)
    aware = datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
    try:
        ctx = _infer_context(
            [
                {"energy": 1.0, "accuracy": 0.9, "timestamp": naive},
                {"energy": 2.0, "accuracy": 0.8, "timestamp": aware},
            ],
            None,
        )
        _check(
            "mixed timestamps do not raise",
            True,
        )
        _check(
            "aware timestamp wins",
            ctx.time_range_start == aware,
        )
    except TypeError:
        _check("mixed timestamps do not raise", False)

    # ---- 6. Units validation ---------------------------------- #
    print("units validation:")
    results = [{"energy": 1.0, "accuracy": 0.9}]

    try:
        _normalise_results("not-a-list")
        _check("string results rejected", False)
    except ParetoExplorerError:
        _check("string results rejected", True)

    try:
        _normalise_results(None)  # type: ignore[arg-type]
        _check("None results rejected", False)
    except ParetoExplorerError:
        _check("None results rejected", True)

    # Filtering non-mappings
    mixed = [
        {"energy": 1.0, "accuracy": 0.9},
        "not-a-mapping",
        {"energy": 2.0, "accuracy": 0.8},
    ]
    filtered = _normalise_results(mixed)  # type: ignore[arg-type]
    _check("non-mapping entries filtered", len(filtered) == 2)

    # ---- 7. Frontier coordinate extraction -------------------- #
    print("frontier coords:")
    coords = _frontier_coords(
        [
            {"agent_id": "A", "energy": 1.0, "accuracy": 0.9},
            {"agent_id": "B", "energy": 2.0, "accuracy": 0.8},
            {"agent_id": "C", "energy": float("nan"), "accuracy": 0.95},
        ],
        ["A", "B", "C"],
    )
    _check(
        "coords extracted for finite points",
        coords == [(1.0, 0.9), (2.0, 0.8)],
    )

    coords = _frontier_coords(
        [{"agent_id": "A", "energy": 1.0, "accuracy": 0.9}],
        ["A"],
    )
    _check("single-point frontier returns 1 coord", len(coords) == 1)

    # ---- 8. Integration with mocked ParetoExplorer ------------ #
    print("integration (mocked):")
    import sys as _sys
    this_module = _sys.modules[__name__]

    fake_fig = MagicMock()
    fake_fig.add_trace = MagicMock()
    fake_fig.axes = [MagicMock()]

    with patch.object(this_module, "ParetoExplorer") as mock_explorer_cls:
        mock_explorer = MagicMock()
        mock_explorer.backend = "plotly"
        mock_explorer.theme = DEFAULT_THEME
        mock_explorer.plot_tradeoff = MagicMock(return_value=fake_fig)
        mock_explorer_cls.return_value = mock_explorer

        # Default call: auto-compute frontier
        fig = plot_accuracy_vs_energy(
            [
                {"agent_id": "A", "energy": 1.0, "accuracy": 0.7},
                {"agent_id": "B", "energy": 2.0, "accuracy": 0.8},
                {"agent_id": "C", "energy": 3.0, "accuracy": 0.9},
            ],
        )
        _check("returns the explorer's figure", fig is fake_fig)
        _check(
            "plot_tradeoff called once",
            mock_explorer.plot_tradeoff.call_count == 1,
        )
        call_kwargs = mock_explorer.plot_tradeoff.call_args.kwargs
        _check("x=energy", call_kwargs["x"] == "energy")
        _check("y=accuracy", call_kwargs["y"] == "accuracy")
        _check(
            "title preserved",
            "Accuracy vs Energy" in call_kwargs["title"],
        )
        _check(
            "frontier line added for 3-point frontier",
            fake_fig.add_trace.call_count == 1,
        )

        # Explicit empty frontier → no line
        fake_fig.add_trace.reset_mock()
        plot_accuracy_vs_energy(
            [
                {"agent_id": "A", "energy": 1.0, "accuracy": 0.7},
            ],
            [],
        )
        _check(
            "empty frontier → no line",
            fake_fig.add_trace.call_count == 0,
        )

        # Explicit frontier with int IDs
        fake_fig.add_trace.reset_mock()
        plot_accuracy_vs_energy(
            [
                {"agent_id": 1, "energy": 1.0, "accuracy": 0.7},
                {"agent_id": 2, "energy": 2.0, "accuracy": 0.8},
            ],
            [
                {"agent_id": 1, "energy": 1.0, "accuracy": 0.7},
                {"agent_id": 2, "energy": 2.0, "accuracy": 0.8},
            ],
        )
        _check(
            "int frontier IDs produce a line",
            fake_fig.add_trace.call_count == 1,
        )

    # ---- 9. Empty results ------------------------------------ #
    print("empty results:")
    with patch.object(this_module, "ParetoExplorer") as mock_explorer_cls:
        mock_explorer = MagicMock()
        mock_explorer.backend = "plotly"
        mock_explorer.theme = DEFAULT_THEME
        mock_explorer_cls.return_value = mock_explorer

        with patch.object(this_module, "_empty_figure") as mock_empty:
            mock_empty.return_value = "empty_fig"
            fig = plot_accuracy_vs_energy([])
            _check(
                "empty results → _empty_figure",
                fig == "empty_fig",
            )
            _check(
                "_empty_figure called once",
                mock_empty.call_count == 1,
            )
            _check(
                "backend passed through",
                mock_empty.call_args.kwargs["backend"] == "plotly",
            )

    # ---- 10. Error type -------------------------------------- #
    print("error type:")
    _check(
        "ParetoExplorerError is ValueError",
        issubclass(ParetoExplorerError, ValueError),
    )

    # ---- 11. __all__ / __dir__ ------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    _check(
        "plot_accuracy_vs_energy exported",
        "plot_accuracy_vs_energy" in __all__,
    )
    _check(
        "ParetoExplorerError exported",
        "ParetoExplorerError" in __all__,
    )

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
