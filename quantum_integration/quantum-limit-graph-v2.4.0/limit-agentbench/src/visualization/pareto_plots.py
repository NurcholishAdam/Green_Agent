# src/visualization/pareto_plots.py

"""
Pareto scatter plots for Green Agent.

Two of the folder's classic Pareto scatter views, delegating the actual
scatter rendering to the consolidated
:class:`renderers.pareto_plots.ParetoExplorer`, and the wrapper
plumbing to :mod:`renderers._wrappers`:

- :func:`plot_accuracy_vs_carbon` — carbon on X, accuracy on Y.
- :func:`plot_latency_vs_energy`  — energy on X, latency on Y.

Axis convention
---------------
The public functions are named ``plot_X_vs_Y`` where **``X`` is the
horizontal axis** and **``Y`` is the vertical axis** — matching the
common plotting convention (``plot_price_vs_time`` puts price on Y,
time on X). The docstring of each function states its axes explicitly.

Original API (preserved)::

    plot_accuracy_vs_carbon(results)   # returned None; called plt.show()
    plot_latency_vs_energy(results)    # returned None; called plt.show()

Enhanced API (all keyword-only beyond ``results`` / ``pareto``)::

    plot_accuracy_vs_carbon(
        results,
        pareto=None,        # None → auto-compute; [] → no frontier
        *,
        frontier_ids=None,  # alternative to pareto; IDs only
        context=None,
        theme=None,
        backend="plotly",
        export_path=None,
        include_frontier_line=True,
        units=None,
    ) -> Figure

``plot_latency_vs_energy`` accepts the same keyword-only arguments.

``units`` semantics
-------------------
``units`` **only labels the axes**. It is not a scaling parameter: a
caller who passes ``units={"accuracy": "percent"}`` while their
``accuracy`` field is a ratio in ``[0, 1]`` gets an axis labelled
"percent" with ticks like ``0.85``, ``0.90``, ``0.95`` — a mismatch the
reader will notice. Pre-scale the values yourself, or pass
``units={"accuracy": "ratio"}`` and let the reader interpret.
:func:`_warn_if_unit_mismatch` emits a ``WARNING`` for the most common
case (accuracy labelled "percent" with all values below ``2.0``, which
is almost always a ratio).

Frontier semantics
------------------
- ``frontier_ids`` supplied → use those IDs (overrides ``pareto``).
- ``pareto is None`` (default) → **auto-compute** the frontier from
  ``results``. This is a behavior change from the pre-enhancement
  revision, which rendered every point as "dominated" when ``pareto``
  was omitted. Pass ``pareto=[]`` to restore the old behavior.
- ``pareto == []`` → explicitly no frontier highlighted.
- ``pareto == [...]`` → use the supplied frontier entries' ``agent_id``.

Carbon-axis behavior
--------------------
:func:`plot_accuracy_vs_carbon` plots **carbon on the X axis**. Points
that carry only the enhanced ``carbon_operational_kg`` field are
promoted to ``carbon`` via :func:`_carbon_axis_normalizer`, so they
appear on the chart instead of being silently dropped (which was the
pre-enhancement behavior — the old normaliser only fired when carbon
was on the Y axis, and it never is here). When a single chart mixes
``carbon`` and ``carbon_operational_kg`` sources a ``WARNING`` is
logged.

Behavior changes over the previous revision
-------------------------------------------
1. Import paths fixed (``contracts.theme``, ``renderers.pareto_plots``,
   ``renderers._wrappers``).
2. Carbon normaliser works regardless of axis position — silent drop of
   ``carbon_operational_kg``-only points is fixed.
3. ``int`` ``agent_id`` works — the shared ``extract_frontier_ids``
   coerces IDs to ``str`` and drops entries without an identifier with
   a ``WARNING``.
4. Mixed-timezone timestamps no longer crash context inference — the
   shared ``_time_range`` prefers aware timestamps and warns on mix.
5. Chaos-injected runs report ``trust=MEDIUM``, not ``HIGH``.
6. ``pareto`` defaults to ``None`` (auto-compute) instead of ``()``
   (no frontier).
7. ``frontier_ids`` is a real parameter (previously advertised in the
   docstring but not accepted).
8. ``include_frontier_line`` is implemented via post-processing (the
   previous docstring advertised it; the previous code never drew a
   line).
9. Export delegates to ``renderers.export.save_figure_detailed``.
10. Empty-figure fallback uses the resolved backend; a machine with
    neither backend installed raises ``ParetoExplorerError`` at
    construction rather than leaking ``ModuleNotFoundError``.
11. All parameters validated: ``results``, ``pareto``, ``units``,
    ``export_path``, ``include_frontier_line``, ``context``,
    ``theme``, ``backend``, ``frontier_ids``.
12. Five duplicated helpers removed — they live in
    ``renderers._wrappers`` and are shared with ``energy_pareto`` and
    ``leaderboard_plots``.
13. **``units`` is validated before label extraction**, so a malformed
    ``units`` raises ``ParetoExplorerError`` regardless of whether
    ``context`` was supplied. Previously the failure mode depended on
    that unrelated argument.
14. **``ChartContext.units`` is filtered to the active axes** when the
    context is inferred locally. A latency chart no longer carries
    "accuracy" or "carbon" in its units dict. A caller-supplied
    ``context`` is preserved untouched.
15. **``units``-vs-data mismatch is warned** for the most common case
    (accuracy labelled "percent" with ratio values).
16. Honest docstrings. The previous "10 enhancements" list claimed six
    features this file never implemented (``route``, ``precision``,
    ``deployment_id``, reviewer metadata, a separate carbon plot,
    adaptive precision). Those claims have been removed.

A note on the "consolidated" claim
----------------------------------
The scatter **rendering** is genuinely consolidated in
``ParetoExplorer``. The **wrappers** in this module are, however, a
parallel implementation alongside ``energy_pareto.py`` and
``leaderboard_plots.py``. This file shares the wrapper plumbing through
``renderers._wrappers``; the other two wrappers should be migrated to
the same shared module for full consolidation. Until then, both
``plot_accuracy_vs_carbon`` and ``plot_latency_vs_energy`` may
legitimately coexist with their counterparts in the sibling files,
provided callers import from one file consistently. A future
consolidation step would pick a canonical home per function name and
re-export from the others.

Compatibility
-------------
- ``plot_accuracy_vs_carbon(results)`` — same positional call.
  **Behavior change**: frontier auto-computed; pass ``pareto=[]`` to
  restore the old "no frontier" default.
- ``plot_latency_vs_energy(results)`` — same, plus ``pareto``,
  ``frontier_ids``, ``context``, ``theme``, ``backend``, ``export_path``,
  ``include_frontier_line``, ``units`` as keyword-only enhancements.
- Return type unchanged: a Plotly or matplotlib figure, never ``None``.

Requires Python 3.8+.
"""

from __future__ import annotations

import logging
import math
from dataclasses import replace
from pathlib import Path
from typing import (
    Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple,
    Union,
)

from .contracts.chart_schema import ChartContext
from .contracts.theme import VisualTheme
from .renderers._wrappers import (
    finite_or_none,
    infer_context,
    render_tradeoff_figure,
)
from .renderers.pareto_plots import ParetoExplorerError

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Units validation (local, small)
# --------------------------------------------------------------------------- #
def _validate_units(
    value: Any,
) -> Optional[Dict[str, str]]:
    """
    Return a validated, freshly-copied units dict (or ``None``).

    Duplicated from ``_wrappers._validate_units`` because this module
    needs to validate ``units`` **before** the label-extraction step,
    which runs before ``render_tradeoff_figure`` (the shared pipeline)
    re-validates them. The duplicate is a few lines; it keeps the
    pre-pipeline validation honest without requiring an
    ``_wrappers`` change.
    """
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ParetoExplorerError(
            f"units must be a Mapping or None, got "
            f"{type(value).__name__}."
        )
    result: Dict[str, str] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k:
            raise ParetoExplorerError(
                f"units keys must be non-empty strings, got {k!r}."
            )
        if not isinstance(v, str):
            raise ParetoExplorerError(
                f"units[{k!r}] must be a string, got "
                f"{type(v).__name__}."
            )
        result[k] = v
    return result


# --------------------------------------------------------------------------- #
# Context filtering
# --------------------------------------------------------------------------- #
def _filter_context_units(
    context: ChartContext,
    active_axes: Set[str],
) -> ChartContext:
    """
    Return a copy of ``context`` whose ``units`` covers only
    ``active_axes``.

    ``renderers._wrappers.infer_context`` populates a default units dict
    containing every known axis (energy, accuracy, latency, carbon). A
    latency chart therefore carries "accuracy" and "carbon" in its
    ``ChartContext.units`` — useful for nobody and misleading for
    downstream consumers that treat ``context.units.keys()`` as "the
    axes on this chart".

    This helper narrows the units to the chart's actual axes. It is
    applied only to inferred contexts — a caller-supplied ``context``
    is preserved unchanged.
    """
    filtered_units = {
        k: v for k, v in context.units.items() if k in active_axes
    }
    if filtered_units == context.units:
        return context
    return replace(context, units=filtered_units)


# --------------------------------------------------------------------------- #
# Units-vs-data mismatch warnings
# --------------------------------------------------------------------------- #
def _numeric_values(
    results: Sequence[Mapping[str, Any]], field: str,
) -> List[float]:
    """Return finite numeric values of ``field`` across ``results``."""
    values: List[float] = []
    for r in results:
        fv = finite_or_none(r.get(field))
        if fv is not None:
            values.append(fv)
    return values


def _warn_if_unit_mismatch(
    results: Sequence[Mapping[str, Any]],
    field: str,
    declared_unit: Optional[str],
) -> None:
    """
    Emit a ``WARNING`` when ``declared_unit`` contradicts the data's
    observable range.

    Only the two most common confusions are covered:

    - ``accuracy`` labelled ``"percent"`` with all values below ``2.0``
      (which is almost certainly a ratio in ``[0, 1]``).
    - ``accuracy`` labelled ``"ratio"`` with all values above ``2.0``
      (which is almost certainly a percentage).

    The check is advisory — the caller's intent is honored regardless.
    It exists so a chart that will visibly disagree with its own axis
    label is flagged at render time, not discovered by a reader.
    """
    if field != "accuracy":
        return
    if declared_unit is None:
        return

    values = _numeric_values(results, field)
    if not values:
        return

    unit_lc = declared_unit.strip().lower()

    if unit_lc in ("percent", "%", "percent (%)") and all(
        v < 2.0 for v in values
    ):
        logger.warning(
            "Accuracy is labelled %r but all values are < 2.0; if the "
            "underlying field is a ratio in [0, 1], either pass "
            "units={'accuracy': 'ratio'} or pre-scale the values by "
            "100 and pass 'percent'.",
            declared_unit,
        )
        return

    if unit_lc in ("ratio", "fraction") and all(
        v > 2.0 for v in values
    ):
        logger.warning(
            "Accuracy is labelled %r but all values are > 2.0; if the "
            "underlying field is a percentage, either pass "
            "units={'accuracy': 'percent'} or divide the values by "
            "100 and pass 'ratio'.",
            declared_unit,
        )


# --------------------------------------------------------------------------- #
# Carbon-axis normalisation (unique to this file's X-axis carbon chart)
# --------------------------------------------------------------------------- #
def _carbon_axis_normalizer(
    results: Sequence[Mapping[str, Any]],
    _y_field: str,
) -> List[Mapping[str, Any]]:
    """
    Promote ``carbon_operational_kg`` → ``carbon`` for every point.

    Unlike the pre-enhancement implementation, this works **regardless
    of which axis carbon appears on**. In :func:`plot_accuracy_vs_carbon`
    carbon is the *X* axis — the old normaliser only fired when the Y
    axis was carbon, so it never fired here and any point that carried
    only ``carbon_operational_kg`` was silently dropped.

    A ``WARNING`` is logged when a single chart ends up mixing the two
    sources, because the axis then displays two different metrics.
    """
    out: List[Mapping[str, Any]] = []
    from_operational = 0
    from_legacy = 0
    for r in results:
        p = dict(r)
        op = finite_or_none(p.get("carbon_operational_kg"))
        if op is not None:
            p["carbon"] = op
            from_operational += 1
            out.append(p)
            continue
        if finite_or_none(p.get("carbon")) is not None:
            from_legacy += 1
        out.append(p)

    if from_operational and from_legacy:
        logger.warning(
            "Carbon axis mixes %d point(s) sourced from "
            "'carbon_operational_kg' and %d point(s) sourced from "
            "'carbon'; the two are not directly comparable.",
            from_operational, from_legacy,
        )
    return out


# --------------------------------------------------------------------------- #
# Shared render body
# --------------------------------------------------------------------------- #
def _render_axis(
    results: Sequence[Mapping[str, Any]],
    pareto: Optional[Sequence[Mapping[str, Any]]],
    *,
    frontier_ids: Optional[Iterable[Any]],
    x: str,
    y: str,
    title: str,
    x_default_label: str,
    y_default_label: str,
    context: Optional[ChartContext],
    theme: Optional[VisualTheme],
    backend: str,
    export_path: Optional[Union[str, Path]],
    include_frontier_line: bool,
    units: Optional[Mapping[str, str]],
    point_normalizer: Optional[Any] = None,
    x_direction: str = "min",
    y_direction: str = "max",
) -> Any:
    """
    Validate ``units``, compute labels + filtered context, then dispatch
    to the shared pipeline.

    Kept local rather than in ``_wrappers.py`` because the label
    extraction and the context filtering are specific to the
    *two*-axis wrapper idiom; ``render_tradeoff_figure`` remains the
    general-purpose pipeline.
    """
    # ---- Units first — labels depend on it, so validate before use  #
    validated_units = _validate_units(units)

    x_label = (validated_units or {}).get(x, x_default_label)
    y_label = (validated_units or {}).get(y, y_default_label)

    # ---- Advisory warning when the declared unit contradicts data   #
    _warn_if_unit_mismatch(
        results, "accuracy", (validated_units or {}).get("accuracy"),
    )

    # ---- Filter inferred context to the chart's actual axes        #
    effective_context = context
    if effective_context is None:
        inferred = infer_context(results, validated_units)
        effective_context = _filter_context_units(
            inferred, active_axes={x, y},
        )

    # ---- Dispatch to the shared pipeline                           #
    return render_tradeoff_figure(
        results,
        pareto,
        frontier_ids=frontier_ids,
        x=x,
        y=y,
        title=title,
        x_label=x_label,
        y_label=y_label,
        context=effective_context,
        theme=theme,
        backend=backend,
        export_path=export_path,
        include_frontier_line=include_frontier_line,
        units=None,   # already consumed above; the shared pipeline
                      # does not need to re-infer units
        point_normalizer=point_normalizer,
        x_direction=x_direction,
        y_direction=y_direction,
    )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
def plot_accuracy_vs_carbon(
    results: Sequence[Mapping[str, Any]],
    pareto: Optional[Sequence[Mapping[str, Any]]] = None,
    *,
    frontier_ids: Optional[Iterable[Any]] = None,
    context: Optional[ChartContext] = None,
    theme: Optional[VisualTheme] = None,
    backend: str = "plotly",
    export_path: Optional[Union[str, Path]] = None,
    include_frontier_line: bool = True,
    units: Optional[Mapping[str, str]] = None,
) -> Any:
    """
    Accuracy vs carbon Pareto scatter.

    **Axes**: carbon on X, accuracy on Y. The title
    "Accuracy vs Carbon" follows the plotting convention "Y vs X".

    **Carbon axis**: drawn from ``carbon_operational_kg`` when that
    field is present and finite, otherwise from the legacy ``carbon``
    field. Points that carry only ``carbon_operational_kg`` are
    promoted so they appear on the chart — the pre-enhancement
    implementation silently dropped them. When a single chart mixes
    the two sources, a ``WARNING`` is logged.

    Parameters
    ----------
    results : sequence of mappings
        Points with numeric ``carbon`` (or ``carbon_operational_kg``)
        and ``accuracy`` fields.
    pareto : sequence of mappings, optional
        Frontier entries; ``None`` auto-computes, ``[]`` disables
        highlighting.
    frontier_ids : iterable of str / int, optional
        IDs to highlight; overrides ``pareto``.
    context : ChartContext, optional
        Provenance context. When supplied, its ``units`` are used
        as-is. When omitted, a context is inferred and its ``units``
        are narrowed to just ``{"carbon", "accuracy"}``.
    theme : VisualTheme, optional
        Defaults to ``contracts.theme.DEFAULT_THEME``.
    backend : ``"plotly"`` or ``"matplotlib"``, default ``"plotly"``
    export_path : str or Path, optional
        When provided, the figure is written to this path. Export
        failures are logged, not raised — a rendered chart must not be
        lost to an export problem.
    include_frontier_line : bool, default True
        When True and the frontier has at least two points, a line is
        drawn through them.
    units : Mapping[str, str], optional
        Axis label overrides. **Labels only** — this does not scale
        the underlying data. See the module docstring's "units
        semantics" section.

    Returns
    -------
    Plotly Figure or matplotlib Figure
        Never ``None``.

    Raises
    ------
    ParetoExplorerError
        If any input is malformed, or neither renderer backend is
        installed.
    """
    return _render_axis(
        results,
        pareto,
        frontier_ids=frontier_ids,
        x="carbon",
        y="accuracy",
        title="Accuracy vs Carbon",
        x_default_label="Carbon (kg CO₂)",
        y_default_label="Accuracy",
        context=context,
        theme=theme,
        backend=backend,
        export_path=export_path,
        include_frontier_line=include_frontier_line,
        units=units,
        point_normalizer=_carbon_axis_normalizer,
        x_direction="min",
        y_direction="max",
    )


def plot_latency_vs_energy(
    results: Sequence[Mapping[str, Any]],
    pareto: Optional[Sequence[Mapping[str, Any]]] = None,
    *,
    frontier_ids: Optional[Iterable[Any]] = None,
    context: Optional[ChartContext] = None,
    theme: Optional[VisualTheme] = None,
    backend: str = "plotly",
    export_path: Optional[Union[str, Path]] = None,
    include_frontier_line: bool = True,
    units: Optional[Mapping[str, str]] = None,
) -> Any:
    """
    Latency vs energy Pareto scatter.

    **Axes**: energy on X, latency on Y. The title
    "Latency vs Energy" follows the plotting convention "Y vs X".

    **Behavior change**: previously the function returned ``None`` and
    called ``plt.show()``; it now returns a figure. Frontier
    highlighting is on by default (auto-computed); pass ``pareto=[]``
    to restore the old "no highlighting" behavior.
    """
    return _render_axis(
        results,
        pareto,
        frontier_ids=frontier_ids,
        x="energy",
        y="latency",
        title="Latency vs Energy",
        x_default_label="Energy (Wh)",
        y_default_label="Latency (s)",
        context=context,
        theme=theme,
        backend=backend,
        export_path=export_path,
        include_frontier_line=include_frontier_line,
        units=units,
        x_direction="min",
        y_direction="min",
    )


# --------------------------------------------------------------------------- #
# Public API surface
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "ParetoExplorerError",
    "plot_accuracy_vs_carbon",
    "plot_latency_vs_energy",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m visualization.pareto_plots
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys
    from datetime import datetime, timezone
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

    # ---- 1. Units validation runs before label extraction ----- #
    print("units validation (both branches):")
    # Without context — units validated before inference
    try:
        plot_accuracy_vs_carbon(
            [{"carbon": 0.05, "accuracy": 0.9}],
            units={"carbon": 42},  # type: ignore[dict-item]
        )
        _check("bad units raises without context", False)
    except ParetoExplorerError:
        _check("bad units raises without context", True)

    # With context — previously silent-wrong
    try:
        plot_accuracy_vs_carbon(
            [{"carbon": 0.05, "accuracy": 0.9}],
            context=infer_context(
                [{"carbon": 0.05, "accuracy": 0.9}], None,
            ),
            units={"carbon": 42},  # type: ignore[dict-item]
        )
        _check("bad units raises WITH context", False)
    except ParetoExplorerError:
        _check("bad units raises WITH context", True)

    # units="kWh" (a string, not a Mapping)
    try:
        plot_accuracy_vs_carbon(
            [{"carbon": 0.05, "accuracy": 0.9}],
            units="kWh",  # type: ignore[arg-type]
        )
        _check("string units raises", False)
    except ParetoExplorerError:
        _check("string units raises", True)

    # ---- 2. Context units filtered to active axes ------------- #
    print("context units filtered to active axes:")
    from .renderers._wrappers import infer_context as _infer

    # Direct test of the helper
    raw = _infer(
        [{"carbon": 0.05, "accuracy": 0.9}], None,
    )
    _check(
        "raw infer_context has 4 units (superset)",
        len(raw.units) >= 4,
    )
    filtered = _filter_context_units(
        raw, active_axes={"carbon", "accuracy"},
    )
    _check(
        "filtered context only has chart's units",
        set(filtered.units.keys()) == {"carbon", "accuracy"},
    )
    _check(
        "filter preserves carbon unit value",
        filtered.units["carbon"] == raw.units["carbon"],
    )
    _check(
        "filter preserves accuracy unit value",
        filtered.units["accuracy"] == raw.units["accuracy"],
    )
    _check(
        "raw context unchanged",
        len(raw.units) >= 4,
    )

    # Same test for latency/energy axes
    raw_lat = _infer(
        [{"energy": 1.0, "latency": 0.2}], None,
    )
    filtered_lat = _filter_context_units(
        raw_lat, active_axes={"energy", "latency"},
    )
    _check(
        "latency filter keeps only energy/latency",
        set(filtered_lat.units.keys()) == {"energy", "latency"},
    )
    _check(
        "latency filter drops accuracy",
        "accuracy" not in filtered_lat.units,
    )
    _check(
        "latency filter drops carbon",
        "carbon" not in filtered_lat.units,
    )

    # ---- 3. Unit mismatch warning (accuracy percent vs ratio) - #
    print("unit mismatch warning:")
    import logging as _logging

    warn_records: List[_logging.LogRecord] = []

    class _CaptureHandler(_logging.Handler):
        def emit(self, record):
            warn_records.append(record)

    capture = _CaptureHandler()
    logger.addHandler(capture)
    try:
        # accuracy labelled "percent" but values are ratios < 2.0
        _warn_if_unit_mismatch(
            [{"accuracy": 0.85}, {"accuracy": 0.92}],
            "accuracy", "percent",
        )
        _check(
            "percent-vs-ratio mismatch warned",
            any("Accuracy is labelled" in r.message for r in warn_records),
        )
    finally:
        logger.removeHandler(capture)

    # No mismatch — values already in percent range
    warn_records.clear()
    logger.addHandler(capture)
    try:
        _warn_if_unit_mismatch(
            [{"accuracy": 85.0}, {"accuracy": 92.0}],
            "accuracy", "percent",
        )
        _check(
            "no warning when values match percent range",
            not any(
                "Accuracy is labelled" in r.message for r in warn_records
            ),
        )
    finally:
        logger.removeHandler(capture)

    # "ratio" label with percent-range values
    warn_records.clear()
    logger.addHandler(capture)
    try:
        _warn_if_unit_mismatch(
            [{"accuracy": 85.0}, {"accuracy": 92.0}],
            "accuracy", "ratio",
        )
        _check(
            "ratio-vs-percent mismatch warned",
            any("Accuracy is labelled" in r.message for r in warn_records),
        )
    finally:
        logger.removeHandler(capture)

    # Non-accuracy field — warning never fires
    warn_records.clear()
    logger.addHandler(capture)
    try:
        _warn_if_unit_mismatch(
            [{"latency": 0.2}], "latency", "ms",
        )
        _check(
            "non-accuracy field never warns",
            not any("Accuracy" in r.message for r in warn_records),
        )
    finally:
        logger.removeHandler(capture)

    # ---- 4. Carbon normalizer (X-axis fix) -------------------- #
    print("carbon normalizer (X-axis):")
    mixed = [
        {"agent_id": "A", "accuracy": 0.9,
         "carbon_operational_kg": 0.02},
        {"agent_id": "B", "accuracy": 0.8,
         "carbon": 0.05, "carbon_operational_kg": 0.03},
        {"agent_id": "C", "accuracy": 0.7, "carbon": 0.04},
    ]
    normalised = _carbon_axis_normalizer(mixed, "accuracy")
    _check(
        "operational-only point gets carbon populated",
        normalised[0]["carbon"] == 0.02,
    )
    _check(
        "operational preferred when both present",
        normalised[1]["carbon"] == 0.03,
    )
    _check(
        "legacy used when operational absent",
        normalised[2]["carbon"] == 0.04,
    )
    _check(
        "input dicts not mutated",
        "carbon" not in mixed[0],
    )

    # NaN operational falls back to legacy
    nan_case = [
        {"agent_id": "A", "accuracy": 0.9,
         "carbon": 0.05, "carbon_operational_kg": float("nan")},
    ]
    normalised = _carbon_axis_normalizer(nan_case, "accuracy")
    _check(
        "NaN operational falls back to legacy carbon",
        normalised[0]["carbon"] == 0.05,
    )

    # ---- 5. Integration with mocked ParetoExplorer ------------ #
    print("integration (mocked):")
    import sys as _sys
    this_module = _sys.modules[__name__]

    def _run(fn_name, *args, **kwargs):
        fake_fig = MagicMock()
        fake_fig.add_trace = MagicMock()
        fake_fig.axes = [MagicMock()]

        with patch(
            "visualization.renderers._wrappers.ParetoExplorer",
        ) as mock_cls:
            mock_explorer = MagicMock()
            mock_explorer.backend = "plotly"
            mock_explorer.plot_tradeoff = MagicMock(return_value=fake_fig)
            mock_cls.return_value = mock_explorer

            fn = getattr(this_module, fn_name)
            fig = fn(*args, **kwargs)
        return fig, mock_explorer, fake_fig

    # Default call — auto-compute frontier
    fig, explorer, fake_fig = _run(
        "plot_accuracy_vs_carbon",
        [
            {"agent_id": "A", "accuracy": 0.7, "carbon": 1.0},
            {"agent_id": "B", "accuracy": 0.8, "carbon": 2.0},
            {"agent_id": "C", "accuracy": 0.9, "carbon": 3.0},
        ],
    )
    _check("returns figure", fig is fake_fig)
    call_kwargs = explorer.plot_tradeoff.call_args.kwargs
    _check("x axis is carbon", call_kwargs["x"] == "carbon")
    _check("y axis is accuracy", call_kwargs["y"] == "accuracy")
    _check(
        "title preserved",
        call_kwargs["title"] == "Accuracy vs Carbon",
    )
    _check(
        "context.units filtered to carbon+accuracy",
        set(call_kwargs["context"].units.keys())
        == {"carbon", "accuracy"},
    )
    _check(
        "frontier line drawn for 3-point frontier",
        fake_fig.add_trace.call_count == 1,
    )

    # Point with only carbon_operational_kg is not dropped
    fig, explorer, fake_fig = _run(
        "plot_accuracy_vs_carbon",
        [
            {"agent_id": "A", "accuracy": 0.9,
             "carbon_operational_kg": 0.02},
            {"agent_id": "B", "accuracy": 0.8,
             "carbon_operational_kg": 0.03},
        ],
    )
    points_sent = explorer.plot_tradeoff.call_args.kwargs["points"]
    _check(
        "both operational-only points have carbon",
        all("carbon" in p for p in points_sent),
    )

    # Explicit empty pareto → no frontier
    fig, explorer, fake_fig = _run(
        "plot_accuracy_vs_carbon",
        [{"agent_id": "A", "accuracy": 0.9, "carbon": 0.05}],
        [],
    )
    call_kwargs = explorer.plot_tradeoff.call_args.kwargs
    _check(
        "empty pareto → frontier_ids=[]",
        call_kwargs["frontier_ids"] == [],
    )
    _check(
        "empty pareto → no frontier line",
        fake_fig.add_trace.call_count == 0,
    )

    # int agent_id in pareto
    fig, explorer, fake_fig = _run(
        "plot_accuracy_vs_carbon",
        [
            {"agent_id": 1, "accuracy": 0.7, "carbon": 1.0},
            {"agent_id": 2, "accuracy": 0.8, "carbon": 2.0},
        ],
        [
            {"agent_id": 1, "accuracy": 0.7, "carbon": 1.0},
            {"agent_id": 2, "accuracy": 0.8, "carbon": 2.0},
        ],
    )
    call_kwargs = explorer.plot_tradeoff.call_args.kwargs
    _check(
        "int pareto IDs coerced to str",
        sorted(call_kwargs["frontier_ids"]) == ["1", "2"],
    )

    # frontier_ids overrides pareto
    fig, explorer, fake_fig = _run(
        "plot_accuracy_vs_carbon",
        [
            {"agent_id": "A", "accuracy": 0.7, "carbon": 1.0},
            {"agent_id": "B", "accuracy": 0.8, "carbon": 2.0},
        ],
        pareto=[],
        frontier_ids=["B"],
    )
    _check(
        "frontier_ids overrides empty pareto",
        explorer.plot_tradeoff.call_args.kwargs["frontier_ids"] == ["B"],
    )

    # latency vs energy — context units filtered to energy+latency
    fig, explorer, fake_fig = _run(
        "plot_latency_vs_energy",
        [
            {"agent_id": "A", "energy": 1.0, "latency": 0.2},
            {"agent_id": "B", "energy": 2.0, "latency": 0.1},
        ],
    )
    call_kwargs = explorer.plot_tradeoff.call_args.kwargs
    _check(
        "latency context units filtered to energy+latency",
        set(call_kwargs["context"].units.keys())
        == {"energy", "latency"},
    )
    _check(
        "latency context drops accuracy",
        "accuracy" not in call_kwargs["context"].units,
    )

    # Caller-supplied context is preserved unchanged
    caller_ctx = _infer(
        [{"energy": 1.0, "latency": 0.2}], None,
    )
    _check(
        "caller's context carries all four units",
        len(caller_ctx.units) >= 4,
    )
    fig, explorer, fake_fig = _run(
        "plot_latency_vs_energy",
        [{"agent_id": "A", "energy": 1.0, "latency": 0.2}],
        context=caller_ctx,
    )
    _check(
        "caller-supplied context is preserved untouched",
        len(explorer.plot_tradeoff.call_args.kwargs["context"].units) >= 4,
    )

    # ---- 6. Empty results path -------------------------------- #
    print("empty results:")
    with patch(
        "visualization.renderers._wrappers.ParetoExplorer",
    ) as mock_cls:
        mock_explorer = MagicMock()
        mock_explorer.backend = "plotly"
        mock_cls.return_value = mock_explorer

        with patch(
            "visualization.renderers._wrappers.empty_figure",
        ) as mock_empty:
            mock_empty.return_value = "empty_fig"
            fig = plot_accuracy_vs_carbon([])
            _check(
                "empty results returns annotated figure",
                fig == "empty_fig",
            )

    # ---- 7. Mixed-timezone timestamps ------------------------ #
    print("mixed-timezone timestamps:")
    naive = datetime(2026, 1, 1, 12, 0, 0)
    aware = datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
    try:
        ctx = infer_context([
            {"carbon": 0.05, "accuracy": 0.9, "timestamp": naive},
            {"carbon": 0.06, "accuracy": 0.8, "timestamp": aware},
        ], None)
        _check("mixed timestamps do not raise", True)
        _check(
            "aware timestamp wins",
            ctx.time_range_start == aware,
        )
    except TypeError:
        _check("mixed timestamps do not raise", False)

    # ---- 8. Chaos trust downgrade ---------------------------- #
    print("chaos trust downgrade:")
    ctx = infer_context([
        {"source": "measured", "chaos_injected": True},
    ], None)
    _check(
        "chaos → trust=medium",
        ctx.trust_level == "medium",
    )

    # ---- 9. __all__ / __dir__ -------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "ParetoExplorerError",
        "plot_accuracy_vs_carbon",
        "plot_latency_vs_energy",
    ):
        _check(f"{_name} exported", _name in __all__)

    # ---- 10. Error type -------------------------------------- #
    print("error type:")
    _check(
        "ParetoExplorerError is ValueError",
        issubclass(ParetoExplorerError, ValueError),
    )

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
