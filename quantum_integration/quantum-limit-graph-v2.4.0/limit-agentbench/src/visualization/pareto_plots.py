# src/visualization/pareto_plots.py

"""
Pareto scatter plots for Green Agent.

Two of the folder's classic Pareto scatter views, delegating to the
consolidated :class:`renderers.pareto_plots.ParetoExplorer` via the
shared wrapper pipeline in :mod:`renderers._wrappers`:

- :func:`plot_accuracy_vs_carbon` — carbon vs accuracy.
- :func:`plot_latency_vs_energy`  — energy vs latency.

Original API (preserved)::

    plot_accuracy_vs_carbon(results)   # returned None; called plt.show()
    plot_latency_vs_energy(results)    # returned None; called plt.show()

Enhanced API (all keyword-only, all optional)::

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

Same keyword-only arguments for :func:`plot_latency_vs_energy`.

Frontier semantics
------------------
- ``frontier_ids`` supplied → use those IDs (overrides the others).
- ``pareto is None`` (default) → **auto-compute** the frontier from
  ``results``. This is a behavior change from the previous revision,
  which rendered every point as "dominated" when ``pareto`` was
  omitted. Pass ``pareto=[]`` to restore the previous behavior.
- ``pareto == []`` → explicitly no frontier highlighted.
- ``pareto == [...]`` → use the supplied frontier entries' ``agent_id``.

Carbon-axis behavior (unique to this file)
------------------------------------------
:func:`plot_accuracy_vs_carbon` plots **carbon on the X axis**. The
previous revision's normaliser only promoted
``carbon_operational_kg`` → ``carbon`` when the *Y* axis was carbon —
which is never true here. As a result, any point that carried only the
enhanced ``carbon_operational_kg`` field was silently dropped from the
plot with no error.

The enhanced file uses :func:`_carbon_axis_normalizer`, which promotes
``carbon_operational_kg`` → ``carbon`` regardless of axis position.
Points with only ``carbon_operational_kg`` now appear on the chart; a
``WARNING`` is logged when a single chart mixes sources.

Behavior changes over the previous revision
-------------------------------------------
1. **Import paths fixed** — the previous revision imported
   ``contracts.visual_theme`` and ``tradeoffs.pareto``; neither module
   exists in the enhanced tree. The correct paths are
   ``contracts.theme`` and ``renderers.pareto_plots``, and the shared
   pipeline lives in ``renderers._wrappers``.

2. **Carbon normaliser works on the X axis.** As above, a silent-drop
   bug on ``plot_accuracy_vs_carbon`` for points with only
   ``carbon_operational_kg``.

3. **``int`` ``agent_id`` works.** The shared
   :func:`renderers._wrappers.extract_frontier_ids` coerces IDs to
   ``str`` and drops entries without an identifier with a ``WARNING``.
   The previous revision silently misclassified every point when
   ``agent_id`` was an ``int``.

4. **Mixed-timezone timestamps no longer crash the context inference.**
   The shared :func:`renderers._wrappers.infer_context` prefers aware
   timestamps and warns on mixed sets.

5. **Chaos-injected runs report ``trust=MEDIUM``, not ``HIGH``.**
   Previously the subtitle could say "trust=high · chaos-injected", a
   self-contradiction.

6. **``pareto`` now defaults to ``None``** (auto-compute) instead of
   ``()`` (no frontier), matching :mod:`energy_pareto`'s enhanced
   semantics. Pass ``pareto=[]`` to restore the previous "no frontier"
   default.

7. **``frontier_ids`` is a real parameter.** The previous docstring
   advertised it but the functions didn't accept it. Both
   ``pareto=`` (full entries) and ``frontier_ids=`` (IDs only) are
   supported; ``frontier_ids`` takes precedence.

8. **``include_frontier_line`` is now implemented.** The previous
   revision's docstring advertised "Frontier highlighting (color +
   line)" but no line was ever drawn. The shared pipeline post-processes
   the figure via :func:`renderers._wrappers.add_frontier_line`.

9. **Export delegates to** :func:`renderers.export.save_figure_detailed`.
   ``Path`` inputs, ``.hidden`` / ``out.`` extension edge cases, and
   Plotly image export without ``kaleido`` are all handled correctly.

10. **Empty-figure fallback uses the resolved backend.** If neither
    backend is installed, the caller gets :class:`ParetoExplorerError`
    at construction time.

11. **All parameters validated.** ``results``, ``pareto``, ``units``,
    ``export_path``, ``include_frontier_line``, ``context``,
    ``theme``, ``backend``, ``frontier_ids``. Bad input raises
    :class:`ParetoExplorerError` before any rendering work is done.

12. **Five duplicated helpers removed.** ``_extract_frontier_ids``,
    ``_infer_context``, ``_empty_figure``, ``_save``, and
    ``_normalise_points`` now live in :mod:`renderers._wrappers` and
    are shared with :mod:`energy_pareto` and
    :mod:`leaderboard_plots`.

13. **Honest docstring.** The previous "10 enhancements" list claimed
    six features this file never implemented (``route``,
    ``deployment_id``, ``precision``, reviewer metadata, a separate
    carbon plot, adaptive precision). Those claims have been removed.
    What remains is what the module actually does.

Compatibility
-------------
- ``plot_accuracy_vs_carbon(results)`` — works with the same positional
  argument. **Behavior change**: frontier is now auto-computed; pass
  ``pareto=[]`` to restore "no frontier".
- ``plot_latency_vs_energy(results)`` — same, plus ``pareto``,
  ``frontier_ids``, ``context``, ``theme``, ``backend``, ``export_path``,
  ``include_frontier_line``, ``units`` as keyword-only enhancements.
- Return type unchanged: a Plotly or matplotlib figure, never ``None``.

Note on the duplicate ``plot_latency_vs_energy``
------------------------------------------------
This function also exists in :mod:`visualization.leaderboard_plots`
with a different title ("Latency vs Energy (Pure Efficiency)"). Both
delegate to the same pipeline; only the title and the default
``pareto`` differ. A future consolidation should pick one canonical
module and re-export; until then, this file keeps its original title
for backward compatibility.

Requires Python 3.8+.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import (
    Any, Iterable, List, Mapping, Optional, Sequence, Union,
)

from .contracts.chart_schema import ChartContext
from .contracts.theme import VisualTheme
from .renderers._wrappers import (
    finite_or_none,
    render_tradeoff_figure,
)
from .renderers.pareto_plots import ParetoExplorerError

logger = logging.getLogger(__name__)


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

    The carbon axis is drawn from ``carbon_operational_kg`` when that
    field is present and finite, otherwise from the legacy ``carbon``
    field. When a single chart ends up mixing the two sources, a
    ``WARNING`` is logged because the axis would otherwise be
    displaying two different metrics.

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
    context, theme, backend, export_path, include_frontier_line, units
        See the module docstring.

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
    return render_tradeoff_figure(
        results,
        pareto,
        frontier_ids=frontier_ids,
        x="carbon",
        y="accuracy",
        title="Accuracy vs Carbon",
        x_label=(units or {}).get("carbon", "Carbon (kg CO₂)"),
        y_label=(units or {}).get("accuracy", "Accuracy"),
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

    Original signature was ``plot_latency_vs_energy(results)``.
    Enhanced: adds ``pareto`` (auto-computed when omitted),
    ``frontier_ids``, and the provenance / export keyword arguments.
    """
    return render_tradeoff_figure(
        results,
        pareto,
        frontier_ids=frontier_ids,
        x="energy",
        y="latency",
        title="Latency vs Energy",
        x_label=(units or {}).get("energy", "Energy (Wh)"),
        y_label=(units or {}).get("latency", "Latency (s)"),
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
# Carbon-axis normalisation (unique to this file's X-axis carbon chart)
# --------------------------------------------------------------------------- #
def _carbon_axis_normalizer(
    results: Sequence[Mapping[str, Any]],
    _y_field: str,
) -> List[Mapping[str, Any]]:
    """
    Promote ``carbon_operational_kg`` → ``carbon`` for every point.

    Unlike the previous implementation, this works **regardless of
    which axis carbon appears on**. In :func:`plot_accuracy_vs_carbon`
    carbon is the *X* axis — the previous normaliser only fired when
    the Y axis was carbon, so it never fired here and any point that
    carried only ``carbon_operational_kg`` was silently dropped.

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

    # ---- 1. Carbon normaliser (bug fix on X axis) -------------- #
    print("carbon normaliser (X-axis fix):")
    # Point with only carbon_operational_kg must be plotted
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

    # No carbon fields → no crash, no addition
    no_carbon = [{"agent_id": "A", "accuracy": 0.9}]
    normalised = _carbon_axis_normalizer(no_carbon, "accuracy")
    _check(
        "no carbon fields → point passed through",
        "carbon" not in normalised[0],
    )

    # ---- 2. Public function integration (mocked) -------------- #
    print("plot_accuracy_vs_carbon (mocked):")
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
            {"agent_id": "A", "accuracy": 0.7,
             "carbon": 1.0},
            {"agent_id": "B", "accuracy": 0.8,
             "carbon": 2.0},
            {"agent_id": "C", "accuracy": 0.9,
             "carbon": 3.0},
        ],
    )
    _check("returns figure", fig is fake_fig)
    _check(
        "plot_tradeoff called once",
        explorer.plot_tradeoff.call_count == 1,
    )
    call_kwargs = explorer.plot_tradeoff.call_args.kwargs
    _check("x axis is carbon", call_kwargs["x"] == "carbon")
    _check("y axis is accuracy", call_kwargs["y"] == "accuracy")
    _check(
        "title correct",
        call_kwargs["title"] == "Accuracy vs Carbon",
    )
    _check(
        "no include_frontier_line kwarg",
        "include_frontier_line" not in call_kwargs,
    )
    _check(
        "frontier line added for ≥2 frontier points",
        fake_fig.add_trace.call_count == 1,
    )

    # Point that only has carbon_operational_kg is NOT dropped
    fig, explorer, fake_fig = _run(
        "plot_accuracy_vs_carbon",
        [
            {"agent_id": "A", "accuracy": 0.9,
             "carbon_operational_kg": 0.02},
            {"agent_id": "B", "accuracy": 0.8,
             "carbon_operational_kg": 0.03},
        ],
    )
    call_kwargs = explorer.plot_tradeoff.call_args.kwargs
    # Both points should now have carbon populated
    points_sent = call_kwargs["points"]
    _check(
        "both operational-only points have carbon",
        all("carbon" in p for p in points_sent),
    )
    _check(
        "carbon values from operational",
        points_sent[0]["carbon"] == 0.02
        and points_sent[1]["carbon"] == 0.03,
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

    # frontier_ids= overrides pareto
    fig, explorer, fake_fig = _run(
        "plot_accuracy_vs_carbon",
        [
            {"agent_id": "A", "accuracy": 0.7, "carbon": 1.0},
            {"agent_id": "B", "accuracy": 0.8, "carbon": 2.0},
        ],
        pareto=[],
        frontier_ids=["B"],
    )
    call_kwargs = explorer.plot_tradeoff.call_args.kwargs
    _check(
        "frontier_ids overrides empty pareto",
        call_kwargs["frontier_ids"] == ["B"],
    )

    # ---- 3. plot_latency_vs_energy (mocked) ------------------- #
    print("plot_latency_vs_energy (mocked):")
    fig, explorer, fake_fig = _run(
        "plot_latency_vs_energy",
        [
            {"agent_id": "A", "energy": 1.0, "latency": 0.2},
            {"agent_id": "B", "energy": 2.0, "latency": 0.1},
        ],
    )
    _check("returns figure", fig is fake_fig)
    call_kwargs = explorer.plot_tradeoff.call_args.kwargs
    _check("x axis is energy", call_kwargs["x"] == "energy")
    _check("y axis is latency", call_kwargs["y"] == "latency")
    _check(
        "title preserved",
        call_kwargs["title"] == "Latency vs Energy",
    )

    # ---- 4. Empty results path -------------------------------- #
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
            _check(
                "backend passed through",
                mock_empty.call_args.kwargs["backend"] == "plotly",
            )

    # ---- 5. Input validation ---------------------------------- #
    print("input validation:")
    for bad_kwargs in (
        dict(results="not-a-list"),
        dict(results=[{"a": 1}], include_frontier_line="yes"),
        dict(results=[{"a": 1}], units="kWh"),
        dict(results=[{"a": 1}], units={"carbon": 42}),
        dict(results=[{"a": 1}], theme="not-a-theme"),
        dict(results=[{"a": 1}], context="not-a-context"),
        dict(results=[{"a": 1}], backend="bokeh"),
        dict(results=[{"a": 1}], export_path=""),
    ):
        try:
            plot_accuracy_vs_carbon(**bad_kwargs)  # type: ignore[arg-type]
            _check(f"reject {list(bad_kwargs)}", False)
        except ParetoExplorerError:
            _check(f"reject {list(bad_kwargs)}", True)
        except Exception as exc:
            _check(
                f"reject {list(bad_kwargs)} (wrong type: "
                f"{type(exc).__name__})", False,
            )

    # ---- 6. Mixed-timezone via shared context inference ------- #
    print("mixed-timezone timestamps:")
    from .renderers._wrappers import infer_context

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

    # ---- 7. Chaos trust downgrade ----------------------------- #
    print("chaos trust downgrade:")
    ctx = infer_context([
        {"source": "measured", "chaos_injected": True},
    ], None)
    _check(
        "chaos → trust=medium",
        ctx.trust_level == "medium",
    )

    # ---- 8. __all__ / __dir__ --------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "ParetoExplorerError",
        "plot_accuracy_vs_carbon",
        "plot_latency_vs_energy",
    ):
        _check(f"{_name} exported", _name in __all__)

    # ---- 9. Error type ---------------------------------------- #
    print("error type:")
    _check(
        "ParetoExplorerError is ValueError",
        issubclass(ParetoExplorerError, ValueError),
    )

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
