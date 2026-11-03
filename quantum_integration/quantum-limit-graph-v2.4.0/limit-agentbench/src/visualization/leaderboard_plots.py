# src/visualization/leaderboard_plots.py

"""
Leaderboard visualization for green benchmarking.

Three leaderboard-style scatter plots. Backward-compatible entry points
that delegate to the consolidated :class:`renderers.pareto_plots.ParetoExplorer`
via the shared pipeline in :mod:`renderers._wrappers`.

Original API (preserved)::

    plot_accuracy_vs_energy(results, pareto)
    plot_latency_vs_energy(results)
    plot_carbon_vs_energy(results)

Enhanced API (all keyword-only, all optional)::

    <any function>(
        results,
        pareto=None,           # None → auto-compute; [] → no frontier
        *,
        frontier_ids=None,     # alternative to pareto; IDs only
        context=None,
        theme=None,
        backend="plotly",
        export_path=None,
        include_frontier_line=True,
        units=None,
    )

Frontier semantics
------------------
- ``frontier_ids`` supplied  → use those IDs (overrides the others).
- ``pareto is None`` (default) → **auto-compute** the frontier from
  ``results``. This is a behavior change from the previous version,
  which rendered every point as "dominated" when ``pareto`` was
  omitted. Pass ``pareto=[]`` to restore the old behavior.
- ``pareto == []``            → explicitly no frontier highlighted.
- ``pareto == [...]``         → use the supplied frontier entries.

Carbon-axis behavior
--------------------
:func:`plot_carbon_vs_energy` prefers ``carbon_operational_kg`` over the
legacy ``carbon`` field when the former is present and finite — this is
what "never conflates operational with contractual" means in practice.
When a single chart ends up plotting some points from the operational
field and others from the legacy field, a ``WARNING`` is logged and the
provenance note is extended so the reader sees the mix on the banner.

Behavior changes over the previous revision
-------------------------------------------
1. **Import paths fixed** — the previous revision imported
   ``contracts.visual_theme`` and ``tradeoffs.pareto``; neither module
   exists in the enhanced tree.

2. **``include_frontier_line`` is now honored.** The previous revision
   passed it as a keyword argument to ``ParetoExplorer.plot_tradeoff``,
   which does not accept it — every call raised ``TypeError``. The
   shared pipeline draws the line via post-processing.

3. **``int`` ``agent_id`` works.** ``_extract_frontier_ids`` in the
   previous revision only accepted ``str``; ``int`` IDs fell into a
   positional-ID fallback the explorer never matched against, silently
   classifying every point as "dominated". The shared helper coerces
   to ``str``; entries without an ID are dropped with a ``WARNING``.

4. **Mixed-timezone timestamps no longer crash the context inference.**
   Aware timestamps are preferred; naive ones are used only when no
   aware timestamps are present.

5. **``plot_carbon_vs_energy`` honours operational emissions.** The
   previous revision only used ``carbon_operational_kg`` when ``carbon``
   was absent — a no-op for datasets carrying both. The enhanced
   version prefers operational when present and finite, and warns when
   a single chart mixes the two sources.

6. **Chaos-injected runs report ``trust=MEDIUM``, not ``HIGH``.**
   Previously the subtitle could say "trust=high · chaos-injected", a
   self-contradiction.

7. **``pareto`` now defaults to ``None``** (auto-compute) for all three
   functions, matching :mod:`energy_pareto`'s enhanced semantics.
   Pass ``pareto=[]`` to restore the previous "no frontier" default.

8. **``frontier_ids`` is a real parameter** on all three functions.
   The previous docstring advertised it but the functions didn't
   accept it. Both ``pareto=`` (full entries) and ``frontier_ids=``
   (IDs only) are supported; ``frontier_ids`` takes precedence.

9. **Export delegates to** :func:`renderers.export.save_figure_detailed`.
   ``Path`` inputs, ``.hidden`` / ``out.`` extension edge cases, and
   Plotly image export without ``kaleido`` are all handled correctly.

10. **Empty-figure fallback uses the resolved backend**, not a
    duck-typed import. If neither backend is installed, the caller gets
    :class:`ParetoExplorerError` at construction time.

11. **All parameters validated.** ``results``, ``pareto``, ``units``,
    ``export_path``, ``include_frontier_line``, ``context``, ``theme``,
    ``backend``, ``id_key``. Bad input raises :class:`ParetoExplorerError`
    before any rendering work is done.

12. **Shared pipeline.** The five duplicated helpers from the previous
    revision (``_extract_frontier_ids``, ``_infer_context``,
    ``_empty_figure``, ``_save``, plus ``_normalise_points``) now live
    in :mod:`renderers._wrappers` and are shared with
    :mod:`energy_pareto`.

13. **Honest docstring.** The previous "10 enhancements" list claimed
    six features this file never implemented (``route``,
    ``deployment_id``, ``precision``, reviewer metadata, a separate
    carbon plot, adaptive precision). Those claims have been removed.
    What remains is what the module actually does.

Compatibility
-------------
- ``plot_accuracy_vs_energy(results, pareto)`` — works with the same
  positional arguments as before.
- ``plot_latency_vs_energy(results)`` /
  ``plot_carbon_vs_energy(results)`` — **frontier highlighting is now
  on by default**. The previous version showed no frontier at all.
  Pass ``pareto=[]`` to restore the previous behavior.
- All keyword-only parameters accepted with the same names and
  meanings as the previous revision, plus ``frontier_ids=``.
- Return type unchanged: a Plotly or matplotlib figure, never
  ``None``.

Requires Python 3.8+.
"""

from __future__ import annotations

import logging
import math
from typing import (
    Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union,
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
# Public entry points
# --------------------------------------------------------------------------- #
def plot_accuracy_vs_energy(
    results: Sequence[Mapping[str, Any]],
    pareto: Optional[Sequence[Mapping[str, Any]]] = None,
    *,
    frontier_ids: Optional[Iterable[Any]] = None,
    context: Optional[ChartContext] = None,
    theme: Optional[VisualTheme] = None,
    backend: str = "plotly",
    export_path: Optional[Union[str, "Any"]] = None,
    include_frontier_line: bool = True,
    units: Optional[Mapping[str, str]] = None,
) -> Any:
    """
    Accuracy vs energy leaderboard scatter.

    Backward-compatible: ``pareto`` remains positional. Enhanced: it
    defaults to ``None`` (auto-compute); pass ``[]`` for no frontier.
    """
    return render_tradeoff_figure(
        results,
        pareto,
        frontier_ids=frontier_ids,
        x="energy",
        y="accuracy",
        title="Green Leaderboard: Accuracy vs Energy",
        x_label=(units or {}).get("energy", "Energy (Wh)"),
        y_label=(units or {}).get("accuracy", "Accuracy"),
        context=context,
        theme=theme,
        backend=backend,
        export_path=export_path,
        include_frontier_line=include_frontier_line,
        units=units,
    )


def plot_latency_vs_energy(
    results: Sequence[Mapping[str, Any]],
    pareto: Optional[Sequence[Mapping[str, Any]]] = None,
    *,
    frontier_ids: Optional[Iterable[Any]] = None,
    context: Optional[ChartContext] = None,
    theme: Optional[VisualTheme] = None,
    backend: str = "plotly",
    export_path: Optional[Union[str, "Any"]] = None,
    include_frontier_line: bool = True,
    units: Optional[Mapping[str, str]] = None,
) -> Any:
    """
    Latency vs energy leaderboard scatter.

    The original signature was ``plot_latency_vs_energy(results)``.
    Enhanced: adds ``pareto`` (auto-computed when omitted),
    ``frontier_ids``, and the provenance / export keyword arguments.
    """
    return render_tradeoff_figure(
        results,
        pareto,
        frontier_ids=frontier_ids,
        x="energy",
        y="latency",
        title="Latency vs Energy (Pure Efficiency)",
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


def plot_carbon_vs_energy(
    results: Sequence[Mapping[str, Any]],
    pareto: Optional[Sequence[Mapping[str, Any]]] = None,
    *,
    frontier_ids: Optional[Iterable[Any]] = None,
    context: Optional[ChartContext] = None,
    theme: Optional[VisualTheme] = None,
    backend: str = "plotly",
    export_path: Optional[Union[str, "Any"]] = None,
    include_frontier_line: bool = True,
    units: Optional[Mapping[str, str]] = None,
) -> Any:
    """
    Carbon vs energy leaderboard scatter.

    The Y axis is drawn from each point's ``carbon_operational_kg`` field
    when it is present and finite, otherwise from the legacy ``carbon``
    field. This preserves the "operational emissions are never
    conflated with contractual offsets" contract. When a chart mixes the
    two sources, a ``WARNING`` is logged.
    """
    return render_tradeoff_figure(
        results,
        pareto,
        frontier_ids=frontier_ids,
        x="energy",
        y="carbon",
        title="Carbon vs Energy (Pure Green Plot)",
        x_label=(units or {}).get("energy", "Energy (Wh)"),
        y_label=(units or {}).get("carbon", "Carbon (kg CO₂)"),
        context=context,
        theme=theme,
        backend=backend,
        export_path=export_path,
        include_frontier_line=include_frontier_line,
        units=units,
        point_normalizer=_normalise_carbon_points,
        x_direction="min",
        y_direction="min",
    )


# --------------------------------------------------------------------------- #
# Carbon-axis normalisation
# --------------------------------------------------------------------------- #
def _normalise_carbon_points(
    results: Sequence[Mapping[str, Any]],
    y_field: str,
) -> List[Mapping[str, Any]]:
    """
    Populate ``carbon`` from ``carbon_operational_kg`` when the latter is
    present and finite.

    Points with only the legacy ``carbon`` field are kept as-is. A
    ``WARNING`` is logged when a single chart ends up mixing the two
    sources, because the axis then displays two different metrics.
    """
    if y_field != "carbon":
        return [dict(r) for r in results]

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
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "ParetoExplorerError",
    "plot_accuracy_vs_energy",
    "plot_carbon_vs_energy",
    "plot_latency_vs_energy",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m visualization.leaderboard_plots
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

    # ---- Import surface --------------------------------------- #
    print("imports:")
    for _name in (
        "ParetoExplorerError", "plot_accuracy_vs_energy",
        "plot_carbon_vs_energy", "plot_latency_vs_energy",
    ):
        _check(f"{_name} exported", _name in __all__)

    # ---- Carbon normalisation (bug fix) ----------------------- #
    print("carbon normalisation:")
    mixed = [
        {"agent_id": "A", "energy": 1.0,
         "carbon": 0.05, "carbon_operational_kg": 0.02},
        {"agent_id": "B", "energy": 2.0, "carbon": 0.03},
        {"agent_id": "C", "energy": 3.0,
         "carbon_operational_kg": 0.01},
    ]
    normalised = _normalise_carbon_points(mixed, "carbon")
    _check(
        "operational preferred when present",
        normalised[0]["carbon"] == 0.02,
    )
    _check(
        "legacy used when operational absent",
        normalised[1]["carbon"] == 0.03,
    )
    _check(
        "operational used when carbon absent",
        normalised[2]["carbon"] == 0.01,
    )

    # Original dicts unchanged
    _check(
        "input dict not mutated",
        mixed[0]["carbon"] == 0.05,
    )

    # NaN operational → falls back to legacy
    nan_case = [
        {"agent_id": "A", "energy": 1.0,
         "carbon": 0.05, "carbon_operational_kg": float("nan")},
    ]
    normalised = _normalise_carbon_points(nan_case, "carbon")
    _check(
        "NaN operational falls back to legacy",
        normalised[0]["carbon"] == 0.05,
    )

    # Non-carbon axis untouched
    normalised = _normalise_carbon_points(
        [{"a": 1}], "accuracy",
    )
    _check(
        "non-carbon axis returns copies",
        normalised == [{"a": 1}],
    )

    # ---- Public function integration (mocked) ------------------ #
    print("integration (mocked):")

    import sys as _sys
    this_module = _sys.modules[__name__]

    def _run(fn_name, *args, **kwargs):
        """Run a wrapper against a mocked ParetoExplorer."""
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

    # Basic call, auto-compute frontier
    fig, explorer, fake_fig = _run(
        "plot_accuracy_vs_energy",
        [
            {"agent_id": "A", "energy": 1.0, "accuracy": 0.7},
            {"agent_id": "B", "energy": 2.0, "accuracy": 0.8},
            {"agent_id": "C", "energy": 3.0, "accuracy": 0.9},
        ],
    )
    _check("returns figure", fig is fake_fig)
    _check(
        "plot_tradeoff called once",
        explorer.plot_tradeoff.call_count == 1,
    )
    call_kwargs = explorer.plot_tradeoff.call_args.kwargs
    _check(
        "no include_frontier_line kwarg passed",
        "include_frontier_line" not in call_kwargs,
    )
    _check(
        "frontier line added for >=2 frontier points",
        fake_fig.add_trace.call_count == 1,
    )

    # Explicit empty pareto → no frontier line
    fig, explorer, fake_fig = _run(
        "plot_latency_vs_energy",
        [{"agent_id": "A", "energy": 1.0, "latency": 0.2}],
        [],
    )
    _check(
        "empty pareto → no frontier line",
        fake_fig.add_trace.call_count == 0,
    )
    call_kwargs = explorer.plot_tradeoff.call_args.kwargs
    _check(
        "empty pareto → frontier_ids=[]",
        call_kwargs["frontier_ids"] == [],
    )

    # int agent_id in pareto
    fig, explorer, fake_fig = _run(
        "plot_accuracy_vs_energy",
        [
            {"agent_id": 1, "energy": 1.0, "accuracy": 0.7},
            {"agent_id": 2, "energy": 2.0, "accuracy": 0.8},
        ],
        [
            {"agent_id": 1, "energy": 1.0, "accuracy": 0.7},
            {"agent_id": 2, "energy": 2.0, "accuracy": 0.8},
        ],
    )
    call_kwargs = explorer.plot_tradeoff.call_args.kwargs
    _check(
        "int pareto IDs coerced to str",
        sorted(call_kwargs["frontier_ids"]) == ["1", "2"],
    )

    # frontier_ids= parameter
    fig, explorer, fake_fig = _run(
        "plot_accuracy_vs_energy",
        [
            {"agent_id": "A", "energy": 1.0, "accuracy": 0.7},
            {"agent_id": "B", "energy": 2.0, "accuracy": 0.8},
        ],
        frontier_ids=["A"],
    )
    call_kwargs = explorer.plot_tradeoff.call_args.kwargs
    _check(
        "frontier_ids overrides pareto",
        call_kwargs["frontier_ids"] == ["A"],
    )

    # Empty results → empty figure path
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
            from .renderers._wrappers import render_tradeoff_figure
            fig = render_tradeoff_figure(
                [],
                x="energy", y="accuracy",
                title="Empty Test",
                x_label="X", y_label="Y",
            )
            _check(
                "empty results returns annotated figure",
                fig == "empty_fig",
            )

    # ---- Input validation ------------------------------------ #
    print("input validation:")
    for bad_kwargs in (
        dict(results="not-a-list"),
        dict(results=[{"e": 1, "a": 0.5}],
             include_frontier_line="yes"),
        dict(results=[{"e": 1, "a": 0.5}], units="kWh"),
        dict(results=[{"e": 1, "a": 0.5}],
             units={"energy": 42}),
        dict(results=[{"e": 1, "a": 0.5}], theme="not-a-theme"),
        dict(results=[{"e": 1, "a": 0.5}], context="not-a-context"),
        dict(results=[{"e": 1, "a": 0.5}], backend="bokeh"),
        dict(results=[{"e": 1, "a": 0.5}], export_path=""),
    ):
        try:
            plot_accuracy_vs_energy(**bad_kwargs)  # type: ignore[arg-type]
            _check(f"reject {list(bad_kwargs)}", False)
        except ParetoExplorerError:
            _check(f"reject {list(bad_kwargs)}", True)
        except Exception as exc:
            _check(
                f"reject {list(bad_kwargs)} (wrong type: "
                f"{type(exc).__name__})", False,
            )

    # ---- Mixed-timezone timestamps --------------------------- #
    print("mixed-timezone timestamps:")
    from .renderers._wrappers import infer_context

    naive = datetime(2026, 1, 1, 12, 0, 0)
    aware = datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
    try:
        ctx = infer_context([
            {"energy": 1.0, "accuracy": 0.9, "timestamp": naive},
            {"energy": 2.0, "accuracy": 0.8, "timestamp": aware},
        ], None)
        _check("mixed timestamps do not raise", True)
        _check(
            "aware timestamp wins",
            ctx.time_range_start == aware,
        )
    except TypeError:
        _check("mixed timestamps do not raise", False)

    # ---- Chaos trust downgrade ------------------------------- #
    print("chaos trust downgrade:")
    ctx = infer_context([
        {"source": "measured", "chaos_injected": True},
    ], None)
    _check(
        "chaos → trust=medium",
        ctx.trust_level == "medium",
    )

    # ---- Empty-figure backend awareness ---------------------- #
    print("empty_figure backend awareness:")
    from .renderers._wrappers import empty_figure
    try:
        empty_figure(
            title="test", context=None,
            theme=__import__(
                "visualization.contracts.theme",
                fromlist=["DEFAULT_THEME"],
            ).DEFAULT_THEME,
            backend="plotly",
        )
        _check("empty_figure plotly path runs", True)
    except Exception:
        _check("empty_figure plotly path runs", False)

    # ---- __dir__ --------------------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
