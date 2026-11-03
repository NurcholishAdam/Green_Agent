# src/visualization/pareto_plotter.py

"""
Specialized 2D Pareto plots for Green_Agent.

Three policy-oriented visualizations, each answering a different
deployment question:

1. **Accuracy vs Carbon** — sustainability reviewers.
   "What performance am I paying per unit environmental cost?"
2. **Latency vs Energy** — systems engineers.
   "Are fast agents inherently wasteful?"
3. **Carbon vs Energy (Pure Green)** — green-AI researchers.
   "Which agents are environmentally efficient independent of
   performance?"

Rendering delegates to :class:`renderers.pareto_plots.ParetoExplorer`
via the shared pipeline in :mod:`renderers._wrappers`. The class adds
only the plot-specific decorations that the general pipeline does not
know about: accuracy-per-carbon reference diagonals, SLA verticals,
grid-intensity diagonals, simulated-marker restyling, and a corner
annotation on the pure-green chart.

Frontier semantics
------------------
Each ``plot_*`` method accepts ``frontier`` as an optional positional
argument, following the same convention as the function-level wrappers
in :mod:`energy_pareto`, :mod:`leaderboard_plots`, and
:mod:`pareto_plots`:

- ``frontier=None`` (default) → **auto-compute** from ``agents``.
- ``frontier == []`` → explicitly no frontier highlighted.
- ``frontier == [...]`` → use the supplied entries' ``agent_id``.

``agents`` accepts either dicts or objects with matching attributes
(``ExtendedParetoPoint``, dataclasses, duck-typed records). Attribute-
based objects have their fields extracted into the dict shape
:class:`ParetoExplorer` expects.

Class-specific parameters
-------------------------
- ``reference_ratios`` — ``(50, 100, 200)`` by default. Draws
  ``accuracy_% = ratio × carbon_g / 1000`` diagonals on the
  accuracy-vs-carbon chart, labelled ``"ratio%/kg"``. The ratio means
  "accuracy-% per kilogram of carbon".
- ``sla_lines`` — ``(100, 500, 1000)`` ms by default. Vertical
  reference lines on the latency-vs-energy chart.
- ``grid_intensities`` — ``(("US-CA Grid", 0.2), ...)`` by default.
  Diagonal reference lines on the carbon-vs-energy chart, one per
  named grid intensity (kg CO₂/kWh).
- ``color_dominated_by`` — draw the dominated trace on the carbon-vs-
  energy chart with a continuous Viridis scale keyed to the named
  field (accuracy by default). Simulated points retain their distinct
  color/marker via a symbol override.
- ``annotation_text`` — a corner annotation on the pure-green chart.

Behavior changes over the previous revision
-------------------------------------------
1. Import paths fixed (``contracts.theme``, ``renderers.pareto_plots``,
   ``renderers._wrappers``, ``renderers.export``, ``renderers.base``).
2. ``plot_tradeoff`` is no longer called with unsupported kwargs
   (``include_frontier_line``, ``x_log_scale``, ``y_log_scale``,
   ``color_by``). The previous code raised ``TypeError`` on the first
   call to every ``plot_*`` method.
3. ``self._explorer`` dead state removed.
4. Matplotlib titles no longer render literal ``<br><sub>`` tags.
5. ``_restyle_simulated_markers`` uses ``update_traces`` and applies
   to both traces; previously it silently mutated in-memory trace
   objects that Plotly ignored, and missed simulated frontier points.
6. Reference-line labels are correct (``%/kg``, not ``%/g``) and
   visible (legend shown, ``hoverinfo`` no longer skipped).
7. ``accuracy_pct`` heuristic warns in the ambiguous ``[0.01, 1.0]``
   range.
8. ``save_figure_detailed`` return value is checked and logged.
9. Backend availability uses ``renderers/base.is_available`` — no
   module-level ``import plotly`` / ``import matplotlib``, no import-
   time warnings.
10. ``__init__`` validates ``theme``, ``context``, ``backend``, and
    the ``show_*`` flags. Failure raises
    ``ParetoPlotterError``/``ParetoExplorerError``, not
    ``ImportError``.
11. ``attach_context`` validates its argument.
12. ``_infer_context`` (fourth copy in the tree) removed — uses
    ``renderers._wrappers.infer_context``.
13. ``_agent_to_dict`` handles ``id=0`` correctly and rejects non-
    agent inputs.
14. ``_add_sla_lines`` / ``_add_corner_annotation`` failures are
    logged, not swallowed.
15. ``plot_all_projections`` creates ``save_dir`` when needed.
16. ``__repr__`` added.
17. ``_check()``-based smoke test — survives ``python -O``.

Requires Python 3.8+; ``renderers._wrappers``, ``renderers.pareto_plots``,
``renderers.export``, ``renderers.base``, ``contracts.theme``, and
``contracts.chart_schema`` must all be importable.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from pathlib import Path
from typing import (
    Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union,
)

from .contracts.chart_schema import ChartContext
from .contracts.theme import VisualTheme
from .renderers._wrappers import (
    finite_or_none,
    infer_context,
    render_tradeoff_figure,
    try_export,
)
from .renderers.pareto_plots import ParetoExplorerError

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ParetoPlotterError(ValueError):
    """Raised for invalid ParetoPlotter configuration or inputs."""


# --------------------------------------------------------------------------- #
# Small validators
# --------------------------------------------------------------------------- #
def _validate_backend(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ParetoPlotterError(
            f"backend must be a non-empty string, got "
            f"{type(value).__name__}."
        )
    name = value.lower()
    if name not in ("plotly", "matplotlib"):
        raise ParetoPlotterError(
            f"unknown backend {value!r}; expected 'plotly' or "
            f"'matplotlib'."
        )
    return name


def _validate_theme(value: Any) -> Optional[VisualTheme]:
    if value is None:
        return None
    if not isinstance(value, VisualTheme):
        raise ParetoPlotterError(
            f"theme must be a VisualTheme or None, got "
            f"{type(value).__name__}."
        )
    return value


def _validate_context(value: Any) -> Optional[ChartContext]:
    if value is None:
        return None
    if not isinstance(value, ChartContext):
        raise ParetoPlotterError(
            f"context must be a ChartContext or None, got "
            f"{type(value).__name__}."
        )
    return value


def _validate_bool(name: str, value: Any) -> bool:
    if not isinstance(value, bool):
        raise ParetoPlotterError(
            f"{name} must be a bool, got {type(value).__name__}."
        )
    return value


# --------------------------------------------------------------------------- #
# Agent conversion
# --------------------------------------------------------------------------- #
_NUMERIC_PROBE_FIELDS = (
    "accuracy", "accuracy_pct",
    "energy_kwh", "energy_wh",
    "carbon_co2e_kg", "carbon_operational_kg",
    "carbon_contractual_kg", "carbon_g",
    "latency_ms",
)

_ATTRIBUTE_FIELDS = (
    "agent_id", "id",
    "accuracy",
    "energy_kwh", "energy_wh",
    "carbon_co2e_kg", "carbon_operational_kg",
    "carbon_contractual_kg", "carbon_g",
    "latency_ms",
    "memory_mb", "circuit_depth", "variance_score",
    "precision", "region", "hardware_profile",
    "simulated", "source", "chaos_injected",
    "policy_version", "system_version",
    "deployment_id", "run_id", "timestamp",
    "metadata",
)


def _agent_to_dict(agent: Any) -> Dict[str, Any]:
    """
    Convert an ``ExtendedParetoPoint`` (attribute-based) or dict into
    the canonical dict shape used by the shared pipeline.

    Raises :class:`ParetoPlotterError` when neither dict nor attribute
    access yields at least one numeric metric — that is not an agent,
    and silently plotting it at ``(0, 0)`` misrepresents the data.
    """
    if isinstance(agent, Mapping):
        d: Dict[str, Any] = dict(agent)
    else:
        d = {}
        for attr in _ATTRIBUTE_FIELDS:
            if hasattr(agent, attr):
                d[attr] = getattr(agent, attr)

        # Nested metadata often carries the audit fields.
        meta = d.get("metadata") or {}
        if isinstance(meta, Mapping):
            for k in (
                "source", "simulated", "policy_version",
                "system_version", "deployment_id", "run_id",
            ):
                if k not in d and k in meta:
                    d[k] = meta[k]

    # ---- agent_id canonicalisation ------------------------------ #
    # `id=0` is valid; the pre-enhancement `d.get("id") or "unknown"`
    # silently converted it to "unknown".
    if d.get("agent_id") is None:
        raw_id = d.get("id")
        d["agent_id"] = str(raw_id) if raw_id is not None else "unknown"
    else:
        d["agent_id"] = str(d["agent_id"])

    # ---- reject non-agents --------------------------------------- #
    if not any(
        isinstance(d.get(f), (int, float)) and not isinstance(d.get(f), bool)
        for f in _NUMERIC_PROBE_FIELDS
    ):
        raise ParetoPlotterError(
            f"cannot extract any numeric metric from {type(agent).__name__}; "
            f"expected one of {_NUMERIC_PROBE_FIELDS}."
        )

    # ---- energy → Wh --------------------------------------------- #
    if "energy_wh" not in d:
        kwh = d.get("energy_kwh")
        if isinstance(kwh, (int, float)) and not isinstance(kwh, bool):
            d["energy_wh"] = float(kwh) * 1000.0
        else:
            d["energy_wh"] = 0.0

    # ---- carbon → grams ------------------------------------------ #
    if "carbon_g" not in d:
        co2e_kg = d.get("carbon_co2e_kg")
        op_kg = d.get("carbon_operational_kg")
        if isinstance(co2e_kg, (int, float)) and not isinstance(co2e_kg, bool):
            d["carbon_g"] = float(co2e_kg) * 1000.0
        elif isinstance(op_kg, (int, float)) and not isinstance(op_kg, bool):
            d["carbon_g"] = float(op_kg) * 1000.0
        else:
            d["carbon_g"] = 0.0

    # ---- accuracy → percent -------------------------------------- #
    if "accuracy_pct" not in d:
        acc = d.get("accuracy")
        if isinstance(acc, (int, float)) and not isinstance(acc, bool):
            acc = float(acc)
            if 0.01 <= acc <= 1.0:
                logger.warning(
                    "accuracy=%r is in the ambiguous range [0.01, 1.0]; "
                    "treating as a ratio (× 100 → %%). If your field is "
                    "already a percentage, pre-scale it or pass "
                    "'accuracy_pct' directly.", acc,
                )
                d["accuracy_pct"] = acc * 100.0
            elif acc <= 1.0:
                # Very small value; treat as ratio (unambiguous).
                d["accuracy_pct"] = acc * 100.0
            else:
                d["accuracy_pct"] = acc
        else:
            d["accuracy_pct"] = 0.0

    # ---- defaults ------------------------------------------------ #
    d.setdefault("latency_ms", 0.0)
    d.setdefault("accuracy", float(d["accuracy_pct"]) / 100.0)
    return d


def _normalize_carbon(results: Sequence[Mapping[str, Any]], _y: str):
    """
    Prefer ``carbon_operational_kg`` over ``carbon_g`` when present.

    The physical-emissions contract is explicit: contractual offsets
    are never plotted as if they were measured emissions. When a chart
    mixes operational and legacy sources a ``WARNING`` is logged.
    """
    out: List[Mapping[str, Any]] = []
    from_op = 0
    from_legacy = 0
    for r in results:
        p = dict(r)
        op = finite_or_none(p.get("carbon_operational_kg"))
        if op is not None:
            p["carbon_g"] = op * 1000.0
            from_op += 1
        elif finite_or_none(p.get("carbon_g")) is not None:
            from_legacy += 1
        out.append(p)
    if from_op and from_legacy:
        logger.warning(
            "Carbon axis mixes %d point(s) from 'carbon_operational_kg' "
            "and %d point(s) from 'carbon_g'; the two are not directly "
            "comparable.",
            from_op, from_legacy,
        )
    return out


# --------------------------------------------------------------------------- #
# Title banner
# --------------------------------------------------------------------------- #
def _title_with_banner(
    title: str,
    context: Optional[ChartContext],
    enabled: bool,
    backend: str,
) -> str:
    """
    Append the context subtitle to ``title`` in the backend's native
    markup.

    The pre-enhancement implementation always used ``<br><sub>``, which
    matplotlib renders as literal text on every chart.
    """
    if not enabled or context is None:
        return title
    subtitle = context.to_subtitle()
    if not subtitle:
        return title
    if backend == "plotly":
        return f"{title}<br><sub>{subtitle}</sub>"
    return f"{title}\n{subtitle}"


# --------------------------------------------------------------------------- #
# Reference-line helpers
# --------------------------------------------------------------------------- #
def _add_accuracy_carbon_references(
    fig: Any,
    agent_dicts: Sequence[Mapping[str, Any]],
    ratios: Sequence[float],
    theme: VisualTheme,
) -> None:
    """
    Add diagonal "accuracy % per kg carbon" reference lines.

    The ratio ``r`` defines the line ``accuracy_pct = r * carbon_g / 1000``
    — i.e. ``r`` is accuracy-% per kilogram of carbon. The label is
    ``"r%/kg"``; the pre-enhancement label ``"r%/g"`` was off by 1000×.
    Lines are visible in the legend (``showlegend=True``); the previous
    ``showlegend=False`` + ``hoverinfo="skip"`` made them invisible.
    """
    try:
        import plotly.graph_objects as go  # type: ignore
    except ImportError:  # pragma: no cover — defensive
        return
    if not agent_dicts:
        return
    max_carbon = max(
        (finite_or_none(d.get("carbon_g")) or 0.0) for d in agent_dicts
    ) or 1.0
    for ratio in ratios:
        fig.add_trace(go.Scatter(
            x=[0, max_carbon],
            y=[0, max_carbon * float(ratio) / 1000.0],
            mode="lines",
            line=dict(color=theme.palette.grid, width=1, dash="dash"),
            showlegend=True,
            name=f"{ratio}%/kg",
        ))


def _add_sla_lines(
    fig: Any,
    sla_lines: Sequence[float],
    theme: VisualTheme,
) -> None:
    """Add vertical SLA reference lines to a Plotly figure."""
    for sla_ms in sla_lines:
        try:
            fig.add_vline(
                x=sla_ms,
                line_dash="dash",
                line_color=theme.palette.warning,
                opacity=0.3,
                annotation_text=f"{sla_ms}ms SLA",
            )
        except Exception as exc:
            logger.warning(
                "SLA line for %sms not drawn: %s", sla_ms, exc,
            )


def _add_grid_intensity_references(
    fig: Any,
    agent_dicts: Sequence[Mapping[str, Any]],
    intensities: Sequence[Tuple[str, float]],
    theme: VisualTheme,
) -> None:
    """
    Add diagonal grid-intensity reference lines.

    Each ``(label, intensity_kg_per_kwh)`` entry yields
    ``carbon_g = energy_wh * intensity_kg_per_kwh`` — the exact
    physical relationship for the given grid intensity.
    """
    try:
        import plotly.graph_objects as go  # type: ignore
    except ImportError:  # pragma: no cover — defensive
        return
    if not agent_dicts:
        return
    max_energy = max(
        (finite_or_none(d.get("energy_wh")) or 0.0) for d in agent_dicts
    ) or 1.0
    for label, intensity in intensities:
        try:
            intensity_f = float(intensity)
        except (TypeError, ValueError):
            logger.warning(
                "Grid intensity %r for %r is not numeric; skipping.",
                intensity, label,
            )
            continue
        fig.add_trace(go.Scatter(
            x=[0, max_energy],
            y=[0, max_energy * intensity_f],
            mode="lines",
            line=dict(color=theme.palette.grid, width=1, dash="dash"),
            name=label,
            showlegend=True,
        ))


# --------------------------------------------------------------------------- #
# Marker restyling
# --------------------------------------------------------------------------- #
def _restyle_markers(
    fig: Any,
    agent_dicts: Sequence[Mapping[str, Any]],
    theme: VisualTheme,
    *,
    color_dominated_by: Optional[str],
    show_simulated: bool,
) -> None:
    """
    Restyle the dominated and frontier traces of a Plotly figure.

    When ``color_dominated_by`` is set, the dominated trace is coloured
    with a Viridis scale keyed to that field. Simulated points are then
    distinguished by **symbol** (``"x"``), because a per-point colour
    array would collide with the continuous scale.

    When ``color_dominated_by`` is not set and ``show_simulated`` is
    True, simulated points are coloured with the theme's simulated
    palette entry.

    The pre-enhancement implementation mutated ``trace.marker.color``
    in-place, which Plotly 5+ does not reliably propagate to the
    serialized figure, and only touched the dominated trace.
    """
    if not agent_dicts:
        return
    by_id = {
        str(d["agent_id"]): d for d in agent_dicts
        if d.get("agent_id") is not None
    }
    for trace in fig.data:
        name = getattr(trace, "name", "")
        if name not in ("dominated", "frontier"):
            continue
        texts = list(getattr(trace, "text", []) or [])
        if not texts:
            continue

        base_color = (
            theme.palette.dominated if name == "dominated"
            else theme.palette.frontier
        )

        # ---- color_dominated_by: continuous scale ----------------- #
        if name == "dominated" and color_dominated_by is not None:
            values = []
            for t in texts:
                agent = by_id.get(str(t))
                v = agent.get(color_dominated_by) if agent else None
                values.append(
                    float(v)
                    if isinstance(v, (int, float))
                    and not isinstance(v, bool) else None
                )
            if any(v is not None for v in values):
                fig.update_traces(
                    marker=dict(
                        color=[
                            v if v is not None else 0.0
                            for v in values
                        ],
                        colorscale="Viridis",
                        showscale=True,
                        colorbar=dict(title=color_dominated_by),
                    ),
                    selector=dict(name="dominated"),
                )
                if show_simulated:
                    symbols = []
                    for t in texts:
                        agent = by_id.get(str(t))
                        if agent and bool(agent.get("simulated", False)):
                            symbols.append("x")
                        else:
                            symbols.append("circle")
                    fig.update_traces(
                        marker=dict(symbol=symbols),
                        selector=dict(name="dominated"),
                    )
                continue

        # ---- simulated marker override -------------------------- #
        if show_simulated:
            colors = []
            has_simulated = False
            for t in texts:
                agent = by_id.get(str(t))
                if agent and bool(agent.get("simulated", False)):
                    colors.append(theme.palette.simulated)
                    has_simulated = True
                else:
                    colors.append(base_color)
            if has_simulated:
                fig.update_traces(
                    marker=dict(color=colors),
                    selector=dict(name=name),
                )


def _add_corner_annotation(
    fig: Any,
    text: str,
    theme: VisualTheme,
) -> None:
    """Add a corner annotation to a Plotly figure."""
    try:
        fig.add_annotation(
            text=text,
            xref="paper", yref="paper",
            x=0.02, y=0.98,
            showarrow=False,
            bgcolor=theme.palette.frontier,
            opacity=0.15,
        )
    except Exception as exc:
        logger.warning("Corner annotation not drawn: %s", exc)


# --------------------------------------------------------------------------- #
# ParetoPlotter
# --------------------------------------------------------------------------- #
class ParetoPlotter:
    """
    Specialized 2D Pareto visualizations (see module docstring).

    Delegates rendering to :class:`ParetoExplorer` via
    :func:`renderers._wrappers.render_tradeoff_figure`. Adds per-chart
    decorations (reference diagonals, SLA verticals, grid-intensity
    diagonals, corner annotation) as post-processing.
    """

    DEFAULT_ACCURACY_CARBON_RATIOS: Tuple[float, ...] = (50, 100, 200)
    DEFAULT_SLA_LINES_MS: Tuple[float, ...] = (100, 500, 1000)
    DEFAULT_GRID_INTENSITIES: Tuple[Tuple[str, float], ...] = (
        ("US-CA Grid", 0.2),
        ("CN Grid", 0.6),
        ("FR Grid", 0.05),
    )

    def __init__(
        self,
        backend: str = "plotly",
        *,
        theme: Optional[VisualTheme] = None,
        context: Optional[ChartContext] = None,
        show_provenance_banner: bool = True,
        show_simulated_marker: bool = True,
    ) -> None:
        self.backend: str = _validate_backend(backend)
        self.theme: VisualTheme = _validate_theme(theme) or _default_theme()
        self.context: Optional[ChartContext] = _validate_context(context)
        self.show_provenance_banner: bool = _validate_bool(
            "show_provenance_banner", show_provenance_banner,
        )
        self.show_simulated_marker: bool = _validate_bool(
            "show_simulated_marker", show_simulated_marker,
        )

        # Backend availability — raised here, not at render time.
        from .renderers.base import is_available, select_backend, RendererError
        if not is_available(self.backend):
            raise ParetoPlotterError(
                f"backend {self.backend!r} is not installed; install "
                f"it or choose the other backend."
            )
        try:
            select_backend(self.backend, strict=True)
        except RendererError as exc:
            raise ParetoPlotterError(str(exc)) from exc

        logger.debug(
            "ParetoPlotter initialised (backend=%s, theme=%s, "
            "context=%s, banners=%s, simulated=%s)",
            self.backend, self.theme.palette.name,
            "attached" if self.context else "inferred",
            self.show_provenance_banner, self.show_simulated_marker,
        )

    # ---------------------------------------------------------- introspection
    @classmethod
    def available_backends(cls) -> Dict[str, bool]:
        """Return which renderer backends are importable in this env."""
        from .renderers.base import is_available
        return {
            "plotly": is_available("plotly"),
            "matplotlib": is_available("matplotlib"),
        }

    def attach_context(self, context: ChartContext) -> None:
        """Attach a default :class:`ChartContext` for future plots."""
        self.context = _validate_context(context)
        logger.debug("ChartContext attached to ParetoPlotter")

    # ---------------------------------------------------------- plot 1
    def plot_accuracy_vs_carbon(
        self,
        agents: Optional[Sequence[Any]],
        frontier: Optional[Sequence[Any]] = None,
        title: str = "Accuracy vs Carbon Footprint",
        save_path: Optional[Union[str, Path]] = None,
        *,
        context: Optional[ChartContext] = None,
        theme: Optional[VisualTheme] = None,
        reference_ratios: Optional[Sequence[float]] = None,
        show_references: bool = True,
        include_frontier_line: bool = True,
        units: Optional[Mapping[str, str]] = None,
    ) -> Any:
        """
        Accuracy vs carbon footprint.

        Axes: **carbon (g CO₂e)** on X, **accuracy (%)** on Y.

        Carbon is drawn from ``carbon_operational_kg`` when present;
        contractual offsets are never plotted as physical emissions.
        """
        return self._render(
            agents,
            frontier,
            x="carbon_g",
            y="accuracy_pct",
            title=title,
            save_path=save_path,
            context=context,
            theme=theme,
            include_frontier_line=include_frontier_line,
            units=units,
            point_normalizer=_normalize_carbon,
            x_direction="min",
            y_direction="max",
            post_render=self._decorate_accuracy_carbon(
                reference_ratios, show_references,
            ),
        )

    def _decorate_accuracy_carbon(
        self,
        reference_ratios: Optional[Sequence[float]],
        show_references: bool,
    ):
        def decorate(fig, agent_dicts, theme):
            if show_references and self.backend == "plotly":
                ratios = (
                    reference_ratios
                    if reference_ratios is not None
                    else self.DEFAULT_ACCURACY_CARBON_RATIOS
                )
                _add_accuracy_carbon_references(
                    fig, agent_dicts, ratios, theme,
                )
            if self.show_simulated_marker:
                _restyle_markers(
                    fig, agent_dicts, theme,
                    color_dominated_by=None,
                    show_simulated=True,
                )
        return decorate

    # ---------------------------------------------------------- plot 2
    def plot_latency_vs_energy(
        self,
        agents: Optional[Sequence[Any]],
        frontier: Optional[Sequence[Any]] = None,
        title: str = "Latency vs Energy Consumption",
        save_path: Optional[Union[str, Path]] = None,
        *,
        context: Optional[ChartContext] = None,
        theme: Optional[VisualTheme] = None,
        sla_lines: Optional[Sequence[float]] = None,
        show_sla_lines: bool = True,
        include_frontier_line: bool = True,
        units: Optional[Mapping[str, str]] = None,
    ) -> Any:
        """
        Latency vs energy consumption.

        Axes: **energy (Wh)** on X, **latency (ms)** on Y.
        """
        return self._render(
            agents,
            frontier,
            x="energy_wh",
            y="latency_ms",
            title=title,
            save_path=save_path,
            context=context,
            theme=theme,
            include_frontier_line=include_frontier_line,
            units=units,
            x_direction="min",
            y_direction="min",
            post_render=self._decorate_latency_energy(
                sla_lines, show_sla_lines,
            ),
        )

    def _decorate_latency_energy(
        self,
        sla_lines: Optional[Sequence[float]],
        show_sla_lines: bool,
    ):
        def decorate(fig, agent_dicts, theme):
            if show_sla_lines and self.backend == "plotly":
                lines = (
                    sla_lines if sla_lines is not None
                    else self.DEFAULT_SLA_LINES_MS
                )
                _add_sla_lines(fig, lines, theme)
            if self.show_simulated_marker:
                _restyle_markers(
                    fig, agent_dicts, theme,
                    color_dominated_by=None,
                    show_simulated=True,
                )
        return decorate

    # ---------------------------------------------------------- plot 3
    def plot_carbon_vs_energy(
        self,
        agents: Optional[Sequence[Any]],
        frontier: Optional[Sequence[Any]] = None,
        title: str = "Pure Green: Carbon vs Energy",
        save_path: Optional[Union[str, Path]] = None,
        *,
        context: Optional[ChartContext] = None,
        theme: Optional[VisualTheme] = None,
        grid_intensities: Optional[Sequence[Tuple[str, float]]] = None,
        show_grid_references: bool = True,
        color_dominated_by: Optional[str] = "accuracy",
        include_frontier_line: bool = True,
        annotation_text: Optional[str] = None,
        units: Optional[Mapping[str, str]] = None,
    ) -> Any:
        """
        Pure green: carbon footprint vs energy consumption.

        Axes: **energy (Wh)** on X, **carbon (g CO₂e)** on Y.
        """
        return self._render(
            agents,
            frontier,
            x="energy_wh",
            y="carbon_g",
            title=title,
            save_path=save_path,
            context=context,
            theme=theme,
            include_frontier_line=include_frontier_line,
            units=units,
            point_normalizer=_normalize_carbon,
            x_direction="min",
            y_direction="min",
            post_render=self._decorate_carbon_energy(
                grid_intensities,
                show_grid_references,
                color_dominated_by,
                annotation_text,
            ),
        )

    def _decorate_carbon_energy(
        self,
        grid_intensities: Optional[Sequence[Tuple[str, float]]],
        show_grid_references: bool,
        color_dominated_by: Optional[str],
        annotation_text: Optional[str],
    ):
        def decorate(fig, agent_dicts, theme):
            if show_grid_references and self.backend == "plotly":
                refs = (
                    grid_intensities if grid_intensities is not None
                    else self.DEFAULT_GRID_INTENSITIES
                )
                _add_grid_intensity_references(
                    fig, agent_dicts, refs, theme,
                )
            if color_dominated_by is not None or self.show_simulated_marker:
                _restyle_markers(
                    fig, agent_dicts, theme,
                    color_dominated_by=color_dominated_by,
                    show_simulated=self.show_simulated_marker,
                )
            if self.backend == "plotly":
                text = annotation_text or (
                    "Lower-left = Greenest<br>Independent of accuracy"
                )
                _add_corner_annotation(fig, text, theme)
        return decorate

    # ---------------------------------------------------------- all projections
    def plot_all_projections(
        self,
        agents: Optional[Sequence[Any]],
        frontier: Optional[Sequence[Any]] = None,
        save_dir: Optional[Union[str, Path]] = None,
        *,
        context: Optional[ChartContext] = None,
        theme: Optional[VisualTheme] = None,
        prefix: str = "",
    ) -> Dict[str, Any]:
        """Generate all three specialised plots."""
        effective_context = context or self.context
        effective_theme = theme or self.theme

        if save_dir is not None:
            try:
                Path(save_dir).mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise ParetoPlotterError(
                    f"could not create save_dir {save_dir!r}: {exc}"
                ) from exc

        def _path(name: str) -> Optional[Path]:
            if save_dir is None:
                return None
            return Path(save_dir) / f"{prefix}{name}.html"

        plots: Dict[str, Any] = {
            "accuracy_vs_carbon": self.plot_accuracy_vs_carbon(
                agents, frontier,
                context=effective_context, theme=effective_theme,
                save_path=_path("accuracy_vs_carbon"),
            ),
            "latency_vs_energy": self.plot_latency_vs_energy(
                agents, frontier,
                context=effective_context, theme=effective_theme,
                save_path=_path("latency_vs_energy"),
            ),
            "carbon_vs_energy": self.plot_carbon_vs_energy(
                agents, frontier,
                context=effective_context, theme=effective_theme,
                save_path=_path("carbon_vs_energy"),
            ),
        }
        logger.info("Generated %d projection plots", len(plots))
        return plots

    # ---------------------------------------------------------- shared pipeline
    def _render(
        self,
        agents: Optional[Sequence[Any]],
        frontier: Optional[Sequence[Any]],
        *,
        x: str,
        y: str,
        title: str,
        save_path: Optional[Union[str, Path]],
        context: Optional[ChartContext],
        theme: Optional[VisualTheme],
        include_frontier_line: bool,
        units: Optional[Mapping[str, str]],
        point_normalizer: Optional[Any] = None,
        x_direction: str = "min",
        y_direction: str = "max",
        post_render: Optional[Any] = None,
    ) -> Any:
        """Shared body for all three plot methods."""
        agent_dicts = self._agent_list(agents, "agents")
        frontier_dicts = (
            None if frontier is None
            else self._agent_list(frontier, "frontier")
        )

        effective_theme = _validate_theme(theme) or self.theme
        effective_context = (
            _validate_context(context)
            or self.context
            or infer_context(agent_dicts, {"accuracy": "%"})
        )

        # Backend-aware banner: matplotlib uses \n, plotly uses <br><sub>.
        titled = _title_with_banner(
            title,
            effective_context,
            self.show_provenance_banner,
            self.backend,
        )

        # Base render via the shared pipeline.
        fig = render_tradeoff_figure(
            results=agent_dicts,
            pareto=frontier_dicts,
            x=x, y=y,
            title=titled,
            x_label=(units or {}).get(x, x),
            y_label=(units or {}).get(y, y),
            context=effective_context,
            theme=effective_theme,
            backend=self.backend,
            export_path=None,   # exported after post-processing
            include_frontier_line=include_frontier_line,
            units=units,
            point_normalizer=point_normalizer,
            x_direction=x_direction,
            y_direction=y_direction,
        )

        # Per-chart decorations.
        if post_render is not None and agent_dicts:
            try:
                post_render(fig, agent_dicts, effective_theme)
            except Exception as exc:
                logger.warning(
                    "post_render for %s failed: %s", title, exc,
                )

        # Export after decorations are applied.
        if save_path is not None:
            try_export(fig, save_path)

        return fig

    def _agent_list(
        self, agents: Optional[Sequence[Any]], name: str,
    ) -> List[Mapping[str, Any]]:
        """Normalise ``agents``/``frontier`` to a list of dicts."""
        if agents is None:
            return []
        if isinstance(agents, (str, bytes)):
            raise ParetoPlotterError(
                f"{name} must be a sequence of agents, not a string."
            )
        try:
            entries = list(agents)
        except TypeError as exc:
            raise ParetoPlotterError(
                f"{name} must be iterable, got "
                f"{type(agents).__name__}: {exc}"
            ) from exc
        out: List[Mapping[str, Any]] = []
        skipped = 0
        for a in entries:
            if a is None:
                skipped += 1
                continue
            try:
                out.append(_agent_to_dict(a))
            except ParetoPlotterError as exc:
                logger.warning(
                    "%s entry dropped: %s", name, exc,
                )
                skipped += 1
        if skipped:
            logger.warning(
                "%s: %d/%d entries skipped.", name, skipped, len(entries),
            )
        return out

    # ---------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "ParetoPlotter("
            f"backend={self.backend!r}, "
            f"theme={self.theme.palette.name!r}, "
            f"context={'attached' if self.context else 'inferred'}, "
            f"banner={self.show_provenance_banner}, "
            f"simulated={self.show_simulated_marker})"
        )


def _default_theme() -> VisualTheme:
    from .contracts.theme import DEFAULT_THEME
    return DEFAULT_THEME


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "ParetoExplorerError",
    "ParetoPlotter",
    "ParetoPlotterError",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m visualization.pareto_plotter
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys
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

    # ---- 1. _agent_to_dict ------------------------------------ #
    print("_agent_to_dict:")
    from dataclasses import dataclass

    @dataclass
    class _Agent:
        agent_id: str
        accuracy: float
        energy_kwh: float
        carbon_co2e_kg: float
        latency_ms: float
        simulated: bool = False
        source: str = "measured"

    d = _agent_to_dict(_Agent("gpt4", 0.95, 0.005, 0.001, 200))
    _check("dict: agent_id preserved", d["agent_id"] == "gpt4")
    _check("dict: energy in Wh", d["energy_wh"] == 5.0)
    _check("dict: carbon in g", d["carbon_g"] == 1.0)
    _check(
        "dict: accuracy in pct (ratio → ×100)",
        d["accuracy_pct"] == 95.0,
    )

    # dict input
    d = _agent_to_dict({
        "agent_id": "x", "accuracy": 0.9, "energy_wh": 10.0,
        "carbon_g": 2.0, "latency_ms": 50.0,
    })
    _check("mapping input accepted", d["agent_id"] == "x")
    _check(
        "already-percent accuracy preserved",
        _agent_to_dict({"agent_id": "y", "accuracy_pct": 85.0,
                        "energy_wh": 1, "carbon_g": 1,
                        "latency_ms": 1})["accuracy_pct"] == 85.0,
    )

    # id=0 handling
    d = _agent_to_dict({
        "id": 0, "accuracy": 0.9, "energy_wh": 1,
        "carbon_g": 1, "latency_ms": 1,
    })
    _check("id=0 → agent_id='0'", d["agent_id"] == "0")

    # non-agent rejected
    for bad in (42, "not-an-agent", object()):
        try:
            _agent_to_dict(bad)
            _check(f"reject {type(bad).__name__}", False)
        except ParetoPlotterError:
            _check(f"reject {type(bad).__name__}", True)

    # ---- 2. _title_with_banner -------------------------------- #
    print("_title_with_banner:")
    from visualization.contracts.chart_schema import (
        ChartContext, DataSource, TrustLevel,
    )
    ctx = ChartContext(
        source=DataSource.MEASURED.value,
        trust_level=TrustLevel.HIGH.value,
        system_version="v5",
    )
    plotly_title = _title_with_banner("Accuracy", ctx, True, "plotly")
    _check(
        "plotly title uses <br><sub>",
        "<br><sub>" in plotly_title and "<br>" not in
        plotly_title.split("</sub>")[-1],
    )
    mpl_title = _title_with_banner("Accuracy", ctx, True, "matplotlib")
    _check(
        "matplotlib title uses \\n, no HTML",
        "\n" in mpl_title and "<br>" not in mpl_title,
    )
    _check(
        "banner disabled → plain title",
        _title_with_banner("X", ctx, False, "plotly") == "X",
    )
    _check(
        "context=None → plain title",
        _title_with_banner("X", None, True, "plotly") == "X",
    )

    # ---- 3. Class construction -------------------------------- #
    print("construction:")
    if not ParetoPlotter.available_backends().get("plotly"):
        print("  (skipping plotly-dependent tests)")
    else:
        p = ParetoPlotter(backend="plotly")
        _check("constructs", isinstance(p, ParetoPlotter))
        _check("repr mentions backend", "backend='plotly'" in repr(p))
        _check("backend property", p.backend == "plotly")
        _check(
            "available_backends is a dict",
            isinstance(ParetoPlotter.available_backends(), dict),
        )
        _check(
            "plotly reported available",
            ParetoPlotter.available_backends()["plotly"] is True,
        )

        # Validation
        for bad_kwargs in (
            dict(backend="bokeh"),
            dict(backend=42),
            dict(theme="not-a-theme"),
            dict(context="not-a-context"),
            dict(show_provenance_banner="yes"),
            dict(show_simulated_marker=1),
        ):
            try:
                ParetoPlotter(**bad_kwargs)  # type: ignore[arg-type]
                _check(f"reject {list(bad_kwargs)}", False)
            except ParetoPlotterError:
                _check(f"reject {list(bad_kwargs)}", True)
            except Exception:
                _check(f"reject {list(bad_kwargs)}", False)

    # ---- 4. Render dispatch (mocked pipeline) ----------------- #
    print("render dispatch (mocked):")

    class _MockTrace:
        def __init__(self, name):
            self.name = name
            self.text = []

    class _MockFig:
        def __init__(self):
            self.data = [
                _MockTrace("dominated"),
                _MockTrace("frontier"),
            ]
            self._update_traces_calls = []
            self._add_trace_calls = []
            self._annotations = []

        def update_traces(self, **kw):
            self._update_traces_calls.append(kw)

        def add_trace(self, trace):
            self._add_trace_calls.append(trace)

        def add_vline(self, **kw):
            pass

        def add_annotation(self, **kw):
            self._annotations.append(kw)

        def update_layout(self, **kw):
            pass

        def update_xaxes(self, **kw):
            pass

        def update_yaxes(self, **kw):
            pass

    agents = [
        {"agent_id": "A", "accuracy": 0.95, "energy_wh": 5.0,
         "carbon_g": 1.0, "latency_ms": 200, "source": "measured"},
        {"agent_id": "B", "accuracy": 0.88, "energy_wh": 2.0,
         "carbon_g": 0.4, "latency_ms": 120, "source": "measured"},
    ]

    with patch(
        "visualization.pareto_plotter.render_tradeoff_figure",
    ) as mock_render:
        mock_fig = _MockFig()
        mock_render.return_value = mock_fig

        if ParetoPlotter.available_backends().get("plotly"):
            p = ParetoPlotter(backend="plotly")
            fig = p.plot_accuracy_vs_carbon(agents, [])
            _check("returns mocked figure", fig is mock_fig)
            _check(
                "pipeline called once",
                mock_render.call_count == 1,
            )
            kwargs = mock_render.call_args.kwargs
            _check("x axis is carbon_g", kwargs["x"] == "carbon_g")
            _check(
                "y axis is accuracy_pct",
                kwargs["y"] == "accuracy_pct",
            )
            _check(
                "point_normalizer present",
                kwargs["point_normalizer"] is not None,
            )
            _check(
                "export_path is None (post-decor export)",
                kwargs["export_path"] is None,
            )

    # ---- 5. Post-render decorations --------------------------- #
    print("decorations:"):
    fig = _MockFig()
    fig.data[0].text = ["A"]
    fig.data[1].text = ["B"]

    _restyle_markers(
        fig, agents, _default_theme(),
        color_dominated_by=None,
        show_simulated=True,
    )
    _check(
        "no simulated points → no update_traces",
        len(fig._update_traces_calls) == 0,
    )

    # With a simulated point in the dominated trace
    sim_agents = [
        {"agent_id": "A", "simulated": True},
        {"agent_id": "B", "simulated": False},
    ]
    fig = _MockFig()
    fig.data[0].text = ["A", "B"]
    _restyle_markers(
        fig, sim_agents, _default_theme(),
        color_dominated_by=None,
        show_simulated=True,
    )
    _check(
        "simulated point triggers update_traces",
        len(fig._update_traces_calls) >= 1,
    )

    # color_dominated_by path
    fig = _MockFig()
    fig.data[0].text = ["A", "B"]
    colour_agents = [
        {"agent_id": "A", "accuracy": 0.9},
        {"agent_id": "B", "accuracy": 0.8},
    ]
    _restyle_markers(
        fig, colour_agents, _default_theme(),
        color_dominated_by="accuracy",
        show_simulated=False,
    )
    has_colorscale = any(
        "colorscale" in c.get("marker", {})
        for c in fig._update_traces_calls
    )
    _check(
        "color_dominated_by uses colorscale",
        has_colorscale,
    )

    # ---- 6. plot_all_projections creates save_dir ------------- #
    print("plot_all_projections:")
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        target = f"{tmp}/plots"
        with patch(
            "visualization.pareto_plotter.render_tradeoff_figure",
        ) as mock_render, patch(
            "visualization.pareto_plotter.try_export",
        ) as mock_export:
            mock_render.return_value = _MockFig()
            mock_export.return_value = None

            if ParetoPlotter.available_backends().get("plotly"):
                p = ParetoPlotter(backend="plotly")
                plots = p.plot_all_projections(
                    agents, [], save_dir=target,
                )
                _check(
                    "three plots returned",
                    len(plots) == 3,
                )
                _check(
                    "save_dir created",
                    Path(target).is_dir(),
                )
                _check(
                    "try_export called 3 times",
                    mock_export.call_count == 3,
                )

    # ---- 7. Public API surface -------------------------------- #
    print("exports:")
    _check("ParetoPlotter exported", "ParetoPlotter" in __all__)
    _check(
        "ParetoPlotterError exported",
        "ParetoPlotterError" in __all__,
    )
    _check(
        "ParetoExplorerError re-exported",
        "ParetoExplorerError" in __all__,
    )
    _check(
        "ParetoPlotterError is ValueError",
        issubclass(ParetoPlotterError, ValueError),
    )
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
