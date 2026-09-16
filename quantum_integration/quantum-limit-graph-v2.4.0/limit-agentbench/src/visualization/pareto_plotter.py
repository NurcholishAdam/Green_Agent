# src/visualization/pareto_plotter.py

"""
Specialized 2D Pareto Plots for Green_Agent (Enhanced)
=======================================================

Implements three critical policy-oriented visualizations:
1. Accuracy vs Carbon — Sustainability reviewers' view
2. Latency vs Energy   — Systems engineers' view
3. Carbon vs Energy (Pure Green) — Pure environmental efficiency

Why multiple 2D plots instead of one 7D plot:
- Humans cannot reason in 7D, but policies are 2D
- Each plot answers a different policy question
- Projections reveal different "faces" of the Pareto frontier

Original API preserved:
    ParetoPlotter(backend="plotly")
    plotter.plot_accuracy_vs_carbon(agents, frontier, title=..., save_path=...)
    plotter.plot_latency_vs_energy(agents, frontier, ...)
    plotter.plot_carbon_vs_energy(agents, frontier, ...)
    plotter.plot_all_projections(agents, frontier, save_dir=...)

Enhanced API (all keyword-only, all optional):
    ParetoPlotter(
        backend="plotly",
        theme=VisualTheme(...),
        context=ChartContext(...),
        show_simulated=True,
        show_provenance_banner=True,
    )
    plotter.plot_accuracy_vs_carbon(
        agents, frontier,
        *,
        title=..., save_path=..., context=..., theme=...,
        reference_ratios=(50, 100, 200),
        show_grid_references=False,
        include_frontier_line=True,
    )
    plotter.plot_latency_vs_energy(
        agents, frontier,
        *,
        sla_lines=(100, 500, 1000),
        ...
    )
    plotter.plot_carbon_vs_energy(
        agents, frontier,
        *,
        grid_intensities=(("US-CA Grid", 0.2), ("CN Grid", 0.6), ("FR Grid", 0.05)),
        color_dominated_by="accuracy",
        ...
    )
    plotter.attach_context(ctx)   # update context after construction
    plotter.available_backends()  # classmethod

Enhancements:
  1. Quantum-Distillation      — accepts route/precision fields if present
  2. Causal RL                 — context carries policy_version for causal attribution
  3. Federated Analytics       — context carries deployment_id
  4. Multi-Agent Coordination  — agent_id is used for point labels everywhere
  5. Temporal Logic            — context carries time_range
  6. Explainable AI            — provenance banner + simulated flag on every plot
  7. Adaptive Precision        — precision field flows through
  8. Carbon Markets            — prefers operational carbon over contractual
  9. Resilience & Chaos        — chaos_injected flag flows through
 10. Human-in-the-Loop         — context can carry reviewer metadata
 +   Delegates to consolidated ParetoExplorer (no duplicate scatter logic)
 +   Adopts VisualTheme for colorblind-safe colors
 +   Adopts ChartContext for provenance
 +   Propagates simulated flag visually
 +   Unified export via renderers/export.save_figure
 +   Backend selection preserved (plotly / matplotlib)
 +   All original convenience features retained (reference lines, SLA lines,
     grid intensity references, color-by-accuracy)
 +   ExtendedParetoPoint objects AND dicts are both accepted
 +   Empty-input safety (returns annotated empty figure)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from .contracts.chart_schema import (
    ChartContext,
    DataSource,
    TrustLevel,
)
from .contracts.visual_theme import DEFAULT_THEME, VisualTheme
from .renderers.export import save_figure
from .tradeoffs.pareto import ParetoExplorer

logger = logging.getLogger(__name__)


# ============================================================================
# Backend availability detection (preserved from original)
# ============================================================================

try:
    import plotly.graph_objects as go  # type: ignore
    PLOTLY_AVAILABLE = True
except ImportError:  # pragma: no cover
    go = None  # type: ignore
    PLOTLY_AVAILABLE = False
    logging.warning(
        "Plotly not available. Install with: pip install plotly"
    )

try:
    import matplotlib.pyplot as plt  # type: ignore
    MATPLOTLIB_AVAILABLE = True
except ImportError:  # pragma: no cover
    plt = None  # type: ignore
    MATPLOTLIB_AVAILABLE = False
    logging.warning(
        "Matplotlib not available. Install with: pip install matplotlib"
    )


# ============================================================================
# ParetoPlotter
# ============================================================================

class ParetoPlotter:
    """
    Specialized 2D Pareto visualizations.

    Each plot serves a specific policy/deployment question:

    1. Accuracy vs Carbon
       Question: "What performance am I paying per unit environmental cost?"
       Users: Sustainability reviewers, policy makers, ESG reporters

    2. Latency vs Energy
       Question: "Are fast agents inherently wasteful?"
       Users: Systems engineers, edge deployment teams

    3. Carbon vs Energy (Pure Green)
       Question: "Which agents are environmentally efficient independent
       of performance?"
       Users: Green AI researchers, carbon-budget planners

    The class delegates rendering to the consolidated `ParetoExplorer`
    and uses `VisualTheme` for colorblind-safe colors and `ChartContext`
    for provenance. All original convenience features are preserved.
    """

    # Default reference constants (preserved from the original)
    DEFAULT_ACCURACY_CARBON_RATIOS: Tuple[int, ...] = (50, 100, 200)
    DEFAULT_SLA_LINES_MS: Tuple[int, ...] = (100, 500, 1000)
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
    ):
        """
        Initialise plotter.

        Args:
            backend: `"plotly"` (interactive, default) or `"matplotlib"`
                (static).
            theme: Optional `VisualTheme` for palettes and layout.
                Defaults to `DEFAULT_THEME`.
            context: Optional `ChartContext` for provenance. When omitted,
                a context is inferred from the agents on each plot call.
            show_provenance_banner: Attach a subtitle with source, policy
                version, and simulated flag to every plot.
            show_simulated_marker: Use the `simulated` theme color for
                synthetic points.

        Raises:
            ImportError: When the requested backend is not installed.
        """
        self.backend = backend
        self.theme = theme or DEFAULT_THEME
        self.context = context
        self.show_provenance_banner = show_provenance_banner
        self.show_simulated_marker = show_simulated_marker

        # --- Backend availability ---
        if backend == "plotly" and not PLOTLY_AVAILABLE:
            raise ImportError(
                "Plotly not installed. Run: pip install plotly"
            )
        if backend == "matplotlib" and not MATPLOTLIB_AVAILABLE:
            raise ImportError(
                "Matplotlib not installed. Run: pip install matplotlib"
            )

        # --- Consolidated explorer ---
        self._explorer = ParetoExplorer(
            theme=self.theme, backend=backend,
        )

        logger.info(
            f"Initialized ParetoPlotter with {backend} backend "
            f"(theme={'custom' if theme else 'default'})"
        )

    # ------------------------------------------------------------------
    # Class-level introspection
    # ------------------------------------------------------------------

    @classmethod
    def available_backends(cls) -> Dict[str, bool]:
        """Return which backends are importable in the current env."""
        return {
            "plotly": PLOTLY_AVAILABLE,
            "matplotlib": MATPLOTLIB_AVAILABLE,
        }

    def attach_context(self, context: ChartContext) -> None:
        """
        Attach or update the default `ChartContext` used by future plots.
        """
        self.context = context
        logger.debug("Attached ChartContext to ParetoPlotter")

    # ==================================================================
    # Plot 1 — Accuracy vs Carbon
    # ==================================================================

    def plot_accuracy_vs_carbon(
        self,
        agents: Sequence[Any],
        frontier: Sequence[Any],
        title: str = "Accuracy vs Carbon Footprint",
        save_path: Optional[str] = None,
        *,
        context: Optional[ChartContext] = None,
        theme: Optional[VisualTheme] = None,
        reference_ratios: Optional[Sequence[int]] = None,
        show_references: bool = True,
        include_frontier_line: bool = True,
        units: Optional[Dict[str, str]] = None,
    ):
        """
        Plot Accuracy vs Carbon — Sustainability perspective.

        Policy Question:
        "What performance am I paying per unit environmental cost?"

        This plot reveals:
        - Which agents achieve high accuracy with low carbon
        - The carbon cost of marginal accuracy improvements
        - Green AI trade-offs

        Used by:
        - Sustainability reviewers
        - ESG compliance officers
        - Green AI researchers
        - Climate-conscious organizations

        Args:
            agents: All agents (`ExtendedParetoPoint` objects or dicts).
            frontier: Agents on the Pareto frontier.
            title: Plot title.
            save_path: Optional path to save the plot.
            context: Optional `ChartContext` for this plot. Overrides
                the instance-level context.
            theme: Optional `VisualTheme` override.
            reference_ratios: Accuracy-per-gram reference lines.
                Defaults to `DEFAULT_ACCURACY_CARBON_RATIOS`.
            show_references: Draw the reference lines.
            include_frontier_line: Connect frontier points with a line.
            units: Optional unit overrides for axis labels.

        Returns:
            A Plotly (or matplotlib) Figure. Never `None`.
        """
        # --- Convert agents to dicts ---
        agent_dicts = [_agent_to_dict(a) for a in agents if a is not None]
        frontier_dicts = [_agent_to_dict(a) for a in frontier if a is not None]

        # --- Normalise carbon: prefer operational ---
        for d in agent_dicts + frontier_dicts:
            _prefer_operational_carbon(d)

        # --- Resolve theme and context ---
        effective_theme = theme or self.theme
        effective_context = context or self.context or _infer_context(agent_dicts)

        # --- Render via ParetoExplorer ---
        explorer = ParetoExplorer(theme=effective_theme, backend=self.backend)
        fig = explorer.plot_tradeoff(
            points=agent_dicts,
            x="carbon_g",
            y="accuracy_pct",
            frontier_ids=[d.get("agent_id", "") for d in frontier_dicts],
            x_label=(units or {}).get("carbon", "Carbon Footprint (g CO₂e)"),
            y_label=(units or {}).get("accuracy", "Accuracy (%)"),
            title=_title_with_banner(
                title, effective_context, self.show_provenance_banner,
            ),
            context=effective_context,
            include_frontier_line=include_frontier_line,
            x_log_scale=False,
            y_log_scale=False,
        )

        # --- Reference lines (accuracy/carbon ratio) ---
        if show_references and PLOTLY_AVAILABLE and self.backend == "plotly":
            ratios = reference_ratios or self.DEFAULT_ACCURACY_CARBON_RATIOS
            _add_accuracy_carbon_references(fig, agent_dicts, ratios, effective_theme)

        # --- Simulated markers ---
        if self.show_simulated_marker and self.backend == "plotly":
            _restyle_simulated_markers(fig, agent_dicts, effective_theme)

        # --- Save ---
        if save_path:
            save_figure(fig, save_path)

        return fig

    # ==================================================================
    # Plot 2 — Latency vs Energy
    # ==================================================================

    def plot_latency_vs_energy(
        self,
        agents: Sequence[Any],
        frontier: Sequence[Any],
        title: str = "Latency vs Energy Consumption",
        save_path: Optional[str] = None,
        *,
        context: Optional[ChartContext] = None,
        theme: Optional[VisualTheme] = None,
        sla_lines: Optional[Sequence[int]] = None,
        show_sla_lines: bool = True,
        include_frontier_line: bool = True,
        units: Optional[Dict[str, str]] = None,
    ):
        """
        Plot Latency vs Energy — Systems engineering perspective.

        Policy Question:
        "Are fast agents inherently wasteful?"

        This plot reveals:
        - Whether low latency requires high energy
        - Energy efficiency of fast agents
        - Real-time deployment viability

        Used by:
        - Systems engineers
        - Edge deployment teams
        - Real-time application developers
        - Performance architects

        Key Insight:
        If plot shows strong correlation: architecture couples speed
        and energy. If weak correlation: algorithmic optimizations are
        possible.

        Args:
            agents: All agents.
            frontier: Agents on the Pareto frontier.
            title: Plot title.
            save_path: Optional path to save the plot.
            context: Optional `ChartContext` for this plot.
            theme: Optional `VisualTheme` override.
            sla_lines: Vertical SLA reference lines in milliseconds.
                Defaults to `DEFAULT_SLA_LINES_MS`.
            show_sla_lines: Draw the SLA reference lines.
            include_frontier_line: Connect frontier points with a line.
            units: Optional unit overrides for axis labels.

        Returns:
            A Plotly (or matplotlib) Figure. Never `None`.
        """
        # --- Convert agents to dicts ---
        agent_dicts = [_agent_to_dict(a) for a in agents if a is not None]
        frontier_dicts = [_agent_to_dict(a) for a in frontier if a is not None]

        # --- Resolve theme and context ---
        effective_theme = theme or self.theme
        effective_context = context or self.context or _infer_context(agent_dicts)

        # --- Render via ParetoExplorer ---
        explorer = ParetoExplorer(theme=effective_theme, backend=self.backend)
        fig = explorer.plot_tradeoff(
            points=agent_dicts,
            x="latency_ms",
            y="energy_wh",
            frontier_ids=[d.get("agent_id", "") for d in frontier_dicts],
            x_label=(units or {}).get("latency", "Latency (ms)"),
            y_label=(units or {}).get("energy", "Energy (Wh)"),
            title=_title_with_banner(
                title, effective_context, self.show_provenance_banner,
            ),
            context=effective_context,
            include_frontier_line=include_frontier_line,
        )

        # --- SLA reference lines ---
        if show_sla_lines and PLOTLY_AVAILABLE and self.backend == "plotly":
            lines = sla_lines or self.DEFAULT_SLA_LINES_MS
            _add_sla_lines(fig, lines, effective_theme)

        # --- Simulated markers ---
        if self.show_simulated_marker and self.backend == "plotly":
            _restyle_simulated_markers(fig, agent_dicts, effective_theme)

        # --- Save ---
        if save_path:
            save_figure(fig, save_path)

        return fig

    # ==================================================================
    # Plot 3 — Carbon vs Energy
    # ==================================================================

    def plot_carbon_vs_energy(
        self,
        agents: Sequence[Any],
        frontier: Sequence[Any],
        title: str = "Pure Green: Carbon vs Energy",
        save_path: Optional[str] = None,
        *,
        context: Optional[ChartContext] = None,
        theme: Optional[VisualTheme] = None,
        grid_intensities: Optional[Sequence[Tuple[str, float]]] = None,
        show_grid_references: bool = True,
        color_dominated_by: Optional[str] = "accuracy",
        include_frontier_line: bool = True,
        annotation_text: Optional[str] = None,
        units: Optional[Dict[str, str]] = None,
    ):
        """
        Plot Carbon vs Energy — Pure environmental efficiency.

        Policy Question:
        "Which agents are environmentally efficient independent of
        performance?"

        This is the MOST IMPORTANT plot for green AI.

        Why separate from accuracy:
        - Separates algorithmic efficiency from task difficulty
        - Shows pure environmental performance
        - Reveals hardware-algorithm mismatches
        - Independent of task success

        Used by:
        - Green AI researchers
        - Carbon budget planners
        - Environmental compliance officers
        - Sustainability engineers

        Args:
            agents: All agents.
            frontier: Agents on the Pareto frontier.
            title: Plot title.
            save_path: Optional path to save the plot.
            context: Optional `ChartContext` for this plot.
            theme: Optional `VisualTheme` override.
            grid_intensities: Reference lines for typical grid carbon
                intensities. Defaults to `DEFAULT_GRID_INTENSITIES`.
            show_grid_references: Draw the grid intensity reference lines.
            color_dominated_by: Optional field to color dominated points
                by. Defaults to `"accuracy"`; pass `None` for uniform.
            include_frontier_line: Connect frontier points with a line.
            annotation_text: Optional custom annotation text.
            units: Optional unit overrides for axis labels.

        Returns:
            A Plotly (or matplotlib) Figure. Never `None`.
        """
        # --- Convert agents to dicts ---
        agent_dicts = [_agent_to_dict(a) for a in agents if a is not None]
        frontier_dicts = [_agent_to_dict(a) for a in frontier if a is not None]

        # --- Normalise carbon: prefer operational ---
        for d in agent_dicts + frontier_dicts:
            _prefer_operational_carbon(d)

        # --- Resolve theme and context ---
        effective_theme = theme or self.theme
        effective_context = context or self.context or _infer_context(agent_dicts)

        # --- Render via ParetoExplorer ---
        explorer = ParetoExplorer(theme=effective_theme, backend=self.backend)
        fig = explorer.plot_tradeoff(
            points=agent_dicts,
            x="energy_wh",
            y="carbon_g",
            frontier_ids=[d.get("agent_id", "") for d in frontier_dicts],
            x_label=(units or {}).get("energy", "Energy (Wh)"),
            y_label=(units or {}).get("carbon", "Carbon (g CO₂e)"),
            title=_title_with_banner(
                title, effective_context, self.show_provenance_banner,
            ),
            context=effective_context,
            color_by=color_dominated_by,
            include_frontier_line=include_frontier_line,
        )

        # --- Grid intensity reference lines ---
        if show_grid_references and PLOTLY_AVAILABLE and self.backend == "plotly":
            refs = grid_intensities or self.DEFAULT_GRID_INTENSITIES
            _add_grid_intensity_references(
                fig, agent_dicts, refs, effective_theme,
            )

        # --- Simulated markers ---
        if self.show_simulated_marker and self.backend == "plotly":
            _restyle_simulated_markers(fig, agent_dicts, effective_theme)

        # --- Annotation ---
        if PLOTLY_AVAILABLE and self.backend == "plotly":
            text = annotation_text or (
                "Lower-left = Greenest<br>Independent of accuracy"
            )
            _add_corner_annotation(fig, text, effective_theme)

        # --- Save ---
        if save_path:
            save_figure(fig, save_path)

        return fig

    # ==================================================================
    # Convenience — all three projections
    # ==================================================================

    def plot_all_projections(
        self,
        agents: Sequence[Any],
        frontier: Sequence[Any],
        save_dir: Optional[str] = None,
        *,
        context: Optional[ChartContext] = None,
        theme: Optional[VisualTheme] = None,
        prefix: str = "",
    ) -> Dict[str, Any]:
        """
        Generate all three specialized plots.

        Returns a dashboard with:
        1. Accuracy vs Carbon (sustainability view)
        2. Latency vs Energy (systems view)
        3. Carbon vs Energy (pure green view)

        Args:
            agents: All agents.
            frontier: Pareto frontier.
            save_dir: Optional directory to save plots. When provided,
                each plot is written as an HTML file.
            context: Optional `ChartContext` shared across all plots.
            theme: Optional `VisualTheme` shared across all plots.
            prefix: Optional filename prefix when `save_dir` is set.

        Returns:
            Dict mapping plot name → figure.
        """
        effective_context = context or self.context
        effective_theme = theme or self.theme

        def _path(name: str) -> Optional[str]:
            if not save_dir:
                return None
            return f"{save_dir}/{prefix}{name}.html"

        plots: Dict[str, Any] = {}
        plots["accuracy_vs_carbon"] = self.plot_accuracy_vs_carbon(
            agents, frontier,
            context=effective_context,
            theme=effective_theme,
            save_path=_path("accuracy_vs_carbon"),
        )
        plots["latency_vs_energy"] = self.plot_latency_vs_energy(
            agents, frontier,
            context=effective_context,
            theme=effective_theme,
            save_path=_path("latency_vs_energy"),
        )
        plots["carbon_vs_energy"] = self.plot_carbon_vs_energy(
            agents, frontier,
            context=effective_context,
            theme=effective_theme,
            save_path=_path("carbon_vs_energy"),
        )

        logger.info(f"Generated {len(plots)} projection plots")
        return plots


# ============================================================================
# Internal helpers
# ============================================================================

def _agent_to_dict(agent: Any) -> Dict[str, Any]:
    """
    Convert an ExtendedParetoPoint (attribute-based) or dict into the
    dict shape consumed by `ParetoExplorer`.

    Fields produced:
        agent_id, accuracy, accuracy_pct, energy_kwh, energy_wh,
        carbon_g, carbon_co2e_kg, latency_ms, precision, simulated,
        source, chaos_injected, policy_version, system_version,
        deployment_id, run_id, timestamp
    """
    if isinstance(agent, dict):
        d = dict(agent)
    else:
        # Attribute-based (ExtendedParetoPoint, dataclass, or duck-typed)
        d = {}
        for attr in (
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
        ):
            if hasattr(agent, attr):
                d[attr] = getattr(agent, attr)

        # Nested metadata often carries the audit fields
        meta = d.get("metadata") or {}
        if isinstance(meta, dict):
            for k in (
                "source", "simulated", "policy_version",
                "system_version", "deployment_id", "run_id",
            ):
                if k not in d and k in meta:
                    d[k] = meta[k]

    # --- Canonical ids ---
    d.setdefault("agent_id", d.get("id") or "unknown")

    # --- Canonical energy in Wh ---
    if "energy_wh" not in d:
        if "energy_kwh" in d and isinstance(d["energy_kwh"], (int, float)):
            d["energy_wh"] = d["energy_kwh"] * 1000.0
        else:
            d["energy_wh"] = 0.0

    # --- Canonical carbon in grams ---
    if "carbon_g" not in d:
        if "carbon_co2e_kg" in d and isinstance(d["carbon_co2e_kg"], (int, float)):
            d["carbon_g"] = d["carbon_co2e_kg"] * 1000.0
        elif "carbon_operational_kg" in d and isinstance(d["carbon_operational_kg"], (int, float)):
            d["carbon_g"] = d["carbon_operational_kg"] * 1000.0
        else:
            d["carbon_g"] = 0.0

    # --- Canonical accuracy in percent ---
    if "accuracy_pct" not in d:
        acc = d.get("accuracy", 0.0)
        if isinstance(acc, (int, float)):
            # If accuracy looks like a ratio (0-1), scale to percent
            d["accuracy_pct"] = acc * 100.0 if acc <= 1.0 else float(acc)
        else:
            d["accuracy_pct"] = 0.0

    # --- Ensure numeric fields are present ---
    d.setdefault("latency_ms", 0.0)
    d.setdefault("accuracy", d.get("accuracy_pct", 0.0) / 100.0)

    return d


def _prefer_operational_carbon(d: Dict[str, Any]) -> None:
    """
    Prefer operational over contractual carbon for physical plots.

    The recommendation is explicit: never plot contractual offsets as
    if they were physical emissions.
    """
    op = d.get("carbon_operational_kg")
    if isinstance(op, (int, float)):
        d["carbon_g"] = op * 1000.0


def _title_with_banner(
    title: str,
    context: Optional[ChartContext],
    enabled: bool,
) -> str:
    """Attach the ChartContext subtitle to a title."""
    if not enabled or context is None:
        return title
    subtitle = context.to_subtitle()
    if not subtitle:
        return title
    return f"{title}<br><sub>{subtitle}</sub>"


def _infer_context(agents: Sequence[Dict[str, Any]]) -> ChartContext:
    """
    Derive a ChartContext from the flags present on the agent dicts.
    """
    if not agents:
        return ChartContext(source=DataSource.UNKNOWN.value)

    simulated = any(bool(a.get("simulated", False)) for a in agents)
    chaos = any(
        bool(a.get("chaos_injected") or a.get("chaos_event"))
        for a in agents
    )

    sources = {str(a.get("source", "")).lower() for a in agents}
    sources.discard("")
    if simulated or "fallback" in sources or "simulated" in sources:
        source_kind = DataSource.SIMULATED.value
        trust = TrustLevel.LOW.value
    elif sources == {"measured"} or sources == {"codecarbon"}:
        source_kind = DataSource.MEASURED.value
        trust = TrustLevel.HIGH.value
    elif "estimated" in sources:
        source_kind = DataSource.ESTIMATED.value
        trust = TrustLevel.MEDIUM.value
    else:
        source_kind = DataSource.UNKNOWN.value
        trust = TrustLevel.LOW.value

    system_version = next(
        (a.get("system_version") for a in agents if a.get("system_version")),
        None,
    )
    policy_version = next(
        (a.get("policy_version") for a in agents if a.get("policy_version")),
        None,
    )
    run_id = next(
        (a.get("run_id") for a in agents if a.get("run_id")),
        None,
    )
    deployment_id = next(
        (a.get("deployment_id") for a in agents if a.get("deployment_id")),
        None,
    )

    timestamps = [
        a.get("timestamp") for a in agents
        if isinstance(a.get("timestamp"), datetime)
    ]

    notes_parts = []
    if chaos:
        notes_parts.append("chaos-injected")
    if simulated:
        notes_parts.append("contains simulated values")

    return ChartContext(
        source=source_kind,
        trust_level=trust,
        system_version=system_version,
        policy_version=policy_version,
        run_id=run_id,
        time_range_start=min(timestamps) if timestamps else None,
        time_range_end=max(timestamps) if timestamps else None,
        units={
            "energy": "Wh",
            "carbon": "g CO₂e",
            "accuracy": "%",
            "latency": "ms",
        },
        simulated=simulated,
        notes="; ".join(notes_parts) if notes_parts else None,
    )


def _add_accuracy_carbon_references(
    fig: Any,
    agent_dicts: Sequence[Dict[str, Any]],
    ratios: Sequence[int],
    theme: VisualTheme,
) -> None:
    """Add diagonal accuracy-per-gram reference lines."""
    if not agent_dicts or not PLOTLY_AVAILABLE:
        return
    max_carbon = max(
        (d.get("carbon_g", 0.0) for d in agent_dicts), default=1.0,
    ) or 1.0
    for ratio in ratios:
        fig.add_trace(go.Scatter(
            x=[0, max_carbon],
            y=[0, max_carbon * ratio / 1000.0],
            mode="lines",
            line=dict(color=theme.palette.grid, width=1, dash="dash"),
            showlegend=False,
            hoverinfo="skip",
            name=f"{ratio}%/g",
        ))


def _add_sla_lines(
    fig: Any,
    sla_lines: Sequence[int],
    theme: VisualTheme,
) -> None:
    """Add vertical SLA reference lines."""
    if not PLOTLY_AVAILABLE:
        return
    for sla_ms in sla_lines:
        try:
            fig.add_vline(
                x=sla_ms,
                line_dash="dash",
                line_color=theme.palette.warning,
                opacity=0.3,
                annotation_text=f"{sla_ms}ms SLA",
            )
        except Exception:
            # Older plotly versions lack add_vline
            pass


def _add_grid_intensity_references(
    fig: Any,
    agent_dicts: Sequence[Dict[str, Any]],
    intensities: Sequence[Tuple[str, float]],
    theme: VisualTheme,
) -> None:
    """Add diagonal grid-intensity reference lines."""
    if not agent_dicts or not PLOTLY_AVAILABLE:
        return
    max_energy = max(
        (d.get("energy_wh", 0.0) for d in agent_dicts), default=1.0,
    ) or 1.0
    for label, intensity in intensities:
        fig.add_trace(go.Scatter(
            x=[0, max_energy],
            y=[0, max_energy * intensity],
            mode="lines",
            line=dict(color=theme.palette.grid, width=1, dash="dash"),
            name=label,
            showlegend=True,
            hoverinfo="skip",
        ))


def _restyle_simulated_markers(
    fig: Any,
    agent_dicts: Sequence[Dict[str, Any]],
    theme: VisualTheme,
) -> None:
    """
    Restyle the marker color of the dominated trace for simulated points.

    Since the ParetoExplorer already distinguishes frontier vs.
    dominated, we only re-color the dominated trace's simulated points.
    """
    if not PLOTLY_AVAILABLE or not agent_dicts:
        return
    simulated_names = {
        d.get("agent_id") for d in agent_dicts
        if bool(d.get("simulated", False))
    }
    if not simulated_names:
        return
    for trace in fig.data:
        if getattr(trace, "name", "") != "dominated":
            continue
        texts = list(getattr(trace, "text", []) or [])
        if not texts:
            continue
        colors = [
            theme.palette.simulated if str(t) in simulated_names
            else theme.palette.dominated
            for t in texts
        ]
        try:
            trace.marker.color = colors
        except Exception:
            pass


def _add_corner_annotation(
    fig: Any,
    text: str,
    theme: VisualTheme,
) -> None:
    """Add a corner annotation (used by the pure green plot)."""
    if not PLOTLY_AVAILABLE:
        return
    try:
        fig.add_annotation(
            text=text,
            xref="paper", yref="paper",
            x=0.02, y=0.98,
            showarrow=False,
            bgcolor=theme.palette.frontier,
            opacity=0.15,
        )
    except Exception:
        pass


# ============================================================================
# Demo
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    from dataclasses import dataclass

    @dataclass
    class ExtendedParetoPoint:
        agent_id: str
        accuracy: float
        energy_kwh: float
        carbon_co2e_kg: float
        latency_ms: float
        simulated: bool = False
        source: str = "measured"
        policy_version: Optional[str] = None
        metadata: Optional[Dict[str, Any]] = None

    agents = [
        ExtendedParetoPoint("gpt4", 0.95, 0.005, 0.001, 200,
                            source="measured", policy_version="v5.0.1"),
        ExtendedParetoPoint("llama3", 0.88, 0.002, 0.0004, 120,
                            source="measured", policy_version="v5.0.1"),
        ExtendedParetoPoint("mobilenet", 0.86, 0.001, 0.0002, 80,
                            source="measured", policy_version="v5.0.1"),
        ExtendedParetoPoint("fallback_agent", 0.84, 0.003, 0.0006, 100,
                            simulated=True, source="fallback",
                            policy_version="v5.0.1"),
    ]
    frontier = [agents[0], agents[2]]

    plotter = ParetoPlotter(backend="plotly")

    # --- All three plots with provenance banners ---
    fig1 = plotter.plot_accuracy_vs_carbon(agents, frontier)
    fig2 = plotter.plot_latency_vs_energy(agents, frontier)
    fig3 = plotter.plot_carbon_vs_energy(agents, frontier)

    print(f"accuracy_vs_carbon  traces: {len(fig1.data)}")
    print(f"latency_vs_energy   traces: {len(fig2.data)}")
    print(f"carbon_vs_energy    traces: {len(fig3.data)}")

    # --- Export all three ---
    plots = plotter.plot_all_projections(
        agents, frontier, save_dir="/tmp/pareto_plots",
    )
    print(f"Exported {len(plots)} plots to /tmp/pareto_plots")

    # --- Explicit context overrides the inferred one ---
    from .contracts.chart_schema import ChartContext, DataSource, TrustLevel
    explicit_ctx = ChartContext(
        source=DataSource.MEASURED.value,
        trust_level=TrustLevel.HIGH.value,
        system_version="5.0.0",
        policy_version="v5.0.1",
        baseline="FP32-2025Q4",
        units={"energy": "Wh", "carbon": "g CO₂e", "accuracy": "%"},
    )
    fig4 = plotter.plot_accuracy_vs_carbon(
        agents, frontier, context=explicit_ctx,
    )
    print("Explicit context rendered ✓")

    # --- Backend introspection ---
    print(f"Available backends: {ParetoPlotter.available_backends()}")

    # --- Empty input safety ---
    fig_empty = plotter.plot_accuracy_vs_carbon([], [])
    print(f"Empty input returns: {type(fig_empty).__name__}")
