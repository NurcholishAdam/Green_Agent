"""
Consolidated Pareto visualizer.

Replaces the four overlapping legacy modules (energy_pareto.py,
leaderboard_plots.py, pareto_plots.py, and pareto_plotter.py) with a
single tested class. Legacy function names are preserved as thin
wrappers in the __init__ of the folder.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _try_plotly():
    try:
        import plotly.graph_objects as go  # type: ignore
        return go
    except ImportError:
        return None


class ParetoExplorer:
    """
    Interactive Pareto explorer.

    Renders 2D projections of a multi-objective frontier with frontier
    highlighting, provenance banners, and audience-aware colors.

    Backend: Plotly (interactive, exportable) by default; falls back to
    matplotlib if Plotly is unavailable.
    """

    def __init__(
        self,
        *,
        theme=None,
        backend: str = "plotly",
    ):
        from ..contracts.visual_theme import DEFAULT_THEME
        self.theme = theme or DEFAULT_THEME
        self.backend = backend

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def plot_tradeoff(
        self,
        points: List[Dict[str, Any]],
        *,
        x: str,
        y: str,
        frontier_ids: Optional[Iterable[str]] = None,
        x_label: Optional[str] = None,
        y_label: Optional[str] = None,
        title: Optional[str] = None,
        context=None,
        color_by: Optional[str] = None,
    ):
        """
        Plot a 2D projection of a Pareto frontier.

        `points` is a list of dicts with at least `x`, `y`, and an
        identifier key (defaults to `agent_id`).

        `frontier_ids` lists which points are on the frontier. Points not
        in the list are rendered as "dominated".
        """
        go = _try_plotly()
        if go is None and self.backend == "plotly":
            logger.warning("Plotly unavailable; falling back to matplotlib")
            self.backend = "matplotlib"

        frontier_set = set(frontier_ids or [])
        id_key = color_by or "agent_id"

        # --- Split points by frontier status ---
        non_frontier: List[Dict[str, Any]] = []
        frontier: List[Dict[str, Any]] = []
        for p in points or []:
            pid = str(p.get(id_key, ""))
            if pid in frontier_set:
                frontier.append(p)
            else:
                non_frontier.append(p)

        # --- Extract coordinates, skipping invalid entries ---
        def coords(seq):
            xs, ys, names = [], [], []
            for p in seq:
                xv, yv = p.get(x), p.get(y)
                if isinstance(xv, (int, float)) and isinstance(yv, (int, float)):
                    xs.append(xv); ys.append(yv)
                    names.append(str(p.get(id_key, "")))
            return xs, ys, names

        nx, ny, nnames = coords(non_frontier)
        fx, fy, fnames = coords(frontier)

        subtitle = context.to_subtitle() if context is not None else ""

        if self.backend == "plotly":
            fig = go.Figure()
            if nx:
                fig.add_trace(go.Scatter(
                    x=nx, y=ny, mode="markers",
                    name="dominated",
                    text=nnames,
                    marker=dict(
                        size=self.theme.marker_size_dominated,
                        color=self.theme.palette.dominated,
                    ),
                ))
            if fx:
                fig.add_trace(go.Scatter(
                    x=fx, y=fy, mode="markers",
                    name="frontier",
                    text=fnames,
                    marker=dict(
                        size=self.theme.marker_size_frontier,
                        color=self.theme.palette.frontier,
                        symbol="diamond",
                    ),
                ))
            full_title = title or f"{x} vs {y}"
            if subtitle:
                full_title = f"{full_title}<br><sub>{subtitle}</sub>"
            fig.update_layout(
                title=full_title,
                xaxis_title=x_label or x,
                yaxis_title=y_label or y,
                plot_bgcolor=self.theme.palette.background,
                font=dict(family=self.theme.font_family),
            )
            fig.update_xaxes(gridcolor=self.theme.palette.grid)
            fig.update_yaxes(gridcolor=self.theme.palette.grid)
            return fig

        # --- Matplotlib fallback ---
        import matplotlib.pyplot as plt  # type: ignore
        fig, ax = plt.subplots(
            figsize=(
                self.theme.figure_width_px / 100,
                self.theme.figure_height_px / 100,
            )
        )
        if nx:
            ax.scatter(
                nx, ny, alpha=0.5,
                color=self.theme.palette.dominated,
                label="dominated",
            )
        if fx:
            ax.scatter(
                fx, fy,
                color=self.theme.palette.frontier,
                edgecolors="black",
                s=self.theme.marker_size_frontier ** 2,
                label="frontier",
            )
        full_title = title or f"{x} vs {y}"
        if subtitle:
            full_title = f"{full_title}\n{subtitle}"
        ax.set_xlabel(x_label or x)
        ax.set_ylabel(y_label or y)
        ax.set_title(full_title)
        ax.grid(True, color=self.theme.palette.grid)
        ax.legend()
        return fig


# ---------------------------------------------------------------------------
# Legacy-compatible convenience functions (preserved for backward compat)
# ---------------------------------------------------------------------------

def plot_accuracy_vs_energy(results, pareto_front, context=None):
    """Legacy wrapper. Returns a Plotly figure instead of calling show()."""
    explorer = ParetoExplorer()
    frontier_ids = [
        r.get("agent_id", str(i)) for i, r in enumerate(pareto_front or [])
    ]
    return explorer.plot_tradeoff(
        points=results,
        x="energy", y="accuracy",
        frontier_ids=frontier_ids,
        x_label="Energy (Wh)",
        y_label="Accuracy",
        title="Accuracy vs Energy (Green Pareto Frontier)",
        context=context,
    )


def plot_latency_vs_energy(results, context=None):
    """Legacy wrapper. Returns a Plotly figure instead of calling show()."""
    explorer = ParetoExplorer()
    return explorer.plot_tradeoff(
        points=results,
        x="energy", y="latency",
        x_label="Energy (Wh)", y_label="Latency (s)",
        title="Latency vs Energy",
        context=context,
    )


def plot_carbon_vs_energy(results, context=None):
    """Legacy wrapper. Returns a Plotly figure instead of calling show()."""
    explorer = ParetoExplorer()
    return explorer.plot_tradeoff(
        points=results,
        x="energy", y="carbon",
        x_label="Energy (Wh)", y_label="Carbon (kg CO₂)",
        title="Carbon vs Energy",
        context=context,
    )


def plot_accuracy_vs_carbon(results, context=None):
    """Legacy wrapper. Returns a Plotly figure instead of calling show()."""
    explorer = ParetoExplorer()
    return explorer.plot_tradeoff(
        points=results,
        x="carbon", y="accuracy",
        x_label="Carbon (kg CO₂)", y_label="Accuracy",
        title="Accuracy vs Carbon",
        context=context,
    )
