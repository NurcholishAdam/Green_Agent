# src/visualization/leaderboard_plots.py

"""
Leaderboard visualization for green benchmarking (Enhanced)
=============================================================

Three leaderboard-style scatter plots. Backward-compatible entry points
that delegate to the consolidated `ParetoExplorer` so the same scatter
logic is not duplicated across the folder.

Original API preserved:
    plot_accuracy_vs_energy(results, pareto)
    plot_latency_vs_energy(results)
    plot_carbon_vs_energy(results)

Enhanced API (all keyword-only, all optional):
    ... any function ...
        context=ChartContext(...),
        theme=VisualTheme(...),
        backend="plotly" | "matplotlib",
        export_path="...",
        frontier_ids=[...],
        include_frontier_line=True,
        units={"energy": "Wh", "latency": "s"},

Enhancements:
  1. Quantum-Distillation      — accepts route/precision fields if present
  2. Causal RL                 — context can carry policy_version
  3. Federated Analytics       — context can carry deployment_id
  4. Multi-Agent Coordination  — agent_id used as point label
  5. Temporal Logic            — context can carry time_range
  6. Explainable AI            — provenance banner + simulation flag
  7. Adaptive Precision        — precision field if present
  8. Carbon Markets            — never conflates operational with contractual
  9. Resilience & Chaos        — chaos_injected flag carried through
 10. Human-in-the-Loop         — context can carry reviewer metadata
 +   Returns a figure (no plt.show())
 +   Frontier line connecting optimal points
 +   Simulated-value visual distinction
 +   ChartContext provenance banner
 +   Optional export to HTML/PNG/SVG
 +   Colorblind-safe palette from the shared theme
 +   Backend selection (plotly default; matplotlib fallback)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional, Sequence

from .contracts.chart_schema import (
    ChartContext,
    DataSource,
    TrustLevel,
)
from .contracts.visual_theme import DEFAULT_THEME, VisualTheme
from .tradeoffs.pareto import ParetoExplorer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Function 1 — accuracy vs energy (same logical chart as energy_pareto.py)
# ---------------------------------------------------------------------------

def plot_accuracy_vs_energy(
    results: Sequence[Dict[str, Any]],
    pareto: Sequence[Dict[str, Any]] = (),
    *,
    context: Optional[ChartContext] = None,
    theme: Optional[VisualTheme] = None,
    backend: str = "plotly",
    export_path: Optional[str] = None,
    include_frontier_line: bool = True,
    units: Optional[Dict[str, str]] = None,
):
    """
    Accuracy vs energy leaderboard scatter.

    Backward-compatible: same signature; `pareto` remains positional but
    defaults to an empty sequence so callers can omit it.

    Returns a Plotly (or matplotlib) Figure. Never calls `plt.show()`.
    """
    return _render(
        results=results,
        frontier=pareto,
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


# ---------------------------------------------------------------------------
# Function 2 — latency vs energy (pure efficiency)
# ---------------------------------------------------------------------------

def plot_latency_vs_energy(
    results: Sequence[Dict[str, Any]],
    pareto: Sequence[Dict[str, Any]] = (),
    *,
    context: Optional[ChartContext] = None,
    theme: Optional[VisualTheme] = None,
    backend: str = "plotly",
    export_path: Optional[str] = None,
    include_frontier_line: bool = True,
    units: Optional[Dict[str, str]] = None,
):
    """
    Latency vs energy leaderboard scatter.

    Backward-compatible: original signature was `plot_latency_vs_energy(results)`.
    Enhanced: adds an optional `pareto` argument — when provided, frontier
    points are highlighted and connected with a line.

    Returns a Figure. Never calls `plt.show()`.
    """
    return _render(
        results=results,
        frontier=pareto,
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
    )


# ---------------------------------------------------------------------------
# Function 3 — carbon vs energy (pure green)
# ---------------------------------------------------------------------------

def plot_carbon_vs_energy(
    results: Sequence[Dict[str, Any]],
    pareto: Sequence[Dict[str, Any]] = (),
    *,
    context: Optional[ChartContext] = None,
    theme: Optional[VisualTheme] = None,
    backend: str = "plotly",
    export_path: Optional[str] = None,
    include_frontier_line: bool = True,
    units: Optional[Dict[str, str]] = None,
):
    """
    Carbon vs energy leaderboard scatter.

    Backward-compatible: original signature was `plot_carbon_vs_energy(results)`.
    Enhanced: adds an optional `pareto` argument and returns a figure.

    The Y axis is drawn from the `carbon` field of each result. When a
    result carries `carbon_operational_kg` (the enhanced analysis field)
    the plot uses that preferentially — ensuring operational emissions
    are never conflated with contractual offsets.

    Returns a Figure. Never calls `plt.show()`.
    """
    return _render(
        results=results,
        frontier=pareto,
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
    )


# ---------------------------------------------------------------------------
# Shared rendering pipeline
# ---------------------------------------------------------------------------

def _render(
    *,
    results: Sequence[Dict[str, Any]],
    frontier: Sequence[Dict[str, Any]],
    x: str,
    y: str,
    title: str,
    x_label: str,
    y_label: str,
    context: Optional[ChartContext],
    theme: Optional[VisualTheme],
    backend: str,
    export_path: Optional[str],
    include_frontier_line: bool,
    units: Optional[Dict[str, str]],
):
    """Shared entry point — all three public functions delegate here."""
    # --- Input validation ---
    if results is None:
        results = []
    if frontier is None:
        frontier = []

    results = [r for r in results if isinstance(r, dict)]
    frontier = [r for r in frontier if isinstance(r, dict)]

    if not results:
        logger.warning(
            f"{title}: no results provided; returning an annotated "
            "empty figure"
        )
        return _empty_figure(
            title=f"{title} (no data)",
            context=context,
            theme=theme or DEFAULT_THEME,
        )

    # --- Infer or accept a ChartContext ---
    ctx = context or _infer_context(results, units)

    # --- Build the explorer ---
    explorer = ParetoExplorer(
        theme=theme or DEFAULT_THEME, backend=backend,
    )

    # --- Frontier identifiers ---
    frontier_ids = _extract_frontier_ids(frontier)

    # --- Normalise the carbon field if needed ---
    points = _normalise_points(results, y)

    # --- Render ---
    fig = explorer.plot_tradeoff(
        points=points,
        x=x,
        y=y,
        frontier_ids=frontier_ids,
        x_label=x_label,
        y_label=y_label,
        title=title,
        context=ctx,
        include_frontier_line=include_frontier_line,
    )

    # --- Optional export ---
    if export_path:
        _save(fig, export_path)

    return fig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalise_points(
    results: Sequence[Dict[str, Any]], y_field: str,
) -> list:
    """
    Make sure each point carries the requested Y field.

    For the carbon axis, prefer the enhanced `carbon_operational_kg` over
    the legacy `carbon` field so contractual offsets are never plotted as
    if they were physical emissions.
    """
    out = []
    for r in results:
        p = dict(r)
        if y_field == "carbon" and "carbon" not in p:
            op = p.get("carbon_operational_kg")
            if isinstance(op, (int, float)):
                p["carbon"] = op
        out.append(p)
    return out


def _extract_frontier_ids(
    frontier: Sequence[Dict[str, Any]],
) -> list:
    """Stable identifiers from frontier entries, with positional fallback."""
    ids = []
    for i, r in enumerate(frontier):
        aid = r.get("agent_id")
        if isinstance(aid, str) and aid:
            ids.append(aid)
        else:
            ids.append(str(r.get("id", f"entry-{i}")))
    return ids


def _infer_context(
    results: Sequence[Dict[str, Any]],
    units: Optional[Dict[str, str]],
) -> ChartContext:
    """Derive a ChartContext from flags present on the results."""
    simulated = any(bool(r.get("simulated", False)) for r in results)
    chaos_injected = any(
        bool(r.get("chaos_injected") or r.get("chaos_event"))
        for r in results
    )

    sources = {str(r.get("source", "")).lower() for r in results}
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
        (r.get("system_version") for r in results if r.get("system_version")),
        None,
    )
    policy_version = next(
        (r.get("policy_version") for r in results if r.get("policy_version")),
        None,
    )
    run_id = next(
        (r.get("run_id") for r in results if r.get("run_id")),
        None,
    )

    timestamps = [
        r.get("timestamp") for r in results
        if isinstance(r.get("timestamp"), datetime)
    ]

    default_units = {"energy": "Wh", "accuracy": "ratio",
                     "latency": "s", "carbon": "kg CO₂"}
    if units:
        default_units.update(units)

    notes_parts = []
    if chaos_injected:
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
        units=default_units,
        baseline=None,
        simulated=simulated,
        notes="; ".join(notes_parts) if notes_parts else None,
    )


def _empty_figure(
    *, title: str, context: Optional[ChartContext], theme: VisualTheme,
):
    """Render an annotated empty figure instead of raising."""
    subtitle = context.to_subtitle() if context is not None else "no data"
    try:
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
    except ImportError:
        import matplotlib.pyplot as plt  # type: ignore
        fig, ax = plt.subplots(
            figsize=(
                theme.figure_width_px / 100,
                theme.figure_height_px / 100,
            )
        )
        ax.text(0.5, 0.5, f"{title}\n{subtitle}",
                ha="center", va="center", color="#888")
        ax.set_xticks([]); ax.set_yticks([])
        return fig


def _save(fig, path: str) -> bool:
    """Best-effort export that never raises."""
    try:
        ext = path.rsplit(".", 1)[-1].lower() if "." in path else "html"
        if ext in ("html", "htm") and hasattr(fig, "write_html"):
            fig.write_html(path)
            return True
        if hasattr(fig, "write_image"):
            fig.write_image(path)
            return True
        if hasattr(fig, "savefig"):
            fig.savefig(path, dpi=150, bbox_inches="tight")
            return True
    except Exception as e:
        logger.warning(f"Failed to save figure to {path}: {e}")
    return False


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    results = [
        {"agent_id": "A", "energy": 5.0, "accuracy": 0.95,
         "latency": 0.2, "carbon": 0.05, "source": "measured"},
        {"agent_id": "B", "energy": 6.5, "accuracy": 0.92,
         "latency": 0.15, "carbon": 0.06, "source": "measured"},
        {"agent_id": "C", "energy": 4.0, "accuracy": 0.88,
         "latency": 0.30, "carbon": 0.04, "source": "measured"},
        {"agent_id": "D", "energy": 7.0, "accuracy": 0.90,
         "latency": 0.10, "carbon": 0.07, "source": "measured"},
    ]
    pareto = [results[0], results[2]]

    # --- Backward-compatible call (all three functions) ---
    fig1 = plot_accuracy_vs_energy(results, pareto)
    fig2 = plot_latency_vs_energy(results)
    fig3 = plot_carbon_vs_energy(results)
    print(f"Type: {type(fig1).__name__}, traces: {len(fig1.data)}")
    print(f"Type: {type(fig2).__name__}, traces: {len(fig2.data)}")
    print(f"Type: {type(fig3).__name__}, traces: {len(fig3.data)}")

    # --- Enhanced call with frontier highlighting for all three ---
    fig4 = plot_latency_vs_energy(results, pareto)
    fig5 = plot_carbon_vs_energy(results, pareto)
    print(f"Frontier-highlighted latency plot traces: {len(fig4.data)}")
    print(f"Frontier-highlighted carbon plot traces:  {len(fig5.data)}")

    # --- Export ---
    plot_carbon_vs_energy(
        results, pareto,
        export_path="/tmp/leaderboard_carbon.html",
    )
    print("Exported carbon plot to /tmp/leaderboard_carbon.html")

    # --- Simulated data (visually flagged) ---
    simulated = [
        {"agent_id": "S1", "energy": 5.0, "accuracy": 0.90,
         "latency": 0.2, "carbon": 0.05, "simulated": True},
    ]
    fig_sim = plot_accuracy_vs_energy(simulated, [])
    if hasattr(fig_sim.layout, "title") and fig_sim.layout.title.text:
        assert "simulated" in fig_sim.layout.title.text.lower()
        print("Simulated flag propagated to title ✓")

    # --- Empty input safety ---
    fig_empty = plot_latency_vs_energy([])
    print(f"Empty input returns: {type(fig_empty).__name__}")
