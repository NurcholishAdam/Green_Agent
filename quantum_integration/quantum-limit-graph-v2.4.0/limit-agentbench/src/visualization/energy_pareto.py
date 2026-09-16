# src/visualization/energy_pareto.py

"""
Accuracy vs Energy Pareto visualizer (Enhanced)
================================================

Backward-compatible entry point for the classic accuracy-vs-energy
Pareto scatter. Delegates to the consolidated `ParetoExplorer` so the
same logic is not duplicated across the folder.

Original API preserved:
    plot_accuracy_vs_energy(results, pareto_front)
    # Original returned None and called plt.show()
    # Enhanced returns a Plotly Figure (or matplotlib fallback)

Enhanced API:
    plot_accuracy_vs_energy(
        results,
        pareto_front,
        *,
        context=None,
        theme=None,
        backend="plotly",
        export_path=None,
    ) -> Figure

Enhancements (all opt-in; defaults preserve original semantics):
  1. Quantum-Distillation      — accepts route/precision fields if present
  2. Causal RL                 — context can carry policy_version
  3. Federated Analytics       — context can carry deployment_id
  4. Multi-Agent Coordination  — accepts agent_id as point label
  5. Temporal Logic            — context can carry time_range
  6. Explainable AI            — titles include source/simulation banner
  7. Adaptive Precision        — accepts precision field if present
  8. Carbon Markets            — separate carbon plot not conflated
  9. Resilience & Chaos        — accepts chaos_injected flag
 10. Human-in-the-Loop         — context can carry reviewer metadata
 +   Returns a figure (no plt.show())
 +   Frontier line connecting optimal points
 +   Simulated-value visual distinction
 +   ChartContext provenance banner
 +   Optional export to HTML/PNG/SVG
 +   Colorblind-safe palette from the shared theme
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .contracts.chart_schema import (
    ChartContext,
    DataSource,
    TradeoffKind,
    TrustLevel,
)
from .contracts.visual_theme import DEFAULT_THEME, VisualTheme
from .tradeoffs.pareto import ParetoExplorer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Original function — preserved signature
# ---------------------------------------------------------------------------

def plot_accuracy_vs_energy(
    results: Sequence[Dict[str, Any]],
    pareto_front: Sequence[Dict[str, Any]],
    *,
    # --- Enhancement keyword-only args (all optional) ---
    context: Optional[ChartContext] = None,
    theme: Optional[VisualTheme] = None,
    backend: str = "plotly",
    export_path: Optional[str] = None,
    include_frontier_line: bool = True,
    units: Optional[Dict[str, str]] = None,
):
    """
    Render the accuracy vs energy Pareto scatter.

    Backward-compatible: `plot_accuracy_vs_energy(results, pareto_front)`
    still produces the same logical chart.

    Enhanced: returns the figure, adds a provenance banner, and (when a
    backend supports it) draws a line through the frontier.

    Args:
        results: All evaluated points. Each is a dict with at least
            `energy` and `accuracy`; optional `agent_id`, `simulated`,
            `source`, `precision`, `chaos_injected`.
        pareto_front: The subset of `results` that is Pareto-optimal.
            Used to highlight the frontier.
        context: A `ChartContext` describing the data source, units,
            baseline, and versions. When omitted, an inferred context is
            built from the results (see `_infer_context`).
        theme: Optional visual theme. Defaults to `DEFAULT_THEME`.
        backend: `"plotly"` (interactive, default) or `"matplotlib"`.
        export_path: If provided, the figure is saved to this path.
            Format is inferred from the extension.
        include_frontier_line: Draw a line through the frontier points.
        units: Optional `{"energy": "Wh", "accuracy": "%"}` override.

    Returns:
        A Plotly `Figure` (or matplotlib `Figure` when backend is
        matplotlib). Never `None`.
    """
    # --- Input validation ---
    if results is None:
        results = []
    if pareto_front is None:
        pareto_front = []

    results = [r for r in results if isinstance(r, dict)]
    pareto_front = [r for r in pareto_front if isinstance(r, dict)]

    if not results:
        logger.warning(
            "plot_accuracy_vs_energy called with no results; "
            "returning an annotated empty figure"
        )
        return _empty_figure(
            title="Accuracy vs Energy (no data)",
            context=context,
            theme=theme or DEFAULT_THEME,
        )

    # --- Infer a ChartContext if the caller did not supply one ---
    ctx = context or _infer_context(results, pareto_front, units)

    # --- Build the explorer ---
    explorer = ParetoExplorer(theme=theme or DEFAULT_THEME, backend=backend)

    # --- Frontier IDs: preserve whatever id the caller used ---
    frontier_ids = _extract_frontier_ids(pareto_front)

    # --- Render ---
    fig = explorer.plot_tradeoff(
        points=results,
        x="energy",
        y="accuracy",
        frontier_ids=frontier_ids,
        x_label=(units or {}).get("energy", "Energy (Wh)"),
        y_label=(units or {}).get("accuracy", "Accuracy"),
        title="Accuracy vs Energy (Green Pareto Frontier)",
        context=ctx,
        include_frontier_line=include_frontier_line,
    )

    # --- Optional export ---
    if export_path:
        _save(fig, export_path)

    return fig


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_frontier_ids(
    pareto_front: Sequence[Dict[str, Any]],
) -> List[str]:
    """
    Pull stable identifiers from the frontier entries.

    Falls back to positional IDs when `agent_id` is absent so the
    highlighting still works on legacy data.
    """
    ids: List[str] = []
    for i, r in enumerate(pareto_front):
        aid = r.get("agent_id")
        if isinstance(aid, str) and aid:
            ids.append(aid)
        else:
            # Synthesise a positional ID matching what the explorer does
            ids.append(str(r.get("id", f"entry-{i}")))
    return ids


def _infer_context(
    results: Sequence[Dict[str, Any]],
    pareto_front: Sequence[Dict[str, Any]],
    units: Optional[Dict[str, str]],
) -> ChartContext:
    """
    Derive a ChartContext from flags present on the results.

    Simulated values, chaos-injected runs, and missing sources are all
    surfaced so a reader cannot mistake a synthetic plot for a measured
    one.
    """
    # --- Simulated / chaos detection ---
    simulated = any(bool(r.get("simulated", False)) for r in results)
    chaos_injected = any(
        bool(r.get("chaos_injected") or r.get("chaos_event"))
        for r in results
    )

    # --- Source inference (honest about defaults) ---
    sources = {
        str(r.get("source", "")).lower()
        for r in results
    }
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

    # --- Version extraction (first non-null wins) ---
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

    # --- Time range ---
    timestamps = [
        r.get("timestamp") for r in results
        if isinstance(r.get("timestamp"), datetime)
    ]
    ts_start = min(timestamps) if timestamps else None
    ts_end = max(timestamps) if timestamps else None

    # --- Default units ---
    default_units = {"energy": "Wh", "accuracy": "ratio"}
    if units:
        default_units.update(units)

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
        units=default_units,
        baseline=None,
        simulated=simulated,
        notes=notes,
    )


def _empty_figure(
    *,
    title: str,
    context: Optional[ChartContext],
    theme: VisualTheme,
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
        ax.text(
            0.5, 0.5, f"{title}\n{subtitle}",
            ha="center", va="center", color="#888",
        )
        ax.set_xticks([])
        ax.set_yticks([])
        return fig


def _save(fig, path: str) -> bool:
    """Best-effort save that never raises."""
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

    # --- Backward-compatible usage (same call as before) ---
    results = [
        {"agent_id": "A", "energy": 5.0, "accuracy": 0.95},
        {"agent_id": "B", "energy": 6.5, "accuracy": 0.92},
        {"agent_id": "C", "energy": 4.0, "accuracy": 0.88},
        {"agent_id": "D", "energy": 7.0, "accuracy": 0.90},
    ]
    pareto_front = [results[0], results[2]]

    fig = plot_accuracy_vs_energy(results, pareto_front)
    print(f"Figure type: {type(fig).__name__}")
    print(f"Traces:      {len(fig.data)}")

    # --- Enhanced usage with explicit context ---
    ctx = ChartContext(
        source=DataSource.MEASURED.value,
        trust_level=TrustLevel.HIGH.value,
        system_version="5.0.0",
        policy_version="v5.0.1",
        units={"energy": "Wh", "accuracy": "ratio"},
        baseline="FP32-2025Q4",
        simulated=False,
    )
    fig2 = plot_accuracy_vs_energy(
        results, pareto_front,
        context=ctx,
        export_path="/tmp/accuracy_vs_energy.html",
    )
    print(f"Figure 2 subtitle: {ctx.to_subtitle()}")

    # --- Simulated data (visually flagged) ---
    simulated_results = [
        {"agent_id": "S1", "energy": 5.0, "accuracy": 0.90, "simulated": True},
        {"agent_id": "S2", "energy": 6.0, "accuracy": 0.85, "simulated": True},
    ]
    fig3 = plot_accuracy_vs_energy(simulated_results, [simulated_results[0]])
    if hasattr(fig3.layout, "title") and fig3.layout.title.text:
        assert "simulated" in fig3.layout.title.text.lower(), \
            "Simulated flag must be visible in the title"
        print("Simulated flag propagated to title ✓")
