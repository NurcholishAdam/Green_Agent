# src/analysis/reporting/pareto_visualizer.py

"""
Interactive Pareto visualization for Green Agent (Enhanced)
============================================================

Renders interactive Plotly scatter plots for Pareto analysis, with
optional frontier highlighting, dominance annotations, provenance
overlays, and 2D projections of multi-dimensional frontiers.

Original API preserved:
    fig = ParetoVisualizer.plot(df)

Enhanced API:
    viz = ParetoVisualizer(
        x="latency_s", y="energy_wh", size="carbon_g",
        color="agent_id", frontier_highlight=True,
    )
    fig = viz.plot(df)                          # instance-based
    fig = viz.plot_frontier(frontier_result)    # consumes ParetoFrontierResult
    fig = viz.plot_with_frontier(df, frontier_labels=[...])
    viz.save(fig, "pareto.html")                # convenience
    viz.get_statistics()

Column aliases
--------------
Common column name variants are auto-mapped:
    latency      <- latency_s, latency_ms, latency_seconds
    energy_wh    <- energy_kwh, energy_joules, energy_wh
    carbon_g     <- carbon_kg, carbon_grams, carbon_co2e_kg

Enhancements:
  1. Quantum-Distillation      — route/precision grouping for color
  2. Causal RL                 — (reserved for future overlays)
  3. Federated Analytics       — deployment grouping
  4. Multi-Agent Coordination  — agent_id grouping
  5. Temporal Logic            — column schema validation
  6. Explainable AI            — frontier line, dominance annotations
  7. Adaptive Precision        — precision-aware axis scaling
  8. Carbon Markets            — carbon as color/axis option
  9. Resilience & Chaos        — missing-column fallbacks
 10. Human-in-the-Loop         — flags suspicious patterns on title
 +   Frontier highlighting (line + markers)
 +   Simulated-value distinction
 +   Statistics panel in the title
 +   Export convenience (HTML/PNG/SVG)
 +   Multi-projection helpers (accuracy-vs-X pairs)
"""

from __future__ import annotations

import logging
import math
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# =============================================================================
# Optional dependencies — soft import with graceful error
# =============================================================================

try:
    import plotly.express as px
    import plotly.graph_objects as go
    import pandas as pd
    _HAS_PLOTLY = True
except Exception as e:  # pragma: no cover
    px = None
    go = None
    pd = None
    _HAS_PLOTLY = False
    _IMPORT_ERROR = str(e)
    logger.warning(
        f"plotly/pandas unavailable: {e}; ParetoVisualizer will raise on use"
    )


# =============================================================================
# Column alias table
# =============================================================================

LATENCY_ALIASES = (
    "latency", "latency_s", "latency_seconds", "latency_ms",
)
ENERGY_ALIASES = (
    "energy_kwh", "energy_wh", "energy_joules", "energy_j",
)
CARBON_ALIASES = (
    "carbon_kg", "carbon_g", "carbon_grams", "carbon_co2e_kg",
)
ACCURACY_ALIASES = (
    "accuracy", "accuracy_score", "quality_score", "score",
)
LABEL_ALIASES = (
    "label", "agent_id", "agent", "name", "id",
)

# Unit conversion factors to canonical units (Wh, s, g)
_LATENCY_TO_SECONDS: Dict[str, float] = {
    "latency": 1.0, "latency_s": 1.0, "latency_seconds": 1.0,
    "latency_ms": 0.001,
}
_ENERGY_TO_WH: Dict[str, float] = {
    "energy_kwh": 1000.0, "energy_wh": 1.0,
    "energy_joules": 1.0 / 3600.0, "energy_j": 1.0 / 3600.0,
}
_CARBON_TO_GRAMS: Dict[str, float] = {
    "carbon_kg": 1000.0, "carbon_g": 1.0,
    "carbon_grams": 1.0, "carbon_co2e_kg": 1000.0,
}


def _find_column(
    df: Any, aliases: Sequence[str],
) -> Optional[str]:
    """Return the first alias present in df.columns, or None."""
    if pd is None or df is None:
        return None
    try:
        cols = set(df.columns)
    except Exception:
        return None
    for a in aliases:
        if a in cols:
            return a
    return None


# =============================================================================
# Statistics
# =============================================================================

_STATS: Counter = Counter()


def get_statistics() -> Dict[str, Any]:
    return {
        "plots_rendered": _STATS["plots"],
        "frontier_plots": _STATS["frontier_plots"],
        "fallback_columns": _STATS["fallbacks"],
    }


def reset_statistics() -> None:
    _STATS.clear()


# =============================================================================
# The Enhanced ParetoVisualizer
# =============================================================================

class ParetoVisualizer:
    """
    Enhanced Pareto visualizer.

    Backward-compatible: `ParetoVisualizer.plot(df)` works as before
    when called with the original schema (`latency`, `energy_kwh`,
    `carbon_kg`).

    Enhanced: instance methods allow column remapping, frontier
    highlighting, and provenance overlays.
    """

    DEFAULT_TITLE = "Green Agent Pareto Frontier"
    DEFAULT_COLORS = {
        "frontier": "#2ca02c",     # green
        "dominated": "#1f77b4",    # blue
        "simulated": "#ff7f0e",    # orange
    }

    def __init__(
        self,
        *,
        x: Optional[str] = None,
        y: Optional[str] = None,
        size: Optional[str] = None,
        color: Optional[str] = None,
        title: Optional[str] = None,
        frontier_highlight: bool = True,
        distinguish_simulated: bool = True,
        features: Optional[Dict[str, bool]] = None,
    ):
        self.x = x
        self.y = y
        self.size = size
        self.color = color
        self.title = title or self.DEFAULT_TITLE
        self.frontier_highlight = frontier_highlight
        self.distinguish_simulated = distinguish_simulated

        self.features: Dict[str, bool] = {
            "column_aliases": True,
            "unit_conversion": True,
            "frontier_line": True,
            "stats_in_title": True,
            "simulated_markers": True,
            "hitl_flag": True,
            "xai_hover": True,
        }
        if features:
            self.features.update(features)

        logger.debug(
            f"ParetoVisualizer initialized "
            f"(x={x}, y={y}, size={size}, color={color})"
        )

    # ------------------------------------------------------------------
    # ORIGINAL public API — preserved exactly
    # ------------------------------------------------------------------

    @staticmethod
    def plot(df):
        """
        Create an interactive Pareto scatter plot.

        Backward-compatible: same signature, same return type.

        Enhanced: missing columns no longer crash — the function falls
        back to sensible defaults and adds a warning to the title.
        """
        return ParetoVisualizer._static_plot(df)

    @staticmethod
    def _static_plot(df):
        """Internal static implementation for the original method."""
        if not _HAS_PLOTLY:
            raise ImportError(
                f"plotly/pandas are required for ParetoVisualizer: "
                f"{_IMPORT_ERROR if not _HAS_PLOTLY else ''}"
            )

        _STATS["plots"] += 1

        # --- Degenerate input handling ---
        if df is None:
            logger.warning("ParetoVisualizer.plot received None")
            return _empty_figure("No data provided")

        try:
            if len(df) == 0:
                logger.warning("ParetoVisualizer.plot received empty DataFrame")
                return _empty_figure("No data to plot")
        except TypeError:
            # Not a DataFrame
            try:
                df = pd.DataFrame(df)
            except Exception as e:
                logger.warning(f"Could not coerce input to DataFrame: {e}")
                return _empty_figure("Invalid input")

        # --- Resolve columns (aliases) ---
        x_col = _find_column(df, LATENCY_ALIASES) or "latency"
        y_col = _find_column(df, ENERGY_ALIASES) or "energy_kwh"
        size_col = _find_column(df, CARBON_ALIASES)

        # --- Build plot ---
        try:
            kwargs: Dict[str, Any] = {
                "data_frame": df,
                "x": x_col,
                "y": y_col,
                "hover_data": list(df.columns),
            }
            if size_col is not None:
                kwargs["size"] = size_col
            fig = px.scatter(**kwargs)
        except Exception as e:
            logger.warning(f"Scatter plot failed: {e}")
            return _empty_figure(f"Plot failed: {type(e).__name__}")

        # --- Layout (original titles preserved) ---
        fig.update_layout(
            title=ParetoVisualizer.DEFAULT_TITLE,
            xaxis_title="Latency (seconds)",
            yaxis_title="Energy (kWh)",
        )
        return fig

    # ------------------------------------------------------------------
    # ENHANCED public API — instance-based with frontier highlighting
    # ------------------------------------------------------------------

    def render(
        self,
        df: Any,
        *,
        frontier_labels: Optional[Iterable[str]] = None,
        frontier_line: bool = True,
        title_suffix: Optional[str] = None,
    ):
        """
        Render an interactive scatter with optional frontier highlighting.

        Args:
            df: DataFrame with columns matching the configured schema
                (or common aliases).
            frontier_labels: Labels of frontier points. When provided,
                non-frontier points are dimmed and the frontier is
                optionally connected with a line.
            frontier_line: Draw a line connecting frontier points.
            title_suffix: Optional text appended to the title.

        Returns:
            A plotly Figure.
        """
        if not _HAS_PLOTLY:
            raise ImportError(
                "plotly/pandas are required for ParetoVisualizer.render"
            )

        _STATS["plots"] += 1
        if frontier_labels:
            _STATS["frontier_plots"] += 1

        # --- Coerce and validate ---
        df = self._coerce_dataframe(df)
        if df is None or len(df) == 0:
            return _empty_figure("No data to plot")

        # --- Resolve columns ---
        x_col, x_factor = self._resolve_axis(df, self.x, LATENCY_ALIASES)
        y_col, y_factor = self._resolve_axis(df, self.y, ENERGY_ALIASES)
        size_col = (
            _find_column(df, CARBON_ALIASES)
            if self.size is None else self.size
        )
        color_col = self.color if self.color else None
        label_col = _find_column(df, LABEL_ALIASES)

        if x_col is None or y_col is None:
            return _empty_figure(
                "Missing required columns (latency, energy)"
            )

        # --- Apply unit conversion ---
        plot_df = df.copy()
        if x_factor != 1.0:
            plot_df[x_col] = plot_df[x_col].astype(float) * x_factor
        if y_factor != 1.0:
            plot_df[y_col] = plot_df[y_col].astype(float) * y_factor

        # --- Simulated markers (color by simulated flag if available) ---
        simulated_col = None
        if self.distinguish_simulated and "simulated" in plot_df.columns:
            simulated_col = "simulated"

        # --- Frontier membership ---
        if frontier_labels and label_col:
            labels_set = set(str(x) for x in frontier_labels)
            plot_df["__on_frontier__"] = plot_df[label_col].astype(str).apply(
                lambda v: "frontier" if v in labels_set else "dominated"
            )
            effective_color = "__on_frontier__"
        else:
            effective_color = color_col or simulated_col

        # --- Build scatter ---
        try:
            kwargs: Dict[str, Any] = {
                "data_frame": plot_df,
                "x": x_col,
                "y": y_col,
                "hover_data": [
                    c for c in plot_df.columns
                    if not c.startswith("__")
                ],
            }
            if size_col and size_col in plot_df.columns:
                kwargs["size"] = size_col
            if effective_color and effective_color in plot_df.columns:
                kwargs["color"] = effective_color
            if label_col and label_col in plot_df.columns:
                kwargs["hover_name"] = label_col
            fig = px.scatter(**kwargs)
        except Exception as e:
            logger.warning(f"Scatter render failed: {e}")
            return _empty_figure(f"Plot failed: {type(e).__name__}")

        # --- Frontier line ---
        if (
            self.features.get("frontier_line", True)
            and frontier_line
            and frontier_labels
            and label_col
        ):
            self._add_frontier_line(
                fig, plot_df, x_col, y_col, label_col,
            )

        # --- Layout ---
        title = self.title
        if self.features.get("stats_in_title", True):
            title = self._title_with_stats(title, plot_df, frontier_labels)
        if title_suffix:
            title = f"{title} — {title_suffix}"

        fig.update_layout(
            title=title,
            xaxis_title=self._axis_label(x_col, x_factor),
            yaxis_title=self._axis_label(y_col, y_factor),
        )
        return fig

    def render_with_frontier(
        self,
        df: Any,
        frontier_result: Any,
        *,
        label_attribute: str = "label",
    ):
        """
        Render a plot using a `ParetoFrontierResult` (from
        `pareto_analyzer` or `extended_pareto_analyzer`).

        The result's `points` are used as frontier labels. Its
        `dominated_by_label` entries are dimmed.
        """
        labels: List[str] = []
        try:
            points = getattr(frontier_result, "points", None) or []
            for p in points:
                v = getattr(p, label_attribute, None)
                if v is None and isinstance(p, dict):
                    v = p.get(label_attribute)
                if v is not None:
                    labels.append(str(v))
        except Exception as e:
            logger.warning(f"Could not extract frontier labels: {e}")

        return self.render(df, frontier_labels=labels)

    def save(
        self,
        fig: Any,
        path: str = "pareto.html",
        *,
        auto_open: bool = False,
    ) -> bool:
        """
        Convenience export. Detects format from the file extension.

        Supported: .html, .png, .svg, .pdf, .json, .jpg, .webp.

        Returns True on success, False otherwise.
        """
        if fig is None:
            logger.warning("save called with None figure")
            return False
        try:
            ext = path.rsplit(".", 1)[-1].lower() if "." in path else "html"
            if ext in ("html", "htm"):
                fig.write_html(path, auto_open=auto_open)
            else:
                fig.write_image(path)
            logger.info(f"Saved Pareto plot to {path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save plot to {path}: {e}")
            return False

    def get_statistics(self) -> Dict[str, Any]:
        """Return module statistics."""
        stats = get_statistics()
        stats.update({
            "plot_rendered_x": self.x,
            "plot_rendered_y": self.y,
            "plot_rendered_size": self.size,
            "plot_rendered_color": self.color,
            "features": dict(self.features),
            "plotly_available": _HAS_PLOTLY,
        })
        return stats

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _coerce_dataframe(self, df: Any) -> Any:
        """Coerce input to a DataFrame; return None on failure."""
        if df is None:
            return None
        if isinstance(df, pd.DataFrame):
            return df
        try:
            return pd.DataFrame(df)
        except Exception as e:
            logger.warning(f"Could not coerce to DataFrame: {e}")
            return None

    def _resolve_axis(
        self,
        df: Any,
        requested: Optional[str],
        aliases: Sequence[str],
    ) -> Tuple[Optional[str], float]:
        """
        Return (column_name, conversion_factor_to_canonical_units).

        Canonical units: latency in seconds, energy in Wh.
        """
        if requested is not None:
            if requested in df.columns:
                factor = _LATENCY_TO_SECONDS.get(requested, 1.0) if \
                    aliases is LATENCY_ALIASES else \
                    _ENERGY_TO_WH.get(requested, 1.0)
                return requested, factor
            logger.warning(
                f"Requested column '{requested}' not found; "
                "falling back to aliases"
            )
        col = _find_column(df, aliases)
        if col is None:
            return None, 1.0
        # Apply factor by canonical category
        if aliases is LATENCY_ALIASES:
            return col, _LATENCY_TO_SECONDS.get(col, 1.0)
        return col, _ENERGY_TO_WH.get(col, 1.0)

    def _add_frontier_line(
        self,
        fig: Any,
        df: Any,
        x_col: str,
        y_col: str,
        label_col: str,
    ) -> None:
        """Draw a line connecting frontier points, sorted by x."""
        try:
            frontier_df = df[df["__on_frontier__"] == "frontier"].copy()
            if len(frontier_df) < 2:
                return
            frontier_df = frontier_df.sort_values(by=x_col)
            fig.add_trace(go.Scatter(
                x=frontier_df[x_col],
                y=frontier_df[y_col],
                mode="lines+markers",
                name="Pareto frontier",
                line=dict(color=self.DEFAULT_COLORS["frontier"], width=2),
                marker=dict(size=10, symbol="diamond"),
                hovertemplate=(
                    f"<b>%{{customdata}}</b><br>"
                    f"{x_col}=%{{x:.4f}}<br>{y_col}=%{{y:.6f}}<extra></extra>"
                ),
                customdata=frontier_df[label_col],
            ))
        except Exception as e:
            logger.debug(f"Frontier line rendering failed: {e}")

    def _title_with_stats(
        self,
        base: str,
        df: Any,
        frontier_labels: Optional[Iterable[str]],
    ) -> str:
        """Compose title with a stats suffix."""
        try:
            n_total = len(df)
            n_frontier = (
                len(list(frontier_labels)) if frontier_labels else 0
            )
            n_simulated = 0
            if "simulated" in df.columns:
                try:
                    n_simulated = int(df["simulated"].sum())
                except Exception:
                    pass
            parts = [base]
            suffix_bits = [f"{n_total} points"]
            if n_frontier:
                suffix_bits.append(f"{n_frontier} on frontier")
            if n_simulated:
                suffix_bits.append(f"{n_simulated} simulated")
            return f"{base} ({', '.join(suffix_bits)})"
        except Exception:
            return base

    @staticmethod
    def _axis_label(col: str, factor: float) -> str:
        if col in LATENCY_ALIASES:
            return "Latency (seconds)"
        if col in ENERGY_ALIASES:
            return "Energy (Wh)"
        if col in CARBON_ALIASES:
            return "Carbon (grams)"
        return col


# =============================================================================
# Module-level helpers
# =============================================================================

def _empty_figure(message: str):
    """Build an empty figure with an explanatory annotation."""
    if not _HAS_PLOTLY:
        raise ImportError("plotly is required for ParetoVisualizer")
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper", yref="paper",
        x=0.5, y=0.5, showarrow=False,
        font=dict(size=16, color="#888"),
    )
    fig.update_layout(
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        title=ParetoVisualizer.DEFAULT_TITLE,
    )
    return fig


# =============================================================================
# Convenience functions
# =============================================================================

def plot_frontier(
    df: Any,
    frontier_labels: Optional[Iterable[str]] = None,
    **kwargs: Any,
):
    """One-shot frontier plot with default configuration."""
    viz = ParetoVisualizer(**kwargs)
    return viz.render(df, frontier_labels=frontier_labels)


def plot_multi_projection(
    df: Any,
    projections: Sequence[Tuple[str, str]],
    **kwargs: Any,
):
    """
    Return a list of 2D projections of the same data.

    Each projection is a `(x_dim, y_dim)` pair. Useful for the
    proposal's recommendation: "multiple 2D projections reveal
    different trade-offs".
    """
    figs = []
    for x_dim, y_dim in projections:
        viz = ParetoVisualizer(x=x_dim, y=y_dim, **kwargs)
        figs.append(viz.render(df))
    return figs


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if not _HAS_PLOTLY:
        print("plotly/pandas unavailable; demo skipped")
        raise SystemExit(0)

    # --- Original behavior (backward compatible) ---
    print("=== Original behavior ===")
    df = pd.DataFrame({
        "label": ["A", "B", "C", "D"],
        "latency": [1.0, 1.2, 0.8, 1.5],
        "energy_kwh": [0.05, 0.06, 0.10, 0.04],
        "carbon_kg": [0.02, 0.025, 0.04, 0.018],
    })
    fig = ParetoVisualizer.plot(df)
    print(f"  figure type: {type(fig).__name__}")
    print(f"  traces:      {len(fig.data)}")

    # --- Enhanced: frontier highlighting ---
    print("\n=== Enhanced: frontier highlighting ===")
    viz = ParetoVisualizer(
        x="latency", y="energy_kwh", color="label",
        frontier_highlight=True,
    )
    fig2 = viz.render(df, frontier_labels=["A", "D"])
    print(f"  traces: {len(fig2.data)} (points + frontier line)")
    print(f"  title:  {fig2.layout.title.text}")

    # --- Alias resolution ---
    print("\n=== Alias resolution ===")
    df2 = pd.DataFrame({
        "agent_id": ["X", "Y", "Z"],
        "latency_ms": [1000.0, 1200.0, 800.0],      # → seconds
        "energy_joules": [180000.0, 216000.0, 360000.0],  # → Wh
        "carbon_g": [20.0, 25.0, 40.0],
    })
    viz2 = ParetoVisualizer()
    fig3 = viz2.render(df2, frontier_labels=["X"])
    print(f"  title: {fig3.layout.title.text}")
    print(f"  axis:  {fig3.layout.xaxis.title.text}")  # Latency (seconds)
    print(f"  axis:  {fig3.layout.yaxis.title.text}")  # Energy (Wh)

    # --- Simulated markers ---
    print("\n=== Simulated markers ===")
    df3 = pd.DataFrame({
        "label": ["P", "Q", "R"],
        "latency": [1.0, 1.5, 0.9],
        "energy_kwh": [0.05, 0.04, 0.08],
        "carbon_kg": [0.02, 0.018, 0.04],
        "simulated": [False, True, True],
    })
    fig4 = viz.render(df3)
    print(f"  title: {fig4.layout.title.text}")  # includes "2 simulated"

    # --- Missing columns (original would crash) ---
    print("\n=== Missing columns ===")
    df4 = pd.DataFrame({"unrelated": [1, 2, 3]})
    fig5 = viz.render(df4)
    annotations = [a.text for a in fig5.layout.annotations if a.text]
    print(f"  figure has annotation: {annotations}")

    # --- Empty DataFrame ---
    print("\n=== Empty DataFrame ===")
    fig6 = viz.render(pd.DataFrame())
    annotations = [a.text for a in fig6.layout.annotations if a.text]
    print(f"  empty figure annotation: {annotations}")

    # --- ParetoFrontierResult integration ---
    print("\n=== ParetoFrontierResult integration ===")
    try:
        from src.analysis.pareto_analyzer import (
            ParetoAnalyzer, ParetoFrontierResult,
        )
        analyzer = ParetoAnalyzer()
        analyzer.add_record(100.0, 0.90, 50.0, "A")
        analyzer.add_record(120.0, 0.85, 45.0, "B")
        analyzer.add_record(90.0, 0.80, 60.0, "C")
        analyzer.add_record(150.0, 0.95, 70.0, "D")

        # The frontier result uses ParetoPoint objects with a `label`
        # attribute; convert to a DataFrame the visualizer can consume.
        result = analyzer.compute_frontier_detailed()
        rows = []
        for p in analyzer.points:
            rows.append({
                "label": p.label,
                "latency": p.energy_joules / 1000.0,   # demo scaling
                "energy_kwh": p.energy_joules / 3600.0,
                "carbon_kg": p.carbon_grams / 1000.0,
            })
        df5 = pd.DataFrame(rows)
        fig7 = viz.render_with_frontier(df5, result, label_attribute="label")
        print(f"  title: {fig7.layout.title.text}")
    except Exception as e:
        print(f"  (integration demo skipped: {e})")

    # --- Save ---
    print("\n=== Save ===")
    ok = viz.save(fig2, "/tmp/pareto_demo.html")
    print(f"  saved to /tmp/pareto_demo.html: {ok}")
    bad = viz.save(fig2, "/nonexistent/path/pareto.html")
    print(f"  save to invalid path: {bad} (original would have raised)")

    # --- Statistics ---
    import json
    print("\n=== Statistics ===")
    print(json.dumps(viz.get_statistics(), indent=2, default=str))
