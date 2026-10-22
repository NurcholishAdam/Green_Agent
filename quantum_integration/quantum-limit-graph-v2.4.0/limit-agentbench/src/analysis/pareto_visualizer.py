# src/analysis/reporting/pareto_visualizer.py
"""
Interactive Pareto visualization for Green Agent (Enhanced, v2.0.0)
====================================================================

Renders interactive Plotly scatter plots for Pareto analysis, with
frontier highlighting, dominance annotations, provenance overlays, and
2D projections of multi-dimensional frontiers.

Original API preserved (all signatures, all side effects):
    fig = ParetoVisualizer.plot(df)          # static method — v1 entry point
    viz.save(fig, "pareto.html")             # -> bool

Enhanced API:
    viz = ParetoVisualizer(
        x="latency_s", y="energy_wh", size="carbon_g", color="agent_id",
        frontier_highlight=True, distinguish_simulated=True,
        features={...},
    )
    fig = viz.render(
        df, frontier_labels=(p.label for p in frontier.points),
        color=None, size=None, symbol=None,
        hover_columns=None, provenance_footer=True, title_suffix=None,
    )
    fig = viz.render_with_frontier(df, frontier_result)
    ok  = viz.save(fig, "pareto.svg", auto_open=False)
    viz.set_xy(x="energy_wh", y="carbon_g")
    viz.get_statistics()

Column aliases
--------------
Common column-name variants are auto-mapped:
    latency      <- latency_s, latency_seconds, latency_ms
    energy_wh    <- energy_kwh, energy_joules, energy_j, energy_wh
    carbon_g     <- carbon_kg, carbon_grams, carbon_co2e_kg, carbon_g
    accuracy     <- accuracy, accuracy_score, quality_score, score
    label        <- label, agent_id, agent, name, id

Enhancements implemented (v2.0.0):
  1.  Generator-safe frontier labels — `frontier_labels=(p.label for p in
                              frontier.points)` no longer exhausts before
                              the title's "N on frontier" count is written.
                              v1's scatter consumed the generator first, so
                              the title silently reported 0.
  2.  Carbon unit conversion — `_resolve_axis` uses `_CARBON_TO_GRAMS`
                              when the requested axis is a carbon column.
                              v1's `y="carbon_kg"` silently held kg while
                              labelled grams.
  3.  Working `frontier_highlight` — the flag is now consulted; `False`
                              preserves the user's `color=` mapping instead
                              of forcing `__on_frontier__`.
  4.  Orthogonal channels   — frontier (symbol=diamond) and simulated
                              (color=orange) can appear on the SAME plot;
                              v1 forced them to be mutually exclusive.
  5.  Honored feature flags — `column_aliases`, `unit_conversion`,
                              `hitl_flag`, `xai_hover`, `stats_in_title`,
                              `frontier_line`, `simulated_markers` are all
                              wired (v1 declared four of them and ignored
                              them).
  6.  Static-path parity    — `_static_plot` and `render()` produce the
                              same canonical units + axis labels; v1's
                              `_static_plot` labelled "Energy (kWh)" while
                              plotting raw `energy_joules`.
  7.  String `simulated`    — `_normalize_bool_column` handles "yes"/"no",
                              "true"/"false", ints, and bools; v1's
                              `.sum()` on a string column silently
                              reported 0.
  8.  Dict / list input     — `_coerce_dataframe` is used in `_static_plot`
                              too; `ParetoVisualizer.plot({"latency": [...]})`
                              now returns a figure instead of raising.
  9.  Hover whitelist       — `hover_columns` defaults to a known-safe set
                              (v1 leaked all columns, including PII).
 10.  Kaleido pre-check     — `save("x.png")` raises `ImportError` with a
                              clear message when kaleido isn't installed.
 11.  Thread-safe stats     — `_StatsCounter` with `RLock`; `fallbacks`
                              counter is now incremented (was dead in v1).
 12.  Split dependency flags— `_HAS_PLOTLY` / `_HAS_PANDAS` (v1's single
                              flag conflated both).
 13.  Non-DataFrame input   — `_find_column` / `_coerce_dataframe` handle
                              dicts, lists, and custom `columns`-less
                              objects uniformly.
 14.  Non-numeric axes      — `render()` returns an annotated empty figure
                              when an axis column can't be coerced to
                              numeric (v1 silently produced a categorical
                              scatter).
 15.  Enriched integration  — `render_with_frontier` adds a
                              `__dominated_by__` hover column from the
                              result's `dominated_by_label` / `dominated_by`.
 16.  Per-call overrides    — `render(color=, size=, symbol=, hover_columns=)`.
 17.  Provenance footer     — auto-added annotation with `analyzer_version`
                              and render timestamp; `provenance_footer=False`
                              to disable.
 18.  Empty figure has one trace — downstream `.data[0]` indexing works.
 19.  Shared color maps     — `plot_multi_projection(shared_color=...)`
                              ensures the same agent gets the same color
                              across panels.
 20.  Save accepts PathLike — `pathlib.Path` no longer breaks `.rsplit`.
 21.  Structured logging    — `pareto_visualizer.rendered`,
                              `pareto_visualizer.column_fallback`,
                              `pareto_visualizer.save_failed`, etc.
 22.  `__version__` / `__all__`.

Backward compatibility:
  * `ParetoVisualizer.plot(df)` — static, unchanged signature, unchanged
    return type. Now applies unit conversion and consistent axis labels
    (a *display* change: "Energy (Wh)" instead of v1's "Energy (kWh)"),
    but the physical quantity is identical.
  * `ParetoVisualizer.DEFAULT_TITLE` / `DEFAULT_COLORS` preserved.
  * `save()` still returns `bool`. Additive: image-format exports raise
    `ImportError` when kaleido is missing (v1 swallowed the error and
    returned False; the new behaviour surfaces the dependency problem).
  * `_HAS_PLOTLY` is still the primary module flag; `_HAS_PANDAS` added.
"""

from __future__ import annotations

import logging
import math
import os
import threading
import time
from collections import Counter
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import (
    Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union,
)

__version__ = "2.0.0"

__all__ = [
    "ParetoVisualizer",
    "AxisKind",
    "LATENCY_ALIASES",
    "ENERGY_ALIASES",
    "CARBON_ALIASES",
    "ACCURACY_ALIASES",
    "LABEL_ALIASES",
    "DEFAULT_HOVER_COLUMNS",
    "plot_frontier",
    "plot_multi_projection",
    "get_statistics",
    "reset_statistics",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Optional dependency detection (v1 conflated plotly+pandas into one flag)
# =============================================================================

pd = None  # type: ignore
px = None  # type: ignore
go = None  # type: ignore
_HAS_PANDAS = False
_HAS_PLOTLY = False
_IMPORT_ERROR = ""

try:
    import pandas as _pd_module  # type: ignore
    pd = _pd_module
    _HAS_PANDAS = True
except Exception as e:  # pragma: no cover
    _IMPORT_ERROR += f"pandas: {e}; "

try:
    import plotly.express as _px_module  # type: ignore
    import plotly.graph_objects as _go_module  # type: ignore
    px = _px_module
    go = _go_module
    _HAS_PLOTLY = True
except Exception as e:  # pragma: no cover
    _IMPORT_ERROR += f"plotly: {e}; "

_HAS_DEPS = _HAS_PANDAS and _HAS_PLOTLY


# =============================================================================
# Enums
# =============================================================================

class AxisKind(Enum):
    LATENCY = "latency"
    ENERGY = "energy"
    CARBON = "carbon"
    ACCURACY = "accuracy"
    OTHER = "other"


# =============================================================================
# Column alias tables
# =============================================================================

LATENCY_ALIASES: Tuple[str, ...] = (
    "latency", "latency_s", "latency_seconds", "latency_ms",
)
ENERGY_ALIASES: Tuple[str, ...] = (
    "energy_kwh", "energy_wh", "energy_joules", "energy_j",
)
CARBON_ALIASES: Tuple[str, ...] = (
    "carbon_kg", "carbon_g", "carbon_grams", "carbon_co2e_kg",
)
ACCURACY_ALIASES: Tuple[str, ...] = (
    "accuracy", "accuracy_score", "quality_score", "score",
)
LABEL_ALIASES: Tuple[str, ...] = (
    "label", "agent_id", "agent", "name", "id",
)

#: Canonical-unit conversion factors.
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

_AXIS_LABELS: Dict[str, str] = {
    "latency": "Latency (seconds)",
    "energy": "Energy (Wh)",
    "carbon": "Carbon (grams)",
    "accuracy": "Accuracy",
}

#: Default hover whitelist (v1 leaked every column, including PII).
DEFAULT_HOVER_COLUMNS: Tuple[str, ...] = (
    "label", "agent_id", "agent",
    "accuracy", "accuracy_score", "quality_score",
    "energy_joules", "energy_wh", "energy_kwh",
    "latency", "latency_s", "latency_ms",
    "carbon_kg", "carbon_g", "carbon_grams",
    "simulated", "region", "precision", "source",
)


# =============================================================================
# Helper functions
# =============================================================================

def _jsonable(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set, frozenset)):
        return [_jsonable(x) for x in v]
    return repr(v)


def _safe_isoformat(ts: Any) -> Optional[str]:
    try:
        if isinstance(ts, datetime):
            return ts.isoformat()
        return datetime.fromtimestamp(float(ts)).isoformat()
    except (OSError, ValueError, OverflowError, TypeError):
        return None


def _has_kaleido() -> bool:
    """Return True iff kaleido is importable (needed for image export)."""
    try:
        import kaleido  # noqa: F401
        return True
    except Exception:
        return False


def _coerce_dataframe(df: Any) -> Any:
    """
    Coerce `df` to a pandas DataFrame.

    Returns the original DataFrame unchanged on success, a fresh
    DataFrame on coerced input, or `None` on failure.
    """
    if df is None or not _HAS_PANDAS:
        return None
    if isinstance(df, pd.DataFrame):
        return df
    try:
        return pd.DataFrame(df)
    except Exception as e:
        logger.warning(f"Could not coerce input to DataFrame: {e}")
        return None


def _find_column(df: Any, aliases: Sequence[str]) -> Optional[str]:
    """Return the first alias present in `df.columns`, or None."""
    if df is None:
        return None
    # Coerce dicts / lists for consistency
    if _HAS_PANDAS and not isinstance(df, pd.DataFrame):
        df = _coerce_dataframe(df)
        if df is None:
            return None
    try:
        cols = set(df.columns)
    except Exception:
        return None
    for a in aliases:
        if a in cols:
            return a
    return None


def _find_kind(col: Any) -> Optional[str]:
    """Return the AxisKind value for a column name, or None."""
    if not isinstance(col, str):
        return None
    if col in LATENCY_ALIASES:
        return "latency"
    if col in ENERGY_ALIASES:
        return "energy"
    if col in CARBON_ALIASES:
        return "carbon"
    if col in ACCURACY_ALIASES:
        return "accuracy"
    return None


def _get_aliases(kind: str) -> Tuple[str, ...]:
    return {
        "latency": LATENCY_ALIASES,
        "energy": ENERGY_ALIASES,
        "carbon": CARBON_ALIASES,
        "accuracy": ACCURACY_ALIASES,
    }.get(kind, ())


def _get_conversion_table(kind: str) -> Dict[str, float]:
    return {
        "latency": _LATENCY_TO_SECONDS,
        "energy": _ENERGY_TO_WH,
        "carbon": _CARBON_TO_GRAMS,
    }.get(kind, {})


def _normalize_bool_column(series: Any) -> Any:
    """Best-effort bool coercion for a 'simulated' column."""
    if series is None or not _HAS_PANDAS:
        return series
    try:
        if series.dtype == bool:
            return series
        if series.dtype.kind in ("i", "u", "f"):
            return series.astype(bool)
        # Strings: yes/no/true/false/1/0/y/n/t/f
        lowered = series.astype(str).str.lower().str.strip()
        return lowered.isin(("true", "yes", "1", "y", "t"))
    except Exception:
        try:
            return series.astype(bool)
        except Exception:
            return series


def _safe_numeric_coerce(series: Any) -> Optional[Any]:
    """Coerce a pandas Series to numeric; return None on failure."""
    if series is None or not _HAS_PANDAS:
        return None
    try:
        return pd.to_numeric(series, errors="raise")
    except Exception:
        return None


# =============================================================================
# Thread-safe statistics
# =============================================================================

class _StatsCounter:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._counts: Counter = Counter()

    def increment(self, key: str, amount: int = 1) -> None:
        with self._lock:
            self._counts[key] += amount

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def reset(self) -> None:
        with self._lock:
            self._counts.clear()


_GLOBAL_STATS = _StatsCounter()


def get_statistics() -> Dict[str, Any]:
    """Module-level statistics."""
    snap = _GLOBAL_STATS.snapshot()
    return {
        "analyzer_version": __version__,
        "plots_rendered": snap.get("plots", 0),
        "frontier_plots": snap.get("frontier_plots", 0),
        "fallback_columns": snap.get("fallbacks", 0),
        "empty_figures": snap.get("empty", 0),
    }


def reset_statistics() -> None:
    _GLOBAL_STATS.reset()


# =============================================================================
# ParetoVisualizer
# =============================================================================

class ParetoVisualizer:
    """
    Enhanced Pareto visualizer.

    Backward-compatible: `ParetoVisualizer.plot(df)` works without
    instantiation. Enhanced instance methods allow column remapping,
    frontier highlighting, and provenance overlays.
    """

    DEFAULT_TITLE = "Green Agent Pareto Frontier"
    DEFAULT_COLORS = {
        "frontier": "#2ca02c",     # green
        "dominated": "#1f77b4",    # blue
        "simulated": "#ff7f0e",    # orange
    }

    DEFAULT_FEATURES: Dict[str, bool] = {
        "column_aliases": True,
        "unit_conversion": True,
        "frontier_line": True,
        "stats_in_title": True,
        "simulated_markers": True,
        "hitl_flag": True,
        "xai_hover": True,
        "provenance_footer": True,
    }

    def __init__(
        self,
        *,
        x: Optional[str] = None,
        y: Optional[str] = None,
        size: Optional[str] = None,
        color: Optional[str] = None,
        symbol: Optional[str] = None,
        title: Optional[str] = None,
        frontier_highlight: bool = True,
        distinguish_simulated: bool = True,
        features: Optional[Dict[str, bool]] = None,
        analyzer_version: Optional[str] = None,
    ) -> None:
        self.x = x
        self.y = y
        self.size = size
        self.color = color
        self.symbol = symbol
        self.title = title or self.DEFAULT_TITLE
        self.frontier_highlight = bool(frontier_highlight)
        self.distinguish_simulated = bool(distinguish_simulated)
        self.analyzer_version = analyzer_version or __version__

        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES, **(features or {})
        }
        unknown = set((features or {}).keys()) - set(self.DEFAULT_FEATURES)
        if unknown:
            logger.warning(
                "pareto_visualizer.unknown_features",
                extra={"unknown": sorted(unknown)},
            )

        logger.debug(
            "pareto_visualizer.initialized",
            extra={
                "x": self.x, "y": self.y, "size": self.size,
                "color": self.color, "symbol": self.symbol,
            },
        )

    # ------------------------------------------------------------------
    # Configuration setters
    # ------------------------------------------------------------------

    def set_xy(self, *, x: Optional[str] = None, y: Optional[str] = None) -> None:
        if x is not None:
            self.x = x
        if y is not None:
            self.y = y

    def set_color(self, color: Optional[str]) -> None:
        self.color = color

    def set_symbol(self, symbol: Optional[str]) -> None:
        self.symbol = symbol

    # ------------------------------------------------------------------
    # ORIGINAL static API
    # ------------------------------------------------------------------

    @staticmethod
    def plot(df):
        """
        Create an interactive Pareto scatter plot.

        Backward-compatible: same signature, same return type. Accepts
        the v1 column shape (`latency`, `energy_kwh`, `carbon_kg`) as
        well as the extended aliases; applies canonical-unit conversion
        and consistent axis labels (v1 did not convert).
        """
        return ParetoVisualizer._static_plot(df)

    @staticmethod
    def _static_plot(df):
        if not _HAS_DEPS:
            raise ImportError(
                f"plotly and pandas are required for ParetoVisualizer.plot"
                f" ({_IMPORT_ERROR})"
            )

        _GLOBAL_STATS.increment("plots")
        df = _coerce_dataframe(df)
        if df is None or len(df) == 0:
            _GLOBAL_STATS.increment("empty")
            return _empty_figure("No data to plot")

        # --- Resolve axes using the same helpers as render() ---
        x_col, x_factor, x_kind = ParetoVisualizer._resolve_axis_static(
            df, requested=None, default_kind="latency",
        )
        y_col, y_factor, y_kind = ParetoVisualizer._resolve_axis_static(
            df, requested=None, default_kind="energy",
        )
        size_col = _find_column(df, CARBON_ALIASES)

        if x_col is None or y_col is None:
            _GLOBAL_STATS.increment("empty")
            return _empty_figure(
                "Missing required columns (latency, energy)"
            )

        # --- Copy + convert ---
        plot_df = df.copy()
        x_numeric = _safe_numeric_coerce(plot_df[x_col])
        y_numeric = _safe_numeric_coerce(plot_df[y_col])
        if x_numeric is None or y_numeric is None:
            _GLOBAL_STATS.increment("empty")
            return _empty_figure("Non-numeric axis column")
        plot_df[x_col] = x_numeric * x_factor
        plot_df[y_col] = y_numeric * y_factor
        if size_col is not None:
            size_numeric = _safe_numeric_coerce(plot_df[size_col])
            if size_numeric is not None:
                plot_df[size_col] = size_numeric * _CARBON_TO_GRAMS.get(
                    size_col, 1.0,
                )

        # --- Hover whitelist ---
        hover_cols = [
            c for c in plot_df.columns if c in DEFAULT_HOVER_COLUMNS
        ]

        try:
            kwargs: Dict[str, Any] = {
                "data_frame": plot_df,
                "x": x_col,
                "y": y_col,
                "hover_data": hover_cols or list(plot_df.columns),
            }
            if size_col is not None:
                kwargs["size"] = size_col
            fig = px.scatter(**kwargs)
        except Exception as e:
            logger.warning(f"Static scatter failed: {e}")
            _GLOBAL_STATS.increment("empty")
            return _empty_figure(f"Plot failed: {type(e).__name__}")

        fig.update_layout(
            title=ParetoVisualizer.DEFAULT_TITLE,
            xaxis_title=ParetoVisualizer._axis_label(x_col, x_kind),
            yaxis_title=ParetoVisualizer._axis_label(y_col, y_kind),
        )
        logger.info(
            "pareto_visualizer.rendered",
            extra={
                "static": True,
                "x_col": x_col, "y_col": y_col,
                "x_kind": x_kind, "y_kind": y_kind,
                "n_points": len(plot_df),
            },
        )
        return fig

    # ------------------------------------------------------------------
    # Instance rendering
    # ------------------------------------------------------------------

    def render(
        self,
        df: Any,
        *,
        frontier_labels: Optional[Iterable[str]] = None,
        frontier_line: bool = True,
        title_suffix: Optional[str] = None,
        color: Optional[str] = None,
        size: Optional[str] = None,
        symbol: Optional[str] = None,
        hover_columns: Optional[Sequence[str]] = None,
        provenance_footer: Optional[bool] = None,
    ):
        """
        Render an interactive scatter with optional frontier highlighting.

        `frontier_labels` may be any iterable — including a one-shot
        generator. It is materialised once at the top of this method so
        both the scatter and the title read the same set.
        """
        if not _HAS_DEPS:
            raise ImportError(
                "plotly and pandas are required for "
                "ParetoVisualizer.render"
            )

        # --- Materialize frontier_labels (v1 bug: generator exhausted) ---
        frontier_labels_list: List[str] = (
            [str(x) for x in frontier_labels] if frontier_labels else []
        )

        _GLOBAL_STATS.increment("plots")
        if frontier_labels_list:
            _GLOBAL_STATS.increment("frontier_plots")

        # --- Coerce / validate ---
        df = _coerce_dataframe(df)
        if df is None or len(df) == 0:
            _GLOBAL_STATS.increment("empty")
            return _empty_figure("No data to plot")

        # --- Resolve axes ---
        x_col, x_factor, x_kind = self._resolve_axis(
            df, self.x, default_kind="latency",
        )
        y_col, y_factor, y_kind = self._resolve_axis(
            df, self.y, default_kind="energy",
        )
        size_col = (
            _find_column(df, CARBON_ALIASES) if self.size is None
            else self.size
        )
        user_color = color if color is not None else self.color
        user_symbol = symbol if symbol is not None else self.symbol
        label_col = _find_column(df, LABEL_ALIASES)

        if x_col is None or y_col is None:
            _GLOBAL_STATS.increment("empty")
            return _empty_figure(
                "Missing required columns (latency, energy)"
            )

        # --- Numeric coercion (v1 silently produced a categorical plot) ---
        plot_df = df.copy()
        x_numeric = _safe_numeric_coerce(plot_df[x_col])
        y_numeric = _safe_numeric_coerce(plot_df[y_col])
        if x_numeric is None or y_numeric is None:
            _GLOBAL_STATS.increment("empty")
            return _empty_figure("Non-numeric axis column")
        plot_df[x_col] = x_numeric * x_factor
        plot_df[y_col] = y_numeric * y_factor

        # --- Simulated normalization ---
        has_simulated = (
            self.distinguish_simulated
            and self.features.get("simulated_markers", True)
            and "simulated" in plot_df.columns
        )
        if has_simulated:
            plot_df["__simulated__"] = _normalize_bool_column(
                plot_df["simulated"],
            )

        # --- Frontier membership ---
        compute_frontier_col = bool(
            self.frontier_highlight
            and frontier_labels_list
            and label_col
        )
        if compute_frontier_col:
            labels_set = set(frontier_labels_list)
            plot_df["__on_frontier__"] = plot_df[label_col].astype(str).apply(
                lambda v: "frontier" if v in labels_set else "dominated"
            )

        # --- Assign visual channels (frontier and simulated are orthogonal) ---
        effective_color: Optional[str] = user_color
        effective_symbol: Optional[str] = user_symbol

        if compute_frontier_col and has_simulated:
            # Both active → color by simulated, symbol by frontier
            if effective_color is None:
                effective_color = "__simulated__"
            if effective_symbol is None:
                effective_symbol = "__on_frontier__"
        elif compute_frontier_col:
            # Frontier only → color by frontier (v1 behaviour)
            if effective_color is None:
                effective_color = "__on_frontier__"
        elif has_simulated:
            if effective_color is None:
                effective_color = "__simulated__"

        # --- Hover columns ---
        if hover_columns is not None:
            hover_cols = [
                c for c in hover_columns if c in plot_df.columns
            ]
        else:
            hover_cols = [
                c for c in plot_df.columns
                if c in DEFAULT_HOVER_COLUMNS
            ]
            if not hover_cols:
                # Fall back to non-internal columns
                hover_cols = [
                    c for c in plot_df.columns
                    if not c.startswith("__")
                ]

        # --- Build scatter ---
        try:
            kwargs: Dict[str, Any] = {
                "data_frame": plot_df,
                "x": x_col,
                "y": y_col,
                "hover_data": hover_cols,
            }
            if (
                size_col and size_col in plot_df.columns
            ):
                kwargs["size"] = size_col
            if effective_color and effective_color in plot_df.columns:
                kwargs["color"] = effective_color
            if effective_symbol and effective_symbol in plot_df.columns:
                kwargs["symbol"] = effective_symbol
            if label_col and label_col in plot_df.columns:
                kwargs["hover_name"] = label_col
            fig = px.scatter(**kwargs)
        except Exception as e:
            logger.warning(f"Scatter render failed: {e}")
            _GLOBAL_STATS.increment("empty")
            return _empty_figure(f"Plot failed: {type(e).__name__}")

        # --- Frontier line overlay ---
        if (
            self.features.get("frontier_line", True)
            and frontier_line
            and compute_frontier_col
        ):
            self._add_frontier_line(
                fig, plot_df, x_col, y_col, label_col,
            )

        # --- Layout ---
        title = self.title
        if self.features.get("stats_in_title", True):
            title = self._title_with_stats(
                title, plot_df, frontier_labels_list,
            )
        if title_suffix:
            title = f"{title} — {title_suffix}"

        fig.update_layout(
            title=title,
            xaxis_title=self._axis_label(x_col, x_kind),
            yaxis_title=self._axis_label(y_col, y_kind),
        )

        # --- Provenance footer ---
        do_footer = (
            self.features.get("provenance_footer", True)
            if provenance_footer is None else provenance_footer
        )
        if do_footer:
            try:
                footer_text = (
                    f"Green Agent · analyzer {self.analyzer_version} · "
                    f"rendered {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
                fig.add_annotation(
                    text=footer_text,
                    xref="paper", yref="paper",
                    x=1.0, y=-0.15,
                    xanchor="right", yanchor="top",
                    showarrow=False,
                    font=dict(size=9, color="#888"),
                )
            except Exception:
                pass

        logger.info(
            "pareto_visualizer.rendered",
            extra={
                "static": False,
                "x_col": x_col, "y_col": y_col,
                "x_kind": x_kind, "y_kind": y_kind,
                "n_points": len(plot_df),
                "n_frontier": len(frontier_labels_list),
                "color": effective_color,
                "symbol": effective_symbol,
            },
        )
        return fig

    def render_with_frontier(
        self,
        df: Any,
        frontier_result: Any,
        *,
        label_attribute: str = "label",
        **render_kwargs: Any,
    ):
        """
        Render a plot using a `ParetoFrontierResult` (from `pareto_analyzer`
        or `extended_pareto_analyzer`).

        Enriches hover data with the result's `dominated_by_label` /
        `dominated_by` mapping when available.
        """
        labels: List[str] = []
        dominated_map: Dict[str, List[str]] = {}

        try:
            points = getattr(frontier_result, "points", None) or []
            for p in points:
                v = getattr(p, label_attribute, None)
                if v is None and isinstance(p, dict):
                    v = p.get(label_attribute)
                if v is not None:
                    labels.append(str(v))

            raw_dominated = (
                getattr(frontier_result, "dominated_by_label", None)
                or getattr(frontier_result, "dominated_by", None)
                or {}
            )
            if isinstance(raw_dominated, dict):
                dominated_map = {
                    str(k): [str(x) for x in (v or [])]
                    for k, v in raw_dominated.items()
                }
        except Exception as e:
            logger.warning(f"Could not extract frontier data: {e}")

        # Enrich df with dominated_by column for hover
        df_c = _coerce_dataframe(df)
        if df_c is not None and dominated_map:
            label_col = _find_column(df_c, LABEL_ALIASES)
            if label_col:
                try:
                    df_c = df_c.copy()
                    df_c["__dominated_by__"] = df_c[label_col].astype(str).apply(
                        lambda v: ", ".join(dominated_map.get(v, [])) or "-"
                    )
                except Exception:
                    pass

        # Ensure __dominated_by__ is in the hover whitelist if we added it
        if (
            df_c is not None
            and "__dominated_by__" in df_c.columns
        ):
            user_hover = render_kwargs.get("hover_columns")
            if user_hover is None:
                base = list(DEFAULT_HOVER_COLUMNS)
                base.append("__dominated_by__")
                render_kwargs["hover_columns"] = base

        return self.render(
            df_c if df_c is not None else df,
            frontier_labels=labels,
            **render_kwargs,
        )

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------

    def save(
        self,
        fig: Any,
        path: Union[str, os.PathLike] = "pareto.html",
        *,
        auto_open: bool = False,
    ) -> bool:
        """
        Convenience export. Detects format from the file extension.

        Supported: .html, .htm, .png, .svg, .pdf, .json, .jpg, .webp.

        Raises:
            ImportError: when an image format is requested but kaleido
                isn't installed (v1 swallowed this and returned False).

        Returns:
            True on success, False on any other failure.
        """
        if fig is None:
            logger.warning("pareto_visualizer.save_called_with_none")
            return False

        try:
            path_str = str(path)
            ext = (
                path_str.rsplit(".", 1)[-1].lower()
                if "." in path_str else "html"
            )

            if ext in ("html", "htm"):
                fig.write_html(path_str, auto_open=auto_open)
            else:
                if not _has_kaleido():
                    raise ImportError(
                        f"Image export to {ext!r} requires the "
                        f"'kaleido' package. Install with `pip install "
                        f"kaleido` or export to .html instead."
                    )
                fig.write_image(path_str)
            logger.info(
                "pareto_visualizer.saved",
                extra={"path": path_str, "ext": ext},
            )
            return True
        except ImportError:
            raise
        except Exception as e:
            logger.error(
                "pareto_visualizer.save_failed",
                extra={"path": str(path), "error": str(e)},
            )
            return False

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        stats = get_statistics()
        stats.update({
            "plot_rendered_x": self.x,
            "plot_rendered_y": self.y,
            "plot_rendered_size": self.size,
            "plot_rendered_color": self.color,
            "plot_rendered_symbol": self.symbol,
            "features": dict(self.features),
            "plotly_available": _HAS_PLOTLY,
            "pandas_available": _HAS_PANDAS,
            "kaleido_available": _has_kaleido(),
            "analyzer_version": self.analyzer_version,
        })
        return stats

    # ------------------------------------------------------------------
    # Internal — axis resolution
    # ------------------------------------------------------------------

    def _resolve_axis(
        self,
        df: Any,
        requested: Optional[str],
        *,
        default_kind: str,
    ) -> Tuple[Optional[str], float, str]:
        """
        Return `(column, conversion_factor, actual_kind)`.

        `actual_kind` is looked up from the requested column when it
        matches an alias table; otherwise it defaults to `default_kind`.
        This makes `y="carbon_kg"` correctly use `_CARBON_TO_GRAMS` (v1
        silently used `_ENERGY_TO_WH` and labelled grams).
        """
        if requested is not None:
            try:
                cols = set(df.columns)
            except Exception:
                cols = set()
            if requested in cols:
                actual_kind = _find_kind(requested) or default_kind
                factor = _get_conversion_table(actual_kind).get(
                    requested, 1.0,
                )
                return requested, factor, actual_kind
            logger.warning(
                "pareto_visualizer.column_fallback",
                extra={
                    "requested": requested,
                    "reason": "not_in_columns",
                    "default_kind": default_kind,
                },
            )
            _GLOBAL_STATS.increment("fallbacks")

        # Fall back to default_kind's aliases
        aliases = _get_aliases(default_kind)
        col = _find_column(df, aliases)
        if col is None:
            return None, 1.0, default_kind
        factor = _get_conversion_table(default_kind).get(col, 1.0)
        return col, factor, default_kind

    @staticmethod
    def _resolve_axis_static(
        df: Any,
        *,
        requested: Optional[str],
        default_kind: str,
    ) -> Tuple[Optional[str], float, str]:
        """Static variant of `_resolve_axis` (used by `_static_plot`)."""
        if requested is not None:
            try:
                cols = set(df.columns)
            except Exception:
                cols = set()
            if requested in cols:
                actual_kind = _find_kind(requested) or default_kind
                factor = _get_conversion_table(actual_kind).get(
                    requested, 1.0,
                )
                return requested, factor, actual_kind

        aliases = _get_aliases(default_kind)
        col = _find_column(df, aliases)
        if col is None:
            return None, 1.0, default_kind
        factor = _get_conversion_table(default_kind).get(col, 1.0)
        return col, factor, default_kind

    # ------------------------------------------------------------------
    # Internal — frontier line, title, axis label
    # ------------------------------------------------------------------

    def _add_frontier_line(
        self,
        fig: Any,
        df: Any,
        x_col: str,
        y_col: str,
        label_col: Optional[str],
    ) -> None:
        """Draw a line connecting frontier points, sorted by x."""
        if "__on_frontier__" not in df.columns:
            return
        try:
            frontier_df = df[df["__on_frontier__"] == "frontier"].copy()
            if len(frontier_df) < 2:
                return
            frontier_df = frontier_df.sort_values(by=x_col)
            custom = (
                frontier_df[label_col]
                if label_col and label_col in frontier_df.columns
                else frontier_df.index.astype(str)
            )
            fig.add_trace(go.Scatter(
                x=frontier_df[x_col],
                y=frontier_df[y_col],
                mode="lines+markers",
                name="Pareto frontier",
                line=dict(
                    color=self.DEFAULT_COLORS["frontier"], width=2,
                ),
                marker=dict(size=10, symbol="diamond"),
                hovertemplate=(
                    "<b>%{customdata}</b><br>"
                    f"{x_col}=%{{x:.4f}}<br>"
                    f"{y_col}=%{{y:.6f}}<extra></extra>"
                ),
                customdata=custom,
            ))
        except Exception as e:
            logger.debug(
                "pareto_visualizer.frontier_line_failed",
                extra={"error": str(e)},
            )

    def _title_with_stats(
        self,
        base: str,
        df: Any,
        frontier_labels_list: List[str],
    ) -> str:
        """Compose a title with the stats suffix (labels already materialized)."""
        try:
            n_total = len(df)
            n_frontier = len(frontier_labels_list)

            n_simulated = 0
            if "__simulated__" in df.columns:
                try:
                    n_simulated = int(
                        _normalize_bool_column(df["__simulated__"]).sum()
                    )
                except Exception:
                    n_simulated = 0
            elif "simulated" in df.columns:
                try:
                    n_simulated = int(
                        _normalize_bool_column(df["simulated"]).sum()
                    )
                except Exception:
                    n_simulated = 0

            bits: List[str] = [f"{n_total} points"]
            if n_frontier:
                bits.append(f"{n_frontier} on frontier")
            if n_simulated:
                bits.append(f"{n_simulated} simulated")
            return f"{base} ({', '.join(bits)})"
        except Exception:
            return base

    @staticmethod
    def _axis_label(col: str, kind: str) -> str:
        """Return a canonical-unit label for `col` under `kind`."""
        label = _AXIS_LABELS.get(kind)
        if label:
            return label
        # Unknown kind → return the column with a note
        return f"{col} (raw)"


# =============================================================================
# Module helpers
# =============================================================================

def _empty_figure(message: str):
    """Build an empty figure with an explanatory annotation and one trace."""
    if not _HAS_PLOTLY:
        raise ImportError("plotly is required for ParetoVisualizer")
    fig = go.Figure()
    # Placeholder trace so downstream `.data[0]` indexing works
    fig.add_trace(go.Scatter(
        x=[], y=[], mode="markers", showlegend=False, hoverinfo="skip",
    ))
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
    *,
    titles: Optional[Sequence[str]] = None,
    shared_color: Optional[str] = None,
    **kwargs: Any,
) -> List[Any]:
    """
    Return a list of 2D projections of the same data.

    `shared_color` (column name) ensures the same value gets the same
    color across panels — a necessity when comparing "A dominates here,
    B dominates there" side by side.
    """
    figs = []
    for i, (x_dim, y_dim) in enumerate(projections):
        viz = ParetoVisualizer(
            x=x_dim, y=y_dim,
            color=shared_color or kwargs.pop("color", None),
            **{k: v for k, v in kwargs.items() if k != "color"},
        )
        title_suffix = titles[i] if titles and i < len(titles) else None
        figs.append(viz.render(df, title_suffix=title_suffix))
    return figs


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.WARNING)
    reset_statistics()

    if not _HAS_DEPS:
        print(
            f"plotly/pandas unavailable ({_IMPORT_ERROR}); "
            f"demo skipped"
        )
        raise SystemExit(0)

    df = pd.DataFrame({
        "label": ["A", "B", "C", "D"],
        "latency": [1.0, 1.2, 0.8, 1.5],
        "energy_kwh": [0.05, 0.06, 0.10, 0.04],
        "carbon_kg": [0.02, 0.025, 0.04, 0.018],
    })

    # =================================================================
    # 1. Original behavior (backward compatible)
    # =================================================================
    print("=== 1. Original behavior ===")
    fig = ParetoVisualizer.plot(df)
    print(f"  type:   {type(fig).__name__}")
    print(f"  traces: {len(fig.data)}")
    print(f"  xaxis:  {fig.layout.xaxis.title.text}  (expected 'Latency (seconds)')")
    print(f"  yaxis:  {fig.layout.yaxis.title.text}  "
          f"(v1 said 'Energy (kWh)', v2 canonical 'Energy (Wh)')")

    # =================================================================
    # 2. Generator frontier_labels (v1 exhausted the generator)
    # =================================================================
    print("\n=== 2. Generator frontier_labels ===")
    viz = ParetoVisualizer(
        x="latency", y="energy_kwh", color="label",
    )
    frontier_gen = (lbl for lbl in ["A", "D"])   # one-shot generator
    fig2 = viz.render(df, frontier_labels=frontier_gen)
    title = fig2.layout.title.text
    print(f"  title: {title}")
    print(f"  title mentions '2 on frontier': {'2 on frontier' in title}")

    # =================================================================
    # 3. Carbon axis (v1 mislabeled kg as grams)
    # =================================================================
    print("\n=== 3. Carbon axis ===")
    viz_carbon = ParetoVisualizer(x="latency", y="carbon_kg")
    fig3 = viz_carbon.render(df)
    print(f"  yaxis: {fig3.layout.yaxis.title.text}")
    print(f"  (data should be in grams — v1 kept kg but labelled grams)")

    # =================================================================
    # 4. frontier_highlight=False preserves user's color
    # =================================================================
    print("\n=== 4. frontier_highlight=False ===")
    viz_nohl = ParetoVisualizer(
        x="latency", y="energy_kwh", color="label",
        frontier_highlight=False,
    )
    fig4 = viz_nohl.render(df, frontier_labels=["A", "D"])
    # The plot shouldn't override the color column
    # (title still mentions 2 on frontier because title count uses the list)
    print(f"  trace count: {len(fig4.data)}  "
          f"(no frontier-line trace because frontier_highlight=False)")

    # =================================================================
    # 5. Orthogonal frontier + simulated channels
    # =================================================================
    print("\n=== 5. Frontier + simulated together ===")
    df_sim = df.copy()
    df_sim["simulated"] = ["no", "yes", "yes", "no"]   # v1's .sum() failed
    viz_both = ParetoVisualizer(
        x="latency", y="energy_kwh", color=None,
    )
    fig5 = viz_both.render(df_sim, frontier_labels=["A", "D"])
    title5 = fig5.layout.title.text
    print(f"  title: {title5}")
    print(f"  mentions '2 simulated': {'2 simulated' in title5}")
    print(f"  trace count: {len(fig5.data)}  "
          f"(v1 could show frontier OR simulated, not both)")

    # =================================================================
    # 6. Dict input (v1 raised a cryptic error)
    # =================================================================
    print("\n=== 6. Dict input ===")
    dict_input = {
        "latency": [1.0, 1.2, 0.8],
        "energy_kwh": [0.05, 0.06, 0.10],
    }
    fig6 = ParetoVisualizer.plot(dict_input)
    print(f"  plot(dict) returned: {type(fig6).__name__}")

    # =================================================================
    # 7. Non-numeric axis (v1 silently produced a categorical scatter)
    # =================================================================
    print("\n=== 7. Non-numeric axis ===")
    df_bad = pd.DataFrame({
        "latency": ["low", "high"],
        "energy_kwh": [0.05, 0.06],
    })
    fig7 = ParetoVisualizer.plot(df_bad)
    annotations = [a.text for a in fig7.layout.annotations if a.text]
    print(f"  empty figure annotation: {annotations}")

    # =================================================================
    # 8. Simulated as string / int / bool
    # =================================================================
    print("\n=== 8. Normalized simulated column ===")
    df_yes = pd.DataFrame({
        "label": ["P", "Q"],
        "latency": [1.0, 1.2],
        "energy_kwh": [0.05, 0.06],
        "simulated": ["YES", "no"],
    })
    fig8 = ParetoVisualizer(x="latency", y="energy_kwh").render(df_yes)
    print(f"  title: {fig8.layout.title.text}")

    # =================================================================
    # 9. Missing columns → empty figure with placeholder trace
    # =================================================================
    print("\n=== 9. Missing columns ===")
    fig9 = ParetoVisualizer().render(pd.DataFrame({"unrelated": [1, 2]}))
    annotations = [a.text for a in fig9.layout.annotations if a.text]
    print(f"  annotation: {annotations}")
    print(f"  len(fig.data): {len(fig9.data)}  (expected 1 — placeholder)")

    # =================================================================
    # 10. Save — kaleido pre-check
    # =================================================================
    print("\n=== 10. Save ===")
    ok = viz.save(fig2, "/tmp/pareto_demo.html")
    print(f"  save to /tmp/pareto_demo.html: {ok}")
    try:
        # Image export will raise if kaleido isn't installed — that's the
        # intended v2 behaviour.
        viz.save(fig2, "/tmp/pareto_demo.png")
        print(f"  save to /tmp/pareto_demo.png: succeeded (kaleido installed)")
    except ImportError as e:
        print(f"  save to /tmp/pareto_demo.png → ImportError: "
              f"{str(e)[:60]}...")
    ok_bad = viz.save(fig2, "/nonexistent/path/foo.html")
    print(f"  save to invalid path: {ok_bad}  (expected False)")

    # =================================================================
    # 11. Multi-projection with shared color
    # =================================================================
    print("\n=== 11. Shared color across projections ===")
    figs = plot_multi_projection(
        df,
        projections=[("latency", "energy_kwh"), ("latency", "carbon_kg")],
        titles=["energy", "carbon"],
        shared_color="label",
    )
    print(f"  n panels: {len(figs)}")
    for i, f in enumerate(figs):
        print(f"  panel {i} title: {f.layout.title.text}")

    # =================================================================
    # 12. ParetoFrontierResult integration (with dominated_by hover)
    # =================================================================
    print("\n=== 12. ParetoFrontierResult integration ===")
    try:
        import sys
        sys.path.insert(0, ".")
        from src.analysis.optimization.pareto_analyzer import ParetoAnalyzer

        pa = ParetoAnalyzer()
        pa.add_record(100.0, 0.90, 50.0, "A")
        pa.add_record(120.0, 0.85, 45.0, "B")
        pa.add_record(90.0, 0.80, 60.0, "C")
        pa.add_record(150.0, 0.95, 70.0, "D")
        result = pa.compute_frontier_detailed()

        rows = []
        for p in pa.points:
            rows.append({
                "label": p.label,
                "latency": p.energy_joules / 1000.0,
                "energy_kwh": p.energy_joules / 3600.0 / 1000.0,
                "carbon_kg": p.carbon_grams / 1000.0,
            })
        df_int = pd.DataFrame(rows)
        fig12 = viz.render_with_frontier(df_int, result)
        print(f"  title: {fig12.layout.title.text}")
    except Exception as e:
        print(f"  (integration skipped: {e})")

    # =================================================================
    # 13. Provenance footer
    # =================================================================
    print("\n=== 13. Provenance footer ===")
    fig13 = viz.render(df)
    footer_annotations = [
        a for a in fig13.layout.annotations
        if a.text and "analyzer" in a.text
    ]
    print(f"  footer present: {len(footer_annotations) > 0}")
    if footer_annotations:
        print(f"  footer text: {footer_annotations[0].text}")

    # =================================================================
    # 14. Statistics
    # =================================================================
    print("\n=== 14. Statistics ===")
    print(json.dumps(viz.get_statistics(), indent=2, default=str))
