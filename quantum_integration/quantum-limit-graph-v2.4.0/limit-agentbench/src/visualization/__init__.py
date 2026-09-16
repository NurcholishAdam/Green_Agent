# src/visualization/__init__.py

"""
Visualization package for Green Agent (Enhanced)
=================================================

Interactive evidence-exploration layer. Turns validated data from
`analysis` and `reporting` into charts, graphs, timelines, and
inspection views. Performs no policy execution and modifies no live
decisions.

Public API
----------
Contracts:
    ChartContext, DataSource, TrustLevel, VisualTheme, Palette

Trade-offs:
    ParetoExplorer       — consolidated Pareto visualization
    TradeoffKind         — the supported trade-off pairs

Decision cards:
    XAIDecisionCard      — the inspectable decision view
    render_decision_card — convenience renderer

Renderers:
    RendererBackend, get_renderer, save_figure, export_html

Backward compatibility:
    ParetoPlotter        — original class (from pareto_plotter)
    plot_accuracy_vs_energy       — original function
    plot_latency_vs_energy        — original function
    plot_carbon_vs_energy         — original function
    plot_accuracy_vs_carbon       — original function
"""

from __future__ import annotations

# --- Contracts --------------------------------------------------------------
from .contracts.chart_schema import (
    ChartContext,
    DataSource,
    TrustLevel,
    TradeoffKind,
)
from .contracts.visual_theme import (
    VisualTheme,
    Palette,
    DEFAULT_THEME,
)
from .contracts.accessibility import (
    check_contrast,
    colorblind_safe_palette,
)

# --- Trade-off visuals ------------------------------------------------------
from .tradeoffs.pareto import (
    ParetoExplorer,
)
from .tradeoffs.energy_carbon import (
    plot_energy_carbon_tradeoff,
)
from .tradeoffs.quality_latency import (
    plot_quality_latency_tradeoff,
)

# --- Decision cards ---------------------------------------------------------
from .assurance.xai_decision_card import (
    XAIDecisionCard,
    render_decision_card,
)

# --- Renderers --------------------------------------------------------------
from .renderers.base import (
    RendererBackend,
    get_renderer,
)
from .renderers.export import (
    save_figure,
    export_html,
)

# --- Backward-compatible re-exports -----------------------------------------
from .pareto_plotter import ParetoPlotter  # legacy class, retained

__all__ = [
    # Contracts
    "ChartContext",
    "DataSource",
    "TrustLevel",
    "TradeoffKind",
    "VisualTheme",
    "Palette",
    "DEFAULT_THEME",
    "check_contrast",
    "colorblind_safe_palette",
    # Trade-offs
    "ParetoExplorer",
    "plot_energy_carbon_tradeoff",
    "plot_quality_latency_tradeoff",
    # Decision cards
    "XAIDecisionCard",
    "render_decision_card",
    # Renderers
    "RendererBackend",
    "get_renderer",
    "save_figure",
    "export_html",
    # Legacy
    "ParetoPlotter",
]

__version__ = "5.0.0"
