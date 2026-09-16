"""
Visual theme — palette, typography, and layout constants.

Every visual uses this theme so plots are consistent across the folder.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class Palette:
    """A named set of colors used across the folder."""
    frontier: str = "#2ca02c"        # green — the "best" data
    dominated: str = "#b0b0b0"       # gray — the rejected data
    simulated: str = "#ff7f0e"       # orange — the synthetic data
    measured: str = "#1f77b4"        # blue — the real data
    warning: str = "#d62728"         # red — a problem
    critical: str = "#8c0000"        # dark red — a severe problem
    background: str = "#ffffff"
    grid: str = "#e0e0e0"
    text: str = "#202020"
    accent: str = "#9467bd"


@dataclass
class VisualTheme:
    """Palette + typography + layout."""
    palette: Palette = field(default_factory=Palette)
    font_family: str = "system-ui, -apple-system, sans-serif"
    font_size_title: int = 16
    font_size_axis: int = 12
    font_size_footnote: int = 10
    figure_width_px: int = 900
    figure_height_px: int = 600
    marker_size_dominated: int = 8
    marker_size_frontier: int = 12
    line_width: float = 2.0

    def color_for(self, source: str, is_frontier: bool = False) -> str:
        """Pick the right color based on data source and frontier status."""
        if source == "simulated":
            return self.palette.simulated
        if is_frontier:
            return self.palette.frontier
        if source == "measured":
            return self.palette.measured
        return self.palette.dominated


DEFAULT_THEME = VisualTheme()
