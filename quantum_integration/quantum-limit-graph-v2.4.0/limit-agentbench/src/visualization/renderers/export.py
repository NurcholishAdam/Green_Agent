# src/visualization/renderers/export.py

"""
Figure export helpers.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def save_figure(fig, path: str, *, scale: float = 2.0) -> bool:
    """
    Save a Plotly or matplotlib figure to disk.

    Format is inferred from the extension. Returns True on success.
    """
    if fig is None:
        logger.warning("save_figure received None")
        return False
    try:
        ext = path.rsplit(".", 1)[-1].lower() if "." in path else "html"
        if hasattr(fig, "write_html") and ext in ("html", "htm"):
            fig.write_html(path)
            return True
        if hasattr(fig, "write_image"):
            fig.write_image(path, scale=scale)
            return True
        if hasattr(fig, "savefig"):
            fig.savefig(path, dpi=150, bbox_inches="tight")
            return True
    except Exception as e:
        logger.error(f"save_figure failed for {path}: {e}")
        return False
    logger.warning(f"Unknown figure type for {path}")
    return False


def export_html(fig, path: str, *, include_plotlyjs: str = "cdn") -> bool:
    """Export a Plotly figure as a standalone HTML file."""
    if fig is None or not hasattr(fig, "write_html"):
        logger.warning("export_html requires a Plotly figure")
        return False
    try:
        fig.write_html(path, include_plotlyjs=include_plotlyjs)
        return True
    except Exception as e:
        logger.error(f"export_html failed: {e}")
        return False
