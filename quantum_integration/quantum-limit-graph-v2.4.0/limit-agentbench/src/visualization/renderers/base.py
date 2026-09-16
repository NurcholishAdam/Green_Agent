# src/visualization/renderers/base.py

"""
Renderer backend selection.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Optional


class RendererBackend(Enum):
    PLOTLY = "plotly"
    MATPLOTLIB = "matplotlib"


def get_renderer(backend: str = "plotly") -> RendererBackend:
    """Return the requested backend, falling back to matplotlib."""
    if backend.lower() == "plotly":
        try:
            import plotly  # noqa: F401
            return RendererBackend.PLOTLY
        except ImportError:
            return RendererBackend.MATPLOTLIB
    return RendererBackend.MATPLOTLIB
