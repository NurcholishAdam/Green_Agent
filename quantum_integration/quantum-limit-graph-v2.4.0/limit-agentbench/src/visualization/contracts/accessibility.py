"""
Accessibility helpers — contrast checks and colorblind-safe palettes.
"""

from __future__ import annotations

from typing import Dict, List, Tuple


def _hex_to_rgb(h: str) -> Tuple[int, int, int]:
    h = h.lstrip("#")
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))  # type: ignore


def _relative_luminance(rgb: Tuple[int, int, int]) -> float:
    def channel(c: int) -> float:
        c_norm = c / 255.0
        return c_norm / 12.92 if c_norm <= 0.03928 else (
            ((c_norm + 0.055) / 1.055) ** 2.4
        )
    r, g, b = (channel(c) for c in rgb)
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def check_contrast(fg: str, bg: str) -> float:
    """
    Return the WCAG contrast ratio between two hex colors.

    Values ≥ 4.5 pass AA for normal text; ≥ 3.0 pass AA for large text.
    """
    l1 = _relative_luminance(_hex_to_rgb(fg))
    l2 = _relative_luminance(_hex_to_rgb(bg))
    lighter, darker = max(l1, l2), min(l1, l2)
    return (lighter + 0.05) / (darker + 0.05)


# A colorblind-safe palette based on Okabe–Ito
COLORBLIND_SAFE: Dict[str, str] = {
    "frontier": "#009E73",       # bluish green
    "dominated": "#999999",      # gray
    "simulated": "#E69F00",      # orange
    "measured": "#0072B2",       # blue
    "warning": "#D55E00",        # vermillion
    "critical": "#CC79A7",       # reddish purple
    "accent": "#56B4E9",         # sky blue
}


def colorblind_safe_palette() -> Dict[str, str]:
    """Return a colorblind-safe color map."""
    return dict(COLORBLIND_SAFE)
