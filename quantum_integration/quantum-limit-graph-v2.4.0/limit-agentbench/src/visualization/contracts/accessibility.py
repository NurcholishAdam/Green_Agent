# src/visualization/contracts/accessibility.py

"""
Accessibility helpers — contrast checks and colorblind-safe palettes.

This module is a **contract** for the visualization layer:

- :data:`WCAG_AA_NORMAL` / :data:`WCAG_AA_LARGE` /
  :data:`WCAG_AAA_NORMAL` / :data:`WCAG_AAA_LARGE` — the WCAG 2.1
  thresholds, exported so callers can write their own checks against the
  same numbers this module uses.
- :class:`WcagResult` — the shape returned by :func:`check_wcag`.
- :data:`PaletteRole` — the closed set of roles the palette defines.
- :data:`COLORBLIND_SAFE` / :data:`PALETTE_DESCRIPTIONS` — read-only
  mappings.

Two tools:

- :func:`check_contrast` — WCAG 2.1 contrast ratio between two hex
  colors (raw float).
- :func:`check_wcag` / :func:`passes_aa` / :func:`passes_aaa` — WCAG
  classification. **These round to two decimal places before comparing**,
  matching every accessibility auditing tool (axe, Lighthouse, Pa11y).

Colorblind-safe is **not** contrast-safe
----------------------------------------
CVD-distinguishability and WCAG contrast are orthogonal properties. The
Okabe–Ito palette is optimized for the first. Measured against ``#ffffff``
using the WCAG 2.1 formula, only :data:`COLORBLIND_SAFE`\\ ``["measured"]``
(``#0072B2``, ratio ≈ 5.18) meets AA for normal text. Two of the seven
fail even AA for large text:

========================  =========  ==========  ==============  ===============
role                      hex        vs #fff     AA-large (3.0)  AA-normal (4.5)
========================  =========  ==========  ==============  ===============
``frontier``              #009E73    ≈ 3.42      ✅              ❌
``dominated``             #999999    ≈ 2.85      ❌              ❌
``simulated``             #E69F00    ≈ 2.25      ❌              ❌
``measured``              #0072B2    ≈ 5.18      ✅              ✅
``warning``               #D55E00    ≈ 3.87      ✅              ❌
``critical``              #CC79A7    ≈ 3.06      ✅              ❌
``accent``                #56B4E9    ≈ 2.31      ❌              ❌
========================  =========  ==========  ==============  ===============

Use these colors as **fills**, **lines**, or **large-text glyphs**; for
small body text on a white background, pick a darker variant or check
against your actual surface color with
:func:`check_palette_contrast`.

Implementation notes
--------------------
- **Luma coefficients.** ``0.2126 / 0.7152 / 0.0722`` are the ITU-R
  BT.709 coefficients for the sRGB primaries. These are the coefficients
  WCAG 2.1 specifies. Rec. 601 coefficients (``0.299 / 0.587 / 0.114``)
  would give visibly different luminance values for saturated colors —
  don't substitute them.
- **Linearization threshold.** WCAG 2.1 §relative luminance specifies
  ``0.03928`` as the threshold between the linear and gamma regions. The
  mathematically exact value from the sRGB EOTF inverse is ``0.04045``;
  the difference is below ``1e-4`` for any channel and every auditing
  tool uses ``0.03928``. It is retained here for reproducibility.
- **Symmetry.** The contrast ratio is symmetric: ``(a, b)`` and
  ``(b, a)`` always return the same value, and the result is in
  ``[1.0, 21.0]``.

Accepted color formats
----------------------
``#rgb``, ``#rrggbb``, ``rgb``, ``rrggbb`` — a single leading ``#`` is
optional, case is insensitive, alpha forms (``#rrggbbaa``) are rejected.
Everything else raises :class:`AccessibilityError`.

Compatibility
-------------
- ``check_contrast(fg, bg)`` — same signature, same return type.
  **Behaviour change**: malformed inputs now raise
  :class:`AccessibilityError` (subclass of ``ValueError``) instead of
  ``ValueError`` / ``AttributeError`` / silently returning a wrong
  answer. Callers catching ``ValueError`` keep working.
- ``colorblind_safe_palette()`` — returns the same values in a fresh
  mutable ``dict``.
- ``COLORBLIND_SAFE`` is now read-only; anything that mutated it
  (``COLORBLIND_SAFE["frontier"] = "#ff0000"``) raises ``TypeError``.
  Use ``colorblind_safe_palette()`` if a mutable copy is needed.

Requires Python 3.8+.
"""

from __future__ import annotations

import logging
import math
import re
from types import MappingProxyType
from typing import (
    Any, Dict, Final, List, Literal, Mapping, Tuple, TypedDict,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class AccessibilityError(ValueError):
    """Raised for invalid color inputs or accessibility-check failures."""


# --------------------------------------------------------------------------- #
# WCAG thresholds (WCAG 2.1 §1.4.3, §1.4.6)
# --------------------------------------------------------------------------- #
WCAG_AA_NORMAL: Final[float] = 4.5
WCAG_AA_LARGE: Final[float] = 3.0
WCAG_AAA_NORMAL: Final[float] = 7.0
WCAG_AAA_LARGE: Final[float] = 4.5


# --------------------------------------------------------------------------- #
# Palette contract
# --------------------------------------------------------------------------- #
PaletteRole = Literal[
    "frontier",
    "dominated",
    "simulated",
    "measured",
    "warning",
    "critical",
    "accent",
]


class WcagResult(TypedDict):
    """Return shape of :func:`check_wcag`."""

    ratio: float
    aa_normal: bool
    aa_large: bool
    aaa_normal: bool
    aaa_large: bool


# --------------------------------------------------------------------------- #
# Hex parsing
# --------------------------------------------------------------------------- #
_HEX_RE = re.compile(r"^#?([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$")


def _hex_to_rgb(value: Any) -> Tuple[int, int, int]:
    """
    Parse a CSS-style hex color into ``(r, g, b)`` with each channel in
    ``[0, 255]``.

    Accepts ``#rgb``, ``#rrggbb``, ``rgb``, and ``rrggbb`` (single
    optional leading ``#``, case-insensitive). Rejects everything else,
    including alpha forms (``#rrggbbaa``) and multiple leading ``#``,
    with :class:`AccessibilityError`.
    """
    if not isinstance(value, str):
        raise AccessibilityError(
            f"color must be a string, got {type(value).__name__}."
        )
    match = _HEX_RE.match(value)
    if match is None:
        raise AccessibilityError(
            f"invalid hex color {value!r}; expected #rgb, #rrggbb, rgb, "
            f"or rrggbb."
        )
    digits = match.group(1)
    if len(digits) == 3:
        # Expand #rgb shorthand: each character duplicated.
        digits = digits[0] * 2 + digits[1] * 2 + digits[2] * 2
    # Unpacked into a fixed-length tuple so the return annotation is
    # exact; a generator + tuple() would be Tuple[int, ...].
    return (
        int(digits[0:2], 16),
        int(digits[2:4], 16),
        int(digits[4:6], 16),
    )


# --------------------------------------------------------------------------- #
# Luminance and contrast
# --------------------------------------------------------------------------- #
def _relative_luminance_rgb(rgb: Tuple[int, int, int]) -> float:
    """
    WCAG 2.1 relative luminance of an sRGB color in ``[0, 1]``.

    Linearization uses the WCAG 2.1 threshold (``0.03928``); see the
    module docstring for the rationale.
    """

    def channel(c: int) -> float:
        c_norm = c / 255.0
        if c_norm <= 0.03928:
            return c_norm / 12.92
        return ((c_norm + 0.055) / 1.055) ** 2.4

    r, g, b = (channel(c) for c in rgb)
    # BT.709 luma coefficients — the correct coefficients for sRGB.
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def relative_luminance(color: str) -> float:
    """
    Return the WCAG 2.1 relative luminance of ``color``.

    ``color`` is any hex form accepted by :func:`check_contrast`.
    """
    return _relative_luminance_rgb(_hex_to_rgb(color))


def check_contrast(fg: str, bg: str) -> float:
    """
    Return the raw WCAG contrast ratio between two hex colors.

    The ratio is **symmetric** — ``check_contrast(a, b)`` equals
    ``check_contrast(b, a)`` for all ``a``, ``b``. The result is in
    ``[1.0, 21.0]``: ``1.0`` for identical colors, ``21.0`` for pure
    black on pure white.

    This is the **raw** ratio; no rounding is applied. Use
    :func:`passes_aa`, :func:`passes_aaa`, or :func:`check_wcag` for a
    WCAG classification, which round to two decimal places before
    comparing (matching accessibility auditing tools).

    Raises
    ------
    AccessibilityError
        If either color is malformed.
    """
    l1 = _relative_luminance_rgb(_hex_to_rgb(fg))
    l2 = _relative_luminance_rgb(_hex_to_rgb(bg))
    lighter, darker = max(l1, l2), min(l1, l2)
    return (lighter + 0.05) / (darker + 0.05)


# --------------------------------------------------------------------------- #
# WCAG classification
# --------------------------------------------------------------------------- #
def _classify(ratio: float, threshold: float) -> bool:
    """
    Compare a raw contrast ratio against a WCAG threshold.

    Rounds to two decimal places before comparing, matching every
    accessibility auditing tool (axe, Lighthouse, Pa11y). The ``1e-9``
    epsilon absorbs float-representation noise at the exact boundary
    without affecting any real classification decision.
    """
    return round(ratio + 1e-9, 2) >= threshold


def check_wcag(fg: str, bg: str) -> WcagResult:
    """
    Return the full WCAG 2.1 classification of ``fg`` on ``bg``.

    The returned :class:`WcagResult` carries the raw ratio (unrounded)
    and four booleans computed with WCAG-auditor rounding.
    """
    ratio = check_contrast(fg, bg)
    return {
        "ratio": ratio,
        "aa_normal": _classify(ratio, WCAG_AA_NORMAL),
        "aa_large": _classify(ratio, WCAG_AA_LARGE),
        "aaa_normal": _classify(ratio, WCAG_AAA_NORMAL),
        "aaa_large": _classify(ratio, WCAG_AAA_LARGE),
    }


def passes_aa(fg: str, bg: str, *, large: bool = False) -> bool:
    """
    Return True when ``fg`` on ``bg`` meets WCAG AA.

    ``large=True`` uses the AA large-text threshold (3.0); the default
    uses the AA normal-text threshold (4.5).
    """
    threshold = WCAG_AA_LARGE if large else WCAG_AA_NORMAL
    return _classify(check_contrast(fg, bg), threshold)


def passes_aaa(fg: str, bg: str, *, large: bool = False) -> bool:
    """Return True when ``fg`` on ``bg`` meets WCAG AAA."""
    threshold = WCAG_AAA_LARGE if large else WCAG_AAA_NORMAL
    return _classify(check_contrast(fg, bg), threshold)


# --------------------------------------------------------------------------- #
# Colorblind-safe palette
# --------------------------------------------------------------------------- #
# Okabe–Ito-derived, with two deliberate departures documented below:
#   - `dominated` uses neutral gray (#999999) instead of Okabe–Ito's
#     black — gray reads as "deprioritized" in a chart legend.
#   - Okabe–Ito's yellow (#F0E442) is omitted — it fails WCAG AA on
#     white (contrast ≈ 1.2), which makes it unusable as a text color.
_COLORBLIND_SAFE: Dict[PaletteRole, str] = {
    "frontier":  "#009E73",       # bluish green
    "dominated": "#999999",       # gray (custom; not Okabe–Ito)
    "simulated": "#E69F00",       # orange
    "measured":  "#0072B2",       # blue
    "warning":   "#D55E00",       # vermillion
    "critical":  "#CC79A7",       # reddish purple
    "accent":    "#56B4E9",       # sky blue
}
COLORBLIND_SAFE: Mapping[PaletteRole, str] = MappingProxyType(
    _COLORBLIND_SAFE
)


_PALETTE_DESCRIPTIONS: Dict[PaletteRole, str] = {
    "frontier":  "Points on the Pareto frontier (best trade-offs).",
    "dominated": "Points strictly worse than at least one other.",
    "simulated": "Simulated or predicted results.",
    "measured":  "Measured / observed results.",
    "warning":   "Threshold crossed; attention needed.",
    "critical":  "Hard limit exceeded; action required.",
    "accent":    "Supplementary / decorative colour.",
}
PALETTE_DESCRIPTIONS: Mapping[PaletteRole, str] = MappingProxyType(
    _PALETTE_DESCRIPTIONS
)


def colorblind_safe_palette() -> Dict[PaletteRole, str]:
    """
    Return a fresh, mutable copy of :data:`COLORBLIND_SAFE`.

    The returned dict can be freely modified; the module-level
    :data:`COLORBLIND_SAFE` is unaffected.
    """
    return dict(_COLORBLIND_SAFE)


def check_palette_contrast(background: str) -> Dict[PaletteRole, float]:
    """
    Return the **raw** WCAG contrast ratio of every palette color against
    ``background``.

    Useful for verifying that a chart palette is readable against the
    dashboard's actual surface color — which is rarely pure white.
    Classification for each role is a one-liner on top:
    ``passes_aa(COLORBLIND_SAFE[role], background)``.

    Raises
    ------
    AccessibilityError
        If ``background`` is malformed.
    """
    # Validate the background once, so the loop can't fail mid-way.
    _hex_to_rgb(background)
    return {
        role: check_contrast(color, background)
        for role, color in _COLORBLIND_SAFE.items()
    }


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "AccessibilityError",
    "COLORBLIND_SAFE",
    "PALETTE_DESCRIPTIONS",
    "PaletteRole",
    "WCAG_AA_LARGE",
    "WCAG_AA_NORMAL",
    "WCAG_AAA_LARGE",
    "WCAG_AAA_NORMAL",
    "WcagResult",
    "check_contrast",
    "check_palette_contrast",
    "check_wcag",
    "colorblind_safe_palette",
    "passes_aa",
    "passes_aaa",
    "relative_luminance",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m visualization.contracts.accessibility
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    passed = 0
    failed = 0

    def _check(name: str, cond: bool) -> None:
        global passed, failed
        if cond:
            passed += 1
            print(f"  ✅ {name}")
        else:
            failed += 1
            print(f"  ❌ {name}")

    def _approx(a: float, b: float, tol: float = 1e-3) -> bool:
        return math.isfinite(a) and math.isfinite(b) and abs(a - b) <= tol

    # ---- 1. Known contrast ratios -------------------------------- #
    print("known contrast ratios:")
    _check(
        "black on white == 21.0",
        _approx(check_contrast("#000000", "#ffffff"), 21.0),
    )
    _check(
        "black on white via shorthand",
        _approx(check_contrast("#000", "#fff"), 21.0),
    )
    _check(
        "white on black == 21.0",
        _approx(check_contrast("#ffffff", "#000000"), 21.0),
    )
    _check(
        "identical colors == 1.0",
        _approx(check_contrast("#123456", "#123456"), 1.0),
    )
    _check(
        "#009E73 on white ≈ 3.42",
        _approx(check_contrast("#009E73", "#ffffff"), 3.42, tol=0.05),
    )
    _check(
        "#0072B2 on white ≈ 5.18",
        _approx(check_contrast("#0072B2", "#ffffff"), 5.18, tol=0.05),
    )
    _check(
        "contrast is symmetric",
        _approx(
            check_contrast("#009E73", "#ffffff"),
            check_contrast("#ffffff", "#009E73"),
        ),
    )

    # ---- 2. Input formats accepted ------------------------------- #
    print("input formats:")
    _check(
        "#fff == #ffffff",
        _approx(
            check_contrast("#fff", "#000"),
            check_contrast("#ffffff", "#000000"),
        ),
    )
    _check(
        "#-less accepted",
        _approx(
            check_contrast("fff", "000"),
            check_contrast("#ffffff", "#000000"),
        ),
    )
    _check(
        "mixed case accepted",
        _approx(
            check_contrast("#FFF", "#000"),
            check_contrast("#ffffff", "#000000"),
        ),
    )

    # ---- 3. Malformed inputs rejected ---------------------------- #
    print("malformed inputs:")
    bad_inputs = (
        "#fffff",    # 5 chars
        "#fffffff",  # 7 chars
        "#-1-2-3",   # signed
        "#+1+2+3",   # signed
        "#GGGGGG",   # non-hex
        "#",         # empty digits
        "",          # empty string
        "##fff",     # double #
        "  #fff",    # leading whitespace
        "#fff ",     # trailing whitespace
        "#ffffff00", # alpha form
        "red",       # named color
        None,        # not a string
        42,          # not a string
        ("#", "fff"),# not a string
    )
    for bad in bad_inputs:
        try:
            check_contrast(bad, "#ffffff")  # type: ignore[arg-type]
            _check(f"reject {bad!r}", False)
        except AccessibilityError:
            _check(f"reject {bad!r}", True)
        except Exception:
            # A non-AccessibilityError exception is a bug in the module.
            _check(f"reject {bad!r} with AccessibilityError", False)

    # ---- 4. WCAG classification (raw ratio in result) ------------ #
    print("check_wcag:")
    r = check_wcag("#000", "#fff")
    _check("black on white is WcagResult", isinstance(r, dict))
    _check("aa_normal", r["aa_normal"] is True)
    _check("aa_large", r["aa_large"] is True)
    _check("aaa_normal", r["aaa_normal"] is True)
    _check("aaa_large", r["aaa_large"] is True)
    _check("ratio is raw 21.0", _approx(r["ratio"], 21.0))

    r = check_wcag("#777", "#fff")  # raw ≈ 4.478 — fails AA normal
    _check("#777 aa_normal is False", r["aa_normal"] is False)
    _check("#777 aa_large is True", r["aa_large"] is True)

    # ---- 5. WCAG-auditor rounding at the boundary ---------------- #
    print("auditor rounding at the boundary:")
    _check(
        "_classify(4.499999, 4.5) is True (rounds up)",
        _classify(4.499999, 4.5) is True,
    )
    _check(
        "_classify(4.494, 4.5) is False (rounds to 4.49)",
        _classify(4.494, 4.5) is False,
    )
    _check(
        "_classify(3.0, 3.0) is True (exact)",
        _classify(3.0, 3.0) is True,
    )
    _check(
        "_classify(2.994, 3.0) is False",
        _classify(2.994, 3.0) is False,
    )

    # ---- 6. passes_aa / passes_aaa ------------------------------- #
    print("passes_*:")
    _check(
        "passes_aa(#000, #fff) is True",
        passes_aa("#000", "#fff") is True,
    )
    _check(
        "passes_aa(#777, #fff) is False",
        passes_aa("#777", "#fff") is False,
    )
    _check(
        "passes_aa(#777, #fff, large=True) is True",
        passes_aa("#777", "#fff", large=True) is True,
    )
    _check(
        "passes_aaa(#000, #fff) is True",
        passes_aaa("#000", "#fff") is True,
    )
    _check(
        "passes_aaa(#009E73, #fff) is False",
        passes_aaa("#009E73", "#fff") is False,
    )

    # ---- 7. relative_luminance ---------------------------------- #
    print("relative_luminance:")
    _check(
        "black is 0.0",
        _approx(relative_luminance("#000"), 0.0, tol=1e-6),
    )
    _check(
        "white is 1.0",
        _approx(relative_luminance("#fff"), 1.0, tol=1e-6),
    )

    # ---- 8. Palette --------------------------------------------- #
    print("palette:")
    _check(
        "colorblind_safe_palette returns a dict",
        isinstance(colorblind_safe_palette(), dict),
    )
    _check(
        "palette has 7 entries",
        len(colorblind_safe_palette()) == 7,
    )
    _check(
        "PALETTE_DESCRIPTIONS has the same keys",
        set(PALETTE_DESCRIPTIONS) == set(COLORBLIND_SAFE),
    )

    try:
        COLORBLIND_SAFE["frontier"] = "#ff0000"  # type: ignore[index]
        _check("COLORBLIND_SAFE is frozen", False)
    except TypeError:
        _check("COLORBLIND_SAFE is frozen", True)

    try:
        PALETTE_DESCRIPTIONS["frontier"] = "hacked"  # type: ignore[index]
        _check("PALETTE_DESCRIPTIONS is frozen", False)
    except TypeError:
        _check("PALETTE_DESCRIPTIONS is frozen", True)

    copy = colorblind_safe_palette()
    copy["frontier"] = "#ff0000"
    _check(
        "mutating the copy does not affect the constant",
        COLORBLIND_SAFE["frontier"] == "#009E73",
    )

    # ---- 9. check_palette_contrast ------------------------------ #
    print("check_palette_contrast:")
    ratios = check_palette_contrast("#ffffff")
    _check(
        "returns one entry per palette color",
        set(ratios) == set(COLORBLIND_SAFE),
    )
    _check(
        "measured (#0072B2) passes AA normal on white",
        ratios["measured"] >= WCAG_AA_NORMAL,
    )
    _check(
        "simulated (#E69F00) fails AA normal on white",
        ratios["simulated"] < WCAG_AA_NORMAL,
    )
    _check(
        "simulated fails AA large on white",
        ratios["simulated"] < WCAG_AA_LARGE,
    )
    _check(
        "all ratios in [1.0, 21.0]",
        all(
            1.0 <= r <= 21.0 and math.isfinite(r)
            for r in ratios.values()
        ),
    )
    try:
        check_palette_contrast("not-a-hex")
        _check("bad background rejected", False)
    except AccessibilityError:
        _check("bad background rejected", True)

    # ---- 10. Threshold constants --------------------------------- #
    print("WCAG constants:")
    _check("WCAG_AA_NORMAL == 4.5", WCAG_AA_NORMAL == 4.5)
    _check("WCAG_AA_LARGE == 3.0", WCAG_AA_LARGE == 3.0)
    _check("WCAG_AAA_NORMAL == 7.0", WCAG_AAA_NORMAL == 7.0)
    _check("WCAG_AAA_LARGE == 4.5", WCAG_AAA_LARGE == 4.5)

    # ---- 11. __all__ / __dir__ ---------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "AccessibilityError", "COLORBLIND_SAFE",
        "PALETTE_DESCRIPTIONS", "PaletteRole",
        "WCAG_AA_NORMAL", "WCAG_AA_LARGE",
        "WCAG_AAA_NORMAL", "WCAG_AAA_LARGE",
        "WcagResult", "check_contrast", "check_palette_contrast",
        "check_wcag", "colorblind_safe_palette",
        "passes_aa", "passes_aaa", "relative_luminance",
    ):
        _check(f"{_name} exported", _name in __all__)

    # ---- 12. Error type is a ValueError ------------------------ #
    print("error type:")
    _check(
        "AccessibilityError subclasses ValueError",
        issubclass(AccessibilityError, ValueError),
    )

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
