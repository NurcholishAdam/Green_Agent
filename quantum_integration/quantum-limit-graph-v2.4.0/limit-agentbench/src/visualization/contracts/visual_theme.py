# src/visualization/contracts/theme.py

"""
Visual theme — palette, typography, and layout constants.

Every visual uses this theme so plots are consistent across the folder.

Accessibility
-------------
Two palettes are shipped:

- :data:`DEFAULT_THEME` — the legacy ``matplotlib tab10`` palette. Retained
  for backward compatibility, but **not colorblind-safe**: the red
  (``#d62728``) / green (``#2ca02c``) pair it uses for "warning" and
  "frontier" is indistinguishable for deuteranopes and protanopes
  (~8% of male readers). Contrast verification in
  :meth:`VisualTheme.__post_init__` logs a ``WARNING`` for each role that
  fails WCAG AA-large against the palette's own background.
- :data:`COLORBLIND_SAFE_THEME` — :data:`DEFAULT_THEME` with the palette
  replaced by :func:`accessibility_palette`, which pulls the Okabe–Ito
  derived colors from :mod:`visualization.contracts.accessibility`. Use
  this for any user-facing chart.

Callers who want to opt in without changing their code path can call
``DEFAULT_THEME.with_colorblind_safe_palette()``.

Behavior changes over the previous revision
-------------------------------------------
1. **Both dataclasses are frozen + slotted.**
   ``DEFAULT_THEME`` can no longer be corrupted by a consumer that mutates
   ``DEFAULT_THEME.palette.frontier = "#ff0000"``. Use
   :meth:`VisualTheme.with_palette` / :meth:`VisualTheme.with_font_sizes` /
   ``dataclasses.replace`` instead.

2. **Full ``__post_init__`` validation.** Every color is checked against
   the same ``#rgb`` / ``#rrggbb`` accept-set as
   :mod:`visualization.contracts.accessibility`; every numeric field is
   checked for type, finiteness, and positivity. Errors raise
   :class:`ThemeError`.

3. **``color_for`` raises on unknown sources** instead of silently
   returning the "dominated" color. The palette gained colors for every
   :class:`~visualization.contracts.chart_schema.DataSource` value
   (``measured``, ``estimated``, ``simulated``, ``federated``,
   ``external``, ``unknown``).

4. **``color_for`` accepts ``DataSource`` enum members** as well as
   their canonical strings (case-insensitive).

5. **New fields:**
   - ``Palette.name`` — an identifier rendered in logs and ``to_dict()``.
   - ``Palette.estimated`` / ``federated`` / ``external`` / ``unknown``.
   - ``VisualTheme.font_family_python`` — the font name passed to
     matplotlib and other Python-native renderers (which cannot parse the
     CSS ``font-family`` fallback syntax in ``font_family``).
   - ``font_size_axis_title`` / ``font_size_legend`` /
     ``font_size_annotation``.
   - ``marker_frontier`` / ``marker_dominated`` — shape redundancy so
     frontier vs. dominated points are distinguishable without relying on
     color alone.
   - ``line_style_simulated`` — dash-pattern redundancy for simulated
     lines.
   - ``dpi`` — explicit DPI for the pixel dimensions.
   - ``verify_contrast`` — opt-out of the constructor's contrast
     verification (default ``True``).

6. **Round-trip serialization** — ``to_dict`` / ``from_dict`` on both
   dataclasses, plus ``VisualTheme.to_dict`` (which nests the palette's
   dict), plus ``to_json`` / ``from_json`` wrappers on ``VisualTheme``.

7. **Frozen-style helpers** — :meth:`VisualTheme.with_palette`,
   :meth:`VisualTheme.with_font_sizes`,
   :meth:`VisualTheme.with_colorblind_safe_palette`,
   :meth:`Palette.with_color`.

Compatibility
-------------
- ``Palette(...)`` / ``VisualTheme(...)`` accept the same keyword
  arguments as before. All new fields are additive with defaults.
- Both dataclasses are now ``frozen=True, slots=True``. Anything that
  mutated ``DEFAULT_THEME.palette.*`` or added new attributes must switch
  to ``dataclasses.replace`` or the ``with_*`` helpers.
- ``color_for(unknown_source)`` now raises :class:`ThemeError`. Callers
  that relied on the silent "dominated" fall-through must handle the new
  error (or pass a valid source).
- ``font_family`` is unchanged. Its value is CSS syntax and is intended
  for HTML renderers; use ``font_family_python`` for matplotlib and other
  native plotting libraries.
- ``DEFAULT_THEME`` retains the legacy ``tab10`` palette for backward
  compatibility. The contrast verification logs ``WARNING``s at import
  time for the roles that fail AA-large. Callers who want to silence
  those warnings can construct ``VisualTheme(verify_contrast=False)``.
- ``DEFAULT_THEME``'s palette is unchanged, so existing color-output
  snapshots are preserved.

Requires Python 3.8+ (``slots=True`` requires 3.10+; drop it for older
versions — everything else is 3.8-compatible).
"""

from __future__ import annotations

import json
import logging
import math
import re
from dataclasses import dataclass, field, replace
from typing import (
    Any, Dict, List, Mapping, Optional, Tuple,
)

from ..accessibility.helpers import (
    AccessibilityError,
    COLORBLIND_SAFE,
    check_palette_contrast,
    WCAG_AA_LARGE,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ThemeError(ValueError):
    """Raised for invalid visual-theme inputs."""


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
_HEX_RE = re.compile(r"^#?([0-9a-fA-F]{3}|[0-9a-fA-F]{6})$")


def _require_hex(name: str, value: Any) -> str:
    if not isinstance(value, str) or _HEX_RE.match(value) is None:
        raise ThemeError(
            f"{name} must be a hex color (#rgb or #rrggbb), got "
            f"{value!r}."
        )
    return value


def _require_nonempty_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise ThemeError(f"{name} must be a non-empty string.")
    return value


def _require_positive_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ThemeError(f"{name} must be an int.")
    if value <= 0:
        raise ThemeError(f"{name} must be > 0, got {value}.")
    return value


def _require_positive_float(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ThemeError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv) or fv <= 0:
        raise ThemeError(
            f"{name} must be finite and > 0, got {value!r}."
        )
    return fv


# --------------------------------------------------------------------------- #
# Palette
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class Palette:
    """
    A named set of colors used across the folder.

    All color values must be valid hex strings (``#rgb`` or ``#rrggbb``).
    The ``name`` field is a short identifier rendered in ``repr`` and
    ``to_dict``; it is not required to be unique.
    """

    name: str = "tab10"

    # ---- data-source colors -------------------------------------- #
    frontier: str = "#2ca02c"        # green
    dominated: str = "#b0b0b0"       # gray
    simulated: str = "#ff7f0e"       # orange
    measured: str = "#1f77b4"        # blue
    estimated: str = "#7f7f7f"       # dark gray
    federated: str = "#bcbd22"       # olive
    external: str = "#17becf"        # teal
    unknown: str = "#7f7f7f"         # neutral gray

    # ---- semantic colors ----------------------------------------- #
    warning: str = "#d62728"         # red
    critical: str = "#8c0000"        # dark red
    accent: str = "#9467bd"          # purple

    # ---- surface colors ------------------------------------------ #
    background: str = "#ffffff"
    grid: str = "#e0e0e0"
    text: str = "#202020"

    # Ordered tuple of color field names, used by to_dict and
    # __post_init__. Kept as a class-level constant so adding a new
    # color only requires updating one place.
    _COLOR_FIELDS: Tuple[str, ...] = (
        "frontier", "dominated", "simulated", "measured",
        "estimated", "federated", "external", "unknown",
        "warning", "critical", "accent",
        "background", "grid", "text",
    )

    def __post_init__(self) -> None:
        _require_nonempty_str("name", self.name)
        for field_name in self._COLOR_FIELDS:
            _require_hex(field_name, getattr(self, field_name))

    def with_color(self, role: str, value: str) -> "Palette":
        """
        Return a copy with ``role`` replaced.

        ``role`` must be one of the palette's color fields; ``value``
        must be a valid hex color.
        """
        if role not in self._COLOR_FIELDS:
            raise ThemeError(
                f"unknown palette role {role!r}; expected one of "
                f"{self._COLOR_FIELDS}."
            )
        _require_hex(role, value)
        return replace(self, **{role: value})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            **{f: getattr(self, f) for f in self._COLOR_FIELDS},
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Palette":
        if not isinstance(data, Mapping):
            raise ThemeError(
                f"Palette.from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        valid = {"name", *cls._COLOR_FIELDS}
        try:
            return cls(**{
                k: v for k, v in data.items() if k in valid
            })
        except ThemeError:
            raise
        except (TypeError, ValueError) as exc:
            raise ThemeError(
                f"invalid Palette payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "Palette("
            f"name={self.name!r}, "
            f"frontier={self.frontier!r}, "
            f"measured={self.measured!r}, "
            f"background={self.background!r})"
        )


# --------------------------------------------------------------------------- #
# CVD-safe palette factory
# --------------------------------------------------------------------------- #
def accessibility_palette() -> Palette:
    """
    Return a :class:`Palette` populated from the CVD-safe
    :data:`accessibility.helpers.COLORBLIND_SAFE` palette.

    Roles without a corresponding CVD-safe color (``estimated``,
    ``federated``, ``external``, ``unknown``) use neutral colors that are
    distinct from the six Okabe–Ito members.
    """
    cs = COLORBLIND_SAFE
    return Palette(
        name="okabe-ito",
        frontier=cs["frontier"],
        dominated=cs["dominated"],
        simulated=cs["simulated"],
        measured=cs["measured"],
        estimated="#666666",
        federated="#117733",
        external="#44AA99",
        unknown="#666666",
        warning=cs["warning"],
        critical=cs["critical"],
        accent=cs["accent"],
        background="#ffffff",
        grid="#e0e0e0",
        text="#202020",
    )


# --------------------------------------------------------------------------- #
# VisualTheme
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class VisualTheme:
    """
    Palette + typography + layout.

    ``font_family`` uses CSS ``font-family`` syntax and is intended for
    HTML renderers; ``font_family_python`` is the single font name passed
    to matplotlib and other native plotting libraries.
    """

    palette: Palette = field(default_factory=Palette)

    # ---- typography ---------------------------------------------- #
    font_family: str = "system-ui, -apple-system, sans-serif"
    font_family_python: str = "DejaVu Sans"
    font_size_title: int = 16
    font_size_axis_title: int = 13
    font_size_axis: int = 12
    font_size_legend: int = 11
    font_size_annotation: int = 11
    font_size_footnote: int = 10

    # ---- layout -------------------------------------------------- #
    figure_width_px: int = 900
    figure_height_px: int = 600
    dpi: int = 96

    # ---- markers ------------------------------------------------- #
    marker_size_dominated: int = 8
    marker_size_frontier: int = 12
    marker_frontier: str = "o"
    marker_dominated: str = "x"

    # ---- lines --------------------------------------------------- #
    line_width: float = 2.0
    line_style_simulated: str = "dashed"

    # ---- behaviour ----------------------------------------------- #
    verify_contrast: bool = True

    def __post_init__(self) -> None:
        if not isinstance(self.palette, Palette):
            raise ThemeError(
                f"palette must be a Palette, got "
                f"{type(self.palette).__name__}."
            )

        _require_nonempty_str("font_family", self.font_family)
        _require_nonempty_str(
            "font_family_python", self.font_family_python,
        )

        for field_name in (
            "font_size_title", "font_size_axis_title", "font_size_axis",
            "font_size_legend", "font_size_annotation",
            "font_size_footnote",
            "figure_width_px", "figure_height_px", "dpi",
            "marker_size_dominated", "marker_size_frontier",
        ):
            _require_positive_int(field_name, getattr(self, field_name))

        _require_positive_float("line_width", self.line_width)

        _require_nonempty_str("marker_frontier", self.marker_frontier)
        _require_nonempty_str(
            "marker_dominated", self.marker_dominated,
        )
        _require_nonempty_str(
            "line_style_simulated", self.line_style_simulated,
        )

        if not isinstance(self.verify_contrast, bool):
            raise ThemeError("verify_contrast must be a bool.")

        # ---- contrast verification ------------------------------- #
        # Logs one WARNING per palette role whose contrast against the
        # palette's own background is below WCAG AA-large. Does not
        # raise — a low-contrast palette is a quality concern, not a
        # construction error.
        if self.verify_contrast:
            try:
                ratios = check_palette_contrast(self.palette.background)
            except AccessibilityError as exc:
                raise ThemeError(
                    f"contrast verification failed: {exc}"
                ) from exc
            for role, ratio in ratios.items():
                if ratio < WCAG_AA_LARGE:
                    logger.warning(
                        "Palette role %r has contrast %.2f on %r, "
                        "below WCAG AA-large (%.1f); text in this "
                        "color may be unreadable for some users.",
                        role, ratio, self.palette.background,
                        WCAG_AA_LARGE,
                    )

    # ---------------------------------------------------------- resolution
    def color_for(
        self, source: Any, is_frontier: bool = False,
    ) -> str:
        """
        Return the color for ``source``.

        Precedence is ``simulated`` > ``frontier`` > source-specific.
        Unknown sources raise :class:`ThemeError` — the previous
        implementation silently returned the "dominated" color, which
        meant "rejected by Pareto dominance" and was therefore
        semantically wrong for a merely-unknown source.
        """
        if hasattr(source, "value"):
            source_str = str(source.value).lower()
        elif isinstance(source, str):
            source_str = source.lower()
        else:
            raise ThemeError(
                f"source must be a string or an enum with .value, got "
                f"{type(source).__name__}."
            )

        if source_str == "simulated":
            return self.palette.simulated
        if is_frontier:
            return self.palette.frontier
        if source_str == "measured":
            return self.palette.measured
        if source_str == "estimated":
            return self.palette.estimated
        if source_str == "federated":
            return self.palette.federated
        if source_str == "external":
            return self.palette.external
        if source_str == "unknown":
            return self.palette.unknown
        raise ThemeError(
            f"unknown data source {source!r}; expected one of "
            f"'measured', 'estimated', 'simulated', 'federated', "
            f"'external', 'unknown'."
        )

    # ---------------------------------------------------------- frozen helpers
    def with_palette(self, palette: Palette) -> "VisualTheme":
        """Return a copy with ``palette`` replaced."""
        return replace(self, palette=palette)

    def with_font_sizes(
        self,
        *,
        title: Optional[int] = None,
        axis: Optional[int] = None,
        axis_title: Optional[int] = None,
        legend: Optional[int] = None,
        annotation: Optional[int] = None,
        footnote: Optional[int] = None,
    ) -> "VisualTheme":
        """
        Return a copy with font sizes replaced.

        Only the fields explicitly supplied are changed; passing
        ``None`` leaves the corresponding size unchanged.
        """
        changes: Dict[str, int] = {}
        if title is not None:
            changes["font_size_title"] = title
        if axis is not None:
            changes["font_size_axis"] = axis
        if axis_title is not None:
            changes["font_size_axis_title"] = axis_title
        if legend is not None:
            changes["font_size_legend"] = legend
        if annotation is not None:
            changes["font_size_annotation"] = annotation
        if footnote is not None:
            changes["font_size_footnote"] = footnote
        if not changes:
            return self
        return replace(self, **changes)

    def with_colorblind_safe_palette(self) -> "VisualTheme":
        """Return a copy using the CVD-safe palette."""
        return replace(self, palette=accessibility_palette())

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "palette": self.palette.to_dict(),
            "font_family": self.font_family,
            "font_family_python": self.font_family_python,
            "font_size_title": self.font_size_title,
            "font_size_axis_title": self.font_size_axis_title,
            "font_size_axis": self.font_size_axis,
            "font_size_legend": self.font_size_legend,
            "font_size_annotation": self.font_size_annotation,
            "font_size_footnote": self.font_size_footnote,
            "figure_width_px": self.figure_width_px,
            "figure_height_px": self.figure_height_px,
            "dpi": self.dpi,
            "marker_size_dominated": self.marker_size_dominated,
            "marker_size_frontier": self.marker_size_frontier,
            "marker_frontier": self.marker_frontier,
            "marker_dominated": self.marker_dominated,
            "line_width": self.line_width,
            "line_style_simulated": self.line_style_simulated,
            "verify_contrast": self.verify_contrast,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "VisualTheme":
        if not isinstance(data, Mapping):
            raise ThemeError(
                f"VisualTheme.from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        try:
            payload = dict(data)
            palette_raw = payload.get("palette")
            if isinstance(palette_raw, Mapping):
                payload["palette"] = Palette.from_dict(palette_raw)
            elif palette_raw is None or isinstance(palette_raw, Palette):
                pass
            else:
                raise ThemeError(
                    f"palette must be a Mapping or Palette, got "
                    f"{type(palette_raw).__name__}."
                )
            valid = set(cls.__dataclass_fields__)
            return cls(**{
                k: v for k, v in payload.items() if k in valid
            })
        except ThemeError:
            raise
        except (TypeError, ValueError) as exc:
            raise ThemeError(
                f"invalid VisualTheme payload: {exc}"
            ) from exc

    def to_json(self, *, indent: Optional[int] = None) -> str:
        """Serialize to JSON. The payload is primitive-only."""
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_json(cls, payload: str) -> "VisualTheme":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ThemeError(
                f"invalid VisualTheme JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    # ---------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "VisualTheme("
            f"palette={self.palette.name!r}, "
            f"font_size_title={self.font_size_title}, "
            f"figure={self.figure_width_px}x{self.figure_height_px}, "
            f"dpi={self.dpi})"
        )


# --------------------------------------------------------------------------- #
# Default instances
# --------------------------------------------------------------------------- #
DEFAULT_THEME: VisualTheme = VisualTheme()

COLORBLIND_SAFE_THEME: VisualTheme = VisualTheme(
    palette=accessibility_palette(),
)


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "COLORBLIND_SAFE_THEME",
    "DEFAULT_THEME",
    "Palette",
    "ThemeError",
    "VisualTheme",
    "accessibility_palette",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m visualization.contracts.theme
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

    # ---- 1. Defaults & repr ------------------------------------- #
    print("defaults:")
    p = Palette()
    _check("Palette.name == 'tab10'", p.name == "tab10")
    _check("Palette.frontier == '#2ca02c'", p.frontier == "#2ca02c")
    _check(
        "Palette has every DataSource color",
        all(
            hasattr(p, role)
            for role in (
                "measured", "estimated", "simulated",
                "federated", "external", "unknown",
            )
        ),
    )
    _check(
        "Palette.__repr__ mentions name",
        "name='tab10'" in repr(p),
    )

    t = VisualTheme()
    _check(
        "VisualTheme.__repr__ mentions palette name",
        "palette='tab10'" in repr(t),
    )
    _check("VisualTheme default dpi == 96", t.dpi == 96)
    _check(
        "VisualTheme default verify_contrast == True",
        t.verify_contrast is True,
    )

    # ---- 2. Validation on Palette ------------------------------- #
    print("Palette validation:")
    for bad_kwargs in (
        dict(frontier="not-a-color"),
        dict(frontier="#gg0000"),
        dict(frontier=42),
        dict(frontier=""),
        dict(frontier=None),
        dict(name=""),
        dict(name=42),
    ):
        try:
            Palette(**bad_kwargs)  # type: ignore[arg-type]
            _check(f"reject Palette({list(bad_kwargs)})", False)
        except ThemeError:
            _check(f"reject Palette({list(bad_kwargs)})", True)
        except Exception:
            _check(
                f"reject Palette({list(bad_kwargs)}) (wrong type)", False,
            )

    # ---- 3. Validation on VisualTheme --------------------------- #
    print("VisualTheme validation:")
    for bad_kwargs in (
        dict(palette="not-a-palette"),
        dict(font_family=""),
        dict(font_family_python=42),
        dict(font_size_title=0),
        dict(font_size_title=-1),
        dict(font_size_title="large"),
        dict(font_size_title=True),
        dict(figure_width_px=0),
        dict(figure_height_px=-1),
        dict(dpi=0),
        dict(marker_size_frontier=-2),
        dict(line_width=-1.0),
        dict(line_width=0),
        dict(line_width="thin"),
        dict(marker_frontier=""),
        dict(line_style_simulated=""),
        dict(verify_contrast="yes"),
    ):
        try:
            VisualTheme(**bad_kwargs)  # type: ignore[arg-type]
            _check(f"reject VisualTheme({list(bad_kwargs)})", False)
        except ThemeError:
            _check(f"reject VisualTheme({list(bad_kwargs)})", True)
        except Exception:
            _check(
                f"reject VisualTheme({list(bad_kwargs)}) (wrong type)",
                False,
            )

    # ---- 4. Frozen dataclass ------------------------------------ #
    print("frozen:")
    try:
        p.frontier = "#ff0000"  # type: ignore[misc]
        _check("Palette mutation rejected", False)
    except (AttributeError, Exception):
        _check("Palette mutation rejected", True)

    try:
        t.font_size_title = 99  # type: ignore[misc]
        _check("VisualTheme mutation rejected", False)
    except (AttributeError, Exception):
        _check("VisualTheme mutation rejected", True)

    try:
        t.palette.frontier = "#ff0000"  # type: ignore[misc]
        _check("Nested Palette mutation rejected", False)
    except (AttributeError, Exception):
        _check("Nested Palette mutation rejected", True)

    # ---- 5. with_* helpers ------------------------------------- #
    print("with_* helpers:")
    new_p = p.with_color("frontier", "#009E73")
    _check("with_color replaces field", new_p.frontier == "#009E73")
    _check(
        "with_color leaves original unchanged",
        p.frontier == "#2ca02c",
    )
    try:
        p.with_color("not-a-role", "#ff0000")
        _check("with_color rejects unknown role", False)
    except ThemeError:
        _check("with_color rejects unknown role", True)
    try:
        p.with_color("frontier", "not-a-color")
        _check("with_color rejects invalid color", False)
    except ThemeError:
        _check("with_color rejects invalid color", True)

    new_t = t.with_font_sizes(title=20, axis=14)
    _check("with_font_sizes updates title", new_t.font_size_title == 20)
    _check("with_font_sizes updates axis", new_t.font_size_axis == 14)
    _check(
        "with_font_sizes leaves original unchanged",
        t.font_size_title == 16,
    )
    _check("with_font_sizes with no args is a no-op", t.with_font_sizes() is t)

    new_t2 = t.with_palette(accessibility_palette())
    _check(
        "with_palette changes palette",
        new_t2.palette.name == "okabe-ito",
    )
    _check(
        "with_palette leaves original unchanged",
        t.palette.name == "tab10",
    )

    new_t3 = t.with_colorblind_safe_palette()
    _check(
        "with_colorblind_safe_palette works",
        new_t3.palette.name == "okabe-ito",
    )

    # ---- 6. color_for precedence ------------------------------- #
    print("color_for:")
    _check(
        "measured → measured color",
        t.color_for("measured") == t.palette.measured,
    )
    _check(
        "simulated → simulated color",
        t.color_for("simulated") == t.palette.simulated,
    )
    _check(
        "frontier → frontier color",
        t.color_for("measured", is_frontier=True) == t.palette.frontier,
    )
    _check(
        "simulated overrides frontier",
        t.color_for("simulated", is_frontier=True)
        == t.palette.simulated,
    )
    _check(
        "estimated → estimated color",
        t.color_for("estimated") == t.palette.estimated,
    )
    _check(
        "federated → federated color",
        t.color_for("federated") == t.palette.federated,
    )
    _check(
        "external → external color",
        t.color_for("external") == t.palette.external,
    )
    _check(
        "unknown → unknown color",
        t.color_for("unknown") == t.palette.unknown,
    )

    # Enum input accepted when chart_schema is importable
    try:
        from .chart_schema import DataSource  # type: ignore[import]
    except Exception:
        DataSource = None  # type: ignore[assignment]
    if DataSource is not None:
        _check(
            "DataSource enum accepted",
            t.color_for(DataSource.MEASURED) == t.palette.measured,
        )

    try:
        t.color_for("not-a-source")
        _check("unknown source raises", False)
    except ThemeError:
        _check("unknown source raises", True)

    try:
        t.color_for(None)  # type: ignore[arg-type]
        _check("None source raises", False)
    except ThemeError:
        _check("None source raises", True)

    _check(
        "case-insensitive source",
        t.color_for("MEASURED") == t.palette.measured,
    )

    # ---- 7. Round-trip ----------------------------------------- #
    print("round-trip:")
    theme = VisualTheme(
        palette=Palette(name="custom", frontier="#00ff00"),
        font_size_title=20,
        dpi=144,
        verify_contrast=False,
    )
    payload = theme.to_dict()
    _check(
        "payload includes palette dict",
        isinstance(payload["palette"], dict),
    )
    _check(
        "payload palette carries name",
        payload["palette"]["name"] == "custom",
    )
    restored = VisualTheme.from_dict(payload)
    _check("restored theme equals original", restored == theme)

    json_payload = theme.to_json(indent=2)
    restored2 = VisualTheme.from_json(json_payload)
    _check("json round-trip equals", restored2 == theme)

    empty_restored = VisualTheme.from_dict(VisualTheme().to_dict())
    _check("empty round-trip equals", empty_restored == VisualTheme())

    p_payload = p.to_dict()
    p_restored = Palette.from_dict(p_payload)
    _check("palette round-trip equals", p_restored == p)

    # ---- 8. from_dict error handling --------------------------- #
    print("from_dict errors:")
    for bad_payload in (
        "not-a-mapping",
        {"palette": "not-a-palette"},
        {"palette": {"frontier": "not-a-color"}},
        {"font_size_title": -1},
        {"line_width": "wide"},
    ):
        try:
            VisualTheme.from_dict(bad_payload)  # type: ignore[arg-type]
            _check(f"reject {bad_payload!r}", False)
        except ThemeError:
            _check(f"reject {bad_payload!r}", True)

    try:
        VisualTheme.from_json("{not json")
        _check("bad JSON rejected", False)
    except ThemeError:
        _check("bad JSON rejected", True)

    # ---- 9. accessibility_palette ------------------------------- #
    print("accessibility_palette:")
    ap = accessibility_palette()
    _check("accessibility_palette name", ap.name == "okabe-ito")
    _check(
        "frontier matches COLORBLIND_SAFE",
        ap.frontier == COLORBLIND_SAFE["frontier"],
    )
    _check(
        "measured matches COLORBLIND_SAFE",
        ap.measured == COLORBLIND_SAFE["measured"],
    )
    _check(
        "warning matches COLORBLIND_SAFE",
        ap.warning == COLORBLIND_SAFE["warning"],
    )
    _check(
        "critical matches COLORBLIND_SAFE",
        ap.critical == COLORBLIND_SAFE["critical"],
    )

    # ---- 10. DEFAULT_THEME / COLORBLIND_SAFE_THEME -------------- #
    print("module-level themes:")
    _check(
        "DEFAULT_THEME uses tab10",
        DEFAULT_THEME.palette.name == "tab10",
    )
    _check(
        "COLORBLIND_SAFE_THEME uses okabe-ito",
        COLORBLIND_SAFE_THEME.palette.name == "okabe-ito",
    )

    try:
        DEFAULT_THEME.font_size_title = 99  # type: ignore[misc]
        _check("DEFAULT_THEME is frozen", False)
    except (AttributeError, Exception):
        _check("DEFAULT_THEME is frozen", True)

    try:
        DEFAULT_THEME.palette.frontier = "#ff0000"  # type: ignore[misc]
        _check("DEFAULT_THEME palette is frozen", False)
    except (AttributeError, Exception):
        _check("DEFAULT_THEME palette is frozen", True)

    # ---- 11. __all__ / __dir__ --------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))
    for _name in (
        "COLORBLIND_SAFE_THEME", "DEFAULT_THEME", "Palette",
        "ThemeError", "VisualTheme", "accessibility_palette",
    ):
        _check(f"{_name} exported", _name in __all__)

    # ---- 12. Error type ---------------------------------------- #
    print("error type:")
    _check(
        "ThemeError subclasses ValueError",
        issubclass(ThemeError, ValueError),
    )

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
