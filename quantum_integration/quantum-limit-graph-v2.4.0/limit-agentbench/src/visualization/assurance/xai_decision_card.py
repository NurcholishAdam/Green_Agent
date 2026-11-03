# src/explainability/xai_decision_card.py

"""
XAI Decision Card.
==================

The recommendation's first-priority feature. Produces an inspectable
view that connects telemetry, policy, safety, human review, and
multi-objective optimisation into one screen.

Escaping guarantees
-------------------
Both renderers escape all user-supplied text:

- **HTML** — every interpolated value passes through ``html.escape``,
  so a rationale or evidence value containing ``<script>`` cannot inject
  markup.
- **Markdown** — text values pass through ``_md_text``, which escapes
  ``\\ ` * _ [ ] < >`` and normalises newlines to spaces so a rationale
  containing ``*asterisk*`` or ``<b>`` cannot break the surrounding
  document or corrupt adjacent list items. ``alternatives_rejected``
  action identifiers and evidence / provenance values use
  ``_md_inline_code``, which wraps in a backtick fence longer than any
  backtick run inside — so a value containing `` ` `` still renders as
  literal code.

Fractional unit conventions
---------------------------
``observed_carbon_kg`` is labelled ``kgCO₂e`` (with U+2082 subscript) in
both renderers to match the recommendation's example. If your environment
requires maximum font compatibility (some terminals, older static-site
generators), substitute ``kgCO2e`` in your own renderer or post-process
the output.

Empty sections
--------------
Sections whose data is empty (no ``rationale``, no observations, no
``evidence``, no ``provenance``) are **suppressed entirely** — a card
never shows a bare heading over an empty list. A card with no rationale
renders without a ``## Reason`` section rather than an empty one.

Fragments
---------
``to_html`` / ``to_markdown`` produce a full, self-contained document.
``to_html_fragment`` / ``to_markdown_fragment`` produce a section that
can be embedded inside a larger report: the HTML fragment is wrapped in
``<div class="xai-decision-card">`` with its style block scoped to that
class, and the Markdown fragment uses ``##`` headings instead of ``#``
so it does not compete with the surrounding document's hierarchy.

Enhancements
------------
- ``XAIDecisionCard`` — ``frozen=True``, ``slots=True``; ``__post_init__``
  validates every field (non-empty strings, list-of-strings for text
  fields, finite / non-negative numerics, ``ChartContext`` for
  ``context``); concise ``__repr__``.
- **``from_dict`` / ``to_json`` / ``from_json``** — full round-trip;
  ``to_dict`` builds the payload manually to avoid ``asdict``'s deep-copy
  of ``context``.
- **Markdown escaping** — rationale, constraints, alternatives, evidence,
  and provenance all escaped (``_md_text`` / ``_md_inline_code``);
  newlines normalised.
- **``provenance`` is now rendered** in both HTML and Markdown.
- **Empty sections suppressed** in both renderers.
- ``alternatives_rejected`` typed as ``List[RejectedAlternative]``
  (TypedDict) with per-entry validation.
- ``render_decision_card(card, *, renderer_format="markdown",
  fragment=False)`` — validated top-level entry point; ``format=`` renamed
  to ``renderer_format=`` so it no longer shadows the builtin; ``fragment``
  kwarg switches between full documents and embeddable fragments.
- ``to_html(card, *, style=None)`` — optional CSS override; the default
  style is exposed as ``DEFAULT_STYLE`` and scoped to ``.xai-decision-card``.
- ``to_html_fragment`` / ``to_markdown_fragment`` companions.
- ``title="passed"`` / ``title="failed"`` on constraint ``<li>`` elements
  so a colorblind user (or an exported PDF) can distinguish them without
  relying on the CSS colors.
- ``XAIDecisionCardError(ValueError)`` — narrow error type; raised from
  ``__post_init__``, ``from_dict``, and ``render_decision_card``.
- ``__dir__`` on the module.
- ``__main__`` smoke test uses ``_check()`` helpers so ``python -O`` no
  longer passes silently; covers markdown escaping, provenance, empty
  sections, validation errors, format validation, round-trip, and
  fragment rendering.

Compatibility
-------------
- ``XAIDecisionCard(...)`` accepts the same keyword arguments as before;
  the dataclass is now frozen, so callers who mutated the card between
  renders (``card.confidence = ...``) must construct a new card instead.
- ``render_decision_card(card, format="html")`` — **breaking**: the
  parameter is now ``renderer_format=``. Update callers or use
  ``DecisionCardRenderer.to_html(card)`` directly.
- ``DecisionCardRenderer.to_html(card)`` / ``to_markdown(card)`` still
  return the same full-document output shape.
- Invalid cards (empty ``decision_id``, out-of-range ``confidence``,
  non-list ``rationale``, etc.) now raise ``XAIDecisionCardError`` at
  construction — previously they silently constructed and rendered.
- Empty sections are now omitted from the output — a card with no
  rationale produces no ``## Reason`` heading.

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop ``slots=True`` for 3.8/3.9.
"""

from __future__ import annotations

import html
import json
import logging
import math
import re
from dataclasses import dataclass, field
from typing import (
    Any, Dict, List, Mapping, Optional, TypedDict,
)

from ..contracts.chart_schema import ChartContext

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class XAIDecisionCardError(ValueError):
    """Raised for invalid decision-card inputs or rendering requests."""


# --------------------------------------------------------------------------- #
# Rejected-alternative shape
# --------------------------------------------------------------------------- #
class RejectedAlternative(TypedDict, total=False):
    """
    Implicit schema for ``XAIDecisionCard.alternatives_rejected`` entries.

    At runtime the value is a plain ``dict``; this TypedDict documents the
    two keys the renderer reads.
    """

    action: str
    reason: str


# --------------------------------------------------------------------------- #
# Default style
# --------------------------------------------------------------------------- #
DEFAULT_STYLE: str = (
    ".xai-decision-card{"
    "font-family:system-ui,-apple-system,sans-serif;"
    "line-height:1.5;max-width:80ch;margin:2em}"
    ".xai-decision-card h1{font-size:1.3em;margin:0 0 0.5em}"
    ".xai-decision-card h2{font-size:1.05em;margin:0.8em 0 0.3em}"
    ".xai-decision-card section{margin:1.2em 0}"
    ".xai-decision-card ul{margin:0.4em 0 0.8em 1.2em;padding:0}"
    ".xai-decision-card li{margin:0.2em 0}"
    ".xai-decision-card code{"
    "background:#f4f4f4;padding:2px 4px;border-radius:3px;"
    "white-space:pre-wrap;word-break:break-word}"
    ".xai-decision-card .bad{color:#b00}"
    ".xai-decision-card .good{color:#0b0}"
)


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #
def _require_nonempty_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise XAIDecisionCardError(f"{name} must be a non-empty string.")
    return value


def _require_str_list(name: str, value: Any) -> List[str]:
    if not isinstance(value, list):
        raise XAIDecisionCardError(
            f"{name} must be a list of strings, got "
            f"{type(value).__name__}."
        )
    for i, item in enumerate(value):
        if not isinstance(item, str):
            raise XAIDecisionCardError(f"{name}[{i}] must be a string.")
    return value


def _require_mapping_or_none(
    name: str, value: Any
) -> Optional[Mapping[str, Any]]:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise XAIDecisionCardError(
            f"{name} must be a Mapping or None, got "
            f"{type(value).__name__}."
        )
    return value


def _require_finite_optional(name: str, value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise XAIDecisionCardError(
            f"{name} must be numeric or None, got "
            f"{type(value).__name__}."
        )
    fv = float(value)
    if not math.isfinite(fv):
        raise XAIDecisionCardError(
            f"{name} must be finite, got {value!r}."
        )
    return fv


def _require_unit_interval_optional(
    name: str, value: Any
) -> Optional[float]:
    fv = _require_finite_optional(name, value)
    if fv is not None and not 0.0 <= fv <= 1.0:
        raise XAIDecisionCardError(
            f"{name} must be in [0, 1], got {fv}."
        )
    return fv


def _require_nonneg_optional(name: str, value: Any) -> Optional[float]:
    fv = _require_finite_optional(name, value)
    if fv is not None and fv < 0.0:
        raise XAIDecisionCardError(
            f"{name} must be >= 0, got {fv}."
        )
    return fv


# --------------------------------------------------------------------------- #
# Markdown escaping
# --------------------------------------------------------------------------- #
_MD_ESCAPE_RE = re.compile(r"([\\`*_\[\]<>])")
_NEWLINE_RE = re.compile(r"\r\n|\r|\n")


def _normalize_newlines(text: str) -> str:
    """Replace every newline variant with a single space."""
    return _NEWLINE_RE.sub(" ", text)


def _md_text(text: str) -> str:
    """
    Escape a text value for inline Markdown rendering.

    Escapes the "must-escape" set (``\\ ` * _ [ ] < >``) and normalises
    newlines. Sufficient for rationale, constraints, reasons, and the
    inline ``key`` part of evidence / provenance entries.
    """
    return _MD_ESCAPE_RE.sub(r"\\\1", _normalize_newlines(str(text)))


def _md_inline_code(text: str) -> str:
    """
    Wrap ``text`` in a backtick fence that is longer than any backtick
    run inside it.

    The content itself is **not** escaped — code spans render their
    content literally. The fence length is chosen per CommonMark: if the
    content has a run of N contiguous backticks, the fence is at least
    N+1 backticks long, so the parser cannot mistake an inner run for the
    closing fence.
    """
    text = _normalize_newlines(str(text))
    if "`" not in text:
        return f"`{text}`"
    max_run = 1
    while "`" * (max_run + 1) in text:
        max_run += 1
    fence = "`" * (max_run + 1)
    return f"{fence} {text} {fence}"


# --------------------------------------------------------------------------- #
# Card dataclass
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class XAIDecisionCard:
    """
    Structured XAI card for a single decision.

    Mirrors the recommendation's example card:

        Decision: Use INT8 model on edge device
        Reason: ...
        Constraints: ...
        Alternatives: ...
        Observed outcome: ...
        Evidence: ...
    """

    decision_id: str
    selected_action: str
    rationale: List[str] = field(default_factory=list)
    constraints_passed: List[str] = field(default_factory=list)
    constraints_failed: List[str] = field(default_factory=list)
    alternatives_rejected: List[RejectedAlternative] = field(
        default_factory=list,
    )
    observed_quality: Optional[float] = None
    observed_energy_kwh: Optional[float] = None
    observed_carbon_kg: Optional[float] = None
    observed_latency_ms: Optional[float] = None
    evidence: Dict[str, Any] = field(default_factory=dict)
    confidence: Optional[float] = None
    provenance: Optional[Dict[str, Any]] = None
    context: Optional[ChartContext] = None

    def __post_init__(self) -> None:
        _require_nonempty_str("decision_id", self.decision_id)
        _require_nonempty_str("selected_action", self.selected_action)
        _require_str_list("rationale", self.rationale)
        _require_str_list("constraints_passed", self.constraints_passed)
        _require_str_list("constraints_failed", self.constraints_failed)

        # --- alternatives_rejected -------------------------------- #
        if not isinstance(self.alternatives_rejected, list):
            raise XAIDecisionCardError(
                "alternatives_rejected must be a list."
            )
        for i, alt in enumerate(self.alternatives_rejected):
            if not isinstance(alt, Mapping):
                raise XAIDecisionCardError(
                    f"alternatives_rejected[{i}] must be a Mapping, "
                    f"got {type(alt).__name__}."
                )
            if "action" in alt and not isinstance(alt["action"], str):
                raise XAIDecisionCardError(
                    f"alternatives_rejected[{i}].action must be a string."
                )
            if "reason" in alt and not isinstance(alt["reason"], str):
                raise XAIDecisionCardError(
                    f"alternatives_rejected[{i}].reason must be a string."
                )

        # --- observed numerics ------------------------------------ #
        _require_nonneg_optional(
            "observed_quality", self.observed_quality,
        )
        _require_nonneg_optional(
            "observed_energy_kwh", self.observed_energy_kwh,
        )
        _require_nonneg_optional(
            "observed_carbon_kg", self.observed_carbon_kg,
        )
        _require_nonneg_optional(
            "observed_latency_ms", self.observed_latency_ms,
        )
        _require_unit_interval_optional("confidence", self.confidence)

        # --- evidence / provenance / context ---------------------- #
        if not isinstance(self.evidence, Mapping):
            raise XAIDecisionCardError(
                f"evidence must be a Mapping, got "
                f"{type(self.evidence).__name__}."
            )
        _require_mapping_or_none("provenance", self.provenance)
        if self.context is not None and not isinstance(
            self.context, ChartContext
        ):
            raise XAIDecisionCardError(
                f"context must be a ChartContext or None, got "
                f"{type(self.context).__name__}."
            )

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "decision_id": self.decision_id,
            "selected_action": self.selected_action,
            "rationale": list(self.rationale),
            "constraints_passed": list(self.constraints_passed),
            "constraints_failed": list(self.constraints_failed),
            "alternatives_rejected": [
                dict(a) for a in self.alternatives_rejected
            ],
            "observed_quality": self.observed_quality,
            "observed_energy_kwh": self.observed_energy_kwh,
            "observed_carbon_kg": self.observed_carbon_kg,
            "observed_latency_ms": self.observed_latency_ms,
            "evidence": dict(self.evidence),
            "confidence": self.confidence,
            "provenance": (
                None if self.provenance is None
                else dict(self.provenance)
            ),
            "context": None,
        }
        if self.context is not None:
            try:
                out["context"] = self.context.to_dict()
            except Exception as exc:
                raise XAIDecisionCardError(
                    f"ChartContext.to_dict() failed: {exc}"
                ) from exc
        return out

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "XAIDecisionCard":
        if not isinstance(data, Mapping):
            raise XAIDecisionCardError(
                "XAIDecisionCard.from_dict expects a Mapping."
            )
        try:
            raw_ctx = data.get("context")
            context: Optional[ChartContext]
            if raw_ctx is None:
                context = None
            elif isinstance(raw_ctx, ChartContext):
                context = raw_ctx
            elif isinstance(raw_ctx, Mapping):
                if hasattr(ChartContext, "from_dict"):
                    context = ChartContext.from_dict(dict(raw_ctx))
                else:
                    context = ChartContext(**dict(raw_ctx))
            else:
                raise XAIDecisionCardError(
                    f"context must be Mapping|ChartContext|None, got "
                    f"{type(raw_ctx).__name__}."
                )

            return cls(
                decision_id=str(data["decision_id"]),
                selected_action=str(data["selected_action"]),
                rationale=[str(x) for x in data.get("rationale", []) or []],
                constraints_passed=[
                    str(x) for x in data.get("constraints_passed", []) or []
                ],
                constraints_failed=[
                    str(x) for x in data.get("constraints_failed", []) or []
                ],
                alternatives_rejected=[
                    dict(a)
                    for a in data.get("alternatives_rejected", []) or []
                ],
                observed_quality=data.get("observed_quality"),
                observed_energy_kwh=data.get("observed_energy_kwh"),
                observed_carbon_kg=data.get("observed_carbon_kg"),
                observed_latency_ms=data.get("observed_latency_ms"),
                evidence=dict(data.get("evidence", {}) or {}),
                confidence=data.get("confidence"),
                provenance=(
                    None if data.get("provenance") is None
                    else dict(data["provenance"])
                ),
                context=context,
            )
        except XAIDecisionCardError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise XAIDecisionCardError(
                f"invalid XAIDecisionCard payload: {exc}"
            ) from exc

    def to_json(self, *, indent: Optional[int] = None, **kwargs: Any) -> str:
        # Payload is primitive-only; no default= needed.
        return json.dumps(self.to_dict(), indent=indent, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "XAIDecisionCard":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise XAIDecisionCardError(
                f"invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    def __repr__(self) -> str:
        return (
            "XAIDecisionCard("
            f"decision_id={self.decision_id!r}, "
            f"selected_action={self.selected_action!r}, "
            f"confidence={self.confidence!r})"
        )


# --------------------------------------------------------------------------- #
# Renderer
# --------------------------------------------------------------------------- #
class DecisionCardRenderer:
    """Renders an :class:`XAIDecisionCard` as HTML or Markdown."""

    # ---------------------------------------------------------- HTML
    @staticmethod
    def to_html(
        card: XAIDecisionCard, *, style: Optional[str] = None,
    ) -> str:
        """Render a card as a complete HTML document."""
        return DecisionCardRenderer._to_html(
            card, style=style, full_document=True,
        )

    @staticmethod
    def to_html_fragment(
        card: XAIDecisionCard, *, style: Optional[str] = None,
    ) -> str:
        """Render a card as an HTML fragment (``<div>`` wrapper + style)."""
        return DecisionCardRenderer._to_html(
            card, style=style, full_document=False,
        )

    @staticmethod
    def _to_html(
        card: XAIDecisionCard,
        *,
        style: Optional[str],
        full_document: bool,
    ) -> str:
        if not isinstance(card, XAIDecisionCard):
            raise XAIDecisionCardError(
                f"card must be an XAIDecisionCard, got "
                f"{type(card).__name__}."
            )
        if style is None:
            style = DEFAULT_STYLE
        esc = html.escape
        parts: List[str] = []

        if full_document:
            parts.append("<!doctype html><html><head><meta charset='utf-8'>")
            parts.append(f"<title>Decision {esc(card.decision_id)}</title>")
            parts.append("</head><body>")

        parts.append('<div class="xai-decision-card">')
        parts.append(f"<style>{style}</style>")
        parts.append(
            f"<h1>Decision — {esc(card.selected_action)}</h1>"
        )

        subtitle = DecisionCardRenderer._subtitle(card)
        if subtitle:
            parts.append(f"<p><em>{esc(subtitle)}</em></p>")

        if card.rationale:
            parts.append("<section><h2>Reason</h2><ul>")
            for r in card.rationale:
                parts.append(f"<li>{esc(r)}</li>")
            parts.append("</ul></section>")

        if card.constraints_passed or card.constraints_failed:
            parts.append("<section><h2>Constraints</h2><ul>")
            for c in card.constraints_passed:
                parts.append(
                    f"<li class='good' title='passed'>✓ {esc(c)}</li>"
                )
            for c in card.constraints_failed:
                parts.append(
                    f"<li class='bad' title='failed'>✗ {esc(c)}</li>"
                )
            parts.append("</ul></section>")

        if card.alternatives_rejected:
            parts.append("<section><h2>Alternatives</h2><ul>")
            for alt in card.alternatives_rejected:
                a = esc(str(alt.get("action", "?")))
                r = esc(str(alt.get("reason", "")))
                parts.append(f"<li><code>{a}</code> — {r}</li>")
            parts.append("</ul></section>")

        obs: List[str] = []
        if card.observed_quality is not None:
            obs.append(f"<li>Quality: {card.observed_quality:.3f}</li>")
        if card.observed_energy_kwh is not None:
            obs.append(
                f"<li>Energy: {card.observed_energy_kwh:.6f} kWh</li>"
            )
        if card.observed_carbon_kg is not None:
            obs.append(
                f"<li>Carbon: {card.observed_carbon_kg:.6f} kgCO₂e</li>"
            )
        if card.observed_latency_ms is not None:
            obs.append(
                f"<li>Latency: {card.observed_latency_ms:.1f} ms</li>"
            )
        if obs:
            parts.append("<section><h2>Observed outcome</h2><ul>")
            parts.extend(obs)
            parts.append("</ul></section>")

        if card.evidence:
            parts.append("<section><h2>Evidence</h2><ul>")
            for k, v in card.evidence.items():
                parts.append(
                    f"<li><strong>{esc(str(k))}</strong>: "
                    f"<code>{esc(str(v))}</code></li>"
                )
            parts.append("</ul></section>")

        if card.provenance:
            parts.append("<section><h2>Provenance</h2><ul>")
            for k, v in card.provenance.items():
                parts.append(
                    f"<li><strong>{esc(str(k))}</strong>: "
                    f"<code>{esc(str(v))}</code></li>"
                )
            parts.append("</ul></section>")

        if card.confidence is not None:
            parts.append(
                f"<p><strong>Confidence</strong>: "
                f"{card.confidence:.2f}</p>"
            )

        parts.append("</div>")

        if full_document:
            parts.append("</body></html>")

        return "\n".join(parts)

    # ---------------------------------------------------------- Markdown
    @staticmethod
    def to_markdown(card: XAIDecisionCard) -> str:
        """Render a card as a complete Markdown document (``#`` heading)."""
        return DecisionCardRenderer._to_markdown(card, heading_level=1)

    @staticmethod
    def to_markdown_fragment(card: XAIDecisionCard) -> str:
        """Render a card as a Markdown fragment (``##`` heading)."""
        return DecisionCardRenderer._to_markdown(card, heading_level=2)

    @staticmethod
    def _to_markdown(
        card: XAIDecisionCard, *, heading_level: int,
    ) -> str:
        if not isinstance(card, XAIDecisionCard):
            raise XAIDecisionCardError(
                f"card must be an XAIDecisionCard, got "
                f"{type(card).__name__}."
            )
        h = "#" * heading_level
        lines: List[str] = []

        lines.append(f"{h} Decision — {_md_text(card.selected_action)}")

        subtitle = DecisionCardRenderer._subtitle(card)
        if subtitle:
            lines.append("")
            lines.append(f"*{_md_text(subtitle)}*")

        if card.rationale:
            lines.append("")
            lines.append(f"{h}# Reason")
            for r in card.rationale:
                lines.append(f"- {_md_text(r)}")

        if card.constraints_passed or card.constraints_failed:
            lines.append("")
            lines.append(f"{h}# Constraints")
            for c in card.constraints_passed:
                lines.append(f"- ✅ {_md_text(c)}")
            for c in card.constraints_failed:
                lines.append(f"- ❌ {_md_text(c)}")

        if card.alternatives_rejected:
            lines.append("")
            lines.append(f"{h}# Alternatives")
            for alt in card.alternatives_rejected:
                a = _md_inline_code(str(alt.get("action", "?")))
                r = _md_text(str(alt.get("reason", "")))
                lines.append(f"- {a} — {r}")

        obs: List[str] = []
        if card.observed_quality is not None:
            obs.append(f"- Quality: {card.observed_quality:.3f}")
        if card.observed_energy_kwh is not None:
            obs.append(
                f"- Energy: {card.observed_energy_kwh:.6f} kWh"
            )
        if card.observed_carbon_kg is not None:
            obs.append(
                f"- Carbon: {card.observed_carbon_kg:.6f} kgCO₂e"
            )
        if card.observed_latency_ms is not None:
            obs.append(
                f"- Latency: {card.observed_latency_ms:.1f} ms"
            )
        if obs:
            lines.append("")
            lines.append(f"{h}# Observed outcome")
            lines.extend(obs)

        if card.evidence:
            lines.append("")
            lines.append(f"{h}# Evidence")
            for k, v in card.evidence.items():
                lines.append(
                    f"- **{_md_text(str(k))}**: "
                    f"{_md_inline_code(str(v))}"
                )

        if card.provenance:
            lines.append("")
            lines.append(f"{h}# Provenance")
            for k, v in card.provenance.items():
                lines.append(
                    f"- **{_md_text(str(k))}**: "
                    f"{_md_inline_code(str(v))}"
                )

        if card.confidence is not None:
            lines.append("")
            lines.append(f"**Confidence**: {card.confidence:.2f}")

        return "\n".join(lines)

    # ---------------------------------------------------------- shared
    @staticmethod
    def _subtitle(card: XAIDecisionCard) -> Optional[str]:
        """Return ``context.to_subtitle()`` if available, else ``None``."""
        if card.context is None:
            return None
        try:
            sub = card.context.to_subtitle()
        except Exception:
            logger.warning(
                "ChartContext.to_subtitle() failed", exc_info=True,
            )
            return None
        return sub if isinstance(sub, str) and sub else None

    def __dir__(self) -> List[str]:
        return sorted(
            name for name in type(self).__dict__
            if not name.startswith("_") and callable(
                getattr(type(self), name)
            )
        )


# --------------------------------------------------------------------------- #
# Top-level entry point
# --------------------------------------------------------------------------- #
_HTML_FORMATS = frozenset({"html", "htm"})
_MARKDOWN_FORMATS = frozenset({"markdown", "md"})


def render_decision_card(
    card: XAIDecisionCard,
    *,
    renderer_format: str = "markdown",
    fragment: bool = False,
) -> str:
    """
    Render a card in ``'markdown'`` (alias ``'md'``) or ``'html'`` (alias
    ``'htm'``).

    Parameters
    ----------
    card : XAIDecisionCard
    renderer_format : str, default ``"markdown"``
    fragment : bool, default False
        If True, render an embeddable fragment instead of a full document.

    Raises
    ------
    XAIDecisionCardError
        If ``card`` is not an :class:`XAIDecisionCard`, or
        ``renderer_format`` is not recognised.
    """
    if not isinstance(card, XAIDecisionCard):
        raise XAIDecisionCardError(
            f"card must be an XAIDecisionCard, got "
            f"{type(card).__name__}."
        )
    if not isinstance(renderer_format, str) or not renderer_format:
        raise XAIDecisionCardError(
            "renderer_format must be a non-empty string."
        )
    fmt = renderer_format.lower()
    if fmt in _HTML_FORMATS:
        return (
            DecisionCardRenderer.to_html_fragment(card)
            if fragment
            else DecisionCardRenderer.to_html(card)
        )
    if fmt in _MARKDOWN_FORMATS:
        return (
            DecisionCardRenderer.to_markdown_fragment(card)
            if fragment
            else DecisionCardRenderer.to_markdown(card)
        )
    raise XAIDecisionCardError(
        f"unsupported renderer_format {renderer_format!r}; "
        f"expected one of "
        f"{sorted(_HTML_FORMATS | _MARKDOWN_FORMATS)}."
    )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "DEFAULT_STYLE",
    "DecisionCardRenderer",
    "RejectedAlternative",
    "XAIDecisionCard",
    "XAIDecisionCardError",
    "render_decision_card",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m explainability.xai_decision_card
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

    # ---- 1. Basic card + render ---------------------------------- #
    print("basic render:")
    card = XAIDecisionCard(
        decision_id="d1",
        selected_action="Use INT8 model on edge device",
        rationale=["Lower energy for similar quality", "Fits latency budget"],
        constraints_passed=["Latency < 100ms", "Memory < 500MB"],
        constraints_failed=["Quality < 95% (got 94.2%)"],
        alternatives_rejected=[
            {"action": "FP16 on edge", "reason": "exceeds memory budget"},
        ],
        observed_quality=0.942,
        observed_energy_kwh=0.0032,
        observed_carbon_kg=0.0006,
        observed_latency_ms=87.4,
        evidence={"benchmark": "run-42", "device": "raspberry-pi-4"},
        confidence=0.78,
        provenance={"source": "planner-v3", "run_id": "abc123"},
    )
    print("  repr:", card)

    md = DecisionCardRenderer.to_markdown(card)
    _check(
        "markdown has title",
        "# Decision — Use INT8 model on edge device" in md,
    )
    _check("markdown has reason", "## Reason" in md)
    _check("markdown has constraints", "✅ Latency < 100ms" in md)
    _check("markdown has failed constraint", "❌ Quality" in md)
    _check("markdown has alternatives", "FP16 on edge" in md)
    _check("markdown has evidence", "benchmark" in md)
    _check(
        "markdown has PROVENANCE (new)",
        "## Provenance" in md and "planner-v3" in md,
    )
    _check("markdown has confidence", "Confidence" in md)

    html_out = DecisionCardRenderer.to_html(card)
    _check("html is doctype", html_out.startswith("<!doctype"))
    _check("html has scoped wrapper", 'class="xai-decision-card"' in html_out)
    _check("html has provenance", "Provenance" in html_out)
    _check("html has title attribute", "title='passed'" in html_out)

    # ---- 2. Markdown injection defense --------------------------- #
    print("markdown injection defense:")
    inject_card = XAIDecisionCard(
        decision_id="d1",
        selected_action="test",
        rationale=[
            "has *asterisk* and <script>alert(1)</script>",
            "has `backtick` inside",
        ],
        constraints_passed=["constraint with * asterisk"],
        evidence={"note": "value with `backtick` and <b>bold</b>"},
    )
    md = DecisionCardRenderer.to_markdown(inject_card)
    _check(
        "asterisk escaped",
        "\\*asterisk\\*" in md,
    )
    _check(
        "angle brackets escaped",
        "\\<script\\>" in md and "\\</script\\>" in md,
    )
    _check(
        "backtick escaped in text",
        "\\`backtick\\`" in md,
    )
    _check(
        "double-backtick fence for backtick content",
        "`` value with `backtick`" in md,
    )
    _check(
        "no raw <script> in output",
        "<script>" not in md,
    )

    # ---- 3. Newline normalization -------------------------------- #
    print("newline normalization:")
    nl_card = XAIDecisionCard(
        decision_id="d1",
        selected_action="test",
        rationale=["line 1\nline 2\r\nline 3"],
    )
    md = DecisionCardRenderer.to_markdown(nl_card)
    _check(
        "rationale newlines become spaces",
        "- line 1 line 2 line 3" in md,
    )

    # ---- 4. Empty sections suppressed ---------------------------- #
    print("empty sections suppressed:")
    empty_card = XAIDecisionCard(
        decision_id="d1",
        selected_action="test",
    )
    md = DecisionCardRenderer.to_markdown(empty_card)
    _check("no Reason heading", "## Reason" not in md)
    _check("no Constraints heading", "## Constraints" not in md)
    _check("no Observed outcome heading", "## Observed outcome" not in md)
    _check("no Evidence heading", "## Evidence" not in md)
    _check("no Provenance heading", "## Provenance" not in md)

    # ---- 5. Fragment rendering ---------------------------------- #
    print("fragments:")
    md_frag = DecisionCardRenderer.to_markdown_fragment(card)
    _check(
        "fragment uses ## heading",
        md_frag.startswith("## Decision"),
    )
    _check(
        "fragment does not start with # (single)",
        not md_frag.startswith("# Decision"),
    )
    html_frag = DecisionCardRenderer.to_html_fragment(card)
    _check(
        "html fragment is div wrapper",
        html_frag.startswith('<div class="xai-decision-card">'),
    )
    _check(
        "html fragment has no doctype",
        "<!doctype" not in html_frag,
    )

    # ---- 6. render_decision_card -------------------------------- #
    print("render_decision_card:")
    out = render_decision_card(card, renderer_format="md")
    _check("md alias works", "## Reason" in out)
    out = render_decision_card(card, renderer_format="htm")
    _check("htm alias works", out.startswith("<!doctype"))
    out = render_decision_card(card, renderer_format="html", fragment=True)
    _check(
        "fragment flag works",
        out.startswith('<div class="xai-decision-card">'),
    )
    for bad in (None, "", "pdf", "docx"):
        try:
            render_decision_card(card, renderer_format=bad)  # type: ignore
            _check(f"reject format={bad!r}", False)
        except XAIDecisionCardError:
            _check(f"reject format={bad!r}", True)

    try:
        render_decision_card("not a card")  # type: ignore[arg-type]
        _check("reject non-card", False)
    except XAIDecisionCardError:
        _check("reject non-card", True)

    # ---- 7. Validation on construction --------------------------- #
    print("validation on construction:")
    for bad_kwargs in (
        dict(decision_id="", selected_action="x"),
        dict(decision_id="x", selected_action=""),
        dict(decision_id="x", selected_action="x", rationale="not-a-list"),
        dict(
            decision_id="x", selected_action="x",
            rationale=["ok", 42],
        ),
        dict(
            decision_id="x", selected_action="x",
            confidence=2.0,
        ),
        dict(
            decision_id="x", selected_action="x",
            confidence=float("nan"),
        ),
        dict(
            decision_id="x", selected_action="x",
            observed_quality=-1.0,
        ),
        dict(
            decision_id="x", selected_action="x",
            observed_energy_kwh=float("inf"),
        ),
        dict(
            decision_id="x", selected_action="x",
            alternatives_rejected="not-a-list",
        ),
        dict(
            decision_id="x", selected_action="x",
            alternatives_rejected=[{"action": 42}],
        ),
        dict(
            decision_id="x", selected_action="x",
            context={"not": "a ChartContext"},
        ),
    ):
        try:
            XAIDecisionCard(**bad_kwargs)  # type: ignore[arg-type]
            _check(f"reject {list(bad_kwargs)}", False)
        except XAIDecisionCardError:
            _check(f"reject {list(bad_kwargs)}", True)

    # ---- 8. Round-trip ------------------------------------------ #
    print("round-trip:")
    d = card.to_dict()
    _check("to_dict returns dict", isinstance(d, dict))
    _check("to_dict preserves decision_id", d["decision_id"] == "d1")
    restored = XAIDecisionCard.from_dict(d)
    _check(
        "from_dict preserves selected_action",
        restored.selected_action == card.selected_action,
    )
    _check(
        "from_dict preserves evidence",
        restored.evidence == card.evidence,
    )
    _check(
        "from_dict preserves provenance",
        restored.provenance == card.provenance,
    )

    payload = card.to_json(indent=2)
    restored2 = XAIDecisionCard.from_json(payload)
    _check(
        "json round-trip",
        restored2.to_dict() == card.to_dict(),
    )

    try:
        XAIDecisionCard.from_dict({"selected_action": "x"})  # no decision_id
        _check("missing decision_id rejected", False)
    except XAIDecisionCardError:
        _check("missing decision_id rejected", True)

    try:
        XAIDecisionCard.from_json("{not json")
        _check("bad JSON rejected", False)
    except XAIDecisionCardError:
        _check("bad JSON rejected", True)

    # ---- 9. Custom style --------------------------------------- #
    print("custom style:")
    html_out = DecisionCardRenderer.to_html(
        card, style=".xai-decision-card{color:#111}",
    )
    _check(
        "custom style injected",
        "color:#111" in html_out,
    )
    _check(
        "default style not present when overridden",
        "font-family:system-ui" not in html_out,
    )

    # ---- 10. Frozen dataclass ---------------------------------- #
    print("frozen dataclass:")
    try:
        card.selected_action = "other"  # type: ignore[misc]
        _check("mutation rejected", False)
    except (AttributeError, Exception):
        _check("mutation rejected", True)

    # ---- 11. __dir__ ------------------------------------------- #
    print("dir:")
    _check("module __dir__ matches __all__", __dir__() == sorted(__all__))
    _check(
        "renderer exposes to_html / to_markdown",
        "to_html" in dir(DecisionCardRenderer)
        and "to_markdown" in dir(DecisionCardRenderer),
    )

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
