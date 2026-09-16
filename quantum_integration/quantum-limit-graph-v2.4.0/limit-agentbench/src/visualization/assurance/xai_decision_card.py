"""
XAI Decision Card.

The recommendation's first-priority feature. Produces an inspectable
view that connects telemetry, policy, safety, human review, and
multi-objective optimisation into one screen.
"""

from __future__ import annotations

import html
import json
import logging
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

from ..contracts.chart_schema import ChartContext

logger = logging.getLogger(__name__)


@dataclass
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
    alternatives_rejected: List[Dict[str, Any]] = field(default_factory=list)
    observed_quality: Optional[float] = None
    observed_energy_kwh: Optional[float] = None
    observed_carbon_kg: Optional[float] = None
    observed_latency_ms: Optional[float] = None
    evidence: Dict[str, Any] = field(default_factory=dict)
    confidence: Optional[float] = None
    provenance: Optional[Dict[str, Any]] = None
    context: Optional[ChartContext] = None

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        if self.context is not None:
            out["context"] = self.context.to_dict()
        return out


class DecisionCardRenderer:
    """Renders an XAIDecisionCard as HTML or Markdown."""

    @staticmethod
    def to_html(card: XAIDecisionCard) -> str:
        esc = html.escape
        parts: List[str] = []
        parts.append("<!doctype html><html><head><meta charset='utf-8'>")
        parts.append(f"<title>Decision {esc(card.decision_id)}</title>")
        parts.append(
            "<style>"
            "body{font-family:system-ui,-apple-system,sans-serif;margin:2em;max-width:80ch}"
            "h1{font-size:1.3em}"
            "section{margin:1.2em 0}"
            "ul{margin:0.4em 0 0.8em 1.2em}"
            "code{background:#f4f4f4;padding:2px 4px;border-radius:3px}"
            ".bad{color:#b00}"
            ".good{color:#0b0}"
            "</style>"
        )
        parts.append("</head><body>")
        parts.append(f"<h1>Decision — {esc(card.selected_action)}</h1>")

        if card.context is not None:
            parts.append(
                f"<p><em>{esc(card.context.to_subtitle())}</em></p>"
            )

        parts.append("<section><h2>Reason</h2><ul>")
        for r in card.rationale:
            parts.append(f"<li>{esc(r)}</li>")
        parts.append("</ul></section>")

        if card.constraints_passed or card.constraints_failed:
            parts.append("<section><h2>Constraints</h2><ul>")
            for c in card.constraints_passed:
                parts.append(f"<li class='good'>✓ {esc(c)}</li>")
            for c in card.constraints_failed:
                parts.append(f"<li class='bad'>✗ {esc(c)}</li>")
            parts.append("</ul></section>")

        if card.alternatives_rejected:
            parts.append("<section><h2>Alternatives</h2><ul>")
            for alt in card.alternatives_rejected:
                a = esc(str(alt.get("action", "?")))
                r = esc(str(alt.get("reason", "")))
                parts.append(f"<li><code>{a}</code> — {r}</li>")
            parts.append("</ul></section>")

        parts.append("<section><h2>Observed outcome</h2><ul>")
        if card.observed_quality is not None:
            parts.append(f"<li>Quality: {card.observed_quality:.3f}</li>")
        if card.observed_energy_kwh is not None:
            parts.append(f"<li>Energy: {card.observed_energy_kwh:.6f} kWh</li>")
        if card.observed_carbon_kg is not None:
            parts.append(f"<li>Carbon: {card.observed_carbon_kg:.6f} kgCO₂e</li>")
        if card.observed_latency_ms is not None:
            parts.append(f"<li>Latency: {card.observed_latency_ms:.1f} ms</li>")
        parts.append("</ul></section>")

        if card.evidence:
            parts.append("<section><h2>Evidence</h2><ul>")
            for k, v in card.evidence.items():
                parts.append(
                    f"<li><strong>{esc(str(k))}</strong>: "
                    f"<code>{esc(str(v))}</code></li>"
                )
            parts.append("</ul></section>")

        if card.confidence is not None:
            parts.append(
                f"<p><strong>Confidence</strong>: {card.confidence:.2f}</p>"
            )

        parts.append("</body></html>")
        return "\n".join(parts)

    @staticmethod
    def to_markdown(card: XAIDecisionCard) -> str:
        lines: List[str] = []
        lines.append(f"# Decision — {card.selected_action}")
        if card.context is not None:
            lines.append("")
            lines.append(f"*{card.context.to_subtitle()}*")
        lines.append("")
        lines.append("## Reason")
        for r in card.rationale:
            lines.append(f"- {r}")
        if card.constraints_passed or card.constraints_failed:
            lines.append("")
            lines.append("## Constraints")
            for c in card.constraints_passed:
                lines.append(f"- ✅ {c}")
            for c in card.constraints_failed:
                lines.append(f"- ❌ {c}")
        if card.alternatives_rejected:
            lines.append("")
            lines.append("## Alternatives")
            for alt in card.alternatives_rejected:
                lines.append(
                    f"- `{alt.get('action', '?')}` — "
                    f"{alt.get('reason', '')}"
                )
        lines.append("")
        lines.append("## Observed outcome")
        if card.observed_quality is not None:
            lines.append(f"- Quality: {card.observed_quality:.3f}")
        if card.observed_energy_kwh is not None:
            lines.append(f"- Energy: {card.observed_energy_kwh:.6f} kWh")
        if card.observed_carbon_kg is not None:
            lines.append(f"- Carbon: {card.observed_carbon_kg:.6f} kgCO₂e")
        if card.observed_latency_ms is not None:
            lines.append(f"- Latency: {card.observed_latency_ms:.1f} ms")
        if card.evidence:
            lines.append("")
            lines.append("## Evidence")
            for k, v in card.evidence.items():
                lines.append(f"- **{k}**: `{v}`")
        if card.confidence is not None:
            lines.append("")
            lines.append(f"**Confidence**: {card.confidence:.2f}")
        return "\n".join(lines)


def render_decision_card(
    card: XAIDecisionCard, *, format: str = "markdown",
) -> str:
    """Render a card in 'markdown' or 'html'."""
    if format.lower() == "html":
        return DecisionCardRenderer.to_html(card)
    return DecisionCardRenderer.to_markdown(card)
