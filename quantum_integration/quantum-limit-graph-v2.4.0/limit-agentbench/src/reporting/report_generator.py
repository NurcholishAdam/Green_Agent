"""
Report Generation for Green Agent (Enhanced)
=============================================

Renders EvidenceBundles for four stakeholder audiences through four
formats. Backward-compatible with the original three-method API.

Original API preserved:
    ReportGenerator
        .generate_executive_summary(full_report) -> str
        .generate_technical_report(full_report) -> str
        .generate_research_report(full_report) -> str

Enhanced API:
    ReportGenerator
        .render_bundle(bundle, audience, format) -> str
        .render_many(bundles, audience, format) -> str
        .select_audience(bundle, audience) -> EvidenceBundle

    JSONRenderer, MarkdownRenderer, HTMLRenderer, CSVRenderer

Enhancements:
  1. Audience selection (Operator, Technical, Governance, Executive)
  2. Four renderers (JSON, Markdown, HTML, CSV)
  3. Operational/contractual carbon separation in output
  4. Simulated-value warnings in every rendered report
  5. Provenance summary per metric
  6. Verification summary
  7. Explanation cards
  8. Human review status
  9. Original plain-text methods preserved
"""

from __future__ import annotations

import dataclasses
import html
import io
import json
import logging
from dataclasses import asdict
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional

from .layered_reporter import (
    Audience,
    EvidenceBundle,
    LayeredReporter,
)

logger = logging.getLogger(__name__)


# =============================================================================
# ORIGINAL ReportGenerator — preserved + extended
# =============================================================================

class ReportGenerator:
    """
    Original string-reporting class, extended with bundle-aware methods.
    """

    def __init__(self, reporter: Optional[LayeredReporter] = None):
        # Original constructor took no arguments; keep optional for BC.
        self.reporter = reporter or LayeredReporter()

    # ------------------------------------------------------------------
    # ORIGINAL methods — preserved verbatim
    # ------------------------------------------------------------------

    def generate_executive_summary(self, full_report: Dict[str, Any]) -> str:
        """Executive summary string (original behavior)."""
        l1 = full_report.get("layer1", {})
        l3 = full_report.get("layer3", {})
        return f"""
{'=' * 70}
EXECUTIVE SUMMARY — {l3.get('scenario_name', 'unknown').upper()} SCENARIO
{'=' * 70}
Accuracy:       {l1.get('accuracy', 0):.2%}
Energy:         {l1.get('energy_wh', 0):.2f} Wh
Carbon:         {l1.get('carbon_co2_g', 0):.2f} gCO2
Latency:        {l1.get('latency_ms', 0):.0f} ms
Weighted Score: {l3.get('weighted_score', 0):.4f}
{'=' * 70}
""".strip()

    def generate_technical_report(self, full_report: Dict[str, Any]) -> str:
        """Technical report string (original behavior)."""
        l1 = full_report.get("layer1", {})
        l2 = full_report.get("layer2", {})
        l3 = full_report.get("layer3", {})
        return f"""
{'=' * 70}
TECHNICAL REPORT
{'=' * 70}
Layer 1 — Raw Metrics
  Accuracy:      {l1.get('accuracy', 0):.4f}
  Energy (Wh):   {l1.get('energy_wh', 0):.4f}
  Carbon (gCO2): {l1.get('carbon_co2_g', 0):.4f}
  Latency (ms):  {l1.get('latency_ms', 0):.2f}

Layer 2 — Normalized Metrics
  Energy/task:               {l2.get('energy_per_task', 0):.6f}
  Carbon/correct answer:     {l2.get('carbon_per_correct_answer', 0):.4f}
  Latency/reasoning step:    {l2.get('latency_per_reasoning_step', 0):.2f}
  Efficiency score:          {l2.get('efficiency_score', 0):.4f}
  Task complexity:           {l2.get('task_complexity', 0):.4f}
  Complexity tier:           {l2.get('complexity_tier', 'unknown')}

Layer 3 — Scenario Score
  Scenario:       {l3.get('scenario_name', 'unknown')}
  Weighted score: {l3.get('weighted_score', 0):.4f}
  Weights:        {l3.get('weights_used', {})}
{'=' * 70}
""".strip()

    def generate_research_report(self, full_report: Dict[str, Any]) -> str:
        """Research report string (original behavior)."""
        l1 = full_report.get("layer1", {})
        l2 = full_report.get("layer2", {})
        l3 = full_report.get("layer3", {})
        return f"""
{'=' * 70}
RESEARCH REPORT
{'=' * 70}
Raw Observations:
  Accuracy:      {l1.get('accuracy', 0):.4f}
  Energy (Wh):   {l1.get('energy_wh', 0):.4f}
  Carbon (gCO2): {l1.get('carbon_co2_g', 0):.4f}
  Latency (ms):  {l1.get('latency_ms', 0):.2f}

Normalized:
  Energy/task:               {l2.get('energy_per_task', 0):.6f}
  Carbon/correct answer:     {l2.get('carbon_per_correct_answer', 0):.4f}
  Latency/reasoning step:    {l2.get('latency_per_reasoning_step', 0):.2f}
  Efficiency score:          {l2.get('efficiency_score', 0):.4f}

Scenario Analysis:
  Scenario:       {l3.get('scenario_name', 'unknown')}
  Weighted score: {l3.get('weighted_score', 0):.4f}
  Weights:        {l3.get('weights_used', {})}
{'=' * 70}
""".strip()

    # ------------------------------------------------------------------
    # ENHANCED methods — bundle-aware
    # ------------------------------------------------------------------

    def select_audience(
        self,
        bundle: EvidenceBundle,
        audience: Audience,
    ) -> EvidenceBundle:
        """Redact a bundle for the requested audience."""
        return self.reporter.redact(bundle, audience)

    def render_bundle(
        self,
        bundle: EvidenceBundle,
        *,
        audience: Audience = Audience.TECHNICAL,
        format: str = "markdown",
    ) -> str:
        """
        Render a bundle for the requested audience and format.

        Formats: "json", "markdown", "html", "csv".
        """
        # Redact first
        redacted = self.select_audience(bundle, audience)

        # Dispatch to the appropriate renderer
        renderers: Dict[str, Callable[[EvidenceBundle, Audience], str]] = {
            "json": JSONRenderer.render,
            "markdown": MarkdownRenderer.render,
            "html": HTMLRenderer.render,
            "csv": CSVRenderer.render,
        }
        renderer = renderers.get(format.lower())
        if renderer is None:
            raise ValueError(
                f"Unknown format: {format!r}; "
                f"expected one of {list(renderers)}"
            )
        return renderer(redacted, audience)

    def render_many(
        self,
        bundles: Iterable[EvidenceBundle],
        *,
        audience: Audience = Audience.TECHNICAL,
        format: str = "markdown",
    ) -> str:
        """Render a sequence of bundles into a single document."""
        bundles = list(bundles)
        if format.lower() == "json":
            return json.dumps(
                [b.to_dict() for b in bundles], indent=2, default=str,
            )
        if format.lower() == "csv":
            return CSVRenderer.render_many(bundles)
        # Markdown/HTML — concatenate
        parts = [
            self.render_bundle(b, audience=audience, format=format)
            for b in bundles
        ]
        return "\n\n---\n\n".join(parts)


# =============================================================================
# Renderers
# =============================================================================

class JSONRenderer:
    """Machine-readable JSON output."""

    @staticmethod
    def render(bundle: EvidenceBundle, audience: Audience) -> str:
        payload = bundle.to_dict()
        payload["_audience"] = audience.value
        return json.dumps(payload, indent=2, default=str)


class MarkdownRenderer:
    """Human-readable Markdown output."""

    @staticmethod
    def render(bundle: EvidenceBundle, audience: Audience) -> str:
        d = bundle.to_dict()
        lines: List[str] = []
        lines.append(f"# Evidence Bundle — {d['run_id']}")
        lines.append("")
        lines.append(f"**Audience**: {audience.value}")
        lines.append(f"**System version**: {d['system_version']}")
        lines.append(f"**Policy version**: {d['policy_version']}")
        lines.append(f"**Timestamp**: {d['timestamp']}")
        lines.append(f"**Bundle version**: {d['bundle_version']}")
        if d.get("content_hash"):
            lines.append(f"**Content hash**: `{d['content_hash']}`")
        if d.get("signature"):
            lines.append(f"**Signature**: `{d['signature']}` (signer: {d.get('signer')})")
        lines.append("")

        # --- Operational sustainability ---
        lines.append("## Operational sustainability")
        lines.append("")
        lines.append(f"- **Energy**: {d['energy_kwh']:.6f} kWh")
        lines.append(
            f"- **Operational CO₂e**: {d['operational_co2e_kg']:.6f} kg"
        )
        lines.append(f"- **Helium**: {d['helium_units']:.6f} units")
        if d.get("simulated"):
            lines.append("- ⚠️ **Simulated values present**")
        if d.get("chaos_injected"):
            lines.append("- ⚠️ **Chaos-injected**")
        lines.append("")

        # --- Contractual instruments (kept separate) ---
        if d.get("carbon_instruments"):
            lines.append("## Contractual carbon instruments")
            lines.append("")
            lines.append(
                "> **Note**: Operational and contractual emissions are "
                "kept separate. Never sum them."
            )
            lines.append("")
            for inst in d["carbon_instruments"]:
                lines.append(
                    f"- `{inst.get('instrument_id')}` "
                    f"({inst.get('kind')}): "
                    f"{inst.get('quantity')} {inst.get('unit')}"
                )
            lines.append("")

        # --- Quality and latency ---
        lines.append("## Quality & latency")
        lines.append("")
        for k, v in d.get("quality_metrics", {}).items():
            lines.append(f"- **{k}**: {v}")
        for k, v in d.get("latency_metrics", {}).items():
            lines.append(f"- **{k}**: {v}")
        lines.append("")

        # --- Safety verdict ---
        lines.append("## Safety verdict")
        lines.append("")
        sv = d.get("safety_verdict") or {}
        lines.append(f"- **Verdict**: {sv.get('verdict', 'n/a')}")
        if sv.get("violations"):
            lines.append(f"- **Violations**: {sv['violations']}")
        lines.append("")

        # --- Verification (audience-gated) ---
        if audience in (Audience.TECHNICAL, Audience.GOVERNANCE) and d.get("verification"):
            lines.append("## Verification evidence")
            lines.append("")
            v = d["verification"]
            lines.append(f"- **Verdict**: {v.get('verdict')}")
            lines.append(f"- **Coverage**: {v.get('coverage_pct', 0):.1%}")
            lines.append(f"- **Properties checked**: {v.get('properties_checked')}")
            if v.get("counterexamples"):
                lines.append(f"- **Counterexamples**: {len(v['counterexamples'])}")
            lines.append("")

        # --- Explanation card ---
        exp = d.get("explanation") or {}
        if exp:
            lines.append("## Explanation")
            lines.append("")
            lines.append(f"- **Selected action**: {exp.get('selected_action')}")
            for r in exp.get("rationale", []):
                lines.append(f"  - {r}")
            if exp.get("confidence"):
                lines.append(f"- **Confidence**: {exp['confidence']:.2f}")
            lines.append("")

        # --- Human review ---
        if d.get("human_review"):
            lines.append("## Human review")
            lines.append("")
            for k, v in d["human_review"].items():
                lines.append(f"- **{k}**: {v}")
            lines.append("")

        # --- Provenance (audience-gated) ---
        if audience in (Audience.TECHNICAL, Audience.GOVERNANCE):
            lines.append("## Metric provenance")
            lines.append("")
            for metric, p in (d.get("metric_provenance") or {}).items():
                if isinstance(p, dict):
                    lines.append(
                        f"- **{metric}**: {p.get('source_kind')} "
                        f"({p.get('trust_level', '?')})"
                    )
            lines.append("")

        return "\n".join(lines)


class HTMLRenderer:
    """HTML output (safe-escaped, no external dependencies)."""

    @staticmethod
    def render(bundle: EvidenceBundle, audience: Audience) -> str:
        d = bundle.to_dict()
        esc = html.escape
        parts: List[str] = []
        parts.append("<!doctype html>")
        parts.append("<html><head><meta charset='utf-8'>")
        parts.append(f"<title>Evidence Bundle — {esc(d['run_id'])}</title>")
        parts.append("<style>")
        parts.append("body{font-family:system-ui,-apple-system,sans-serif;margin:2em;max-width:80ch}")
        parts.append("table{border-collapse:collapse;width:100%}")
        parts.append("td,th{border:1px solid #ddd;padding:6px;text-align:left}")
        parts.append(".warn{color:#b00;font-weight:bold}")
        parts.append("</style></head><body>")
        parts.append(f"<h1>Evidence Bundle — {esc(d['run_id'])}</h1>")
        parts.append(f"<p><strong>Audience</strong>: {esc(audience.value)}</p>")
        parts.append(f"<p><strong>Policy version</strong>: {esc(d['policy_version'])}</p>")
        parts.append(f"<p><strong>Content hash</strong>: <code>{esc(d.get('content_hash') or '')}</code></p>")

        if d.get("simulated"):
            parts.append("<p class='warn'>⚠️ Simulated values present.</p>")
        if d.get("chaos_injected"):
            parts.append("<p class='warn'>⚠️ Chaos-injected run.</p>")

        parts.append("<h2>Operational sustainability</h2>")
        parts.append("<table>")
        parts.append(f"<tr><th>Energy (kWh)</th><td>{d['energy_kwh']:.6f}</td></tr>")
        parts.append(f"<tr><th>Operational CO₂e (kg)</th><td>{d['operational_co2e_kg']:.6f}</td></tr>")
        parts.append(f"<tr><th>Helium (units)</th><td>{d['helium_units']:.6f}</td></tr>")
        parts.append("</table>")

        if d.get("carbon_instruments"):
            parts.append("<h2>Contractual carbon instruments</h2>")
            parts.append("<p><em>Kept separate from operational emissions.</em></p>")
            parts.append("<ul>")
            for inst in d["carbon_instruments"]:
                parts.append(
                    f"<li><code>{esc(str(inst.get('instrument_id')))}</code>: "
                    f"{esc(str(inst.get('quantity')))} {esc(str(inst.get('unit')))}</li>"
                )
            parts.append("</ul>")

        parts.append("</body></html>")
        return "\n".join(parts)


class CSVRenderer:
    """CSV output — one row per bundle, with flattened columns."""

    _COLUMNS = [
        "run_id",
        "timestamp",
        "system_version",
        "policy_version",
        "deployment_id",
        "agent_id",
        "task_id",
        "energy_kwh",
        "operational_co2e_kg",
        "contractual_co2e_kg",
        "helium_units",
        "accuracy",
        "latency_ms",
        "tool_calls",
        "precision",
        "region",
        "simulated",
        "chaos_injected",
        "content_hash",
    ]

    @classmethod
    def _row(cls, bundle: EvidenceBundle) -> Dict[str, Any]:
        d = bundle.to_dict()
        q = d.get("quality_metrics", {}) or {}
        l = d.get("latency_metrics", {}) or {}
        return {
            "run_id": d["run_id"],
            "timestamp": d["timestamp"],
            "system_version": d["system_version"],
            "policy_version": d["policy_version"],
            "deployment_id": d.get("deployment_id"),
            "agent_id": d.get("agent_id"),
            "task_id": d.get("task_id"),
            "energy_kwh": d["energy_kwh"],
            "operational_co2e_kg": d["operational_co2e_kg"],
            "contractual_co2e_kg": d.get("contractual_co2e_kg", 0.0),
            "helium_units": d.get("helium_units", 0.0),
            "accuracy": q.get("accuracy"),
            "latency_ms": l.get("latency_ms"),
            "tool_calls": l.get("tool_calls"),
            "precision": d.get("precision"),
            "region": d.get("region"),
            "simulated": d.get("simulated"),
            "chaos_injected": d.get("chaos_injected"),
            "content_hash": d.get("content_hash"),
        }

    @classmethod
    def render(cls, bundle: EvidenceBundle, audience: Audience) -> str:
        import csv
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=cls._COLUMNS)
        writer.writeheader()
        writer.writerow(cls._row(bundle))
        return buf.getvalue()

    @classmethod
    def render_many(cls, bundles: Iterable[EvidenceBundle]) -> str:
        import csv
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=cls._COLUMNS)
        writer.writeheader()
        for b in bundles:
            writer.writerow(cls._row(b))
        return buf.getvalue()


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    from .layered_reporter import (
        BundleBuilder, CarbonInstruments, ExplanationCard,
        VerificationEvidence, Audience,
    )

    logging.basicConfig(level=logging.INFO)

    result = {
        "accuracy": 0.92,
        "energy_kwh": 0.045,
        "carbon_kg": 0.018,
        "latency_ms": 120.0,
        "task_id": "task-1",
        "run_id": "run-001",
        "policy_version": "v5.0.1",
        "deployment_id": "us-ca-prod-01",
        "agent_id": "worker-A",
        "precision": "int8",
        "region": "US-CA",
        "source": "measured",
    }

    instruments = CarbonInstruments()
    instruments.add("rec-2026-001", "rec", 5.0, "MWh",
                    vintage_year=2026, matching_period="2026-03")

    bundle = BundleBuilder.build(
        result,
        carbon_instruments=instruments,
        explanation=ExplanationCard(
            decision_id="dec-1",
            selected_action="route_to_lora",
            rationale=["lowest carbon route"],
            confidence=0.92,
        ),
    )

    reporter = LayeredReporter()
    bundle = reporter.sign_bundle(bundle, key="secret-key")

    gen = ReportGenerator(reporter)

    # Original behavior — plain text
    print("=== Original plain-text (executive) ===")
    full = reporter.generate_full_report(result)
    print(gen.generate_executive_summary(full)[:300] + "...")

    # Enhanced — JSON
    print("\n=== Enhanced JSON (technical) ===")
    print(gen.render_bundle(bundle, audience=Audience.TECHNICAL, format="json")[:400] + "...")

    # Enhanced — Markdown
    print("\n=== Enhanced Markdown (governance) ===")
    print(gen.render_bundle(bundle, audience=Audience.GOVERNANCE, format="markdown")[:800] + "...")

    # Enhanced — CSV
    print("\n=== Enhanced CSV ===")
    print(gen.render_bundle(bundle, audience=Audience.OPERATOR, format="csv"))

    # Redaction check
    print("=== Redaction ===")
    exec_view = gen.select_audience(bundle, Audience.EXECUTIVE)
    print(f"  Executive sees verification: {bool(exec_view.verification)}")
    gov_view = gen.select_audience(bundle, Audience.GOVERNANCE)
    print(f"  Governance sees verification: {bool(gov_view.verification)}")
