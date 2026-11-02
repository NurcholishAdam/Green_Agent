# src/r1/report_generator.py

"""
Report Generation for Green Agent
=================================

Renders EvidenceBundles for four stakeholder audiences through four
formats. Backward-compatible with the original three-method API.

Public API
----------
Original (preserved verbatim in behaviour):

    ReportGenerator
        .generate_executive_summary(full_report) -> str
        .generate_technical_report(full_report) -> str
        .generate_research_report(full_report) -> str

Enhanced:

    ReportGenerator
        .render_bundle(bundle, *, audience, format) -> str
        .render_many(bundles, *, audience, format) -> str
        .select_audience(bundle, audience) -> EvidenceBundle
        .redact(bundle, audience) -> EvidenceBundle    # passthrough

    JSONRenderer, MarkdownRenderer, HTMLRenderer, CSVRenderer

Design
------
- **Redaction is uniform.** ``render_bundle`` and ``render_many`` both
  call :meth:`LayeredReporter.redact` **before** dispatch, for every
  format. There is no output path that bypasses audience filtering.
- **Injection-safe.** Markdown values that originate from bundle
  content are escaped via :func:`_md` / :func:`_md_inline`. HTML uses
  :func:`html.escape`.
- **Audience / format normalised once** at the API boundary. String
  inputs are accepted; invalid values raise ``TypeError`` /
  ``ValueError`` with a clear message — never an ``AttributeError``
  mid-render.
- **HTML batch is a single document.** :meth:`HTMLRenderer.render_fragment`
  is used by :meth:`render_many` to concatenate ``<hr>``-separated
  fragments inside one ``<!doctype html>`` wrapper.
- **Reproducible output.** JSON uses ``sort_keys`` and
  ``allow_nan=False``; CSV uses ``lineterminator="\\n"``.

Version: 2.0.0
"""

from __future__ import annotations

import csv
import html
import io
import json
import logging
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional

__all__ = [
    "VERSION",
    "ReportGenerator",
    "JSONRenderer",
    "MarkdownRenderer",
    "HTMLRenderer",
    "CSVRenderer",
]

VERSION = "2.0.0"

logger = logging.getLogger(__name__)

try:
    from .layered_reporter import (
        Audience,
        EvidenceBundle,
        LayeredReporter,
    )
except ImportError:  # pragma: no cover — script-style fallback
    from layered_reporter import (  # type: ignore
        Audience,
        EvidenceBundle,
        LayeredReporter,
    )


# =============================================================================
# Normalisation helpers
# =============================================================================

_VALID_FORMATS = ("json", "markdown", "html", "csv")


def _normalise_audience(value: Audience | str) -> Audience:
    """Return the :class:`Audience` for ``value``."""
    if isinstance(value, Audience):
        return value
    if isinstance(value, str):
        try:
            return Audience(value.lower().strip())
        except ValueError as exc:
            raise ValueError(
                f"unknown audience {value!r}: {exc}"
            ) from exc
    raise TypeError(
        f"audience must be Audience or str, "
        f"got {type(value).__name__}."
    )


def _normalise_format(value: str) -> str:
    """Return the lowercase format name for ``value``."""
    if not isinstance(value, str):
        raise TypeError(
            f"format must be a str, got {type(value).__name__}."
        )
    fmt = value.lower().strip()
    if fmt not in _VALID_FORMATS:
        raise ValueError(
            f"unknown format {value!r}; "
            f"expected one of {list(_VALID_FORMATS)}."
        )
    return fmt


def _coerce_bundle(
    bundle: EvidenceBundle | Mapping[str, Any],
) -> EvidenceBundle:
    """Accept an :class:`EvidenceBundle` or a ``to_dict()`` payload."""
    if isinstance(bundle, EvidenceBundle):
        return bundle
    if isinstance(bundle, Mapping):
        try:
            return EvidenceBundle.from_dict(bundle)
        except Exception as exc:
            raise ValueError(
                f"could not reconstruct EvidenceBundle from payload: "
                f"{exc}"
            ) from exc
    raise TypeError(
        f"bundle must be EvidenceBundle or Mapping, "
        f"got {type(bundle).__name__}."
    )


# =============================================================================
# Markdown escaping
# =============================================================================

_MD_ESCAPE_RE = re.compile(r"([\\`*_{}\[\]()#+\-.!|>])")


def _md(value: Any) -> str:
    """
    Escape Markdown metacharacters in a user-controlled value.

    Applied to block-context strings (paragraphs, list items, headings).
    Line breaks are collapsed to spaces so a single value cannot inject
    a heading or list.
    """
    if value is None:
        return ""
    s = str(value).replace("\r\n", " ").replace("\n", " ")
    return _MD_ESCAPE_RE.sub(r"\\\1", s)


def _md_inline(value: Any) -> str:
    """Escape for inline code spans (no newlines, escape backticks)."""
    if value is None:
        return ""
    s = str(value).replace("\r\n", " ").replace("\n", " ")
    return s.replace("`", "\\`")


# =============================================================================
# Original ReportGenerator — preserved + extended
# =============================================================================

class ReportGenerator:
    """
    Original string-reporting class, extended with bundle-aware methods.
    """

    def __init__(self, reporter: Optional[LayeredReporter] = None):
        self.reporter = reporter or LayeredReporter()

    # ------------------------------------------------------------------
    # ORIGINAL methods — preserved verbatim
    # ------------------------------------------------------------------

    def generate_executive_summary(
        self, full_report: Dict[str, Any]
    ) -> str:
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

    def generate_technical_report(
        self, full_report: Dict[str, Any]
    ) -> str:
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

    def generate_research_report(
        self, full_report: Dict[str, Any]
    ) -> str:
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
        bundle: EvidenceBundle | Mapping[str, Any],
        audience: Audience | str,
    ) -> EvidenceBundle:
        """
        Redact ``bundle`` for ``audience``.

        Accepts an :class:`EvidenceBundle` or a ``to_dict()`` payload.
        """
        bundle_obj = _coerce_bundle(bundle)
        aud = _normalise_audience(audience)
        return self.reporter.redact(bundle_obj, aud)

    def redact(
        self,
        bundle: EvidenceBundle | Mapping[str, Any],
        audience: Audience | str,
    ) -> EvidenceBundle:
        """
        Passthrough to :meth:`LayeredReporter.redact`, so callers do not
        need a reference to the internal reporter.
        """
        return self.select_audience(bundle, audience)

    def render_bundle(
        self,
        bundle: EvidenceBundle | Mapping[str, Any],
        *,
        audience: Audience | str = Audience.TECHNICAL,
        format: str = "markdown",
    ) -> str:
        """
        Render a bundle for the requested audience and format.

        Formats: ``"json"``, ``"markdown"``, ``"html"``, ``"csv"``
        (case-insensitive). The bundle is redacted for ``audience``
        before rendering.
        """
        bundle_obj = _coerce_bundle(bundle)
        aud = _normalise_audience(audience)
        fmt = _normalise_format(format)

        redacted = self.reporter.redact(bundle_obj, aud)

        renderer = _RENDERERS[fmt]
        return renderer(redacted, aud)

    def render_many(
        self,
        bundles: Iterable[EvidenceBundle | Mapping[str, Any]],
        *,
        audience: Audience | str = Audience.TECHNICAL,
        format: str = "markdown",
    ) -> str:
        """
        Render a sequence of bundles into a single document.

        Every bundle is redacted for ``audience`` **before** rendering,
        regardless of format. Empty input produces ``"[]"`` for JSON,
        a header-only CSV, and ``""`` for Markdown/HTML.
        """
        aud = _normalise_audience(audience)
        fmt = _normalise_format(format)

        redacted: List[EvidenceBundle] = [
            self.reporter.redact(_coerce_bundle(b), aud)
            for b in bundles
        ]

        if fmt == "json":
            payload: List[Dict[str, Any]] = []
            for b in redacted:
                item = b.to_dict()
                item["_audience"] = aud.value
                payload.append(item)
            return json.dumps(
                payload,
                indent=2,
                sort_keys=True,
                allow_nan=False,
                ensure_ascii=True,
            )

        if fmt == "csv":
            return CSVRenderer.render_many(redacted)

        if fmt == "html":
            body = "\n<hr>\n".join(
                HTMLRenderer.render_fragment(b, aud)
                for b in redacted
            )
            return _wrap_html_document(
                f"Report — {len(redacted)} bundle(s)", body,
            )

        # markdown
        if not redacted:
            return ""
        parts = [
            MarkdownRenderer.render(b, aud) for b in redacted
        ]
        return "\n\n---\n\n".join(parts)


# =============================================================================
# Renderer dispatch
# =============================================================================

_RENDERERS = {
    "json": lambda b, a: JSONRenderer.render(b, a),
    "markdown": lambda b, a: MarkdownRenderer.render(b, a),
    "html": lambda b, a: HTMLRenderer.render(b, a),
    "csv": lambda b, a: CSVRenderer.render(b, a),
}


# =============================================================================
# Renderers
# =============================================================================

class JSONRenderer:
    """Machine-readable JSON output."""

    @staticmethod
    def render(bundle: EvidenceBundle, audience: Audience) -> str:
        payload = bundle.to_dict()
        payload["_audience"] = audience.value
        return json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            allow_nan=False,
            ensure_ascii=True,
        )


class MarkdownRenderer:
    """Human-readable Markdown output (injection-safe)."""

    @staticmethod
    def render(bundle: EvidenceBundle, audience: Audience) -> str:
        d = bundle.to_dict()
        aud = audience.value

        lines: List[str] = []
        lines.append(f"# Evidence Bundle — {_md(d['run_id'])}")
        lines.append("")
        lines.append(f"**Audience**: {_md(aud)}")
        lines.append(f"**System version**: {_md(d['system_version'])}")
        lines.append(f"**Policy version**: {_md(d['policy_version'])}")
        lines.append(f"**Timestamp**: {_md(d['timestamp'])}")
        lines.append(f"**Bundle version**: {_md(d['bundle_version'])}")
        if d.get("content_hash"):
            lines.append(
                f"**Content hash**: `{_md_inline(d['content_hash'])}`"
            )
        if d.get("signature"):
            signer = d.get("signer") or ""
            lines.append(
                f"**Signature**: `{_md_inline(d['signature'])}` "
                f"(signer: {_md(signer)})"
            )
        lines.append("")

        # --- Operational sustainability ---
        lines.append("## Operational sustainability")
        lines.append("")
        lines.append(f"- **Energy**: {d['energy_kwh']:.6f} kWh")
        lines.append(
            f"- **Operational CO₂e**: "
            f"{d['operational_co2e_kg']:.6f} kg"
        )
        lines.append(f"- **Helium**: {d['helium_units']:.6f} units")
        if d.get("simulated"):
            lines.append("- ⚠️ **Simulated values present**")
        if d.get("chaos_injected"):
            lines.append("- ⚠️ **Chaos-injected**")
        lines.append("")

        # --- Contractual instruments ---
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
                    f"- `{_md_inline(inst.get('instrument_id'))}` "
                    f"({_md(inst.get('kind'))}): "
                    f"{_md(inst.get('quantity'))} "
                    f"{_md(inst.get('unit'))}"
                )
            lines.append("")

        # --- Quality and latency ---
        lines.append("## Quality & latency")
        lines.append("")
        for k, v in (d.get("quality_metrics") or {}).items():
            lines.append(f"- **{_md(k)}**: {_md(v)}")
        for k, v in (d.get("latency_metrics") or {}).items():
            lines.append(f"- **{_md(k)}**: {_md(v)}")
        lines.append("")

        # --- Safety verdict ---
        lines.append("## Safety verdict")
        lines.append("")
        sv = d.get("safety_verdict") or {}
        lines.append(f"- **Verdict**: {_md(sv.get('verdict', 'n/a'))}")
        if sv.get("violations"):
            lines.append(
                f"- **Violations**: {_md(sv['violations'])}"
            )
        lines.append("")

        # --- Verification (audience-gated by upstream redact) ---
        if d.get("verification"):
            lines.append("## Verification evidence")
            lines.append("")
            v = d["verification"]
            lines.append(
                f"- **Verdict**: {_md(v.get('verdict'))}"
            )
            coverage = v.get("coverage_pct", 0) or 0
            try:
                coverage_str = f"{float(coverage):.1%}"
            except (TypeError, ValueError):
                coverage_str = str(coverage)
            lines.append(f"- **Coverage**: {coverage_str}")
            lines.append(
                f"- **Properties checked**: "
                f"{_md(v.get('properties_checked'))}"
            )
            if v.get("counterexamples"):
                lines.append(
                    f"- **Counterexamples**: "
                    f"{len(v['counterexamples'])}"
                )
            lines.append("")

        # --- Explanation card ---
        exp = d.get("explanation") or {}
        if exp:
            lines.append("## Explanation")
            lines.append("")
            lines.append(
                f"- **Selected action**: "
                f"{_md(exp.get('selected_action'))}"
            )
            for r in exp.get("rationale", []) or []:
                lines.append(f"  - {_md(r)}")
            confidence = exp.get("confidence")
            if confidence is not None:
                try:
                    lines.append(
                        f"- **Confidence**: {float(confidence):.2f}"
                    )
                except (TypeError, ValueError):
                    lines.append(
                        f"- **Confidence**: {_md(confidence)}"
                    )
            lines.append("")

        # --- Human review ---
        if d.get("human_review"):
            lines.append("## Human review")
            lines.append("")
            for k, v in d["human_review"].items():
                lines.append(f"- **{_md(k)}**: {_md(v)}")
            lines.append("")

        # --- Provenance ---
        if d.get("metric_provenance"):
            lines.append("## Metric provenance")
            lines.append("")
            for metric, p in d["metric_provenance"].items():
                if isinstance(p, dict):
                    lines.append(
                        f"- **{_md(metric)}**: "
                        f"{_md(p.get('source_kind'))} "
                        f"({_md(p.get('trust_level', '?'))})"
                    )
            lines.append("")

        return "\n".join(lines)


def _wrap_html_document(title: str, body: str) -> str:
    """Wrap ``body`` in a minimal HTML document shell."""
    esc = html.escape
    return (
        "<!doctype html>\n"
        "<html><head><meta charset='utf-8'>"
        f"<title>{esc(title)}</title>"
        "<style>"
        "body{font-family:system-ui,-apple-system,sans-serif;"
        "margin:2em;max-width:80ch}"
        "table{border-collapse:collapse;width:100%}"
        "td,th{border:1px solid #ddd;padding:6px;text-align:left}"
        ".warn{color:#b00;font-weight:bold}"
        "</style></head><body>\n"
        f"{body}\n"
        "</body></html>\n"
    )


class HTMLRenderer:
    """HTML output (safe-escaped, no external dependencies)."""

    @staticmethod
    def render_fragment(
        bundle: EvidenceBundle, audience: Audience
    ) -> str:
        """
        Return the ``<body>`` inner HTML for one bundle (no document
        wrapper). Used by :meth:`ReportGenerator.render_many`.
        """
        d = bundle.to_dict()
        esc = html.escape

        parts: List[str] = []
        parts.append(
            f"<h1>Evidence Bundle — {esc(d['run_id'])}</h1>"
        )
        parts.append(
            f"<p><strong>Audience</strong>: {esc(audience.value)}</p>"
        )
        parts.append(
            f"<p><strong>Policy version</strong>: "
            f"{esc(d['policy_version'])}</p>"
        )
        if d.get("content_hash"):
            parts.append(
                "<p><strong>Content hash</strong>: "
                f"<code>{esc(d['content_hash'])}</code></p>"
            )

        if d.get("simulated"):
            parts.append(
                "<p class='warn'>⚠️ Simulated values present.</p>"
            )
        if d.get("chaos_injected"):
            parts.append(
                "<p class='warn'>⚠️ Chaos-injected run.</p>"
            )

        parts.append("<h2>Operational sustainability</h2>")
        parts.append("<table>")
        parts.append(
            f"<tr><th>Energy (kWh)</th>"
            f"<td>{d['energy_kwh']:.6f}</td></tr>"
        )
        parts.append(
            f"<tr><th>Operational CO₂e (kg)</th>"
            f"<td>{d['operational_co2e_kg']:.6f}</td></tr>"
        )
        parts.append(
            f"<tr><th>Helium (units)</th>"
            f"<td>{d['helium_units']:.6f}</td></tr>"
        )
        parts.append("</table>")

        if d.get("carbon_instruments"):
            parts.append("<h2>Contractual carbon instruments</h2>")
            parts.append(
                "<p><em>Kept separate from operational emissions.</em></p>"
            )
            parts.append("<ul>")
            for inst in d["carbon_instruments"]:
                parts.append(
                    f"<li><code>"
                    f"{esc(str(inst.get('instrument_id')))}</code>: "
                    f"{esc(str(inst.get('quantity')))} "
                    f"{esc(str(inst.get('unit')))}</li>"
                )
            parts.append("</ul>")

        return "\n".join(parts)

    @staticmethod
    def render(bundle: EvidenceBundle, audience: Audience) -> str:
        """Full single-document HTML."""
        return _wrap_html_document(
            f"Evidence Bundle — {bundle.run_id}",
            HTMLRenderer.render_fragment(bundle, audience),
        )


class CSVRenderer:
    """
    CSV output — one row per bundle, with flattened columns.

    The column list is a stable, versioned contract. Consumers that
    parse this CSV should read by column name (the header), not by
    position — new columns are added at the end on a major version
    bump (see ``VERSION``).
    """

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
        q = d.get("quality_metrics") or {}
        l = d.get("latency_metrics") or {}
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
    def render(
        cls, bundle: EvidenceBundle, audience: Audience
    ) -> str:
        buf = io.StringIO()
        writer = csv.DictWriter(
            buf,
            fieldnames=cls._COLUMNS,
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerow(cls._row(bundle))
        return buf.getvalue()

    @classmethod
    def render_many(
        cls, bundles: Iterable[EvidenceBundle]
    ) -> str:
        buf = io.StringIO()
        writer = csv.DictWriter(
            buf,
            fieldnames=cls._COLUMNS,
            lineterminator="\n",
        )
        writer.writeheader()
        for b in bundles:
            writer.writerow(cls._row(b))
        return buf.getvalue()


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":  # pragma: no cover
    import logging as _logging

    try:
        from .layered_reporter import (
            BundleBuilder,
            CarbonInstruments,
            ExplanationCard,
        )
    except ImportError:
        from layered_reporter import (  # type: ignore
            BundleBuilder,
            CarbonInstruments,
            ExplanationCard,
        )

    _logging.basicConfig(level=_logging.INFO)
    print("VERSION:", VERSION)

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
    instruments.add(
        "rec-2026-001", "rec", 5.0, "MWh",
        vintage_year=2026, matching_period="2026-03",
    )

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

    # ---- Original behavior -----------------------------------------
    print("\n=== Original plain-text (executive) ===")
    full = reporter.generate_full_report(result)
    print(gen.generate_executive_summary(full)[:200] + "...")

    # ---- Enhanced single-bundle renders ----------------------------
    print("\n=== Enhanced JSON (technical) ===")
    print(
        gen.render_bundle(
            bundle,
            audience=Audience.TECHNICAL,
            format="json",
        )[:300] + "..."
    )

    print("\n=== Enhanced Markdown (governance) ===")
    print(
        gen.render_bundle(
            bundle,
            audience=Audience.GOVERNANCE,
            format="markdown",
        )[:400] + "..."
    )

    print("\n=== Enhanced CSV (operator) ===")
    print(
        gen.render_bundle(
            bundle,
            audience=Audience.OPERATOR,
            format="csv",
        )
    )

    # ---- String audience / uppercase format ----------------------
    print("=== String audience + uppercase format ===")
    _ = gen.render_bundle(
        bundle, audience="executive", format="JSON",
    )
    print("  accepted string audience + case-insensitive format")

    # ---- Redaction parity ----------------------------------------
    print("\n=== Redaction parity across formats ===")
    for fmt in ("json", "csv", "markdown", "html"):
        out = gen.render_bundle(
            bundle, audience=Audience.EXECUTIVE, format=fmt,
        )
        has_verification = (
            "verification" in out.lower()
            and "null" not in out.lower().split(
                "verification", 1
            )[1][:50]
        )
        print(f"  {fmt:<9} verification leakage: {has_verification}")

    # ---- Batch renders redact consistently -----------------------
    print("\n=== Batch redaction ===")
    exec_json_batch = gen.render_many(
        [bundle], audience=Audience.EXECUTIVE, format="json",
    )
    assert '"verification": null' in exec_json_batch
    assert "abc123" not in exec_json_batch   # artifact hashes redacted
    print("  batch JSON: verification + artifacts redacted")

    # ---- HTML batch is a single document -------------------------
    print("\n=== HTML batch — single document ===")
    html_batch = gen.render_many(
        [bundle, bundle], format="html",
    )
    assert html_batch.count("<!doctype html>") == 1
    print("  single <!doctype html> root confirmed")

    # ---- Markdown injection defence ------------------------------
    print("\n=== Markdown injection defence ===")
    evil = {
        **result,
        "run_id": "bad\n## INJECTED\n# Heading",
    }
    evil_bundle = BundleBuilder.build(evil)
    md = gen.render_bundle(
        evil_bundle, audience=Audience.TECHNICAL, format="markdown",
    )
    assert "## INJECTED" not in md
    print("  heading injection blocked")

    # ---- Confidence 0.0 renders ----------------------------------
    print("\n=== ExplanationCard confidence 0.0 ===")
    zero_conf_bundle = BundleBuilder.build(
        result,
        explanation=ExplanationCard(
            decision_id="d", selected_action="a", confidence=0.0,
        ),
    )
    md = gen.render_bundle(
        zero_conf_bundle,
        audience=Audience.TECHNICAL,
        format="markdown",
    )
    assert "Confidence**: 0.00" in md
    print("  zero confidence rendered")

    # ---- Bundle-as-Mapping ---------------------------------------
    print("\n=== render_bundle from dict payload ===")
    payload = bundle.to_dict()
    out = gen.render_bundle(
        payload, audience=Audience.TECHNICAL, format="markdown",
    )
    assert "Evidence Bundle" in out
    print("  dict payload accepted")

    # ---- Error surfaces ------------------------------------------
    print("\n=== Error surfaces ===")
    for case, fn in (
        ("bogus format", lambda: gen.render_bundle(
            bundle, format="xml",
        )),
        ("non-str format", lambda: gen.render_bundle(
            bundle, format=None,  # type: ignore[arg-type]
        )),
        ("bogus audience", lambda: gen.render_bundle(
            bundle, audience="wizard",
        )),
        ("non-str audience", lambda: gen.render_bundle(
            bundle, audience=42,  # type: ignore[arg-type]
        )),
    ):
        try:
            fn()
        except (ValueError, TypeError) as exc:
            print(f"  {case:<16}: {type(exc).__name__}")

    print("\nSmoke test passed.")
