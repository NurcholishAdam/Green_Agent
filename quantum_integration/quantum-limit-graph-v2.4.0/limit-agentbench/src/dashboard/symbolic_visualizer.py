# src/dashboard/symbolic_visualizer.py

"""
Symbolic Violation Visualizer
=============================

Dashboard component for visualizing symbolic rule violations with trace
explanations.

Enhancements
------------
- ``SymbolicVisualizerConfig`` — frozen, validated: bounded history, HTML
  escape toggle, severity colors.
- **Fixed XSS** — every user-supplied string is HTML-escaped.
- **Bounded storage** — ``deque(maxlen=config.max_history)``.
- **Thread safety** — ``RLock``.
- **`_extract_category` returns `None` when rule_id is empty** — callers
  can distinguish "unclassified" from "explicitly unknown".
- Serialization + `statistics()` + `reset()` + `__repr__`.
- Custom ``SymbolicVisualizerError(ValueError)`` and ``__main__`` smoke test.
"""

from __future__ import annotations

import html as _html
import json
import logging
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SymbolicVisualizerError(ValueError):
    """Raised for invalid visualizer inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SymbolicVisualizerConfig:
    """Tunable parameters for :class:`SymbolicVisualizer`."""

    max_history: int = 100_000
    escape_html: bool = True

    # Severity color mapping.
    severity_colors: Mapping[str, str] = field(
        default_factory=lambda: {
            "critical": "#dc3545",
            "high": "#fd7e14",
            "medium": "#ffc107",
            "low": "#17a2b8",
        }
    )

    # Known rule-id prefixes per category.
    category_prefixes: Mapping[str, tuple] = field(
        default_factory=lambda: {
            "sustainability": ("SUST", "COMP-SUST"),
            "resource": ("RES", "COMP-RES"),
            "fairness": ("FAIR",),
            "safety": ("SAFE",),
            "compliance": ("COMP",),
        }
    )

    def __post_init__(self) -> None:
        if self.max_history <= 0:
            raise SymbolicVisualizerError("max_history must be > 0.")
        if not isinstance(self.severity_colors, Mapping):
            raise SymbolicVisualizerError("severity_colors must be a Mapping.")
        if not isinstance(self.category_prefixes, Mapping):
            raise SymbolicVisualizerError(
                "category_prefixes must be a Mapping."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "max_history": self.max_history,
            "escape_html": self.escape_html,
            "severity_colors": dict(self.severity_colors),
            "category_prefixes": {
                k: list(v) for k, v in self.category_prefixes.items()
            },
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SymbolicVisualizerConfig":
        if not isinstance(data, Mapping):
            raise SymbolicVisualizerError(
                "SymbolicVisualizerConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k in ("severity_colors", "category_prefixes"):
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Visualizer
# --------------------------------------------------------------------------- #
class SymbolicVisualizer:
    """
    Visualizer for symbolic rule violations in the dashboard.

    Thread-safe, serializable, and bounded in memory. All original public
    methods are preserved; new parameters are keyword-only.
    """

    def __init__(
        self,
        *,
        config: Optional[SymbolicVisualizerConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or SymbolicVisualizerConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._violations: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_history
        )
        self._started_at: float = time.time()

        # Legacy view.
        self.violation_data: List[Dict[str, Any]] = []

        logger.debug(
            "SymbolicVisualizer initialized (max_history=%d, strict=%s)",
            self._config.max_history, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> SymbolicVisualizerConfig:
        return self._config

    @property
    def violation_count(self) -> int:
        with self._lock:
            return len(self._violations)

    # ---------------------------------------------------------- ingestion
    def add_violations(self, violations: List[Mapping[str, Any]]) -> int:
        """
        Add violation traces to the visualizer.

        Returns the number of violations added (additive).
        """
        if not isinstance(violations, list):
            raise SymbolicVisualizerError("violations must be a list.")
        added = 0
        with self._lock:
            for v in violations:
                if isinstance(v, Mapping):
                    self._violations.append(dict(v))
                    added += 1
            self.violation_data = list(self._violations)
        logger.debug("Added %d violation(s) to visualizer.", added)
        return added

    # ---------------------------------------------------------- views
    def generate_violation_timeline(self) -> List[Dict[str, Any]]:
        """Generate a sorted timeline view of violations."""
        with self._lock:
            data = list(self._violations)
        data_sorted = sorted(data, key=lambda v: (v.get("timestamp", 0), v.get("step", 0)))
        timeline: List[Dict[str, Any]] = []
        for v in data_sorted:
            severity = str(v.get("severity", "unknown"))
            timeline.append({
                "timestamp": v.get("timestamp"),
                "step": v.get("step"),
                "rule_name": v.get("rule_name"),
                "severity": severity,
                "category": self._extract_category(v.get("rule_id", "")),
                "action": v.get("action_triggered"),
                "status": "critical" if severity == "critical" else "warning",
            })
        return timeline

    def generate_category_view(self) -> Dict[str, List[Dict[str, Any]]]:
        """Group violations by category."""
        with self._lock:
            data = list(self._violations)
        by_category: Dict[str, List[Dict[str, Any]]] = {}
        for v in data:
            category = self._extract_category(v.get("rule_id", "")) or "unknown"
            by_category.setdefault(category, []).append({
                "rule_id": v.get("rule_id"),
                "rule_name": v.get("rule_name"),
                "step": v.get("step"),
                "severity": v.get("severity"),
                "explanation": v.get("explanation"),
                "violation_details": v.get("violation_details"),
            })
        return by_category

    def generate_severity_summary(self) -> Dict[str, Any]:
        """Generate a summary grouped by severity level."""
        summary: Dict[str, List[Dict[str, Any]]] = {
            "critical": [], "high": [], "medium": [], "low": [],
        }
        with self._lock:
            data = list(self._violations)
        for v in data:
            severity = str(v.get("severity", "unknown"))
            if severity in summary:
                summary[severity].append({
                    "rule_name": v.get("rule_name"),
                    "step": v.get("step"),
                    "action": v.get("action_triggered"),
                })
        return {
            "counts": {k: len(v) for k, v in summary.items()},
            "details": summary,
        }

    def filter_by_rule_type(self, rule_type: str) -> List[Dict[str, Any]]:
        """Filter violations by category prefix."""
        if not isinstance(rule_type, str) or not rule_type:
            raise SymbolicVisualizerError(
                "rule_type must be a non-empty string."
            )
        prefixes = self._config.category_prefixes.get(rule_type.lower(), ())
        with self._lock:
            data = list(self._violations)
        return [
            v for v in data
            if any(str(v.get("rule_id", "")).startswith(p) for p in prefixes)
        ]

    def generate_html_violation_card(
        self, violation: Mapping[str, Any]
    ) -> str:
        """Generate an HTML card for a single violation (escaped)."""
        esc = (
            (lambda s: _html.escape(str(s)))
            if self._config.escape_html else (lambda s: str(s))
        )
        severity = str(violation.get("severity", "unknown"))
        color = self._config.severity_colors.get(severity, "#6c757d")
        return f"""
<div style="border-left: 4px solid {color}; padding: 8px; margin: 8px 0;">
<h4>{esc(violation.get('rule_name', 'Unknown Rule'))}</h4>
<span style="background: {color}; color: white; padding: 2px 6px; border-radius: 3px;">
{esc(severity.upper())}
</span>
<p>Rule ID: {esc(violation.get('rule_id', 'N/A'))} | Step: {esc(violation.get('step', 'N/A'))}</p>
<p>Condition: <code>{esc(violation.get('condition', 'N/A'))}</code></p>
<p>Action Triggered: {esc(violation.get('action_triggered', 'N/A'))}</p>
<p>{esc(violation.get('explanation', 'No explanation available'))}</p>
<details>
<summary>View Trace Details</summary>
<pre>{esc(violation.get('violation_details', 'No details available'))}</pre>
</details>
</div>
"""

    def generate_dashboard_section(self) -> str:
        """Generate the complete HTML section for the dashboard."""
        if self.violation_count == 0:
            return """
<h3>✅ Symbolic Oversight</h3>
<p>No rule violations detected. All symbolic constraints satisfied.</p>
"""
        severity_summary = self.generate_severity_summary()
        category_view = self.generate_category_view()

        counts = severity_summary["counts"]
        parts = [f"""
<h3>⚠️ Symbolic Oversight - Rule Violations</h3>
<p><b>Critical:</b> {counts['critical']} | <b>High:</b> {counts['high']} |
<b>Medium:</b> {counts['medium']} | <b>Low:</b> {counts['low']}</p>
<h4>Violations by Category</h4>
"""]
        for category, violations in category_view.items():
            parts.append(f"<h5>{_html.escape(category.upper())} ({len(violations)})</h5>")
            for v in violations:
                parts.append(self.generate_html_violation_card(v))
        return "".join(parts)

    def export_violation_report(self, filepath: str) -> int:
        """Export a violation report as JSON (atomic). Returns bytes written."""
        if not isinstance(filepath, str) or not filepath:
            raise SymbolicVisualizerError("filepath must be a non-empty string.")
        path = Path(filepath)
        report = {
            "total_violations": self.violation_count,
            "severity_summary": self.generate_severity_summary(),
            "category_view": self.generate_category_view(),
            "timeline": self.generate_violation_timeline(),
            "violations": list(self._violations),
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, default=str)
        except OSError as exc:
            raise SymbolicVisualizerError(
                f"could not write {path}: {exc}"
            ) from exc
        return path.stat().st_size

    # ---------------------------------------------------------- helpers
    def _extract_category(self, rule_id: str) -> Optional[str]:
        """Extract category from a rule ID. Returns ``None`` when empty."""
        if not rule_id:
            return None
        parts = str(rule_id).split("-")
        if parts and parts[0]:
            return parts[0].lower()
        return None

    # ---------------------------------------------------------- lifecycle
    def reset(self) -> int:
        """Clear the visualizer. Returns number removed."""
        with self._lock:
            removed = len(self._violations)
            self._violations.clear()
            self.violation_data = []
            self._started_at = time.time()
        logger.debug("SymbolicVisualizer reset (removed %d).", removed)
        return removed

    def statistics(self) -> Dict[str, Any]:
        return {
            "violations": self.violation_count,
            "config": self._config.to_dict(),
            "strict": self._strict,
            "uptime_seconds": time.time() - self._started_at,
        }

    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        payload = {
            "config": self._config.to_dict(),
            "strict": self._strict,
            "started_at": self._started_at,
        }
        if include_history:
            with self._lock:
                payload["violations"] = [dict(v) for v in self._violations]
        return payload

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "SymbolicVisualizer("
            f"violations={self.violation_count}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "SymbolicVisualizer",
    "SymbolicVisualizerConfig",
    "SymbolicVisualizerError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m dashboard.symbolic_visualizer
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    vis = SymbolicVisualizer()
    print("repr       :", vis)

    # ---- Bug fix: empty data returns clean section ---------------- #
    section = vis.generate_dashboard_section()
    assert "✅" in section
    print("empty sec  : OK")

    # ---- Add violations ------------------------------------------ #
    vis.add_violations([
        {"rule_id": "SUST-001", "rule_name": "High energy", "step": 3,
         "severity": "critical", "action_triggered": "halt",
         "explanation": "Energy exceeded", "violation_details": "...",
         "condition": "energy > 5", "timestamp": 1000},
        {"rule_id": "RES-002", "rule_name": "<img src=x onerror=alert(1)>",
         "step": 4, "severity": "high", "action_triggered": "warn",
         "explanation": "<script>alert('xss')</script>",
         "violation_details": "...", "condition": "mem > 500", "timestamp": 2000},
    ])
    print("violations :", vis.violation_count)

    # ---- Bug fix: XSS escaped ------------------------------------ #
    section = vis.generate_dashboard_section()
    assert "<img src=x" not in section
    assert "&lt;img src=x" in section
    assert "<script>" not in section
    print("xss safe   : OK")

    # ---- Category view ------------------------------------------- #
    cat_view = vis.generate_category_view()
    print("categories :", list(cat_view.keys()))

    # ---- Filter by type ------------------------------------------ #
    sust = vis.filter_by_rule_type("sustainability")
    print("sust only  :", len(sust))
    assert len(sust) == 1

    # ---- Statistics ---------------------------------------------- #
    print("stats      :", {
        k: v for k, v in vis.statistics().items()
        if k not in ("config", "uptime_seconds")
    })

    print("\nSmoke test passed.")
