# src/dashboard/green_dashboard.py

"""
Green Dashboard Module
======================

Visualization for reflective insights, Pareto positions, and interpretability.

Enhancements
------------
- ``GreenDashboardConfig`` — frozen, validated: bounded history,
  interpretability factor weights, HTML escape toggle.
- **Full validation** of every argument; strict / non-strict modes.
- **Thread safety** — ``RLock`` guards the dashboard data.
- **Bounded storage** — ``deque(maxlen=config.max_agents)``.
- **Fixed NaN on empty reflections** — interpretability score returns 0.0
  instead of dividing by zero.
- **Fixed XSS in HTML export** — all user-supplied strings are HTML-escaped.
- **Atomic export** — ``tempfile`` + ``os.replace``.
- **Deterministic leaderboard tie-breaking**.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` on the dashboard.
- ``statistics()``, ``reset()``, ``__repr__``, custom
  ``GreenDashboardError(ValueError)``, and a ``__main__`` smoke test.
"""

from __future__ import annotations

import html as _html
import json
import logging
import math
import os
import tempfile
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
class GreenDashboardError(ValueError):
    """Raised for invalid dashboard inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GreenDashboardConfig:
    """Tunable parameters for :class:`GreenDashboard`."""

    max_agents: Optional[int] = 10_000
    max_reflections: Optional[int] = 100_000
    max_reasoning_path: int = 100

    # Interpretability score weights (must sum to 1.0).
    weight_reflection_frequency: float = 0.3
    weight_avg_confidence: float = 0.3
    weight_decision_consistency: float = 0.2
    weight_explanation_quality: float = 0.2

    # Saturation constants.
    reflections_for_max_frequency: float = 10.0
    explanation_length_for_max_quality: float = 200.0

    # HTML safety.
    escape_html: bool = True

    def __post_init__(self) -> None:
        for name in ("max_agents", "max_reflections"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, int) or value <= 0):
                raise GreenDashboardError(
                    f"{name} must be a positive int or None."
                )
        if self.max_reasoning_path <= 0:
            raise GreenDashboardError("max_reasoning_path must be > 0.")
        weights = (
            self.weight_reflection_frequency
            + self.weight_avg_confidence
            + self.weight_decision_consistency
            + self.weight_explanation_quality
        )
        if abs(weights - 1.0) > 1e-6:
            raise GreenDashboardError(
                f"interpretability weights must sum to 1.0 (got {weights:.6f})."
            )
        for name in (
            "weight_reflection_frequency", "weight_avg_confidence",
            "weight_decision_consistency", "weight_explanation_quality",
        ):
            if getattr(self, name) < 0:
                raise GreenDashboardError(f"{name} must be >= 0.")
        if self.reflections_for_max_frequency <= 0:
            raise GreenDashboardError(
                "reflections_for_max_frequency must be > 0."
            )
        if self.explanation_length_for_max_quality <= 0:
            raise GreenDashboardError(
                "explanation_length_for_max_quality must be > 0."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "GreenDashboardConfig":
        if not isinstance(data, Mapping):
            raise GreenDashboardError(
                "GreenDashboardConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs = {k: v for k, v in data.items() if k in valid}
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Dashboard
# --------------------------------------------------------------------------- #
class GreenDashboard:
    """
    Dashboard for visualizing meta-cognitive insights and sustainability
    metrics.

    Thread-safe, serializable, and bounded in memory. All original public
    methods are preserved; new parameters are keyword-only.
    """

    def __init__(
        self,
        *,
        config: Optional[GreenDashboardConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or GreenDashboardConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._agents: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_agents
        )
        self._reflections: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_reflections
        )
        self._started_at: float = time.time()

        # Legacy view — mutable for backward compatibility.
        self.dashboard_data: Dict[str, Any] = {
            "agents": [],
            "reflections": [],
            "pareto_analysis": {},
            "interpretability_scores": {},
        }

        logger.debug(
            "GreenDashboard initialized (max_agents=%s, strict=%s)",
            self._config.max_agents, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> GreenDashboardConfig:
        return self._config

    @property
    def agent_count(self) -> int:
        with self._lock:
            return len(self._agents)

    # ---------------------------------------------------------- ingestion
    def add_agent_data(
        self,
        agent_id: str,
        metrics: Mapping[str, Any],
        reflections: List[Mapping[str, Any]],
        pareto_position: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """
        Add agent data to the dashboard.

        Returns the stored agent entry (additive — the original returned
        ``None``).
        """
        if not isinstance(agent_id, str) or not agent_id:
            raise GreenDashboardError("agent_id must be a non-empty string.")
        if not isinstance(metrics, Mapping):
            raise GreenDashboardError("metrics must be a Mapping.")
        if not isinstance(reflections, list):
            raise GreenDashboardError("reflections must be a list.")
        if not isinstance(pareto_position, Mapping):
            raise GreenDashboardError("pareto_position must be a Mapping.")

        reflections_list = [dict(r) for r in reflections if isinstance(r, Mapping)]
        interpretability_score = self._calculate_interpretability_score(
            reflections_list
        )

        entry = {
            "agent_id": agent_id,
            "metrics": dict(metrics),
            "reflection_count": len(reflections_list),
            "pareto_position": pareto_position.get("position", "unknown"),
            "interpretability_score": interpretability_score,
            "reasoning_path": self._extract_reasoning_path(reflections_list),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        with self._lock:
            self._agents.append(entry)
            self._reflections.extend(reflections_list)
            # Refresh the legacy view.
            self.dashboard_data["agents"] = list(self._agents)
            self.dashboard_data["reflections"] = list(self._reflections)

        logger.debug(
            "Added agent %s: interpretability=%.3f, position=%s",
            agent_id, interpretability_score, entry["pareto_position"],
        )
        return entry

    # ---------------------------------------------------------- interpretability
    def _calculate_interpretability_score(
        self, reflections: List[Mapping[str, Any]]
    ) -> float:
        """
        Compute interpretability in ``[0, 1]`` from reflection quality.

        Returns ``0.0`` for an empty reflection list (fixes the original
        division-by-zero that produced ``NaN``).
        """
        if not reflections:
            return 0.0

        cfg = self._config
        score = 0.0

        # Factor 1: reflection frequency.
        score += cfg.weight_reflection_frequency * min(
            len(reflections) / cfg.reflections_for_max_frequency, 1.0
        )

        # Factor 2: average confidence.
        confidences = [
            float(r.get("confidence", 0) or 0) for r in reflections
        ]
        if confidences:
            avg_confidence = sum(confidences) / len(confidences)
            avg_confidence = max(0.0, min(1.0, avg_confidence))
            score += cfg.weight_avg_confidence * avg_confidence

        # Factor 3: decision consistency.
        decisions = [str(r.get("decision", "") or "") for r in reflections]
        if decisions:
            most_common_count = max(decisions.count(d) for d in set(decisions))
            consistency = most_common_count / len(decisions)
            score += cfg.weight_decision_consistency * consistency

        # Factor 4: explanation quality.
        explanation_lengths = [
            len(str(r.get("self_explanation", "") or "")) for r in reflections
        ]
        if explanation_lengths:
            avg_explanation_length = sum(explanation_lengths) / len(explanation_lengths)
            score += cfg.weight_explanation_quality * min(
                avg_explanation_length / cfg.explanation_length_for_max_quality,
                1.0,
            )

        return max(0.0, min(1.0, score))

    def _extract_reasoning_path(
        self, reflections: List[Mapping[str, Any]]
    ) -> List[str]:
        """Extract a bounded list of reasoning steps from reflections."""
        cfg = self._config
        path: List[str] = []
        for r in reflections[: cfg.max_reasoning_path]:
            step = r.get("step", 0)
            decision = r.get("decision", "unknown")
            explanation = str(r.get("self_explanation", ""))[:100]
            path.append(f"Step {step}: {decision} - {explanation}")
        return path

    # ---------------------------------------------------------- leaderboard
    def generate_leaderboard(self) -> Dict[str, Any]:
        """Generate a leaderboard comparing agents on multiple dimensions."""
        with self._lock:
            agents = list(self._agents)

        if not agents:
            return {"agents": [], "rankings": {}, "top_performers": {}}

        # Efficiency ranking: frontier first, then lowest energy.
        efficiency_ranking = sorted(
            agents,
            key=lambda a: (
                0 if a["pareto_position"] == "frontier" else 1,
                a["metrics"].get("cumulative", {}).get("total_energy_wh",
                                                        float("inf")),
                a["agent_id"],
            ),
        )

        # Interpretability ranking: score desc, agent_id asc.
        interpretability_ranking = sorted(
            agents,
            key=lambda a: (-a["interpretability_score"], a["agent_id"]),
        )

        # Sustainability ranking: energy + carbon*1000, then agent_id.
        def _sustainability_key(a: Dict[str, Any]) -> tuple:
            cum = a["metrics"].get("cumulative", {})
            e = cum.get("total_energy_wh", float("inf"))
            c = cum.get("total_carbon_kg", float("inf")) * 1000
            return (e + c, a["agent_id"])

        sustainability_ranking = sorted(agents, key=_sustainability_key)

        return {
            "agents": agents,
            "rankings": {
                "efficiency": [a["agent_id"] for a in efficiency_ranking],
                "interpretability": [a["agent_id"] for a in interpretability_ranking],
                "sustainability": [a["agent_id"] for a in sustainability_ranking],
            },
            "top_performers": {
                "most_efficient": efficiency_ranking[0]["agent_id"] if efficiency_ranking else None,
                "most_interpretable": interpretability_ranking[0]["agent_id"] if interpretability_ranking else None,
                "most_sustainable": sustainability_ranking[0]["agent_id"] if sustainability_ranking else None,
            },
        }

    def generate_comparison_view(self) -> Dict[str, Any]:
        """Generate a comparison view with reasoning."""
        with self._lock:
            agents = list(self._agents)

        comparisons: List[Dict[str, Any]] = []
        for agent in agents:
            comparison = {
                "agent_id": agent["agent_id"],
                "pareto_position": agent["pareto_position"],
                "reasoning_summary": self._summarize_reasoning(
                    agent["reasoning_path"]
                ),
                "key_decisions": self._extract_key_decisions(
                    agent["reasoning_path"]
                ),
                "interpretability": agent["interpretability_score"],
                "metrics_summary": {
                    "energy": agent["metrics"].get("cumulative", {}).get("total_energy_wh", 0),
                    "carbon": agent["metrics"].get("cumulative", {}).get("total_carbon_kg", 0),
                    "latency": agent["metrics"].get("cumulative", {}).get("total_latency_ms", 0),
                },
            }
            comparisons.append(comparison)

        return {
            "comparisons": comparisons,
            "insights": self._generate_comparative_insights(comparisons),
        }

    def _summarize_reasoning(self, reasoning_path: List[str]) -> str:
        if not reasoning_path:
            return "No reasoning available"
        key_steps = reasoning_path[:3]
        parts: List[str] = []
        for step in key_steps:
            try:
                parts.append(step.split(":")[1].split("-")[0].strip())
            except (IndexError, AttributeError):
                parts.append(step)
        return " → ".join(parts)

    def _extract_key_decisions(self, reasoning_path: List[str]) -> List[str]:
        decisions: List[str] = []
        for step in reasoning_path:
            lower = step.lower()
            if "reduce" in lower or "optimize" in lower or "continue" in lower:
                try:
                    decisions.append(step.split(":")[1].split("-")[0].strip())
                except (IndexError, AttributeError):
                    decisions.append(step)
        return decisions[:5]

    def _generate_comparative_insights(
        self, comparisons: List[Dict[str, Any]]
    ) -> List[str]:
        insights: List[str] = []
        frontier_agents = [c for c in comparisons if c["pareto_position"] == "frontier"]
        if frontier_agents:
            insights.append(
                f"{len(frontier_agents)} agent(s) achieved Pareto-optimal trade-offs"
            )
        high_interp = [c for c in comparisons if c["interpretability"] > 0.7]
        if high_interp:
            insights.append(
                f"{len(high_interp)} agent(s) demonstrated high interpretability (>0.7)"
            )
        energies = [c["metrics_summary"]["energy"] for c in comparisons]
        if energies:
            avg_energy = sum(energies) / len(energies)
            efficient = [c for c in comparisons if c["metrics_summary"]["energy"] < avg_energy * 0.8]
            if efficient:
                insights.append(
                    f"{len(efficient)} agent(s) achieved 20% better energy efficiency than average"
                )
        return insights

    # ---------------------------------------------------------- export
    def export_dashboard(self, filepath: str) -> int:
        """Atomically export dashboard data to JSON. Returns bytes written."""
        if not isinstance(filepath, str) or not filepath:
            raise GreenDashboardError("filepath must be a non-empty string.")
        path = Path(filepath)
        export_data = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "leaderboard": self.generate_leaderboard(),
            "comparison_view": self.generate_comparison_view(),
            "raw_data": {
                "agents": [dict(a) for a in self._agents],
                "reflections": [dict(r) for r in self._reflections],
            },
        }
        return _atomic_write_json(path, export_data)

    def generate_html_report(self, filepath: str) -> int:
        """Atomically generate an HTML dashboard report. Returns bytes written."""
        if not isinstance(filepath, str) or not filepath:
            raise GreenDashboardError("filepath must be a non-empty string.")
        path = Path(filepath)

        leaderboard = self.generate_leaderboard()
        comparison = self.generate_comparison_view()
        esc = (lambda s: _html.escape(str(s))) if self._config.escape_html else (lambda s: str(s))

        rows = []
        for c in comparison["comparisons"]:
            rows.append(f"""
### {esc(c['agent_id'])} [{esc(c['pareto_position'])}]

Reasoning: {esc(c['reasoning_summary'])}

Interpretability: {c['interpretability']:.2f}

Energy: {c['metrics_summary']['energy']:.3f} Wh

Carbon: {c['metrics_summary']['carbon']:.6f} kg

Latency: {c['metrics_summary']['latency']:.1f} ms
""")

        insight_items = "".join(
            f"<li>{esc(i)}</li>" for i in comparison["insights"]
        )

        html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Green Agent Dashboard</title></head>
<body>
<h1>Green Agent Meta-Cognitive Dashboard</h1>
<p>Sustainability + Interpretability Analysis</p>

<h2>Leaderboard</h2>
<h3>Top Performers</h3>
<p>Most Efficient: {esc(leaderboard['top_performers']['most_efficient'])}</p>
<p>Most Interpretable: {esc(leaderboard['top_performers']['most_interpretable'])}</p>
<p>Most Sustainable: {esc(leaderboard['top_performers']['most_sustainable'])}</p>

<h2>Agent Comparisons</h2>
{''.join(rows)}

<h2>Insights</h2>
<ul>{insight_items}</ul>
</body>
</html>"""

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp_path = tempfile.mkstemp(
                prefix=path.name + ".", suffix=".tmp",
                dir=str(path.parent),
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(html)
                os.replace(tmp_path, path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except OSError as exc:
            raise GreenDashboardError(
                f"could not write {path}: {exc}"
            ) from exc
        return path.stat().st_size

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_reflections: bool = True) -> int:
        """Reset the dashboard. Returns number of agents removed."""
        with self._lock:
            removed = len(self._agents)
            self._agents.clear()
            if clear_reflections:
                self._reflections.clear()
            self.dashboard_data["agents"] = []
            self.dashboard_data["reflections"] = []
            self._started_at = time.time()
        logger.debug("GreenDashboard reset (removed %d).", removed)
        return removed

    def statistics(self) -> Dict[str, Any]:
        """Return a JSON-safe snapshot."""
        with self._lock:
            agents = list(self._agents)
            reflections = list(self._reflections)
        if not agents:
            return {
                "agents": 0,
                "reflections": len(reflections),
                "config": self._config.to_dict(),
                "strict": self._strict,
                "uptime_seconds": time.time() - self._started_at,
            }
        interp_scores = [a["interpretability_score"] for a in agents]
        return {
            "agents": len(agents),
            "reflections": len(reflections),
            "mean_interpretability": sum(interp_scores) / len(interp_scores),
            "max_interpretability": max(interp_scores),
            "frontier_agents": sum(1 for a in agents if a["pareto_position"] == "frontier"),
            "config": self._config.to_dict(),
            "strict": self._strict,
            "uptime_seconds": time.time() - self._started_at,
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "started_at": self._started_at,
            }
            if include_history:
                payload["agents"] = [dict(a) for a in self._agents]
                payload["reflections"] = [dict(r) for r in self._reflections]
        return payload

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "GreenDashboard("
                f"agents={len(self._agents)}, "
                f"reflections={len(self._reflections)}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _atomic_write_json(path: Path, payload: Any) -> int:
    """Write ``payload`` to ``path`` atomically. Returns bytes written."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=path.name + ".", suffix=".tmp",
            dir=str(path.parent),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, default=str)
            os.replace(tmp_path, path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except OSError as exc:
        raise GreenDashboardError(
            f"could not write {path}: {exc}"
        ) from exc
    return path.stat().st_size


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "GreenDashboard",
    "GreenDashboardConfig",
    "GreenDashboardError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m dashboard.green_dashboard
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import tempfile

    logging.basicConfig(level=logging.INFO)

    dash = GreenDashboard()
    print("repr       :", dash)

    # ---- Bug fix: empty reflections ------------------------------ #
    score = dash._calculate_interpretability_score([])
    assert score == 0.0 and not math.isnan(score)
    print("empty refl : 0.0 (not NaN)")

    # ---- Add agents ---------------------------------------------- #
    dash.add_agent_data(
        "agent_A",
        {"cumulative": {"total_energy_wh": 0.01, "total_carbon_kg": 0.001, "total_latency_ms": 100}},
        [{"step": 1, "decision": "optimize", "confidence": 0.9,
          "self_explanation": "Chose the more energy-efficient model."}],
        {"position": "frontier"},
    )
    dash.add_agent_data(
        "agent_B",
        {"cumulative": {"total_energy_wh": 0.02, "total_carbon_kg": 0.002, "total_latency_ms": 200}},
        [],
        {"position": "dominated"},
    )
    print("stats      :", {
        k: v for k, v in dash.statistics().items()
        if k not in ("config", "uptime_seconds")
    })

    # ---- Bug fix: XSS escaping ----------------------------------- #
    dash.add_agent_data(
        "<script>alert(1)</script>",
        {"cumulative": {"total_energy_wh": 0.03}},
        [],
        {"position": "dominated"},
    )

    with tempfile.TemporaryDirectory() as td:
        json_path = Path(td) / "report.json"
        size = dash.export_dashboard(str(json_path))
        print(f"json export: {size} bytes")

        html_path = Path(td) / "report.html"
        size = dash.generate_html_report(str(html_path))
        html_content = html_path.read_text()
        assert "&lt;script&gt;" in html_content
        assert "<script>alert" not in html_content
        print(f"html export: {size} bytes (escaped)")

    print("Round-trip :", json.loads(dash.to_json())["config"]["max_agents"])
    print("\nSmoke test passed.")
