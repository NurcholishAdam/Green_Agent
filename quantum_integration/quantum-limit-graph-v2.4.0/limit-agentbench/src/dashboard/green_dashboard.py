# src/dashboard/green_dashboard.py

"""
Green Dashboard Module
======================

Visualization for reflective insights, Pareto positions, and interpretability.

Enhancements
------------
- ``GreenDashboardConfig`` — frozen, ``__repr__``, fully validated: bounded
  history, interpretability factor weights, HTML escape toggle. All fields
  check their type (``bool`` rejected for ints and floats), then their
  bounds; failures raise :class:`GreenDashboardError`.
- **Full validation** of every argument; malformed inputs fail with
  :class:`GreenDashboardError`, never a raw ``TypeError`` / ``ValueError``.
- **Thread safety** — ``RLock`` guards the dashboard data. ``export_dashboard``
  and ``generate_html_report`` take a **single** snapshot under one lock, so
  the leaderboard, comparison view, and raw data all reflect the same state.
- **Bounded storage** — ``deque(maxlen=config.max_agents)``.
- **Fixed NaN on empty reflections** — interpretability score returns 0.0
  for empty / non-mapping reflection lists.
- **Fixed NaN poisoning** — a `confidence=nan` (or `inf` / non-numeric)
  no longer corrupts the score for the whole agent. Non-finite values are
  logged and coerced to 0.0 before averaging.
- **Fixed crash on non-numeric metrics** — a `total_energy_wh="10"` (or
  any other string / non-finite value) is coerced at ingestion time
  (`metrics_summary`) with a ``WARNING``. Leaderboard sorting and the
  comparison view use only safe floats.
- **Fixed XSS in HTML export** — all user-supplied strings HTML-escaped
  (``escape_html`` config toggle preserved).
- **Atomic, durable export** — ``tempfile`` + ``fdopen`` + ``fsync`` +
  ``os.replace``; temp file cleaned on any failure; fd closed on every
  path.
- **Deterministic leaderboard tie-breaking** — final sort key is
  ``agent_id``.
- **Structured reasoning steps** — each entry carries a
  ``reasoning_steps`` list of ``{step, decision, explanation}`` dicts, so
  the comparison view no longer re-parses the rendered
  ``reasoning_path`` string.
- **Duplicate ``agent_id`` rejected** — a second ``add_agent_data`` call
  with the same ``agent_id`` raises instead of producing two entries.
- **``pareto_position`` validated** — must be one of
  ``{"frontier", "dominated", "unknown"}``.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``
  on the dashboard. ``to_json(include_history=True)`` now works (the kwarg
  routes to ``to_dict``). Counters are restored by ``from_dict``.
- ``statistics()``, ``reset()``, ``__repr__``, ``__version__``, custom
  :class:`GreenDashboardError`, and a ``_check()``-based smoke test that
  survives ``python -O``.

Compatibility
-------------
- ``GreenDashboard(*, config=None, strict=True)`` — same signature.
  ``strict`` must now be a ``bool``; passing ``strict="yes"`` raises.
- ``add_agent_data`` — same signature; returns the stored entry (as
  before). New behaviour: raises on duplicate ``agent_id``, raises on
  invalid ``pareto_position``, logs and drops non-Mapping reflection
  entries instead of silently filtering them, and stores
  ``metrics_summary`` / ``reasoning_steps`` on the entry.
- ``generate_leaderboard`` / ``generate_comparison_view`` — same return
  shape. Numeric metric values are coerced; strings and non-finite
  numbers are treated as missing (ranked worst, displayed as ``0.0``).
- ``export_dashboard`` / ``generate_html_report`` — same signatures,
  same return values (bytes written). Now atomic, durable, and
  internally consistent.
- ``to_json(include_history=True)`` now works.
- ``from_dict`` / ``from_json`` added to ``GreenDashboard``.
- ``GreenDashboardConfig.from_dict(..., strict=True)`` default; unknown
  keys rejected.
- ``GreenDashboardConfig.escape_html`` is now validated as a ``bool``.
- ``default=str`` removed from ``to_json`` and ``_atomic_write_json``.

Requires Python 3.8+.
"""

from __future__ import annotations

import copy
import html as _html
import json
import logging
import math
import os
import tempfile
import threading
import time
from collections import Counter, deque
from collections.abc import Mapping as ABCMapping
from collections.abc import Sequence as ABCSequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any, Callable, Deque, Dict, List, Mapping, Optional, Sequence, Tuple,
    Union,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"

_VALID_PARETO_POSITIONS = frozenset({"frontier", "dominated", "unknown"})


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class GreenDashboardError(ValueError):
    """Raised for invalid dashboard inputs or configuration."""


# --------------------------------------------------------------------------- #
# Validators
# --------------------------------------------------------------------------- #
def _require_nonempty_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise GreenDashboardError(
            f"{name} must be a non-empty string, got "
            f"{type(value).__name__}."
        )
    return value


def _require_bool(name: str, value: Any) -> bool:
    if not isinstance(value, bool):
        raise GreenDashboardError(
            f"{name} must be a bool, got {type(value).__name__}."
        )
    return value


def _require_positive_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise GreenDashboardError(f"{name} must be an int.")
    if value <= 0:
        raise GreenDashboardError(f"{name} must be > 0, got {value}.")
    return value


def _require_optional_positive_int(
    name: str, value: Any,
) -> Optional[int]:
    if value is None:
        return None
    return _require_positive_int(name, value)


def _require_finite_nonneg_float(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GreenDashboardError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv) or fv < 0:
        raise GreenDashboardError(
            f"{name} must be finite and >= 0, got {value!r}."
        )
    return fv


def _require_finite_positive_float(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GreenDashboardError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv) or fv <= 0:
        raise GreenDashboardError(
            f"{name} must be finite and > 0, got {value!r}."
        )
    return fv


def _coerce_path(value: Any, *, name: str = "path") -> Path:
    """Coerce ``value`` to :class:`pathlib.Path`; raise on invalid types."""
    if isinstance(value, Path):
        return value
    if isinstance(value, str):
        if not value:
            raise GreenDashboardError(f"{name} must not be empty.")
        return Path(value)
    if hasattr(value, "__fspath__"):
        return Path(os.fspath(value))
    raise GreenDashboardError(
        f"{name} must be str, Path, or os.PathLike, got "
        f"{type(value).__name__}."
    )


# --------------------------------------------------------------------------- #
# Numeric coercion helpers
# --------------------------------------------------------------------------- #
def _coerce_float_or_none(
    value: Any, *, context: str,
) -> Optional[float]:
    """
    Return ``float(value)`` when ``value`` is finite; otherwise ``None``.

    A ``None`` input is treated as "value not supplied" and returns
    ``None`` silently. Any other non-numeric or non-finite value logs a
    ``WARNING`` before returning ``None``.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        logger.warning("%s is a bool; treating as missing.", context)
        return None
    if not isinstance(value, (int, float)):
        logger.warning(
            "%s=%r is not numeric; treating as missing.",
            context, value,
        )
        return None
    fv = float(value)
    if not math.isfinite(fv):
        logger.warning(
            "%s=%r is not finite; treating as missing.",
            context, value,
        )
        return None
    return fv


def _display_metric(value: Optional[float]) -> float:
    """Return ``value`` or ``0.0`` for display."""
    return 0.0 if value is None else value


def _ranking_metric(value: Optional[float]) -> float:
    """Return ``value`` or ``+inf`` for ranking (missing sorts worst)."""
    return float("inf") if value is None else value


def _build_metrics_summary(
    metrics: Mapping[str, Any], *, agent_id: str,
) -> Dict[str, Optional[float]]:
    """Extract and coerce the numeric metrics used by ranking / display."""
    cum = metrics.get("cumulative", {})
    if not isinstance(cum, ABCMapping):
        cum = {}
    return {
        "energy": _coerce_float_or_none(
            cum.get("total_energy_wh"),
            context=f"agents[{agent_id!r}].total_energy_wh",
        ),
        "carbon": _coerce_float_or_none(
            cum.get("total_carbon_kg"),
            context=f"agents[{agent_id!r}].total_carbon_kg",
        ),
        "latency": _coerce_float_or_none(
            cum.get("total_latency_ms"),
            context=f"agents[{agent_id!r}].total_latency_ms",
        ),
    }


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
        _require_optional_positive_int("max_agents", self.max_agents)
        _require_optional_positive_int(
            "max_reflections", self.max_reflections,
        )
        _require_positive_int(
            "max_reasoning_path", self.max_reasoning_path,
        )
        for name in (
            "weight_reflection_frequency", "weight_avg_confidence",
            "weight_decision_consistency", "weight_explanation_quality",
        ):
            _require_finite_nonneg_float(name, getattr(self, name))
        weights = (
            self.weight_reflection_frequency
            + self.weight_avg_confidence
            + self.weight_decision_consistency
            + self.weight_explanation_quality
        )
        if abs(weights - 1.0) > 1e-6:
            raise GreenDashboardError(
                f"interpretability weights must sum to 1.0 "
                f"(got {weights:.6f})."
            )
        _require_finite_positive_float(
            "reflections_for_max_frequency",
            self.reflections_for_max_frequency,
        )
        _require_finite_positive_float(
            "explanation_length_for_max_quality",
            self.explanation_length_for_max_quality,
        )
        _require_bool("escape_html", self.escape_html)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = True,
    ) -> "GreenDashboardConfig":
        if not isinstance(data, Mapping):
            raise GreenDashboardError(
                "GreenDashboardConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        if strict:
            unknown = [k for k in data if k not in valid]
            if unknown:
                raise GreenDashboardError(
                    f"unknown GreenDashboardConfig keys: {unknown}"
                )
        try:
            kwargs = {k: v for k, v in data.items() if k in valid}
            return cls(**kwargs)
        except GreenDashboardError:
            raise
        except (TypeError, ValueError) as exc:
            raise GreenDashboardError(
                f"invalid GreenDashboardConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "GreenDashboardConfig("
            f"max_agents={self.max_agents}, "
            f"max_reflections={self.max_reflections}, "
            f"escape_html={self.escape_html})"
        )


# --------------------------------------------------------------------------- #
# Dashboard
# --------------------------------------------------------------------------- #
class GreenDashboard:
    """
    Dashboard for visualizing meta-cognitive insights and sustainability
    metrics.

    Thread-safe, serializable, and bounded in memory.
    """

    def __init__(
        self,
        *,
        config: Optional[GreenDashboardConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None and not isinstance(
            config, GreenDashboardConfig
        ):
            raise GreenDashboardError(
                f"config must be a GreenDashboardConfig or None, got "
                f"{type(config).__name__}."
            )
        self._config = config or GreenDashboardConfig()
        _require_bool("strict", strict)
        self._strict = strict

        self._lock = threading.RLock()
        self._agents: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_agents
        )
        self._reflections: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_reflections
        )
        self._started_at: float = time.time()

        # Legacy view — mutable for backward compatibility. Updated after
        # every ``add_agent_data`` and ``reset``. Entries here are deep
        # copies; mutating them does **not** affect the internal deques.
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
    def strict(self) -> bool:
        return self._strict

    @property
    def agent_count(self) -> int:
        with self._lock:
            return len(self._agents)

    # ---------------------------------------------------------- ingestion
    def add_agent_data(
        self,
        agent_id: str,
        metrics: Mapping[str, Any],
        reflections: Sequence[Mapping[str, Any]],
        pareto_position: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """
        Add agent data to the dashboard.

        Returns the stored agent entry.

        Raises
        ------
        GreenDashboardError
            For invalid ``agent_id`` / ``metrics`` / ``reflections`` /
            ``pareto_position``, or when ``agent_id`` is already
            registered.
        """
        _require_nonempty_str("agent_id", agent_id)
        if not isinstance(metrics, Mapping):
            raise GreenDashboardError("metrics must be a Mapping.")
        if isinstance(reflections, (str, bytes)) or not isinstance(
            reflections, ABCSequence
        ):
            raise GreenDashboardError(
                "reflections must be a sequence of mappings "
                "(not a string)."
            )
        if not isinstance(pareto_position, Mapping):
            raise GreenDashboardError(
                "pareto_position must be a Mapping."
            )

        # ---- Reflections: filter with a warning per drop ---------- #
        reflections_list: List[Dict[str, Any]] = []
        dropped_reflections = 0
        for i, r in enumerate(reflections):
            if isinstance(r, Mapping):
                reflections_list.append(dict(r))
            else:
                dropped_reflections += 1
                logger.warning(
                    "add_agent_data(%r): reflections[%d] is not a "
                    "Mapping (%s); dropped.",
                    agent_id, i, type(r).__name__,
                )
        if dropped_reflections:
            logger.warning(
                "add_agent_data(%r): %d/%d reflections dropped as "
                "non-Mapping.", agent_id, dropped_reflections,
                len(reflections),
            )

        # ---- Pareto position ------------------------------------- #
        pos = pareto_position.get("position", "unknown")
        if not isinstance(pos, str) or pos not in _VALID_PARETO_POSITIONS:
            raise GreenDashboardError(
                f"pareto_position.position must be one of "
                f"{sorted(_VALID_PARETO_POSITIONS)}, got {pos!r}."
            )

        # ---- Metric summary (safe floats) ------------------------ #
        metrics_summary = _build_metrics_summary(
            metrics, agent_id=agent_id,
        )

        interpretability_score = self._calculate_interpretability_score(
            reflections_list
        )

        # ---- Structured reasoning steps -------------------------- #
        reasoning_steps = [
            {
                "step": r.get("step", 0),
                "decision": str(
                    r.get("decision", "unknown") or "unknown"
                ),
                "explanation": str(
                    r.get("self_explanation", "") or ""
                )[:100],
            }
            for r in reflections_list[: self._config.max_reasoning_path]
        ]

        entry: Dict[str, Any] = {
            "agent_id": agent_id,
            "metrics": copy.deepcopy(dict(metrics)),
            "metrics_summary": metrics_summary,
            "reflection_count": len(reflections_list),
            "pareto_position": pos,
            "interpretability_score": interpretability_score,
            "reasoning_path": self._extract_reasoning_path(
                reflections_list
            ),
            "reasoning_steps": reasoning_steps,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        with self._lock:
            if any(a["agent_id"] == agent_id for a in self._agents):
                raise GreenDashboardError(
                    f"agent_id {agent_id!r} is already registered; "
                    f"call reset() or use a unique ID."
                )
            self._agents.append(entry)
            self._reflections.extend(
                copy.deepcopy(r) for r in reflections_list
            )
            self.dashboard_data["agents"] = copy.deepcopy(
                list(self._agents)
            )
            self.dashboard_data["reflections"] = copy.deepcopy(
                list(self._reflections)
            )

        logger.debug(
            "Added agent %s: interpretability=%.3f, position=%s",
            agent_id, interpretability_score, pos,
        )
        return entry

    # ---------------------------------------------------------- interpretability
    def _calculate_interpretability_score(
        self, reflections: Sequence[Any],
    ) -> float:
        """
        Compute interpretability in ``[0, 1]`` from reflection quality.

        Returns ``0.0`` for an empty reflection list (fixes the original
        division-by-zero that produced ``NaN``). Non-finite or
        non-numeric ``confidence`` values are logged and treated as
        ``0.0`` so a single bad value cannot poison the score.
        """
        if not reflections:
            return 0.0
        reflections = [r for r in reflections if isinstance(r, Mapping)]
        if not reflections:
            return 0.0

        cfg = self._config
        score = 0.0

        # Factor 1: reflection frequency.
        score += cfg.weight_reflection_frequency * min(
            len(reflections) / cfg.reflections_for_max_frequency, 1.0,
        )

        # Factor 2: average confidence.
        confidences: List[float] = []
        for i, r in enumerate(reflections):
            fv = _coerce_float_or_none(
                r.get("confidence"),
                context=f"reflections[{i}].confidence",
            )
            confidences.append(0.0 if fv is None else fv)
        if confidences:
            avg = sum(confidences) / len(confidences)
            avg = max(0.0, min(1.0, avg))
            score += cfg.weight_avg_confidence * avg

        # Factor 3: decision consistency.
        decisions = [
            str(r.get("decision", "") or "") for r in reflections
        ]
        if decisions:
            counter = Counter(decisions)
            most_common_count = counter.most_common(1)[0][1]
            consistency = most_common_count / len(decisions)
            score += cfg.weight_decision_consistency * consistency

        # Factor 4: explanation quality.
        explanation_lengths = [
            len(str(r.get("self_explanation", "") or ""))
            for r in reflections
        ]
        if explanation_lengths:
            avg_len = sum(explanation_lengths) / len(explanation_lengths)
            score += cfg.weight_explanation_quality * min(
                avg_len / cfg.explanation_length_for_max_quality, 1.0,
            )

        return max(0.0, min(1.0, score))

    def _extract_reasoning_path(
        self, reflections: Sequence[Mapping[str, Any]],
    ) -> List[str]:
        """Extract a bounded list of reasoning steps from reflections."""
        cfg = self._config
        path: List[str] = []
        for r in list(reflections)[: cfg.max_reasoning_path]:
            step = r.get("step", 0)
            decision = r.get("decision", "unknown")
            explanation = str(r.get("self_explanation", ""))[:100]
            path.append(f"Step {step}: {decision} - {explanation}")
        return path

    # ---------------------------------------------------------- leaderboard
    def generate_leaderboard(self) -> Dict[str, Any]:
        """Generate a leaderboard comparing agents on multiple dimensions."""
        with self._lock:
            agents = [dict(a) for a in self._agents]
        return self._leaderboard_from(agents)

    def _leaderboard_from(
        self, agents: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Pure helper — no lock; caller supplies a snapshot."""
        if not agents:
            return {"agents": [], "rankings": {}, "top_performers": {}}

        efficiency_ranking = sorted(
            agents,
            key=lambda a: (
                0 if a["pareto_position"] == "frontier" else 1,
                _ranking_metric(a["metrics_summary"]["energy"]),
                a["agent_id"],
            ),
        )
        interpretability_ranking = sorted(
            agents,
            key=lambda a: (-a["interpretability_score"], a["agent_id"]),
        )

        def _sustainability_key(a: Dict[str, Any]) -> Tuple[float, str]:
            e = _ranking_metric(a["metrics_summary"]["energy"])
            c = _ranking_metric(a["metrics_summary"]["carbon"])
            return (e + c * 1000, a["agent_id"])

        sustainability_ranking = sorted(agents, key=_sustainability_key)

        return {
            "agents": agents,
            "rankings": {
                "efficiency": [
                    a["agent_id"] for a in efficiency_ranking
                ],
                "interpretability": [
                    a["agent_id"] for a in interpretability_ranking
                ],
                "sustainability": [
                    a["agent_id"] for a in sustainability_ranking
                ],
            },
            "top_performers": {
                "most_efficient": (
                    efficiency_ranking[0]["agent_id"]
                    if efficiency_ranking else None
                ),
                "most_interpretable": (
                    interpretability_ranking[0]["agent_id"]
                    if interpretability_ranking else None
                ),
                "most_sustainable": (
                    sustainability_ranking[0]["agent_id"]
                    if sustainability_ranking else None
                ),
            },
        }

    def generate_comparison_view(self) -> Dict[str, Any]:
        """Generate a comparison view with reasoning."""
        with self._lock:
            agents = [dict(a) for a in self._agents]
        return self._comparison_from(agents)

    def _comparison_from(
        self, agents: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Pure helper — no lock; caller supplies a snapshot."""
        comparisons: List[Dict[str, Any]] = []
        for agent in agents:
            steps = agent.get("reasoning_steps") or []
            path = agent.get("reasoning_path") or []
            summary = agent.get("metrics_summary", {})
            comparisons.append({
                "agent_id": agent["agent_id"],
                "pareto_position": agent["pareto_position"],
                "reasoning_summary": self._summarize_from_steps(
                    steps, fallback_path=path,
                ),
                "key_decisions": self._extract_key_decisions_from_steps(
                    steps, fallback_path=path,
                ),
                "interpretability": agent["interpretability_score"],
                "metrics_summary": {
                    "energy": _display_metric(summary.get("energy")),
                    "carbon": _display_metric(summary.get("carbon")),
                    "latency": _display_metric(summary.get("latency")),
                },
            })

        return {
            "comparisons": comparisons,
            "insights": self._generate_comparative_insights(comparisons),
        }

    def _summarize_reasoning(self, reasoning_path: List[str]) -> str:
        """Fallback summarizer that parses the rendered reasoning path."""
        if not reasoning_path:
            return "No reasoning available"
        parts: List[str] = []
        for step in reasoning_path[:3]:
            try:
                parts.append(step.split(":")[1].split("-")[0].strip())
            except (IndexError, AttributeError):
                parts.append(step)
        return " → ".join(parts) if parts else "No reasoning available"

    def _summarize_from_steps(
        self,
        steps: Sequence[Mapping[str, Any]],
        *,
        fallback_path: Optional[Sequence[str]] = None,
    ) -> str:
        """Prefer structured steps; fall back to string parsing."""
        parts: List[str] = []
        for s in list(steps)[:3]:
            decision = str(s.get("decision", "") or "").strip()
            if decision:
                parts.append(decision)
        if parts:
            return " → ".join(parts)
        return self._summarize_reasoning(list(fallback_path or []))

    def _extract_key_decisions(
        self, reasoning_path: List[str],
    ) -> List[str]:
        """Fallback extractor that parses the rendered reasoning path."""
        decisions: List[str] = []
        for step in reasoning_path:
            lower = step.lower()
            if (
                "reduce" in lower
                or "optimize" in lower
                or "continue" in lower
            ):
                try:
                    decisions.append(
                        step.split(":")[1].split("-")[0].strip()
                    )
                except (IndexError, AttributeError):
                    decisions.append(step)
        return decisions[:5]

    def _extract_key_decisions_from_steps(
        self,
        steps: Sequence[Mapping[str, Any]],
        *,
        fallback_path: Optional[Sequence[str]] = None,
    ) -> List[str]:
        """Prefer structured steps; fall back to string parsing."""
        keywords = ("reduce", "optimize", "continue")
        decisions: List[str] = []
        for s in steps:
            decision = str(s.get("decision", "") or "").strip()
            if any(k in decision.lower() for k in keywords):
                decisions.append(decision)
                if len(decisions) >= 5:
                    break
        if decisions:
            return decisions
        return self._extract_key_decisions(list(fallback_path or []))

    def _generate_comparative_insights(
        self, comparisons: List[Dict[str, Any]],
    ) -> List[str]:
        insights: List[str] = []
        frontier_agents = [
            c for c in comparisons
            if c["pareto_position"] == "frontier"
        ]
        if frontier_agents:
            insights.append(
                f"{len(frontier_agents)} agent(s) achieved Pareto-optimal "
                f"trade-offs"
            )
        high_interp = [
            c for c in comparisons if c["interpretability"] > 0.7
        ]
        if high_interp:
            insights.append(
                f"{len(high_interp)} agent(s) demonstrated high "
                f"interpretability (>0.7)"
            )
        energies = [
            c["metrics_summary"]["energy"] for c in comparisons
        ]
        if energies:
            avg_energy = sum(energies) / len(energies)
            efficient = [
                c for c in comparisons
                if c["metrics_summary"]["energy"] < avg_energy * 0.8
            ]
            if efficient:
                insights.append(
                    f"{len(efficient)} agent(s) achieved 20% better "
                    f"energy efficiency than average"
                )
        return insights

    # ---------------------------------------------------------- export
    def export_dashboard(self, filepath: Union[str, Path]) -> int:
        """
        Atomically export dashboard data to JSON.

        Takes a single snapshot under the lock so the leaderboard,
        comparison view, and raw data all reflect the same state.
        Returns bytes written.
        """
        path = _coerce_path(filepath, name="filepath")
        with self._lock:
            agents = [dict(a) for a in self._agents]
            reflections = [dict(r) for r in self._reflections]
            leaderboard = self._leaderboard_from(agents)
            comparison = self._comparison_from(agents)

        export_data = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "leaderboard": leaderboard,
            "comparison_view": comparison,
            "raw_data": {
                "agents": agents,
                "reflections": reflections,
            },
        }
        return _atomic_write_json(path, export_data)

    def generate_html_report(self, filepath: Union[str, Path]) -> int:
        """
        Atomically generate an HTML dashboard report.

        Returns bytes written.
        """
        path = _coerce_path(filepath, name="filepath")
        with self._lock:
            agents = [dict(a) for a in self._agents]
            leaderboard = self._leaderboard_from(agents)
            comparison = self._comparison_from(agents)

        esc = (
            (lambda s: _html.escape(str(s)))
            if self._config.escape_html
            else (lambda s: str(s))
        )

        rows: List[str] = []
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

        html_content = f"""<!DOCTYPE html>
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

        return _atomic_write_text(path, html_content)

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
            started_at = self._started_at
        if not agents:
            return {
                "agents": 0,
                "reflections": len(reflections),
                "config": self._config.to_dict(),
                "strict": self._strict,
                "uptime_seconds": time.time() - started_at,
            }
        interp_scores = [a["interpretability_score"] for a in agents]
        return {
            "agents": len(agents),
            "reflections": len(reflections),
            "mean_interpretability": (
                sum(interp_scores) / len(interp_scores)
            ),
            "max_interpretability": max(interp_scores),
            "frontier_agents": sum(
                1 for a in agents
                if a["pareto_position"] == "frontier"
            ),
            "config": self._config.to_dict(),
            "strict": self._strict,
            "uptime_seconds": time.time() - started_at,
        }

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_history: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "started_at": self._started_at,
            }
            if include_history:
                payload["agents"] = [dict(a) for a in self._agents]
                payload["reflections"] = [
                    dict(r) for r in self._reflections
                ]
        return payload

    def to_json(
        self, *, include_history: bool = False, **kwargs: Any,
    ) -> str:
        """Serialize to JSON.

        ``include_history=True`` includes agents and reflections
        (additive — the previous ``to_json(**kwargs)`` signature
        forwarded its kwargs to ``json.dumps`` and raised ``TypeError``
        on ``include_history``).
        """
        return json.dumps(
            self.to_dict(include_history=include_history), **kwargs
        )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: Optional[bool] = None,
        restore_history: bool = True,
    ) -> "GreenDashboard":
        """
        Reconstruct a dashboard from a ``to_dict`` payload.

        Parameters
        ----------
        data : Mapping
            Payload produced by :meth:`to_dict`.
        strict : bool, optional
            Overrides the payload's ``strict`` flag when supplied.
        restore_history : bool, default True
            Restore agents and reflections when the payload contains
            them.
        """
        if not isinstance(data, Mapping):
            raise GreenDashboardError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        cfg_blob = data.get("config", {})
        config = (
            cfg_blob
            if isinstance(cfg_blob, GreenDashboardConfig)
            else GreenDashboardConfig.from_dict(cfg_blob)
        )
        resolved_strict = (
            bool(data.get("strict", True))
            if strict is None else strict
        )
        dash = cls(config=config, strict=resolved_strict)

        if restore_history:
            for entry_data in data.get("agents", []) or []:
                if not isinstance(entry_data, Mapping):
                    logger.warning(
                        "from_dict: skipping non-Mapping agent entry."
                    )
                    continue
                stored = dict(entry_data)
                # Recompute metrics_summary for ranking safety.
                metrics_raw = stored.get("metrics")
                if isinstance(metrics_raw, Mapping):
                    stored["metrics_summary"] = _build_metrics_summary(
                        metrics_raw,
                        agent_id=stored.get("agent_id", "?"),
                    )
                elif not isinstance(
                    stored.get("metrics_summary"), Mapping
                ):
                    stored["metrics_summary"] = {
                        "energy": None, "carbon": None, "latency": None,
                    }
                with dash._lock:
                    dash._agents.append(stored)

            for entry_data in data.get("reflections", []) or []:
                if isinstance(entry_data, Mapping):
                    with dash._lock:
                        dash._reflections.append(dict(entry_data))

            with dash._lock:
                dash.dashboard_data["agents"] = [
                    dict(a) for a in dash._agents
                ]
                dash.dashboard_data["reflections"] = [
                    dict(r) for r in dash._reflections
                ]

        if data.get("started_at") is not None:
            try:
                dash._started_at = float(data["started_at"])
            except (TypeError, ValueError):
                pass

        return dash

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: Optional[bool] = None,
        restore_history: bool = True,
    ) -> "GreenDashboard":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise GreenDashboardError(
                f"invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(
            data, strict=strict, restore_history=restore_history,
        )

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
# Atomic I/O
# --------------------------------------------------------------------------- #
def _atomic_write(path: Path, write_fn: Callable[[Any], None]) -> int:
    """
    Write to ``path`` atomically using ``write_fn``.

    Uses ``mkstemp`` + ``fdopen`` + ``fsync`` + ``os.replace``. Cleans
    up the temp file and closes the fd on every failure path. Returns
    the final file size in bytes.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=path.name + ".", suffix=".tmp",
            dir=str(path.parent),
        )
    except OSError as exc:
        raise GreenDashboardError(
            f"could not prepare {path}: {exc}"
        ) from exc

    # Acquire ownership of the fd. If fdopen itself fails, close the
    # fd manually — the previous revision leaked it.
    try:
        f = os.fdopen(fd, "w", encoding="utf-8")
    except OSError:
        os.close(fd)
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    try:
        with f:
            write_fn(f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except GreenDashboardError:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
    except (OSError, TypeError, ValueError) as exc:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise GreenDashboardError(
            f"could not write {path}: {exc}"
        ) from exc
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return path.stat().st_size


def _atomic_write_json(path: Path, payload: Any) -> int:
    """Atomically write ``payload`` as JSON. Returns bytes written."""
    return _atomic_write(
        path, lambda f: json.dump(payload, f, indent=2),
    )


def _atomic_write_text(path: Path, text: str) -> int:
    """Atomically write ``text`` verbatim. Returns bytes written."""
    return _atomic_write(path, lambda f: f.write(text))


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__: List[str] = [
    "__version__",
    "GreenDashboard",
    "GreenDashboardConfig",
    "GreenDashboardError",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m dashboard.green_dashboard
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys
    import tempfile

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

    def _expect_error(name: str, expected: type, fn, *args, **kwargs):
        global failed
        try:
            fn(*args, **kwargs)
            _check(name, False)
        except expected:
            _check(name, True)
        except Exception as exc:
            print(
                f"  ❌ {name} — wrong exception "
                f"{type(exc).__name__}: {exc}"
            )
            failed += 1

    # ---- 1. Config validation ---------------------------------- #
    print("config:")
    cfg = GreenDashboardConfig()
    _check("default config constructs", cfg.max_agents == 10_000)
    _check("repr concise", "GreenDashboardConfig(" in repr(cfg))
    _check(
        "config round-trip",
        GreenDashboardConfig.from_dict(cfg.to_dict()) == cfg,
    )

    for label, bad in (
        ("negative max_agents", dict(max_agents=-1)),
        ("non-int max_agents", dict(max_agents="10")),
        ("bool max_agents", dict(max_agents=True)),
        ("zero max_reasoning_path", dict(max_reasoning_path=0)),
        ("string max_reasoning_path", dict(max_reasoning_path="100")),
        ("bad weight sum", dict(weight_explanation_quality=0.9)),
        ("negative weight", dict(weight_reflection_frequency=-0.1)),
        ("string weight", dict(weight_avg_confidence="0.3")),
        ("zero saturation", dict(reflections_for_max_frequency=0)),
        ("string saturation", dict(explanation_length_for_max_quality="x")),
        ("non-bool escape_html", dict(escape_html="yes")),
    ):
        _expect_error(
            f"reject cfg {label}",
            GreenDashboardError,
            GreenDashboardConfig,
            **bad,
        )

    # Config from_dict strict
    _expect_error(
        "config from_dict rejects unknown keys (strict default)",
        GreenDashboardError,
        GreenDashboardConfig.from_dict,
        {"max_agents": 5000, "typo_field": 1},
    )
    _check(
        "config from_dict strict=False accepts unknown keys",
        GreenDashboardConfig.from_dict(
            {"max_agents": 5000, "typo_field": 1}, strict=False,
        ).max_agents == 5000,
    )

    # ---- 2. Construction + strict validation ------------------- #
    print("construction:")
    dash = GreenDashboard()
    _check("dashboard constructs", dash.agent_count == 0)
    _check("strict is bool", dash.strict is True)
    _expect_error(
        "reject non-bool strict",
        GreenDashboardError,
        GreenDashboard,
        strict="yes",
    )

    # ---- 3. Empty reflections = 0.0 ---------------------------- #
    print("interpretability:")
    _check(
        "empty reflections → 0.0",
        dash._calculate_interpretability_score([]) == 0.0,
    )
    dash.add_agent_data(
        "empty_refl",
        {"cumulative": {"total_energy_wh": 1.0}},
        [],
        {"position": "dominated"},
    )
    empty_entry = next(
        e for e in dash.dashboard_data["agents"]
        if e["agent_id"] == "empty_refl"
    )
    _check(
        "public API: empty reflections score 0.0",
        empty_entry["interpretability_score"] == 0.0,
    )

    # NaN confidence must not poison the score
    dash.add_agent_data(
        "nan_conf",
        {"cumulative": {"total_energy_wh": 2.0}},
        [
            {"step": 1, "decision": "go", "confidence": 0.9,
             "self_explanation": "a" * 100},
            {"step": 2, "decision": "go",
             "confidence": float("nan"),
             "self_explanation": "b" * 100},
        ],
        {"position": "dominated"},
    )
    nan_entry = next(
        e for e in dash.dashboard_data["agents"]
        if e["agent_id"] == "nan_conf"
    )
    _check(
        "NaN confidence → finite score",
        math.isfinite(nan_entry["interpretability_score"]),
    )

    # String confidence must not crash
    dash.add_agent_data(
        "str_conf",
        {"cumulative": {"total_energy_wh": 3.0}},
        [
            {"step": 1, "decision": "go",
             "confidence": "abc",
             "self_explanation": "c" * 50},
        ],
        {"position": "dominated"},
    )
    str_entry = next(
        e for e in dash.dashboard_data["agents"]
        if e["agent_id"] == "str_conf"
    )
    _check(
        "non-numeric confidence tolerated",
        math.isfinite(str_entry["interpretability_score"]),
    )

    # Non-mapping reflections warned + dropped
    dash.add_agent_data(
        "mixed_refl",
        {"cumulative": {"total_energy_wh": 4.0}},
        [{"step": 1, "decision": "ok"}, "not-a-dict"],
        {"position": "dominated"},
    )
    mixed_entry = next(
        e for e in dash.dashboard_data["agents"]
        if e["agent_id"] == "mixed_refl"
    )
    _check(
        "non-mapping reflections dropped",
        mixed_entry["reflection_count"] == 1,
    )

    # ---- 4. Metrics coercion ----------------------------------- #
    print("metrics coercion:")
    dash.add_agent_data(
        "bad_metric",
        {"cumulative": {"total_energy_wh": "10"}},
        [],
        {"position": "dominated"},
    )
    bad_entry = next(
        e for e in dash.dashboard_data["agents"]
        if e["agent_id"] == "bad_metric"
    )
    _check(
        "string metric → None in summary",
        bad_entry["metrics_summary"]["energy"] is None,
    )

    # Leaderboard handles string metrics without crashing
    lb = dash.generate_leaderboard()
    _check("leaderboard built", isinstance(lb, dict))
    _check(
        "leaderboard rankings present",
        "efficiency" in lb["rankings"],
    )

    # Comparison view handles missing metrics
    cmp_view = dash.generate_comparison_view()
    _check(
        "comparison view built",
        "comparisons" in cmp_view and len(cmp_view["comparisons"]) > 0,
    )

    # ---- 5. Duplicate + pareto validation ---------------------- #
    print("ingestion validation:")
    _expect_error(
        "duplicate agent_id rejected",
        GreenDashboardError,
        dash.add_agent_data,
        "empty_refl",
        {"cumulative": {"total_energy_wh": 1.0}},
        [],
        {"position": "dominated"},
    )
    _expect_error(
        "invalid pareto_position rejected",
        GreenDashboardError,
        dash.add_agent_data,
        "bogus_agent",
        {"cumulative": {"total_energy_wh": 1.0}},
        [],
        {"position": "not-a-position"},
    )
    _expect_error(
        "string reflections rejected",
        GreenDashboardError,
        dash.add_agent_data,
        "x",
        {"cumulative": {}},
        "not-a-list",  # type: ignore[arg-type]
        {"position": "dominated"},
    )

    # Tuple of reflections is accepted
    try:
        dash.add_agent_data(
            "tuple_refl",
            {"cumulative": {"total_energy_wh": 1.0}},
            ({"step": 1, "decision": "go"},),
            {"position": "dominated"},
        )
        _check("tuple reflections accepted", True)
    except Exception:
        _check("tuple reflections accepted", False)

    # ---- 6. to_json / from_dict round-trip --------------------- #
    print("serialization:")
    payload = dash.to_json(include_history=True)
    parsed = json.loads(payload)
    _check(
        "to_json(include_history=True) works",
        "agents" in parsed and "reflections" in parsed,
    )
    _check(
        "no Infinity/NaN in JSON",
        "Infinity" not in payload and "NaN" not in payload,
    )

    restored = GreenDashboard.from_json(payload)
    _check(
        "from_json restores agent count",
        restored.agent_count == dash.agent_count,
    )

    # ---- 7. Export + HTML XSS ---------------------------------- #
    print("export:")
    with tempfile.TemporaryDirectory() as td:
        json_path = Path(td) / "report.json"
        size = dash.export_dashboard(json_path)
        _check("json export size > 0", size > 0)
        _check("json file exists", json_path.exists())

        # Add an XSS agent first
        dash.add_agent_data(
            "<script>alert(1)</script>",
            {"cumulative": {"total_energy_wh": 5.0}},
            [],
            {"position": "dominated"},
        )
        html_path = Path(td) / "report.html"
        size = dash.generate_html_report(html_path)
        _check("html export size > 0", size > 0)
        content = html_path.read_text()
        _check(
            "XSS escaped in HTML",
            "&lt;script&gt;" in content
            and "<script>alert" not in content,
        )

    # ---- 8. Reset --------------------------------------------- #
    print("reset:")
    removed = dash.reset()
    _check("reset removed agents", removed > 0)
    _check("agent_count == 0", dash.agent_count == 0)
    _check(
        "dashboard_data cleared",
        dash.dashboard_data["agents"] == [],
    )

    # ---- 9. Public API ---------------------------------------- #
    print("public API:")
    for name in (
        "__version__",
        "GreenDashboard",
        "GreenDashboardConfig",
        "GreenDashboardError",
    ):
        _check(f"{name} exported", name in __all__)
    _check(
        "GreenDashboardError is ValueError",
        issubclass(GreenDashboardError, ValueError),
    )
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
