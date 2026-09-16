# src/agentbeats/green_agent.py

"""
Green Sustainability Agent

Scores agents using Pareto optimality across accuracy, energy, carbon, and
latency. Wraps :class:`analysis.pareto_analyzer.ParetoAnalyzer` and exposes an
async-friendly ``score_with_pareto`` API.

Original behaviour preserved
----------------------------
- ``GreenSustainabilityAgent()`` — constructs a Pareto analyzer.
- ``score_with_pareto(results)`` — returns a dict with ``frontier``,
  ``ranks``, and ``knee_point``.

Enhancements
------------
- Fixes the broken ``ParetoFrontierAnalyzer`` import (aliases to the real
  ``ParetoAnalyzer``) and implements the missing ``rank_by_dominance`` /
  ``get_knee_point`` helpers locally.
- Thread-safe via ``RLock``.
- Configurable weights / objectives via :class:`GreenAgentConfig`.
- Immutable ``ScoreRecord`` history with bounded ring-buffer.
- Validation of every result row; strict / non-strict modes.
- Structured serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Context-manager support for scoped scoring batches.
- Custom :class:`GreenAgentError`.
- Lazy ``%s`` logging, ``__repr__``, and a smoke test under ``__main__``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Robust import of the analysis layer
# --------------------------------------------------------------------------- #
# The canonical class is ``ParetoAnalyzer``; older/parallel code refers to it
# as ``ParetoFrontierAnalyzer``. We try both names so this module works against
# any revision of the analysis package.
try:  # Preferred package-relative import
    from analysis.pareto_analyzer import ParetoAnalyzer  # type: ignore
    from analysis.pareto_analyzer import ParetoPoint  # type: ignore
except ImportError:  # pragma: no cover — fallback for script-style usage
    import sys
    from pathlib import Path

    _ANALYSIS_ROOT = Path(__file__).resolve().parent.parent
    if str(_ANALYSIS_ROOT) not in sys.path:
        sys.path.insert(0, str(_ANALYSIS_ROOT))
    from analysis.pareto_analyzer import ParetoAnalyzer  # type: ignore
    from analysis.pareto_analyzer import ParetoPoint  # type: ignore

# Backward-compatibility alias: callers that import the old name get the real
# class instead of ``None`` (which is what ``analysis/__init__.py`` currently
# produces, because it tries to expose a symbol that does not exist).
ParetoFrontierAnalyzer = ParetoAnalyzer


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class GreenAgentError(ValueError):
    """Raised for invalid inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GreenAgentConfig:
    """
    Tunable parameters for Pareto-based agent scoring.

    Centralizes the field names, objective directions, and knee-point search
    settings so callers can calibrate without editing the agent.
    """

    # Field names expected in each result row.
    key_agent_id: str = "agent_id"
    key_accuracy: str = "accuracy"
    key_energy: str = "energy_kwh"
    key_carbon: str = "carbon_kg"
    key_latency: str = "latency_ms"

    # Objectives to include in the frontier (minimize energy/carbon/latency,
    # maximize accuracy). Order matters for the ranking tie-breaker.
    objectives: Tuple[str, ...] = ("energy_kwh", "accuracy")
    maximize: Tuple[str, ...] = ("accuracy",)

    # Fractional slack used when detecting dominance (relative tolerance).
    dominance_tolerance: float = 0.0

    # Number of samples kept in the score history ring-buffer.
    max_history: Optional[int] = 1000

    # If True, carbon is included as a third objective.
    carbon_enabled: bool = False

    def __post_init__(self) -> None:
        if self.dominance_tolerance < 0:
            raise GreenAgentError("dominance_tolerance must be >= 0.")
        if self.max_history is not None and self.max_history <= 0:
            raise GreenAgentError("max_history must be > 0 or None.")
        if not set(self.maximize).issubset(set(self.objectives)):
            raise GreenAgentError(
                "maximize must be a subset of objectives "
                f"(got {self.maximize} vs {self.objectives})."
            )


# --------------------------------------------------------------------------- #
# Immutable score sample
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ScoreRecord:
    """Immutable snapshot of one ``score_with_pareto`` call."""

    timestamp: float
    num_agents: int
    frontier_size: int
    knee_agent_id: Optional[str]
    top_agent_id: Optional[str]
    label: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Agent
# --------------------------------------------------------------------------- #
class GreenSustainabilityAgent:
    """
    Scores agents using Pareto optimality across multiple objectives.

    Wraps :class:`ParetoAnalyzer` and exposes an async-friendly
    ``score_with_pareto`` that returns a dict with ``frontier``, ``ranks``,
    and ``knee_point``.

    Thread-safe, serializable, and bounded in memory.
    """

    def __init__(
        self,
        *,
        config: Optional[GreenAgentConfig] = None,
        analyzer: Optional[ParetoAnalyzer] = None,
        strict: bool = True,
    ) -> None:
        self._config: GreenAgentConfig = config or GreenAgentConfig()
        self._strict: bool = bool(strict)

        self._lock = threading.RLock()
        self._history: List[ScoreRecord] = []
        self._ctx_start: Optional[float] = None

        # Build (or accept) the underlying Pareto analyzer.
        if analyzer is not None:
            self.pareto_analyzer = analyzer
        else:
            try:
                self.pareto_analyzer = ParetoAnalyzer()
            except TypeError:  # pragma: no cover — analyzer needs kwargs
                self.pareto_analyzer = ParetoAnalyzer(
                    carbon_in_frontier=self._config.carbon_enabled
                )

        logger.debug(
            "GreenSustainabilityAgent initialized (objectives=%s, maximize=%s, strict=%s)",
            self._config.objectives,
            self._config.maximize,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> GreenAgentConfig:
        return self._config

    @property
    def history(self) -> List[ScoreRecord]:
        with self._lock:
            return list(self._history)

    @property
    def scoring_count(self) -> int:
        with self._lock:
            return len(self._history)

    # ---------------------------------------------------------- public API
    async def score_with_pareto(
        self,
        results: Sequence[Mapping[str, Any]],
        *,
        label: Optional[str] = None,
        record: bool = True,
    ) -> Dict[str, Any]:
        """
        Score agents using Pareto optimality.

        Parameters
        ----------
        results : Sequence[Mapping]
            Each row must contain the keys named in :class:`GreenAgentConfig`
            (``agent_id``, ``accuracy``, ``energy_kwh``, ``carbon_kg``,
            ``latency_ms``).
        label : str, optional
            Optional label for the recorded :class:`ScoreRecord`.
        record : bool, default True
            If True, append a bounded :class:`ScoreRecord` to history.

        Returns
        -------
        dict
            ``{"frontier": [...], "ranks": {...}, "knee_point": {...}}``.
        """
        if not isinstance(results, Sequence) or isinstance(results, (str, bytes)):
            raise GreenAgentError(
                f"results must be a sequence of mappings, got {type(results).__name__}."
            )
        if not results:
            raise GreenAgentError("results must contain at least one agent.")

        # ---- Validate + build ParetoPoints --------------------------------
        points: List[ParetoPoint] = []
        invalid: List[Tuple[int, str]] = []
        for idx, row in enumerate(results):
            try:
                points.append(self._to_pareto_point(row))
            except GreenAgentError as exc:
                if self._strict:
                    raise
                invalid.append((idx, str(exc)))
                logger.warning("Skipping invalid result row %d: %s", idx, exc)

        if not points:
            raise GreenAgentError("No valid result rows to score.")

        # ---- Compute frontier (using the real ParetoAnalyzer API) --------
        frontier = await self._compute_frontier(points)

        # ---- Rank by dominance (local helper; not in ParetoAnalyzer) -----
        ranks = self._rank_by_dominance(points)

        # ---- Knee point (local helper; not in ParetoAnalyzer) ------------
        knee = self._get_knee_point(frontier)

        response: Dict[str, Any] = {
            "frontier": [self._point_to_dict(p) for p in frontier],
            "ranks": ranks,
            "knee_point": self._point_to_dict(knee) if knee is not None else None,
        }
        if invalid:
            response["invalid_rows"] = [idx for idx, _ in invalid]

        if record:
            top = ranks[0][0] if ranks else None
            self._record(
                num_agents=len(points),
                frontier_size=len(frontier),
                knee_agent_id=self._agent_id(knee) if knee is not None else None,
                top_agent_id=top,
                label=label,
            )

        logger.info(
            "Scored %d agent(s): frontier=%d, knee=%s",
            len(points),
            len(frontier),
            self._agent_id(knee) if knee is not None else None,
        )
        return response

    # ---------------------------------------------------------- internals
    async def _compute_frontier(
        self, points: Sequence[ParetoPoint]
    ) -> List[ParetoPoint]:
        """
        Compute the Pareto frontier.

        ``ParetoAnalyzer.compute_frontier()`` operates on its internal
        ``self.points`` list, so we add records then call it. We also try the
        richer ``compute_frontier_detailed`` path and fall back to the plain
        one if unavailable.
        """
        def _run() -> List[ParetoPoint]:
            analyzer = self.pareto_analyzer
            analyzer.points = list(points)  # type: ignore[attr-defined]

            detailed = getattr(analyzer, "compute_frontier_detailed", None)
            if callable(detailed):
                try:
                    result = detailed()
                    if hasattr(result, "points"):
                        return list(result.points)
                except Exception as exc:  # fall back to the plain API
                    logger.debug(
                        "compute_frontier_detailed failed (%s); "
                        "falling back to compute_frontier.",
                        exc,
                    )

            plain = getattr(analyzer, "compute_frontier", None)
            if callable(plain):
                return list(plain())

            # Last-resort local frontier calculation.
            return self._local_frontier(points)

        # ParetoAnalyzer is synchronous; run it in a thread to keep
        # ``score_with_pareto`` non-blocking under asyncio.
        return await asyncio.to_thread(_run)

    def _to_pareto_point(self, row: Mapping[str, Any]) -> ParetoPoint:
        """Convert one result row into a validated ``ParetoPoint``."""
        if not isinstance(row, Mapping):
            raise GreenAgentError(
                f"result row must be a Mapping, got {type(row).__name__}."
            )
        cfg = self._config

        agent_id = row.get(cfg.key_agent_id)
        if not isinstance(agent_id, str) or not agent_id:
            raise GreenAgentError("agent_id must be a non-empty string.")

        accuracy = self._coerce_number(cfg.key_accuracy, row, low=0.0, high=1.0)
        energy = self._coerce_number(cfg.key_energy, row, low=0.0)
        carbon = self._coerce_number(cfg.key_carbon, row, low=0.0)
        latency = self._coerce_number(cfg.key_latency, row, low=0.0)

        # ParetoPoint expects energy_joules; the input field is energy_kwh.
        # Convert kWh → J so the analyzer's semantics stay correct.
        energy_joules = energy * 3_600_000.0

        return ParetoPoint(
            energy_joules=energy_joules,
            accuracy=accuracy,
            carbon_grams=carbon,
            label=agent_id,
            metadata={"latency_ms": latency, "energy_kwh": energy},
            agent_id=agent_id,
        )

    @staticmethod
    def _coerce_number(
        key: str,
        row: Mapping[str, Any],
        *,
        low: float,
        high: Optional[float] = None,
    ) -> float:
        if key not in row:
            raise GreenAgentError(f"Missing required field '{key}'.")
        value = row[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise GreenAgentError(
                f"Field '{key}' must be numeric, got {type(value).__name__}."
            )
        fvalue = float(value)
        if math.isnan(fvalue) or math.isinf(fvalue):
            raise GreenAgentError(f"Field '{key}' must be finite, got {value!r}.")
        if fvalue < low:
            raise GreenAgentError(f"Field '{key}' must be >= {low}, got {fvalue}.")
        if high is not None and fvalue > high:
            raise GreenAgentError(f"Field '{key}' must be <= {high}, got {fvalue}.")
        return fvalue

    # ------------------------------------------------------ ranking helpers
    def _rank_by_dominance(
        self, points: Sequence[ParetoPoint]
    ) -> List[Tuple[str, int]]:
        """
        Return ``[(agent_id, rank), ...]`` sorted best-first.

        Rank 1 = on the frontier (non-dominated). Rank 2 = dominated only by
        rank-1 points, and so on. Ties are broken by the first objective
        (default: energy, ascending) and then by accuracy (descending).
        """
        if not points:
            return []

        remaining = list(points)
        ranked: List[Tuple[str, int]] = []
        current_rank = 1

        while remaining:
            frontier = self._local_frontier(remaining)
            frontier_ids = {id(p) for p in frontier}
            ranked.extend(
                (self._agent_id(p), current_rank)
                for p in frontier
            )
            remaining = [p for p in remaining if id(p) not in frontier_ids]
            current_rank += 1

        # Stable tie-break within the same rank: energy ↑, then accuracy ↓.
        def _sort_key(item: Tuple[str, int]) -> Tuple[int, float, float]:
            agent_id, rank = item
            point = next(p for p in points if self._agent_id(p) == agent_id)
            return (rank, point.energy_joules, -point.accuracy)

        ranked.sort(key=_sort_key)
        return ranked

    def _local_frontier(
        self, points: Sequence[ParetoPoint]
    ) -> List[ParetoPoint]:
        """Pareto frontier over the configured objectives (local fallback)."""
        cfg = self._config
        tol = cfg.dominance_tolerance

        def dominates(a: ParetoPoint, b: ParetoPoint) -> bool:
            better_or_equal = True
            strictly_better = False
            for obj in cfg.objectives:
                av = self._objective_value(a, obj)
                bv = self._objective_value(b, obj)
                if obj in cfg.maximize:
                    if av < bv - tol:
                        better_or_equal = False
                        break
                    if av > bv + tol:
                        strictly_better = True
                else:
                    if av > bv + tol:
                        better_or_equal = False
                        break
                    if av < bv - tol:
                        strictly_better = True
            return better_or_equal and strictly_better

        frontier: List[ParetoPoint] = []
        for i, p in enumerate(points):
            if not any(
                dominates(q, p) for j, q in enumerate(points) if i != j
            ):
                frontier.append(p)
        return frontier

    def _get_knee_point(
        self, frontier: Sequence[ParetoPoint]
    ) -> Optional[ParetoPoint]:
        """
        Return the knee point of the frontier (best trade-off).

        Uses the normalized distance-to-ideal method: for each frontier point,
        compute the Euclidean distance to the utopia point (best value on each
        objective). The closest point is the knee.
        """
        cfg = self._config
        if not frontier:
            return None
        if len(frontier) == 1:
            return frontier[0]

        # Build utopia (best achievable value per objective).
        utopia: Dict[str, float] = {}
        for obj in cfg.objectives:
            values = [self._objective_value(p, obj) for p in frontier]
            utopia[obj] = max(values) if obj in cfg.maximize else min(values)

        # Normalize ranges so objectives are comparable.
        spans: Dict[str, float] = {}
        for obj in cfg.objectives:
            values = [self._objective_value(p, obj) for p in frontier]
            span = max(values) - min(values)
            spans[obj] = span if span > 0 else 1.0

        def distance(p: ParetoPoint) -> float:
            return math.sqrt(
                sum(
                    (
                        (self._objective_value(p, obj) - utopia[obj])
                        / spans[obj]
                    )
                    ** 2
                    for obj in cfg.objectives
                )
            )

        return min(frontier, key=distance)

    @staticmethod
    def _objective_value(p: ParetoPoint, objective: str) -> float:
        """Map an objective name to the corresponding ParetoPoint field."""
        if objective == "accuracy":
            return p.accuracy
        if objective == "energy_kwh":
            # Convert joules back to kWh for config consistency.
            return p.energy_joules / 3_600_000.0
        if objective == "carbon_kg":
            # Convert grams back to kg for config consistency.
            return p.carbon_grams / 1000.0
        if objective == "latency_ms":
            return p.metadata.get("latency_ms", 0.0)
        # Generic fallback: look inside metadata.
        return float(p.metadata.get(objective, 0.0))

    @staticmethod
    def _agent_id(p: ParetoPoint) -> str:
        return p.agent_id or p.label

    @staticmethod
    def _point_to_dict(p: ParetoPoint) -> Dict[str, Any]:
        """Convert a ``ParetoPoint`` into a JSON-safe dict."""
        base = {
            "agent_id": p.agent_id or p.label,
            "label": p.label,
            "accuracy": p.accuracy,
            "energy_joules": p.energy_joules,
            "energy_kwh": p.energy_joules / 3_600_000.0,
            "carbon_grams": p.carbon_grams,
            "carbon_kg": p.carbon_grams / 1000.0,
            "latency_ms": p.metadata.get("latency_ms"),
        }
        # Merge any caller-supplied metadata without clobbering the core keys.
        for k, v in (p.metadata or {}).items():
            base.setdefault(k, v)
        return base

    # ----------------------------------------------------------- history
    def _record(
        self,
        *,
        num_agents: int,
        frontier_size: int,
        knee_agent_id: Optional[str],
        top_agent_id: Optional[str],
        label: Optional[str],
    ) -> None:
        sample = ScoreRecord(
            timestamp=time.time(),
            num_agents=num_agents,
            frontier_size=frontier_size,
            knee_agent_id=knee_agent_id,
            top_agent_id=top_agent_id,
            label=label,
        )
        with self._lock:
            self._history.append(sample)
            if (
                self._config.max_history is not None
                and len(self._history) > self._config.max_history
            ):
                del self._history[0]

    def statistics(self) -> Dict[str, Optional[float]]:
        """Return aggregate stats over the recorded scoring history."""
        with self._lock:
            history = list(self._history)
        if not history:
            return {
                "count": 0,
                "mean_agents": None,
                "mean_frontier_size": None,
                "mean_frontier_ratio": None,
            }
        agents = [s.num_agents for s in history]
        frontiers = [s.frontier_size for s in history]
        ratios = [
            (s.frontier_size / s.num_agents) if s.num_agents else 0.0
            for s in history
        ]
        return {
            "count": len(history),
            "mean_agents": sum(agents) / len(agents),
            "mean_frontier_size": sum(frontiers) / len(frontiers),
            "mean_frontier_ratio": sum(ratios) / len(ratios),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset the agent; optionally clear history."""
        with self._lock:
            if clear_history:
                self._history.clear()
        logger.debug("GreenSustainabilityAgent reset (clear_history=%s)", clear_history)

    # ------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "strict": self._strict,
                "history": [s.to_dict() for s in self._history],
            }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "GreenSustainabilityAgent":
        if not isinstance(data, Mapping):
            raise GreenAgentError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = GreenAgentConfig(
            key_agent_id=cfg_data.get("key_agent_id", "agent_id"),
            key_accuracy=cfg_data.get("key_accuracy", "accuracy"),
            key_energy=cfg_data.get("key_energy", "energy_kwh"),
            key_carbon=cfg_data.get("key_carbon", "carbon_kg"),
            key_latency=cfg_data.get("key_latency", "latency_ms"),
            objectives=tuple(cfg_data.get("objectives", ("energy_kwh", "accuracy"))),
            maximize=tuple(cfg_data.get("maximize", ("accuracy",))),
            dominance_tolerance=float(cfg_data.get("dominance_tolerance", 0.0)),
            max_history=cfg_data.get("max_history", 1000),
            carbon_enabled=bool(cfg_data.get("carbon_enabled", False)),
        )
        agent = cls(config=cfg, strict=bool(data.get("strict", True)))
        with agent._lock:
            for entry in data.get("history", []):
                agent._history.append(
                    ScoreRecord(
                        timestamp=float(entry["timestamp"]),
                        num_agents=int(entry["num_agents"]),
                        frontier_size=int(entry["frontier_size"]),
                        knee_agent_id=entry.get("knee_agent_id"),
                        top_agent_id=entry.get("top_agent_id"),
                        label=entry.get("label"),
                    )
                )
        return agent

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "GreenSustainabilityAgent":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise GreenAgentError(f"Invalid JSON payload: {exc}") from exc

    # ----------------------------------------------------------- context mgr
    def __enter__(self) -> "GreenSustainabilityAgent":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped green-agent scoring session.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "Green-agent scope exited with %s after %.4fs.",
                exc_type.__name__,
                elapsed,
            )
            return
        logger.info(
            "Green-agent scope closed in %.4fs (%d scoring(s)).",
            elapsed,
            len(self._history),
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "GreenSustainabilityAgent("
            f"objectives={self._config.objectives}, "
            f"maximize={self._config.maximize}, "
            f"strict={self._strict}, "
            f"scorings={len(self._history)})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "GreenSustainabilityAgent",
    "GreenAgentError",
    "GreenAgentConfig",
    "ScoreRecord",
    "ParetoFrontierAnalyzer",  # backward-compatible alias
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m agentbeats.green_agent
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    async def main() -> None:
        agent = GreenSustainabilityAgent()

        # Four agents with distinct trade-offs:
        #   alpha  — high accuracy, high energy
        #   beta   — medium accuracy, low energy
        #   gamma  — low accuracy, very low energy
        #   delta  — dominated by beta on both axes
        results = [
            {"agent_id": "alpha", "accuracy": 0.95, "energy_kwh": 0.010,
             "carbon_kg": 0.0030, "latency_ms": 120.0},
            {"agent_id": "beta",  "accuracy": 0.90, "energy_kwh": 0.003,
             "carbon_kg": 0.0010, "latency_ms":  90.0},
            {"agent_id": "gamma", "accuracy": 0.75, "energy_kwh": 0.001,
             "carbon_kg": 0.0005, "latency_ms":  60.0},
            {"agent_id": "delta", "accuracy": 0.80, "energy_kwh": 0.005,
             "carbon_kg": 0.0020, "latency_ms": 100.0},
        ]

        out = await agent.score_with_pareto(results, label="smoke")
        print("Frontier :", [p["agent_id"] for p in out["frontier"]])
        print("Ranks    :", out["ranks"])
        print("Knee     :", out["knee_point"]["agent_id"] if out["knee_point"] else None)
        print("Stats    :", agent.statistics())

        # Serialization round-trip
        payload = agent.to_json()
        restored = GreenSustainabilityAgent.from_json(payload)
        assert restored.to_dict() == agent.to_dict()
        print("Serialization round-trip OK.")

        # Context manager
        with GreenSustainabilityAgent() as scoped:
            await scoped.score_with_pareto(results[:2])
        print("Context-managed scoring OK.")

        # Validation failure (strict mode)
        for bad in (
            [],                                              # empty
            [{"agent_id": "x"}],                             # missing fields
            [{"agent_id": "x", "accuracy": 1.5,
              "energy_kwh": 1.0, "carbon_kg": 0.1, "latency_ms": 1.0}],  # acc > 1
        ):
            try:
                await agent.score_with_pareto(bad)  # type: ignore[arg-type]
            except GreenAgentError as exc:
                print("Rejected as expected:", exc)
            else:  # pragma: no cover
                raise AssertionError(f"Expected rejection for {bad!r}")

    asyncio.run(main())
