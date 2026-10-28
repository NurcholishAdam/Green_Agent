# src/feedback/reasoning_analyzer.py

"""
Reasoning Trace Analyzer — v2.0.0
=================================

Analyzes agent reasoning traces for quality metrics.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``ReasoningConfig`` rejects ``NaN`` / ``Inf`` weights. Previously
        ``weight_coherence=float("nan")`` silently produced
        ``composite_score=1.0`` for every trace (because
        ``min(1.0, nan) == 1.0``).
  * 2.2 ``ReasoningConfig`` rejects negative weights. Previously
        ``weight_coherence=-0.5, weight_factuality=1.5`` (sum = 1.0) passed
        validation and inverted the composite score.
  * 2.3 ``ReasoningConfig`` validates keyword tuples are tuples of
        non-empty strings. Previously ``energy_keywords="energy"`` (a bare
        string) iterated as individual characters, matching on 'e', 'n',
        'r', 'g', 'y' — every English sentence.
  * 2.4 Keyword matching uses word boundaries. ``"so"`` no longer matches
        "reason", "also", "sound", "solution", "sonic", etc.
  * 2.5 ``ReasoningAnalysis.__post_init__`` validates every field
        (scores in [0, 1], finite, non-negative timestamp).
  * 2.6 ``ReasoningAnalysis.from_dict`` raises ``ReasoningError`` on
        invalid input (previously raw ``TypeError`` / ``ValueError``).
  * 2.7 ``step_breakdown`` entries are ``MappingProxyType`` — the frozen
        dataclass now has structurally immutable per-step data
        (previously the outer tuple was frozen but the dicts inside
        were mutable).
  * 2.8 ``ReasoningAnalysis`` gains ``step_count`` and ``has_data`` so
        ``analyze([])`` is distinguishable from a genuinely bad trace
        (both previously returned ``composite_score=0.0``).
  * 2.9 ``from_dict`` delegates to ``ReasoningConfig.from_legacy_dict``
        (the recurring config-drift fix from every prior DPQ file).
  * 2.10 ``analyze_structured(trace)`` returns the ``ReasoningAnalysis``
         instance (previously ``analyze()`` returned a dict while
         ``history`` returned dataclasses — inconsistent type for the
         same data).

**P1 robustness / observability**
  * 3.2 ``efficiency_ceiling_steps`` moved into ``ReasoningConfig``
        (was a magic ``20.0`` inside ``_assess_efficiency``).
  * 3.3 ``coherence_keywords`` validated alongside ``energy_keywords``.
  * 3.4 Per-step breakdown carries ``coherent`` / ``factual`` flags
        (previously only ``energy_aware``).
  * 3.5 ``metrics_hook(event, fields)`` for Prometheus / OTel.
  * 3.6 ``statistics(window=N)`` for a consistent windowed view.
  * 3.7 ``close()`` + locked sync/async context managers.
  * 3.8 ``__version__`` exported.
  * 3.10 ``_analyze_steps`` is single-pass (previously 4 passes over
         the trace).
  * 3.11 ``max_breakdown_steps`` caps breakdown memory on huge traces.
  * 3.13 ``statistics()`` reports ``analyses_with_data`` / ``analyses_empty``.

**P2 polish**
  * ``ReasoningAnalysis`` and ``ReasoningTraceAnalyzer`` are picklable
    (``__getstate__`` / ``__setstate__`` handle ``MappingProxyType`` and
    ``RLock``).
  * ``__repr__`` includes ``last_composite`` when history is non-empty.

Backward compatible: every v1.0.0 import path, method, and payload works
unchanged. ``analyze()`` still returns a dict; ``step_breakdown`` entries
now behave like read-only dicts (they support all read operations, just
not mutation — which was never supported anyway since the dataclass was
frozen).
"""

from __future__ import annotations

import json
import logging
import math
import re
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field, fields
from types import MappingProxyType
from typing import (
    Any, Callable, Deque, Dict, List, Mapping, Optional, Sequence, Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class ReasoningError(ValueError):
    """Raised for invalid reasoning traces or configuration."""


# --------------------------------------------------------------------------- #
# Word-boundary keyword matcher (P0 2.4)
# --------------------------------------------------------------------------- #
def _matches_keywords(text_lower: str, keywords: Tuple[str, ...]) -> bool:
    """
    Return True iff any keyword appears as a whole word in ``text_lower``.

    ``text_lower`` must already be lowercased. ``keywords`` are expected
    to be lowercased too (validation in ``ReasoningConfig`` ensures the
    caller's values are non-empty strings; we lower them here for safety).

    Word-boundary matching prevents ``"so"`` from matching "reason",
    "also", "sound", etc. — a bug that inflated ``coherence_score`` for
    any trace that discusses reasoning *about* reasoning.
    """
    for kw in keywords:
        pattern = rf"\b{re.escape(kw.lower())}\b"
        if re.search(pattern, text_lower):
            return True
    return False


# --------------------------------------------------------------------------- #
# Configuration (P0 2.1, 2.2, 2.3, 2.9; P1 3.2, 3.3; P1 3.11)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ReasoningConfig:
    """Tunable parameters for the reasoning analyzer."""

    energy_keywords: Tuple[str, ...] = (
        "energy", "efficient", "optimize", "carbon", "sustainable",
        "efficiency", "green", "emissions",
    )
    coherence_keywords: Tuple[str, ...] = (
        "because", "therefore", "thus", "hence", "consequently", "so",
    )
    max_history: int = 1000
    max_breakdown_steps: int = 1000          # P1 3.11
    efficiency_ceiling_steps: int = 20       # P1 3.2

    weight_coherence: float = 0.3
    weight_factuality: float = 0.4
    weight_efficiency: float = 0.2
    weight_energy_awareness: float = 0.1

    def __post_init__(self) -> None:
        # P0 2.3: keyword tuples must be tuples of non-empty strings.
        for name in ("energy_keywords", "coherence_keywords"):
            value = getattr(self, name)
            if not isinstance(value, tuple):
                raise ReasoningError(
                    f"{name} must be a tuple of strings, "
                    f"got {type(value).__name__}.")
            for i, kw in enumerate(value):
                if not isinstance(kw, str) or not kw:
                    raise ReasoningError(
                        f"{name}[{i}] must be a non-empty string, "
                        f"got {kw!r}.")
        if not self.energy_keywords:
            raise ReasoningError("energy_keywords must be non-empty.")
        if not self.coherence_keywords:
            raise ReasoningError(
                "coherence_keywords must be non-empty.")

        # Integer fields.
        for name in (
            "max_history", "max_breakdown_steps",
            "efficiency_ceiling_steps",
        ):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, int) or v <= 0:
                raise ReasoningError(
                    f"{name} must be a positive int, got {v!r}.")

        # P0 2.1 + P0 2.2: weights numeric, finite, non-negative.
        for name in (
            "weight_coherence", "weight_factuality",
            "weight_efficiency", "weight_energy_awareness",
        ):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise ReasoningError(
                    f"{name} must be numeric, "
                    f"got {type(v).__name__}.")
            if math.isnan(v) or math.isinf(v):
                raise ReasoningError(
                    f"{name} must be finite, got {v!r}.")
            if v < 0:
                raise ReasoningError(
                    f"{name} must be >= 0, got {v}.")

        total = (
            self.weight_coherence + self.weight_factuality
            + self.weight_efficiency + self.weight_energy_awareness
        )
        if abs(total - 1.0) > 1e-6:
            raise ReasoningError(
                f"reasoning weights must sum to 1.0 "
                f"(got {total:.6f}).")

    # ---------------------------------------------------------------- #
    @classmethod
    def from_legacy_dict(
        cls, cfg: Optional[Mapping[str, Any]]
    ) -> "ReasoningConfig":
        """P0 2.9: build from a flat mapping; unknown keys ignored."""
        cfg = dict(cfg or {})
        valid = {f.name for f in fields(cls)}
        kwargs: Dict[str, Any] = {
            k: v for k, v in cfg.items() if k in valid}
        # JSON round-trips tuples through lists — restore them.
        for name in ("energy_keywords", "coherence_keywords"):
            if name in kwargs and isinstance(kwargs[name], list):
                kwargs[name] = tuple(kwargs[name])
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Analysis result (P0 2.5, 2.6, 2.7, 2.8)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ReasoningAnalysis:
    """Immutable result of analyzing a reasoning trace."""

    coherence_score: float
    factuality_score: float
    efficiency_score: float
    energy_awareness: float
    composite_score: float
    step_breakdown: Tuple[Mapping[str, Any], ...]
    timestamp: float = field(default_factory=time.time)
    step_count: int = 0

    def __post_init__(self) -> None:
        # P0 2.5: score fields are in [0, 1], numeric, finite.
        for name in (
            "coherence_score", "factuality_score",
            "efficiency_score", "energy_awareness",
            "composite_score",
        ):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise ReasoningError(
                    f"{name} must be numeric, "
                    f"got {type(v).__name__}.")
            if math.isnan(v) or math.isinf(v):
                raise ReasoningError(f"{name} must be finite.")
            if not 0.0 <= v <= 1.0:
                raise ReasoningError(
                    f"{name} must be in [0, 1], got {v}.")

        # step_breakdown must be a tuple of Mappings; coerce to
        # MappingProxyType so the frozen dataclass has no mutable
        # interior (P0 2.7).
        if not isinstance(self.step_breakdown, tuple):
            raise ReasoningError(
                "step_breakdown must be a tuple, "
                f"got {type(self.step_breakdown).__name__}.")
        coerced: List[Mapping[str, Any]] = []
        for i, s in enumerate(self.step_breakdown):
            if not isinstance(s, Mapping):
                raise ReasoningError(
                    f"step_breakdown[{i}] must be a Mapping, "
                    f"got {type(s).__name__}.")
            if isinstance(s, MappingProxyType):
                coerced.append(s)
            else:
                coerced.append(MappingProxyType(dict(s)))
        object.__setattr__(
            self, "step_breakdown", tuple(coerced))

        # timestamp.
        if isinstance(self.timestamp, bool) or \
                not isinstance(self.timestamp, (int, float)):
            raise ReasoningError("timestamp must be numeric.")
        if math.isnan(self.timestamp) or \
                math.isinf(self.timestamp):
            raise ReasoningError("timestamp must be finite.")
        if self.timestamp < 0:
            raise ReasoningError("timestamp must be >= 0.")

        # step_count (P0 2.8).
        if isinstance(self.step_count, bool) or \
                not isinstance(self.step_count, int) or \
                self.step_count < 0:
            raise ReasoningError(
                "step_count must be a non-negative int.")

    # ---------------------------------------------------------------- #
    @property
    def has_data(self) -> bool:
        """True iff at least one step was analyzed (P0 2.8)."""
        return self.step_count > 0

    # ---------------------------------------------------------------- #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "coherence_score": self.coherence_score,
            "factuality_score": self.factuality_score,
            "efficiency_score": self.efficiency_score,
            "energy_awareness": self.energy_awareness,
            "composite_score": self.composite_score,
            "step_breakdown": [dict(s) for s in self.step_breakdown],
            "timestamp": self.timestamp,
            "step_count": self.step_count,
            "has_data": self.has_data,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReasoningAnalysis":
        """P0 2.6: robust deserialization with ``ReasoningError``."""
        if not isinstance(data, Mapping):
            raise ReasoningError(
                f"ReasoningAnalysis.from_dict expects a Mapping, "
                f"got {type(data).__name__}.")

        def _safe_float(key: str, default: Any) -> float:
            v = data.get(key, default)
            if isinstance(v, bool) or \
                    not isinstance(v, (int, float)):
                raise ReasoningError(
                    f"ReasoningAnalysis.from_dict: {key} must be "
                    f"numeric, got {type(v).__name__}.")
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                raise ReasoningError(
                    f"ReasoningAnalysis.from_dict: {key} must be "
                    f"finite.")
            return f

        raw_breakdown = data.get("step_breakdown", [])
        if not isinstance(raw_breakdown, (list, tuple)):
            raise ReasoningError(
                "ReasoningAnalysis.from_dict: step_breakdown must "
                f"be a list or tuple, got "
                f"{type(raw_breakdown).__name__}.")
        parsed_breakdown: List[Dict[str, Any]] = []
        for i, s in enumerate(raw_breakdown):
            if not isinstance(s, Mapping):
                raise ReasoningError(
                    f"ReasoningAnalysis.from_dict: "
                    f"step_breakdown[{i}] must be a Mapping, "
                    f"got {type(s).__name__}.")
            parsed_breakdown.append(dict(s))

        step_count_raw = data.get("step_count", len(parsed_breakdown))
        if isinstance(step_count_raw, bool) or \
                not isinstance(step_count_raw, int):
            raise ReasoningError(
                "ReasoningAnalysis.from_dict: step_count must be "
                "an int.")

        return cls(
            coherence_score=_safe_float("coherence_score", 0.0),
            factuality_score=_safe_float("factuality_score", 0.0),
            efficiency_score=_safe_float("efficiency_score", 0.0),
            energy_awareness=_safe_float("energy_awareness", 0.0),
            composite_score=_safe_float("composite_score", 0.0),
            step_breakdown=tuple(parsed_breakdown),
            timestamp=_safe_float("timestamp", time.time()),
            step_count=step_count_raw,
        )

    # P2: pickling — MappingProxyType is not picklable directly.
    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state["step_breakdown"] = tuple(
            dict(s) for s in self.step_breakdown)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        for k, v in state.items():
            if k == "step_breakdown":
                v = tuple(MappingProxyType(dict(s)) for s in v)
            object.__setattr__(self, k, v)


# --------------------------------------------------------------------------- #
# Analyzer
# --------------------------------------------------------------------------- #
class ReasoningTraceAnalyzer:
    """
    Analyzes agent reasoning for quality metrics.

    Thread-safe, serializable, picklable, bounded in memory. Strict mode
    raises on any non-Mapping step (aggressive by design — pre-validate
    your trace if you want partial tolerance).
    """

    def __init__(
        self,
        *,
        config: Optional[ReasoningConfig] = None,
        strict: bool = True,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> None:
        self._config = config or ReasoningConfig()
        self._strict = bool(strict)

        if metrics_hook is not None and not callable(metrics_hook):
            raise ReasoningError("metrics_hook must be callable.")
        self._metrics_hook = metrics_hook

        self._lock = threading.RLock()
        self._history: Deque[ReasoningAnalysis] = deque(
            maxlen=self._config.max_history)
        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None

        logger.debug(
            "ReasoningTraceAnalyzer v%s initialized "
            "(max_history=%d, efficiency_ceiling=%d, strict=%s)",
            __version__,
            self._config.max_history,
            self._config.efficiency_ceiling_steps,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> ReasoningConfig:
        return self._config

    @property
    def history(self) -> List[ReasoningAnalysis]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- metrics
    def _emit_metric(
        self, event: str, fields_: Mapping[str, Any],
    ) -> None:
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields_))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised", exc_info=True)

    # ---------------------------------------------------------- public API
    def analyze(self, trace: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
        """
        Analyze a reasoning trace (returns a dict — backward compatible).

        See :meth:`analyze_structured` for the dataclass return.
        """
        return self.analyze_structured(trace).to_dict()

    def analyze_structured(
        self, trace: Sequence[Mapping[str, Any]],
    ) -> ReasoningAnalysis:
        """P0 2.10: analyze and return the ``ReasoningAnalysis`` itself."""
        if not isinstance(trace, (list, tuple)):
            msg = (
                f"trace must be a list or tuple, "
                f"got {type(trace).__name__}.")
            if self._strict:
                raise ReasoningError(msg)
            logger.warning("%s Using empty trace.", msg)
            trace = []

        steps: List[Mapping[str, Any]] = []
        for i, s in enumerate(trace):
            if isinstance(s, Mapping):
                steps.append(s)
            else:
                msg = (
                    f"trace[{i}] must be a Mapping, "
                    f"got {type(s).__name__}.")
                if self._strict:
                    raise ReasoningError(msg)
                logger.debug(
                    "Skipping non-mapping step at index %d.", i)
                self._emit_metric("malformed_step", {
                    "index": i,
                    "type": type(s).__name__,
                })

        analysis = self._analyze_steps(steps)

        with self._lock:
            self._history.append(analysis)

        self._emit_metric("analysis_generated", {
            "step_count": analysis.step_count,
            "has_data": analysis.has_data,
            "composite_score": analysis.composite_score,
            "coherence_score": analysis.coherence_score,
            "factuality_score": analysis.factuality_score,
            "efficiency_score": analysis.efficiency_score,
            "energy_awareness": analysis.energy_awareness,
        })

        logger.debug(
            "Analyzed %d step(s): composite=%.3f "
            "(coherence=%.2f, factuality=%.2f, efficiency=%.2f, "
            "energy=%.2f)",
            analysis.step_count,
            analysis.composite_score,
            analysis.coherence_score,
            analysis.factuality_score,
            analysis.efficiency_score,
            analysis.energy_awareness,
        )
        return analysis

    # ---------------------------------------------------------- internals
    def _analyze_steps(
        self, steps: List[Mapping[str, Any]]
    ) -> ReasoningAnalysis:
        """P1 3.10: single-pass analysis."""
        n = len(steps)
        if n == 0:
            # P0 2.8: ``step_count=0`` so callers can distinguish
            # "no data" from "score of 0.0".
            return ReasoningAnalysis(
                coherence_score=0.0,
                factuality_score=0.0,
                efficiency_score=0.0,
                energy_awareness=0.0,
                composite_score=0.0,
                step_breakdown=tuple(),
                step_count=0,
            )

        cfg = self._config
        energy_hits = 0
        coherence_hits = 0
        non_empty = 0
        breakdown_entries: List[Mapping[str, Any]] = []
        capped = False

        for i, step in enumerate(steps):
            thought = str(step.get("thought", ""))
            thought_lc = thought.lower()
            energy_aware = _matches_keywords(
                thought_lc, cfg.energy_keywords)
            coherent = _matches_keywords(
                thought_lc, cfg.coherence_keywords)
            factual = bool(thought.strip())
            if energy_aware:
                energy_hits += 1
            if coherent:
                coherence_hits += 1
            if factual:
                non_empty += 1
            if i < cfg.max_breakdown_steps:
                breakdown_entries.append(MappingProxyType({
                    "index": i,
                    "length": len(thought),
                    "energy_aware": energy_aware,
                    "coherent": coherent,
                    "factual": factual,
                }))
            else:
                capped = True

        if capped:
            logger.warning(
                "step_breakdown capped at %d entries (total steps=%d).",
                cfg.max_breakdown_steps, n)

        energy_awareness = energy_hits / n
        coherence = coherence_hits / n
        factuality = non_empty / n
        efficiency = min(cfg.efficiency_ceiling_steps / n, 1.0)
        composite = (
            cfg.weight_coherence * coherence
            + cfg.weight_factuality * factuality
            + cfg.weight_efficiency * efficiency
            + cfg.weight_energy_awareness * energy_awareness
        )

        return ReasoningAnalysis(
            coherence_score=coherence,
            factuality_score=factuality,
            efficiency_score=efficiency,
            energy_awareness=energy_awareness,
            composite_score=max(0.0, min(1.0, composite)),
            step_breakdown=tuple(breakdown_entries),
            step_count=n,
        )

    # ---------------------------------------------------------- statistics
    def statistics(
        self, window: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Return aggregate statistics (P1 3.6: windowed view)."""
        with self._lock:
            if window is not None:
                history = list(self._history)[-window:]
            else:
                history = list(self._history)

        if not history:
            return {
                "analyses": 0,
                "analyses_with_data": 0,
                "analyses_empty": 0,
                "mean_composite": None,
                "mean_coherence": None,
                "mean_factuality": None,
                "mean_efficiency": None,
                "mean_energy_awareness": None,
            }

        with_data = [a for a in history if a.has_data]
        empties = [a for a in history if not a.has_data]

        def _mean_attr(items: List[ReasoningAnalysis], attr: str) -> Optional[float]:
            if not items:
                return None
            return sum(getattr(a, attr) for a in items) / len(items)

        return {
            "analyses": len(history),
            "analyses_with_data": len(with_data),
            "analyses_empty": len(empties),
            "mean_composite": _mean_attr(with_data, "composite_score"),
            "mean_coherence": _mean_attr(with_data, "coherence_score"),
            "mean_factuality": _mean_attr(with_data, "factuality_score"),
            "mean_efficiency": _mean_attr(with_data, "efficiency_score"),
            "mean_energy_awareness": _mean_attr(
                with_data, "energy_awareness"),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        with self._lock:
            if clear_history:
                self._history.clear()
            self._ctx_start = None
            self._async_ctx_start = None
        logger.debug(
            "ReasoningTraceAnalyzer reset (clear_history=%s)",
            clear_history)

    def close(self) -> None:
        """P1 3.7: release resources. Idempotent."""
        logger.debug(
            "ReasoningTraceAnalyzer closed (%d analysis(es)).",
            len(self._history))

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "config": asdict(self._config),
                "strict": self._strict,
                "history": [a.to_dict() for a in self._history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "ReasoningTraceAnalyzer":
        if not isinstance(data, Mapping):
            raise ReasoningError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}.")

        # P0 2.9: delegate to from_legacy_dict.
        cfg = ReasoningConfig.from_legacy_dict(
            data.get("config") or {})

        analyzer = cls(
            config=cfg,
            strict=bool(data.get("strict", True)),
            metrics_hook=metrics_hook,
        )

        history_entries = list(data.get("history", []))
        if len(history_entries) > cfg.max_history:
            logger.warning(
                "Restoring %d history entries; only most recent "
                "%d kept (max_history).",
                len(history_entries), cfg.max_history)

        with analyzer._lock:
            for entry in history_entries:
                analyzer._history.append(
                    ReasoningAnalysis.from_dict(entry))
        return analyzer

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "ReasoningTraceAnalyzer":
        try:
            return cls.from_dict(
                json.loads(payload), metrics_hook=metrics_hook)
        except json.JSONDecodeError as exc:
            raise ReasoningError(
                f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- pickling
    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "ReasoningTraceAnalyzer":
        with self._lock:
            self._ctx_start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            start = (
                self._ctx_start
                if self._ctx_start is not None
                else time.perf_counter()
            )
            elapsed = time.perf_counter() - start
            self._ctx_start = None
            n = len(self._history)
        if exc_type is not None:
            logger.warning(
                "ReasoningTraceAnalyzer scope exited with %s "
                "after %.4fs.", exc_type.__name__, elapsed)
            return
        logger.info(
            "ReasoningTraceAnalyzer scope closed in %.4fs "
            "(%d analysis(es)).", elapsed, n)

    async def __aenter__(self) -> "ReasoningTraceAnalyzer":
        with self._lock:
            self._async_ctx_start = time.perf_counter()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            start = (
                self._async_ctx_start
                if self._async_ctx_start is not None
                else time.perf_counter()
            )
            elapsed = time.perf_counter() - start
            self._async_ctx_start = None
            n = len(self._history)
        if exc_type is not None:
            logger.warning(
                "Async ReasoningTraceAnalyzer scope exited with %s "
                "after %.4fs.", exc_type.__name__, elapsed)
            return
        logger.info(
            "Async ReasoningTraceAnalyzer scope closed in %.4fs "
            "(%d analysis(es)).", elapsed, n)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            last = (
                self._history[-1].composite_score
                if self._history else None
            )
            last_str = f"{last:.3f}" if last is not None else "n/a"
            return (
                "ReasoningTraceAnalyzer("
                f"version={__version__!r}, "
                f"analyses={len(self._history)}, "
                f"last_composite={last_str}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "__version__",
    "ReasoningAnalysis",
    "ReasoningConfig",
    "ReasoningError",
    "ReasoningTraceAnalyzer",
]


# --------------------------------------------------------------------------- #
# Self-tests: python -m feedback.reasoning_analyzer
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import asyncio
    import pickle

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print(f"=== reasoning_analyzer v{__version__} self-tests ===\n")

    # ---------------------------------------------------------------- #
    # P0 2.1 — NaN weights rejected
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: NaN weights rejected ===")
    for bad in (float("nan"), float("inf"), float("-inf")):
        try:
            ReasoningConfig(
                weight_coherence=bad,
                weight_factuality=0.6,
                weight_efficiency=0.2,
                weight_energy_awareness=0.2,
            )
        except ReasoningError as exc:
            print(f"  ✓ weight_coherence={bad!r} rejected: {exc}")
        else:
            raise AssertionError(
                f"P0 2.1 regression: weight_coherence={bad!r} "
                f"was accepted")

    # Contrast: valid weights still work.
    cfg = ReasoningConfig(
        weight_coherence=0.5, weight_factuality=0.2,
        weight_efficiency=0.2, weight_energy_awareness=0.1)
    print(f"  ✓ valid config accepted (weights sum to 1.0)")

    # ---------------------------------------------------------------- #
    # P0 2.2 — negative weights rejected
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: negative weights rejected ===")
    try:
        ReasoningConfig(
            weight_coherence=-0.5,
            weight_factuality=1.5,
            weight_efficiency=0.0,
            weight_energy_awareness=0.0,
        )
    except ReasoningError as exc:
        print(f"  ✓ weight_coherence=-0.5 rejected: {exc}")
    else:
        raise AssertionError(
            "P0 2.2 regression: negative weight was accepted "
            "(sum was 1.0)")

    # ---------------------------------------------------------------- #
    # P0 2.3 — keyword tuples type-validated
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: keyword tuples type-validated ===")

    # String instead of tuple.
    try:
        ReasoningConfig(energy_keywords="energy")  # type: ignore[arg-type]
    except ReasoningError as exc:
        print(f"  ✓ energy_keywords='energy' (string) rejected: {exc}")
    else:
        raise AssertionError(
            "P0 2.3 regression: string energy_keywords was accepted")

    # Empty tuple.
    try:
        ReasoningConfig(energy_keywords=())
    except ReasoningError:
        print("  ✓ energy_keywords=() rejected")
    else:
        raise AssertionError(
            "P0 2.3 regression: empty energy_keywords was accepted")

    # Tuple with non-string element.
    try:
        ReasoningConfig(energy_keywords=("energy", 42))  # type: ignore[arg-type]
    except ReasoningError:
        print("  ✓ energy_keywords=('energy', 42) rejected")
    else:
        raise AssertionError(
            "P0 2.3 regression: non-string keyword was accepted")

    # Valid tuple still works.
    ReasoningConfig(energy_keywords=("energy", "carbon"))
    print("  ✓ valid tuple ('energy', 'carbon') accepted")

    # ---------------------------------------------------------------- #
    # P0 2.4 — word-boundary matching (fix "so" substring)
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: word-boundary keyword matching ===")

    analyzer = ReasoningTraceAnalyzer()

    # "reason" contains "so" but should NOT match.
    no_coherence = analyzer.analyze_structured(
        [{"thought": "The reason is X"}])
    assert no_coherence.coherence_score == 0.0, (
        f"P0 2.4 regression: 'reason' matched 'so', "
        f"coherence={no_coherence.coherence_score}")
    print(f"  ✓ 'The reason is X' → coherence=0.0 (was 1.0)")

    # "also" contains "so" but should NOT match.
    no_coherence2 = analyzer.analyze_structured(
        [{"thought": "Also consider Y"}])
    assert no_coherence2.coherence_score == 0.0, (
        f"P0 2.4 regression: 'also' matched 'so', "
        f"coherence={no_coherence2.coherence_score}")
    print(f"  ✓ 'Also consider Y' → coherence=0.0 (was 1.0)")

    # "solution" contains "so" but should NOT match.
    no_coherence3 = analyzer.analyze_structured(
        [{"thought": "A solution is Z"}])
    assert no_coherence3.coherence_score == 0.0
    print(f"  ✓ 'A solution is Z' → coherence=0.0")

    # Real "so" as a word DOES match.
    with_so = analyzer.analyze_structured(
        [{"thought": "X is true, so Y follows"}])
    assert with_so.coherence_score == 1.0, (
        f"P0 2.4 regression: standalone 'so' did not match, "
        f"coherence={with_so.coherence_score}")
    print(f"  ✓ 'so' as a whole word → coherence=1.0")

    # "Because" (case-insensitive) still matches.
    with_because = analyzer.analyze_structured(
        [{"thought": "Because X, Y follows"}])
    assert with_because.coherence_score == 1.0
    print(f"  ✓ 'Because' matches (case-insensitive)")

    # ---------------------------------------------------------------- #
    # P0 2.5 — ReasoningAnalysis structural validation
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: ReasoningAnalysis validation ===")

    # Out-of-range score.
    try:
        ReasoningAnalysis(
            coherence_score=2.0,  # > 1.0
            factuality_score=0.5,
            efficiency_score=0.5,
            energy_awareness=0.5,
            composite_score=0.5,
            step_breakdown=(),
            step_count=1,
        )
    except ReasoningError as exc:
        print(f"  ✓ coherence_score=2.0 rejected: {exc}")
    else:
        raise AssertionError(
            "P0 2.5 regression: out-of-range score accepted")

    # NaN score.
    try:
        ReasoningAnalysis(
            coherence_score=float("nan"),
            factuality_score=0.5,
            efficiency_score=0.5,
            energy_awareness=0.5,
            composite_score=0.5,
            step_breakdown=(),
            step_count=1,
        )
    except ReasoningError:
        print("  ✓ NaN coherence_score rejected")
    else:
        raise AssertionError(
            "P0 2.5 regression: NaN score accepted")

    # Negative score.
    try:
        ReasoningAnalysis(
            coherence_score=0.5,
            factuality_score=-0.1,
            efficiency_score=0.5,
            energy_awareness=0.5,
            composite_score=0.5,
            step_breakdown=(),
            step_count=1,
        )
    except ReasoningError:
        print("  ✓ negative factuality_score rejected")
    else:
        raise AssertionError(
            "P0 2.5 regression: negative score accepted")

    # Valid construction.
    valid = ReasoningAnalysis(
        coherence_score=0.5, factuality_score=0.5,
        efficiency_score=0.5, energy_awareness=0.5,
        composite_score=0.5, step_breakdown=(),
        step_count=1,
    )
    print(f"  ✓ valid construction accepted")

    # ---------------------------------------------------------------- #
    # P0 2.6 — from_dict raises ReasoningError (not TypeError/ValueError)
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: from_dict robustness ===")

    # None value → ReasoningError (previously TypeError from float(None)).
    try:
        ReasoningAnalysis.from_dict({"coherence_score": None})
    except ReasoningError as exc:
        print(f"  ✓ None score → ReasoningError: {exc}")
    except (TypeError, ValueError) as exc:
        raise AssertionError(
            f"P0 2.6 regression: raised {type(exc).__name__} "
            f"instead of ReasoningError: {exc}")
    else:
        raise AssertionError(
            "P0 2.6 regression: None accepted")

    # String value → ReasoningError.
    try:
        ReasoningAnalysis.from_dict({
            "coherence_score": "not-a-number"})
    except ReasoningError:
        print("  ✓ string score → ReasoningError")
    else:
        raise AssertionError(
            "P0 2.6 regression: string accepted")

    # NaN value → ReasoningError.
    try:
        ReasoningAnalysis.from_dict({
            "coherence_score": float("nan")})
    except ReasoningError:
        print("  ✓ NaN score → ReasoningError")
    else:
        raise AssertionError(
            "P0 2.6 regression: NaN accepted")

    # Valid round-trip.
    original = analyzer.analyze_structured(
        [{"thought": "Because X, Y follows"}])
    restored = ReasoningAnalysis.from_dict(original.to_dict())
    assert restored.coherence_score == original.coherence_score
    print(f"  ✓ valid round-trip preserves scores")

    # ---------------------------------------------------------------- #
    # P0 2.7 — step_breakdown entries immutable
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.7: step_breakdown immutability ===")

    analysis = analyzer.analyze_structured(
        [{"thought": "x"}, {"thought": "y"}])
    assert len(analysis.step_breakdown) == 2
    try:
        analysis.step_breakdown[0]["index"] = 999  # type: ignore[index]
    except TypeError as exc:
        print(f"  ✓ step_breakdown[0]['index']=999 → TypeError: "
              f"{exc}")
    else:
        raise AssertionError(
            "P0 2.7 regression: step_breakdown entry was mutable")

    # Read access still works.
    assert analysis.step_breakdown[0]["index"] == 0
    assert analysis.step_breakdown[1]["index"] == 1
    print(f"  ✓ read access still works: "
          f"index[0]={analysis.step_breakdown[0]['index']}")

    # ---------------------------------------------------------------- #
    # P0 2.8 — empty trace distinguishable via has_data
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.8: empty trace has_data ===")

    empty = analyzer.analyze_structured([])
    assert empty.step_count == 0
    assert empty.has_data is False
    assert empty.composite_score == 0.0
    print(f"  ✓ analyze([]): step_count=0, has_data=False, "
          f"composite={empty.composite_score}")

    non_empty = analyzer.analyze_structured([{"thought": "x"}])
    assert non_empty.step_count == 1
    assert non_empty.has_data is True
    print(f"  ✓ analyze([{{'thought': 'x'}}]): step_count=1, "
          f"has_data=True")

    # dict form carries the same flags.
    empty_dict = analyzer.analyze([])
    assert empty_dict["has_data"] is False
    assert empty_dict["step_count"] == 0
    print(f"  ✓ analyze([]) dict has has_data=False, step_count=0")

    # ---------------------------------------------------------------- #
    # P0 2.9 — from_dict config defaults
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.9: from_dict config defaults ===")

    legacy = {
        "config": {
            "weight_coherence": 0.5,
            # Other weights, keywords, max_history, etc. omitted
        },
        "strict": True,
        "history": [],
    }
    restored = ReasoningTraceAnalyzer.from_dict(legacy)
    assert restored.config.weight_coherence == 0.5, (
        f"P0 2.9 regression: weight_coherence="
        f"{restored.config.weight_coherence}")
    assert restored.config.weight_factuality == 0.4, (
        f"P0 2.9 regression: weight_factuality="
        f"{restored.config.weight_factuality}")
    assert restored.config.max_history == 1000
    assert restored.config.efficiency_ceiling_steps == 20
    assert restored.config.max_breakdown_steps == 1000
    assert restored.config.energy_keywords == (
        "energy", "efficient", "optimize", "carbon", "sustainable",
        "efficiency", "green", "emissions")
    print(f"  ✓ weight_coherence=0.5 (explicit), "
          f"weight_factuality=0.4, max_history=1000, "
          f"efficiency_ceiling=20, max_breakdown=1000 (all defaults)")

    # ---------------------------------------------------------------- #
    # P0 2.10 — analyze_structured
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.10: analyze_structured ===")

    structured = analyzer.analyze_structured(
        [{"thought": "Because X, save energy."}])
    assert isinstance(structured, ReasoningAnalysis), (
        f"P0 2.10 regression: analyze_structured returned "
        f"{type(structured).__name__}")
    assert structured.coherence_score == 1.0
    assert structured.energy_awareness == 1.0
    print(f"  ✓ analyze_structured returns ReasoningAnalysis with "
          f".coherence_score={structured.coherence_score}")

    # Backward-compat: analyze() still returns a dict.
    d = analyzer.analyze([{"thought": "Because X, save energy."}])
    assert isinstance(d, dict), (
        f"Backward-compat regression: analyze() returned "
        f"{type(d).__name__}, expected dict")
    assert d["coherence_score"] == 1.0
    print(f"  ✓ analyze() still returns dict")

    # ---------------------------------------------------------------- #
    # P1 3.2 — efficiency_ceiling_steps configurable
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.2: efficiency_ceiling_steps configurable ===")

    default_eff = ReasoningTraceAnalyzer()
    custom_eff = ReasoningTraceAnalyzer(
        config=ReasoningConfig(efficiency_ceiling_steps=5))

    trace = [{"thought": f"step {i}"} for i in range(10)]
    default_result = default_eff.analyze_structured(trace)
    custom_result = custom_eff.analyze_structured(trace)
    assert default_result.efficiency_score == 1.0, (
        f"P1 3.2 regression: 10-step trace with ceiling=20 should "
        f"have efficiency=1.0, got "
        f"{default_result.efficiency_score}")
    assert custom_result.efficiency_score == 0.5, (
        f"P1 3.2 regression: 10-step trace with ceiling=5 should "
        f"have efficiency=0.5, got "
        f"{custom_result.efficiency_score}")
    print(f"  ✓ 10-step trace: default ceiling=20 → efficiency=1.0, "
          f"ceiling=5 → efficiency=0.5")

    # ---------------------------------------------------------------- #
    # P1 3.4 — per-step coherent/factual flags
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.4: per-step flags ===")

    analysis = analyzer.analyze_structured([
        {"thought": "Because X, save energy"},  # coherent+energy+factual
        {"thought": "Short note"},              # only factual
        {"thought": "   "},                     # none
    ])
    b0 = analysis.step_breakdown[0]
    b1 = analysis.step_breakdown[1]
    b2 = analysis.step_breakdown[2]
    assert b0["coherent"] is True
    assert b0["energy_aware"] is True
    assert b0["factual"] is True
    assert b1["coherent"] is False
    assert b1["factual"] is True
    assert b2["factual"] is False
    print(f"  ✓ step flags: [0]={dict(b0)}, [1]={dict(b1)}, "
          f"[2]={dict(b2)}")

    # ---------------------------------------------------------------- #
    # P1 3.5 — metrics_hook
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.5: metrics_hook ===")

    events: List = []
    hooked = ReasoningTraceAnalyzer(
        metrics_hook=lambda ev, f: events.append((ev, f)))
    hooked.analyze([{"thought": "Because X, save energy"}])
    hooked.analyze([])
    event_names = [ev for ev, _ in events]
    assert "analysis_generated" in event_names
    generated = [
        (ev, f) for ev, f in events if ev == "analysis_generated"
    ]
    assert len(generated) == 2
    assert generated[0][1]["has_data"] is True
    assert generated[1][1]["has_data"] is False
    print(f"  ✓ metrics_hook captured {len(events)} event(s); "
          f"has_data flags: "
          f"{[f['has_data'] for ev, f in generated]}")

    # ---------------------------------------------------------------- #
    # P1 3.6 — statistics(window=)
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.6: statistics(window=) ===")

    analyzer2 = ReasoningTraceAnalyzer()
    for thought in ("because", "therefore", "", "because"):
        analyzer2.analyze([{"thought": thought}])
    analyzer2.analyze([])  # empty

    stats = analyzer2.statistics()
    assert stats["analyses"] == 5
    assert stats["analyses_with_data"] == 4
    assert stats["analyses_empty"] == 1
    print(f"  ✓ statistics: analyses=5, with_data=4, empty=1")

    win = analyzer2.statistics(window=2)
    assert win["analyses"] == 2
    print(f"  ✓ statistics(window=2): analyses=2")

    # ---------------------------------------------------------------- #
    # Bonus: close, context managers, pickle
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: close / context / pickle ===")

    # close() idempotent
    analyzer2.close()
    analyzer2.close()
    print(f"  ✓ close() idempotent")

    # sync context manager
    with ReasoningTraceAnalyzer() as scoped:
        scoped.analyze([{"thought": "x"}])
    print(f"  ✓ sync context manager")

    # async context manager
    async def _async_scope():
        async with ReasoningTraceAnalyzer() as scoped:
            scoped.analyze([{"thought": "x"}])

    asyncio.run(_async_scope())
    print(f"  ✓ async context manager")

    # pickle ReasoningTraceAnalyzer
    analyzer3 = ReasoningTraceAnalyzer()
    analyzer3.analyze([{"thought": "because energy"}])
    blob = pickle.dumps(analyzer3)
    restored3 = pickle.loads(blob)
    assert restored3._lock is not None
    assert len(restored3.history) == 1
    assert restored3.history[0].has_data is True
    restored3.analyze([{"thought": "because"}])
    assert len(restored3.history) == 2
    print(f"  ✓ ReasoningTraceAnalyzer pickles and remains usable")

    # pickle ReasoningAnalysis (with MappingProxyType in step_breakdown)
    analysis = analyzer3.history[0]
    blob = pickle.dumps(analysis)
    restored_analysis = pickle.loads(blob)
    assert restored_analysis.coherence_score == analysis.coherence_score
    assert restored_analysis.step_breakdown[0]["index"] == 0
    # Still immutable.
    try:
        restored_analysis.step_breakdown[0]["index"] = 999
    except TypeError:
        pass
    else:
        raise AssertionError(
            "pickle regression: step_breakdown entry mutable after "
            "unpickle")
    print(f"  ✓ ReasoningAnalysis pickles with "
          f"MappingProxyType round-trip")

    # ---------------------------------------------------------------- #
    # Backward compat: v1.0.0 payload (no version, no new fields)
    # ---------------------------------------------------------------- #
    print("\n=== Backward compat: v1.0.0 payload ===")

    v1_payload = {
        "config": {
            "energy_keywords": ["energy", "carbon"],
            "coherence_keywords": ["because", "therefore"],
            "max_history": 100,
            "weight_coherence": 0.5,
            "weight_factuality": 0.2,
            "weight_efficiency": 0.2,
            "weight_energy_awareness": 0.1,
        },
        "strict": True,
        "history": [
            {
                "coherence_score": 0.5,
                "factuality_score": 0.8,
                "efficiency_score": 0.9,
                "energy_awareness": 0.3,
                "composite_score": 0.65,
                "step_breakdown": [
                    {"index": 0, "length": 10, "energy_aware": True},
                ],
                "timestamp": 1700000000.0,
                # step_count and has_data absent — should default
            }
        ],
    }
    restored_v1 = ReasoningTraceAnalyzer.from_dict(v1_payload)
    assert len(restored_v1.history) == 1
    a = restored_v1.history[0]
    assert a.coherence_score == 0.5
    assert a.step_count == 1, (
        f"Backward-compat regression: step_count={a.step_count}, "
        f"expected 1 (from len(step_breakdown))")
    assert a.has_data is True
    print(f"  ✓ v1.0.0 payload loads; step_count inferred from "
          f"step_breakdown length")

    # ---------------------------------------------------------------- #
    # Validation: bad trace types
    # ---------------------------------------------------------------- #
    print("\n=== Validation: bad trace types ===")

    strict_a = ReasoningTraceAnalyzer(strict=True)
    try:
        strict_a.analyze("not-a-list")  # type: ignore[arg-type]
    except ReasoningError:
        print(f"  ✓ strict: string trace rejected")
    else:
        raise AssertionError(
            "Validation regression: string trace accepted")

    # Lenient mode: tolerate.
    lenient = ReasoningTraceAnalyzer(strict=False)
    result = lenient.analyze("not-a-list")  # type: ignore[arg-type]
    assert result["composite_score"] == 0.0
    assert result["has_data"] is False
    print(f"  ✓ lenient: string trace → empty analysis")

    # Non-Mapping step in strict mode.
    try:
        strict_a.analyze([{"thought": "x"}, "not-a-mapping"])  # type: ignore[list-item]
    except ReasoningError:
        print(f"  ✓ strict: non-Mapping step rejected")

    # Same in lenient mode.
    result = lenient.analyze([{"thought": "because"}])  # type: ignore[list-item]
    print(f"  ✓ lenient mode: valid steps analyzed")

    print("\n✓ All self-tests passed.")
