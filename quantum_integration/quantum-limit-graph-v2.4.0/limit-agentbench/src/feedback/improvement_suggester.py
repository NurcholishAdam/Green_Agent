# src/feedback/improvement_suggester.py

"""
Improvement Suggester — v2.0.0
==============================

Generates actionable sustainability feedback for agents.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``SuggesterConfig`` and ``SustainabilityImprovementSuggester``
        reject ``bool``, ``NaN``, ``Inf``, ``None``, and non-numeric values.
        Previously ``baseline_kwh=True`` silently became ``1.0`` (a 400×
        threshold shift) and ``baseline_kwh=float("nan")`` made the energy
        threshold inert.
  * 2.2 ``_coerce_number`` rejects negatives. ``generate({},
        {"energy_kwh": -5.0})`` no longer produces a suggestion based on a
        negative value — the metric is coerced to the default and the
        coercion is surfaced via ``metrics_hook``.
  * 2.3 ``strict=False`` now applies to ``result`` too. Previously
        ``SustainabilityImprovementSuggester(strict=False).generate({},
        None)`` raised ``SuggesterError`` despite the documented lenient
        mode.
  * 2.4 ``ImprovementSuggestion`` validates every field via
        ``__post_init__``. Previously ``category="garbage"``,
        ``text=42``, and ``value=float("nan")`` all constructed cleanly
        and corrupted the recorded history.
  * 2.5 ``from_dict`` delegates to ``SuggesterConfig.from_legacy_dict``
        so new config fields default correctly.

**P1 robustness / observability**
  * 3.1 ``SuggestionCategory`` enum with case-insensitive coercion.
        ``ImprovementSuggestion(category="ENERGY").category ==
        SuggestionCategory.ENERGY``.
  * 3.2 ``ImprovementSuggestion.from_dict`` rejects non-numeric and
        missing fields with ``SuggesterError`` (not raw ``KeyError`` /
        ``TypeError``).
  * 3.3 ``latency`` is a first-class category. ``latency_threshold_ms``
        added to ``SuggesterConfig``; ``generate({}, {"latency_ms": 5000})``
        produces a latency suggestion when the threshold is crossed.
  * 3.4 ``metrics_hook`` fires ``suggestions_skipped{key, reason}`` for
        each coerced metric (``reason`` ∈ ``"missing"`` / ``"non_numeric"``
        / ``"non_finite"`` / ``"negative"``), so callers can distinguish
        "efficient" from "no data".
  * 3.5 ``metrics_hook`` fires ``suggestion_generated{category, value,
        threshold}`` for each emitted suggestion.
  * 3.6 ``analysis=None`` is documented as valid.
  * 3.7 ``statistics(window=N)`` for a consistent windowed view.
  * 3.8 ``close()``, locked context managers (sync + async).
  * 3.9 ``__version__`` exported.
  * 3.10 ``from_dict`` logs a ``WARNING`` when the payload exceeds
         ``max_history``.

**P2 polish**
  * ``to_dict``/``from_dict`` round-trip on ``SuggesterConfig`` and
    ``ImprovementSuggestion`` (both were asymmetric in v1.0.0).
  * ``__all__`` includes ``SuggestionCategory`` and ``__version__``.

Backward compatible: every v1.0.0 import path, field, and payload works
unchanged. ``category`` is now stored as a ``SuggestionCategory`` enum
instance, which compares equal to its string value (``SuggestionCategory
(str, Enum)``), so ``suggestion.category == "energy"`` still works.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field, fields
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Mapping, Optional, Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SuggesterError(ValueError):
    """Raised for invalid inputs or configuration."""


# --------------------------------------------------------------------------- #
# Category enum (P1 3.1)
# --------------------------------------------------------------------------- #
class SuggestionCategory(str, Enum):
    """Closed set of suggestion categories."""

    ENERGY = "energy"
    CARBON = "carbon"
    LATENCY = "latency"

    def __str__(self) -> str:
        return self.value


def _coerce_category(
    value: Any, *, field_name: str = "category",
) -> SuggestionCategory:
    """Coerce a value to SuggestionCategory case-insensitively."""
    if isinstance(value, SuggestionCategory):
        return value
    if value is None or isinstance(value, bool):
        raise SuggesterError(
            f"{field_name} must be a SuggestionCategory, got {value!r}.")
    try:
        normalized = str(value).strip().lower()
    except Exception:  # noqa: BLE001
        raise SuggesterError(
            f"{field_name} must be a SuggestionCategory, "
            f"got {type(value).__name__}.") from None
    try:
        return SuggestionCategory(normalized)
    except ValueError as exc:
        raise SuggesterError(
            f"{field_name} must be one of "
            f"{[c.value for c in SuggestionCategory]}, "
            f"got {value!r}.") from exc


# --------------------------------------------------------------------------- #
# Configuration (P0 2.1, P0 2.5)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SuggesterConfig:
    """Tunable parameters for the improvement suggester."""

    baseline_kwh: float = 0.0025
    carbon_threshold_kg: float = 0.001
    energy_ratio_trigger: float = 1.5
    latency_threshold_ms: float = 2000.0   # P1 3.3
    max_history: int = 1000

    def __post_init__(self) -> None:
        # Numeric fields: reject bool, non-numeric, NaN, Inf.
        for name in (
            "baseline_kwh", "carbon_threshold_kg",
            "energy_ratio_trigger", "latency_threshold_ms",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or \
                    not isinstance(value, (int, float)):
                raise SuggesterError(
                    f"{name} must be numeric, "
                    f"got {type(value).__name__}.")
            if math.isnan(value) or math.isinf(value):
                raise SuggesterError(
                    f"{name} must be finite, got {value!r}.")

        if self.baseline_kwh <= 0:
            raise SuggesterError("baseline_kwh must be > 0.")
        if self.carbon_threshold_kg < 0:
            raise SuggesterError(
                "carbon_threshold_kg must be >= 0.")
        if self.energy_ratio_trigger <= 1.0:
            raise SuggesterError(
                "energy_ratio_trigger must be > 1.0.")
        if self.latency_threshold_ms < 0:
            raise SuggesterError(
                "latency_threshold_ms must be >= 0.")

        if isinstance(self.max_history, bool) or \
                not isinstance(self.max_history, int) or \
                self.max_history <= 0:
            raise SuggesterError("max_history must be > 0.")

    # ---------------------------------------------------------------- #
    @classmethod
    def from_legacy_dict(
        cls, cfg: Optional[Mapping[str, Any]]
    ) -> "SuggesterConfig":
        """P0 2.5: build from a flat mapping; unknown keys ignored."""
        cfg = dict(cfg or {})
        valid = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in cfg.items() if k in valid})

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Optional[Mapping[str, Any]]
    ) -> "SuggesterConfig":
        return cls.from_legacy_dict(data)


# --------------------------------------------------------------------------- #
# Suggestion record (P0 2.4, P1 3.1, P1 3.2)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ImprovementSuggestion:
    """Immutable record of a generated suggestion."""

    category: SuggestionCategory
    text: str
    value: float
    threshold: float
    timestamp: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        # P1 3.1: coerce category case-insensitively.
        if not isinstance(self.category, SuggestionCategory):
            object.__setattr__(
                self, "category",
                _coerce_category(self.category),
            )

        if not isinstance(self.text, str) or not self.text:
            raise SuggesterError(
                "text must be a non-empty string.")

        for name in ("value", "threshold"):
            v = getattr(self, name)
            if isinstance(v, bool) or \
                    not isinstance(v, (int, float)):
                raise SuggesterError(
                    f"{name} must be numeric, "
                    f"got {type(v).__name__}.")
            if math.isnan(v) or math.isinf(v):
                raise SuggesterError(f"{name} must be finite.")
            if v < 0:
                raise SuggesterError(f"{name} must be >= 0.")

        if isinstance(self.timestamp, bool) or \
                not isinstance(self.timestamp, (int, float)):
            raise SuggesterError(
                "timestamp must be numeric, "
                f"got {type(self.timestamp).__name__}.")
        if math.isnan(self.timestamp) or \
                math.isinf(self.timestamp):
            raise SuggesterError("timestamp must be finite.")
        if self.timestamp < 0:
            raise SuggesterError("timestamp must be >= 0.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "category": self.category.value,
            "text": self.text,
            "value": self.value,
            "threshold": self.threshold,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ImprovementSuggestion":
        """P1 3.2: robust deserialization."""
        if not isinstance(data, Mapping):
            raise SuggesterError(
                f"ImprovementSuggestion.from_dict expects a Mapping, "
                f"got {type(data).__name__}.")
        if "category" not in data:
            raise SuggesterError(
                "ImprovementSuggestion.from_dict: missing 'category'.")
        if "text" not in data:
            raise SuggesterError(
                "ImprovementSuggestion.from_dict: missing 'text'.")
        text_raw = data["text"]
        if not isinstance(text_raw, str) or not text_raw:
            raise SuggesterError(
                "ImprovementSuggestion.from_dict: text must be a "
                "non-empty string.")

        def _safe_float(key: str, default: Any) -> float:
            v = data.get(key, default)
            if isinstance(v, bool) or \
                    not isinstance(v, (int, float)):
                raise SuggesterError(
                    f"ImprovementSuggestion.from_dict: {key} must be "
                    f"numeric, got {type(v).__name__}.")
            f = float(v)
            if math.isnan(f) or math.isinf(f):
                raise SuggesterError(
                    f"ImprovementSuggestion.from_dict: {key} must be "
                    f"finite.")
            if f < 0:
                raise SuggesterError(
                    f"ImprovementSuggestion.from_dict: {key} must be "
                    f">= 0.")
            return f

        return cls(
            category=data["category"],
            text=text_raw,
            value=_safe_float("value", 0.0),
            threshold=_safe_float("threshold", 0.0),
            timestamp=_safe_float("timestamp", time.time()),
        )

    def __repr__(self) -> str:
        return (
            "ImprovementSuggestion("
            f"category={self.category.value!r}, "
            f"value={self.value:.4f}, "
            f"threshold={self.threshold:.4f})"
        )


# --------------------------------------------------------------------------- #
# Suggester (P0 2.2, P0 2.3, P1 3.3, P1 3.4, P1 3.5)
# --------------------------------------------------------------------------- #
class SustainabilityImprovementSuggester:
    """
    Generates actionable sustainability feedback for agents.

    Thread-safe, serializable, and bounded in memory.
    """

    def __init__(
        self,
        baseline: float = 0.0025,
        carbon_threshold: float = 0.001,
        *,
        config: Optional[SuggesterConfig] = None,
        strict: bool = True,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            # P0 2.1: validate the legacy two params directly.
            for name, value in (
                ("baseline", baseline),
                ("carbon_threshold", carbon_threshold),
            ):
                if isinstance(value, bool) or \
                        not isinstance(value, (int, float)):
                    raise SuggesterError(
                        f"{name} must be numeric, "
                        f"got {type(value).__name__}.")
                if math.isnan(value) or math.isinf(value):
                    raise SuggesterError(
                        f"{name} must be finite, got {value!r}.")
            if baseline <= 0:
                raise SuggesterError(
                    "baseline must be a positive number.")
            if carbon_threshold < 0:
                raise SuggesterError(
                    "carbon_threshold must be a non-negative number.")
            self._config = SuggesterConfig(
                baseline_kwh=float(baseline),
                carbon_threshold_kg=float(carbon_threshold),
            )

        if metrics_hook is not None and not callable(metrics_hook):
            raise SuggesterError("metrics_hook must be callable.")

        self._strict: bool = bool(strict)
        self._metrics_hook = metrics_hook

        # Legacy attributes preserved (v1.0.0 bug fix).
        self.baseline: float = self._config.baseline_kwh
        self.carbon_threshold: float = self._config.carbon_threshold_kg

        self._lock = threading.RLock()
        self._history: Deque[ImprovementSuggestion] = deque(
            maxlen=self._config.max_history)
        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None

        logger.debug(
            "SustainabilityImprovementSuggester v%s initialized "
            "(baseline=%.4f kWh, carbon_threshold=%.4f kg, "
            "latency_threshold=%.0f ms, strict=%s)",
            __version__,
            self.baseline,
            self.carbon_threshold,
            self._config.latency_threshold_ms,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> SuggesterConfig:
        return self._config

    @property
    def history(self) -> List[ImprovementSuggestion]:
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
    def generate(
        self,
        analysis: Mapping[str, Any],
        result: Mapping[str, Any],
        *,
        record: bool = True,
    ) -> List[str]:
        """Generate sustainability suggestions (legacy string API)."""
        records = self.generate_structured(
            analysis, result, record=record)
        return [r.text for r in records]

    def generate_structured(
        self,
        analysis: Optional[Mapping[str, Any]],
        result: Optional[Mapping[str, Any]],
        *,
        record: bool = True,
    ) -> List[ImprovementSuggestion]:
        """Generate sustainability suggestions (structured API)."""
        # P0 2.3: strict mode now applies to result too.
        if not isinstance(result, Mapping):
            msg = (
                f"result must be a Mapping, "
                f"got {type(result).__name__}.")
            if self._strict:
                raise SuggesterError(msg)
            logger.warning("%s Using empty mapping.", msg)
            result = {}

        if analysis is not None and \
                not isinstance(analysis, Mapping):
            msg = (
                f"analysis must be a Mapping, "
                f"got {type(analysis).__name__}.")
            if self._strict:
                raise SuggesterError(msg)
            logger.warning("%s Ignoring.", msg)
            analysis = {}

        # P0 2.2: extraction returns (value, coerced_reason_tuple).
        energy, energy_coerced = self._coerce_number(
            result, "energy_kwh", default=0.0)
        carbon, carbon_coerced = self._coerce_number(
            result, "carbon_co2e_kg", default=0.0)
        latency, latency_coerced = self._coerce_number(
            result, "latency_ms", default=0.0)

        # P1 3.4 / P0 2.2: fire suggestions_skipped for each coerced
        # metric so callers can distinguish "efficient" from "no data".
        for coerced in (energy_coerced, carbon_coerced, latency_coerced):
            if coerced is not None:
                reason, key = coerced
                self._emit_metric("suggestions_skipped", {
                    "key": key,
                    "reason": reason,
                })

        suggestions: List[ImprovementSuggestion] = []

        # ---- Energy ----
        energy_threshold = (
            self.baseline * self._config.energy_ratio_trigger)
        if energy > energy_threshold:
            text = (
                f"⚡ Energy usage {energy:.4f} kWh is "
                f"{(energy / self.baseline - 1) * 100:.0f}% above "
                f"baseline {self.baseline:.4f} kWh. Consider:\n"
                f"  • Reducing inference calls\n"
                f"  • Using smaller models for subtasks\n"
                f"  • Caching repeated computations"
            )
            s = ImprovementSuggestion(
                category=SuggestionCategory.ENERGY,
                text=text,
                value=energy,
                threshold=energy_threshold,
            )
            suggestions.append(s)
            self._emit_metric("suggestion_generated", {
                "category": s.category.value,
                "value": s.value,
                "threshold": s.threshold,
            })

        # ---- Carbon ----
        if carbon > self.carbon_threshold:
            text = (
                f"🌱 Carbon emissions {carbon:.4f} kg CO2e exceed "
                f"threshold {self.carbon_threshold:.4f} kg. Try:\n"
                f"  • Scheduling tasks during low-carbon hours\n"
                f"  • Using more efficient hardware profiles\n"
                f"  • Optimizing model selection"
            )
            s = ImprovementSuggestion(
                category=SuggestionCategory.CARBON,
                text=text,
                value=carbon,
                threshold=self.carbon_threshold,
            )
            suggestions.append(s)
            self._emit_metric("suggestion_generated", {
                "category": s.category.value,
                "value": s.value,
                "threshold": s.threshold,
            })

        # ---- Latency (P1 3.3) ----
        if latency > self._config.latency_threshold_ms:
            text = (
                f"⏱️ Latency {latency:.1f} ms exceeds threshold "
                f"{self._config.latency_threshold_ms:.1f} ms. Try:\n"
                f"  • Reducing per-inference work\n"
                f"  • Warming up the model cache\n"
                f"  • Batching requests"
            )
            s = ImprovementSuggestion(
                category=SuggestionCategory.LATENCY,
                text=text,
                value=latency,
                threshold=self._config.latency_threshold_ms,
            )
            suggestions.append(s)
            self._emit_metric("suggestion_generated", {
                "category": s.category.value,
                "value": s.value,
                "threshold": s.threshold,
            })

        if record:
            with self._lock:
                for s in suggestions:
                    self._history.append(s)

        logger.debug(
            "Generated %d suggestion(s) "
            "(energy=%.4f, carbon=%.4f, latency=%.1f).",
            len(suggestions), energy, carbon, latency,
        )
        return suggestions

    # ---------------------------------------------------------- helpers
    @staticmethod
    def _coerce_number(
        mapping: Mapping[str, Any],
        key: str,
        *,
        default: float,
    ) -> Tuple[float, Optional[Tuple[str, str]]]:
        """
        Return ``(value, coerced)``.

        ``coerced`` is ``None`` when the metric is present, numeric, finite,
        and non-negative. Otherwise it is ``(reason, key)`` where ``reason``
        is one of ``"missing"`` / ``"non_numeric"`` / ``"non_finite"`` /
        ``"negative"``.
        """
        if key not in mapping:
            logger.debug(
                "mapping[%r] missing; using default %.4f.",
                key, default)
            return default, ("missing", key)

        value = mapping[key]
        if isinstance(value, bool) or \
                not isinstance(value, (int, float)):
            logger.debug(
                "mapping[%r] not numeric; using default %.4f.",
                key, default)
            return default, ("non_numeric", key)

        f = float(value)
        if math.isnan(f) or math.isinf(f):
            logger.debug(
                "mapping[%r] non-finite; using default %.4f.",
                key, default)
            return default, ("non_finite", key)

        # P0 2.2: reject negatives.
        if f < 0:
            logger.debug(
                "mapping[%r] negative; using default %.4f.",
                key, default)
            return default, ("negative", key)

        return f, None

    # ---------------------------------------------------------- stats
    def statistics(
        self, window: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Return aggregate statistics (P1 3.7: windowed view)."""
        with self._lock:
            if window is not None:
                history = list(self._history)[-window:]
            else:
                history = list(self._history)

        if not history:
            return {
                "suggestions": 0,
                "by_category": {},
                "baseline_kwh": self.baseline,
                "carbon_threshold_kg": self.carbon_threshold,
            }

        by_category: Dict[str, int] = {}
        for s in history:
            key = s.category.value
            by_category[key] = by_category.get(key, 0) + 1

        return {
            "suggestions": len(history),
            "by_category": by_category,
            "baseline_kwh": self.baseline,
            "carbon_threshold_kg": self.carbon_threshold,
        }

    def reset(self, *, clear_history: bool = False) -> None:
        with self._lock:
            if clear_history:
                self._history.clear()
            self._ctx_start = None
            self._async_ctx_start = None
        logger.debug(
            "Suggester reset (clear_history=%s)", clear_history)

    def close(self) -> None:
        """P1 3.8: release resources. Idempotent."""
        logger.debug(
            "SustainabilityImprovementSuggester closed "
            "(%d suggestion(s) in history).", len(self._history))

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "config": asdict(self._config),
                "strict": self._strict,
                "history": [s.to_dict() for s in self._history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "SustainabilityImprovementSuggester":
        if not isinstance(data, Mapping):
            raise SuggesterError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}.")

        # P0 2.5: delegate to from_legacy_dict.
        cfg = SuggesterConfig.from_legacy_dict(
            data.get("config") or {})

        suggester = cls(
            config=cfg,
            strict=bool(data.get("strict", True)),
            metrics_hook=metrics_hook,
        )

        # P1 3.10: warn before silent truncation.
        history_entries = list(data.get("history", []))
        if len(history_entries) > cfg.max_history:
            logger.warning(
                "Restoring %d history entries; only most recent "
                "%d kept (max_history).",
                len(history_entries), cfg.max_history)

        with suggester._lock:
            for entry in history_entries:
                suggester._history.append(
                    ImprovementSuggestion.from_dict(entry))
        return suggester

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
    ) -> "SustainabilityImprovementSuggester":
        try:
            return cls.from_dict(
                json.loads(payload), metrics_hook=metrics_hook)
        except json.JSONDecodeError as exc:
            raise SuggesterError(
                f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "SustainabilityImprovementSuggester":
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
                "SustainabilityImprovementSuggester scope exited "
                "with %s after %.4fs.", exc_type.__name__, elapsed)
            return
        logger.info(
            "SustainabilityImprovementSuggester scope closed in "
            "%.4fs (%d suggestion(s)).", elapsed, n)

    async def __aenter__(self) -> "SustainabilityImprovementSuggester":
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
                "Async SustainabilityImprovementSuggester scope "
                "exited with %s after %.4fs.",
                exc_type.__name__, elapsed)
            return
        logger.info(
            "Async SustainabilityImprovementSuggester scope closed "
            "in %.4fs (%d suggestion(s)).", elapsed, n)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "SustainabilityImprovementSuggester("
                f"version={__version__!r}, "
                f"baseline={self.baseline:.4f} kWh, "
                f"carbon_threshold={self.carbon_threshold:.4f} kg, "
                f"latency_threshold="
                f"{self._config.latency_threshold_ms:.0f} ms, "
                f"suggestions={len(self._history)})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "__version__",
    "ImprovementSuggestion",
    "SuggesterConfig",
    "SuggesterError",
    "SuggestionCategory",
    "SustainabilityImprovementSuggester",
]


# --------------------------------------------------------------------------- #
# Self-tests: python -m feedback.improvement_suggester
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import pickle

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print(f"=== improvement_suggester v{__version__} self-tests ===\n")

    # ---------------------------------------------------------------- #
    # P0 2.1 — config rejects bool / NaN / Inf / non-numeric
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: config validation ===")

    for bad in (True, False, float("nan"), float("inf"),
                float("-inf"), "0.5", None):
        try:
            SuggesterConfig(baseline_kwh=bad)  # type: ignore[arg-type]
        except SuggesterError as exc:
            print(f"  ✓ SuggesterConfig(baseline_kwh={bad!r}) "
                  f"→ SuggesterError")
        else:
            raise AssertionError(
                f"P0 2.1 regression: SuggesterConfig(baseline_kwh="
                f"{bad!r}) was accepted")

    # Same via the suggester's legacy two-param path.
    for bad in (True, float("nan"), float("inf"), "0.5", None):
        try:
            SustainabilityImprovementSuggester(baseline=bad)  # type: ignore[arg-type]
        except SuggesterError:
            print(f"  ✓ Suggester(baseline={bad!r}) → SuggesterError")
        else:
            raise AssertionError(
                f"P0 2.1 regression: Suggester(baseline={bad!r}) "
                f"was accepted")

    # Valid config still works.
    cfg = SuggesterConfig(baseline_kwh=0.005)
    assert cfg.baseline_kwh == 0.005
    print(f"  ✓ valid SuggesterConfig(baseline_kwh=0.005) accepted")

    # ---------------------------------------------------------------- #
    # P0 2.2 — negative energy is coerced and visible via metrics_hook
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: negative metric coercion is visible ===")

    events: List[Tuple[str, Mapping[str, Any]]] = []
    s = SustainabilityImprovementSuggester(
        metrics_hook=lambda ev, f: events.append((ev, f)))
    result = s.generate({}, {"energy_kwh": -5.0})
    assert result == [], (
        f"P0 2.2 regression: negative energy produced a suggestion: "
        f"{result}")
    skipped = [
        (ev, f) for ev, f in events if ev == "suggestions_skipped"
    ]
    assert any(
        f["key"] == "energy_kwh" and f["reason"] == "negative"
        for ev, f in skipped), (
        f"P0 2.2 regression: no suggestions_skipped for "
        f"energy_kwh=negative; got {skipped}")
    print(f"  ✓ energy_kwh=-5.0: no suggestion, "
          f"suggestions_skipped fired with reason='negative'")

    # Contrast: genuine 0.0 should NOT fire suggestions_skipped.
    events.clear()
    s.generate({}, {"energy_kwh": 0.0, "carbon_co2e_kg": 0.0})
    energy_skipped = [
        f for ev, f in events
        if ev == "suggestions_skipped" and f["key"] == "energy_kwh"
    ]
    assert energy_skipped == [], (
        f"P0 2.2 regression: genuine energy_kwh=0.0 fired "
        f"suggestions_skipped: {energy_skipped}")
    print(f"  ✓ genuine energy_kwh=0.0 does NOT fire "
          f"suggestions_skipped")

    # ---------------------------------------------------------------- #
    # P0 2.3 — strict=False applies to `result` too
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: strict=False on result ===")

    lenient = SustainabilityImprovementSuggester(strict=False)
    out = lenient.generate({}, None)  # type: ignore[arg-type]
    assert out == [], (
        f"P0 2.3 regression: strict=False raised or returned "
        f"non-empty; got {out}")
    print(f"  ✓ strict=False: generate({{}}, None) → []")

    strict_s = SustainabilityImprovementSuggester(strict=True)
    try:
        strict_s.generate({}, None)  # type: ignore[arg-type]
    except SuggesterError:
        print(f"  ✓ strict=True: generate({{}}, None) → SuggesterError")
    else:
        raise AssertionError(
            "P0 2.3 regression: strict=True did not raise")

    # ---------------------------------------------------------------- #
    # P0 2.4 — ImprovementSuggestion structural validation
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: ImprovementSuggestion validation ===")

    # Bad category
    try:
        ImprovementSuggestion(
            category="garbage",  # type: ignore[arg-type]
            text="msg", value=0.0, threshold=0.0,
        )
    except SuggesterError as exc:
        print(f"  ✓ category='garbage' rejected: {exc}")
    else:
        raise AssertionError(
            "P0 2.4 regression: garbage category accepted")

    # text=42
    try:
        ImprovementSuggestion(
            category="energy",
            text=42,  # type: ignore[arg-type]
            value=0.0, threshold=0.0,
        )
    except SuggesterError:
        print(f"  ✓ text=42 rejected")
    else:
        raise AssertionError(
            "P0 2.4 regression: text=42 accepted")

    # value=nan
    try:
        ImprovementSuggestion(
            category="energy", text="msg",
            value=float("nan"), threshold=0.0,
        )
    except SuggesterError:
        print(f"  ✓ value=nan rejected")
    else:
        raise AssertionError(
            "P0 2.4 regression: NaN value accepted")

    # All together (matching the user's example)
    try:
        ImprovementSuggestion(
            category="garbage",  # type: ignore[arg-type]
            text=42,             # type: ignore[arg-type]
            value=float("nan"),
            threshold=0.0,
        )
    except SuggesterError:
        print(f"  ✓ (garbage, 42, nan) rejected")

    # Valid construction.
    valid = ImprovementSuggestion(
        category="energy", text="valid",
        value=0.01, threshold=0.001)
    assert valid.category == SuggestionCategory.ENERGY
    print(f"  ✓ valid construction OK: category="
          f"{valid.category.value}")

    # ---------------------------------------------------------------- #
    # P0 2.5 — from_dict picks up declared defaults
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: config drift in from_dict ===")

    legacy = {
        "config": {"baseline_kwh": 0.01},
        "strict": True,
        "history": [],
    }
    restored = SustainabilityImprovementSuggester.from_dict(legacy)
    assert restored.config.baseline_kwh == 0.01
    assert restored.config.energy_ratio_trigger == 1.5, (
        f"P0 2.5 regression: energy_ratio_trigger="
        f"{restored.config.energy_ratio_trigger}, expected 1.5")
    assert restored.config.max_history == 1000, (
        f"P0 2.5 regression: max_history="
        f"{restored.config.max_history}, expected 1000")
    assert restored.config.latency_threshold_ms == 2000.0, (
        f"P0 2.5 regression: latency_threshold_ms="
        f"{restored.config.latency_threshold_ms}, expected 2000.0")
    print(f"  ✓ baseline_kwh=0.01 (explicit), "
          f"energy_ratio_trigger=1.5, max_history=1000, "
          f"latency_threshold_ms=2000.0 (all defaults)")

    # ---------------------------------------------------------------- #
    # P1 3.1 — case-insensitive category coercion
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: case-insensitive category ===")

    for spelling in ("ENERGY", "energy", " Energy ", "eNeRgY"):
        inst = ImprovementSuggestion(
            category=spelling,  # type: ignore[arg-type]
            text="t", value=0.0, threshold=0.0)
        assert inst.category == SuggestionCategory.ENERGY, (
            f"P1 3.1 regression: {spelling!r} → "
            f"{inst.category!r}")
    print(f"  ✓ 'ENERGY', 'energy', ' Energy ', 'eNeRgY' all → "
          f"SuggestionCategory.ENERGY")

    # Enum instances pass through unchanged.
    inst = ImprovementSuggestion(
        category=SuggestionCategory.CARBON,
        text="t", value=0.0, threshold=0.0)
    assert inst.category == SuggestionCategory.CARBON
    print(f"  ✓ SuggestionCategory.CARBON preserved")

    # Invalid category raises.
    try:
        ImprovementSuggestion(
            category="garbage",  # type: ignore[arg-type]
            text="t", value=0.0, threshold=0.0)
    except SuggesterError:
        print(f"  ✓ 'garbage' rejected")

    # ---------------------------------------------------------------- #
    # P1 3.3 — latency suggestion
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.3: latency suggestion ===")

    suggester = SustainabilityImprovementSuggester()
    # Above threshold: fires.
    out = suggester.generate_structured({}, {"latency_ms": 5000.0})
    assert len(out) == 1, (
        f"P1 3.3 regression: expected 1 suggestion for "
        f"latency_ms=5000, got {len(out)}")
    assert out[0].category == SuggestionCategory.LATENCY
    assert out[0].value == 5000.0
    assert out[0].threshold == 2000.0
    print(f"  ✓ latency_ms=5000 → latency suggestion "
          f"(threshold=2000)")

    # Below threshold: no suggestion.
    suggester.reset(clear_history=True)
    out = suggester.generate({}, {"latency_ms": 500.0})
    assert out == [], (
        f"P1 3.3 regression: latency_ms=500 should not trigger, "
        f"got {out}")
    print(f"  ✓ latency_ms=500 → no suggestion")

    # ---------------------------------------------------------------- #
    # P1 3.4 — suggestions_skipped per coerced metric
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.4: suggestions_skipped per coerced metric ===")

    events: List[Tuple[str, Mapping[str, Any]]] = []
    s = SustainabilityImprovementSuggester(
        metrics_hook=lambda ev, f: events.append((ev, f)))
    # Empty result → all three metrics missing.
    s.generate({}, {})
    skipped = [
        (ev, f) for ev, f in events if ev == "suggestions_skipped"
    ]
    keys = {f["key"] for ev, f in skipped}
    reasons = {f["key"]: f["reason"] for ev, f in skipped}
    assert len(skipped) == 3, (
        f"P1 3.4 regression: expected 3 skipped events, "
        f"got {len(skipped)}: {skipped}")
    assert keys == {"energy_kwh", "carbon_co2e_kg", "latency_ms"}, (
        f"P1 3.4 regression: unexpected keys {keys}")
    assert all(r == "missing" for r in reasons.values()), (
        f"P1 3.4 regression: reasons={reasons}, all should be "
        f"'missing'")
    print(f"  ✓ empty result: 3 suggestions_skipped events "
          f"(all reason='missing')")

    # Partial: only latency missing → 1 event.
    events.clear()
    s.generate({}, {"energy_kwh": 0.0, "carbon_co2e_kg": 0.0})
    skipped = [
        (ev, f) for ev, f in events if ev == "suggestions_skipped"
    ]
    assert len(skipped) == 1, (
        f"P1 3.4 regression: expected 1 skipped event, "
        f"got {len(skipped)}: {skipped}")
    assert skipped[0][1]["key"] == "latency_ms"
    print(f"  ✓ partial data: only latency_ms skipped")

    # Bad values: non_numeric, non_finite, negative reasons.
    events.clear()
    s.generate({}, {
        "energy_kwh": "not-a-number",
        "carbon_co2e_kg": float("nan"),
        "latency_ms": -100.0,
    })
    skipped = [
        (ev, f) for ev, f in events if ev == "suggestions_skipped"
    ]
    reasons = {f["key"]: f["reason"] for ev, f in skipped}
    assert reasons == {
        "energy_kwh": "non_numeric",
        "carbon_co2e_kg": "non_finite",
        "latency_ms": "negative",
    }, f"P1 3.4 regression: reasons={reasons}"
    print(f"  ✓ distinct reasons: non_numeric / non_finite / negative")

    # ---------------------------------------------------------------- #
    # P1 3.10 — history truncation warning
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.10: history truncation warning ===")

    # Build a payload with more history than max_history allows.
    small = SustainabilityImprovementSuggester(
        config=SuggesterConfig(max_history=5))
    payload = small.to_dict()
    payload["history"] = [
        {
            "category": "energy",
            "text": f"test {i}",
            "value": 0.01,
            "threshold": 0.001,
            "timestamp": time.time(),
        }
        for i in range(10)
    ]

    # Capture log warnings.
    class _CaptureHandler(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.records: List[logging.LogRecord] = []

        def emit(self, record: logging.LogRecord) -> None:
            self.records.append(record)

    handler = _CaptureHandler()
    logger.addHandler(handler)
    old_level = logger.level
    logger.setLevel(logging.WARNING)
    try:
        restored = SustainabilityImprovementSuggester.from_dict(payload)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)

    assert len(restored.history) == 5, (
        f"P1 3.10 regression: expected 5 history entries, "
        f"got {len(restored.history)}")
    trunc_warnings = [
        r for r in handler.records
        if "Restoring" in r.getMessage()
    ]
    assert len(trunc_warnings) >= 1, (
        f"P1 3.10 regression: no truncation warning logged; "
        f"captured messages: "
        f"{[r.getMessage() for r in handler.records]}")
    print(f"  ✓ 10 payload entries → 5 kept, warning logged: "
          f"{trunc_warnings[0].getMessage()[:80]}...")

    # ---------------------------------------------------------------- #
    # Bonus: round-trip, statistics, metrics, close
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: round-trip / stats / metrics / close ===")

    suggester = SustainabilityImprovementSuggester()
    suggester.generate({}, {"energy_kwh": 0.01, "carbon_co2e_kg": 0.005})
    suggester.generate({}, {"energy_kwh": 0.02})
    suggester.generate({}, {"latency_ms": 5000.0})

    stats = suggester.statistics()
    assert stats["suggestions"] == 4
    assert stats["by_category"] == {
        "energy": 2, "carbon": 1, "latency": 1}
    win = suggester.statistics(window=2)
    assert win["suggestions"] == 2
    print(f"  ✓ statistics: {stats['by_category']}, "
          f"window(2)={win['suggestions']}")

    payload = suggester.to_json()
    restored = SustainabilityImprovementSuggester.from_json(payload)
    assert restored.to_dict() == suggester.to_dict()
    print(f"  ✓ JSON round-trip OK")

    suggester.close()  # idempotent
    suggester.close()
    print(f"  ✓ close() idempotent")

    async def _async_scope() -> None:
        async with SustainabilityImprovementSuggester() as scoped:
            scoped.generate({}, {"energy_kwh": 0.01})

    asyncio.run(_async_scope())
    print(f"  ✓ async context manager OK")

    # ---------------------------------------------------------------- #
    # Backward compat: v1.0.0 payload without version / latency_threshold
    # ---------------------------------------------------------------- #
    print("\n=== Backward compat: v1.0.0 payload ===")

    v1_payload = {
        "config": {
            "baseline_kwh": 0.0025,
            "carbon_threshold_kg": 0.001,
            "energy_ratio_trigger": 1.5,
            "max_history": 1000,
            # latency_threshold_ms absent — must default to 2000.0
        },
        "strict": True,
        "history": [
            {
                "category": "energy",
                "text": "old message",
                "value": 0.01,
                "threshold": 0.001,
                "timestamp": 1700000000.0,
            }
        ],
    }
    restored_v1 = SustainabilityImprovementSuggester.from_dict(v1_payload)
    assert restored_v1.config.latency_threshold_ms == 2000.0
    assert len(restored_v1.history) == 1
    assert restored_v1.history[0].category == SuggestionCategory.ENERGY
    print(f"  ✓ v1.0.0 payload loads; latency_threshold_ms defaults "
          f"to 2000.0")

    print("\n✓ All self-tests passed.")
