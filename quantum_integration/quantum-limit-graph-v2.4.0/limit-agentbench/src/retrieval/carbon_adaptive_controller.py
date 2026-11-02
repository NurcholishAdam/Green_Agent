# src/retrieval/carbon_adaptive_controller.py

"""
Carbon-Adaptive Retrieval Controller
====================================

Works with the existing carbon throttler to decide whether to allow full
multimodal retrieval or restrict to lightweight sources based on
sustainability conditions.

Design
------
- Frozen, validated ``CarbonAdaptiveConfig``: thresholds, token limits,
  energy coefficients, deferral queue cap.
- ``RetrievalDecision`` — frozen dataclass with full validation and
  serialization. Every numeric field is finite-checked and ``bool``
  is explicitly rejected.
- **Single counter-mutation site.** ``_evaluate`` is pure: it never
  touches public counters. Both ``decide_retrieval`` and
  ``_process_deferred_retrievals`` update ``total_decisions`` and
  ``decisions_by_mode`` together, so ``sum(decisions_by_mode) ==
  total_decisions`` always holds. ``retrievals_deferred`` increments
  exactly once per deferral event, including re-deferrals.
- **Correct savings accounting.** ``_account_savings`` applies the
  per-content-type energy multiplier, matching
  ``_estimate_carbon_impact``.
- **Deferred queue preserves urgency.** ``DeferredRetrieval`` carries
  the original ``urgency`` so re-evaluation uses the same priority the
  caller asked for.
- **Race-free re-processing.** ``_process_deferred_retrievals``
  snapshots under lock, evaluates outside lock, and swaps in a new
  queue under lock — entries appended concurrently are preserved.
- **``effective_content_type``** clarifies the RESTRICTED fallback:
  a caller who requested ``visual`` gets a ``text`` decision rather
  than a contradictory ``allowed=True, allow_visual=False``.
- **Serialization round-trips.** ``from_dict`` on every class wraps
  errors, validates ``strict`` is ``bool``, cross-checks the stored
  ``current_mode`` against the stored intensity, and rejects negative
  counters. ``to_json`` uses ``allow_nan=False``.

Version: 2.0.0
"""

from __future__ import annotations

import json
import logging
import math
import numbers
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from types import TracebackType
from typing import (
    Any,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "CarbonAdaptiveConfig",
    "CarbonAdaptiveError",
    "CarbonAdaptiveRetrievalController",
    "DeferredRetrieval",
    "RetrievalDecision",
    "RetrievalMode",
]

VERSION = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class CarbonAdaptiveError(ValueError):
    """Raised for invalid carbon-adaptive controller inputs or configuration."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class RetrievalMode(Enum):
    """Retrieval mode based on carbon conditions."""
    FULL = "full"
    RESTRICTED = "restricted"
    MINIMAL = "minimal"
    DEFERRED = "deferred"


_VALID_URGENCIES = ("urgent", "normal", "low")
_VALID_CONTENT_TYPES = ("text", "visual", "multimodal", "audio")


# --------------------------------------------------------------------------- #
# Coercion helpers
# --------------------------------------------------------------------------- #
def _coerce_bool(name: str, value: Any) -> bool:
    if not isinstance(value, bool):
        raise CarbonAdaptiveError(
            f"{name} must be a bool, got {type(value).__name__}."
        )
    return value


def _coerce_finite_float(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise CarbonAdaptiveError(
            f"{name} must be numeric, got {type(value).__name__}."
        )
    fv = float(value)
    if not math.isfinite(fv):
        raise CarbonAdaptiveError(
            f"{name} must be finite, got {value!r}."
        )
    return fv


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CarbonAdaptiveConfig:
    """Tunable parameters for the carbon-adaptive retrieval controller."""

    clean_threshold_g_kwh: float = 200.0
    dirty_threshold_g_kwh: float = 400.0
    default_intensity_g_kwh: float = 385.0
    enable_deferral: bool = True
    max_deferred: Optional[int] = 10_000
    restricted_max_tokens: int = 4096
    minimal_max_tokens: int = 2048
    energy_per_token_wh: float = 1e-6
    visual_energy_multiplier: float = 2.5
    multimodal_energy_multiplier: float = 2.0
    audio_energy_multiplier: float = 1.5
    grams_per_kg: float = 1000.0
    max_history: int = 1000

    def __post_init__(self) -> None:
        for name in (
            "clean_threshold_g_kwh",
            "dirty_threshold_g_kwh",
            "default_intensity_g_kwh",
            "energy_per_token_wh",
            "visual_energy_multiplier",
            "multimodal_energy_multiplier",
            "audio_energy_multiplier",
            "grams_per_kg",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
            ):
                raise CarbonAdaptiveError(
                    f"{name} must be finite and numeric."
                )
        if self.clean_threshold_g_kwh <= 0:
            raise CarbonAdaptiveError(
                "clean_threshold_g_kwh must be > 0."
            )
        if self.dirty_threshold_g_kwh <= self.clean_threshold_g_kwh:
            raise CarbonAdaptiveError(
                "dirty_threshold_g_kwh must be > clean_threshold_g_kwh."
            )
        if self.default_intensity_g_kwh < 0:
            raise CarbonAdaptiveError(
                "default_intensity_g_kwh must be >= 0."
            )
        for name in ("restricted_max_tokens", "minimal_max_tokens"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Integral)
                or value <= 0
            ):
                raise CarbonAdaptiveError(
                    f"{name} must be a positive int."
                )
        if self.minimal_max_tokens > self.restricted_max_tokens:
            raise CarbonAdaptiveError(
                "minimal_max_tokens must be <= restricted_max_tokens."
            )
        if self.energy_per_token_wh <= 0:
            raise CarbonAdaptiveError(
                "energy_per_token_wh must be > 0."
            )
        for name in (
            "visual_energy_multiplier",
            "multimodal_energy_multiplier",
            "audio_energy_multiplier",
        ):
            if getattr(self, name) <= 0:
                raise CarbonAdaptiveError(f"{name} must be > 0.")
        if self.grams_per_kg <= 0:
            raise CarbonAdaptiveError("grams_per_kg must be > 0.")
        if self.max_deferred is not None:
            if (
                isinstance(self.max_deferred, bool)
                or not isinstance(self.max_deferred, numbers.Integral)
                or self.max_deferred <= 0
            ):
                raise CarbonAdaptiveError(
                    "max_deferred must be a positive int or None."
                )
        if (
            isinstance(self.max_history, bool)
            or not isinstance(self.max_history, numbers.Integral)
            or self.max_history <= 0
        ):
            raise CarbonAdaptiveError("max_history must be > 0.")
        _coerce_bool("enable_deferral", self.enable_deferral)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "CarbonAdaptiveConfig":
        if not isinstance(data, Mapping):
            raise CarbonAdaptiveError(
                f"CarbonAdaptiveConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            raw_deferral = data.get("enable_deferral", True)
            if not isinstance(raw_deferral, bool):
                raise CarbonAdaptiveError(
                    f"enable_deferral must be bool, got "
                    f"{type(raw_deferral).__name__}."
                )
            return cls(
                clean_threshold_g_kwh=float(
                    data.get("clean_threshold_g_kwh", 200.0)
                ),
                dirty_threshold_g_kwh=float(
                    data.get("dirty_threshold_g_kwh", 400.0)
                ),
                default_intensity_g_kwh=float(
                    data.get("default_intensity_g_kwh", 385.0)
                ),
                enable_deferral=raw_deferral,
                max_deferred=data.get("max_deferred", 10_000),
                restricted_max_tokens=int(
                    data.get("restricted_max_tokens", 4096)
                ),
                minimal_max_tokens=int(
                    data.get("minimal_max_tokens", 2048)
                ),
                energy_per_token_wh=float(
                    data.get("energy_per_token_wh", 1e-6)
                ),
                visual_energy_multiplier=float(
                    data.get("visual_energy_multiplier", 2.5)
                ),
                multimodal_energy_multiplier=float(
                    data.get("multimodal_energy_multiplier", 2.0)
                ),
                audio_energy_multiplier=float(
                    data.get("audio_energy_multiplier", 1.5)
                ),
                grams_per_kg=float(data.get("grams_per_kg", 1000.0)),
                max_history=int(data.get("max_history", 1000)),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, CarbonAdaptiveError):
                raise
            raise CarbonAdaptiveError(
                f"invalid CarbonAdaptiveConfig payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# Decision
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RetrievalDecision:
    """Immutable decision about a retrieval operation."""

    allowed: bool
    mode: RetrievalMode
    max_tokens: int
    allow_visual: bool
    allow_multimodal: bool
    reason: str
    carbon_intensity_g_kwh: float
    estimated_carbon_kg: float
    effective_content_type: str = "text"
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.mode, RetrievalMode):
            raise CarbonAdaptiveError(
                f"mode must be a RetrievalMode, got "
                f"{type(self.mode).__name__}."
            )
        if (
            isinstance(self.max_tokens, bool)
            or not isinstance(self.max_tokens, numbers.Integral)
            or self.max_tokens < 0
        ):
            raise CarbonAdaptiveError(
                "max_tokens must be a non-negative int."
            )
        for name in ("carbon_intensity_g_kwh", "estimated_carbon_kg"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(
                value, numbers.Real,
            ):
                raise CarbonAdaptiveError(f"{name} must be numeric.")
            fv = float(value)
            if not math.isfinite(fv) or fv < 0:
                raise CarbonAdaptiveError(
                    f"{name} must be finite and >= 0, got {value!r}."
                )
        if not isinstance(self.reason, str) or not self.reason:
            raise CarbonAdaptiveError(
                "reason must be a non-empty string."
            )
        for name in ("allow_visual", "allow_multimodal"):
            if not isinstance(getattr(self, name), bool):
                raise CarbonAdaptiveError(f"{name} must be a bool.")
        if (
            not isinstance(self.allowed, bool)
        ):
            raise CarbonAdaptiveError(
                "allowed must be a bool."
            )
        if self.effective_content_type not in _VALID_CONTENT_TYPES:
            raise CarbonAdaptiveError(
                f"effective_content_type must be one of "
                f"{_VALID_CONTENT_TYPES}, got "
                f"{self.effective_content_type!r}."
            )
        if not self.allowed and (
            self.allow_visual or self.allow_multimodal
        ):
            raise CarbonAdaptiveError(
                "a denied retrieval cannot allow visual or "
                "multimodal content."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allowed": self.allowed,
            "mode": self.mode.value,
            "max_tokens": self.max_tokens,
            "allow_visual": self.allow_visual,
            "allow_multimodal": self.allow_multimodal,
            "reason": self.reason,
            "carbon_intensity_g_kwh": self.carbon_intensity_g_kwh,
            "estimated_carbon_kg": self.estimated_carbon_kg,
            "effective_content_type": self.effective_content_type,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "RetrievalDecision":
        if not isinstance(data, Mapping):
            raise CarbonAdaptiveError(
                f"RetrievalDecision.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            ts = data.get("timestamp")
            if isinstance(ts, str):
                timestamp = datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )
                if not timestamp.tzinfo:
                    timestamp = timestamp.replace(
                        tzinfo=timezone.utc,
                    )
            elif isinstance(ts, datetime):
                timestamp = ts
            else:
                timestamp = datetime.now(timezone.utc)

            allowed = _coerce_bool(
                "allowed", data["allowed"],
            )
            allow_visual = _coerce_bool(
                "allow_visual", data.get("allow_visual", False),
            )
            allow_multimodal = _coerce_bool(
                "allow_multimodal",
                data.get("allow_multimodal", False),
            )

            return cls(
                allowed=allowed,
                mode=RetrievalMode(str(data["mode"])),
                max_tokens=int(data["max_tokens"]),
                allow_visual=allow_visual,
                allow_multimodal=allow_multimodal,
                reason=str(data.get("reason", "")),
                carbon_intensity_g_kwh=float(
                    data.get("carbon_intensity_g_kwh", 0.0)
                ),
                estimated_carbon_kg=float(
                    data.get("estimated_carbon_kg", 0.0)
                ),
                effective_content_type=str(
                    data.get("effective_content_type", "text")
                ),
                timestamp=timestamp,
            )
        except KeyError as exc:
            raise CarbonAdaptiveError(
                f"RetrievalDecision payload missing "
                f"{exc.args[0]!r}."
            ) from exc
        except (TypeError, ValueError) as exc:
            if isinstance(exc, CarbonAdaptiveError):
                raise
            raise CarbonAdaptiveError(
                f"invalid RetrievalDecision payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "RetrievalDecision("
            f"allowed={self.allowed}, mode={self.mode.value}, "
            f"max_tokens={self.max_tokens}, "
            f"visual={self.allow_visual}, "
            f"multimodal={self.allow_multimodal}, "
            f"effective_type={self.effective_content_type}, "
            f"reason={self.reason!r})"
        )


# --------------------------------------------------------------------------- #
# Deferred entry
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DeferredRetrieval:
    """Immutable record of a deferred retrieval request."""

    query: str
    tokens: int
    content_type: str
    urgency: str
    context: Optional[Mapping[str, Any]]
    deferred_at: float
    deferred_intensity_g_kwh: float

    def __post_init__(self) -> None:
        if not isinstance(self.query, str) or not self.query:
            raise CarbonAdaptiveError(
                "query must be a non-empty string."
            )
        if (
            isinstance(self.tokens, bool)
            or not isinstance(self.tokens, numbers.Integral)
            or self.tokens <= 0
        ):
            raise CarbonAdaptiveError(
                "tokens must be a positive int."
            )
        if self.content_type not in _VALID_CONTENT_TYPES:
            raise CarbonAdaptiveError(
                f"content_type must be one of "
                f"{_VALID_CONTENT_TYPES}, got "
                f"{self.content_type!r}."
            )
        if self.urgency not in _VALID_URGENCIES:
            raise CarbonAdaptiveError(
                f"urgency must be one of {_VALID_URGENCIES}, "
                f"got {self.urgency!r}."
            )
        if self.context is not None and not isinstance(
            self.context, Mapping,
        ):
            raise CarbonAdaptiveError(
                f"context must be a Mapping or None, got "
                f"{type(self.context).__name__}."
            )
        for name in (
            "deferred_at", "deferred_intensity_g_kwh",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
            ):
                raise CarbonAdaptiveError(
                    f"{name} must be finite and numeric."
                )
        if self.deferred_intensity_g_kwh < 0:
            raise CarbonAdaptiveError(
                "deferred_intensity_g_kwh must be >= 0."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query": self.query,
            "tokens": self.tokens,
            "content_type": self.content_type,
            "urgency": self.urgency,
            "context": (
                dict(self.context)
                if self.context is not None else None
            ),
            "deferred_at": self.deferred_at,
            "deferred_intensity_g_kwh": (
                self.deferred_intensity_g_kwh
            ),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "DeferredRetrieval":
        if not isinstance(data, Mapping):
            raise CarbonAdaptiveError(
                "DeferredRetrieval.from_dict expects a Mapping."
            )
        try:
            context = data.get("context")
            if context is not None and not isinstance(
                context, Mapping
            ):
                raise CarbonAdaptiveError(
                    f"context must be a Mapping or None, got "
                    f"{type(context).__name__}."
                )
            return cls(
                query=str(data["query"]),
                tokens=int(data["tokens"]),
                content_type=str(data["content_type"]),
                urgency=str(data.get("urgency", "low")),
                context=(
                    dict(context) if context is not None else None
                ),
                deferred_at=float(
                    data.get("deferred_at", time.time())
                ),
                deferred_intensity_g_kwh=float(
                    data.get("deferred_intensity_g_kwh", 0.0)
                ),
            )
        except KeyError as exc:
            raise CarbonAdaptiveError(
                f"DeferredRetrieval payload missing "
                f"{exc.args[0]!r}."
            ) from exc
        except (TypeError, ValueError) as exc:
            if isinstance(exc, CarbonAdaptiveError):
                raise
            raise CarbonAdaptiveError(
                f"invalid DeferredRetrieval payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "DeferredRetrieval("
            f"query={self.query!r}, tokens={self.tokens}, "
            f"content_type={self.content_type!r}, "
            f"urgency={self.urgency!r})"
        )


# --------------------------------------------------------------------------- #
# Controller
# --------------------------------------------------------------------------- #
class CarbonAdaptiveRetrievalController:
    """
    Carbon-adaptive controller for retrieval operations.

    Thread-safe, serializable, and bounded in memory.
    """

    def __init__(
        self,
        carbon_throttler: Optional[Any] = None,
        clean_grid_threshold_g_kwh: float = 200.0,
        dirty_grid_threshold_g_kwh: float = 400.0,
        enable_deferral: bool = True,
        *,
        config: Optional[CarbonAdaptiveConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = CarbonAdaptiveConfig(
                clean_threshold_g_kwh=float(
                    clean_grid_threshold_g_kwh
                ),
                dirty_threshold_g_kwh=float(
                    dirty_grid_threshold_g_kwh
                ),
                enable_deferral=bool(enable_deferral),
            )
        self._strict = bool(strict)

        self.carbon_throttler = carbon_throttler
        if carbon_throttler is not None:
            logger.warning(
                "carbon_throttler is deprecated and ignored; the "
                "controller manages its own grid state via "
                "update_carbon_intensity()."
            )

        # Legacy attributes preserved.
        self.clean_threshold = self._config.clean_threshold_g_kwh
        self.dirty_threshold = self._config.dirty_threshold_g_kwh
        self.enable_deferral = self._config.enable_deferral

        self.current_intensity_g_kwh = (
            self._config.default_intensity_g_kwh
        )
        self.current_mode = self._determine_mode(
            self.current_intensity_g_kwh
        )

        self._lock = threading.RLock()
        self.deferred_retrievals: Deque[DeferredRetrieval] = deque(
            maxlen=self._config.max_deferred
        )
        self._deferred_evictions: int = 0
        self._history: Deque[RetrievalDecision] = deque(
            maxlen=self._config.max_history
        )

        self.total_decisions: int = 0
        self.decisions_by_mode: Dict[RetrievalMode, int] = {
            mode: 0 for mode in RetrievalMode
        }
        self.carbon_saved_kg: float = 0.0
        self.retrievals_deferred: int = 0

        logger.debug(
            "CarbonAdaptiveRetrievalController initialized "
            "(clean<%.0f dirty<%.0f default=%.0f deferral=%s "
            "strict=%s)",
            self.clean_threshold, self.dirty_threshold,
            self._config.default_intensity_g_kwh,
            self.enable_deferral, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> CarbonAdaptiveConfig:
        return self._config

    @property
    def pending_deferred(self) -> int:
        with self._lock:
            return len(self.deferred_retrievals)

    @property
    def history(self) -> List[RetrievalDecision]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- grid state
    def update_carbon_intensity(
        self, intensity_g_kwh: float,
    ) -> RetrievalMode:
        """Update grid intensity; re-evaluate deferred on transition to FULL."""
        intensity = self._validate_intensity(intensity_g_kwh)
        if intensity is None:
            return self.current_mode

        with self._lock:
            old_mode = self.current_mode
            self.current_intensity_g_kwh = intensity
            new_mode = self._determine_mode(intensity)
            self.current_mode = new_mode

        if (
            old_mode != RetrievalMode.FULL
            and new_mode == RetrievalMode.FULL
        ):
            processed = self._process_deferred_retrievals()
            logger.debug(
                "Grid became clean; processed %d deferred "
                "retrieval(s).",
                len(processed),
            )

        logger.info(
            "Carbon intensity updated to %.1f g/kWh; mode=%s.",
            intensity, new_mode.value,
        )
        return new_mode

    def _determine_mode(
        self, intensity_g_kwh: float,
    ) -> RetrievalMode:
        if intensity_g_kwh < self.clean_threshold:
            return RetrievalMode.FULL
        if intensity_g_kwh < self.dirty_threshold:
            return RetrievalMode.RESTRICTED
        return RetrievalMode.MINIMAL

    # ---------------------------------------------------------- decision
    def decide_retrieval(
        self,
        query: str,
        estimated_tokens: int,
        content_type: str = "text",
        urgency: str = "normal",
        context: Optional[Dict[str, Any]] = None,
    ) -> RetrievalDecision:
        """
        Decide whether and how to perform a retrieval.

        Public counters are updated exactly once per call (including
        the invalid-input path in non-strict mode).
        """
        query = self._validate_query(query)
        tokens = self._validate_tokens(estimated_tokens)
        content_type = self._validate_content_type(content_type)
        urgency = self._validate_urgency(urgency)

        if context is not None and not isinstance(context, Mapping):
            msg = (
                f"context must be a Mapping or None, got "
                f"{type(context).__name__}."
            )
            if self._strict:
                raise CarbonAdaptiveError(msg)
            logger.warning("%s Ignoring context.", msg)
            context = None

        if (
            query is None
            or tokens is None
            or content_type is None
            or urgency is None
        ):
            with self._lock:
                self.total_decisions += 1
                self.decisions_by_mode[
                    RetrievalMode.MINIMAL
                ] += 1
                intensity = self.current_intensity_g_kwh
            decision = RetrievalDecision(
                allowed=False,
                mode=RetrievalMode.MINIMAL,
                max_tokens=0,
                allow_visual=False,
                allow_multimodal=False,
                reason="invalid_input_rejected",
                carbon_intensity_g_kwh=intensity,
                estimated_carbon_kg=0.0,
                effective_content_type="text",
            )
            self._record(decision)
            return decision

        decision = self._evaluate(
            query=query,
            tokens=tokens,
            content_type=content_type,
            urgency=urgency,
            context=context,
        )

        # Single counter-mutation site (matches ``_evaluate``'s
        # deferral, which no longer touches counters).
        with self._lock:
            self.total_decisions += 1
            self.decisions_by_mode[decision.mode] += 1
            if decision.mode == RetrievalMode.DEFERRED:
                self.retrievals_deferred += 1
            self._history.append(decision)

        logger.debug(
            "Decision: mode=%s allowed=%s tokens=%d reason=%s",
            decision.mode.value, decision.allowed,
            decision.max_tokens, decision.reason,
        )
        return decision

    def _evaluate(
        self,
        *,
        query: str,
        tokens: int,
        content_type: str,
        urgency: str,
        context: Optional[Mapping[str, Any]],
    ) -> RetrievalDecision:
        """
        Pure evaluation. **Never touches public counters** — the
        caller updates them in one place to maintain the invariant
        ``sum(decisions_by_mode) == total_decisions``.
        """
        with self._lock:
            mode = self.current_mode
            intensity = self.current_intensity_g_kwh

        estimated_carbon = self._estimate_carbon_impact(
            tokens=tokens,
            content_type=content_type,
            intensity_g_kwh=intensity,
        )

        should_defer = (
            self.enable_deferral
            and urgency == "low"
            and mode == RetrievalMode.MINIMAL
        )
        if should_defer:
            self._enqueue_deferred(
                query, tokens, content_type, urgency,
                context, intensity,
            )
            return RetrievalDecision(
                allowed=False,
                mode=RetrievalMode.DEFERRED,
                max_tokens=0,
                allow_visual=False,
                allow_multimodal=False,
                reason="deferred_due_to_dirty_grid_and_low_urgency",
                carbon_intensity_g_kwh=intensity,
                estimated_carbon_kg=estimated_carbon,
                effective_content_type="text",
            )

        if mode == RetrievalMode.FULL:
            return RetrievalDecision(
                allowed=True,
                mode=mode,
                max_tokens=tokens,
                allow_visual=True,
                allow_multimodal=True,
                reason="clean_grid_full_retrieval_allowed",
                carbon_intensity_g_kwh=intensity,
                estimated_carbon_kg=estimated_carbon,
                effective_content_type=content_type,
            )

        if mode == RetrievalMode.RESTRICTED:
            max_tokens = min(
                tokens, self._config.restricted_max_tokens,
            )
            if content_type in ("visual", "multimodal"):
                effective_type = "text"
                allow_visual = False
            else:
                effective_type = content_type
                allow_visual = True
            if max_tokens < tokens:
                self._account_savings(
                    tokens - max_tokens,
                    content_type,
                    intensity,
                )
            return RetrievalDecision(
                allowed=True,
                mode=mode,
                max_tokens=max_tokens,
                allow_visual=allow_visual,
                allow_multimodal=False,
                reason="moderate_grid_restricted_retrieval",
                carbon_intensity_g_kwh=intensity,
                estimated_carbon_kg=estimated_carbon,
                effective_content_type=effective_type,
            )

        # MINIMAL mode
        if urgency == "urgent":
            max_tokens = min(
                tokens, self._config.minimal_max_tokens,
            )
            saved = tokens - max_tokens
            if saved > 0:
                self._account_savings(
                    saved, content_type, intensity,
                )
            return RetrievalDecision(
                allowed=True,
                mode=mode,
                max_tokens=max_tokens,
                allow_visual=False,
                allow_multimodal=False,
                reason="dirty_grid_urgent_minimal_retrieval",
                carbon_intensity_g_kwh=intensity,
                estimated_carbon_kg=estimated_carbon,
                effective_content_type="text",
            )

        # MINIMAL + non-urgent: deny and credit the full estimate.
        self._account_savings(tokens, content_type, intensity)
        return RetrievalDecision(
            allowed=False,
            mode=mode,
            max_tokens=0,
            allow_visual=False,
            allow_multimodal=False,
            reason="dirty_grid_non_urgent_retrieval_denied",
            carbon_intensity_g_kwh=intensity,
            estimated_carbon_kg=estimated_carbon,
            effective_content_type="text",
        )

    def _account_savings(
        self,
        saved_tokens: int,
        content_type: str,
        intensity_g_kwh: float,
    ) -> None:
        """
        Credit ``saved_tokens`` worth of avoided carbon to
        ``carbon_saved_kg``.

        Applies the same per-content-type multiplier used by
        :meth:`_estimate_carbon_impact` — a visual saved token costs
        more than a text saved token.
        """
        if saved_tokens <= 0:
            return
        cfg = self._config
        multiplier = {
            "text": 1.0,
            "visual": cfg.visual_energy_multiplier,
            "multimodal": cfg.multimodal_energy_multiplier,
            "audio": cfg.audio_energy_multiplier,
        }.get(content_type, 1.0)
        energy_wh = (
            saved_tokens
            * cfg.energy_per_token_wh
            * multiplier
        )
        saved_kg = energy_wh * intensity_g_kwh / 1e6
        with self._lock:
            self.carbon_saved_kg += saved_kg

    # ---------------------------------------------------------- estimation
    def _estimate_carbon_impact(
        self,
        tokens: int,
        content_type: str,
        intensity_g_kwh: float,
    ) -> float:
        """Estimate carbon impact in kilograms CO2e."""
        cfg = self._config
        multiplier = {
            "text": 1.0,
            "visual": cfg.visual_energy_multiplier,
            "multimodal": cfg.multimodal_energy_multiplier,
            "audio": cfg.audio_energy_multiplier,
        }.get(content_type, 1.0)
        base_energy_wh = (
            tokens * cfg.energy_per_token_wh * multiplier
        )
        return (base_energy_wh * intensity_g_kwh) / 1e6

    # ---------------------------------------------------------- deferral
    def _enqueue_deferred(
        self,
        query: str,
        tokens: int,
        content_type: str,
        urgency: str,
        context: Optional[Mapping[str, Any]],
        intensity: float,
    ) -> None:
        entry = DeferredRetrieval(
            query=query,
            tokens=tokens,
            content_type=content_type,
            urgency=urgency,
            context=dict(context) if context is not None else None,
            deferred_at=time.time(),
            deferred_intensity_g_kwh=intensity,
        )
        with self._lock:
            if (
                self._config.max_deferred is not None
                and len(self.deferred_retrievals)
                >= self._config.max_deferred
            ):
                self._deferred_evictions += 1
            self.deferred_retrievals.append(entry)

    def _process_deferred_retrievals(
        self,
    ) -> List[Dict[str, Any]]:
        """
        Re-evaluate deferred retrievals when the grid becomes clean.

        Snapshots under lock, evaluates outside lock, then swaps in a
        new queue under lock — concurrent appends that arrive during
        evaluation are preserved (they remain in the queue). Allowed
        entries are removed by identity (``id()``), not by value.
        """
        with self._lock:
            snapshot = list(self.deferred_retrievals)

        if not snapshot:
            return []

        processed_payloads: List[Dict[str, Any]] = []
        processed_ids: Set[int] = set()

        for entry in snapshot:
            decision = self._evaluate(
                query=entry.query,
                tokens=entry.tokens,
                content_type=entry.content_type,
                urgency=entry.urgency,
                context=entry.context,
            )
            with self._lock:
                self.total_decisions += 1
                self.decisions_by_mode[decision.mode] += 1
                if decision.mode == RetrievalMode.DEFERRED:
                    self.retrievals_deferred += 1
                self._history.append(decision)

            if decision.allowed:
                processed_ids.add(id(entry))
                processed_payloads.append({
                    **entry.to_dict(),
                    "processed_at": time.time(),
                    "processed_intensity": (
                        decision.carbon_intensity_g_kwh
                    ),
                    "decision": decision.to_dict(),
                })

        # Swap: remove allowed entries by identity; keep everything
        # else (including entries appended during evaluation).
        with self._lock:
            new_queue = deque(maxlen=self._config.max_deferred)
            for entry in self.deferred_retrievals:
                if id(entry) not in processed_ids:
                    new_queue.append(entry)
            # Detect overflow during re-defer (rare, but honest).
            overflow = max(
                0,
                len(self.deferred_retrievals)
                - len(processed_ids)
                - len(new_queue),
            )
            if overflow:
                self._deferred_evictions += overflow
            self.deferred_retrievals.clear()
            self.deferred_retrievals.extend(new_queue)

        logger.info(
            "Processed %d deferred retrieval(s); %d re-deferred.",
            len(processed_payloads),
            len(snapshot) - len(processed_payloads),
        )
        return processed_payloads

    def _record(self, decision: RetrievalDecision) -> None:
        with self._lock:
            self._history.append(decision)

    # ---------------------------------------------------------- validation
    def _validate_intensity(
        self, intensity_g_kwh: Any,
    ) -> Optional[float]:
        if isinstance(intensity_g_kwh, bool) or not isinstance(
            intensity_g_kwh, numbers.Real,
        ):
            msg = (
                f"intensity_g_kwh must be numeric, "
                f"got {type(intensity_g_kwh).__name__}."
            )
            if self._strict:
                raise CarbonAdaptiveError(msg)
            logger.warning("%s Ignoring update.", msg)
            return None
        fv = float(intensity_g_kwh)
        if not math.isfinite(fv) or fv < 0:
            msg = (
                f"intensity_g_kwh must be finite and >= 0, got "
                f"{intensity_g_kwh!r}."
            )
            if self._strict:
                raise CarbonAdaptiveError(msg)
            logger.warning("%s Ignoring update.", msg)
            return None
        return fv

    def _validate_query(self, query: Any) -> Optional[str]:
        if not isinstance(query, str) or not query:
            msg = (
                f"query must be a non-empty string, got {query!r}."
            )
            if self._strict:
                raise CarbonAdaptiveError(msg)
            logger.warning("%s Rejecting retrieval.", msg)
            return None
        return query

    def _validate_tokens(
        self, estimated_tokens: Any,
    ) -> Optional[int]:
        if (
            isinstance(estimated_tokens, bool)
            or not isinstance(
                estimated_tokens, numbers.Integral,
            )
            or estimated_tokens <= 0
        ):
            msg = (
                f"estimated_tokens must be a positive int, got "
                f"{estimated_tokens!r}."
            )
            if self._strict:
                raise CarbonAdaptiveError(msg)
            logger.warning("%s Rejecting retrieval.", msg)
            return None
        return int(estimated_tokens)

    def _validate_content_type(
        self, content_type: Any,
    ) -> Optional[str]:
        if content_type not in _VALID_CONTENT_TYPES:
            msg = (
                f"content_type must be one of "
                f"{_VALID_CONTENT_TYPES}, got {content_type!r}."
            )
            if self._strict:
                raise CarbonAdaptiveError(msg)
            logger.warning(
                "%s Defaulting to 'text'.", msg,
            )
            return "text"
        return content_type

    def _validate_urgency(
        self, urgency: Any,
    ) -> Optional[str]:
        if urgency not in _VALID_URGENCIES:
            msg = (
                f"urgency must be one of {_VALID_URGENCIES}, "
                f"got {urgency!r}."
            )
            if self._strict:
                raise CarbonAdaptiveError(msg)
            logger.warning(
                "%s Defaulting to 'normal'.", msg,
            )
            return "normal"
        return urgency

    # ---------------------------------------------------------- statistics
    def get_controller_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_decisions": self.total_decisions,
                "decisions_by_mode": {
                    mode.value: count
                    for mode, count in
                    self.decisions_by_mode.items()
                },
                "carbon_saved_kg": self.carbon_saved_kg,
                "retrievals_deferred": self.retrievals_deferred,
                "pending_deferred": len(
                    self.deferred_retrievals
                ),
                "current_mode": self.current_mode.value,
                "current_intensity_g_kwh": (
                    self.current_intensity_g_kwh
                ),
                "deferred_evictions": self._deferred_evictions,
                "history_size": len(self._history),
            }

    def statistics(self) -> Dict[str, Any]:
        return self.get_controller_stats()

    def get_recommendations(self) -> List[str]:
        recommendations: List[str] = []
        with self._lock:
            mode = self.current_mode
            intensity = self.current_intensity_g_kwh
            stats_snapshot = {
                "pending_deferred": len(
                    self.deferred_retrievals
                ),
                "carbon_saved_kg": self.carbon_saved_kg,
            }
        dirty_threshold = self.dirty_threshold

        if mode == RetrievalMode.MINIMAL:
            recommendations.append(
                "Operating in MINIMAL mode due to dirty grid. "
                "Only urgent text retrievals allowed."
            )
        elif mode == RetrievalMode.RESTRICTED:
            recommendations.append(
                "Operating in RESTRICTED mode. "
                "Visual and multimodal content limited."
            )
        else:
            recommendations.append(
                "Operating in FULL mode. Clean grid allows full "
                "retrieval."
            )

        if stats_snapshot["pending_deferred"] > 0:
            recommendations.append(
                f"{stats_snapshot['pending_deferred']} retrievals "
                f"deferred. Will process when grid is cleaner."
            )

        if stats_snapshot["carbon_saved_kg"] > 0.001:
            recommendations.append(
                f"Carbon-adaptive control has saved "
                f"{stats_snapshot['carbon_saved_kg']:.6f} kg CO2."
            )

        if intensity > dirty_threshold:
            recommendations.append(
                "Consider scheduling non-urgent retrievals for "
                "cleaner grid periods (typically overnight or during "
                "high renewable generation)."
            )

        return recommendations

    def get_deferred_summary(self) -> Dict[str, Any]:
        with self._lock:
            snapshot = list(self.deferred_retrievals)
            evictions = self._deferred_evictions
            now = time.time()

        if not snapshot:
            return {"pending": 0, "deferred_evictions": evictions}

        total_tokens = sum(r.tokens for r in snapshot)
        avg_wait = (
            sum(now - r.deferred_at for r in snapshot)
            / len(snapshot)
        )
        by_type: Dict[str, int] = {}
        for r in snapshot:
            by_type[r.content_type] = (
                by_type.get(r.content_type, 0) + 1
            )

        return {
            "pending": len(snapshot),
            "total_tokens_deferred": total_tokens,
            "avg_wait_time_seconds": avg_wait,
            "oldest_deferred": min(
                r.deferred_at for r in snapshot
            ),
            "deferred_by_type": by_type,
            "deferred_evictions": evictions,
        }

    def force_process_deferred(self) -> List[Dict[str, Any]]:
        """
        Force-process all deferred retrievals, bypassing the carbon
        check. Each forced entry produces an evaluation just like
        the normal path (recorded in counters and history), with an
        additional ``"forced": True`` marker.
        """
        with self._lock:
            snapshot = list(self.deferred_retrievals)
            self.deferred_retrievals.clear()

        now = time.time()
        processed: List[Dict[str, Any]] = []
        for entry in snapshot:
            # Force-process by pretending the urgency is urgent so
            # ``_evaluate`` allows it (in MINIMAL) or by re-evaluating
            # in FULL mode.
            with self._lock:
                original_mode = self.current_mode
                self.current_mode = RetrievalMode.FULL
            try:
                decision = self._evaluate(
                    query=entry.query,
                    tokens=entry.tokens,
                    content_type=entry.content_type,
                    urgency=entry.urgency,
                    context=entry.context,
                )
            finally:
                with self._lock:
                    self.current_mode = original_mode
            with self._lock:
                self.total_decisions += 1
                self.decisions_by_mode[decision.mode] += 1
                self._history.append(decision)

            processed.append({
                **entry.to_dict(),
                "processed_at": now,
                "processed_intensity": (
                    self.current_intensity_g_kwh
                ),
                "decision": decision.to_dict(),
                "forced": True,
            })

        if processed:
            logger.warning(
                "Force-processed %d deferred retrieval(s) "
                "regardless of carbon intensity.",
                len(processed),
            )
        return processed

    def reset(self, *, clear_deferred: bool = True) -> None:
        with self._lock:
            if clear_deferred:
                self.deferred_retrievals.clear()
            self.total_decisions = 0
            self.decisions_by_mode = {
                mode: 0 for mode in RetrievalMode
            }
            self.carbon_saved_kg = 0.0
            self.retrievals_deferred = 0
            self._deferred_evictions = 0
            self._history.clear()
        logger.debug("CarbonAdaptiveRetrievalController reset.")

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_history: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "version": VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "current_intensity_g_kwh": (
                    self.current_intensity_g_kwh
                ),
                "current_mode": self.current_mode.value,
                "counters": {
                    "total_decisions": self.total_decisions,
                    "decisions_by_mode": {
                        mode.value: count
                        for mode, count in
                        self.decisions_by_mode.items()
                    },
                    "carbon_saved_kg": self.carbon_saved_kg,
                    "retrievals_deferred": (
                        self.retrievals_deferred
                    ),
                    "deferred_evictions": (
                        self._deferred_evictions
                    ),
                },
                "deferred_retrievals": [
                    r.to_dict()
                    for r in self.deferred_retrievals
                ],
            }
            if include_history:
                payload["history"] = [
                    d.to_dict() for d in self._history
                ]
        return payload

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "CarbonAdaptiveRetrievalController":
        if not isinstance(data, Mapping):
            raise CarbonAdaptiveError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        try:
            raw_config = data.get("config") or {}
            if not isinstance(raw_config, Mapping):
                raise CarbonAdaptiveError(
                    "config must be a Mapping."
                )
            cfg = CarbonAdaptiveConfig.from_dict(
                dict(raw_config)
            )

            strict_raw = data.get("strict", True)
            if not isinstance(strict_raw, bool):
                raise CarbonAdaptiveError(
                    f"strict must be bool, got "
                    f"{type(strict_raw).__name__}."
                )
            controller = cls(config=cfg, strict=strict_raw)

            raw_intensity = data.get(
                "current_intensity_g_kwh",
                cfg.default_intensity_g_kwh,
            )
            try:
                intensity = float(raw_intensity)
            except (TypeError, ValueError) as exc:
                raise CarbonAdaptiveError(
                    f"invalid current_intensity_g_kwh: {exc}"
                ) from exc
            if not math.isfinite(intensity) or intensity < 0:
                raise CarbonAdaptiveError(
                    f"current_intensity_g_kwh must be finite and "
                    f">= 0, got {intensity}."
                )

            raw_mode = data.get("current_mode")
            if raw_mode is None:
                derived_mode = controller._determine_mode(
                    intensity
                )
            else:
                try:
                    stored_mode = RetrievalMode(str(raw_mode))
                except ValueError as exc:
                    raise CarbonAdaptiveError(
                        f"invalid current_mode: {exc}"
                    ) from exc
                derived_mode = controller._determine_mode(
                    intensity
                )
                if stored_mode != derived_mode:
                    logger.warning(
                        "stored current_mode=%s does not match "
                        "intensity=%.1f (derived=%s); using "
                        "derived.",
                        stored_mode.value, intensity,
                        derived_mode.value,
                    )

            with controller._lock:
                controller.current_intensity_g_kwh = intensity
                controller.current_mode = derived_mode

                raw_counters = data.get("counters") or {}
                if not isinstance(raw_counters, Mapping):
                    raise CarbonAdaptiveError(
                        "counters must be a Mapping."
                    )
                try:
                    controller.total_decisions = int(
                        raw_counters.get(
                            "total_decisions", 0,
                        )
                    )
                    controller.carbon_saved_kg = float(
                        raw_counters.get("carbon_saved_kg", 0.0)
                    )
                    controller.retrievals_deferred = int(
                        raw_counters.get(
                            "retrievals_deferred", 0,
                        )
                    )
                    controller._deferred_evictions = int(
                        raw_counters.get(
                            "deferred_evictions", 0,
                        )
                    )
                except (TypeError, ValueError) as exc:
                    raise CarbonAdaptiveError(
                        f"invalid counter: {exc}"
                    ) from exc
                for name, value in (
                    (
                        "total_decisions",
                        controller.total_decisions,
                    ),
                    (
                        "carbon_saved_kg",
                        controller.carbon_saved_kg,
                    ),
                    (
                        "retrievals_deferred",
                        controller.retrievals_deferred,
                    ),
                    (
                        "deferred_evictions",
                        controller._deferred_evictions,
                    ),
                ):
                    if value < 0:
                        raise CarbonAdaptiveError(
                            f"counter {name} must be >= 0, got "
                            f"{value}."
                        )

                by_mode_raw = raw_counters.get(
                    "decisions_by_mode", {}
                ) or {}
                if not isinstance(by_mode_raw, Mapping):
                    raise CarbonAdaptiveError(
                        "decisions_by_mode must be a Mapping."
                    )
                for mode in RetrievalMode:
                    try:
                        count = int(
                            by_mode_raw.get(mode.value, 0)
                        )
                    except (TypeError, ValueError) as exc:
                        raise CarbonAdaptiveError(
                            f"invalid decisions_by_mode[{mode.value}]: "
                            f"{exc}"
                        ) from exc
                    if count < 0:
                        raise CarbonAdaptiveError(
                            f"decisions_by_mode[{mode.value}] "
                            f"must be >= 0."
                        )
                    controller.decisions_by_mode[mode] = count

                raw_deferred = data.get(
                    "deferred_retrievals"
                ) or []
                if not isinstance(raw_deferred, list):
                    raise CarbonAdaptiveError(
                        "deferred_retrievals must be a list."
                    )
                for entry in raw_deferred:
                    if not isinstance(entry, Mapping):
                        raise CarbonAdaptiveError(
                            "deferred_retrievals entries must be "
                            "Mappings."
                        )
                    controller.deferred_retrievals.append(
                        DeferredRetrieval.from_dict(entry)
                    )

                raw_history = data.get("history") or []
                if not isinstance(raw_history, list):
                    raise CarbonAdaptiveError(
                        "history must be a list."
                    )
                for entry in raw_history:
                    if not isinstance(entry, Mapping):
                        raise CarbonAdaptiveError(
                            "history entries must be Mappings."
                        )
                    controller._history.append(
                        RetrievalDecision.from_dict(entry)
                    )

            return controller
        except CarbonAdaptiveError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise CarbonAdaptiveError(
                f"invalid CarbonAdaptiveRetrievalController payload: "
                f"{exc}"
            ) from exc

    def to_json(
        self, *, include_history: bool = False, **kwargs: Any,
    ) -> str:
        kwargs.setdefault("allow_nan", False)
        kwargs.setdefault("sort_keys", True)
        return json.dumps(
            self.to_dict(include_history=include_history),
            **kwargs,
        )

    @classmethod
    def from_json(
        cls, payload: str,
    ) -> "CarbonAdaptiveRetrievalController":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise CarbonAdaptiveError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "CarbonAdaptiveRetrievalController":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if exc_type is not None:
            logger.warning(
                "CarbonAdaptiveRetrievalController scope exited "
                "with %s.", exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "CarbonAdaptiveRetrievalController("
                f"mode={self.current_mode.value}, "
                f"intensity={self.current_intensity_g_kwh:.1f}g/kWh, "
                f"decisions={self.total_decisions}, "
                f"pending_deferred="
                f"{len(self.deferred_retrievals)}, "
                f"saved={self.carbon_saved_kg:.6f}kg, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("VERSION    :", VERSION)

    controller = CarbonAdaptiveRetrievalController()
    print("repr       :", controller)

    # ---- Mode transitions --------------------------------------- #
    controller.update_carbon_intensity(120.0)
    d1 = controller.decide_retrieval(
        "summarize renewable energy", 8000, content_type="text",
    )
    print("clean      :", d1)
    assert d1.mode == RetrievalMode.FULL

    controller.update_carbon_intensity(280.0)
    d2 = controller.decide_retrieval(
        "fetch chart", 8000, content_type="visual",
    )
    print("moderate   :", d2)
    assert d2.allowed and not d2.allow_visual
    assert d2.effective_content_type == "text"
    assert d2.max_tokens <= 4096
    print("effective  :", d2.effective_content_type)

    controller.update_carbon_intensity(500.0)
    d3 = controller.decide_retrieval(
        "critical incident", 8000,
        content_type="text", urgency="urgent",
    )
    print("dirty-urg  :", d3)
    assert d3.allowed and d3.max_tokens == 2048

    d4 = controller.decide_retrieval(
        "background", 5000,
        content_type="text", urgency="low",
    )
    print("deferred   :", d4)
    assert d4.mode == RetrievalMode.DEFERRED

    d5 = controller.decide_retrieval(
        "nice", 3000, content_type="text", urgency="normal",
    )
    print("denied     :", d5)
    assert not d5.allowed

    # ---- Counter invariant (was broken) ------------------------ #
    stats = controller.get_controller_stats()
    sum_by_mode = sum(stats["decisions_by_mode"].values())
    total = stats["total_decisions"]
    print(
        f"invariant  : total={total}, sum_by_mode={sum_by_mode}"
    )
    assert sum_by_mode == total, (
        f"invariant violated: "
        f"sum={sum_by_mode} != total={total}"
    )
    # DEFERRED should count exactly once per deferral.
    assert stats["decisions_by_mode"]["deferred"] == 1, (
        f"deferred doubled: {stats['decisions_by_mode']}"
    )
    assert stats["retrievals_deferred"] == 1
    print("invariant  : OK")

    # ---- Savings multiplier accounting ------------------------- #
    # Fresh controller for clean accounting check.
    save_ctrl = CarbonAdaptiveRetrievalController()
    save_ctrl.update_carbon_intensity(500.0)  # MINIMAL
    # Visual non-urgent: full 1000-token saving at 2.5x multiplier.
    save_ctrl.decide_retrieval(
        "vis", 1000, content_type="visual", urgency="normal",
    )
    vis_saved = save_ctrl.carbon_saved_kg
    save_ctrl.reset()
    save_ctrl.update_carbon_intensity(500.0)
    # Text non-urgent: full 1000-token saving at 1.0x multiplier.
    save_ctrl.decide_retrieval(
        "txt", 1000, content_type="text", urgency="normal",
    )
    txt_saved = save_ctrl.carbon_saved_kg
    assert vis_saved > txt_saved * 2.4, (
        f"visual savings ({vis_saved}) should be ~2.5x text "
        f"({txt_saved})"
    )
    print(
        f"savings    : visual={vis_saved:.9f}, "
        f"text={txt_saved:.9f} "
        f"(ratio={vis_saved/txt_saved:.2f}x)"
    )

    # ---- Deferred processing: no double-count ------------------ #
    before_total = controller.total_decisions
    before_deferred_count = (
        controller.decisions_by_mode[RetrievalMode.DEFERRED]
    )
    assert controller.pending_deferred == 1

    controller.update_carbon_intensity(80.0)

    after_total = controller.total_decisions
    after_deferred_count = (
        controller.decisions_by_mode[RetrievalMode.DEFERRED]
    )
    # Exactly +1 total, +1 FULL.
    assert after_total == before_total + 1
    # DEFERRED counter unchanged (this item was originally counted).
    assert after_deferred_count == before_deferred_count
    print("no-double  : OK")

    # Re-verify invariant holds.
    stats = controller.get_controller_stats()
    assert sum(
        stats["decisions_by_mode"].values()
    ) == stats["total_decisions"]
    print("invariant  : OK (after deferred processing)")

    # ---- Urgency preserved on deferral ------------------------- #
    urg_ctrl = CarbonAdaptiveRetrievalController()
    urg_ctrl.update_carbon_intensity(500.0)
    urg_ctrl.decide_retrieval(
        "u1", 1000, content_type="text", urgency="low",
    )
    assert urg_ctrl.pending_deferred == 1
    # Switch to RESTRICTED (not FULL) — the deferred item should
    # stay deferred because the mode isn't clean enough to
    # re-process.
    urg_ctrl.update_carbon_intensity(280.0)
    # Item stays in queue.
    assert urg_ctrl.pending_deferred == 1
    stored = list(urg_ctrl.deferred_retrievals)[0]
    assert stored.urgency == "low", (
        f"urgency not preserved: {stored.urgency}"
    )
    print("urgency    : preserved")

    # ---- Deferred summary / force-process --------------------- #
    forc_ctrl = CarbonAdaptiveRetrievalController()
    forc_ctrl.update_carbon_intensity(600.0)
    for i in range(3):
        forc_ctrl.decide_retrieval(
            f"bg-{i}", 1000 + i,
            content_type="text", urgency="low",
        )
    summary = forc_ctrl.get_deferred_summary()
    print(
        "summary    :", {
            "pending": summary["pending"],
            "total_tokens": summary["total_tokens_deferred"],
            "by_type": summary["deferred_by_type"],
        }
    )

    forced = forc_ctrl.force_process_deferred()
    assert len(forced) == 3
    for entry in forced:
        assert "decision" in entry, (
            "force-processed entries must include the decision"
        )
        assert entry["forced"] is True
    print("force      : OK (decision included)")

    # ---- Race-safe processing --------------------------------- #
    # (Functional check: concurrent deferral during processing
    # should not be lost.)
    race_ctrl = CarbonAdaptiveRetrievalController()
    race_ctrl.update_carbon_intensity(500.0)
    race_ctrl.decide_retrieval(
        "pre-race", 1000, content_type="text", urgency="low",
    )
    # Simulate a new deferral arriving during processing.
    def race_add():
        race_ctrl.decide_retrieval(
            "during", 1000, content_type="text", urgency="low",
        )
    import threading as _threading
    race_ctrl.update_carbon_intensity(80.0)
    # Trigger another deferral while processing happens elsewhere.
    t = _threading.Thread(target=race_add)
    t.start()
    t.join()
    # The race-added entry should still be in the queue
    # (unless it was allowed because grid was clean).
    pending = race_ctrl.pending_deferred
    print(f"race       : pending={pending}")

    # ---- Serialization round-trip ----------------------------- #
    payload = controller.to_json()
    restored = CarbonAdaptiveRetrievalController.from_json(payload)
    assert restored.to_dict() == controller.to_dict()
    print("round-trip : OK")

    # ---- to_json rejects NaN ---------------------------------- #
    tainted = CarbonAdaptiveRetrievalController()
    object.__setattr__(
        tainted._config,
        "default_intensity_g_kwh",
        float("nan"),
    )
    try:
        tainted.to_json()
    except ValueError as exc:
        print("NaN json   :", str(exc)[:60])

    # ---- Config validation ------------------------------------ #
    for bad_cfg in (
        dict(clean_threshold_g_kwh=0),
        dict(
            clean_threshold_g_kwh=400,
            dirty_threshold_g_kwh=200,
        ),
        dict(restricted_max_tokens=0),
        dict(minimal_max_tokens=0),
        dict(
            minimal_max_tokens=9000,
            restricted_max_tokens=4096,
        ),
        dict(energy_per_token_wh=0),
        dict(visual_energy_multiplier=0),
        dict(max_deferred=0),
        dict(max_history=0),
        dict(enable_deferral="yes"),
    ):
        try:
            CarbonAdaptiveConfig(
                **bad_cfg  # type: ignore[arg-type]
            )
        except CarbonAdaptiveError as exc:
            print("Rejected c :", str(exc)[:50])

    # ---- from_dict error wrapping ----------------------------- #
    for bad in (
        {"strict": "yes"},
        {"current_intensity_g_kwh": "abc"},
        {"current_intensity_g_kwh": -1.0},
        {"current_mode": "bogus"},
        {"counters": {"total_decisions": "abc"}},
        {"counters": {"total_decisions": -1}},
        {"deferred_retrievals": "not-a-list"},
        {"deferred_retrievals": [{"query": "x"}]},
        {"history": "not-a-list"},
        {"config": {"enable_deferral": "yes"}},
        {"config": {"clean_threshold_g_kwh": "abc"}},
    ):
        try:
            CarbonAdaptiveRetrievalController.from_dict(bad)
        except CarbonAdaptiveError as exc:
            print("Rejected p :", str(exc)[:60])

    # ---- Strict-mode input validation ------------------------- #
    strict = CarbonAdaptiveRetrievalController(strict=True)
    for bad_call in (
        lambda: strict.update_carbon_intensity(float("nan")),
        lambda: strict.update_carbon_intensity(-1.0),
        lambda: strict.update_carbon_intensity(float("inf")),
        lambda: strict.decide_retrieval("", 100),
        lambda: strict.decide_retrieval("q", 0),
        lambda: strict.decide_retrieval("q", -1),
        lambda: strict.decide_retrieval(
            "q", 100, content_type="bogus",
        ),
        lambda: strict.decide_retrieval(
            "q", 100, urgency="emergency",
        ),
    ):
        try:
            bad_call()
        except CarbonAdaptiveError as exc:
            print("Rejected   :", str(exc)[:50])

    # ---- Non-strict coerces safely --------------------------- #
    lenient = CarbonAdaptiveRetrievalController(strict=False)
    lenient.update_carbon_intensity(float("nan"))
    lenient.decide_retrieval("q", 100, content_type="bogus")
    lenient.decide_retrieval("q", 100, urgency="bogus")
    d = lenient.decide_retrieval("", 100)
    assert not d.allowed
    print("lenient    : OK")

    # ---- Context manager ------------------------------------- #
    with CarbonAdaptiveRetrievalController() as scoped:
        scoped.update_carbon_intensity(50.0)
        scoped.decide_retrieval(
            "hello", 500, content_type="text",
        )
    print("context    : OK")

    print("\nSmoke test passed.")
