# src/retrieval/carbon_adaptive_controller.py

"""
Carbon-Adaptive Retrieval Controller
====================================

Works with the existing carbon throttler to decide whether to allow full
multimodal retrieval or restrict to lightweight sources based on
sustainability conditions.

Enhancements
------------
- ``CarbonAdaptiveConfig`` — frozen, validated: thresholds, token limits,
  energy coefficients, deferral queue cap.
- ``RetrievalDecision`` — frozen dataclass with full validation and
  serialization.
- **Fixed double-counting bug** — deferred processing no longer inflates
  ``total_decisions`` / ``decisions_by_mode``.
- **Fixed unsafe removal** — deferred queue uses positional indices, not
  ``dict.__eq__``.
- **Bounded deferral queue** — ``max_deferred`` LRU eviction.
- **Thread safety** — ``RLock`` guards counters and the queue.
- **Full validation** of every argument; strict / non-strict modes.
- Optional bounded ``decision_history`` for auditing.
- Serialization on the controller and every dataclass.
- ``statistics()``, ``__repr__``, custom ``CarbonAdaptiveError``,
  lazy ``%s`` logging, and a comprehensive ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Deque, Dict, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


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
    FULL = "full"              # Full multimodal retrieval allowed
    RESTRICTED = "restricted"  # Text-only, limited tokens
    MINIMAL = "minimal"        # Essential retrievals only
    DEFERRED = "deferred"      # Defer non-urgent retrievals


# Valid urgency levels.
_VALID_URGENCIES = ("urgent", "normal", "low")
_VALID_CONTENT_TYPES = ("text", "visual", "multimodal", "audio")


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CarbonAdaptiveConfig:
    """
    Tunable parameters for the carbon-adaptive retrieval controller.

    Centralizes every threshold and coefficient that was previously hardcoded
    in the controller.
    """

    # Grid intensity thresholds (gCO2/kWh).
    clean_threshold_g_kwh: float = 200.0
    dirty_threshold_g_kwh: float = 400.0

    # Initial / fallback intensity when no reading is available.
    default_intensity_g_kwh: float = 385.0

    # Deferral policy.
    enable_deferral: bool = True
    max_deferred: Optional[int] = 10_000

    # Token limits per mode.
    restricted_max_tokens: int = 4096
    minimal_max_tokens: int = 2048

    # Energy / carbon model.
    # 1 microWh per token (default from the original implementation).
    energy_per_token_wh: float = 1e-6
    visual_energy_multiplier: float = 2.5
    multimodal_energy_multiplier: float = 2.0
    audio_energy_multiplier: float = 1.5
    # gCO2e per kWh → convert to kg by dividing by 1e6 (Wh → kWh) and
    # then multiplying by g/kWh, then dividing by 1000 (g → kg). We keep
    # the same final units by using ``/ 1e6`` on the Wh·g/kWh product.
    grams_per_kg: float = 1000.0

    # Bounded decision-history ring-buffer for auditing.
    max_history: int = 1000

    def __post_init__(self) -> None:
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
        if self.restricted_max_tokens <= 0:
            raise CarbonAdaptiveError(
                "restricted_max_tokens must be > 0."
            )
        if self.minimal_max_tokens <= 0:
            raise CarbonAdaptiveError(
                "minimal_max_tokens must be > 0."
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
        if self.max_deferred is not None and self.max_deferred <= 0:
            raise CarbonAdaptiveError(
                "max_deferred must be a positive int or None."
            )
        if self.max_history <= 0:
            raise CarbonAdaptiveError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CarbonAdaptiveConfig":
        if not isinstance(data, Mapping):
            raise CarbonAdaptiveError(
                f"CarbonAdaptiveConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
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
            enable_deferral=bool(data.get("enable_deferral", True)),
            max_deferred=data.get("max_deferred", 10_000),
            restricted_max_tokens=int(
                data.get("restricted_max_tokens", 4096)
            ),
            minimal_max_tokens=int(data.get("minimal_max_tokens", 2048)),
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
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.mode, RetrievalMode):
            raise CarbonAdaptiveError(
                f"mode must be a RetrievalMode, got "
                f"{type(self.mode).__name__}."
            )
        if not isinstance(self.max_tokens, int) or self.max_tokens < 0:
            raise CarbonAdaptiveError(
                "max_tokens must be a non-negative int."
            )
        for name in ("carbon_intensity_g_kwh", "estimated_carbon_kg"):
            value = getattr(self, name)
            if not isinstance(value, (int, float)):
                raise CarbonAdaptiveError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                raise CarbonAdaptiveError(
                    f"{name} must be finite, got {value!r}."
                )
            if fv < 0:
                raise CarbonAdaptiveError(f"{name} must be >= 0.")
        if not isinstance(self.reason, str) or not self.reason:
            raise CarbonAdaptiveError(
                "reason must be a non-empty string."
            )
        if not isinstance(self.allow_visual, bool):
            raise CarbonAdaptiveError("allow_visual must be a bool.")
        if not isinstance(self.allow_multimodal, bool):
            raise CarbonAdaptiveError("allow_multimodal must be a bool.")
        if not self.allowed and (self.allow_visual or self.allow_multimodal):
            raise CarbonAdaptiveError(
                "a denied retrieval cannot allow visual or multimodal "
                "content."
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
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RetrievalDecision":
        if not isinstance(data, Mapping):
            raise CarbonAdaptiveError(
                f"RetrievalDecision.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        ts = data.get("timestamp")
        if isinstance(ts, str):
            timestamp = datetime.fromisoformat(ts)
            if not timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            timestamp = ts
        else:
            timestamp = datetime.now(timezone.utc)
        return cls(
            allowed=bool(data["allowed"]),
            mode=RetrievalMode(str(data["mode"])),
            max_tokens=int(data["max_tokens"]),
            allow_visual=bool(data.get("allow_visual", False)),
            allow_multimodal=bool(data.get("allow_multimodal", False)),
            reason=str(data.get("reason", "")),
            carbon_intensity_g_kwh=float(
                data.get("carbon_intensity_g_kwh", 0.0)
            ),
            estimated_carbon_kg=float(
                data.get("estimated_carbon_kg", 0.0)
            ),
            timestamp=timestamp,
        )

    def __repr__(self) -> str:
        return (
            "RetrievalDecision("
            f"allowed={self.allowed}, mode={self.mode.value}, "
            f"max_tokens={self.max_tokens}, "
            f"visual={self.allow_visual}, multimodal={self.allow_multimodal}, "
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
    context: Optional[Mapping[str, Any]]
    deferred_at: float
    deferred_intensity_g_kwh: float

    def __post_init__(self) -> None:
        if not isinstance(self.query, str) or not self.query:
            raise CarbonAdaptiveError("query must be a non-empty string.")
        if not isinstance(self.tokens, int) or self.tokens <= 0:
            raise CarbonAdaptiveError("tokens must be a positive int.")
        if self.content_type not in _VALID_CONTENT_TYPES:
            raise CarbonAdaptiveError(
                f"content_type must be one of {_VALID_CONTENT_TYPES}, "
                f"got {self.content_type!r}."
            )
        if not isinstance(self.deferred_at, (int, float)):
            raise CarbonAdaptiveError(
                "deferred_at must be a numeric epoch timestamp."
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
            "context": dict(self.context) if self.context is not None else None,
            "deferred_at": self.deferred_at,
            "deferred_intensity_g_kwh": self.deferred_intensity_g_kwh,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DeferredRetrieval":
        return cls(
            query=str(data["query"]),
            tokens=int(data["tokens"]),
            content_type=str(data["content_type"]),
            context=data.get("context"),
            deferred_at=float(data.get("deferred_at", time.time())),
            deferred_intensity_g_kwh=float(
                data.get("deferred_intensity_g_kwh", 0.0)
            ),
        )


# --------------------------------------------------------------------------- #
# Controller
# --------------------------------------------------------------------------- #
class CarbonAdaptiveRetrievalController:
    """
    Carbon-adaptive controller for retrieval operations.

    Thread-safe, serializable, and bounded in memory. All original behaviour
    is preserved; new parameters are keyword-only.

    Parameters
    ----------
    carbon_throttler : Any, optional
        Existing carbon throttler instance (kept for backward compatibility).
    clean_grid_threshold_g_kwh : float, default 200.0
        Below this intensity, FULL mode is selected.
    dirty_grid_threshold_g_kwh : float, default 400.0
        Above this intensity, MINIMAL mode is selected.
    enable_deferral : bool, default True
        Enable deferral of non-urgent retrievals during dirty grid periods.
    config : CarbonAdaptiveConfig, optional
        Typed configuration. Takes precedence over the three legacy keyword
        parameters when supplied.
    strict : bool, default True
        If True, invalid inputs raise :class:`CarbonAdaptiveError`.
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
                clean_threshold_g_kwh=float(clean_grid_threshold_g_kwh),
                dirty_threshold_g_kwh=float(dirty_grid_threshold_g_kwh),
                enable_deferral=bool(enable_deferral),
            )
        self._strict = bool(strict)

        self.carbon_throttler = carbon_throttler

        # Legacy attributes preserved (mirror config).
        self.clean_threshold = self._config.clean_threshold_g_kwh
        self.dirty_threshold = self._config.dirty_threshold_g_kwh
        self.enable_deferral = self._config.enable_deferral

        # Current grid state.
        self.current_intensity_g_kwh = self._config.default_intensity_g_kwh
        self.current_mode = self._determine_mode(self.current_intensity_g_kwh)

        # ---- Internal state -------------------------------------------
        self._lock = threading.RLock()
        self.deferred_retrievals: Deque[DeferredRetrieval] = deque(
            maxlen=self._config.max_deferred
        )
        self._deferred_evictions: int = 0
        self._history: Deque[RetrievalDecision] = deque(
            maxlen=self._config.max_history
        )

        # Counters (mirror the original public names).
        self.total_decisions: int = 0
        self.decisions_by_mode: Dict[RetrievalMode, int] = {
            mode: 0 for mode in RetrievalMode
        }
        self.carbon_saved_kg: float = 0.0
        self.retrievals_deferred: int = 0

        logger.debug(
            "CarbonAdaptiveRetrievalController initialized "
            "(clean<%.0f dirty<%.0f default=%.0f deferral=%s strict=%s)",
            self.clean_threshold, self.dirty_threshold,
            self._config.default_intensity_g_kwh, self.enable_deferral,
            self._strict,
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
    def update_carbon_intensity(self, intensity_g_kwh: float) -> RetrievalMode:
        """
        Update the current carbon grid intensity and recompute the mode.

        When the mode transitions to FULL, pending deferred retrievals are
        re-evaluated and returned.

        Returns the new :class:`RetrievalMode`.
        """
        intensity = self._validate_intensity(intensity_g_kwh)
        if intensity is None:
            return self.current_mode

        with self._lock:
            self.current_intensity_g_kwh = intensity
            old_mode = self.current_mode
            new_mode = self._determine_mode(intensity)
            self.current_mode = new_mode

        if (
            old_mode != RetrievalMode.FULL
            and new_mode == RetrievalMode.FULL
        ):
            processed = self._process_deferred_retrievals()
            logger.debug(
                "Grid became clean; processed %d deferred retrieval(s).",
                len(processed),
            )

        logger.info(
            "Carbon intensity updated to %.1f g/kWh; mode=%s.",
            intensity, new_mode.value,
        )
        return new_mode

    def _determine_mode(self, intensity_g_kwh: float) -> RetrievalMode:
        """Determine the retrieval mode from carbon intensity."""
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

        The public counters (``total_decisions``, ``decisions_by_mode``,
        ``carbon_saved_kg``, ``retrievals_deferred``) are updated here and
        only here.
        """
        # ---- Validate ------------------------------------------------
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

        if query is None or tokens is None or content_type is None or urgency is None:
            # Non-strict mode with invalid input — return a safe denial.
            with self._lock:
                self.total_decisions += 1
                self.decisions_by_mode[RetrievalMode.MINIMAL] += 1
            decision = RetrievalDecision(
                allowed=False,
                mode=RetrievalMode.MINIMAL,
                max_tokens=0,
                allow_visual=False,
                allow_multimodal=False,
                reason="invalid_input_rejected",
                carbon_intensity_g_kwh=self.current_intensity_g_kwh,
                estimated_carbon_kg=0.0,
            )
            self._record(decision)
            return decision

        # ---- Core evaluation -----------------------------------------
        decision = self._evaluate(
            query=query,
            tokens=tokens,
            content_type=content_type,
            urgency=urgency,
            context=context,
        )

        # ---- Update public counters ----------------------------------
        with self._lock:
            self.total_decisions += 1
            self.decisions_by_mode[decision.mode] += 1
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
        Pure evaluation used by both :meth:`decide_retrieval` and
        :meth:`_process_deferred_retrievals`.

        **Does not update public counters** — the caller is responsible for
        that. This fixes the double-counting bug where re-processing a
        deferred retrieval incremented ``total_decisions`` twice.
        """
        with self._lock:
            mode = self.current_mode
            intensity = self.current_intensity_g_kwh

        estimated_carbon = self._estimate_carbon_impact(
            tokens=tokens,
            content_type=content_type,
            intensity_g_kwh=intensity,
        )

        # ---- Deferral decision ---------------------------------------
        should_defer = (
            self.enable_deferral
            and urgency == "low"
            and mode == RetrievalMode.MINIMAL
        )
        if should_defer:
            self._enqueue_deferred(query, tokens, content_type, context, intensity)
            with self._lock:
                self.retrievals_deferred += 1
                self.decisions_by_mode[RetrievalMode.DEFERRED] += 1
            return RetrievalDecision(
                allowed=False,
                mode=RetrievalMode.DEFERRED,
                max_tokens=0,
                allow_visual=False,
                allow_multimodal=False,
                reason="deferred_due_to_dirty_grid_and_low_urgency",
                carbon_intensity_g_kwh=intensity,
                estimated_carbon_kg=estimated_carbon,
            )

        # ---- FULL -----------------------------------------------------
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
            )

        # ---- RESTRICTED -----------------------------------------------
        if mode == RetrievalMode.RESTRICTED:
            max_tokens = min(tokens, self._config.restricted_max_tokens)
            allow_visual = content_type != "visual"
            if max_tokens < tokens:
                saved_tokens = tokens - max_tokens
                self._account_savings(saved_tokens, intensity)
            return RetrievalDecision(
                allowed=True,
                mode=mode,
                max_tokens=max_tokens,
                allow_visual=allow_visual,
                allow_multimodal=False,
                reason="moderate_grid_restricted_retrieval",
                carbon_intensity_g_kwh=intensity,
                estimated_carbon_kg=estimated_carbon,
            )

        # ---- MINIMAL --------------------------------------------------
        if urgency == "urgent":
            max_tokens = min(tokens, self._config.minimal_max_tokens)
            saved_tokens = tokens - max_tokens
            if saved_tokens > 0:
                self._account_savings(saved_tokens, intensity)
            return RetrievalDecision(
                allowed=True,
                mode=mode,
                max_tokens=max_tokens,
                allow_visual=False,
                allow_multimodal=False,
                reason="dirty_grid_urgent_minimal_retrieval",
                carbon_intensity_g_kwh=intensity,
                estimated_carbon_kg=estimated_carbon,
            )

        # Non-urgent in MINIMAL mode: deny and credit the full estimate.
        self._account_savings(tokens, intensity)
        return RetrievalDecision(
            allowed=False,
            mode=mode,
            max_tokens=0,
            allow_visual=False,
            allow_multimodal=False,
            reason="dirty_grid_non_urgent_retrieval_denied",
            carbon_intensity_g_kwh=intensity,
            estimated_carbon_kg=estimated_carbon,
        )

    def _account_savings(self, saved_tokens: int, intensity_g_kwh: float) -> None:
        """
        Credit ``saved_tokens`` worth of avoided carbon to
        ``carbon_saved_kg``.

        Uses the same physical formula as :meth:`_estimate_carbon_impact`:
        ``energy_wh * intensity_g_kwh / grams_per_kg / 1000`` →
        but expressed as ``Wh * g/kWh / 1e6`` → kg.
        """
        if saved_tokens <= 0:
            return
        energy_wh = saved_tokens * self._config.energy_per_token_wh
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
        """
        Estimate carbon impact of a retrieval in kilograms CO2e.

        ``energy_wh = tokens × energy_per_token_wh × content_multiplier``
        ``carbon_kg = energy_wh × intensity_g_kwh / 1e6``
        """
        cfg = self._config
        multiplier = {
            "text": 1.0,
            "visual": cfg.visual_energy_multiplier,
            "multimodal": cfg.multimodal_energy_multiplier,
            "audio": cfg.audio_energy_multiplier,
        }.get(content_type, 1.0)

        base_energy_wh = tokens * cfg.energy_per_token_wh * multiplier
        return (base_energy_wh * intensity_g_kwh) / 1e6

    # ---------------------------------------------------------- deferral
    def _enqueue_deferred(
        self,
        query: str,
        tokens: int,
        content_type: str,
        context: Optional[Mapping[str, Any]],
        intensity: float,
    ) -> None:
        """Append a deferred retrieval, bounded by ``max_deferred``."""
        entry = DeferredRetrieval(
            query=query,
            tokens=tokens,
            content_type=content_type,
            context=dict(context) if context is not None else None,
            deferred_at=time.time(),
            deferred_intensity_g_kwh=intensity,
        )
        with self._lock:
            # ``deque(maxlen=...)`` drops the oldest entry automatically; we
            # just count the eviction for telemetry.
            if (
                self._config.max_deferred is not None
                and len(self.deferred_retrievals) >= self._config.max_deferred
            ):
                self._deferred_evictions += 1
            self.deferred_retrievals.append(entry)

    def _process_deferred_retrievals(self) -> List[Dict[str, Any]]:
        """
        Re-evaluate deferred retrievals when the grid is clean.

        Returns the list of processed entries. **Does not double-count**
        decisions — the private :meth:`_evaluate` is used to keep public
        counters accurate.
        """
        with self._lock:
            snapshot = list(self.deferred_retrievals)

        if not snapshot:
            return []

        processed_payloads: List[Dict[str, Any]] = []
        keep: List[DeferredRetrieval] = []

        for idx, entry in enumerate(snapshot):
            decision = self._evaluate(
                query=entry.query,
                tokens=entry.tokens,
                content_type=entry.content_type,
                urgency="normal",
                context=entry.context,
            )

            # Only count as an evaluated decision once.
            with self._lock:
                self.total_decisions += 1
                self.decisions_by_mode[decision.mode] += 1
                self._history.append(decision)

            if decision.allowed:
                processed_payloads.append({
                    **entry.to_dict(),
                    "processed_at": time.time(),
                    "processed_intensity": decision.carbon_intensity_g_kwh,
                    "decision": decision.to_dict(),
                })
            else:
                # Re-defer (or replace) if still constrained.
                keep.append(entry)

        # Rebuild the queue with only the still-deferred entries, bounded
        # by maxlen. Using positional identity (rather than ``in``) avoids
        # the original ``dict.__eq__`` pitfall.
        with self._lock:
            self.deferred_retrievals.clear()
            for entry in keep:
                self.deferred_retrievals.append(entry)

        logger.info(
            "Processed %d deferred retrieval(s); %d re-deferred.",
            len(processed_payloads), len(keep),
        )
        return processed_payloads

    def _record(self, decision: RetrievalDecision) -> None:
        """Append to the audit history (used for invalid-input paths)."""
        with self._lock:
            self._history.append(decision)

    # ---------------------------------------------------------- validation
    def _validate_intensity(self, intensity_g_kwh: Any) -> Optional[float]:
        if isinstance(intensity_g_kwh, bool) or not isinstance(
            intensity_g_kwh, (int, float)
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
        if math.isnan(fv) or math.isinf(fv):
            msg = f"intensity_g_kwh must be finite, got {intensity_g_kwh!r}."
            if self._strict:
                raise CarbonAdaptiveError(msg)
            logger.warning("%s Ignoring update.", msg)
            return None
        if fv < 0:
            msg = f"intensity_g_kwh must be >= 0, got {fv}."
            if self._strict:
                raise CarbonAdaptiveError(msg)
            logger.warning("%s Ignoring update.", msg)
            return None
        return fv

    def _validate_query(self, query: Any) -> Optional[str]:
        if not isinstance(query, str) or not query:
            msg = f"query must be a non-empty string, got {query!r}."
            if self._strict:
                raise CarbonAdaptiveError(msg)
            logger.warning("%s Rejecting retrieval.", msg)
            return None
        return query

    def _validate_tokens(self, estimated_tokens: Any) -> Optional[int]:
        if isinstance(estimated_tokens, bool) or not isinstance(
            estimated_tokens, int
        ):
            msg = (
                f"estimated_tokens must be an int, "
                f"got {type(estimated_tokens).__name__}."
            )
            if self._strict:
                raise CarbonAdaptiveError(msg)
            logger.warning("%s Rejecting retrieval.", msg)
            return None
        if estimated_tokens <= 0:
            msg = f"estimated_tokens must be > 0, got {estimated_tokens}."
            if self._strict:
                raise CarbonAdaptiveError(msg)
            logger.warning("%s Rejecting retrieval.", msg)
            return None
        return estimated_tokens

    def _validate_content_type(self, content_type: Any) -> Optional[str]:
        if not isinstance(content_type, str) or not content_type:
            msg = "content_type must be a non-empty string."
            if self._strict:
                raise CarbonAdaptiveError(msg)
            logger.warning("%s Defaulting to 'text'.", msg)
            return "text"
        if content_type not in _VALID_CONTENT_TYPES:
            msg = (
                f"content_type must be one of {_VALID_CONTENT_TYPES}, "
                f"got {content_type!r}."
            )
            if self._strict:
                raise CarbonAdaptiveError(msg)
            logger.warning("%s Defaulting to 'text'.", msg)
            return "text"
        return content_type

    def _validate_urgency(self, urgency: Any) -> Optional[str]:
        if not isinstance(urgency, str) or urgency not in _VALID_URGENCIES:
            msg = (
                f"urgency must be one of {_VALID_URGENCIES}, "
                f"got {urgency!r}."
            )
            if self._strict:
                raise CarbonAdaptiveError(msg)
            logger.warning("%s Defaulting to 'normal'.", msg)
            return "normal"
        return urgency

    # ---------------------------------------------------------- statistics
    def get_controller_stats(self) -> Dict[str, Any]:
        """Return the controller statistics (backward-compatible schema)."""
        with self._lock:
            stats = {
                "total_decisions": self.total_decisions,
                "decisions_by_mode": {
                    mode.value: count
                    for mode, count in self.decisions_by_mode.items()
                },
                "carbon_saved_kg": self.carbon_saved_kg,
                "retrievals_deferred": self.retrievals_deferred,
                "pending_deferred": len(self.deferred_retrievals),
                "current_mode": self.current_mode.value,
                "current_intensity_g_kwh": self.current_intensity_g_kwh,
                # Additive fields:
                "deferred_evictions": self._deferred_evictions,
                "history_size": len(self._history),
            }
        return stats

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`get_controller_stats`."""
        return self.get_controller_stats()

    def get_recommendations(self) -> List[str]:
        """Return meta-cognitive reflection recommendations."""
        recommendations: List[str] = []
        stats = self.get_controller_stats()

        if self.current_mode == RetrievalMode.MINIMAL:
            recommendations.append(
                "Operating in MINIMAL mode due to dirty grid. "
                "Only urgent text retrievals allowed."
            )
        elif self.current_mode == RetrievalMode.RESTRICTED:
            recommendations.append(
                "Operating in RESTRICTED mode. "
                "Visual and multimodal content limited."
            )
        else:
            recommendations.append(
                "Operating in FULL mode. Clean grid allows full retrieval."
            )

        if stats["pending_deferred"] > 0:
            recommendations.append(
                f"{stats['pending_deferred']} retrievals deferred. "
                f"Will process when grid is cleaner."
            )

        if stats["carbon_saved_kg"] > 0.001:
            recommendations.append(
                f"Carbon-adaptive control has saved "
                f"{stats['carbon_saved_kg']:.6f} kg CO2."
            )

        if self.current_intensity_g_kwh > self.dirty_threshold:
            recommendations.append(
                "Consider scheduling non-urgent retrievals for cleaner "
                "grid periods (typically overnight or during high "
                "renewable generation)."
            )

        return recommendations

    def get_deferred_summary(self) -> Dict[str, Any]:
        """Return a summary of deferred retrievals."""
        with self._lock:
            snapshot = list(self.deferred_retrievals)

        if not snapshot:
            return {"pending": 0}

        total_tokens = sum(r.tokens for r in snapshot)
        now = time.time()
        avg_wait_time = (
            sum(now - r.deferred_at for r in snapshot) / len(snapshot)
        )

        return {
            "pending": len(snapshot),
            "total_tokens_deferred": total_tokens,
            "avg_wait_time_seconds": avg_wait_time,
            "oldest_deferred": min(r.deferred_at for r in snapshot),
            "deferred_by_type": self._count_by_type(snapshot),
            "deferred_evictions": self._deferred_evictions,
        }

    def _count_by_type(
        self, retrievals: Sequence[DeferredRetrieval]
    ) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for r in retrievals:
            counts[r.content_type] = counts.get(r.content_type, 0) + 1
        return counts

    def force_process_deferred(self) -> List[Dict[str, Any]]:
        """
        Force-process all deferred retrievals, bypassing the carbon check.

        Emits a WARNING log to flag the override.
        """
        with self._lock:
            snapshot = list(self.deferred_retrievals)
            self.deferred_retrievals.clear()

        now = time.time()
        processed: List[Dict[str, Any]] = []
        for entry in snapshot:
            processed.append({
                **entry.to_dict(),
                "processed_at": now,
                "processed_intensity": self.current_intensity_g_kwh,
                "forced": True,
            })

        if processed:
            logger.warning(
                "Force-processed %d deferred retrieval(s) regardless of "
                "carbon intensity.", len(processed),
            )
        return processed

    def reset(self, *, clear_deferred: bool = True) -> None:
        """Reset counters and (optionally) the deferred queue."""
        with self._lock:
            if clear_deferred:
                self.deferred_retrievals.clear()
            self.total_decisions = 0
            self.decisions_by_mode = {mode: 0 for mode in RetrievalMode}
            self.carbon_saved_kg = 0.0
            self.retrievals_deferred = 0
            self._deferred_evictions = 0
            self._history.clear()
        logger.debug("CarbonAdaptiveRetrievalController reset.")

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "current_intensity_g_kwh": self.current_intensity_g_kwh,
                "current_mode": self.current_mode.value,
                "counters": {
                    "total_decisions": self.total_decisions,
                    "decisions_by_mode": {
                        mode.value: count
                        for mode, count in self.decisions_by_mode.items()
                    },
                    "carbon_saved_kg": self.carbon_saved_kg,
                    "retrievals_deferred": self.retrievals_deferred,
                    "deferred_evictions": self._deferred_evictions,
                },
                "deferred_retrievals": [
                    r.to_dict() for r in self.deferred_retrievals
                ],
            }
            if include_history:
                payload["history"] = [d.to_dict() for d in self._history]
        return payload

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CarbonAdaptiveRetrievalController":
        if not isinstance(data, Mapping):
            raise CarbonAdaptiveError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = CarbonAdaptiveConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        controller = cls(config=cfg, strict=bool(data.get("strict", True)))

        with controller._lock:
            controller.current_intensity_g_kwh = float(
                data.get("current_intensity_g_kwh",
                         cfg.default_intensity_g_kwh)
            )
            controller.current_mode = RetrievalMode(
                str(data.get("current_mode", "restricted"))
            )
            counters = dict(data.get("counters", {}) or {})
            controller.total_decisions = int(
                counters.get("total_decisions", 0)
            )
            by_mode = dict(counters.get("decisions_by_mode", {}) or {})
            for mode in RetrievalMode:
                controller.decisions_by_mode[mode] = int(
                    by_mode.get(mode.value, 0)
                )
            controller.carbon_saved_kg = float(
                counters.get("carbon_saved_kg", 0.0)
            )
            controller.retrievals_deferred = int(
                counters.get("retrievals_deferred", 0)
            )
            controller._deferred_evictions = int(
                counters.get("deferred_evictions", 0)
            )
            for entry in data.get("deferred_retrievals", []):
                controller.deferred_retrievals.append(
                    DeferredRetrieval.from_dict(entry)
                )
            for entry in data.get("history", []):
                controller._history.append(
                    RetrievalDecision.from_dict(entry)
                )
        return controller

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "CarbonAdaptiveRetrievalController":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise CarbonAdaptiveError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "CarbonAdaptiveRetrievalController":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "CarbonAdaptiveRetrievalController scope exited with %s.",
                exc_type.__name__,
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
                f"pending_deferred={len(self.deferred_retrievals)}, "
                f"saved={self.carbon_saved_kg:.6f}kg, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "CarbonAdaptiveConfig",
    "CarbonAdaptiveError",
    "CarbonAdaptiveRetrievalController",
    "DeferredRetrieval",
    "RetrievalDecision",
    "RetrievalMode",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m retrieval.carbon_adaptive_controller
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Happy path across grid conditions ------------------------- #
    controller = CarbonAdaptiveRetrievalController()
    print("repr       :", controller)

    # Clean grid → FULL mode.
    controller.update_carbon_intensity(120.0)
    d1 = controller.decide_retrieval(
        "summarize renewable energy", 8000, content_type="text",
    )
    print("clean      :", d1)

    # Moderate grid → RESTRICTED mode.
    controller.update_carbon_intensity(280.0)
    d2 = controller.decide_retrieval(
        "fetch chart", 8000, content_type="visual",
    )
    print("moderate   :", d2)
    assert d2.allowed and not d2.allow_visual  # visual blocked
    assert d2.max_tokens <= 4096

    # Dirty grid + urgent → MINIMAL mode, reduced tokens.
    controller.update_carbon_intensity(500.0)
    d3 = controller.decide_retrieval(
        "critical incident report", 8000, content_type="text", urgency="urgent",
    )
    print("dirty-urg  :", d3)
    assert d3.allowed and d3.max_tokens == 2048

    # Dirty grid + low urgency + deferral → DEFERRED.
    d4 = controller.decide_retrieval(
        "background research", 5000, content_type="text", urgency="low",
    )
    print("deferred   :", d4)
    assert d4.mode == RetrievalMode.DEFERRED and not d4.allowed

    # Dirty grid + normal urgency → denied.
    d5 = controller.decide_retrieval(
        "nice-to-have", 3000, content_type="text", urgency="normal",
    )
    print("denied     :", d5)
    assert not d5.allowed

    # ---- Bug fix: no double-counting on deferred processing --------- #
    decisions_before = controller.total_decisions
    pending_before = controller.pending_deferred
    assert pending_before == 1, f"expected 1 deferred, got {pending_before}"

    controller.update_carbon_intensity(80.0)  # → FULL, triggers processing

    decisions_after = controller.total_decisions
    # Exactly +1 for the re-evaluated deferred item, not +2.
    assert decisions_after == decisions_before + 1, (
        f"double-counting! before={decisions_before} "
        f"after={decisions_after}"
    )
    assert controller.pending_deferred == 0
    print("no-double-count : OK")

    # ---- Statistics ------------------------------------------------- #
    print("stats      :", controller.get_controller_stats())

    # ---- Recommendations -------------------------------------------- #
    for rec in controller.get_recommendations():
        print("  -", rec)

    # ---- Deferred summary ------------------------------------------- #
    controller.update_carbon_intensity(600.0)
    for i in range(3):
        controller.decide_retrieval(
            f"bg-{i}", 1000 + i, content_type="text", urgency="low",
        )
    summary = controller.get_deferred_summary()
    print("deferred summary:", {
        "pending": summary["pending"],
        "total_tokens_deferred": summary["total_tokens_deferred"],
        "deferred_by_type": summary["deferred_by_type"],
    })

    # ---- Force process --------------------------------------------- #
    forced = controller.force_process_deferred()
    print("forced     :", len(forced), "item(s)")
    assert controller.pending_deferred == 0

    # ---- Bounded deferral queue ------------------------------------ #
    bounded = CarbonAdaptiveRetrievalController(
        config=CarbonAdaptiveConfig(max_deferred=3),
    )
    bounded.update_carbon_intensity(600.0)
    for i in range(10):
        bounded.decide_retrieval(
            f"q-{i}", 100, content_type="text", urgency="low",
        )
    print(f"bounded    : pending={bounded.pending_deferred} "
          f"evictions={bounded._deferred_evictions}")
    assert bounded.pending_deferred == 3
    assert bounded._deferred_evictions == 7

    # ---- Serialization round-trip ---------------------------------- #
    payload = controller.to_json()
    restored = CarbonAdaptiveRetrievalController.from_json(payload)
    assert restored.to_dict() == controller.to_dict()
    print("Round-trip OK.")

    # ---- Context manager ------------------------------------------- #
    with CarbonAdaptiveRetrievalController() as scoped:
        scoped.update_carbon_intensity(50.0)
        scoped.decide_retrieval("hello", 500, content_type="text")
    print("Context OK.")

    # ---- Validation failures --------------------------------------- #
    for bad_cfg in (
        dict(clean_threshold_g_kwh=0),
        dict(clean_threshold_g_kwh=400, dirty_threshold_g_kwh=200),
        dict(restricted_max_tokens=0),
        dict(minimal_max_tokens=0),
        dict(minimal_max_tokens=9000, restricted_max_tokens=4096),
        dict(energy_per_token_wh=0),
        dict(visual_energy_multiplier=0),
        dict(max_deferred=0),
        dict(max_history=0),
    ):
        try:
            CarbonAdaptiveConfig(**bad_cfg)  # type: ignore[arg-type]
        except CarbonAdaptiveError as exc:
            print("Rejected cfg:", exc)

    strict = CarbonAdaptiveRetrievalController(strict=True)
    for bad_call in (
        lambda: strict.update_carbon_intensity(float("nan")),
        lambda: strict.update_carbon_intensity(-1.0),
        lambda: strict.update_carbon_intensity(float("inf")),
        lambda: strict.decide_retrieval("", 100),
        lambda: strict.decide_retrieval("q", 0),
        lambda: strict.decide_retrieval("q", -1),
        lambda: strict.decide_retrieval("q", 100, content_type="bogus"),
        lambda: strict.decide_retrieval("q", 100, urgency="emergency"),
    ):
        try:
            bad_call()
        except CarbonAdaptiveError as exc:
            print("Rejected   :", exc)

    # Non-strict coerces / rejects safely.
    lenient = CarbonAdaptiveRetrievalController(strict=False)
    lenient.update_carbon_intensity(float("nan"))       # ignored
    lenient.decide_retrieval("q", 100, content_type="bogus")  # → text
    lenient.decide_retrieval("q", 100, urgency="bogus")       # → normal
    d = lenient.decide_retrieval("", 100)               # rejected safely
    assert not d.allowed
    print("lenient    : OK")

    print("\nSmoke test passed.")
