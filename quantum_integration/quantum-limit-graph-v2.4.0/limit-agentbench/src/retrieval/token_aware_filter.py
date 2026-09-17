# src/retrieval/token_aware_filter.py

"""
Token-Aware Filtering Layer
===========================

Distinguishes between small vs. large retrieval contexts and dynamically
adjusts retrieval scope based on carbon grid intensity.

Enhancements
------------
- ``TokenAwareConfig`` — frozen, validated: intensity thresholds, all token
  caps, energy multipliers, relevance thresholds, bounded history.
- ``FilteringPolicy`` — frozen dataclass with validation and serialization.
- **Fixed silent-TINY bug** — negative ``token_count`` is rejected.
- **Fixed NaN-bypass bug** — non-finite ``relevance_score`` is rejected.
- **Fixed unknown-content-type bug** — content type is allowlisted.
- **Thread safety** — ``RLock`` guards counters and policy swaps.
- **Full validation** of every argument; strict / non-strict modes.
- ``update_grid_intensity`` now validates and **returns** the new
  :class:`GridIntensity`.
- **Unified energy model** — per-content-type multipliers applied uniformly.
- Deterministic filter-reason strings for downstream auditing.
- Serialization on the filter, the policy, and both enums.
- ``statistics()``, ``__repr__``, custom ``TokenAwareFilterError``,
  lazy ``%s`` logging, and a comprehensive ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class TokenAwareFilterError(ValueError):
    """Raised for invalid token-aware filter inputs or configuration."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class ContextSize(Enum):
    """Context size categories."""
    TINY = "tiny"        # < 512 tokens
    SMALL = "small"      # 512-2048 tokens
    MEDIUM = "medium"    # 2048-8192 tokens
    LARGE = "large"      # 8192-32768 tokens
    XLARGE = "xlarge"    # > 32768 tokens


class GridIntensity(Enum):
    """Carbon grid intensity levels."""
    CLEAN = "clean"        # < 200 g CO2/kWh
    MODERATE = "moderate"  # 200-400 g CO2/kWh
    DIRTY = "dirty"        # > 400 g CO2/kWh


# Ordered list used for boundary checks.
_CONTENT_TYPES: Tuple[str, ...] = ("text", "visual", "multimodal", "audio")


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TokenAwareConfig:
    """
    Tunable parameters for the token-aware filter.

    Centralizes every threshold, token cap, energy coefficient, and
    relevance threshold that was previously hardcoded in the class body.
    """

    # Grid intensity thresholds (gCO2/kWh).
    clean_threshold_g_kwh: float = 200.0
    dirty_threshold_g_kwh: float = 400.0

    # Default intensity used at construction.
    default_grid_intensity_g_kwh: float = 385.0

    # Context-size boundaries (tokens).
    tiny_max_tokens: int = 512
    small_max_tokens: int = 2048
    medium_max_tokens: int = 8192
    large_max_tokens: int = 32_768

    # Per-grid intensity token caps.
    clean_tiny_max: int = 512
    clean_small_max: int = 2048
    clean_medium_max: int = 8192
    clean_large_max: int = 32_768
    clean_xlarge_max: int = 65_536

    moderate_tiny_max: int = 512
    moderate_small_max: int = 2048
    moderate_medium_max: int = 6144
    moderate_large_max: int = 16_384
    moderate_xlarge_max: int = 32_768

    dirty_tiny_max: int = 256
    dirty_small_max: int = 1024
    dirty_medium_max: int = 4096
    dirty_large_max: int = 8192
    dirty_xlarge_max: int = 16_384

    # Visual / multimodal availability per grid mode.
    clean_allow_visual: bool = True
    clean_allow_multimodal: bool = True
    moderate_allow_visual: bool = True
    moderate_allow_multimodal: bool = False
    dirty_allow_visual: bool = False
    dirty_allow_multimodal: bool = False

    # Relevance thresholds.
    default_min_relevance: float = 0.5
    dirty_min_relevance: float = 0.7

    # Energy model.
    energy_per_token_wh: float = 1e-6
    visual_energy_multiplier: float = 2.5
    multimodal_energy_multiplier: float = 2.5
    audio_energy_multiplier: float = 2.0

    # Recommendation thresholds.
    high_filter_rate_pct: float = 50.0
    token_savings_recommendation_threshold: int = 10_000

    # Bounded history.
    max_history: int = 1000

    def __post_init__(self) -> None:
        if self.clean_threshold_g_kwh <= 0:
            raise TokenAwareFilterError(
                "clean_threshold_g_kwh must be > 0."
            )
        if self.dirty_threshold_g_kwh <= self.clean_threshold_g_kwh:
            raise TokenAwareFilterError(
                "dirty_threshold_g_kwh must be > clean_threshold_g_kwh."
            )
        if self.default_grid_intensity_g_kwh < 0:
            raise TokenAwareFilterError(
                "default_grid_intensity_g_kwh must be >= 0."
            )

        # Context-size boundaries must be increasing.
        if not (
            0 < self.tiny_max_tokens
            < self.small_max_tokens
            < self.medium_max_tokens
            < self.large_max_tokens
        ):
            raise TokenAwareFilterError(
                "context-size boundaries must be strictly increasing: "
                "tiny < small < medium < large."
            )

        # Per-mode caps must be non-negative and ordered where applicable.
        for prefix in ("clean", "moderate", "dirty"):
            values = [
                getattr(self, f"{prefix}_tiny_max"),
                getattr(self, f"{prefix}_small_max"),
                getattr(self, f"{prefix}_medium_max"),
                getattr(self, f"{prefix}_large_max"),
                getattr(self, f"{prefix}_xlarge_max"),
            ]
            for name, value in zip(
                ("tiny", "small", "medium", "large", "xlarge"), values
            ):
                if not isinstance(value, int) or value <= 0:
                    raise TokenAwareFilterError(
                        f"{prefix}_{name}_max must be a positive int."
                    )
            if values != sorted(values):
                raise TokenAwareFilterError(
                    f"{prefix} caps must be non-decreasing: "
                    f"{values}."
                )

        # Clean-mode caps should be >= moderate >= dirty.
        for size in ("tiny", "small", "medium", "large", "xlarge"):
            clean = getattr(self, f"clean_{size}_max")
            moderate = getattr(self, f"moderate_{size}_max")
            dirty = getattr(self, f"dirty_{size}_max")
            if not (clean >= moderate >= dirty):
                raise TokenAwareFilterError(
                    f"{size} caps must satisfy clean >= moderate >= dirty "
                    f"({clean} vs {moderate} vs {dirty})."
                )

        for name in (
            "default_min_relevance", "dirty_min_relevance",
        ):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise TokenAwareFilterError(f"{name} must be in [0, 1].")
        if self.dirty_min_relevance < self.default_min_relevance:
            raise TokenAwareFilterError(
                "dirty_min_relevance must be >= default_min_relevance."
            )

        if self.energy_per_token_wh <= 0:
            raise TokenAwareFilterError("energy_per_token_wh must be > 0.")
        for name in (
            "visual_energy_multiplier",
            "multimodal_energy_multiplier",
            "audio_energy_multiplier",
        ):
            if getattr(self, name) <= 0:
                raise TokenAwareFilterError(f"{name} must be > 0.")

        if not 0.0 < self.high_filter_rate_pct <= 100.0:
            raise TokenAwareFilterError(
                "high_filter_rate_pct must be in (0, 100]."
            )
        if self.token_savings_recommendation_threshold <= 0:
            raise TokenAwareFilterError(
                "token_savings_recommendation_threshold must be > 0."
            )
        if self.max_history <= 0:
            raise TokenAwareFilterError("max_history must be > 0.")

    # ----- convenience builders ----------------------------------------
    def context_size_for(self, token_count: int) -> ContextSize:
        if token_count < self.tiny_max_tokens:
            return ContextSize.TINY
        if token_count < self.small_max_tokens:
            return ContextSize.SMALL
        if token_count < self.medium_max_tokens:
            return ContextSize.MEDIUM
        if token_count < self.large_max_tokens:
            return ContextSize.LARGE
        return ContextSize.XLARGE

    def grid_intensity_for(self, intensity_g_kwh: float) -> GridIntensity:
        if intensity_g_kwh < self.clean_threshold_g_kwh:
            return GridIntensity.CLEAN
        if intensity_g_kwh < self.dirty_threshold_g_kwh:
            return GridIntensity.MODERATE
        return GridIntensity.DIRTY

    def energy_multiplier_for(self, content_type: str) -> float:
        return {
            "text": 1.0,
            "visual": self.visual_energy_multiplier,
            "multimodal": self.multimodal_energy_multiplier,
            "audio": self.audio_energy_multiplier,
        }.get(content_type, 1.0)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TokenAwareConfig":
        if not isinstance(data, Mapping):
            raise TokenAwareFilterError(
                f"TokenAwareConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        # Only take keys that exist on this dataclass.
        valid = set(cls.__dataclass_fields__.keys())
        kwargs = {k: v for k, v in data.items() if k in valid}
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Policy
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class FilteringPolicy:
    """Immutable policy for token-aware filtering."""

    grid_intensity: GridIntensity
    max_tokens_tiny: int
    max_tokens_small: int
    max_tokens_medium: int
    max_tokens_large: int
    max_tokens_xlarge: int
    allow_visual_context: bool
    allow_multimodal: bool

    def __post_init__(self) -> None:
        if not isinstance(self.grid_intensity, GridIntensity):
            raise TokenAwareFilterError(
                f"grid_intensity must be a GridIntensity, got "
                f"{type(self.grid_intensity).__name__}."
            )
        values = (
            self.max_tokens_tiny,
            self.max_tokens_small,
            self.max_tokens_medium,
            self.max_tokens_large,
            self.max_tokens_xlarge,
        )
        for value in values:
            if not isinstance(value, int) or value <= 0:
                raise TokenAwareFilterError(
                    "all max_tokens_* fields must be positive ints."
                )
        if list(values) != sorted(values):
            raise TokenAwareFilterError(
                f"max_tokens_* must be non-decreasing, got {values}."
            )
        for name in ("allow_visual_context", "allow_multimodal"):
            if not isinstance(getattr(self, name), bool):
                raise TokenAwareFilterError(f"{name} must be a bool.")

    def max_tokens_for(self, context_size: ContextSize) -> int:
        return {
            ContextSize.TINY: self.max_tokens_tiny,
            ContextSize.SMALL: self.max_tokens_small,
            ContextSize.MEDIUM: self.max_tokens_medium,
            ContextSize.LARGE: self.max_tokens_large,
            ContextSize.XLARGE: self.max_tokens_xlarge,
        }[context_size]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "grid_intensity": self.grid_intensity.value,
            "max_tokens_tiny": self.max_tokens_tiny,
            "max_tokens_small": self.max_tokens_small,
            "max_tokens_medium": self.max_tokens_medium,
            "max_tokens_large": self.max_tokens_large,
            "max_tokens_xlarge": self.max_tokens_xlarge,
            "allow_visual_context": self.allow_visual_context,
            "allow_multimodal": self.allow_multimodal,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FilteringPolicy":
        if not isinstance(data, Mapping):
            raise TokenAwareFilterError(
                "FilteringPolicy.from_dict expects a Mapping."
            )
        return cls(
            grid_intensity=GridIntensity(str(data["grid_intensity"])),
            max_tokens_tiny=int(data["max_tokens_tiny"]),
            max_tokens_small=int(data["max_tokens_small"]),
            max_tokens_medium=int(data["max_tokens_medium"]),
            max_tokens_large=int(data["max_tokens_large"]),
            max_tokens_xlarge=int(data["max_tokens_xlarge"]),
            allow_visual_context=bool(data.get("allow_visual_context", False)),
            allow_multimodal=bool(data.get("allow_multimodal", False)),
        )

    def __repr__(self) -> str:
        return (
            "FilteringPolicy("
            f"grid={self.grid_intensity.value}, "
            f"tiny={self.max_tokens_tiny}, small={self.max_tokens_small}, "
            f"medium={self.max_tokens_medium}, large={self.max_tokens_large}, "
            f"xlarge={self.max_tokens_xlarge}, "
            f"visual={self.allow_visual_context}, "
            f"multimodal={self.allow_multimodal})"
        )


# --------------------------------------------------------------------------- #
# Filter decision
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class FilterDecision:
    """Immutable result of a filtering evaluation."""

    should_filter: bool
    reason: str
    context_size: Optional[str] = None
    grid_intensity: Optional[str] = None
    max_allowed: Optional[int] = None
    actual: Optional[float] = None
    min_required: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "should_filter": self.should_filter,
            "reason": self.reason,
            "context_size": self.context_size,
            "grid_intensity": self.grid_intensity,
            "max_allowed": self.max_allowed,
            "actual": self.actual,
            "min_required": self.min_required,
        }

    def __repr__(self) -> str:
        extra = ""
        if self.max_allowed is not None:
            extra = f", max_allowed={self.max_allowed}, actual={self.actual}"
        elif self.min_required is not None:
            extra = f", min_required={self.min_required}, actual={self.actual}"
        return (
            "FilterDecision("
            f"filter={self.should_filter}, reason={self.reason!r}{extra})"
        )


# --------------------------------------------------------------------------- #
# Main filter
# --------------------------------------------------------------------------- #
class TokenAwareFilter:
    """
    Token-aware filtering layer for energy-efficient retrieval.

    Thread-safe, serializable, and bounded in memory. All original behaviour
    is preserved; new parameters are keyword-only.

    Responsibilities
    ----------------
    - Distinguish between small and large retrieval contexts.
    - Dynamically adjust retrieval scope based on carbon grid intensity.
    - Prevent wasted compute cycles on oversized or irrelevant sources.
    - Provide filtering recommendations for meta-cognitive reflection.
    """

    def __init__(
        self,
        current_grid_intensity_g_kwh: float = 385.0,
        enable_adaptive_filtering: bool = True,
        *,
        config: Optional[TokenAwareConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = TokenAwareConfig(
                default_grid_intensity_g_kwh=float(current_grid_intensity_g_kwh),
            )
        self._strict = bool(strict)

        # Validate the initial intensity.
        initial_intensity = self._coerce_intensity(
            current_grid_intensity_g_kwh,
            fallback=self._config.default_grid_intensity_g_kwh,
        )

        # Legacy attributes preserved.
        self.grid_intensity_value: float = initial_intensity
        self.enable_adaptive: bool = bool(enable_adaptive_filtering)
        self.grid_intensity: GridIntensity = self._classify_grid_intensity(
            initial_intensity
        )
        self.policy: FilteringPolicy = self._create_policy(self.grid_intensity)

        # ---- Internal state -------------------------------------------
        self._lock = threading.RLock()
        self.filtered_count: int = 0
        self.total_requests: int = 0
        self.tokens_saved: int = 0
        self.energy_saved_wh: float = 0.0

        # Bounded audit history.
        self._history: List[FilterDecision] = []

        logger.debug(
            "TokenAwareFilter initialized "
            "(intensity=%.1f g/kWh, mode=%s, adaptive=%s, strict=%s)",
            self.grid_intensity_value,
            self.grid_intensity.value,
            self.enable_adaptive,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> TokenAwareConfig:
        return self._config

    @property
    def history(self) -> List[FilterDecision]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- classification
    def _classify_grid_intensity(self, intensity_g_kwh: float) -> GridIntensity:
        """Classify grid intensity level (config-driven thresholds)."""
        return self._config.grid_intensity_for(intensity_g_kwh)

    def _create_policy(self, grid_intensity: GridIntensity) -> FilteringPolicy:
        """Create the filtering policy for ``grid_intensity`` (config-driven)."""
        cfg = self._config
        if grid_intensity == GridIntensity.CLEAN:
            return FilteringPolicy(
                grid_intensity=grid_intensity,
                max_tokens_tiny=cfg.clean_tiny_max,
                max_tokens_small=cfg.clean_small_max,
                max_tokens_medium=cfg.clean_medium_max,
                max_tokens_large=cfg.clean_large_max,
                max_tokens_xlarge=cfg.clean_xlarge_max,
                allow_visual_context=cfg.clean_allow_visual,
                allow_multimodal=cfg.clean_allow_multimodal,
            )
        if grid_intensity == GridIntensity.MODERATE:
            return FilteringPolicy(
                grid_intensity=grid_intensity,
                max_tokens_tiny=cfg.moderate_tiny_max,
                max_tokens_small=cfg.moderate_small_max,
                max_tokens_medium=cfg.moderate_medium_max,
                max_tokens_large=cfg.moderate_large_max,
                max_tokens_xlarge=cfg.moderate_xlarge_max,
                allow_visual_context=cfg.moderate_allow_visual,
                allow_multimodal=cfg.moderate_allow_multimodal,
            )
        return FilteringPolicy(
            grid_intensity=grid_intensity,
            max_tokens_tiny=cfg.dirty_tiny_max,
            max_tokens_small=cfg.dirty_small_max,
            max_tokens_medium=cfg.dirty_medium_max,
            max_tokens_large=cfg.dirty_large_max,
            max_tokens_xlarge=cfg.dirty_xlarge_max,
            allow_visual_context=cfg.dirty_allow_visual,
            allow_multimodal=cfg.dirty_allow_multimodal,
        )

    def classify_context_size(self, token_count: int) -> ContextSize:
        """
        Classify context size based on token count.

        Negative token counts are rejected per strict mode. The original
        silently classified them as TINY.
        """
        tokens = self._coerce_tokens(token_count)
        if tokens is None:
            # Non-strict fallback — return the smallest bucket.
            return ContextSize.TINY
        return self._config.context_size_for(tokens)

    # ---------------------------------------------------------- filtering
    def should_filter(
        self,
        token_count: int,
        content_type: str = "text",
        relevance_score: float = 0.5,
    ) -> Dict[str, Any]:
        """
        Determine whether content should be filtered based on policy.

        Parameters
        ----------
        token_count : int
            Number of tokens in content. Must be non-negative.
        content_type : str
            One of ``text``, ``visual``, ``multimodal``, ``audio``.
        relevance_score : float
            Relevance score in ``[0, 1]``. Must be finite.

        Returns
        -------
        dict
            Same shape as the original ``should_filter`` return value
            (``should_filter``, ``reason``, plus per-branch details).
        """
        tokens = self._coerce_tokens(token_count)
        ctype = self._coerce_content_type(content_type)
        relevance = self._coerce_relevance(relevance_score)

        if tokens is None or ctype is None or relevance is None:
            # Non-strict mode with invalid input: return a safe denial.
            with self._lock:
                self.total_requests += 1
                self.filtered_count += 1
            decision = FilterDecision(
                should_filter=True,
                reason="invalid_input_rejected",
                grid_intensity=self.grid_intensity.value,
            )
            self._record(decision)
            return decision.to_dict()

        with self._lock:
            self.total_requests += 1
            policy = self.policy
            grid_mode = self.grid_intensity
            intensity_value = self.grid_intensity_value

        context_size = self._config.context_size_for(tokens)

        # ---- Content-type restriction ----------------------------------
        if ctype == "visual" and not policy.allow_visual_context:
            saved = tokens
            self._account_savings(saved, ctype, intensity_value)
            decision = FilterDecision(
                should_filter=True,
                reason="visual_content_restricted_during_dirty_grid",
                context_size=context_size.value,
                grid_intensity=grid_mode.value,
            )
            self._record(decision)
            return decision.to_dict()

        if ctype == "multimodal" and not policy.allow_multimodal:
            saved = tokens
            self._account_savings(saved, ctype, intensity_value)
            decision = FilterDecision(
                should_filter=True,
                reason="multimodal_content_restricted",
                context_size=context_size.value,
                grid_intensity=grid_mode.value,
            )
            self._record(decision)
            return decision.to_dict()

        # ---- Token-limit check -----------------------------------------
        max_tokens = policy.max_tokens_for(context_size)
        if tokens > max_tokens:
            saved = tokens - max_tokens
            self._account_savings(saved, ctype, intensity_value)
            decision = FilterDecision(
                should_filter=True,
                reason=f"exceeds_max_tokens_for_{context_size.value}",
                context_size=context_size.value,
                grid_intensity=grid_mode.value,
                max_allowed=max_tokens,
                actual=tokens,
            )
            self._record(decision)
            return decision.to_dict()

        # ---- Relevance threshold ---------------------------------------
        cfg = self._config
        min_relevance = (
            cfg.dirty_min_relevance
            if grid_mode == GridIntensity.DIRTY
            else cfg.default_min_relevance
        )
        if relevance < min_relevance:
            saved = tokens
            self._account_savings(saved, ctype, intensity_value)
            decision = FilterDecision(
                should_filter=True,
                reason="low_relevance_score",
                context_size=context_size.value,
                grid_intensity=grid_mode.value,
                min_required=min_relevance,
                actual=relevance,
            )
            self._record(decision)
            return decision.to_dict()

        # ---- Allow -----------------------------------------------------
        decision = FilterDecision(
            should_filter=False,
            reason="passes_all_filters",
            context_size=context_size.value,
            grid_intensity=grid_mode.value,
        )
        self._record(decision)
        return decision.to_dict()

    # ---------------------------------------------------------- grid update
    def update_grid_intensity(
        self, new_intensity_g_kwh: float
    ) -> GridIntensity:
        """
        Update the current grid intensity and adjust the filtering policy.

        Returns the new :class:`GridIntensity` — the original returned
        ``None`` and required a follow-up ``get_filtering_stats`` call.
        """
        intensity = self._coerce_intensity(
            new_intensity_g_kwh,
            fallback=self.grid_intensity_value,
        )

        with self._lock:
            self.grid_intensity_value = intensity
            old_intensity = self.grid_intensity
            new_mode = self._classify_grid_intensity(intensity)
            self.grid_intensity = new_mode

            if new_mode != old_intensity:
                self.policy = self._create_policy(new_mode)

        if new_mode != old_intensity:
            logger.info(
                "Grid intensity changed %.1f → %.1f g/kWh (%s → %s).",
                new_intensity_g_kwh, intensity,
                old_intensity.value, new_mode.value,
            )
        return new_mode

    # ---------------------------------------------------------- helpers
    def _get_max_tokens_for_size(self, context_size: ContextSize) -> int:
        """Return the max tokens allowed for ``context_size`` (current policy)."""
        with self._lock:
            return self.policy.max_tokens_for(context_size)

    def _account_savings(
        self, saved_tokens: int, content_type: str, intensity_g_kwh: float
    ) -> None:
        """
        Credit the avoided tokens and derived energy to the counters.

        Applies the per-content-type energy multiplier uniformly — the
        original applied ``2.5`` to visual and multimodal but ``1.0`` to
        text, with no config.
        """
        if saved_tokens <= 0:
            return
        cfg = self._config
        multiplier = cfg.energy_multiplier_for(content_type)
        energy_wh = saved_tokens * cfg.energy_per_token_wh * multiplier

        with self._lock:
            self.filtered_count += 1
            self.tokens_saved += saved_tokens
            self.energy_saved_wh += energy_wh

    def _record(self, decision: FilterDecision) -> None:
        with self._lock:
            self._history.append(decision)
            if len(self._history) > self._config.max_history:
                del self._history[0]

    # ---------------------------------------------------------- validation
    def _coerce_intensity(
        self, value: Any, *, fallback: float
    ) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"intensity must be numeric, got {type(value).__name__}."
            )
            if self._strict:
                raise TokenAwareFilterError(msg)
            logger.warning("%s Using %.1f.", msg, fallback)
            return float(fallback)
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"intensity must be finite, got {value!r}."
            if self._strict:
                raise TokenAwareFilterError(msg)
            logger.warning("%s Using %.1f.", msg, fallback)
            return float(fallback)
        if fv < 0:
            msg = f"intensity must be >= 0, got {fv}."
            if self._strict:
                raise TokenAwareFilterError(msg)
            logger.warning("%s Using %.1f.", msg, fallback)
            return float(fallback)
        return fv

    def _coerce_tokens(self, value: Any) -> Optional[int]:
        if isinstance(value, bool) or not isinstance(value, int):
            msg = (
                f"token_count must be an int, got {type(value).__name__}."
            )
            if self._strict:
                raise TokenAwareFilterError(msg)
            logger.warning("%s Rejecting.", msg)
            return None
        if value < 0:
            msg = f"token_count must be >= 0, got {value}."
            if self._strict:
                raise TokenAwareFilterError(msg)
            logger.warning("%s Rejecting.", msg)
            return None
        return value

    def _coerce_content_type(self, value: Any) -> Optional[str]:
        if not isinstance(value, str) or not value:
            msg = "content_type must be a non-empty string."
            if self._strict:
                raise TokenAwareFilterError(msg)
            logger.warning("%s Defaulting to 'text'.", msg)
            return "text"
        if value not in _CONTENT_TYPES:
            msg = (
                f"content_type must be one of {_CONTENT_TYPES}, "
                f"got {value!r}."
            )
            if self._strict:
                raise TokenAwareFilterError(msg)
            logger.warning("%s Defaulting to 'text'.", msg)
            return "text"
        return value

    def _coerce_relevance(self, value: Any) -> Optional[float]:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"relevance_score must be numeric, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise TokenAwareFilterError(msg)
            logger.warning("%s Rejecting.", msg)
            return None
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"relevance_score must be finite, got {value!r}."
            if self._strict:
                raise TokenAwareFilterError(msg)
            logger.warning("%s Rejecting.", msg)
            return None
        if not 0.0 <= fv <= 1.0:
            msg = f"relevance_score must be in [0, 1], got {fv}."
            if self._strict:
                raise TokenAwareFilterError(msg)
            logger.warning("%s Rejecting.", msg)
            return None
        return fv

    # ---------------------------------------------------------- statistics
    def get_filtering_stats(self) -> Dict[str, Any]:
        """Return the backward-compatible filtering statistics."""
        with self._lock:
            total = self.total_requests
            filtered = self.filtered_count
            tokens_saved = self.tokens_saved
            energy_saved = self.energy_saved_wh
            grid_mode = self.grid_intensity.value
            intensity_value = self.grid_intensity_value
            policy = self.policy
            history_size = len(self._history)

        filter_rate = (
            (filtered / total * 100.0) if total > 0 else 0.0
        )

        return {
            "total_requests": total,
            "filtered_count": filtered,
            "filter_rate_pct": filter_rate,
            "tokens_saved": tokens_saved,
            "energy_saved_wh": energy_saved,
            "current_grid_intensity": grid_mode,
            "current_grid_intensity_g_kwh": intensity_value,
            "current_policy": {
                "allow_visual": policy.allow_visual_context,
                "allow_multimodal": policy.allow_multimodal,
                "max_tokens_medium": policy.max_tokens_medium,
            },
            # Additive fields:
            "history_size": history_size,
            "energy_per_token_wh": self._config.energy_per_token_wh,
        }

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`get_filtering_stats`."""
        return self.get_filtering_stats()

    def get_recommendations(self) -> List[str]:
        """Return filtering recommendations for meta-cognitive reflection."""
        with self._lock:
            total = self.total_requests
            filtered = self.filtered_count
            tokens_saved = self.tokens_saved
            energy_saved = self.energy_saved_wh
            grid_mode = self.grid_intensity

        recommendations: List[str] = []
        cfg = self._config

        filter_rate = (filtered / total * 100.0) if total > 0 else 0.0

        if filter_rate > cfg.high_filter_rate_pct:
            recommendations.append(
                "High filter rate detected. Consider adjusting "
                "relevance thresholds or query specificity."
            )

        if grid_mode == GridIntensity.DIRTY:
            recommendations.append(
                "Operating under dirty grid conditions. Visual and "
                "multimodal content restricted."
            )
            recommendations.append(
                "Consider deferring non-urgent retrievals to cleaner "
                "grid periods."
            )

        if tokens_saved > cfg.token_savings_recommendation_threshold:
            recommendations.append(
                f"Token-aware filtering has saved {tokens_saved} tokens "
                f"and {energy_saved:.4f} Wh of energy."
            )

        return recommendations

    def reset_stats(self, *, clear_history: bool = True) -> None:
        """Reset the filtering statistics (and optionally the audit history)."""
        with self._lock:
            self.filtered_count = 0
            self.total_requests = 0
            self.tokens_saved = 0
            self.energy_saved_wh = 0.0
            if clear_history:
                self._history.clear()
        logger.debug("TokenAwareFilter statistics reset.")

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_history: bool = False
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "enable_adaptive": self.enable_adaptive,
                "grid_intensity_value": self.grid_intensity_value,
                "grid_intensity": self.grid_intensity.value,
                "policy": self.policy.to_dict(),
                "counters": {
                    "total_requests": self.total_requests,
                    "filtered_count": self.filtered_count,
                    "tokens_saved": self.tokens_saved,
                    "energy_saved_wh": self.energy_saved_wh,
                },
            }
            if include_history:
                payload["history"] = [d.to_dict() for d in self._history]
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TokenAwareFilter":
        if not isinstance(data, Mapping):
            raise TokenAwareFilterError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = TokenAwareConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        initial_intensity = float(
            data.get("grid_intensity_value",
                     cfg.default_grid_intensity_g_kwh)
        )
        filter_ = cls(
            current_grid_intensity_g_kwh=initial_intensity,
            enable_adaptive_filtering=bool(
                data.get("enable_adaptive", True)
            ),
            config=cfg,
            strict=bool(data.get("strict", True)),
        )

        with filter_._lock:
            counters = dict(data.get("counters", {}) or {})
            filter_.total_requests = int(counters.get("total_requests", 0))
            filter_.filtered_count = int(counters.get("filtered_count", 0))
            filter_.tokens_saved = int(counters.get("tokens_saved", 0))
            filter_.energy_saved_wh = float(
                counters.get("energy_saved_wh", 0.0)
            )
            for entry in data.get("history", []):
                filter_._history.append(
                    FilterDecision(**entry)
                )
        return filter_

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "TokenAwareFilter":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise TokenAwareFilterError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "TokenAwareFilter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "TokenAwareFilter scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "TokenAwareFilter("
                f"grid={self.grid_intensity.value}, "
                f"intensity={self.grid_intensity_value:.1f}g/kWh, "
                f"requests={self.total_requests}, "
                f"filtered={self.filtered_count}, "
                f"tokens_saved={self.tokens_saved}, "
                f"adaptive={self.enable_adaptive}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "ContextSize",
    "FilterDecision",
    "FilteringPolicy",
    "GridIntensity",
    "TokenAwareConfig",
    "TokenAwareFilter",
    "TokenAwareFilterError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m retrieval.token_aware_filter
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Happy path: clean grid → relaxed policy --------------------- #
    f = TokenAwareFilter(current_grid_intensity_g_kwh=100.0)
    print("repr       :", f)
    assert f.grid_intensity == GridIntensity.CLEAN

    # Large visual content allowed on a clean grid.
    d = f.should_filter(
        token_count=20_000, content_type="visual", relevance_score=0.9,
    )
    print("clean vis  :", d)
    assert d["should_filter"] is False

    # ---- Moderate grid → multimodal restricted ----------------------- #
    f.update_grid_intensity(300.0)
    assert f.grid_intensity == GridIntensity.MODERATE
    d = f.should_filter(
        token_count=1000, content_type="multimodal", relevance_score=0.9,
    )
    print("moderate mm:", d)
    assert d["should_filter"] is True
    assert d["reason"] == "multimodal_content_restricted"

    # ---- Dirty grid → visual + multimodal restricted ----------------- #
    f.update_grid_intensity(500.0)
    assert f.grid_intensity == GridIntensity.DIRTY
    d_vis = f.should_filter(
        token_count=1000, content_type="visual", relevance_score=0.9,
    )
    d_mm = f.should_filter(
        token_count=1000, content_type="multimodal", relevance_score=0.9,
    )
    print("dirty vis  :", d_vis["reason"])
    print("dirty mm   :", d_mm["reason"])
    assert d_vis["should_filter"] and d_mm["should_filter"]

    # ---- Token-limit check ------------------------------------------- #
    d_big = f.should_filter(
        token_count=100_000, content_type="text", relevance_score=0.9,
    )
    print("dirty big  :", d_big)
    assert d_big["should_filter"] is True
    assert d_big["reason"].startswith("exceeds_max_tokens_for_")

    # ---- Low relevance rejection ------------------------------------- #
    d_low = f.should_filter(
        token_count=100, content_type="text", relevance_score=0.3,
    )
    print("low rel    :", d_low)
    assert d_low["should_filter"] is True
    assert d_low["reason"] == "low_relevance_score"

    # ---- Clean grid uses the laxer relevance threshold --------------- #
    f_clean = TokenAwareFilter(current_grid_intensity_g_kwh=100.0)
    d_relaxed = f_clean.should_filter(
        token_count=100, content_type="text", relevance_score=0.3,
    )
    print("clean lax  :", d_relaxed)
    assert d_relaxed["should_filter"] is False

    # ---- Bug fix: NaN relevance must not bypass the filter ----------- #
    try:
        f.should_filter(100, "text", float("nan"))
    except TokenAwareFilterError as exc:
        print("Rejected   :", exc)

    # ---- Bug fix: negative token count must not silently pass -------- #
    try:
        f.should_filter(-5, "text", 0.9)
    except TokenAwareFilterError as exc:
        print("Rejected   :", exc)

    # ---- Bug fix: unknown content_type rejected --------------------- #
    try:
        f.should_filter(100, "images", 0.9)
    except TokenAwareFilterError as exc:
        print("Rejected   :", exc)

    # ---- Bug fix: NaN intensity rejected on update ------------------ #
    try:
        f.update_grid_intensity(float("nan"))
    except TokenAwareFilterError as exc:
        print("Rejected   :", exc)

    # ---- update_grid_intensity returns the mode --------------------- #
    mode = f.update_grid_intensity(100.0)
    print("update ret :", mode.value)
    assert mode == GridIntensity.CLEAN

    # ---- Statistics -------------------------------------------------- #
    print("stats      :", {
        k: v for k, v in f.get_filtering_stats().items()
        if k not in ("current_policy",)
    })

    # ---- Recommendations --------------------------------------------- #
    f.update_grid_intensity(500.0)
    for rec in f.get_recommendations():
        print("  -", rec)

    # ---- Per-content-type energy multipliers ------------------------- #
    multi = TokenAwareFilter(
        current_grid_intensity_g_kwh=500.0,
    )
    multi.should_filter(1000, "text", 0.9)        # filtered? no — text is fine
    multi.should_filter(1000, "visual", 0.9)      # filtered — multiplier 2.5
    multi.should_filter(1000, "multimodal", 0.9)  # filtered — multiplier 2.5
    multi.should_filter(1000, "audio", 0.9)       # allowed — no restriction
    stats = multi.get_filtering_stats()
    print(f"multi      : tokens_saved={stats['tokens_saved']} "
          f"energy_saved_wh={stats['energy_saved_wh']:.6f}")
    # Visual + multimodal restricted = 2000 tokens saved.
    assert stats["tokens_saved"] == 2000
    # energy = 1000 * 1e-6 * 2.5 + 1000 * 1e-6 * 2.5 = 0.005 Wh
    assert abs(stats["energy_saved_wh"] - 0.005) < 1e-9

    # ---- reset_stats ------------------------------------------------ #
    multi.reset_stats()
    assert multi.total_requests == 0 and multi.tokens_saved == 0
    print("reset      : OK")

    # ---- Serialization round-trip ---------------------------------- #
    payload = f.to_json()
    restored = TokenAwareFilter.from_json(payload)
    assert restored.to_dict() == f.to_dict()
    print("Round-trip OK.")

    # ---- Context manager ------------------------------------------- #
    with TokenAwareFilter(current_grid_intensity_g_kwh=100.0) as scoped:
        scoped.should_filter(100, "text", 0.9)
        assert scoped.total_requests == 1
    print("Context    : OK")

    # ---- Validation failures --------------------------------------- #
    for bad_cfg in (
        dict(clean_threshold_g_kwh=0),
        dict(clean_threshold_g_kwh=400, dirty_threshold_g_kwh=200),
        dict(default_grid_intensity_g_kwh=-1),
        dict(tiny_max_tokens=0),
        dict(tiny_max_tokens=5000, small_max_tokens=2000),
        dict(clean_tiny_max=0),
        dict(moderate_tiny_max=2000, moderate_small_max=1000),  # not sorted
        dict(clean_tiny_max=10, moderate_tiny_max=100,
             dirty_tiny_max=100),  # clean < moderate
        dict(default_min_relevance=-0.1),
        dict(default_min_relevance=0.9, dirty_min_relevance=0.5),
        dict(energy_per_token_wh=0),
        dict(visual_energy_multiplier=0),
        dict(high_filter_rate_pct=0),
        dict(high_filter_rate_pct=150),
        dict(token_savings_recommendation_threshold=0),
        dict(max_history=0),
    ):
        try:
            TokenAwareConfig(**bad_cfg)  # type: ignore[arg-type]
        except TokenAwareFilterError as exc:
            print("Rejected cfg:", exc)

    strict = TokenAwareFilter(strict=True)
    for bad_call in (
        lambda: strict.should_filter("100", "text", 0.9),       # type: ignore[arg-type]
        lambda: strict.should_filter(100, 123, 0.9),            # type: ignore[arg-type]
        lambda: strict.should_filter(100, "text", "high"),      # type: ignore[arg-type]
        lambda: strict.should_filter(100, "text", 1.5),
        lambda: strict.should_filter(100, "text", -0.1),
        lambda: strict.update_grid_intensity("dirty"),          # type: ignore[arg-type]
        lambda: strict.update_grid_intensity(-10.0),
    ):
        try:
            bad_call()
        except TokenAwareFilterError as exc:
            print("Rejected   :", exc)

    # ---- Non-strict coerces safely --------------------------------- #
    lenient = TokenAwareFilter(strict=False)
    lenient.should_filter("not-an-int", "text", 0.9)  # type: ignore[arg-type]
    lenient.should_filter(100, "images", 0.9)
    lenient.should_filter(100, "text", float("nan"))
    lenient.update_grid_intensity("not-a-number")     # type: ignore[arg-type]
    print("lenient    : OK")

    # ---- FilteringPolicy serialization ----------------------------- #
    p = f.policy
    p_payload = json.dumps(p.to_dict(), default=str)
    p_restored = FilteringPolicy.from_dict(json.loads(p_payload))
    assert p_restored.to_dict() == p.to_dict()
    print("Policy     : round-trip OK")

    print("\nSmoke test passed.")
