# src/retrieval/token_aware_filter.py

"""
Token-Aware Filtering Layer
===========================

Distinguishes between small vs. large retrieval contexts and dynamically
adjusts retrieval scope based on carbon grid intensity.

Design
------
- Frozen, validated ``TokenAwareConfig`` with cross-field invariants.
- ``FilteringPolicy`` — frozen dataclass with validation and
  serialization.
- ``FilterDecision`` — frozen dataclass with validation on every field
  (``should_filter`` must be bool, ``reason`` non-empty, ``actual`` /
  ``min_required`` finite and non-negative, enum-like string fields
  allowlisted).
- **Bounded audit history** — ``deque(maxlen=max_history)``; a
  payload that exceeds the cap is truncated to its newest
  ``max_history`` entries on ``from_dict``.
- **Non-finite inputs rejected** — ``NaN`` relevance, ``Inf``
  intensity, negative token counts, unknown content types, and
  out-of-range relevance all raise ``TokenAwareFilterError`` in
  strict mode; non-strict mode silently degrades to documented
  fallbacks.
- **Round-trip serialization** — ``to_dict(include_history=...)`` /
  ``from_dict`` / ``to_json(allow_nan=False)``. The payload's
  ``grid_intensity`` field is cross-checked against the recomputed
  value; a mismatch logs a warning and uses the recomputed value.
- **Validation wrapping** — every ``from_dict`` wraps failures in
  ``TokenAwareFilterError``; a corrupt payload never surfaces a raw
  ``TypeError`` / ``KeyError``.

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
from typing import Any, Deque, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "ContextSize",
    "FilterDecision",
    "FilteringPolicy",
    "GridIntensity",
    "TokenAwareConfig",
    "TokenAwareFilter",
    "TokenAwareFilterError",
]

VERSION = "2.0.0"


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
    TINY = "tiny"
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"
    XLARGE = "xlarge"


class GridIntensity(Enum):
    """Carbon grid intensity levels."""
    CLEAN = "clean"
    MODERATE = "moderate"
    DIRTY = "dirty"


_CONTENT_TYPES: Tuple[str, ...] = (
    "text", "visual", "multimodal", "audio",
)
_CONTEXT_SIZES: Tuple[str, ...] = tuple(cs.value for cs in ContextSize)
_GRID_INTENSITIES: Tuple[str, ...] = tuple(
    gi.value for gi in GridIntensity
)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TokenAwareConfig:
    """Tunable parameters for the token-aware filter."""

    clean_threshold_g_kwh: float = 200.0
    dirty_threshold_g_kwh: float = 400.0
    default_grid_intensity_g_kwh: float = 385.0

    tiny_max_tokens: int = 512
    small_max_tokens: int = 2048
    medium_max_tokens: int = 8192
    large_max_tokens: int = 32_768

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

    clean_allow_visual: bool = True
    clean_allow_multimodal: bool = True
    moderate_allow_visual: bool = True
    moderate_allow_multimodal: bool = False
    dirty_allow_visual: bool = False
    dirty_allow_multimodal: bool = False

    default_min_relevance: float = 0.5
    dirty_min_relevance: float = 0.7

    energy_per_token_wh: float = 1e-6
    visual_energy_multiplier: float = 2.5
    multimodal_energy_multiplier: float = 2.5
    audio_energy_multiplier: float = 2.0

    high_filter_rate_pct: float = 50.0
    token_savings_recommendation_threshold: int = 10_000

    max_history: int = 1000

    def __post_init__(self) -> None:
        if (
            isinstance(self.clean_threshold_g_kwh, bool)
            or not isinstance(
                self.clean_threshold_g_kwh, numbers.Real
            )
            or not math.isfinite(
                float(self.clean_threshold_g_kwh)
            )
            or self.clean_threshold_g_kwh <= 0
        ):
            raise TokenAwareFilterError(
                "clean_threshold_g_kwh must be finite and > 0."
            )
        if (
            isinstance(self.dirty_threshold_g_kwh, bool)
            or not isinstance(
                self.dirty_threshold_g_kwh, numbers.Real
            )
            or not math.isfinite(
                float(self.dirty_threshold_g_kwh)
            )
            or self.dirty_threshold_g_kwh
            <= self.clean_threshold_g_kwh
        ):
            raise TokenAwareFilterError(
                "dirty_threshold_g_kwh must be finite and > "
                "clean_threshold_g_kwh."
            )
        if (
            isinstance(self.default_grid_intensity_g_kwh, bool)
            or not isinstance(
                self.default_grid_intensity_g_kwh, numbers.Real
            )
            or not math.isfinite(
                float(self.default_grid_intensity_g_kwh)
            )
            or self.default_grid_intensity_g_kwh < 0
        ):
            raise TokenAwareFilterError(
                "default_grid_intensity_g_kwh must be finite and "
                ">= 0."
            )

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

        for prefix in ("clean", "moderate", "dirty"):
            values = [
                getattr(self, f"{prefix}_tiny_max"),
                getattr(self, f"{prefix}_small_max"),
                getattr(self, f"{prefix}_medium_max"),
                getattr(self, f"{prefix}_large_max"),
                getattr(self, f"{prefix}_xlarge_max"),
            ]
            for name, value in zip(
                ("tiny", "small", "medium", "large", "xlarge"),
                values,
            ):
                if (
                    isinstance(value, bool)
                    or not isinstance(value, numbers.Integral)
                    or value <= 0
                ):
                    raise TokenAwareFilterError(
                        f"{prefix}_{name}_max must be a positive int."
                    )
            if values != sorted(values):
                raise TokenAwareFilterError(
                    f"{prefix} caps must be non-decreasing: "
                    f"{values}."
                )

        for size in ("tiny", "small", "medium", "large", "xlarge"):
            clean = getattr(self, f"clean_{size}_max")
            moderate = getattr(self, f"moderate_{size}_max")
            dirty = getattr(self, f"dirty_{size}_max")
            if not (clean >= moderate >= dirty):
                raise TokenAwareFilterError(
                    f"{size} caps must satisfy clean >= moderate >= "
                    f"dirty ({clean} vs {moderate} vs {dirty})."
                )

        for name in (
            "default_min_relevance", "dirty_min_relevance",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
                or not 0.0 <= float(value) <= 1.0
            ):
                raise TokenAwareFilterError(
                    f"{name} must be finite and in [0, 1]."
                )
        if (
            self.dirty_min_relevance
            < self.default_min_relevance
        ):
            raise TokenAwareFilterError(
                "dirty_min_relevance must be >= default_min_relevance."
            )

        if (
            isinstance(self.energy_per_token_wh, bool)
            or not isinstance(
                self.energy_per_token_wh, numbers.Real
            )
            or not math.isfinite(
                float(self.energy_per_token_wh)
            )
            or self.energy_per_token_wh <= 0
        ):
            raise TokenAwareFilterError(
                "energy_per_token_wh must be finite and > 0."
            )
        for name in (
            "visual_energy_multiplier",
            "multimodal_energy_multiplier",
            "audio_energy_multiplier",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
                or value <= 0
            ):
                raise TokenAwareFilterError(
                    f"{name} must be finite and > 0."
                )

        if not 0.0 < self.high_filter_rate_pct <= 100.0:
            raise TokenAwareFilterError(
                "high_filter_rate_pct must be in (0, 100]."
            )
        if (
            isinstance(
                self.token_savings_recommendation_threshold, bool
            )
            or not isinstance(
                self.token_savings_recommendation_threshold,
                numbers.Integral,
            )
            or self.token_savings_recommendation_threshold <= 0
        ):
            raise TokenAwareFilterError(
                "token_savings_recommendation_threshold must be > 0."
            )
        if (
            isinstance(self.max_history, bool)
            or not isinstance(self.max_history, numbers.Integral)
            or self.max_history <= 0
        ):
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

    def grid_intensity_for(
        self, intensity_g_kwh: float,
    ) -> GridIntensity:
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
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "TokenAwareConfig":
        if not isinstance(data, Mapping):
            raise TokenAwareFilterError(
                f"TokenAwareConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        try:
            valid = set(cls.__dataclass_fields__.keys())
            unknown = set(data) - valid
            if unknown:
                logger.debug(
                    "TokenAwareConfig.from_dict dropping unknown "
                    "fields: %s", sorted(unknown),
                )
            kwargs = {
                k: v for k, v in data.items() if k in valid
            }
            return cls(**kwargs)
        except (TypeError, ValueError) as exc:
            if isinstance(exc, TokenAwareFilterError):
                raise
            raise TokenAwareFilterError(
                f"invalid TokenAwareConfig payload: {exc}"
            ) from exc


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
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Integral)
                or value <= 0
            ):
                raise TokenAwareFilterError(
                    "all max_tokens_* fields must be positive ints."
                )
        if list(values) != sorted(values):
            raise TokenAwareFilterError(
                f"max_tokens_* must be non-decreasing, "
                f"got {values}."
            )
        for name in (
            "allow_visual_context", "allow_multimodal",
        ):
            if not isinstance(getattr(self, name), bool):
                raise TokenAwareFilterError(
                    f"{name} must be a bool."
                )

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
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "FilteringPolicy":
        if not isinstance(data, Mapping):
            raise TokenAwareFilterError(
                "FilteringPolicy.from_dict expects a Mapping."
            )
        try:
            grid_raw = data["grid_intensity"]
            grid = GridIntensity(str(grid_raw))
            return cls(
                grid_intensity=grid,
                max_tokens_tiny=int(data["max_tokens_tiny"]),
                max_tokens_small=int(data["max_tokens_small"]),
                max_tokens_medium=int(
                    data["max_tokens_medium"]
                ),
                max_tokens_large=int(data["max_tokens_large"]),
                max_tokens_xlarge=int(
                    data["max_tokens_xlarge"]
                ),
                allow_visual_context=bool(
                    data.get("allow_visual_context", False)
                ),
                allow_multimodal=bool(
                    data.get("allow_multimodal", False)
                ),
            )
        except KeyError as exc:
            raise TokenAwareFilterError(
                f"FilteringPolicy payload missing "
                f"{exc.args[0]!r}."
            ) from exc
        except (TypeError, ValueError) as exc:
            if isinstance(exc, TokenAwareFilterError):
                raise
            raise TokenAwareFilterError(
                f"invalid FilteringPolicy payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "FilteringPolicy("
            f"grid={self.grid_intensity.value}, "
            f"tiny={self.max_tokens_tiny}, "
            f"small={self.max_tokens_small}, "
            f"medium={self.max_tokens_medium}, "
            f"large={self.max_tokens_large}, "
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

    def __post_init__(self) -> None:
        if not isinstance(self.should_filter, bool):
            raise TokenAwareFilterError(
                f"should_filter must be bool, got "
                f"{type(self.should_filter).__name__}."
            )
        if not isinstance(self.reason, str) or not self.reason:
            raise TokenAwareFilterError(
                "reason must be a non-empty string."
            )
        if self.context_size is not None:
            if self.context_size not in _CONTEXT_SIZES:
                raise TokenAwareFilterError(
                    f"context_size must be one of "
                    f"{_CONTEXT_SIZES}, got "
                    f"{self.context_size!r}."
                )
        if self.grid_intensity is not None:
            if self.grid_intensity not in _GRID_INTENSITIES:
                raise TokenAwareFilterError(
                    f"grid_intensity must be one of "
                    f"{_GRID_INTENSITIES}, got "
                    f"{self.grid_intensity!r}."
                )
        if self.max_allowed is not None:
            if (
                isinstance(self.max_allowed, bool)
                or not isinstance(
                    self.max_allowed, numbers.Integral
                )
                or self.max_allowed < 0
            ):
                raise TokenAwareFilterError(
                    "max_allowed must be a non-negative int or None."
                )
        if self.actual is not None:
            if (
                isinstance(self.actual, bool)
                or not isinstance(
                    self.actual, numbers.Real
                )
                or not math.isfinite(float(self.actual))
                or float(self.actual) < 0
            ):
                raise TokenAwareFilterError(
                    "actual must be finite and >= 0, "
                    f"got {self.actual!r}."
                )
        if self.min_required is not None:
            if (
                isinstance(self.min_required, bool)
                or not isinstance(
                    self.min_required, numbers.Real
                )
                or not math.isfinite(
                    float(self.min_required)
                )
                or not 0.0 <= float(self.min_required) <= 1.0
            ):
                raise TokenAwareFilterError(
                    "min_required must be finite and in [0, 1], "
                    f"got {self.min_required!r}."
                )

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

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "FilterDecision":
        if not isinstance(data, Mapping):
            raise TokenAwareFilterError(
                "FilterDecision.from_dict expects a Mapping."
            )
        if "should_filter" not in data or (
            "reason" not in data
        ):
            raise TokenAwareFilterError(
                "FilterDecision payload missing "
                "'should_filter' or 'reason'."
            )
        try:
            return cls(
                should_filter=data["should_filter"],
                reason=str(data["reason"]),
                context_size=data.get("context_size"),
                grid_intensity=data.get("grid_intensity"),
                max_allowed=data.get("max_allowed"),
                actual=data.get("actual"),
                min_required=data.get("min_required"),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, TokenAwareFilterError):
                raise
            raise TokenAwareFilterError(
                f"invalid FilterDecision payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        extra = ""
        if self.max_allowed is not None:
            extra = (
                f", max_allowed={self.max_allowed}, "
                f"actual={self.actual}"
            )
        elif self.min_required is not None:
            extra = (
                f", min_required={self.min_required}, "
                f"actual={self.actual}"
            )
        return (
            "FilterDecision("
            f"filter={self.should_filter}, "
            f"reason={self.reason!r}{extra})"
        )


# --------------------------------------------------------------------------- #
# Main filter
# --------------------------------------------------------------------------- #
class TokenAwareFilter:
    """
    Token-aware filtering layer for energy-efficient retrieval.

    Thread-safe, serializable, bounded in memory.

    Notes
    -----
    ``enable_adaptive`` is preserved for backward compatibility but
    not currently consulted by the filtering logic — every decision is
    driven by the ``FilteringPolicy`` derived from the current grid
    intensity. Callers who toggled this flag expecting dynamic
    behavior should also update ``policy`` / ``grid_intensity``
    explicitly.
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
                default_grid_intensity_g_kwh=float(
                    current_grid_intensity_g_kwh
                ),
            )
        self._strict = bool(strict)

        initial_intensity = self._coerce_intensity(
            current_grid_intensity_g_kwh,
            fallback=(
                self._config.default_grid_intensity_g_kwh
            ),
        )

        self.grid_intensity_value: float = initial_intensity
        self.enable_adaptive: bool = bool(
            enable_adaptive_filtering
        )
        self.grid_intensity: GridIntensity = (
            self._classify_grid_intensity(initial_intensity)
        )
        self.policy: FilteringPolicy = self._create_policy(
            self.grid_intensity
        )

        self._lock = threading.RLock()
        self.filtered_count: int = 0
        self.total_requests: int = 0
        self.tokens_saved: int = 0
        self.energy_saved_wh: float = 0.0

        # Bounded audit history — deque ensures the cap holds even
        # for entries loaded via from_dict.
        self._history: Deque[FilterDecision] = deque(
            maxlen=self._config.max_history
        )

        logger.debug(
            "TokenAwareFilter initialized "
            "(intensity=%.1f g/kWh, mode=%s, adaptive=%s, "
            "strict=%s)",
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
    def _classify_grid_intensity(
        self, intensity_g_kwh: float,
    ) -> GridIntensity:
        return self._config.grid_intensity_for(intensity_g_kwh)

    def _create_policy(
        self, grid_intensity: GridIntensity,
    ) -> FilteringPolicy:
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

    def classify_context_size(
        self, token_count: int,
    ) -> ContextSize:
        tokens = self._coerce_tokens(token_count)
        if tokens is None:
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
        """
        tokens = self._coerce_tokens(token_count)
        ctype = self._coerce_content_type(content_type)
        relevance = self._coerce_relevance(relevance_score)

        if tokens is None or relevance is None:
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
            self._account_savings(
                tokens, ctype, intensity_value,
            )
            decision = FilterDecision(
                should_filter=True,
                reason=(
                    "visual_content_restricted_during_dirty_grid"
                ),
                context_size=context_size.value,
                grid_intensity=grid_mode.value,
            )
            self._record(decision)
            return decision.to_dict()

        if (
            ctype == "multimodal"
            and not policy.allow_multimodal
        ):
            self._account_savings(
                tokens, ctype, intensity_value,
            )
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
            self._account_savings(
                saved, ctype, intensity_value,
            )
            decision = FilterDecision(
                should_filter=True,
                reason=(
                    f"exceeds_max_tokens_for_{context_size.value}"
                ),
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
            self._account_savings(
                tokens, ctype, intensity_value,
            )
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
        self, new_intensity_g_kwh: float,
    ) -> GridIntensity:
        """Update the current grid intensity; returns the new mode."""
        intensity = self._coerce_intensity(
            new_intensity_g_kwh,
            fallback=self.grid_intensity_value,
        )

        with self._lock:
            old_value = self.grid_intensity_value
            self.grid_intensity_value = intensity
            old_intensity = self.grid_intensity
            new_mode = self._classify_grid_intensity(intensity)
            self.grid_intensity = new_mode

            if new_mode != old_intensity:
                self.policy = self._create_policy(new_mode)

        if new_mode != old_intensity:
            logger.info(
                "Grid intensity changed %.1f → %.1f g/kWh "
                "(%s → %s).",
                old_value, intensity,
                old_intensity.value, new_mode.value,
            )
        return new_mode

    # ---------------------------------------------------------- helpers
    def _account_savings(
        self,
        saved_tokens: int,
        content_type: str,
        intensity_g_kwh: float,
    ) -> None:
        if saved_tokens <= 0:
            return
        cfg = self._config
        multiplier = cfg.energy_multiplier_for(content_type)
        energy_wh = (
            saved_tokens
            * cfg.energy_per_token_wh
            * multiplier
        )
        with self._lock:
            self.filtered_count += 1
            self.tokens_saved += saved_tokens
            self.energy_saved_wh += energy_wh

    def _record(self, decision: FilterDecision) -> None:
        with self._lock:
            self._history.append(decision)

    # ---------------------------------------------------------- validation
    def _coerce_intensity(
        self, value: Any, *, fallback: float,
    ) -> float:
        if isinstance(value, bool) or not isinstance(
            value, numbers.Real,
        ):
            msg = (
                f"intensity must be numeric, "
                f"got {type(value).__name__}."
            )
            if self._strict:
                raise TokenAwareFilterError(msg)
            logger.warning("%s Using %.1f.", msg, fallback)
            return float(fallback)
        fv = float(value)
        if not math.isfinite(fv):
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
        if isinstance(value, bool) or not isinstance(
            value, numbers.Integral,
        ):
            msg = (
                f"token_count must be an int, "
                f"got {type(value).__name__}."
            )
            if self._strict:
                raise TokenAwareFilterError(msg)
            logger.warning("%s Rejecting.", msg)
            return None
        iv = int(value)
        if iv < 0:
            msg = f"token_count must be >= 0, got {iv}."
            if self._strict:
                raise TokenAwareFilterError(msg)
            logger.warning("%s Rejecting.", msg)
            return None
        return iv

    def _coerce_content_type(self, value: Any) -> str:
        # Returns ``str`` always — non-strict invalid input defaults
        # to ``"text"``. The annotation is ``str`` (not Optional) so
        # callers can rely on a non-None result.
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

    def _coerce_relevance(
        self, value: Any,
    ) -> Optional[float]:
        if isinstance(value, bool) or not isinstance(
            value, numbers.Real,
        ):
            msg = (
                f"relevance_score must be numeric, "
                f"got {type(value).__name__}."
            )
            if self._strict:
                raise TokenAwareFilterError(msg)
            logger.warning("%s Rejecting.", msg)
            return None
        fv = float(value)
        if not math.isfinite(fv):
            msg = (
                f"relevance_score must be finite, "
                f"got {value!r}."
            )
            if self._strict:
                raise TokenAwareFilterError(msg)
            logger.warning("%s Rejecting.", msg)
            return None
        if not 0.0 <= fv <= 1.0:
            msg = (
                f"relevance_score must be in [0, 1], "
                f"got {fv}."
            )
            if self._strict:
                raise TokenAwareFilterError(msg)
            logger.warning("%s Rejecting.", msg)
            return None
        return fv

    # ---------------------------------------------------------- statistics
    def get_filtering_stats(self) -> Dict[str, Any]:
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
            "history_size": history_size,
            "history_capacity": self._config.max_history,
            "energy_per_token_wh": (
                self._config.energy_per_token_wh
            ),
        }

    def statistics(self) -> Dict[str, Any]:
        return self.get_filtering_stats()

    def get_recommendations(self) -> List[str]:
        with self._lock:
            total = self.total_requests
            filtered = self.filtered_count
            tokens_saved = self.tokens_saved
            energy_saved = self.energy_saved_wh
            grid_mode = self.grid_intensity

        recommendations: List[str] = []
        cfg = self._config

        filter_rate = (
            (filtered / total * 100.0) if total > 0 else 0.0
        )

        if filter_rate > cfg.high_filter_rate_pct:
            recommendations.append(
                "High filter rate detected. Consider adjusting "
                "relevance thresholds or query specificity."
            )

        if grid_mode == GridIntensity.DIRTY:
            recommendations.append(
                "Operating under dirty grid conditions. Visual "
                "and multimodal content restricted."
            )
            recommendations.append(
                "Consider deferring non-urgent retrievals to "
                "cleaner grid periods."
            )

        if (
            tokens_saved
            > cfg.token_savings_recommendation_threshold
        ):
            recommendations.append(
                f"Token-aware filtering has saved "
                f"{tokens_saved} tokens and "
                f"{energy_saved:.4f} Wh of energy."
            )

        return recommendations

    def reset_stats(
        self, *, clear_history: bool = True,
    ) -> None:
        with self._lock:
            self.filtered_count = 0
            self.total_requests = 0
            self.tokens_saved = 0
            self.energy_saved_wh = 0.0
            if clear_history:
                self._history.clear()
        logger.debug(
            "TokenAwareFilter statistics reset."
        )

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_history: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "version": VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "enable_adaptive": self.enable_adaptive,
                "grid_intensity_value": (
                    self.grid_intensity_value
                ),
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
                payload["history"] = [
                    d.to_dict() for d in self._history
                ]
        return payload

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "TokenAwareFilter":
        if not isinstance(data, Mapping):
            raise TokenAwareFilterError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        try:
            raw_config = data.get("config") or {}
            if not isinstance(raw_config, Mapping):
                raise TokenAwareFilterError(
                    "config must be a Mapping."
                )
            cfg = TokenAwareConfig.from_dict(dict(raw_config))

            strict_raw = data.get("strict", True)
            if not isinstance(strict_raw, bool):
                raise TokenAwareFilterError(
                    f"strict must be bool, got "
                    f"{type(strict_raw).__name__}."
                )

            raw_intensity = data.get("grid_intensity_value")
            if raw_intensity is None:
                initial_intensity = (
                    cfg.default_grid_intensity_g_kwh
                )
            else:
                if (
                    isinstance(raw_intensity, bool)
                    or not isinstance(
                        raw_intensity, numbers.Real
                    )
                ):
                    raise TokenAwareFilterError(
                        "grid_intensity_value must be numeric."
                    )
                initial_intensity = float(raw_intensity)
                if not math.isfinite(initial_intensity):
                    raise TokenAwareFilterError(
                        "grid_intensity_value must be finite."
                    )

            filter_ = cls(
                current_grid_intensity_g_kwh=initial_intensity,
                enable_adaptive_filtering=bool(
                    data.get("enable_adaptive", True)
                ),
                config=cfg,
                strict=strict_raw,
            )

            # ---- Cross-check payload's grid_intensity field -------
            stored_mode_raw = data.get("grid_intensity")
            if stored_mode_raw is not None:
                try:
                    stored_mode = GridIntensity(
                        str(stored_mode_raw)
                    )
                except ValueError as exc:
                    raise TokenAwareFilterError(
                        f"invalid grid_intensity "
                        f"{stored_mode_raw!r}: {exc}"
                    ) from exc
                if stored_mode != filter_.grid_intensity:
                    logger.warning(
                        "Stored grid_intensity=%s does not match "
                        "recomputed %s for intensity=%.1f; using "
                        "recomputed.",
                        stored_mode.value,
                        filter_.grid_intensity.value,
                        initial_intensity,
                    )

            # ---- Counters -----------------------------------------
            raw_counters = data.get("counters") or {}
            if not isinstance(raw_counters, Mapping):
                raise TokenAwareFilterError(
                    "counters must be a Mapping."
                )
            try:
                total_requests = int(
                    raw_counters.get("total_requests", 0)
                )
                filtered_count = int(
                    raw_counters.get("filtered_count", 0)
                )
                tokens_saved = int(
                    raw_counters.get("tokens_saved", 0)
                )
                energy_saved_wh = float(
                    raw_counters.get("energy_saved_wh", 0.0)
                )
            except (TypeError, ValueError) as exc:
                raise TokenAwareFilterError(
                    f"invalid counter: {exc}"
                ) from exc
            for name, value in (
                ("total_requests", total_requests),
                ("filtered_count", filtered_count),
                ("tokens_saved", tokens_saved),
            ):
                if value < 0:
                    raise TokenAwareFilterError(
                        f"counter {name} must be >= 0, "
                        f"got {value}."
                    )
            if not math.isfinite(energy_saved_wh) or (
                energy_saved_wh < 0
            ):
                raise TokenAwareFilterError(
                    "counter energy_saved_wh must be finite and "
                    ">= 0."
                )

            # ---- History ------------------------------------------
            raw_history = data.get("history") or []
            if not isinstance(raw_history, list):
                raise TokenAwareFilterError(
                    f"history must be a list, got "
                    f"{type(raw_history).__name__}."
                )
            if len(raw_history) > cfg.max_history:
                logger.warning(
                    "history has %d entries, exceeds max_history="
                    "%d; truncating to newest %d.",
                    len(raw_history),
                    cfg.max_history,
                    cfg.max_history,
                )
                raw_history = raw_history[
                    -cfg.max_history :
                ]

            with filter_._lock:
                filter_.total_requests = total_requests
                filter_.filtered_count = filtered_count
                filter_.tokens_saved = tokens_saved
                filter_.energy_saved_wh = energy_saved_wh
                for entry in raw_history:
                    filter_._history.append(
                        FilterDecision.from_dict(entry)
                    )
            return filter_
        except TokenAwareFilterError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise TokenAwareFilterError(
                f"invalid TokenAwareFilter payload: {exc}"
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
    ) -> "TokenAwareFilter":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise TokenAwareFilterError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "TokenAwareFilter":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
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
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("VERSION    :", VERSION)

    f = TokenAwareFilter(current_grid_intensity_g_kwh=100.0)
    print("repr       :", f)
    assert f.grid_intensity == GridIntensity.CLEAN

    d = f.should_filter(
        token_count=20_000,
        content_type="visual",
        relevance_score=0.9,
    )
    assert d["should_filter"] is False
    print("clean vis  : OK")

    f.update_grid_intensity(300.0)
    assert f.grid_intensity == GridIntensity.MODERATE
    d = f.should_filter(1000, "multimodal", 0.9)
    assert d["should_filter"] is True
    assert d["reason"] == "multimodal_content_restricted"
    print("moderate mm: OK")

    f.update_grid_intensity(500.0)
    assert f.grid_intensity == GridIntensity.DIRTY
    d_vis = f.should_filter(1000, "visual", 0.9)
    d_mm = f.should_filter(1000, "multimodal", 0.9)
    assert d_vis["should_filter"] and d_mm["should_filter"]

    d_big = f.should_filter(100_000, "text", 0.9)
    assert d_big["should_filter"] is True
    assert d_big["reason"].startswith(
        "exceeds_max_tokens_for_"
    )

    d_low = f.should_filter(100, "text", 0.3)
    assert d_low["should_filter"] is True
    assert d_low["reason"] == "low_relevance_score"

    # ---- Strict-mode validation --------------------------------- #
    for bad in (
        lambda: f.should_filter(100, "text", float("nan")),
        lambda: f.should_filter(-5, "text", 0.9),
        lambda: f.should_filter(100, "images", 0.9),
        lambda: f.update_grid_intensity(float("nan")),
        lambda: f.should_filter("100", "text", 0.9),
    ):
        try:
            bad()
        except TokenAwareFilterError as exc:
            print("Rejected   :", str(exc)[:60])

    # ---- update returns the mode -------------------------------- #
    mode = f.update_grid_intensity(100.0)
    assert mode == GridIntensity.CLEAN
    print("update ret :", mode.value)

    # ---- FilterDecision invariants ---------------------------- #
    for bad_d in (
        lambda: FilterDecision(
            should_filter="yes", reason="x",  # type: ignore[arg-type]
        ),
        lambda: FilterDecision(
            should_filter=True, reason="",
        ),
        lambda: FilterDecision(
            should_filter=True, reason="x",
            context_size="bogus",
        ),
        lambda: FilterDecision(
            should_filter=True, reason="x",
            grid_intensity="bogus",
        ),
        lambda: FilterDecision(
            should_filter=True, reason="x",
            max_allowed=-5,
        ),
        lambda: FilterDecision(
            should_filter=True, reason="x",
            actual=float("nan"),
        ),
        lambda: FilterDecision(
            should_filter=True, reason="x",
            min_required=1.5,
        ),
    ):
        try:
            bad_d()
        except TokenAwareFilterError as exc:
            print("Rejected d :", str(exc)[:50])

    # ---- Bounded history (was broken) ------------------------- #
    # Load a payload with more entries than max_history — the
    # enhanced from_dict must truncate to max_history.
    orig = TokenAwareFilter(
        config=TokenAwareConfig(max_history=3)
    )
    for i in range(3):
        orig.should_filter(100, "text", 0.9)

    payload = orig.to_dict(include_history=True)
    payload["history"] = payload["history"] * 10  # 30 entries
    reloaded = TokenAwareFilter.from_dict(payload)
    assert len(reloaded._history) <= 3, (
        f"history not bounded: {len(reloaded._history)}"
    )
    print(
        f"bounded    : {len(reloaded._history)} entries "
        f"(cap=3)"
    )

    # ---- Round-trip preserves history ------------------------- #
    full_payload = orig.to_dict(include_history=True)
    restored = TokenAwareFilter.from_dict(full_payload)
    assert len(restored._history) == len(orig._history)
    assert (
        restored.to_dict(include_history=True)
        == orig.to_dict(include_history=True)
    )
    print("round-trip : OK (history)")

    # ---- from_dict errors wrapped ----------------------------- #
    for bad in (
        {"history": "abc"},
        {"history": None, "config": {}},
        {"history": [{"should_filter": "yes", "reason": "x"}]},
        {"history": [{}]},  # missing required
        {"strict": "yes"},
        {"counters": {"total_requests": "abc"}},
        {"config": {"clean_threshold_g_kwh": "abc"}},
        {"grid_intensity_value": "abc"},
        {"grid_intensity_value": float("nan")},
        {"counters": {"energy_saved_wh": -1.0}},
    ):
        try:
            TokenAwareFilter.from_dict(bad)
        except TokenAwareFilterError as exc:
            print("Rejected p :", str(exc)[:60])

    # ---- to_json rejects NaN ---------------------------------- #
    tainted = TokenAwareFilter()
    tainted.should_filter(100, "text", 0.9)
    object.__setattr__(
        tainted, "energy_saved_wh", float("nan"),
    )
    try:
        tainted.to_json()
    except ValueError as exc:
        print("NaN json   :", str(exc)[:60])

    # ---- Statistics ------------------------------------------- #
    print("stats      :", {
        k: v for k, v in f.get_filtering_stats().items()
        if k not in ("current_policy",)
    })

    # ---- Per-content-type energy multipliers ------------------ #
    multi = TokenAwareFilter(
        current_grid_intensity_g_kwh=500.0,
    )
    multi.should_filter(1000, "text", 0.9)
    multi.should_filter(1000, "visual", 0.9)
    multi.should_filter(1000, "multimodal", 0.9)
    multi.should_filter(1000, "audio", 0.9)
    stats = multi.get_filtering_stats()
    assert stats["tokens_saved"] == 2000
    assert abs(
        stats["energy_saved_wh"] - 0.005
    ) < 1e-9
    print("multipliers: OK")

    # ---- Config validation ------------------------------------ #
    for bad_cfg in (
        dict(clean_threshold_g_kwh=0),
        dict(
            clean_threshold_g_kwh=400,
            dirty_threshold_g_kwh=200,
        ),
        dict(default_grid_intensity_g_kwh=-1),
        dict(tiny_max_tokens=0),
        dict(tiny_max_tokens=5000, small_max_tokens=2000),
        dict(clean_tiny_max=0),
        dict(
            moderate_tiny_max=2000, moderate_small_max=1000,
        ),
        dict(
            clean_tiny_max=10,
            moderate_tiny_max=100,
            dirty_tiny_max=100,
        ),
        dict(default_min_relevance=-0.1),
        dict(
            default_min_relevance=0.9,
            dirty_min_relevance=0.5,
        ),
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
            print("Rejected c :", str(exc)[:50])

    # ---- Non-strict mode ------------------------------------- #
    lenient = TokenAwareFilter(strict=False)
    assert lenient.should_filter(
        "not-an-int", "text", 0.9,  # type: ignore[arg-type]
    )["should_filter"] is True
    assert lenient.should_filter(
        100, "images", 0.9,
    )["reason"] != "invalid_input_rejected"  # coerced to text
    lenient.update_grid_intensity(
        "not-a-number"  # type: ignore[arg-type]
    )
    print("lenient    : OK")

    # ---- FilteringPolicy round-trip -------------------------- #
    p = f.policy
    p_restored = FilteringPolicy.from_dict(p.to_dict())
    assert p_restored.to_dict() == p.to_dict()
    print("policy rt  : OK")

    # ---- FilterDecision round-trip --------------------------- #
    fd = FilterDecision.from_dict(d_low)
    assert fd.to_dict() == d_low
    print("decision rt: OK")

    print("\nSmoke test passed.")
