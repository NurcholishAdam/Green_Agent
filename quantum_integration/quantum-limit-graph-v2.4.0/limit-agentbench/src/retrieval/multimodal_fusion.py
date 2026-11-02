# src/retrieval/multimodal_fusion.py

"""
Multimodal Fusion Module
========================

Fuses text, visual, audio, and multimodal retrieval results with
energy-aware weighting. Inspired by VimRAG's multimodal memory graph
navigation.

Design
------
- Frozen, validated ``MultimodalFusionConfig`` centralizes every
  coefficient: per-modality weights, energy penalty, cost table,
  keyword maps, and bounded history.
- ``FusionMethod`` accepts both the enum and its string value.
- ``fuse_multimodal_results`` honours both ``energy_budget`` and
  ``token_budget`` — the same two budgets
  ``adaptive_modality_selection`` uses.
- **Confidence is used consistently** by both ``weighted_sum`` and
  ``adaptive`` fusion. Neither method multiplies raw relevance without
  scaling by the caller's confidence.
- **Live state vs. config**: ``modality_weights`` and
  ``energy_penalty`` are the *live* values used at fusion time.
  ``config`` provides the initial values; ``to_dict`` / ``from_dict``
  serialize and restore the live values exactly.
- **Serialization is lossless** — ``from_dict`` validates every
  numeric field, restores ``energy_penalty`` and ``modality_weights``,
  rejects over-cap histories, and wraps all failures in
  :class:`MultimodalFusionError`. ``to_json`` uses
  ``allow_nan=False``.
- ``VERSION`` tracks the schema independently of any deployment.

Version: 2.0.0
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
from types import TracebackType
from typing import (
    Any,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "FusedResult",
    "FusionMethod",
    "ModalityScore",
    "ModalityType",
    "MultimodalFusion",
    "MultimodalFusionConfig",
    "MultimodalFusionError",
]

VERSION = "2.0.0"


# --------------------------------------------------------------------------- #
# Optional NumPy
# --------------------------------------------------------------------------- #
try:  # pragma: no cover
    import numpy as np  # type: ignore

    _NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False
    logger.debug("numpy not importable; using pure-Python mean.")


def _mean(values: Sequence[float]) -> float:
    """Arithmetic mean, using NumPy when available."""
    if not values:
        return 0.0
    if _NUMPY_AVAILABLE and np is not None:
        return float(np.mean(values))
    return sum(values) / len(values)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MultimodalFusionError(ValueError):
    """Raised for invalid multimodal fusion inputs or configuration."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class ModalityType(Enum):
    """Types of modalities."""
    TEXT = "text"
    VISUAL = "visual"
    AUDIO = "audio"
    MULTIMODAL = "multimodal"


class FusionMethod(Enum):
    """Fusion strategies."""
    WEIGHTED_SUM = "weighted_sum"
    MAX = "max"
    ADAPTIVE = "adaptive"


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MultimodalFusionConfig:
    """
    Tunable parameters for :class:`MultimodalFusion`.

    The values here are **initial** values. Once a
    :class:`MultimodalFusion` is constructed, the live values used at
    fusion time are ``fusion.modality_weights`` and
    ``fusion.energy_penalty``; those are what ``to_dict`` and
    ``from_dict`` round-trip.
    """

    text_weight: float = 0.6
    visual_weight: float = 0.4
    audio_weight: float = 0.3
    multimodal_weight: float = 1.0

    energy_penalty_factor: float = 0.1
    min_energy_penalty: float = 0.1

    epsilon: float = 1e-6

    text_energy: float = 0.001
    text_tokens: int = 500
    visual_energy: float = 0.0025
    visual_tokens: int = 256
    audio_energy: float = 0.002
    audio_tokens: int = 300
    multimodal_energy: float = 0.004
    multimodal_tokens: int = 800

    visual_keywords: tuple = (
        "image", "visual", "picture", "diagram", "photo", "chart",
    )
    audio_keywords: tuple = (
        "audio", "sound", "speech", "voice", "listen", "recording",
    )

    default_text_relevance: float = 0.8
    visual_keyword_relevance: float = 0.9
    visual_default_relevance: float = 0.3
    audio_keyword_relevance: float = 0.9
    audio_default_relevance: float = 0.2
    multimodal_default_relevance: float = 0.5

    # Fallback confidence when a payload's confidence is missing or
    # non-finite. 0.0 = worst-case (safer than 1.0 = best-case).
    default_confidence: float = 0.0

    max_history: int = 10_000

    def __post_init__(self) -> None:
        for name in (
            "text_weight", "visual_weight",
            "audio_weight", "multimodal_weight",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise MultimodalFusionError(
                    f"{name} must be finite and >= 0."
                )
        if (
            isinstance(self.energy_penalty_factor, bool)
            or not isinstance(
                self.energy_penalty_factor, (int, float)
            )
            or not math.isfinite(float(self.energy_penalty_factor))
            or self.energy_penalty_factor < 0
        ):
            raise MultimodalFusionError(
                "energy_penalty_factor must be finite and >= 0."
            )
        if not 0.0 <= self.min_energy_penalty <= 1.0:
            raise MultimodalFusionError(
                "min_energy_penalty must be in [0, 1]."
            )
        if (
            isinstance(self.epsilon, bool)
            or not isinstance(self.epsilon, (int, float))
            or not math.isfinite(float(self.epsilon))
            or self.epsilon <= 0
        ):
            raise MultimodalFusionError(
                "epsilon must be finite and > 0."
            )
        for name in (
            "text_energy", "visual_energy",
            "audio_energy", "multimodal_energy",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise MultimodalFusionError(
                    f"{name} must be finite and >= 0."
                )
        for name in (
            "text_tokens", "visual_tokens",
            "audio_tokens", "multimodal_tokens",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
            ):
                raise MultimodalFusionError(
                    f"{name} must be a non-negative int."
                )
        for name in (
            "default_text_relevance",
            "visual_keyword_relevance",
            "visual_default_relevance",
            "audio_keyword_relevance",
            "audio_default_relevance",
            "multimodal_default_relevance",
            "default_confidence",
        ):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise MultimodalFusionError(
                    f"{name} must be in [0, 1]."
                )
        if not isinstance(self.visual_keywords, tuple):
            raise MultimodalFusionError(
                "visual_keywords must be a tuple."
            )
        if not isinstance(self.audio_keywords, tuple):
            raise MultimodalFusionError(
                "audio_keywords must be a tuple."
            )
        if (
            isinstance(self.max_history, bool)
            or not isinstance(self.max_history, int)
            or self.max_history <= 0
        ):
            raise MultimodalFusionError("max_history must be > 0.")

    def modality_weight(self, modality: ModalityType) -> float:
        return {
            ModalityType.TEXT: self.text_weight,
            ModalityType.VISUAL: self.visual_weight,
            ModalityType.AUDIO: self.audio_weight,
            ModalityType.MULTIMODAL: self.multimodal_weight,
        }[modality]

    def modality_cost(
        self, modality: ModalityType,
    ) -> Tuple[float, int]:
        """Return ``(energy_wh, tokens)`` for ``modality``."""
        return {
            ModalityType.TEXT: (
                self.text_energy, self.text_tokens,
            ),
            ModalityType.VISUAL: (
                self.visual_energy, self.visual_tokens,
            ),
            ModalityType.AUDIO: (
                self.audio_energy, self.audio_tokens,
            ),
            ModalityType.MULTIMODAL: (
                self.multimodal_energy, self.multimodal_tokens,
            ),
        }[modality]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "MultimodalFusionConfig":
        if not isinstance(data, Mapping):
            raise MultimodalFusionError(
                f"MultimodalFusionConfig.from_dict expects a "
                f"Mapping, got {type(data).__name__}."
            )
        try:
            kwargs: Dict[str, Any] = {}
            for key in (
                "text_weight", "visual_weight",
                "audio_weight", "multimodal_weight",
                "energy_penalty_factor", "min_energy_penalty",
                "epsilon",
                "text_energy", "text_tokens",
                "visual_energy", "visual_tokens",
                "audio_energy", "audio_tokens",
                "multimodal_energy", "multimodal_tokens",
                "default_text_relevance",
                "visual_keyword_relevance",
                "visual_default_relevance",
                "audio_keyword_relevance",
                "audio_default_relevance",
                "multimodal_default_relevance",
                "default_confidence",
                "max_history",
            ):
                if key in data:
                    kwargs[key] = data[key]
            for key in ("visual_keywords", "audio_keywords"):
                if key in data:
                    kwargs[key] = tuple(data[key])
            return cls(**kwargs)
        except (TypeError, ValueError) as exc:
            if isinstance(exc, MultimodalFusionError):
                raise
            raise MultimodalFusionError(
                f"invalid MultimodalFusionConfig payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# Dataclasses
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ModalityScore:
    """Immutable score for a specific modality."""

    modality: ModalityType
    relevance_score: float
    confidence: float
    energy_cost: float
    token_cost: int

    def __post_init__(self) -> None:
        if not isinstance(self.modality, ModalityType):
            raise MultimodalFusionError(
                f"modality must be a ModalityType, got "
                f"{type(self.modality).__name__}."
            )
        for name in (
            "relevance_score", "confidence", "energy_cost",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(
                value, (int, float)
            ):
                raise MultimodalFusionError(
                    f"{name} must be numeric."
                )
            fv = float(value)
            if not math.isfinite(fv) or fv < 0:
                raise MultimodalFusionError(
                    f"{name} must be finite and >= 0, "
                    f"got {value!r}."
                )
        if not 0.0 <= self.relevance_score <= 1.0:
            raise MultimodalFusionError(
                "relevance_score must be in [0, 1]."
            )
        if not 0.0 <= self.confidence <= 1.0:
            raise MultimodalFusionError(
                "confidence must be in [0, 1]."
            )
        if (
            isinstance(self.token_cost, bool)
            or not isinstance(self.token_cost, int)
            or self.token_cost < 0
        ):
            raise MultimodalFusionError(
                "token_cost must be a non-negative int."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "modality": self.modality.value,
            "relevance_score": self.relevance_score,
            "confidence": self.confidence,
            "energy_cost": self.energy_cost,
            "token_cost": self.token_cost,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "ModalityScore":
        if not isinstance(data, Mapping):
            raise MultimodalFusionError(
                "ModalityScore.from_dict expects a Mapping."
            )
        if "modality" not in data:
            raise MultimodalFusionError(
                "ModalityScore requires 'modality'."
            )
        try:
            return cls(
                modality=ModalityType(str(data["modality"])),
                relevance_score=float(
                    data.get("relevance_score", 0.0)
                ),
                confidence=float(
                    data.get("confidence", 1.0)
                ),
                energy_cost=float(
                    data.get("energy_cost", 0.0)
                ),
                token_cost=int(data.get("token_cost", 0)),
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, MultimodalFusionError):
                raise
            raise MultimodalFusionError(
                f"invalid ModalityScore payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "ModalityScore("
            f"modality={self.modality.value}, "
            f"relevance={self.relevance_score:.3f}, "
            f"confidence={self.confidence:.3f}, "
            f"energy={self.energy_cost:.5f}, "
            f"tokens={self.token_cost})"
        )


@dataclass(frozen=True)
class FusedResult:
    """Immutable result from multimodal fusion."""

    content_id: str
    modalities: Tuple[ModalityType, ...]
    fused_score: float
    modality_scores: Mapping[ModalityType, ModalityScore]
    total_energy: float
    total_tokens: int
    fusion_method: str
    dropped_modalities: Tuple[str, ...] = ()
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.content_id, str) or not self.content_id:
            raise MultimodalFusionError(
                "content_id must be a non-empty string."
            )
        if not isinstance(self.modalities, tuple):
            object.__setattr__(
                self, "modalities", tuple(self.modalities)
            )
        if not isinstance(self.dropped_modalities, tuple):
            object.__setattr__(
                self, "dropped_modalities",
                tuple(self.dropped_modalities),
            )
        for name in ("fused_score", "total_energy"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
            ):
                raise MultimodalFusionError(
                    f"{name} must be numeric."
                )
            fv = float(value)
            if not math.isfinite(fv) or fv < 0:
                raise MultimodalFusionError(
                    f"{name} must be finite and >= 0, "
                    f"got {value!r}."
                )
        if (
            isinstance(self.total_tokens, bool)
            or not isinstance(self.total_tokens, int)
            or self.total_tokens < 0
        ):
            raise MultimodalFusionError(
                "total_tokens must be a non-negative int."
            )
        if not isinstance(self.fusion_method, str) or not self.fusion_method:
            raise MultimodalFusionError(
                "fusion_method must be a non-empty string."
            )
        if not isinstance(self.modality_scores, Mapping):
            raise MultimodalFusionError(
                f"modality_scores must be a Mapping, got "
                f"{type(self.modality_scores).__name__}."
            )
        for modality, score in self.modality_scores.items():
            if not isinstance(modality, ModalityType):
                raise MultimodalFusionError(
                    "modality_scores keys must be ModalityType."
                )
            if not isinstance(score, ModalityScore):
                raise MultimodalFusionError(
                    f"modality_scores[{modality.value!r}] must be "
                    f"a ModalityScore."
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "content_id": self.content_id,
            "modalities": [m.value for m in self.modalities],
            "fused_score": self.fused_score,
            "modality_scores": {
                m.value: s.to_dict()
                for m, s in self.modality_scores.items()
            },
            "total_energy": self.total_energy,
            "total_tokens": self.total_tokens,
            "fusion_method": self.fusion_method,
            "dropped_modalities": list(self.dropped_modalities),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "FusedResult":
        if not isinstance(data, Mapping):
            raise MultimodalFusionError(
                "FusedResult.from_dict expects a Mapping."
            )
        if "content_id" not in data:
            raise MultimodalFusionError(
                "FusedResult requires 'content_id'."
            )
        try:
            ts = data.get("timestamp")
            if isinstance(ts, str):
                timestamp = datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )
                if not timestamp.tzinfo:
                    timestamp = timestamp.replace(
                        tzinfo=timezone.utc
                    )
            elif isinstance(ts, datetime):
                timestamp = ts
            else:
                timestamp = datetime.now(timezone.utc)

            raw_scores = data.get("modality_scores") or {}
            if not isinstance(raw_scores, Mapping):
                raise MultimodalFusionError(
                    "modality_scores must be a Mapping."
                )

            return cls(
                content_id=str(data["content_id"]),
                modalities=tuple(
                    ModalityType(str(m))
                    for m in (data.get("modalities") or [])
                ),
                fused_score=float(
                    data.get("fused_score", 0.0)
                ),
                modality_scores={
                    ModalityType(str(k)): ModalityScore.from_dict(v)
                    for k, v in raw_scores.items()
                },
                total_energy=float(
                    data.get("total_energy", 0.0)
                ),
                total_tokens=int(
                    data.get("total_tokens", 0)
                ),
                fusion_method=str(
                    data.get("fusion_method", "weighted_sum")
                ),
                dropped_modalities=tuple(
                    str(m)
                    for m in (data.get("dropped_modalities") or [])
                ),
                timestamp=timestamp,
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, MultimodalFusionError):
                raise
            raise MultimodalFusionError(
                f"invalid FusedResult payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "FusedResult("
            f"id={self.content_id!r}, "
            f"modalities={[m.value for m in self.modalities]}, "
            f"score={self.fused_score:.3f}, "
            f"energy={self.total_energy:.5f}, "
            f"tokens={self.total_tokens})"
        )


# --------------------------------------------------------------------------- #
# Fusion engine
# --------------------------------------------------------------------------- #
class MultimodalFusion:
    """
    Multimodal fusion engine for combining retrieval results.

    Thread-safe, serializable, bounded in memory.

    Live state vs. config
    ---------------------
    ``modality_weights`` and ``energy_penalty`` are the **live**
    values used during fusion. ``self.config`` provides initial values
    at construction. ``to_dict`` / ``from_dict`` serialize and restore
    the live values exactly, so an out-of-band mutation survives a
    round-trip.
    """

    def __init__(
        self,
        text_weight: float = 0.6,
        visual_weight: float = 0.4,
        energy_penalty_factor: float = 0.1,
        *,
        config: Optional[MultimodalFusionConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = MultimodalFusionConfig(
                text_weight=float(text_weight),
                visual_weight=float(visual_weight),
                energy_penalty_factor=float(energy_penalty_factor),
            )
        self._strict = bool(strict)

        self.modality_weights: Dict[ModalityType, float] = {
            ModalityType.TEXT: self._config.text_weight,
            ModalityType.VISUAL: self._config.visual_weight,
            ModalityType.AUDIO: self._config.audio_weight,
            ModalityType.MULTIMODAL: self._config.multimodal_weight,
        }
        self.energy_penalty: float = (
            self._config.energy_penalty_factor
        )

        self._lock = threading.RLock()
        self.fusion_history: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_history
        )
        self.modality_usage: Dict[ModalityType, int] = {
            m: 0 for m in ModalityType
        }
        self._started_at: float = time.time()

        logger.debug(
            "MultimodalFusion initialized "
            "(text=%.3f, visual=%.3f, audio=%.3f, "
            "multimodal=%.3f, energy_penalty=%.3f, strict=%s)",
            self._config.text_weight, self._config.visual_weight,
            self._config.audio_weight, self._config.multimodal_weight,
            self.energy_penalty, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> MultimodalFusionConfig:
        return self._config

    @property
    def history_size(self) -> int:
        with self._lock:
            return len(self.fusion_history)

    # ---------------------------------------------------------- public API
    def fuse_multimodal_results(
        self,
        results: List[Mapping[str, Any]],
        fusion_method: Any = FusionMethod.WEIGHTED_SUM,
        energy_budget: Optional[float] = None,
        token_budget: Optional[int] = None,
    ) -> List[FusedResult]:
        """
        Fuse multimodal retrieval results.

        Parameters
        ----------
        results : list of Mapping
            Each entry may contain one or more keys named after a
            :class:`ModalityType` value. The value under each key is
            either a Mapping with ``score``, ``confidence``,
            ``energy``, ``tokens`` keys, or a bare numeric score.
        fusion_method : FusionMethod | str
            ``"weighted_sum"``, ``"max"``, or ``"adaptive"``.
        energy_budget : float, optional
            When provided, per-result modalities are reduced (dropping
            the least cost-effective first) until total energy fits.
        token_budget : int, optional
            When provided, per-result modalities are reduced until
            total tokens fit. Applied together with
            ``energy_budget`` when both are given.

        Returns
        -------
        list of FusedResult
            Sorted by descending ``fused_score``; ties broken by
            ``content_id`` ascending.
        """
        if not isinstance(results, Sequence) or isinstance(
            results, (str, bytes),
        ):
            msg = (
                f"results must be a list of mappings, "
                f"got {type(results).__name__}."
            )
            if self._strict:
                raise MultimodalFusionError(msg)
            logger.warning("%s Returning empty list.", msg)
            return []

        method = self._coerce_fusion_method(fusion_method)

        budget: Optional[float] = None
        if energy_budget is not None:
            if (
                isinstance(energy_budget, bool)
                or not isinstance(energy_budget, (int, float))
            ):
                raise MultimodalFusionError(
                    "energy_budget must be numeric or None."
                )
            fb = float(energy_budget)
            if not math.isfinite(fb) or fb < 0:
                raise MultimodalFusionError(
                    f"energy_budget must be finite and >= 0, "
                    f"got {energy_budget!r}."
                )
            budget = fb

        token_cap: Optional[int] = None
        if token_budget is not None:
            if (
                isinstance(token_budget, bool)
                or not isinstance(token_budget, int)
                or token_budget < 0
            ):
                raise MultimodalFusionError(
                    "token_budget must be a non-negative int or "
                    "None."
                )
            token_cap = token_budget

        reduce_active = (
            (budget is not None and budget > 0)
            or token_cap is not None
        )

        fused_results: List[FusedResult] = []
        for idx, result in enumerate(results):
            if not isinstance(result, Mapping):
                msg = (
                    f"results[{idx}] must be a Mapping, got "
                    f"{type(result).__name__}."
                )
                if self._strict:
                    raise MultimodalFusionError(msg)
                logger.warning("%s Skipping.", msg)
                continue

            try:
                modality_scores = self._extract_modality_scores(
                    result,
                )
            except MultimodalFusionError:
                if self._strict:
                    raise
                logger.warning(
                    "Skipping malformed result %d.", idx,
                )
                continue

            if not modality_scores:
                logger.debug(
                    "Result %d has no recognized modalities.", idx,
                )
                continue

            dropped: List[str] = []
            if reduce_active:
                modality_scores, dropped = self._reduce_modalities(
                    modality_scores, budget, token_cap,
                )
                if not modality_scores:
                    logger.debug(
                        "Result %d dropped: no modality fits "
                        "budget(s) energy=%s tokens=%s.",
                        idx, budget, token_cap,
                    )
                    continue

            fused_score = self._apply_fusion(
                modality_scores, method, budget,
            )
            total_energy = sum(
                s.energy_cost for s in modality_scores.values()
            )
            total_tokens = sum(
                s.token_cost for s in modality_scores.values()
            )

            fused = FusedResult(
                content_id=str(result.get(
                    "content_id", "unknown",
                )),
                modalities=tuple(modality_scores.keys()),
                fused_score=fused_score,
                modality_scores=dict(modality_scores),
                total_energy=total_energy,
                total_tokens=total_tokens,
                fusion_method=method.value,
                dropped_modalities=tuple(dropped),
            )
            fused_results.append(fused)

            with self._lock:
                for modality in modality_scores.keys():
                    self.modality_usage[modality] += 1
                self.fusion_history.append({
                    "method": method.value,
                    "score": fused_score,
                    "energy": total_energy,
                    "tokens": total_tokens,
                    "modalities": [
                        m.value
                        for m in modality_scores.keys()
                    ],
                    "dropped": list(dropped),
                })

        fused_results.sort(
            key=lambda f: (-f.fused_score, f.content_id)
        )

        logger.debug(
            "Fused %d/%d result(s) using method=%s "
            "(energy_budget=%s, token_budget=%s).",
            len(fused_results), len(results), method.value,
            f"{budget:.4f}" if budget is not None else "none",
            token_cap,
        )
        return fused_results

    def adaptive_modality_selection(
        self,
        query: str,
        available_modalities: List[ModalityType],
        energy_budget: float,
        token_budget: int,
    ) -> List[ModalityType]:
        """
        Adaptively select modalities based on query and budgets.

        Modalities are ranked by ``relevance / (energy + epsilon)``
        and added while both budgets allow. Selected modalities are
        recorded in ``modality_usage``.
        """
        if not isinstance(query, str) or not query:
            raise MultimodalFusionError(
                "query must be a non-empty string."
            )
        if not isinstance(available_modalities, list):
            raise MultimodalFusionError(
                "available_modalities must be a list."
            )
        for i, m in enumerate(available_modalities):
            if not isinstance(m, ModalityType):
                raise MultimodalFusionError(
                    f"available_modalities[{i}] must be a "
                    f"ModalityType, got {type(m).__name__}."
                )
        if (
            isinstance(energy_budget, bool)
            or not isinstance(energy_budget, (int, float))
        ):
            raise MultimodalFusionError(
                "energy_budget must be numeric."
            )
        fb = float(energy_budget)
        if not math.isfinite(fb) or fb < 0:
            raise MultimodalFusionError(
                "energy_budget must be finite and >= 0."
            )
        if (
            isinstance(token_budget, bool)
            or not isinstance(token_budget, int)
            or token_budget < 0
        ):
            raise MultimodalFusionError(
                "token_budget must be a non-negative int."
            )

        remaining_energy = fb
        remaining_tokens = token_budget

        modality_relevance = self._score_modality_relevance(
            query, available_modalities,
        )

        candidates: List[Tuple[ModalityType, float]] = []
        for m in available_modalities:
            energy, _tokens = self._config.modality_cost(m)
            ratio = modality_relevance.get(m, 0.5) / (
                energy + self._config.epsilon
            )
            candidates.append((m, ratio))
        candidates.sort(key=lambda t: (-t[1], t[0].value))

        selected: List[ModalityType] = []
        for modality, _ratio in candidates:
            energy, tokens = self._config.modality_cost(modality)
            if (
                energy <= remaining_energy
                and tokens <= remaining_tokens
            ):
                selected.append(modality)
                remaining_energy -= energy
                remaining_tokens -= tokens

        with self._lock:
            for m in selected:
                self.modality_usage[m] += 1

        logger.debug(
            "adaptive_modality_selection: query=%r selected=%s "
            "(budget energy=%.4f tokens=%d, remaining energy="
            "%.4f tokens=%d)",
            query, [m.value for m in selected],
            fb, token_budget,
            remaining_energy, remaining_tokens,
        )
        return selected

    def get_fusion_statistics(self) -> Dict[str, Any]:
        with self._lock:
            history = list(self.fusion_history)
            usage = dict(self.modality_usage)

        if not history:
            return {
                "status": "no_fusions",
                "total_fusions": 0,
                "modality_usage": {
                    m.value: c for m, c in usage.items()
                },
                "most_used_modality": None,
            }

        scores = [h["score"] for h in history]
        energies = [h["energy"] for h in history]
        tokens = [h["tokens"] for h in history]

        non_zero = {m: c for m, c in usage.items() if c > 0}
        most_used = (
            max(non_zero.items(), key=lambda kv: kv[1])[0].value
            if non_zero else None
        )

        return {
            "total_fusions": len(history),
            "avg_fused_score": _mean(scores),
            "avg_energy_per_fusion": _mean(energies),
            "avg_tokens_per_fusion": _mean(tokens),
            "modality_usage": {
                m.value: c for m, c in usage.items()
            },
            "most_used_modality": most_used,
            "total_energy": sum(energies),
            "total_tokens": sum(tokens),
            "numpy_available": _NUMPY_AVAILABLE,
            "history_capacity": self._config.max_history,
            "uptime_seconds": time.time() - self._started_at,
        }

    def statistics(self) -> Dict[str, Any]:
        return self.get_fusion_statistics()

    def optimize_fusion_weights(self) -> Dict[ModalityType, float]:
        """
        Suggest rebalanced modality weights from observed usage.

        Returns a new dict summing to 1.0. Does **not** mutate
        ``self.modality_weights``.
        """
        with self._lock:
            usage = dict(self.modality_usage)
        total_usage = sum(usage.values())

        if total_usage == 0:
            return dict(self.modality_weights)

        optimized: Dict[ModalityType, float] = {}
        for modality, count in usage.items():
            usage_ratio = count / total_usage
            current = self.modality_weights.get(modality, 0.0)
            optimized[modality] = current * (
                1.0 + usage_ratio * 0.2
            )

        total_weight = sum(optimized.values())
        if total_weight <= 0:
            n = max(1, len(optimized))
            return {m: 1.0 / n for m in optimized}

        optimized = {
            m: w / total_weight for m, w in optimized.items()
        }
        logger.debug(
            "optimize_fusion_weights suggested: %s",
            {m.value: round(w, 4) for m, w in optimized.items()},
        )
        return optimized

    # ---------------------------------------------------------- internals
    def _coerce_fusion_method(self, value: Any) -> FusionMethod:
        if isinstance(value, FusionMethod):
            return value
        if isinstance(value, str):
            try:
                return FusionMethod(value)
            except ValueError:
                msg = f"unknown fusion_method {value!r}."
                if self._strict:
                    raise MultimodalFusionError(msg)
                logger.warning(
                    "%s Falling back to weighted_sum.", msg,
                )
                return FusionMethod.WEIGHTED_SUM
        msg = (
            f"fusion_method must be a FusionMethod or its string "
            f"value, got {type(value).__name__}."
        )
        if self._strict:
            raise MultimodalFusionError(msg)
        logger.warning(
            "%s Falling back to weighted_sum.", msg,
        )
        return FusionMethod.WEIGHTED_SUM

    def _apply_fusion(
        self,
        modality_scores: Mapping[ModalityType, ModalityScore],
        method: FusionMethod,
        energy_budget: Optional[float],
    ) -> float:
        if method == FusionMethod.WEIGHTED_SUM:
            return self._weighted_sum_fusion(modality_scores)
        elif method == FusionMethod.MAX:
            return self._max_fusion(modality_scores)
        else:  # ADAPTIVE
            return self._adaptive_fusion(
                modality_scores, energy_budget,
            )

    def _extract_modality_scores(
        self, result: Mapping[str, Any],
    ) -> Dict[ModalityType, ModalityScore]:
        """
        Extract modality scores from a raw result dict.

        Strict mode rejects out-of-range scores; non-strict clamps.
        Non-finite scores are dropped in either mode (matching the
        historical behaviour for confidence/energy).
        """
        cfg = self._config
        scores: Dict[ModalityType, ModalityScore] = {}
        for modality in ModalityType:
            key = modality.value
            if key not in result:
                continue
            payload = result[key]

            if isinstance(payload, Mapping):
                try:
                    relevance = float(
                        payload.get("score", 0.0)
                    )
                except (TypeError, ValueError):
                    relevance = 0.0
                try:
                    confidence = float(
                        payload.get(
                            "confidence", cfg.default_confidence,
                        )
                    )
                except (TypeError, ValueError):
                    confidence = cfg.default_confidence
                try:
                    energy = float(payload.get("energy", 0.0))
                except (TypeError, ValueError):
                    energy = 0.0
                try:
                    tokens = int(payload.get("tokens", 0))
                except (TypeError, ValueError):
                    tokens = 0
            elif (
                isinstance(payload, (int, float))
                and not isinstance(payload, bool)
            ):
                relevance = float(payload)
                confidence = cfg.default_confidence
                default_energy, default_tokens = (
                    cfg.modality_cost(modality)
                )
                energy = default_energy
                tokens = default_tokens
            else:
                msg = (
                    f"result[{key!r}] must be a Mapping or "
                    f"numeric, got {type(payload).__name__}."
                )
                if self._strict:
                    raise MultimodalFusionError(msg)
                logger.warning(
                    "%s Skipping modality.", msg,
                )
                continue

            # ---- Relevance ------------------------------------------
            if not math.isfinite(relevance):
                msg = (
                    f"result[{key!r}].score is non-finite: "
                    f"{relevance}."
                )
                if self._strict:
                    raise MultimodalFusionError(msg)
                logger.warning("%s Skipping.", msg)
                continue
            if not 0.0 <= relevance <= 1.0:
                msg = (
                    f"result[{key!r}].score must be in [0, 1], "
                    f"got {relevance}."
                )
                if self._strict:
                    raise MultimodalFusionError(msg)
                logger.warning("%s Clamping.", msg)
                relevance = max(0.0, min(1.0, relevance))

            # ---- Confidence -----------------------------------------
            if not math.isfinite(confidence):
                logger.debug(
                    "result[%r].confidence non-finite; "
                    "using default_confidence %.3f.",
                    key, cfg.default_confidence,
                )
                confidence = cfg.default_confidence
            if not 0.0 <= confidence <= 1.0:
                if self._strict:
                    raise MultimodalFusionError(
                        f"result[{key!r}].confidence must be in "
                        f"[0, 1], got {confidence}."
                    )
                logger.warning(
                    "Clamping result[%r].confidence %.3f to "
                    "[0, 1].", key, confidence,
                )
                confidence = max(0.0, min(1.0, confidence))

            # ---- Energy ---------------------------------------------
            if not math.isfinite(energy) or energy < 0:
                energy = 0.0
            if tokens < 0:
                tokens = 0

            try:
                scores[modality] = ModalityScore(
                    modality=modality,
                    relevance_score=relevance,
                    confidence=confidence,
                    energy_cost=energy,
                    token_cost=tokens,
                )
            except MultimodalFusionError:
                if self._strict:
                    raise
                logger.warning(
                    "Dropping malformed ModalityScore for %s.",
                    key,
                )
        return scores

    def _weighted_sum_fusion(
        self,
        modality_scores: Mapping[ModalityType, ModalityScore],
    ) -> float:
        """
        Weighted-sum fusion with per-modality energy penalty.

        Confidence scales the relevance contribution, matching
        :meth:`_adaptive_fusion`. A ``ModalityScore`` with
        ``confidence=0.5`` contributes half of what an identical
        score with ``confidence=1.0`` contributes.
        """
        cfg = self._config
        total_score = 0.0
        total_weight = 0.0

        for modality, score in modality_scores.items():
            weight = self.modality_weights.get(modality, 0.0)
            penalty = 1.0 - (
                score.energy_cost * self.energy_penalty
            )
            penalty = max(cfg.min_energy_penalty, penalty)
            total_score += (
                score.relevance_score
                * score.confidence
                * weight
                * penalty
            )
            total_weight += weight

        return total_score / max(total_weight, cfg.epsilon)

    def _max_fusion(
        self,
        modality_scores: Mapping[ModalityType, ModalityScore],
    ) -> float:
        """Take the single highest *confidence-scaled* relevance."""
        if not modality_scores:
            return 0.0
        return max(
            s.relevance_score * s.confidence
            for s in modality_scores.values()
        )

    def _adaptive_fusion(
        self,
        modality_scores: Mapping[ModalityType, ModalityScore],
        energy_budget: Optional[float],
    ) -> float:
        """
        Confidence-weighted fusion with inverse-energy factor.

        Returns the weighted average of confidence-scaled relevance,
        clamped to ``[0, 1]``.
        """
        if not modality_scores:
            return 0.0
        cfg = self._config
        numerators: List[float] = []
        denominators: List[float] = []
        for _modality, score in modality_scores.items():
            energy_weight = 1.0 / max(
                score.energy_cost, cfg.epsilon,
            )
            if (
                energy_budget is not None
                and score.energy_cost > energy_budget
            ):
                energy_weight *= 0.5
            numerators.append(
                score.relevance_score
                * score.confidence
                * energy_weight
            )
            denominators.append(energy_weight)
        total = sum(denominators)
        if total <= cfg.epsilon:
            return 0.0
        return max(
            0.0, min(1.0, sum(numerators) / total),
        )

    def _reduce_modalities(
        self,
        modality_scores: Dict[ModalityType, ModalityScore],
        energy_budget: Optional[float],
        token_budget: Optional[int],
    ) -> Tuple[Dict[ModalityType, ModalityScore], List[str]]:
        """
        Trim the modality set to fit the given budgets.

        Modalities are ordered by ``relevance / (energy + eps)``
        descending; each is kept while both energy and token budgets
        allow. Returns ``(kept, dropped_names)``.
        """
        cfg = self._config
        ordered = sorted(
            modality_scores.items(),
            key=lambda kv: (
                -(
                    kv[1].relevance_score
                    / (kv[1].energy_cost + cfg.epsilon)
                ),
                kv[0].value,
            ),
        )

        reduced: Dict[ModalityType, ModalityScore] = {}
        total_energy = 0.0
        total_tokens = 0
        dropped: List[str] = []
        for modality, score in ordered:
            next_energy = total_energy + score.energy_cost
            next_tokens = total_tokens + score.token_cost
            fits_energy = (
                energy_budget is None
                or next_energy <= energy_budget
            )
            fits_tokens = (
                token_budget is None
                or next_tokens <= token_budget
            )
            if fits_energy and fits_tokens:
                reduced[modality] = score
                total_energy = next_energy
                total_tokens = next_tokens
            else:
                dropped.append(modality.value)

        if dropped:
            logger.debug(
                "Reduced modalities; dropped=%s, "
                "energy=%.4f, tokens=%d.",
                dropped, total_energy, total_tokens,
            )
        return reduced, dropped

    def _score_modality_relevance(
        self, query: str, modalities: List[ModalityType],
    ) -> Dict[ModalityType, float]:
        """
        Score modalities by query keyword presence (config-driven).
        """
        cfg = self._config
        query_lower = query.lower()
        scores: Dict[ModalityType, float] = {}

        for modality in modalities:
            if modality == ModalityType.TEXT:
                scores[modality] = cfg.default_text_relevance
            elif modality == ModalityType.VISUAL:
                if any(
                    kw in query_lower
                    for kw in cfg.visual_keywords
                ):
                    scores[modality] = cfg.visual_keyword_relevance
                else:
                    scores[modality] = cfg.visual_default_relevance
            elif modality == ModalityType.AUDIO:
                if any(
                    kw in query_lower
                    for kw in cfg.audio_keywords
                ):
                    scores[modality] = cfg.audio_keyword_relevance
                else:
                    scores[modality] = cfg.audio_default_relevance
            else:
                scores[modality] = (
                    cfg.multimodal_default_relevance
                )

        return scores

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_history: bool = True) -> int:
        with self._lock:
            removed = len(self.fusion_history)
            if clear_history:
                self.fusion_history.clear()
            for m in self.modality_usage:
                self.modality_usage[m] = 0
            self._started_at = time.time()
        logger.debug(
            "MultimodalFusion reset (removed %d entry(ies)).",
            removed,
        )
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_history: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "version": VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "modality_weights": {
                    m.value: w
                    for m, w in self.modality_weights.items()
                },
                "energy_penalty": self.energy_penalty,
                "modality_usage": {
                    m.value: c
                    for m, c in self.modality_usage.items()
                },
                "started_at": self._started_at,
            }
            if include_history:
                payload["fusion_history"] = list(
                    self.fusion_history
                )
        return payload

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "MultimodalFusion":
        if not isinstance(data, Mapping):
            raise MultimodalFusionError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        try:
            cfg = MultimodalFusionConfig.from_dict(
                dict(data.get("config", {}) or {})
            )

            strict_raw = data.get("strict", True)
            if not isinstance(strict_raw, bool):
                raise MultimodalFusionError(
                    f"strict must be bool, got "
                    f"{type(strict_raw).__name__}."
                )

            fusion = cls(config=cfg, strict=strict_raw)

            with fusion._lock:
                # ---- Live weights -------------------------------
                weights_in = data.get("modality_weights")
                if isinstance(weights_in, Mapping):
                    for key, value in weights_in.items():
                        try:
                            modality = ModalityType(str(key))
                        except ValueError:
                            continue
                        try:
                            fv = float(value)
                        except (TypeError, ValueError) as exc:
                            raise MultimodalFusionError(
                                f"invalid modality weight for "
                                f"{key!r}: {exc}"
                            ) from exc
                        if not math.isfinite(fv) or fv < 0:
                            raise MultimodalFusionError(
                                f"modality weight for {key!r} "
                                f"must be finite and >= 0, "
                                f"got {fv}."
                            )
                        fusion.modality_weights[modality] = fv

                # ---- Live energy_penalty ------------------------
                if "energy_penalty" in data:
                    try:
                        ep = float(data["energy_penalty"])
                    except (TypeError, ValueError) as exc:
                        raise MultimodalFusionError(
                            f"invalid energy_penalty: {exc}"
                        ) from exc
                    if not math.isfinite(ep) or ep < 0:
                        raise MultimodalFusionError(
                            f"energy_penalty must be finite and "
                            f">= 0, got {ep}."
                        )
                    fusion.energy_penalty = ep

                # ---- Usage counters -----------------------------
                usage_in = data.get("modality_usage")
                if isinstance(usage_in, Mapping):
                    for key, value in usage_in.items():
                        try:
                            modality = ModalityType(str(key))
                        except ValueError:
                            continue
                        try:
                            iv = int(value)
                        except (TypeError, ValueError) as exc:
                            raise MultimodalFusionError(
                                f"invalid modality_usage for "
                                f"{key!r}: {exc}"
                            ) from exc
                        if iv < 0:
                            raise MultimodalFusionError(
                                f"modality_usage for {key!r} "
                                f"must be >= 0, got {iv}."
                            )
                        fusion.modality_usage[modality] = iv

                # ---- History (cap enforced) ---------------------
                raw_history = data.get("fusion_history") or []
                if not isinstance(raw_history, list):
                    raise MultimodalFusionError(
                        f"fusion_history must be a list, got "
                        f"{type(raw_history).__name__}."
                    )
                if len(raw_history) > cfg.max_history:
                    raise MultimodalFusionError(
                        f"fusion_history has {len(raw_history)} "
                        f"entries, exceeds max_history="
                        f"{cfg.max_history}."
                    )
                for entry in raw_history:
                    if not isinstance(entry, Mapping):
                        raise MultimodalFusionError(
                            "fusion_history entries must be "
                            "Mappings."
                        )
                    fusion.fusion_history.append(dict(entry))

                # ---- started_at ---------------------------------
                try:
                    fusion._started_at = float(
                        data.get("started_at", time.time())
                    )
                except (TypeError, ValueError) as exc:
                    raise MultimodalFusionError(
                        f"invalid started_at: {exc}"
                    ) from exc
            return fusion
        except MultimodalFusionError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise MultimodalFusionError(
                f"invalid MultimodalFusion payload: {exc}"
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
    ) -> "MultimodalFusion":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MultimodalFusionError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "MultimodalFusion":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if exc_type is not None:
            logger.warning(
                "MultimodalFusion scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        # Live state — not config — is what fusion actually uses.
        with self._lock:
            return (
                "MultimodalFusion("
                f"weights={{text:{self.modality_weights.get(ModalityType.TEXT, 0.0):.2f}, "
                f"visual:{self.modality_weights.get(ModalityType.VISUAL, 0.0):.2f}, "
                f"audio:{self.modality_weights.get(ModalityType.AUDIO, 0.0):.2f}, "
                f"multimodal:{self.modality_weights.get(ModalityType.MULTIMODAL, 0.0):.2f}}}, "
                f"energy_penalty={self.energy_penalty:.3f}, "
                f"fusions={len(self.fusion_history)}, "
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

    fusion = MultimodalFusion()
    print("repr       :", fusion)

    results = [
        {
            "content_id": "doc_a",
            "text": {
                "score": 0.9, "confidence": 0.95,
                "energy": 0.001, "tokens": 400,
            },
            "visual": {
                "score": 0.7, "confidence": 0.85,
                "energy": 0.002, "tokens": 200,
            },
        },
        {
            "content_id": "doc_b",
            "text": {
                "score": 0.75, "confidence": 0.9,
                "energy": 0.001, "tokens": 350,
            },
        },
        {
            "content_id": "doc_c",
            "text": {
                "score": 0.6, "confidence": 0.8,
                "energy": 0.001, "tokens": 300,
            },
            "visual": {
                "score": 0.95, "confidence": 0.9,
                "energy": 0.002, "tokens": 250,
            },
            "audio": {
                "score": 0.4, "confidence": 0.7,
                "energy": 0.0015, "tokens": 150,
            },
        },
    ]

    fused = fusion.fuse_multimodal_results(results)
    print("weighted   :", [
        f"{f.content_id}={f.fused_score:.3f}" for f in fused
    ])
    assert fused[0].content_id in ("doc_a", "doc_c")

    # ---- Confidence is used by weighted_sum --------------------- #
    high_conf = fusion.fuse_multimodal_results([{
        "content_id": "hc",
        "text": {
            "score": 0.8, "confidence": 1.0,
            "energy": 0.001, "tokens": 100,
        },
    }])[0]
    low_conf = fusion.fuse_multimodal_results([{
        "content_id": "lc",
        "text": {
            "score": 0.8, "confidence": 0.5,
            "energy": 0.001, "tokens": 100,
        },
    }])[0]
    assert high_conf.fused_score > low_conf.fused_score, (
        f"confidence ignored: hc={high_conf.fused_score:.4f} "
        f"lc={low_conf.fused_score:.4f}"
    )
    print("confidence : weighted_sum uses it")

    # ---- Token budget reduction -------------------------------- #
    token_budget_fused = fusion.fuse_multimodal_results(
        results, token_budget=100,
    )
    for f in token_budget_fused:
        assert f.total_tokens <= 100, (
            f"token budget exceeded: {f.total_tokens}"
        )
    print("token budget: OK")

    # ---- Both budgets together --------------------------------- #
    both_fused = fusion.fuse_multimodal_results(
        results, energy_budget=0.0015, token_budget=500,
    )
    for f in both_fused:
        assert f.total_energy <= 0.0015 + 1e-9
        assert f.total_tokens <= 500
    print("both budgets: OK")

    # ---- dropped_modalities recorded ---------------------------- #
    trimmed = fusion.fuse_multimodal_results(
        results, energy_budget=0.0015,
    )
    assert any(
        f.dropped_modalities for f in trimmed
    ), "expected some dropped modalities"
    print("dropped     : recorded in FusedResult")

    # ---- Backward-compatible string method --------------------- #
    fused_str = fusion.fuse_multimodal_results(
        results, fusion_method="max",
    )
    print("max         :", [
        f"{f.content_id}={f.fused_score:.3f}"
        for f in fused_str
    ])

    # ---- Adaptive method --------------------------------------- #
    fused_adapt = fusion.fuse_multimodal_results(
        results, fusion_method=FusionMethod.ADAPTIVE,
    )
    print("adaptive    :", [
        f"{f.content_id}={f.fused_score:.3f}"
        for f in fused_adapt
    ])

    # ---- Bare-numeric payload ---------------------------------- #
    bare = fusion.fuse_multimodal_results([{
        "content_id": "bare", "text": 0.8, "visual": 0.6,
    }])
    print("bare        :", bare[0])

    # ---- Adaptive modality selection --------------------------- #
    selected = fusion.adaptive_modality_selection(
        "find me an image and an audio clip",
        available_modalities=list(ModalityType),
        energy_budget=0.005,
        token_budget=800,
    )
    print("selected    :", [m.value for m in selected])
    assert (
        ModalityType.TEXT in selected
        or ModalityType.VISUAL in selected
    )

    # ---- adaptive selection updates usage --------------------- #
    usage_before = dict(fusion.modality_usage)
    fusion.adaptive_modality_selection(
        "audio please",
        available_modalities=[ModalityType.AUDIO],
        energy_budget=0.01,
        token_budget=1000,
    )
    assert (
        fusion.modality_usage[ModalityType.AUDIO]
        > usage_before[ModalityType.AUDIO]
    )
    print("usage       : adaptive selection counted")

    # ---- Statistics -------------------------------------------- #
    stats = fusion.get_fusion_statistics()
    print("stats       :", {
        k: v for k, v in stats.items()
        if k not in ("modality_usage", "uptime_seconds")
    })

    # ---- Optimize doesn't mutate live weights ------------------ #
    before_weights = dict(fusion.modality_weights)
    optimized = fusion.optimize_fusion_weights()
    assert fusion.modality_weights == before_weights
    assert abs(sum(optimized.values()) - 1.0) < 1e-6
    print("no-mutate   : OK")

    # ---- energy_penalty survives round-trip -------------------- #
    fusion.energy_penalty = 0.5
    payload = fusion.to_json(include_history=True)
    restored = MultimodalFusion.from_json(payload)
    assert restored.energy_penalty == 0.5, restored.energy_penalty
    assert (
        restored.to_dict(include_history=True)
        == fusion.to_dict(include_history=True)
    )
    print("energy_pen. : round-trip OK")

    # ---- Custom modality weight survives round-trip ------------ #
    fusion.modality_weights[ModalityType.TEXT] = 0.9
    rt = MultimodalFusion.from_dict(
        fusion.to_dict(include_history=True),
    )
    assert (
        rt.modality_weights[ModalityType.TEXT] == 0.9
    )
    print("weights     : round-trip OK")

    # ---- Bounded history --------------------------------------- #
    bounded = MultimodalFusion(
        config=MultimodalFusionConfig(max_history=2)
    )
    for i in range(10):
        bounded.fuse_multimodal_results([{
            "content_id": f"d{i}",
            "text": {
                "score": 0.5, "energy": 0.001, "tokens": 100,
            },
        }])
    assert bounded.history_size == 2
    print("bounded     : OK")

    # ---- from_dict rejects over-cap history -------------------- #
    try:
        MultimodalFusion.from_dict({
            "config": MultimodalFusionConfig(
                max_history=2,
            ).to_dict(),
            "fusion_history": [
                {"method": "max", "score": 0.5}
                for _ in range(10)
            ],
        })
    except MultimodalFusionError as exc:
        print("Rejected over-cap:", str(exc)[:50])

    # ---- from_dict rejects negative weights -------------------- #
    try:
        MultimodalFusion.from_dict({
            "config": {},
            "modality_weights": {"text": -1.0},
        })
    except MultimodalFusionError as exc:
        print("Rejected weights :", str(exc)[:50])

    # ---- from_dict rejects negative energy_penalty ------------- #
    try:
        MultimodalFusion.from_dict({
            "config": {},
            "energy_penalty": -1.0,
        })
    except MultimodalFusionError as exc:
        print("Rejected ep      :", str(exc)[:50])

    # ---- from_dict rejects bad strict type --------------------- #
    try:
        MultimodalFusion.from_dict({
            "config": {},
            "strict": "yes",
        })
    except MultimodalFusionError as exc:
        print("Rejected strict  :", str(exc)[:50])

    # ---- from_dict null history treated as empty --------------- #
    null_history = MultimodalFusion.from_dict({
        "config": {}, "fusion_history": None,
    })
    assert null_history.history_size == 0
    print("null history: OK")

    # ---- FusedResult / ModalityScore round-trip ---------------- #
    fr = fused[0]
    fr_rt = FusedResult.from_dict(fr.to_dict())
    assert fr_rt.to_dict() == fr.to_dict()
    ms = list(fr.modality_scores.values())[0]
    ms_rt = ModalityScore.from_dict(ms.to_dict())
    assert ms_rt.to_dict() == ms.to_dict()
    print("dataclass   : round-trip OK")

    # ---- Non-strict container returns [] ---------------------- #
    lenient = MultimodalFusion(strict=False)
    assert lenient.fuse_multimodal_results(
        "not-a-list",  # type: ignore[arg-type]
    ) == []
    print("non-strict  : container check → []")

    # ---- Strict out-of-range score rejected ------------------- #
    strict = MultimodalFusion(strict=True)
    try:
        strict.fuse_multimodal_results([{
            "content_id": "x",
            "text": {"score": 1.5},
        }])
    except MultimodalFusionError as exc:
        print("Rejected score :", str(exc)[:50])

    # ---- Non-strict clamps out-of-range ----------------------- #
    out = lenient.fuse_multimodal_results([{
        "content_id": "x",
        "text": {"score": 1.5},
    }])
    assert out and out[0].fused_score <= 1.0
    print("non-strict  : score clamped")

    # ---- Non-finite confidence uses default ------------------- #
    nan_conf = fusion.fuse_multimodal_results([{
        "content_id": "nan",
        "text": {
            "score": 0.8, "confidence": float("nan"),
            "energy": 0.001, "tokens": 100,
        },
    }])[0]
    # default_confidence = 0.0, so fused score should be ~0.
    assert nan_conf.fused_score < 0.1, nan_conf.fused_score
    print("nan conf    : default to worst-case")

    # ---- Non-strict malformed payload ------------------------- #
    lenient.fuse_multimodal_results([{"content_id": "x"}])
    lenient.fuse_multimodal_results([{
        "content_id": "y",
        "text": {"score": float("nan")},
    }])
    print("lenient     : malformed payloads skipped")

    # ---- Config validation ------------------------------------ #
    for bad_cfg in (
        dict(text_weight=-0.1),
        dict(text_weight=float("nan")),
        dict(energy_penalty_factor=-0.1),
        dict(min_energy_penalty=-0.1),
        dict(min_energy_penalty=1.5),
        dict(epsilon=0),
        dict(epsilon=float("nan")),
        dict(text_energy=-0.1),
        dict(text_tokens=-1),
        dict(text_tokens=True),
        dict(default_text_relevance=1.5),
        dict(default_confidence=-0.1),
        dict(visual_keywords="not-a-tuple"),
        dict(max_history=0),
    ):
        try:
            MultimodalFusionConfig(**bad_cfg)  # type: ignore[arg-type]
        except MultimodalFusionError as exc:
            print("Rejected cfg:", exc)

    # ---- Strict-mode argument validation ---------------------- #
    for bad_call in (
        lambda: strict.fuse_multimodal_results(
            [{"content_id": "x"}], fusion_method="bogus",
        ),
        lambda: strict.fuse_multimodal_results(
            [{"content_id": "x"}], energy_budget=-1.0,
        ),
        lambda: strict.fuse_multimodal_results(
            [{"content_id": "x"}], token_budget=-1,
        ),
        lambda: strict.fuse_multimodal_results(
            [{"content_id": "x", "text": "not-a-dict"}],
        ),
        lambda: strict.adaptive_modality_selection(
            "", list(ModalityType), 0.01, 100,
        ),
        lambda: strict.adaptive_modality_selection(
            "q", "not-a-list", 0.01, 100,  # type: ignore[arg-type]
        ),
        lambda: strict.adaptive_modality_selection(
            "q", ["text"], 0.01, 100,  # type: ignore[list-item]
        ),
        lambda: strict.adaptive_modality_selection(
            "q", list(ModalityType), -0.1, 100,
        ),
        lambda: strict.adaptive_modality_selection(
            "q", list(ModalityType), 0.01, -1,
        ),
    ):
        try:
            bad_call()
        except MultimodalFusionError as exc:
            print("Rejected    :", exc)

    # ---- FusedResult validation ------------------------------- #
    try:
        FusedResult(
            content_id="x",
            modalities=(ModalityType.TEXT,),
            fused_score=0.5,
            modality_scores=[  # type: ignore[arg-type]
                ("text", ms),
            ],
            total_energy=0.001,
            total_tokens=100,
            fusion_method="weighted_sum",
        )
    except MultimodalFusionError as exc:
        print("Rejected FusedResult:", str(exc)[:50])

    # ---- to_json rejects NaN ---------------------------------- #
    tainted = MultimodalFusion()
    tainted.fuse_multimodal_results([{
        "content_id": "x",
        "text": {
            "score": 0.5, "energy": 0.001, "tokens": 100,
        },
    }])
    object.__setattr__(
        tainted, "energy_penalty", float("nan"),
    )
    try:
        tainted.to_json()
    except ValueError as exc:
        print("NaN in to_json:", str(exc)[:50])

    # ---- Context manager -------------------------------------- #
    with MultimodalFusion() as scoped:
        scoped.fuse_multimodal_results([{
            "content_id": "x",
            "text": {
                "score": 0.5, "energy": 0.001, "tokens": 100,
            },
        }])
    print("context     : OK")

    print("\nSmoke test passed.")
