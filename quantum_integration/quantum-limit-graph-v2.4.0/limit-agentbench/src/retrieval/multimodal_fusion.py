# src/retrieval/multimodal_fusion.py

"""
Multimodal Fusion Module
========================

Fuses text, visual, audio, and multimodal retrieval results with
energy-aware weighting. Inspired by VimRAG's multimodal memory graph
navigation.

Enhancements
------------
- ``MultimodalFusionConfig`` — frozen, validated: per-modality weights,
  energy penalty, cost table, keyword maps, bounded history.
- ``FusionMethod`` enum — replaces the string ``fusion_method`` argument
  while accepting strings for backward compatibility.
- ``ModalityScore`` / ``FusedResult`` — frozen dataclasses with full
  validation and serialization.
- **Optional NumPy** — module imports cleanly without numpy.
- **Thread safety** — ``RLock`` guards counters and history.
- **Bounded fusion history** — ``deque(maxlen=config.max_history)``.
- **Fixed `_extract_modality_scores`** — handles non-Mapping payloads.
- **Fixed `0.0001` zero-guard** — proper ``epsilon`` from config.
- **Deterministic tie-breaking** on fusion ties.
- **Full validation** of every argument; strict / non-strict modes.
- **Serialization** — ``to_dict`` / ``from_dict`` / ``to_json`` on the
  fusion engine and every dataclass.
- ``statistics()``, ``__repr__``, custom ``MultimodalFusionError``,
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
from typing import Any, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional NumPy
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import numpy as np  # type: ignore

    _NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False
    logger.debug("numpy not importable; using pure-Python mean.")


def _mean(values: Sequence[float]) -> float:
    """Return the arithmetic mean, using NumPy when available."""
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

    Centralizes every previously hardcoded coefficient: modality weights,
    energy penalty, per-modality costs, relevance keywords, and bounded
    history.
    """

    # Modality weights. Only the primary two are user-tunable; the rest have
    # sensible defaults that can be overridden via ``extra_weights``.
    text_weight: float = 0.6
    visual_weight: float = 0.4
    audio_weight: float = 0.3
    multimodal_weight: float = 1.0

    # Energy penalty applied to each modality's relevance.
    energy_penalty_factor: float = 0.1
    # Floor for the energy penalty so it never goes negative.
    min_energy_penalty: float = 0.1

    # Epsilon used to avoid zero-division in ratio-based selection.
    epsilon: float = 1e-6

    # Per-modality default costs (energy_wh, tokens).
    text_energy: float = 0.001
    text_tokens: int = 500
    visual_energy: float = 0.0025
    visual_tokens: int = 256
    audio_energy: float = 0.002
    audio_tokens: int = 300
    multimodal_energy: float = 0.004
    multimodal_tokens: int = 800

    # Query keyword maps for modality relevance scoring.
    visual_keywords: tuple = (
        "image", "visual", "picture", "diagram", "photo", "chart",
    )
    audio_keywords: tuple = (
        "audio", "sound", "speech", "voice", "listen", "recording",
    )

    # Relevance scores when keywords are present / absent.
    default_text_relevance: float = 0.8
    visual_keyword_relevance: float = 0.9
    visual_default_relevance: float = 0.3
    audio_keyword_relevance: float = 0.9
    audio_default_relevance: float = 0.2
    multimodal_default_relevance: float = 0.5

    # Bounded history.
    max_history: int = 10_000

    def __post_init__(self) -> None:
        for name in (
            "text_weight", "visual_weight", "audio_weight", "multimodal_weight",
        ):
            value = getattr(self, name)
            if value < 0:
                raise MultimodalFusionError(f"{name} must be >= 0.")
        if self.energy_penalty_factor < 0:
            raise MultimodalFusionError(
                "energy_penalty_factor must be >= 0."
            )
        if not 0.0 <= self.min_energy_penalty <= 1.0:
            raise MultimodalFusionError(
                "min_energy_penalty must be in [0, 1]."
            )
        if self.epsilon <= 0:
            raise MultimodalFusionError("epsilon must be > 0.")
        for name in (
            "text_energy", "visual_energy", "audio_energy", "multimodal_energy",
        ):
            value = getattr(self, name)
            if value < 0:
                raise MultimodalFusionError(f"{name} must be >= 0.")
        for name in (
            "text_tokens", "visual_tokens", "audio_tokens", "multimodal_tokens",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or value < 0:
                raise MultimodalFusionError(f"{name} must be >= 0.")
        for name in (
            "default_text_relevance", "visual_keyword_relevance",
            "visual_default_relevance", "audio_keyword_relevance",
            "audio_default_relevance", "multimodal_default_relevance",
        ):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise MultimodalFusionError(f"{name} must be in [0, 1].")
        if not isinstance(self.visual_keywords, tuple):
            raise MultimodalFusionError("visual_keywords must be a tuple.")
        if not isinstance(self.audio_keywords, tuple):
            raise MultimodalFusionError("audio_keywords must be a tuple.")
        if self.max_history <= 0:
            raise MultimodalFusionError("max_history must be > 0.")

    def modality_weight(self, modality: ModalityType) -> float:
        return {
            ModalityType.TEXT: self.text_weight,
            ModalityType.VISUAL: self.visual_weight,
            ModalityType.AUDIO: self.audio_weight,
            ModalityType.MULTIMODAL: self.multimodal_weight,
        }[modality]

    def modality_cost(self, modality: ModalityType) -> Tuple[float, int]:
        """Return ``(energy_wh, tokens)`` for ``modality``."""
        return {
            ModalityType.TEXT: (self.text_energy, self.text_tokens),
            ModalityType.VISUAL: (self.visual_energy, self.visual_tokens),
            ModalityType.AUDIO: (self.audio_energy, self.audio_tokens),
            ModalityType.MULTIMODAL: (
                self.multimodal_energy, self.multimodal_tokens,
            ),
        }[modality]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "MultimodalFusionConfig":
        if not isinstance(data, Mapping):
            raise MultimodalFusionError(
                f"MultimodalFusionConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        kwargs: Dict[str, Any] = {}
        for key in (
            "text_weight", "visual_weight", "audio_weight",
            "multimodal_weight", "energy_penalty_factor",
            "min_energy_penalty", "epsilon",
            "text_energy", "text_tokens",
            "visual_energy", "visual_tokens",
            "audio_energy", "audio_tokens",
            "multimodal_energy", "multimodal_tokens",
            "default_text_relevance", "visual_keyword_relevance",
            "visual_default_relevance", "audio_keyword_relevance",
            "audio_default_relevance", "multimodal_default_relevance",
            "max_history",
        ):
            if key in data:
                kwargs[key] = data[key]
        for key in ("visual_keywords", "audio_keywords"):
            if key in data:
                kwargs[key] = tuple(data[key])
        return cls(**kwargs)


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
        for name in ("relevance_score", "confidence", "energy_cost"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise MultimodalFusionError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                raise MultimodalFusionError(
                    f"{name} must be finite, got {value!r}."
                )
            if fv < 0:
                raise MultimodalFusionError(f"{name} must be >= 0.")
        if not 0.0 <= self.relevance_score <= 1.0:
            raise MultimodalFusionError(
                "relevance_score must be in [0, 1]."
            )
        if not 0.0 <= self.confidence <= 1.0:
            raise MultimodalFusionError("confidence must be in [0, 1].")
        if not isinstance(self.token_cost, int) or self.token_cost < 0:
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
    def from_dict(cls, data: Mapping[str, Any]) -> "ModalityScore":
        if not isinstance(data, Mapping):
            raise MultimodalFusionError(
                "ModalityScore.from_dict expects a Mapping."
            )
        return cls(
            modality=ModalityType(str(data["modality"])),
            relevance_score=float(data.get("relevance_score", 0.0)),
            confidence=float(data.get("confidence", 1.0)),
            energy_cost=float(data.get("energy_cost", 0.0)),
            token_cost=int(data.get("token_cost", 0)),
        )

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
        for name in ("fused_score", "total_energy"):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                raise MultimodalFusionError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                raise MultimodalFusionError(
                    f"{name} must be finite, got {value!r}."
                )
            if fv < 0:
                raise MultimodalFusionError(f"{name} must be >= 0.")
        if not isinstance(self.total_tokens, int) or self.total_tokens < 0:
            raise MultimodalFusionError(
                "total_tokens must be a non-negative int."
            )
        if not isinstance(self.fusion_method, str) or not self.fusion_method:
            raise MultimodalFusionError(
                "fusion_method must be a non-empty string."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "content_id": self.content_id,
            "modalities": [m.value for m in self.modalities],
            "fused_score": self.fused_score,
            "modality_scores": {
                m.value: s.to_dict() for m, s in self.modality_scores.items()
            },
            "total_energy": self.total_energy,
            "total_tokens": self.total_tokens,
            "fusion_method": self.fusion_method,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FusedResult":
        if not isinstance(data, Mapping):
            raise MultimodalFusionError(
                "FusedResult.from_dict expects a Mapping."
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
            content_id=str(data["content_id"]),
            modalities=tuple(
                ModalityType(str(m)) for m in data.get("modalities", [])
            ),
            fused_score=float(data.get("fused_score", 0.0)),
            modality_scores={
                ModalityType(str(k)): ModalityScore.from_dict(v)
                for k, v in (data.get("modality_scores") or {}).items()
            },
            total_energy=float(data.get("total_energy", 0.0)),
            total_tokens=int(data.get("total_tokens", 0)),
            fusion_method=str(data.get("fusion_method", "weighted_sum")),
            timestamp=timestamp,
        )

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

    Thread-safe, serializable, and bounded in memory. The original public API
    is preserved; ``fusion_method`` accepts both the :class:`FusionMethod`
    enum and its string value.

    Features
    --------
    - Energy-aware modality weighting.
    - Adaptive fusion strategies (weighted-sum / max / adaptive).
    - Carbon-conscious modality selection under energy + token budgets.
    - Token-efficient multimodal representation.
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

        # Legacy attributes preserved.
        self.modality_weights: Dict[ModalityType, float] = {
            ModalityType.TEXT: self._config.text_weight,
            ModalityType.VISUAL: self._config.visual_weight,
            ModalityType.AUDIO: self._config.audio_weight,
            ModalityType.MULTIMODAL: self._config.multimodal_weight,
        }
        self.energy_penalty: float = self._config.energy_penalty_factor

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
            "(text=%.3f, visual=%.3f, audio=%.3f, multimodal=%.3f, "
            "energy_penalty=%.3f, strict=%s)",
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
    ) -> List[FusedResult]:
        """
        Fuse multimodal retrieval results.

        Parameters
        ----------
        results : list of Mapping
            Each entry may contain one or more keys named after a
            :class:`ModalityType` value (``"text"``, ``"visual"``, etc.).
            The value under each key is either a Mapping with ``score``,
            ``confidence``, ``energy``, and ``tokens`` keys, or a bare
            numeric score.
        fusion_method : FusionMethod | str
            ``"weighted_sum"``, ``"max"``, or ``"adaptive"``.
        energy_budget : float, optional
            If provided, per-result modalities are reduced (dropping the
            least cost-effective) until the total fits the budget.

        Returns
        -------
        list of FusedResult
            Sorted by descending ``fused_score``.
        """
        if not isinstance(results, Sequence) or isinstance(results, (str, bytes)):
            raise MultimodalFusionError(
                f"results must be a list of mappings, "
                f"got {type(results).__name__}."
            )
        method = self._coerce_fusion_method(fusion_method)

        budget: Optional[float] = None
        if energy_budget is not None:
            if isinstance(energy_budget, bool) or not isinstance(
                energy_budget, (int, float)
            ):
                raise MultimodalFusionError(
                    "energy_budget must be numeric or None."
                )
            fb = float(energy_budget)
            if math.isnan(fb) or math.isinf(fb) or fb < 0:
                raise MultimodalFusionError(
                    f"energy_budget must be finite and >= 0, got "
                    f"{energy_budget!r}."
                )
            budget = fb

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
                modality_scores = self._extract_modality_scores(result)
            except MultimodalFusionError:
                if self._strict:
                    raise
                logger.warning("Skipping malformed result %d.", idx)
                continue

            if not modality_scores:
                logger.debug("Result %d has no recognized modalities.", idx)
                continue

            # ---- Reduce under budget BEFORE scoring so the fused score
            # reflects the truncated modality set.
            if budget is not None and budget > 0:
                modality_scores = self._reduce_modalities(
                    modality_scores, budget,
                )
                if not modality_scores:
                    logger.debug(
                        "Result %d dropped: no modality fits budget %.4f.",
                        idx, budget,
                    )
                    continue

            fused_score = self._apply_fusion(modality_scores, method, budget)
            total_energy = sum(s.energy_cost for s in modality_scores.values())
            total_tokens = sum(s.token_cost for s in modality_scores.values())

            fused = FusedResult(
                content_id=str(result.get("content_id", "unknown")),
                modalities=tuple(modality_scores.keys()),
                fused_score=fused_score,
                modality_scores=dict(modality_scores),
                total_energy=total_energy,
                total_tokens=total_tokens,
                fusion_method=method.value,
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
                    "modalities": [m.value for m in modality_scores.keys()],
                })

        # Deterministic tie-breaking: score desc, then content_id asc.
        fused_results.sort(key=lambda f: (-f.fused_score, f.content_id))

        logger.debug(
            "Fused %d/%d result(s) using method=%s (budget=%s).",
            len(fused_results), len(results), method.value,
            f"{budget:.4f}" if budget is not None else "none",
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

        Modalities are ranked by ``relevance / energy_cost`` and added to the
        selection while both the energy and token budgets allow.
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
                    f"available_modalities[{i}] must be a ModalityType, "
                    f"got {type(m).__name__}."
                )
        if isinstance(energy_budget, bool) or not isinstance(
            energy_budget, (int, float)
        ):
            raise MultimodalFusionError("energy_budget must be numeric.")
        fb = float(energy_budget)
        if math.isnan(fb) or math.isinf(fb) or fb < 0:
            raise MultimodalFusionError(
                "energy_budget must be finite and >= 0."
            )
        if not isinstance(token_budget, int) or token_budget < 0:
            raise MultimodalFusionError(
                "token_budget must be a non-negative int."
            )

        remaining_energy = fb
        remaining_tokens = token_budget

        modality_relevance = self._score_modality_relevance(
            query, available_modalities,
        )

        # Rank by relevance / (energy + epsilon) so cheap modalities are
        # preferred on ties.
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
            if energy <= remaining_energy and tokens <= remaining_tokens:
                selected.append(modality)
                remaining_energy -= energy
                remaining_tokens -= tokens

        logger.debug(
            "adaptive_modality_selection: query=%r selected=%s "
            "(budget energy=%.4f tokens=%d, remaining energy=%.4f tokens=%d)",
            query,
            [m.value for m in selected],
            fb, token_budget, remaining_energy, remaining_tokens,
        )
        return selected

    def get_fusion_statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics about the fusion history."""
        with self._lock:
            history = list(self.fusion_history)
            usage = dict(self.modality_usage)

        if not history:
            return {
                "status": "no_fusions",
                "total_fusions": 0,
                "modality_usage": {m.value: c for m, c in usage.items()},
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
            "modality_usage": {m.value: c for m, c in usage.items()},
            "most_used_modality": most_used,
            # Additive:
            "total_energy": sum(energies),
            "total_tokens": sum(tokens),
            "numpy_available": _NUMPY_AVAILABLE,
            "history_capacity": self._config.max_history,
            "uptime_seconds": time.time() - self._started_at,
        }

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`get_fusion_statistics`."""
        return self.get_fusion_statistics()

    def optimize_fusion_weights(self) -> Dict[ModalityType, float]:
        """
        Suggest rebalanced modality weights based on observed usage.

        Returns a new weights dict summing to 1.0. Does **not** mutate
        ``self.modality_weights`` — callers must apply the result explicitly.
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
            optimized[modality] = current * (1.0 + usage_ratio * 0.2)

        total_weight = sum(optimized.values())
        if total_weight <= 0:
            # Degenerate: fall back to uniform.
            n = max(1, len(optimized))
            return {m: 1.0 / n for m in optimized}

        optimized = {m: w / total_weight for m, w in optimized.items()}

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
                logger.warning("%s Falling back to weighted_sum.", msg)
                return FusionMethod.WEIGHTED_SUM
        msg = (
            f"fusion_method must be a FusionMethod or its string value, "
            f"got {type(value).__name__}."
        )
        if self._strict:
            raise MultimodalFusionError(msg)
        logger.warning("%s Falling back to weighted_sum.", msg)
        return FusionMethod.WEIGHTED_SUM

    def _apply_fusion(
        self,
        modality_scores: Mapping[ModalityType, ModalityScore],
        method: FusionMethod,
        energy_budget: Optional[float],
    ) -> float:
        if method == FusionMethod.WEIGHTED_SUM:
            return self._weighted_sum_fusion(modality_scores)
        if method == FusionMethod.MAX:
            return self._max_fusion(modality_scores)
        return self._adaptive_fusion(modality_scores, energy_budget)

    def _extract_modality_scores(
        self, result: Mapping[str, Any]
    ) -> Dict[ModalityType, ModalityScore]:
        """
        Extract modality scores from a raw result dict.

        Accepts either a nested Mapping (``{"score": ..., "confidence": ...}``)
        or a bare numeric score. Non-mapping payloads that are not numeric
        are rejected per strict mode.
        """
        scores: Dict[ModalityType, ModalityScore] = {}
        for modality in ModalityType:
            key = modality.value
            if key not in result:
                continue
            payload = result[key]

            if isinstance(payload, Mapping):
                try:
                    relevance = float(payload.get("score", 0.0))
                except (TypeError, ValueError):
                    relevance = 0.0
                try:
                    confidence = float(payload.get("confidence", 1.0))
                except (TypeError, ValueError):
                    confidence = 1.0
                try:
                    energy = float(payload.get("energy", 0.0))
                except (TypeError, ValueError):
                    energy = 0.0
                try:
                    tokens = int(payload.get("tokens", 0))
                except (TypeError, ValueError):
                    tokens = 0
            elif isinstance(payload, (int, float)) and not isinstance(payload, bool):
                # Bare numeric score → use defaults for the rest.
                relevance = float(payload)
                confidence = 1.0
                default_energy, default_tokens = self._config.modality_cost(modality)
                energy = default_energy
                tokens = default_tokens
            else:
                msg = (
                    f"result[{key!r}] must be a Mapping or numeric, got "
                    f"{type(payload).__name__}."
                )
                if self._strict:
                    raise MultimodalFusionError(msg)
                logger.warning("%s Skipping modality.", msg)
                continue

            # Clamp relevance / confidence to [0, 1] and reject NaN/inf.
            if math.isnan(relevance) or math.isinf(relevance):
                if self._strict:
                    raise MultimodalFusionError(
                        f"result[{key!r}].score is non-finite."
                    )
                logger.warning(
                    "Rejecting non-finite score for modality %s.", key,
                )
                continue
            relevance = max(0.0, min(1.0, relevance))
            if math.isnan(confidence) or math.isinf(confidence):
                confidence = 1.0
            confidence = max(0.0, min(1.0, confidence))
            if math.isnan(energy) or math.isinf(energy) or energy < 0:
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
                    "Dropping malformed ModalityScore for %s.", key,
                )
        return scores

    def _weighted_sum_fusion(
        self, modality_scores: Mapping[ModalityType, ModalityScore]
    ) -> float:
        """Weighted-sum fusion with a per-modality energy penalty."""
        cfg = self._config
        total_score = 0.0
        total_weight = 0.0

        for modality, score in modality_scores.items():
            weight = self.modality_weights.get(modality, 0.5)
            energy_penalty = 1.0 - (score.energy_cost * self.energy_penalty)
            energy_penalty = max(cfg.min_energy_penalty, energy_penalty)
            total_score += score.relevance_score * weight * energy_penalty
            total_weight += weight

        return total_score / max(total_weight, cfg.epsilon)

    def _max_fusion(
        self, modality_scores: Mapping[ModalityType, ModalityScore]
    ) -> float:
        """Take the single highest relevance across modalities."""
        if not modality_scores:
            return 0.0
        return max(s.relevance_score for s in modality_scores.values())

    def _adaptive_fusion(
        self,
        modality_scores: Mapping[ModalityType, ModalityScore],
        energy_budget: Optional[float],
    ) -> float:
        """
        Confidence-weighted fusion with an inverse-energy factor.

        The original returned the arithmetic mean of *raw* products, which
        could exceed 1.0. This version returns the weighted average of
        confidence-scaled relevance values, clamped to ``[0, 1]``.
        """
        if not modality_scores:
            return 0.0
        cfg = self._config
        numerators: List[float] = []
        denominators: List[float] = []
        for _modality, score in modality_scores.items():
            energy_weight = 1.0 / max(score.energy_cost, cfg.epsilon)
            if energy_budget is not None and score.energy_cost > energy_budget:
                energy_weight *= 0.5
            numerators.append(score.relevance_score * score.confidence * energy_weight)
            denominators.append(energy_weight)
        total = sum(denominators)
        if total <= cfg.epsilon:
            return 0.0
        return max(0.0, min(1.0, sum(numerators) / total))

    def _reduce_modalities(
        self,
        modality_scores: Dict[ModalityType, ModalityScore],
        energy_budget: float,
    ) -> Dict[ModalityType, ModalityScore]:
        """
        Trim the modality set to fit ``energy_budget``.

        Modalities are ordered by ``relevance_score / (energy_cost + eps)``
        descending; each is added while the budget allows. The dropped
        modalities are logged for auditability.
        """
        cfg = self._config
        ordered = sorted(
            modality_scores.items(),
            key=lambda kv: (
                -(kv[1].relevance_score / (kv[1].energy_cost + cfg.epsilon)),
                kv[0].value,
            ),
        )

        reduced: Dict[ModalityType, ModalityScore] = {}
        total_energy = 0.0
        dropped: List[str] = []
        for modality, score in ordered:
            if total_energy + score.energy_cost <= energy_budget:
                reduced[modality] = score
                total_energy += score.energy_cost
            else:
                dropped.append(modality.value)

        if dropped:
            logger.debug(
                "Reduced modalities to fit budget %.4f; dropped=%s, "
                "total_energy=%.4f.",
                energy_budget, dropped, total_energy,
            )
        return reduced

    def _estimate_modality_costs(
        self, modalities: List[ModalityType]
    ) -> Dict[ModalityType, Dict[str, float]]:
        """Return cost estimates for ``modalities`` (config-driven)."""
        out: Dict[ModalityType, Dict[str, float]] = {}
        for m in modalities:
            energy, tokens = self._config.modality_cost(m)
            out[m] = {"energy": energy, "tokens": float(tokens)}
        return out

    def _score_modality_relevance(
        self, query: str, modalities: List[ModalityType]
    ) -> Dict[ModalityType, float]:
        """
        Score modalities by query keyword presence (config-driven).

        The default text-relevance, keyword-triggered relevances, and
        default relevances are all centralized in :class:`MultimodalFusionConfig`.
        """
        cfg = self._config
        query_lower = query.lower()
        scores: Dict[ModalityType, float] = {}

        for modality in modalities:
            if modality == ModalityType.TEXT:
                scores[modality] = cfg.default_text_relevance
            elif modality == ModalityType.VISUAL:
                if any(kw in query_lower for kw in cfg.visual_keywords):
                    scores[modality] = cfg.visual_keyword_relevance
                else:
                    scores[modality] = cfg.visual_default_relevance
            elif modality == ModalityType.AUDIO:
                if any(kw in query_lower for kw in cfg.audio_keywords):
                    scores[modality] = cfg.audio_keyword_relevance
                else:
                    scores[modality] = cfg.audio_default_relevance
            else:
                scores[modality] = cfg.multimodal_default_relevance

        return scores

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_history: bool = True) -> int:
        """Reset counters and (optionally) the fusion history."""
        with self._lock:
            removed = len(self.fusion_history)
            if clear_history:
                self.fusion_history.clear()
            for m in self.modality_usage:
                self.modality_usage[m] = 0
            self._started_at = time.time()
        logger.debug("MultimodalFusion reset (removed %d entry(ies)).", removed)
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "modality_weights": {
                    m.value: w for m, w in self.modality_weights.items()
                },
                "energy_penalty": self.energy_penalty,
                "modality_usage": {
                    m.value: c for m, c in self.modality_usage.items()
                },
                "started_at": self._started_at,
            }
            if include_history:
                payload["fusion_history"] = list(self.fusion_history)
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "MultimodalFusion":
        if not isinstance(data, Mapping):
            raise MultimodalFusionError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = MultimodalFusionConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        fusion = cls(config=cfg, strict=bool(data.get("strict", True)))

        with fusion._lock:
            # Preserve a custom modality_weights dict if it was serialized.
            weights_in = data.get("modality_weights")
            if isinstance(weights_in, Mapping):
                for key, value in weights_in.items():
                    try:
                        modality = ModalityType(str(key))
                    except ValueError:
                        continue
                    fusion.modality_weights[modality] = float(value)

            usage_in = data.get("modality_usage")
            if isinstance(usage_in, Mapping):
                for key, value in usage_in.items():
                    try:
                        modality = ModalityType(str(key))
                    except ValueError:
                        continue
                    fusion.modality_usage[modality] = int(value)

            for entry in data.get("fusion_history", []):
                if isinstance(entry, Mapping):
                    fusion.fusion_history.append(dict(entry))

            fusion._started_at = float(data.get("started_at", time.time()))
        return fusion

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "MultimodalFusion":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise MultimodalFusionError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "MultimodalFusion":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "MultimodalFusion scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "MultimodalFusion("
                f"weights={{text:{self._config.text_weight:.2f}, "
                f"visual:{self._config.visual_weight:.2f}, "
                f"audio:{self._config.audio_weight:.2f}, "
                f"multimodal:{self._config.multimodal_weight:.2f}}}, "
                f"energy_penalty={self.energy_penalty:.3f}, "
                f"fusions={len(self.fusion_history)}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "FusedResult",
    "FusionMethod",
    "ModalityScore",
    "ModalityType",
    "MultimodalFusion",
    "MultimodalFusionConfig",
    "MultimodalFusionError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m retrieval.multimodal_fusion
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Happy path: weighted-sum fusion --------------------------- #
    fusion = MultimodalFusion()
    print("repr       :", fusion)

    results = [
        {
            "content_id": "doc_a",
            "text": {"score": 0.9, "confidence": 0.95, "energy": 0.001, "tokens": 400},
            "visual": {"score": 0.7, "confidence": 0.85, "energy": 0.002, "tokens": 200},
        },
        {
            "content_id": "doc_b",
            "text": {"score": 0.75, "confidence": 0.9, "energy": 0.001, "tokens": 350},
        },
        {
            "content_id": "doc_c",
            "text": {"score": 0.6, "confidence": 0.8, "energy": 0.001, "tokens": 300},
            "visual": {"score": 0.95, "confidence": 0.9, "energy": 0.002, "tokens": 250},
            "audio": {"score": 0.4, "confidence": 0.7, "energy": 0.0015, "tokens": 150},
        },
    ]

    fused = fusion.fuse_multimodal_results(results)
    print("weighted   :", [f"{f.content_id}={f.fused_score:.3f}" for f in fused])
    assert fused[0].content_id in ("doc_a", "doc_c")

    # ---- Backward-compatible string method ------------------------- #
    fused_str = fusion.fuse_multimodal_results(results, fusion_method="max")
    print("max        :", [f"{f.content_id}={f.fused_score:.3f}" for f in fused_str])

    # ---- Adaptive method ------------------------------------------- #
    fused_adapt = fusion.fuse_multimodal_results(
        results, fusion_method=FusionMethod.ADAPTIVE,
    )
    print("adaptive   :", [f"{f.content_id}={f.fused_score:.3f}" for f in fused_adapt])

    # ---- Bare-numeric modality payload ----------------------------- #
    bare = fusion.fuse_multimodal_results(
        [{"content_id": "bare", "text": 0.8, "visual": 0.6}]
    )
    print("bare       :", bare[0])

    # ---- Energy budget reduction ----------------------------------- #
    budget_fused = fusion.fuse_multimodal_results(
        results, energy_budget=0.0015,
    )
    for f in budget_fused:
        assert f.total_energy <= 0.0015 + 1e-9, (
            f"budget exceeded: {f.total_energy}"
        )
    print("budget     :", [f"{f.content_id} e={f.total_energy:.5f}" for f in budget_fused])

    # ---- Adaptive modality selection ------------------------------- #
    selected = fusion.adaptive_modality_selection(
        "find me an image and an audio clip",
        available_modalities=list(ModalityType),
        energy_budget=0.005,
        token_budget=800,
    )
    print("selected   :", [m.value for m in selected])
    assert ModalityType.TEXT in selected or ModalityType.VISUAL in selected

    # ---- Statistics ------------------------------------------------ #
    stats = fusion.get_fusion_statistics()
    print("stats      :", {
        k: v for k, v in stats.items()
        if k not in ("modality_usage", "uptime_seconds")
    })
    print("usage      :", stats["modality_usage"])

    # ---- Optimize weights ------------------------------------------ #
    optimized = fusion.optimize_fusion_weights()
    print("optimized  :", {m.value: round(w, 4) for m, w in optimized.items()})
    assert abs(sum(optimized.values()) - 1.0) < 1e-6

    # ---- Optimize doesn't mutate live state ------------------------ #
    before = dict(fusion.modality_weights)
    fusion.optimize_fusion_weights()
    assert fusion.modality_weights == before, "optimize must not mutate weights"
    print("no-mutate  : OK")

    # ---- Bounded history ------------------------------------------- #
    bounded = MultimodalFusion(
        config=MultimodalFusionConfig(max_history=2)
    )
    for i in range(10):
        bounded.fuse_multimodal_results(
            [{"content_id": f"d{i}", "text": {"score": 0.5, "energy": 0.001, "tokens": 100}}]
        )
    assert bounded.history_size == 2, bounded.history_size
    print("bounded    : OK")

    # ---- Serialization round-trip ---------------------------------- #
    payload = fusion.to_json()
    restored = MultimodalFusion.from_json(payload)
    assert restored.to_dict() == fusion.to_dict()
    print("Round-trip OK.")

    # ---- FusedResult / ModalityScore serialization ----------------- #
    fr = fused[0]
    fr_payload = json.dumps(fr.to_dict(), default=str)
    restored_fr = FusedResult.from_dict(json.loads(fr_payload))
    assert restored_fr.to_dict() == fr.to_dict()
    ms = list(fr.modality_scores.values())[0]
    ms_payload = json.dumps(ms.to_dict(), default=str)
    restored_ms = ModalityScore.from_dict(json.loads(ms_payload))
    assert restored_ms.to_dict() == ms.to_dict()
    print("Dataclass round-trips OK.")

    # ---- Context manager ------------------------------------------- #
    with MultimodalFusion() as scoped:
        scoped.fuse_multimodal_results(
            [{"content_id": "x", "text": {"score": 0.5, "energy": 0.001, "tokens": 100}}]
        )
    print("Context    : OK")

    # ---- Validation failures --------------------------------------- #
    for bad_cfg in (
        dict(text_weight=-0.1),
        dict(energy_penalty_factor=-0.1),
        dict(min_energy_penalty=-0.1),
        dict(min_energy_penalty=1.5),
        dict(epsilon=0),
        dict(text_energy=-0.1),
        dict(text_tokens=-1),
        dict(default_text_relevance=1.5),
        dict(visual_keywords="not-a-tuple"),
        dict(max_history=0),
    ):
        try:
            MultimodalFusionConfig(**bad_cfg)  # type: ignore[arg-type]
        except MultimodalFusionError as exc:
            print("Rejected cfg:", exc)

    strict = MultimodalFusion(strict=True)
    for bad_call in (
        lambda: strict.fuse_multimodal_results("not-a-list"),  # type: ignore[arg-type]
        lambda: strict.fuse_multimodal_results([{"content_id": "x"}],
                                               fusion_method="bogus"),
        lambda: strict.fuse_multimodal_results([{"content_id": "x"}],
                                               energy_budget=-1.0),
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
            print("Rejected   :", exc)

    # ---- Non-strict mode coerces ----------------------------------- #
    lenient = MultimodalFusion(strict=False)
    lenient.fuse_multimodal_results([{"content_id": "x"}])  # no modalities → skipped
    lenient.fuse_multimodal_results(
        [{"content_id": "y", "text": {"score": float("nan")}}]
    )  # NaN score → modality dropped
    out = lenient.fuse_multimodal_results(
        [{"content_id": "z", "text": {"score": 1.5}}]
    )  # score > 1 → clamped
    assert out and out[0].fused_score <= 1.0
    print("lenient    : OK")

    # ---- Malformed modality payload in strict mode ----------------- #
    try:
        strict.fuse_multimodal_results(
            [{"content_id": "bad", "text": "not-a-dict-or-number"}]
        )
    except MultimodalFusionError as exc:
        print("Rejected   :", exc)

    print("\nSmoke test passed.")
