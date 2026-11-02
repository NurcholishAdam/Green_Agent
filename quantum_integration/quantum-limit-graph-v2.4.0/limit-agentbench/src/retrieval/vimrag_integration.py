# src/retrieval/vimrag_integration.py

"""
VimRAG Integration Orchestrator
===============================

Complete integration of all VimRAG modules into a unified retrieval
pipeline. Coordinates: topological allocation, semantic scoring, graph
traversal, carbon adaptation, multimodal fusion, and serendipity logging.

Design
------
- Optional ``networkx`` / ``numpy`` imports guarded by ``_*_AVAILABLE``.
- **Defensive sub-module imports** — package-relative first, flat
  second, ``None`` on failure. Every sub-module is optional; the
  orchestrator degrades rather than failing to load.
- **Duplicate-aware `add_document`** — a caller-supplied ``node_id``
  is honored via a ``_rekey_node`` helper that prefers the core
  module's ``rename_node`` (if present) and falls back to a guarded
  manipulation of ``memory_graph`` and ``graph_edges``. Works with
  both ``Set[str]`` and ``List[str]`` edge storage.
- **Carbon metrics are independent of serendipity** —
  ``efficiency_gains`` and ``carbon_savings_kg`` are computed from
  the baseline/actual delta regardless of whether the serendipity
  logger is available.
- **Pipeline and carbon mode are separate** — results carry both
  ``pipeline`` (``"balanced"`` etc.) and ``retrieval_mode`` (the
  carbon controller's ``"FULL"`` / ``"MINIMAL"`` / ``"blocked"``).
- **Real fusion payloads** — multimodal fusion is fed scores derived
  from the semantic scorer rather than hardcoded constants.
- **`warnings` field** — non-fatal errors during the pipeline are
  captured so callers can detect silently-skipped sub-modules.
- **Strict serialization** — ``to_json(allow_nan=False)`` and
  ``export_integration_report`` use a strict ``_safe_json_default``
  that raises on unknown types instead of silently stringifying.
- **`from_dict` on all three classes** — integration, config, and
  result.

Version: 2.0.0
"""

from __future__ import annotations

import json
import logging
import math
import numbers
import os
import tempfile
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from types import TracebackType
from typing import (
    Any,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
)

logger = logging.getLogger(__name__)

__all__ = [
    "VERSION",
    "IntegratedRetrievalResult",
    "RetrievalPipeline",
    "VimRAGIntegration",
    "VimRAGIntegrationConfig",
    "VimRAGIntegrationError",
    "create_vimrag_pipeline",
]

VERSION = "2.0.0"


# --------------------------------------------------------------------------- #
# Optional dependencies
# --------------------------------------------------------------------------- #
try:  # pragma: no cover
    import networkx as nx  # type: ignore

    _NETWORKX_AVAILABLE = True
except ImportError:  # pragma: no cover
    nx = None  # type: ignore[assignment]
    _NETWORKX_AVAILABLE = False
    logger.debug(
        "networkx not importable; VimRAGIntegration will skip graph."
    )

try:  # pragma: no cover
    import numpy as np  # type: ignore

    _NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Defensive sub-module imports
# --------------------------------------------------------------------------- #
_VimRAGCoRetrieval = None
_RetrievalNode = None
_RetrievalTrace = None
_TokenAwareFilter = None
_ContextSize = None
_GridIntensity = None
_CarbonAdaptiveRetrievalController = None
_RetrievalMode = None
_HierarchicalRetrieval = None
_RetrievalLevel = None
_MultimodalFusion = None
_ModalityType = None
_FusedResult = None
_ContextCompressor = None
_CompressionResult = None
_VisualContextHandler = None
_VisualContextType = None
_SerendipityTraceLogger = None
_SerendipityEvent = None
_TopologicalTokenAllocator = None
_ImportanceMetric = None
_create_local_scorer = None


def _try_import(name: str) -> Any:
    """Try ``from .module import *`` then ``from module import *``."""
    pkg = __name__.rsplit(".", 1)[0] if "." in __name__ else ""
    if pkg:
        try:
            return __import__(f"{pkg}.{name}", fromlist=["*"])
        except Exception:
            pass
    try:
        return __import__(name, fromlist=["*"])
    except Exception:
        return None


_core_mod = _try_import("vimrag_coretrieval")
if _core_mod is not None:
    _VimRAGCoRetrieval = getattr(
        _core_mod, "VimRAGCoRetrieval", None,
    )
    _RetrievalNode = getattr(_core_mod, "RetrievalNode", None)
    _RetrievalTrace = getattr(_core_mod, "RetrievalTrace", None)

_filter_mod = _try_import("token_aware_filter")
if _filter_mod is not None:
    _TokenAwareFilter = getattr(
        _filter_mod, "TokenAwareFilter", None,
    )
    _ContextSize = getattr(_filter_mod, "ContextSize", None)
    _GridIntensity = getattr(_filter_mod, "GridIntensity", None)

_carbon_mod = _try_import("carbon_adaptive_controller")
if _carbon_mod is not None:
    _CarbonAdaptiveRetrievalController = getattr(
        _carbon_mod, "CarbonAdaptiveRetrievalController", None,
    )
    _RetrievalMode = getattr(_carbon_mod, "RetrievalMode", None)

_hier_mod = _try_import("hierarchical_retrieval")
if _hier_mod is not None:
    _HierarchicalRetrieval = getattr(
        _hier_mod, "HierarchicalRetrieval", None,
    )
    _RetrievalLevel = getattr(_hier_mod, "RetrievalLevel", None)

_fusion_mod = _try_import("multimodal_fusion")
if _fusion_mod is not None:
    _MultimodalFusion = getattr(
        _fusion_mod, "MultimodalFusion", None,
    )
    _ModalityType = getattr(_fusion_mod, "ModalityType", None)
    _FusedResult = getattr(_fusion_mod, "FusedResult", None)

_comp_mod = _try_import("context_compression")
if _comp_mod is not None:
    _ContextCompressor = getattr(
        _comp_mod, "ContextCompressor", None,
    )
    _CompressionResult = getattr(
        _comp_mod, "CompressionResult", None,
    )

_visual_mod = _try_import("visual_context_handler")
if _visual_mod is not None:
    _VisualContextHandler = getattr(
        _visual_mod, "VisualContextHandler", None,
    )
    _VisualContextType = getattr(
        _visual_mod, "VisualContextType", None,
    )

_serendip_mod = _try_import("serendipity_logger")
if _serendip_mod is not None:
    _SerendipityTraceLogger = getattr(
        _serendip_mod, "SerendipityTraceLogger", None,
    )
    _SerendipityEvent = getattr(
        _serendip_mod, "SerendipityEvent", None,
    )

_topo_mod = _try_import("topological_allocator")
if _topo_mod is not None:
    _TopologicalTokenAllocator = getattr(
        _topo_mod, "TopologicalTokenAllocator", None,
    )
    _ImportanceMetric = getattr(
        _topo_mod, "ImportanceMetric", None,
    )

_semantic_mod = _try_import("semantic_scorer")
if _semantic_mod is not None:
    _create_local_scorer = getattr(
        _semantic_mod, "create_local_scorer", None,
    )


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class VimRAGIntegrationError(ValueError):
    """Raised for invalid integration inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class VimRAGIntegrationConfig:
    """Tunable parameters for :class:`VimRAGIntegration`."""

    grid_region: str = "US-CA"
    embedding_model: str = "all-MiniLM-L6-v2"

    # Module toggles.
    enable_semantic_scoring: bool = True
    enable_topological_allocation: bool = True
    enable_carbon_adaptation: bool = True
    enable_serendipity: bool = True

    # Default budgets.
    default_energy_budget_wh: float = 0.05
    green_energy_budget_wh: float = 0.01
    fast_token_budget: int = 2048
    balanced_token_budget: int = 4096

    # Floor applied to the token budget when the token filter fires.
    # See the ``filter_fallback_multiplier`` docstring.
    min_token_budget: int = 16

    # Carbon intensity model (kg CO2e / Wh).
    default_carbon_intensity_kg_per_wh: float = 0.385

    # Baseline energy model.
    baseline_energy_per_token_wh: float = 1e-6

    # When ``should_filter`` returns True, the pipeline **shrinks**
    # the token budget to ``max(min_token_budget, int(tb * mul))``
    # rather than aborting. The token filter's ``should_filter=True``
    # means "the requested content-type/size combination exceeds the
    # current grid-friendly budget"; the fallback lets the pipeline
    # proceed with a smaller scope. Set ``filter_fallback_multiplier=1.0``
    # to disable shrinking.
    filter_fallback_multiplier: float = 0.5

    # Compression heuristic.
    default_compression_ratio: float = 0.5

    # Bounded internal history (retained only when retain_results=True).
    retain_results: bool = False
    max_history: int = 1000

    def __post_init__(self) -> None:
        if (
            not isinstance(self.grid_region, str)
            or not self.grid_region
        ):
            raise VimRAGIntegrationError(
                "grid_region must be a non-empty string."
            )
        if (
            not isinstance(self.embedding_model, str)
            or not self.embedding_model
        ):
            raise VimRAGIntegrationError(
                "embedding_model must be a non-empty string."
            )
        for name in (
            "enable_semantic_scoring",
            "enable_topological_allocation",
            "enable_carbon_adaptation",
            "enable_serendipity",
            "retain_results",
        ):
            if not isinstance(getattr(self, name), bool):
                raise VimRAGIntegrationError(
                    f"{name} must be a bool."
                )
        for name in (
            "default_energy_budget_wh",
            "green_energy_budget_wh",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
                or value <= 0
            ):
                raise VimRAGIntegrationError(
                    f"{name} must be finite and > 0."
                )
        for name in (
            "fast_token_budget",
            "balanced_token_budget",
            "min_token_budget",
            "max_history",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Integral)
                or value <= 0
            ):
                raise VimRAGIntegrationError(
                    f"{name} must be a positive int."
                )
        if (
            isinstance(
                self.default_carbon_intensity_kg_per_wh, bool,
            )
            or not isinstance(
                self.default_carbon_intensity_kg_per_wh,
                numbers.Real,
            )
            or not math.isfinite(
                float(self.default_carbon_intensity_kg_per_wh)
            )
            or self.default_carbon_intensity_kg_per_wh < 0
        ):
            raise VimRAGIntegrationError(
                "default_carbon_intensity_kg_per_wh must be finite "
                "and >= 0."
            )
        if (
            isinstance(self.baseline_energy_per_token_wh, bool)
            or not isinstance(
                self.baseline_energy_per_token_wh, numbers.Real,
            )
            or not math.isfinite(
                float(self.baseline_energy_per_token_wh)
            )
            or self.baseline_energy_per_token_wh <= 0
        ):
            raise VimRAGIntegrationError(
                "baseline_energy_per_token_wh must be finite and "
                "> 0."
            )
        if not 0.0 < self.filter_fallback_multiplier <= 1.0:
            raise VimRAGIntegrationError(
                "filter_fallback_multiplier must be in (0, 1]."
            )
        if not 0.0 < self.default_compression_ratio <= 1.0:
            raise VimRAGIntegrationError(
                "default_compression_ratio must be in (0, 1]."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "VimRAGIntegrationConfig":
        if not isinstance(data, Mapping):
            raise VimRAGIntegrationError(
                f"VimRAGIntegrationConfig.from_dict expects a "
                f"Mapping, got {type(data).__name__}."
            )
        try:
            valid = set(cls.__dataclass_fields__.keys())
            unknown = set(data) - valid
            if unknown:
                logger.debug(
                    "VimRAGIntegrationConfig.from_dict dropping "
                    "unknown fields: %s", sorted(unknown),
                )
            kwargs: Dict[str, Any] = {}
            for k, v in data.items():
                if k not in valid:
                    continue
                if k in (
                    "fast_token_budget",
                    "balanced_token_budget",
                    "min_token_budget",
                    "max_history",
                ):
                    if (
                        isinstance(v, bool)
                        or not isinstance(v, int)
                    ):
                        raise VimRAGIntegrationError(
                            f"{k} must be an int, got "
                            f"{type(v).__name__}."
                        )
                if k in (
                    "enable_semantic_scoring",
                    "enable_topological_allocation",
                    "enable_carbon_adaptation",
                    "enable_serendipity",
                    "retain_results",
                ):
                    if not isinstance(v, bool):
                        raise VimRAGIntegrationError(
                            f"{k} must be bool, got "
                            f"{type(v).__name__}."
                        )
                kwargs[k] = v
            return cls(**kwargs)
        except (TypeError, ValueError) as exc:
            if isinstance(exc, VimRAGIntegrationError):
                raise
            raise VimRAGIntegrationError(
                f"invalid VimRAGIntegrationConfig payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# Pipeline mode
# --------------------------------------------------------------------------- #
class RetrievalPipeline(Enum):
    """Retrieval pipeline modes."""
    FAST = "fast"
    BALANCED = "balanced"
    COMPREHENSIVE = "comprehensive"
    GREEN = "green"
    MULTIMODAL = "multimodal"


# --------------------------------------------------------------------------- #
# JSON helpers
# --------------------------------------------------------------------------- #
def _safe(obj: Any) -> Any:
    """Best-effort JSON-safe representation for arbitrary objects."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if hasattr(obj, "to_dict") and callable(obj.to_dict):
        try:
            return obj.to_dict()
        except Exception:
            pass
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Mapping):
        return {str(k): _safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_safe(v) for v in obj]
    return repr(obj)


def _safe_json_default(obj: Any) -> Any:
    """
    Strict JSON default for ``json.dump``/``json.dumps``.

    Accepts objects with ``to_dict()``, ``Enum``, and ``datetime``;
    raises ``TypeError`` for anything else so unknown types never
    silently become opaque strings.
    """
    if hasattr(obj, "to_dict") and callable(obj.to_dict):
        return obj.to_dict()
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(
        f"cannot serialize object of type "
        f"{type(obj).__name__} to JSON."
    )


# --------------------------------------------------------------------------- #
# Result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class IntegratedRetrievalResult:
    """Immutable result of a full VimRAG pipeline run."""

    retrieved_nodes: Tuple[Any, ...]
    retrieval_trace: Optional[Any]
    token_allocations: Mapping[str, Any]
    topological_summary: Mapping[str, Any]
    semantic_scores: Tuple[Any, ...]
    avg_similarity: float
    total_energy_wh: float
    total_carbon_kg: float
    retrieval_mode: str
    pipeline: str = "balanced"
    carbon_savings_kg: float = 0.0
    modalities_used: Tuple[str, ...] = tuple()
    fusion_results: Optional[Any] = None
    hierarchy_result: Optional[Any] = None
    compression_ratio: float = 1.0
    tokens_saved: int = 0
    serendipity_events: Tuple[Any, ...] = tuple()
    efficiency_gains: float = 0.0
    query: str = ""
    timestamp: float = 0.0
    total_time_ms: float = 0.0
    cache_hits: int = 0
    warnings: Tuple[str, ...] = tuple()
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        for name in (
            "retrieved_nodes",
            "semantic_scores",
            "modalities_used",
            "serendipity_events",
            "warnings",
        ):
            value = getattr(self, name)
            if not isinstance(value, tuple):
                object.__setattr__(self, name, tuple(value))

        for name in (
            "avg_similarity",
            "total_energy_wh",
            "total_carbon_kg",
            "carbon_savings_kg",
            "compression_ratio",
            "efficiency_gains",
            "total_time_ms",
            "timestamp",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
            ):
                raise VimRAGIntegrationError(
                    f"{name} must be numeric."
                )
            fv = float(value)
            if not math.isfinite(fv):
                raise VimRAGIntegrationError(
                    f"{name} must be finite, got {value!r}."
                )
            if fv < 0:
                raise VimRAGIntegrationError(
                    f"{name} must be >= 0."
                )
        if not 0.0 <= self.avg_similarity <= 1.0:
            raise VimRAGIntegrationError(
                "avg_similarity must be in [0, 1]."
            )
        if not 0.0 <= self.compression_ratio <= 1.0:
            raise VimRAGIntegrationError(
                "compression_ratio must be in [0, 1]."
            )
        for name in ("tokens_saved", "cache_hits"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Integral)
                or value < 0
            ):
                raise VimRAGIntegrationError(
                    f"{name} must be a non-negative int."
                )
        if not isinstance(self.query, str):
            raise VimRAGIntegrationError(
                "query must be a string."
            )
        if (
            not isinstance(self.retrieval_mode, str)
            or not self.retrieval_mode
        ):
            raise VimRAGIntegrationError(
                "retrieval_mode must be a non-empty string."
            )
        if not isinstance(self.pipeline, str) or (
            not self.pipeline
        ):
            raise VimRAGIntegrationError(
                "pipeline must be a non-empty string."
            )

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-safe dict (complex objects are ``to_dict``-ed)."""
        return {
            "version": VERSION,
            "retrieved_nodes": [
                _safe(o) for o in self.retrieved_nodes
            ],
            "retrieval_trace": _safe(self.retrieval_trace),
            "token_allocations": _safe(self.token_allocations),
            "topological_summary": _safe(
                self.topological_summary
            ),
            "semantic_scores": [
                _safe(o) for o in self.semantic_scores
            ],
            "avg_similarity": self.avg_similarity,
            "total_energy_wh": self.total_energy_wh,
            "total_carbon_kg": self.total_carbon_kg,
            "retrieval_mode": self.retrieval_mode,
            "pipeline": self.pipeline,
            "carbon_savings_kg": self.carbon_savings_kg,
            "modalities_used": list(self.modalities_used),
            "fusion_results": _safe(self.fusion_results),
            "hierarchy_result": _safe(self.hierarchy_result),
            "compression_ratio": self.compression_ratio,
            "tokens_saved": self.tokens_saved,
            "serendipity_events": [
                _safe(e) for e in self.serendipity_events
            ],
            "efficiency_gains": self.efficiency_gains,
            "query": self.query,
            "timestamp": self.timestamp,
            "total_time_ms": self.total_time_ms,
            "cache_hits": self.cache_hits,
            "warnings": list(self.warnings),
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "IntegratedRetrievalResult":
        if not isinstance(data, Mapping):
            raise VimRAGIntegrationError(
                f"IntegratedRetrievalResult.from_dict expects a "
                f"Mapping, got {type(data).__name__}."
            )
        try:
            ts = data.get("created_at")
            if isinstance(ts, str):
                created_at = datetime.fromisoformat(
                    ts.replace("Z", "+00:00")
                )
                if not created_at.tzinfo:
                    created_at = created_at.replace(
                        tzinfo=timezone.utc
                    )
            elif isinstance(ts, datetime):
                created_at = ts
            else:
                created_at = datetime.now(timezone.utc)

            return cls(
                retrieved_nodes=tuple(
                    data.get("retrieved_nodes") or []
                ),
                retrieval_trace=data.get("retrieval_trace"),
                token_allocations=dict(
                    data.get("token_allocations") or {}
                ),
                topological_summary=dict(
                    data.get("topological_summary") or {}
                ),
                semantic_scores=tuple(
                    data.get("semantic_scores") or []
                ),
                avg_similarity=float(
                    data.get("avg_similarity", 0.0)
                ),
                total_energy_wh=float(
                    data.get("total_energy_wh", 0.0)
                ),
                total_carbon_kg=float(
                    data.get("total_carbon_kg", 0.0)
                ),
                retrieval_mode=str(
                    data.get("retrieval_mode", "FULL")
                ),
                pipeline=str(
                    data.get("pipeline", "balanced")
                ),
                carbon_savings_kg=float(
                    data.get("carbon_savings_kg", 0.0)
                ),
                modalities_used=tuple(
                    data.get("modalities_used") or []
                ),
                fusion_results=data.get("fusion_results"),
                hierarchy_result=data.get(
                    "hierarchy_result"
                ),
                compression_ratio=float(
                    data.get("compression_ratio", 1.0)
                ),
                tokens_saved=int(
                    data.get("tokens_saved", 0)
                ),
                serendipity_events=tuple(
                    data.get("serendipity_events") or []
                ),
                efficiency_gains=float(
                    data.get("efficiency_gains", 0.0)
                ),
                query=str(data.get("query", "")),
                timestamp=float(
                    data.get("timestamp", 0.0)
                ),
                total_time_ms=float(
                    data.get("total_time_ms", 0.0)
                ),
                cache_hits=int(data.get("cache_hits", 0)),
                warnings=tuple(
                    data.get("warnings") or []
                ),
                created_at=created_at,
            )
        except (TypeError, ValueError) as exc:
            if isinstance(exc, VimRAGIntegrationError):
                raise
            raise VimRAGIntegrationError(
                f"invalid IntegratedRetrievalResult payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "IntegratedRetrievalResult("
            f"query={self.query!r}, "
            f"pipeline={self.pipeline}, "
            f"nodes={len(self.retrieved_nodes)}, "
            f"energy_wh={self.total_energy_wh:.6f}, "
            f"carbon_kg={self.total_carbon_kg:.6f}, "
            f"mode={self.retrieval_mode}, "
            f"time_ms={self.total_time_ms:.2f})"
        )


# --------------------------------------------------------------------------- #
# Integration orchestrator
# --------------------------------------------------------------------------- #
class VimRAGIntegration:
    """
    Complete VimRAG integration orchestrator.

    Thread-safe, serializable, and defensive against missing
    sub-modules.
    """

    def __init__(
        self,
        grid_region: str = "US-CA",
        enable_semantic_scoring: bool = True,
        enable_topological_allocation: bool = True,
        enable_carbon_adaptation: bool = True,
        enable_serendipity: bool = True,
        embedding_model: str = "all-MiniLM-L6-v2",
        *,
        config: Optional[VimRAGIntegrationConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = VimRAGIntegrationConfig(
                grid_region=str(grid_region),
                embedding_model=str(embedding_model),
                enable_semantic_scoring=bool(
                    enable_semantic_scoring
                ),
                enable_topological_allocation=bool(
                    enable_topological_allocation
                ),
                enable_carbon_adaptation=bool(
                    enable_carbon_adaptation
                ),
                enable_serendipity=bool(enable_serendipity),
            )
        self._strict = bool(strict)

        # ---- Core retrieval ------------------------------------------
        if _VimRAGCoRetrieval is None:
            raise VimRAGIntegrationError(
                "VimRAGCoRetrieval is unavailable; install the "
                "retrieval sub-package or fix the import path."
            )
        try:
            self.core_retrieval = _VimRAGCoRetrieval(
                max_tokens_per_retrieval=4096,
                max_energy_per_retrieval_wh=0.01,
                enable_visual_context=True,
            )
        except Exception as exc:
            raise VimRAGIntegrationError(
                f"could not initialize VimRAGCoRetrieval: {exc}"
            ) from exc

        # ---- Optional sub-modules ------------------------------------
        self.carbon_controller = self._safe_construct(
            _CarbonAdaptiveRetrievalController,
            "carbon controller",
            clean_grid_threshold_g_kwh=200.0,
            dirty_grid_threshold_kwh=400.0,
            enable_deferral=True,
        ) if (
            self._config.enable_carbon_adaptation
            and _CarbonAdaptiveRetrievalController is not None
        ) else None

        self.token_filter = self._safe_construct(
            _TokenAwareFilter,
            "token filter",
            current_grid_intensity_g_kwh=385.0,
            enable_adaptive_filtering=True,
        )

        self.hierarchical = self._safe_construct(
            _HierarchicalRetrieval,
            "hierarchical retrieval",
            coarse_budget_tokens=500,
            medium_budget_tokens=1500,
            fine_budget_tokens=4096,
        )

        self.fusion = self._safe_construct(
            _MultimodalFusion,
            "multimodal fusion",
            text_weight=0.6,
            visual_weight=0.4,
            energy_penalty_factor=0.1,
        )

        self.compressor = self._safe_construct(
            _ContextCompressor,
            "context compressor",
            target_compression_ratio=0.5,
            preserve_entities=True,
            preserve_numbers=True,
        )

        self.visual_handler = self._safe_construct(
            _VisualContextHandler,
            "visual handler",
            visual_token_cost=256,
            visual_energy_multiplier=2.5,
            max_visual_contexts=5,
        )

        self.serendipity = (
            self._safe_construct(
                _SerendipityTraceLogger,
                "serendipity logger",
                negawatt_reward_rate=100.0,
                min_savings_threshold_pct=10.0,
            )
            if (
                self._config.enable_serendipity
                and _SerendipityTraceLogger is not None
            ) else None
        )

        self.graph = (
            nx.DiGraph() if _NETWORKX_AVAILABLE
            and nx is not None else None
        )

        self.topo_allocator = (
            self._safe_construct(
                _TopologicalTokenAllocator,
                "topological allocator",
                importance_metric=(
                    _ImportanceMetric.COMBINED
                    if _ImportanceMetric is not None else None
                ),
                enable_dynamic_adjustment=True,
            )
            if (
                self._config.enable_topological_allocation
                and _TopologicalTokenAllocator is not None
                and _ImportanceMetric is not None
            ) else None
        )
        if (
            self._config.enable_topological_allocation
            and self.topo_allocator is None
        ):
            logger.warning(
                "TopologicalTokenAllocator not available; "
                "topological allocation disabled.",
            )

        self.semantic_scorer = (
            self._safe_construct(
                _create_local_scorer,
                "semantic scorer",
                model_name=self._config.embedding_model,
                cache_embeddings=True,
            )
            if (
                self._config.enable_semantic_scoring
                and _create_local_scorer is not None
            ) else None
        )
        if (
            self._config.enable_semantic_scoring
            and self.semantic_scorer is None
        ):
            logger.warning(
                "SemanticScorer not available; using keyword matching.",
            )

        # ---- State ---------------------------------------------------
        self._lock = threading.RLock()
        self.retrieval_count: int = 0
        self.total_energy_consumed: float = 0.0
        self.total_carbon_emitted: float = 0.0
        self.total_carbon_saved: float = 0.0
        # Live carbon intensity (kg CO2e / Wh) — updated by
        # ``update_grid_intensity`` and used for per-retrieval carbon
        # accounting. Falls back to the config default.
        self._live_carbon_intensity_kg_per_wh: float = (
            self._config.default_carbon_intensity_kg_per_wh
        )

        self._result_history: Optional[
            Deque[IntegratedRetrievalResult]
        ] = (
            deque(maxlen=self._config.max_history)
            if self._config.retain_results else None
        )

        logger.debug(
            "VimRAGIntegration initialized "
            "(region=%s, semantic=%s, topo=%s, carbon=%s, "
            "serendipity=%s, strict=%s)",
            self._config.grid_region,
            self.semantic_scorer is not None,
            self.topo_allocator is not None,
            self.carbon_controller is not None,
            self.serendipity is not None,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> VimRAGIntegrationConfig:
        return self._config

    @property
    def live_carbon_intensity_kg_per_wh(self) -> float:
        return self._live_carbon_intensity_kg_per_wh

    # ---------------------------------------------------------- helpers
    @staticmethod
    def _safe_construct(
        factory: Any, name: str, **kwargs: Any,
    ) -> Optional[Any]:
        """Construct an optional sub-module; ``None`` on failure."""
        if factory is None:
            return None
        # Filter None values so optional kwargs don't break ctors
        # that validate their inputs strictly.
        clean = {k: v for k, v in kwargs.items() if v is not None}
        try:
            return factory(**clean)
        except Exception as exc:
            logger.warning("%s init failed: %s", name, exc)
            return None

    def _rekey_node(self, old_id: str, new_id: str) -> bool:
        """
        Move a node from ``old_id`` to ``new_id``.

        Prefers a public ``rename_node`` on the core module (if the
        enhanced ``vimrag_coretrieval`` exposes one). Falls back to a
        guarded manipulation of ``memory_graph`` and ``graph_edges``
        under the core's lock, handling both ``Set[str]`` (enhanced)
        and ``List[str]`` (original) edge storage.

        Returns True on success.
        """
        # Preferred: public API.
        rename = getattr(self.core_retrieval, "rename_node", None)
        if callable(rename):
            try:
                return bool(rename(old_id, new_id))
            except Exception as exc:
                logger.warning(
                    "core_retrieval.rename_node(%s, %s) failed: %s",
                    old_id, new_id, exc,
                )
                return False

        # Fallback: guarded direct manipulation.
        lock = getattr(self.core_retrieval, "_lock", None)
        graph = getattr(
            self.core_retrieval, "memory_graph", None,
        )
        edges = getattr(
            self.core_retrieval, "graph_edges", None,
        )
        if lock is None or graph is None or edges is None:
            return False
        try:
            with lock:
                if old_id not in graph or new_id in graph:
                    return False
                node = graph.pop(old_id)
                if _RetrievalNode is not None:
                    renamed = _RetrievalNode(
                        node_id=new_id,
                        content_type=node.content_type,
                        content=node.content,
                        token_size=node.token_size,
                        file_weight=node.file_weight,
                        metadata=node.metadata,
                        timestamp=node.timestamp,
                    )
                    graph[new_id] = renamed
                else:
                    graph[new_id] = node
                if old_id in edges:
                    incoming = edges.pop(old_id)
                    edges[new_id] = incoming
                # Update incoming edges to point at new_id.
                for targets in edges.values():
                    if old_id in targets:
                        if hasattr(targets, "discard"):
                            targets.discard(old_id)
                            targets.add(new_id)
                        elif hasattr(targets, "remove"):
                            targets.remove(old_id)
                            targets.append(new_id)
            return True
        except Exception as exc:
            logger.warning(
                "rekey fallback %s -> %s failed: %s",
                old_id, new_id, exc,
            )
            return False

    # ---------------------------------------------------------- ingestion
    def add_document(
        self,
        content: Any,
        content_type: str,
        metadata: Optional[Dict[str, Any]] = None,
        node_id: Optional[str] = None,
    ) -> str:
        """
        Add a document to the VimRAG memory graph.

        When ``node_id`` is supplied, the caller's identifier becomes
        the canonical node key. If ``node_id`` is already taken, strict
        mode raises; non-strict mode logs and returns the existing ID.
        If the content hashes to an existing node under a different ID,
        the node is renamed to ``node_id`` (using
        ``core_retrieval.rename_node`` if available, else a guarded
        fallback).
        """
        if not isinstance(content_type, str) or not content_type:
            raise VimRAGIntegrationError(
                "content_type must be a non-empty string."
            )
        if metadata is not None and not isinstance(
            metadata, Mapping,
        ):
            raise VimRAGIntegrationError(
                "metadata must be a Mapping or None."
            )
        if node_id is not None and (
            not isinstance(node_id, str) or not node_id
        ):
            raise VimRAGIntegrationError(
                "node_id must be a non-empty string or None."
            )

        if node_id is None:
            effective_id = self.core_retrieval.add_node(
                content,
                content_type,
                dict(metadata) if metadata else None,
            )
        else:
            # Target ID already taken?
            with self._lock:
                target_exists = (
                    node_id in self.core_retrieval.memory_graph
                )
            if target_exists:
                if self._strict:
                    raise VimRAGIntegrationError(
                        f"node_id {node_id!r} already exists in "
                        f"the graph."
                    )
                logger.warning(
                    "node_id %r already exists; skipping.",
                    node_id,
                )
                return node_id

            try:
                generated = self.core_retrieval.add_node(
                    content,
                    content_type,
                    dict(metadata) if metadata else None,
                )
            except Exception as exc:
                if self._strict:
                    raise VimRAGIntegrationError(
                        f"add_node failed: {exc}"
                    ) from exc
                logger.warning("add_node failed: %s", exc)
                return node_id

            if generated == node_id:
                effective_id = node_id
            elif self._rekey_node(generated, node_id):
                effective_id = node_id
            else:
                logger.warning(
                    "could not rename %s -> %s; keeping "
                    "generated ID.",
                    generated, node_id,
                )
                effective_id = generated

        # NetworkX mirror.
        if self.graph is not None:
            try:
                self.graph.add_node(
                    effective_id,
                    content=content,
                    content_type=content_type,
                )
            except Exception:  # pragma: no cover
                pass

        # Visual handler mirror.
        if (
            content_type in ("visual", "multimodal")
            and self.visual_handler is not None
        ):
            visual_type = (
                _VisualContextType.IMAGE
                if _VisualContextType is not None else None
            )
            if visual_type is not None:
                try:
                    self.visual_handler.add_visual_context(
                        source_path=str(content),
                        context_type=visual_type,
                        caption=(
                            metadata.get("caption")
                            if metadata else None
                        ),
                    )
                except Exception as exc:
                    logger.warning(
                        "visual_handler.add_visual_context "
                        "failed: %s", exc,
                    )

        return effective_id

    def connect_documents(
        self, source_id: str, target_id: str,
        weight: float = 1.0,
    ) -> None:
        """Create a semantic connection between two documents."""
        if not isinstance(source_id, str) or not source_id:
            raise VimRAGIntegrationError(
                "source_id must be a non-empty string."
            )
        if not isinstance(target_id, str) or not target_id:
            raise VimRAGIntegrationError(
                "target_id must be a non-empty string."
            )
        if (
            isinstance(weight, bool)
            or not isinstance(weight, numbers.Real)
        ):
            raise VimRAGIntegrationError(
                "weight must be numeric."
            )
        fw = float(weight)
        if not math.isfinite(fw) or fw < 0:
            raise VimRAGIntegrationError(
                f"weight must be finite and >= 0, got "
                f"{weight!r}."
            )

        try:
            self.core_retrieval.connect_nodes(
                source_id, target_id,
            )
        except Exception as exc:
            if self._strict:
                raise VimRAGIntegrationError(
                    f"connect_nodes failed: {exc}"
                ) from exc
            logger.warning("connect_nodes failed: %s", exc)

        if self.graph is not None:
            try:
                self.graph.add_edge(
                    source_id, target_id, weight=fw,
                )
            except Exception:  # pragma: no cover
                pass

    # ---------------------------------------------------------- retrieval
    def retrieve(
        self,
        query: str,
        pipeline: Any = RetrievalPipeline.BALANCED,
        max_results: int = 10,
        energy_budget_wh: Optional[float] = None,
        token_budget: Optional[int] = None,
        include_visual: bool = True,
        agent_id: str = "default_agent",
    ) -> IntegratedRetrievalResult:
        """
        Execute the complete VimRAG retrieval pipeline.

        ``pipeline`` accepts either a :class:`RetrievalPipeline` or
        its string value.
        """
        # ---- Validate ------------------------------------------------
        if not isinstance(query, str) or not query.strip():
            raise VimRAGIntegrationError(
                "query must be a non-empty string."
            )
        if isinstance(pipeline, str):
            try:
                pipeline = RetrievalPipeline(pipeline.lower())
            except ValueError as exc:
                raise VimRAGIntegrationError(
                    f"unknown pipeline {pipeline!r}: {exc}"
                ) from exc
        elif not isinstance(pipeline, RetrievalPipeline):
            raise VimRAGIntegrationError(
                f"pipeline must be a RetrievalPipeline or its "
                f"string value, got {type(pipeline).__name__}."
            )
        if (
            isinstance(max_results, bool)
            or not isinstance(max_results, numbers.Integral)
            or max_results <= 0
        ):
            raise VimRAGIntegrationError(
                "max_results must be a positive int."
            )
        if energy_budget_wh is not None:
            if (
                isinstance(energy_budget_wh, bool)
                or not isinstance(
                    energy_budget_wh, numbers.Real,
                )
            ):
                raise VimRAGIntegrationError(
                    "energy_budget_wh must be numeric or None."
                )
            fb = float(energy_budget_wh)
            if not math.isfinite(fb) or fb <= 0:
                raise VimRAGIntegrationError(
                    f"energy_budget_wh must be finite and > 0, "
                    f"got {energy_budget_wh!r}."
                )
        if token_budget is not None and (
            isinstance(token_budget, bool)
            or not isinstance(token_budget, numbers.Integral)
            or token_budget <= 0
        ):
            raise VimRAGIntegrationError(
                "token_budget must be a positive int or None."
            )
        if not isinstance(include_visual, bool):
            raise VimRAGIntegrationError(
                "include_visual must be a bool."
            )
        if not isinstance(agent_id, str) or not agent_id:
            raise VimRAGIntegrationError(
                "agent_id must be a non-empty string."
            )

        start_time = datetime.now(timezone.utc)
        warnings: List[str] = []

        # ---- Defaults -----------------------------------------------
        if energy_budget_wh is None:
            energy_budget_wh = (
                self._config.green_energy_budget_wh
                if pipeline == RetrievalPipeline.GREEN
                else self._config.default_energy_budget_wh
            )
        if token_budget is None:
            token_budget = (
                self._config.fast_token_budget
                if pipeline == RetrievalPipeline.FAST
                else self._config.balanced_token_budget
            )

        baseline_energy = self._estimate_baseline_energy(
            query, int(token_budget),
        )

        # ---- 1. Carbon adaptation -----------------------------------
        retrieval_mode = "FULL"
        if self.carbon_controller is not None:
            try:
                carbon_decision = (
                    self.carbon_controller.decide_retrieval(
                        query=query,
                        estimated_tokens=int(token_budget),
                        content_type=(
                            "multimodal" if include_visual
                            else "text"
                        ),
                        urgency="normal",
                    )
                )
            except Exception as exc:
                if self._strict:
                    raise VimRAGIntegrationError(
                        f"carbon decision failed: {exc}"
                    ) from exc
                warnings.append(
                    f"carbon_decision_failed:{type(exc).__name__}"
                )
                logger.warning(
                    "carbon decision failed: %s", exc,
                )
                carbon_decision = None

            if carbon_decision is not None:
                if not getattr(
                    carbon_decision, "allowed", True,
                ):
                    logger.info(
                        "Retrieval deferred by carbon "
                        "controller: %s",
                        getattr(
                            carbon_decision, "reason",
                            "unknown",
                        ),
                    )
                    return self._create_empty_result(
                        query,
                        "deferred_by_carbon_controller",
                        pipeline=pipeline.value,
                        start_time=start_time,
                        warnings=tuple(warnings),
                    )
                mode_attr = getattr(
                    carbon_decision, "mode", None,
                )
                retrieval_mode = (
                    mode_attr.value
                    if mode_attr is not None else "FULL"
                )
                max_tokens_attr = getattr(
                    carbon_decision, "max_tokens", None,
                )
                if isinstance(max_tokens_attr, int):
                    token_budget = min(
                        token_budget, max_tokens_attr,
                    )
                allow_visual_attr = getattr(
                    carbon_decision, "allow_visual", None,
                )
                if allow_visual_attr is not None:
                    include_visual = (
                        include_visual and allow_visual_attr
                    )

        # ---- 2. Token filtering --------------------------------------
        filter_decision: Optional[Mapping[str, Any]] = None
        if self.token_filter is not None:
            try:
                raw_decision = self.token_filter.should_filter(
                    token_count=int(token_budget),
                    content_type=(
                        "multimodal" if include_visual
                        else "text"
                    ),
                    relevance_score=0.5,
                )
                if isinstance(raw_decision, Mapping):
                    filter_decision = dict(raw_decision)
                elif hasattr(raw_decision, "to_dict"):
                    filter_decision = raw_decision.to_dict()
                else:
                    warnings.append(
                        "token_filter_returned_unexpected_type"
                    )
                    logger.warning(
                        "token_filter returned unexpected type "
                        "%s; skipping filter fallback.",
                        type(raw_decision).__name__,
                    )
            except Exception as exc:
                if self._strict:
                    raise VimRAGIntegrationError(
                        f"token filter failed: {exc}"
                    ) from exc
                warnings.append(
                    f"token_filter_failed:{type(exc).__name__}"
                )
                logger.warning(
                    "token filter failed: %s", exc,
                )

            if filter_decision and filter_decision.get(
                "should_filter"
            ):
                shrunk = int(
                    int(token_budget)
                    * self._config.filter_fallback_multiplier
                )
                token_budget = max(
                    self._config.min_token_budget, shrunk,
                )

        # ---- 3. Topological allocation -------------------------------
        token_allocations: Dict[str, Any] = {}
        topo_summary: Dict[str, Any] = {}
        if (
            self.topo_allocator is not None
            and self.graph is not None
            and pipeline in (
                RetrievalPipeline.COMPREHENSIVE,
                RetrievalPipeline.BALANCED,
            )
        ):
            try:
                token_allocations = (
                    self.topo_allocator.allocate_tokens(
                        graph=self.graph,
                        total_token_budget=int(token_budget),
                        base_tokens_per_node=100,
                        query_nodes=None,
                    )
                )
                topo_summary = (
                    self.topo_allocator.get_allocation_summary(
                        token_allocations
                    )
                )
            except Exception as exc:
                if self._strict:
                    raise VimRAGIntegrationError(
                        f"topological allocation failed: {exc}"
                    ) from exc
                warnings.append(
                    f"topo_allocation_failed:"
                    f"{type(exc).__name__}"
                )
                logger.warning(
                    "topological allocation failed: %s", exc,
                )

        # ---- 4. Semantic scoring -------------------------------------
        semantic_scores: List[Any] = []
        avg_similarity = 0.0
        if (
            self.semantic_scorer is not None
            and pipeline != RetrievalPipeline.FAST
        ):
            try:
                contents = [
                    (nid, str(node.content))
                    for nid, node
                    in self.core_retrieval.memory_graph.items()
                ]
                semantic_scores = (
                    self.semantic_scorer.score_and_rank(
                        query=query,
                        contents=contents,
                        top_k=int(max_results) * 2,
                    )
                )
                if semantic_scores:
                    avg_similarity = sum(
                        s.similarity_score
                        for s in semantic_scores
                    ) / len(semantic_scores)
            except Exception as exc:
                if self._strict:
                    raise VimRAGIntegrationError(
                        f"semantic scoring failed: {exc}"
                    ) from exc
                warnings.append(
                    f"semantic_scoring_failed:"
                    f"{type(exc).__name__}"
                )
                logger.warning(
                    "semantic scoring failed: %s", exc,
                )

        # ---- 5. Hierarchical retrieval -------------------------------
        hierarchy_result: Optional[Any] = None
        if (
            pipeline == RetrievalPipeline.COMPREHENSIVE
            and self.hierarchical is not None
            and _RetrievalLevel is not None
        ):
            try:
                hierarchy_result = (
                    self.hierarchical.hierarchical_retrieve(
                        query=query,
                        start_level=_RetrievalLevel.COARSE,
                        max_results=int(max_results),
                    )
                )
            except Exception as exc:
                if self._strict:
                    raise VimRAGIntegrationError(
                        f"hierarchical retrieval failed: {exc}"
                    ) from exc
                warnings.append(
                    f"hierarchical_failed:"
                    f"{type(exc).__name__}"
                )
                logger.warning(
                    "hierarchical retrieval failed: %s", exc,
                )

        # ---- 6. Core retrieval ---------------------------------------
        try:
            retrieved_nodes, retrieval_trace = (
                self.core_retrieval.retrieve(
                    query=query,
                    max_nodes=int(max_results),
                    include_visual=include_visual,
                    energy_budget_wh=energy_budget_wh,
                )
            )
        except Exception as exc:
            raise VimRAGIntegrationError(
                f"core retrieval failed: {exc}"
            ) from exc

        # ---- 7. Multimodal fusion ------------------------------------
        # Payloads are computed from real semantic scores instead of
        # the previous fabricated constants.
        fusion_results: Optional[Any] = None
        modalities_used: List[str] = ["text"]
        if (
            include_visual
            and self.fusion is not None
            and pipeline in (
                RetrievalPipeline.MULTIMODAL,
                RetrievalPipeline.COMPREHENSIVE,
            )
            and semantic_scores
        ):
            score_by_id = {
                getattr(s, "node_id", None): s
                for s in semantic_scores
            }
            multimodal_payloads: List[Dict[str, Any]] = []
            for node in retrieved_nodes:
                if node.content_type not in (
                    "visual", "multimodal",
                ):
                    continue
                sem = score_by_id.get(node.node_id)
                if sem is None:
                    continue
                sim = float(
                    getattr(sem, "similarity_score", 0.0)
                )
                conf = float(
                    getattr(sem, "confidence", 0.0)
                )
                multimodal_payloads.append({
                    "content_id": node.node_id,
                    "text": {
                        "score": sim,
                        "confidence": conf,
                        "energy": (
                            self._config
                            .baseline_energy_per_token_wh
                            * node.token_size
                        ),
                        "tokens": node.token_size,
                    },
                    "visual": {
                        "score": sim,
                        "confidence": conf,
                        "energy": node.file_weight,
                        "tokens": 256,
                    },
                })
            if multimodal_payloads:
                try:
                    fusion_results = (
                        self.fusion.fuse_multimodal_results(
                            results=multimodal_payloads,
                            fusion_method="adaptive",
                            energy_budget=energy_budget_wh,
                        )
                    )
                    modalities_used = ["text", "visual"]
                except Exception as exc:
                    if self._strict:
                        raise VimRAGIntegrationError(
                            f"multimodal fusion failed: {exc}"
                        ) from exc
                    warnings.append(
                        f"fusion_failed:"
                        f"{type(exc).__name__}"
                    )
                    logger.warning(
                        "multimodal fusion failed: %s", exc,
                    )

        # ---- 8. Context compression ----------------------------------
        compression_ratio = 1.0
        tokens_saved = 0
        if (
            retrieval_trace is not None
            and retrieval_trace.total_tokens > int(token_budget)
        ):
            compression_ratio = (
                self._config.default_compression_ratio
            )
            tokens_saved = int(
                retrieval_trace.total_tokens
                * (1 - compression_ratio)
            )

        # ---- 9. Aggregate metrics (independent of serendipity) ------
        actual_energy = (
            retrieval_trace.total_energy_wh
            if retrieval_trace is not None else 0.0
        )

        efficiency_gains = 0.0
        carbon_savings = 0.0
        if baseline_energy > 0 and actual_energy < baseline_energy:
            energy_saved = baseline_energy - actual_energy
            efficiency_gains = energy_saved / baseline_energy
            carbon_savings = (
                energy_saved
                * self._live_carbon_intensity_kg_per_wh
            )

        # Serendipity logging is independent of the metrics above.
        serendipity_events: List[Any] = []
        if (
            self.serendipity is not None
            and efficiency_gains > 0.0
        ):
            try:
                event = self.serendipity.log_efficiency_gain(
                    description=(
                        f"Efficient retrieval for query: "
                        f"{query[:50]}"
                    ),
                    baseline_energy_wh=baseline_energy,
                    actual_energy_wh=actual_energy,
                    retrieval_path=(
                        retrieval_trace.retrieval_path
                        if retrieval_trace is not None
                        else []
                    ),
                    agent_id=agent_id,
                )
                if event is not None:
                    serendipity_events.append(event)
            except Exception as exc:
                if self._strict:
                    raise VimRAGIntegrationError(
                        f"serendipity logging failed: {exc}"
                    ) from exc
                warnings.append(
                    f"serendipity_failed:"
                    f"{type(exc).__name__}"
                )
                logger.warning(
                    "serendipity logging failed: %s", exc,
                )

        # ---- Aggregation ---------------------------------------------
        total_carbon = (
            actual_energy
            * self._live_carbon_intensity_kg_per_wh
        )
        elapsed_ms = (
            datetime.now(timezone.utc) - start_time
        ).total_seconds() * 1000.0

        with self._lock:
            self.retrieval_count += 1
            self.total_energy_consumed += actual_energy
            self.total_carbon_emitted += total_carbon
            self.total_carbon_saved += carbon_savings

        result = IntegratedRetrievalResult(
            retrieved_nodes=tuple(retrieved_nodes),
            retrieval_trace=retrieval_trace,
            token_allocations=dict(token_allocations),
            topological_summary=dict(topo_summary),
            semantic_scores=tuple(semantic_scores),
            avg_similarity=float(avg_similarity),
            total_energy_wh=float(actual_energy),
            total_carbon_kg=float(total_carbon),
            retrieval_mode=str(retrieval_mode),
            pipeline=pipeline.value,
            carbon_savings_kg=float(carbon_savings),
            modalities_used=tuple(modalities_used),
            fusion_results=fusion_results,
            hierarchy_result=hierarchy_result,
            compression_ratio=float(compression_ratio),
            tokens_saved=int(tokens_saved),
            serendipity_events=tuple(serendipity_events),
            efficiency_gains=float(efficiency_gains),
            query=query,
            timestamp=start_time.timestamp(),
            total_time_ms=float(elapsed_ms),
            cache_hits=(
                int(self.semantic_scorer.cache_hits)
                if self.semantic_scorer is not None else 0
            ),
            warnings=tuple(warnings),
        )

        if self._result_history is not None:
            try:
                self._result_history.append(result)
            except Exception:  # pragma: no cover
                pass

        logger.debug(
            "Retrieval complete: query=%r, pipeline=%s, "
            "nodes=%d, energy=%.6f Wh, carbon=%.6f kg, "
            "mode=%s, time=%.2fms, warnings=%d",
            query, pipeline.value, len(retrieved_nodes),
            actual_energy, total_carbon, retrieval_mode,
            elapsed_ms, len(warnings),
        )
        return result

    # ---------------------------------------------------------- statistics
    def get_integration_stats(self) -> Dict[str, Any]:
        """Return comprehensive integration statistics."""
        with self._lock:
            retrievals = self.retrieval_count
            energy = self.total_energy_consumed
            carbon = self.total_carbon_emitted
            saved = self.total_carbon_saved

        stats: Dict[str, Any] = {
            "total_retrievals": retrievals,
            "total_energy_consumed_wh": energy,
            "total_carbon_emitted_kg": carbon,
            "total_carbon_saved_kg": saved,
            "avg_energy_per_retrieval": (
                energy / retrievals if retrievals > 0 else 0.0
            ),
            "memory_graph_nodes": len(
                self.core_retrieval.memory_graph
            ),
            "memory_graph_edges": sum(
                len(edges)
                for edges in
                self.core_retrieval.graph_edges.values()
            ),
            "live_carbon_intensity_kg_per_wh": (
                self._live_carbon_intensity_kg_per_wh
            ),
            "carbon_controller_available": (
                self.carbon_controller is not None
            ),
            "token_filter_available": (
                self.token_filter is not None
            ),
            "semantic_scorer_available": (
                self.semantic_scorer is not None
            ),
            "topological_allocator_available": (
                self.topo_allocator is not None
            ),
            "serendipity_available": (
                self.serendipity is not None
            ),
            "networkx_available": _NETWORKX_AVAILABLE,
            "numpy_available": _NUMPY_AVAILABLE,
        }

        for attr, method in (
            ("carbon_controller", "get_controller_stats"),
            ("token_filter", "get_filtering_stats"),
            ("semantic_scorer", "get_cache_stats"),
            ("serendipity", "get_serendipity_summary"),
        ):
            module = getattr(self, attr, None)
            if module is None:
                continue
            fn = getattr(module, method, None)
            if fn is None:
                continue
            try:
                stats[attr] = fn()
            except Exception as exc:  # pragma: no cover
                logger.debug(
                    "%s.%s() failed: %s", attr, method, exc,
                )

        return stats

    def statistics(self) -> Dict[str, Any]:
        return self.get_integration_stats()

    # ---------------------------------------------------------- grid
    def update_grid_intensity(self, intensity_g_kwh: float) -> None:
        """
        Update the carbon grid intensity across all modules.

        Also updates ``_live_carbon_intensity_kg_per_wh`` so subsequent
        per-retrieval carbon accounting reflects the new value. The
        frozen ``config.default_carbon_intensity_kg_per_wh`` remains
        unchanged.
        """
        if (
            isinstance(intensity_g_kwh, bool)
            or not isinstance(intensity_g_kwh, numbers.Real)
        ):
            raise VimRAGIntegrationError(
                "intensity_g_kwh must be numeric."
            )
        fv = float(intensity_g_kwh)
        if not math.isfinite(fv) or fv < 0:
            raise VimRAGIntegrationError(
                f"intensity_g_kwh must be finite and >= 0, got "
                f"{intensity_g_kwh!r}."
            )

        # Update the live carbon intensity (g/kWh → kg/Wh).
        with self._lock:
            self._live_carbon_intensity_kg_per_wh = fv * 1e-3

        if self.carbon_controller is not None:
            update = getattr(
                self.carbon_controller,
                "update_carbon_intensity",
                None,
            )
            if update is not None:
                try:
                    update(fv)
                except Exception as exc:
                    if self._strict:
                        raise VimRAGIntegrationError(
                            f"carbon controller update failed: "
                            f"{exc}"
                        ) from exc
                    logger.warning(
                        "carbon controller update failed: %s",
                        exc,
                    )
            else:
                logger.warning(
                    "carbon_controller lacks "
                    "update_carbon_intensity; live intensity "
                    "updated but controller state is stale.",
                )

        if self.token_filter is not None:
            update = getattr(
                self.token_filter, "update_grid_intensity", None,
            )
            if update is not None:
                try:
                    update(fv)
                except Exception as exc:
                    if self._strict:
                        raise VimRAGIntegrationError(
                            f"token filter update failed: {exc}"
                        ) from exc
                    logger.warning(
                        "token filter update failed: %s", exc,
                    )

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_graph: bool = False) -> None:
        """
        Reset counters. Optionally clear the memory graph.
        """
        with self._lock:
            self.retrieval_count = 0
            self.total_energy_consumed = 0.0
            self.total_carbon_emitted = 0.0
            self.total_carbon_saved = 0.0
            self._live_carbon_intensity_kg_per_wh = (
                self._config
                .default_carbon_intensity_kg_per_wh
            )
            if self._result_history is not None:
                self._result_history.clear()

        if clear_graph:
            reset_fn = getattr(
                self.core_retrieval, "reset", None,
            )
            if reset_fn is not None:
                try:
                    reset_fn(clear_graph=True)
                except Exception as exc:
                    if self._strict:
                        raise VimRAGIntegrationError(
                            f"core_retrieval.reset failed: {exc}"
                        ) from exc
                    logger.warning(
                        "core_retrieval.reset failed: %s", exc,
                    )
            if self.graph is not None:
                try:
                    self.graph.clear()
                except Exception:  # pragma: no cover
                    pass

        logger.debug("VimRAGIntegration reset.")

    # ---------------------------------------------------------- helpers
    def _estimate_baseline_energy(
        self, query: str, token_budget: int,
    ) -> float:
        """
        Estimate baseline energy for serendipity comparison.

        The ``query`` argument is preserved for backward
        compatibility; only the token budget is currently used. A
        future version may weight by query complexity.
        """
        return max(0, int(token_budget)) * (
            self._config.baseline_energy_per_token_wh
        )

    def _create_empty_result(
        self,
        query: str,
        reason: str,
        *,
        pipeline: str,
        start_time: Optional[datetime] = None,
        warnings: Tuple[str, ...] = tuple(),
    ) -> IntegratedRetrievalResult:
        """
        Create an empty result when retrieval is blocked. The
        retrieval counter is still incremented so blocked paths
        appear in statistics.
        """
        if start_time is None:
            start_time = datetime.now(timezone.utc)
        elapsed_ms = (
            datetime.now(timezone.utc) - start_time
        ).total_seconds() * 1000.0

        with self._lock:
            self.retrieval_count += 1

        logger.debug(
            "Empty retrieval result created (reason=%s, "
            "query=%r).", reason, query,
        )

        return IntegratedRetrievalResult(
            retrieved_nodes=tuple(),
            retrieval_trace=None,
            token_allocations={},
            topological_summary={"reason": reason},
            semantic_scores=tuple(),
            avg_similarity=0.0,
            total_energy_wh=0.0,
            total_carbon_kg=0.0,
            retrieval_mode="blocked",
            pipeline=pipeline,
            carbon_savings_kg=0.0,
            modalities_used=tuple(),
            fusion_results=None,
            hierarchy_result=None,
            compression_ratio=0.0,
            tokens_saved=0,
            serendipity_events=tuple(),
            efficiency_gains=0.0,
            query=query,
            timestamp=start_time.timestamp(),
            total_time_ms=float(elapsed_ms),
            cache_hits=0,
            warnings=warnings,
        )

    # ---------------------------------------------------------- export
    def export_integration_report(self, filepath: str) -> int:
        """
        Atomically export a comprehensive integration report.
        """
        if not isinstance(filepath, str) or not filepath:
            raise VimRAGIntegrationError(
                "filepath must be a non-empty string."
            )
        path = Path(filepath)

        report = _safe({
            "version": VERSION,
            "statistics": self.get_integration_stats(),
            "configuration": {
                "grid_region": self._config.grid_region,
                "carbon_adaptation_enabled": (
                    self.carbon_controller is not None
                ),
                "semantic_scoring_enabled": (
                    self.semantic_scorer is not None
                ),
                "topological_allocation_enabled": (
                    self.topo_allocator is not None
                ),
                "serendipity_logging_enabled": (
                    self.serendipity is not None
                ),
                "networkx_available": _NETWORKX_AVAILABLE,
                "numpy_available": _NUMPY_AVAILABLE,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise VimRAGIntegrationError(
                f"could not create parent directory for "
                f"{path}: {exc}"
            ) from exc

        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=path.name + ".",
                suffix=".tmp",
                dir=str(path.parent),
            )
            try:
                with os.fdopen(
                    fd, "w", encoding="utf-8",
                ) as f:
                    json.dump(
                        report, f,
                        indent=2,
                        allow_nan=False,
                        sort_keys=True,
                        default=_safe_json_default,
                    )
                os.replace(tmp_path, path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except (OSError, ValueError, TypeError) as exc:
            raise VimRAGIntegrationError(
                f"could not write integration report to "
                f"{path}: {exc}"
            ) from exc

        size = path.stat().st_size
        logger.debug(
            "Exported integration report to %s (%d bytes).",
            path, size,
        )
        return size

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_history: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            counters = {
                "retrieval_count": self.retrieval_count,
                "total_energy_consumed_wh": (
                    self.total_energy_consumed
                ),
                "total_carbon_emitted_kg": (
                    self.total_carbon_emitted
                ),
                "total_carbon_saved_kg": (
                    self.total_carbon_saved
                ),
            }
            history: Optional[List[Dict[str, Any]]] = None
            if include_history and (
                self._result_history is not None
            ):
                history = [
                    r.to_dict() for r in self._result_history
                ]

        payload: Dict[str, Any] = {
            "version": VERSION,
            "config": self._config.to_dict(),
            "strict": self._strict,
            "counters": counters,
            "live_carbon_intensity_kg_per_wh": (
                self._live_carbon_intensity_kg_per_wh
            ),
            "modules": {
                "carbon_controller": (
                    self.carbon_controller is not None
                ),
                "token_filter": self.token_filter is not None,
                "hierarchical": (
                    self.hierarchical is not None
                ),
                "fusion": self.fusion is not None,
                "compressor": self.compressor is not None,
                "visual_handler": (
                    self.visual_handler is not None
                ),
                "serendipity": self.serendipity is not None,
                "topological_allocator": (
                    self.topo_allocator is not None
                ),
                "semantic_scorer": (
                    self.semantic_scorer is not None
                ),
            },
        }
        if history is not None:
            payload["result_history"] = history
        return payload

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *,
        strict: Optional[bool] = None,
    ) -> "VimRAGIntegration":
        """
        Rebuild a VimRAGIntegration from ``to_dict`` output.

        Note: the memory graph, traces, and per-module state are not
        serialized by ``to_dict`` — this restores configuration,
        counters, and live carbon intensity only. The caller must
        repopulate the graph.
        """
        if not isinstance(data, Mapping):
            raise VimRAGIntegrationError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        try:
            raw_config = data.get("config") or {}
            if not isinstance(raw_config, Mapping):
                raise VimRAGIntegrationError(
                    "config must be a Mapping."
                )
            cfg = VimRAGIntegrationConfig.from_dict(
                dict(raw_config)
            )

            strict_raw = data.get("strict", True)
            if strict is None:
                if not isinstance(strict_raw, bool):
                    raise VimRAGIntegrationError(
                        f"strict must be bool, got "
                        f"{type(strict_raw).__name__}."
                    )
                strict_flag = strict_raw
            else:
                strict_flag = strict

            rag = cls(config=cfg, strict=strict_flag)

            raw_counters = data.get("counters") or {}
            if not isinstance(raw_counters, Mapping):
                raise VimRAGIntegrationError(
                    "counters must be a Mapping."
                )
            try:
                rag.retrieval_count = int(
                    raw_counters.get("retrieval_count", 0)
                )
                rag.total_energy_consumed = float(
                    raw_counters.get(
                        "total_energy_consumed_wh", 0.0,
                    )
                )
                rag.total_carbon_emitted = float(
                    raw_counters.get(
                        "total_carbon_emitted_kg", 0.0,
                    )
                )
                rag.total_carbon_saved = float(
                    raw_counters.get(
                        "total_carbon_saved_kg", 0.0,
                    )
                )
            except (TypeError, ValueError) as exc:
                raise VimRAGIntegrationError(
                    f"invalid counter: {exc}"
                ) from exc
            for name, value in (
                ("retrieval_count", rag.retrieval_count),
                (
                    "total_energy_consumed_wh",
                    rag.total_energy_consumed,
                ),
                (
                    "total_carbon_emitted_kg",
                    rag.total_carbon_emitted,
                ),
                (
                    "total_carbon_saved_kg",
                    rag.total_carbon_saved,
                ),
            ):
                if value < 0:
                    raise VimRAGIntegrationError(
                        f"counter {name} must be >= 0, "
                        f"got {value}."
                    )

            if "live_carbon_intensity_kg_per_wh" in data:
                try:
                    lc = float(
                        data["live_carbon_intensity_kg_per_wh"]
                    )
                except (TypeError, ValueError) as exc:
                    raise VimRAGIntegrationError(
                        f"invalid live_carbon_intensity_kg_per_wh: "
                        f"{exc}"
                    ) from exc
                if not math.isfinite(lc) or lc < 0:
                    raise VimRAGIntegrationError(
                        f"live_carbon_intensity_kg_per_wh must "
                        f"be finite and >= 0, got {lc}."
                    )
                rag._live_carbon_intensity_kg_per_wh = lc

            return rag
        except VimRAGIntegrationError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise VimRAGIntegrationError(
                f"invalid VimRAGIntegration payload: {exc}"
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
        cls, payload: str, *,
        strict: Optional[bool] = None,
    ) -> "VimRAGIntegration":
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise VimRAGIntegrationError(
                f"invalid JSON payload: {exc}"
            ) from exc
        return cls.from_dict(raw, strict=strict)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "VimRAGIntegration":
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> Optional[bool]:
        if exc_type is not None:
            logger.warning(
                "VimRAGIntegration scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __len__(self) -> int:
        return len(self.core_retrieval.memory_graph)

    def __repr__(self) -> str:
        with self._lock:
            return (
                "VimRAGIntegration("
                f"region={self._config.grid_region!r}, "
                f"retrievals={self.retrieval_count}, "
                f"nodes={len(self.core_retrieval.memory_graph)}, "
                f"modules="
                f"{{semantic:"
                f"{self.semantic_scorer is not None}, "
                f"topo:"
                f"{self.topo_allocator is not None}, "
                f"carbon:"
                f"{self.carbon_controller is not None}, "
                f"serendipity:"
                f"{self.serendipity is not None}}}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Convenience factory
# --------------------------------------------------------------------------- #
def create_vimrag_pipeline(
    mode: str = "balanced",
    grid_region: str = "US-CA",
    *,
    strict: bool = False,
) -> VimRAGIntegration:
    """
    Create a VimRAG pipeline with a preset configuration.

    Parameters
    ----------
    mode : str
        One of ``"fast"``, ``"balanced"``, ``"green"``,
        ``"comprehensive"``.
    grid_region : str
        Carbon grid region identifier.
    strict : bool, default False
        If True, an unknown ``mode`` raises; otherwise it falls back
        to ``"balanced"`` with a warning.
    """
    if not isinstance(mode, str):
        raise VimRAGIntegrationError("mode must be a string.")
    mode_lower = mode.lower()
    if mode_lower not in (
        "fast", "balanced", "green", "comprehensive",
    ):
        msg = (
            f"unknown mode {mode!r}; expected one of fast, "
            f"balanced, green, comprehensive."
        )
        if strict:
            raise VimRAGIntegrationError(msg)
        logger.warning(
            "%s Falling back to 'balanced'.", msg,
        )
        mode_lower = "balanced"

    if mode_lower == "fast":
        return VimRAGIntegration(
            grid_region=grid_region,
            enable_semantic_scoring=False,
            enable_topological_allocation=False,
            enable_carbon_adaptation=False,
            enable_serendipity=False,
        )
    if mode_lower == "green":
        return VimRAGIntegration(
            grid_region=grid_region,
            enable_semantic_scoring=True,
            enable_topological_allocation=True,
            enable_carbon_adaptation=True,
            enable_serendipity=True,
        )
    if mode_lower == "comprehensive":
        return VimRAGIntegration(
            grid_region=grid_region,
            enable_semantic_scoring=True,
            enable_topological_allocation=True,
            enable_carbon_adaptation=True,
            enable_serendipity=True,
        )
    # balanced (default)
    return VimRAGIntegration(
        grid_region=grid_region,
        enable_semantic_scoring=True,
        enable_topological_allocation=False,
        enable_carbon_adaptation=True,
        enable_serendipity=True,
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

    vimrag = VimRAGIntegration()
    print("repr       :", vimrag)

    # ---- Document ingestion ------------------------------------- #
    d1 = vimrag.add_document(
        "Pneumonia symptoms include cough, fever, and chest pain.",
        "text",
        metadata={"topic": "medical"},
    )
    d2 = vimrag.add_document(
        "/path/to/chest_xray.png", "visual",
        metadata={"caption": "Chest X-ray showing opacity"},
    )
    d3 = vimrag.add_document(
        "Treatment protocol for bacterial pneumonia",
        "text",
    )
    print(f"added      : {d1}, {d2}, {d3}")
    assert d1 and d2 and d3

    # ---- Custom node_id — no rekey corruption ------------------ #
    custom = vimrag.add_document(
        "Custom content", "text", node_id="my_custom_id",
    )
    assert custom == "my_custom_id"
    assert "my_custom_id" in vimrag.core_retrieval.memory_graph
    # Original node must not linger under a ghost ID.
    assert len(vimrag.core_retrieval.memory_graph) == 4
    print("custom id  : OK")

    # Target ID already taken → strict error
    try:
        vimrag.add_document(
            "different content", "text", node_id="my_custom_id",
        )
    except VimRAGIntegrationError as exc:
        print("dup target :", str(exc)[:60])

    # ---- Duplicate content with on_duplicate_node="error" ------ #
    strict_dup = VimRAGIntegration(
        config=VimRAGIntegrationConfig(
            default_energy_budget_wh=0.05,
        ),
    )
    strict_dup.add_document("hello", "text")
    # Same content, no node_id — falls back to same ID, no crash.
    id_a = strict_dup.add_document("hello", "text")
    print("dup content:", id_a)

    # ---- Connect ----------------------------------------------- #
    vimrag.connect_documents(d1, d3, weight=0.8)
    vimrag.connect_documents(d1, d2, weight=0.5)
    print(
        "edges      :",
        vimrag.core_retrieval.graph_edges.get(d1),
    )

    # ---- Retrieve (BALANCED) ------------------------------------ #
    result = vimrag.retrieve(
        query="pneumonia diagnosis",
        pipeline=RetrievalPipeline.BALANCED,
        max_results=5,
    )
    print("BALANCED   :", result)
    assert result.pipeline == "balanced"
    assert result.retrieval_mode  # may be FULL or MINIMAL etc.

    # ---- Retrieve (FAST) with string pipeline ------------------ #
    fast = vimrag.retrieve(
        query="pneumonia", pipeline="fast", max_results=3,
    )
    assert fast.pipeline == "fast"
    print("FAST       :", fast)

    # ---- Retrieve (GREEN) -------------------------------------- #
    green = vimrag.retrieve(
        query="energy-efficient retrieval",
        pipeline="green",
    )
    print("GREEN      :", green)
    assert green.pipeline == "green"

    # ---- Retrieve (COMPREHENSIVE) ------------------------------ #
    comprehensive = vimrag.retrieve(
        query="X-ray pneumonia findings",
        pipeline=RetrievalPipeline.COMPREHENSIVE,
        include_visual=True,
    )
    print("COMPREHEN. :", comprehensive)
    # hierarchy_result now recorded (if hierarchical module present).
    if vimrag.hierarchical is not None:
        assert comprehensive.hierarchy_result is not None

    # ---- Statistics -------------------------------------------- #
    stats = vimrag.get_integration_stats()
    print("stats      :", {
        k: v for k, v in stats.items()
        if not isinstance(v, (dict, list))
    })

    # ---- Carbon-blocked path still counts ---------------------- #
    vimrag.update_grid_intensity(1000.0)
    blocked = vimrag.retrieve(
        "non-urgent query", max_results=5,
    )
    print(
        f"blocked    : mode={blocked.retrieval_mode} "
        f"pipeline={blocked.pipeline} "
        f"nodes={len(blocked.retrieved_nodes)}"
    )
    assert blocked.retrieval_mode in (
        "blocked", "MINIMAL", "minimal",
    )
    assert vimrag.retrieval_count > 0

    # ---- Live carbon intensity reflects update ----------------- #
    assert abs(
        vimrag.live_carbon_intensity_kg_per_wh - 1.0
    ) < 1e-6  # 1000 g/kWh = 1.0 kg/Wh
    print("live carbon: OK")

    vimrag.update_grid_intensity(100.0)
    print("grid reset : OK")

    # ---- Carbon savings independent of serendipity ------------- #
    no_seren = VimRAGIntegration(enable_serendipity=False)
    no_seren.add_document("hello world", "text")
    r = no_seren.retrieve("hello world")
    # Even without serendipity, savings metrics should be populated
    # when the actual energy is below the baseline.
    print(
        f"no-seren   : eff_gains={r.efficiency_gains:.4f} "
        f"carbon_saved={r.carbon_savings_kg:.8f}"
    )

    # ---- Pipeline string coercion error ------------------------ #
    try:
        vimrag.retrieve("q", pipeline="bogus")
    except VimRAGIntegrationError as exc:
        print("bad pipe   :", str(exc)[:60])

    # ---- Empty query rejected ---------------------------------- #
    try:
        vimrag.retrieve("")
    except VimRAGIntegrationError as exc:
        print("empty q    :", str(exc)[:60])

    # ---- Atomic export ----------------------------------------- #
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "nested" / "report.json"
        size = vimrag.export_integration_report(str(path))
        assert path.exists() and size > 0
        print(f"export     : {size} bytes")
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        assert "statistics" in data and "configuration" in data
        assert data["version"] == VERSION
        print("readback   : OK")

    # ---- Integration round-trip -------------------------------- #
    payload = vimrag.to_json()
    restored = VimRAGIntegration.from_json(payload)
    assert (
        restored.to_dict() == vimrag.to_dict()
    )
    print("round-trip : OK")

    # ---- Result round-trip ------------------------------------- #
    rd = result.to_dict()
    assert "pipeline" in rd and rd["pipeline"] == "balanced"
    assert "warnings" in rd
    rt_result = IntegratedRetrievalResult.from_dict(rd)
    assert rt_result.pipeline == "balanced"
    print("result rt  : OK")

    # ---- Reset ------------------------------------------------- #
    vimrag.reset()
    assert vimrag.retrieval_count == 0
    print("reset      : OK")

    # ---- Factory ----------------------------------------------- #
    for mode in ("fast", "balanced", "green", "comprehensive"):
        p = create_vimrag_pipeline(mode=mode)
        assert isinstance(p, VimRAGIntegration)
    print("factory    : OK")

    try:
        create_vimrag_pipeline(mode="bogus", strict=True)
    except VimRAGIntegrationError as exc:
        print("bad mode   :", str(exc)[:60])

    # ---- Context manager -------------------------------------- #
    with VimRAGIntegration() as scoped:
        scoped.add_document("ctx", "text")
        scoped.retrieve("ctx")
    print("context    : OK")

    # ---- Config validation ------------------------------------ #
    for bad_cfg in (
        dict(grid_region=""),
        dict(default_energy_budget_wh=0),
        dict(green_energy_budget_wh=0),
        dict(fast_token_budget=0),
        dict(balanced_token_budget=0),
        dict(min_token_budget=0),
        dict(default_carbon_intensity_kg_per_wh=-1),
        dict(baseline_energy_per_token_wh=0),
        dict(filter_fallback_multiplier=0),
        dict(filter_fallback_multiplier=1.5),
        dict(default_compression_ratio=0),
        dict(default_compression_ratio=1.5),
        dict(max_history=0),
    ):
        try:
            VimRAGIntegrationConfig(
                **bad_cfg  # type: ignore[arg-type]
            )
        except VimRAGIntegrationError as exc:
            print("Rejected c :", str(exc)[:50])

    # ---- from_dict error wrapping ----------------------------- #
    for bad in (
        {"config": {"fast_token_budget": True}},
        {"config": {"fast_token_budget": "abc"}},
        {"config": {"enable_semantic_scoring": 1}},
        {"strict": "yes"},
        {"counters": {"retrieval_count": -1}},
        {"counters": {"total_energy_consumed_wh": "abc"}},
        {"live_carbon_intensity_kg_per_wh": -1.0},
    ):
        try:
            VimRAGIntegration.from_dict(bad)
        except VimRAGIntegrationError as exc:
            print("Rejected p :", str(exc)[:60])

    # ---- to_json rejects NaN ---------------------------------- #
    tainted = VimRAGIntegration()
    tainted.add_document("x", "text")
    object.__setattr__(
        tainted._config,
        "baseline_energy_per_token_wh",
        float("nan"),
    )
    try:
        tainted.to_json()
    except ValueError as exc:
        print("NaN json   :", str(exc)[:60])

    print("\nSmoke test passed.")
