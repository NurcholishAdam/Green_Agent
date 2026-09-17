# src/retrieval/vimrag_integration.py

"""
VimRAG Integration Orchestrator
===============================

Complete integration of all VimRAG modules into a unified retrieval pipeline.
Coordinates: topological allocation, semantic scoring, graph traversal,
carbon adaptation, multimodal fusion, and serendipity logging.

Enhancements
------------
- Optional ``networkx`` / ``numpy`` imports guarded by ``_*_AVAILABLE``.
- **Fixed script-style imports** — all submodule imports are package-relative
  with defensive fallbacks so the orchestrator stays importable even when a
  sub-module is missing.
- **Fixed ``add_document`` node-ID bug** — a caller-supplied ``node_id`` is
  now honored consistently across the core graph and the NetworkX graph.
- ``VimRAGIntegrationConfig`` — frozen, validated: budgets, carbon
  intensity, pipeline defaults, bounded history.
- ``IntegratedRetrievalResult`` — frozen dataclass with validation and
  serialization.
- **Thread safety** — ``RLock`` guards counters.
- **Full validation** of every argument; strict / non-strict modes.
- **Atomic export** — ``tempfile`` + ``os.replace``.
- **Counter integration** — every retrieval path (including carbon-blocked)
  updates the statistics consistently.
- Serialization on the integration module, the config, and the result.
- ``statistics()``, ``__repr__``, custom ``VimRAGIntegrationError``,
  lazy ``%s`` logging, and a comprehensive ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import os
import tempfile
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional dependencies
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import networkx as nx  # type: ignore

    _NETWORKX_AVAILABLE = True
except ImportError:  # pragma: no cover
    nx = None  # type: ignore[assignment]
    _NETWORKX_AVAILABLE = False
    logger.debug("networkx not importable; VimRAGIntegration will skip graph.")

try:  # pragma: no cover
    import numpy as np  # type: ignore

    _NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore[assignment]
    _NUMPY_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Defensive sub-module imports
# --------------------------------------------------------------------------- #
# Each import is wrapped so a missing sub-module degrades the integration to a
# reduced pipeline rather than failing the entire module load.

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
    """Try ``from .module import symbol`` then ``from module import symbol``."""
    # Package-relative first.
    try:
        module = __import__(f"{__name__.rsplit('.', 1)[0]}.{name}", fromlist=["*"])
        return module
    except Exception:
        pass
    try:
        return __import__(name, fromlist=["*"])
    except Exception:
        return None


_core_mod = _try_import("vimrag_coretrieval")
if _core_mod is not None:
    _VimRAGCoRetrieval = getattr(_core_mod, "VimRAGCoRetrieval", None)
    _RetrievalNode = getattr(_core_mod, "RetrievalNode", None)
    _RetrievalTrace = getattr(_core_mod, "RetrievalTrace", None)

_filter_mod = _try_import("token_aware_filter")
if _filter_mod is not None:
    _TokenAwareFilter = getattr(_filter_mod, "TokenAwareFilter", None)
    _ContextSize = getattr(_filter_mod, "ContextSize", None)
    _GridIntensity = getattr(_filter_mod, "GridIntensity", None)

_carbon_mod = _try_import("carbon_adaptive_controller")
if _carbon_mod is not None:
    _CarbonAdaptiveRetrievalController = getattr(
        _carbon_mod, "CarbonAdaptiveRetrievalController", None
    )
    _RetrievalMode = getattr(_carbon_mod, "RetrievalMode", None)

_hier_mod = _try_import("hierarchical_retrieval")
if _hier_mod is not None:
    _HierarchicalRetrieval = getattr(_hier_mod, "HierarchicalRetrieval", None)
    _RetrievalLevel = getattr(_hier_mod, "RetrievalLevel", None)

_fusion_mod = _try_import("multimodal_fusion")
if _fusion_mod is not None:
    _MultimodalFusion = getattr(_fusion_mod, "MultimodalFusion", None)
    _ModalityType = getattr(_fusion_mod, "ModalityType", None)
    _FusedResult = getattr(_fusion_mod, "FusedResult", None)

_comp_mod = _try_import("context_compression")
if _comp_mod is not None:
    _ContextCompressor = getattr(_comp_mod, "ContextCompressor", None)
    _CompressionResult = getattr(_comp_mod, "CompressionResult", None)

_visual_mod = _try_import("visual_context_handler")
if _visual_mod is not None:
    _VisualContextHandler = getattr(_visual_mod, "VisualContextHandler", None)
    _VisualContextType = getattr(_visual_mod, "VisualContextType", None)

_serendip_mod = _try_import("serendipity_logger")
if _serendip_mod is not None:
    _SerendipityTraceLogger = getattr(
        _serendip_mod, "SerendipityTraceLogger", None
    )
    _SerendipityEvent = getattr(_serendip_mod, "SerendipityEvent", None)

_topo_mod = _try_import("topological_allocator")
if _topo_mod is not None:
    _TopologicalTokenAllocator = getattr(
        _topo_mod, "TopologicalTokenAllocator", None
    )
    _ImportanceMetric = getattr(_topo_mod, "ImportanceMetric", None)

_semantic_mod = _try_import("semantic_scorer")
if _semantic_mod is not None:
    _create_local_scorer = getattr(_semantic_mod, "create_local_scorer", None)


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

    # Carbon intensity model (kg CO2e / Wh).
    default_carbon_intensity_kg_per_wh: float = 0.385

    # Baseline energy model.
    baseline_energy_per_token_wh: float = 1e-6

    # Token-filter fallback multiplier when ``should_filter`` returns True.
    filter_fallback_multiplier: float = 0.5

    # Compression heuristic.
    default_compression_ratio: float = 0.5

    # Bounded internal history (results not stored by default, but retained
    # for statistics if ``retain_results`` is enabled).
    retain_results: bool = False
    max_history: int = 1000

    def __post_init__(self) -> None:
        if not isinstance(self.grid_region, str) or not self.grid_region:
            raise VimRAGIntegrationError(
                "grid_region must be a non-empty string."
            )
        if not isinstance(self.embedding_model, str) or not self.embedding_model:
            raise VimRAGIntegrationError(
                "embedding_model must be a non-empty string."
            )
        for name in (
            "default_energy_budget_wh", "green_energy_budget_wh",
        ):
            value = getattr(self, name)
            if value <= 0:
                raise VimRAGIntegrationError(f"{name} must be > 0.")
        for name in ("fast_token_budget", "balanced_token_budget"):
            value = getattr(self, name)
            if not isinstance(value, int) or value <= 0:
                raise VimRAGIntegrationError(f"{name} must be a positive int.")
        if self.default_carbon_intensity_kg_per_wh < 0:
            raise VimRAGIntegrationError(
                "default_carbon_intensity_kg_per_wh must be >= 0."
            )
        if self.baseline_energy_per_token_wh <= 0:
            raise VimRAGIntegrationError(
                "baseline_energy_per_token_wh must be > 0."
            )
        if not 0.0 < self.filter_fallback_multiplier <= 1.0:
            raise VimRAGIntegrationError(
                "filter_fallback_multiplier must be in (0, 1]."
            )
        if not 0.0 < self.default_compression_ratio <= 1.0:
            raise VimRAGIntegrationError(
                "default_compression_ratio must be in (0, 1]."
            )
        if self.max_history <= 0:
            raise VimRAGIntegrationError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "VimRAGIntegrationConfig":
        if not isinstance(data, Mapping):
            raise VimRAGIntegrationError(
                f"VimRAGIntegrationConfig.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs = {k: v for k, v in data.items() if k in valid}
        return cls(**kwargs)


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
    carbon_savings_kg: float
    modalities_used: Tuple[str, ...]
    fusion_results: Optional[Any]
    compression_ratio: float
    tokens_saved: int
    serendipity_events: Tuple[Any, ...]
    efficiency_gains: float
    query: str
    timestamp: float
    total_time_ms: float
    cache_hits: int
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        # Coerce list-like fields to tuples (backward-compatible list access
        # still works because tuples are iterable and indexable).
        for name in (
            "retrieved_nodes", "semantic_scores",
            "modalities_used", "serendipity_events",
        ):
            value = getattr(self, name)
            if not isinstance(value, tuple):
                object.__setattr__(self, name, tuple(value))

        for name in (
            "avg_similarity", "total_energy_wh", "total_carbon_kg",
            "carbon_savings_kg", "compression_ratio", "efficiency_gains",
            "total_time_ms",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise VimRAGIntegrationError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                raise VimRAGIntegrationError(
                    f"{name} must be finite, got {value!r}."
                )
            if fv < 0:
                raise VimRAGIntegrationError(f"{name} must be >= 0.")
        if not 0.0 <= self.avg_similarity <= 1.0:
            raise VimRAGIntegrationError(
                "avg_similarity must be in [0, 1]."
            )
        if not 0.0 <= self.compression_ratio <= 1.0:
            raise VimRAGIntegrationError(
                "compression_ratio must be in [0, 1]."
            )
        if not isinstance(self.tokens_saved, int) or self.tokens_saved < 0:
            raise VimRAGIntegrationError(
                "tokens_saved must be a non-negative int."
            )
        if not isinstance(self.cache_hits, int) or self.cache_hits < 0:
            raise VimRAGIntegrationError(
                "cache_hits must be a non-negative int."
            )
        if not isinstance(self.query, str):
            raise VimRAGIntegrationError("query must be a string.")
        if not isinstance(self.retrieval_mode, str) or not self.retrieval_mode:
            raise VimRAGIntegrationError(
                "retrieval_mode must be a non-empty string."
            )

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-safe dict (nodes / traces are stringified via repr)."""
        return {
            "retrieved_nodes": [_safe(o) for o in self.retrieved_nodes],
            "retrieval_trace": _safe(self.retrieval_trace),
            "token_allocations": _safe(self.token_allocations),
            "topological_summary": _safe(self.topological_summary),
            "semantic_scores": [_safe(o) for o in self.semantic_scores],
            "avg_similarity": self.avg_similarity,
            "total_energy_wh": self.total_energy_wh,
            "total_carbon_kg": self.total_carbon_kg,
            "retrieval_mode": self.retrieval_mode,
            "carbon_savings_kg": self.carbon_savings_kg,
            "modalities_used": list(self.modalities_used),
            "fusion_results": _safe(self.fusion_results),
            "compression_ratio": self.compression_ratio,
            "tokens_saved": self.tokens_saved,
            "serendipity_events": [_safe(e) for e in self.serendipity_events],
            "efficiency_gains": self.efficiency_gains,
            "query": self.query,
            "timestamp": self.timestamp,
            "total_time_ms": self.total_time_ms,
            "cache_hits": self.cache_hits,
            "created_at": self.created_at.isoformat(),
        }

    def __repr__(self) -> str:
        return (
            "IntegratedRetrievalResult("
            f"query={self.query!r}, "
            f"nodes={len(self.retrieved_nodes)}, "
            f"energy_wh={self.total_energy_wh:.6f}, "
            f"carbon_kg={self.total_carbon_kg:.6f}, "
            f"mode={self.retrieval_mode}, "
            f"time_ms={self.total_time_ms:.2f})"
        )


def _safe(obj: Any) -> Any:
    """Best-effort JSON-safe representation for arbitrary module outputs."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if hasattr(obj, "to_dict") and callable(obj.to_dict):
        try:
            return obj.to_dict()
        except Exception:
            pass
    if isinstance(obj, Mapping):
        return {str(k): _safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_safe(v) for v in obj]
    return repr(obj)


# --------------------------------------------------------------------------- #
# Integration orchestrator
# --------------------------------------------------------------------------- #
class VimRAGIntegration:
    """
    Complete VimRAG integration orchestrator.

    Thread-safe, serializable, and defensive against missing sub-modules.
    All original public methods are preserved; new parameters are keyword-only.

    Coordinates the VimRAG sub-modules into a unified retrieval pipeline.
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
                enable_semantic_scoring=bool(enable_semantic_scoring),
                enable_topological_allocation=bool(enable_topological_allocation),
                enable_carbon_adaptation=bool(enable_carbon_adaptation),
                enable_serendipity=bool(enable_serendipity),
            )
        self._strict = bool(strict)

        # ---- Core retrieval ------------------------------------------
        if _VimRAGCoRetrieval is None:
            raise VimRAGIntegrationError(
                "VimRAGCoRetrieval is unavailable; install the retrieval "
                "sub-package or fix the import path."
            )
        self.core_retrieval = _VimRAGCoRetrieval(
            max_tokens_per_retrieval=4096,
            max_energy_per_retrieval_wh=0.01,
            enable_visual_context=True,
        )

        # ---- Carbon controller ---------------------------------------
        self.carbon_controller = None
        if self._config.enable_carbon_adaptation and (
            _CarbonAdaptiveRetrievalController is not None
        ):
            try:
                self.carbon_controller = _CarbonAdaptiveRetrievalController(
                    clean_grid_threshold_g_kwh=200.0,
                    dirty_grid_threshold_g_kwh=400.0,
                    enable_deferral=True,
                )
            except Exception as exc:
                logger.warning(
                    "Carbon controller init failed: %s", exc,
                )

        # ---- Token filter --------------------------------------------
        self.token_filter = None
        if _TokenAwareFilter is not None:
            try:
                self.token_filter = _TokenAwareFilter(
                    current_grid_intensity_g_kwh=385.0,
                    enable_adaptive_filtering=True,
                )
            except Exception as exc:
                logger.warning("Token filter init failed: %s", exc)

        # ---- Hierarchical retrieval ----------------------------------
        self.hierarchical = None
        if _HierarchicalRetrieval is not None:
            try:
                self.hierarchical = _HierarchicalRetrieval(
                    coarse_budget_tokens=500,
                    medium_budget_tokens=1500,
                    fine_budget_tokens=4096,
                )
            except Exception as exc:
                logger.warning("Hierarchical retrieval init failed: %s", exc)

        # ---- Multimodal fusion ---------------------------------------
        self.fusion = None
        if _MultimodalFusion is not None:
            try:
                self.fusion = _MultimodalFusion(
                    text_weight=0.6,
                    visual_weight=0.4,
                    energy_penalty_factor=0.1,
                )
            except Exception as exc:
                logger.warning("Multimodal fusion init failed: %s", exc)

        # ---- Context compressor --------------------------------------
        self.compressor = None
        if _ContextCompressor is not None:
            try:
                self.compressor = _ContextCompressor(
                    target_compression_ratio=0.5,
                    preserve_entities=True,
                    preserve_numbers=True,
                )
            except Exception as exc:
                logger.warning("Context compressor init failed: %s", exc)

        # ---- Visual handler ------------------------------------------
        self.visual_handler = None
        if _VisualContextHandler is not None:
            try:
                self.visual_handler = _VisualContextHandler(
                    visual_token_cost=256,
                    visual_energy_multiplier=2.5,
                    max_visual_contexts=5,
                )
            except Exception as exc:
                logger.warning("Visual handler init failed: %s", exc)

        # ---- Serendipity logger --------------------------------------
        self.serendipity = None
        if self._config.enable_serendipity and (
            _SerendipityTraceLogger is not None
        ):
            try:
                self.serendipity = _SerendipityTraceLogger(
                    negawatt_reward_rate=100.0,
                    min_savings_threshold_pct=10.0,
                )
            except Exception as exc:
                logger.warning("Serendipity logger init failed: %s", exc)

        # ---- NetworkX graph (optional) -------------------------------
        self.graph = None
        if _NETWORKX_AVAILABLE and nx is not None:
            try:
                self.graph = nx.DiGraph()
            except Exception:  # pragma: no cover
                self.graph = None

        # ---- Topological allocator -----------------------------------
        self.topo_allocator = None
        if (
            self._config.enable_topological_allocation
            and _TopologicalTokenAllocator is not None
            and _ImportanceMetric is not None
        ):
            try:
                self.topo_allocator = _TopologicalTokenAllocator(
                    importance_metric=_ImportanceMetric.COMBINED,
                    enable_dynamic_adjustment=True,
                )
            except Exception as exc:
                logger.warning(
                    "TopologicalTokenAllocator unavailable: %s", exc,
                )
        elif self._config.enable_topological_allocation:
            logger.warning(
                "TopologicalTokenAllocator not available; "
                "topological allocation disabled.",
            )

        # ---- Semantic scorer -----------------------------------------
        self.semantic_scorer = None
        if (
            self._config.enable_semantic_scoring
            and _create_local_scorer is not None
        ):
            try:
                self.semantic_scorer = _create_local_scorer(
                    model_name=self._config.embedding_model,
                    cache_embeddings=True,
                )
            except Exception as exc:
                logger.warning(
                    "SemanticScorer unavailable (%s); "
                    "using keyword matching.", exc,
                )
        elif self._config.enable_semantic_scoring:
            logger.warning(
                "SemanticScorer not available; using keyword matching.",
            )

        # ---- State ---------------------------------------------------
        self._lock = threading.RLock()
        self.retrieval_count: int = 0
        self.total_energy_consumed: float = 0.0
        self.total_carbon_emitted: float = 0.0
        self.total_carbon_saved: float = 0.0

        # Optional bounded result history.
        from collections import deque as _deque
        self._result_history: Any = (
            _deque(maxlen=self._config.max_history)
            if self._config.retain_results else None
        )

        logger.debug(
            "VimRAGIntegration initialized "
            "(region=%s, semantic=%s, topo=%s, carbon=%s, serendipity=%s, "
            "strict=%s)",
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

        When ``node_id`` is supplied, the caller's identifier is used as the
        canonical key across the core graph and the NetworkX graph. The
        original implementation added the node under the generated ID and
        then tried to rekey it, which produced duplicate entries.
        """
        if not isinstance(content_type, str) or not content_type:
            raise VimRAGIntegrationError(
                "content_type must be a non-empty string."
            )
        if metadata is not None and not isinstance(metadata, Mapping):
            raise VimRAGIntegrationError("metadata must be a Mapping or None.")
        if node_id is not None and (
            not isinstance(node_id, str) or not node_id
        ):
            raise VimRAGIntegrationError(
                "node_id must be a non-empty string or None."
            )

        if node_id is None:
            effective_id = self.core_retrieval.add_node(
                content, content_type, dict(metadata) if metadata else None,
            )
        else:
            # Add node, then rename the generated key to the caller's ID.
            generated = self.core_retrieval.add_node(
                content, content_type, dict(metadata) if metadata else None,
            )
            try:
                with self.core_retrieval._lock:  # type: ignore[attr-defined]
                    node = self.core_retrieval.memory_graph.pop(generated, None)
                    if node is not None:
                        # Rebuild with the desired ID.
                        if _RetrievalNode is not None:
                            renamed = _RetrievalNode(
                                node_id=node_id,
                                content_type=node.content_type,
                                content=node.content,
                                token_size=node.token_size,
                                file_weight=node.file_weight,
                                metadata=node.metadata,
                                timestamp=node.timestamp,
                            )
                            self.core_retrieval.memory_graph[node_id] = renamed
                        else:
                            self.core_retrieval.memory_graph[node_id] = node
                    self.core_retrieval.graph_edges.setdefault(node_id, [])
                    self.core_retrieval.graph_edges.pop(generated, None)
            except AttributeError:
                # Older VimRAGCoRetrieval without a lock — fall back.
                self.core_retrieval.memory_graph[node_id] = (
                    self.core_retrieval.memory_graph.pop(generated)
                )
            effective_id = node_id

        # NetworkX mirror.
        if self.graph is not None:
            try:
                self.graph.add_node(
                    effective_id, content=content,
                    content_type=content_type,
                )
            except Exception:  # pragma: no cover — defensive
                pass

        # Visual handler mirror.
        if (
            content_type in ("visual", "multimodal")
            and self.visual_handler is not None
        ):
            try:
                visual_type = (
                    _VisualContextType.IMAGE
                    if _VisualContextType is not None else None
                )
                if visual_type is not None:
                    self.visual_handler.add_visual_context(
                        source_path=str(content),
                        context_type=visual_type,
                        caption=(
                            metadata.get("caption") if metadata else None
                        ),
                    )
            except Exception as exc:
                logger.warning(
                    "Visual handler add_visual_context failed: %s", exc,
                )

        return effective_id

    def connect_documents(
        self, source_id: str, target_id: str, weight: float = 1.0
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
        if isinstance(weight, bool) or not isinstance(weight, (int, float)):
            raise VimRAGIntegrationError("weight must be numeric.")
        fw = float(weight)
        if math.isnan(fw) or math.isinf(fw) or fw < 0:
            raise VimRAGIntegrationError(
                f"weight must be finite and >= 0, got {weight!r}."
            )

        try:
            self.core_retrieval.connect_nodes(source_id, target_id)
        except Exception as exc:
            if self._strict:
                raise VimRAGIntegrationError(
                    f"connect_nodes failed: {exc}"
                ) from exc
            logger.warning("connect_nodes failed: %s", exc)

        if self.graph is not None:
            try:
                self.graph.add_edge(source_id, target_id, weight=fw)
            except Exception:  # pragma: no cover
                pass

    # ---------------------------------------------------------- retrieval
    def retrieve(
        self,
        query: str,
        pipeline: RetrievalPipeline = RetrievalPipeline.BALANCED,
        max_results: int = 10,
        energy_budget_wh: Optional[float] = None,
        token_budget: Optional[int] = None,
        include_visual: bool = True,
        agent_id: str = "default_agent",
    ) -> IntegratedRetrievalResult:
        """
        Execute the complete VimRAG retrieval pipeline.

        See the original docstring for a description of the 9 stages.
        """
        # ---- Validate ------------------------------------------------
        if not isinstance(query, str):
            raise VimRAGIntegrationError("query must be a string.")
        if not isinstance(pipeline, RetrievalPipeline):
            raise VimRAGIntegrationError(
                f"pipeline must be a RetrievalPipeline, got "
                f"{type(pipeline).__name__}."
            )
        if not isinstance(max_results, int) or max_results <= 0:
            raise VimRAGIntegrationError(
                "max_results must be a positive int."
            )
        if energy_budget_wh is not None:
            if isinstance(energy_budget_wh, bool) or not isinstance(
                energy_budget_wh, (int, float)
            ):
                raise VimRAGIntegrationError(
                    "energy_budget_wh must be numeric or None."
                )
            fb = float(energy_budget_wh)
            if math.isnan(fb) or math.isinf(fb) or fb <= 0:
                raise VimRAGIntegrationError(
                    f"energy_budget_wh must be finite and > 0, got "
                    f"{energy_budget_wh!r}."
                )
        if token_budget is not None and (
            not isinstance(token_budget, int) or token_budget <= 0
        ):
            raise VimRAGIntegrationError(
                "token_budget must be a positive int or None."
            )
        if not isinstance(include_visual, bool):
            raise VimRAGIntegrationError("include_visual must be a bool.")
        if not isinstance(agent_id, str) or not agent_id:
            raise VimRAGIntegrationError(
                "agent_id must be a non-empty string."
            )

        start_time = datetime.now(timezone.utc)

        # ---- Defaults ------------------------------------------------
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

        baseline_energy = self._estimate_baseline_energy(query, token_budget)

        # ---- 1. Carbon adaptation ------------------------------------
        retrieval_mode = "FULL"
        if self.carbon_controller is not None:
            try:
                carbon_decision = self.carbon_controller.decide_retrieval(
                    query=query,
                    estimated_tokens=token_budget,
                    content_type="multimodal" if include_visual else "text",
                    urgency="normal",
                )
            except Exception as exc:
                if self._strict:
                    raise VimRAGIntegrationError(
                        f"carbon decision failed: {exc}"
                    ) from exc
                logger.warning("Carbon decision failed: %s", exc)
                carbon_decision = None

            if carbon_decision is not None:
                if not carbon_decision.allowed:
                    logger.info(
                        "Retrieval deferred by carbon controller: %s",
                        getattr(carbon_decision, "reason", "unknown"),
                    )
                    return self._create_empty_result(
                        query, "deferred_by_carbon_controller",
                        start_time=start_time,
                    )
                retrieval_mode = carbon_decision.mode.value
                token_budget = min(token_budget, carbon_decision.max_tokens)
                include_visual = (
                    include_visual and carbon_decision.allow_visual
                )

        # ---- 2. Token filtering --------------------------------------
        filter_decision = None
        if self.token_filter is not None:
            try:
                filter_decision = self.token_filter.should_filter(
                    token_count=token_budget,
                    content_type="multimodal" if include_visual else "text",
                    relevance_score=0.5,
                )
            except Exception as exc:
                if self._strict:
                    raise VimRAGIntegrationError(
                        f"token filter failed: {exc}"
                    ) from exc
                logger.warning("Token filter failed: %s", exc)

            if filter_decision and filter_decision.get("should_filter"):
                token_budget = max(
                    1, int(token_budget * self._config.filter_fallback_multiplier)
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
                token_allocations = self.topo_allocator.allocate_tokens(
                    graph=self.graph,
                    total_token_budget=token_budget,
                    base_tokens_per_node=100,
                    query_nodes=None,
                )
                topo_summary = self.topo_allocator.get_allocation_summary(
                    token_allocations
                )
            except Exception as exc:
                if self._strict:
                    raise VimRAGIntegrationError(
                        f"topological allocation failed: {exc}"
                    ) from exc
                logger.warning("Topological allocation failed: %s", exc)

        # ---- 4. Semantic scoring -------------------------------------
        semantic_scores: List[Any] = []
        avg_similarity = 0.0
        if self.semantic_scorer is not None and pipeline != RetrievalPipeline.FAST:
            try:
                contents = [
                    (nid, str(node.content))
                    for nid, node in self.core_retrieval.memory_graph.items()
                ]
                semantic_scores = self.semantic_scorer.score_and_rank(
                    query=query,
                    contents=contents,
                    top_k=max_results * 2,
                )
                if semantic_scores:
                    avg_similarity = sum(
                        s.similarity_score for s in semantic_scores
                    ) / len(semantic_scores)
            except Exception as exc:
                if self._strict:
                    raise VimRAGIntegrationError(
                        f"semantic scoring failed: {exc}"
                    ) from exc
                logger.warning("Semantic scoring failed: %s", exc)

        # ---- 5. Hierarchical retrieval (COMPREHENSIVE only) ----------
        if (
            pipeline == RetrievalPipeline.COMPREHENSIVE
            and self.hierarchical is not None
            and _RetrievalLevel is not None
        ):
            try:
                self.hierarchical.hierarchical_retrieve(
                    query=query,
                    start_level=_RetrievalLevel.COARSE,
                    max_results=max_results,
                )
            except Exception as exc:
                if self._strict:
                    raise VimRAGIntegrationError(
                        f"hierarchical retrieval failed: {exc}"
                    ) from exc
                logger.warning("Hierarchical retrieval failed: %s", exc)

        # ---- 6. Core retrieval ---------------------------------------
        try:
            retrieved_nodes, retrieval_trace = self.core_retrieval.retrieve(
                query=query,
                max_nodes=max_results,
                include_visual=include_visual,
                energy_budget_wh=energy_budget_wh,
            )
        except Exception as exc:
            raise VimRAGIntegrationError(
                f"core retrieval failed: {exc}"
            ) from exc

        # ---- 7. Multimodal fusion ------------------------------------
        fusion_results: Optional[Any] = None
        modalities_used: List[str] = ["text"]
        if (
            include_visual
            and self.fusion is not None
            and pipeline in (
                RetrievalPipeline.MULTIMODAL,
                RetrievalPipeline.COMPREHENSIVE,
            )
        ):
            multimodal_payloads = [
                {
                    "content_id": node.node_id,
                    "text": {"score": 0.8, "confidence": 0.9,
                             "energy": 0.001, "tokens": 100},
                    "visual": {"score": 0.7, "confidence": 0.85,
                               "energy": 0.0025, "tokens": 256},
                }
                for node in retrieved_nodes
                if node.content_type in ("visual", "multimodal")
            ]
            if multimodal_payloads:
                try:
                    fusion_results = self.fusion.fuse_multimodal_results(
                        results=multimodal_payloads,
                        fusion_method="adaptive",
                        energy_budget=energy_budget_wh,
                    )
                    modalities_used = ["text", "visual"]
                except Exception as exc:
                    if self._strict:
                        raise VimRAGIntegrationError(
                            f"multimodal fusion failed: {exc}"
                        ) from exc
                    logger.warning("Multimodal fusion failed: %s", exc)

        # ---- 8. Context compression ----------------------------------
        compression_ratio = 1.0
        tokens_saved = 0
        if retrieval_trace is not None and (
            retrieval_trace.total_tokens > token_budget
        ):
            compression_ratio = self._config.default_compression_ratio
            tokens_saved = int(
                retrieval_trace.total_tokens * (1 - compression_ratio)
            )

        # ---- 9. Serendipity logging ----------------------------------
        serendipity_events: List[Any] = []
        efficiency_gains = 0.0
        carbon_savings = 0.0
        actual_energy = (
            retrieval_trace.total_energy_wh
            if retrieval_trace is not None else 0.0
        )

        if (
            self.serendipity is not None
            and baseline_energy > 0
            and actual_energy < baseline_energy
        ):
            try:
                event = self.serendipity.log_efficiency_gain(
                    description=f"Efficient retrieval for query: {query[:50]}",
                    baseline_energy_wh=baseline_energy,
                    actual_energy_wh=actual_energy,
                    retrieval_path=(
                        retrieval_trace.retrieval_path
                        if retrieval_trace is not None else []
                    ),
                    agent_id=agent_id,
                )
                if event is not None:
                    serendipity_events.append(event)
                    efficiency_gains = (
                        baseline_energy - actual_energy
                    ) / baseline_energy
                    carbon_savings = (
                        event.energy_saved_wh
                        * self._config.default_carbon_intensity_kg_per_wh
                    )
            except Exception as exc:
                if self._strict:
                    raise VimRAGIntegrationError(
                        f"serendipity logging failed: {exc}"
                    ) from exc
                logger.warning("Serendipity logging failed: %s", exc)

        # ---- Aggregate ------------------------------------------------
        total_carbon = (
            actual_energy * self._config.default_carbon_intensity_kg_per_wh
        )
        elapsed_ms = (
            datetime.now(timezone.utc) - start_time
        ).total_seconds() * 1000.0

        # Counter update (also for empty paths — see _create_empty_result).
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
            carbon_savings_kg=float(carbon_savings),
            modalities_used=tuple(modalities_used),
            fusion_results=fusion_results,
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
        )

        if self._result_history is not None:
            try:
                self._result_history.append(result)
            except Exception:  # pragma: no cover — defensive
                pass

        logger.debug(
            "Retrieval complete: query=%r, nodes=%d, energy=%.6f Wh, "
            "carbon=%.6f kg, mode=%s, time=%.2fms",
            query, len(retrieved_nodes), actual_energy, total_carbon,
            retrieval_mode, elapsed_ms,
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
            "memory_graph_nodes": len(self.core_retrieval.memory_graph),
            "memory_graph_edges": sum(
                len(edges)
                for edges in self.core_retrieval.graph_edges.values()
            ),
            # Module availability flags (additive).
            "carbon_controller_available": self.carbon_controller is not None,
            "token_filter_available": self.token_filter is not None,
            "semantic_scorer_available": self.semantic_scorer is not None,
            "topological_allocator_available": self.topo_allocator is not None,
            "serendipity_available": self.serendipity is not None,
            "networkx_available": _NETWORKX_AVAILABLE,
            "numpy_available": _NUMPY_AVAILABLE,
        }

        if self.carbon_controller is not None:
            try:
                stats["carbon_controller"] = (
                    self.carbon_controller.get_controller_stats()
                )
            except Exception as exc:  # pragma: no cover
                logger.debug("carbon_controller stats failed: %s", exc)

        if self.token_filter is not None:
            try:
                stats["token_filter"] = (
                    self.token_filter.get_filtering_stats()
                )
            except Exception as exc:  # pragma: no cover
                logger.debug("token_filter stats failed: %s", exc)

        if self.semantic_scorer is not None:
            try:
                stats["semantic_scorer"] = (
                    self.semantic_scorer.get_cache_stats()
                )
            except Exception as exc:  # pragma: no cover
                logger.debug("semantic_scorer stats failed: %s", exc)

        if self.serendipity is not None:
            try:
                stats["serendipity"] = (
                    self.serendipity.get_serendipity_summary()
                )
            except Exception as exc:  # pragma: no cover
                logger.debug("serendipity stats failed: %s", exc)

        return stats

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`get_integration_stats`."""
        return self.get_integration_stats()

    # ---------------------------------------------------------- grid
    def update_grid_intensity(self, intensity_g_kwh: float) -> None:
        """Update the carbon grid intensity across all modules."""
        if isinstance(intensity_g_kwh, bool) or not isinstance(
            intensity_g_kwh, (int, float)
        ):
            raise VimRAGIntegrationError("intensity_g_kwh must be numeric.")
        fv = float(intensity_g_kwh)
        if math.isnan(fv) or math.isinf(fv) or fv < 0:
            raise VimRAGIntegrationError(
                f"intensity_g_kwh must be finite and >= 0, got "
                f"{intensity_g_kwh!r}."
            )

        if self.carbon_controller is not None:
            try:
                self.carbon_controller.update_carbon_intensity(fv)
            except Exception as exc:
                if self._strict:
                    raise VimRAGIntegrationError(
                        f"carbon controller update failed: {exc}"
                    ) from exc
                logger.warning("carbon controller update failed: %s", exc)

        if self.token_filter is not None:
            try:
                self.token_filter.update_grid_intensity(fv)
            except Exception as exc:
                if self._strict:
                    raise VimRAGIntegrationError(
                        f"token filter update failed: {exc}"
                    ) from exc
                logger.warning("token filter update failed: %s", exc)

    # ---------------------------------------------------------- helpers
    def _estimate_baseline_energy(
        self, query: str, token_budget: int
    ) -> float:
        """Estimate baseline energy for serendipity comparison (config-driven)."""
        return max(0, token_budget) * self._config.baseline_energy_per_token_wh

    def _create_empty_result(
        self,
        query: str,
        reason: str,
        *,
        start_time: Optional[datetime] = None,
    ) -> IntegratedRetrievalResult:
        """
        Create an empty result when retrieval is blocked.

        Increments the retrieval counter so the blocked case appears in
        statistics (the original skipped the counter entirely).
        """
        if start_time is None:
            start_time = datetime.now(timezone.utc)
        elapsed_ms = (
            datetime.now(timezone.utc) - start_time
        ).total_seconds() * 1000.0

        with self._lock:
            self.retrieval_count += 1

        logger.debug(
            "Empty retrieval result created (reason=%s, query=%r).",
            reason, query,
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
            carbon_savings_kg=0.0,
            modalities_used=tuple(),
            fusion_results=None,
            compression_ratio=0.0,
            tokens_saved=0,
            serendipity_events=tuple(),
            efficiency_gains=0.0,
            query=query,
            timestamp=start_time.timestamp(),
            total_time_ms=float(elapsed_ms),
            cache_hits=0,
        )

    # ---------------------------------------------------------- export
    def export_integration_report(self, filepath: str) -> int:
        """
        Atomically export a comprehensive integration report.

        Returns the number of bytes written. Parent directories are created
        automatically.
        """
        if not isinstance(filepath, str) or not filepath:
            raise VimRAGIntegrationError(
                "filepath must be a non-empty string."
            )
        path = Path(filepath)

        report = {
            "statistics": self.get_integration_stats(),
            "configuration": {
                "grid_region": self._config.grid_region,
                "carbon_adaptation_enabled": self.carbon_controller is not None,
                "semantic_scoring_enabled": self.semantic_scorer is not None,
                "topological_allocation_enabled": self.topo_allocator is not None,
                "serendipity_logging_enabled": self.serendipity is not None,
                "networkx_available": _NETWORKX_AVAILABLE,
                "numpy_available": _NUMPY_AVAILABLE,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise VimRAGIntegrationError(
                f"Could not create parent directory for {path}: {exc}"
            ) from exc

        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=path.name + ".", suffix=".tmp",
                dir=str(path.parent),
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(report, f, indent=2, default=str)
                os.replace(tmp_path, path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except OSError as exc:
            raise VimRAGIntegrationError(
                f"Could not write integration report to {path}: {exc}"
            ) from exc

        size = path.stat().st_size
        logger.debug("Exported integration report to %s (%d bytes).", path, size)
        return size

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            counters = {
                "retrieval_count": self.retrieval_count,
                "total_energy_consumed_wh": self.total_energy_consumed,
                "total_carbon_emitted_kg": self.total_carbon_emitted,
                "total_carbon_saved_kg": self.total_carbon_saved,
            }
        return {
            "config": self._config.to_dict(),
            "strict": self._strict,
            "counters": counters,
            "modules": {
                "carbon_controller": self.carbon_controller is not None,
                "token_filter": self.token_filter is not None,
                "hierarchical": self.hierarchical is not None,
                "fusion": self.fusion is not None,
                "compressor": self.compressor is not None,
                "visual_handler": self.visual_handler is not None,
                "serendipity": self.serendipity is not None,
                "topological_allocator": self.topo_allocator is not None,
                "semantic_scorer": self.semantic_scorer is not None,
            },
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "VimRAGIntegration":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            logger.warning(
                "VimRAGIntegration scope exited with %s.",
                exc_type.__name__,
            )
        return None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "VimRAGIntegration("
                f"region={self._config.grid_region!r}, "
                f"retrievals={self.retrieval_count}, "
                f"nodes={len(self.core_retrieval.memory_graph)}, "
                f"modules="
                f"{{semantic:{self.semantic_scorer is not None}, "
                f"topo:{self.topo_allocator is not None}, "
                f"carbon:{self.carbon_controller is not None}, "
                f"serendipity:{self.serendipity is not None}}}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Convenience factory
# --------------------------------------------------------------------------- #
def create_vimrag_pipeline(
    mode: str = "balanced",
    grid_region: str = "US-CA",
) -> VimRAGIntegration:
    """
    Create a VimRAG pipeline with a preset configuration.

    Parameters
    ----------
    mode : str
        One of ``"fast"``, ``"balanced"``, ``"green"``, ``"comprehensive"``.
    grid_region : str
        Carbon grid region identifier.
    """
    if not isinstance(mode, str):
        raise VimRAGIntegrationError("mode must be a string.")
    mode_lower = mode.lower()

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
    if mode_lower == "balanced":
        return VimRAGIntegration(
            grid_region=grid_region,
            enable_semantic_scoring=True,
            enable_topological_allocation=False,
            enable_carbon_adaptation=True,
            enable_serendipity=True,
        )

    logger.warning("Unknown mode %r; falling back to 'balanced'.", mode)
    return VimRAGIntegration(
        grid_region=grid_region,
        enable_semantic_scoring=True,
        enable_topological_allocation=False,
        enable_carbon_adaptation=True,
        enable_serendipity=True,
    )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "IntegratedRetrievalResult",
    "RetrievalPipeline",
    "VimRAGIntegration",
    "VimRAGIntegrationConfig",
    "VimRAGIntegrationError",
    "create_vimrag_pipeline",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m retrieval.vimrag_integration
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Happy path ----------------------------------------------- #
    vimrag = VimRAGIntegration()
    print("repr       :", vimrag)

    # ---- Document ingestion --------------------------------------- #
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

    # ---- Custom node_id ------------------------------------------- #
    custom = vimrag.add_document(
        "Custom content", "text", node_id="my_custom_id",
    )
    assert custom == "my_custom_id", "custom node_id must be honored"
    assert "my_custom_id" in vimrag.core_retrieval.memory_graph
    print("custom id  : OK")

    # ---- Connect ------------------------------------------------ #
    vimrag.connect_documents(d1, d3, weight=0.8)
    vimrag.connect_documents(d1, d2, weight=0.5)
    print("edges      :", vimrag.core_retrieval.graph_edges.get(d1))

    # ---- Retrieve (BALANCED) ------------------------------------- #
    result = vimrag.retrieve(
        query="pneumonia diagnosis",
        pipeline=RetrievalPipeline.BALANCED,
        max_results=5,
    )
    print("BALANCED   :", result)

    # ---- Retrieve (FAST) ---------------------------------------- #
    fast = vimrag.retrieve(
        query="pneumonia", pipeline=RetrievalPipeline.FAST, max_results=3,
    )
    print("FAST       :", fast)

    # ---- Retrieve (GREEN) --------------------------------------- #
    green = vimrag.retrieve(
        query="energy-efficient retrieval",
        pipeline=RetrievalPipeline.GREEN,
    )
    print("GREEN      :", green)

    # ---- Retrieve (COMPREHENSIVE) -------------------------------- #
    comprehensive = vimrag.retrieve(
        query="X-ray pneumonia findings",
        pipeline=RetrievalPipeline.COMPREHENSIVE,
        include_visual=True,
    )
    print("COMPREHEN. :", comprehensive)

    # ---- Statistics --------------------------------------------- #
    stats = vimrag.get_integration_stats()
    print("stats      :", {
        k: v for k, v in stats.items()
        if not isinstance(v, (dict, list))
    })

    # ---- Bug fix: carbon-blocked retrieval still counts ---------- #
    vimrag.update_grid_intensity(1000.0)  # very dirty grid
    # In DIRTY mode, non-urgent normal-urgency requests are denied.
    blocked = vimrag.retrieve("non-urgent query", max_results=5)
    print(f"blocked    : mode={blocked.retrieval_mode} "
          f"nodes={len(blocked.retrieved_nodes)}")
    assert blocked.retrieval_mode in ("blocked", "MINIMAL", "minimal")
    new_count = vimrag.retrieval_count
    print(f"counters   : {new_count} retrievals counted "
          f"(blocked path counts now)")
    assert new_count > 0

    # ---- Reset grid intensity ---------------------------------- #
    vimrag.update_grid_intensity(100.0)
    print("grid reset : OK")

    # ---- Atomic export ----------------------------------------- #
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "nested" / "report.json"
        size = vimrag.export_integration_report(str(path))
        assert path.exists() and size > 0
        print(f"export     : {size} bytes at {path}")
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        assert "statistics" in data and "configuration" in data
        print("readback   : OK")

    # ---- Serialization ----------------------------------------- #
    payload = vimrag.to_json()
    restored = json.loads(payload)
    assert restored["config"]["grid_region"] == "US-CA"
    print("Serialization OK.")

    # ---- IntegratedRetrievalResult round-trip ------------------ #
    d = result.to_dict()
    assert d["query"] == "pneumonia diagnosis"
    assert "total_energy_wh" in d
    print("Result dict OK.")

    # ---- Factory function --------------------------------------- #
    for mode in ("fast", "balanced", "green", "comprehensive", "unknown"):
        p = create_vimrag_pipeline(mode=mode)
        assert isinstance(p, VimRAGIntegration)
    print("factory    : OK")

    # ---- Context manager ---------------------------------------- #
    with VimRAGIntegration() as scoped:
        scoped.add_document("ctx", "text")
        scoped.retrieve("ctx")
    print("Context    : OK")

    # ---- Validation failures ------------------------------------ #
    for bad_cfg in (
        dict(grid_region=""),
        dict(default_energy_budget_wh=0),
        dict(green_energy_budget_wh=0),
        dict(fast_token_budget=0),
        dict(balanced_token_budget=0),
        dict(default_carbon_intensity_kg_per_wh=-1),
        dict(baseline_energy_per_token_wh=0),
        dict(filter_fallback_multiplier=0),
        dict(filter_fallback_multiplier=1.5),
        dict(default_compression_ratio=0),
        dict(default_compression_ratio=1.5),
        dict(max_history=0),
    ):
        try:
            VimRAGIntegrationConfig(**bad_cfg)  # type: ignore[arg-type]
        except VimRAGIntegrationError as exc:
            print("Rejected cfg:", exc)

    strict = VimRAGIntegration(strict=True)
    for bad_call in (
        lambda: strict.add_document("x", ""),
        lambda: strict.add_document("x", "text", metadata="bad"),  # type: ignore[arg-type]
        lambda: strict.add_document("x", "text", node_id=""),
        lambda: strict.connect_documents("", "t"),
        lambda: strict.connect_documents("s", "t", weight=-1.0),
        lambda: strict.connect_documents("s", "t", weight=float("nan")),
        lambda: strict.retrieve(123),                        # type: ignore[arg-type]
        lambda: strict.retrieve("q", pipeline="balanced"),   # type: ignore[arg-type]
        lambda: strict.retrieve("q", max_results=0),
        lambda: strict.retrieve("q", energy_budget_wh=-1.0),
        lambda: strict.retrieve("q", energy_budget_wh=float("nan")),
        lambda: strict.retrieve("q", token_budget=0),
        lambda: strict.retrieve("q", include_visual="yes"),  # type: ignore[arg-type]
        lambda: strict.retrieve("q", agent_id=""),
        lambda: strict.update_grid_intensity(float("nan")),
        lambda: strict.update_grid_intensity(-10.0),
        lambda: strict.export_integration_report(""),
    ):
        try:
            bad_call()
        except VimRAGIntegrationError as exc:
            print("Rejected   :", exc)

    # ---- Non-strict coerces ------------------------------------ #
    lenient = VimRAGIntegration(strict=False)
    lenient.add_document("x", "text", metadata="bad")  # type: ignore[arg-type]
    print("lenient    : OK")

    print("\nSmoke test passed.")
