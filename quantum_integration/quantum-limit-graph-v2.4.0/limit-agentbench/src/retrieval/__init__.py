# -*- coding: utf-8 -*-
"""
VimRAG-Inspired Retrieval Module
================================

Energy-efficient multimodal retrieval with carbon-adaptive controls.

Sub-packages and modules
------------------------
- ``vimrag_coretrieval``           — multimodal memory graph.
- ``token_aware_filter``           — token-aware filtering layer.
- ``serendipity_logger``           — efficiency-discovery logger.
- ``carbon_adaptive_controller``   — grid-aware retrieval gating.
- ``hierarchical_retrieval``       — coarse-to-fine traversal.
- ``multimodal_fusion``            — text + visual fusion engine.
- ``graph_traversal_optimizer``    — adjacency-based path optimizer.
- ``enhanced_graph_traversal``     — NetworkX-backed path optimizer.
- ``context_compression``          — compression + semantic chunking.
- ``visual_context_handler``       — visual context storage + alignment.
- ``vimrag_integration``           — orchestrator over the whole pipeline.

Enhancements
------------
- **Defensive sub-module imports** — every optional dependency (``networkx``,
  ``numpy``, ``sentence_transformers``) is guarded by a ``_*_AVAILABLE``
  flag. A missing sub-module degrades the package instead of breaking it.
- **Full public API re-export** — every symbol that the sub-modules expose
  is surfaced from :mod:`retrieval` for flat imports.
- **Registry pattern** — ``RETRIEVAL_REGISTRY`` maps human-readable names to
  their classes, with ``register_retrieval_component`` /
  ``get_retrieval_component`` / ``list_retrieval_components`` helpers.
- **Compatibility manifest** — ``RETRIEVAL_AVAILABILITY`` reports which
  sub-modules loaded successfully.
- Optional import-time validation via the
  ``GREEN_AGENT_VALIDATE_RETRIEVAL=1`` environment variable (fail-fast in CI).
- ``__version__`` constant for packaging / telemetry.
"""

from __future__ import annotations

import logging
import os as _os
from typing import Any, Dict, List, Optional, Type

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Package metadata
# --------------------------------------------------------------------------- #
__version__ = "2.4.0"


# --------------------------------------------------------------------------- #
# Availability manifest (populated by the guarded imports below)
# --------------------------------------------------------------------------- #
RETRIEVAL_AVAILABILITY: Dict[str, bool] = {}


# --------------------------------------------------------------------------- #
# Guarded sub-module imports
# --------------------------------------------------------------------------- #
# Each block attempts a package-relative import, logs the outcome, and
# records the availability. The package stays importable even when a
# sub-module's optional dependency is missing.

# ---- Core retrieval --------------------------------------------------------
try:
    from .vimrag_coretrieval import (
        RetrievalNode,
        RetrievalTrace,
        VimRAGCoRetrieval,
    )

    RETRIEVAL_AVAILABILITY["vimrag_coretrieval"] = True
except ImportError as exc:  # pragma: no cover — environment-dependent
    logger.warning("vimrag_coretrieval unavailable: %s", exc)
    RetrievalNode = None  # type: ignore[assignment,misc]
    RetrievalTrace = None  # type: ignore[assignment,misc]
    VimRAGCoRetrieval = None  # type: ignore[assignment,misc]
    RETRIEVAL_AVAILABILITY["vimrag_coretrieval"] = False


# ---- Token-aware filter ----------------------------------------------------
try:
    from .token_aware_filter import (
        ContextSize,
        FilteringPolicy,
        GridIntensity,
        TokenAwareFilter,
        TokenAwareFilterError,
    )

    RETRIEVAL_AVAILABILITY["token_aware_filter"] = True
except ImportError as exc:  # pragma: no cover
    logger.warning("token_aware_filter unavailable: %s", exc)
    ContextSize = None  # type: ignore[assignment,misc]
    FilteringPolicy = None  # type: ignore[assignment,misc]
    GridIntensity = None  # type: ignore[assignment,misc]
    TokenAwareFilter = None  # type: ignore[assignment,misc]
    TokenAwareFilterError = None  # type: ignore[assignment,misc]
    RETRIEVAL_AVAILABILITY["token_aware_filter"] = False


# ---- Serendipity logger ----------------------------------------------------
try:
    from .serendipity_logger import (
        NegawattReward,
        SerendipityEvent,
        SerendipityTraceLogger,
    )

    RETRIEVAL_AVAILABILITY["serendipity_logger"] = True
except ImportError as exc:  # pragma: no cover
    logger.warning("serendipity_logger unavailable: %s", exc)
    NegawattReward = None  # type: ignore[assignment,misc]
    SerendipityEvent = None  # type: ignore[assignment,misc]
    SerendipityTraceLogger = None  # type: ignore[assignment,misc]
    RETRIEVAL_AVAILABILITY["serendipity_logger"] = False


# ---- Carbon-adaptive controller -------------------------------------------
try:
    from .carbon_adaptive_controller import (
        CarbonAdaptiveRetrievalController,
        RetrievalDecision,
        RetrievalMode,
    )

    RETRIEVAL_AVAILABILITY["carbon_adaptive_controller"] = True
except ImportError as exc:  # pragma: no cover
    logger.warning("carbon_adaptive_controller unavailable: %s", exc)
    CarbonAdaptiveRetrievalController = None  # type: ignore[assignment,misc]
    RetrievalDecision = None  # type: ignore[assignment,misc]
    RetrievalMode = None  # type: ignore[assignment,misc]
    RETRIEVAL_AVAILABILITY["carbon_adaptive_controller"] = False


# ---- Hierarchical retrieval ------------------------------------------------
try:
    from .hierarchical_retrieval import (
        HierarchicalNode,
        HierarchicalRetrieval,
        RetrievalLevel,
    )

    RETRIEVAL_AVAILABILITY["hierarchical_retrieval"] = True
except ImportError as exc:  # pragma: no cover
    logger.warning("hierarchical_retrieval unavailable: %s", exc)
    HierarchicalNode = None  # type: ignore[assignment,misc]
    HierarchicalRetrieval = None  # type: ignore[assignment,misc]
    RetrievalLevel = None  # type: ignore[assignment,misc]
    RETRIEVAL_AVAILABILITY["hierarchical_retrieval"] = False


# ---- Multimodal fusion -----------------------------------------------------
try:
    from .multimodal_fusion import (
        FusedResult,
        FusionMethod,
        ModalityScore,
        ModalityType,
        MultimodalFusion,
    )

    RETRIEVAL_AVAILABILITY["multimodal_fusion"] = True
except ImportError as exc:  # pragma: no cover
    logger.warning("multimodal_fusion unavailable: %s", exc)
    FusedResult = None  # type: ignore[assignment,misc]
    FusionMethod = None  # type: ignore[assignment,misc]
    ModalityScore = None  # type: ignore[assignment,misc]
    ModalityType = None  # type: ignore[assignment,misc]
    MultimodalFusion = None  # type: ignore[assignment,misc]
    RETRIEVAL_AVAILABILITY["multimodal_fusion"] = False


# ---- Graph traversal optimizer (adjacency-based) --------------------------
try:
    from .graph_traversal_optimizer import (
        GraphEdge,
        GraphTraversalOptimizer,
        TraversalPath,
        TraversalStrategy,
    )

    RETRIEVAL_AVAILABILITY["graph_traversal_optimizer"] = True
except ImportError as exc:  # pragma: no cover
    logger.warning("graph_traversal_optimizer unavailable: %s", exc)
    GraphEdge = None  # type: ignore[assignment,misc]
    GraphTraversalOptimizer = None  # type: ignore[assignment,misc]
    TraversalPath = None  # type: ignore[assignment,misc]
    TraversalStrategy = None  # type: ignore[assignment,misc]
    RETRIEVAL_AVAILABILITY["graph_traversal_optimizer"] = False


# ---- Enhanced graph traversal (NetworkX) -----------------------------------
try:
    from .enhanced_graph_traversal import (
        EnhancedGraphTraversalOptimizer,
        GraphStatistics,
    )

    RETRIEVAL_AVAILABILITY["enhanced_graph_traversal"] = True
except ImportError as exc:  # pragma: no cover — optional networkx dep
    logger.warning("enhanced_graph_traversal unavailable: %s", exc)
    EnhancedGraphTraversalOptimizer = None  # type: ignore[assignment,misc]
    GraphStatistics = None  # type: ignore[assignment,misc]
    RETRIEVAL_AVAILABILITY["enhanced_graph_traversal"] = False


# ---- Context compression ---------------------------------------------------
try:
    from .context_compression import (
        CompressionResult,
        ContextCompressor,
        SemanticChunker,
    )

    RETRIEVAL_AVAILABILITY["context_compression"] = True
except ImportError as exc:  # pragma: no cover
    logger.warning("context_compression unavailable: %s", exc)
    CompressionResult = None  # type: ignore[assignment,misc]
    ContextCompressor = None  # type: ignore[assignment,misc]
    SemanticChunker = None  # type: ignore[assignment,misc]
    RETRIEVAL_AVAILABILITY["context_compression"] = False


# ---- Visual context handler ------------------------------------------------
try:
    from .visual_context_handler import (
        VisualContext,
        VisualContextHandler,
        VisualContextType,
        VisualRetrievalResult,
    )

    RETRIEVAL_AVAILABILITY["visual_context_handler"] = True
except ImportError as exc:  # pragma: no cover
    logger.warning("visual_context_handler unavailable: %s", exc)
    VisualContext = None  # type: ignore[assignment,misc]
    VisualContextHandler = None  # type: ignore[assignment,misc]
    VisualContextType = None  # type: ignore[assignment,misc]
    VisualRetrievalResult = None  # type: ignore[assignment,misc]
    RETRIEVAL_AVAILABILITY["visual_context_handler"] = False


# ---- Semantic scorer (optional embedding backend) --------------------------
try:
    from .semantic_scorer import (
        CustomEmbeddingProvider,
        EmbeddingModel,
        EmbeddingProvider,
        SemanticScore,
        SemanticScorer,
        create_custom_scorer,
        create_local_scorer,
        create_openai_scorer,
    )

    RETRIEVAL_AVAILABILITY["semantic_scorer"] = True
except ImportError as exc:  # pragma: no cover — optional sentence-transformers
    logger.warning("semantic_scorer unavailable: %s", exc)
    CustomEmbeddingProvider = None  # type: ignore[assignment,misc]
    EmbeddingModel = None  # type: ignore[assignment,misc]
    EmbeddingProvider = None  # type: ignore[assignment,misc]
    SemanticScore = None  # type: ignore[assignment,misc]
    SemanticScorer = None  # type: ignore[assignment,misc]
    create_custom_scorer = None  # type: ignore[assignment,misc]
    create_local_scorer = None  # type: ignore[assignment,misc]
    create_openai_scorer = None  # type: ignore[assignment,misc]
    RETRIEVAL_AVAILABILITY["semantic_scorer"] = False


# ---- Topological token allocator (optional networkx dep) -------------------
try:
    from .topological_allocator import (
        ImportanceMetric,
        ResolutionLevel,
        TokenAllocation,
        TopologicalTokenAllocator,
    )

    RETRIEVAL_AVAILABILITY["topological_allocator"] = True
except ImportError as exc:  # pragma: no cover — optional networkx dep
    logger.warning("topological_allocator unavailable: %s", exc)
    ImportanceMetric = None  # type: ignore[assignment,misc]
    ResolutionLevel = None  # type: ignore[assignment,misc]
    TokenAllocation = None  # type: ignore[assignment,misc]
    TopologicalTokenAllocator = None  # type: ignore[assignment,misc]
    RETRIEVAL_AVAILABILITY["topological_allocator"] = False


# ---- Integration orchestrator (depends on many of the above) ---------------
try:
    from .vimrag_integration import (
        IntegratedRetrievalResult,
        RetrievalPipeline,
        VimRAGIntegration,
    )

    RETRIEVAL_AVAILABILITY["vimrag_integration"] = True
except ImportError as exc:  # pragma: no cover
    logger.warning("vimrag_integration unavailable: %s", exc)
    IntegratedRetrievalResult = None  # type: ignore[assignment,misc]
    RetrievalPipeline = None  # type: ignore[assignment,misc]
    VimRAGIntegration = None  # type: ignore[assignment,misc]
    RETRIEVAL_AVAILABILITY["vimrag_integration"] = False


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    # Package metadata
    "__version__",
    # Core retrieval
    "VimRAGCoRetrieval",
    "RetrievalNode",
    "RetrievalTrace",
    # Filtering and control
    "TokenAwareFilter",
    "TokenAwareFilterError",
    "ContextSize",
    "FilteringPolicy",
    "GridIntensity",
    "SerendipityTraceLogger",
    "SerendipityEvent",
    "NegawattReward",
    "CarbonAdaptiveRetrievalController",
    "RetrievalDecision",
    "RetrievalMode",
    # Integration
    "VimRAGIntegration",
    "IntegratedRetrievalResult",
    "RetrievalPipeline",
    # Hierarchical retrieval
    "HierarchicalRetrieval",
    "RetrievalLevel",
    "HierarchicalNode",
    # Multimodal fusion
    "MultimodalFusion",
    "ModalityType",
    "ModalityScore",
    "FusedResult",
    "FusionMethod",
    # Graph traversal (adjacency-based)
    "GraphTraversalOptimizer",
    "TraversalStrategy",
    "TraversalPath",
    "GraphEdge",
    # Graph traversal (NetworkX-backed)
    "EnhancedGraphTraversalOptimizer",
    "GraphStatistics",
    # Context compression
    "ContextCompressor",
    "CompressionResult",
    "SemanticChunker",
    # Visual context
    "VisualContextHandler",
    "VisualContext",
    "VisualContextType",
    "VisualRetrievalResult",
    # Semantic scoring
    "SemanticScorer",
    "SemanticScore",
    "EmbeddingModel",
    "EmbeddingProvider",
    "CustomEmbeddingProvider",
    "create_local_scorer",
    "create_openai_scorer",
    "create_custom_scorer",
    # Topological allocation
    "TopologicalTokenAllocator",
    "ImportanceMetric",
    "ResolutionLevel",
    "TokenAllocation",
    # Registry
    "RETRIEVAL_REGISTRY",
    "RETRIEVAL_AVAILABILITY",
    "get_retrieval_component",
    "list_retrieval_components",
    "register_retrieval_component",
]


# --------------------------------------------------------------------------- #
# Component registry
# --------------------------------------------------------------------------- #
# Central lookup so benchmarking / instrumentation / dashboard layers can
# resolve a component by name without importing concrete classes directly.
#
# Note: only *available* classes are registered; a None entry is skipped so
# downstream code can rely on ``get_retrieval_component`` returning a live
# class rather than ``None``.
RETRIEVAL_REGISTRY: Dict[str, Type[Any]] = {}


def _register_if_available(name: str, cls: Optional[Type[Any]]) -> None:
    """Register ``cls`` under ``name`` when it is not ``None``."""
    if cls is not None:
        RETRIEVAL_REGISTRY[name] = cls


# ---- Top-level components ---------------------------------------------------
_register_if_available("integration", VimRAGIntegration)
_register_if_available("core", VimRAGCoRetrieval)
_register_if_available("hierarchical", HierarchicalRetrieval)

# ---- Filtering / control ----------------------------------------------------
_register_if_available("filter", TokenAwareFilter)
_register_if_available("carbon", CarbonAdaptiveRetrievalController)
_register_if_available("serendipity", SerendipityTraceLogger)

# ---- Fusion / compression ---------------------------------------------------
_register_if_available("fusion", MultimodalFusion)
_register_if_available("compressor", ContextCompressor)
_register_if_available("chunker", SemanticChunker)

# ---- Graph traversal --------------------------------------------------------
_register_if_available("graph", GraphTraversalOptimizer)
_register_if_available("enhanced_graph", EnhancedGraphTraversalOptimizer)

# ---- Visual / semantic ------------------------------------------------------
_register_if_available("visual", VisualContextHandler)
_register_if_available("semantic", SemanticScorer)
_register_if_available("topological", TopologicalTokenAllocator)


def register_retrieval_component(name: str, cls: Type[Any]) -> None:
    """
    Register a retrieval component class under ``name``.

    Raises
    ------
    ValueError
        If ``name`` is empty or already registered to a different class.
    TypeError
        If ``cls`` is not a class.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("Component name must be a non-empty string.")
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}.")
    existing = RETRIEVAL_REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(
            f"Retrieval component '{name}' already registered to "
            f"{existing.__name__}."
        )
    RETRIEVAL_REGISTRY[name] = cls
    logger.debug("Registered retrieval component '%s' -> %s", name, cls.__name__)


def get_retrieval_component(name: str) -> Type[Any]:
    """
    Return the retrieval component class registered under ``name``.

    Raises
    ------
    KeyError
        If ``name`` is not registered.
    """
    try:
        return RETRIEVAL_REGISTRY[name]
    except KeyError as exc:
        raise KeyError(
            f"Unknown retrieval component '{name}'. "
            f"Available: {sorted(RETRIEVAL_REGISTRY)}"
        ) from exc


def list_retrieval_components() -> Dict[str, str]:
    """Return a mapping of registered name -> class name."""
    return {n: c.__name__ for n, c in RETRIEVAL_REGISTRY.items()}


# --------------------------------------------------------------------------- #
# Optional import-time validation
# --------------------------------------------------------------------------- #
# Enable in CI with ``GREEN_AGENT_VALIDATE_RETRIEVAL=1`` to fail-fast if a
# sub-module is broken or a component cannot be constructed. Skipped by
# default so the import stays cheap.
if _os.environ.get("GREEN_AGENT_VALIDATE_RETRIEVAL") == "1":
    _missing = [k for k, ok in RETRIEVAL_AVAILABILITY.items() if not ok]
    if _missing:
        logger.warning(
            "Retrieval sub-modules unavailable at import time: %s",
            sorted(_missing),
        )
    for _name, _cls in list(RETRIEVAL_REGISTRY.items()):
        try:
            # Component classes are not necessarily zero-arg constructible
            # (e.g. `VimRAGCoRetrieval` accepts no required args, but
            # `SemanticScorer` needs a provider). We only verify that the
            # class is defined and inspectable — not that it can be
            # instantiated without arguments.
            if not callable(_cls):
                raise TypeError(f"{_cls!r} is not callable.")
            logger.debug(
                "Retrieval component '%s' (%s) OK.",
                _name, _cls.__name__,
            )
        except Exception:  # pragma: no cover — CI-only diagnostic
            logger.exception(
                "Retrieval component '%s' (%s) failed validation.",
                _name, _cls.__name__,
            )
            raise
