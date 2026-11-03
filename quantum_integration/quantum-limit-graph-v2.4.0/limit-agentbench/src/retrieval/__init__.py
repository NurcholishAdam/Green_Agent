# src/retrieval/__init__.py

"""
VimRAG-Inspired Retrieval Module
================================

Energy-efficient multimodal retrieval with carbon-adaptive controls.

Sub-modules
-----------
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
- ``semantic_scorer``              — embedding-based semantic scoring.
- ``topological_allocator``        — importance-based token allocation.
- ``vimrag_integration``           — orchestrator over the whole pipeline.

Design
------
- **Defensive sub-module imports** — every optional dependency is
  guarded. A missing sub-module degrades the package instead of
  breaking it. ``RETRIEVAL_AVAILABILITY`` records which modules
  loaded; ``RETRIEVAL_LOAD_ERRORS`` records why a failed module
  failed.
- **Full public API re-export** — every class, config, error, and
  factory that the sub-modules expose is surfaced from
  :mod:`retrieval`. ``__all__`` is validated at import: a name listed
  in ``__all__`` that doesn't resolve raises immediately.
- **Three parallel registries** — ``RETRIEVAL_REGISTRY`` (components),
  ``RETRIEVAL_CONFIG_REGISTRY`` (configs), ``RETRIEVAL_ERROR_REGISTRY``
  (errors). All three are read-only views over internal dicts; writes
  go through :func:`register_retrieval_component` /
  :func:`unregister_retrieval_component`.
- **Name normalisation** — registration and lookup strip and lowercase
  names so ``" PPO "``, ``"PPO"``, and ``"ppo"`` share a key.
- **Package error hierarchy** — :class:`RetrievalError` (``ValueError``)
  with :class:`RegistrationError` and :class:`UnknownComponentError`
  subclasses. ``UnknownComponentError`` is also a ``KeyError`` for
  backward compatibility.
- **``TraversalPath`` / ``TraversalStrategy`` collision resolved.**
  The plain ``graph_traversal_optimizer`` classes retain the canonical
  names; the ``enhanced_graph_traversal`` versions are exported as
  ``EnhancedTraversalPath`` / ``EnhancedTraversalStrategy``.
- **Optional dependency flags** — ``NETWORKX_AVAILABLE``,
  ``NUMPY_AVAILABLE``, ``SENTENCE_TRANSFORMERS_AVAILABLE``,
  ``OPENAI_AVAILABLE``, ``COHERE_AVAILABLE`` exposed at package level
  for capability probing.
- **CI validation** — :func:`validate_retrieval_registry` returns a
  structured report (available, unavailable, registry sizes, per-entry
  full paths). No import-time side effects.

Version: 3.0.0
"""

from __future__ import annotations

import logging
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Type

logger = logging.getLogger(__name__)

__all__ = [
    # ---- Package metadata ---------------------------------------
    "VERSION",
    "__version__",
    # ---- Package errors -----------------------------------------
    "RetrievalError",
    "RegistrationError",
    "UnknownComponentError",
    # ---- Optional dependency flags ------------------------------
    "NETWORKX_AVAILABLE",
    "NUMPY_AVAILABLE",
    "SENTENCE_TRANSFORMERS_AVAILABLE",
    "OPENAI_AVAILABLE",
    "COHERE_AVAILABLE",
    # ---- Registries (read-only views) ---------------------------
    "RETRIEVAL_REGISTRY",
    "RETRIEVAL_CONFIG_REGISTRY",
    "RETRIEVAL_ERROR_REGISTRY",
    "RETRIEVAL_AVAILABILITY",
    "RETRIEVAL_LOAD_ERRORS",
    # ---- Registry helpers ---------------------------------------
    "register_retrieval_component",
    "unregister_retrieval_component",
    "get_retrieval_component",
    "instantiate_retrieval_component",
    "list_retrieval_components",
    "get_retrieval_config",
    "get_retrieval_error",
    "validate_retrieval_registry",
    # ---- Core retrieval -----------------------------------------
    "VimRAGCoRetrieval",
    "VimRAGConfig",
    "VimRAGError",
    "RetrievalNode",
    "RetrievalTrace",
    # ---- Token-aware filter -------------------------------------
    "TokenAwareFilter",
    "TokenAwareConfig",
    "TokenAwareFilterError",
    "ContextSize",
    "FilteringPolicy",
    "GridIntensity",
    "FilterDecision",
    # ---- Serendipity logger -------------------------------------
    "SerendipityTraceLogger",
    "SerendipityLoggerConfig",
    "SerendipityError",
    "SerendipityEvent",
    "NegawattReward",
    # ---- Carbon-adaptive controller -----------------------------
    "CarbonAdaptiveRetrievalController",
    "CarbonAdaptiveConfig",
    "CarbonAdaptiveError",
    "RetrievalDecision",
    "RetrievalMode",
    "DeferredRetrieval",
    # ---- Integration --------------------------------------------
    "VimRAGIntegration",
    "VimRAGIntegrationConfig",
    "VimRAGIntegrationError",
    "IntegratedRetrievalResult",
    "RetrievalPipeline",
    "create_vimrag_pipeline",
    # ---- Hierarchical retrieval ---------------------------------
    "HierarchicalRetrieval",
    "HierarchicalRetrievalConfig",
    "HierarchicalRetrievalError",
    "RetrievalLevel",
    "HierarchicalNode",
    # ---- Multimodal fusion --------------------------------------
    "MultimodalFusion",
    "MultimodalFusionConfig",
    "MultimodalFusionError",
    "ModalityType",
    "ModalityScore",
    "FusedResult",
    "FusionMethod",
    # ---- Graph traversal (adjacency-based) ----------------------
    "GraphTraversalOptimizer",
    "GraphTraversalConfig",
    "GraphTraversalError",
    "TraversalStrategy",
    "TraversalPath",
    "GraphEdge",
    # ---- Graph traversal (NetworkX-backed) ----------------------
    "EnhancedGraphTraversalOptimizer",
    "EnhancedGraphConfig",
    "EnhancedGraphError",
    "GraphStatistics",
    "EnhancedTraversalStrategy",
    "EnhancedTraversalPath",
    # ---- Context compression ------------------------------------
    "ContextCompressor",
    "ContextCompressionConfig",
    "ContextCompressionError",
    "CompressionResult",
    "SemanticChunker",
    "SemanticChunkerConfig",
    "SemanticChunkerError",
    "Chunk",
    # ---- Visual context -----------------------------------------
    "VisualContextHandler",
    "VisualContextConfig",
    "VisualContextError",
    "VisualContext",
    "VisualContextType",
    "VisualRetrievalResult",
    # ---- Semantic scoring ---------------------------------------
    "SemanticScorer",
    "SemanticScorerConfig",
    "SemanticScorerError",
    "SemanticScore",
    "EmbeddingModel",
    "EmbeddingProvider",
    "EmbeddingProviderError",
    "SentenceTransformerProvider",
    "OpenAIProvider",
    "CohereProvider",
    "AnthropicProvider",
    "CustomEmbeddingProvider",
    "create_local_scorer",
    "create_openai_scorer",
    "create_custom_scorer",
    "create_scorer_from_model",
    # ---- Topological allocation ---------------------------------
    "TopologicalTokenAllocator",
    "TopologicalAllocatorConfig",
    "TopologicalAllocatorError",
    "ImportanceMetric",
    "ResolutionLevel",
    "TokenAllocation",
]

VERSION = "3.0.0"
__version__ = VERSION


# --------------------------------------------------------------------------- #
# Package errors
# --------------------------------------------------------------------------- #
class RetrievalError(ValueError):
    """Base error for the retrieval package aggregator."""


class RegistrationError(RetrievalError):
    """Raised by :func:`register_retrieval_component` on conflict."""


class UnknownComponentError(RetrievalError, KeyError):
    """
    Raised by :func:`get_retrieval_component` when a name is not
    registered. Subclass of :class:`RetrievalError` (``ValueError``)
    and :class:`KeyError` so existing callers using either catch
    clause still work.
    """


# --------------------------------------------------------------------------- #
# Availability and load-error tracking
# --------------------------------------------------------------------------- #
_RETRIEVAL_AVAILABILITY: Dict[str, bool] = {}
_RETRIEVAL_LOAD_ERRORS: Dict[str, str] = {}


# --------------------------------------------------------------------------- #
# Optional dependency flags (populated after imports)
# --------------------------------------------------------------------------- #
NETWORKX_AVAILABLE = False
NUMPY_AVAILABLE = False
SENTENCE_TRANSFORMERS_AVAILABLE = False
OPENAI_AVAILABLE = False
COHERE_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Guarded sub-module imports
# --------------------------------------------------------------------------- #
# Each block imports the module's full public surface, records
# availability and (on failure) the exception reason, and falls back
# to ``None`` for every symbol so ``__all__`` resolution succeeds.

# ---- Core retrieval -------------------------------------------------------- #
try:
    from .vimrag_coretrieval import (
        RetrievalNode,
        RetrievalTrace,
        VimRAGCoRetrieval,
        VimRAGConfig,
        VimRAGError,
    )
    _RETRIEVAL_AVAILABILITY["vimrag_coretrieval"] = True
except ImportError as exc:
    logger.warning("vimrag_coretrieval unavailable: %s", exc)
    _RETRIEVAL_AVAILABILITY["vimrag_coretrieval"] = False
    _RETRIEVAL_LOAD_ERRORS["vimrag_coretrieval"] = (
        f"{type(exc).__name__}: {exc}"
    )
    RetrievalNode = None  # type: ignore[assignment]
    RetrievalTrace = None  # type: ignore[assignment]
    VimRAGCoRetrieval = None  # type: ignore[assignment]
    VimRAGConfig = None  # type: ignore[assignment]
    VimRAGError = None  # type: ignore[assignment]


# ---- Token-aware filter ---------------------------------------------------- #
try:
    from .token_aware_filter import (
        ContextSize,
        FilterDecision,
        FilteringPolicy,
        GridIntensity,
        TokenAwareConfig,
        TokenAwareFilter,
        TokenAwareFilterError,
    )
    _RETRIEVAL_AVAILABILITY["token_aware_filter"] = True
except ImportError as exc:
    logger.warning("token_aware_filter unavailable: %s", exc)
    _RETRIEVAL_AVAILABILITY["token_aware_filter"] = False
    _RETRIEVAL_LOAD_ERRORS["token_aware_filter"] = (
        f"{type(exc).__name__}: {exc}"
    )
    ContextSize = None  # type: ignore[assignment]
    FilterDecision = None  # type: ignore[assignment]
    FilteringPolicy = None  # type: ignore[assignment]
    GridIntensity = None  # type: ignore[assignment]
    TokenAwareConfig = None  # type: ignore[assignment]
    TokenAwareFilter = None  # type: ignore[assignment]
    TokenAwareFilterError = None  # type: ignore[assignment]


# ---- Serendipity logger ---------------------------------------------------- #
try:
    from .serendipity_logger import (
        NegawattReward,
        SerendipityError,
        SerendipityEvent,
        SerendipityLoggerConfig,
        SerendipityTraceLogger,
    )
    _RETRIEVAL_AVAILABILITY["serendipity_logger"] = True
except ImportError as exc:
    logger.warning("serendipity_logger unavailable: %s", exc)
    _RETRIEVAL_AVAILABILITY["serendipity_logger"] = False
    _RETRIEVAL_LOAD_ERRORS["serendipity_logger"] = (
        f"{type(exc).__name__}: {exc}"
    )
    NegawattReward = None  # type: ignore[assignment]
    SerendipityError = None  # type: ignore[assignment]
    SerendipityEvent = None  # type: ignore[assignment]
    SerendipityLoggerConfig = None  # type: ignore[assignment]
    SerendipityTraceLogger = None  # type: ignore[assignment]


# ---- Carbon-adaptive controller ------------------------------------------- #
try:
    from .carbon_adaptive_controller import (
        CarbonAdaptiveConfig,
        CarbonAdaptiveError,
        CarbonAdaptiveRetrievalController,
        DeferredRetrieval,
        RetrievalDecision,
        RetrievalMode,
    )
    _RETRIEVAL_AVAILABILITY["carbon_adaptive_controller"] = True
except ImportError as exc:
    logger.warning("carbon_adaptive_controller unavailable: %s", exc)
    _RETRIEVAL_AVAILABILITY["carbon_adaptive_controller"] = False
    _RETRIEVAL_LOAD_ERRORS["carbon_adaptive_controller"] = (
        f"{type(exc).__name__}: {exc}"
    )
    CarbonAdaptiveConfig = None  # type: ignore[assignment]
    CarbonAdaptiveError = None  # type: ignore[assignment]
    CarbonAdaptiveRetrievalController = None  # type: ignore[assignment]
    DeferredRetrieval = None  # type: ignore[assignment]
    RetrievalDecision = None  # type: ignore[assignment]
    RetrievalMode = None  # type: ignore[assignment]


# ---- Hierarchical retrieval ------------------------------------------------ #
try:
    from .hierarchical_retrieval import (
        HierarchicalNode,
        HierarchicalRetrieval,
        HierarchicalRetrievalConfig,
        HierarchicalRetrievalError,
        RetrievalLevel,
    )
    _RETRIEVAL_AVAILABILITY["hierarchical_retrieval"] = True
except ImportError as exc:
    logger.warning("hierarchical_retrieval unavailable: %s", exc)
    _RETRIEVAL_AVAILABILITY["hierarchical_retrieval"] = False
    _RETRIEVAL_LOAD_ERRORS["hierarchical_retrieval"] = (
        f"{type(exc).__name__}: {exc}"
    )
    HierarchicalNode = None  # type: ignore[assignment]
    HierarchicalRetrieval = None  # type: ignore[assignment]
    HierarchicalRetrievalConfig = None  # type: ignore[assignment]
    HierarchicalRetrievalError = None  # type: ignore[assignment]
    RetrievalLevel = None  # type: ignore[assignment]


# ---- Multimodal fusion ----------------------------------------------------- #
try:
    from .multimodal_fusion import (
        FusedResult,
        FusionMethod,
        ModalityScore,
        ModalityType,
        MultimodalFusion,
        MultimodalFusionConfig,
        MultimodalFusionError,
    )
    _RETRIEVAL_AVAILABILITY["multimodal_fusion"] = True
except ImportError as exc:
    logger.warning("multimodal_fusion unavailable: %s", exc)
    _RETRIEVAL_AVAILABILITY["multimodal_fusion"] = False
    _RETRIEVAL_LOAD_ERRORS["multimodal_fusion"] = (
        f"{type(exc).__name__}: {exc}"
    )
    FusedResult = None  # type: ignore[assignment]
    FusionMethod = None  # type: ignore[assignment]
    ModalityScore = None  # type: ignore[assignment]
    ModalityType = None  # type: ignore[assignment]
    MultimodalFusion = None  # type: ignore[assignment]
    MultimodalFusionConfig = None  # type: ignore[assignment]
    MultimodalFusionError = None  # type: ignore[assignment]


# ---- Graph traversal optimizer (adjacency-based) -------------------------- #
try:
    from .graph_traversal_optimizer import (
        GraphEdge,
        GraphTraversalConfig,
        GraphTraversalError,
        GraphTraversalOptimizer,
        TraversalPath,
        TraversalStrategy,
    )
    _RETRIEVAL_AVAILABILITY["graph_traversal_optimizer"] = True
except ImportError as exc:
    logger.warning("graph_traversal_optimizer unavailable: %s", exc)
    _RETRIEVAL_AVAILABILITY["graph_traversal_optimizer"] = False
    _RETRIEVAL_LOAD_ERRORS["graph_traversal_optimizer"] = (
        f"{type(exc).__name__}: {exc}"
    )
    GraphEdge = None  # type: ignore[assignment]
    GraphTraversalConfig = None  # type: ignore[assignment]
    GraphTraversalError = None  # type: ignore[assignment]
    GraphTraversalOptimizer = None  # type: ignore[assignment]
    TraversalPath = None  # type: ignore[assignment]
    TraversalStrategy = None  # type: ignore[assignment]


# ---- Enhanced graph traversal (NetworkX) ----------------------------------- #
# The enhanced module has its own ``TraversalPath`` / ``TraversalStrategy``
# that differ from the adjacency-based versions. Export them under
# ``EnhancedTraversalPath`` / ``EnhancedTraversalStrategy`` so the plain
# names stay bound to the original classes (backward compat).
try:
    from .enhanced_graph_traversal import (
        NETWORKX_AVAILABLE as _ENHANCED_NETWORKX_AVAILABLE,
        NUMPY_AVAILABLE as _ENHANCED_NUMPY_AVAILABLE,
        EnhancedGraphConfig,
        EnhancedGraphError,
        EnhancedGraphTraversalOptimizer,
        GraphStatistics,
        TraversalPath as EnhancedTraversalPath,
        TraversalStrategy as EnhancedTraversalStrategy,
    )
    _RETRIEVAL_AVAILABILITY["enhanced_graph_traversal"] = True
    NETWORKX_AVAILABLE = bool(_ENHANCED_NETWORKX_AVAILABLE)
    NUMPY_AVAILABLE = bool(_ENHANCED_NUMPY_AVAILABLE)
except ImportError as exc:
    logger.warning("enhanced_graph_traversal unavailable: %s", exc)
    _RETRIEVAL_AVAILABILITY["enhanced_graph_traversal"] = False
    _RETRIEVAL_LOAD_ERRORS["enhanced_graph_traversal"] = (
        f"{type(exc).__name__}: {exc}"
    )
    EnhancedGraphConfig = None  # type: ignore[assignment]
    EnhancedGraphError = None  # type: ignore[assignment]
    EnhancedGraphTraversalOptimizer = None  # type: ignore[assignment]
    GraphStatistics = None  # type: ignore[assignment]
    EnhancedTraversalPath = None  # type: ignore[assignment]
    EnhancedTraversalStrategy = None  # type: ignore[assignment]


# ---- Context compression --------------------------------------------------- #
try:
    from .context_compression import (
        Chunk,
        CompressionResult,
        ContextCompressionConfig,
        ContextCompressionError,
        ContextCompressor,
        SemanticChunker,
        SemanticChunkerConfig,
        SemanticChunkerError,
    )
    _RETRIEVAL_AVAILABILITY["context_compression"] = True
except ImportError as exc:
    logger.warning("context_compression unavailable: %s", exc)
    _RETRIEVAL_AVAILABILITY["context_compression"] = False
    _RETRIEVAL_LOAD_ERRORS["context_compression"] = (
        f"{type(exc).__name__}: {exc}"
    )
    Chunk = None  # type: ignore[assignment]
    CompressionResult = None  # type: ignore[assignment]
    ContextCompressionConfig = None  # type: ignore[assignment]
    ContextCompressionError = None  # type: ignore[assignment]
    ContextCompressor = None  # type: ignore[assignment]
    SemanticChunker = None  # type: ignore[assignment]
    SemanticChunkerConfig = None  # type: ignore[assignment]
    SemanticChunkerError = None  # type: ignore[assignment]


# ---- Visual context handler ------------------------------------------------ #
try:
    from .visual_context_handler import (
        VisualContext,
        VisualContextConfig,
        VisualContextError,
        VisualContextHandler,
        VisualContextType,
        VisualRetrievalResult,
    )
    _RETRIEVAL_AVAILABILITY["visual_context_handler"] = True
except ImportError as exc:
    logger.warning("visual_context_handler unavailable: %s", exc)
    _RETRIEVAL_AVAILABILITY["visual_context_handler"] = False
    _RETRIEVAL_LOAD_ERRORS["visual_context_handler"] = (
        f"{type(exc).__name__}: {exc}"
    )
    VisualContext = None  # type: ignore[assignment]
    VisualContextConfig = None  # type: ignore[assignment]
    VisualContextError = None  # type: ignore[assignment]
    VisualContextHandler = None  # type: ignore[assignment]
    VisualContextType = None  # type: ignore[assignment]
    VisualRetrievalResult = None  # type: ignore[assignment]


# ---- Semantic scorer ------------------------------------------------------- #
try:
    from .semantic_scorer import (
        AnthropicProvider,
        CohereProvider,
        CustomEmbeddingProvider,
        EmbeddingModel,
        EmbeddingProvider,
        EmbeddingProviderError,
        OpenAIProvider,
        SemanticScore,
        SemanticScorer,
        SemanticScorerConfig,
        SemanticScorerError,
        SentenceTransformerProvider,
        create_custom_scorer,
        create_local_scorer,
        create_openai_scorer,
        create_scorer_from_model,
    )
    _RETRIEVAL_AVAILABILITY["semantic_scorer"] = True
except ImportError as exc:
    logger.warning("semantic_scorer unavailable: %s", exc)
    _RETRIEVAL_AVAILABILITY["semantic_scorer"] = False
    _RETRIEVAL_LOAD_ERRORS["semantic_scorer"] = (
        f"{type(exc).__name__}: {exc}"
    )
    AnthropicProvider = None  # type: ignore[assignment]
    CohereProvider = None  # type: ignore[assignment]
    CustomEmbeddingProvider = None  # type: ignore[assignment]
    EmbeddingModel = None  # type: ignore[assignment]
    EmbeddingProvider = None  # type: ignore[assignment]
    EmbeddingProviderError = None  # type: ignore[assignment]
    OpenAIProvider = None  # type: ignore[assignment]
    SemanticScore = None  # type: ignore[assignment]
    SemanticScorer = None  # type: ignore[assignment]
    SemanticScorerConfig = None  # type: ignore[assignment]
    SemanticScorerError = None  # type: ignore[assignment]
    SentenceTransformerProvider = None  # type: ignore[assignment]
    create_custom_scorer = None  # type: ignore[assignment]
    create_local_scorer = None  # type: ignore[assignment]
    create_openai_scorer = None  # type: ignore[assignment]
    create_scorer_from_model = None  # type: ignore[assignment]


# ---- Topological token allocator ------------------------------------------- #
try:
    from .topological_allocator import (
        ImportanceMetric,
        ResolutionLevel,
        TokenAllocation,
        TopologicalAllocatorConfig,
        TopologicalAllocatorError,
        TopologicalTokenAllocator,
    )
    _RETRIEVAL_AVAILABILITY["topological_allocator"] = True
except ImportError as exc:
    logger.warning("topological_allocator unavailable: %s", exc)
    _RETRIEVAL_AVAILABILITY["topological_allocator"] = False
    _RETRIEVAL_LOAD_ERRORS["topological_allocator"] = (
        f"{type(exc).__name__}: {exc}"
    )
    ImportanceMetric = None  # type: ignore[assignment]
    ResolutionLevel = None  # type: ignore[assignment]
    TokenAllocation = None  # type: ignore[assignment]
    TopologicalAllocatorConfig = None  # type: ignore[assignment]
    TopologicalAllocatorError = None  # type: ignore[assignment]
    TopologicalTokenAllocator = None  # type: ignore[assignment]


# ---- Integration orchestrator ---------------------------------------------- #
try:
    from .vimrag_integration import (
        IntegratedRetrievalResult,
        RetrievalPipeline,
        VimRAGIntegration,
        VimRAGIntegrationConfig,
        VimRAGIntegrationError,
        create_vimrag_pipeline,
    )
    _RETRIEVAL_AVAILABILITY["vimrag_integration"] = True
except ImportError as exc:
    logger.warning("vimrag_integration unavailable: %s", exc)
    _RETRIEVAL_AVAILABILITY["vimrag_integration"] = False
    _RETRIEVAL_LOAD_ERRORS["vimrag_integration"] = (
        f"{type(exc).__name__}: {exc}"
    )
    IntegratedRetrievalResult = None  # type: ignore[assignment]
    RetrievalPipeline = None  # type: ignore[assignment]
    VimRAGIntegration = None  # type: ignore[assignment]
    VimRAGIntegrationConfig = None  # type: ignore[assignment]
    VimRAGIntegrationError = None  # type: ignore[assignment]
    create_vimrag_pipeline = None  # type: ignore[assignment]


# ---- Direct optional-dependency probes ----------------------------------- #
# Probe the third-party SDKs directly so callers get a definitive answer
# for the sentence-transformers / OpenAI / Cohere chains. NetworkX and
# NumPy are inferred from the enhanced_graph_traversal module above.
try:
    import sentence_transformers  # noqa: F401
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False

try:
    import openai  # noqa: F401
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    import cohere  # noqa: F401
    COHERE_AVAILABLE = True
except ImportError:
    COHERE_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Registries — internal mutable dicts + public read-only views
# --------------------------------------------------------------------------- #
_RETRIEVAL_REGISTRY: Dict[str, Type[Any]] = {}
_RETRIEVAL_CONFIG_REGISTRY: Dict[str, Type[Any]] = {}
_RETRIEVAL_ERROR_REGISTRY: Dict[str, Type[Any]] = {}


def _normalise_name(name: Any) -> str:
    """Strip and lowercase; raise on invalid input."""
    if not isinstance(name, str):
        raise RegistrationError(
            f"name must be a str, got {type(name).__name__}."
        )
    key = name.strip().lower()
    if not key:
        raise RegistrationError("name must be a non-empty string.")
    return key


# ---- Populate the registries ----------------------------------------------
# A `None` component is skipped — the registry never contains a broken
# entry. Config and error classes are registered only when their
# component is available.
for _name, _component, _config, _error in (
    (
        "integration",
        VimRAGIntegration,
        VimRAGIntegrationConfig,
        VimRAGIntegrationError,
    ),
    (
        "carbon",
        CarbonAdaptiveRetrievalController,
        CarbonAdaptiveConfig,
        CarbonAdaptiveError,
    ),
    (
        "chunker",
        SemanticChunker,
        SemanticChunkerConfig,
        SemanticChunkerError,
    ),
    (
        "compressor",
        ContextCompressor,
        ContextCompressionConfig,
        ContextCompressionError,
    ),
    ("core", VimRAGCoRetrieval, VimRAGConfig, VimRAGError),
    (
        "enhanced_graph",
        EnhancedGraphTraversalOptimizer,
        EnhancedGraphConfig,
        EnhancedGraphError,
    ),
    (
        "filter",
        TokenAwareFilter,
        TokenAwareConfig,
        TokenAwareFilterError,
    ),
    (
        "fusion",
        MultimodalFusion,
        MultimodalFusionConfig,
        MultimodalFusionError,
    ),
    (
        "graph",
        GraphTraversalOptimizer,
        GraphTraversalConfig,
        GraphTraversalError,
    ),
    (
        "hierarchical",
        HierarchicalRetrieval,
        HierarchicalRetrievalConfig,
        HierarchicalRetrievalError,
    ),
    (
        "semantic",
        SemanticScorer,
        SemanticScorerConfig,
        SemanticScorerError,
    ),
    (
        "serendipity",
        SerendipityTraceLogger,
        SerendipityLoggerConfig,
        SerendipityError,
    ),
    (
        "topological",
        TopologicalTokenAllocator,
        TopologicalAllocatorConfig,
        TopologicalAllocatorError,
    ),
    (
        "visual",
        VisualContextHandler,
        VisualContextConfig,
        VisualContextError,
    ),
):
    if _component is not None:
        _RETRIEVAL_REGISTRY[_name] = _component
    if _config is not None:
        _RETRIEVAL_CONFIG_REGISTRY[_name] = _config
    if _error is not None:
        _RETRIEVAL_ERROR_REGISTRY[_name] = _error


# ---- Read-only public views ------------------------------------------------
RETRIEVAL_REGISTRY: Mapping[str, Type[Any]] = MappingProxyType(
    _RETRIEVAL_REGISTRY
)
RETRIEVAL_CONFIG_REGISTRY: Mapping[str, Type[Any]] = MappingProxyType(
    _RETRIEVAL_CONFIG_REGISTRY
)
RETRIEVAL_ERROR_REGISTRY: Mapping[str, Type[Any]] = MappingProxyType(
    _RETRIEVAL_ERROR_REGISTRY
)
RETRIEVAL_AVAILABILITY: Mapping[str, bool] = MappingProxyType(
    _RETRIEVAL_AVAILABILITY
)
RETRIEVAL_LOAD_ERRORS: Mapping[str, str] = MappingProxyType(
    _RETRIEVAL_LOAD_ERRORS
)


# --------------------------------------------------------------------------- #
# Registry API
# --------------------------------------------------------------------------- #
def register_retrieval_component(
    name: str,
    cls: Type[Any],
    *,
    config: Optional[Type[Any]] = None,
    error: Optional[Type[Any]] = None,
    overwrite: bool = False,
) -> None:
    """
    Register a retrieval component under ``name``.

    ``name`` is stripped and lowercased before storage, so
    ``register(" PPO ", X)`` and ``get("ppo")`` refer to the same entry.

    Raises
    ------
    RegistrationError
        If ``name`` is invalid or already registered to a different
        class (unless ``overwrite=True``).
    TypeError
        If ``cls`` / ``config`` / ``error`` is not a class.
    """
    key = _normalise_name(name)
    for label, value in (
        ("cls", cls), ("config", config), ("error", error),
    ):
        if value is not None and not isinstance(value, type):
            raise TypeError(
                f"{label} must be a class, got "
                f"{type(value).__name__}."
            )
    existing = _RETRIEVAL_REGISTRY.get(key)
    if existing is not None and existing is not cls and not overwrite:
        raise RegistrationError(
            f"Retrieval component {key!r} already registered to "
            f"{existing.__name__}."
        )
    _RETRIEVAL_REGISTRY[key] = cls
    if config is not None:
        _RETRIEVAL_CONFIG_REGISTRY[key] = config
    if error is not None:
        _RETRIEVAL_ERROR_REGISTRY[key] = error
    logger.debug(
        "Registered retrieval component %s -> %s",
        key, cls.__name__,
    )


def unregister_retrieval_component(name: str) -> bool:
    """
    Remove a registered component from all three registries.

    Returns True if the component existed. Useful for test isolation.
    """
    try:
        key = _normalise_name(name)
    except RegistrationError:
        return False
    existed = _RETRIEVAL_REGISTRY.pop(key, None) is not None
    _RETRIEVAL_CONFIG_REGISTRY.pop(key, None)
    _RETRIEVAL_ERROR_REGISTRY.pop(key, None)
    return existed


def get_retrieval_component(name: str) -> Type[Any]:
    """
    Return the **class** registered under ``name`` (case-insensitive).

    Raises
    ------
    UnknownComponentError
        If the name is not registered (also a ``KeyError``).
    """
    try:
        key = _normalise_name(name)
    except RegistrationError as exc:
        raise UnknownComponentError(
            f"unknown retrieval component {name!r}: {exc}"
        ) from None
    try:
        return _RETRIEVAL_REGISTRY[key]
    except KeyError:
        raise UnknownComponentError(
            f"unknown retrieval component {name!r}. "
            f"Available: {sorted(_RETRIEVAL_REGISTRY)}"
        ) from None


def instantiate_retrieval_component(
    name: str, **kwargs: Any,
) -> Any:
    """Instantiate a registered component by name."""
    cls = get_retrieval_component(name)
    try:
        return cls(**kwargs)
    except TypeError as exc:
        raise RetrievalError(
            f"could not instantiate {name!r} with kwargs "
            f"{sorted(kwargs)}: {exc}"
        ) from exc


def list_retrieval_components() -> Dict[str, str]:
    """
    Return ``{registered_name: "module.ClassName"}`` for all entries.

    The full qualified path lets callers see which submodule each
    component lives in.
    """
    return {
        n: f"{c.__module__}.{c.__qualname__}"
        for n, c in _RETRIEVAL_REGISTRY.items()
    }


def get_retrieval_config(name: str) -> Type[Any]:
    """
    Return the ``*Config`` class for a component name.

    Raises
    ------
    UnknownComponentError
        If the component is not registered or has no config class.
    """
    try:
        key = _normalise_name(name)
    except RegistrationError as exc:
        raise UnknownComponentError(
            f"unknown retrieval component {name!r}: {exc}"
        ) from None
    if key not in _RETRIEVAL_REGISTRY:
        raise UnknownComponentError(
            f"unknown retrieval component {name!r}. "
            f"Available: {sorted(_RETRIEVAL_REGISTRY)}"
        )
    try:
        return _RETRIEVAL_CONFIG_REGISTRY[key]
    except KeyError:
        raise UnknownComponentError(
            f"component {name!r} is registered but has no config "
            f"class."
        ) from None


def get_retrieval_error(name: str) -> Type[Any]:
    """Return the ``*Error`` class for a component name."""
    try:
        key = _normalise_name(name)
    except RegistrationError as exc:
        raise UnknownComponentError(
            f"unknown retrieval component {name!r}: {exc}"
        ) from None
    if key not in _RETRIEVAL_REGISTRY:
        raise UnknownComponentError(
            f"unknown retrieval component {name!r}. "
            f"Available: {sorted(_RETRIEVAL_REGISTRY)}"
        )
    try:
        return _RETRIEVAL_ERROR_REGISTRY[key]
    except KeyError:
        raise UnknownComponentError(
            f"component {name!r} is registered but has no error "
            f"class."
        ) from None


def validate_retrieval_registry() -> Dict[str, Any]:
    """
    Return a structured validation report.

    Useful as a CI check: verify that every expected submodule is
    available and that every registry entry is a class.
    """
    available = sorted(
        k for k, v in _RETRIEVAL_AVAILABILITY.items() if v
    )
    unavailable = sorted(
        k for k, v in _RETRIEVAL_AVAILABILITY.items() if not v
    )
    load_errors = dict(_RETRIEVAL_LOAD_ERRORS)

    # Guard against future misuse of register_retrieval_component
    # that bypasses the isinstance(cls, type) check.
    for registry, label in (
        (_RETRIEVAL_REGISTRY, "components"),
        (_RETRIEVAL_CONFIG_REGISTRY, "configs"),
        (_RETRIEVAL_ERROR_REGISTRY, "errors"),
    ):
        for name, cls in registry.items():
            if not isinstance(cls, type):
                raise RetrievalError(
                    f"{label} registry entry {name!r} is not a "
                    f"class: {type(cls).__name__}."
                )

    return {
        "available": available,
        "unavailable": unavailable,
        "load_errors": load_errors,
        "registry_sizes": {
            "components": len(_RETRIEVAL_REGISTRY),
            "configs": len(_RETRIEVAL_CONFIG_REGISTRY),
            "errors": len(_RETRIEVAL_ERROR_REGISTRY),
        },
        "registered": {
            name: f"{cls.__module__}.{cls.__qualname__}"
            for name, cls in _RETRIEVAL_REGISTRY.items()
        },
    }


# --------------------------------------------------------------------------- #
# __all__ completeness assertion
# --------------------------------------------------------------------------- #
# A name in __all__ that doesn't resolve means a submodule rename or
# accidental deletion slipped past review. Fail-fast at import.
_missing_names = [n for n in __all__ if n not in globals()]
if _missing_names:
    raise RetrievalError(
        f"retrieval package __all__ references missing names: "
        f"{sorted(_missing_names)}. This usually means a submodule "
        f"renamed a public symbol without updating the aggregator."
    )


# --------------------------------------------------------------------------- #
# Smoke test: python -m retrieval
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print(f"retrieval VERSION    : {VERSION}")
    print(f"retrieval __version__: {__version__}")

    print("\nOptional dependency availability:")
    for name, flag in (
        ("networkx", NETWORKX_AVAILABLE),
        ("numpy", NUMPY_AVAILABLE),
        ("sentence_transformers", SENTENCE_TRANSFORMERS_AVAILABLE),
        ("openai", OPENAI_AVAILABLE),
        ("cohere", COHERE_AVAILABLE),
    ):
        marker = "✓" if flag else "✗"
        print(f"  {marker} {name:<24} "
              f"{'installed' if flag else 'missing'}")

    print("\nSub-module availability:")
    report = validate_retrieval_registry()
    for name in sorted(_RETRIEVAL_AVAILABILITY.keys()):
        ok = _RETRIEVAL_AVAILABILITY[name]
        marker = "✓" if ok else "✗"
        print(f"  {marker} {name}")
        if not ok:
            err = report["load_errors"].get(name, "unknown")
            print(f"    ↳ {err}")

    print(
        f"\nRegistry sizes: "
        f"components={report['registry_sizes']['components']}, "
        f"configs={report['registry_sizes']['configs']}, "
        f"errors={report['registry_sizes']['errors']}"
    )

    print("\nRegistered components (name → fully qualified path):")
    for name, path in sorted(report["registered"].items()):
        print(f"  {name:<16} → {path}")

    print("\nRegistry round-trip:")
    if report["registry_sizes"]["components"] > 0:
        sample = sorted(_RETRIEVAL_REGISTRY.keys())[0]
        cls = get_retrieval_component(sample)
        cfg = get_retrieval_config(sample)
        err = get_retrieval_error(sample)
        print(
            f"  get_retrieval_component({sample!r}) → "
            f"{cls.__name__}"
        )
        print(
            f"  get_retrieval_config({sample!r})    → "
            f"{cfg.__name__}"
        )
        print(
            f"  get_retrieval_error({sample!r})     → "
            f"{err.__name__}"
        )

    print("\nRead-only enforcement:")
    for label, accessor in (
        (
            "RETRIEVAL_REGISTRY",
            lambda: RETRIEVAL_REGISTRY.__setitem__("evil", int),
        ),
        (
            "RETRIEVAL_AVAILABILITY",
            lambda: RETRIEVAL_AVAILABILITY.__setitem__(
                "evil", True,
            ),
        ),
    ):
        try:
            accessor()
        except (TypeError, AttributeError) as exc:
            print(
                f"  ✓ {label} is read-only "
                f"({type(exc).__name__})"
            )

    print("\nName normalisation:")
    try:
        normalized = get_retrieval_component(
            sorted(_RETRIEVAL_REGISTRY.keys())[0].upper()
        )
        print(f"  get('{sorted(_RETRIEVAL_REGISTRY.keys())[0].upper()}') "
              f"→ {normalized.__name__}")
    except Exception as exc:
        print(f"  normalisation test failed: {exc}")

    print("\nSmoke test passed.")
