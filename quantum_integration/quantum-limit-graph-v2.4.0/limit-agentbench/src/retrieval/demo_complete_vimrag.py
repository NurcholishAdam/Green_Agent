# -*- coding: utf-8 -*-
"""
Complete VimRAG Demo
====================

Demonstrates all enhanced VimRAG capabilities:

1. Topological token allocation.
2. Semantic scoring with embeddings.
3. NetworkX graph operations.
4. VimRAG integration orchestrator.

Enhancements
------------
- Wrapped in ``main()`` with an ``if __name__ == "__main__"`` guard so the
  module is importable without executing the demos.
- Package-relative imports with defensive fallbacks (identical to the
  convention used across the ``retrieval`` package).
- ``DemoConfig`` — frozen, validated: model name, budgets, grid intensity,
  output verbosity, ANSI-color toggle.
- Each demo is a self-contained ``run_demo_N(cfg, out) -> DemoOutcome``
  function that returns a structured record (status, duration, error).
- CLI argument parsing: ``--only``, ``--skip``, ``--json``, ``--quiet``,
  ``--no-color``, ``--embedding-model``, ``--grid-intensity``.
- Diagnostics go through :mod:`logging`; the printed output (the demo's
  deliverable) goes through a small ``_Printer`` helper that honors
  ``--quiet`` and ``--no-color``.
- ``--json`` emits a machine-readable summary of every demo's outcome.
- Fully typed, with ``__all__`` and a comprehensive docstring.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
_DEFAULT_EMBEDDING_MODEL = "all-MiniLM-L6-v2"
_DEFAULT_GRID_INTENSITY = 180.0
_DEMO_SEPARATOR = "=" * 80
_DEMO_SUBSEPARATOR = "-" * 80


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DemoConfig:
    """Tunable parameters for the demo run."""

    embedding_model: str = _DEFAULT_EMBEDDING_MODEL
    grid_intensity_g_kwh: float = _DEFAULT_GRID_INTENSITY

    # Demo 1 budgets.
    topology_total_budget: int = 5_000
    topology_base_tokens_per_node: int = 100

    # Demo 2 budgets.
    semantic_top_k: int = 3

    # Demo 4 budgets (per-pipeline energy budget in Wh).
    fast_energy_budget_wh: float = 0.005
    green_energy_budget_wh: float = 0.003
    balanced_energy_budget_wh: float = 0.010

    # Output options.
    quiet: bool = False
    no_color: bool = False
    verbose: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.embedding_model, str) or not self.embedding_model:
            raise ValueError("embedding_model must be a non-empty string.")
        if self.grid_intensity_g_kwh < 0:
            raise ValueError("grid_intensity_g_kwh must be >= 0.")
        if not isinstance(self.topology_total_budget, int) or self.topology_total_budget <= 0:
            raise ValueError("topology_total_budget must be a positive int.")
        if (
            not isinstance(self.topology_base_tokens_per_node, int)
            or self.topology_base_tokens_per_node <= 0
        ):
            raise ValueError(
                "topology_base_tokens_per_node must be a positive int."
            )
        if not isinstance(self.semantic_top_k, int) or self.semantic_top_k <= 0:
            raise ValueError("semantic_top_k must be a positive int.")
        for name in (
            "fast_energy_budget_wh",
            "green_energy_budget_wh",
            "balanced_energy_budget_wh",
        ):
            value = getattr(self, name)
            if value <= 0:
                raise ValueError(f"{name} must be > 0.")


# --------------------------------------------------------------------------- #
# Outcome record
# --------------------------------------------------------------------------- #
@dataclass
class DemoOutcome:
    """Result of running one demo."""

    demo_id: str
    status: str                # "ok" | "skipped" | "error"
    duration_ms: float
    reason: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Printer (honors quiet / no-color)
# --------------------------------------------------------------------------- #
class _Printer:
    """Small print helper that respects ``--quiet`` and ``--no-color``."""

    def __init__(self, *, quiet: bool = False, no_color: bool = False) -> None:
        self.quiet = quiet
        self.no_color = no_color

    def line(self, text: str = "") -> None:
        if not self.quiet:
            print(text)

    def section(self, title: str) -> None:
        self.line()
        self.line(_DEMO_SEPARATOR)
        self.line(title)
        self.line(_DEMO_SEPARATOR)

    def rule(self) -> None:
        self.line(_DEMO_SUBSEPARATOR)


# --------------------------------------------------------------------------- #
# Import helpers
# --------------------------------------------------------------------------- #
def _import_attr(module_name: str, attr: str) -> Any:
    """
    Import ``attr`` from ``retrieval.<module_name>`` with a flat-module
    fallback. Returns ``None`` when neither resolves.
    """
    try:
        module = __import__(
            f"{__name__.rsplit('.', 1)[0]}.{module_name}", fromlist=[attr]
        )
    except Exception:
        try:
            module = __import__(module_name, fromlist=[attr])
        except Exception:
            return None
    return getattr(module, attr, None)


# --------------------------------------------------------------------------- #
# Demo 1 — topological token allocation
# --------------------------------------------------------------------------- #
def run_demo_1(cfg: DemoConfig, out: _Printer) -> DemoOutcome:
    """Topological token allocation (importance-based budgeting)."""
    start = time.perf_counter()

    out.line("📊 DEMO 1: Topological Token Allocation")

    Allocator = _import_attr("topological_allocator", "TopologicalTokenAllocator")
    Metric = _import_attr("topological_allocator", "ImportanceMetric")
    if Allocator is None or Metric is None:
        reason = "topological_allocator module not available"
        out.line(f"⚠️  Skipping demo: {reason}")
        return DemoOutcome(
            demo_id="topological_allocation",
            status="skipped",
            duration_ms=(time.perf_counter() - start) * 1000.0,
            reason=reason,
        )

    try:
        import networkx as nx
    except ImportError as exc:
        reason = f"networkx not installed: {exc}"
        out.line(f"⚠️  Skipping demo: {reason}")
        return DemoOutcome(
            demo_id="topological_allocation",
            status="skipped",
            duration_ms=(time.perf_counter() - start) * 1000.0,
            reason=reason,
        )

    # ---- Build the medical knowledge graph ---------------------------
    G = nx.DiGraph()

    nodes = {
        "symptom_fever": {"type": "symptom", "importance": "high"},
        "symptom_cough": {"type": "symptom", "importance": "high"},
        "symptom_fatigue": {"type": "symptom", "importance": "medium"},
        "condition_pneumonia": {"type": "condition", "importance": "critical"},
        "condition_flu": {"type": "condition", "importance": "high"},
        "condition_cold": {"type": "condition", "importance": "medium"},
        "treatment_antibiotics": {"type": "treatment", "importance": "high"},
        "treatment_rest": {"type": "treatment", "importance": "low"},
        "general_health": {"type": "general", "importance": "low"},
    }
    for node_id, attrs in nodes.items():
        G.add_node(node_id, **attrs)

    edges = [
        ("symptom_fever", "condition_pneumonia", 0.9),
        ("symptom_cough", "condition_pneumonia", 0.8),
        ("symptom_fever", "condition_flu", 0.7),
        ("symptom_cough", "condition_flu", 0.6),
        ("symptom_fatigue", "condition_flu", 0.5),
        ("symptom_cough", "condition_cold", 0.4),
        ("condition_pneumonia", "treatment_antibiotics", 0.9),
        ("condition_flu", "treatment_rest", 0.6),
        ("condition_cold", "treatment_rest", 0.8),
        ("symptom_fatigue", "general_health", 0.3),
    ]
    for source, target, weight in edges:
        G.add_edge(source, target, weight=weight)

    # ---- Run the allocator -------------------------------------------
    try:
        allocator = Allocator(
            importance_metric=Metric.COMBINED,
            critical_threshold=0.8,
            high_threshold=0.6,
            medium_threshold=0.4,
            low_threshold=0.2,
        )
        allocations = allocator.allocate_tokens(
            graph=G,
            total_token_budget=cfg.topology_total_budget,
            base_tokens_per_node=cfg.topology_base_tokens_per_node,
        )
    except Exception as exc:
        logger.exception("Demo 1 failed: %s", exc)
        out.line(f"❌ Demo 1 failed: {exc}")
        return DemoOutcome(
            demo_id="topological_allocation",
            status="error",
            duration_ms=(time.perf_counter() - start) * 1000.0,
            reason=str(exc),
        )

    # ---- Display results --------------------------------------------
    out.line()
    out.line(
        f"📈 Token Allocation Results "
        f"(Total Budget: {cfg.topology_total_budget} tokens)"
    )
    out.line(
        f"{'Node':<30} {'Resolution':<12} {'Tokens':<8} {'Importance':<12}"
    )
    out.line(_DEMO_SUBSEPARATOR.replace("-", "-", 1)[:70])

    for node_id in sorted(
        allocations.keys(),
        key=lambda x: allocations[x].allocated_tokens,
        reverse=True,
    ):
        alloc = allocations[node_id]
        out.line(
            f"{node_id:<30} {alloc.resolution_level.value:<12} "
            f"{alloc.allocated_tokens:<8} {alloc.importance_score:.3f}"
        )

    summary = allocator.get_allocation_summary(allocations)
    out.line()
    out.line("📊 Allocation Summary:")
    out.line(f"  Total nodes: {summary['total_nodes']}")
    out.line(f"  Total tokens allocated: {summary['total_tokens_allocated']}")
    out.line(f"  Average tokens/node: {summary['avg_tokens_per_node']:.1f}")
    out.line()
    out.line("  Distribution by resolution:")
    for level, count in summary["nodes_by_resolution"].items():
        tokens = summary["tokens_by_resolution"][level]
        out.line(f"    {level}: {count} nodes ({tokens} tokens)")

    out.line()
    out.line("✅ Topological allocation complete!")

    return DemoOutcome(
        demo_id="topological_allocation",
        status="ok",
        duration_ms=(time.perf_counter() - start) * 1000.0,
        details={
            "total_nodes": summary["total_nodes"],
            "total_tokens_allocated": summary["total_tokens_allocated"],
            "nodes_by_resolution": summary["nodes_by_resolution"],
        },
    )


# --------------------------------------------------------------------------- #
# Demo 2 — semantic scoring
# --------------------------------------------------------------------------- #
def run_demo_2(cfg: DemoConfig, out: _Printer) -> DemoOutcome:
    """Semantic scoring with embeddings (vs keyword matching)."""
    start = time.perf_counter()

    out.line("🧠 DEMO 2: Semantic Scoring with Embeddings")

    create_local_scorer = _import_attr("semantic_scorer", "create_local_scorer")
    if create_local_scorer is None:
        reason = "semantic_scorer module not available"
        out.line(f"⚠️  Skipping demo: {reason}")
        return DemoOutcome(
            demo_id="semantic_scoring",
            status="skipped",
            duration_ms=(time.perf_counter() - start) * 1000.0,
            reason=reason,
        )

    # ---- Load the model ---------------------------------------------
    out.line(f"Loading embedding model ({cfg.embedding_model})...")
    try:
        scorer = create_local_scorer(
            model_name=cfg.embedding_model,
            cache_embeddings=True,
        )
    except Exception as exc:
        logger.exception("Demo 2 failed to construct scorer: %s", exc)
        out.line(f"⚠️  Skipping demo (install sentence-transformers): {exc}")
        return DemoOutcome(
            demo_id="semantic_scoring",
            status="skipped",
            duration_ms=(time.perf_counter() - start) * 1000.0,
            reason=str(exc),
        )

    # ---- Sample documents ------------------------------------------
    documents = [
        ("doc_1", "Pneumonia is a lung infection causing fever and cough"),
        ("doc_2", "Common cold symptoms include runny nose and sneezing"),
        ("doc_3", "Antibiotics are used to treat bacterial infections"),
        ("doc_4", "Machine learning models require large datasets"),
        ("doc_5", "Climate change affects global temperatures"),
    ]
    query = "How to treat lung infection with fever?"

    out.line()
    out.line(f"🔍 Query: '{query}'")
    out.line()
    out.line(f"📄 Scoring {len(documents)} documents...")

    try:
        scores = scorer.score_and_rank(
            query=query,
            contents=documents,
            top_k=cfg.semantic_top_k,
        )
    except Exception as exc:
        logger.exception("Demo 2 scoring failed: %s", exc)
        out.line(f"❌ Demo 2 failed: {exc}")
        return DemoOutcome(
            demo_id="semantic_scoring",
            status="error",
            duration_ms=(time.perf_counter() - start) * 1000.0,
            reason=str(exc),
        )

    out.line()
    out.line("🏆 Top Results:")
    out.line(
        f"{'Rank':<6} {'Doc ID':<10} {'Similarity':<12} "
        f"{'Confidence':<12} {'Content':<50}"
    )
    out.line("-" * 90)

    for i, score in enumerate(scores, 1):
        doc_content = next(c for d, c in documents if d == score.node_id)
        out.line(
            f"{i:<6} {score.node_id:<10} {score.similarity_score:.4f}       "
            f"{score.confidence:.4f}       {doc_content[:47]}..."
        )

    try:
        cache_stats = scorer.get_cache_stats()
    except Exception:
        cache_stats = {}

    if cache_stats:
        out.line()
        out.line("💾 Cache Statistics:")
        out.line(f"  Cache size: {cache_stats.get('cache_size', 0)} embeddings")
        out.line(f"  Cache hits: {cache_stats.get('cache_hits', 0)}")
        out.line(f"  Hit rate: {cache_stats.get('hit_rate', 0.0):.1%}")
        out.line(
            f"  Embedding dimension: "
            f"{cache_stats.get('embedding_dimension', 0)}"
        )

    out.line()
    out.line("✅ Semantic scoring complete!")

    return DemoOutcome(
        demo_id="semantic_scoring",
        status="ok",
        duration_ms=(time.perf_counter() - start) * 1000.0,
        details={
            "query": query,
            "top_k": len(scores),
            "top_result_id": scores[0].node_id if scores else None,
            "top_result_similarity": (
                scores[0].similarity_score if scores else None
            ),
        },
    )


# --------------------------------------------------------------------------- #
# Demo 3 — enhanced graph traversal
# --------------------------------------------------------------------------- #
def run_demo_3(cfg: DemoConfig, out: _Printer) -> DemoOutcome:
    """Enhanced NetworkX-backed graph traversal with community detection."""
    start = time.perf_counter()

    out.line("🗺️  DEMO 3: Enhanced Graph Traversal with NetworkX")

    Optimizer = _import_attr(
        "enhanced_graph_traversal", "EnhancedGraphTraversalOptimizer"
    )
    Strategy = _import_attr("enhanced_graph_traversal", "TraversalStrategy")
    if Optimizer is None or Strategy is None:
        reason = "enhanced_graph_traversal module not available"
        out.line(f"⚠️  Skipping demo: {reason}")
        return DemoOutcome(
            demo_id="graph_traversal",
            status="skipped",
            duration_ms=(time.perf_counter() - start) * 1000.0,
            reason=reason,
        )

    try:
        optimizer = Optimizer(
            graph_type="directed",
            energy_weight=0.5,
            relevance_weight=0.5,
            enable_communities=True,
        )
    except Exception as exc:
        logger.exception("Demo 3 failed to construct optimizer: %s", exc)
        out.line(f"❌ Demo 3 failed: {exc}")
        return DemoOutcome(
            demo_id="graph_traversal",
            status="error",
            duration_ms=(time.perf_counter() - start) * 1000.0,
            reason=str(exc),
        )

    # ---- Build the medical graph ----------------------------------
    out.line("Building medical knowledge graph...")

    medical_nodes = {
        "symptoms": (0.05, 0.8),
        "fever": (0.1, 0.9),
        "cough": (0.1, 0.9),
        "diagnosis": (0.3, 0.95),
        "pneumonia": (0.4, 1.0),
        "treatment": (0.3, 0.9),
        "antibiotics": (0.2, 0.85),
        "recovery": (0.1, 0.7),
    }
    for node_id, (cost, relevance) in medical_nodes.items():
        optimizer.add_node(node_id, cost=cost, relevance=relevance)

    medical_edges = [
        ("symptoms", "fever", 0.8),
        ("symptoms", "cough", 0.7),
        ("fever", "diagnosis", 0.9),
        ("cough", "diagnosis", 0.8),
        ("diagnosis", "pneumonia", 0.95),
        ("pneumonia", "treatment", 0.9),
        ("treatment", "antibiotics", 0.85),
        ("antibiotics", "recovery", 0.8),
    ]
    for source, target, weight in medical_edges:
        optimizer.add_edge(source, target, weight=weight)

    # ---- Community detection --------------------------------------
    out.line("Detecting communities...")
    try:
        communities = optimizer.detect_communities(algorithm="louvain")
    except Exception as exc:
        logger.exception("Community detection failed: %s", exc)
        communities = []

    out.line()
    out.line(f"🏘️  Found {len(communities)} communities:")
    for i, comm in enumerate(communities, 1):
        out.line(f"  Community {i}: {', '.join(sorted(comm))}")

    # ---- Path finding ---------------------------------------------
    out.line()
    out.line("🛤️  Finding paths from 'symptoms' to 'recovery':")

    strategies = [
        Strategy.ENERGY_OPTIMAL,
        Strategy.RELEVANCE_GUIDED,
        Strategy.COMMUNITY_AWARE,
    ]

    paths_found: Dict[str, Any] = {}
    for strategy in strategies:
        try:
            path = optimizer.find_optimal_path(
                start="symptoms",
                goal="recovery",
                strategy=strategy,
                max_depth=10,
            )
        except Exception as exc:
            logger.warning(
                "Pathfinding failed for %s: %s", strategy.value, exc
            )
            continue

        if path:
            paths_found[strategy.value] = {
                "nodes": list(path.nodes),
                "energy_cost": path.energy_cost,
                "relevance_score": path.relevance_score,
                "unique_communities": path.metadata.get("unique_communities"),
            }
            out.line()
            out.line(f"  {strategy.value}:")
            out.line(f"    Path: {' → '.join(path.nodes)}")
            out.line(f"    Energy cost: {path.energy_cost:.4f} Wh")
            out.line(f"    Relevance: {path.relevance_score:.3f}")
            out.line(
                f"    Communities: "
                f"{path.metadata.get('unique_communities')}"
            )

    # ---- Graph statistics -----------------------------------------
    try:
        stats = optimizer.get_graph_statistics()
        out.line()
        out.line("📊 Graph Statistics:")
        out.line(f"  Nodes: {stats.num_nodes}")
        out.line(f"  Edges: {stats.num_edges}")
        out.line(f"  Density: {stats.density:.3f}")
        out.line(f"  Communities: {stats.num_communities}")
        out.line(f"  Connected: {stats.is_connected}")

        stats_payload = stats.to_dict()
    except Exception as exc:
        logger.exception("Graph statistics failed: %s", exc)
        stats_payload = {}

    out.line()
    out.line("✅ Graph traversal complete!")

    return DemoOutcome(
        demo_id="graph_traversal",
        status="ok",
        duration_ms=(time.perf_counter() - start) * 1000.0,
        details={
            "communities": len(communities),
            "paths_found": paths_found,
            "graph_statistics": stats_payload,
        },
    )


# --------------------------------------------------------------------------- #
# Demo 4 — complete VimRAG integration
# --------------------------------------------------------------------------- #
def run_demo_4(cfg: DemoConfig, out: _Printer) -> DemoOutcome:
    """Complete VimRAG integration orchestrator."""
    start = time.perf_counter()

    out.line("🚀 DEMO 4: Complete VimRAG Integration")

    Integration = _import_attr("vimrag_integration", "VimRAGIntegration")
    Pipeline = _import_attr("vimrag_integration", "RetrievalPipeline")
    if Integration is None or Pipeline is None:
        reason = "vimrag_integration module not available"
        out.line(f"⚠️  Skipping demo: {reason}")
        return DemoOutcome(
            demo_id="vimrag_integration",
            status="skipped",
            duration_ms=(time.perf_counter() - start) * 1000.0,
            reason=reason,
        )

    # ---- Construct pipeline ---------------------------------------
    out.line("Initializing VimRAG integration...")
    try:
        vimrag = Integration(
            grid_region="US-CA",
            enable_semantic_scoring=False,  # Skip for demo speed
            enable_topological_allocation=True,
            enable_carbon_adaptation=True,
            enable_serendipity=True,
        )
    except Exception as exc:
        logger.exception("Demo 4 failed to construct integration: %s", exc)
        out.line(f"❌ Demo 4 failed: {exc}")
        return DemoOutcome(
            demo_id="vimrag_integration",
            status="error",
            duration_ms=(time.perf_counter() - start) * 1000.0,
            reason=str(exc),
        )

    # ---- Add documents --------------------------------------------
    out.line()
    out.line("📚 Adding medical documents to memory graph...")

    documents = [
        {
            "content": (
                "Pneumonia symptoms include high fever, productive cough, "
                "and chest pain. Requires immediate medical attention."
            ),
            "type": "text",
            "metadata": {"category": "diagnosis", "importance": 1.0},
        },
        {
            "content": (
                "Treatment for bacterial pneumonia typically involves "
                "antibiotics like azithromycin or amoxicillin."
            ),
            "type": "text",
            "metadata": {"category": "treatment", "importance": 0.9},
        },
        {
            "content": (
                "X-ray imaging shows infiltrates in lower right lung lobe "
                "consistent with pneumonia."
            ),
            "type": "visual",
            "metadata": {"category": "imaging", "importance": 0.95},
        },
        {
            "content": (
                "Patient recovery timeline: antibiotics for 7-10 days, "
                "rest, and fluid intake."
            ),
            "type": "text",
            "metadata": {"category": "treatment", "importance": 0.7},
        },
        {
            "content": (
                "Common cold is usually viral and resolves without "
                "antibiotics."
            ),
            "type": "text",
            "metadata": {"category": "diagnosis", "importance": 0.5},
        },
    ]

    node_ids: List[str] = []
    for doc in documents:
        try:
            node_id = vimrag.add_document(
                content=doc["content"],
                content_type=doc["type"],
                metadata=doc["metadata"],
            )
        except Exception as exc:
            logger.exception("add_document failed: %s", exc)
            continue
        node_ids.append(node_id)
        out.line(f"  Added: {node_id[:16]}... ({doc['type']})")

    # ---- Connect documents ----------------------------------------
    if len(node_ids) >= 4:
        try:
            vimrag.connect_documents(node_ids[0], node_ids[1], weight=0.9)
            vimrag.connect_documents(node_ids[0], node_ids[2], weight=0.85)
            vimrag.connect_documents(node_ids[1], node_ids[3], weight=0.8)
        except Exception as exc:
            logger.warning("connect_documents failed: %s", exc)

    # ---- Simulate a clean grid ------------------------------------
    try:
        vimrag.update_grid_intensity(cfg.grid_intensity_g_kwh)
    except Exception as exc:
        logger.warning("update_grid_intensity failed: %s", exc)

    # ---- Run each pipeline mode -----------------------------------
    query = "How to diagnose and treat pneumonia?"
    pipelines: List[Tuple[Any, float]] = [
        (Pipeline.FAST, cfg.fast_energy_budget_wh),
        (Pipeline.GREEN, cfg.green_energy_budget_wh),
        (Pipeline.BALANCED, cfg.balanced_energy_budget_wh),
    ]

    out.line()
    out.line(f"🔍 Query: '{query}'")
    out.line()
    out.line("Testing different pipeline modes:")
    out.line()

    pipeline_results: Dict[str, Any] = {}
    for pipeline, energy_budget in pipelines:
        out.line(
            f"  {pipeline.value.upper()} mode "
            f"(budget: {energy_budget * 1000:.1f} mWh)"
        )
        try:
            result = vimrag.retrieve(
                query=query,
                pipeline=pipeline,
                max_results=3,
                energy_budget_wh=energy_budget,
                include_visual=True,
                agent_id="demo_agent",
            )
        except Exception as exc:
            logger.exception(
                "Retrieve failed for %s: %s", pipeline.value, exc,
            )
            out.line(f"    ❌ Retrieval failed: {exc}")
            out.line()
            continue

        out.line(f"    Retrieved: {len(result.retrieved_nodes)} nodes")
        out.line(f"    Energy: {result.total_energy_wh * 1000:.3f} mWh")
        out.line(
            f"    Carbon: {result.total_carbon_kg * 1000:.3f} g CO2"
        )
        out.line(f"    Mode: {result.retrieval_mode}")
        out.line(f"    Time: {result.total_time_ms:.1f} ms")

        if result.serendipity_events:
            out.line(
                f"    💡 Efficiency gain: {result.efficiency_gains:.1%}"
            )
            out.line(
                f"    💚 Carbon saved: "
                f"{result.carbon_savings_kg * 1000:.3f} g CO2"
            )

        pipeline_results[pipeline.value] = {
            "retrieved_nodes": len(result.retrieved_nodes),
            "total_energy_wh": result.total_energy_wh,
            "total_carbon_kg": result.total_carbon_kg,
            "retrieval_mode": result.retrieval_mode,
            "total_time_ms": result.total_time_ms,
            "efficiency_gains": result.efficiency_gains,
            "carbon_savings_kg": result.carbon_savings_kg,
        }
        out.line()

    # ---- Integration stats ----------------------------------------
    try:
        stats = vimrag.get_integration_stats()
    except Exception as exc:
        logger.warning("get_integration_stats failed: %s", exc)
        stats = {}

    if stats:
        out.line("📊 Integration Statistics:")
        out.line(f"  Total retrievals: {stats.get('total_retrievals', 0)}")
        out.line(
            f"  Total energy consumed: "
            f"{stats.get('total_energy_consumed_wh', 0.0) * 1000:.2f} mWh"
        )
        out.line(
            f"  Total carbon emitted: "
            f"{stats.get('total_carbon_emitted_kg', 0.0) * 1000:.2f} g CO2"
        )
        out.line(
            f"  Total carbon saved: "
            f"{stats.get('total_carbon_saved_kg', 0.0) * 1000:.2f} g CO2"
        )
        out.line(
            f"  Memory graph: {stats.get('memory_graph_nodes', 0)} nodes, "
            f"{stats.get('memory_graph_edges', 0)} edges"
        )

    out.line()
    out.line("✅ VimRAG integration complete!")

    return DemoOutcome(
        demo_id="vimrag_integration",
        status="ok",
        duration_ms=(time.perf_counter() - start) * 1000.0,
        details={
            "documents_added": len(node_ids),
            "pipelines": pipeline_results,
            "integration_stats": {
                k: v for k, v in stats.items() if isinstance(v, (int, float, str))
            } if stats else {},
        },
    )


# --------------------------------------------------------------------------- #
# Orchestrator
# --------------------------------------------------------------------------- #
_DEMOS: Tuple[Tuple[str, Callable[[DemoConfig, _Printer], DemoOutcome]], ...] = (
    ("topological", run_demo_1),
    ("semantic", run_demo_2),
    ("graph", run_demo_3),
    ("integration", run_demo_4),
)


def run_all_demos(
    cfg: Optional[DemoConfig] = None,
    *,
    only: Optional[Sequence[str]] = None,
    skip: Optional[Sequence[str]] = None,
) -> List[DemoOutcome]:
    """
    Run the selected demos and return their outcomes.

    Parameters
    ----------
    cfg : DemoConfig, optional
        Configuration for the run. Defaults to ``DemoConfig()``.
    only : sequence of str, optional
        If provided, run only these demo IDs (aliases:
        ``"topological"``, ``"semantic"``, ``"graph"``, ``"integration"``).
    skip : sequence of str, optional
        Demo IDs to skip.
    """
    cfg = cfg or DemoConfig()
    only_set = set(only) if only else None
    skip_set = set(skip) if skip else set()

    printer = _Printer(quiet=cfg.quiet, no_color=cfg.no_color)

    printer.section("VimRAG Enhanced Retrieval System - Complete Demo")

    outcomes: List[DemoOutcome] = []
    for demo_id, fn in _DEMOS:
        if only_set is not None and demo_id not in only_set:
            continue
        if demo_id in skip_set:
            outcomes.append(DemoOutcome(
                demo_id=demo_id,
                status="skipped",
                duration_ms=0.0,
                reason="user requested skip",
            ))
            continue

        printer.line()
        try:
            outcome = fn(cfg, printer)
        except Exception as exc:  # pragma: no cover — last-resort safety net
            logger.exception("Demo %s crashed: %s", demo_id, exc)
            outcome = DemoOutcome(
                demo_id=demo_id,
                status="error",
                duration_ms=0.0,
                reason=str(exc),
            )
        outcomes.append(outcome)
        printer.line()

    # ---- Summary --------------------------------------------------
    printer.section("✨ All VimRAG Enhancement Demos Complete!")
    printer.line("Key Features Demonstrated:")
    printer.line(
        "  ✅ Topological token allocation (importance-based budgeting)"
    )
    printer.line(
        "  ✅ Semantic scoring with embeddings (vs keyword matching)"
    )
    printer.line(
        "  ✅ NetworkX graph operations (communities, optimal paths)"
    )
    printer.line(
        "  ✅ Complete VimRAG integration (all modules coordinated)"
    )
    printer.line()
    printer.line("Sustainability Impact:")
    printer.line("  🌱 Carbon-adaptive retrieval based on grid intensity")
    printer.line("  🌱 Energy-optimal graph traversal")
    printer.line("  🌱 Token-aware filtering and compression")
    printer.line("  🌱 Serendipity logging for efficiency discovery")
    printer.line()
    printer.line("Next Steps:")
    printer.line(
        "  1. Install missing dependencies: "
        "pip install sentence-transformers networkx"
    )
    printer.line("  2. Test with your own documents and queries")
    printer.line(
        "  3. Integrate with Level 4 (NSN) and Level 5 (MetaAgent)"
    )
    printer.line("  4. Deploy to Green Agent benchmarking platform")
    printer.line()
    printer.line(_DEMO_SEPARATOR)

    return outcomes


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the complete VimRAG enhancement demo.",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        choices=[d[0] for d in _DEMOS],
        help="Run only these demo IDs.",
    )
    parser.add_argument(
        "--skip",
        nargs="+",
        choices=[d[0] for d in _DEMOS],
        help="Skip these demo IDs.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a JSON summary of demo outcomes instead of text.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress demo text output (useful with --json).",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI colors in future versions.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )
    parser.add_argument(
        "--embedding-model",
        default=_DEFAULT_EMBEDDING_MODEL,
        help="Sentence-transformers model name.",
    )
    parser.add_argument(
        "--grid-intensity",
        type=float,
        default=_DEFAULT_GRID_INTENSITY,
        help="Carbon intensity (gCO2/kWh) used in the demos.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point. Returns the process exit code."""
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    try:
        cfg = DemoConfig(
            embedding_model=args.embedding_model,
            grid_intensity_g_kwh=args.grid_intensity,
            quiet=args.quiet,
            no_color=args.no_color,
            verbose=args.verbose,
        )
    except ValueError as exc:
        parser.error(str(exc))
        return 2  # pragma: no cover

    outcomes = run_all_demos(cfg, only=args.only, skip=args.skip)

    if args.json:
        summary = {
            "config": {
                "embedding_model": cfg.embedding_model,
                "grid_intensity_g_kwh": cfg.grid_intensity_g_kwh,
            },
            "demos": [o.to_dict() for o in outcomes],
            "overall": {
                "total": len(outcomes),
                "ok": sum(1 for o in outcomes if o.status == "ok"),
                "skipped": sum(1 for o in outcomes if o.status == "skipped"),
                "errors": sum(1 for o in outcomes if o.status == "error"),
            },
        }
        # JSON goes to stdout regardless of --quiet so it can be piped.
        print(json.dumps(summary, indent=2, default=str))

    # Non-zero exit code only on hard errors.
    return 0 if all(o.status != "error" for o in outcomes) else 1


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "DemoConfig",
    "DemoOutcome",
    "main",
    "run_all_demos",
    "run_demo_1",
    "run_demo_2",
    "run_demo_3",
    "run_demo_4",
]


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
