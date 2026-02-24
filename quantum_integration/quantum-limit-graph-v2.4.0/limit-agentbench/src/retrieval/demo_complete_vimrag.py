# -*- coding: utf-8 -*-
"""
Complete VimRAG Demo

Demonstrates all enhanced VimRAG capabilities:
1. Topological token allocation
2. Semantic scoring with embeddings
3. NetworkX graph operations
4. VimRAG integration orchestrator
"""

import sys
import networkx as nx
import numpy as np
from datetime import datetime

print("=" * 80)
print("VimRAG Enhanced Retrieval System - Complete Demo")
print("=" * 80)
print()

# ============================================================================
# DEMO 1: Topological Token Allocation
# ============================================================================

print("📊 DEMO 1: Topological Token Allocation")
print("-" * 80)

try:
    from topological_allocator import (
        TopologicalTokenAllocator,
        ImportanceMetric,
        ResolutionLevel
    )
    
    # Create a sample knowledge graph
    G = nx.DiGraph()
    
    # Medical diagnosis scenario
    nodes = {
        "symptom_fever": {"type": "symptom", "importance": "high"},
        "symptom_cough": {"type": "symptom", "importance": "high"},
        "symptom_fatigue": {"type": "symptom", "importance": "medium"},
        "condition_pneumonia": {"type": "condition", "importance": "critical"},
        "condition_flu": {"type": "condition", "importance": "high"},
        "condition_cold": {"type": "condition", "importance": "medium"},
        "treatment_antibiotics": {"type": "treatment", "importance": "high"},
        "treatment_rest": {"type": "treatment", "importance": "low"},
        "general_health": {"type": "general", "importance": "low"}
    }
    
    # Add nodes
    for node_id, attrs in nodes.items():
        G.add_node(node_id, **attrs)
    
    # Add edges (semantic connections)
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
        ("symptom_fatigue", "general_health", 0.3)
    ]
    
    for source, target, weight in edges:
        G.add_edge(source, target, weight=weight)
    
    # Initialize allocator
    allocator = TopologicalTokenAllocator(
        importance_metric=ImportanceMetric.COMBINED,
        critical_threshold=0.8,
        high_threshold=0.6,
        medium_threshold=0.4,
        low_threshold=0.2
    )
    
    # Allocate tokens
    total_budget = 5000  # Total token budget
    allocations = allocator.allocate_tokens(
        graph=G,
        total_token_budget=total_budget,
        base_tokens_per_node=100
    )
    
    # Display results
    print(f"\n📈 Token Allocation Results (Total Budget: {total_budget} tokens)")
    print(f"{'Node':<30} {'Resolution':<12} {'Tokens':<8} {'Importance':<12}")
    print("-" * 70)
    
    for node_id in sorted(allocations.keys(), key=lambda x: allocations[x].allocated_tokens, reverse=True):
        alloc = allocations[node_id]
        print(f"{node_id:<30} {alloc.resolution_level.value:<12} {alloc.allocated_tokens:<8} {alloc.importance_score:.3f}")
    
    # Summary
    summary = allocator.get_allocation_summary(allocations)
    print(f"\n📊 Allocation Summary:")
    print(f"  Total nodes: {summary['total_nodes']}")
    print(f"  Total tokens allocated: {summary['total_tokens_allocated']}")
    print(f"  Average tokens/node: {summary['avg_tokens_per_node']:.1f}")
    print(f"\n  Distribution by resolution:")
    for level, count in summary['nodes_by_resolution'].items():
        tokens = summary['tokens_by_resolution'][level]
        print(f"    {level}: {count} nodes ({tokens} tokens)")
    
    print("\n✅ Topological allocation complete!")
    
except ImportError as e:
    print(f"⚠️  Skipping demo (module not found): {e}")

print()

# ============================================================================
# DEMO 2: Semantic Scoring with Embeddings
# ============================================================================

print("🧠 DEMO 2: Semantic Scoring with Embeddings")
print("-" * 80)

try:
    from semantic_scorer import create_local_scorer
    
    # Create semantic scorer
    print("Loading embedding model (all-MiniLM-L6-v2)...")
    scorer = create_local_scorer(
        model_name="all-MiniLM-L6-v2",
        cache_embeddings=True
    )
    
    # Sample documents
    documents = [
        ("doc_1", "Pneumonia is a lung infection causing fever and cough"),
        ("doc_2", "Common cold symptoms include runny nose and sneezing"),
        ("doc_3", "Antibiotics are used to treat bacterial infections"),
        ("doc_4", "Machine learning models require large datasets"),
        ("doc_5", "Climate change affects global temperatures")
    ]
    
    # Query
    query = "How to treat lung infection with fever?"
    
    print(f"\n🔍 Query: '{query}'")
    print(f"\n📄 Scoring {len(documents)} documents...")
    
    # Score and rank
    scores = scorer.score_and_rank(
        query=query,
        contents=documents,
        top_k=3
    )
    
    print(f"\n🏆 Top Results:")
    print(f"{'Rank':<6} {'Doc ID':<10} {'Similarity':<12} {'Confidence':<12} {'Content':<50}")
    print("-" * 90)
    
    for i, score in enumerate(scores, 1):
        doc_content = next(c for d, c in documents if d == score.node_id)
        print(f"{i:<6} {score.node_id:<10} {score.similarity_score:.4f}       {score.confidence:.4f}       {doc_content[:47]}...")
    
    # Cache statistics
    cache_stats = scorer.get_cache_stats()
    print(f"\n💾 Cache Statistics:")
    print(f"  Cache size: {cache_stats['cache_size']} embeddings")
    print(f"  Cache hits: {cache_stats['cache_hits']}")
    print(f"  Hit rate: {cache_stats['hit_rate']:.1%}")
    print(f"  Embedding dimension: {cache_stats['embedding_dimension']}")
    
    print("\n✅ Semantic scoring complete!")
    
except ImportError as e:
    print(f"⚠️  Skipping demo (module not found): {e}")
    print("   Install with: pip install sentence-transformers")

print()

# ============================================================================
# DEMO 3: Enhanced Graph Traversal with NetworkX
# ============================================================================

print("🗺️  DEMO 3: Enhanced Graph Traversal with NetworkX")
print("-" * 80)

try:
    from enhanced_graph_traversal import (
        EnhancedGraphTraversalOptimizer,
        TraversalStrategy
    )
    
    # Create graph optimizer
    optimizer = EnhancedGraphTraversalOptimizer(
        graph_type="directed",
        energy_weight=0.5,
        relevance_weight=0.5,
        enable_communities=True
    )
    
    # Build medical knowledge graph
    print("Building medical knowledge graph...")
    
    # Add nodes with costs and relevance
    medical_nodes = {
        "symptoms": (0.05, 0.8),
        "fever": (0.1, 0.9),
        "cough": (0.1, 0.9),
        "diagnosis": (0.3, 0.95),
        "pneumonia": (0.4, 1.0),
        "treatment": (0.3, 0.9),
        "antibiotics": (0.2, 0.85),
        "recovery": (0.1, 0.7)
    }
    
    for node_id, (cost, relevance) in medical_nodes.items():
        optimizer.add_node(node_id, cost=cost, relevance=relevance)
    
    # Add edges
    medical_edges = [
        ("symptoms", "fever", 0.8),
        ("symptoms", "cough", 0.7),
        ("fever", "diagnosis", 0.9),
        ("cough", "diagnosis", 0.8),
        ("diagnosis", "pneumonia", 0.95),
        ("pneumonia", "treatment", 0.9),
        ("treatment", "antibiotics", 0.85),
        ("antibiotics", "recovery", 0.8)
    ]
    
    for source, target, weight in medical_edges:
        optimizer.add_edge(source, target, weight=weight)
    
    # Detect communities
    print("Detecting communities...")
    communities = optimizer.detect_communities(algorithm="louvain")
    
    print(f"\n🏘️  Found {len(communities)} communities:")
    for i, comm in enumerate(communities, 1):
        print(f"  Community {i}: {', '.join(sorted(comm))}")
    
    # Find optimal paths
    print(f"\n🛤️  Finding paths from 'symptoms' to 'recovery':")
    
    strategies = [
        TraversalStrategy.ENERGY_OPTIMAL,
        TraversalStrategy.RELEVANCE_GUIDED,
        TraversalStrategy.COMMUNITY_AWARE
    ]
    
    for strategy in strategies:
        path = optimizer.find_optimal_path(
            start="symptoms",
            goal="recovery",
            strategy=strategy,
            max_depth=10
        )
        
        if path:
            print(f"\n  {strategy.value}:")
            print(f"    Path: {' → '.join(path.nodes)}")
            print(f"    Energy cost: {path.energy_cost:.4f} Wh")
            print(f"    Relevance: {path.relevance_score:.3f}")
            print(f"    Communities: {path.metadata['unique_communities']}")
    
    # Graph statistics
    stats = optimizer.get_graph_statistics()
    print(f"\n📊 Graph Statistics:")
    print(f"  Nodes: {stats.num_nodes}")
    print(f"  Edges: {stats.num_edges}")
    print(f"  Density: {stats.density:.3f}")
    print(f"  Communities: {stats.num_communities}")
    print(f"  Connected: {stats.is_connected}")
    
    print("\n✅ Graph traversal complete!")
    
except ImportError as e:
    print(f"⚠️  Skipping demo (module not found): {e}")

print()

# ============================================================================
# DEMO 4: Complete VimRAG Integration
# ============================================================================

print("🚀 DEMO 4: Complete VimRAG Integration")
print("-" * 80)

try:
    from vimrag_integration import VimRAGIntegration, RetrievalPipeline
    
    # Create VimRAG pipeline
    print("Initializing VimRAG integration...")
    vimrag = VimRAGIntegration(
        grid_region="US-CA",
        enable_semantic_scoring=False,  # Skip for demo speed
        enable_topological_allocation=True,
        enable_carbon_adaptation=True,
        enable_serendipity=True
    )
    
    # Add medical documents
    print("\n📚 Adding medical documents to memory graph...")
    
    documents = [
        {
            "content": "Pneumonia symptoms include high fever, productive cough, and chest pain. Requires immediate medical attention.",
            "type": "text",
            "metadata": {"category": "diagnosis", "importance": 1.0}
        },
        {
            "content": "Treatment for bacterial pneumonia typically involves antibiotics like azithromycin or amoxicillin.",
            "type": "text",
            "metadata": {"category": "treatment", "importance": 0.9}
        },
        {
            "content": "X-ray imaging shows infiltrates in lower right lung lobe consistent with pneumonia.",
            "type": "visual",
            "metadata": {"category": "imaging", "importance": 0.95}
        },
        {
            "content": "Patient recovery timeline: antibiotics for 7-10 days, rest, and fluid intake.",
            "type": "text",
            "metadata": {"category": "treatment", "importance": 0.7}
        },
        {
            "content": "Common cold is usually viral and resolves without antibiotics.",
            "type": "text",
            "metadata": {"category": "diagnosis", "importance": 0.5}
        }
    ]
    
    node_ids = []
    for doc in documents:
        node_id = vimrag.add_document(
            content=doc["content"],
            content_type=doc["type"],
            metadata=doc["metadata"]
        )
        node_ids.append(node_id)
        print(f"  Added: {node_id[:16]}... ({doc['type']})")
    
    # Connect related documents
    vimrag.connect_documents(node_ids[0], node_ids[1], weight=0.9)  # Diagnosis → Treatment
    vimrag.connect_documents(node_ids[0], node_ids[2], weight=0.85)  # Diagnosis → Imaging
    vimrag.connect_documents(node_ids[1], node_ids[3], weight=0.8)  # Treatment → Recovery
    
    # Simulate clean grid
    vimrag.update_grid_intensity(180.0)  # Clean grid: 180 g CO2/kWh
    
    # Test different pipeline modes
    query = "How to diagnose and treat pneumonia?"
    
    pipelines = [
        (RetrievalPipeline.FAST, 0.005),
        (RetrievalPipeline.GREEN, 0.003),
        (RetrievalPipeline.BALANCED, 0.01)
    ]
    
    print(f"\n🔍 Query: '{query}'")
    print("\nTesting different pipeline modes:\n")
    
    for pipeline, energy_budget in pipelines:
        print(f"  {pipeline.value.upper()} mode (budget: {energy_budget*1000:.1f} mWh)")
        
        result = vimrag.retrieve(
            query=query,
            pipeline=pipeline,
            max_results=3,
            energy_budget_wh=energy_budget,
            include_visual=True,
            agent_id="demo_agent"
        )
        
        print(f"    Retrieved: {len(result.retrieved_nodes)} nodes")
        print(f"    Energy: {result.total_energy_wh*1000:.3f} mWh")
        print(f"    Carbon: {result.total_carbon_kg*1000:.3f} g CO2")
        print(f"    Mode: {result.retrieval_mode}")
        print(f"    Time: {result.total_time_ms:.1f} ms")
        
        if result.serendipity_events:
            print(f"    💡 Efficiency gain: {result.efficiency_gains:.1%}")
            print(f"    💚 Carbon saved: {result.carbon_savings_kg*1000:.3f} g CO2")
        
        print()
    
    # Integration statistics
    print("📊 Integration Statistics:")
    stats = vimrag.get_integration_stats()
    print(f"  Total retrievals: {stats['total_retrievals']}")
    print(f"  Total energy consumed: {stats['total_energy_consumed_wh']*1000:.2f} mWh")
    print(f"  Total carbon emitted: {stats['total_carbon_emitted_kg']*1000:.2f} g CO2")
    print(f"  Total carbon saved: {stats['total_carbon_saved_kg']*1000:.2f} g CO2")
    print(f"  Memory graph: {stats['memory_graph_nodes']} nodes, {stats['memory_graph_edges']} edges")
    
    print("\n✅ VimRAG integration complete!")
    
except ImportError as e:
    print(f"⚠️  Skipping demo (module not found): {e}")

print()

# ============================================================================
# Summary
# ============================================================================

print("=" * 80)
print("✨ All VimRAG Enhancement Demos Complete!")
print("=" * 80)
print()
print("Key Features Demonstrated:")
print("  ✅ Topological token allocation (importance-based budgeting)")
print("  ✅ Semantic scoring with embeddings (vs keyword matching)")
print("  ✅ NetworkX graph operations (communities, optimal paths)")
print("  ✅ Complete VimRAG integration (all modules coordinated)")
print()
print("Sustainability Impact:")
print("  🌱 Carbon-adaptive retrieval based on grid intensity")
print("  🌱 Energy-optimal graph traversal")
print("  🌱 Token-aware filtering and compression")
print("  🌱 Serendipity logging for efficiency discovery")
print()
print("Next Steps:")
print("  1. Install missing dependencies: pip install sentence-transformers networkx")
print("  2. Test with your own documents and queries")
print("  3. Integrate with Level 4 (NSN) and Level 5 (MetaAgent)")
print("  4. Deploy to Green Agent benchmarking platform")
print()
print("=" * 80)
