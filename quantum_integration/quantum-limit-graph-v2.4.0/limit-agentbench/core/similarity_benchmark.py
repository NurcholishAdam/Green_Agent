"""
Phase 1 — Similarity Benchmark Integration
============================================
Provides the factory helpers that BenchmarkHarness calls to:
  1. Build an ExecutionSubgraph from a completed result  (store_result)
  2. Retrieve a performance prior BEFORE a new run      (get_prior)
  3. Check for outliers AFTER a run                     (check_outliers)

How to integrate into existing BenchmarkHarness.run_benchmark():

    # top of method, before agent execution
    prior_result = get_benchmark_prior(store, task, hardware, grid, framework)
    if prior_result["prior"]["available"]:
        print("Prior available:", prior_result["prior"])

    # bottom of method, after result is collected
    subgraph = build_execution_subgraph(task, hardware, grid, framework, metrics)
    store.store_execution(subgraph)
    outliers = store.flag_outlier(prior_result["prior"], metrics)
    if outliers:
        print("OUTLIER DETECTED:", outliers)
"""

import uuid
from datetime import datetime, timezone
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from metrics.graph_similarity_store import GraphSimilarityStore, ExecutionSubgraph


# ---------------------------------------------------------------------------
# Subgraph factory
# ---------------------------------------------------------------------------

def build_execution_subgraph(
    task: dict,
    hardware_profile: str,
    grid_region: str,
    framework: str,
    outcome_metrics: dict,
) -> ExecutionSubgraph:
    """
    Convert a completed benchmark result into a labelled ExecutionSubgraph
    ready for storage in GraphSimilarityStore.

    Parameters
    ----------
    task             : AgentBench task dict (task_id, task_type, suite)
    hardware_profile : e.g. "nvidia_a100"
    grid_region      : e.g. "US-CA"
    framework        : e.g. "langchain", "autogen"
    outcome_metrics  : dict with accuracy, energy_kwh, carbon_co2e_kg, SI
    """
    nodes = [
        {
            "node_id": "n_task",
            "node_type": "workload",
            "label": task.get("task_type", "unknown"),
            "value": None,
        },
        {
            "node_id": "n_suite",
            "node_type": "suite",
            "label": task.get("suite", "unknown"),
            "value": None,
        },
        {
            "node_id": "n_hw",
            "node_type": "hardware",
            "label": hardware_profile,
            "value": None,
        },
        {
            "node_id": "n_grid",
            "node_type": "grid",
            "label": grid_region,
            "value": None,
        },
        {
            "node_id": "n_fw",
            "node_type": "framework",
            "label": framework,
            "value": None,
        },
        {
            "node_id": "n_acc",
            "node_type": "outcome",
            "label": "accuracy",
            "value": outcome_metrics.get("accuracy", 0.0),
        },
        {
            "node_id": "n_energy",
            "node_type": "outcome",
            "label": "energy_kwh",
            "value": outcome_metrics.get("energy_kwh", 0.0),
        },
        {
            "node_id": "n_si",
            "node_type": "outcome",
            "label": "sustainability_index",
            "value": outcome_metrics.get("sustainability_index", 0.0),
        },
    ]

    edges = [
        {"source_id": "n_task", "target_id": "n_suite",
         "relation": "belongs_to", "weight": 1.0},
        {"source_id": "n_task", "target_id": "n_hw",
         "relation": "ran_on", "weight": 1.0},
        {"source_id": "n_task", "target_id": "n_grid",
         "relation": "constrained_by", "weight": 1.0},
        {"source_id": "n_fw", "target_id": "n_task",
         "relation": "executed", "weight": 1.0},
        {
            "source_id": "n_task", "target_id": "n_acc",
            "relation": "produced",
            "weight": outcome_metrics.get("accuracy", 0.0),
        },
        {
            "source_id": "n_task", "target_id": "n_energy",
            "relation": "consumed",
            "weight": outcome_metrics.get("energy_kwh", 0.0),
        },
        {
            "source_id": "n_task", "target_id": "n_si",
            "relation": "produced",
            "weight": outcome_metrics.get("sustainability_index", 0.0),
        },
    ]

    return ExecutionSubgraph(
        graph_id=str(uuid.uuid4()),
        nodes=nodes,
        edges=edges,
        outcome_metrics={
            "accuracy": outcome_metrics.get("accuracy", 0.0),
            "energy_kwh": outcome_metrics.get("energy_kwh", 0.0),
            "carbon_co2e_kg": outcome_metrics.get("carbon_co2e_kg", 0.0),
            "sustainability_index": outcome_metrics.get("sustainability_index", 0.0),
        },
        timestamp=datetime.now(timezone.utc).isoformat(),
        framework=framework,
    )


# ---------------------------------------------------------------------------
# Prior retrieval helper
# ---------------------------------------------------------------------------

def get_benchmark_prior(
    store: GraphSimilarityStore,
    task: dict,
    hardware_profile: str,
    grid_region: str,
    framework: str,
    top_k: int = 3,
) -> dict:
    """
    Call this BEFORE running a benchmark task to retrieve a performance prior.

    Returns
    -------
    {
        "prior": { "available": bool, "n_samples": int, "metrics": {...} },
        "similar_executions": [ top-K result dicts ],
        "prior_available": bool,
    }

    If no history exists yet (cold start), prior["available"] == False
    and the harness proceeds with no prior — exactly the current behaviour.
    """
    query_nodes = [
        {"node_id": "q_task", "node_type": "workload",
         "label": task.get("task_type", "unknown"), "value": None},
        {"node_id": "q_suite", "node_type": "suite",
         "label": task.get("suite", "unknown"), "value": None},
        {"node_id": "q_hw", "node_type": "hardware",
         "label": hardware_profile, "value": None},
        {"node_id": "q_grid", "node_type": "grid",
         "label": grid_region, "value": None},
        {"node_id": "q_fw", "node_type": "framework",
         "label": framework, "value": None},
    ]

    query_edges = [
        {"source_id": "q_task", "target_id": "q_hw",
         "relation": "ran_on"},
        {"source_id": "q_task", "target_id": "q_grid",
         "relation": "constrained_by"},
        {"source_id": "q_fw", "target_id": "q_task",
         "relation": "executed"},
    ]

    similar = store.find_similar(query_nodes, query_edges, top_k=top_k)
    prior = store.build_prior(similar)

    return {
        "prior": prior,
        "similar_executions": similar,
        "prior_available": prior.get("available", False),
    }
