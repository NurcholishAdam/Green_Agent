"""
Phase 1 — Graph Similarity Store
=================================
Stores every benchmark execution as a labeled subgraph.
Each node = a workload dimension (task type, hardware, grid, framework).
Each edge = a causal relationship annotated with performance outcomes.

On a new task arrival, a cosine similarity search over stored feature
vectors returns the top-K most similar past executions, giving the
harness a statistical prior BEFORE the benchmark dry-run begins.

Priority: FIRST — additive on top of existing ledger, lowest risk.
"""

import json
import math
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ExecutionNode:
    node_id: str
    node_type: str          # "workload" | "hardware" | "grid" | "framework" | "outcome"
    label: str              # e.g. "question_answering", "nvidia_a100", "US-CA"
    value: Optional[float] = None   # numeric value where applicable (accuracy, SI score)


@dataclass
class ExecutionEdge:
    source_id: str
    target_id: str
    relation: str           # "produced" | "constrained_by" | "ran_on" | "belongs_to"
    weight: float = 1.0    # outcome-quality modifier on this edge


@dataclass
class ExecutionSubgraph:
    graph_id: str
    nodes: list             # list of ExecutionNode dicts (serialised)
    edges: list             # list of ExecutionEdge dicts (serialised)
    outcome_metrics: dict   # accuracy, energy_kwh, carbon_co2e_kg, sustainability_index
    timestamp: str
    framework: str


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

class GraphSimilarityStore:
    """
    Persists execution subgraphs as JSON files and maintains a flat
    in-memory index of feature vectors for fast coarse similarity search.

    Search strategy (hybrid — fast for small-to-medium stores):
      1. Cosine similarity on node-label feature vectors  →  coarse filter top-20
      2. Edge-overlap ratio on the top-20                 →  fine-grained rerank
      3. Return top-K after reranking
    """

    def __init__(self, storage_path: str = "./graph_store"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self._index: list[dict] = self._load_index()

    # ------------------------------------------------------------------
    # Index persistence
    # ------------------------------------------------------------------

    def _index_path(self) -> Path:
        return self.storage_path / "index.json"

    def _load_index(self) -> list[dict]:
        p = self._index_path()
        if p.exists():
            with open(p) as f:
                return json.load(f)
        return []

    def _save_index(self):
        with open(self._index_path(), "w") as f:
            json.dump(self._index, f, indent=2)

    # ------------------------------------------------------------------
    # Feature extraction
    # ------------------------------------------------------------------

    def _feature_vector(self, nodes: list[dict]) -> dict[str, float]:
        """
        Build a sparse feature dict from node labels.
        Key format: "{node_type}:{label}" → value (1.0 if no numeric value).
        """
        vec: dict[str, float] = {}
        for node in nodes:
            key = f"{node['node_type']}:{node['label']}"
            vec[key] = float(node.get("value") or 1.0)
        return vec

    def _edge_signature(self, edges: list[dict]) -> set[str]:
        """Set of "source_type:target_type:relation" strings for overlap check."""
        return {f"{e['source_id']}:{e['target_id']}:{e['relation']}" for e in edges}

    # ------------------------------------------------------------------
    # Similarity metrics
    # ------------------------------------------------------------------

    @staticmethod
    def _cosine(vec_a: dict, vec_b: dict) -> float:
        all_keys = set(vec_a) | set(vec_b)
        if not all_keys:
            return 0.0
        dot = sum(vec_a.get(k, 0.0) * vec_b.get(k, 0.0) for k in all_keys)
        mag_a = math.sqrt(sum(v ** 2 for v in vec_a.values()))
        mag_b = math.sqrt(sum(v ** 2 for v in vec_b.values()))
        if mag_a == 0 or mag_b == 0:
            return 0.0
        return dot / (mag_a * mag_b)

    @staticmethod
    def _edge_overlap(sig_a: set, sig_b: set) -> float:
        if not sig_a and not sig_b:
            return 1.0
        intersection = len(sig_a & sig_b)
        union = len(sig_a | sig_b)
        return intersection / union if union else 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def store_execution(self, subgraph: ExecutionSubgraph):
        """
        Persist a completed execution subgraph and update the index.

        Call this at the END of every benchmark run inside BenchmarkHarness.
        """
        graph_file = self.storage_path / f"{subgraph.graph_id}.json"
        fvec = self._feature_vector(subgraph.nodes)
        esig = list(self._edge_signature(subgraph.edges))

        payload = {
            "graph_id": subgraph.graph_id,
            "nodes": subgraph.nodes,
            "edges": subgraph.edges,
            "outcome_metrics": subgraph.outcome_metrics,
            "timestamp": subgraph.timestamp,
            "framework": subgraph.framework,
        }
        with open(graph_file, "w") as f:
            json.dump(payload, f, indent=2)

        self._index.append({
            "graph_id": subgraph.graph_id,
            "feature_vector": fvec,
            "edge_signature": esig,
            "outcome_metrics": subgraph.outcome_metrics,
            "framework": subgraph.framework,
        })
        self._save_index()

    def find_similar(self, query_nodes: list[dict],
                     query_edges: list[dict] = None,
                     top_k: int = 3) -> list[dict]:
        """
        Hybrid similarity search.
        Returns top_k results with similarity_score and outcome_metrics.
        """
        query_vec = self._feature_vector(query_nodes)
        query_sig = self._edge_signature(query_edges or [])

        scored = []
        for entry in self._index:
            cos = self._cosine(query_vec, entry["feature_vector"])
            edge_sim = self._edge_overlap(query_sig, set(entry.get("edge_signature", [])))
            # Weighted combination: 70% node similarity, 30% edge overlap
            combined = 0.70 * cos + 0.30 * edge_sim
            scored.append({
                "graph_id": entry["graph_id"],
                "similarity_score": round(combined, 4),
                "cosine_similarity": round(cos, 4),
                "edge_overlap": round(edge_sim, 4),
                "outcome_metrics": entry["outcome_metrics"],
                "framework": entry.get("framework", "unknown"),
            })

        scored.sort(key=lambda x: x["similarity_score"], reverse=True)
        return scored[:top_k]

    def build_prior(self, similar_results: list[dict]) -> dict:
        """
        Aggregate top-K results into a statistical performance prior.
        Returns expected ranges for each outcome metric.

        Usage: show this prior in BenchmarkHarness BEFORE running the task,
        then flag actual results that fall outside [min, max].
        """
        if not similar_results:
            return {"available": False}

        keys = ["accuracy", "energy_kwh", "carbon_co2e_kg", "sustainability_index"]
        prior: dict = {"available": True, "n_samples": len(similar_results), "metrics": {}}

        for key in keys:
            values = [
                r["outcome_metrics"][key]
                for r in similar_results
                if key in r["outcome_metrics"]
            ]
            if not values:
                continue
            mean = sum(values) / len(values)
            variance = sum((v - mean) ** 2 for v in values) / len(values)
            prior["metrics"][key] = {
                "min": round(min(values), 6),
                "max": round(max(values), 6),
                "mean": round(mean, 6),
                "std_dev": round(math.sqrt(variance), 6),
            }
        return prior

    def flag_outlier(self, prior: dict, actual_metrics: dict,
                     sigma_threshold: float = 2.0) -> list[dict]:
        """
        Compare actual benchmark results against the prior.
        Returns a list of outlier flags for metrics that deviate > sigma_threshold.

        Enables BenchmarkHarness to raise anomaly alerts automatically.
        """
        if not prior.get("available"):
            return []
        flags = []
        for key, stats in prior.get("metrics", {}).items():
            if key not in actual_metrics:
                continue
            actual = actual_metrics[key]
            mean = stats["mean"]
            std = stats["std_dev"]
            if std == 0:
                continue
            z_score = abs(actual - mean) / std
            if z_score > sigma_threshold:
                flags.append({
                    "metric": key,
                    "actual": actual,
                    "expected_mean": mean,
                    "z_score": round(z_score, 3),
                    "severity": "critical" if z_score > 3.0 else "warning",
                })
        return flags
