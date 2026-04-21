"""
graph_registry.py  —  Recommendation 1
========================================
Centralised lifecycle manager for every graph type in the Green Agent
platform: policy, causal, execution (DecisionGraph), and ideal (Graph B).

Why this is necessary
---------------------
Without a registry, each module instantiates its own graph objects
independently:
  • Two callers can create separate CausalGraph instances with diverging
    edge weights — online learning writes to different objects.
  • PolicyGraph is garbage-collected between benchmark runs, losing
    all learned edge weights.
  • DualGraphEvaluator's ideal-path store is unreachable from
    BenchmarkHarness because they hold separate evaluator objects.

The registry solves this by providing:
  1. Singleton access to named graph instances (get_or_create pattern).
  2. Type-checked registration — prevents storing a CausalGraph under
     a "policy" slot.
  3. Snapshotting — saves the full state of every registered graph to
     disk and restores it on next startup, so learned edge weights
     survive process restarts.
  4. Health reporting — surfaces per-graph statistics in one call,
     ready to be consumed by the metrics exporter.

Usage
-----
    from core.graph_registry import GraphRegistry, GraphType

    reg = GraphRegistry(snapshot_dir="./snapshots")

    # Get-or-create pattern: safe to call multiple times
    causal  = reg.get_or_create(GraphType.CAUSAL)
    policy  = reg.get_or_create(GraphType.POLICY)

    # Register an execution graph produced by a benchmark run
    dg = DecisionGraph(graph_id="run_007", graph_type="executor")
    reg.register_execution(dg)

    # Persist learned weights to disk
    reg.snapshot()

    # Health check for the monitoring exporter
    print(reg.health())
"""

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.causal_graph import CausalGraph
from core.policy_graph import PolicyGraph
from core.dual_graph_evaluator import DecisionGraph, DualGraphEvaluator


# ---------------------------------------------------------------------------
# Graph type enum
# ---------------------------------------------------------------------------

class GraphType(str, Enum):
    CAUSAL    = "causal"
    POLICY    = "policy"
    EXECUTION = "execution"   # live DecisionGraph instances (many per session)
    IDEAL     = "ideal"       # DualGraphEvaluator ideal-path store


# ---------------------------------------------------------------------------
# Internal record
# ---------------------------------------------------------------------------

@dataclass
class GraphRecord:
    graph_type:   GraphType
    graph_id:     str
    instance:     Any
    created_at:   str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    access_count: int = 0
    last_accessed: Optional[str] = None

    def touch(self):
        self.access_count += 1
        self.last_accessed = datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class GraphRegistry:
    """
    Thread-safe singleton store for all Green Agent graph instances.

    Singletons (one per type):
      CAUSAL   →  CausalGraph
      POLICY   →  PolicyGraph
      IDEAL    →  DualGraphEvaluator  (holds the ideal-path sub-store)

    Collections (many per session):
      EXECUTION →  {graph_id: DecisionGraph}  (one per benchmark run)
    """

    # Graph type → constructor callable
    _FACTORIES = {
        GraphType.CAUSAL:  CausalGraph,
        GraphType.POLICY:  PolicyGraph,
        GraphType.IDEAL:   DualGraphEvaluator,
    }

    def __init__(self, snapshot_dir: str = "./snapshots"):
        self._lock = threading.RLock()
        self._singletons: dict[GraphType, GraphRecord] = {}
        self._executions: dict[str, GraphRecord] = {}   # graph_id → record
        self.snapshot_dir = Path(snapshot_dir)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        self._restore_snapshots()

    # ------------------------------------------------------------------
    # Singleton access (CAUSAL, POLICY, IDEAL)
    # ------------------------------------------------------------------

    def get_or_create(self, graph_type: GraphType) -> Any:
        """
        Return the singleton instance for graph_type, creating it if needed.
        Safe to call from multiple threads — creation is serialised.
        """
        if graph_type == GraphType.EXECUTION:
            raise ValueError(
                "Use register_execution() / get_execution() for EXECUTION graphs."
            )
        with self._lock:
            if graph_type not in self._singletons:
                factory = self._FACTORIES[graph_type]
                instance = factory()
                self._singletons[graph_type] = GraphRecord(
                    graph_type=graph_type,
                    graph_id=f"singleton_{graph_type.value}",
                    instance=instance,
                )
            record = self._singletons[graph_type]
            record.touch()
            return record.instance

    def get(self, graph_type: GraphType) -> Optional[Any]:
        """Return the singleton if it exists, None otherwise."""
        with self._lock:
            record = self._singletons.get(graph_type)
            if record:
                record.touch()
                return record.instance
            return None

    # ------------------------------------------------------------------
    # Execution graph collection
    # ------------------------------------------------------------------

    def register_execution(self, decision_graph: DecisionGraph) -> str:
        """Store a DecisionGraph from a benchmark run. Returns graph_id."""
        with self._lock:
            record = GraphRecord(
                graph_type=GraphType.EXECUTION,
                graph_id=decision_graph.graph_id,
                instance=decision_graph,
            )
            self._executions[decision_graph.graph_id] = record
            return decision_graph.graph_id

    def get_execution(self, graph_id: str) -> Optional[DecisionGraph]:
        with self._lock:
            record = self._executions.get(graph_id)
            if record:
                record.touch()
                return record.instance
            return None

    def list_executions(self) -> list[dict]:
        with self._lock:
            return [
                {
                    "graph_id": r.graph_id,
                    "created_at": r.created_at,
                    "access_count": r.access_count,
                    "node_count": len(r.instance.nodes),
                    "edge_count": len(r.instance.edges),
                    "graph_type": r.instance.graph_type,
                }
                for r in self._executions.values()
            ]

    def prune_executions(self, keep_last: int = 100):
        """
        Trim the execution store to the most recent keep_last graphs.
        Call periodically in long-running benchmark sessions to prevent
        unbounded memory growth.
        """
        with self._lock:
            if len(self._executions) <= keep_last:
                return
            sorted_ids = sorted(
                self._executions,
                key=lambda k: self._executions[k].created_at,
            )
            for gid in sorted_ids[:-keep_last]:
                del self._executions[gid]

    # ------------------------------------------------------------------
    # Snapshot persistence (edge-weight survival across restarts)
    # ------------------------------------------------------------------

    def snapshot(self):
        """
        Persist learned edge weights for CAUSAL and POLICY singletons.
        Called automatically after every benchmark run that calls feedback().
        """
        with self._lock:
            state = {}
            causal = self._singletons.get(GraphType.CAUSAL)
            if causal:
                state["causal_edges"] = causal.instance.export_state()["edges"]

            policy = self._singletons.get(GraphType.POLICY)
            if policy:
                state["policy_edges"] = policy.instance.export_weights()

            state["snapshot_at"] = datetime.now(timezone.utc).isoformat()
            snap_file = self.snapshot_dir / "graph_weights.json"
            with open(snap_file, "w") as f:
                json.dump(state, f, indent=2)

    def _restore_snapshots(self):
        """
        Load persisted edge weights into freshly constructed singletons.
        Called once at __init__ time.
        """
        snap_file = self.snapshot_dir / "graph_weights.json"
        if not snap_file.exists():
            return

        with open(snap_file) as f:
            state = json.load(f)

        # Restore causal edge weights
        if "causal_edges" in state:
            causal: CausalGraph = self.get_or_create(GraphType.CAUSAL)
            weight_map = {
                (e["source"], e["target"]): (e["weight"], e["confidence"])
                for e in state["causal_edges"]
            }
            for edge in causal.edges:
                key = (edge.source_id, edge.target_id)
                if key in weight_map:
                    edge.weight, edge.confidence = weight_map[key]

        # Restore policy edge weights
        if "policy_edges" in state:
            policy: PolicyGraph = self.get_or_create(GraphType.POLICY)
            weight_map = {
                (e["source"], e["target"]): e["weight"]
                for e in state["policy_edges"]
            }
            policy.update_edge_weight  # verify method exists
            for edge in policy.edges:
                key = (edge.source_id, edge.target_id)
                if key in weight_map:
                    edge.weight = weight_map[key]

    # ------------------------------------------------------------------
    # Health reporting
    # ------------------------------------------------------------------

    def health(self) -> dict:
        """
        Return a health summary for every registered graph.
        Consumed by GraphMetricsExporter.collect().
        """
        with self._lock:
            report = {
                "snapshot_dir": str(self.snapshot_dir),
                "singletons": {},
                "execution_count": len(self._executions),
            }
            for gtype, record in self._singletons.items():
                inst = record.instance
                info: dict = {
                    "graph_id": record.graph_id,
                    "access_count": record.access_count,
                    "last_accessed": record.last_accessed,
                }
                if isinstance(inst, CausalGraph):
                    info["node_count"] = len(inst.nodes)
                    info["edge_count"] = len(inst.edges)
                    info["anomaly_count"] = len(inst.get_anomalies())
                elif isinstance(inst, PolicyGraph):
                    info["node_count"] = len(inst.nodes)
                    info["edge_count"] = len(inst.edges)
                elif isinstance(inst, DualGraphEvaluator):
                    info["ideal_path_count"] = len(inst._ideal_store)
                report["singletons"][gtype.value] = info
            return report

    # ------------------------------------------------------------------
    # Convenience: cross-graph operations
    # ------------------------------------------------------------------

    def feed_diagnosis_to_policy(self, diagnosis_report: dict):
        """
        Convenience bridge: after a MetaCognition diagnosis, propagate the
        recommended action into the PolicyGraph as a context override.
        This connects Phase 2 (meta-cognition) to Phase 4 (policy graph)
        through the registry rather than requiring direct coupling.
        """
        action = diagnosis_report.get("recommended_action", "")
        policy: PolicyGraph = self.get_or_create(GraphType.POLICY)
        # Map meta-cognition actions to policy zone overrides
        zone_map = {
            "defer_until_grid_stabilizes": "red",
            "throttle_model_accuracy":     "yellow",
            "switch_to_low_power_mode":    "yellow",
            "execute_immediately":         "green",
            "proceed":                     "green",
        }
        # Inject the recommended zone into the next policy decision context
        # by updating a lightweight "override" attribute the PolicyGraph reads
        policy._meta_zone_override = zone_map.get(action, None)
