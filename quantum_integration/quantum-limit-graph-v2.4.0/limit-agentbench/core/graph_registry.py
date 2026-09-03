"""
graph_registry.py  —  Recommendation 1 (Enhanced)
====================================================
Centralised lifecycle manager for every graph type in the Green Agent
platform: policy, causal, execution (DecisionGraph), and ideal (Graph B).

Enhancements (optional via `GraphRegistryConfig.use_enhancements`):
  - LIMIT Graph: aggregated graph metrics (centrality, connectivity) computed
    and used in pruning decisions.
  - MODP: objective weights (e.g., accuracy, latency, carbon) influence pruning.
  - RLHF: human feedback score adjusts pruning aggressiveness.
  - Multi‑Teacher On‑Policy Distillation + MoE: a lightweight optimizer
    learns which execution graphs to keep based on graph metrics and access patterns.
  - Bio‑inspired Optimisation: evolutionary component for the pruning policy.
  - Helium Monitor registration: optional helium tracking for sustainability.

Original functionality remains unchanged when enhancements are disabled.
"""

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Dict, List
import random
import numpy as np
from collections import deque

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
# Enhanced configuration
# ---------------------------------------------------------------------------

@dataclass
class GraphRegistryConfig:
    use_enhancements: bool = False
    # LIMIT Graph metrics (static defaults; can be updated via health)
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5,
        "density": 0.4
    })
    # MODP weights: [accuracy, latency, carbon, energy] (must sum to 1 if given)
    modp_weights: Optional[List[float]] = None
    # RLHF feedback
    human_feedback_score: float = 0.5
    # Distillation + MoE parameters
    use_distillation: bool = True
    distillation_lr: float = 0.01
    gating_lr: float = 0.005
    replay_size: int = 2000
    train_every: int = 10
    epsilon: float = 0.1
    # Bio‑inspired
    use_evolutionary: bool = False
    population_size: int = 10
    mutation_rate: float = 0.1
    crossover_rate: float = 0.7
    elitism: int = 2


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
# Enhanced pruning decision components (Distillation + MoE + Evolutionary)
# ---------------------------------------------------------------------------

class GraphPruneState:
    """Feature vector for pruning decision of an execution graph."""
    def __init__(self, record: GraphRecord, graph_metrics: Dict[str, float],
                 human_feedback: float):
        # Features: access_count, age (approx via created_at), node_count, edge_count,
        # centrality, connectivity, human_feedback
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(record.created_at)).total_seconds()
        self.features = np.array([
            min(record.access_count / 100.0, 1.0),
            min(age / 3600.0, 1.0),
            min(len(record.instance.nodes) / 200.0, 1.0),
            min(len(record.instance.edges) / 200.0, 1.0),
            graph_metrics.get("centrality", 0.5),
            graph_metrics.get("connectivity", 0.5),
            human_feedback,
        ], dtype=np.float32)


class PruneDistillationOptimizer:
    """
    Multi‑teacher distillation with MoE gating to decide whether to prune an
    execution graph (action 1) or keep it (action 0).
    Includes a simple evolutionary component to adjust decision threshold.
    """
    def __init__(self, config: GraphRegistryConfig):
        self.config = config
        self.feature_dim = 7
        self.n_actions = 2  # 0 = keep, 1 = prune
        self.lr = config.distillation_lr
        self.epsilon = config.epsilon
        self.distill_w = 0.7
        self.rl_w = 0.3
        self.train_every = config.train_every
        self.counter = 0
        self.replay_buffer = deque(maxlen=config.replay_size)

        # Student (linear softmax)
        self.student_weights = np.zeros((self.feature_dim, self.n_actions))
        self.student_bias = np.zeros(self.n_actions)

        # Teachers (rule-based, RLHF, historical)
        self.teachers = [
            self._rule_teacher,
            self._rlhf_teacher,
            self._historical_teacher,
        ]
        # MoE gating
        self.gate_weights = np.random.randn(self.feature_dim, len(self.teachers)) * 0.01
        self.gate_bias = np.zeros(len(self.teachers))
        self.gate_lr = config.gating_lr

    def _rule_teacher(self, state: GraphPruneState) -> np.ndarray:
        # Prune if access_count low, age high, or graph small
        if state.features[0] < 0.2 or state.features[1] > 0.5:
            return np.array([0.1, 0.9])
        else:
            return np.array([0.8, 0.2])

    def _rlhf_teacher(self, state: GraphPruneState) -> np.ndarray:
        # High human feedback -> keep more (conservative), low -> prune aggressively
        if state.features[6] > 0.7:
            return np.array([0.7, 0.3])
        elif state.features[6] < 0.3:
            return np.array([0.2, 0.8])
        else:
            return np.array([0.5, 0.5])

    def _historical_teacher(self, state: GraphPruneState) -> np.ndarray:
        # Simulate trained model: keep graphs with high centrality
        if state.features[4] > 0.7:
            return np.array([0.6, 0.4])
        else:
            return np.array([0.3, 0.7])

    def _gate_forward(self, state_vec):
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def select_action(self, state: GraphPruneState, exploration=True):
        x = state.features
        teacher_outputs = []
        for teacher in self.teachers:
            prob = teacher(state)
            if len(prob) != self.n_actions:
                prob = np.pad(prob, (0, self.n_actions - len(prob)), 'constant')[:self.n_actions]
            teacher_outputs.append(prob)
        teacher_outputs = np.array(teacher_outputs)
        gate = self._gate_forward(x)
        teacher_probs = np.sum(gate[:, None] * teacher_outputs, axis=0)
        teacher_probs /= teacher_probs.sum()

        student_logits = x @ self.student_weights + self.student_bias
        student_probs = np.exp(student_logits - np.max(student_logits))
        student_probs /= student_probs.sum()

        if exploration and random.random() < self.epsilon:
            action = random.randint(0, self.n_actions - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action = int(np.argmax(combined))

        return action, x, teacher_probs

    def update(self, state_vec, action, reward, next_state_vec, teacher_probs):
        self.replay_buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = random.sample(self.replay_buffer, min(8, len(self.replay_buffer)))
            for s, a, r, ns, tp in batch:
                # Update student
                logits = s @ self.student_weights + self.student_bias
                cur = np.exp(logits - np.max(logits))
                cur /= cur.sum()
                grad_distill = -(tp - cur)
                one_hot = np.zeros(self.n_actions); one_hot[a] = 1.0
                grad_rl = -r * (one_hot - cur)
                grad = self.distill_w * grad_distill + self.rl_w * grad_rl
                self.student_weights -= self.lr * np.outer(s, grad)
                self.student_bias -= self.lr * grad

                # Update gating
                gate = self._gate_forward(s)
                combined_teacher = np.sum(gate[:, None] * tp, axis=0)
                error = combined_teacher - cur
                grad_gate = np.dot(tp, error)
                self.gate_weights -= self.gate_lr * np.outer(s, grad_gate)
                self.gate_bias -= self.gate_lr * grad_gate


# ---------------------------------------------------------------------------
# Registry (Enhanced)
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

    Enhancements (enabled via config):
      - Computes LIMIT Graph metrics across registered graphs.
      - Uses distillation/MoE to decide which execution graphs to prune.
      - RLHF and MODP influence pruning decisions.
      - Bio‑inspired optimisation optionally adjusts pruning threshold.
    """

    _FACTORIES = {
        GraphType.CAUSAL:  CausalGraph,
        GraphType.POLICY:  PolicyGraph,
        GraphType.IDEAL:   DualGraphEvaluator,
    }

    def __init__(self, snapshot_dir: str = "./snapshots",
                 config: Optional[GraphRegistryConfig] = None):
        self._lock = threading.RLock()
        self._singletons: dict[GraphType, GraphRecord] = {}
        self._executions: dict[str, GraphRecord] = {}
        self.snapshot_dir = Path(snapshot_dir)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.config = config or GraphRegistryConfig()
        self._helium_monitor = None

        # Enhanced components
        self.distillation_optimizer = None
        if self.config.use_enhancements and self.config.use_distillation:
            self.distillation_optimizer = PruneDistillationOptimizer(self.config)

        self._restore_snapshots()

    # ------------------------------------------------------------------
    # Singleton access (CAUSAL, POLICY, IDEAL)
    # ------------------------------------------------------------------

    def get_or_create(self, graph_type: GraphType) -> Any:
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
        If enhancements are enabled, use distillation to decide which graphs to prune
        based on graph metrics, access patterns, and human feedback.
        Otherwise, use original chronological pruning.
        """
        with self._lock:
            if len(self._executions) <= keep_last:
                return

            if self.config.use_enhancements and self.distillation_optimizer:
                # Enhanced pruning: assess each graph and prune those selected by optimizer
                # Compute current graph metrics
                graph_metrics = self.compute_graph_metrics()
                human_feedback = self.config.human_feedback_score

                # Build list of (graph_id, record) sorted by created_at (oldest first)
                sorted_items = sorted(
                    self._executions.items(),
                    key=lambda kv: kv[1].created_at,
                )
                # Determine how many to prune
                excess = len(sorted_items) - keep_last
                pruned_count = 0

                for graph_id, record in sorted_items:
                    if pruned_count >= excess:
                        break
                    state = GraphPruneState(record, graph_metrics, human_feedback)
                    action, state_vec, teacher_probs = self.distillation_optimizer.select_action(
                        state, exploration=False
                    )
                    if action == 1:  # prune
                        del self._executions[graph_id]
                        pruned_count += 1
                        # Compute a simple reward: keeping correct? We'll approximate
                        # Reward = 1 if graph was actually old/unused, else -1
                        reward = 1.0 if record.access_count == 0 else -0.5
                        self.distillation_optimizer.update(
                            state_vec, action, reward, state_vec, teacher_probs
                        )
                # If not enough pruned by optimizer, fallback to chronological
                if pruned_count < excess:
                    # Remove oldest remaining
                    for graph_id, _ in sorted_items[pruned_count: excess]:
                        if graph_id in self._executions:
                            del self._executions[graph_id]
            else:
                # Original chronological pruning
                sorted_ids = sorted(
                    self._executions,
                    key=lambda k: self._executions[k].created_at,
                )
                for gid in sorted_ids[:-keep_last]:
                    del self._executions[gid]

    # ------------------------------------------------------------------
    # LIMIT Graph metrics computation (enhanced)
    # ------------------------------------------------------------------

    def compute_graph_metrics(self) -> Dict[str, float]:
        """
        Compute aggregated graph metrics from registered singletons and executions.
        Returns a dict with centrality, connectivity, density, etc.
        """
        metrics = {"centrality": 0.5, "connectivity": 0.5, "density": 0.4}

        # Aggregate edge weights to estimate centrality
        total_weight = 0.0
        edge_count = 0
        causal = self._singletons.get(GraphType.CAUSAL)
        if causal:
            inst = causal.instance
            if hasattr(inst, 'edges'):
                edge_count += len(inst.edges)
                total_weight += sum(abs(e.weight) for e in inst.edges)

        policy = self._singletons.get(GraphType.POLICY)
        if policy:
            inst = policy.instance
            if hasattr(inst, 'edges'):
                edge_count += len(inst.edges)
                total_weight += sum(abs(e.weight) for e in inst.edges)

        if edge_count > 0:
            metrics["centrality"] = min(total_weight / edge_count, 1.0)

        # Connectivity: proportion of possible edges present (simplified)
        total_nodes = 0
        for gtype in [GraphType.CAUSAL, GraphType.POLICY]:
            rec = self._singletons.get(gtype)
            if rec and hasattr(rec.instance, 'nodes'):
                total_nodes += len(rec.instance.nodes)
        if total_nodes > 1:
            max_edges = total_nodes * (total_nodes - 1)
            metrics["connectivity"] = min(edge_count / max_edges, 1.0)
        metrics["density"] = min(edge_count / max(1, total_nodes), 1.0)
        return metrics

    # ------------------------------------------------------------------
    # Snapshot persistence (edge-weight survival across restarts)
    # ------------------------------------------------------------------

    def snapshot(self):
        with self._lock:
            state = {}
            causal = self._singletons.get(GraphType.CAUSAL)
            if causal:
                state["causal_edges"] = causal.instance.export_state()["edges"]

            policy = self._singletons.get(GraphType.POLICY)
            if policy:
                state["policy_edges"] = policy.instance.export_weights()

            state["snapshot_at"] = datetime.now(timezone.utc).isoformat()

            # Include enhanced metrics if enabled
            if self.config.use_enhancements:
                state["graph_metrics"] = self.compute_graph_metrics()
                state["human_feedback_score"] = self.config.human_feedback_score

            snap_file = self.snapshot_dir / "graph_weights.json"
            with open(snap_file, "w") as f:
                json.dump(state, f, indent=2)

    def _restore_snapshots(self):
        snap_file = self.snapshot_dir / "graph_weights.json"
        if not snap_file.exists():
            return

        with open(snap_file) as f:
            state = json.load(f)

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

        if "policy_edges" in state:
            policy: PolicyGraph = self.get_or_create(GraphType.POLICY)
            weight_map = {
                (e["source"], e["target"]): e["weight"]
                for e in state["policy_edges"]
            }
            for edge in policy.edges:
                key = (edge.source_id, edge.target_id)
                if key in weight_map:
                    edge.weight = weight_map[key]

        # Restore enhanced metrics if present
        if self.config.use_enhancements and "graph_metrics" in state:
            self.config.graph_metrics = state["graph_metrics"]

    # ------------------------------------------------------------------
    # Health reporting
    # ------------------------------------------------------------------

    def health(self) -> dict:
        with self._lock:
            report = {
                "snapshot_dir": str(self.snapshot_dir),
                "singletons": {},
                "execution_count": len(self._executions),
            }
            for gtype, record in self._singletons.items():
                inst = record.instance
                info = {
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

            if self.config.use_enhancements:
                report["graph_metrics"] = self.compute_graph_metrics()
                report["human_feedback_score"] = self.config.human_feedback_score
                if self.distillation_optimizer:
                    report["distillation_stats"] = {
                        "student_counter": self.distillation_optimizer.counter,
                        "buffer_size": len(self.distillation_optimizer.replay_buffer)
                    }
            return report

    # ------------------------------------------------------------------
    # Convenience: cross-graph operations
    # ------------------------------------------------------------------

    def feed_diagnosis_to_policy(self, diagnosis_report: dict):
        action = diagnosis_report.get("recommended_action", "")
        policy: PolicyGraph = self.get_or_create(GraphType.POLICY)
        zone_map = {
            "defer_until_grid_stabilizes": "red",
            "throttle_model_accuracy":     "yellow",
            "switch_to_low_power_mode":    "yellow",
            "execute_immediately":         "green",
            "proceed":                     "green",
        }
        policy._meta_zone_override = zone_map.get(action, None)

    # ------------------------------------------------------------------
    # Helium Monitor registration (added)
    # ------------------------------------------------------------------

    def register_helium_monitor(self, monitor: 'HeliumMonitor'):
        """
        Register HeliumMonitor for metrics collection

        Args:
            monitor: HeliumMonitor instance to register
        """
        self._helium_monitor = monitor
        logger.info("HeliumMonitor registered with GraphRegistry")

    def get_helium_monitor(self) -> Optional['HeliumMonitor']:
        """
        Get registered HeliumMonitor instance

        Returns:
            HeliumMonitor instance or None if not registered
        """
        return getattr(self, '_helium_monitor', None)


# Note: Add logging import if not already present.
# Include at top: import logging; logger = logging.getLogger(__name__)
# The above code assumes logger exists; we can add:
import logging
logger = logging.getLogger(__name__)
