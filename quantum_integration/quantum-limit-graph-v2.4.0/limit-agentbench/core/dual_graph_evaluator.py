"""
Phase 5 — Dual-Graph AI Evaluator (Enhanced)
====================================
Compares Graph A (actual agent decision path) against Graph B (ideal/
counterfactual path) using an approximate Graph Edit Distance (GED)
algorithm. Produces an Explainable AI (XAI) evaluation report that
explains WHY a decision path was suboptimal, not just THAT it was.

Enhancements (optional via `config`):
  - LIMIT Graph metrics (centrality, connectivity) influence gap severity.
  - MODP: objective weights combine accuracy, energy, latency, carbon.
  - RLHF: human feedback score re‑weights XAI severity.
  - Multi‑Teacher On‑Policy Distillation + MoE: learns to adjust edit costs.
  - Bio‑inspired optimisation: evolutionary tuning of edit costs.

Original functionality remains unchanged when enhancements are disabled.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
import random
import numpy as np
from collections import deque


# ---------------------------------------------------------------------------
# Decision graph primitives
# ---------------------------------------------------------------------------

@dataclass
class DecisionNode:
    node_id: str
    action: str
    context_snapshot: dict = field(default_factory=dict)
    outcome_delta_si: float = 0.0


@dataclass
class DecisionEdge:
    source_id: str
    target_id: str
    transition_type: str = "followed_by"
    weight: float = 1.0


class DecisionGraph:
    """Represents a single agent execution as a sequence of labelled decision nodes."""

    def __init__(self, graph_id: str, graph_type: str = "executor"):
        self.graph_id = graph_id
        self.graph_type = graph_type
        self.nodes: dict[str, DecisionNode] = {}
        self.edges: list[DecisionEdge] = []
        self._sequence: list[str] = []

    def add_step(self, node: DecisionNode, after_id: Optional[str] = None):
        self.nodes[node.node_id] = node
        self._sequence.append(node.node_id)
        if after_id and after_id in self.nodes:
            self.edges.append(DecisionEdge(source_id=after_id, target_id=node.node_id))

    def get_action_sequence(self) -> list[str]:
        return [self.nodes[nid].action for nid in self._sequence if nid in self.nodes]

    def total_outcome_delta(self) -> float:
        return sum(n.outcome_delta_si for n in self.nodes.values())


# ---------------------------------------------------------------------------
# Edit operation
# ---------------------------------------------------------------------------

@dataclass
class EditOperation:
    op_type: str
    step_index: int
    actual_action: Optional[str] = None
    ideal_action: Optional[str] = None
    cost: float = 0.0
    gap_type: str = ""
    explanation: str = ""


# ---------------------------------------------------------------------------
# Enhanced configuration for dual-graph evaluator
# ---------------------------------------------------------------------------

@dataclass
class DualGraphConfig:
    use_enhancements: bool = False
    # LIMIT Graph
    graph_metrics: Dict[str, float] = field(default_factory=lambda: {
        "centrality": 0.5,
        "connectivity": 0.5
    })
    # MODP weights: [energy, latency, carbon, quality]
    modp_weights: Optional[List[float]] = None  # default [0.3, 0.3, 0.2, 0.2]
    # RLHF
    human_feedback_score: float = 0.5
    # Distillation + MoE
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
# Teachers for edit cost prediction (enhanced)
# ---------------------------------------------------------------------------

class EditCostTeacher:
    """Base teacher for adjusting edit costs."""
    def predict(self, seq_a_len: int, seq_b_len: int, graph_metrics: Dict[str, float]) -> float:
        raise NotImplementedError


class RuleBasedTeacher(EditCostTeacher):
    def predict(self, seq_a_len, seq_b_len, graph_metrics):
        # Heuristic: if graph centrality high, prefer relabel cost lower (more lenient)
        centrality = graph_metrics.get('centrality', 0.5)
        if centrality > 0.7:
            return 0.6  # lower relabel cost
        elif seq_a_len + seq_b_len > 10:
            return 0.8
        else:
            return 0.7  # default


class RLHFTeacher(EditCostTeacher):
    def predict(self, seq_a_len, seq_b_len, graph_metrics):
        # Human feedback: high -> prefer lower relabel cost (trust agent more)
        # This is a placeholder; real implementation would use a trained model
        return 0.7


class HistoricalTeacher(EditCostTeacher):
    def __init__(self):
        self.weights = np.array([0.05, 0.1, 0.02])  # example
    def predict(self, seq_a_len, seq_b_len, graph_metrics):
        # Simple linear model on features
        feat = np.array([seq_a_len/10.0, seq_b_len/10.0, graph_metrics.get('centrality', 0.5)])
        return float(np.dot(feat, self.weights)) + 0.7


# ---------------------------------------------------------------------------
# Distillation optimizer for edit costs (enhanced)
# ---------------------------------------------------------------------------

class EditCostDistillationOptimizer:
    """
    Multi‑teacher distillation with MoE gating to predict the relabel edit cost.
    Action space: continuous relabel cost in [0.5, 1.0].
    """
    def __init__(self, config: DualGraphConfig):
        self.config = config
        self.feature_dim = 3  # seq_a_len_norm, seq_b_len_norm, centrality
        self.student_weights = np.zeros(self.feature_dim)
        self.student_bias = 0.7  # default relabel cost
        self.lr = config.distillation_lr
        self.epsilon = config.epsilon
        self.teachers = [RuleBasedTeacher(), RLHFTeacher(), HistoricalTeacher()]
        # MoE gating network
        self.gate_weights = np.random.randn(self.feature_dim, len(self.teachers)) * 0.01
        self.gate_bias = np.zeros(len(self.teachers))
        self.gate_lr = config.gating_lr
        self.replay = deque(maxlen=config.replay_size)
        self.counter = 0
        self.train_every = config.train_every

    def predict_relabel_cost(self, seq_a_len: int, seq_b_len: int,
                             graph_metrics: Dict[str, float]) -> float:
        # Build state features
        state = np.array([
            min(seq_a_len / 10.0, 1.0),
            min(seq_b_len / 10.0, 1.0),
            min(graph_metrics.get('centrality', 0.5), 1.0)
        ], dtype=np.float32)

        # Teacher predictions
        teacher_preds = np.array([t.predict(seq_a_len, seq_b_len, graph_metrics)
                                  for t in self.teachers])
        # MoE gating
        logits = state @ self.gate_weights + self.gate_bias
        gate_probs = np.exp(logits - np.max(logits))
        gate_probs /= gate_probs.sum()
        teacher_combined = np.dot(gate_probs, teacher_preds)

        # Student prediction
        student_pred = np.dot(state, self.student_weights) + self.student_bias

        # Blend (with exploration)
        if random.random() < self.epsilon:
            predicted = teacher_combined
        else:
            predicted = 0.8 * student_pred + 0.2 * teacher_combined

        # Clip to valid range
        return float(np.clip(predicted, 0.5, 1.0))

    def update(self, state_vec: np.ndarray, reward: float):
        self.replay.append((state_vec, reward))
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay) >= 8:
            batch = random.sample(self.replay, min(8, len(self.replay)))
            for s, r in batch:
                # Update student (simple regression towards reward)
                pred = np.dot(s, self.student_weights) + self.student_bias
                # Target: reward >0 means we want higher relabel cost (or lower? define reward)
                # We define reward as negative normalized GED, so higher is better.
                # We'll just move student towards a target based on reward sign.
                target = 0.8 if r > 0 else 0.6
                grad = (pred - target) * s
                self.student_weights -= self.lr * grad
                self.student_bias -= self.lr * (pred - target)

                # Update gating (simplified)
                gate_probs = self._gate_forward(s)
                combined = np.dot(gate_probs, [t.predict(0,0,{}) for t in self.teachers])  # placeholder
                error = combined - target
                grad_gate = combined * error
                self.gate_weights -= self.gate_lr * np.outer(s, grad_gate)
                self.gate_bias -= self.gate_lr * grad_gate

    def _gate_forward(self, state_vec):
        logits = state_vec @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()


# ---------------------------------------------------------------------------
# Evaluator with enhancements
# ---------------------------------------------------------------------------

class DualGraphEvaluator:
    """
    GED-based evaluator with XAI reasoning gap classification.

    Enhanced with optional distillation, MoE, RLHF, LIMIT Graph, MODP, and evolutionary.
    """

    EDIT_COSTS = {
        "insert":  1.0,
        "delete":  1.0,
        "relabel": 0.7,
        "no_op":   0.0,
    }

    CRITICAL_SUBSTITUTIONS: dict[tuple, str] = {
        ("defer",    "execute"):  "unnecessary_deferral",
        ("execute",  "defer"):    "failed_to_defer",
        ("quantize", "prune"):    "wrong_optimization_strategy",
        ("throttle", "execute"):  "over_throttled",
        ("execute",  "throttle"): "under_throttled",
        ("prune",    "quantize"): "wrong_optimization_strategy",
    }

    def __init__(self, config: Optional[DualGraphConfig] = None):
        self._ideal_store: dict[str, DecisionGraph] = {}
        self.config = config or DualGraphConfig()

        # Enhanced components
        if self.config.use_enhancements:
            if self.config.use_distillation:
                self.distillation_optimizer = EditCostDistillationOptimizer(self.config)
            else:
                self.distillation_optimizer = None

            # Set MODP weights (default)
            if self.config.modp_weights is None:
                self.config.modp_weights = [0.3, 0.3, 0.2, 0.2]  # energy, latency, carbon, quality
            else:
                total = sum(self.config.modp_weights)
                self.config.modp_weights = [w / total for w in self.config.modp_weights]
        else:
            self.distillation_optimizer = None

    # ------------------------------------------------------------------
    # Ideal path registry
    # ------------------------------------------------------------------

    def register_ideal_path(self, task_type: str, graph_b: DecisionGraph):
        self._ideal_store[task_type] = graph_b

    def build_ideal_from_sequence(self, task_type: str,
                                   action_sequence: list[str]) -> DecisionGraph:
        graph_b = DecisionGraph(graph_id=f"ideal_{task_type}", graph_type="evaluator")
        prev_id = None
        for i, action in enumerate(action_sequence):
            node = DecisionNode(node_id=f"ideal_step_{i}", action=action)
            graph_b.add_step(node, after_id=prev_id)
            prev_id = node.node_id
        self.register_ideal_path(task_type, graph_b)
        return graph_b

    # ------------------------------------------------------------------
    # GED computation (DP Levenshtein on action sequences)
    # ------------------------------------------------------------------

    def compute_ged(self, graph_a: DecisionGraph, graph_b: DecisionGraph) -> dict:
        """
        Approximate GED via sequence alignment on action labels.
        If enhancements enabled, relabel cost is dynamically set via distillation.
        """
        seq_a = graph_a.get_action_sequence()
        seq_b = graph_b.get_action_sequence()

        # Enhanced: adjust relabel cost using distillation
        if self.config.use_enhancements and self.distillation_optimizer:
            relabel_cost = self.distillation_optimizer.predict_relabel_cost(
                len(seq_a), len(seq_b), self.config.graph_metrics
            )
            edit_costs = {
                "insert": 1.0,
                "delete": 1.0,
                "relabel": relabel_cost,
                "no_op": 0.0,
            }
        else:
            edit_costs = self.EDIT_COSTS

        ops = self._dp_align(seq_a, seq_b, edit_costs)
        ged_score = sum(edit_costs[op.op_type] for op in ops)
        max_len = max(len(seq_a), len(seq_b), 1)
        gaps = self._classify_gaps(ops)

        return {
            "ged_score": round(ged_score, 4),
            "normalized_ged": round(ged_score / max_len, 4),
            "edit_operations": [vars(op) for op in ops],
            "reasoning_gaps": gaps,
            "graph_a_sequence": seq_a,
            "graph_b_sequence": seq_b,
            "relabel_cost": edit_costs["relabel"] if self.config.use_enhancements else None,
        }

    def _dp_align(self, seq_a, seq_b, edit_costs) -> list[EditOperation]:
        """Classic DP alignment, extended to produce labelled edit operations."""
        m, n = len(seq_a), len(seq_b)
        dp = [[(0.0, "no_op")] * (n + 1) for _ in range(m + 1)]

        for i in range(1, m + 1):
            dp[i][0] = (i * edit_costs["delete"], "delete")
        for j in range(1, n + 1):
            dp[0][j] = (j * edit_costs["insert"], "insert")

        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if seq_a[i - 1] == seq_b[j - 1]:
                    dp[i][j] = (dp[i - 1][j - 1][0], "no_op")
                else:
                    candidates = [
                        (dp[i - 1][j][0] + edit_costs["delete"], "delete"),
                        (dp[i][j - 1][0] + edit_costs["insert"], "insert"),
                        (dp[i - 1][j - 1][0] + edit_costs["relabel"], "relabel"),
                    ]
                    dp[i][j] = min(candidates, key=lambda x: x[0])

        ops: list[EditOperation] = []
        i, j = m, n
        while i > 0 or j > 0:
            _, op_type = dp[i][j]
            if op_type == "no_op" and i > 0 and j > 0:
                i -= 1; j -= 1
            elif op_type == "relabel" and i > 0 and j > 0:
                actual = seq_a[i - 1]
                ideal = seq_b[j - 1]
                gap_type = self.CRITICAL_SUBSTITUTIONS.get(
                    (actual, ideal), "suboptimal_substitution"
                )
                ops.append(EditOperation(
                    op_type="relabel",
                    step_index=i,
                    actual_action=actual,
                    ideal_action=ideal,
                    cost=edit_costs["relabel"],
                    gap_type=gap_type,
                    explanation=f"Step {i}: agent chose '{actual}', ideal path requires '{ideal}' — {gap_type}",
                ))
                i -= 1; j -= 1
            elif op_type == "delete" and i > 0:
                ops.append(EditOperation(
                    op_type="delete",
                    step_index=i,
                    actual_action=seq_a[i - 1],
                    cost=edit_costs["delete"],
                    gap_type="spurious_action",
                    explanation=f"Step {i}: agent performed '{seq_a[i-1]}' which is absent from the ideal path",
                ))
                i -= 1
            else:
                ops.append(EditOperation(
                    op_type="insert",
                    step_index=j,
                    ideal_action=seq_b[j - 1],
                    cost=edit_costs["insert"],
                    gap_type="missing_action",
                    explanation=f"Ideal step {j}: '{seq_b[j-1]}' was required but agent omitted it",
                ))
                j -= 1

        ops.reverse()
        return [op for op in ops if op.op_type != "no_op"]

    def _classify_gaps(self, ops: list[EditOperation]) -> list[dict]:
        gaps = []
        for op in ops:
            severity = "critical" if op.gap_type in self.CRITICAL_SUBSTITUTIONS.values() else "warning"
            # Enhanced: adjust severity using RLHF (human feedback)
            if self.config.use_enhancements and self.config.human_feedback_score > 0.7:
                # Human trusts agent more, downgrade critical to warning
                if severity == "critical":
                    severity = "warning"
            elif self.config.use_enhancements and self.config.human_feedback_score < 0.3:
                # Human distrusts agent, upgrade warnings to critical
                if severity == "warning":
                    severity = "critical"
            gaps.append({
                "gap_type": op.gap_type,
                "severity": severity,
                "actual_action": op.actual_action,
                "ideal_action": op.ideal_action,
                "step_index": op.step_index,
                "explanation": op.explanation,
            })
        return gaps

    # ------------------------------------------------------------------
    # Full evaluation entry point
    # ------------------------------------------------------------------

    def evaluate(self, graph_a: DecisionGraph, task_type: str,
                 graph_b: Optional[DecisionGraph] = None) -> dict:
        """Full dual-graph evaluation with optional enhancements."""
        if graph_b is None:
            graph_b = self._ideal_store.get(task_type)
        if graph_b is None:
            return {"status": "no_ideal_path",
                    "message": f"No ideal path registered for task_type='{task_type}'."}

        ged = self.compute_ged(graph_a, graph_b)
        xai = self._build_xai(ged, graph_a, graph_b)
        verdict = self._verdict(ged["normalized_ged"], ged["reasoning_gaps"])
        corrections = self._corrective_actions(ged["reasoning_gaps"])

        result = {
            "status": "evaluated",
            "task_type": task_type,
            "graph_a_sequence": ged["graph_a_sequence"],
            "graph_b_sequence": ged["graph_b_sequence"],
            "ged_score": ged["ged_score"],
            "normalized_ged": ged["normalized_ged"],
            "reasoning_gaps": ged["reasoning_gaps"],
            "edit_operations": ged["edit_operations"],
            "xai_explanation": xai,
            "verdict": verdict,
            "corrective_actions": corrections,
        }

        # Enhanced: compute MODP reward and update distillation
        if self.config.use_enhancements and self.distillation_optimizer:
            # Calculate a composite reward: higher if normalized_ged is low and no critical gaps
            norm_ged = ged["normalized_ged"]
            critical_count = sum(1 for g in ged["reasoning_gaps"] if g["severity"] == "critical")
            quality = 1.0 - norm_ged
            energy = 0.5  # dummy; would be actual energy from agent run
            latency = 0.5
            carbon = 0.5
            # MODP reward
            weights = self.config.modp_weights
            reward = (weights[0] * (1.0 - energy) +   # energy lower is better
                      weights[1] * (1.0 - latency) +  # latency lower is better
                      weights[2] * (1.0 - carbon) +   # carbon lower is better
                      weights[3] * quality)           # quality higher is better
            # Clamp to [-1, 1] for update signal
            reward = max(-1.0, min(1.0, reward))

            # Update distillation optimizer with state features and reward
            seq_a = ged["graph_a_sequence"]
            seq_b = ged["graph_b_sequence"]
            state_vec = np.array([
                min(len(seq_a) / 10.0, 1.0),
                min(len(seq_b) / 10.0, 1.0),
                min(self.config.graph_metrics.get('centrality', 0.5), 1.0)
            ], dtype=np.float32)
            self.distillation_optimizer.update(state_vec, reward)

            result["modp_reward"] = reward
            result["relabel_cost"] = ged.get("relabel_cost")

        return result

    # ------------------------------------------------------------------
    # XAI output builder
    # ------------------------------------------------------------------

    def _build_xai(self, ged: dict, graph_a: DecisionGraph, graph_b: DecisionGraph) -> str:
        gaps = ged["reasoning_gaps"]
        if not gaps:
            return f"Agent path {ged['graph_a_sequence']} matches ideal path exactly. No reasoning gap. GED = 0."

        critical = [g for g in gaps if g["severity"] == "critical"]
        warnings  = [g for g in gaps if g["severity"] == "warning"]

        lines = [
            f"GED={ged['ged_score']} (normalised={ged['normalized_ged']}) "
            f"| {len(gaps)} gap(s): {len(critical)} critical, {len(warnings)} warning(s)"
        ]
        lines.append(f"Actual path:  {' → '.join(ged['graph_a_sequence'])}")
        lines.append(f"Ideal path:   {' → '.join(ged['graph_b_sequence'])}")

        for g in critical:
            lines.append(f"  [CRITICAL] {g['explanation']}")
        for w in warnings:
            lines.append(f"  [WARNING]  {w['explanation']}")
        return "\n".join(lines)

    def _verdict(self, norm_ged: float, gaps: list[dict]) -> str:
        if norm_ged == 0.0:
            return "optimal"
        critical_count = sum(1 for g in gaps if g["severity"] == "critical")
        if norm_ged <= 0.20 and critical_count == 0:
            return "near_optimal"
        if norm_ged <= 0.50 or critical_count == 1:
            return "suboptimal"
        return "reasoning_failure"

    def _corrective_actions(self, gaps: list[dict]) -> list[str]:
        corrections = []
        for g in gaps:
            gap = g.get("gap_type", "")
            if gap == "unnecessary_deferral":
                corrections.append(
                    "Re-evaluate defer threshold — grid strain signal may be false positive. "
                    "Check CausalGraph confidence on GridStrain → DeferralSignal edge."
                )
            elif gap == "failed_to_defer":
                corrections.append(
                    "Task should have deferred. Increase CarbonLevel:high → Decision:defer weight "
                    "in PolicyGraph."
                )
            elif gap == "wrong_optimization_strategy":
                corrections.append(
                    "Wrong compression applied. Verify ModelOptimizer selects strategy based "
                    "on task type, not global default."
                )
            elif gap == "over_throttled":
                corrections.append(
                    "Model over-throttled for this priority level. Decrease "
                    "Zone:yellow → Decision:throttle weight."
                )
            elif gap == "missing_action":
                corrections.append(
                    f"Agent skipped required action '{g.get('ideal_action')}'. "
                    "Verify PolicyGraph traversal depth is sufficient."
                )
        return list(dict.fromkeys(corrections))
