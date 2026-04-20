"""
Phase 5 — Dual-Graph AI Evaluator
====================================
Compares Graph A (actual agent decision path) against Graph B (ideal/
counterfactual path) using an approximate Graph Edit Distance (GED)
algorithm. Produces an Explainable AI (XAI) evaluation report that
explains WHY a decision path was suboptimal, not just THAT it was.

Key concept:
  GED = minimum number of node insertions + deletions + relabelings
        needed to transform Graph A into Graph B.
  GED = 0   → agent followed the optimal path exactly
  GED = N   → N decision nodes differ; each is labelled with a gap type

The beam-search DP approximation used here converges to optimal
for paths of length ≤ 8 (typical for agentic task chains) in O(m×n) time.

Integration:
  1. Build a DecisionGraph for each agent run by calling add_step().
  2. Register ideal paths via register_ideal_path() (human-annotated for
     canonical task types, or synthetic from PolicyGraph.decide() otherwise).
  3. Call evaluate(graph_a, task_type) to get the full GED + XAI report.
  4. Feed the verdict back into MetaCognitionLayer.feedback() and
     PolicyGraph.update_edge_weight() for online learning.

Priority: FIFTH — requires Phases 1–4 for meaningful ideal path generation.
"""

from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Decision graph primitives
# ---------------------------------------------------------------------------

@dataclass
class DecisionNode:
    node_id: str
    action: str             # e.g. "quantize" | "defer" | "execute" | "throttle" | "prune"
    context_snapshot: dict = field(default_factory=dict)  # state when decision was made
    outcome_delta_si: float = 0.0   # Δ sustainability_index from this step (signed)


@dataclass
class DecisionEdge:
    source_id: str
    target_id: str
    transition_type: str = "followed_by"
    weight: float = 1.0


class DecisionGraph:
    """
    Represents a single agent execution as a sequence of labelled decision nodes.
    Supports both sequential (linear) and branching decision paths.
    """

    def __init__(self, graph_id: str, graph_type: str = "executor"):
        self.graph_id = graph_id
        self.graph_type = graph_type  # "executor" | "evaluator"
        self.nodes: dict[str, DecisionNode] = {}
        self.edges: list[DecisionEdge] = []
        self._sequence: list[str] = []

    def add_step(self, node: DecisionNode, after_id: Optional[str] = None):
        """Append a decision step. after_id links it to its predecessor."""
        self.nodes[node.node_id] = node
        self._sequence.append(node.node_id)
        if after_id and after_id in self.nodes:
            self.edges.append(DecisionEdge(
                source_id=after_id,
                target_id=node.node_id,
            ))

    def get_action_sequence(self) -> list[str]:
        """Return the ordered list of action labels for GED comparison."""
        return [self.nodes[nid].action
                for nid in self._sequence if nid in self.nodes]

    def total_outcome_delta(self) -> float:
        return sum(n.outcome_delta_si for n in self.nodes.values())


# ---------------------------------------------------------------------------
# Edit operation
# ---------------------------------------------------------------------------

@dataclass
class EditOperation:
    op_type: str            # "insert" | "delete" | "relabel" | "no_op"
    step_index: int
    actual_action: Optional[str] = None
    ideal_action: Optional[str] = None
    cost: float = 0.0
    gap_type: str = ""
    explanation: str = ""


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------

class DualGraphEvaluator:
    """
    GED-based evaluator with XAI reasoning gap classification.

    Ideal path store: task_type (str) → DecisionGraph (Graph B).
    Populate via register_ideal_path() or synthesise from PolicyGraph.
    """

    EDIT_COSTS = {
        "insert":  1.0,
        "delete":  1.0,
        "relabel": 0.7,
        "no_op":   0.0,
    }

    # (actual_action, ideal_action) → gap classification
    CRITICAL_SUBSTITUTIONS: dict[tuple, str] = {
        ("defer",    "execute"):  "unnecessary_deferral",
        ("execute",  "defer"):    "failed_to_defer",
        ("quantize", "prune"):    "wrong_optimization_strategy",
        ("throttle", "execute"):  "over_throttled",
        ("execute",  "throttle"): "under_throttled",
        ("prune",    "quantize"): "wrong_optimization_strategy",
    }

    def __init__(self):
        self._ideal_store: dict[str, DecisionGraph] = {}

    # ------------------------------------------------------------------
    # Ideal path registry
    # ------------------------------------------------------------------

    def register_ideal_path(self, task_type: str, graph_b: DecisionGraph):
        """
        Register a human-annotated or synthesised ideal path for a task type.

        For canonical benchmark task types, this should be human-annotated.
        For novel task types, synthesise from PolicyGraph.decide() and record
        the winning path as a DecisionGraph.
        """
        self._ideal_store[task_type] = graph_b

    def build_ideal_from_sequence(self, task_type: str,
                                   action_sequence: list[str]) -> DecisionGraph:
        """
        Convenience factory: create Graph B from a plain list of action strings.
        Automatically registers the result.
        """
        graph_b = DecisionGraph(
            graph_id=f"ideal_{task_type}",
            graph_type="evaluator",
        )
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

    def compute_ged(self, graph_a: DecisionGraph,
                    graph_b: DecisionGraph) -> dict:
        """
        Approximate GED via sequence alignment on action labels.

        Produces:
        {
            "ged_score": float,
            "normalized_ged": float (0.0 = optimal, 1.0 = completely wrong),
            "edit_operations": [EditOperation dicts],
            "reasoning_gaps": [gap classification dicts],
            "graph_a_sequence": [...],
            "graph_b_sequence": [...],
        }
        """
        seq_a = graph_a.get_action_sequence()
        seq_b = graph_b.get_action_sequence()

        ops = self._dp_align(seq_a, seq_b)
        ged_score = sum(self.EDIT_COSTS[op.op_type] for op in ops)
        max_len = max(len(seq_a), len(seq_b), 1)

        gaps = self._classify_gaps(ops)
        return {
            "ged_score": round(ged_score, 4),
            "normalized_ged": round(ged_score / max_len, 4),
            "edit_operations": [vars(op) for op in ops],
            "reasoning_gaps": gaps,
            "graph_a_sequence": seq_a,
            "graph_b_sequence": seq_b,
        }

    def _dp_align(self, seq_a: list[str], seq_b: list[str]) -> list[EditOperation]:
        """
        Classic DP sequence alignment, extended to produce labelled edit operations.
        Runs in O(|a| × |b|) time.
        """
        m, n = len(seq_a), len(seq_b)
        # dp[i][j] = (min_cost, operation_type)
        dp = [[(0.0, "no_op")] * (n + 1) for _ in range(m + 1)]

        for i in range(1, m + 1):
            dp[i][0] = (i * self.EDIT_COSTS["delete"], "delete")
        for j in range(1, n + 1):
            dp[0][j] = (j * self.EDIT_COSTS["insert"], "insert")

        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if seq_a[i - 1] == seq_b[j - 1]:
                    dp[i][j] = (dp[i - 1][j - 1][0], "no_op")
                else:
                    candidates = [
                        (dp[i - 1][j][0] + self.EDIT_COSTS["delete"],  "delete"),
                        (dp[i][j - 1][0] + self.EDIT_COSTS["insert"],  "insert"),
                        (dp[i-1][j-1][0] + self.EDIT_COSTS["relabel"], "relabel"),
                    ]
                    dp[i][j] = min(candidates, key=lambda x: x[0])

        # Backtrack
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
                    cost=self.EDIT_COSTS["relabel"],
                    gap_type=gap_type,
                    explanation=(
                        f"Step {i}: agent chose '{actual}', "
                        f"ideal path requires '{ideal}' — {gap_type}"
                    ),
                ))
                i -= 1; j -= 1
            elif op_type == "delete" and i > 0:
                ops.append(EditOperation(
                    op_type="delete",
                    step_index=i,
                    actual_action=seq_a[i - 1],
                    cost=self.EDIT_COSTS["delete"],
                    gap_type="spurious_action",
                    explanation=(
                        f"Step {i}: agent performed '{seq_a[i-1]}' "
                        f"which is absent from the ideal path"
                    ),
                ))
                i -= 1
            else:   # insert
                ops.append(EditOperation(
                    op_type="insert",
                    step_index=j,
                    ideal_action=seq_b[j - 1],
                    cost=self.EDIT_COSTS["insert"],
                    gap_type="missing_action",
                    explanation=(
                        f"Ideal step {j}: '{seq_b[j-1]}' was required "
                        f"but agent omitted it"
                    ),
                ))
                j -= 1

        ops.reverse()
        return [op for op in ops if op.op_type != "no_op"]

    # ------------------------------------------------------------------
    # Gap classification
    # ------------------------------------------------------------------

    def _classify_gaps(self, ops: list[EditOperation]) -> list[dict]:
        gaps = []
        for op in ops:
            severity = (
                "critical" if op.gap_type in self.CRITICAL_SUBSTITUTIONS.values()
                else "warning"
            )
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

    def evaluate(
        self,
        graph_a: DecisionGraph,
        task_type: str,
        graph_b: Optional[DecisionGraph] = None,
    ) -> dict:
        """
        Full dual-graph evaluation.

        Resolves Graph B from the ideal store if not explicitly provided.
        Returns a complete evaluation dict including XAI explanation and verdict.

        {
            "status": "evaluated" | "no_ideal_path",
            "ged_score": float,
            "normalized_ged": float,
            "reasoning_gaps": [...],
            "edit_operations": [...],
            "xai_explanation": str,
            "verdict": "optimal" | "near_optimal" | "suboptimal" | "reasoning_failure",
            "corrective_actions": [...],
        }
        """
        if graph_b is None:
            graph_b = self._ideal_store.get(task_type)
        if graph_b is None:
            return {
                "status": "no_ideal_path",
                "message": (
                    f"No ideal path registered for task_type='{task_type}'. "
                    "Call register_ideal_path() or build_ideal_from_sequence()."
                ),
            }

        ged = self.compute_ged(graph_a, graph_b)
        xai = self._build_xai(ged, graph_a, graph_b)
        verdict = self._verdict(ged["normalized_ged"], ged["reasoning_gaps"])
        corrections = self._corrective_actions(ged["reasoning_gaps"])

        return {
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

    # ------------------------------------------------------------------
    # XAI output builder
    # ------------------------------------------------------------------

    def _build_xai(self, ged: dict, graph_a: DecisionGraph,
                   graph_b: DecisionGraph) -> str:
        gaps = ged["reasoning_gaps"]
        if not gaps:
            return (
                f"Agent path {ged['graph_a_sequence']} matches ideal path exactly. "
                "No reasoning gap. GED = 0."
            )

        critical = [g for g in gaps if g["severity"] == "critical"]
        warnings  = [g for g in gaps if g["severity"] == "warning"]

        lines = [
            f"GED={ged['ged_score']} (normalised={ged['normalized_ged']}) "
            f"| {len(gaps)} gap(s): {len(critical)} critical, {len(warnings)} warning(s)"
        ]
        lines.append(f"Actual path:  {' → '.join(ged['graph_a_sequence'])}")
        lines.append(f"Ideal path:   {' → '.join(ged['graph_b_sequence'])}")

        for g in critical:
            lines.append(
                f"  [CRITICAL] {g['explanation']}"
            )
        for w in warnings:
            lines.append(
                f"  [WARNING]  {w['explanation']}"
            )
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
        return list(dict.fromkeys(corrections))  # deduplicate, preserve order
