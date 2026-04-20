"""
Phase 2 — Causal Graph
========================
Models the Green Agent's runtime environment as a directed causal graph.

Nodes  = measurable system variables (CarbonIntensity, GridStrain, BatteryLevel …)
Edges  = causal relationships with learned weights (cause → effect)

Core capability: backward traversal from an anomalous variable traces the
causal chain to its root, producing an ordered list of "why this happened"
explanations instead of a bare flag.

After every benchmark result, call update_edge_weight() so the graph
progressively learns which causal relationships are strong vs spurious
in the actual deployment environment — the Bayesian update mechanism.

Priority: SECOND — augments interpretability of every metric already collected.
"""

from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class CausalNode:
    node_id: str
    variable: str
    current_value: float = 0.0
    threshold_high: Optional[float] = None
    threshold_low: Optional[float] = None
    anomalous: bool = False
    unit: str = ""          # e.g. "g_CO2/kWh", "pct", "dimensionless"

    def check_anomaly(self) -> bool:
        hi_breach = self.threshold_high is not None and self.current_value > self.threshold_high
        lo_breach = self.threshold_low is not None and self.current_value < self.threshold_low
        self.anomalous = hi_breach or lo_breach
        return self.anomalous


@dataclass
class CausalEdge:
    source_id: str          # cause variable
    target_id: str          # effect variable
    weight: float = 0.7    # 0–1 positive = causes/amplifies; negative = suppresses
    label: str = "causes"  # human-readable relationship label
    confidence: float = 0.5  # prior confidence (increases with evidence)


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------

class CausalGraph:
    """
    Directed causal graph of Green Agent runtime variables.

    Default structure encodes the known causal chain:
      WeatherEvent → RenewableShortfall → GridStrain → CarbonIntensity
      CarbonIntensity → DeferralSignal → ModelThrottleDecision
      BatteryLevel  —suppresses→ DeferralSignal
      TaskPriority  —suppresses→ ModelThrottleDecision

    Weights start from the DEFAULT_STRUCTURE priors and are refined
    by calling update_edge_weight() after each benchmark outcome.
    """

    # (source, target, initial_weight, label)
    DEFAULT_STRUCTURE: list[tuple] = [
        ("WeatherEvent",        "RenewableShortfall",    0.80, "causes"),
        ("RenewableShortfall",  "GridStrain",            0.90, "amplifies"),
        ("GridStrain",          "CarbonIntensity",       0.85, "modulates"),
        ("CarbonIntensity",     "DeferralSignal",        0.75, "triggers"),
        ("DeferralSignal",      "ModelThrottleDecision", 0.70, "triggers"),
        ("BatteryLevel",        "DeferralSignal",       -0.60, "suppresses"),
        ("TaskPriority",        "ModelThrottleDecision",-0.80, "suppresses"),
        ("GridStrain",          "DeferralSignal",        0.65, "reinforces"),
        ("QueueDepth",          "ModelThrottleDecision", 0.50, "modulates"),
        ("ModelThrottleDecision","AccuracyDrop",         0.60, "causes"),
        ("DeferralSignal",      "LatencyIncrease",       0.55, "causes"),
    ]

    DEFAULT_THRESHOLDS: dict[str, dict] = {
        "CarbonIntensity":  {"high": 400.0,   "unit": "g_CO2/kWh"},
        "GridStrain":       {"high": 0.80,    "unit": "ratio"},
        "BatteryLevel":     {"low": 0.25,     "unit": "ratio"},
        "TaskPriority":     {"low": 0.20,     "unit": "ratio"},
        "QueueDepth":       {"high": 100.0,   "unit": "tasks"},
        "AccuracyDrop":     {"high": 0.15,    "unit": "ratio"},
        "LatencyIncrease":  {"high": 500.0,   "unit": "ms"},
    }

    def __init__(self):
        self.nodes: dict[str, CausalNode] = {}
        self.edges: list[CausalEdge] = []
        # Adjacency: target_id → [source_ids]  (for backward traversal)
        self._parents: dict[str, list[str]] = {}
        self._initialize()

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def _initialize(self):
        # Collect all variable names
        variables: set[str] = set()
        for src, tgt, _, _ in self.DEFAULT_STRUCTURE:
            variables.add(src)
            variables.add(tgt)

        for var in variables:
            thresholds = self.DEFAULT_THRESHOLDS.get(var, {})
            self.nodes[var] = CausalNode(
                node_id=var,
                variable=var,
                threshold_high=thresholds.get("high"),
                threshold_low=thresholds.get("low"),
                unit=thresholds.get("unit", ""),
            )

        for src, tgt, weight, label in self.DEFAULT_STRUCTURE:
            self._add_edge(CausalEdge(source_id=src, target_id=tgt,
                                      weight=weight, label=label))

    def _add_edge(self, edge: CausalEdge):
        self.edges.append(edge)
        if edge.target_id not in self._parents:
            self._parents[edge.target_id] = []
        self._parents[edge.target_id].append(edge.source_id)

    # ------------------------------------------------------------------
    # Runtime updates
    # ------------------------------------------------------------------

    def observe(self, variable: str, value: float,
                threshold_high: float = None, threshold_low: float = None):
        """
        Update a variable's current value and run anomaly detection.
        Pass threshold overrides only when the deployment environment
        uses different thresholds than the defaults.
        """
        if variable not in self.nodes:
            # Auto-register unknown variables without threshold
            self.nodes[variable] = CausalNode(node_id=variable, variable=variable)

        node = self.nodes[variable]
        node.current_value = value
        if threshold_high is not None:
            node.threshold_high = threshold_high
        if threshold_low is not None:
            node.threshold_low = threshold_low
        node.check_anomaly()

    def observe_batch(self, snapshot: dict):
        """
        Convenience method for observing an entire state snapshot at once.

        snapshot = {
            "CarbonIntensity": 430,       # value
            "CarbonIntensity_high": 400,  # threshold override (optional)
            "GridStrain": 0.92,
            "BatteryLevel": 0.18,
            "BatteryLevel_low": 0.25,
        }
        """
        for key, val in snapshot.items():
            if key.endswith("_high") or key.endswith("_low"):
                continue
            hi = snapshot.get(f"{key}_high")
            lo = snapshot.get(f"{key}_low")
            self.observe(key, val, hi, lo)

    # ------------------------------------------------------------------
    # Root-cause traversal
    # ------------------------------------------------------------------

    def trace_root_causes(self, anomaly_variable: str,
                          max_depth: int = 5,
                          min_weight: float = 0.2) -> list[dict]:
        """
        Backward BFS from an anomalous node to its root causes.

        Returns a list of causal chains sorted by cumulative edge weight
        (highest first = most likely cause).

        Each chain:
        {
            "root_cause": str,
            "path": [root → ... → anomaly_variable],
            "path_labels": ["causes", "amplifies", ...],
            "cumulative_weight": float,
            "root_is_anomalous": bool,
        }
        """
        if anomaly_variable not in self.nodes:
            return []

        chains: list[dict] = []
        # BFS state: (current_node, path_so_far, label_path, cumulative_weight)
        queue = [(anomaly_variable, [anomaly_variable], [], 1.0)]
        visited_paths: set[tuple] = set()

        while queue:
            current, path, labels, cum_weight = queue.pop(0)

            if len(path) > max_depth:
                continue

            parents = self._parents.get(current, [])

            if not parents:
                # Root node reached
                key = tuple(path)
                if key not in visited_paths:
                    visited_paths.add(key)
                    chains.append({
                        "root_cause": current,
                        "path": list(reversed(path)),
                        "path_labels": list(reversed(labels)),
                        "cumulative_weight": round(cum_weight, 4),
                        "root_is_anomalous": self.nodes.get(
                            current, CausalNode("", "")
                        ).anomalous,
                    })
                continue

            for parent in parents:
                edge_w = self._get_edge_weight(parent, current)
                if abs(edge_w) < min_weight:
                    continue  # prune weak edges
                edge_label = self._get_edge_label(parent, current)
                new_weight = cum_weight * abs(edge_w)
                queue.append((
                    parent,
                    path + [parent],
                    labels + [edge_label],
                    new_weight,
                ))

        chains.sort(key=lambda c: c["cumulative_weight"], reverse=True)
        return chains[:10]  # top-10 causal chains

    def get_anomalies(self) -> list[str]:
        return [nid for nid, node in self.nodes.items() if node.anomalous]

    # ------------------------------------------------------------------
    # Online Bayesian edge weight update
    # ------------------------------------------------------------------

    def update_edge_weight(self, source: str, target: str,
                           outcome_correct: bool,
                           learning_rate: float = 0.05):
        """
        After each benchmark result, call this to refine edge weights.

        If the causal chain correctly predicted the outcome, strengthen edges.
        If the chain was a false positive, weaken them.

        Weight is clamped to [-1.0, 1.0].
        Confidence is tracked separately and modulates learning rate:
        high-confidence edges update more slowly (reduced learning rate).
        """
        for edge in self.edges:
            if edge.source_id == source and edge.target_id == target:
                # High-confidence edges are more resistant to single updates
                effective_lr = learning_rate * (1.0 - 0.5 * edge.confidence)
                delta = effective_lr if outcome_correct else -effective_lr
                edge.weight = max(-1.0, min(1.0, edge.weight + delta))
                # Increase confidence if prediction was correct
                edge.confidence = min(1.0, edge.confidence + 0.02 if outcome_correct
                                      else edge.confidence)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_edge_weight(self, source: str, target: str) -> float:
        for edge in self.edges:
            if edge.source_id == source and edge.target_id == target:
                return edge.weight
        return 0.0

    def _get_edge_label(self, source: str, target: str) -> str:
        for edge in self.edges:
            if edge.source_id == source and edge.target_id == target:
                return edge.label
        return "unknown"

    def export_state(self) -> dict:
        """Serialise the full graph state for logging/debugging."""
        return {
            "nodes": {
                nid: {
                    "value": n.current_value,
                    "anomalous": n.anomalous,
                    "threshold_high": n.threshold_high,
                    "threshold_low": n.threshold_low,
                }
                for nid, n in self.nodes.items()
            },
            "edges": [
                {
                    "source": e.source_id,
                    "target": e.target_id,
                    "weight": round(e.weight, 4),
                    "confidence": round(e.confidence, 4),
                    "label": e.label,
                }
                for e in self.edges
            ],
        }
