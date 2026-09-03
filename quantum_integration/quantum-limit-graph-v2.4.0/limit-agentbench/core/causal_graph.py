"""
Phase 2 — Causal Graph (Enhanced)
==================================
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

Enhancements (optional, enabled via config):
  - LIMIT Graph integration: graph centrality & connectivity metrics influence root-cause ranking.
  - MODP: multi‑objective rewards (carbon, latency, energy) drive edge weight updates.
  - RLHF: human feedback score filters/re‑weights root-cause chains.
  - Multi‑Teacher Distillation + MoE: learned edge‑weight predictor blends rule‑based, historical, and RLHF teachers.
  - Bio‑inspired optimisation: evolutionary search over edge weights.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
import numpy as np
import random
from collections import deque


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


@dataclass
class CausalGraphConfig:
    """Configuration for enhanced CausalGraph."""
    use_enhancements: bool = False
    # LIMIT Graph
    compute_graph_metrics: bool = True
    # MODP weights: [carbon, latency, energy]
    modp_weights: Optional[List[float]] = None   # default [0.4, 0.3, 0.3]
    # RLHF
    human_feedback_score: float = 0.5
    # Distillation + MoE
    use_distillation: bool = False
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

    def __init__(self, config: Optional[CausalGraphConfig] = None):
        self.nodes: dict[str, CausalNode] = {}
        self.edges: list[CausalEdge] = []
        self._parents: dict[str, list[str]] = {}
        self.config = config or CausalGraphConfig()
        self._initialize()

        # Enhanced components
        if self.config.use_enhancements:
            self._init_enhancements()

    def _init_enhancements(self):
        """Initialize enhanced components (distillation, evolutionary, graph metrics)."""
        # MODP weights default
        if self.config.modp_weights is None:
            self.config.modp_weights = [0.4, 0.3, 0.3]
        else:
            total = sum(self.config.modp_weights)
            self.config.modp_weights = [w / total for w in self.config.modp_weights]

        # Distillation + MoE
        if self.config.use_distillation:
            self.distillation_optimizer = EdgeWeightDistillationOptimizer(
                feature_dim=self._get_edge_feature_dim(),
                config=self.config
            )
        else:
            self.distillation_optimizer = None

        # Evolutionary
        if self.config.use_evolutionary:
            self.evolutionary_optimizer = EdgeWeightEvolutionaryOptimizer(
                n_edges=len(self.edges),
                config=self.config
            )
        else:
            self.evolutionary_optimizer = None

    def _get_edge_feature_dim(self) -> int:
        """Feature dimension for edge state (source/target node features)."""
        # We'll use a fixed dim: source_anomaly, target_anomaly, source_value, target_value, graph_centrality, human_feedback, etc.
        return 6

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def _initialize(self):
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
    # LIMIT Graph metrics
    # ------------------------------------------------------------------
    def compute_graph_metrics(self) -> dict:
        """
        Compute simple graph centrality and connectivity metrics from edge weights.
        Returns dict with 'centrality' (avg out-degree weight) and 'connectivity' (density).
        """
        if not self.edges:
            return {'centrality': 0.5, 'connectivity': 0.5}
        total_weight = sum(abs(e.weight) for e in self.edges)
        avg_weight = total_weight / len(self.edges)
        centrality = min(avg_weight, 1.0)
        # Connectivity: actual edges / possible edges (approximate)
        n_nodes = len(self.nodes)
        max_edges = n_nodes * (n_nodes - 1) if n_nodes > 1 else 1
        connectivity = len(self.edges) / max_edges
        return {'centrality': centrality, 'connectivity': min(connectivity, 1.0)}

    # ------------------------------------------------------------------
    # Runtime updates
    # ------------------------------------------------------------------

    def observe(self, variable: str, value: float,
                threshold_high: float = None, threshold_low: float = None):
        if variable not in self.nodes:
            self.nodes[variable] = CausalNode(node_id=variable, variable=variable)

        node = self.nodes[variable]
        node.current_value = value
        if threshold_high is not None:
            node.threshold_high = threshold_high
        if threshold_low is not None:
            node.threshold_low = threshold_low
        node.check_anomaly()

    def observe_batch(self, snapshot: dict):
        for key, val in snapshot.items():
            if key.endswith("_high") or key.endswith("_low"):
                continue
            hi = snapshot.get(f"{key}_high")
            lo = snapshot.get(f"{key}_low")
            self.observe(key, val, hi, lo)

    # ------------------------------------------------------------------
    # Root-cause traversal (with optional enhancement)
    # ------------------------------------------------------------------

    def trace_root_causes(self, anomaly_variable: str,
                          max_depth: int = 5,
                          min_weight: float = 0.2) -> list[dict]:
        """
        Backward BFS from an anomalous node to its root causes.
        If enhancements are enabled, the result may be re‑ranked using
        RLHF and LIMIT Graph metrics.
        """
        if anomaly_variable not in self.nodes:
            return []

        chains: list[dict] = []
        queue = [(anomaly_variable, [anomaly_variable], [], 1.0)]
        visited_paths: set[tuple] = set()

        while queue:
            current, path, labels, cum_weight = queue.pop(0)

            if len(path) > max_depth:
                continue

            parents = self._parents.get(current, [])

            if not parents:
                key = tuple(path)
                if key not in visited_paths:
                    visited_paths.add(key)
                    chains.append({
                        "root_cause": current,
                        "path": list(reversed(path)),
                        "path_labels": list(reversed(labels)),
                        "cumulative_weight": round(cum_weight, 4),
                        "root_is_anomalous": self.nodes.get(current, CausalNode("", "")).anomalous,
                    })
                continue

            for parent in parents:
                edge_w = self._get_edge_weight(parent, current)
                if abs(edge_w) < min_weight:
                    continue
                edge_label = self._get_edge_label(parent, current)
                new_weight = cum_weight * abs(edge_w)
                queue.append((
                    parent,
                    path + [parent],
                    labels + [edge_label],
                    new_weight,
                ))

        chains.sort(key=lambda c: c["cumulative_weight"], reverse=True)

        # Enhanced re-ranking
        if self.config.use_enhancements:
            chains = self._apply_rlhf_and_graph_metrics(chains)

        return chains[:10]

    def _apply_rlhf_and_graph_metrics(self, chains: list[dict]) -> list[dict]:
        """
        Re‑rank chains using human feedback score and graph centrality.
        Higher human feedback boosts chains that lead to nodes with high
        centrality (more influential nodes), but also considers original weight.
        """
        graph_metrics = self.compute_graph_metrics()
        centrality = graph_metrics['centrality']
        human = self.config.human_feedback_score

        def score_chain(chain):
            # Combine cumulative weight, human feedback, and centrality of root cause
            root_node = chain['root_cause']
            root_centrality = centrality  # simplified: global centrality
            # Human feedback: if high (>0.7) prefer shorter paths? We'll just adjust weight
            weight_factor = 1.0 + 0.2 * human * (1.0 - abs(chain['cumulative_weight']))
            return chain['cumulative_weight'] * weight_factor + root_centrality * 0.1

        chains.sort(key=score_chain, reverse=True)
        return chains

    def get_anomalies(self) -> list[str]:
        return [nid for nid, node in self.nodes.items() if node.anomalous]

    # ------------------------------------------------------------------
    # Online Bayesian edge weight update (with MODP / distillation / evolutionary options)
    # ------------------------------------------------------------------

    def update_edge_weight(self, source: str, target: str,
                           outcome_correct: bool,
                           learning_rate: float = 0.05):
        """
        After each benchmark result, call this to refine edge weights.
        If enhanced, may use distillation or evolutionary to propose update.
        """
        if self.config.use_enhancements and self.config.use_distillation:
            # Use distillation to predict update
            self._distillation_update_edge(source, target, outcome_correct, learning_rate)
        elif self.config.use_enhancements and self.config.use_evolutionary:
            # Use evolutionary to adjust weights globally
            self._evolutionary_update_edge(source, target, outcome_correct, learning_rate)
        else:
            # Original Bayesian update
            for edge in self.edges:
                if edge.source_id == source and edge.target_id == target:
                    effective_lr = learning_rate * (1.0 - 0.5 * edge.confidence)
                    delta = effective_lr if outcome_correct else -effective_lr
                    edge.weight = max(-1.0, min(1.0, edge.weight + delta))
                    edge.confidence = min(1.0, edge.confidence + 0.02 if outcome_correct else edge.confidence)

    def _distillation_update_edge(self, source, target, outcome_correct, learning_rate):
        """Use distillation optimizer to propose new edge weight."""
        edge_idx = self._get_edge_index(source, target)
        if edge_idx is None:
            return
        # Build state
        state = self._build_edge_state(edge_idx)
        action = 1 if outcome_correct else 0  # action: 1=strengthen, 0=weaken
        # Predict recommended weight change via distillation
        delta = self.distillation_optimizer.predict_weight_change(state)
        # Apply change
        self.edges[edge_idx].weight = max(-1.0, min(1.0, self.edges[edge_idx].weight + delta))
        # Update teacher/student
        reward = 1.0 if outcome_correct else 0.0
        self.distillation_optimizer.update(state, action, reward)

    def _evolutionary_update_edge(self, source, target, outcome_correct, learning_rate):
        """Use evolutionary optimizer to adjust all edge weights."""
        reward = 1.0 if outcome_correct else -1.0
        self.evolutionary_optimizer.update_fitness(reward)
        # Apply best weights
        best_weights = self.evolutionary_optimizer.get_best_weights()
        for i, edge in enumerate(self.edges):
            edge.weight = best_weights[i]

    def _get_edge_index(self, source, target) -> Optional[int]:
        for i, edge in enumerate(self.edges):
            if edge.source_id == source and edge.target_id == target:
                return i
        return None

    def _build_edge_state(self, edge_idx) -> np.ndarray:
        """Construct feature vector for edge."""
        edge = self.edges[edge_idx]
        src_node = self.nodes[edge.source_id]
        tgt_node = self.nodes[edge.target_id]
        graph_metrics = self.compute_graph_metrics()
        # Features: source anomaly, target anomaly, source value, target value, centrality, human feedback
        return np.array([
            float(src_node.anomalous),
            float(tgt_node.anomalous),
            min(src_node.current_value / 1000.0, 1.0),
            min(tgt_node.current_value / 1000.0, 1.0),
            graph_metrics['centrality'],
            self.config.human_feedback_score,
        ], dtype=np.float32)

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
        state = {
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
        if self.config.use_enhancements:
            state["graph_metrics"] = self.compute_graph_metrics()
            state["human_feedback_score"] = self.config.human_feedback_score
        return state


# ---------------------------------------------------------------------------
# Enhanced components: Distillation, Evolutionary, Teachers
# ---------------------------------------------------------------------------

class EdgeWeightTeacher:
    """Base teacher for edge weight prediction."""
    def predict(self, state: np.ndarray) -> float:
        raise NotImplementedError


class RuleBasedTeacher(EdgeWeightTeacher):
    def predict(self, state: np.ndarray) -> float:
        # state: [src_anom, tgt_anom, src_val, tgt_val, centrality, human]
        if state[0] > 0.5 and state[1] > 0.5:
            return 0.1  # strengthen
        elif state[4] > 0.7:
            return 0.05
        else:
            return -0.05

class RLHFTeacher(EdgeWeightTeacher):
    def predict(self, state: np.ndarray) -> float:
        # human feedback at index 5
        return 0.08 * (state[5] - 0.5)

class HistoricalTeacher(EdgeWeightTeacher):
    def __init__(self):
        self.weights = np.array([0.02, 0.02, -0.01, -0.01, 0.03, 0.01])
    def predict(self, state: np.ndarray) -> float:
        return float(np.dot(self.weights, state))


class EdgeWeightDistillationOptimizer:
    """
    Multi‑teacher distillation with MoE gating to predict edge weight changes.
    """
    def __init__(self, feature_dim: int, config: CausalGraphConfig):
        self.feature_dim = feature_dim
        self.config = config
        self.student_weights = np.zeros(feature_dim)
        self.student_bias = 0.0
        self.lr = config.distillation_lr
        self.epsilon = config.epsilon
        self.teachers = [RuleBasedTeacher(), RLHFTeacher(), HistoricalTeacher()]
        self.gate_weights = np.random.randn(feature_dim, len(self.teachers)) * 0.01
        self.gate_bias = np.zeros(len(self.teachers))
        self.gate_lr = config.gating_lr
        self.replay_buffer = deque(maxlen=config.replay_size)
        self.counter = 0
        self.train_every = config.train_every

    def predict_weight_change(self, state: np.ndarray) -> float:
        # Get teacher predictions
        teacher_preds = np.array([t.predict(state) for t in self.teachers])
        # MoE gating
        logits = state @ self.gate_weights + self.gate_bias
        gate_probs = np.exp(logits - np.max(logits))
        gate_probs /= gate_probs.sum()
        teacher_combined = np.dot(gate_probs, teacher_preds)
        # Student
        student_pred = np.dot(state, self.student_weights) + self.student_bias
        # Blend
        if random.random() < self.epsilon:
            return teacher_combined
        else:
            return 0.8 * student_pred + 0.2 * teacher_combined

    def update(self, state, action, reward):
        self.replay_buffer.append((state, action, reward))
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = random.sample(self.replay_buffer, min(8, len(self.replay_buffer)))
            for s, a, r in batch:
                # Update student (simple gradient on MSE between predicted and reward)
                pred = np.dot(s, self.student_weights) + self.student_bias
                # For simplicity, we update towards a target: +1 if reward>0 else -1
                target = 0.1 if r > 0 else -0.1
                grad = (pred - target) * s
                self.student_weights -= self.lr * grad
                self.student_bias -= self.lr * (pred - target)

                # Update gating (simplified)
                teacher_preds = np.array([t.predict(s) for t in self.teachers])
                gate_probs = self._gate_forward(s)
                combined = np.dot(gate_probs, teacher_preds)
                error = combined - target
                grad_gate = teacher_preds * error
                self.gate_weights -= self.gate_lr * np.outer(s, grad_gate)
                self.gate_bias -= self.gate_lr * grad_gate

    def _gate_forward(self, state):
        logits = state @ self.gate_weights + self.gate_bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()


class EdgeWeightEvolutionaryOptimizer:
    """Evolve all edge weights using genetic algorithm."""
    def __init__(self, n_edges: int, config: CausalGraphConfig):
        self.n_edges = n_edges
        self.config = config
        self.population = [np.random.uniform(-1, 1, n_edges) for _ in range(config.population_size)]
        self.fitness = np.zeros(config.population_size)
        self.best_weights = self.population[0]
        self.best_fitness = 0.0
        self.elitism = config.elitism

    def update_fitness(self, reward: float, index: int = 0):
        self.fitness[index] = reward
        best_idx = np.argmax(self.fitness)
        self.best_weights = self.population[best_idx]
        self.best_fitness = self.fitness[best_idx]
        # Evolve
        sorted_indices = np.argsort(self.fitness)[::-1]
        new_pop = [self.population[i] for i in sorted_indices[:self.elitism]]
        while len(new_pop) < len(self.population):
            p1 = self.population[random.randint(0, len(self.population)-1)]
            p2 = self.population[random.randint(0, len(self.population)-1)]
            if random.random() < self.config.crossover_rate:
                alpha = random.random()
                child = alpha * p1 + (1 - alpha) * p2
            else:
                child = p1.copy()
            child += np.random.normal(0, self.config.mutation_rate, self.n_edges)
            child = np.clip(child, -1, 1)
            new_pop.append(child)
        self.population = new_pop
        self.fitness = np.zeros(len(self.population))

    def get_best_weights(self):
        return self.best_weights
