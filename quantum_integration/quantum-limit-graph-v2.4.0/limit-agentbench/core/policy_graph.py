"""
Phase 4 — Policy Graph Engine
================================
Encodes green_policy.yaml rules as a weighted directed graph and
performs multi-hop, context-aware traversal to produce decisions.

Replaces the flat conditional logic:
    if carbon > 400: defer
with a graph traversal that reasons:
    Task(low-priority) + Battery(low) + Carbon(390) → Defer
    Task(low-priority) + Battery(high) + Carbon(390) → Execute

Each context variable activates one or more policy nodes. The traversal
follows weighted edges from those nodes to decision leaf nodes. The path
with the highest cumulative weight is the winning decision.

Edge weights start from DEFAULT_POLICY priors and are updated online via
update_edge_weight() when benchmark outcomes provide feedback.

Priority: FOURTH — requires accumulated data from Phase 3 DAG ledger
to deliver meaningful online learning.
"""

from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class PolicyNode:
    node_id: str
    variable: str           # e.g. "CarbonLevel", "BatteryState", "TaskPriority"
    condition: str          # e.g. "high", "low", "critical", "green"
    is_decision: bool = False
    decision: Optional[str] = None   # "execute" | "defer" | "throttle"  (leaf only)


@dataclass
class PolicyEdge:
    source_id: str
    target_id: str
    weight: float = 1.0    # positive = promotes target; negative = suppresses
    context_tag: str = ""  # human-readable annotation for XAI reasoning trace


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------

class PolicyGraph:
    """
    Multi-hop weighted policy graph.

    Graph traversal algorithm:
      1. Map observable context to a set of active source nodes.
      2. DFS from each active source with weight accumulation.
      3. When a Decision leaf is reached, record cumulative weight.
      4. Return the Decision with the highest aggregated weight,
         along with the full path and a natural-language reasoning string.
    """

    # (source_id, target_id, weight, context_tag)
    DEFAULT_POLICY: list[tuple] = [
        # === Carbon level → decisions ===
        ("CarbonLevel:critical",  "Decision:defer",    0.95, "carbon_critical_override"),
        ("CarbonLevel:high",      "Decision:defer",    0.70, "carbon_high_primary"),
        ("CarbonLevel:high",      "Decision:throttle", 0.30, "carbon_high_alternative"),
        ("CarbonLevel:medium",    "Decision:execute",  0.60, "carbon_medium_neutral"),
        ("CarbonLevel:low",       "Decision:execute",  0.90, "carbon_low_favorable"),
        # === Battery modifiers ===
        ("BatteryState:low",      "Decision:defer",    0.50, "battery_low_pushes_defer"),
        ("BatteryState:low",      "CarbonLevel:high",  0.40, "battery_low_amplifies_carbon"),
        ("BatteryState:high",     "Decision:execute",  0.60, "battery_high_enables_execute"),
        ("BatteryState:high",     "Decision:defer",   -0.35, "battery_high_suppresses_defer"),
        # === Task priority overrides ===
        ("TaskPriority:critical", "Decision:execute",  0.85, "priority_critical_override"),
        ("TaskPriority:critical", "Decision:defer",   -0.70, "priority_critical_veto_defer"),
        ("TaskPriority:low",      "Decision:defer",    0.45, "priority_low_nudges_defer"),
        ("TaskPriority:low",      "Decision:throttle", 0.40, "priority_low_throttle"),
        # === Zone enforcement ===
        ("Zone:red",              "Decision:defer",    0.80, "zone_red_enforces_defer"),
        ("Zone:red",              "Decision:execute", -0.60, "zone_red_vetoes_execute"),
        ("Zone:yellow",           "Decision:throttle", 0.65, "zone_yellow_caution"),
        ("Zone:green",            "Decision:execute",  0.75, "zone_green_permits_execute"),
        # === Queue depth ===
        ("QueueDepth:high",       "Decision:throttle", 0.55, "queue_high_throttle"),
        ("QueueDepth:high",       "Decision:defer",    0.35, "queue_high_defer"),
        ("QueueDepth:low",        "Decision:execute",  0.40, "queue_low_execute"),
    ]

    DECISION_IDS: set[str] = {"Decision:execute", "Decision:defer", "Decision:throttle"}

    def __init__(self, policy_yaml_path: Optional[str] = None):
        self.nodes: dict[str, PolicyNode] = {}
        self.edges: list[PolicyEdge] = []
        self._adj: dict[str, list[tuple[str, float, str]]] = {}
        self._build_default()
        if policy_yaml_path:
            self._load_yaml(policy_yaml_path)

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def _build_default(self):
        all_ids: set[str] = set()
        for src, tgt, _, _ in self.DEFAULT_POLICY:
            all_ids.add(src)
            all_ids.add(tgt)

        for nid in all_ids:
            parts = nid.split(":", 1)
            variable = parts[0]
            condition = parts[1] if len(parts) > 1 else ""
            is_dec = nid in self.DECISION_IDS
            self.nodes[nid] = PolicyNode(
                node_id=nid,
                variable=variable,
                condition=condition,
                is_decision=is_dec,
                decision=condition if is_dec else None,
            )

        for src, tgt, weight, tag in self.DEFAULT_POLICY:
            self._add_edge(PolicyEdge(source_id=src, target_id=tgt,
                                      weight=weight, context_tag=tag))

    def _add_edge(self, edge: PolicyEdge):
        self.edges.append(edge)
        self._adj.setdefault(edge.source_id, []).append(
            (edge.target_id, edge.weight, edge.context_tag)
        )

    def _load_yaml(self, path: str):
        """
        Extend the policy graph from a green_policy.yaml file.

        Expected YAML structure:
          rules:
            - variable: CarbonLevel
              condition: high
              action: defer
              weight: 0.80
            - variable: TaskPriority
              condition: critical
              action: execute
              weight: 0.90
        """
        try:
            import yaml
        except ImportError:
            return  # PyYAML optional; default policy always available

        try:
            with open(path) as f:
                policy = yaml.safe_load(f)
            for rule in policy.get("rules", []):
                src = f"{rule['variable']}:{rule['condition']}"
                tgt = f"Decision:{rule['action']}"
                weight = float(rule.get("weight", 0.70))
                tag = rule.get("tag", "yaml_rule")
                # Ensure nodes exist
                for nid in (src, tgt):
                    if nid not in self.nodes:
                        parts = nid.split(":", 1)
                        self.nodes[nid] = PolicyNode(
                            node_id=nid,
                            variable=parts[0],
                            condition=parts[1] if len(parts) > 1 else "",
                            is_decision=(nid in self.DECISION_IDS),
                            decision=parts[1] if nid in self.DECISION_IDS else None,
                        )
                self._add_edge(PolicyEdge(source_id=src, target_id=tgt,
                                          weight=weight, context_tag=tag))
        except (FileNotFoundError, KeyError, TypeError):
            pass  # silently skip malformed YAML

    # ------------------------------------------------------------------
    # Decision traversal
    # ------------------------------------------------------------------

    def decide(self, context: dict) -> dict:
        """
        Multi-hop weighted traversal from observable context to a decision.

        context = {
            "carbon_g_per_kwh": 390,    # float
            "battery_pct": 0.85,         # 0.0 – 1.0
            "task_priority": 0.3,        # 0.0 (low) – 1.0 (critical)
            "zone": "yellow",            # "green" | "yellow" | "red"
            "queue_depth": 30,           # int
        }

        Returns:
        {
            "decision": "execute" | "defer" | "throttle",
            "score": float,
            "all_scores": { "execute": ..., "defer": ..., "throttle": ... },
            "active_nodes": [...],
            "winning_path": [...],
            "reasoning": "natural-language explanation",
        }
        """
        active = self._resolve_active_nodes(context)
        scores: dict[str, float] = {d: 0.0 for d in self.DECISION_IDS}
        best_paths: dict[str, list[str]] = {d: [] for d in self.DECISION_IDS}
        best_tags: dict[str, list[str]] = {d: [] for d in self.DECISION_IDS}

        for start in active:
            self._dfs(
                current=start,
                path=[start],
                tags=[],
                weight=1.0,
                scores=scores,
                best_paths=best_paths,
                best_tags=best_tags,
                visited=set(),
                depth=0,
                max_depth=6,
            )

        winner = max(scores, key=scores.get)
        return {
            "decision": winner.split(":")[1],
            "score": round(scores[winner], 4),
            "all_scores": {
                k.split(":")[1]: round(v, 4) for k, v in scores.items()
            },
            "active_nodes": active,
            "winning_path": best_paths[winner],
            "winning_tags": best_tags[winner],
            "reasoning": self._reasoning(
                context, winner, best_paths[winner], best_tags[winner]
            ),
        }

    def _dfs(self, current: str, path: list, tags: list,
             weight: float, scores: dict, best_paths: dict, best_tags: dict,
             visited: set, depth: int, max_depth: int):
        if depth > max_depth or current in visited:
            return
        visited = visited | {current}

        if current in self.DECISION_IDS:
            if weight > scores[current]:
                scores[current] = weight
                best_paths[current] = list(path)
                best_tags[current] = list(tags)
            return

        for target, edge_w, tag in self._adj.get(current, []):
            new_w = weight * edge_w if edge_w > 0 else 0.0  # ignore suppressing for now
            if new_w > 0.01:  # prune negligible paths
                self._dfs(
                    target, path + [target], tags + [tag],
                    new_w, scores, best_paths, best_tags,
                    visited, depth + 1, max_depth,
                )

    def _resolve_active_nodes(self, context: dict) -> list[str]:
        """Map numeric context values → active PolicyNode IDs."""
        active: list[str] = []

        carbon = context.get("carbon_g_per_kwh", 200.0)
        if carbon >= 500:
            active.append("CarbonLevel:critical")
        elif carbon >= 400:
            active.append("CarbonLevel:high")
        elif carbon >= 250:
            active.append("CarbonLevel:medium")
        else:
            active.append("CarbonLevel:low")

        battery = context.get("battery_pct", 1.0)
        active.append("BatteryState:low" if battery < 0.40 else "BatteryState:high")

        priority = context.get("task_priority", 0.5)
        if priority >= 0.80:
            active.append("TaskPriority:critical")
        elif priority <= 0.30:
            active.append("TaskPriority:low")

        zone = context.get("zone", "green")
        active.append(f"Zone:{zone}")

        queue = context.get("queue_depth", 0)
        if queue > 80:
            active.append("QueueDepth:high")
        elif queue < 20:
            active.append("QueueDepth:low")

        return [n for n in active if n in self.nodes]

    def _reasoning(self, context: dict, winner: str,
                   path: list, tags: list) -> str:
        path_readable = " → ".join(n.split(":")[-1] for n in path)
        return (
            f"Context: carbon={context.get('carbon_g_per_kwh','?')}g/kWh, "
            f"battery={int(context.get('battery_pct', 1) * 100)}%, "
            f"priority={context.get('task_priority','?')}, "
            f"zone={context.get('zone','?')} | "
            f"Decision path: {path_readable}"
        )

    # ------------------------------------------------------------------
    # Online learning
    # ------------------------------------------------------------------

    def update_edge_weight(self, source_id: str, target_id: str,
                           decision_was_correct: bool,
                           learning_rate: float = 0.05):
        """
        Adjust a policy edge weight based on outcome feedback.
        Call after each benchmark result with the winning edge pair.
        """
        delta = learning_rate if decision_was_correct else -learning_rate
        for edge in self.edges:
            if edge.source_id == source_id and edge.target_id == target_id:
                edge.weight = max(-1.0, min(1.0, edge.weight + delta))
                # Sync adjacency list
                adj = self._adj.get(source_id, [])
                for i, (tgt, w, tag) in enumerate(adj):
                    if tgt == target_id:
                        adj[i] = (tgt, edge.weight, tag)

    def export_weights(self) -> list[dict]:
        """Export all edge weights for persistence or dashboard display."""
        return [
            {
                "source": e.source_id,
                "target": e.target_id,
                "weight": round(e.weight, 4),
                "context_tag": e.context_tag,
            }
            for e in self.edges
        ]
