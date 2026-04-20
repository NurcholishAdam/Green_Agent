"""
Phase 3 — DAG Carbon Ledger
=============================
Replaces the sequential immutable CarbonLedger with a Directed Acyclic Graph.

Why DAG instead of a flat list?
  In the flat ledger each entry is independent. There is no way to trace that
  Task C consumed high energy because Task B left the GPU in a non-quantized
  state, which itself happened because Task A triggered a full model reload.

  In the DAG, parent→child edges encode those causal dependencies explicitly.
  carbon_backpropagate() then propagates a node's carbon cost upstream so
  every ancestor accumulates an "inherited carbon debt" — analogous to
  backpropagation through a compute graph, but for CO2 accounting.

Integration:
  • Call add_execution() wherever the old CarbonLedger.log() was called.
  • Pass parent_task_ids to link causally dependent executions.
  • Call backpropagate_carbon() after a run to update ancestor debts.
  • get_lineage() replaces a flat ledger query for audit purposes.

Priority: THIRD — foundational change that Phases 4 and 5 depend on.
"""

import json
import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class CarbonNode:
    node_id: str
    task_id: str
    framework: str
    energy_kwh: float
    carbon_co2e_kg: float
    accuracy: float
    sustainability_index: float
    timestamp: str
    parent_ids: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    # Set by backpropagation — zero until backpropagate_carbon() is called
    inherited_carbon_debt: float = 0.0


@dataclass
class CarbonEdge:
    source_id: str          # parent (caused or enabled the child execution)
    target_id: str          # child (the dependent execution)
    dependency_type: str    # "sequential" | "model_state" | "resource_contention"
    carbon_transfer: float = 0.0  # populated by backpropagation


# ---------------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------------

class DAGCarbonLedger:
    """
    Directed Acyclic Graph carbon ledger with upstream debt propagation.

    Storage: two JSON files (nodes.json, edges.json) under storage_path.
    Both files are reloaded on construction and persisted after every mutation.

    Thread-safety: single-threaded writes assumed (no lock). Add a threading.Lock
    if BenchmarkHarness uses concurrent workers.
    """

    def __init__(self, storage_path: str = "./dag_ledger"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.nodes: dict[str, CarbonNode] = {}
        self.edges: list[CarbonEdge] = []
        self._children: dict[str, list[str]] = {}  # node_id → [child_ids]
        self._parents: dict[str, list[str]] = {}   # node_id → [parent_ids]
        self._load()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _nodes_path(self) -> Path:
        return self.storage_path / "nodes.json"

    def _edges_path(self) -> Path:
        return self.storage_path / "edges.json"

    def _load(self):
        if self._nodes_path().exists():
            with open(self._nodes_path()) as f:
                raw = json.load(f)
            for nid, nd in raw.items():
                self.nodes[nid] = CarbonNode(**nd)

        if self._edges_path().exists():
            with open(self._edges_path()) as f:
                for ed in json.load(f):
                    edge = CarbonEdge(**ed)
                    self.edges.append(edge)
                    self._link(edge)

    def _persist(self):
        with open(self._nodes_path(), "w") as f:
            json.dump({nid: vars(nd) for nid, nd in self.nodes.items()}, f, indent=2)
        with open(self._edges_path(), "w") as f:
            json.dump([vars(e) for e in self.edges], f, indent=2)

    def _link(self, edge: CarbonEdge):
        self._children.setdefault(edge.source_id, []).append(edge.target_id)
        self._parents.setdefault(edge.target_id, []).append(edge.source_id)

    # ------------------------------------------------------------------
    # Write API
    # ------------------------------------------------------------------

    def add_execution(
        self,
        task_id: str,
        framework: str,
        energy_kwh: float,
        carbon_co2e_kg: float,
        accuracy: float,
        sustainability_index: float,
        parent_task_ids: Optional[list[str]] = None,
        dependency_type: str = "sequential",
        metadata: Optional[dict] = None,
    ) -> str:
        """
        Record a new task execution.

        parent_task_ids  — list of node_ids that causally preceded this execution.
                           Leave empty for root tasks (no dependency).
        dependency_type  — how the parent caused/enabled this node:
                           "sequential"          simply ran before
                           "model_state"         parent left model in a state this task inherits
                           "resource_contention" parent consumed resources this task needed

        Returns the new node_id (use it as parent_task_id for downstream tasks).
        """
        node_id = str(uuid.uuid4())
        node = CarbonNode(
            node_id=node_id,
            task_id=task_id,
            framework=framework,
            energy_kwh=energy_kwh,
            carbon_co2e_kg=carbon_co2e_kg,
            accuracy=accuracy,
            sustainability_index=sustainability_index,
            timestamp=datetime.now(timezone.utc).isoformat(),
            parent_ids=list(parent_task_ids or []),
            metadata=metadata or {},
        )
        self.nodes[node_id] = node

        for parent_id in (parent_task_ids or []):
            if parent_id in self.nodes:
                edge = CarbonEdge(
                    source_id=parent_id,
                    target_id=node_id,
                    dependency_type=dependency_type,
                )
                self.edges.append(edge)
                self._link(edge)

        self._persist()
        return node_id

    # ------------------------------------------------------------------
    # Carbon backpropagation
    # ------------------------------------------------------------------

    def backpropagate_carbon(
        self, node_id: str, transfer_rate: float = 0.30
    ) -> dict[str, float]:
        """
        Propagate a node's carbon cost upstream through its ancestor chain.

        Each parent receives transfer_rate × (carbon reaching the child).
        This recurses so grandparents receive transfer_rate² × cost, etc.

        transfer_rate = 0.30 means 30% of each node's carbon is attributed
        to its direct parents, 9% to grandparents, 2.7% to great-grandparents.

        Returns  {node_id: attributed_carbon}  for every affected ancestor.

        Effect: self.nodes[ancestor].inherited_carbon_debt is updated in-place.
        """
        if node_id not in self.nodes:
            return {}

        attributed: dict[str, float] = {node_id: self.nodes[node_id].carbon_co2e_kg}
        queue = [(node_id, self.nodes[node_id].carbon_co2e_kg)]

        while queue:
            current_id, carbon_amount = queue.pop(0)
            transferred = carbon_amount * transfer_rate
            if transferred < 1e-9:
                continue  # below floating-point noise floor — stop propagating
            for parent_id in self._parents.get(current_id, []):
                if parent_id not in self.nodes:
                    continue
                self.nodes[parent_id].inherited_carbon_debt += transferred
                attributed[parent_id] = attributed.get(parent_id, 0.0) + transferred
                queue.append((parent_id, transferred))

        self._persist()
        return {k: round(v, 8) for k, v in attributed.items()}

    # ------------------------------------------------------------------
    # Read API
    # ------------------------------------------------------------------

    def get_lineage(self, node_id: str) -> list[dict]:
        """
        Return the full ancestor chain ordered from root → node_id.
        Each entry includes direct + inherited carbon totals for attribution.
        """
        if node_id not in self.nodes:
            return []

        chain: list[dict] = []
        visited: set[str] = set()
        stack = [node_id]

        while stack:
            current = stack.pop()
            if current in visited:
                continue
            visited.add(current)
            n = self.nodes[current]
            chain.append({
                "node_id": current,
                "task_id": n.task_id,
                "framework": n.framework,
                "direct_carbon_co2e_kg": round(n.carbon_co2e_kg, 8),
                "inherited_carbon_debt": round(n.inherited_carbon_debt, 8),
                "total_attributed_carbon": round(
                    n.carbon_co2e_kg + n.inherited_carbon_debt, 8
                ),
                "energy_kwh": n.energy_kwh,
                "accuracy": n.accuracy,
                "sustainability_index": n.sustainability_index,
                "timestamp": n.timestamp,
                "dependency_type": next(
                    (e.dependency_type for e in self.edges
                     if e.target_id == current), "root",
                ),
            })
            for parent_id in self._parents.get(current, []):
                stack.append(parent_id)

        chain.reverse()  # root first
        return chain

    def get_summary(self) -> dict:
        """High-level ledger statistics — drop-in replacement for old ledger summary()."""
        if not self.nodes:
            return {"total_executions": 0}

        total_carbon = sum(n.carbon_co2e_kg for n in self.nodes.values())
        total_energy = sum(n.energy_kwh for n in self.nodes.values())
        total_inherited = sum(n.inherited_carbon_debt for n in self.nodes.values())

        by_framework: dict[str, float] = {}
        by_dep_type: dict[str, int] = {}
        for n in self.nodes.values():
            by_framework[n.framework] = (
                by_framework.get(n.framework, 0.0) + n.carbon_co2e_kg
            )
        for e in self.edges:
            by_dep_type[e.dependency_type] = by_dep_type.get(e.dependency_type, 0) + 1

        avg_si = (
            sum(n.sustainability_index for n in self.nodes.values()) / len(self.nodes)
        )
        return {
            "total_executions": len(self.nodes),
            "dag_edges": len(self.edges),
            "total_carbon_co2e_kg": round(total_carbon, 6),
            "total_inherited_carbon_co2e_kg": round(total_inherited, 6),
            "total_energy_kwh": round(total_energy, 6),
            "average_sustainability_index": round(avg_si, 4),
            "carbon_by_framework": {k: round(v, 6) for k, v in by_framework.items()},
            "edges_by_dependency_type": by_dep_type,
        }

    def find_high_debt_nodes(self, top_k: int = 5) -> list[dict]:
        """
        Return the top-K nodes ranked by total attributed carbon
        (direct + inherited). Useful for surfacing the most carbon-expensive
        execution chains in the leaderboard dashboard.
        """
        ranked = sorted(
            self.nodes.values(),
            key=lambda n: n.carbon_co2e_kg + n.inherited_carbon_debt,
            reverse=True,
        )
        return [
            {
                "node_id": n.node_id,
                "task_id": n.task_id,
                "framework": n.framework,
                "total_attributed_carbon": round(
                    n.carbon_co2e_kg + n.inherited_carbon_debt, 8
                ),
            }
            for n in ranked[:top_k]
        ]
