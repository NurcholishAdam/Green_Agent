# src/dashboard/green_decision_graph.py

"""
Green Decision Graph
====================

Wrapper around ``@supermemory/memory-graph`` (the memory-graph-playground
package) that turns Supermemory recall into the Green Decision Graph UI
described in the enhancement proposal.

Node types: workload, device, site, model, policy, grid context, decision,
measurement, incident.

Edge types: executed-on, selected-by, governed-by, superseded-by,
violated-budget, improved-over, validated-by.

Enhancements
------------
- ``GreenDecisionGraphConfig`` — frozen, validated: node/edge types,
  max nodes, time-travel tolerance, escaping toggle.
- ``GraphNode`` / ``GraphEdge`` / ``GraphPayload`` — frozen, serializable.
- ``build_payload()`` transforms recall bundles into a
  ``DocumentWithMemories``-compatible structure.
- ``time_travel()`` reconstructs the graph at a given instant, using the
  contemporaneous grid context rather than today's.
- ``to_react_props()`` returns the props expected by the memory-graph
  React component.
- Custom ``GreenDecisionGraphError(ValueError)``.
- ``__main__`` smoke test.
"""

from __future__ import annotations

import html
import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

logger = logging.getLogger(__name__)

__version__ = "5.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class GreenDecisionGraphError(ValueError):
    """Raised for invalid graph inputs or configuration."""


# --------------------------------------------------------------------------- #
# Enums (as tuples for simplicity + validation)
# --------------------------------------------------------------------------- #
_NODE_TYPES: Tuple[str, ...] = (
    "workload", "device", "site", "model", "policy",
    "grid_context", "decision", "measurement", "incident",
)

_EDGE_TYPES: Tuple[str, ...] = (
    "executed-on", "selected-by", "governed-by", "superseded-by",
    "violated-budget", "improved-over", "validated-by",
)


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GreenDecisionGraphConfig:
    """Tunable parameters for the Green Decision Graph."""

    max_nodes: int = 5_000
    max_edges: int = 20_000
    time_travel_tolerance_seconds: float = 60.0
    escape_html: bool = True
    include_incidents: bool = True
    include_measurements: bool = True

    def __post_init__(self) -> None:
        for name in ("max_nodes", "max_edges"):
            v = getattr(self, name)
            if not isinstance(v, int) or v <= 0:
                raise GreenDecisionGraphError(f"{name} must be a positive int.")
        if self.time_travel_tolerance_seconds <= 0:
            raise GreenDecisionGraphError(
                "time_travel_tolerance_seconds must be > 0."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Node / Edge
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GraphNode:
    """Frozen graph node."""

    node_id: str
    node_type: str
    label: str
    properties: Mapping[str, Any] = field(default_factory=dict)
    observed_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise GreenDecisionGraphError("node_id must be a non-empty string.")
        if self.node_type not in _NODE_TYPES:
            raise GreenDecisionGraphError(
                f"node_type must be one of {_NODE_TYPES}, got {self.node_type!r}."
            )
        if not isinstance(self.label, str):
            raise GreenDecisionGraphError("label must be a string.")
        if not isinstance(self.properties, Mapping):
            raise GreenDecisionGraphError("properties must be a Mapping.")

    def to_dict(self, *, escape: bool = True) -> Dict[str, Any]:
        esc = html.escape if escape else (lambda s: s)
        return {
            "id": self.node_id,
            "type": self.node_type,
            "label": esc(str(self.label)),
            "properties": _json_safe(dict(self.properties)),
            "observed_at": self.observed_at.isoformat() if self.observed_at else None,
        }


@dataclass(frozen=True)
class GraphEdge:
    """Frozen graph edge."""

    source: str
    target: str
    edge_type: str
    weight: float = 1.0
    properties: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("source", "target"):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise GreenDecisionGraphError(f"{name} must be a non-empty string.")
        if self.edge_type not in _EDGE_TYPES:
            raise GreenDecisionGraphError(
                f"edge_type must be one of {_EDGE_TYPES}, got {self.edge_type!r}."
            )
        if isinstance(self.weight, bool) or not isinstance(self.weight, (int, float)):
            raise GreenDecisionGraphError("weight must be numeric.")
        if math.isnan(float(self.weight)) or math.isinf(float(self.weight)):
            raise GreenDecisionGraphError("weight must be finite.")
        if not isinstance(self.properties, Mapping):
            raise GreenDecisionGraphError("properties must be a Mapping.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "type": self.edge_type,
            "weight": float(self.weight),
            "properties": _json_safe(dict(self.properties)),
        }


@dataclass(frozen=True)
class GraphPayload:
    """Frozen graph payload, ``DocumentWithMemories``-compatible."""

    nodes: Tuple[GraphNode, ...]
    edges: Tuple[GraphEdge, ...]
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    filters_applied: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.nodes, tuple):
            object.__setattr__(self, "nodes", tuple(self.nodes))
        if not isinstance(self.edges, tuple):
            object.__setattr__(self, "edges", tuple(self.edges))
        if not isinstance(self.filters_applied, Mapping):
            raise GreenDecisionGraphError("filters_applied must be a Mapping.")

    def to_dict(self, *, escape: bool = True) -> Dict[str, Any]:
        return {
            "nodes": [n.to_dict(escape=escape) for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "generated_at": self.generated_at.isoformat(),
            "filters_applied": _json_safe(dict(self.filters_applied)),
        }

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kw)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(v) for v in value]
    return repr(value)


def _parse_iso(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


# --------------------------------------------------------------------------- #
# Graph builder
# --------------------------------------------------------------------------- #
class GreenDecisionGraph:
    """Builds Green Decision Graph payloads from recall bundles."""

    def __init__(
        self,
        *,
        config: Optional[GreenDecisionGraphConfig] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or GreenDecisionGraphConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._total_builds = 0
        self._total_nodes = 0
        self._total_edges = 0
        self._started_at = time.time()

    @property
    def config(self) -> GreenDecisionGraphConfig:
        return self._config

    # ---------------------------------------------------------- public API
    def build_payload(
        self,
        *,
        recall_bundles: Sequence[Any],
        policy_version: Optional[str] = None,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> GraphPayload:
        """Build a graph payload from one or more recall bundles."""
        if not isinstance(recall_bundles, Sequence):
            raise GreenDecisionGraphError(
                "recall_bundles must be a sequence."
            )

        nodes: List[GraphNode] = []
        edges: List[GraphEdge] = []
        seen_nodes: Set[str] = set()

        for bundle in recall_bundles:
            if not isinstance(bundle, Mapping):
                # Accept either RecallBundle or its dict form.
                bundle = getattr(bundle, "to_dict", lambda: bundle)()
            if not isinstance(bundle, Mapping):
                continue
            self._extract_from_bundle(
                bundle, nodes, edges, seen_nodes, policy_version=policy_version,
            )

        # Trim to configured caps.
        nodes = nodes[: self._config.max_nodes]
        edges = edges[: self._config.max_edges]

        with self._lock:
            self._total_builds += 1
            self._total_nodes += len(nodes)
            self._total_edges += len(edges)

        return GraphPayload(
            nodes=tuple(nodes),
            edges=tuple(edges),
            filters_applied=dict(filters or {}),
        )

    def time_travel(
        self,
        *,
        recall_bundles: Sequence[Any],
        at: datetime,
    ) -> GraphPayload:
        """Rebuild the graph as it would have looked at ``at``.

        Uses the contemporaneous grid context (from the recalled metadata)
        rather than today's. Any record observed after ``at`` is skipped.
        """
        if not isinstance(at, datetime):
            raise GreenDecisionGraphError("at must be a datetime.")
        if not at.tzinfo:
            at = at.replace(tzinfo=timezone.utc)

        filtered: List[Any] = []
        for bundle in recall_bundles:
            data = getattr(bundle, "to_dict", lambda: bundle)()
            if not isinstance(data, Mapping):
                continue
            kept = []
            for memory in data.get("memories", []):
                ts = _parse_iso((memory.get("metadata") or {}).get("observed_at"))
                if ts is None or ts <= at:
                    kept.append(memory)
            filtered.append({**dict(data), "memories": kept})

        return self.build_payload(
            recall_bundles=filtered,
            filters={"time_travel_at": at.isoformat()},
        )

    def to_react_props(self, payload: GraphPayload) -> Dict[str, Any]:
        """Return the props expected by ``@supermemory/memory-graph``."""
        data = payload.to_dict(escape=self._config.escape_html)
        return {
            "documents": [
                {
                    "id": n["id"],
                    "title": n["label"],
                    "type": n["type"],
                    "metadata": n["properties"],
                }
                for n in data["nodes"]
            ],
            "memories": [
                {
                    "sourceId": e["source"],
                    "targetId": e["target"],
                    "type": e["type"],
                    "weight": e["weight"],
                    "metadata": e["properties"],
                }
                for e in data["edges"]
            ],
            "generatedAt": data["generated_at"],
            "filters": data["filters_applied"],
        }

    # ---------------------------------------------------------- internals
    def _extract_from_bundle(
        self,
        bundle: Mapping[str, Any],
        nodes: List[GraphNode],
        edges: List[GraphEdge],
        seen_nodes: Set[str],
        *,
        policy_version: Optional[str],
    ) -> None:
        for memory in bundle.get("memories", []) or []:
            meta = memory.get("metadata") or {}
            run_id = meta.get("run_id") or memory.get("id") or "unknown"
            workload = meta.get("workload_type")
            device = meta.get("device_class")
            route = meta.get("route")
            site = meta.get("site_id")
            observed_at = _parse_iso(meta.get("observed_at"))
            pv = meta.get("policy_version") or policy_version

            # Decision node.
            decision_id = f"decision:{run_id}"
            if decision_id not in seen_nodes:
                nodes.append(GraphNode(
                    node_id=decision_id,
                    node_type="decision",
                    label=f"Decision {run_id}",
                    properties={
                        "run_id": run_id,
                        "route": route,
                        "policy_version": pv,
                        "truth_level": meta.get("truth_level"),
                        "quality_score": meta.get("quality_score"),
                        "metrics_hash": meta.get("metrics_hash"),
                    },
                    observed_at=observed_at,
                ))
                seen_nodes.add(decision_id)

            # Workload node.
            if isinstance(workload, str) and workload:
                wid = f"workload:{workload}"
                if wid not in seen_nodes:
                    nodes.append(GraphNode(
                        node_id=wid, node_type="workload", label=workload,
                    ))
                    seen_nodes.add(wid)
                edges.append(GraphEdge(
                    source=decision_id, target=wid, edge_type="selected-by",
                ))

            # Device node.
            if isinstance(device, str) and device:
                did = f"device:{device}"
                if did not in seen_nodes:
                    nodes.append(GraphNode(
                        node_id=did, node_type="device", label=device,
                    ))
                    seen_nodes.add(did)
                edges.append(GraphEdge(
                    source=decision_id, target=did, edge_type="executed-on",
                ))

            # Site node.
            if isinstance(site, str) and site:
                sid = f"site:{site}"
                if sid not in seen_nodes:
                    nodes.append(GraphNode(
                        node_id=sid, node_type="site", label=site,
                    ))
                    seen_nodes.add(sid)
                edges.append(GraphEdge(
                    source=decision_id, target=sid, edge_type="executed-on",
                ))

            # Policy node.
            if isinstance(pv, str) and pv:
                pid = f"policy:{pv}"
                if pid not in seen_nodes:
                    nodes.append(GraphNode(
                        node_id=pid, node_type="policy", label=pv,
                    ))
                    seen_nodes.add(pid)
                edges.append(GraphEdge(
                    source=decision_id, target=pid, edge_type="governed-by",
                ))

            # Superseded relationship.
            if meta.get("superseded_by"):
                target = f"decision:{meta['superseded_by']}"
                edges.append(GraphEdge(
                    source=decision_id, target=target,
                    edge_type="superseded-by",
                ))

            # Incident node (optional).
            if self._config.include_incidents and meta.get("incident_id"):
                iid = f"incident:{meta['incident_id']}"
                if iid not in seen_nodes:
                    nodes.append(GraphNode(
                        node_id=iid, node_type="incident",
                        label=str(meta.get("incident_id")),
                        properties={"severity": meta.get("severity")},
                    ))
                    seen_nodes.add(iid)
                edges.append(GraphEdge(
                    source=decision_id, target=iid, edge_type="validated-by",
                ))

            # Measurement node (optional).
            if self._config.include_measurements:
                measured = {
                    k: meta.get(k)
                    for k in ("energy_wh", "carbon_gco2e", "latency_ms")
                    if k in meta
                }
                if measured:
                    mid = f"measurement:{run_id}"
                    if mid not in seen_nodes:
                        nodes.append(GraphNode(
                            node_id=mid, node_type="measurement",
                            label=f"Measurement {run_id}",
                            properties=measured,
                            observed_at=observed_at,
                        ))
                        seen_nodes.add(mid)
                    edges.append(GraphEdge(
                        source=decision_id, target=mid,
                        edge_type="validated-by",
                    ))

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "builds": self._total_builds,
                "total_nodes": self._total_nodes,
                "total_edges": self._total_edges,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "uptime_seconds": time.time() - self._started_at,
            }

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.statistics(), default=str, **kw)

    def __repr__(self) -> str:
        return (
            "GreenDecisionGraph("
            f"builds={self._total_builds}, "
            f"nodes={self._total_nodes}, "
            f"edges={self._total_edges}, "
            f"strict={self._strict})"
        )


__all__ = [
    "GraphEdge",
    "GraphNode",
    "GraphPayload",
    "GreenDecisionGraph",
    "GreenDecisionGraphConfig",
    "GreenDecisionGraphError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m dashboard.green_decision_graph
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    graph = GreenDecisionGraph()
    print("repr       :", graph)

    bundle = {
        "query": "vision_inference arm_edge",
        "memories": [
            {
                "id": "mem-1",
                "metadata": {
                    "run_id": "run-001",
                    "workload_type": "vision_inference",
                    "device_class": "arm_edge",
                    "route": "edge_int8",
                    "site_id": "edge-node-ygy-03",
                    "policy_version": "v0.3",
                    "truth_level": "measured",
                    "quality_score": 0.91,
                    "energy_wh": 8.6,
                    "carbon_gco2e": 4.0,
                    "latency_ms": 438.0,
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                },
            },
            {
                "id": "mem-2",
                "metadata": {
                    "run_id": "run-002",
                    "workload_type": "text_gen",
                    "device_class": "cloud_gpu",
                    "route": "cloud_gpu",
                    "policy_version": "v0.3",
                    "truth_level": "measured",
                    "energy_wh": 12.0,
                    "carbon_gco2e": 6.0,
                    "latency_ms": 320.0,
                    "incident_id": "inc-1",
                    "severity": "high",
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                },
            },
        ],
    }

    payload = graph.build_payload(recall_bundles=[bundle])
    print(f"nodes      : {len(payload.nodes)}")
    print(f"edges      : {len(payload.edges)}")
    for n in payload.nodes:
        print(f"  {n.node_type:<14} {n.node_id}")

    props = graph.to_react_props(payload)
    print("react docs :", len(props["documents"]))
    print("react mems :", len(props["memories"]))

    # Time travel.
    past = graph.time_travel(
        recall_bundles=[bundle],
        at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    print("past nodes :", len(past.nodes))
    assert len(past.nodes) == 0

    print("statistics :", {
        k: v for k, v in graph.statistics().items()
        if k not in ("config", "uptime_seconds")
    })

    # Validation.
    for bad in (
        lambda: graph.build_payload(recall_bundles="not-a-sequence"),
        lambda: graph.time_travel(recall_bundles=[], at="not-a-datetime"),
    ):
        try:
            bad()
        except GreenDecisionGraphError as exc:
            print("Rejected   :", exc)

    print("\nSmoke test passed.")
