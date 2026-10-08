# src/quantum_integration/digital_twin/digital_twin_limit_graph.py

"""LIMIT graph manager — nodes, edges, metadata."""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional

from .digital_twin_errors import DigitalTwinInputError

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 1


def _validate_id(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise DigitalTwinInputError(f"{name} must be a non-empty string.")
    return value


class LimitGraphManager:
    """Manages graph structure with an optional Storage backend.

    In-memory state is initialized eagerly so concurrent ``create_graph``
    calls can't race on ``self._graphs`` initialization.
    """

    def __init__(self, storage: Optional[Any] = None) -> None:
        self.storage = storage
        self._graphs: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    def create_graph(
        self,
        graph_id: str,
        description: str,
        configuration: Dict[str, Any],
    ) -> None:
        _validate_id("graph_id", graph_id)
        if not isinstance(description, str):
            raise DigitalTwinInputError("description must be a string.")
        if not isinstance(configuration, dict):
            raise DigitalTwinInputError("configuration must be a dict.")
        if self.storage and hasattr(self.storage, "save_limit_graph_metadata"):
            self.storage.save_limit_graph_metadata(
                graph_id, description, configuration,
            )
            return
        with self._lock:
            self._graphs[graph_id] = {
                "description": description,
                "configuration": dict(configuration),
                "nodes": {},
                "edges": {},
            }

    def has_graph(self, graph_id: str) -> bool:
        _validate_id("graph_id", graph_id)
        if self.storage and hasattr(self.storage, "get_limit_graph_metadata"):
            return self.storage.get_limit_graph_metadata(graph_id) is not None
        with self._lock:
            return graph_id in self._graphs

    def delete_graph(self, graph_id: str) -> bool:
        _validate_id("graph_id", graph_id)
        with self._lock:
            return self._graphs.pop(graph_id, None) is not None

    def list_graphs(self) -> List[str]:
        with self._lock:
            return sorted(self._graphs.keys())

    def add_node(
        self,
        graph_id: str,
        node_id: str,
        node_type: Optional[str],
        attributes: Dict[str, Any],
    ) -> None:
        _validate_id("graph_id", graph_id)
        _validate_id("node_id", node_id)
        if not isinstance(attributes, dict):
            raise DigitalTwinInputError("attributes must be a dict.")
        if self.storage and hasattr(self.storage, "save_limit_graph_node"):
            self.storage.save_limit_graph_node(
                node_id, graph_id, node_type, attributes,
            )
            return
        with self._lock:
            graph = self._graphs.get(graph_id)
            if graph is None:
                raise DigitalTwinInputError(
                    f"graph {graph_id!r} does not exist."
                )
            graph["nodes"][node_id] = {
                "node_type": node_type,
                "attributes": dict(attributes),
            }

    def add_edge(
        self,
        graph_id: str,
        edge_id: str,
        source: str,
        target: str,
        weight: Optional[float],
        attributes: Dict[str, Any],
    ) -> None:
        _validate_id("graph_id", graph_id)
        _validate_id("edge_id", edge_id)
        _validate_id("source", source)
        _validate_id("target", target)
        if weight is not None and not isinstance(weight, (int, float)):
            raise DigitalTwinInputError("weight must be numeric or None.")
        if not isinstance(attributes, dict):
            raise DigitalTwinInputError("attributes must be a dict.")
        if self.storage and hasattr(self.storage, "save_limit_graph_edge"):
            self.storage.save_limit_graph_edge(
                edge_id, graph_id, source, target, weight, attributes,
            )
            return
        with self._lock:
            graph = self._graphs.get(graph_id)
            if graph is None:
                raise DigitalTwinInputError(
                    f"graph {graph_id!r} does not exist."
                )
            graph["edges"][edge_id] = {
                "source": source,
                "target": target,
                "weight": float(weight) if weight is not None else None,
                "attributes": dict(attributes),
            }

    def get_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        _validate_id("graph_id", graph_id)
        if self.storage and hasattr(self.storage, "get_limit_graph_nodes"):
            return self.storage.get_limit_graph_nodes(graph_id)
        with self._lock:
            graph = self._graphs.get(graph_id)
            if graph is None:
                return []
            return [dict(v, node_id=k) for k, v in graph["nodes"].items()]

    def get_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        _validate_id("graph_id", graph_id)
        if self.storage and hasattr(self.storage, "get_limit_graph_edges"):
            return self.storage.get_limit_graph_edges(graph_id)
        with self._lock:
            graph = self._graphs.get(graph_id)
            if graph is None:
                return []
            return [dict(v, edge_id=k) for k, v in graph["edges"].items()]

    def get_metadata(self, graph_id: str) -> Optional[Dict[str, Any]]:
        _validate_id("graph_id", graph_id)
        if self.storage and hasattr(self.storage, "get_limit_graph_metadata"):
            return self.storage.get_limit_graph_metadata(graph_id)
        with self._lock:
            return dict(self._graphs.get(graph_id, {}))

    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "schema_version": SCHEMA_VERSION,
                "graphs": sorted(self._graphs.keys()),
                "graph_count": len(self._graphs),
                "uses_storage": self.storage is not None,
            }

    def close(self, *, flush: bool = True) -> None:
        return None


__all__ = ["SCHEMA_VERSION", "LimitGraphManager"]
