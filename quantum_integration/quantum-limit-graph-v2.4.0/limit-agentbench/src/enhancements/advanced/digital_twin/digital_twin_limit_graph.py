# src/quantum_integration/digital_twin/digital_twin_limit_graph.py

"""LIMIT graph manager — nodes, edges, metadata.

The :class:`LimitGraphManager` maintains graph metadata, nodes, and edges
either in-process (default) or via an optional storage backend. When a
storage backend is provided, every public method delegates to that
backend *if* the backend implements the corresponding hook; otherwise
the in-process view is used. This lets v1 backends (metadata + nodes +
edges only) continue to work while still gaining the extended API
(``get_node``, ``remove_edge``, cascading ``remove_node``, etc.).

Thread-safety
-------------
The in-process state is guarded by a re-entrant lock (``self._lock``).
The lock covers *only* the in-process view. Storage backends are assumed
to be thread-safe on their own; the manager does not wrap storage calls
in the local lock.

Freezing
--------
Every value returned by a public read method is deep-frozen: mappings
become :class:`types.MappingProxyType`, sequences become tuples, sets
become frozensets. Callers cannot mutate the manager's state through a
returned object. Use :func:`digital_twin_helpers._to_plain` to obtain
plain ``dict`` / ``list`` structures for serialization.

Storage protocol
----------------
Backends are duck-typed against :class:`_LimitGraphStorage`. All methods
are optional; the manager probes ``hasattr`` on each call and falls back
to the in-process view when a hook is absent.
"""

from __future__ import annotations

import logging
import math
import threading
from collections.abc import Mapping as ABCMapping
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Protocol,
    runtime_checkable,
)

from .digital_twin_errors import (
    DigitalTwinInputError,
    DigitalTwinNotFoundError,
)
from .digital_twin_helpers import _deep_freeze, _to_plain

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 2

#: Sentinel returned by ``_try_storage`` when the backend does not
#: implement the requested hook. Distinct from ``None`` so callers can
#: tell "unsupported" apart from "returned None".
_NO_STORAGE: Any = object()


# --------------------------------------------------------------------------- #
# Storage protocol
# --------------------------------------------------------------------------- #

@runtime_checkable
class _LimitGraphStorage(Protocol):
    """Duck-typed protocol for LIMIT graph storage backends.

    Every method is optional. The manager probes for individual methods
    at runtime so a backend may implement only a subset.
    """

    # -- metadata ---------------------------------------------------------- #
    def save_limit_graph_metadata(
        self, graph_id: str, description: str, configuration: Any,
    ) -> None: ...

    def get_limit_graph_metadata(
        self, graph_id: str,
    ) -> Optional[Dict[str, Any]]: ...

    def delete_limit_graph(self, graph_id: str) -> bool: ...

    def list_limit_graphs(self) -> List[str]: ...

    # -- nodes ------------------------------------------------------------- #
    def save_limit_graph_node(
        self,
        node_id: str,
        graph_id: str,
        node_type: Optional[str],
        attributes: Any,
    ) -> None: ...

    def get_limit_graph_nodes(
        self, graph_id: str,
    ) -> List[Dict[str, Any]]: ...

    def get_limit_graph_node(
        self, graph_id: str, node_id: str,
    ) -> Optional[Dict[str, Any]]: ...

    def remove_limit_graph_node(
        self, graph_id: str, node_id: str,
    ) -> bool: ...

    # -- edges ------------------------------------------------------------- #
    def save_limit_graph_edge(
        self,
        edge_id: str,
        graph_id: str,
        source: str,
        target: str,
        weight: Optional[float],
        attributes: Any,
    ) -> None: ...

    def get_limit_graph_edges(
        self, graph_id: str,
    ) -> List[Dict[str, Any]]: ...

    def get_limit_graph_edge(
        self, graph_id: str, edge_id: str,
    ) -> Optional[Dict[str, Any]]: ...

    def remove_limit_graph_edge(
        self, graph_id: str, edge_id: str,
    ) -> bool: ...

    # -- lifecycle (optional) --------------------------------------------- #
    def flush(self) -> None: ...

    def close(self) -> None: ...


# --------------------------------------------------------------------------- #
# Validation helpers
# --------------------------------------------------------------------------- #

def _validate_id(name: str, value: Any) -> str:
    """Return ``value`` if it is a non-empty, non-whitespace string."""
    if not isinstance(value, str) or not value.strip():
        raise DigitalTwinInputError(
            f"{name} must be a non-empty, non-whitespace string."
        )
    return value


def _validate_optional_str(name: str, value: Any) -> Optional[str]:
    """Return ``value`` if it is ``None`` or a string."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise DigitalTwinInputError(f"{name} must be a string or None.")
    return value


def _validate_mapping(name: str, value: Any) -> Mapping[str, Any]:
    """Return ``value`` if it is a mapping, else raise."""
    if not isinstance(value, ABCMapping):
        raise DigitalTwinInputError(f"{name} must be a mapping.")
    return value


def _validate_weight(value: Any) -> Optional[float]:
    """Return ``value`` as a finite float, or ``None``.

    Rejects ``bool``, ``NaN``, and ``±inf``.
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DigitalTwinInputError(
            "weight must be a finite number or None."
        )
    if not math.isfinite(value):
        raise DigitalTwinInputError(
            "weight must be finite (no NaN or infinity)."
        )
    return float(value)


def _snapshot(value: Any) -> Any:
    """Return a fresh plain-data copy (dict/list/scalars only).

    Used when forwarding data to a storage backend so the backend does
    not need to understand ``MappingProxyType`` / tuples produced by
    :func:`_deep_freeze`.
    """
    return _to_plain(_deep_freeze(value))


# --------------------------------------------------------------------------- #
# Manager
# --------------------------------------------------------------------------- #

class LimitGraphManager:
    """Manages graph structure with an optional Storage backend.

    In-memory state is initialized eagerly so concurrent ``create_graph``
    calls cannot race on ``self._graphs`` initialization.

    Parameters
    ----------
    storage:
        Optional duck-typed storage backend. See
        :class:`_LimitGraphStorage`.
    allow_self_loops:
        When ``False`` (default), :meth:`add_edge` rejects edges whose
        ``source`` and ``target`` are equal.
    """

    def __init__(
        self,
        storage: Optional[Any] = None,
        *,
        allow_self_loops: bool = False,
    ) -> None:
        self.storage = storage
        self.allow_self_loops = bool(allow_self_loops)
        self._graphs: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        if storage is not None and not isinstance(storage, _LimitGraphStorage):
            logger.debug(
                "Storage backend %s does not fully implement "
                "_LimitGraphStorage; partial support will be used.",
                type(storage).__name__,
            )

    # -- storage dispatch -------------------------------------------------- #

    def _try_storage(
        self, method_name: str, *args: Any, **kwargs: Any,
    ) -> Any:
        """Call a storage hook if present, else return :data:`_NO_STORAGE`.

        A single dispatch helper for every public method, ensuring
        consistent "storage-vs-memory" semantics across the manager.
        """
        if self.storage is None:
            return _NO_STORAGE
        fn = getattr(self.storage, method_name, None)
        if fn is None:
            return _NO_STORAGE
        return fn(*args, **kwargs)

    # -- shared existence checks ------------------------------------------ #

    def _ensure_graph_exists(self, graph_id: str) -> None:
        """Raise :class:`DigitalTwinNotFoundError` if graph is missing.

        Uses the storage backend's metadata lookup when available,
        otherwise falls back to the in-process view. If the backend
        supports node/edge writes but not metadata reads, existence is
        trusted to the backend.
        """
        if self.storage is not None:
            if hasattr(self.storage, "get_limit_graph_metadata"):
                if self.storage.get_limit_graph_metadata(graph_id) is None:
                    raise DigitalTwinNotFoundError(
                        f"graph {graph_id!r} does not exist.",
                        graph_id=graph_id,
                    )
                return
            # Storage handles nodes/edges but not metadata reads: trust it.
            if (
                hasattr(self.storage, "save_limit_graph_node")
                or hasattr(self.storage, "save_limit_graph_edge")
            ):
                return
        with self._lock:
            if graph_id not in self._graphs:
                raise DigitalTwinNotFoundError(
                    f"graph {graph_id!r} does not exist.",
                    graph_id=graph_id,
                )

    # -- metadata ---------------------------------------------------------- #

    def create_graph(
        self,
        graph_id: str,
        description: str,
        configuration: Mapping[str, Any],
    ) -> None:
        """Create a new graph. Raises if ``graph_id`` already exists."""
        _validate_id("graph_id", graph_id)
        if not isinstance(description, str):
            raise DigitalTwinInputError("description must be a string.")
        _validate_mapping("configuration", configuration)

        frozen_config = _deep_freeze(dict(configuration))

        # Storage path — first probe for duplicate, then save.
        existing = self._try_storage("get_limit_graph_metadata", graph_id)
        if existing is not _NO_STORAGE:
            if existing is not None:
                raise DigitalTwinInputError(
                    f"graph {graph_id!r} already exists."
                )
            saved = self._try_storage(
                "save_limit_graph_metadata",
                graph_id,
                description,
                _snapshot(configuration),
            )
            if saved is not _NO_STORAGE:
                return

        with self._lock:
            if graph_id in self._graphs:
                raise DigitalTwinInputError(
                    f"graph {graph_id!r} already exists."
                )
            self._graphs[graph_id] = {
                "description": description,
                "configuration": frozen_config,
                "nodes": {},
                "edges": {},
            }

    def has_graph(self, graph_id: str) -> bool:
        """Return ``True`` if the graph exists."""
        _validate_id("graph_id", graph_id)
        result = self._try_storage("get_limit_graph_metadata", graph_id)
        if result is not _NO_STORAGE:
            return result is not None
        with self._lock:
            return graph_id in self._graphs

    def delete_graph(self, graph_id: str) -> bool:
        """Delete a graph. Returns ``True`` if it existed."""
        _validate_id("graph_id", graph_id)
        result = self._try_storage("delete_limit_graph", graph_id)
        if result is not _NO_STORAGE:
            return bool(result)
        with self._lock:
            return self._graphs.pop(graph_id, None) is not None

    def list_graphs(self) -> List[str]:
        """Return sorted graph IDs."""
        result = self._try_storage("list_limit_graphs")
        if result is not _NO_STORAGE:
            return sorted(result)
        with self._lock:
            return sorted(self._graphs.keys())

    def get_metadata(self, graph_id: str) -> Optional[Mapping[str, Any]]:
        """Return metadata for ``graph_id``, or ``None`` if missing.

        Only ``description`` and ``configuration`` are returned; nodes
        and edges are excluded. The returned mapping is deep-frozen.
        """
        _validate_id("graph_id", graph_id)
        result = self._try_storage("get_limit_graph_metadata", graph_id)
        if result is not _NO_STORAGE:
            return None if result is None else _project_metadata(result)
        with self._lock:
            graph = self._graphs.get(graph_id)
            if graph is None:
                return None
            return _project_metadata(graph)

    # -- nodes ------------------------------------------------------------- #

    def add_node(
        self,
        graph_id: str,
        node_id: str,
        node_type: Optional[str],
        attributes: Mapping[str, Any],
    ) -> None:
        """Add a node to an existing graph.

        Raises
        ------
        DigitalTwinInputError
            On duplicate ``node_id`` or invalid input.
        DigitalTwinNotFoundError
            If the graph does not exist.
        """
        _validate_id("graph_id", graph_id)
        _validate_id("node_id", node_id)
        _validate_optional_str("node_type", node_type)
        _validate_mapping("attributes", attributes)

        frozen_attrs = _deep_freeze(dict(attributes))

        self._ensure_graph_exists(graph_id)
        self._ensure_node_absent(graph_id, node_id)

        saved = self._try_storage(
            "save_limit_graph_node",
            node_id,
            graph_id,
            node_type,
            _snapshot(attributes),
        )
        if saved is not _NO_STORAGE:
            return

        with self._lock:
            graph = self._graphs.get(graph_id)
            if graph is None:
                raise DigitalTwinNotFoundError(
                    f"graph {graph_id!r} does not exist.",
                    graph_id=graph_id,
                )
            if node_id in graph["nodes"]:
                raise DigitalTwinInputError(
                    f"node {node_id!r} already exists in graph {graph_id!r}."
                )
            graph["nodes"][node_id] = {
                "node_type": node_type,
                "attributes": frozen_attrs,
            }

    def get_nodes(self, graph_id: str) -> List[Mapping[str, Any]]:
        """Return all nodes for ``graph_id`` as frozen mappings.

        Raises :class:`DigitalTwinNotFoundError` if the graph is missing.
        """
        _validate_id("graph_id", graph_id)
        self._ensure_graph_exists(graph_id)

        result = self._try_storage("get_limit_graph_nodes", graph_id)
        if result is not _NO_STORAGE:
            return [_freeze_node(n) for n in result]

        with self._lock:
            graph = self._graphs.get(graph_id)
            if graph is None:
                raise DigitalTwinNotFoundError(
                    f"graph {graph_id!r} does not exist.",
                    graph_id=graph_id,
                )
            return [
                _freeze_node({**v, "node_id": k})
                for k, v in graph["nodes"].items()
            ]

    def get_node(
        self, graph_id: str, node_id: str,
    ) -> Optional[Mapping[str, Any]]:
        """Return a single node as a frozen mapping, or ``None``."""
        _validate_id("graph_id", graph_id)
        _validate_id("node_id", node_id)
        self._ensure_graph_exists(graph_id)

        result = self._try_storage("get_limit_graph_node", graph_id, node_id)
        if result is not _NO_STORAGE:
            return None if result is None else _freeze_node(result)

        # Backend has no single-node hook: probe via list.
        nodes = self._try_storage("get_limit_graph_nodes", graph_id)
        if nodes is not _NO_STORAGE:
            match = next(
                (n for n in nodes if n.get("node_id") == node_id), None,
            )
            return None if match is None else _freeze_node(match)

        with self._lock:
            graph = self._graphs.get(graph_id)
            if graph is None:
                raise DigitalTwinNotFoundError(
                    f"graph {graph_id!r} does not exist.",
                    graph_id=graph_id,
                )
            node = graph["nodes"].get(node_id)
            if node is None:
                return None
            return _freeze_node({**node, "node_id": node_id})

    def remove_node(self, graph_id: str, node_id: str) -> bool:
        """Remove a node. Returns ``True`` if it existed.

        Edges referencing the removed node are also removed (cascade).
        """
        _validate_id("graph_id", graph_id)
        _validate_id("node_id", node_id)
        self._ensure_graph_exists(graph_id)

        result = self._try_storage(
            "remove_limit_graph_node", graph_id, node_id,
        )
        if result is not _NO_STORAGE:
            return bool(result)

        with self._lock:
            graph = self._graphs.get(graph_id)
            if graph is None:
                raise DigitalTwinNotFoundError(
                    f"graph {graph_id!r} does not exist.",
                    graph_id=graph_id,
                )
            if node_id not in graph["nodes"]:
                return False
            del graph["nodes"][node_id]
            # Cascade: drop any edge that references this node.
            stale = [
                eid for eid, e in graph["edges"].items()
                if e["source"] == node_id or e["target"] == node_id
            ]
            for eid in stale:
                del graph["edges"][eid]
            return True

    # -- edges ------------------------------------------------------------- #

    def add_edge(
        self,
        graph_id: str,
        edge_id: str,
        source: str,
        target: str,
        weight: Optional[float],
        attributes: Mapping[str, Any],
    ) -> None:
        """Add an edge to an existing graph.

        Raises
        ------
        DigitalTwinInputError
            On duplicate ``edge_id``, invalid input, self-loops (unless
            ``allow_self_loops=True``), or ``weight`` not finite.
        DigitalTwinNotFoundError
            If the graph, ``source`` node, or ``target`` node is missing.
        """
        _validate_id("graph_id", graph_id)
        _validate_id("edge_id", edge_id)
        _validate_id("source", source)
        _validate_id("target", target)
        _validate_mapping("attributes", attributes)
        w = _validate_weight(weight)

        if source == target and not self.allow_self_loops:
            raise DigitalTwinInputError(
                f"self-loops are not allowed (source == target == {source!r})."
            )

        self._ensure_graph_exists(graph_id)

        for role, node_id in (("source", source), ("target", target)):
            if self.get_node(graph_id, node_id) is None:
                raise DigitalTwinNotFoundError(
                    f"{role} node {node_id!r} does not exist in graph "
                    f"{graph_id!r}.",
                    graph_id=graph_id,
                    node_id=node_id,
                    role=role,
                )

        self._ensure_edge_absent(graph_id, edge_id)

        frozen_attrs = _deep_freeze(dict(attributes))
        saved = self._try_storage(
            "save_limit_graph_edge",
            edge_id,
            graph_id,
            source,
            target,
            w,
            _snapshot(attributes),
        )
        if saved is not _NO_STORAGE:
            return

        with self._lock:
            graph = self._graphs.get(graph_id)
            if graph is None:
                raise DigitalTwinNotFoundError(
                    f"graph {graph_id!r} does not exist.",
                    graph_id=graph_id,
                )
            if edge_id in graph["edges"]:
                raise DigitalTwinInputError(
                    f"edge {edge_id!r} already exists in graph {graph_id!r}."
                )
            graph["edges"][edge_id] = {
                "source": source,
                "target": target,
                "weight": w,
                "attributes": frozen_attrs,
            }

    def get_edges(self, graph_id: str) -> List[Mapping[str, Any]]:
        """Return all edges for ``graph_id`` as frozen mappings.

        Raises :class:`DigitalTwinNotFoundError` if the graph is missing.
        """
        _validate_id("graph_id", graph_id)
        self._ensure_graph_exists(graph_id)

        result = self._try_storage("get_limit_graph_edges", graph_id)
        if result is not _NO_STORAGE:
            return [_freeze_edge(e) for e in result]

        with self._lock:
            graph = self._graphs.get(graph_id)
            if graph is None:
                raise DigitalTwinNotFoundError(
                    f"graph {graph_id!r} does not exist.",
                    graph_id=graph_id,
                )
            return [
                _freeze_edge({**v, "edge_id": k})
                for k, v in graph["edges"].items()
            ]

    def get_edge(
        self, graph_id: str, edge_id: str,
    ) -> Optional[Mapping[str, Any]]:
        """Return a single edge as a frozen mapping, or ``None``."""
        _validate_id("graph_id", graph_id)
        _validate_id("edge_id", edge_id)
        self._ensure_graph_exists(graph_id)

        result = self._try_storage("get_limit_graph_edge", graph_id, edge_id)
        if result is not _NO_STORAGE:
            return None if result is None else _freeze_edge(result)

        edges = self._try_storage("get_limit_graph_edges", graph_id)
        if edges is not _NO_STORAGE:
            match = next(
                (e for e in edges if e.get("edge_id") == edge_id), None,
            )
            return None if match is None else _freeze_edge(match)

        with self._lock:
            graph = self._graphs.get(graph_id)
            if graph is None:
                raise DigitalTwinNotFoundError(
                    f"graph {graph_id!r} does not exist.",
                    graph_id=graph_id,
                )
            edge = graph["edges"].get(edge_id)
            if edge is None:
                return None
            return _freeze_edge({**edge, "edge_id": edge_id})

    def remove_edge(self, graph_id: str, edge_id: str) -> bool:
        """Remove an edge. Returns ``True`` if it existed."""
        _validate_id("graph_id", graph_id)
        _validate_id("edge_id", edge_id)
        self._ensure_graph_exists(graph_id)

        result = self._try_storage(
            "remove_limit_graph_edge", graph_id, edge_id,
        )
        if result is not _NO_STORAGE:
            return bool(result)

        with self._lock:
            graph = self._graphs.get(graph_id)
            if graph is None:
                raise DigitalTwinNotFoundError(
                    f"graph {graph_id!r} does not exist.",
                    graph_id=graph_id,
                )
            return graph["edges"].pop(edge_id, None) is not None

    # -- internal existence probes ---------------------------------------- #

    def _ensure_node_absent(self, graph_id: str, node_id: str) -> None:
        """Raise if the node already exists. No-op when unknown."""
        result = self._try_storage(
            "get_limit_graph_node", graph_id, node_id,
        )
        if result is not _NO_STORAGE:
            if result is not None:
                raise DigitalTwinInputError(
                    f"node {node_id!r} already exists in graph "
                    f"{graph_id!r}."
                )
            return
        nodes = self._try_storage("get_limit_graph_nodes", graph_id)
        if nodes is not _NO_STORAGE:
            if any(n.get("node_id") == node_id for n in nodes):
                raise DigitalTwinInputError(
                    f"node {node_id!r} already exists in graph "
                    f"{graph_id!r}."
                )

    def _ensure_edge_absent(self, graph_id: str, edge_id: str) -> None:
        """Raise if the edge already exists. No-op when unknown."""
        result = self._try_storage(
            "get_limit_graph_edge", graph_id, edge_id,
        )
        if result is not _NO_STORAGE:
            if result is not None:
                raise DigitalTwinInputError(
                    f"edge {edge_id!r} already exists in graph "
                    f"{graph_id!r}."
                )
            return
        edges = self._try_storage("get_limit_graph_edges", graph_id)
        if edges is not _NO_STORAGE:
            if any(e.get("edge_id") == edge_id for e in edges):
                raise DigitalTwinInputError(
                    f"edge {edge_id!r} already exists in graph "
                    f"{graph_id!r}."
                )

    # -- statistics & lifecycle ------------------------------------------- #

    def statistics(self) -> Dict[str, Any]:
        """Return a summary of the manager's state.

        Counts are derived from the same views used by ``list_graphs`` /
        ``get_nodes`` / ``get_edges``, so they reflect storage when a
        backend is present.
        """
        graph_ids = self.list_graphs()
        node_counts: Dict[str, int] = {}
        edge_counts: Dict[str, int] = {}
        total_nodes = 0
        total_edges = 0

        for gid in graph_ids:
            try:
                nodes = self.get_nodes(gid)
                node_counts[gid] = len(nodes)
                total_nodes += len(nodes)
            except DigitalTwinNotFoundError:
                node_counts[gid] = 0
            try:
                edges = self.get_edges(gid)
                edge_counts[gid] = len(edges)
                total_edges += len(edges)
            except DigitalTwinNotFoundError:
                edge_counts[gid] = 0

        return {
            "schema_version": SCHEMA_VERSION,
            "graphs": graph_ids,
            "graph_count": len(graph_ids),
            "node_counts": node_counts,
            "edge_counts": edge_counts,
            "total_nodes": total_nodes,
            "total_edges": total_edges,
            "uses_storage": self.storage is not None,
            "storage_type": (
                type(self.storage).__name__
                if self.storage is not None else None
            ),
            "allow_self_loops": self.allow_self_loops,
        }

    def close(self, *, flush: bool = True) -> None:
        """Flush and/or close the storage backend, if any.

        ``flush=False`` skips ``storage.flush()`` but still calls
        ``storage.close()`` when available.
        """
        if self.storage is None:
            return
        if flush:
            fn = getattr(self.storage, "flush", None)
            if fn is not None:
                fn()
        fn = getattr(self.storage, "close", None)
        if fn is not None:
            fn()

    # -- dunder helpers ---------------------------------------------------- #

    def __contains__(self, graph_id: object) -> bool:
        """Support ``"g" in manager`` without raising on bad input."""
        if not isinstance(graph_id, str) or not graph_id.strip():
            return False
        return self.has_graph(graph_id)

    def __repr__(self) -> str:
        with self._lock:
            n = len(self._graphs)
        storage_name = (
            type(self.storage).__name__ if self.storage is not None else None
        )
        return (
            f"{type(self).__name__}(graphs={n}, storage={storage_name}, "
            f"allow_self_loops={self.allow_self_loops})"
        )


# --------------------------------------------------------------------------- #
# Freezing / projection helpers
# --------------------------------------------------------------------------- #

def _project_metadata(graph: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return only the metadata fields, deep-frozen."""
    return _deep_freeze({
        "description": graph.get("description"),
        "configuration": graph.get("configuration", {}),
    })


def _freeze_node(node: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return a deep-frozen copy of a node record."""
    return _deep_freeze(dict(node))


def _freeze_edge(edge: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return a deep-frozen copy of an edge record."""
    return _deep_freeze(dict(edge))


__all__ = [
    "SCHEMA_VERSION",
    "LimitGraphManager",
    "_LimitGraphStorage",
]
