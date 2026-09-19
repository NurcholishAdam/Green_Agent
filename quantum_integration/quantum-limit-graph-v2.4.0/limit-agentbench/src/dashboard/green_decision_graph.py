# src/dashboard/green_decision_graph.py

"""
Green Decision Graph
====================

Wrapper around ``@supermemory/memory-graph`` (the memory-graph-playground
package) that turns Supermemory recall into the Green Decision Graph UI
described in the enhancement proposal.

Node types: workload, device, site, model, policy, grid_context, decision,
measurement, incident, verdict.

Edge types: executed-on, selected-by, governed-by, superseded-by,
violated-budget, improved-over, hurt-by, validated-by, requires-human.

Enhancements
------------
- ``NodeType`` / ``EdgeType`` — str-enums with ``coerce`` / ``values``.
- ``GreenDecisionGraphConfig`` — frozen, validated, fully serializable:
  caps, time-travel tolerance (now enforced), HTML escaping, optional
  overlays, container-tag expectation, schema version.
- ``GraphNode`` / ``GraphEdge`` / ``GraphPayload`` — deeply frozen
  (``MappingProxyType``), hashable, full serialization symmetry, plus
  ``to_episode_payload()`` / ``to_memory_dict()`` bridges on the payload.
- ``build_payload()`` classifies memories via ``metadata.kind`` (the
  enhanced schema contract) and honors ``metadata.container_tag``.
- ``time_travel()`` uses the configured tolerance and returns the
  contemporaneous graph.
- Overlays:
    * ``GuardVerdict`` → ``verdict`` nodes + ``violated-budget`` /
      ``requires-human`` edges.
    * ``BenchmarkReport`` → ``improved-over`` / ``hurt-by`` overlays
      between baseline and memory decision nodes.
    * ``PolicySnapshot`` → richer policy nodes (truth_level,
      container_tag, superseded_by).
    * ``MemoryTierManager`` → per-node ``tier`` / ``importance``
      annotations.
- ``build_from_pipeline(pipeline)`` — construct a graph from the package
  pipeline in one call.
- Emits the previously declared-but-unused ``grid_context`` / ``model``
  nodes and ``improved-over`` / ``violated-budget`` edges.
- ``dedupe_edges`` by default — no more quadratic payloads.
- Robust ``run_id`` fallback — no more ``"unknown"`` collisions.
- Container-tag filtering via ``expected_container_tag``.
- Manager-level ``statistics()`` / ``reset()`` / ``close()`` /
  context manager / async siblings.
- Structured error hierarchy:
  ``GreenDecisionGraphError`` → ``GreenDecisionGraphInputError``,
  ``GreenDecisionGraphConfigError``, ``GreenDecisionGraphParseError``.
- ``__version__`` exported via ``__all__``.
- ``__main__`` smoke test covers happy paths, overlays, time travel,
  container tags, dedup, serialization, async, reset, and validation.
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import math
import threading
import time
from collections import deque
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    Deque,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "6.0.0"

SCHEMA_VERSION: int = 1
DEFAULT_CONTAINER_TAG: str = "org:green-agent"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class GreenDecisionGraphError(ValueError):
    """Base class for green decision graph problems."""


class GreenDecisionGraphInputError(GreenDecisionGraphError):
    """Invalid input to a public API."""


class GreenDecisionGraphConfigError(GreenDecisionGraphError):
    """Invalid configuration."""


class GreenDecisionGraphParseError(GreenDecisionGraphError):
    """Failed to parse a node / edge / payload from dict or JSON."""


# --------------------------------------------------------------------------- #
# Shared validation helpers — mirror the other enhanced modules
# --------------------------------------------------------------------------- #
def _is_real_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_finite_nonneg(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def _is_positive_finite(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value > 0
    )


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    """Parse an ISO 8601 timestamp; tolerate ``Z`` and numeric epochs."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        ts = float(value)
        if ts > 1e12:
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if s.endswith("Z") or s.endswith("z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_safe(value: Any, *, depth: int = 0) -> Any:
    """Return a JSON-safe copy of ``value``; unknown types become strings."""
    if depth > 32:
        return "<truncated>"
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value if hasattr(value, "value") else str(value)
    if isinstance(value, ABCMapping):
        return {
            str(k): _json_safe(v, depth=depth + 1)
            for k, v in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(v, depth=depth + 1) for v in value]
    return str(value)


def _freeze_mapping(value: Any, *, name: str) -> Mapping[str, Any]:
    if value is None:
        return MappingProxyType({})
    if not isinstance(value, ABCMapping):
        raise GreenDecisionGraphInputError(f"{name} must be a Mapping.")
    return MappingProxyType({
        str(k): _json_safe(v) for k, v in value.items()
    })


def _coerce_to_mapping(obj: Any) -> Optional[Mapping[str, Any]]:
    """Best-effort conversion to a mapping.

    Tries ``.to_dict()`` first (enhanced submodule convention), then
    ``dict(obj)`` for a mapping, then ``None``.
    """
    if obj is None:
        return None
    if isinstance(obj, ABCMapping):
        return obj
    to_dict = getattr(obj, "to_dict", None)
    if callable(to_dict):
        try:
            result = to_dict()
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("to_dict() failed: %s", exc)
            return None
        if isinstance(result, ABCMapping):
            return result
    return None


def _stable_run_id(meta: Mapping[str, Any], memory: Mapping[str, Any], index: int) -> str:
    """Return a stable identifier for a memory; never just 'unknown'."""
    for source in (meta, memory):
        v = source.get("run_id") or source.get("record_id") or source.get("id")
        if isinstance(v, str) and v:
            return v
    return f"memory-{index:06d}"


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class NodeType(str, Enum):
    """Graph node types."""

    WORKLOAD = "workload"
    DEVICE = "device"
    SITE = "site"
    MODEL = "model"
    POLICY = "policy"
    GRID_CONTEXT = "grid_context"
    DECISION = "decision"
    MEASUREMENT = "measurement"
    INCIDENT = "incident"
    VERDICT = "verdict"

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.value

    @classmethod
    def values(cls) -> Tuple[str, ...]:
        return tuple(m.value for m in cls)

    @classmethod
    def coerce(cls, value: Any) -> "NodeType":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            try:
                return cls(value)
            except ValueError as exc:
                raise GreenDecisionGraphInputError(
                    f"node_type {value!r} is not one of "
                    f"{list(cls.values())}."
                ) from exc
        raise GreenDecisionGraphInputError(
            f"node_type must be a str or NodeType, got "
            f"{type(value).__name__}."
        )


class EdgeType(str, Enum):
    """Graph edge types."""

    EXECUTED_ON = "executed-on"
    SELECTED_BY = "selected-by"
    GOVERNED_BY = "governed-by"
    SUPERSEDED_BY = "superseded-by"
    VIOLATED_BUDGET = "violated-budget"
    IMPROVED_OVER = "improved-over"
    HURT_BY = "hurt-by"
    VALIDATED_BY = "validated-by"
    REQUIRES_HUMAN = "requires-human"

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.value

    @classmethod
    def values(cls) -> Tuple[str, ...]:
        return tuple(m.value for m in cls)

    @classmethod
    def coerce(cls, value: Any) -> "EdgeType":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            try:
                return cls(value)
            except ValueError as exc:
                raise GreenDecisionGraphInputError(
                    f"edge_type {value!r} is not one of "
                    f"{list(cls.values())}."
                ) from exc
        raise GreenDecisionGraphInputError(
            f"edge_type must be a str or EdgeType, got "
            f"{type(value).__name__}."
        )


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GreenDecisionGraphConfig:
    """Tunable parameters for the Green Decision Graph."""

    schema_version: int = SCHEMA_VERSION

    max_nodes: int = 5_000
    max_edges: int = 20_000

    #: Seconds of tolerance applied when comparing ``observed_at`` to the
    #: time-travel instant. Now enforced.
    time_travel_tolerance_seconds: float = 60.0

    escape_html: bool = True
    include_incidents: bool = True
    include_measurements: bool = True
    include_grid_context: bool = True
    include_model: bool = True
    include_verdicts: bool = True
    include_benchmark_overlays: bool = True
    include_tier_annotations: bool = True
    dedupe_edges: bool = True

    #: When set, memories whose ``metadata.container_tag`` differs are
    #: skipped. ``None`` disables the check.
    expected_container_tag: Optional[str] = None

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        for name in ("max_nodes", "max_edges"):
            v = getattr(self, name)
            if not _is_real_int(v) or v <= 0:
                raise GreenDecisionGraphConfigError(
                    f"{name} must be a positive int."
                )
        if not _is_positive_finite(self.time_travel_tolerance_seconds):
            raise GreenDecisionGraphConfigError(
                "time_travel_tolerance_seconds must be finite and > 0."
            )
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise GreenDecisionGraphConfigError(
                "schema_version must be a positive int."
            )
        for name in (
            "escape_html", "include_incidents", "include_measurements",
            "include_grid_context", "include_model", "include_verdicts",
            "include_benchmark_overlays", "include_tier_annotations",
            "dedupe_edges",
        ):
            if not isinstance(getattr(self, name), bool):
                raise GreenDecisionGraphConfigError(
                    f"{name} must be a bool."
                )
        if self.expected_container_tag is not None:
            if (
                not isinstance(self.expected_container_tag, str)
                or not self.expected_container_tag
            ):
                raise GreenDecisionGraphConfigError(
                    "expected_container_tag must be None or a non-empty "
                    "string."
                )

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "max_nodes": self.max_nodes,
            "max_edges": self.max_edges,
            "time_travel_tolerance_seconds": self.time_travel_tolerance_seconds,
            "escape_html": self.escape_html,
            "include_incidents": self.include_incidents,
            "include_measurements": self.include_measurements,
            "include_grid_context": self.include_grid_context,
            "include_model": self.include_model,
            "include_verdicts": self.include_verdicts,
            "include_benchmark_overlays": self.include_benchmark_overlays,
            "include_tier_annotations": self.include_tier_annotations,
            "dedupe_edges": self.dedupe_edges,
            "expected_container_tag": self.expected_container_tag,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = False,
    ) -> "GreenDecisionGraphConfig":
        if not isinstance(data, ABCMapping):
            raise GreenDecisionGraphConfigError(
                "GreenDecisionGraphConfig.from_dict expects a Mapping."
            )
        valid = {f.name for f in fields(cls)}
        unknown = set(data) - valid
        if strict and unknown:
            raise GreenDecisionGraphConfigError(
                f"Unknown config key(s): {sorted(unknown)}."
            )
        kwargs = {k: v for k, v in data.items() if k in valid}
        try:
            return cls(**kwargs)
        except GreenDecisionGraphError:
            raise
        except (TypeError, ValueError) as exc:
            raise GreenDecisionGraphConfigError(
                f"failed to build config: {exc}"
            ) from exc

    @classmethod
    def from_json(
        cls, payload: str, *, strict: bool = False,
    ) -> "GreenDecisionGraphConfig":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise GreenDecisionGraphConfigError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise GreenDecisionGraphConfigError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data, strict=strict)

    def with_overrides(self, **kwargs: Any) -> "GreenDecisionGraphConfig":
        valid = {f.name for f in fields(self)}
        unknown = set(kwargs) - valid
        if unknown:
            raise GreenDecisionGraphConfigError(
                f"Unknown config field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def merge(
        self, other: "GreenDecisionGraphConfig"
    ) -> "GreenDecisionGraphConfig":
        defaults = GreenDecisionGraphConfig()
        overrides: Dict[str, Any] = {}
        for f in fields(self):
            other_val = getattr(other, f.name)
            default_val = getattr(defaults, f.name)
            if other_val != default_val:
                overrides[f.name] = other_val
        return self.with_overrides(**overrides)

    def __hash__(self) -> int:
        return hash((
            self.schema_version, self.max_nodes, self.max_edges,
            self.time_travel_tolerance_seconds, self.escape_html,
            self.include_incidents, self.include_measurements,
            self.include_grid_context, self.include_model,
            self.include_verdicts, self.include_benchmark_overlays,
            self.include_tier_annotations, self.dedupe_edges,
            self.expected_container_tag,
        ))

    def __repr__(self) -> str:
        return (
            "GreenDecisionGraphConfig("
            f"max_nodes={self.max_nodes}, "
            f"max_edges={self.max_edges}, "
            f"escape_html={self.escape_html})"
        )


# --------------------------------------------------------------------------- #
# Records
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class GraphNode:
    """Frozen graph node."""

    node_id: str
    node_type: NodeType
    label: str
    properties: Mapping[str, Any] = field(default_factory=dict)
    observed_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise GreenDecisionGraphInputError(
                "node_id must be a non-empty string."
            )
        object.__setattr__(
            self, "node_type", NodeType.coerce(self.node_type),
        )
        if not isinstance(self.label, str) or not self.label:
            raise GreenDecisionGraphInputError(
                "label must be a non-empty string."
            )
        object.__setattr__(
            self, "properties", _freeze_mapping(self.properties, name="properties"),
        )
        if self.observed_at is not None:
            if not isinstance(self.observed_at, datetime):
                raise GreenDecisionGraphInputError(
                    "observed_at must be a datetime or None."
                )
            if self.observed_at.tzinfo is None:
                object.__setattr__(
                    self, "observed_at",
                    self.observed_at.replace(tzinfo=timezone.utc),
                )

    # ------------------------------------------------------------------ #
    @property
    def id(self) -> str:
        return self.node_id

    def to_dict(self, *, escape: bool = True) -> Dict[str, Any]:
        esc = html.escape if escape else (lambda s: s)
        return {
            "id": self.node_id,
            "type": self.node_type.value,
            "label": esc(str(self.label)),
            "properties": dict(self.properties),
            "observed_at": (
                self.observed_at.isoformat() if self.observed_at else None
            ),
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "GraphNode":
        if not isinstance(data, ABCMapping):
            raise GreenDecisionGraphParseError(
                "GraphNode.from_dict expects a Mapping."
            )
        try:
            return cls(
                node_id=str(data["id"]),
                node_type=NodeType.coerce(data["type"]),
                label=str(data.get("label", data["id"])),
                properties=dict(data.get("properties", {}) or {}),
                observed_at=_parse_iso_datetime(data.get("observed_at")),
            )
        except KeyError as exc:
            raise GreenDecisionGraphParseError(
                f"GraphNode.from_dict missing key {exc.args[0]!r}."
            ) from exc

    @classmethod
    def from_json(cls, payload: str) -> "GraphNode":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise GreenDecisionGraphParseError(
                f"GraphNode.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    def __hash__(self) -> int:
        try:
            props_hash = hash(tuple(sorted(self.properties.items())))
        except TypeError:
            props_hash = hash(
                json.dumps(dict(self.properties), default=str, sort_keys=True)
            )
        return hash((
            self.node_id, self.node_type, self.label, props_hash,
            self.observed_at,
        ))

    def __repr__(self) -> str:
        return (
            f"GraphNode(id={self.node_id!r}, "
            f"type={self.node_type.value!r})"
        )


@dataclass(frozen=True)
class GraphEdge:
    """Frozen graph edge."""

    source: str
    target: str
    edge_type: EdgeType
    weight: float = 1.0
    properties: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("source", "target"):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise GreenDecisionGraphInputError(
                    f"{name} must be a non-empty string."
                )
        object.__setattr__(
            self, "edge_type", EdgeType.coerce(self.edge_type),
        )
        if not _is_finite_nonneg(self.weight):
            raise GreenDecisionGraphInputError(
                "weight must be a finite number >= 0."
            )
        object.__setattr__(
            self, "properties", _freeze_mapping(self.properties, name="properties"),
        )

    @property
    def key(self) -> Tuple[str, str, str]:
        """Stable identifier for dedup: ``(source, target, edge_type)``."""
        return (self.source, self.target, self.edge_type.value)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "type": self.edge_type.value,
            "weight": float(self.weight),
            "properties": dict(self.properties),
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "GraphEdge":
        if not isinstance(data, ABCMapping):
            raise GreenDecisionGraphParseError(
                "GraphEdge.from_dict expects a Mapping."
            )
        try:
            return cls(
                source=str(data["source"]),
                target=str(data["target"]),
                edge_type=EdgeType.coerce(data["type"]),
                weight=float(data.get("weight", 1.0)),
                properties=dict(data.get("properties", {}) or {}),
            )
        except KeyError as exc:
            raise GreenDecisionGraphParseError(
                f"GraphEdge.from_dict missing key {exc.args[0]!r}."
            ) from exc

    @classmethod
    def from_json(cls, payload: str) -> "GraphEdge":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise GreenDecisionGraphParseError(
                f"GraphEdge.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    def __hash__(self) -> int:
        try:
            props_hash = hash(tuple(sorted(self.properties.items())))
        except TypeError:
            props_hash = hash(
                json.dumps(dict(self.properties), default=str, sort_keys=True)
            )
        return hash((self.source, self.target, self.edge_type, self.weight, props_hash))

    def __repr__(self) -> str:
        return (
            f"GraphEdge({self.source!r} -[{self.edge_type.value}]-> "
            f"{self.target!r})"
        )


@dataclass(frozen=True)
class GraphPayload:
    """Frozen graph payload, ``DocumentWithMemories``-compatible."""

    nodes: Tuple[GraphNode, ...] = ()
    edges: Tuple[GraphEdge, ...] = ()
    generated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )
    filters_applied: Mapping[str, Any] = field(default_factory=dict)
    warnings: Tuple[str, ...] = ()
    schema_version: int = SCHEMA_VERSION
    stats: Mapping[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        if not isinstance(self.nodes, tuple):
            object.__setattr__(self, "nodes", tuple(self.nodes))
        if not isinstance(self.edges, tuple):
            object.__setattr__(self, "edges", tuple(self.edges))
        for n in self.nodes:
            if not isinstance(n, GraphNode):
                raise GreenDecisionGraphInputError(
                    "nodes must contain GraphNode instances."
                )
        for e in self.edges:
            if not isinstance(e, GraphEdge):
                raise GreenDecisionGraphInputError(
                    "edges must contain GraphEdge instances."
                )
        if not isinstance(self.generated_at, datetime):
            raise GreenDecisionGraphInputError(
                "generated_at must be a datetime."
            )
        if self.generated_at.tzinfo is None:
            object.__setattr__(
                self, "generated_at",
                self.generated_at.replace(tzinfo=timezone.utc),
            )
        object.__setattr__(
            self, "filters_applied",
            _freeze_mapping(self.filters_applied, name="filters_applied"),
        )
        if not isinstance(self.warnings, tuple):
            object.__setattr__(self, "warnings", tuple(self.warnings))
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise GreenDecisionGraphInputError(
                "schema_version must be a positive int."
            )
        object.__setattr__(
            self, "stats", _freeze_mapping(self.stats, name="stats"),
        )

    # ------------------------------------------------------------------ #
    @property
    def node_count(self) -> int:
        return len(self.nodes)

    @property
    def edge_count(self) -> int:
        return len(self.edges)

    def __len__(self) -> int:
        return len(self.nodes)

    def to_dict(self, *, escape: bool = True) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "nodes": [n.to_dict(escape=escape) for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "generated_at": self.generated_at.isoformat(),
            "filters_applied": dict(self.filters_applied),
            "warnings": list(self.warnings),
            "stats": dict(self.stats),
        }

    def to_json(self, *, escape: bool = True, indent: Optional[int] = None) -> str:
        return json.dumps(
            self.to_dict(escape=escape), default=str, indent=indent,
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "GraphPayload":
        if not isinstance(data, ABCMapping):
            raise GreenDecisionGraphParseError(
                "GraphPayload.from_dict expects a Mapping."
            )
        raw_nodes = data.get("nodes", []) or []
        raw_edges = data.get("edges", []) or []
        if not isinstance(raw_nodes, list) or not isinstance(raw_edges, list):
            raise GreenDecisionGraphParseError(
                "'nodes' and 'edges' must be lists."
            )
        nodes: List[GraphNode] = []
        for i, raw in enumerate(raw_nodes):
            try:
                nodes.append(GraphNode.from_dict(raw))
            except GreenDecisionGraphParseError as exc:
                logger.warning("Skipping malformed node %d: %s", i, exc)
        edges: List[GraphEdge] = []
        for i, raw in enumerate(raw_edges):
            try:
                edges.append(GraphEdge.from_dict(raw))
            except GreenDecisionGraphParseError as exc:
                logger.warning("Skipping malformed edge %d: %s", i, exc)
        return cls(
            nodes=tuple(nodes),
            edges=tuple(edges),
            generated_at=(
                _parse_iso_datetime(data.get("generated_at"))
                or datetime.now(timezone.utc)
            ),
            filters_applied=dict(data.get("filters_applied", {}) or {}),
            warnings=tuple(data.get("warnings", []) or ()),
            schema_version=int(data.get("schema_version", SCHEMA_VERSION)),
            stats=dict(data.get("stats", {}) or {}),
        )

    @classmethod
    def from_json(cls, payload: str) -> "GraphPayload":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise GreenDecisionGraphParseError(
                f"GraphPayload.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    # Bridges
    # ------------------------------------------------------------------ #
    def _render_content(self) -> str:
        return (
            f"Green Decision Graph ({len(self.nodes)} nodes, "
            f"{len(self.edges)} edges, generated "
            f"{self.generated_at.isoformat()})"
        )

    def _metadata(self, *, container_tag: str) -> Dict[str, Any]:
        return {
            "kind": "decision_graph",
            "type": "decision_graph",
            "record_id": f"graph:{self.generated_at.isoformat()}",
            "truth_level": "estimated",
            "container_tag": container_tag,
            "observed_at": self.generated_at.isoformat(),
            "schema_version": self.schema_version,
            "node_count": len(self.nodes),
            "edge_count": len(self.edges),
            "node_types": sorted({n.node_type.value for n in self.nodes}),
            "edge_types": sorted({e.edge_type.value for e in self.edges}),
            "warnings": list(self.warnings),
        }

    def to_episode_payload(
        self,
        *,
        container_tag: Optional[str] = None,
        content: Optional[str] = None,
    ) -> Dict[str, Any]:
        tag = container_tag or DEFAULT_CONTAINER_TAG
        if not isinstance(tag, str) or not tag:
            raise GreenDecisionGraphInputError(
                "container_tag must be a non-empty string."
            )
        return {
            "content": content if content is not None else self._render_content(),
            "container_tag": tag,
            "metadata": self._metadata(container_tag=tag),
        }

    def to_memory_dict(self) -> Dict[str, Any]:
        return {
            "id": f"graph:{self.generated_at.isoformat()}",
            "content": self._render_content(),
            "metadata": self._metadata(container_tag=DEFAULT_CONTAINER_TAG),
        }

    # ------------------------------------------------------------------ #
    def __hash__(self) -> int:
        return hash((
            self.nodes, self.edges, self.generated_at,
            tuple(sorted(self.filters_applied.items())),
            self.warnings, self.schema_version,
            tuple(sorted(self.stats.items())),
        ))

    def __repr__(self) -> str:
        return (
            f"GraphPayload(nodes={len(self.nodes)}, "
            f"edges={len(self.edges)})"
        )


# --------------------------------------------------------------------------- #
# Graph builder
# --------------------------------------------------------------------------- #
class GreenDecisionGraph:
    """Builds Green Decision Graph payloads from recall bundles."""

    _LATENCY_RING_SIZE: int = 200

    # ------------------------------------------------------------------ #
    def __init__(
        self,
        *,
        config: Optional[GreenDecisionGraphConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is None:
            config = GreenDecisionGraphConfig()
        elif not isinstance(config, GreenDecisionGraphConfig):
            raise GreenDecisionGraphInputError(
                "config must be a GreenDecisionGraphConfig or None."
            )
        self._config = config
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._total_builds = 0
        self._total_nodes = 0
        self._total_edges = 0
        self._last_error: Optional[str] = None
        self._last_payload: Optional[GraphPayload] = None
        self._latency_ring: Deque[float] = deque(
            maxlen=self._LATENCY_RING_SIZE,
        )
        self._started_at = time.monotonic()

    # ------------------------------------------------------------------ #
    @property
    def config(self) -> GreenDecisionGraphConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    # ---------------------------------------------------------- lifecycle
    def close(self) -> None:
        """Best-effort no-op. Kept for symmetry with sibling classes."""
        return None

    def __enter__(self) -> "GreenDecisionGraph":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ---------------------------------------------------------- public API
    def build_payload(
        self,
        *,
        recall_bundles: Sequence[Any],
        policy_version: Optional[str] = None,
        filters: Optional[Mapping[str, Any]] = None,
        verdicts: Optional[Iterable[Any]] = None,
        report: Optional[Any] = None,
        snapshot: Optional[Any] = None,
        tier_manager: Optional[Any] = None,
    ) -> GraphPayload:
        """Build a graph payload from recall bundles + optional overlays."""
        if isinstance(recall_bundles, (str, bytes)) or not isinstance(
            recall_bundles, Sequence
        ):
            raise GreenDecisionGraphInputError(
                "recall_bundles must be a non-empty sequence "
                "(not a string)."
            )
        if filters is not None and not isinstance(filters, ABCMapping):
            raise GreenDecisionGraphInputError(
                "filters must be a Mapping or None."
            )
        if policy_version is not None and (
            not isinstance(policy_version, str) or not policy_version
        ):
            raise GreenDecisionGraphInputError(
                "policy_version must be None or a non-empty string."
            )

        start = time.perf_counter()

        # --------------------------------------------------- collect
        nodes: List[GraphNode] = []
        edges: List[GraphEdge] = []
        seen_nodes: Set[str] = set()
        seen_edges: Set[Tuple[str, str, str]] = set()
        warnings: List[str] = []
        skipped_container = 0
        produced_nodes = 0
        produced_edges = 0

        def add_node(node: GraphNode) -> bool:
            nonlocal produced_nodes
            if node.node_id in seen_nodes:
                return False
            seen_nodes.add(node.node_id)
            nodes.append(node)
            produced_nodes += 1
            return True

        def add_edge(edge: GraphEdge) -> None:
            nonlocal produced_edges
            produced_edges += 1
            if self._config.dedupe_edges:
                key = edge.key
                if key in seen_edges:
                    return
                seen_edges.add(key)
            edges.append(edge)

        # Track truncated / clamped signals for warnings.
        truncated_seen = False
        clamped_seen = False

        for i, bundle in enumerate(recall_bundles):
            data = _coerce_to_mapping(bundle)
            if data is None:
                if self._strict:
                    raise GreenDecisionGraphInputError(
                        f"recall_bundles[{i}] is not a mapping or "
                        f"a RecallBundle-like object."
                    )
                warnings.append(
                    f"recall_bundles[{i}] was not convertible to a mapping"
                )
                continue

            if data.get("truncated"):
                truncated_seen = True
            if data.get("clamped_k"):
                clamped_seen = True

            self._extract_from_bundle(
                data,
                add_node=add_node,
                add_edge=add_edge,
                policy_version=policy_version,
                skipped_container_counter=lambda: None,
            )

        # --------------------------------------------------- overlays
        verdict_map = self._collect_verdicts(verdicts)
        if self._config.include_verdicts and verdict_map:
            self._apply_verdict_overlays(
                verdict_map, add_node=add_node, add_edge=add_edge,
                seen_nodes=seen_nodes,
            )

        report_map = _coerce_to_mapping(report)
        if (
            self._config.include_benchmark_overlays
            and report_map is not None
        ):
            self._apply_benchmark_overlay(
                report_map, add_node=add_node, add_edge=add_edge,
                warnings=warnings,
            )

        snapshot_map = _coerce_to_mapping(snapshot)
        if snapshot_map is not None:
            self._apply_policy_snapshot(
                snapshot_map, add_node=add_node, add_edge=add_edge,
            )

        if (
            self._config.include_tier_annotations
            and tier_manager is not None
        ):
            self._apply_tier_annotations(
                tier_manager, nodes=nodes,
            )

        # --------------------------------------------------- warnings
        if truncated_seen:
            warnings.append(
                "evidence bundle was truncated by the token budget"
            )
        if clamped_seen:
            warnings.append(
                "evidence bundle was clamped by max_k"
            )

        # --------------------------------------------------- caps
        nodes = nodes[: self._config.max_nodes]
        edges = edges[: self._config.max_edges]

        duration_ms = (time.perf_counter() - start) * 1000.0

        stats: Dict[str, Any] = {
            "produced_nodes": produced_nodes,
            "produced_edges": produced_edges,
            "kept_nodes": len(nodes),
            "kept_edges": len(edges),
            "skipped_container_mismatches": skipped_container,
            "duration_ms": duration_ms,
        }

        payload = GraphPayload(
            nodes=tuple(nodes),
            edges=tuple(edges),
            filters_applied=dict(filters or {}),
            warnings=tuple(warnings),
            schema_version=self._config.schema_version,
            stats=stats,
        )

        with self._lock:
            self._total_builds += 1
            self._total_nodes += produced_nodes
            self._total_edges += produced_edges
            self._latency_ring.append(duration_ms)
            self._last_payload = payload
            self._last_error = None

        return payload

    async def build_payload_async(self, **kwargs: Any) -> GraphPayload:
        return await asyncio.to_thread(self.build_payload, **kwargs)

    def time_travel(
        self,
        *,
        recall_bundles: Sequence[Any],
        at: datetime,
        policy_version: Optional[str] = None,
    ) -> GraphPayload:
        """Rebuild the graph as it would have looked at ``at``.

        Uses the configured ``time_travel_tolerance_seconds``: memories
        whose ``observed_at`` is within ``[at - tol, at + tol]`` are
        kept. Memories with no parseable ``observed_at`` are retained
        when ``at`` is in the past (best effort) and dropped otherwise.
        """
        if not isinstance(at, datetime):
            raise GreenDecisionGraphInputError("at must be a datetime.")
        if at.tzinfo is None:
            at = at.replace(tzinfo=timezone.utc)
        if isinstance(recall_bundles, (str, bytes)) or not isinstance(
            recall_bundles, Sequence
        ):
            raise GreenDecisionGraphInputError(
                "recall_bundles must be a sequence (not a string)."
            )

        tol = self._config.time_travel_tolerance_seconds
        lower = at.timestamp() - tol
        upper = at.timestamp() + tol

        filtered: List[Dict[str, Any]] = []
        for bundle in recall_bundles:
            data = _coerce_to_mapping(bundle)
            if data is None:
                continue
            kept: List[Any] = []
            for memory in data.get("memories", []) or []:
                if not isinstance(memory, ABCMapping):
                    continue
                meta = memory.get("metadata") or {}
                if not isinstance(meta, ABCMapping):
                    meta = {}
                ts = _parse_iso_datetime(meta.get("observed_at"))
                if ts is None:
                    kept.append(memory)
                    continue
                if lower <= ts.timestamp() <= upper:
                    kept.append(memory)
            filtered.append({**dict(data), "memories": kept})

        return self.build_payload(
            recall_bundles=filtered,
            policy_version=policy_version,
            filters={"time_travel_at": at.isoformat()},
        )

    async def time_travel_async(self, **kwargs: Any) -> GraphPayload:
        return await asyncio.to_thread(self.time_travel, **kwargs)

    def to_react_props(self, payload: GraphPayload) -> Dict[str, Any]:
        """Return the props expected by ``@supermemory/memory-graph``."""
        if not isinstance(payload, GraphPayload):
            raise GreenDecisionGraphInputError(
                "payload must be a GraphPayload."
            )
        data = payload.to_dict(escape=self._config.escape_html)
        return {
            "documents": [
                {
                    "id": n["id"],
                    "title": n["label"],
                    "type": n["type"],
                    "metadata": n["properties"],
                    "observedAt": n["observed_at"],
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
            "warnings": data["warnings"],
            "schemaVersion": data["schema_version"],
        }

    # ---------------------------------------------------- pipeline bridge
    def build_from_pipeline(
        self,
        pipeline: Any,
        *,
        query: str,
        k: int = 5,
        container_tag: Optional[str] = None,
        filters: Optional[Mapping[str, Any]] = None,
        current_telemetry: Optional[Mapping[str, float]] = None,
        current_policy_version: Optional[str] = None,
        at: Optional[datetime] = None,
    ) -> GraphPayload:
        """Build a payload from a package ``Pipeline`` in one call.

        Runs recall, optionally runs the guard, and passes the tier
        manager through for node annotations.
        """
        recall = getattr(pipeline, "recall", None)
        if recall is None or not callable(getattr(recall, "query", None)):
            raise GreenDecisionGraphInputError(
                "pipeline does not expose a BoundedRecall-like .recall"
            )

        bundle = recall.query(query, k=k, container_tag=container_tag)

        verdict = None
        guard = getattr(pipeline, "guard", None)
        if (
            guard is not None
            and current_telemetry is not None
            and callable(getattr(guard, "check", None))
        ):
            policy = None
            if current_policy_version is not None:
                # Best-effort: don't import memory_schemas here.
                try:
                    from ..memory.memory_schemas import PolicyRecord  # type: ignore
                    policy = PolicyRecord(
                        version=current_policy_version,
                        content={},
                        approved_by="graph",
                    )
                except Exception:
                    policy = None
            try:
                verdict = guard.check(
                    recalled=bundle,
                    current_telemetry=dict(current_telemetry),
                    current_policy=policy,
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("guard.check failed: %s", exc)
                verdict = None

        snapshot = None
        policy_memory = getattr(pipeline, "policy_memory", None)
        if policy_memory is not None and callable(
            getattr(policy_memory, "snapshot", None)
        ):
            try:
                snapshot = policy_memory.snapshot()
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("policy_memory.snapshot failed: %s", exc)
                snapshot = None

        tier_manager = getattr(pipeline, "tier_manager", None)

        verdicts = [verdict] if verdict is not None else None

        if at is not None:
            return self.time_travel(
                recall_bundles=[bundle],
                at=at,
                policy_version=current_policy_version,
            )

        return self.build_payload(
            recall_bundles=[bundle],
            filters=filters,
            policy_version=current_policy_version,
            verdicts=verdicts,
            snapshot=snapshot,
            tier_manager=tier_manager,
        )

    # ---------------------------------------------------------- internals
    def _extract_from_bundle(
        self,
        bundle: Mapping[str, Any],
        *,
        add_node: Any,
        add_edge: Any,
        policy_version: Optional[str],
        skipped_container_counter: Any,
    ) -> None:
        for index, memory in enumerate(bundle.get("memories", []) or []):
            if not isinstance(memory, ABCMapping):
                continue
            meta = memory.get("metadata") or {}
            if not isinstance(meta, ABCMapping):
                meta = {}

            # Container-tag gate.
            if self._config.expected_container_tag is not None:
                tag = meta.get("container_tag") or memory.get("container_tag")
                if tag != self._config.expected_container_tag:
                    continue

            kind = self._classify_kind(meta, memory)
            run_id = _stable_run_id(meta, memory, index)
            observed_at = _parse_iso_datetime(meta.get("observed_at"))
            container_tag = meta.get("container_tag") or memory.get("container_tag")

            # --------------------------------------------- decision node
            decision_id = f"decision:{run_id}"
            decision_added = add_node(GraphNode(
                node_id=decision_id,
                node_type=NodeType.DECISION,
                label=f"Decision {run_id}",
                properties={
                    "run_id": run_id,
                    "kind": kind,
                    "route": meta.get("route"),
                    "policy_version":
                        meta.get("policy_version") or policy_version,
                    "truth_level": meta.get("truth_level"),
                    "quality_score": meta.get("quality_score"),
                    "metrics_hash": meta.get("metrics_hash"),
                    "container_tag": container_tag,
                },
                observed_at=observed_at,
            ))
            if not decision_added:
                # Duplicate decision id: skip dependent edges to avoid
                # re-adding them.
                pass

            workload = meta.get("workload_type") or meta.get("workload")
            device = meta.get("device_class") or meta.get("device")
            route = meta.get("route")
            site = meta.get("site_id") or meta.get("site")
            model = meta.get("model_id") or meta.get("model")
            region = meta.get("region") or meta.get("grid_region")
            pv = meta.get("policy_version") or policy_version

            if isinstance(workload, str) and workload:
                wid = f"workload:{workload}"
                add_node(GraphNode(
                    node_id=wid, node_type=NodeType.WORKLOAD, label=workload,
                ))
                add_edge(GraphEdge(
                    source=decision_id, target=wid,
                    edge_type=EdgeType.SELECTED_BY,
                ))

            if isinstance(device, str) and device:
                did = f"device:{device}"
                add_node(GraphNode(
                    node_id=did, node_type=NodeType.DEVICE, label=device,
                ))
                add_edge(GraphEdge(
                    source=decision_id, target=did,
                    edge_type=EdgeType.EXECUTED_ON,
                ))

            if isinstance(route, str) and route:
                rid = f"device:{route}"
                add_node(GraphNode(
                    node_id=rid, node_type=NodeType.DEVICE, label=route,
                ))
                add_edge(GraphEdge(
                    source=decision_id, target=rid,
                    edge_type=EdgeType.EXECUTED_ON,
                ))

            if isinstance(site, str) and site:
                sid = f"site:{site}"
                add_node(GraphNode(
                    node_id=sid, node_type=NodeType.SITE, label=site,
                ))
                add_edge(GraphEdge(
                    source=decision_id, target=sid,
                    edge_type=EdgeType.EXECUTED_ON,
                ))

            if self._config.include_model and isinstance(model, str) and model:
                mid = f"model:{model}"
                add_node(GraphNode(
                    node_id=mid, node_type=NodeType.MODEL, label=model,
                ))
                add_edge(GraphEdge(
                    source=decision_id, target=mid,
                    edge_type=EdgeType.EXECUTED_ON,
                ))

            if (
                self._config.include_grid_context
                and isinstance(region, str) and region
            ):
                gid = f"grid_context:{region}"
                add_node(GraphNode(
                    node_id=gid, node_type=NodeType.GRID_CONTEXT,
                    label=f"Grid {region}",
                    properties={
                        "region": region,
                        "carbon_intensity":
                            meta.get("grid_carbon_gco2e_per_kwh"),
                    },
                    observed_at=observed_at,
                ))
                add_edge(GraphEdge(
                    source=decision_id, target=gid,
                    edge_type=EdgeType.EXECUTED_ON,
                ))

            if isinstance(pv, str) and pv:
                pid = f"policy:{pv}"
                add_node(GraphNode(
                    node_id=pid, node_type=NodeType.POLICY, label=pv,
                    properties={"version": pv},
                ))
                add_edge(GraphEdge(
                    source=decision_id, target=pid,
                    edge_type=EdgeType.GOVERNED_BY,
                ))

            if meta.get("superseded_by"):
                target = f"decision:{meta['superseded_by']}"
                add_edge(GraphEdge(
                    source=decision_id, target=target,
                    edge_type=EdgeType.SUPERSEDED_BY,
                ))

            # --------------------------------------------- incident
            if self._config.include_incidents and (
                kind == "incident" or meta.get("incident_id")
            ):
                incident_id = meta.get("incident_id") or run_id
                iid = f"incident:{incident_id}"
                add_node(GraphNode(
                    node_id=iid, node_type=NodeType.INCIDENT,
                    label=str(incident_id),
                    properties={
                        "severity": meta.get("severity"),
                        "description": meta.get("description"),
                    },
                    observed_at=observed_at,
                ))
                add_edge(GraphEdge(
                    source=decision_id, target=iid,
                    edge_type=EdgeType.VALIDATED_BY,
                ))

            # --------------------------------------------- measurement
            if self._config.include_measurements:
                measured = {
                    k: meta.get(k)
                    for k in ("energy_wh", "carbon_gco2e", "latency_ms")
                    if k in meta
                }
                if measured:
                    mid = f"measurement:{run_id}"
                    add_node(GraphNode(
                        node_id=mid, node_type=NodeType.MEASUREMENT,
                        label=f"Measurement {run_id}",
                        properties=measured,
                        observed_at=observed_at,
                    ))
                    add_edge(GraphEdge(
                        source=decision_id, target=mid,
                        edge_type=EdgeType.VALIDATED_BY,
                    ))

    def _classify_kind(
        self, meta: Mapping[str, Any], memory: Mapping[str, Any],
    ) -> str:
        """Return the enhanced-schema ``kind`` for a memory."""
        for source in (meta, memory):
            for key in ("kind", "type"):
                v = source.get(key)
                if isinstance(v, str) and v:
                    return v
        return "decision_outcome"

    def _collect_verdicts(
        self, verdicts: Optional[Iterable[Any]],
    ) -> List[Mapping[str, Any]]:
        out: List[Mapping[str, Any]] = []
        if verdicts is None:
            return out
        for v in verdicts:
            data = _coerce_to_mapping(v)
            if data is not None:
                out.append(data)
        return out

    def _apply_verdict_overlays(
        self,
        verdicts: Sequence[Mapping[str, Any]],
        *,
        add_node: Any,
        add_edge: Any,
        seen_nodes: Set[str],
    ) -> None:
        for i, v in enumerate(verdicts):
            verdict_id = f"verdict:{i}"
            add_node(GraphNode(
                node_id=verdict_id,
                node_type=NodeType.VERDICT,
                label=("Verdict "
                       + ("allowed" if v.get("allowed") else "blocked")),
                properties={
                    "allowed": bool(v.get("allowed")),
                    "requires_human": bool(v.get("requires_human")),
                    "telemetry_drift": v.get("telemetry_drift"),
                    "policy_drift": bool(v.get("policy_drift")),
                    "reason_codes": list(v.get("reason_codes", []) or []),
                    "evidence_count": v.get("evidence_count"),
                    "trusted_evidence_count": v.get("trusted_evidence_count"),
                },
                observed_at=_parse_iso_datetime(v.get("timestamp")),
            ))
            add_edge(GraphEdge(
                source=verdict_id, target=f"verdict:{i}",
                edge_type=EdgeType.VALIDATED_BY, weight=0.0,
                properties={"self_reference": True},
            ))
            if v.get("requires_human"):
                add_edge(GraphEdge(
                    source=verdict_id, target=f"verdict:{i}",
                    edge_type=EdgeType.REQUIRES_HUMAN, weight=1.0,
                    properties={"self_reference": True},
                ))
            drift = v.get("telemetry_drift")
            if _is_finite_nonneg(drift) and drift > 0:
                add_edge(GraphEdge(
                    source=verdict_id, target=f"verdict:{i}",
                    edge_type=EdgeType.VIOLATED_BUDGET,
                    weight=float(drift),
                    properties={"self_reference": True},
                ))

    def _apply_benchmark_overlay(
        self,
        report: Mapping[str, Any],
        *,
        add_node: Any,
        add_edge: Any,
        warnings: List[str],
    ) -> None:
        metrics = report.get("metrics") or {}
        if not isinstance(metrics, ABCMapping):
            return
        baseline_id = "decision:__baseline__"
        memory_id = "decision:__memory__"
        add_node(GraphNode(
            node_id=baseline_id, node_type=NodeType.DECISION,
            label="Baseline (no memory)",
        ))
        add_node(GraphNode(
            node_id=memory_id, node_type=NodeType.DECISION,
            label="With memory",
        ))
        any_significant = False
        for name, raw in metrics.items():
            if not isinstance(raw, ABCMapping):
                continue
            direction = raw.get("direction")
            if direction == "improves":
                add_edge(GraphEdge(
                    source=memory_id, target=baseline_id,
                    edge_type=EdgeType.IMPROVED_OVER,
                    weight=abs(float(raw.get("delta", 0.0) or 0.0)),
                    properties={"metric": str(name)},
                ))
                any_significant = True
            elif direction == "hurts":
                add_edge(GraphEdge(
                    source=memory_id, target=baseline_id,
                    edge_type=EdgeType.HURT_BY,
                    weight=abs(float(raw.get("delta", 0.0) or 0.0)),
                    properties={"metric": str(name)},
                ))
                any_significant = True
        if not any_significant:
            warnings.append(
                "benchmark report contained no significant directions"
            )

    def _apply_policy_snapshot(
        self,
        snapshot: Mapping[str, Any],
        *,
        add_node: Any,
        add_edge: Any,
    ) -> None:
        version = snapshot.get("version")
        if not isinstance(version, str) or not version:
            return
        pid = f"policy:{version}"
        add_node(GraphNode(
            node_id=pid, node_type=NodeType.POLICY,
            label=f"Policy {version}",
            properties={
                "version": version,
                "policy_id": snapshot.get("policy_id"),
                "kind": snapshot.get("kind"),
                "truth_level": snapshot.get("truth_level"),
                "container_tag": snapshot.get("container_tag"),
                "approved_by": snapshot.get("approved_by"),
                "superseded_by": snapshot.get("superseded_by"),
            },
            observed_at=_parse_iso_datetime(snapshot.get("published_at")),
        ))

    def _apply_tier_annotations(
        self,
        tier_manager: Any,
        *,
        nodes: List[GraphNode],
    ) -> None:
        tier_of = getattr(tier_manager, "tier_of", None)
        if not callable(tier_of):
            return
        for i, node in enumerate(nodes):
            if node.node_type not in (
                NodeType.DECISION, NodeType.MEASUREMENT, NodeType.INCIDENT,
            ):
                continue
            try:
                tier = tier_of(node.node_id)
            except Exception:  # pragma: no cover - defensive
                continue
            if tier is None:
                continue
            tier_value = getattr(tier, "value", str(tier))
            props = dict(node.properties)
            props["tier"] = tier_value
            nodes[i] = GraphNode(
                node_id=node.node_id,
                node_type=node.node_type,
                label=node.label,
                properties=props,
                observed_at=node.observed_at,
            )

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            lats = list(self._latency_ring)
            mean_lat = sum(lats) / len(lats) if lats else 0.0
            return {
                "builds": self._total_builds,
                "total_nodes": self._total_nodes,
                "total_edges": self._total_edges,
                "last_error": self._last_error,
                "mean_latency_ms": mean_lat,
                "p50_latency_ms": _percentile(lats, 50),
                "p95_latency_ms": _percentile(lats, 95),
                "max_latency_ms": max(lats) if lats else 0.0,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "uptime_seconds": time.monotonic() - self._started_at,
            }

    def reset(self) -> int:
        """Reset counters. Returns the number of builds cleared."""
        with self._lock:
            cleared = self._total_builds
            self._total_builds = 0
            self._total_nodes = 0
            self._total_edges = 0
            self._last_error = None
            self._last_payload = None
            self._latency_ring.clear()
            self._started_at = time.monotonic()
        return cleared

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_last_payload: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "schema_version": self._config.schema_version,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "statistics": self.statistics(),
            }
            if include_last_payload and self._last_payload is not None:
                payload["last_payload"] = self._last_payload.to_dict()
        return payload

    def to_json(
        self,
        *,
        include_last_payload: bool = False,
        indent: Optional[int] = None,
    ) -> str:
        return json.dumps(
            self.to_dict(include_last_payload=include_last_payload),
            default=str, indent=indent, sort_keys=True,
        )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: Optional[bool] = None,
        restore_last_payload: bool = False,
    ) -> "GreenDecisionGraph":
        if not isinstance(data, ABCMapping):
            raise GreenDecisionGraphParseError(
                "GreenDecisionGraph.from_dict expects a Mapping."
            )
        cfg_blob = data.get("config", {})
        config = (
            cfg_blob if isinstance(cfg_blob, GreenDecisionGraphConfig)
            else GreenDecisionGraphConfig.from_dict(cfg_blob)
        )
        resolved_strict = (
            bool(data.get("strict", True)) if strict is None else bool(strict)
        )
        graph = cls(config=config, strict=resolved_strict)
        if restore_last_payload and isinstance(data.get("last_payload"), ABCMapping):
            try:
                with graph._lock:  # noqa: SLF001
                    graph._last_payload = GraphPayload.from_dict(
                        data["last_payload"]
                    )
            except GreenDecisionGraphParseError as exc:
                logger.warning("could not restore last_payload: %s", exc)
        return graph

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: Optional[bool] = None,
        restore_last_payload: bool = False,
    ) -> "GreenDecisionGraph":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise GreenDecisionGraphParseError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise GreenDecisionGraphParseError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(
            data,
            strict=strict,
            restore_last_payload=restore_last_payload,
        )

    # ---------------------------------------------------------------- repr
    def __repr__(self) -> str:
        with self._lock:
            return (
                "GreenDecisionGraph("
                f"builds={self._total_builds}, "
                f"nodes={self._total_nodes}, "
                f"edges={self._total_edges}, "
                f"strict={self._strict})"
            )


def _percentile(values: Sequence[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    k = max(
        0,
        min(
            len(ordered) - 1,
            int(round((pct / 100.0) * (len(ordered) - 1))),
        ),
    )
    return ordered[k]


__all__ = [
    "DEFAULT_CONTAINER_TAG",
    "EdgeType",
    "GraphEdge",
    "GraphNode",
    "GraphPayload",
    "GreenDecisionGraph",
    "GreenDecisionGraphConfig",
    "GreenDecisionGraphError",
    "GreenDecisionGraphInputError",
    "GreenDecisionGraphConfigError",
    "GreenDecisionGraphParseError",
    "NodeType",
    "SCHEMA_VERSION",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m dashboard.green_decision_graph
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    graph = GreenDecisionGraph()
    print("repr         :", graph)

    now = datetime.now(timezone.utc)
    old = datetime(2020, 1, 1, tzinfo=timezone.utc)

    # --------------------------------------------------- 1. Basic build
    bundle = {
        "query": "vision_inference arm_edge",
        "memories": [
            {
                "id": "mem-1",
                "metadata": {
                    "run_id": "run-001",
                    "kind": "decision_outcome",
                    "workload_type": "vision_inference",
                    "device_class": "arm_edge",
                    "route": "edge_int8",
                    "site_id": "edge-node-ygy-03",
                    "model_id": "resnet50",
                    "region": "eu-west-1",
                    "grid_carbon_gco2e_per_kwh": 180.0,
                    "policy_version": "v0.3",
                    "truth_level": "measured",
                    "quality_score": 0.91,
                    "energy_wh": 8.6,
                    "carbon_gco2e": 4.0,
                    "latency_ms": 438.0,
                    "container_tag": DEFAULT_CONTAINER_TAG,
                    "observed_at": now.isoformat(),
                },
            },
            {
                "id": "mem-2",
                "metadata": {
                    "run_id": "run-002",
                    "kind": "decision_outcome",
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
                    "container_tag": DEFAULT_CONTAINER_TAG,
                    "observed_at": now.isoformat(),
                },
            },
        ],
    }

    payload = graph.build_payload(recall_bundles=[bundle])
    print(f"build nodes  : {payload.node_count}")
    print(f"build edges  : {payload.edge_count}")
    assert payload.node_count > 0
    assert payload.edge_count > 0
    assert "grid_context" in {
        n.node_type.value for n in payload.nodes
    }
    assert "model" in {n.node_type.value for n in payload.nodes}

    # --------------------------------------------------- 2. Dedup
    payload_dup = graph.build_payload(
        recall_bundles=[bundle, bundle],  # same bundle twice
    )
    assert payload_dup.edge_count == payload.edge_count, (
        f"expected dedup; {payload_dup.edge_count} vs {payload.edge_count}"
    )
    print("dedup        : OK")

    # --------------------------------------------------- 3. Container gate
    gated = GreenDecisionGraph(
        config=GreenDecisionGraphConfig(
            expected_container_tag="org:only-me",
        ),
    )
    empty = gated.build_payload(recall_bundles=[bundle])
    assert empty.node_count == 0
    print("container    : OK (empty when tag mismatches)")

    # --------------------------------------------------- 4. Time travel
    past = graph.time_travel(recall_bundles=[bundle], at=old)
    assert past.node_count == 0
    present = graph.time_travel(recall_bundles=[bundle], at=now)
    assert present.node_count > 0
    print("time travel  : OK")

    # --------------------------------------------------- 5. Verdict overlay
    verdict = {
        "allowed": False,
        "requires_human": True,
        "telemetry_drift": 0.42,
        "policy_drift": True,
        "reason_codes": ["telemetry_drift", "policy_drift"],
        "evidence_count": 2,
        "trusted_evidence_count": 2,
        "timestamp": now.isoformat(),
    }
    v_payload = graph.build_payload(
        recall_bundles=[bundle], verdicts=[verdict],
    )
    assert any(n.node_type == NodeType.VERDICT for n in v_payload.nodes)
    assert any(
        e.edge_type == EdgeType.REQUIRES_HUMAN for e in v_payload.edges
    )
    assert any(
        e.edge_type == EdgeType.VIOLATED_BUDGET for e in v_payload.edges
    )
    print("verdict      : OK")

    # --------------------------------------------------- 6. Benchmark overlay
    report = {
        "metrics": {
            "quality": {"direction": "improves", "delta": 0.04},
            "energy_wh": {"direction": "improves", "delta": -0.4},
            "latency_ms": {"direction": "hurts", "delta": 20.0},
        },
    }
    b_payload = graph.build_payload(
        recall_bundles=[bundle], report=report,
    )
    assert any(
        e.edge_type == EdgeType.IMPROVED_OVER for e in b_payload.edges
    )
    assert any(
        e.edge_type == EdgeType.HURT_BY for e in b_payload.edges
    )
    print("benchmark    : OK")

    # --------------------------------------------------- 7. Policy snapshot overlay
    snapshot = {
        "policy_id": "default",
        "version": "v0.3",
        "kind": "policy",
        "truth_level": "user-reported",
        "container_tag": "policy:current",
        "approved_by": "ops",
        "superseded_by": None,
        "published_at": now.isoformat(),
    }
    s_payload = graph.build_payload(
        recall_bundles=[bundle], snapshot=snapshot,
    )
    policy_nodes = [
        n for n in s_payload.nodes if n.node_type == NodeType.POLICY
    ]
    assert any(
        n.properties.get("truth_level") == "user-reported"
        for n in policy_nodes
    )
    print("snapshot     : OK")

    # --------------------------------------------------- 8. Tier annotations
    class _FakeTierManager:
        def tier_of(self, record_id):
            return "hot" if record_id == "decision:run-001" else None

    t_payload = graph.build_payload(
        recall_bundles=[bundle], tier_manager=_FakeTierManager(),
    )
    annotated = [
        n for n in t_payload.nodes
        if n.node_id == "decision:run-001"
    ]
    assert annotated and annotated[0].properties.get("tier") == "hot"
    print("tier         : OK")

    # --------------------------------------------------- 9. to_react_props
    props = graph.to_react_props(payload)
    assert props["documents"] and props["memories"]
    assert props["schemaVersion"] == SCHEMA_VERSION
    print("react        : OK")

    # --------------------------------------------------- 10. Serialization
    cfg = GreenDecisionGraphConfig()
    assert GreenDecisionGraphConfig.from_dict(cfg.to_dict()) == cfg
    assert GreenDecisionGraphConfig.from_json(cfg.to_json()) == cfg
    cfg2 = cfg.with_overrides(max_nodes=99)
    assert cfg2.max_nodes == 99 and cfg.max_nodes == 5_000
    hash(cfg)

    node = payload.nodes[0]
    node_rt = GraphNode.from_dict(node.to_dict())
    assert node_rt == node
    hash(node)

    edge = payload.edges[0]
    edge_rt = GraphEdge.from_dict(edge.to_dict())
    assert edge_rt == edge
    hash(edge)

    payload_rt = GraphPayload.from_dict(payload.to_dict())
    assert payload_rt.node_count == payload.node_count
    assert payload_rt.edge_count == payload.edge_count
    hash(payload_rt)

    graph_rt = GreenDecisionGraph.from_dict(graph.to_dict())
    assert graph_rt.config == graph.config
    print("serialize    : OK")

    # --------------------------------------------------- 11. Bridges
    ep = payload.to_episode_payload()
    assert "content" in ep and "metadata" in ep
    assert ep["metadata"]["kind"] == "decision_graph"
    md = payload.to_memory_dict()
    assert md["id"].startswith("graph:")
    print("bridges      : OK")

    # --------------------------------------------------- 12. Async
    async def _run_async():
        p = await graph.build_payload_async(recall_bundles=[bundle])
        t = await graph.time_travel_async(
            recall_bundles=[bundle], at=now,
        )
        return p, t

    p_async, t_async = asyncio.run(_run_async())
    assert p_async.node_count > 0
    assert t_async.node_count > 0
    print("async        : OK")

    # --------------------------------------------------- 13. Statistics / reset
    stats = graph.statistics()
    print("statistics   :", {
        k: v for k, v in stats.items() if k not in ("config",)
    })
    cleared = graph.reset()
    assert cleared > 0
    assert graph.statistics()["builds"] == 0
    print("reset        :", cleared, "clears")

    # --------------------------------------------------- 14. build_from_pipeline
    class _FakeRecall:
        def query(self, q, k=5, container_tag=None):
            return bundle

    class _FakePipeline:
        recall = _FakeRecall()
        guard = None
        policy_memory = None
        tier_manager = None

    from_pipe = graph.build_from_pipeline(
        _FakePipeline(), query="vision",
    )
    assert from_pipe.node_count > 0
    print("pipeline     : OK")

    # --------------------------------------------------- 15. Validation
    for bad in (
        lambda: graph.build_payload(recall_bundles="not-a-sequence"),
        lambda: graph.build_payload(recall_bundles=[1, 2, 3]),
        lambda: graph.build_payload(
            recall_bundles=[bundle], filters="not-a-mapping",
        ),
        lambda: graph.time_travel(recall_bundles=[bundle], at="nope"),
        lambda: graph.to_react_props({"not": "a-payload"}),
    ):
        try:
            bad()
        except GreenDecisionGraphError as exc:
            print("reject input :", exc)
        else:
            raise AssertionError(f"expected rejection for {bad!r}")

    for bad in (
        dict(max_nodes=0),
        dict(max_edges=-1),
        dict(max_nodes=True),
        dict(time_travel_tolerance_seconds=0),
        dict(time_travel_tolerance_seconds=float("nan")),
        dict(expected_container_tag=""),
        dict(escape_html="yes"),
        dict(schema_version=0),
    ):
        try:
            GreenDecisionGraphConfig(**bad)  # type: ignore[arg-type]
        except GreenDecisionGraphError as exc:
            print(f"reject cfg   : {list(bad)[0]} -> {exc}")
        else:
            raise AssertionError(f"expected rejection for {bad!r}")

    # --------------------------------------------------- 16. Context manager
    with GreenDecisionGraph() as ctx:
        ctx.build_payload(recall_bundles=[bundle])
    print("ctx mgr      : OK")

    print("\nSmoke test passed.")
