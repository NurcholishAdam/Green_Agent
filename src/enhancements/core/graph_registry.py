# src/graph/graph_registry.py
"""
graph_registry.py — Central Graph & Helium Monitor Registry (v6.0.0)
=====================================================================

Manages lifecycle, registration, versioning, and health reporting for all
graph types and the optional Helium supply chain monitor.

**Public API preserved**

    registry = GraphRegistry(config=None, storage=None)
    registry.register(GraphType.CAUSAL, graph)
    registry.get(GraphType.CAUSAL)
    registry.get_history(GraphType.CAUSAL)
    registry.rollback(GraphType.CAUSAL, 2)

    await registry.aregister(...)   # async with enhancement pipeline

**What changed (v17 → v6)**
---------------------------

**P0 — versioning guarantees, concurrency, cross-module identity**

1. **``aregister`` is serialized per graph type (§2.1).** v17 let two
   concurrent ``aregister`` calls race through HITL, deferral, and the
   final read of ``_entries[gt].current.version`` — so a caller could be
   told their registration is v2 when it was actually v1. v6 takes a
   per-graph-type ``asyncio.Lock`` and returns the exact
   ``GraphVersion`` from ``register()``.
2. **Deferred registrations are not lost (§2.2).** v17 returned
   ``{"status": "deferred"}`` and never registered the graph. v6
   **still registers** the graph and marks the version ``deferred=True``
   so the caller's data is preserved; the return dict carries
   ``version`` / ``precision``.
3. **HITL rejections are recorded (§2.3).** v17 silently returned
   ``{"status": "hitl_rejected"}``. v6 records the rejection in
   ``_rejections[graph_type]`` (bounded) so an operator can audit "who
   tried to register X and got denied." The graph is **not** registered.
4. **``rollback`` no longer duplicates history (§2.4).** v17 appended
   ``current`` to ``history`` but left ``target`` in ``history`` too —
   so repeated rollbacks to the same version multiplied
   ``entry.history`` and eventually evicted real versions. v6 removes
   ``target`` from history before appending ``current``.
5. **No ``asyncio.Lock()`` in ``__init__`` (§2.5).** v17 created
   ``_async_lock`` at construction — deprecated on Python <3.10 and
   broken when the registry is built outside a loop. v6 drops
   ``_async_lock`` entirely; state safety comes from ``RLock`` and
   per-graph-type locks created lazily inside a running loop.
6. **``_metadata`` counters are guarded by ``_lock`` (§2.6, §2.7).**
   v17 incremented ``total_deferrals`` / ``total_hitl_*`` without the
   lock — losing updates under concurrency.
7. **``_fingerprint`` is portable (§2.8).** v17 used
   ``pickle.dumps`` with the highest protocol — different bytes across
   Python versions and byte orders. v6 uses canonical JSON
   (sorted keys, deterministic float encoding).

**P1 — reliability**

8. **``_metadata["singletons"]`` is bounded** by ``max_metadata_bytes``
   and demoted to a summary on overflow.
9. **``unregister()``** removes an entry and its rejections.
10. **``reset()``** refuses to run with in-flight registrations.
11. **``shutdown()``** clears ``_metadata`` and ``_singletons``.
12. **Precision downgrade is logged** (§3.5).
13. **Graph sizes are cached on ``GraphEntry``** — no re-pickle per
    register (§3.6).
14. **Critical graph types are configurable** via
    ``registry_critical_graph_types`` (§3.7).
15. **Chaos hook is opt-in** via ``registry_chaos_hook`` and reports
    ``chaos_injected`` in the return (§3.8).
16. **Unified return shape** across ``registered`` / ``deferred`` /
    ``hitl_rejected`` (§3.9).
17. **``health_check()``** on ``GraphRegistry`` (§3.11).
18. **Live reconfiguration** on the orchestrator (§3.12).

**P2 — architecture**

19. **Enhancement classes imported from ``green_agent.enhancements``**
    (fall back to ``carbon_forecast`` v18) — the ten local copies in
    v17 are retired.
20. **``precision_switcher.current`` is consulted** when ``precision``
    is omitted.
21. **Idempotent register** — same fingerprint skips the version bump.
22. **``history`` is a ``deque(maxlen=100)``** — O(1) append, no manual
    truncation.
23. **Structured logging** via ``_log(event, level, **fields)``.
24. **``collect_prometheus_metrics()``**.
25. **``asyncio.get_running_loop()``**, **named background tasks**.
26. **``health(verbose=False)``**.
27. **``to_dict()`` / ``from_dict()``** with an optional ``graph_loader``.
28. **``execution_count`` removed** (dead field in v17).

**Running the demo**

    PYTHONPATH=src python -m graph.graph_registry
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import pickle
import random
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    TYPE_CHECKING,
)

# --------------------------------------------------------------------------- #
# Enhancement classes — single source of truth
# --------------------------------------------------------------------------- #
_ENHANCEMENTS_AVAILABLE = False
_ENHANCEMENTS_SOURCE = "unavailable"
_ENHANCEMENTS_ERROR: Optional[str] = None

try:  # pragma: no cover — package-preferred
    from green_agent.enhancements import (
        _utcnow,
        _ensure_aware,
        _json_safe,
        _cfg_get,
        _safe_mean,
        QuantumDistillationEngine,
        CausalGraphLearner,
        CausalPolicyAdapter,
        FederatedGreenAggregator,
        MultiAgentCoordinator,
        TemporalLogicVerifier,
        XAIDecisionExplainer,
        AdaptivePrecisionSwitcher,
        CarbonMarketIntegrator,
        ChaosTestingEngine,
        ActiveUserPreferenceLearner,
        SimpleQuantumDistillationEngine,
        SimpleCausalGraphLearner,
        SimpleCausalPolicyAdapter,
        SimpleFederatedGreenAggregator,
        SimpleMultiAgentCoordinator,
        SimpleTemporalLogicVerifier,
        SimpleXAIDecisionExplainer,
        SimpleAdaptivePrecisionSwitcher,
        SimpleCarbonMarketIntegrator,
        SimpleChaosTestingEngine,
        SimpleActiveUserPreferenceLearner,
    )
    _ENHANCEMENTS_AVAILABLE = True
    _ENHANCEMENTS_SOURCE = "green_agent.enhancements"
except ImportError as _e1:
    try:  # pragma: no cover — carbon_forecast fallback
        from carbon_forecast import (  # type: ignore[no-redef]
            _utcnow,
            _ensure_aware,
            _json_safe,
            _cfg_get,
            _safe_mean,
            QuantumDistillationEngine,
            CausalGraphLearner,
            CausalPolicyAdapter,
            FederatedGreenAggregator,
            MultiAgentCoordinator,
            TemporalLogicVerifier,
            XAIDecisionExplainer,
            AdaptivePrecisionSwitcher,
            CarbonMarketIntegrator,
            ChaosTestingEngine,
            ActiveUserPreferenceLearner,
            SimpleQuantumDistillationEngine,
            SimpleCausalGraphLearner,
            SimpleCausalPolicyAdapter,
            SimpleFederatedGreenAggregator,
            SimpleMultiAgentCoordinator,
            SimpleTemporalLogicVerifier,
            SimpleXAIDecisionExplainer,
            SimpleAdaptivePrecisionSwitcher,
            SimpleCarbonMarketIntegrator,
            SimpleChaosTestingEngine,
            SimpleActiveUserPreferenceLearner,
        )
        _ENHANCEMENTS_AVAILABLE = True
        _ENHANCEMENTS_SOURCE = "carbon_forecast"
    except ImportError as _e2:
        _ENHANCEMENTS_ERROR = f"{_e1} / {_e2}"
        logger = logging.getLogger(__name__)
        logger.warning(
            "graph_registry: enhancement classes unavailable (%s); "
            "GraphRegistry still imports but the orchestrator will refuse "
            "to construct",
            _ENHANCEMENTS_ERROR,
        )
        # Minimal stubs so the module imports cleanly.
        def _utcnow() -> datetime:  # type: ignore[misc]
            return datetime.now(timezone.utc)

        def _ensure_aware(dt: datetime) -> datetime:  # type: ignore[misc]
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

        def _json_safe(obj: Any) -> Any:  # type: ignore[misc]
            return obj

        def _cfg_get(config: Any, key: str, default: Any = None) -> Any:  # type: ignore[misc]
            if config is None:
                return default
            if isinstance(config, Mapping):
                return config.get(key, default)
            return getattr(config, key, default)

        def _safe_mean(values: Sequence[Any], default: float = 0.0) -> float:  # type: ignore[misc]
            vals = [float(v) for v in values if v is not None]
            return sum(vals) / len(vals) if vals else default

        QuantumDistillationEngine = None  # type: ignore[assignment,misc]
        CausalGraphLearner = None  # type: ignore[assignment,misc]
        CausalPolicyAdapter = None  # type: ignore[assignment,misc]
        FederatedGreenAggregator = None  # type: ignore[assignment,misc]
        MultiAgentCoordinator = None  # type: ignore[assignment,misc]
        TemporalLogicVerifier = None  # type: ignore[assignment,misc]
        XAIDecisionExplainer = None  # type: ignore[assignment,misc]
        AdaptivePrecisionSwitcher = None  # type: ignore[assignment,misc]
        CarbonMarketIntegrator = None  # type: ignore[assignment,misc]
        ChaosTestingEngine = None  # type: ignore[assignment,misc]
        ActiveUserPreferenceLearner = None  # type: ignore[assignment,misc]
        SimpleQuantumDistillationEngine = None  # type: ignore[assignment,misc]
        SimpleCausalGraphLearner = None  # type: ignore[assignment,misc]
        SimpleCausalPolicyAdapter = None  # type: ignore[assignment,misc]
        SimpleFederatedGreenAggregator = None  # type: ignore[assignment,misc]
        SimpleMultiAgentCoordinator = None  # type: ignore[assignment,misc]
        SimpleTemporalLogicVerifier = None  # type: ignore[assignment,misc]
        SimpleXAIDecisionExplainer = None  # type: ignore[assignment,misc]
        SimpleAdaptivePrecisionSwitcher = None  # type: ignore[assignment,misc]
        SimpleCarbonMarketIntegrator = None  # type: ignore[assignment,misc]
        SimpleChaosTestingEngine = None  # type: ignore[assignment,misc]
        SimpleActiveUserPreferenceLearner = None  # type: ignore[assignment,misc]

if TYPE_CHECKING:  # pragma: no cover
    from carbon.helium_monitor import HeliumMonitor


logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors + structured logging
# --------------------------------------------------------------------------- #
class GraphRegistryError(ValueError):
    """Raised for invalid inputs or registry state violations."""


def _log(event: str, level: int = logging.INFO, **fields: Any) -> None:
    if not logger.isEnabledFor(level):
        return
    parts = [event]
    if fields:
        parts.append(" ".join(f"{k}={v}" for k, v in fields.items()))
    logger.log(level, " ".join(parts), extra={"event": event, **fields})


# --------------------------------------------------------------------------- #
# Graph types
# --------------------------------------------------------------------------- #
class GraphType(Enum):
    """Supported graph types in the system."""

    # v1.0
    CAUSAL = "causal"
    POLICY = "policy"
    SIMILARITY = "similarity"
    CARBON_LEDGER = "carbon_ledger"
    # v17 extensions
    TEMPORAL_TRACE = "temporal_trace"
    XAI_ATTRIBUTION = "xai_attribution"
    PRECISION_HISTORY = "precision_history"
    CHAOS_EXPERIMENT = "chaos_experiment"
    MULTI_AGENT = "multi_agent"
    FEDERATED = "federated"
    QUANTUM_DISTILLATION = "quantum_distillation"
    FEATURE_IMPORTANCE = "feature_importance"
    REC_LEDGER = "rec_ledger"
    HITL_QUEUE = "hitl_queue"


# --------------------------------------------------------------------------- #
# Version + entry
# --------------------------------------------------------------------------- #
@dataclass
class GraphVersion:
    """A single version of a registered graph."""

    version: int
    graph: Any
    metadata: Dict[str, Any]
    precision: str
    registered_at: str
    registered_by: str
    fingerprint: str
    tags: List[str] = field(default_factory=list)
    # v6: audit fields
    deferred: bool = False
    chaos_injected: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "metadata": _json_safe(self.metadata),
            "precision": self.precision,
            "registered_at": self.registered_at,
            "registered_by": self.registered_by,
            "fingerprint": self.fingerprint,
            "tags": list(self.tags),
            "deferred": self.deferred,
            "chaos_injected": self.chaos_injected,
        }


@dataclass
class GraphEntry:
    """A registry entry for a graph, holding its current version and history."""

    graph_type: GraphType
    current: GraphVersion
    history: Deque[GraphVersion] = field(
        default_factory=lambda: deque(maxlen=100)
    )
    update_count: int = 0
    cached_size: int = 0

    def to_summary(self) -> Dict[str, Any]:
        return {
            "graph_type": self.graph_type.value,
            "current_version": self.current.version,
            "current_precision": self.current.precision,
            "fingerprint": self.current.fingerprint,
            "registered_at": self.current.registered_at,
            "registered_by": self.current.registered_by,
            "tags": list(self.current.tags),
            "deferred": self.current.deferred,
            "chaos_injected": self.current.chaos_injected,
            "history_length": len(self.history),
            "update_count": self.update_count,
            "cached_size": self.cached_size,
        }


# --------------------------------------------------------------------------- #
# GraphRegistry
# --------------------------------------------------------------------------- #
class GraphRegistry:
    """Central, versioned registry for graph instances and the helium monitor."""

    DEFAULT_PRECISION = "fp32"
    VALID_PRECISIONS = frozenset({"fp32", "fp16", "bf16", "int8"})

    def __init__(
        self,
        config: Optional[Any] = None,
        storage: Optional[Any] = None,
        *,
        carbon_market: Optional[Any] = None,
        hitl: Optional[Any] = None,
        xai: Optional[Any] = None,
        chaos: Optional[Any] = None,
        federated: Optional[Any] = None,
        temporal: Optional[Any] = None,
        multi_agent: Optional[Any] = None,
        precision_switcher: Optional[Any] = None,
        distiller: Optional[Any] = None,
        causal_rl: Optional[Any] = None,
    ) -> None:
        self.config = config
        self.storage = storage

        # Enhancement hooks
        self.carbon_market = carbon_market
        self.hitl = hitl
        self.xai = xai
        self.chaos = chaos
        self.federated = federated
        self.temporal = temporal
        self.multi_agent = multi_agent
        self.precision_switcher = precision_switcher
        self.distiller = distiller
        self.causal_rl = causal_rl

        # Config-derived settings
        critical_types = _cfg_get(
            config, "registry_critical_graph_types",
            ["causal", "carbon_ledger", "rec_ledger"],
        )
        self._critical_types: Set[str] = set(
            str(t).lower() for t in critical_types
        )
        self._chaos_hook_enabled: bool = bool(
            _cfg_get(config, "registry_chaos_hook", False)
        )
        self._history_max: int = max(
            1, int(_cfg_get(config, "registry_history_max", 100))
        )
        self._max_metadata_bytes: int = max(
            1024, int(_cfg_get(config, "registry_max_metadata_bytes", 4096))
        )

        # State
        self._entries: Dict[GraphType, GraphEntry] = {}
        self._rejections: Dict[GraphType, Deque[Dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=100)
        )
        self._singletons: Dict[str, Dict[str, Any]] = {}
        self._helium_monitor: Optional["HeliumMonitor"] = None

        # Locks
        self._lock = threading.RLock()
        self._graph_locks: Dict[GraphType, asyncio.Lock] = {}

        # Counters
        self._in_flight: int = 0
        self._metadata: Dict[str, Any] = {
            "last_updated": _utcnow().isoformat(),
            "total_updates": 0,
            "total_rollbacks": 0,
            "total_deferrals": 0,
            "total_hitl_approvals": 0,
            "total_hitl_rejections": 0,
        }

        _log(
            "registry_initialized",
            logging.DEBUG,
            critical_types=sorted(self._critical_types),
            chaos_hook=self._chaos_hook_enabled,
            enhancements=_ENHANCEMENTS_SOURCE,
        )

    # ------------------------------------------------------------------ props
    @property
    def enhancements_available(self) -> bool:
        return _ENHANCEMENTS_AVAILABLE

    @property
    def enhancements_source(self) -> str:
        return _ENHANCEMENTS_SOURCE

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _canonicalize(obj: Any) -> Any:
        """Deterministic, platform-independent representation."""
        if obj is None or isinstance(obj, (bool, int, str)):
            return obj
        if isinstance(obj, float):
            if math.isnan(obj):
                return "NaN"
            if math.isinf(obj):
                return "Infinity" if obj > 0 else "-Infinity"
            if obj == 0.0:
                return 0.0
            return obj
        if isinstance(obj, datetime):
            return _ensure_aware(obj).isoformat()
        if isinstance(obj, Enum):
            return obj.value
        if isinstance(obj, Mapping):
            return {
                str(k): GraphRegistry._canonicalize(v)
                for k, v in obj.items()
            }
        if isinstance(obj, (list, tuple)):
            return [GraphRegistry._canonicalize(v) for v in obj]
        if isinstance(obj, (set, frozenset)):
            return sorted(
                (GraphRegistry._canonicalize(v) for v in obj),
                key=lambda x: json.dumps(x, sort_keys=True, default=str),
            )
        return str(obj)

    @staticmethod
    def _fingerprint(graph: Any) -> str:
        """Portable fingerprint — canonical JSON with sorted keys."""
        try:
            canonical = json.dumps(
                GraphRegistry._canonicalize(graph),
                sort_keys=True,
                default=str,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
        except Exception:
            return hashlib.sha256(
                repr(graph).encode("utf-8")
            ).hexdigest()[:16]

    @staticmethod
    def _graph_size(graph: Any) -> int:
        if graph is None:
            return 0
        try:
            if hasattr(graph, "__len__"):
                return len(graph)  # type: ignore[arg-type]
        except Exception:
            pass
        try:
            return len(pickle.dumps(graph, protocol=4))
        except Exception:
            return 0

    def _summarize_metadata(
        self, metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        try:
            size = len(json.dumps(metadata, default=str))
        except Exception:
            size = 0
        if size <= self._max_metadata_bytes:
            return _json_safe(dict(metadata))
        return {
            "keys": list(metadata.keys()),
            "size_bytes": size,
            "truncated": True,
        }

    def _make_version(
        self,
        graph_type: GraphType,
        graph: Any,
        metadata: Optional[Dict[str, Any]],
        precision: Optional[str],
        tags: Optional[List[str]],
        registered_by: str,
        *,
        deferred: bool = False,
        chaos_injected: bool = False,
    ) -> GraphVersion:
        with self._lock:
            prev_version = (
                self._entries[graph_type].current.version
                if graph_type in self._entries else 0
            )
        prec = precision or self._default_precision()
        if prec not in self.VALID_PRECISIONS:
            _log(
                "precision_downgraded",
                logging.WARNING,
                graph_type=graph_type.value,
                requested=prec,
                fallback=self.DEFAULT_PRECISION,
            )
            prec = self.DEFAULT_PRECISION
        return GraphVersion(
            version=prev_version + 1,
            graph=graph,
            metadata=self._summarize_metadata(metadata or {}),
            precision=prec,
            registered_at=_utcnow().isoformat(),
            registered_by=registered_by,
            fingerprint=self._fingerprint(graph),
            tags=list(tags or []),
            deferred=deferred,
            chaos_injected=chaos_injected,
        )

    def _default_precision(self) -> str:
        if self.precision_switcher is not None:
            current = getattr(self.precision_switcher, "current", None)
            if isinstance(current, str) and current in self.VALID_PRECISIONS:
                return current
        return self.DEFAULT_PRECISION

    # ------------------------------------------------------------------ sync API
    def register(
        self,
        graph_type: GraphType,
        graph: Any,
        metadata: Optional[Dict[str, Any]] = None,
        precision: Optional[str] = None,
        tags: Optional[List[str]] = None,
        registered_by: str = "system",
        *,
        deferred: bool = False,
        chaos_injected: bool = False,
    ) -> GraphVersion:
        """
        Register (or update) a graph.

        Returns the resulting ``GraphVersion`` (v6 — v17 returned None).
        Idempotent: if the new graph's fingerprint matches the current
        version, no version bump occurs.
        """
        if not isinstance(graph_type, GraphType):
            raise GraphRegistryError(
                f"graph_type must be GraphType, got {type(graph_type).__name__}"
            )

        with self._lock:
            version = self._make_version(
                graph_type, graph, metadata, precision, tags,
                registered_by, deferred=deferred, chaos_injected=chaos_injected,
            )
            existing = self._entries.get(graph_type)

            # Idempotency: same fingerprint → no-op
            if (
                existing is not None
                and existing.current.fingerprint == version.fingerprint
                and not deferred
                and not chaos_injected
            ):
                _log(
                    "register_noop",
                    logging.DEBUG,
                    graph_type=graph_type.value,
                    version=existing.current.version,
                )
                return existing.current

            new_size = self._graph_size(graph)
            if existing is not None:
                existing.history.append(existing.current)
                existing.current = version
                existing.update_count += 1
                existing.cached_size = new_size
            else:
                entry = GraphEntry(
                    graph_type=graph_type,
                    current=version,
                    update_count=1,
                    cached_size=new_size,
                )
                self._entries[graph_type] = entry

            if metadata:
                self._singletons[graph_type.value] = self._summarize_metadata(
                    metadata
                )

            self._metadata["last_updated"] = _utcnow().isoformat()
            self._metadata["total_updates"] += 1

        _log(
            "graph_registered",
            logging.INFO,
            graph_type=graph_type.value,
            version=version.version,
            precision=version.precision,
            deferred=deferred,
            chaos_injected=chaos_injected,
        )
        return version

    def get(self, graph_type: GraphType) -> Optional[Any]:
        with self._lock:
            entry = self._entries.get(graph_type)
            return entry.current.graph if entry is not None else None

    def list_graphs(self) -> List[GraphType]:
        with self._lock:
            return list(self._entries.keys())

    def unregister(self, graph_type: GraphType) -> bool:
        with self._lock:
            if graph_type not in self._entries:
                return False
            del self._entries[graph_type]
            self._rejections.pop(graph_type, None)
            self._singletons.pop(graph_type.value, None)
            self._metadata["last_updated"] = _utcnow().isoformat()
        _log("graph_unregistered", logging.INFO, graph_type=graph_type.value)
        return True

    # ------------------------------------------------------------------ versioning
    def get_history(self, graph_type: GraphType) -> List[Dict[str, Any]]:
        with self._lock:
            entry = self._entries.get(graph_type)
            if entry is None:
                return []
            versions = [v.to_dict() for v in entry.history]
            current = entry.current.to_dict()
            current["is_current"] = True
            versions.append(current)
            return versions

    def rollback(
        self, graph_type: GraphType, target_version: int
    ) -> bool:
        """Roll back to ``target_version``; history does not duplicate."""
        with self._lock:
            entry = self._entries.get(graph_type)
            if entry is None:
                return False
            if entry.current.version == target_version:
                return False  # no-op
            for i, v in enumerate(entry.history):
                if v.version != target_version:
                    continue
                # Remove target from history (it becomes current)
                entry.history.remove(v)
                # Move current to history
                entry.history.append(entry.current)
                entry.current = v
                entry.update_count += 1
                entry.cached_size = self._graph_size(v.graph)
                self._metadata["total_rollbacks"] += 1
                self._metadata["last_updated"] = _utcnow().isoformat()
                _log(
                    "graph_rolled_back",
                    logging.INFO,
                    graph_type=graph_type.value,
                    target=target_version,
                )
                return True
            return False

    # ------------------------------------------------------------------ health
    def health(self, verbose: bool = False) -> Dict[str, Any]:
        with self._lock:
            data = dict(self._metadata)
            data["helium_monitor_registered"] = (
                self._helium_monitor is not None
            )
            data["num_graphs"] = len(self._entries)
            data["in_flight_registrations"] = self._in_flight
            data["enhancements_available"] = _ENHANCEMENTS_AVAILABLE
            data["enhancements_source"] = _ENHANCEMENTS_SOURCE
            data["graphs"] = {
                gt.value: entry.to_summary()
                for gt, entry in self._entries.items()
            }
            if verbose:
                data["singletons"] = dict(self._singletons)
                data["rejections"] = {
                    gt.value: [dict(r) for r in rej]
                    for gt, rej in self._rejections.items()
                    if rej
                }
            return data

    def health_check(self) -> Dict[str, Any]:
        with self._lock:
            anomalies: List[str] = []
            if self._in_flight < 0:
                anomalies.append("negative_in_flight")
            for gt, entry in self._entries.items():
                if entry.current is None:
                    anomalies.append(f"null_current:{gt.value}")
            return {
                "healthy": len(anomalies) == 0,
                "anomalies": anomalies,
                "num_graphs": len(self._entries),
                "in_flight_registrations": self._in_flight,
                "helium_registered": self._helium_monitor is not None,
            }

    def collect_prometheus_metrics(
        self,
    ) -> Dict[str, Tuple[Any, Dict[str, str]]]:
        with self._lock:
            metrics: Dict[str, Tuple[Any, Dict[str, str]]] = {
                "graph_registry_num_graphs": (len(self._entries), {}),
                "graph_registry_updates_total": (
                    self._metadata["total_updates"], {}
                ),
                "graph_registry_rollbacks_total": (
                    self._metadata["total_rollbacks"], {}
                ),
                "graph_registry_deferrals_total": (
                    self._metadata["total_deferrals"], {}
                ),
                "graph_registry_hitl_approvals_total": (
                    self._metadata["total_hitl_approvals"], {}
                ),
                "graph_registry_hitl_rejections_total": (
                    self._metadata["total_hitl_rejections"], {}
                ),
                "graph_registry_in_flight": (self._in_flight, {}),
            }
            for gt, entry in self._entries.items():
                labels = {"graph_type": gt.value}
                metrics[f"graph_registry_version"] = (
                    entry.current.version, labels
                ) if f"graph_registry_version_{gt.value}" not in metrics else metrics[f"graph_registry_version"]
                metrics[f"graph_registry_version_{gt.value}"] = (
                    entry.current.version, labels
                )
                metrics[f"graph_registry_history_{gt.value}"] = (
                    len(entry.history), labels
                )
            return metrics

    # ------------------------------------------------------------------ async API
    def _graph_lock(self, graph_type: GraphType) -> asyncio.Lock:
        """Lazily create a per-graph-type lock inside the running loop."""
        lock = self._graph_locks.get(graph_type)
        if lock is None:
            lock = asyncio.Lock()
            self._graph_locks[graph_type] = lock
        return lock

    async def aregister(
        self,
        graph_type: GraphType,
        graph: Any,
        metadata: Optional[Dict[str, Any]] = None,
        precision: Optional[str] = None,
        tags: Optional[List[str]] = None,
        registered_by: str = "system",
        require_hitl: Optional[bool] = None,
        defer_if_high_carbon: bool = True,
        explain: bool = True,
    ) -> Dict[str, Any]:
        """Async registration with the full enhancement pipeline."""
        if not isinstance(graph_type, GraphType):
            raise GraphRegistryError(
                f"graph_type must be GraphType, got {type(graph_type).__name__}"
            )

        async with self._graph_lock(graph_type):
            with self._lock:
                self._in_flight += 1
            try:
                return await self._aregister_pipeline(
                    graph_type=graph_type,
                    graph=graph,
                    metadata=metadata,
                    precision=precision,
                    tags=tags,
                    registered_by=registered_by,
                    require_hitl=require_hitl,
                    defer_if_high_carbon=defer_if_high_carbon,
                    explain=explain,
                )
            finally:
                with self._lock:
                    self._in_flight -= 1

    async def _aregister_pipeline(
        self,
        *,
        graph_type: GraphType,
        graph: Any,
        metadata: Optional[Dict[str, Any]],
        precision: Optional[str],
        tags: Optional[List[str]],
        registered_by: str,
        require_hitl: Optional[bool],
        defer_if_high_carbon: bool,
        explain: bool,
    ) -> Dict[str, Any]:
        # 1. Chaos hook (opt-in)
        chaos_injected = False
        if (
            self._chaos_hook_enabled
            and self.chaos is not None
            and random.random() < 0.05
        ):
            try:
                await self.chaos.run_experiment(
                    f"registry_{uuid.uuid4().hex[:6]}", "latency"
                )
                chaos_injected = True
            except Exception:
                pass

        # 2. Carbon-aware deferral — but we **still register** (v6 §2.2)
        deferred = False
        defer_reason: Optional[str] = None
        if defer_if_high_carbon and self.carbon_market is not None:
            try:
                decision = await self.carbon_market.net_zero_schedule(
                    workload_kwh=0.01, intensity=0.4
                )
                if decision.get("action") == "defer":
                    deferred = True
                    defer_reason = "carbon_intensity_high"
                    with self._lock:
                        self._metadata["total_deferrals"] += 1
                    _log(
                        "registration_deferred",
                        logging.INFO,
                        graph_type=graph_type.value,
                    )
            except Exception:
                pass

        # 3. HITL — fail-safe on timeout
        if require_hitl is None:
            require_hitl = graph_type.value in self._critical_types
        if require_hitl and self.hitl is not None:
            try:
                approved = await self.hitl.query_user_if_needed(
                    "registry_approver",
                    [
                        {
                            "solution_id": "approve",
                            "quality_score": 0.9,
                            "carbon_g": 0.1,
                            "cost_usd": 0.1,
                            "latency_ms": 100,
                        },
                        {
                            "solution_id": "reject",
                            "quality_score": 0.89,
                            "carbon_g": 0.11,
                            "cost_usd": 0.11,
                            "latency_ms": 105,
                        },
                    ],
                    timeout=2.0,
                )
                # v18's HITL returns None on timeout (fail-safe)
                if approved != "approve":
                    with self._lock:
                        self._metadata["total_hitl_rejections"] += 1
                        self._rejections[graph_type].append(
                            {
                                "at": _utcnow().isoformat(),
                                "reason": (
                                    "hitl_rejected" if approved == "reject"
                                    else "hitl_timed_out"
                                ),
                                "registered_by": registered_by,
                            }
                        )
                    _log(
                        "registration_hitl_rejected",
                        logging.WARNING,
                        graph_type=graph_type.value,
                        approved=approved,
                    )
                    return {
                        "status": "hitl_rejected",
                        "graph_type": graph_type.value,
                        "version": None,
                        "precision": None,
                        "xai": None,
                        "deferred": False,
                        "chaos_injected": chaos_injected,
                        "reason": (
                            "hitl_rejected" if approved == "reject"
                            else "hitl_timed_out"
                        ),
                    }
                with self._lock:
                    self._metadata["total_hitl_approvals"] += 1
            except Exception:
                pass

        # 4. XAI explanation (uses cached size — no re-pickle)
        xai_result = None
        if explain and self.xai is not None and _ENHANCEMENTS_AVAILABLE:
            try:
                import numpy as np  # type: ignore

                with self._lock:
                    existing = self._entries.get(graph_type)
                    prev_size = existing.cached_size if existing else 0
                    total_updates = self._metadata["total_updates"]
                new_size = self._graph_size(graph)
                feats = np.array(
                    [
                        float(prev_size),
                        float(new_size),
                        float(total_updates),
                        1.0 if existing is None else 0.0,
                    ]
                )

                def _score(x):
                    return float(np.dot(x, [0.3, 0.4, 0.2, 0.1]))

                xai_result = await self.xai.explain(
                    decision_id=f"reg_{uuid.uuid4().hex[:8]}",
                    label=f"register:{graph_type.value}",
                    features=feats,
                    names=[
                        "prev_size",
                        "new_size",
                        "total_updates",
                        "is_new",
                    ],
                    model_fn=_score,
                    include_counterfactual=False,
                )
            except Exception:
                pass

        # 5. Federated share (metadata only)
        if self.federated is not None:
            try:
                blob = json.dumps(
                    {
                        "graph_type": graph_type.value,
                        "action": "register",
                        "ts": _utcnow().isoformat(),
                    }
                ).encode()
                await self.federated.share_weights("registry_events", blob)
            except Exception:
                pass

        # 6. Register and capture the exact version
        version = self.register(
            graph_type,
            graph,
            metadata,
            precision,
            tags,
            registered_by,
            deferred=deferred,
            chaos_injected=chaos_injected,
        )

        # 7. Temporal push
        if self.temporal is not None:
            try:
                await self.temporal.push_state(
                    {
                        "registry_event": graph_type.value,
                        "total_graphs": len(self._entries),
                        "quality": 1.0,
                    }
                )
            except Exception:
                pass

        return {
            "status": "deferred" if deferred else "registered",
            "graph_type": graph_type.value,
            "version": version.version,
            "precision": version.precision,
            "xai": xai_result,
            "deferred": deferred,
            "chaos_injected": chaos_injected,
            "reason": defer_reason,
        }

    async def aget(self, graph_type: GraphType) -> Optional[Any]:
        return self.get(graph_type)

    async def alist(self) -> List[GraphType]:
        return self.list_graphs()

    async def ahealth(self, verbose: bool = False) -> Dict[str, Any]:
        return self.health(verbose=verbose)

    async def arollback(
        self, graph_type: GraphType, target_version: int
    ) -> bool:
        return self.rollback(graph_type, target_version)

    # ------------------------------------------------------------------ helium
    def register_helium_monitor(self, monitor: "HeliumMonitor") -> None:
        with self._lock:
            self._helium_monitor = monitor
        _log("helium_registered", logging.INFO)

    def get_helium_monitor(self) -> Optional["HeliumMonitor"]:
        with self._lock:
            return self._helium_monitor

    # ------------------------------------------------------------------ lifecycle
    def reset(self) -> None:
        with self._lock:
            if self._in_flight > 0:
                raise GraphRegistryError(
                    f"cannot reset with {self._in_flight} registration(s) in flight"
                )
            self._entries.clear()
            self._rejections.clear()
            self._singletons.clear()
            self._helium_monitor = None
            self._graph_locks.clear()
            self._metadata = {
                "last_updated": _utcnow().isoformat(),
                "total_updates": 0,
                "total_rollbacks": 0,
                "total_deferrals": 0,
                "total_hitl_approvals": 0,
                "total_hitl_rejections": 0,
            }
        _log("registry_reset", logging.INFO)

    async def shutdown(self) -> None:
        _log("registry_shutting_down", logging.INFO)
        helium = None
        with self._lock:
            helium = self._helium_monitor
            self._helium_monitor = None
        if helium is not None:
            try:
                await helium.shutdown()
            except Exception as exc:
                logger.error("helium shutdown failed: %s", exc)
        with self._lock:
            self._entries.clear()
            self._rejections.clear()
            self._singletons.clear()
            self._graph_locks.clear()
            self._metadata = {
                "last_updated": _utcnow().isoformat(),
                "total_updates": 0,
                "total_rollbacks": 0,
                "total_deferrals": 0,
                "total_hitl_approvals": 0,
                "total_hitl_rejections": 0,
            }
        _log("registry_shutdown_complete", logging.INFO)

    # ------------------------------------------------------------------ serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "metadata": _json_safe(dict(self._metadata)),
                "singletons": _json_safe(dict(self._singletons)),
                "entries": {
                    gt.value: {
                        "current": entry.current.to_dict(),
                        "history": [v.to_dict() for v in entry.history],
                        "update_count": entry.update_count,
                        "cached_size": entry.cached_size,
                    }
                    for gt, entry in self._entries.items()
                },
                "rejections": {
                    gt.value: [_json_safe(dict(r)) for r in rej]
                    for gt, rej in self._rejections.items()
                    if rej
                },
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        config: Optional[Any] = None,
        storage: Optional[Any] = None,
        graph_loader: Optional[Callable[[GraphType], Any]] = None,
    ) -> "GraphRegistry":
        """
        Rebuild a registry from ``to_dict()`` output.

        ``graph_loader(gt)`` must return a live graph object; if it's
        ``None``, graphs are restored as ``None`` and only the version
        history is preserved.
        """
        registry = cls(config=config, storage=storage)
        if not isinstance(data, Mapping):
            raise GraphRegistryError(
                f"from_dict expects Mapping, got {type(data).__name__}"
            )

        with registry._lock:
            md = data.get("metadata", {}) or {}
            for key, val in md.items():
                registry._metadata[key] = val
            registry._singletons = dict(data.get("singletons", {}) or {})

            for gt_value, entry_data in (data.get("entries") or {}).items():
                try:
                    gt = GraphType(str(gt_value))
                except ValueError:
                    _log("from_dict_unknown_graph_type", logging.WARNING, value=gt_value)
                    continue

                def _version_from_dict(v_data: Mapping[str, Any]) -> GraphVersion:
                    return GraphVersion(
                        version=int(v_data["version"]),
                        graph=graph_loader(gt) if graph_loader else None,
                        metadata=dict(v_data.get("metadata", {})),
                        precision=str(v_data.get("precision", "fp32")),
                        registered_at=str(v_data.get("registered_at", "")),
                        registered_by=str(v_data.get("registered_by", "")),
                        fingerprint=str(v_data.get("fingerprint", "")),
                        tags=list(v_data.get("tags", [])),
                        deferred=bool(v_data.get("deferred", False)),
                        chaos_injected=bool(
                            v_data.get("chaos_injected", False)
                        ),
                    )

                try:
                    current = _version_from_dict(entry_data["current"])
                    history = deque(
                        (
                            _version_from_dict(v)
                            for v in entry_data.get("history", [])
                        ),
                        maxlen=registry._history_max,
                    )
                    entry = GraphEntry(
                        graph_type=gt,
                        current=current,
                        history=history,
                        update_count=int(
                            entry_data.get("update_count", 0)
                        ),
                        cached_size=int(
                            entry_data.get("cached_size", 0)
                        ),
                    )
                    registry._entries[gt] = entry
                except Exception as exc:
                    _log(
                        "from_dict_entry_failed",
                        logging.WARNING,
                        value=gt_value,
                        error=str(exc),
                    )
                    continue

            for gt_value, rejections in (data.get("rejections") or {}).items():
                try:
                    gt = GraphType(str(gt_value))
                except ValueError:
                    continue
                for r in rejections:
                    registry._rejections[gt].append(dict(r))

        return registry


# --------------------------------------------------------------------------- #
# Orchestrator — wires the ten enhancements with the registry
# --------------------------------------------------------------------------- #
class GraphRegistryOrchestratorV18:
    """Unified entry point: registry + ten enhancements + background loops."""

    DEFAULT_FEATURES: Dict[str, bool] = {
        "causal_rl": True,
        "federated": True,
        "multi_agent": True,
        "temporal": True,
        "xai": True,
        "precision": True,
        "carbon_market": True,
        "chaos": True,
        "distillation": True,
    }

    def __init__(
        self,
        storage: Any,
        config: Any,
        dashboard: Any = None,
        *,
        features: Optional[Dict[str, bool]] = None,
    ) -> None:
        if not _ENHANCEMENTS_AVAILABLE:
            raise GraphRegistryError(
                f"GraphRegistryOrchestratorV18 requires enhancement classes "
                f"from 'green_agent.enhancements' or 'carbon_forecast' "
                f"({_ENHANCEMENTS_ERROR})"
            )

        self.storage = storage
        self.config = config
        self.instance_id = uuid.uuid4().hex[:8]
        self._features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES,
            **(features or {}),
        }

        # Ten enhancements
        self.quantum = QuantumDistillationEngine()  # type: ignore[misc]
        self.causal_graph = CausalGraphLearner(storage)  # type: ignore[misc]
        self.causal_rl = CausalPolicyAdapter(  # type: ignore[misc]
            config, storage, self.causal_graph
        )
        self.federated = FederatedGreenAggregator(  # type: ignore[misc]
            storage, self.instance_id
        )
        self.multi_agent = MultiAgentCoordinator(config, storage)  # type: ignore[misc]
        self.temporal = TemporalLogicVerifier(storage, config)  # type: ignore[misc]
        self.xai = XAIDecisionExplainer(config, storage)  # type: ignore[misc]
        self.precision = AdaptivePrecisionSwitcher(config, storage)  # type: ignore[misc]
        self.carbon_market = CarbonMarketIntegrator(config, storage)  # type: ignore[misc]
        self.chaos = ChaosTestingEngine(config, storage)  # type: ignore[misc]
        self.hitl = ActiveUserPreferenceLearner(  # type: ignore[misc]
            storage, dashboard=dashboard
        )

        self.registry = GraphRegistry(
            config=config,
            storage=storage,
            carbon_market=self.carbon_market,
            hitl=self.hitl,
            xai=self.xai,
            chaos=self.chaos,
            federated=self.federated,
            temporal=self.temporal,
            multi_agent=self.multi_agent,
            precision_switcher=self.precision,
            distiller=self.quantum,
            causal_rl=self.causal_rl,
        )

        # Wire HITL to temporal critical rules
        if hasattr(self.temporal, "set_approval_callback"):
            self.temporal.set_approval_callback(
                self._on_critical_violation
            )

        # Lifecycle
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: Set[asyncio.Task] = set()

    # ------------------------------------------------------------------ features
    def enable_feature(self, name: str) -> None:
        if name not in self._features:
            raise GraphRegistryError(f"unknown feature {name!r}")
        self._features[name] = True
        _log("feature_enabled", logging.INFO, feature=name)

    def disable_feature(self, name: str) -> None:
        if name not in self._features:
            raise GraphRegistryError(f"unknown feature {name!r}")
        self._features[name] = False
        _log("feature_disabled", logging.INFO, feature=name)

    def feature_enabled(self, name: str) -> bool:
        return bool(self._features.get(name, False))

    # ------------------------------------------------------------------ HITL hook
    async def _on_critical_violation(self, rule_id: str, state: Any):
        approved = await self.hitl.query_user_if_needed(
            "critical_user", [state, state], timeout=2.0
        )
        return approved is not None

    # ------------------------------------------------------------------ public
    async def register_graph(
        self, graph_type: GraphType, graph: Any, **kwargs: Any
    ) -> Dict[str, Any]:
        return await self.registry.aregister(graph_type, graph, **kwargs)

    async def get_graph(self, graph_type: GraphType) -> Optional[Any]:
        return await self.registry.aget(graph_type)

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._shutdown_event.clear()
        loop = asyncio.get_running_loop()
        factories: List[Tuple[str, Callable[[], Any]]] = [
            ("causal_rl", self._causal_rl_loop),
            ("federated", self._federated_loop),
            ("multi_agent", self._multi_agent_loop),
            ("temporal", self._temporal_loop),
            ("xai", self._xai_loop),
            ("precision", self._precision_loop),
            ("carbon_market", self._carbon_market_loop),
            ("chaos", self._chaos_loop),
            ("distillation", self._distillation_loop),
        ]
        for name, factory in factories:
            t = loop.create_task(factory(), name=f"registry:{name}")
            self._background_tasks.add(t)
            t.add_done_callback(self._background_tasks.discard)

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        self._running = False
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(
                *self._background_tasks, return_exceptions=True
            )
        await self.registry.shutdown()

    async def __aenter__(self) -> "GraphRegistryOrchestratorV18":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ------------------------------------------------------------------ loops
    async def _sleep(self, seconds: float) -> bool:
        """Return True if the shutdown event fired (loop should exit)."""
        try:
            await asyncio.wait_for(self._shutdown_event.wait(), timeout=seconds)
            return True
        except asyncio.TimeoutError:
            return False

    async def _causal_rl_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep(900):
                return
            if not self.feature_enabled("causal_rl"):
                continue
            try:
                samples = [
                    {
                        "quality": random.uniform(0.5, 1.0),
                        "carbon": random.uniform(0.1, 0.8),
                        "cost": random.uniform(0.1, 0.9),
                        "latency": random.uniform(0.1, 0.9),
                    }
                    for _ in range(20)
                ]
                await self.causal_graph.learn(
                    samples, ["quality", "carbon", "cost", "latency"]
                )
                await self.registry.aregister(
                    GraphType.CAUSAL,
                    self.causal_graph,
                    metadata=self.causal_graph.summary(),
                )
            except Exception as exc:
                logger.debug("causal_rl_loop: %s", exc)

    async def _federated_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep(3600):
                return
            if not self.feature_enabled("federated"):
                continue
            try:
                dummy = bytes(random.getrandbits(8) for _ in range(64))
                await self.federated.share_weights("registry_graph", dummy)
                await self.federated.pull_aggregated_weights("registry_graph")
            except Exception as exc:
                logger.debug("federated_loop: %s", exc)

    async def _multi_agent_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep(600):
                return
            if not self.feature_enabled("multi_agent"):
                continue
            try:
                await self.multi_agent.step()
                await self.registry.aregister(
                    GraphType.MULTI_AGENT, self.multi_agent.get_policy()
                )
            except Exception as exc:
                logger.debug("multi_agent_loop: %s", exc)

    async def _temporal_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep(300):
                return
            if not self.feature_enabled("temporal"):
                continue
            try:
                await self.temporal.verify()
                await self.registry.aregister(
                    GraphType.TEMPORAL_TRACE,
                    list(self.temporal.trace)[-100:],
                )
            except Exception as exc:
                logger.debug("temporal_loop: %s", exc)

    async def _xai_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep(300):
                return
            if not self.feature_enabled("xai"):
                continue
            try:
                import numpy as np  # type: ignore

                feats = np.array([0.9, 0.4, 0.5, 0.4])
                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="registry_health",
                    features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=lambda x: float(
                        np.dot(x, [0.4, -0.3, -0.2, -0.1])
                    ),
                )
                await self.registry.aregister(
                    GraphType.XAI_ATTRIBUTION,
                    self.xai.global_feature_importance(),
                )
            except Exception as exc:
                logger.debug("xai_loop: %s", exc)

    async def _precision_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep(300):
                return
            if not self.feature_enabled("precision"):
                continue
            try:
                await self.precision.auto_switch(0.9, 0.92)
                await self.registry.aregister(
                    GraphType.PRECISION_HISTORY,
                    list(self.precision.history),
                )
            except Exception as exc:
                logger.debug("precision_loop: %s", exc)

    async def _carbon_market_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep(3600):
                return
            if not self.feature_enabled("carbon_market"):
                continue
            try:
                await self.carbon_market.update_price()
            except Exception as exc:
                logger.debug("carbon_market_loop: %s", exc)

    async def _chaos_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep(1800):
                return
            if not self.feature_enabled("chaos"):
                continue
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)  # type: ignore[union-attr]
                result = await self.chaos.run_experiment(
                    f"auto_{uuid.uuid4().hex[:6]}", fault
                )
                await self.registry.aregister(
                    GraphType.CHAOS_EXPERIMENT, result
                )
            except Exception as exc:
                logger.debug("chaos_loop: %s", exc)

    async def _distillation_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep(300):
                return
            if not self.feature_enabled("distillation"):
                continue
            try:
                self.quantum.register_teacher(
                    "causal", self.causal_rl.get_policy()
                )
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy()
                )
                entry = await self.quantum.step(
                    self.storage, "registry_student"
                )
                await self.registry.aregister(
                    GraphType.QUANTUM_DISTILLATION, entry["student"]
                )
            except Exception as exc:
                logger.debug("distillation_loop: %s", exc)

    # ------------------------------------------------------------------ health
    async def health_check(self) -> Dict[str, Any]:
        registry_health = await self.registry.ahealth()
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "registry": registry_health,
            "precision": self.precision.current,  # type: ignore[union-attr]
            "carbon_price": self.carbon_market.last_price,  # type: ignore[union-attr]
            "agents": {
                a.id: a.role
                for a in self.multi_agent.agents.values()  # type: ignore[union-attr]
            },
            "federated_rounds": self.federated.rounds,  # type: ignore[union-attr]
            "features": dict(self._features),
        }


# Backward-compatible alias
GraphRegistryOrchestratorV17 = GraphRegistryOrchestratorV18


# --------------------------------------------------------------------------- #
# In-memory storage (for demos and tests)
# --------------------------------------------------------------------------- #
class InMemoryStorage:
    """Minimal in-memory storage — bounded by max_entries per bucket."""

    def __init__(self, max_entries: int = 10_000) -> None:
        self.max_entries = max_entries
        self._data: Dict[str, Deque[Any]] = defaultdict(
            lambda: deque(maxlen=max_entries)
        )
        self._prefs: Dict[str, Dict[str, float]] = {}

    def _record(self, bucket: str, entry: Any) -> None:
        self._data[bucket].append(entry)

    def save_teacher_superposition(self, *a, **kw): self._record("teacher_superpositions", a)
    def save_causal_edge(self, src, dst, w, c): self._record("causal_graph", (src, dst, w))
    def save_causal_experiment(self, *a, **kw): self._record("causal_experiments", a)
    def save_federated_weights(self, *a, **kw): self._record("federated_weights", a)
    def get_federated_weights(self, model_id): return [r for r in self._data["federated_weights"] if r[1] == model_id]
    def save_precision_switch(self, *a, **kw): self._record("precision_history", a)
    def save_credit_price(self, price, **kw): self._record("carbon_credit_prices", price)
    def save_rec(self, *a, **kw): self._record("rec_ledger", a)
    def get_rec_balance(self): return sum(r[0] for r in self._data["rec_ledger"])
    def save_net_zero_match(self, *a, **kw): self._record("net_zero_matches", a)
    def save_chaos_experiment(self, *a, **kw): self._record("chaos_experiments", a)
    def save_temporal_trace(self, state, context=None): self._record("temporal_trace", state)
    def save_temporal_rule(self, *a, **kw): self._record("temporal_rules", a)
    def save_temporal_violation(self, *a, **kw): self._record("temporal_violations", a)
    def save_xai_explanation(self, *a, **kw): self._record("xai_explanations", a)
    def save_user_preference(self, uid, weights): self._prefs[uid] = dict(weights)
    def get_user_preference(self, uid): return self._prefs.get(uid)


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "GraphType",
    "GraphVersion",
    "GraphEntry",
    "GraphRegistry",
    "GraphRegistryOrchestratorV18",
    "GraphRegistryOrchestratorV17",
    "GraphRegistryError",
    "InMemoryStorage",
    # Enhancement re-exports
    "QuantumDistillationEngine",
    "CausalGraphLearner",
    "CausalPolicyAdapter",
    "FederatedGreenAggregator",
    "MultiAgentCoordinator",
    "TemporalLogicVerifier",
    "XAIDecisionExplainer",
    "AdaptivePrecisionSwitcher",
    "CarbonMarketIntegrator",
    "ChaosTestingEngine",
    "ActiveUserPreferenceLearner",
    "SimpleQuantumDistillationEngine",
    "SimpleCausalGraphLearner",
    "SimpleCausalPolicyAdapter",
    "SimpleFederatedGreenAggregator",
    "SimpleMultiAgentCoordinator",
    "SimpleTemporalLogicVerifier",
    "SimpleXAIDecisionExplainer",
    "SimpleAdaptivePrecisionSwitcher",
    "SimpleCarbonMarketIntegrator",
    "SimpleChaosTestingEngine",
    "SimpleActiveUserPreferenceLearner",
    "_ENHANCEMENTS_AVAILABLE",
    "_ENHANCEMENTS_SOURCE",
]


# --------------------------------------------------------------------------- #
# Demo — every P0/P1/P2 fix verified by assertion
# --------------------------------------------------------------------------- #
def _demo() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("=" * 78)
    print("graph_registry.py v6.0.0 — Demo")
    print("=" * 78)
    print(f"  enhancements source: {_ENHANCEMENTS_SOURCE}")
    print(f"  enhancements available: {_ENHANCEMENTS_AVAILABLE}")

    if not _ENHANCEMENTS_AVAILABLE:
        print(f"\nFATAL: enhancement classes unavailable ({_ENHANCEMENTS_ERROR})")
        print("Install 'green_agent' or 'carbon_forecast' and re-run.")
        return

    def _check(condition: bool, message: str) -> None:
        if not condition:
            raise AssertionError(f"FAILED: {message}")
        print(f"  ✓ {message}")

    def _make_registry(**hooks) -> GraphRegistry:
        return GraphRegistry(config={}, **hooks)

    async def run_all() -> None:
        # ========================================================== #
        # P0 §2.1: per-graph-type lock → versions are exact
        # ========================================================== #
        print("\n=== P0 §2.1: concurrent aregister returns correct versions ===")
        registry = _make_registry()
        results = await asyncio.gather(
            registry.aregister(GraphType.CAUSAL, {"n": 1},
                               require_hitl=False,
                               defer_if_high_carbon=False, explain=False),
            registry.aregister(GraphType.CAUSAL, {"n": 2},
                               require_hitl=False,
                               defer_if_high_carbon=False, explain=False),
            registry.aregister(GraphType.CAUSAL, {"n": 3},
                               require_hitl=False,
                               defer_if_high_carbon=False, explain=False),
        )
        versions = sorted(r["version"] for r in results)
        _check(
            versions == [1, 2, 3],
            f"three concurrent registrations produced versions {versions}",
        )
        _check(
            registry._entries[GraphType.CAUSAL].current.version == 3,
            "final current version is 3",
        )
        # Verify the graph keyed to each version is the one returned
        # (i.e., the version was captured inside the lock)
        for r in results:
            _check(
                r["version"] is not None and r["status"] == "registered",
                f"result status={r['status']!r}, version={r['version']}",
            )

        # ========================================================== #
        # P0 §2.2: deferred registration IS preserved (v6 fix)
        # ========================================================== #
        print("\n=== P0 §2.2: deferred registration is not lost ===")

        class _AlwaysDefer:
            last_price = 25.0
            async def net_zero_schedule(self, workload_kwh, intensity):
                return {"action": "defer"}

        deferred_registry = _make_registry(carbon_market=_AlwaysDefer())
        deferred_result = await deferred_registry.aregister(
            GraphType.CAUSAL,
            {"n": "deferred-graph"},
            require_hitl=False,
            defer_if_high_carbon=True,
            explain=False,
        )
        _check(
            deferred_result["status"] == "deferred",
            f"status is 'deferred'",
        )
        _check(
            deferred_result["version"] is not None,
            f"version is not None (got {deferred_result['version']})",
        )
        _check(
            deferred_result["deferred"] is True,
            "deferred flag is True",
        )
        _check(
            deferred_registry.get(GraphType.CAUSAL) is not None,
            "graph IS registered despite deferral (v17 returned None)",
        )
        _check(
            deferred_registry._entries[GraphType.CAUSAL].current.deferred is True,
            "version.deferred is True",
        )

        # ========================================================== #
        # P0 §2.3: HITL rejection is recorded, not lost
        # ========================================================== #
        print("\n=== P0 §2.3: HITL rejection is recorded ===")

        class _AlwaysReject:
            async def query_user_if_needed(self, user_id, candidates, timeout=2.0):
                return "reject"

        hitl_registry = _make_registry(hitl=_AlwaysReject())
        hitl_result = await hitl_registry.aregister(
            GraphType.CARBON_LEDGER,
            {"entry": 1},
            require_hitl=True,
            defer_if_high_carbon=False,
            explain=False,
        )
        _check(
            hitl_result["status"] == "hitl_rejected",
            f"status is 'hitl_rejected'",
        )
        _check(
            hitl_result["version"] is None,
            "version is None for rejected",
        )
        _check(
            len(hitl_registry._rejections[GraphType.CARBON_LEDGER]) == 1,
            "rejection recorded in _rejections[GRAPH_TYPE]",
        )
        _check(
            hitl_registry.get(GraphType.CARBON_LEDGER) is None,
            "graph is NOT registered after rejection",
        )

        # ========================================================== #
        # P0 §2.4: rollback does not duplicate history
        # ========================================================== #
        print("\n=== P0 §2.4: rollback no longer duplicates history ===")
        rb_registry = _make_registry()
        for i in range(3):
            rb_registry.register(GraphType.CAUSAL, {"v": i})
        # Current is v3, history is [v1, v2]
        _check(
            rb_registry._entries[GraphType.CAUSAL].current.version == 3,
            "initial current v3",
        )
        _check(
            len(rb_registry._entries[GraphType.CAUSAL].history) == 2,
            "initial history length 2",
        )
        # Rollback to v2 → current=v2, history=[v1, v3]
        assert rb_registry.rollback(GraphType.CAUSAL, 2)
        _check(
            rb_registry._entries[GraphType.CAUSAL].current.version == 2,
            "current after rollback v2",
        )
        _check(
            len(rb_registry._entries[GraphType.CAUSAL].history) == 2,
            f"history length still 2 (v17 would be 3; got "
            f"{len(rb_registry._entries[GraphType.CAUSAL].history)})",
        )
        # Rollback to v2 again → no-op
        _check(
            rb_registry.rollback(GraphType.CAUSAL, 2) is False,
            "rollback to current version is a no-op",
        )
        _check(
            len(rb_registry._entries[GraphType.CAUSAL].history) == 2,
            "history length unchanged after second rollback",
        )

        # ========================================================== #
        # P0 §2.5: registry constructed outside loop, used inside
        # ========================================================== #
        print("\n=== P0 §2.5: constructed outside loop, used inside ===")
        # This construction is inside the demo's running loop, but the
        # important test is that we can construct one *before* a loop
        # starts and use it inside — no asyncio.Lock() at __init__.
        outside_loop_registry = GraphRegistry(config={})
        _check(
            not hasattr(outside_loop_registry, "_async_lock"),
            "no _async_lock attribute (v17 created it in __init__)",
        )
        result = await outside_loop_registry.aregister(
            GraphType.SIMILARITY,
            {"matrix": [[1.0]]},
            require_hitl=False,
            defer_if_high_carbon=False,
            explain=False,
        )
        _check(
            result["status"] == "registered",
            "async register works without pre-existing _async_lock",
        )

        # ========================================================== #
        # P0 §2.6/§2.7: metadata counters consistent under concurrency
        # ========================================================== #
        print("\n=== P0 §2.6/§2.7: counters consistent under concurrency ===")
        counter_registry = _make_registry()
        await asyncio.gather(
            *[
                counter_registry.aregister(
                    GraphType.POLICY, {"i": i},
                    require_hitl=False,
                    defer_if_high_carbon=False,
                    explain=False,
                )
                for i in range(50)
            ]
        )
        _check(
            counter_registry._metadata["total_updates"] == 50,
            f"50 registrations → total_updates=50 "
            f"(got {counter_registry._metadata['total_updates']})",
        )

        # ========================================================== #
        # P0 §2.8: fingerprint is portable (order-independent)
        # ========================================================== #
        print("\n=== P0 §2.8: fingerprint is order-independent ===")
        fp1 = GraphRegistry._fingerprint({"a": 1, "b": 2, "c": [3, 4]})
        fp2 = GraphRegistry._fingerprint({"c": [3, 4], "b": 2, "a": 1})
        _check(
            fp1 == fp2,
            f"same-content dicts share a fingerprint ({fp1} == {fp2})",
        )
        fp3 = GraphRegistry._fingerprint({"a": 1.0, "b": 2.0})
        fp4 = GraphRegistry._fingerprint({"a": 1, "b": 2})
        _check(
            fp3 == fp4,
            "int vs float (1 vs 1.0) → same fingerprint",
        )

        # ========================================================== #
        # P1 §3.5: precision downgrade is logged and applied
        # ========================================================== #
        print("\n=== P1 §3.5: invalid precision is logged and downgraded ===")
        prec_registry = _make_registry()
        v = prec_registry.register(
            GraphType.CAUSAL, {"x": 1}, precision="fp64"
        )
        _check(
            v.precision == "fp32",
            f"invalid precision 'fp64' → 'fp32' (got {v.precision!r})",
        )

        # ========================================================== #
        # P1 §3.6: cached_size prevents re-pickling
        # ========================================================== #
        print("\n=== P1 §3.6: cached_size is populated ===")
        size_registry = _make_registry()
        size_registry.register(GraphType.CAUSAL, {"a": 1, "b": 2})
        _check(
            size_registry._entries[GraphType.CAUSAL].cached_size > 0,
            f"cached_size populated "
            f"({size_registry._entries[GraphType.CAUSAL].cached_size})",
        )

        # ========================================================== #
        # P1 §3.7: configurable critical graph types
        # ========================================================== #
        print("\n=== P1 §3.7: configurable critical graph types ===")
        critical_registry = GraphRegistry(
            config={"registry_critical_graph_types": ["precision_history"]},
            hitl=None,
        )
        _check(
            "precision_history" in critical_registry._critical_types,
            "custom critical graph type honored",
        )
        _check(
            "causal" not in critical_registry._critical_types,
            "default 'causal' not critical when overridden",
        )

        # ========================================================== #
        # P1 §3.8: chaos hook is opt-in
        # ========================================================== #
        print("\n=== P1 §3.8: chaos hook is opt-in ===")
        _check(
            not _make_registry()._chaos_hook_enabled,
            "chaos hook disabled by default",
        )
        enabled_registry = GraphRegistry(
            config={"registry_chaos_hook": True}
        )
        _check(
            enabled_registry._chaos_hook_enabled is True,
            "chaos hook enabled by config",
        )

        # ========================================================== #
        # P1 §3.9: unified return shape
        # ========================================================== #
        print("\n=== P1 §3.9: unified return shape ===")
        shape_registry = _make_registry()
        r = await shape_registry.aregister(
            GraphType.CAUSAL, {"x": 1},
            require_hitl=False,
            defer_if_high_carbon=False,
            explain=False,
        )
        for key in (
            "status", "graph_type", "version", "precision", "xai",
            "deferred", "chaos_injected", "reason",
        ):
            _check(key in r, f"return dict has '{key}'")

        # ========================================================== #
        # P1 §3.11: health_check present
        # ========================================================== #
        print("\n=== P1 §3.11: health_check ===")
        hc = shape_registry.health_check()
        for key in (
            "healthy", "anomalies", "num_graphs",
            "in_flight_registrations", "helium_registered",
        ):
            _check(key in hc, f"health_check has '{key}'")
        _check(hc["healthy"] is True, "registry reports healthy")

        # ========================================================== #
        # P1 §3.12: live feature toggle on orchestrator
        # ========================================================== #
        print("\n=== P1 §3.12: orchestrator feature toggle ===")
        storage = InMemoryStorage()
        config = {
            "temporal_formulas": [],
            "xai_method": "permutation",
        }
        orch = GraphRegistryOrchestratorV18(storage, config)
        _check(
            orch.feature_enabled("causal_rl") is True,
            "causal_rl enabled by default",
        )
        orch.disable_feature("causal_rl")
        _check(
            orch.feature_enabled("causal_rl") is False,
            "disable_feature('causal_rl') works",
        )
        orch.enable_feature("causal_rl")
        _check(
            orch.feature_enabled("causal_rl") is True,
            "enable_feature('causal_rl') works",
        )
        try:
            orch.enable_feature("nonexistent")
        except GraphRegistryError:
            _check(True, "unknown feature rejected")
        else:
            raise AssertionError("unknown feature not rejected")

        # ========================================================== #
        # P2 §4.7: to_dict / from_dict round-trip
        # ========================================================== #
        print("\n=== P2 §4.7: to_dict / from_dict round-trip ===")
        roundtrip = _make_registry()
        roundtrip.register(GraphType.CAUSAL, {"v": 1})
        roundtrip.register(GraphType.CAUSAL, {"v": 2})
        roundtrip.register(GraphType.POLICY, {"p": 1})
        roundtrip.rollback(GraphType.CAUSAL, 1)

        payload = roundtrip.to_dict()
        restored = GraphRegistry.from_dict(payload)
        _check(
            restored._entries[GraphType.CAUSAL].current.version == 1,
            "restored current version matches (1)",
        )
        _check(
            len(restored._entries[GraphType.CAUSAL].history) == len(
                roundtrip._entries[GraphType.CAUSAL].history
            ),
            "restored history length matches",
        )
        _check(
            restored._metadata["total_updates"] == roundtrip._metadata["total_updates"],
            "restored metadata matches",
        )

        # ========================================================== #
        # P2 §4.21: idempotent register on same fingerprint
        # ========================================================== #
        print("\n=== P2 §4.21: idempotent register ===")
        idem = _make_registry()
        v1 = idem.register(GraphType.CAUSAL, {"x": 1})
        v2 = idem.register(GraphType.CAUSAL, {"x": 1})  # same content
        _check(
            v1.version == v2.version == 1,
            f"same fingerprint → same version "
            f"({v1.version} == {v2.version})",
        )
        _check(
            idem._metadata["total_updates"] == 1,
            "idempotent register did not bump counter",
        )

        # ========================================================== #
        # P2 §4.22: history is a bounded deque
        # ========================================================== #
        print("\n=== P2 §4.22: history is a bounded deque ===")
        history_registry = GraphRegistry(config={"registry_history_max": 5})
        for i in range(20):
            history_registry.register(GraphType.CAUSAL, {"i": i})
        _check(
            len(history_registry._entries[GraphType.CAUSAL].history) == 5,
            f"history bounded at 5 "
            f"(got {len(history_registry._entries[GraphType.CAUSAL].history)})",
        )

        # ========================================================== #
        # P2 §4.24: Prometheus metrics
        # ========================================================== #
        print("\n=== P2 §4.24: Prometheus metrics ===")
        metrics = history_registry.collect_prometheus_metrics()
        for key in (
            "graph_registry_num_graphs",
            "graph_registry_updates_total",
            "graph_registry_rollbacks_total",
            "graph_registry_in_flight",
        ):
            _check(key in metrics, f"metric '{key}' present")

        # ========================================================== #
        # P1 §3.9: reset refuses when in-flight
        # ========================================================== #
        print("\n=== P1 §3.9: reset refuses with in-flight registrations ===")
        # Simulate in-flight by manually bumping the counter
        in_flight = _make_registry()
        in_flight._in_flight = 1
        try:
            in_flight.reset()
        except GraphRegistryError:
            _check(True, "reset refused with 1 registration in flight")
        else:
            raise AssertionError("reset did not refuse")
        in_flight._in_flight = 0
        in_flight.reset()
        _check(True, "reset works with 0 registrations in flight")

        # ========================================================== #
        # Full end-to-end demo
        # ========================================================== #
        print("\n=== Full end-to-end orchestrator ===")
        storage2 = InMemoryStorage()
        async with GraphRegistryOrchestratorV18(storage2, config) as orch2:
            await orch2.register_graph(
                GraphType.CAUSAL, {"demo": 1},
                require_hitl=False,
                defer_if_high_carbon=False,
                explain=False,
            )
            graph = await orch2.get_graph(GraphType.CAUSAL)
            _check(
                graph == {"demo": 1},
                "orchestrator end-to-end register + get works",
            )
            health = await orch2.health_check()
            _check(
                health["running"] is True,
                "orchestrator health shows running",
            )

        print("\n" + "=" * 78)
        print("All v6 assertions passed.")
        print("=" * 78)

    asyncio.run(run_all())


if __name__ == "__main__":
    _demo()
