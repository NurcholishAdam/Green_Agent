# src/distributed/agent_registry.py

"""
Agent Registry
==============

Central registry of distributed :class:`AgentNode` instances.

Provides:
- Registration / deregistration of agent nodes by ID.
- Lookup by ID, tag, or capability.
- Health tracking (alive / suspect / dead) with TTL-based reaping.
- Cluster-wide statistics for the distributed execution layer.

The registry is decorated with ``@ray.remote`` when Ray is available, so it
can be deployed as a named Ray actor (``get_actor("agent_registry")``). When
Ray is not installed the decorator is a no-op and the registry runs in-process.

Original behaviour preserved
----------------------------
- ``AgentRegistry()`` — constructs an empty registry.
- ``register(...)`` — adds an entry.
- ``lookup(agent_id)`` — returns an entry or ``None``.
- ``list_agents()`` — returns all entries.

Enhancements
------------
- Optional Ray integration via ``@ray.remote`` fallback decorator.
- Configurable via :class:`AgentRegistryConfig` (TTL, heartbeat, capacity).
- Thread-safe via ``RLock`` + bounded per-node history ring-buffer.
- Full validation of every argument; strict / non-strict modes.
- Health states (``alive`` / ``suspect`` / ``dead``) with configurable TTL.
- Tag- and capability-based discovery.
- Deterministic eviction when capacity is exceeded (oldest heartbeat first).
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Sync and async context managers for scoped sessions.
- ``statistics()``, ``__repr__``, custom :class:`AgentRegistryError`, and a
  ``__main__`` smoke test that exercises the full lifecycle.
- Lazy ``%s`` logging throughout.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Deque, Dict, Iterable, List, Mapping, Optional, Set, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional Ray integration
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import ray  # type: ignore

    _RAY_AVAILABLE = True

    def _ray_remote(cls):
        """Wrap ``cls`` with ``@ray.remote`` when Ray is available."""
        return ray.remote(cls)

except ImportError:  # pragma: no cover
    ray = None  # type: ignore[assignment]
    _RAY_AVAILABLE = False

    def _ray_remote(cls):
        """No-op fallback when Ray is not installed."""
        return cls


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class AgentRegistryError(ValueError):
    """Raised for invalid inputs, configuration, or registry failures."""


# --------------------------------------------------------------------------- #
# Health state
# --------------------------------------------------------------------------- #
class AgentHealth(str, Enum):
    """Health classification of a registered agent node."""

    ALIVE = "alive"
    SUSPECT = "suspect"
    DEAD = "dead"
    UNKNOWN = "unknown"


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class AgentRegistryConfig:
    """
    Tunable parameters for the agent registry.

    Centralizes TTL / capacity / heartbeat policy so deployments can calibrate
    without editing the class.
    """

    # Health TTLs (seconds).
    suspect_after_seconds: float = 30.0
    dead_after_seconds: float = 120.0
    reap_interval_seconds: float = 15.0

    # Capacity / history limits.
    max_agents: Optional[int] = 10_000
    max_history_per_agent: int = 100

    # Registry identity (useful for multi-tenant clusters).
    registry_id: str = "default-registry"

    def __post_init__(self) -> None:
        for name in (
            "suspect_after_seconds",
            "dead_after_seconds",
            "reap_interval_seconds",
        ):
            value = getattr(self, name)
            if value <= 0:
                raise AgentRegistryError(f"{name} must be > 0 (got {value}).")
        if self.dead_after_seconds <= self.suspect_after_seconds:
            raise AgentRegistryError(
                "dead_after_seconds must be > suspect_after_seconds."
            )
        if self.max_agents is not None and self.max_agents <= 0:
            raise AgentRegistryError("max_agents must be > 0 or None.")
        if self.max_history_per_agent <= 0:
            raise AgentRegistryError("max_history_per_agent must be > 0.")
        if not isinstance(self.registry_id, str) or not self.registry_id:
            raise AgentRegistryError(
                "registry_id must be a non-empty string."
            )


# --------------------------------------------------------------------------- #
# Agent entry
# --------------------------------------------------------------------------- #
@dataclass
class AgentEntry:
    """
    Registry record for a single agent node.

    The ``handle`` field holds the live ``AgentNode`` reference (or Ray actor
    handle). It is intentionally excluded from serialization — see
    :meth:`to_dict`.
    """

    agent_id: str
    node_id: str
    handle: Any = None
    tags: Tuple[str, ...] = ()
    capabilities: Tuple[str, ...] = ()
    metadata: Dict[str, Any] = field(default_factory=dict)
    registered_at: float = field(default_factory=time.time)
    last_heartbeat: float = field(default_factory=time.time)
    health: AgentHealth = AgentHealth.ALIVE
    heartbeat_history: Deque[float] = field(
        default_factory=lambda: deque(maxlen=100)
    )

    def to_dict(self, *, include_handle: bool = False) -> Dict[str, Any]:
        """Return a JSON-safe representation of this entry."""
        return {
            "agent_id": self.agent_id,
            "node_id": self.node_id,
            "tags": list(self.tags),
            "capabilities": list(self.capabilities),
            "metadata": dict(self.metadata),
            "registered_at": self.registered_at,
            "last_heartbeat": self.last_heartbeat,
            "health": self.health.value if isinstance(self.health, AgentHealth) else str(self.health),
            "handle_included": bool(include_handle and self.handle is not None),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "AgentEntry":
        if not isinstance(data, Mapping):
            raise AgentRegistryError(
                f"AgentEntry.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        health_raw = data.get("health", "unknown")
        try:
            health = AgentHealth(str(health_raw))
        except ValueError:
            health = AgentHealth.UNKNOWN

        return cls(
            agent_id=str(data["agent_id"]),
            node_id=str(data["node_id"]),
            handle=None,
            tags=tuple(data.get("tags", ())),
            capabilities=tuple(data.get("capabilities", ())),
            metadata=dict(data.get("metadata", {})),
            registered_at=float(data.get("registered_at", time.time())),
            last_heartbeat=float(data.get("last_heartbeat", time.time())),
            health=health,
        )


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
@_ray_remote
class AgentRegistry:
    """
    Central registry of distributed agent nodes.

    Thread-safe, serializable, bounded in memory, and Ray-deployable. All
    original behaviour is preserved; new parameters are keyword-only with
    backward-compatible defaults.

    Parameters
    ----------
    config : AgentRegistryConfig, optional
        TTL / capacity / history policy. Defaults to ``AgentRegistryConfig()``.
    strict : bool, default True
        If True, invalid inputs raise :class:`AgentRegistryError`.
        If False, invalid inputs are logged and coerced.
    """

    # ------------------------------------------------------------------ init
    def __init__(
        self,
        config: Optional[AgentRegistryConfig] = None,
        *,
        strict: bool = True,
    ) -> None:
        self._config: AgentRegistryConfig = config or AgentRegistryConfig()
        self._strict: bool = bool(strict)

        self._lock = threading.RLock()
        self._agents: Dict[str, AgentEntry] = {}
        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None
        self._started_at: float = time.time()
        self._last_reap_at: float = time.time()

        logger.debug(
            "AgentRegistry initialized (registry_id=%s, strict=%s, "
            "suspect_after=%.1fs, dead_after=%.1fs, ray=%s)",
            self._config.registry_id,
            self._strict,
            self._config.suspect_after_seconds,
            self._config.dead_after_seconds,
            _RAY_AVAILABLE,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> AgentRegistryConfig:
        return self._config

    @property
    def registry_id(self) -> str:
        return self._config.registry_id

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._agents)

    @property
    def agent_ids(self) -> List[str]:
        with self._lock:
            return list(self._agents.keys())

    # ---------------------------------------------------------- public API
    def register(
        self,
        agent_id: str,
        node_id: str,
        handle: Any = None,
        *,
        tags: Optional[Iterable[str]] = None,
        capabilities: Optional[Iterable[str]] = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> AgentEntry:
        """
        Register an agent node.

        Parameters
        ----------
        agent_id : str
            Unique agent identifier. Re-registering the same ID updates the
            existing entry in place.
        node_id : str
            Physical / logical node the agent runs on.
        handle : Any, optional
            Live ``AgentNode`` instance or Ray actor handle.
        tags : iterable of str, optional
            Free-form labels for tag-based discovery.
        capabilities : iterable of str, optional
            Declared capabilities (e.g. ``{"gpu", "helium-dependent"}``).
        metadata : Mapping, optional
            Arbitrary extra fields.

        Returns
        -------
        AgentEntry
            The stored entry (mutable reference).
        """
        self._validate_id("agent_id", agent_id)
        self._validate_id("node_id", node_id)

        tags_t = tuple(self._normalize_str_iterable("tags", tags))
        caps_t = tuple(self._normalize_str_iterable("capabilities", capabilities))
        meta = dict(metadata) if isinstance(metadata, Mapping) else {}

        now = time.time()

        with self._lock:
            existing = self._agents.get(agent_id)
            if existing is not None:
                existing.node_id = node_id
                existing.handle = handle if handle is not None else existing.handle
                existing.tags = tags_t or existing.tags
                existing.capabilities = caps_t or existing.capabilities
                if meta:
                    existing.metadata.update(meta)
                existing.last_heartbeat = now
                existing.health = AgentHealth.ALIVE
                existing.heartbeat_history.append(now)
                logger.debug("Agent %s re-registered.", agent_id)
                return existing

            entry = AgentEntry(
                agent_id=agent_id,
                node_id=node_id,
                handle=handle,
                tags=tags_t,
                capabilities=caps_t,
                metadata=meta,
                registered_at=now,
                last_heartbeat=now,
                health=AgentHealth.ALIVE,
                heartbeat_history=deque(
                    [now], maxlen=self._config.max_history_per_agent
                ),
            )
            self._agents[agent_id] = entry
            self._enforce_capacity_locked()

        logger.debug(
            "Agent registered: id=%s node=%s tags=%s caps=%s",
            agent_id,
            node_id,
            tags_t,
            caps_t,
        )
        return entry

    def deregister(self, agent_id: str) -> bool:
        """Remove an agent from the registry. Returns True if it existed."""
        self._validate_id("agent_id", agent_id)
        with self._lock:
            removed = self._agents.pop(agent_id, None)
        if removed is not None:
            logger.debug("Agent deregistered: id=%s", agent_id)
            return True
        return False

    def lookup(self, agent_id: str) -> Optional[AgentEntry]:
        """Return the entry for ``agent_id`` or ``None``."""
        self._validate_id("agent_id", agent_id)
        with self._lock:
            return self._agents.get(agent_id)

    def require(self, agent_id: str) -> AgentEntry:
        """Return the entry for ``agent_id`` or raise :class:`AgentRegistryError`."""
        entry = self.lookup(agent_id)
        if entry is None:
            raise AgentRegistryError(f"Agent '{agent_id}' not found.")
        return entry

    def heartbeat(self, agent_id: str) -> AgentHealth:
        """
        Record a heartbeat for ``agent_id`` and return the new health state.

        Raises
        ------
        AgentRegistryError
            If the agent is not registered.
        """
        self._validate_id("agent_id", agent_id)
        with self._lock:
            entry = self._agents.get(agent_id)
            if entry is None:
                raise AgentRegistryError(
                    f"Cannot heartbeat unknown agent '{agent_id}'."
                )
            now = time.time()
            entry.last_heartbeat = now
            entry.heartbeat_history.append(now)
            entry.health = AgentHealth.ALIVE
        return AgentHealth.ALIVE

    def list_agents(
        self,
        *,
        health: Optional[AgentHealth] = None,
        tags: Optional[Iterable[str]] = None,
        capabilities: Optional[Iterable[str]] = None,
        include_handle: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Return a list of entries matching the given filters.

        Filters are conjunctive (all supplied filters must match). ``health``
        uses the last computed state; run :meth:`reap` first to refresh
        stale entries.
        """
        want_tags = set(self._normalize_str_iterable("tags", tags))
        want_caps = set(self._normalize_str_iterable("capabilities", capabilities))

        with self._lock:
            entries = list(self._agents.values())

        results: List[Dict[str, Any]] = []
        for entry in entries:
            if health is not None and entry.health != health:
                continue
            if want_tags and not want_tags.issubset(set(entry.tags)):
                continue
            if want_caps and not want_caps.issubset(set(entry.capabilities)):
                continue
            results.append(entry.to_dict(include_handle=include_handle))
        return results

    def find_by_tag(self, tag: str) -> List[AgentEntry]:
        """Return all entries carrying ``tag``."""
        self._validate_id("tag", tag)
        with self._lock:
            return [e for e in self._agents.values() if tag in e.tags]

    def find_by_capability(self, capability: str) -> List[AgentEntry]:
        """Return all entries declaring ``capability``."""
        self._validate_id("capability", capability)
        with self._lock:
            return [
                e for e in self._agents.values()
                if capability in e.capabilities
            ]

    def alive_agents(self) -> List[AgentEntry]:
        """Return all entries whose current health is ``ALIVE``."""
        with self._lock:
            return [
                e for e in self._agents.values()
                if e.health == AgentHealth.ALIVE
            ]

    # ---------------------------------------------------------- reaping
    def reap(self) -> Dict[str, int]:
        """
        Recompute health for every entry based on heartbeat freshness.

        - ``now - last_heartbeat > dead_after`` → ``DEAD``
        - ``now - last_heartbeat > suspect_after`` → ``SUSPECT``
        - otherwise → ``ALIVE``

        Returns a dict of ``{alive, suspect, dead}`` counts.
        """
        cfg = self._config
        now = time.time()
        alive = suspect = dead = 0

        with self._lock:
            for entry in self._agents.values():
                age = now - entry.last_heartbeat
                if age > cfg.dead_after_seconds:
                    entry.health = AgentHealth.DEAD
                    dead += 1
                elif age > cfg.suspect_after_seconds:
                    entry.health = AgentHealth.SUSPECT
                    suspect += 1
                else:
                    entry.health = AgentHealth.ALIVE
                    alive += 1
            self._last_reap_at = now

        return {"alive": alive, "suspect": suspect, "dead": dead}

    def purge_dead(self) -> int:
        """Remove all entries currently marked ``DEAD``. Returns count removed."""
        with self._lock:
            dead_ids = [
                aid for aid, e in self._agents.items()
                if e.health == AgentHealth.DEAD
            ]
            for aid in dead_ids:
                self._agents.pop(aid, None)
        if dead_ids:
            logger.debug("Purged %d dead agent(s).", len(dead_ids))
        return len(dead_ids)

    # ---------------------------------------------------------- capacity
    def _enforce_capacity_locked(self) -> None:
        cap = self._config.max_agents
        if cap is None or len(self._agents) <= cap:
            return
        # Evict the oldest-heartbeat entry first.
        ordered = sorted(
            self._agents.items(), key=lambda kv: kv[1].last_heartbeat
        )
        excess = len(self._agents) - cap
        for agent_id, _ in ordered[:excess]:
            self._agents.pop(agent_id, None)
            logger.debug(
                "Evicted agent %s to respect max_agents=%d.", agent_id, cap
            )

    # ---------------------------------------------------------- validation
    @staticmethod
    def _validate_id(name: str, value: Any) -> None:
        if not isinstance(value, str) or not value:
            raise AgentRegistryError(f"{name} must be a non-empty string.")

    def _normalize_str_iterable(
        self, name: str, value: Optional[Iterable[str]]
    ) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            # A bare string is coerced to a single-element list.
            return [value]
        try:
            return [str(v) for v in value if v is not None]
        except TypeError as exc:
            msg = (
                f"{name} must be an iterable of strings, "
                f"got {type(value).__name__}."
            )
            if self._strict:
                raise AgentRegistryError(msg) from exc
            logger.warning("%s Ignoring.", msg)
            return []

    # ---------------------------------------------------------- stats
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the registry."""
        with self._lock:
            entries = list(self._agents.values())
            last_reap = self._last_reap_at

        if not entries:
            return {
                "registry_id": self._config.registry_id,
                "size": 0,
                "by_health": {},
                "by_node": {},
                "unique_tags": [],
                "unique_capabilities": [],
                "oldest_heartbeat_age_seconds": None,
                "last_reap_ago_seconds": time.time() - last_reap,
                "uptime_seconds": time.time() - self._started_at,
            }

        by_health: Dict[str, int] = {}
        by_node: Dict[str, int] = {}
        tags: Set[str] = set()
        caps: Set[str] = set()
        now = time.time()
        for e in entries:
            by_health[e.health.value] = by_health.get(e.health.value, 0) + 1
            by_node[e.node_id] = by_node.get(e.node_id, 0) + 1
            tags.update(e.tags)
            caps.update(e.capabilities)

        return {
            "registry_id": self._config.registry_id,
            "size": len(entries),
            "by_health": by_health,
            "by_node": by_node,
            "unique_tags": sorted(tags),
            "unique_capabilities": sorted(caps),
            "oldest_heartbeat_age_seconds": now - min(
                e.last_heartbeat for e in entries
            ),
            "last_reap_ago_seconds": now - last_reap,
            "uptime_seconds": now - self._started_at,
        }

    def clear(self) -> int:
        """Remove every entry. Returns the number of entries removed."""
        with self._lock:
            n = len(self._agents)
            self._agents.clear()
        logger.debug("AgentRegistry cleared (%d entries).", n)
        return n

    def reset(self) -> None:
        """Reset the registry to its initial empty state."""
        self.clear()
        self._started_at = time.time()
        self._last_reap_at = time.time()
        logger.debug("AgentRegistry reset.")

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_handles: bool = False) -> Dict[str, Any]:
        with self._lock:
            entries = [
                e.to_dict(include_handle=include_handles)
                for e in self._agents.values()
            ]
        return {
            "registry_id": self._config.registry_id,
            "config": asdict(self._config),
            "strict": self._strict,
            "ray_available": _RAY_AVAILABLE,
            "started_at": self._started_at,
            "last_reap_at": self._last_reap_at,
            "entries": entries,
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        handles: Optional[Mapping[str, Any]] = None,
    ) -> "AgentRegistry":
        """
        Rebuild an ``AgentRegistry`` from a dict produced by :meth:`to_dict`.

        ``handles`` maps ``agent_id → live handle`` (an ``AgentNode`` instance
        or Ray actor). Live handles are not serialized, so they must be
        re-injected here.
        """
        if not isinstance(data, Mapping):
            raise AgentRegistryError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = AgentRegistryConfig(
            suspect_after_seconds=float(
                cfg_data.get("suspect_after_seconds", 30.0)
            ),
            dead_after_seconds=float(
                cfg_data.get("dead_after_seconds", 120.0)
            ),
            reap_interval_seconds=float(
                cfg_data.get("reap_interval_seconds", 15.0)
            ),
            max_agents=cfg_data.get("max_agents", 10_000),
            max_history_per_agent=int(
                cfg_data.get("max_history_per_agent", 100)
            ),
            registry_id=str(cfg_data.get("registry_id", "default-registry")),
        )

        registry = cls(config=cfg, strict=bool(data.get("strict", True)))
        handles = dict(handles or {})

        with registry._lock:
            for entry_data in data.get("entries", []):
                entry = AgentEntry.from_dict(entry_data)
                entry.handle = handles.get(entry.agent_id)
                entry.heartbeat_history = deque(
                    [entry.last_heartbeat],
                    maxlen=cfg.max_history_per_agent,
                )
                registry._agents[entry.agent_id] = entry
            registry._started_at = float(data.get("started_at", time.time()))
            registry._last_reap_at = float(data.get("last_reap_at", time.time()))
        return registry

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        handles: Optional[Mapping[str, Any]] = None,
    ) -> "AgentRegistry":
        try:
            return cls.from_dict(json.loads(payload), handles=handles)
        except json.JSONDecodeError as exc:
            raise AgentRegistryError(f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "AgentRegistry":
        self._ctx_start = time.perf_counter()
        logger.debug(
            "Entering scoped AgentRegistry session (%s).",
            self._config.registry_id,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "AgentRegistry scope exited with %s after %.4fs.",
                exc_type.__name__,
                elapsed,
            )
            return
        logger.info(
            "AgentRegistry scope closed in %.4fs (%d agent(s)).",
            elapsed,
            self.size,
        )

    async def __aenter__(self) -> "AgentRegistry":
        self._async_ctx_start = time.perf_counter()
        logger.debug("Entering async AgentRegistry session.")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._async_ctx_start
            if self._async_ctx_start is not None
            else time.perf_counter()
        )
        self._async_ctx_start = None
        if exc_type is not None:
            logger.warning(
                "Async AgentRegistry scope exited with %s after %.4fs.",
                exc_type.__name__,
                elapsed,
            )
            return
        logger.info(
            "Async AgentRegistry scope closed in %.4fs (%d agent(s)).",
            elapsed,
            self.size,
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "AgentRegistry("
                f"registry_id={self._config.registry_id!r}, "
                f"size={len(self._agents)}, "
                f"strict={self._strict}, "
                f"ray={_RAY_AVAILABLE})"
            )

    def __contains__(self, agent_id: object) -> bool:
        if not isinstance(agent_id, str):
            return False
        with self._lock:
            return agent_id in self._agents

    def __len__(self) -> int:
        return self.size


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "AgentRegistry",
    "AgentRegistryConfig",
    "AgentRegistryError",
    "AgentEntry",
    "AgentHealth",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m distributed.agent_registry
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    class _FakeNode:
        def __init__(self, node_id: str) -> None:
            self.node_id = node_id

        def __repr__(self) -> str:
            return f"_FakeNode({self.node_id})"

    # ---- Happy path --------------------------------------------------- #
    registry = AgentRegistry(
        config=AgentRegistryConfig(
            suspect_after_seconds=1.0,
            dead_after_seconds=3.0,
            max_history_per_agent=5,
        ),
    )

    registry.register(
        agent_id="agent-001",
        node_id="node-a",
        handle=_FakeNode("node-a"),
        tags=["nlp", "gpu"],
        capabilities=["pareto-scoring", "helium-aware"],
        metadata={"region": "eu-west-1"},
    )
    registry.register(
        agent_id="agent-002",
        node_id="node-b",
        handle=_FakeNode("node-b"),
        tags=["vision"],
        capabilities=["pareto-scoring"],
    )
    registry.register(
        agent_id="agent-003",
        node_id="node-a",
        handle=_FakeNode("node-a"),
        tags=["nlp"],
        capabilities=["carbon-aware"],
    )

    print("registry     :", registry)
    print("size         :", registry.size)
    print("contains     :", "agent-001" in registry, "agent-999" in registry)

    # ---- Lookup / discovery ------------------------------------------- #
    entry = registry.lookup("agent-001")
    print("lookup 001   :", entry.to_dict() if entry else None)
    print("nlp agents   :", [e.agent_id for e in registry.find_by_tag("nlp")])
    print(
        "pareto caps  :",
        [e.agent_id for e in registry.find_by_capability("pareto-scoring")],
    )
    print(
        "node-a       :",
        [a["agent_id"] for a in registry.list_agents(health=AgentHealth.ALIVE)
         if a["node_id"] == "node-a"],
    )

    # ---- Heartbeat + reap --------------------------------------------- #
    registry.heartbeat("agent-002")
    time.sleep(1.2)  # cross the suspect TTL
    reaped = registry.reap()
    print("reap#1       :", reaped)

    time.sleep(2.0)  # cross the dead TTL for the non-heartbeated agents
    reaped = registry.reap()
    print("reap#2       :", reaped)
    print("alive only   :", [e.agent_id for e in registry.alive_agents()])

    purged = registry.purge_dead()
    print("purged       :", purged, "remaining:", registry.size)

    # ---- Statistics --------------------------------------------------- #
    print("statistics   :", registry.statistics())

    # ---- Serialization round-trip ------------------------------------- #
    payload = registry.to_json()
    restored = AgentRegistry.from_json(payload)
    assert restored.to_dict() == registry.to_dict()
    print("Serialization round-trip OK.")

    # ---- Re-registration updates in place ----------------------------- #
    registry.register(
        agent_id="agent-001",
        node_id="node-a",
        tags=["nlp", "gpu", "updated"],
    )
    print("update check :", registry.lookup("agent-001").tags)

    # ---- Context managers --------------------------------------------- #
    with AgentRegistry() as scoped:
        scoped.register(agent_id="ctx-1", node_id="node-c")
        scoped.register(agent_id="ctx-2", node_id="node-c")
        assert scoped.size == 2
    print("Sync context OK.")

    async def _async_scope() -> None:
        async with AgentRegistry() as scoped:
            scoped.register(agent_id="actx-1", node_id="node-d")
            assert scoped.size == 1

    import asyncio
    asyncio.run(_async_scope())
    print("Async context OK.")

    # ---- Validation failures ------------------------------------------ #
    for bad_kwargs in (
        dict(agent_id="", node_id="node-x"),
        dict(agent_id="a", node_id=""),
    ):
        try:
            registry.register(**bad_kwargs)  # type: ignore[arg-type]
        except AgentRegistryError as exc:
            print("Rejected as expected:", exc)
        else:  # pragma: no cover
            raise AssertionError(f"Expected rejection: {bad_kwargs!r}")

    try:
        registry.heartbeat("unknown-agent")
    except AgentRegistryError as exc:
        print("Rejected heartbeat:", exc)

    try:
        registry.require("not-registered")
    except AgentRegistryError as exc:
        print("Rejected require  :", exc)

    # Config validation.
    for bad_cfg in (
        dict(suspect_after_seconds=0),
        dict(suspect_after_seconds=10, dead_after_seconds=5),
        dict(max_agents=0),
        dict(registry_id=""),
    ):
        try:
            AgentRegistryConfig(**bad_cfg)  # type: ignore[arg-type]
        except AgentRegistryError as exc:
            print("Rejected config   :", exc)
        else:  # pragma: no cover
            raise AssertionError(f"Expected config rejection: {bad_cfg!r}")

    print("\nSmoke test passed.")
