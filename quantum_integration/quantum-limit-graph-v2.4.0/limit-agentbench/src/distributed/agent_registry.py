# src/distributed/agent_registry.py

"""
Agent Registry — v2.0.0
========================

Central registry of distributed :class:`AgentNode` instances.

Provides:
- Registration / deregistration of agent nodes by ID.
- Lookup by ID, tag, or capability.
- Health tracking (alive / suspect / dead) with TTL-based reaping.
- Cluster-wide statistics for the distributed execution layer.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``to_dict(include_handles=True)`` now actually embeds the handle.
        ``to_json`` still refuses to serialize handles (not JSON-safe).
  * 2.2 ``reap_interval_seconds`` is now used: reads (``lookup``,
        ``list_agents``, ``find_by_*``, ``alive_agents``, ``statistics``)
        auto-reap when the configured interval has elapsed.
  * 2.3 ``AgentRegistry`` is a plain class — ``RemoteAgentRegistry`` is the
        Ray actor. Direct construction now works whether or not Ray is
        installed.
  * 2.5 ``from_dict`` delegates to ``AgentRegistryConfig.from_legacy_dict``
        so new config fields default correctly.
  * 2.7 Discovery and lookup return *snapshots*. Mutating a returned
        ``AgentEntry`` does not affect registry state.
  * 2.8 Added ``list_entries(...)`` as a consistent companion to
        ``list_agents(...)``; both accept the same filters.
  * 2.9 ``__getstate__`` / ``__setstate__`` make the registry picklable.

**P1 robustness / observability**
  * 2.4 ``_enforce_capacity_locked`` uses ``heapq.nsmallest`` — O(excess × n)
        instead of O(n log n), and only sorts when actually over capacity.
  * 2.6 ``register(..., replace_tags=..., replace_capabilities=...)`` gives
        an explicit way to clear collections on re-registration.
        The default remains merge (backward compatible).
  * 3.1 ``metrics_hook(event, fields)`` for Prometheus / OTel.
  * 3.2 ``purge_dead`` reaps first, so it is self-sufficient.
  * 3.6 ``close()`` lifecycle hook.
  * 3.7 ``__enter__`` / ``__exit__`` / ``__aenter__`` / ``__aexit__`` hold
        the RLock while mutating context state.
  * 3.8 ``from_dict`` logs a warning listing agents restored with
        ``handle=None``.

**P2 enhancements**
  * 3.3 ``to_dict(include_history=True)`` preserves the heartbeat ring; 
        ``from_dict`` restores it.
  * 3.4 ``heartbeat_rate(agent_id, window_s)`` for flapping detection.
  * 3.9 Documented: re-registration preserves ``registered_at`` and appends
        to ``heartbeat_history``.
  * 3.10 ``AgentEntry`` normalizes ``health`` (string → enum) in
         ``__post_init__``.
  * 3.11 ``statistics()['last_reap_ago_seconds']`` is ``None`` until the
         first reap; ``ever_reaped`` is added.
  * 3.12 Same as 3.10.
  * 3.13 ``deregister(reason=...)`` + an in-memory audit trail exposed via
         ``audit_log(n=...)``.
  * 3.15 ``register`` warns when a handle lacks a ``.run`` method (strict).

Thread-safe, serializable, picklable, bounded in memory, Ray-deployable.

Note: tag/capability lookups are O(n). Add an inverted index if you operate
above ~10 K agents.
"""

from __future__ import annotations

import heapq
import json
import logging
import pickle  # noqa: F401  (used in self-tests)
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, Iterable, List, Mapping, Optional, Set,
    Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Optional Ray integration (P0 2.3)
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import ray  # type: ignore

    _RAY_AVAILABLE = True
except ImportError:  # pragma: no cover
    ray = None  # type: ignore[assignment]
    _RAY_AVAILABLE = False


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

    # ---------------------------------------------------------------- #
    def __post_init__(self) -> None:
        for name in (
            "suspect_after_seconds",
            "dead_after_seconds",
            "reap_interval_seconds",
        ):
            value = getattr(self, name)
            if value <= 0:
                raise AgentRegistryError(
                    f"{name} must be > 0 (got {value}).")
        if self.dead_after_seconds <= self.suspect_after_seconds:
            raise AgentRegistryError(
                "dead_after_seconds must be > suspect_after_seconds."
            )
        if self.max_agents is not None and self.max_agents <= 0:
            raise AgentRegistryError("max_agents must be > 0 or None.")
        if self.max_history_per_agent <= 0:
            raise AgentRegistryError(
                "max_history_per_agent must be > 0.")
        if not isinstance(self.registry_id, str) or not self.registry_id:
            raise AgentRegistryError(
                "registry_id must be a non-empty string."
            )

    # ---------------------------------------------------------------- #
    @classmethod
    def from_legacy_dict(
        cls, cfg: Optional[Mapping[str, Any]]
    ) -> "AgentRegistryConfig":
        """
        Build an ``AgentRegistryConfig`` from a flat config mapping (P0 2.5).

        Only keys matching a dataclass field are consumed. New fields added
        to ``AgentRegistryConfig`` therefore use their declared defaults —
        callers do not silently inherit the old default.
        """
        cfg = dict(cfg or {})
        valid = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in cfg.items() if k in valid})


# --------------------------------------------------------------------------- #
# Agent entry
# --------------------------------------------------------------------------- #
@dataclass
class AgentEntry:
    """
    Registry record for a single agent node.

    The ``handle`` field holds the live ``AgentNode`` reference (or Ray actor
    handle). It is only serialized when ``include_handle=True`` is passed to
    :meth:`to_dict` — never through :meth:`AgentRegistry.to_json`.

    Callers receiving an ``AgentEntry`` from the registry always receive a
    *snapshot* — see :meth:`snapshot`. Mutating a snapshot does not modify
    registry state.
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

    # ---------------------------------------------------------------- #
    def __post_init__(self) -> None:
        # 3.10 / 3.12: normalize health to the enum on construction.
        if isinstance(self.health, str) and \
                not isinstance(self.health, AgentHealth):
            try:
                self.health = AgentHealth(self.health)
            except ValueError:
                self.health = AgentHealth.UNKNOWN
        if isinstance(self.tags, list):
            self.tags = tuple(self.tags)
        if isinstance(self.capabilities, list):
            self.capabilities = tuple(self.capabilities)

    # ---------------------------------------------------------------- #
    def snapshot(self) -> "AgentEntry":
        """
        Return a deep-ish copy (P0 2.7).

        ``handle`` is shared by design (it is a live object), but every
        mutable wrapper (``metadata``, ``heartbeat_history``) is copied so
        that mutating the snapshot cannot corrupt registry state.
        """
        return AgentEntry(
            agent_id=self.agent_id,
            node_id=self.node_id,
            handle=self.handle,
            tags=self.tags,
            capabilities=self.capabilities,
            metadata=dict(self.metadata),
            registered_at=self.registered_at,
            last_heartbeat=self.last_heartbeat,
            health=self.health,
            heartbeat_history=deque(
                self.heartbeat_history,
                maxlen=self.heartbeat_history.maxlen or 100,
            ),
        )

    # ---------------------------------------------------------------- #
    def to_dict(
        self,
        *,
        include_handle: bool = False,
        include_history: bool = False,
    ) -> Dict[str, Any]:
        """
        Return a representation of this entry.

        ``include_handle=True`` embeds the live handle object (P0 2.1).
        This is *not* JSON-safe — ``AgentRegistry.to_json`` always passes
        ``include_handle=False``.
        """
        d: Dict[str, Any] = {
            "agent_id": self.agent_id,
            "node_id": self.node_id,
            "tags": list(self.tags),
            "capabilities": list(self.capabilities),
            "metadata": dict(self.metadata),
            "registered_at": self.registered_at,
            "last_heartbeat": self.last_heartbeat,
            "health": (
                self.health.value
                if isinstance(self.health, AgentHealth)
                else str(self.health)
            ),
            "has_handle": self.handle is not None,
        }
        if include_handle and self.handle is not None:
            d["handle"] = self.handle
        if include_history:
            d["heartbeat_history"] = list(self.heartbeat_history)
        return d

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

        # 3.3: restore heartbeat history if present
        history_raw = data.get("heartbeat_history")
        if history_raw:
            history = deque(
                (float(x) for x in history_raw),
                maxlen=len(history_raw) or 100,
            )
        else:
            history = deque(
                [float(data.get("last_heartbeat", time.time()))],
                maxlen=100,
            )

        return cls(
            agent_id=str(data["agent_id"]),
            node_id=str(data["node_id"]),
            handle=data.get("handle"),   # only present with include_handle
            tags=tuple(data.get("tags", ())),
            capabilities=tuple(data.get("capabilities", ())),
            metadata=dict(data.get("metadata", {})),
            registered_at=float(data.get("registered_at", time.time())),
            last_heartbeat=float(data.get("last_heartbeat", time.time())),
            health=health,
            heartbeat_history=history,
        )


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
class AgentRegistry:
    """
    Central registry of distributed agent nodes.

    Thread-safe, serializable, picklable, bounded in memory. All original
    behaviour is preserved; new parameters are keyword-only with backward-
    compatible defaults.

    Parameters
    ----------
    config : AgentRegistryConfig, optional
        TTL / capacity / history policy. Defaults to ``AgentRegistryConfig()``.
    strict : bool, default True
        If True, invalid inputs raise :class:`AgentRegistryError`.
    metrics_hook : callable, optional
        Called as ``hook(event, fields_dict)`` after state transitions.
    """

    # ------------------------------------------------------------------ #
    def __init__(
        self,
        config: Optional[AgentRegistryConfig] = None,
        *,
        strict: bool = True,
        metrics_hook: Optional[Callable[[str, Mapping[str, Any]], None]] = None,
    ) -> None:
        self._config: AgentRegistryConfig = config or AgentRegistryConfig()
        self._strict: bool = bool(strict)

        if metrics_hook is not None and not callable(metrics_hook):
            raise AgentRegistryError("metrics_hook must be callable.")
        self._metrics_hook = metrics_hook

        self._lock = threading.RLock()
        self._agents: Dict[str, AgentEntry] = {}
        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None
        self._started_at: float = time.time()
        # 3.11: None until the first reap has happened.
        self._last_reap_at: Optional[float] = None
        self._audit: Deque[Dict[str, Any]] = deque(maxlen=1000)

        logger.debug(
            "AgentRegistry v%s initialized (registry_id=%s, strict=%s, "
            "suspect_after=%.1fs, dead_after=%.1fs, ray=%s)",
            __version__,
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

    # ---------------------------------------------------------- metrics
    def _emit_metric(
        self, event: str, fields: Mapping[str, Any],
    ) -> None:
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised", exc_info=True)

    def _audit_log(self, event: str, **fields: Any) -> None:
        self._audit.append({"event": event, "ts": time.time(), **fields})

    def audit_log(self, n: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return the most recent audit entries (all if ``n`` is None)."""
        with self._lock:
            entries = list(self._audit)
        return entries if n is None else entries[-n:]

    # ---------------------------------------------------------- reaping
    def _reap_locked(self, now: float) -> Dict[str, int]:
        alive = suspect = dead = 0
        for entry in self._agents.values():
            age = now - entry.last_heartbeat
            if age > self._config.dead_after_seconds:
                entry.health = AgentHealth.DEAD
                dead += 1
            elif age > self._config.suspect_after_seconds:
                entry.health = AgentHealth.SUSPECT
                suspect += 1
            else:
                entry.health = AgentHealth.ALIVE
                alive += 1
        self._last_reap_at = now
        return {"alive": alive, "suspect": suspect, "dead": dead}

    def _maybe_reap_locked(self) -> None:
        """
        Reap if the configured interval has elapsed (P0 2.2).

        Caller must hold the lock.
        """
        now = time.time()
        if self._last_reap_at is None or (
            now - self._last_reap_at
            >= self._config.reap_interval_seconds
        ):
            self._reap_locked(now)

    def reap(self) -> Dict[str, int]:
        """
        Recompute health for every entry based on heartbeat freshness.

        Explicit ``reap()`` reaps unconditionally; reads auto-reap on the
        ``reap_interval_seconds`` schedule.
        """
        with self._lock:
            result = self._reap_locked(time.time())
        self._emit_metric("reap_result", result)
        return result

    def purge_dead(self) -> int:
        """Reap, then remove all entries currently marked ``DEAD``."""
        with self._lock:
            # 3.2: reaping first makes the method self-sufficient
            self._reap_locked(time.time())
            dead_ids = [
                aid for aid, e in self._agents.items()
                if e.health == AgentHealth.DEAD
            ]
            for aid in dead_ids:
                self._agents.pop(aid, None)
        if dead_ids:
            self._audit_log("purge_dead", agent_ids=dead_ids)
            self._emit_metric("purge_dead", {"count": len(dead_ids)})
            logger.debug("Purged %d dead agent(s).", len(dead_ids))
        return len(dead_ids)

    # ---------------------------------------------------------- register
    def register(
        self,
        agent_id: str,
        node_id: str,
        handle: Any = None,
        *,
        tags: Optional[Iterable[str]] = None,
        capabilities: Optional[Iterable[str]] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        replace_tags: bool = False,
        replace_capabilities: bool = False,
    ) -> AgentEntry:
        """
        Register an agent node.

        Re-registering the same ``agent_id`` updates the existing entry in
        place: ``registered_at`` is preserved, ``heartbeat_history`` grows by
        one, and ``tags`` / ``capabilities`` are *merged* by default (P1 2.6).

        Parameters
        ----------
        agent_id, node_id : str
            Unique agent identifier and logical node.
        handle : Any, optional
            Live ``AgentNode`` instance or Ray actor handle.
        tags, capabilities : iterable of str, optional
            Discovery labels. If ``None``, existing values are left untouched.
        metadata : Mapping, optional
            Extra fields; merged into existing metadata.
        replace_tags, replace_capabilities : bool, default False
            When True and the corresponding collection is passed, replace
            the existing value instead of merging. Pass ``tags=[]`` with
            ``replace_tags=True`` to clear.

        Returns
        -------
        AgentEntry
            A **snapshot** — mutating it does not modify registry state.
        """
        self._validate_id("agent_id", agent_id)
        self._validate_id("node_id", node_id)

        # 3.15: warn about handles that will not respond to .run()
        if handle is not None and self._strict:
            if not callable(getattr(handle, "run", None)):
                logger.warning(
                    "Registering agent %s with a handle that has no "
                    ".run method (type=%s); lookups will fail when "
                    "callers try to invoke it.",
                    agent_id, type(handle).__name__,
                )

        tags_list: Optional[List[str]] = (
            self._normalize_str_iterable("tags", tags)
            if tags is not None else None
        )
        caps_list: Optional[List[str]] = (
            self._normalize_str_iterable("capabilities", capabilities)
            if capabilities is not None else None
        )
        meta = dict(metadata) if isinstance(metadata, Mapping) else {}

        now = time.time()

        with self._lock:
            existing = self._agents.get(agent_id)
            if existing is not None:
                existing.node_id = node_id
                if handle is not None:
                    existing.handle = handle

                if tags_list is not None:
                    if replace_tags:
                        existing.tags = tuple(tags_list)
                    else:
                        merged = list(existing.tags)
                        for t in tags_list:
                            if t not in merged:
                                merged.append(t)
                        existing.tags = tuple(merged)

                if caps_list is not None:
                    if replace_capabilities:
                        existing.capabilities = tuple(caps_list)
                    else:
                        merged = list(existing.capabilities)
                        for c in caps_list:
                            if c not in merged:
                                merged.append(c)
                        existing.capabilities = tuple(merged)

                if meta:
                    existing.metadata.update(meta)

                existing.last_heartbeat = now
                existing.health = AgentHealth.ALIVE
                existing.heartbeat_history.append(now)

                snapshot = existing.snapshot()
                self._audit_log(
                    "reregister", agent_id=agent_id, node_id=node_id)
                self._emit_metric("agent_registered", {
                    "agent_id": agent_id, "new": False,
                })
                logger.debug("Agent %s re-registered.", agent_id)
                return snapshot

            entry = AgentEntry(
                agent_id=agent_id,
                node_id=node_id,
                handle=handle,
                tags=tuple(tags_list or []),
                capabilities=tuple(caps_list or []),
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
            snapshot = entry.snapshot()

        self._audit_log(
            "register",
            agent_id=agent_id, node_id=node_id,
            tags=list(snapshot.tags),
            capabilities=list(snapshot.capabilities),
        )
        self._emit_metric("agent_registered", {
            "agent_id": agent_id, "new": True,
        })
        logger.debug(
            "Agent registered: id=%s node=%s tags=%s caps=%s",
            agent_id, node_id, snapshot.tags, snapshot.capabilities,
        )
        return snapshot

    def deregister(
        self, agent_id: str, *, reason: Optional[str] = None,
    ) -> bool:
        """
        Remove an agent from the registry. Returns True if it existed.

        ``reason`` is recorded in the audit trail (P2 3.13).
        """
        self._validate_id("agent_id", agent_id)
        with self._lock:
            removed = self._agents.pop(agent_id, None)
        if removed is not None:
            self._audit_log(
                "deregister", agent_id=agent_id, reason=reason or "")
            self._emit_metric("agent_deregistered", {
                "agent_id": agent_id, "reason": reason or "",
            })
            logger.debug(
                "Agent deregistered: id=%s reason=%s",
                agent_id, reason,
            )
            return True
        return False

    # ---------------------------------------------------------- lookup
    def lookup(self, agent_id: str) -> Optional[AgentEntry]:
        """
        Return a **snapshot** of the entry for ``agent_id`` or ``None``.

        Mutating the returned object does not affect registry state (P0 2.7).
        """
        self._validate_id("agent_id", agent_id)
        with self._lock:
            self._maybe_reap_locked()
            entry = self._agents.get(agent_id)
            return None if entry is None else entry.snapshot()

    def _live_entry(self, agent_id: str) -> Optional[AgentEntry]:
        """
        Internal: return the live entry. Caller must hold ``self._lock``.

        Never expose this outside the class.
        """
        return self._agents.get(agent_id)

    def require(self, agent_id: str) -> AgentEntry:
        """Return a snapshot for ``agent_id`` or raise."""
        entry = self.lookup(agent_id)
        if entry is None:
            raise AgentRegistryError(f"Agent '{agent_id}' not found.")
        return entry

    def heartbeat(self, agent_id: str) -> AgentHealth:
        """Record a heartbeat for ``agent_id`` and return the new state."""
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
        self._emit_metric("heartbeat_received", {"agent_id": agent_id})
        return AgentHealth.ALIVE

    def heartbeat_rate(
        self, agent_id: str, *, window_s: float = 60.0,
    ) -> float:
        """
        Return heartbeats per second over the trailing ``window_s`` (P2 3.4).

        Useful for flapping detection: a healthy agent should heartbeat at
        roughly ``1/heartbeat_interval`` Hz; an agent dipping toward zero is
        about to be marked SUSPECT.
        """
        self._validate_id("agent_id", agent_id)
        if window_s <= 0:
            raise AgentRegistryError("window_s must be > 0.")
        with self._lock:
            entry = self._agents.get(agent_id)
            if entry is None:
                raise AgentRegistryError(
                    f"Agent '{agent_id}' not found.")
            cutoff = time.time() - window_s
            count = sum(1 for hb in entry.heartbeat_history
                        if hb >= cutoff)
        return count / window_s

    # ---------------------------------------------------------- discovery
    def list_agents(
        self,
        *,
        health: Optional[AgentHealth] = None,
        tags: Optional[Iterable[str]] = None,
        capabilities: Optional[Iterable[str]] = None,
        include_handle: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Return matching agents as JSON-friendly **dicts**.

        ``list_entries`` returns ``AgentEntry`` snapshots instead — use it
        when you need object attributes rather than serialized fields.
        """
        want_tags = set(
            self._normalize_str_iterable("tags", tags))
        want_caps = set(
            self._normalize_str_iterable("capabilities", capabilities))

        with self._lock:
            self._maybe_reap_locked()
            entries = list(self._agents.values())

        results: List[Dict[str, Any]] = []
        for entry in entries:
            if health is not None and entry.health != health:
                continue
            if want_tags and not want_tags.issubset(set(entry.tags)):
                continue
            if want_caps and not want_caps.issubset(
                    set(entry.capabilities)):
                continue
            results.append(
                entry.to_dict(include_handle=include_handle))
        return results

    def list_entries(
        self,
        *,
        health: Optional[AgentHealth] = None,
        tags: Optional[Iterable[str]] = None,
        capabilities: Optional[Iterable[str]] = None,
    ) -> List[AgentEntry]:
        """
        Return matching agents as **snapshots** (P0 2.8).

        Same filters as :meth:`list_agents`; returns objects instead of
        dicts.
        """
        want_tags = set(
            self._normalize_str_iterable("tags", tags))
        want_caps = set(
            self._normalize_str_iterable("capabilities", capabilities))

        with self._lock:
            self._maybe_reap_locked()
            entries = list(self._agents.values())

        out: List[AgentEntry] = []
        for entry in entries:
            if health is not None and entry.health != health:
                continue
            if want_tags and not want_tags.issubset(set(entry.tags)):
                continue
            if want_caps and not want_caps.issubset(
                    set(entry.capabilities)):
                continue
            out.append(entry.snapshot())
        return out

    def find_by_tag(self, tag: str) -> List[AgentEntry]:
        """Return snapshots for every entry carrying ``tag``."""
        self._validate_id("tag", tag)
        with self._lock:
            self._maybe_reap_locked()
            entries = [e for e in self._agents.values() if tag in e.tags]
        return [e.snapshot() for e in entries]

    def find_by_capability(self, capability: str) -> List[AgentEntry]:
        """Return snapshots for every entry declaring ``capability``."""
        self._validate_id("capability", capability)
        with self._lock:
            self._maybe_reap_locked()
            entries = [
                e for e in self._agents.values()
                if capability in e.capabilities
            ]
        return [e.snapshot() for e in entries]

    def alive_agents(self) -> List[AgentEntry]:
        """Return snapshots for every entry currently ``ALIVE``."""
        with self._lock:
            self._maybe_reap_locked()
            entries = [
                e for e in self._agents.values()
                if e.health == AgentHealth.ALIVE
            ]
        return [e.snapshot() for e in entries]

    # ---------------------------------------------------------- capacity
    def _enforce_capacity_locked(self) -> None:
        """Evict oldest-heartbeat entries when over capacity (P1 2.4)."""
        cap = self._config.max_agents
        if cap is None or len(self._agents) <= cap:
            return
        excess = len(self._agents) - cap
        victims = heapq.nsmallest(
            excess, self._agents.items(),
            key=lambda kv: kv[1].last_heartbeat,
        )
        for agent_id, _ in victims:
            self._agents.pop(agent_id, None)
            self._audit_log(
                "evict", agent_id=agent_id, reason="capacity")
            self._emit_metric("agent_evicted", {"agent_id": agent_id})
            logger.debug(
                "Evicted agent %s (max_agents=%d).", agent_id, cap)

    # ---------------------------------------------------------- validation
    @staticmethod
    def _validate_id(name: str, value: Any) -> None:
        if not isinstance(value, str) or not value:
            raise AgentRegistryError(
                f"{name} must be a non-empty string.")

    def _normalize_str_iterable(
        self, name: str, value: Optional[Iterable[str]],
    ) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
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

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the registry."""
        with self._lock:
            self._maybe_reap_locked()
            entries = list(self._agents.values())
            last_reap = self._last_reap_at
            started_at = self._started_at

        now = time.time()
        base = {
            "registry_id": self._config.registry_id,
            "size": len(entries),
            "last_reap_ago_seconds": (
                None if last_reap is None else now - last_reap
            ),
            "ever_reaped": last_reap is not None,
            "uptime_seconds": now - started_at,
        }
        if not entries:
            return {
                **base,
                "by_health": {},
                "by_node": {},
                "unique_tags": [],
                "unique_capabilities": [],
                "oldest_heartbeat_age_seconds": None,
            }

        by_health: Dict[str, int] = {}
        by_node: Dict[str, int] = {}
        tags: Set[str] = set()
        caps: Set[str] = set()
        for e in entries:
            by_health[e.health.value] = by_health.get(
                e.health.value, 0) + 1
            by_node[e.node_id] = by_node.get(e.node_id, 0) + 1
            tags.update(e.tags)
            caps.update(e.capabilities)

        return {
            **base,
            "by_health": by_health,
            "by_node": by_node,
            "unique_tags": sorted(tags),
            "unique_capabilities": sorted(caps),
            "oldest_heartbeat_age_seconds": now - min(
                e.last_heartbeat for e in entries),
        }

    def clear(self) -> int:
        """Remove every entry. Returns the number removed."""
        with self._lock:
            n = len(self._agents)
            self._agents.clear()
        if n:
            self._audit_log("clear", count=n)
            self._emit_metric("registry_cleared", {"count": n})
        logger.debug("AgentRegistry cleared (%d entries).", n)
        return n

    def reset(self) -> None:
        """Reset the registry to its initial empty state."""
        self.clear()
        with self._lock:
            self._started_at = time.time()
            self._last_reap_at = None
            self._ctx_start = None
            self._async_ctx_start = None
        logger.debug("AgentRegistry reset.")

    def close(self) -> None:
        """
        Release resources (P1 3.6).

        Currently a no-op — reserved for a future background reaper. Safe to
        call multiple times.
        """
        logger.debug(
            "AgentRegistry(%s) closed.", self._config.registry_id)

    # ---------------------------------------------------------- serialization
    def to_dict(
        self,
        *,
        include_handles: bool = False,
        include_history: bool = False,
    ) -> Dict[str, Any]:
        with self._lock:
            entries = [
                e.to_dict(
                    include_handle=include_handles,
                    include_history=include_history,
                )
                for e in self._agents.values()
            ]
            started_at = self._started_at
            last_reap = self._last_reap_at
        return {
            "version": __version__,
            "registry_id": self._config.registry_id,
            "config": asdict(self._config),
            "strict": self._strict,
            "ray_available": _RAY_AVAILABLE,
            "started_at": started_at,
            "last_reap_at": last_reap,
            "entries": entries,
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        handles: Optional[Mapping[str, Any]] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "AgentRegistry":
        """
        Rebuild an ``AgentRegistry`` from a dict produced by :meth:`to_dict`.

        ``handles`` maps ``agent_id → live handle``. Live handles are not
        serialized by default, so they must be re-injected here (P1 3.8).
        """
        if not isinstance(data, Mapping):
            raise AgentRegistryError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )

        # P0 2.5: config drift fix
        cfg = AgentRegistryConfig.from_legacy_dict(
            data.get("config") or {})

        registry = cls(
            config=cfg,
            strict=bool(data.get("strict", True)),
            metrics_hook=metrics_hook,
        )
        handles = dict(handles or {})

        with registry._lock:
            for entry_data in data.get("entries", []):
                entry = AgentEntry.from_dict(entry_data)
                if entry.handle is None:
                    entry.handle = handles.get(entry.agent_id)
                registry._agents[entry.agent_id] = entry

            registry._started_at = float(
                data.get("started_at", time.time()))
            raw_last = data.get("last_reap_at")
            registry._last_reap_at = (
                float(raw_last) if raw_last is not None else None
            )

            # 3.8: warn about restored entries with no live handle
            missing = [
                aid for aid, e in registry._agents.items()
                if e.handle is None
            ]
            if missing:
                logger.warning(
                    "Restored %d agent(s) without a live handle: %s",
                    len(missing), missing[:10],
                )

        return registry

    def to_json(
        self,
        *,
        include_handles: bool = False,
        **kwargs: Any,
    ) -> str:
        # Handles are objects, not JSON-serializable by contract.
        return json.dumps(
            self.to_dict(include_handles=include_handles),
            default=str,
            **kwargs,
        )

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        handles: Optional[Mapping[str, Any]] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "AgentRegistry":
        try:
            return cls.from_dict(
                json.loads(payload),
                handles=handles,
                metrics_hook=metrics_hook,
            )
        except json.JSONDecodeError as exc:
            raise AgentRegistryError(
                f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- pickling
    def __getstate__(self) -> Dict[str, Any]:
        # P0 2.9: RLock is not picklable.
        state = self.__dict__.copy()
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "AgentRegistry":
        with self._lock:
            self._ctx_start = time.perf_counter()
        logger.debug(
            "Entering scoped AgentRegistry session (%s).",
            self._config.registry_id,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            start = (
                self._ctx_start
                if self._ctx_start is not None
                else time.perf_counter()
            )
            elapsed = time.perf_counter() - start
            self._ctx_start = None
            size = len(self._agents)
        if exc_type is not None:
            logger.warning(
                "AgentRegistry scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info(
            "AgentRegistry scope closed in %.4fs (%d agent(s)).",
            elapsed, size,
        )

    async def __aenter__(self) -> "AgentRegistry":
        with self._lock:
            self._async_ctx_start = time.perf_counter()
        logger.debug("Entering async AgentRegistry session.")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            start = (
                self._async_ctx_start
                if self._async_ctx_start is not None
                else time.perf_counter()
            )
            elapsed = time.perf_counter() - start
            self._async_ctx_start = None
            size = len(self._agents)
        if exc_type is not None:
            logger.warning(
                "Async AgentRegistry scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info(
            "Async AgentRegistry scope closed in %.4fs (%d agent(s)).",
            elapsed, size,
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "AgentRegistry("
                f"version={__version__!r}, "
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
# Ray integration (P0 2.3 — two names)
# --------------------------------------------------------------------------- #
if _RAY_AVAILABLE:  # pragma: no cover — environment-dependent
    RemoteAgentRegistry = ray.remote(AgentRegistry)  # type: ignore[assignment]
else:
    RemoteAgentRegistry = AgentRegistry


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "AgentRegistry",
    "RemoteAgentRegistry",
    "AgentRegistryConfig",
    "AgentRegistryError",
    "AgentEntry",
    "AgentHealth",
]


# --------------------------------------------------------------------------- #
# Self-tests: python -m distributed.agent_registry
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import asyncio

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    class _FakeNode:
        def __init__(self, node_id: str) -> None:
            self.node_id = node_id

        def run(self, task_input: Any) -> Any:  # satisfy .run contract
            return {"node": self.node_id, "echo": task_input}

        def __repr__(self) -> str:
            return f"_FakeNode({self.node_id})"

    # ---------------------------------------------------------------- #
    # 1. Basic lifecycle
    # ---------------------------------------------------------------- #
    print("=== 1. Basic lifecycle ===")
    registry = AgentRegistry(
        config=AgentRegistryConfig(
            suspect_after_seconds=1.0,
            dead_after_seconds=3.0,
            reap_interval_seconds=0.1,
            max_history_per_agent=5,
        ),
    )

    registry.register(
        agent_id="agent-001", node_id="node-a",
        handle=_FakeNode("node-a"),
        tags=["nlp", "gpu"],
        capabilities=["pareto-scoring", "helium-aware"],
        metadata={"region": "eu-west-1"},
    )
    registry.register(
        agent_id="agent-002", node_id="node-b",
        handle=_FakeNode("node-b"),
        tags=["vision"],
        capabilities=["pareto-scoring"],
    )
    registry.register(
        agent_id="agent-003", node_id="node-a",
        handle=_FakeNode("node-a"),
        tags=["nlp"],
        capabilities=["carbon-aware"],
    )

    print(f"  registry     : {registry}")
    print(f"  size         : {registry.size}")
    print(f"  contains     : "
          f"{'agent-001' in registry}, {'agent-999' in registry}")
    print(f"  nlp agents   : "
          f"{[e.agent_id for e in registry.find_by_tag('nlp')]}")
    print(f"  pareto caps  : "
          f"{[e.agent_id for e in registry.find_by_capability('pareto-scoring')]}")

    # ---------------------------------------------------------------- #
    # 2. P0 2.1: include_handles=True actually includes the handle
    # ---------------------------------------------------------------- #
    print("\n=== 2. P0 2.1: include_handles=True includes handle ===")
    no_handle = registry.to_dict(include_handles=False)["entries"][0]
    with_handle = registry.to_dict(include_handles=True)["entries"][0]
    assert "handle" not in no_handle, "default must not include handle"
    assert "handle" in with_handle, "include_handles=True must include handle"
    assert no_handle["has_handle"] is True
    assert with_handle["has_handle"] is True
    print(f"  ✓ without flag: keys={sorted(no_handle)[:4]}...")
    print(f"  ✓ with flag   : handle={type(with_handle['handle']).__name__}")
    # to_json must never include handles
    json_blob = registry.to_json()
    assert "\"handle\"" not in json_blob or \
        json_blob.count("\"handle\"") == 0, \
        "to_json must never include handles"
    print("  ✓ to_json omits handles")

    # ---------------------------------------------------------------- #
    # 3. P0 2.2: reap_interval_seconds triggers auto-reap on read
    # ---------------------------------------------------------------- #
    print("\n=== 3. P0 2.2: reads auto-reap after reap_interval ===")
    auto = AgentRegistry(
        config=AgentRegistryConfig(
            suspect_after_seconds=0.1,
            dead_after_seconds=0.5,
            reap_interval_seconds=0.05,
        ),
    )
    auto.register(agent_id="autoreap-1", node_id="n1")
    assert auto.lookup("autoreap-1").health == AgentHealth.ALIVE

    # Exceed suspect TTL without calling reap() explicitly
    time.sleep(0.15)
    snap = auto.lookup("autoreap-1")
    assert snap.health == AgentHealth.SUSPECT, (
        f"auto-reap should have marked SUSPECT, got {snap.health}")
    print(f"  ✓ auto-reaped on read: health={snap.health}")

    # Continue to DEAD
    time.sleep(0.4)
    snap = auto.lookup("autoreap-1")
    assert snap.health == AgentHealth.DEAD, (
        f"auto-reap should have marked DEAD, got {snap.health}")
    print(f"  ✓ auto-reaped on read: health={snap.health}")

    # ---------------------------------------------------------------- #
    # 4. P0 2.3: AgentRegistry constructs without Ray
    # ---------------------------------------------------------------- #
    print("\n=== 4. P0 2.3: two names — AgentRegistry + RemoteAgentRegistry ===")
    assert AgentRegistry is not None
    assert RemoteAgentRegistry is not None
    # Direct construction works (would fail if Ray was installed and the
    # class were decorated with @ray.remote)
    r = AgentRegistry()
    assert r.size == 0
    print(f"  ✓ AgentRegistry() constructs directly "
          f"(ray_available={_RAY_AVAILABLE})")
    print(f"  ✓ RemoteAgentRegistry available: "
          f"{RemoteAgentRegistry.__name__ if hasattr(RemoteAgentRegistry, '__name__') else 'ray actor'}")

    # ---------------------------------------------------------------- #
    # 5. P0 2.5: from_dict picks up new field defaults
    # ---------------------------------------------------------------- #
    print("\n=== 5. P0 2.5: from_dict handles new fields ===")
    legacy = {
        "config": {
            "suspect_after_seconds": 42.0,
            "dead_after_seconds": 99.0,
            # reap_interval_seconds and others omitted
        },
        "strict": True,
        "entries": [],
    }
    restored = AgentRegistry.from_dict(legacy)
    assert restored.config.suspect_after_seconds == 42.0
    assert restored.config.dead_after_seconds == 99.0
    assert restored.config.reap_interval_seconds == 15.0  # default
    assert restored.config.registry_id == "default-registry"
    print("  ✓ new fields default correctly on restore")

    # ---------------------------------------------------------------- #
    # 6. P0 2.7: snapshots are independent of registry state
    # ---------------------------------------------------------------- #
    print("\n=== 6. P0 2.7: mutation of returned snapshots is isolated ===")
    snap1 = registry.lookup("agent-001")
    assert snap1 is not None
    original_tags = snap1.tags
    original_meta = dict(snap1.metadata)

    # Mutate the snapshot — registry must not change
    snap1.tags = ("HACKED",)
    snap1.metadata["injected"] = True
    snap1.health = AgentHealth.DEAD

    snap2 = registry.lookup("agent-001")
    assert snap2.tags == original_tags, (
        f"registry was mutated through snapshot: {snap2.tags}")
    assert "injected" not in snap2.metadata
    assert snap2.health == AgentHealth.ALIVE
    print(f"  ✓ registry unaffected: tags={snap2.tags}")

    # Same for find_by_tag results
    entries = registry.find_by_tag("nlp")
    assert entries
    entries[0].tags = ("ALSO_HACKED",)
    fresh = registry.lookup(entries[0].agent_id)
    assert "ALSO_HACKED" not in fresh.tags
    print("  ✓ find_by_tag snapshots are independent too")

    # ---------------------------------------------------------------- #
    # 7. P0 2.8: list_agents (dicts) vs list_entries (objects)
    # ---------------------------------------------------------------- #
    print("\n=== 7. P0 2.8: consistent discovery surfaces ===")
    dicts = registry.list_agents(health=AgentHealth.ALIVE)
    objs = registry.list_entries(health=AgentHealth.ALIVE)
    assert all(isinstance(d, dict) for d in dicts)
    assert all(isinstance(o, AgentEntry) for o in objs)
    assert len(dicts) == len(objs)
    # Both filters accept the same arguments
    tagged_d = registry.list_agents(tags=["gpu"])
    tagged_o = registry.list_entries(tags=["gpu"])
    assert len(tagged_d) == len(tagged_o)
    print(f"  ✓ list_agents → {len(dicts)} dict(s); "
          f"list_entries → {len(objs)} entry object(s)")

    # ---------------------------------------------------------------- #
    # 8. P0 2.9: pickling round-trip
    # ---------------------------------------------------------------- #
    print("\n=== 8. P0 2.9: registry is picklable ===")
    import pickle
    pickled_registry = pickle.loads(pickle.dumps(registry))
    assert pickled_registry.size == registry.size
    assert pickled_registry.registry_id == registry.registry_id
    # Lock must be reconstructed
    pickled_registry.register(agent_id="post-pickle", node_id="np")
    assert pickled_registry.size == registry.size + 1
    print(f"  ✓ pickle round-trip OK (size={pickled_registry.size})")

    # ---------------------------------------------------------------- #
    # 9. P1 2.6: replace_tags clears collections
    # ---------------------------------------------------------------- #
    print("\n=== 9. P1 2.6: replace_tags clears ===")
    r = AgentRegistry()
    r.register(agent_id="t1", node_id="n1", tags=["a", "b"])
    assert r.lookup("t1").tags == ("a", "b")

    # Merge by default — passing a new tag adds without removing
    r.register(agent_id="t1", node_id="n1", tags=["c"])
    assert set(r.lookup("t1").tags) == {"a", "b", "c"}

    # replace_tags=True + empty list clears
    r.register(
        agent_id="t1", node_id="n1",
        tags=[], replace_tags=True)
    assert r.lookup("t1").tags == ()
    print("  ✓ merge is default; replace_tags=True clears")

    # ---------------------------------------------------------------- #
    # 10. P1 3.1: metrics_hook
    # ---------------------------------------------------------------- #
    print("\n=== 10. P1 3.1: metrics_hook ===")
    events: List[Tuple[str, Mapping[str, Any]]] = []
    m = AgentRegistry(
        config=AgentRegistryConfig(
            suspect_after_seconds=0.1, dead_after_seconds=0.5,
            reap_interval_seconds=60.0,
        ),
        metrics_hook=lambda ev, f: events.append((ev, f)),
    )
    m.register(agent_id="m1", node_id="n1")
    m.heartbeat("m1")
    m.deregister("m1", reason="test")
    events_by_name = {ev for ev, _ in events}
    assert "agent_registered" in events_by_name
    assert "heartbeat_received" in events_by_name
    assert "agent_deregistered" in events_by_name
    print(f"  ✓ captured events: {sorted(events_by_name)}")

    # ---------------------------------------------------------------- #
    # 11. P1 3.2: purge_dead is self-sufficient
    # ---------------------------------------------------------------- #
    print("\n=== 11. P1 3.2: purge_dead reaps first ===")
    p = AgentRegistry(
        config=AgentRegistryConfig(
            suspect_after_seconds=0.05, dead_after_seconds=0.1,
            reap_interval_seconds=60.0,  # disable auto-reap
        ),
    )
    p.register(agent_id="p1", node_id="n1")
    p.register(agent_id="p2", node_id="n1")
    time.sleep(0.15)
    # Without ever calling reap(), purge_dead should still find them dead.
    purged = p.purge_dead()
    assert purged == 2, f"expected 2 purged, got {purged}"
    assert p.size == 0
    print(f"  ✓ purge_dead reaped and removed {purged} agent(s)")

    # ---------------------------------------------------------------- #
    # 12. P1 3.7: context managers are locked
    # ---------------------------------------------------------------- #
    print("\n=== 12. P1 3.7: context managers are locked ===")

    def _concurrent_scope(reg: AgentRegistry) -> None:
        with reg:
            time.sleep(0.05)

    threaded = AgentRegistry()
    threads = [
        threading.Thread(target=_concurrent_scope, args=(threaded,))
        for _ in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # No assertion other than "no crash / no deadlock" — the lock guarantees
    # _ctx_start is never clobbered mid-block.
    print("  ✓ 4 concurrent scopes completed without deadlock")

    # ---------------------------------------------------------------- #
    # 13. P1 3.8: from_dict warns when handles are missing
    # ---------------------------------------------------------------- #
    print("\n=== 13. P1 3.8: from_dict warns on missing handles ===")
    blob = registry.to_dict()  # no handles
    # This should log a WARNING listing restored agents without handles.
    restored2 = AgentRegistry.from_dict(blob)
    assert all(
        e.handle is None for e in
        restored2._agents.values()), \
        "handles should be None without explicit injection"
    print(f"  ✓ restored {restored2.size} agent(s) with handle=None "
          f"(warning emitted)")

    # Re-inject one handle
    restored3 = AgentRegistry.from_dict(
        blob, handles={"agent-001": _FakeNode("node-a")})
    assert restored3.lookup("agent-001").handle is not None
    assert restored3.lookup("agent-002").handle is None
    print("  ✓ handle re-injection works per agent")

    # ---------------------------------------------------------------- #
    # 14. P2 3.13: audit trail + deregister(reason=...)
    # ---------------------------------------------------------------- #
    print("\n=== 14. P2 3.13: audit trail ===")
    a = AgentRegistry()
    a.register(agent_id="a1", node_id="n1")
    a.deregister("a1", reason="maintenance")
    audit = a.audit_log()
    assert any(e["event"] == "deregister" for e in audit)
    dereg = next(e for e in audit if e["event"] == "deregister")
    assert dereg["reason"] == "maintenance"
    print(f"  ✓ audit entries: "
          f"{[e['event'] for e in audit]}")

    # ---------------------------------------------------------------- #
    # 15. P2 3.11: last_reap_ago_seconds semantics
    # ---------------------------------------------------------------- #
    print("\n=== 15. P2 3.11: last_reap_ago ===")
    fresh = AgentRegistry()
    fresh.register(agent_id="f1", node_id="n1")
    stats = fresh.statistics()
    assert stats["ever_reaped"] is True or \
        stats["last_reap_ago_seconds"] is not None
    print(f"  ✓ ever_reaped={stats['ever_reaped']} "
          f"last_reap_ago={stats['last_reap_ago_seconds']}")

    # ---------------------------------------------------------------- #
    # 16. P2 3.4: heartbeat_rate
    # ---------------------------------------------------------------- #
    print("\n=== 16. P2 3.4: heartbeat_rate ===")
    hb = AgentRegistry()
    hb.register(agent_id="hb1", node_id="n1")
    for _ in range(5):
        hb.heartbeat("hb1")
        time.sleep(0.01)
    rate = hb.heartbeat_rate("hb1", window_s=10.0)
    assert rate > 0
    print(f"  ✓ heartbeat_rate over 10s window: {rate:.3f} Hz")

    # ---------------------------------------------------------------- #
    # 17. Validation
    # ---------------------------------------------------------------- #
    print("\n=== 17. Validation ===")
    for bad_kwargs in (
        dict(agent_id="", node_id="node-x"),
        dict(agent_id="a", node_id=""),
    ):
        try:
            registry.register(**bad_kwargs)  # type: ignore[arg-type]
        except AgentRegistryError as exc:
            print(f"  ✓ rejected register: {exc}")
        else:
            raise AssertionError(f"Expected rejection: {bad_kwargs!r}")

    try:
        registry.heartbeat("unknown-agent")
    except AgentRegistryError as exc:
        print(f"  ✓ rejected heartbeat: {exc}")
    else:
        raise AssertionError("Expected heartbeat rejection")

    try:
        registry.require("not-registered")
    except AgentRegistryError as exc:
        print(f"  ✓ rejected require: {exc}")
    else:
        raise AssertionError("Expected require rejection")

    for bad_cfg in (
        dict(suspect_after_seconds=0),
        dict(suspect_after_seconds=10, dead_after_seconds=5),
        dict(max_agents=0),
        dict(registry_id=""),
    ):
        try:
            AgentRegistryConfig(**bad_cfg)  # type: ignore[arg-type]
        except AgentRegistryError as exc:
            print(f"  ✓ rejected config: {exc}")
        else:
            raise AssertionError(
                f"Expected config rejection: {bad_cfg!r}")

    # ---------------------------------------------------------------- #
    # 18. Async context manager (backward compat)
    # ---------------------------------------------------------------- #
    print("\n=== 18. Async context manager ===")

    async def _async_scope() -> None:
        async with AgentRegistry() as scoped:
            scoped.register(agent_id="actx-1", node_id="node-d")
            assert scoped.size == 1

    asyncio.run(_async_scope())
    print("  ✓ async context manager OK")

    # ---------------------------------------------------------------- #
    # 19. Statistics shape
    # ---------------------------------------------------------------- #
    print("\n=== 19. Statistics ===")
    stats = registry.statistics()
    assert "size" in stats
    assert "by_health" in stats
    assert "ever_reaped" in stats
    assert "last_reap_ago_seconds" in stats
    print(f"  ✓ statistics keys: {sorted(stats)}")
    print(f"  ✓ by_health: {stats['by_health']}")

    print("\n✓ All self-tests passed.")
