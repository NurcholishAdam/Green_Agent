# src/memory/memory_tier.py

"""
Memory Tier
===========

Implements "Tier memory by value" — hot / warm / cold storage with explicit
promotion and demotion policies.

Enhancements
------------
- ``MemoryTier`` StrEnum — HOT / WARM / COLD.
- ``MemoryTierConfig`` — frozen, validated: thresholds, TTLs, capacity.
- ``TieredRecord`` — frozen record with tier metadata.
- ``MemoryTierPolicy`` — deterministic tier selection based on record
  importance, truth level, and age.
- ``MemoryTierManager`` — thread-safe hot/warm/cold stores with LRU
  promotion and TTL-driven demotion.
- ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Deque, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

__version__ = "5.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MemoryTierError(ValueError):
    """Raised for invalid tier inputs or configuration."""


# --------------------------------------------------------------------------- #
# Tier enum
# --------------------------------------------------------------------------- #
class MemoryTier(str, Enum):
    """Storage tiers, ordered by access latency."""

    HOT = "hot"      # in-process LRU, TTL ~minutes
    WARM = "warm"    # Supermemory container, TTL ~days
    COLD = "cold"    # archive, TTL ~forever

    @property
    def description(self) -> str:
        return {
            MemoryTier.HOT: "In-process LRU, sub-millisecond access",
            MemoryTier.WARM: "Supermemory container, millisecond access",
            MemoryTier.COLD: "Archive, best-effort access",
        }[self]


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MemoryTierConfig:
    """Tunable parameters for tiering."""

    hot_capacity: int = 256
    warm_capacity: int = 4_096
    cold_capacity: int = 100_000

    hot_ttl_seconds: int = 300           # 5 minutes
    warm_ttl_seconds: int = 7 * 24 * 3600  # 7 days
    cold_ttl_seconds: int = 365 * 24 * 3600  # 1 year

    # Records with importance >= this threshold start HOT.
    hot_importance_threshold: float = 0.8
    # Trusted truth levels start WARM at minimum.
    trusted_truth_levels: Tuple[str, ...] = ("measured",)
    # Untrusted truth levels start COLD.
    untrusted_truth_levels: Tuple[str, ...] = ("simulated", "user-reported")

    def __post_init__(self) -> None:
        for name in ("hot_capacity", "warm_capacity", "cold_capacity"):
            v = getattr(self, name)
            if not isinstance(v, int) or v <= 0:
                raise MemoryTierError(f"{name} must be a positive int.")
        for name in ("hot_ttl_seconds", "warm_ttl_seconds", "cold_ttl_seconds"):
            v = getattr(self, name)
            if not isinstance(v, int) or v <= 0:
                raise MemoryTierError(f"{name} must be a positive int.")
        if not 0.0 <= self.hot_importance_threshold <= 1.0:
            raise MemoryTierError(
                "hot_importance_threshold must be in [0, 1]."
            )
        for name in ("trusted_truth_levels", "untrusted_truth_levels"):
            v = getattr(self, name)
            if not isinstance(v, tuple) or not all(isinstance(x, str) for x in v):
                raise MemoryTierError(f"{name} must be a tuple of strings.")

    def ttl_for(self, tier: MemoryTier) -> int:
        return {
            MemoryTier.HOT: self.hot_ttl_seconds,
            MemoryTier.WARM: self.warm_ttl_seconds,
            MemoryTier.COLD: self.cold_ttl_seconds,
        }[tier]

    def capacity_for(self, tier: MemoryTier) -> int:
        return {
            MemoryTier.HOT: self.hot_capacity,
            MemoryTier.WARM: self.warm_capacity,
            MemoryTier.COLD: self.cold_capacity,
        }[tier]

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "hot_capacity": self.hot_capacity,
            "warm_capacity": self.warm_capacity,
            "cold_capacity": self.cold_capacity,
            "hot_ttl_seconds": self.hot_ttl_seconds,
            "warm_ttl_seconds": self.warm_ttl_seconds,
            "cold_ttl_seconds": self.cold_ttl_seconds,
            "hot_importance_threshold": self.hot_importance_threshold,
            "trusted_truth_levels": list(self.trusted_truth_levels),
            "untrusted_truth_levels": list(self.untrusted_truth_levels),
        }
        return d


# --------------------------------------------------------------------------- #
# Record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TieredRecord:
    """Frozen record stored in a tier."""

    record_id: str
    kind: str
    payload: Mapping[str, Any]
    tier: MemoryTier
    importance: float
    truth_level: str
    stored_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    expires_at: float = 0.0
    access_count: int = 0

    def __post_init__(self) -> None:
        for name in ("record_id", "kind", "truth_level"):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise MemoryTierError(f"{name} must be a non-empty string.")
        if not isinstance(self.payload, Mapping):
            raise MemoryTierError("payload must be a Mapping.")
        if not isinstance(self.tier, MemoryTier):
            raise MemoryTierError("tier must be a MemoryTier.")
        if not 0.0 <= self.importance <= 1.0:
            raise MemoryTierError("importance must be in [0, 1].")

    def is_expired(self, *, now: Optional[float] = None) -> bool:
        if self.expires_at <= 0:
            return False
        return (now if now is not None else time.time()) >= self.expires_at

    def to_dict(self) -> Dict[str, Any]:
        return {
            "record_id": self.record_id,
            "kind": self.kind,
            "payload": dict(self.payload),
            "tier": self.tier.value,
            "importance": self.importance,
            "truth_level": self.truth_level,
            "stored_at": self.stored_at,
            "last_accessed": self.last_accessed,
            "expires_at": self.expires_at,
            "access_count": self.access_count,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TieredRecord":
        return cls(
            record_id=str(data["record_id"]),
            kind=str(data["kind"]),
            payload=dict(data.get("payload", {})),
            tier=MemoryTier(data["tier"]),
            importance=float(data.get("importance", 0.0)),
            truth_level=str(data.get("truth_level", "estimated")),
            stored_at=float(data.get("stored_at", time.time())),
            last_accessed=float(data.get("last_accessed", time.time())),
            expires_at=float(data.get("expires_at", 0.0)),
            access_count=int(data.get("access_count", 0)),
        )


# --------------------------------------------------------------------------- #
# Policy
# --------------------------------------------------------------------------- #
class MemoryTierPolicy:
    """Deterministic tier selection based on record metadata."""

    def __init__(self, *, config: Optional[MemoryTierConfig] = None) -> None:
        self._config = config or MemoryTierConfig()

    @property
    def config(self) -> MemoryTierConfig:
        return self._config

    def select_tier(
        self,
        *,
        importance: float,
        truth_level: str,
    ) -> MemoryTier:
        """Return the initial tier for a new record."""
        if not 0.0 <= importance <= 1.0:
            raise MemoryTierError("importance must be in [0, 1].")
        if not isinstance(truth_level, str) or not truth_level:
            raise MemoryTierError("truth_level must be a non-empty string.")

        # Untrusted truth levels start COLD.
        if truth_level in self._config.untrusted_truth_levels:
            return MemoryTier.COLD
        # High-importance trusted records start HOT.
        if (
            importance >= self._config.hot_importance_threshold
            and truth_level in self._config.trusted_truth_levels
        ):
            return MemoryTier.HOT
        # Everything else starts WARM.
        return MemoryTier.WARM


# --------------------------------------------------------------------------- #
# Manager
# --------------------------------------------------------------------------- #
class MemoryTierManager:
    """Thread-safe manager for hot / warm / cold tiers."""

    def __init__(
        self,
        *,
        config: Optional[MemoryTierConfig] = None,
        policy: Optional[MemoryTierPolicy] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or MemoryTierConfig()
        self._policy = policy or MemoryTierPolicy(config=self._config)
        self._strict = bool(strict)

        self._lock = threading.RLock()
        # Use OrderedDict for LRU semantics per tier.
        self._tiers: Dict[MemoryTier, "OrderedDict[str, TieredRecord]"] = {
            MemoryTier.HOT: OrderedDict(),
            MemoryTier.WARM: OrderedDict(),
            MemoryTier.COLD: OrderedDict(),
        }
        self._promotions = 0
        self._demotions = 0
        self._evictions = 0
        self._expirations = 0
        self._started_at = time.time()

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> MemoryTierConfig:
        return self._config

    @property
    def policy(self) -> MemoryTierPolicy:
        return self._policy

    def size(self, tier: Optional[MemoryTier] = None) -> int:
        with self._lock:
            if tier is None:
                return sum(len(d) for d in self._tiers.values())
            return len(self._tiers[tier])

    # ---------------------------------------------------------- public API
    def store(
        self,
        *,
        record_id: str,
        kind: str,
        payload: Mapping[str, Any],
        importance: float = 0.5,
        truth_level: str = "estimated",
        tier: Optional[MemoryTier] = None,
    ) -> TieredRecord:
        """Store a record, choosing its tier automatically if not given."""
        if not isinstance(record_id, str) or not record_id:
            raise MemoryTierError("record_id must be a non-empty string.")
        if not isinstance(kind, str) or not kind:
            raise MemoryTierError("kind must be a non-empty string.")
        if not isinstance(payload, Mapping):
            raise MemoryTierError("payload must be a Mapping.")

        chosen = tier or self._policy.select_tier(
            importance=importance, truth_level=truth_level,
        )
        now = time.time()
        record = TieredRecord(
            record_id=record_id,
            kind=kind,
            payload=dict(payload),
            tier=chosen,
            importance=float(importance),
            truth_level=truth_level,
            stored_at=now,
            last_accessed=now,
            expires_at=now + self._config.ttl_for(chosen),
            access_count=0,
        )
        with self._lock:
            self._tiers[chosen][record_id] = record
            self._enforce_capacity_locked(chosen)
        logger.debug("Stored %s in %s (kind=%s).", record_id, chosen.value, kind)
        return record

    def get(self, record_id: str) -> Optional[TieredRecord]:
        """Return a record, promoting it to HOT on access."""
        if not isinstance(record_id, str) or not record_id:
            raise MemoryTierError("record_id must be a non-empty string.")
        with self._lock:
            for tier in (MemoryTier.HOT, MemoryTier.WARM, MemoryTier.COLD):
                bucket = self._tiers[tier]
                record = bucket.get(record_id)
                if record is None:
                    continue
                if record.is_expired():
                    bucket.pop(record_id, None)
                    self._expirations += 1
                    return None
                # Touch + promote.
                bucket.move_to_end(record_id)
                touched = TieredRecord(
                    record_id=record.record_id,
                    kind=record.kind,
                    payload=dict(record.payload),
                    tier=record.tier,
                    importance=record.importance,
                    truth_level=record.truth_level,
                    stored_at=record.stored_at,
                    last_accessed=time.time(),
                    expires_at=record.expires_at,
                    access_count=record.access_count + 1,
                )
                if tier != MemoryTier.HOT:
                    bucket.pop(record_id, None)
                    promoted = TieredRecord(
                        record_id=touched.record_id,
                        kind=touched.kind,
                        payload=dict(touched.payload),
                        tier=MemoryTier.HOT,
                        importance=touched.importance,
                        truth_level=touched.truth_level,
                        stored_at=touched.stored_at,
                        last_accessed=touched.last_accessed,
                        expires_at=time.time() + self._config.hot_ttl_seconds,
                        access_count=touched.access_count,
                    )
                    self._tiers[MemoryTier.HOT][record_id] = promoted
                    self._promotions += 1
                    self._enforce_capacity_locked(MemoryTier.HOT)
                    return promoted
                bucket[record_id] = touched
                return touched
        return None

    def demote(self, record_id: str) -> Optional[TieredRecord]:
        """Force demotion of ``record_id`` to the next-lower tier."""
        if not isinstance(record_id, str) or not record_id:
            raise MemoryTierError("record_id must be a non-empty string.")
        with self._lock:
            for tier in (MemoryTier.HOT, MemoryTier.WARM):
                bucket = self._tiers[tier]
                record = bucket.pop(record_id, None)
                if record is None:
                    continue
                lower = (
                    MemoryTier.WARM if tier == MemoryTier.HOT
                    else MemoryTier.COLD
                )
                demoted = TieredRecord(
                    record_id=record.record_id,
                    kind=record.kind,
                    payload=dict(record.payload),
                    tier=lower,
                    importance=record.importance,
                    truth_level=record.truth_level,
                    stored_at=record.stored_at,
                    last_accessed=record.last_accessed,
                    expires_at=time.time() + self._config.ttl_for(lower),
                    access_count=record.access_count,
                )
                self._tiers[lower][record_id] = demoted
                self._demotions += 1
                self._enforce_capacity_locked(lower)
                return demoted
        return None

    def expire(self) -> int:
        """Sweep all tiers, dropping expired records. Returns count removed."""
        now = time.time()
        removed = 0
        with self._lock:
            for bucket in self._tiers.values():
                expired = [k for k, v in bucket.items() if v.is_expired(now=now)]
                for k in expired:
                    bucket.pop(k, None)
                    removed += 1
            self._expirations += removed
        return removed

    def tier_of(self, record_id: str) -> Optional[MemoryTier]:
        with self._lock:
            for tier, bucket in self._tiers.items():
                if record_id in bucket:
                    return tier
        return None

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "sizes": {t.value: len(b) for t, b in self._tiers.items()},
                "promotions": self._promotions,
                "demotions": self._demotions,
                "evictions": self._evictions,
                "expirations": self._expirations,
                "uptime_seconds": time.time() - self._started_at,
            }

    # ---------------------------------------------------------- internals
    def _enforce_capacity_locked(self, tier: MemoryTier) -> None:
        cap = self._config.capacity_for(tier)
        bucket = self._tiers[tier]
        while len(bucket) > cap:
            # Evict the least recently used.
            bucket.popitem(last=False)
            self._evictions += 1
            # Try to demote rather than drop when possible.
            # (Kept simple: LRU eviction is preferred over silent demotion
            # to avoid runaway chain-promotions.)

    def statistics(self) -> Dict[str, Any]:
        snap = self.snapshot()
        snap["config"] = self._config.to_dict()
        snap["strict"] = self._strict
        return snap

    def to_dict(self, *, include_records: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "statistics": self.statistics(),
            }
            if include_records:
                payload["records"] = {
                    tier.value: [r.to_dict() for r in bucket.values()]
                    for tier, bucket in self._tiers.items()
                }
        return payload

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kw)

    def __repr__(self) -> str:
        with self._lock:
            return (
                "MemoryTierManager("
                f"hot={len(self._tiers[MemoryTier.HOT])}, "
                f"warm={len(self._tiers[MemoryTier.WARM])}, "
                f"cold={len(self._tiers[MemoryTier.COLD])}, "
                f"strict={self._strict})"
            )


__all__ = [
    "MemoryTier",
    "MemoryTierConfig",
    "MemoryTierError",
    "MemoryTierManager",
    "MemoryTierPolicy",
    "TieredRecord",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.memory_tier
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    mgr = MemoryTierManager()
    print("repr       :", mgr)

    # Store a few records in different tiers.
    mgr.store(
        record_id="dec-001", kind="decision_outcome",
        payload={"route": "edge_int8"}, importance=0.9,
        truth_level="measured",
    )
    mgr.store(
        record_id="dec-002", kind="decision_outcome",
        payload={"route": "cloud_gpu"}, importance=0.4,
        truth_level="measured",
    )
    mgr.store(
        record_id="sim-001", kind="simulation",
        payload={"note": "simulated"}, importance=0.9,
        truth_level="simulated",
    )

    print("tiers      :", mgr.snapshot()["sizes"])
    assert mgr.tier_of("dec-001") == MemoryTier.HOT
    assert mgr.tier_of("dec-002") == MemoryTier.WARM
    assert mgr.tier_of("sim-001") == MemoryTier.COLD

    # Promote a cold record to hot by reading it.
    got = mgr.get("sim-001")
    assert got is not None
    assert mgr.tier_of("sim-001") == MemoryTier.HOT
    print("promoted   :", mgr.snapshot()["sizes"])

    # Demote explicitly.
    demoted = mgr.demote("sim-001")
    assert demoted is not None and demoted.tier == MemoryTier.WARM
    print("demoted    :", mgr.snapshot()["sizes"])

    # Expire sweep.
    removed = mgr.expire()
    print("expired    :", removed)

    print("statistics :", {
        k: v for k, v in mgr.statistics().items()
        if k not in ("config", "sizes")
    })

    # Validation.
    for bad in (
        lambda: mgr.store(record_id="", kind="x", payload={}),
        lambda: mgr.store(record_id="x", kind="", payload={}),
        lambda: mgr.store(record_id="x", kind="x", payload={}, importance=1.5),
    ):
        try:
            bad()
        except MemoryTierError as exc:
            print("Rejected   :", exc)

    print("\nSmoke test passed.")
