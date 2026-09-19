# src/memory/policy_memory.py

"""
Policy Memory
=============

Version-aware policy storage with ``superseded_by`` semantics, TTL policies,
and safe publication via the write governor.

Enhancements
------------
- ``PolicyMemoryConfig`` — frozen, validated: semver policy, retention,
  TTL per policy kind, bounded history.
- ``PolicyMemory`` — thread-safe, serializable, governed writes.
- Automatic semver bumping on publish.
- Old policies marked ``superseded_by`` (never silently overwritten).
- ``PolicySnapshot`` — frozen, JSON-safe record.
- Custom ``PolicyMemoryError(ValueError)``.
- ``__main__`` smoke test with a mocked adapter and governor.
"""

from __future__ import annotations

import json
import logging
import math
import re
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Mapping, Optional, Tuple

from .memory_schemas import PolicyRecord, TruthLevel
from .supermemory_adapter import SupermemoryAdapter, SupermemoryAdapterError
from .write_governor import WriteGovernor, WriteGovernorError

logger = logging.getLogger(__name__)

__version__ = "5.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class PolicyMemoryError(ValueError):
    """Raised for invalid policy inputs or configuration."""


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
_SEMVER_RE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)$")


@dataclass(frozen=True)
class PolicyMemoryConfig:
    """Tunable parameters for policy memory."""

    policy_id: str = "default"
    initial_version: str = "v0.1.0"
    container_tag: str = "policy:current"
    max_history: int = 100
    max_versions_retained: int = 1_000

    # Policy kind → TTL (seconds) used when publishing.
    ttl_by_kind: Mapping[str, int] = field(
        default_factory=lambda: {
            "carbon": 365 * 24 * 3600,
            "helium": 180 * 24 * 3600,
            "safety": 365 * 24 * 3600,
            "default": 90 * 24 * 3600,
        }
    )

    require_governor: bool = True
    writer_id: str = "policy_editor"

    def __post_init__(self) -> None:
        for name in ("policy_id", "initial_version", "container_tag", "writer_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise PolicyMemoryError(f"{name} must be a non-empty string.")
        if not _SEMVER_RE.match(self.initial_version):
            raise PolicyMemoryError(
                f"initial_version must match vX.Y.Z, got {self.initial_version!r}."
            )
        for name in ("max_history", "max_versions_retained"):
            v = getattr(self, name)
            if not isinstance(v, int) or v <= 0:
                raise PolicyMemoryError(f"{name} must be a positive int.")
        if not isinstance(self.ttl_by_kind, Mapping):
            raise PolicyMemoryError("ttl_by_kind must be a Mapping.")
        for k, v in self.ttl_by_kind.items():
            if not isinstance(k, str) or not isinstance(v, int) or v <= 0:
                raise PolicyMemoryError(
                    "ttl_by_kind entries must be (str, positive int)."
                )

    def ttl_for(self, kind: str) -> int:
        return int(self.ttl_by_kind.get(kind, self.ttl_by_kind.get("default", 90 * 24 * 3600)))

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "policy_id": self.policy_id,
            "initial_version": self.initial_version,
            "container_tag": self.container_tag,
            "max_history": self.max_history,
            "max_versions_retained": self.max_versions_retained,
            "ttl_by_kind": dict(self.ttl_by_kind),
            "require_governor": self.require_governor,
            "writer_id": self.writer_id,
        }
        return d

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PolicyMemoryConfig":
        if not isinstance(data, Mapping):
            raise PolicyMemoryError("PolicyMemoryConfig.from_dict expects a Mapping.")
        valid = set(cls.__dataclass_fields__.keys())
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k == "ttl_by_kind":
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Snapshot
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PolicySnapshot:
    """Frozen snapshot of the current policy state."""

    policy_id: str
    version: str
    content: Mapping[str, Any]
    approved_by: str
    ttl_seconds: int
    published_at: datetime
    superseded_by: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "version": self.version,
            "content": dict(self.content),
            "approved_by": self.approved_by,
            "ttl_seconds": self.ttl_seconds,
            "published_at": self.published_at.isoformat(),
            "superseded_by": self.superseded_by,
        }

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kw)


# --------------------------------------------------------------------------- #
# Policy memory
# --------------------------------------------------------------------------- #
class PolicyMemory:
    """Versioned, governed storage for policies.

    Parameters
    ----------
    adapter : SupermemoryAdapter
    governor : WriteGovernor, optional
        Required when ``config.require_governor=True``.
    config : PolicyMemoryConfig, optional
    strict : bool, default True
    """

    def __init__(
        self,
        adapter: SupermemoryAdapter,
        *,
        governor: Optional[WriteGovernor] = None,
        config: Optional[PolicyMemoryConfig] = None,
        strict: bool = True,
    ) -> None:
        if not isinstance(adapter, SupermemoryAdapter):
            raise PolicyMemoryError("adapter must be a SupermemoryAdapter.")
        self._config = config or PolicyMemoryConfig()
        if self._config.require_governor and governor is None:
            raise PolicyMemoryError(
                "governor is required when require_governor=True."
            )
        if governor is not None and not isinstance(governor, WriteGovernor):
            raise PolicyMemoryError("governor must be a WriteGovernor.")

        self._adapter = adapter
        self._governor = governor
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._history: Deque[PolicyRecord] = deque(
            maxlen=self._config.max_versions_retained
        )
        self._current: Optional[PolicyRecord] = None
        self._publish_errors: int = 0
        self._supersede_count: int = 0
        self._started_at: float = time.time()

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> PolicyMemoryConfig:
        return self._config

    @property
    def current_version(self) -> Optional[str]:
        with self._lock:
            return self._current.version if self._current else None

    @property
    def current(self) -> Optional[PolicyRecord]:
        with self._lock:
            return self._current

    @property
    def history_size(self) -> int:
        with self._lock:
            return len(self._history)

    # ---------------------------------------------------------- public API
    def publish(
        self,
        content: Mapping[str, Any],
        *,
        kind: str = "default",
        approved_by: str = "system",
        bump: str = "minor",
    ) -> str:
        """Publish a new policy version.

        Parameters
        ----------
        content : Mapping
            The policy body.
        kind : str
            Policy kind (used to look up TTL).
        approved_by : str
            Identifier of the approver.
        bump : {"major", "minor", "patch"}
            Semver bump type applied to the current version.

        Returns
        -------
        str
            The new version string.
        """
        if not isinstance(content, Mapping):
            raise PolicyMemoryError("content must be a Mapping.")
        if not isinstance(kind, str) or not kind:
            raise PolicyMemoryError("kind must be a non-empty string.")
        if not isinstance(approved_by, str) or not approved_by:
            raise PolicyMemoryError("approved_by must be a non-empty string.")
        if bump not in ("major", "minor", "patch"):
            raise PolicyMemoryError("bump must be 'major', 'minor', or 'patch'.")

        with self._lock:
            current_version = (
                self._current.version
                if self._current else self._config.initial_version
            )
        new_version = self._bump_version(current_version, bump)

        record = PolicyRecord(
            version=new_version,
            content=dict(content),
            policy_id=self._config.policy_id,
            approved_by=approved_by,
            container_tag=self._config.container_tag,
            superseded_by=None,
        )

        # ---- Governance ------------------------------------------------
        if self._governor is not None:
            try:
                promoted = self._governor.promote(
                    record, writer_id=self._config.writer_id, kind="policy",
                )
            except WriteGovernorError as exc:
                if self._strict:
                    raise PolicyMemoryError(
                        f"governor rejected policy publish: {exc}"
                    ) from exc
                promoted = False
            if not promoted:
                with self._lock:
                    self._publish_errors += 1
                if self._strict:
                    raise PolicyMemoryError(
                        "policy publish rejected by write governor."
                    )
                return current_version

        # ---- Mark old version superseded ------------------------------
        with self._lock:
            old = self._current
            if old is not None:
                superseded = PolicyRecord(
                    version=old.version,
                    content=dict(old.content),
                    policy_id=old.policy_id,
                    approved_by=old.approved_by,
                    container_tag=old.container_tag,
                    published_at=old.published_at,
                    superseded_by=new_version,
                )
                try:
                    self._adapter.remember_policy(superseded)
                    self._supersede_count += 1
                except SupermemoryAdapterError as exc:
                    logger.warning("could not persist supersede: %s", exc)
                if len(self._history) >= 0:
                    self._history.append(superseded)

            # Persist the new policy.
            try:
                self._adapter.remember_policy(record)
            except SupermemoryAdapterError as exc:
                if self._strict:
                    raise PolicyMemoryError(
                        f"adapter rejected policy publish: {exc}"
                    ) from exc
                self._publish_errors += 1
                return current_version

            self._current = record
            self._history.append(record)

        logger.info(
            "Published policy %s -> %s (kind=%s, approved_by=%s).",
            self._config.policy_id, new_version, kind, approved_by,
        )
        return new_version

    def snapshot(self) -> Optional[PolicySnapshot]:
        """Return a frozen snapshot of the current policy."""
        with self._lock:
            current = self._current
        if current is None:
            return None
        return PolicySnapshot(
            policy_id=current.policy_id,
            version=current.version,
            content=dict(current.content),
            approved_by=current.approved_by,
            ttl_seconds=self._config.ttl_for("default"),
            published_at=current.published_at,
            superseded_by=current.superseded_by,
        )

    def get(self, version: str) -> Optional[PolicyRecord]:
        """Return a historical version by version string."""
        if not isinstance(version, str) or not version:
            raise PolicyMemoryError("version must be a non-empty string.")
        with self._lock:
            for record in reversed(self._history):
                if record.version == version:
                    return record
        return None

    def history(self, *, limit: Optional[int] = None) -> List[PolicyRecord]:
        with self._lock:
            records = list(self._history)
        if limit is not None:
            if not isinstance(limit, int) or limit <= 0:
                raise PolicyMemoryError("limit must be a positive int.")
            records = records[-limit:]
        return records

    def supersede(self, old_version: str, new_version: str) -> bool:
        """Explicitly mark ``old_version`` as superseded by ``new_version``."""
        if not old_version or not new_version:
            raise PolicyMemoryError(
                "old_version and new_version must be non-empty strings."
            )
        with self._lock:
            for i, record in enumerate(self._history):
                if record.version == old_version:
                    marked = PolicyRecord(
                        version=record.version,
                        content=dict(record.content),
                        policy_id=record.policy_id,
                        approved_by=record.approved_by,
                        container_tag=record.container_tag,
                        published_at=record.published_at,
                        superseded_by=new_version,
                    )
                    self._history[i] = marked
                    try:
                        self._adapter.remember_policy(marked)
                    except SupermemoryAdapterError as exc:
                        if self._strict:
                            raise PolicyMemoryError(
                                f"could not persist supersede: {exc}"
                            ) from exc
                    self._supersede_count += 1
                    return True
        return False

    # ---------------------------------------------------------- helpers
    @staticmethod
    def _bump_version(version: str, bump: str) -> str:
        m = _SEMVER_RE.match(version)
        if not m:
            raise PolicyMemoryError(f"cannot parse version {version!r}.")
        major, minor, patch = (int(x) for x in m.groups())
        if bump == "major":
            return f"v{major + 1}.0.0"
        if bump == "minor":
            return f"v{major}.{minor + 1}.0"
        return f"v{major}.{minor}.{patch + 1}"

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            history = list(self._history)
            current = self._current
        return {
            "policy_id": self._config.policy_id,
            "current_version": current.version if current else None,
            "history_size": len(history),
            "supersede_count": self._supersede_count,
            "publish_errors": self._publish_errors,
            "config": self._config.to_dict(),
            "strict": self._strict,
            "uptime_seconds": time.time() - self._started_at,
        }

    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "current_version": self._current.version if self._current else None,
                "statistics": self.statistics(),
            }
            if include_history:
                payload["history"] = [r.to_dict() for r in self._history]
        return payload

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kw)

    def __repr__(self) -> str:
        with self._lock:
            return (
                "PolicyMemory("
                f"policy_id={self._config.policy_id!r}, "
                f"current={self._current.version if self._current else None}, "
                f"history={len(self._history)}, "
                f"strict={self._strict})"
            )


__all__ = [
    "PolicyMemory",
    "PolicyMemoryConfig",
    "PolicyMemoryError",
    "PolicySnapshot",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.policy_memory
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    from .supermemory_adapter import SupermemoryAdapter
    from .write_governor import WriteGovernor

    adapter = SupermemoryAdapter()
    governor = WriteGovernor()
    governor.register_writer("policy_editor", capabilities={"write:policy"})

    memory = PolicyMemory(adapter=adapter, governor=governor)
    print("repr       :", memory)

    v1 = memory.publish(
        {"max_carbon_gco2e": 200.0, "allowed_regions": ["eu-west-1"]},
        kind="carbon",
        approved_by="ops",
    )
    print("published  :", v1)

    v2 = memory.publish(
        {"max_carbon_gco2e": 150.0, "allowed_regions": ["eu-west-1", "us-west-2"]},
        kind="carbon",
        approved_by="ops",
    )
    print("published  :", v2)

    # The old version should be marked superseded.
    old = memory.get(v1)
    assert old is not None and old.superseded_by == v2
    print("superseded :", old.version, "->", old.superseded_by)

    print("snapshot   :", memory.snapshot().to_dict())
    print("statistics :", {
        k: v for k, v in memory.statistics().items()
        if k not in ("config", "uptime_seconds")
    })

    # Rejected by governor (writer lacks capability).
    gov2 = WriteGovernor()
    gov2.register_writer("policy_editor", capabilities=set())
    mem2 = PolicyMemory(adapter=adapter, governor=gov2, strict=False)
    v_none = mem2.publish({"a": 1})
    assert v_none == mem2.config.initial_version
    print("rejected   : OK (returned initial_version)")

    print("\nSmoke test passed.")
