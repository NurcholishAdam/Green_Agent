# src/memory/policy_memory.py

"""
Policy Memory
=============

Version-aware policy storage with ``superseded_by`` semantics, TTL policies,
and safe publication via the write governor.

Enhancements
------------
- ``PolicyMemoryConfig`` — frozen, validated, fully serializable: semver
  policy, retention, TTL per policy kind (aligned with
  ``SupermemoryConfig.ttl_seconds``), truth-level vocabulary imported from
  ``memory_schemas``, bounded history, ``MappingProxyType``-frozen
  ``ttl_by_kind``, ``with_overrides`` / ``merge`` / ``from_dict`` /
  ``from_json``.
- ``PolicySnapshot`` — deeply frozen, hashable, with ``id``, ``kind``,
  ``truth_level``, ``container_tag``, ``observed_at``, ``schema_version``.
  ``to_episode_payload()`` and ``to_memory_dict()`` bridges. Full
  serialization symmetry.
- ``PolicyMemory`` — thread-safe, serializable, governed writes,
  ``statistics()`` / ``reset()`` / ``close()`` / context manager,
  async siblings, ``kind``-aware history, and a **fixed first-publish
  path** (uses ``initial_version`` as-is instead of bumping it) plus a
  **fixed history path** (replaces superseded entries in place instead
  of duplicating them).
- Correct ``kind`` pass-through to ``WriteGovernor.promote()``.
- ``kind`` persisted on each record (reserved ``__policy_kind__`` key)
  and exposed via ``PolicySnapshot.kind`` / ``kind_for(version)``.
- Structured error hierarchy:
  ``PolicyMemoryError`` → ``PolicyMemoryInputError``,
  ``PolicyMemoryConfigError``, ``PolicyMemoryGovernorError``,
  ``PolicyMemoryAdapterError``, ``PolicyMemoryParseError``.
- ``PolicyRecord.with_superseded_by()`` is used everywhere a superseded
  record needs to be produced — no brittle manual reconstruction.
- Non-strict failures return ``None`` (never a silent stale version).
- ``__version__`` exported via ``__all__``.
- ``__main__`` smoke test asserts exact version strings.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from types import MappingProxyType
from typing import (
    Any,
    Deque,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
)

from .memory_schemas import (
    PolicyRecord,
    TruthLevel,
)
from .supermemory_adapter import (
    SupermemoryAdapter,
    SupermemoryAdapterError,
)
from .write_governor import (
    WriteGovernor,
    WriteGovernorError,
)

logger = logging.getLogger(__name__)

__version__ = "6.0.0"

#: Version of the policy-memory contract itself.
SCHEMA_VERSION: int = 1

#: Aligned with ``SupermemoryConfig.ttl_seconds["policy"]`` = 1 year.
DEFAULT_POLICY_TTL: int = 365 * 24 * 3600

#: Default TTLs by policy kind (module-aligned + subject overrides).
DEFAULT_TTL_BY_KIND: Mapping[str, int] = MappingProxyType({
    # Generic policy record kind (matches SupermemoryConfig / MemoryTier).
    "policy":  DEFAULT_POLICY_TTL,
    # Subject-specific overrides.
    "carbon":  DEFAULT_POLICY_TTL,
    "helium":  180 * 24 * 3600,
    "safety":  DEFAULT_POLICY_TTL,
    # Fallback.
    "default": DEFAULT_POLICY_TTL,
})

#: Reserved content key used to persist the specific kind on records.
_RESERVED_CONTENT_KEY: str = "__policy_kind__"

_SEMVER_RE = re.compile(r"^(v?)(\d+)\.(\d+)\.(\d+)$")
_BUMP_TYPES: Tuple[str, ...] = ("major", "minor", "patch")


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class PolicyMemoryError(ValueError):
    """Base class for policy-memory problems."""


class PolicyMemoryInputError(PolicyMemoryError):
    """Invalid input to a public API."""


class PolicyMemoryConfigError(PolicyMemoryError):
    """Invalid configuration."""


class PolicyMemoryGovernorError(PolicyMemoryError):
    """Write governor rejected a publish or failed."""


class PolicyMemoryAdapterError(PolicyMemoryError):
    """Adapter failed to persist a policy."""


class PolicyMemoryParseError(PolicyMemoryError):
    """Failed to parse a config, snapshot or record from dict/JSON."""


# --------------------------------------------------------------------------- #
# Validation helpers — mirror the other enhanced modules
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


def _percentile(values: List[float], pct: float) -> float:
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
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PolicyMemoryConfig:
    """Tunable parameters for policy memory."""

    policy_id: str = "default"
    initial_version: str = "v0.1.0"
    container_tag: str = "policy:current"
    max_history: int = 100
    max_versions_retained: int = 1_000

    #: TTL by policy kind. Keys are aligned with
    #: ``SupermemoryConfig.ttl_seconds`` (``"policy"``, ``"carbon"``,
    #: ``"helium"``, ``"safety"``) with a ``"default"`` fallback.
    ttl_by_kind: Mapping[str, int] = field(
        default_factory=lambda: dict(DEFAULT_TTL_BY_KIND),
    )

    require_governor: bool = True
    writer_id: str = "policy_editor"

    #: Truth level assigned to every published policy.
    truth_level: str = TruthLevel.USER_REPORTED.value

    #: Schema version stamped on published records / snapshots.
    schema_version: int = SCHEMA_VERSION

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        for name in (
            "policy_id", "initial_version", "container_tag", "writer_id",
        ):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise PolicyMemoryConfigError(
                    f"{name} must be a non-empty string."
                )
        if not _SEMVER_RE.match(self.initial_version):
            raise PolicyMemoryConfigError(
                f"initial_version must match vX.Y.Z, got "
                f"{self.initial_version!r}."
            )
        for name in ("max_history", "max_versions_retained"):
            v = getattr(self, name)
            if not _is_real_int(v) or v <= 0:
                raise PolicyMemoryConfigError(
                    f"{name} must be a positive int (got {v!r})."
                )
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise PolicyMemoryConfigError(
                "schema_version must be a positive int."
            )
        if not isinstance(self.require_governor, bool):
            raise PolicyMemoryConfigError(
                "require_governor must be a bool."
            )
        try:
            truth = TruthLevel.coerce(self.truth_level).value
        except Exception as exc:
            raise PolicyMemoryConfigError(
                f"truth_level invalid: {exc}"
            ) from exc
        object.__setattr__(self, "truth_level", truth)

        if not isinstance(self.ttl_by_kind, ABCMapping):
            raise PolicyMemoryConfigError("ttl_by_kind must be a Mapping.")
        frozen_ttls: Dict[str, int] = {}
        for k, v in self.ttl_by_kind.items():
            if not isinstance(k, str) or not k:
                raise PolicyMemoryConfigError(
                    "ttl_by_kind keys must be non-empty strings."
                )
            if not _is_real_int(v) or v <= 0:
                raise PolicyMemoryConfigError(
                    f"ttl_by_kind[{k!r}] must be a positive int (got {v!r})."
                )
            frozen_ttls[k] = int(v)
        object.__setattr__(
            self, "ttl_by_kind", MappingProxyType(frozen_ttls),
        )

    # ------------------------------------------------------------------ #
    def ttl_for(self, kind: str) -> int:
        if kind in self.ttl_by_kind:
            return int(self.ttl_by_kind[kind])
        return int(self.ttl_by_kind.get("default", DEFAULT_POLICY_TTL))

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "initial_version": self.initial_version,
            "container_tag": self.container_tag,
            "max_history": self.max_history,
            "max_versions_retained": self.max_versions_retained,
            "ttl_by_kind": dict(self.ttl_by_kind),
            "require_governor": self.require_governor,
            "writer_id": self.writer_id,
            "truth_level": self.truth_level,
            "schema_version": self.schema_version,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> "PolicyMemoryConfig":
        if not isinstance(data, ABCMapping):
            raise PolicyMemoryConfigError(
                "PolicyMemoryConfig.from_dict expects a Mapping."
            )
        valid = {f.name for f in fields(cls)}
        unknown = set(data) - valid
        if strict and unknown:
            raise PolicyMemoryConfigError(
                f"Unknown config key(s): {sorted(unknown)}."
            )
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k == "ttl_by_kind":
                if not isinstance(v, ABCMapping):
                    raise PolicyMemoryConfigError(
                        "ttl_by_kind must be a Mapping."
                    )
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v
        try:
            return cls(**kwargs)
        except PolicyMemoryError:
            raise
        except (TypeError, ValueError) as exc:
            raise PolicyMemoryConfigError(
                f"failed to build PolicyMemoryConfig: {exc}"
            ) from exc

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: bool = False,
    ) -> "PolicyMemoryConfig":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise PolicyMemoryConfigError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise PolicyMemoryConfigError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data, strict=strict)

    # ------------------------------------------------------------------ #
    def with_overrides(self, **kwargs: Any) -> "PolicyMemoryConfig":
        valid = {f.name for f in fields(self)}
        unknown = set(kwargs) - valid
        if unknown:
            raise PolicyMemoryConfigError(
                f"Unknown config field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def merge(self, other: "PolicyMemoryConfig") -> "PolicyMemoryConfig":
        defaults = PolicyMemoryConfig()
        overrides: Dict[str, Any] = {}
        for f in fields(self):
            other_val = getattr(other, f.name)
            default_val = getattr(defaults, f.name)
            if other_val != default_val:
                overrides[f.name] = other_val
        return self.with_overrides(**overrides)

    def __hash__(self) -> int:
        return hash((
            self.policy_id,
            self.initial_version,
            self.container_tag,
            self.max_history,
            self.max_versions_retained,
            tuple(sorted(self.ttl_by_kind.items())),
            self.require_governor,
            self.writer_id,
            self.truth_level,
            self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            "PolicyMemoryConfig("
            f"policy_id={self.policy_id!r}, "
            f"initial_version={self.initial_version!r}, "
            f"retained={self.max_versions_retained})"
        )


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
    kind: str = "default"
    truth_level: str = TruthLevel.USER_REPORTED.value
    container_tag: str = "policy:current"
    schema_version: int = SCHEMA_VERSION

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        for name in (
            "policy_id", "version", "approved_by", "kind", "container_tag",
        ):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise PolicyMemoryInputError(
                    f"{name} must be a non-empty string."
                )
        try:
            object.__setattr__(
                self, "truth_level",
                TruthLevel.coerce(self.truth_level).value,
            )
        except Exception as exc:
            raise PolicyMemoryInputError(
                f"truth_level invalid: {exc}"
            ) from exc
        if not _is_real_int(self.ttl_seconds) or self.ttl_seconds <= 0:
            raise PolicyMemoryInputError(
                "ttl_seconds must be a positive int."
            )
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise PolicyMemoryInputError(
                "schema_version must be a positive int."
            )
        if not isinstance(self.published_at, datetime):
            raise PolicyMemoryInputError(
                "published_at must be a datetime."
            )
        if self.published_at.tzinfo is None:
            object.__setattr__(
                self, "published_at",
                self.published_at.replace(tzinfo=timezone.utc),
            )
        if self.superseded_by is not None:
            if not isinstance(self.superseded_by, str) or not self.superseded_by:
                raise PolicyMemoryInputError(
                    "superseded_by must be None or a non-empty string."
                )
        if not isinstance(self.content, ABCMapping):
            raise PolicyMemoryInputError("content must be a Mapping.")
        object.__setattr__(
            self, "content",
            MappingProxyType(dict(self.content)),
        )

    # ------------------------------------------------------------------ #
    @property
    def id(self) -> str:
        return f"{self.policy_id}:{self.version}"

    @property
    def observed_at(self) -> datetime:
        return self.published_at

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "version": self.version,
            "content": dict(self.content),
            "approved_by": self.approved_by,
            "ttl_seconds": self.ttl_seconds,
            "published_at": self.published_at.isoformat(),
            "superseded_by": self.superseded_by,
            "kind": self.kind,
            "truth_level": self.truth_level,
            "container_tag": self.container_tag,
            "schema_version": self.schema_version,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PolicySnapshot":
        if not isinstance(data, ABCMapping):
            raise PolicyMemoryParseError(
                "PolicySnapshot.from_dict expects a Mapping."
            )
        try:
            published = (
                _parse_iso_datetime(data.get("published_at"))
                or datetime.now(timezone.utc)
            )
            return cls(
                policy_id=str(data["policy_id"]),
                version=str(data["version"]),
                content=dict(data.get("content", {})),
                approved_by=str(data.get("approved_by", "system")),
                ttl_seconds=int(data.get("ttl_seconds", DEFAULT_POLICY_TTL)),
                published_at=published,
                superseded_by=data.get("superseded_by"),
                kind=str(data.get("kind", "default")),
                truth_level=data.get(
                    "truth_level", TruthLevel.USER_REPORTED.value,
                ),
                container_tag=str(
                    data.get("container_tag", "policy:current"),
                ),
                schema_version=int(
                    data.get("schema_version", SCHEMA_VERSION),
                ),
            )
        except KeyError as exc:
            raise PolicyMemoryParseError(
                f"PolicySnapshot.from_dict missing key {exc.args[0]!r}."
            ) from exc

    @classmethod
    def from_json(cls, payload: str) -> "PolicySnapshot":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise PolicyMemoryParseError(
                f"PolicySnapshot.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    def _render_content(self) -> str:
        return (
            f"Policy {self.policy_id} {self.version} "
            f"(kind={self.kind}) approved by {self.approved_by}:\n"
            f"{json.dumps(dict(self.content), indent=2, sort_keys=True)}"
        )

    def _metadata(self, *, container_tag: str) -> Dict[str, Any]:
        return {
            "kind": self.kind,
            "type": "policy",
            "record_id": self.id,
            "policy_id": self.policy_id,
            "version": self.version,
            "approved_by": self.approved_by,
            "truth_level": self.truth_level,
            "container_tag": container_tag,
            "observed_at": self.published_at.isoformat(),
            "published_at": self.published_at.isoformat(),
            "superseded_by": self.superseded_by,
            "ttl_seconds": self.ttl_seconds,
            "schema_version": self.schema_version,
        }

    def to_episode_payload(
        self,
        *,
        container_tag: Optional[str] = None,
        content: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Persistence bridge for ``EpisodicMemory`` / ``SupermemoryAdapter``."""
        tag = container_tag if container_tag is not None else self.container_tag
        if not isinstance(tag, str) or not tag:
            raise PolicyMemoryInputError(
                "container_tag must be a non-empty string."
            )
        return {
            "content": content if content is not None else self._render_content(),
            "container_tag": tag,
            "metadata": self._metadata(container_tag=tag),
        }

    def to_memory_dict(self) -> Dict[str, Any]:
        """Return ``{"id", "content", "metadata"}`` for ``BoundedRecall``."""
        return {
            "id": self.id,
            "content": self._render_content(),
            "metadata": self._metadata(container_tag=self.container_tag),
        }

    # ------------------------------------------------------------------ #
    def __hash__(self) -> int:
        try:
            content_hash = hash(tuple(sorted(self.content.items())))
        except TypeError:
            content_hash = hash(
                json.dumps(dict(self.content), default=str, sort_keys=True)
            )
        return hash((
            self.policy_id, self.version, content_hash, self.approved_by,
            self.ttl_seconds, self.published_at, self.superseded_by,
            self.kind, self.truth_level, self.container_tag,
            self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            f"PolicySnapshot(policy_id={self.policy_id!r}, "
            f"version={self.version!r}, "
            f"kind={self.kind!r}, "
            f"superseded_by={self.superseded_by!r})"
        )


# --------------------------------------------------------------------------- #
# Policy memory
# --------------------------------------------------------------------------- #
class PolicyMemory:
    """Versioned, governed storage for policies.

    Parameters
    ----------
    adapter : SupermemoryAdapter
        Persistence backend for policy records.
    governor : WriteGovernor, optional
        Required when ``config.require_governor=True``.
    config : PolicyMemoryConfig, optional
        Frozen configuration.
    strict : bool, default True
        If True, governor rejections and adapter failures raise. If False,
        ``publish()`` returns ``None`` and the failure is recorded on
        ``statistics()``.
    """

    _LATENCY_RING_SIZE: int = 200

    def __init__(
        self,
        adapter: SupermemoryAdapter,
        *,
        governor: Optional[WriteGovernor] = None,
        config: Optional[PolicyMemoryConfig] = None,
        strict: bool = True,
    ) -> None:
        if not isinstance(adapter, SupermemoryAdapter):
            raise PolicyMemoryInputError(
                "adapter must be a SupermemoryAdapter."
            )
        if config is None:
            config = PolicyMemoryConfig()
        elif not isinstance(config, PolicyMemoryConfig):
            raise PolicyMemoryInputError(
                "config must be a PolicyMemoryConfig or None."
            )
        if config.require_governor and governor is None:
            raise PolicyMemoryConfigError(
                "governor is required when require_governor=True."
            )
        if governor is not None and not isinstance(governor, WriteGovernor):
            raise PolicyMemoryInputError(
                "governor must be a WriteGovernor or None."
            )

        self._adapter = adapter
        self._governor = governor
        self._config = config
        self._strict = bool(strict)

        self._lock = threading.RLock()

        # OrderedDict preserves publication order and supports O(1)
        # replacement of a superseded entry by version.
        self._history: "OrderedDict[str, PolicyRecord]" = OrderedDict()
        self._current: Optional[PolicyRecord] = None

        # Counters
        self._publish_successes: int = 0
        self._publish_errors: int = 0
        self._governor_rejections: int = 0
        self._adapter_errors: int = 0
        self._supersede_count: int = 0
        self._last_error: Optional[str] = None
        self._last_published_at: Optional[float] = None
        self._latency_ring: Deque[float] = deque(
            maxlen=self._LATENCY_RING_SIZE
        )
        self._started_at: float = time.monotonic()

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> PolicyMemoryConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def adapter(self) -> SupermemoryAdapter:
        return self._adapter

    @property
    def governor(self) -> Optional[WriteGovernor]:
        return self._governor

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

    def __len__(self) -> int:
        return self.history_size

    def __contains__(self, version: object) -> bool:
        if not isinstance(version, str):
            return False
        with self._lock:
            return version in self._history

    def __iter__(self) -> Iterator[PolicyRecord]:
        with self._lock:
            return iter(list(self._history.values()))

    # ---------------------------------------------------------- lifecycle
    def close(self) -> None:
        """Best-effort no-op. Kept for symmetry with sibling classes."""
        return None

    def __enter__(self) -> "PolicyMemory":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ---------------------------------------------------------- public API
    def publish(
        self,
        content: Mapping[str, Any],
        *,
        kind: str = "default",
        approved_by: str = "system",
        bump: str = "minor",
    ) -> Optional[str]:
        """Publish a new policy version.

        On the very first publish, ``initial_version`` is used as-is
        (not bumped). On every subsequent publish, ``bump`` is applied
        to the current version.

        Returns the newly-published version string on success, or
        ``None`` when a non-strict failure occurs.
        """
        # ------------------------------------------------------- validate
        if not isinstance(content, ABCMapping):
            raise PolicyMemoryInputError("content must be a Mapping.")
        if not isinstance(kind, str) or not kind:
            raise PolicyMemoryInputError(
                "kind must be a non-empty string."
            )
        if not isinstance(approved_by, str) or not approved_by:
            raise PolicyMemoryInputError(
                "approved_by must be a non-empty string."
            )
        if bump not in _BUMP_TYPES:
            raise PolicyMemoryInputError(
                f"bump must be one of {_BUMP_TYPES}, got {bump!r}."
            )

        start = time.perf_counter()

        with self._lock:
            current = self._current
            is_first = current is None and not self._history
            if is_first:
                new_version = self._config.initial_version
            else:
                base_version = (
                    current.version if current else self._config.initial_version
                )
                new_version = self._bump_version(base_version, bump)

        # Inject the reserved kind key so it survives persistence.
        record_content: Dict[str, Any] = dict(content)
        record_content[_RESERVED_CONTENT_KEY] = kind

        record = PolicyRecord(
            version=new_version,
            content=record_content,
            policy_id=self._config.policy_id,
            approved_by=approved_by,
            container_tag=self._config.container_tag,
            truth_level=self._config.truth_level,
            superseded_by=None,
            schema_version=self._config.schema_version,
        )

        # ------------------------------------------------------- governor
        if self._governor is not None:
            try:
                promoted = self._governor.promote(
                    record,
                    writer_id=self._config.writer_id,
                    kind=kind,
                )
            except WriteGovernorError as exc:
                with self._lock:
                    self._publish_errors += 1
                    self._governor_rejections += 1
                    self._last_error = f"governor: {exc}"
                self._record_latency(start)
                if self._strict:
                    raise PolicyMemoryGovernorError(
                        f"governor rejected policy publish: {exc}"
                    ) from exc
                logger.warning(
                    "governor rejected policy publish: %s", exc,
                )
                return None
            except Exception as exc:  # noqa: BLE001 - defensive
                with self._lock:
                    self._publish_errors += 1
                    self._governor_rejections += 1
                    self._last_error = f"governor: {exc}"
                self._record_latency(start)
                if self._strict:
                    raise PolicyMemoryGovernorError(
                        f"governor error: {exc}"
                    ) from exc
                logger.warning("governor error: %s", exc)
                return None

            if not promoted:
                with self._lock:
                    self._publish_errors += 1
                    self._governor_rejections += 1
                    self._last_error = "governor rejected"
                self._record_latency(start)
                if self._strict:
                    raise PolicyMemoryGovernorError(
                        "policy publish rejected by write governor."
                    )
                logger.warning("policy publish rejected by write governor.")
                return None

        # ------------------------------------------------------- adapter
        with self._lock:
            old = self._current

            if old is not None:
                superseded = self._build_superseded(old, new_version)
                try:
                    self._adapter.remember_policy(superseded)
                    self._supersede_count += 1
                except SupermemoryAdapterError as exc:
                    logger.warning("could not persist supersede: %s", exc)
                    with self._lock:
                        self._adapter_errors += 1
                        self._last_error = f"supersede: {exc}"
                # Replace the existing entry in-place — never duplicate.
                self._history[old.version] = superseded

            try:
                self._adapter.remember_policy(record)
            except SupermemoryAdapterError as exc:
                with self._lock:
                    self._publish_errors += 1
                    self._adapter_errors += 1
                    self._last_error = f"adapter: {exc}"
                self._record_latency(start)
                if self._strict:
                    raise PolicyMemoryAdapterError(
                        f"adapter rejected policy publish: {exc}"
                    ) from exc
                logger.warning("adapter rejected policy publish: %s", exc)
                return None

            self._current = record
            self._history[record.version] = record
            self._trim_history_locked()
            self._publish_successes += 1
            self._last_published_at = time.time()

        self._record_latency(start)
        logger.info(
            "Published policy %s -> %s (kind=%s, approved_by=%s).",
            self._config.policy_id, new_version, kind, approved_by,
        )
        return new_version

    async def publish_async(
        self,
        content: Mapping[str, Any],
        *,
        kind: str = "default",
        approved_by: str = "system",
        bump: str = "minor",
    ) -> Optional[str]:
        return await asyncio.to_thread(
            self.publish,
            content,
            kind=kind,
            approved_by=approved_by,
            bump=bump,
        )

    def snapshot(self) -> Optional[PolicySnapshot]:
        """Return a frozen snapshot of the current policy."""
        with self._lock:
            current = self._current
            if current is None:
                return None
            kind = self._kind_of(current)
            content = {
                k: v for k, v in current.content.items()
                if k != _RESERVED_CONTENT_KEY
            }
        return PolicySnapshot(
            policy_id=current.policy_id,
            version=current.version,
            content=content,
            approved_by=current.approved_by,
            ttl_seconds=self._config.ttl_for(kind),
            published_at=current.published_at,
            superseded_by=current.superseded_by,
            kind=kind,
            truth_level=current.truth_level,
            container_tag=current.container_tag,
            schema_version=current.schema_version,
        )

    async def snapshot_async(self) -> Optional[PolicySnapshot]:
        return await asyncio.to_thread(self.snapshot)

    def get(self, version: str) -> Optional[PolicyRecord]:
        """Return a historical version by version string (unique)."""
        if not isinstance(version, str) or not version:
            raise PolicyMemoryInputError(
                "version must be a non-empty string."
            )
        with self._lock:
            return self._history.get(version)

    async def get_async(self, version: str) -> Optional[PolicyRecord]:
        return await asyncio.to_thread(self.get, version)

    def kind_for(self, version: str) -> Optional[str]:
        """Return the specific policy kind for ``version``, if known."""
        if not isinstance(version, str) or not version:
            raise PolicyMemoryInputError(
                "version must be a non-empty string."
            )
        with self._lock:
            record = self._history.get(version)
        if record is None:
            return None
        return self._kind_of(record)

    def history(
        self,
        *,
        limit: Optional[int] = None,
    ) -> List[PolicyRecord]:
        """Return historical records in publication order."""
        if limit is not None:
            if not _is_real_int(limit) or limit <= 0:
                raise PolicyMemoryInputError(
                    "limit must be a positive int."
                )
        with self._lock:
            records = list(self._history.values())
        if limit is not None:
            records = records[-limit:]
        return records

    async def history_async(
        self,
        *,
        limit: Optional[int] = None,
    ) -> List[PolicyRecord]:
        return await asyncio.to_thread(self.history, limit=limit)

    def history_by_kind(self, kind: str) -> List[PolicyRecord]:
        if not isinstance(kind, str) or not kind:
            raise PolicyMemoryInputError(
                "kind must be a non-empty string."
            )
        with self._lock:
            return [
                r for r in self._history.values()
                if self._kind_of(r) == kind
            ]

    def supersede(self, old_version: str, new_version: str) -> bool:
        """Explicitly mark ``old_version`` as superseded by ``new_version``."""
        if not isinstance(old_version, str) or not old_version:
            raise PolicyMemoryInputError(
                "old_version must be a non-empty string."
            )
        if not isinstance(new_version, str) or not new_version:
            raise PolicyMemoryInputError(
                "new_version must be a non-empty string."
            )
        with self._lock:
            record = self._history.get(old_version)
            if record is None:
                return False
            marked = self._build_superseded(record, new_version)
            self._history[old_version] = marked
            if self._current is not None and self._current.version == old_version:
                self._current = marked
            try:
                self._adapter.remember_policy(marked)
                self._supersede_count += 1
            except SupermemoryAdapterError as exc:
                with self._lock:
                    self._adapter_errors += 1
                    self._last_error = f"supersede: {exc}"
                if self._strict:
                    raise PolicyMemoryAdapterError(
                        f"could not persist supersede: {exc}"
                    ) from exc
                logger.warning("could not persist supersede: %s", exc)
                return False
            return True

    # ---------------------------------------------------------- internals
    def _kind_of(self, record: PolicyRecord) -> str:
        raw = record.content.get(_RESERVED_CONTENT_KEY)
        if isinstance(raw, str) and raw:
            return raw
        return "default"

    def _build_superseded(
        self, old: PolicyRecord, new_version: str,
    ) -> PolicyRecord:
        """Produce a superseded copy of ``old``.

        Prefers ``PolicyRecord.with_superseded_by()`` when available
        (the enhanced schema exposes it); falls back to reconstruction
        otherwise.
        """
        helper = getattr(old, "with_superseded_by", None)
        if callable(helper):
            try:
                return helper(new_version)
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug(
                    "with_superseded_by failed, reconstructing: %s", exc,
                )
        # Fallback: reconstruct by hand.
        return PolicyRecord(
            version=old.version,
            content=dict(old.content),
            policy_id=old.policy_id,
            approved_by=old.approved_by,
            container_tag=old.container_tag,
            truth_level=getattr(old, "truth_level", TruthLevel.USER_REPORTED.value),
            published_at=old.published_at,
            superseded_by=new_version,
            schema_version=getattr(old, "schema_version", SCHEMA_VERSION),
        )

    def _trim_history_locked(self) -> None:
        while len(self._history) > self._config.max_versions_retained:
            self._history.popitem(last=False)

    @staticmethod
    def _bump_version(version: str, bump: str) -> str:
        m = _SEMVER_RE.match(version)
        if not m:
            raise PolicyMemoryInputError(
                f"cannot parse version {version!r}."
            )
        prefix, major_s, minor_s, patch_s = m.groups()
        major, minor, patch = int(major_s), int(minor_s), int(patch_s)
        if bump == "major":
            return f"{prefix}{major + 1}.0.0"
        if bump == "minor":
            return f"{prefix}{major}.{minor + 1}.0"
        return f"{prefix}{major}.{minor}.{patch + 1}"

    def _record_latency(self, start: float) -> None:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        with self._lock:
            self._latency_ring.append(elapsed_ms)

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            lats = list(self._latency_ring)
            mean_lat = sum(lats) / len(lats) if lats else 0.0
            return {
                "policy_id": self._config.policy_id,
                "current_version": (
                    self._current.version if self._current else None
                ),
                "history_size": len(self._history),
                "publish_successes": self._publish_successes,
                "publish_errors": self._publish_errors,
                "governor_rejections": self._governor_rejections,
                "adapter_errors": self._adapter_errors,
                "supersede_count": self._supersede_count,
                "last_error": self._last_error,
                "last_published_at": self._last_published_at,
                "mean_latency_ms": mean_lat,
                "p50_latency_ms": _percentile(lats, 50),
                "p95_latency_ms": _percentile(lats, 95),
                "max_latency_ms": max(lats) if lats else 0.0,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "has_governor": self._governor is not None,
                "uptime_seconds": time.monotonic() - self._started_at,
            }

    def reset(self, *, clear_history: bool = False) -> int:
        """Reset counters (and optionally the in-memory history).

        Returns the number of history entries cleared.
        """
        with self._lock:
            removed = 0
            if clear_history:
                removed = len(self._history)
                self._history.clear()
                self._current = None
            self._publish_successes = 0
            self._publish_errors = 0
            self._governor_rejections = 0
            self._adapter_errors = 0
            self._supersede_count = 0
            self._last_error = None
            self._last_published_at = None
            self._latency_ring.clear()
            self._started_at = time.monotonic()
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            current = self._current
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "current_version": current.version if current else None,
                "statistics": self.statistics(),
            }
            if include_history:
                payload["history"] = [
                    r.to_dict() for r in self._history.values()
                ]
        return payload

    def to_json(
        self,
        *,
        include_history: bool = False,
        indent: Optional[int] = None,
    ) -> str:
        return json.dumps(
            self.to_dict(include_history=include_history),
            default=str, indent=indent, sort_keys=True,
        )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        adapter: SupermemoryAdapter,
        governor: Optional[WriteGovernor] = None,
        strict: Optional[bool] = None,
        restore_history: bool = False,
    ) -> "PolicyMemory":
        if not isinstance(data, ABCMapping):
            raise PolicyMemoryInputError(
                "PolicyMemory.from_dict expects a Mapping."
            )
        cfg_blob = data.get("config", {})
        config = (
            cfg_blob if isinstance(cfg_blob, PolicyMemoryConfig)
            else PolicyMemoryConfig.from_dict(cfg_blob)
        )
        resolved_strict = (
            bool(data.get("strict", True)) if strict is None else bool(strict)
        )
        mem = cls(
            adapter=adapter,
            governor=governor,
            config=config,
            strict=resolved_strict,
        )
        if restore_history and isinstance(data.get("history"), list):
            with mem._lock:  # noqa: SLF001 - intentional
                for raw in data["history"]:
                    try:
                        rec = PolicyRecord.from_dict(raw)
                    except Exception as exc:
                        logger.warning(
                            "from_dict: skipping malformed policy: %s", exc,
                        )
                        continue
                    mem._history[rec.version] = rec
                    if rec.superseded_by is None:
                        mem._current = rec
                mem._trim_history_locked()
        return mem

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        adapter: SupermemoryAdapter,
        governor: Optional[WriteGovernor] = None,
        strict: Optional[bool] = None,
        restore_history: bool = False,
    ) -> "PolicyMemory":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise PolicyMemoryInputError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise PolicyMemoryInputError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(
            data,
            adapter=adapter,
            governor=governor,
            strict=strict,
            restore_history=restore_history,
        )

    # ---------------------------------------------------------------- repr
    def __repr__(self) -> str:
        with self._lock:
            return (
                "PolicyMemory("
                f"policy_id={self._config.policy_id!r}, "
                f"current="
                f"{self._current.version if self._current else None}, "
                f"history={len(self._history)}, "
                f"strict={self._strict})"
            )


__all__ = [
    "DEFAULT_POLICY_TTL",
    "DEFAULT_TTL_BY_KIND",
    "PolicyMemory",
    "PolicyMemoryConfig",
    "PolicyMemoryError",
    "PolicyMemoryInputError",
    "PolicyMemoryConfigError",
    "PolicyMemoryGovernorError",
    "PolicyMemoryAdapterError",
    "PolicyMemoryParseError",
    "PolicySnapshot",
    "SCHEMA_VERSION",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.policy_memory
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    from .supermemory_adapter import SupermemoryAdapter
    from .write_governor import WriteGovernor

    # ----------------------------------------------------------- 1. Basic publish
    adapter = SupermemoryAdapter()          # offline, in-memory mirror
    governor = WriteGovernor()
    governor.register_writer("policy_editor", capabilities={"write:policy"})

    memory = PolicyMemory(adapter=adapter, governor=governor)
    print("repr         :", memory)

    v1 = memory.publish(
        {"max_carbon_gco2e": 200.0, "allowed_regions": ["eu-west-1"]},
        kind="carbon",
        approved_by="ops",
    )
    assert v1 == "v0.1.0", f"first publish should be initial_version, got {v1!r}"
    print("first        :", v1)

    v2 = memory.publish(
        {"max_carbon_gco2e": 150.0, "allowed_regions": ["eu-west-1", "us-west-2"]},
        kind="carbon",
        approved_by="ops",
    )
    assert v2 == "v0.2.0", f"minor bump expected, got {v2!r}"
    print("minor        :", v2)

    v3 = memory.publish(
        {"max_carbon_gco2e": 140.0},
        kind="carbon",
        approved_by="ops",
        bump="patch",
    )
    assert v3 == "v0.2.1", f"patch bump expected, got {v3!r}"
    print("patch        :", v3)

    # ----------------------------------------------------------- 2. History integrity
    # History should contain exactly 3 unique versions (no duplicates).
    history = memory.history()
    versions = [r.version for r in history]
    assert versions == ["v0.1.0", "v0.2.0", "v0.2.1"], versions
    assert len(set(versions)) == len(versions) == 3
    print("history size :", memory.history_size, "(unique)")

    # The first version should be marked superseded.
    old = memory.get("v0.1.0")
    assert old is not None and old.superseded_by == "v0.2.0"
    old2 = memory.get("v0.2.0")
    assert old2 is not None and old2.superseded_by == "v0.2.1"
    assert memory.get("v0.2.1").superseded_by is None
    print("superseded   : OK")

    # ----------------------------------------------------------- 3. Snapshot
    snap = memory.snapshot()
    assert snap is not None
    assert snap.version == "v0.2.1"
    assert snap.kind == "carbon"
    assert snap.id == "default:v0.2.1"
    assert snap.ttl_seconds == DEFAULT_TTL_BY_KIND["carbon"]
    # The reserved content key is stripped from the snapshot.
    assert "__policy_kind__" not in snap.content
    assert snap.truth_level == TruthLevel.USER_REPORTED.value
    print("snapshot     :", snap.version, snap.kind)

    # ----------------------------------------------------------- 4. kind_for
    assert memory.kind_for("v0.1.0") == "carbon"
    assert memory.kind_for("v0.2.1") == "carbon"
    assert memory.kind_for("nope") is None
    print("kind_for     : OK")

    # ----------------------------------------------------------- 5. history_by_kind
    all_carbon = memory.history_by_kind("carbon")
    assert len(all_carbon) == 3
    assert memory.history_by_kind("helium") == []
    print("history_by_kind : OK")

    # ----------------------------------------------------------- 6. Supersede explicit
    assert memory.supersede("v0.2.1", "v0.3.0") is True
    updated = memory.get("v0.2.1")
    assert updated is not None and updated.superseded_by == "v0.3.0"
    assert memory.supersede("nonexistent", "v0.4.0") is False
    print("supersede()  : OK")

    # ----------------------------------------------------------- 7. Governor rejects
    gov2 = WriteGovernor()
    gov2.register_writer("policy_editor", capabilities=set())
    mem2 = PolicyMemory(adapter=adapter, governor=gov2, strict=False)
    result = mem2.publish({"a": 1})
    assert result is None, "non-strict rejection should return None"
    stats = mem2.statistics()
    assert stats["governor_rejections"] == 1
    assert stats["publish_errors"] == 1
    print("non-strict   : returned None, counted")

    mem_strict = PolicyMemory(adapter=adapter, governor=gov2, strict=True)
    try:
        mem_strict.publish({"a": 1})
    except PolicyMemoryGovernorError as exc:
        print("strict       :", exc)
    else:
        raise AssertionError("expected governor rejection")

    # ----------------------------------------------------------- 8. Bridges
    payload = snap.to_episode_payload(container_tag="policy:carbon")
    assert payload["metadata"]["kind"] == "carbon"
    assert payload["metadata"]["policy_id"] == "default"
    assert payload["metadata"]["version"] == "v0.2.1"
    md = snap.to_memory_dict()
    assert md["id"] == "default:v0.2.1"
    assert md["metadata"]["container_tag"] == snap.container_tag
    print("bridges      : OK")

    # ----------------------------------------------------------- 9. Serialization
    cfg = PolicyMemoryConfig()
    assert PolicyMemoryConfig.from_dict(cfg.to_dict()) == cfg
    assert PolicyMemoryConfig.from_json(cfg.to_json()) == cfg
    cfg2 = cfg.with_overrides(max_history=42)
    assert cfg2.max_history == 42 and cfg.max_history == 100
    hash(cfg)
    print("cfg RT       : OK")

    snap_rt = PolicySnapshot.from_dict(snap.to_dict())
    assert snap_rt.version == snap.version
    assert snap_rt.kind == snap.kind
    assert dict(snap_rt.content) == dict(snap.content)
    hash(snap)
    print("snapshot RT  : OK (hashable)")

    mem_payload = memory.to_dict(include_history=True)
    assert mem_payload["current_version"] == "v0.2.1"
    assert len(mem_payload["history"]) == 3
    print("memory RT    : OK")

    # ----------------------------------------------------------- 10. Config validation
    bad_configs = [
        dict(policy_id=""),
        dict(initial_version="not-a-version"),
        dict(max_history=0),
        dict(max_history=True),
        dict(max_versions_retained=-1),
        dict(ttl_by_kind={"policy": 0}),
        dict(ttl_by_kind={"policy": True}),
        dict(truth_level="bogus"),
        dict(require_governor="yes"),
    ]
    for bad in bad_configs:
        try:
            PolicyMemoryConfig(**bad)  # type: ignore[arg-type]
        except PolicyMemoryError as exc:
            print(f"reject cfg   : {list(bad)[0]} -> {exc}")
        else:
            raise AssertionError(f"expected rejection for {bad!r}")

    # ----------------------------------------------------------- 11. Async
    async def _run_async():
        gov3 = WriteGovernor()
        gov3.register_writer("policy_editor", capabilities={"write:policy"})
        mem3 = PolicyMemory(adapter=adapter, governor=gov3)
        v_async = await mem3.publish_async(
            {"async": True}, kind="safety", approved_by="ci",
        )
        snap_async = await mem3.snapshot_async()
        hist = await mem3.history_async()
        return v_async, snap_async, hist

    v_async, snap_async, hist = asyncio.run(_run_async())
    assert v_async == "v0.1.0"
    assert snap_async is not None and snap_async.kind == "safety"
    assert len(hist) == 1
    print("async        : OK")

    # ----------------------------------------------------------- 12. Context manager
    with PolicyMemory(
        adapter=adapter, governor=governor,
    ) as ctx:
        assert ctx.publish({"x": 1}) == "v0.1.0"
    print("ctx mgr      : OK")

    # ----------------------------------------------------------- 13. Statistics / reset
    stats = memory.statistics()
    print("statistics   :", {
        k: v for k, v in stats.items()
        if k not in ("config", "last_published_at")
    })
    cleared = memory.reset(clear_history=True)
    assert cleared == 3
    assert memory.current_version is None
    assert memory.history_size == 0
    print("reset        :", cleared, "cleared")

    print("\nSmoke test passed.")
