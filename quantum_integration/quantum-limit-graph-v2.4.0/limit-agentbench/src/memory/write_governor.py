# src/memory/write_governor.py

"""
Write Governor
==============

Implements the "Govern writes: only validated agents or a post-run
evaluator should promote a result into durable memory" constraint.

Enhancements
------------
- ``WriteGovernorConfig`` — frozen, validated: per-kind capabilities
  (frozen ``MappingProxyType``), quarantine cap, audit history size,
  quarantine-on-rejection policy, ``schema_version``. Fully serializable
  with ``with_overrides`` / ``merge`` / ``from_dict`` / ``from_json``;
  hashable.
- ``ApprovalDecision`` — frozen, hashable, with ``id``, ``scope``,
  ``schema_version``, full serialization symmetry, and
  ``to_episode_payload()`` / ``to_memory_dict()`` bridges.
- **Cross-module capability map fix**: the default ``required_capability``
  now covers ``carbon`` / ``helium`` / ``safety`` (policy subjects used
  by ``PolicyMemory.publish``) and ``run`` (the outcome TTL kind), so
  the default governor authorizes the kinds the rest of the pipeline
  actually publishes.
- ``RLock``-guarded registry, quarantine queue, audit log; wall-clock
  only for serialized timestamps; monotonic for uptime.
- ``register_writer(overwrite=False)`` refuses silent capability
  clobbering by default.
- ``_infer_kind`` prefers the enhanced ``record.kind`` property, falls
  back to class-name matching, and rejects ambiguity rather than
  guessing.
- ``_safe()`` never leaks full ``repr`` — falls back to
  ``{"repr_type": type(record).__name__}``.
- Quarantine-on-rejection is configurable; failed writes are counted
  separately from quarantine entries.
- Full validation of ``writer_id``, ``scope``, and record kinds.
- Observability: ``snapshot()``, ``statistics()`` (with
  ``schema_version``, ``last_error``, quarantine eviction counter),
  ``audit_tail`` / ``quarantine_tail``, filtered ``audit_filter``.
- ``reset()`` / ``close()`` / sync + async context manager.
- ``from_config()`` / ``from_pipeline()`` constructors; async siblings
  for every public method; ``assert_compatible()`` on the class.
- Structured error hierarchy:
  ``WriteGovernorError`` → ``WriteGovernorInputError``,
  ``WriteGovernorConfigError``, ``WriteGovernorParseError``.
- ``__version__`` / ``SCHEMA_VERSION`` / ``DEFAULT_CONTAINER_TAG``
  exported via ``__all__``.
- ``__main__`` smoke test covering every new path.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
from collections import deque
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from types import MappingProxyType
from typing import (
    Any,
    Deque,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "6.1.0"

#: Version of the write-governor contract itself.
SCHEMA_VERSION: int = 1

#: Default container tag (matches ``SupermemoryConfig.default_container_tag``
#: and ``memory_schemas.DEFAULT_CONTAINER_TAG``).
DEFAULT_CONTAINER_TAG: str = "org:green-agent"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class WriteGovernorError(ValueError):
    """Base class for governor problems."""


class WriteGovernorInputError(WriteGovernorError):
    """Invalid input to a public API."""


class WriteGovernorConfigError(WriteGovernorError):
    """Invalid configuration."""


class WriteGovernorParseError(WriteGovernorError):
    """Failed to parse a config or decision from dict/JSON."""


# --------------------------------------------------------------------------- #
# Shared helpers
# --------------------------------------------------------------------------- #
def _is_real_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


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


def _coerce_int(name: str, value: Any, *, positive: bool = False) -> int:
    if isinstance(value, bool):
        raise WriteGovernorParseError(f"{name} must be an int.")
    if isinstance(value, int):
        iv = int(value)
    elif isinstance(value, float) and math.isfinite(value) and value.is_integer():
        iv = int(value)
    elif isinstance(value, str):
        s = value.strip()
        try:
            iv = int(s)
        except ValueError as exc:
            raise WriteGovernorParseError(
                f"{name} must be an int (got {value!r})."
            ) from exc
    else:
        raise WriteGovernorParseError(
            f"{name} must be an int (got {type(value).__name__})."
        )
    if positive and iv <= 0:
        raise WriteGovernorParseError(f"{name} must be a positive int.")
    return iv


def _coerce_nonempty_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise WriteGovernorInputError(
            f"{name} must be a non-empty string."
        )
    return value


# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
#: Default per-kind capabilities, mirroring the enhanced schema TTL kinds
#: and the policy subjects used by ``PolicyMemory.publish(kind=...)``.
#:
#: Item fix #3: this map is the reason ``PolicyMemory.publish(kind="carbon")``
#: used to be rejected by a default governor. Every kind the rest of the
#: pipeline publishes must appear here.
_DEFAULT_REQUIRED_CAPABILITY: Mapping[str, str] = MappingProxyType({
    # Schema record kinds.
    "decision_outcome": "write:decision",
    "policy":            "write:policy",
    "incident":          "write:incident",
    "outcome":           "write:outcome",
    "run":               "write:outcome",
    # Policy subject kinds — all authorized by write:policy.
    "carbon":            "write:policy",
    "helium":            "write:policy",
    "safety":            "write:policy",
    # Tier/subject kinds used by memory_tier and run_memory.
    "grid_forecast":     "write:policy",
    "thermal_state":     "write:policy",
    "connectivity":      "write:policy",
    "meta_policy":       "write:policy",
    "default":           "write:policy",
})


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class WriteGovernorConfig:
    """Tunable parameters for the write governor."""

    schema_version: int = SCHEMA_VERSION

    #: Per-kind capabilities required to promote a record. Frozen.
    required_capability: Mapping[str, str] = field(
        default_factory=lambda: dict(_DEFAULT_REQUIRED_CAPABILITY),
    )

    quarantine_capacity: int = 1_000
    audit_history_capacity: int = 10_000

    #: If True (default), every rejection is also written to quarantine.
    #: If False, only explicit ``quarantine()`` calls push entries.
    quarantine_on_rejection: bool = True

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise WriteGovernorConfigError(
                "schema_version must be a positive int."
            )
        if not isinstance(self.required_capability, ABCMapping):
            raise WriteGovernorConfigError(
                "required_capability must be a Mapping."
            )
        frozen: Dict[str, str] = {}
        for k, v in self.required_capability.items():
            if not isinstance(k, str) or not k:
                raise WriteGovernorConfigError(
                    "required_capability keys must be non-empty strings."
                )
            if not isinstance(v, str) or not v:
                raise WriteGovernorConfigError(
                    "required_capability values must be non-empty strings."
                )
            frozen[k] = v
        # Item fix #2: freeze the mapping.
        object.__setattr__(
            self, "required_capability", MappingProxyType(frozen),
        )
        for name in ("quarantine_capacity", "audit_history_capacity"):
            v = getattr(self, name)
            if not _is_real_int(v) or v <= 0:
                raise WriteGovernorConfigError(
                    f"{name} must be a positive int (got {v!r})."
                )
        if not isinstance(self.quarantine_on_rejection, bool):
            raise WriteGovernorConfigError(
                "quarantine_on_rejection must be a bool."
            )

    # ------------------------------------------------------------------ #
    def capability_for(self, kind: str) -> Optional[str]:
        """Return the required capability for ``kind``, or ``None``."""
        return self.required_capability.get(kind)

    def kinds(self) -> Tuple[str, ...]:
        return tuple(sorted(self.required_capability.keys()))

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        # Item fix #11: explicit field map — no more ``asdict``.
        return {
            "schema_version": self.schema_version,
            "required_capability": dict(self.required_capability),
            "quarantine_capacity": self.quarantine_capacity,
            "audit_history_capacity": self.audit_history_capacity,
            "quarantine_on_rejection": self.quarantine_on_rejection,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> "WriteGovernorConfig":
        if not isinstance(data, ABCMapping):
            raise WriteGovernorConfigError(
                "WriteGovernorConfig.from_dict expects a Mapping."
            )
        valid = {f.name for f in fields(cls)}
        unknown = set(data) - valid
        if strict and unknown:
            raise WriteGovernorConfigError(
                f"Unknown config key(s): {sorted(unknown)}."
            )
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k == "required_capability":
                if not isinstance(v, ABCMapping):
                    raise WriteGovernorConfigError(
                        "required_capability must be a Mapping."
                    )
                kwargs[k] = dict(v)
            else:
                kwargs[k] = v
        try:
            return cls(**kwargs)
        except WriteGovernorError:
            raise
        except (TypeError, ValueError) as exc:
            raise WriteGovernorConfigError(
                f"failed to build WriteGovernorConfig: {exc}"
            ) from exc

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: bool = False,
    ) -> "WriteGovernorConfig":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise WriteGovernorConfigError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise WriteGovernorConfigError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data, strict=strict)

    # ------------------------------------------------------------------ #
    def with_overrides(self, **kwargs: Any) -> "WriteGovernorConfig":
        valid = {f.name for f in fields(self)}
        unknown = set(kwargs) - valid
        if unknown:
            raise WriteGovernorConfigError(
                f"Unknown config field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def merge(self, other: "WriteGovernorConfig") -> "WriteGovernorConfig":
        defaults = WriteGovernorConfig()
        overrides: Dict[str, Any] = {}
        for f in fields(self):
            other_val = getattr(other, f.name)
            default_val = getattr(defaults, f.name)
            if other_val != default_val:
                overrides[f.name] = other_val
        return self.with_overrides(**overrides)

    def __hash__(self) -> int:
        # Item fix #10: hashable because required_capability is frozen.
        return hash((
            self.schema_version,
            tuple(sorted(self.required_capability.items())),
            self.quarantine_capacity,
            self.audit_history_capacity,
            self.quarantine_on_rejection,
        ))

    def __repr__(self) -> str:
        return (
            "WriteGovernorConfig("
            f"kinds={len(self.required_capability)}, "
            f"quarantine_cap={self.quarantine_capacity}, "
            f"audit_cap={self.audit_history_capacity})"
        )


# --------------------------------------------------------------------------- #
# Approval decision
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ApprovalDecision:
    """Immutable record of one write-approval check."""

    writer_id: str
    kind: str
    approved: bool
    reason: str
    scope: str = "default"
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )
    schema_version: int = SCHEMA_VERSION

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        for name in ("writer_id", "kind", "reason", "scope"):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise WriteGovernorInputError(
                    f"{name} must be a non-empty string."
                )
        if not isinstance(self.approved, bool):
            raise WriteGovernorInputError("approved must be a bool.")
        if not isinstance(self.timestamp, datetime):
            raise WriteGovernorInputError(
                "timestamp must be a datetime."
            )
        if self.timestamp.tzinfo is None:
            object.__setattr__(
                self, "timestamp",
                self.timestamp.replace(tzinfo=timezone.utc),
            )
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise WriteGovernorInputError(
                "schema_version must be a positive int."
            )

    # ------------------------------------------------------------------ #
    @property
    def id(self) -> str:
        return (
            f"approval:{self.writer_id}:"
            f"{self.timestamp.isoformat()}"
        )

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "writer_id": self.writer_id,
            "kind": self.kind,
            "approved": self.approved,
            "reason": self.reason,
            "scope": self.scope,
            "timestamp": self.timestamp.isoformat(),
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ApprovalDecision":
        if not isinstance(data, ABCMapping):
            raise WriteGovernorParseError(
                "ApprovalDecision.from_dict expects a Mapping."
            )
        try:
            writer_id = data["writer_id"]
            kind = data["kind"]
        except KeyError as exc:
            raise WriteGovernorParseError(
                f"ApprovalDecision.from_dict missing key "
                f"{exc.args[0]!r}."
            ) from exc
        ts = _parse_iso_datetime(data.get("timestamp")) or datetime.now(
            timezone.utc,
        )
        return cls(
            writer_id=str(writer_id),
            kind=str(kind),
            approved=bool(data.get("approved", False)),
            reason=str(data.get("reason", "unspecified")),
            scope=str(data.get("scope", "default")),
            timestamp=ts,
            schema_version=_coerce_int(
                "schema_version",
                data.get("schema_version", SCHEMA_VERSION),
                positive=True,
            ),
        )

    @classmethod
    def from_json(cls, payload: str) -> "ApprovalDecision":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise WriteGovernorParseError(
                f"ApprovalDecision.from_json invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise WriteGovernorParseError(
                "ApprovalDecision.from_json expected a JSON object."
            )
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    # Bridges
    # ------------------------------------------------------------------ #
    def _render_content(self) -> str:
        status = "APPROVED" if self.approved else "REJECTED"
        return (
            f"Write approval {status}: writer={self.writer_id!r} "
            f"kind={self.kind!r} scope={self.scope!r} "
            f"reason={self.reason!r}"
        )

    def _metadata(self, *, container_tag: str) -> Dict[str, Any]:
        return {
            "kind": "write_approval",
            "type": "write_approval",
            "record_id": self.id,
            "truth_level": "measured",
            "container_tag": container_tag,
            "observed_at": self.timestamp.isoformat(),
            "schema_version": self.schema_version,
            "writer_id": self.writer_id,
            "record_kind": self.kind,
            "scope": self.scope,
            "approved": self.approved,
            "reason": self.reason,
        }

    def to_episode_payload(
        self,
        *,
        container_tag: Optional[str] = None,
        content: Optional[str] = None,
        truth_level: Optional[str] = None,
    ) -> Dict[str, Any]:
        tag = container_tag or DEFAULT_CONTAINER_TAG
        if not isinstance(tag, str) or not tag:
            raise WriteGovernorInputError(
                "container_tag must be a non-empty string."
            )
        meta = self._metadata(container_tag=tag)
        if truth_level is not None:
            if not isinstance(truth_level, str) or not truth_level:
                raise WriteGovernorInputError(
                    "truth_level must be None or a non-empty string."
                )
            meta["truth_level"] = truth_level
        return {
            "content": content if content is not None else self._render_content(),
            "container_tag": tag,
            "metadata": meta,
        }

    def to_memory_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "content": self._render_content(),
            "metadata": self._metadata(container_tag=DEFAULT_CONTAINER_TAG),
        }

    # ------------------------------------------------------------------ #
    def __hash__(self) -> int:
        return hash((
            self.writer_id, self.kind, self.approved, self.reason,
            self.scope, self.timestamp, self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            f"ApprovalDecision(writer_id={self.writer_id!r}, "
            f"kind={self.kind!r}, approved={self.approved}, "
            f"reason={self.reason!r})"
        )


# --------------------------------------------------------------------------- #
# Governor
# --------------------------------------------------------------------------- #
class WriteGovernor:
    """Decides which writers may promote which kinds of records."""

    _LATENCY_RING_SIZE: int = 200

    def __init__(
        self,
        *,
        config: Optional[WriteGovernorConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is None:
            config = WriteGovernorConfig()
        elif not isinstance(config, WriteGovernorConfig):
            raise WriteGovernorInputError(
                "config must be a WriteGovernorConfig or None."
            )
        self._config = config
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._writers: Dict[str, Set[str]] = {}
        self._scopes: Dict[str, str] = {}
        self._quarantine: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.quarantine_capacity
        )
        self._audit: Deque[ApprovalDecision] = deque(
            maxlen=self._config.audit_history_capacity
        )
        self._quarantine_evictions: int = 0
        self._audit_evictions: int = 0
        self._rejections_quarantined: int = 0
        self._last_error: Optional[str] = None
        self._latency_ring: Deque[float] = deque(
            maxlen=self._LATENCY_RING_SIZE
        )
        # Item fix #1: monotonic clock for uptime.
        self._started_at: float = time.monotonic()

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> WriteGovernorConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def quarantine_size(self) -> int:
        with self._lock:
            return len(self._quarantine)

    @property
    def audit_size(self) -> int:
        with self._lock:
            return len(self._audit)

    def __len__(self) -> int:
        with self._lock:
            return len(self._writers)

    def __contains__(self, writer_id: object) -> bool:
        if not isinstance(writer_id, str) or not writer_id:
            return False
        with self._lock:
            return writer_id in self._writers

    # ---------------------------------------------------------- constructors
    @classmethod
    def from_config(
        cls,
        config: WriteGovernorConfig,
        *,
        strict: bool = True,
    ) -> "WriteGovernor":
        return cls(config=config, strict=strict)

    @classmethod
    def from_pipeline(
        cls,
        pipeline: Any,
        *,
        config: Optional[WriteGovernorConfig] = None,
        strict: Optional[bool] = None,
    ) -> "WriteGovernor":
        """Return ``pipeline.governor`` if present, else build a fresh one."""
        existing = getattr(pipeline, "governor", None)
        if isinstance(existing, cls):
            return existing
        resolved = True if strict is None else bool(strict)
        return cls(config=config, strict=resolved)

    # ---------------------------------------------------------- lifecycle
    def close(self) -> None:
        """Best-effort no-op. Kept for symmetry with sibling classes."""
        return None

    def __enter__(self) -> "WriteGovernor":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    async def __aenter__(self) -> "WriteGovernor":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        self.close()

    # ---------------------------------------------------------- writers
    def register_writer(
        self,
        writer_id: str,
        *,
        scope: str = "default",
        capabilities: Optional[Iterable[str]] = None,
        overwrite: bool = False,
    ) -> None:
        """Register a writer.

        Parameters
        ----------
        writer_id : str
        scope : str
        capabilities : Iterable[str], optional
        overwrite : bool
            Item fix #5: if False (default) and the writer is already
            registered with different capabilities, raise. If True,
            replace silently.

        Raises
        ------
        WriteGovernorInputError
            If inputs are invalid, or the writer is already registered
            and ``overwrite=False``.
        """
        _coerce_nonempty_str("writer_id", writer_id)
        _coerce_nonempty_str("scope", scope)
        if capabilities is None:
            caps: Set[str] = set()
        else:
            if isinstance(capabilities, (str, bytes)):
                raise WriteGovernorInputError(
                    "capabilities must be a sequence of strings, "
                    "not a bare string."
                )
            try:
                caps = set(capabilities)
            except TypeError as exc:
                raise WriteGovernorInputError(
                    f"capabilities must be iterable: {exc}"
                ) from exc
        for c in caps:
            if not isinstance(c, str) or not c:
                raise WriteGovernorInputError(
                    "capabilities entries must be non-empty strings."
                )

        with self._lock:
            existing = self._writers.get(writer_id)
            if (
                existing is not None
                and existing != caps
                and not overwrite
            ):
                raise WriteGovernorInputError(
                    f"writer {writer_id!r} is already registered with "
                    f"capabilities {sorted(existing)}; pass "
                    f"overwrite=True to replace."
                )
            self._writers[writer_id] = caps
            self._scopes[writer_id] = scope
        logger.debug(
            "Registered writer %s (scope=%s, caps=%s, overwrite=%s).",
            writer_id, scope, sorted(caps), overwrite,
        )

    def unregister_writer(self, writer_id: str) -> bool:
        """Remove a writer from the registry. Returns True if it existed."""
        # Item fix #9: validate the id.
        _coerce_nonempty_str("writer_id", writer_id)
        with self._lock:
            existed = writer_id in self._writers
            self._writers.pop(writer_id, None)
            self._scopes.pop(writer_id, None)
        return existed

    def capabilities(self, writer_id: str) -> FrozenSet[str]:
        """Return the capabilities for ``writer_id`` (empty if unknown)."""
        _coerce_nonempty_str("writer_id", writer_id)
        with self._lock:
            return frozenset(self._writers.get(writer_id, set()))

    def writer_scope(self, writer_id: str) -> Optional[str]:
        """Return the scope of ``writer_id``, or ``None`` if unknown."""
        _coerce_nonempty_str("writer_id", writer_id)
        with self._lock:
            return self._scopes.get(writer_id)

    def has_capability(self, writer_id: str, capability: str) -> bool:
        """Return True if ``writer_id`` has ``capability``."""
        _coerce_nonempty_str("writer_id", writer_id)
        _coerce_nonempty_str("capability", capability)
        with self._lock:
            caps = self._writers.get(writer_id)
        if caps is None:
            return False
        return capability in caps

    def list_writers(self) -> Dict[str, Dict[str, Any]]:
        """Return a snapshot of registered writers."""
        with self._lock:
            return {
                wid: {
                    "scope": self._scopes.get(wid, "default"),
                    "capabilities": sorted(caps),
                }
                for wid, caps in self._writers.items()
            }

    # ---------------------------------------------------------- approvals
    def approve(
        self,
        writer_id: str,
        record: Any,
        *,
        kind: Optional[str] = None,
    ) -> ApprovalDecision:
        """Return an ``ApprovalDecision`` for ``record`` written by ``writer_id``."""
        _coerce_nonempty_str("writer_id", writer_id)
        if record is None:
            raise WriteGovernorInputError("record must not be None.")
        if kind is not None:
            _coerce_nonempty_str("kind", kind)

        start = time.perf_counter()
        resolved_kind = kind or self._infer_kind(record)
        if resolved_kind is None:
            decision = ApprovalDecision(
                writer_id=writer_id,
                kind="unknown",
                approved=False,
                reason="unable to infer record kind",
                scope=self.writer_scope(writer_id) or "default",
            )
            self._record(decision)
            self._record_latency(start)
            return decision

        required = self._config.required_capability.get(resolved_kind)
        if required is None:
            decision = ApprovalDecision(
                writer_id=writer_id,
                kind=resolved_kind,
                approved=False,
                reason=(
                    f"no capability configured for kind {resolved_kind!r}"
                ),
                scope=self.writer_scope(writer_id) or "default",
            )
            self._record(decision)
            self._record_latency(start)
            return decision

        with self._lock:
            caps = self._writers.get(writer_id)
            scope = self._scopes.get(writer_id, "default")
        if caps is None:
            decision = ApprovalDecision(
                writer_id=writer_id,
                kind=resolved_kind,
                approved=False,
                reason=f"writer {writer_id!r} is not registered",
                scope=scope,
            )
        elif required not in caps:
            decision = ApprovalDecision(
                writer_id=writer_id,
                kind=resolved_kind,
                approved=False,
                reason=(
                    f"writer {writer_id!r} lacks capability {required!r}"
                ),
                scope=scope,
            )
        else:
            decision = ApprovalDecision(
                writer_id=writer_id,
                kind=resolved_kind,
                approved=True,
                reason=f"capability {required!r} present",
                scope=scope,
            )
        self._record(decision)
        self._record_latency(start)
        return decision

    def promote(
        self,
        record: Any,
        *,
        writer_id: str,
        kind: Optional[str] = None,
    ) -> bool:
        """Return True if the record is approved for durable memory.

        Item fix #8: quarantine-on-rejection is configurable via
        ``config.quarantine_on_rejection``. When enabled, only the
        record's payload is quarantined — the audit log receives the
        decision regardless.
        """
        decision = self.approve(writer_id, record, kind=kind)
        if decision.approved:
            return True
        if self._config.quarantine_on_rejection:
            self.quarantine(record, reason=decision.reason, kind=decision.kind)
        else:
            logger.debug(
                "promote(): rejecting %s/%s but not quarantining "
                "(quarantine_on_rejection=False).",
                decision.writer_id, decision.kind,
            )
        return False

    def quarantine(
        self,
        record: Any,
        *,
        reason: str,
        kind: Optional[str] = None,
    ) -> None:
        """Append a record to the quarantine queue."""
        _coerce_nonempty_str("reason", reason)
        if kind is not None:
            _coerce_nonempty_str("kind", kind)
        resolved_kind = kind or self._infer_kind(record) or "unknown"
        entry = {
            "reason": reason,
            "kind": resolved_kind,
            "timestamp": time.time(),   # wall clock — serializable
            "record": self._safe(record),
        }
        with self._lock:
            before = len(self._quarantine)
            self._quarantine.append(entry)
            if before == self._quarantine.maxlen:
                self._quarantine_evictions += 1
        logger.warning("Quarantined record (%s): %s", resolved_kind, reason)

    # ---------------------------------------------------------- helpers
    def _infer_kind(self, record: Any) -> Optional[str]:
        """Infer a record kind from the record itself.

        Item fix #6: prefers the enhanced ``record.kind`` property
        (which mirrors ``TTL_KIND``), then ``record.TYPE_NAME``, then a
        class-name match, then ``to_supermemory_payload()`` — never
        guessing from ambiguous strings.
        """
        if record is None:
            return None

        # 1) The enhanced schemas expose a ``kind`` property.
        kind_attr = getattr(record, "kind", None)
        if callable(kind_attr):
            try:
                v = kind_attr()
            except Exception:
                v = None
            if isinstance(v, str) and v:
                return v
        elif isinstance(kind_attr, str) and kind_attr:
            return kind_attr

        # 2) The class-level ``TTL_KIND`` constant.
        ttl_kind = getattr(record, "TTL_KIND", None)
        if isinstance(ttl_kind, str) and ttl_kind:
            return ttl_kind

        # 3) The class-level ``TYPE_NAME`` constant.
        type_name = getattr(record, "TYPE_NAME", None)
        if isinstance(type_name, str) and type_name:
            return type_name

        # 4) Class-name match (legacy). Note that we use `isinstance`-
        # aware mapping so subclasses still map.
        name = type(record).__name__
        for candidate, kind in (
            ("DecisionRecord", "decision_outcome"),
            ("PolicyRecord", "policy"),
            ("IncidentRecord", "incident"),
            ("OutcomeRecord", "outcome"),
        ):
            if name == candidate:
                return kind

        # 5) Duck-typed payload — read the canonical ``metadata.kind``
        # first, falling back to ``metadata.type`` only for legacy
        # records that never carried ``kind``.
        to_payload = getattr(record, "to_supermemory_payload", None)
        if callable(to_payload):
            try:
                payload = to_payload()
            except Exception:
                return None
            if isinstance(payload, ABCMapping):
                meta = payload.get("metadata") or {}
                if isinstance(meta, ABCMapping):
                    v = meta.get("kind") or meta.get("type")
                    if isinstance(v, str) and v:
                        return v
        return None

    def _safe(self, record: Any) -> Any:
        """Return a JSON-safe view of ``record``.

        Item fix #7: never falls back to ``repr(record)`` — that could
        leak secrets from arbitrary objects into logs and quarantine
        entries. Instead, the fallback is a minimal stub.
        """
        to_dict = getattr(record, "to_dict", None)
        if callable(to_dict):
            try:
                result = to_dict()
            except Exception:
                result = None
            if isinstance(result, ABCMapping):
                try:
                    json.dumps(result, default=str)
                except (TypeError, ValueError):
                    pass
                else:
                    return dict(result)
        # Fallback: expose only the type name.
        return {"repr_type": type(record).__name__}

    def _record(self, decision: ApprovalDecision) -> None:
        with self._lock:
            before = len(self._audit)
            self._audit.append(decision)
            if before == self._audit.maxlen:
                self._audit_evictions += 1

    def _record_latency(self, start: float) -> None:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        with self._lock:
            self._latency_ring.append(elapsed_ms)

    # ---------------------------------------------------------- audit view
    def audit_tail(self, n: int = 10) -> List[ApprovalDecision]:
        """Return the ``n`` most recent approval decisions."""
        if not _is_real_int(n) or n <= 0:
            raise WriteGovernorInputError("n must be a positive int.")
        with self._lock:
            return list(self._audit)[-n:]

    def quarantine_tail(self, n: int = 10) -> List[Dict[str, Any]]:
        """Return the ``n`` most recent quarantine entries."""
        if not _is_real_int(n) or n <= 0:
            raise WriteGovernorInputError("n must be a positive int.")
        with self._lock:
            return [dict(e) for e in list(self._quarantine)[-n:]]

    def audit_filter(
        self,
        *,
        writer_id: Optional[str] = None,
        kind: Optional[str] = None,
        approved: Optional[bool] = None,
        limit: Optional[int] = None,
    ) -> List[ApprovalDecision]:
        """Return audit entries matching the filters."""
        if writer_id is not None:
            _coerce_nonempty_str("writer_id", writer_id)
        if kind is not None:
            _coerce_nonempty_str("kind", kind)
        if approved is not None and not isinstance(approved, bool):
            raise WriteGovernorInputError("approved must be None or a bool.")
        if limit is not None and (not _is_real_int(limit) or limit <= 0):
            raise WriteGovernorInputError(
                "limit must be None or a positive int."
            )
        with self._lock:
            entries = list(self._audit)
        out = [
            d for d in entries
            if (writer_id is None or d.writer_id == writer_id)
            and (kind is None or d.kind == kind)
            and (approved is None or d.approved is approved)
        ]
        if limit is not None:
            out = out[-limit:]
        return out

    def clear_quarantine(self) -> int:
        """Clear the quarantine queue. Returns the number of entries removed."""
        with self._lock:
            removed = len(self._quarantine)
            self._quarantine.clear()
        return removed

    def clear_audit(self) -> int:
        """Clear the audit log. Returns the number of entries removed."""
        with self._lock:
            removed = len(self._audit)
            self._audit.clear()
        return removed

    # ---------------------------------------------------------- async
    async def register_writer_async(self, *args: Any, **kwargs: Any) -> None:
        return await asyncio.to_thread(
            self.register_writer, *args, **kwargs,
        )

    async def approve_async(
        self,
        writer_id: str,
        record: Any,
        *,
        kind: Optional[str] = None,
    ) -> ApprovalDecision:
        return await asyncio.to_thread(
            self.approve, writer_id, record, kind=kind,
        )

    async def promote_async(
        self,
        record: Any,
        *,
        writer_id: str,
        kind: Optional[str] = None,
    ) -> bool:
        return await asyncio.to_thread(
            self.promote, record, writer_id=writer_id, kind=kind,
        )

    # ---------------------------------------------------------- serialization
    @classmethod
    def assert_compatible(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> None:
        if not isinstance(data, ABCMapping):
            raise WriteGovernorParseError(
                "WriteGovernor.assert_compatible expects a Mapping."
            )
        v = data.get("schema_version", SCHEMA_VERSION)
        if not _is_real_int(v) or v <= 0:
            raise WriteGovernorParseError(
                f"invalid schema_version {v!r} in governor payload."
            )
        if v > SCHEMA_VERSION:
            raise WriteGovernorParseError(
                f"governor payload schema_version {v} is newer than the "
                f"current contract {SCHEMA_VERSION}."
            )
        if strict and v < SCHEMA_VERSION:
            raise WriteGovernorParseError(
                f"governor payload schema_version {v} is older than the "
                f"current contract {SCHEMA_VERSION}."
            )

    def snapshot(self) -> Dict[str, Any]:
        """Return a read-only snapshot of the governor's full state."""
        with self._lock:
            writers = {
                wid: {
                    "scope": self._scopes.get(wid, "default"),
                    "capabilities": sorted(caps),
                }
                for wid, caps in self._writers.items()
            }
            audit = list(self._audit)
            quarantine = [dict(e) for e in self._quarantine]
        return {
            "schema_version": SCHEMA_VERSION,
            "config": self._config.to_dict(),
            "strict": self._strict,
            "writers": writers,
            "audit": [d.to_dict() for d in audit],
            "quarantine": quarantine,
            "statistics": self.statistics(),
        }

    def to_dict(
        self,
        *,
        include_audit: bool = False,
        include_quarantine: bool = False,
    ) -> Dict[str, Any]:
        """Return a JSON-friendly dict.

        Item fix #11: the returned shape carries ``config``, ``strict``,
        ``statistics`` and optionally ``writers`` / ``audit`` /
        ``quarantine``. It replaces the older ``to_json()`` that only
        serialized ``statistics()``.
        """
        with self._lock:
            writers = {
                wid: {
                    "scope": self._scopes.get(wid, "default"),
                    "capabilities": sorted(caps),
                }
                for wid, caps in self._writers.items()
            }
            payload: Dict[str, Any] = {
                "schema_version": SCHEMA_VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "writers": writers,
                "statistics": self.statistics(),
            }
            if include_audit:
                payload["audit"] = [d.to_dict() for d in self._audit]
            if include_quarantine:
                payload["quarantine"] = [
                    dict(e) for e in self._quarantine
                ]
        return payload

    def to_json(
        self,
        *,
        include_audit: bool = False,
        include_quarantine: bool = False,
        indent: Optional[int] = None,
    ) -> str:
        return json.dumps(
            self.to_dict(
                include_audit=include_audit,
                include_quarantine=include_quarantine,
            ),
            default=str,
            indent=indent,
            sort_keys=True,
        )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: Optional[bool] = None,
        restore_writers: bool = False,
    ) -> "WriteGovernor":
        """Rebuild a governor from a ``to_dict()`` payload."""
        if not isinstance(data, ABCMapping):
            raise WriteGovernorParseError(
                "WriteGovernor.from_dict expects a Mapping."
            )
        cls.assert_compatible(data)

        cfg_blob = data.get("config", {})
        if isinstance(cfg_blob, WriteGovernorConfig):
            config = cfg_blob
        else:
            config = WriteGovernorConfig.from_dict(cfg_blob)

        resolved_strict = (
            bool(data.get("strict", True))
            if strict is None
            else bool(strict)
        )
        gov = cls(config=config, strict=resolved_strict)

        if restore_writers and isinstance(data.get("writers"), ABCMapping):
            with gov._lock:  # noqa: SLF001 - intentional
                for wid, info in data["writers"].items():
                    if not isinstance(wid, str) or not wid:
                        continue
                    if not isinstance(info, ABCMapping):
                        continue
                    scope = str(info.get("scope", "default")) or "default"
                    caps = info.get("capabilities", []) or []
                    try:
                        cap_set = {str(c) for c in caps}
                    except TypeError:
                        continue
                    gov._writers[wid] = cap_set
                    gov._scopes[wid] = scope
        return gov

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: Optional[bool] = None,
        restore_writers: bool = False,
    ) -> "WriteGovernor":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise WriteGovernorParseError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise WriteGovernorParseError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(
            data, strict=strict, restore_writers=restore_writers,
        )

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Item fix #1 / #4: monotonic uptime, ``schema_version``, and
        a unified ``last_error`` plus eviction counters."""
        with self._lock:
            lats = list(self._latency_ring)
            mean_lat = sum(lats) / len(lats) if lats else 0.0
            audit = list(self._audit)
            writers_count = len(self._writers)
            quarantine_size = len(self._quarantine)
            audit_size = len(audit)
            q_evictions = self._quarantine_evictions
            a_evictions = self._audit_evictions
            rej_q = self._rejections_quarantined
        approvals = sum(1 for d in audit if d.approved)
        rejections = audit_size - approvals
        return {
            "schema_version": SCHEMA_VERSION,
            "writers": writers_count,
            "approvals": approvals,
            "rejections": rejections,
            "quarantine_size": quarantine_size,
            "quarantine_capacity": self._config.quarantine_capacity,
            "quarantine_evictions": q_evictions,
            "rejections_quarantined": rej_q,
            "audit_size": audit_size,
            "audit_capacity": self._config.audit_history_capacity,
            "audit_evictions": a_evictions,
            "mean_latency_ms": mean_lat,
            "p50_latency_ms": _percentile(lats, 50),
            "p95_latency_ms": _percentile(lats, 95),
            "max_latency_ms": max(lats) if lats else 0.0,
            "last_error": self._last_error,
            "config": self._config.to_dict(),
            "strict": self._strict,
            "uptime_seconds": time.monotonic() - self._started_at,
        }

    def reset(
        self,
        *,
        clear_writers: bool = False,
        clear_quarantine: bool = True,
        clear_audit: bool = True,
    ) -> int:
        """Reset counters (and optionally writers / queues).

        Returns the total number of items cleared.
        """
        with self._lock:
            cleared = 0
            if clear_writers:
                cleared += len(self._writers)
                self._writers.clear()
                self._scopes.clear()
            if clear_quarantine:
                cleared += len(self._quarantine)
                self._quarantine.clear()
            if clear_audit:
                cleared += len(self._audit)
                self._audit.clear()
            self._quarantine_evictions = 0
            self._audit_evictions = 0
            self._rejections_quarantined = 0
            self._last_error = None
            self._latency_ring.clear()
            self._started_at = time.monotonic()
        return cleared

    # ---------------------------------------------------------------- repr
    def __repr__(self) -> str:
        with self._lock:
            return (
                "WriteGovernor("
                f"writers={len(self._writers)}, "
                f"audit={len(self._audit)}, "
                f"quarantine={len(self._quarantine)}, "
                f"strict={self._strict})"
            )


__all__ = [
    "ApprovalDecision",
    "DEFAULT_CONTAINER_TAG",
    "SCHEMA_VERSION",
    "WriteGovernor",
    "WriteGovernorConfig",
    "WriteGovernorError",
    "WriteGovernorInputError",
    "WriteGovernorConfigError",
    "WriteGovernorParseError",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.write_governor
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    from .memory_schemas import DecisionRecord, PolicyRecord

    gov = WriteGovernor()
    print("repr       :", gov)

    # Register two writers with different capabilities.
    gov.register_writer("orchestrator", capabilities={"write:decision"})
    gov.register_writer(
        "ops",
        capabilities={"write:policy", "write:incident"},
        scope="operations",
    )

    d = DecisionRecord(
        run_id="run-001", workload_type="vision_inference",
        device_class="arm_edge", route="edge_int8",
        policy_version="v0.3", truth_level="measured",
        predicted_energy_wh=8.1, predicted_carbon_gco2e=3.7,
        predicted_latency_ms=410.0, reason="SLA met.",
    )

    # Approved write.
    assert gov.promote(d, writer_id="orchestrator") is True
    print("approved   : orchestrator -> decision")

    # Rejected write (unknown writer).
    assert gov.promote(d, writer_id="ghost") is False
    print("rejected   : ghost")

    # Rejected write (writer lacks capability).
    assert gov.promote(d, writer_id="ops") is False
    print("rejected   : ops (no decision capability)")

    print("quarantine :", gov.quarantine_size)
    print("statistics :", {
        k: v for k, v in gov.statistics().items()
        if k not in ("config", "uptime_seconds")
    })

    # ----------------------------------------------------------- item #3 (cross-module fix)
    # ``PolicyMemory.publish(kind="carbon")`` MUST be approved by the
    # default governor when the writer has ``write:policy``.
    policy = PolicyRecord(
        version="v0.3", content={"max_carbon_gco2e": 200.0},
        approved_by="ops",
    )
    for k in ("carbon", "helium", "safety", "policy", "meta_policy"):
        assert gov.approve("ops", policy, kind=k).approved is True, k
        print(f"policy kind {k!r:14} : approved")
    # And with the run/outcome variants.
    assert gov.approve("ops", policy, kind="run").approved is False  # no write:outcome
    print("policy 'run' (no cap): rejected as expected")

    # ----------------------------------------------------------- item #4 — SCHEMA_VERSION
    assert gov.statistics()["schema_version"] == SCHEMA_VERSION
    assert gov.to_dict()["schema_version"] == SCHEMA_VERSION

    # ----------------------------------------------------------- item #5 — register clobber
    try:
        gov.register_writer("ops", capabilities={"write:policy"})
    except WriteGovernorInputError as exc:
        print("no clobber: OK ->", exc)
    else:
        raise AssertionError("expected clobber rejection")
    # overwrite=True is accepted.
    gov.register_writer(
        "ops", capabilities={"write:policy"}, overwrite=True,
    )
    assert gov.capabilities("ops") == frozenset({"write:policy"})
    gov.register_writer(
        "ops",
        capabilities={"write:policy", "write:incident"},
        scope="operations",
        overwrite=True,
    )
    print("overwrite  : OK")

    # ----------------------------------------------------------- item #6 — _infer_kind
    # Uses the enhanced `.kind` property when available.
    assert gov._infer_kind(d) == "decision_outcome"
    assert gov._infer_kind(policy) == "policy"
    print("infer kind : OK")

    # ----------------------------------------------------------- item #7 — _safe()
    class _Secretive:
        secret = "supersecret"
        def __repr__(self):
            return f"<Secretive secret={self.secret}>"

    entry = gov._safe(_Secretive())
    assert "supersecret" not in json.dumps(entry)
    print("safe repr  : OK ->", entry)

    # ----------------------------------------------------------- item #8 — quarantine policy
    quiet_gov = WriteGovernor(
        config=WriteGovernorConfig(quarantine_on_rejection=False),
    )
    quiet_gov.promote(d, writer_id="ghost")
    assert quiet_gov.quarantine_size == 0
    print("no quarantine : OK")

    # ----------------------------------------------------------- item #9 — input validation
    for bad in (
        lambda: gov.register_writer(""),
        lambda: gov.register_writer("w", scope=""),
        lambda: gov.unregister_writer(""),
        lambda: gov.capabilities(""),
        lambda: gov.writer_scope(""),
        lambda: gov.has_capability("", "x"),
        lambda: gov.has_capability("w", ""),
        lambda: gov.approve("", d),
        lambda: gov.approve("w", None),
        lambda: gov.promote(d, writer_id=""),
        lambda: gov.quarantine(d, reason=""),
    ):
        try:
            bad()
        except WriteGovernorError as exc:
            print("Rejected   :", exc)

    # ----------------------------------------------------------- item #10 — hashable config
    cfg = WriteGovernorConfig()
    hash(cfg)
    print("cfg hash   : OK")

    # ----------------------------------------------------------- item #11 — full to_dict
    full = gov.to_dict(include_audit=True, include_quarantine=True)
    assert full["schema_version"] == SCHEMA_VERSION
    assert "config" in full and "writers" in full
    assert "audit" in full and "quarantine" in full
    print("to_dict    : OK")

    # ----------------------------------------------------------- item #12 — from_dict / reset
    rt = WriteGovernor.from_dict(full, restore_writers=True)
    assert "ops" in rt.list_writers()
    assert rt.statistics()["approvals"] >= 1
    print("from_dict  : OK")

    cleared = gov.reset()
    assert cleared >= 1
    assert gov.audit_size == 0
    print("reset      : OK ->", cleared, "cleared")

    # ----------------------------------------------------------- new helpers
    gov2 = WriteGovernor()
    gov2.register_writer("a", capabilities={"write:decision"})
    gov2.register_writer("b", capabilities={"write:policy"}, scope="ops")
    gov2.approve("a", d)
    gov2.approve("b", policy, kind="carbon")
    tail = gov2.audit_tail(1)
    assert len(tail) == 1
    assert gov2.audit_filter(writer_id="b")[0].writer_id == "b"
    assert gov2.has_capability("a", "write:decision")
    assert not gov2.has_capability("b", "write:decision")
    assert gov2.writer_scope("b") == "ops"
    snap = gov2.snapshot()
    assert snap["schema_version"] == SCHEMA_VERSION
    assert "a" in snap["writers"]
    assert gov2.clear_audit() >= 1
    print("helpers    : OK")

    # ----------------------------------------------------------- assert_compatible
    WriteGovernor.assert_compatible({"schema_version": SCHEMA_VERSION})
    try:
        WriteGovernor.assert_compatible(
            {"schema_version": SCHEMA_VERSION + 1},
        )
    except WriteGovernorParseError:
        print("assert_compat: OK")

    # ----------------------------------------------------------- from_config / from_pipeline
    gc = WriteGovernor.from_config(WriteGovernorConfig())
    assert isinstance(gc, WriteGovernor)

    class _FakePipeline:
        governor = gov2

    gp = WriteGovernor.from_pipeline(_FakePipeline())
    assert gp is gov2
    print("from_*     : OK")

    # ----------------------------------------------------------- async context manager + promote_async
    async def _async_path():
        async with WriteGovernor() as a:
            await a.register_writer_async(
                "ci", capabilities={"write:decision"},
            )
            return await a.promote_async(d, writer_id="ci")

    assert asyncio.run(_async_path()) is True
    print("async path : OK")

    # ----------------------------------------------------------- ApprovalDecision bridges
    dec = gov2.approve("a", d)
    assert dec.id.startswith("approval:")
    ep = dec.to_episode_payload(truth_level="measured")
    assert ep["metadata"]["kind"] == "write_approval"
    assert ep["metadata"]["schema_version"] == SCHEMA_VERSION
    assert "supersecret" not in json.dumps(ep)
    md = dec.to_memory_dict()
    assert {"id", "content", "metadata"} <= set(md)
    rt_dec = ApprovalDecision.from_dict(dec.to_dict())
    assert rt_dec == dec
    hash(dec)
    print("decision RT: OK")

    # ----------------------------------------------------------- config RT / merge / with_overrides
    cfg = WriteGovernorConfig()
    assert WriteGovernorConfig.from_dict(cfg.to_dict()) == cfg
    assert WriteGovernorConfig.from_json(cfg.to_json()) == cfg
    cfg2 = cfg.with_overrides(quarantine_capacity=42)
    assert cfg2.quarantine_capacity == 42 and cfg.quarantine_capacity == 1_000
    print("cfg RT     : OK")

    # ----------------------------------------------------------- config validation
    for bad_cfg in (
        dict(quarantine_capacity=0),
        dict(quarantine_capacity=True),
        dict(audit_history_capacity=-1),
        dict(schema_version=0),
        dict(quarantine_on_rejection="yes"),
        dict(required_capability={"": "write:x"}),
        dict(required_capability={"x": ""}),
    ):
        try:
            WriteGovernorConfig(**bad_cfg)  # type: ignore[arg-type]
        except WriteGovernorConfigError as exc:
            print(f"reject cfg : {list(bad_cfg)[0]} -> {exc}")

    print("\nSmoke test passed.")
