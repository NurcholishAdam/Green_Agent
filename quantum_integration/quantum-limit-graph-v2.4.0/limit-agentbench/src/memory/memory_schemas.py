# src/memory/memory_schemas.py

"""
Memory Schemas
==============

Canonical event schemas persisted to Supermemory. These records are the
stable contract for the whole memory layer; the graph UI and MCP tools
consume them directly.

Schemas
-------
- ``TruthLevel`` — one of ``measured`` / ``estimated`` / ``simulated`` /
  ``user-reported``.
- ``SeverityLevel`` — one of ``low`` / ``medium`` / ``high`` / ``critical``.
- ``RecordType`` — one of the stable type identifiers used in payload
  metadata.
- ``DecisionRecord`` — one routing decision.
- ``PolicyRecord`` — one versioned policy.
- ``IncidentRecord`` — one failed or unsafe event.
- ``OutcomeRecord`` — predicted vs measured outcome of a run.

Enhancements
------------
- Frozen dataclasses with ``__post_init__`` validation.
- Structured error hierarchy:
  ``MemorySchemaError`` → ``MemorySchemaInputError``,
  ``MemorySchemaConfigError``, ``MemorySchemaParseError``.
- ``RecordType`` and ``SeverityLevel`` str-enums.
- ``TruthLevel`` str-enum with a class-level description mapping.
- ``TYPE_NAME``, ``TTL_KIND`` and ``KIND`` class constants aligned with
  ``SupermemoryConfig.ttl_seconds`` so TTLs apply transparently.
- ``id`` and ``kind`` properties on every record for uniform access.
- ``schema_version`` field for forward-compatible migrations.
- ``MappingProxyType``-frozen mappings (``content``, ``predicted``,
  ``measured``); every record is hashable.
- Universal ``metadata`` in ``to_supermemory_payload()`` — every payload
  carries ``kind``, ``type``, ``truth_level``, ``container_tag`` and
  ``observed_at`` so ``EpisodicMemory`` pruning, ``BoundedRecall``
  filtering, ``FeedbackLoopGuard`` staleness checks and
  ``MemoryBenchmark`` extractors all apply uniformly.
- ``to_memory_dict()`` returns ``{"id", "content", "metadata"}`` shaped
  for ``BoundedRecall`` / ``RecallBundle``.
- ``to_episode_payload()`` mirrors the persistence bridge on
  ``GuardVerdict`` / ``BenchmarkReport``.
- ``DecisionRecord.with_superseded_by()`` validates the new id.
- Naive datetimes are assumed UTC (never local).
- ISO parsing tolerates ``"Z"`` and numeric epochs; parse failures raise
  ``MemorySchemaParseError``.
- Rejects ``bool`` for numeric fields; ``NaN``/``inf`` rejected.
- Rejects strings and non-iterables for ``candidates``; no
  character-splitting; duplicates removed preserving order.
- ``__version__`` exported via ``__all__``.
- ``__main__`` smoke test covers every record, every round-trip, and
  every validation path.
"""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Iterable, Mapping as ABCMapping
from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "6.0.0"

#: Version of the schema contract itself. Bump when a field is added or
#: its meaning changes.
SCHEMA_VERSION: int = 1

#: Module-wide default container tag.
DEFAULT_CONTAINER_TAG: str = "org:green-agent"

#: Module-wide truth-level vocabulary.
DEFAULT_TRUTH_LEVELS: Tuple[str, ...] = (
    "measured", "estimated", "simulated", "user-reported",
)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MemorySchemaError(ValueError):
    """Base class for memory-record problems."""


class MemorySchemaInputError(MemorySchemaError):
    """Invalid input to a record constructor."""


class MemorySchemaConfigError(MemorySchemaError):
    """Invalid enum value or class-level constant."""


class MemorySchemaParseError(MemorySchemaError):
    """Failed to parse a record from dict/JSON input."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class TruthLevel(str, Enum):
    """Provenance classification for a memory record."""

    MEASURED = "measured"
    ESTIMATED = "estimated"
    SIMULATED = "simulated"
    USER_REPORTED = "user-reported"

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.value

    @classmethod
    def values(cls) -> Tuple[str, ...]:
        return tuple(member.value for member in cls)

    @classmethod
    def coerce(cls, value: Any) -> "TruthLevel":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            try:
                return cls(value)
            except ValueError as exc:
                raise MemorySchemaInputError(
                    f"truth_level {value!r} is not one of "
                    f"{list(cls.values())}."
                ) from exc
        raise MemorySchemaInputError(
            f"truth_level must be a str or TruthLevel, got "
            f"{type(value).__name__}."
        )


_TRUTH_DESCRIPTIONS: Mapping[str, str] = MappingProxyType({
    TruthLevel.MEASURED.value:
        "Directly observed from instrumentation",
    TruthLevel.ESTIMATED.value:
        "Derived from a model or heuristic",
    TruthLevel.SIMULATED.value:
        "Produced by a simulation",
    TruthLevel.USER_REPORTED.value:
        "Provided by a human operator",
})


def _truth_description(level: str) -> str:
    return _TRUTH_DESCRIPTIONS.get(level, "Unknown provenance")


class SeverityLevel(str, Enum):
    """Severity classification for incidents."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.value

    @classmethod
    def values(cls) -> Tuple[str, ...]:
        return tuple(member.value for member in cls)

    @classmethod
    def coerce(cls, value: Any) -> "SeverityLevel":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            try:
                return cls(value)
            except ValueError as exc:
                raise MemorySchemaInputError(
                    f"severity {value!r} is not one of "
                    f"{list(cls.values())}."
                ) from exc
        raise MemorySchemaInputError(
            f"severity must be a str or SeverityLevel, got "
            f"{type(value).__name__}."
        )


class RecordType(str, Enum):
    """Stable identifiers used in payload metadata."""

    DECISION = "decision_outcome"
    POLICY = "policy"
    INCIDENT = "incident"
    OUTCOME = "outcome"

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.value


# --------------------------------------------------------------------------- #
# Shared validation helpers (mirror the other enhanced modules)
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


def _coerce_float(name: str, value: Any) -> float:
    """Coerce to finite float; reject ``bool`` and non-numerics."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MemorySchemaInputError(
            f"{name} must be numeric, got {type(value).__name__}."
        )
    fv = float(value)
    if not math.isfinite(fv):
        raise MemorySchemaInputError(
            f"{name} must be finite, got {value!r}."
        )
    return fv


def _coerce_nonneg_float(name: str, value: Any) -> float:
    fv = _coerce_float(name, value)
    if fv < 0:
        raise MemorySchemaInputError(f"{name} must be >= 0.")
    return fv


def _coerce_nonempty_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise MemorySchemaInputError(
            f"{name} must be a non-empty string."
        )
    return value


def _coerce_optional_str(name: str, value: Any) -> Optional[str]:
    if value is None:
        return None
    return _coerce_nonempty_str(name, value)


def _coerce_datetime(name: str, value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise MemorySchemaInputError(f"{name} must be a datetime.")
    return value


def _coerce_schema_version(value: Any) -> int:
    if not _is_real_int(value) or value <= 0:
        raise MemorySchemaInputError(
            f"schema_version must be a positive int (got {value!r})."
        )
    return value


def _iso(dt: datetime) -> str:
    """Return an ISO 8601 timestamp with a UTC offset.

    Naive datetimes are interpreted as UTC — *not* as local time — so
    serialization is deterministic across machines.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _parse_iso(name: str, value: Any) -> datetime:
    """Parse an ISO 8601 timestamp; tolerate ``Z`` and numeric epochs."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        ts = float(value)
        if ts > 1e12:
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OverflowError, OSError, ValueError) as exc:
            raise MemorySchemaParseError(
                f"{name} is not a valid epoch: {value!r}."
            ) from exc
    if isinstance(value, str) and value.strip():
        s = value.strip()
        if s.endswith("Z") or s.endswith("z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except ValueError as exc:
            raise MemorySchemaParseError(
                f"{name} is not a valid ISO 8601 timestamp: {value!r}."
            ) from exc
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    raise MemorySchemaParseError(
        f"{name} must be a datetime, ISO string or epoch, got "
        f"{type(value).__name__}."
    )


def _coerce_candidates(value: Any) -> Tuple[str, ...]:
    """Normalize a candidate sequence; reject strings; dedupe in order."""
    if value is None:
        return ()
    if isinstance(value, str) or not isinstance(
        value, (list, tuple, set, frozenset)
    ):
        raise MemorySchemaInputError(
            "candidates must be a sequence of strings (not a bare string)."
        )
    seen: set = set()
    out: List[str] = []
    for item in value:
        if not isinstance(item, str) or not item:
            raise MemorySchemaInputError(
                "candidates entries must be non-empty strings."
            )
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return tuple(out)


def _freeze_str_float_map(
    name: str, value: Any, *, non_negative: bool = False,
) -> Mapping[str, float]:
    """Validate + freeze a mapping of finite floats."""
    if not isinstance(value, ABCMapping):
        raise MemorySchemaInputError(f"{name} must be a Mapping.")
    out: Dict[str, float] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k:
            raise MemorySchemaInputError(
                f"{name} keys must be non-empty strings."
            )
        fv = _coerce_float(f"{name}[{k!r}]", v)
        if non_negative and fv < 0:
            raise MemorySchemaInputError(
                f"{name}[{k!r}] must be >= 0."
            )
        out[k] = fv
    return MappingProxyType(out)


def _freeze_json_map(name: str, value: Any) -> Mapping[str, Any]:
    """Validate + freeze a JSON-serializable mapping."""
    if not isinstance(value, ABCMapping):
        raise MemorySchemaInputError(f"{name} must be a Mapping.")
    out: Dict[str, Any] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not k:
            raise MemorySchemaInputError(
                f"{name} keys must be non-empty strings."
            )
        out[k] = v
    # Confirm serializability up front; MappingProxyType is not JSON-aware,
    # so we test the raw contents.
    try:
        json.dumps(out)
    except (TypeError, ValueError) as exc:
        raise MemorySchemaInputError(
            f"{name} must be JSON-serializable: {exc}"
        ) from exc
    return MappingProxyType(out)


# --------------------------------------------------------------------------- #
# Decision record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class DecisionRecord:
    """One routing decision with evidence provenance."""

    #: Human-readable identifier stored in payload ``metadata["type"]``.
    TYPE_NAME: ClassVar[str] = RecordType.DECISION.value

    #: TTL key (see ``SupermemoryConfig.ttl_seconds``).
    TTL_KIND: ClassVar[str] = "decision_outcome"

    #: Default container tag.
    DEFAULT_CONTAINER_TAG: ClassVar[str] = DEFAULT_CONTAINER_TAG

    run_id: str
    workload_type: str
    device_class: str
    route: str
    policy_version: str
    truth_level: str
    predicted_energy_wh: float
    predicted_carbon_gco2e: float
    predicted_latency_ms: float
    reason: str
    candidates: Tuple[str, ...] = ()
    site_id: Optional[str] = None
    quality_score: Optional[float] = None
    container_tag: str = DEFAULT_CONTAINER_TAG
    observed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )
    metrics_hash: Optional[str] = None
    superseded_by: Optional[str] = None
    schema_version: int = SCHEMA_VERSION

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        for name in (
            "run_id", "workload_type", "device_class", "route",
            "policy_version", "reason",
        ):
            object.__setattr__(
                self, name,
                _coerce_nonempty_str(name, getattr(self, name)),
            )
        object.__setattr__(
            self, "container_tag",
            _coerce_nonempty_str("container_tag", self.container_tag),
        )

        # Truth level
        object.__setattr__(
            self, "truth_level",
            TruthLevel.coerce(self.truth_level).value,
        )

        # Numeric predictions
        for name in (
            "predicted_energy_wh", "predicted_carbon_gco2e",
            "predicted_latency_ms",
        ):
            object.__setattr__(
                self, name,
                _coerce_nonneg_float(name, getattr(self, name)),
            )

        # Candidates
        object.__setattr__(
            self, "candidates", _coerce_candidates(self.candidates),
        )

        # Optional strings
        object.__setattr__(
            self, "site_id", _coerce_optional_str("site_id", self.site_id),
        )
        object.__setattr__(
            self, "metrics_hash",
            _coerce_optional_str("metrics_hash", self.metrics_hash),
        )
        object.__setattr__(
            self, "superseded_by",
            _coerce_optional_str("superseded_by", self.superseded_by),
        )

        # Quality score
        if self.quality_score is not None:
            q = _coerce_float("quality_score", self.quality_score)
            if not 0.0 <= q <= 1.0:
                raise MemorySchemaInputError(
                    "quality_score must be in [0, 1]."
                )
            object.__setattr__(self, "quality_score", q)

        object.__setattr__(
            self, "observed_at",
            _coerce_datetime("observed_at", self.observed_at),
        )
        object.__setattr__(
            self, "schema_version",
            _coerce_schema_version(self.schema_version),
        )

    # ------------------------------------------------------------------ #
    # Accessors
    # ------------------------------------------------------------------ #
    @property
    def id(self) -> str:
        """Stable record identifier (the run id)."""
        return self.run_id

    @property
    def kind(self) -> str:
        """TTL key used by ``EpisodicMemory`` pruning."""
        return self.TTL_KIND

    @property
    def type_name(self) -> str:
        return self.TYPE_NAME

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "workload_type": self.workload_type,
            "device_class": self.device_class,
            "route": self.route,
            "policy_version": self.policy_version,
            "truth_level": self.truth_level,
            "predicted_energy_wh": self.predicted_energy_wh,
            "predicted_carbon_gco2e": self.predicted_carbon_gco2e,
            "predicted_latency_ms": self.predicted_latency_ms,
            "reason": self.reason,
            "candidates": list(self.candidates),
            "site_id": self.site_id,
            "quality_score": self.quality_score,
            "container_tag": self.container_tag,
            "observed_at": _iso(self.observed_at),
            "metrics_hash": self.metrics_hash,
            "superseded_by": self.superseded_by,
            "schema_version": self.schema_version,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DecisionRecord":
        if not isinstance(data, ABCMapping):
            raise MemorySchemaParseError(
                "DecisionRecord.from_dict expects a Mapping."
            )
        try:
            return cls(
                run_id=data["run_id"],
                workload_type=data["workload_type"],
                device_class=data["device_class"],
                route=data["route"],
                policy_version=data["policy_version"],
                truth_level=data["truth_level"],
                predicted_energy_wh=data["predicted_energy_wh"],
                predicted_carbon_gco2e=data["predicted_carbon_gco2e"],
                predicted_latency_ms=data["predicted_latency_ms"],
                reason=data.get("reason", ""),
                candidates=tuple(data.get("candidates", ())),
                site_id=data.get("site_id"),
                quality_score=data.get("quality_score"),
                container_tag=data.get(
                    "container_tag", DEFAULT_CONTAINER_TAG,
                ),
                observed_at=_parse_iso(
                    "observed_at", data.get("observed_at")
                    or datetime.now(timezone.utc),
                ),
                metrics_hash=data.get("metrics_hash"),
                superseded_by=data.get("superseded_by"),
                schema_version=data.get("schema_version", SCHEMA_VERSION),
            )
        except KeyError as exc:
            raise MemorySchemaParseError(
                f"DecisionRecord.from_dict missing key {exc.args[0]!r}."
            ) from exc

    @classmethod
    def from_json(cls, payload: str) -> "DecisionRecord":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemorySchemaParseError(
                f"DecisionRecord.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    # Payloads
    # ------------------------------------------------------------------ #
    def _metadata(self) -> Dict[str, Any]:
        return {
            "type": self.TYPE_NAME,
            "kind": self.TTL_KIND,
            "record_id": self.id,
            "run_id": self.run_id,
            "truth_level": self.truth_level,
            "container_tag": self.container_tag,
            "observed_at": _iso(self.observed_at),
            "schema_version": self.schema_version,
            "workload_type": self.workload_type,
            "device_class": self.device_class,
            "route": self.route,
            "policy_version": self.policy_version,
            "quality_score": self.quality_score,
            "metrics_hash": self.metrics_hash,
            "site_id": self.site_id,
            "superseded_by": self.superseded_by,
            "energy_wh": self.predicted_energy_wh,
            "carbon_gco2e": self.predicted_carbon_gco2e,
            "latency_ms": self.predicted_latency_ms,
        }

    def _render_content(self) -> str:
        quality = (
            f"Quality: {self.quality_score:.3f}"
            if self.quality_score is not None else "Quality: n/a"
        )
        return (
            f"Green Agent decision outcome:\n"
            f"Run: {self.run_id}\n"
            f"Workload: {self.workload_type}, "
            f"device_class={self.device_class}\n"
            f"Candidates: {', '.join(self.candidates) or 'n/a'}\n"
            f"Chosen: {self.route}\n"
            f"Predicted: {self.predicted_energy_wh:.4g} Wh, "
            f"{self.predicted_carbon_gco2e:.4g} gCO2e, "
            f"{self.predicted_latency_ms:.1f} ms\n"
            f"{quality}; truth_level={self.truth_level}; "
            f"policy={self.policy_version}\n"
            f"Reason: {self.reason}"
        )

    def to_supermemory_payload(self) -> Dict[str, Any]:
        """Return the ``{"content", "container_tag", "metadata"}`` triple."""
        return {
            "content": self._render_content(),
            "container_tag": self.container_tag,
            "metadata": self._metadata(),
        }

    def to_memory_dict(self) -> Dict[str, Any]:
        """Return ``{"id", "content", "metadata"}`` for ``BoundedRecall``."""
        return {
            "id": self.id,
            "content": self._render_content(),
            "metadata": self._metadata(),
        }

    def to_episode_payload(
        self,
        *,
        container_tag: Optional[str] = None,
        content: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Persistence bridge for ``EpisodicMemory`` / ``SupermemoryAdapter``."""
        tag = container_tag if container_tag is not None else self.container_tag
        _coerce_nonempty_str("container_tag", tag)
        return {
            "content": content if content is not None else self._render_content(),
            "container_tag": tag,
            "metadata": self._metadata(),
        }

    # ------------------------------------------------------------------ #
    def with_superseded_by(self, new_run_id: str) -> "DecisionRecord":
        """Return a copy marked as superseded by ``new_run_id``."""
        _coerce_nonempty_str("new_run_id", new_run_id)
        return DecisionRecord(
            run_id=self.run_id,
            workload_type=self.workload_type,
            device_class=self.device_class,
            route=self.route,
            policy_version=self.policy_version,
            truth_level=self.truth_level,
            predicted_energy_wh=self.predicted_energy_wh,
            predicted_carbon_gco2e=self.predicted_carbon_gco2e,
            predicted_latency_ms=self.predicted_latency_ms,
            reason=self.reason,
            candidates=self.candidates,
            site_id=self.site_id,
            quality_score=self.quality_score,
            container_tag=self.container_tag,
            observed_at=self.observed_at,
            metrics_hash=self.metrics_hash,
            superseded_by=new_run_id,
            schema_version=self.schema_version,
        )

    # ------------------------------------------------------------------ #
    def __hash__(self) -> int:
        return hash((
            self.run_id, self.workload_type, self.device_class,
            self.route, self.policy_version, self.truth_level,
            self.predicted_energy_wh, self.predicted_carbon_gco2e,
            self.predicted_latency_ms, self.reason, self.candidates,
            self.site_id, self.quality_score, self.container_tag,
            self.observed_at, self.metrics_hash, self.superseded_by,
            self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            f"DecisionRecord(run_id={self.run_id!r}, "
            f"route={self.route!r}, "
            f"truth_level={self.truth_level!r})"
        )


# --------------------------------------------------------------------------- #
# Policy record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PolicyRecord:
    """A versioned policy snapshot."""

    TYPE_NAME: ClassVar[str] = RecordType.POLICY.value
    TTL_KIND: ClassVar[str] = "policy"
    DEFAULT_CONTAINER_TAG: ClassVar[str] = "policy:current"
    DEFAULT_TRUTH_LEVEL: ClassVar[str] = TruthLevel.USER_REPORTED.value

    version: str
    content: Mapping[str, Any]
    policy_id: str = "default"
    approved_by: str = "system"
    container_tag: str = "policy:current"
    truth_level: str = TruthLevel.USER_REPORTED.value
    published_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )
    superseded_by: Optional[str] = None
    supersedes: Optional[str] = None
    schema_version: int = SCHEMA_VERSION

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        for name in ("version", "policy_id", "approved_by", "container_tag"):
            object.__setattr__(
                self, name,
                _coerce_nonempty_str(name, getattr(self, name)),
            )
        object.__setattr__(
            self, "truth_level",
            TruthLevel.coerce(self.truth_level).value,
        )
        object.__setattr__(
            self, "content", _freeze_json_map("content", self.content),
        )
        object.__setattr__(
            self, "published_at",
            _coerce_datetime("published_at", self.published_at),
        )
        object.__setattr__(
            self, "superseded_by",
            _coerce_optional_str("superseded_by", self.superseded_by),
        )
        object.__setattr__(
            self, "supersedes",
            _coerce_optional_str("supersedes", self.supersedes),
        )
        object.__setattr__(
            self, "schema_version",
            _coerce_schema_version(self.schema_version),
        )

    # ------------------------------------------------------------------ #
    # Accessors
    # ------------------------------------------------------------------ #
    @property
    def id(self) -> str:
        return f"{self.policy_id}:{self.version}"

    @property
    def kind(self) -> str:
        return self.TTL_KIND

    @property
    def type_name(self) -> str:
        return self.TYPE_NAME

    @property
    def observed_at(self) -> datetime:
        """Alias for ``published_at`` — matches the module-wide convention."""
        return self.published_at

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "content": dict(self.content),
            "policy_id": self.policy_id,
            "approved_by": self.approved_by,
            "container_tag": self.container_tag,
            "truth_level": self.truth_level,
            "published_at": _iso(self.published_at),
            "superseded_by": self.superseded_by,
            "supersedes": self.supersedes,
            "schema_version": self.schema_version,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PolicyRecord":
        if not isinstance(data, ABCMapping):
            raise MemorySchemaParseError(
                "PolicyRecord.from_dict expects a Mapping."
            )
        try:
            content = data.get("content", {})
            return cls(
                version=data["version"],
                content=content,
                policy_id=data.get("policy_id", "default"),
                approved_by=data.get("approved_by", "system"),
                container_tag=data.get(
                    "container_tag", cls.DEFAULT_CONTAINER_TAG,
                ),
                truth_level=data.get(
                    "truth_level", cls.DEFAULT_TRUTH_LEVEL,
                ),
                published_at=_parse_iso(
                    "published_at",
                    data.get("published_at")
                    or datetime.now(timezone.utc),
                ),
                superseded_by=data.get("superseded_by"),
                supersedes=data.get("supersedes"),
                schema_version=data.get("schema_version", SCHEMA_VERSION),
            )
        except KeyError as exc:
            raise MemorySchemaParseError(
                f"PolicyRecord.from_dict missing key {exc.args[0]!r}."
            ) from exc

    @classmethod
    def from_json(cls, payload: str) -> "PolicyRecord":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemorySchemaParseError(
                f"PolicyRecord.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    # Payloads
    # ------------------------------------------------------------------ #
    def _metadata(self) -> Dict[str, Any]:
        return {
            "type": self.TYPE_NAME,
            "kind": self.TTL_KIND,
            "record_id": self.id,
            "policy_id": self.policy_id,
            "version": self.version,
            "approved_by": self.approved_by,
            "truth_level": self.truth_level,
            "container_tag": self.container_tag,
            "observed_at": _iso(self.published_at),
            "published_at": _iso(self.published_at),
            "superseded_by": self.superseded_by,
            "supersedes": self.supersedes,
            "schema_version": self.schema_version,
        }

    def _render_content(self) -> str:
        return (
            f"Policy {self.policy_id} {self.version} approved by "
            f"{self.approved_by}:\n"
            f"{json.dumps(dict(self.content), indent=2, sort_keys=True)}"
        )

    def to_supermemory_payload(self) -> Dict[str, Any]:
        return {
            "content": self._render_content(),
            "container_tag": self.container_tag,
            "metadata": self._metadata(),
        }

    def to_memory_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "content": self._render_content(),
            "metadata": self._metadata(),
        }

    def to_episode_payload(
        self,
        *,
        container_tag: Optional[str] = None,
        content: Optional[str] = None,
    ) -> Dict[str, Any]:
        tag = container_tag if container_tag is not None else self.container_tag
        _coerce_nonempty_str("container_tag", tag)
        return {
            "content": (
                content if content is not None else self._render_content()
            ),
            "container_tag": tag,
            "metadata": self._metadata(),
        }

    # ------------------------------------------------------------------ #
    def with_superseded_by(self, new_id: str) -> "PolicyRecord":
        _coerce_nonempty_str("new_id", new_id)
        return PolicyRecord(
            version=self.version,
            content=self.content,
            policy_id=self.policy_id,
            approved_by=self.approved_by,
            container_tag=self.container_tag,
            truth_level=self.truth_level,
            published_at=self.published_at,
            superseded_by=new_id,
            supersedes=self.supersedes,
            schema_version=self.schema_version,
        )

    def __hash__(self) -> int:
        return hash((
            self.version,
            tuple(sorted(self.content.items())),
            self.policy_id,
            self.approved_by,
            self.container_tag,
            self.truth_level,
            self.published_at,
            self.superseded_by,
            self.supersedes,
            self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            f"PolicyRecord(policy_id={self.policy_id!r}, "
            f"version={self.version!r}, "
            f"approved_by={self.approved_by!r})"
        )


# --------------------------------------------------------------------------- #
# Incident record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class IncidentRecord:
    """One failed or unsafe event."""

    TYPE_NAME: ClassVar[str] = RecordType.INCIDENT.value
    TTL_KIND: ClassVar[str] = "incident"
    DEFAULT_CONTAINER_TAG: ClassVar[str] = "incident:default"
    DEFAULT_TRUTH_LEVEL: ClassVar[str] = TruthLevel.MEASURED.value

    incident_id: str
    severity: str
    description: str
    run_id: Optional[str] = None
    mitigation: Optional[str] = None
    container_tag: str = "incident:default"
    truth_level: str = TruthLevel.MEASURED.value
    observed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )
    schema_version: int = SCHEMA_VERSION

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        for name in ("incident_id", "description", "container_tag"):
            object.__setattr__(
                self, name,
                _coerce_nonempty_str(name, getattr(self, name)),
            )
        object.__setattr__(
            self, "severity", SeverityLevel.coerce(self.severity).value,
        )
        object.__setattr__(
            self, "truth_level",
            TruthLevel.coerce(self.truth_level).value,
        )
        object.__setattr__(
            self, "run_id", _coerce_optional_str("run_id", self.run_id),
        )
        object.__setattr__(
            self, "mitigation",
            _coerce_optional_str("mitigation", self.mitigation),
        )
        object.__setattr__(
            self, "observed_at",
            _coerce_datetime("observed_at", self.observed_at),
        )
        object.__setattr__(
            self, "schema_version",
            _coerce_schema_version(self.schema_version),
        )

    # ------------------------------------------------------------------ #
    @property
    def id(self) -> str:
        return self.incident_id

    @property
    def kind(self) -> str:
        return self.TTL_KIND

    @property
    def type_name(self) -> str:
        return self.TYPE_NAME

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "severity": self.severity,
            "description": self.description,
            "run_id": self.run_id,
            "mitigation": self.mitigation,
            "container_tag": self.container_tag,
            "truth_level": self.truth_level,
            "observed_at": _iso(self.observed_at),
            "schema_version": self.schema_version,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "IncidentRecord":
        if not isinstance(data, ABCMapping):
            raise MemorySchemaParseError(
                "IncidentRecord.from_dict expects a Mapping."
            )
        try:
            return cls(
                incident_id=data["incident_id"],
                severity=data["severity"],
                description=data["description"],
                run_id=data.get("run_id"),
                mitigation=data.get("mitigation"),
                container_tag=data.get(
                    "container_tag", cls.DEFAULT_CONTAINER_TAG,
                ),
                truth_level=data.get(
                    "truth_level", cls.DEFAULT_TRUTH_LEVEL,
                ),
                observed_at=_parse_iso(
                    "observed_at",
                    data.get("observed_at")
                    or datetime.now(timezone.utc),
                ),
                schema_version=data.get("schema_version", SCHEMA_VERSION),
            )
        except KeyError as exc:
            raise MemorySchemaParseError(
                f"IncidentRecord.from_dict missing key {exc.args[0]!r}."
            ) from exc

    @classmethod
    def from_json(cls, payload: str) -> "IncidentRecord":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemorySchemaParseError(
                f"IncidentRecord.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    def _metadata(self) -> Dict[str, Any]:
        return {
            "type": self.TYPE_NAME,
            "kind": self.TTL_KIND,
            "record_id": self.id,
            "incident_id": self.incident_id,
            "severity": self.severity,
            "run_id": self.run_id,
            "truth_level": self.truth_level,
            "container_tag": self.container_tag,
            "observed_at": _iso(self.observed_at),
            "schema_version": self.schema_version,
        }

    def _render_content(self) -> str:
        return (
            f"Incident {self.incident_id} ({self.severity}): "
            f"{self.description}\n"
            f"Run: {self.run_id or 'n/a'}\n"
            f"Mitigation: {self.mitigation or 'n/a'}"
        )

    def to_supermemory_payload(self) -> Dict[str, Any]:
        return {
            "content": self._render_content(),
            "container_tag": self.container_tag,
            "metadata": self._metadata(),
        }

    def to_memory_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "content": self._render_content(),
            "metadata": self._metadata(),
        }

    def to_episode_payload(
        self,
        *,
        container_tag: Optional[str] = None,
        content: Optional[str] = None,
    ) -> Dict[str, Any]:
        tag = container_tag if container_tag is not None else self.container_tag
        _coerce_nonempty_str("container_tag", tag)
        return {
            "content": (
                content if content is not None else self._render_content()
            ),
            "container_tag": tag,
            "metadata": self._metadata(),
        }

    def __hash__(self) -> int:
        return hash((
            self.incident_id, self.severity, self.description,
            self.run_id, self.mitigation, self.container_tag,
            self.truth_level, self.observed_at, self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            f"IncidentRecord(incident_id={self.incident_id!r}, "
            f"severity={self.severity!r})"
        )


# --------------------------------------------------------------------------- #
# Outcome record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class OutcomeRecord:
    """Predicted vs measured outcome of a run."""

    TYPE_NAME: ClassVar[str] = RecordType.OUTCOME.value
    TTL_KIND: ClassVar[str] = "run"
    DEFAULT_CONTAINER_TAG: ClassVar[str] = DEFAULT_CONTAINER_TAG
    DEFAULT_TRUTH_LEVEL: ClassVar[str] = TruthLevel.MEASURED.value

    run_id: str
    predicted: Mapping[str, float]
    measured: Mapping[str, float]
    truth_level: str = TruthLevel.MEASURED.value
    container_tag: str = DEFAULT_CONTAINER_TAG
    observed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )
    schema_version: int = SCHEMA_VERSION

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        object.__setattr__(
            self, "run_id",
            _coerce_nonempty_str("run_id", self.run_id),
        )
        object.__setattr__(
            self, "truth_level",
            TruthLevel.coerce(self.truth_level).value,
        )
        object.__setattr__(
            self, "container_tag",
            _coerce_nonempty_str("container_tag", self.container_tag),
        )
        object.__setattr__(
            self, "predicted",
            _freeze_str_float_map("predicted", self.predicted),
        )
        object.__setattr__(
            self, "measured",
            _freeze_str_float_map("measured", self.measured),
        )
        object.__setattr__(
            self, "observed_at",
            _coerce_datetime("observed_at", self.observed_at),
        )
        object.__setattr__(
            self, "schema_version",
            _coerce_schema_version(self.schema_version),
        )

    # ------------------------------------------------------------------ #
    @property
    def id(self) -> str:
        return self.run_id

    @property
    def kind(self) -> str:
        return self.TTL_KIND

    @property
    def type_name(self) -> str:
        return self.TYPE_NAME

    def delta(self) -> Dict[str, float]:
        """Return ``measured - predicted`` per shared key."""
        keys = set(self.predicted) & set(self.measured)
        return {
            k: float(self.measured[k]) - float(self.predicted[k])
            for k in keys
        }

    def missing_keys(self) -> Dict[str, Tuple[str, ...]]:
        """Return keys present in one side but not the other."""
        p = set(self.predicted)
        m = set(self.measured)
        return {
            "predicted_only": tuple(sorted(p - m)),
            "measured_only": tuple(sorted(m - p)),
        }

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "predicted": dict(self.predicted),
            "measured": dict(self.measured),
            "truth_level": self.truth_level,
            "container_tag": self.container_tag,
            "observed_at": _iso(self.observed_at),
            "schema_version": self.schema_version,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "OutcomeRecord":
        if not isinstance(data, ABCMapping):
            raise MemorySchemaParseError(
                "OutcomeRecord.from_dict expects a Mapping."
            )
        try:
            return cls(
                run_id=data["run_id"],
                predicted=dict(data.get("predicted", {})),
                measured=dict(data.get("measured", {})),
                truth_level=data.get(
                    "truth_level", cls.DEFAULT_TRUTH_LEVEL,
                ),
                container_tag=data.get(
                    "container_tag", cls.DEFAULT_CONTAINER_TAG,
                ),
                observed_at=_parse_iso(
                    "observed_at",
                    data.get("observed_at")
                    or datetime.now(timezone.utc),
                ),
                schema_version=data.get("schema_version", SCHEMA_VERSION),
            )
        except KeyError as exc:
            raise MemorySchemaParseError(
                f"OutcomeRecord.from_dict missing key {exc.args[0]!r}."
            ) from exc

    @classmethod
    def from_json(cls, payload: str) -> "OutcomeRecord":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemorySchemaParseError(
                f"OutcomeRecord.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    def _metadata(self) -> Dict[str, Any]:
        meta: Dict[str, Any] = {
            "type": self.TYPE_NAME,
            "kind": self.TTL_KIND,
            "record_id": self.id,
            "run_id": self.run_id,
            "truth_level": self.truth_level,
            "container_tag": self.container_tag,
            "observed_at": _iso(self.observed_at),
            "schema_version": self.schema_version,
        }
        # Flatten predicted/measured so BoundedRecall / FeedbackLoopGuard
        # can read the common telemetry keys directly.
        for k, v in self.measured.items():
            meta.setdefault(k, v)
        for k, v in self.predicted.items():
            meta.setdefault(f"predicted_{k}", v)
        return meta

    def _render_content(self) -> str:
        delta = self.delta()
        lines = [f"Outcome for run {self.run_id}:"]
        for k in sorted(set(self.predicted) | set(self.measured)):
            p = self.predicted.get(k)
            m = self.measured.get(k)
            d = delta.get(k)
            lines.append(
                f"  {k}: predicted={p} measured={m} delta={d}"
            )
        return "\n".join(lines)

    def to_supermemory_payload(self) -> Dict[str, Any]:
        return {
            "content": self._render_content(),
            "container_tag": self.container_tag,
            "metadata": self._metadata(),
        }

    def to_memory_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "content": self._render_content(),
            "metadata": self._metadata(),
        }

    def to_episode_payload(
        self,
        *,
        container_tag: Optional[str] = None,
        content: Optional[str] = None,
    ) -> Dict[str, Any]:
        tag = container_tag if container_tag is not None else self.container_tag
        _coerce_nonempty_str("container_tag", tag)
        return {
            "content": (
                content if content is not None else self._render_content()
            ),
            "container_tag": tag,
            "metadata": self._metadata(),
        }

    def __hash__(self) -> int:
        return hash((
            self.run_id,
            tuple(sorted(self.predicted.items())),
            tuple(sorted(self.measured.items())),
            self.truth_level,
            self.container_tag,
            self.observed_at,
            self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            f"OutcomeRecord(run_id={self.run_id!r}, "
            f"keys={sorted(set(self.predicted) | set(self.measured))})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "DecisionRecord",
    "IncidentRecord",
    "MemorySchemaError",
    "MemorySchemaInputError",
    "MemorySchemaConfigError",
    "MemorySchemaParseError",
    "OutcomeRecord",
    "PolicyRecord",
    "RecordType",
    "SCHEMA_VERSION",
    "SeverityLevel",
    "TruthLevel",
    "DEFAULT_CONTAINER_TAG",
    "DEFAULT_TRUTH_LEVELS",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.memory_schemas
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    now_iso = datetime.now(timezone.utc).isoformat()

    # --------------------------------------------------- 1. Decision
    d = DecisionRecord(
        run_id="run-001",
        workload_type="vision_inference",
        device_class="arm_edge",
        route="edge_int8",
        policy_version="v0.3",
        truth_level=TruthLevel.MEASURED.value,
        predicted_energy_wh=8.1,
        predicted_carbon_gco2e=3.7,
        predicted_latency_ms=410.0,
        reason="Met SLA with lower carbon and avoided unreliable network.",
        candidates=("edge_int8", "cloud_gpu", "defer"),
        quality_score=0.91,
    )
    print("decision id :", d.id, "| kind:", d.kind)
    assert d.id == "run-001"
    assert d.kind == "decision_outcome"
    assert d.type_name == RecordType.DECISION.value

    # Round-trip
    assert DecisionRecord.from_dict(d.to_dict()) == d
    assert DecisionRecord.from_json(d.to_json()) == d
    print("decision RT : OK")

    # Payload metadata is universal.
    sm = d.to_supermemory_payload()
    assert {
        "type", "kind", "truth_level", "container_tag", "observed_at",
        "record_id", "run_id", "schema_version",
    } <= set(sm["metadata"])
    md = d.to_memory_dict()
    assert md["id"] == d.id and md["metadata"]["kind"] == d.kind
    ep = d.to_episode_payload(container_tag="org:other")
    assert ep["container_tag"] == "org:other"
    assert ep["metadata"]["container_tag"] == d.container_tag  # unmodified
    print("decision payloads : OK")

    # Supersede validates new id.
    d2 = d.with_superseded_by("run-002")
    assert d2.superseded_by == "run-002"
    try:
        d.with_superseded_by("")
    except MemorySchemaError as exc:
        print("supersede validation : OK ->", exc)
    else:
        raise AssertionError("expected validation error")

    # --------------------------------------------------- 2. Policy
    p = PolicyRecord(
        version="v0.3",
        content={"max_carbon_gco2e": 200.0, "allowed_regions": ["eu-west-1"]},
        approved_by="ops",
    )
    print("policy id   :", p.id, "| kind:", p.kind)
    assert p.id == "default:v0.3"
    assert p.kind == "policy"
    assert p.observed_at == p.published_at
    assert PolicyRecord.from_dict(p.to_dict()) == p
    assert PolicyRecord.from_json(p.to_json()) == p

    # Frozen content.
    try:
        p.content["max_carbon_gco2e"] = 0  # type: ignore[index]
    except TypeError:
        print("policy content frozen : OK")
    hash(p)
    print("policy hashable       : OK")

    p_meta = p.to_supermemory_payload()["metadata"]
    assert p_meta["kind"] == "policy"
    assert p_meta["truth_level"] == TruthLevel.USER_REPORTED.value
    assert p_meta["container_tag"] == "policy:current"

    # --------------------------------------------------- 3. Incident
    i = IncidentRecord(
        incident_id="inc-1",
        severity="high",
        description="Cloud route exceeded carbon budget.",
        run_id="run-002",
        mitigation="Rolled back to edge INT8.",
    )
    print("incident id :", i.id, "| kind:", i.kind)
    assert i.kind == "incident"
    assert i.truth_level == TruthLevel.MEASURED.value
    assert IncidentRecord.from_dict(i.to_dict()) == i
    assert IncidentRecord.from_json(i.to_json()) == i

    # Severity as enum.
    i2 = IncidentRecord(
        incident_id="inc-2",
        severity=SeverityLevel.CRITICAL,
        description="x",
    )
    assert i2.severity == "critical"

    # --------------------------------------------------- 4. Outcome
    o = OutcomeRecord(
        run_id="run-001",
        predicted={"energy_wh": 8.1, "carbon_gco2e": 3.7, "latency_ms": 410.0},
        measured={"energy_wh": 8.6, "carbon_gco2e": 4.0, "latency_ms": 438.0},
    )
    print("outcome id  :", o.id, "| kind:", o.kind)
    assert o.id == "run-001"
    assert o.kind == "run"
    assert OutcomeRecord.from_dict(o.to_dict()) == o
    assert OutcomeRecord.from_json(o.to_json()) == o

    # delta
    delta = o.delta()
    assert abs(delta["energy_wh"] - 0.5) < 1e-9
    assert o.missing_keys() == {"predicted_only": (), "measured_only": ()}

    # Frozen maps.
    try:
        o.predicted["energy_wh"] = 0.0  # type: ignore[index]
    except TypeError:
        print("outcome maps frozen : OK")
    hash(o)

    # Flattened metadata for recall/guard.
    o_meta = o.to_supermemory_payload()["metadata"]
    assert o_meta["energy_wh"] == 8.6
    assert o_meta["predicted_energy_wh"] == 8.1

    # --------------------------------------------------- 5. Validation paths
    bad_decisions = [
        dict(run_id="", workload_type="x", device_class="x", route="x",
             policy_version="x", truth_level="measured",
             predicted_energy_wh=1.0, predicted_carbon_gco2e=1.0,
             predicted_latency_ms=1.0, reason="x"),
        dict(run_id="x", workload_type="x", device_class="x", route="x",
             policy_version="x", truth_level="bogus",
             predicted_energy_wh=1.0, predicted_carbon_gco2e=1.0,
             predicted_latency_ms=1.0, reason="x"),
        dict(run_id="x", workload_type="x", device_class="x", route="x",
             policy_version="x", truth_level="measured",
             predicted_energy_wh=float("nan"), predicted_carbon_gco2e=1.0,
             predicted_latency_ms=1.0, reason="x"),
        dict(run_id="x", workload_type="x", device_class="x", route="x",
             policy_version="x", truth_level="measured",
             predicted_energy_wh=-1.0, predicted_carbon_gco2e=1.0,
             predicted_latency_ms=1.0, reason="x"),
        dict(run_id="x", workload_type="x", device_class="x", route="x",
             policy_version="x", truth_level="measured",
             predicted_energy_wh=1.0, predicted_carbon_gco2e=1.0,
             predicted_latency_ms=1.0, reason="x", candidates="not-a-tuple"),
        dict(run_id="x", workload_type="x", device_class="x", route="x",
             policy_version="x", truth_level="measured",
             predicted_energy_wh=1.0, predicted_carbon_gco2e=1.0,
             predicted_latency_ms=1.0, reason="x", candidates=None),
        dict(run_id="x", workload_type="x", device_class="x", route="x",
             policy_version="x", truth_level="measured",
             predicted_energy_wh=1.0, predicted_carbon_gco2e=1.0,
             predicted_latency_ms=1.0, reason="x", candidates=(1, 2)),
        dict(run_id="x", workload_type="x", device_class="x", route="x",
             policy_version="x", truth_level="measured",
             predicted_energy_wh=1.0, predicted_carbon_gco2e=1.0,
             predicted_latency_ms=1.0, reason="x", quality_score=2.0),
        dict(run_id="x", workload_type="x", device_class="x", route="x",
             policy_version="x", truth_level="measured",
             predicted_energy_wh=1.0, predicted_carbon_gco2e=1.0,
             predicted_latency_ms=1.0, reason="x", quality_score=True),
    ]
    for bad in bad_decisions:
        try:
            DecisionRecord(**bad)  # type: ignore[arg-type]
        except MemorySchemaError as exc:
            print(f"reject decision : {exc}")
        else:
            raise AssertionError(f"expected rejection for {bad!r}")

    # Candidates dedupe + tuple normalization.
    dd = DecisionRecord(
        run_id="x", workload_type="x", device_class="x", route="x",
        policy_version="x", truth_level="measured",
        predicted_energy_wh=1.0, predicted_carbon_gco2e=1.0,
        predicted_latency_ms=1.0, reason="x",
        candidates=["a", "a", "b", "a", "b"],
    )
    assert dd.candidates == ("a", "b")
    print("candidates dedupe : OK")

    # Policy validation.
    for bad in (
        dict(version="", content={}),
        dict(version="v1", content="not a mapping"),
        dict(version="v1", content={"x": object()}),  # not JSON-serializable
    ):
        try:
            PolicyRecord(**bad)  # type: ignore[arg-type]
        except MemorySchemaError as exc:
            print(f"reject policy   : {exc}")

    # Incident validation.
    for bad in (
        dict(incident_id="x", severity="bogus", description="x"),
        dict(incident_id="", severity="low", description="x"),
    ):
        try:
            IncidentRecord(**bad)  # type: ignore[arg-type]
        except MemorySchemaError as exc:
            print(f"reject incident : {exc}")

    # Outcome validation.
    for bad in (
        dict(run_id="x", predicted={"a": "b"}, measured={"a": 1.0}),
        dict(run_id="x", predicted={"a": True}, measured={"a": 1.0}),
        dict(run_id="x", predicted="not a mapping", measured={}),
    ):
        try:
            OutcomeRecord(**bad)  # type: ignore[arg-type]
        except MemorySchemaError as exc:
            print(f"reject outcome  : {exc}")

    # --------------------------------------------------- 6. Parse errors
    try:
        DecisionRecord.from_json("{not valid")
    except MemorySchemaParseError as exc:
        print("parse error  : OK ->", exc)

    try:
        DecisionRecord.from_dict({"run_id": "x"})  # missing keys
    except MemorySchemaParseError as exc:
        print("missing key  : OK ->", exc)

    # --------------------------------------------------- 7. ISO parsing
    z = DecisionRecord(
        run_id="z", workload_type="w", device_class="d", route="r",
        policy_version="v", truth_level="measured",
        predicted_energy_wh=1.0, predicted_carbon_gco2e=1.0,
        predicted_latency_ms=1.0, reason="x",
        observed_at="2024-01-01T00:00:00Z",  # type: ignore[arg-type]
    )
    assert z.observed_at.tzinfo is not None
    print("Z parse      : OK")

    # Naive datetime → assumed UTC, deterministic serialization.
    naive = datetime(2024, 1, 1, 12, 0, 0)
    n = DecisionRecord(
        run_id="n", workload_type="w", device_class="d", route="r",
        policy_version="v", truth_level="measured",
        predicted_energy_wh=1.0, predicted_carbon_gco2e=1.0,
        predicted_latency_ms=1.0, reason="x", observed_at=naive,
    )
    assert n.observed_at.replace(tzinfo=timezone.utc) == n.observed_at
    print("naive UTC    : OK")

    # --------------------------------------------------- 8. Truth level coercion
    assert DecisionRecord(
        run_id="t", workload_type="w", device_class="d", route="r",
        policy_version="v", truth_level=TruthLevel.SIMULATED,  # type: ignore[arg-type]
        predicted_energy_wh=1.0, predicted_carbon_gco2e=1.0,
        predicted_latency_ms=1.0, reason="x",
    ).truth_level == "simulated"

    # --------------------------------------------------- 9. Metadata uniformity
    for rec in (d, p, i, o):
        meta = rec.to_supermemory_payload()["metadata"]
        for required in (
            "type", "kind", "record_id", "truth_level",
            "container_tag", "observed_at", "schema_version",
        ):
            assert required in meta, f"{type(rec).__name__} missing {required}"
    print("metadata universal : OK")

    # --------------------------------------------------- 10. Schema version
    assert d.schema_version == SCHEMA_VERSION

    # --------------------------------------------------- 11. Cross-record hash
    records = {d, p, i, o}
    assert len(records) == 4
    print("hashable records   : OK")

    print("\nSmoke test passed.")
