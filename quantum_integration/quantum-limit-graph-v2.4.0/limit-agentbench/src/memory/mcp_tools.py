# src/memory/mcp_tools.py

"""
Memory MCP Tools
================

Exposes the Supermemory memory layer as MCP tools that the existing
``agentbeats/mcp_server.py`` can register.

Tools
-----
Read tools:
- ``recall_similar_runs``
- ``list_spaces``
- ``list_memories``
- ``memory_statistics``

Write tools (governed):
- ``remember_decision``
- ``remember_policy``
- ``record_outcome``
- ``record_incident``
- ``forget_memory``

Pipeline tools:
- ``check_feedback_loop``     (FeedbackLoopGuard)
- ``benchmark_memory``        (MemoryBenchmark)
- ``list_policies``           (PolicyMemory)
- ``snapshot_policy``         (PolicyMemory)
- ``supersede_policy``        (PolicyMemory)
- ``prune_episodes``          (EpisodicMemory)
- ``expire_tiers``            (MemoryTierManager)
- ``tier_of``                 (MemoryTierManager)

Enhancements
------------
- ``MemoryMCPConfig`` — frozen, validated, fully serializable:
  identity, schema version, tool prefix, bounded history, per-role
  writer ids, ``MappingProxyType``-frozen mappings, ``with_overrides`` /
  ``merge``.
- ``ToolCallRecord`` — deeply frozen, hashable, carries arguments,
  request id, error type, and result summary. Full serialization.
- ``MemoryMCPBridge`` — thread-safe, encapsulation-safe (uses
  ``adapter.peek_mirror()`` and ``adapter.statistics()``), exposes the
  full pipeline, returns a uniform schema-versioned response envelope
  for every tool.
- Structured error hierarchy:
  ``MemoryMCPError`` → ``MemoryMCPInputError``,
  ``MemoryMCPConfigError``, ``MemoryMCPGovernorError``,
  ``MemoryMCPAdapterError``, ``MemoryMCPToolError``,
  ``MemoryMCPUnavailableError``.
- Every tool has an async sibling (``*_async``).
- ``statistics()`` / ``reset()`` / ``close()`` / context manager.
- ``tool_prefix`` applied to the manifest returned by ``list_tools()``.
- Uses ``TTL_KIND`` / ``DEFAULT_CONTAINER_TAG`` / ``TruthLevel.coerce``
  from ``memory_schemas``.
- ``__version__`` exported via ``__all__``.
- ``__main__`` smoke test covers happy paths, governor rejection,
  missing governor, invalid truth level, missing capability,
  strict vs non-strict, async, and reset.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import threading
import time
import uuid
from collections import deque
from collections.abc import Mapping as ABCMapping
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from .bounded_recall import (
    BoundedRecall,
    BoundedRecallError,
    RecallBundle,
)
from .episodic_memory import (
    EpisodicMemory,
    EpisodicMemoryError,
)
from .feedback_loop_guard import (
    FeedbackLoopGuard,
    FeedbackLoopGuardError,
    GuardVerdict,
)
from .memory_benchmark import (
    BenchmarkReport,
    MemoryBenchmark,
    MemoryBenchmarkError,
)
from .memory_schemas import (
    DEFAULT_CONTAINER_TAG,
    DecisionRecord,
    IncidentRecord,
    MemorySchemaError,
    OutcomeRecord,
    PolicyRecord,
    TruthLevel,
)
from .memory_tier import (
    MemoryTier,
    MemoryTierError,
    MemoryTierManager,
)
from .policy_memory import (
    PolicyMemory,
    PolicyMemoryError,
    PolicySnapshot,
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

#: Version stamped on every tool response envelope.
RESPONSE_SCHEMA_VERSION: str = "mcp-memory/v1"


# --------------------------------------------------------------------------- #
# MCP tool decorator (graceful fallback when the SDK is absent)
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    from mcp.server.fastmcp import tool  # type: ignore

    _MCP_TOOL_AVAILABLE = True
except ImportError:  # pragma: no cover
    try:
        from mcp.server import tool  # type: ignore

        _MCP_TOOL_AVAILABLE = True
    except ImportError:

        def tool(*args: Any, **kwargs: Any) -> Callable:  # type: ignore
            def _wrap(fn: Callable) -> Callable:
                fn.__mcp_tool__ = True  # type: ignore[attr-defined]
                fn.__mcp_tool_kwargs__ = kwargs  # type: ignore[attr-defined]
                return fn

            if args and callable(args[0]) and not kwargs:
                return _wrap(args[0])
            return _wrap

        _MCP_TOOL_AVAILABLE = False
        logger.debug("MCP SDK unavailable; using fallback @tool decorator.")


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class MemoryMCPError(ValueError):
    """Base class for memory-MCP problems."""


class MemoryMCPInputError(MemoryMCPError):
    """Invalid input to a tool."""


class MemoryMCPConfigError(MemoryMCPError):
    """Invalid configuration."""


class MemoryMCPGovernorError(MemoryMCPError):
    """Write governor rejected a request or failed."""


class MemoryMCPAdapterError(MemoryMCPError):
    """The underlying adapter failed."""


class MemoryMCPToolError(MemoryMCPError):
    """A downstream tool failed."""


class MemoryMCPUnavailableError(MemoryMCPError):
    """A required collaborator was not provided."""


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


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
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


_MAX_ARG_STRING = 256
_MAX_ARG_SEQUENCE = 32
_MAX_ARG_MAPPING = 32


def _freeze_arg_value(value: Any) -> Any:
    if isinstance(value, str):
        if len(value) > _MAX_ARG_STRING:
            return value[: _MAX_ARG_STRING] + "…"
        return value
    if isinstance(value, (list, tuple)):
        if len(value) > _MAX_ARG_SEQUENCE:
            return list(value[: _MAX_ARG_SEQUENCE]) + [
                f"…({len(value) - _MAX_ARG_SEQUENCE} more)"
            ]
        return list(value)
    if isinstance(value, ABCMapping):
        if len(value) > _MAX_ARG_MAPPING:
            keys = list(value.keys())[: _MAX_ARG_MAPPING]
            return {k: value[k] for k in keys}
        return dict(value)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return str(value)


def _freeze_args(args: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(
        {k: _freeze_arg_value(v) for k, v in args.items()}
    )


def _derive_result_summary(data: Any) -> str:
    """Return a short human-readable summary of a tool's `data` payload."""
    if not isinstance(data, ABCMapping):
        return type(data).__name__
    for key in ("status", "verdict", "tier"):
        v = data.get(key)
        if isinstance(v, str) and v:
            return f"{key}={v}"
    for key in ("count", "removed", "size", "history_size"):
        v = data.get(key)
        if _is_real_int(v):
            return f"{key}={v}"
    return "ok"


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MemoryMCPConfig:
    """Tunable parameters for the memory MCP bridge."""

    server_name: str = "green-agent-memory"
    server_version: str = __version__
    schema_version: str = RESPONSE_SCHEMA_VERSION
    tool_prefix: str = "memory_"
    max_history: int = 1_000

    # Per-role writer ids. ``None`` falls back to ``default_writer_id``.
    default_writer_id: str = "mcp_client"
    decision_writer_id: Optional[str] = None
    policy_writer_id: Optional[str] = None
    outcome_writer_id: Optional[str] = None
    incident_writer_id: Optional[str] = None

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        for name in ("server_name", "server_version", "schema_version"):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise MemoryMCPConfigError(
                    f"{name} must be a non-empty string."
                )
        if not isinstance(self.tool_prefix, str):
            raise MemoryMCPConfigError("tool_prefix must be a string.")
        if not _is_real_int(self.max_history) or self.max_history <= 0:
            raise MemoryMCPConfigError(
                "max_history must be a positive int."
            )
        if not isinstance(self.default_writer_id, str) or not self.default_writer_id:
            raise MemoryMCPConfigError(
                "default_writer_id must be a non-empty string."
            )
        for name in (
            "decision_writer_id", "policy_writer_id",
            "outcome_writer_id", "incident_writer_id",
        ):
            v = getattr(self, name)
            if v is None:
                continue
            if not isinstance(v, str) or not v:
                raise MemoryMCPConfigError(
                    f"{name} must be None or a non-empty string."
                )

    # ------------------------------------------------------------------ #
    def writer_for(self, role: str) -> str:
        mapping = {
            "decision": self.decision_writer_id,
            "policy": self.policy_writer_id,
            "outcome": self.outcome_writer_id,
            "incident": self.incident_writer_id,
        }
        return mapping.get(role) or self.default_writer_id

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "server_name": self.server_name,
            "server_version": self.server_version,
            "schema_version": self.schema_version,
            "tool_prefix": self.tool_prefix,
            "max_history": self.max_history,
            "default_writer_id": self.default_writer_id,
            "decision_writer_id": self.decision_writer_id,
            "policy_writer_id": self.policy_writer_id,
            "outcome_writer_id": self.outcome_writer_id,
            "incident_writer_id": self.incident_writer_id,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any], *, strict: bool = False,
    ) -> "MemoryMCPConfig":
        if not isinstance(data, ABCMapping):
            raise MemoryMCPConfigError(
                "MemoryMCPConfig.from_dict expects a Mapping."
            )
        valid = {f.name for f in fields(cls)}
        unknown = set(data) - valid
        if strict and unknown:
            raise MemoryMCPConfigError(
                f"Unknown config key(s): {sorted(unknown)}."
            )
        kwargs: Dict[str, Any] = {
            k: v for k, v in data.items() if k in valid
        }
        try:
            return cls(**kwargs)
        except MemoryMCPError:
            raise
        except (TypeError, ValueError) as exc:
            raise MemoryMCPConfigError(
                f"failed to build MemoryMCPConfig: {exc}"
            ) from exc

    @classmethod
    def from_json(
        cls, payload: str, *, strict: bool = False,
    ) -> "MemoryMCPConfig":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemoryMCPConfigError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise MemoryMCPConfigError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data, strict=strict)

    def with_overrides(self, **kwargs: Any) -> "MemoryMCPConfig":
        valid = {f.name for f in fields(self)}
        unknown = set(kwargs) - valid
        if unknown:
            raise MemoryMCPConfigError(
                f"Unknown config field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def merge(self, other: "MemoryMCPConfig") -> "MemoryMCPConfig":
        defaults = MemoryMCPConfig()
        overrides: Dict[str, Any] = {}
        for f in fields(self):
            other_val = getattr(other, f.name)
            default_val = getattr(defaults, f.name)
            if other_val != default_val:
                overrides[f.name] = other_val
        return self.with_overrides(**overrides)

    def __hash__(self) -> int:
        return hash((
            self.server_name, self.server_version, self.schema_version,
            self.tool_prefix, self.max_history, self.default_writer_id,
            self.decision_writer_id, self.policy_writer_id,
            self.outcome_writer_id, self.incident_writer_id,
        ))

    def __repr__(self) -> str:
        return (
            "MemoryMCPConfig("
            f"server={self.server_name!r}, "
            f"prefix={self.tool_prefix!r}, "
            f"history={self.max_history})"
        )


# --------------------------------------------------------------------------- #
# Tool call record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ToolCallRecord:
    """Immutable snapshot of one MCP tool invocation."""

    tool_name: str
    outcome: str            # "ok" | "error" | "unavailable"
    duration_ms: float
    request_id: str
    arguments: Mapping[str, Any] = field(default_factory=dict)
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    result_summary: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    schema_version: str = RESPONSE_SCHEMA_VERSION

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        for name in ("tool_name", "request_id", "schema_version"):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise MemoryMCPInputError(
                    f"{name} must be a non-empty string."
                )
        if self.outcome not in ("ok", "error", "unavailable"):
            raise MemoryMCPInputError(
                f"outcome must be one of "
                f"('ok', 'error', 'unavailable'), got {self.outcome!r}."
            )
        if not _is_finite_nonneg(self.duration_ms):
            raise MemoryMCPInputError(
                "duration_ms must be finite and >= 0."
            )
        if not _is_finite_nonneg(self.timestamp):
            raise MemoryMCPInputError(
                "timestamp must be finite and >= 0."
            )
        if self.error_type is not None and not isinstance(self.error_type, str):
            raise MemoryMCPInputError(
                "error_type must be None or a string."
            )
        if self.error_message is not None and not isinstance(self.error_message, str):
            raise MemoryMCPInputError(
                "error_message must be None or a string."
            )
        if self.result_summary is not None and not isinstance(self.result_summary, str):
            raise MemoryMCPInputError(
                "result_summary must be None or a string."
            )
        if not isinstance(self.arguments, ABCMapping):
            raise MemoryMCPInputError("arguments must be a Mapping.")
        object.__setattr__(
            self, "arguments",
            MappingProxyType(dict(self.arguments)),
        )

    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "tool_name": self.tool_name,
            "outcome": self.outcome,
            "duration_ms": self.duration_ms,
            "request_id": self.request_id,
            "arguments": dict(self.arguments),
            "error_type": self.error_type,
            "error_message": self.error_message,
            "result_summary": self.result_summary,
            "timestamp": self.timestamp,
            "schema_version": self.schema_version,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ToolCallRecord":
        if not isinstance(data, ABCMapping):
            raise MemoryMCPInputError(
                "ToolCallRecord.from_dict expects a Mapping."
            )
        try:
            return cls(
                tool_name=str(data["tool_name"]),
                outcome=str(data.get("outcome", "ok")),
                duration_ms=float(data.get("duration_ms", 0.0)),
                request_id=str(data.get("request_id") or uuid.uuid4().hex),
                arguments=dict(data.get("arguments", {})),
                error_type=data.get("error_type"),
                error_message=data.get("error_message"),
                result_summary=data.get("result_summary"),
                timestamp=float(data.get("timestamp", time.time())),
                schema_version=str(
                    data.get("schema_version", RESPONSE_SCHEMA_VERSION)
                ),
            )
        except KeyError as exc:
            raise MemoryMCPInputError(
                f"ToolCallRecord.from_dict missing key {exc.args[0]!r}."
            ) from exc

    @classmethod
    def from_json(cls, payload: str) -> "ToolCallRecord":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemoryMCPInputError(
                f"ToolCallRecord.from_json invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    def __hash__(self) -> int:
        try:
            arg_hash = hash(tuple(sorted(self.arguments.items())))
        except TypeError:
            arg_hash = hash(
                json.dumps(dict(self.arguments), default=str, sort_keys=True)
            )
        return hash((
            self.tool_name, self.outcome, self.duration_ms,
            self.request_id, arg_hash, self.error_type,
            self.error_message, self.result_summary,
            self.timestamp, self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            f"ToolCallRecord(tool={self.tool_name!r}, "
            f"outcome={self.outcome}, "
            f"duration={self.duration_ms:.2f}ms)"
        )


# --------------------------------------------------------------------------- #
# Bridge
# --------------------------------------------------------------------------- #
class MemoryMCPBridge:
    """Exposes the memory layer as MCP tools.

    Parameters
    ----------
    adapter : SupermemoryAdapter
    recall : BoundedRecall
    governor : WriteGovernor, optional
        Required for governed write tools.
    guard : FeedbackLoopGuard, optional
    benchmark : MemoryBenchmark, optional
    policy_memory : PolicyMemory, optional
    episodic : EpisodicMemory, optional
    tier_manager : MemoryTierManager, optional
    config : MemoryMCPConfig, optional
    strict : bool, default True
    """

    _LATENCY_RING_SIZE: int = 200

    _TOOL_NAMES: Tuple[str, ...] = (
        "recall_similar_runs",
        "remember_decision",
        "remember_policy",
        "record_outcome",
        "record_incident",
        "forget_memory",
        "list_spaces",
        "list_memories",
        "memory_statistics",
        "check_feedback_loop",
        "benchmark_memory",
        "list_policies",
        "snapshot_policy",
        "supersede_policy",
        "prune_episodes",
        "expire_tiers",
        "tier_of",
    )

    # ------------------------------------------------------------------ #
    def __init__(
        self,
        *,
        adapter: SupermemoryAdapter,
        recall: BoundedRecall,
        governor: Optional[WriteGovernor] = None,
        guard: Optional[FeedbackLoopGuard] = None,
        benchmark: Optional[MemoryBenchmark] = None,
        policy_memory: Optional[PolicyMemory] = None,
        episodic: Optional[EpisodicMemory] = None,
        tier_manager: Optional[MemoryTierManager] = None,
        config: Optional[MemoryMCPConfig] = None,
        strict: bool = True,
    ) -> None:
        # --------------------------------------------------- validate
        if not isinstance(adapter, SupermemoryAdapter):
            raise MemoryMCPInputError(
                "adapter must be a SupermemoryAdapter."
            )
        if not isinstance(recall, BoundedRecall):
            raise MemoryMCPInputError(
                "recall must be a BoundedRecall."
            )
        if governor is not None and not isinstance(governor, WriteGovernor):
            raise MemoryMCPInputError(
                "governor must be a WriteGovernor or None."
            )
        if guard is not None and not isinstance(guard, FeedbackLoopGuard):
            raise MemoryMCPInputError(
                "guard must be a FeedbackLoopGuard or None."
            )
        if benchmark is not None and not isinstance(benchmark, MemoryBenchmark):
            raise MemoryMCPInputError(
                "benchmark must be a MemoryBenchmark or None."
            )
        if policy_memory is not None and not isinstance(policy_memory, PolicyMemory):
            raise MemoryMCPInputError(
                "policy_memory must be a PolicyMemory or None."
            )
        if episodic is not None and not isinstance(episodic, EpisodicMemory):
            raise MemoryMCPInputError(
                "episodic must be an EpisodicMemory or None."
            )
        if tier_manager is not None and not isinstance(tier_manager, MemoryTierManager):
            raise MemoryMCPInputError(
                "tier_manager must be a MemoryTierManager or None."
            )
        if config is None:
            config = MemoryMCPConfig()
        elif not isinstance(config, MemoryMCPConfig):
            raise MemoryMCPInputError(
                "config must be a MemoryMCPConfig or None."
            )

        self._adapter = adapter
        self._recall = recall
        self._governor = governor
        self._guard = guard
        self._benchmark = benchmark
        self._policy_memory = policy_memory
        self._episodic = episodic
        self._tier_manager = tier_manager
        self._config = config
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._history: Deque[ToolCallRecord] = deque(
            maxlen=self._config.max_history
        )

        # Counters
        self._total_calls = 0
        self._errors = 0
        self._unavailable = 0
        self._last_error: Optional[str] = None
        self._last_call: Optional[ToolCallRecord] = None
        self._latency_ring: Deque[float] = deque(
            maxlen=self._LATENCY_RING_SIZE
        )
        self._started_at = time.monotonic()

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> MemoryMCPConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def adapter(self) -> SupermemoryAdapter:
        return self._adapter

    @property
    def recall(self) -> BoundedRecall:
        return self._recall

    @property
    def governor(self) -> Optional[WriteGovernor]:
        return self._governor

    @property
    def guard(self) -> Optional[FeedbackLoopGuard]:
        return self._guard

    @property
    def benchmark(self) -> Optional[MemoryBenchmark]:
        return self._benchmark

    @property
    def policy_memory(self) -> Optional[PolicyMemory]:
        return self._policy_memory

    @property
    def episodic(self) -> Optional[EpisodicMemory]:
        return self._episodic

    @property
    def tier_manager(self) -> Optional[MemoryTierManager]:
        return self._tier_manager

    @property
    def history(self) -> List[ToolCallRecord]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- lifecycle
    def close(self) -> None:
        """Best-effort no-op. Kept for symmetry with sibling classes."""
        return None

    def __enter__(self) -> "MemoryMCPBridge":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # =================================================================== #
    # SYNC TOOLS
    # =================================================================== #
    @tool(description="Recall comparable historical runs from memory.")
    def recall_similar_runs(
        self,
        query: str,
        k: int = 5,
        container_tag: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Recall up to ``k`` comparable runs matching ``query``."""
        return self._run_tool(
            "recall_similar_runs",
            lambda: self._impl_recall_similar_runs(
                query, k, container_tag,
            ),
            arguments={
                "query": query, "k": k, "container_tag": container_tag,
            },
        )

    @tool(description="Persist a decision outcome to memory.")
    def remember_decision(
        self,
        run_id: str,
        workload_type: str,
        device_class: str,
        route: str,
        policy_version: str,
        predicted_energy_wh: float,
        predicted_carbon_gco2e: float,
        predicted_latency_ms: float,
        reason: str = "",
        truth_level: str = "measured",
        container_tag: Optional[str] = None,
        writer_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Persist a decision record via the governed write path."""
        return self._run_tool(
            "remember_decision",
            lambda: self._impl_remember_decision(
                run_id=run_id,
                workload_type=workload_type,
                device_class=device_class,
                route=route,
                policy_version=policy_version,
                predicted_energy_wh=predicted_energy_wh,
                predicted_carbon_gco2e=predicted_carbon_gco2e,
                predicted_latency_ms=predicted_latency_ms,
                reason=reason,
                truth_level=truth_level,
                container_tag=container_tag,
                writer_id=writer_id,
            ),
            arguments={
                "run_id": run_id, "workload_type": workload_type,
                "device_class": device_class, "route": route,
                "policy_version": policy_version,
                "predicted_energy_wh": predicted_energy_wh,
                "predicted_carbon_gco2e": predicted_carbon_gco2e,
                "predicted_latency_ms": predicted_latency_ms,
                "truth_level": truth_level,
                "container_tag": container_tag,
                "writer_id": writer_id,
            },
            requires=("governor",),
        )

    @tool(description="Persist a policy version to memory.")
    def remember_policy(
        self,
        version: str,
        content: Dict[str, Any],
        approved_by: str = "system",
        container_tag: Optional[str] = None,
        writer_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Persist a policy record via the governed write path."""
        return self._run_tool(
            "remember_policy",
            lambda: self._impl_remember_policy(
                version=version,
                content=content,
                approved_by=approved_by,
                container_tag=container_tag,
                writer_id=writer_id,
            ),
            arguments={
                "version": version, "content": content,
                "approved_by": approved_by,
                "container_tag": container_tag,
                "writer_id": writer_id,
            },
            requires=("governor",),
        )

    @tool(description="Record the measured outcome of a run.")
    def record_outcome(
        self,
        run_id: str,
        predicted: Dict[str, float],
        measured: Dict[str, float],
        truth_level: str = "measured",
        container_tag: Optional[str] = None,
        writer_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Persist a predicted-vs-measured outcome record."""
        return self._run_tool(
            "record_outcome",
            lambda: self._impl_record_outcome(
                run_id=run_id,
                predicted=predicted,
                measured=measured,
                truth_level=truth_level,
                container_tag=container_tag,
                writer_id=writer_id,
            ),
            arguments={
                "run_id": run_id, "predicted": predicted,
                "measured": measured, "truth_level": truth_level,
                "container_tag": container_tag, "writer_id": writer_id,
            },
            requires=("governor",),
        )

    @tool(description="Persist an incident record to memory.")
    def record_incident(
        self,
        incident_id: str,
        severity: str,
        description: str,
        run_id: Optional[str] = None,
        mitigation: Optional[str] = None,
        container_tag: Optional[str] = None,
        writer_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Persist an incident via the governed write path."""
        return self._run_tool(
            "record_incident",
            lambda: self._impl_record_incident(
                incident_id=incident_id,
                severity=severity,
                description=description,
                run_id=run_id,
                mitigation=mitigation,
                container_tag=container_tag,
                writer_id=writer_id,
            ),
            arguments={
                "incident_id": incident_id, "severity": severity,
                "description": description, "run_id": run_id,
                "mitigation": mitigation,
                "container_tag": container_tag,
                "writer_id": writer_id,
            },
            requires=("governor",),
        )

    @tool(description="Forget a memory by id (best-effort).")
    def forget_memory(self, memory_id: str) -> Dict[str, Any]:
        """Delete a memory from the adapter (also scrubs the local mirror)."""
        return self._run_tool(
            "forget_memory",
            lambda: self._impl_forget_memory(memory_id),
            arguments={"memory_id": memory_id},
        )

    @tool(description="List the spaces (container tags) known to Supermemory.")
    def list_spaces(self) -> Dict[str, Any]:
        """Return the container tags observed by the adapter's mirror."""
        return self._run_tool(
            "list_spaces", self._impl_list_spaces, arguments={},
        )

    @tool(description="List memories in a given container tag.")
    def list_memories(
        self,
        container_tag: Optional[str] = None,
        limit: int = 50,
        kind: Optional[str] = None,
        truth_level: Optional[str] = None,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Return memories in ``container_tag`` (best-effort)."""
        return self._run_tool(
            "list_memories",
            lambda: self._impl_list_memories(
                container_tag, limit, kind, truth_level, offset,
            ),
            arguments={
                "container_tag": container_tag, "limit": limit,
                "kind": kind, "truth_level": truth_level,
                "offset": offset,
            },
        )

    @tool(description="Return the adapter's statistics.")
    def memory_statistics(self) -> Dict[str, Any]:
        """Return the underlying adapter's statistics."""
        return self._run_tool(
            "memory_statistics", self._impl_memory_statistics,
            arguments={},
        )

    @tool(description="Check a recalled recommendation against current telemetry.")
    def check_feedback_loop(
        self,
        query: str,
        current_telemetry: Dict[str, float],
        current_policy_version: Optional[str] = None,
        k: int = 5,
        container_tag: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run the feedback-loop guard over freshly recalled evidence."""
        return self._run_tool(
            "check_feedback_loop",
            lambda: self._impl_check_feedback_loop(
                query, current_telemetry, current_policy_version,
                k, container_tag,
            ),
            arguments={
                "query": query,
                "current_telemetry": current_telemetry,
                "current_policy_version": current_policy_version,
                "k": k, "container_tag": container_tag,
            },
            requires=("guard",),
        )

    @tool(description="Run the memory benchmark on pre-computed runner outputs.")
    def benchmark_memory(
        self,
        baseline: List[Dict[str, float]],
        memory: List[Dict[str, float]],
    ) -> Dict[str, Any]:
        """Compare two sequences of per-task metric maps."""
        return self._run_tool(
            "benchmark_memory",
            lambda: self._impl_benchmark_memory(baseline, memory),
            arguments={"baseline": baseline, "memory": memory},
            requires=("benchmark",),
        )

    @tool(description="List the policy history retained by PolicyMemory.")
    def list_policies(self, limit: Optional[int] = None) -> Dict[str, Any]:
        return self._run_tool(
            "list_policies",
            lambda: self._impl_list_policies(limit),
            arguments={"limit": limit},
            requires=("policy_memory",),
        )

    @tool(description="Return a snapshot of the current policy.")
    def snapshot_policy(self) -> Dict[str, Any]:
        return self._run_tool(
            "snapshot_policy",
            self._impl_snapshot_policy,
            arguments={},
            requires=("policy_memory",),
        )

    @tool(description="Mark an old policy version as superseded.")
    def supersede_policy(
        self, old_version: str, new_version: str,
    ) -> Dict[str, Any]:
        return self._run_tool(
            "supersede_policy",
            lambda: self._impl_supersede_policy(old_version, new_version),
            arguments={
                "old_version": old_version,
                "new_version": new_version,
            },
            requires=("policy_memory",),
        )

    @tool(description="Sweep expired episodes from EpisodicMemory.")
    def prune_episodes(self) -> Dict[str, Any]:
        return self._run_tool(
            "prune_episodes",
            self._impl_prune_episodes,
            arguments={},
            requires=("episodic",),
        )

    @tool(description="Sweep expired records across all memory tiers.")
    def expire_tiers(self) -> Dict[str, Any]:
        return self._run_tool(
            "expire_tiers",
            self._impl_expire_tiers,
            arguments={},
            requires=("tier_manager",),
        )

    @tool(description="Return the tier a record currently lives in.")
    def tier_of(self, record_id: str) -> Dict[str, Any]:
        return self._run_tool(
            "tier_of",
            lambda: self._impl_tier_of(record_id),
            arguments={"record_id": record_id},
            requires=("tier_manager",),
        )

    # =================================================================== #
    # ASYNC SIBLINGS
    # =================================================================== #
    async def recall_similar_runs_async(self, **kwargs: Any) -> Dict[str, Any]:
        return await asyncio.to_thread(self.recall_similar_runs, **kwargs)

    async def remember_decision_async(self, **kwargs: Any) -> Dict[str, Any]:
        return await asyncio.to_thread(self.remember_decision, **kwargs)

    async def remember_policy_async(self, **kwargs: Any) -> Dict[str, Any]:
        return await asyncio.to_thread(self.remember_policy, **kwargs)

    async def record_outcome_async(self, **kwargs: Any) -> Dict[str, Any]:
        return await asyncio.to_thread(self.record_outcome, **kwargs)

    async def record_incident_async(self, **kwargs: Any) -> Dict[str, Any]:
        return await asyncio.to_thread(self.record_incident, **kwargs)

    async def forget_memory_async(self, **kwargs: Any) -> Dict[str, Any]:
        return await asyncio.to_thread(self.forget_memory, **kwargs)

    async def list_spaces_async(self) -> Dict[str, Any]:
        return await asyncio.to_thread(self.list_spaces)

    async def list_memories_async(self, **kwargs: Any) -> Dict[str, Any]:
        return await asyncio.to_thread(self.list_memories, **kwargs)

    async def memory_statistics_async(self) -> Dict[str, Any]:
        return await asyncio.to_thread(self.memory_statistics)

    async def check_feedback_loop_async(self, **kwargs: Any) -> Dict[str, Any]:
        return await asyncio.to_thread(self.check_feedback_loop, **kwargs)

    async def benchmark_memory_async(self, **kwargs: Any) -> Dict[str, Any]:
        return await asyncio.to_thread(self.benchmark_memory, **kwargs)

    async def list_policies_async(self, **kwargs: Any) -> Dict[str, Any]:
        return await asyncio.to_thread(self.list_policies, **kwargs)

    async def snapshot_policy_async(self) -> Dict[str, Any]:
        return await asyncio.to_thread(self.snapshot_policy)

    async def supersede_policy_async(self, **kwargs: Any) -> Dict[str, Any]:
        return await asyncio.to_thread(self.supersede_policy, **kwargs)

    async def prune_episodes_async(self) -> Dict[str, Any]:
        return await asyncio.to_thread(self.prune_episodes)

    async def expire_tiers_async(self) -> Dict[str, Any]:
        return await asyncio.to_thread(self.expire_tiers)

    async def tier_of_async(self, **kwargs: Any) -> Dict[str, Any]:
        return await asyncio.to_thread(self.tier_of, **kwargs)

    # =================================================================== #
    # IMPLEMENTATIONS
    # =================================================================== #
    def _impl_recall_similar_runs(
        self, query: str, k: int, container_tag: Optional[str],
    ) -> Dict[str, Any]:
        if not isinstance(query, str) or not query:
            raise MemoryMCPInputError(
                "query must be a non-empty string."
            )
        if not _is_real_int(k) or k <= 0:
            raise MemoryMCPInputError("k must be a positive int.")
        if container_tag is not None and (
            not isinstance(container_tag, str) or not container_tag
        ):
            raise MemoryMCPInputError(
                "container_tag must be None or a non-empty string."
            )
        try:
            bundle: RecallBundle = self._recall.query(
                query, k=k, container_tag=container_tag,
            )
        except BoundedRecallError as exc:
            raise MemoryMCPToolError(f"recall failed: {exc}") from exc
        return {
            "status": "ok",
            "query": bundle.query,
            "count": len(bundle.memories),
            "citations": list(bundle.citations),
            "unique_citations": list(bundle.unique_citations),
            "token_cost": bundle.token_cost,
            "latency_ms": bundle.latency_ms,
            "adapter_latency_ms": bundle.adapter_latency_ms,
            "truncated": bundle.truncated,
            "clamped_k": bundle.clamped_k,
            "candidates_considered": bundle.candidates_considered,
            "truth_level_mix": dict(bundle.truth_level_mix),
            "summary": self._recall.summarize(bundle),
            "memories": [
                {"id": m.get("id"), "metadata": dict(m.get("metadata") or {})}
                for m in bundle.memories
            ],
        }

    def _impl_remember_decision(
        self,
        *,
        run_id: str,
        workload_type: str,
        device_class: str,
        route: str,
        policy_version: str,
        predicted_energy_wh: float,
        predicted_carbon_gco2e: float,
        predicted_latency_ms: float,
        reason: str,
        truth_level: str,
        container_tag: Optional[str],
        writer_id: Optional[str],
    ) -> Dict[str, Any]:
        if self._governor is None:
            raise MemoryMCPUnavailableError(
                "remember_decision requires a governor."
            )
        try:
            truth = TruthLevel.coerce(truth_level).value
        except MemorySchemaError as exc:
            raise MemoryMCPInputError(
                f"truth_level invalid: {exc}"
            ) from exc
        tag = container_tag or self._adapter.config.default_container_tag
        try:
            record = DecisionRecord(
                run_id=run_id,
                workload_type=workload_type,
                device_class=device_class,
                route=route,
                policy_version=policy_version,
                truth_level=truth,
                predicted_energy_wh=predicted_energy_wh,
                predicted_carbon_gco2e=predicted_carbon_gco2e,
                predicted_latency_ms=predicted_latency_ms,
                reason=reason,
                container_tag=tag,
            )
        except MemorySchemaError as exc:
            raise MemoryMCPInputError(str(exc)) from exc

        effective_writer = writer_id or self._config.writer_for("decision")
        kind = DecisionRecord.TTL_KIND
        try:
            promoted = self._governor.promote(
                record, writer_id=effective_writer, kind=kind,
            )
        except WriteGovernorError as exc:
            raise MemoryMCPGovernorError(str(exc)) from exc

        memory_id: Optional[str] = None
        if promoted:
            try:
                memory_id = self._adapter.remember_decision(record)
            except SupermemoryAdapterError as exc:
                raise MemoryMCPAdapterError(str(exc)) from exc
        return {
            "status": "ok" if promoted else "rejected",
            "promoted": promoted,
            "memory_id": memory_id,
            "run_id": run_id,
            "kind": kind,
            "writer_id": effective_writer,
        }

    def _impl_remember_policy(
        self,
        *,
        version: str,
        content: Mapping[str, Any],
        approved_by: str,
        container_tag: Optional[str],
        writer_id: Optional[str],
    ) -> Dict[str, Any]:
        if self._governor is None:
            raise MemoryMCPUnavailableError(
                "remember_policy requires a governor."
            )
        if not isinstance(content, ABCMapping):
            raise MemoryMCPInputError("content must be a Mapping.")
        tag = container_tag or PolicyRecord.DEFAULT_CONTAINER_TAG
        try:
            record = PolicyRecord(
                version=version,
                content=dict(content),
                approved_by=approved_by,
                container_tag=tag,
                truth_level=TruthLevel.USER_REPORTED.value,
            )
        except MemorySchemaError as exc:
            raise MemoryMCPInputError(str(exc)) from exc

        effective_writer = writer_id or self._config.writer_for("policy")
        kind = PolicyRecord.TTL_KIND
        try:
            promoted = self._governor.promote(
                record, writer_id=effective_writer, kind=kind,
            )
        except WriteGovernorError as exc:
            raise MemoryMCPGovernorError(str(exc)) from exc

        memory_id: Optional[str] = None
        if promoted:
            try:
                memory_id = self._adapter.remember_policy(record)
            except SupermemoryAdapterError as exc:
                raise MemoryMCPAdapterError(str(exc)) from exc
        return {
            "status": "ok" if promoted else "rejected",
            "promoted": promoted,
            "memory_id": memory_id,
            "version": version,
            "kind": kind,
            "writer_id": effective_writer,
        }

    def _impl_record_outcome(
        self,
        *,
        run_id: str,
        predicted: Mapping[str, float],
        measured: Mapping[str, float],
        truth_level: str,
        container_tag: Optional[str],
        writer_id: Optional[str],
    ) -> Dict[str, Any]:
        if self._governor is None:
            raise MemoryMCPUnavailableError(
                "record_outcome requires a governor."
            )
        try:
            truth = TruthLevel.coerce(truth_level).value
        except MemorySchemaError as exc:
            raise MemoryMCPInputError(
                f"truth_level invalid: {exc}"
            ) from exc
        tag = container_tag or self._adapter.config.default_container_tag
        try:
            record = OutcomeRecord(
                run_id=run_id,
                predicted=dict(predicted),
                measured=dict(measured),
                truth_level=truth,
                container_tag=tag,
            )
        except MemorySchemaError as exc:
            raise MemoryMCPInputError(str(exc)) from exc

        effective_writer = writer_id or self._config.writer_for("outcome")
        kind = OutcomeRecord.TTL_KIND
        try:
            promoted = self._governor.promote(
                record, writer_id=effective_writer, kind=kind,
            )
        except WriteGovernorError as exc:
            raise MemoryMCPGovernorError(str(exc)) from exc

        memory_id: Optional[str] = None
        if promoted:
            try:
                memory_id = self._adapter.record_outcome(record)
            except SupermemoryAdapterError as exc:
                raise MemoryMCPAdapterError(str(exc)) from exc
        return {
            "status": "ok" if promoted else "rejected",
            "promoted": promoted,
            "memory_id": memory_id,
            "run_id": run_id,
            "delta": record.delta(),
            "missing_keys": record.missing_keys(),
            "kind": kind,
            "writer_id": effective_writer,
        }

    def _impl_record_incident(
        self,
        *,
        incident_id: str,
        severity: str,
        description: str,
        run_id: Optional[str],
        mitigation: Optional[str],
        container_tag: Optional[str],
        writer_id: Optional[str],
    ) -> Dict[str, Any]:
        if self._governor is None:
            raise MemoryMCPUnavailableError(
                "record_incident requires a governor."
            )
        tag = container_tag or IncidentRecord.DEFAULT_CONTAINER_TAG
        try:
            record = IncidentRecord(
                incident_id=incident_id,
                severity=severity,
                description=description,
                run_id=run_id,
                mitigation=mitigation,
                container_tag=tag,
            )
        except MemorySchemaError as exc:
            raise MemoryMCPInputError(str(exc)) from exc

        effective_writer = writer_id or self._config.writer_for("incident")
        kind = IncidentRecord.TTL_KIND
        try:
            promoted = self._governor.promote(
                record, writer_id=effective_writer, kind=kind,
            )
        except WriteGovernorError as exc:
            raise MemoryMCPGovernorError(str(exc)) from exc

        memory_id: Optional[str] = None
        if promoted:
            try:
                memory_id = self._adapter.remember(record.to_supermemory_payload())
            except SupermemoryAdapterError as exc:
                raise MemoryMCPAdapterError(str(exc)) from exc
        return {
            "status": "ok" if promoted else "rejected",
            "promoted": promoted,
            "memory_id": memory_id,
            "incident_id": incident_id,
            "kind": kind,
            "writer_id": effective_writer,
        }

    def _impl_forget_memory(self, memory_id: str) -> Dict[str, Any]:
        if not isinstance(memory_id, str) or not memory_id:
            raise MemoryMCPInputError(
                "memory_id must be a non-empty string."
            )
        try:
            ok = self._adapter.forget(memory_id)
        except SupermemoryAdapterError as exc:
            raise MemoryMCPAdapterError(str(exc)) from exc
        return {"status": "ok", "deleted": bool(ok), "memory_id": memory_id}

    def _impl_list_spaces(self) -> Dict[str, Any]:
        try:
            snapshot = self._adapter.peek_mirror()
        except Exception as exc:  # pragma: no cover - defensive
            raise MemoryMCPAdapterError(
                f"peek_mirror failed: {exc}"
            ) from exc
        spaces = sorted({
            str(m.get("container_tag"))
            for m in snapshot
            if isinstance(m, ABCMapping) and m.get("container_tag")
        })
        return {"status": "ok", "spaces": spaces, "source": "adapter_mirror"}

    def _impl_list_memories(
        self,
        container_tag: Optional[str],
        limit: int,
        kind: Optional[str],
        truth_level: Optional[str],
        offset: int,
    ) -> Dict[str, Any]:
        if not _is_real_int(limit) or limit <= 0:
            raise MemoryMCPInputError("limit must be a positive int.")
        if not _is_real_int(offset) or offset < 0:
            raise MemoryMCPInputError("offset must be a non-negative int.")
        if kind is not None and (not isinstance(kind, str) or not kind):
            raise MemoryMCPInputError(
                "kind must be None or a non-empty string."
            )
        if truth_level is not None:
            try:
                truth_level = TruthLevel.coerce(truth_level).value
            except MemorySchemaError as exc:
                raise MemoryMCPInputError(
                    f"truth_level invalid: {exc}"
                ) from exc

        tag = container_tag or self._adapter.config.default_container_tag
        snapshot = self._adapter.peek_mirror()
        memories: List[Dict[str, Any]] = []
        for m in snapshot:
            if not isinstance(m, ABCMapping):
                continue
            if m.get("container_tag") != tag:
                continue
            meta = m.get("metadata") or {}
            if not isinstance(meta, ABCMapping):
                meta = {}
            if kind is not None and meta.get("kind") != kind:
                continue
            if (
                truth_level is not None
                and meta.get("truth_level") != truth_level
            ):
                continue
            memories.append({
                "id": m.get("id") or meta.get("record_id"),
                "content": m.get("content"),
                "metadata": dict(meta),
            })
        total = len(memories)
        window = memories[offset : offset + limit]
        return {
            "status": "ok",
            "container_tag": tag,
            "total": total,
            "count": len(window),
            "offset": offset,
            "limit": limit,
            "memories": window,
        }

    def _impl_memory_statistics(self) -> Dict[str, Any]:
        try:
            stats = self._adapter.statistics()
        except Exception as exc:  # pragma: no cover - defensive
            raise MemoryMCPAdapterError(
                f"adapter.statistics failed: {exc}"
            ) from exc
        return {"status": "ok", "adapter": dict(stats)}

    def _impl_check_feedback_loop(
        self,
        query: str,
        current_telemetry: Mapping[str, float],
        current_policy_version: Optional[str],
        k: int,
        container_tag: Optional[str],
    ) -> Dict[str, Any]:
        if self._guard is None:
            raise MemoryMCPUnavailableError(
                "check_feedback_loop requires a guard."
            )
        if not isinstance(query, str) or not query:
            raise MemoryMCPInputError(
                "query must be a non-empty string."
            )
        if not isinstance(current_telemetry, ABCMapping):
            raise MemoryMCPInputError(
                "current_telemetry must be a Mapping."
            )
        if not _is_real_int(k) or k <= 0:
            raise MemoryMCPInputError("k must be a positive int.")

        try:
            bundle = self._recall.query(
                query, k=k, container_tag=container_tag,
            )
        except BoundedRecallError as exc:
            raise MemoryMCPToolError(f"recall failed: {exc}") from exc

        policy: Optional[PolicyRecord] = None
        if current_policy_version is not None:
            if not isinstance(current_policy_version, str) or not current_policy_version:
                raise MemoryMCPInputError(
                    "current_policy_version must be None or a non-empty "
                    "string."
                )
            policy = PolicyRecord(
                version=current_policy_version,
                content={},
                approved_by="system",
                container_tag=PolicyRecord.DEFAULT_CONTAINER_TAG,
            )

        try:
            verdict: GuardVerdict = self._guard.check(
                recalled=bundle,
                current_telemetry=dict(current_telemetry),
                current_policy=policy,
            )
        except FeedbackLoopGuardError as exc:
            raise MemoryMCPToolError(f"guard failed: {exc}") from exc
        except MemorySchemaError as exc:
            raise MemoryMCPInputError(str(exc)) from exc

        return {"status": "ok", "verdict": verdict.to_dict()}

    def _impl_benchmark_memory(
        self,
        baseline: List[Mapping[str, float]],
        memory: List[Mapping[str, float]],
    ) -> Dict[str, Any]:
        if self._benchmark is None:
            raise MemoryMCPUnavailableError(
                "benchmark_memory requires a benchmark."
            )
        if not isinstance(baseline, (list, tuple)) or not isinstance(
            memory, (list, tuple)
        ):
            raise MemoryMCPInputError(
                "baseline and memory must be sequences."
            )
        if len(baseline) != len(memory):
            raise MemoryMCPInputError(
                "baseline and memory must have the same length."
            )
        if not baseline:
            raise MemoryMCPInputError(
                "baseline and memory must not be empty."
            )

        baseline_maps = [dict(m) for m in baseline]
        memory_maps = [dict(m) for m in memory]

        def _runner(metric_maps: List[Dict[str, float]]) -> Callable[[Any], Dict[str, float]]:
            def runner(task: Any) -> Dict[str, float]:
                return metric_maps[task["index"]]
            return runner

        tasks = [{"index": i} for i in range(len(baseline_maps))]
        try:
            report: BenchmarkReport = self._benchmark.run_pair(
                tasks=tasks,
                without_memory=_runner(baseline_maps),
                with_memory=_runner(memory_maps),
            )
        except MemoryBenchmarkError as exc:
            raise MemoryMCPToolError(f"benchmark failed: {exc}") from exc
        return {"status": "ok", "report": report.to_dict()}

    def _impl_list_policies(self, limit: Optional[int]) -> Dict[str, Any]:
        if self._policy_memory is None:
            raise MemoryMCPUnavailableError(
                "list_policies requires a policy_memory."
            )
        if limit is not None and (not _is_real_int(limit) or limit <= 0):
            raise MemoryMCPInputError("limit must be None or a positive int.")
        try:
            records = self._policy_memory.history(limit=limit)
        except PolicyMemoryError as exc:
            raise MemoryMCPToolError(str(exc)) from exc
        return {
            "status": "ok",
            "count": len(records),
            "policies": [r.to_dict() for r in records],
        }

    def _impl_snapshot_policy(self) -> Dict[str, Any]:
        if self._policy_memory is None:
            raise MemoryMCPUnavailableError(
                "snapshot_policy requires a policy_memory."
            )
        try:
            snap: Optional[PolicySnapshot] = self._policy_memory.snapshot()
        except PolicyMemoryError as exc:
            raise MemoryMCPToolError(str(exc)) from exc
        if snap is None:
            return {"status": "ok", "snapshot": None}
        return {"status": "ok", "snapshot": snap.to_dict()}

    def _impl_supersede_policy(
        self, old_version: str, new_version: str,
    ) -> Dict[str, Any]:
        if self._policy_memory is None:
            raise MemoryMCPUnavailableError(
                "supersede_policy requires a policy_memory."
            )
        try:
            ok = self._policy_memory.supersede(old_version, new_version)
        except PolicyMemoryError as exc:
            raise MemoryMCPToolError(str(exc)) from exc
        return {
            "status": "ok",
            "superseded": bool(ok),
            "old_version": old_version,
            "new_version": new_version,
        }

    def _impl_prune_episodes(self) -> Dict[str, Any]:
        if self._episodic is None:
            raise MemoryMCPUnavailableError(
                "prune_episodes requires an episodic memory."
            )
        try:
            removed = self._episodic.prune()
        except EpisodicMemoryError as exc:
            raise MemoryMCPToolError(str(exc)) from exc
        return {"status": "ok", "removed": int(removed)}

    def _impl_expire_tiers(self) -> Dict[str, Any]:
        if self._tier_manager is None:
            raise MemoryMCPUnavailableError(
                "expire_tiers requires a tier manager."
            )
        try:
            removed = self._tier_manager.expire()
        except MemoryTierError as exc:
            raise MemoryMCPToolError(str(exc)) from exc
        return {"status": "ok", "removed": int(removed)}

    def _impl_tier_of(self, record_id: str) -> Dict[str, Any]:
        if self._tier_manager is None:
            raise MemoryMCPUnavailableError(
                "tier_of requires a tier manager."
            )
        if not isinstance(record_id, str) or not record_id:
            raise MemoryMCPInputError(
                "record_id must be a non-empty string."
            )
        try:
            tier: Optional[MemoryTier] = self._tier_manager.tier_of(record_id)
        except MemoryTierError as exc:
            raise MemoryMCPToolError(str(exc)) from exc
        return {
            "status": "ok",
            "record_id": record_id,
            "tier": tier.value if tier is not None else None,
        }

    # =================================================================== #
    # DISPATCHER
    # =================================================================== #
    def _run_tool(
        self,
        tool_name: str,
        impl: Callable[[], Any],
        *,
        arguments: Mapping[str, Any],
        requires: Sequence[str] = (),
    ) -> Dict[str, Any]:
        request_id = uuid.uuid4().hex
        frozen_args = _freeze_args(arguments)
        start = time.perf_counter()

        # ------------------------------------------------ availability
        missing = [
            name for name in requires
            if getattr(self, "_" + name, None) is None
        ]
        if missing:
            duration_ms = (time.perf_counter() - start) * 1000.0
            err_type = "MemoryMCPUnavailableError"
            err_msg = (
                f"tool '{tool_name}' requires collaborator(s): "
                f"{missing}"
            )
            self._record_call(
                ToolCallRecord(
                    tool_name=tool_name,
                    outcome="unavailable",
                    duration_ms=duration_ms,
                    request_id=request_id,
                    arguments=frozen_args,
                    error_type=err_type,
                    error_message=err_msg,
                ),
            )
            if self._strict:
                raise MemoryMCPUnavailableError(err_msg)
            return self._envelope(
                tool=tool_name,
                request_id=request_id,
                duration_ms=duration_ms,
                status="unavailable",
                error=(err_type, err_msg),
            )

        # ------------------------------------------------ dispatch
        status = "ok"
        data: Optional[Mapping[str, Any]] = None
        error: Optional[Tuple[str, str]] = None
        try:
            raw = impl()
            if not isinstance(raw, ABCMapping):
                raise MemoryMCPToolError(
                    f"tool '{tool_name}' returned "
                    f"{type(raw).__name__}; expected Mapping."
                )
            data = dict(raw)
        except MemoryMCPInputError as exc:
            error = (type(exc).__name__, str(exc))
            status = "error"
            if self._strict:
                self._record_after(
                    start, tool_name, request_id, frozen_args,
                    status="error", error=error,
                )
                raise
        except MemoryMCPUnavailableError as exc:
            error = (type(exc).__name__, str(exc))
            status = "unavailable"
            if self._strict:
                self._record_after(
                    start, tool_name, request_id, frozen_args,
                    status="unavailable", error=error,
                )
                raise
        except MemoryMCPGovernorError as exc:
            error = (type(exc).__name__, str(exc))
            status = "error"
            if self._strict:
                self._record_after(
                    start, tool_name, request_id, frozen_args,
                    status="error", error=error,
                )
                raise
        except MemoryMCPAdapterError as exc:
            error = (type(exc).__name__, str(exc))
            status = "error"
            if self._strict:
                self._record_after(
                    start, tool_name, request_id, frozen_args,
                    status="error", error=error,
                )
                raise
        except MemoryMCPError as exc:
            error = (type(exc).__name__, str(exc))
            status = "error"
            if self._strict:
                self._record_after(
                    start, tool_name, request_id, frozen_args,
                    status="error", error=error,
                )
                raise
        except (
            BoundedRecallError, FeedbackLoopGuardError,
            MemoryBenchmarkError, MemoryTierError,
            PolicyMemoryError, EpisodicMemoryError,
        ) as exc:
            error = (type(exc).__name__, str(exc))
            status = "error"
            if self._strict:
                self._record_after(
                    start, tool_name, request_id, frozen_args,
                    status="error", error=error,
                )
                raise MemoryMCPToolError(str(exc)) from exc
        except MemorySchemaError as exc:
            error = ("MemorySchemaError", str(exc))
            status = "error"
            if self._strict:
                self._record_after(
                    start, tool_name, request_id, frozen_args,
                    status="error", error=error,
                )
                raise MemoryMCPInputError(str(exc)) from exc
        except WriteGovernorError as exc:
            error = ("WriteGovernorError", str(exc))
            status = "error"
            if self._strict:
                self._record_after(
                    start, tool_name, request_id, frozen_args,
                    status="error", error=error,
                )
                raise MemoryMCPGovernorError(str(exc)) from exc
        except SupermemoryAdapterError as exc:
            error = ("SupermemoryAdapterError", str(exc))
            status = "error"
            if self._strict:
                self._record_after(
                    start, tool_name, request_id, frozen_args,
                    status="error", error=error,
                )
                raise MemoryMCPAdapterError(str(exc)) from exc
        except Exception as exc:
            logger.exception("tool '%s' failed: %s", tool_name, exc)
            error = (type(exc).__name__, str(exc))
            status = "error"
            if self._strict:
                self._record_after(
                    start, tool_name, request_id, frozen_args,
                    status="error", error=error,
                )
                raise MemoryMCPToolError(
                    f"tool '{tool_name}' failed: {exc}"
                ) from exc

        duration_ms = (time.perf_counter() - start) * 1000.0
        summary = _derive_result_summary(data) if data else None
        self._record_call(
            ToolCallRecord(
                tool_name=tool_name,
                outcome=status,
                duration_ms=duration_ms,
                request_id=request_id,
                arguments=frozen_args,
                error_type=error[0] if error else None,
                error_message=error[1] if error else None,
                result_summary=summary,
            ),
        )
        return self._envelope(
            tool=tool_name,
            request_id=request_id,
            duration_ms=duration_ms,
            status=status,
            data=data,
            error=error,
        )

    def _record_after(
        self,
        start: float,
        tool_name: str,
        request_id: str,
        frozen_args: Mapping[str, Any],
        *,
        status: str,
        error: Tuple[str, str],
    ) -> None:
        duration_ms = (time.perf_counter() - start) * 1000.0
        self._record_call(
            ToolCallRecord(
                tool_name=tool_name,
                outcome=status,
                duration_ms=duration_ms,
                request_id=request_id,
                arguments=frozen_args,
                error_type=error[0],
                error_message=error[1],
            ),
        )

    def _record_call(self, record: ToolCallRecord) -> None:
        with self._lock:
            self._history.append(record)
            self._total_calls += 1
            self._latency_ring.append(record.duration_ms)
            self._last_call = record
            if record.outcome == "error":
                self._errors += 1
                self._last_error = (
                    f"{record.tool_name}: {record.error_message}"
                )
            elif record.outcome == "unavailable":
                self._unavailable += 1
                self._last_error = (
                    f"{record.tool_name}: {record.error_message}"
                )

    @staticmethod
    def _envelope(
        *,
        tool: str,
        request_id: str,
        duration_ms: float,
        status: str,
        data: Optional[Mapping[str, Any]] = None,
        error: Optional[Tuple[str, str]] = None,
    ) -> Dict[str, Any]:
        env: Dict[str, Any] = {
            "status": status,
            "tool": tool,
            "schema_version": RESPONSE_SCHEMA_VERSION,
            "request_id": request_id,
            "duration_ms": duration_ms,
        }
        if data is not None:
            env["data"] = dict(data)
        if error is not None:
            env["error"] = {"type": error[0], "message": error[1]}
        return env

    # =================================================================== #
    # STATISTICS / SERIALIZATION
    # =================================================================== #
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            lats = list(self._latency_ring)
            mean_lat = sum(lats) / len(lats) if lats else 0.0
            by_tool: Dict[str, int] = {}
            for rec in self._history:
                by_tool[rec.tool_name] = by_tool.get(rec.tool_name, 0) + 1
            return {
                "calls": self._total_calls,
                "errors": self._errors,
                "unavailable": self._unavailable,
                "by_tool": by_tool,
                "mean_duration_ms": mean_lat,
                "p50_duration_ms": _percentile(lats, 50),
                "p95_duration_ms": _percentile(lats, 95),
                "max_duration_ms": max(lats) if lats else 0.0,
                "last_error": self._last_error,
                "last_call": (
                    self._last_call.to_dict()
                    if self._last_call is not None else None
                ),
                "config": self._config.to_dict(),
                "strict": self._strict,
                "mcp_sdk_available": _MCP_TOOL_AVAILABLE,
                "collaborators": {
                    "governor": self._governor is not None,
                    "guard": self._guard is not None,
                    "benchmark": self._benchmark is not None,
                    "policy_memory": self._policy_memory is not None,
                    "episodic": self._episodic is not None,
                    "tier_manager": self._tier_manager is not None,
                },
                "uptime_seconds": time.monotonic() - self._started_at,
            }

    def reset(self, *, clear_history: bool = True) -> int:
        """Reset counters (and optionally the tool-call history)."""
        with self._lock:
            removed = len(self._history) if clear_history else 0
            if clear_history:
                self._history.clear()
            self._total_calls = 0
            self._errors = 0
            self._unavailable = 0
            self._last_error = None
            self._last_call = None
            self._latency_ring.clear()
            self._started_at = time.monotonic()
        return removed

    def list_tools(self) -> List[Dict[str, Any]]:
        """Return the MCP tool manifest."""
        manifest: List[Dict[str, Any]] = []
        for name in self._TOOL_NAMES:
            method = getattr(self, name, None)
            if method is None or not callable(method):
                continue
            try:
                sig = inspect.signature(method)
            except (TypeError, ValueError):  # pragma: no cover
                continue
            params: Dict[str, Dict[str, Any]] = {}
            for pname, p in sig.parameters.items():
                if pname == "self":
                    continue
                entry: Dict[str, Any] = {
                    "type": (
                        p.annotation
                        if p.annotation is not inspect.Parameter.empty
                        else "Any"
                    ),
                }
                if p.default is not inspect.Parameter.empty:
                    entry["default"] = p.default
                    entry["required"] = False
                else:
                    entry["required"] = True
                params[pname] = entry
            doc = (method.__doc__ or "").strip().splitlines()
            manifest.append({
                "name": f"{self._config.tool_prefix}{name}",
                "method": name,
                "description": doc[0] if doc else "",
                "parameters": params,
            })
        manifest.sort(key=lambda x: x["name"])
        return manifest

    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "statistics": self.statistics(),
            }
            if include_history:
                payload["history"] = [r.to_dict() for r in self._history]
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
        recall: BoundedRecall,
        governor: Optional[WriteGovernor] = None,
        guard: Optional[FeedbackLoopGuard] = None,
        benchmark: Optional[MemoryBenchmark] = None,
        policy_memory: Optional[PolicyMemory] = None,
        episodic: Optional[EpisodicMemory] = None,
        tier_manager: Optional[MemoryTierManager] = None,
        strict: Optional[bool] = None,
        restore_history: bool = False,
    ) -> "MemoryMCPBridge":
        if not isinstance(data, ABCMapping):
            raise MemoryMCPInputError(
                "MemoryMCPBridge.from_dict expects a Mapping."
            )
        cfg_blob = data.get("config", {})
        config = (
            cfg_blob if isinstance(cfg_blob, MemoryMCPConfig)
            else MemoryMCPConfig.from_dict(cfg_blob)
        )
        resolved_strict = (
            bool(data.get("strict", True)) if strict is None else bool(strict)
        )
        bridge = cls(
            adapter=adapter,
            recall=recall,
            governor=governor,
            guard=guard,
            benchmark=benchmark,
            policy_memory=policy_memory,
            episodic=episodic,
            tier_manager=tier_manager,
            config=config,
            strict=resolved_strict,
        )
        if restore_history and isinstance(data.get("history"), list):
            with bridge._lock:  # noqa: SLF001 - intentional
                for raw in data["history"]:
                    try:
                        rec = ToolCallRecord.from_dict(raw)
                    except MemoryMCPError as exc:
                        logger.warning(
                            "from_dict: skipping malformed call: %s", exc,
                        )
                        continue
                    bridge._history.append(rec)
        return bridge

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        adapter: SupermemoryAdapter,
        recall: BoundedRecall,
        governor: Optional[WriteGovernor] = None,
        guard: Optional[FeedbackLoopGuard] = None,
        benchmark: Optional[MemoryBenchmark] = None,
        policy_memory: Optional[PolicyMemory] = None,
        episodic: Optional[EpisodicMemory] = None,
        tier_manager: Optional[MemoryTierManager] = None,
        strict: Optional[bool] = None,
        restore_history: bool = False,
    ) -> "MemoryMCPBridge":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise MemoryMCPInputError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise MemoryMCPInputError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(
            data,
            adapter=adapter,
            recall=recall,
            governor=governor,
            guard=guard,
            benchmark=benchmark,
            policy_memory=policy_memory,
            episodic=episodic,
            tier_manager=tier_manager,
            strict=strict,
            restore_history=restore_history,
        )

    def __repr__(self) -> str:
        with self._lock:
            return (
                "MemoryMCPBridge("
                f"calls={self._total_calls}, "
                f"errors={self._errors}, "
                f"strict={self._strict}, "
                f"mcp_sdk={_MCP_TOOL_AVAILABLE})"
            )


__all__ = [
    "MemoryMCPBridge",
    "MemoryMCPConfig",
    "MemoryMCPError",
    "MemoryMCPInputError",
    "MemoryMCPConfigError",
    "MemoryMCPGovernorError",
    "MemoryMCPAdapterError",
    "MemoryMCPToolError",
    "MemoryMCPUnavailableError",
    "RESPONSE_SCHEMA_VERSION",
    "ToolCallRecord",
    "_MCP_TOOL_AVAILABLE",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.mcp_tools
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    from .bounded_recall import BoundedRecall, BoundedRecallConfig
    from .episodic_memory import EpisodicMemory, IN_MEMORY_PATH
    from .feedback_loop_guard import FeedbackLoopGuard
    from .memory_benchmark import MemoryBenchmark, MemoryBenchmarkConfig
    from .memory_tier import MemoryTier, MemoryTierConfig, MemoryTierManager
    from .policy_memory import PolicyMemory
    from .supermemory_adapter import SupermemoryAdapter
    from .write_governor import WriteGovernor

    # ----------------------------------------------------------- setup
    adapter = SupermemoryAdapter()
    recall = BoundedRecall(adapter)
    governor = WriteGovernor()
    governor.register_writer(
        "mcp_client",
        capabilities={
            "write:decision", "write:policy",
            "write:outcome", "write:incident",
        },
    )
    guard = FeedbackLoopGuard()
    benchmark = MemoryBenchmark(
        config=MemoryBenchmarkConfig(min_task_count=2),
    )
    policy_memory = PolicyMemory(adapter=adapter, governor=governor)
    episodic = EpisodicMemory(memory_file=IN_MEMORY_PATH, autosave=False)
    tier_manager = MemoryTierManager(
        config=MemoryTierConfig(hot_capacity=8, warm_capacity=16),
    )

    bridge = MemoryMCPBridge(
        adapter=adapter,
        recall=recall,
        governor=governor,
        guard=guard,
        benchmark=benchmark,
        policy_memory=policy_memory,
        episodic=episodic,
        tier_manager=tier_manager,
    )
    print("repr         :", bridge)

    # ----------------------------------------------------------- 1. happy write
    r = bridge.remember_decision(
        run_id="run-001",
        workload_type="vision_inference",
        device_class="arm_edge",
        route="edge_int8",
        policy_version="v0.3",
        predicted_energy_wh=8.1,
        predicted_carbon_gco2e=3.7,
        predicted_latency_ms=410.0,
        reason="SLA met.",
    )
    assert r["status"] == "ok"
    assert r["schema_version"] == RESPONSE_SCHEMA_VERSION
    assert r["data"]["promoted"] is True
    assert r["data"]["kind"] == "decision_outcome"
    print("decision     :", r["status"], r["data"]["memory_id"])

    # ----------------------------------------------------------- 2. recall
    r = bridge.recall_similar_runs("vision_inference edge_int8", k=3)
    assert r["status"] == "ok"
    assert r["data"]["count"] >= 1
    assert "summary" in r["data"]
    print("recall       :", r["data"]["count"], "memories")

    # ----------------------------------------------------------- 3. policy write
    r = bridge.remember_policy(
        version="v0.3",
        content={"max_carbon_gco2e": 200.0},
        approved_by="ops",
    )
    assert r["status"] == "ok" and r["data"]["promoted"] is True
    assert r["data"]["kind"] == "policy"
    print("policy       :", r["status"])

    # ----------------------------------------------------------- 4. outcome
    r = bridge.record_outcome(
        run_id="run-001",
        predicted={"energy_wh": 8.1, "carbon_g": 3.7},
        measured={"energy_wh": 8.6, "carbon_g": 4.0},
    )
    assert r["status"] == "ok"
    assert r["data"]["kind"] == "run"
    assert "delta" in r["data"]
    print("outcome      :", r["data"]["delta"])

    # ----------------------------------------------------------- 5. incident
    r = bridge.record_incident(
        incident_id="inc-1",
        severity="high",
        description="Cloud overshoot.",
    )
    assert r["status"] == "ok"
    assert r["data"]["kind"] == "incident"
    print("incident     :", r["status"])

    # ----------------------------------------------------------- 6. spaces & memories
    r = bridge.list_spaces()
    assert r["status"] == "ok"
    assert isinstance(r["data"]["spaces"], list)
    print("spaces       :", r["data"]["spaces"][:3], "…")

    r = bridge.list_memories(limit=5, kind="decision_outcome")
    assert r["status"] == "ok"
    assert r["data"]["total"] >= 1
    print("list_mem     :", r["data"]["total"], "matching")

    # ----------------------------------------------------------- 7. forget
    target_id = None
    for m in adapter.peek_mirror():
        meta = m.get("metadata") or {}
        if meta.get("kind") == "decision_outcome":
            target_id = m.get("id")
            break
    if target_id:
        r = bridge.forget_memory(target_id)
        assert r["status"] == "ok"
        print("forget       :", r["data"]["deleted"])

    # ----------------------------------------------------------- 8. statistics tool
    r = bridge.memory_statistics()
    assert r["status"] == "ok"
    assert "mode" in r["data"]["adapter"] or "sdk_available" in r["data"]["adapter"]
    print("stats tool   :", list(r["data"]["adapter"].keys())[:5])

    # ----------------------------------------------------------- 9. guard
    r = bridge.check_feedback_loop(
        query="vision_inference",
        current_telemetry={"energy_wh": 8.1, "carbon_gco2e": 3.7},
        current_policy_version="v0.3",
    )
    assert r["status"] == "ok"
    assert "verdict" in r["data"]
    print("guard        :", r["data"]["verdict"]["allowed"])

    # ----------------------------------------------------------- 10. benchmark
    baseline = [
        {"quality": 0.8, "latency_ms": 400.0, "tokens": 1800.0,
         "energy_wh": 9.0, "carbon_gco2e": 4.2, "decision_consistency": 0.55}
        for _ in range(6)
    ]
    memory = [
        {"quality": 0.85, "latency_ms": 380.0, "tokens": 1400.0,
         "energy_wh": 8.6, "carbon_gco2e": 4.0, "decision_consistency": 0.72}
        for _ in range(6)
    ]
    r = bridge.benchmark_memory(baseline=baseline, memory=memory)
    assert r["status"] == "ok"
    assert r["data"]["report"]["verdict"] in (
        "memory_helps", "memory_neutral", "memory_hurts",
    )
    print("benchmark    :", r["data"]["report"]["verdict"])

    # ----------------------------------------------------------- 11. policy tools
    r = bridge.list_policies()
    assert r["status"] == "ok"
    print("policies     :", r["data"]["count"])

    r = bridge.snapshot_policy()
    assert r["status"] == "ok"
    assert r["data"]["snapshot"] is not None
    print("policy snap  :", r["data"]["snapshot"]["version"])

    r = bridge.supersede_policy("v0.3", "v0.4")
    assert r["status"] == "ok"
    print("supersede    :", r["data"]["superseded"])

    # ----------------------------------------------------------- 12. episodic / tier
    episodic.store({"content": "x"}, kind="run", run_id="r-1")
    r = bridge.prune_episodes()
    assert r["status"] == "ok"
    print("prune        :", r["data"]["removed"])

    tier_manager.store(
        record_id="t1", kind="policy", payload={"v": 1},
        importance=0.9, truth_level="measured",
    )
    r = bridge.tier_of("t1")
    assert r["status"] == "ok"
    assert r["data"]["tier"] == "hot"
    print("tier_of      :", r["data"]["tier"])

    r = bridge.expire_tiers()
    assert r["status"] == "ok"
    print("expire       :", r["data"]["removed"])

    # ----------------------------------------------------------- 13. missing collaborator
    no_gov = MemoryMCPBridge(
        adapter=adapter, recall=recall, governor=None, strict=False,
    )
    r = no_gov.remember_decision(
        run_id="x", workload_type="w", device_class="d", route="r",
        policy_version="v", predicted_energy_wh=1.0,
        predicted_carbon_gco2e=1.0, predicted_latency_ms=1.0,
    )
    assert r["status"] == "unavailable"
    assert r["error"]["type"] == "MemoryMCPUnavailableError"
    print("no governor  :", r["status"])

    # ----------------------------------------------------------- 14. strict raises
    strict_no_gov = MemoryMCPBridge(
        adapter=adapter, recall=recall, strict=True,
    )
    try:
        strict_no_gov.remember_decision(
            run_id="x", workload_type="w", device_class="d", route="r",
            policy_version="v", predicted_energy_wh=1.0,
            predicted_carbon_gco2e=1.0, predicted_latency_ms=1.0,
        )
    except MemoryMCPUnavailableError as exc:
        print("strict miss  :", exc)
    else:
        raise AssertionError("expected MemoryMCPUnavailableError")

    # ----------------------------------------------------------- 15. governor rejection
    gov2 = WriteGovernor()
    gov2.register_writer("mcp_client", capabilities=set())
    rej_bridge = MemoryMCPBridge(
        adapter=adapter, recall=recall, governor=gov2, strict=False,
    )
    r = rej_bridge.remember_policy(
        version="v0.4", content={"x": 1},
    )
    assert r["status"] == "ok"
    assert r["data"]["promoted"] is False
    print("rejected     :", r["data"]["status"])

    # strict version of governor rejection
    rej_strict = MemoryMCPBridge(
        adapter=adapter, recall=recall, governor=gov2, strict=True,
    )
    try:
        rej_strict.remember_policy(version="v0.4", content={"x": 1})
    except MemoryMCPGovernorError as exc:
        print("strict rej   :", exc)
    else:
        raise AssertionError("expected MemoryMCPGovernorError")

    # ----------------------------------------------------------- 16. invalid truth level
    r = bridge.remember_decision(
        run_id="x", workload_type="w", device_class="d", route="r",
        policy_version="v", predicted_energy_wh=1.0,
        predicted_carbon_gco2e=1.0, predicted_latency_ms=1.0,
        truth_level="bogus",
    ) if not bridge.strict else None
    # Use non-strict variant to see the envelope.
    ns_bridge = MemoryMCPBridge(
        adapter=adapter, recall=recall, governor=governor, strict=False,
    )
    r = ns_bridge.remember_decision(
        run_id="x", workload_type="w", device_class="d", route="r",
        policy_version="v", predicted_energy_wh=1.0,
        predicted_carbon_gco2e=1.0, predicted_latency_ms=1.0,
        truth_level="bogus",
    )
    assert r["status"] == "error"
    assert r["error"]["type"] == "MemoryMCPInputError"
    print("bad truth    :", r["error"]["message"][:60], "…")

    # ----------------------------------------------------------- 17. async
    async def _run_async():
        write = await bridge.recall_similar_runs_async("vision", k=2)
        stats = await bridge.memory_statistics_async()
        return write, stats

    write, stats = asyncio.run(_run_async())
    assert write["status"] == "ok"
    assert stats["status"] == "ok"
    print("async        :", write["data"]["count"], "memories")

    # ----------------------------------------------------------- 18. manifest
    manifest = bridge.list_tools()
    names = [m["name"] for m in manifest]
    assert "memory_recall_similar_runs" in names
    assert "memory_check_feedback_loop" in names
    assert "memory_benchmark_memory" in names
    assert "memory_prune_episodes" in names
    assert "memory_tier_of" in names
    assert all(m["name"].startswith("memory_") for m in manifest)
    print("manifest     :", len(manifest), "tools")

    # ----------------------------------------------------------- 19. serialization
    cfg = MemoryMCPConfig()
    assert MemoryMCPConfig.from_dict(cfg.to_dict()) == cfg
    assert MemoryMCPConfig.from_json(cfg.to_json()) == cfg
    cfg2 = cfg.with_overrides(max_history=42)
    assert cfg2.max_history == 42 and cfg.max_history == 1_000
    hash(cfg)
    print("cfg RT       : OK")

    rec = ToolCallRecord(
        tool_name="x", outcome="ok", duration_ms=1.0,
        request_id="req-1", arguments={"a": 1},
    )
    rec_rt = ToolCallRecord.from_dict(rec.to_dict())
    assert rec_rt == rec
    hash(rec)
    print("record RT    : OK (hashable)")

    bridge_dict = bridge.to_dict(include_history=True)
    assert "config" in bridge_dict and "history" in bridge_dict
    restored = MemoryMCPBridge.from_dict(
        bridge_dict,
        adapter=adapter, recall=recall, governor=governor,
        guard=guard, benchmark=benchmark,
        policy_memory=policy_memory, episodic=episodic,
        tier_manager=tier_manager,
        restore_history=True,
    )
    assert len(restored.history) == len(bridge.history)
    print("bridge RT    : OK")

    # ----------------------------------------------------------- 20. stats / reset
    stats = bridge.statistics()
    print("statistics   :", {
        k: v for k, v in stats.items()
        if k not in ("config", "last_call", "collaborators", "by_tool")
    })
    cleared = bridge.reset()
    assert cleared > 0
    assert bridge.statistics()["calls"] == 0
    print("reset        :", cleared, "calls cleared")

    # ----------------------------------------------------------- 21. context manager
    with MemoryMCPBridge(
        adapter=adapter, recall=recall, governor=governor,
    ) as ctx:
        ctx.list_spaces()
    print("ctx mgr      : OK")

    # ----------------------------------------------------------- 22. config validation
    for bad in (
        dict(server_name=""),
        dict(schema_version=""),
        dict(max_history=0),
        dict(max_history=True),
        dict(default_writer_id=""),
        dict(decision_writer_id=""),
    ):
        try:
            MemoryMCPConfig(**bad)  # type: ignore[arg-type]
        except MemoryMCPConfigError as exc:
            print(f"reject cfg   : {list(bad)[0]} -> {exc}")
        else:
            raise AssertionError(f"expected rejection for {bad!r}")

    print("\nSmoke test passed.")
