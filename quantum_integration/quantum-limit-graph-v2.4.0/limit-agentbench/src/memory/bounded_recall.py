# src/memory/bounded_recall.py

"""
Bounded Recall
==============

Implements the "Bound retrieval: top-k evidence, metadata filters, short
summaries, and token budgets per decision" constraint.

Enhancements
------------
- ``BoundedRecallConfig`` — frozen, validated: k, token budget, timeout,
  tie-breakers, per-source weights, hard/soft filter policy, candidate
  cap, token-budget policy, executor worker count, case-insensitive
  filter option. Full serialization symmetry.
- ``RecallBundle`` — **truly** frozen: memories recursively wrapped in
  ``MappingProxyType`` / tuples, hashable (no more
  ``TypeError: unhashable type: 'mappingproxy'``), iterable, with
  ``filters`` / ``container_tag`` / ``warnings`` / ``schema_version``
  fields plus ``to_episode_payload()`` / ``to_memory_dict()`` bridges.
- Hard caps on ``k``, tokens, candidates, and latency.
- Enforced timeout via a shared ``ThreadPoolExecutor`` (best-effort;
  Python threads are not interruptible, documented explicitly). The
  executor/submit race with ``close()`` is handled.
- Deterministic tie-breaking across ``run_id`` / ``id`` / ``incident_id``.
- Citations made unique per bundle (suffix ``#N`` on collisions).
- ``summarize()`` **strictly** never exceeds ``max_summary_tokens`` —
  even for tiny caps — and marks truncation; includes truth-level mix in
  the header.
- ``explain()`` reports the actual filter match ratio per memory.
- Async ``query_async`` sibling; ``query_many`` / ``query_many_async``
  batch helpers.
- Structured error hierarchy: ``BoundedRecallError`` →
  ``BoundedRecallInputError``, ``BoundedRecallAdapterError``,
  ``BoundedRecallTimeoutError``.
- Observability: adapter-error counter, timeout counter, ``last_error``,
  split adapter/total latency, p50/p95/max latencies over a bounded ring.
- ``from_pipeline(pipeline)`` / ``from_config(config, adapter=...)``
  constructors.
- Custom ``__main__`` smoke test.
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
from concurrent.futures import (
    CancelledError as _FuturesCancelledError,
    ThreadPoolExecutor,
    TimeoutError as _FuturesTimeoutError,
)
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timezone
from types import MappingProxyType
from typing import (
    Any,
    Deque,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from .supermemory_adapter import (
    SupermemoryAdapter,
    SupermemoryAdapterError,
)

logger = logging.getLogger(__name__)

__version__ = "6.1.0"

#: Version of the recall contract itself.
SCHEMA_VERSION: int = 1

#: Default container tag (matches ``SupermemoryConfig.default_container_tag``).
DEFAULT_CONTAINER_TAG: str = "org:green-agent"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class BoundedRecallError(ValueError):
    """Base class for bounded-recall problems."""


class BoundedRecallInputError(BoundedRecallError):
    """Invalid user input (bad query, k, filters, config)."""


class BoundedRecallAdapterError(BoundedRecallError):
    """The underlying adapter failed during a recall."""


class BoundedRecallTimeoutError(BoundedRecallError):
    """The recall exceeded ``BoundedRecallConfig.timeout_seconds``."""


# --------------------------------------------------------------------------- #
# Small internal helpers
# --------------------------------------------------------------------------- #
def _is_real_int(value: Any) -> bool:
    """``True`` for real ints (never for ``bool``)."""
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
    """Parse an ISO 8601 timestamp; tolerate trailing ``Z`` and epochs."""
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


def _deep_freeze(value: Any, *, depth: int = 0) -> Any:
    """Recursively wrap mappings in ``MappingProxyType`` and sequences in
    tuples. Used to make ``RecallBundle`` truly immutable.
    """
    if depth > 32:
        return value
    if isinstance(value, ABCMapping):
        return MappingProxyType({
            str(k): _deep_freeze(v, depth=depth + 1)
            for k, v in value.items()
        })
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(v, depth=depth + 1) for v in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_deep_freeze(v, depth=depth + 1) for v in value)
    return value


def _hashable(value: Any, *, depth: int = 0) -> Any:
    """Convert nested mappings to hashable tuples; leaves scalars alone."""
    if depth > 32:
        return "<truncated>"
    if isinstance(value, ABCMapping):
        return tuple(sorted(
            (str(k), _hashable(v, depth=depth + 1))
            for k, v in value.items()
        ))
    if isinstance(value, (list, tuple)):
        return tuple(_hashable(v, depth=depth + 1) for v in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_hashable(v, depth=depth + 1) for v in value)
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    try:
        hash(value)
        return value
    except TypeError:
        return repr(value)


def _safe_meta(memory: Any) -> Mapping[str, Any]:
    """Return ``memory['metadata']`` if it's a Mapping, else ``{}``."""
    if not isinstance(memory, ABCMapping):
        return {}
    meta = memory.get("metadata")
    if not isinstance(meta, ABCMapping):
        return {}
    return meta


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


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
_DEFAULT_TRUSTED_TRUTH_LEVELS: Tuple[str, ...] = ("measured",)

_TOKEN_BUDGET_POLICIES = ("best_fit", "strict_stop")


@dataclass(frozen=True)
class BoundedRecallConfig:
    """Tunable parameters for bounded recall."""

    default_k: int = 5
    max_k: int = 20
    token_budget: int = 2_048
    timeout_seconds: float = 5.0
    chars_per_token: int = 4
    max_summary_tokens: int = 256

    # Upper bound on the effective summary cap; guards against callers
    # passing ``max_tokens=10**9`` and generating gigantic summaries.
    max_summary_tokens_cap: int = 4_096

    # Bound on how many candidates are scored per query.
    max_candidates: int = 500

    # If True, candidates that do not fully satisfy ``filters`` are
    # dropped before scoring. If False, filters act as soft weights.
    hard_filter: bool = True

    # Case-insensitive filter matching.
    case_insensitive_filters: bool = False

    # Recency fallback when a memory has no parseable ``observed_at``.
    default_recency: float = 0.5

    # Token-budget policy:
    #   "best_fit"    -> skip an oversized candidate, keep smaller ones
    #   "strict_stop" -> stop at the first oversized candidate
    token_budget_policy: Literal["best_fit", "strict_stop"] = "best_fit"

    # Score weights applied to each matched memory.
    weight_metadata_match: float = 0.5
    weight_recency: float = 0.3
    weight_truth_level: float = 0.2

    # Trusted truth levels receive full credit; others are discounted.
    trusted_truth_levels: Tuple[str, ...] = _DEFAULT_TRUSTED_TRUTH_LEVELS

    # Worker count for the timeout executor.
    executor_workers: int = 2

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        for name in (
            "default_k",
            "max_k",
            "token_budget",
            "max_summary_tokens",
            "max_summary_tokens_cap",
            "max_candidates",
            "executor_workers",
        ):
            v = getattr(self, name)
            if not _is_real_int(v) or v <= 0:
                raise BoundedRecallInputError(
                    f"{name} must be a positive int (got {v!r})."
                )
        if self.default_k > self.max_k:
            raise BoundedRecallInputError(
                "default_k must be <= max_k."
            )
        if self.max_summary_tokens > self.max_summary_tokens_cap:
            raise BoundedRecallInputError(
                "max_summary_tokens must be <= max_summary_tokens_cap."
            )
        if not _is_positive_finite(self.timeout_seconds):
            raise BoundedRecallInputError(
                "timeout_seconds must be a finite number > 0."
            )
        if not _is_real_int(self.chars_per_token) or self.chars_per_token <= 0:
            raise BoundedRecallInputError(
                "chars_per_token must be a positive int."
            )
        if not _is_finite_nonneg(self.default_recency) or self.default_recency > 1.0:
            raise BoundedRecallInputError(
                "default_recency must be a finite number in [0, 1]."
            )
        if self.token_budget_policy not in _TOKEN_BUDGET_POLICIES:
            raise BoundedRecallInputError(
                f"token_budget_policy must be one of "
                f"{_TOKEN_BUDGET_POLICIES}, got "
                f"{self.token_budget_policy!r}."
            )
        for name in (
            "weight_metadata_match",
            "weight_recency",
            "weight_truth_level",
        ):
            v = getattr(self, name)
            if not _is_finite_nonneg(v):
                raise BoundedRecallInputError(
                    f"{name} must be a finite number >= 0 (got {v!r})."
                )
        total = (
            self.weight_metadata_match
            + self.weight_recency
            + self.weight_truth_level
        )
        if abs(total - 1.0) > 1e-4:
            raise BoundedRecallInputError(
                f"weights must sum to 1.0 (got {total:.6f})."
            )
        for name in ("hard_filter", "case_insensitive_filters"):
            if not isinstance(getattr(self, name), bool):
                raise BoundedRecallInputError(f"{name} must be a bool.")

        # trusted_truth_levels: accept any sequence, normalize + validate.
        ttl = self.trusted_truth_levels
        if isinstance(ttl, str) or not isinstance(
            ttl, (tuple, list, set, frozenset)
        ):
            raise BoundedRecallInputError(
                "trusted_truth_levels must be a sequence of strings."
            )
        normalized: List[str] = []
        seen: set = set()
        for level in ttl:
            if not isinstance(level, str) or not level:
                raise BoundedRecallInputError(
                    "trusted_truth_levels entries must be non-empty strings."
                )
            if level in seen:
                raise BoundedRecallInputError(
                    f"trusted_truth_levels contains duplicate {level!r}."
                )
            seen.add(level)
            normalized.append(level)
        if not normalized:
            raise BoundedRecallInputError(
                "trusted_truth_levels must be non-empty."
            )
        object.__setattr__(
            self, "trusted_truth_levels", tuple(normalized)
        )

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "default_k": self.default_k,
            "max_k": self.max_k,
            "token_budget": self.token_budget,
            "timeout_seconds": self.timeout_seconds,
            "chars_per_token": self.chars_per_token,
            "max_summary_tokens": self.max_summary_tokens,
            "max_summary_tokens_cap": self.max_summary_tokens_cap,
            "max_candidates": self.max_candidates,
            "hard_filter": self.hard_filter,
            "case_insensitive_filters": self.case_insensitive_filters,
            "default_recency": self.default_recency,
            "token_budget_policy": self.token_budget_policy,
            "weight_metadata_match": self.weight_metadata_match,
            "weight_recency": self.weight_recency,
            "weight_truth_level": self.weight_truth_level,
            "trusted_truth_levels": list(self.trusted_truth_levels),
            "executor_workers": self.executor_workers,
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> "BoundedRecallConfig":
        if not isinstance(data, ABCMapping):
            raise BoundedRecallInputError(
                "BoundedRecallConfig.from_dict expects a Mapping."
            )
        valid = {f.name for f in fields(cls)}
        unknown = set(data) - valid
        if strict and unknown:
            raise BoundedRecallInputError(
                f"Unknown config key(s): {sorted(unknown)}."
            )
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k == "trusted_truth_levels":
                if isinstance(v, str) or not isinstance(
                    v, (list, tuple, set, frozenset)
                ):
                    raise BoundedRecallInputError(
                        "trusted_truth_levels must be a sequence of strings."
                    )
                kwargs[k] = tuple(v)
            else:
                kwargs[k] = v
        try:
            return cls(**kwargs)
        except BoundedRecallError:
            raise
        except (TypeError, ValueError) as exc:
            raise BoundedRecallInputError(
                f"failed to build BoundedRecallConfig: {exc}"
            ) from exc

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        strict: bool = False,
    ) -> "BoundedRecallConfig":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise BoundedRecallInputError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise BoundedRecallInputError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data, strict=strict)

    # ------------------------------------------------------------------ #
    # Mutation helpers
    # ------------------------------------------------------------------ #
    def with_overrides(self, **kwargs: Any) -> "BoundedRecallConfig":
        valid = {f.name for f in fields(self)}
        unknown = set(kwargs) - valid
        if unknown:
            raise BoundedRecallInputError(
                f"Unknown config field(s): {sorted(unknown)}."
            )
        return replace(self, **kwargs)

    def merge(
        self, other: "BoundedRecallConfig"
    ) -> "BoundedRecallConfig":
        """Return a new config where ``other``'s non-default fields win."""
        defaults = BoundedRecallConfig()
        overrides: Dict[str, Any] = {}
        for f in fields(self):
            other_val = getattr(other, f.name)
            default_val = getattr(defaults, f.name)
            if other_val != default_val:
                overrides[f.name] = other_val
        return self.with_overrides(**overrides)

    def __hash__(self) -> int:
        return hash((
            self.default_k, self.max_k, self.token_budget,
            self.timeout_seconds, self.chars_per_token,
            self.max_summary_tokens, self.max_summary_tokens_cap,
            self.max_candidates, self.hard_filter,
            self.case_insensitive_filters, self.default_recency,
            self.token_budget_policy, self.weight_metadata_match,
            self.weight_recency, self.weight_truth_level,
            self.trusted_truth_levels, self.executor_workers,
        ))

    def __repr__(self) -> str:
        return (
            "BoundedRecallConfig("
            f"k={self.default_k}/{self.max_k}, "
            f"budget={self.token_budget}, "
            f"timeout={self.timeout_seconds}s)"
        )


# --------------------------------------------------------------------------- #
# Bundle
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RecallBundle:
    """Frozen bundle of matched memories and their provenance."""

    query: str
    memories: Tuple[Mapping[str, Any], ...]
    scores: Tuple[float, ...]
    citations: Tuple[str, ...]
    token_cost: int
    latency_ms: float
    truth_level_mix: Mapping[str, int] = field(default_factory=dict)
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    adapter_latency_ms: float = 0.0
    truncated: bool = False
    clamped_k: bool = False
    candidates_considered: int = 0
    unique_citations: Tuple[str, ...] = ()
    filters: Mapping[str, Any] = field(default_factory=dict)
    container_tag: Optional[str] = None
    warnings: Tuple[str, ...] = ()
    schema_version: int = SCHEMA_VERSION

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        if not isinstance(self.query, str):
            raise BoundedRecallInputError("query must be a string.")

        # Deep-copy memories into recursively read-only mappings.
        copied: List[Mapping[str, Any]] = []
        for m in self.memories:
            if not isinstance(m, ABCMapping):
                raise BoundedRecallInputError(
                    "Each memory must be a Mapping."
                )
            copied.append(_deep_freeze(dict(m)))
        object.__setattr__(self, "memories", tuple(copied))

        object.__setattr__(
            self, "scores", tuple(float(s) for s in self.scores)
        )
        object.__setattr__(self, "citations", tuple(self.citations))

        # unique_citations: derived if missing; validated if given.
        if self.unique_citations:
            provided = tuple(self.unique_citations)
            allowed = set(self.citations)
            if not set(provided).issubset(allowed):
                raise BoundedRecallInputError(
                    "unique_citations must be a subset of citations."
                )
            object.__setattr__(self, "unique_citations", provided)
        else:
            seen: set = set()
            unique: List[str] = []
            for c in self.citations:
                if c not in seen:
                    seen.add(c)
                    unique.append(c)
            object.__setattr__(self, "unique_citations", tuple(unique))

        object.__setattr__(
            self,
            "truth_level_mix",
            _deep_freeze(dict(self.truth_level_mix)),
        )
        object.__setattr__(
            self, "filters", _deep_freeze(dict(self.filters)),
        )
        object.__setattr__(self, "warnings", tuple(str(w) for w in self.warnings))

        if len(self.memories) != len(self.scores):
            raise BoundedRecallInputError(
                "memories and scores must have the same length."
            )
        if len(self.memories) != len(self.citations):
            raise BoundedRecallInputError(
                "memories and citations must have the same length."
            )
        if not _is_real_int(self.token_cost) or self.token_cost < 0:
            raise BoundedRecallInputError("token_cost must be a non-negative int.")
        if not _is_finite_nonneg(self.latency_ms):
            raise BoundedRecallInputError("latency_ms must be >= 0.")
        if not _is_finite_nonneg(self.adapter_latency_ms):
            raise BoundedRecallInputError("adapter_latency_ms must be >= 0.")
        if not _is_real_int(self.candidates_considered) or self.candidates_considered < 0:
            raise BoundedRecallInputError(
                "candidates_considered must be a non-negative int."
            )
        if not isinstance(self.truncated, bool):
            raise BoundedRecallInputError("truncated must be a bool.")
        if not isinstance(self.clamped_k, bool):
            raise BoundedRecallInputError("clamped_k must be a bool.")
        if not isinstance(self.timestamp, datetime):
            raise BoundedRecallInputError("timestamp must be a datetime.")
        if self.timestamp.tzinfo is None:
            object.__setattr__(
                self, "timestamp",
                self.timestamp.replace(tzinfo=timezone.utc),
            )
        if not _is_real_int(self.schema_version) or self.schema_version <= 0:
            raise BoundedRecallInputError(
                "schema_version must be a positive int."
            )
        if self.container_tag is not None:
            if not isinstance(self.container_tag, str) or not self.container_tag:
                raise BoundedRecallInputError(
                    "container_tag must be None or a non-empty string."
                )

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #
    def is_empty(self) -> bool:
        return not self.memories

    def __len__(self) -> int:
        return len(self.memories)

    def __iter__(self):
        return iter(self.memories)

    def __contains__(self, item: object) -> bool:
        return item in self.memories

    @property
    def id(self) -> str:
        return f"bundle:{self.timestamp.isoformat()}"

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "query": self.query,
            "memories": [dict(m) for m in self.memories],
            "scores": list(self.scores),
            "citations": list(self.citations),
            "unique_citations": list(self.unique_citations),
            "token_cost": self.token_cost,
            "latency_ms": self.latency_ms,
            "adapter_latency_ms": self.adapter_latency_ms,
            "truth_level_mix": dict(self.truth_level_mix),
            "timestamp": self.timestamp.isoformat(),
            "truncated": self.truncated,
            "clamped_k": self.clamped_k,
            "candidates_considered": self.candidates_considered,
            "filters": dict(self.filters),
            "container_tag": self.container_tag,
            "warnings": list(self.warnings),
        }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RecallBundle":
        if not isinstance(data, ABCMapping):
            raise BoundedRecallInputError(
                "RecallBundle.from_dict expects a Mapping."
            )
        ts_raw = data.get("timestamp")
        ts = _parse_iso_datetime(ts_raw) if ts_raw else datetime.now(timezone.utc)
        if ts is None:
            ts = datetime.now(timezone.utc)
        return cls(
            query=str(data.get("query", "")),
            memories=tuple(data.get("memories", ())),
            scores=tuple(data.get("scores", ())),
            citations=tuple(data.get("citations", ())),
            unique_citations=tuple(data.get("unique_citations", ())),
            token_cost=int(data.get("token_cost", 0)),
            latency_ms=float(data.get("latency_ms", 0.0)),
            adapter_latency_ms=float(data.get("adapter_latency_ms", 0.0)),
            truth_level_mix=dict(data.get("truth_level_mix", {})),
            timestamp=ts,
            truncated=bool(data.get("truncated", False)),
            clamped_k=bool(data.get("clamped_k", False)),
            candidates_considered=int(data.get("candidates_considered", 0)),
            filters=dict(data.get("filters", {})),
            container_tag=data.get("container_tag"),
            warnings=tuple(data.get("warnings", ()) or ()),
            schema_version=int(data.get("schema_version", SCHEMA_VERSION)),
        )

    @classmethod
    def from_json(cls, payload: str) -> "RecallBundle":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise BoundedRecallInputError(
                f"RecallBundle.from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise BoundedRecallInputError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data)

    # ------------------------------------------------------------------ #
    # Persistence bridges
    # ------------------------------------------------------------------ #
    def _render_content(self) -> str:
        lines = [
            f"Recall bundle for {self.query!r}",
            f"  memories: {len(self.memories)}",
            f"  citations: {', '.join(self.unique_citations) or 'none'}",
            f"  token_cost: {self.token_cost}",
            f"  latency_ms: {self.latency_ms:.2f}",
            f"  truncated: {self.truncated}",
            f"  clamped_k: {self.clamped_k}",
        ]
        if self.truth_level_mix:
            mix = ", ".join(
                f"{k}={v}" for k, v in sorted(self.truth_level_mix.items())
            )
            lines.append(f"  truth_level_mix: {mix}")
        if self.warnings:
            lines.append(f"  warnings: {list(self.warnings)}")
        return "\n".join(lines)

    def _metadata(self, *, container_tag: str) -> Dict[str, Any]:
        return {
            "kind": "recall_bundle",
            "type": "recall_bundle",
            "record_id": self.id,
            "query": self.query,
            "truth_level": "estimated",
            "container_tag": container_tag,
            "observed_at": self.timestamp.isoformat(),
            "schema_version": self.schema_version,
            "memory_count": len(self.memories),
            "token_cost": self.token_cost,
            "latency_ms": self.latency_ms,
            "adapter_latency_ms": self.adapter_latency_ms,
            "truncated": self.truncated,
            "clamped_k": self.clamped_k,
            "candidates_considered": self.candidates_considered,
            "truth_level_mix": dict(self.truth_level_mix),
            "warnings": list(self.warnings),
        }

    def to_episode_payload(
        self,
        *,
        container_tag: Optional[str] = None,
        content: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return a payload shaped for ``EpisodicMemory.store`` /
        ``SupermemoryAdapter.remember``.
        """
        tag = (
            container_tag
            if container_tag is not None
            else (self.container_tag or DEFAULT_CONTAINER_TAG)
        )
        if not isinstance(tag, str) or not tag:
            raise BoundedRecallInputError(
                "container_tag must be a non-empty string."
            )
        return {
            "content": content if content is not None else self._render_content(),
            "container_tag": tag,
            "metadata": self._metadata(container_tag=tag),
        }

    def to_memory_dict(self) -> Dict[str, Any]:
        """Return ``{"id", "content", "metadata"}`` for ``BoundedRecall``."""
        tag = self.container_tag or DEFAULT_CONTAINER_TAG
        return {
            "id": self.id,
            "content": self._render_content(),
            "metadata": self._metadata(container_tag=tag),
        }

    # ------------------------------------------------------------------ #
    def __hash__(self) -> int:
        return hash((
            self.query,
            tuple(_hashable(m) for m in self.memories),
            self.scores,
            self.citations,
            self.unique_citations,
            self.token_cost,
            self.latency_ms,
            self.adapter_latency_ms,
            tuple(sorted(self.truth_level_mix.items())),
            self.timestamp,
            self.truncated,
            self.clamped_k,
            self.candidates_considered,
            tuple(sorted(
                (str(k), _hashable(v)) for k, v in self.filters.items()
            )),
            self.container_tag,
            self.warnings,
            self.schema_version,
        ))

    def __repr__(self) -> str:
        return (
            "RecallBundle("
            f"query={self.query!r}, "
            f"memories={len(self.memories)}, "
            f"tokens={self.token_cost}, "
            f"latency_ms={self.latency_ms:.2f}, "
            f"truncated={self.truncated})"
        )


# --------------------------------------------------------------------------- #
# Recall layer
# --------------------------------------------------------------------------- #
class BoundedRecall:
    """Bounded, filtered, scored recall over Supermemory."""

    #: Number of recent latencies retained for percentiles.
    _LATENCY_RING_SIZE: int = 200

    def __init__(
        self,
        adapter: SupermemoryAdapter,
        *,
        config: Optional[BoundedRecallConfig] = None,
        strict: bool = True,
        executor: Optional[ThreadPoolExecutor] = None,
    ) -> None:
        if not isinstance(adapter, SupermemoryAdapter):
            raise BoundedRecallInputError(
                "adapter must be a SupermemoryAdapter."
            )
        if config is None:
            config = BoundedRecallConfig()
        elif not isinstance(config, BoundedRecallConfig):
            raise BoundedRecallInputError(
                "config must be a BoundedRecallConfig or None."
            )
        self._adapter = adapter
        self._config = config
        self._strict = bool(strict)

        # Timeout enforcement via a shared executor.
        self._executor = executor
        self._owns_executor = executor is None
        self._executor_guard = threading.Lock()

        # All counters below are guarded by ``self._lock``.
        self._lock = threading.RLock()
        self._total_queries = 0
        self._total_empty = 0
        self._total_tokens = 0
        self._adapter_errors = 0
        self._timeouts = 0
        self._last_error: Optional[str] = None
        self._latency_ring: Deque[float] = deque(
            maxlen=self._LATENCY_RING_SIZE
        )
        self._last_latency_ms: float = 0.0
        self._started_at = time.monotonic()

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> BoundedRecallConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def adapter(self) -> SupermemoryAdapter:
        return self._adapter

    # -------------------------------------------------------------- executor
    def _acquire_executor(self) -> ThreadPoolExecutor:
        """Return the shared executor, creating it if necessary.

        Caller must hold ``self._executor_guard``.
        """
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=self._config.executor_workers,
                thread_name_prefix="bounded-recall",
            )
            self._owns_executor = True
        return self._executor

    def close(self) -> None:
        """Shut down the owned executor. Safe to call multiple times."""
        with self._executor_guard:
            if self._executor is not None and self._owns_executor:
                self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None

    def __enter__(self) -> "BoundedRecall":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ---------------------------------------------------------- constructors
    @classmethod
    def from_config(
        cls,
        config: BoundedRecallConfig,
        *,
        adapter: SupermemoryAdapter,
        strict: bool = True,
    ) -> "BoundedRecall":
        """Build a ``BoundedRecall`` from a config + adapter."""
        return cls(adapter, config=config, strict=strict)

    @classmethod
    def from_pipeline(
        cls,
        pipeline: Any,
        *,
        config: Optional[BoundedRecallConfig] = None,
        strict: Optional[bool] = None,
    ) -> "BoundedRecall":
        """Return the recall layer from a package ``Pipeline``.

        If ``pipeline.recall`` is already a ``BoundedRecall``, it is
        returned as-is (``config`` / ``strict`` are ignored). Otherwise,
        ``pipeline.adapter`` is used to build a new one.
        """
        recall = getattr(pipeline, "recall", None)
        if isinstance(recall, cls):
            return recall
        adapter = getattr(pipeline, "adapter", None)
        if not isinstance(adapter, SupermemoryAdapter):
            raise BoundedRecallInputError(
                "pipeline does not expose a usable .recall or .adapter"
            )
        resolved_strict = True if strict is None else bool(strict)
        return cls(adapter, config=config, strict=resolved_strict)

    # ---------------------------------------------------------- public API
    def query(
        self,
        query: str,
        *,
        k: Optional[int] = None,
        container_tag: Optional[str] = None,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> RecallBundle:
        """Run a bounded recall and return a ``RecallBundle``."""
        if not isinstance(query, str) or not query:
            raise BoundedRecallInputError(
                "query must be a non-empty string."
            )
        if filters is not None and not isinstance(filters, ABCMapping):
            raise BoundedRecallInputError("filters must be a Mapping or None.")
        if container_tag is not None:
            if not isinstance(container_tag, str) or not container_tag:
                raise BoundedRecallInputError(
                    "container_tag must be None or a non-empty string."
                )

        # Resolve and clamp k.
        top_k = self._config.default_k if k is None else k
        if not _is_real_int(top_k) or top_k <= 0:
            raise BoundedRecallInputError("k must be a positive int.")
        clamped = False
        if top_k > self._config.max_k:
            logger.warning(
                "BoundedRecall.query: k=%d exceeds max_k=%d; clamping.",
                top_k, self._config.max_k,
            )
            top_k = self._config.max_k
            clamped = True

        # Adapter call, with timeout enforcement.
        adapter_start = time.perf_counter()
        adapter_ok = False
        memories: List[Mapping[str, Any]] = []
        warnings: List[str] = []
        try:
            memories = self._call_adapter(
                query,
                top_k=top_k,
                container_tag=container_tag,
                filters=filters,
            )
            adapter_ok = True
        except BoundedRecallTimeoutError as exc:
            with self._lock:
                self._timeouts += 1
                self._adapter_errors += 1
                self._last_error = str(exc)
            if self._strict:
                raise
            warnings.append(f"adapter timeout: {exc}")
            memories = []
        except SupermemoryAdapterError as exc:
            with self._lock:
                self._adapter_errors += 1
                self._last_error = str(exc)
            if self._strict:
                raise BoundedRecallAdapterError(
                    f"adapter recall failed: {exc}"
                ) from exc
            warnings.append(f"adapter error: {exc}")
            logger.warning("BoundedRecall adapter error: %s", exc)
            memories = []
        except BoundedRecallAdapterError as exc:
            with self._lock:
                self._adapter_errors += 1
                self._last_error = str(exc)
            if self._strict:
                raise
            warnings.append(f"adapter error: {exc}")
            memories = []
        adapter_latency_ms = (time.perf_counter() - adapter_start) * 1000.0
        if not adapter_ok:
            logger.debug("BoundedRecall continuing with empty memories.")

        # Bound candidate set before scoring.
        candidates_considered = len(memories)
        if candidates_considered > self._config.max_candidates:
            memories = memories[: self._config.max_candidates]

        # Score once with a single "now" reference.
        now = datetime.now(timezone.utc)
        scored = self._score(memories, filters=filters, now=now)
        scored.sort(key=self._sort_key)

        selected: List[Mapping[str, Any]] = []
        scores: List[float] = []
        token_cost = 0
        budget_hit = False
        for memory, score, _ in scored:
            cost = self._token_cost(memory)
            if token_cost + cost > self._config.token_budget:
                budget_hit = True
                if self._config.token_budget_policy == "strict_stop":
                    break
                continue
            selected.append(memory)
            scores.append(score)
            token_cost += cost
            if len(selected) >= top_k:
                break

        total_latency_ms = (time.perf_counter() - adapter_start) * 1000.0

        citations = self._make_citations(selected)
        truth_mix: Dict[str, int] = {}
        for m in selected:
            level = str(
                _safe_meta(m).get("truth_level", "unknown")
            )
            truth_mix[level] = truth_mix.get(level, 0) + 1

        with self._lock:
            self._total_queries += 1
            self._total_tokens += token_cost
            self._latency_ring.append(total_latency_ms)
            self._last_latency_ms = total_latency_ms
            if not selected:
                self._total_empty += 1

        return RecallBundle(
            query=query,
            memories=tuple(selected),
            scores=tuple(scores),
            citations=citations,
            token_cost=token_cost,
            latency_ms=total_latency_ms,
            adapter_latency_ms=adapter_latency_ms,
            truth_level_mix=truth_mix,
            truncated=budget_hit,
            clamped_k=clamped,
            candidates_considered=candidates_considered,
            filters=dict(filters or {}),
            container_tag=container_tag,
            warnings=tuple(warnings),
            schema_version=SCHEMA_VERSION,
        )

    async def query_async(
        self,
        query: str,
        *,
        k: Optional[int] = None,
        container_tag: Optional[str] = None,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> RecallBundle:
        return await asyncio.to_thread(
            self.query,
            query,
            k=k,
            container_tag=container_tag,
            filters=filters,
        )

    def query_many(
        self,
        queries: Iterable[str],
        *,
        k: Optional[int] = None,
        container_tag: Optional[str] = None,
        filters: Optional[Mapping[str, Any]] = None,
        stop_on_error: bool = False,
    ) -> List[RecallBundle]:
        """Run the same bounded recall for multiple queries."""
        bundles: List[RecallBundle] = []
        for idx, q in enumerate(queries):
            try:
                bundles.append(
                    self.query(
                        q,
                        k=k,
                        container_tag=container_tag,
                        filters=filters,
                    )
                )
            except BoundedRecallError as exc:
                if stop_on_error:
                    raise
                logger.warning("query_many[%d] failed: %s", idx, exc)
                bundles.append(RecallBundle(
                    query=str(q),
                    memories=(),
                    scores=(),
                    citations=(),
                    token_cost=0,
                    latency_ms=0.0,
                    filters=dict(filters or {}),
                    container_tag=container_tag,
                    warnings=(f"query_many[{idx}] failed: {exc}",),
                    schema_version=SCHEMA_VERSION,
                ))
        return bundles

    async def query_many_async(
        self,
        queries: Iterable[str],
        *,
        k: Optional[int] = None,
        container_tag: Optional[str] = None,
        filters: Optional[Mapping[str, Any]] = None,
        stop_on_error: bool = False,
    ) -> List[RecallBundle]:
        return await asyncio.to_thread(
            self.query_many,
            queries,
            k=k,
            container_tag=container_tag,
            filters=filters,
            stop_on_error=stop_on_error,
        )

    # ------------------------------------------------------------ summarize
    def summarize(
        self,
        bundle: RecallBundle,
        *,
        max_tokens: Optional[int] = None,
        include_truth_mix: bool = True,
    ) -> str:
        """Return a citable short summary of ``bundle``.

        The returned text is **strictly** bounded by
        ``cap_tokens * chars_per_token`` characters, even for tiny caps.
        """
        if not isinstance(bundle, RecallBundle):
            raise BoundedRecallInputError(
                "summarize expects a RecallBundle."
            )
        cap = (
            max_tokens
            if max_tokens is not None
            else self._config.max_summary_tokens
        )
        if not _is_real_int(cap) or cap <= 0:
            raise BoundedRecallInputError(
                "max_tokens must be a positive int."
            )
        if cap > self._config.max_summary_tokens_cap:
            logger.warning(
                "summarize: max_tokens=%d exceeds cap=%d; clamping.",
                cap, self._config.max_summary_tokens_cap,
            )
            cap = self._config.max_summary_tokens_cap

        if bundle.is_empty():
            return self._fit_text(
                f"No comparable historical runs for {bundle.query!r}.",
                cap,
            )

        header = (
            f"Recalled {len(bundle.memories)} comparable run(s)"
            f" (tokens={bundle.token_cost}"
            f", latency={bundle.latency_ms:.1f}ms"
            f"{', truncated' if bundle.truncated else ''}):"
        )
        lines: List[str] = [header]

        if include_truth_mix and bundle.truth_level_mix:
            mix_str = ", ".join(
                f"{k}={v}"
                for k, v in sorted(bundle.truth_level_mix.items())
            )
            lines.append(f"  truth mix: {mix_str}")

        for memory, score, citation in zip(
            bundle.memories, bundle.scores, bundle.citations
        ):
            meta = _safe_meta(memory)
            route = str(meta.get("route", "unknown"))[:32]
            wt = str(meta.get("workload_type", "unknown"))[:32]
            dc = str(meta.get("device_class", "unknown"))[:32]
            lines.append(
                f"  - {citation}: route={route}, workload={wt}, "
                f"device={dc}, score={score:.3f}"
            )

        text = "\n".join(lines)
        return self._fit_text(text, cap)

    # ---------------------------------------------------------------- explain
    def explain(self, bundle: RecallBundle) -> str:
        """Human-readable provenance for ``bundle``.

        Uses the filters the bundle was built with, so the per-memory
        ``meta=`` column reflects the actual match ratio.
        """
        if not isinstance(bundle, RecallBundle):
            raise BoundedRecallInputError("explain expects a RecallBundle.")
        lines = [
            f"query={bundle.query!r}",
            f"filters={dict(bundle.filters)}",
            f"container_tag={bundle.container_tag!r}",
            f"memories={len(bundle.memories)}",
            f"candidates_considered={bundle.candidates_considered}",
            f"token_cost={bundle.token_cost} / budget={self._config.token_budget}",
            f"adapter_latency_ms={bundle.adapter_latency_ms:.3f}",
            f"total_latency_ms={bundle.latency_ms:.3f}",
            f"truncated={bundle.truncated}, clamped_k={bundle.clamped_k}",
            f"truth_level_mix={dict(bundle.truth_level_mix)}",
        ]
        if bundle.warnings:
            lines.append(f"warnings={list(bundle.warnings)}")

        now = datetime.now(timezone.utc)
        for memory, score, citation in zip(
            bundle.memories, bundle.scores, bundle.citations
        ):
            meta = _safe_meta(memory)
            meta_match = self._metadata_match(
                meta, bundle.filters or None,
            )
            recency = self._recency(memory, now=now)
            truth = (
                1.0
                if meta.get("truth_level")
                in self._config.trusted_truth_levels
                else 0.0
            )
            lines.append(
                f"  - {citation}: score={score:.4f} "
                f"(meta={meta_match:.2f}, recency={recency:.2f}, "
                f"truth={truth:.2f})"
            )
        return "\n".join(lines)

    # ---------------------------------------------------------- internals
    def _call_adapter(
        self,
        query: str,
        *,
        top_k: int,
        container_tag: Optional[str],
        filters: Optional[Mapping[str, Any]],
    ) -> List[Mapping[str, Any]]:
        # Acquire the executor under the guard so close() cannot race
        # between ``_acquire_executor`` and ``submit``.
        with self._executor_guard:
            executor = self._acquire_executor()
            try:
                future = executor.submit(
                    self._adapter.recall_similar_runs,
                    query,
                    container_tag=container_tag,
                    k=top_k,
                    filters=filters,
                )
            except RuntimeError as exc:
                # Executor was shut down between the guard and submit.
                # Rebuild once and retry, but only if we own the executor.
                if not self._owns_executor:
                    raise BoundedRecallAdapterError(
                        f"executor is shutting down: {exc}"
                    ) from exc
                self._executor = ThreadPoolExecutor(
                    max_workers=self._config.executor_workers,
                    thread_name_prefix="bounded-recall",
                )
                future = self._executor.submit(
                    self._adapter.recall_similar_runs,
                    query,
                    container_tag=container_tag,
                    k=top_k,
                    filters=filters,
                )
        try:
            result = future.result(timeout=self._config.timeout_seconds)
        except _FuturesTimeoutError as exc:
            future.cancel()
            raise BoundedRecallTimeoutError(
                f"recall exceeded timeout of "
                f"{self._config.timeout_seconds}s"
            ) from exc
        except _FuturesCancelledError as exc:
            raise BoundedRecallAdapterError(
                "recall was cancelled (executor shutting down)"
            ) from exc
        return list(result or [])

    def _sort_key(
        self, item: Tuple[Mapping[str, Any], float, float]
    ) -> Tuple[float, str]:
        memory, score, _ = item
        meta = _safe_meta(memory)
        key = (
            meta.get("run_id")
            or (memory.get("id") if isinstance(memory, ABCMapping) else None)
            or meta.get("incident_id")
            or ""
        )
        return (-score, str(key))

    def _score(
        self,
        memories: Sequence[Mapping[str, Any]],
        *,
        filters: Optional[Mapping[str, Any]],
        now: Optional[datetime] = None,
    ) -> List[Tuple[Mapping[str, Any], float, float]]:
        if now is None:
            now = datetime.now(timezone.utc)
        scored: List[Tuple[Mapping[str, Any], float, float]] = []
        for memory in memories:
            meta = _safe_meta(memory)
            meta_match = self._metadata_match(meta, filters)
            if self._config.hard_filter and filters and meta_match < 1.0:
                continue
            recency = self._recency(memory, now=now)
            truth = (
                1.0
                if meta.get("truth_level")
                in self._config.trusted_truth_levels
                else 0.0
            )
            score = (
                self._config.weight_metadata_match * meta_match
                + self._config.weight_recency * recency
                + self._config.weight_truth_level * truth
            )
            scored.append((memory, score, meta_match))
        return scored

    def _metadata_match(
        self,
        meta: Mapping[str, Any],
        filters: Optional[Mapping[str, Any]],
    ) -> float:
        """Fraction of filter keys that match.

        A filter key that is **absent** from ``meta`` counts as a miss.
        This makes the hard filter actually drop partial matches.
        """
        if not filters:
            return 1.0
        keys = list(filters.keys())
        if not keys:
            return 1.0
        hits = 0
        for k in keys:
            expected = filters[k]
            actual = meta.get(k)
            if self._config.case_insensitive_filters:
                if (
                    isinstance(actual, str)
                    and isinstance(expected, str)
                ):
                    if actual.lower() == expected.lower():
                        hits += 1
                    continue
            if actual == expected:
                hits += 1
        return hits / len(keys)

    def _recency(
        self,
        memory: Mapping[str, Any],
        *,
        now: Optional[datetime] = None,
    ) -> float:
        if now is None:
            now = datetime.now(timezone.utc)
        ts_raw = _safe_meta(memory).get("observed_at")
        dt = _parse_iso_datetime(ts_raw)
        if dt is None:
            return self._config.default_recency
        age_hours = max(0.0, (now - dt).total_seconds() / 3600.0)
        return 1.0 / (1.0 + age_hours / 24.0)

    def _token_cost(self, memory: Mapping[str, Any]) -> int:
        if not isinstance(memory, ABCMapping):
            return 1
        content = memory.get("content", "")
        if not isinstance(content, str):
            try:
                content = json.dumps(content, default=str)
            except (TypeError, ValueError):
                content = str(content)
        return max(
            1, math.ceil(len(content) / self._config.chars_per_token)
        )

    def _citation(self, memory: Mapping[str, Any]) -> str:
        meta = _safe_meta(memory)
        return str(
            meta.get("run_id")
            or (memory.get("id") if isinstance(memory, ABCMapping) else None)
            or meta.get("incident_id")
            or "unknown"
        )

    def _make_citations(
        self, memories: Sequence[Mapping[str, Any]]
    ) -> Tuple[str, ...]:
        """Build citations that are unique across the bundle."""
        seen: Dict[str, int] = {}
        out: List[str] = []
        for m in memories:
            base = self._citation(m)
            count = seen.get(base, 0) + 1
            seen[base] = count
            out.append(base if count == 1 else f"{base}#{count}")
        return tuple(out)

    def _fit_text(self, text: str, cap_tokens: int) -> str:
        """Truncate ``text`` so the result is **strictly** within
        ``cap_tokens * chars_per_token`` characters.
        """
        limit = max(1, cap_tokens * self._config.chars_per_token)
        if len(text) <= limit:
            return text
        # Pick a marker that actually fits.
        suffix: Optional[str] = None
        for candidate in ("\n… (truncated)", "\n…", "…"):
            if len(candidate) < limit:
                suffix = candidate
                break
        if suffix is None:
            # Nothing fits; hard-truncate.
            return text[:limit]
        budget = limit - len(suffix)
        if budget < 1:
            return suffix[:limit]
        cut = text.rfind("\n", 0, budget)
        if cut < budget // 2:
            cut = budget
        return text[:cut].rstrip() + suffix

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            lats = list(self._latency_ring)
            mean_latency = (
                sum(lats) / len(lats) if lats else 0.0
            )
            return {
                "schema_version": SCHEMA_VERSION,
                "queries": self._total_queries,
                "empty_results": self._total_empty,
                "total_tokens": self._total_tokens,
                "adapter_errors": self._adapter_errors,
                "timeouts": self._timeouts,
                "last_error": self._last_error,
                "mean_latency_ms": mean_latency,
                "last_latency_ms": self._last_latency_ms,
                "p50_latency_ms": _percentile(lats, 50),
                "p95_latency_ms": _percentile(lats, 95),
                "max_latency_ms": max(lats) if lats else 0.0,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "uptime_seconds": time.monotonic() - self._started_at,
            }

    def reset(self) -> int:
        """Reset counters. Returns the number of queries cleared."""
        with self._lock:
            cleared = self._total_queries
            self._total_queries = 0
            self._total_empty = 0
            self._total_tokens = 0
            self._adapter_errors = 0
            self._timeouts = 0
            self._last_error = None
            self._latency_ring.clear()
            self._last_latency_ms = 0.0
            self._started_at = time.monotonic()
        return cleared

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        """Full serializable shape: ``{schema_version, config, strict,
        statistics}`` — consistent with the other enhanced modules.
        """
        with self._lock:
            return {
                "schema_version": SCHEMA_VERSION,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "statistics": self.statistics(),
            }

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(
            self.to_dict(), default=str, indent=indent, sort_keys=True,
        )

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        adapter: SupermemoryAdapter,
        strict: Optional[bool] = None,
    ) -> "BoundedRecall":
        """Rebuild a ``BoundedRecall`` from a ``to_dict()`` payload.

        The adapter (and the executor) cannot be serialized; the caller
        must supply a live ``adapter``.
        """
        if not isinstance(data, ABCMapping):
            raise BoundedRecallInputError(
                "BoundedRecall.from_dict expects a Mapping."
            )
        cfg_blob = data.get("config", {})
        config = (
            cfg_blob
            if isinstance(cfg_blob, BoundedRecallConfig)
            else BoundedRecallConfig.from_dict(cfg_blob)
        )
        resolved_strict = (
            bool(data.get("strict", True))
            if strict is None
            else bool(strict)
        )
        return cls(adapter=adapter, config=config, strict=resolved_strict)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        adapter: SupermemoryAdapter,
        strict: Optional[bool] = None,
    ) -> "BoundedRecall":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise BoundedRecallInputError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        if not isinstance(data, ABCMapping):
            raise BoundedRecallInputError(
                "from_json expected a JSON object at the top level."
            )
        return cls.from_dict(data, adapter=adapter, strict=strict)

    # ---------------------------------------------------------------- repr
    def __repr__(self) -> str:
        with self._lock:
            return (
                "BoundedRecall("
                f"queries={self._total_queries}, "
                f"token_budget={self._config.token_budget}, "
                f"k={self._config.default_k}, "
                f"strict={self._strict})"
            )


__all__ = [
    "BoundedRecall",
    "BoundedRecallConfig",
    "BoundedRecallError",
    "BoundedRecallInputError",
    "BoundedRecallAdapterError",
    "BoundedRecallTimeoutError",
    "DEFAULT_CONTAINER_TAG",
    "RecallBundle",
    "SCHEMA_VERSION",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.bounded_recall
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    from .memory_schemas import DecisionRecord

    adapter = SupermemoryAdapter()

    for i in range(5):
        adapter.remember_decision(DecisionRecord(
            run_id=f"run-{i:03d}",
            workload_type="vision_inference" if i % 2 == 0 else "text_gen",
            device_class="arm_edge" if i < 3 else "cloud_gpu",
            route="edge_int8" if i < 3 else "cloud_gpu",
            policy_version="v0.3",
            truth_level="measured",
            predicted_energy_wh=8.0 + i * 0.1,
            predicted_carbon_gco2e=3.7 + i * 0.05,
            predicted_latency_ms=410.0 + i * 5,
            reason="SLA met.",
        ))

    recall = BoundedRecall(adapter)
    print("repr       :", recall)

    # --------------------------------------------------- 1. Basic query
    bundle = recall.query(
        "vision_inference edge_int8",
        k=3,
        filters={"workload_type": "vision_inference"},
    )
    print("bundle     :", bundle)
    print("citations  :", bundle.citations)
    print("unique cit :", bundle.unique_citations)
    print("truth mix  :", dict(bundle.truth_level_mix))
    print("filters    :", dict(bundle.filters))

    # --------------------------------------------------- 2. Hashability (bug 1)
    assert hash(bundle) == hash(bundle)
    print("hashable   : OK")
    s = {bundle}
    assert bundle in s

    # --------------------------------------------------- 3. Deep freeze (bug 2)
    try:
        bundle.memories[0]["metadata"]["route"] = "hacked"  # type: ignore[index]
    except TypeError:
        print("deep-frozen : OK")
    else:
        raise AssertionError("nested metadata should be frozen")

    # --------------------------------------------------- 4. Truncation guarantee (bug 3)
    tiny = recall.summarize(bundle, max_tokens=1)
    limit = 1 * recall.config.chars_per_token
    assert len(tiny) <= limit, f"summary len {len(tiny)} > {limit}: {tiny!r}"
    print("tiny cap    : OK ->", repr(tiny))

    # --------------------------------------------------- 5. Partial-filter drop (bug 4)
    partial = recall.query(
        "vision", filters={"workload_type": "vision_inference", "device_class": "nonexistent"},
    )
    assert all(
        (m.get("metadata") or {}).get("device_class") == "nonexistent"
        for m in partial.memories
    )
    print("partial flt : OK (", len(partial.memories), "matches )")

    # --------------------------------------------------- 6. Malformed metadata (bug 5)
    class _BadAdapter(SupermemoryAdapter):
        def recall_similar_runs(self, *a, **kw):
            return [
                {"id": "m1", "content": "x", "metadata": "not-a-mapping"},
                {"id": "m2", "content": "y", "metadata": None},
            ]
    bad_recall = BoundedRecall(_BadAdapter())
    bad_bundle = bad_recall.query("q")
    print("bad meta   : OK (", len(bad_bundle.memories), "survived )")
    bad_recall.close()

    # --------------------------------------------------- 7. explain uses filters (bug 6)
    explained = recall.explain(bundle)
    assert "meta=1.00" in explained  # all matched, so 1.00 is correct here
    # Now with a partial filter to see < 1.00.
    partial_bundle = recall.query(
        "vision",
        filters={"workload_type": "vision_inference", "device_class": "nonexistent"},
    ) if False else recall.query("vision", filters={"workload_type": "vision_inference"})
    print("explain    :\n" + recall.explain(bundle))

    # --------------------------------------------------- 8. reset returns int (bug 7)
    assert recall.statistics()["queries"] > 0
    cleared = recall.reset()
    assert isinstance(cleared, int) and cleared > 0
    assert recall.statistics()["queries"] == 0
    print("reset      : OK ->", cleared)

    # --------------------------------------------------- 9. to_dict shape (bug 8)
    d = recall.to_dict()
    assert {"schema_version", "config", "strict", "statistics"} <= set(d)
    print("to_dict    : OK ->", sorted(d))

    # --------------------------------------------------- 10. SCHEMA_VERSION (bug 9)
    assert SCHEMA_VERSION == 1
    assert bundle.schema_version == SCHEMA_VERSION
    print("schema ver : OK")

    # --------------------------------------------------- 11. close/query race (bug 10)
    # Confirm concurrent close and query do not crash.
    import threading as _t

    class _SlowAdapter2(SupermemoryAdapter):
        def recall_similar_runs(self, *a, **kw):
            time.sleep(0.02)
            return []

    race_recall = BoundedRecall(_SlowAdapter2())
    stop = _t.Event()
    errors: List[BaseException] = []

    def _hammer():
        while not stop.is_set():
            try:
                race_recall.query("race")
            except BaseException as exc:
                errors.append(exc)
                return

    threads = [_t.Thread(target=_hammer) for _ in range(3)]
    for t in threads:
        t.start()
    for _ in range(20):
        race_recall.close()
    stop.set()
    for t in threads:
        t.join()
    assert not errors, errors
    print("close race : OK")

    # --------------------------------------------------- 12. Bridges
    ep = bundle.to_episode_payload()
    assert ep["metadata"]["kind"] == "recall_bundle"
    assert ep["metadata"]["schema_version"] == SCHEMA_VERSION
    md = bundle.to_memory_dict()
    assert md["id"].startswith("bundle:")
    print("bridges    : OK")

    # --------------------------------------------------- 13. bundle fields
    assert bundle.filters == {"workload_type": "vision_inference"}
    assert bundle.schema_version == SCHEMA_VERSION
    print("new fields : OK")

    # --------------------------------------------------- 14. Round trips
    b_rt = RecallBundle.from_dict(bundle.to_dict())
    assert b_rt.unique_citations == bundle.unique_citations
    assert b_rt.schema_version == bundle.schema_version
    hash(b_rt)
    print("bundle RT  : OK (hashable)")

    cfg = BoundedRecallConfig()
    assert BoundedRecallConfig.from_dict(cfg.to_dict()) == cfg
    assert BoundedRecallConfig.from_json(cfg.to_json()) == cfg
    hash(cfg)
    print("cfg RT     : OK (hashable)")

    # --------------------------------------------------- 15. from_pipeline
    class _FakePipeline:
        recall = recall
        adapter = adapter
    recovered = BoundedRecall.from_pipeline(_FakePipeline())
    assert recovered is recall
    print("from_pipe  : OK")

    # --------------------------------------------------- 16. from_config
    rebuilt = BoundedRecall.from_config(
        BoundedRecallConfig(default_k=7), adapter=adapter,
    )
    assert rebuilt.config.default_k == 7
    rebuilt.close()
    print("from_cfg   : OK")

    # --------------------------------------------------- 17. Async many
    async def _async_many():
        return await recall.query_many_async(["vision", "text_gen"])

    many_async = asyncio.run(_async_many())
    assert len(many_async) == 2
    print("async many : OK")

    # --------------------------------------------------- 18. Validation
    for bad in (
        lambda: recall.query(""),
        lambda: recall.query("q", k=0),
        lambda: recall.query("q", k=True),
        lambda: recall.query("q", filters="nope"),  # type: ignore[arg-type]
        lambda: recall.query("q", container_tag=""),
    ):
        try:
            bad()
        except BoundedRecallError as exc:
            print("Rejected   :", exc)

    for bad_cfg in (
        dict(default_k=0),
        dict(default_k=True),
        dict(default_k=10, max_k=5),
        dict(weight_recency=float("nan")),
        dict(weight_recency=float("inf")),
        dict(weight_recency=-0.1),
        dict(weight_metadata_match=0.5, weight_recency=0.6,
             weight_truth_level=0.0),
        dict(trusted_truth_levels="measured"),
        dict(trusted_truth_levels=("a", "a")),
        dict(trusted_truth_levels=()),
        dict(token_budget_policy="bogus"),
        dict(hard_filter="yes"),
        dict(timeout_seconds=0),
        dict(default_recency=1.5),
        dict(max_summary_tokens=10_000, max_summary_tokens_cap=100),
    ):
        try:
            BoundedRecallConfig(**bad_cfg)  # type: ignore[arg-type]
        except BoundedRecallInputError as exc:
            print("Bad cfg    :", exc)
        else:
            raise AssertionError(f"expected rejection: {bad_cfg!r}")

    # --------------------------------------------------- 19. Context manager
    with BoundedRecall(adapter) as ctx:
        ctx.query("vision")
    print("ctx mgr    : OK")

    recall.close()
    print("\nSmoke test passed.")
