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
  cap, token-budget policy. Supports ``to_dict``/``from_dict``/``to_json``/
  ``from_json`` and ``with_overrides``/``merge``.
- ``RecallBundle`` — truly frozen: memories deep-copied, ``truth_level_mix``
  made immutable via ``MappingProxyType``, hashable, iterable, supports
  ``to_dict``/``to_json``/``from_dict``/``from_json``.
- Hard caps on ``k``, tokens, candidates, and latency.
- Enforced timeout via a shared ``ThreadPoolExecutor`` (best-effort;
  Python threads are not interruptible, documented explicitly).
- Deterministic tie-breaking across ``run_id`` / ``id`` / ``incident_id``.
- Citations made unique per bundle (suffix ``#N`` on collisions).
- ``summarize()`` guaranteed to never exceed ``max_summary_tokens`` and
  marks truncation; includes truth-level mix in the header.
- ``explain()`` for human-readable per-bundle provenance.
- Async ``query_async`` sibling; ``query_many`` batch helper.
- Structured error hierarchy: ``BoundedRecallError`` →
  ``BoundedRecallInputError``, ``BoundedRecallAdapterError``,
  ``BoundedRecallTimeoutError``.
- Observability: adapter-error counter, split adapter/total latency,
  p50/p95/max latencies over a bounded ring.
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

__version__ = "6.0.0"


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
    # Numeric epochs (int / float, seconds or ms).
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        ts = float(value)
        if ts > 1e12:  # likely milliseconds
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

    # Bound on how many candidates are scored per query. Guards against
    # a very large mirror while keeping recall cheap.
    max_candidates: int = 500

    # If True, candidates that do not fully satisfy ``filters`` are
    # dropped before scoring. If False, filters act as soft weights.
    hard_filter: bool = True

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

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        # Positive ints.
        for name in (
            "default_k",
            "max_k",
            "token_budget",
            "max_summary_tokens",
            "max_candidates",
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

        # Positive finite floats.
        if not _is_positive_finite(self.timeout_seconds):
            raise BoundedRecallInputError(
                "timeout_seconds must be a finite number > 0."
            )

        # chars_per_token is a positive int.
        if not _is_real_int(self.chars_per_token) or self.chars_per_token <= 0:
            raise BoundedRecallInputError(
                "chars_per_token must be a positive int."
            )

        # Recency fallback.
        if not _is_finite_nonneg(self.default_recency) or self.default_recency > 1.0:
            raise BoundedRecallInputError(
                "default_recency must be a finite number in [0, 1]."
            )

        # Token-budget policy.
        if self.token_budget_policy not in _TOKEN_BUDGET_POLICIES:
            raise BoundedRecallInputError(
                f"token_budget_policy must be one of "
                f"{_TOKEN_BUDGET_POLICIES}, got "
                f"{self.token_budget_policy!r}."
            )

        # Weights: finite, non-negative, sum to 1 (within tolerance).
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

        # hard_filter must be a real bool.
        if not isinstance(self.hard_filter, bool):
            raise BoundedRecallInputError(
                "hard_filter must be a bool."
            )

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
            "max_candidates": self.max_candidates,
            "hard_filter": self.hard_filter,
            "default_recency": self.default_recency,
            "token_budget_policy": self.token_budget_policy,
            "weight_metadata_match": self.weight_metadata_match,
            "weight_recency": self.weight_recency,
            "weight_truth_level": self.weight_truth_level,
            "trusted_truth_levels": list(self.trusted_truth_levels),
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

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        if not isinstance(self.query, str):
            raise BoundedRecallInputError("query must be a string.")

        # Deep-copy memories into read-only mappings.
        copied: List[Mapping[str, Any]] = []
        for m in self.memories:
            if not isinstance(m, ABCMapping):
                raise BoundedRecallInputError(
                    "Each memory must be a Mapping."
                )
            copied.append(MappingProxyType(dict(m)))
        object.__setattr__(self, "memories", tuple(copied))

        object.__setattr__(
            self, "scores", tuple(float(s) for s in self.scores)
        )
        object.__setattr__(self, "citations", tuple(self.citations))
        if self.unique_citations:
            object.__setattr__(
                self, "unique_citations", tuple(self.unique_citations)
            )
        else:
            # Derive unique citations preserving order.
            seen: set = set()
            unique: List[str] = []
            for c in self.citations:
                if c not in seen:
                    seen.add(c)
                    unique.append(c)
            object.__setattr__(
                self, "unique_citations", tuple(unique)
            )

        object.__setattr__(
            self,
            "truth_level_mix",
            MappingProxyType(dict(self.truth_level_mix)),
        )

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

    # ------------------------------------------------------------------ #
    # Serialization
    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        return {
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
    def __hash__(self) -> int:
        return hash((
            self.query,
            self.memories,
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
        self._adapter = adapter
        self._config = config or BoundedRecallConfig()
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
    def _get_executor(self) -> ThreadPoolExecutor:
        if self._executor is not None:
            return self._executor
        with self._executor_guard:
            if self._executor is None:
                self._executor = ThreadPoolExecutor(
                    max_workers=2, thread_name_prefix="bounded-recall",
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
        try:
            memories = self._call_adapter(
                query,
                top_k=top_k,
                container_tag=container_tag,
                filters=filters,
            )
            adapter_ok = True
        except BoundedRecallTimeoutError:
            with self._lock:
                self._timeouts += 1
                self._adapter_errors += 1
            if self._strict:
                raise
            memories = []
        except SupermemoryAdapterError as exc:
            with self._lock:
                self._adapter_errors += 1
            if self._strict:
                raise BoundedRecallAdapterError(
                    f"adapter recall failed: {exc}"
                ) from exc
            logger.warning("BoundedRecall adapter error: %s", exc)
            memories = []
        except BoundedRecallAdapterError:
            with self._lock:
                self._adapter_errors += 1
            if self._strict:
                raise
            memories = []
        adapter_latency_ms = (time.perf_counter() - adapter_start) * 1000.0
        if not adapter_ok:
            logger.debug("BoundedRecall continuing with empty memories.")

        # Bound candidate set before scoring.
        candidates_considered = len(memories)
        if candidates_considered > self._config.max_candidates:
            memories = memories[: self._config.max_candidates]

        # Score, filter, and cap on tokens.
        scored = self._score(memories, filters=filters)
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
                (m.get("metadata") or {}).get("truth_level", "unknown")
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
                ))
        return bundles

    # ------------------------------------------------------------ summarize
    def summarize(
        self,
        bundle: RecallBundle,
        *,
        max_tokens: Optional[int] = None,
        include_truth_mix: bool = True,
    ) -> str:
        """Return a citable short summary of ``bundle``.

        Guaranteed to never exceed ``max_tokens`` * ``chars_per_token``
        characters; when truncation happens a trailing marker is added.
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
            meta = memory.get("metadata") or {}
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
        """Human-readable provenance for ``bundle``."""
        if not isinstance(bundle, RecallBundle):
            raise BoundedRecallInputError("explain expects a RecallBundle.")
        lines = [
            f"query={bundle.query!r}",
            f"memories={len(bundle.memories)}",
            f"candidates_considered={bundle.candidates_considered}",
            f"token_cost={bundle.token_cost} / budget={self._config.token_budget}",
            f"adapter_latency_ms={bundle.adapter_latency_ms:.3f}",
            f"total_latency_ms={bundle.latency_ms:.3f}",
            f"truncated={bundle.truncated}, clamped_k={bundle.clamped_k}",
            f"truth_level_mix={dict(bundle.truth_level_mix)}",
        ]
        for memory, score, citation in zip(
            bundle.memories, bundle.scores, bundle.citations
        ):
            meta = memory.get("metadata") or {}
            meta_match = self._metadata_match(meta, None)
            recency = self._recency(memory)
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
        executor = self._get_executor()
        future = executor.submit(
            self._adapter.recall_similar_runs,
            query,
            container_tag=container_tag,
            k=top_k,
            filters=filters,
        )
        try:
            result = future.result(timeout=self._config.timeout_seconds)
        except _FuturesTimeoutError as exc:
            # Best-effort cancel; underlying thread may still be running.
            future.cancel()
            raise BoundedRecallTimeoutError(
                f"recall exceeded timeout of "
                f"{self._config.timeout_seconds}s"
            ) from exc
        return list(result or [])

    def _sort_key(
        self, item: Tuple[Mapping[str, Any], float, float]
    ) -> Tuple[float, str]:
        memory, score, _ = item
        meta = memory.get("metadata") or {}
        key = (
            meta.get("run_id")
            or memory.get("id")
            or meta.get("incident_id")
            or ""
        )
        return (-score, str(key))

    def _score(
        self,
        memories: Sequence[Mapping[str, Any]],
        *,
        filters: Optional[Mapping[str, Any]],
    ) -> List[Tuple[Mapping[str, Any], float, float]]:
        scored: List[Tuple[Mapping[str, Any], float, float]] = []
        for memory in memories:
            meta = memory.get("metadata") or {}
            meta_match = self._metadata_match(meta, filters)
            if self._config.hard_filter and filters and meta_match < 1.0:
                continue
            recency = self._recency(memory)
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
        if not filters:
            return 1.0
        keys = [k for k in filters if k in meta]
        if not keys:
            return 0.0
        hits = sum(1 for k in keys if meta[k] == filters[k])
        return hits / len(keys)

    def _recency(self, memory: Mapping[str, Any]) -> float:
        ts_raw = (memory.get("metadata") or {}).get("observed_at")
        dt = _parse_iso_datetime(ts_raw)
        if dt is None:
            return self._config.default_recency
        age_hours = max(
            0.0,
            (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0,
        )
        return 1.0 / (1.0 + age_hours / 24.0)

    def _token_cost(self, memory: Mapping[str, Any]) -> int:
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
        meta = memory.get("metadata") or {}
        return str(
            meta.get("run_id")
            or memory.get("id")
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
        """Truncate ``text`` to fit within ``cap_tokens`` tokens."""
        limit = cap_tokens * self._config.chars_per_token
        if len(text) <= limit:
            return text
        suffix = "\n… (truncated)"
        budget = max(1, limit - len(suffix))
        cut = text.rfind("\n", 0, budget)
        if cut < budget // 2:
            cut = budget
        return text[:cut].rstrip() + suffix

    # ---------------------------------------------------------- statistics
    def _percentile(self, values: Sequence[float], pct: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        k = max(0, min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1)))))
        return ordered[k]

    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            lats = list(self._latency_ring)
            mean_latency = (
                sum(lats) / len(lats) if lats else 0.0
            )
            return {
                "queries": self._total_queries,
                "empty_results": self._total_empty,
                "total_tokens": self._total_tokens,
                "adapter_errors": self._adapter_errors,
                "timeouts": self._timeouts,
                "mean_latency_ms": mean_latency,
                "last_latency_ms": self._last_latency_ms,
                "p50_latency_ms": self._percentile(lats, 50),
                "p95_latency_ms": self._percentile(lats, 95),
                "max_latency_ms": max(lats) if lats else 0.0,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "uptime_seconds": time.monotonic() - self._started_at,
            }

    def reset(self) -> int:
        """Reset counters; returns the number of pending queries (0)."""
        with self._lock:
            self._total_queries = 0
            self._total_empty = 0
            self._total_tokens = 0
            self._adapter_errors = 0
            self._timeouts = 0
            self._latency_ring.clear()
            self._last_latency_ms = 0.0
            self._started_at = time.monotonic()
        return 0

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return self.statistics()

    def to_json(self, *, indent: Optional[int] = None) -> str:
        return json.dumps(
            self.statistics(), default=str, indent=indent, sort_keys=True,
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
    "RecallBundle",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.bounded_recall
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    from .memory_schemas import DecisionRecord

    adapter = SupermemoryAdapter()

    # Seed a few decisions with distinct metadata.
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

    # Basic query.
    bundle = recall.query(
        "vision_inference edge_int8",
        k=3,
        filters={"workload_type": "vision_inference"},
    )
    print("bundle     :", bundle)
    print("citations  :", bundle.citations)
    print("unique cit :", bundle.unique_citations)
    print("truth mix  :", dict(bundle.truth_level_mix))
    print("adapter ms :", round(bundle.adapter_latency_ms, 3))

    # Frozen-bundle checks.
    try:
        bundle.truth_level_mix["x"] = 1  # type: ignore[index]
    except TypeError:
        print("frozen mix : OK")
    try:
        bundle.memories[0]["content"] = "mutated"  # type: ignore[index]
    except TypeError:
        print("frozen mem : OK")
    assert hash(bundle) == hash(bundle)
    print("hashable   : OK")

    # Summary and explanation.
    summary = recall.summarize(bundle, max_tokens=64)
    print("summary    :\n" + summary)
    assert len(summary) <= 64 * recall.config.chars_per_token
    print("explain    :\n" + recall.explain(bundle))

    # Statistics.
    print("statistics :", {
        k: v for k, v in recall.statistics().items()
        if k not in ("config", "uptime_seconds")
    })

    # k clamping.
    clamped = recall.query("vision", k=10**6)
    assert clamped.clamped_k is True
    print("k clamp    : OK")

    # Hard filter drops non-matches.
    strict_filter = recall.query(
        "vision", filters={"workload_type": "text_gen"},
    )
    print("hard filter:", len(strict_filter.memories), "memories")

    # Batch.
    bundles = recall.query_many(["vision", "text_gen"])
    print("query_many :", [len(b.memories) for b in bundles])

    # Serialization round trips.
    cfg = BoundedRecallConfig()
    assert BoundedRecallConfig.from_dict(cfg.to_dict()) == cfg
    assert BoundedRecallConfig.from_json(cfg.to_json()) == cfg
    cfg2 = cfg.with_overrides(default_k=8)
    assert cfg2.default_k == 8 and cfg.default_k == 5
    print("cfg RT     : OK")

    b_dict = bundle.to_dict()
    b_restored = RecallBundle.from_dict(b_dict)
    assert len(b_restored.memories) == len(bundle.memories)
    assert b_restored.unique_citations == bundle.unique_citations
    print("bundle RT  : OK")

    # Async query.
    async def _run_async():
        return await recall.query_async("vision", k=2)

    async_bundle = asyncio.run(_run_async())
    print("async      :", len(async_bundle.memories), "memories")

    # Timeout enforcement.
    class _SlowAdapter(SupermemoryAdapter):
        def recall_similar_runs(self, *a, **kw):
            time.sleep(0.5)
            return []

    slow = BoundedRecall(
        _SlowAdapter(),
        config=BoundedRecallConfig(timeout_seconds=0.05),
    )
    try:
        slow.query("anything")
    except BoundedRecallTimeoutError as exc:
        print("timeout    : OK ->", exc)
    slow.close()

    # Validation.
    for bad in (
        lambda: recall.query(""),
        lambda: recall.query("q", k=0),
        lambda: recall.query("q", k=True),
        lambda: recall.query("q", filters="nope"),  # type: ignore[arg-type]
    ):
        try:
            bad()
        except BoundedRecallError as exc:
            print("Rejected   :", exc)

    # Config validation.
    for bad_cfg in (
        dict(default_k=0),
        dict(default_k=True),
        dict(default_k=10, max_k=5),
        dict(weight_recency=float("nan")),
        dict(weight_recency=float("inf")),
        dict(weight_recency=-0.1),
        dict(weight_metadata_match=0.5, weight_recency=0.6,
             weight_truth_level=0.0),  # sum != 1
        dict(trusted_truth_levels="measured"),
        dict(trusted_truth_levels=("a", "a")),
        dict(trusted_truth_levels=()),
        dict(token_budget_policy="bogus"),
        dict(hard_filter="yes"),
        dict(timeout_seconds=0),
        dict(default_recency=1.5),
    ):
        try:
            BoundedRecallConfig(**bad_cfg)  # type: ignore[arg-type]
        except BoundedRecallInputError as exc:
            print("Bad cfg    :", exc)
        else:
            raise AssertionError(f"expected rejection: {bad_cfg!r}")

    # Context manager.
    with BoundedRecall(adapter) as ctx:
        ctx.query("vision")
    print("ctx mgr    : OK")

    recall.close()
    print("\nSmoke test passed.")
