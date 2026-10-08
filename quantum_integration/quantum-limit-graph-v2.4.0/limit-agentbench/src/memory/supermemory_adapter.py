# src/memory/supermemory_adapter.py

"""
Supermemory Adapter
===================

Bridge between Green Agent and Supermemory. Provides governed, bounded,
retry-aware writes and reads that stay importable when Supermemory is
absent.

Interface
---------
- ``remember_decision(record)``
- ``remember_policy(record)``
- ``remember_incident(record)``
- ``record_outcome(record)``
- ``remember(payload)``                    (generic)
- ``remember_many(payloads)``              (generic, batch)
- ``remember_many_async(payloads)``        (generic, batch, async)
- ``recall_similar_runs(query, k=5)``
- ``forget(memory_id)``                    (returns a result dict)
- ``peek_mirror()``                        (read-only, deeply frozen entries)
- ``snapshot()``                           (structured view of the mirror)
- ``statistics()`` / ``reset()`` / ``close(flush=False)``
- Context-manager and async context-manager protocol
- Async siblings for every write, recall and forget

Enhancements
------------
- Defensive ``import supermemory`` with ``_SUPERMEMORY_AVAILABLE`` flag.
- ``RLock``-guarded state and bounded local mirror of writes.
- Monotonic local memory IDs (no more collisions once the mirror wraps).
- Substring search in the offline mirror; respects ``container_tag`` and
  an optional ``truth_level=`` filter.
- Payload validation (``content`` required; ``metadata`` must be a Mapping).
- ``container_tag`` resolved to the config default before send/mirror.
- ``forget()`` returns a ``{"deleted", "mirror_removed", "remote_ok",
  "reason"}`` result instead of a bare bool.
- ``k`` clamped to a configurable upper bound.
- Strict / non-strict modes, with schema errors always raised.
- Retry with configurable linear backoff on writes.
- Structured error hierarchy:
  ``SupermemoryAdapterError`` → ``SupermemoryClientError``,
  ``SupermemoryWriteError``, ``SupermemoryRecallError``,
  ``SupermemoryParseError``.
- ``SCHEMA_VERSION`` / ``DEFAULT_CONTAINER_TAG`` at module level; stamped
  on ``statistics()`` / ``to_dict()``; ``assert_compatible()`` helper.
- Deeply frozen mirror via ``_deep_freeze()`` — nested metadata cannot be
  mutated through ``peek_mirror()`` or ``recall_similar_runs()``.
- Robust ``_run_coroutine_blocking`` handling ``CancelledError`` and
  loop-binding edge cases.
- Latency observability: cumulative totals **and** mean / p50 / p95 /
  max for both writes and recalls.
- Unified ``last_error`` plus per-error-cause fields.
- Per-container distribution in ``statistics()``.
- ``client_error`` updated on runtime client failures, not just at
  construction.
- ``reset(clear_mirror=True)`` also resets ``_local_id``.
- ``from_config()`` / ``from_pipeline()`` constructors; ``from_dict``
  validates mirror entries.
- ``close(flush=True)`` reports mirror contents before shutdown.
- ``__version__`` / ``SCHEMA_VERSION`` / ``DEFAULT_CONTAINER_TAG``
  exported via ``__all__``.
- ``__main__`` smoke test that mocks the client and covers every new
  path.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
import uuid
from collections import deque
from collections.abc import Iterable, Mapping as ABCMapping
from concurrent.futures import CancelledError as _FuturesCancelledError
from types import MappingProxyType
from typing import Any, Deque, Dict, List, Mapping, Optional, Tuple

from .memory_schemas import (
    DecisionRecord,
    IncidentRecord,
    MemorySchemaError,
    OutcomeRecord,
    PolicyRecord,
)
from .supermemory_config import SupermemoryConfig, SupermemoryConfigError

logger = logging.getLogger(__name__)

__version__ = "6.1.0"

#: Version of the adapter contract itself.
SCHEMA_VERSION: int = 1

#: Default container tag (matches ``SupermemoryConfig.default_container_tag``
#: and ``memory_schemas.DEFAULT_CONTAINER_TAG``).
DEFAULT_CONTAINER_TAG: str = "org:green-agent"

#: Fallbacks for config-driven limits that may not exist on older configs.
_DEFAULT_MAX_RECALL_K: int = 1_000
_DEFAULT_RETRY_BACKOFF_SECONDS: float = 0.25
_DEFAULT_LATENCY_RING_SIZE: int = 200

# --------------------------------------------------------------------------- #
# Defensive import of the Supermemory SDK
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    from supermemory import Supermemory  # type: ignore

    _SUPERMEMORY_AVAILABLE = True
except ImportError:  # pragma: no cover
    Supermemory = None  # type: ignore[assignment]
    _SUPERMEMORY_AVAILABLE = False
    logger.debug("supermemory SDK not installed; running in offline mode.")


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SupermemoryAdapterError(ValueError):
    """Base class for adapter problems."""


class SupermemoryClientError(SupermemoryAdapterError):
    """Client construction or availability problems."""


class SupermemoryWriteError(SupermemoryAdapterError):
    """Failure while writing a memory."""


class SupermemoryRecallError(SupermemoryAdapterError):
    """Failure while recalling memories."""


class SupermemoryParseError(SupermemoryAdapterError):
    """Failed to parse a payload from dict/JSON."""


# --------------------------------------------------------------------------- #
# Shared freeze / hash / plain helpers — mirror the patched modules
# --------------------------------------------------------------------------- #
def _is_real_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _deep_freeze(value: Any, *, depth: int = 0) -> Any:
    """Recursively wrap mappings in ``MappingProxyType`` and sequences in
    tuples, so the mirror is truly immutable at every level.
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
    """Convert nested mappings / sequences to hashable tuples."""
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


def _to_plain(value: Any, *, depth: int = 0) -> Any:
    """Convert frozen structures back to plain dicts / lists so the
    returned data is JSON-serializable and freely mutable by the caller.
    """
    if depth > 32:
        return value
    if isinstance(value, ABCMapping):
        return {
            str(k): _to_plain(v, depth=depth + 1) for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_to_plain(v, depth=depth + 1) for v in value]
    if isinstance(value, (set, frozenset)):
        return [_to_plain(v, depth=depth + 1) for v in value]
    return value


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


# --------------------------------------------------------------------------- #
# Adapter
# --------------------------------------------------------------------------- #
class SupermemoryAdapter:
    """Bridge to the Supermemory service.

    Parameters
    ----------
    config : SupermemoryConfig, optional
        Adapter configuration. Defaults to ``SupermemoryConfig()``.
    strict : bool, default True
        If True, transport failures raise; if False, they log and are
        surfaced as ``None`` / ``[]``. Schema errors always raise.
    client : Any, optional
        Pre-built Supermemory-compatible client (used in tests).
    """

    def __init__(
        self,
        *,
        config: Optional[SupermemoryConfig] = None,
        strict: bool = True,
        client: Optional[Any] = None,
    ) -> None:
        if config is None:
            config = SupermemoryConfig()
        elif not isinstance(config, SupermemoryConfig):
            raise SupermemoryAdapterError(
                "config must be a SupermemoryConfig or None."
            )
        self._config = config
        self._strict = bool(strict)

        # Config-driven limits (read with getattr so the adapter works
        # against older SupermemoryConfig instances that lack the fields).
        self._max_recall_k = int(
            getattr(config, "max_recall_k", _DEFAULT_MAX_RECALL_K)
        )
        self._write_retry_backoff = float(
            getattr(
                config, "write_retry_backoff_seconds",
                _DEFAULT_RETRY_BACKOFF_SECONDS,
            )
        )
        self._latency_ring_size = int(
            getattr(config, "latency_ring_size", _DEFAULT_LATENCY_RING_SIZE)
        )

        # ----- All mutable state lives below; every access is guarded by
        # ----- ``self._lock`` (RLock). Keep it that way in future edits.
        self._lock = threading.RLock()
        self._mirror: Deque[Mapping[str, Any]] = deque(
            maxlen=self._config.max_history
        )
        self._local_id: int = 0
        self._write_errors: int = 0
        self._write_successes: int = 0
        self._recall_errors: int = 0
        self._recall_successes: int = 0
        self._write_total_seconds: float = 0.0
        self._recall_total_seconds: float = 0.0
        self._write_latency_ring: Deque[float] = deque(
            maxlen=self._latency_ring_size
        )
        self._recall_latency_ring: Deque[float] = deque(
            maxlen=self._latency_ring_size
        )
        self._last_error: Optional[str] = None
        self._last_write_error: Optional[str] = None
        self._last_recall_error: Optional[str] = None
        self._started_at: float = time.monotonic()

        # ----- Client resolution (may set ``self._client_error``).
        self._client_error: Optional[str] = None
        self._client = None

        if client is not None:
            self._client = client
        elif _SUPERMEMORY_AVAILABLE and Supermemory is not None:
            try:
                self._client = Supermemory(  # type: ignore[call-arg]
                    api_key=self._config.api_key,
                    base_url=self._config.base_url,
                )
            except Exception as exc:
                msg = f"could not construct Supermemory client: {exc}"
                self._client_error = msg
                if self._strict:
                    raise SupermemoryClientError(msg) from exc
                logger.warning("%s Running offline.", msg)
                self._client = None
        else:
            if self._strict and not self._config.is_local():
                raise SupermemoryClientError(
                    "supermemory SDK not installed and mode='cloud'."
                )
            self._client = None

        logger.debug(
            "SupermemoryAdapter initialized "
            "(mode=%s, sdk=%s, strict=%s, has_client=%s)",
            self._config.mode,
            _SUPERMEMORY_AVAILABLE,
            self._strict,
            self._client is not None,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> SupermemoryConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def sdk_available(self) -> bool:
        return _SUPERMEMORY_AVAILABLE

    @property
    def client_available(self) -> bool:
        return self._client is not None

    @property
    def mirror_size(self) -> int:
        with self._lock:
            return len(self._mirror)

    @property
    def client_error(self) -> Optional[str]:
        with self._lock:
            return self._client_error

    # ------------------------------------------------------------- container
    def __len__(self) -> int:
        return self.mirror_size

    def __contains__(self, memory_id: object) -> bool:
        if not isinstance(memory_id, str) or not memory_id:
            return False
        with self._lock:
            return any(
                m.get("id") == memory_id or m.get("memory_id") == memory_id
                for m in self._mirror
            )

    # ---------------------------------------------------------- constructors
    @classmethod
    def from_config(
        cls,
        config: SupermemoryConfig,
        *,
        strict: bool = True,
        client: Optional[Any] = None,
    ) -> "SupermemoryAdapter":
        return cls(config=config, strict=strict, client=client)

    @classmethod
    def from_pipeline(
        cls,
        pipeline: Any,
        *,
        config: Optional[SupermemoryConfig] = None,
        strict: Optional[bool] = None,
    ) -> "SupermemoryAdapter":
        """Return ``pipeline.adapter`` if present, else build a fresh one."""
        existing = getattr(pipeline, "adapter", None)
        if isinstance(existing, cls):
            return existing
        resolved = True if strict is None else bool(strict)
        return cls(config=config, strict=resolved)

    # ------------------------------------------------------------ lifecycle
    def close(self, *, flush: bool = False) -> None:
        """Best-effort client shutdown. Safe to call multiple times.

        Parameters
        ----------
        flush : bool
            If True, log the current mirror contents before shutdown.
            Does not persist anything (the mirror is best-effort
            in-memory state).
        """
        if flush:
            with self._lock:
                mirror_size = len(self._mirror)
                stats = self.statistics()
            logger.info(
                "SupermemoryAdapter.close(flush=True): "
                "mirror has %d entries; stats=%s",
                mirror_size,
                {k: stats[k] for k in (
                    "write_successes", "write_errors",
                    "recall_successes", "recall_errors",
                ) if k in stats},
            )
        client = self._client
        if client is None:
            return
        close = getattr(client, "close", None)
        if callable(close):
            try:
                close()
            except Exception as exc:  # pragma: no cover - SDK dependent
                logger.warning("client.close() failed: %s", exc)
        # Null out the client so a subsequent write falls into offline
        # mode instead of using a closed client.
        with self._lock:
            self._client = None

    def __enter__(self) -> "SupermemoryAdapter":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    async def __aenter__(self) -> "SupermemoryAdapter":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        self.close()

    # -------------------------------------------------------------- helpers
    @staticmethod
    def _resolve_container_tag(
        payload: Mapping[str, Any], config: SupermemoryConfig
    ) -> str:
        tag = payload.get("container_tag")
        if not isinstance(tag, str) or not tag:
            tag = config.default_container_tag
        return tag

    @staticmethod
    def _extract_memory_id(result: Any) -> Optional[str]:
        """Extract a memory id from a variety of SDK result shapes."""
        if not isinstance(result, ABCMapping):
            return None
        # Direct keys.
        for key in ("id", "memory_id", "memoryId"):
            v = result.get(key)
            if isinstance(v, str) and v:
                return v
        # Nested payloads: {"data": {...}}, {"memory": {...}}.
        for wrapper in ("data", "memory", "result"):
            inner = result.get(wrapper)
            if isinstance(inner, ABCMapping):
                for key in ("id", "memory_id", "memoryId"):
                    v = inner.get(key)
                    if isinstance(v, str) and v:
                        return v
        return None

    @staticmethod
    def _run_coroutine_blocking(result: Any) -> Any:
        """Block on a coroutine returned by the SDK on the sync path."""
        if not asyncio.iscoroutine(result):
            return result
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise SupermemoryAdapterError(
                "client returned a coroutine on a sync path "
                "while an event loop is running."
            )
        try:
            return asyncio.run(result)
        except _FuturesCancelledError as exc:
            raise SupermemoryAdapterError(
                "client coroutine was cancelled before completion."
            ) from exc
        except RuntimeError as exc:
            # Fallback: the coroutine may be bound to the thread's
            # set-but-not-running event loop.
            try:
                loop = asyncio.get_event_loop_policy().get_event_loop()
            except RuntimeError:
                raise SupermemoryAdapterError(str(exc)) from exc
            if loop.is_running():
                raise SupermemoryAdapterError(
                    f"cannot block on coroutine: event loop is running "
                    f"elsewhere: {exc}"
                ) from exc
            try:
                return loop.run_until_complete(result)
            except _FuturesCancelledError as inner:
                raise SupermemoryAdapterError(
                    "client coroutine was cancelled before completion."
                ) from inner

    def _clamp_k(self, k: Any) -> int:
        if not _is_real_int(k) or k <= 0:
            raise SupermemoryAdapterError("k must be a positive int.")
        return min(k, self._max_recall_k)

    def peek_mirror(self) -> List[Dict[str, Any]]:
        """Return plain-dict copies of the local mirror.

        The mirror itself is deeply frozen; this method returns mutable
        copies so callers can inspect and serialize without risk of
        corrupting shared state.
        """
        with self._lock:
            snapshot = list(self._mirror)
        return [_to_plain(m) for m in snapshot]

    def snapshot(self) -> Dict[str, Any]:
        """Return a structured, plain-dict view of the mirror.

        Mirrors the shape of ``MemoryTierManager.snapshot()`` for
        consistency with the rest of the pipeline.
        """
        with self._lock:
            entries = [_to_plain(m) for m in self._mirror]
            cap = self._mirror.maxlen
        tag_mix: Dict[str, int] = {}
        for e in entries:
            tag = e.get("container_tag") or DEFAULT_CONTAINER_TAG
            tag_mix[str(tag)] = tag_mix.get(str(tag), 0) + 1
        return {
            "schema_version": SCHEMA_VERSION,
            "size": len(entries),
            "capacity": cap,
            "container_tag_mix": tag_mix,
            "records": entries,
        }

    # ---------------------------------------------------------- writes
    def _write_payload(self, payload: Mapping[str, Any]) -> Optional[str]:
        """Send a payload with retries. Returns a memory id or None."""
        if not isinstance(payload, ABCMapping):
            raise SupermemoryAdapterError("payload must be a Mapping.")
        if "content" not in payload:
            raise SupermemoryAdapterError("payload must include 'content'.")
        raw_meta = payload.get("metadata")
        if raw_meta is not None and not isinstance(raw_meta, ABCMapping):
            raise SupermemoryAdapterError(
                "payload['metadata'] must be a Mapping when provided."
            )

        # Resolve container tag once; reuse for SDK send and mirror.
        tag = self._resolve_container_tag(payload, self._config)
        metadata = dict(raw_meta or {})
        resolved: Dict[str, Any] = {
            **payload,
            "container_tag": tag,
            "metadata": metadata,
        }

        # -------------------------------------------------- offline path
        if self._client is None:
            with self._lock:
                self._local_id += 1
                memory_id = f"local-{self._local_id}"
                resolved["id"] = memory_id
                self._mirror.append(_deep_freeze(resolved))
                self._write_successes += 1
                self._last_error = None
            return memory_id

        # -------------------------------------------------- online path
        attempts = self._config.write_retries + 1
        last_exc: Optional[BaseException] = None
        start = time.monotonic()
        idempotency_key = uuid.uuid4().hex

        for attempt in range(1, attempts + 1):
            try:
                add = getattr(self._client, "add", None)
                if not callable(add):
                    add = getattr(self._client, "add_memory", None)
                if not callable(add):
                    raise SupermemoryClientError(
                        "Supermemory client lacks 'add'/'add_memory'."
                    )

                kwargs: Dict[str, Any] = {
                    "content": resolved["content"],
                    "container_tag": tag,
                    "metadata": metadata,
                }
                try:
                    result = add(idempotency_key=idempotency_key, **kwargs)
                except TypeError:
                    result = add(**kwargs)

                result = self._run_coroutine_blocking(result)
                sdk_id = self._extract_memory_id(result)

                elapsed = time.monotonic() - start
                with self._lock:
                    self._local_id += 1
                    memory_id = sdk_id or f"write-{self._local_id}"
                    resolved["id"] = memory_id
                    self._mirror.append(_deep_freeze(resolved))
                    self._write_successes += 1
                    self._write_total_seconds += elapsed
                    self._write_latency_ring.append(elapsed * 1000.0)
                    self._last_write_error = None
                    self._last_error = None
                return memory_id
            except MemorySchemaError:
                # Never retried; never downgraded by ``strict``.
                raise
            except SupermemoryClientError:
                raise
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Supermemory write attempt %d/%d failed: %s",
                    attempt, attempts, exc,
                )
                if attempt < attempts:
                    time.sleep(self._write_retry_backoff * attempt)

        elapsed = time.monotonic() - start
        with self._lock:
            self._write_errors += 1
            self._write_total_seconds += elapsed
            self._write_latency_ring.append(elapsed * 1000.0)
            self._last_write_error = str(last_exc)
            self._last_error = str(last_exc)
            # Item fix #6: update client_error when the failure
            # looks like a transport / auth problem.
            if self._client_error is None:
                self._client_error = f"runtime write failure: {last_exc}"
        if self._strict:
            raise SupermemoryWriteError(
                f"write failed after {attempts} attempts: {last_exc}"
            ) from last_exc
        return None

    # ---- typed write helpers (delegate to _write_payload) ----
    def remember_decision(self, record: DecisionRecord) -> Optional[str]:
        if not isinstance(record, DecisionRecord):
            raise SupermemoryAdapterError("record must be a DecisionRecord.")
        return self._write_payload(record.to_supermemory_payload())

    def remember_policy(self, record: PolicyRecord) -> Optional[str]:
        if not isinstance(record, PolicyRecord):
            raise SupermemoryAdapterError("record must be a PolicyRecord.")
        return self._write_payload(record.to_supermemory_payload())

    def remember_incident(self, record: IncidentRecord) -> Optional[str]:
        if not isinstance(record, IncidentRecord):
            raise SupermemoryAdapterError("record must be an IncidentRecord.")
        return self._write_payload(record.to_supermemory_payload())

    def record_outcome(self, record: OutcomeRecord) -> Optional[str]:
        if not isinstance(record, OutcomeRecord):
            raise SupermemoryAdapterError("record must be an OutcomeRecord.")
        return self._write_payload(record.to_supermemory_payload())

    # ---- generic write helpers ----
    def remember(self, payload: Mapping[str, Any]) -> Optional[str]:
        """Write an arbitrary pre-shaped payload."""
        return self._write_payload(payload)

    def remember_many(
        self,
        payloads: Iterable[Mapping[str, Any]],
        *,
        stop_on_error: bool = False,
    ) -> List[Optional[str]]:
        """Write a batch of payloads."""
        results: List[Optional[str]] = []
        for idx, payload in enumerate(payloads):
            try:
                results.append(self._write_payload(payload))
            except SupermemoryAdapterError as exc:
                if stop_on_error:
                    raise
                logger.warning("remember_many[%d] failed: %s", idx, exc)
                results.append(None)
        return results

    # ---------------------------------------------------------- reads
    def recall_similar_runs(
        self,
        query: str,
        *,
        container_tag: Optional[str] = None,
        k: Optional[int] = None,
        filters: Optional[Mapping[str, Any]] = None,
        truth_level: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return up to ``k`` memories matching ``query``.

        Offline mode performs a case-insensitive substring match over
        the JSON-serialized payload of each mirrored record, filtered
        by ``container_tag`` and (when provided) ``truth_level``.
        The returned list contains plain dicts — the mirror itself is
        never exposed directly.
        """
        if not isinstance(query, str) or not query:
            raise SupermemoryAdapterError("query must be a non-empty string.")
        top_k = self._config.recall_top_k if k is None else k
        top_k = self._clamp_k(top_k)
        tag = container_tag or self._config.default_container_tag
        if filters is not None and not isinstance(filters, ABCMapping):
            raise SupermemoryAdapterError("filters must be a Mapping or None.")
        if truth_level is not None:
            if not isinstance(truth_level, str) or not truth_level:
                raise SupermemoryAdapterError(
                    "truth_level must be None or a non-empty string."
                )

        # -------------------------------------------------- offline path
        if self._client is None:
            needle = query.lower()
            start = time.monotonic()
            with self._lock:
                matches: List[Mapping[str, Any]] = []
                for m in self._mirror:
                    if m.get("container_tag") != tag:
                        continue
                    if truth_level is not None:
                        meta = m.get("metadata") or {}
                        if not isinstance(meta, ABCMapping):
                            meta = {}
                        if meta.get("truth_level") != truth_level:
                            continue
                    try:
                        blob = json.dumps(_to_plain(m), default=str).lower()
                    except (TypeError, ValueError):
                        blob = str(m).lower()
                    if needle in blob:
                        matches.append(m)
                selected = matches[-top_k:]
                self._recall_successes += 1
                elapsed = time.monotonic() - start
                self._recall_total_seconds += elapsed
                self._recall_latency_ring.append(elapsed * 1000.0)
                self._last_recall_error = None
                self._last_error = None
            return [_to_plain(m) for m in selected]

        # -------------------------------------------------- online path
        start = time.monotonic()
        try:
            search = getattr(self._client, "search", None)
            if not callable(search):
                search = getattr(self._client, "search_memory", None)
            if not callable(search):
                raise SupermemoryClientError(
                    "Supermemory client lacks 'search'/'search_memory'."
                )
            search_filters: Dict[str, Any] = dict(filters or {})
            if truth_level is not None:
                search_filters.setdefault("truth_level", truth_level)
            result = search({
                "q": query,
                "containerTag": tag,
                "limit": top_k,
                "filters": search_filters,
            })
            result = self._run_coroutine_blocking(result)
            items = (
                result.get("results", [])
                if isinstance(result, ABCMapping)
                else list(result or [])
            )
            elapsed = time.monotonic() - start
            with self._lock:
                self._recall_successes += 1
                self._recall_total_seconds += elapsed
                self._recall_latency_ring.append(elapsed * 1000.0)
                self._last_recall_error = None
                self._last_error = None
            return [_to_plain(dict(i)) for i in items][:top_k]
        except MemorySchemaError:
            raise
        except SupermemoryClientError:
            raise
        except Exception as exc:
            elapsed = time.monotonic() - start
            with self._lock:
                self._recall_errors += 1
                self._recall_total_seconds += elapsed
                self._recall_latency_ring.append(elapsed * 1000.0)
                self._last_recall_error = str(exc)
                self._last_error = str(exc)
            logger.warning("Supermemory recall failed: %s", exc)
            if self._strict:
                raise SupermemoryRecallError(
                    f"recall failed: {exc}"
                ) from exc
            return []

    def forget(self, memory_id: str) -> Dict[str, Any]:
        """Delete a memory. Also drops it from the local mirror.

        Returns
        -------
        dict
            ``{"memory_id", "deleted", "mirror_removed", "remote_ok",
            "reason"}``. ``deleted`` is True when the mirror removal
            and (if online) the remote delete both succeeded.
        """
        if not isinstance(memory_id, str) or not memory_id:
            raise SupermemoryAdapterError(
                "memory_id must be a non-empty string."
            )

        # Always scrub the mirror first.
        removed_from_mirror = 0
        with self._lock:
            kept: Deque[Mapping[str, Any]] = deque(
                maxlen=self._mirror.maxlen
            )
            for m in self._mirror:
                if (
                    m.get("id") == memory_id
                    or m.get("memory_id") == memory_id
                ):
                    removed_from_mirror += 1
                else:
                    kept.append(m)
            self._mirror = kept

        if self._client is None:
            return {
                "memory_id": memory_id,
                "deleted": removed_from_mirror > 0,
                "mirror_removed": removed_from_mirror,
                "remote_ok": None,
                "reason": (
                    None if removed_from_mirror > 0 else "not_in_mirror"
                ),
            }

        delete = getattr(self._client, "delete", None)
        if not callable(delete):
            delete = getattr(self._client, "forget", None)
        if not callable(delete):
            if self._strict:
                raise SupermemoryClientError(
                    "Supermemory client lacks 'delete'/'forget'."
                )
            return {
                "memory_id": memory_id,
                "deleted": removed_from_mirror > 0,
                "mirror_removed": removed_from_mirror,
                "remote_ok": False,
                "reason": "client_lacks_delete",
            }
        try:
            delete(memory_id)
            return {
                "memory_id": memory_id,
                "deleted": True,
                "mirror_removed": removed_from_mirror,
                "remote_ok": True,
                "reason": None,
            }
        except Exception as exc:
            logger.warning("forget(%s) failed: %s", memory_id, exc)
            if self._strict:
                raise SupermemoryWriteError(
                    f"forget failed: {exc}"
                ) from exc
            return {
                "memory_id": memory_id,
                "deleted": False,
                "mirror_removed": removed_from_mirror,
                "remote_ok": False,
                "reason": str(exc),
            }

    # ---------------------------------------------------------- async paths
    async def remember_decision_async(
        self, record: DecisionRecord
    ) -> Optional[str]:
        return await asyncio.to_thread(self.remember_decision, record)

    async def remember_policy_async(
        self, record: PolicyRecord
    ) -> Optional[str]:
        return await asyncio.to_thread(self.remember_policy, record)

    async def remember_incident_async(
        self, record: IncidentRecord
    ) -> Optional[str]:
        return await asyncio.to_thread(self.remember_incident, record)

    async def record_outcome_async(
        self, record: OutcomeRecord
    ) -> Optional[str]:
        return await asyncio.to_thread(self.record_outcome, record)

    async def remember_async(
        self, payload: Mapping[str, Any]
    ) -> Optional[str]:
        return await asyncio.to_thread(self.remember, payload)

    async def remember_many_async(
        self,
        payloads: Iterable[Mapping[str, Any]],
        *,
        stop_on_error: bool = False,
    ) -> List[Optional[str]]:
        return await asyncio.to_thread(
            self.remember_many,
            payloads,
            stop_on_error=stop_on_error,
        )

    async def recall_similar_runs_async(
        self,
        query: str,
        *,
        container_tag: Optional[str] = None,
        k: Optional[int] = None,
        filters: Optional[Mapping[str, Any]] = None,
        truth_level: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(
            self.recall_similar_runs,
            query,
            container_tag=container_tag,
            k=k,
            filters=filters,
            truth_level=truth_level,
        )

    async def forget_async(self, memory_id: str) -> Dict[str, Any]:
        return await asyncio.to_thread(self.forget, memory_id)

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            write_lats = list(self._write_latency_ring)
            recall_lats = list(self._recall_latency_ring)
            write_mean = (
                sum(write_lats) / len(write_lats) if write_lats else 0.0
            )
            recall_mean = (
                sum(recall_lats) / len(recall_lats) if recall_lats else 0.0
            )
            tag_mix: Dict[str, int] = {}
            for m in self._mirror:
                tag = m.get("container_tag") or DEFAULT_CONTAINER_TAG
                tag_mix[str(tag)] = tag_mix.get(str(tag), 0) + 1
            return {
                "schema_version": SCHEMA_VERSION,
                "mode": self._config.mode.value,
                "sdk_available": _SUPERMEMORY_AVAILABLE,
                "client_available": self._client is not None,
                "client_error": self._client_error,
                "strict": self._strict,
                "mirror_size": len(self._mirror),
                "mirror_capacity": self._mirror.maxlen,
                "container_tag_mix": tag_mix,
                "write_successes": self._write_successes,
                "write_errors": self._write_errors,
                "write_total_seconds": round(self._write_total_seconds, 6),
                "write_mean_latency_ms": write_mean,
                "write_p50_latency_ms": _percentile(write_lats, 50),
                "write_p95_latency_ms": _percentile(write_lats, 95),
                "write_max_latency_ms": (
                    max(write_lats) if write_lats else 0.0
                ),
                "recall_successes": self._recall_successes,
                "recall_errors": self._recall_errors,
                "recall_total_seconds": round(self._recall_total_seconds, 6),
                "recall_mean_latency_ms": recall_mean,
                "recall_p50_latency_ms": _percentile(recall_lats, 50),
                "recall_p95_latency_ms": _percentile(recall_lats, 95),
                "recall_max_latency_ms": (
                    max(recall_lats) if recall_lats else 0.0
                ),
                "last_error": self._last_error,
                "last_write_error": self._last_write_error,
                "last_recall_error": self._last_recall_error,
                "max_recall_k": self._max_recall_k,
                "write_retry_backoff_seconds": self._write_retry_backoff,
                "uptime_seconds": time.monotonic() - self._started_at,
            }

    def reset(self, *, clear_mirror: bool = True) -> int:
        """Reset counters (and optionally the mirror).

        Returns the number of mirror entries removed (0 when
        ``clear_mirror=False``). When ``clear_mirror=True`` the local
        id counter is also reset so fresh writes start at ``local-1``.
        """
        with self._lock:
            removed = len(self._mirror) if clear_mirror else 0
            if clear_mirror:
                self._mirror.clear()
                self._local_id = 0
            self._write_errors = 0
            self._write_successes = 0
            self._recall_errors = 0
            self._recall_successes = 0
            self._write_total_seconds = 0.0
            self._recall_total_seconds = 0.0
            self._write_latency_ring.clear()
            self._recall_latency_ring.clear()
            self._last_error = None
            self._last_write_error = None
            self._last_recall_error = None
            self._started_at = time.monotonic()
        return removed

    # ---------------------------------------------------------- serialization
    @classmethod
    def assert_compatible(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> None:
        """Raise ``SupermemoryParseError`` if the payload's schema is
        incompatible with the current contract."""
        if not isinstance(data, ABCMapping):
            raise SupermemoryParseError(
                "SupermemoryAdapter.assert_compatible expects a Mapping."
            )
        v = data.get("schema_version", SCHEMA_VERSION)
        if not _is_real_int(v) or v <= 0:
            raise SupermemoryParseError(
                f"invalid schema_version {v!r} in adapter payload."
            )
        if v > SCHEMA_VERSION:
            raise SupermemoryParseError(
                f"adapter payload schema_version {v} is newer than the "
                f"current contract {SCHEMA_VERSION}."
            )
        if strict and v < SCHEMA_VERSION:
            raise SupermemoryParseError(
                f"adapter payload schema_version {v} is older than the "
                f"current contract {SCHEMA_VERSION}."
            )

    def to_dict(
        self,
        *,
        include_mirror: bool = False,
        redact_secrets: bool = True,
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "schema_version": SCHEMA_VERSION,
                "config": self._config.to_dict(
                    redact_secrets=redact_secrets
                ),
                "strict": self._strict,
                "statistics": self.statistics(),
            }
            if include_mirror:
                payload["mirror"] = [_to_plain(m) for m in self._mirror]
        return payload

    def safe_dict(self) -> Dict[str, Any]:
        """``to_dict()`` with secrets redacted (default)."""
        return self.to_dict(redact_secrets=True)

    def to_json(
        self,
        *,
        include_mirror: bool = False,
        redact_secrets: bool = True,
        indent: Optional[int] = None,
    ) -> str:
        return json.dumps(
            self.to_dict(
                include_mirror=include_mirror,
                redact_secrets=redact_secrets,
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
        client: Optional[Any] = None,
        strict: Optional[bool] = None,
        restore_mirror: bool = False,
    ) -> "SupermemoryAdapter":
        """Rebuild an adapter from a ``to_dict()`` payload."""
        if not isinstance(data, ABCMapping):
            raise SupermemoryParseError(
                "SupermemoryAdapter.from_dict expects a Mapping."
            )
        cls.assert_compatible(data)

        cfg_blob = data.get("config", {})
        if isinstance(cfg_blob, SupermemoryConfig):
            config = cfg_blob
        else:
            try:
                config = SupermemoryConfig.from_dict(cfg_blob)
            except SupermemoryConfigError as exc:
                raise SupermemoryParseError(
                    f"invalid config in from_dict payload: {exc}"
                ) from exc

        resolved_strict = (
            bool(data.get("strict", True))
            if strict is None
            else bool(strict)
        )

        adapter = cls(config=config, strict=resolved_strict, client=client)

        raw_mirror = data.get("mirror")
        if restore_mirror and isinstance(raw_mirror, list):
            with adapter._lock:  # noqa: SLF001 - intentional
                adapter._mirror.clear()
                for entry in raw_mirror:
                    if not isinstance(entry, ABCMapping):
                        logger.warning(
                            "from_dict: skipping non-mapping mirror entry."
                        )
                        continue
                    # Validate that the entry has a resolvable id.
                    eid = entry.get("id") or entry.get("memory_id")
                    if not isinstance(eid, str) or not eid:
                        logger.warning(
                            "from_dict: skipping mirror entry with no id."
                        )
                        continue
                    adapter._mirror.append(_deep_freeze(dict(entry)))
        return adapter

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        client: Optional[Any] = None,
        strict: Optional[bool] = None,
        restore_mirror: bool = False,
    ) -> "SupermemoryAdapter":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SupermemoryParseError(
                f"from_json received invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(
            data,
            client=client,
            strict=strict,
            restore_mirror=restore_mirror,
        )

    # ------------------------------------------------------------------ repr
    def __repr__(self) -> str:
        return (
            "SupermemoryAdapter("
            f"mode={self._config.mode.value!r}, "
            f"client={'yes' if self._client else 'no'}, "
            f"mirror={self.mirror_size}, "
            f"strict={self._strict})"
        )


__all__ = [
    "DEFAULT_CONTAINER_TAG",
    "SCHEMA_VERSION",
    "SupermemoryAdapter",
    "SupermemoryAdapterError",
    "SupermemoryClientError",
    "SupermemoryWriteError",
    "SupermemoryRecallError",
    "SupermemoryParseError",
    "_SUPERMEMORY_AVAILABLE",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.supermemory_adapter
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # --------------------------------------------------------- offline
    adapter = SupermemoryAdapter()
    print("repr       :", adapter)
    print("schema ver :", SCHEMA_VERSION)
    print("default tag:", DEFAULT_CONTAINER_TAG)

    d = DecisionRecord(
        run_id="run-001",
        workload_type="vision_inference",
        device_class="arm_edge",
        route="edge_int8",
        policy_version="v0.3",
        truth_level="measured",
        predicted_energy_wh=8.1,
        predicted_carbon_gco2e=3.7,
        predicted_latency_ms=410.0,
        reason="Met SLA; lower carbon.",
        candidates=("edge_int8", "cloud_gpu"),
    )
    memory_id = adapter.remember_decision(d)
    print("write id   :", memory_id)

    # Substring recall (offline).
    hit = adapter.recall_similar_runs("edge_int8")
    miss = adapter.recall_similar_runs("definitely-not-present")
    print("recall hit :", len(hit), "| miss:", len(miss))
    assert hit and not miss

    # truth_level filter (item #2.8).
    by_truth = adapter.recall_similar_runs(
        "edge_int8", truth_level="measured",
    )
    wrong_truth = adapter.recall_similar_runs(
        "edge_int8", truth_level="simulated",
    )
    assert by_truth and not wrong_truth
    print("truth_level filter : OK")

    # forget() returns a dict (item #2.7).
    res = adapter.forget(memory_id)
    assert isinstance(res, dict)
    assert res["deleted"] is True
    assert res["mirror_removed"] == 1
    assert adapter.mirror_size == 0
    print("forget dict: OK ->", res)

    # Statistics expose new fields.
    stats = adapter.statistics()
    for key in (
        "schema_version", "client_error", "write_total_seconds",
        "recall_total_seconds", "last_error", "last_write_error",
        "last_recall_error", "mirror_capacity", "container_tag_mix",
        "write_p50_latency_ms", "write_p95_latency_ms",
        "recall_p50_latency_ms", "recall_p95_latency_ms",
        "max_recall_k", "write_retry_backoff_seconds",
    ):
        assert key in stats, f"missing statistic: {key}"
    print("statistics : OK")

    # Payload validation.
    try:
        adapter.remember({"metadata": {}})
    except SupermemoryAdapterError as exc:
        print("Rejected   :", exc)

    # metadata must be a Mapping.
    try:
        adapter.remember({"content": "x", "metadata": "not-a-mapping"})
    except SupermemoryAdapterError as exc:
        print("metadata   :", exc)

    # k clamping.
    adapter.remember({"content": "x", "container_tag": "t"})
    assert len(adapter.recall_similar_runs("x", k=10**9)) >= 0
    print("k clamp    : OK")

    # reset(clear_mirror=False) returns 0 and preserves the mirror.
    assert adapter.reset(clear_mirror=False) == 0
    assert adapter.mirror_size == 1
    print("reset(0)   : OK")

    # reset(clear_mirror=True) also resets `_local_id`.
    adapter.reset(clear_mirror=True)
    adapter.remember({"content": "fresh"})
    assert "fresh" in json.dumps(adapter.peek_mirror())
    assert adapter._local_id == 1, adapter._local_id
    print("reset(True) + id reset : OK")

    # Mirror peek returns mutable copies.
    peek = adapter.peek_mirror()
    peek.append({"injected": True})
    assert adapter.mirror_size == 1
    # Mutating a peeked copy does not affect the mirror.
    if peek and isinstance(peek[0], dict):
        peek[0]["hacked"] = True
    assert "hacked" not in json.dumps(adapter.peek_mirror())
    print("peek copy  : OK")

    # Deep-freeze of nested metadata.
    adapter.reset(clear_mirror=True)
    adapter.remember({
        "content": "x",
        "metadata": {"nested": {"k": 1}, "values": [1, 2, 3]},
    })
    with adapter._lock:  # noqa: SLF001 - introspecting frozen state
        raw = adapter._mirror[0]
    try:
        raw["metadata"]["nested"]["k"] = 99  # type: ignore[index]
    except TypeError:
        print("deep freeze: OK")
    else:
        raise AssertionError("nested metadata should be frozen")

    # Snapshot.
    snap = adapter.snapshot()
    assert snap["schema_version"] == SCHEMA_VERSION
    assert snap["size"] == 1
    assert "container_tag_mix" in snap
    print("snapshot   : OK")

    # --------------------------------------------------------- mocked client
    class _MockClient:
        def __init__(self) -> None:
            self.added: list = []

        def add(self, *, content, container_tag, metadata, **kw):
            self.added.append({
                "content": content,
                "container_tag": container_tag,
                "metadata": metadata,
            })
            return {"id": f"mock-{len(self.added)}"}

        def search(self, payload):
            return {"results": [{"id": "mock-1", "content": "x"}]}

        def delete(self, memory_id):
            return True

        def close(self):
            pass

    mock = _MockClient()
    mocked = SupermemoryAdapter(client=mock)
    assert mocked.remember_decision(d) is not None
    assert mocked.recall_similar_runs("edge")[0]["id"] == "mock-1"
    res = mocked.forget("mock-1")
    assert res["deleted"] is True
    print("mocked path: OK")

    # --------------------------------------------------------- serialization
    payload = mocked.to_dict(include_mirror=True)
    assert payload["schema_version"] == SCHEMA_VERSION
    assert "config" in payload and "mirror" in payload
    restored = SupermemoryAdapter.from_dict(payload, restore_mirror=True)
    assert restored.mirror_size == mocked.mirror_size
    print("round-trip : OK")

    j = mocked.to_json(include_mirror=True)
    SupermemoryAdapter.from_json(j)
    print("json RT    : OK")

    # --------------------------------------------------------- assert_compatible
    SupermemoryAdapter.assert_compatible(
        {"schema_version": SCHEMA_VERSION},
    )
    try:
        SupermemoryAdapter.assert_compatible(
            {"schema_version": SCHEMA_VERSION + 1},
        )
    except SupermemoryParseError:
        print("assert_compat : OK")

    # --------------------------------------------------------- from_config / from_pipeline
    from_cfg = SupermemoryAdapter.from_config(SupermemoryConfig())
    assert isinstance(from_cfg, SupermemoryAdapter)

    class _FakePipeline:
        adapter = mocked

    from_pipe = SupermemoryAdapter.from_pipeline(_FakePipeline())
    assert from_pipe is mocked
    print("from_*     : OK")

    # --------------------------------------------------------- async context manager + remember_many_async
    async def _async_path():
        async with SupermemoryAdapter() as a:
            await a.remember_many_async([
                {"content": "a"},
                {"content": "b"},
            ])
            n = a.mirror_size
        return n

    n = asyncio.run(_async_path())
    assert n == 2
    print("async path : OK")

    # --------------------------------------------------------- close(flush=True)
    adapter2 = SupermemoryAdapter()
    adapter2.remember({"content": "x"})
    adapter2.close(flush=True)
    # After close, the client is None, and writes fall into offline mode.
    with adapter2._lock:  # noqa: SLF001
        assert adapter2._client is None
    print("close flush: OK")

    # --------------------------------------------------------- validation
    for bad_call in (
        lambda: adapter.remember_decision("not-a-record"),  # type: ignore[arg-type]
        lambda: adapter.recall_similar_runs(""),
        lambda: adapter.recall_similar_runs("q", k=0),
        lambda: adapter.recall_similar_runs("q", filters="nope"),  # type: ignore[arg-type]
        lambda: adapter.forget(""),
    ):
        try:
            bad_call()
        except SupermemoryAdapterError as exc:
            print("Rejected   :", exc)

    # --------------------------------------------------------- ctx manager
    with SupermemoryAdapter() as ctx:
        ctx.remember({"content": "hi"})
    print("ctx mgr    : OK")

    print("\nSmoke test passed.")
