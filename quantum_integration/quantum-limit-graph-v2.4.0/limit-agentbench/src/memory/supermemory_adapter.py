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
- ``remember(payload)``               (generic)
- ``remember_many(payloads)``         (generic, batch)
- ``recall_similar_runs(query, k=5)``
- ``forget(memory_id)``
- ``peek_mirror()``
- ``statistics()``
- ``reset(clear_mirror=True)``
- ``close()`` / context-manager protocol
- Async siblings (``remember_decision_async``, ``recall_similar_runs_async``)

Enhancements
------------
- Defensive ``import supermemory`` with ``_SUPERMEMORY_AVAILABLE`` flag.
- ``RLock``-guarded state and bounded local mirror of writes.
- Monotonic local memory IDs (no more collisions once the mirror wraps).
- Substring search in the offline mirror; respects ``container_tag``.
- Payload validation (``content`` required) and ``container_tag``
  resolution to the config default before send/mirror.
- ``forget()`` removes matching entries from the local mirror.
- ``k`` clamped to a sane upper bound; non-strict failures logged with
  their cause and recorded on ``statistics()``.
- Strict / non-strict modes, with schema errors always raised.
- Retry with linear backoff on writes.
- Structured error hierarchy:
  ``SupermemoryAdapterError`` → ``SupermemoryClientError``,
  ``SupermemoryWriteError``, ``SupermemoryRecallError``.
- Latency and last-error observability in ``statistics()``.
- Full serialization: ``to_dict`` / ``to_json`` / ``from_dict`` /
  ``from_json``.
- ``__main__`` smoke test that mocks the client.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
import uuid
from collections import deque
from collections.abc import Iterable, Mapping as ABCMapping
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

__version__ = "6.0.0"

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
    """Base class for adapter problems.

    Inherits from ``ValueError`` for backwards compatibility with the
    previous release.
    """


class SupermemoryClientError(SupermemoryAdapterError):
    """Client construction or availability problems."""


class SupermemoryWriteError(SupermemoryAdapterError):
    """Failure while writing a memory."""


class SupermemoryRecallError(SupermemoryAdapterError):
    """Failure while recalling memories."""


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

    #: Upper bound applied to ``recall_similar_runs(k=...)``.
    _MAX_RECALL_K: int = 1_000

    def __init__(
        self,
        *,
        config: Optional[SupermemoryConfig] = None,
        strict: bool = True,
        client: Optional[Any] = None,
    ) -> None:
        self._config = config or SupermemoryConfig()
        self._strict = bool(strict)

        # ----- All mutable state lives below; every access is guarded by
        # ----- ``self._lock`` (RLock). Keep it that way in future edits.
        self._lock = threading.RLock()
        self._mirror: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_history
        )
        self._local_id: int = 0
        self._write_errors: int = 0
        self._write_successes: int = 0
        self._recall_errors: int = 0
        self._recall_successes: int = 0
        self._write_total_seconds: float = 0.0
        self._recall_total_seconds: float = 0.0
        self._last_write_error: Optional[str] = None
        self._last_recall_error: Optional[str] = None
        self._started_at: float = time.time()

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

    # ------------------------------------------------------------- container
    def __len__(self) -> int:
        return self.mirror_size

    def __contains__(self, memory_id: object) -> bool:
        if not isinstance(memory_id, str):
            return False
        with self._lock:
            return any(
                m.get("id") == memory_id or m.get("memory_id") == memory_id
                for m in self._mirror
            )

    # ------------------------------------------------------------ lifecycle
    def close(self) -> None:
        """Best-effort client shutdown. Safe to call multiple times."""
        client = self._client
        if client is None:
            return
        close = getattr(client, "close", None)
        if callable(close):
            try:
                close()
            except Exception as exc:  # pragma: no cover - SDK dependent
                logger.warning("client.close() failed: %s", exc)

    def __enter__(self) -> "SupermemoryAdapter":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ---------------------------------------------------------- helpers
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
        if isinstance(result, ABCMapping):
            for key in ("id", "memory_id", "memoryId"):
                v = result.get(key)
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
            return asyncio.run(result)
        raise SupermemoryAdapterError(
            "client returned a coroutine on a sync path "
            "while an event loop is running."
        )

    @classmethod
    def _clamp_k(cls, k: Any) -> int:
        if not isinstance(k, int) or isinstance(k, bool) or k <= 0:
            raise SupermemoryAdapterError("k must be a positive int.")
        return min(k, cls._MAX_RECALL_K)

    def peek_mirror(self) -> List[Dict[str, Any]]:
        """Return a shallow copy of the local mirror (thread-safe)."""
        with self._lock:
            return [dict(m) for m in self._mirror]

    # ---------------------------------------------------------- writes
    def _write_payload(self, payload: Mapping[str, Any]) -> Optional[str]:
        """Send a payload with retries. Returns a memory id or None."""
        if not isinstance(payload, ABCMapping):
            raise SupermemoryAdapterError("payload must be a Mapping.")
        if "content" not in payload:
            raise SupermemoryAdapterError("payload must include 'content'.")

        # Resolve container tag once; reuse for SDK send and mirror.
        tag = self._resolve_container_tag(payload, self._config)
        metadata = dict(payload.get("metadata") or {})
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
                self._mirror.append(resolved)
                self._write_successes += 1
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

                # The SDK may accept ``idempotency_key``; tolerate its
                # absence by inspecting the signature indirectly.
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

                with self._lock:
                    self._local_id += 1
                    memory_id = sdk_id or f"write-{self._local_id}"
                    resolved["id"] = memory_id
                    self._mirror.append(resolved)
                    self._write_successes += 1
                    self._write_total_seconds += time.monotonic() - start
                    self._last_write_error = None
                return memory_id
            except MemorySchemaError:
                # Never retried; never downgraded by ``strict``.
                raise
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Supermemory write attempt %d/%d failed: %s",
                    attempt, attempts, exc,
                )
                if attempt < attempts:
                    time.sleep(0.25 * attempt)

        with self._lock:
            self._write_errors += 1
            self._write_total_seconds += time.monotonic() - start
            self._last_write_error = str(last_exc)
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
        """Write an arbitrary pre-shaped payload.

        The mapping must include ``content``; other keys are passed
        through to the SDK (``container_tag``, ``metadata``).
        """
        return self._write_payload(payload)

    def remember_many(
        self,
        payloads: Iterable[Mapping[str, Any]],
        *,
        stop_on_error: bool = False,
    ) -> List[Optional[str]]:
        """Write a batch of payloads.

        Parameters
        ----------
        payloads : Iterable[Mapping]
            Payloads to write.
        stop_on_error : bool, default False
            If True, propagate the first failure. If False, keep going
            and return ``None`` for the failed entries.
        """
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
    ) -> List[Dict[str, Any]]:
        """Return up to ``k`` memories matching ``query``.

        Offline mode performs a case-insensitive substring match over
        the JSON-serialized payload of each mirrored record, filtered
        by ``container_tag``.
        """
        if not isinstance(query, str) or not query:
            raise SupermemoryAdapterError("query must be a non-empty string.")
        top_k = self._config.recall_top_k if k is None else k
        top_k = self._clamp_k(top_k)
        tag = container_tag or self._config.default_container_tag

        # -------------------------------------------------- offline path
        if self._client is None:
            needle = query.lower()
            start = time.monotonic()
            with self._lock:
                matches: List[Dict[str, Any]] = []
                for m in self._mirror:
                    if m.get("container_tag") != tag:
                        continue
                    try:
                        blob = json.dumps(m, default=str).lower()
                    except (TypeError, ValueError):
                        blob = str(m).lower()
                    if needle in blob:
                        matches.append(dict(m))
                self._recall_successes += 1
                self._recall_total_seconds += time.monotonic() - start
                self._last_recall_error = None
            return matches[-top_k:]

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
            result = search({
                "q": query,
                "containerTag": tag,
                "limit": top_k,
                "filters": dict(filters or {}),
            })
            result = self._run_coroutine_blocking(result)
            items = (
                result.get("results", [])
                if isinstance(result, ABCMapping)
                else list(result or [])
            )
            with self._lock:
                self._recall_successes += 1
                self._recall_total_seconds += time.monotonic() - start
                self._last_recall_error = None
            return [dict(i) for i in items][:top_k]
        except MemorySchemaError:
            raise
        except Exception as exc:
            with self._lock:
                self._recall_errors += 1
                self._recall_total_seconds += time.monotonic() - start
                self._last_recall_error = str(exc)
            logger.warning("Supermemory recall failed: %s", exc)
            if self._strict:
                raise SupermemoryRecallError(
                    f"recall failed: {exc}"
                ) from exc
            return []

    def forget(self, memory_id: str) -> bool:
        """Delete a memory. Also drops it from the local mirror.

        Returns True if the memory was found in the mirror and/or the
        remote delete succeeded; False otherwise.
        """
        if not isinstance(memory_id, str) or not memory_id:
            raise SupermemoryAdapterError(
                "memory_id must be a non-empty string."
            )

        # Always scrub the mirror first.
        removed_from_mirror = 0
        with self._lock:
            kept = deque(maxlen=self._mirror.maxlen)
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
            return removed_from_mirror > 0

        delete = getattr(self._client, "delete", None)
        if not callable(delete):
            delete = getattr(self._client, "forget", None)
        if not callable(delete):
            if self._strict:
                raise SupermemoryClientError(
                    "Supermemory client lacks 'delete'/'forget'."
                )
            return removed_from_mirror > 0
        try:
            delete(memory_id)
            return True
        except Exception as exc:
            logger.warning("forget(%s) failed: %s", memory_id, exc)
            if self._strict:
                raise SupermemoryWriteError(
                    f"forget failed: {exc}"
                ) from exc
            return removed_from_mirror > 0

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

    async def recall_similar_runs_async(
        self,
        query: str,
        *,
        container_tag: Optional[str] = None,
        k: Optional[int] = None,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(
            self.recall_similar_runs,
            query,
            container_tag=container_tag,
            k=k,
            filters=filters,
        )

    async def forget_async(self, memory_id: str) -> bool:
        return await asyncio.to_thread(self.forget, memory_id)

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "mode": self._config.mode.value,
                "sdk_available": _SUPERMEMORY_AVAILABLE,
                "client_available": self._client is not None,
                "client_error": self._client_error,
                "strict": self._strict,
                "mirror_size": len(self._mirror),
                "mirror_capacity": self._mirror.maxlen,
                "write_successes": self._write_successes,
                "write_errors": self._write_errors,
                "write_total_seconds": round(self._write_total_seconds, 6),
                "recall_successes": self._recall_successes,
                "recall_errors": self._recall_errors,
                "recall_total_seconds": round(self._recall_total_seconds, 6),
                "last_write_error": self._last_write_error,
                "last_recall_error": self._last_recall_error,
                "uptime_seconds": time.time() - self._started_at,
            }

    def reset(self, *, clear_mirror: bool = True) -> int:
        """Reset counters (and optionally the mirror).

        Returns the number of mirror entries removed (0 when
        ``clear_mirror=False``).
        """
        with self._lock:
            removed = len(self._mirror) if clear_mirror else 0
            if clear_mirror:
                self._mirror.clear()
            self._write_errors = 0
            self._write_successes = 0
            self._recall_errors = 0
            self._recall_successes = 0
            self._write_total_seconds = 0.0
            self._recall_total_seconds = 0.0
            self._last_write_error = None
            self._last_recall_error = None
            self._started_at = time.time()
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(
        self,
        *,
        include_mirror: bool = False,
        redact_secrets: bool = True,
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(
                    redact_secrets=redact_secrets
                ),
                "strict": self._strict,
                "statistics": self.statistics(),
            }
            if include_mirror:
                payload["mirror"] = [dict(m) for m in self._mirror]
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
        """Rebuild an adapter from a ``to_dict()`` payload.

        Notes
        -----
        - The Supermemory client cannot be serialized. Pass ``client=``
          explicitly to restore a live client, or leave ``None`` to end
          up in offline mode.
        - Counters are not restored; only configuration, ``strict`` and
          (optionally) the mirror contents.
        """
        if not isinstance(data, ABCMapping):
            raise SupermemoryAdapterError(
                "SupermemoryAdapter.from_dict expects a Mapping."
            )

        cfg_blob = data.get("config", {})
        if isinstance(cfg_blob, SupermemoryConfig):
            config = cfg_blob
        else:
            try:
                config = SupermemoryConfig.from_dict(cfg_blob)
            except SupermemoryConfigError as exc:
                raise SupermemoryAdapterError(
                    f"invalid config in from_dict payload: {exc}"
                ) from exc

        resolved_strict = (
            bool(data.get("strict", True))
            if strict is None
            else bool(strict)
        )

        adapter = cls(config=config, strict=resolved_strict, client=client)

        if restore_mirror and isinstance(data.get("mirror"), list):
            with adapter._lock:  # noqa: SLF001 - intentional
                adapter._mirror.clear()
                for entry in data["mirror"]:
                    if isinstance(entry, ABCMapping):
                        adapter._mirror.append(dict(entry))
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
            raise SupermemoryAdapterError(
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
    "SupermemoryAdapter",
    "SupermemoryAdapterError",
    "SupermemoryClientError",
    "SupermemoryWriteError",
    "SupermemoryRecallError",
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

    # Substring recall (offline): should find the record by a keyword in
    # the payload, not by every query.
    hit = adapter.recall_similar_runs("edge_int8")
    miss = adapter.recall_similar_runs("definitely-not-present")
    print("recall hit :", len(hit), "| miss:", len(miss))
    assert hit and not miss

    # forget() scrubs the mirror.
    assert adapter.forget(memory_id)
    assert adapter.mirror_size == 0
    print("forget     : OK")

    # Statistics expose the new fields.
    stats = adapter.statistics()
    for key in (
        "client_error", "write_total_seconds", "recall_total_seconds",
        "last_write_error", "last_recall_error", "mirror_capacity",
    ):
        assert key in stats, f"missing statistic: {key}"
    print("statistics :", stats)

    # Payload validation.
    try:
        adapter.remember({"metadata": {}})
    except SupermemoryAdapterError as exc:
        print("Rejected   :", exc)

    # k clamping.
    adapter.remember({"content": "x", "container_tag": "t"})
    assert len(adapter.recall_similar_runs("x", k=10**9)) >= 0
    print("k clamp    : OK")

    # reset(clear_mirror=False) returns 0.
    assert adapter.reset(clear_mirror=False) == 0
    print("reset(0)   : OK")

    # Mirror peek is a copy.
    peek = adapter.peek_mirror()
    peek.append({"injected": True})
    assert adapter.mirror_size == 1
    print("peek copy  : OK")

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

    mock = _MockClient()
    mocked = SupermemoryAdapter(client=mock)
    assert mocked.remember_decision(d) is not None
    assert mocked.recall_similar_runs("edge")[0]["id"] == "mock-1"
    assert mocked.forget("mock-1") is True
    print("mocked path: OK")

    # --------------------------------------------------------- serialization
    payload = mocked.to_dict(include_mirror=True)
    assert "config" in payload and "mirror" in payload
    restored = SupermemoryAdapter.from_dict(payload, restore_mirror=True)
    assert restored.mirror_size == mocked.mirror_size
    print("round-trip : OK")

    j = mocked.to_json(include_mirror=True)
    SupermemoryAdapter.from_json(j)
    print("json RT    : OK")

    # --------------------------------------------------------- validation
    for bad_call in (
        lambda: adapter.remember_decision("not-a-record"),  # type: ignore[arg-type]
        lambda: adapter.recall_similar_runs(""),
        lambda: adapter.recall_similar_runs("q", k=0),
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
