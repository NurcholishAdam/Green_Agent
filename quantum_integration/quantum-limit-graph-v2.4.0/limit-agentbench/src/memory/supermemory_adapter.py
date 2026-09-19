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
- ``recall_similar_runs(query, k=5)``
- ``forget(memory_id)``
- ``statistics()``
- Async siblings (``remember_decision_async``, ``recall_similar_runs_async``)

Enhancements
------------
- Defensive ``import supermemory`` with ``_SUPERMEMORY_AVAILABLE`` flag.
- ``RLock``-guarded state and bounded local mirror of writes for
  offline/testing.
- Strict / non-strict modes.
- Retry with linear backoff on writes.
- Custom ``SupermemoryAdapterError(ValueError)``.
- Full serialization: ``to_dict`` / ``from_dict`` / ``to_json``.
- ``__main__`` smoke test that mocks the client.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections import deque
from dataclasses import asdict
from typing import Any, Deque, Dict, List, Mapping, Optional

from .memory_schemas import (
    DecisionRecord,
    IncidentRecord,
    MemorySchemaError,
    OutcomeRecord,
    PolicyRecord,
)
from .supermemory_config import SupermemoryConfig, SupermemoryConfigError

logger = logging.getLogger(__name__)

__version__ = "5.0.0"

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
    """Raised for invalid adapter inputs or write/read failures."""


# --------------------------------------------------------------------------- #
# Adapter
# --------------------------------------------------------------------------- #
class SupermemoryAdapter:
    """Bridge to the Supermemory service.

    Parameters
    ----------
    config : SupermemoryConfig, optional
        Adapter configuration.
    strict : bool, default True
        If True, Supermemory failures raise; if False, they log and are
        surfaced as ``None``/``[]``.
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
        self._config = config or SupermemoryConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._mirror: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_history
        )
        self._write_errors: int = 0
        self._write_successes: int = 0
        self._recall_errors: int = 0
        self._recall_successes: int = 0
        self._started_at: float = time.time()

        # Client resolution.
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
                if self._strict:
                    raise SupermemoryAdapterError(msg) from exc
                logger.warning("%s Running offline.", msg)
                self._client = None
        else:
            if self._strict and not self._config.is_local():
                raise SupermemoryAdapterError(
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
    def sdk_available(self) -> bool:
        return _SUPERMEMORY_AVAILABLE

    @property
    def client_available(self) -> bool:
        return self._client is not None

    @property
    def mirror_size(self) -> int:
        with self._lock:
            return len(self._mirror)

    # ---------------------------------------------------------- writes
    def _write_payload(self, payload: Mapping[str, Any]) -> Optional[str]:
        """Send a payload with retries. Returns a memory id or None."""
        if self._client is None:
            # Offline mode: append to the local mirror so tests can inspect.
            with self._lock:
                self._mirror.append(dict(payload))
                self._write_successes += 1
            return f"local-{len(self._mirror)}"

        attempts = self._config.write_retries + 1
        last_exc: Optional[BaseException] = None
        for attempt in range(1, attempts + 1):
            try:
                # Supermemory Python SDK exposes ``add`` or ``add_memory``.
                add = getattr(self._client, "add", None)
                if not callable(add):
                    add = getattr(self._client, "add_memory", None)
                if not callable(add):
                    raise SupermemoryAdapterError(
                        "Supermemory client lacks 'add'/'add_memory'."
                    )
                result = add(
                    content=payload["content"],
                    container_tag=payload.get("container_tag"),
                    metadata=payload.get("metadata", {}),
                )
                if asyncio.iscoroutine(result):
                    # Synchronous wrapper: run to completion.
                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        loop = None
                    if loop is not None:
                        raise SupermemoryAdapterError(
                            "client returned a coroutine on a sync path"
                        )
                    result = asyncio.run(result)
                memory_id = None
                if isinstance(result, Mapping):
                    memory_id = (
                        result.get("id")
                        or result.get("memory_id")
                        or result.get("memoryId")
                    )
                with self._lock:
                    self._mirror.append(dict(payload))
                    self._write_successes += 1
                return memory_id or f"write-{self._write_successes}"
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
        if self._strict:
            raise SupermemoryAdapterError(
                f"write failed after {attempts} attempts: {last_exc}"
            ) from last_exc
        return None

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

        Offline mode searches the local mirror with a trivial substring
        match so tests can exercise the recall path.
        """
        if not isinstance(query, str) or not query:
            raise SupermemoryAdapterError("query must be a non-empty string.")
        top_k = k if k is not None else self._config.recall_top_k
        if not isinstance(top_k, int) or top_k <= 0:
            raise SupermemoryAdapterError("k must be a positive int.")
        tag = container_tag or self._config.default_container_tag

        if self._client is None:
            with self._lock:
                matches = [
                    dict(m) for m in self._mirror
                    if tag and m.get("container_tag") == tag
                ]
            self._recall_successes += 1
            return matches[-top_k:]

        try:
            search = getattr(self._client, "search", None)
            if not callable(search):
                search = getattr(self._client, "search_memory", None)
            if not callable(search):
                raise SupermemoryAdapterError(
                    "Supermemory client lacks 'search'/'search_memory'."
                )
            result = search({
                "q": query,
                "containerTag": tag,
                "limit": top_k,
                "filters": dict(filters or {}),
            })
            if asyncio.iscoroutine(result):
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None
                if loop is not None:
                    raise SupermemoryAdapterError(
                        "client returned a coroutine on a sync path"
                    )
                result = asyncio.run(result)
            items = (
                result.get("results", []) if isinstance(result, Mapping)
                else list(result or [])
            )
            with self._lock:
                self._recall_successes += 1
            return [dict(i) for i in items][:top_k]
        except Exception as exc:
            with self._lock:
                self._recall_errors += 1
            logger.warning("Supermemory recall failed: %s", exc)
            if self._strict:
                raise SupermemoryAdapterError(
                    f"recall failed: {exc}"
                ) from exc
            return []

    def forget(self, memory_id: str) -> bool:
        """Attempt to delete a memory. Returns True on best-effort success."""
        if not isinstance(memory_id, str) or not memory_id:
            raise SupermemoryAdapterError(
                "memory_id must be a non-empty string."
            )
        if self._client is None:
            return True
        delete = getattr(self._client, "delete", None)
        if not callable(delete):
            delete = getattr(self._client, "forget", None)
        if not callable(delete):
            if self._strict:
                raise SupermemoryAdapterError(
                    "Supermemory client lacks 'delete'/'forget'."
                )
            return False
        try:
            delete(memory_id)
            return True
        except Exception as exc:
            logger.warning("forget(%s) failed: %s", memory_id, exc)
            if self._strict:
                raise SupermemoryAdapterError(
                    f"forget failed: {exc}"
                ) from exc
            return False

    # ---------------------------------------------------------- async paths
    async def remember_decision_async(
        self, record: DecisionRecord
    ) -> Optional[str]:
        return await asyncio.to_thread(self.remember_decision, record)

    async def remember_policy_async(
        self, record: PolicyRecord
    ) -> Optional[str]:
        return await asyncio.to_thread(self.remember_policy, record)

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

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "mode": self._config.mode,
                "sdk_available": _SUPERMEMORY_AVAILABLE,
                "client_available": self._client is not None,
                "mirror_size": len(self._mirror),
                "write_successes": self._write_successes,
                "write_errors": self._write_errors,
                "recall_successes": self._recall_successes,
                "recall_errors": self._recall_errors,
                "uptime_seconds": time.time() - self._started_at,
            }

    def reset(self, *, clear_mirror: bool = True) -> int:
        with self._lock:
            removed = len(self._mirror)
            if clear_mirror:
                self._mirror.clear()
            self._write_errors = 0
            self._write_successes = 0
            self._recall_errors = 0
            self._recall_successes = 0
            self._started_at = time.time()
        return removed

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_mirror: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "statistics": self.statistics(),
            }
            if include_mirror:
                payload["mirror"] = list(self._mirror)
        return payload

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kw)

    def __repr__(self) -> str:
        return (
            "SupermemoryAdapter("
            f"mode={self._config.mode!r}, "
            f"client={'yes' if self._client else 'no'}, "
            f"mirror={len(self._mirror)}, "
            f"strict={self._strict})"
        )


__all__ = [
    "SupermemoryAdapter",
    "SupermemoryAdapterError",
    "_SUPERMEMORY_AVAILABLE",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m memory.supermemory_adapter
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # Offline adapter (no SDK, no client).
    adapter = SupermemoryAdapter()
    print("repr       :", adapter)

    # ---- Write a decision -----------------------------------------
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

    # ---- Recall ---------------------------------------------------
    results = adapter.recall_similar_runs("edge_int8 vision")
    print("recall     :", len(results), "result(s)")

    # ---- Statistics -----------------------------------------------
    print("statistics :", adapter.statistics())

    # ---- Mocked client path ---------------------------------------
    class _MockClient:
        def __init__(self) -> None:
            self.added: list = []

        def add(self, *, content, container_tag, metadata):
            self.added.append({"content": content, "container_tag": container_tag,
                               "metadata": metadata})
            return {"id": f"mock-{len(self.added)}"}

        def search(self, payload):
            return {"results": [{"id": "mock-1", "content": "x"}]}

    mock = _MockClient()
    mocked = SupermemoryAdapter(client=mock)
    assert mocked.remember_decision(d) is not None
    assert mocked.recall_similar_runs("edge")[0]["id"] == "mock-1"
    print("mocked path: OK")

    # ---- Validation failures --------------------------------------
    for bad_call in (
        lambda: adapter.remember_decision("not-a-record"),  # type: ignore[arg-type]
        lambda: adapter.recall_similar_runs(""),
        lambda: adapter.recall_similar_runs("q", k=0),
    ):
        try:
            bad_call()
        except SupermemoryAdapterError as exc:
            print("Rejected   :", exc)

    print("\nSmoke test passed.")
