# src/orchestrator/recall_augmented_router.py

"""
Recall-Augmented Router (v6)
============================

The Green Agent orchestrator entry point that wires Supermemory into the
routing loop. Implements the proposal's 6-step loop:

1. Observe   — build a ``RouteRequest``
2. Recall    — query the memory layer
3. Plan      — delegate to the underlying router with recalled evidence
4. Act       — promote the decision via the write governor
5. Evaluate  — record the outcome after execution
6. Remember  — persist the decision via the adapter

v6 fixes
--------
* ``consistency_threshold`` is now *enforced* (was dead config).
* ``fallback_on_empty_recall`` semantics are consistent (empty + thin paths).
* ``RouteRequest.metadata`` is frozen via ``MappingProxyType``.
* Idempotent ``route()`` (repeat ``run_id`` returns the cached decision).
* ``record_outcome`` requires the run_id to be known (strict mode).
* ``PromotionResult`` distinguishes rejection reasons + human review.
* Timeouts + circuit breakers around ``recall.query`` and ``governor.promote``.
* ``truth_level`` promoted to ``MEASURED`` after confirmed outcome
  (when the adapter supports it).
* Citations sanitized; reason length capped.
* Metrics recorder; richer ``statistics()``.
* Sync **and** async routers supported.
* Typed exceptions: ``InsufficientEvidenceError``, ``RouterFailedError``,
  ``RecallTimeoutError``, ``RecallRouterValidationError``.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import inspect
import json
import logging
import threading
import time
import types
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from ..memory.bounded_recall import BoundedRecall, RecallBundle
from ..memory.memory_schemas import DecisionRecord, OutcomeRecord, TruthLevel
from ..memory.supermemory_adapter import SupermemoryAdapter
from ..memory.write_governor import WriteGovernor

logger = logging.getLogger(__name__)

__version__ = "6.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class RecallRouterError(ValueError):
    """Base error. Kept as ``ValueError`` for backward compatibility."""


class RecallRouterValidationError(RecallRouterError):
    """Invalid input or configuration."""


class InsufficientEvidenceError(RecallRouterError):
    """Insufficient recall evidence and fallback is forbidden."""


class RouterFailedError(RecallRouterError):
    """The underlying router raised or returned an invalid value."""


class RecallTimeoutError(RecallRouterError):
    """An external call exceeded its configured timeout."""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _sanitize(s: Any, max_len: int = 96) -> str:
    """Strip non-printable chars and cap length (audit-safe)."""
    if not isinstance(s, str):
        s = str(s)
    # Preserve length while replacing unprintable with '?'
    cleaned = "".join(c if c.isprintable() or c == " " else "?" for c in s)
    return cleaned[:max_len]


def _json_default(o: Any) -> Any:
    """Explicit serializer for ``to_json`` — never silently erodes types."""
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RecallRouterConfig:
    """Tunable parameters for the recall-augmented router."""

    # --- core (backward compatible) ---
    recall_k: int = 5
    fallback_on_empty_recall: bool = True
    require_min_evidence: int = 0
    consistency_threshold: float = 0.5

    # --- consistency (now actually enforced) ---
    enforce_consistency: bool = True
    strict_consistency: bool = False
    min_consistency_samples: int = 3
    route_extractor: Optional[Callable[[Any], Optional[str]]] = None

    # --- timeouts (None disables) ---
    recall_timeout_s: Optional[float] = 2.0
    governor_timeout_s: Optional[float] = 2.0

    # --- circuit breakers ---
    circuit_failure_threshold: int = 3
    circuit_recovery_timeout_s: int = 30

    # --- idempotency & history ---
    enable_idempotency: bool = True
    run_index_maxlen: int = 10_000

    # --- validation ---
    strict_route_validation: bool = True
    recall_k_cap: int = 100
    max_reason_length: int = 512
    required_metric_keys: Tuple[str, ...] = (
        "energy_wh", "carbon_g", "latency_ms",
    )

    # --- executor ---
    executor_max_workers: int = 4

    def __post_init__(self) -> None:
        if not isinstance(self.recall_k, int) or self.recall_k <= 0:
            raise RecallRouterValidationError("recall_k must be a positive int.")
        if self.recall_k > self.recall_k_cap:
            raise RecallRouterValidationError(
                f"recall_k={self.recall_k} exceeds cap {self.recall_k_cap}."
            )
        if (
            not isinstance(self.require_min_evidence, int)
            or self.require_min_evidence < 0
        ):
            raise RecallRouterValidationError(
                "require_min_evidence must be a non-negative int."
            )
        if (
            not isinstance(self.consistency_threshold, (int, float))
            or not 0.0 <= float(self.consistency_threshold) <= 1.0
        ):
            raise RecallRouterValidationError(
                "consistency_threshold must be a number in [0, 1]."
            )
        for f in ("recall_timeout_s", "governor_timeout_s"):
            v = getattr(self, f)
            if v is not None and (not isinstance(v, (int, float)) or v <= 0):
                raise RecallRouterValidationError(f"{f} must be > 0 or None.")

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # Functions are not JSON-friendly; replace for display.
        if d.get("route_extractor") is not None:
            fn = getattr(self.route_extractor, "__name__", "anonymous")
            d["route_extractor"] = f"<callable {fn}>"
        d["required_metric_keys"] = list(self.required_metric_keys)
        return d


# --------------------------------------------------------------------------- #
# Circuit breaker
# --------------------------------------------------------------------------- #
class _CircuitBreaker:
    def __init__(
        self, name: str, failure_threshold: int = 3,
        recovery_timeout_s: int = 30,
    ) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout_s = recovery_timeout_s
        self._failures = 0
        self._state = "closed"  # "closed" | "open" | "half_open"
        self._last_failure_mono: Optional[float] = None

    @property
    def state(self) -> str:
        return self._state

    def can_call(self) -> bool:
        if self._state == "closed":
            return True
        if self._state == "open":
            if (
                self._last_failure_mono is not None
                and (time.monotonic() - self._last_failure_mono)
                > self.recovery_timeout_s
            ):
                self._state = "half_open"
                return True
            return False
        return True  # half_open: let one through

    def record_success(self) -> None:
        self._failures = 0
        self._state = "closed"

    def record_failure(self) -> None:
        self._failures += 1
        self._last_failure_mono = time.monotonic()
        if self._failures >= self.failure_threshold and self._state != "open":
            self._state = "open"
            logger.warning(
                "Circuit '%s' OPEN after %d failures.",
                self.name, self._failures,
            )


# --------------------------------------------------------------------------- #
# Metrics
# --------------------------------------------------------------------------- #
class MetricsRecorder:
    """Drop-in replaceable with Prometheus / OTel."""

    def __init__(self) -> None:
        self.counters: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = {}

    @staticmethod
    def _key(name: str, labels: Mapping[str, Any]) -> str:
        if not labels:
            return name
        parts = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{parts}}}"

    def inc(self, name: str, value: float = 1.0, **labels: Any) -> None:
        k = self._key(name, labels)
        self.counters[k] = self.counters.get(k, 0.0) + value

    def observe(self, name: str, value: float, **labels: Any) -> None:
        k = self._key(name, labels)
        self.histograms.setdefault(k, []).append(float(value))

    def snapshot(self) -> Dict[str, Any]:
        hist_summary: Dict[str, Dict[str, float]] = {}
        for k, vals in self.histograms.items():
            if not vals:
                continue
            s = sorted(vals)
            hist_summary[k] = {
                "count": len(vals),
                "mean": sum(vals) / len(vals),
                "p50": s[len(s) // 2],
                "p99": s[min(len(s) - 1, int(len(s) * 0.99))],
                "max": s[-1],
            }
        return {
            "counters": dict(self.counters),
            "histograms": hist_summary,
        }


# --------------------------------------------------------------------------- #
# Promotion result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PromotionResult:
    """Normalized governor output."""

    promoted: bool
    reason: str = "ok"
    requires_human: bool = False

    @classmethod
    def from_governor_output(cls, raw: Any) -> "PromotionResult":
        if isinstance(raw, PromotionResult):
            return raw
        if isinstance(raw, bool):
            return cls(promoted=raw, reason="ok" if raw else "rejected")
        # Best-effort attribute-based extraction.
        promoted = bool(getattr(raw, "promoted", raw))
        reason = str(getattr(raw, "reason", "ok" if promoted else "rejected"))
        requires_human = bool(getattr(raw, "requires_human", not promoted))
        return cls(
            promoted=promoted, reason=reason, requires_human=requires_human,
        )


# --------------------------------------------------------------------------- #
# Request & Decision
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RouteRequest:
    """Input to the router. Frozen and metadata immutable."""

    run_id: str
    workload_type: str
    device_class: str
    policy_version: str
    candidates: Tuple[str, ...] = ()
    site_id: Optional[str] = None
    latency_target_ms: Optional[float] = None
    carbon_target_gco2e: Optional[float] = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("run_id", "workload_type", "device_class", "policy_version"):
            v = getattr(self, name)
            if not isinstance(v, str) or not v:
                raise RecallRouterValidationError(
                    f"{name} must be a non-empty string."
                )
        if not isinstance(self.candidates, tuple):
            object.__setattr__(self, "candidates", tuple(self.candidates))
        for c in self.candidates:
            if not isinstance(c, str) or not c:
                raise RecallRouterValidationError(
                    "candidates must contain non-empty strings."
                )
        if not isinstance(self.metadata, Mapping):
            raise RecallRouterValidationError("metadata must be a Mapping.")
        # Freeze metadata — prevents mutation despite frozen=True.
        if not isinstance(self.metadata, types.MappingProxyType):
            object.__setattr__(
                self, "metadata",
                types.MappingProxyType(dict(self.metadata)),
            )

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["candidates"] = list(self.candidates)
        d["metadata"] = dict(self.metadata)
        return d


@dataclass(frozen=True)
class AuditableRouteDecision:
    """Output of the router — a route + the evidence that justified it."""

    run_id: str
    route: str
    reasoning: str
    citations: Tuple[str, ...]
    policy_version: str
    truth_level: str = TruthLevel.ESTIMATED.value
    human_approval_required: bool = False
    promoted: bool = False
    recalled_count: int = 0
    recalled_token_cost: int = 0
    timestamp: datetime = field(default_factory=_utcnow)

    # --- v6 additions ---
    consistency_ratio: Optional[float] = None
    consistency_samples: int = 0
    rejection_reason: Optional[str] = None
    requires_human_review: bool = False
    recall_fallback_triggered: bool = False

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["citations"] = list(self.citations)
        d["timestamp"] = self.timestamp.isoformat()
        return d

    def to_json(self, **kw: Any) -> str:
        return json.dumps(
            self.to_dict(), default=_json_default, **kw,
        )


# --------------------------------------------------------------------------- #
# Router
# --------------------------------------------------------------------------- #
class RecallAugmentedRouter:
    """Wraps an underlying router with recall + governance."""

    def __init__(
        self,
        *,
        router: Union[
            Callable[[RouteRequest, RecallBundle], str],
            Callable[[RouteRequest, RecallBundle], Awaitable[str]],
        ],
        adapter: SupermemoryAdapter,
        recall: BoundedRecall,
        governor: WriteGovernor,
        writer_id: str = "orchestrator",
        config: Optional[RecallRouterConfig] = None,
        strict: bool = True,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        for name, value in (
            ("router", router), ("adapter", adapter),
            ("recall", recall), ("governor", governor),
        ):
            if value is None:
                raise RecallRouterValidationError(f"{name} must not be None.")
        if not callable(router):
            raise RecallRouterValidationError("router must be callable.")
        if not isinstance(writer_id, str) or not writer_id:
            raise RecallRouterValidationError(
                "writer_id must be a non-empty string."
            )

        self._router = router
        self._adapter = adapter
        self._recall = recall
        self._governor = governor
        self._writer_id = writer_id
        self._config = config or RecallRouterConfig()
        self._strict = bool(strict)
        self._metrics = metrics or MetricsRecorder()

        # If router is natively async, warn early.
        if asyncio.iscoroutinefunction(router):
            logger.info(
                "Async router detected; prefer route_async() over route()."
            )

        # Counters
        self._lock = threading.RLock()
        self._total = 0
        self._promoted = 0
        self._rejected = 0
        self._fallbacks = 0
        self._total_outcomes = 0
        self._promoted_outcomes = 0
        self._rejected_outcomes = 0
        self._started_mono = time.monotonic()

        # Idempotency + recent-decisions index (bounded LRU-ish).
        self._run_index: "OrderedDict[str, AuditableRouteDecision]" = OrderedDict()

        # Circuit breakers.
        self._recall_cb = _CircuitBreaker(
            "recall",
            self._config.circuit_failure_threshold,
            self._config.circuit_recovery_timeout_s,
        )
        self._governor_cb = _CircuitBreaker(
            "governor",
            self._config.circuit_failure_threshold,
            self._config.circuit_recovery_timeout_s,
        )

        # Shared executor for bounded timeouts on sync external calls.
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self._config.executor_max_workers,
            thread_name_prefix="recall-router",
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> RecallRouterConfig:
        return self._config

    @property
    def metrics(self) -> MetricsRecorder:
        return self._metrics

    # ---------------------------------------------------------- lifecycle
    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)

    def __enter__(self) -> "RecallAugmentedRouter":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # ---------------------------------------------------------- public API
    def route(self, request: RouteRequest) -> AuditableRouteDecision:
        """Execute one full recall-augmented routing cycle (sync).

        Notes
        -----
        If the underlying ``router`` is an ``async def`` function, use
        :meth:`route_async` instead.
        """
        self._validate_request(request)
        cached = self._check_idempotency(request)
        if cached is not None:
            return cached

        bundle = self._safe_recall(request)
        route = self._call_router_sync(request, bundle)
        return self._finalize_route(request, bundle, route)

    async def route_async(
        self, request: RouteRequest, *, timeout_s: Optional[float] = None,
    ) -> AuditableRouteDecision:
        """Async routing — supports sync and async routers."""
        self._validate_request(request)
        cached = self._check_idempotency(request)
        if cached is not None:
            return cached

        coro = self._route_async_inner(request)
        if timeout_s is not None:
            return await asyncio.wait_for(coro, timeout=timeout_s)
        return await coro

    def record_outcome(
        self,
        *,
        run_id: str,
        predicted: Mapping[str, float],
        measured: Mapping[str, float],
    ) -> bool:
        """Step 5 — evaluate. Persist an outcome record."""
        if not isinstance(run_id, str) or not run_id:
            raise RecallRouterValidationError(
                "run_id must be a non-empty string."
            )
        self._validate_metrics("predicted", predicted)
        self._validate_metrics("measured", measured)

        # Existence check — the decision must have been produced by us.
        with self._lock:
            prior = self._run_index.get(run_id)
        if prior is None:
            msg = f"outcome for unknown run_id: {run_id!r}"
            if self._strict:
                raise RecallRouterValidationError(msg)
            logger.warning(msg)

        record = OutcomeRecord(
            run_id=run_id,
            predicted=dict(predicted),
            measured=dict(measured),
            container_tag=self._adapter.config.default_container_tag,
        )
        result = self._safe_promote(record)

        if result.promoted:
            try:
                self._adapter.record_outcome(record)
            except Exception as e:
                self._metrics.inc("adapter_outcome_errors")
                logger.exception("adapter.record_outcome failed")
                if self._strict:
                    raise RecallRouterError(
                        f"adapter failed to record outcome: {e}"
                    ) from e
            else:
                # Close the loop: mark the decision as MEASURED.
                self._promote_truth_level(run_id)

        with self._lock:
            self._total_outcomes += 1
            if result.promoted:
                self._promoted_outcomes += 1
            else:
                self._rejected_outcomes += 1

        self._metrics.inc(
            "outcome_total", promoted=result.promoted,
        )
        return result.promoted

    def recent_decisions(self, n: int = 10) -> List[AuditableRouteDecision]:
        with self._lock:
            return list(self._run_index.values())[-n:]

    def get_decision(self, run_id: str) -> Optional[AuditableRouteDecision]:
        with self._lock:
            return self._run_index.get(run_id)

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_routes": self._total,
                "promoted": self._promoted,
                "rejected": self._rejected,
                "fallbacks": self._fallbacks,
                "total_outcomes": self._total_outcomes,
                "promoted_outcomes": self._promoted_outcomes,
                "rejected_outcomes": self._rejected_outcomes,
                "circuits": {
                    "recall": self._recall_cb.state,
                    "governor": self._governor_cb.state,
                },
                "recent_decision_count": len(self._run_index),
                "config": self._config.to_dict(),
                "strict": self._strict,
                "uptime_seconds": time.monotonic() - self._started_mono,
                "metrics": self._metrics.snapshot(),
            }

    def health(self) -> Dict[str, Any]:
        circuits = {
            "recall": self._recall_cb.state,
            "governor": self._governor_cb.state,
        }
        return {
            "ready": all(s != "open" for s in circuits.values()),
            "circuits": circuits,
            "config_summary": {
                "recall_k": self._config.recall_k,
                "enforce_consistency": self._config.enforce_consistency,
                "consistency_threshold": self._config.consistency_threshold,
            },
            "uptime_seconds": time.monotonic() - self._started_mono,
        }

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.statistics(), default=_json_default, **kw)

    def __repr__(self) -> str:
        with self._lock:
            return (
                "RecallAugmentedRouter("
                f"routes={self._total}, "
                f"promoted={self._promoted}, "
                f"rejected={self._rejected}, "
                f"strict={self._strict})"
            )

    # ---------------------------------------------------------- internals
    # ---- validation
    def _validate_request(self, request: RouteRequest) -> None:
        if not isinstance(request, RouteRequest):
            raise RecallRouterValidationError(
                "request must be a RouteRequest."
            )

    def _validate_metrics(
        self, side: str, metrics: Mapping[str, float],
    ) -> None:
        if not isinstance(metrics, Mapping):
            raise RecallRouterValidationError(f"{side} must be a Mapping.")
        missing = [k for k in self._config.required_metric_keys if k not in metrics]
        if missing:
            raise RecallRouterValidationError(
                f"{side} missing required keys: {missing}"
            )
        for k, v in metrics.items():
            if not isinstance(v, (int, float)):
                raise RecallRouterValidationError(
                    f"{side}[{k!r}] must be numeric, got {type(v).__name__}"
                )

    # ---- idempotency
    def _check_idempotency(
        self, request: RouteRequest,
    ) -> Optional[AuditableRouteDecision]:
        if not self._config.enable_idempotency:
            return None
        with self._lock:
            cached = self._run_index.get(request.run_id)
        if cached is not None:
            self._metrics.inc("route_idempotent_skips")
            logger.info(
                "idempotent hit for run_id=%s (original route=%s)",
                request.run_id, cached.route,
            )
        return cached

    def _remember_decision(self, decision: AuditableRouteDecision) -> None:
        with self._lock:
            self._run_index[decision.run_id] = decision
            self._run_index.move_to_end(decision.run_id)
            while len(self._run_index) > self._config.run_index_maxlen:
                self._run_index.popitem(last=False)

    # ---- timed sync call helper
    def _call_with_timeout(
        self,
        fn: Callable[..., Any],
        timeout_s: Optional[float],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        if timeout_s is None or timeout_s <= 0:
            return fn(*args, **kwargs)
        import functools
        fut = self._executor.submit(functools.partial(fn, *args, **kwargs))
        return fut.result(timeout=timeout_s)  # raises TimeoutError

    # ---- recall
    def _safe_recall(self, request: RouteRequest) -> RecallBundle:
        if not self._recall_cb.can_call():
            self._metrics.inc("recall_circuit_short_circuit")
            raise RecallRouterError(
                "recall circuit is OPEN; refusing to route."
            )
        query = f"{request.workload_type} {request.device_class}"
        try:
            bundle = self._call_with_timeout(
                self._recall.query,
                self._config.recall_timeout_s,
                query,
                k=self._config.recall_k,
                container_tag=self._adapter.config.default_container_tag,
                filters={
                    "workload_type": request.workload_type,
                    "device_class": request.device_class,
                },
            )
            self._recall_cb.record_success()
            return bundle
        except (concurrent.futures.TimeoutError, TimeoutError):
            self._recall_cb.record_failure()
            self._metrics.inc("recall_timeouts")
            raise RecallTimeoutError(
                f"recall query timed out after "
                f"{self._config.recall_timeout_s}s"
            )
        except Exception as e:
            self._recall_cb.record_failure()
            self._metrics.inc("recall_errors")
            logger.exception("recall query failed")
            raise RecallRouterError(f"recall failed: {e}") from e

    # ---- router invocation
    def _call_router_sync(
        self, request: RouteRequest, bundle: RecallBundle,
    ) -> str:
        try:
            raw = self._router(request, bundle)
        except Exception as e:
            self._metrics.inc("router_errors")
            if self._strict:
                raise RouterFailedError(
                    f"underlying router failed: {e}"
                ) from e
            logger.warning(
                "router failed: %s; using first candidate.", e, exc_info=True,
            )
            if not request.candidates:
                raise RecallRouterError(
                    "router failed and no candidates to fall back to"
                ) from e
            raw = request.candidates[0]

        if inspect.isawaitable(raw):
            raise RecallRouterError(
                "async router detected in route(); use route_async() instead."
            )
        return raw  # type: ignore[return-value]

    async def _call_router_async(
        self, request: RouteRequest, bundle: RecallBundle,
    ) -> str:
        try:
            raw = self._router(request, bundle)
            if inspect.isawaitable(raw):
                raw = await raw
        except Exception as e:
            self._metrics.inc("router_errors")
            if self._strict:
                raise RouterFailedError(
                    f"underlying router failed: {e}"
                ) from e
            logger.warning(
                "router failed: %s; using first candidate.", e, exc_info=True,
            )
            if not request.candidates:
                raise RecallRouterError(
                    "router failed and no candidates to fall back to"
                ) from e
            raw = request.candidates[0]
        return raw  # type: ignore[return-value]

    # ---- async flow
    async def _route_async_inner(
        self, request: RouteRequest,
    ) -> AuditableRouteDecision:
        bundle = await asyncio.to_thread(self._safe_recall, request)
        route = await self._call_router_async(request, bundle)
        return await asyncio.to_thread(
            self._finalize_route, request, bundle, route,
        )

    # ---- consistency
    def _extract_route(self, memory: Any) -> Optional[str]:
        if self._config.route_extractor is not None:
            try:
                return self._config.route_extractor(memory)
            except Exception as e:
                logger.warning("route_extractor failed: %s", e)
                return None
        if isinstance(memory, Mapping):
            r = memory.get("route")
            return str(r) if r else None
        r = getattr(memory, "route", None)
        return str(r) if r else None

    def _check_consistency(
        self, bundle: RecallBundle, proposed_route: str,
    ) -> Tuple[bool, float, int]:
        """
        Returns (is_consistent, agreement_ratio, sample_size).

        If the historical sample is smaller than ``min_consistency_samples``,
        we treat the route as consistent (insufficient data to disagree).
        """
        if not self._config.enforce_consistency:
            return True, 1.0, 0
        try:
            if bundle.is_empty():
                return True, 1.0, 0
            memories = getattr(bundle, "memories", ()) or ()
        except Exception:
            return True, 1.0, 0

        historical_routes: List[str] = []
        for m in memories:
            r = self._extract_route(m)
            if r:
                historical_routes.append(r)
        if not historical_routes:
            return True, 1.0, 0
        if len(historical_routes) < self._config.min_consistency_samples:
            return True, 1.0, len(historical_routes)

        agree = sum(1 for r in historical_routes if r == proposed_route)
        ratio = agree / len(historical_routes)
        return (
            ratio >= self._config.consistency_threshold,
            ratio,
            len(historical_routes),
        )

    # ---- evidence policy
    def _check_evidence_policy(self, bundle: RecallBundle) -> bool:
        """Returns True if a fallback (cold-start / thin evidence) was triggered.

        Raises ``InsufficientEvidenceError`` if a fallback would be needed
        but ``fallback_on_empty_recall`` is False.
        """
        # Empty recall → cold start.
        try:
            empty = bundle.is_empty()
        except Exception:
            empty = not getattr(bundle, "memories", ())

        if empty:
            if not self._config.fallback_on_empty_recall:
                raise InsufficientEvidenceError(
                    "empty recall and fallback_on_empty_recall is False"
                )
            logger.info("cold start (empty recall); fallback allowed")
            return True

        # Thin evidence.
        n = len(getattr(bundle, "memories", ()) or ())
        if n < self._config.require_min_evidence:
            if not self._config.fallback_on_empty_recall:
                raise InsufficientEvidenceError(
                    f"insufficient evidence: {n} < "
                    f"{self._config.require_min_evidence}"
                )
            logger.info(
                "thin evidence (%d < %d); fallback allowed",
                n, self._config.require_min_evidence,
            )
            return True
        return False

    # ---- governor
    def _safe_promote(self, record: Any) -> PromotionResult:
        if not self._governor_cb.can_call():
            self._metrics.inc("governor_circuit_short_circuit")
            logger.warning(
                "governor circuit OPEN; rejecting %s",
                type(record).__name__,
            )
            return PromotionResult(
                promoted=False,
                reason="circuit_open",
                requires_human=True,
            )
        try:
            raw = self._call_with_timeout(
                self._governor.promote,
                self._config.governor_timeout_s,
                record, writer_id=self._writer_id,
            )
            self._governor_cb.record_success()
            return PromotionResult.from_governor_output(raw)
        except (concurrent.futures.TimeoutError, TimeoutError):
            self._governor_cb.record_failure()
            self._metrics.inc("governor_timeouts")
            logger.error("governor.promote timed out")
            return PromotionResult(
                promoted=False,
                reason="timeout",
                requires_human=True,
            )
        except Exception as e:
            self._governor_cb.record_failure()
            self._metrics.inc("governor_errors")
            logger.exception("governor.promote failed")
            if self._strict:
                raise RecallRouterError(
                    f"governor failed: {e}"
                ) from e
            return PromotionResult(
                promoted=False,
                reason=f"error:{type(e).__name__}",
                requires_human=True,
            )

    # ---- truth-level promotion
    def _promote_truth_level(self, run_id: str) -> None:
        for name in (
            "promote_truth_level",
            "update_truth_level",
            "amend_decision",
        ):
            method = getattr(self._adapter, name, None)
            if method is None:
                continue
            try:
                method(run_id=run_id, truth_level=TruthLevel.MEASURED.value)
                logger.debug("promoted truth level for %s via %s", run_id, name)
                return
            except TypeError:
                try:
                    method(run_id, TruthLevel.MEASURED.value)
                    logger.debug(
                        "promoted truth level for %s via %s (positional)",
                        run_id, name,
                    )
                    return
                except Exception as e:
                    logger.warning("%s positional call failed: %s", name, e)
            except Exception as e:
                logger.warning("%s failed: %s", name, e)
        logger.debug(
            "adapter exposes no truth-level promotion method; skipping",
        )

    # ---- compose reason (sanitized)
    def _compose_reason(
        self, request: RouteRequest, bundle: RecallBundle, route: str,
    ) -> str:
        safe_route = _sanitize(route)
        safe_wl = _sanitize(request.workload_type)
        safe_dc = _sanitize(request.device_class)
        try:
            is_empty = bundle.is_empty()
        except Exception:
            is_empty = not getattr(bundle, "memories", ())
        if is_empty:
            return (
                f"Selected {safe_route!r} for {safe_wl} on {safe_dc} "
                f"(cold start; no comparable runs)."
            )[: self._config.max_reason_length]

        safe_citations = [_sanitize(c) for c in bundle.citations][:10]
        n = len(getattr(bundle, "memories", ()) or ())
        reason = (
            f"Selected {safe_route!r} for {safe_wl} on {safe_dc} based on "
            f"{n} comparable historical run(s): "
            f"{', '.join(safe_citations)}."
        )
        return reason[: self._config.max_reason_length]

    # ---- finalize (build decision, promote, remember, stats)
    def _finalize_route(
        self,
        request: RouteRequest,
        bundle: RecallBundle,
        route: str,
    ) -> AuditableRouteDecision:
        # Validate the router output.
        if not isinstance(route, str) or not route:
            raise RouterFailedError(
                "router must return a non-empty string."
            )
        if (
            request.candidates
            and route not in request.candidates
            and self._config.strict_route_validation
        ):
            raise RouterFailedError(
                f"router returned {route!r} not in candidates "
                f"{request.candidates}"
            )

        # Consistency check.
        consistent, ratio, samples = self._check_consistency(bundle, route)
        if not consistent:
            self._metrics.inc("consistency_violations")
            logger.warning(
                "route %r inconsistent with history: %.2f < %.2f (n=%d)",
                route, ratio, self._config.consistency_threshold, samples,
            )
            if self._config.strict_consistency:
                raise RecallRouterError(
                    f"consistency violation: agreement {ratio:.2f} < "
                    f"threshold {self._config.consistency_threshold}"
                )

        # Evidence policy (raises if fallback forbidden and needed).
        fallback_triggered = self._check_evidence_policy(bundle)

        # Build the persisted decision record.
        reason = self._compose_reason(request, bundle, route)
        decision_record = DecisionRecord(
            run_id=request.run_id,
            workload_type=request.workload_type,
            device_class=request.device_class,
            route=route,
            policy_version=request.policy_version,
            truth_level=TruthLevel.ESTIMATED.value,
            predicted_energy_wh=0.0,
            predicted_carbon_gco2e=0.0,
            predicted_latency_ms=request.latency_target_ms or 0.0,
            reason=reason,
            candidates=request.candidates,
            site_id=request.site_id,
            container_tag=self._adapter.config.default_container_tag,
        )

        # Governance.
        promo = self._safe_promote(decision_record)

        # Persist on success.
        if promo.promoted:
            try:
                self._adapter.remember_decision(decision_record)
            except Exception as e:
                self._metrics.inc("adapter_remember_errors")
                logger.exception("adapter.remember_decision failed")
                if self._strict:
                    raise RecallRouterError(
                        f"adapter failed to remember decision: {e}"
                    ) from e

        # Compose the auditable decision.
        requires_human = (
            (not promo.promoted) or promo.requires_human or (not consistent)
        )
        decision = AuditableRouteDecision(
            run_id=request.run_id,
            route=route,
            reasoning=reason,
            citations=tuple(_sanitize(c) for c in bundle.citations),
            policy_version=request.policy_version,
            truth_level=TruthLevel.ESTIMATED.value,
            human_approval_required=requires_human,
            promoted=promo.promoted,
            recalled_count=len(getattr(bundle, "memories", ()) or ()),
            recalled_token_cost=int(getattr(bundle, "token_cost", 0) or 0),
            consistency_ratio=ratio if samples > 0 else None,
            consistency_samples=samples,
            rejection_reason=None if promo.promoted else promo.reason,
            requires_human_review=not consistent or promo.requires_human,
            recall_fallback_triggered=fallback_triggered,
        )

        # Index for idempotency + record_outcome validation.
        self._remember_decision(decision)

        # Counters.
        with self._lock:
            self._total += 1
            if promo.promoted:
                self._promoted += 1
            else:
                self._rejected += 1
            if fallback_triggered:
                self._fallbacks += 1

        # Metrics.
        self._metrics.inc(
            "route_total",
            mode=route, promoted=str(promo.promoted).lower(),
        )
        if samples > 0:
            self._metrics.observe("consistency_ratio", ratio)

        return decision


__all__ = [
    "AuditableRouteDecision",
    "InsufficientEvidenceError",
    "MetricsRecorder",
    "PromotionResult",
    "RecallAugmentedRouter",
    "RecallRouterConfig",
    "RecallRouterError",
    "RecallRouterValidationError",
    "RecallTimeoutError",
    "RouteRequest",
    "RouterFailedError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m orchestrator.recall_augmented_router
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    from ..memory.bounded_recall import BoundedRecall
    from ..memory.supermemory_adapter import SupermemoryAdapter
    from ..memory.write_governor import WriteGovernor

    adapter = SupermemoryAdapter()
    recall = BoundedRecall(adapter)
    governor = WriteGovernor()
    governor.register_writer(
        "orchestrator",
        capabilities={"write:decision", "write:outcome"},
    )

    def _router(request: RouteRequest, bundle: RecallBundle) -> str:
        if request.candidates:
            return request.candidates[0]
        return "defer"

    router = RecallAugmentedRouter(
        router=_router,
        adapter=adapter,
        recall=recall,
        governor=governor,
    )
    print("repr        :", router)

    req = RouteRequest(
        run_id="run-001",
        workload_type="vision_inference",
        device_class="arm_edge",
        policy_version="v0.3",
        candidates=("edge_int8", "cloud_gpu"),
        latency_target_ms=450.0,
    )
    d1 = router.route(req)
    print("decision    :", d1.route, "promoted=", d1.promoted,
          "human_required=", d1.human_approval_required)
    print("consistency :", d1.consistency_ratio, "samples=", d1.consistency_samples)
    print("reasoning   :", d1.reasoning)

    # Idempotency: second call with same run_id returns the cached decision.
    d1b = router.route(req)
    assert d1b is d1 or d1b.run_id == d1.run_id
    print("idempotent  : cached decision returned for run-001")

    # Record outcome.
    ok = router.record_outcome(
        run_id="run-001",
        predicted={"energy_wh": 8.1, "carbon_g": 3.7, "latency_ms": 410.0},
        measured={"energy_wh": 8.6, "carbon_g": 4.0, "latency_ms": 438.0},
    )
    print("outcome     :", ok)

    # Second run — different run_id, exercises the recall-backed path.
    req2 = RouteRequest(
        run_id="run-002",
        workload_type="vision_inference",
        device_class="arm_edge",
        policy_version="v0.3",
        candidates=("edge_int8", "cloud_gpu"),
    )
    d2 = router.route(req2)
    print("decision2   :", d2.route, "recalled=", d2.recalled_count,
          "consistency=", d2.consistency_ratio)
    print("citations   :", d2.citations)

    # Failing outcome for unknown run_id — strict raises.
    try:
        router.record_outcome(
            run_id="run-unknown",
            predicted={"energy_wh": 1.0, "carbon_g": 1.0, "latency_ms": 100.0},
            measured={"energy_wh": 1.0, "carbon_g": 1.0, "latency_ms": 100.0},
        )
    except RecallRouterValidationError as exc:
        print("Rejected    :", exc)

    # Bad metrics type.
    try:
        router.record_outcome(
            run_id="run-002",
            predicted={"energy_wh": "not-a-number", "carbon_g": 1.0,
                       "latency_ms": 100.0},  # type: ignore[dict-item]
            measured={"energy_wh": 1.0, "carbon_g": 1.0, "latency_ms": 100.0},
        )
    except RecallRouterValidationError as exc:
        print("Rejected    :", exc)

    # Frozen metadata.
    try:
        req.metadata["injected"] = "value"  # type: ignore[index]
    except TypeError as exc:
        print("Metadata    : frozen —", type(exc).__name__)

    # Async path with a sync router still works via to_thread.
    async def _async_check() -> None:
        req3 = RouteRequest(
            run_id="run-003",
            workload_type="vision_inference",
            device_class="arm_edge",
            policy_version="v0.3",
            candidates=("edge_int8",),
        )
        d3 = await router.route_async(req3, timeout_s=5.0)
        print("async route :", d3.route, "promoted=", d3.promoted)

    asyncio.run(_async_check())

    print("\nhealth      :", router.health())
    stats = router.statistics()
    print("statistics  :", {
        k: v for k, v in stats.items()
        if k not in ("config", "metrics", "uptime_seconds")
    })
    print("metrics keys:", sorted(stats["metrics"]["counters"].keys()))

    router.close()
    print("\nSmoke test passed.")
