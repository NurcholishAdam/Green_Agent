# src/orchestrator/recall_augmented_router.py

"""
Recall-Augmented Router
=======================

The Green Agent orchestrator entry point that wires Supermemory into the
routing loop. Implements the proposal's 6-step loop:

1. Observe   — build a ``RouteRequest``
2. Recall    — query the memory layer
3. Plan      — delegate to the underlying router with recalled evidence
4. Act       — promote the decision via the write governor
5. Evaluate  — record the outcome after execution
6. Remember  — persist the decision via the adapter

Enhancements
------------
- ``RecallRouterConfig`` — frozen, validated: top-k, fallback behaviour,
  decision-consistency thresholds.
- ``RouteRequest`` and ``AuditableRouteDecision`` — frozen, serializable.
- ``RLock``-guarded statistics.
- Custom ``RecallRouterError(ValueError)``.
- ``__main__`` smoke test with a mocked router.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

from ..memory.bounded_recall import BoundedRecall, RecallBundle
from ..memory.memory_schemas import DecisionRecord, OutcomeRecord, TruthLevel
from ..memory.supermemory_adapter import SupermemoryAdapter
from ..memory.write_governor import WriteGovernor

logger = logging.getLogger(__name__)

__version__ = "5.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class RecallRouterError(ValueError):
    """Raised for invalid router inputs or configuration."""


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RecallRouterConfig:
    """Tunable parameters for the recall-augmented router."""

    recall_k: int = 5
    fallback_on_empty_recall: bool = True
    require_min_evidence: int = 0
    consistency_threshold: float = 0.5

    def __post_init__(self) -> None:
        if not isinstance(self.recall_k, int) or self.recall_k <= 0:
            raise RecallRouterError("recall_k must be a positive int.")
        if not isinstance(self.require_min_evidence, int) or self.require_min_evidence < 0:
            raise RecallRouterError(
                "require_min_evidence must be a non-negative int."
            )
        if not 0.0 <= self.consistency_threshold <= 1.0:
            raise RecallRouterError(
                "consistency_threshold must be in [0, 1]."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Request & Decision
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RouteRequest:
    """Input to the router."""

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
                raise RecallRouterError(f"{name} must be a non-empty string.")
        if not isinstance(self.candidates, tuple):
            object.__setattr__(self, "candidates", tuple(self.candidates))
        if not isinstance(self.metadata, Mapping):
            raise RecallRouterError("metadata must be a Mapping.")

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
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["citations"] = list(self.citations)
        d["timestamp"] = self.timestamp.isoformat()
        return d

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kw)


# --------------------------------------------------------------------------- #
# Router
# --------------------------------------------------------------------------- #
class RecallAugmentedRouter:
    """Wraps an underlying router with recall + governance.

    Parameters
    ----------
    router : callable
        ``(RouteRequest, RecallBundle) -> route_id``. The underlying
        router chooses a route given the request and the recalled evidence.
    adapter : SupermemoryAdapter
        Memory adapter used for writes.
    recall : BoundedRecall
        Bounded recall layer.
    governor : WriteGovernor
        Write-approval gate.
    writer_id : str, default ``"orchestrator"``
        Writer ID presented to the governor.
    config : RecallRouterConfig, optional
    strict : bool, default True
    """

    def __init__(
        self,
        *,
        router: Callable[[RouteRequest, RecallBundle], str],
        adapter: SupermemoryAdapter,
        recall: BoundedRecall,
        governor: WriteGovernor,
        writer_id: str = "orchestrator",
        config: Optional[RecallRouterConfig] = None,
        strict: bool = True,
    ) -> None:
        for name, value in (
            ("router", router), ("adapter", adapter),
            ("recall", recall), ("governor", governor),
        ):
            if value is None:
                raise RecallRouterError(f"{name} must not be None.")
        if not callable(router):
            raise RecallRouterError("router must be callable.")
        if not isinstance(writer_id, str) or not writer_id:
            raise RecallRouterError("writer_id must be a non-empty string.")

        self._router = router
        self._adapter = adapter
        self._recall = recall
        self._governor = governor
        self._writer_id = writer_id
        self._config = config or RecallRouterConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self._total = 0
        self._promoted = 0
        self._rejected = 0
        self._fallbacks = 0
        self._started_at = time.time()

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> RecallRouterConfig:
        return self._config

    # ---------------------------------------------------------- public API
    def route(self, request: RouteRequest) -> AuditableRouteDecision:
        """Execute one full recall-augmented routing cycle (sync)."""
        if not isinstance(request, RouteRequest):
            raise RecallRouterError("request must be a RouteRequest.")

        # ---- 2. Recall ------------------------------------------------
        bundle = self._recall.query(
            f"{request.workload_type} {request.device_class}",
            k=self._config.recall_k,
            container_tag=self._adapter.config.default_container_tag,
            filters={
                "workload_type": request.workload_type,
                "device_class": request.device_class,
            },
        )

        # Enforce minimum evidence policy.
        if (
            self._config.require_min_evidence > 0
            and len(bundle.memories) < self._config.require_min_evidence
        ):
            if not self._config.fallback_on_empty_recall:
                raise RecallRouterError(
                    f"insufficient evidence: {len(bundle.memories)} < "
                    f"{self._config.require_min_evidence}"
                )
            with self._lock:
                self._fallbacks += 1
            logger.info(
                "Insufficient evidence for %s; falling back to cold start.",
                request.run_id,
            )

        # ---- 3. Plan --------------------------------------------------
        try:
            route = self._router(request, bundle)
        except Exception as exc:
            if self._strict:
                raise RecallRouterError(
                    f"underlying router failed: {exc}"
                ) from exc
            logger.warning("router failed: %s; using first candidate.", exc)
            route = request.candidates[0] if request.candidates else "unknown"

        if not isinstance(route, str) or not route:
            raise RecallRouterError("router must return a non-empty string.")

        # ---- 4. Act (governance) -------------------------------------
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
            reason=self._compose_reason(request, bundle, route),
            candidates=request.candidates,
            site_id=request.site_id,
            container_tag=self._adapter.config.default_container_tag,
        )
        promoted = self._governor.promote(
            decision_record, writer_id=self._writer_id,
        )

        # ---- 6. Remember ---------------------------------------------
        if promoted:
            self._adapter.remember_decision(decision_record)

        with self._lock:
            self._total += 1
            if promoted:
                self._promoted += 1
            else:
                self._rejected += 1

        return AuditableRouteDecision(
            run_id=request.run_id,
            route=route,
            reasoning=decision_record.reason,
            citations=bundle.citations,
            policy_version=request.policy_version,
            truth_level=TruthLevel.ESTIMATED.value,
            human_approval_required=not promoted,
            promoted=promoted,
            recalled_count=len(bundle.memories),
            recalled_token_cost=bundle.token_cost,
        )

    async def route_async(self, request: RouteRequest) -> AuditableRouteDecision:
        import asyncio
        return await asyncio.to_thread(self.route, request)

    def record_outcome(
        self,
        *,
        run_id: str,
        predicted: Mapping[str, float],
        measured: Mapping[str, float],
    ) -> bool:
        """Step 5 — evaluate. Persist an outcome record."""
        record = OutcomeRecord(
            run_id=run_id,
            predicted=dict(predicted),
            measured=dict(measured),
            container_tag=self._adapter.config.default_container_tag,
        )
        promoted = self._governor.promote(
            record, writer_id=self._writer_id,
        )
        if promoted:
            self._adapter.record_outcome(record)
        return promoted

    # ---------------------------------------------------------- internals
    def _compose_reason(
        self, request: RouteRequest, bundle: RecallBundle, route: str
    ) -> str:
        if bundle.is_empty():
            return (
                f"Selected {route!r} for {request.workload_type} on "
                f"{request.device_class} (cold start; no comparable runs)."
            )
        return (
            f"Selected {route!r} for {request.workload_type} on "
            f"{request.device_class} based on {len(bundle.memories)} "
            f"comparable historical run(s): "
            f"{', '.join(bundle.citations)}."
        )

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_routes": self._total,
                "promoted": self._promoted,
                "rejected": self._rejected,
                "fallbacks": self._fallbacks,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "uptime_seconds": time.time() - self._started_at,
            }

    def to_json(self, **kw: Any) -> str:
        return json.dumps(self.statistics(), default=str, **kw)

    def __repr__(self) -> str:
        with self._lock:
            return (
                "RecallAugmentedRouter("
                f"routes={self._total}, "
                f"promoted={self._promoted}, "
                f"rejected={self._rejected}, "
                f"strict={self._strict})"
            )


__all__ = [
    "AuditableRouteDecision",
    "RecallAugmentedRouter",
    "RecallRouterConfig",
    "RecallRouterError",
    "RouteRequest",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m orchestrator.recall_augmented_router
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    from ..memory.supermemory_adapter import SupermemoryAdapter
    from ..memory.bounded_recall import BoundedRecall
    from ..memory.write_governor import WriteGovernor

    adapter = SupermemoryAdapter()
    recall = BoundedRecall(adapter)
    governor = WriteGovernor()
    governor.register_writer("orchestrator", capabilities={"write:decision", "write:outcome"})

    def _router(request: RouteRequest, bundle: RecallBundle) -> str:
        # Naive policy: pick the first candidate, or "defer" if empty.
        if request.candidates:
            return request.candidates[0]
        return "defer"

    router = RecallAugmentedRouter(
        router=_router,
        adapter=adapter,
        recall=recall,
        governor=governor,
    )
    print("repr       :", router)

    req = RouteRequest(
        run_id="run-001",
        workload_type="vision_inference",
        device_class="arm_edge",
        policy_version="v0.3",
        candidates=("edge_int8", "cloud_gpu"),
        latency_target_ms=450.0,
    )
    decision = router.route(req)
    print("decision   :", decision.route, "promoted=", decision.promoted)
    print("reasoning  :", decision.reasoning)

    # Record outcome.
    ok = router.record_outcome(
        run_id="run-001",
        predicted={"energy_wh": 8.1, "carbon_g": 3.7, "latency_ms": 410.0},
        measured={"energy_wh": 8.6, "carbon_g": 4.0, "latency_ms": 438.0},
    )
    print("outcome    :", ok)

    # A second request should now recall the first.
    req2 = RouteRequest(
        run_id="run-002",
        workload_type="vision_inference",
        device_class="arm_edge",
        policy_version="v0.3",
        candidates=("edge_int8", "cloud_gpu"),
    )
    decision2 = router.route(req2)
    print("decision2  :", decision2.route, "recalled=", decision2.recalled_count)
    print("citations  :", decision2.citations)

    print("statistics :", {
        k: v for k, v in router.statistics().items()
        if k not in ("config", "uptime_seconds")
    })

    # Validation.
    for bad in (
        lambda: router.route("not-a-request"),  # type: ignore[arg-type]
    ):
        try:
            bad()
        except RecallRouterError as exc:
            print("Rejected   :", exc)

    print("\nSmoke test passed.")
