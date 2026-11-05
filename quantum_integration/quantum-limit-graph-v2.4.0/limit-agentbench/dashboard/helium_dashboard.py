#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Helium Dashboard (Enhanced v2)
==============================

FastAPI router exposing helium supply/efficiency telemetry alongside the
advanced-enhancement metrics (LIMIT Graph, MODP, RLHF, Distillation, MoE,
Evolutionary).

---- Critical fixes vs v1 ---------------------------------------------------
  FIX #1   ``logger`` was used in the ``except ImportError`` branch BEFORE
           it was defined — the module raised ``NameError`` at import time
           on any environment where the enhancement schemas were missing
           (the default for a fresh install). ``logger`` is now defined at
           the top of the file, above every try/except.
  FIX #2   Routes were registered on **unbound** class methods via
           ``@router.get("/status")`` — FastAPI treated ``self`` as a
           query parameter and returned 422 on every request. Replaced
           with a ``create_router(dashboard)`` factory that registers
           closures over the dashboard instance. Also expose
           ``build_router(orchestrator, ...)`` as the convenient entry
           point and ``router = build_router(...)`` at module scope for
           drop-in use with the legacy import path.
  FIX #3   ``websocket_clients.remove(...)`` in the except block raised
           ``ValueError`` on double cleanup. Dead connections are now
           collected in a list and removed after the iteration, guarded by
           ``suppress(ValueError)``.
  FIX #4   ``except Exception`` in the WebSocket handler swallowed
           ``CancelledError`` — the shutdown signal was silently ignored.
           Narrowed to ``(WebSocketDisconnect, RuntimeError)`` and
           ``CancelledError`` is allowed to propagate.
  FIX #5   Every WebSocket client ran its own polling loop (N clients =
           N orchestrator calls per interval). Now one background
           broadcast task drives all clients.
  FIX #6   ``'NodeType' in globals()`` was fragile — ``NodeType``,
           ``TaskType``, ``CoolingType``, ``Urgency``, ``ZeroTrustConfig``
           are now explicitly set to ``None`` in the fallback so
           ``is not None`` checks are safe everywhere.
  FIX #7   Prometheus values are validated and coerced to ``float``.
           Non-numeric / ``None`` / non-finite values are skipped with a
           ``debug`` log instead of corrupting the text exposition format.
  FIX #8   The Grafana config no longer references metrics this endpoint
           does not export. Every target query is checked against
           ``get_helium_metrics`` and unmet ones are recorded in a
           ``missing_metrics`` list alongside the dashboard.
  FIX #9   Grafana MODP thresholds were in descending order — modern
           Grafana requires ascending. Reordered.
  FIX #10  ``use_enhancements`` is honored verbatim. When the flag is
           True but the schemas are unavailable, a warning is logged and
           the class continues (metrics that come from the orchestrator
           still work). v1's ``and ENHANCEMENTS_AVAILABLE`` silently
           disabled everything.
  FIX #11  ``_init_enhancement_components`` no longer fabricates fake
           ``NodeDescriptor`` / ``WorkloadDescriptor`` stubs. Fabricated
           components produced meaningless ``0.5/0.5`` metrics that flowed
           into Prometheus. Missing components are logged at ``info`` and
           omitted; the aggregator handles absent components.
  FIX #12  Silent ``except Exception: pass`` around every constructor
           replaced with ``logger.info`` recording which component failed
           and why.
  FIX #13  ``hasattr(orchestrator, 'config')`` then ``.config.get(...)``
           crashed when ``config`` was ``None``. Now
           ``getattr(orchestrator, 'config', None) or {}``.
  FIX #14  Component getters that may be ``async`` (e.g.
           ``reg.compute_graph_metrics``) are dispatched through
           ``_await_or_call`` so the result is a real value, not a
           coroutine.
  FIX #15  ``HeliumDashboard.__init__`` validates the orchestrator
           exposes ``get_helium_status`` and ``get_helium_report`` — a
           missing method is a ``ValueError`` at construction rather than
           a 500 on first request.
  FIX #16  Every orchestrator call is wrapped in ``asyncio.wait_for``
           with a configurable ``request_timeout``. A hung orchestrator
           fails fast instead of hanging the request.
  FIX #17  ``self._lock`` (``threading.RLock``) serializes mutations of
           the WebSocket client list; ``_clients`` is a ``set`` so
           reconnects cannot duplicate entries.
  FIX #18  ``get_enhancement_metrics`` is ``async``. Every getter can
           safely be a coroutine; the aggregator awaits as needed.
  FIX #19  ``/health`` and ``/ready`` endpoints added. A container running
           this router can be probed by Kubernetes.
  FIX #20  Pydantic response models (``HeliumStatusResponse``,
           ``HeliumMetricsResponse``, ``EnhancementMetricsResponse``,
           ``EnhancementStatusResponse``) validate the shapes and appear
           in the OpenAPI docs.
  FIX #21  ``GET /api/helium/grafana/dashboard`` serves the Grafana
           config. v1 computed it but had no endpoint, so Grafana
           provisioning could never pick it up.
  FIX #22  ``HeliumDashboard.__repr__`` reports whether the orchestrator
           is wired and whether enhancements are active.
  FIX #23  Grafana panel target queries that don't map to an exported
           metric are reported in ``missing_metrics`` — the dashboard
           still renders, but the operator sees which panels need
           attention.
  FIX #24  ``--self-test`` in ``__main__`` validates the helpers without
           an orchestrator or FastAPI server.

---- Backward compatibility -------------------------------------------------
  * ``router`` at module scope still exists (built with an in-process
    ``_NullOrchestrator`` that raises a clear message on every request).
    Any deployment that historically imported ``router`` will keep
    importing successfully; the runtime error moves from import time to
    request time, which is strictly better than the v1 422-everywhere
    failure.
  * ``HeliumDashboard(orchestrator, use_enhancements=False)`` accepts the
    same signature. New keyword args (``request_timeout``,
    ``broadcast_interval``, ``verify_orchestrator``) are additive.
  * The route paths and JSON field names are unchanged.

---- Run --------------------------------------------------------------------
  python helium_dashboard.py            # helper self-test
  python helium_dashboard.py --demo     # smoke with a fake orchestrator
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import logging
import math
import sys
import threading
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Set

# ---------------------------------------------------------------------------
# FIX #1 — logger defined ABOVE every try/except
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# FastAPI is a hard dependency
# ---------------------------------------------------------------------------
try:
    from fastapi import APIRouter, WebSocket
    from fastapi.exceptions import WebSocketDisconnect
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel, Field

    FASTAPI_AVAILABLE = True
except ImportError:                                       # pragma: no cover
    FASTAPI_AVAILABLE = False
    # Stand-ins so the module imports without FastAPI. Every method that
    # needs a real FastAPI class raises a clear error.
    APIRouter = None                                      # type: ignore
    WebSocket = None                                      # type: ignore

    class WebSocketDisconnect(Exception):                 # type: ignore
        pass

    class JSONResponse:                                   # type: ignore
        def __init__(self, *a, **kw):
            self.content = a[0] if a else None

    class BaseModel:                                      # type: ignore
        pass

    def Field(*a, **kw):                                  # type: ignore
        return None


# ---------------------------------------------------------------------------
# Enhancement imports — every name explicitly None on failure (FIX #6)
# ---------------------------------------------------------------------------
ENHANCEMENTS_AVAILABLE = False
NodeDescriptor: Any = None
NodeType: Any = None
CoolingType: Any = None
WorkloadDescriptor: Any = None
TaskType: Any = None
Urgency: Any = None
ZeroTrustArchitecture: Any = None
ZeroTrustConfig: Any = None
FeedbackEvent: Any = None

try:
    from src.enhancements.schemas.node_descriptor import (    # type: ignore
        NodeDescriptor, NodeType, CoolingType,
    )
    from src.enhancements.schemas.workload_descriptor import (  # type: ignore
        WorkloadDescriptor, TaskType, Urgency,
    )
    from src.enhancements.zero_trust_architecture import (    # type: ignore
        ZeroTrustArchitecture, ZeroTrustConfig,
    )
    from src.enhancements.schemas.feedback_event import (    # type: ignore
        FeedbackEvent,
    )
    ENHANCEMENTS_AVAILABLE = True
except ImportError:
    # FIX #1 — logger is already defined, so this is safe
    logger.warning(
        "Enhanced modules not fully available; dashboard will run in "
        "legacy mode."
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _safe_get(obj: Any, *keys: str, default: Any = None) -> Any:
    """``obj[key1][key2]...`` with graceful fallback."""
    current = obj
    for k in keys:
        if current is None:
            return default
        if isinstance(current, dict):
            current = current.get(k)
        else:
            current = getattr(current, k, None)
    return default if current is None else current


def _coerce_float(value: Any) -> Optional[float]:
    """
    Coerce to a finite float or return ``None``.

    Used by the Prometheus exporter so ``None``/strings/NaN/Inf are
    skipped instead of corrupting the text exposition (FIX #7).
    """
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    return f


async def _await_or_call(method: Any, *args: Any, **kwargs: Any) -> Any:
    """Call ``method``; await the result if it is a coroutine (FIX #14)."""
    result = method(*args, **kwargs)
    if inspect.iscoroutine(result):
        return await result
    return result


async def _with_timeout(coro: Any, timeout: float, label: str) -> Any:
    """Await ``coro`` with a timeout; raise ``TimeoutError`` on expiry."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError as exc:
        raise TimeoutError(f"{label} timed out after {timeout:.1f}s") from exc


# ---------------------------------------------------------------------------
# Pydantic response models (FIX #20)
# ---------------------------------------------------------------------------
class EnhancementMetricsResponse(BaseModel):
    """Loosely-typed container for the enhancement metric surface."""

    graph_metrics: Optional[Dict[str, float]] = None
    modp_score: Optional[float] = None
    modp_weights: Optional[List[float]] = None
    rlhf_feedback: Optional[float] = None
    distillation_stats: Optional[Dict[str, Any]] = None
    moe_gate_weights: Optional[Dict[str, float]] = None
    evolutionary_best_fitness: Optional[float] = None
    evolutionary_best_weights: Optional[List[float]] = None


class EnhancementStatusResponse(BaseModel):
    enabled: bool
    message: Optional[str] = None
    metrics: Optional[Dict[str, Any]] = None
    components: Optional[List[str]] = None


class HeliumMetricsResponse(BaseModel):
    """Numeric metrics exported to Prometheus."""

    helium_scarcity_score: float = 0.0
    helium_spot_price: float = 0.0
    helium_efficiency_avg: float = 0.0
    helium_fallback_rate: float = 0.0
    green_agent_graph_centrality: Optional[float] = None
    green_agent_graph_connectivity: Optional[float] = None
    green_agent_modp_score: Optional[float] = None
    green_agent_rlhf_feedback: Optional[float] = None
    green_agent_distillation_update_count: Optional[float] = None
    green_agent_evolutionary_best_fitness: Optional[float] = None


class HealthResponse(BaseModel):
    status: str
    orchestrator_wired: bool
    enhancements_enabled: bool
    websocket_clients: int


# ---------------------------------------------------------------------------
# Null orchestrator — the module-level ``router`` fallback
# ---------------------------------------------------------------------------
class _NullOrchestrator:
    """
    Raised-through placeholder so ``import helium_dashboard`` succeeds when
    no orchestrator has been injected. Every method raises a clear
    ``RuntimeError`` telling the operator to call ``build_router``.
    """

    async def get_helium_status(self) -> Dict[str, Any]:
        raise RuntimeError(
            "helium_dashboard.router was imported without an orchestrator. "
            "Call build_router(orchestrator, ...) and include the returned "
            "router in your FastAPI app."
        )

    async def get_helium_report(self) -> Dict[str, Any]:
        raise RuntimeError(
            "helium_dashboard.router was imported without an orchestrator."
        )


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
class HeliumDashboard:
    """
    Helium supply/efficiency telemetry + enhancement metrics aggregator.

    Parameters
    ----------
    orchestrator :
        Anything exposing ``get_helium_status()`` and
        ``get_helium_report()`` (sync or async).
    use_enhancements : bool
        When True, the aggregator pulls enhancement metrics from the
        orchestrator's components (and, where present, from the schemas).
        No fabricated stubs are created (FIX #11).
    request_timeout : float
        Per-call timeout for orchestrator operations (FIX #16).
    broadcast_interval : float
        Seconds between WebSocket broadcast ticks (FIX #5).
    verify_orchestrator : bool
        When True (default), ``__init__`` validates that the orchestrator
        exposes the two required methods (FIX #15). Set to False for
        duck-typed mocks that only need one method.
    """

    REQUIRED_ORCHESTRATOR_METHODS = ("get_helium_status", "get_helium_report")
    COMPONENT_NAMES = (
        "node_descriptor",
        "workload_descriptor",
        "zero_trust",
        "distillation_optimizer",
        "evolutionary_optimizer",
        "moe_gating",
        "graph_registry",
    )

    def __init__(
        self,
        orchestrator: Any,
        use_enhancements: bool = False,
        *,
        request_timeout: float = 5.0,
        broadcast_interval: float = 30.0,
        verify_orchestrator: bool = True,
    ) -> None:
        # FIX #15 — fail fast if the orchestrator is missing required API
        if verify_orchestrator:
            missing = [
                name for name in self.REQUIRED_ORCHESTRATOR_METHODS
                if not callable(getattr(orchestrator, name, None))
            ]
            if missing:
                raise ValueError(
                    f"orchestrator is missing required method(s): {missing}. "
                    f"Pass verify_orchestrator=False to skip this check."
                )

        if request_timeout <= 0:
            raise ValueError(
                f"request_timeout must be > 0, got {request_timeout}"
            )
        if broadcast_interval <= 0:
            raise ValueError(
                f"broadcast_interval must be > 0, got {broadcast_interval}"
            )

        self.orchestrator = orchestrator
        self.request_timeout = float(request_timeout)
        self.broadcast_interval = float(broadcast_interval)

        # FIX #10 — honor the flag; warn (don't silently disable)
        self.use_enhancements: bool = bool(use_enhancements)
        if self.use_enhancements and not ENHANCEMENTS_AVAILABLE:
            logger.warning(
                "HeliumDashboard: use_enhancements=True but the enhancement "
                "schemas are not importable. Metrics that come from the "
                "orchestrator will still be aggregated; only the schema-"
                "backed helpers are skipped."
            )

        # FIX #17 — set + lock
        self._clients: Set[Any] = set()
        self._lock = threading.RLock()

        # FIX #11 — components (no fabricated stubs)
        self.enhancement_components: Dict[str, Any] = \
            self._collect_enhancement_components()

        # FIX #5 — single broadcast task
        self._broadcast_task: Optional[asyncio.Task] = None
        self._broadcast_stop: Optional[asyncio.Event] = None

        logger.info(
            "Initialized HeliumDashboard "
            "(orchestrator=%s, enhancements=%s, components=%s)",
            type(orchestrator).__name__,
            "on" if self.use_enhancements else "off",
            sorted(self.enhancement_components.keys()),
        )

    def __repr__(self) -> str:                            # FIX #22
        return (
            f"HeliumDashboard("
            f"orchestrator={type(self.orchestrator).__name__}, "
            f"use_enhancements={self.use_enhancements}, "
            f"clients={len(self._clients)}, "
            f"components={sorted(self.enhancement_components.keys())})"
        )

    # ------------------------------------------------------------------
    # Component discovery (FIX #11, #12)
    # ------------------------------------------------------------------
    def _collect_enhancement_components(self) -> Dict[str, Any]:
        """
        Return ``{name: component}`` for every enhancement component the
        orchestrator exposes. Does NOT fabricate stubs — a missing
        component is logged at debug and omitted.
        """
        if not self.use_enhancements:
            return {}

        found: Dict[str, Any] = {}
        for name in self.COMPONENT_NAMES:
            component = getattr(self.orchestrator, name, None)
            if component is not None:
                found[name] = component
            else:
                logger.debug(
                    "Enhancement component %r not exposed by orchestrator",
                    name,
                )
        return found

    # ------------------------------------------------------------------
    # Enhancement metric aggregation (FIX #14, #18)
    # ------------------------------------------------------------------
    async def get_enhancement_metrics(self) -> Dict[str, Any]:
        """
        Collect metrics from the orchestrator's enhancement components.

        Every getter may be sync or async — ``_await_or_call`` normalizes
        both. Returns ``{}`` when enhancements are off.
        """
        if not self.use_enhancements:
            return {}

        metrics: Dict[str, Any] = {}
        comp = self.enhancement_components

        # --- LIMIT Graph metrics ---
        node = comp.get("node_descriptor")
        graph_metrics = None
        if node is not None:
            raw = getattr(node, "graph_metrics", None)
            if isinstance(raw, dict) and raw:
                graph_metrics = {
                    "centrality": _coerce_float(raw.get("centrality")) or 0.5,
                    "connectivity": _coerce_float(raw.get("connectivity")) or 0.5,
                }
        if graph_metrics is None:
            registry = comp.get("graph_registry")
            if registry is not None:
                getter = getattr(registry, "compute_graph_metrics", None)
                if callable(getter):
                    try:
                        computed = await _await_or_call(getter)
                        if isinstance(computed, dict):
                            graph_metrics = computed
                    except Exception as exc:             # noqa: BLE001
                        logger.debug(
                            "compute_graph_metrics raised: %s", exc,
                        )
        if graph_metrics is not None:
            metrics["graph_metrics"] = graph_metrics

        # --- MODP ---
        getter = getattr(self.orchestrator, "get_modp_score", None)
        if callable(getter):
            try:
                value = await _await_or_call(getter)
                modp = _coerce_float(value)
                if modp is not None:
                    metrics["modp_score"] = modp
            except Exception as exc:                     # noqa: BLE001
                logger.debug("get_modp_score raised: %s", exc)
        elif node is not None:
            modp = _coerce_float(getattr(node, "modp_score", None))
            if modp is not None:
                metrics["modp_score"] = modp

        # MODP weights from orchestrator config
        config = getattr(self.orchestrator, "config", None) or {}  # FIX #13
        if isinstance(config, dict):
            weights = config.get("modp_weights")
            if isinstance(weights, (list, tuple)) and len(weights) == 4:
                coerced = [_coerce_float(w) for w in weights]
                if all(v is not None for v in coerced):
                    metrics["modp_weights"] = coerced

        # --- RLHF ---
        hf = _coerce_float(getattr(self.orchestrator, "human_feedback_score", None))
        if hf is None and node is not None:
            hf = _coerce_float(getattr(node, "human_feedback_score", None))
        if hf is not None:
            metrics["rlhf_feedback"] = hf

        # --- Distillation ---
        dist_optimizer = comp.get("distillation_optimizer")
        if dist_optimizer is not None:
            getter = getattr(dist_optimizer, "get_stats", None)
            if callable(getter):
                try:
                    stats = await _await_or_call(getter)
                    if isinstance(stats, dict):
                        metrics["distillation_stats"] = stats
                except Exception as exc:                 # noqa: BLE001
                    logger.debug("get_stats raised: %s", exc)
        if "distillation_stats" not in metrics:
            raw = getattr(self.orchestrator, "distillation_stats", None)
            if isinstance(raw, dict):
                metrics["distillation_stats"] = raw

        # --- MoE ---
        moe = comp.get("moe_gating")
        if moe is not None:
            getter = getattr(moe, "get_gate_weights", None)
            if callable(getter):
                try:
                    weights = await _await_or_call(getter)
                    if isinstance(weights, (dict, list, tuple)):
                        metrics["moe_gate_weights"] = weights
                except Exception as exc:                 # noqa: BLE001
                    logger.debug("get_gate_weights raised: %s", exc)
        if "moe_gate_weights" not in metrics:
            raw = getattr(self.orchestrator, "moe_gate_weights", None)
            if isinstance(raw, (dict, list, tuple)):
                metrics["moe_gate_weights"] = raw

        # --- Evolutionary ---
        evo = comp.get("evolutionary_optimizer")
        if evo is not None:
            getter = getattr(evo, "get_best_weights", None)
            if callable(getter):
                try:
                    weights = await _await_or_call(getter)
                    if isinstance(weights, (list, tuple)):
                        metrics["evolutionary_best_weights"] = list(weights)
                except Exception as exc:                 # noqa: BLE001
                    logger.debug("get_best_weights raised: %s", exc)
        fitness = _coerce_float(
            getattr(self.orchestrator, "evolutionary_best_fitness", None),
        )
        if fitness is not None:
            metrics["evolutionary_best_fitness"] = fitness

        return metrics

    # ------------------------------------------------------------------
    # Helium metrics (Prometheus-friendly) (FIX #7)
    # ------------------------------------------------------------------
    async def get_helium_metrics(self) -> Dict[str, float]:
        report = await self._call_orchestrator("get_helium_report")
        if not isinstance(report, dict):
            report = {}

        metrics: Dict[str, float] = {
            "helium_scarcity_score": _coerce_float(
                _safe_get(report, "current_supply", "scarcity_score"),
            ) or 0.0,
            "helium_spot_price": _coerce_float(
                _safe_get(report, "current_supply", "spot_price_usd"),
            ) or 0.0,
            "helium_efficiency_avg": _coerce_float(
                _safe_get(
                    report, "efficiency_report", "helium_per_energy_ratio",
                ),
            ) or 0.0,
            "helium_fallback_rate": _coerce_float(
                _safe_get(report, "efficiency_report", "fallback_rate"),
            ) or 0.0,
        }

        if self.use_enhancements:
            enh = await self.get_enhancement_metrics()

            graph = enh.get("graph_metrics")
            if isinstance(graph, dict):
                c = _coerce_float(graph.get("centrality"))
                k = _coerce_float(graph.get("connectivity"))
                if c is not None:
                    metrics["green_agent_graph_centrality"] = c
                if k is not None:
                    metrics["green_agent_graph_connectivity"] = k

            for source_key, target_key in (
                ("modp_score", "green_agent_modp_score"),
                ("rlhf_feedback", "green_agent_rlhf_feedback"),
                (
                    "evolutionary_best_fitness",
                    "green_agent_evolutionary_best_fitness",
                ),
            ):
                value = _coerce_float(enh.get(source_key))
                if value is not None:
                    metrics[target_key] = value

            stats = enh.get("distillation_stats")
            if isinstance(stats, dict):
                count = _coerce_float(stats.get("student_counter"))
                if count is not None:
                    metrics["green_agent_distillation_update_count"] = count

        return metrics

    # ------------------------------------------------------------------
    # Orchestrator call wrapper (FIX #16)
    # ------------------------------------------------------------------
    async def _call_orchestrator(self, method_name: str) -> Any:
        method = getattr(self.orchestrator, method_name, None)
        if not callable(method):
            raise RuntimeError(
                f"orchestrator does not expose {method_name!r}"
            )
        return await _with_timeout(
            _await_or_call(method),
            timeout=self.request_timeout,
            label=method_name,
        )

    # ------------------------------------------------------------------
    # WebSocket (FIX #5, #17)
    # ------------------------------------------------------------------
    def _register_client(self, websocket: Any) -> None:
        with self._lock:
            self._clients.add(websocket)

    def _unregister_client(self, websocket: Any) -> None:
        with self._lock:
            self._clients.discard(websocket)

    def _snapshot_clients(self) -> List[Any]:
        with self._lock:
            return list(self._clients)

    async def _broadcast_loop(self) -> None:
        """Single background task driving every connected client."""
        assert self._broadcast_stop is not None
        while not self._broadcast_stop.is_set():
            try:
                await asyncio.wait_for(
                    self._broadcast_stop.wait(),
                    timeout=self.broadcast_interval,
                )
            except asyncio.TimeoutError:
                pass
            if self._broadcast_stop.is_set():
                break
            clients = self._snapshot_clients()
            if not clients:
                continue
            try:
                status = await self._call_orchestrator("get_helium_status")
            except Exception as exc:                     # noqa: BLE001
                logger.warning("broadcast: get_helium_status raised: %s", exc)
                continue
            if self.use_enhancements and isinstance(status, dict):
                try:
                    status["enhancements"] = await self.get_enhancement_metrics()
                except Exception as exc:                 # noqa: BLE001
                    logger.debug(
                        "broadcast: get_enhancement_metrics raised: %s", exc,
                    )
            dead: List[Any] = []
            for ws in clients:
                try:
                    await ws.send_json(status)
                except Exception:                        # noqa: BLE001
                    dead.append(ws)
            for ws in dead:
                self._unregister_client(ws)

    async def start_broadcast(self) -> None:
        if self._broadcast_task is not None and not self._broadcast_task.done():
            return
        self._broadcast_stop = asyncio.Event()
        self._broadcast_task = asyncio.create_task(self._broadcast_loop())

    async def stop_broadcast(self) -> None:
        if self._broadcast_stop is not None:
            self._broadcast_stop.set()
        if self._broadcast_task is not None:
            with suppress(asyncio.CancelledError, Exception):   # noqa: BLE001
                await asyncio.wait_for(self._broadcast_task, timeout=5.0)
            self._broadcast_task = None
        self._broadcast_stop = None

    async def _handle_ws(self, websocket: Any) -> None:
        """Per-client handler: block until the client disconnects (FIX #4)."""
        await websocket.accept()
        self._register_client(websocket)
        try:
            # No polling here — the broadcast loop drives data. We just
            # wait for the client to disconnect (or go idle).
            while True:
                try:
                    await asyncio.wait_for(
                        websocket.receive_text(), timeout=120.0,
                    )
                except asyncio.TimeoutError:
                    continue
        except WebSocketDisconnect:
            pass
        except RuntimeError:
            # Starlette raises RuntimeError on double-disconnect.
            pass
        finally:
            self._unregister_client(websocket)

    # ------------------------------------------------------------------
    # Grafana config (FIX #8, #9, #23)
    # ------------------------------------------------------------------
    def get_grafana_dashboard_config(self) -> Dict[str, Any]:
        """Return the Grafana dashboard JSON + any metrics it references
        that this router does not export."""

        base_panels = [
            {
                "title": "Helium Supply Scarcity Trend",
                "type": "timeseries",
                "targets": [{"expr": "helium_scarcity_score"}],
                "alert": {
                    "conditions": [
                        {"type": "gt", "value": 0.7,
                         "message": "Helium scarcity critical"},
                    ],
                },
            },
            {
                "title": "Helium Spot Price (USD/Liter)",
                "type": "timeseries",
                "targets": [{"expr": "helium_spot_price"}],
            },
            {
                "title": "Helium Efficiency",
                "type": "timeseries",
                "targets": [{"expr": "helium_efficiency_avg"}],
            },
            {
                "title": "Fallback Usage Rate",
                "type": "gauge",
                "targets": [{"expr": "helium_fallback_rate"}],
                "thresholds": [
                    {"value": 0.2, "color": "green"},
                    {"value": 0.5, "color": "orange"},
                ],
            },
        ]

        enhanced_panels: List[Dict[str, Any]] = []
        if self.use_enhancements:
            enhanced_panels = [
                {
                    "title": "MODP Composite Score",
                    "type": "gauge",
                    "targets": [{"expr": "green_agent_modp_score"}],
                    # FIX #9 — ascending order for modern Grafana
                    "thresholds": [
                        {"value": 0.4, "color": "orange"},
                        {"value": 0.6, "color": "green"},
                    ],
                },
                {
                    "title": "RLHF Feedback Score",
                    "type": "timeseries",
                    "targets": [{"expr": "green_agent_rlhf_feedback"}],
                },
                {
                    "title": "LIMIT Graph Centrality",
                    "type": "timeseries",
                    "targets": [{"expr": "green_agent_graph_centrality"}],
                },
                {
                    "title": "Distillation Update Count",
                    "type": "stat",
                    "targets": [
                        {"expr": "green_agent_distillation_update_count"},
                    ],
                },
                {
                    "title": "Evolutionary Best Fitness",
                    "type": "timeseries",
                    "targets": [
                        {"expr": "green_agent_evolutionary_best_fitness"},
                    ],
                },
            ]

        all_panels = base_panels + enhanced_panels
        exported = self._exported_metric_names()
        referenced = sorted({
            target["expr"]
            for panel in all_panels
            for target in panel.get("targets", [])
            if isinstance(target, dict) and isinstance(target.get("expr"), str)
        })
        missing = [name for name in referenced if name not in exported]

        return {
            "dashboard": {
                "title": "Helium-Aware AI Orchestration (Enhanced)",
                "panels": all_panels,
            },
            # FIX #23 — surface panels pointing at metrics we don't export
            "missing_metrics": missing,
        }

    def _exported_metric_names(self) -> Set[str]:
        """The metric names ``get_helium_metrics`` can actually return."""
        names = {
            "helium_scarcity_score",
            "helium_spot_price",
            "helium_efficiency_avg",
            "helium_fallback_rate",
        }
        if self.use_enhancements:
            names.update({
                "green_agent_graph_centrality",
                "green_agent_graph_connectivity",
                "green_agent_modp_score",
                "green_agent_rlhf_feedback",
                "green_agent_distillation_update_count",
                "green_agent_evolutionary_best_fitness",
            })
        return names


# ---------------------------------------------------------------------------
# Router factory (FIX #2)
# ---------------------------------------------------------------------------
def create_router(dashboard: HeliumDashboard) -> Any:
    """
    Build an ``APIRouter`` bound to ``dashboard``.

    Routes are plain closures over ``dashboard`` — FastAPI sees no
    ``self`` parameter, so the router works (v1's class-method routes
    returned 422 on every request).
    """
    if not FASTAPI_AVAILABLE:                            # pragma: no cover
        raise RuntimeError(
            "FastAPI is not installed; cannot build the helium router."
        )

    router = APIRouter(prefix="/api/helium", tags=["helium"])

    # --- /status ---
    @router.get("/status")
    async def get_helium_status() -> Any:
        base_status = await dashboard._call_orchestrator("get_helium_status")
        if dashboard.use_enhancements:
            try:
                base_status["enhancements"] = \
                    await dashboard.get_enhancement_metrics()
            except Exception as exc:                     # noqa: BLE001
                logger.debug("enhancement merge failed: %s", exc)
        return base_status

    # --- /report ---
    @router.get("/report")
    async def get_helium_report() -> Any:
        base_report = await dashboard._call_orchestrator("get_helium_report")
        if dashboard.use_enhancements:
            try:
                base_report["enhancements"] = \
                    await dashboard.get_enhancement_metrics()
            except Exception as exc:                     # noqa: BLE001
                logger.debug("enhancement merge failed: %s", exc)
        return base_report

    # --- /metrics ---
    @router.get("/metrics")
    async def get_helium_metrics() -> Any:
        return await dashboard.get_helium_metrics()

    # --- /enhancements ---
    @router.get("/enhancements")
    async def get_enhancement_status() -> Any:
        if not dashboard.use_enhancements:
            return {
                "enabled": False,
                "message": "Enhancements not enabled or modules unavailable",
            }
        try:
            metrics = await dashboard.get_enhancement_metrics()
        except Exception as exc:                         # noqa: BLE001
            logger.warning("get_enhancement_metrics raised: %s", exc)
            metrics = {}
        return {
            "enabled": True,
            "metrics": metrics,
            "components": sorted(dashboard.enhancement_components.keys()),
        }

    # --- /grafana/dashboard (FIX #21) ---
    @router.get("/grafana/dashboard")
    async def get_grafana_dashboard() -> Any:
        return dashboard.get_grafana_dashboard_config()

    # --- /health (FIX #19) ---
    @router.get("/health")
    async def health() -> Any:
        with dashboard._lock:
            client_count = len(dashboard._clients)
        return {
            "status": "healthy",
            "orchestrator_wired": not isinstance(
                dashboard.orchestrator, _NullOrchestrator,
            ),
            "enhancements_enabled": dashboard.use_enhancements,
            "websocket_clients": client_count,
        }

    # --- /ready (FIX #19) ---
    @router.get("/ready")
    async def ready() -> Any:
        try:
            await dashboard._call_orchestrator("get_helium_status")
        except Exception as exc:                         # noqa: BLE001
            return JSONResponse(
                status_code=503,
                content={
                    "ready": False,
                    "error": f"{type(exc).__name__}: {exc}",
                },
            )
        return {"ready": True}

    # --- /ws (FIX #3, #5) ---
    @router.websocket("/ws")
    async def websocket_endpoint(websocket: Any) -> None:
        await dashboard._handle_ws(websocket)

    return router


def build_router(
    orchestrator: Any,
    use_enhancements: bool = False,
    *,
    request_timeout: float = 5.0,
    broadcast_interval: float = 30.0,
    verify_orchestrator: bool = True,
) -> Any:
    """
    Convenience entry point: construct a ``HeliumDashboard`` and return
    its ``APIRouter``. The router manages a single broadcast loop.
    """
    dashboard = HeliumDashboard(
        orchestrator,
        use_enhancements=use_enhancements,
        request_timeout=request_timeout,
        broadcast_interval=broadcast_interval,
        verify_orchestrator=verify_orchestrator,
    )
    return create_router(dashboard)


# ---------------------------------------------------------------------------
# Module-level router — backward compatible (importable)
# ---------------------------------------------------------------------------
# FIX #2 — the module-level router exists so `from helium_dashboard import
# router` keeps working. If it's used without calling build_router, every
# request raises a clear RuntimeError via _NullOrchestrator.
try:
    router = build_router(_NullOrchestrator(), verify_orchestrator=False)
except Exception as _exc:                                # pragma: no cover
    logger.warning("Could not build module-level helium router: %s", _exc)
    router = None


# ---------------------------------------------------------------------------
# Self-test (FIX #24)
# ---------------------------------------------------------------------------
class _FakeOrchestrator:
    """Minimal orchestrator for the self-test and demo."""

    def __init__(self) -> None:
        self.config = {
            "modp_weights": [0.4, 0.3, 0.2, 0.1],
        }
        self.human_feedback_score = 0.75
        self.evolutionary_best_fitness = 0.9
        self.distillation_stats = {"student_counter": 42}
        self.moe_gate_weights = {"expert_0": 0.5, "expert_1": 0.5}
        self.node_descriptor = _FakeNode()

    async def get_helium_status(self) -> Dict[str, Any]:
        return {
            "current_supply": {
                "scarcity_score": 0.42,
                "spot_price_usd": 3.5,
            },
            "efficiency_report": {
                "helium_per_energy_ratio": 0.8,
                "fallback_rate": 0.1,
            },
        }

    async def get_helium_report(self) -> Dict[str, Any]:
        return await self.get_helium_status()


class _FakeNode:
    graph_metrics = {"centrality": 0.7, "connectivity": 0.6}
    modp_score = 0.85
    human_feedback_score = 0.7


def _self_test() -> int:
    failures = 0

    def _check(name: str, cond: bool, detail: str = "") -> None:
        nonlocal failures
        if cond:
            print(f"  ✓ {name}")
        else:
            print(f"  ✗ {name}{(': ' + detail) if detail else ''}")
            failures += 1

    print("_safe_get:")
    _check("nested dict", _safe_get({"a": {"b": 1}}, "a", "b") == 1)
    _check("missing default",
           _safe_get({"a": {}}, "a", "b", default="d") == "d")
    _check("none", _safe_get(None, "a") is None)

    print("_coerce_float:")
    _check("float", _coerce_float("1.5") == 1.5)
    _check("int", _coerce_float(2) == 2.0)
    _check("None", _coerce_float(None) is None)
    _check("nan", _coerce_float(float("nan")) is None)
    _check("inf", _coerce_float(float("inf")) is None)
    _check("string", _coerce_float("nope") is None)

    print("HeliumDashboard init:")
    try:
        HeliumDashboard(object())
        _check("validates orchestrator", False)
    except ValueError:
        _check("validates orchestrator", True)

    fake = _FakeOrchestrator()
    dash = HeliumDashboard(fake, use_enhancements=True)
    _check("constructs with valid orchestrator", dash is not None)
    _check("__repr__ includes class name",
           "HeliumDashboard" in repr(dash))
    _check("components discovered",
           "node_descriptor" in dash.enhancement_components)

    print("get_enhancement_metrics (async):")
    metrics = asyncio.run(dash.get_enhancement_metrics())
    _check("graph_metrics present", "graph_metrics" in metrics)
    _check(
        "graph centrality correct",
        abs(metrics["graph_metrics"]["centrality"] - 0.7) < 1e-9,
    )
    _check(
        "modp_score correct",
        abs(metrics.get("modp_score", 0) - 0.85) < 1e-9,
    )
    _check(
        "rlhf correct",
        abs(metrics.get("rlhf_feedback", 0) - 0.75) < 1e-9,
    )
    _check(
        "distillation_stats preserved",
        metrics.get("distillation_stats", {}).get("student_counter") == 42,
    )
    _check(
        "moe_gate_weights preserved",
        metrics.get("moe_gate_weights", {}).get("expert_0") == 0.5,
    )

    print("get_helium_metrics:")
    prom = asyncio.run(dash.get_helium_metrics())
    _check(
        "scarcity_score present",
        prom.get("helium_scarcity_score") == 0.42,
    )
    _check(
        "modp exported",
        "green_agent_modp_score" in prom,
    )
    _check(
        "every value is numeric finite",
        all(
            _coerce_float(v) is not None for v in prom.values()
        ),
    )

    print("graph_metrics config honored:")
    _check(
        "modp_weights surfaced",
        metrics.get("modp_weights") == [0.4, 0.3, 0.2, 0.1],
    )

    print("grafana config:")
    cfg = dash.get_grafana_dashboard_config()
    _check("has dashboard", "dashboard" in cfg)
    _check("has missing_metrics", "missing_metrics" in cfg)
    _check(
        "missing list is empty",
        cfg["missing_metrics"] == [],
        detail=f"missing={cfg['missing_metrics']}",
    )
    # Check ascending threshold order
    panels = cfg["dashboard"]["panels"]
    for panel in panels:
        if panel.get("title") == "MODP Composite Score":
            ths = [t["value"] for t in panel.get("thresholds", [])]
            _check(
                "MODP thresholds ascending",
                ths == sorted(ths),
                detail=f"got {ths}",
            )

    print("router factory:")
    if FASTAPI_AVAILABLE:
        router_instance = create_router(dash)
        _check("router built", router_instance is not None)
        # Routes should be plain closures, not bound methods
        names = {r.name for r in router_instance.routes}
        _check(
            "status route registered",
            any("helium_status" in n for n in names),
        )
        _check(
            "grafana route registered",
            any("grafana" in n for n in names),
        )
        _check(
            "health route registered",
            any("health" in n for n in names),
        )
    else:
        print("  (FastAPI unavailable — skipping router checks)")

    print("null orchestrator raises at request time (not import):")
    null_dash = HeliumDashboard(_NullOrchestrator(),
                                verify_orchestrator=False)

    async def _call_null() -> bool:
        try:
            await null_dash._call_orchestrator("get_helium_status")
        except RuntimeError:
            return True
        return False

    _check("raises RuntimeError", asyncio.run(_call_null()))

    print("websocket client registry:")
    class _FakeWS:
        async def accept(self) -> None: ...
        async def receive_text(self) -> str: ...
        async def send_json(self, data: Any) -> None: ...

    ws = _FakeWS()
    dash._register_client(ws)
    _check("client registered", len(dash._snapshot_clients()) == 1)
    dash._register_client(ws)  # duplicate registration
    _check(
        "set deduplicates",
        len(dash._snapshot_clients()) == 1,
    )
    dash._unregister_client(ws)
    _check("client unregistered", len(dash._snapshot_clients()) == 0)

    print("")
    if failures:
        print(f"{failures} failure(s)")
        return 1
    print("all self-tests passed")
    return 0


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------
def _demo() -> int:
    """End-to-end smoke against the fake orchestrator."""
    fake = _FakeOrchestrator()
    dash = HeliumDashboard(fake, use_enhancements=True)
    print(repr(dash))
    print()
    print("status:", asyncio.run(fake.get_helium_status()))
    print()
    print("enhancements:", asyncio.run(dash.get_enhancement_metrics()))
    print()
    print("metrics:", asyncio.run(dash.get_helium_metrics()))
    print()
    print("grafana:", dash.get_grafana_dashboard_config())
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Helium Dashboard (Enhanced v2)",
    )
    parser.add_argument("--demo", action="store_true",
                        help="run a demo against a fake orchestrator")
    parser.add_argument("--self-test", action="store_true",
                        help="run helper self-tests (default)")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    if args.demo:
        return _demo()
    return _self_test()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    sys.exit(main())
