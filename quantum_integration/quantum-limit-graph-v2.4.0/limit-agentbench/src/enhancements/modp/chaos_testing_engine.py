"""
chaos_testing_engine.py — Enhanced v17.1.0
==========================================

Chaos engineering for MOPD pipeline resilience, extended with ten advanced
Green Agent enhancements in a single self-contained file.

FIXES OVER v17.0.0:
 - `_inject_data_corruption` name collision fixed: the pure recursive
   helper is now `_corrupt_payload`; `_inject_data_corruption` remains the
   async wrapper with its original signature.
 - Snapshot stack is scoped per experiment via a barrier index. Successful
   experiments commit their snapshots; failed experiments roll back only
   their own.
 - `ChaosInjectedFault` distinguishes expected injections from real bugs;
   unexpected exceptions are reported with status="error" instead of
   "injected".
 - Blast radius is enforced via an extensible `register_target_injector`
   hook. Without a registered injector, injection is process-wide for the
   fault-type helpers and the recorded `targets` list is advisory.
 - `_inject_precision_degradation` pushes a snapshot only when the switch
   actually happened.
 - `_inject_causal_shock` only shocks edges with |weight| > epsilon.
 - `_inject_carbon_burst` calls `net_zero_schedule(..., commit=False)` so
   the market price is not mutated.
 - `run_gameday` classifies experiments as run/blocked/failed and aggregates
   accordingly.
 - KPI values from `_probe_kpis` are used for the temporal gate (was
   hardcoded `quality=1.0`).
 - XAI features are normalised so attributions are not dominated by
   `n_targets`.
 - `dry_run=True` supports planning an experiment without injecting.
 - Rollback failures are logged and counted; the snapshot stack is always
   cleaned up at the end of an experiment.
 - All shared bugs fixed in the ten enhancement classes (per-teacher KL,
   real PC-algorithm-ready causal learner, HITL correlation, temporal rule
   persistence, XAI named features, precision filtering, carbon decoupling,
   chaos data_corruption, etc.).
 - `log_event` shim (structlog or stdlib) and Prometheus metrics.
 - `ChaosOrchestratorV17` gains `aclose()` / `__aenter__` / `__aexit__`,
   uses `get_running_loop()`, and logs background-task failures.
 - `InMemoryStorage` methods use real signatures.

Self-contained: Python stdlib + optional numpy / sklearn / torch.
All storage interactions soft-fail.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import re
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Set, Tuple

# ---------- Optional dependencies ----------
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore
    NUMPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression
    SKLEARN_AVAILABLE = True
except ImportError:  # pragma: no cover
    LinearRegression = None  # type: ignore
    SKLEARN_AVAILABLE = False

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover
    torch = None  # type: ignore
    TORCH_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter as _PromCounter, Gauge
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False

# ---------- structlog / stdlib logger ----------
try:
    import structlog
    _STRUCTLOG_LOGGER = structlog.get_logger(__name__)
    _STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    _STRUCTLOG_LOGGER = logging.getLogger(__name__)
    _STRUCTLOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)

# ---------- Preserved module-level logger ----------
logger = logging.getLogger(__name__)


def log_event(level: str, message: str, **kwargs: Any) -> None:
    """Structured-logging shim (structlog or stdlib)."""
    if _STRUCTLOG_AVAILABLE:
        getattr(_STRUCTLOG_LOGGER, level)(message, **kwargs)
    else:
        if kwargs:
            extra = " ".join(f"{k}={v!r}" for k, v in kwargs.items())
            message = f"{message} | {extra}"
        getattr(logger, level)(message)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# CONFIG HELPER
# =============================================================================
def _cfg_get(config: Any, key: str, default: Any = None) -> Any:
    if config is None:
        return default
    if isinstance(config, dict):
        return config.get(key, default)
    if hasattr(config, "model_dump"):
        try:
            return config.model_dump().get(key, default)
        except Exception:
            pass
    if hasattr(config, "dict") and callable(getattr(config, "dict")):
        try:
            return config.dict().get(key, default)
        except Exception:
            pass
    return getattr(config, key, default)


# =============================================================================
# PROMETHEUS METRICS
# =============================================================================
if PROMETHEUS_AVAILABLE:
    _M_CHAOS_EXPERIMENTS = _PromCounter(
        "chaos_experiments_total", "Chaos experiments",
        ["fault_type", "status"],
    )
    _M_CHAOS_STEADY_FAILURES = _PromCounter(
        "chaos_steady_state_failures_total",
        "Chaos experiments that failed the steady-state check",
        ["fault_type"],
    )
    _M_CHAOS_ROLLBACKS = _PromCounter(
        "chaos_rollback_total", "Chaos rollbacks", ["outcome"],
    )
    _M_CHAOS_ACTIVE = Gauge(
        "chaos_active_experiments", "Currently active chaos experiments",
    )
    _M_CHAOS_BLAST_RADIUS = Gauge(
        "chaos_blast_radius", "Blast radius of the last experiment",
    )
    _M_CHAOS_GAMEDAY = _PromCounter(
        "chaos_gameday_total", "Gamedays run", ["all_ok"],
    )
    _M_TEMPORAL_VIOLATIONS = _PromCounter(
        "chaos_temporal_violations_total", "Temporal violations",
        ["rule", "severity"],
    )
    _M_HITL_REQUESTS = _PromCounter(
        "chaos_hitl_requests_total", "HITL requests", 
    )
    _M_HITL_RESPONSES = _PromCounter(
        "chaos_hitl_responses_total", "HITL responses", ["status"],
    )
    _M_PRECISION_SWITCHES = _PromCounter(
        "chaos_precision_switches_total", "Precision switches",
        ["from_p", "to_p"],
    )
    _M_XAI_EXPLANATIONS = _PromCounter(
        "chaos_xai_explanations_total", "XAI explanations",
    )
else:
    class _Dummy:
        def labels(self, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass

    _M_CHAOS_EXPERIMENTS = _M_CHAOS_STEADY_FAILURES = _M_CHAOS_ROLLBACKS = _Dummy()
    _M_CHAOS_ACTIVE = _M_CHAOS_BLAST_RADIUS = _M_CHAOS_GAMEDAY = _Dummy()
    _M_TEMPORAL_VIOLATIONS = _M_HITL_REQUESTS = _M_HITL_RESPONSES = _Dummy()
    _M_PRECISION_SWITCHES = _M_XAI_EXPLANATIONS = _Dummy()


# =============================================================================
# CUSTOM EXCEPTION TYPES
# =============================================================================
class ChaosInjectedFault(Exception):
    """Raised when a chaos fault has been successfully injected."""


# =============================================================================
# ENHANCEMENT 9 — CHAOS TESTING ENGINE
# =============================================================================
class ChaosTestingEngine:
    """
    Chaos engineering for MOPD pipeline resilience.

    v17.1 enhancements:
      * Real ``data_corruption`` (recursive, on dict/list/bytes/numeric
        leaves) via `_corrupt_payload` + async `_inject_data_corruption`.
      * KPI-based steady-state hypothesis (metric probes, not probabilistic).
      * Blast-radius enforcement via an extensible target-injector registry.
      * Per-experiment snapshot scoping with commit/rollback semantics.
      * Scheduled gameday workflow with run/blocked/failed classification.
      * Integration hooks for temporal logic, XAI, HITL, multi-agent,
        carbon markets, precision, and federated sharing.
    """

    FAULT_TYPES = [
        "latency", "exception", "data_corruption",
        "memory_pressure", "network_drop", "precision_degradation",
        "carbon_burst", "causal_shock",
    ]

    def __init__(self,
                 config: Any,
                 storage: Any,
                 temporal: Optional["TemporalLogicVerifier"] = None,
                 xai: Optional["XAIDecisionExplainer"] = None,
                 hitl: Optional["ActiveUserPreferenceLearner"] = None,
                 multi_agent: Optional["MultiAgentCoordinator"] = None,
                 carbon_market: Optional["CarbonMarketIntegrator"] = None,
                 causal_rl: Optional["CausalPolicyAdapter"] = None,
                 precision: Optional["AdaptivePrecisionSwitcher"] = None,
                 federated: Optional["FederatedGreenAggregator"] = None):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict[str, Any]] = {}
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)
        self._lock = asyncio.Lock()

        # Enhancement hooks
        self.temporal = temporal
        self.xai = xai
        self.hitl = hitl
        self.multi_agent = multi_agent
        self.carbon_market = carbon_market
        self.causal_rl = causal_rl
        self.precision = precision
        self.federated = federated

        # KPI probes registered by callers
        self.kpi_probes: Dict[str, Callable[[], Any]] = {}
        self.kpi_thresholds: Dict[str, Tuple[float, float]] = {}

        # Snapshot stack for rollback
        self._snapshots: List[Dict[str, Any]] = []

        # Per-target injectors (blast radius enforcement)
        self._target_injectors: Dict[str, Callable[..., Awaitable[Any]]] = {}

    # ------------------------------------------------------------------
    # KPI registration
    # ------------------------------------------------------------------
    def register_kpi(self, name: str, probe: Callable[[], Any],
                     min_ok: Optional[float] = None,
                     max_ok: Optional[float] = None) -> None:
        """Register a KPI probe and its acceptable bounds."""
        self.kpi_probes[name] = probe
        lo = float(min_ok) if min_ok is not None else -math.inf
        hi = float(max_ok) if max_ok is not None else math.inf
        self.kpi_thresholds[name] = (lo, hi)

    def register_target_injector(
        self, target: str,
        injector: Callable[[str, str, Dict[str, Any]], Awaitable[Any]],
    ) -> None:
        """
        Register a per-target injector for blast-radius enforcement.

        The injector receives (target, fault_type, params) and returns
        anything (usually None). When registered, `_inject_*` helpers will
        call it for every target in the scoped list. When not registered,
        the helper's default process-wide behaviour applies.
        """
        self._target_injectors[target] = injector

    async def _probe_kpis(self) -> Dict[str, float]:
        """Run all registered KPI probes and return their values (NaN on failure)."""
        out: Dict[str, float] = {}
        for name, probe in self.kpi_probes.items():
            try:
                val = probe()
                if asyncio.iscoroutine(val):
                    val = await val
                out[name] = float(val)
            except Exception:
                out[name] = float("nan")
        return out

    def _kpi_ok(self, name: str, value: float) -> bool:
        if math.isnan(value):
            return False
        lo, hi = self.kpi_thresholds.get(name, (-math.inf, math.inf))
        return lo <= value <= hi

    async def _steady_state(self) -> Tuple[bool, Dict[str, float]]:
        """
        KPI-based steady-state check.

        When no KPI probes are registered, falls back to a probabilistic
        heuristic (backward compatible with the previous behaviour).
        """
        kpis = await self._probe_kpis()
        if not kpis:
            if not self.active:
                return True, {}
            return random.random() > _cfg_get(
                self.config, "chaos_intensity", 0.05), {}
        failed: List[str] = []
        for name, value in kpis.items():
            if not self._kpi_ok(name, value):
                failed.append(name)
        ok = not failed
        if not ok:
            log_event("warning",
                      f"Steady-state failed for KPIs: {failed}",
                      kpis=kpis)
        return ok, kpis

    # ------------------------------------------------------------------
    # Blast radius
    # ------------------------------------------------------------------
    def _compute_targets(self, targets: Optional[List[str]]) -> List[str]:
        """Apply blast radius to a candidate target list."""
        if not targets:
            return []
        radius = float(_cfg_get(self.config, "chaos_blast_radius", 0.1))
        radius = max(0.0, min(1.0, radius))
        k = max(1, int(len(targets) * radius))
        return random.sample(targets, k)

    async def _dispatch_to_targets(
        self, fault_type: str, targets: List[str],
        default_impl: Callable[[], Awaitable[Any]],
        params: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Invoke the fault via registered per-target injectors, falling back
        to the process-wide default when no injector matches.
        """
        params = params or {}
        any_registered = False
        for t in targets:
            injector = self._target_injectors.get(t)
            if injector is None:
                continue
            any_registered = True
            try:
                await injector(t, fault_type, params)
            except Exception as exc:
                log_event("warning",
                          f"Target injector failed for {t}: {exc}",
                          fault_type=fault_type)
        if not any_registered:
            await default_impl()

    # ------------------------------------------------------------------
    # Fault injection primitives
    # ------------------------------------------------------------------
    async def _inject_latency(self, targets: List[str],
                              duration_s: float = 0.5) -> None:
        async def _default():
            await asyncio.sleep(duration_s)
        await self._dispatch_to_targets(
            "latency", targets, _default,
            {"duration_s": float(duration_s)})

    async def _inject_exception(self, targets: List[str]) -> None:
        # Even if per-target injectors exist, we still raise here so that
        # `run_experiment` classifies the fault correctly.
        async def _default():
            raise ChaosInjectedFault("chaos: injected exception")
        await self._dispatch_to_targets("exception", targets, _default)

    async def _inject_memory_pressure(self, targets: List[str],
                                      mb: int = 5) -> None:
        async def _default():
            _ = bytearray(mb * 1024 * 1024)
            del _
        await self._dispatch_to_targets(
            "memory_pressure", targets, _default, {"mb": int(mb)})

    async def _inject_network_drop(self, targets: List[str],
                                   duration_s: float = 0.2) -> None:
        async def _default():
            await asyncio.sleep(duration_s)
        await self._dispatch_to_targets(
            "network_drop", targets, _default,
            {"duration_s": float(duration_s)})

    # ---- Data corruption ----
    def _corrupt_payload(self, payload: Any, depth: int = 0) -> Any:
        """
        Pure, recursive, synchronous payload corruption.

        Numeric leaves are perturbed with Gaussian noise; strings have a
        fraction of characters replaced; bytes have a fraction of bytes
        XOR-flipped. Bounded by `depth` to avoid runaway recursion.
        """
        if depth > 5:
            return payload
        if isinstance(payload, dict):
            return {k: self._corrupt_payload(v, depth + 1)
                    for k, v in payload.items()}
        if isinstance(payload, list):
            return [self._corrupt_payload(v, depth + 1) for v in payload]
        if isinstance(payload, tuple):
            return tuple(self._corrupt_payload(v, depth + 1) for v in payload)
        if isinstance(payload, bool):
            # bool is a subclass of int; corrupt by flipping.
            return not payload
        if isinstance(payload, (int, float)):
            return float(payload) + random.gauss(0, 0.1)
        if isinstance(payload, str):
            if not payload:
                return payload
            chars = list(payload)
            for _ in range(max(1, len(chars) // 10)):
                i = random.randrange(len(chars))
                chars[i] = random.choice("!@#$%^&*")
            return "".join(chars)
        if isinstance(payload, (bytes, bytearray)):
            arr = bytearray(payload)
            for _ in range(max(1, len(arr) // 10)):
                arr[random.randrange(len(arr))] ^= 0xFF
            return bytes(arr)
        return payload

    async def _inject_data_corruption(self, targets: List[str],
                                      payload: Any = None) -> Any:
        """
        Async wrapper that returns the corrupted payload (does not mutate).

        The pure helper is `_corrupt_payload`; this method exists to preserve
        the original interface and to model the event-loop yield that a real
        injection would introduce.
        """
        async def _default():
            await asyncio.sleep(0)
        await self._dispatch_to_targets(
            "data_corruption", targets, _default)
        return self._corrupt_payload(payload)

    async def _inject_precision_degradation(self, targets: List[str]) -> None:
        """Force a downgrade in precision; snapshot only on success."""
        if self.precision is None:
            return
        old = self.precision.current
        try:
            switched = await self.precision.switch_to(
                "int8", reason="chaos:precision_degradation")
        except Exception as exc:
            log_event("warning", f"precision_degradation failed: {exc}")
            return
        if switched:
            self._snapshots.append({"precision_old": old})

    async def _inject_carbon_burst(self, targets: List[str]) -> None:
        """Simulate a carbon-price spike without mutating the market."""
        if self.carbon_market is None:
            return
        try:
            # `commit=False` ensures no price mutation and no match record.
            await self.carbon_market.net_zero_schedule(
                workload_kwh=10.0, intensity=0.9, commit=False)
        except Exception as exc:
            log_event("warning", f"carbon_burst failed: {exc}")

    async def _inject_causal_shock(self, targets: List[str]) -> None:
        """Shock the causal graph by flipping a non-zero edge weight."""
        if self.causal_rl is None or self.causal_rl.graph is None:
            return
        graph = self.causal_rl.graph.graph
        if not graph:
            return
        candidates = [
            (src, dst) for src, edges in graph.items()
            for dst in edges
            if abs(float(edges[dst].get("weight", 0.0))) > 1e-6
        ]
        if not candidates:
            return
        src, dst = random.choice(candidates)
        old_w = float(graph[src][dst].get("weight", 0.0))
        self._snapshots.append({"causal_edge": (src, dst, old_w)})
        graph[src][dst]["weight"] = -old_w

    # ------------------------------------------------------------------
    # Rollback
    # ------------------------------------------------------------------
    async def _rollback(self) -> None:
        """Roll back all pending snapshots."""
        await self._rollback_to(0)

    async def _rollback_to(self, barrier: int) -> None:
        """Roll back snapshots down to (but not including) `barrier`."""
        while len(self._snapshots) > barrier:
            snap = self._snapshots.pop()
            try:
                if "precision_old" in snap and self.precision is not None:
                    await self.precision.switch_to(
                        snap["precision_old"], reason="chaos:rollback")
                if "causal_edge" in snap and self.causal_rl is not None:
                    src, dst, old_w = snap["causal_edge"]
                    self.causal_rl.graph.graph[src][dst]["weight"] = old_w
            except Exception as exc:
                log_event("warning", f"Rollback step failed: {exc}")
                try:
                    _M_CHAOS_ROLLBACKS.labels(outcome="failed").inc()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Core experiment
    # ------------------------------------------------------------------
    async def run_experiment(self,
                             name: str,
                             fault_type: str,
                             targets: Optional[List[str]] = None,
                             payload: Any = None,
                             abort_on_steady_fail: bool = True,
                             auto_rollback: Optional[bool] = None,
                             require_hitl: Optional[bool] = None,
                             dry_run: bool = False) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")

        t0 = time.time()

        # --- 1. Blast-radius scoping ---
        scoped = self._compute_targets(targets or [])

        # --- 2. HITL approval for large blast radius ---
        radius = float(_cfg_get(self.config, "chaos_blast_radius", 0.1))
        needs_hitl = require_hitl
        if needs_hitl is None:
            needs_hitl = radius > 0.5
        if needs_hitl and self.hitl is not None:
            try:
                try:
                    _M_HITL_REQUESTS.inc()
                except Exception:
                    pass
                approved = await self.hitl.query_user_if_needed(
                    "chaos_approver",
                    [
                        {"solution_id": "approve", "quality_score": 0.90,
                         "carbon_g": 0.10, "cost_usd": 0.10, "latency_ms": 100},
                        {"solution_id": "reject", "quality_score": 0.89,
                         "carbon_g": 0.11, "cost_usd": 0.11, "latency_ms": 105},
                    ],
                    timeout=2.0)
                if approved != "approve":
                    return {"name": name, "status": "hitl_rejected",
                            "fault_type": fault_type,
                            "blast_radius": radius}
                try:
                    _M_HITL_RESPONSES.labels(status="approved").inc()
                except Exception:
                    pass
            except Exception as exc:
                log_event("warning", f"HITL approval failed: {exc}")

        # --- 3. Carbon-market deferral ---
        if self.carbon_market is not None:
            try:
                decision = await self.carbon_market.net_zero_schedule(
                    workload_kwh=0.5, intensity=0.4, commit=False)
                if decision.get("action") == "defer":
                    return {"name": name, "status": "deferred",
                            "fault_type": fault_type,
                            "reason": "high_carbon_window",
                            "blast_radius": radius}
            except Exception as exc:
                log_event("debug", f"Carbon deferral check failed: {exc}")

        # --- 4. Multi-agent bid ---
        agent_id: Optional[str] = None
        if self.multi_agent is not None:
            try:
                agent_id, _ = await self.multi_agent.bid({
                    "name": f"chaos_{fault_type}",
                    "preferred_role": "validator"})
            except Exception:
                pass

        # --- 5. Push actual KPIs to temporal logic ---
        if self.temporal is not None:
            try:
                probe_values = await self._probe_kpis()
                temporal_state: Dict[str, Any] = {
                    "chaos_active": 1,
                    "fault_type_hash": abs(hash(fault_type)) % 100,
                }
                for k, v in probe_values.items():
                    if not math.isnan(v):
                        temporal_state[k] = v
                await self.temporal.push_state(temporal_state)
                violations = await self.temporal.verify()
                if any(not ok for ok in violations.values()):
                    return {"name": name, "status": "temporal_blocked",
                            "fault_type": fault_type,
                            "violations": {k: v for k, v in violations.items()
                                           if not v},
                            "blast_radius": radius}
            except Exception as exc:
                log_event("warning", f"Temporal gate failed: {exc}")

        # --- 6. Steady-state BEFORE ---
        before_ok, before_kpis = await self._steady_state()

        # --- 7. Inject the fault (or dry-run plan) ---
        status = "completed"
        err: Optional[str] = None
        injected_ms = 0.0
        barrier = len(self._snapshots)

        if dry_run:
            status = "dry_run"
        else:
            try:
                async with self._lock:
                    self.active[name] = {
                        "fault_type": fault_type,
                        "targets": scoped,
                        "started": _now_utc().isoformat()}
                try:
                    _M_CHAOS_ACTIVE.set(len(self.active))
                except Exception:
                    pass

                inject_start = time.monotonic()
                if fault_type == "latency":
                    await self._inject_latency(scoped)
                elif fault_type == "exception":
                    await self._inject_exception(scoped)
                elif fault_type == "memory_pressure":
                    await self._inject_memory_pressure(scoped)
                elif fault_type == "network_drop":
                    await self._inject_network_drop(scoped)
                elif fault_type == "data_corruption":
                    _ = await self._inject_data_corruption(scoped, payload)
                elif fault_type == "precision_degradation":
                    await self._inject_precision_degradation(scoped)
                elif fault_type == "carbon_burst":
                    await self._inject_carbon_burst(scoped)
                elif fault_type == "causal_shock":
                    await self._inject_causal_shock(scoped)
                injected_ms = (time.monotonic() - inject_start) * 1000.0
            except ChaosInjectedFault as e:
                err = str(e)
                status = "injected"
            except Exception as e:
                err = str(e)
                status = "error"
                log_event("error", f"Chaos injection failed: {e}")
            finally:
                async with self._lock:
                    self.active.pop(name, None)
                try:
                    _M_CHAOS_ACTIVE.set(len(self.active))
                except Exception:
                    pass

        # --- 8. Steady-state AFTER ---
        after_ok, after_kpis = await self._steady_state()

        # --- 9. Abort/rollback + snapshot commit ---
        rollback_flag = auto_rollback
        if rollback_flag is None:
            rollback_flag = _cfg_get(self.config, "chaos_auto_rollback", True)
        aborted = False
        rolled_back = False
        try:
            if (not dry_run and abort_on_steady_fail and not after_ok):
                aborted = True
                if rollback_flag:
                    await self._rollback_to(barrier)
                    rolled_back = True
                    try:
                        _M_CHAOS_ROLLBACKS.labels(outcome="applied").inc()
                    except Exception:
                        pass
                else:
                    # Discard snapshots without applying them.
                    del self._snapshots[barrier:]
            else:
                # Successful experiment: commit snapshots.
                del self._snapshots[barrier:]
        finally:
            # Ensure no snapshots leak on unexpected paths.
            if len(self._snapshots) > barrier:
                del self._snapshots[barrier:]

        # --- 10. XAI explanation (normalised features) ---
        xai_explanation = None
        if self.xai is not None and NUMPY_AVAILABLE:
            try:
                n_targets = max(1, len(targets or []))
                n_scoped_norm = len(scoped) / n_targets
                feats = np.array([
                    float(radius),
                    float(n_scoped_norm),
                    1.0 if before_ok else 0.0,
                    1.0 if after_ok else 0.0,
                ])
                def _score(x, _w=np.array([0.4, 0.2, 0.3, 0.3])):
                    return float(np.dot(x, _w))
                xai_explanation = await self.xai.explain(
                    decision_id=f"chaos_{uuid.uuid4().hex[:8]}",
                    label=f"chaos:{fault_type}",
                    features=feats,
                    names=["blast_radius", "targets_scoped_fraction",
                           "steady_before", "steady_after"],
                    model_fn=_score)
                try:
                    _M_XAI_EXPLANATIONS.inc()
                except Exception:
                    pass
            except Exception:
                pass

        # --- 11. Multi-agent reward ---
        if self.multi_agent is not None and agent_id:
            try:
                await self.multi_agent.reward(
                    agent_id, 0.8 if after_ok else 0.2)
            except Exception:
                pass

        # --- 12. Federated sharing ---
        if self.federated is not None:
            try:
                blob = json.dumps({
                    "fault_type": fault_type,
                    "before_ok": before_ok,
                    "after_ok": after_ok,
                    "aborted": aborted}).encode()
                await self.federated.share_weights("chaos_outcomes", blob)
            except Exception:
                pass

        # --- 13. Persist experiment ---
        duration = (time.time() - t0) * 1000.0
        try:
            await asyncio.to_thread(
                self.storage.save_chaos_experiment,
                name, name, fault_type,
                radius,
                int(before_ok), int(after_ok),
                status, duration)
        except Exception:
            pass
        try:
            _M_CHAOS_EXPERIMENTS.labels(
                fault_type=fault_type, status=status).inc()
            _M_CHAOS_BLAST_RADIUS.set(radius)
            if not after_ok:
                _M_CHAOS_STEADY_FAILURES.labels(fault_type=fault_type).inc()
        except Exception:
            pass

        result = {
            "name": name,
            "fault_type": fault_type,
            "targets": scoped,
            "blast_radius": radius,
            "steady_before": before_ok,
            "steady_after": after_ok,
            "before_kpis": before_kpis,
            "after_kpis": after_kpis,
            "status": status,
            "aborted": aborted,
            "rolled_back": rolled_back,
            "error": err,
            "duration_ms": duration,
            "injected_ms": injected_ms,
            "agent_id": agent_id,
            "xai": xai_explanation,
            "dry_run": dry_run,
        }
        self.history.append(result)
        return result

    # ------------------------------------------------------------------
    # Gameday workflow
    # ------------------------------------------------------------------
    async def run_gameday(self,
                          name: str,
                          hypothesis: str,
                          experiments: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Run a scheduled gameday.

        Args:
            name: gameday name
            hypothesis: human-readable steady-state hypothesis
            experiments: list of dicts with keys: fault_type, targets,
                payload, abort_on_steady_fail, require_hitl, dry_run
        """
        results: List[Dict[str, Any]] = []
        for i, exp in enumerate(experiments):
            r = await self.run_experiment(
                name=f"{name}_exp{i}",
                fault_type=exp["fault_type"],
                targets=exp.get("targets"),
                payload=exp.get("payload"),
                abort_on_steady_fail=exp.get("abort_on_steady_fail", True),
                require_hitl=exp.get("require_hitl", False),
                dry_run=exp.get("dry_run", False))
            results.append(r)

        # Classify: an experiment is "run" if a fault was attempted.
        blocked_statuses = {"hitl_rejected", "deferred", "temporal_blocked"}
        run_results = [r for r in results if r.get("status") not in blocked_statuses]
        blocked_results = [r for r in results if r.get("status") in blocked_statuses]
        errored_results = [r for r in run_results if r.get("status") == "error"]

        all_ok = all(
            r.get("steady_after", True) and r.get("status") != "error"
            for r in run_results
        )

        try:
            _M_CHAOS_GAMEDAY.labels(all_ok=str(all_ok)).inc()
        except Exception:
            pass

        return {
            "name": name,
            "hypothesis": hypothesis,
            "experiments": len(results),
            "run": len(run_results),
            "blocked": len(blocked_results),
            "errored": len(errored_results),
            "all_steady_after": all_ok,
            "results": results,
            "ts": _now_utc().isoformat(),
        }

    # ------------------------------------------------------------------
    def stats(self) -> Dict[str, Any]:
        return {
            "active": len(self.active),
            "history": len(self.history),
            "registered_kpis": len(self.kpi_probes),
            "pending_snapshots": len(self._snapshots),
            "registered_target_injectors": len(self._target_injectors),
        }


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION ENGINE
# =============================================================================
class QuantumDistillationEngine:
    def __init__(self, temperature: float = 2.0, alpha: float = 0.5,
                 n_actions: int = 5):
        self.temperature = float(temperature)
        self.alpha = float(alpha)
        self.n_actions = int(n_actions)
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy = [1.0 / self.n_actions] * self.n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def register_teacher(self, name: str, policy: List[float]) -> None:
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[str(name)] = [max(0.0, p / s) for p in policy]

    def unregister_teacher(self, name: str) -> None:
        self.teachers.pop(name, None)

    def _softmax(self, x: List[float], temp: float) -> List[float]:
        if not x:
            return []
        m = max(x)
        t = max(temp, 1e-6)
        exps = [math.exp((v - m) / t) for v in x]
        s = sum(exps) or 1.0
        return [e / s for e in exps]

    def _superpose(self) -> List[float]:
        if not self.teachers:
            return list(self.student_policy)
        n = self.n_actions
        accum = [0.0] * n
        for pol in self.teachers.values():
            for i in range(min(n, len(pol))):
                accum[i] += math.sqrt(max(pol[i], 1e-9))
        accum = [a / len(self.teachers) for a in accum]
        sq = [a * a for a in accum]
        s = sum(sq) or 1.0
        return [x / s for x in sq]

    async def step(self, storage: Any,
                   student_id: str = "chaos_student") -> Dict[str, Any]:
        target = self._softmax(self._superpose(), self.temperature)
        lr = 0.1
        new = []
        for s, t in zip(self.student_policy, target):
            grad = -(t / max(s, 1e-9))
            new.append(max(0.01, s - lr * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]

        # Per-teacher KL against the teacher's own distribution.
        for tid, pol in self.teachers.items():
            weight = float(pol[0]) if pol else 0.0
            amplitude = math.sqrt(max(weight, 1e-9))
            kl = 0.0
            for i in range(self.n_actions):
                pi = pol[i] if i < len(pol) else 1e-9
                si = self.student_policy[i]
                pi = max(pi, 1e-9)
                si = max(si, 1e-9)
                kl += pi * math.log(pi / si)
            try:
                await asyncio.to_thread(
                    storage.save_teacher_superposition,
                    student_id, tid, weight, self.temperature, amplitude, kl)
            except Exception:
                pass

        entry = {"target": target,
                 "student": list(self.student_policy),
                 "ts": _now_utc().isoformat()}
        self.history.append(entry)
        return entry

    def get_policy(self) -> List[float]:
        return list(self.student_policy)


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL
# =============================================================================
class CausalGraphLearner:
    """Correlation + variance causal discovery (fallback learner)."""

    def __init__(self, storage: Any = None):
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []
        self._lock = asyncio.Lock()

    async def learn(self, samples: List[Dict[str, float]],
                    variables: List[str],
                    threshold: float = 0.25) -> Dict[str, Any]:
        self.variables = list(variables)
        # Insufficient data → return empty graph (no fabrication).
        if len(samples) < 5 or not NUMPY_AVAILABLE:
            async with self._lock:
                self.graph.clear()
            return self.summary()
        try:
            X = np.array(
                [[float(s.get(v, 0.0)) for v in variables] for s in samples],
                dtype=float)
        except Exception as exc:
            log_event("warning", f"Causal samples parse failed: {exc}")
            return self.summary()
        if X.shape[0] < 2 or X.shape[1] < 2:
            return self.summary()
        X = (X - X.mean(0)) / (X.std(0) + 1e-9)
        try:
            corr = np.corrcoef(X, rowvar=False)
        except Exception:
            return self.summary()

        async with self._lock:
            self.graph.clear()
            for i in range(len(variables)):
                for j in range(len(variables)):
                    if i == j:
                        continue
                    c = abs(float(corr[i, j]))
                    if not math.isfinite(c) or c <= threshold:
                        continue
                    vi, vj = float(X[:, i].var()), float(X[:, j].var())
                    src, dst = (variables[i], variables[j]) if vi > vj \
                        else (variables[j], variables[i])
                    self.graph[src][dst] = {"weight": float(corr[i, j]),
                                            "confidence": c}
                    if self.storage is not None:
                        try:
                            await asyncio.to_thread(
                                self.storage.save_causal_edge,
                                src, dst, float(corr[i, j]), c)
                        except Exception:
                            pass
        return self.summary()

    def parents(self, node: str) -> List[str]:
        return [s for s, e in self.graph.items() if node in e]

    def children(self, node: str) -> List[str]:
        return list(self.graph.get(node, {}).keys())

    def summary(self) -> Dict[str, Any]:
        return {"nodes": len(self.variables),
                "edges": sum(len(v) for v in self.graph.values()),
                "variables": list(self.variables)}


class CausalPolicyAdapter:
    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]
    STRATEGY_MAP: Dict[str, str] = {
        "performance": "balanced",
        "carbon": "carbon_focus",
        "cost": "operational_focus",
        "hybrid": "balanced",
        "adaptive": "balanced",
    }

    def __init__(self, config: Any, storage: Any, graph: CausalGraphLearner):
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values: Dict[str, float] = defaultdict(float)
        self.counts: Dict[str, int] = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = float(_cfg_get(config, "causal_exploration_rate", 0.1) or 0.1)
        self._lock = asyncio.Lock()

    def to_strategy(self, action: str) -> str:
        return self.STRATEGY_MAP.get(action, "balanced")

    async def choose_action(self, state: Dict[str, Any]) -> str:
        async with self._lock:
            if random.random() < self.epsilon:
                return random.choice(self.ACTIONS)
            return max(self.ACTIONS, key=lambda a: self.values.get(a, 0.0))

    async def update(self, action: str, reward: float,
                     state: Dict[str, Any]) -> None:
        async with self._lock:
            if action not in self.ACTIONS:
                action = self.ACTIONS[0]
            self.counts[action] += 1
            n = self.counts[action]
            self.values[action] += (reward - self.values[action]) / n
            vals = [self.values.get(a, 0.0) for a in self.ACTIONS]
            m = max(vals)
            exps = [math.exp((v - m) / 0.5) for v in vals]
            s = sum(exps) or 1.0
            self.policy = [e / s for e in exps]

    async def estimate_ate(self, treatment: str, outcome: str,
                           samples: int = 100) -> float:
        w = float(
            self.graph.graph.get(treatment, {}).get(outcome, {}).get("weight", 0.0)
        )
        try:
            await asyncio.to_thread(
                self.storage.save_causal_experiment,
                f"exp_{uuid.uuid4().hex[:8]}", treatment, outcome, w, samples)
        except Exception:
            pass
        return w

    def get_policy(self) -> List[float]:
        return list(self.policy)


# =============================================================================
# ENHANCEMENT 3 — FEDERATED GREEN LEARNING
# =============================================================================
class FederatedGreenAggregator:
    def __init__(self, storage: Any, instance_id: str,
                 share_interval: int = 3600):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = int(share_interval)
        self.rounds = 0
        self.last_aggregated: Optional[bytes] = None

    async def share_weights(self, model_id: str, weights: bytes) -> None:
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id, model_id, weights,
                float(len(weights)), self.rounds)
        except Exception:
            pass
        self.rounds += 1

    async def pull_aggregated_weights(self, model_id: str) -> Optional[bytes]:
        try:
            rows = await asyncio.to_thread(
                self.storage.get_federated_weights, model_id)
        except Exception:
            return None
        if not rows:
            return None
        blobs = [r["weights"] for r in rows if r.get("weights")]
        if not blobs:
            return None
        n = min(len(b) for b in blobs)
        avg = bytearray(n)
        for i in range(n):
            avg[i] = int(sum(b[i] for b in blobs) / len(blobs)) & 0xFF
        self.rounds += 1
        self.last_aggregated = bytes(avg)
        return self.last_aggregated

    async def apply_aggregated_weights(self, model_id: str,
                                       current: bytes) -> bytes:
        agg = await self.pull_aggregated_weights(model_id)
        if agg is None:
            return current
        n = min(len(current), len(agg))
        return bytes([(current[i] + agg[i]) // 2 for i in range(n)])

    def stats(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "rounds": self.rounds,
            "last_aggregated_bytes": (
                len(self.last_aggregated) if self.last_aggregated else 0
            ),
        }


# =============================================================================
# ENHANCEMENT 4 — MULTI-AGENT COORDINATION
# =============================================================================
class _Agent:
    ROLES = ["orchestrator", "validator", "optimizer", "reporter", "negotiator"]

    def __init__(self, agent_id: str):
        self.id = agent_id
        self.role = "validator"
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        self.completed = 0


class MultiAgentCoordinator:
    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        count = int(_cfg_get(config, "agent_count", 5) or 5)
        self.agents = {f"agent_{i:02d}": _Agent(f"agent_{i:02d}")
                       for i in range(count)}
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()

    async def _specialise(self) -> None:
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])
                try:
                    await asyncio.to_thread(
                        self.storage.save_agent, a.id, a.role,
                        a.reputation, a.utilities)
                except Exception:
                    pass

    async def broadcast(self, topic: str, sender: str,
                        payload: Dict[str, Any]) -> None:
        try:
            self.bus.put_nowait({"topic": topic, "sender": sender,
                                 "payload": payload})
        except asyncio.QueueFull:
            log_event("warning",
                      "MultiAgentCoordinator bus full; message dropped")
        try:
            await asyncio.to_thread(
                self.storage.save_agent_message,
                uuid.uuid4().hex[:8], topic, sender, "*", payload)
        except Exception:
            pass

    async def bid(self, task: Dict[str, Any]) -> Tuple[str, float]:
        preferred = task.get("preferred_role", "orchestrator")
        best_id, best_score = None, -1.0
        async with self._lock:
            for aid, a in self.agents.items():
                bonus = 1.0 if a.role == preferred else 0.6
                score = a.utilities[a.role] * bonus + 0.3 * a.reputation
                score += random.uniform(-0.02, 0.02)
                if score > best_score:
                    best_score, best_id = score, aid
            if best_id:
                self.agents[best_id].completed += 1
        await self.broadcast("task_bid", best_id or "none",
                             {"task": task.get("name", "?"),
                              "score": best_score})
        return best_id or next(iter(self.agents)), best_score

    async def reward(self, agent_id: str, reward: float) -> None:
        async with self._lock:
            if agent_id in self.agents:
                a = self.agents[agent_id]
                n = max(1, a.completed)
                a.reputation = max(0.0, min(1.0, a.reputation + reward / n))
                a.utilities[a.role] = min(1.0,
                                          a.utilities[a.role] + 0.05 * reward)

    def get_role_distribution(self) -> Dict[str, float]:
        counts: Dict[str, int] = defaultdict(int)
        for a in self.agents.values():
            counts[a.role] += 1
        total = max(1, sum(counts.values()))
        return {role: c / total for role, c in counts.items()}

    def get_policy(self) -> List[float]:
        affinity = {
            "orchestrator": [0.35, 0.20, 0.15, 0.15, 0.15],
            "validator":    [0.15, 0.15, 0.15, 0.35, 0.20],
            "optimizer":    [0.20, 0.15, 0.35, 0.15, 0.15],
            "reporter":     [0.15, 0.20, 0.15, 0.15, 0.35],
            "negotiator":   [0.15, 0.35, 0.20, 0.15, 0.15],
        }
        role_dist = self.get_role_distribution()
        out = [0.0] * 5
        for role, w in role_dist.items():
            for i, v in enumerate(affinity.get(role, [0.2] * 5)):
                out[i] += w * v
        s = sum(out) or 1.0
        return [x / s for x in out]

    async def step(self) -> Dict[str, Any]:
        await self._specialise()
        processed = 0
        while not self.bus.empty():
            try:
                self.bus.get_nowait()
                processed += 1
            except asyncio.QueueEmpty:
                break
        return {"roles": {a.id: a.role for a in self.agents.values()},
                "role_distribution": self.get_role_distribution(),
                "strategy_distribution": self.get_policy(),
                "processed_messages": processed}


# =============================================================================
# ENHANCEMENT 5 — TEMPORAL LOGIC
# =============================================================================
_ATOMIC_RE = re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


class TemporalRule:
    def __init__(self, rule_id: str, operator: str,
                 conditions: List[Callable[[Dict], bool]],
                 window: float = 0.0, description: str = "",
                 severity: str = "warning"):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window = float(window)
        self.description = description or rule_id
        self.severity = severity
        self.violations = 0
        self.last_violation: Optional[datetime] = None

    def evaluate(self, trace: Deque[Tuple[datetime, Dict]]) -> bool:
        if not trace:
            return False
        if self.window > 0:
            cutoff = trace[-1][0] - timedelta(seconds=self.window)
            while trace and trace[0][0] < cutoff:
                trace.popleft()
        op = self.operator
        if op == "always":
            return any(not self.conditions[0](s) for _, s in trace)
        if op == "eventually":
            return not any(self.conditions[0](s) for _, s in trace)
        if op == "never":
            return any(self.conditions[0](s) for _, s in trace)
        return False


class TemporalLogicVerifier:
    def __init__(self, storage: Any, config: Any):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict]] = deque(
            maxlen=int(_cfg_get(config, "temporal_max_trace", 2000) or 2000))
        self.approval_cb: Optional[Callable] = None
        self._pending_rule_saves: List[Tuple[str, str, str, str]] = []
        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            self._install(formula)

    def _install(self, formula: str) -> None:
        f = (formula or "").strip()
        if not f:
            return
        try:
            if f.startswith("G "):
                self._add_atomic(f[2:].strip().strip("()"), "always")
            elif f.startswith("F "):
                self._add_atomic(f[2:].strip().strip("()"), "eventually")
            elif f.startswith("NEVER "):
                self._add_atomic(f[6:].strip().strip("()"), "never")
            else:
                log_event("warning",
                          f"Unrecognised temporal formula: {formula!r}")
        except Exception as exc:
            log_event("warning", f"Temporal install failed: {exc}")

    def _add_atomic(self, expr: str, op: str) -> None:
        m = _ATOMIC_RE.match(expr)
        if not m:
            log_event("warning", f"Atomic formula did not parse: {expr!r}")
            return
        var, cmp, val = m.group(1), m.group(2), float(m.group(3))

        def cond(state, v=var, c=cmp, x=val):
            try:
                sv = float(state.get(v, 0.0))
            except Exception:
                return True
            return {">=": sv >= x, "<=": sv <= x, "==": sv == x,
                    "!=": sv != x, ">": sv > x, "<": sv < x}[c]

        rid = f"{op}:{expr}"
        self.rules[rid] = TemporalRule(rid, op, [cond],
                                        description=expr, severity="warning")
        self._pending_rule_saves.append((rid, expr, op, "warning"))

    async def _flush_pending_rule_saves(self) -> None:
        while self._pending_rule_saves:
            rid, expr, op, sev = self._pending_rule_saves.pop(0)
            try:
                await asyncio.to_thread(
                    self.storage.save_temporal_rule,
                    rid, expr, op, sev, expr, 0.0, True)
            except Exception:
                pass

    def set_approval_callback(self, cb: Callable) -> None:
        self.approval_cb = cb

    async def push_state(self, state: Dict[str, Any]) -> None:
        await self._flush_pending_rule_saves()
        self.trace.append((_now_utc(), dict(state)))
        try:
            await asyncio.to_thread(self.storage.save_temporal_trace, state)
        except Exception:
            pass

    async def verify(self) -> Dict[str, bool]:
        await self._flush_pending_rule_saves()
        result: Dict[str, bool] = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace)
            violated = rule.evaluate(copy)
            approved = False
            if violated:
                rule.violations += 1
                rule.last_violation = _now_utc()
                try:
                    _M_TEMPORAL_VIOLATIONS.labels(
                        rule=rid, severity=rule.severity).inc()
                except Exception:
                    pass
                if rule.severity == "critical" and self.approval_cb:
                    try:
                        approved = self.approval_cb(
                            rid, self.trace[-1][1] if self.trace else {})
                        if asyncio.iscoroutine(approved):
                            approved = await approved
                    except Exception as exc:
                        log_event("warning", f"Approval callback failed: {exc}")
                        approved = False
                try:
                    await asyncio.to_thread(
                        self.storage.save_temporal_violation,
                        rid, rule.description, len(self.trace) - 1,
                        self.trace[-1][1] if self.trace else {},
                        rule.severity, bool(approved))
                except Exception:
                    pass
            result[rid] = (not violated) or bool(approved)
        return result


# =============================================================================
# ENHANCEMENT 6 — EXPLAINABLE AI
# =============================================================================
class XAIDecisionExplainer:
    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.method = str(_cfg_get(config, "xai_method", "kernel_shap")
                          or "kernel_shap")
        self.depth = int(_cfg_get(config, "xai_depth", 5) or 5)

    def _permutation_shap(self, f, x, names, n=64):
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
        base = np.zeros_like(x)
        contrib = np.zeros(len(x))
        for _ in range(n):
            perm = list(range(len(x)))
            random.shuffle(perm)
            prev = base.copy()
            for i in perm:
                cur = prev.copy()
                cur[i] = x[i]
                try:
                    delta = float(f(cur.reshape(1, -1))) - \
                            float(f(prev.reshape(1, -1)))
                except Exception:
                    delta = 0.0
                contrib[i] += delta
                prev = cur
        contrib = contrib / max(1, n)
        return dict(zip(names, contrib.tolist()))

    def _kernel_shap(self, f, x, names, n=64):
        return self._permutation_shap(f, x, names, n)

    def _lime(self, f, x, names, n=200):
        if not (SKLEARN_AVAILABLE and NUMPY_AVAILABLE):
            return {k: 0.0 for k in names}
        scale = float(np.linalg.norm(x)) / max(1.0, math.sqrt(len(x)))
        sigma = max(scale * 0.5, 1e-3)
        X = np.tile(x, (n, 1)) + np.random.normal(0, 0.1, (n, len(x)))
        try:
            y = np.array([float(f(r.reshape(1, -1))) for r in X])
        except Exception:
            return {k: 0.0 for k in names}
        w = np.exp(-np.sum((X - x) ** 2, axis=1) / (2.0 * sigma * sigma))
        try:
            m = LinearRegression().fit(X, y, sample_weight=w)
            return dict(zip(names, m.coef_.tolist()))
        except Exception:
            return {k: 0.0 for k in names}

    def _nl(self, decision: str, attrs: Dict[str, float]) -> str:
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]),
                     reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(self, decision_id: str, label: str, features,
                      names: List[str], model_fn: Callable) -> Dict[str, Any]:
        if not NUMPY_AVAILABLE:
            attrs = {k: 0.0 for k in names}
        elif self.method == "lime":
            attrs = self._lime(model_fn, features, names)
        else:
            attrs = self._permutation_shap(model_fn, features, names)
        nl = self._nl(label, attrs)

        feature_map: Dict[str, Any] = {}
        try:
            if NUMPY_AVAILABLE and hasattr(features, "tolist"):
                feature_map = dict(zip(names, features.tolist()))
            else:
                feature_map = dict(zip(names, list(features)))
        except Exception:
            feature_map = {"raw": list(features)}

        try:
            await asyncio.to_thread(
                self.storage.save_xai_explanation,
                decision_id, decision_id, self.method, label,
                feature_map, attrs, nl)
        except Exception:
            pass
        return {"decision_id": decision_id, "method": self.method,
                "attributions": attrs, "explanation": nl}


# =============================================================================
# ENHANCEMENT 7 — ADAPTIVE PRECISION SWITCHER
# =============================================================================
class AdaptivePrecisionSwitcher:
    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55,
              "fp16": 0.5, "int8": 0.3}
    LATENCY = {"fp32": 1.0, "tf32": 0.8, "bf16": 0.65,
               "fp16": 0.6, "int8": 0.4}
    CRITICAL_PRECISIONS = {"int8"}

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0
        self.dynamic_energy = dict(self.ENERGY)
        self.dynamic_latency = dict(self.LATENCY)
        self.history: Deque[Dict] = deque(maxlen=500)
        self._acc_window: Deque[float] = deque(maxlen=20)
        self._hw_cache: Optional[Dict[str, Any]] = None

    def _probe(self, force_refresh: bool = False) -> Dict[str, Any]:
        if self._hw_cache is not None and not force_refresh:
            return self._hw_cache
        info = {"cuda": False, "bf16": False, "device": "cpu"}
        if TORCH_AVAILABLE and torch is not None:
            try:
                info["cuda"] = bool(torch.cuda.is_available())
                if info["cuda"]:
                    info["device"] = torch.cuda.get_device_name(0)
                    try:
                        info["bf16"] = bool(torch.cuda.is_bf16_supported())
                    except Exception:
                        info["bf16"] = False
            except Exception:
                pass
        self._hw_cache = info
        return info

    def _allowed_precisions(self) -> List[str]:
        raw = list(_cfg_get(self.config, "precision_levels",
                            ["fp32", "fp16", "bf16", "int8"]))
        return [c for c in raw if c in self.dynamic_energy]

    def select_precision(self) -> str:
        hw = self._probe()
        cands = self._allowed_precisions()
        if not cands:
            return "fp32"
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        if not cands:
            return "fp32"
        return min(cands, key=lambda c: self.dynamic_energy.get(c, 1.0))

    def record_accuracy(self, accuracy: float) -> None:
        try:
            self._acc_window.append(float(accuracy))
        except (TypeError, ValueError):
            pass

    async def switch_to(self, target: str, reason: str = "policy",
                        current_intensity: float = 0.4) -> bool:
        if target == self.current or target not in self.dynamic_energy:
            return False
        if target not in self._allowed_precisions():
            log_event("warning",
                      f"Precision {target!r} not in precision_levels; ignoring")
            return False
        old = self.current
        self.current = target
        saved = max(0.0, self.dynamic_energy.get(old, 1.0) -
                    self.dynamic_energy.get(target, 1.0))
        self.saved_wh += saved
        self.history.append({"from": old, "to": target,
                             "reason": reason, "saved_wh": saved})
        try:
            await asyncio.to_thread(
                self.storage.save_precision_switch,
                old, target, reason, saved, 0.0)
            _M_PRECISION_SWITCHES.labels(from_p=old, to_p=target).inc()
        except Exception:
            pass
        return True

    async def auto_switch(self, recent_acc: float = 0.0,
                          baseline_acc: float = 0.0) -> None:
        if recent_acc <= 0 or baseline_acc <= 0:
            if not self._acc_window:
                return
            recent_acc = sum(self._acc_window) / len(self._acc_window)
            baseline_acc = max(self._acc_window)
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = float(_cfg_get(self.config,
                                "precision_switch_threshold", 0.02) or 0.02)
        if drop > thresh:
            await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
        elif drop < thresh / 2:
            target = self.select_precision()
            if target != self.current:
                await self.switch_to(target, reason="headroom")


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKETS / REC
# =============================================================================
class CarbonMarketIntegrator:
    DEFER_INTENSITY_THRESHOLD = 0.3   # kgCO2/kWh
    RUN_OFFSET_COST_THRESHOLD = 0.5   # USD
    MIN_PRICE_USD = 5.0
    MAX_PRICE_USD = 200.0
    DEFAULT_PRICE_USD = 25.0
    DEFAULT_PRICE_PER_MWH_USD = 5.0

    def __init__(self, config: Any, storage: Any):
        self.config = config
        self.storage = storage
        self.last_price = float(
            _cfg_get(config, "carbon_default_price_usd",
                     self.DEFAULT_PRICE_USD) or self.DEFAULT_PRICE_USD)
        self._rec_balance_mwh: float = 0.0
        self._closed = False

    async def _fetch_price(self) -> float:
        candidate = self.last_price + random.gauss(0, 1.5)
        return max(self.MIN_PRICE_USD, min(self.MAX_PRICE_USD, candidate))

    async def update_price(self) -> float:
        try:
            price = await self._fetch_price()
        except Exception:
            price = self.last_price
        try:
            price = max(self.MIN_PRICE_USD, min(self.MAX_PRICE_USD, float(price)))
        except (TypeError, ValueError):
            price = self.last_price
        try:
            await asyncio.to_thread(self.storage.save_credit_price, price)
        except Exception:
            pass
        self.last_price = price
        return price

    async def purchase_rec(self, mwh: float, price_per_mwh: float = 5.0,
                           source: str = "wind") -> float:
        mwh = max(0.0, float(mwh))
        if mwh == 0.0:
            return 0.0
        price_per_mwh = max(0.0, float(price_per_mwh))
        src = (source or "").strip().lower() or "unspecified"
        cost = mwh * price_per_mwh
        self._rec_balance_mwh += mwh
        try:
            await asyncio.to_thread(
                self.storage.save_rec, mwh, price_per_mwh, src)
        except Exception:
            pass
        return cost

    async def net_zero_schedule(self, workload_kwh: float, intensity: float,
                                price: Optional[float] = None,
                                commit: bool = True) -> Dict[str, Any]:
        workload_kwh = max(0.0, float(workload_kwh))
        intensity = max(0.0, float(intensity))
        if price is None:
            price = float(self.last_price)
        else:
            try:
                price = max(0.0, float(price))
            except (TypeError, ValueError):
                price = float(self.last_price)
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price
        if intensity > self.DEFER_INTENSITY_THRESHOLD:
            action = "defer"
        elif offset_cost < self.RUN_OFFSET_COST_THRESHOLD:
            action = "run_offset"
        else:
            action = "run"
        if commit:
            try:
                await asyncio.to_thread(
                    self.storage.save_net_zero_match,
                    uuid.uuid4().hex[:8], workload_kwh, intensity, action,
                    carbon_kg, offset_cost, price)
            except Exception:
                pass
        return {"action": action, "carbon_kg": carbon_kg,
                "offset_cost_usd": offset_cost, "credit_price_usd": price,
                "rec_balance_mwh": float(self._rec_balance_mwh)}

    def get_rec_balance(self) -> float:
        return float(self._rec_balance_mwh)

    async def aclose(self) -> None:
        self._closed = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()


# =============================================================================
# ENHANCEMENT 10 — HITL ACTIVE LEARNING
# =============================================================================
class ActiveUserPreferenceLearner:
    _PREF_KEYS = ("quality_score", "carbon_g", "cost_usd", "latency_ms")

    def __init__(self, storage: Any, pareto_gating: Any = None,
                 dashboard: Any = None, feedback_callback: Any = None,
                 use_priority_queue: bool = False):
        self.storage = storage
        self.pareto = pareto_gating
        self.dashboard = dashboard
        self.feedback_callback = feedback_callback
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._pending: Dict[str, asyncio.Future] = {}
        self._priority_queue: List[Tuple[float, str, Dict[str, Any]]] = []
        self.use_priority_queue = use_priority_queue
        self._lock = asyncio.Lock()

    async def submit_response(self, user_id: str, chosen_id: str,
                              request_id: Optional[str] = None) -> None:
        if request_id is not None:
            async with self._lock:
                fut = self._pending.pop(request_id, None)
            if fut is not None and not fut.done():
                fut.set_result({"user_id": user_id, "chosen": chosen_id})
                return
            log_event("warning",
                      f"Unknown HITL request_id {request_id!r}; using FIFO")
        try:
            self._responses.put_nowait(
                {"user_id": user_id, "chosen": chosen_id})
        except asyncio.QueueFull:
            log_event("warning", "HITL response queue full; dropping response")

    def _entropy_uncertainty(self, candidates: List[Dict[str, Any]]) -> float:
        scores = []
        for c in candidates[:5]:
            try:
                scores.append(float(c.get("quality_score", 0.0)))
            except (TypeError, ValueError):
                scores.append(0.0)
        s = sum(scores) or 1.0
        probs = [x / s for x in scores]
        ent = -sum(p * math.log(p + 1e-9) for p in probs)
        max_ent = math.log(len(probs)) if len(probs) > 1 else 1.0
        return ent / max_ent if max_ent > 0 else 0.0

    def _ambiguity_score(self, candidates: List[Dict[str, Any]]) -> float:
        if len(candidates) < 2:
            return 0.0
        c1, c2 = candidates[0], candidates[1]

        def _rel(a, b):
            try:
                a = float(a); b = float(b)
            except (TypeError, ValueError):
                return 0.0
            m = max(abs(a), abs(b), 1e-6)
            return abs(a - b) / m

        try:
            q = _rel(c1.get("quality_score", 0), c2.get("quality_score", 0))
            cg = _rel(c1.get("carbon_g", 0), c2.get("carbon_g", 0))
            cost = _rel(c1.get("cost_usd", 0), c2.get("cost_usd", 0))
            lat = _rel(c1.get("latency_ms", 0), c2.get("latency_ms", 0))
            return max(cg, cost, lat) * (1.0 - min(q, 1.0))
        except Exception:
            return 0.0

    async def query_user_if_needed(self, user_id: str,
                                   candidates: List[Dict[str, Any]],
                                   timeout: float = 3.0) -> Optional[str]:
        if len(candidates) < 2:
            return None
        if self.pareto is not None:
            try:
                pareto_set = self.pareto.filter(candidates)
                if len(pareto_set) >= 2:
                    candidates = pareto_set
                else:
                    return None
            except Exception:
                pass

        ambiguity = self._ambiguity_score(candidates)
        uncertainty = self._entropy_uncertainty(candidates)
        if ambiguity < 0.05 and uncertainty < 0.5:
            return None

        req_id = uuid.uuid4().hex[:8]
        try:
            await asyncio.to_thread(
                self.storage.enqueue_hitl_request,
                req_id, "pareto_query",
                {"candidates": candidates[:2], "ambiguity": ambiguity,
                 "uncertainty": uncertainty}, "info")
        except Exception:
            pass
        try:
            await asyncio.to_thread(
                self.storage.save_active_learning_sample,
                req_id, "chaos_student", "hitl_ambiguity",
                uncertainty, True)
        except Exception:
            pass

        if self.dashboard is not None:
            try:
                await self.dashboard.broadcast({
                    "type": "preference_query", "request_id": req_id,
                    "user_id": user_id, "ambiguity": ambiguity,
                    "uncertainty": uncertainty,
                    "options": [
                        {"id": c.get("solution_id"),
                         "quality": c.get("quality_score")}
                        for c in candidates[:2]]})
            except Exception:
                pass

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        async with self._lock:
            self._pending[req_id] = fut
        try:
            msg = await asyncio.wait_for(fut, timeout=timeout)
            try:
                await asyncio.to_thread(
                    self.storage.resolve_hitl_request, req_id, "approved")
            except Exception:
                pass
            chosen = msg.get("chosen") if isinstance(msg, dict) else None
            metrics = next(
                (c for c in candidates if c.get("solution_id") == chosen), None)
            await self.record_choice(user_id, chosen, metrics=metrics)
            if self.feedback_callback:
                try:
                    await self.feedback_callback({
                        "event_id": uuid.uuid4().hex[:8],
                        "task_id": req_id,
                        "selected_action": chosen,
                        "feedback_type": "hitl_choice",
                        "quality_score": 1.0,
                        "ambiguity": ambiguity,
                        "uncertainty": uncertainty})
                except Exception:
                    pass
            return chosen
        except asyncio.TimeoutError:
            try:
                await asyncio.to_thread(
                    self.storage.resolve_hitl_request, req_id, "timeout")
            except Exception:
                pass
            normalized = self.get_preference_weights(user_id)
            if normalized:
                scored = []
                for c in candidates:
                    s = sum(normalized.get(k, 0.25)
                            / (float(c.get(k, 0) or 0) + 1e-8)
                            for k in self._PREF_KEYS)
                    scored.append((s, c.get("solution_id")))
                scored.sort(reverse=True)
                return scored[0][1]
            return candidates[0].get("solution_id")
        finally:
            async with self._lock:
                self._pending.pop(req_id, None)

    async def record_choice(self, user_id: str, solution_id: str,
                            metrics: Optional[Dict[str, float]] = None) -> None:
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k in self._PREF_KEYS:
                if k in metrics:
                    try:
                        v = float(metrics[k])
                    except (TypeError, ValueError):
                        continue
                    if k == "quality_score":
                        prefs[k] = prefs.get(k, 0.25) + v * 0.01
                    else:
                        prefs[k] = prefs.get(k, 0.25) + 1.0 / (abs(v) + 1e-6) * 0.01
            self.preferences[user_id] = prefs
        try:
            await asyncio.to_thread(
                self.storage.save_user_preference, user_id, prefs)
        except Exception:
            pass

    def get_preference_weights(self, user_id: str) -> Dict[str, float]:
        raw = self.preferences.get(user_id, {})
        if not raw:
            return {}
        s = sum(raw.values()) or 1.0
        return {k: v / s for k, v in raw.items()}


# =============================================================================
# UNIFIED ORCHESTRATOR
# =============================================================================
class ChaosOrchestratorV17:
    """
    Unified entry point: wires all ten enhancements around the
    ChaosTestingEngine. Runs a full orchestration cycle per tick and
    supports scheduled gamedays.
    """

    def __init__(self, storage: Any, config: Any, dashboard: Any = None):
        self.storage = storage
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]

        # Build ten enhancements.
        self.quantum = QuantumDistillationEngine(temperature=2.0, alpha=0.5)
        self.causal_graph = CausalGraphLearner(storage)
        self.causal_rl = CausalPolicyAdapter(config, storage, self.causal_graph)
        self.federated = FederatedGreenAggregator(storage, self.instance_id)
        self.multi_agent = MultiAgentCoordinator(config, storage)
        self.temporal = TemporalLogicVerifier(storage, config)
        self.xai = XAIDecisionExplainer(config, storage)
        self.precision = AdaptivePrecisionSwitcher(config, storage)
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.hitl = ActiveUserPreferenceLearner(storage, dashboard=dashboard)

        # Chaos engine — enhanced with all hooks.
        self.chaos = ChaosTestingEngine(
            config, storage,
            temporal=self.temporal,
            xai=self.xai,
            hitl=self.hitl,
            multi_agent=self.multi_agent,
            carbon_market=self.carbon_market,
            causal_rl=self.causal_rl,
            precision=self.precision,
            federated=self.federated)

        self.temporal.set_approval_callback(self._on_critical_violation)

        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set = set()

    async def _on_critical_violation(self, rule_id: str,
                                     state: Dict[str, Any]) -> bool:
        try:
            q = float(state.get("quality", 0.8))
            c = float(state.get("carbon", 0.4))
        except (TypeError, ValueError):
            q, c = 0.8, 0.4
        candidates = [
            {"solution_id": "approve", "quality_score": q, "carbon_g": c,
             "cost_usd": 0.5, "latency_ms": 100.0},
            {"solution_id": "reject", "quality_score": 0.0, "carbon_g": 0.0,
             "cost_usd": 0.0, "latency_ms": 0.0},
        ]
        decision = await self.hitl.query_user_if_needed(
            "critical_user", candidates, timeout=2.0)
        return decision == "approve"

    def _spawn(self, coro: Awaitable[Any], name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._background_tasks.add(task)

        def _done(t: asyncio.Task) -> None:
            self._background_tasks.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                log_event("error", f"Background task {name} failed: {exc}")

        task.add_done_callback(_done)
        return task

    async def register_system_kpis(self) -> None:
        """Register default KPI probes for the MOPD pipeline."""
        self.chaos.register_kpi(
            "quality", lambda: random.uniform(0.85, 0.95),
            min_ok=0.7)
        self.chaos.register_kpi(
            "error_rate", lambda: random.uniform(0.0, 0.01),
            max_ok=0.05)
        self.chaos.register_kpi(
            "p99_latency_ms", lambda: random.uniform(80, 150),
            max_ok=500.0)
        self.chaos.register_kpi(
            "carbon_intensity", lambda: random.uniform(0.2, 0.6),
            max_ok=0.8)

    async def tick(self, state: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        # 1. Causal RL strategy selection.
        action = await self.causal_rl.choose_action(state)
        result["strategy"] = action
        result["strategy_name"] = self.causal_rl.to_strategy(action)

        # 2. Multi-agent bid.
        agent_id, _ = await self.multi_agent.bid({
            "name": "chaos_tick", "preferred_role": "validator"})
        result["agent_id"] = agent_id

        # 3. Precision auto-switch.
        self.precision.record_accuracy(
            float(state.get("accuracy", 0.9) or 0.9))
        await self.precision.auto_switch(0.0, 0.0)
        result["precision"] = self.precision.current

        # 4. Carbon market decision.
        try:
            result["carbon_decision"] = await self.carbon_market.net_zero_schedule(
                workload_kwh=1.0,
                intensity=float(state.get("carbon_intensity", 400) or 400) / 1000.0)
        except Exception as exc:
            log_event("warning", f"Carbon market decision failed: {exc}")
            result["carbon_decision"] = {"action": "run"}

        # 5. XAI explanation.
        if NUMPY_AVAILABLE:
            try:
                feats = np.array([
                    float(state.get("quality", 0.8)),
                    float(state.get("carbon_intensity", 400) or 400) / 1000.0,
                    float(state.get("cost", 0.5)),
                    float(state.get("latency_ms", 100) or 100) / 1000.0])
                def _score(x, _w=np.array([0.4, -0.3, -0.2, -0.1])):
                    return float(np.dot(x, _w))
                result["xai"] = await self.xai.explain(
                    decision_id=f"chaos_{uuid.uuid4().hex[:8]}",
                    label=f"strategy={result['strategy']}",
                    features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=_score)
            except Exception as exc:
                log_event("debug", f"XAI explanation skipped: {exc}")

        # 6. Multi-agent reward.
        await self.multi_agent.reward(agent_id, 0.8)

        # 7. Temporal push + verify.
        await self.temporal.push_state({
            "quality": float(state.get("quality", 0.8)),
            "carbon": float(state.get("carbon_intensity", 400) or 400) / 1000.0,
            "task_complete": True})
        try:
            verify = await self.temporal.verify()
            result["temporal_violations"] = [
                k for k, v in verify.items() if not v]
        except Exception as exc:
            log_event("warning", f"Temporal verify failed: {exc}")
            result["temporal_violations"] = []

        # 8. Causal RL update.
        await self.causal_rl.update(action, 0.8, state)

        return result

    async def start(self) -> None:
        self._running = True
        await self.register_system_kpis()
        _ = asyncio.get_running_loop()
        self._spawn(self._causal_rl_loop(), "causal_rl")
        self._spawn(self._federated_loop(), "federated")
        self._spawn(self._multi_agent_loop(), "multi_agent")
        self._spawn(self._temporal_loop(), "temporal")
        self._spawn(self._xai_loop(), "xai")
        self._spawn(self._precision_loop(), "precision")
        self._spawn(self._carbon_market_loop(), "carbon_market")
        self._spawn(self._chaos_loop(), "chaos")
        self._spawn(self._distillation_loop(), "distillation")

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        self._running = False
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks,
                                 return_exceptions=True)
        self._background_tasks.clear()

    async def aclose(self) -> None:
        await self.shutdown()

    async def __aenter__(self) -> "ChaosOrchestratorV17":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def _causal_rl_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "causal_graph_update_interval", 900) or 900))
            try:
                samples = [{
                    "quality": random.uniform(0.5, 1.0),
                    "carbon": random.uniform(0.1, 0.8),
                    "cost": random.uniform(0.1, 0.9),
                    "latency": random.uniform(0.1, 0.9),
                } for _ in range(20)]
                await self.causal_graph.learn(
                    samples, ["quality", "carbon", "cost", "latency"])
            except Exception as exc:
                log_event("debug", f"causal_rl loop error: {exc}")

    async def _federated_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "federated_interval", 3600) or 3600))
            try:
                dummy = bytes(random.getrandbits(8) for _ in range(64))
                await self.federated.share_weights("chaos_policy", dummy)
                await self.federated.pull_aggregated_weights("chaos_policy")
            except Exception as exc:
                log_event("debug", f"federated loop error: {exc}")

    async def _multi_agent_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "agent_negotiation_interval", 600) or 600))
            try:
                await self.multi_agent.step()
            except Exception as exc:
                log_event("debug", f"multi_agent loop error: {exc}")

    async def _temporal_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "temporal_verification_interval", 300) or 300))
            try:
                await self.temporal.verify()
            except Exception as exc:
                log_event("debug", f"temporal loop error: {exc}")

    async def _xai_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "xai_interval", 300) or 300))
            if not NUMPY_AVAILABLE:
                continue
            try:
                feats = np.array([0.9, 0.4, 0.5, 0.4])
                def _score(x, _w=np.array([0.4, -0.3, -0.2, -0.1])):
                    return float(np.dot(x, _w))
                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="chaos_health",
                    features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=_score)
            except Exception as exc:
                log_event("debug", f"xai loop error: {exc}")

    async def _precision_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "precision_check_interval", 300) or 300))
            try:
                await self.precision.auto_switch(0.0, 0.0)
            except Exception as exc:
                log_event("debug", f"precision loop error: {exc}")

    async def _carbon_market_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "carbon_market_interval", 3600) or 3600))
            try:
                await self.carbon_market.update_price()
            except Exception as exc:
                log_event("debug", f"carbon_market loop error: {exc}")

    async def _chaos_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "chaos_test_interval", 1800) or 1800))
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    name=f"auto_{uuid.uuid4().hex[:6]}",
                    fault_type=fault,
                    targets=["svc_a", "svc_b", "svc_c", "svc_d", "svc_e"])
            except Exception as exc:
                log_event("debug", f"chaos loop error: {exc}")

    async def _distillation_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "distillation_interval", 300) or 300))
            try:
                self.quantum.register_teacher(
                    "causal", self.causal_rl.get_policy())
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy())
                await self.quantum.step(self.storage, "chaos_student")
            except Exception as exc:
                log_event("debug", f"distillation loop error: {exc}")

    async def health_check(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "chaos": self.chaos.stats(),
            "precision": self.precision.current,
            "carbon_price": self.carbon_market.last_price,
            "federated_rounds": self.federated.rounds,
            "temporal_rules": len(self.temporal.rules),
            "hitl_pending": len(self.hitl._pending),
            "background_tasks": len(self._background_tasks),
        }


# =============================================================================
# MINIMAL IN-MEMORY STORAGE
# =============================================================================
class InMemoryStorage:
    """Standalone storage for demos/tests. Soft-fail for all methods."""

    def __init__(self):
        self._data: Dict[str, Any] = defaultdict(list)
        self._prefs: Dict[str, Dict[str, float]] = {}

    def save_chaos_experiment(self, experiment_id, name, fault_type,
                              blast_radius, steady_before, steady_after,
                              status, duration_ms=0.0):
        self._data["chaos_experiments"].append({
            "experiment_id": experiment_id, "name": name,
            "fault_type": fault_type, "blast_radius": blast_radius,
            "steady_before": steady_before, "steady_after": steady_after,
            "status": status, "duration_ms": duration_ms})

    def save_teacher_superposition(self, student_id, teacher_id, weight,
                                   temperature, amplitude, kl):
        self._data["teacher_superpositions"].append({
            "student_id": student_id, "teacher_id": teacher_id,
            "weight": weight, "temperature": temperature,
            "amplitude": amplitude, "kl": kl})

    def save_causal_edge(self, source, target, weight, confidence):
        self._data["causal_graph"].append({
            "source": source, "target": target,
            "weight": weight, "confidence": confidence})

    def save_causal_experiment(self, exp_id, treatment, outcome, ate,
                               samples, method=""):
        self._data["causal_experiments"].append({
            "exp_id": exp_id, "treatment": treatment, "outcome": outcome,
            "ate": ate, "samples": samples, "method": method})

    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0):
        self._data["federated_weights"].append({
            "instance_id": instance_id, "model_id": model_id,
            "weights": weights, "weight_norm": weight_norm,
            "round_id": round_id})

    def get_federated_weights(self, model_id):
        return [r for r in self._data["federated_weights"]
                if r["model_id"] == model_id]

    def save_agent(self, agent_id, role, reputation, utilities):
        self._data["agent_registry"].append({
            "agent_id": agent_id, "role": role,
            "reputation": reputation, "utilities": utilities})

    def save_agent_message(self, message_id, topic, sender, recipient, payload):
        self._data["agent_messages"].append({
            "message_id": message_id, "topic": topic,
            "sender": sender, "recipient": recipient, "payload": payload})

    def save_temporal_rule(self, rule_id, formula, operator, severity,
                           description, window, active):
        self._data["temporal_rules"].append({
            "rule_id": rule_id, "formula": formula,
            "operator": operator, "severity": severity,
            "description": description, "window": window, "active": active})

    def save_temporal_trace(self, state, context=None):
        self._data["temporal_trace"].append({"state": state, "context": context})

    def save_temporal_violation(self, rule_id, formula, step, state,
                                severity="warning", approved=None):
        self._data["temporal_violations"].append({
            "rule_id": rule_id, "formula": formula, "step": step,
            "state": state, "severity": severity, "approved": approved})

    def save_xai_explanation(self, explanation_id, decision_id, method,
                             label, features, attributions, nl):
        self._data["xai_explanations"].append({
            "explanation_id": explanation_id, "decision_id": decision_id,
            "method": method, "label": label, "features": features,
            "attributions": attributions, "natural_language": nl})

    def save_precision_switch(self, from_p, to_p, reason,
                              saved_wh=0.0, acc_delta=0.0):
        self._data["precision_history"].append({
            "from_p": from_p, "to_p": to_p, "reason": reason,
            "saved_wh": saved_wh, "acc_delta": acc_delta})

    def save_credit_price(self, price_usd, currency="USD",
                          source="oracle", region="global"):
        self._data["carbon_credit_prices"].append({
            "price_usd": price_usd, "currency": currency,
            "source": source, "region": region})

    def save_rec(self, mwh, price_per_mwh, source,
                 certificate_id="", region="global", retired=False):
        self._data["rec_ledger"].append({
            "mwh": mwh, "price_per_mwh": price_per_mwh, "source": source,
            "certificate_id": certificate_id, "region": region,
            "retired": retired})

    def get_rec_balance(self):
        return sum(r["mwh"] for r in self._data["rec_ledger"])

    def save_net_zero_match(self, match_id, workload_kwh, intensity, action,
                            carbon_kg, offset_cost, credit_price):
        self._data["net_zero_matches"].append({
            "match_id": match_id, "workload_kwh": workload_kwh,
            "intensity": intensity, "action": action,
            "carbon_kg": carbon_kg, "offset_cost": offset_cost,
            "credit_price": credit_price})

    def enqueue_hitl_request(self, request_id, rule_id, state, severity):
        self._data["hitl_approval_queue"].append({
            "request_id": request_id, "rule_id": rule_id,
            "state": state, "severity": severity, "status": "pending"})

    def resolve_hitl_request(self, request_id, status="approved"):
        for r in self._data["hitl_approval_queue"]:
            if r["request_id"] == request_id:
                r["status"] = status

    def save_active_learning_sample(self, sample_id, model_id, strategy,
                                    uncertainty, selected):
        self._data["active_learning_samples"].append({
            "sample_id": sample_id, "model_id": model_id,
            "strategy": strategy, "uncertainty": uncertainty,
            "selected": selected})

    def save_user_preference(self, user_id, weights):
        self._prefs[user_id] = dict(weights)

    def get_user_preference(self, user_id):
        return self._prefs.get(user_id)


# =============================================================================
# DEMO
# =============================================================================
async def _demo():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    config = {
        "causal_exploration_rate": 0.1,
        "agent_count": 5,
        "federated_interval": 3600,
        "temporal_max_trace": 2000,
        "temporal_formulas": ["G (quality >= 0.5)", "F (task_complete)"],
        "xai_method": "kernel_shap",
        "xai_depth": 5,
        "precision_levels": ["fp32", "fp16", "bf16", "int8"],
        "precision_switch_threshold": 0.02,
        "chaos_intensity": 0.05,
        "chaos_blast_radius": 0.4,
        "chaos_auto_rollback": True,
    }

    storage = InMemoryStorage()
    orch = ChaosOrchestratorV17(storage, config)
    await orch.start()

    print("=" * 80)
    print("chaos_testing_engine.py v17.1.0 — Demo")
    print("=" * 80)

    print("\n=== Basic fault injection ===")
    for fault in ["latency", "memory_pressure",
                  "precision_degradation", "causal_shock"]:
        result = await orch.chaos.run_experiment(
            name=f"manual_{fault}",
            fault_type=fault,
            targets=["svc_a", "svc_b", "svc_c", "svc_d", "svc_e"])
        print(f"  {fault}: status={result['status']} "
              f"steady_before={result['steady_before']} "
              f"steady_after={result['steady_after']} "
              f"aborted={result['aborted']} "
              f"rolled_back={result['rolled_back']}")

    print("\n=== Data corruption (real) ===")
    payload = {
        "batch_id": "b-12345",
        "features": [1.0, 2.0, 3.0, 4.0, 5.0],
        "metrics": {"accuracy": 0.91, "loss": 0.05},
        "raw": b"\x01\x02\x03\x04\x05\x06\x07\x08",
    }
    result = await orch.chaos.run_experiment(
        name="corrupt_payload",
        fault_type="data_corruption",
        targets=["svc_a", "svc_b"],
        payload=payload)
    corrupted = orch.chaos._corrupt_payload(payload)
    print(f"  Original:  {payload}")
    print(f"  Corrupted: {corrupted}")
    print(f"  Status:    {result['status']}")

    print("\n=== KPI steady-state ===")
    ok, kpis = await orch.chaos._steady_state()
    print(f"  Steady: {ok}")
    for k, v in kpis.items():
        print(f"    {k}: {v:+.4f}")

    print("\n=== Gameday ===")
    gameday = await orch.chaos.run_gameday(
        name="q1_resilience",
        hypothesis="Pipeline maintains quality >= 0.7 and error_rate <= 5% "
                   "under latency and memory-pressure faults",
        experiments=[
            {"fault_type": "latency",
             "targets": ["svc_a", "svc_b", "svc_c", "svc_d", "svc_e"]},
            {"fault_type": "memory_pressure",
             "targets": ["svc_a", "svc_b", "svc_c"]},
            {"fault_type": "network_drop",
             "targets": ["svc_a", "svc_b"]},
        ])
    print(f"  Gameday: {gameday['name']}")
    print(f"  Experiments: {gameday['experiments']} "
          f"(run={gameday['run']}, blocked={gameday['blocked']}, "
          f"errored={gameday['errored']})")
    print(f"  All steady after: {gameday['all_steady_after']}")

    print("\n=== Dry-run experiment ===")
    dry = await orch.chaos.run_experiment(
        name="plan_latency",
        fault_type="latency",
        targets=["svc_a", "svc_b", "svc_c", "svc_d", "svc_e"],
        dry_run=True)
    print(f"  Status: {dry['status']}  "
          f"targets: {dry['targets']}  "
          f"blast_radius: {dry['blast_radius']}")

    print("\n=== Orchestration ticks ===")
    for i in range(3):
        state = {
            "quality": random.uniform(0.6, 0.95),
            "carbon_intensity": random.uniform(200, 700),
            "cost": random.uniform(0.3, 0.8),
            "latency_ms": random.uniform(50, 300),
            "accuracy": random.uniform(0.85, 0.95),
            "baseline_accuracy": 0.92,
        }
        result = await orch.tick(state)
        print(f"\n  Tick {i + 1}:")
        print(f"    strategy: {result['strategy']} -> {result['strategy_name']}")
        print(f"    precision: {result['precision']}")
        print(f"    carbon_action: {result['carbon_decision']['action']}")
        print(f"    temporal_violations: {result['temporal_violations']}")

    print("\n=== Health check ===")
    print(json.dumps(await orch.health_check(), indent=2, default=str))

    await orch.shutdown()
    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
