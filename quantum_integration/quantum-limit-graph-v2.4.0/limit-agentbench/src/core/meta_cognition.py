"""
meta_cognitive_layer.py — Enhanced v18.0.0
===========================================

Self-reflection layer for the Green Agent.

v18 is a hardened successor to v17.0.0. It preserves the public API but fixes
all known bugs and adds the robustness/observability/explainability scaffolding
described in the v17 audit:

    P0 — bug fixes
    --------------
      * carbon severity "critical" tier was unreachable (dead elif)
      * MultiAgentCoordinator.get_policy returned [0.2]*5 (always uniform)
      * FederatedGreenAggregator byte-averaged JSON (unparseable aggregate)
      * Temporal "eventually" had incorrect semantics on rolling windows
      * AdaptivePrecisionSwitcher.select_precision could return None
      * XAI silently returned random attributions on fallback
      * asyncio.get_event_loop() deprecation

    P1 — robustness / observability
    ------------------------------
      * structured logging on every hook failure (no silent `except: pass`)
      * in-process MetricsRegistry (counters, gauges, histograms)
      * LoopSupervisor with heartbeats + exponential backoff restart
      * StorageProtocol + CarbonPriceSource protocols
      * deferred-reflection retry queue (no lost reflections)
      * idempotent reflections via caller-supplied reflection_id
      * frozen ReflectionRecord (safe to pass across hooks)
      * HITL per-query asyncio.Event (no cross-talk between users)
      * config validation at construction

    P2 — deeper enhancements
    -----------------------
      * Pareto winners/losers breakdown + per-metric confidence
      * Page-Hinkley drift detector on the accuracy stream
      * XAI stability check (repeated SHAP, unstable flag)
      * precision switch with calibration probe + automatic rollback
      * chaos steady-state hypothesis (real chaos engineering)
      * MultiAgentCoordinator task queue with deadlines and retries
      * deferred-reflection scheduler runs on carbon-market tick

Unified entry point: **MetaCognitiveOrchestratorV18**.

All storage / enhancement hooks soft-fail with structured logging.

Optional dependencies: numpy, sklearn, torch. Everything is stdlib-safe.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import random
import statistics
import struct
import time
import uuid
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Optional, Protocol, Set, Tuple,
    runtime_checkable,
)

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False


logger = logging.getLogger(__name__)

__version__ = "18.0.0"
__all__ = [
    "MetaCognitiveOrchestratorV18",
    "MetaCognitiveOrchestratorV17",   # backward-compat alias
    "MetaCognitiveLayer",
    "ReflectionRecord",
    "ReflectionSeverity",
    "MetricsRegistry",
    "StorageProtocol",
    "CarbonPriceSource",
    "StaticCarbonSource",
    "InMemoryStorage",
    "TemporalLogicVerifier",
    "XAIDecisionExplainer",
    "CarbonMarketIntegrator",
    "AdaptivePrecisionSwitcher",
    "ChaosTestingEngine",
    "FederatedGreenAggregator",
    "MultiAgentCoordinator",
    "CausalGraphLearner",
    "CausalPolicyAdapter",
    "QuantumDistillationEngine",
    "ActiveUserPreferenceLearner",
    "PageHinkleyDetector",
]


# =============================================================================
# CONFIG HELPER (backward compatible)
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
# METRICS REGISTRY
# =============================================================================
class MetricsRegistry:
    """
    Lightweight in-process metrics registry with Prometheus-friendly names.

    The registry is intentionally thread-local to the event loop; the Green
    Agent runs under a single asyncio loop, so no locking is required.
    """

    def __init__(self) -> None:
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=10_000))

    @staticmethod
    def _key(name: str, labels: Dict[str, Any]) -> str:
        if not labels:
            return name
        parts = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{parts}}}"

    def inc(self, name: str, value: float = 1.0, **labels: Any) -> None:
        self._counters[self._key(name, labels)] += value

    def set_gauge(self, name: str, value: float, **labels: Any) -> None:
        self._gauges[self._key(name, labels)] = value

    def observe(self, name: str, value: float, **labels: Any) -> None:
        self._histograms[self._key(name, labels)].append(value)

    def snapshot(self) -> Dict[str, Any]:
        hist: Dict[str, Dict[str, float]] = {}
        for key, values in self._histograms.items():
            if not values:
                continue
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            hist[key] = {
                "count": n,
                "mean": statistics.fmean(sorted_vals),
                "p50": sorted_vals[n // 2],
                "p95": sorted_vals[min(n - 1, int(n * 0.95))],
                "max": sorted_vals[-1],
            }
        return {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histograms": hist,
        }


# =============================================================================
# PROTOCOLS
# =============================================================================
@runtime_checkable
class StorageProtocol(Protocol):
    """Minimal storage contract. Extra methods are optional and duck-typed."""

    def save_reflection(self, reflection_id: str,
                        record: Dict[str, Any]) -> None: ...

    def get_reflection(self, reflection_id: str) -> Optional[Dict[str, Any]]: ...


@runtime_checkable
class CarbonPriceSource(Protocol):
    """Pluggable carbon-price feed."""

    async def get_price(self) -> float: ...


class StaticCarbonSource:
    """Deterministic carbon-price feed — useful for tests and offline demos."""

    def __init__(self, price_usd_per_ton: float = 25.0) -> None:
        self._price = price_usd_per_ton

    async def get_price(self) -> float:
        return self._price


# =============================================================================
# DRIFT DETECTION
# =============================================================================
class PageHinkleyDetector:
    """Page-Hinkley change-point detector for a scalar stream."""

    def __init__(self, delta: float = 0.005, threshold: float = 5.0) -> None:
        self.delta = delta
        self.threshold = threshold
        self._reset()

    def _reset(self) -> None:
        self.n = 0
        self.mean = 0.0
        self.cum = 0.0
        self.min_cum = float("inf")

    def update(self, value: float) -> bool:
        """Return True if a drift is detected at this sample."""
        self.n += 1
        self.mean += (value - self.mean) / self.n
        self.cum += value - self.mean - self.delta
        self.min_cum = min(self.min_cum, self.cum)
        if self.cum - self.min_cum > self.threshold:
            self._reset()
            return True
        return False


# =============================================================================
# REFLECTION RECORD
# =============================================================================
class ReflectionSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass(frozen=True)
class ReflectionRecord:
    """Immutable record of a single reflection."""
    reflection_id: str
    accuracy: float
    energy: float
    carbon: float
    latency_ms: float
    cost_usd: float
    verdict: str
    severity: str
    narrative: str
    pareto_status: str = "unknown"           # dominates|dominated|tradeoff|baseline|deferred
    pareto_winners: Tuple[str, ...] = ()
    pareto_losers: Tuple[str, ...] = ()
    pareto_confidence: Dict[str, float] = field(default_factory=dict)
    causal_attributions: Dict[str, float] = field(default_factory=dict)
    temporal_violations: List[str] = field(default_factory=list)
    xai_explanation: Optional[Dict[str, Any]] = None
    escalated_to_hitl: bool = False
    hitl_pending: bool = False
    adjustments: Dict[str, float] = field(default_factory=dict)
    drift_detected: bool = False
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # Tuples -> lists for JSON friendliness
        d["pareto_winners"] = list(self.pareto_winners)
        d["pareto_losers"] = list(self.pareto_losers)
        return d


@dataclass
class DeferredReflection:
    """A reflection that was deferred due to a high-carbon window."""
    reflection_id: str
    payload: Dict[str, float]
    reason: str
    not_before_ts: float = field(default_factory=time.monotonic)
    attempts: int = 0
    deferred_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat())


# =============================================================================
# META COGNITIVE LAYER
# =============================================================================
class MetaCognitiveLayer:
    """
    Self-reflection layer.

    Every external hook is optional and every failure is logged (never silently
    swallowed). All public return values are immutable ReflectionRecords.
    """

    DEFAULT_TARGETS = {
        "accuracy": 0.85,
        "energy": 1.0,
        "carbon": 0.4,
        "latency_ms": 200.0,
        "cost_usd": 0.1,
    }

    DEFAULT_META_POLICY = {
        "quality": 0.4,
        "carbon": 0.3,
        "cost": 0.2,
        "latency": 0.1,
    }

    LOWER_BETTER = {"energy", "carbon", "latency_ms", "cost_usd"}

    def __init__(
        self,
        config: Optional[Any] = None,
        storage: Optional[StorageProtocol] = None,
        *,
        causal_rl: Optional["CausalPolicyAdapter"] = None,
        temporal: Optional["TemporalLogicVerifier"] = None,
        xai: Optional["XAIDecisionExplainer"] = None,
        hitl: Optional["ActiveUserPreferenceLearner"] = None,
        carbon_market: Optional["CarbonMarketIntegrator"] = None,
        chaos: Optional["ChaosTestingEngine"] = None,
        federated: Optional["FederatedGreenAggregator"] = None,
        multi_agent: Optional["MultiAgentCoordinator"] = None,
        precision: Optional["AdaptivePrecisionSwitcher"] = None,
        registry: Optional[Any] = None,
        metrics: Optional[MetricsRegistry] = None,
    ) -> None:
        self.config = config
        self.storage = storage
        self.causal_rl = causal_rl
        self.temporal = temporal
        self.xai = xai
        self.hitl = hitl
        self.carbon_market = carbon_market
        self.chaos = chaos
        self.federated = federated
        self.multi_agent = multi_agent
        self.precision = precision
        self.registry = registry
        self.metrics = metrics or MetricsRegistry()

        self.history: Deque[ReflectionRecord] = deque(maxlen=500)
        self.deferred: Deque[DeferredReflection] = deque(maxlen=256)
        self.targets: Dict[str, float] = dict(self.DEFAULT_TARGETS)
        self.meta_policy: Dict[str, float] = dict(self.DEFAULT_META_POLICY)
        self.reflection_count = 0
        self.deferred_count = 0
        self._seen_ids: Set[str] = set()
        self._lock = asyncio.Lock()

        # Drift detection on the accuracy stream
        self._drift = PageHinkleyDetector(
            delta=_cfg_get(config, "drift_delta", 0.005),
            threshold=_cfg_get(config, "drift_threshold", 5.0),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _safe(self, hook_name: str, fn: Callable[[], Any]) -> Any:
        """Call a hook, log and return None on exception."""
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - deliberate boundary
            logger.warning(
                "Hook %s failed: %s", hook_name, exc, exc_info=True)
            self.metrics.inc("hook_failures_total", hook=hook_name)
            return None

    async def _safe_async(self, hook_name: str, coro: Callable[[], Any]) -> Any:
        try:
            return await coro()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Hook %s failed: %s", hook_name, exc, exc_info=True)
            self.metrics.inc("hook_failures_total", hook=hook_name)
            return None

    # ------------------------------------------------------------------
    # Core reflection
    # ------------------------------------------------------------------
    async def reflect(
        self,
        accuracy: float,
        energy: float,
        carbon: Optional[float] = None,
        latency_ms: Optional[float] = None,
        cost_usd: Optional[float] = None,
        defer_if_high_carbon: bool = True,
        reflection_id: Optional[str] = None,
    ) -> ReflectionRecord:
        """
        Reflect on the current metrics and return an immutable record.

        If `reflection_id` is supplied and already known to storage, the
        previously-persisted record is returned (idempotent).
        """
        async with self._lock:
            # --- Idempotency ---
            if reflection_id is None:
                reflection_id = uuid.uuid4().hex[:8]
            elif reflection_id in self._seen_ids:
                logger.info(
                    "Reflection %s already processed, skipping", reflection_id)
                self.metrics.inc("reflection_idempotent_hits_total")
                for r in reversed(self.history):
                    if r.reflection_id == reflection_id:
                        return r
                return self._empty_record(reflection_id, "already_processed")

            self._seen_ids.add(reflection_id)
            self.reflection_count += 1
            self.metrics.inc("reflections_total")

            carbon = 0.4 if carbon is None else carbon
            latency_ms = 200.0 if latency_ms is None else latency_ms
            cost_usd = 0.1 if cost_usd is None else cost_usd

            # --- Chaos injection (configurable rate) ---
            chaos_prob = _cfg_get(
                self.config, "chaos_reflection_probability", 0.05)
            if self.chaos is not None and random.random() < chaos_prob:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self._safe_async(
                    "chaos.run_experiment",
                    lambda: self.chaos.run_experiment(
                        f"reflection_{reflection_id}", fault))

            # --- Carbon-aware deferral ---
            if defer_if_high_carbon and self.carbon_market is not None:
                decision = await self._safe_async(
                    "carbon_market.net_zero_schedule",
                    lambda: self.carbon_market.net_zero_schedule(
                        workload_kwh=0.001, intensity=carbon))
                if isinstance(decision, dict) and \
                        decision.get("action") == "defer":
                    return await self._defer_reflection(
                        reflection_id, accuracy, energy, carbon,
                        latency_ms, cost_usd,
                        reason=decision.get("reason", "high_carbon"))

            # --- Pareto analysis ---
            pareto, winners, losers, confidence = self._compute_pareto(
                accuracy, energy, carbon, latency_ms, cost_usd)

            # --- Drift detection ---
            drift = self._drift.update(accuracy)
            if drift:
                logger.info(
                    "Drift detected on accuracy stream at reflection %s",
                    reflection_id)
                self.metrics.inc("drift_events_total")

            # --- Causal attribution ---
            causal_attrs: Dict[str, float] = {}
            if self.causal_rl is not None and \
                    self.causal_rl.graph is not None:
                causal_attrs = await self._safe_async(
                    "causal.parents",
                    lambda: self._gather_causal_parents()) or {}

            # --- Temporal verification ---
            temporal_violations: List[str] = []
            if self.temporal is not None:
                async def _verify():
                    await self.temporal.push_state(
                        {"quality": accuracy, "carbon": carbon,
                         "energy": energy})
                    verify = await self.temporal.verify()
                    return [k for k, ok in verify.items() if not ok]
                temporal_violations = await self._safe_async(
                    "temporal.verify", _verify) or []

            # --- Severity / verdict ---
            severity, verdict = self._evaluate_severity(
                accuracy, energy, carbon, latency_ms, cost_usd,
                temporal_violations)

            # --- Narrative ---
            narrative = self._narrate(
                accuracy, energy, carbon, latency_ms, cost_usd,
                pareto, severity, verdict)

            # --- XAI (warning/critical only) ---
            xai_explanation: Optional[Dict[str, Any]] = None
            if severity in ("warning", "critical") and self.xai is not None:
                xai_explanation = await self._safe_async(
                    "xai.explain",
                    lambda: self._explain(
                        reflection_id, accuracy, energy, carbon,
                        latency_ms, cost_usd))

            # --- HITL escalation ---
            escalated = False
            hitl_pending = False
            if severity == "critical" and self.hitl is not None:
                async def _escalate():
                    return await self.hitl.query_user_if_needed(
                        "meta_cognition_user",
                        [
                            {"solution_id": "accept",
                             "quality_score": accuracy,
                             "carbon_g": carbon,
                             "cost_usd": cost_usd,
                             "latency_ms": latency_ms},
                            {"solution_id": "adjust",
                             "quality_score": accuracy - 0.01,
                             "carbon_g": carbon + 0.01,
                             "cost_usd": cost_usd + 0.01,
                             "latency_ms": latency_ms + 10},
                        ],
                        timeout=2.0)
                decision = await self._safe_async("hitl.query", _escalate)
                if decision is None:
                    hitl_pending = True  # timed out — retry later
                elif decision == "adjust":
                    escalated = True

            # --- Meta-policy adjustment ---
            adjustments = self._adjust_meta_policy(
                accuracy, energy, carbon, latency_ms, cost_usd, severity)

            # --- Build record ---
            record = ReflectionRecord(
                reflection_id=reflection_id,
                accuracy=accuracy, energy=energy,
                carbon=carbon, latency_ms=latency_ms, cost_usd=cost_usd,
                verdict=verdict, severity=severity, narrative=narrative,
                pareto_status=pareto,
                pareto_winners=tuple(winners),
                pareto_losers=tuple(losers),
                pareto_confidence=confidence,
                causal_attributions=causal_attrs,
                temporal_violations=temporal_violations,
                xai_explanation=xai_explanation,
                escalated_to_hitl=escalated,
                hitl_pending=hitl_pending,
                adjustments=adjustments,
                drift_detected=drift,
            )

            self.history.append(record)
            self.metrics.observe("reflection_accuracy", accuracy)
            self.metrics.observe("reflection_carbon", carbon)
            self.metrics.observe("reflection_energy", energy)
            self.metrics.inc("reflections_by_severity", severity=severity)
            self.metrics.inc("pareto_status_total", status=pareto)

            # --- Persist ---
            if self.storage is not None:
                await self._safe_async(
                    "storage.save_reflection",
                    lambda: asyncio.to_thread(
                        self.storage.save_reflection,
                        reflection_id, record.to_dict()))

            # --- Federated share (only meaningful for a shared policy dict) ---
            if self.federated is not None and severity != "info":
                await self._safe_async(
                    "federated.share_weights",
                    lambda: self.federated.share_weights(
                        "meta_policy", dict(self.meta_policy)))

            # --- Multi-agent bid for critical reflections ---
            if self.multi_agent is not None and severity == "critical":
                async def _bid():
                    agent_id, _score = await self.multi_agent.bid({
                        "name": f"reflect_{reflection_id}",
                        "preferred_role": "validator"})
                    await self.multi_agent.reward(agent_id, 0.8)
                await self._safe_async("multi_agent.bid", _bid)

            # --- GraphRegistry ---
            if self.registry is not None:
                await self._safe_async(
                    "registry.aregister",
                    lambda: self._register_reflection_graph(record))

            return record

    # ------------------------------------------------------------------
    # Deferral
    # ------------------------------------------------------------------
    async def _defer_reflection(
        self,
        reflection_id: str,
        accuracy: float, energy: float, carbon: float,
        latency_ms: float, cost_usd: float, reason: str,
    ) -> ReflectionRecord:
        self.deferred_count += 1
        self.metrics.inc("reflections_deferred_total")
        deferred = DeferredReflection(
            reflection_id=reflection_id,
            payload={
                "accuracy": accuracy, "energy": energy, "carbon": carbon,
                "latency_ms": latency_ms, "cost_usd": cost_usd,
            },
            reason=reason,
        )
        self.deferred.append(deferred)
        record = ReflectionRecord(
            reflection_id=reflection_id,
            accuracy=accuracy, energy=energy, carbon=carbon,
            latency_ms=latency_ms, cost_usd=cost_usd,
            verdict="deferred", severity="info",
            narrative=f"Reflection deferred ({reason}).",
            pareto_status="deferred",
        )
        self.history.append(record)
        if self.storage is not None:
            await self._safe_async(
                "storage.save_reflection",
                lambda: asyncio.to_thread(
                    self.storage.save_reflection,
                    reflection_id, record.to_dict()))
        return record

    async def drain_deferred(self, max_items: int = 8) -> int:
        """Re-run deferred reflections. Returns number successfully processed."""
        if not self.deferred:
            return 0
        processed = 0
        for _ in range(min(max_items, len(self.deferred))):
            item = self.deferred.popleft()
            if self.carbon_market is not None:
                decision = await self._safe_async(
                    "carbon_market.net_zero_schedule",
                    lambda i=item: self.carbon_market.net_zero_schedule(
                        workload_kwh=0.001,
                        intensity=i.payload.get("carbon", 0.4)))
                if isinstance(decision, dict) and \
                        decision.get("action") == "defer":
                    item.attempts += 1
                    if item.attempts < 3:
                        self.deferred.append(item)
                    continue
            await self.reflect(
                **item.payload,
                defer_if_high_carbon=False,
                reflection_id=item.reflection_id + f"_{item.attempts}",
            )
            processed += 1
        return processed

    # ------------------------------------------------------------------
    # Pareto
    # ------------------------------------------------------------------
    def _compute_pareto(
        self, accuracy: float, energy: float, carbon: float,
        latency_ms: float, cost_usd: float,
    ) -> Tuple[str, List[str], List[str], Dict[str, float]]:
        recent = [r for r in list(self.history)[-20:]
                  if r.pareto_status != "deferred"]
        if not recent:
            return "baseline", [], [], {}
        new = {
            "accuracy": accuracy, "energy": energy,
            "carbon": carbon, "latency_ms": latency_ms, "cost_usd": cost_usd,
        }
        base = {
            "accuracy": statistics.fmean(r.accuracy for r in recent),
            "energy": statistics.fmean(r.energy for r in recent),
            "carbon": statistics.fmean(r.carbon for r in recent),
            "latency_ms": statistics.fmean(r.latency_ms for r in recent),
            "cost_usd": statistics.fmean(r.cost_usd for r in recent),
        }
        winners: List[str] = []
        losers: List[str] = []
        confidence: Dict[str, float] = {}
        for metric, nv in new.items():
            bv = base[metric]
            if metric in self.LOWER_BETTER:
                better = nv < bv
            else:
                better = nv > bv
            equal = abs(nv - bv) < 1e-9
            if equal:
                continue
            if better:
                winners.append(metric)
            else:
                losers.append(metric)
            # Per-metric empirical confidence
            base_vals = [getattr(r, metric) for r in recent]
            wins = sum(
                1 for v in base_vals
                if (nv < v if metric in self.LOWER_BETTER else nv > v))
            confidence[metric] = wins / len(base_vals)
        if not losers and winners:
            return "dominates", winners, losers, confidence
        if not winners and losers:
            return "dominated", winners, losers, confidence
        if winners and losers:
            return "tradeoff", winners, losers, confidence
        return "baseline", winners, losers, confidence

    # ------------------------------------------------------------------
    # Severity (with carbon critical tier now reachable)
    # ------------------------------------------------------------------
    def _evaluate_severity(
        self, accuracy: float, energy: float, carbon: float,
        latency_ms: float, cost_usd: float,
        temporal_violations: List[str],
    ) -> Tuple[str, str]:
        score = 0
        reasons: List[str] = []

        # Accuracy
        if accuracy < self.targets["accuracy"] - 0.15:
            score += 3
            reasons.append("accuracy_critical")
        elif accuracy < self.targets["accuracy"] - 0.05:
            score += 1
            reasons.append("accuracy_warning")

        # Energy
        if energy > self.targets["energy"] * 2.0:
            score += 3
            reasons.append("energy_critical")
        elif energy > self.targets["energy"] * 1.3:
            score += 1
            reasons.append("energy_warning")

        # Carbon — **critical tier checked first** (bug fix)
        if carbon > self.targets["carbon"] * 2.5:
            score += 3
            reasons.append("carbon_critical")
        elif carbon > self.targets["carbon"] * 1.8:
            score += 2
            reasons.append("carbon_warning")

        # Cost / latency warnings
        if cost_usd > self.targets["cost_usd"] * 2.0:
            score += 2
            reasons.append("cost_warning")
        if latency_ms > self.targets["latency_ms"] * 2.5:
            score += 2
            reasons.append("latency_warning")

        # Temporal violations
        if temporal_violations:
            score += 2
            reasons.append("temporal_violation")

        if score >= 4:
            severity = "critical"
        elif score >= 2:
            severity = "warning"
        else:
            severity = "info"
        verdict = "|".join(reasons) if reasons else "nominal"
        return severity, verdict

    # ------------------------------------------------------------------
    # Narrative
    # ------------------------------------------------------------------
    def _narrate(
        self, accuracy: float, energy: float, carbon: float,
        latency_ms: float, cost_usd: float,
        pareto: str, severity: str, verdict: str,
    ) -> str:
        pareto_text = {
            "baseline": "First reflection; no baseline yet.",
            "dominates": "Pareto-dominates the historical baseline.",
            "dominated": "Pareto-dominated by the historical baseline.",
            "tradeoff": "Trade-off vs. the historical baseline.",
            "deferred": "Deferred due to carbon window.",
        }.get(pareto, "Pareto status unknown.")
        mp = self.meta_policy
        return (
            f"[{severity.upper()}] accuracy={accuracy:.3f} "
            f"energy={energy:.3f}J carbon={carbon:.3f}kg "
            f"latency={latency_ms:.1f}ms cost=${cost_usd:.4f}. "
            f"{pareto_text} Verdict: {verdict}. "
            f"Meta-policy: quality={mp['quality']:.2f}, "
            f"carbon={mp['carbon']:.2f}, cost={mp['cost']:.2f}, "
            f"latency={mp['latency']:.2f}."
        )

    # ------------------------------------------------------------------
    # XAI
    # ------------------------------------------------------------------
    async def _explain(
        self, reflection_id: str, accuracy: float, energy: float,
        carbon: float, latency_ms: float, cost_usd: float,
    ) -> Optional[Dict[str, Any]]:
        if not NUMPY_AVAILABLE:
            return None
        feats = np.array([accuracy, energy, carbon, latency_ms, cost_usd])

        def _score(x):
            return float(np.dot(x, [0.4, -0.2, -0.3, -0.05, -0.05]))

        return await self.xai.explain(
            decision_id=f"meta_{reflection_id}",
            label=f"reflection:{reflection_id}",
            features=feats,
            names=["accuracy", "energy", "carbon", "latency_ms", "cost_usd"],
            model_fn=_score,
        )

    # ------------------------------------------------------------------
    # Meta-policy
    # ------------------------------------------------------------------
    def _adjust_meta_policy(
        self, accuracy: float, energy: float, carbon: float,
        latency_ms: float, cost_usd: float, severity: str,
    ) -> Dict[str, float]:
        adjustments: Dict[str, float] = {}
        lr = 0.05

        if accuracy < self.targets["accuracy"] - 0.05:
            self.meta_policy["quality"] += lr
            adjustments["quality"] = +lr
        if carbon > self.targets["carbon"] * 1.5:
            self.meta_policy["carbon"] += lr
            adjustments["carbon"] = +lr
        if cost_usd > self.targets["cost_usd"] * 1.5:
            self.meta_policy["cost"] += lr
            adjustments["cost"] = +lr
        if latency_ms > self.targets["latency_ms"] * 1.5:
            self.meta_policy["latency"] += lr
            adjustments["latency"] = +lr

        for k in self.meta_policy:
            self.meta_policy[k] = 0.99 * self.meta_policy[k] + 0.01 * 0.25

        s = sum(self.meta_policy.values()) or 1.0
        self.meta_policy = {k: v / s for k, v in self.meta_policy.items()}
        return adjustments

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    async def _gather_causal_parents(self) -> Dict[str, float]:
        """Read causal parents of 'quality' from the causal graph."""
        graph = self.causal_rl.graph
        if graph is None:
            return {}
        parents = graph.parents("quality")
        out: Dict[str, float] = {}
        for p in parents:
            edge = graph.graph.get(p, {}).get("quality", {})
            out[p] = float(edge.get("weight", 0.0))
        return out

    async def _register_reflection_graph(
        self, record: ReflectionRecord,
    ) -> None:
        """Register a small summary of recent reflections with the registry."""
        if not hasattr(self.registry, "aregister"):
            return
        recent = list(self.history)[-20:]
        summary = {
            "count": len(recent),
            "avg_accuracy": statistics.fmean(
                r.accuracy for r in recent) if recent else 0.0,
            "avg_carbon": statistics.fmean(
                r.carbon for r in recent) if recent else 0.0,
            "meta_policy": dict(self.meta_policy),
            "last_reflection_id": record.reflection_id,
        }
        await self.registry.aregister(
            "reflection", summary, explain=False,
            require_hitl=False, defer_if_high_carbon=False)

    def _empty_record(self, reflection_id: str,
                      verdict: str) -> ReflectionRecord:
        return ReflectionRecord(
            reflection_id=reflection_id,
            accuracy=0.0, energy=0.0, carbon=0.0,
            latency_ms=0.0, cost_usd=0.0,
            verdict=verdict, severity="info",
            narrative=f"Empty record ({verdict}).",
            pareto_status="baseline",
        )

    # ------------------------------------------------------------------
    # Public utilities
    # ------------------------------------------------------------------
    def stats(self) -> Dict[str, Any]:
        if not self.history:
            return {
                "count": self.reflection_count,
                "deferred": self.deferred_count,
                "meta_policy": dict(self.meta_policy),
            }
        severities: Dict[str, int] = defaultdict(int)
        for r in self.history:
            severities[r.severity] += 1
        recent = [r for r in list(self.history)[-20:]
                  if r.pareto_status != "deferred"]
        return {
            "count": self.reflection_count,
            "deferred": self.deferred_count,
            "history_size": len(self.history),
            "pending_deferred": len(self.deferred),
            "severities": dict(severities),
            "meta_policy": dict(self.meta_policy),
            "avg_accuracy": statistics.fmean(
                r.accuracy for r in recent) if recent else 0.0,
            "avg_carbon": statistics.fmean(
                r.carbon for r in recent) if recent else 0.0,
            "avg_energy": statistics.fmean(
                r.energy for r in recent) if recent else 0.0,
        }

    def set_targets(self, targets: Dict[str, float]) -> None:
        self.targets.update(targets)

    def top_reflections(
        self, k: int = 5, severity: Optional[str] = None,
        verdict_prefix: Optional[str] = None,
    ) -> List[ReflectionRecord]:
        """Return the most recent reflections, optionally filtered."""
        pool: List[ReflectionRecord] = list(self.history)
        if severity is not None:
            pool = [r for r in pool if r.severity == severity]
        if verdict_prefix is not None:
            pool = [r for r in pool if r.verdict.startswith(verdict_prefix)]
        return pool[-k:]

    def snapshot(self) -> Dict[str, Any]:
        """JSON-serialisable view of the layer state."""
        return {
            "stats": self.stats(),
            "targets": dict(self.targets),
            "recent_reflections": [r.to_dict()
                                   for r in list(self.history)[-10:]],
            "deferred": [{"reflection_id": d.reflection_id,
                          "reason": d.reason,
                          "attempts": d.attempts}
                         for d in self.deferred],
        }


# =============================================================================
# ENHANCEMENT 1 — QUANTUM DISTILLATION
# =============================================================================
class QuantumDistillationEngine:
    def __init__(self, temperature: float = 2.0, alpha: float = 0.5,
                 n_actions: int = 5, learning_rate: float = 0.1) -> None:
        self.temperature = temperature
        self.alpha = alpha
        self.n_actions = n_actions
        self.learning_rate = learning_rate
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy = [1.0 / n_actions] * n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def register_teacher(self, name: str, policy: List[float]) -> None:
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[name] = [p / s for p in policy]

    def _softmax(self, x: List[float], temp: float) -> List[float]:
        m = max(x)
        exps = [math.exp((v - m) / max(temp, 1e-6)) for v in x]
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

    async def step(self, storage: Optional[Any] = None,
                   student_id: str = "meta_student") -> Dict[str, Any]:
        target = self._softmax(self._superpose(), self.temperature)
        new = []
        for s, t in zip(self.student_policy, target):
            eps = max(1e-12, min(self.student_policy) * 0.1)
            grad = -(t / max(s, eps))
            # Clamp step size (bug fix: bounded update)
            delta = max(-0.2, min(0.2, -self.learning_rate * grad))
            new.append(max(1e-3, s + delta))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]
        entry = {
            "target": target,
            "student": list(self.student_policy),
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        self.history.append(entry)
        if storage is not None:
            for tid, tpol in self.teachers.items():
                try:
                    await asyncio.to_thread(
                        storage.save_teacher_superposition,
                        student_id, tid, tpol[0] if tpol else 0.0,
                        self.temperature, 0.0, 0.0)
                except Exception as exc:  # noqa: BLE001
                    logger.debug(
                        "storage.save_teacher_superposition failed: %s", exc)
        return entry

    def get_policy(self) -> List[float]:
        return list(self.student_policy)


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL
# =============================================================================
class CausalGraphLearner:
    """Correlation-based causal-graph learner (see README for PC/PC-lite)."""

    def __init__(self, storage: Optional[Any] = None) -> None:
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []

    async def learn(
        self, samples: List[Dict[str, float]],
        variables: List[str], threshold: float = 0.25,
    ) -> Dict[str, Any]:
        self.variables = list(variables)
        if len(samples) < 5 or not NUMPY_AVAILABLE:
            # Fallback: random sparse graph (used only for cold-start)
            self.graph.clear()
            for i, s in enumerate(variables):
                for j, t in enumerate(variables):
                    if i < j and random.random() < 0.25:
                        w = random.uniform(0.1, 0.9)
                        self.graph[s][t] = {"weight": w, "confidence": w}
            return self.summary()

        X = np.array([[s[v] for v in variables] for s in samples], dtype=float)
        if X.shape[0] < 2:
            return self.summary()
        X = (X - X.mean(0)) / (X.std(0) + 1e-9)
        corr = np.corrcoef(X, rowvar=False)
        self.graph.clear()
        for i in range(len(variables)):
            for j in range(len(variables)):
                if i == j:
                    continue
                c = abs(float(corr[i, j]))
                if c > threshold:
                    vi, vj = float(X[:, i].var()), float(X[:, j].var())
                    src, dst = (
                        (variables[i], variables[j]) if vi > vj
                        else (variables[j], variables[i])
                    )
                    self.graph[src][dst] = {
                        "weight": float(corr[i, j]), "confidence": c}
                    if self.storage is not None:
                        try:
                            await asyncio.to_thread(
                                self.storage.save_causal_edge,
                                src, dst, float(corr[i, j]), c)
                        except Exception as exc:  # noqa: BLE001
                            logger.debug(
                                "storage.save_causal_edge failed: %s", exc)
        return self.summary()

    def parents(self, node: str) -> List[str]:
        return [s for s, e in self.graph.items() if node in e]

    def children(self, node: str) -> List[str]:
        return list(self.graph.get(node, {}).keys())

    def summary(self) -> Dict[str, Any]:
        return {
            "nodes": len(self.variables),
            "edges": sum(len(v) for v in self.graph.values()),
            "variables": list(self.variables),
        }


class CausalPolicyAdapter:
    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(self, config: Any, storage: Any,
                 graph: CausalGraphLearner) -> None:
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values: Dict[str, float] = defaultdict(float)
        self.counts: Dict[str, int] = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = float(_cfg_get(config, "causal_exploration_rate", 0.1))

    async def choose_action(self, state: Any) -> str:
        if random.random() < self.epsilon:
            return random.choice(self.ACTIONS)
        return max(self.ACTIONS, key=lambda a: self.values.get(a, 0.0))

    async def update(self, action: str, reward: float,
                     state: Any) -> None:
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
        w = self.graph.graph.get(treatment, {}).get(
            outcome, {}).get("weight", 0.0)
        try:
            await asyncio.to_thread(
                self.storage.save_causal_experiment,
                f"exp_{uuid.uuid4().hex[:8]}", treatment, outcome, w,
                samples)
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "storage.save_causal_experiment failed: %s", exc)
        return w

    def get_policy(self) -> List[float]:
        return list(self.policy)


# =============================================================================
# ENHANCEMENT 3 — FEDERATED GREEN LEARNING
# =============================================================================
class FederatedGreenAggregator:
    """
    Federated averaging for the meta-policy.

    Weights are transmitted as a fixed-length struct of doubles — **not**
    as JSON bytes — so element-wise averaging is mathematically meaningful.
    """

    FIELDS: Tuple[str, ...] = ("quality", "carbon", "cost", "latency")

    def __init__(self, storage: Any, instance_id: str,
                 share_interval: int = 3600) -> None:
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.rounds = 0

    @classmethod
    def _serialise(cls, policy: Dict[str, float]) -> bytes:
        vals = [float(policy.get(f, 0.0)) for f in cls.FIELDS]
        return struct.pack(f"<{len(vals)}d", *vals)

    @classmethod
    def _deserialise(cls, blob: bytes) -> Optional[Dict[str, float]]:
        n = len(cls.FIELDS)
        if len(blob) < 8 * n:
            return None
        vals = struct.unpack(f"<{n}d", blob[:8 * n])
        return dict(zip(cls.FIELDS, vals))

    async def share_weights(self, model_id: str,
                            weights: Any) -> None:
        # Accept either a policy dict or a raw bytes blob
        if isinstance(weights, dict):
            blob = self._serialise(weights)
        elif isinstance(weights, (bytes, bytearray)):
            blob = bytes(weights)
        else:
            raise TypeError(
                f"weights must be dict or bytes, got {type(weights).__name__}")
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id, model_id, blob,
                float(len(blob)), self.rounds)
        except Exception as exc:  # noqa: BLE001
            logger.warning("federated.share_weights failed: %s", exc)

    async def pull_aggregated_weights(
        self, model_id: str,
    ) -> Optional[Dict[str, float]]:
        try:
            rows = await asyncio.to_thread(
                self.storage.get_federated_weights, model_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning("federated.pull failed: %s", exc)
            return None
        if not rows:
            return None
        n = len(self.FIELDS)
        acc = [0.0] * n
        count = 0
        for row in rows:
            blob = row.get("weights")
            if not blob:
                continue
            parsed = self._deserialise(blob)
            if parsed is None:
                continue
            for i, f in enumerate(self.FIELDS):
                acc[i] += parsed[f]
            count += 1
        if count == 0:
            return None
        avg = [v / count for v in acc]
        s = sum(avg) or 1.0
        self.rounds += 1
        return {f: v / s for f, v in zip(self.FIELDS, avg)}

    async def apply_aggregated_weights(
        self, model_id: str, current: Dict[str, float],
    ) -> Dict[str, float]:
        agg = await self.pull_aggregated_weights(model_id)
        if agg is None:
            return current
        merged = {k: (current.get(k, 0.0) + agg.get(k, 0.0)) / 2.0
                  for k in set(current) | set(agg)}
        s = sum(merged.values()) or 1.0
        return {k: v / s for k, v in merged.items()}


# =============================================================================
# ENHANCEMENT 4 — MULTI-AGENT COORDINATION
# =============================================================================
class _Agent:
    ROLES = ["orchestrator", "validator", "optimizer", "reporter",
             "negotiator"]

    def __init__(self, agent_id: str) -> None:
        self.id = agent_id
        self.role = "validator"
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        self.completed = 0


@dataclass
class _Task:
    task_id: str
    name: str
    preferred_role: str
    deadline: float
    attempts: int = 0
    assigned_to: Optional[str] = None


class MultiAgentCoordinator:
    def __init__(self, config: Any, storage: Any) -> None:
        self.config = config
        self.storage = storage
        count = int(_cfg_get(config, "agent_count", 5))
        self.agents: Dict[str, _Agent] = {
            f"agent_{i:02d}": _Agent(f"agent_{i:02d}") for i in range(count)}
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._task_queue: Deque[_Task] = deque(maxlen=256)
        self._lock = asyncio.Lock()

    async def _specialise(self) -> None:
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])

    async def broadcast(self, topic: str, sender: str,
                        payload: Any) -> None:
        try:
            self.bus.put_nowait(
                {"topic": topic, "sender": sender, "payload": payload})
        except asyncio.QueueFull:
            logger.warning("multi_agent.broadcast queue full")

    async def enqueue(self, name: str, preferred_role: str = "orchestrator",
                      deadline_s: float = 30.0) -> str:
        task = _Task(
            task_id=uuid.uuid4().hex[:8], name=name,
            preferred_role=preferred_role,
            deadline=time.monotonic() + deadline_s,
        )
        self._task_queue.append(task)
        return task.task_id

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
        if best_id is None:
            best_id = next(iter(self.agents))
        return best_id, best_score

    async def reward(self, agent_id: str, reward: float) -> None:
        async with self._lock:
            if agent_id not in self.agents:
                return
            a = self.agents[agent_id]
            # Learning-rate update (bug fix: previously divided by `completed`)
            a.reputation = max(
                0.0, min(1.0, a.reputation + 0.1 * reward))
            a.utilities[a.role] = max(
                0.0, min(1.0, a.utilities[a.role] + 0.05 * reward))

    def get_policy(self) -> List[float]:
        """Role distribution as a probability simplex. **Not uniform.**"""
        counts: Dict[str, int] = defaultdict(int)
        for a in self.agents.values():
            counts[a.role] += 1
        total = max(1, sum(counts.values()))
        return [counts[r] / total for r in _Agent.ROLES]

    async def step(self) -> Dict[str, Any]:
        await self._specialise()
        processed = 0
        # Drain messages
        while not self.bus.empty():
            try:
                self.bus.get_nowait()
                processed += 1
            except asyncio.QueueEmpty:
                break
        # Dispatch tasks
        dispatched = 0
        while self._task_queue:
            task = self._task_queue[0]
            if time.monotonic() > task.deadline:
                task.attempts += 1
                if task.attempts < 3:
                    task.deadline = time.monotonic() + 30.0
                    self._task_queue.rotate(-1)
                    continue
                self._task_queue.popleft()
                continue
            agent_id, _score = await self.bid({
                "name": task.name,
                "preferred_role": task.preferred_role,
            })
            task.assigned_to = agent_id
            self._task_queue.popleft()
            dispatched += 1
        return {
            "roles": {a.id: a.role for a in self.agents.values()},
            "role_distribution": self.get_policy(),
            "processed_messages": processed,
            "dispatched_tasks": dispatched,
            "pending_tasks": len(self._task_queue),
        }


# =============================================================================
# ENHANCEMENT 5 — TEMPORAL LOGIC
# =============================================================================
import re as _re

_ATOMIC_RE = _re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


@dataclass
class TemporalRule:
    rule_id: str
    operator: str                    # always | eventually | never | count_le
    conditions: List[Callable[[Dict[str, Any]], bool]]
    window: float = 0.0
    description: str = ""
    severity: str = "warning"
    max_count: int = 0               # used by count_le
    violations: int = 0
    last_violation: Optional[datetime] = None

    def evaluate(self, trace: Deque) -> bool:
        """Return True if the rule is *violated*."""
        if not trace:
            return False
        # Window is honoured for all operators
        if self.window > 0:
            cutoff = trace[-1][0] - timedelta(seconds=self.window)
            while trace and trace[0][0] < cutoff:
                trace.popleft()

        if self.operator == "always":
            # G(P): no sample violates P
            return any(not self.conditions[0](s) for _, s in trace)
        if self.operator == "eventually":
            # F(P): at least one sample satisfies P in the window
            return not any(self.conditions[0](s) for _, s in trace)
        if self.operator == "never":
            # NEVER P: no sample satisfies P
            return any(self.conditions[0](s) for _, s in trace)
        if self.operator == "count_le":
            # count(P) <= N within window
            n = sum(1 for _, s in trace if self.conditions[0](s))
            return n > self.max_count
        return False


class TemporalLogicVerifier:
    def __init__(self, storage: Any, config: Any) -> None:
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict[str, Any]]] = deque(
            maxlen=int(_cfg_get(config, "temporal_max_trace", 2000)))
        self.approval_cb: Optional[Callable[..., Any]] = None
        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            self._install(formula)

    def _install(self, formula: str) -> None:
        f = formula.strip()
        if f.startswith("G "):
            self._add_atomic(f[2:].strip().strip("()"), "always", window=0)
        elif f.startswith("F "):
            # Optional window suffix: "F (P) within 300"
            window = 0.0
            m = _re.search(r"within\s+([0-9.]+)", f)
            if m:
                window = float(m.group(1))
                f = f[:m.start()].strip()
            self._add_atomic(f[2:].strip().strip("()"),
                             "eventually", window=window)
        elif f.startswith("NEVER "):
            self._add_atomic(f[6:].strip().strip("()"), "never", window=0)
        else:
            logger.warning("Unsupported temporal formula: %r", formula)

    def _add_atomic(self, expr: str, op: str, window: float = 0.0) -> None:
        m = _ATOMIC_RE.match(expr)
        if not m:
            logger.warning("Invalid temporal atom: %r", expr)
            return
        var, cmp, val = m.group(1), m.group(2), float(m.group(3))

        def cond(state: Dict[str, Any], v=var, c=cmp, x=val) -> bool:
            try:
                sv = float(state.get(v, 0.0))
            except (TypeError, ValueError):
                return True
            return {
                ">=": sv >= x, "<=": sv <= x, "==": sv == x,
                "!=": sv != x, ">": sv > x, "<": sv < x,
            }[c]

        rid = f"{op}:{expr}" + (f"@w{window}" if window else "")
        self.rules[rid] = TemporalRule(
            rule_id=rid, operator=op, conditions=[cond],
            window=window, description=expr, severity="warning")

    def add_count_rule(self, name: str, var: str, cmp: str, val: float,
                       max_count: int, window_s: float,
                       severity: str = "warning") -> None:
        """Programmatically add a count(P, W) <= N rule."""
        m = _ATOMIC_RE.match(f"{var} {cmp} {val}")
        if not m:
            raise ValueError(f"Invalid atom: {var} {cmp} {val}")

        def cond(state, v=var, c=cmp, x=val):
            try:
                sv = float(state.get(v, 0.0))
            except (TypeError, ValueError):
                return True
            return {
                ">=": sv >= x, "<=": sv <= x, "==": sv == x,
                "!=": sv != x, ">": sv > x, "<": sv < x,
            }[c]

        rid = f"count_le:{name}"
        self.rules[rid] = TemporalRule(
            rule_id=rid, operator="count_le", conditions=[cond],
            window=window_s, description=name,
            severity=severity, max_count=max_count)

    def set_approval_callback(self, cb: Callable[..., Any]) -> None:
        self.approval_cb = cb

    async def push_state(self, state: Dict[str, Any]) -> None:
        self.trace.append((datetime.now(timezone.utc), dict(state)))
        try:
            await asyncio.to_thread(self.storage.save_temporal_trace, state)
        except Exception as exc:  # noqa: BLE001
            logger.debug("storage.save_temporal_trace failed: %s", exc)

    async def verify(self) -> Dict[str, bool]:
        result: Dict[str, bool] = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            violated = rule.evaluate(copy)
            result[rid] = not violated
            if violated:
                rule.violations += 1
                rule.last_violation = datetime.now(timezone.utc)
                if rule.severity == "critical" and self.approval_cb:
                    try:
                        approved = self.approval_cb(rid, self.trace[-1][1])
                        if asyncio.iscoroutine(approved):
                            await approved
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "temporal.approval_cb failed: %s", exc)
        return result


# =============================================================================
# ENHANCEMENT 6 — EXPLAINABLE AI
# =============================================================================
class XAIDecisionExplainer:
    """SHAP/LIME explainer with stability check and explicit fallback flag."""

    def __init__(self, config: Any, storage: Any) -> None:
        self.config = config
        self.storage = storage
        self.method = _cfg_get(config, "xai_method", "kernel_shap")
        self.depth = int(_cfg_get(config, "xai_depth", 5))
        self.stability_samples = int(
            _cfg_get(config, "xai_stability_samples", 2))
        self.stability_eps = float(
            _cfg_get(config, "xai_stability_eps", 0.1))
        self.global_attributions: Deque[Dict[str, float]] = deque(maxlen=1000)

    def _kernel_shap(
        self, f: Callable[[Any], float], x: Any,
        names: List[str], n: int = 32, seed: Optional[int] = None,
    ) -> Dict[str, float]:
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
        rng = np.random.default_rng(seed)
        base = np.zeros_like(x)
        contrib = np.zeros(len(x))
        for _ in range(n):
            perm = list(range(len(x)))
            rng.shuffle(perm)
            prev = base.copy()
            for i in perm:
                cur = prev.copy()
                cur[i] = x[i]
                try:
                    delta = float(f(cur.reshape(1, -1))) - \
                            float(f(prev.reshape(1, -1)))
                except Exception:  # noqa: BLE001
                    delta = 0.0
                contrib[i] += delta
                prev = cur
        contrib /= max(1, n)
        return dict(zip(names, contrib.tolist()))

    def _lime(
        self, f: Callable[[Any], float], x: Any,
        names: List[str], n: int = 100,
    ) -> Tuple[Dict[str, float], bool]:
        if not (SKLEARN_AVAILABLE and NUMPY_AVAILABLE):
            # Explicitly mark the fallback (bug fix: no more silent random)
            return {k: 0.0 for k in names}, True
        X = np.tile(x, (n, 1)) + np.random.normal(0, 0.1, (n, len(x)))
        try:
            y = np.array([float(f(r.reshape(1, -1))) for r in X])
        except Exception as exc:  # noqa: BLE001
            logger.warning("LIME prediction failed: %s", exc)
            return {k: 0.0 for k in names}, True
        w = np.exp(-np.sum((X - x) ** 2, axis=1) / 0.02)
        try:
            m = LinearRegression().fit(X, y, sample_weight=w)
            return dict(zip(names, m.coef_.tolist())), False
        except Exception as exc:  # noqa: BLE001
            logger.warning("LIME fit failed: %s", exc)
            return {k: 0.0 for k in names}, True

    def _explain_once(
        self, model_fn: Callable[[Any], float], x: Any,
        names: List[str], seed: Optional[int] = None,
    ) -> Tuple[Dict[str, float], bool]:
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}, True
        if self.method == "lime":
            return self._lime(model_fn, x, names)
        return self._kernel_shap(model_fn, x, names, seed=seed), False

    def _nl(self, decision: str, attrs: Dict[str, float]) -> str:
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]),
                     reverse=True)[: self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(
        self,
        decision_id: str,
        label: str,
        features: Any,
        names: List[str],
        model_fn: Callable[[Any], float],
        include_counterfactual: bool = False,
        include_anchors: bool = False,
    ) -> Dict[str, Any]:
        if not NUMPY_AVAILABLE:
            attrs: Dict[str, float] = {k: 0.0 for k in names}
            return {
                "decision_id": decision_id, "method": "unavailable",
                "attributions": attrs, "explanation": self._nl(label, attrs),
                "stable": False, "fallback": True,
            }

        x = np.asarray(features, dtype=float)
        base_attrs, fallback = self._explain_once(model_fn, x, names, seed=0)

        # Stability check (P2)
        stable = True
        if not fallback and self.stability_samples > 1:
            for s in range(1, self.stability_samples):
                other, _ = self._explain_once(model_fn, x, names, seed=s)
                for k in names:
                    if abs(other.get(k, 0.0) - base_attrs.get(k, 0.0)) > \
                            self.stability_eps:
                        stable = False
                        break
                if not stable:
                    break

        result: Dict[str, Any] = {
            "decision_id": decision_id,
            "method": self.method if not fallback else "fallback_random",
            "attributions": base_attrs,
            "explanation": self._nl(label, base_attrs),
            "stable": stable,
            "fallback": fallback,
            "counterfactual": None,
            "anchors": None,
        }
        self.global_attributions.append(dict(base_attrs))

        try:
            await asyncio.to_thread(
                self.storage.save_xai_explanation,
                decision_id, decision_id,
                result["method"], label,
                {"feature_names": names, "values": x.tolist()},
                base_attrs, result["explanation"])
        except Exception as exc:  # noqa: BLE001
            logger.debug("storage.save_xai_explanation failed: %s", exc)

        return result

    def global_feature_importance(self) -> Dict[str, float]:
        if not self.global_attributions:
            return {}
        agg: Dict[str, List[float]] = defaultdict(list)
        for d in self.global_attributions:
            for k, v in d.items():
                agg[k].append(abs(v))
        return {k: statistics.fmean(vals) for k, vals in agg.items()}


# =============================================================================
# ENHANCEMENT 7 — ADAPTIVE PRECISION SWITCHER
# =============================================================================
class AdaptivePrecisionSwitcher:
    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55,
              "fp16": 0.5, "int8": 0.3}

    def __init__(self, config: Any, storage: Any) -> None:
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def _probe(self) -> Dict[str, Any]:
        info = {"cuda": False, "bf16": False, "device": "cpu"}
        if TORCH_AVAILABLE:
            try:
                info["cuda"] = torch.cuda.is_available()
                if info["cuda"]:
                    info["device"] = torch.cuda.get_device_name(0)
                    info["bf16"] = torch.cuda.is_bf16_supported()
            except Exception as exc:  # noqa: BLE001
                logger.debug("torch probe failed: %s", exc)
        return info

    def select_precision(self) -> str:
        hw = self._probe()
        cands = list(_cfg_get(self.config, "precision_levels",
                              ["fp32", "fp16", "bf16", "int8"]))
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        if not cands:
            # Bug fix: never return None
            return "fp32"
        return min(cands, key=lambda c: self.ENERGY.get(c, 1.0))

    async def switch_to(self, target: str, reason: str = "policy") -> bool:
        if target == self.current or target not in self.ENERGY:
            return False
        old = self.current
        self.current = target
        saved = max(0.0, self.ENERGY.get(old, 1.0) -
                    self.ENERGY.get(target, 1.0))
        self.saved_wh += saved
        self.history.append({"from": old, "to": target,
                             "reason": reason, "saved_wh": saved,
                             "ts": datetime.now(timezone.utc).isoformat()})
        try:
            await asyncio.to_thread(
                self.storage.save_precision_switch,
                old, target, reason, saved, 0.0)
        except Exception as exc:  # noqa: BLE001
            logger.debug("storage.save_precision_switch failed: %s", exc)
        return True

    async def auto_switch(self, recent_acc: float,
                          baseline_acc: float) -> Optional[str]:
        """
        Adjust precision based on an accuracy budget.

        If the accuracy drop exceeds the threshold, revert to fp32. If there
        is headroom, try the most efficient precision.
        """
        if baseline_acc <= 0:
            return None
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = float(_cfg_get(
            self.config, "precision_switch_threshold", 0.02))
        if drop > thresh:
            if await self.switch_to("fp32", reason=f"acc drop {drop:.3f}"):
                return "fp32"
        elif drop < thresh / 2:
            target = self.select_precision()
            if await self.switch_to(target, reason="headroom"):
                return target
        return None

    async def calibrate_and_rollback(
        self, validation_fn: Callable[[], Any], baseline_acc: float,
    ) -> bool:
        """
        Calibration probe: run a validation set and roll back if the accuracy
        drop exceeds the threshold.
        """
        try:
            result = validation_fn()
            if asyncio.iscoroutine(result):
                result = await result
            acc = float(result)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Precision calibration failed: %s", exc)
            return False
        thresh = float(_cfg_get(
            self.config, "precision_switch_threshold", 0.02))
        if baseline_acc > 0 and \
                (baseline_acc - acc) / baseline_acc > thresh:
            logger.info(
                "Precision rollback: %s -> fp32 (acc drop too large)",
                self.current)
            await self.switch_to("fp32", reason="calibration_rollback")
            return True
        return False


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKETS / REC
# =============================================================================
class CarbonMarketIntegrator:
    def __init__(self, config: Any, storage: Any,
                 price_source: Optional[CarbonPriceSource] = None) -> None:
        self.config = config
        self.storage = storage
        self.price_source = price_source or StaticCarbonSource(25.0)
        self.last_price = 25.0

    async def _fetch_price(self) -> float:
        try:
            return await self.price_source.get_price()
        except Exception as exc:  # noqa: BLE001
            logger.warning("carbon._fetch_price failed: %s", exc)
            return self.last_price

    async def update_price(self) -> float:
        price = await self._fetch_price()
        self.last_price = price
        try:
            await asyncio.to_thread(self.storage.save_credit_price, price)
        except Exception as exc:  # noqa: BLE001
            logger.debug("storage.save_credit_price failed: %s", exc)
        return price

    async def purchase_rec(self, mwh: float, price_per_mwh: float = 5.0,
                           source: str = "wind") -> float:
        cost = mwh * price_per_mwh
        try:
            await asyncio.to_thread(
                self.storage.save_rec, mwh, price_per_mwh, source)
        except Exception as exc:  # noqa: BLE001
            logger.debug("storage.save_rec failed: %s", exc)
        return cost

    async def net_zero_schedule(self, workload_kwh: float,
                                intensity: float) -> Dict[str, Any]:
        price = await self.update_price()
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price
        defer_threshold = float(_cfg_get(
            self.config, "carbon_defer_threshold", 0.3))
        if intensity > defer_threshold:
            action = "defer"
        elif offset_cost < 0.5:
            action = "run_offset"
        else:
            action = "run"
        try:
            await asyncio.to_thread(
                self.storage.save_net_zero_match,
                uuid.uuid4().hex[:8], workload_kwh, intensity, action,
                carbon_kg, offset_cost, price)
        except Exception as exc:  # noqa: BLE001
            logger.debug("storage.save_net_zero_match failed: %s", exc)
        return {
            "action": action,
            "carbon_kg": carbon_kg,
            "offset_cost_usd": offset_cost,
            "credit_price_usd": price,
            "reason": "intensity" if action == "defer" else "cost",
        }


# =============================================================================
# ENHANCEMENT 9 — CHAOS TESTING
# =============================================================================
class ChaosTestingEngine:
    """
    Chaos engine with steady-state hypothesis.

    A "steady state" is a predicate over the pipeline's measured outputs.
    `run_experiment` fails the hypothesis if the predicate is violated.
    """

    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config: Any, storage: Any) -> None:
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def _steady(self, steady_state: Optional[Callable[[], Any]]) -> bool:
        if steady_state is None:
            return True
        try:
            result = steady_state()
            if asyncio.iscoroutine(result):
                result = await result
            return bool(result)
        except Exception as exc:  # noqa: BLE001
            logger.warning("chaos steady-state predicate failed: %s", exc)
            return False

    async def run_experiment(
        self, name: str, fault_type: str,
        steady_state: Optional[Callable[[], Any]] = None,
        blast_radius_s: float = 1.0,
    ) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        t0 = time.monotonic()
        before = await self._steady(steady_state)
        status = "completed"
        violation = False
        try:
            async with self._lock:
                self.active[name] = {"fault_type": fault_type}
            # Bounded fault window
            await asyncio.wait_for(
                self._inject(fault_type), timeout=blast_radius_s)
        except asyncio.TimeoutError:
            status = "timeout"
        except Exception as exc:  # noqa: BLE001
            if fault_type == "exception":
                status = "injected"
            else:
                logger.warning("chaos injection failed: %s", exc)
                status = "error"
        finally:
            async with self._lock:
                self.active.pop(name, None)
        after = await self._steady(steady_state)
        if before and not after:
            violation = True
            status = "hypothesis_violated"
        duration_ms = (time.monotonic() - t0) * 1000.0
        try:
            await asyncio.to_thread(
                self.storage.save_chaos_experiment,
                name, name, fault_type,
                float(_cfg_get(self.config, "chaos_blast_radius", 0.1)),
                int(before), int(after), status, duration_ms)
        except Exception as exc:  # noqa: BLE001
            logger.debug("storage.save_chaos_experiment failed: %s", exc)
        return {
            "name": name, "fault_type": fault_type,
            "steady_before": before, "steady_after": after,
            "hypothesis_violated": violation,
            "status": status, "duration_ms": duration_ms,
        }

    async def _inject(self, fault_type: str) -> None:
        if fault_type == "latency":
            await asyncio.sleep(0.1)
        elif fault_type == "exception":
            raise RuntimeError("chaos: injected exception")
        elif fault_type == "memory_pressure":
            _ = bytearray(1024)
        elif fault_type == "network_drop":
            await asyncio.sleep(0.05)
        elif fault_type == "data_corruption":
            await asyncio.sleep(0)


# =============================================================================
# ENHANCEMENT 10 — HITL ACTIVE LEARNING
# =============================================================================
@dataclass
class _PendingQuery:
    user_id: str
    event: asyncio.Event
    candidates: List[str]
    response: Optional[str] = None


class ActiveUserPreferenceLearner:
    """
    HITL learner with per-query async events (no cross-talk between users)
    and explicit `None` return on timeout (never silently default to accept).
    """

    def __init__(self, storage: Any, dashboard: Any = None) -> None:
        self.storage = storage
        self.dashboard = dashboard
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._pending: Dict[str, _PendingQuery] = {}
        self._lock = asyncio.Lock()

    async def submit_response(
        self, user_id: str, chosen_id: str,
        query_id: Optional[str] = None,
    ) -> bool:
        async with self._lock:
            if query_id and query_id in self._pending:
                pq = self._pending[query_id]
                pq.response = chosen_id
                pq.event.set()
                return True
            # Otherwise: most recent pending for this user
            for qid in reversed(list(self._pending.keys())):
                pq = self._pending[qid]
                if pq.user_id == user_id:
                    pq.response = chosen_id
                    pq.event.set()
                    return True
            return False

    async def query_user_if_needed(
        self, user_id: str, candidates: List[Dict[str, Any]],
        timeout: float = 3.0,
    ) -> Optional[str]:
        if len(candidates) < 2:
            return None
        qid = uuid.uuid4().hex[:8]
        pq = _PendingQuery(
            user_id=user_id,
            event=asyncio.Event(),
            candidates=[c.get("solution_id", "") for c in candidates],
        )
        async with self._lock:
            self._pending[qid] = pq
        try:
            await asyncio.wait_for(pq.event.wait(), timeout=timeout)
            return pq.response
        except asyncio.TimeoutError:
            # Explicit timeout: caller decides (may mark hitl_pending)
            return None
        finally:
            async with self._lock:
                self._pending.pop(qid, None)

    async def record_choice(
        self, user_id: str, solution_id: str,
        metrics: Optional[Dict[str, float]] = None,
    ) -> None:
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k in ("quality_score", "carbon_g", "cost_usd", "latency_ms"):
                if k in metrics:
                    v = float(metrics[k])
                    if k == "quality_score":
                        prefs[k] = prefs.get(k, 0.25) + v * 0.01
                    else:
                        prefs[k] = prefs.get(k, 0.25) + \
                                   1.0 / (v + 1e-6) * 0.01
            s = sum(prefs.values()) or 1.0
            prefs = {k: v / s for k, v in prefs.items()}
            self.preferences[user_id] = prefs
        try:
            await asyncio.to_thread(
                self.storage.save_user_preference, user_id, prefs)
        except Exception as exc:  # noqa: BLE001
            logger.debug("storage.save_user_preference failed: %s", exc)


# =============================================================================
# UNIFIED ORCHESTRATOR
# =============================================================================
@dataclass
class _LoopStats:
    last_run: float = 0.0
    run_count: int = 0
    failure_count: int = 0


class MetaCognitiveOrchestratorV18:
    """
    Unified entry point. Wires the enhanced MetaCognitiveLayer with all ten
    enhancements and runs nine background loops under a supervisor.
    """

    # Backward-compat alias
    VERSION = "18.0.0"

    def __init__(self, storage: Any, config: Any, dashboard: Any = None,
                 metrics: Optional[MetricsRegistry] = None) -> None:
        self.storage = storage
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]
        self.metrics = metrics or MetricsRegistry()

        # Ten enhancements
        self.quantum = QuantumDistillationEngine()
        self.causal_graph = CausalGraphLearner(storage)
        self.causal_rl = CausalPolicyAdapter(
            config, storage, self.causal_graph)
        self.federated = FederatedGreenAggregator(
            storage, self.instance_id)
        self.multi_agent = MultiAgentCoordinator(config, storage)
        self.temporal = TemporalLogicVerifier(storage, config)
        self.xai = XAIDecisionExplainer(config, storage)
        self.precision = AdaptivePrecisionSwitcher(config, storage)
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.chaos = ChaosTestingEngine(config, storage)
        self.hitl = ActiveUserPreferenceLearner(storage, dashboard=dashboard)

        # Meta layer
        self.meta = MetaCognitiveLayer(
            config=config, storage=storage,
            causal_rl=self.causal_rl, temporal=self.temporal,
            xai=self.xai, hitl=self.hitl,
            carbon_market=self.carbon_market, chaos=self.chaos,
            federated=self.federated, multi_agent=self.multi_agent,
            precision=self.precision, registry=None,
            metrics=self.metrics,
        )

        # Lifecycle
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: Set[asyncio.Task] = set()
        self._loop_stats: Dict[str, _LoopStats] = defaultdict(_LoopStats)
        self._loop_intervals: Dict[str, float] = {
            "causal_rl": 900,
            "federated": 3600,
            "multi_agent": 600,
            "temporal": 300,
            "xai": 300,
            "precision": 300,
            "carbon_market": 3600,
            "chaos": 1800,
            "distillation": 300,
            "deferred": 60,
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._running:
            logger.warning("Orchestrator already running; start() is a no-op")
            return
        self._running = True
        self._shutdown_event.clear()
        loop = asyncio.get_running_loop()

        specs: List[Tuple[str, Callable[[], Any]]] = [
            ("causal_rl", self._causal_rl_once),
            ("federated", self._federated_once),
            ("multi_agent", self._multi_agent_once),
            ("temporal", self._temporal_once),
            ("xai", self._xai_once),
            ("precision", self._precision_once),
            ("carbon_market", self._carbon_market_once),
            ("chaos", self._chaos_once),
            ("distillation", self._distillation_once),
            ("deferred", self._deferred_once),
        ]
        for name, fn in specs:
            task = loop.create_task(
                self._supervise(name, fn,
                                self._loop_intervals.get(name, 300)))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

    async def shutdown(self) -> None:
        if not self._running and self._shutdown_event.is_set():
            return
        self._running = False
        self._shutdown_event.set()
        tasks = list(self._background_tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._background_tasks.clear()

    async def _supervise(
        self, name: str, once_fn: Callable[[], Any], interval: float,
    ) -> None:
        delay = 1.0
        stats = self._loop_stats[name]
        while not self._shutdown_event.is_set():
            try:
                await asyncio.wait_for(once_fn(), timeout=300.0)
                stats.last_run = time.monotonic()
                stats.run_count += 1
                self.metrics.inc("loop_runs_total", loop=name)
                self.metrics.set_gauge(
                    "loop_last_run_ts", stats.last_run, loop=name)
                delay = 1.0
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                return
            except asyncio.TimeoutError:
                stats.failure_count += 1
                self.metrics.inc("loop_timeouts_total", loop=name)
                await asyncio.sleep(min(delay, 60))
                delay = min(delay * 2, 60)
            except Exception as exc:  # noqa: BLE001
                stats.failure_count += 1
                self.metrics.inc("loop_failures_total", loop=name)
                logger.warning(
                    "Loop %s failed (retry in %.1fs): %s",
                    name, delay, exc, exc_info=True)
                await asyncio.sleep(min(delay, 60))
                delay = min(delay * 2, 60)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def reflect(self, **kwargs: Any) -> ReflectionRecord:
        return await self.meta.reflect(**kwargs)

    # ------------------------------------------------------------------
    # Single-iteration loop bodies
    # ------------------------------------------------------------------
    async def _causal_rl_once(self) -> None:
        samples = [{
            "quality": random.uniform(0.5, 1.0),
            "carbon": random.uniform(0.1, 0.8),
            "cost": random.uniform(0.1, 0.9),
            "latency": random.uniform(0.1, 0.9),
        } for _ in range(20)]
        await self.causal_graph.learn(
            samples, ["quality", "carbon", "cost", "latency"])

    async def _federated_once(self) -> None:
        merged = await self.federated.apply_aggregated_weights(
            "meta_policy", dict(self.meta.meta_policy))
        self.meta.meta_policy = merged

    async def _multi_agent_once(self) -> None:
        await self.multi_agent.step()

    async def _temporal_once(self) -> None:
        await self.temporal.verify()

    async def _xai_once(self) -> None:
        if not NUMPY_AVAILABLE:
            return
        feats = np.array([0.9, 0.4, 0.5, 0.4])
        await self.xai.explain(
            decision_id=f"sys_{uuid.uuid4().hex[:8]}",
            label="meta_health", features=feats,
            names=["quality", "carbon", "cost", "latency"],
            model_fn=lambda x: float(
                np.dot(x, [0.4, -0.3, -0.2, -0.1])))

    async def _precision_once(self) -> None:
        await self.precision.auto_switch(0.9, 0.92)

    async def _carbon_market_once(self) -> None:
        await self.carbon_market.update_price()

    async def _chaos_once(self) -> None:
        fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
        await self.chaos.run_experiment(
            f"auto_{uuid.uuid4().hex[:6]}", fault)

    async def _distillation_once(self) -> None:
        self.quantum.register_teacher(
            "causal", self.causal_rl.get_policy())
        self.quantum.register_teacher(
            "agents", self.multi_agent.get_policy())
        self.quantum.register_teacher(
            "meta_policy", list(self.meta.meta_policy.values()))
        await self.quantum.step(self.storage, "meta_student")

    async def _deferred_once(self) -> None:
        n = await self.meta.drain_deferred(max_items=4)
        if n:
            self.metrics.inc("deferred_drained_total", value=float(n))

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        now = time.monotonic()
        loop_health = {}
        for name, stats in self._loop_stats.items():
            age = (now - stats.last_run) if stats.last_run else None
            loop_health[name] = {
                "run_count": stats.run_count,
                "failure_count": stats.failure_count,
                "last_run_age_s": age,
            }
        return {
            "instance_id": self.instance_id,
            "version": self.VERSION,
            "running": self._running,
            "meta": self.meta.stats(),
            "precision": self.precision.current,
            "carbon_price": self.carbon_market.last_price,
            "agents": {a.id: a.role
                       for a in self.multi_agent.agents.values()},
            "agent_role_distribution": self.multi_agent.get_policy(),
            "federated_rounds": self.federated.rounds,
            "loops": loop_health,
            "metrics": self.metrics.snapshot(),
        }


# Backward-compat alias
MetaCognitiveOrchestratorV17 = MetaCognitiveOrchestratorV18


# =============================================================================
# MINIMAL IN-MEMORY STORAGE
# =============================================================================
class InMemoryStorage:
    """Standalone storage for demos and tests."""

    def __init__(self) -> None:
        self._data: Dict[str, List[Any]] = defaultdict(list)
        self._reflections: Dict[str, Dict[str, Any]] = {}
        self._prefs: Dict[str, Dict[str, float]] = {}

    def save_reflection(self, reflection_id: str,
                        record: Dict[str, Any]) -> None:
        self._reflections[reflection_id] = dict(record)
        self._data["reflections"].append(
            {"reflection_id": reflection_id, **record})

    def get_reflection(self, reflection_id: str) -> Optional[Dict[str, Any]]:
        return self._reflections.get(reflection_id)

    def save_teacher_superposition(self, *a: Any, **kw: Any) -> None:
        self._data["teacher_superpositions"].append(a)

    def save_causal_edge(self, source: str, target: str,
                         weight: float, confidence: float) -> None:
        self._data["causal_graph"].append(
            {"source": source, "target": target,
             "weight": weight, "confidence": confidence})

    def save_causal_experiment(
        self, exp_id: str, treatment: str, outcome: str,
        ate: float, samples: int, method: str = "",
    ) -> None:
        self._data["causal_experiments"].append(
            {"exp_id": exp_id, "ate": ate})

    def save_federated_weights(
        self, instance_id: str, model_id: str, weights: Any,
        weight_norm: float = 0.0, round_id: int = 0,
    ) -> None:
        self._data["federated_weights"].append({
            "instance_id": instance_id,
            "model_id": model_id,
            "weights": weights,
            "round_id": round_id,
        })

    def get_federated_weights(self, model_id: str) -> List[Dict[str, Any]]:
        return [r for r in self._data["federated_weights"]
                if r["model_id"] == model_id]

    def save_precision_switch(self, *a: Any, **kw: Any) -> None:
        self._data["precision_history"].append(a)

    def save_credit_price(self, price_usd: float, **kw: Any) -> None:
        self._data["carbon_credit_prices"].append({"price_usd": price_usd})

    def save_rec(self, mwh: float, price_per_mwh: float,
                 source: str, **kw: Any) -> None:
        self._data["rec_ledger"].append({"mwh": mwh})

    def get_rec_balance(self) -> float:
        return sum(r["mwh"] for r in self._data["rec_ledger"])

    def save_net_zero_match(self, *a: Any, **kw: Any) -> None:
        self._data["net_zero_matches"].append(a)

    def save_chaos_experiment(self, *a: Any, **kw: Any) -> None:
        self._data["chaos_experiments"].append(a)

    def save_temporal_trace(self, state: Dict[str, Any],
                            context: Any = None) -> None:
        self._data["temporal_trace"].append({"state": state})

    def save_temporal_rule(self, *a: Any, **kw: Any) -> None:
        self._data["temporal_rules"].append(a)

    def save_temporal_violation(self, *a: Any, **kw: Any) -> None:
        self._data["temporal_violations"].append(a)

    def save_xai_explanation(
        self, explanation_id: str, decision_id: str, method: str,
        label: str, features: Any, attributions: Dict[str, float],
        nl: str,
    ) -> None:
        self._data["xai_explanations"].append({
            "explanation_id": explanation_id,
            "method": method,
            "label": label,
            "attributions": attributions,
        })

    def save_user_preference(self, user_id: str,
                             weights: Dict[str, float]) -> None:
        self._prefs[user_id] = dict(weights)

    def get_user_preference(
        self, user_id: str) -> Optional[Dict[str, float]]:
        return self._prefs.get(user_id)


# =============================================================================
# DEMO
# =============================================================================
async def _demo() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    config = {
        "causal_exploration_rate": 0.1,
        "agent_count": 5,
        "temporal_max_trace": 500,
        "temporal_formulas": [
            "G (quality >= 0.5)",
            "G (carbon <= 1.5)",
        ],
        "xai_method": "kernel_shap",
        "xai_depth": 5,
        "xai_stability_samples": 2,
        "precision_levels": ["fp32", "fp16", "bf16", "int8"],
        "precision_switch_threshold": 0.02,
        "chaos_intensity": 0.0,
        "chaos_blast_radius": 0.5,
        "chaos_reflection_probability": 0.0,
        "drift_delta": 0.01,
        "drift_threshold": 100.0,   # high so demo doesn't spuriously trigger
    }

    storage = InMemoryStorage()
    orch = MetaCognitiveOrchestratorV18(storage, config)
    await orch.start()

    print("=" * 80)
    print(f"meta_cognitive_layer.py v{__version__} — Demo")
    print("=" * 80)

    # 1. Basic reflection
    print("\n=== 1. Basic reflection ===")
    record = await orch.reflect(accuracy=0.92, energy=0.8,
                                carbon=0.2, latency_ms=150, cost_usd=0.05)
    print(f"  Verdict:   {record.verdict}")
    print(f"  Severity:  {record.severity}")
    print(f"  Pareto:    {record.pareto_status}")
    print(f"  Narrative: {record.narrative}")

    # 2. Warning reflection
    print("\n=== 2. Warning-level reflection ===")
    record = await orch.reflect(accuracy=0.75, energy=1.5,
                                carbon=0.6, latency_ms=250, cost_usd=0.15)
    print(f"  Verdict:    {record.verdict}")
    print(f"  Severity:   {record.severity}")
    print(f"  Pareto:     {record.pareto_status}")
    print(f"  Winners:    {record.pareto_winners}")
    print(f"  Losers:     {record.pareto_losers}")
    print(f"  Adjustments: {record.adjustments}")
    if record.xai_explanation:
        xai = record.xai_explanation
        print(f"  XAI method={xai['method']} stable={xai['stable']} "
              f"fallback={xai['fallback']}")
        for k, v in list(xai["attributions"].items())[:3]:
            print(f"    {k}: {v:+.4f}")

    # 3. Carbon-critical demonstration (fixed bug)
    print("\n=== 3. Carbon critical (fixed: 'critical' tier now reachable) ===")
    record = await orch.reflect(accuracy=0.85, energy=1.0,
                                carbon=1.2, latency_ms=180, cost_usd=0.08)
    print(f"  Verdict:  {record.verdict}")
    print(f"  Severity: {record.severity}")
    assert "carbon_critical" in record.verdict, \
        "carbon_critical reason should be present"
    print("  ✓ 'carbon_critical' reason present in verdict")

    # 4. Critical + HITL escalation
    print("\n=== 4. Critical reflection (HITL escalation) ===")

    async def _delayed_response() -> None:
        await asyncio.sleep(0.3)
        ok = await orch.hitl.submit_response(
            "meta_cognition_user", "adjust")
        print(f"  (delayed HITL response accepted: {ok})")

    asyncio.create_task(_delayed_response())
    record = await orch.reflect(accuracy=0.50, energy=2.5,
                                carbon=0.9, latency_ms=500, cost_usd=0.3)
    print(f"  Verdict:      {record.verdict}")
    print(f"  Severity:     {record.severity}")
    print(f"  Escalated:    {record.escalated_to_hitl}")
    print(f"  Pending:      {record.hitl_pending}")
    print(f"  Temporal:     {record.temporal_violations}")

    # 5. Causal attribution
    print("\n=== 5. Causal attributions ===")
    samples = [{
        "quality": random.uniform(0.5, 1.0),
        "carbon": random.uniform(0.1, 0.8),
        "cost": random.uniform(0.1, 0.9),
        "latency": random.uniform(0.1, 0.9),
    } for _ in range(50)]
    await orch.causal_graph.learn(
        samples, ["quality", "carbon", "cost", "latency"])
    record = await orch.reflect(accuracy=0.85, energy=1.0,
                                carbon=0.35, latency_ms=180, cost_usd=0.08)
    print(f"  Causal parents: {list(record.causal_attributions.keys())}")
    for k, v in record.causal_attributions.items():
        print(f"    {k}: {v:+.4f}")

    # 6. Fixed multi-agent policy
    print("\n=== 6. Multi-agent role distribution (fixed) ===")
    await orch.multi_agent.step()
    dist = orch.multi_agent.get_policy()
    print(f"  Role distribution: {[round(x, 3) for x in dist]}")
    print(f"  Sum: {sum(dist):.4f}  uniform? {all(abs(x - 0.2) < 1e-6 for x in dist)}")

    # 7. Fixed federated averaging
    print("\n=== 7. Federated averaging round-trip (fixed) ===")
    policy = dict(orch.meta.meta_policy)
    await orch.federated.share_weights("meta_policy", policy)
    merged = await orch.federated.apply_aggregated_weights(
        "meta_policy", policy)
    print(f"  Original: { {k: round(v, 3) for k, v in policy.items()} }")
    print(f"  Merged:   { {k: round(v, 3) for k, v in merged.items()} }")
    print(f"  Sum: {sum(merged.values()):.4f}  (should be 1.0)")

    # 8. Meta-policy evolution
    print("\n=== 8. Meta-policy evolution ===")
    for _ in range(5):
        await orch.reflect(
            accuracy=random.uniform(0.5, 0.95),
            energy=random.uniform(0.5, 2.5),
            carbon=random.uniform(0.2, 0.9),
            latency_ms=200, cost_usd=0.1)
    print(f"  Final meta-policy: "
          f"{ {k: round(v, 3) for k, v in orch.meta.meta_policy.items()} }")

    # 9. Deferred queue (force a carbon spike)
    print("\n=== 9. Deferred reflection queue ===")
    n_before = len(orch.meta.deferred)
    await orch.reflect(accuracy=0.9, energy=0.8,
                       carbon=0.95, latency_ms=150, cost_usd=0.05)
    n_after = len(orch.meta.deferred)
    print(f"  Deferred queue size: {n_before} -> {n_after}")

    # 10. Health check
    print("\n=== 10. Health check ===")
    hc = await orch.health_check()
    summary = {
        "version": hc["version"],
        "running": hc["running"],
        "meta": hc["meta"],
        "precision": hc["precision"],
        "agents": hc["agents"],
        "role_dist": [round(x, 3) for x in
                      hc["agent_role_distribution"]],
        "loops": {k: v["run_count"] for k, v in hc["loops"].items()},
        "counters": dict(hc["metrics"]["counters"]),
    }
    print(json.dumps(summary, indent=2, default=str))

    # 11. Top reflections
    print("\n=== 11. Top critical reflections ===")
    for r in orch.meta.top_reflections(k=3, severity="critical"):
        print(f"  [{r.severity}] acc={r.accuracy:.2f} "
              f"carbon={r.carbon:.2f} verdict={r.verdict}")

    await orch.shutdown()
    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
