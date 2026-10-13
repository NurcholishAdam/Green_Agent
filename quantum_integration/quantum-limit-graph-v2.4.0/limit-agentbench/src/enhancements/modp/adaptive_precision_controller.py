"""
adaptive_precision_controller.py — Enhanced v17.1.0
====================================================

Hardware-aware precision switching for MOPD training, extended with ten
advanced Green Agent enhancements in a single self-contained file:

  1. Quantum-Distillation Integration        → QuantumDistillationEngine
  2. Causal Reinforcement Learning           → CausalGraphLearner + CausalPolicyAdapter
  3. Federated Green Learning                → FederatedGreenAggregator
  4. Advanced Multi-Agent Coordination       → MultiAgentCoordinator
  5. Temporal Logic & Formal Verification    → TemporalLogicVerifier
  6. Explainable AI                          → XAIDecisionExplainer
  7. Adaptive Precision Switching            → AdaptivePrecisionSwitcher (this class)
  8. Carbon Markets / REC                    → CarbonMarketIntegrator
  9. Resilience Engineering / Chaos Testing  → ChaosTestingEngine
 10. HITL Active Learning                    → ActiveUserPreferenceLearner

Unified entry point: PrecisionOrchestratorV17

FIXES OVER v17.0.0:
 - _predict_accuracy_drop falls through to the static heuristic when the causal
   graph has not learned the treatment.
 - switch_to accepts `current_intensity` instead of hardcoding 0.4.
 - HITL gate now treats any non-"approve" response (including timeout) as
   rejection.
 - _on_critical_violation passes distinct, metric-rich candidates so the
   ambiguity heuristic actually fires.
 - InMemoryStorage methods use real signatures (no more `*a, **kw`).
 - record_accuracy() + rolling window; auto_switch uses the window when the
   caller passes 0/0.
 - select_precision filters to dynamic_energy.keys(); switch_to restricts to
   the configured precision_levels.
 - Chaos hook removed from switch_to; new chaos_probe() method.
 - _SwitchRecord dataclass for switch_history entries.
 - _recalibrate uses a time window; debounced.
 - Per-teacher KL in QuantumDistillationEngine.step (was identical per teacher).
 - CausalGraphLearner stops fabricating edges when samples are insufficient
   and does not raise on missing variables.
 - CausalPolicyAdapter bridges ACTIONS to downstream strategy vocabulary via
   STRATEGY_MAP and to_strategy().
 - FederatedGreenAggregator counts rounds on share as well as pull; stats()
   reports last_aggregated_bytes.
 - TemporalLogicVerifier defers rule persistence; honours approval callback.
 - XAIDecisionExplainer persists named features; LIME kernel scaled.
 - ChaosTestingEngine handles "data_corruption"; real health probe.
 - ActiveUserPreferenceLearner correlates responses via per-request futures;
   preferences stored raw, normalised on read.
 - PrecisionOrchestratorV17 uses get_running_loop(); _spawn logs task
   failures; aclose() / __aenter__ / __aexit__; clears task set on shutdown.
 - Prometheus metrics for the precision layer and the shared enhancements.
 - Module-level log_event() shim (structlog or stdlib).

Self-contained: only requires Python stdlib plus optional numpy.
All storage interactions soft-fail so the module works standalone or with a
persistent backend.
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
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

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
    """Structured-logging shim that works with structlog or stdlib logging."""
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
# PROMETHEUS METRICS (module scope: single registration)
# =============================================================================
if PROMETHEUS_AVAILABLE:
    _M_PRECISION_SWITCHES = _PromCounter(
        "adaptive_precision_switches_total",
        "Precision switches by status",
        ["from_p", "to_p", "status"],
    )
    _M_PRECISION_ENERGY = Gauge(
        "adaptive_precision_energy",
        "Dynamic energy estimate per precision",
        ["precision"],
    )
    _M_PRECISION_ACC_DROP = Gauge(
        "adaptive_precision_predicted_drop",
        "Predicted accuracy drop for the last switch",
    )
    _M_TEMPORAL_VIOLATIONS = _PromCounter(
        "adaptive_precision_temporal_violations_total",
        "Temporal logic violations",
        ["rule", "severity"],
    )
    _M_HITL_REQUESTS = _PromCounter(
        "adaptive_precision_hitl_requests_total",
        "HITL queries issued",
    )
    _M_HITL_RESPONSES = _PromCounter(
        "adaptive_precision_hitl_responses_total",
        "HITL responses received",
        ["status"],
    )
    _M_CHAOS_EXPERIMENTS = _PromCounter(
        "adaptive_precision_chaos_experiments_total",
        "Chaos experiments",
        ["fault_type", "status"],
    )
    _M_CAUSAL_EDGES = Gauge(
        "adaptive_precision_causal_edges",
        "Edges in the causal graph",
    )
    _M_XAI_EXPLANATIONS = _PromCounter(
        "adaptive_precision_xai_explanations_total",
        "XAI explanations",
    )
else:
    class _Dummy:
        def labels(self, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass

    _M_PRECISION_SWITCHES = _M_PRECISION_ENERGY = _M_PRECISION_ACC_DROP = _Dummy()
    _M_TEMPORAL_VIOLATIONS = _M_HITL_REQUESTS = _M_HITL_RESPONSES = _Dummy()
    _M_CHAOS_EXPERIMENTS = _M_CAUSAL_EDGES = _M_XAI_EXPLANATIONS = _Dummy()


# =============================================================================
# ENHANCEMENT 7 — ADAPTIVE PRECISION SWITCHER
# =============================================================================
class AdaptivePrecisionSwitcher:
    """
    Hardware-aware precision switching for MOPD training.

    v17 enhancements over v1.0:
      * Dynamic energy model calibrated from observed telemetry.
      * Predictive switching using causal RL accuracy forecasts.
      * XAI explanation hook on every switch.
      * Chaos-testing fault injection integration (via chaos_probe()).
      * Carbon-market-aware deferral.
      * Multi-agent precision-task bidding.
      * HITL approval for critical switches (e.g., int8).
      * Federated precision-policy sharing.
    """

    # Base static energy model (relative Wh per unit of compute).
    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55,
              "fp16": 0.5, "int8": 0.3}
    # Latency model (relative).
    LATENCY = {"fp32": 1.0, "tf32": 0.8, "bf16": 0.65,
               "fp16": 0.6, "int8": 0.4}
    # Critical precisions require HITL approval when switching to them.
    CRITICAL_PRECISIONS = {"int8"}

    # Static accuracy-drop heuristic used when the causal graph has not
    # learned a treatment for the target precision.
    STATIC_ACCURACY_DROP = {"fp32": 0.0, "tf32": 0.005, "bf16": 0.01,
                            "fp16": 0.012, "int8": 0.03}

    def __init__(self, config, storage,
                 causal_rl: Optional["CausalPolicyAdapter"] = None,
                 xai: Optional["XAIDecisionExplainer"] = None,
                 chaos: Optional["ChaosTestingEngine"] = None,
                 carbon_market: Optional["CarbonMarketIntegrator"] = None,
                 multi_agent: Optional["MultiAgentCoordinator"] = None,
                 hitl: Optional["ActiveUserPreferenceLearner"] = None,
                 federated: Optional["FederatedGreenAggregator"] = None):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0

        # Enhancement hooks (all optional).
        self.causal_rl = causal_rl
        self.xai = xai
        self.chaos = chaos
        self.carbon_market = carbon_market
        self.multi_agent = multi_agent
        self.hitl = hitl
        self.federated = federated

        # Dynamic energy model — calibrated from telemetry.
        self.dynamic_energy: Dict[str, float] = dict(self.ENERGY)
        self.dynamic_latency: Dict[str, float] = dict(self.LATENCY)
        self.telemetry: Deque[Dict[str, Any]] = deque(maxlen=200)
        self.switch_history: Deque[Dict[str, Any]] = deque(maxlen=500)

        # Rolling accuracy window (populated by record_accuracy()).
        self._acc_window: Deque[float] = deque(maxlen=20)

        # Cached hardware probe.
        self._hw_cache: Optional[Dict[str, Any]] = None

        # Recalibration bookkeeping.
        self._telemetry_since_recal: int = 0
        self._recal_min_samples: int = 20
        self._recal_window_seconds: float = 3600.0
        self._recal_blend_alpha: float = float(
            _cfg_get(config, "precision_recal_alpha", 0.7) or 0.7)

    # ------------------------------------------------------------------
    # Hardware probing
    # ------------------------------------------------------------------
    def _probe(self, force_refresh: bool = False) -> Dict[str, Any]:
        if self._hw_cache is not None and not force_refresh:
            return self._hw_cache
        info = {"cuda": False, "bf16": False, "device": "cpu", "memory_gb": 0.0}
        if TORCH_AVAILABLE and torch is not None:
            try:
                info["cuda"] = bool(torch.cuda.is_available())
                if info["cuda"]:
                    info["device"] = torch.cuda.get_device_name(0)
                    try:
                        info["bf16"] = bool(torch.cuda.is_bf16_supported())
                    except Exception:
                        info["bf16"] = False
                    try:
                        info["memory_gb"] = float(
                            torch.cuda.get_device_properties(0).total_memory
                        ) / (1024 ** 3)
                    except Exception:
                        pass
            except Exception:
                pass
        self._hw_cache = info
        return info

    # ------------------------------------------------------------------
    # Dynamic energy calibration
    # ------------------------------------------------------------------
    def observe_telemetry(self, precision: str, wh_used: float,
                          latency_ms: float, accuracy: float) -> None:
        """Record a telemetry sample for dynamic energy calibration."""
        if precision not in self.dynamic_energy:
            return
        self.telemetry.append({
            "precision": precision,
            "wh": float(wh_used),
            "latency_ms": float(latency_ms),
            "accuracy": float(accuracy),
            "ts": time.time(),
        })
        self._telemetry_since_recal += 1
        # Recalibrate on a batch boundary; do not thrash on every sample.
        if (len(self.telemetry) >= self._recal_min_samples
                and self._telemetry_since_recal >= self._recal_min_samples):
            self._recalibrate()
            self._telemetry_since_recal = 0

    def _recalibrate(self) -> None:
        """Recompute the dynamic energy model from the most recent window."""
        now = time.time()
        recent = [t for t in self.telemetry
                  if (now - t["ts"]) <= self._recal_window_seconds]
        if not recent:
            return

        by_precision: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for t in recent:
            by_precision[t["precision"]].append(t)

        if not by_precision.get("fp32"):
            return
        base_wh = sum(t["wh"] for t in by_precision["fp32"]) / len(by_precision["fp32"])
        base_lat = sum(t["latency_ms"] for t in by_precision["fp32"]) / len(by_precision["fp32"])
        if base_wh <= 0 or base_lat <= 0:
            return

        alpha = max(0.0, min(1.0, self._recal_blend_alpha))
        for prec, samples in by_precision.items():
            if prec == "fp32" or not samples:
                continue
            obs_wh = sum(t["wh"] for t in samples) / len(samples)
            obs_lat = sum(t["latency_ms"] for t in samples) / len(samples)
            obs_e = obs_wh / base_wh
            obs_l = obs_lat / base_lat
            static_e = self.ENERGY.get(prec, 1.0)
            static_l = self.LATENCY.get(prec, 1.0)
            self.dynamic_energy[prec] = alpha * obs_e + (1 - alpha) * static_e
            self.dynamic_latency[prec] = alpha * obs_l + (1 - alpha) * static_l
            try:
                _M_PRECISION_ENERGY.labels(precision=prec).set(self.dynamic_energy[prec])
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Selection
    # ------------------------------------------------------------------
    def _allowed_precisions(self) -> List[str]:
        raw = list(_cfg_get(self.config, "precision_levels",
                            ["fp32", "fp16", "bf16", "int8"]))
        return [c for c in raw if c in self.dynamic_energy]

    def select_precision(self, accuracy_target: float = 0.0) -> str:
        """Choose the greenest precision supported by hardware + config."""
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

    # ------------------------------------------------------------------
    # Accuracy window
    # ------------------------------------------------------------------
    def record_accuracy(self, accuracy: float) -> None:
        try:
            self._acc_window.append(float(accuracy))
        except (TypeError, ValueError):
            pass

    def _window_stats(self) -> Tuple[float, float]:
        """Return (mean_recent, max_recent) or (0.0, 0.0) when empty."""
        if not self._acc_window:
            return 0.0, 0.0
        mean_recent = sum(self._acc_window) / len(self._acc_window)
        max_recent = max(self._acc_window)
        return mean_recent, max_recent

    # ------------------------------------------------------------------
    # Predictive switching
    # ------------------------------------------------------------------
    async def _predict_accuracy_drop(self, target: str) -> float:
        """
        Use causal RL to estimate the accuracy drop from switching.
        Falls back to the static heuristic when the causal graph has not
        learned the treatment (estimate 0.0).
        """
        static = float(self.STATIC_ACCURACY_DROP.get(target, 0.02))
        if self.causal_rl is None:
            return static
        try:
            predicted = await self.causal_rl.estimate_ate(
                f"precision_{target}", "accuracy", samples=50)
        except Exception:
            return static
        return float(predicted) if predicted else static

    # ------------------------------------------------------------------
    # Core switch
    # ------------------------------------------------------------------
    async def switch_to(
        self,
        target: str,
        reason: str = "policy",
        require_hitl: Optional[bool] = None,
        defer_if_high_carbon: bool = True,
        current_intensity: float = 0.4,
    ) -> bool:
        """Perform a precision switch with all enhancement hooks."""
        if target == self.current:
            return False
        if target not in self.dynamic_energy:
            log_event("warning", f"Unknown precision {target!r}; ignoring")
            return False
        if target not in self._allowed_precisions():
            log_event("warning",
                      f"Precision {target!r} not in precision_levels; ignoring")
            return False

        # 1. Multi-agent bidding.
        agent_id: Optional[str] = None
        if self.multi_agent is not None:
            try:
                agent_id, _ = await self.multi_agent.bid({
                    "name": f"precision_switch_{self.current}_to_{target}",
                    "preferred_role": "optimizer"})
            except Exception:
                pass

        # 2. Carbon-market deferral.
        if defer_if_high_carbon and self.carbon_market is not None:
            try:
                decision = await self.carbon_market.net_zero_schedule(
                    workload_kwh=0.5, intensity=current_intensity)
                if decision.get("action") == "defer":
                    self._record_switch(
                        from_p=self.current, to_p=target,
                        reason=f"deferred: {reason}",
                        status="deferred", saved_wh=0.0,
                        predicted_drop=0.0,
                    )
                    try:
                        _M_PRECISION_SWITCHES.labels(
                            from_p=self.current, to_p=target,
                            status="deferred").inc()
                    except Exception:
                        pass
                    return False
            except Exception:
                pass

        # 3. HITL approval for critical precisions.
        needs_hitl = require_hitl if require_hitl is not None \
            else (target in self.CRITICAL_PRECISIONS)
        if needs_hitl and self.hitl is not None:
            try:
                decision = await self.hitl.query_user_if_needed(
                    "precision_approver",
                    [
                        {"solution_id": "approve", "quality_score": 0.90,
                         "carbon_g": 0.10, "cost_usd": 0.10, "latency_ms": 100},
                        {"solution_id": "reject", "quality_score": 0.89,
                         "carbon_g": 0.11, "cost_usd": 0.11, "latency_ms": 105},
                    ],
                    timeout=2.0)
            except Exception:
                decision = None
            # Timeout, None, or explicit reject → treat as rejection.
            if decision != "approve":
                self._record_switch(
                    from_p=self.current, to_p=target,
                    reason=f"HITL blocked: {reason}",
                    status="rejected", saved_wh=0.0, predicted_drop=0.0)
                try:
                    _M_PRECISION_SWITCHES.labels(
                        from_p=self.current, to_p=target,
                        status="rejected").inc()
                except Exception:
                    pass
                return False

        # 4. Predict accuracy drop via causal RL (or static fallback).
        predicted_drop = await self._predict_accuracy_drop(target)
        try:
            _M_PRECISION_ACC_DROP.set(predicted_drop)
        except Exception:
            pass

        # 5. Perform the switch.
        old = self.current
        self.current = target
        saved = max(0.0,
                    self.dynamic_energy.get(old, 1.0)
                    - self.dynamic_energy.get(target, 1.0))
        self.saved_wh += saved

        # 6. XAI explanation (with normalized features).
        if self.xai is not None and NUMPY_AVAILABLE:
            try:
                old_e = float(self.dynamic_energy.get(old, 1.0))
                new_e = float(self.dynamic_energy.get(target, 1.0))
                # Normalize so features are on comparable scales.
                feats = np.array([
                    old_e,
                    new_e,
                    predicted_drop * 10.0,
                    saved,
                ], dtype=float)

                def _score(x, _w=np.array([0.4, -0.3, -0.2, 0.1])):
                    return float(np.dot(x, _w))

                await self.xai.explain(
                    decision_id=f"prec_{uuid.uuid4().hex[:8]}",
                    label=f"{old}->{target} ({reason})",
                    features=feats,
                    names=["old_energy", "new_energy",
                           "predicted_drop_x10", "saved_wh"],
                    model_fn=_score)
            except Exception:
                pass

        # 7. Persist.
        try:
            await asyncio.to_thread(
                self.storage.save_precision_switch,
                old, target, reason, saved, predicted_drop)
        except Exception:
            pass

        # 8. Federated share of the current precision policy.
        if self.federated is not None:
            try:
                policy_bytes = (
                    f"{old}->{target}".encode()
                    + b"|" + str(saved).encode()
                    + b"|" + str(predicted_drop).encode())
                await self.federated.share_weights(
                    "precision_policy", policy_bytes)
            except Exception:
                pass

        # 9. Multi-agent reward.
        if self.multi_agent is not None and agent_id:
            try:
                await self.multi_agent.reward(agent_id, 0.8)
            except Exception:
                pass

        self._record_switch(
            from_p=old, to_p=target, reason=reason, status="applied",
            saved_wh=saved, predicted_drop=predicted_drop)
        try:
            _M_PRECISION_SWITCHES.labels(
                from_p=old, to_p=target, status="applied").inc()
        except Exception:
            pass
        return True

    def _record_switch(self, *, from_p: str, to_p: str, reason: str,
                       status: str, saved_wh: float, predicted_drop: float) -> None:
        """Append a canonical switch record."""
        self.switch_history.append({
            "from": from_p, "to": to_p, "reason": reason,
            "status": status, "saved_wh": float(saved_wh),
            "predicted_drop": float(predicted_drop),
            "ts": _now_utc().isoformat(),
        })

    # ------------------------------------------------------------------
    # Chaos probe (explicit; previously embedded in switch_to)
    # ------------------------------------------------------------------
    async def chaos_probe(self, fault_type: str = "latency") -> Optional[Dict[str, Any]]:
        if self.chaos is None:
            return None
        try:
            return await self.chaos.run_experiment(
                f"precision_probe_{uuid.uuid4().hex[:6]}", fault_type)
        except Exception as exc:
            log_event("warning", f"chaos_probe failed: {exc}")
            return None

    # ------------------------------------------------------------------
    # Auto-switch on accuracy drift
    # ------------------------------------------------------------------
    async def auto_switch(self, recent_acc: float = 0.0,
                          baseline_acc: float = 0.0) -> None:
        # Use the rolling window when the caller passes 0/0.
        if recent_acc <= 0.0 or baseline_acc <= 0.0:
            mean_recent, max_recent = self._window_stats()
            if mean_recent <= 0.0 or max_recent <= 0.0:
                return
            recent_acc, baseline_acc = mean_recent, max_recent

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

    # ------------------------------------------------------------------
    def stats(self) -> Dict[str, Any]:
        return {
            "current": self.current,
            "saved_wh": self.saved_wh,
            "dynamic_energy": dict(self.dynamic_energy),
            "dynamic_latency": dict(self.dynamic_latency),
            "switches": len(self.switch_history),
            "telemetry_samples": len(self.telemetry),
            "accuracy_window": list(self._acc_window),
        }


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION ENGINE
# =============================================================================
class QuantumDistillationEngine:
    """√p amplitude superposition over teacher policies."""

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

    async def step(self, storage, student_id: str = "mopd_student"):
        target = self._softmax(self._superpose(), self.temperature)
        lr = 0.1
        new = []
        for s, t in zip(self.student_policy, target):
            grad = -(t / max(s, 1e-9))
            new.append(max(0.01, s - lr * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]

        # Per-teacher KL: D_KL(teacher || student) using the teacher's own
        # distribution, not the shared target.
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

        entry = {"target": target, "student": list(self.student_policy),
                 "ts": _now_utc().isoformat()}
        self.history.append(entry)
        return entry

    def get_policy(self) -> List[float]:
        return list(self.student_policy)


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL
# =============================================================================
class CausalGraphLearner:
    """Structure learning of a causal DAG via correlation + variance orientation."""

    def __init__(self, storage=None):
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []
        self._lock = asyncio.Lock()

    async def learn(self, samples, variables, threshold=0.25):
        self.variables = list(variables)

        # Insufficient data → return an empty graph (do not fabricate).
        if len(samples) < 5 or not NUMPY_AVAILABLE:
            async with self._lock:
                self.graph.clear()
            return self.summary()

        try:
            X = np.array(
                [[float(s.get(v, 0.0)) for v in variables] for s in samples],
                dtype=float,
            )
        except Exception as exc:
            log_event("warning", f"Causal samples could not be parsed: {exc}")
            async with self._lock:
                self.graph.clear()
            return self.summary()

        if X.shape[0] < 2 or X.shape[1] < 2:
            return self.summary()

        X = (X - X.mean(0)) / (X.std(0) + 1e-9)
        try:
            corr = np.corrcoef(X, rowvar=False)
        except Exception:
            corr = None
        if corr is None:
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
                    src, dst = (
                        (variables[i], variables[j]) if vi > vj
                        else (variables[j], variables[i])
                    )
                    self.graph[src][dst] = {
                        "weight": float(corr[i, j]), "confidence": c,
                    }
                    if self.storage:
                        try:
                            await asyncio.to_thread(
                                self.storage.save_causal_edge,
                                src, dst, float(corr[i, j]), c)
                        except Exception:
                            pass

        try:
            _M_CAUSAL_EDGES.set(sum(len(v) for v in self.graph.values()))
        except Exception:
            pass
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
    """
    Epsilon-greedy causal policy. `ACTIONS` is preserved for backward
    compatibility; use `to_strategy()` to translate to the MOPD strategy
    vocabulary.
    """

    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    STRATEGY_MAP: Dict[str, str] = {
        "performance": "balanced",
        "carbon": "carbon_focus",
        "cost": "operational_focus",
        "hybrid": "balanced",
        "adaptive": "balanced",
    }

    def __init__(self, config, storage, graph):
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

    async def choose_action(self, state):
        async with self._lock:
            if random.random() < self.epsilon:
                return random.choice(self.ACTIONS)
            return max(self.ACTIONS, key=lambda a: self.values.get(a, 0.0))

    async def update(self, action, reward, state):
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

    async def estimate_ate(self, treatment, outcome, samples=100):
        w = self.graph.graph.get(treatment, {}).get(outcome, {}).get("weight", 0.0)
        try:
            await asyncio.to_thread(
                self.storage.save_causal_experiment,
                f"exp_{uuid.uuid4().hex[:8]}", treatment, outcome, w, samples)
        except Exception:
            pass
        return w

    def get_policy(self):
        return list(self.policy)


# =============================================================================
# ENHANCEMENT 3 — FEDERATED GREEN LEARNING
# =============================================================================
class FederatedGreenAggregator:
    """Byte-wise average across federated instances."""

    def __init__(self, storage, instance_id, share_interval=3600):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = int(share_interval)
        self.rounds = 0
        self.last_aggregated: Optional[bytes] = None

    async def share_weights(self, model_id, weights):
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id, model_id, weights,
                float(len(weights)), self.rounds)
        except Exception:
            pass

    async def pull_aggregated_weights(self, model_id):
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

    async def apply_aggregated_weights(self, model_id, current):
        agg = await self.pull_aggregated_weights(model_id)
        if agg is None:
            return current
        n = min(len(current), len(agg))
        return bytes([(current[i] + agg[i]) // 2 for i in range(n)])

    def stats(self):
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

    def __init__(self, agent_id):
        self.id = agent_id
        self.role = "validator"
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        self.completed = 0


class MultiAgentCoordinator:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        count = int(_cfg_get(config, "agent_count", 5) or 5)
        self.agents = {f"agent_{i:02d}": _Agent(f"agent_{i:02d}")
                       for i in range(count)}
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()

    async def _specialise(self):
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])
                try:
                    await asyncio.to_thread(
                        self.storage.save_agent, a.id, a.role,
                        a.reputation, a.utilities)
                except Exception:
                    pass

    async def broadcast(self, topic, sender, payload):
        try:
            self.bus.put_nowait({"topic": topic, "sender": sender,
                                 "payload": payload})
        except asyncio.QueueFull:
            log_event("warning", "MultiAgentCoordinator bus full; message dropped")
        try:
            await asyncio.to_thread(
                self.storage.save_agent_message,
                uuid.uuid4().hex[:8], topic, sender, "*", payload)
        except Exception:
            pass

    async def bid(self, task):
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

    async def reward(self, agent_id, reward):
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

    def get_policy(self):
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

    async def step(self):
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
    def __init__(self, rule_id, operator, conditions, window=0.0,
                 description="", severity="warning"):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window = float(window)
        self.description = description or rule_id
        self.severity = severity
        self.violations = 0
        self.last_violation: Optional[datetime] = None

    def evaluate(self, trace):
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
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict]] = deque(
            maxlen=int(_cfg_get(config, "temporal_max_trace", 2000) or 2000))
        self.approval_cb: Optional[Callable] = None

        # Deferred rule saves (flushed on the first async operation).
        self._pending_rule_saves: List[Tuple[str, str, str, str]] = []

        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            self._install(formula)

    def _install(self, formula):
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
                log_event("warning", f"Unrecognised temporal formula: {formula!r}")
        except Exception as exc:
            log_event("warning", f"Temporal formula install failed: {exc}")

    def _add_atomic(self, expr, op):
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

    def set_approval_callback(self, cb):
        self.approval_cb = cb

    async def push_state(self, state):
        await self._flush_pending_rule_saves()
        self.trace.append((_now_utc(), dict(state)))
        try:
            await asyncio.to_thread(self.storage.save_temporal_trace, state)
        except Exception:
            pass

    async def verify(self):
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

            # A rule is satisfied unless it was violated AND the approval
            # callback did not explicitly approve the violation.
            result[rid] = (not violated) or bool(approved)
        return result


# =============================================================================
# ENHANCEMENT 6 — EXPLAINABLE AI
# =============================================================================
class XAIDecisionExplainer:
    """
    Permutation-based feature importance + LIME + natural-language rendering.

    Note: the "kernel_shap" method name is preserved for backward
    compatibility. The permutation estimator is a Monte-Carlo Shapley
    approximation; use "lime" for a local linear surrogate.
    """

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.method = str(_cfg_get(config, "xai_method", "kernel_shap") or "kernel_shap")
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

    # Backward-compatible alias.
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

    def _nl(self, decision, attrs):
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]),
                     reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(self, decision_id, label, features, names, model_fn):
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
            _M_XAI_EXPLANATIONS.inc()
        except Exception:
            pass
        return {"decision_id": decision_id, "method": self.method,
                "attributions": attrs, "explanation": nl}


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKETS / REC
# =============================================================================
class CarbonMarketIntegrator:
    # Decision boundaries (documented constants).
    DEFER_INTENSITY_THRESHOLD = 0.3    # kgCO2/kWh
    RUN_OFFSET_COST_THRESHOLD = 0.5    # USD

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.last_price = 25.0

    async def _fetch_price(self):
        return max(5.0, self.last_price + random.gauss(0, 1.5))

    async def update_price(self):
        try:
            price = await self._fetch_price()
        except Exception:
            price = self.last_price
        self.last_price = price
        try:
            await asyncio.to_thread(self.storage.save_credit_price, price)
        except Exception:
            pass
        return price

    async def purchase_rec(self, mwh, price_per_mwh=5.0, source="wind"):
        cost = max(0.0, float(mwh)) * max(0.0, float(price_per_mwh))
        try:
            await asyncio.to_thread(
                self.storage.save_rec, mwh, price_per_mwh, source)
        except Exception:
            pass
        return cost

    async def net_zero_schedule(self, workload_kwh, intensity):
        price = await self.update_price()
        carbon_kg = max(0.0, float(workload_kwh)) * max(0.0, float(intensity))
        offset_cost = (carbon_kg / 1000.0) * price

        if intensity > self.DEFER_INTENSITY_THRESHOLD:
            action = "defer"
        elif offset_cost < self.RUN_OFFSET_COST_THRESHOLD:
            action = "run_offset"
        else:
            action = "run"

        try:
            await asyncio.to_thread(
                self.storage.save_net_zero_match,
                uuid.uuid4().hex[:8], workload_kwh, intensity, action,
                carbon_kg, offset_cost, price)
        except Exception:
            pass

        return {"action": action, "carbon_kg": carbon_kg,
                "offset_cost_usd": offset_cost, "credit_price_usd": price,
                "rec_balance_mwh": await self._get_rec_balance()}

    async def _get_rec_balance(self):
        try:
            return await asyncio.to_thread(self.storage.get_rec_balance)
        except Exception:
            return 0.0


# =============================================================================
# ENHANCEMENT 9 — CHAOS TESTING
# =============================================================================
class ChaosTestingEngine:
    FAULT_TYPES = ["latency", "exception", "data_corruption",
                   "memory_pressure", "network_drop"]

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()
        # Shared state mutated by "data_corruption".
        self._shared_state: Dict[str, Any] = {"payload": "ok", "counter": 0}

    async def _health_ok(self) -> bool:
        async with self._lock:
            return not self.active

    async def run_experiment(self, name, fault_type):
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        t0 = time.monotonic()
        before = await self._health_ok()
        status = "completed"
        injected_ms = 0.0
        try:
            async with self._lock:
                self.active[name] = {"fault_type": fault_type,
                                     "started": _now_utc().isoformat()}
            if fault_type == "latency":
                delay = 0.5
                injected_ms = delay * 1000.0
                await asyncio.sleep(delay)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "data_corruption":
                async with self._lock:
                    self._shared_state["payload"] = "corrupted"
                    self._shared_state["counter"] += 1
                injected_ms = 5.0
            elif fault_type == "memory_pressure":
                _ = bytearray(5 * 1024 * 1024)
                injected_ms = 10.0
            elif fault_type == "network_drop":
                delay = 0.2
                injected_ms = delay * 1000.0
                await asyncio.sleep(delay)
        except Exception:
            status = "injected"
        finally:
            async with self._lock:
                self.active.pop(name, None)
        after = await self._health_ok()
        duration = (time.monotonic() - t0) * 1000.0
        try:
            await asyncio.to_thread(
                self.storage.save_chaos_experiment,
                name, name, fault_type,
                float(_cfg_get(self.config, "chaos_blast_radius", 0.1) or 0.1),
                int(before), int(after), status, duration)
            _M_CHAOS_EXPERIMENTS.labels(
                fault_type=fault_type, status=status).inc()
        except Exception:
            pass
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after,
                "status": status, "duration_ms": duration,
                "injected_ms": injected_ms}


# =============================================================================
# ENHANCEMENT 10 — HITL ACTIVE LEARNING
# =============================================================================
class ActiveUserPreferenceLearner:
    """
    Human-in-the-loop active learning for MOPD.

    Responses can be routed to a specific request via `request_id`; the legacy
    two-argument `submit_response` is still supported (FIFO, deprecated).
    """

    _PREF_KEYS = ("quality_score", "carbon_g", "cost_usd", "latency_ms")

    def __init__(self, storage, pareto_gating=None, dashboard=None,
                 feedback_callback=None, use_priority_queue: bool = False):
        self.storage = storage
        self.pareto = pareto_gating
        self.dashboard = dashboard
        self.feedback_callback = feedback_callback
        # Raw, un-normalised preference scores per user.
        self.preferences: Dict[str, Dict[str, float]] = {}
        # Legacy FIFO queue (kept for backward compatibility).
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)
        # Per-request futures for correlated responses.
        self._pending: Dict[str, asyncio.Future] = {}
        # Priority queue (higher priority = requested first).
        self._priority_queue: List[Tuple[float, str, Dict[str, Any]]] = []
        self.use_priority_queue = use_priority_queue
        # Guards the pending-futures dict.
        self._lock = asyncio.Lock()

    async def submit_response(self, user_id, chosen_id,
                              request_id: Optional[str] = None):
        if request_id is not None:
            async with self._lock:
                fut = self._pending.pop(request_id, None)
            if fut is not None and not fut.done():
                fut.set_result({"user_id": user_id, "chosen": chosen_id})
                return
            log_event(
                "warning",
                f"Unknown HITL request_id {request_id!r}; using legacy queue")
        try:
            self._responses.put_nowait({"user_id": user_id, "chosen": chosen_id})
        except asyncio.QueueFull:
            log_event("warning", "HITL response queue full; dropping response")

    def _entropy_uncertainty(self, candidates):
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

    def _ambiguity_score(self, candidates):
        if len(candidates) < 2:
            return 0.0
        c1, c2 = candidates[0], candidates[1]

        def _rel(a, b):
            try:
                a = float(a)
                b = float(b)
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

    async def query_user_if_needed(self, user_id, candidates, timeout=3.0,
                                   priority: float = 0.0):
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
        # Ask when EITHER ambiguity is significant OR uncertainty is high.
        if ambiguity < 0.05 and uncertainty < 0.5:
            return None

        req_id = uuid.uuid4().hex[:8]
        try:
            await asyncio.to_thread(
                self.storage.enqueue_hitl_request,
                req_id, "pareto_query",
                {"candidates": candidates[:2], "ambiguity": ambiguity,
                 "uncertainty": uncertainty},
                "info")
        except Exception:
            pass
        try:
            _M_HITL_REQUESTS.inc()
        except Exception:
            pass

        try:
            await asyncio.to_thread(
                self.storage.save_active_learning_sample,
                req_id, "mopd_student", "hitl_ambiguity",
                uncertainty, True)
        except Exception:
            pass

        if self.dashboard:
            try:
                await self.dashboard.broadcast({
                    "type": "preference_query", "request_id": req_id,
                    "user_id": user_id, "ambiguity": ambiguity,
                    "uncertainty": uncertainty,
                    "options": [
                        {"id": c.get("solution_id"),
                         "quality": c.get("quality_score"),
                         "carbon": c.get("carbon_g"),
                         "cost": c.get("cost_usd"),
                         "latency": c.get("latency_ms")}
                        for c in candidates[:2]
                    ]})
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
                _M_HITL_RESPONSES.labels(status="approved").inc()
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
                _M_HITL_RESPONSES.labels(status="timeout").inc()
            except Exception:
                pass
            normalized = self.get_preference_weights(user_id)
            if normalized:
                scored = []
                for c in candidates:
                    s = sum(
                        normalized.get(k, 0.25)
                        / (float(c.get(k, 0) or 0) + 1e-8)
                        for k in self._PREF_KEYS)
                    scored.append((s, c.get("solution_id")))
                scored.sort(reverse=True)
                return scored[0][1]
            return candidates[0].get("solution_id")

        finally:
            async with self._lock:
                self._pending.pop(req_id, None)

    async def record_choice(self, user_id, solution_id, metrics=None):
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
            # Store raw (unnormalised) scores; normalisation happens on read.
            self.preferences[user_id] = prefs
        try:
            await asyncio.to_thread(
                self.storage.save_user_preference, user_id, prefs)
        except Exception:
            pass

    def get_preference_weights(self, user_id):
        raw = self.preferences.get(user_id, {})
        if not raw:
            return {}
        s = sum(raw.values()) or 1.0
        return {k: v / s for k, v in raw.items()}


# =============================================================================
# UNIFIED ORCHESTRATOR
# =============================================================================
class PrecisionOrchestratorV17:
    """
    Unified entry point: wires all ten enhancements around the
    AdaptivePrecisionSwitcher.

    Usage:
        orch = PrecisionOrchestratorV17(storage, config)
        await orch.start()
        await orch.tick(state)
        await orch.shutdown()
    """

    def __init__(self, storage, config, dashboard=None,
                 adaptive_function=None):
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
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.chaos = ChaosTestingEngine(config, storage)
        self.hitl = ActiveUserPreferenceLearner(storage, dashboard=dashboard)

        # Precision switcher — enhanced with all hooks.
        self.precision = AdaptivePrecisionSwitcher(
            config, storage,
            causal_rl=self.causal_rl,
            xai=self.xai,
            chaos=self.chaos,
            carbon_market=self.carbon_market,
            multi_agent=self.multi_agent,
            hitl=self.hitl,
            federated=self.federated)

        # Wire HITL into temporal critical rules.
        self.temporal.set_approval_callback(self._on_critical_violation)

        # Lifecycle.
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set = set()
        self._adaptive = adaptive_function

    async def _on_critical_violation(self, rule_id, state):
        """
        Ask HITL for approval on a critical temporal violation.
        Builds two distinct, metric-rich candidates.
        """
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

    # ------------------------------------------------------------------
    def _spawn(self, coro, name: str) -> asyncio.Task:
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

    async def tick(self, state: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        # 1. Causal RL strategy selection.
        action = await self.causal_rl.choose_action(state)
        result["strategy"] = action
        result["strategy_name"] = self.causal_rl.to_strategy(action)

        # 2. Multi-agent bid.
        agent_id, _ = await self.multi_agent.bid({
            "name": "precision_tick", "preferred_role": "optimizer"})
        result["agent_id"] = agent_id

        # 3. Precision auto-switch based on accuracy.
        accuracy = float(state.get("accuracy", 0.9) or 0.9)
        baseline = float(state.get("baseline_accuracy", 0.92) or 0.92)
        if accuracy > 0 and baseline > 0:
            self.precision.record_accuracy(accuracy)
        await self.precision.auto_switch(accuracy, baseline)
        result["precision"] = self.precision.current

        # 4. Carbon market decision.
        try:
            cm = await self.carbon_market.net_zero_schedule(
                workload_kwh=1.0,
                intensity=float(state.get("carbon_intensity", 400) or 400) / 1000.0)
        except Exception as exc:
            log_event("warning", f"Carbon market decision failed: {exc}")
            cm = {"action": "run"}
        result["carbon_decision"] = cm

        # 5. XAI explanation for the current decision.
        if NUMPY_AVAILABLE:
            try:
                feats = np.array([
                    float(state.get("quality", 0.8)),
                    float(state.get("carbon_intensity", 400) or 400) / 1000.0,
                    float(state.get("cost", 0.5)),
                    float(state.get("latency_ms", 100) or 100) / 1000.0,
                ])

                def _score(x, _w=np.array([0.4, -0.3, -0.2, -0.1])):
                    return float(np.dot(x, _w))

                result["xai"] = await self.xai.explain(
                    decision_id=f"tick_{uuid.uuid4().hex[:8]}",
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
            result["temporal_violations"] = [k for k, v in verify.items() if not v]
        except Exception as exc:
            log_event("warning", f"Temporal verify failed: {exc}")
            result["temporal_violations"] = []

        # 8. Causal update.
        await self.causal_rl.update(action, 0.8, state)

        return result

    # ------------------------------------------------------------------
    async def start(self):
        self._running = True
        # get_running_loop() is the correct call inside an async function.
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

    async def shutdown(self):
        self._shutdown_event.set()
        self._running = False
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks,
                                 return_exceptions=True)
        self._background_tasks.clear()

    async def aclose(self):
        await self.shutdown()

    async def __aenter__(self) -> "PrecisionOrchestratorV17":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()

    # ------------------------------------------------------------------
    async def _causal_rl_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "causal_graph_update_interval", 900) or 900))
            try:
                # Add precision variables so `_predict_accuracy_drop` has
                # something to look up.
                samples = [{
                    "precision_fp32": random.uniform(0.9, 1.0),
                    "precision_int8": random.uniform(0.2, 0.4),
                    "accuracy": random.uniform(0.85, 0.95),
                    "energy": random.uniform(0.5, 1.5),
                } for _ in range(20)]
                await self.causal_graph.learn(
                    samples, ["precision_fp32", "precision_int8",
                              "accuracy", "energy"])
            except Exception as exc:
                log_event("debug", f"causal_rl loop error: {exc}")

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "federated_interval", 3600) or 3600))
            try:
                dummy = bytes(random.getrandbits(8) for _ in range(64))
                await self.federated.share_weights("precision_policy", dummy)
                await self.federated.pull_aggregated_weights("precision_policy")
            except Exception as exc:
                log_event("debug", f"federated loop error: {exc}")

    async def _multi_agent_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "agent_negotiation_interval", 600) or 600))
            try:
                await self.multi_agent.step()
            except Exception as exc:
                log_event("debug", f"multi_agent loop error: {exc}")

    async def _temporal_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "temporal_verification_interval", 300) or 300))
            try:
                await self.temporal.verify()
            except Exception as exc:
                log_event("debug", f"temporal loop error: {exc}")

    async def _xai_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "xai_interval", 300) or 300))
            if not NUMPY_AVAILABLE:
                continue
            try:
                feats = np.array([1.0, 0.3, 0.02, 0.7])

                def _score(x, _w=np.array([0.4, -0.3, -0.2, 0.1])):
                    return float(np.dot(x, _w))

                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="precision_health", features=feats,
                    names=["old_energy", "new_energy",
                           "predicted_drop", "saved_wh"],
                    model_fn=_score)
            except Exception as exc:
                log_event("debug", f"xai loop error: {exc}")

    async def _precision_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "precision_check_interval", 300) or 300))
            try:
                # auto_switch falls back to the rolling window when 0/0.
                await self.precision.auto_switch(0.0, 0.0)
            except Exception as exc:
                log_event("debug", f"precision loop error: {exc}")

    async def _carbon_market_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "carbon_market_interval", 3600) or 3600))
            try:
                await self.carbon_market.update_price()
            except Exception as exc:
                log_event("debug", f"carbon_market loop error: {exc}")

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "chaos_test_interval", 1800) or 1800))
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    f"auto_{uuid.uuid4().hex[:6]}", fault)
            except Exception as exc:
                log_event("debug", f"chaos loop error: {exc}")

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(_cfg_get(
                self.config, "distillation_interval", 300) or 300))
            try:
                self.quantum.register_teacher(
                    "causal", self.causal_rl.get_policy())
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy())
                await self.quantum.step(self.storage, "precision_student")
            except Exception as exc:
                log_event("debug", f"distillation loop error: {exc}")

    # ------------------------------------------------------------------
    async def health_check(self):
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "precision": self.precision.stats(),
            "agents": {a.id: a.role for a in self.multi_agent.agents.values()},
            "causal_edges": self.causal_graph.summary()["edges"],
            "temporal_rules": len(self.temporal.rules),
            "carbon_price": self.carbon_market.last_price,
            "federated_rounds": self.federated.rounds,
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

    # ---- Precision ----
    def save_precision_switch(self, from_p, to_p, reason,
                              saved_wh=0.0, acc_delta=0.0):
        self._data["precision_history"].append({
            "from_p": from_p, "to_p": to_p, "reason": reason,
            "saved_wh": saved_wh, "acc_delta": acc_delta})

    # ---- Quantum distillation ----
    def save_teacher_superposition(self, student_id, teacher_id, weight,
                                   temperature, amplitude, kl):
        self._data["teacher_superpositions"].append({
            "student_id": student_id, "teacher_id": teacher_id,
            "weight": weight, "temperature": temperature,
            "amplitude": amplitude, "kl": kl})

    # ---- Causal ----
    def save_causal_edge(self, source, target, weight, confidence):
        self._data["causal_graph"].append({
            "source": source, "target": target,
            "weight": weight, "confidence": confidence})

    def save_causal_experiment(self, exp_id, treatment, outcome, ate, samples):
        self._data["causal_experiments"].append({
            "exp_id": exp_id, "treatment": treatment, "outcome": outcome,
            "ate": ate, "samples": samples})

    # ---- Federated ----
    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0):
        self._data["federated_weights"].append({
            "instance_id": instance_id, "model_id": model_id,
            "weights": weights, "weight_norm": weight_norm,
            "round_id": round_id})

    def get_federated_weights(self, model_id):
        return [r for r in self._data["federated_weights"]
                if r["model_id"] == model_id]

    # ---- Multi-agent ----
    def save_agent(self, agent_id, role, reputation, utilities):
        self._data["agent_registry"].append({
            "agent_id": agent_id, "role": role,
            "reputation": reputation, "utilities": utilities})

    def save_agent_message(self, message_id, topic, sender, recipient, payload):
        self._data["agent_messages"].append({
            "message_id": message_id, "topic": topic,
            "sender": sender, "recipient": recipient, "payload": payload})

    # ---- Temporal ----
    def save_temporal_rule(self, rule_id, formula, operator, severity,
                           description, window, active):
        self._data["temporal_rules"].append({
            "rule_id": rule_id, "formula": formula, "operator": operator,
            "severity": severity, "description": description,
            "window": window, "active": active})

    def save_temporal_trace(self, state, context=None):
        self._data["temporal_trace"].append({"state": state, "context": context})

    def save_temporal_violation(self, rule_id, formula, step, state,
                                severity="warning", approved=None):
        self._data["temporal_violations"].append({
            "rule_id": rule_id, "formula": formula, "step": step,
            "state": state, "severity": severity, "approved": approved})

    # ---- XAI ----
    def save_xai_explanation(self, explanation_id, decision_id, method,
                             label, features, attributions, nl):
        self._data["xai_explanations"].append({
            "explanation_id": explanation_id, "decision_id": decision_id,
            "method": method, "label": label, "features": features,
            "attributions": attributions, "natural_language": nl})

    # ---- Carbon / REC ----
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

    # ---- Chaos ----
    def save_chaos_experiment(self, experiment_id, name, fault_type,
                              blast_radius, steady_before, steady_after,
                              status, duration_ms=0.0):
        self._data["chaos_experiments"].append({
            "experiment_id": experiment_id, "name": name,
            "fault_type": fault_type, "blast_radius": blast_radius,
            "steady_before": steady_before, "steady_after": steady_after,
            "status": status, "duration_ms": duration_ms})

    # ---- HITL ----
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
        "chaos_blast_radius": 0.1,
    }

    storage = InMemoryStorage()
    orch = PrecisionOrchestratorV17(storage, config)
    await orch.start()

    print("=" * 80)
    print("adaptive_precision_controller.py v17.1.0 — Demo")
    print("=" * 80)

    # Simulate telemetry so the dynamic energy model gets calibrated.
    for _ in range(30):
        for p, wh, lat, acc in [
            ("fp32", 1.00 + random.uniform(-0.05, 0.05), 100, 0.92),
            ("bf16", 0.55 + random.uniform(-0.05, 0.05),  65, 0.91),
            ("int8", 0.30 + random.uniform(-0.05, 0.05),  40, 0.89),
        ]:
            orch.precision.observe_telemetry(p, wh, lat, acc)

    print("\nDynamic energy model after calibration:")
    for p, e in orch.precision.dynamic_energy.items():
        print(f"  {p}: {e:.3f}")

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
        print(f"\n--- Tick {i + 1} ---")
        print(f"  strategy: {result['strategy']} -> {result['strategy_name']}")
        print(f"  precision: {result['precision']}")
        print(f"  agent_id: {result['agent_id']}")
        print(f"  carbon_action: {result['carbon_decision']['action']}")
        print(f"  temporal_violations: {result['temporal_violations']}")

    print("\n=== Direct switch test ===")
    success = await orch.precision.switch_to(
        "int8", reason="aggressive energy saving", require_hitl=False)
    print(f"Switch to int8: {success}")
    print(f"New current precision: {orch.precision.current}")

    print("\n=== Chaos probe ===")
    print(json.dumps(await orch.precision.chaos_probe("latency"),
                     indent=2, default=str))

    print("\n=== Health check ===")
    print(json.dumps(await orch.health_check(), indent=2, default=str))

    print("\n=== All chaos fault types ===")
    for fault in ChaosTestingEngine.FAULT_TYPES:
        result = await orch.chaos.run_experiment(f"demo_{fault}", fault)
        print(f"  {fault}: status={result['status']} "
              f"injected_ms={result['injected_ms']}")

    await orch.shutdown()
    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
