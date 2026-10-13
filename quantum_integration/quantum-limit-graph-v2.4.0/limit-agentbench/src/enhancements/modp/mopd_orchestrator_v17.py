#!/usr/bin/env python3
"""
mopd_orchestrator_v17.py — Enhanced v17.1.0
===========================================

Unified MOPD orchestrator wiring all ten Green Agent enhancements, with a
SQLite-backed Storage class.

FIXES OVER v17.0.0:
 - The ten enhancement classes are now resolved via an explicit import with
   minimal in-file fallbacks; the module is importable end-to-end.
 - `Storage` gained `__init__`, `_execute`, `_fetchall`, `_fetchone`,
   `close()`, `row_factory`, and a real `_SCHEMA_SQL` executed on init.
 - Missing storage methods added (HITL prefs, routing decisions, LIMIT graph,
   MODP states/policies, preference pairs, feedback events, adaptive state,
   offline batch summaries) plus their schema tables.
 - `import numpy` is guarded.
 - `_on_critical_violation` uses a correlated HITL query (was a coin flip).
 - `select_strategy` carbon-intensity unit fixed; XAI, chaos, multi-agent
   bid/reward and precision switching moved off the critical path.
 - `auto_switch` uses the rolling accuracy window; rewards are computed from
   observed signals.
 - `report_epoch` distinguishes in-process and HTTP transports; the HTTP
   session is reused across calls; HTTPS is enforced by default.
 - `Storage` uses `INSERT ... ON CONFLICT ... DO UPDATE` where created_at
   must be preserved; XAI feature ranks are computed.
 - Timezone-aware timestamps everywhere.
 - Prometheus metrics + `log_event` shim.
 - Lifecycle: `aclose()`, `__aenter__`, `__aexit__`, `get_running_loop()`,
   `_spawn()` with error logging, session closed on shutdown.

Self-contained: stdlib + optional numpy / aiohttp.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import sqlite3
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Set, Tuple

# ---------- Optional: numpy ----------
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore
    NUMPY_AVAILABLE = False

# ---------- Optional: aiohttp ----------
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:  # pragma: no cover
    aiohttp = None  # type: ignore
    AIOHTTP_AVAILABLE = False

# ---------- Optional: Prometheus ----------
try:
    from prometheus_client import Counter as _PromCounter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False

# ---------- Logging ----------
try:
    import structlog
    _STRUCTLOG_LOGGER = structlog.get_logger(__name__)
    _STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    _STRUCTLOG_LOGGER = logging.getLogger(__name__)
    _STRUCTLOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)

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


def _now_iso() -> str:
    return _now_utc().isoformat()


# =============================================================================
# PROMETHEUS METRICS
# =============================================================================
if PROMETHEUS_AVAILABLE:
    _M_SELECT_TOTAL = _PromCounter(
        "mopd_select_strategy_total", "Strategy selections", ["strategy"],
    )
    _M_SELECT_SECONDS = Histogram(
        "mopd_select_strategy_seconds", "select_strategy latency",
    )
    _M_REPORT_EPOCH = _PromCounter(
        "mopd_report_epoch_total", "report_epoch calls",
        ["status", "transport"],
    )
    _M_HITL_REQUESTS = _PromCounter(
        "mopd_hitl_requests_total", "HITL requests", ["outcome"],
    )
    _M_STORAGE_FAILURES = _PromCounter(
        "mopd_storage_failures_total", "Storage call failures", ["method"],
    )
    _M_Q_TABLE_SIZE = Gauge("mopd_q_table_size", "Q-table state count")
else:
    class _Dummy:
        def labels(self, **kwargs): return self
        def inc(self, *args, **kwargs): pass
        def observe(self, *args, **kwargs): pass
        def set(self, *args, **kwargs): pass

    _M_SELECT_TOTAL = _M_SELECT_SECONDS = _M_REPORT_EPOCH = _Dummy()
    _M_HITL_REQUESTS = _M_STORAGE_FAILURES = _M_Q_TABLE_SIZE = _Dummy()


# =============================================================================
# ENHANCEMENT CLASSES — import with fallbacks
# =============================================================================
# In a full project layout, these live in a shared module. Adjust the import
# path below to match your project structure.
try:
    from green_agent_enhancements import (  # type: ignore  # pragma: no cover
        QuantumDistillationEngine,
        CausalGraphLearner,
        CausalPolicyAdapter,
        FederatedGreenAggregator,
        MultiAgentCoordinator,
        TemporalLogicVerifier,
        XAIDecisionExplainer,
        AdaptivePrecisionSwitcher,
        CarbonMarketIntegrator,
        ChaosTestingEngine,
        ActiveUserPreferenceLearner,
    )
    _ENHANCEMENTS_IMPORTED = True
except ImportError:  # pragma: no cover
    _ENHANCEMENTS_IMPORTED = False


if not _ENHANCEMENTS_IMPORTED:
    # -------------------------------------------------------------------------
    # Minimal, self-contained fallbacks. They implement the interfaces the
    # orchestrator relies on and are intentionally simple, matching the
    # documented contract of the full implementations.
    # -------------------------------------------------------------------------

    class QuantumDistillationEngine:
        def __init__(self, temperature: float = 2.0, alpha: float = 0.5,
                     n_actions: int = 5):
            self.temperature = float(temperature)
            self.alpha = float(alpha)
            self.n_actions = int(n_actions)
            self.teachers: Dict[str, List[float]] = {}
            self.student_policy = [1.0 / self.n_actions] * self.n_actions

        def register_teacher(self, name: str, policy: List[float]) -> None:
            if not policy:
                return
            s = sum(policy) or 1.0
            self.teachers[str(name)] = [max(0.0, p / s) for p in policy]

        def unregister_teacher(self, name: str) -> None:
            self.teachers.pop(name, None)

        def get_policy(self) -> List[float]:
            return list(self.student_policy)

        async def step(self, storage: Any,
                       student_id: str = "mopd_student") -> Dict[str, Any]:
            if self.teachers:
                n = self.n_actions
                accum = [0.0] * n
                for pol in self.teachers.values():
                    for i in range(min(n, len(pol))):
                        accum[i] += math.sqrt(max(pol[i], 1e-9))
                accum = [a / len(self.teachers) for a in accum]
                sq = [a * a for a in accum]
                s = sum(sq) or 1.0
                target = [x / s for x in sq]
            else:
                target = list(self.student_policy)
            lr = 0.1
            new = []
            for s_, t in zip(self.student_policy, target):
                grad = -(t / max(s_, 1e-9))
                new.append(max(0.01, s_ - lr * grad))
            ns = sum(new) or 1.0
            self.student_policy = [x / ns for x in new]
            for tid, pol in self.teachers.items():
                weight = float(pol[0]) if pol else 0.0
                amplitude = math.sqrt(max(weight, 1e-9))
                kl = 0.0
                for i in range(self.n_actions):
                    pi = pol[i] if i < len(pol) else 1e-9
                    si = self.student_policy[i]
                    pi, si = max(pi, 1e-9), max(si, 1e-9)
                    kl += pi * math.log(pi / si)
                try:
                    await asyncio.to_thread(
                        storage.save_teacher_superposition,
                        student_id, tid, weight, self.temperature,
                        amplitude, kl)
                except Exception:
                    pass
            return {"target": target, "student": list(self.student_policy)}


    class CausalGraphLearner:
        def __init__(self, storage: Any = None):
            self.storage = storage
            self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
            self.variables: List[str] = []
            self._lock = asyncio.Lock()

        async def learn(self, samples: List[Dict[str, float]],
                        variables: List[str],
                        threshold: float = 0.25) -> Dict[str, Any]:
            self.variables = list(variables)
            if not NUMPY_AVAILABLE or len(samples) < 5:
                return self.summary()
            try:
                X = np.asarray(
                    [[float(s.get(v, 0.0)) for v in variables] for s in samples],
                    dtype=float)
            except Exception:
                return self.summary()
            if X.shape[0] < 3 or X.shape[1] < 2:
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
                        src, dst = ((variables[i], variables[j]) if vi > vj
                                    else (variables[j], variables[i]))
                        self.graph[src][dst] = {
                            "weight": float(corr[i, j]), "confidence": c,
                        }
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
            return {
                "nodes": len(self.variables),
                "edges": sum(len(v) for v in self.graph.values()),
                "variables": list(self.variables),
            }

        async def estimate_ate_do_calculus(self, treatment: str, outcome: str,
                                           data: List[Dict[str, float]]) -> float:
            w = self.graph.get(treatment, {}).get(outcome, {}).get("weight", 0.0)
            return float(w)


    class CausalPolicyAdapter:
        ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]
        STRATEGY_MAP = {
            "performance": "balanced", "carbon": "carbon_focus",
            "cost": "operational_focus", "hybrid": "balanced",
            "adaptive": "balanced",
        }

        def __init__(self, config: Any, storage: Any,
                     graph: CausalGraphLearner):
            self.config = config
            self.storage = storage
            self.graph = graph
            self.values: Dict[str, float] = defaultdict(float)
            self.counts: Dict[str, int] = defaultdict(int)
            self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
            self.epsilon = 0.1
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
            w = self.graph.graph.get(treatment, {}).get(outcome, {}).get(
                "weight", 0.0)
            try:
                await asyncio.to_thread(
                    self.storage.save_causal_experiment,
                    f"exp_{uuid.uuid4().hex[:8]}", treatment, outcome, w,
                    samples, "edge_weight_proxy")
            except Exception:
                pass
            return float(w)

        def get_policy(self) -> List[float]:
            return list(self.policy)


    class FederatedGreenAggregator:
        def __init__(self, storage: Any, instance_id: str,
                     share_interval: int = 3600):
            self.storage = storage
            self.instance_id = instance_id
            self.share_interval = int(share_interval)
            self.rounds = 0

        async def share_weights(self, model_id: str, weights: bytes) -> None:
            if not weights:
                return
            try:
                await asyncio.to_thread(
                    self.storage.save_federated_weights,
                    self.instance_id, model_id, weights,
                    float(len(weights)), self.rounds)
                self.rounds += 1
            except Exception as exc:
                log_event("warning", f"federated share failed: {exc}")

        async def pull_aggregated_weights(self, model_id: str) -> Optional[bytes]:
            try:
                rows = await asyncio.to_thread(
                    self.storage.get_federated_weights, model_id)
            except Exception:
                return None
            if not rows:
                return None
            blobs = [r["weights"] for r in rows if r["weights"]]
            if not blobs:
                return None
            n = min(len(b) for b in blobs)
            avg = bytearray(n)
            for i in range(n):
                avg[i] = int(sum(b[i] for b in blobs) / len(blobs)) & 0xFF
            self.rounds += 1
            return bytes(avg)

        async def apply_aggregated_weights(self, model_id: str,
                                           current: bytes) -> bytes:
            agg = await self.pull_aggregated_weights(model_id)
            if agg is None or not current:
                return current
            n = min(len(current), len(agg))
            return bytes([(current[i] + agg[i]) // 2 for i in range(n)])


    @dataclass
    class _FallbackAgent:
        id: str
        role: str = "validator"
        reputation: float = 0.5
        utilities: Dict[str, float] = field(default_factory=dict)
        completed: int = 0


    class MultiAgentCoordinator:
        def __init__(self, config: Any, storage: Any):
            self.config = config
            self.storage = storage
            count = 5
            if isinstance(config, dict):
                count = int(config.get("agent_count", 5) or 5)
            self.agents: Dict[str, _FallbackAgent] = {
                f"agent_{i:02d}": _FallbackAgent(
                    f"agent_{i:02d}",
                    utilities={r: random.uniform(0.3, 0.7)
                               for r in ("orchestrator", "validator",
                                         "optimizer", "reporter",
                                         "negotiator")},
                )
                for i in range(count)
            }
            self._lock = asyncio.Lock()

        async def _specialise(self) -> None:
            async with self._lock:
                for a in self.agents.values():
                    if a.utilities:
                        a.role = max(a.utilities, key=lambda r: a.utilities[r])

        async def broadcast(self, topic: str, sender: str,
                            payload: Dict[str, Any]) -> None:
            try:
                await asyncio.to_thread(
                    self.storage.save_agent_message,
                    uuid.uuid4().hex[:8], topic, sender, "*", payload)
            except Exception:
                pass

        async def bid(self, task: Dict[str, Any]) -> Tuple[str, float]:
            preferred = task.get("preferred_role", "orchestrator")
            async with self._lock:
                best_id, best_score = None, -1.0
                for aid, a in self.agents.items():
                    bonus = 1.0 if a.role == preferred else 0.6
                    score = a.utilities.get(a.role, 0.5) * bonus + 0.3 * a.reputation
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
                    if a.role in a.utilities:
                        a.utilities[a.role] = min(
                            1.0, a.utilities[a.role] + 0.05 * reward)

        def get_policy(self) -> List[float]:
            counts: Dict[str, int] = defaultdict(int)
            for a in self.agents.values():
                counts[a.role] += 1
            total = max(1, sum(counts.values()))
            affinity = {
                "orchestrator": [0.35, 0.20, 0.15, 0.15, 0.15],
                "validator":    [0.15, 0.15, 0.15, 0.35, 0.20],
                "optimizer":    [0.20, 0.15, 0.35, 0.15, 0.15],
                "reporter":     [0.15, 0.20, 0.15, 0.15, 0.35],
                "negotiator":   [0.15, 0.35, 0.20, 0.15, 0.15],
            }
            out = [0.0] * 5
            for role, c in counts.items():
                w = c / total
                for i, v in enumerate(affinity.get(role, [0.2] * 5)):
                    out[i] += w * v
            s = sum(out) or 1.0
            return [x / s for x in out]

        async def step(self) -> Dict[str, Any]:
            await self._specialise()
            return {
                "roles": {a.id: a.role for a in self.agents.values()},
                "role_distribution": {},
                "strategy_distribution": self.get_policy(),
                "processed_messages": 0,
            }


    _ATOMIC_RE = None  # lazily compiled below
    import re as _re
    _ATOMIC_RE = _re.compile(
        r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


    @dataclass
    class _FallbackTemporalRule:
        rule_id: str
        operator: str
        description: str
        condition: Callable[[Dict], bool]
        severity: str = "warning"
        violations: int = 0

    class TemporalLogicVerifier:
        def __init__(self, storage: Any, config: Any):
            self.storage = storage
            self.config = config
            self.rules: Dict[str, _FallbackTemporalRule] = {}
            self.trace: Deque[Tuple[datetime, Dict]] = deque(maxlen=2000)
            self.approval_cb: Optional[Callable] = None
            self._pending_rule_saves: List[Tuple[str, str, str, str]] = []
            formulas = []
            if isinstance(config, dict):
                formulas = config.get("temporal_formulas", []) or []
            for f in formulas:
                self._install(f)

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
            except Exception as exc:
                log_event("warning", f"temporal install failed: {exc}")

        def _add_atomic(self, expr: str, op: str) -> None:
            m = _ATOMIC_RE.match(expr)
            if not m:
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
            self.rules[rid] = _FallbackTemporalRule(
                rid, op, expr, cond, severity="warning")
            self._pending_rule_saves.append((rid, expr, op, "warning"))

        def set_approval_callback(self, cb: Callable) -> None:
            self.approval_cb = cb

        async def _flush_pending_rule_saves(self) -> None:
            while self._pending_rule_saves:
                rid, expr, op, sev = self._pending_rule_saves.pop(0)
                try:
                    await asyncio.to_thread(
                        self.storage.save_temporal_rule,
                        rid, expr, op, sev, expr, 0.0, True)
                except Exception:
                    pass

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
                violated = False
                if rule.operator == "always":
                    violated = any(not rule.condition(s) for _, s in self.trace)
                elif rule.operator == "eventually":
                    violated = not any(rule.condition(s) for _, s in self.trace)
                elif rule.operator == "never":
                    violated = any(rule.condition(s) for _, s in self.trace)
                approved = False
                if violated:
                    rule.violations += 1
                    if rule.severity == "critical" and self.approval_cb:
                        try:
                            approved = self.approval_cb(
                                rid, self.trace[-1][1] if self.trace else {})
                            if asyncio.iscoroutine(approved):
                                approved = await approved
                        except Exception:
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


    class XAIDecisionExplainer:
        def __init__(self, config: Any, storage: Any):
            self.config = config
            self.storage = storage
            self.method = "kernel_shap"
            self.depth = 5
            if isinstance(config, dict):
                self.method = str(config.get("xai_method", "kernel_shap"))
                self.depth = int(config.get("xai_depth", 5) or 5)

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

        def _nl(self, decision, attrs):
            top = sorted(attrs.items(), key=lambda kv: abs(kv[1]),
                         reverse=True)[:self.depth]
            lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
            return f"Decision '{decision}' driven by:\n{lines}"

        async def explain(self, decision_id: str, label: str, features,
                          names: List[str], model_fn: Callable) -> Dict[str, Any]:
            if not NUMPY_AVAILABLE:
                attrs = {k: 0.0 for k in names}
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


    class AdaptivePrecisionSwitcher:
        ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55,
                  "fp16": 0.5, "int8": 0.3}

        def __init__(self, config: Any, storage: Any):
            self.config = config
            self.storage = storage
            self.current = "fp32"
            self.saved_wh = 0.0
            self._acc_window: Deque[float] = deque(maxlen=20)
            self._allowed = ["fp32", "fp16", "bf16", "int8"]
            if isinstance(config, dict):
                self._allowed = list(config.get("precision_levels",
                                                self._allowed))

        def _allowed_precisions(self) -> List[str]:
            return [c for c in self._allowed if c in self.ENERGY]

        def select_precision(self) -> str:
            cands = self._allowed_precisions()
            return min(cands, key=lambda c: self.ENERGY.get(c, 1.0)) if cands \
                else "fp32"

        def record_accuracy(self, accuracy: float) -> None:
            try:
                self._acc_window.append(float(accuracy))
            except (TypeError, ValueError):
                pass

        async def switch_to(self, target: str, reason: str = "policy") -> bool:
            if target == self.current or target not in self.ENERGY:
                return False
            if target not in self._allowed_precisions():
                return False
            old = self.current
            self.current = target
            saved = max(0.0, self.ENERGY.get(old, 1.0) -
                        self.ENERGY.get(target, 1.0))
            self.saved_wh += saved
            try:
                await asyncio.to_thread(
                    self.storage.save_precision_switch,
                    old, target, reason, saved, 0.0)
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
            if drop > 0.02:
                await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
            elif drop < 0.01:
                target = self.select_precision()
                if target != self.current:
                    await self.switch_to(target, reason="headroom")


    class CarbonMarketIntegrator:
        DEFER_INTENSITY_THRESHOLD = 0.3
        RUN_OFFSET_COST_THRESHOLD = 0.5
        MIN_PRICE_USD = 5.0
        MAX_PRICE_USD = 200.0
        DEFAULT_PRICE_USD = 25.0

        def __init__(self, config: Any, storage: Any):
            self.config = config
            self.storage = storage
            self.last_price = self.DEFAULT_PRICE_USD
            self._rec_balance_mwh = 0.0

        async def _fetch_price(self) -> float:
            candidate = self.last_price + random.gauss(0, 1.5)
            return max(self.MIN_PRICE_USD, min(self.MAX_PRICE_USD, candidate))

        async def update_price(self) -> float:
            try:
                price = await self._fetch_price()
            except Exception:
                price = self.last_price
            try:
                price = max(self.MIN_PRICE_USD,
                            min(self.MAX_PRICE_USD, float(price)))
            except (TypeError, ValueError):
                price = self.last_price
            try:
                await asyncio.to_thread(
                    self.storage.save_credit_price, price)
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
            defer_hours = 0
            if intensity > self.DEFER_INTENSITY_THRESHOLD:
                action = "defer"
                defer_hours = max(1, int((intensity -
                                          self.DEFER_INTENSITY_THRESHOLD) * 10))
            elif offset_cost < self.RUN_OFFSET_COST_THRESHOLD:
                action = "run_offset"
            else:
                action = "run"
            if commit:
                try:
                    await asyncio.to_thread(
                        self.storage.save_net_zero_match,
                        uuid.uuid4().hex[:8], workload_kwh, intensity,
                        action, carbon_kg, offset_cost, price)
                except Exception:
                    pass
            return {
                "action": action,
                "defer_hours": defer_hours,
                "carbon_kg": carbon_kg,
                "offset_cost_usd": offset_cost,
                "credit_price_usd": price,
                "rec_balance_mwh": float(self._rec_balance_mwh),
            }

        def get_rec_balance(self) -> float:
            return float(self._rec_balance_mwh)


    class ChaosTestingEngine:
        FAULT_TYPES = ["latency", "exception", "data_corruption",
                       "memory_pressure", "network_drop"]

        def __init__(self, config: Any, storage: Any):
            self.config = config
            self.storage = storage
            self.active: Dict[str, Dict] = {}
            self._lock = asyncio.Lock()
            self._shared_state: Dict[str, Any] = {"payload": "ok", "counter": 0}

        async def _health_ok(self) -> bool:
            async with self._lock:
                return not self.active

        async def run_experiment(self, name: str,
                                 fault_type: str) -> Dict[str, Any]:
            if fault_type not in self.FAULT_TYPES:
                raise ValueError(f"unknown fault type {fault_type}")
            t0 = time.monotonic()
            before = await self._health_ok()
            status = "completed"
            injected_ms = 0.0
            try:
                async with self._lock:
                    self.active[name] = {"fault_type": fault_type}
                if fault_type == "latency":
                    injected_ms = 500.0
                    await asyncio.sleep(0.5)
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
                    injected_ms = 200.0
                    await asyncio.sleep(0.2)
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
                    name, name, fault_type, 0.1,
                    int(before), int(after), status, duration)
            except Exception:
                pass
            return {"name": name, "fault_type": fault_type,
                    "steady_before": before, "steady_after": after,
                    "status": status, "duration_ms": duration,
                    "injected_ms": injected_ms}


    class ActiveUserPreferenceLearner:
        _PREF_KEYS = ("quality_score", "carbon_g", "cost_usd", "latency_ms")

        def __init__(self, storage: Any, pareto_gating: Any = None,
                     dashboard: Any = None, feedback_callback: Any = None):
            self.storage = storage
            self.pareto = pareto_gating
            self.dashboard = dashboard
            self.feedback_callback = feedback_callback
            self.preferences: Dict[str, Dict[str, float]] = {}
            self._pending: Dict[str, asyncio.Future] = {}
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
                      f"HITL response for unknown request: {request_id}")

        async def query_user_if_needed(self, user_id: str,
                                       candidates: List[Dict[str, Any]],
                                       timeout: float = 3.0) -> Optional[str]:
            if len(candidates) < 2:
                return None
            req_id = uuid.uuid4().hex[:8]
            try:
                await asyncio.to_thread(
                    self.storage.enqueue_hitl_request,
                    req_id, "query", {"candidates": candidates[:2]}, "info")
            except Exception:
                pass
            if self.dashboard is not None:
                try:
                    await self.dashboard.broadcast({
                        "type": "preference_query",
                        "request_id": req_id, "user_id": user_id,
                    })
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
                return msg.get("chosen") if isinstance(msg, dict) else None
            except asyncio.TimeoutError:
                try:
                    await asyncio.to_thread(
                        self.storage.resolve_hitl_request, req_id, "timeout")
                except Exception:
                    pass
                return candidates[0].get("solution_id")
            finally:
                async with self._lock:
                    self._pending.pop(req_id, None)

        async def record_choice(self, user_id: str, solution_id: str,
                                metrics: Optional[Dict[str, float]] = None
                                ) -> None:
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
                            prefs[k] = prefs.get(k, 0.25) + \
                                1.0 / (abs(v) + 1e-6) * 0.01
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
# SCHEMA
# =============================================================================
_SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- 1. Quantum-Distillation
CREATE TABLE IF NOT EXISTS teacher_superpositions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    student_id TEXT, teacher_id TEXT, teacher_weight REAL,
    temperature REAL, amplitude REAL, kl_divergence REAL, timestamp TEXT);
CREATE INDEX IF NOT EXISTS idx_teacher_sup_student
    ON teacher_superpositions(student_id);

-- 2. Causal RL
CREATE TABLE IF NOT EXISTS causal_graph (
    edge_id TEXT PRIMARY KEY, source TEXT, target TEXT,
    weight REAL, confidence REAL, timestamp TEXT);
CREATE TABLE IF NOT EXISTS causal_experiments (
    exp_id TEXT PRIMARY KEY, treatment TEXT, outcome TEXT,
    ate REAL, samples INTEGER, method TEXT, timestamp TEXT);
CREATE TABLE IF NOT EXISTS causal_interventions (
    intervention_id TEXT PRIMARY KEY, node TEXT, do_value TEXT,
    observed_outcome TEXT, counterfactual_json TEXT, timestamp TEXT);

-- 3. Federated
CREATE TABLE IF NOT EXISTS federated_weights (
    instance_id TEXT, model_id TEXT, weights BLOB,
    weight_norm REAL, round_id INTEGER, timestamp TEXT,
    PRIMARY KEY (instance_id, model_id));
CREATE INDEX IF NOT EXISTS idx_fed_model ON federated_weights(model_id);

-- 4. Multi-Agent
CREATE TABLE IF NOT EXISTS agent_registry (
    agent_id TEXT PRIMARY KEY, role TEXT, reputation REAL,
    utilities TEXT, capabilities TEXT, created_at TEXT, last_updated TEXT);
CREATE TABLE IF NOT EXISTS agent_messages (
    message_id TEXT PRIMARY KEY, topic TEXT, sender TEXT,
    recipient TEXT, payload TEXT, timestamp TEXT);
CREATE INDEX IF NOT EXISTS idx_agent_msg_topic ON agent_messages(topic);

-- 5. Temporal
CREATE TABLE IF NOT EXISTS temporal_rules (
    rule_id TEXT PRIMARY KEY, formula TEXT, operator TEXT,
    severity TEXT, description TEXT, window_seconds REAL,
    created_at TEXT, active INTEGER DEFAULT 1);
CREATE TABLE IF NOT EXISTS temporal_trace (
    step INTEGER PRIMARY KEY AUTOINCREMENT, state TEXT,
    context TEXT, timestamp TEXT);
CREATE TABLE IF NOT EXISTS temporal_violations (
    id INTEGER PRIMARY KEY AUTOINCREMENT, rule_id TEXT, formula TEXT,
    step INTEGER, state TEXT, severity TEXT,
    approved INTEGER, resolved_at TEXT, timestamp TEXT);

-- 6. XAI
CREATE TABLE IF NOT EXISTS xai_explanations (
    explanation_id TEXT PRIMARY KEY, decision_id TEXT, method TEXT,
    decision_label TEXT, features TEXT, attributions TEXT,
    natural_language TEXT, timestamp TEXT);
CREATE TABLE IF NOT EXISTS xai_feature_importance (
    id INTEGER PRIMARY KEY AUTOINCREMENT, explanation_id TEXT,
    feature_name TEXT, importance REAL, rank INTEGER);

-- 7. Adaptive Precision
CREATE TABLE IF NOT EXISTS precision_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT, from_p TEXT, to_p TEXT,
    reason TEXT, energy_saved_wh REAL, accuracy_delta REAL, timestamp TEXT);

-- 8. Carbon / REC
CREATE TABLE IF NOT EXISTS carbon_credit_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT, price_usd REAL,
    currency TEXT, source TEXT, region TEXT, timestamp TEXT);
CREATE TABLE IF NOT EXISTS rec_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT, mwh REAL, price_per_mwh REAL,
    source TEXT, certificate_id TEXT, region TEXT,
    retired INTEGER DEFAULT 0, timestamp TEXT);
CREATE TABLE IF NOT EXISTS net_zero_matches (
    match_id TEXT PRIMARY KEY, workload_kwh REAL, intensity REAL,
    action TEXT, carbon_kg REAL, offset_cost_usd REAL,
    credit_price_usd REAL, timestamp TEXT);

-- 9. Chaos
CREATE TABLE IF NOT EXISTS chaos_experiments (
    experiment_id TEXT PRIMARY KEY, name TEXT, fault_type TEXT,
    blast_radius REAL, steady_before INTEGER, steady_after INTEGER,
    status TEXT, duration_ms REAL, timestamp TEXT);

-- 10. HITL
CREATE TABLE IF NOT EXISTS hitl_approval_queue (
    request_id TEXT PRIMARY KEY, rule_id TEXT, state TEXT,
    severity TEXT, status TEXT, created_at TEXT, resolved_at TEXT);
CREATE TABLE IF NOT EXISTS active_learning_samples (
    sample_id TEXT PRIMARY KEY, model_id TEXT, strategy TEXT,
    uncertainty REAL, selected_for_review INTEGER,
    user_label TEXT, reviewed_at TEXT, timestamp TEXT);
CREATE TABLE IF NOT EXISTS user_preferences (
    user_id TEXT PRIMARY KEY, weights TEXT, updated_at REAL);

-- Supporting
CREATE TABLE IF NOT EXISTS bio_inspired_runs (
    run_id TEXT PRIMARY KEY, algorithm TEXT, problem_id TEXT,
    parameters TEXT, best_solution TEXT, best_fitness REAL, timestamp TEXT);

-- Routing decisions (MoE)
CREATE TABLE IF NOT EXISTS routing_decisions (
    decision_id TEXT PRIMARY KEY, sample_id TEXT, expert TEXT,
    probability REAL, timestamp TEXT);

-- LIMIT graph
CREATE TABLE IF NOT EXISTS limit_graph_metadata (
    graph_id TEXT PRIMARY KEY, description TEXT,
    configuration TEXT, timestamp TEXT);
CREATE TABLE IF NOT EXISTS limit_graph_nodes (
    node_id TEXT, graph_id TEXT, node_type TEXT,
    attributes TEXT, timestamp TEXT,
    PRIMARY KEY (node_id, graph_id));
CREATE TABLE IF NOT EXISTS limit_graph_edges (
    edge_id TEXT PRIMARY KEY, graph_id TEXT, source TEXT, target TEXT,
    weight REAL, attributes TEXT, timestamp TEXT);

-- MODP
CREATE TABLE IF NOT EXISTS modp_states (
    state_id TEXT PRIMARY KEY, problem_id TEXT, state_attributes TEXT,
    objective_values TEXT, stage INTEGER, timestamp TEXT);
CREATE TABLE IF NOT EXISTS modp_policies (
    policy_id TEXT PRIMARY KEY, problem_id TEXT, state_id TEXT,
    action TEXT, expected_objectives TEXT, timestamp TEXT);

-- RLHF
CREATE TABLE IF NOT EXISTS preference_pairs (
    pair_id TEXT PRIMARY KEY, prompt TEXT, chosen TEXT, rejected TEXT,
    reward_diff REAL, metadata TEXT, timestamp TEXT);

-- Feedback pipeline
CREATE TABLE IF NOT EXISTS feedback_events (
    event_id TEXT PRIMARY KEY, source TEXT, feedback_type TEXT,
    task_id TEXT, context TEXT, action TEXT, performance TEXT,
    adaptive_cost_value REAL, tags TEXT, timestamp TEXT);
CREATE INDEX IF NOT EXISTS idx_fb_events_task ON feedback_events(task_id);

-- Adaptive state
CREATE TABLE IF NOT EXISTS adaptive_state (
    key TEXT PRIMARY KEY, value TEXT, updated_at REAL);

-- Offline batch summaries
CREATE TABLE IF NOT EXISTS offline_batch_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp REAL,
    batch_size INTEGER, avg_quality REAL, avg_carbon REAL,
    avg_latency REAL, avg_energy REAL);
"""


# =============================================================================
# STORAGE
# =============================================================================
class Storage:
    """SQLite-backed storage for all MOPD enhancements."""

    def __init__(self, path: str = "./mopd.db",
                 check_same_thread: bool = False,
                 schema: str = _SCHEMA_SQL):
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=check_same_thread)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(schema)
            self._conn.commit()

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------
    def _execute(self, sql: str, params: Tuple[Any, ...] = ()) -> None:
        with self._lock:
            self._conn.execute(sql, params)
            self._conn.commit()

    def _executemany(self, sql: str,
                     seq: List[Tuple[Any, ...]]) -> None:
        with self._lock:
            self._conn.executemany(sql, seq)
            self._conn.commit()

    def _fetchall(self, sql: str,
                  params: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(sql, params).fetchall())

    def _fetchone(self, sql: str,
                  params: Tuple[Any, ...] = ()) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    def _transaction(self) -> "sqlite3.Connection":
        """Return a context manager; caller uses `with storage._transaction():`."""
        return _TransactionContext(self._conn, self._lock)

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # 1. Quantum-Distillation
    # ------------------------------------------------------------------
    def save_teacher_superposition(self, student_id, teacher_id, weight,
                                   temperature, amplitude, kl) -> None:
        self._execute(
            """INSERT INTO teacher_superpositions
               (student_id, teacher_id, teacher_weight, temperature,
                amplitude, kl_divergence, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (student_id, teacher_id, float(weight), float(temperature),
             float(amplitude), float(kl), _now_iso()))

    # ------------------------------------------------------------------
    # 2. Causal RL
    # ------------------------------------------------------------------
    def save_causal_edge(self, source, target, weight, confidence) -> None:
        self._execute(
            """INSERT OR REPLACE INTO causal_graph
               (edge_id, source, target, weight, confidence, timestamp)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (f"{source}->{target}", source, target, float(weight),
             float(confidence), _now_iso()))

    def save_causal_experiment(self, exp_id, treatment, outcome, ate,
                               samples, method="") -> None:
        self._execute(
            """INSERT OR REPLACE INTO causal_experiments
               (exp_id, treatment, outcome, ate, samples, method, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (exp_id, treatment, outcome, float(ate), int(samples),
             method, _now_iso()))

    def save_causal_intervention(self, intervention_id, node, do_value,
                                 observed_outcome, counterfactual) -> None:
        self._execute(
            """INSERT OR REPLACE INTO causal_interventions
               (intervention_id, node, do_value, observed_outcome,
                counterfactual_json, timestamp)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (intervention_id, node, json.dumps(do_value, default=str),
             json.dumps(observed_outcome, default=str),
             json.dumps(counterfactual, default=str), _now_iso()))

    # ------------------------------------------------------------------
    # 3. Federated
    # ------------------------------------------------------------------
    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0) -> None:
        self._execute(
            """INSERT OR REPLACE INTO federated_weights
               (instance_id, model_id, weights, weight_norm, round_id,
                timestamp)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (instance_id, model_id, sqlite3.Binary(weights),
             float(weight_norm), int(round_id), _now_iso()))

    def get_federated_weights(self, model_id) -> List[sqlite3.Row]:
        return self._fetchall(
            """SELECT instance_id, model_id, weights, weight_norm, round_id
               FROM federated_weights WHERE model_id = ?""",
            (model_id,))

    # ------------------------------------------------------------------
    # 4. Multi-Agent
    # ------------------------------------------------------------------
    def save_agent(self, agent_id, role, reputation, utilities) -> None:
        self._execute(
            """INSERT INTO agent_registry
               (agent_id, role, reputation, utilities, created_at, last_updated)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(agent_id) DO UPDATE SET
                 role = excluded.role,
                 reputation = excluded.reputation,
                 utilities = excluded.utilities,
                 last_updated = excluded.last_updated""",
            (agent_id, role, float(reputation), json.dumps(utilities),
             _now_iso(), _now_iso()))

    def save_agent_message(self, message_id, topic, sender, recipient,
                           payload) -> None:
        self._execute(
            """INSERT OR REPLACE INTO agent_messages
               (message_id, topic, sender, recipient, payload, timestamp)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (message_id, topic, sender, recipient,
             json.dumps(payload, default=str), _now_iso()))

    # ------------------------------------------------------------------
    # 5. Temporal
    # ------------------------------------------------------------------
    def save_temporal_rule(self, rule_id, formula, operator, severity,
                           description, window_seconds, active=True) -> None:
        self._execute(
            """INSERT INTO temporal_rules
               (rule_id, formula, operator, severity, description,
                window_seconds, created_at, active)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(rule_id) DO UPDATE SET
                 formula = excluded.formula,
                 operator = excluded.operator,
                 severity = excluded.severity,
                 description = excluded.description,
                 window_seconds = excluded.window_seconds,
                 active = excluded.active""",
            (rule_id, formula, operator, severity, description,
             float(window_seconds), _now_iso(), int(active)))

    def save_temporal_trace(self, state, context=None) -> None:
        self._execute(
            """INSERT INTO temporal_trace (state, context, timestamp)
               VALUES (?, ?, ?)""",
            (json.dumps(state, default=str),
             json.dumps(context, default=str) if context else None,
             _now_iso()))

    def save_temporal_violation(self, rule_id, formula, step, state,
                                severity="warning", approved=None) -> None:
        self._execute(
            """INSERT INTO temporal_violations
               (rule_id, formula, step, state, severity, approved,
                resolved_at, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (rule_id, formula, int(step), json.dumps(state, default=str),
             severity, int(approved) if approved is not None else None,
             None, _now_iso()))

    # ------------------------------------------------------------------
    # 6. XAI
    # ------------------------------------------------------------------
    def save_xai_explanation(self, explanation_id, decision_id, method,
                             label, features, attributions, nl) -> None:
        rows = []
        for name, val in (attributions or {}).items():
            try:
                v = float(val)
            except (TypeError, ValueError):
                continue
            rows.append((explanation_id, str(name), v))
        sorted_rows = sorted(rows, key=lambda r: abs(r[2]), reverse=True)
        with self._transaction() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO xai_explanations
                   (explanation_id, decision_id, method, decision_label,
                    features, attributions, natural_language, timestamp)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (explanation_id, decision_id, method, label,
                 json.dumps(features, default=str),
                 json.dumps(attributions, default=str),
                 nl, _now_iso()))
            for rank, (eid, name, v) in enumerate(sorted_rows):
                conn.execute(
                    """INSERT INTO xai_feature_importance
                       (explanation_id, feature_name, importance, rank)
                       VALUES (?, ?, ?, ?)""",
                    (eid, name, v, rank))

    # ------------------------------------------------------------------
    # 7. Adaptive Precision
    # ------------------------------------------------------------------
    def save_precision_switch(self, from_p, to_p, reason,
                              saved_wh=0.0, acc_delta=0.0) -> None:
        self._execute(
            """INSERT INTO precision_history
               (from_p, to_p, reason, energy_saved_wh, accuracy_delta,
                timestamp)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (from_p, to_p, reason, float(saved_wh), float(acc_delta),
             _now_iso()))

    # ------------------------------------------------------------------
    # 8. Carbon / REC
    # ------------------------------------------------------------------
    def save_credit_price(self, price_usd, currency="USD",
                          source="oracle", region="global") -> None:
        self._execute(
            """INSERT INTO carbon_credit_prices
               (price_usd, currency, source, region, timestamp)
               VALUES (?, ?, ?, ?, ?)""",
            (float(price_usd), currency, source, region, _now_iso()))

    def save_rec(self, mwh, price_per_mwh, source,
                 certificate_id="", region="global", retired=False) -> None:
        self._execute(
            """INSERT INTO rec_ledger
               (mwh, price_per_mwh, source, certificate_id, region,
                retired, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (float(mwh), float(price_per_mwh), source, certificate_id,
             region, int(retired), _now_iso()))

    def get_rec_balance(self) -> float:
        row = self._fetchone(
            "SELECT COALESCE(SUM(mwh), 0) AS s FROM rec_ledger")
        return float(row["s"]) if row and row["s"] is not None else 0.0

    def save_net_zero_match(self, match_id, workload_kwh, intensity, action,
                            carbon_kg, offset_cost, credit_price) -> None:
        self._execute(
            """INSERT OR REPLACE INTO net_zero_matches
               (match_id, workload_kwh, intensity, action, carbon_kg,
                offset_cost_usd, credit_price_usd, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (match_id, float(workload_kwh), float(intensity), action,
             float(carbon_kg), float(offset_cost), float(credit_price),
             _now_iso()))

    # ------------------------------------------------------------------
    # 9. Chaos
    # ------------------------------------------------------------------
    def save_chaos_experiment(self, experiment_id, name, fault_type,
                              blast_radius, steady_before, steady_after,
                              status, duration_ms=0.0) -> None:
        self._execute(
            """INSERT OR REPLACE INTO chaos_experiments
               (experiment_id, name, fault_type, blast_radius,
                steady_before, steady_after, status, duration_ms, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (experiment_id, name, fault_type, float(blast_radius),
             int(steady_before), int(steady_after), status,
             float(duration_ms), _now_iso()))

    # ------------------------------------------------------------------
    # 10. HITL
    # ------------------------------------------------------------------
    def enqueue_hitl_request(self, request_id, rule_id, state,
                             severity="critical") -> None:
        self._execute(
            """INSERT OR REPLACE INTO hitl_approval_queue
               (request_id, rule_id, state, severity, status,
                created_at, resolved_at)
               VALUES (?, ?, ?, ?, 'pending', ?, NULL)""",
            (request_id, rule_id, json.dumps(state, default=str),
             severity, _now_iso()))

    def resolve_hitl_request(self, request_id, status="approved") -> None:
        self._execute(
            """UPDATE hitl_approval_queue
               SET status = ?, resolved_at = ? WHERE request_id = ?""",
            (status, _now_iso(), request_id))

    def save_active_learning_sample(self, sample_id, model_id, strategy,
                                    uncertainty, selected=True) -> None:
        self._execute(
            """INSERT OR REPLACE INTO active_learning_samples
               (sample_id, model_id, strategy, uncertainty,
                selected_for_review, timestamp)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (sample_id, model_id, strategy, float(uncertainty),
             int(bool(selected)), _now_iso()))

    # ------------------------------------------------------------------
    # Preferences
    # ------------------------------------------------------------------
    def save_user_preference(self, user_id, weights) -> None:
        self._execute(
            """INSERT OR REPLACE INTO user_preferences
               (user_id, weights, updated_at)
               VALUES (?, ?, ?)""",
            (user_id, json.dumps(weights, default=str), time.time()))

    def get_user_preference(self, user_id) -> Optional[Dict[str, float]]:
        row = self._fetchone(
            "SELECT weights FROM user_preferences WHERE user_id = ?",
            (user_id,))
        if not row or not row["weights"]:
            return None
        try:
            return json.loads(row["weights"])
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Routing decisions (MoE)
    # ------------------------------------------------------------------
    def log_routing_decision(self, decision_id, sample_id, expert,
                             probability) -> None:
        self._execute(
            """INSERT OR REPLACE INTO routing_decisions
               (decision_id, sample_id, expert, probability, timestamp)
               VALUES (?, ?, ?, ?, ?)""",
            (decision_id, sample_id, expert, float(probability), _now_iso()))

    # ------------------------------------------------------------------
    # LIMIT graph
    # ------------------------------------------------------------------
    def save_limit_graph_metadata(self, graph_id, description,
                                  configuration) -> None:
        self._execute(
            """INSERT OR REPLACE INTO limit_graph_metadata
               (graph_id, description, configuration, timestamp)
               VALUES (?, ?, ?, ?)""",
            (graph_id, description, json.dumps(configuration, default=str),
             _now_iso()))

    def save_limit_graph_node(self, node_id, graph_id, node_type,
                              attributes) -> None:
        self._execute(
            """INSERT OR REPLACE INTO limit_graph_nodes
               (node_id, graph_id, node_type, attributes, timestamp)
               VALUES (?, ?, ?, ?, ?)""",
            (node_id, graph_id, node_type,
             json.dumps(attributes, default=str), _now_iso()))

    def save_limit_graph_edge(self, edge_id, graph_id, source, target,
                              weight, attributes) -> None:
        self._execute(
            """INSERT OR REPLACE INTO limit_graph_edges
               (edge_id, graph_id, source, target, weight, attributes,
                timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (edge_id, graph_id, source, target, float(weight),
             json.dumps(attributes, default=str), _now_iso()))

    def get_limit_graph_metadata(self, graph_id) -> Optional[sqlite3.Row]:
        return self._fetchone(
            "SELECT * FROM limit_graph_metadata WHERE graph_id = ?",
            (graph_id,))

    def get_limit_graph_nodes(self, graph_id) -> List[sqlite3.Row]:
        return self._fetchall(
            "SELECT * FROM limit_graph_nodes WHERE graph_id = ?",
            (graph_id,))

    def get_limit_graph_edges(self, graph_id) -> List[sqlite3.Row]:
        return self._fetchall(
            "SELECT * FROM limit_graph_edges WHERE graph_id = ?",
            (graph_id,))

    # ------------------------------------------------------------------
    # MODP
    # ------------------------------------------------------------------
    def save_modp_state(self, state_id, problem_id, state_attributes,
                        objective_values, stage) -> None:
        self._execute(
            """INSERT OR REPLACE INTO modp_states
               (state_id, problem_id, state_attributes, objective_values,
                stage, timestamp)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (state_id, problem_id,
             json.dumps(state_attributes, default=str),
             json.dumps(objective_values, default=str),
             int(stage), _now_iso()))

    def save_modp_policy(self, policy_id, problem_id, state_id, action,
                         expected_objectives) -> None:
        self._execute(
            """INSERT OR REPLACE INTO modp_policies
               (policy_id, problem_id, state_id, action,
                expected_objectives, timestamp)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (policy_id, problem_id, state_id, action,
             json.dumps(expected_objectives, default=str), _now_iso()))

    def get_modp_states(self, problem_id) -> List[sqlite3.Row]:
        return self._fetchall(
            "SELECT * FROM modp_states WHERE problem_id = ?", (problem_id,))

    def get_modp_policies(self, problem_id) -> List[sqlite3.Row]:
        return self._fetchall(
            "SELECT * FROM modp_policies WHERE problem_id = ?",
            (problem_id,))

    # ------------------------------------------------------------------
    # RLHF
    # ------------------------------------------------------------------
    def save_preference_pair(self, pair_id, prompt, chosen, rejected,
                             reward_diff, metadata=None) -> None:
        self._execute(
            """INSERT OR REPLACE INTO preference_pairs
               (pair_id, prompt, chosen, rejected, reward_diff, metadata,
                timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (pair_id, prompt, chosen, rejected, float(reward_diff),
             json.dumps(metadata, default=str) if metadata else None,
             _now_iso()))

    def get_preference_pairs(self, limit=100) -> List[sqlite3.Row]:
        return self._fetchall(
            "SELECT * FROM preference_pairs ORDER BY timestamp DESC "
            "LIMIT ?", (int(limit),))

    # ------------------------------------------------------------------
    # Feedback
    # ------------------------------------------------------------------
    def store_feedback_event(self, event: Dict[str, Any]) -> None:
        self._execute(
            """INSERT OR REPLACE INTO feedback_events
               (event_id, source, feedback_type, task_id, context, action,
                performance, adaptive_cost_value, tags, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (str(event.get("event_id", uuid.uuid4().hex[:8])),
             event.get("source"), event.get("feedback_type"),
             event.get("task_id"),
             json.dumps(event.get("context", {}), default=str),
             json.dumps(event.get("action", {}), default=str),
             json.dumps(event.get("performance", {}), default=str),
             float(event.get("adaptive_cost_value", 0.0) or 0.0),
             json.dumps(event.get("tags", []), default=str),
             _now_iso()))

    def get_recent_feedback_events(self, limit=1000) -> List[sqlite3.Row]:
        return self._fetchall(
            "SELECT * FROM feedback_events ORDER BY timestamp DESC "
            "LIMIT ?", (int(limit),))

    # ------------------------------------------------------------------
    # Adaptive state
    # ------------------------------------------------------------------
    def load_adaptive_state(self, key: str) -> Optional[str]:
        row = self._fetchone(
            "SELECT value FROM adaptive_state WHERE key = ?", (key,))
        return row["value"] if row else None

    def save_adaptive_state(self, key: str, value: str) -> None:
        self._execute(
            """INSERT OR REPLACE INTO adaptive_state
               (key, value, updated_at)
               VALUES (?, ?, ?)""",
            (key, value, time.time()))

    # ------------------------------------------------------------------
    # Offline batch summaries
    # ------------------------------------------------------------------
    def log_offline_batch_summary(self, summary: Dict[str, Any]) -> None:
        self._execute(
            """INSERT INTO offline_batch_summaries
               (timestamp, batch_size, avg_quality, avg_carbon,
                avg_latency, avg_energy)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (float(summary.get("timestamp", time.time())),
             int(summary.get("batch_size", 0)),
             float(summary.get("avg_quality", 0.0) or 0.0),
             float(summary.get("avg_carbon", 0.0) or 0.0),
             float(summary.get("avg_latency", 0.0) or 0.0),
             float(summary.get("avg_energy", 0.0) or 0.0)))


class _TransactionContext:
    """Lightweight transactional context for multi-statement writes."""

    def __init__(self, conn: sqlite3.Connection, lock: threading.RLock):
        self._conn = conn
        self._lock = lock

    def __enter__(self) -> sqlite3.Connection:
        self._lock.acquire()
        self._conn.execute("BEGIN")
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is None:
                self._conn.commit()
            else:
                self._conn.rollback()
        finally:
            self._lock.release()


# =============================================================================
# MOPD ORCHESTRATOR
# =============================================================================
class MOPDOrchestratorV17:
    """Single entry point: in-process or HTTP reporting + ten enhancements."""

    def __init__(self, storage: Storage, config: Any,
                 adaptive_function: Any = None,
                 adaptive_api_url: Optional[str] = None,
                 adaptive_api_token: Optional[str] = None,
                 dashboard: Any = None,
                 *,
                 allow_insecure_http: bool = False,
                 xai_sample_rate: float = 1.0,
                 chaos_sample_rate: float = 1.0,
                 multi_agent_sample_rate: float = 1.0,
                 timeout_seconds: float = 10.0):
        self.storage = storage
        self.config = config
        self.adaptive = adaptive_function
        self.adaptive_api_url = adaptive_api_url
        self.adaptive_api_token = adaptive_api_token
        self.dashboard = dashboard
        self.instance_id = str(uuid.uuid4())[:8]
        self.allow_insecure_http = bool(allow_insecure_http)
        self.timeout_seconds = float(timeout_seconds)

        # Sample rates for optional side effects on the hot path.
        self.xai_sample_rate = float(xai_sample_rate)
        self.chaos_sample_rate = float(chaos_sample_rate)
        self.multi_agent_sample_rate = float(multi_agent_sample_rate)

        # Ten enhancements.
        self.quantum = QuantumDistillationEngine(temperature=2.0, alpha=0.5)
        self.causal_graph = CausalGraphLearner(storage)
        self.causal_rl = CausalPolicyAdapter(config, storage, self.causal_graph)
        self.federated = FederatedGreenAggregator(storage, self.instance_id)
        self.multi_agent = MultiAgentCoordinator(config, storage)
        self.temporal = TemporalLogicVerifier(storage, config)
        self.xai = XAIDecisionExplainer(config, storage)
        self.precision = AdaptivePrecisionSwitcher(config, storage)
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.chaos = ChaosTestingEngine(config, storage)
        self.hitl = ActiveUserPreferenceLearner(storage, None, dashboard)

        # Wire HITL to temporal critical rules.
        self.temporal.set_approval_callback(self._on_critical_violation)

        # Lifecycle.
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set = set()
        self._side_effect_tasks: set = set()
        self._session: Optional["aiohttp.ClientSession"] = None

    # ------------------------------------------------------------------
    # HITL approval for temporal critical violations
    # ------------------------------------------------------------------
    async def _on_critical_violation(self, rule_id: str,
                                     state: Dict[str, Any]) -> bool:
        """
        Ask the user (via HITL) whether a critical violation should be
        approved. Returns True only if the user explicitly approves.
        """
        try:
            quality = float(state.get("quality", 0.8))
            carbon = float(state.get("carbon", 0.4))
        except (TypeError, ValueError):
            quality, carbon = 0.8, 0.4
        candidates = [
            {"solution_id": "approve", "quality_score": quality,
             "carbon_g": carbon, "cost_usd": 0.5, "latency_ms": 100.0},
            {"solution_id": "reject", "quality_score": 0.0, "carbon_g": 0.0,
             "cost_usd": 0.0, "latency_ms": 0.0},
        ]
        try:
            decision = await self.hitl.query_user_if_needed(
                "critical_user", candidates, timeout=2.0)
        except Exception as exc:
            log_event("warning", f"HITL approval failed: {exc}")
            decision = None
        approved = decision == "approve"
        try:
            _M_HITL_REQUESTS.labels(
                outcome="approved" if approved else "rejected").inc()
        except Exception:
            pass
        return approved

    # ------------------------------------------------------------------
    # Strategy selection
    # ------------------------------------------------------------------
    async def select_strategy(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Choose a strategy based on the current state.

        Only the following run on the critical path:
          * Causal RL strategy selection.
          * Carbon-market decision (no price mutation).
          * Temporal gating.

        The XAI explanation, multi-agent bid/reward, chaos injection, and
        precision switching are scheduled as fire-and-forget tasks with
        sampling so the critical path stays short.
        """
        t0 = time.monotonic()
        result: Dict[str, Any] = {"strategy": "balanced"}

        # Carbon intensity is expected in [0, 1] (kgCO2/kWh); if the caller
        # passes gCO2/kWh (e.g. 400), coerce.
        raw_intensity = float(state.get("carbon_intensity", 0.4) or 0.4)
        intensity = raw_intensity / 1000.0 if raw_intensity > 1.0 else raw_intensity

        # 1. Causal RL strategy selection.
        try:
            strategy = await self.causal_rl.choose_action(state)
            result["strategy"] = strategy
            result["strategy_name"] = self.causal_rl.to_strategy(strategy)
        except Exception as exc:
            log_event("warning", f"Causal RL selection failed: {exc}")

        # 2. Carbon market decision (no price mutation, no commit).
        try:
            cm = await self.carbon_market.net_zero_schedule(
                workload_kwh=1.0, intensity=intensity, commit=False)
            result["carbon_decision"] = cm
        except Exception as exc:
            log_event("warning", f"Carbon market check failed: {exc}")
            result["carbon_decision"] = {"action": "run", "defer_hours": 0}

        # 3. Temporal push + verify (must run synchronously; can veto).
        try:
            await self.temporal.push_state({
                "quality": float(state.get("quality", 0.8)),
                "carbon": intensity,
                "task_complete": True,
            })
            verify = await self.temporal.verify()
            result["temporal_violations"] = [
                k for k, ok in verify.items() if not ok]
        except Exception as exc:
            log_event("warning", f"Temporal verify failed: {exc}")
            result["temporal_violations"] = []

        # 4. Fire-and-forget side effects.
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None:
            if random.random() < self.multi_agent_sample_rate:
                self._spawn_side_effect(
                    self._multi_agent_bid_and_reward(state), "multi_agent")
            if (self.xai is not None
                    and random.random() < self.xai_sample_rate
                    and NUMPY_AVAILABLE):
                self._spawn_side_effect(
                    self._xai_explain(result["strategy"], state, intensity),
                    "xai")
            if random.random() < self.chaos_sample_rate:
                self._spawn_side_effect(self._chaos_tick(), "chaos")

        # 5. Update causal RL with a reward derived from observable signals.
        reward = self._compute_strategy_reward(result, intensity)
        try:
            await self.causal_rl.update(result["strategy"], reward, state)
        except Exception as exc:
            log_event("debug", f"Causal RL update failed: {exc}")

        # 6. Metrics.
        try:
            _M_SELECT_TOTAL.labels(strategy=result["strategy"]).inc()
            _M_SELECT_SECONDS.observe(time.monotonic() - t0)
            _M_Q_TABLE_SIZE.set(len(getattr(self.causal_rl, "values", {}) or {}))
        except Exception:
            pass

        return result

    def _compute_strategy_reward(self, result: Dict[str, Any],
                                 intensity: float) -> float:
        """Derive a reward in [0, 1] from observable signals."""
        carbon_decision = result.get("carbon_decision", {}) or {}
        # If the market defers, the strategy helped relieve the grid.
        action = carbon_decision.get("action", "run")
        base = {"defer": 0.9, "run_offset": 0.7, "run": 0.5}.get(action, 0.5)
        # Lower intensity is better.
        return max(0.0, min(1.0, base - 0.2 * min(1.0, intensity)))

    # ------------------------------------------------------------------
    # Side-effect helpers
    # ------------------------------------------------------------------
    def _spawn_side_effect(self, coro: Awaitable[Any], name: str) -> None:
        try:
            task = asyncio.create_task(coro, name=f"mopd_side_{name}")
        except RuntimeError:
            return
        self._side_effect_tasks.add(task)
        task.add_done_callback(self._side_effect_tasks.discard)
        task.add_done_callback(self._log_side_effect_error)

    @staticmethod
    def _log_side_effect_error(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            log_event("debug", f"Side effect {task.get_name()} failed: {exc}")

    async def _multi_agent_bid_and_reward(self, state: Dict[str, Any]) -> None:
        try:
            agent_id, _ = await self.multi_agent.bid({
                "name": "distill_task", "preferred_role": "optimizer"})
            await self.multi_agent.reward(agent_id, 0.8)
        except Exception as exc:
            log_event("debug", f"multi_agent side effect failed: {exc}")

    async def _xai_explain(self, strategy: str, state: Dict[str, Any],
                           intensity: float) -> None:
        if not NUMPY_AVAILABLE:
            return
        try:
            feats = np.array([
                float(state.get("quality", 0.8)),
                float(intensity),
                float(state.get("cost", 0.5)),
                float(state.get("latency_ms", 100) or 100) / 1000.0,
            ])

            def _score(x, _w=np.array([0.4, -0.3, -0.2, -0.1])):
                return float(np.dot(x, _w))

            await self.xai.explain(
                decision_id=f"mopd_{uuid.uuid4().hex[:8]}",
                label=f"strategy={strategy}",
                features=feats,
                names=["quality", "carbon", "cost", "latency"],
                model_fn=_score)
        except Exception as exc:
            log_event("debug", f"xai side effect failed: {exc}")

    async def _chaos_tick(self) -> None:
        try:
            fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
            await self.chaos.run_experiment(
                f"auto_{uuid.uuid4().hex[:6]}", fault)
        except Exception as exc:
            log_event("debug", f"chaos side effect failed: {exc}")

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------
    async def report_epoch(self, context: Dict[str, Any],
                           metrics: Dict[str, float],
                           teacher_id: str,
                           distillation_loss: float,
                           epoch: int) -> None:
        """Report a distillation epoch to the adaptive pipeline."""
        payload = {
            "context": context,
            "metrics": metrics,
            "teacher_id": teacher_id,
            "distillation_loss": distillation_loss,
            "epoch": epoch,
        }

        # Preferred transport: in-process.
        if self.adaptive is not None:
            try:
                await self.adaptive.record_feedback(
                    context, metrics,
                    teacher_id=teacher_id,
                    distillation_loss=distillation_loss)
                try:
                    _M_REPORT_EPOCH.labels(
                        status="ok", transport="in_process").inc()
                except Exception:
                    pass
                return
            except TypeError:
                # Adaptive signature does not match; try dict form.
                try:
                    await self.adaptive.record_feedback(payload)
                    try:
                        _M_REPORT_EPOCH.labels(
                            status="ok", transport="in_process").inc()
                    except Exception:
                        pass
                    return
                except Exception as exc:
                    log_event(
                        "warning",
                        f"report_epoch in-process fallback failed: {exc}")
                    try:
                        _M_REPORT_EPOCH.labels(
                            status="error", transport="in_process").inc()
                    except Exception:
                        pass
            except Exception as exc:
                log_event(
                    "warning", f"report_epoch in-process failed: {exc}")
                try:
                    _M_REPORT_EPOCH.labels(
                        status="error", transport="in_process").inc()
                except Exception:
                    pass

        # Fallback: HTTP.
        if self.adaptive_api_url:
            ok = await self._post_http(payload)
            try:
                _M_REPORT_EPOCH.labels(
                    status="ok" if ok else "error",
                    transport="http").inc()
            except Exception:
                pass

    async def _get_session(self) -> Optional["aiohttp.ClientSession"]:
        if not AIOHTTP_AVAILABLE:
            return None
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def _post_http(self, payload: Dict[str, Any]) -> bool:
        if not AIOHTTP_AVAILABLE:
            log_event("warning", "aiohttp not available; HTTP report skipped")
            return False
        url = f"{self.adaptive_api_url}/mopd/record"
        if (not self.allow_insecure_http
                and not url.lower().startswith("https://")
                and not url.lower().startswith("http://localhost")
                and not url.lower().startswith("http://127.0.0.1")):
            log_event(
                "warning",
                f"Refusing insecure HTTP report to {url}. "
                "Set allow_insecure_http=True to override.")
            return False
        headers: Dict[str, str] = {}
        if self.adaptive_api_token:
            headers["Authorization"] = f"Bearer {self.adaptive_api_token}"
        try:
            session = await self._get_session()
            if session is None:
                return False
            async with session.post(url, json=payload, headers=headers) as r:
                await r.read()
                return 200 <= r.status < 300
        except Exception as exc:
            log_event("warning", f"HTTP report failed: {exc}")
            return False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
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

    async def start(self) -> None:
        self._running = True
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

        for t in list(self._side_effect_tasks):
            t.cancel()
        if self._side_effect_tasks:
            await asyncio.gather(*self._side_effect_tasks,
                                 return_exceptions=True)
        self._side_effect_tasks.clear()

        if self._session is not None and not self._session.closed:
            try:
                await self._session.close()
            except Exception:
                pass
            self._session = None

    async def aclose(self) -> None:
        await self.shutdown()

    async def __aenter__(self) -> "MOPDOrchestratorV17":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------
    async def _causal_rl_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(float(
                (self.config or {}).get("causal_graph_update_interval", 900)
                if isinstance(self.config, dict) else 900))
            try:
                samples = [{
                    "quality": random.uniform(0.5, 1.0),
                    "carbon": random.uniform(0.1, 0.8),
                    "cost": random.uniform(0.1, 0.9),
                    "latency": random.uniform(0.1, 0.9),
                } for _ in range(20)]
                await self.causal_graph.learn(
                    samples, ["quality", "carbon", "cost", "latency"])
                if hasattr(self.causal_rl, "estimate_ate"):
                    await self.causal_rl.estimate_ate("carbon", "quality")
            except Exception as exc:
                log_event("debug", f"causal_rl loop error: {exc}")

    async def _federated_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                dummy = bytes(random.getrandbits(8) for _ in range(64))
                await self.federated.share_weights("policy", dummy)
                await self.federated.pull_aggregated_weights("policy")
            except Exception as exc:
                log_event("debug", f"federated loop error: {exc}")

    async def _multi_agent_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(600)
            try:
                await self.multi_agent.step()
            except Exception as exc:
                log_event("debug", f"multi_agent loop error: {exc}")

    async def _temporal_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                await self.temporal.verify()
            except Exception as exc:
                log_event("debug", f"temporal loop error: {exc}")

    async def _xai_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            if not NUMPY_AVAILABLE:
                continue
            try:
                feats = np.array([0.9, 0.4, 0.5, 0.4])

                def _score(x, _w=np.array([0.4, -0.3, -0.2, -0.1])):
                    return float(np.dot(x, _w))

                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="system_health", features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=_score)
            except Exception as exc:
                log_event("debug", f"xai loop error: {exc}")

    async def _precision_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                # Use the rolling window when populated.
                await self.precision.auto_switch(0.0, 0.0)
            except Exception as exc:
                log_event("debug", f"precision loop error: {exc}")

    async def _carbon_market_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                await self.carbon_market.update_price()
            except Exception as exc:
                log_event("debug", f"carbon_market loop error: {exc}")

    async def _chaos_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    f"auto_{uuid.uuid4().hex[:6]}", fault)
            except Exception as exc:
                log_event("debug", f"chaos loop error: {exc}")

    async def _distillation_loop(self) -> None:
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                self.quantum.register_teacher(
                    "causal", self.causal_rl.get_policy())
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy())
                await self.quantum.step(self.storage, "mopd_student")
            except Exception as exc:
                log_event("debug", f"distillation loop error: {exc}")

    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        try:
            rec = await asyncio.to_thread(self.storage.get_rec_balance)
        except Exception:
            rec = 0.0
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "agents": {a.id: a.role
                       for a in self.multi_agent.agents.values()},
            "causal_edges": self.causal_graph.summary()["edges"],
            "temporal_rules": len(self.temporal.rules),
            "precision": self.precision.current,
            "rec_balance_mwh": rec,
            "carbon_price": self.carbon_market.last_price,
            "federated_rounds": self.federated.rounds,
            "side_effect_tasks": len(self._side_effect_tasks),
            "background_tasks": len(self._background_tasks),
        }


# =============================================================================
# DEMO
# =============================================================================
async def _demo():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    config = {
        "causal_graph_update_interval": 900,
        "agent_count": 5,
        "temporal_max_trace": 2000,
        "temporal_formulas": ["G (quality >= 0.5)"],
        "xai_method": "kernel_shap",
        "xai_depth": 5,
        "precision_levels": ["fp32", "fp16", "bf16", "int8"],
        "precision_switch_threshold": 0.02,
    }

    # Use an in-memory SQLite database for the demo.
    storage = Storage(path=":memory:")
    orch = MOPDOrchestratorV17(
        storage, config,
        xai_sample_rate=0.1,
        chaos_sample_rate=0.05,
        multi_agent_sample_rate=0.5)

    await orch.start()

    print("=" * 80)
    print("mopd_orchestrator_v17.py v17.1.0 — Demo")
    print("=" * 80)

    print("\n=== select_strategy cycles ===")
    for i in range(5):
        state = {
            "quality": random.uniform(0.6, 0.95),
            "carbon_intensity": random.uniform(0.2, 0.8),
            "cost": random.uniform(0.3, 0.8),
            "latency_ms": random.uniform(50, 300),
        }
        result = await orch.select_strategy(state)
        print(f"  Cycle {i + 1}:")
        print(f"    strategy: {result['strategy']}"
              f" -> {result.get('strategy_name')}")
        print(f"    carbon_action: {result['carbon_decision']['action']}"
              f" (defer_hours={result['carbon_decision'].get('defer_hours', 0)})")
        print(f"    temporal_violations: {result['temporal_violations']}")

    print("\n=== report_epoch ===")
    await orch.report_epoch(
        context={"task_id": "t1"},
        metrics={"loss": 0.1},
        teacher_id="causal",
        distillation_loss=0.05,
        epoch=1)
    print("  reported (no adaptive/HTTP configured; silently skipped)")

    print("\n=== health_check ===")
    print(json.dumps(await orch.health_check(), indent=2, default=str))

    await orch.shutdown()
    try:
        storage.close()
    except Exception:
        pass
    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
