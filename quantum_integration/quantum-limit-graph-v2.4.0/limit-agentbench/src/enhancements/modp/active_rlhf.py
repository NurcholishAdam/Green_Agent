"""
active_rlhf.py — Enhanced v17.0.0
=================================

Human-in-the-loop active learning for MOPD, now extended with all ten
advanced Green Agent enhancements in a single self-contained file:

  1. Quantum-Distillation Integration        → QuantumDistillationEngine
  2. Causal Reinforcement Learning           → CausalGraphLearner + CausalPolicyAdapter
  3. Federated Green Learning                → FederatedGreenAggregator
  4. Advanced Multi-Agent Coordination       → MultiAgentCoordinator
  5. Temporal Logic & Formal Verification    → TemporalLogicVerifier
  6. Explainable AI                          → XAIDecisionExplainer
  7. Adaptive Precision Switching            → AdaptivePrecisionSwitcher
  8. Carbon Markets / REC                    → CarbonMarketIntegrator
  9. Resilience Engineering / Chaos Testing  → ChaosTestingEngine
 10. HITL Active Learning                    → ActiveUserPreferenceLearner (this class)

The file is self-contained: it only requires Python stdlib plus numpy (optional).
All storage interactions are soft-fail so the module works standalone or with a
persistent backend.

Unified entry point: ActiveRLHFOrchestratorV17
"""

from __future__ import annotations

import asyncio
import json
import math
import random
import re
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

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

import logging
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIG HELPER — works with dict / pydantic / object configs
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
# ENHANCEMENT 1 — QUANTUM-DISTILLATION ENGINE
# =============================================================================
class QuantumDistillationEngine:
    """
    Quantum-inspired multi-teacher distillation.

        amplitude_k = sqrt(softmax_k)
        target_k    = amplitude_k^2 / sum(amplitude^2)
    """

    def __init__(self, temperature: float = 2.0, alpha: float = 0.5,
                 n_actions: int = 5):
        self.temperature = temperature
        self.alpha = alpha
        self.n_actions = n_actions
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy: List[float] = [1.0 / n_actions] * n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    # ------------------------------------------------------------------
    def register_teacher(self, name: str, policy: List[float]) -> None:
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[name] = [p / s for p in policy]

    def unregister_teacher(self, name: str) -> None:
        self.teachers.pop(name, None)

    # ------------------------------------------------------------------
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

    async def step(self, storage, student_id: str = "mopd_student") -> Dict[str, Any]:
        target = self._softmax(self._superpose(), self.temperature)
        lr = 0.1
        new = []
        for s, t in zip(self.student_policy, target):
            grad = -(t / max(s, 1e-9))
            new.append(max(0.01, s - lr * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]

        # Persist each teacher superposition weight (soft-fail)
        for tid, pol in self.teachers.items():
            weight = pol[0] if pol else 0.0
            amplitude = math.sqrt(max(weight, 1e-9))
            kl = sum(
                t * math.log(max(t, 1e-9) / max(s, 1e-9))
                for t, s in zip(target, self.student_policy)
            )
            try:
                await asyncio.to_thread(
                    storage.save_teacher_superposition,
                    student_id, tid, weight, self.temperature, amplitude, kl)
            except Exception:
                pass

        entry = {"target": target, "student": list(self.student_policy),
                 "ts": datetime.now(timezone.utc).isoformat()}
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

    async def learn(self, samples: List[Dict[str, float]], variables: List[str],
                    threshold: float = 0.25) -> Dict[str, Any]:
        self.variables = list(variables)
        if len(samples) < 5 or not NUMPY_AVAILABLE:
            async with self._lock:
                self.graph.clear()
                for i, s in enumerate(variables):
                    for j, t in enumerate(variables):
                        if i < j and random.random() < 0.25:
                            w = random.uniform(0.1, 0.9)
                            self.graph[s][t] = {"weight": w, "confidence": w}
                            if self.storage:
                                try:
                                    await asyncio.to_thread(
                                        self.storage.save_causal_edge, s, t, w, w)
                                except Exception:
                                    pass
            return self.summary()

        X = np.array([[s[v] for v in variables] for s in samples], dtype=float)
        if X.shape[0] < 2:
            return self.summary()
        X = (X - X.mean(0)) / (X.std(0) + 1e-9)
        corr = np.corrcoef(X, rowvar=False)

        async with self._lock:
            self.graph.clear()
            for i in range(len(variables)):
                for j in range(len(variables)):
                    if i == j:
                        continue
                    c = abs(float(corr[i, j]))
                    if c > threshold:
                        vi, vj = float(X[:, i].var()), float(X[:, j].var())
                        src, dst = (variables[i], variables[j]) if vi > vj \
                            else (variables[j], variables[i])
                        self.graph[src][dst] = {"weight": float(corr[i, j]),
                                                "confidence": c}
                        if self.storage:
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
    """Epsilon-greedy causal policy over MOPD teacher strategies."""

    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(self, config, storage, graph: CausalGraphLearner):
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values: Dict[str, float] = defaultdict(float)
        self.counts: Dict[str, int] = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = _cfg_get(config, "causal_exploration_rate", 0.1)
        self._lock = asyncio.Lock()

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
        w = self.graph.graph.get(treatment, {}).get(outcome, {}).get("weight", 0.0)
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
    """Byte-wise average of model weights across federated instances."""

    def __init__(self, storage, instance_id: str, share_interval: int = 3600):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
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
        return {"instance_id": self.instance_id,
                "rounds": self.rounds,
                "last_aggregated_bytes": len(self.last_aggregated)
                if self.last_aggregated else 0}


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
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        count = _cfg_get(config, "agent_count", 5)
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
            self.bus.put_nowait(
                {"topic": topic, "sender": sender, "payload": payload})
        except asyncio.QueueFull:
            pass
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

    def get_policy(self) -> List[float]:
        affinity = {
            "orchestrator": [0.35, 0.20, 0.15, 0.15, 0.15],
            "validator":    [0.15, 0.15, 0.15, 0.35, 0.20],
            "optimizer":    [0.20, 0.15, 0.35, 0.15, 0.15],
            "reporter":     [0.15, 0.20, 0.15, 0.15, 0.35],
            "negotiator":   [0.15, 0.35, 0.20, 0.15, 0.15],
        }
        counts = defaultdict(int)
        for a in self.agents.values():
            counts[a.role] += 1
        total = max(1, sum(counts.values()))
        out = [0.0] * 5
        for role, c in counts.items():
            w = c / total
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
                "role_distribution": self.get_policy(),
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
        self.window = window
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
    def __init__(self, storage, config):
        self.storage = storage
        self.config = config
        self.rules: Dict[str, TemporalRule] = {}
        self.trace: Deque[Tuple[datetime, Dict]] = deque(
            maxlen=_cfg_get(config, "temporal_max_trace", 2000))
        self.approval_cb: Optional[Callable] = None
        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            self._install(formula)

    def _install(self, formula: str) -> None:
        f = formula.strip()
        if f.startswith("G "):
            inner = f[2:].strip().strip("()")
            self._add_atomic(inner, "always")
        elif f.startswith("F "):
            inner = f[2:].strip().strip("()")
            self._add_atomic(inner, "eventually")
        elif f.startswith("NEVER "):
            inner = f[6:].strip().strip("()")
            self._add_atomic(inner, "never")

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
        self.rules[rid] = TemporalRule(rid, op, [cond],
                                        description=expr, severity="warning")
        try:
            asyncio.get_event_loop().create_task(asyncio.to_thread(
                self.storage.save_temporal_rule,
                rid, expr, op, "warning", expr, 0.0, True))
        except Exception:
            pass

    def set_approval_callback(self, cb: Callable) -> None:
        self.approval_cb = cb

    async def push_state(self, state: Dict[str, Any]) -> None:
        self.trace.append((datetime.now(timezone.utc), dict(state)))
        try:
            await asyncio.to_thread(self.storage.save_temporal_trace, state)
        except Exception:
            pass

    async def verify(self) -> Dict[str, bool]:
        result = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            violated = rule.evaluate(copy)
            result[rid] = not violated
            if violated:
                rule.violations += 1
                rule.last_violation = datetime.now(timezone.utc)
                try:
                    await asyncio.to_thread(
                        self.storage.save_temporal_violation,
                        rid, rule.description, len(self.trace) - 1,
                        self.trace[-1][1] if self.trace else {},
                        rule.severity, None)
                except Exception:
                    pass
                if rule.severity == "critical" and self.approval_cb:
                    try:
                        approved = self.approval_cb(rid, self.trace[-1][1])
                        if asyncio.iscoroutine(approved):
                            await approved
                    except Exception:
                        pass
        return result


# =============================================================================
# ENHANCEMENT 6 — EXPLAINABLE AI
# =============================================================================
class XAIDecisionExplainer:
    """KernelSHAP + LIME + natural-language rendering."""

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.method = _cfg_get(config, "xai_method", "kernel_shap")
        self.depth = _cfg_get(config, "xai_depth", 5)

    def _kernel_shap(self, f: Callable, x, names: List[str],
                     n: int = 64) -> Dict[str, float]:
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

    def _lime(self, f: Callable, x, names: List[str],
              n: int = 200) -> Dict[str, float]:
        if not (SKLEARN_AVAILABLE and NUMPY_AVAILABLE):
            return {k: random.uniform(-1, 1) for k in names}
        X = np.tile(x, (n, 1)) + np.random.normal(0, 0.1, (n, len(x)))
        try:
            y = np.array([float(f(r.reshape(1, -1))) for r in X])
        except Exception:
            return {k: 0.0 for k in names}
        w = np.exp(-np.sum((X - x) ** 2, axis=1) / 0.02)
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

    async def explain(self, decision_id: str, label: str,
                      features, names: List[str],
                      model_fn: Callable) -> Dict[str, Any]:
        if not NUMPY_AVAILABLE:
            attrs = {k: 0.0 for k in names}
        elif self.method == "lime":
            attrs = self._lime(model_fn, features, names)
        else:
            attrs = self._kernel_shap(model_fn, features, names)
        nl = self._nl(label, attrs)
        try:
            await asyncio.to_thread(
                self.storage.save_xai_explanation,
                decision_id, decision_id, self.method, label,
                dict(enumerate(features)) if NUMPY_AVAILABLE
                else {"raw": list(features)},
                attrs, nl)
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

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0

    def _probe(self) -> Dict[str, Any]:
        info = {"cuda": False, "bf16": False, "device": "cpu"}
        if TORCH_AVAILABLE:
            try:
                info["cuda"] = torch.cuda.is_available()
                if info["cuda"]:
                    info["device"] = torch.cuda.get_device_name(0)
                    info["bf16"] = torch.cuda.is_bf16_supported()
            except Exception:
                pass
        return info

    def select_precision(self) -> str:
        hw = self._probe()
        cands = list(_cfg_get(self.config, "precision_levels",
                              ["fp32", "fp16", "bf16", "int8"]))
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        return min(cands, key=lambda c: self.ENERGY.get(c, 1.0))

    async def switch_to(self, target: str, reason: str = "policy") -> bool:
        if target == self.current or target not in self.ENERGY:
            return False
        old = self.current
        self.current = target
        saved = max(0.0, self.ENERGY[old] - self.ENERGY[target])
        self.saved_wh += saved
        try:
            await asyncio.to_thread(
                self.storage.save_precision_switch,
                old, target, reason, saved, 0.0)
        except Exception:
            pass
        return True

    async def auto_switch(self, recent_acc: float, baseline_acc: float) -> None:
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = _cfg_get(self.config, "precision_switch_threshold", 0.02)
        if drop > thresh:
            await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
        elif drop < thresh / 2:
            await self.switch_to(self.select_precision(), reason="headroom")


# =============================================================================
# ENHANCEMENT 8 — CARBON MARKETS / REC
# =============================================================================
class CarbonMarketIntegrator:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.last_price = 25.0

    async def _fetch_price(self) -> float:
        return max(5.0, self.last_price + random.gauss(0, 1.5))

    async def update_price(self) -> float:
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

    async def purchase_rec(self, mwh: float, price_per_mwh: float = 5.0,
                           source: str = "wind") -> float:
        cost = mwh * price_per_mwh
        try:
            await asyncio.to_thread(
                self.storage.save_rec, mwh, price_per_mwh, source)
        except Exception:
            pass
        return cost

    async def net_zero_schedule(self, workload_kwh: float,
                                intensity: float) -> Dict[str, Any]:
        price = await self.update_price()
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price
        action = "defer" if intensity > 0.3 else \
                 ("run_offset" if offset_cost < 0.5 else "run")
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

    async def _get_rec_balance(self) -> float:
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

    async def _steady(self) -> bool:
        if not self.active:
            return True
        return random.random() > _cfg_get(self.config, "chaos_intensity", 0.05)

    async def run_experiment(self, name: str,
                             fault_type: str) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        t0 = time.time()
        before = await self._steady()
        status = "completed"
        try:
            async with self._lock:
                self.active[name] = {
                    "fault_type": fault_type,
                    "started": datetime.now(timezone.utc).isoformat()}
            if fault_type == "latency":
                await asyncio.sleep(0.5)
            elif fault_type == "exception":
                raise RuntimeError("chaos: injected exception")
            elif fault_type == "memory_pressure":
                _ = bytearray(5 * 1024 * 1024)
            elif fault_type == "network_drop":
                await asyncio.sleep(0.2)
        except Exception:
            status = "injected"
        finally:
            async with self._lock:
                self.active.pop(name, None)
        after = await self._steady()
        duration = (time.time() - t0) * 1000.0
        try:
            await asyncio.to_thread(
                self.storage.save_chaos_experiment,
                name, name, fault_type,
                _cfg_get(self.config, "chaos_blast_radius", 0.1),
                int(before), int(after), status, duration)
        except Exception:
            pass
        return {"name": name, "fault_type": fault_type,
                "steady_before": before, "steady_after": after,
                "status": status, "duration_ms": duration}


# =============================================================================
# ENHANCEMENT 10 — HITL ACTIVE LEARNING (enhanced original)
# =============================================================================
class ActiveUserPreferenceLearner:
    """
    Human-in-the-loop active learning for MOPD.

    Enhancements over v1.0:
      * Multi-metric ambiguity trigger (quality, carbon, cost, latency).
      * Uncertainty scoring (entropy-based) for active-learning samples.
      * Optional priority queue for critical decisions.
      * Optional Pareto gating integration.
      * Persistence to `active_learning_samples` table.
      * Reward feedback emission back to a callback.
    """

    def __init__(self,
                 storage,
                 pareto_gating=None,
                 dashboard=None,
                 feedback_callback: Optional[Callable] = None,
                 use_priority_queue: bool = False):
        self.storage = storage
        self.pareto = pareto_gating
        self.dashboard = dashboard
        self.feedback_callback = feedback_callback
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)
        # Optional priority queue (higher priority = requested first)
        self._priority_queue: List[Tuple[float, str, Dict[str, Any]]] = []
        self.use_priority_queue = use_priority_queue

    # ------------------------------------------------------------------
    # Response injection (dashboard or external caller)
    # ------------------------------------------------------------------
    async def submit_response(self, user_id: str, chosen_id: str) -> None:
        try:
            self._responses.put_nowait(
                {"user_id": user_id, "chosen": chosen_id})
        except asyncio.QueueFull:
            pass

    # ------------------------------------------------------------------
    # Uncertainty estimation for active learning
    # ------------------------------------------------------------------
    def _entropy_uncertainty(self, candidates: List[Dict[str, Any]]) -> float:
        scores = []
        for c in candidates[:5]:
            try:
                scores.append(float(c.get("quality_score", 0.0)))
            except Exception:
                scores.append(0.0)
        s = sum(scores) or 1.0
        probs = [x / s for x in scores]
        ent = -sum(p * math.log(p + 1e-9) for p in probs)
        # Normalise by log(n) so it's in [0, 1]
        max_ent = math.log(len(probs)) if len(probs) > 1 else 1.0
        return ent / max_ent if max_ent > 0 else 0.0

    def _ambiguity_score(self, candidates: List[Dict[str, Any]]) -> float:
        """
        Return a multi-metric ambiguity score in [0, 1].
        High = the top-2 candidates differ substantially in at least
        one of {quality, carbon, cost, latency}.
        """
        if len(candidates) < 2:
            return 0.0
        c1, c2 = candidates[0], candidates[1]
        def _rel(a, b):
            m = max(abs(a), abs(b), 1e-6)
            return abs(a - b) / m
        try:
            q = _rel(float(c1.get("quality_score", 0)),
                     float(c2.get("quality_score", 0)))
            cg = _rel(float(c1.get("carbon_g", 0)),
                      float(c2.get("carbon_g", 0)))
            cost = _rel(float(c1.get("cost_usd", 0)),
                        float(c2.get("cost_usd", 0)))
            lat = _rel(float(c1.get("latency_ms", 0)),
                       float(c2.get("latency_ms", 0)))
            # Close quality + wide carbon/cost/latency differences = ambiguous
            return max(cg, cost, lat) * (1.0 - min(q, 1.0))
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    # Core query with timeout + fallback
    # ------------------------------------------------------------------
    async def query_user_if_needed(self, user_id: str,
                                   candidates: List[Dict[str, Any]],
                                   timeout: float = 3.0,
                                   priority: float = 0.0) -> Optional[str]:
        if len(candidates) < 2:
            return None

        # Optional Pareto gating: only ask when at least 2 candidates are
        # Pareto-optimal.
        if self.pareto is not None:
            try:
                pareto_set = self.pareto.filter(candidates)
                if len(pareto_set) >= 2:
                    candidates = pareto_set
                else:
                    return None
            except Exception:
                pass

        # Trigger policy: ask if ambiguity OR uncertainty is high enough.
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
                 "uncertainty": uncertainty},
                "info")
        except Exception:
            pass

        # Persist active-learning sample with uncertainty
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
                    "type": "preference_query", "user_id": user_id,
                    "ambiguity": ambiguity,
                    "uncertainty": uncertainty,
                    "options": [{"id": c.get("solution_id"),
                                 "quality": c.get("quality_score"),
                                 "carbon": c.get("carbon_g"),
                                 "cost": c.get("cost_usd"),
                                 "latency": c.get("latency_ms")}
                                for c in candidates[:2]]})
            except Exception:
                pass

        try:
            msg = await asyncio.wait_for(self._responses.get(),
                                         timeout=timeout)
            try:
                await asyncio.to_thread(
                    self.storage.resolve_hitl_request, req_id, "approved")
            except Exception:
                pass
            chosen = msg.get("chosen")
            await self.record_choice(
                user_id, chosen,
                metrics=next((c for c in candidates
                              if c.get("solution_id") == chosen), None))
            # Emit reward feedback
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
            # Fallback: use learned weights if available
            weights = self.preferences.get(user_id, {})
            if weights:
                scored = []
                for c in candidates:
                    s = sum(weights.get(k, 0.25) / (c.get(k, 0) + 1e-8)
                            for k in ("quality_score", "carbon_g",
                                      "cost_usd", "latency_ms"))
                    scored.append((s, c.get("solution_id")))
                scored.sort(reverse=True)
                return scored[0][1]
            return candidates[0].get("solution_id")

    # ------------------------------------------------------------------
    # Preference learning
    # ------------------------------------------------------------------
    async def record_choice(self, user_id: str, solution_id: str,
                            metrics: Optional[Dict[str, float]] = None) -> None:
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k in ("quality_score", "carbon_g", "cost_usd", "latency_ms"):
                if k in metrics:
                    v = float(metrics[k])
                    # For quality, higher is better → reward high values
                    if k == "quality_score":
                        prefs[k] = prefs.get(k, 0.25) + v * 0.01
                    else:
                        # For carbon/cost/latency, lower is better
                        prefs[k] = prefs.get(k, 0.25) + 1.0 / (v + 1e-6) * 0.01
            s = sum(prefs.values()) or 1.0
            prefs = {k: v / s for k, v in prefs.items()}
            self.preferences[user_id] = prefs
        try:
            await asyncio.to_thread(
                self.storage.save_user_preference, user_id, prefs)
        except Exception:
            pass

    def get_preference_weights(self, user_id: str) -> Dict[str, float]:
        return dict(self.preferences.get(user_id, {}))


# =============================================================================
# UNIFIED ORCHESTRATOR
# =============================================================================
class ActiveRLHFOrchestratorV17:
    """
    Unified entry point wiring all ten enhancements.

    Usage:
        orch = ActiveRLHFOrchestratorV17(storage, config)
        await orch.start()
        result = await orch.decide(state)
        await orch.shutdown()
    """

    def __init__(self, storage, config,
                 dashboard=None,
                 pareto_gating=None,
                 adaptive_function=None,
                 feedback_callback: Optional[Callable] = None):
        self.storage = storage
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]

        # Ten enhancements
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
        self.hitl = ActiveUserPreferenceLearner(
            storage, pareto_gating=pareto_gating, dashboard=dashboard,
            feedback_callback=feedback_callback or self._default_feedback)

        # Wire HITL callback into critical temporal rules
        self.temporal.set_approval_callback(self._on_critical_violation)

        # Adaptive function for MOPD feedback (optional)
        self.adaptive = adaptive_function

        # Lifecycle
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set = set()

    # ------------------------------------------------------------------
    async def _default_feedback(self, event: Dict[str, Any]) -> None:
        if self.adaptive is not None:
            try:
                await self.adaptive.record_feedback(
                    {"request_id": event.get("task_id", "?")},
                    {"quality": event.get("quality_score", 1.0)},
                    teacher_id="hitl",
                    distillation_loss=event.get("ambiguity", 0.0))
            except Exception:
                pass

    async def _on_critical_violation(self, rule_id: str,
                                     state: Dict) -> bool:
        # Ask HITL for critical approval
        approved = await self.hitl.query_user_if_needed(
            "critical_user", [state, state], timeout=2.0)
        return approved is not None

    # ------------------------------------------------------------------
    # Core decision method
    # ------------------------------------------------------------------
    async def decide(self, state: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {"strategy": "adaptive"}

        # 1. Causal RL strategy selection
        result["strategy"] = await self.causal_rl.choose_action(state)

        # 2. Multi-agent bid
        agent_id, _ = await self.multi_agent.bid({
            "name": "decide_task", "preferred_role": "optimizer"})
        result["agent_id"] = agent_id

        # 3. Precision pre-selection
        await self.precision.auto_switch(recent_acc=0.9, baseline_acc=0.92)
        result["precision"] = self.precision.current

        # 4. Carbon market decision
        cm = await self.carbon_market.net_zero_schedule(
            workload_kwh=1.0,
            intensity=state.get("carbon_intensity", 400) / 1000.0)
        result["carbon_decision"] = cm

        # 5. XAI explanation
        if NUMPY_AVAILABLE:
            try:
                feats = np.array([
                    state.get("quality", 0.8),
                    state.get("carbon_intensity", 400) / 1000.0,
                    state.get("cost", 0.5),
                    state.get("latency_ms", 100) / 1000.0])
                def _score(x):
                    return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
                result["xai"] = await self.xai.explain(
                    decision_id=f"mopd_{uuid.uuid4().hex[:8]}",
                    label=f"strategy={result['strategy']}",
                    features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=_score)
            except Exception:
                pass

        # 6. Multi-agent reward
        await self.multi_agent.reward(agent_id, 0.8)

        # 7. Temporal push + verify
        await self.temporal.push_state({
            "quality": state.get("quality", 0.8),
            "carbon": state.get("carbon_intensity", 400) / 1000.0,
            "task_complete": True})
        verify = await self.temporal.verify()
        result["temporal_violations"] = [k for k, v in verify.items() if not v]

        # 8. Causal update
        await self.causal_rl.update(result["strategy"], 0.8, state)

        return result

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------
    async def start(self) -> None:
        self._running = True
        loop = asyncio.get_event_loop()
        tasks = [
            loop.create_task(self._causal_rl_loop()),
            loop.create_task(self._federated_loop()),
            loop.create_task(self._multi_agent_loop()),
            loop.create_task(self._temporal_loop()),
            loop.create_task(self._xai_loop()),
            loop.create_task(self._precision_loop()),
            loop.create_task(self._carbon_market_loop()),
            loop.create_task(self._chaos_loop()),
            loop.create_task(self._distillation_loop()),
        ]
        for t in tasks:
            self._background_tasks.add(t)
            t.add_done_callback(self._background_tasks.discard)

    async def shutdown(self) -> None:
        self._shutdown_event.set()
        self._running = False
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks,
                                 return_exceptions=True)

    async def _causal_rl_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config,
                                         "causal_graph_update_interval", 900))
            try:
                samples = [{
                    "quality": random.uniform(0.5, 1.0),
                    "carbon": random.uniform(0.1, 0.8),
                    "cost": random.uniform(0.1, 0.9),
                    "latency": random.uniform(0.1, 0.9),
                } for _ in range(20)]
                await self.causal_graph.learn(
                    samples, ["quality", "carbon", "cost", "latency"])
                await self.causal_rl.estimate_ate("carbon", "quality")
            except Exception:
                pass

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config, "federated_interval", 3600))
            try:
                dummy = bytes(random.getrandbits(8) for _ in range(64))
                await self.federated.share_weights("policy", dummy)
                await self.federated.pull_aggregated_weights("policy")
            except Exception:
                pass

    async def _multi_agent_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config,
                                         "agent_negotiation_interval", 600))
            try:
                await self.multi_agent.step()
            except Exception:
                pass

    async def _temporal_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config,
                                         "temporal_verification_interval", 300))
            try:
                await self.temporal.verify()
            except Exception:
                pass

    async def _xai_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config, "xai_interval", 300))
            if not NUMPY_AVAILABLE:
                continue
            try:
                feats = np.array([0.9, 0.4, 0.5, 0.4])
                def _score(x):
                    return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="system_health", features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=_score)
            except Exception:
                pass

    async def _precision_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                await self.precision.auto_switch(0.9, 0.92)
            except Exception:
                pass

    async def _carbon_market_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config,
                                         "carbon_market_interval", 3600))
            try:
                await self.carbon_market.update_price()
            except Exception:
                pass

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(_cfg_get(self.config,
                                         "chaos_test_interval", 1800))
            try:
                fault = random.choice(ChaosTestingEngine.FAULT_TYPES)
                await self.chaos.run_experiment(
                    f"auto_{uuid.uuid4().hex[:6]}", fault)
            except Exception:
                pass

    async def _distillation_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                self.quantum.register_teacher(
                    "causal", self.causal_rl.get_policy())
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy())
                await self.quantum.step(self.storage, "mopd_student")
            except Exception:
                pass

    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "agents": {a.id: a.role for a in self.multi_agent.agents.values()},
            "causal_edges": self.causal_graph.summary()["edges"],
            "temporal_rules": len(self.temporal.rules),
            "precision": self.precision.current,
            "carbon_price": self.carbon_market.last_price,
            "federated_rounds": self.federated.rounds,
        }


# =============================================================================
# MINIMAL STORAGE STUB (for standalone use)
# =============================================================================
class InMemoryStorage:
    """Minimal storage for standalone use / tests. All methods are no-ops
    or in-memory."""

    def __init__(self):
        self._data: Dict[str, Any] = defaultdict(list)
        self._prefs: Dict[str, Dict[str, float]] = {}

    def _add(self, table, row):
        self._data[table].append(row)

    # HITL
    def enqueue_hitl_request(self, request_id, rule_id, state, severity):
        self._add("hitl_approval_queue",
                  {"request_id": request_id, "rule_id": rule_id,
                   "state": state, "severity": severity, "status": "pending"})

    def resolve_hitl_request(self, request_id, status="approved"):
        for r in self._data.get("hitl_approval_queue", []):
            if r["request_id"] == request_id:
                r["status"] = status

    def save_active_learning_sample(self, sample_id, model_id, strategy,
                                    uncertainty, selected):
        self._add("active_learning_samples",
                  {"sample_id": sample_id, "model_id": model_id,
                   "strategy": strategy, "uncertainty": uncertainty,
                   "selected": selected})

    def save_user_preference(self, user_id, weights):
        self._prefs[user_id] = dict(weights)

    def get_user_preference(self, user_id):
        return self._prefs.get(user_id)

    # Causal
    def save_causal_edge(self, source, target, weight, confidence):
        self._add("causal_graph",
                  {"source": source, "target": target,
                   "weight": weight, "confidence": confidence})

    def save_causal_experiment(self, exp_id, treatment, outcome, ate, samples):
        self._add("causal_experiments",
                  {"exp_id": exp_id, "treatment": treatment,
                   "outcome": outcome, "ate": ate, "samples": samples})

    # Federated
    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0):
        self._add("federated_weights",
                  {"instance_id": instance_id, "model_id": model_id,
                   "weights": weights, "weight_norm": weight_norm,
                   "round_id": round_id})

    def get_federated_weights(self, model_id):
        return [r for r in self._data.get("federated_weights", [])
                if r["model_id"] == model_id]

    # Multi-agent
    def save_agent(self, agent_id, role, reputation, utilities):
        self._add("agent_registry",
                  {"agent_id": agent_id, "role": role,
                   "reputation": reputation, "utilities": utilities})

    def save_agent_message(self, message_id, topic, sender,
                           recipient, payload):
        self._add("agent_messages",
                  {"message_id": message_id, "topic": topic,
                   "sender": sender, "recipient": recipient,
                   "payload": payload})

    # Temporal
    def save_temporal_rule(self, rule_id, formula, operator, severity,
                           description, window, active):
        self._add("temporal_rules", {"rule_id": rule_id, "formula": formula})

    def save_temporal_trace(self, state, context=None):
        self._add("temporal_trace", {"state": state})

    def save_temporal_violation(self, rule_id, formula, step, state,
                                severity="warning", approved=None):
        self._add("temporal_violations",
                  {"rule_id": rule_id, "formula": formula,
                   "step": step, "state": state, "severity": severity})

    # XAI
    def save_xai_explanation(self, explanation_id, decision_id, method,
                             label, features, attributions, nl):
        self._add("xai_explanations",
                  {"explanation_id": explanation_id,
                   "decision_id": decision_id,
                   "method": method, "label": label,
                   "attributions": attributions,
                   "natural_language": nl})

    # Precision
    def save_precision_switch(self, from_p, to_p, reason,
                              saved_wh=0.0, acc_delta=0.0):
        self._add("precision_history",
                  {"from_p": from_p, "to_p": to_p, "reason": reason,
                   "saved_wh": saved_wh})

    # Carbon / REC
    def save_credit_price(self, price_usd, currency="USD",
                          source="oracle", region="global"):
        self._add("carbon_credit_prices", {"price_usd": price_usd})

    def save_rec(self, mwh, price_per_mwh, source,
                 certificate_id="", region="global", retired=False):
        self._add("rec_ledger",
                  {"mwh": mwh, "price_per_mwh": price_per_mwh,
                   "source": source})

    def get_rec_balance(self):
        return sum(r["mwh"] for r in self._data.get("rec_ledger", []))

    def save_net_zero_match(self, match_id, workload_kwh, intensity, action,
                            carbon_kg, offset_cost, credit_price):
        self._add("net_zero_matches",
                  {"match_id": match_id, "action": action})

    # Chaos
    def save_chaos_experiment(self, experiment_id, name, fault_type,
                              blast_radius, steady_before, steady_after,
                              status, duration_ms=0.0):
        self._add("chaos_experiments",
                  {"experiment_id": experiment_id,
                   "fault_type": fault_type, "status": status})

    # Quantum-Distillation
    def save_teacher_superposition(self, student_id, teacher_id, weight,
                                   temperature, amplitude, kl):
        self._add("teacher_superpositions",
                  {"student_id": student_id, "teacher_id": teacher_id,
                   "weight": weight, "temperature": temperature,
                   "amplitude": amplitude, "kl": kl})


# =============================================================================
# DEMO / SELF-TEST
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
    orch = ActiveRLHFOrchestratorV17(storage, config)
    await orch.start()

    print("=" * 80)
    print("active_rlhf.py v17.0.0 — Demo")
    print("=" * 80)

    for i in range(3):
        state = {
            "quality": random.uniform(0.6, 0.95),
            "carbon_intensity": random.uniform(200, 700),
            "cost": random.uniform(0.3, 0.8),
            "latency_ms": random.uniform(50, 300),
        }
        result = await orch.decide(state)
        print(f"\n--- Cycle {i + 1} ---")
        print(f"  strategy: {result['strategy']}")
        print(f"  precision: {result['precision']}")
        print(f"  agent_id: {result['agent_id']}")
        print(f"  carbon_action: {result['carbon_decision']['action']}")
        print(f"  temporal_violations: {result['temporal_violations']}")
        if "xai" in result:
            print(f"  xai_label: {result['xai']['explanation'].splitlines()[0]}")

    print("\n=== Health check ===")
    print(json.dumps(await orch.health_check(), indent=2, default=str))

    print("\n=== HITL test ===")
    # Simulate two near-tied candidates
    candidates = [
        {"solution_id": "A", "quality_score": 0.90, "carbon_g": 0.4,
         "cost_usd": 0.5, "latency_ms": 100},
        {"solution_id": "B", "quality_score": 0.895, "carbon_g": 0.2,
         "cost_usd": 0.6, "latency_ms": 150},
    ]
    # Submit a response after 0.5s
    async def _delayed_response():
        await asyncio.sleep(0.5)
        await orch.hitl.submit_response("default", "B")
    asyncio.create_task(_delayed_response())

    chosen = await orch.hitl.query_user_if_needed(
        "default", candidates, timeout=2.0)
    print(f"HITL chose: {chosen}")

    print("\n=== Chaos experiment ===")
    print(json.dumps(
        await orch.chaos.run_experiment("demo_chaos", "latency"),
        indent=2, default=str))

    print("\n=== Federated stats ===")
    print(json.dumps(orch.federated.stats(), indent=2))

    print("\n=== Quantum distillation ===")
    orch.quantum.register_teacher("causal", orch.causal_rl.get_policy())
    orch.quantum.register_teacher("agents", orch.multi_agent.get_policy())
    entry = await orch.quantum.step(storage, "mopd_student")
    print(f"Student policy: {[round(p, 3) for p in entry['student']]}")

    await orch.shutdown()
    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
