# src/carbon/carbon_ledger.py — Enhanced v18.0.0
# =============================================================================
# Extended Carbon Ledger with Helium Accounting — v18.0.0
# =============================================================================
"""
Extended Carbon Ledger — v18.0.0 (single-file build)
=====================================================

System of record for the Green Agent, hardened for production.

This file is **self-contained**: it carries the ten enhancement classes
that v17 duplicated across `carbon_forecast.py` and `carbon_ledger.py`,
so the drift that plagued v17 cannot recur.

P0 fixes
--------
* **Atomic, locked persistence** — tempfile + ``fsync`` + ``os.replace``,
  guarded by a dedicated ``_save_lock``.
* **Frozen entries** with a **``previous_hash`` chain**. Post-insert
  mutation is blocked; ``verify_integrity()`` walks the chain.
* **Stable hashes across save/reload** — timestamps, snapshots, and
  every field are canonicalised via ``_json_safe``; no ``nan``/``inf``
  ever reaches the file.
* **``_safe_mean``** everywhere a v17 ``statistics.mean`` could see an
  empty iterator.
* **HITL does not auto-approve** — ``hitl_status`` ∈
  ``{not_required, pending, approved, rejected, timed_out}``.
* **Per-call precision savings** — ``predicted_energy_saved_wh`` is a
  signed delta for the current task, not the cumulative total.

P1 fixes
--------
* **Filterable reports** — every ``get_*`` accepts ``since`` / ``until``;
  top-level ``filter(**kw)``, ``get_window()``, ``query_by_task()``,
  ``query_by_agent()``, ``iter_entries()``.
* **``_load_ledger`` validates** every field (coerces numerics, parses
  timestamps, migrates v1). Non-strict mode logs-and-skips.
* **``add_entry`` accepts dicts as well as objects.**
* **``asyncio.get_running_loop()``**; ``start()`` idempotent;
  ``_shutdown_event`` cleared on start.
* **``state_snapshot`` cannot override** the orchestrator's computed
  ``quality`` / ``carbon`` values.
* **Chaos respects ``chaos_intensity``** in the orchestrator.
* **Causal ATE** is observational, from ledger data, not the graph.
* **Bounded ``InMemoryStorage``**, snapshot size cap, PII redaction.

P2 fixes
--------
* All ten enhancement classes live here — single source of truth.
* ``Simple*`` aliases exported.
* ``register_enhancement`` is load-bearing via ``hook()`` / ``report_for()``.
* ``get_aggregated_metrics`` returns a precision *distribution*.
* ``get_hitl_impact`` distinguishes approved / rejected / timed_out /
  pending.
* ``CarbonLedgerOrchestratorV17`` aliased to ``...V18``.
* Schema version in the on-disk file (v1 files auto-migrate).

Backward compatibility
----------------------
* ``add_entry(unified_result, execution_decision, helium_signal)`` works
  with dicts *or* objects.
* ``add_entry_v17(record)`` unchanged; ``add_entry_v18`` aliases it.
* All ``get_*`` methods keep signatures; gain keyword-only ``since`` /
  ``until``.
* ``LedgerEntry`` keeps all v17 fields, gains ``previous_hash``,
  ``schema_version``, ``hitl_status``.

Self-contained: Python stdlib + optional numpy / sklearn / torch.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import re
import statistics
import tempfile
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
)

# --------------------------------------------------------------------------- #
# Optional dependencies
# --------------------------------------------------------------------------- #
try:
    import numpy as np  # type: ignore
    NUMPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    NUMPY_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression  # type: ignore
    SKLEARN_AVAILABLE = True
except ImportError:  # pragma: no cover
    SKLEARN_AVAILABLE = False

try:
    import torch  # type: ignore
    TORCH_AVAILABLE = True
except ImportError:  # pragma: no cover
    TORCH_AVAILABLE = False


logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
_MEMORY_SENTINEL = ":memory:"
_VALID_PRECISIONS = frozenset({"fp32", "tf32", "bf16", "fp16", "int8"})
_VALID_FAIL_MODES = ("silent", "log", "raise")
_VALID_HITL_STATUSES = frozenset(
    {"not_required", "pending", "approved", "rejected", "timed_out"}
)
_PII_PATTERN: Optional["re.Pattern[str]"] = None


# =========================================================================== #
# SHARED HELPERS
# =========================================================================== #
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_timestamp(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return _ensure_aware(raw)
    if isinstance(raw, str):
        s = raw.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return _ensure_aware(datetime.fromisoformat(s))
        except ValueError:
            return _utcnow()
    return _utcnow()


def _json_safe(obj: Any) -> Any:
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, datetime):
        return _ensure_aware(obj).isoformat()
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Mapping):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(v) for v in obj]
    return obj


def _cfg_get(config: Any, key: str, default: Any = None) -> Any:
    if config is None:
        return default
    if isinstance(config, Mapping):
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


def _safe_mean(
    values: Iterable[Optional[float]], default: float = 0.0
) -> float:
    """Mean of finite numeric values, ignoring ``None`` / NaN / inf."""
    items: List[float] = []
    for v in values:
        if v is None:
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(f):
            continue
        items.append(f)
    if not items:
        return default
    try:
        return statistics.mean(items)
    except statistics.StatisticsError:
        return default


def _safe_pstdev(
    values: Iterable[Optional[float]], default: float = 0.0
) -> float:
    items: List[float] = []
    for v in values:
        if v is None:
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if math.isfinite(f):
            items.append(f)
    if len(items) < 2:
        return default
    return statistics.pstdev(items)


def _redact(obj: Any) -> Any:
    """Recursively redact likely-PII keys from a nested structure."""
    global _PII_PATTERN
    if _PII_PATTERN is None:
        _PII_PATTERN = re.compile(
            r"(email|password|token|secret|ssn|phone|api_key|auth)",
            re.IGNORECASE,
        )
    if isinstance(obj, Mapping):
        return {
            k: ("[REDACTED]" if _PII_PATTERN.search(str(k)) else _redact(v))
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_redact(v) for v in obj]
    return obj


def _num_or_none(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _int_or_none(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# =========================================================================== #
# ENHANCEMENT 1 — Quantum-distillation (simplified reference)
# =========================================================================== #
class QuantumDistillationEngine:
    """
    **Simplified reference implementation** of teacher→student distillation.

    Teacher superposition uses Hellinger (√p) averaging; the student is
    updated by cross-entropy gradient on the softmaxed target. Not a
    quantum information-theoretic algorithm; demonstrates the integration
    shape.
    """

    def __init__(
        self,
        temperature: float = 2.0,
        alpha: float = 0.5,
        n_actions: int = 5,
        learning_rate: float = 0.1,
    ) -> None:
        self.temperature = temperature
        self.alpha = alpha
        self.n_actions = n_actions
        self.learning_rate = learning_rate
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy: List[float] = [1.0 / n_actions] * n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def register_teacher(self, name: str, policy: Sequence[float]) -> None:
        if not policy:
            return
        s = sum(policy) or 1.0
        self.teachers[name] = [p / s for p in policy]

    def _softmax(self, x: Sequence[float], temp: float) -> List[float]:
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

    async def step(
        self, storage: Any = None, student_id: str = "carbon_student"
    ) -> Dict[str, Any]:
        target = self._softmax(self._superpose(), self.temperature)
        new = []
        for s, t in zip(self.student_policy, target):
            eps = max(1e-12, min(self.student_policy) * 0.1)
            grad = -(t / max(s, eps))
            new.append(max(1e-3, s - self.learning_rate * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]
        entry = {
            "target": target,
            "student": list(self.student_policy),
            "ts": _utcnow().isoformat(),
        }
        self.history.append(entry)
        if storage is not None:
            for tid, pol in self.teachers.items():
                try:
                    await asyncio.to_thread(
                        storage.save_teacher_superposition,
                        student_id,
                        tid,
                        pol[0] if pol else 0.0,
                        self.temperature,
                        0.0,
                        0.0,
                    )
                except Exception:
                    pass
        return entry

    def get_policy(self) -> List[float]:
        return list(self.student_policy)


# =========================================================================== #
# ENHANCEMENT 2 — Causal RL (simplified reference)
# =========================================================================== #
class CausalGraphLearner:
    """
    **Simplified reference implementation**: correlation-thresholded graph
    with variance-based direction inference. Not causal inference.
    """

    def __init__(self, storage: Any = None) -> None:
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []

    async def learn(
        self,
        samples: Sequence[Mapping[str, float]],
        variables: Sequence[str],
        threshold: float = 0.25,
    ) -> Dict[str, Any]:
        self.variables = list(variables)
        if len(samples) < 5 or not NUMPY_AVAILABLE:
            self.graph.clear()
            for i, s in enumerate(variables):
                for j, t in enumerate(variables):
                    if i < j and random.random() < 0.25:
                        w = random.uniform(0.1, 0.9)
                        self.graph[s][t] = {"weight": w, "confidence": w}
            return self.summary()

        X = np.array(
            [[s[v] for v in variables] for s in samples], dtype=float
        )
        if X.shape[0] < 2:
            return self.summary()

        std = X.std(0)
        safe_std = np.where(std > 1e-9, std, 1.0)
        X = (X - X.mean(0)) / safe_std

        corr = np.corrcoef(X, rowvar=False)
        if corr.ndim == 0:
            return self.summary()

        self.graph.clear()
        for i in range(len(variables)):
            for j in range(len(variables)):
                if i == j:
                    continue
                c = abs(float(corr[i, j]))
                if c > threshold:
                    vi = float(X[:, i].var())
                    vj = float(X[:, j].var())
                    src, dst = (
                        (variables[i], variables[j])
                        if vi > vj
                        else (variables[j], variables[i])
                    )
                    self.graph[src][dst] = {
                        "weight": float(corr[i, j]),
                        "confidence": c,
                    }
                    if self.storage is not None:
                        try:
                            await asyncio.to_thread(
                                self.storage.save_causal_edge,
                                src,
                                dst,
                                float(corr[i, j]),
                                c,
                            )
                        except Exception:
                            pass
        return self.summary()

    def parents(self, node: str) -> List[str]:
        return [s for s, e in self.graph.items() if node in e]

    def summary(self) -> Dict[str, Any]:
        return {
            "nodes": len(self.variables),
            "edges": sum(len(v) for v in self.graph.values()),
            "variables": list(self.variables),
        }


class CausalPolicyAdapter:
    """**Simplified reference implementation**: ε-greedy + value update."""

    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(
        self, config: Any, storage: Any, graph: CausalGraphLearner
    ) -> None:
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values: Dict[str, float] = defaultdict(float)
        self.counts: Dict[str, int] = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = float(
            _cfg_get(config, "causal_exploration_rate", 0.1)
        )

    async def choose_action(self, state: Any) -> str:
        if random.random() < self.epsilon:
            return random.choice(self.ACTIONS)
        return max(self.ACTIONS, key=lambda a: self.values.get(a, 0.0))

    async def update(
        self, action: str, reward: float, state: Any
    ) -> None:
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

    async def estimate_ate(
        self, treatment: str, outcome: str, samples: int = 100
    ) -> float:
        w = (
            self.graph.graph.get(treatment, {})
            .get(outcome, {})
            .get("weight", 0.0)
        )
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_causal_experiment,
                    f"exp_{uuid.uuid4().hex[:8]}",
                    treatment,
                    outcome,
                    w,
                    samples,
                )
            except Exception:
                pass
        return w

    def get_policy(self) -> List[float]:
        return list(self.policy)


# =========================================================================== #
# ENHANCEMENT 3 — Federated green learning (simplified reference)
# =========================================================================== #
class FederatedGreenAggregator:
    """
    **Simplified reference implementation**: byte-wise averaging of opaque
    blobs. Not parameter-level federated averaging.
    """

    def __init__(
        self,
        storage: Any,
        instance_id: str,
        share_interval: float = 3600.0,
    ) -> None:
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.rounds = 0

    async def share_weights(self, model_id: str, weights: bytes) -> None:
        try:
            await asyncio.to_thread(
                self.storage.save_federated_weights,
                self.instance_id,
                model_id,
                weights,
                float(len(weights)),
                self.rounds,
            )
        except Exception:
            pass

    async def pull_aggregated_weights(
        self, model_id: str
    ) -> Optional[bytes]:
        try:
            rows = await asyncio.to_thread(
                self.storage.get_federated_weights, model_id
            )
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
        return bytes(avg)

    async def apply_aggregated_weights(
        self, model_id: str, current: bytes
    ) -> bytes:
        agg = await self.pull_aggregated_weights(model_id)
        if agg is None:
            return current
        n = min(len(current), len(agg))
        return bytes((current[i] + agg[i]) // 2 for i in range(n))


# =========================================================================== #
# ENHANCEMENT 4 — Multi-agent coordination (simplified reference)
# =========================================================================== #
class _Agent:
    ROLES = [
        "orchestrator",
        "validator",
        "optimizer",
        "reporter",
        "negotiator",
    ]

    def __init__(self, agent_id: str) -> None:
        self.id = agent_id
        self.role = "validator"
        self.reputation = 0.5
        self.utilities: Dict[str, float] = {
            r: random.uniform(0.3, 0.7) for r in self.ROLES
        }
        self.completed = 0


class MultiAgentCoordinator:
    """**Simplified reference implementation**: fixed pool, role = argmax(utility)."""

    def __init__(self, config: Any, storage: Any) -> None:
        self.config = config
        self.storage = storage
        count = int(_cfg_get(config, "agent_count", 5))
        if count <= 0:
            raise ValueError("agent_count must be > 0")
        self.agents: Dict[str, _Agent] = {
            f"agent_{i:02d}": _Agent(f"agent_{i:02d}") for i in range(count)
        }
        self.bus: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()

    async def _specialise(self) -> None:
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])

    async def broadcast(
        self, topic: str, sender: str, payload: Any
    ) -> None:
        try:
            self.bus.put_nowait(
                {"topic": topic, "sender": sender, "payload": payload}
            )
        except asyncio.QueueFull:
            pass

    async def bid(self, task: Mapping[str, Any]) -> Tuple[str, float]:
        preferred = task.get("preferred_role", "orchestrator")
        best_id: Optional[str] = None
        best_score = -1.0
        async with self._lock:
            for aid, a in self.agents.items():
                bonus = 1.0 if a.role == preferred else 0.6
                score = a.utilities[a.role] * bonus + 0.3 * a.reputation
                score += random.uniform(-0.02, 0.02)
                if score > best_score:
                    best_score, best_id = score, aid
            if best_id:
                self.agents[best_id].completed += 1
        return best_id or next(iter(self.agents)), best_score

    async def reward(self, agent_id: str, reward: float) -> None:
        async with self._lock:
            if agent_id in self.agents:
                a = self.agents[agent_id]
                n = max(1, a.completed)
                a.reputation = max(
                    0.0, min(1.0, a.reputation + reward / n)
                )
                a.utilities[a.role] = min(
                    1.0, a.utilities[a.role] + 0.05 * reward
                )

    def get_policy(self) -> List[float]:
        counts: Dict[str, int] = defaultdict(int)
        for a in self.agents.values():
            counts[a.role] += 1
        total = max(1, sum(counts.values()))
        out = [0.0] * 5
        for _role, c in counts.items():
            w = c / total
            for i in range(5):
                out[i] += w * 0.2
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
        return {
            "roles": {a.id: a.role for a in self.agents.values()},
            "role_distribution": self.get_policy(),
            "processed_messages": processed,
        }


# =========================================================================== #
# ENHANCEMENT 5 — Temporal logic (simplified reference)
# =========================================================================== #
_ATOMIC_RE = re.compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$"
)


class TemporalRule:
    def __init__(
        self,
        rule_id: str,
        operator: str,
        conditions: List[Callable[[Mapping[str, Any]], bool]],
        window: float = 0.0,
        description: str = "",
        severity: str = "warning",
    ) -> None:
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window = window
        self.description = description or rule_id
        self.severity = severity
        self.violations = 0
        self.last_violation: Optional[datetime] = None

    def evaluate(
        self, trace: Deque[Tuple[datetime, Dict[str, Any]]]
    ) -> bool:
        if not trace:
            return False
        if self.window > 0:
            cutoff = trace[-1][0] - timedelta(seconds=self.window)
            while trace and trace[0][0] < cutoff:
                trace.popleft()
        if self.operator == "always":
            return any(not self.conditions[0](s) for _, s in trace)
        if self.operator == "eventually":
            return not any(self.conditions[0](s) for _, s in trace)
        if self.operator == "never":
            return any(self.conditions[0](s) for _, s in trace)
        return False


class TemporalLogicVerifier:
    """
    **Simplified reference implementation**: bounded G / F / NEVER over
    atomic comparisons. Full LTL (U, &&, →) is out of scope.
    """

    def __init__(self, storage: Any, config: Any) -> None:
        self.storage = storage
        self.config = config
        maxlen = int(_cfg_get(config, "temporal_max_trace", 2000))
        self.trace: Deque[Tuple[datetime, Dict[str, Any]]] = deque(
            maxlen=maxlen
        )
        self.rules: Dict[str, TemporalRule] = {}
        self.approval_cb: Optional[Callable[..., Any]] = None
        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            self._install(formula)

    def _install(self, formula: str) -> None:
        f = formula.strip()
        if f.startswith("G "):
            self._add_atomic(f[2:].strip().strip("()"), "always")
        elif f.startswith("F "):
            self._add_atomic(f[2:].strip().strip("()"), "eventually")
        elif f.startswith("NEVER "):
            self._add_atomic(f[6:].strip().strip("()"), "never")

    def _add_atomic(self, expr: str, op: str) -> None:
        m = _ATOMIC_RE.match(expr)
        if not m:
            logger.warning("Ignoring malformed temporal formula: %r", expr)
            return
        var, cmp, val = m.group(1), m.group(2), float(m.group(3))

        def cond(
            state: Mapping[str, Any],
            v: str = var,
            c: str = cmp,
            x: float = val,
        ) -> bool:
            try:
                sv = float(state.get(v, 0.0))
            except Exception:
                return True
            return {
                ">=": sv >= x,
                "<=": sv <= x,
                "==": sv == x,
                "!=": sv != x,
                ">": sv > x,
                "<": sv < x,
            }[c]

        rid = f"{op}:{expr}"
        self.rules[rid] = TemporalRule(
            rid, op, [cond], description=expr, severity="warning"
        )

    def set_approval_callback(self, cb: Callable[..., Any]) -> None:
        self.approval_cb = cb

    async def push_state(self, state: Mapping[str, Any]) -> None:
        self.trace.append((_utcnow(), dict(state)))
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_temporal_trace, state
                )
            except Exception:
                pass

    async def verify(self) -> Dict[str, bool]:
        result: Dict[str, bool] = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            violated = rule.evaluate(copy)
            result[rid] = not violated
            if violated:
                rule.violations += 1
                rule.last_violation = _utcnow()
                if rule.severity == "critical" and self.approval_cb:
                    try:
                        approved = self.approval_cb(
                            rid, self.trace[-1][1]
                        )
                        if asyncio.iscoroutine(approved):
                            await approved
                    except Exception:
                        pass
        return result


# =========================================================================== #
# ENHANCEMENT 6 — XAI (simplified reference)
# =========================================================================== #
class XAIDecisionExplainer:
    """
    **Simplified reference implementation** of local feature attribution.

    * ``"permutation"``  — permutation importance vs. a zero-vector baseline.
    * ``"local_linear"`` — local linear surrogate with a Gaussian kernel.

    Neither is SHAP or LIME in the strict sense.
    """

    def __init__(self, config: Any, storage: Any) -> None:
        self.config = config
        self.storage = storage
        self.method = _cfg_get(config, "xai_method", "permutation")
        if self.method == "kernel_shap":
            self.method = "permutation"
        elif self.method == "lime":
            self.method = "local_linear"
        if self.method not in ("permutation", "local_linear"):
            raise ValueError(
                f"xai_method must be 'permutation' or 'local_linear', "
                f"got {self.method!r}"
            )
        self.depth = int(_cfg_get(config, "xai_depth", 5))
        self.global_attributions: Deque[Dict[str, float]] = deque(
            maxlen=1000
        )

    def _permutation_importance(
        self,
        f: Callable[[Any], float],
        x: Any,
        names: Sequence[str],
        n: int = 32,
    ) -> Dict[str, float]:
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
                    delta = float(f(cur.reshape(1, -1))) - float(
                        f(prev.reshape(1, -1))
                    )
                except Exception:
                    delta = 0.0
                contrib[i] += delta
                prev = cur
        contrib /= max(1, n)
        return dict(zip(names, contrib.tolist()))

    def _local_linear(
        self,
        f: Callable[[Any], float],
        x: Any,
        names: Sequence[str],
        n: int = 100,
    ) -> Dict[str, float]:
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

    def _nl(self, decision: str, attrs: Mapping[str, float]) -> str:
        top = sorted(
            attrs.items(), key=lambda kv: abs(kv[1]), reverse=True
        )[: self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(
        self,
        decision_id: str,
        label: str,
        features: Any,
        names: Sequence[str],
        model_fn: Callable[[Any], float],
        include_counterfactual: bool = False,
        include_anchors: bool = False,
    ) -> Dict[str, Any]:
        if not NUMPY_AVAILABLE:
            attrs = {k: 0.0 for k in names}
        else:
            x = np.asarray(features, dtype=float)
            if self.method == "local_linear":
                attrs = self._local_linear(model_fn, x, names)
            else:
                attrs = self._permutation_importance(model_fn, x, names)
        nl = self._nl(label, attrs)
        result = {
            "decision_id": decision_id,
            "method": self.method,
            "attributions": attrs,
            "explanation": nl,
            "counterfactual": None,
            "anchors": None,
        }
        self.global_attributions.append(dict(attrs))
        if self.storage is not None:
            try:
                features_json = _json_safe(
                    list(features)
                    if not isinstance(features, Mapping)
                    else features
                )
                await asyncio.to_thread(
                    self.storage.save_xai_explanation,
                    decision_id,
                    decision_id,
                    self.method,
                    label,
                    features_json,
                    attrs,
                    nl,
                )
            except Exception:
                pass
        return result

    def global_feature_importance(self) -> Dict[str, float]:
        if not self.global_attributions:
            return {}
        agg: Dict[str, List[float]] = defaultdict(list)
        for d in self.global_attributions:
            for k, v in d.items():
                agg[k].append(abs(v))
        return {k: _safe_mean(vals) for k, vals in agg.items()}


# =========================================================================== #
# ENHANCEMENT 7 — Adaptive precision switcher (simplified reference)
# =========================================================================== #
class AdaptivePrecisionSwitcher:
    """**Simplified reference implementation**: relative energy cost table."""

    ENERGY = {
        "fp32": 1.0,
        "tf32": 0.75,
        "bf16": 0.55,
        "fp16": 0.5,
        "int8": 0.3,
    }

    def __init__(self, config: Any, storage: Any) -> None:
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def _probe(self) -> Dict[str, Any]:
        info: Dict[str, Any] = {
            "cuda": False,
            "bf16": False,
            "device": "cpu",
        }
        if TORCH_AVAILABLE:
            try:
                info["cuda"] = bool(torch.cuda.is_available())
                if info["cuda"]:
                    info["device"] = torch.cuda.get_device_name(0)
                    info["bf16"] = bool(
                        torch.cuda.is_bf16_supported()
                    )
            except Exception:
                pass
        return info

    def select_precision(self) -> str:
        hw = self._probe()
        cands = list(
            _cfg_get(
                self.config,
                "precision_levels",
                ["fp32", "fp16", "bf16", "int8"],
            )
        )
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        if not cands:
            return "fp32"
        return min(cands, key=lambda c: self.ENERGY.get(c, 1.0))

    async def switch_to(
        self, target: str, reason: str = "policy"
    ) -> bool:
        if target == self.current or target not in self.ENERGY:
            return False
        old = self.current
        self.current = target
        saved = max(
            0.0,
            self.ENERGY.get(old, 1.0) - self.ENERGY.get(target, 1.0),
        )
        self.saved_wh += saved
        self.history.append(
            {
                "from": old,
                "to": target,
                "reason": reason,
                "saved_wh": saved,
            }
        )
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_precision_switch,
                    old,
                    target,
                    reason,
                    saved,
                    0.0,
                )
            except Exception:
                pass
        return True

    async def auto_switch(
        self, recent_acc: float, baseline_acc: float
    ) -> None:
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = float(
            _cfg_get(self.config, "precision_switch_threshold", 0.02)
        )
        if drop > thresh:
            await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
        elif drop < thresh / 2:
            await self.switch_to(
                self.select_precision(), reason="headroom"
            )


# =========================================================================== #
# ENHANCEMENT 8 — Carbon markets / REC (simplified reference)
# =========================================================================== #
class CarbonMarketIntegrator:
    """**Simplified reference implementation**: random-walk price + threshold policy."""

    def __init__(self, config: Any, storage: Any) -> None:
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
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_credit_price, price
                )
            except Exception:
                pass
        return price

    async def purchase_rec(
        self, mwh: float, price_per_mwh: float = 5.0, source: str = "wind"
    ) -> float:
        cost = mwh * price_per_mwh
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_rec, mwh, price_per_mwh, source
                )
            except Exception:
                pass
        return cost

    async def net_zero_schedule(
        self, workload_kwh: float, intensity: float
    ) -> Dict[str, Any]:
        price = await self.update_price()
        carbon_kg = workload_kwh * intensity
        offset_cost = (carbon_kg / 1000.0) * price
        action = (
            "defer"
            if intensity > 0.3
            else ("run_offset" if offset_cost < 0.5 else "run")
        )
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_net_zero_match,
                    uuid.uuid4().hex[:8],
                    workload_kwh,
                    intensity,
                    action,
                    carbon_kg,
                    offset_cost,
                    price,
                )
            except Exception:
                pass
        return {
            "action": action,
            "carbon_kg": carbon_kg,
            "offset_cost_usd": offset_cost,
            "credit_price_usd": price,
        }


# =========================================================================== #
# ENHANCEMENT 9 — Chaos testing (simplified reference)
# =========================================================================== #
class ChaosTestingEngine:
    """**Simplified reference implementation**: single-shot fault injection."""

    FAULT_TYPES = [
        "latency",
        "exception",
        "data_corruption",
        "memory_pressure",
        "network_drop",
    ]

    def __init__(self, config: Any, storage: Any) -> None:
        self.config = config
        self.storage = storage
        self.active: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def _steady(self) -> bool:
        if not self.active:
            return True
        return random.random() > float(
            _cfg_get(self.config, "chaos_intensity", 0.05)
        )

    async def run_experiment(
        self, name: str, fault_type: str
    ) -> Dict[str, Any]:
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        t0 = time.perf_counter()
        before = await self._steady()
        status = "completed"
        try:
            async with self._lock:
                self.active[name] = {"fault_type": fault_type}
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
        except Exception:
            status = "injected"
        finally:
            async with self._lock:
                self.active.pop(name, None)
        after = await self._steady()
        duration = (time.perf_counter() - t0) * 1000.0
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_chaos_experiment,
                    name,
                    name,
                    fault_type,
                    float(_cfg_get(self.config, "chaos_blast_radius", 0.1)),
                    int(before),
                    int(after),
                    status,
                    duration,
                )
            except Exception:
                pass
        return {
            "name": name,
            "fault_type": fault_type,
            "steady_before": before,
            "steady_after": after,
            "status": status,
            "duration_ms": duration,
        }


# =========================================================================== #
# ENHANCEMENT 10 — HITL active learning (simplified reference)
# =========================================================================== #
class ActiveUserPreferenceLearner:
    """
    **Simplified reference implementation**: queue-based preference
    elicitation. ``query_user_if_needed`` returns ``None`` on timeout —
    callers must treat that as "no human available", not auto-approve.
    """

    def __init__(self, storage: Any, dashboard: Any = None) -> None:
        self.storage = storage
        self.dashboard = dashboard
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue(
            maxsize=100
        )

    async def submit_response(
        self, user_id: str, chosen_id: str
    ) -> None:
        try:
            self._responses.put_nowait(
                {"user_id": user_id, "chosen": chosen_id}
            )
        except asyncio.QueueFull:
            pass

    async def query_user_if_needed(
        self,
        user_id: str,
        candidates: Sequence[Mapping[str, Any]],
        timeout: float = 3.0,
    ) -> Optional[str]:
        if len(candidates) < 2:
            return None
        try:
            msg = await asyncio.wait_for(
                self._responses.get(), timeout=timeout
            )
            return msg.get("chosen")
        except asyncio.TimeoutError:
            return None

    async def record_choice(
        self,
        user_id: str,
        solution_id: str,
        metrics: Optional[Mapping[str, float]] = None,
    ) -> None:
        prefs = self.preferences.setdefault(user_id, {})
        if metrics:
            for k in (
                "quality_score",
                "carbon_g",
                "cost_usd",
                "latency_ms",
            ):
                if k in metrics:
                    v = float(metrics[k])
                    if k == "quality_score":
                        prefs[k] = prefs.get(k, 0.25) + v * 0.01
                    else:
                        prefs[k] = (
                            prefs.get(k, 0.25) + 1.0 / (v + 1e-6) * 0.01
                        )
            s = sum(prefs.values()) or 1.0
            prefs = {k: v / s for k, v in prefs.items()}
            self.preferences[user_id] = prefs
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_user_preference, user_id, prefs
                )
            except Exception:
                pass


# --------------------------------------------------------------------------- #
# Honest aliases
# --------------------------------------------------------------------------- #
SimpleQuantumDistillationEngine = QuantumDistillationEngine
SimpleCausalGraphLearner = CausalGraphLearner
SimpleCausalPolicyAdapter = CausalPolicyAdapter
SimpleFederatedGreenAggregator = FederatedGreenAggregator
SimpleMultiAgentCoordinator = MultiAgentCoordinator
SimpleTemporalLogicVerifier = TemporalLogicVerifier
SimpleXAIDecisionExplainer = XAIDecisionExplainer
SimpleAdaptivePrecisionSwitcher = AdaptivePrecisionSwitcher
SimpleCarbonMarketIntegrator = CarbonMarketIntegrator
SimpleChaosTestingEngine = ChaosTestingEngine
SimpleActiveUserPreferenceLearner = ActiveUserPreferenceLearner


# =========================================================================== #
# LEDGER
# =========================================================================== #
class LedgerError(RuntimeError):
    """Raised for invalid configuration, malformed data, or I/O failure."""


@dataclass(frozen=True)
class LedgerEntry:
    """
    A single immutable entry in the extended carbon ledger.

    **Frozen**: field reassignment is blocked; list defaults are coerced to
    tuples in ``__post_init__``. Corrections must be recorded as new
    reversing entries.

    The hash chains to ``previous_hash``; ``verify_integrity()`` walks the
    chain and re-hashes each entry.
    """

    # --- v1.0 core ---
    timestamp: datetime = field(default_factory=_utcnow)
    task_id: str = ""
    energy_kwh: float = 0.0
    carbon_kg: float = 0.0
    helium_zone: Optional[str] = None
    helium_usage: float = 0.0
    helium_supply_at_execution: str = "unknown"
    helium_spot_price: float = 4.0
    hardware_type: str = "unknown"
    power_budget: float = 0.0
    fallback_used: bool = False

    # --- Enhancement 1 ---
    distillation_teacher_id: Optional[str] = None
    distillation_weight: Optional[float] = None

    # --- Enhancement 2 ---
    causal_action: Optional[str] = None
    causal_ate_estimate: Optional[float] = None
    causal_parents: Tuple[str, ...] = field(default_factory=tuple)

    # --- Enhancement 3 ---
    federated_instance_id: Optional[str] = None
    federated_round: Optional[int] = None

    # --- Enhancement 4 ---
    agent_id: Optional[str] = None
    agent_role: Optional[str] = None
    agent_reputation: Optional[float] = None

    # --- Enhancement 5 ---
    state_snapshot: Optional[Dict[str, Any]] = None
    temporal_violations: Tuple[str, ...] = field(default_factory=tuple)

    # --- Enhancement 6 ---
    xai_explanation: Optional[Dict[str, float]] = None
    xai_feature_importance: Optional[Dict[str, float]] = None

    # --- Enhancement 7 ---
    precision_level: str = "fp32"
    predicted_energy_saved_wh: Optional[float] = None

    # --- Enhancement 8 ---
    carbon_offset_kg: float = 0.0
    rec_mwh: float = 0.0
    offset_cost_usd: float = 0.0
    net_carbon_kg: Optional[float] = None

    # --- Enhancement 9 ---
    chaos_experiment_id: Optional[str] = None
    chaos_fault_type: Optional[str] = None

    # --- Enhancement 10 ---
    human_review_required: bool = False
    human_approved: Optional[bool] = None
    hitl_user_id: Optional[str] = None
    hitl_status: str = "not_required"

    # --- Provenance / integrity ---
    previous_hash: str = ""
    hash: str = ""
    schema_version: int = 2

    # ------------------------------------------------------------------
    def __post_init__(self) -> None:
        ts = self.timestamp
        if not isinstance(ts, datetime):
            ts = _parse_timestamp(ts)
        elif ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        object.__setattr__(self, "timestamp", ts)

        if not isinstance(self.causal_parents, tuple):
            object.__setattr__(
                self,
                "causal_parents",
                tuple(self.causal_parents or ()),
            )
        if not isinstance(self.temporal_violations, tuple):
            object.__setattr__(
                self,
                "temporal_violations",
                tuple(self.temporal_violations or ()),
            )

        if self.precision_level not in _VALID_PRECISIONS:
            object.__setattr__(self, "precision_level", "fp32")
        if self.hitl_status not in _VALID_HITL_STATUSES:
            object.__setattr__(self, "hitl_status", "not_required")

    # ------------------------------------------------------------------
    def _canonical_dict(self, *, include_hash: bool = False) -> Dict[str, Any]:
        d: Dict[str, Any] = {}
        for f in fields(self):
            if f.name == "hash" and not include_hash:
                continue
            v = getattr(self, f.name)
            if f.name == "timestamp":
                v = _ensure_aware(v).isoformat()
            elif isinstance(v, tuple):
                v = list(v)
            elif isinstance(v, (dict, list)):
                v = _json_safe(v)
            d[f.name] = v
        return d

    def compute_hash(self) -> str:
        canonical = json.dumps(
            self._canonical_dict(include_hash=False),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
            allow_nan=False,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(self._canonical_dict(include_hash=True))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "LedgerEntry":
        known = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in known}
        filtered.setdefault("timestamp", _utcnow())

        for fname in (
            "energy_kwh",
            "carbon_kg",
            "helium_usage",
            "helium_spot_price",
            "power_budget",
            "distillation_weight",
            "causal_ate_estimate",
            "agent_reputation",
            "predicted_energy_saved_wh",
            "carbon_offset_kg",
            "rec_mwh",
            "offset_cost_usd",
            "net_carbon_kg",
        ):
            if filtered.get(fname) is not None:
                try:
                    filtered[fname] = float(filtered[fname])
                except (TypeError, ValueError):
                    filtered[fname] = None

        if filtered.get("federated_round") is not None:
            try:
                filtered["federated_round"] = int(
                    filtered["federated_round"]
                )
            except (TypeError, ValueError):
                filtered["federated_round"] = None

        for fname in ("fallback_used", "human_review_required"):
            v = filtered.get(fname)
            if isinstance(v, str):
                filtered[fname] = v.lower() in ("true", "1", "yes")
            else:
                filtered[fname] = bool(v) if v is not None else False

        if filtered.get("human_approved") is not None:
            v = filtered["human_approved"]
            if isinstance(v, str):
                filtered["human_approved"] = v.lower() in (
                    "true",
                    "1",
                    "yes",
                )
            elif not isinstance(v, bool):
                filtered["human_approved"] = bool(v)

        return cls(**filtered)


# --------------------------------------------------------------------------- #
# Helper functions for the ledger
# --------------------------------------------------------------------------- #
def _agent_summary(
    agent_id: str, entries: List[LedgerEntry]
) -> Dict[str, Any]:
    if not entries:
        return {"agent_id": agent_id, "count": 0, "quality_score": 0.0}
    avg_carbon = _safe_mean(e.carbon_kg for e in entries)
    avg_energy = _safe_mean(e.energy_kwh for e in entries)
    fallback_rate = (
        sum(1 for e in entries if e.fallback_used) / len(entries)
    )
    quality = max(
        0.0,
        1.0
        - 0.5 * fallback_rate
        - 0.001 * avg_carbon
        - 0.01 * avg_energy,
    )
    return {
        "agent_id": agent_id,
        "count": len(entries),
        "avg_carbon_kg": avg_carbon,
        "avg_energy_kwh": avg_energy,
        "fallback_rate": fallback_rate,
        "quality_score": quality,
    }


def _rebuild_entry_with_chain(
    entry: LedgerEntry, previous_hash: str
) -> LedgerEntry:
    fields_dict = entry._canonical_dict(include_hash=False)  # type: ignore[attr-defined]
    fields_dict.pop("hash", None)
    fields_dict["previous_hash"] = previous_hash
    fields_dict["timestamp"] = entry.timestamp
    fields_dict["causal_parents"] = tuple(entry.causal_parents)
    fields_dict["temporal_violations"] = tuple(entry.temporal_violations)
    provisional = LedgerEntry(**fields_dict)
    fields_dict["hash"] = provisional.compute_hash()
    return LedgerEntry(**fields_dict)


def _copy_v17_record(record: Mapping[str, Any]) -> Dict[str, Any]:
    def _num(key: str, default: float = 0.0) -> float:
        v = record.get(key, default)
        try:
            return float(v) if v is not None else default
        except (TypeError, ValueError):
            return default

    def _str(key: str, default: str = "") -> str:
        v = record.get(key, default)
        return str(v) if v is not None else default

    return {
        "timestamp": _parse_timestamp(
            record.get("timestamp") or _utcnow()
        ),
        "task_id": _str("task_id", str(uuid.uuid4())[:8]),
        "energy_kwh": _num("energy_kwh"),
        "carbon_kg": _num("carbon_kg"),
        "helium_zone": record.get("helium_zone"),
        "helium_usage": _num("helium_usage"),
        "helium_supply_at_execution": _str(
            "helium_supply_at_execution", "unknown"
        ),
        "helium_spot_price": _num("helium_spot_price", 4.0),
        "hardware_type": _str("hardware_type", "unknown"),
        "power_budget": _num("power_budget"),
        "fallback_used": bool(record.get("fallback_used", False)),
        "distillation_teacher_id": record.get("distillation_teacher_id"),
        "distillation_weight": _num_or_none(
            record.get("distillation_weight")
        ),
        "causal_action": record.get("causal_action"),
        "causal_ate_estimate": _num_or_none(
            record.get("causal_ate_estimate")
        ),
        "causal_parents": tuple(record.get("causal_parents", []) or ()),
        "federated_instance_id": record.get("federated_instance_id"),
        "federated_round": _int_or_none(record.get("federated_round")),
        "agent_id": record.get("agent_id"),
        "agent_role": record.get("agent_role"),
        "agent_reputation": _num_or_none(
            record.get("agent_reputation")
        ),
        "state_snapshot": record.get("state_snapshot"),
        "temporal_violations": tuple(
            record.get("temporal_violations", []) or ()
        ),
        "xai_explanation": record.get("xai_explanation"),
        "xai_feature_importance": record.get("xai_feature_importance"),
        "precision_level": _str("precision_level", "fp32"),
        "predicted_energy_saved_wh": _num_or_none(
            record.get("predicted_energy_saved_wh")
        ),
        "carbon_offset_kg": _num("carbon_offset_kg"),
        "rec_mwh": _num("rec_mwh"),
        "offset_cost_usd": _num("offset_cost_usd"),
        "chaos_experiment_id": record.get("chaos_experiment_id"),
        "chaos_fault_type": record.get("chaos_fault_type"),
        "human_review_required": bool(
            record.get("human_review_required", False)
        ),
        "human_approved": record.get("human_approved"),
        "hitl_user_id": record.get("hitl_user_id"),
        "hitl_status": _str("hitl_status", "not_required"),
    }


# --------------------------------------------------------------------------- #
# Extended carbon ledger
# --------------------------------------------------------------------------- #
class ExtendedCarbonLedger:
    """
    Extended carbon ledger with helium accounting, hash chaining, and
    enhancement-aware reporting.

    Recognised ``config`` keys:

    * ``ledger_file``            — path, or ``":memory:"`` (default
                                   ``carbon_ledger.json``).
    * ``max_state_snapshot_bytes`` — default 64 KiB.
    * ``redact_pii``             — bool, default True.
    * ``strict``                 — bool, default True.
    * ``max_entries``            — optional ring-buffer bound.
    """

    SCHEMA_VERSION = 2

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        self.config: Dict[str, Any] = dict(config or {})
        self._strict: bool = bool(self.config.get("strict", True))

        self._max_entries: Optional[int] = self.config.get(
            "max_entries", None
        )
        if self._max_entries is not None:
            if isinstance(self._max_entries, bool) or not isinstance(
                self._max_entries, int
            ):
                raise LedgerError("max_entries must be int or None")
            if self._max_entries <= 0:
                raise LedgerError("max_entries must be > 0 or None")

        self._max_snapshot_bytes: int = int(
            self.config.get("max_state_snapshot_bytes", 64 * 1024)
        )
        self._redact_pii_enabled: bool = bool(
            self.config.get("redact_pii", True)
        )

        raw_file = self.config.get("ledger_file", "carbon_ledger.json")
        if str(raw_file).strip() == _MEMORY_SENTINEL:
            self._memory_only = True
            self.ledger_file = Path(_MEMORY_SENTINEL)
        else:
            self._memory_only = False
            self.ledger_file = Path(raw_file)

        self._ledger: List[LedgerEntry] = []
        self._last_hash: str = ""
        self._created_at: datetime = _utcnow()
        self._hooks: Dict[str, Any] = {}
        self._last_save_error: Optional[str] = None
        self._last_save_success: Optional[datetime] = None

        self._lock = threading.RLock()
        self._save_lock = threading.Lock()

        if not self._memory_only:
            self._load_ledger()

        logger.info(
            "ExtendedCarbonLedger v18.0.0 initialized "
            "(file=%s, entries=%d, memory=%s, strict=%s)",
            self.ledger_file,
            len(self._ledger),
            self._memory_only,
            self._strict,
        )

    # ------------------------------------------------------------------
    # Properties / hook registry
    # ------------------------------------------------------------------
    @property
    def entries(self) -> Tuple[LedgerEntry, ...]:
        with self._lock:
            return tuple(self._ledger)

    def register_enhancement(self, name: str, hook: Any) -> None:
        self._hooks[name] = hook
        logger.debug("Registered enhancement hook: %s", name)

    def hook(self, name: str) -> Any:
        return self._hooks.get(name)

    def report_for(self, name: str) -> Dict[str, Any]:
        hook = self._hooks.get(name)
        if hook is None:
            raise LedgerError(
                f"No enhancement registered under {name!r}"
            )
        for method in ("full_report", "stats", "get_full_report"):
            fn = getattr(hook, method, None)
            if callable(fn):
                result = fn()
                return (
                    result
                    if isinstance(result, dict)
                    else {"result": result}
                )
        return {
            "registered": True,
            "name": name,
            "type": type(hook).__name__,
        }

    # ------------------------------------------------------------------
    # Sanitisation
    # ------------------------------------------------------------------
    def _sanitize_snapshot(
        self, snapshot: Optional[Mapping[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        if snapshot is None:
            return None
        safe = _json_safe(dict(snapshot))
        if self._redact_pii_enabled:
            safe = _redact(safe)
        try:
            encoded = json.dumps(safe, sort_keys=True).encode("utf-8")
        except (TypeError, ValueError) as e:
            raise LedgerError(
                f"state_snapshot is not JSON-serialisable: {e}"
            ) from e
        if len(encoded) > self._max_snapshot_bytes:
            raise LedgerError(
                f"state_snapshot is {len(encoded)} bytes, exceeds "
                f"max_state_snapshot_bytes={self._max_snapshot_bytes}"
            )
        return safe

    # ------------------------------------------------------------------
    # Builder
    # ------------------------------------------------------------------
    def _build_entry(self, fields_dict: Dict[str, Any]) -> LedgerEntry:
        fields_dict["timestamp"] = _parse_timestamp(
            fields_dict.get("timestamp") or _utcnow()
        )
        fields_dict["state_snapshot"] = self._sanitize_snapshot(
            fields_dict.get("state_snapshot")
        )
        if fields_dict.get("hitl_status") not in _VALID_HITL_STATUSES:
            fields_dict["hitl_status"] = "not_required"
        if fields_dict.get("precision_level") not in _VALID_PRECISIONS:
            fields_dict["precision_level"] = "fp32"
        fields_dict["schema_version"] = self.SCHEMA_VERSION
        fields_dict["causal_parents"] = tuple(
            fields_dict.get("causal_parents") or ()
        )
        fields_dict["temporal_violations"] = tuple(
            fields_dict.get("temporal_violations") or ()
        )

        try:
            fields_dict["net_carbon_kg"] = float(
                fields_dict.get("carbon_kg", 0.0)
            ) - float(fields_dict.get("carbon_offset_kg", 0.0))
        except (TypeError, ValueError):
            fields_dict["net_carbon_kg"] = None

        with self._lock:
            fields_dict["previous_hash"] = self._last_hash
            fields_dict["hash"] = ""
            provisional = LedgerEntry(**fields_dict)
            fields_dict["hash"] = provisional.compute_hash()
            entry = LedgerEntry(**fields_dict)
            self._ledger.append(entry)
            self._last_hash = entry.hash
            if (
                self._max_entries is not None
                and len(self._ledger) > self._max_entries
            ):
                del self._ledger[0]

        self._save_ledger()
        return entry

    # ------------------------------------------------------------------
    # v1.0 API
    # ------------------------------------------------------------------
    def add_entry(
        self,
        unified_result: Any,
        execution_decision: Any = None,
        helium_signal: Any = None,
    ) -> LedgerEntry:
        def _get(source: Any, key: str, default: Any = None) -> Any:
            if source is None:
                return default
            if isinstance(source, Mapping):
                return source.get(key, default)
            return getattr(source, key, default)

        helium_zone_raw = _get(execution_decision, "helium_zone", None)
        if hasattr(helium_zone_raw, "value"):
            helium_zone = helium_zone_raw.value
        elif helium_zone_raw is None:
            helium_zone = None
        else:
            helium_zone = str(helium_zone_raw)

        scarcity = _get(helium_signal, "scarcity_level", "unknown")
        if hasattr(scarcity, "value"):
            scarcity = scarcity.value

        fields_dict: Dict[str, Any] = {
            "timestamp": _utcnow(),
            "task_id": _get(
                unified_result, "task_id", str(uuid.uuid4())[:8]
            ),
            "energy_kwh": float(
                _get(unified_result, "energy_consumed_kwh", 0.0) or 0.0
            ),
            "carbon_kg": float(
                _get(unified_result, "carbon_emitted_kg", 0.0) or 0.0
            ),
            "helium_zone": helium_zone,
            "helium_usage": float(
                _get(unified_result, "helium_usage", 0.0) or 0.0
            ),
            "helium_supply_at_execution": str(scarcity),
            "helium_spot_price": float(
                _get(helium_signal, "spot_price_usd_per_liter", 4.0)
                or 4.0
            ),
            "hardware_type": str(
                _get(unified_result, "worker_type", "unknown")
            ),
            "power_budget": float(
                _get(execution_decision, "power_budget", 0.0) or 0.0
            ),
            "fallback_used": bool(
                _get(unified_result, "fallback_used", False)
            ),
        }

        self._populate_v17_fields(
            fields_dict, unified_result, execution_decision
        )
        return self._build_entry(fields_dict)

    def _populate_v17_fields(
        self,
        fields_dict: Dict[str, Any],
        unified_result: Any,
        execution_decision: Any,
    ) -> None:
        v17_attrs = (
            "distillation_teacher_id",
            "distillation_weight",
            "causal_action",
            "causal_ate_estimate",
            "federated_instance_id",
            "federated_round",
            "agent_id",
            "agent_role",
            "agent_reputation",
            "xai_explanation",
            "xai_feature_importance",
            "precision_level",
            "predicted_energy_saved_wh",
            "carbon_offset_kg",
            "rec_mwh",
            "offset_cost_usd",
            "chaos_experiment_id",
            "chaos_fault_type",
            "human_review_required",
            "human_approved",
            "hitl_user_id",
            "hitl_status",
        )

        def _get(source: Any, key: str) -> Any:
            if source is None:
                return None
            if isinstance(source, Mapping):
                return source.get(key)
            return getattr(source, key, None)

        for attr in v17_attrs:
            v = _get(unified_result, attr)
            if v is None:
                v = _get(execution_decision, attr)
            if v is not None:
                fields_dict[attr] = v

        for attr in (
            "state_snapshot",
            "causal_parents",
            "temporal_violations",
        ):
            v = _get(unified_result, attr)
            if v is None:
                v = _get(execution_decision, attr)
            if v is not None:
                fields_dict[attr] = v

    # ------------------------------------------------------------------
    # Explicit API
    # ------------------------------------------------------------------
    def add_entry_v17(self, record: Mapping[str, Any]) -> LedgerEntry:
        return self._build_entry(_copy_v17_record(record))

    def add_entry_v18(self, record: Mapping[str, Any]) -> LedgerEntry:
        return self.add_entry_v17(record)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _save_ledger(self) -> None:
        if self._memory_only:
            return
        path = self.ledger_file
        path.parent.mkdir(parents=True, exist_ok=True)

        with self._save_lock:
            with self._lock:
                payload = {
                    "schema_version": self.SCHEMA_VERSION,
                    "created_at": self._created_at.isoformat(),
                    "last_hash": self._last_hash,
                    "entries": [e.to_dict() for e in self._ledger],
                }
            tmp_path: Optional[str] = None
            try:
                fd, tmp_path = tempfile.mkstemp(
                    prefix=path.name + ".",
                    suffix=".tmp",
                    dir=str(path.parent),
                )
                try:
                    with os.fdopen(fd, "w", encoding="utf-8") as f:
                        json.dump(
                            payload,
                            f,
                            indent=2,
                            allow_nan=False,
                            default=str,
                        )
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(tmp_path, path)
                    tmp_path = None
                finally:
                    if tmp_path is not None:
                        try:
                            os.unlink(tmp_path)
                        except OSError:
                            pass
                with self._lock:
                    self._last_save_error = None
                    self._last_save_success = _utcnow()
            except OSError as e:
                with self._lock:
                    self._last_save_error = f"{type(e).__name__}: {e}"
                logger.error("Failed to save ledger to %s: %s", path, e)
                if self._strict:
                    raise LedgerError(
                        f"Failed to save ledger: {path}"
                    ) from e

    def _load_ledger(self) -> None:
        path = self.ledger_file
        if not path.exists():
            logger.info(
                "No existing ledger at %s; starting fresh", path
            )
            return

        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.error("Failed to read ledger %s: %s", path, e)
            if self._strict:
                raise LedgerError(
                    f"Corrupted ledger file: {path}"
                ) from e
            logger.warning(
                "Starting with empty ledger (non-strict mode)"
            )
            return

        schema = 1
        entries_raw: Sequence[Any]
        if isinstance(data, list):
            entries_raw = data
            schema = 1
        elif isinstance(data, Mapping) and "entries" in data:
            entries_raw = data.get("entries", [])
            schema = int(data.get("schema_version", 2))
        else:
            msg = f"Unrecognised ledger file format in {path}"
            if self._strict:
                raise LedgerError(msg)
            logger.warning(msg)
            return

        if not isinstance(entries_raw, Sequence):
            msg = "entries must be a list"
            if self._strict:
                raise LedgerError(msg)
            logger.warning(msg)
            return

        loaded: List[LedgerEntry] = []
        for i, raw in enumerate(entries_raw):
            try:
                if not isinstance(raw, Mapping):
                    raise LedgerError(
                        f"entry is {type(raw).__name__}, not a mapping"
                    )
                loaded.append(LedgerEntry.from_dict(raw))
            except (LedgerError, TypeError, ValueError) as e:
                if self._strict:
                    raise LedgerError(
                        f"Malformed entry at index {i}: {e}"
                    ) from e
                logger.warning(
                    "Skipping malformed entry at index %d: %s", i, e
                )

        needs_save = False
        if schema < self.SCHEMA_VERSION:
            logger.info(
                "Migrating ledger from schema v%d to v%d",
                schema,
                self.SCHEMA_VERSION,
            )
            migrated: List[LedgerEntry] = []
            prev = ""
            for entry in loaded:
                migrated.append(_rebuild_entry_with_chain(entry, prev))
                prev = migrated[-1].hash
            loaded = migrated
            needs_save = True

        with self._lock:
            self._ledger = loaded
            self._last_hash = loaded[-1].hash if loaded else ""
            if (
                self._max_entries is not None
                and len(self._ledger) > self._max_entries
            ):
                trimmed = self._ledger[-self._max_entries:]
                prev = ""
                rechained: List[LedgerEntry] = []
                for e in trimmed:
                    rechained.append(
                        _rebuild_entry_with_chain(e, prev)
                    )
                    prev = rechained[-1].hash
                self._ledger = rechained
                self._last_hash = (
                    rechained[-1].hash if rechained else ""
                )
                needs_save = True

        if needs_save:
            try:
                self._save_ledger()
            except LedgerError:
                logger.warning(
                    "Migration save failed; original file left intact"
                )

        logger.info(
            "Loaded %d entries from %s (schema v%d)",
            len(self._ledger),
            path,
            schema,
        )

    # ------------------------------------------------------------------
    # Integrity
    # ------------------------------------------------------------------
    def verify_integrity(self) -> bool:
        with self._lock:
            snapshot = list(self._ledger)
        prev = ""
        for i, entry in enumerate(snapshot):
            expected = entry.compute_hash()
            if entry.hash != expected:
                logger.error("Hash mismatch at index %d", i)
                return False
            if entry.previous_hash != prev:
                logger.error(
                    "Chain break at index %d: previous_hash=%s "
                    "expected=%s",
                    i,
                    entry.previous_hash[:12],
                    prev[:12],
                )
                return False
            prev = entry.hash
        return True

    # ------------------------------------------------------------------
    # Query / filter
    # ------------------------------------------------------------------
    def filter(
        self,
        *,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        **field_filters: Any,
    ) -> List[LedgerEntry]:
        with self._lock:
            entries = list(self._ledger)
        since_aware = _ensure_aware(since) if since is not None else None
        until_aware = _ensure_aware(until) if until is not None else None

        def _match(e: LedgerEntry) -> bool:
            if (
                since_aware is not None
                and _ensure_aware(e.timestamp) < since_aware
            ):
                return False
            if (
                until_aware is not None
                and _ensure_aware(e.timestamp) > until_aware
            ):
                return False
            for k, v in field_filters.items():
                if not hasattr(e, k):
                    return False
                if getattr(e, k) != v:
                    return False
            return True

        return [e for e in entries if _match(e)]

    def get_window(
        self, start: datetime, end: datetime
    ) -> List[LedgerEntry]:
        return self.filter(since=start, until=end)

    def query_by_task(self, task_id: str) -> List[LedgerEntry]:
        return self.filter(task_id=task_id)

    def query_by_agent(self, agent_id: str) -> List[LedgerEntry]:
        return self.filter(agent_id=agent_id)

    def iter_entries(self) -> Iterator[LedgerEntry]:
        with self._lock:
            snapshot = list(self._ledger)
        return iter(snapshot)

    # ------------------------------------------------------------------
    # Reports (all accept since/until)
    # ------------------------------------------------------------------
    def get_helium_efficiency_report(
        self,
        task_id: Optional[str] = None,
        *,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        if task_id is not None:
            entries = self.filter(
                since=since, until=until, task_id=task_id
            )
        else:
            entries = self.filter(since=since, until=until)
        if not entries:
            return {"error": "No entries found", "total_entries": 0}

        total_helium = sum(e.helium_usage for e in entries)
        total_energy = sum(e.energy_kwh for e in entries)
        zones: Dict[str, int] = defaultdict(int)
        for e in entries:
            if e.helium_zone:
                zones[e.helium_zone] += 1

        return {
            "total_entries": len(entries),
            "total_helium_usage": total_helium,
            "total_energy_kwh": total_energy,
            "helium_per_energy_ratio": (
                total_helium / total_energy if total_energy > 0 else 0.0
            ),
            "tasks_by_helium_zone": dict(zones),
            "fallback_rate": (
                sum(1 for e in entries if e.fallback_used)
                / len(entries)
            ),
        }

    def get_distillation_teacher_stats(
        self,
        *,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        by_teacher: Dict[str, List[LedgerEntry]] = defaultdict(list)
        for e in self.filter(since=since, until=until):
            if e.distillation_teacher_id:
                by_teacher[e.distillation_teacher_id].append(e)
        out: Dict[str, Any] = {}
        for tid, entries in by_teacher.items():
            out[tid] = {
                "task_count": len(entries),
                "avg_energy_kwh": _safe_mean(
                    e.energy_kwh for e in entries
                ),
                "avg_carbon_kg": _safe_mean(
                    e.carbon_kg for e in entries
                ),
                "avg_weight": _safe_mean(
                    e.distillation_weight for e in entries
                ),
            }
        return out

    def get_causal_action_stats(
        self,
        *,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        by_action: Dict[str, List[LedgerEntry]] = defaultdict(list)
        for e in self.filter(since=since, until=until):
            if e.causal_action:
                by_action[e.causal_action].append(e)
        out: Dict[str, Any] = {}
        for action, entries in by_action.items():
            out[action] = {
                "count": len(entries),
                "avg_carbon_kg": _safe_mean(
                    e.carbon_kg for e in entries
                ),
                "avg_energy_kwh": _safe_mean(
                    e.energy_kwh for e in entries
                ),
                "avg_ate": _safe_mean(
                    e.causal_ate_estimate for e in entries
                ),
            }
        return out

    def estimate_action_ate(self, action: str) -> float:
        """
        Observational ATE estimate: mean carbon of entries with ``action``
        minus mean carbon of all entries. **Not causal.**
        """
        with self._lock:
            entries = list(self._ledger)
        if len(entries) < 2:
            return 0.0
        with_action = [
            e.carbon_kg for e in entries if e.causal_action == action
        ]
        if len(with_action) < 2:
            return 0.0
        return float(
            _safe_mean(with_action)
            - _safe_mean(e.carbon_kg for e in entries)
        )

    def get_aggregated_metrics(
        self,
        *,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        entries = self.filter(since=since, until=until)
        if not entries:
            return {
                "count": 0,
                "total_energy_kwh": 0.0,
                "total_carbon_kg": 0.0,
                "total_helium_usage": 0.0,
                "total_carbon_offset_kg": 0.0,
                "avg_energy_kwh": 0.0,
                "avg_carbon_kg": 0.0,
                "precision_distribution": {},
            }
        precision_counts: Dict[str, int] = defaultdict(int)
        for e in entries:
            precision_counts[e.precision_level] += 1
        total = max(1, sum(precision_counts.values()))
        precision_dist = {
            k: v / total for k, v in precision_counts.items()
        }
        return {
            "count": len(entries),
            "total_energy_kwh": sum(e.energy_kwh for e in entries),
            "total_carbon_kg": sum(e.carbon_kg for e in entries),
            "total_helium_usage": sum(
                e.helium_usage for e in entries
            ),
            "total_carbon_offset_kg": sum(
                e.carbon_offset_kg for e in entries
            ),
            "avg_energy_kwh": _safe_mean(
                e.energy_kwh for e in entries
            ),
            "avg_carbon_kg": _safe_mean(e.carbon_kg for e in entries),
            "precision_distribution": precision_dist,
        }

    def get_agent_performance(
        self,
        agent_id: Optional[str] = None,
        *,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        scoped = self.filter(since=since, until=until)
        by_agent: Dict[str, List[LedgerEntry]] = defaultdict(list)
        for e in scoped:
            if e.agent_id:
                by_agent[e.agent_id].append(e)
        if agent_id is not None:
            return _agent_summary(
                agent_id, by_agent.get(agent_id, [])
            )
        return {
            aid: _agent_summary(aid, es)
            for aid, es in by_agent.items()
        }

    def get_temporal_violation_summary(
        self,
        *,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        entries = self.filter(since=since, until=until)
        violations: Dict[str, int] = defaultdict(int)
        violating_entries = 0
        for e in entries:
            if e.temporal_violations:
                violating_entries += 1
                for v in e.temporal_violations:
                    violations[v] += 1
        return {
            "total_entries": len(entries),
            "violating_entries": violating_entries,
            "violation_rate": (
                violating_entries / len(entries) if entries else 0.0
            ),
            "per_rule": dict(violations),
        }

    def get_xai_aggregate_importance(
        self,
        *,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> Dict[str, float]:
        agg: Dict[str, List[float]] = defaultdict(list)
        for e in self.filter(since=since, until=until):
            if not e.xai_feature_importance:
                continue
            for k, v in e.xai_feature_importance.items():
                try:
                    agg[k].append(abs(float(v)))
                except (TypeError, ValueError):
                    continue
        return {k: _safe_mean(vs) for k, vs in agg.items()}

    def get_precision_energy_savings(
        self,
        *,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        entries = self.filter(since=since, until=until)
        by_precision: Dict[str, List[LedgerEntry]] = defaultdict(list)
        for e in entries:
            by_precision[e.precision_level].append(e)
        return {
            "total_predicted_saved_wh": sum(
                (e.predicted_energy_saved_wh or 0.0) for e in entries
            ),
            "by_precision": {
                p: {
                    "count": len(es),
                    "avg_energy_kwh": _safe_mean(
                        e.energy_kwh for e in es
                    ),
                    "avg_saved_wh": _safe_mean(
                        e.predicted_energy_saved_wh for e in es
                    ),
                }
                for p, es in by_precision.items()
            },
        }

    def get_net_carbon_position(
        self,
        *,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        entries = self.filter(since=since, until=until)
        gross = sum(e.carbon_kg for e in entries)
        offset = sum(e.carbon_offset_kg for e in entries)
        rec = sum(e.rec_mwh for e in entries)
        cost = sum(e.offset_cost_usd for e in entries)
        net = gross - offset
        return {
            "gross_carbon_kg": gross,
            "offset_kg": offset,
            "net_carbon_kg": net,
            "rec_mwh": rec,
            "offset_cost_usd": cost,
            "net_zero": net <= 0.0,
        }

    def get_chaos_impact(
        self,
        experiment_id: Optional[str] = None,
        *,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        entries = self.filter(since=since, until=until)
        with_chaos: List[LedgerEntry] = []
        without_chaos: List[LedgerEntry] = []
        for e in entries:
            if e.chaos_experiment_id:
                if (
                    experiment_id is None
                    or e.chaos_experiment_id == experiment_id
                ):
                    with_chaos.append(e)
            else:
                without_chaos.append(e)

        def _summary(bucket: List[LedgerEntry]) -> Dict[str, float]:
            if not bucket:
                return {
                    "count": 0,
                    "avg_energy_kwh": 0.0,
                    "avg_carbon_kg": 0.0,
                    "fallback_rate": 0.0,
                }
            return {
                "count": len(bucket),
                "avg_energy_kwh": _safe_mean(
                    e.energy_kwh for e in bucket
                ),
                "avg_carbon_kg": _safe_mean(
                    e.carbon_kg for e in bucket
                ),
                "fallback_rate": sum(
                    1 for e in bucket if e.fallback_used
                )
                / len(bucket),
            }

        return {
            "experiment_id": experiment_id,
            "with_chaos": _summary(with_chaos),
            "without_chaos": _summary(without_chaos),
        }

    def get_hitl_impact(
        self,
        *,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        reviewed = [
            e
            for e in self.filter(since=since, until=until)
            if e.human_review_required
        ]
        approved = [
            e for e in reviewed if e.hitl_status == "approved"
        ]
        rejected = [
            e for e in reviewed if e.hitl_status == "rejected"
        ]
        timed_out = [
            e for e in reviewed if e.hitl_status == "timed_out"
        ]
        pending = [
            e for e in reviewed if e.hitl_status == "pending"
        ]

        def _avg(entries: List[LedgerEntry], attr: str) -> float:
            return _safe_mean(getattr(e, attr, 0.0) for e in entries)

        return {
            "total_reviewed": len(reviewed),
            "approved": len(approved),
            "rejected": len(rejected),
            "timed_out": len(timed_out),
            "pending": len(pending),
            "avg_carbon_kg_approved": _avg(approved, "carbon_kg"),
            "avg_carbon_kg_rejected": _avg(rejected, "carbon_kg"),
            "avg_energy_kwh_approved": _avg(approved, "energy_kwh"),
            "avg_energy_kwh_rejected": _avg(rejected, "energy_kwh"),
        }

    # ------------------------------------------------------------------
    # Full report
    # ------------------------------------------------------------------
    def get_full_report(
        self,
        *,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        as_json: bool = False,
    ) -> Dict[str, Any]:
        report: Dict[str, Any] = {
            "schema_version": self.SCHEMA_VERSION,
            "ledger": {
                "entry_count": len(self._ledger),
                "integrity_ok": self.verify_integrity(),
                "last_hash": self._last_hash[:16]
                if self._last_hash
                else "",
            },
            "helium": self.get_helium_efficiency_report(
                since=since, until=until
            ),
            "distillation": self.get_distillation_teacher_stats(
                since=since, until=until
            ),
            "causal": self.get_causal_action_stats(
                since=since, until=until
            ),
            "federated": self.get_aggregated_metrics(
                since=since, until=until
            ),
            "agents": self.get_agent_performance(
                since=since, until=until
            ),
            "temporal": self.get_temporal_violation_summary(
                since=since, until=until
            ),
            "xai": self.get_xai_aggregate_importance(
                since=since, until=until
            ),
            "precision": self.get_precision_energy_savings(
                since=since, until=until
            ),
            "carbon_market": self.get_net_carbon_position(
                since=since, until=until
            ),
            "chaos": self.get_chaos_impact(
                since=since, until=until
            ),
            "hitl": self.get_hitl_impact(since=since, until=until),
        }
        return _json_safe(report) if as_json else report

    # ------------------------------------------------------------------
    # Stats / lifecycle
    # ------------------------------------------------------------------
    def stats(self) -> Dict[str, Any]:
        with self._lock:
            entries = len(self._ledger)
            last_hash = self._last_hash[:16] if self._last_hash else ""
            last_err = self._last_save_error
            last_ok = self._last_save_success
        file_size = 0
        if not self._memory_only and self.ledger_file.exists():
            try:
                file_size = self.ledger_file.stat().st_size
            except OSError:
                pass
        return {
            "schema_version": self.SCHEMA_VERSION,
            "entries": entries,
            "last_hash": last_hash,
            "integrity_ok": self.verify_integrity(),
            "file_path": (
                str(self.ledger_file)
                if not self._memory_only
                else _MEMORY_SENTINEL
            ),
            "file_size_bytes": file_size,
            "last_save_error": last_err,
            "last_save_success": (
                last_ok.isoformat() if last_ok else None
            ),
            "memory_only": self._memory_only,
            "strict": self._strict,
            "registered_enhancements": sorted(self._hooks.keys()),
        }

    def reset(self, *, clear_results: bool = False) -> None:
        with self._lock:
            if clear_results:
                self._ledger.clear()
                self._last_hash = ""
                self._created_at = _utcnow()
        if clear_results:
            self._save_ledger()
        logger.debug(
            "ExtendedCarbonLedger reset (clear_results=%s)",
            clear_results,
        )

    def save(self) -> None:
        self._save_ledger()

    def refresh(self) -> int:
        self._load_ledger()
        with self._lock:
            return len(self._ledger)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return _json_safe(
                {
                    "schema_version": self.SCHEMA_VERSION,
                    "created_at": self._created_at.isoformat(),
                    "last_hash": self._last_hash,
                    "config": dict(self.config),
                    "entries": [e.to_dict() for e in self._ledger],
                }
            )

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("allow_nan", False)
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        autosave: bool = False,
    ) -> "ExtendedCarbonLedger":
        if not isinstance(data, Mapping):
            raise LedgerError(
                f"from_dict expects Mapping, got {type(data).__name__}"
            )
        cfg = dict(data.get("config", {}) or {})
        cfg.setdefault("ledger_file", _MEMORY_SENTINEL)
        ledger = cls(cfg)
        raw_entries = data.get("entries", [])
        if not isinstance(raw_entries, Sequence):
            raise LedgerError("entries must be a list")

        loaded: List[LedgerEntry] = []
        for i, raw in enumerate(raw_entries):
            try:
                loaded.append(LedgerEntry.from_dict(raw))
            except (LedgerError, TypeError, ValueError) as e:
                if ledger._strict:
                    raise LedgerError(
                        f"Malformed entry at index {i}: {e}"
                    ) from e
                logger.warning(
                    "Skipping malformed entry at index %d: %s", i, e
                )

        prev = ""
        rechained: List[LedgerEntry] = []
        for e in loaded:
            rechained.append(_rebuild_entry_with_chain(e, prev))
            prev = rechained[-1].hash

        with ledger._lock:
            ledger._ledger = rechained
            ledger._last_hash = (
                rechained[-1].hash if rechained else ""
            )
        if autosave:
            ledger._save_ledger()
        return ledger

    @classmethod
    def from_json(
        cls, payload: str, *, autosave: bool = False
    ) -> "ExtendedCarbonLedger":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as e:
            raise LedgerError(f"Invalid JSON payload: {e}") from e
        return cls.from_dict(data, autosave=autosave)

    # ------------------------------------------------------------------
    # Optional pandas
    # ------------------------------------------------------------------
    def to_dataframe(self):  # type: ignore[no-untyped-def]
        try:
            import pandas as pd  # type: ignore
        except ImportError as e:
            raise ImportError(
                "pandas required for to_dataframe()"
            ) from e
        return pd.DataFrame([e.to_dict() for e in self.iter_entries()])


# =========================================================================== #
# Orchestrator
# =========================================================================== #
def _validate_orchestrator_config(config: Any) -> None:
    if config is None:
        return
    fail_mode = _cfg_get(config, "fail_mode", "log")
    if fail_mode not in _VALID_FAIL_MODES:
        raise LedgerError(
            f"fail_mode must be one of {_VALID_FAIL_MODES}, "
            f"got {fail_mode!r}"
        )
    intensity = _cfg_get(config, "chaos_intensity", 0.0)
    if not isinstance(intensity, (int, float)) or not (
        0.0 <= float(intensity) <= 1.0
    ):
        raise LedgerError(
            f"chaos_intensity must be in [0, 1], got {intensity!r}"
        )


def _estimate_precision_savings(
    before: str, after: str, energy_kwh: float
) -> float:
    """Energy saved (Wh) by switching from ``before`` to ``after``."""
    ratios = AdaptivePrecisionSwitcher.ENERGY
    delta = ratios.get(before, 1.0) - ratios.get(after, 1.0)
    return max(0.0, delta * energy_kwh * 1000.0)


class CarbonLedgerOrchestratorV18:
    """
    Unified entry point: wires ``ExtendedCarbonLedger`` with the ten
    enhancements and exposes ``record_task``.

    ``start()`` is idempotent; ``_shutdown_event`` is cleared on start so
    ``start()`` can follow ``shutdown()``. Background loops exit promptly
    on shutdown.
    """

    def __init__(
        self,
        storage: Any,
        config: Any,
        dashboard: Any = None,
    ) -> None:
        _validate_orchestrator_config(config)

        self.storage = storage
        self.config = config
        self._fail_mode = _cfg_get(config, "fail_mode", "log")
        if self._fail_mode not in _VALID_FAIL_MODES:
            raise LedgerError(
                f"fail_mode must be one of {_VALID_FAIL_MODES}, "
                f"got {self._fail_mode!r}"
            )
        self.instance_id = uuid.uuid4().hex[:8]

        # Ten enhancements (simplified reference implementations)
        self.quantum = QuantumDistillationEngine()
        self.causal_graph = CausalGraphLearner(storage)
        self.causal_rl = CausalPolicyAdapter(
            config, storage, self.causal_graph
        )
        self.federated = FederatedGreenAggregator(
            storage, self.instance_id
        )
        self.multi_agent = MultiAgentCoordinator(config, storage)
        self.temporal = TemporalLogicVerifier(storage, config)
        self.xai = XAIDecisionExplainer(config, storage)
        self.precision = AdaptivePrecisionSwitcher(config, storage)
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.chaos = ChaosTestingEngine(config, storage)
        self.hitl = ActiveUserPreferenceLearner(
            storage, dashboard=dashboard
        )

        ledger_cfg = {
            "ledger_file": _cfg_get(
                config, "ledger_file", "carbon_ledger_v18.json"
            ),
            "strict": bool(_cfg_get(config, "ledger_strict", True)),
            "max_entries": _cfg_get(config, "ledger_max_entries", None),
            "max_state_snapshot_bytes": int(
                _cfg_get(config, "ledger_max_snapshot_bytes", 64 * 1024)
            ),
            "redact_pii": bool(
                _cfg_get(config, "ledger_redact_pii", True)
            ),
        }
        self.ledger = ExtendedCarbonLedger(ledger_cfg)

        for name, hook in (
            ("federated", self.federated),
            ("xai", self.xai),
            ("precision", self.precision),
            ("carbon_market", self.carbon_market),
            ("chaos", self.chaos),
            ("hitl", self.hitl),
            ("causal", self.causal_rl),
            ("multi_agent", self.multi_agent),
            ("temporal", self.temporal),
        ):
            self.ledger.register_enhancement(name, hook)

        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: Set["asyncio.Task[Any]"] = set()
        self._lifecycle_lock = asyncio.Lock()
        self._hook_failures: int = 0

    # ------------------------------------------------------------------
    async def _run_hook(
        self, name: str, fn: Any, *args: Any, **kwargs: Any
    ) -> Any:
        try:
            result = fn(*args, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            return result
        except Exception as e:
            self._hook_failures += 1
            if self._fail_mode == "raise":
                raise
            if self._fail_mode == "log":
                logger.warning("Hook %r failed: %s", name, e)
            return None

    # ------------------------------------------------------------------
    async def record_task(
        self,
        task_id: str,
        energy_kwh: float,
        carbon_kg: float,
        helium_zone: Optional[str] = None,
        helium_usage: float = 0.0,
        helium_spot_price: float = 4.0,
        hardware_type: str = "cpu",
        power_budget: float = 0.0,
        fallback_used: bool = False,
        state: Optional[Mapping[str, Any]] = None,
        force_chaos: Optional[str] = None,
        force_human_review: bool = False,
    ) -> LedgerEntry:
        state_dict = dict(state or {})

        # 1. Causal RL
        causal_action = await self._run_hook(
            "causal_rl_choose",
            self.causal_rl.choose_action,
            state_dict,
        )
        if causal_action is None:
            causal_action = CausalPolicyAdapter.ACTIONS[0]
        causal_parents = self.causal_graph.parents("carbon")
        causal_ate = self.ledger.estimate_action_ate(causal_action)

        # 2. Multi-agent bid
        bid = await self._run_hook(
            "multi_agent_bid",
            self.multi_agent.bid,
            {"name": task_id, "preferred_role": "optimizer"},
        )
        if bid:
            agent_id, _score = bid
        else:
            agent_id = next(iter(self.multi_agent.agents))
        agent = self.multi_agent.agents[agent_id]
        agent_role = agent.role
        agent_rep = agent.reputation

        # 3. Precision (per-call delta)
        precision_before = self.precision.current
        await self._run_hook(
            "precision_auto_switch",
            self.precision.auto_switch,
            0.9,
            0.92,
        )
        precision_after = self.precision.current
        predicted_saved_wh = _estimate_precision_savings(
            precision_before, precision_after, energy_kwh
        )

        # 4. Carbon market
        cm = await self._run_hook(
            "carbon_market_schedule",
            self.carbon_market.net_zero_schedule,
            workload_kwh=energy_kwh,
            intensity=(carbon_kg / max(energy_kwh, 1e-6)),
        )
        carbon_offset_kg = 0.0
        offset_cost_usd = 0.0
        if cm and cm.get("action") == "run_offset":
            carbon_offset_kg = carbon_kg
            offset_cost_usd = float(cm.get("offset_cost_usd", 0.0))

        # 5. Temporal (orchestrator values win over `state`)
        temporal_state: Dict[str, Any] = dict(state_dict)
        temporal_state["quality"] = 1.0 - float(fallback_used) * 0.5
        temporal_state["carbon"] = carbon_kg
        await self._run_hook(
            "temporal_push", self.temporal.push_state, temporal_state
        )
        verify = await self._run_hook(
            "temporal_verify", self.temporal.verify
        )
        temporal_violations: Tuple[str, ...] = ()
        if verify:
            temporal_violations = tuple(
                k for k, ok in verify.items() if not ok
            )

        # 6. XAI
        xai_explanation: Optional[Dict[str, float]] = None
        if NUMPY_AVAILABLE:

            async def _xai_call() -> Optional[Dict[str, float]]:
                feats = np.array(
                    [
                        energy_kwh,
                        carbon_kg,
                        power_budget,
                        float(helium_usage),
                    ]
                )
                result = await self.xai.explain(
                    decision_id=f"ledger_{task_id}",
                    label=f"task:{task_id}",
                    features=feats,
                    names=[
                        "energy_kwh",
                        "carbon_kg",
                        "power_budget",
                        "helium_usage",
                    ],
                    model_fn=lambda x: float(
                        np.dot(x, [0.4, 0.3, 0.2, 0.1])
                    ),
                )
                return result.get("attributions") if result else None

            xai_explanation = await self._run_hook("xai", _xai_call)

        # 7. Chaos
        chaos_experiment_id: Optional[str] = None
        chaos_fault_type: Optional[str] = None
        intensity = float(_cfg_get(self.config, "chaos_intensity", 0.0))
        if force_chaos is not None or (
            intensity > 0 and random.random() < intensity
        ):
            fault = force_chaos or random.choice(
                ChaosTestingEngine.FAULT_TYPES
            )
            experiment_id = f"chaos_{uuid.uuid4().hex[:6]}"
            await self._run_hook(
                "chaos", self.chaos.run_experiment, experiment_id, fault
            )
            chaos_experiment_id = experiment_id
            chaos_fault_type = fault

        # 8. HITL
        human_approved: Optional[bool] = None
        hitl_status = "not_required"
        hitl_user_id: Optional[str] = None
        if force_human_review:
            hitl_user_id = "ledger_user"
            response = await self._run_hook(
                "hitl",
                self.hitl.query_user_if_needed,
                "ledger_user",
                [
                    {"solution_id": "approve", "quality_score": 0.9},
                    {"solution_id": "reject", "quality_score": 0.8},
                ],
                timeout=0.5,
            )
            if response is None:
                hitl_status = "timed_out"
                human_approved = None
            elif response == "approve":
                hitl_status = "approved"
                human_approved = True
            elif response == "reject":
                hitl_status = "rejected"
                human_approved = False
            else:
                hitl_status = "pending"
                human_approved = None

        # 9. Federated
        async def _federated_call() -> None:
            agg = self.ledger.get_aggregated_metrics()
            blob = json.dumps(agg, default=str).encode()
            await self.federated.share_weights("carbon_ledger", blob)

        await self._run_hook("federated", _federated_call)

        # 10. Distillation teacher
        self.quantum.register_teacher(
            "causal", self.causal_rl.get_policy()
        )
        teacher_policy = self.causal_rl.get_policy()
        teacher_weight = teacher_policy[0] if teacher_policy else 0.0

        entry = self.ledger.add_entry_v18(
            {
                "task_id": task_id,
                "energy_kwh": energy_kwh,
                "carbon_kg": carbon_kg,
                "helium_zone": helium_zone,
                "helium_usage": helium_usage,
                "helium_spot_price": helium_spot_price,
                "hardware_type": hardware_type,
                "power_budget": power_budget,
                "fallback_used": fallback_used,
                "distillation_teacher_id": "causal",
                "distillation_weight": teacher_weight,
                "causal_action": causal_action,
                "causal_ate_estimate": causal_ate,
                "causal_parents": tuple(causal_parents),
                "federated_instance_id": self.instance_id,
                "federated_round": self.federated.rounds,
                "agent_id": agent_id,
                "agent_role": agent_role,
                "agent_reputation": agent_rep,
                "state_snapshot": state_dict,
                "temporal_violations": temporal_violations,
                "xai_explanation": xai_explanation,
                "xai_feature_importance": xai_explanation,
                "precision_level": precision_after,
                "predicted_energy_saved_wh": predicted_saved_wh,
                "carbon_offset_kg": carbon_offset_kg,
                "rec_mwh": 0.0,
                "offset_cost_usd": offset_cost_usd,
                "chaos_experiment_id": chaos_experiment_id,
                "chaos_fault_type": chaos_fault_type,
                "human_review_required": force_human_review,
                "human_approved": human_approved,
                "hitl_user_id": hitl_user_id,
                "hitl_status": hitl_status,
            }
        )

        reward = max(0.0, 1.0 - float(fallback_used) * 0.5)
        await self._run_hook(
            "multi_agent_reward", self.multi_agent.reward, agent_id, reward
        )

        return entry

    # ------------------------------------------------------------------
    async def start(self) -> None:
        async with self._lifecycle_lock:
            if self._running:
                logger.debug(
                    "Orchestrator already running; start() is a no-op"
                )
                return
            self._shutdown_event.clear()
            self._running = True
            loop = asyncio.get_running_loop()
            factories: List[Tuple[str, Any]] = [
                ("causal_rl", self._causal_rl_loop),
                ("federated", self._federated_loop),
                ("multi_agent", self._multi_agent_loop),
                ("temporal", self._temporal_loop),
                ("xai", self._xai_loop),
                ("precision", self._precision_loop),
                ("carbon_market", self._carbon_market_loop),
                ("chaos", self._chaos_loop),
                ("distillation", self._distillation_loop),
            ]
            for name, factory in factories:
                t = loop.create_task(factory(), name=f"cl_{name}")
                self._background_tasks.add(t)
                t.add_done_callback(self._background_tasks.discard)
            logger.info(
                "CarbonLedgerOrchestratorV18 started (%d loops)",
                len(self._background_tasks),
            )

    async def shutdown(self) -> None:
        async with self._lifecycle_lock:
            self._shutdown_event.set()
            self._running = False
            tasks = list(self._background_tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        with self._lifecycle_lock:
            self._background_tasks.clear()
        try:
            self.ledger.save()
        except LedgerError:
            logger.exception("Failed to persist ledger on shutdown")
        logger.info("CarbonLedgerOrchestratorV18 shut down")

    async def __aenter__(self) -> "CarbonLedgerOrchestratorV18":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ------------------------------------------------------------------
    async def _sleep_until_shutdown(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(
                self._shutdown_event.wait(), timeout=timeout
            )
            return True
        except asyncio.TimeoutError:
            return False

    async def _causal_rl_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep_until_shutdown(900):
                return
            try:
                samples = [
                    {
                        "carbon": random.uniform(0.05, 0.5),
                        "carbon_intensity": random.uniform(150, 700),
                        "quality": random.uniform(0.5, 1.0),
                        "cost": random.uniform(0.1, 0.9),
                        "latency": random.uniform(0.1, 0.9),
                    }
                    for _ in range(20)
                ]
                await self.causal_graph.learn(
                    samples,
                    [
                        "carbon",
                        "carbon_intensity",
                        "quality",
                        "cost",
                        "latency",
                    ],
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("causal_rl_loop failed: %s", e)

    async def _federated_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep_until_shutdown(3600):
                return
            try:
                agg = self.ledger.get_aggregated_metrics()
                blob = json.dumps(agg, default=str).encode()
                await self.federated.share_weights(
                    "carbon_ledger", blob
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("federated_loop failed: %s", e)

    async def _multi_agent_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep_until_shutdown(600):
                return
            try:
                await self.multi_agent.step()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("multi_agent_loop failed: %s", e)

    async def _temporal_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep_until_shutdown(300):
                return
            try:
                await self.temporal.verify()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("temporal_loop failed: %s", e)

    async def _xai_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep_until_shutdown(300):
                return
            try:
                imp = self.ledger.get_xai_aggregate_importance()
                if imp:
                    logger.debug("Ledger XAI aggregate: %s", imp)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("xai_loop failed: %s", e)

    async def _precision_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep_until_shutdown(300):
                return
            try:
                await self.precision.auto_switch(0.9, 0.92)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("precision_loop failed: %s", e)

    async def _carbon_market_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep_until_shutdown(3600):
                return
            try:
                await self.carbon_market.update_price()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("carbon_market_loop failed: %s", e)

    async def _chaos_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep_until_shutdown(1800):
                return
            try:
                intensity = float(
                    _cfg_get(self.config, "chaos_intensity", 0.0)
                )
                if intensity > 0 and random.random() < intensity:
                    fault = random.choice(
                        ChaosTestingEngine.FAULT_TYPES
                    )
                    await self.chaos.run_experiment(
                        f"auto_{uuid.uuid4().hex[:6]}", fault
                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("chaos_loop failed: %s", e)

    async def _distillation_loop(self) -> None:
        while not self._shutdown_event.is_set():
            if await self._sleep_until_shutdown(300):
                return
            try:
                self.quantum.register_teacher(
                    "causal", self.causal_rl.get_policy()
                )
                self.quantum.register_teacher(
                    "agents", self.multi_agent.get_policy()
                )
                await self.quantum.step(
                    self.storage, student_id="ledger_student"
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("distillation_loop failed: %s", e)

    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        stats = self.ledger.stats()
        return {
            "version": "18.0.0",
            "instance_id": self.instance_id,
            "running": self._running,
            "ledger": stats,
            "precision": self.precision.current,
            "agents": {
                a.id: a.role
                for a in self.multi_agent.agents.values()
            },
            "federated_rounds": self.federated.rounds,
            "hook_failures_total": self._hook_failures,
            "fail_mode": self._fail_mode,
        }

    def full_report(self, *, as_json: bool = False) -> Dict[str, Any]:
        return self.ledger.get_full_report(as_json=as_json)


# Backward-compatible alias
CarbonLedgerOrchestratorV17 = CarbonLedgerOrchestratorV18


# =========================================================================== #
# Bounded in-memory storage (test / demo)
# =========================================================================== #
class InMemoryStorage:
    """Bounded in-memory storage for demos and tests."""

    def __init__(self, max_entries: int = 10_000) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries must be > 0")
        self.max_entries = max_entries
        self._data: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=max_entries)
        )
        self._prefs: Dict[str, Dict[str, float]] = {}

    def _record(self, bucket: str, entry: Any) -> None:
        self._data[bucket].append(entry)

    def save_carbon_intensity(
        self,
        region: str,
        intensity: float,
        timestamp: Optional[str] = None,
    ) -> None:
        self._record(
            "carbon_intensity",
            {
                "region": region,
                "intensity": intensity,
                "ts": timestamp,
            },
        )

    def save_teacher_superposition(
        self, *a: Any, **kw: Any
    ) -> None:
        self._record("teacher_superpositions", a)

    def save_causal_edge(
        self,
        source: str,
        target: str,
        weight: float,
        confidence: float,
    ) -> None:
        self._record(
            "causal_graph",
            {"source": source, "target": target, "weight": weight},
        )

    def save_causal_experiment(
        self,
        exp_id: str,
        treatment: str,
        outcome: str,
        ate: float,
        samples: int,
        method: str = "",
    ) -> None:
        self._record(
            "causal_experiments", {"exp_id": exp_id, "ate": ate}
        )

    def save_federated_weights(
        self,
        instance_id: str,
        model_id: str,
        weights: bytes,
        weight_norm: float = 0.0,
        round_id: int = 0,
    ) -> None:
        self._record(
            "federated_weights",
            {
                "instance_id": instance_id,
                "model_id": model_id,
                "weights": weights,
            },
        )

    def get_federated_weights(
        self, model_id: str
    ) -> List[Dict[str, Any]]:
        return [
            r
            for r in self._data["federated_weights"]
            if r["model_id"] == model_id
        ]

    def save_precision_switch(self, *a: Any, **kw: Any) -> None:
        self._record("precision_history", a)

    def save_credit_price(
        self, price_usd: float, **kw: Any
    ) -> None:
        self._record(
            "carbon_credit_prices", {"price_usd": price_usd}
        )

    def save_rec(
        self,
        mwh: float,
        price_per_mwh: float,
        source: str,
        **kw: Any,
    ) -> None:
        self._record("rec_ledger", {"mwh": mwh})

    def save_net_zero_match(self, *a: Any, **kw: Any) -> None:
        self._record("net_zero_matches", a)

    def save_chaos_experiment(self, *a: Any, **kw: Any) -> None:
        self._record("chaos_experiments", a)

    def save_temporal_trace(
        self, state: Mapping[str, Any], context: Any = None
    ) -> None:
        self._record("temporal_trace", {"state": dict(state)})

    def save_xai_explanation(self, *a: Any, **kw: Any) -> None:
        self._record("xai_explanations", a)

    def save_user_preference(
        self, user_id: str, weights: Mapping[str, float]
    ) -> None:
        self._prefs[user_id] = dict(weights)

    def get_user_preference(
        self, user_id: str
    ) -> Optional[Dict[str, float]]:
        return self._prefs.get(user_id)


# =========================================================================== #
# Public API
# =========================================================================== #
__all__ = [
    # Errors
    "LedgerError",
    # Constants
    "NUMPY_AVAILABLE",
    "SKLEARN_AVAILABLE",
    "TORCH_AVAILABLE",
    "_MEMORY_SENTINEL",
    # Helpers
    "_utcnow",
    "_ensure_aware",
    "_parse_timestamp",
    "_json_safe",
    "_cfg_get",
    "_safe_mean",
    "_safe_pstdev",
    "_redact",
    # Types
    "LedgerEntry",
    # Main
    "ExtendedCarbonLedger",
    "CarbonLedgerOrchestratorV18",
    "CarbonLedgerOrchestratorV17",
    # Storage
    "InMemoryStorage",
    # Enhancements (simplified reference implementations)
    "QuantumDistillationEngine",
    "CausalGraphLearner",
    "CausalPolicyAdapter",
    "FederatedGreenAggregator",
    "MultiAgentCoordinator",
    "TemporalRule",
    "TemporalLogicVerifier",
    "XAIDecisionExplainer",
    "AdaptivePrecisionSwitcher",
    "CarbonMarketIntegrator",
    "ChaosTestingEngine",
    "ActiveUserPreferenceLearner",
    # Simple aliases
    "SimpleQuantumDistillationEngine",
    "SimpleCausalGraphLearner",
    "SimpleCausalPolicyAdapter",
    "SimpleFederatedGreenAggregator",
    "SimpleMultiAgentCoordinator",
    "SimpleTemporalLogicVerifier",
    "SimpleXAIDecisionExplainer",
    "SimpleAdaptivePrecisionSwitcher",
    "SimpleCarbonMarketIntegrator",
    "SimpleChaosTestingEngine",
    "SimpleActiveUserPreferenceLearner",
]


# =========================================================================== #
# Demo
# =========================================================================== #
async def _demo() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        ),
    )

    config = {
        "causal_exploration_rate": 0.1,
        "agent_count": 5,
        "temporal_max_trace": 500,
        "temporal_formulas": [
            "G (quality >= 0.5)",
            "G (carbon <= 500)",
        ],
        "xai_method": "permutation",
        "xai_depth": 5,
        "precision_levels": ["fp32", "fp16", "bf16", "int8"],
        "precision_switch_threshold": 0.02,
        "chaos_intensity": 0.3,
        "chaos_blast_radius": 0.1,
        "fail_mode": "log",
        "ledger_file": "/tmp/carbon_ledger_v18_demo.json",
        "ledger_max_entries": 1000,
    }

    storage = InMemoryStorage(max_entries=5000)

    async with CarbonLedgerOrchestratorV18(storage, config) as orch:
        print("=" * 80)
        print("carbon_ledger.py v18.0.0 — Demo")
        print("=" * 80)

        print("\n=== Recording 10 tasks ===")
        for i in range(10):
            entry = await orch.record_task(
                task_id=f"task_{i:03d}",
                energy_kwh=random.uniform(0.5, 2.0),
                carbon_kg=random.uniform(0.05, 0.5),
                helium_zone=random.choice(
                    ["zone_a", "zone_b", None]
                ),
                helium_usage=random.uniform(0.0, 0.01),
                helium_spot_price=random.uniform(3.0, 6.0),
                hardware_type=random.choice(
                    ["cpu", "gpu", "tpu"]
                ),
                power_budget=random.uniform(100, 400),
                fallback_used=random.random() < 0.1,
                state={
                    "carbon_intensity": random.uniform(150, 600),
                    "queue_length": random.randint(0, 5),
                },
                force_human_review=(i % 5 == 0),
            )
            print(
                f"  {entry.task_id}  "
                f"hash={entry.hash[:10]}  "
                f"prev={entry.previous_hash[:10]}  "
                f"action={entry.causal_action}  "
                f"precision={entry.precision_level}  "
                f"hitl={entry.hitl_status}"
            )

        print("\n=== Integrity check ===")
        ok = orch.ledger.verify_integrity()
        print(f"  Ledger integrity: {ok}")
        assert ok, "integrity check must pass"

        print("\n=== Filter (agent + precision) ===")
        agents = {
            e.agent_id
            for e in orch.ledger.iter_entries()
            if e.agent_id
        }
        some_agent = next(iter(agents))
        filtered = orch.ledger.filter(
            agent_id=some_agent, precision_level="fp32"
        )
        print(
            f"  {len(filtered)} entries for {some_agent} with fp32"
        )

        print("\n=== Full report ===")
        report = orch.full_report()
        for section, value in report.items():
            if isinstance(value, dict):
                print(f"  {section:<16}: {len(value)} key(s)")
            else:
                print(f"  {section:<16}: {value}")

        print("\n=== HITL impact (v18 status) ===")
        print(json.dumps(report["hitl"], indent=2, default=str))

        print("\n=== Health check ===")
        print(
            json.dumps(
                await orch.health_check(), indent=2, default=str
            )
        )

        print("\n=== Serialization round-trip ===")
        payload = orch.ledger.to_json()
        assert "Infinity" not in payload
        assert "NaN" not in payload
        restored = ExtendedCarbonLedger.from_json(payload)
        assert (
            restored.stats()["entries"]
            == orch.ledger.stats()["entries"]
        )
        assert restored.verify_integrity()
        print(
            f"  Round-trip OK "
            f"({restored.stats()['entries']} entries, "
            f"integrity={restored.verify_integrity()})"
        )

        print("\n=== Tamper detection ===")
        tampered = ExtendedCarbonLedger(
            {"ledger_file": ":memory:"}
        )
        for e in orch.ledger.iter_entries():
            copy_entry = LedgerEntry.from_dict(e.to_dict())
            tampered._ledger.append(copy_entry)
        tampered._last_hash = orch.ledger._last_hash
        # Replace (don't mutate) the target so the original is untouched
        old = tampered._ledger[3]
        new_dict = old.to_dict()
        new_dict["carbon_kg"] = 999.0
        tampered._ledger[3] = LedgerEntry.from_dict(new_dict)
        assert not tampered.verify_integrity(), (
            "tampered ledger must fail integrity"
        )
        print("  Tampered ledger correctly fails integrity check")

        print("\n=== Lifecycle: shutdown then start ===")
        await orch.shutdown()
        await orch.start()
        print("  shutdown() then start() works")

    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
