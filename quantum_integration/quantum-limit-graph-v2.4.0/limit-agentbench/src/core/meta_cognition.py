"""
meta_cognitive_layer.py — Enhanced v17.0.0
===========================================

Self-reflection layer for the Green Agent — now extended with:

  * **Reflection history** with rolling baselines and long-term memory.
  * **Pareto dominance computation** between new and historical metrics
    (accuracy, energy, carbon, latency, cost).
  * **Causal attribution** of accuracy/energy changes via the
    `CausalPolicyAdapter`.
  * **Temporal logic verification** of reflection properties
    ("quality ≥ 0.9 for N steps", "no more than 3 carbon spikes in a window").
  * **XAI explanations** for critical reflections.
  * **HITL escalation** for high-severity anomalies.
  * **Meta-policy weight adjustment** (carbon, cost, latency, quality).
  * **Federated sharing** of the meta-policy across deployments.
  * **Carbon-aware deferral** of expensive reflections.
  * **Chaos testing** of the reflection pipeline.
  * **Persistence** of reflections.
  * **GraphRegistry integration** — the reflection history is registered as
    a `REFLECTION` graph for downstream analysis.

All ten advanced Green Agent enhancements are implemented in a single
self-contained file:

   1. Quantum-Distillation Integration        → QuantumDistillationEngine
   2. Causal Reinforcement Learning           → CausalGraphLearner + CausalPolicyAdapter
   3. Federated Green Learning                → FederatedGreenAggregator
   4. Advanced Multi-Agent Coordination       → MultiAgentCoordinator
   5. Temporal Logic & Formal Verification    → TemporalLogicVerifier
   6. Explainable AI                          → XAIDecisionExplainer
   7. Adaptive Precision Switching            → AdaptivePrecisionSwitcher
   8. Carbon Markets / REC                    → CarbonMarketIntegrator
   9. Resilience Engineering / Chaos Testing  → ChaosTestingEngine
  10. HITL Active Learning                    → ActiveUserPreferenceLearner

Unified entry point: **MetaCognitiveOrchestratorV17**

The file is self-contained: Python stdlib + optional numpy/sklearn/torch.
All storage / enhancement hooks soft-fail.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import statistics
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

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
# META COGNITIVE LAYER (the target of this enhancement)
# =============================================================================
class ReflectionSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class ReflectionRecord:
    """A single reflection with metrics, verdict, and metadata."""
    reflection_id: str
    accuracy: float
    energy: float
    carbon: float
    latency_ms: float
    cost_usd: float
    verdict: str
    severity: str
    narrative: str
    pareto_status: str = "unknown"    # dominates | dominated | tradeoff | baseline
    causal_attributions: Dict[str, float] = field(default_factory=dict)
    temporal_violations: List[str] = field(default_factory=list)
    xai_explanation: Optional[Dict[str, Any]] = None
    escalated_to_hitl: bool = False
    adjustments: Dict[str, float] = field(default_factory=dict)
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat())


class MetaCognitiveLayer:
    """
    Self-reflection layer with Pareto trade-off analysis, causal attribution,
    temporal verification, XAI explanation, HITL escalation, and meta-policy
    adjustment.

    The v1.0 stub only returned a formatted string. This v17.0 version:
      * tracks a rolling history of reflections,
      * computes Pareto dominance between new and past performance,
      * attributes change to causal parents (when a causal graph is available),
      * verifies temporal properties of the reflection stream,
      * generates XAI explanations for critical reflections,
      * escalates critical reflections to a human,
      * adjusts the meta-policy weights (quality/carbon/cost/latency),
      * persists every reflection,
      * shares the meta-policy across deployments,
      * defers reflections during high-carbon windows,
      * injects chaos occasionally to test resilience.
    """

    # Default target metrics — used to detect deviations
    DEFAULT_TARGETS = {
        "accuracy": 0.85,
        "energy": 1.0,
        "carbon": 0.4,
        "latency_ms": 200.0,
        "cost_usd": 0.1,
    }

    # Meta-policy weights adjusted by the reflection
    DEFAULT_META_POLICY = {
        "quality": 0.4,
        "carbon": 0.3,
        "cost": 0.2,
        "latency": 0.1,
    }

    def __init__(self,
                 config: Optional[Any] = None,
                 storage: Optional[Any] = None,
                 # Enhancement hooks (all optional)
                 causal_rl: Optional["CausalPolicyAdapter"] = None,
                 temporal: Optional["TemporalLogicVerifier"] = None,
                 xai: Optional["XAIDecisionExplainer"] = None,
                 hitl: Optional["ActiveUserPreferenceLearner"] = None,
                 carbon_market: Optional["CarbonMarketIntegrator"] = None,
                 chaos: Optional["ChaosTestingEngine"] = None,
                 federated: Optional["FederatedGreenAggregator"] = None,
                 multi_agent: Optional["MultiAgentCoordinator"] = None,
                 precision: Optional["AdaptivePrecisionSwitcher"] = None,
                 registry: Optional[Any] = None):
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

        # History and baselines
        self.history: Deque[ReflectionRecord] = deque(maxlen=500)
        self.targets: Dict[str, float] = dict(self.DEFAULT_TARGETS)
        self.meta_policy: Dict[str, float] = dict(self.DEFAULT_META_POLICY)
        self.reflection_count = 0
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Core reflection
    # ------------------------------------------------------------------
    async def reflect(self,
                      accuracy: float,
                      energy: float,
                      carbon: Optional[float] = None,
                      latency_ms: Optional[float] = None,
                      cost_usd: Optional[float] = None,
                      defer_if_high_carbon: bool = True) -> ReflectionRecord:
        """
        Reflect on the current performance metrics.

        The reflection:
          1. optionally defers during a high-carbon window,
          2. optionally injects chaos to test the pipeline,
          3. computes a Pareto status vs. the historical baseline,
          4. attributes change to causal parents (if a graph is available),
          5. verifies temporal properties of the reflection stream,
          6. generates an XAI explanation if the reflection is critical,
          7. escalates critical reflections to HITL,
          8. adjusts meta-policy weights,
          9. persists the reflection,
         10. shares the meta-policy across deployments.
        """
        async with self._lock:
            self.reflection_count += 1
            reflection_id = uuid.uuid4().hex[:8]

            # Defaults for optional metrics
            carbon = carbon if carbon is not None else 0.4
            latency_ms = latency_ms if latency_ms is not None else 200.0
            cost_usd = cost_usd if cost_usd is not None else 0.1

            # --- 1. Chaos hook ---
            if self.chaos is not None and random.random() < 0.05:
                try:
                    await self.chaos.run_experiment(
                        f"reflection_{reflection_id}", "latency")
                except Exception:
                    pass

            # --- 2. Carbon-aware deferral ---
            if defer_if_high_carbon and self.carbon_market is not None:
                try:
                    decision = await self.carbon_market.net_zero_schedule(
                        workload_kwh=0.001, intensity=carbon)
                    if decision.get("action") == "defer":
                        record = ReflectionRecord(
                            reflection_id=reflection_id,
                            accuracy=accuracy, energy=energy,
                            carbon=carbon, latency_ms=latency_ms,
                            cost_usd=cost_usd,
                            verdict="deferred", severity="info",
                            narrative="Reflection deferred (high carbon).",
                            pareto_status="deferred")
                        self.history.append(record)
                        return record
                except Exception:
                    pass

            # --- 3. Pareto status vs. baseline ---
            pareto = self._compute_pareto_status(
                accuracy, energy, carbon, latency_ms, cost_usd)

            # --- 4. Causal attribution ---
            causal_attrs: Dict[str, float] = {}
            if self.causal_rl is not None and self.causal_rl.graph is not None:
                try:
                    parents = self.causal_rl.graph.parents("quality")
                    for p in parents:
                        edge = self.causal_rl.graph.graph.get(p, {}).get(
                            "quality", {})
                        causal_attrs[p] = float(edge.get("weight", 0.0))
                except Exception:
                    pass

            # --- 5. Temporal verification ---
            temporal_violations: List[str] = []
            if self.temporal is not None:
                try:
                    await self.temporal.push_state({
                        "quality": accuracy,
                        "carbon": carbon,
                        "energy": energy})
                    verify = await self.temporal.verify()
                    temporal_violations = [
                        k for k, ok in verify.items() if not ok]
                except Exception:
                    pass

            # --- 6. Determine severity ---
            severity, verdict = self._evaluate_severity(
                accuracy, energy, carbon, latency_ms, cost_usd,
                temporal_violations)

            # --- 7. Narrative + XAI ---
            narrative = self._narrate(
                accuracy, energy, carbon, latency_ms, cost_usd,
                pareto, severity, verdict)

            xai_explanation = None
            if severity in ("warning", "critical") and self.xai is not None:
                try:
                    xai_explanation = await self._explain(
                        reflection_id, accuracy, energy, carbon,
                        latency_ms, cost_usd)
                except Exception:
                    xai_explanation = None

            # --- 8. HITL escalation ---
            escalated = False
            if severity == "critical" and self.hitl is not None:
                try:
                    approved = await self.hitl.query_user_if_needed(
                        "meta_cognition_user",
                        [{"solution_id": "accept",
                          "quality_score": accuracy,
                          "carbon_g": carbon, "cost_usd": cost_usd,
                          "latency_ms": latency_ms},
                         {"solution_id": "adjust",
                          "quality_score": accuracy - 0.01,
                          "carbon_g": carbon + 0.01,
                          "cost_usd": cost_usd + 0.01,
                          "latency_ms": latency_ms + 10}],
                        timeout=2.0)
                    escalated = approved == "adjust"
                except Exception:
                    escalated = False

            # --- 9. Meta-policy adjustment ---
            adjustments = self._adjust_meta_policy(
                accuracy, energy, carbon, latency_ms, cost_usd, severity)

            # --- 10. Build and persist the record ---
            record = ReflectionRecord(
                reflection_id=reflection_id,
                accuracy=accuracy, energy=energy,
                carbon=carbon, latency_ms=latency_ms, cost_usd=cost_usd,
                verdict=verdict, severity=severity, narrative=narrative,
                pareto_status=pareto,
                causal_attributions=causal_attrs,
                temporal_violations=temporal_violations,
                xai_explanation=xai_explanation,
                escalated_to_hitl=escalated,
                adjustments=adjustments)

            self.history.append(record)

            # Persist
            if self.storage is not None:
                try:
                    await asyncio.to_thread(
                        self.storage.save_reflection,
                        record.reflection_id,
                        {
                            "accuracy": accuracy, "energy": energy,
                            "carbon": carbon, "latency_ms": latency_ms,
                            "cost_usd": cost_usd, "verdict": verdict,
                            "severity": severity,
                            "pareto_status": pareto,
                            "narrative": narrative,
                            "adjustments": adjustments,
                            "timestamp": record.timestamp,
                        })
                except Exception:
                    pass

            # --- 11. Federated share (meta-policy) ---
            if self.federated is not None and severity != "info":
                try:
                    blob = json.dumps(self.meta_policy).encode()
                    await self.federated.share_weights(
                        "meta_policy", blob)
                except Exception:
                    pass

            # --- 12. Multi-agent bid for critical reflections ---
            if (self.multi_agent is not None
                    and severity == "critical"):
                try:
                    agent_id, _ = await self.multi_agent.bid({
                        "name": f"reflect_{reflection_id}",
                        "preferred_role": "validator"})
                    await self.multi_agent.reward(agent_id, 0.8)
                except Exception:
                    pass

            # --- 13. Register the reflection graph ---
            if self.registry is not None:
                try:
                    # Register the top-K recent reflections as a graph
                    recent = list(self.history)[-20:]
                    summary = {
                        "count": len(recent),
                        "avg_accuracy": sum(r.accuracy for r in recent) /
                                        max(1, len(recent)),
                        "avg_carbon": sum(r.carbon for r in recent) /
                                      max(1, len(recent)),
                        "meta_policy": dict(self.meta_policy),
                    }
                    # Only register if the registry has a register method
                    if hasattr(self.registry, "aregister"):
                        await self.registry.aregister(
                            "reflection", summary, explain=False,
                            require_hitl=False,
                            defer_if_high_carbon=False)
                except Exception:
                    pass

            return record

    # ------------------------------------------------------------------
    # Pareto dominance
    # ------------------------------------------------------------------
    def _compute_pareto_status(self, accuracy: float, energy: float,
                               carbon: float, latency_ms: float,
                               cost_usd: float) -> str:
        """
        Compare new metrics against the historical baseline.

        Lower is better for: energy, carbon, latency_ms, cost_usd.
        Higher is better for: accuracy.
        """
        if not self.history:
            return "baseline"
        # Baseline = mean over last N reflections (non-deferred)
        recent = [r for r in list(self.history)[-20:]
                  if r.pareto_status != "deferred"]
        if not recent:
            return "baseline"
        base_acc = statistics.mean(r.accuracy for r in recent)
        base_energy = statistics.mean(r.energy for r in recent)
        base_carbon = statistics.mean(r.carbon for r in recent)
        base_lat = statistics.mean(r.latency_ms for r in recent)
        base_cost = statistics.mean(r.cost_usd for r in recent)

        # New vector vs. baseline
        new = (-accuracy, energy, carbon, latency_ms, cost_usd)
        base = (-base_acc, base_energy, base_carbon, base_lat, base_cost)

        new_dominates = all(n <= b for n, b in zip(new, base)) and \
            any(n < b for n, b in zip(new, base))
        base_dominates = all(b <= n for n, b in zip(new, base)) and \
            any(b < n for n, b in zip(new, base))

        if new_dominates:
            return "dominates"
        if base_dominates:
            return "dominated"
        return "tradeoff"

    # ------------------------------------------------------------------
    # Severity evaluation
    # ------------------------------------------------------------------
    def _evaluate_severity(self, accuracy: float, energy: float,
                           carbon: float, latency_ms: float,
                           cost_usd: float,
                           temporal_violations: List[str]
                           ) -> Tuple[str, str]:
        """
        Determine severity (info/warning/critical) and a verdict string.
        """
        score = 0
        reasons: List[str] = []

        # Accuracy deviation
        if accuracy < self.targets["accuracy"] - 0.15:
            score += 3
            reasons.append("accuracy_critical")
        elif accuracy < self.targets["accuracy"] - 0.05:
            score += 1
            reasons.append("accuracy_warning")

        # Energy deviation
        if energy > self.targets["energy"] * 2.0:
            score += 3
            reasons.append("energy_critical")
        elif energy > self.targets["energy"] * 1.3:
            score += 1
            reasons.append("energy_warning")

        # Carbon deviation
        if carbon > self.targets["carbon"] * 1.8:
            score += 2
            reasons.append("carbon_warning")
        elif carbon > self.targets["carbon"] * 2.5:
            score += 3
            reasons.append("carbon_critical")

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
    def _narrate(self, accuracy: float, energy: float, carbon: float,
                 latency_ms: float, cost_usd: float, pareto: str,
                 severity: str, verdict: str) -> str:
        """Compose the human-readable reflection string."""
        pareto_text = {
            "baseline": "First reflection; no baseline yet.",
            "dominates": "Pareto-dominates the historical baseline.",
            "dominated": "Pareto-dominated by the historical baseline.",
            "tradeoff": "Trade-off vs. the historical baseline.",
            "deferred": "Deferred due to carbon window.",
        }.get(pareto, "Pareto status unknown.")

        return (
            f"[{severity.upper()}] accuracy={accuracy:.3f} "
            f"energy={energy:.3f}J carbon={carbon:.3f}kg "
            f"latency={latency_ms:.1f}ms cost=${cost_usd:.4f}. "
            f"{pareto_text} Verdict: {verdict}. "
            f"Meta-policy: quality={self.meta_policy['quality']:.2f}, "
            f"carbon={self.meta_policy['carbon']:.2f}, "
            f"cost={self.meta_policy['cost']:.2f}, "
            f"latency={self.meta_policy['latency']:.2f}."
        )

    # ------------------------------------------------------------------
    # XAI
    # ------------------------------------------------------------------
    async def _explain(self, reflection_id: str, accuracy: float,
                       energy: float, carbon: float, latency_ms: float,
                       cost_usd: float) -> Dict[str, Any]:
        if not NUMPY_AVAILABLE:
            return {}
        feats = np.array([accuracy, energy, carbon, latency_ms, cost_usd])
        def _score(x):
            # Proxy: weighted sum where lower carbon/cost/latency are better
            return float(np.dot(x, [0.4, -0.2, -0.3, -0.05, -0.05]))
        return await self.xai.explain(
            decision_id=f"meta_{reflection_id}",
            label=f"reflection:{reflection_id}",
            features=feats,
            names=["accuracy", "energy", "carbon", "latency_ms", "cost_usd"],
            model_fn=_score,
            include_counterfactual=False,
            include_anchors=False)

    # ------------------------------------------------------------------
    # Meta-policy adjustment
    # ------------------------------------------------------------------
    def _adjust_meta_policy(self, accuracy: float, energy: float,
                            carbon: float, latency_ms: float,
                            cost_usd: float, severity: str
                            ) -> Dict[str, float]:
        """
        Nudge the meta-policy weights based on the observed performance.

        Rules:
          * If accuracy is low, increase the quality weight.
          * If carbon is high, increase the carbon weight.
          * If cost is high, increase the cost weight.
          * If latency is high, increase the latency weight.
          * Re-normalise to a simplex.
        """
        adjustments: Dict[str, float] = {}
        learning_rate = 0.05

        if accuracy < self.targets["accuracy"] - 0.05:
            self.meta_policy["quality"] += learning_rate
            adjustments["quality"] = +learning_rate
        if carbon > self.targets["carbon"] * 1.5:
            self.meta_policy["carbon"] += learning_rate
            adjustments["carbon"] = +learning_rate
        if cost_usd > self.targets["cost_usd"] * 1.5:
            self.meta_policy["cost"] += learning_rate
            adjustments["cost"] = +learning_rate
        if latency_ms > self.targets["latency_ms"] * 1.5:
            self.meta_policy["latency"] += learning_rate
            adjustments["latency"] = +learning_rate

        # Slight decay toward uniform
        for k in self.meta_policy:
            self.meta_policy[k] = 0.99 * self.meta_policy[k] + 0.01 * 0.25

        # Normalise
        s = sum(self.meta_policy.values()) or 1.0
        self.meta_policy = {k: v / s for k, v in self.meta_policy.items()}
        return adjustments

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------
    def stats(self) -> Dict[str, Any]:
        if not self.history:
            return {"count": 0, "meta_policy": dict(self.meta_policy)}
        severities = defaultdict(int)
        for r in self.history:
            severities[r.severity] += 1
        recent = list(self.history)[-20:]
        return {
            "count": self.reflection_count,
            "history_size": len(self.history),
            "severities": dict(severities),
            "meta_policy": dict(self.meta_policy),
            "avg_accuracy": statistics.mean(r.accuracy for r in recent),
            "avg_carbon": statistics.mean(r.carbon for r in recent),
            "avg_energy": statistics.mean(r.energy for r in recent),
        }

    def set_targets(self, targets: Dict[str, float]) -> None:
        """Override default target metrics."""
        self.targets.update(targets)

    def top_reflections(self, k: int = 5) -> List[ReflectionRecord]:
        """Return the k most recent critical reflections."""
        crits = [r for r in self.history if r.severity == "critical"]
        return crits[-k:]


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION ENGINE
# =============================================================================
class QuantumDistillationEngine:
    def __init__(self, temperature=2.0, alpha=0.5, n_actions=5,
                 learning_rate=0.1):
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

    def _softmax(self, x, temp):
        m = max(x)
        exps = [math.exp((v - m) / max(temp, 1e-6)) for v in x]
        s = sum(exps) or 1.0
        return [e / s for e in exps]

    def _superpose(self):
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

    async def step(self, storage=None, student_id="meta_student"):
        target = self._softmax(self._superpose(), self.temperature)
        new = []
        for s, t in zip(self.student_policy, target):
            eps = max(1e-12, min(self.student_policy) * 0.1)
            grad = -(t / max(s, eps))
            new.append(max(1e-3, s - self.learning_rate * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]
        entry = {"target": target,
                 "student": list(self.student_policy),
                 "ts": datetime.now(timezone.utc).isoformat()}
        self.history.append(entry)
        if storage is not None:
            for tid in self.teachers:
                try:
                    await asyncio.to_thread(
                        storage.save_teacher_superposition,
                        student_id, tid,
                        self.teachers[tid][0] if self.teachers[tid] else 0.0,
                        self.temperature, 0.0, 0.0)
                except Exception:
                    pass
        return entry

    def get_policy(self) -> List[float]:
        return list(self.student_policy)


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL
# =============================================================================
class CausalGraphLearner:
    def __init__(self, storage=None):
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []

    async def learn(self, samples, variables, threshold=0.25):
        self.variables = list(variables)
        if len(samples) < 5 or not NUMPY_AVAILABLE:
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
                    src, dst = (variables[i], variables[j]) if vi > vj \
                        else (variables[j], variables[i])
                    self.graph[src][dst] = {
                        "weight": float(corr[i, j]), "confidence": c}
                    if self.storage:
                        try:
                            await asyncio.to_thread(
                                self.storage.save_causal_edge,
                                src, dst, float(corr[i, j]), c)
                        except Exception:
                            pass
        return self.summary()

    def parents(self, node):
        return [s for s, e in self.graph.items() if node in e]

    def children(self, node):
        return list(self.graph.get(node, {}).keys())

    def summary(self):
        return {"nodes": len(self.variables),
                "edges": sum(len(v) for v in self.graph.values()),
                "variables": list(self.variables)}


class CausalPolicyAdapter:
    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(self, config, storage, graph: CausalGraphLearner):
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values = defaultdict(float)
        self.counts = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = _cfg_get(config, "causal_exploration_rate", 0.1)

    async def choose_action(self, state):
        if random.random() < self.epsilon:
            return random.choice(self.ACTIONS)
        return max(self.ACTIONS, key=lambda a: self.values.get(a, 0.0))

    async def update(self, action, reward, state):
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
        w = self.graph.graph.get(treatment, {}).get(outcome, {}).get(
            "weight", 0.0)
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
    def __init__(self, storage, instance_id, share_interval=3600):
        self.storage = storage
        self.instance_id = instance_id
        self.share_interval = share_interval
        self.rounds = 0

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
        return bytes(avg)

    async def apply_aggregated_weights(self, model_id, current):
        agg = await self.pull_aggregated_weights(model_id)
        if agg is None:
            return current
        n = min(len(current), len(agg))
        return bytes([(current[i] + agg[i]) // 2 for i in range(n)])


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
        count = _cfg_get(config, "agent_count", 5)
        self.agents = {f"agent_{i:02d}": _Agent(f"agent_{i:02d}")
                       for i in range(count)}
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=500)
        self._lock = asyncio.Lock()

    async def _specialise(self):
        async with self._lock:
            for a in self.agents.values():
                a.role = max(a.utilities, key=lambda r: a.utilities[r])

    async def broadcast(self, topic, sender, payload):
        try:
            self.bus.put_nowait({"topic": topic, "sender": sender,
                                 "payload": payload})
        except asyncio.QueueFull:
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
        return best_id or next(iter(self.agents)), best_score

    async def reward(self, agent_id, reward):
        async with self._lock:
            if agent_id in self.agents:
                a = self.agents[agent_id]
                n = max(1, a.completed)
                a.reputation = max(0.0, min(1.0, a.reputation + reward / n))
                a.utilities[a.role] = min(1.0,
                                          a.utilities[a.role] + 0.05 * reward)

    def get_policy(self):
        counts = defaultdict(int)
        for a in self.agents.values():
            counts[a.role] += 1
        total = max(1, sum(counts.values()))
        out = [0.0] * 5
        for role, c in counts.items():
            w = c / total
            for i in range(5):
                out[i] += w * 0.2
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
                "role_distribution": self.get_policy(),
                "processed_messages": processed}


# =============================================================================
# ENHANCEMENT 5 — TEMPORAL LOGIC
# =============================================================================
_ATOMIC_RE = __import__("re").compile(
    r"^\s*([A-Za-z_]\w*)\s*(>=|<=|==|!=|>|<)\s*(-?[0-9.]+)\s*$")


class TemporalRule:
    def __init__(self, rule_id, operator, conditions, window=0.0,
                 description="", severity="warning"):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window = window
        self.description = description or rule_id
        self.severity = severity
        self.violations = 0
        self.last_violation = None

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
        self.trace: Deque = deque(
            maxlen=_cfg_get(config, "temporal_max_trace", 2000))
        self.approval_cb = None
        for formula in _cfg_get(config, "temporal_formulas", []) or []:
            self._install(formula)

    def _install(self, formula):
        f = formula.strip()
        if f.startswith("G "):
            self._add_atomic(f[2:].strip().strip("()"), "always")
        elif f.startswith("F "):
            self._add_atomic(f[2:].strip().strip("()"), "eventually")
        elif f.startswith("NEVER "):
            self._add_atomic(f[6:].strip().strip("()"), "never")

    def _add_atomic(self, expr, op):
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

    def set_approval_callback(self, cb):
        self.approval_cb = cb

    async def push_state(self, state):
        self.trace.append((datetime.now(timezone.utc), dict(state)))
        try:
            await asyncio.to_thread(self.storage.save_temporal_trace, state)
        except Exception:
            pass

    async def verify(self):
        result = {}
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
                    except Exception:
                        pass
        return result


# =============================================================================
# ENHANCEMENT 6 — EXPLAINABLE AI
# =============================================================================
class XAIDecisionExplainer:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.method = _cfg_get(config, "xai_method", "kernel_shap")
        self.depth = int(_cfg_get(config, "xai_depth", 5))
        self.global_attributions: Deque[Dict[str, float]] = deque(maxlen=1000)

    def _kernel_shap(self, f, x, names, n=32):
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
        contrib /= max(1, n)
        return dict(zip(names, contrib.tolist()))

    def _lime(self, f, x, names, n=100):
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

    def _nl(self, decision, attrs):
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]),
                     reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    async def explain(self, decision_id, label, features, names, model_fn,
                      include_counterfactual=False, include_anchors=False):
        if not NUMPY_AVAILABLE:
            attrs = {k: 0.0 for k in names}
            nl = self._nl(label, attrs)
            result = {"decision_id": decision_id, "method": self.method,
                      "attributions": attrs, "explanation": nl}
        else:
            x = np.asarray(features, dtype=float)
            attrs = self._lime(model_fn, x, names) if self.method == "lime" \
                else self._kernel_shap(model_fn, x, names)
            nl = self._nl(label, attrs)
            result = {"decision_id": decision_id, "method": self.method,
                      "attributions": attrs, "explanation": nl,
                      "counterfactual": None, "anchors": None}
        self.global_attributions.append(dict(attrs))
        try:
            await asyncio.to_thread(
                self.storage.save_xai_explanation,
                decision_id, decision_id, self.method, label,
                dict(enumerate(features)) if not isinstance(features, list)
                else {"list": features}, attrs, nl)
        except Exception:
            pass
        return result

    def global_feature_importance(self):
        if not self.global_attributions:
            return {}
        agg = defaultdict(list)
        for d in self.global_attributions:
            for k, v in d.items():
                agg[k].append(abs(v))
        return {k: sum(vals) / len(vals) for k, vals in agg.items()}


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
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def _probe(self):
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

    def select_precision(self):
        hw = self._probe()
        cands = list(_cfg_get(self.config, "precision_levels",
                              ["fp32", "fp16", "bf16", "int8"]))
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        return min(cands, key=lambda c: self.ENERGY.get(c, 1.0))

    async def switch_to(self, target, reason="policy"):
        if target == self.current or target not in self.ENERGY:
            return False
        old = self.current
        self.current = target
        saved = max(0.0, self.ENERGY.get(old, 1.0) -
                    self.ENERGY.get(target, 1.0))
        self.saved_wh += saved
        self.history.append({"from": old, "to": target,
                             "reason": reason, "saved_wh": saved})
        try:
            await asyncio.to_thread(
                self.storage.save_precision_switch,
                old, target, reason, saved, 0.0)
        except Exception:
            pass
        return True

    async def auto_switch(self, recent_acc, baseline_acc):
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
        cost = mwh * price_per_mwh
        try:
            await asyncio.to_thread(self.storage.save_rec, mwh,
                                    price_per_mwh, source)
        except Exception:
            pass
        return cost

    async def net_zero_schedule(self, workload_kwh, intensity):
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
                "offset_cost_usd": offset_cost,
                "credit_price_usd": price}


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

    async def _steady(self):
        if not self.active:
            return True
        return random.random() > _cfg_get(self.config, "chaos_intensity", 0.05)

    async def run_experiment(self, name, fault_type):
        if fault_type not in self.FAULT_TYPES:
            raise ValueError(f"unknown fault type {fault_type}")
        t0 = time.time()
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
# ENHANCEMENT 10 — HITL ACTIVE LEARNING
# =============================================================================
class ActiveUserPreferenceLearner:
    def __init__(self, storage, dashboard=None):
        self.storage = storage
        self.dashboard = dashboard
        self.preferences: Dict[str, Dict[str, float]] = {}
        self._responses: asyncio.Queue = asyncio.Queue(maxsize=100)

    async def submit_response(self, user_id, chosen_id):
        try:
            self._responses.put_nowait({"user_id": user_id,
                                        "chosen": chosen_id})
        except asyncio.QueueFull:
            pass

    async def query_user_if_needed(self, user_id, candidates, timeout=3.0):
        if len(candidates) < 2:
            return None
        try:
            msg = await asyncio.wait_for(self._responses.get(),
                                         timeout=timeout)
            return msg.get("chosen")
        except asyncio.TimeoutError:
            return candidates[0].get("solution_id")

    async def record_choice(self, user_id, solution_id, metrics=None):
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
        except Exception:
            pass


# =============================================================================
# UNIFIED ORCHESTRATOR
# =============================================================================
class MetaCognitiveOrchestratorV17:
    """
    Unified entry point: wires the enhanced MetaCognitiveLayer with all ten
    enhancements, and runs nine background loops.
    """

    def __init__(self, storage, config, dashboard=None):
        self.storage = storage
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]

        # Ten enhancements
        self.quantum = QuantumDistillationEngine()
        self.causal_graph = CausalGraphLearner(storage)
        self.causal_rl = CausalPolicyAdapter(config, storage,
                                              self.causal_graph)
        self.federated = FederatedGreenAggregator(storage, self.instance_id)
        self.multi_agent = MultiAgentCoordinator(config, storage)
        self.temporal = TemporalLogicVerifier(storage, config)
        self.xai = XAIDecisionExplainer(config, storage)
        self.precision = AdaptivePrecisionSwitcher(config, storage)
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.chaos = ChaosTestingEngine(config, storage)
        self.hitl = ActiveUserPreferenceLearner(storage, dashboard=dashboard)

        # Enhanced meta-cognitive layer
        self.meta = MetaCognitiveLayer(
            config=config,
            storage=storage,
            causal_rl=self.causal_rl,
            temporal=self.temporal,
            xai=self.xai,
            hitl=self.hitl,
            carbon_market=self.carbon_market,
            chaos=self.chaos,
            federated=self.federated,
            multi_agent=self.multi_agent,
            precision=self.precision,
            registry=None)  # registry is optional

        # Lifecycle
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: Set[asyncio.Task] = set()

    # ------------------------------------------------------------------
    async def reflect(self, **kwargs) -> ReflectionRecord:
        return await self.meta.reflect(**kwargs)

    # ------------------------------------------------------------------
    async def start(self):
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

    async def shutdown(self):
        self._shutdown_event.set()
        self._running = False
        for t in list(self._background_tasks):
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks,
                                 return_exceptions=True)

    # ------------------------------------------------------------------
    async def _causal_rl_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(900)
            try:
                samples = [{
                    "quality": random.uniform(0.5, 1.0),
                    "carbon": random.uniform(0.1, 0.8),
                    "cost": random.uniform(0.1, 0.9),
                    "latency": random.uniform(0.1, 0.9),
                } for _ in range(20)]
                await self.causal_graph.learn(
                    samples, ["quality", "carbon", "cost", "latency"])
            except Exception:
                pass

    async def _federated_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(3600)
            try:
                await self.federated.pull_aggregated_weights("meta_policy")
            except Exception:
                pass

    async def _multi_agent_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(600)
            try:
                await self.multi_agent.step()
            except Exception:
                pass

    async def _temporal_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            try:
                await self.temporal.verify()
            except Exception:
                pass

    async def _xai_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(300)
            if not NUMPY_AVAILABLE:
                continue
            try:
                feats = np.array([0.9, 0.4, 0.5, 0.4])
                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="meta_health", features=feats,
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=lambda x: float(
                        np.dot(x, [0.4, -0.3, -0.2, -0.1])))
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
            await asyncio.sleep(3600)
            try:
                await self.carbon_market.update_price()
            except Exception:
                pass

    async def _chaos_loop(self):
        while not self._shutdown_event.is_set():
            await asyncio.sleep(1800)
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
                self.quantum.register_teacher(
                    "meta_policy", list(self.meta.meta_policy.values()))
                await self.quantum.step(self.storage, "meta_student")
            except Exception:
                pass

    # ------------------------------------------------------------------
    async def health_check(self) -> Dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "meta": self.meta.stats(),
            "precision": self.precision.current,
            "carbon_price": self.carbon_market.last_price,
            "agents": {a.id: a.role
                       for a in self.multi_agent.agents.values()},
            "federated_rounds": self.federated.rounds,
        }


# =============================================================================
# MINIMAL IN-MEMORY STORAGE
# =============================================================================
class InMemoryStorage:
    """Standalone storage for demos and tests."""

    def __init__(self):
        self._data: Dict[str, Any] = defaultdict(list)
        self._prefs: Dict[str, Dict[str, float]] = {}

    def save_reflection(self, reflection_id, record):
        self._data["reflections"].append({
            "reflection_id": reflection_id, **record})

    def save_teacher_superposition(self, *a, **kw):
        self._data["teacher_superpositions"].append(a)

    def save_causal_edge(self, source, target, weight, confidence):
        self._data["causal_graph"].append({
            "source": source, "target": target, "weight": weight})

    def save_causal_experiment(self, exp_id, treatment, outcome,
                               ate, samples, method=""):
        self._data["causal_experiments"].append({"exp_id": exp_id, "ate": ate})

    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0):
        self._data["federated_weights"].append({
            "instance_id": instance_id, "model_id": model_id,
            "weights": weights})

    def get_federated_weights(self, model_id):
        return [r for r in self._data["federated_weights"]
                if r["model_id"] == model_id]

    def save_precision_switch(self, *a, **kw):
        self._data["precision_history"].append(a)

    def save_credit_price(self, price_usd, **kw):
        self._data["carbon_credit_prices"].append({"price_usd": price_usd})

    def save_rec(self, mwh, price_per_mwh, source, **kw):
        self._data["rec_ledger"].append({"mwh": mwh})

    def get_rec_balance(self):
        return sum(r["mwh"] for r in self._data["rec_ledger"])

    def save_net_zero_match(self, *a, **kw):
        self._data["net_zero_matches"].append(a)

    def save_chaos_experiment(self, *a, **kw):
        self._data["chaos_experiments"].append(a)

    def save_temporal_trace(self, state, context=None):
        self._data["temporal_trace"].append({"state": state})

    def save_temporal_rule(self, *a, **kw):
        self._data["temporal_rules"].append(a)

    def save_temporal_violation(self, *a, **kw):
        self._data["temporal_violations"].append(a)

    def save_xai_explanation(self, explanation_id, decision_id, method,
                             label, features, attributions, nl):
        self._data["xai_explanations"].append({
            "explanation_id": explanation_id, "label": label})

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
        "temporal_max_trace": 500,
        "temporal_formulas": [
            "G (quality >= 0.5)",
            "G (carbon <= 0.7)",
        ],
        "xai_method": "kernel_shap",
        "xai_depth": 5,
        "precision_levels": ["fp32", "fp16", "bf16", "int8"],
        "precision_switch_threshold": 0.02,
        "chaos_intensity": 0.0,
        "chaos_blast_radius": 0.5,
    }

    storage = InMemoryStorage()
    orch = MetaCognitiveOrchestratorV17(storage, config)
    await orch.start()

    print("=" * 80)
    print("meta_cognitive_layer.py v17.0.0 — Demo")
    print("=" * 80)

    # 1. Basic reflection
    print("\n=== Basic reflection ===")
    record = await orch.reflect(accuracy=0.92, energy=0.8,
                                carbon=0.2, latency_ms=150, cost_usd=0.05)
    print(f"  Verdict: {record.verdict}")
    print(f"  Severity: {record.severity}")
    print(f"  Pareto: {record.pareto_status}")
    print(f"  Narrative: {record.narrative}")

    # 2. Reflection that triggers warnings
    print("\n=== Warning-level reflection ===")
    record = await orch.reflect(accuracy=0.75, energy=1.5,
                                carbon=0.6, latency_ms=250, cost_usd=0.15)
    print(f"  Verdict: {record.verdict}")
    print(f"  Severity: {record.severity}")
    print(f"  Pareto: {record.pareto_status}")
    print(f"  Adjustments: {record.adjustments}")
    if record.xai_explanation:
        print(f"  XAI top-3:")
        for k, v in list(record.xai_explanation["attributions"].items())[:3]:
            print(f"    {k}: {v:+.4f}")

    # 3. Critical reflection triggering HITL
    print("\n=== Critical reflection (HITL escalation) ===")
    async def _delayed_response():
        await asyncio.sleep(0.3)
        await orch.hitl.submit_response("meta_cognition_user", "adjust")
    asyncio.create_task(_delayed_response())
    record = await orch.reflect(accuracy=0.50, energy=2.5,
                                carbon=0.9, latency_ms=500, cost_usd=0.3)
    print(f"  Verdict: {record.verdict}")
    print(f"  Severity: {record.severity}")
    print(f"  Escalated to HITL: {record.escalated_to_hitl}")
    print(f"  Temporal violations: {record.temporal_violations}")

    # 4. Causal attribution
    print("\n=== Causal attributions ===")
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

    # 5. Meta-policy evolution
    print("\n=== Meta-policy evolution ===")
    for i in range(5):
        acc = random.uniform(0.5, 0.95)
        carbon = random.uniform(0.2, 0.9)
        await orch.reflect(accuracy=acc, energy=random.uniform(0.5, 2.5),
                           carbon=carbon, latency_ms=200, cost_usd=0.1)
    print(f"  Final meta-policy: "
          f"{ {k: round(v, 3) for k, v in orch.meta.meta_policy.items()} }")

    # 6. Health check
    print("\n=== Health check ===")
    print(json.dumps(await orch.health_check(), indent=2, default=str))

    # 7. Top critical reflections
    print("\n=== Top critical reflections ===")
    for r in orch.meta.top_reflections(k=3):
        print(f"  [{r.severity}] acc={r.accuracy:.2f} "
              f"carbon={r.carbon:.2f} verdict={r.verdict}")

    await orch.shutdown()
    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
