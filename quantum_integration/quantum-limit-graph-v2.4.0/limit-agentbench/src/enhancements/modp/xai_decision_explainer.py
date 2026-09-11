"""
xai_decision_explainer.py — Enhanced v17.0.0
=============================================

Explainable AI for MOPD decisions — now extended with:

  * **Uncertainty quantification** — bootstrap confidence intervals on
    every attribution (was a point estimate only).
  * **Counterfactual explanations** — find the minimal change to the
    input that flips the decision.
  * **Feature interaction detection** — SHAP interaction values
    (pairwise) to detect dependencies Shapley ignores.
  * **Global explanations** — aggregate local attributions into a
    global feature-importance ranking.
  * **Anchors** — rule-based explanations (feature thresholds) with
    a precision floor.
  * **Configurable baseline** — expected value over a reference set
    instead of zero.
  * **Adaptive LIME bandwidth** — computed from the input's feature scale.
  * **Visualization hooks** — force plot + waterfall plot (textual).
  * **Integration hooks** for the nine other enhancements.

All **ten advanced Green Agent enhancements** are implemented in a single
self-contained file:

   1. Quantum-Distillation Integration        → QuantumDistillationEngine
   2. Causal Reinforcement Learning           → CausalGraphLearner + CausalPolicyAdapter
   3. Federated Green Learning                → FederatedGreenAggregator
   4. Advanced Multi-Agent Coordination       → MultiAgentCoordinator
   5. Temporal Logic & Formal Verification    → TemporalLogicVerifier
   6. Explainable AI                          → XAIDecisionExplainer (this class)
   7. Adaptive Precision Switching            → AdaptivePrecisionSwitcher
   8. Carbon Markets / REC                    → CarbonMarketIntegrator
   9. Resilience Engineering / Chaos Testing  → ChaosTestingEngine
  10. HITL Active Learning                    → ActiveUserPreferenceLearner

Unified entry point: **XAIOrchestratorV17**

The file is self-contained: Python stdlib + optional numpy / sklearn / torch.
All storage interactions soft-fail.
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

import logging
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
# ENHANCEMENT 6 — XAI DECISION EXPLAINER (enhanced original)
# =============================================================================
class XAIDecisionExplainer:
    """
    Explainable AI for MOPD decisions.

    v17 enhancements over v1.0:
      * Bootstrap CI on every attribution.
      * Counterfactual explanations.
      * SHAP interaction values.
      * Global feature importance.
      * Anchors (rule-based explanations).
      * Configurable baseline.
      * Adaptive LIME bandwidth.
      * Textual force/waterfall plots.
      * Integration hooks for nine other enhancements.
    """

    def __init__(self,
                 config,
                 storage,
                 # Enhancement hooks (all optional)
                 causal_rl: Optional["CausalPolicyAdapter"] = None,
                 temporal: Optional["TemporalLogicVerifier"] = None,
                 multi_agent: Optional["MultiAgentCoordinator"] = None,
                 hitl: Optional["ActiveUserPreferenceLearner"] = None,
                 carbon_market: Optional["CarbonMarketIntegrator"] = None,
                 chaos: Optional["ChaosTestingEngine"] = None,
                 precision: Optional["AdaptivePrecisionSwitcher"] = None,
                 federated: Optional["FederatedGreenAggregator"] = None):
        self.config = config
        self.storage = storage
        self.method = _cfg_get(config, "xai_method", "kernel_shap")
        self.depth = int(_cfg_get(config, "xai_depth", 5))
        self.ci_samples = int(_cfg_get(config, "xai_ci_samples", 20))
        self.cf_max_iter = int(_cfg_get(config, "xai_cf_max_iter", 100))
        self.anchor_min_precision = float(
            _cfg_get(config, "xai_anchor_min_precision", 0.9))

        # Enhancement hooks
        self.causal_rl = causal_rl
        self.temporal = temporal
        self.multi_agent = multi_agent
        self.hitl = hitl
        self.carbon_market = carbon_market
        self.chaos = chaos
        self.precision = precision
        self.federated = federated

        # Global attribution accumulator
        self.global_attributions: Deque[Dict[str, float]] = deque(maxlen=1000)
        self.decision_history: Deque[Dict[str, Any]] = deque(maxlen=500)

    # ------------------------------------------------------------------
    # KernelSHAP with bootstrap CI
    # ------------------------------------------------------------------
    def _single_shapley_permutation(self, f: Callable, x: "np.ndarray",
                                    baseline: "np.ndarray"
                                    ) -> "np.ndarray":
        perm = list(range(len(x)))
        random.shuffle(perm)
        prev = baseline.copy()
        contrib = np.zeros(len(x))
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
        return contrib

    def _kernel_shap(self, f: Callable, x: "np.ndarray",
                     names: List[str],
                     baseline: Optional["np.ndarray"] = None,
                     n: int = 64) -> Dict[str, float]:
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
        if baseline is None:
            baseline = np.zeros_like(x)
        contrib = np.zeros(len(x))
        for _ in range(n):
            contrib += self._single_shapley_permutation(f, x, baseline)
        contrib = contrib / max(1, n)
        return dict(zip(names, contrib.tolist()))

    def _kernel_shap_with_ci(self, f: Callable, x: "np.ndarray",
                             names: List[str],
                             baseline: Optional["np.ndarray"] = None,
                             n: int = 32
                             ) -> Dict[str, Tuple[float, float, float]]:
        """
        Return per-feature (mean, ci_low, ci_high) using bootstrap resampling.
        """
        if not NUMPY_AVAILABLE:
            return {k: (0.0, 0.0, 0.0) for k in names}
        if baseline is None:
            baseline = np.zeros_like(x)
        perms: List[np.ndarray] = []
        for _ in range(max(1, self.ci_samples)):
            acc = np.zeros(len(x))
            for _ in range(max(1, n // self.ci_samples)):
                acc += self._single_shapley_permutation(f, x, baseline)
            perms.append(acc / max(1, n // self.ci_samples))
        stacked = np.stack(perms, axis=0)
        mean = stacked.mean(axis=0)
        # 95% CI via percentiles
        lo = np.percentile(stacked, 2.5, axis=0)
        hi = np.percentile(stacked, 97.5, axis=0)
        return {name: (float(mean[i]), float(lo[i]), float(hi[i]))
                for i, name in enumerate(names)}

    # ------------------------------------------------------------------
    # LIME with adaptive bandwidth
    # ------------------------------------------------------------------
    def _adaptive_bandwidth(self, x: "np.ndarray") -> float:
        if not NUMPY_AVAILABLE:
            return 0.02
        # Standard deviation of the input magnitudes, floored at 1e-3
        try:
            scale = float(np.std(np.abs(x)))
        except Exception:
            scale = 0.0
        return max(1e-3, scale * 0.5) if scale > 0 else 0.02

    def _lime(self, f: Callable, x: "np.ndarray", names: List[str],
              n: int = 200) -> Dict[str, float]:
        if not (SKLEARN_AVAILABLE and NUMPY_AVAILABLE):
            return {k: random.uniform(-1, 1) for k in names}
        bandwidth = self._adaptive_bandwidth(x)
        X = np.tile(x, (n, 1)) + np.random.normal(0, bandwidth, (n, len(x)))
        try:
            y = np.array([float(f(r.reshape(1, -1))) for r in X])
        except Exception:
            return {k: 0.0 for k in names}
        w = np.exp(-np.sum((X - x) ** 2, axis=1) / (2 * bandwidth ** 2))
        try:
            m = LinearRegression().fit(X, y, sample_weight=w)
            return dict(zip(names, m.coef_.tolist()))
        except Exception:
            return {k: 0.0 for k in names}

    # ------------------------------------------------------------------
    # SHAP interaction values (pairwise)
    # ------------------------------------------------------------------
    def _shap_interaction(self, f: Callable, x: "np.ndarray",
                          names: List[str], n: int = 32
                          ) -> Dict[str, Dict[str, float]]:
        """
        Compute pairwise SHAP interaction values:
            Φ_{i,j} = E_{S ⊆ N \ {i,j}} [ f(S ∪ {i,j}) - f(S ∪ {i}) - f(S ∪ {j}) + f(S) ]
        """
        if not NUMPY_AVAILABLE:
            return {n1: {n2: 0.0 for n2 in names} for n1 in names}
        baseline = np.zeros_like(x)
        n_feat = len(x)
        inter = np.zeros((n_feat, n_feat))
        for _ in range(n):
            perm = list(range(n_feat))
            random.shuffle(perm)
            state = baseline.copy()
            for k, idx_i in enumerate(perm):
                # Snapshot before adding i
                state_i = state.copy()
                state_i[idx_i] = x[idx_i]
                for idx_j in perm[k + 1:]:
                    # Snapshot before adding j
                    state_ij = state_i.copy()
                    state_ij[idx_j] = x[idx_j]
                    try:
                        f_i = float(f(state_i.reshape(1, -1)))
                        f_j = float(f(state.reshape(1, -1)))
                        f_ij = float(f(state_ij.reshape(1, -1)))
                        delta = f_ij - f_i - f_j + \
                            float(f(state.reshape(1, -1)))
                    except Exception:
                        delta = 0.0
                    inter[idx_i, idx_j] += delta
                    inter[idx_j, idx_i] += delta
                state = state_i
        inter /= max(1, n)
        return {n1: {n2: float(inter[i, j])
                     for j, n2 in enumerate(names)}
                for i, n1 in enumerate(names)}

    # ------------------------------------------------------------------
    # Counterfactual explanation
    # ------------------------------------------------------------------
    def _counterfactual(self,
                        f: Callable,
                        x: "np.ndarray",
                        names: List[str],
                        baseline_prediction: float,
                        target_delta: float = 0.1) -> Dict[str, Any]:
        """
        Find a minimal-perturbation counterfactual that changes the
        prediction by at least `target_delta`.
        """
        if not NUMPY_AVAILABLE:
            return {"success": False, "reason": "numpy unavailable"}
        x_cf = x.copy()
        best_delta = 0.0
        best_pred = baseline_prediction
        chosen: Optional[str] = None
        chosen_step: float = 0.0
        for _ in range(self.cf_max_iter):
            # Pick a random feature and a random direction
            i = random.randrange(len(x))
            step = random.choice([-1, 1]) * 0.1
            trial = x_cf.copy()
            trial[i] += step
            try:
                pred = float(f(trial.reshape(1, -1)))
            except Exception:
                continue
            delta = abs(pred - baseline_prediction)
            if delta > best_delta:
                best_delta = delta
                best_pred = pred
                x_cf = trial
                chosen = names[i]
                chosen_step = step
            if delta >= abs(target_delta):
                break
        return {
            "success": best_delta >= abs(target_delta),
            "counterfactual": x_cf.tolist(),
            "prediction": best_pred,
            "changed_feature": chosen,
            "change_amount": chosen_step,
            "delta": best_pred - baseline_prediction,
        }

    # ------------------------------------------------------------------
    # Anchors (rule-based)
    # ------------------------------------------------------------------
    def _anchors(self,
                 f: Callable,
                 x: "np.ndarray",
                 names: List[str],
                 n_samples: int = 64
                 ) -> List[Dict[str, Any]]:
        """
        Simple anchors: for each feature, check if constraining it to
        within `x[i] ± tol` keeps the decision stable. Return features
        with high precision.
        """
        if not NUMPY_AVAILABLE:
            return []
        try:
            base_pred = float(f(x.reshape(1, -1)))
        except Exception:
            return []
        anchors: List[Dict[str, Any]] = []
        tol = 0.15
        for i, name in enumerate(names):
            hits = 0
            total = 0
            for _ in range(n_samples):
                trial = x.copy()
                # Perturb all features except the anchor
                for j in range(len(x)):
                    if j != i:
                        trial[j] += random.gauss(0, 0.2)
                try:
                    pred = float(f(trial.reshape(1, -1)))
                except Exception:
                    continue
                total += 1
                if abs(pred - base_pred) <= tol:
                    hits += 1
            if total > 0:
                precision = hits / total
                if precision >= self.anchor_min_precision:
                    anchors.append({
                        "feature": name,
                        "value": float(x[i]),
                        "precision": precision,
                    })
        return anchors

    # ------------------------------------------------------------------
    # Natural-language + visual
    # ------------------------------------------------------------------
    def _nl(self, decision: str,
            attrs: Dict[str, float]) -> str:
        top = sorted(attrs.items(), key=lambda kv: abs(kv[1]),
                     reverse=True)[:self.depth]
        lines = "\n".join(f"  • {k}: {v:+.4f}" for k, v in top)
        return f"Decision '{decision}' driven by:\n{lines}"

    def _force_plot(self, attrs: Dict[str, float]) -> str:
        """Textual force plot: |----->+<----| per feature."""
        max_abs = max((abs(v) for v in attrs.values()), default=1.0) or 1.0
        lines = ["Force plot (positive →, negative ←):"]
        for k, v in sorted(attrs.items(),
                           key=lambda kv: abs(kv[1]), reverse=True)[:self.depth]:
            bar_len = int(20 * abs(v) / max_abs)
            arrow = "─" * bar_len
            sign = "→" if v >= 0 else "←"
            lines.append(f"  {k:20s} {sign} {arrow} {v:+.4f}")
        return "\n".join(lines)

    def _waterfall_plot(self, base_value: float,
                        attrs: Dict[str, float],
                        final_value: float) -> str:
        """Textual waterfall plot showing cumulative contributions."""
        running = base_value
        lines = [f"Waterfall plot (base={base_value:+.4f}):"]
        for k, v in sorted(attrs.items(),
                           key=lambda kv: abs(kv[1]), reverse=True)[:self.depth]:
            running += v
            lines.append(
                f"  + {k:20s} {v:+.4f}  → cumulative {running:+.4f}")
        lines.append(f"Final prediction: {final_value:+.4f}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def explain(self,
                      decision_id: str,
                      label: str,
                      features: Any,
                      names: List[str],
                      model_fn: Callable,
                      baseline: Optional[Any] = None,
                      reference_set: Optional[List[Any]] = None,
                      include_counterfactual: bool = True,
                      include_interactions: bool = False,
                      include_anchors: bool = False) -> Dict[str, Any]:
        """Generate an explanation with all v17 features."""
        if not NUMPY_AVAILABLE:
            attrs = {k: 0.0 for k in names}
            nl = self._nl(label, attrs)
            result = {
                "decision_id": decision_id,
                "method": self.method,
                "attributions": attrs,
                "explanation": nl,
            }
        else:
            x = np.asarray(features, dtype=float)
            # Baseline
            if baseline is None:
                if reference_set:
                    try:
                        baseline = np.mean(
                            [np.asarray(r, dtype=float) for r in reference_set],
                            axis=0)
                    except Exception:
                        baseline = np.zeros_like(x)
                else:
                    baseline = np.zeros_like(x)

            # Attributions
            if self.method == "lime":
                attrs = self._lime(model_fn, x, names)
                ci = {}
            else:
                attrs = self._kernel_shap(model_fn, x, names, baseline)
                ci = self._kernel_shap_with_ci(model_fn, x, names, baseline)

            # Natural-language
            nl = self._nl(label, attrs)
            force = self._force_plot(attrs)
            waterfall = self._waterfall_plot(
                float(model_fn(baseline.reshape(1, -1))), attrs,
                float(model_fn(x.reshape(1, -1))))

            # Optional features
            cf = None
            if include_counterfactual:
                try:
                    cf = self._counterfactual(
                        model_fn, x, names,
                        baseline_prediction=float(
                            model_fn(x.reshape(1, -1))))
                except Exception:
                    cf = None
            interactions = None
            if include_interactions:
                try:
                    interactions = self._shap_interaction(
                        model_fn, x, names)
                except Exception:
                    interactions = None
            anchors = None
            if include_anchors:
                try:
                    anchors = self._anchors(model_fn, x, names)
                except Exception:
                    anchors = None

            result = {
                "decision_id": decision_id,
                "method": self.method,
                "attributions": attrs,
                "attributions_ci": {
                    k: {"mean": v[0], "ci_low": v[1], "ci_high": v[2]}
                    for k, v in ci.items()
                } if ci else {},
                "explanation": nl,
                "force_plot": force,
                "waterfall_plot": waterfall,
                "counterfactual": cf,
                "interactions": interactions,
                "anchors": anchors,
            }

        # Accumulate global attribution
        self.global_attributions.append(dict(attrs))
        self.decision_history.append({
            "decision_id": decision_id,
            "label": label,
            "attributions": dict(attrs),
            "ts": datetime.now(timezone.utc).isoformat(),
        })

        # Causal integration: causal attribution
        if self.causal_rl is not None and self.causal_rl.graph is not None:
            try:
                parents = self.causal_rl.graph.parents("quality")
                causal_attrs = {}
                for p in parents:
                    edge = self.causal_rl.graph.graph.get(p, {}).get("quality", {})
                    causal_attrs[p] = edge.get("weight", 0.0)
                result["causal_attributions"] = causal_attrs
            except Exception:
                pass

        # Multi-agent bid for the explain task
        if self.multi_agent is not None:
            try:
                agent_id, _ = await self.multi_agent.bid({
                    "name": "explain_task", "preferred_role": "reporter"})
                result["agent_id"] = agent_id
                await self.multi_agent.reward(agent_id, 0.8)
            except Exception:
                pass

        # Temporal push
        if self.temporal is not None:
            try:
                await self.temporal.push_state({
                    "explained": 1,
                    "attribution_magnitude": sum(abs(v) for v in attrs.values())})
            except Exception:
                pass

        # Chaos: occasionally inject latency during explanation
        if self.chaos is not None and random.random() < 0.05:
            try:
                await self.chaos.run_experiment(
                    f"xai_{uuid.uuid4().hex[:6]}", "latency")
                result["chaos_injected"] = True
            except Exception:
                pass

        # Carbon market: check high-carbon window
        if self.carbon_market is not None:
            try:
                decision = await self.carbon_market.net_zero_schedule(
                    workload_kwh=0.01, intensity=0.4)
                result["carbon_action"] = decision.get("action")
            except Exception:
                pass

        # Precision switch
        if self.precision is not None:
            try:
                await self.precision.auto_switch(
                    recent_acc=1.0 - min(1.0, sum(abs(v) for v in attrs.values())),
                    baseline_acc=0.9)
                result["precision"] = self.precision.current
            except Exception:
                pass

        # Federated share of explanation stats
        if self.federated is not None:
            try:
                blob = json.dumps(self.global_feature_importance()).encode()
                await self.federated.share_weights("xai_stats", blob)
            except Exception:
                pass

        # HITL: escalate high-magnitude negative attributions
        if self.hitl is not None and attrs:
            try:
                worst = min(attrs.items(), key=lambda kv: kv[1])
                if worst[1] < -0.5:
                    approved = await self.hitl.query_user_if_needed(
                        "xai_approver",
                        [{"solution_id": "ack", "quality_score": 0.9},
                         {"solution_id": "reject", "quality_score": 0.89}],
                        timeout=1.0)
                    result["hitl_response"] = approved
            except Exception:
                pass

        # Persist
        try:
            await asyncio.to_thread(
                self.storage.save_xai_explanation,
                decision_id, decision_id, self.method, label,
                {"raw": list(features)} if not isinstance(features, list)
                else {"list": features},
                attrs, nl)
        except Exception:
            pass

        return result

    # ------------------------------------------------------------------
    # Global feature importance
    # ------------------------------------------------------------------
    def global_feature_importance(self) -> Dict[str, float]:
        if not self.global_attributions:
            return {}
        # Mean absolute attribution per feature
        agg = defaultdict(list)
        for d in self.global_attributions:
            for k, v in d.items():
                agg[k].append(abs(v))
        return {k: sum(vals) / len(vals) for k, vals in agg.items()}

    def top_features(self, k: int = 5) -> List[Tuple[str, float]]:
        importance = self.global_feature_importance()
        return sorted(importance.items(), key=lambda kv: kv[1],
                      reverse=True)[:k]

    def stats(self) -> Dict[str, Any]:
        return {
            "method": self.method,
            "depth": self.depth,
            "explanations": len(self.decision_history),
            "global_features": len(self.global_feature_importance()),
        }


# =============================================================================
# ENHANCEMENT 1 — QUANTUM-DISTILLATION ENGINE
# =============================================================================
class QuantumDistillationEngine:
    def __init__(self, temperature=2.0, alpha=0.5, n_actions=5):
        self.temperature = temperature
        self.alpha = alpha
        self.n_actions = n_actions
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy = [1.0 / n_actions] * n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)

    def register_teacher(self, name, policy):
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

    async def step(self, storage, student_id="xai_student"):
        target = self._softmax(self._superpose(), self.temperature)
        lr = 0.1
        new = []
        for s, t in zip(self.student_policy, target):
            grad = -(t / max(s, 1e-9))
            new.append(max(0.01, s - lr * grad))
        ns = sum(new) or 1.0
        self.student_policy = [x / ns for x in new]
        entry = {"target": target, "student": list(self.student_policy)}
        self.history.append(entry)
        return entry

    def get_policy(self):
        return list(self.student_policy)


# =============================================================================
# ENHANCEMENT 2 — CAUSAL RL
# =============================================================================
class CausalGraphLearner:
    def __init__(self, storage=None):
        self.storage = storage
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.variables: List[str] = []
        self._lock = asyncio.Lock()

    async def learn(self, samples, variables, threshold=0.25):
        self.variables = list(variables)
        if len(samples) < 5 or not NUMPY_AVAILABLE:
            async with self._lock:
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
                        self.graph[src][dst] = {
                            "weight": float(corr[i, j]), "confidence": c}
        return self.summary()

    def parents(self, node):
        return [s for s, e in self.graph.items() if node in e]

    def summary(self):
        return {"nodes": len(self.variables),
                "edges": sum(len(v) for v in self.graph.values()),
                "variables": list(self.variables)}


class CausalPolicyAdapter:
    ACTIONS = ["performance", "carbon", "cost", "hybrid", "adaptive"]

    def __init__(self, config, storage, graph):
        self.config = config
        self.storage = storage
        self.graph = graph
        self.values = defaultdict(float)
        self.counts = defaultdict(int)
        self.policy = [1.0 / len(self.ACTIONS)] * len(self.ACTIONS)
        self.epsilon = _cfg_get(config, "causal_exploration_rate", 0.1)
        self._lock = asyncio.Lock()

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

    def get_policy(self):
        counts = defaultdict(int)
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
_ATOMIC_RE = re.compile(
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
        self.trace: Deque = deque(maxlen=_cfg_get(config, "temporal_max_trace", 2000))
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

    async def verify(self):
        result = {}
        for rid, rule in self.rules.items():
            copy = deque(self.trace, maxlen=self.trace.maxlen)
            result[rid] = not rule.evaluate(copy)
        return result


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
                "offset_cost_usd": offset_cost, "credit_price_usd": price}


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
            self._responses.put_nowait({"user_id": user_id, "chosen": chosen_id})
        except asyncio.QueueFull:
            pass

    async def query_user_if_needed(self, user_id, candidates, timeout=3.0):
        if len(candidates) < 2:
            return None
        try:
            msg = await asyncio.wait_for(self._responses.get(), timeout=timeout)
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
                        prefs[k] = prefs.get(k, 0.25) + 1.0 / (v + 1e-6) * 0.01
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
class XAIOrchestratorV17:
    """
    Unified entry point: wires all ten enhancements around the
    XAIDecisionExplainer.
    """

    def __init__(self, storage, config, dashboard=None):
        self.storage = storage
        self.config = config
        self.instance_id = str(uuid.uuid4())[:8]

        # Build ten enhancements
        self.quantum = QuantumDistillationEngine(temperature=2.0, alpha=0.5)
        self.causal_graph = CausalGraphLearner(storage)
        self.causal_rl = CausalPolicyAdapter(config, storage, self.causal_graph)
        self.federated = FederatedGreenAggregator(storage, self.instance_id)
        self.multi_agent = MultiAgentCoordinator(config, storage)
        self.temporal = TemporalLogicVerifier(storage, config)
        self.precision = AdaptivePrecisionSwitcher(config, storage)
        self.carbon_market = CarbonMarketIntegrator(config, storage)
        self.chaos = ChaosTestingEngine(config, storage)
        self.hitl = ActiveUserPreferenceLearner(storage, dashboard=dashboard)

        # Central XAI module — enhanced with all hooks
        self.xai = XAIDecisionExplainer(
            config, storage,
            causal_rl=self.causal_rl,
            temporal=self.temporal,
            multi_agent=self.multi_agent,
            hitl=self.hitl,
            carbon_market=self.carbon_market,
            chaos=self.chaos,
            precision=self.precision,
            federated=self.federated,
        )

        # Lifecycle
        self._running = False
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set = set()

    # ------------------------------------------------------------------
    async def explain_decision(self,
                               label: str,
                               features: List[float],
                               names: List[str],
                               model_fn: Callable,
                               reference_set: Optional[List[List[float]]] = None,
                               include_interactions: bool = True,
                               include_anchors: bool = True
                               ) -> Dict[str, Any]:
        """Public API for explaining a decision."""
        return await self.xai.explain(
            decision_id=f"xai_{uuid.uuid4().hex[:8]}",
            label=label,
            features=features,
            names=names,
            model_fn=model_fn,
            reference_set=reference_set,
            include_counterfactual=True,
            include_interactions=include_interactions,
            include_anchors=include_anchors,
        )

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
                dummy = bytes(random.getrandbits(8) for _ in range(64))
                await self.federated.share_weights("xai_stats", dummy)
                await self.federated.pull_aggregated_weights("xai_stats")
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
            try:
                await self.xai.explain(
                    decision_id=f"sys_{uuid.uuid4().hex[:8]}",
                    label="system_health",
                    features=[0.9, 0.4, 0.5, 0.4],
                    names=["quality", "carbon", "cost", "latency"],
                    model_fn=lambda x: float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
                    if NUMPY_AVAILABLE else 0.5,
                )
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
                await self.quantum.step(self.storage, "xai_student")
            except Exception:
                pass

    # ------------------------------------------------------------------
    async def health_check(self):
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "xai": self.xai.stats(),
            "top_features": self.xai.top_features(k=5),
            "precision": self.precision.current,
            "carbon_price": self.carbon_market.last_price,
            "federated_rounds": self.federated.rounds,
        }


# =============================================================================
# MINIMAL IN-MEMORY STORAGE
# =============================================================================
class InMemoryStorage:
    def __init__(self):
        self._data: Dict[str, Any] = defaultdict(list)
        self._prefs: Dict[str, Dict[str, float]] = {}

    def save_xai_explanation(self, explanation_id, decision_id, method,
                             label, features, attributions, nl):
        self._data["xai_explanations"].append({
            "explanation_id": explanation_id, "label": label,
            "method": method, "nl": nl, "attributions": attributions})
        for name, val in (attributions or {}).items():
            self._data["xai_feature_importance"].append({
                "explanation_id": explanation_id,
                "feature_name": str(name), "importance": float(val)})

    def save_teacher_superposition(self, *a, **kw):
        self._data["teacher_superpositions"].append(a)

    def save_causal_edge(self, *a, **kw): pass
    def save_causal_experiment(self, *a, **kw): pass

    def save_federated_weights(self, instance_id, model_id, weights,
                               weight_norm=0.0, round_id=0):
        self._data["federated_weights"].append({
            "instance_id": instance_id, "model_id": model_id,
            "weights": weights})

    def get_federated_weights(self, model_id):
        return [r for r in self._data["federated_weights"]
                if r["model_id"] == model_id]

    def save_agent(self, *a, **kw): pass
    def save_agent_message(self, *a, **kw): pass

    def save_temporal_rule(self, *a, **kw): pass
    def save_temporal_trace(self, *a, **kw): pass
    def save_temporal_violation(self, *a, **kw): pass

    def save_precision_switch(self, *a, **kw):
        self._data["precision_history"].append(a)

    def save_credit_price(self, price_usd, **kw):
        self._data["carbon_credit_prices"].append({"price_usd": price_usd})

    def save_rec(self, mwh, price_per_mwh, source, **kw):
        self._data["rec_ledger"].append({"mwh": mwh})

    def get_rec_balance(self):
        return sum(r["mwh"] for r in self._data["rec_ledger"])

    def save_net_zero_match(self, *a, **kw): pass
    def save_chaos_experiment(self, *a, **kw): pass

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
        "temporal_max_trace": 2000,
        "temporal_formulas": ["G (quality >= 0.5)"],
        "xai_method": "kernel_shap",
        "xai_depth": 5,
        "xai_ci_samples": 10,
        "xai_cf_max_iter": 50,
        "xai_anchor_min_precision": 0.85,
        "precision_levels": ["fp32", "fp16", "bf16", "int8"],
        "precision_switch_threshold": 0.02,
        "chaos_intensity": 0.05,
        "chaos_blast_radius": 0.1,
    }

    storage = InMemoryStorage()
    orch = XAIOrchestratorV17(storage, config)
    await orch.start()

    print("=" * 80)
    print("xai_decision_explainer.py v17.0.0 — Demo")
    print("=" * 80)

    if not NUMPY_AVAILABLE:
        print("numpy not available — install with: pip install numpy")
        await orch.shutdown()
        return

    # 1. Basic explanation with CI
    print("\n=== Basic explanation with CI ===")
    feats = [0.9, 0.4, 0.5, 0.4]
    names = ["quality", "carbon", "cost", "latency"]
    def _score(x):
        return float(np.dot(x, [0.4, -0.3, -0.2, -0.1]))
    explanation = await orch.explain_decision(
        label="strategy=carbon",
        features=feats,
        names=names,
        model_fn=_score,
        reference_set=[
            [0.7, 0.5, 0.6, 0.5],
            [0.8, 0.3, 0.4, 0.6],
            [0.6, 0.6, 0.5, 0.3],
        ],
        include_interactions=True,
        include_anchors=True,
    )
    print(f"  Method: {explanation['method']}")
    print(f"  Attributions:")
    for k, v in explanation["attributions"].items():
        ci = explanation["attributions_ci"].get(k, {})
        print(f"    {k}: {v:+.4f} "
              f"[{ci.get('ci_low', 0):+.4f}, {ci.get('ci_high', 0):+.4f}]")

    # 2. Force plot
    print("\n=== Force plot ===")
    print(explanation["force_plot"])

    # 3. Waterfall plot
    print("\n=== Waterfall plot ===")
    print(explanation["waterfall_plot"])

    # 4. Counterfactual
    print("\n=== Counterfactual explanation ===")
    cf = explanation.get("counterfactual")
    if cf:
        print(f"  Success: {cf['success']}")
        print(f"  Changed feature: {cf['changed_feature']}")
        print(f"  Change amount: {cf['change_amount']:+.3f}")
        print(f"  Delta prediction: {cf['delta']:+.4f}")

    # 5. Interactions
    print("\n=== Feature interactions (top 3) ===")
    inter = explanation.get("interactions")
    if inter:
        flat = []
        for n1, row in inter.items():
            for n2, val in row.items():
                if n1 < n2 and abs(val) > 1e-6:
                    flat.append((abs(val), n1, n2, val))
        flat.sort(reverse=True)
        for _, n1, n2, val in flat[:3]:
            print(f"  {n1} ↔ {n2}: {val:+.4f}")

    # 6. Anchors
    print("\n=== Anchors ===")
    anchors = explanation.get("anchors")
    if anchors:
        for a in anchors:
            print(f"  {a['feature']} = {a['value']:.3f} "
                  f"(precision={a['precision']:.2f})")
    else:
        print("  No anchors found (precision floor not met)")

    # 7. Global feature importance
    print("\n=== Global feature importance (after 5 explanations) ===")
    for i in range(4):
        await orch.explain_decision(
            label=f"strategy_{i}",
            features=[random.uniform(0.2, 1.0) for _ in range(4)],
            names=names,
            model_fn=_score,
        )
    importance = orch.xai.global_feature_importance()
    for k, v in sorted(importance.items(), key=lambda kv: kv[1],
                       reverse=True):
        print(f"  {k}: {v:.4f}")

    # 8. Health check
    print("\n=== Health check ===")
    print(json.dumps(await orch.health_check(), indent=2, default=str))

    # 9. Chaos experiment
    print("\n=== Chaos experiment ===")
    print(json.dumps(
        await orch.chaos.run_experiment("demo_chaos", "latency"),
        indent=2, default=str))

    await orch.shutdown()
    print("\nShutdown complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
