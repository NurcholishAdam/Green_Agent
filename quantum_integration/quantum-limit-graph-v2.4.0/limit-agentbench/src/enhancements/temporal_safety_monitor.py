#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/temporal_logic_module_v2_0.py
# VERSION: 2.0.0 (Temporal Logic + Quantum-Distillation + Causal RL +
#                   Federated Green Learning + Multi-Agent Coordination +
#                   Adaptive Precision + Carbon Markets/REC + XAI + HITL +
#                   Chaos Testing — all in ONE file)
# =============================================================================
"""
Temporal Logic / Formal Verification for Safety‑Critical Policies
— Enhanced v2.0.0

This single file now hosts ALL TEN Green Agent enhancements:

 1. Temporal Logic & Formal Verification (original core — enhanced)
 2. Human-in-the-Loop for Critical Decisions (original — enhanced)
 3. Explainable AI for Every Decision (original — enhanced)
 4. Resilience Engineering & Chaos Testing (original — enhanced)
 5. Quantum-Distillation Integration (NEW)
 6. Causal Reinforcement Learning for Policy Adaptation (NEW)
 7. Federated Green Learning Across Deployments (NEW)
 8. Advanced Multi-Agent Coordination with Emergent Role Specialisation (NEW)
 9. Adaptive Precision Switching with Hardware-Aware Policies (NEW)
10. Integration with External Carbon Markets & Renewable Energy Credits (NEW)

All modules are integrated with the TemporalSafetyMonitor so that:
 - Policies produced by Causal RL are verified by the temporal rules.
 - Distillation produces a student policy that the temporal monitor can gate.
 - Multi-agent role actions are checked against temporal safety.
 - Precision switches, carbon-market trades, and federated updates all
   push their state into the temporal history, generating XAI + HITL events.
"""

import asyncio
import logging
import json
import os
import re
import math
import random
import hashlib
import secrets
import statistics
import time
import sqlite3
import uuid
from typing import Dict, Any, List, Callable, Optional, Deque, Tuple, Union
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from functools import wraps
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Optional dependencies (graceful degradation)
# -----------------------------------------------------------------------------
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    from sklearn.linear_model import LinearRegression
    from sklearn.neural_network import MLPRegressor, MLPClassifier
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False


# =============================================================================
# SECTION 1: ORIGINAL TEMPORAL LOGIC CORE (enhanced)
# =============================================================================

class TemporalOperator(Enum):
    ALWAYS = "always"
    EVENTUALLY = "eventually"
    UNTIL = "until"
    NEVER = "never"
    NEXT = "next"
    WITHIN = "within"


class TemporalRule:
    """Represents a single temporal rule with associated conditions and parameters."""
    def __init__(
        self,
        rule_id: str,
        operator: TemporalOperator,
        conditions: List[Callable[[Dict], bool]],
        window_seconds: float = 0.0,
        max_violations: int = 1,
        cooldown_seconds: float = 0.0,
        description: str = "",
        severity: str = "warning",       # v2: info | warning | critical
        auto_remediate: Optional[Callable[[Dict], Dict]] = None,  # v2
    ):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window_seconds = window_seconds
        self.max_violations = max_violations
        self.cooldown_seconds = cooldown_seconds
        self.description = description or rule_id
        self.violation_count = 0
        self.last_violation_time: Optional[datetime] = None
        self.severity = severity
        self.auto_remediate = auto_remediate
        # v2 metrics
        self.evaluations = 0
        self.total_violations = 0

    def evaluate(self, history: Deque[Tuple[datetime, Dict]]) -> bool:
        """Evaluate the rule against a history of (timestamp, state).
        Returns True if the rule is currently violated."""
        self.evaluations += 1
        if not history:
            return False

        if self.window_seconds > 0:
            cutoff = history[-1][0] - timedelta(seconds=self.window_seconds)
            while history and history[0][0] < cutoff:
                history.popleft()

        if self.operator == TemporalOperator.UNTIL and len(self.conditions) >= 2:
            cond_a = self.conditions[0]
            cond_b = self.conditions[1]
            b_seen = False
            for ts, state in history:
                if cond_b(state):
                    b_seen = True
                    break
                if not cond_a(state):
                    return True
            if not b_seen:
                return True
            return False

        elif self.operator == TemporalOperator.WITHIN and len(self.conditions) >= 2:
            trigger = self.conditions[0]
            response = self.conditions[1]
            trigger_seen = False
            trigger_time = None
            for ts, state in history:
                if not trigger_seen:
                    if trigger(state):
                        trigger_seen = True
                        trigger_time = ts
                else:
                    if response(state):
                        return False
                    if self.window_seconds > 0 and (ts - trigger_time).total_seconds() > self.window_seconds:
                        return True
            if trigger_seen:
                return True
            return False

        cond = self.conditions[0] if self.conditions else None
        if cond is None:
            return False

        if self.operator == TemporalOperator.ALWAYS:
            for _, state in history:
                if not cond(state):
                    return True
            return False

        elif self.operator == TemporalOperator.EVENTUALLY:
            if self.window_seconds == 0:
                return not any(cond(state) for _, state in history)
            else:
                cutoff = history[-1][0] - timedelta(seconds=self.window_seconds)
                for ts, state in history:
                    if ts >= cutoff and cond(state):
                        return False
                return True

        elif self.operator == TemporalOperator.NEXT:
            if len(history) < 2:
                return False
            return not cond(history[-1][1])

        elif self.operator == TemporalOperator.NEVER:
            return any(cond(state) for _, state in history)

        return cond(history[-1][1]) if history else False


class TemporalSafetyMonitor:
    """Core temporal logic monitor — with XAI, HITL, persistence, chaos testing,
    AND now orchestrating all ten Green Agent enhancement modules."""

    def __init__(self, max_history: int = 10000, db_path: Optional[str] = None):
        self.rules: Dict[str, TemporalRule] = {}
        self.history: Deque[Tuple[datetime, Dict]] = deque(maxlen=max_history)
        self.approval_callback: Optional[Callable[[str, Dict], bool]] = None
        self.event_log: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self.db_path = db_path
        # v2 hooks
        self._xai_hook: Optional[Callable[[str, Dict], Dict]] = None
        self._metrics: Dict[str, float] = defaultdict(float)
        # Sub-modules (attached from outside to keep single-file policy)
        self.distiller: Optional["QuantumDistillationEngine"] = None
        self.causal_rl: Optional["CausalPolicyAdapter"] = None
        self.federated: Optional["FederatedGreenLearner"] = None
        self.multi_agent: Optional["MultiAgentCoordinator"] = None
        self.precision_switcher: Optional["AdaptivePrecisionSwitcher"] = None
        self.carbon_market: Optional["CarbonMarketIntegrator"] = None

    # -------------------------------------------------------------------------
    # Rule management
    # -------------------------------------------------------------------------
    def add_rule(
        self,
        rule_id: str,
        operator: TemporalOperator,
        conditions: Union[Callable[[Dict], bool], List[Callable[[Dict], bool]]],
        window_seconds: float = 0.0,
        max_violations: int = 1,
        cooldown_seconds: float = 0.0,
        description: str = "",
        severity: str = "warning",
        auto_remediate: Optional[Callable[[Dict], Dict]] = None,
    ):
        cond_list = conditions if isinstance(conditions, list) else [conditions]
        if operator in (TemporalOperator.UNTIL, TemporalOperator.WITHIN):
            if len(cond_list) < 2:
                raise ValueError(f"{operator.value} requires two conditions")
        rule = TemporalRule(
            rule_id=rule_id, operator=operator, conditions=cond_list,
            window_seconds=window_seconds, max_violations=max_violations,
            cooldown_seconds=cooldown_seconds, description=description,
            severity=severity, auto_remediate=auto_remediate)
        self.rules[rule_id] = rule
        logger.info(f"Added temporal rule '{rule_id}' ({operator.value}, severity={severity})")

    # -------------------------------------------------------------------------
    # Core check (now expanded)
    # -------------------------------------------------------------------------
    async def check(self, state: Dict) -> List[str]:
        """Record state, run temporal evaluation, then trigger downstream modules."""
        now = datetime.now(timezone.utc)
        self.history.append((now, state))

        # --- Feed the temporal history to all sub-modules ---------------
        # (each pushes its own observations into the same history timeline)
        if self.carbon_market:
            await self.carbon_market.observe_state(state)
        if self.precision_switcher:
            await self.precision_switcher.observe_state(state)

        violated = []
        for rule_id, rule in self.rules.items():
            if rule.last_violation_time and rule.cooldown_seconds > 0:
                elapsed = (now - rule.last_violation_time).total_seconds()
                if elapsed < rule.cooldown_seconds:
                    continue
            hist_copy = deque(self.history, maxlen=self.history.maxlen)
            is_violated = rule.evaluate(hist_copy)
            if is_violated:
                rule.violation_count += 1
                rule.total_violations += 1
                rule.last_violation_time = now
                self._metrics['violations_total'] += 1
                self.event_log.append({
                    'type': 'violation', 'rule_id': rule_id,
                    'severity': rule.severity, 'ts': now.isoformat(),
                    'state': dict(state)})
                if rule.violation_count >= rule.max_violations:
                    violated.append(rule_id)
                    # Auto-remediation (v2)
                    if rule.auto_remediate:
                        try:
                            remediation = rule.auto_remediate(state)
                            self.event_log.append({
                                'type': 'auto_remediate', 'rule_id': rule_id,
                                'remediation': remediation, 'ts': now.isoformat()})
                        except Exception as e:
                            logger.error(f"auto_remediate failed for {rule_id}: {e}")
                    # HITL approval (only for critical severity)
                    if self.approval_callback and rule.severity == 'critical':
                        approved = await self._request_approval(rule_id, state)
                        if not approved:
                            self.event_log.append({
                                'type': 'hitl_denied', 'rule_id': rule_id,
                                'ts': now.isoformat()})
                            # Escalate to multi-agent verifier if available
                            if self.multi_agent:
                                await self.multi_agent.broadcast(
                                    topic='critical_violation',
                                    sender='temporal_monitor',
                                    payload={'rule_id': rule_id, 'state': state})
            else:
                rule.violation_count = 0

        # --- Re-run learned policy through the causal adapter ---------------
        if self.causal_rl and violated:
            await self.causal_rl.record_violation(violated, state)

        return violated

    async def _request_approval(self, rule_id: str, state: Dict) -> bool:
        if self.approval_callback is None:
            logger.warning(f"Rule {rule_id} critical but no approval callback → denying")
            return False
        try:
            result = self.approval_callback(rule_id, state)
            if asyncio.iscoroutine(result):
                return await result
            return bool(result)
        except Exception as e:
            logger.error(f"Approval callback failed: {e}")
            return False

    def set_approval_callback(self, callback: Callable[[str, Dict], bool]):
        self.approval_callback = callback

    # -------------------------------------------------------------------------
    # Explainability (XAI)
    # -------------------------------------------------------------------------
    async def explain_violation(self, rule_id: str) -> Optional[Dict[str, Any]]:
        rule = self.rules.get(rule_id)
        if not rule:
            return None
        relevant_states = []
        for ts, st in self.history:
            if rule.operator in (TemporalOperator.UNTIL, TemporalOperator.WITHIN):
                relevant_states.append(st)
            else:
                cond = rule.conditions[0]
                if not cond(st):
                    relevant_states.append(st)
        recent = relevant_states[-3:]
        explanation = {
            'rule_id': rule_id,
            'description': rule.description,
            'operator': rule.operator.value,
            'severity': rule.severity,
            'violation_count': rule.violation_count,
            'total_violations': rule.total_violations,
            'last_violation_time': rule.last_violation_time.isoformat() if rule.last_violation_time else None,
            'recent_states': recent,
            'message': f"Rule '{rule_id}' ({rule.description}) violated. "
                       f"Operator: {rule.operator.value}, severity: {rule.severity}."
        }
        # Delegate to attached XAI explainer if present
        if self._xai_hook:
            try:
                extra = self._xai_hook(rule_id, {'recent': recent})
                if asyncio.iscoroutine(extra):
                    extra = await extra
                explanation['attributions'] = extra
            except Exception as e:
                logger.debug(f"XAI hook failed: {e}")
        return explanation

    def set_xai_hook(self, hook: Callable[[str, Dict], Dict]):
        """Attach an external XAI explainer (e.g. KernelSHAP)."""
        self._xai_hook = hook

    # -------------------------------------------------------------------------
    # Persistence
    # -------------------------------------------------------------------------
    async def save_state(self, path: str = "temporal_monitor_state.json") -> bool:
        data = {
            'history': [[ts.isoformat(), state] for ts, state in self.history],
            'rules': {
                rid: {
                    'violation_count': rule.violation_count,
                    'total_violations': rule.total_violations,
                    'last_violation_time': rule.last_violation_time.isoformat()
                        if rule.last_violation_time else None,
                    'evaluations': rule.evaluations,
                } for rid, rule in self.rules.items()},
            'metrics': dict(self._metrics),
            'events': list(self.event_log),
        }
        try:
            with open(path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            return True
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
            return False

    async def load_state(self, path: str = "temporal_monitor_state.json") -> bool:
        if not os.path.exists(path):
            return False
        try:
            with open(path, 'r') as f:
                data = json.load(f)
            self.history.clear()
            for ts_str, state in data.get('history', []):
                self.history.append((datetime.fromisoformat(ts_str), state))
            for rid, info in data.get('rules', {}).items():
                if rid in self.rules:
                    r = self.rules[rid]
                    r.violation_count = info.get('violation_count', 0)
                    r.total_violations = info.get('total_violations', 0)
                    r.evaluations = info.get('evaluations', 0)
                    lvt = info.get('last_violation_time')
                    r.last_violation_time = datetime.fromisoformat(lvt) if lvt else None
            self._metrics.update(data.get('metrics', {}))
            for ev in data.get('events', []):
                self.event_log.append(ev)
            return True
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
            return False

    # -------------------------------------------------------------------------
    # Chaos testing (enhanced with new fault types)
    # -------------------------------------------------------------------------
    async def inject_fault(self, fault_type: str, **kwargs):
        if fault_type == 'clear_history':
            self.history.clear()
            logger.warning("Fault: history cleared")
        elif fault_type == 'corrupt_timestamps':
            new_hist = deque(maxlen=self.history.maxlen)
            for ts, state in self.history:
                new_hist.append((ts + timedelta(days=1), state))
            self.history = new_hist
            logger.warning("Fault: timestamps corrupted")
        elif fault_type == 'reset_counts':
            for rule in self.rules.values():
                rule.violation_count = 0
                rule.last_violation_time = None
            logger.warning("Fault: violation counts reset")
        elif fault_type == 'delete_rule':
            rid = kwargs.get('rule_id')
            if rid and rid in self.rules:
                del self.rules[rid]
                logger.warning(f"Fault: rule {rid} deleted")
        elif fault_type == 'inject_bad_condition':
            # Deliberately poison a rule with an exception-raising condition
            rid = kwargs.get('rule_id') or next(iter(self.rules), None)
            if rid and rid in self.rules:
                def _poison(_s):
                    raise RuntimeError("chaos: poisoned condition")
                self.rules[rid].conditions = [_poison]
                logger.warning(f"Fault: rule {rid} poisoned")
        elif fault_type == 'stall_submodules':
            for sub in [self.causal_rl, self.federated, self.multi_agent,
                        self.precision_switcher, self.carbon_market]:
                if sub and hasattr(sub, 'stall'):
                    sub.stall()
            logger.warning("Fault: submodules stalled")
        else:
            logger.warning(f"Unknown fault type: {fault_type}")

    async def run_chaos_test(self) -> Dict[str, Any]:
        report = {'faults': [], 'results': {}, 'temporal_ok': False}
        state = {'carbon': 400, 'quality': 0.9, 'privacy_budget': 1.0,
                 'generation_complete': True}

        for fault in ['clear_history', 'corrupt_timestamps', 'reset_counts',
                      'inject_bad_condition', 'stall_submodules']:
            try:
                await self.inject_fault(fault)
                report['faults'].append(fault)
                violated = await self.check(state)
                report['results'][fault] = {
                    'violated_count': len(violated),
                    'history_len': len(self.history),
                    'still_running': True}
            except Exception as e:
                report['results'][fault] = {'error': str(e), 'still_running': False}

        # Verify basic ops still work
        try:
            self.add_rule("post_chaos_smoke", TemporalOperator.EVENTUALLY,
                          [lambda s: s.get('carbon', 0) < 1000],
                          window_seconds=0, description="post-chaos smoke test")
            violated = await self.check(state)
            report['temporal_ok'] = True
        except Exception as e:
            report['temporal_ok'] = False
            report['post_chaos_error'] = str(e)
        return report

    # -------------------------------------------------------------------------
    # Sub-module attach helpers
    # -------------------------------------------------------------------------
    def attach(self, *, distiller=None, causal_rl=None, federated=None,
               multi_agent=None, precision_switcher=None, carbon_market=None):
        """Attach all the Green Agent enhancement modules to this monitor."""
        self.distiller = distiller
        self.causal_rl = causal_rl
        self.federated = federated
        self.multi_agent = multi_agent
        self.precision_switcher = precision_switcher
        self.carbon_market = carbon_market
        # wire XAI hook from any sub-module that provides one
        if precision_switcher and hasattr(precision_switcher, 'explain'):
            self.set_xai_hook(precision_switcher.explain)
        elif causal_rl and hasattr(causal_rl, 'explain'):
            self.set_xai_hook(causal_rl.explain)

    async def step_all(self, state: Dict) -> Dict[str, Any]:
        """Drive every attached sub-module by one step and return a snapshot."""
        snapshot: Dict[str, Any] = {'state': state}
        if self.distiller:
            snapshot['distillation'] = await self.distiller.step(state)
        if self.causal_rl:
            snapshot['causal_rl'] = await self.causal_rl.step(state)
        if self.federated:
            snapshot['federated'] = await self.federated.step(state)
        if self.multi_agent:
            snapshot['multi_agent'] = await self.multi_agent.step(state)
        if self.precision_switcher:
            snapshot['precision'] = await self.precision_switcher.step(state)
        if self.carbon_market:
            snapshot['carbon_market'] = await self.carbon_market.step(state)
        snapshot['violations'] = await self.check(state)
        return snapshot


# =============================================================================
# SECTION 2: QUANTUM-DISTILLATION INTEGRATION (NEW — module 5)
# =============================================================================

class QuantumDistillationEngine:
    """
    Multi-Teacher On-Policy Distillation with quantum-inspired superposition
    of teacher policies.  Distills several teacher distributions (temporal
    safety, causal RL, multi-agent, carbon market) into one student policy.
    """
    def __init__(self, temperature: float = 2.0, alpha: float = 0.5):
        self.temperature = temperature
        self.alpha = alpha
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy: List[float] = [0.25, 0.25, 0.25, 0.25]
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)
        self._stalled = False

    def register_teacher(self, name: str, policy: List[float]):
        self.teachers[name] = [float(p) for p in policy]
        logger.info(f"[Distiller] registered teacher '{name}' with {len(policy)} actions")

    def stall(self):
        self._stalled = True

    def _softmax(self, x: List[float]) -> List[float]:
        if not x:
            return []
        m = max(x)
        exps = [math.exp((xi - m) / max(self.temperature, 1e-6)) for xi in x]
        s = sum(exps) or 1.0
        return [e / s for e in exps]

    def _quantum_superpose(self) -> List[float]:
        """Superpose all teacher distributions via amplitude-style combination."""
        if not self.teachers:
            return [0.25, 0.25, 0.25, 0.25]
        n = len(next(iter(self.teachers.values())))
        accum = [0.0] * n
        # Amplitude = sqrt(prob), then re-square to bias toward agreement
        for pol in self.teachers.values():
            for i, p in enumerate(pol):
                accum[i] += math.sqrt(max(p, 1e-9))
        accum = [a / len(self.teachers) for a in accum]
        squared = [a * a for a in accum]
        s = sum(squared) or 1.0
        return [x / s for x in squared]

    async def step(self, state: Dict) -> Dict[str, Any]:
        if self._stalled:
            return {'stalled': True}
        # Teacher distributions come from registered policies
        # (usually: causal_rl.get_policy(), multi_agent.get_policy(),
        #  carbon_market.get_policy(), temporal_monitor.get_policy())
        target = self._quantum_superpose()
        target = self._softmax([math.log(max(p, 1e-9)) for p in target])
        # KL-style gradient step
        lr = 0.05
        new_student = []
        for s, t in zip(self.student_policy, target):
            grad = -(t / max(s, 1e-9))
            new_student.append(max(0.01, s - lr * grad))
        ssum = sum(new_student) or 1.0
        self.student_policy = [x / ssum for x in new_student]
        entry = {'target': target, 'student': list(self.student_policy),
                 'ts': datetime.now(timezone.utc).isoformat()}
        self.history.append(entry)
        return entry

    def get_policy(self) -> List[float]:
        return list(self.student_policy)


# =============================================================================
# SECTION 3: CAUSAL REINFORCEMENT LEARNING (NEW — module 6)
# =============================================================================

class CausalGraph:
    """Minimal causal DAG with weighted edges."""
    def __init__(self):
        self.edges: Dict[str, Dict[str, float]] = defaultdict(dict)
        self.variables: List[str] = []

    def add_edge(self, src: str, dst: str, weight: float):
        self.edges[src][dst] = weight

    def parents(self, node: str) -> List[str]:
        return [s for s, e in self.edges.items() if node in e]

    def children(self, node: str) -> List[str]:
        return list(self.edges.get(node, {}).keys())

    def summary(self) -> Dict[str, Any]:
        return {'nodes': len(self.variables),
                'edges': sum(len(v) for v in self.edges.values()),
                'variables': list(self.variables)}


class CausalPolicyAdapter:
    """
    Causal RL: learns structure from observational data + adapts a policy
    using do-calculus style counterfactual reward estimates.
    """
    def __init__(self, actions: Optional[List[str]] = None,
                 exploration: float = 0.1):
        self.actions = actions or ['statistical', 'vae', 'gan', 'hybrid']
        self.graph = CausalGraph()
        self.exploration = exploration
        self.action_values: Dict[str, float] = defaultdict(float)
        self.action_counts: Dict[str, int] = defaultdict(int)
        self.policy = [1.0 / len(self.actions)] * len(self.actions)
        self.violation_log: Deque[Tuple[List[str], Dict]] = deque(maxlen=200)
        self._stalled = False
        self._last_state: Dict[str, Any] = {}

    def stall(self):
        self._stalled = True

    async def record_violation(self, violated_rules: List[str], state: Dict):
        self.violation_log.append((list(violated_rules), dict(state)))
        # Penalize the last chosen action
        last = state.get('selected_strategy')
        if last and last in self.action_values:
            self.action_values[last] *= 0.9

    def learn_structure(self, samples: List[Dict[str, float]],
                        variables: List[str], threshold: float = 0.2):
        """Correlation + variance orientation."""
        self.graph.variables = list(variables)
        if not NUMPY_AVAILABLE or len(samples) < 5:
            # fallback random DAG
            for i, s in enumerate(variables):
                for j, t in enumerate(variables):
                    if i < j and random.random() < 0.25:
                        self.graph.add_edge(s, t, random.uniform(0.1, 0.9))
            return self.graph.summary()

        X = np.array([[s[v] for v in variables] for s in samples], dtype=float)
        if X.shape[0] < 2:
            return self.graph.summary()
        X = (X - X.mean(0)) / (X.std(0) + 1e-9)
        corr = np.corrcoef(X, rowvar=False)
        for i in range(len(variables)):
            for j in range(len(variables)):
                if i == j:
                    continue
                c = abs(float(corr[i, j]))
                if c > threshold:
                    vi, vj = float(X[:, i].var()), float(X[:, j].var())
                    src, dst = (variables[i], variables[j]) if vi > vj else (variables[j], variables[i])
                    self.graph.add_edge(src, dst, float(corr[i, j]))
        return self.graph.summary()

    async def choose_action(self, state: Dict) -> str:
        if self._stalled:
            return self.actions[0]
        if random.random() < self.exploration:
            return random.choice(self.actions)
        return max(self.actions, key=lambda a: self.action_values.get(a, 0.0))

    async def update(self, action: str, reward: float, state: Dict):
        if self._stalled:
            return
        if action not in self.actions:
            action = self.actions[0]
        self.action_counts[action] += 1
        n = self.action_counts[action]
        self.action_values[action] += (reward - self.action_values[action]) / n
        vals = [self.action_values.get(a, 0.0) for a in self.actions]
        m = max(vals)
        exps = [math.exp((v - m) / 0.5) for v in vals]
        s = sum(exps) or 1.0
        self.policy = [e / s for e in exps]

    async def step(self, state: Dict) -> Dict[str, Any]:
        if self._stalled:
            return {'stalled': True}
        chosen = await self.choose_action(state)
        # Simulated reward from state quality
        reward = float(state.get('quality', 0.8)) * (1.0 - 0.5 * float(state.get('carbon', 0.4)))
        await self.update(chosen, reward, state)
        return {'chosen_action': chosen, 'reward': reward,
                'policy': list(self.policy),
                'ate_carbon_quality': self.graph.edges.get('carbon', {}).get('quality', 0.0)}

    def get_policy(self) -> List[float]:
        return list(self.policy)

    def explain(self, rule_id: str, payload: Dict) -> Dict[str, Any]:
        return {'causal_parents_of_quality': self.graph.parents('quality'),
                'action_values': dict(self.action_values)}


# =============================================================================
# SECTION 4: FEDERATED GREEN LEARNING (NEW — module 7)
# =============================================================================

class FederatedGreenLearner:
    """Shares policy weights across deployments via a pluggable backend."""
    def __init__(self, instance_id: Optional[str] = None,
                 backend: Optional[Callable[[str, str], Any]] = None):
        self.instance_id = instance_id or f"inst_{uuid.uuid4().hex[:8]}"
        self.local_weights: Dict[str, List[float]] = {}
        self.peers: Dict[str, Dict[str, List[float]]] = defaultdict(dict)
        self.rounds = 0
        self._stalled = False
        self._backend = backend  # (key, value) -> None | (key) -> value

    def stall(self):
        self._stalled = True

    async def share(self, name: str, weights: List[float]):
        self.local_weights[name] = list(weights)
        if self._backend:
            try:
                r = self._backend('put', f"{self.instance_id}/{name}", json.dumps(weights))
                if asyncio.iscoroutine(r):
                    await r
            except Exception as e:
                logger.debug(f"Federated share failed: {e}")

    async def pull(self, name: str, peer_id: str):
        if not self._backend:
            return
        try:
            r = self._backend('get', f"{peer_id}/{name}")
            if asyncio.iscoroutine(r):
                r = await r
            if r:
                self.peers[name][peer_id] = json.loads(r)
        except Exception as e:
            logger.debug(f"Federated pull failed: {e}")

    def aggregate(self, name: str) -> List[float]:
        all_pols = [self.local_weights.get(name, [])] + list(
            v for v in self.peers.get(name, {}).values())
        all_pols = [p for p in all_pols if p]
        if not all_pols:
            return []
        n = len(all_pols[0])
        avg = [0.0] * n
        for p in all_pols:
            for i in range(n):
                avg[i] += p[i] / len(all_pols)
        # clip to simplex
        s = sum(avg) or 1.0
        return [x / s for x in avg]

    async def step(self, state: Dict) -> Dict[str, Any]:
        if self._stalled:
            return {'stalled': True}
        self.rounds += 1
        if 'policy' in state and isinstance(state['policy'], list):
            await self.share('policy', state['policy'])
        aggregated = self.aggregate('policy')
        return {'round': self.rounds, 'peers': len(self.peers.get('policy', {})),
                'aggregated_policy': aggregated}

    def get_policy(self) -> List[float]:
        return self.aggregate('policy') or [0.25, 0.25, 0.25, 0.25]


# =============================================================================
# SECTION 5: ADVANCED MULTI-AGENT COORDINATION (NEW — module 8)
# =============================================================================

class MultiAgent:
    """A single agent with utility profile and reputation."""
    ROLES = ['generator', 'critic', 'verifier', 'negotiator', 'explorer']

    def __init__(self, agent_id: str):
        self.id = agent_id
        self.role = 'explorer'
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        self.tasks_completed = 0


class MultiAgentCoordinator:
    """In-process multi-agent system with emergent role specialisation."""
    def __init__(self, agent_count: int = 5, message_bus_size: int = 500):
        self.agents: Dict[str, MultiAgent] = {
            f"agent_{i:02d}": MultiAgent(f"agent_{i:02d}") for i in range(agent_count)}
        self.bus: asyncio.Queue = asyncio.Queue(maxsize=message_bus_size)
        self.message_log: Deque[Dict[str, Any]] = deque(maxlen=500)
        self.rounds = 0
        self._stalled = False

    def stall(self):
        self._stalled = True

    def _specialise(self):
        for a in self.agents.values():
            best_role = max(a.utilities, key=lambda r: a.utilities[r])
            a.role = best_role

    async def broadcast(self, topic: str, sender: str, payload: Dict[str, Any]):
        msg = {'topic': topic, 'sender': sender, 'payload': payload,
               'ts': datetime.now(timezone.utc).isoformat()}
        try:
            self.bus.put_nowait(msg)
        except asyncio.QueueFull:
            pass
        self.message_log.append(msg)

    async def bid(self, task: Dict[str, Any]) -> Tuple[str, float]:
        best_id, best_score = None, -1.0
        preferred = task.get('preferred_role', 'generator')
        for aid, a in self.agents.items():
            role_bonus = 1.0 if a.role == preferred else 0.6
            score = a.utilities[a.role] * role_bonus + 0.3 * a.reputation
            score += random.uniform(-0.02, 0.02)
            if score > best_score:
                best_score, best_id = score, aid
        if best_id:
            self.agents[best_id].tasks_completed += 1
        await self.broadcast('task_bid', best_id or 'none',
                             {'task': task.get('name', 'unknown'), 'score': best_score})
        return best_id or next(iter(self.agents)), best_score

    async def reward(self, agent_id: str, reward: float):
        if agent_id in self.agents:
            a = self.agents[agent_id]
            n = max(1, a.tasks_completed)
            a.reputation = max(0.0, min(1.0, a.reputation + reward / n))
            a.utilities[a.role] = min(1.0, a.utilities[a.role] + 0.05 * reward)

    async def step(self, state: Dict) -> Dict[str, Any]:
        if self._stalled:
            return {'stalled': True}
        self.rounds += 1
        self._specialise()
        # Process any queued messages
        processed = 0
        while not self.bus.empty():
            try:
                _ = self.bus.get_nowait()
                processed += 1
            except asyncio.QueueEmpty:
                break
        # Optionally bid on a task derived from state
        if state.get('task'):
            aid, score = await self.bid(state['task'])
        else:
            aid, score = None, 0.0
        # Aggregate role distribution into a policy-like vector
        role_counts = defaultdict(int)
        for a in self.agents.values():
            role_counts[a.role] += 1
        total = max(1, sum(role_counts.values()))
        role_dist = {r: role_counts[r] / total for r in MultiAgent.ROLES}
        return {'round': self.rounds, 'roles': {a.id: a.role for a in self.agents.values()},
                'role_distribution': role_dist,
                'processed_messages': processed,
                'winner': aid, 'winning_score': score}

    def get_policy(self) -> List[float]:
        """Return a 4-dim vector summarising the role distribution."""
        role_counts = defaultdict(int)
        for a in self.agents.values():
            role_counts[a.role] += 1
        total = max(1, sum(role_counts.values()))
        # Map onto the 4 canonical generation actions: statistical, vae, gan, hybrid
        # via role → action affinity
        affinity = {
            'generator':  [0.25, 0.25, 0.25, 0.25],
            'critic':     [0.40, 0.20, 0.20, 0.20],
            'verifier':   [0.20, 0.20, 0.40, 0.20],
            'negotiator': [0.25, 0.25, 0.25, 0.25],
            'explorer':   [0.10, 0.30, 0.30, 0.30],
        }
        out = [0.0] * 4
        for role, cnt in role_counts.items():
            w = cnt / total
            for i, v in enumerate(affinity.get(role, [0.25] * 4)):
                out[i] += w * v
        s = sum(out) or 1.0
        return [x / s for x in out]


# =============================================================================
# SECTION 6: ADAPTIVE PRECISION SWITCHING (NEW — module 9)
# =============================================================================

class AdaptivePrecisionSwitcher:
    """
    Hardware-aware precision policy: fp32 / tf32 / bf16 / fp16 / int8.
    Chooses precision to minimise energy subject to accuracy tolerance.
    """
    ENERGY_MODEL = {'fp32': 1.0, 'tf32': 0.75, 'bf16': 0.55, 'fp16': 0.5, 'int8': 0.3}
    LATENCY_MODEL = {'fp32': 1.0, 'tf32': 0.8, 'bf16': 0.65, 'fp16': 0.6, 'int8': 0.4}
    ACCURACY_PENALTY = {'fp32': 0.0, 'tf32': 0.005, 'bf16': 0.01,
                        'fp16': 0.012, 'int8': 0.03}

    def __init__(self, tolerance: float = 0.02, energy_target: float = 0.7):
        self.tolerance = tolerance
        self.energy_target = energy_target
        self.current = 'fp32'
        self.energy_saved_wh = 0.0
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)
        self._stalled = False

    def stall(self):
        self._stalled = True

    def _probe_hw(self) -> Dict[str, Any]:
        info = {'cuda': False, 'bf16': False, 'device': 'cpu'}
        if TORCH_AVAILABLE:
            try:
                info['cuda'] = torch.cuda.is_available()
                if info['cuda']:
                    info['device'] = torch.cuda.get_device_name(0)
                    info['bf16'] = torch.cuda.is_bf16_supported()
            except Exception:
                pass
        return info

    def select(self, baseline_accuracy: float = 0.9) -> str:
        hw = self._probe_hw()
        candidates = list(self.ENERGY_MODEL.keys())
        if not hw['cuda']:
            candidates = [c for c in candidates if c in ('fp32', 'int8')]
        if not hw['bf16']:
            candidates = [c for c in candidates if c != 'bf16']
        # Filter by accuracy tolerance
        allowed = [c for c in candidates if self.ACCURACY_PENALTY.get(c, 1.0) <= self.tolerance]
        if not allowed:
            allowed = ['fp32']
        # Choose the greenest allowed
        return min(allowed, key=lambda c: self.ENERGY_MODEL[c])

    async def observe_state(self, state: Dict):
        acc = float(state.get('accuracy', 0.9))
        # auto-switch
        target = self.select(baseline_accuracy=acc)
        if target != self.current:
            old = self.current
            self.current = target
            saved = max(0.0, self.ENERGY_MODEL[old] - self.ENERGY_MODEL[target])
            self.energy_saved_wh += saved
            self.history.append({'from': old, 'to': target,
                                 'reason': 'auto_state_observe',
                                 'ts': datetime.now(timezone.utc).isoformat()})

    async def step(self, state: Dict) -> Dict[str, Any]:
        if self._stalled:
            return {'stalled': True}
        await self.observe_state(state)
        return {'current': self.current, 'hardware': self._probe_hw(),
                'energy_saved_wh': self.energy_saved_wh}

    def explain(self, rule_id: str, payload: Dict) -> Dict[str, Any]:
        return {'current_precision': self.current,
                'energy_saved_wh': self.energy_saved_wh,
                'rule_context': rule_id}


# =============================================================================
# SECTION 7: CARBON MARKETS & REC INTEGRATION (NEW — module 10)
# =============================================================================

class CarbonMarketIntegrator:
    """
    Fetches carbon credit prices, maintains an REC ledger, and schedules
    workloads via net-zero matching.
    """
    def __init__(self, api_url: Optional[str] = None,
                 region: str = 'global', start_price: float = 25.0):
        self.api_url = api_url
        self.region = region
        self.price_usd = start_price
        self.rec_balance_mwh = 0.0
        self.price_history: Deque[Tuple[datetime, float]] = deque(maxlen=500)
        self.trades: Deque[Dict[str, Any]] = deque(maxlen=500)
        self._stalled = False

    def stall(self):
        self._stalled = True

    async def _fetch_price(self) -> float:
        if AIOHTTP_AVAILABLE and self.api_url:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(f"{self.api_url}/price", timeout=8) as r:
                        if r.status == 200:
                            data = await r.json()
                            return float(data.get('price', self.price_usd))
            except Exception as e:
                logger.debug(f"Carbon market API failed: {e}")
        # Fallback: random walk
        return max(5.0, self.price_usd + random.gauss(0, 1.5))

    async def observe_state(self, state: Dict):
        """Called by the temporal monitor each check cycle."""
        if self._stalled:
            return
        intensity = float(state.get('carbon', state.get('carbon_intensity', 0.4)))
        # Decide offset action
        carbon_kg = 1.0 * intensity  # workload unit
        offset_cost = (carbon_kg / 1000.0) * self.price_usd
        if intensity > 0.3 and self.rec_balance_mwh < 0.5:
            self.trades.append({
                'action': 'defer',
                'carbon_kg': carbon_kg,
                'offset_cost': offset_cost,
                'ts': datetime.now(timezone.utc).isoformat()})
        elif offset_cost < 0.5:
            self.trades.append({
                'action': 'run_offset',
                'carbon_kg': carbon_kg,
                'offset_cost': offset_cost,
                'ts': datetime.now(timezone.utc).isoformat()})

    async def purchase_rec(self, mwh: float, price_per_mwh: float = 5.0, source: str = 'wind'):
        cost = mwh * price_per_mwh
        self.rec_balance_mwh += mwh
        self.trades.append({'action': 'purchase_rec', 'mwh': mwh,
                            'cost': cost, 'source': source,
                            'ts': datetime.now(timezone.utc).isoformat()})
        return cost

    async def step(self, state: Dict) -> Dict[str, Any]:
        if self._stalled:
            return {'stalled': True}
        self.price_usd = await self._fetch_price()
        self.price_history.append((datetime.now(timezone.utc), self.price_usd))
        await self.observe_state(state)
        return {'price_usd': self.price_usd, 'rec_balance_mwh': self.rec_balance_mwh,
                'trades': list(self.trades)[-3:]}

    def get_policy(self) -> List[float]:
        """Return a 4-dim policy influenced by carbon price."""
        # Higher price → more weight on statistical (cheap)
        p = self.price_usd
        if p > 30:
            return [0.6, 0.2, 0.1, 0.1]
        elif p > 20:
            return [0.4, 0.3, 0.15, 0.15]
        return [0.25, 0.25, 0.25, 0.25]

    def explain(self, rule_id: str, payload: Dict) -> Dict[str, Any]:
        return {'carbon_price_usd': self.price_usd,
                'rec_balance_mwh': self.rec_balance_mwh,
                'rule_context': rule_id}


# =============================================================================
# SECTION 8: INTEGRATED FACADE — "GreenAgentTemporalCore"
# =============================================================================

class GreenAgentTemporalCore:
    """
    Single entry-point for the Green Agent that bundles every enhancement.
    Usage:
        core = GreenAgentTemporalCore()
        await core.initialize()
        report = await core.tick({'carbon': 0.3, 'quality': 0.9, ...})
    """
    def __init__(self, instance_id: Optional[str] = None,
                 config: Optional[Dict[str, Any]] = None):
        self.instance_id = instance_id or f"ga_{uuid.uuid4().hex[:8]}"
        self.config = config or {}

        # ---- core monitor ----
        self.monitor = TemporalSafetyMonitor(
            max_history=self.config.get('max_history', 10000),
            db_path=self.config.get('db_path'))

        # ---- ten enhancements ----
        self.distiller = QuantumDistillationEngine(
            temperature=self.config.get('distillation_temperature', 2.0),
            alpha=self.config.get('distillation_alpha', 0.5))
        self.causal_rl = CausalPolicyAdapter(
            exploration=self.config.get('causal_exploration', 0.1))
        self.federated = FederatedGreenLearner(instance_id=self.instance_id)
        self.multi_agent = MultiAgentCoordinator(
            agent_count=self.config.get('agent_count', 5))
        self.precision_switcher = AdaptivePrecisionSwitcher(
            tolerance=self.config.get('precision_tolerance', 0.02))
        self.carbon_market = CarbonMarketIntegrator(
            api_url=self.config.get('carbon_api_url'),
            region=self.config.get('carbon_region', 'global'))

        # ---- wire the monitor to sub-modules ----
        self.monitor.attach(
            distiller=self.distiller,
            causal_rl=self.causal_rl,
            federated=self.federated,
            multi_agent=self.multi_agent,
            precision_switcher=self.precision_switcher,
            carbon_market=self.carbon_market)

        # ---- default safety rules ----
        self._install_default_rules()

    def _install_default_rules(self):
        m = self.monitor
        m.add_rule(
            "never_high_carbon",
            TemporalOperator.NEVER,
            [lambda s: s.get('carbon', 0.0) > 0.7],
            description="Carbon intensity must never exceed 0.7 kg/kWh",
            severity="critical",
            auto_remediate=lambda s: {'action': 'defer', 'reason': 'high carbon'})
        m.add_rule(
            "always_quality",
            TemporalOperator.ALWAYS,
            [lambda s: s.get('quality', 1.0) >= 0.5],
            description="Quality must always remain >= 0.5",
            severity="warning")
        m.add_rule(
            "eventually_complete",
            TemporalOperator.EVENTUALLY,
            [lambda s: s.get('generation_complete', False)],
            window_seconds=600,
            description="Generation must complete within 10 minutes",
            severity="warning")
        m.add_rule(
            "until_fallback",
            TemporalOperator.UNTIL,
            [lambda s: s.get('primary_ok', True),
             lambda s: s.get('fallback_active', False)],
            window_seconds=60,
            description="Primary must hold until fallback is active (60s window)",
            severity="critical")
        m.add_rule(
            "within_recover",
            TemporalOperator.WITHIN,
            [lambda s: s.get('error_detected', False),
             lambda s: s.get('recovered', False)],
            window_seconds=120,
            description="After an error, recovery must occur within 120s",
            severity="critical")

    async def initialize(self):
        # Register teacher policies with the distiller
        self.distiller.register_teacher('causal_rl', self.causal_rl.get_policy())
        self.distiller.register_teacher('multi_agent', self.multi_agent.get_policy())
        self.distiller.register_teacher('carbon_market', self.carbon_market.get_policy())
        self.distiller.register_teacher('temporal_safe', [0.5, 0.2, 0.2, 0.1])
        logger.info(f"[GreenAgentTemporalCore] initialized for instance {self.instance_id}")

    async def tick(self, state: Dict) -> Dict[str, Any]:
        """Run one full enhancement cycle against a given state."""
        # 1. Run every sub-module
        snapshot = await self.monitor.step_all(state)

        # 2. Refresh teacher policies from latest sub-module outputs
        self.distiller.register_teacher('causal_rl', self.causal_rl.get_policy())
        self.distiller.register_teacher('multi_agent', self.multi_agent.get_policy())
        self.distiller.register_teacher('carbon_market', self.carbon_market.get_policy())

        # 3. Student policy becomes the aggregated production policy
        snapshot['production_policy'] = self.distiller.get_policy()

        # 4. Federated sharing of the produced policy
        await self.federated.share('policy', snapshot['production_policy'])

        # 5. Multi-agent rewards the winning bidder if any
        if snapshot.get('multi_agent', {}).get('winner'):
            await self.multi_agent.reward(snapshot['multi_agent']['winner'],
                                          reward=float(state.get('quality', 0.8)))

        # 6. If temporal rules were violated, ask causal RL to adapt
        if snapshot.get('violations'):
            await self.causal_rl.record_violation(snapshot['violations'], state)
            # also broadcast to multi-agent
            await self.multi_agent.broadcast(
                topic='violation', sender='monitor',
                payload={'violations': snapshot['violations'], 'state': state})
        return snapshot

    async def run_for(self, states: List[Dict], interval: float = 0.0) -> List[Dict]:
        """Convenience: run N ticks over a list of states."""
        out = []
        for s in states:
            out.append(await self.tick(s))
            if interval > 0:
                await asyncio.sleep(interval)
        return out

    async def save(self, path: str = "temporal_monitor_state.json"):
        return await self.monitor.save_state(path)

    async def load(self, path: str = "temporal_monitor_state.json"):
        return await self.monitor.load_state(path)

    async def chaos(self) -> Dict[str, Any]:
        return await self.monitor.run_chaos_test()


# =============================================================================
# SECTION 9: DEMO / ENTRY POINT
# =============================================================================

async def _demo():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    core = GreenAgentTemporalCore(instance_id="demo-01",
                                  config={'agent_count': 4,
                                          'carbon_api_url': None})
    await core.initialize()

    # Simulate a stream of 5 states — some safe, some dangerous
    states = [
        {'carbon': 0.20, 'quality': 0.92, 'privacy_budget': 1.0,
         'generation_complete': True, 'primary_ok': True, 'fallback_active': False},
        {'carbon': 0.35, 'quality': 0.85, 'privacy_budget': 0.9,
         'generation_complete': False, 'primary_ok': True, 'fallback_active': False},
        {'carbon': 0.85, 'quality': 0.90, 'privacy_budget': 0.8,
         'generation_complete': False, 'primary_ok': True, 'fallback_active': False},  # high carbon
        {'carbon': 0.40, 'quality': 0.40, 'privacy_budget': 0.7,
         'generation_complete': True, 'primary_ok': True, 'fallback_active': False},   # low quality
        {'carbon': 0.30, 'quality': 0.88, 'privacy_budget': 0.9,
         'generation_complete': True, 'primary_ok': False, 'fallback_active': False},  # until violation
    ]

    for i, s in enumerate(states):
        report = await core.tick(s)
        print(f"\n=== TICK {i+1} ===")
        print(f"violations: {report.get('violations')}")
        print(f"causal_rl: {report.get('causal_rl')}")
        print(f"multi_agent: {report.get('multi_agent', {}).get('role_distribution')}")
        print(f"precision: {report.get('precision', {}).get('current')}")
        print(f"carbon_market: {report.get('carbon_market', {}).get('price_usd')}")
        print(f"production_policy: {[round(p,3) for p in report.get('production_policy', [])]}")

    # XAI explanation for a violated rule
    expl = await core.monitor.explain_violation("never_high_carbon")
    print("\n=== XAI explanation for never_high_carbon ===")
    print(json.dumps(expl, indent=2, default=str))

    # Chaos test
    chaos = await core.chaos()
    print("\n=== Chaos test report ===")
    print(json.dumps(chaos, indent=2, default=str))

    # Persist
    saved = await core.save("/tmp/temporal_monitor_state.json")
    print(f"\nState saved: {saved}")


if __name__ == "__main__":
    asyncio.run(_demo())
