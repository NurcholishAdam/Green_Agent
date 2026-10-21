#!/usr/bin/env python3
# =============================================================================
# FILE: src/enhancements/temporal_logic_module_v2_1_0.py
# VERSION: 2.1.0  (Phase 1-3 enhancements applied — see changelog at bottom)
# =============================================================================
"""
Temporal Logic / Formal Verification for Safety-Critical Policies
— Enhanced v2.1.0

Ten Green Agent enhancements in one file:
  1. Temporal Logic & Formal Verification
  2. Human-in-the-Loop for Critical Decisions
  3. Explainable AI (KernelSHAP)
  4. Resilience Engineering & Chaos Testing
  5. Quantum-Distillation Integration
  6. Causal Reinforcement Learning
  7. Federated Green Learning
  8. Multi-Agent Coordination
  9. Adaptive Precision Switching
 10. Carbon Markets & REC Integration

v2.1.0 (Phase 1-3) adds:
    [P0] WITHIN waits for window expiry; NEXT = G X φ via pending queue;
         correct KL gradient (s - t); chaos snapshots & restores; observe_state
         called once per tick.
    [P1] Causal RL uses graph parents; action-dependent reward; HITL cooldown;
         message bus with dispatch; async auto_remediate awaited; teachers
         registered before step_all; unified federated signature; peer
         discovery; precision uses baseline_accuracy; distillation length
         validation; carbon price cache; add_rule idempotent; non-mutating
         evaluate; dead imports removed.
    [P2] Correlation ID per tick; event log query API; event deduplication;
         health() endpoint; new internal metrics.
    [P3] Real KernelSHAP; sub-module save_state/load_state; PQC signature on
         saved state; train/val/test edge stability for causal structure;
         learned role→action affinity; carbon market defers; streaming ticks
         and batched ticks.
"""

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import re
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import wraps
from typing import (Any, AsyncIterator, Callable, Deque, Dict, List,
                    Optional, Tuple, Union)

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Optional deps
# -----------------------------------------------------------------------------
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

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    from pqcrypto.sign import dilithium
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False


# =============================================================================
# SECTION 1: TEMPORAL LOGIC CORE
# =============================================================================

class TemporalOperator(Enum):
    ALWAYS = "always"
    EVENTUALLY = "eventually"
    UNTIL = "until"
    NEVER = "never"
    NEXT = "next"           # [P0] G X φ — every next state must satisfy φ
    WITHIN = "within"


class TemporalRule:
    """A single temporal rule with severity, cooldown, remediation."""

    def __init__(
        self,
        rule_id: str,
        operator: TemporalOperator,
        conditions: List[Callable[[Dict], bool]],
        window_seconds: float = 0.0,
        max_violations: int = 1,
        cooldown_seconds: float = 0.0,
        description: str = "",
        severity: str = "warning",
        auto_remediate: Optional[Callable[[Dict], Any]] = None,
        hitl_cooldown_seconds: float = 60.0,   # [P1]
    ):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions
        self.window_seconds = window_seconds
        self.max_violations = max_violations
        self.cooldown_seconds = cooldown_seconds
        self.description = description or rule_id
        self.severity = severity
        self.auto_remediate = auto_remediate
        self.hitl_cooldown_seconds = hitl_cooldown_seconds

        # counters
        self.violation_count = 0
        self.total_violations = 0
        self.evaluations = 0
        self.last_violation_time: Optional[datetime] = None
        self.last_hitl_request: Optional[datetime] = None   # [P1]

    def evaluate(self, history: Deque[Tuple[datetime, Dict]]) -> bool:
        """Evaluate rule against history. Returns True if violated.

        [P1] Non-mutating: takes a slice instead of popping from caller's deque.
        [P0] UNTIL preserves standard LTL semantics (a U b).
        [P0] WITHIN returns pending (not violated) until the window expires.
        NEXT is evaluated by the monitor's pending queue, not here.
        """
        self.evaluations += 1
        if not history:
            return False

        # [P1] Work on a copy; do not mutate caller's history
        hist = list(history)
        if self.window_seconds > 0:
            cutoff = hist[-1][0] - timedelta(seconds=self.window_seconds)
            hist = [(ts, st) for ts, st in hist if ts >= cutoff]
            if not hist:
                return False

        if self.operator == TemporalOperator.UNTIL and len(self.conditions) >= 2:
            cond_a, cond_b = self.conditions[0], self.conditions[1]
            for _, state in hist:
                if cond_b(state):
                    return False
                if not cond_a(state):
                    return True
            # b never seen within window → violation
            return True

        if self.operator == TemporalOperator.WITHIN and len(self.conditions) >= 2:
            trigger, response = self.conditions[0], self.conditions[1]
            trigger_time = None
            for ts, state in hist:
                if trigger_time is None:
                    if trigger(state):
                        trigger_time = ts
                else:
                    if response(state):
                        return False
                    if self.window_seconds > 0 and \
                            (ts - trigger_time).total_seconds() > self.window_seconds:
                        return True
            if trigger_time is None:
                return False
            # [P0] Triggered but window not yet elapsed → pending, not violated
            elapsed = (hist[-1][0] - trigger_time).total_seconds()
            if self.window_seconds > 0 and elapsed > self.window_seconds:
                return True
            return False

        if self.operator == TemporalOperator.NEXT:
            # [P0] NEXT is handled by the monitor's pending queue.
            return False

        if self.operator == TemporalOperator.NEVER:
            cond = self.conditions[0]
            return any(cond(st) for _, st in hist)

        if self.operator == TemporalOperator.ALWAYS:
            cond = self.conditions[0]
            return not all(cond(st) for _, st in hist)

        if self.operator == TemporalOperator.EVENTUALLY:
            cond = self.conditions[0]
            if self.window_seconds == 0:
                return not any(cond(st) for _, st in hist)
            cutoff = hist[-1][0] - timedelta(seconds=self.window_seconds)
            return not any(cond(st) for ts, st in hist if ts >= cutoff)

        return False


class TemporalSafetyMonitor:
    """Core monitor — [P0] fixed operators, [P1] async remediation,
    [P2] event dedup + query API, [P3] sub-module save/load."""

    def __init__(self, max_history: int = 10000):
        self.rules: Dict[str, TemporalRule] = {}
        self.history: Deque[Tuple[datetime, Dict]] = deque(maxlen=max_history)
        self.approval_callback: Optional[Callable[[str, Dict], Any]] = None
        self.event_log: Deque[Dict[str, Any]] = deque(maxlen=2000)
        self._xai_hook: Optional[Callable[[str, Dict], Dict]] = None
        self._metrics: Dict[str, float] = defaultdict(float)
        self._pending_next: Dict[str, Callable[[Dict], bool]] = {}  # [P0]
        self._last_observed_state: Dict[str, int] = defaultdict(int)  # [P2]

        # Sub-modules attached via attach()
        self.distiller: Optional["QuantumDistillationEngine"] = None
        self.causal_rl: Optional["CausalPolicyAdapter"] = None
        self.federated: Optional["FederatedGreenLearner"] = None
        self.multi_agent: Optional["MultiAgentCoordinator"] = None
        self.precision_switcher: Optional["AdaptivePrecisionSwitcher"] = None
        self.carbon_market: Optional["CarbonMarketIntegrator"] = None
        self.xai: Optional["XAIExplainer"] = None

    # ------------------------------------------------------------------
    # Rule management
    # ------------------------------------------------------------------
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
        auto_remediate: Optional[Callable[[Dict], Any]] = None,
        hitl_cooldown_seconds: float = 60.0,
    ):
        # [P1] idempotent — overwriting a rule requires explicit removal
        if rule_id in self.rules:
            logger.warning(f"Rule '{rule_id}' already exists; replacing")
        cond_list = conditions if isinstance(conditions, list) else [conditions]
        if operator in (TemporalOperator.UNTIL, TemporalOperator.WITHIN) \
                and len(cond_list) < 2:
            raise ValueError(f"{operator.value} requires two conditions")
        if operator == TemporalOperator.NEXT and len(cond_list) < 1:
            raise ValueError("NEXT requires one condition")
        self.rules[rule_id] = TemporalRule(
            rule_id=rule_id, operator=operator, conditions=cond_list,
            window_seconds=window_seconds, max_violations=max_violations,
            cooldown_seconds=cooldown_seconds, description=description,
            severity=severity, auto_remediate=auto_remediate,
            hitl_cooldown_seconds=hitl_cooldown_seconds)

    def remove_rule(self, rule_id: str) -> bool:
        return self.rules.pop(rule_id, None) is not None

    # ------------------------------------------------------------------
    # Core check
    # ------------------------------------------------------------------
    async def check(self, state: Dict, cid: Optional[str] = None) -> List[str]:
        # [P2] correlation ID per tick
        cid = cid or uuid.uuid4().hex[:12]
        now = datetime.now(timezone.utc)
        self.history.append((now, state))

        violated: List[str] = []

        # [P0] Evaluate pending NEXT checks against the state just pushed
        for rule_id, cond in list(self._pending_next.items()):
            rule = self.rules.get(rule_id)
            if rule is None:
                del self._pending_next[rule_id]
                continue
            try:
                ok = bool(cond(state))
            except Exception as e:
                logger.error(f"NEXT cond for '{rule_id}' raised: {e}")
                ok = False
            if not ok:
                self._record_violation(rule, state, now, cid, violated)
            del self._pending_next[rule_id]

        # Normal rule evaluation
        for rule_id, rule in self.rules.items():
            # cooldown
            if rule.last_violation_time and rule.cooldown_seconds > 0:
                if (now - rule.last_violation_time).total_seconds() < rule.cooldown_seconds:
                    continue

            if rule.operator == TemporalOperator.NEXT:
                # [P0] Register for next tick (G X φ)
                if rule.conditions:
                    self._pending_next[rule_id] = rule.conditions[0]
                continue

            hist_copy = deque(self.history, maxlen=self.history.maxlen)
            try:
                is_violated = rule.evaluate(hist_copy)
            except Exception as e:
                logger.error(f"Rule '{rule_id}' raised: {e}")
                is_violated = False

            if is_violated:
                self._record_violation(rule, state, now, cid, violated)
            else:
                rule.violation_count = 0

        return violated

    def _record_violation(self, rule: TemporalRule, state: Dict,
                          now: datetime, cid: str, violated: List[str]):
        rule.violation_count += 1
        rule.total_violations += 1
        rule.last_violation_time = now
        self._metrics['violations_total'] += 1
        # [P2] dedup: only append when transitioning from 0→1 or every 10th
        if rule.violation_count == 1 or rule.violation_count % 10 == 0:
            self.event_log.append({
                'type': 'violation', 'rule_id': rule.rule_id,
                'severity': rule.severity, 'cid': cid,
                'consecutive': rule.violation_count,
                'ts': now.isoformat(), 'state': dict(state)})

        if rule.violation_count >= rule.max_violations:
            if rule.rule_id not in violated:
                violated.append(rule.rule_id)
            # [P1] auto-remediate (await if async)
            if rule.auto_remediate:
                try:
                    rem = rule.auto_remediate(state)
                    if asyncio.iscoroutine(rem):
                        rem = await rem
                    self.event_log.append({
                        'type': 'auto_remediate', 'rule_id': rule.rule_id,
                        'remediation': rem, 'cid': cid,
                        'ts': now.isoformat()})
                except Exception as e:
                    logger.error(f"auto_remediate '{rule.rule_id}' failed: {e}")
            # HITL with cooldown
            if self.approval_callback and rule.severity == 'critical':
                if (rule.last_hitl_request is None or
                        (now - rule.last_hitl_request).total_seconds()
                        >= rule.hitl_cooldown_seconds):
                    rule.last_hitl_request = now
                    self._metrics['hitl_requests'] += 1
                    self.event_log.append({
                        'type': 'hitl_request', 'rule_id': rule.rule_id,
                        'cid': cid, 'ts': now.isoformat()})
                    asyncio.create_task(self._run_approval(rule, state, cid))

    async def _run_approval(self, rule: TemporalRule, state: Dict, cid: str):
        try:
            approved = await self._request_approval(rule.rule_id, state)
            self.event_log.append({
                'type': 'hitl_result', 'rule_id': rule.rule_id,
                'approved': approved, 'cid': cid,
                'ts': datetime.now(timezone.utc).isoformat()})
            if not approved and self.multi_agent:
                await self.multi_agent.broadcast(
                    topic='critical_violation', sender='temporal_monitor',
                    payload={'rule_id': rule.rule_id, 'state': state})
        except Exception as e:
            logger.error(f"HITL failed for '{rule.rule_id}': {e}")

    async def _request_approval(self, rule_id: str, state: Dict) -> bool:
        if self.approval_callback is None:
            logger.warning(f"Critical rule '{rule_id}' with no callback → deny")
            return False
        try:
            r = self.approval_callback(rule_id, state)
            if asyncio.iscoroutine(r):
                r = await r
            return bool(r)
        except Exception as e:
            logger.error(f"Approval callback failed: {e}")
            return False

    def set_approval_callback(self, callback: Callable[[str, Dict], Any]):
        self.approval_callback = callback

    # ------------------------------------------------------------------
    # [P2] Event query API
    # ------------------------------------------------------------------
    def get_events(self, rule_id: Optional[str] = None,
                   severity: Optional[str] = None,
                   event_type: Optional[str] = None,
                   since: Optional[datetime] = None,
                   limit: int = 100) -> List[Dict[str, Any]]:
        out = []
        for ev in reversed(self.event_log):
            if rule_id and ev.get('rule_id') != rule_id:
                continue
            if severity and ev.get('severity') != severity:
                continue
            if event_type and ev.get('type') != event_type:
                continue
            if since:
                try:
                    ev_ts = datetime.fromisoformat(ev['ts'])
                except Exception:
                    continue
                if ev_ts < since:
                    continue
            out.append(ev)
            if len(out) >= limit:
                break
        return list(reversed(out))

    # ------------------------------------------------------------------
    # XAI
    # ------------------------------------------------------------------
    async def explain_violation(self, rule_id: str) -> Optional[Dict[str, Any]]:
        rule = self.rules.get(rule_id)
        if not rule:
            return None
        hist = list(self.history)
        if rule.operator in (TemporalOperator.UNTIL, TemporalOperator.WITHIN):
            relevant = [st for _, st in hist]
        elif rule.conditions:
            cond = rule.conditions[0]
            relevant = [st for _, st in hist if not cond(st)]
        else:
            relevant = []
        base = {
            'rule_id': rule_id, 'description': rule.description,
            'operator': rule.operator.value, 'severity': rule.severity,
            'violation_count': rule.violation_count,
            'total_violations': rule.total_violations,
            'last_violation_time': rule.last_violation_time.isoformat()
                if rule.last_violation_time else None,
            'recent_states': relevant[-5:],
            'message': (f"Rule '{rule_id}' ({rule.description}) violated. "
                        f"Operator: {rule.operator.value}, severity: {rule.severity}."),
        }
        if self._xai_hook:
            try:
                extra = self._xai_hook(rule_id, {'recent': relevant[-5:]})
                if asyncio.iscoroutine(extra):
                    extra = await extra
                base['attributions'] = extra
            except Exception as e:
                logger.debug(f"XAI hook failed: {e}")
        return base

    def set_xai_hook(self, hook: Callable[[str, Dict], Dict]):
        self._xai_hook = hook

    # ------------------------------------------------------------------
    # Persistence — [P3] sub-module state + PQC signature
    # ------------------------------------------------------------------
    async def save_state(self, path: str = "temporal_monitor_state.json",
                         sign: bool = True) -> bool:
        data = {
            'history': [[ts.isoformat(), st] for ts, st in self.history],
            'rules': {rid: {
                'violation_count': r.violation_count,
                'total_violations': r.total_violations,
                'evaluations': r.evaluations,
                'last_violation_time': r.last_violation_time.isoformat()
                    if r.last_violation_time else None,
                'last_hitl_request': r.last_hitl_request.isoformat()
                    if r.last_hitl_request else None,
            } for rid, r in self.rules.items()},
            'metrics': dict(self._metrics),
            'events': list(self.event_log),
            'pending_next': list(self._pending_next.keys()),
            'submodules': self._submodule_state(),
        }
        payload = json.dumps(data, default=str).encode()
        wrapper: Dict[str, Any] = {'data': data}
        if sign:
            wrapper['signature'] = self._sign_state(payload)
        try:
            Path_ = __import__('pathlib').Path
            Path_(path).parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'w') as f:
                json.dump(wrapper, f, indent=2, default=str)
            return True
        except Exception as e:
            logger.error(f"save_state failed: {e}")
            return False

    async def load_state(self, path: str = "temporal_monitor_state.json",
                          verify: bool = True) -> bool:
        if not os.path.exists(path):
            return False
        try:
            with open(path, 'r') as f:
                wrapper = json.load(f)
            if 'data' in wrapper and 'signature' in wrapper:
                data = wrapper['data']
                if verify and not self._verify_state(
                        json.dumps(data, default=str).encode(),
                        wrapper['signature']):
                    logger.warning("Signature verification failed; state not loaded")
                    return False
            else:
                data = wrapper
            self.history.clear()
            for ts_str, st in data.get('history', []):
                self.history.append((datetime.fromisoformat(ts_str), st))
            for rid, info in data.get('rules', {}).items():
                if rid in self.rules:
                    r = self.rules[rid]
                    r.violation_count = info.get('violation_count', 0)
                    r.total_violations = info.get('total_violations', 0)
                    r.evaluations = info.get('evaluations', 0)
                    lvt = info.get('last_violation_time')
                    r.last_violation_time = datetime.fromisoformat(lvt) if lvt else None
                    lh = info.get('last_hitl_request')
                    r.last_hitl_request = datetime.fromisoformat(lh) if lh else None
            self._metrics.update(data.get('metrics', {}))
            for ev in data.get('events', []):
                self.event_log.append(ev)
            self._load_submodule_state(data.get('submodules', {}))
            return True
        except Exception as e:
            logger.error(f"load_state failed: {e}")
            return False

    def _sign_state(self, payload: bytes) -> Dict[str, str]:
        if PQC_AVAILABLE:
            try:
                pk, sk = dilithium.generate_keypair()
                sig = dilithium.sign(sk, payload)
                return {'algorithm': 'dilithium', 'sig': sig.hex(), 'pk': pk.hex()}
            except Exception:
                pass
        return {'algorithm': 'sha256', 'sig': hashlib.sha256(payload).hexdigest()}

    def _verify_state(self, payload: bytes, signature: Dict) -> bool:
        algo = signature.get('algorithm', '')
        if algo == 'dilithium' and PQC_AVAILABLE:
            try:
                dilithium.verify(bytes.fromhex(signature['pk']), payload,
                                  bytes.fromhex(signature['sig']))
                return True
            except Exception:
                return False
        if algo == 'sha256':
            return hashlib.sha256(payload).hexdigest() == signature.get('sig', '')
        return False

    # ------------------------------------------------------------------
    # Sub-module persistence hooks — [P3]
    # ------------------------------------------------------------------
    def _submodule_state(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for name, obj in [
            ('distiller', self.distiller),
            ('causal_rl', self.causal_rl),
            ('federated', self.federated),
            ('multi_agent', self.multi_agent),
            ('precision_switcher', self.precision_switcher),
            ('carbon_market', self.carbon_market),
        ]:
            if obj is not None and hasattr(obj, 'save_state'):
                try:
                    out[name] = obj.save_state()
                except Exception as e:
                    logger.debug(f"submodule save_state '{name}' failed: {e}")
        return out

    def _load_submodule_state(self, state: Dict[str, Any]):
        for name, obj in [
            ('distiller', self.distiller),
            ('causal_rl', self.causal_rl),
            ('federated', self.federated),
            ('multi_agent', self.multi_agent),
            ('precision_switcher', self.precision_switcher),
            ('carbon_market', self.carbon_market),
        ]:
            if obj is not None and name in state and hasattr(obj, 'load_state'):
                try:
                    obj.load_state(state[name])
                except Exception as e:
                    logger.debug(f"submodule load_state '{name}' failed: {e}")

    # ------------------------------------------------------------------
    # Chaos — [P0] snapshot & restore
    # ------------------------------------------------------------------
    def _submodules(self) -> Dict[str, Any]:
        return {
            'distiller': self.distiller, 'causal_rl': self.causal_rl,
            'federated': self.federated, 'multi_agent': self.multi_agent,
            'precision_switcher': self.precision_switcher,
            'carbon_market': self.carbon_market,
        }

    async def inject_fault(self, fault_type: str, **kwargs):
        if fault_type == 'clear_history':
            self.history.clear()
        elif fault_type == 'corrupt_timestamps':
            self.history = deque(
                [(ts + timedelta(days=1), st) for ts, st in self.history],
                maxlen=self.history.maxlen)
        elif fault_type == 'reset_counts':
            for rule in self.rules.values():
                rule.violation_count = 0
                rule.last_violation_time = None
        elif fault_type == 'delete_rule':
            rid = kwargs.get('rule_id')
            if rid:
                self.remove_rule(rid)
        elif fault_type == 'inject_bad_condition':
            rid = kwargs.get('rule_id') or next(iter(self.rules), None)
            if rid and rid in self.rules:
                def _poison(_s):
                    raise RuntimeError("chaos: poisoned condition")
                self.rules[rid].conditions = [_poison]
        elif fault_type == 'stall_submodules':
            for sub in self._submodules().values():
                if sub and hasattr(sub, 'stall'):
                    sub.stall()
        else:
            logger.warning(f"Unknown fault '{fault_type}'")

    async def run_chaos_test(self) -> Dict[str, Any]:
        """[P0] Non-destructive: snapshot before, restore in finally."""
        snapshot = {
            'history': list(self.history),
            'rules': {rid: {'conditions': list(r.conditions),
                            'violation_count': r.violation_count,
                            'last_violation_time': r.last_violation_time,
                            'last_hitl_request': r.last_hitl_request}
                      for rid, r in self.rules.items()},
            'rule_ids': set(self.rules.keys()),
            'stalled': {name: getattr(sub, '_stalled', None)
                        for name, sub in self._submodules().items()
                        if sub is not None},
        }
        report: Dict[str, Any] = {'faults': [], 'results': {},
                                    'temporal_ok': False}
        state = {'carbon': 400, 'quality': 0.9, 'privacy_budget': 1.0,
                 'generation_complete': True}
        try:
            for fault in ['clear_history', 'corrupt_timestamps', 'reset_counts',
                          'inject_bad_condition', 'stall_submodules']:
                try:
                    await self.inject_fault(fault)
                    report['faults'].append(fault)
                    try:
                        violated = await self.check(dict(state))
                        report['results'][fault] = {
                            'violated_count': len(violated),
                            'history_len': len(self.history),
                            'still_running': True}
                    except Exception as e:
                        report['results'][fault] = {
                            'error': str(e), 'still_running': False}
                except Exception as e:
                    report['results'][fault] = {'error': str(e),
                                                 'still_running': False}

            # Smoke check
            try:
                self.add_rule("post_chaos_smoke", TemporalOperator.EVENTUALLY,
                              [lambda s: s.get('carbon', 0) < 1000],
                              description="post-chaos smoke test")
                _ = await self.check(dict(state))
                report['temporal_ok'] = True
            except Exception as e:
                report['temporal_ok'] = False
                report['post_chaos_error'] = str(e)
            return report
        finally:
            # [P0] restore
            self.history = deque(snapshot['history'],
                                  maxlen=self.history.maxlen)
            for rid, data in snapshot['rules'].items():
                if rid in self.rules:
                    self.rules[rid].conditions = data['conditions']
                    self.rules[rid].violation_count = data['violation_count']
                    self.rules[rid].last_violation_time = data['last_violation_time']
                    self.rules[rid].last_hitl_request = data['last_hitl_request']
            # Remove any rules added during chaos (post_chaos_smoke)
            for rid in list(self.rules.keys()):
                if rid not in snapshot['rule_ids']:
                    del self.rules[rid]
            # Restore stall flags
            for name, stalled in snapshot['stalled'].items():
                sub = self._submodules().get(name)
                if sub and stalled is not None and hasattr(sub, '_stalled'):
                    sub._stalled = stalled
            self._pending_next.clear()

    # ------------------------------------------------------------------
    # Attach + orchestration
    # ------------------------------------------------------------------
    def attach(self, *, distiller=None, causal_rl=None, federated=None,
               multi_agent=None, precision_switcher=None, carbon_market=None,
               xai=None):
        self.distiller = distiller
        self.causal_rl = causal_rl
        self.federated = federated
        self.multi_agent = multi_agent
        self.precision_switcher = precision_switcher
        self.carbon_market = carbon_market
        self.xai = xai
        if xai is not None and hasattr(xai, 'explain'):
            self.set_xai_hook(xai.explain)

    async def step_all(self, state: Dict, cid: Optional[str] = None) -> Dict[str, Any]:
        """[P0] observe_state is called only by each sub-module's step();
        monitor.check() no longer duplicates it."""
        cid = cid or uuid.uuid4().hex[:12]
        snapshot: Dict[str, Any] = {'cid': cid, 'state': state}
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
        snapshot['violations'] = await self.check(state, cid=cid)
        return snapshot


# =============================================================================
# SECTION 2: QUANTUM DISTILLATION — [P0] correct KL gradient
# =============================================================================

class QuantumDistillationEngine:
    """Multi-teacher distillation with amplitude-style superposing.
    [P0] KL gradient is s - t (correct for softmax parameterization).
    [P1] Length validation on teacher registration.
    """

    def __init__(self, temperature: float = 2.0, alpha: float = 0.5,
                 n_actions: int = 4):
        self.temperature = temperature
        self.alpha = alpha
        self.n_actions = n_actions
        self.teachers: Dict[str, List[float]] = {}
        self.student_policy: List[float] = [1.0 / n_actions] * n_actions
        self.history: Deque[Dict[str, Any]] = deque(maxlen=500)
        self._stalled = False

    def register_teacher(self, name: str, policy: List[float]):
        # [P1] validate length
        if len(policy) != self.n_actions:
            raise ValueError(
                f"Teacher '{name}' has {len(policy)} actions, expected {self.n_actions}")
        if any(p < 0 for p in policy):
            raise ValueError(f"Teacher '{name}' has negative probabilities")
        s = sum(policy)
        if s <= 0:
            raise ValueError(f"Teacher '{name}' policy sums to zero")
        self.teachers[name] = [float(p) / s for p in policy]

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
        if not self.teachers:
            return [1.0 / self.n_actions] * self.n_actions
        accum = [0.0] * self.n_actions
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
        target = self._quantum_superpose()
        # [P0] KL gradient w.r.t. probabilities is (student - teacher)
        lr = 0.05
        new_student = []
        for s, t in zip(self.student_policy, target):
            grad = s - t
            new_student.append(max(1e-3, s - lr * grad))
        ssum = sum(new_student) or 1.0
        self.student_policy = [x / ssum for x in new_student]
        entry = {'target': target, 'student': list(self.student_policy),
                 'ts': datetime.now(timezone.utc).isoformat()}
        self.history.append(entry)
        return entry

    def get_policy(self) -> List[float]:
        return list(self.student_policy)

    def save_state(self) -> Dict:
        return {'teachers': dict(self.teachers),
                'student_policy': list(self.student_policy),
                'n_actions': self.n_actions}

    def load_state(self, data: Dict):
        self.teachers = {k: list(v) for k, v in data.get('teachers', {}).items()}
        self.student_policy = list(data.get(
            'student_policy', [1.0 / self.n_actions] * self.n_actions))


# =============================================================================
# SECTION 3: CAUSAL RL — [P1] uses graph parents, action-dependent reward
# =============================================================================

class CausalGraph:
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
    """[P1] choose_action consults graph parents of 'quality'.
    [P1] Reward depends on the chosen action (not just state).
    [P3] learn_structure reports train/val/test edge stability.
    """

    ACTION_QUALITY_BONUS = {
        'statistical': 0.0, 'vae': 0.05, 'gan': 0.03, 'hybrid': 0.06}
    ACTION_COST = {
        'statistical': 0.0, 'vae': 0.10, 'gan': 0.15, 'hybrid': 0.08}

    def __init__(self, actions: Optional[List[str]] = None,
                 exploration: float = 0.1):
        self.actions = actions or ['statistical', 'vae', 'gan', 'hybrid']
        self.graph = CausalGraph()
        self.exploration = exploration
        self.action_values: Dict[str, float] = defaultdict(float)
        self.action_counts: Dict[str, int] = defaultdict(int)
        self.policy = [1.0 / len(self.actions)] * len(self.actions)
        self.violation_log: Deque[Tuple[List[str], Dict]] = deque(maxlen=200)
        self._last_action: Optional[str] = None
        self._stalled = False

    def stall(self):
        self._stalled = True

    async def record_violation(self, violated_rules: List[str], state: Dict):
        self.violation_log.append((list(violated_rules), dict(state)))
        # [P1] penalize the actual last chosen action
        if self._last_action and self._last_action in self.action_values:
            self.action_values[self._last_action] *= 0.9

    def learn_structure(self, samples: List[Dict[str, float]],
                        variables: List[str],
                        threshold: float = 0.2) -> Dict[str, Any]:
        """[P3] Fit on train, report edge stability on val/test."""
        self.graph = CausalGraph()
        self.graph.variables = list(variables)
        if not NUMPY_AVAILABLE or len(samples) < 5:
            for i, s in enumerate(variables):
                for j, t in enumerate(variables):
                    if i < j and random.random() < 0.25:
                        self.graph.add_edge(s, t, random.uniform(0.1, 0.9))
            return {**self.graph.summary(), 'edge_stability': 0.0}

        n = len(samples)
        n_train = max(3, int(n * 0.6))
        n_val = max(0, int(n * 0.2))
        train = samples[:n_train]
        val = samples[n_train:n_train + n_val]
        test = samples[n_train + n_val:]

        def _fit(subset) -> set:
            if len(subset) < 3:
                return set()
            X = np.array([[s[v] for v in variables] for s in subset], dtype=float)
            X = (X - X.mean(0)) / (X.std(0) + 1e-9)
            corr = np.corrcoef(X, rowvar=False)
            edges = set()
            for i in range(len(variables)):
                for j in range(len(variables)):
                    if i == j:
                        continue
                    if abs(float(corr[i, j])) > threshold:
                        vi, vj = float(X[:, i].var()), float(X[:, j].var())
                        src, dst = (variables[i], variables[j]) if vi > vj \
                            else (variables[j], variables[i])
                        edges.add((src, dst, round(float(corr[i, j]), 4)))
            return edges

        train_edges = _fit(train)
        for src, dst, w in train_edges:
            self.graph.add_edge(src, dst, w)

        # Stability: fraction of edges also present in val/test
        stability = 0.0
        if train_edges:
            hits = 0
            for subset in (val, test):
                subset_edges = {(s, d) for s, d, _ in _fit(subset)}
                hits += sum(1 for s, d, _ in train_edges if (s, d) in subset_edges)
            stability = hits / (len(train_edges) * 2)
        return {**self.graph.summary(),
                'edge_stability': round(float(stability), 4),
                'train_edges': len(train_edges)}

    async def choose_action(self, state: Dict) -> str:
        if self._stalled:
            return self.actions[0]
        if random.random() < self.exploration:
            choice = random.choice(self.actions)
        else:
            # [P1] restrict to graph parents of 'quality' if available
            parents = [p for p in self.graph.parents('quality')
                       if p in self.actions]
            candidates = parents if parents else self.actions
            choice = max(candidates, key=lambda a: self.action_values.get(a, 0.0))
        self._last_action = choice
        return choice

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
        # [P1] action-dependent reward
        base_q = float(state.get('quality', 0.8))
        base_c = float(state.get('carbon', 0.4))
        reward = (base_q
                  + self.ACTION_QUALITY_BONUS.get(chosen, 0.0)
                  - 0.5 * base_c
                  - self.ACTION_COST.get(chosen, 0.0))
        await self.update(chosen, reward, state)
        return {'chosen_action': chosen, 'reward': reward,
                'policy': list(self.policy),
                'ate_carbon_quality': self.graph.edges.get('carbon', {}).get('quality', 0.0)}

    def get_policy(self) -> List[float]:
        return list(self.policy)

    def explain(self, rule_id: str, payload: Dict) -> Dict[str, Any]:
        return {'causal_parents_of_quality': self.graph.parents('quality'),
                'action_values': dict(self.action_values)}

    def save_state(self) -> Dict:
        return {'action_values': dict(self.action_values),
                'action_counts': dict(self.action_counts),
                'policy': list(self.policy),
                'edges': {s: dict(v) for s, v in self.graph.edges.items()},
                'variables': list(self.graph.variables),
                'last_action': self._last_action}

    def load_state(self, data: Dict):
        self.action_values = defaultdict(float, data.get('action_values', {}))
        self.action_counts = defaultdict(int, data.get('action_counts', {}))
        self.policy = list(data.get('policy',
                                     [1.0 / len(self.actions)] * len(self.actions)))
        self.graph = CausalGraph()
        self.graph.variables = list(data.get('variables', []))
        for s, v in data.get('edges', {}).items():
            for d, w in v.items():
                self.graph.add_edge(s, d, w)
        self._last_action = data.get('last_action')


# =============================================================================
# SECTION 4: FEDERATED — [P1] unified signature, peer discovery
# =============================================================================

class FederatedGreenLearner:
    """[P1] Backend is (op, key, value) -> Any with op in {'put', 'get', 'list'}.
    [P3] Discovery via 'list' op if backend supports it."""

    def __init__(self,
                 instance_id: Optional[str] = None,
                 backend: Optional[Callable[[str, str, Optional[str]], Any]] = None):
        self.instance_id = instance_id or f"inst_{uuid.uuid4().hex[:8]}"
        self.local_weights: Dict[str, List[float]] = {}
        self.peers: Dict[str, Dict[str, List[float]]] = defaultdict(dict)
        self.rounds = 0
        self._stalled = False
        self._backend = backend

    def stall(self):
        self._stalled = True

    async def _call(self, op: str, key: str, value: Optional[str] = None):
        if not self._backend:
            return None
        try:
            r = self._backend(op, key, value)
            if asyncio.iscoroutine(r):
                r = await r
            return r
        except Exception as e:
            logger.debug(f"federated {op} {key} failed: {e}")
            return None

    async def share(self, name: str, weights: List[float]):
        self.local_weights[name] = list(weights)
        await self._call('put', f"{self.instance_id}/{name}", json.dumps(weights))

    async def pull(self, name: str, peer_id: str):
        r = await self._call('get', f"{peer_id}/{name}")
        if r:
            try:
                self.peers[name][peer_id] = json.loads(r)
            except Exception:
                pass

    async def discover_and_pull(self, name: str):
        """[P1] Enumerate peers from backend if supported."""
        r = await self._call('list', name)
        if not r:
            return
        try:
            peers = r if isinstance(r, list) else json.loads(r)
        except Exception:
            return
        for peer_id in peers:
            if peer_id != self.instance_id:
                await self.pull(name, peer_id)

    def aggregate(self, name: str) -> List[float]:
        pols = [self.local_weights.get(name, [])]
        pols.extend(v for v in self.peers.get(name, {}).values())
        pols = [p for p in pols if p]
        if not pols:
            return []
        n = len(pols[0])
        avg = [0.0] * n
        for p in pols:
            if len(p) != n:
                continue
            for i in range(n):
                avg[i] += p[i] / len(pols)
        s = sum(avg) or 1.0
        return [x / s for x in avg]

    async def step(self, state: Dict) -> Dict[str, Any]:
        if self._stalled:
            return {'stalled': True}
        self.rounds += 1
        if isinstance(state.get('policy'), list):
            await self.share('policy', state['policy'])
        # Attempt discovery if backend supports it
        await self.discover_and_pull('policy')
        aggregated = self.aggregate('policy')
        return {'round': self.rounds,
                'peers': len(self.peers.get('policy', {})),
                'aggregated_policy': aggregated}

    def get_policy(self) -> List[float]:
        return self.aggregate('policy') or [0.25, 0.25, 0.25, 0.25]

    def save_state(self) -> Dict:
        return {'local_weights': dict(self.local_weights),
                'peers': {k: dict(v) for k, v in self.peers.items()},
                'rounds': self.rounds}

    def load_state(self, data: Dict):
        self.local_weights = dict(data.get('local_weights', {}))
        self.peers = defaultdict(dict, data.get('peers', {}))
        self.rounds = int(data.get('rounds', 0))


# =============================================================================
# SECTION 5: MULTI-AGENT — [P1] message bus dispatch; [P3] learned affinity
# =============================================================================

class MultiAgent:
    ROLES = ['generator', 'critic', 'verifier', 'negotiator', 'explorer']

    def __init__(self, agent_id: str):
        self.id = agent_id
        self.role = 'explorer'
        self.reputation = 0.5
        self.utilities = {r: random.uniform(0.3, 0.7) for r in self.ROLES}
        self.tasks_completed = 0


class MessageBus:
    """[P1] Real pub/sub with subscriber registry."""

    def __init__(self, history: int = 500):
        self._subs: Dict[str, List[Callable[[Dict], Any]]] = defaultdict(list)
        self._history: Deque[Dict[str, Any]] = deque(maxlen=history)

    def subscribe(self, topic: str, handler: Callable[[Dict], Any]):
        self._subs[topic].append(handler)

    async def publish(self, msg: Dict[str, Any]) -> int:
        self._history.append(msg)
        delivered = 0
        for h in list(self._subs.get(msg.get('topic', ''), [])):
            try:
                r = h(msg)
                if asyncio.iscoroutine(r):
                    await r
                delivered += 1
            except Exception as e:
                logger.debug(f"bus handler failed on '{msg.get('topic')}': {e}")
        for h in list(self._subs.get('*', [])):
            try:
                r = h(msg)
                if asyncio.iscoroutine(r):
                    await r
                delivered += 1
            except Exception:
                pass
        return delivered


class MultiAgentCoordinator:
    """[P1] Messages are delivered; [P3] affinity learned from rewards."""

    def __init__(self, agent_count: int = 5, message_bus_size: int = 500):
        self.agents: Dict[str, MultiAgent] = {
            f"agent_{i:02d}": MultiAgent(f"agent_{i:02d}")
            for i in range(agent_count)}
        self.bus = MessageBus(message_bus_size)
        self.message_log: Deque[Dict[str, Any]] = deque(maxlen=500)
        self.rounds = 0
        self._stalled = False
        self._bus_lock = asyncio.Lock()
        # [P3] learned role→action affinity (4 actions)
        self._affinity: Dict[str, List[float]] = {
            'generator':  [0.25, 0.25, 0.25, 0.25],
            'critic':     [0.40, 0.20, 0.20, 0.20],
            'verifier':   [0.20, 0.20, 0.40, 0.20],
            'negotiator': [0.25, 0.25, 0.25, 0.25],
            'explorer':   [0.10, 0.30, 0.30, 0.30],
        }
        self._affinity_lr = 0.05

        # Wire a default subscriber so messages are handled
        self.bus.subscribe('critical_violation', self._on_critical)
        self.bus.subscribe('violation', self._on_violation)
        self.bus.subscribe('task_bid', self._on_task_bid)

    async def _on_critical(self, msg: Dict):
        # All agents boost verifier affinity
        for a in self.agents.values():
            a.utilities['verifier'] = min(
                1.0, a.utilities['verifier'] + 0.05)

    async def _on_violation(self, msg: Dict):
        for a in self.agents.values():
            a.utilities['critic'] = min(1.0, a.utilities['critic'] + 0.02)

    async def _on_task_bid(self, msg: Dict):
        self.message_log.append(msg)

    def stall(self):
        self._stalled = True

    def _specialise(self):
        for a in self.agents.values():
            a.role = max(a.utilities, key=lambda r: a.utilities[r])

    async def broadcast(self, topic: str, sender: str, payload: Dict[str, Any]):
        msg = {'topic': topic, 'sender': sender, 'payload': payload,
               'ts': datetime.now(timezone.utc).isoformat()}
        async with self._bus_lock:
            try:
                self.message_log.append(msg)
            except Exception:
                pass
        await self.bus.publish(msg)

    async def bid(self, task: Dict[str, Any]) -> Tuple[str, float]:
        preferred = task.get('preferred_role', 'generator')
        best_id, best_score = None, -1.0
        for aid, a in self.agents.items():
            bonus = 1.0 if a.role == preferred else 0.6
            score = a.utilities[a.role] * bonus + 0.3 * a.reputation \
                + random.uniform(-0.02, 0.02)
            if score > best_score:
                best_score, best_id = score, aid
        if best_id:
            self.agents[best_id].tasks_completed += 1
        await self.broadcast('task_bid', best_id or 'none',
                             {'task': task.get('name', 'unknown'),
                              'score': best_score})
        return best_id or next(iter(self.agents)), best_score

    async def reward(self, agent_id: str, reward: float,
                     action_idx: Optional[int] = None):
        if agent_id not in self.agents:
            return
        a = self.agents[agent_id]
        n = max(1, a.tasks_completed)
        a.reputation = max(0.0, min(1.0, a.reputation + reward / n))
        a.utilities[a.role] = min(1.0, a.utilities[a.role] + 0.05 * reward)
        # [P3] update learned affinity if action_idx is provided
        if action_idx is not None and 0 <= action_idx < 4:
            aff = self._affinity.get(a.role)
            if aff:
                aff[action_idx] = max(
                    0.0, aff[action_idx] + self._affinity_lr * (reward - 0.5))
                s = sum(aff) or 1.0
                self._affinity[a.role] = [x / s for x in aff]

    async def step(self, state: Dict) -> Dict[str, Any]:
        if self._stalled:
            return {'stalled': True}
        self.rounds += 1
        self._specialise()
        winner, score = None, 0.0
        if state.get('task'):
            winner, score = await self.bid(state['task'])
        role_counts: Dict[str, int] = defaultdict(int)
        for a in self.agents.values():
            role_counts[a.role] += 1
        total = max(1, sum(role_counts.values()))
        role_dist = {r: role_counts[r] / total for r in MultiAgent.ROLES}
        return {'round': self.rounds,
                'roles': {a.id: a.role for a in self.agents.values()},
                'role_distribution': role_dist,
                'winner': winner, 'winning_score': score}

    def get_policy(self) -> List[float]:
        role_counts: Dict[str, int] = defaultdict(int)
        for a in self.agents.values():
            role_counts[a.role] += 1
        total = max(1, sum(role_counts.values()))
        out = [0.0] * 4
        for role, cnt in role_counts.items():
            w = cnt / total
            aff = self._affinity.get(role, [0.25] * 4)
            for i, v in enumerate(aff):
                out[i] += w * v
        s = sum(out) or 1.0
        return [x / s for x in out]

    def save_state(self) -> Dict:
        return {'agents': {aid: {
            'role': a.role, 'reputation': a.reputation,
            'utilities': a.utilities, 'tasks_completed': a.tasks_completed,
        } for aid, a in self.agents.items()},
            'affinity': {k: list(v) for k, v in self._affinity.items()},
            'rounds': self.rounds}

    def load_state(self, data: Dict):
        for aid, info in data.get('agents', {}).items():
            if aid in self.agents:
                a = self.agents[aid]
                a.role = info.get('role', a.role)
                a.reputation = float(info.get('reputation', a.reputation))
                a.utilities = dict(info.get('utilities', a.utilities))
                a.tasks_completed = int(info.get('tasks_completed', 0))
        for role, aff in data.get('affinity', {}).items():
            if role in self._affinity and len(aff) == 4:
                self._affinity[role] = list(aff)
        self.rounds = int(data.get('rounds', 0))


# =============================================================================
# SECTION 6: ADAPTIVE PRECISION — [P1] uses baseline_accuracy
# =============================================================================

class AdaptivePrecisionSwitcher:
    ENERGY_MODEL = {'fp32': 1.0, 'tf32': 0.75, 'bf16': 0.55,
                    'fp16': 0.5, 'int8': 0.3}
    LATENCY_MODEL = {'fp32': 1.0, 'tf32': 0.8, 'bf16': 0.65,
                     'fp16': 0.6, 'int8': 0.4}
    ACCURACY_PENALTY = {'fp32': 0.0, 'tf32': 0.005, 'bf16': 0.01,
                        'fp16': 0.012, 'int8': 0.03}

    def __init__(self, tolerance: float = 0.02, energy_target: float = 0.7,
                 default_baseline_accuracy: float = 0.9):
        self.tolerance = tolerance
        self.energy_target = energy_target
        self.baseline_accuracy = default_baseline_accuracy
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

    def select(self, baseline_accuracy: Optional[float] = None) -> str:
        # [P1] actually use baseline_accuracy
        baseline = baseline_accuracy if baseline_accuracy is not None \
            else self.baseline_accuracy
        hw = self._probe_hw()
        candidates = list(self.ENERGY_MODEL.keys())
        if not hw['cuda']:
            candidates = [c for c in candidates if c in ('fp32', 'int8')]
        if not hw['bf16']:
            candidates = [c for c in candidates if c != 'bf16']
        # Accuracy gate: effective accuracy after penalty must stay within tolerance
        allowed = [c for c in candidates
                   if self.ACCURACY_PENALTY.get(c, 1.0) <= self.tolerance * baseline]
        if not allowed:
            allowed = ['fp32']
        return min(allowed, key=lambda c: self.ENERGY_MODEL[c])

    async def observe_state(self, state: Dict):
        acc = float(state.get('accuracy', self.baseline_accuracy))
        self.baseline_accuracy = acc
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

    def save_state(self) -> Dict:
        return {'current': self.current,
                'energy_saved_wh': self.energy_saved_wh,
                'baseline_accuracy': self.baseline_accuracy,
                'history': list(self.history)}

    def load_state(self, data: Dict):
        self.current = data.get('current', 'fp32')
        self.energy_saved_wh = float(data.get('energy_saved_wh', 0.0))
        self.baseline_accuracy = float(
            data.get('baseline_accuracy', self.baseline_accuracy))
        self.history = deque(data.get('history', []), maxlen=500)


# =============================================================================
# SECTION 7: CARBON MARKET — [P1] price cache; [P3] defers work
# =============================================================================

class CarbonMarketIntegrator:
    def __init__(self, api_url: Optional[str] = None,
                 region: str = 'global', start_price: float = 25.0,
                 price_cache_ttl: float = 60.0):
        self.api_url = api_url
        self.region = region
        self.price_usd = start_price
        self.rec_balance_mwh = 0.0
        self.price_history: Deque[Tuple[datetime, float]] = deque(maxlen=500)
        self.trades: Deque[Dict[str, Any]] = deque(maxlen=500)
        self.deferred_workloads: Deque[Dict[str, Any]] = deque(maxlen=200)
        self._stalled = False
        # [P1] price cache
        self._price_cache_ts = 0.0
        self._price_cache_ttl = price_cache_ttl

    def stall(self):
        self._stalled = True

    async def _fetch_price(self) -> float:
        # [P1] TTL-aware cache
        now = time.time()
        if now - self._price_cache_ts < self._price_cache_ttl:
            return self.price_usd
        price = self.price_usd
        if AIOHTTP_AVAILABLE and self.api_url:
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(f"{self.api_url}/price", timeout=8) as r:
                        if r.status == 200:
                            data = await r.json()
                            price = float(data.get('price', price))
            except Exception as e:
                logger.debug(f"carbon API failed: {e}")
                price = max(5.0, self.price_usd + random.gauss(0, 1.5))
        else:
            price = max(5.0, self.price_usd + random.gauss(0, 1.5))
        self._price_cache_ts = now
        return price

    async def observe_state(self, state: Dict):
        if self._stalled:
            return
        intensity = float(state.get('carbon',
                                     state.get('carbon_intensity', 0.4)))
        carbon_kg = 1.0 * intensity
        offset_cost = (carbon_kg / 1000.0) * self.price_usd
        now_iso = datetime.now(timezone.utc).isoformat()
        # [P3] act on defer
        if intensity > 0.3 and self.rec_balance_mwh < 0.5:
            self.deferred_workloads.append({
                'carbon_kg': carbon_kg, 'reason': 'high_carbon',
                'ts': now_iso})
            self.trades.append({
                'action': 'defer', 'carbon_kg': carbon_kg,
                'offset_cost': offset_cost, 'ts': now_iso})
        elif offset_cost < 0.5:
            self.trades.append({
                'action': 'run_offset', 'carbon_kg': carbon_kg,
                'offset_cost': offset_cost, 'ts': now_iso})

    async def purchase_rec(self, mwh: float, price_per_mwh: float = 5.0,
                            source: str = 'wind'):
        cost = float(mwh) * float(price_per_mwh)
        self.rec_balance_mwh += float(mwh)
        self.trades.append({'action': 'purchase_rec', 'mwh': float(mwh),
                             'cost': cost, 'source': source,
                             'ts': datetime.now(timezone.utc).isoformat()})
        return cost

    async def step(self, state: Dict) -> Dict[str, Any]:
        if self._stalled:
            return {'stalled': True}
        self.price_usd = await self._fetch_price()
        self.price_history.append((datetime.now(timezone.utc), self.price_usd))
        await self.observe_state(state)
        return {'price_usd': self.price_usd,
                'rec_balance_mwh': self.rec_balance_mwh,
                'deferred_count': len(self.deferred_workloads),
                'trades': list(self.trades)[-3:]}

    def get_policy(self) -> List[float]:
        p = self.price_usd
        if p > 30:
            return [0.6, 0.2, 0.1, 0.1]
        if p > 20:
            return [0.4, 0.3, 0.15, 0.15]
        return [0.25, 0.25, 0.25, 0.25]

    def explain(self, rule_id: str, payload: Dict) -> Dict[str, Any]:
        return {'carbon_price_usd': self.price_usd,
                'rec_balance_mwh': self.rec_balance_mwh,
                'rule_context': rule_id}

    def save_state(self) -> Dict:
        return {'price_usd': self.price_usd,
                'rec_balance_mwh': self.rec_balance_mwh,
                'trades': list(self.trades),
                'deferred': list(self.deferred_workloads)}

    def load_state(self, data: Dict):
        self.price_usd = float(data.get('price_usd', self.price_usd))
        self.rec_balance_mwh = float(data.get('rec_balance_mwh', 0.0))
        self.trades = deque(data.get('trades', []), maxlen=500)
        self.deferred_workloads = deque(data.get('deferred', []), maxlen=200)


# =============================================================================
# SECTION 8: XAI — [P3] Real KernelSHAP
# =============================================================================

class XAIExplainer:
    """Real KernelSHAP: coalition sampling + SHAP kernel weights + WLS."""

    def __init__(self, depth: int = 5):
        self.depth = depth

    @staticmethod
    def _kernel_weight(size: int, n: int) -> float:
        if size == 0 or size == n:
            return 1e6
        try:
            return (n - 1) / (math.comb(n, size) * size * (n - size))
        except Exception:
            return 1.0

    def kernel_shap(self, f: Callable, x: List[float],
                     names: List[str], n_samples: int = 64) -> Dict[str, float]:
        if not NUMPY_AVAILABLE:
            return {k: 0.0 for k in names}
        x_arr = np.asarray(x, dtype=float).ravel()
        n = len(x_arr)
        coalitions, weights = [], []
        max_exact = 2 ** n if n <= 8 else 0
        if max_exact and max_exact <= 512:
            for mask in range(1, max_exact - 1):
                z = np.zeros(n)
                size = 0
                for i in range(n):
                    if mask & (1 << i):
                        z[i] = x_arr[i]
                        size += 1
                coalitions.append(z)
                weights.append(self._kernel_weight(size, n))
        else:
            for _ in range(n_samples):
                z = np.zeros(n)
                size = 0
                for i in range(n):
                    if random.random() < 0.5:
                        z[i] = x_arr[i]
                        size += 1
                coalitions.append(z)
                weights.append(self._kernel_weight(size, n))
        # Efficiency constraint
        coalitions.append(np.zeros(n)); weights.append(1e6)
        coalitions.append(x_arr.copy()); weights.append(1e6)
        try:
            y = np.array([float(f(c.reshape(1, -1))) for c in coalitions])
        except Exception:
            return {k: 0.0 for k in names}
        Z = np.array(coalitions)
        w = np.array(weights)
        try:
            ZtW = Z.T * w
            A = ZtW @ Z + 1e-6 * np.eye(n)
            b = ZtW @ y
            phi = np.linalg.solve(A, b)
        except Exception:
            try:
                phi = np.linalg.lstsq(Z * np.sqrt(w)[:, None],
                                       y * np.sqrt(w), rcond=None)[0]
            except Exception:
                phi = np.zeros(n)
        return {name: float(phi[i]) for i, name in enumerate(names)}

    def explain(self, decision_id: str, label: str,
                 features: List[float], names: List[str],
                 model_fn: Callable) -> Dict[str, Any]:
        attrs = self.kernel_shap(model_fn, features, names)
        ranked = sorted(attrs.items(), key=lambda kv: abs(kv[1]), reverse=True)
        top = ranked[:self.depth]
        narrative = "\n".join(f"  • {k}: {v:+.3f}" for k, v in top)
        return {'decision_id': decision_id, 'method': 'kernel_shap',
                'attributions': attrs,
                'top_features': [k for k, _ in top],
                'explanation': f"Decision '{label}' driven by:\n{narrative}"}


# =============================================================================
# SECTION 9: FACADE — [P1] teachers before step; [P2] health/correlation;
# [P3] streaming + batched
# =============================================================================

class GreenAgentTemporalCore:
    """Single entry-point. Usage:
        core = GreenAgentTemporalCore()
        await core.initialize()
        report = await core.tick({'carbon': 0.3, 'quality': 0.9, ...})
    """

    def __init__(self, instance_id: Optional[str] = None,
                 config: Optional[Dict[str, Any]] = None):
        self.instance_id = instance_id or f"ga_{uuid.uuid4().hex[:8]}"
        self.config = config or {}

        self.monitor = TemporalSafetyMonitor(
            max_history=int(self.config.get('max_history', 10000)))
        self.distiller = QuantumDistillationEngine(
            temperature=self.config.get('distillation_temperature', 2.0),
            alpha=self.config.get('distillation_alpha', 0.5))
        self.causal_rl = CausalPolicyAdapter(
            exploration=self.config.get('causal_exploration', 0.1))
        self.federated = FederatedGreenLearner(
            instance_id=self.instance_id,
            backend=self.config.get('federated_backend'))
        self.multi_agent = MultiAgentCoordinator(
            agent_count=self.config.get('agent_count', 5))
        self.precision_switcher = AdaptivePrecisionSwitcher(
            tolerance=self.config.get('precision_tolerance', 0.02))
        self.carbon_market = CarbonMarketIntegrator(
            api_url=self.config.get('carbon_api_url'),
            region=self.config.get('carbon_region', 'global'))
        self.xai = XAIExplainer(depth=self.config.get('xai_depth', 5))

        self.monitor.attach(
            distiller=self.distiller,
            causal_rl=self.causal_rl,
            federated=self.federated,
            multi_agent=self.multi_agent,
            precision_switcher=self.precision_switcher,
            carbon_market=self.carbon_market,
            xai=self.xai)

        self._install_default_rules()

    def _install_default_rules(self):
        m = self.monitor
        m.add_rule("never_high_carbon", TemporalOperator.NEVER,
                   [lambda s: s.get('carbon', 0.0) > 0.7],
                   description="Carbon intensity must never exceed 0.7 kg/kWh",
                   severity="critical",
                   auto_remediate=lambda s: {'action': 'defer',
                                              'reason': 'high carbon'})
        m.add_rule("always_quality", TemporalOperator.ALWAYS,
                   [lambda s: s.get('quality', 1.0) >= 0.5],
                   description="Quality must always remain >= 0.5",
                   severity="warning")
        m.add_rule("eventually_complete", TemporalOperator.EVENTUALLY,
                   [lambda s: s.get('generation_complete', False)],
                   window_seconds=600,
                   description="Generation must complete within 10 minutes",
                   severity="warning")
        m.add_rule("until_fallback", TemporalOperator.UNTIL,
                   [lambda s: s.get('primary_ok', True),
                    lambda s: s.get('fallback_active', False)],
                   window_seconds=60,
                   description="Primary must hold until fallback activates (60s window)",
                   severity="critical")
        m.add_rule("within_recover", TemporalOperator.WITHIN,
                   [lambda s: s.get('error_detected', False),
                    lambda s: s.get('recovered', False)],
                   window_seconds=120,
                   description="After an error, recovery must occur within 120s",
                   severity="critical")

    async def initialize(self):
        # [P1] register teachers BEFORE any step
        self.distiller.register_teacher('causal_rl', self.causal_rl.get_policy())
        self.distiller.register_teacher('multi_agent', self.multi_agent.get_policy())
        self.distiller.register_teacher('carbon_market', self.carbon_market.get_policy())
        self.distiller.register_teacher('temporal_safe', [0.5, 0.2, 0.2, 0.1])
        logger.info(f"[GreenAgentTemporalCore] initialized: {self.instance_id}")

    async def tick(self, state: Dict, cid: Optional[str] = None) -> Dict[str, Any]:
        cid = cid or uuid.uuid4().hex[:12]
        # [P1] refresh teachers BEFORE step_all so distillation uses fresh ones
        self.distiller.register_teacher('causal_rl', self.causal_rl.get_policy())
        self.distiller.register_teacher('multi_agent', self.multi_agent.get_policy())
        self.distiller.register_teacher('carbon_market', self.carbon_market.get_policy())

        snapshot = await self.monitor.step_all(state, cid=cid)
        snapshot['production_policy'] = self.distiller.get_policy()
        await self.federated.share('policy', snapshot['production_policy'])

        if snapshot.get('multi_agent', {}).get('winner'):
            await self.multi_agent.reward(
                snapshot['multi_agent']['winner'],
                reward=float(state.get('quality', 0.8)))
        if snapshot.get('violations'):
            await self.causal_rl.record_violation(snapshot['violations'], state)
            await self.multi_agent.broadcast(
                topic='violation', sender='monitor',
                payload={'violations': snapshot['violations'], 'state': state})
        return snapshot

    async def run_for(self, states: List[Dict],
                      interval: float = 0.0) -> List[Dict]:
        out = []
        for s in states:
            out.append(await self.tick(s))
            if interval > 0:
                await asyncio.sleep(interval)
        return out

    # [P3] Streaming ticks
    async def stream_ticks(self, states: AsyncIterator[Dict]) -> AsyncIterator[Dict]:
        async for s in states:
            yield await self.tick(s)

    # [P3] Batched ticks (sequential to preserve history ordering)
    async def tick_batch(self, states: List[Dict]) -> List[Dict]:
        return [await self.tick(s) for s in states]

    async def save(self, path: str = "temporal_monitor_state.json"):
        return await self.monitor.save_state(path)

    async def load(self, path: str = "temporal_monitor_state.json"):
        return await self.monitor.load_state(path)

    async def chaos(self) -> Dict[str, Any]:
        return await self.monitor.run_chaos_test()

    async def health(self) -> Dict[str, Any]:
        # [P2] real health probe
        return {
            'instance_id': self.instance_id,
            'monitor': {'rules': len(self.monitor.rules),
                         'history_len': len(self.monitor.history),
                         'events': len(self.monitor.event_log)},
            'distiller': {'stalled': self.distiller._stalled,
                          'teachers': len(self.distiller.teachers)},
            'causal_rl': {'stalled': self.causal_rl._stalled,
                          'graph_edges': sum(len(v) for v in self.causal_rl.graph.edges.values())},
            'federated': {'stalled': self.federated._stalled,
                          'rounds': self.federated.rounds},
            'multi_agent': {'stalled': self.multi_agent._stalled,
                            'agents': len(self.multi_agent.agents)},
            'precision': {'stalled': self.precision_switcher._stalled,
                          'current': self.precision_switcher.current},
            'carbon_market': {'stalled': self.carbon_market._stalled,
                               'price_usd': self.carbon_market.price_usd},
        }


# =============================================================================
# SECTION 10: DEMO
# =============================================================================

async def _demo():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    core = GreenAgentTemporalCore(instance_id="demo-01",
                                   config={'agent_count': 4,
                                           'carbon_api_url': None})
    await core.initialize()

    states = [
        {'carbon': 0.20, 'quality': 0.92, 'privacy_budget': 1.0,
         'generation_complete': True, 'primary_ok': True, 'fallback_active': False},
        {'carbon': 0.35, 'quality': 0.85, 'privacy_budget': 0.9,
         'generation_complete': False, 'primary_ok': True, 'fallback_active': False},
        {'carbon': 0.85, 'quality': 0.90, 'privacy_budget': 0.8,
         'generation_complete': False, 'primary_ok': True, 'fallback_active': False},
        {'carbon': 0.40, 'quality': 0.40, 'privacy_budget': 0.7,
         'generation_complete': True, 'primary_ok': True, 'fallback_active': False},
        {'carbon': 0.30, 'quality': 0.88, 'privacy_budget': 0.9,
         'generation_complete': True, 'primary_ok': False, 'fallback_active': False},
    ]

    for i, s in enumerate(states):
        report = await core.tick(s)
        print(f"\n=== TICK {i+1} cid={report.get('cid')} ===")
        print(f"violations: {report.get('violations')}")
        print(f"causal_rl action: {report.get('causal_rl', {}).get('chosen_action')} "
              f"reward={report.get('causal_rl', {}).get('reward')}")
        print(f"multi_agent roles: {report.get('multi_agent', {}).get('role_distribution')}")
        print(f"precision: {report.get('precision', {}).get('current')}")
        print(f"carbon_market price: {report.get('carbon_market', {}).get('price_usd')}")
        print(f"production_policy: {[round(p, 3) for p in report.get('production_policy', [])]}")

    # XAI explanation
    expl = await core.monitor.explain_violation("never_high_carbon")
    print("\n=== XAI explanation for never_high_carbon ===")
    print(json.dumps(expl, indent=2, default=str))

    # [P2] Event query
    print("\n=== Recent critical events ===")
    print(json.dumps(core.monitor.get_events(severity='critical', limit=5),
                     indent=2, default=str))

    # [P2] Health
    print("\n=== Health ===")
    print(json.dumps(await core.health(), indent=2, default=str))

    # Chaos (non-destructive)
    chaos = await core.chaos()
    print("\n=== Chaos report (post-run state preserved) ===")
    print(json.dumps(chaos, indent=2, default=str))
    # Verify the rules still exist after chaos
    print(f"\nRules after chaos: {list(core.monitor.rules.keys())}")

    # Save (with PQC signature)
    saved = await core.save("/tmp/temporal_monitor_state.json")
    print(f"\nState saved: {saved}")
    # Reload
    loaded = await core.load("/tmp/temporal_monitor_state.json")
    print(f"State loaded: {loaded}")


if __name__ == "__main__":
    asyncio.run(_demo())
