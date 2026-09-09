#!/usr/bin/env python3
"""
Temporal Logic / Formal Verification for Safety‑Critical Policies.

Enhanced version: simple but expressive temporal rule engine.
Supports operators: ALWAYS, EVENTUALLY, UNTIL, NEVER, NEXT, WITHIN.
Includes Boolean conditions, history tracking, cooldown, explanation (XAI),
human approval hook, persistence, and chaos testing.
"""

import asyncio
import logging
import json
import os
from typing import Dict, Any, List, Callable, Optional, Deque, Tuple, Union
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from enum import Enum

logger = logging.getLogger(__name__)


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
    ):
        self.rule_id = rule_id
        self.operator = operator
        self.conditions = conditions  # list of callables; order matters for UNTIL/WITHIN
        self.window_seconds = window_seconds
        self.max_violations = max_violations
        self.cooldown_seconds = cooldown_seconds
        self.description = description or rule_id
        self.violation_count = 0
        self.last_violation_time: Optional[datetime] = None

    def evaluate(self, history: Deque[Tuple[datetime, Dict]]) -> bool:
        """
        Evaluate the rule against a history of (timestamp, state).
        Returns True if the rule is currently violated.
        """
        if not history:
            return False

        # Prune history to window if window_seconds > 0
        if self.window_seconds > 0:
            cutoff = history[-1][0] - timedelta(seconds=self.window_seconds)
            while history and history[0][0] < cutoff:
                history.popleft()

        # For operators that need two conditions, ensure we have at least 2 states if necessary
        if self.operator == TemporalOperator.UNTIL and len(self.conditions) >= 2:
            cond_a = self.conditions[0]
            cond_b = self.conditions[1]
            # UNTIL: A must hold until B becomes true (within window)
            # Violation if B never becomes true and A eventually false? Actually UNTIL violation if B hasn't occurred and A becomes false before B.
            # Simplified: If we have any state where A is false and B has not yet been true, violation.
            b_seen = False
            for ts, state in history:
                if cond_b(state):
                    b_seen = True
                    break
                if not cond_a(state):
                    return True  # A failed before B
            if not b_seen:
                # If window ended and B never came, violation if A is still required
                return True
            return False

        elif self.operator == TemporalOperator.WITHIN and len(self.conditions) >= 2:
            trigger = self.conditions[0]
            response = self.conditions[1]
            # WITHIN: after trigger becomes true, response must become true within window
            trigger_seen = False
            trigger_time = None
            for ts, state in history:
                if not trigger_seen:
                    if trigger(state):
                        trigger_seen = True
                        trigger_time = ts
                else:
                    if response(state):
                        return False  # satisfied
                    # check if window expired
                    if self.window_seconds > 0 and (ts - trigger_time).total_seconds() > self.window_seconds:
                        return True
            # If trigger was seen but no response within window (or no more history), violation
            if trigger_seen:
                # If window_seconds is 0, we require response in same state? For simplicity, require response in any subsequent state.
                # So violation if no response observed after trigger
                # Since we checked response in loop, if we exit loop and trigger_seen but not returned False, then no response observed -> violation
                return True
            return False

        # For unary operators
        cond = self.conditions[0] if self.conditions else None
        if cond is None:
            return False

        if self.operator == TemporalOperator.ALWAYS:
            # violation if any state violates condition
            for _, state in history:
                if not cond(state):
                    return True
            return False

        elif self.operator == TemporalOperator.EVENTUALLY:
            # violation if condition never becomes true in history (within window)
            # If window_seconds is 0, we just need to see if any state satisfies
            if self.window_seconds == 0:
                return not any(cond(state) for _, state in history)
            else:
                # Check if within window there is at least one satisfying state
                cutoff = history[-1][0] - timedelta(seconds=self.window_seconds)
                for ts, state in history:
                    if ts >= cutoff and cond(state):
                        return False
                return True  # no satisfying state within window

        elif self.operator == TemporalOperator.NEXT:
            # violation if the next state (after current) does not satisfy condition
            # We define "next" as the state right after the most recent one, i.e., we need at least two states.
            # If history length < 2, we cannot evaluate; assume not violated.
            if len(history) < 2:
                return False
            return not cond(history[-1][1])

        elif self.operator == TemporalOperator.NEVER:
            # violation if any state satisfies condition
            return any(cond(state) for _, state in history)

        # Default: simple condition on current state (legacy)
        return cond(history[-1][1]) if history else False


class TemporalSafetyMonitor:
    def __init__(self):
        self.rules: Dict[str, TemporalRule] = {}
        self.history: Deque[Tuple[datetime, Dict]] = deque(maxlen=10000)
        self.approval_callback: Optional[Callable[[str, Dict], bool]] = None

    def add_rule(
        self,
        rule_id: str,
        operator: TemporalOperator,
        conditions: Union[Callable[[Dict], bool], List[Callable[[Dict], bool]]],
        window_seconds: float = 0.0,
        max_violations: int = 1,
        cooldown_seconds: float = 0.0,
        description: str = "",
    ):
        """
        Add a temporal rule.

        For UNTIL and WITHIN, conditions must be a list of two callables [A, B].
        Otherwise, it can be a single callable or list containing one callable.
        """
        if isinstance(conditions, list):
            cond_list = conditions
        else:
            cond_list = [conditions]

        if operator in (TemporalOperator.UNTIL, TemporalOperator.WITHIN):
            if len(cond_list) < 2:
                raise ValueError(f"{operator.value} requires two conditions")

        rule = TemporalRule(
            rule_id=rule_id,
            operator=operator,
            conditions=cond_list,
            window_seconds=window_seconds,
            max_violations=max_violations,
            cooldown_seconds=cooldown_seconds,
            description=description,
        )
        self.rules[rule_id] = rule
        logger.info(f"Added temporal rule '{rule_id}' ({operator.value})")

    async def check(self, state: Dict) -> List[str]:
        """
        Record current state and evaluate all rules.
        Returns list of violated rule IDs.
        """
        now = datetime.now(timezone.utc)
        self.history.append((now, state))

        violated = []
        for rule_id, rule in self.rules.items():
            # Cooldown check
            if rule.last_violation_time and rule.cooldown_seconds > 0:
                elapsed = (now - rule.last_violation_time).total_seconds()
                if elapsed < rule.cooldown_seconds:
                    # Skip evaluation during cooldown (rule is suppressed)
                    continue

            # Evaluate on a copy of history pruned for this rule
            hist_copy = self.history.copy()  # shallow copy; we might modify in rule.evaluate
            is_violated = rule.evaluate(hist_copy)

            if is_violated:
                rule.violation_count += 1
                rule.last_violation_time = now
                if rule.violation_count >= rule.max_violations:
                    violated.append(rule_id)
                    # If human approval is required, request it (optional)
                    if self.approval_callback:
                        approved = await self._request_approval(rule_id, state)
                        if not approved:
                            # If not approved, maybe reset violation count or log
                            logger.warning(f"Human approval not granted for rule {rule_id}")
                            # We could choose to keep violation flag; for now, we leave it.
            else:
                rule.violation_count = 0

        return violated

    async def _request_approval(self, rule_id: str, state: Dict) -> bool:
        """Request human approval for a critical violation. Default auto‑denies if no callback."""
        if self.approval_callback is None:
            logger.warning(f"Rule {rule_id} violated but no approval callback set. Auto‑denying.")
            return False
        result = self.approval_callback(rule_id, state)
        if asyncio.iscoroutine(result):
            return await result
        return bool(result)

    def set_approval_callback(self, callback: Callable[[str, Dict], bool]):
        self.approval_callback = callback

    # ------------------ Explainability (XAI) ------------------
    async def explain_violation(self, rule_id: str) -> Optional[Dict[str, Any]]:
        """Generate explanation for latest violation of a rule."""
        rule = self.rules.get(rule_id)
        if not rule:
            return None
        # Find recent states that caused violation
        relevant_states = []
        hist_copy = self.history.copy()
        for ts, st in hist_copy:
            # We can check each state against the condition(s)
            if rule.operator in (TemporalOperator.UNTIL, TemporalOperator.WITHIN):
                # For two-condition rules, just note the latest state
                relevant_states.append(st)
            else:
                cond = rule.conditions[0]
                if not cond(st):
                    relevant_states.append(st)
        # Keep only last few
        recent = relevant_states[-3:]
        explanation = {
            'rule_id': rule_id,
            'description': rule.description,
            'operator': rule.operator.value,
            'violation_count': rule.violation_count,
            'last_violation_time': rule.last_violation_time.isoformat() if rule.last_violation_time else None,
            'recent_states': recent,
            'message': f"Rule '{rule_id}' ({rule.description}) violated. Operator: {rule.operator.value}."
        }
        return explanation

    # ------------------ Persistence ------------------
    async def save_state(self, path: str = "temporal_monitor_state.json") -> bool:
        """Save violation counts and last violation times (rules themselves are not saved)."""
        data = {
            'history': [[ts.isoformat(), state] for ts, state in self.history],
            'rules': {
                rid: {
                    'violation_count': rule.violation_count,
                    'last_violation_time': rule.last_violation_time.isoformat() if rule.last_violation_time else None,
                }
                for rid, rule in self.rules.items()
            }
        }
        try:
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
            return False

    async def load_state(self, path: str = "temporal_monitor_state.json") -> bool:
        """Load previously saved state. Note: rules must be re-added before loading."""
        if not os.path.exists(path):
            return False
        try:
            with open(path, 'r') as f:
                data = json.load(f)
            # Restore history
            self.history.clear()
            for ts_str, state in data.get('history', []):
                ts = datetime.fromisoformat(ts_str)
                self.history.append((ts, state))
            # Restore rule counts
            for rid, info in data.get('rules', {}).items():
                if rid in self.rules:
                    self.rules[rid].violation_count = info.get('violation_count', 0)
                    lvt = info.get('last_violation_time')
                    self.rules[rid].last_violation_time = datetime.fromisoformat(lvt) if lvt else None
            return True
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
            return False

    # ------------------ Chaos Testing ------------------
    async def inject_fault(self, fault_type: str, **kwargs):
        """Inject a fault to test monitor resilience."""
        if fault_type == 'clear_history':
            self.history.clear()
            logger.warning("Fault: history cleared")
        elif fault_type == 'corrupt_timestamps':
            # Shift all timestamps into future to break cooldown
            for i, (ts, state) in enumerate(self.history):
                self.history[i] = (ts + timedelta(days=1), state)
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
        else:
            logger.warning(f"Unknown fault type: {fault_type}")

    async def run_chaos_test(self) -> Dict[str, Any]:
        """Simple chaos test: inject faults and ensure basic operations still work."""
        report = {'faults': [], 'results': {}}
        # Test clear history
        await self.inject_fault('clear_history')
        report['faults'].append('clear_history')
        state = {'carbon': 400}
        violated = await self.check(state)
        report['results']['clear_history'] = {
            'violated_count': len(violated),
            'history_len': len(self.history)
        }
        # Test corrupt timestamps
        await self.inject_fault('corrupt_timestamps')
        report['faults'].append('corrupt_timestamps')
        violated = await self.check(state)
        report['results']['corrupt_timestamps'] = {
            'violated_count': len(violated),
            'history_len': len(self.history)
        }
        return report
