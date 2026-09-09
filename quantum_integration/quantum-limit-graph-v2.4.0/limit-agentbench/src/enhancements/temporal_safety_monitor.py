#!/usr/bin/env python3
"""
Temporal Logic / Formal Verification for Safety‑Critical Policies.

Simple LTL‑like monitor that checks invariants over time windows.
Rules: (condition, max_violations, cooldown)
"""

import asyncio
import logging
from typing import Dict, Any, List, Callable, Deque
from collections import defaultdict, deque
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class TemporalSafetyMonitor:
    def __init__(self):
        self.rules: List[Dict[str, Any]] = []
        self.violation_counts: Dict[str, int] = defaultdict(int)
        self.last_violation_time: Dict[str, datetime] = {}
        self.violation_history: Deque[Dict] = deque(maxlen=1000)

    def add_rule(self, rule_id: str, condition: Callable[[Dict], bool],
                 max_violations: int = 1, cooldown_seconds: float = 0.0):
        self.rules.append({
            'id': rule_id,
            'condition': condition,
            'max_violations': max_violations,
            'cooldown': cooldown_seconds,
        })

    async def check(self, state: Dict) -> List[str]:
        violated = []
        now = datetime.now(timezone.utc)
        for rule in self.rules:
            if rule['condition'](state):
                # Cooldown check
                last = self.last_violation_time.get(rule['id'])
                if last and rule['cooldown'] > 0 and (now - last).total_seconds() < rule['cooldown']:
                    continue  # within cooldown, ignore
                self.violation_counts[rule['id']] += 1
                self.last_violation_time[rule['id']] = now
                self.violation_history.append({
                    'timestamp': now.isoformat(),
                    'rule_id': rule['id'],
                    'state': state,
                })
                if self.violation_counts[rule['id']] >= rule['max_violations']:
                    violated.append(rule['id'])
            else:
                self.violation_counts[rule['id']] = 0
        return violated

    def reset(self):
        self.violation_counts.clear()
        self.last_violation_time.clear()
        self.violation_history.clear()
