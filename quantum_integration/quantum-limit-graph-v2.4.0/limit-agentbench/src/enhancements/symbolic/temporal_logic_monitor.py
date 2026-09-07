# src/enhancements/symbolic/temporal_logic_monitor.py
"""
Temporal Logic Monitor.

Extends the symbolic rules with a lightweight temporal layer.
Maintains a history of metrics and evaluates temporal conditions like:
  - ALWAYS(carbon < 400)   -> violation if carbon >= 400 at any point in window
  - EVENTUALLY(defer)      -> satisfied if action 'defer' appears in window

Usage:
    monitor = TemporalLogicMonitor(history_size=10)
    monitor.add_rule("CarbonSafe", "carbon", "<", 400, temporal_op="ALWAYS", window=3)
    monitor.add_observation({"carbon": 420}, step=1)
    violations = monitor.evaluate_rules()
"""

from collections import deque
from typing import Dict, List, Any

class TemporalLogicMonitor:
    def __init__(self, history_size: int = 10):
        self.history = deque(maxlen=history_size)
        self.rules = []   # list of dicts with 'condition', 'operator', 'threshold', 'window'

    def add_rule(self, name: str, metric: str, operator: str, threshold: float,
                 temporal_op: str = "ALWAYS", window: int = 3):
        """
        Args:
            name: rule name
            metric: metric name (e.g., 'carbon')
            operator: comparison operator ('<', '>', '<=', '>=')
            threshold: numeric threshold
            temporal_op: 'ALWAYS' or 'EVENTUALLY'
            window: number of recent steps to consider
        """
        self.rules.append({
            "name": name,
            "metric": metric,
            "operator": operator,
            "threshold": threshold,
            "temporal_op": temporal_op,
            "window": window
        })

    def add_observation(self, metrics: Dict[str, float], step: int):
        self.history.append({"metrics": metrics, "step": step})

    def evaluate_rules(self) -> List[Dict[str, Any]]:
        violations = []
        if not self.history:
            return violations
        # Determine the largest window among rules
        max_window = max((r['window'] for r in self.rules), default=1)
        recent = list(self.history)[-min(len(self.history), max_window):]

        for rule in self.rules:
            window_obs = recent[-rule['window']:]
            if not window_obs:
                continue
            values = []
            for obs in window_obs:
                if rule['metric'] in obs['metrics']:
                    values.append(obs['metrics'][rule['metric']])

            if not values:
                continue

            # Evaluate condition for each value
            check = lambda v: self._compare(v, rule['operator'], rule['threshold'])

            if rule['temporal_op'] == "ALWAYS":
                # Violation if any value fails the condition
                if not all(check(v) for v in values):
                    violations.append({
                        "rule": rule['name'],
                        "temporal_op": rule['temporal_op'],
                        "metric": rule['metric'],
                        "observed_values": values,
                        "window": rule['window']
                    })
            elif rule['temporal_op'] == "EVENTUALLY":
                # Violation if no value satisfies the condition (i.e., never true)
                if not any(check(v) for v in values):
                    violations.append({
                        "rule": rule['name'],
                        "temporal_op": rule['temporal_op'],
                        "metric": rule['metric'],
                        "observed_values": values,
                        "window": rule['window']
                    })
        return violations

    @staticmethod
    def _compare(value: float, op: str, threshold: float) -> bool:
        if op == '<': return value < threshold
        elif op == '<=': return value <= threshold
        elif op == '>': return value > threshold
        elif op == '>=': return value >= threshold
        elif op == '==': return value == threshold
        elif op == '!=': return value != threshold
        else: raise ValueError(f"Unsupported operator {op}")
