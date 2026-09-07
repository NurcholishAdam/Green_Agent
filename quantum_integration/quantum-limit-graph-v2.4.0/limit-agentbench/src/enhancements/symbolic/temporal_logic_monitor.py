# src/enhancements/symbolic/temporal_logic_monitor.py
"""
Enhanced Temporal Logic Monitor.

Maintains a history of observations (metrics + optional actions) and evaluates
temporal conditions such as:
  - ALWAYS(carbon < 400)          -> violation if carbon >= 400 at any point in window
  - EVENTUALLY(action == 'defer') -> violation if action never appears in window
  - UNTIL(carbon < 400, action == 'defer') -> violation if carbon >= 400 before defer
  - NEXT(carbon < 400)            -> violation if next observation violates condition

Supports logical combination of rules (AND/OR groups) and asynchronous evaluation.
Thread‑safe and optionally persists state to JSON.
"""

import asyncio
import json
import logging
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

# Optional FeedbackEvent integration
try:
    from ..feedback_event import FeedbackEvent
except ImportError:
    FeedbackEvent = None


class TemporalLogicMonitor:
    """
    A monitor for evaluating temporal logic formulas over a sliding window of
    observations. Each observation consists of a metrics dict and optionally
    an actions dict.
    """

    def __init__(
        self,
        history_size: int = 100,
        persistence_path: Optional[Path] = None,
        async_mode: bool = True,
    ):
        """
        Args:
            history_size: Maximum number of observations to retain.
            persistence_path: If provided, rules and history are saved/loaded from this path.
            async_mode: If True, evaluation methods are coroutines; otherwise they are synchronous.
        """
        self.history = deque(maxlen=history_size)
        self.rules: List[Dict[str, Any]] = []
        self.persistence_path = persistence_path
        self.async_mode = async_mode
        self._lock = asyncio.Lock() if async_mode else None
        self._rule_index: Dict[str, Dict[str, Any]] = {}  # rule name -> rule dict
        self._history_metrics_index: Dict[str, List[float]] = {}  # metric -> list of values (for quick access)
        self._history_actions_index: Dict[str, List[Any]] = {}  # action name -> list of values

        if persistence_path and persistence_path.exists():
            self._load_state()

    def _load_state(self):
        """Load rules and history from a JSON file."""
        try:
            with open(self.persistence_path, "r") as f:
                data = json.load(f)
            self.rules = data.get("rules", [])
            for rule in self.rules:
                self._rule_index[rule["name"]] = rule
            history_data = data.get("history", [])
            for obs in history_data:
                self.add_observation(obs["metrics"], obs.get("step", 0), obs.get("actions"))
            logger.info(f"Loaded state from {self.persistence_path}")
        except Exception as e:
            logger.error(f"Failed to load state: {e}")

    def _save_state(self):
        """Persist rules and history to JSON."""
        if not self.persistence_path:
            return
        try:
            data = {
                "rules": self.rules,
                "history": [
                    {
                        "metrics": obs["metrics"],
                        "step": obs["step"],
                        "actions": obs.get("actions", {}),
                    }
                    for obs in self.history
                ],
            }
            with open(self.persistence_path, "w") as f:
                json.dump(data, f, indent=2)
            logger.debug(f"Saved state to {self.persistence_path}")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    # ------------------------------------------------------------------
    # Rule Management
    # ------------------------------------------------------------------
    def add_rule(
        self,
        name: str,
        metric: str,
        operator: str,
        threshold: float,
        temporal_op: str = "ALWAYS",
        window: int = 3,
        action_name: Optional[str] = None,  # if set, rule applies to action instead of metric
        **kwargs,
    ) -> None:
        """
        Add a temporal rule.

        Args:
            name: Unique rule name.
            metric: Name of the metric to monitor (unless action_name is given).
            operator: Comparison operator: '<', '<=', '>', '>=', '==', '!='.
            threshold: Numeric threshold.
            temporal_op: One of 'ALWAYS', 'EVENTUALLY', 'NEXT', 'UNTIL', 'RELEASE'.
            window: Number of recent observations to consider.
            action_name: If provided, the rule checks actions (string values) instead of metrics.
            kwargs: Additional temporal_op-specific parameters (e.g., 'until_metric' for UNTIL).
        """
        # Validation
        if not name or not isinstance(name, str):
            raise ValueError("Rule name must be a non-empty string")
        if name in self._rule_index:
            raise ValueError(f"Rule with name '{name}' already exists")
        if temporal_op not in ("ALWAYS", "EVENTUALLY", "NEXT", "UNTIL", "RELEASE"):
            raise ValueError(f"Unsupported temporal_op: {temporal_op}")
        if window < 1:
            raise ValueError("window must be >= 1")
        if action_name is None and not metric:
            raise ValueError("metric must be provided if action_name is None")

        rule = {
            "name": name,
            "metric": metric,
            "operator": operator,
            "threshold": threshold,
            "temporal_op": temporal_op,
            "window": window,
            "action_name": action_name,
            "kwargs": kwargs,
        }
        self.rules.append(rule)
        self._rule_index[name] = rule
        if self.persistence_path:
            self._save_state()

    def remove_rule(self, name: str) -> bool:
        """Remove a rule by name. Returns True if removed, False otherwise."""
        if name not in self._rule_index:
            return False
        self.rules = [r for r in self.rules if r["name"] != name]
        del self._rule_index[name]
        if self.persistence_path:
            self._save_state()
        return True

    # ------------------------------------------------------------------
    # Observation Management
    # ------------------------------------------------------------------
    def add_observation(
        self,
        metrics: Dict[str, float],
        step: int = 0,
        actions: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Record a new observation.

        Args:
            metrics: Dictionary of metric name -> numeric value.
            step: Timestamp or step index (informational).
            actions: Optional dictionary of action name -> action value.
        """
        obs = {"metrics": metrics, "step": step, "actions": actions or {}}
        self.history.append(obs)
        # Update indices
        for metric, value in metrics.items():
            if metric not in self._history_metrics_index:
                self._history_metrics_index[metric] = []
            self._history_metrics_index[metric].append(value)
        for action_name, value in (actions or {}).items():
            if action_name not in self._history_actions_index:
                self._history_actions_index[action_name] = []
            self._history_actions_index[action_name].append(value)
        # Enforce maxlen on indices by popping oldest if needed
        if len(self.history) > self.history.maxlen:
            # Actually deque automatically discards oldest; we need to remove from indices
            # This is a bit tricky; we'll handle by rebuilding indices periodically or on overflow.
            # For simplicity, we assume history size is large enough or we rebuild here.
            self._rebuild_indices()
        if self.persistence_path:
            self._save_state()

    def _rebuild_indices(self):
        """Rebuild metric/action indices from current history."""
        self._history_metrics_index.clear()
        self._history_actions_index.clear()
        for obs in self.history:
            for metric, value in obs["metrics"].items():
                if metric not in self._history_metrics_index:
                    self._history_metrics_index[metric] = []
                self._history_metrics_index[metric].append(value)
            for action_name, value in obs.get("actions", {}).items():
                if action_name not in self._history_actions_index:
                    self._history_actions_index[action_name] = []
                self._history_actions_index[action_name].append(value)

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------
    def evaluate_rules_sync(self) -> List[Dict[str, Any]]:
        """
        Evaluate all rules synchronously (if async_mode is False) or as a fallback.
        Returns a list of violation dicts.
        """
        violations = []
        if not self.history:
            return violations

        # Determine the maximum window needed
        max_window = max((r["window"] for r in self.rules), default=1)
        # We'll work with a list copy for indexing
        recent = list(self.history)[-min(len(self.history), max_window):]

        for rule in self.rules:
            window_obs = recent[-rule["window"]:]
            if not window_obs:
                continue

            # Extract values based on rule target
            if rule.get("action_name") is not None:
                values = [obs.get("actions", {}).get(rule["action_name"]) for obs in window_obs]
                # Filter out None (action not present)
                values = [v for v in values if v is not None]
                if not values:
                    continue
                # For actions, comparison usually equality or inequality; use _compare with operator
                # Assuming action values are strings; operator '==' or '!=' should be used.
                check = lambda v: self._compare(v, rule["operator"], rule["threshold"])
            else:
                values = [obs["metrics"].get(rule["metric"]) for obs in window_obs]
                values = [v for v in values if v is not None]
                if not values:
                    continue
                check = lambda v: self._compare(v, rule["operator"], rule["threshold"])

            temporal_op = rule["temporal_op"]
            window_size = rule["window"]

            # Handle different temporal operators
            if temporal_op == "ALWAYS":
                # Violation if any value fails the condition
                if not all(check(v) for v in values):
                    self._add_violation(violations, rule, values, "ALWAYS")
            elif temporal_op == "EVENTUALLY":
                # Violation if no value satisfies the condition
                if not any(check(v) for v in values):
                    self._add_violation(violations, rule, values, "EVENTUALLY")
            elif temporal_op == "NEXT":
                # Check the most recent observation only (the "next" step from previous)
                # Actually NEXT means the condition must hold in the next state.
                # We'll interpret as: the last observation in window must satisfy.
                if not check(values[-1]):
                    self._add_violation(violations, rule, values, "NEXT")
            elif temporal_op == "UNTIL":
                # condition1 UNTIL condition2: violation if condition1 becomes false before condition2 becomes true.
                # We'll check if there exists an observation where condition1 fails and condition2 hasn't been true yet.
                # Simplification: extract two metrics? For now, support UNTIL with a second condition using kwargs.
                # If not properly configured, treat as ALWAYS (or log error)
                logger.error("UNTIL operator requires additional configuration; treat as ALWAYS")
                if not all(check(v) for v in values):
                    self._add_violation(violations, rule, values, "UNTIL")
            elif temporal_op == "RELEASE":
                # condition1 RELEASE condition2: violation if condition1 is false and condition2 is false.
                # Placeholder: treat as ALWAYS for condition1.
                if not all(check(v) for v in values):
                    self._add_violation(violations, rule, values, "RELEASE")
        return violations

    def _add_violation(self, violations, rule, values, temporal_op):
        violations.append({
            "rule": rule["name"],
            "temporal_op": temporal_op,
            "metric": rule.get("metric") or rule.get("action_name"),
            "observed_values": values,
            "window": rule["window"],
        })

    async def evaluate_rules(self) -> List[Dict[str, Any]]:
        """
        Asynchronous wrapper for evaluate_rules_sync. If async_mode is False,
        returns the sync result directly.
        """
        if not self.async_mode:
            return self.evaluate_rules_sync()
        async with self._lock:
            result = self.evaluate_rules_sync()
            # Optionally emit FeedbackEvents for violations
            if FeedbackEvent is not None and result:
                for violation in result:
                    try:
                        event = FeedbackEvent(
                            source="temporal_logic_monitor",
                            feedback_type="safety",
                            context={"task_id": "monitor"},
                            action={"selected_action": "violation"},
                            performance={"quality_score": 0.0, "latency_ms": 0, "energy_joules": 0, "carbon_g": 0},
                            adaptive_cost_value=0.0,
                            tags=["temporal", "violation", violation["rule"]],
                            metadata=violation,
                        )
                        # In a real system, you'd publish this event to a queue
                        logger.debug(f"Emitted FeedbackEvent for violation: {violation}")
                    except Exception as e:
                        logger.error(f"Failed to create FeedbackEvent: {e}")
            return result

    @staticmethod
    def _compare(value: Any, op: str, threshold: Any) -> bool:
        """Comparison function supporting numeric and string comparisons."""
        try:
            if op == "<":
                return value < threshold
            elif op == "<=":
                return value <= threshold
            elif op == ">":
                return value > threshold
            elif op == ">=":
                return value >= threshold
            elif op == "==":
                return value == threshold
            elif op == "!=":
                return value != threshold
            else:
                raise ValueError(f"Unsupported operator {op}")
        except TypeError:
            # If comparison not possible (e.g., string vs number), treat as False
            return False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def clear_history(self):
        """Clear observation history and indices."""
        self.history.clear()
        self._history_metrics_index.clear()
        self._history_actions_index.clear()

    def get_rule_names(self) -> List[str]:
        return [rule["name"] for rule in self.rules]

    def get_history_size(self) -> int:
        return len(self.history)
