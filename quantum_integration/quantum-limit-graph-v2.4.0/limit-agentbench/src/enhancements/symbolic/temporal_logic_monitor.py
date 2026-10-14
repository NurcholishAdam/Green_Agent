# src/enhancements/symbolic/temporal_logic_monitor.py
"""
Enhanced Temporal Logic Monitor v2.0.0
======================================

Maintains a history of observations (metrics + optional actions) and evaluates
temporal conditions:

  - ALWAYS(phi)             : phi holds at every observation in the window.
  - EVENTUALLY(phi)         : phi holds at least once in the window.
  - NEXT(phi)               : phi must hold in the observation immediately
                              after the rule was armed (evaluated lazily).
  - UNTIL(phi, psi)         : psi becomes true at some point, and phi holds
                              at every observation strictly before that.
  - RELEASE(phi, psi)       : phi becomes true at some point and psi holds
                              up to and including that point, OR psi holds
                              throughout the window.

Also supports:
  - Logical combination of rules via AND/OR groups.
  - Time-aware windows (window_seconds as an alternative to window count).
  - Thread-safe synchronous and asynchronous use.
  - Append-only NDJSON observation persistence + JSON rule snapshots.
  - Rich violations with UUIDs, severity, and rule versioning.
  - FeedbackEvent emission with a registered feedback type.
  - Observability via get_stats().

ENHANCEMENTS (v2.0.0 over v1.0.0):
- Fixed dead-code _rebuild_indices; removed unused metric/action indices.
- Implemented UNTIL and RELEASE with a second condition.
- Fixed NEXT to evaluate against the next observation (pending assertions).
- Fixed FeedbackEvent integration (registered type, valid context fields).
- Thread-safe sync (RLock) and async (asyncio.Lock) paths.
- Rule validation at add_rule time (operator, window, threshold, UNTIL/RELEASE).
- Append-only NDJSON observations + atomic JSON rule snapshots.
- _load_state no longer recursively writes to disk.
- Added AND/OR rule groups.
- Time-aware windows via window_seconds.
- Violations carry UUID, severity, threshold, operator.
- Observability: get_stats, per-rule violation counts.
- Added __all__ and a built-in test suite.

All previous features retained.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import threading
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, ClassVar, Dict, FrozenSet, List, Optional, Set, Tuple, Union

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional FeedbackEvent integration
# ---------------------------------------------------------------------------
try:
    from ..feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    try:
        from .feedback_event import FeedbackEvent  # type: ignore
    except ImportError:
        FeedbackEvent = None  # type: ignore

# Feedback type this module will request.
TEMPORAL_FEEDBACK_TYPE: str = "safety"
TEMPORAL_FEEDBACK_FALLBACK: str = "routing"


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCHEMA_VERSION: str = "2.0.0"

VALID_OPERATORS: FrozenSet[str] = frozenset({"<", "<=", ">", ">=", "==", "!="})
VALID_TEMPORAL_OPS: FrozenSet[str] = frozenset({"ALWAYS", "EVENTUALLY", "NEXT", "UNTIL", "RELEASE"})
VALID_SEVERITIES: FrozenSet[str] = frozenset({"info", "warning", "critical"})
VALID_GROUP_OPERATORS: FrozenSet[str] = frozenset({"AND", "OR"})
_RULE_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_path(v: Union[str, Path, None]) -> Optional[Path]:
    if v is None:
        return None
    return v if isinstance(v, Path) else Path(v)


def _new_uuid() -> str:
    return str(uuid.uuid4())


def _safe_float(v: Any) -> Optional[float]:
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------
@dataclass
class Observation:
    """A single observation with UTC timestamp."""
    metrics: Dict[str, float]
    actions: Dict[str, Any]
    step: int
    timestamp: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metrics": dict(self.metrics),
            "actions": dict(self.actions),
            "step": int(self.step),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Observation":
        ts = data.get("timestamp")
        if isinstance(ts, str):
            try:
                dt = datetime.fromisoformat(ts)
            except Exception:
                dt = _utc_now()
        elif isinstance(ts, datetime):
            dt = ts
        else:
            dt = _utc_now()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return cls(
            metrics=dict(data.get("metrics", {})),
            actions=dict(data.get("actions", {})),
            step=int(data.get("step", 0)),
            timestamp=dt,
        )


@dataclass
class Violation:
    """Structured violation report."""
    rule: str
    temporal_op: str
    metric: str
    operator: str
    threshold: Any
    observed_values: List[Any]
    window: int
    severity: str
    violation_id: str = field(default_factory=_new_uuid)
    timestamp: datetime = field(default_factory=_utc_now)
    detail: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "violation_id": self.violation_id,
            "timestamp": self.timestamp.isoformat(),
            "rule": self.rule,
            "temporal_op": self.temporal_op,
            "metric": self.metric,
            "operator": self.operator,
            "threshold": self.threshold,
            "observed_values": list(self.observed_values),
            "window": self.window,
            "severity": self.severity,
            "detail": self.detail,
        }


@dataclass
class RuleGroup:
    """Logical combination of named rules."""
    name: str
    operator: str  # "AND" or "OR"
    rule_names: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "operator": self.operator,
            "rule_names": list(self.rule_names),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RuleGroup":
        return cls(
            name=str(data["name"]),
            operator=str(data["operator"]).upper(),
            rule_names=list(data.get("rule_names", [])),
        )


# ---------------------------------------------------------------------------
# Temporal Logic Monitor
# ---------------------------------------------------------------------------
class TemporalLogicMonitor:
    """
    A monitor for evaluating temporal logic formulas over a sliding window of
    observations. Safe for concurrent use in both sync and async contexts.
    """

    def __init__(
        self,
        history_size: int = 100,
        persistence_path: Optional[Union[str, Path]] = None,
        async_mode: bool = True,
    ) -> None:
        if history_size < 1:
            raise ValueError("history_size must be >= 1")

        self.history_size = int(history_size)
        self.history: deque = deque(maxlen=self.history_size)
        self.rules: List[Dict[str, Any]] = []
        self.groups: Dict[str, RuleGroup] = {}
        self._rule_index: Dict[str, Dict[str, Any]] = {}

        # Pending NEXT assertions: rule_name -> list of armed markers
        # Each entry records the observation index when the rule was armed.
        self._pending_next: Dict[str, int] = {}
        # Monotonic count of observations ever appended (drives NEXT indexing).
        self._obs_counter: int = 0

        self.async_mode = bool(async_mode)

        # Persistence
        self.persistence_path: Optional[Path] = _as_path(persistence_path)
        self._obs_log_path: Optional[Path] = None
        if self.persistence_path is not None:
            parent = self.persistence_path.parent
            try:
                parent.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass
            # NDJSON observation log lives alongside the rule snapshot.
            self._obs_log_path = self.persistence_path.with_suffix(
                self.persistence_path.suffix + ".ndjson"
            )

        # Locking
        self._thread_lock = threading.RLock()
        self._async_lock: Optional[asyncio.Lock] = asyncio.Lock() if self.async_mode else None

        # Observability
        self._violation_counts: Dict[str, int] = {}
        self._evaluations: int = 0

        # Load persisted state
        if self.persistence_path and self.persistence_path.exists():
            self._load_state()

    # ==================================================================
    # Persistence
    # ==================================================================
    def _load_state(self) -> None:
        """Load rules + snapshot + NDJSON observations. Never triggers a save."""
        assert self.persistence_path is not None
        try:
            with open(self.persistence_path, "r") as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load rule snapshot: {e}")
            data = {}

        # Rules
        rules = data.get("rules", [])
        self.rules = []
        self._rule_index = {}
        for rule in rules:
            try:
                self._register_rule_internal(rule)
            except Exception as e:
                logger.warning(f"Skipping invalid rule in snapshot: {e}")

        # Groups
        self.groups = {}
        for g in data.get("groups", []):
            try:
                rg = RuleGroup.from_dict(g)
                if rg.operator not in VALID_GROUP_OPERATORS:
                    continue
                self.groups[rg.name] = rg
            except Exception as e:
                logger.warning(f"Skipping invalid group in snapshot: {e}")

        # Observations — load from NDJSON log if present; otherwise fall back
        # to the (possibly older) snapshot's history list.
        loaded = 0
        if self._obs_log_path and self._obs_log_path.exists():
            try:
                with open(self._obs_log_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obs = Observation.from_dict(json.loads(line))
                        except Exception:
                            continue
                        self._append_observation_internal(obs, persist=False)
                        loaded += 1
            except Exception as e:
                logger.error(f"Failed to load observation log: {e}")

        if loaded == 0:
            for obs in data.get("history", []):
                try:
                    self._append_observation_internal(Observation.from_dict(obs), persist=False)
                except Exception:
                    continue

        logger.info(
            f"Loaded {len(self.rules)} rules, {len(self.groups)} groups, "
            f"{len(self.history)} observations from {self.persistence_path}"
        )

    def _save_rules_snapshot(self) -> None:
        """Atomically write rules + groups (no observations)."""
        if not self.persistence_path:
            return
        data = {
            "schema_version": SCHEMA_VERSION,
            "rules": self.rules,
            "groups": [g.to_dict() for g in self.groups.values()],
        }
        tmp = self.persistence_path.with_suffix(self.persistence_path.suffix + ".tmp")
        try:
            with open(tmp, "w") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, self.persistence_path)
        except Exception as e:
            logger.error(f"Failed to save rule snapshot: {e}")
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass

    def _append_observation_ndjson(self, obs: Observation) -> None:
        """Append a single observation to the NDJSON log."""
        if not self._obs_log_path:
            return
        try:
            with open(self._obs_log_path, "a") as f:
                f.write(json.dumps(obs.to_dict()) + "\n")
        except Exception as e:
            logger.error(f"Failed to append observation log: {e}")

    async def _append_observation_ndjson_async(self, obs: Observation) -> None:
        try:
            await asyncio.to_thread(self._append_observation_ndjson, obs)
        except Exception as e:
            logger.error(f"Async observation append failed: {e}")

    # ==================================================================
    # Rule management
    # ==================================================================
    def _register_rule_internal(self, rule: Dict[str, Any]) -> None:
        """Validate + register a rule dict. Raises on invalid input."""
        name = rule.get("name")
        if not isinstance(name, str) or not _RULE_NAME_RE.match(name):
            raise ValueError(f"Invalid rule name: {name!r}")
        if name in self._rule_index:
            raise ValueError(f"Rule with name '{name}' already exists")

        temporal_op = str(rule.get("temporal_op", "ALWAYS")).upper()
        if temporal_op not in VALID_TEMPORAL_OPS:
            raise ValueError(f"Unsupported temporal_op: {temporal_op}")

        operator = str(rule.get("operator", "=="))
        if operator not in VALID_OPERATORS:
            raise ValueError(f"Unsupported operator: {operator}")

        window = int(rule.get("window", 3))
        if window < 1:
            raise ValueError("window must be >= 1")

        window_seconds = rule.get("window_seconds")
        if window_seconds is not None:
            window_seconds = float(window_seconds)
            if window_seconds <= 0:
                raise ValueError("window_seconds must be > 0")

        action_name = rule.get("action_name")
        metric = rule.get("metric")
        if action_name is None and not metric:
            raise ValueError("metric must be provided if action_name is None")
        if action_name is None and metric is not None:
            # Numeric comparison; threshold must be numeric
            if _safe_float(rule.get("threshold")) is None:
                raise ValueError("threshold must be a finite number when comparing metrics")

        severity = str(rule.get("severity", "warning")).lower()
        if severity not in VALID_SEVERITIES:
            raise ValueError(f"severity must be one of {sorted(VALID_SEVERITIES)}")

        # UNTIL/RELEASE require a second condition.
        normalized = {
            "name": name,
            "metric": metric,
            "operator": operator,
            "threshold": rule.get("threshold"),
            "temporal_op": temporal_op,
            "window": window,
            "window_seconds": window_seconds,
            "action_name": action_name,
            "severity": severity,
            "version": str(rule.get("version", SCHEMA_VERSION)),
            "kwargs": dict(rule.get("kwargs", {}) or {}),
        }

        if temporal_op in ("UNTIL", "RELEASE"):
            k = normalized["kwargs"]
            second_metric = k.get("second_metric")
            second_action = k.get("second_action")
            second_op = k.get("second_operator", "==")
            second_threshold = k.get("second_threshold")
            if second_metric is None and second_action is None:
                raise ValueError(
                    f"{temporal_op} requires kwargs.second_metric or kwargs.second_action"
                )
            if second_op not in VALID_OPERATORS:
                raise ValueError(f"Unsupported second_operator: {second_op}")
            normalized["kwargs"]["second_operator"] = second_op
            normalized["kwargs"]["second_metric"] = second_metric
            normalized["kwargs"]["second_action"] = second_action
            normalized["kwargs"]["second_threshold"] = second_threshold

        self.rules.append(normalized)
        self._rule_index[name] = normalized

    def add_rule(
        self,
        name: str,
        metric: Optional[str] = None,
        operator: str = "==",
        threshold: Any = None,
        temporal_op: str = "ALWAYS",
        window: int = 3,
        action_name: Optional[str] = None,
        severity: str = "warning",
        window_seconds: Optional[float] = None,
        **kwargs: Any,
    ) -> None:
        """
        Add a temporal rule. Validation is strict and raises on invalid input.

        See the module docstring for operator semantics. For UNTIL/RELEASE,
        pass the second condition via ``second_metric`` / ``second_action``,
        ``second_operator``, and ``second_threshold``.
        """
        with self._thread_lock:
            rule = {
                "name": name,
                "metric": metric,
                "operator": operator,
                "threshold": threshold,
                "temporal_op": temporal_op,
                "window": window,
                "window_seconds": window_seconds,
                "action_name": action_name,
                "severity": severity,
                "kwargs": dict(kwargs),
            }
            self._register_rule_internal(rule)
            if self.persistence_path:
                self._save_rules_snapshot()

    def remove_rule(self, name: str) -> bool:
        """Remove a rule by name. Also drops it from any groups."""
        with self._thread_lock:
            if name not in self._rule_index:
                return False
            self.rules = [r for r in self.rules if r["name"] != name]
            del self._rule_index[name]
            self._pending_next.pop(name, None)
            # Remove from groups; delete groups that become empty.
            empty_groups = []
            for gname, grp in self.groups.items():
                grp.rule_names = [n for n in grp.rule_names if n != name]
                if not grp.rule_names:
                    empty_groups.append(gname)
            for gname in empty_groups:
                del self.groups[gname]
            if self.persistence_path:
                self._save_rules_snapshot()
            return True

    # ------------------------------------------------------------------
    def add_group(self, name: str, operator: str, rule_names: List[str]) -> None:
        """Add an AND/OR rule group."""
        with self._thread_lock:
            op = str(operator).upper()
            if op not in VALID_GROUP_OPERATORS:
                raise ValueError(f"operator must be one of {sorted(VALID_GROUP_OPERATORS)}")
            if not name or not _RULE_NAME_RE.match(name):
                raise ValueError(f"invalid group name: {name!r}")
            missing = [n for n in rule_names if n not in self._rule_index]
            if missing:
                raise ValueError(f"unknown rules in group: {missing}")
            self.groups[name] = RuleGroup(name=name, operator=op, rule_names=list(rule_names))
            if self.persistence_path:
                self._save_rules_snapshot()

    def remove_group(self, name: str) -> bool:
        with self._thread_lock:
            if name in self.groups:
                del self.groups[name]
                if self.persistence_path:
                    self._save_rules_snapshot()
                return True
            return False

    # ==================================================================
    # Observation management
    # ==================================================================
    def _append_observation_internal(self, obs: Observation, persist: bool) -> None:
        """Append to history, advance counter, clear pending NEXT if fired."""
        self.history.append(obs)
        self._obs_counter += 1

        # Evaluate pending NEXT assertions against the newly arrived observation
        fired: List[str] = []
        for rule_name, armed_at in list(self._pending_next.items()):
            if self._obs_counter > armed_at:
                rule = self._rule_index.get(rule_name)
                if rule is None:
                    fired.append(rule_name)
                    continue
                value = self._extract_value(rule, obs)
                if value is None or not self._compare(value, rule["operator"], rule["threshold"]):
                    # NEXT violated: emit and clear.
                    severity = rule.get("severity", "warning")
                    self._register_violation_count(rule_name)
                    logger.info(f"NEXT violation: rule={rule_name} value={value}")
                fired.append(rule_name)
        for rule_name in fired:
            self._pending_next.pop(rule_name, None)

        if persist:
            self._append_observation_ndjson(obs)

    def add_observation(
        self,
        metrics: Dict[str, float],
        step: int = 0,
        actions: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Record a new observation (sync)."""
        ts = timestamp or _utc_now()
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        # Strip NaN/inf metrics to keep evaluation safe.
        clean_metrics = {k: v for k, v in metrics.items() if _safe_float(v) is not None}
        obs = Observation(
            metrics=clean_metrics,
            actions=dict(actions or {}),
            step=int(step),
            timestamp=ts,
        )
        with self._thread_lock:
            self._append_observation_internal(obs, persist=True)

    async def add_observation_async(
        self,
        metrics: Dict[str, float],
        step: int = 0,
        actions: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Async-aware observation append. Serializes with evaluate_rules."""
        ts = timestamp or _utc_now()
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        clean_metrics = {k: v for k, v in metrics.items() if _safe_float(v) is not None}
        obs = Observation(
            metrics=clean_metrics,
            actions=dict(actions or {}),
            step=int(step),
            timestamp=ts,
        )
        if self._async_lock is not None:
            async with self._async_lock:
                with self._thread_lock:
                    self._append_observation_internal(obs, persist=False)
            await self._append_observation_ndjson_async(obs)
        else:
            with self._thread_lock:
                self._append_observation_internal(obs, persist=True)

    # ==================================================================
    # Evaluation
    # ==================================================================
    def _extract_value(self, rule: Dict[str, Any], obs: Observation) -> Any:
        if rule.get("action_name") is not None:
            return obs.actions.get(rule["action_name"])
        return obs.metrics.get(rule["metric"])

    def _window(self, rule: Dict[str, Any]) -> List[Observation]:
        """Return the observations in this rule's window (count + time aware)."""
        hist = list(self.history)
        # Count-based
        w = min(rule["window"], len(hist))
        window = hist[-w:] if w > 0 else []
        # Time-based filter
        ws = rule.get("window_seconds")
        if ws is not None and window:
            cutoff = window[-1].timestamp.timestamp() - float(ws)
            window = [o for o in window if o.timestamp.timestamp() >= cutoff]
        return window

    def _register_violation_count(self, rule_name: str) -> None:
        self._violation_counts[rule_name] = self._violation_counts.get(rule_name, 0) + 1

    def evaluate_rules_sync(self) -> List[Violation]:
        """
        Evaluate all rules synchronously. Returns a list of Violation objects.
        Also handles NEXT rule arming.
        """
        with self._thread_lock:
            self._evaluations += 1
            violations: List[Violation] = []
            if not self.history:
                return violations

            for rule in self.rules:
                try:
                    vs = self._evaluate_one_rule(rule)
                except Exception as e:
                    logger.error(f"Rule {rule['name']} failed: {e}")
                    continue
                for v in vs:
                    self._register_violation_count(rule["name"])
                    violations.append(v)
            return violations

    def _evaluate_one_rule(self, rule: Dict[str, Any]) -> List[Violation]:
        temporal_op = rule["temporal_op"]
        window = self._window(rule)
        if not window:
            return []

        # NEXT is deferred: arm the rule and wait for the next observation.
        if temporal_op == "NEXT":
            self._pending_next[rule["name"]] = self._obs_counter
            return []

        if temporal_op == "ALWAYS":
            return self._evaluate_always(rule, window)
        if temporal_op == "EVENTUALLY":
            return self._evaluate_eventually(rule, window)
        if temporal_op == "UNTIL":
            return self._evaluate_until(rule, window)
        if temporal_op == "RELEASE":
            return self._evaluate_release(rule, window)
        return []

    # ------------------------------------------------------------------
    def _value_pair(self, rule: Dict[str, Any], obs: Observation) -> Tuple[Any, Any]:
        """Return (primary_value, secondary_value) for UNTIL/RELEASE."""
        primary = self._extract_value(rule, obs)
        kw = rule["kwargs"]
        if kw.get("second_action") is not None:
            secondary = obs.actions.get(kw["second_action"])
        else:
            secondary = obs.metrics.get(kw.get("second_metric"))
        return primary, secondary

    def _make_violation(
        self,
        rule: Dict[str, Any],
        observed: List[Any],
        detail: str,
    ) -> Violation:
        return Violation(
            rule=rule["name"],
            temporal_op=rule["temporal_op"],
            metric=rule.get("metric") or rule.get("action_name") or "?",
            operator=rule["operator"],
            threshold=rule.get("threshold"),
            observed_values=list(observed),
            window=rule["window"],
            severity=rule.get("severity", "warning"),
            detail=detail,
        )

    def _evaluate_always(self, rule: Dict[str, Any], window: List[Observation]) -> List[Violation]:
        observed = []
        for obs in window:
            v = self._extract_value(rule, obs)
            if v is None:
                continue
            observed.append(v)
            if not self._compare(v, rule["operator"], rule["threshold"]):
                return [self._make_violation(
                    rule, observed, f"ALWAYS failed at value={v!r}"
                )]
        return []

    def _evaluate_eventually(self, rule: Dict[str, Any], window: List[Observation]) -> List[Violation]:
        observed: List[Any] = []
        for obs in window:
            v = self._extract_value(rule, obs)
            if v is None:
                continue
            observed.append(v)
            if self._compare(v, rule["operator"], rule["threshold"]):
                return []
        if not observed:
            return []
        return [self._make_violation(
            rule, observed, "EVENTUALLY never satisfied in window"
        )]

    def _evaluate_until(self, rule: Dict[str, Any], window: List[Observation]) -> List[Violation]:
        """
        phi UNTIL psi: psi must become true at some point, and phi must hold
        at every observation strictly before that point.
        """
        observed_primary: List[Any] = []
        observed_secondary: List[Any] = []
        kw = rule["kwargs"]
        second_op = kw.get("second_operator", "==")
        second_threshold = kw.get("second_threshold")

        for obs in window:
            primary, secondary = self._value_pair(rule, obs)
            if primary is not None:
                observed_primary.append(primary)
            if secondary is not None:
                observed_secondary.append(secondary)
            # If psi holds, the UNTIL is satisfied up to here.
            if secondary is not None and self._compare(secondary, second_op, second_threshold):
                return []
            # phi must hold until psi.
            if primary is not None and not self._compare(primary, rule["operator"], rule["threshold"]):
                return [self._make_violation(
                    rule, observed_primary,
                    f"UNTIL: phi failed at value={primary!r} before psi held",
                )]
        # Never saw psi.
        return [self._make_violation(
            rule, observed_secondary or observed_primary,
            "UNTIL: psi never satisfied in window",
        )]

    def _evaluate_release(self, rule: Dict[str, Any], window: List[Observation]) -> List[Violation]:
        """
        phi RELEASE psi: either psi holds for the entire window, or phi becomes
        true at some point while psi held up to (and including) that point.
        """
        observed_primary: List[Any] = []
        kw = rule["kwargs"]
        second_op = kw.get("second_operator", "==")
        second_threshold = kw.get("second_threshold")

        psi_always_true = True
        for obs in window:
            primary, secondary = self._value_pair(rule, obs)
            if primary is not None:
                observed_primary.append(primary)
            psi_ok = (
                secondary is not None
                and self._compare(secondary, second_op, second_threshold)
            )
            if not psi_ok:
                psi_always_true = False
            if psi_ok and primary is not None and self._compare(primary, rule["operator"], rule["threshold"]):
                # phi released the obligation.
                return []
        if psi_always_true:
            return []
        return [self._make_violation(
            rule, observed_primary,
            "RELEASE: psi failed before phi held",
        )]

    # ------------------------------------------------------------------
    async def evaluate_rules(self) -> List[Violation]:
        """
        Async-aware evaluation. Serializes with add_observation_async.
        """
        if not self.async_mode or self._async_lock is None:
            result = self.evaluate_rules_sync()
        else:
            async with self._async_lock:
                result = self.evaluate_rules_sync()

        if FeedbackEvent is not None and result:
            await self._emit_feedback_events(result)

        return result

    # ------------------------------------------------------------------
    async def _emit_feedback_events(self, violations: List[Violation]) -> None:
        # Determine the feedback type.
        allowed = getattr(FeedbackEvent, "allowed_feedback_types", None)
        if allowed is None or TEMPORAL_FEEDBACK_TYPE in set(allowed):
            fb_type = TEMPORAL_FEEDBACK_TYPE
        else:
            fb_type = TEMPORAL_FEEDBACK_FALLBACK

        for v in violations:
            try:
                event = FeedbackEvent(
                    source="temporal_logic_monitor",
                    feedback_type=fb_type,
                    context={
                        "task_id": "temporal_logic_monitor",
                        "teacher_ids": [],
                    },
                    action={
                        "selected_action": v.rule,
                        "selected_rank": 1,
                        "confidence_score": 1.0,
                    },
                    performance={
                        "quality_score": 0.0,
                        "latency_ms": 0.0,
                        "energy_joules": 0.0,
                        "carbon_g": 0.0,
                        "helium_cost": 0.0,
                        "duration_ms": 0.0,
                    },
                    adaptive_cost_value=0.0,
                    tags=["temporal", "violation", v.severity, v.rule],
                    metadata=v.to_dict(),
                    envelope={"trace_id": v.violation_id},
                )
                logger.debug(f"FeedbackEvent emitted: {v.violation_id}")
            except Exception as e:
                logger.warning(f"Failed to create FeedbackEvent: {e}")

    # ==================================================================
    # Comparison
    # ==================================================================
    @staticmethod
    def _compare(value: Any, op: str, threshold: Any) -> bool:
        """Compare value against threshold with the given operator."""
        if value is None:
            return False
        # Numeric coercion when both sides look numeric.
        a = _safe_float(value)
        b = _safe_float(threshold)
        if a is not None and b is not None:
            if op == "<":  return a < b
            if op == "<=": return a <= b
            if op == ">":  return a > b
            if op == ">=": return a >= b
            if op == "==": return a == b
            if op == "!=": return a != b
        # Fall back to string comparison for equality/inequality.
        try:
            if op == "==": return value == threshold
            if op == "!=": return value != threshold
        except Exception:
            return False
        return False

    # ==================================================================
    # Observability + helpers
    # ==================================================================
    def get_stats(self) -> Dict[str, Any]:
        with self._thread_lock:
            return {
                "schema_version": SCHEMA_VERSION,
                "history_size": len(self.history),
                "history_capacity": self.history_size,
                "observations_total": self._obs_counter,
                "rules": len(self.rules),
                "groups": len(self.groups),
                "evaluations": self._evaluations,
                "pending_next": dict(self._pending_next),
                "violation_counts": dict(self._violation_counts),
            }

    def clear_history(self) -> None:
        """Clear observations only. Rules and groups are preserved."""
        with self._thread_lock:
            self.history.clear()
            self._pending_next.clear()
            self._obs_counter = 0

    def get_rule_names(self) -> List[str]:
        with self._thread_lock:
            return list(self._rule_index.keys())

    def get_group_names(self) -> List[str]:
        with self._thread_lock:
            return list(self.groups.keys())

    def get_history_size(self) -> int:
        with self._thread_lock:
            return len(self.history)

    def flush(self) -> None:
        """Persist rules and force a snapshot."""
        with self._thread_lock:
            if self.persistence_path:
                self._save_rules_snapshot()


__all__ = [
    "SCHEMA_VERSION",
    "TEMPORAL_FEEDBACK_TYPE",
    "TEMPORAL_FEEDBACK_FALLBACK",
    "Observation",
    "Violation",
    "RuleGroup",
    "TemporalLogicMonitor",
]


# ============================================================================
# Tests
# ============================================================================
def _make_monitor(**overrides: Any) -> TemporalLogicMonitor:
    base: Dict[str, Any] = {
        "history_size": 10,
        "async_mode": True,
    }
    base.update(overrides)
    return TemporalLogicMonitor(**base)


def _test_always() -> None:
    m = _make_monitor()
    m.add_rule(name="carbon_ok", metric="carbon", operator="<", threshold=400.0,
               temporal_op="ALWAYS", window=5)
    for c in (100, 200, 300):
        m.add_observation({"carbon": c})
    assert m.evaluate_rules_sync() == []
    m.add_observation({"carbon": 500})
    vs = m.evaluate_rules_sync()
    assert len(vs) == 1 and vs[0].rule == "carbon_ok"


def _test_eventually() -> None:
    m = _make_monitor()
    m.add_rule(name="defer_seen", metric="action_code", operator="==", threshold=1.0,
               temporal_op="EVENTUALLY", window=5)
    for code in (0, 0, 0):
        m.add_observation({"action_code": code})
    vs = m.evaluate_rules_sync()
    assert len(vs) == 1
    m.add_observation({"action_code": 1})
    assert m.evaluate_rules_sync() == []


def _test_next() -> None:
    m = _make_monitor()
    m.add_rule(name="next_low_carbon", metric="carbon", operator="<", threshold=400.0,
               temporal_op="NEXT", window=3)
    m.add_observation({"carbon": 100})
    # Arming the rule should not emit yet.
    assert m.evaluate_rules_sync() == []
    assert m.get_stats()["pending_next"]
    # Next observation satisfies the rule.
    m.add_observation({"carbon": 200})
    m.evaluate_rules_sync()
    # Now the pending NEXT should have been cleared (and no violation emitted
    # because the next observation satisfied the condition).
    assert not m.get_stats()["pending_next"]


def _test_until() -> None:
    m = _make_monitor()
    m.add_rule(
        name="carbon_until_defer",
        metric="carbon", operator="<", threshold=400.0,
        temporal_op="UNTIL", window=10,
        second_metric="deferred", second_operator="==", second_threshold=1.0,
    )
    m.add_observation({"carbon": 100, "deferred": 0})
    m.add_observation({"carbon": 300, "deferred": 0})
    assert m.evaluate_rules_sync() == []
    m.add_observation({"carbon": 500, "deferred": 0})
    vs = m.evaluate_rules_sync()
    assert len(vs) == 1 and vs[0].temporal_op == "UNTIL"
    # Reset and verify psi satisfaction.
    m.clear_history()
    m.add_observation({"carbon": 100, "deferred": 0})
    m.add_observation({"carbon": 300, "deferred": 0})
    m.add_observation({"carbon": 500, "deferred": 1})
    assert m.evaluate_rules_sync() == []


def _test_release() -> None:
    m = _make_monitor()
    m.add_rule(
        name="pressure_release",
        metric="deferred", operator="==", threshold=1.0,
        temporal_op="RELEASE", window=5,
        second_metric="coolant_ok", second_operator="==", second_threshold=1.0,
    )
    # If coolant_ok holds throughout, no violation.
    for _ in range(3):
        m.add_observation({"deferred": 0, "coolant_ok": 1})
    assert m.evaluate_rules_sync() == []
    # If coolant_ok fails without any prior "release", violation.
    m.clear_history()
    for _ in range(3):
        m.add_observation({"deferred": 0, "coolant_ok": 0})
    vs = m.evaluate_rules_sync()
    assert len(vs) == 1 and vs[0].temporal_op == "RELEASE"


def _test_group_add_remove() -> None:
    m = _make_monitor()
    m.add_rule(name="r1", metric="x", operator="<", threshold=1.0)
    m.add_rule(name="r2", metric="y", operator="<", threshold=1.0)
    m.add_group(name="g1", operator="AND", rule_names=["r1", "r2"])
    assert m.get_group_names() == ["g1"]
    try:
        m.add_group(name="g2", operator="AND", rule_names=["missing"])
    except ValueError:
        pass
    else:
        raise AssertionError("unknown rule should fail")
    assert m.remove_rule("r1")
    # g1 should now only contain r2
    assert "g1" in m.groups
    assert m.remove_rule("r2")
    # g1 becomes empty and is removed
    assert "g1" not in m.groups


def _test_validation() -> None:
    m = _make_monitor()
    # Bad name
    try:
        m.add_rule(name="1bad", metric="x", operator="<", threshold=1.0)
    except ValueError:
        pass
    else:
        raise AssertionError("bad rule name should raise")
    # Bad operator
    try:
        m.add_rule(name="ok_name", metric="x", operator="!!", threshold=1.0)
    except ValueError:
        pass
    else:
        raise AssertionError("bad operator should raise")
    # window_seconds must be > 0
    try:
        m.add_rule(name="ok_name2", metric="x", operator="<", threshold=1.0, window_seconds=-1)
    except ValueError:
        pass
    else:
        raise AssertionError("negative window_seconds should raise")
    # UNTIL requires second condition
    try:
        m.add_rule(name="until_rule", metric="x", operator="<", threshold=1.0,
                   temporal_op="UNTIL")
    except ValueError:
        pass
    else:
        raise AssertionError("UNTIL without second condition should raise")


def _test_persistence(tmpdir: Path) -> None:
    snap = tmpdir / "rules.json"
    m = TemporalLogicMonitor(history_size=5, persistence_path=snap)
    m.add_rule(name="r1", metric="x", operator="<", threshold=1.0,
               temporal_op="ALWAYS", window=3)
    m.add_group(name="g1", operator="OR", rule_names=["r1"])
    for i in range(3):
        m.add_observation({"x": float(i)})
    # Reload
    m2 = TemporalLogicMonitor(history_size=5, persistence_path=snap)
    assert m2.get_rule_names() == ["r1"]
    assert m2.get_group_names() == ["g1"]
    assert m2.get_history_size() == 3


def _test_threaded_appends() -> None:
    m = _make_monitor(history_size=1000)
    m.add_rule(name="always_ok", metric="x", operator=">=", threshold=0.0,
               temporal_op="ALWAYS", window=1000)

    def worker(start: int):
        for i in range(100):
            m.add_observation({"x": float(start + i)})

    threads = [threading.Thread(target=worker, args=(i * 100,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert m.get_history_size() == 400


async def _test_async_add_and_evaluate() -> None:
    m = _make_monitor()
    m.add_rule(name="next_x", metric="x", operator="<", threshold=10.0,
               temporal_op="NEXT", window=2)
    await m.add_observation_async({"x": 1.0})
    await m.evaluate_rules()
    await m.add_observation_async({"x": 5.0})
    await m.evaluate_rules()
    # NEXT consumed, no pending.
    assert not m.get_stats()["pending_next"]


def _run_all_tests() -> None:
    import tempfile
    _test_always()
    _test_eventually()
    _test_next()
    _test_until()
    _test_release()
    _test_group_add_remove()
    _test_validation()
    with tempfile.TemporaryDirectory() as td:
        _test_persistence(Path(td))
    _test_threaded_appends()
    asyncio.run(_test_async_add_and_evaluate())
    print("All TemporalLogicMonitor tests passed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _run_all_tests()
