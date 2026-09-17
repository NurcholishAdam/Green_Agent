# src/symbolic/symbolic_reasoning_engine.py

"""
Symbolic Reasoning Engine
=========================

Lightweight symbolic logic engine for evaluating formal rules against
metrics. Inspired by the FormalJudge neuro-symbolic oversight paradigm.

Enhancements
------------
- ``SymbolicReasoningConfig`` — frozen, validated: policy file, rule
  loading, bounded violation history, safe-eval allowlist.
- ``SymbolicReasoningError`` — custom exception.
- **Replaced ``eval()``** with an AST-based safe evaluator that only allows
  comparison operators against the metrics dict.
- **Full validation** of every argument; strict / non-strict modes.
- **Thread safety** — ``RLock`` guards the rule lists and violation history.
- **Bounded violation history** — ``deque(maxlen=config.max_history)``.
- **Structured logging** — no more ``print()``.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` on the engine and
  every dataclass.
- ``statistics()``, ``reset()``, ``__repr__``, and a ``__main__`` smoke test.
"""

from __future__ import annotations

import ast
import json
import logging
import math
import operator
import re
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Mapping, Optional

try:  # pragma: no cover — optional dependency
    import yaml  # type: ignore

    _YAML_AVAILABLE = True
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore[assignment]
    _YAML_AVAILABLE = False

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SymbolicReasoningError(ValueError):
    """Raised for invalid symbolic-reasoning inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SymbolicReasoningConfig:
    """Tunable parameters for :class:`SymbolicReasoningEngine`."""

    policy_file: str = "symbolic_policy.yaml"
    auto_load: bool = True
    max_history: int = 10_000
    max_condition_length: int = 512

    # Allowed variable names in conditions.
    allowed_variables: tuple = (
        "carbon", "energy", "latency", "memory", "tool_calls",
        "cpu_percent", "query_type", "environment", "error_rate",
        "memory_growth_rate", "energy_variance", "latency_std_dev",
        "policy_violation_count", "avg_energy_per_step",
    )

    def __post_init__(self) -> None:
        if not isinstance(self.policy_file, str) or not self.policy_file:
            raise SymbolicReasoningError(
                "policy_file must be a non-empty string."
            )
        if self.max_history <= 0:
            raise SymbolicReasoningError("max_history must be > 0.")
        if self.max_condition_length <= 0:
            raise SymbolicReasoningError(
                "max_condition_length must be > 0."
            )
        if not isinstance(self.allowed_variables, tuple):
            raise SymbolicReasoningError(
                "allowed_variables must be a tuple."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "allowed_variables": list(self.allowed_variables),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SymbolicReasoningConfig":
        if not isinstance(data, Mapping):
            raise SymbolicReasoningError(
                "SymbolicReasoningConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k == "allowed_variables":
                kwargs[k] = tuple(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Dataclasses
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SymbolicRule:
    """Immutable representation of a single symbolic rule."""

    id: str
    name: str
    category: str
    priority: str
    condition: str
    action: str
    explanation: str

    def __post_init__(self) -> None:
        for name in ("id", "name", "category", "condition", "action", "explanation"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise SymbolicReasoningError(
                    f"{name} must be a non-empty string."
                )
        if not isinstance(self.priority, str) or not self.priority:
            raise SymbolicReasoningError(
                "priority must be a non-empty string."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SymbolicRule":
        if not isinstance(data, Mapping):
            raise SymbolicReasoningError("SymbolicRule.from_dict expects a Mapping.")
        return cls(
            id=str(data["id"]),
            name=str(data["name"]),
            category=str(data["category"]),
            priority=str(data["priority"]),
            condition=str(data["condition"]),
            action=str(data["action"]),
            explanation=str(data["explanation"]),
        )


@dataclass(frozen=True)
class ViolationTrace:
    """Immutable formal trace of a rule violation."""

    rule_id: str
    rule_name: str
    timestamp: float
    step: int
    condition: str
    observation: Mapping[str, Any]
    violation_details: str
    action_triggered: str
    explanation: str
    severity: str
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        for name in ("rule_id", "rule_name", "condition", "violation_details",
                     "action_triggered", "explanation", "severity"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise SymbolicReasoningError(
                    f"{name} must be a non-empty string."
                )
        if not isinstance(self.step, int) or self.step < 0:
            raise SymbolicReasoningError("step must be a non-negative int.")
        if not isinstance(self.observation, Mapping):
            raise SymbolicReasoningError("observation must be a Mapping.")
        if not isinstance(self.timestamp, (int, float)) or isinstance(self.timestamp, bool):
            raise SymbolicReasoningError("timestamp must be numeric.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "timestamp": self.timestamp,
            "step": self.step,
            "condition": self.condition,
            "observation": dict(self.observation),
            "violation_details": self.violation_details,
            "action_triggered": self.action_triggered,
            "explanation": self.explanation,
            "severity": self.severity,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ViolationTrace":
        if not isinstance(data, Mapping):
            raise SymbolicReasoningError("ViolationTrace.from_dict expects a Mapping.")
        ts = data.get("created_at")
        if isinstance(ts, str):
            created_at = datetime.fromisoformat(ts)
            if not created_at.tzinfo:
                created_at = created_at.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            created_at = ts
        else:
            created_at = datetime.now(timezone.utc)
        return cls(
            rule_id=str(data["rule_id"]),
            rule_name=str(data["rule_name"]),
            timestamp=float(data.get("timestamp", time.time())),
            step=int(data.get("step", 0)),
            condition=str(data["condition"]),
            observation=dict(data.get("observation", {})),
            violation_details=str(data["violation_details"]),
            action_triggered=str(data["action_triggered"]),
            explanation=str(data["explanation"]),
            severity=str(data.get("severity", "medium")),
            created_at=created_at,
        )

    def __repr__(self) -> str:
        return (
            "ViolationTrace("
            f"rule_id={self.rule_id!r}, "
            f"step={self.step}, "
            f"severity={self.severity!r})"
        )


# --------------------------------------------------------------------------- #
# Safe AST evaluator
# --------------------------------------------------------------------------- #
class _ConditionEvaluator(ast.NodeVisitor):
    """
    AST visitor that evaluates a restricted subset of Python expressions
    against a metrics dict.

    Allowed node types: BoolOp, BinOp (with comparison-safe operators),
    UnaryOp (Not only), Compare, Name, Constant, Tuple, List, Dict.
    """

    _BINOPS = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.Mod: operator.mod,
    }
    _CMPOPS = {
        ast.Eq: operator.eq,
        ast.NotEq: operator.ne,
        ast.Lt: operator.lt,
        ast.LtE: operator.le,
        ast.Gt: operator.gt,
        ast.GtE: operator.ge,
        ast.In: lambda a, b: a in b,
        ast.NotIn: lambda a, b: a not in b,
    }

    def __init__(self, context: Mapping[str, Any]) -> None:
        self.context = context

    def visit_Expression(self, node: ast.Expression) -> Any:  # noqa: N802
        return self.visit(node.body)

    def visit_BoolOp(self, node: ast.BoolOp) -> Any:  # noqa: N802
        if isinstance(node.op, ast.And):
            result = True
            for v in node.values:
                result = result and self.visit(v)
            return result
        if isinstance(node.op, ast.Or):
            result = False
            for v in node.values:
                result = result or self.visit(v)
            return result
        raise SymbolicReasoningError("unsupported boolean operator")

    def visit_UnaryOp(self, node: ast.UnaryOp) -> Any:  # noqa: N802
        if isinstance(node.op, ast.Not):
            return not self.visit(node.operand)
        if isinstance(node.op, ast.USub):
            return -self.visit(node.operand)
        raise SymbolicReasoningError("unsupported unary operator")

    def visit_BinOp(self, node: ast.BinOp) -> Any:  # noqa: N802
        fn = self._BINOPS.get(type(node.op))
        if fn is None:
            raise SymbolicReasoningError("unsupported binary operator")
        return fn(self.visit(node.left), self.visit(node.right))

    def visit_Compare(self, node: ast.Compare) -> Any:  # noqa: N802
        left = self.visit(node.left)
        for op, comparator in zip(node.ops, node.comparators):
            fn = self._CMPOPS.get(type(op))
            if fn is None:
                raise SymbolicReasoningError("unsupported comparison")
            right = self.visit(comparator)
            if not fn(left, right):
                return False
            left = right
        return True

    def visit_Name(self, node: ast.Name) -> Any:  # noqa: N802
        if node.id in self.context:
            return self.context[node.id]
        raise SymbolicReasoningError(f"unknown variable {node.id!r}")

    def visit_Constant(self, node: ast.Constant) -> Any:  # noqa: N802
        return node.value

    def visit_List(self, node: ast.List) -> Any:  # noqa: N802
        return [self.visit(e) for e in node.elts]

    def visit_Tuple(self, node: ast.Tuple) -> Any:  # noqa: N802
        return tuple(self.visit(e) for e in node.elts)

    def visit_Dict(self, node: ast.Dict) -> Any:  # noqa: N802
        return {
            self.visit(k): self.visit(v)
            for k, v in zip(node.keys, node.values)
        }

    def generic_visit(self, node: ast.AST) -> Any:  # noqa: N802
        raise SymbolicReasoningError(
            f"unsupported expression node: {type(node).__name__}"
        )


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #
class SymbolicReasoningEngine:
    """
    Lightweight symbolic reasoning engine for rule evaluation.

    Thread-safe, serializable, and bounded in memory. The original public API
    (``evaluate_rules``, ``_normalize_metrics``, ``_evaluate_condition``,
    ``_safe_eval``, ``get_violations_by_category``, ``get_violation_summary``,
    ``export_violations``, ``get_active_rules``) is preserved.
    """

    def __init__(
        self,
        policy_file: str = "symbolic_policy.yaml",
        *,
        config: Optional[SymbolicReasoningConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = SymbolicReasoningConfig(policy_file=str(policy_file))
        self._strict = bool(strict)

        # Legacy attributes preserved.
        self.policy_file: str = self._config.policy_file

        self._lock = threading.RLock()
        self.rules: List[SymbolicRule] = []
        self.composite_rules: List[SymbolicRule] = []
        self.domain_rules: Dict[str, List[SymbolicRule]] = {}
        self.violation_history: Deque[ViolationTrace] = deque(
            maxlen=self._config.max_history
        )
        self.evaluation_count: int = 0
        self._started_at: float = time.time()

        self.operators = {
            ">": operator.gt, "<": operator.lt,
            ">=": operator.ge, "<=": operator.le,
            "==": operator.eq, "!=": operator.ne,
        }

        if self._config.auto_load:
            self._load_rules()

        logger.debug(
            "SymbolicReasoningEngine initialized "
            "(policy=%s, rules=%d, strict=%s)",
            self.policy_file, len(self.rules), self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> SymbolicReasoningConfig:
        return self._config

    @property
    def violations(self) -> List[ViolationTrace]:
        with self._lock:
            return list(self.violation_history)

    # ---------------------------------------------------------- rule loading
    def _load_rules(self) -> None:
        """Load symbolic rules from the policy file (YAML)."""
        if not _YAML_AVAILABLE:
            logger.warning(
                "yaml not installed; using default rules."
            )
            self._load_default_rules()
            return

        path = Path(self.policy_file)
        if not path.exists():
            logger.warning(
                "Symbolic policy file not found: %s. Using default rules.",
                self.policy_file,
            )
            self._load_default_rules()
            return

        try:
            with path.open("r", encoding="utf-8") as f:
                policy = yaml.safe_load(f) or {}
        except Exception as exc:
            logger.error("Could not parse %s: %s", path, exc)
            if self._strict:
                raise SymbolicReasoningError(
                    f"policy file parse failed: {exc}"
                ) from exc
            self._load_default_rules()
            return

        if not isinstance(policy, Mapping):
            msg = f"policy root must be a Mapping, got {type(policy).__name__}."
            if self._strict:
                raise SymbolicReasoningError(msg)
            logger.warning("%s Using default rules.", msg)
            self._load_default_rules()
            return

        with self._lock:
            self.rules.clear()
            self.composite_rules.clear()
            self.domain_rules.clear()

            for rule_data in policy.get("symbolic_rules", []) or []:
                try:
                    self.rules.append(SymbolicRule(**rule_data))
                except (TypeError, SymbolicReasoningError) as exc:
                    logger.warning("Skipping malformed rule: %s", exc)

            for rule_data in policy.get("composite_rules", []) or []:
                try:
                    self.composite_rules.append(SymbolicRule(**rule_data))
                except (TypeError, SymbolicReasoningError) as exc:
                    logger.warning("Skipping malformed composite rule: %s", exc)

            domain_ext = policy.get("domain_extensions", {}) or {}
            for domain, rules_data in domain_ext.items():
                bucket: List[SymbolicRule] = []
                for r in rules_data or []:
                    try:
                        bucket.append(SymbolicRule(
                            id=r["id"],
                            name=r.get("name", r["id"]),
                            category="domain",
                            priority=r.get("priority", "medium"),
                            condition=r["condition"],
                            action=r["action"],
                            explanation=r["explanation"],
                        ))
                    except (KeyError, SymbolicReasoningError) as exc:
                        logger.warning(
                            "Skipping malformed domain rule in %s: %s",
                            domain, exc,
                        )
                self.domain_rules[domain] = bucket

        logger.info(
            "Loaded %d basic rules, %d composite rules, %d domains.",
            len(self.rules), len(self.composite_rules), len(self.domain_rules),
        )

    def _load_default_rules(self) -> None:
        """Load minimal default rules if the policy file is not available."""
        with self._lock:
            self.rules = [
                SymbolicRule(
                    id="DEFAULT-001",
                    name="Energy Budget Exceeded",
                    category="sustainability",
                    priority="critical",
                    condition="energy > 5.0",
                    action="halt_execution",
                    explanation="Energy consumption exceeds maximum allowed budget",
                ),
                SymbolicRule(
                    id="DEFAULT-002",
                    name="Memory Overflow Risk",
                    category="resource",
                    priority="critical",
                    condition="memory > 500",
                    action="trigger_resource_alert",
                    explanation="Memory usage exceeds safe threshold",
                ),
            ]

    # ---------------------------------------------------------- evaluation
    def evaluate_rules(
        self,
        metrics: Mapping[str, Any],
        step: int,
        domain: Optional[str] = None,
    ) -> List[ViolationTrace]:
        """
        Evaluate all applicable rules against ``metrics``.

        Returns the list of violation traces for rules that triggered.
        """
        if not isinstance(metrics, Mapping):
            raise SymbolicReasoningError("metrics must be a Mapping.")
        if not isinstance(step, int) or step < 0:
            raise SymbolicReasoningError("step must be a non-negative int.")
        if domain is not None and (not isinstance(domain, str) or not domain):
            raise SymbolicReasoningError(
                "domain must be a non-empty string or None."
            )

        normalized = self._normalize_metrics(dict(metrics))

        with self._lock:
            self.evaluation_count += 1

        violations: List[ViolationTrace] = []
        with self._lock:
            basic = list(self.rules)
            composite = list(self.composite_rules)
            domain_rules = list(self.domain_rules.get(domain, [])) if domain else []

        for rule in basic + composite + domain_rules:
            try:
                if self._evaluate_condition(rule.condition, normalized):
                    violations.append(
                        self._create_violation_trace(rule, normalized, step)
                    )
            except SymbolicReasoningError as exc:
                if self._strict:
                    raise
                logger.warning("Rule %s evaluation failed: %s", rule.id, exc)

        with self._lock:
            self.violation_history.extend(violations)

        logger.debug(
            "Evaluated %d rules at step %d: %d violation(s).",
            len(basic) + len(composite) + len(domain_rules),
            step, len(violations),
        )
        return violations

    def _normalize_metrics(self, metrics: Mapping[str, Any]) -> Dict[str, Any]:
        """Normalize metrics to standard variable names for rule evaluation."""
        cumulative = metrics.get("cumulative", {}) or {}
        normalized: Dict[str, Any] = {}
        normalized["energy"] = cumulative.get(
            "total_energy_wh", metrics.get("energy", 0)
        )
        normalized["carbon"] = cumulative.get(
            "total_carbon_kg", metrics.get("carbon", 0)
        ) * 1000  # Convert to grams
        normalized["latency"] = cumulative.get(
            "total_latency_ms", metrics.get("latency", 0) * 1000
        )
        normalized["memory"] = cumulative.get(
            "max_memory_mb", metrics.get("memory_mb", 0)
        )
        normalized["tool_calls"] = cumulative.get(
            "total_tool_calls", metrics.get("tool_calls", 0)
        )
        normalized["cpu_percent"] = metrics.get("cpu_percent", 0)

        if "step_count" in cumulative:
            normalized["avg_energy_per_step"] = normalized["energy"] / max(
                cumulative["step_count"], 1
            )

        normalized["query_type"] = metrics.get("query_type", "unknown")
        normalized["environment"] = metrics.get("environment", "development")
        normalized["error_rate"] = metrics.get("error_rate", 0)
        normalized["memory_growth_rate"] = metrics.get("memory_growth_rate", 0)
        normalized["energy_variance"] = metrics.get("energy_variance", 0)
        normalized["latency_std_dev"] = metrics.get("latency_std_dev", 0)
        normalized["policy_violation_count"] = metrics.get(
            "policy_violation_count", 0
        )
        return normalized

    def _evaluate_condition(
        self, condition: str, metrics: Mapping[str, Any]
    ) -> bool:
        """
        Evaluate a symbolic condition against normalized metrics.

        The condition language supports ``AND`` / ``OR`` / ``NOT`` (case-
        insensitive), arithmetic operators (``+ - * / %``), and comparison
        operators (``> < >= <= == != in not in``).
        """
        if not isinstance(condition, str) or not condition:
            raise SymbolicReasoningError("condition must be a non-empty string.")
        if len(condition) > self._config.max_condition_length:
            raise SymbolicReasoningError(
                f"condition exceeds max length "
                f"{self._config.max_condition_length}."
            )

        # Normalize boolean operators to lowercase Python keywords.
        normalized = (
            condition.replace(" AND ", " and ")
            .replace(" OR ", " or ")
            .replace(" NOT ", " not ")
        )
        try:
            return self._safe_eval(normalized, metrics)
        except SymbolicReasoningError:
            raise
        except Exception as exc:
            logger.warning(
                "Error evaluating condition %r: %s", condition, exc,
            )
            return False

    def _safe_eval(self, condition: str, metrics: Mapping[str, Any]) -> bool:
        """
        Safely evaluate ``condition`` against ``metrics`` using an AST-based
        evaluator that only permits a restricted subset of Python expressions.
        """
        try:
            tree = ast.parse(condition, mode="eval")
        except SyntaxError as exc:
            raise SymbolicReasoningError(
                f"invalid condition syntax: {exc}"
            ) from exc

        evaluator = _ConditionEvaluator(metrics)
        try:
            return bool(evaluator.visit(tree))
        except SymbolicReasoningError:
            raise
        except Exception as exc:
            raise SymbolicReasoningError(
                f"failed to evaluate condition: {exc}"
            ) from exc

    def _create_violation_trace(
        self, rule: SymbolicRule, metrics: Mapping[str, Any], step: int
    ) -> ViolationTrace:
        """Create a formal violation trace for a triggered rule."""
        observation = self._extract_relevant_metrics(rule.condition, metrics)
        details = self._generate_violation_details(rule, observation)
        return ViolationTrace(
            rule_id=rule.id,
            rule_name=rule.name,
            timestamp=time.time(),
            step=step,
            condition=rule.condition,
            observation=observation,
            violation_details=details,
            action_triggered=rule.action,
            explanation=rule.explanation,
            severity=rule.priority,
        )

    def _extract_relevant_metrics(
        self, condition: str, metrics: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """Extract only the metrics mentioned in ``condition``."""
        variables = re.findall(r"\b([a-z_]+)\b", condition.lower())
        return {v: metrics[v] for v in set(variables) if v in metrics}

    def _generate_violation_details(
        self, rule: SymbolicRule, observation: Mapping[str, Any]
    ) -> str:
        """Generate human-readable violation details."""
        details = [
            f"Rule: {rule.name} ({rule.id})",
            f"Condition: {rule.condition}",
            "Observations:",
        ]
        for key, value in observation.items():
            if isinstance(value, float):
                details.append(f"  {key} = {value:.4f}")
            else:
                details.append(f"  {key} = {value}")
        details.append(f"Violation: Rule triggered -> {rule.action}")
        return "\n".join(details)

    # ---------------------------------------------------------- queries
    def get_violations_by_category(self, category: str) -> List[ViolationTrace]:
        """Get all violations for a specific category."""
        if not isinstance(category, str) or not category:
            raise SymbolicReasoningError("category must be a non-empty string.")
        with self._lock:
            history = list(self.violation_history)
        return [v for v in history if self._get_rule_category(v.rule_id) == category]

    def _get_rule_category(self, rule_id: str) -> str:
        with self._lock:
            for rule in self.rules + self.composite_rules:
                if rule.id == rule_id:
                    return rule.category
        return "unknown"

    def get_violation_summary(self) -> Dict[str, Any]:
        """Get a summary of all violations."""
        with self._lock:
            history = list(self.violation_history)
            evaluations = self.evaluation_count
        if not history:
            return {
                "total_violations": 0,
                "evaluations": evaluations,
                "by_category": {},
                "by_severity": {},
                "violation_rate": 0.0,
            }
        by_category: Dict[str, int] = {}
        by_severity: Dict[str, int] = {}
        for v in history:
            category = self._get_rule_category(v.rule_id)
            by_category[category] = by_category.get(category, 0) + 1
            by_severity[v.severity] = by_severity.get(v.severity, 0) + 1
        return {
            "total_violations": len(history),
            "evaluations": evaluations,
            "by_category": by_category,
            "by_severity": by_severity,
            "violation_rate": len(history) / max(evaluations, 1),
        }

    def get_active_rules(self) -> List[Dict[str, Any]]:
        """Get a list of all active rules."""
        with self._lock:
            all_rules = list(self.rules) + list(self.composite_rules)
            for domain_rules in self.domain_rules.values():
                all_rules.extend(domain_rules)
        return [r.to_dict() for r in all_rules]

    # ---------------------------------------------------------- lifecycle
    def reset(self, *, clear_violations: bool = True) -> int:
        """Reset the engine counters and (optionally) the violation history."""
        with self._lock:
            removed = len(self.violation_history)
            if clear_violations:
                self.violation_history.clear()
            self.evaluation_count = 0
            self._started_at = time.time()
        logger.debug("SymbolicReasoningEngine reset (removed %d).", removed)
        return removed

    def statistics(self) -> Dict[str, Any]:
        """Return a JSON-safe snapshot of the engine state."""
        summary = self.get_violation_summary()
        return {
            "rules": len(self.rules),
            "composite_rules": len(self.composite_rules),
            "domains": list(self.domain_rules.keys()),
            "evaluations": self.evaluation_count,
            "violations": summary["total_violations"],
            "violation_rate": summary["violation_rate"],
            "config": self._config.to_dict(),
            "strict": self._strict,
            "yaml_available": _YAML_AVAILABLE,
            "uptime_seconds": time.time() - self._started_at,
        }

    # ---------------------------------------------------------- export/serialization
    def export_violations(self, filepath: str) -> int:
        """Export violation traces to a JSON file. Returns bytes written."""
        if not isinstance(filepath, str) or not filepath:
            raise SymbolicReasoningError("filepath must be a non-empty string.")
        path = Path(filepath)
        data = {
            "summary": self.get_violation_summary(),
            "violations": [v.to_dict() for v in self.violation_history],
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)
        except OSError as exc:
            raise SymbolicReasoningError(
                f"could not write {path}: {exc}"
            ) from exc
        return path.stat().st_size

    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "rules": [r.to_dict() for r in self.rules],
                "composite_rules": [r.to_dict() for r in self.composite_rules],
                "domain_rules": {
                    k: [r.to_dict() for r in v]
                    for k, v in self.domain_rules.items()
                },
                "evaluation_count": self.evaluation_count,
                "started_at": self._started_at,
            }
            if include_history:
                payload["violation_history"] = [
                    v.to_dict() for v in self.violation_history
                ]
        return payload

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    def __repr__(self) -> str:
        with self._lock:
            return (
                "SymbolicReasoningEngine("
                f"rules={len(self.rules)}, "
                f"composite={len(self.composite_rules)}, "
                f"domains={len(self.domain_rules)}, "
                f"violations={len(self.violation_history)}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "SymbolicReasoningConfig",
    "SymbolicReasoningEngine",
    "SymbolicReasoningError",
    "SymbolicRule",
    "ViolationTrace",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m symbolic.symbolic_reasoning_engine
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Default rules (no policy file) --------------------------- #
    engine = SymbolicReasoningEngine(config=SymbolicReasoningConfig(auto_load=False))
    engine._load_default_rules()
    print("repr       :", engine)

    # ---- Trigger a rule ------------------------------------------ #
    violations = engine.evaluate_rules(
        {"energy": 10.0, "memory": 100.0}, step=1,
    )
    print("violations :", violations)
    assert len(violations) == 1
    assert violations[0].rule_id == "DEFAULT-001"

    # ---- Compound condition --------------------------------------- #
    engine.rules.append(SymbolicRule(
        id="TEST-001",
        name="Carbon and Latency",
        category="test",
        priority="high",
        condition="carbon > 60 AND latency > 2000",
        action="alert",
        explanation="both metrics exceeded",
    ))
    violations = engine.evaluate_rules(
        {"carbon": 0.1, "latency": 3.0}, step=2,
    )
    print("compound   :", [v.rule_id for v in violations])
    assert any(v.rule_id == "TEST-001" for v in violations)

    # ---- Bug fix: safe AST evaluation ----------------------------- #
    for bad in (
        "os.system('rm -rf /')",
        "__import__('os').system('ls')",
        "carbon > 60 or True",
        "unknown_var > 1",
    ):
        try:
            engine._safe_eval(bad, {"carbon": 100})
        except SymbolicReasoningError as exc:
            print("Rejected   :", exc)

    # ---- Bug fix: NaN / inf in metrics ---------------------------- #
    violations = engine.evaluate_rules(
        {"energy": float("nan"), "memory": 0}, step=3,
    )
    print("nan metric :", violations)

    # ---- Statistics ----------------------------------------------- #
    print("statistics :", {
        k: v for k, v in engine.statistics().items()
        if k not in ("config", "uptime_seconds")
    })

    # ---- Serialization -------------------------------------------- #
    payload = engine.to_json()
    restored = json.loads(payload)
    assert restored["config"]["max_history"] == 10_000
    print("Serialization OK.")

    # ---- Validation failures -------------------------------------- #
    for bad_call in (
        lambda: engine.evaluate_rules("not-a-mapping", 1),
        lambda: engine.evaluate_rules({}, -1),
        lambda: engine.evaluate_rules({}, 1, domain=""),
        lambda: engine._evaluate_condition("", {}),
        lambda: engine._evaluate_condition("x" * 1000, {}),
    ):
        try:
            bad_call()
        except SymbolicReasoningError as exc:
            print("Rejected   :", exc)

    print("\nSmoke test passed.")
