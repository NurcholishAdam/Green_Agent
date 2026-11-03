# src/symbolic/symbolic_reasoning_engine.py

"""
Symbolic Reasoning Engine
=========================

Lightweight symbolic logic engine for evaluating formal rules against
metrics. Inspired by the FormalJudge neuro-symbolic oversight paradigm.

Condition language
------------------
The condition language supports ``AND`` / ``OR`` / ``NOT`` (case-insensitive),
arithmetic operators (``+ - * / %``), and comparison operators
(``> < >= <= == != in not in``). Conditions are parsed with ``ast.parse``
and evaluated by a restricted AST visitor — no builtins, no attribute
access, no comprehensions, no string interpolation.

Variable allowlist and trivially-forced conditions
--------------------------------------------------
The engine rejects (in both strict and non-strict modes):

- Any ``Name`` node not listed in ``config.allowed_variables``.
- Any ``Constant(True)`` / ``Constant(False)`` that appears as the root
  expression, as a direct operand of a ``BoolOp`` that would force its
  result (``... or True``, ``... and False``), or as the operand of
  ``not`` (``not True``, ``not False``).
- Any AST node not handled by ``_ConditionEvaluator`` — including
  ``Call``, ``Attribute``, ``Subscript``, comprehensions, f-strings,
  lambdas, and named expressions.

The first two checks close the "policy rule trivially enabled / disabled"
loophole: a rule whose condition is ``carbon > 60 or True`` no longer
silently fires on every step.

Unit conventions (``_normalize_metrics``)
-----------------------------------------
Normalization assumes ``cumulative.total_carbon_kg`` is in **kg** and
``cumulative.total_latency_ms`` is in **ms**; both are converted by
``config.carbon_unit_factor`` / ``config.latency_unit_factor`` (default
``1000``). If your upstream already emits grams or milliseconds, set
those factors to ``1.0`` — otherwise rules will fire at 1/1000 of their
intended threshold.

NaN policy
----------
``config.nan_policy`` ∈ ``{"coerce", "raise"}``:

- ``"coerce"`` (default; backward-compatible): non-finite metrics are
  replaced with ``0.0`` and a ``WARNING`` is logged.
- ``"raise"``: non-finite metrics raise :class:`SymbolicReasoningError`.

For safety-critical deployments, prefer ``"raise"`` — a NaN silently
treated as 0 can suppress a rule that should have fired.

Enhancements
------------
- ``SymbolicReasoningConfig`` — frozen, ``slots``, ``__repr__``; every
  field type-checked; new fields ``nan_policy``, ``carbon_unit_factor``,
  ``latency_unit_factor``; ``from_dict(..., strict=True)`` rejects
  unknown keys and wraps parse errors.
- ``SymbolicRule`` / ``ViolationTrace`` — ``slots``; ``ViolationTrace``
  gains a ``category`` field (populated at creation time, no more
  O(n×m) lookups).
- **``from_dict`` / ``from_json`` on the engine** — round-trips the
  full state, including counters and (optionally) history.
- **``to_json(include_history=True)`` now works** — the kwarg is routed
  to ``to_dict``; ``default=str`` removed everywhere.
- **``allowed_variables`` is enforced** — parsed AST is rejected before
  evaluation if any name is not in the allowlist.
- **Trivially-forced conditions rejected** — ``or True`` / ``and False``
  / ``not True`` etc. raise instead of silently firing.
- **Strict mode raises on malformed rules** — a policy file that parses
  but contains a malformed rule no longer silently drops the rule.
- **``_normalize_metrics`` failures wrapped** — ``cumulative`` non-Mapping
  and non-numeric values raise :class:`SymbolicReasoningError`, not
  ``AttributeError`` / ``TypeError``.
- **``_evaluate_condition`` no longer swallows exceptions** — any
  ``SymbolicReasoningError`` from evaluation propagates.
- **Lifecycle counters** — ``total_evaluations`` and ``total_violations``
  are cumulative for the object's lifetime; ``evaluation_count`` remains
  reset-able for backward compatibility.
- **``evaluate_rules`` sorts violations** by severity (critical first),
  then rule id — deterministic across policy reloads.
- **``__eq__`` / ``__hash__`` on the engine**; ``__repr__`` on the config.
- **Smoke test uses ``_check()`` helpers** — ``python -O`` no longer
  passes silently.

Compatibility
-------------
- ``SymbolicReasoningEngine(policy_file="...", *, config=None,
  strict=True)`` unchanged.
- ``evaluate_rules(metrics, step, domain=None) -> List[ViolationTrace]``
  unchanged in signature; return type preserves iteration order
  semantics but is now sorted by severity (documented).
- ``ViolationTrace`` gained ``category`` (default ``"unknown"``) — old
  payloads round-trip via ``from_dict`` and have category recomputed
  from the loaded rules.
- ``_safe_eval`` now additionally rejects names outside
  ``config.allowed_variables`` and trivially-forced literal operands —
  a policy rule that relied on either now raises on evaluation. This is
  the intended tightening.
- ``_load_rules`` raises in strict mode on malformed rules instead of
  silently skipping.
- ``_normalize_metrics`` raises on non-Mapping ``cumulative`` and
  non-numeric values (previously raw ``AttributeError`` / ``TypeError``).
- ``config.nan_policy`` defaults to ``"coerce"`` for backward
  compatibility — existing callers with NaN metrics see no change.

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop it for older versions.
"""

from __future__ import annotations

import ast
import json
import logging
import math
import operator
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Mapping, Optional, Tuple, TypedDict

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
# Constants
# --------------------------------------------------------------------------- #
_PRIORITY_RANK: Dict[str, int] = {
    "critical": 0, "high": 1, "medium": 2, "low": 3,
}
_PRIORITY_RANK_DEFAULT = 4

_NAN_POLICIES = ("coerce", "raise")


# --------------------------------------------------------------------------- #
# Statistics TypedDict
# --------------------------------------------------------------------------- #
class SymbolicReasoningStatistics(TypedDict):
    """Return shape of :meth:`SymbolicReasoningEngine.statistics`."""

    rules: int
    composite_rules: int
    domains: List[str]
    evaluations: int
    total_evaluations: int
    violations: int
    total_violations: int
    violation_rate: float
    config: Dict[str, Any]
    strict: bool
    yaml_available: bool
    uptime_seconds: float


# --------------------------------------------------------------------------- #
# Numeric helpers
# --------------------------------------------------------------------------- #
def _require_finite_positive(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SymbolicReasoningError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv) or fv <= 0:
        raise SymbolicReasoningError(
            f"{name} must be finite and > 0, got {value!r}."
        )
    return fv


def _require_positive_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SymbolicReasoningError(f"{name} must be an int.")
    if value <= 0:
        raise SymbolicReasoningError(f"{name} must be > 0.")
    return value


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class SymbolicReasoningConfig:
    """Tunable parameters for :class:`SymbolicReasoningEngine`."""

    policy_file: str = "symbolic_policy.yaml"
    auto_load: bool = True
    max_history: int = 10_000
    max_condition_length: int = 512

    # Names permitted in condition expressions. Enforced via AST walk.
    allowed_variables: tuple = (
        "carbon", "energy", "latency", "memory", "tool_calls",
        "cpu_percent", "query_type", "environment", "error_rate",
        "memory_growth_rate", "energy_variance", "latency_std_dev",
        "policy_violation_count", "avg_energy_per_step",
    )

    # NaN / inf policy for metric values: "coerce" (0.0 + warning) or
    # "raise". Default "coerce" for backward compatibility; "raise" is
    # recommended for safety-critical deployments.
    nan_policy: str = "coerce"

    # Unit conversion factors applied in _normalize_metrics.
    carbon_unit_factor: float = 1000.0   # kg -> grams
    latency_unit_factor: float = 1000.0  # s -> ms

    def __post_init__(self) -> None:
        if not isinstance(self.policy_file, str) or not self.policy_file:
            raise SymbolicReasoningError(
                "policy_file must be a non-empty string."
            )
        if not isinstance(self.auto_load, bool):
            raise SymbolicReasoningError("auto_load must be a bool.")
        _require_positive_int("max_history", self.max_history)
        _require_positive_int(
            "max_condition_length", self.max_condition_length,
        )
        if not isinstance(self.allowed_variables, tuple):
            raise SymbolicReasoningError(
                "allowed_variables must be a tuple."
            )
        for entry in self.allowed_variables:
            if not isinstance(entry, str) or not entry:
                raise SymbolicReasoningError(
                    "allowed_variables entries must be non-empty strings."
                )
        if self.nan_policy not in _NAN_POLICIES:
            raise SymbolicReasoningError(
                f"nan_policy must be one of {_NAN_POLICIES}, got "
                f"{self.nan_policy!r}."
            )
        _require_finite_positive(
            "carbon_unit_factor", self.carbon_unit_factor,
        )
        _require_finite_positive(
            "latency_unit_factor", self.latency_unit_factor,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "allowed_variables": list(self.allowed_variables),
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = True,
    ) -> "SymbolicReasoningConfig":
        if not isinstance(data, Mapping):
            raise SymbolicReasoningError(
                "SymbolicReasoningConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        if strict:
            unknown = [k for k in data if k not in valid]
            if unknown:
                raise SymbolicReasoningError(
                    f"unknown SymbolicReasoningConfig keys: {unknown}"
                )
        try:
            kwargs: Dict[str, Any] = {}
            for k, v in data.items():
                if k not in valid:
                    continue
                if k == "allowed_variables":
                    kwargs[k] = tuple(v)
                else:
                    kwargs[k] = v
            return cls(**kwargs)
        except SymbolicReasoningError:
            raise
        except (TypeError, ValueError) as exc:
            raise SymbolicReasoningError(
                f"invalid SymbolicReasoningConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "SymbolicReasoningConfig("
            f"policy={self.policy_file!r}, "
            f"auto_load={self.auto_load}, "
            f"max_history={self.max_history}, "
            f"nan_policy={self.nan_policy!r}, "
            f"vars={len(self.allowed_variables)})"
        )


# --------------------------------------------------------------------------- #
# Rule
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
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
        for name in (
            "id", "name", "category", "priority",
            "condition", "action", "explanation",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise SymbolicReasoningError(
                    f"{name} must be a non-empty string."
                )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SymbolicRule":
        if not isinstance(data, Mapping):
            raise SymbolicReasoningError(
                "SymbolicRule.from_dict expects a Mapping."
            )
        try:
            return cls(
                id=str(data["id"]),
                name=str(data["name"]),
                category=str(data["category"]),
                priority=str(data["priority"]),
                condition=str(data["condition"]),
                action=str(data["action"]),
                explanation=str(data["explanation"]),
            )
        except KeyError as exc:
            raise SymbolicReasoningError(
                f"missing required key in SymbolicRule: {exc}"
            ) from exc
        except (TypeError, ValueError) as exc:
            raise SymbolicReasoningError(
                f"invalid SymbolicRule payload: {exc}"
            ) from exc


# --------------------------------------------------------------------------- #
# Violation trace
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class ViolationTrace:
    """
    Immutable formal trace of a rule violation.

    ``category`` is populated from the rule at creation time. This
    eliminates the O(violations × rules) lookup that
    :meth:`SymbolicReasoningEngine.get_violation_summary` previously
    performed, and preserves the category even after a policy reload
    removes the originating rule.
    """

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
    category: str = "unknown"
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        for name in (
            "rule_id", "rule_name", "condition", "violation_details",
            "action_triggered", "explanation", "severity", "category",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise SymbolicReasoningError(
                    f"{name} must be a non-empty string."
                )
        if isinstance(self.step, bool) or not isinstance(self.step, int):
            raise SymbolicReasoningError("step must be a non-negative int.")
        if self.step < 0:
            raise SymbolicReasoningError("step must be a non-negative int.")
        if not isinstance(self.observation, Mapping):
            raise SymbolicReasoningError("observation must be a Mapping.")
        if isinstance(self.timestamp, bool) or not isinstance(
            self.timestamp, (int, float)
        ):
            raise SymbolicReasoningError("timestamp must be numeric.")
        if not math.isfinite(float(self.timestamp)):
            raise SymbolicReasoningError("timestamp must be finite.")

    def to_dict(self) -> Dict[str, Any]:
        # Shallow copy of observation — values are primitives in practice.
        return {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "timestamp": float(self.timestamp),
            "step": int(self.step),
            "condition": self.condition,
            "observation": dict(self.observation),
            "violation_details": self.violation_details,
            "action_triggered": self.action_triggered,
            "explanation": self.explanation,
            "severity": self.severity,
            "category": self.category,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ViolationTrace":
        if not isinstance(data, Mapping):
            raise SymbolicReasoningError(
                "ViolationTrace.from_dict expects a Mapping."
            )
        try:
            ts = data.get("created_at")
            if ts is None:
                created_at = datetime.now(timezone.utc)
            elif isinstance(ts, str):
                created_at = datetime.fromisoformat(ts)
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
            elif isinstance(ts, datetime):
                created_at = ts
            else:
                raise SymbolicReasoningError(
                    f"created_at must be str|datetime|None, "
                    f"got {type(ts).__name__}."
                )
            return cls(
                rule_id=str(data["rule_id"]),
                rule_name=str(data["rule_name"]),
                timestamp=float(data.get("timestamp", time.time())),
                step=int(data.get("step", 0)),
                condition=str(data["condition"]),
                observation=dict(data.get("observation", {}) or {}),
                violation_details=str(data["violation_details"]),
                action_triggered=str(data["action_triggered"]),
                explanation=str(data["explanation"]),
                severity=str(data.get("severity", "unknown")),
                category=str(data.get("category", "unknown")),
                created_at=created_at,
            )
        except SymbolicReasoningError:
            raise
        except KeyError as exc:
            raise SymbolicReasoningError(
                f"missing required key in ViolationTrace: {exc}"
            ) from exc
        except (TypeError, ValueError) as exc:
            raise SymbolicReasoningError(
                f"invalid ViolationTrace payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "ViolationTrace("
            f"rule_id={self.rule_id!r}, "
            f"step={self.step}, "
            f"severity={self.severity!r}, "
            f"category={self.category!r})"
        )


# --------------------------------------------------------------------------- #
# Safe AST evaluator
# --------------------------------------------------------------------------- #
class _ConditionEvaluator(ast.NodeVisitor):
    """
    AST visitor that evaluates a restricted subset of Python expressions
    against a metrics dict.

    Allowed node types: ``Expression``, ``BoolOp``, ``UnaryOp`` (``Not``
    / ``USub``), ``BinOp`` (arithmetic), ``Compare``, ``Name``,
    ``Constant``, ``Tuple``, ``List``, ``Dict``. Everything else (``Call``,
    ``Attribute``, ``Subscript``, comprehensions, f-strings, lambdas,
    named expressions, ...) is rejected by ``generic_visit``.
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
            result: Any = True
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

    Thread-safe for all public methods. The original public API is
    preserved; new parameters are keyword-only.

    ``_started_at`` lifecycle
    -------------------------
    Set at construction. :meth:`reset` resets it to ``time.time()``.
    :meth:`from_dict` restores the serialized value, so
    ``statistics()["uptime_seconds"]`` on a restored engine reports the
    **lifetime of the serialized object**, not the current instance.
    """

    def __init__(
        self,
        policy_file: str = "symbolic_policy.yaml",
        *,
        config: Optional[SymbolicReasoningConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            if not isinstance(config, SymbolicReasoningConfig):
                raise SymbolicReasoningError(
                    f"config must be a SymbolicReasoningConfig, got "
                    f"{type(config).__name__}."
                )
            self._config = config
        else:
            self._config = SymbolicReasoningConfig(
                policy_file=str(policy_file),
            )
        self._strict = bool(strict)

        # Legacy attribute; see class docstring.
        self.policy_file: str = self._config.policy_file

        self._lock = threading.RLock()
        self.rules: List[SymbolicRule] = []
        self.composite_rules: List[SymbolicRule] = []
        self.domain_rules: Dict[str, List[SymbolicRule]] = {}
        self.violation_history: Deque[ViolationTrace] = deque(
            maxlen=self._config.max_history
        )
        # Reset-able counter (backward-compatible public attribute).
        self.evaluation_count: int = 0
        # Lifetime counters (not reset by reset()).
        self._total_evaluations: int = 0
        self._total_violations: int = 0
        self._started_at: float = time.time()

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
    def strict(self) -> bool:
        return self._strict

    @property
    def violations(self) -> List[ViolationTrace]:
        with self._lock:
            return list(self.violation_history)

    @property
    def total_evaluations(self) -> int:
        with self._lock:
            return self._total_evaluations

    @property
    def total_violations(self) -> int:
        with self._lock:
            return self._total_violations

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
            msg = (
                f"policy root must be a Mapping, got "
                f"{type(policy).__name__}."
            )
            if self._strict:
                raise SymbolicReasoningError(msg)
            logger.warning("%s Using default rules.", msg)
            self._load_default_rules()
            return

        with self._lock:
            self.rules.clear()
            self.composite_rules.clear()
            self.domain_rules.clear()

            self._load_rule_list(
                policy.get("symbolic_rules", []) or [],
                self.rules, "symbolic_rules",
            )
            self._load_rule_list(
                policy.get("composite_rules", []) or [],
                self.composite_rules, "composite_rules",
            )

            domain_ext = policy.get("domain_extensions", {}) or {}
            if not isinstance(domain_ext, Mapping):
                if self._strict:
                    raise SymbolicReasoningError(
                        "domain_extensions must be a Mapping."
                    )
                logger.warning(
                    "domain_extensions is not a Mapping; ignoring."
                )
                domain_ext = {}
            for domain, rules_data in domain_ext.items():
                if not isinstance(domain, str) or not domain:
                    continue
                bucket: List[SymbolicRule] = []
                for r in rules_data or []:
                    try:
                        bucket.append(SymbolicRule(
                            id=str(r["id"]),
                            name=str(r.get("name", r["id"])),
                            category="domain",
                            priority=str(r.get("priority", "medium")),
                            condition=str(r["condition"]),
                            action=str(r["action"]),
                            explanation=str(r["explanation"]),
                        ))
                    except (KeyError, TypeError, ValueError,
                            SymbolicReasoningError) as exc:
                        if self._strict:
                            raise SymbolicReasoningError(
                                f"malformed rule in domain "
                                f"{domain!r}: {exc}"
                            ) from exc
                        logger.warning(
                            "Skipping malformed domain rule in %s: %s",
                            domain, exc,
                        )
                self.domain_rules[domain] = bucket

        logger.info(
            "Loaded %d basic rules, %d composite rules, %d domains.",
            len(self.rules), len(self.composite_rules),
            len(self.domain_rules),
        )

    def _load_rule_list(
        self,
        rules_data: Any,
        target: List[SymbolicRule],
        label: str,
    ) -> None:
        """Load a list of rule dicts into ``target``."""
        if not isinstance(rules_data, list):
            if self._strict:
                raise SymbolicReasoningError(
                    f"{label} must be a list."
                )
            logger.warning("%s is not a list; ignoring.", label)
            return
        for rule_data in rules_data:
            try:
                target.append(SymbolicRule(**rule_data))
            except (TypeError, SymbolicReasoningError) as exc:
                if self._strict:
                    raise SymbolicReasoningError(
                        f"malformed rule in {label}: {exc}"
                    ) from exc
                logger.warning("Skipping malformed %s rule: %s", label, exc)

    def _load_default_rules(self) -> None:
        """
        Load minimal default rules if the policy file is not available.

        This method **replaces** the current rule lists — call it only
        on a fresh engine, or be prepared to lose any rules already
        loaded.
        """
        with self._lock:
            self.rules = [
                SymbolicRule(
                    id="DEFAULT-001",
                    name="Energy Budget Exceeded",
                    category="sustainability",
                    priority="critical",
                    condition="energy > 5.0",
                    action="halt_execution",
                    explanation=(
                        "Energy consumption exceeds maximum allowed budget"
                    ),
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

        Returns the list of violation traces for rules that triggered,
        sorted by severity (``critical`` first) then rule id.

        In strict mode, any evaluation error for any rule raises
        :class:`SymbolicReasoningError`. In non-strict mode, rule-level
        errors are logged at ``WARNING`` and the rule is skipped for
        that step.
        """
        if not isinstance(metrics, Mapping):
            raise SymbolicReasoningError("metrics must be a Mapping.")
        if isinstance(step, bool) or not isinstance(step, int):
            raise SymbolicReasoningError("step must be a non-negative int.")
        if step < 0:
            raise SymbolicReasoningError("step must be a non-negative int.")
        if domain is not None and (
            not isinstance(domain, str) or not domain
        ):
            raise SymbolicReasoningError(
                "domain must be a non-empty string or None."
            )

        normalized = self._normalize_metrics(dict(metrics))

        with self._lock:
            self.evaluation_count += 1
            self._total_evaluations += 1
            basic = list(self.rules)
            composite = list(self.composite_rules)
            domain_rules = (
                list(self.domain_rules.get(domain, [])) if domain else []
            )

        violations: List[ViolationTrace] = []
        for rule in basic + composite + domain_rules:
            try:
                if self._evaluate_condition(rule.condition, normalized):
                    violations.append(
                        self._create_violation_trace(
                            rule, normalized, step,
                        )
                    )
            except SymbolicReasoningError as exc:
                if self._strict:
                    raise
                logger.warning(
                    "Rule %s evaluation failed: %s", rule.id, exc,
                )

        # Sort by severity rank, then by rule id for stability.
        violations.sort(
            key=lambda v: (
                _PRIORITY_RANK.get(v.severity, _PRIORITY_RANK_DEFAULT),
                v.rule_id,
            )
        )

        with self._lock:
            self.violation_history.extend(violations)
            self._total_violations += len(violations)

        logger.debug(
            "Evaluated %d rules at step %d: %d violation(s).",
            len(basic) + len(composite) + len(domain_rules),
            step, len(violations),
        )
        return violations

    def _normalize_metrics(
        self, metrics: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """
        Normalize metrics to standard variable names for rule evaluation.

        Unit conversions (see module docstring): ``carbon`` is assumed kg
        upstream and converted to grams; ``latency`` is assumed seconds
        and converted to milliseconds. Adjust
        ``config.carbon_unit_factor`` / ``config.latency_unit_factor`` if
        your source uses different units.
        """
        try:
            cumulative_raw = metrics.get("cumulative", {}) or {}
            if not isinstance(cumulative_raw, Mapping):
                raise SymbolicReasoningError(
                    f"'cumulative' must be a Mapping, got "
                    f"{type(cumulative_raw).__name__}."
                )
            cumulative = cumulative_raw
            cfg = self._config

            normalized: Dict[str, Any] = {}
            normalized["energy"] = self._sanitize_numeric(
                "energy",
                cumulative.get(
                    "total_energy_wh", metrics.get("energy", 0),
                ),
            )
            normalized["carbon"] = (
                self._sanitize_numeric(
                    "carbon",
                    cumulative.get(
                        "total_carbon_kg", metrics.get("carbon", 0),
                    ),
                ) * cfg.carbon_unit_factor
            )
            normalized["latency"] = (
                self._sanitize_numeric(
                    "latency",
                    cumulative.get(
                        "total_latency_ms",
                        self._sanitize_numeric(
                            "latency_seconds_raw",
                            metrics.get("latency", 0),
                        ) * (cfg.latency_unit_factor / 1000.0),
                    ),
                )
            )
            # The above latent formula reproduces the original behavior:
            # metrics.get("latency", 0) * 1000. Use the config factor.
            normalized["latency"] = self._sanitize_numeric(
                "latency",
                cumulative.get(
                    "total_latency_ms",
                    self._sanitize_numeric(
                        "latency_seconds_raw",
                        metrics.get("latency", 0),
                    ) * cfg.latency_unit_factor,
                ),
            )
            normalized["memory"] = self._sanitize_numeric(
                "memory",
                cumulative.get(
                    "max_memory_mb", metrics.get("memory_mb", 0),
                ),
            )
            normalized["tool_calls"] = self._sanitize_numeric(
                "tool_calls",
                cumulative.get(
                    "total_tool_calls", metrics.get("tool_calls", 0),
                ),
            )
            normalized["cpu_percent"] = self._sanitize_numeric(
                "cpu_percent", metrics.get("cpu_percent", 0),
            )

            step_count_raw = cumulative.get("step_count")
            if step_count_raw is not None:
                step_count = self._sanitize_numeric(
                    "step_count", step_count_raw,
                )
                normalized["avg_energy_per_step"] = (
                    normalized["energy"] / max(step_count, 1)
                )

            normalized["query_type"] = str(
                metrics.get("query_type", "unknown")
            )
            normalized["environment"] = str(
                metrics.get("environment", "development")
            )
            normalized["error_rate"] = self._sanitize_numeric(
                "error_rate", metrics.get("error_rate", 0),
            )
            normalized["memory_growth_rate"] = self._sanitize_numeric(
                "memory_growth_rate",
                metrics.get("memory_growth_rate", 0),
            )
            normalized["energy_variance"] = self._sanitize_numeric(
                "energy_variance", metrics.get("energy_variance", 0),
            )
            normalized["latency_std_dev"] = self._sanitize_numeric(
                "latency_std_dev", metrics.get("latency_std_dev", 0),
            )
            normalized["policy_violation_count"] = self._sanitize_numeric(
                "policy_violation_count",
                metrics.get("policy_violation_count", 0),
            )
            return normalized
        except SymbolicReasoningError:
            raise
        except (TypeError, AttributeError, ValueError) as exc:
            raise SymbolicReasoningError(
                f"failed to normalize metrics: {exc}"
            ) from exc

    def _sanitize_numeric(self, name: str, value: Any) -> float:
        """Return a finite float; raise / coerce per ``nan_policy``."""
        if isinstance(value, bool):
            raise SymbolicReasoningError(
                f"{name} must be numeric, got bool."
            )
        if not isinstance(value, (int, float)):
            try:
                fv = float(value)
            except (TypeError, ValueError) as exc:
                raise SymbolicReasoningError(
                    f"{name} must be numeric, got "
                    f"{type(value).__name__}."
                ) from exc
        else:
            fv = float(value)
        if not math.isfinite(fv):
            if self._config.nan_policy == "raise":
                raise SymbolicReasoningError(
                    f"{name} must be finite, got {value!r}."
                )
            logger.warning(
                "%s is not finite (%r); coercing to 0.0.", name, value,
            )
            return 0.0
        return fv

    def _evaluate_condition(
        self, condition: str, metrics: Mapping[str, Any]
    ) -> bool:
        """
        Evaluate a symbolic condition against normalized metrics.

        Raises
        ------
        SymbolicReasoningError
            If the condition is empty, too long, references disallowed
            variables, or contains unsupported AST nodes.
        """
        if not isinstance(condition, str) or not condition:
            raise SymbolicReasoningError(
                "condition must be a non-empty string."
            )
        if len(condition) > self._config.max_condition_length:
            raise SymbolicReasoningError(
                f"condition exceeds max length "
                f"{self._config.max_condition_length}."
            )
        normalized = (
            condition.replace(" AND ", " and ")
            .replace(" OR ", " or ")
            .replace(" NOT ", " not ")
        )
        return self._safe_eval(normalized, metrics)

    def _safe_eval(
        self, condition: str, metrics: Mapping[str, Any]
    ) -> bool:
        """
        Safely evaluate ``condition`` against ``metrics``.

        The AST is validated before evaluation: names must be in
        ``config.allowed_variables`` and ``Constant(True/False)`` operands
        that would force the result are rejected. See module docstring.
        """
        try:
            tree = ast.parse(condition, mode="eval")
        except SyntaxError as exc:
            raise SymbolicReasoningError(
                f"invalid condition syntax: {exc}"
            ) from exc

        self._validate_condition_ast(tree)

        evaluator = _ConditionEvaluator(metrics)
        try:
            return bool(evaluator.visit(tree))
        except SymbolicReasoningError:
            raise
        except Exception as exc:
            raise SymbolicReasoningError(
                f"failed to evaluate condition: {exc}"
            ) from exc

    def _validate_condition_ast(self, tree: ast.Expression) -> None:
        """
        Reject names outside ``allowed_variables`` and trivially-forced
        literal operands before evaluation.
        """
        # Reject a root-level literal bool (condition is just `True` / `False`).
        if isinstance(tree.body, ast.Constant) and isinstance(
            tree.body.value, bool
        ):
            raise SymbolicReasoningError(
                "condition is a literal boolean; use a comparison."
            )

        allowed = set(self._config.allowed_variables)
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                if node.id not in allowed:
                    raise SymbolicReasoningError(
                        f"variable {node.id!r} is not in "
                        f"allowed_variables."
                    )
            elif isinstance(node, ast.BoolOp):
                for value in node.values:
                    if (
                        isinstance(value, ast.Constant)
                        and isinstance(value.value, bool)
                    ):
                        if (
                            isinstance(node.op, ast.Or)
                            and value.value is True
                        ):
                            raise SymbolicReasoningError(
                                "trivially-forced condition: "
                                "`... or True` is always truthy."
                            )
                        if (
                            isinstance(node.op, ast.And)
                            and value.value is False
                        ):
                            raise SymbolicReasoningError(
                                "trivially-forced condition: "
                                "`... and False` is always falsy."
                            )
            elif isinstance(node, ast.UnaryOp) and isinstance(
                node.op, ast.Not
            ):
                if isinstance(node.operand, ast.Constant) and isinstance(
                    node.operand.value, bool
                ):
                    raise SymbolicReasoningError(
                        "trivially-forced condition: `not <bool literal>` "
                        "is a constant."
                    )

    def _create_violation_trace(
        self,
        rule: SymbolicRule,
        metrics: Mapping[str, Any],
        step: int,
    ) -> ViolationTrace:
        """Create a formal violation trace for a triggered rule."""
        observation = self._extract_relevant_metrics(
            rule.condition, metrics,
        )
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
            category=rule.category,
        )

    def _extract_relevant_metrics(
        self, condition: str, metrics: Mapping[str, Any]
    ) -> Dict[str, Any]:
        """
        Extract only the metrics mentioned in ``condition``.

        Uses an AST walk — no more regex false-positives on string
        literals like ``"production"``.
        """
        try:
            tree = ast.parse(condition, mode="eval")
        except SyntaxError:
            # A condition we can't parse can't have produced a violation,
            # but guard against surprises.
            return {}
        names = {
            n.id for n in ast.walk(tree)
            if isinstance(n, ast.Name) and n.id in metrics
        }
        return {n: metrics[n] for n in names}

    def _generate_violation_details(
        self,
        rule: SymbolicRule,
        observation: Mapping[str, Any],
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
    def get_violations_by_category(
        self, category: str
    ) -> List[ViolationTrace]:
        """Get all violations for a specific category."""
        if not isinstance(category, str) or not category:
            raise SymbolicReasoningError(
                "category must be a non-empty string."
            )
        with self._lock:
            history = list(self.violation_history)
        return [v for v in history if v.category == category]

    def _get_rule_category(self, rule_id: str) -> str:
        """Best-effort category lookup for a rule id (fallback path)."""
        with self._lock:
            for rule in self.rules + self.composite_rules:
                if rule.id == rule_id:
                    return rule.category
            for rules in self.domain_rules.values():
                for rule in rules:
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
            by_category[v.category] = by_category.get(v.category, 0) + 1
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
        """
        Reset the engine's ``evaluation_count`` and (optionally) the
        violation history.

        Lifetime counters (``total_evaluations`` / ``total_violations``)
        are **not** reset.
        """
        with self._lock:
            removed = len(self.violation_history)
            if clear_violations:
                self.violation_history.clear()
            self.evaluation_count = 0
            self._started_at = time.time()
        logger.debug("SymbolicReasoningEngine reset (removed %d).", removed)
        return removed

    def statistics(self) -> SymbolicReasoningStatistics:
        """Return a JSON-safe, atomic snapshot of the engine state."""
        with self._lock:
            summary = {
                "total_violations": len(self.violation_history),
                "evaluations": self.evaluation_count,
            }
            return {
                "rules": len(self.rules),
                "composite_rules": len(self.composite_rules),
                "domains": sorted(self.domain_rules.keys()),
                "evaluations": self.evaluation_count,
                "total_evaluations": self._total_evaluations,
                "violations": summary["total_violations"],
                "total_violations": self._total_violations,
                "violation_rate": (
                    summary["total_violations"]
                    / max(self.evaluation_count, 1)
                ),
                "config": self._config.to_dict(),
                "strict": self._strict,
                "yaml_available": _YAML_AVAILABLE,
                "uptime_seconds": time.time() - self._started_at,
            }

    # ---------------------------------------------------------- export/serialization
    def export_violations(self, filepath: str) -> int:
        """Export violation traces to a JSON file. Returns bytes written."""
        if not isinstance(filepath, str) or not filepath:
            raise SymbolicReasoningError(
                "filepath must be a non-empty string."
            )
        path = Path(filepath)
        with self._lock:
            data = {
                "summary": self.get_violation_summary(),
                "violations": [
                    v.to_dict() for v in self.violation_history
                ],
            }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as f:
                # Payload is primitive-only; no default= needed.
                json.dump(data, f, indent=2)
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
                "composite_rules": [
                    r.to_dict() for r in self.composite_rules
                ],
                "domain_rules": {
                    k: [r.to_dict() for r in v]
                    for k, v in self.domain_rules.items()
                },
                "evaluation_count": self.evaluation_count,
                "total_evaluations": self._total_evaluations,
                "total_violations": self._total_violations,
                "started_at": self._started_at,
            }
            if include_history:
                payload["violation_history"] = [
                    v.to_dict() for v in self.violation_history
                ]
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SymbolicReasoningEngine":
        """
        Reconstruct an engine from a ``to_dict()`` payload.

        Restores rules, domain rules, counters, and (optionally) history.
        Old payloads without ``category`` on their violation records are
        upgraded by recomputing category from the loaded rules.
        """
        if not isinstance(data, Mapping):
            raise SymbolicReasoningError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        try:
            cfg = SymbolicReasoningConfig.from_dict(
                dict(data.get("config") or {}), strict=True,
            )
            # Bypass auto_load: rules are restored explicitly below.
            engine = cls(
                config=cfg,
                strict=bool(data.get("strict", True)),
            )
            with engine._lock:
                engine.rules = [
                    SymbolicRule.from_dict(r)
                    for r in data.get("rules", []) or []
                ]
                engine.composite_rules = [
                    SymbolicRule.from_dict(r)
                    for r in data.get("composite_rules", []) or []
                ]
                domain_raw = data.get("domain_rules") or {}
                if isinstance(domain_raw, Mapping):
                    engine.domain_rules = {
                        str(k): [
                            SymbolicRule.from_dict(r) for r in v or []
                        ]
                        for k, v in domain_raw.items()
                    }
                else:
                    engine.domain_rules = {}

                engine.evaluation_count = int(
                    data.get("evaluation_count", 0)
                )
                engine._total_evaluations = int(
                    data.get("total_evaluations", 0)
                )
                engine._total_violations = int(
                    data.get("total_violations", 0)
                )
                if data.get("started_at") is not None:
                    engine._started_at = float(data["started_at"])

                for entry in data.get("violation_history", []) or []:
                    trace = ViolationTrace.from_dict(entry)
                    # Recompute category if the payload lacked it.
                    if trace.category == "unknown":
                        trace = ViolationTrace(
                            rule_id=trace.rule_id,
                            rule_name=trace.rule_name,
                            timestamp=trace.timestamp,
                            step=trace.step,
                            condition=trace.condition,
                            observation=trace.observation,
                            violation_details=trace.violation_details,
                            action_triggered=trace.action_triggered,
                            explanation=trace.explanation,
                            severity=trace.severity,
                            category=engine._get_rule_category(
                                trace.rule_id
                            ),
                            created_at=trace.created_at,
                        )
                    engine.violation_history.append(trace)
            return engine
        except SymbolicReasoningError:
            raise
        except (TypeError, ValueError, KeyError) as exc:
            raise SymbolicReasoningError(
                f"invalid SymbolicReasoningEngine payload: {exc}"
            ) from exc

    def to_json(
        self, *, include_history: bool = False, **kwargs: Any
    ) -> str:
        # Payload is primitive-only; no default= needed.
        return json.dumps(
            self.to_dict(include_history=include_history), **kwargs
        )

    @classmethod
    def from_json(cls, payload: str) -> "SymbolicReasoningEngine":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SymbolicReasoningError(
                f"Invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data)

    # ----------------------------------------------------------------- dunder
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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SymbolicReasoningEngine):
            return NotImplemented
        return self._config == other._config and self._strict == other._strict

    def __hash__(self) -> int:
        return hash((self._config, self._strict))


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "SymbolicReasoningConfig",
    "SymbolicReasoningEngine",
    "SymbolicReasoningError",
    "SymbolicReasoningStatistics",
    "SymbolicRule",
    "ViolationTrace",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m symbolic.symbolic_reasoning_engine
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import sys
    import tempfile

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    passed = 0
    failed = 0

    def _check(name: str, cond: bool) -> None:
        global passed, failed
        if cond:
            passed += 1
            print(f"  ✅ {name}")
        else:
            failed += 1
            print(f"  ❌ {name}")

    # ---- 1. Default rules ---------------------------------------- #
    print("default rules:")
    engine = SymbolicReasoningEngine(
        config=SymbolicReasoningConfig(auto_load=False),
    )
    engine._load_default_rules()
    _check("two default rules", len(engine.rules) == 2)

    violations = engine.evaluate_rules(
        {"energy": 10.0, "memory": 100.0}, step=1,
    )
    _check("one violation", len(violations) == 1)
    _check(
        "DEFAULT-001 triggered",
        violations[0].rule_id == "DEFAULT-001",
    )
    _check(
        "category in trace",
        violations[0].category == "sustainability",
    )

    # ---- 2. Compound conditions ---------------------------------- #
    print("compound conditions:")
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
    _check(
        "compound rule triggered",
        any(v.rule_id == "TEST-001" for v in violations),
    )

    # ---- 3. AST safety (all rejected) ---------------------------- #
    print("AST safety:")
    for bad in (
        "os.system('rm -rf /')",
        "__import__('os').system('ls')",
        "carbon > 60 or True",             # trivially forced
        "carbon > 60 and False",           # trivially forced
        "not True",                         # trivially forced
        "True",                             # root literal
        "False",                            # root literal
        "unknown_var > 1",
    ):
        try:
            engine._safe_eval(bad, {"carbon": 100})
            _check(f"reject {bad!r}", False)
        except SymbolicReasoningError:
            _check(f"reject {bad!r}", True)

    # A valid condition still works.
    _check(
        "valid condition still evaluates",
        engine._safe_eval("carbon > 60", {"carbon": 100}) is True,
    )

    # ---- 4. allowed_variables enforcement ------------------------ #
    print("allowed_variables enforcement:")
    custom_cfg = SymbolicReasoningConfig(
        auto_load=False,
        allowed_variables=("energy", "carbon"),
    )
    custom = SymbolicReasoningEngine(config=custom_cfg)
    try:
        custom._safe_eval(
            "tool_calls > 5", {"tool_calls": 10},
        )
        _check("disallowed variable rejected", False)
    except SymbolicReasoningError:
        _check("disallowed variable rejected", True)
    _check(
        "allowed variable still works",
        custom._safe_eval("energy > 5", {"energy": 10}) is True,
    )

    # ---- 5. NaN policy ------------------------------------------- #
    print("nan policy:")
    # Default coerce: NaN becomes 0.0, rule does not fire
    coerce_engine = SymbolicReasoningEngine(
        config=SymbolicReasoningConfig(auto_load=False),
    )
    coerce_engine._load_default_rules()
    coerced_v = coerce_engine.evaluate_rules(
        {"energy": float("nan"), "memory": 0}, step=1,
    )
    _check(
        "coerce: no violation from NaN energy",
        len(coerced_v) == 0,
    )

    # nan_policy="raise": NaN raises
    raise_engine = SymbolicReasoningEngine(
        config=SymbolicReasoningConfig(
            auto_load=False, nan_policy="raise",
        ),
    )
    raise_engine._load_default_rules()
    try:
        raise_engine.evaluate_rules(
            {"energy": float("nan"), "memory": 0}, step=1,
        )
        _check("raise: NaN raises", False)
    except SymbolicReasoningError:
        _check("raise: NaN raises", True)

    # ---- 6. Invalid metrics contract ----------------------------- #
    print("invalid metrics contract:")
    strict_engine = SymbolicReasoningEngine(
        config=SymbolicReasoningConfig(auto_load=False),
    )
    strict_engine._load_default_rules()
    for bad_metrics in (
        {"cumulative": "not a mapping"},
        {"cumulative": {"step_count": "abc"}},
        {"energy": "not-a-number"},
    ):
        try:
            strict_engine.evaluate_rules(bad_metrics, step=1)
            _check(f"reject {bad_metrics}", False)
        except SymbolicReasoningError:
            _check(f"reject {bad_metrics}", True)

    # ---- 7. from_dict / to_json round-trip ----------------------- #
    print("round-trip:")
    engine2 = SymbolicReasoningEngine(
        config=SymbolicReasoningConfig(auto_load=False),
    )
    engine2._load_default_rules()
    engine2.evaluate_rules({"energy": 10.0, "memory": 0}, step=1)
    engine2.evaluate_rules({"energy": 0.0, "memory": 1000.0}, step=2)
    payload = engine2.to_json(include_history=True)
    restored = SymbolicReasoningEngine.from_json(payload)
    _check(
        "rules preserved",
        len(restored.rules) == len(engine2.rules),
    )
    _check(
        "violations preserved",
        len(restored.violation_history)
        == len(engine2.violation_history),
    )
    _check(
        "counter preserved",
        restored.total_evaluations == engine2.total_evaluations,
    )
    _check(
        "category preserved",
        restored.violation_history[0].category
        == engine2.violation_history[0].category,
    )

    # include_history=False now works (was TypeError before)
    payload2 = engine2.to_json(include_history=False)
    _check(
        "include_history=False: no history key",
        "violation_history" not in json.loads(payload2),
    )

    # ---- 8. Category survives policy reload ---------------------- #
    print("category survives reload:")
    engine3 = SymbolicReasoningEngine(
        config=SymbolicReasoningConfig(auto_load=False),
    )
    engine3.rules.append(SymbolicRule(
        id="CAT-001",
        name="test",
        category="my_category",
        priority="high",
        condition="energy > 5",
        action="alert",
        explanation="test",
    ))
    engine3.evaluate_rules({"energy": 10.0}, step=1)
    # Simulate a policy reload that removes the rule.
    engine3.rules.clear()
    summary = engine3.get_violation_summary()
    _check(
        "old violation retains its category",
        summary["by_category"].get("my_category") == 1,
    )

    # ---- 9. Lifecycle counters ----------------------------------- #
    print("lifecycle counters:")
    lc_engine = SymbolicReasoningEngine(
        config=SymbolicReasoningConfig(auto_load=False),
    )
    lc_engine._load_default_rules()
    for i in range(3):
        lc_engine.evaluate_rules(
            {"energy": 10.0, "memory": 0}, step=i,
        )
    stats = lc_engine.statistics()
    _check(
        "evaluations == 3", stats["evaluations"] == 3,
    )
    _check(
        "total_evaluations == 3", stats["total_evaluations"] == 3,
    )
    lc_engine.reset()
    stats_after = lc_engine.statistics()
    _check(
        "reset: evaluations == 0",
        stats_after["evaluations"] == 0,
    )
    _check(
        "reset: total_evaluations preserved",
        stats_after["total_evaluations"] == 3,
    )
    _check(
        "reset: total_violations preserved",
        stats_after["total_violations"] >= 3,
    )

    # ---- 10. Sorted violations ----------------------------------- #
    print("sorted violations:")
    sort_engine = SymbolicReasoningEngine(
        config=SymbolicReasoningConfig(auto_load=False),
    )
    sort_engine.rules = [
        SymbolicRule(
            id="LOW-1", name="LOW", category="t", priority="low",
            condition="energy > 0", action="a", explanation="e",
        ),
        SymbolicRule(
            id="CRIT-1", name="CRIT", category="t", priority="critical",
            condition="energy > 0", action="a", explanation="e",
        ),
        SymbolicRule(
            id="HIGH-1", name="HIGH", category="t", priority="high",
            condition="energy > 0", action="a", explanation="e",
        ),
    ]
    v_sorted = sort_engine.evaluate_rules({"energy": 10.0}, step=1)
    _check(
        "critical first, high, then low",
        [v.severity for v in v_sorted]
        == ["critical", "high", "low"],
    )

    # ---- 11. Domain rules carry category ------------------------- #
    print("domain rules:")
    dom_engine = SymbolicReasoningEngine(
        config=SymbolicReasoningConfig(auto_load=False),
    )
    dom_engine.domain_rules["medical"] = [
        SymbolicRule(
            id="MED-1", name="Med", category="domain", priority="high",
            condition="energy > 0", action="a", explanation="e",
        ),
    ]
    dom_v = dom_engine.evaluate_rules(
        {"energy": 10.0}, step=1, domain="medical",
    )
    _check(
        "domain violation recorded with category=domain",
        len(dom_v) == 1 and dom_v[0].category == "domain",
    )

    # ---- 12. export_violations ---------------------------------- #
    print("export_violations:")
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "violations.json"
        size = engine2.export_violations(str(path))
        _check("file written", path.exists() and size > 0)
        parsed = json.loads(path.read_text())
        _check(
            "summary in payload", "summary" in parsed,
        )
        _check(
            "violations in payload", "violations" in parsed,
        )

    # ---- 13. Validation of public entry point ------------------ #
    print("evaluate_rules validation:")
    for bad_call in (
        lambda: engine2.evaluate_rules("not-a-mapping", 1),
        lambda: engine2.evaluate_rules({}, -1),
        lambda: engine2.evaluate_rules({}, 1, domain=""),
    ):
        try:
            bad_call()
            _check("bad call rejected", False)
        except SymbolicReasoningError:
            _check("bad call rejected", True)

    # ---- 14. eq / hash ----------------------------------------- #
    print("eq/hash:")
    a = SymbolicReasoningEngine(
        config=SymbolicReasoningConfig(auto_load=False),
    )
    b = SymbolicReasoningEngine(
        config=SymbolicReasoningConfig(auto_load=False),
    )
    _check("equal configs", a == b)
    _check("hash equal", hash(a) == hash(b))

    # ---- 15. dir ---------------------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
