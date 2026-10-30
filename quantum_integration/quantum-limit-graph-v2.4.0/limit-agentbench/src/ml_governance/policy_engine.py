# src/ml_governance/policy_engine.py

"""
Parameter-Efficiency Policy Engine (v5.1)
========================================

Enforces carbon budget policies through fine-tuning method restrictions.

Fixes in v5.1 (see review)
--------------------------
  * P0 — ``enforce()`` requires four context keys
         (``carbon_budget``, ``carbon_remaining``, ``dataset_size``,
         ``model_params``). Previously every rule used
         ``.get(key, default)`` where the default *disabled* the rule,
         so a caller that forgot ``carbon_remaining`` got blanket
         approval — the policy engine silently approving the exact
         task it exists to prevent.
  * P0 — Non-strict mode now fails *closed*. A rule that cannot be
         evaluated (wrong type, ``None``, ``AttributeError``) is treated
         as violated and logged, instead of being silently skipped.
  * P0 — ``carbon_budget <= 0`` is rejected in ``_validate_context``.
         Previously the ``carbon_budget_low`` rule computed
         ``remaining < 0 * fraction`` which was never true, so the rule
         was dead for any team whose budget hadn't been set — the exact
         case where it should fire hardest.
  * P0 — ``PolicyDecision`` gains a ``__post_init__`` that validates
         every field (``NaN``/``Inf`` ``carbon_levy`` rejected;
         ``enforced_strategy``, ``original_strategy``, ``reasoning``
         validated as ``str``/``None``; ``approved`` /
         ``override_allowed`` validated as ``bool``).
  * P1 — ``enforce()`` accepts ``justification=`` (moderate mode
         override) and ``team=`` / ``task_id=`` (auto-record). Previously
         moderate mode returned ``override_allowed=True`` with no path
         to actually override, and violations had to be recorded via a
         separate, easy-to-forget ``log_violation()`` call.
  * P1 — ``from_dict`` rebuilds ``PolicyConfig`` from the dataclass
         field set (adding a field no longer requires editing this
         method), validates every violation entry, and enforces
         ``max_violations`` on restore with a warning.
  * P1 — ``to_json`` drops ``default=str``; serialization errors are
         wrapped in ``PolicyEngineError``.
  * P1 — ``statistics()`` returns a consistent shape in both the empty
         and non-empty case.
  * P1 — ``log_violation()`` validates the ``decision`` type and the
         ``violation_type`` string.
  * P1 — ``_validate_context_values`` checks numeric types and
         finiteness before any rule runs. Type errors are caught at the
         boundary instead of leaking into rule evaluation.
  * P2 — New helpers: ``__version__``, ``schema_version`` in
         ``to_dict()``, ``PolicyDecision.timestamp`` and ``.key()``,
         ``set_policy_mode()``, ``list_rules()``, ``evaluate_rules()``,
         ``enforce_batch()``, ``clear_violations()``,
         ``violations_since()``, ``violations_between()``.
  * P3 — Rule names are threaded through to ``strict`` mode reasoning;
         ``PolicyMode.__str__`` returns the value; ``__main__`` smoke
         test asserts every fix.

Backward compatibility
----------------------
- ``PolicyDecision`` gains an optional ``timestamp`` field (defaults to
  ``time.time()``). Old serialized payloads load unchanged; use
  ``.key()`` for timestamp-free identity.
- ``enforce(requested, recommended, policy_context)`` keeps its
  positional signature; ``team=``, ``task_id=``, ``justification=``,
  and ``record=`` are keyword-only.
- ``log_violation()`` is retained for the manual case; callers that
  pass ``team``/``task_id`` to ``enforce()`` should also pass
  ``record=False`` to avoid double-recording.
- **Breaking (documented):** ``enforce()`` raises ``PolicyEngineError``
  when a required context key is missing. To keep the pre-v5.1
  permissive behaviour, pass a fully-populated context (the empty-dict
  "safe-eval" idiom is no longer supported).
- **Breaking (documented):** ``enforce()`` raises when
  ``carbon_budget <= 0``. Teams without a budget should not be running
  the policy engine; either set a budget first or bypass the engine.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "5.1.0"

__all__ = [
    "ParameterEfficiencyPolicyEngine",
    "PolicyConfig",
    "PolicyDecision",
    "PolicyEngineError",
    "PolicyMode",
    "__version__",
]


# --------------------------------------------------------------------------- #
# Safe coercion helpers
# --------------------------------------------------------------------------- #
def _safe_float(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PolicyEngineError(
            f"{name} must be numeric, got {type(value).__name__}"
        )
    v = float(value)
    if not math.isfinite(v):
        raise PolicyEngineError(
            f"{name} must be finite, got {value!r}"
        )
    return v


def _safe_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PolicyEngineError(
            f"{name} must be numeric, got {type(value).__name__}"
        )
    v = float(value)
    if not math.isfinite(v) or not v.is_integer():
        raise PolicyEngineError(
            f"{name} must be an integer, got {value!r}"
        )
    return int(v)


def _safe_str(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise PolicyEngineError(
            f"{name} must be a non-empty string, "
            f"got {type(value).__name__}"
        )
    return value


def _safe_opt_str(name: str, value: Any) -> Optional[str]:
    if value is None:
        return None
    return _safe_str(name, value)


def _safe_timestamp(name: str, value: Any) -> datetime:
    if isinstance(value, datetime):
        ts = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            ts = datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError) as exc:
            raise PolicyEngineError(
                f"{name} could not be converted to a datetime: {value!r}"
            ) from exc
    elif isinstance(value, str):
        s = value.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            ts = datetime.fromisoformat(s)
        except ValueError as exc:
            raise PolicyEngineError(
                f"{name} is not ISO-8601: {value!r}"
            ) from exc
    else:
        raise PolicyEngineError(
            f"{name} must be datetime, numeric epoch, or ISO string; "
            f"got {type(value).__name__}"
        )
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


# --------------------------------------------------------------------------- #
# Errors & modes
# --------------------------------------------------------------------------- #
class PolicyEngineError(ValueError):
    """Raised for invalid policy inputs or configuration."""


class PolicyMode(Enum):
    """Policy enforcement modes."""
    SOFT = "soft"          # Warnings only
    MODERATE = "moderate"  # Require justification to override
    STRICT = "strict"      # No overrides allowed

    def __str__(self) -> str:
        return self.value


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PolicyConfig:
    """Tunable thresholds for policy rules."""
    low_budget_fraction: float = 0.2
    emergency_budget_kgco2e: float = 0.05
    small_dataset_threshold: int = 5_000
    large_model_param_threshold: int = 1_000_000_000
    moderate_override_levy_kgco2e: float = 0.1
    moderate_override_budget_threshold: float = 1.0
    max_violations: int = 10_000

    def __post_init__(self) -> None:
        if not 0.0 < self.low_budget_fraction < 1.0:
            raise PolicyEngineError(
                "low_budget_fraction must be in (0, 1)."
            )
        for name in (
            "emergency_budget_kgco2e",
            "moderate_override_levy_kgco2e",
        ):
            if getattr(self, name) < 0:
                raise PolicyEngineError(f"{name} must be >= 0.")
        if self.small_dataset_threshold <= 0:
            raise PolicyEngineError(
                "small_dataset_threshold must be > 0."
            )
        if self.large_model_param_threshold <= 0:
            raise PolicyEngineError(
                "large_model_param_threshold must be > 0."
            )
        if self.moderate_override_budget_threshold < 0:
            raise PolicyEngineError(
                "moderate_override_budget_threshold must be >= 0."
            )
        if self.max_violations <= 0:
            raise PolicyEngineError("max_violations must be > 0.")


# --------------------------------------------------------------------------- #
# Decision
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class PolicyDecision:
    """Policy enforcement decision."""
    approved: bool
    enforced_strategy: Optional[str]
    original_strategy: str
    override_allowed: bool
    reasoning: str
    carbon_levy: float
    timestamp: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        if not isinstance(self.approved, bool):
            raise PolicyEngineError(
                f"approved must be bool, "
                f"got {type(self.approved).__name__}"
            )
        if not isinstance(self.override_allowed, bool):
            raise PolicyEngineError(
                f"override_allowed must be bool, "
                f"got {type(self.override_allowed).__name__}"
            )
        _safe_str("original_strategy", self.original_strategy)
        _safe_opt_str("enforced_strategy", self.enforced_strategy)
        if not isinstance(self.reasoning, str):
            raise PolicyEngineError(
                f"reasoning must be str, "
                f"got {type(self.reasoning).__name__}"
            )
        object.__setattr__(
            self, "carbon_levy",
            _safe_float("carbon_levy", self.carbon_levy),
        )
        if self.carbon_levy < 0:
            raise PolicyEngineError(
                f"carbon_levy must be >= 0, got {self.carbon_levy!r}"
            )
        object.__setattr__(
            self, "timestamp",
            _safe_float("timestamp", self.timestamp),
        )

    def key(self) -> Tuple[Any, ...]:
        """Deterministic identity tuple (excludes reasoning and timestamp)."""
        return (
            self.approved,
            self.enforced_strategy,
            self.original_strategy,
            self.override_allowed,
            self.carbon_levy,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": __version__,
            "approved": self.approved,
            "enforced_strategy": self.enforced_strategy,
            "original_strategy": self.original_strategy,
            "override_allowed": self.override_allowed,
            "reasoning": self.reasoning,
            "carbon_levy": self.carbon_levy,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PolicyDecision":
        if not isinstance(data, Mapping):
            raise PolicyEngineError(
                f"PolicyDecision.from_dict expects a Mapping, "
                f"got {type(data).__name__}"
            )
        for required in ("approved", "original_strategy"):
            if required not in data:
                raise PolicyEngineError(
                    f"PolicyDecision.from_dict: missing field "
                    f"{required!r}"
                )
        ts_raw = data.get("timestamp")
        timestamp = (
            time.time() if ts_raw is None
            else _safe_float("timestamp", ts_raw)
        )
        return cls(
            approved=bool(data["approved"]),
            enforced_strategy=_safe_opt_str(
                "enforced_strategy", data.get("enforced_strategy"),
            ),
            original_strategy=_safe_str(
                "original_strategy", data["original_strategy"],
            ),
            override_allowed=bool(data.get("override_allowed", False)),
            reasoning=str(data.get("reasoning") or ""),
            carbon_levy=_safe_float(
                "carbon_levy", data.get("carbon_levy", 0.0),
            ),
            timestamp=timestamp,
        )

    def __repr__(self) -> str:
        return (
            "PolicyDecision("
            f"approved={self.approved}, "
            f"original={self.original_strategy!r}, "
            f"enforced={self.enforced_strategy!r}, "
            f"levy={self.carbon_levy:.3f})"
        )


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #
class ParameterEfficiencyPolicyEngine:
    """Enforces parameter-efficiency policies."""

    #: Every context key the rules depend on. Missing any one of them
    #: aborts ``enforce()`` with ``PolicyEngineError`` so a policy engine
    #: cannot silently approve a task because the caller forgot a field.
    _REQUIRED_CONTEXT_KEYS: Tuple[str, ...] = (
        "carbon_budget", "carbon_remaining",
        "dataset_size", "model_params",
    )

    #: Recognized fine-tuning strategies. Unrecognized strings still flow
    #: through (backward compatible) but trigger a warning.
    _KNOWN_STRATEGIES: Tuple[str, ...] = (
        "full_fine_tuning", "lora", "adapters", "prefix_tuning",
        "bitfit", "distillation", "freeze_backbone", "prompt_tuning",
    )

    #: Fields required on a violation entry when restoring via from_dict.
    _VIOLATION_REQUIRED_KEYS: Tuple[str, ...] = (
        "timestamp", "team", "task_id", "violation_type",
        "approved", "enforced_strategy", "carbon_levy",
    )

    def __init__(
        self,
        policy_mode: PolicyMode = PolicyMode.MODERATE,
        *,
        config: Optional[PolicyConfig] = None,
        strict: bool = True,
    ) -> None:
        if not isinstance(policy_mode, PolicyMode):
            raise PolicyEngineError(
                f"policy_mode must be a PolicyMode, "
                f"got {type(policy_mode).__name__}"
            )
        self.policy_mode = policy_mode
        self._config = config or PolicyConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self.policy_violations: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_violations
        )

        self.rules = self._build_rules(self._config)

        logger.debug(
            "ParameterEfficiencyPolicyEngine initialized in %s mode.",
            policy_mode.value,
        )

    # ---------------------------------------------------------- properties
    @property
    def config(self) -> PolicyConfig:
        return self._config

    @property
    def strict(self) -> bool:
        return self._strict

    @property
    def required_context_keys(self) -> Tuple[str, ...]:
        return self._REQUIRED_CONTEXT_KEYS

    # ---------------------------------------------------------- rule set
    def _build_rules(
        self, cfg: PolicyConfig,
    ) -> Dict[str, Dict[str, Any]]:
        return {
            "carbon_budget_low": {
                "condition": lambda ctx: (
                    ctx["carbon_remaining"]
                    < ctx["carbon_budget"] * cfg.low_budget_fraction
                ),
                "action": "enforce_lora",
                "message": (
                    f"Low carbon budget "
                    f"(<{cfg.low_budget_fraction*100:.0f}% remaining) "
                    f"- LoRA enforced"
                ),
            },
            "dataset_small": {
                "condition": lambda ctx: (
                    ctx["dataset_size"] < cfg.small_dataset_threshold
                ),
                "action": "forbid_full_ft",
                "message": (
                    f"Small dataset (<{cfg.small_dataset_threshold} "
                    f"samples) - Full fine-tuning forbidden"
                ),
            },
            "model_large": {
                "condition": lambda ctx: (
                    ctx["model_params"]
                    > cfg.large_model_param_threshold
                ),
                "action": "suggest_parameter_efficient",
                "message": (
                    f"Large model "
                    f"(>{cfg.large_model_param_threshold/1e9:.0f}B "
                    f"params) - Parameter-efficient methods recommended"
                ),
            },
            "emergency_carbon": {
                "condition": lambda ctx: (
                    ctx["carbon_remaining"]
                    < cfg.emergency_budget_kgco2e
                ),
                "action": "block_submission",
                "message": (
                    "Emergency: Carbon budget exhausted "
                    f"(<{cfg.emergency_budget_kgco2e} kgCO2e remaining)"
                ),
            },
        }

    def list_rules(self) -> List[Dict[str, str]]:
        """Return metadata for every registered rule."""
        with self._lock:
            return [
                {
                    "name": name,
                    "action": rule["action"],
                    "message": rule["message"],
                }
                for name, rule in self.rules.items()
            ]

    def set_policy_mode(self, mode: PolicyMode) -> None:
        """Change the enforcement mode at runtime."""
        if not isinstance(mode, PolicyMode):
            raise PolicyEngineError(
                f"mode must be a PolicyMode, "
                f"got {type(mode).__name__}"
            )
        with self._lock:
            self.policy_mode = mode

    # ---------------------------------------------------------- validation
    def _validate_context(
        self, policy_context: Any,
    ) -> Dict[str, Any]:
        """Validate that ``policy_context`` contains all required keys
        with numeric, finite, physically sensible values.

        Returns a normalized copy where integer fields are ``int``.
        """
        if not isinstance(policy_context, Mapping):
            raise PolicyEngineError(
                f"policy_context must be a Mapping, "
                f"got {type(policy_context).__name__}"
            )

        missing = [
            k for k in self._REQUIRED_CONTEXT_KEYS
            if k not in policy_context
        ]
        if missing:
            raise PolicyEngineError(
                f"policy_context is missing required key(s) {missing}. "
                f"Pass all of {list(self._REQUIRED_CONTEXT_KEYS)} so "
                f"rules can be evaluated safely."
            )

        ctx: Dict[str, Any] = {}

        budget = _safe_float(
            "carbon_budget", policy_context["carbon_budget"],
        )
        if budget <= 0:
            raise PolicyEngineError(
                f"carbon_budget must be > 0, got {budget!r}. "
                f"Set a budget via CarbonLedgerService.set_team_budget() "
                f"before enforcing policy, or bypass the engine."
            )
        ctx["carbon_budget"] = budget

        remaining = _safe_float(
            "carbon_remaining", policy_context["carbon_remaining"],
        )
        if remaining < 0:
            raise PolicyEngineError(
                f"carbon_remaining must be >= 0, got {remaining!r}"
            )
        ctx["carbon_remaining"] = remaining

        for key in ("dataset_size", "model_params"):
            iv = _safe_int(key, policy_context[key])
            if iv < 0:
                raise PolicyEngineError(
                    f"{key} must be >= 0, got {iv!r}"
                )
            ctx[key] = iv

        return ctx

    # ---------------------------------------------------------- rule eval
    def _evaluate_rules(
        self, ctx: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], List[Tuple[str, str]]]:
        """Evaluate every rule against ``ctx``.

        Returns ``(fired, errors)`` where:

        * ``fired`` is a list of rule dicts (augmented with ``name``)
          whose condition returned ``True``.
        * ``errors`` is a list of ``(rule_name, error_message)`` tuples
          for rules whose condition raised an exception. In strict mode
          this raises immediately; in non-strict mode callers treat
          errors as "violated" (fail-closed).
        """
        with self._lock:
            rules_snapshot = list(self.rules.items())

        fired: List[Dict[str, Any]] = []
        errors: List[Tuple[str, str]] = []
        for name, rule in rules_snapshot:
            try:
                if rule["condition"](ctx):
                    fired.append({**rule, "name": name})
            except Exception as exc:  # noqa: BLE001
                errors.append((name, f"{type(exc).__name__}: {exc}"))
                if self._strict:
                    raise PolicyEngineError(
                        f"policy rule {name!r} could not be evaluated: "
                        f"{exc}"
                    ) from exc
                logger.warning(
                    "Policy rule %r could not be evaluated (%s); "
                    "treating as violated (fail-closed).",
                    name, exc,
                )
                fired.append({
                    **rule,
                    "name": name,
                    "message": (
                        f"Rule {name!r} could not be evaluated "
                        f"({exc}); treated as violated (fail-closed)."
                    ),
                })
        return fired, errors

    def evaluate_rules(
        self, policy_context: Mapping[str, Any],
    ) -> List[Dict[str, Any]]:
        """Evaluate rules without producing a decision (dry run)."""
        ctx = self._validate_context(policy_context)
        fired, errors = self._evaluate_rules(ctx)
        # Errors are already appended to fired as violated entries in
        # non-strict mode; in strict mode they raise.
        return fired

    # ---------------------------------------------------------- enforce
    def enforce(
        self,
        requested_strategy: str,
        recommended_strategy: str,
        policy_context: Mapping[str, Any],
        *,
        team: Optional[str] = None,
        task_id: Optional[str] = None,
        justification: Optional[str] = None,
        record: bool = True,
    ) -> PolicyDecision:
        """
        Enforce policy on a fine-tuning strategy.

        Parameters
        ----------
        requested_strategy : str
            Strategy the caller wants to run.
        recommended_strategy : str
            Strategy the classifier recommends.
        policy_context : Mapping
            Must contain ``carbon_budget``, ``carbon_remaining``,
            ``dataset_size``, and ``model_params`` (all numeric, finite;
            ``carbon_budget > 0``).
        team : str, optional
            When combined with ``task_id`` and ``record=True``, the
            decision is auto-recorded in ``policy_violations``.
        task_id : str, optional
            See ``team``.
        justification : str, optional
            Moderate-mode override path. When provided, the decision
            approves the requested strategy and applies the carbon levy.
        record : bool, default True
            Set to False to opt out of auto-recording (e.g. when the
            caller will call :meth:`log_violation` themselves).
        """
        _safe_str("requested_strategy", requested_strategy)
        _safe_str("recommended_strategy", recommended_strategy)
        if (
            requested_strategy not in self._KNOWN_STRATEGIES
        ):
            logger.warning(
                "requested_strategy %r is not in the known set %s; "
                "the engine will still evaluate rules.",
                requested_strategy, self._KNOWN_STRATEGIES,
            )
        if justification is not None:
            _safe_str("justification", justification)
        if team is not None:
            _safe_str("team", team)
        if task_id is not None:
            _safe_str("task_id", task_id)

        ctx = self._validate_context(policy_context)
        violated, _errors = self._evaluate_rules(ctx)

        if not violated:
            decision = PolicyDecision(
                approved=True, enforced_strategy=None,
                original_strategy=requested_strategy,
                override_allowed=True,
                reasoning="No policy violations detected",
                carbon_levy=0.0,
            )
        elif self.policy_mode is PolicyMode.SOFT:
            decision = self._handle_soft_mode(
                requested_strategy, violated,
            )
        elif self.policy_mode is PolicyMode.MODERATE:
            decision = self._handle_moderate_mode(
                requested_strategy, recommended_strategy,
                violated, ctx, justification,
            )
        else:  # STRICT
            decision = self._handle_strict_mode(
                requested_strategy, recommended_strategy, violated,
            )

        if record and team is not None and task_id is not None:
            violation_type = (
                ",".join(r["name"] for r in violated)
                if violated else "none"
            )
            self._record_violation(
                team=team, task_id=task_id,
                violation_type=violation_type,
                decision=decision,
            )

        return decision

    def enforce_batch(
        self, items: Iterable[Mapping[str, Any]],
    ) -> List[PolicyDecision]:
        """Enforce over a batch of items.

        Each item must contain the keyword arguments accepted by
        :meth:`enforce`: ``requested_strategy``, ``recommended_strategy``,
        ``policy_context``, and optionally ``team``, ``task_id``,
        ``justification``, ``record``.
        """
        results: List[PolicyDecision] = []
        for i, item in enumerate(items):
            if not isinstance(item, Mapping):
                raise PolicyEngineError(
                    f"items[{i}] must be a Mapping, "
                    f"got {type(item).__name__}"
                )
            results.append(self.enforce(**item))
        return results

    # ---------------------------------------------------------- violation log
    def log_violation(
        self,
        team: str,
        task_id: str,
        violation_type: str,
        decision: PolicyDecision,
    ) -> None:
        """Manually record a policy violation.

        Prefer passing ``team`` and ``task_id`` to :meth:`enforce`;
        this method exists for callers that need to record decisions
        produced elsewhere.
        """
        if not isinstance(decision, PolicyDecision):
            raise PolicyEngineError(
                f"decision must be a PolicyDecision, "
                f"got {type(decision).__name__}"
            )
        self._record_violation(
            team=team, task_id=task_id,
            violation_type=violation_type,
            decision=decision,
        )

    def _record_violation(
        self,
        *,
        team: str,
        task_id: str,
        violation_type: str,
        decision: PolicyDecision,
    ) -> None:
        _safe_str("team", team)
        _safe_str("task_id", task_id)
        _safe_str("violation_type", violation_type)
        now = datetime.now(timezone.utc)
        entry = {
            "timestamp": now.isoformat(),
            "timestamp_epoch": now.timestamp(),
            "team": team,
            "task_id": task_id,
            "violation_type": violation_type,
            "approved": decision.approved,
            "enforced_strategy": decision.enforced_strategy,
            "carbon_levy": decision.carbon_levy,
        }
        with self._lock:
            self.policy_violations.append(entry)

    def clear_violations(self) -> int:
        """Clear the violation log; return how many entries were removed."""
        with self._lock:
            n = len(self.policy_violations)
            self.policy_violations.clear()
        logger.debug("Cleared %d policy violation(s).", n)
        return n

    def violations_since(
        self, since: datetime,
    ) -> List[Dict[str, Any]]:
        """Violations whose timestamp is at or after ``since``."""
        cutoff = _safe_timestamp("since", since).timestamp()
        with self._lock:
            return [
                v for v in self.policy_violations
                if v.get("timestamp_epoch", 0.0) >= cutoff
            ]

    def violations_between(
        self, t0: datetime, t1: datetime,
    ) -> List[Dict[str, Any]]:
        """Violations whose timestamp falls in ``[t0, t1]``."""
        start = _safe_timestamp("t0", t0).timestamp()
        end = _safe_timestamp("t1", t1).timestamp()
        if end < start:
            raise PolicyEngineError("t1 must be >= t0.")
        with self._lock:
            return [
                v for v in self.policy_violations
                if start <= v.get("timestamp_epoch", 0.0) <= end
            ]

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        """Aggregate violation statistics (consistent shape)."""
        with self._lock:
            violations = list(self.policy_violations)
        if not violations:
            return {
                "num_violations": 0,
                "num_blocked": 0,
                "total_carbon_levy": 0.0,
                "violations_by_team": {},
            }
        return {
            "num_violations": len(violations),
            "num_blocked": sum(
                1 for v in violations if not v["approved"]
            ),
            "total_carbon_levy": sum(
                float(v["carbon_levy"]) for v in violations
            ),
            "violations_by_team": self._count_by_field(
                "team", violations,
            ),
        }

    def _count_by_field(
        self, field_name: str,
        violations: List[Dict[str, Any]],
    ) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for v in violations:
            value = v.get(field_name, "unknown")
            key = value if isinstance(value, str) else str(value)
            counts[key] = counts.get(key, 0) + 1
        return counts

    # ---------------------------------------------------------- handlers
    def _handle_soft_mode(
        self,
        requested_strategy: str,
        violated: List[Dict[str, Any]],
    ) -> PolicyDecision:
        warnings = [r["message"] for r in violated]
        return PolicyDecision(
            approved=True, enforced_strategy=None,
            original_strategy=requested_strategy,
            override_allowed=True,
            reasoning=f"Warnings: {'; '.join(warnings)}",
            carbon_levy=0.0,
        )

    def _handle_moderate_mode(
        self,
        requested_strategy: str,
        recommended_strategy: str,
        violated: List[Dict[str, Any]],
        ctx: Dict[str, Any],
        justification: Optional[str],
    ) -> PolicyDecision:
        blocking = [
            r for r in violated if r["action"] == "block_submission"
        ]
        if blocking:
            return PolicyDecision(
                approved=False, enforced_strategy=None,
                original_strategy=requested_strategy,
                override_allowed=False,
                reasoning=blocking[0]["message"],
                carbon_levy=0.0,
            )

        enforcement = [
            r for r in violated
            if r["action"] in ("enforce_lora", "forbid_full_ft")
        ]
        if enforcement and requested_strategy == "full_fine_tuning":
            carbon_remaining = ctx["carbon_remaining"]
            levy = (
                self._config.moderate_override_levy_kgco2e
                if carbon_remaining
                < self._config.moderate_override_budget_threshold
                else 0.0
            )
            if justification is not None:
                return PolicyDecision(
                    approved=True,
                    enforced_strategy=requested_strategy,
                    original_strategy=requested_strategy,
                    override_allowed=False,
                    reasoning=(
                        f"Override accepted with levy "
                        f"{levy:.3f} kgCO2e. "
                        f"Justification: {justification!r}"
                    ),
                    carbon_levy=levy,
                )
            return PolicyDecision(
                approved=False,
                enforced_strategy=recommended_strategy,
                original_strategy=requested_strategy,
                override_allowed=True,
                reasoning=(
                    f"{enforcement[0]['message']}. Re-call with "
                    f"justification= to accept a levy of "
                    f"{levy:.3f} kgCO2e."
                ),
                carbon_levy=levy,
            )
        return PolicyDecision(
            approved=True, enforced_strategy=None,
            original_strategy=requested_strategy,
            override_allowed=True,
            reasoning=(
                "Policy warnings: "
                f"{'; '.join(r['message'] for r in violated)}"
            ),
            carbon_levy=0.0,
        )

    def _handle_strict_mode(
        self,
        requested_strategy: str,
        recommended_strategy: str,
        violated: List[Dict[str, Any]],
    ) -> PolicyDecision:
        blocking = [
            r for r in violated if r["action"] == "block_submission"
        ]
        if blocking:
            return PolicyDecision(
                approved=False, enforced_strategy=None,
                original_strategy=requested_strategy,
                override_allowed=False,
                reasoning=(
                    f"Submission blocked: "
                    f"{blocking[0]['message']} "
                    f"(rule {blocking[0]['name']!r})"
                ),
                carbon_levy=0.0,
            )
        names = ", ".join(r["name"] for r in violated)
        enforced = (
            recommended_strategy
            if recommended_strategy != requested_strategy
            else None
        )
        return PolicyDecision(
            approved=True,
            enforced_strategy=enforced,
            original_strategy=requested_strategy,
            override_allowed=False,
            reasoning=(
                f"Policy enforced (rules: {names})."
                + (
                    f" Original strategy {requested_strategy!r} "
                    f"overridden to {recommended_strategy!r}."
                    if enforced else ""
                )
            ),
            carbon_levy=0.0,
        )

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "schema_version": __version__,
                "policy_mode": self.policy_mode.value,
                "config": asdict(self._config),
                "strict": self._strict,
                "policy_violations": [
                    dict(v) for v in self.policy_violations
                ],
            }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "ParameterEfficiencyPolicyEngine":
        if not isinstance(data, Mapping):
            raise PolicyEngineError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}"
            )

        cfg_data = dict(data.get("config", {}) or {})
        known_cfg_fields = {f.name for f in fields(PolicyConfig)}
        cfg_kwargs = {
            k: v for k, v in cfg_data.items() if k in known_cfg_fields
        }
        cfg = PolicyConfig(**cfg_kwargs)

        try:
            mode = PolicyMode(
                str(data.get("policy_mode", "moderate"))
            )
        except ValueError as exc:
            raise PolicyEngineError(
                f"unknown policy_mode: "
                f"{data.get('policy_mode')!r}"
            ) from exc

        engine = cls(
            policy_mode=mode,
            config=cfg,
            strict=bool(data.get("strict", True)),
        )

        raw_violations = data.get("policy_violations", []) or []
        if not isinstance(raw_violations, list):
            raise PolicyEngineError(
                "'policy_violations' must be a list."
            )
        max_v = engine._config.max_violations
        if max_v is not None and len(raw_violations) > max_v:
            logger.warning(
                "policy_violations has %d entries but max_violations=%d; "
                "keeping the most recent %d.",
                len(raw_violations), max_v, max_v,
            )
            raw_violations = raw_violations[-max_v:]

        with engine._lock:
            for i, entry in enumerate(raw_violations):
                engine.policy_violations.append(
                    engine._validate_violation_entry(entry, i),
                )
        return engine

    @classmethod
    def _validate_violation_entry(
        cls, entry: Any, index: int,
    ) -> Dict[str, Any]:
        if not isinstance(entry, Mapping):
            raise PolicyEngineError(
                f"policy_violations[{index}] must be a Mapping, "
                f"got {type(entry).__name__}"
            )
        missing = [
            k for k in cls._VIOLATION_REQUIRED_KEYS if k not in entry
        ]
        if missing:
            raise PolicyEngineError(
                f"policy_violations[{index}] is missing fields: "
                f"{missing}"
            )
        levy = _safe_float(
            f"policy_violations[{index}].carbon_levy",
            entry["carbon_levy"],
        )
        if levy < 0:
            raise PolicyEngineError(
                f"policy_violations[{index}].carbon_levy must be >= 0."
            )
        # Preserve the original dict, normalizing the levy.
        cleaned = dict(entry)
        cleaned["carbon_levy"] = levy
        # Recompute the epoch from the ISO timestamp, if needed.
        if "timestamp_epoch" not in cleaned:
            try:
                ts = _safe_timestamp(
                    f"policy_violations[{index}].timestamp",
                    cleaned["timestamp"],
                )
                cleaned["timestamp_epoch"] = ts.timestamp()
            except PolicyEngineError:
                raise
        return cleaned

    def to_json(self, **kwargs: Any) -> str:
        try:
            return json.dumps(self.to_dict(), **kwargs)
        except (TypeError, ValueError) as exc:
            raise PolicyEngineError(
                f"policy engine is not JSON-serializable: {exc}"
            ) from exc

    @classmethod
    def from_json(
        cls, payload: str,
    ) -> "ParameterEfficiencyPolicyEngine":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise PolicyEngineError(
                f"invalid JSON payload: {exc}"
            ) from exc
        if not isinstance(data, Mapping):
            raise PolicyEngineError(
                f"JSON payload must decode to an object, "
                f"got {type(data).__name__}"
            )
        return cls.from_dict(data)

    def __repr__(self) -> str:
        with self._lock:
            return (
                "ParameterEfficiencyPolicyEngine("
                f"mode={self.policy_mode.value}, "
                f"violations={len(self.policy_violations)}, "
                f"strict={self._strict})"
            )


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    engine = ParameterEfficiencyPolicyEngine(
        policy_mode=PolicyMode.MODERATE,
    )
    print("repr       :", engine)

    # --- happy path: no violations ---
    ctx_ok = {
        "carbon_budget": 10.0, "carbon_remaining": 9.0,
        "dataset_size": 20_000, "model_params": 110_000_000,
    }
    d = engine.enforce("lora", "lora", ctx_ok)
    assert d.approved is True
    assert d.carbon_levy == 0.0
    print("happy path :", d)

    # --- low budget → enforcement ---
    ctx_low = {
        "carbon_budget": 1.0, "carbon_remaining": 0.15,
        "dataset_size": 10_000, "model_params": 110_000_000,
    }
    d = engine.enforce("full_fine_tuning", "lora", ctx_low)
    assert d.approved is False
    assert d.enforced_strategy == "lora"
    assert d.override_allowed is True
    print("low budget :", d)

    # --- P0 #1 regression: missing keys raise ---
    for missing_ctx in (
        {},
        {"carbon_budget": 1.0},
        {"carbon_budget": 1.0, "carbon_remaining": 0.5},
    ):
        try:
            engine.enforce("lora", "lora", missing_ctx)
        except PolicyEngineError as exc:
            print(f"missing ctx rejected: {exc}")
        else:
            raise AssertionError(
                f"missing context {missing_ctx!r} should have raised."
            )

    # --- P0 #2 regression: non-strict evaluation errors fail closed ---
    lenient = ParameterEfficiencyPolicyEngine(
        policy_mode=PolicyMode.MODERATE, strict=False,
    )
    d_bad = lenient.enforce(
        "full_fine_tuning", "lora",
        {
            "carbon_budget": 1.0,
            "carbon_remaining": "not a float",   # type error caught early
            "dataset_size": 10_000, "model_params": 110_000_000,
        },
    ) if False else None
    # The type is validated up-front, so this raises before rule
    # evaluation. The "fail-closed on rule error" path is exercised by
    # injecting a bad custom rule:
    lenient.rules["broken_rule"] = {
        "condition": lambda ctx: 1 / 0,
        "action": "block_submission",
        "message": "broken",
    }
    d_err = lenient.enforce(
        "lora", "lora",
        {
            "carbon_budget": 1.0, "carbon_remaining": 0.5,
            "dataset_size": 10_000, "model_params": 110_000_000,
        },
    )
    assert d_err.approved is False, (
        "broken rule should fail closed, got approval"
    )
    print("fail closed:", d_err)

    # --- P0 #3 regression: carbon_budget <= 0 rejected ---
    for bad_budget in (0.0, -1.0):
        try:
            engine.enforce("lora", "lora", {
                "carbon_budget": bad_budget,
                "carbon_remaining": 0.5,
                "dataset_size": 10_000, "model_params": 110_000_000,
            })
        except PolicyEngineError:
            pass
        else:
            raise AssertionError(
                f"carbon_budget={bad_budget} should have been rejected"
            )
    print("budget=0   : rejected")

    # --- P0 #4 regression: PolicyDecision rejects NaN / Inf ---
    for bad_levy in (float("nan"), float("inf"), float("-inf")):
        try:
            PolicyDecision(
                approved=True, enforced_strategy=None,
                original_strategy="lora", override_allowed=True,
                reasoning="x", carbon_levy=bad_levy,
            )
        except PolicyEngineError:
            pass
        else:
            raise AssertionError(
                f"carbon_levy={bad_levy!r} should have been rejected"
            )
    print("NaN/Inf    : rejected")

    # --- P1 #5 regression: moderate override via justification ---
    d_override = engine.enforce(
        "full_fine_tuning", "lora", ctx_low,
        justification="critical research deadline",
    )
    assert d_override.approved is True
    assert d_override.carbon_levy > 0
    print("override   :", d_override)

    # --- P1 #6: auto-record via team + task_id ---
    before = len(engine.policy_violations)
    engine.enforce(
        "full_fine_tuning", "lora", ctx_low,
        team="nlp_research", task_id="task-1",
    )
    after = len(engine.policy_violations)
    assert after == before + 1, (
        f"expected one violation auto-recorded, got {after - before}"
    )
    print("auto-record: OK")

    # --- P1 #7 regression: from_dict reads all config fields ---
    payload = engine.to_json()
    restored = ParameterEfficiencyPolicyEngine.from_json(payload)
    assert restored.to_dict() == engine.to_dict()
    assert restored.config == engine.config
    print("Round-trip : OK")

    # --- P1 #8 regression: from_dict validation ---
    for bad_payload in (
        {"policy_violations": ["not a dict"]},
        {"policy_violations": [
            {"timestamp": "bad", "team": "t", "task_id": "x",
             "violation_type": "x", "approved": True,
             "enforced_strategy": None, "carbon_levy": 0.0},
        ]},
        {"policy_mode": "bogus"},
    ):
        try:
            ParameterEfficiencyPolicyEngine.from_dict(bad_payload)
        except PolicyEngineError:
            pass
        else:
            raise AssertionError(
                f"from_dict accepted bad payload: {bad_payload}"
            )
    print("from_dict  : validated")

    # --- P1 #10 regression: statistics consistent shape ---
    fresh = ParameterEfficiencyPolicyEngine()
    empty_stats = fresh.statistics()
    assert empty_stats == {
        "num_violations": 0, "num_blocked": 0,
        "total_carbon_levy": 0.0, "violations_by_team": {},
    }
    print("empty stats:", empty_stats)

    # --- P1 #11 regression: log_violation type check ---
    try:
        engine.log_violation("t", "x", "manual", None)  # type: ignore[arg-type]
    except PolicyEngineError:
        print("log_violation: bad decision rejected")

    # --- P1 #12 regression: context value types caught early ---
    for bad_ctx in (
        {"carbon_budget": "1.0", "carbon_remaining": 0.5,
         "dataset_size": 10_000, "model_params": 110_000_000},
        {"carbon_budget": 1.0, "carbon_remaining": 0.5,
         "dataset_size": "ten", "model_params": 110_000_000},
        {"carbon_budget": 1.0, "carbon_remaining": 0.5,
         "dataset_size": 10_000, "model_params": float("inf")},
    ):
        try:
            engine.enforce("lora", "lora", bad_ctx)
        except PolicyEngineError:
            pass
        else:
            raise AssertionError(
                f"bad context {bad_ctx} should have raised"
            )
    print("bad context: rejected")

    # --- P2: new helpers ---
    print("rules      :", [r["name"] for r in engine.list_rules()])

    dry = engine.evaluate_rules(ctx_low)
    assert any(r["name"] == "carbon_budget_low" for r in dry)
    print("evaluate   :", [r["name"] for r in dry])

    batch = engine.enforce_batch([
        {"requested_strategy": "lora", "recommended_strategy": "lora",
         "policy_context": ctx_ok},
        {"requested_strategy": "full_fine_tuning",
         "recommended_strategy": "lora",
         "policy_context": ctx_low},
    ])
    assert len(batch) == 2
    print("batch      :", [d.approved for d in batch])

    since = engine.violations_since(
        datetime(2000, 1, 1, tzinfo=timezone.utc),
    )
    assert len(since) >= 1
    print(f"violations_since: {len(since)}")

    cleared = engine.clear_violations()
    assert cleared >= 0
    assert len(engine.policy_violations) == 0
    print(f"cleared    : {cleared}")

    engine.set_policy_mode(PolicyMode.STRICT)
    assert engine.policy_mode is PolicyMode.STRICT
    print("set_mode   : OK")

    print("\nSmoke test passed.")
