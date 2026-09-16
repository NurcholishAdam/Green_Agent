# src/ml_governance/policy_engine.py

"""
Parameter-Efficiency Policy Engine
==================================

Enforces carbon budget policies through fine-tuning method restrictions.

Enhancements
------------
- ``PolicyConfig`` — frozen, validated, centralizes rule thresholds.
- Safe rule evaluation via ``.get()`` (no more ``KeyError`` on missing keys).
- ``RLock``-guarded bounded ``policy_violations``.
- UTC timestamps in violation logs.
- Serialization on ``PolicyDecision``.
- Validation of every ``enforce()`` input; strict / non-strict modes.
- Custom ``PolicyEngineError``; ``__repr__``; enhanced ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import threading
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


class PolicyEngineError(ValueError):
    """Raised for invalid policy inputs or configuration."""


class PolicyMode(Enum):
    """Policy enforcement modes."""
    SOFT = "soft"          # Warnings only
    MODERATE = "moderate"  # Require justification to override
    STRICT = "strict"      # No overrides allowed


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
            raise PolicyEngineError("small_dataset_threshold must be > 0.")
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


@dataclass(frozen=True)
class PolicyDecision:
    """Policy enforcement decision."""
    approved: bool
    enforced_strategy: Optional[str]
    original_strategy: str
    override_allowed: bool
    reasoning: str
    carbon_levy: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PolicyDecision":
        if not isinstance(data, Mapping):
            raise PolicyEngineError("from_dict expects a Mapping.")
        return cls(
            approved=bool(data["approved"]),
            enforced_strategy=data.get("enforced_strategy"),
            original_strategy=str(data["original_strategy"]),
            override_allowed=bool(data.get("override_allowed", False)),
            reasoning=str(data.get("reasoning", "")),
            carbon_levy=float(data.get("carbon_levy", 0.0)),
        )

    def __repr__(self) -> str:
        return (
            "PolicyDecision("
            f"approved={self.approved}, "
            f"original={self.original_strategy!r}, "
            f"enforced={self.enforced_strategy!r}, "
            f"levy={self.carbon_levy:.3f})"
        )


class ParameterEfficiencyPolicyEngine:
    """Enforces parameter-efficiency policies."""

    def __init__(
        self,
        policy_mode: PolicyMode = PolicyMode.MODERATE,
        *,
        config: Optional[PolicyConfig] = None,
        strict: bool = True,
    ) -> None:
        if not isinstance(policy_mode, PolicyMode):
            raise PolicyEngineError(
                f"policy_mode must be a PolicyMode, got {type(policy_mode).__name__}."
            )
        self.policy_mode = policy_mode
        self._config = config or PolicyConfig()
        self._strict = bool(strict)

        self._lock = threading.RLock()
        self.policy_violations: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_violations
        )

        cfg = self._config
        self.rules: Dict[str, Dict[str, Any]] = {
            "carbon_budget_low": {
                "condition": lambda ctx: (
                    ctx.get("carbon_remaining", float("inf"))
                    < ctx.get("carbon_budget", 0.0) * cfg.low_budget_fraction
                ),
                "action": "enforce_lora",
                "message": (
                    f"Low carbon budget (<{cfg.low_budget_fraction*100:.0f}% "
                    "remaining) - LoRA enforced"
                ),
            },
            "dataset_small": {
                "condition": lambda ctx: (
                    ctx.get("dataset_size", float("inf"))
                    < cfg.small_dataset_threshold
                ),
                "action": "forbid_full_ft",
                "message": (
                    f"Small dataset (<{cfg.small_dataset_threshold} samples) "
                    "- Full fine-tuning forbidden"
                ),
            },
            "model_large": {
                "condition": lambda ctx: (
                    ctx.get("model_params", 0) > cfg.large_model_param_threshold
                ),
                "action": "suggest_parameter_efficient",
                "message": (
                    f"Large model (>{cfg.large_model_param_threshold/1e9:.0f}B "
                    "params) - Parameter-efficient methods recommended"
                ),
            },
            "emergency_carbon": {
                "condition": lambda ctx: (
                    ctx.get("carbon_remaining", float("inf"))
                    < cfg.emergency_budget_kgco2e
                ),
                "action": "block_submission",
                "message": (
                    "Emergency: Carbon budget exhausted "
                    f"(<{cfg.emergency_budget_kgco2e} kgCO2e remaining)"
                ),
            },
        }

        logger.debug(
            "ParameterEfficiencyPolicyEngine initialized in %s mode.",
            policy_mode.value,
        )

    # ---------------------------------------------------------- public API
    def enforce(
        self,
        requested_strategy: str,
        recommended_strategy: str,
        policy_context: Mapping[str, Any],
    ) -> PolicyDecision:
        """
        Enforce policy on fine-tuning strategy.

        See original docstring for parameter meanings; all inputs validated.
        """
        if not isinstance(requested_strategy, str) or not requested_strategy:
            raise PolicyEngineError(
                "requested_strategy must be a non-empty string."
            )
        if not isinstance(recommended_strategy, str) or not recommended_strategy:
            raise PolicyEngineError(
                "recommended_strategy must be a non-empty string."
            )
        if not isinstance(policy_context, Mapping):
            raise PolicyEngineError(
                f"policy_context must be a Mapping, got "
                f"{type(policy_context).__name__}."
            )

        ctx = dict(policy_context)
        violated_rules: List[Dict[str, Any]] = []
        for rule in self.rules.values():
            try:
                if rule["condition"](ctx):
                    violated_rules.append(rule)
            except Exception as exc:
                logger.warning("Policy rule evaluation failed: %s", exc)
                if self._strict:
                    raise PolicyEngineError(
                        f"Policy rule evaluation failed: {exc}"
                    ) from exc

        if not violated_rules:
            return PolicyDecision(
                approved=True, enforced_strategy=None,
                original_strategy=requested_strategy,
                override_allowed=True,
                reasoning="No policy violations detected",
                carbon_levy=0.0,
            )

        if self.policy_mode == PolicyMode.SOFT:
            return self._handle_soft_mode(requested_strategy, violated_rules)
        if self.policy_mode == PolicyMode.MODERATE:
            return self._handle_moderate_mode(
                requested_strategy, recommended_strategy,
                violated_rules, ctx,
            )
        return self._handle_strict_mode(
            requested_strategy, recommended_strategy, violated_rules
        )

    def log_violation(
        self,
        team: str,
        task_id: str,
        violation_type: str,
        decision: PolicyDecision,
    ) -> None:
        """Log a policy violation."""
        if not isinstance(team, str) or not team:
            raise PolicyEngineError("team must be a non-empty string.")
        if not isinstance(task_id, str) or not task_id:
            raise PolicyEngineError("task_id must be a non-empty string.")
        violation = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "team": team,
            "task_id": task_id,
            "violation_type": violation_type,
            "approved": decision.approved,
            "enforced_strategy": decision.enforced_strategy,
            "carbon_levy": decision.carbon_levy,
        }
        with self._lock:
            self.policy_violations.append(violation)

    def statistics(self) -> Dict[str, Any]:
        """Get policy statistics."""
        with self._lock:
            violations = list(self.policy_violations)
        if not violations:
            return {"num_violations": 0}
        return {
            "num_violations": len(violations),
            "num_blocked": sum(1 for v in violations if not v["approved"]),
            "total_carbon_levy": sum(v["carbon_levy"] for v in violations),
            "violations_by_team": self._count_by_field("team", violations),
        }

    def _count_by_field(
        self, field_name: str, violations: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for v in violations:
            value = v.get(field_name, "unknown")
            counts[value] = counts.get(value, 0) + 1
        return counts

    # ---------------------------------------------------------- handlers
    def _handle_soft_mode(
        self, requested_strategy: str, violated_rules: List[Dict[str, Any]]
    ) -> PolicyDecision:
        warnings = [r["message"] for r in violated_rules]
        return PolicyDecision(
            approved=True, enforced_strategy=None,
            original_strategy=requested_strategy, override_allowed=True,
            reasoning=f"Warnings: {'; '.join(warnings)}", carbon_levy=0.0,
        )

    def _handle_moderate_mode(
        self,
        requested_strategy: str,
        recommended_strategy: str,
        violated_rules: List[Dict[str, Any]],
        policy_context: Mapping[str, Any],
    ) -> PolicyDecision:
        blocking = [r for r in violated_rules if r["action"] == "block_submission"]
        if blocking:
            return PolicyDecision(
                approved=False, enforced_strategy=None,
                original_strategy=requested_strategy, override_allowed=False,
                reasoning=blocking[0]["message"], carbon_levy=0.0,
            )

        enforcement = [
            r for r in violated_rules
            if r["action"] in ("enforce_lora", "forbid_full_ft")
        ]
        if enforcement and requested_strategy == "full_fine_tuning":
            carbon_remaining = policy_context.get("carbon_remaining", 1.0)
            levy = (
                self._config.moderate_override_levy_kgco2e
                if carbon_remaining < self._config.moderate_override_budget_threshold
                else 0.0
            )
            return PolicyDecision(
                approved=False, enforced_strategy=recommended_strategy,
                original_strategy=requested_strategy, override_allowed=True,
                reasoning=(
                    f"{enforcement[0]['message']}. Override with carbon "
                    f"levy: {levy:.3f} kgCO2e"
                ),
                carbon_levy=levy,
            )
        return PolicyDecision(
            approved=True, enforced_strategy=None,
            original_strategy=requested_strategy, override_allowed=True,
            reasoning=(
                f"Policy warnings: "
                f"{'; '.join(r['message'] for r in violated_rules)}"
            ),
            carbon_levy=0.0,
        )

    def _handle_strict_mode(
        self,
        requested_strategy: str,
        recommended_strategy: str,
        violated_rules: List[Dict[str, Any]],
    ) -> PolicyDecision:
        if any(r["action"] == "block_submission" for r in violated_rules):
            return PolicyDecision(
                approved=False, enforced_strategy=None,
                original_strategy=requested_strategy, override_allowed=False,
                reasoning="Submission blocked due to policy violation",
                carbon_levy=0.0,
            )
        return PolicyDecision(
            approved=True, enforced_strategy=recommended_strategy,
            original_strategy=requested_strategy, override_allowed=False,
            reasoning=(
                f"Policy enforced: {recommended_strategy}. "
                f"Violations: {len(violated_rules)}"
            ),
            carbon_levy=0.0,
        )

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "policy_mode": self.policy_mode.value,
                "config": asdict(self._config),
                "strict": self._strict,
                "policy_violations": list(self.policy_violations),
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ParameterEfficiencyPolicyEngine":
        if not isinstance(data, Mapping):
            raise PolicyEngineError("from_dict expects a Mapping.")
        cfg_data = dict(data.get("config", {}) or {})
        cfg = PolicyConfig(
            low_budget_fraction=float(cfg_data.get("low_budget_fraction", 0.2)),
            emergency_budget_kgco2e=float(
                cfg_data.get("emergency_budget_kgco2e", 0.05)
            ),
            small_dataset_threshold=int(
                cfg_data.get("small_dataset_threshold", 5_000)
            ),
            large_model_param_threshold=int(
                cfg_data.get("large_model_param_threshold", 1_000_000_000)
            ),
            moderate_override_levy_kgco2e=float(
                cfg_data.get("moderate_override_levy_kgco2e", 0.1)
            ),
            moderate_override_budget_threshold=float(
                cfg_data.get("moderate_override_budget_threshold", 1.0)
            ),
            max_violations=int(cfg_data.get("max_violations", 10_000)),
        )
        engine = cls(
            policy_mode=PolicyMode(str(data.get("policy_mode", "moderate"))),
            config=cfg, strict=bool(data.get("strict", True)),
        )
        with engine._lock:
            for v in data.get("policy_violations", []):
                engine.policy_violations.append(dict(v))
        return engine

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "ParameterEfficiencyPolicyEngine":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise PolicyEngineError(f"Invalid JSON: {exc}") from exc

    def __repr__(self) -> str:
        with self._lock:
            return (
                "ParameterEfficiencyPolicyEngine("
                f"mode={self.policy_mode.value}, "
                f"violations={len(self.policy_violations)}, "
                f"strict={self._strict})"
            )


__all__ = [
    "ParameterEfficiencyPolicyEngine",
    "PolicyConfig",
    "PolicyDecision",
    "PolicyEngineError",
    "PolicyMode",
]


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    engine = ParameterEfficiencyPolicyEngine(policy_mode=PolicyMode.MODERATE)
    print("repr       :", engine)

    # Low budget scenario → enforcement.
    ctx = {
        "carbon_budget": 1.0, "carbon_remaining": 0.15,
        "dataset_size": 10_000, "model_params": 110_000_000,
    }
    d = engine.enforce("full_fine_tuning", "lora", ctx)
    print("decision   :", d)
    engine.log_violation("nlp_research", "task-1", "carbon_budget_low", d)

    # Missing keys — safe evaluation (no KeyError).
    d2 = engine.enforce("lora", "lora", {})
    print("safe eval  :", d2)

    # Emergency → blocked.
    d3 = engine.enforce("full_fine_tuning", "lora", {"carbon_budget": 1.0,
                                                      "carbon_remaining": 0.01})
    print("emergency  :", d3)

    print("stats      :", engine.statistics())

    # Round-trip.
    payload = engine.to_json()
    restored = ParameterEfficiencyPolicyEngine.from_json(payload)
    assert restored.to_dict() == engine.to_dict()
    print("Round-trip OK.")
