"""
Parameter-Efficiency Policy Engine
===================================

Enforces carbon budget policies through fine-tuning method restrictions.

Location: src/ml_governance/policy_engine.py
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class PolicyMode(Enum):
    """Policy enforcement modes"""
    SOFT = "soft"          # Warnings only
    MODERATE = "moderate"  # Require justification to override
    STRICT = "strict"      # No overrides allowed


@dataclass
class PolicyDecision:
    """Policy enforcement decision"""
    approved: bool
    enforced_strategy: Optional[str]
    original_strategy: str
    override_allowed: bool
    reasoning: str
    carbon_levy: float  # Additional carbon cost if override


class ParameterEfficiencyPolicyEngine:
    """Enforces parameter-efficiency policies"""
    
    def __init__(self, policy_mode: PolicyMode = PolicyMode.MODERATE):
        self.policy_mode = policy_mode
        self.policy_violations: List[Dict[str, Any]] = []
        
        # Policy rules
        self.rules = {
            "carbon_budget_low": {
                "condition": lambda ctx: ctx["carbon_remaining"] < ctx["carbon_budget"] * 0.2,
                "action": "enforce_lora",
                "message": "Low carbon budget (<20% remaining) - LoRA enforced"
            },
            "dataset_small": {
                "condition": lambda ctx: ctx["dataset_size"] < 5_000,
                "action": "forbid_full_ft",
                "message": "Small dataset (<5K samples) - Full fine-tuning forbidden"
            },
            "model_large": {
                "condition": lambda ctx: ctx.get("model_params", 0) > 1_000_000_000,
                "action": "suggest_parameter_efficient",
                "message": "Large model (>1B params) - Parameter-efficient methods recommended"
            },
            "emergency_carbon": {
                "condition": lambda ctx: ctx["carbon_remaining"] < 0.05,
                "action": "block_submission",
                "message": "Emergency: Carbon budget exhausted (<0.05 kgCO2e remaining)"
            }
        }
        
        logger.info(f"Policy engine initialized in {policy_mode.value} mode")
    
    def enforce(
        self,
        requested_strategy: str,
        recommended_strategy: str,
        policy_context: Dict[str, Any]
    ) -> PolicyDecision:
        """
        Enforce policy on fine-tuning strategy
        
        Args:
            requested_strategy: What user requested
            recommended_strategy: What classifier recommended
            policy_context: Context with carbon budget, dataset size, etc.
        
        Returns:
            PolicyDecision with approval and enforcement details
        """
        
        # Evaluate all policy rules
        violated_rules = []
        for rule_name, rule in self.rules.items():
            if rule["condition"](policy_context):
                violated_rules.append(rule)
        
        if not violated_rules:
            # No violations - approve as requested
            return PolicyDecision(
                approved=True,
                enforced_strategy=None,
                original_strategy=requested_strategy,
                override_allowed=True,
                reasoning="No policy violations detected",
                carbon_levy=0.0
            )
        
        # Handle violations based on policy mode
        if self.policy_mode == PolicyMode.SOFT:
            return self._handle_soft_mode(requested_strategy, violated_rules)
        elif self.policy_mode == PolicyMode.MODERATE:
            return self._handle_moderate_mode(
                requested_strategy, recommended_strategy, violated_rules, policy_context
            )
        else:  # STRICT
            return self._handle_strict_mode(
                requested_strategy, recommended_strategy, violated_rules
            )
    
    def _handle_soft_mode(
        self,
        requested_strategy: str,
        violated_rules: List[Dict[str, str]]
    ) -> PolicyDecision:
        """Soft mode: Warnings only"""
        warnings = [rule["message"] for rule in violated_rules]
        return PolicyDecision(
            approved=True,
            enforced_strategy=None,
            original_strategy=requested_strategy,
            override_allowed=True,
            reasoning=f"Warnings: {'; '.join(warnings)}",
            carbon_levy=0.0
        )
    
    def _handle_moderate_mode(
        self,
        requested_strategy: str,
        recommended_strategy: str,
        violated_rules: List[Dict[str, str]],
        policy_context: Dict[str, Any]
    ) -> PolicyDecision:
        """Moderate mode: Require justification"""
        
        # Check for blocking rules
        blocking_rules = [r for r in violated_rules if r["action"] == "block_submission"]
        if blocking_rules:
            return PolicyDecision(
                approved=False,
                enforced_strategy=None,
                original_strategy=requested_strategy,
                override_allowed=False,
                reasoning=blocking_rules[0]["message"],
                carbon_levy=0.0
            )
        
        # Check for enforcement rules
        enforcement_rules = [r for r in violated_rules if r["action"] in ["enforce_lora", "forbid_full_ft"]]
        if enforcement_rules and requested_strategy == "full_fine_tuning":
            # Calculate carbon levy for override
            carbon_remaining = policy_context.get("carbon_remaining", 1.0)
            carbon_levy = 0.1 if carbon_remaining < 1.0 else 0.0
            
            return PolicyDecision(
                approved=False,
                enforced_strategy=recommended_strategy,
                original_strategy=requested_strategy,
                override_allowed=True,
                reasoning=f"{enforcement_rules[0]['message']}. Override with carbon levy: {carbon_levy:.3f} kgCO2e",
                carbon_levy=carbon_levy
            )
        
        # Warning only
        return PolicyDecision(
            approved=True,
            enforced_strategy=None,
            original_strategy=requested_strategy,
            override_allowed=True,
            reasoning=f"Policy warnings: {'; '.join([r['message'] for r in violated_rules])}",
            carbon_levy=0.0
        )
    
    def _handle_strict_mode(
        self,
        requested_strategy: str,
        recommended_strategy: str,
        violated_rules: List[Dict[str, str]]
    ) -> PolicyDecision:
        """Strict mode: No overrides"""
        
        if any(r["action"] == "block_submission" for r in violated_rules):
            return PolicyDecision(
                approved=False,
                enforced_strategy=None,
                original_strategy=requested_strategy,
                override_allowed=False,
                reasoning="Submission blocked due to policy violation",
                carbon_levy=0.0
            )
        
        return PolicyDecision(
            approved=True,
            enforced_strategy=recommended_strategy,
            original_strategy=requested_strategy,
            override_allowed=False,
            reasoning=f"Policy enforced: {recommended_strategy}. Violations: {len(violated_rules)}",
            carbon_levy=0.0
        )
    
    def log_violation(
        self,
        team: str,
        task_id: str,
        violation_type: str,
        decision: PolicyDecision
    ):
        """Log policy violation"""
        violation = {
            "timestamp": datetime.now().isoformat(),
            "team": team,
            "task_id": task_id,
            "violation_type": violation_type,
            "approved": decision.approved,
            "enforced_strategy": decision.enforced_strategy,
            "carbon_levy": decision.carbon_levy
        }
        self.policy_violations.append(violation)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get policy statistics"""
        if not self.policy_violations:
            return {"num_violations": 0}
        
        return {
            "num_violations": len(self.policy_violations),
            "num_blocked": sum(1 for v in self.policy_violations if not v["approved"]),
            "total_carbon_levy": sum(v["carbon_levy"] for v in self.policy_violations),
            "violations_by_team": self._count_by_field("team")
        }
    
    def _count_by_field(self, field: str) -> Dict[str, int]:
        """Count violations by field"""
        counts = {}
        for v in self.policy_violations:
            value = v.get(field, "unknown")
            counts[value] = counts.get(value, 0) + 1
        return counts


if __name__ == "__main__":
    engine = ParameterEfficiencyPolicyEngine(policy_mode=PolicyMode.MODERATE)
    
    # Example: Low carbon budget scenario
    context = {
        "carbon_budget": 1.0,
        "carbon_remaining": 0.15,  # Only 15% left
        "dataset_size": 10_000,
        "model_params": 110_000_000
    }
    
    decision = engine.enforce(
        requested_strategy="full_fine_tuning",
        recommended_strategy="lora",
        policy_context=context
    )
    
    print(f"Approved: {decision.approved}")
    print(f"Enforced: {decision.enforced_strategy}")
    print(f"Reasoning: {decision.reasoning}")
    print(f"Carbon levy: {decision.carbon_levy:.3f} kgCO2e")
