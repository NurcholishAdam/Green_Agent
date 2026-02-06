# -*- coding: utf-8 -*-
"""
Policy Engine Module

Enforces resource budgets and meta-cognitive rules including reflection
frequency and self-adjustment thresholds.
"""

import yaml
from typing import Dict, Any, Optional


class PolicyEngine:
    """
    Policy enforcement engine with meta-cognitive rules.
    
    Responsibilities:
    - Load and parse policy configuration
    - Enforce resource budgets
    - Manage reflection frequency
    - Apply self-adjustment thresholds
    - Trigger adaptive policy changes
    """
    
    def __init__(self, policy_config: Optional[Dict] = None, policy_file: Optional[str] = None):
        """
        Initialize policy engine.
        
        Args:
            policy_config: Policy configuration dictionary
            policy_file: Path to YAML policy file
        """
        if policy_file:
            with open(policy_file, 'r') as f:
                self.policy = yaml.safe_load(f)
        elif policy_config:
            self.policy = policy_config
        else:
            self.policy = self._default_policy()
        
        self.violations = []
        self.adjustments_made = []
        
    def _default_policy(self) -> Dict[str, Any]:
        """Return default policy configuration."""
        return {
            "constraints": {
                "max_energy_per_task_wh": 5.0,
                "max_carbon_per_task_kg": 0.002,
                "max_latency_seconds": 120,
                "memory_limit_gb": 4.0
            },
            "meta_cognitive": {
                "reflection_frequency": 5,
                "self_adjustment_thresholds": {
                    "energy_threshold_pct": 80,
                    "carbon_threshold_pct": 80,
                    "latency_threshold_pct": 80,
                    "memory_threshold_pct": 80
                },
                "adaptive_policy_enabled": True
            },
            "optimization": {
                "priority_weight": {
                    "accuracy": 0.6,
                    "sustainability": 0.4
                }
            }
        }
    
    def get_budgets(self) -> Dict[str, float]:
        """
        Get budget constraints.
        
        Returns:
            Dictionary of budget limits
        """
        constraints = self.policy.get("constraints", {})
        return {
            "max_energy_wh": constraints.get("max_energy_per_task_wh", 5.0),
            "max_carbon_kg": constraints.get("max_carbon_per_task_kg", 0.002),
            "max_latency_s": constraints.get("max_latency_seconds", 120),
            "max_memory_mb": constraints.get("memory_limit_gb", 4.0) * 1024
        }
    
    def get_reflection_frequency(self) -> int:
        """Get reflection checkpoint frequency."""
        return self.policy.get("meta_cognitive", {}).get("reflection_frequency", 5)
    
    def get_self_adjustment_thresholds(self) -> Dict[str, float]:
        """Get thresholds for self-adjustment."""
        return self.policy.get("meta_cognitive", {}).get("self_adjustment_thresholds", {})
    
    def should_reflect(self, step: int) -> bool:
        """Check if agent should reflect at this step."""
        frequency = self.get_reflection_frequency()
        return step > 0 and step % frequency == 0
    
    def enforce(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enforce policy constraints on metrics.
        
        Args:
            metrics: Current metrics snapshot
            
        Returns:
            Enforcement result with violations
        """
        budgets = self.get_budgets()
        violations = []
        
        # Check energy
        if "energy" in metrics:
            if metrics["energy"] > budgets["max_energy_wh"]:
                violations.append({
                    "type": "energy",
                    "limit": budgets["max_energy_wh"],
                    "actual": metrics["energy"]
                })
        
        # Check carbon
        if "carbon" in metrics:
            if metrics["carbon"] > budgets["max_carbon_kg"]:
                violations.append({
                    "type": "carbon",
                    "limit": budgets["max_carbon_kg"],
                    "actual": metrics["carbon"]
                })
        
        # Check latency
        if "latency" in metrics:
            if metrics["latency"] > budgets["max_latency_s"]:
                violations.append({
                    "type": "latency",
                    "limit": budgets["max_latency_s"],
                    "actual": metrics["latency"]
                })
        
        self.violations.extend(violations)
        
        return {
            "passed": len(violations) == 0,
            "violations": violations
        }
    
    def should_self_adjust(self, metrics: Dict[str, Any]) -> bool:
        """
        Check if agent should self-adjust based on thresholds.
        
        Args:
            metrics: Current metrics with utilization percentages
            
        Returns:
            True if self-adjustment is needed
        """
        thresholds = self.get_self_adjustment_thresholds()
        utilization = metrics.get("budget_status", {}).get("utilization", {})
        
        # Check if any metric exceeds threshold
        if utilization.get("energy", 0) > thresholds.get("energy_threshold_pct", 80):
            return True
        if utilization.get("carbon", 0) > thresholds.get("carbon_threshold_pct", 80):
            return True
        if utilization.get("latency", 0) > thresholds.get("latency_threshold_pct", 80):
            return True
        if utilization.get("memory", 0) > thresholds.get("memory_threshold_pct", 80):
            return True
        
        return False
    
    def apply_adaptive_adjustment(self, decision: str) -> Dict[str, Any]:
        """
        Apply adaptive policy adjustment based on decision.
        
        Args:
            decision: Decision from reflection engine
            
        Returns:
            Adjustment details
        """
        if not self.policy.get("meta_cognitive", {}).get("adaptive_policy_enabled", True):
            return {"adjusted": False, "reason": "Adaptive policy disabled"}
        
        adjustment = {"adjusted": True, "decision": decision, "changes": []}
        
        if decision == "reduce_tool_calls":
            # Reduce tool call budget
            adjustment["changes"].append("Reduced tool call frequency")
            
        elif decision == "reduce_energy_usage":
            # Tighten energy constraints
            current_limit = self.policy["constraints"]["max_energy_per_task_wh"]
            new_limit = current_limit * 0.9
            self.policy["constraints"]["max_energy_per_task_wh"] = new_limit
            adjustment["changes"].append(f"Reduced energy limit to {new_limit:.2f} Wh")
            
        elif decision == "optimize_speed":
            # Relax latency constraints slightly
            current_limit = self.policy["constraints"]["max_latency_seconds"]
            new_limit = current_limit * 1.1
            self.policy["constraints"]["max_latency_seconds"] = new_limit
            adjustment["changes"].append(f"Increased latency limit to {new_limit:.1f}s")
            
        elif decision == "reduce_memory_usage":
            # Tighten memory constraints
            current_limit = self.policy["constraints"]["memory_limit_gb"]
            new_limit = current_limit * 0.9
            self.policy["constraints"]["memory_limit_gb"] = new_limit
            adjustment["changes"].append(f"Reduced memory limit to {new_limit:.2f} GB")
        
        self.adjustments_made.append(adjustment)
        return adjustment
    
    def get_enforcement_summary(self) -> Dict[str, Any]:
        """Get summary of policy enforcement."""
        return {
            "total_violations": len(self.violations),
            "violations_by_type": self._count_violations_by_type(),
            "adjustments_made": len(self.adjustments_made),
            "current_budgets": self.get_budgets()
        }
    
    def _count_violations_by_type(self) -> Dict[str, int]:
        """Count violations by type."""
        counts = {}
        for v in self.violations:
            vtype = v.get("type", "unknown")
            counts[vtype] = counts.get(vtype, 0) + 1
        return counts
