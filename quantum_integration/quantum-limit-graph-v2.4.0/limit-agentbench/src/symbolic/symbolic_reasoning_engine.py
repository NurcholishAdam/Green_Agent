# -*- coding: utf-8 -*-
"""
Symbolic Reasoning Engine

Lightweight symbolic logic engine for evaluating formal rules against metrics.
Inspired by FormalJudge neuro-symbolic oversight paradigm.
"""

import re
import yaml
import json
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import operator


@dataclass
class SymbolicRule:
    """Represents a single symbolic rule."""
    id: str
    name: str
    category: str
    priority: str
    condition: str
    action: str
    explanation: str
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ViolationTrace:
    """Formal trace of a rule violation."""
    rule_id: str
    rule_name: str
    timestamp: float
    step: int
    condition: str
    observation: Dict[str, Any]
    violation_details: str
    action_triggered: str
    explanation: str
    severity: str
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SymbolicReasoningEngine:
    """
    Lightweight symbolic reasoning engine for rule evaluation.
    
    Responsibilities:
    - Parse symbolic rules from YAML
    - Evaluate rules against collected metrics
    - Generate formal violation traces
    - Provide interpretable explanations
    """
    
    def __init__(self, policy_file: str = "symbolic_policy.yaml"):
        self.policy_file = policy_file
        self.rules: List[SymbolicRule] = []
        self.composite_rules: List[SymbolicRule] = []
        self.domain_rules: Dict[str, List[SymbolicRule]] = {}
        self.violation_history: List[ViolationTrace] = []
        self.evaluation_count = 0
        
        # Operator mapping for condition evaluation
        self.operators = {
            '>': operator.gt,
            '<': operator.lt,
            '>=': operator.ge,
            '<=': operator.le,
            '==': operator.eq,
            '!=': operator.ne,
        }
        
        self._load_rules()
    
    def _load_rules(self):
        """Load symbolic rules from policy file."""
        try:
            with open(self.policy_file, 'r') as f:
                policy = yaml.safe_load(f)
            
            # Load basic rules
            for rule_data in policy.get('symbolic_rules', []):
                rule = SymbolicRule(**rule_data)
                self.rules.append(rule)
            
            # Load composite rules
            for rule_data in policy.get('composite_rules', []):
                rule = SymbolicRule(**rule_data)
                self.composite_rules.append(rule)
            
            # Load domain-specific rules
            domain_ext = policy.get('domain_extensions', {})
            for domain, rules_data in domain_ext.items():
                self.domain_rules[domain] = [
                    SymbolicRule(
                        id=r['id'],
                        name=r.get('name', r['id']),
                        category='domain',
                        priority=r.get('priority', 'medium'),
                        condition=r['condition'],
                        action=r['action'],
                        explanation=r['explanation']
                    )
                    for r in rules_data
                ]
            
            print(f"✓ Loaded {len(self.rules)} basic rules, {len(self.composite_rules)} composite rules")
            
        except FileNotFoundError:
            print(f"⚠️  Symbolic policy file not found: {self.policy_file}")
            print("   Using default rules")
            self._load_default_rules()
    
    def _load_default_rules(self):
        """Load minimal default rules if policy file not found."""
        self.rules = [
            SymbolicRule(
                id="DEFAULT-001",
                name="Energy Budget Exceeded",
                category="sustainability",
                priority="critical",
                condition="energy > 5.0",
                action="halt_execution",
                explanation="Energy consumption exceeds maximum allowed budget"
            ),
            SymbolicRule(
                id="DEFAULT-002",
                name="Memory Overflow Risk",
                category="resource",
                priority="critical",
                condition="memory > 500",
                action="trigger_resource_alert",
                explanation="Memory usage exceeds safe threshold"
            )
        ]
    
    def evaluate_rules(
        self,
        metrics: Dict[str, Any],
        step: int,
        domain: Optional[str] = None
    ) -> List[ViolationTrace]:
        """
        Evaluate all applicable rules against current metrics.
        
        Args:
            metrics: Current metric values
            step: Current execution step
            domain: Optional domain for domain-specific rules
            
        Returns:
            List of violation traces for rules that triggered
        """
        self.evaluation_count += 1
        violations = []
        
        # Normalize metrics for evaluation
        normalized_metrics = self._normalize_metrics(metrics)
        
        # Evaluate basic rules
        for rule in self.rules:
            if self._evaluate_condition(rule.condition, normalized_metrics):
                violation = self._create_violation_trace(rule, normalized_metrics, step)
                violations.append(violation)
        
        # Evaluate composite rules
        for rule in self.composite_rules:
            if self._evaluate_condition(rule.condition, normalized_metrics):
                violation = self._create_violation_trace(rule, normalized_metrics, step)
                violations.append(violation)
        
        # Evaluate domain-specific rules
        if domain and domain in self.domain_rules:
            for rule in self.domain_rules[domain]:
                if self._evaluate_condition(rule.condition, normalized_metrics):
                    violation = self._create_violation_trace(rule, normalized_metrics, step)
                    violations.append(violation)
        
        # Store violations
        self.violation_history.extend(violations)
        
        return violations
    
    def _normalize_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize metrics to standard variable names for rule evaluation."""
        normalized = {}
        
        # Extract from nested structures
        cumulative = metrics.get('cumulative', {})
        
        # Map to symbolic variables
        normalized['energy'] = cumulative.get('total_energy_wh', metrics.get('energy', 0))
        normalized['carbon'] = cumulative.get('total_carbon_kg', metrics.get('carbon', 0)) * 1000  # Convert to grams
        normalized['latency'] = cumulative.get('total_latency_ms', metrics.get('latency', 0) * 1000)
        normalized['memory'] = cumulative.get('max_memory_mb', metrics.get('memory_mb', 0))
        normalized['tool_calls'] = cumulative.get('total_tool_calls', metrics.get('tool_calls', 0))
        normalized['cpu_percent'] = metrics.get('cpu_percent', 0)
        
        # Calculate derived metrics
        if 'step_count' in cumulative:
            normalized['avg_energy_per_step'] = normalized['energy'] / max(cumulative['step_count'], 1)
        
        # Add metadata
        normalized['query_type'] = metrics.get('query_type', 'unknown')
        normalized['environment'] = metrics.get('environment', 'development')
        
        return normalized
    
    def _evaluate_condition(self, condition: str, metrics: Dict[str, Any]) -> bool:
        """
        Evaluate a symbolic condition against metrics.
        
        Args:
            condition: Symbolic condition string (e.g., "carbon > 60 AND latency > 2000")
            metrics: Normalized metrics dictionary
            
        Returns:
            True if condition is satisfied, False otherwise
        """
        try:
            # Replace logical operators
            condition = condition.replace(' AND ', ' and ')
            condition = condition.replace(' OR ', ' or ')
            condition = condition.replace(' NOT ', ' not ')
            
            # Parse and evaluate condition
            # This is a simplified evaluator - for production, consider using a proper parser
            return self._safe_eval(condition, metrics)
            
        except Exception as e:
            print(f"⚠️  Error evaluating condition '{condition}': {e}")
            return False
    
    def _safe_eval(self, condition: str, metrics: Dict[str, Any]) -> bool:
        """
        Safely evaluate condition with metric substitution.
        
        Args:
            condition: Condition string
            metrics: Metrics dictionary
            
        Returns:
            Boolean result of evaluation
        """
        # Create safe evaluation context
        eval_context = {
            'carbon': metrics.get('carbon', 0),
            'energy': metrics.get('energy', 0),
            'latency': metrics.get('latency', 0),
            'memory': metrics.get('memory', 0),
            'tool_calls': metrics.get('tool_calls', 0),
            'cpu_percent': metrics.get('cpu_percent', 0),
            'query_type': metrics.get('query_type', 'unknown'),
            'environment': metrics.get('environment', 'development'),
            'error_rate': metrics.get('error_rate', 0),
            'memory_growth_rate': metrics.get('memory_growth_rate', 0),
            'energy_variance': metrics.get('energy_variance', 0),
            'latency_std_dev': metrics.get('latency_std_dev', 0),
            'policy_violation_count': metrics.get('policy_violation_count', 0),
        }
        
        try:
            # Use eval with restricted context (only allow comparison operations)
            result = eval(condition, {"__builtins__": {}}, eval_context)
            return bool(result)
        except:
            return False
    
    def _create_violation_trace(
        self,
        rule: SymbolicRule,
        metrics: Dict[str, Any],
        step: int
    ) -> ViolationTrace:
        """Create formal violation trace for a triggered rule."""
        # Extract relevant observations
        observation = self._extract_relevant_metrics(rule.condition, metrics)
        
        # Generate violation details
        violation_details = self._generate_violation_details(rule, observation)
        
        trace = ViolationTrace(
            rule_id=rule.id,
            rule_name=rule.name,
            timestamp=datetime.now().timestamp(),
            step=step,
            condition=rule.condition,
            observation=observation,
            violation_details=violation_details,
            action_triggered=rule.action,
            explanation=rule.explanation,
            severity=rule.priority
        )
        
        return trace
    
    def _extract_relevant_metrics(
        self,
        condition: str,
        metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Extract only metrics mentioned in the condition."""
        relevant = {}
        
        # Find all variable names in condition
        variables = re.findall(r'\b([a-z_]+)\b', condition.lower())
        
        for var in set(variables):
            if var in metrics:
                relevant[var] = metrics[var]
        
        return relevant
    
    def _generate_violation_details(
        self,
        rule: SymbolicRule,
        observation: Dict[str, Any]
    ) -> str:
        """Generate human-readable violation details."""
        details = [f"Rule: {rule.name} ({rule.id})"]
        details.append(f"Condition: {rule.condition}")
        details.append("Observations:")
        
        for key, value in observation.items():
            if isinstance(value, float):
                details.append(f"  {key} = {value:.4f}")
            else:
                details.append(f"  {key} = {value}")
        
        details.append(f"Violation: Rule triggered → {rule.action}")
        
        return "\n".join(details)
    
    def get_violations_by_category(self, category: str) -> List[ViolationTrace]:
        """Get all violations for a specific category."""
        return [v for v in self.violation_history if self._get_rule_category(v.rule_id) == category]
    
    def _get_rule_category(self, rule_id: str) -> str:
        """Get category for a rule by ID."""
        for rule in self.rules + self.composite_rules:
            if rule.id == rule_id:
                return rule.category
        return "unknown"
    
    def get_violation_summary(self) -> Dict[str, Any]:
        """Get summary of all violations."""
        if not self.violation_history:
            return {
                "total_violations": 0,
                "evaluations": self.evaluation_count
            }
        
        # Count by category
        by_category = {}
        by_severity = {}
        
        for violation in self.violation_history:
            category = self._get_rule_category(violation.rule_id)
            by_category[category] = by_category.get(category, 0) + 1
            by_severity[violation.severity] = by_severity.get(violation.severity, 0) + 1
        
        return {
            "total_violations": len(self.violation_history),
            "evaluations": self.evaluation_count,
            "by_category": by_category,
            "by_severity": by_severity,
            "violation_rate": len(self.violation_history) / max(self.evaluation_count, 1)
        }
    
    def export_violations(self, filepath: str):
        """Export violation traces to JSON file."""
        data = {
            "summary": self.get_violation_summary(),
            "violations": [v.to_dict() for v in self.violation_history]
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
    
    def get_active_rules(self) -> List[Dict[str, Any]]:
        """Get list of all active rules."""
        all_rules = self.rules + self.composite_rules
        for domain_rules in self.domain_rules.values():
            all_rules.extend(domain_rules)
        
        return [r.to_dict() for r in all_rules]
