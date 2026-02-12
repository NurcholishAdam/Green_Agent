# -*- coding: utf-8 -*-
"""
Symbolic Violation Visualizer

Dashboard component for visualizing symbolic rule violations with trace explanations.
"""

from typing import Dict, List, Any, Optional
import json


class SymbolicVisualizer:
    """
    Visualizer for symbolic rule violations in dashboard.
    
    Responsibilities:
    - Format violation traces for display
    - Generate violation timeline
    - Create category-based violation views
    - Provide filtering by rule type
    """
    
    def __init__(self):
        self.violation_data: List[Dict[str, Any]] = []
    
    def add_violations(self, violations: List[Dict[str, Any]]):
        """Add violation traces to visualizer."""
        self.violation_data.extend(violations)
    
    def generate_violation_timeline(self) -> List[Dict[str, Any]]:
        """Generate timeline view of violations."""
        timeline = []
        
        for violation in sorted(self.violation_data, key=lambda v: v.get('timestamp', 0)):
            timeline.append({
                'timestamp': violation.get('timestamp'),
                'step': violation.get('step'),
                'rule_name': violation.get('rule_name'),
                'severity': violation.get('severity'),
                'category': self._extract_category(violation.get('rule_id', '')),
                'action': violation.get('action_triggered'),
                'status': 'critical' if violation.get('severity') == 'critical' else 'warning'
            })
        
        return timeline
    
    def generate_category_view(self) -> Dict[str, List[Dict[str, Any]]]:
        """Group violations by category."""
        by_category = {}
        
        for violation in self.violation_data:
            category = self._extract_category(violation.get('rule_id', ''))
            
            if category not in by_category:
                by_category[category] = []
            
            by_category[category].append({
                'rule_id': violation.get('rule_id'),
                'rule_name': violation.get('rule_name'),
                'step': violation.get('step'),
                'severity': violation.get('severity'),
                'explanation': violation.get('explanation'),
                'violation_details': violation.get('violation_details')
            })
        
        return by_category
    
    def generate_severity_summary(self) -> Dict[str, Any]:
        """Generate summary by severity level."""
        summary = {
            'critical': [],
            'high': [],
            'medium': [],
            'low': []
        }
        
        for violation in self.violation_data:
            severity = violation.get('severity', 'unknown')
            if severity in summary:
                summary[severity].append({
                    'rule_name': violation.get('rule_name'),
                    'step': violation.get('step'),
                    'action': violation.get('action_triggered')
                })
        
        return {
            'counts': {k: len(v) for k, v in summary.items()},
            'details': summary
        }
    
    def filter_by_rule_type(self, rule_type: str) -> List[Dict[str, Any]]:
        """Filter violations by rule type (sustainability, fairness, compliance, etc.)."""
        category_map = {
            'sustainability': ['SUST', 'COMP-SUST'],
            'resource': ['RES', 'COMP-RES'],
            'fairness': ['FAIR'],
            'safety': ['SAFE'],
            'compliance': ['COMP']
        }
        
        prefixes = category_map.get(rule_type.lower(), [])
        
        filtered = []
        for violation in self.violation_data:
            rule_id = violation.get('rule_id', '')
            if any(rule_id.startswith(prefix) for prefix in prefixes):
                filtered.append(violation)
        
        return filtered
    
    def generate_html_violation_card(self, violation: Dict[str, Any]) -> str:
        """Generate HTML card for a single violation."""
        severity = violation.get('severity', 'unknown')
        severity_colors = {
            'critical': '#dc3545',
            'high': '#fd7e14',
            'medium': '#ffc107',
            'low': '#17a2b8'
        }
        
        color = severity_colors.get(severity, '#6c757d')
        
        html = f"""
        <div class="violation-card" style="border-left: 4px solid {color}; padding: 15px; margin: 10px 0; background: #f8f9fa;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <h4 style="margin: 0; color: {color};">
                    🚨 {violation.get('rule_name', 'Unknown Rule')}
                </h4>
                <span style="background: {color}; color: white; padding: 4px 8px; border-radius: 4px; font-size: 12px;">
                    {severity.upper()}
                </span>
            </div>
            <p style="margin: 10px 0 5px 0; color: #6c757d; font-size: 14px;">
                <strong>Rule ID:</strong> {violation.get('rule_id', 'N/A')} | 
                <strong>Step:</strong> {violation.get('step', 'N/A')}
            </p>
            <p style="margin: 5px 0;"><strong>Condition:</strong> <code>{violation.get('condition', 'N/A')}</code></p>
            <p style="margin: 5px 0;"><strong>Action Triggered:</strong> {violation.get('action_triggered', 'N/A')}</p>
            <p style="margin: 5px 0; font-style: italic;">{violation.get('explanation', 'No explanation available')}</p>
            <details style="margin-top: 10px;">
                <summary style="cursor: pointer; color: #007bff;">View Trace Details</summary>
                <pre style="background: #fff; padding: 10px; border-radius: 4px; margin-top: 10px; overflow-x: auto;">
{violation.get('violation_details', 'No details available')}
                </pre>
            </details>
        </div>
        """
        
        return html
    
    def generate_dashboard_section(self) -> str:
        """Generate complete HTML section for dashboard."""
        if not self.violation_data:
            return """
            <div class="symbolic-section">
                <h3>✅ Symbolic Oversight</h3>
                <p style="color: #28a745;">No rule violations detected. All symbolic constraints satisfied.</p>
            </div>
            """
        
        severity_summary = self.generate_severity_summary()
        category_view = self.generate_category_view()
        
        html = """
        <div class="symbolic-section">
            <h3>🔍 Symbolic Oversight - Rule Violations</h3>
        """
        
        # Summary statistics
        html += f"""
        <div style="display: flex; gap: 15px; margin: 15px 0;">
            <div style="flex: 1; padding: 15px; background: #dc3545; color: white; border-radius: 8px;">
                <h4 style="margin: 0;">Critical</h4>
                <p style="font-size: 24px; margin: 5px 0;">{severity_summary['counts']['critical']}</p>
            </div>
            <div style="flex: 1; padding: 15px; background: #fd7e14; color: white; border-radius: 8px;">
                <h4 style="margin: 0;">High</h4>
                <p style="font-size: 24px; margin: 5px 0;">{severity_summary['counts']['high']}</p>
            </div>
            <div style="flex: 1; padding: 15px; background: #ffc107; color: white; border-radius: 8px;">
                <h4 style="margin: 0;">Medium</h4>
                <p style="font-size: 24px; margin: 5px 0;">{severity_summary['counts']['medium']}</p>
            </div>
            <div style="flex: 1; padding: 15px; background: #17a2b8; color: white; border-radius: 8px;">
                <h4 style="margin: 0;">Low</h4>
                <p style="font-size: 24px; margin: 5px 0;">{severity_summary['counts']['low']}</p>
            </div>
        </div>
        """
        
        # Category breakdown
        html += "<h4>Violations by Category</h4>"
        for category, violations in category_view.items():
            html += f"""
            <details style="margin: 10px 0;">
                <summary style="cursor: pointer; padding: 10px; background: #e9ecef; border-radius: 4px;">
                    <strong>{category.upper()}</strong> ({len(violations)} violation(s))
                </summary>
                <div style="padding: 10px;">
            """
            
            for violation in violations:
                html += self.generate_html_violation_card(violation)
            
            html += "</div></details>"
        
        html += "</div>"
        
        return html
    
    def export_violation_report(self, filepath: str):
        """Export violation report as JSON."""
        report = {
            'total_violations': len(self.violation_data),
            'severity_summary': self.generate_severity_summary(),
            'category_view': self.generate_category_view(),
            'timeline': self.generate_violation_timeline(),
            'violations': self.violation_data
        }
        
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2)
    
    def _extract_category(self, rule_id: str) -> str:
        """Extract category from rule ID."""
        if not rule_id:
            return 'unknown'
        
        parts = rule_id.split('-')
        if len(parts) >= 1:
            return parts[0].lower()
        
        return 'unknown'
