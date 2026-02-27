# -*- coding: utf-8 -*-
"""
Feedback Integration & Audit Logger

Tracks expert invocations, feedback integration, and sustainability metrics.
Provides transparency reports for accountability.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import datetime
import json
import hashlib


class FeedbackType(Enum):
    """Types of expert feedback."""
    CORRECTION = "correction"
    SUGGESTION = "suggestion"
    APPROVAL = "approval"
    REJECTION = "rejection"
    OPTIMIZATION = "optimization"
    SECURITY_FIX = "security_fix"


class AuditEventType(Enum):
    """Types of audit events."""
    EXPERT_INVOKED = "expert_invoked"
    FEEDBACK_RECEIVED = "feedback_received"
    FEEDBACK_INTEGRATED = "feedback_integrated"
    ESCALATION_TRIGGERED = "escalation_triggered"
    ENERGY_SAVED = "energy_saved"
    CARBON_SAVED = "carbon_saved"


@dataclass
class ExpertFeedback:
    """Expert feedback on agent output."""
    feedback_id: str
    expert_type: str
    feedback_type: FeedbackType
    original_output: Any
    suggested_output: Any
    explanation: str
    confidence: float
    energy_impact: float  # Positive = savings, negative = cost
    timestamp: float


@dataclass
class AuditEvent:
    """Event in audit log."""
    event_id: str
    event_type: AuditEventType
    task_id: str
    agent_id: str
    expert_involved: Optional[str]
    energy_consumed_wh: float
    carbon_emitted_kg: float
    details: Dict[str, Any]
    timestamp: float


@dataclass
class TransparencyReport:
    """Comprehensive transparency report."""
    report_id: str
    period_start: float
    period_end: float
    
    # Task metrics
    total_tasks: int
    tasks_escalated: int
    escalation_rate: float
    
    # Expert metrics
    expert_invocations: int
    experts_by_type: Dict[str, int]
    feedback_received: int
    feedback_integrated: int
    feedback_integration_rate: float
    
    # Sustainability metrics
    total_energy_consumed_wh: float
    energy_saved_wh: float
    net_energy_wh: float
    total_carbon_kg: float
    carbon_saved_kg: float
    net_carbon_kg: float
    
    # Efficiency metrics
    avg_task_energy_wh: float
    avg_expert_energy_wh: float
    sustainability_improvement_pct: float
    
    # Transparency metrics
    audit_events: int
    transparency_score: float


class AuditLogger:
    """
    Audit logger for expert collaboration system.
    
    Features:
    - Event logging
    - Feedback tracking
    - Sustainability metrics
    - Transparency reporting
    """
    
    def __init__(
        self,
        log_file: Optional[str] = None,
        enable_persistence: bool = True
    ):
        """
        Initialize audit logger.
        
        Args:
            log_file: Path to log file
            enable_persistence: Enable persistent logging
        """
        self.log_file = log_file
        self.enable_persistence = enable_persistence
        
        # In-memory storage
        self.audit_events: List[AuditEvent] = []
        self.feedback_history: List[ExpertFeedback] = []
        
        # Metrics
        self.total_energy_consumed = 0.0
        self.total_carbon_emitted = 0.0
        self.total_energy_saved = 0.0
        self.total_carbon_saved = 0.0
        
        # Load from file if exists
        if self.enable_persistence and self.log_file:
            self._load_from_file()
    
    def log_expert_invocation(
        self,
        task_id: str,
        agent_id: str,
        expert_type: str,
        energy_wh: float,
        carbon_kg: float,
        details: Optional[Dict[str, Any]] = None
    ):
        """Log expert model invocation."""
        event = AuditEvent(
            event_id=self._generate_event_id(),
            event_type=AuditEventType.EXPERT_INVOKED,
            task_id=task_id,
            agent_id=agent_id,
            expert_involved=expert_type,
            energy_consumed_wh=energy_wh,
            carbon_emitted_kg=carbon_kg,
            details=details or {},
            timestamp=datetime.now().timestamp()
        )
        
        self._add_event(event)
        
        # Update metrics
        self.total_energy_consumed += energy_wh
        self.total_carbon_emitted += carbon_kg
    
    def log_feedback_received(
        self,
        feedback: ExpertFeedback
    ):
        """Log feedback received from expert."""
        self.feedback_history.append(feedback)
        
        event = AuditEvent(
            event_id=self._generate_event_id(),
            event_type=AuditEventType.FEEDBACK_RECEIVED,
            task_id=feedback.feedback_id,
            agent_id="unknown",
            expert_involved=feedback.expert_type,
            energy_consumed_wh=0.0,
            carbon_emitted_kg=0.0,
            details={
                "feedback_type": feedback.feedback_type.value,
                "confidence": feedback.confidence
            },
            timestamp=feedback.timestamp
        )
        
        self._add_event(event)
    
    def log_feedback_integrated(
        self,
        feedback_id: str,
        task_id: str,
        agent_id: str,
        integration_success: bool,
        energy_impact: float
    ):
        """Log feedback integration."""
        event = AuditEvent(
            event_id=self._generate_event_id(),
            event_type=AuditEventType.FEEDBACK_INTEGRATED,
            task_id=task_id,
            agent_id=agent_id,
            expert_involved=None,
            energy_consumed_wh=0.0,
            carbon_emitted_kg=0.0,
            details={
                "feedback_id": feedback_id,
                "success": integration_success,
                "energy_impact": energy_impact
            },
            timestamp=datetime.now().timestamp()
        )
        
        self._add_event(event)
        
        # Track energy savings
        if energy_impact > 0:
            self.total_energy_saved += energy_impact
            carbon_saved = energy_impact * 0.000385  # Default grid
            self.total_carbon_saved += carbon_saved
    
    def log_escalation(
        self,
        task_id: str,
        agent_id: str,
        reasons: List[str],
        expert_type: str
    ):
        """Log escalation decision."""
        event = AuditEvent(
            event_id=self._generate_event_id(),
            event_type=AuditEventType.ESCALATION_TRIGGERED,
            task_id=task_id,
            agent_id=agent_id,
            expert_involved=expert_type,
            energy_consumed_wh=0.0,
            carbon_emitted_kg=0.0,
            details={"reasons": reasons},
            timestamp=datetime.now().timestamp()
        )
        
        self._add_event(event)
    
    def log_energy_saved(
        self,
        task_id: str,
        agent_id: str,
        energy_saved_wh: float,
        mechanism: str
    ):
        """Log energy savings."""
        carbon_saved = energy_saved_wh * 0.000385
        
        event = AuditEvent(
            event_id=self._generate_event_id(),
            event_type=AuditEventType.ENERGY_SAVED,
            task_id=task_id,
            agent_id=agent_id,
            expert_involved=None,
            energy_consumed_wh=-energy_saved_wh,  # Negative = saved
            carbon_emitted_kg=-carbon_saved,
            details={"mechanism": mechanism},
            timestamp=datetime.now().timestamp()
        )
        
        self._add_event(event)
        
        self.total_energy_saved += energy_saved_wh
        self.total_carbon_saved += carbon_saved
    
    def generate_transparency_report(
        self,
        period_start: Optional[float] = None,
        period_end: Optional[float] = None
    ) -> TransparencyReport:
        """
        Generate comprehensive transparency report.
        
        Args:
            period_start: Start timestamp (None = all time)
            period_end: End timestamp (None = now)
            
        Returns:
            Transparency report
        """
        # Filter events by period
        if period_start is None:
            period_start = min(
                (e.timestamp for e in self.audit_events),
                default=datetime.now().timestamp()
            )
        
        if period_end is None:
            period_end = datetime.now().timestamp()
        
        period_events = [
            e for e in self.audit_events
            if period_start <= e.timestamp <= period_end
        ]
        
        # Calculate metrics
        total_tasks = len(set(e.task_id for e in period_events))
        
        escalations = [
            e for e in period_events
            if e.event_type == AuditEventType.ESCALATION_TRIGGERED
        ]
        tasks_escalated = len(set(e.task_id for e in escalations))
        escalation_rate = tasks_escalated / total_tasks if total_tasks > 0 else 0
        
        expert_invocations_events = [
            e for e in period_events
            if e.event_type == AuditEventType.EXPERT_INVOKED
        ]
        expert_invocations = len(expert_invocations_events)
        
        experts_by_type = {}
        for e in expert_invocations_events:
            expert = e.expert_involved or "unknown"
            experts_by_type[expert] = experts_by_type.get(expert, 0) + 1
        
        feedback_received_events = [
            e for e in period_events
            if e.event_type == AuditEventType.FEEDBACK_RECEIVED
        ]
        feedback_received = len(feedback_received_events)
        
        feedback_integrated_events = [
            e for e in period_events
            if e.event_type == AuditEventType.FEEDBACK_INTEGRATED
        ]
        feedback_integrated = len(feedback_integrated_events)
        
        feedback_integration_rate = (
            feedback_integrated / feedback_received
            if feedback_received > 0 else 0
        )
        
        # Energy metrics
        total_energy = sum(
            e.energy_consumed_wh for e in period_events
            if e.energy_consumed_wh > 0
        )
        
        energy_saved = sum(
            abs(e.energy_consumed_wh) for e in period_events
            if e.energy_consumed_wh < 0
        )
        
        net_energy = total_energy - energy_saved
        
        # Carbon metrics
        total_carbon = sum(
            e.carbon_emitted_kg for e in period_events
            if e.carbon_emitted_kg > 0
        )
        
        carbon_saved = sum(
            abs(e.carbon_emitted_kg) for e in period_events
            if e.carbon_emitted_kg < 0
        )
        
        net_carbon = total_carbon - carbon_saved
        
        # Efficiency metrics
        avg_task_energy = total_energy / total_tasks if total_tasks > 0 else 0
        avg_expert_energy = (
            total_energy / expert_invocations
            if expert_invocations > 0 else 0
        )
        
        sustainability_improvement = (
            (energy_saved / total_energy * 100)
            if total_energy > 0 else 0
        )
        
        # Transparency score (0-1)
        transparency_score = self._calculate_transparency_score(
            feedback_integration_rate,
            escalation_rate,
            len(period_events)
        )
        
        return TransparencyReport(
            report_id=self._generate_report_id(),
            period_start=period_start,
            period_end=period_end,
            total_tasks=total_tasks,
            tasks_escalated=tasks_escalated,
            escalation_rate=escalation_rate,
            expert_invocations=expert_invocations,
            experts_by_type=experts_by_type,
            feedback_received=feedback_received,
            feedback_integrated=feedback_integrated,
            feedback_integration_rate=feedback_integration_rate,
            total_energy_consumed_wh=total_energy,
            energy_saved_wh=energy_saved,
            net_energy_wh=net_energy,
            total_carbon_kg=total_carbon,
            carbon_saved_kg=carbon_saved,
            net_carbon_kg=net_carbon,
            avg_task_energy_wh=avg_task_energy,
            avg_expert_energy_wh=avg_expert_energy,
            sustainability_improvement_pct=sustainability_improvement,
            audit_events=len(period_events),
            transparency_score=transparency_score
        )
    
    def export_report(
        self,
        report: TransparencyReport,
        filepath: str,
        format: str = "json"
    ):
        """Export transparency report to file."""
        report_dict = asdict(report)
        
        if format == "json":
            with open(filepath, 'w') as f:
                json.dump(report_dict, f, indent=2)
        elif format == "html":
            html = self._generate_html_report(report)
            with open(filepath, 'w') as f:
                f.write(html)
    
    def _add_event(self, event: AuditEvent):
        """Add event to log."""
        self.audit_events.append(event)
        
        # Persist if enabled
        if self.enable_persistence and self.log_file:
            self._append_to_file(event)
    
    def _generate_event_id(self) -> str:
        """Generate unique event ID."""
        timestamp = datetime.now().timestamp()
        content = f"event:{timestamp}"
        return hashlib.md5(content.encode()).hexdigest()[:16]
    
    def _generate_report_id(self) -> str:
        """Generate unique report ID."""
        timestamp = datetime.now().timestamp()
        content = f"report:{timestamp}"
        return hashlib.md5(content.encode()).hexdigest()[:16]
    
    def _calculate_transparency_score(
        self,
        integration_rate: float,
        escalation_rate: float,
        event_count: int
    ) -> float:
        """Calculate transparency score (0-1)."""
        # High integration rate = good
        # Moderate escalation rate = good (not too high or too low)
        # High event count = good (detailed logging)
        
        integration_score = integration_rate
        
        # Optimal escalation rate: 0.1-0.3 (10-30%)
        if 0.1 <= escalation_rate <= 0.3:
            escalation_score = 1.0
        elif escalation_rate < 0.1:
            escalation_score = escalation_rate / 0.1
        else:
            escalation_score = max(0, 1.0 - (escalation_rate - 0.3) / 0.7)
        
        # Event count score (saturates at 100 events)
        event_score = min(event_count / 100, 1.0)
        
        # Weighted combination
        score = (
            0.4 * integration_score +
            0.3 * escalation_score +
            0.3 * event_score
        )
        
        return score
    
    def _append_to_file(self, event: AuditEvent):
        """Append event to log file."""
        try:
            with open(self.log_file, 'a') as f:
                f.write(json.dumps(asdict(event)) + '\n')
        except Exception as e:
            print(f"Warning: Failed to write to log file: {e}")
    
    def _load_from_file(self):
        """Load events from log file."""
        try:
            with open(self.log_file, 'r') as f:
                for line in f:
                    event_dict = json.loads(line.strip())
                    event_dict['event_type'] = AuditEventType(event_dict['event_type'])
                    self.audit_events.append(AuditEvent(**event_dict))
        except FileNotFoundError:
            pass  # File doesn't exist yet
        except Exception as e:
            print(f"Warning: Failed to load from log file: {e}")
    
    def _generate_html_report(self, report: TransparencyReport) -> str:
        """Generate HTML transparency report."""
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Transparency Report - {report.report_id}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .metric {{ margin: 10px 0; }}
        .metric-value {{ font-weight: bold; color: #2c3e50; }}
        .section {{ margin: 30px 0; border-left: 4px solid #3498db; padding-left: 15px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #3498db; color: white; }}
    </style>
</head>
<body>
    <h1>Expert Collaboration Transparency Report</h1>
    <p>Report ID: {report.report_id}</p>
    <p>Period: {datetime.fromtimestamp(report.period_start).strftime('%Y-%m-%d %H:%M')} 
       to {datetime.fromtimestamp(report.period_end).strftime('%Y-%m-%d %H:%M')}</p>
    
    <div class="section">
        <h2>Task Metrics</h2>
        <div class="metric">Total Tasks: <span class="metric-value">{report.total_tasks}</span></div>
        <div class="metric">Tasks Escalated: <span class="metric-value">{report.tasks_escalated}</span></div>
        <div class="metric">Escalation Rate: <span class="metric-value">{report.escalation_rate:.1%}</span></div>
    </div>
    
    <div class="section">
        <h2>Expert Metrics</h2>
        <div class="metric">Expert Invocations: <span class="metric-value">{report.expert_invocations}</span></div>
        <div class="metric">Feedback Received: <span class="metric-value">{report.feedback_received}</span></div>
        <div class="metric">Feedback Integrated: <span class="metric-value">{report.feedback_integrated}</span></div>
        <div class="metric">Integration Rate: <span class="metric-value">{report.feedback_integration_rate:.1%}</span></div>
    </div>
    
    <div class="section">
        <h2>Sustainability Metrics</h2>
        <div class="metric">Total Energy Consumed: <span class="metric-value">{report.total_energy_consumed_wh*1000:.1f} mWh</span></div>
        <div class="metric">Energy Saved: <span class="metric-value">{report.energy_saved_wh*1000:.1f} mWh</span></div>
        <div class="metric">Net Energy: <span class="metric-value">{report.net_energy_wh*1000:.1f} mWh</span></div>
        <div class="metric">Carbon Saved: <span class="metric-value">{report.carbon_saved_kg*1000:.1f} g CO2</span></div>
        <div class="metric">Sustainability Improvement: <span class="metric-value">{report.sustainability_improvement_pct:.1f}%</span></div>
    </div>
    
    <div class="section">
        <h2>Transparency Score</h2>
        <div class="metric">Score: <span class="metric-value">{report.transparency_score:.2f} / 1.00</span></div>
    </div>
</body>
</html>
        """
        return html
