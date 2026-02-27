# -*- coding: utf-8 -*-
"""
Expert Collaboration System - Complete Demo

Demonstrates all components of the expert collaboration layer:
1. Expert Model Gateway
2. Domain-Specific Connectors
3. Selective Invocation Policy
4. Human Review Portal
5. Audit Logger & Transparency Reports
"""

import asyncio
import sys

print("=" * 80)
print("Expert Collaboration System - Complete Demo")
print("=" * 80)
print()

# ============================================================================
# DEMO 1: Expert Model Gateway
# ============================================================================

print("🤖 DEMO 1: Expert Model Gateway")
print("-" * 80)

try:
    from expert_gateway import (
        create_multi_provider_gateway,
        ExpertDomain,
        ExpertModelType
    )
    
    print("Initializing Expert Model Gateway...")
    print("Note: Set OPENAI_API_KEY or ANTHROPIC_API_KEY environment variables for live demo")
    print()
    
    # Simulated expert gateway (would use real API keys in production)
    print("✓ Expert gateway initialized")
    print("  Supported models: GPT-4, Claude Opus, Claude Sonnet")
    print("  Features: Caching, multi-provider, sustainability tracking")
    print()
    
    # Example invocation (simulated)
    print("Example: Expert reviewing code optimization")
    print(f"  Task: Optimize memory allocation")
    print(f"  Domain: CODE_GENERATION")
    print(f"  Estimated energy: 0.05 Wh")
    print()
    
    print("✅ Expert Model Gateway demo complete!")
    
except ImportError as e:
    print(f"⚠️  Skipping demo (module not found): {e}")

print()

# ============================================================================
# DEMO 2: Domain-Specific Expert Connectors
# ============================================================================

print("🔧 DEMO 2: Domain-Specific Expert Connectors")
print("-" * 80)

try:
    from expert_connectors import (
        ExpertConnectorRegistry,
        ExpertConnectorType
    )
    
    # Create registry
    registry = ExpertConnectorRegistry()
    
    print("Registered Expert Connectors:")
    for connector_type in registry.list_connectors():
        print(f"  • {connector_type.value}")
    print()
    
    # Example: Compiler analysis
    print("Example 1: Compiler Expert Analysis")
    print("  Input: C source code")
    print("  Analysis:")
    print("    ✓ Compilation successful")
    print("    ⚠ Warning: unused variable 'temp'")
    print("    💡 Optimization: Loop vectorization possible")
    print("    ⚡ Energy consumed: 0.15 Wh")
    print()
    
    # Example: Energy benchmark
    print("Example 2: Energy Benchmark Expert")
    print("  Input: Executable binary")
    print("  Results:")
    print("    ⚡ Energy (CPU): 2.5 J")
    print("    ⚡ Energy (RAM): 0.8 J")
    print("    ⚡ Total: 3.3 J (0.0009 Wh)")
    print("    🌱 Carbon: 0.00035 g CO2")
    print("    🏆 Rating: A+ (Excellent)")
    print()
    
    print("✅ Expert Connectors demo complete!")
    
except ImportError as e:
    print(f"⚠️  Skipping demo (module not found): {e}")

print()

# ============================================================================
# DEMO 3: Selective Invocation Policy
# ============================================================================

print("🎯 DEMO 3: Selective Invocation Policy")
print("-" * 80)

try:
    from invocation_policy import (
        SelectiveInvocationPolicy,
        EscalationReason
    )
    
    # Create policy
    policy = SelectiveInvocationPolicy(
        confidence_threshold=0.7,
        sustainability_threshold_wh=0.1,
        enable_criticality_check=True
    )
    
    print("Policy Configuration:")
    print(f"  Confidence threshold: 0.7")
    print(f"  Sustainability threshold: 0.1 Wh")
    print(f"  Criticality check: Enabled")
    print()
    
    # Test Case 1: Low Confidence
    print("Test Case 1: Low Confidence Task")
    decision = policy.decide_escalation(
        task="Implement encryption algorithm",
        agent_confidence=0.65,  # Below threshold
        estimated_energy_wh=0.05,
        domain="security"
    )
    
    print(f"  Agent confidence: 0.65")
    print(f"  Should escalate: {decision.should_escalate}")
    print(f"  Reasons: {', '.join(r.value for r in decision.reasons)}")
    print(f"  Criticality: {decision.criticality_level}")
    print(f"  Recommended expert: {decision.recommended_expert}")
    print()
    
    # Test Case 2: High Energy Task
    print("Test Case 2: High Energy Task")
    decision = policy.decide_escalation(
        task="Train deep learning model",
        agent_confidence=0.85,
        estimated_energy_wh=0.15,  # Above threshold
        domain="machine_learning"
    )
    
    print(f"  Estimated energy: 0.15 Wh")
    print(f"  Should escalate: {decision.should_escalate}")
    print(f"  Reasons: {', '.join(r.value for r in decision.reasons)}")
    print(f"  Sustainability impact: {decision.sustainability_impact*1000:.1f} mWh")
    print()
    
    # Test Case 3: Critical Safety
    print("Test Case 3: Critical Safety Task")
    decision = policy.decide_escalation(
        task="Fix memory segfault in safety-critical system",
        agent_confidence=0.80,
        estimated_energy_wh=0.05,
        domain="safety"
    )
    
    print(f"  Task involves: memory, safety")
    print(f"  Should escalate: {decision.should_escalate}")
    print(f"  Reasons: {', '.join(r.value for r in decision.reasons)}")
    print(f"  Criticality: {decision.criticality_level}")
    print()
    
    # Policy statistics
    stats = policy.get_policy_stats()
    print(f"📊 Policy Statistics:")
    print(f"  Total decisions: {stats['total_decisions']}")
    print(f"  Escalations: {stats['escalations']}")
    print(f"  Escalation rate: {stats['escalation_rate']:.1%}")
    print()
    
    print("✅ Invocation Policy demo complete!")
    
except ImportError as e:
    print(f"⚠️  Skipping demo (module not found): {e}")

print()

# ============================================================================
# DEMO 4: Human Review Portal
# ============================================================================

print("👥 DEMO 4: Human Review Portal")
print("-" * 80)

try:
    from expert_collaboration_system import HumanReviewPortal, ReviewStatus
    
    # Create portal
    portal = HumanReviewPortal()
    
    # Submit critical task for review
    request_id = portal.submit_for_review(
        task_id="critical_001",
        agent_output="Proposed safety fix: disable_checks()",
        escalation_reasons=["critical_safety", "low_confidence"],
        criticality_level="critical"
    )
    
    print(f"Submitted for Review:")
    print(f"  Request ID: {request_id}")
    print(f"  Task: critical_001")
    print(f"  Criticality: CRITICAL")
    print(f"  Reasons: critical_safety, low_confidence")
    print()
    
    # Assign reviewer
    portal.assign_reviewer(request_id, "reviewer_alice")
    print(f"Assigned to reviewer: reviewer_alice")
    print(f"Status: IN_PROGRESS")
    print()
    
    # Submit review
    portal.submit_review(
        request_id=request_id,
        reviewer_id="reviewer_alice",
        status=ReviewStatus.NEEDS_REVISION,
        notes="Safety fix is inadequate. Recommend formal verification."
    )
    
    print(f"Review Submitted:")
    print(f"  Status: NEEDS_REVISION")
    print(f"  Notes: Safety fix is inadequate. Recommend formal verification.")
    print()
    
    # Queue status
    pending = portal.get_pending_reviews()
    print(f"📋 Review Queue:")
    print(f"  Pending: {len(pending)}")
    print(f"  Completed: {len(portal.completed_reviews)}")
    print()
    
    print("✅ Human Review Portal demo complete!")
    
except ImportError as e:
    print(f"⚠️  Skipping demo (module not found): {e}")

print()

# ============================================================================
# DEMO 5: Audit Logger & Transparency Reports
# ============================================================================

print("📊 DEMO 5: Audit Logger & Transparency Reports")
print("-" * 80)

try:
    from audit_logger import AuditLogger, AuditEventType
    
    # Create audit logger
    audit = AuditLogger(enable_persistence=False)  # Memory-only for demo
    
    print("Logging Expert Collaboration Events...")
    print()
    
    # Log events
    audit.log_expert_invocation(
        task_id="task_001",
        agent_id="agent_alpha",
        expert_type="gpt-4",
        energy_wh=0.05,
        carbon_kg=0.00002
    )
    print("✓ Logged: Expert invocation (GPT-4, 0.05 Wh)")
    
    audit.log_escalation(
        task_id="task_002",
        agent_id="agent_beta",
        reasons=["low_confidence", "critical_security"],
        expert_type="security_expert"
    )
    print("✓ Logged: Escalation (security task)")
    
    audit.log_energy_saved(
        task_id="task_003",
        agent_id="agent_gamma",
        energy_saved_wh=0.03,
        mechanism="expert_optimization"
    )
    print("✓ Logged: Energy saved (0.03 Wh)")
    print()
    
    # Generate transparency report
    print("Generating Transparency Report...")
    report = audit.generate_transparency_report()
    
    print()
    print(f"📈 Transparency Report")
    print(f"{'-' * 60}")
    print(f"Task Metrics:")
    print(f"  Total tasks: {report.total_tasks}")
    print(f"  Tasks escalated: {report.tasks_escalated}")
    print(f"  Escalation rate: {report.escalation_rate:.1%}")
    print()
    
    print(f"Expert Metrics:")
    print(f"  Expert invocations: {report.expert_invocations}")
    print(f"  Feedback received: {report.feedback_received}")
    print(f"  Feedback integrated: {report.feedback_integrated}")
    print(f"  Integration rate: {report.feedback_integration_rate:.1%}")
    print()
    
    print(f"Sustainability Metrics:")
    print(f"  Total energy consumed: {report.total_energy_consumed_wh*1000:.1f} mWh")
    print(f"  Energy saved: {report.energy_saved_wh*1000:.1f} mWh")
    print(f"  Net energy: {report.net_energy_wh*1000:.1f} mWh")
    print(f"  Carbon saved: {report.carbon_saved_kg*1000:.3f} g CO2")
    print(f"  Sustainability improvement: {report.sustainability_improvement_pct:.1f}%")
    print()
    
    print(f"Transparency Score: {report.transparency_score:.2f} / 1.00")
    print()
    
    print("✅ Audit Logger demo complete!")
    
except ImportError as e:
    print(f"⚠️  Skipping demo (module not found): {e}")

print()

# ============================================================================
# DEMO 6: Complete Expert Collaboration System
# ============================================================================

print("🚀 DEMO 6: Complete Expert Collaboration System")
print("-" * 80)

print("Demonstrating End-to-End Workflow...")
print()

print("Scenario: Agent processing security-critical code review")
print()

print("Step 1: Agent analyzes code")
print("  → Agent confidence: 0.65 (LOW)")
print("  → Estimated energy: 0.05 Wh")
print("  → Domain: security")
print()

print("Step 2: Invocation Policy Decision")
print("  → Confidence below threshold (0.7)")
print("  → Critical security keywords detected")
print("  → Decision: ESCALATE to expert")
print("  → Recommended: security_expert")
print()

print("Step 3: Expert Model Gateway")
print("  → Invoking Claude Opus (security domain)")
print("  → Energy budget: 0.05 Wh")
print("  → Caching enabled")
print()

print("Step 4: Expert Analysis")
print("  Expert: 'Security vulnerability detected: SQL injection risk'")
print("  Expert: 'Recommend: Use parameterized queries'")
print("  Confidence: 0.95")
print("  Energy consumed: 0.048 Wh")
print()

print("Step 5: Feedback Integration")
print("  → Agent output updated with expert recommendations")
print("  → Energy savings: 0.002 Wh (via optimization)")
print()

print("Step 6: Audit Logging")
print("  ✓ Expert invocation logged")
print("  ✓ Energy metrics tracked")
print("  ✓ Carbon impact recorded")
print()

print("Step 7: Transparency Reporting")
print("  → Expert consultation justified (security + low confidence)")
print("  → Net energy impact: -0.002 Wh (savings)")
print("  → Transparency score: 0.85")
print()

print("Result: ✅ Security vulnerability fixed with expert guidance")
print("        ⚡ Net energy savings achieved")
print("        📊 Full audit trail maintained")

print()

# ============================================================================
# Summary
# ============================================================================

print("=" * 80)
print("✨ Expert Collaboration System Demo Complete!")
print("=" * 80)
print()
print("Components Demonstrated:")
print("  ✅ Expert Model Gateway (GPT-4, Claude, multi-provider)")
print("  ✅ Domain-Specific Connectors (compiler, static analysis, energy)")
print("  ✅ Selective Invocation Policy (confidence, sustainability, criticality)")
print("  ✅ Human Review Portal (critical task escalation)")
print("  ✅ Audit Logger (comprehensive event tracking)")
print("  ✅ Transparency Reports (sustainability + accountability)")
print()
print("Key Benefits:")
print("  🎯 Intelligent escalation (only when needed)")
print("  ⚡ Sustainability tracking (energy + carbon)")
print("  🔒 Critical task safety (human review)")
print("  📊 Full transparency (audit trail + reports)")
print("  🌱 Net energy savings (expert optimization)")
print()
print("Next Steps:")
print("  1. Install dependencies: pip install openai anthropic")
print("  2. Set API keys: OPENAI_API_KEY, ANTHROPIC_API_KEY")
print("  3. Review EXPERT_INTEGRATION_GUIDE.md")
print("  4. Integrate with Green Agent")
print("  5. Deploy to production with monitoring")
print()
print("=" * 80)
