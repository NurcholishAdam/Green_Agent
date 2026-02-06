#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Demo: Meta-Cognitive Green Agent

Demonstrates the enhanced meta-cognitive architecture with reflection,
long-context reasoning, and sustained memory.
"""

import json
import time
from src.monitoring.metrics_collector import MetricsCollector
from src.reflection.reflection_engine import ReflectionEngine
from src.reflection.long_context_reasoner import LongContextReasoner
from src.policy.policy_engine import PolicyEngine
from src.policy.policy_feedback import PolicyFeedback
from src.analysis.pareto_analyzer import ParetoAnalyzer
from src.memory.run_memory import RunMemory
from src.dashboard.green_dashboard import GreenDashboard


def simulate_agent_execution(steps: int = 15):
    """Simulate agent execution with varying resource usage."""
    print("🌱 Green Agent Meta-Cognitive Demo")
    print("=" * 60)
    
    # Initialize components
    policy = PolicyEngine(policy_file="green_policy.yaml")
    metrics_collector = MetricsCollector()
    reflection_engine = ReflectionEngine(
        reflection_frequency=5,
        policy_budgets=policy.get_budgets()
    )
    long_context_reasoner = LongContextReasoner()
    run_memory = RunMemory()
    feedback_system = PolicyFeedback()
    pareto_analyzer = ParetoAnalyzer()
    dashboard = GreenDashboard()
    
    # Load historical context
    historical_runs = run_memory.get_recent_runs(3)
    for hist_run in historical_runs:
        long_context_reasoner.add_run_to_history(hist_run)
    
    print(f"📚 Loaded {len(historical_runs)} historical runs\n")
    
    all_reflections = []
    
    # Simulate execution steps
    for step in range(1, steps + 1):
        print(f"\n{'='*60}")
        print(f"Step {step}/{steps}")
        print(f"{'='*60}")
        
        metrics_collector.start_step()
        
        # Simulate work (varying resource usage)
        time.sleep(0.1)
        if step % 3 == 0:
            # Simulate higher resource usage every 3 steps
            time.sleep(0.05)
            metrics_collector.record_tool_call()
            metrics_collector.record_tool_call()
        else:
            metrics_collector.record_tool_call()
        
        # Collect metrics
        snapshot = metrics_collector.collect_snapshot()
        print(f"📊 Metrics: Energy={snapshot.energy_wh:.4f}Wh, "
              f"Latency={snapshot.latency_ms:.1f}ms, "
              f"Memory={snapshot.memory_mb:.1f}MB")
        
        # Reflection checkpoint
        if reflection_engine.should_reflect(step):
            print(f"\n🤔 REFLECTION CHECKPOINT")
            print("-" * 60)
            
            reflection_metrics = metrics_collector.get_metrics_for_reflection()
            reflection = reflection_engine.generate_reflection(
                step=step,
                metrics=reflection_metrics,
                timestamp=time.time()
            )
            
            print(f"💭 Self-Explanation:")
            print(f"   {reflection.self_explanation}")
            print(f"\n🎯 Decision: {reflection.decision}")
            print(f"📈 Confidence: {reflection.confidence:.2f}")
            
            # Check budget status
            violations = reflection.budget_status.get("violations", [])
            warnings = reflection.budget_status.get("warnings", [])
            if violations:
                print(f"⚠️  Violations: {violations}")
            if warnings:
                print(f"⚡ Warnings: {warnings}")
            
            all_reflections.append(reflection.to_dict())
            
            # Self-adjustment check
            if policy.should_self_adjust(reflection_metrics):
                print(f"\n⚙️  SELF-ADJUSTMENT TRIGGERED")
                adjustment = policy.apply_adaptive_adjustment(reflection.decision)
                print(f"   Changes: {adjustment.get('changes', [])}")
            
            # Long-context reasoning
            insights = long_context_reasoner.compare_with_past_runs(reflection_metrics)
            if insights:
                print(f"\n🔍 Long-Context Insights:")
                for insight in insights[:2]:  # Show top 2
                    print(f"   • {insight.description} (confidence: {insight.confidence:.2f})")
            
            # Pattern identification
            patterns = reflection_engine.identify_patterns()
            if patterns:
                print(f"\n📈 Patterns Identified:")
                for pattern in patterns:
                    print(f"   • {pattern}")
            
            print("-" * 60)
    
    print(f"\n\n{'='*60}")
    print("EXECUTION COMPLETE - GENERATING ANALYSIS")
    print(f"{'='*60}\n")
    
    # Final analysis
    cumulative = metrics_collector.get_cumulative_metrics()
    print(f"📊 Cumulative Metrics:")
    print(f"   Total Energy: {cumulative['total_energy_wh']:.4f} Wh")
    print(f"   Total Carbon: {cumulative['total_carbon_kg']:.6f} kg")
    print(f"   Total Latency: {cumulative['total_latency_ms']:.1f} ms")
    print(f"   Total Tool Calls: {cumulative['total_tool_calls']}")
    print(f"   Total Reflections: {len(all_reflections)}")
    
    # Store run in memory
    run_data = {
        "cumulative": cumulative,
        "reflections": all_reflections,
        "budget_status": all_reflections[-1]["budget_status"] if all_reflections else {}
    }
    run_memory.add_run(run_data)
    
    # Generate meta-policy
    print(f"\n🧬 Generating Meta-Policy...")
    meta_policy = run_memory.generate_meta_policy()
    if meta_policy:
        print(f"   Recommendations:")
        for rec in meta_policy.get("recommendations", []):
            print(f"   • {rec['metric']}: {rec['action']} - {rec['reason']}")
    
    # Long-term patterns
    print(f"\n🔬 Long-Term Pattern Analysis...")
    long_term_patterns = long_context_reasoner.identify_long_term_patterns()
    if long_term_patterns:
        for pattern in long_term_patterns:
            print(f"   • {pattern.description}")
    
    # Pareto analysis (simulate multiple agents)
    agents = [
        {"query_id": "demo_agent", **cumulative},
        {"query_id": "baseline", "total_energy_wh": cumulative["total_energy_wh"] * 1.2,
         "total_carbon_kg": cumulative["total_carbon_kg"] * 1.2,
         "total_latency_ms": cumulative["total_latency_ms"] * 0.8}
    ]
    
    pareto_position = pareto_analyzer.analyze_agent_position(agents[0], agents)
    print(f"\n🏆 Pareto Analysis:")
    print(f"   Position: {pareto_position['position']}")
    print(f"   Efficiency Score: {pareto_position['efficiency_score']:.3f}")
    
    # Dual-layer feedback
    print(f"\n📋 Dual-Layer Feedback:")
    dual_feedback = feedback_system.generate_dual_layer_feedback(
        pareto_analysis=pareto_position,
        reflections=all_reflections,
        metrics={"cumulative": cumulative}
    )
    print(f"   Alignment: {dual_feedback['synthesis']['alignment']}")
    print(f"   Synthesis: {dual_feedback['synthesis']['synthesis_text']}")
    
    # Dashboard
    dashboard.add_agent_data(
        agent_id="demo_agent",
        metrics={"cumulative": cumulative},
        reflections=all_reflections,
        pareto_position=pareto_position
    )
    
    # Export artifacts
    print(f"\n💾 Exporting Artifacts...")
    metrics_collector.export_history("demo_metrics_history.json")
    reflection_engine.export_reflections("demo_reflections.json")
    long_context_reasoner.export_reasoning_history("demo_reasoning_insights.json")
    dashboard.export_dashboard("demo_dashboard_data.json")
    dashboard.generate_html_report("demo_dashboard.html")
    
    print(f"   ✓ demo_metrics_history.json")
    print(f"   ✓ demo_reflections.json")
    print(f"   ✓ demo_reasoning_insights.json")
    print(f"   ✓ demo_dashboard_data.json")
    print(f"   ✓ demo_dashboard.html")
    
    # Historical summary
    print(f"\n📚 Historical Summary:")
    hist_summary = run_memory.get_historical_summary()
    print(f"   Total Runs: {hist_summary['total_runs']}")
    print(f"   Avg Energy/Run: {hist_summary['avg_energy_per_run']:.4f} Wh")
    print(f"   Meta-Policies Generated: {hist_summary['meta_policies_generated']}")
    
    print(f"\n{'='*60}")
    print("✅ Demo Complete!")
    print(f"{'='*60}")
    print(f"\n📊 Open demo_dashboard.html to view interactive results")


if __name__ == "__main__":
    simulate_agent_execution(steps=15)
