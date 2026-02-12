#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enhanced Green_Agent with Meta-Cognitive Architecture

Canonical AgentBeats entrypoint with sustained reflection and interpretability.
Never throws uncaught exceptions.
"""

import json
import time
import argparse
import yaml
from typing import Dict, Any

from src.policy.policy_engine import PolicyEngine
from src.policy.policy_feedback import PolicyFeedback
from src.analysis.pareto_analyzer import ParetoAnalyzer
from src.runtime.langchain_runtime import LangChainRuntime
from src.runtime.autogen_runtime import AutoGenRuntime
from src.monitoring.metrics_collector import MetricsCollector
from src.reflection.reflection_engine import ReflectionEngine
from src.reflection.long_context_reasoner import LongContextReasoner
from src.memory.run_memory import RunMemory
from src.dashboard.green_dashboard import GreenDashboard
from src.dashboard.symbolic_visualizer import SymbolicVisualizer
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine
from src.chaos import inject_energy_spike

# -------------------------
# Runtime factory
# -------------------------
def create_runtime(name: str):
    """Create runtime instance based on framework name."""
    if name == "langchain":
        return LangChainRuntime()
    if name == "autogen":
        return AutoGenRuntime()
    raise ValueError(f"Unknown runtime: {name}")

# -------------------------
# Main execution with meta-cognitive enhancements
# -------------------------
def main():
    parser = argparse.ArgumentParser(description="Green Agent with Meta-Cognitive Architecture")
    parser.add_argument("--config", required=True, help="Path to configuration JSON")
    parser.add_argument("--policy", default="green_policy.yaml", help="Path to policy YAML")
    parser.add_argument("--output", default="agentbeats_results.json", help="Output results file")
    parser.add_argument("--dashboard", default="dashboard.html", help="Dashboard HTML output")
    parser.add_argument("--memory", default="run_memory.json", help="Persistent memory file")
    args = parser.parse_args()

    print("🌱 Green Agent - Meta-Cognitive Architecture v2.0")
    print("=" * 60)

    # Load configuration
    with open(args.config) as f:
        cfg = json.load(f)

    # Initialize components
    runtime = create_runtime(cfg["framework"])
    runtime.init(cfg.get("runtime", {}))

    # Load policy
    policy = PolicyEngine(policy_file=args.policy)
    budgets = policy.get_budgets()
    print(f"📋 Policy loaded: {budgets}")

    # Initialize meta-cognitive components
    metrics_collector = MetricsCollector(
        grid_intensity_g_kwh=cfg.get("grid_intensity", 385.0),
        pue_factor=cfg.get("pue_factor", 1.2)
    )
    
    reflection_engine = ReflectionEngine(
        reflection_frequency=policy.get_reflection_frequency(),
        policy_budgets=budgets
    )
    
    long_context_reasoner = LongContextReasoner(history_window=10)
    run_memory = RunMemory(memory_file=args.memory)
    feedback_system = PolicyFeedback()
    pareto_analyzer = ParetoAnalyzer()
    dashboard = GreenDashboard()
    
    # Initialize symbolic reasoning engine
    symbolic_engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")
    symbolic_visualizer = SymbolicVisualizer()
    print(f"🔍 Symbolic reasoning engine loaded with {len(symbolic_engine.get_active_rules())} rules")

    # Load historical context
    historical_runs = run_memory.get_recent_runs(5)
    for hist_run in historical_runs:
        long_context_reasoner.add_run_to_history(hist_run)
    
    print(f"🧠 Loaded {len(historical_runs)} historical runs for context")

    all_metrics = []
    all_reflections = []

    # Execute queries with meta-cognitive monitoring
    for step, query in enumerate(cfg["queries"], start=1):
        print(f"\n🔄 Step {step}: Processing query '{query.get('id', 'unknown')}'")
        
        metrics_collector.start_step()
        start_time = time.time()

        try:
            # Execute query
            result = runtime.run(query)
            metrics_collector.record_tool_call()

            # Collect metrics snapshot
            snapshot = metrics_collector.collect_snapshot()
            
            # Build comprehensive metrics
            metrics = {
                "query_id": query.get("id"),
                "step": step,
                "timestamp": snapshot.timestamp,
                "latency": snapshot.latency_ms / 1000.0,  # Convert to seconds
                "energy": snapshot.energy_wh,
                "carbon": snapshot.carbon_kg,
                "memory_mb": snapshot.memory_mb,
                "tool_calls": snapshot.tool_calls,
                **result,
            }

            # Apply chaos engineering (optional)
            if cfg.get("chaos_enabled", False):
                metrics = inject_energy_spike(metrics, probability=0.1)

            # Enforce policy
            enforcement_result = policy.enforce(metrics)
            metrics["policy_enforcement"] = enforcement_result

            # Get cumulative metrics for reflection
            cumulative_metrics = metrics_collector.get_cumulative_metrics()
            metrics["cumulative"] = cumulative_metrics
            
            # Evaluate symbolic rules at each step
            symbolic_violations = symbolic_engine.evaluate_rules(
                metrics=metrics,
                step=step,
                domain=query.get("domain", None)
            )
            
            if symbolic_violations:
                print(f"  ⚠️  {len(symbolic_violations)} symbolic rule violation(s) detected")
                for violation in symbolic_violations:
                    print(f"     - {violation.rule_name} [{violation.severity}]")
                
                metrics["symbolic_violations"] = [v.to_dict() for v in symbolic_violations]
                symbolic_visualizer.add_violations([v.to_dict() for v in symbolic_violations])

            # Reflection checkpoint
            if reflection_engine.should_reflect(step):
                print(f"  🤔 Reflection checkpoint at step {step}")
                
                # Get metrics formatted for reflection
                reflection_metrics = metrics_collector.get_metrics_for_reflection()
                
                # Generate reflection
                reflection = reflection_engine.generate_reflection(
                    step=step,
                    metrics=reflection_metrics,
                    timestamp=time.time()
                )
                
                print(f"  💭 Self-explanation: {reflection.self_explanation}")
                print(f"  🎯 Decision: {reflection.decision}")
                print(f"  📊 Confidence: {reflection.confidence:.2f}")
                
                # Store reflection
                all_reflections.append(reflection.to_dict())
                metrics["reflection"] = reflection.to_dict()
                
                # Check for self-adjustment
                if policy.should_self_adjust(reflection_metrics):
                    print(f"  ⚙️  Self-adjustment triggered")
                    adjustment = policy.apply_adaptive_adjustment(reflection.decision)
                    metrics["policy_adjustment"] = adjustment
                    print(f"  ✓ Applied: {adjustment.get('changes', [])}")
                
                # Long-context reasoning
                insights = long_context_reasoner.compare_with_past_runs(reflection_metrics)
                if insights:
                    print(f"  🔍 Generated {len(insights)} insights from historical comparison")
                    metrics["long_context_insights"] = [
                        {
                            "type": i.insight_type,
                            "description": i.description,
                            "confidence": i.confidence
                        }
                        for i in insights
                    ]
                
                # Identify patterns
                patterns = reflection_engine.identify_patterns()
                if patterns:
                    print(f"  📈 Identified patterns: {patterns}")
                    metrics["patterns"] = patterns

            all_metrics.append(metrics)

        except Exception as e:
            print(f"  ❌ Error: {str(e)}")
            all_metrics.append({
                "query_id": query.get("id"),
                "step": step,
                "error": str(e),
            })
            break

    print("\n" + "=" * 60)
    print("📊 Analyzing results...")

    # Pareto analysis
    frontier = pareto_analyzer.pareto_frontier(all_metrics)
    print(f"✓ Pareto frontier: {len(frontier)} agent(s)")

    # Generate dual-layer feedback
    for i, metrics in enumerate(all_metrics):
        if "error" not in metrics:
            pareto_position = pareto_analyzer.analyze_agent_position(metrics, all_metrics)
            agent_reflections = [r for r in all_reflections if r.get("step", 0) <= metrics.get("step", 0)]
            symbolic_violations = metrics.get("symbolic_violations", [])
            
            dual_feedback = feedback_system.generate_dual_layer_feedback(
                pareto_analysis=pareto_position,
                reflections=agent_reflections,
                metrics=metrics,
                symbolic_violations=symbolic_violations
            )
            metrics["dual_layer_feedback"] = dual_feedback
            
            # Add to dashboard
            dashboard.add_agent_data(
                agent_id=metrics.get("query_id", f"agent_{i}"),
                metrics=metrics,
                reflections=agent_reflections,
                pareto_position=pareto_position
            )

    # Generate meta-policy from historical data
    run_data = {
        "cumulative": metrics_collector.get_cumulative_metrics(),
        "reflections": all_reflections,
        "budget_status": reflection_engine.reflections[-1].budget_status if reflection_engine.reflections else {}
    }
    run_memory.add_run(run_data)
    
    meta_policy = run_memory.generate_meta_policy()
    if meta_policy:
        print(f"🧬 Generated meta-policy with {len(meta_policy.get('recommendations', []))} recommendations")

    # Long-term pattern analysis
    long_term_patterns = long_context_reasoner.identify_long_term_patterns()
    if long_term_patterns:
        print(f"🔬 Identified {len(long_term_patterns)} long-term patterns")

    # Compile output
    output = {
        "framework": cfg["framework"],
        "queries": cfg["queries"],
        "results": all_metrics,
        "pareto_frontier": frontier,
        "reflections": all_reflections,
        "symbolic_oversight": {
            "total_violations": len(symbolic_engine.violation_history),
            "summary": symbolic_engine.get_violation_summary(),
            "violations_by_category": {
                cat: len(symbolic_engine.get_violations_by_category(cat))
                for cat in ['sustainability', 'resource', 'fairness', 'safety', 'compliance']
            }
        },
        "meta_cognitive_summary": {
            "total_reflections": len(all_reflections),
            "patterns_identified": reflection_engine.pattern_memory,
            "long_term_patterns": [
                {"type": p.insight_type, "description": p.description}
                for p in long_term_patterns
            ],
            "meta_policy": meta_policy,
            "policy_enforcement": policy.get_enforcement_summary(),
            "feedback_summary": feedback_system.get_feedback_summary(),
            "historical_summary": run_memory.get_historical_summary()
        }
    }

    # Save results
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)
    print(f"✓ Results saved to {args.output}")

    # Export additional artifacts
    metrics_collector.export_history("metrics_history.json")
    reflection_engine.export_reflections("reflections.json")
    long_context_reasoner.export_reasoning_history("reasoning_insights.json")
    pareto_analyzer.export_analysis(all_metrics, "pareto_analysis.json")
    symbolic_engine.export_violations("symbolic_violations.json")
    symbolic_visualizer.export_violation_report("symbolic_violation_report.json")
    
    # Generate dashboard with symbolic violations
    dashboard.export_dashboard("dashboard_data.json")
    dashboard.generate_html_report(args.dashboard)
    
    # Add symbolic violations section to dashboard
    symbolic_html = symbolic_visualizer.generate_dashboard_section()
    with open(args.dashboard, 'a') as f:
        f.write(symbolic_html)
    
    print(f"✓ Dashboard generated: {args.dashboard}")

    # Cleanup
    runtime.finalize()
    
    print("\n" + "=" * 60)
    print("✅ Green_Agent execution complete with neuro-symbolic oversight")
    print(f"📈 Total reflections: {len(all_reflections)}")
    print(f"🎯 Pareto frontier size: {len(frontier)}")
    print(f"🧠 Historical runs: {run_memory.get_historical_summary()['total_runs']}")
    print(f"🔍 Symbolic violations: {len(symbolic_engine.violation_history)}")
    
    # Print critical violations summary
    critical_violations = [v for v in symbolic_engine.violation_history if v.severity == 'critical']
    if critical_violations:
        print(f"⚠️  CRITICAL: {len(critical_violations)} critical rule violation(s) detected!")
        for v in critical_violations[:3]:  # Show first 3
            print(f"   - {v.rule_name} at step {v.step}")

if __name__ == "__main__":
    main()
