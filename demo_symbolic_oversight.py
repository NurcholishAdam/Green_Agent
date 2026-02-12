#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Demo: Neuro-Symbolic Oversight for Green Agent

Demonstrates the symbolic reasoning engine with formal rule evaluation
and violation trace generation.
"""

import json
from src.symbolic.symbolic_reasoning_engine import SymbolicReasoningEngine
from src.dashboard.symbolic_visualizer import SymbolicVisualizer


def demo_basic_rule_evaluation():
    """Demonstrate basic symbolic rule evaluation."""
    print("=" * 60)
    print("Demo 1: Basic Symbolic Rule Evaluation")
    print("=" * 60)
    
    # Initialize engine
    engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")
    
    # Simulate metrics that violate rules
    test_metrics = {
        "energy": 6.0,  # Exceeds 5.0 limit
        "carbon": 0.0025 * 1000,  # 2.5g (exceeds 2.0g limit when converted)
        "latency": 150000,  # 150 seconds (exceeds 120s limit)
        "memory": 600,  # Exceeds 500MB limit
        "tool_calls": 60,  # Exceeds 50 limit
        "cpu_percent": 85,
        "cumulative": {
            "total_energy_wh": 6.0,
            "total_carbon_kg": 0.0025,
            "total_latency_ms": 150000,
            "max_memory_mb": 600,
            "total_tool_calls": 60,
            "step_count": 10
        }
    }
    
    # Evaluate rules
    violations = engine.evaluate_rules(test_metrics, step=1)
    
    print(f"\n✓ Evaluated {len(engine.get_active_rules())} rules")
    print(f"⚠️  Found {len(violations)} violation(s)\n")
    
    # Display violations
    for violation in violations:
        print(f"Rule: {violation.rule_name} ({violation.rule_id})")
        print(f"Severity: {violation.severity}")
        print(f"Condition: {violation.condition}")
        print(f"Action: {violation.action_triggered}")
        print(f"Explanation: {violation.explanation}")
        print("-" * 60)
    
    return engine, violations


def demo_violation_traces():
    """Demonstrate formal violation trace generation."""
    print("\n" + "=" * 60)
    print("Demo 2: Formal Violation Traces")
    print("=" * 60)
    
    engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")
    
    # Simulate a scenario with carbon and latency issues
    metrics = {
        "energy": 4.5,
        "carbon": 65,  # High carbon in grams
        "latency": 2500,  # High latency in ms
        "memory": 300,
        "tool_calls": 25,
        "cumulative": {
            "total_energy_wh": 4.5,
            "total_carbon_kg": 0.065,
            "total_latency_ms": 2500,
            "max_memory_mb": 300,
            "total_tool_calls": 25,
            "step_count": 5
        }
    }
    
    violations = engine.evaluate_rules(metrics, step=5)
    
    print(f"\n✓ Generated {len(violations)} violation trace(s)\n")
    
    for violation in violations:
        print("Formal Trace:")
        print(violation.violation_details)
        print("\n" + "=" * 60 + "\n")
    
    return violations


def demo_category_filtering():
    """Demonstrate filtering violations by category."""
    print("=" * 60)
    print("Demo 3: Category-Based Filtering")
    print("=" * 60)
    
    engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")
    
    # Create violations across multiple categories
    test_scenarios = [
        {
            "name": "High Energy",
            "metrics": {
                "energy": 6.0,
                "carbon": 50,
                "latency": 1000,
                "memory": 200,
                "tool_calls": 10,
                "cumulative": {
                    "total_energy_wh": 6.0,
                    "total_carbon_kg": 0.05,
                    "total_latency_ms": 1000,
                    "max_memory_mb": 200,
                    "total_tool_calls": 10,
                    "step_count": 3
                }
            }
        },
        {
            "name": "Memory Overflow",
            "metrics": {
                "energy": 2.0,
                "carbon": 20,
                "latency": 1000,
                "memory": 550,
                "tool_calls": 10,
                "cumulative": {
                    "total_energy_wh": 2.0,
                    "total_carbon_kg": 0.02,
                    "total_latency_ms": 1000,
                    "max_memory_mb": 550,
                    "total_tool_calls": 10,
                    "step_count": 3
                }
            }
        }
    ]
    
    for scenario in test_scenarios:
        print(f"\nScenario: {scenario['name']}")
        violations = engine.evaluate_rules(scenario['metrics'], step=1)
        print(f"  Violations: {len(violations)}")
        for v in violations:
            category = v.rule_id.split('-')[0]
            print(f"    - [{category}] {v.rule_name}")
    
    # Show summary by category
    print("\n" + "-" * 60)
    print("Summary by Category:")
    summary = engine.get_violation_summary()
    for category, count in summary.get('by_category', {}).items():
        print(f"  {category.upper()}: {count} violation(s)")
    
    return engine


def demo_dashboard_visualization():
    """Demonstrate dashboard visualization of violations."""
    print("\n" + "=" * 60)
    print("Demo 4: Dashboard Visualization")
    print("=" * 60)
    
    engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")
    visualizer = SymbolicVisualizer()
    
    # Generate some violations
    metrics = {
        "energy": 5.5,
        "carbon": 70,
        "latency": 130000,
        "memory": 520,
        "tool_calls": 55,
        "cumulative": {
            "total_energy_wh": 5.5,
            "total_carbon_kg": 0.07,
            "total_latency_ms": 130000,
            "max_memory_mb": 520,
            "total_tool_calls": 55,
            "step_count": 8
        }
    }
    
    violations = engine.evaluate_rules(metrics, step=8)
    
    # Add to visualizer
    visualizer.add_violations([v.to_dict() for v in violations])
    
    # Generate views
    timeline = visualizer.generate_violation_timeline()
    category_view = visualizer.generate_category_view()
    severity_summary = visualizer.generate_severity_summary()
    
    print(f"\n✓ Generated visualization data")
    print(f"  Timeline entries: {len(timeline)}")
    print(f"  Categories: {len(category_view)}")
    print(f"  Severity breakdown: {severity_summary['counts']}")
    
    # Export HTML section
    html_section = visualizer.generate_dashboard_section()
    with open("demo_symbolic_dashboard.html", "w") as f:
        f.write(f"""
<!DOCTYPE html>
<html>
<head>
    <title>Symbolic Oversight Demo</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .violation-card {{ margin: 15px 0; }}
    </style>
</head>
<body>
    <h1>Green Agent - Symbolic Oversight Demo</h1>
    {html_section}
</body>
</html>
        """)
    
    print(f"✓ Dashboard HTML saved to: demo_symbolic_dashboard.html")
    
    return visualizer


def demo_sustained_reflection():
    """Demonstrate sustained reflection with symbolic violations."""
    print("\n" + "=" * 60)
    print("Demo 5: Sustained Reflection with Symbolic Oversight")
    print("=" * 60)
    
    engine = SymbolicReasoningEngine(policy_file="symbolic_policy.yaml")
    
    # Simulate multiple steps with evolving metrics
    steps = [
        {"step": 1, "energy": 3.0, "carbon": 40, "latency": 50000, "memory": 200},
        {"step": 2, "energy": 4.0, "carbon": 50, "latency": 80000, "memory": 300},
        {"step": 3, "energy": 5.5, "carbon": 65, "latency": 120000, "memory": 450},
        {"step": 4, "energy": 6.0, "carbon": 75, "latency": 140000, "memory": 550},
    ]
    
    print("\nTracking violations across execution steps:\n")
    
    for step_data in steps:
        metrics = {
            **step_data,
            "tool_calls": step_data["step"] * 10,
            "cumulative": {
                "total_energy_wh": step_data["energy"],
                "total_carbon_kg": step_data["carbon"] / 1000,
                "total_latency_ms": step_data["latency"],
                "max_memory_mb": step_data["memory"],
                "total_tool_calls": step_data["step"] * 10,
                "step_count": step_data["step"]
            }
        }
        
        violations = engine.evaluate_rules(metrics, step=step_data["step"])
        
        print(f"Step {step_data['step']}:")
        print(f"  Energy: {step_data['energy']:.1f} Wh | Carbon: {step_data['carbon']}g")
        print(f"  Violations: {len(violations)}")
        
        if violations:
            for v in violations:
                print(f"    ⚠️  {v.rule_name} [{v.severity}]")
        else:
            print(f"    ✅ No violations")
        print()
    
    # Show pattern analysis
    print("-" * 60)
    print("Pattern Analysis:")
    summary = engine.get_violation_summary()
    print(f"  Total evaluations: {summary['evaluations']}")
    print(f"  Total violations: {summary['total_violations']}")
    print(f"  Violation rate: {summary['violation_rate']:.2%}")
    print(f"\nMeta-insight: Agent shows degrading compliance over time")
    print("Recommendation: Implement adaptive throttling at step 3")
    
    return engine


def main():
    """Run all demos."""
    print("\n🌱 Green Agent - Neuro-Symbolic Oversight Demo")
    print("Inspired by FormalJudge paradigm\n")
    
    # Run demos
    engine1, violations1 = demo_basic_rule_evaluation()
    violations2 = demo_violation_traces()
    engine3 = demo_category_filtering()
    visualizer = demo_dashboard_visualization()
    engine5 = demo_sustained_reflection()
    
    # Final summary
    print("\n" + "=" * 60)
    print("✅ Demo Complete")
    print("=" * 60)
    print("\nKey Features Demonstrated:")
    print("  ✓ Symbolic rule evaluation")
    print("  ✓ Formal violation traces")
    print("  ✓ Category-based filtering")
    print("  ✓ Dashboard visualization")
    print("  ✓ Sustained reflection patterns")
    print("\nNext Steps:")
    print("  1. Review symbolic_policy.yaml to customize rules")
    print("  2. Run: python run_agent.py --config example_config.json")
    print("  3. Check dashboard.html for symbolic violations")
    print("  4. Review symbolic_violations.json for detailed traces")


if __name__ == "__main__":
    main()
