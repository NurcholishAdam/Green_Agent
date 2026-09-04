#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Demo: Meta-Cognitive Green Agent

Demonstrates the enhanced meta-cognitive architecture with reflection,
long-context reasoning, and sustained memory.

Enhanced with optional integration of advanced modules:
- LIMIT Graph
- MODP (Multi‑Objective Decision Process)
- RLHF (Reinforcement Learning from Human Feedback)
- Multi‑Teacher On‑Policy Distillation with MoE gating
- Bio‑inspired Optimisation
- FlexGen integration hooks
"""

import json
import time
import asyncio
import importlib
from src.monitoring.metrics_collector import MetricsCollector
from src.reflection.reflection_engine import ReflectionEngine
from src.reflection.long_context_reasoner import LongContextReasoner
from src.policy.policy_engine import PolicyEngine
from src.policy.policy_feedback import PolicyFeedback
from src.analysis.pareto_analyzer import ParetoAnalyzer
from src.memory.run_memory import RunMemory
from src.dashboard.green_dashboard import GreenDashboard

# ------------------------------------------------------------------------------
# Optional imports for advanced enhancements (graceful degradation)
# ------------------------------------------------------------------------------
ENHANCEMENTS_AVAILABLE = True
try:
    from src.enhancements.schemas.node_descriptor import NodeDescriptor, NodeType, CoolingType, RoutingStrategy
    from src.enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency, Priority
    from src.enhancements.schemas.feedback_event import FeedbackEvent
    from src.enhancements.zero_trust_architecture import ZeroTrustArchitecture, ZeroTrustConfig
    from src.enhancements.core.graph_registry import GraphRegistry, GraphType
    from src.enhancements.core.causal_graph import CausalGraph
    from src.enhancements.core.meta_cognition import MetaCognitionLayer
    from src.enhancements.metrics.dag_carbon_ledger import DAGCarbonLedger
except ImportError:
    ENHANCEMENTS_AVAILABLE = False
    NodeDescriptor = None
    WorkloadDescriptor = None
    FeedbackEvent = None
    ZeroTrustArchitecture = None
    ZeroTrustConfig = None
    GraphRegistry = None
    GraphType = None
    CausalGraph = None
    MetaCognitionLayer = None
    DAGCarbonLedger = None


def _demonstrate_enhancements():
    """
    Optional demonstration of advanced modules.
    Runs if ENHANCEMENTS_AVAILABLE is True.
    """
    print("\n" + "=" * 60)
    print("ADVANCED ENHANCEMENTS DEMO (Optional)")
    print("=" * 60)

    # 1. Node Descriptor with distillation + MoE + RLHF + LIMIT Graph
    print("\n1. Creating NodeDescriptor with LIMIT Graph, RLHF, MoE, evolutionary...")
    node = NodeDescriptor(
        id="demo_meta_node",
        type=NodeType.EDGE,
        region="us-east",
        region_carbon_intensity=350.0,
        energy_per_token=0.00004,
        use_evolutionary=True,
        human_feedback_score=0.7,
        graph_metrics={"centrality": 0.8, "connectivity": 0.6},
        metadata={"gating_lr": 0.005}
    )
    print(f"   ✓ NodeDescriptor created: {node.id}")
    print(f"     - Graph centrality: {node.graph_metrics['centrality']}")
    print(f"     - Human feedback: {node.human_feedback_score}")

    # 2. Select routing strategy
    print("\n2. Selecting routing strategy (distillation + MoE)...")
    strategy = asyncio.run(node.select_routing_strategy(exploration=False))
    print(f"   ✓ Selected strategy: {strategy}")

    # 3. Workload Descriptor with MODP weights
    print("\n3. Creating WorkloadDescriptor with MODP weights and RLHF...")
    wl = WorkloadDescriptor(
        task_id="meta_demo_task",
        task_type=TaskType.INFERENCE,
        tokens=1000,
        latency_target=300.0,
        urgency=Urgency.MEDIUM,
        use_evolutionary=True,
        human_feedback_score=0.6,
        graph_metrics={"centrality": 0.7},
        metadata={"latency_weight": 0.5, "carbon_weight": 0.3, "energy_weight": 0.2}
    )
    print(f"   ✓ WorkloadDescriptor created: {wl.task_id}")

    # 4. Select priority
    print("\n4. Selecting priority (distillation + MoE)...")
    priority = asyncio.run(wl.select_priority(exploration=False))
    print(f"   ✓ Selected priority: {priority}")

    # 5. FeedbackEvent with enhanced fields
    print("\n5. Creating FeedbackEvent with enhanced fields...")
    event = FeedbackEvent(
        source="meta_cognitive_demo",
        feedback_type="routing",
        task_id="demo_enhanced",
        context={"region": "us-east"},
        action={"selected_action": strategy, "selected_rank": 1},
        performance={"quality_score": 0.9, "latency_ms": 100, "energy_joules": 100,
                     "carbon_g": 5, "helium_cost": 0, "duration_ms": 100},
        adaptive_cost_value=0.85,
        graph_metrics={"centrality": 0.8},
        human_feedback_score=0.7,
        modp_score=0.75,
        distillation_stats={"student_counter": 5}
    )
    print(f"   ✓ FeedbackEvent created: {event.event_id[:8]}...")
    print(f"     - MODP score: {event.modp_score}")
    print(f"     - Graph centrality: {event.graph_metrics['centrality']}")

    # 6. Zero Trust with enhancements
    print("\n6. Initializing Zero Trust with enhancements...")
    zt_config = ZeroTrustConfig(use_enhancements=True, use_distillation=True, use_evolutionary=True)
    zta = ZeroTrustArchitecture(config=zt_config)
    print("   ✓ ZeroTrustArchitecture initialized")
    print(f"     - Enhancements enabled: {zta.use_enhancements}")

    # 7. LIMIT Graph + Meta-Cognition
    print("\n7. Using LIMIT Graph (CausalGraph + MetaCognition)...")
    registry = GraphRegistry()
    causal_graph = registry.get_or_create(GraphType.CAUSAL)
    meta = MetaCognitionLayer(causal_graph=causal_graph)
    meta.observe_snapshot({
        "CarbonIntensity": 430.0,
        "CarbonIntensity_high": 400.0,
        "GridStrain": 0.91,
    })
    report = meta.diagnose()
    print(f"   ✓ Diagnosis status: {report['status']}")
    print(f"     Recommended action: {report.get('recommended_action')}")

    # 8. DAG Carbon Ledger
    print("\n8. Using DAG Carbon Ledger for carbon backpropagation...")
    ledger = DAGCarbonLedger(storage_path="/tmp/meta_demo_ledger")
    node_a = ledger.add_execution(
        task_id="A", framework="langchain", energy_kwh=0.01,
        carbon_co2e_kg=0.002, accuracy=0.9, sustainability_index=0.8
    )
    node_b = ledger.add_execution(
        task_id="B", framework="langchain", energy_kwh=0.008,
        carbon_co2e_kg=0.0016, accuracy=0.88, sustainability_index=0.75,
        parent_task_ids=[node_a], dependency_type="model_state"
    )
    attributed = ledger.backpropagate_carbon(node_b, transfer_rate=0.3)
    print(f"   ✓ Carbon backpropagated: {attributed}")

    # 9. FlexGen config (simulated)
    print("\n9. FlexGen integration hooks (simulated)...")
    flexgen_config = {
        "enabled": True,
        "model_name": "facebook/opt-6.7b",
        "batch_size": 16,
        "delegation_policy": "adaptive"
    }
    print(f"   ✓ FlexGen config: {flexgen_config}")

    print("\n" + "=" * 60)
    print("✅ Advanced Enhancements demo completed successfully!")
    print("=" * 60 + "\n")


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
            
            violations = reflection.budget_status.get("violations", [])
            warnings = reflection.budget_status.get("warnings", [])
            if violations:
                print(f"⚠️  Violations: {violations}")
            if warnings:
                print(f"⚡ Warnings: {warnings}")
            
            all_reflections.append(reflection.to_dict())
            
            if policy.should_self_adjust(reflection_metrics):
                print(f"\n⚙️  SELF-ADJUSTMENT TRIGGERED")
                adjustment = policy.apply_adaptive_adjustment(reflection.decision)
                print(f"   Changes: {adjustment.get('changes', [])}")
            
            insights = long_context_reasoner.compare_with_past_runs(reflection_metrics)
            if insights:
                print(f"\n🔍 Long-Context Insights:")
                for insight in insights[:2]:
                    print(f"   • {insight.description} (confidence: {insight.confidence:.2f})")
            
            patterns = reflection_engine.identify_patterns()
            if patterns:
                print(f"\n📈 Patterns Identified:")
                for pattern in patterns:
                    print(f"   • {pattern}")
            
            print("-" * 60)
    
    print(f"\n\n{'='*60}")
    print("EXECUTION COMPLETE - GENERATING ANALYSIS")
    print(f"{'='*60}\n")
    
    cumulative = metrics_collector.get_cumulative_metrics()
    print(f"📊 Cumulative Metrics:")
    print(f"   Total Energy: {cumulative['total_energy_wh']:.4f} Wh")
    print(f"   Total Carbon: {cumulative['total_carbon_kg']:.6f} kg")
    print(f"   Total Latency: {cumulative['total_latency_ms']:.1f} ms")
    print(f"   Total Tool Calls: {cumulative['total_tool_calls']}")
    print(f"   Total Reflections: {len(all_reflections)}")
    
    run_data = {
        "cumulative": cumulative,
        "reflections": all_reflections,
        "budget_status": all_reflections[-1]["budget_status"] if all_reflections else {}
    }
    run_memory.add_run(run_data)
    
    print(f"\n🧬 Generating Meta-Policy...")
    meta_policy = run_memory.generate_meta_policy()
    if meta_policy:
        print(f"   Recommendations:")
        for rec in meta_policy.get("recommendations", []):
            print(f"   • {rec['metric']}: {rec['action']} - {rec['reason']}")
    
    print(f"\n🔬 Long-Term Pattern Analysis...")
    long_term_patterns = long_context_reasoner.identify_long_term_patterns()
    if long_term_patterns:
        for pattern in long_term_patterns:
            print(f"   • {pattern.description}")
    
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
    
    print(f"\n📋 Dual-Layer Feedback:")
    dual_feedback = feedback_system.generate_dual_layer_feedback(
        pareto_analysis=pareto_position,
        reflections=all_reflections,
        metrics={"cumulative": cumulative}
    )
    print(f"   Alignment: {dual_feedback['synthesis']['alignment']}")
    print(f"   Synthesis: {dual_feedback['synthesis']['synthesis_text']}")
    
    dashboard.add_agent_data(
        agent_id="demo_agent",
        metrics={"cumulative": cumulative},
        reflections=all_reflections,
        pareto_position=pareto_position
    )
    
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
    
    print(f"\n📚 Historical Summary:")
    hist_summary = run_memory.get_historical_summary()
    print(f"   Total Runs: {hist_summary['total_runs']}")
    print(f"   Avg Energy/Run: {hist_summary['avg_energy_per_run']:.4f} Wh")
    print(f"   Meta-Policies Generated: {hist_summary['meta_policies_generated']}")
    
    print(f"\n{'='*60}")
    print("✅ Demo Complete!")
    print(f"{'='*60}")
    print(f"\n📊 Open demo_dashboard.html to view interactive results")

    # ----------------------------------------------------------------------
    # Advanced Enhancements (optional)
    # ----------------------------------------------------------------------
    if ENHANCEMENTS_AVAILABLE:
        _demonstrate_enhancements()
    else:
        print("\n⚠️  Advanced enhancement modules not installed. Skipping enhanced demo.")


if __name__ == "__main__":
    simulate_agent_execution(steps=15)
