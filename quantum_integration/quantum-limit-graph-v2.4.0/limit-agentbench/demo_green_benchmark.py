# -*- coding: utf-8 -*-
"""
Demo: Green Agent Benchmarking
Demonstrates the LIMIT-AgentBench platform capabilities,
including optional advanced enhancements (LIMIT Graph, MODP, RLHF,
Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation, MoE expert gating).
"""

import sys
import logging
from typing import Dict, Any
import asyncio

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Mock agent for demonstration
class MockAgent:
    """Mock agent for demonstration purposes."""
    
    def __init__(self, name: str, framework: str):
        self.name = name
        self.framework = framework
    
    def run(self, task_input: Dict[str, Any]) -> Dict[str, Any]:
        """Run agent on task input."""
        # Simulate agent execution
        question = task_input.get('question', '')
        return {
            "answer": f"Mock answer to: {question}",
            "confidence": 0.92
        }


def demo_agentbench_protocol():
    """Demonstrate AgentBench protocol compatibility."""
    print("\n" + "="*80)
    print("DEMO 1: AgentBench Protocol Compatibility")
    print("="*80)
    
    from core.agentbench_adapter import AgentBenchAdapter
    
    adapter = AgentBenchAdapter()
    
    task = adapter.create_task(
        task_id="demo_qa_001",
        suite="question_answering",
        task_type="qa",
        input_data={"question": "What is the capital of France?"},
        expected_output={"answer": "Paris"},
        evaluation_metrics=["accuracy", "latency", "energy_kwh", "carbon_co2e_kg"]
    )
    
    print(f"\n✓ Created task: {task['task_id']}")
    print(f"  Suite: {task['suite']}")
    print(f"  Metrics: {', '.join(task['evaluation']['metrics'])}")
    
    agent = MockAgent("DemoAgent", "langchain")
    
    result = adapter.evaluate_agent(
        agent=agent,
        task=task,
        track_energy=True,
        track_carbon=True
    )
    
    print(f"\n✓ Evaluation complete:")
    print(f"  Agent: {result['agent_name']}")
    print(f"  Framework: {result['framework']}")
    print(f"  Success: {result['success']}")
    print(f"  Latency: {result['metrics']['latency_ms']:.2f} ms")
    if 'energy_kwh' in result['metrics']:
        print(f"  Energy: {result['metrics']['energy_kwh']:.6f} kWh")
        print(f"  Carbon: {result['metrics']['carbon_co2e_kg']:.6f} kg CO2e")


def demo_green_metrics():
    """Demonstrate green metrics tracking."""
    print("\n" + "="*80)
    print("DEMO 2: Green Metrics Tracking")
    print("="*80)
    
    from core.green_metrics import GreenMetricsTracker
    
    tracker = GreenMetricsTracker(
        grid_region="US-CA",
        hardware_profile="nvidia_a100",
        track_energy=True,
        track_carbon=True
    )
    
    print(f"\n✓ Initialized tracker:")
    print(f"  Grid region: US-CA")
    print(f"  Hardware: nvidia_a100")
    print(f"  Carbon intensity: {tracker.carbon_intensity} kg CO2e/kWh")
    
    import time
    tracker.start()
    time.sleep(0.5)  # Simulate work
    tracker.stop()
    
    metrics = tracker.get_metrics()
    
    print(f"\n✓ Tracked metrics:")
    print(f"  Duration: {metrics['duration_seconds']:.2f} s")
    print(f"  Power: {metrics['power_watts']:.2f} W")
    print(f"  Energy: {metrics['energy_kwh']:.6f} kWh")
    print(f"  Carbon: {metrics['carbon_co2e_kg']:.6f} kg CO2e")
    print(f"  Efficiency: {metrics['efficiency_score']:.2f}")


def demo_multi_framework_adapters():
    """Demonstrate multi-framework support."""
    print("\n" + "="*80)
    print("DEMO 3: Multi-Framework Agent Adapters")
    print("="*80)
    
    from adapters.langchain_adapter import LangChainAdapter
    from adapters.autogen_adapter import AutoGenAdapter
    from adapters.limit_graph_adapter import LimitGraphAdapter
    
    langchain_agent = MockAgent("LangChainAgent", "langchain")
    autogen_agent = MockAgent("AutoGenAgent", "autogen")
    limit_agent = MockAgent("LimitGraphAgent", "limit_graph")
    
    adapters = [
        LangChainAdapter(langchain_agent),
        AutoGenAdapter(autogen_agent),
        LimitGraphAdapter(limit_agent)
    ]
    
    print(f"\n✓ Created {len(adapters)} framework adapters:")
    for adapter in adapters:
        metadata = adapter.get_metadata()
        print(f"  - {metadata['agent_name']} ({metadata['framework']})")


def demo_sustainability_index():
    """Demonstrate sustainability index calculation."""
    print("\n" + "="*80)
    print("DEMO 4: Sustainability Index")
    print("="*80)
    
    from metrics.sustainability_index import SustainabilityIndex
    
    si_calc = SustainabilityIndex()
    
    agents = {
        "EfficientAgent": {"accuracy": 0.95, "energy_kwh": 0.002, "carbon_co2e_kg": 0.0004},
        "AccurateAgent": {"accuracy": 0.98, "energy_kwh": 0.005, "carbon_co2e_kg": 0.001},
        "FastAgent": {"accuracy": 0.85, "energy_kwh": 0.001, "carbon_co2e_kg": 0.0002}
    }
    
    print("\n✓ Sustainability Index Rankings:")
    rankings = si_calc.rank_agents(agents)
    
    for i, (agent_name, si) in enumerate(rankings, 1):
        rating = SustainabilityIndex.get_rating(si)
        metrics = agents[agent_name]
        print(f"\n  {i}. {agent_name}")
        print(f"     Sustainability Index: {si:.2f} ({rating})")
        print(f"     Accuracy: {metrics['accuracy']:.2%}")
        print(f"     Energy: {metrics['energy_kwh']:.6f} kWh")
        print(f"     Carbon: {metrics['carbon_co2e_kg']:.6f} kg CO2e")


def demo_green_leaderboard():
    """Demonstrate green leaderboard."""
    print("\n" + "="*80)
    print("DEMO 5: Green Leaderboard")
    print("="*80)
    
    from dashboard.green_leaderboard import GreenLeaderboard
    
    leaderboard = GreenLeaderboard(storage_path="./demo_leaderboard")
    
    agents_data = [
        ("LangChainAgent", "langchain", 0.95, 0.003, 0.0006),
        ("AutoGenAgent", "autogen", 0.92, 0.004, 0.0008),
        ("LimitGraphAgent", "limit_graph", 0.96, 0.002, 0.0004),
        ("CrewAIAgent", "crewai", 0.90, 0.005, 0.001)
    ]
    
    print("\n✓ Submitting results to leaderboard...")
    for agent_name, framework, accuracy, energy, carbon in agents_data:
        leaderboard.submit(
            agent_name=agent_name,
            framework=framework,
            task_suite="demo_benchmark",
            accuracy=accuracy,
            energy_kwh=energy,
            carbon_co2e_kg=carbon,
            latency_ms=150.0
        )
        print(f"  - Submitted {agent_name}")
    
    rankings = leaderboard.get_rankings(sort_by="sustainability_index", limit=10)
    
    print(f"\n✓ Top Agents (by Sustainability Index):")
    for entry in rankings:
        print(f"\n  {entry['rank']}. {entry['agent_name']} ({entry['framework']})")
        print(f"     SI: {entry['metrics']['sustainability_index']:.2f}")
        print(f"     Accuracy: {entry['metrics']['accuracy']:.2%}")
        print(f"     Energy: {entry['metrics']['energy_kwh']:.6f} kWh")
        print(f"     Carbon: {entry['metrics']['carbon_co2e_kg']:.6f} kg CO2e")
    
    framework_stats = leaderboard.get_framework_stats()
    print(f"\n✓ Framework Statistics:")
    for framework, stats in framework_stats.items():
        print(f"\n  {framework}:")
        print(f"    Entries: {stats['count']}")
        print(f"    Avg Accuracy: {stats['avg_accuracy']:.2%}")
        print(f"    Avg Energy: {stats['avg_energy']:.6f} kWh")
        print(f"    Avg Sustainability: {stats['avg_sustainability']:.2f}")


def demo_benchmark_harness():
    """Demonstrate benchmark harness."""
    print("\n" + "="*80)
    print("DEMO 6: Benchmark Harness")
    print("="*80)
    
    from core.benchmark_harness import BenchmarkHarness
    from core.agentbench_adapter import AgentBenchAdapter
    
    harness = BenchmarkHarness(
        output_dir="./demo_benchmark_results",
        grid_region="US-CA",
        hardware_profile="nvidia_a100"
    )
    
    adapter = AgentBenchAdapter()
    tasks = [
        adapter.create_task(
            task_id=f"demo_task_{i}",
            suite="demo_suite",
            task_type="qa",
            input_data={"question": f"Question {i}?"},
            expected_output={"answer": f"Answer {i}"}
        )
        for i in range(3)
    ]
    
    print(f"\n✓ Created task suite with {len(tasks)} tasks")
    
    agent = MockAgent("BenchmarkAgent", "langchain")
    
    print(f"\n✓ Running benchmark...")
    result = harness.run_benchmark(
        agent=agent,
        task_suite=tasks,
        benchmark_name="demo_benchmark"
    )
    
    print(f"\n✓ Benchmark complete:")
    print(f"  Agent: {result['agent_name']}")
    print(f"  Tasks: {result['num_tasks']}")
    print(f"  Duration: {result['duration_seconds']:.2f} s")
    print(f"  Success Rate: {result['aggregated_metrics']['success_rate']:.2%}")


def demo_enhanced_modules():
    """
    DEMO 7: Advanced Enhancements (LIMIT Graph, MODP, RLHF,
    Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation, MoE expert gating).
    This demo only runs if the advanced enhancement modules are installed
    and importable; otherwise, it prints a message and skips.
    """
    print("\n" + "="*80)
    print("DEMO 7: Advanced Enhancements (Optional)")
    print("="*80)

    try:
        from enhancements.schemas.node_descriptor import NodeDescriptor, NodeType, CoolingType
        from enhancements.schemas.workload_descriptor import WorkloadDescriptor, TaskType, Urgency
        from enhancements.schemas.feedback_event import FeedbackEvent
        from enhancements.zero_trust_architecture import ZeroTrustArchitecture, ZeroTrustConfig

        # 1. Create a NodeDescriptor with LIMIT Graph, RLHF, MoE, evolutionary flags
        print("\n1. Creating enhanced NodeDescriptor...")
        node = NodeDescriptor(
            id="enhanced_node",
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
        print(f"     - Evolutionary enabled: {node.use_evolutionary}")

        # 2. Select routing strategy using distillation + MoE
        print("\n2. Selecting routing strategy (distillation + MoE)...")
        strategy = asyncio.run(node.select_routing_strategy(exploration=False))
        print(f"   ✓ Selected strategy: {strategy}")

        # 3. Create a WorkloadDescriptor with MODP weights and RLHF
        print("\n3. Creating enhanced WorkloadDescriptor...")
        wl = WorkloadDescriptor(
            task_id="enhanced_task",
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
        print(f"     - MODP weights: {wl.metadata}")
        print(f"     - Graph centrality: {wl.graph_metrics['centrality']}")

        # 4. Select priority using distillation + MoE
        print("\n4. Selecting priority (distillation + MoE)...")
        priority = asyncio.run(wl.select_priority(exploration=False))
        print(f"   ✓ Selected priority: {priority}")

        # 5. Create a FeedbackEvent with enhanced fields
        print("\n5. Creating FeedbackEvent with enhanced fields...")
        event = FeedbackEvent(
            source="demo",
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

        # 6. Initialize Zero Trust with enhancements (optional)
        print("\n6. Initializing Zero Trust with enhancements...")
        zt_config = ZeroTrustConfig(use_enhancements=True, use_distillation=True, use_evolutionary=True)
        zta = ZeroTrustArchitecture(config=zt_config)
        print("   ✓ ZeroTrustArchitecture initialized")
        print(f"     - Enhancements enabled: {zta.use_enhancements}")

        print("\n" + "="*80)
        print("✓ Advanced enhancements demo completed successfully!")
        print("="*80 + "\n")

    except ImportError as e:
        print("\n⚠ Advanced enhancement modules not installed.")
        print(f"   Error: {e}")
        print("   Skipping enhanced demo.")
        print("="*80 + "\n")
    except Exception as e:
        print(f"\n✗ Enhanced demo failed: {e}")
        print("="*80 + "\n")


def main():
    """Run all demos, including optional advanced enhancements."""
    print("\n" + "="*80)
    print("LIMIT-AgentBench: Green Agent Benchmarking Platform")
    print("Version 2.4.2 - Demo Suite")
    print("="*80)
    
    try:
        demo_agentbench_protocol()
        demo_green_metrics()
        demo_multi_framework_adapters()
        demo_sustainability_index()
        demo_green_leaderboard()
        demo_benchmark_harness()
        demo_enhanced_modules()   # NEW: Advanced enhancements (optional)
        
        print("\n" + "="*80)
        print("✓ All demos completed successfully!")
        print("="*80 + "\n")
        
    except Exception as e:
        logger.error(f"Demo failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
