# -*- coding: utf-8 -*-
"""
Demo: Green Agent Benchmarking
Demonstrates the LIMIT-AgentBench platform capabilities
"""

import sys
import logging
from typing import Dict, Any

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
    
    # Initialize adapter
    adapter = AgentBenchAdapter()
    
    # Create a task
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
    
    # Create mock agent
    agent = MockAgent("DemoAgent", "langchain")
    
    # Evaluate agent
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
    
    # Initialize tracker
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
    
    # Track execution
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
    
    # Create mock agents
    langchain_agent = MockAgent("LangChainAgent", "langchain")
    autogen_agent = MockAgent("AutoGenAgent", "autogen")
    limit_agent = MockAgent("LimitGraphAgent", "limit_graph")
    
    # Create adapters
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
    
    # Initialize calculator
    si_calc = SustainabilityIndex()
    
    # Calculate for different agents
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
    
    # Initialize leaderboard
    leaderboard = GreenLeaderboard(storage_path="./demo_leaderboard")
    
    # Submit some results
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
    
    # Get rankings
    rankings = leaderboard.get_rankings(sort_by="sustainability_index", limit=10)
    
    print(f"\n✓ Top Agents (by Sustainability Index):")
    for entry in rankings:
        print(f"\n  {entry['rank']}. {entry['agent_name']} ({entry['framework']})")
        print(f"     SI: {entry['metrics']['sustainability_index']:.2f}")
        print(f"     Accuracy: {entry['metrics']['accuracy']:.2%}")
        print(f"     Energy: {entry['metrics']['energy_kwh']:.6f} kWh")
        print(f"     Carbon: {entry['metrics']['carbon_co2e_kg']:.6f} kg CO2e")
    
    # Framework stats
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
    
    # Initialize harness
    harness = BenchmarkHarness(
        output_dir="./demo_benchmark_results",
        grid_region="US-CA",
        hardware_profile="nvidia_a100"
    )
    
    # Create task suite
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
    
    # Create mock agent
    agent = MockAgent("BenchmarkAgent", "langchain")
    
    # Run benchmark
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


def main():
    """Run all demos."""
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
        
        print("\n" + "="*80)
        print("✓ All demos completed successfully!")
        print("="*80 + "\n")
        
    except Exception as e:
        logger.error(f"Demo failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
