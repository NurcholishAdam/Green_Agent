"""
Demo: Pareto Frontier Analysis for Green_Agent

This script demonstrates how to use Pareto analysis to compare agents
across multiple objectives (accuracy, energy, carbon, latency).

Run with: python examples/demo_pareto_analysis.py
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from analysis.pareto_analyzer import ParetoPoint, ParetoFrontierAnalyzer
from analysis.complexity_analyzer import TaskComplexity, ComplexityAnalyzer


def demo_basic_pareto():
    """Demo 1: Basic Pareto frontier analysis"""
    print("=" * 60)
    print("DEMO 1: Basic Pareto Frontier Analysis")
    print("=" * 60)
    
    # Create sample agents with different trade-offs
    agents = [
        # High accuracy, high energy
        ParetoPoint('gpt4_agent', 0.95, 0.0050, 0.0010, 200),
        
        # Medium accuracy, medium energy
        ParetoPoint('claude_agent', 0.93, 0.0035, 0.0007, 180),
        
        # Lower accuracy, low energy
        ParetoPoint('llama3_agent', 0.88, 0.0020, 0.0004, 120),
        
        # Good balance
        ParetoPoint('mixtral_agent', 0.91, 0.0025, 0.0005, 150),
        
        # Dominated agent (worse on all metrics)
        ParetoPoint('old_model', 0.80, 0.0080, 0.0016, 250)
    ]
    
    # Create analyzer
    analyzer = ParetoFrontierAnalyzer()
    
    # Compute Pareto frontier
    print("\n📊 Computing Pareto Frontier...")
    frontier = analyzer.compute_frontier(agents)
    
    print(f"\nTotal agents: {len(agents)}")
    print(f"Frontier agents: {len(frontier)}")
    print("\n✨ Agents on Pareto frontier:")
    for agent in frontier:
        print(f"  - {agent.agent_id}")
        print(f"    Accuracy: {agent.accuracy:.2%}")
        print(f"    Energy: {agent.energy_kwh*1000:.2f} Wh")
        print(f"    Carbon: {agent.carbon_co2e_kg*1000:.2f} g CO₂")
        print(f"    Latency: {agent.latency_ms:.0f} ms")
        print()
    
    # Find knee point (best balance)
    knee = analyzer.get_knee_point(frontier)
    print(f"🎯 Knee Point (Best Balance): {knee.agent_id}")
    print(f"   This agent offers the best overall compromise.\n")


def demo_pareto_ranking():
    """Demo 2: Ranking agents by Pareto dominance layers"""
    print("=" * 60)
    print("DEMO 2: Pareto Dominance Ranking")
    print("=" * 60)
    
    # Create agents
    agents = [
        ParetoPoint('excellent', 0.95, 0.002, 0.0004, 100),
        ParetoPoint('very_good', 0.92, 0.003, 0.0006, 120),
        ParetoPoint('good', 0.88, 0.004, 0.0008, 150),
        ParetoPoint('average', 0.85, 0.005, 0.0010, 180),
        ParetoPoint('below_avg', 0.80, 0.007, 0.0014, 220)
    ]
    
    analyzer = ParetoFrontierAnalyzer()
    
    # Rank by dominance
    print("\n📊 Ranking by Pareto Dominance...")
    ranks = analyzer.rank_by_dominance(agents)
    
    for rank, rank_agents in sorted(ranks.items()):
        print(f"\n🏆 Rank {rank} ({len(rank_agents)} agents):")
        for agent in rank_agents:
            print(f"  - {agent.agent_id}: "
                  f"{agent.accuracy:.2%} acc, "
                  f"{agent.energy_kwh*1000:.2f} Wh")


def demo_agent_comparison():
    """Demo 3: Compare specific agents"""
    print("\n" + "=" * 60)
    print("DEMO 3: Agent-to-Agent Comparison")
    print("=" * 60)
    
    # Two agents with trade-offs
    agent_a = ParetoPoint('high_accuracy', 0.95, 0.006, 0.0012, 250)
    agent_b = ParetoPoint('high_efficiency', 0.88, 0.002, 0.0004, 100)
    
    analyzer = ParetoFrontierAnalyzer()
    
    # Compare
    comparison = analyzer.compare_agents(agent_a, agent_b)
    
    print(f"\n🔍 Comparing {agent_a.agent_id} vs {agent_b.agent_id}")
    print(f"Relationship: {comparison['relationship']}")
    print(f"Explanation: {comparison['explanation']}")
    
    if 'trade_offs' in comparison:
        print("\n📊 Trade-offs:")
        trade_offs = comparison['trade_offs']
        print(f"  {agent_a.agent_id} better on: {trade_offs['a_better_on']}")
        print(f"  {agent_b.agent_id} better on: {trade_offs['b_better_on']}")


def demo_complexity_analysis():
    """Demo 4: Task complexity analysis"""
    print("\n" + "=" * 60)
    print("DEMO 4: Task Complexity Analysis")
    print("=" * 60)
    
    # Sample execution traces
    simple_trace = {
        'prompt': "What is 2+2?",
        'reasoning': ["Simple arithmetic"],
        'tool_calls': [],
        'execution_time_ms': 50,
        'context_tokens': 20
    }
    
    complex_trace = {
        'prompt': "Analyze this Cinebench result and compare against similar CPUs...",
        'reasoning': [
            "Step 1: Parse benchmark results",
            "Step 2: Query database for similar CPUs",
            "Step 3: Compare performance metrics",
            "Step 4: Analyze power efficiency",
            "Step 5: Generate recommendation"
        ],
        'tool_calls': [
            {'tool': 'benchmark_db'},
            {'tool': 'cpu_specs_api'},
            {'tool': 'power_calculator'}
        ],
        'execution_time_ms': 2500,
        'context_tokens': 1500
    }
    
    analyzer = ComplexityAnalyzer()
    
    # Analyze simple task
    print("\n📝 Simple Task:")
    simple_complexity = analyzer.analyze_from_trace(simple_trace)
    print(f"  Prompt length: {simple_complexity.prompt_length} tokens")
    print(f"  Reasoning steps: {simple_complexity.reasoning_steps}")
    print(f"  Tool calls: {simple_complexity.tool_calls}")
    print(f"  Complexity score: {simple_complexity.compute_composite_score():.2f}")
    print(f"  Tier: {analyzer.categorize_complexity(simple_complexity)}")
    
    # Analyze complex task
    print("\n📝 Complex Task:")
    complex_complexity = analyzer.analyze_from_trace(complex_trace)
    print(f"  Prompt length: {complex_complexity.prompt_length} tokens")
    print(f"  Reasoning steps: {complex_complexity.reasoning_steps}")
    print(f"  Tool calls: {complex_complexity.tool_calls}")
    print(f"  Complexity score: {complex_complexity.compute_composite_score():.2f}")
    print(f"  Tier: {analyzer.categorize_complexity(complex_complexity)}")
    
    # Compare
    comparison = analyzer.compare_complexities(complex_complexity, simple_complexity)
    print(f"\n🔄 Complexity Difference:")
    print(f"  Score difference: {comparison['score_diff']:.2f}")
    print(f"  More complex: Task {comparison['more_complex']}")


def demo_over_reasoning_detection():
    """Demo 5: Detect over-reasoning agents"""
    print("\n" + "=" * 60)
    print("DEMO 5: Over-Reasoning Detection")
    print("=" * 60)
    
    analyzer = ComplexityAnalyzer()
    
    # Normal reasoning
    normal_trace = {
        'prompt': "Classify this image (100 tokens)",
        'reasoning': ["Step 1", "Step 2", "Step 3"],  # 3 steps for 100 tokens = OK
        'tool_calls': [{'tool': 'vision_api'}],
        'execution_time_ms': 1000,
        'context_tokens': 200
    }
    
    # Over-reasoning
    excessive_trace = {
        'prompt': "Classify this image (100 tokens)",
        'reasoning': [f"Step {i}" for i in range(50)],  # 50 steps for 100 tokens = excessive!
        'tool_calls': [{'tool': 'vision_api'}],
        'execution_time_ms': 5000,
        'context_tokens': 1000
    }
    
    print("\n✅ Normal Agent:")
    normal_complexity = analyzer.analyze_from_trace(normal_trace)
    normal_result = analyzer.detect_over_reasoning(normal_complexity)
    print(f"  Over-reasoning: {normal_result['over_reasoning']}")
    print(f"  Ratio: {normal_result['ratio']:.2f}")
    print(f"  {normal_result['recommendation']}")
    
    print("\n⚠️ Over-Reasoning Agent:")
    excessive_complexity = analyzer.analyze_from_trace(excessive_trace)
    excessive_result = analyzer.detect_over_reasoning(excessive_complexity)
    print(f"  Over-reasoning: {excessive_result['over_reasoning']}")
    print(f"  Ratio: {excessive_result['ratio']:.2f}")
    print(f"  {excessive_result['recommendation']}")


def demo_cinebench_integration():
    """Demo 6: Cinebench classifier comparison (your use case!)"""
    print("\n" + "=" * 60)
    print("DEMO 6: Cinebench Classifier Comparison")
    print("=" * 60)
    
    # Simulate three different CNN models for Cinebench classification
    classifiers = [
        ParetoPoint(
            agent_id='ResNet50',
            accuracy=0.94,
            energy_kwh=0.008,  # 8 Wh per batch
            carbon_co2e_kg=0.0016,
            latency_ms=350
        ),
        ParetoPoint(
            agent_id='EfficientNet',
            accuracy=0.92,
            energy_kwh=0.003,  # 3 Wh per batch - much more efficient!
            carbon_co2e_kg=0.0006,
            latency_ms=180
        ),
        ParetoPoint(
            agent_id='MobileNet',
            accuracy=0.86,
            energy_kwh=0.001,  # 1 Wh per batch - very efficient
            carbon_co2e_kg=0.0002,
            latency_ms=80
        )
    ]
    
    analyzer = ParetoFrontierAnalyzer()
    
    # Compute frontier
    frontier = analyzer.compute_frontier(classifiers)
    
    print("\n📊 Cinebench Classifier Analysis:")
    print(f"\nTotal models evaluated: {len(classifiers)}")
    print(f"Pareto-optimal models: {len(frontier)}")
    
    print("\n✨ Recommended models (on Pareto frontier):")
    for clf in frontier:
        acc_per_wh = clf.accuracy / (clf.energy_kwh * 1000)
        print(f"\n  {clf.agent_id}:")
        print(f"    Accuracy: {clf.accuracy:.2%}")
        print(f"    Energy: {clf.energy_kwh*1000:.2f} Wh/batch")
        print(f"    Accuracy per Wh: {acc_per_wh:.4f}")
        print(f"    Latency: {clf.latency_ms:.0f} ms")
    
    # Find knee point
    knee = analyzer.get_knee_point(frontier)
    print(f"\n🎯 RECOMMENDED FOR PRODUCTION: {knee.agent_id}")
    print(f"   Best balance between accuracy and efficiency")


def main():
    """Run all demos"""
    print("\n" + "🌟" * 30)
    print("Green_Agent Pareto Analysis Demo")
    print("🌟" * 30 + "\n")
    
    demo_basic_pareto()
    demo_pareto_ranking()
    demo_agent_comparison()
    demo_complexity_analysis()
    demo_over_reasoning_detection()
    demo_cinebench_integration()
    
    print("\n" + "=" * 60)
    print("✅ Demo complete!")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Run tests: pytest tests/test_pareto_analysis.py")
    print("2. Integrate with your Green_Agent evaluation pipeline")
    print("3. Use Pareto analysis for AgentBeats submissions")
    print()


if __name__ == '__main__':
    main()
