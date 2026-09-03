"""
Demo: Pareto Frontier Analysis for Green_Agent (Enhanced)

This script demonstrates how to use Pareto analysis to compare agents
across multiple objectives (accuracy, energy, carbon, latency).

Enhanced with optional integration of LIMIT Graph, MODP, RLHF,
Multi‑Teacher On‑Policy Distillation, Bio‑inspired Optimisation, and
MoE expert gating (see demo 7 at the end).

Run with: python examples/demo_pareto_analysis.py
           python examples/demo_pareto_analysis.py --enhanced   # includes enhanced demo
"""

import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from analysis.pareto_analyzer import ParetoPoint, ParetoFrontierAnalyzer
from analysis.complexity_analyzer import TaskComplexity, ComplexityAnalyzer

# ---------------------------------------------------------------------------
# Enhanced imports (optional)
# ---------------------------------------------------------------------------
try:
    import numpy as np
    from collections import deque
    import random
    ENHANCED_AVAILABLE = True
except ImportError:
    ENHANCED_AVAILABLE = False


def demo_basic_pareto():
    """Demo 1: Basic Pareto frontier analysis"""
    print("=" * 60)
    print("DEMO 1: Basic Pareto Frontier Analysis")
    print("=" * 60)
    
    agents = [
        ParetoPoint('gpt4_agent', 0.95, 0.0050, 0.0010, 200),
        ParetoPoint('claude_agent', 0.93, 0.0035, 0.0007, 180),
        ParetoPoint('llama3_agent', 0.88, 0.0020, 0.0004, 120),
        ParetoPoint('mixtral_agent', 0.91, 0.0025, 0.0005, 150),
        ParetoPoint('old_model', 0.80, 0.0080, 0.0016, 250)
    ]
    
    analyzer = ParetoFrontierAnalyzer()
    
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
    
    knee = analyzer.get_knee_point(frontier)
    print(f"🎯 Knee Point (Best Balance): {knee.agent_id}")
    print(f"   This agent offers the best overall compromise.\n")


def demo_pareto_ranking():
    """Demo 2: Ranking agents by Pareto dominance layers"""
    print("=" * 60)
    print("DEMO 2: Pareto Dominance Ranking")
    print("=" * 60)
    
    agents = [
        ParetoPoint('excellent', 0.95, 0.002, 0.0004, 100),
        ParetoPoint('very_good', 0.92, 0.003, 0.0006, 120),
        ParetoPoint('good', 0.88, 0.004, 0.0008, 150),
        ParetoPoint('average', 0.85, 0.005, 0.0010, 180),
        ParetoPoint('below_avg', 0.80, 0.007, 0.0014, 220)
    ]
    
    analyzer = ParetoFrontierAnalyzer()
    
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
    
    agent_a = ParetoPoint('high_accuracy', 0.95, 0.006, 0.0012, 250)
    agent_b = ParetoPoint('high_efficiency', 0.88, 0.002, 0.0004, 100)
    
    analyzer = ParetoFrontierAnalyzer()
    
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
    
    print("\n📝 Simple Task:")
    simple_complexity = analyzer.analyze_from_trace(simple_trace)
    print(f"  Prompt length: {simple_complexity.prompt_length} tokens")
    print(f"  Reasoning steps: {simple_complexity.reasoning_steps}")
    print(f"  Tool calls: {simple_complexity.tool_calls}")
    print(f"  Complexity score: {simple_complexity.compute_composite_score():.2f}")
    print(f"  Tier: {analyzer.categorize_complexity(simple_complexity)}")
    
    print("\n📝 Complex Task:")
    complex_complexity = analyzer.analyze_from_trace(complex_trace)
    print(f"  Prompt length: {complex_complexity.prompt_length} tokens")
    print(f"  Reasoning steps: {complex_complexity.reasoning_steps}")
    print(f"  Tool calls: {complex_complexity.tool_calls}")
    print(f"  Complexity score: {complex_complexity.compute_composite_score():.2f}")
    print(f"  Tier: {analyzer.categorize_complexity(complex_complexity)}")
    
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
    
    normal_trace = {
        'prompt': "Classify this image (100 tokens)",
        'reasoning': ["Step 1", "Step 2", "Step 3"],
        'tool_calls': [{'tool': 'vision_api'}],
        'execution_time_ms': 1000,
        'context_tokens': 200
    }
    
    excessive_trace = {
        'prompt': "Classify this image (100 tokens)",
        'reasoning': [f"Step {i}" for i in range(50)],
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
    """Demo 6: Cinebench classifier comparison"""
    print("\n" + "=" * 60)
    print("DEMO 6: Cinebench Classifier Comparison")
    print("=" * 60)
    
    classifiers = [
        ParetoPoint('ResNet50', 0.94, 0.008, 0.0016, 350),
        ParetoPoint('EfficientNet', 0.92, 0.003, 0.0006, 180),
        ParetoPoint('MobileNet', 0.86, 0.001, 0.0002, 80)
    ]
    
    analyzer = ParetoFrontierAnalyzer()
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
    
    knee = analyzer.get_knee_point(frontier)
    print(f"\n🎯 RECOMMENDED FOR PRODUCTION: {knee.agent_id}")
    print(f"   Best balance between accuracy and efficiency")


# ---------------------------------------------------------------------------
# Enhanced Demo 7: Integration of LIMIT Graph, MODP, RLHF, Distillation, MoE, Bio-inspired
# ---------------------------------------------------------------------------
def demo_enhanced_pareto_with_advanced_techniques():
    """
    Demonstrates how the advanced techniques can be applied to Pareto analysis.
    This is a simplified, self-contained example.
    """
    print("\n" + "=" * 60)
    print("DEMO 7: Advanced Pareto Analysis with Enhancements")
    print("=" * 60)

    if not ENHANCED_AVAILABLE:
        print("Advanced demo requires numpy. Skipping.")
        return

    # Sample agents (same as demo 6)
    agents = [
        ParetoPoint('ResNet50', 0.94, 0.008, 0.0016, 350),
        ParetoPoint('EfficientNet', 0.92, 0.003, 0.0006, 180),
        ParetoPoint('MobileNet', 0.86, 0.001, 0.0002, 80),
    ]

    # Assume some graph metrics and human feedback (could come from LIMIT Graph or RLHF)
    graph_metrics = {"centrality": 0.6, "connectivity": 0.5}
    human_feedback = 0.7  # prefers accuracy over energy

    # 1. MODP: define objective weights (accuracy, energy, carbon, latency)
    # Higher weight means more important (we will minimize energy/carbon/latency, maximize accuracy)
    modp_weights = np.array([0.4, 0.25, 0.2, 0.15])  # accuracy, energy, carbon, latency

    # 2. Bio-inspired: evolutionary tuning of weights (simple demonstration)
    # We'll evolve the weights for a few generations and pick the best based on a fitness function.
    population_size = 20
    mutation_rate = 0.2
    generations = 10
    population = [np.random.dirichlet(np.ones(4)) for _ in range(population_size)]

    # Define a fitness: we want a high weighted score (accuracy high, others low)
    def fitness(weights, agent):
        # Normalize metrics
        acc = agent.accuracy
        energy = 1.0 - min(agent.energy_kwh * 1000 / 10.0, 1.0)  # lower energy is better
        carbon = 1.0 - min(agent.carbon_co2e_kg * 1000 / 2.0, 1.0)
        latency = 1.0 - min(agent.latency_ms / 500.0, 1.0)
        return weights[0]*acc + weights[1]*energy + weights[2]*carbon + weights[3]*latency

    # Evolve weights to maximize average fitness across agents
    def avg_fitness(weights, agents):
        return np.mean([fitness(weights, a) for a in agents])

    for gen in range(generations):
        scores = [avg_fitness(w, agents) for w in population]
        best_idx = np.argmax(scores)
        best_weights = population[best_idx]
        # Create next generation
        new_pop = [best_weights]  # elitism
        while len(new_pop) < population_size:
            parent1 = population[random.randint(0, population_size-1)]
            parent2 = population[random.randint(0, population_size-1)]
            child = parent1.copy()
            if random.random() < 0.7:  # crossover
                alpha = random.random()
                child = alpha * parent1 + (1 - alpha) * parent2
            child += np.random.dirichlet(np.ones(4)) * mutation_rate  # mutation
            child = child / child.sum()
            new_pop.append(child)
        population = new_pop

    evolved_weights = best_weights
    print(f"\n🧬 Evolved MODP weights (accuracy, energy, carbon, latency): {evolved_weights}")

    # 3. Multi-teacher distillation with MoE gating (simplified)
    # Three teachers: Rule-based, RLHF-based, Historical (simulated)
    def rule_teacher(agent, graph_metrics):
        # Rule: prefer high accuracy if centrality high, else prefer energy efficiency
        if graph_metrics["centrality"] > 0.5:
            return agent.accuracy
        else:
            return 1.0 - min(agent.energy_kwh * 1000 / 10.0, 1.0)

    def rlhf_teacher(agent, human_feedback):
        # Human feedback: if high, prefer accuracy; if low, prefer energy
        if human_feedback > 0.5:
            return agent.accuracy
        else:
            return 1.0 - min(agent.energy_kwh * 1000 / 10.0, 1.0)

    def historical_teacher(agent, history=None):
        # Simulate a model that has learned from past data
        # In reality, this would be a trained model
        return 0.7 * agent.accuracy + 0.3 * (1.0 - min(agent.energy_kwh * 1000 / 10.0, 1.0))

    # Simple MoE gating: use a weighted combination of teachers based on graph metrics and human feedback
    def moe_score(agent, graph_metrics, human_feedback, weights):
        # weights is a vector of three values summing to 1 (for the three teachers)
        scores = np.array([
            rule_teacher(agent, graph_metrics),
            rlhf_teacher(agent, human_feedback),
            historical_teacher(agent)
        ])
        return np.dot(weights, scores)

    # Initialize gating weights and do a simple update (not shown for brevity)
    gate_weights = np.array([0.4, 0.4, 0.2])  # initial

    # 4. Evaluate agents using the evolved MODP weights + MoE score
    print("\n📊 Enhanced agent scoring:")
    for agent in agents:
        # MODP score
        modp_score = fitness(evolved_weights, agent)
        # MoE distillation score
        moe = moe_score(agent, graph_metrics, human_feedback, gate_weights)
        # Final blended score (0.6 MODP + 0.4 MoE)
        final_score = 0.6 * modp_score + 0.4 * moe
        print(f"  {agent.agent_id}:")
        print(f"    MODP score: {modp_score:.3f}")
        print(f"    MoE score:  {moe:.3f}")
        print(f"    Final score: {final_score:.3f}")

    # 5. Recommendation based on highest final score
    best_agent = max(agents, key=lambda a: 0.6 * fitness(evolved_weights, a) +
                     0.4 * moe_score(a, graph_metrics, human_feedback, gate_weights))
    print(f"\n🏆 Recommended agent with enhancements: {best_agent.agent_id}")


def main():
    parser = argparse.ArgumentParser(description="Green_Agent Pareto Analysis Demo")
    parser.add_argument("--enhanced", action="store_true",
                        help="Include enhanced demo (Demo 7) showing LIMIT Graph, MODP, RLHF, etc.")
    args = parser.parse_args()

    print("\n" + "🌟" * 30)
    print("Green_Agent Pareto Analysis Demo")
    print("🌟" * 30 + "\n")

    demo_basic_pareto()
    demo_pareto_ranking()
    demo_agent_comparison()
    demo_complexity_analysis()
    demo_over_reasoning_detection()
    demo_cinebench_integration()

    if args.enhanced:
        demo_enhanced_pareto_with_advanced_techniques()

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
