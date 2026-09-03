"""
Demo: Extended 7D Pareto Analysis with Memory, Circuit Depth, Variance

Showcases:
1. Memory footprint analysis (edge deployment constraints)
2. Quantum circuit depth (scalability/fragility)
3. Inference variance (stability under repeated execution)
4. Three specialized 2D plots (policy-oriented visualizations)

Enhanced with optional advanced techniques:
- LIMIT Graph metrics influence decision-making
- MODP (Multi-Objective Decision Process) weights are evolved or tuned
- RLHF (human feedback) adjusts preferences
- Multi-Teacher Distillation with MoE gating combines different ranking methods
- Bio-inspired optimisation (evolutionary algorithm) finds optimal constraint weights

Run with: python examples/demo_extended_dimensions.py [--enhanced]
"""

import sys
import argparse
from pathlib import Path
import random
import numpy as np
from collections import deque

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from analysis.extended_pareto_analyzer import ExtendedParetoPoint, ExtendedParetoAnalyzer
from visualization.pareto_plotter import ParetoPlotter


def print_section(title):
    """Print formatted section header"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70 + "\n")


# ---------------------------------------------------------------------------
# Enhanced Demo 7: Integration of LIMIT Graph, MODP, RLHF, Distillation, MoE, Bio-inspired
# ---------------------------------------------------------------------------
def demo_enhanced_analysis_with_advanced_techniques(agents, analyzer):
    """
    Demonstrates advanced techniques applied to the 7D Pareto analysis.
    """
    print_section("DEMO 7: Enhanced Analysis with LIMIT Graph, MODP, RLHF, Distillation, MoE, Bio-inspired")

    # 1. LIMIT Graph metrics (assumed from causal/policy graphs)
    # In a real system, these would come from GraphRegistry.health()
    graph_metrics = {
        "centrality": 0.7,
        "connectivity": 0.6,
        "density": 0.5
    }
    print(f"🌐 LIMIT Graph Metrics: {graph_metrics}")

    # 2. RLHF: human feedback score (0-1, higher = prefers accuracy over efficiency)
    human_feedback = 0.8
    print(f"👤 RLHF Feedback Score: {human_feedback} (prefers accuracy)")

    # 3. MODP: objective weights (accuracy, energy, carbon, latency, memory, circuit_depth, variance)
    # We'll evolve these using a simple genetic algorithm (bio-inspired)
    print("\n🧬 Evolving MODP weights using genetic algorithm...")
    n_objectives = 7
    population_size = 20
    generations = 15
    mutation_rate = 0.3
    crossover_rate = 0.7
    elitism = 2

    def fitness(weights):
        """Evaluate a weight vector by how well it separates frontier agents from dominated ones."""
        # We want a high score for frontier agents and low for dominated
        frontier_ids = {a.agent_id for a in analyzer.compute_frontier(agents)}
        total = 0.0
        for agent in agents:
            # Normalize metrics (higher better for accuracy, lower for others)
            acc = agent.accuracy
            energy = 1.0 - min(agent.energy_kwh * 1000 / 10.0, 1.0)
            carbon = 1.0 - min(agent.carbon_co2e_kg * 1000 / 2.0, 1.0)
            latency = 1.0 - min(agent.latency_ms / 500.0, 1.0)
            memory = 1.0 - min(agent.memory_mb / 2048.0, 1.0)
            depth = 1.0 - min(agent.circuit_depth / 50.0, 1.0)
            variance = 1.0 - agent.variance_score
            vec = np.array([acc, energy, carbon, latency, memory, depth, variance])
            score = np.dot(weights, vec)
            # If agent is on frontier, we want higher score; else lower
            if agent.agent_id in frontier_ids:
                total += score
            else:
                total -= score * 0.5
        return total / len(agents)

    population = [np.random.dirichlet(np.ones(n_objectives)) for _ in range(population_size)]
    best_weights = population[0]

    for gen in range(generations):
        scores = [fitness(w) for w in population]
        best_idx = np.argmax(scores)
        best_weights = population[best_idx]
        # Create new population
        sorted_indices = np.argsort(scores)[::-1]
        new_pop = [population[i] for i in sorted_indices[:elitism]]
        while len(new_pop) < population_size:
            parent1 = population[random.randint(0, population_size-1)]
            parent2 = population[random.randint(0, population_size-1)]
            if random.random() < crossover_rate:
                alpha = random.random()
                child = alpha * parent1 + (1 - alpha) * parent2
            else:
                child = parent1.copy()
            child += np.random.dirichlet(np.ones(n_objectives)) * mutation_rate
            child = child / child.sum()
            new_pop.append(child)
        population = new_pop

    print(f"   Evolved weights (accuracy, energy, carbon, latency, memory, depth, variance):")
    print(f"   {np.round(best_weights, 3)}")

    # 4. Multi-Teacher Distillation with MoE gating (simplified)
    # Three teachers: Rule-based, RLHF-based, Historical (simulated)
    def rule_teacher(agent):
        # Rule: prefer high accuracy if centrality high, else prefer efficiency
        if graph_metrics["centrality"] > 0.5:
            return agent.accuracy
        else:
            return 1.0 - min(agent.energy_kwh * 1000 / 10.0, 1.0)

    def rlhf_teacher(agent):
        # Human feedback: high -> accuracy; low -> energy efficiency
        if human_feedback > 0.5:
            return agent.accuracy
        else:
            return 1.0 - min(agent.energy_kwh * 1000 / 10.0, 1.0)

    def historical_teacher(agent):
        # Simulate a model that has learned from history
        return 0.7 * agent.accuracy + 0.3 * (1.0 - min(agent.energy_kwh * 1000 / 10.0, 1.0))

    # MoE gating weights (learned or fixed for demo)
    gate_weights = np.array([0.4, 0.4, 0.2])

    def moe_score(agent):
        scores = np.array([rule_teacher(agent), rlhf_teacher(agent), historical_teacher(agent)])
        return np.dot(gate_weights, scores)

    # 5. Combine MODP and MoE scores for final ranking
    print("\n📊 Enhanced agent ranking (MODP + MoE):")
    agent_scores = {}
    for agent in agents:
        # Normalize metrics for MODP
        acc = agent.accuracy
        energy = 1.0 - min(agent.energy_kwh * 1000 / 10.0, 1.0)
        carbon = 1.0 - min(agent.carbon_co2e_kg * 1000 / 2.0, 1.0)
        latency = 1.0 - min(agent.latency_ms / 500.0, 1.0)
        memory = 1.0 - min(agent.memory_mb / 2048.0, 1.0)
        depth = 1.0 - min(agent.circuit_depth / 50.0, 1.0)
        variance = 1.0 - agent.variance_score
        vec = np.array([acc, energy, carbon, latency, memory, depth, variance])
        modp_score = np.dot(best_weights, vec)
        moe = moe_score(agent)
        final_score = 0.6 * modp_score + 0.4 * moe
        agent_scores[agent.agent_id] = final_score
        print(f"   {agent.agent_id}: MODP={modp_score:.3f}, MoE={moe:.3f}, Final={final_score:.3f}")

    best_agent_id = max(agent_scores, key=agent_scores.get)
    print(f"\n🏆 Best agent with enhancements: {best_agent_id}")


def demo_7d_pareto_analysis():
    """Demo 1: 7-dimensional Pareto analysis"""
    print_section("DEMO 1: 7D Pareto Analysis")
    
    print("Scenario: Compare Cinebench classifiers with ALL dimensions\n")
    
    agents = [
        ExtendedParetoPoint(
            agent_id='ResNet50',
            accuracy=0.94,
            energy_kwh=0.008,
            carbon_co2e_kg=0.0016,
            latency_ms=350,
            memory_mb=512,
            circuit_depth=0,
            variance_score=0.05
        ),
        ExtendedParetoPoint(
            agent_id='EfficientNet',
            accuracy=0.92,
            energy_kwh=0.003,
            carbon_co2e_kg=0.0006,
            latency_ms=180,
            memory_mb=256,
            circuit_depth=0,
            variance_score=0.08
        ),
        ExtendedParetoPoint(
            agent_id='MobileNet',
            accuracy=0.86,
            energy_kwh=0.001,
            carbon_co2e_kg=0.0002,
            latency_ms=80,
            memory_mb=128,
            circuit_depth=0,
            variance_score=0.12
        ),
        ExtendedParetoPoint(
            agent_id='QuantumHybrid',
            accuracy=0.91,
            energy_kwh=0.004,
            carbon_co2e_kg=0.0008,
            latency_ms=500,
            memory_mb=384,
            circuit_depth=25,
            variance_score=0.25
        ),
        ExtendedParetoPoint(
            agent_id='FastButUnstable',
            accuracy=0.89,
            energy_kwh=0.005,
            carbon_co2e_kg=0.0010,
            latency_ms=100,
            memory_mb=300,
            circuit_depth=0,
            variance_score=0.35
        ),
        ExtendedParetoPoint(
            agent_id='MemoryHog',
            accuracy=0.96,
            energy_kwh=0.010,
            carbon_co2e_kg=0.0020,
            latency_ms=400,
            memory_mb=2048,
            circuit_depth=0,
            variance_score=0.06
        )
    ]
    
    analyzer = ExtendedParetoAnalyzer()
    frontier = analyzer.compute_frontier(agents)
    
    print("📊 7D Pareto Frontier Results:")
    print(f"   Total agents: {len(agents)}")
    print(f"   Frontier size: {len(frontier)}")
    print(f"\n✨ Agents on 7D frontier:")
    for agent in frontier:
        print(f"      • {agent.agent_id}")
    
    print(f"\n❌ Dominated agents:")
    frontier_ids = {a.agent_id for a in frontier}
    dominated = [a for a in agents if a.agent_id not in frontier_ids]
    for agent in dominated:
        print(f"      • {agent.agent_id}")
    
    return agents, frontier, analyzer


def demo_memory_analysis(agents, analyzer):
    """Demo 2: Memory constraint analysis"""
    print_section("DEMO 2: Memory Footprint Analysis")
    
    print("Scenario: Edge device with 512 MB RAM limit\n")
    
    memory_analysis = analyzer.analyze_memory_constraint(agents, max_memory_mb=512)
    
    print(f"Memory Constraint: {memory_analysis['max_memory_mb']} MB\n")
    
    print(f"✅ Feasible agents ({memory_analysis['feasible_count']}):")
    for agent in memory_analysis['feasible']:
        eff = memory_analysis['memory_efficiency'][agent.agent_id]
        print(f"   • {agent.agent_id}: {agent.memory_mb:.0f} MB "
              f"(efficiency: {eff:.4f} accuracy/MB)")
    
    print(f"\n❌ Infeasible agents ({memory_analysis['infeasible_count']}):")
    for agent in memory_analysis['infeasible']:
        print(f"   • {agent.agent_id}: {agent.memory_mb:.0f} MB - TOO LARGE FOR EDGE!")
    
    print(f"\n🏆 Most memory-efficient: {memory_analysis['best_memory_efficient']}")
    
    print(f"\n📊 Frontier of feasible agents: {len(memory_analysis['frontier_feasible'])}")
    for agent in memory_analysis['frontier_feasible']:
        print(f"   • {agent.agent_id} - Deployable on edge with "
              f"{agent.accuracy:.1%} accuracy")
    
    print("\n💡 Key Insight:")
    print("   Memory is a HARD constraint on edge devices.")
    print("   High-accuracy agents may be useless if they don't fit in RAM!")


def demo_circuit_depth_analysis(agents, analyzer):
    """Demo 3: Quantum circuit depth analysis"""
    print_section("DEMO 3: Quantum Circuit Depth Analysis")
    
    print("Scenario: Analyze quantum/hybrid agent scalability\n")
    
    circuit_analysis = analyzer.analyze_circuit_depth_scalability(agents)
    
    if circuit_analysis.get('quantum_agents_count', 0) == 0:
        print("⚠️  No quantum agents in this batch (all classical)")
        return
    
    print(f"🔬 Quantum Agents: {circuit_analysis['quantum_agents_count']}")
    print(f"\n📊 Circuit Depth Statistics:")
    stats = circuit_analysis['depth_stats']
    print(f"   Mean: {stats['mean']:.1f}")
    print(f"   Median: {stats['median']:.1f}")
    print(f"   Range: {stats['min']:.0f} - {stats['max']:.0f}")
    print(f"   Std Dev: {stats['std']:.1f}")
    
    print(f"\n📈 Correlations:")
    corr = circuit_analysis['correlations']
    print(f"   Accuracy vs Depth: {corr['accuracy_vs_depth']:.3f}")
    print(f"   Energy vs Depth: {corr['energy_vs_depth']:.3f}")
    
    print(f"\n🌟 Shallow Circuit Agents (depth < median):")
    for agent_id in circuit_analysis['shallow_circuit_agents']:
        print(f"   • {agent_id} - Better scalability")
    
    print(f"\n⚠️  Fragility Scores (depth/accuracy - lower is better):")
    fragility = circuit_analysis['fragility_scores']
    sorted_fragility = sorted(fragility.items(), key=lambda x: x[1])
    for agent_id, score in sorted_fragility:
        print(f"   • {agent_id}: {score:.2f}")
    
    print(f"\n🏆 Most robust: {circuit_analysis['most_robust']}")
    print(f"⚠️  Most fragile: {circuit_analysis['most_fragile']}")
    
    print("\n💡 Key Insight:")
    print("   Circuit depth predicts quantum noise and decoherence.")
    print("   Shallow circuits are more robust and scalable!")


def demo_variance_stability(agents, analyzer):
    """Demo 4: Inference variance analysis"""
    print_section("DEMO 4: Inference Variance & Stability")
    
    print("Scenario: Production deployment requires predictability\n")
    
    variance_analysis = analyzer.analyze_variance_stability(
        agents,
        stability_threshold=0.2
    )
    
    print(f"Stability Threshold: {variance_analysis['stability_threshold']}\n")
    
    print(f"✅ Stable agents ({variance_analysis['stable_count']}):")
    for agent in variance_analysis['stable']:
        cost = variance_analysis['variance_cost'][agent.agent_id]
        print(f"   • {agent.agent_id}: σ={agent.variance_score:.3f} "
              f"(P95 energy cost: +{cost*1000:.2f} Wh)")
    
    print(f"\n⚠️  Unstable agents ({variance_analysis['unstable_count']}):")
    for agent in variance_analysis['unstable']:
        cost = variance_analysis['variance_cost'][agent.agent_id]
        print(f"   • {agent.agent_id}: σ={agent.variance_score:.3f} - RISKY! "
              f"(P95 energy cost: +{cost*1000:.2f} Wh)")
    
    print(f"\n📊 Stability Ranking (most → least stable):")
    for i, agent_id in enumerate(variance_analysis['stability_ranking'][:3]):
        print(f"   {i+1}. {agent_id}")
    
    print(f"\n🏆 Most stable: {variance_analysis['most_stable']}")
    print(f"⚠️  Least stable: {variance_analysis['least_stable']}")
    
    print("\n💡 Key Insight:")
    print("   High variance = unpredictable energy spikes")
    print("   → Breaks SLAs, violates carbon caps intermittently")
    print("   → Less green in practice!")


def demo_comprehensive_analysis(agents, analyzer):
    """Demo 5: Comprehensive analysis with all constraints"""
    print_section("DEMO 5: Comprehensive 7D Analysis with Constraints")
    
    print("Scenario: Production deployment with ALL constraints\n")
    
    constraints = {
        'max_memory_mb': 512,
        'max_circuit_depth': 50,
        'max_variance': 0.2
    }
    
    print("🎯 Deployment Constraints:")
    for key, value in constraints.items():
        print(f"   • {key}: {value}")
    
    comprehensive = analyzer.comprehensive_analysis(agents, constraints)
    
    print(f"\n📊 Analysis Results:")
    print(f"   Total agents: {comprehensive['total_agents']}")
    print(f"   7D frontier: {comprehensive['frontier_7d_count']} agents")
    print(f"   Fully compliant: {comprehensive['fully_compliant_count']} agents")
    print(f"   Compliant frontier: {comprehensive['frontier_compliant_count']} agents")
    
    if comprehensive['recommendation']:
        print(f"\n🏆 RECOMMENDED FOR PRODUCTION:")
        print(f"   {comprehensive['recommendation']}")
        print(f"   → Satisfies ALL constraints")
        print(f"   → On Pareto frontier")
        print(f"   → Production-ready!")
    else:
        print(f"\n❌ NO AGENT SATISFIES ALL CONSTRAINTS!")
        print(f"   → Relax constraints or redesign agents")
    
    print("\n💡 Key Insight:")
    print("   Real deployment has MULTIPLE hard constraints.")
    print("   7D Pareto analysis finds agents that satisfy them all!")


def demo_specialized_plots(agents, frontier):
    """Demo 6: Three specialized 2D plots"""
    print_section("DEMO 6: Specialized 2D Policy Plots")
    
    print("Why multiple 2D plots instead of one 7D plot?\n")
    print("   • Humans cannot reason in 7D")
    print("   • Each 2D plot answers a different policy question")
    print("   • Projections reveal different trade-offs\n")
    
    try:
        plotter = ParetoPlotter(backend='plotly')
        
        print("📊 Generating three specialized plots...\n")
        
        print("1️⃣  Accuracy vs Carbon")
        print("   Question: 'What performance per unit environmental cost?'")
        print("   Users: Sustainability reviewers, ESG officers")
        fig1 = plotter.plot_accuracy_vs_carbon(agents, frontier, save_path='accuracy_vs_carbon.html')
        print("   ✅ Saved to: accuracy_vs_carbon.html\n")
        
        print("2️⃣  Latency vs Energy")
        print("   Question: 'Are fast agents inherently wasteful?'")
        print("   Users: Systems engineers, edge teams")
        fig2 = plotter.plot_latency_vs_energy(agents, frontier, save_path='latency_vs_energy.html')
        print("   ✅ Saved to: latency_vs_energy.html\n")
        
        print("3️⃣  Carbon vs Energy (Pure Green!)")
        print("   Question: 'Which agents are environmentally efficient?'")
        print("   Users: Green AI researchers, carbon planners")
        fig3 = plotter.plot_carbon_vs_energy(agents, frontier, save_path='carbon_vs_energy.html')
        print("   ✅ Saved to: carbon_vs_energy.html\n")
        
        print("🎨 All plots saved! Open in browser to interact.\n")
        
        print("💡 Key Insight:")
        print("   Each plot shows a different 'face' of the Pareto frontier.")
        print("   An agent can look excellent in one projection,")
        print("   but poor in another - that's not a bug, it's insight!")
        
    except ImportError as e:
        print(f"⚠️  Visualization requires plotly:")
        print(f"   pip install plotly")
        print(f"\n   Error: {e}")


def main():
    parser = argparse.ArgumentParser(description="Extended 7D Pareto Analysis Demo")
    parser.add_argument("--enhanced", action="store_true",
                        help="Include enhanced demo (Demo 7) with LIMIT Graph, MODP, RLHF, etc.")
    args = parser.parse_args()

    print("\n" + "🌟" * 35)
    print("Green_Agent: Extended 7D Pareto Analysis")
    print("🌟" * 35)
    
    print("\nNew Dimensions:")
    print("  1. Memory Footprint (MB) - Edge deployment constraints")
    print("  2. Circuit Depth - Quantum scalability/fragility")
    print("  3. Variance Score - Stability/predictability")
    print("\nNew Plots:")
    print("  1. Accuracy vs Carbon - Sustainability view")
    print("  2. Latency vs Energy - Systems engineering view")
    print("  3. Carbon vs Energy - Pure green efficiency\n")
    
    input("Press Enter to start demos...")
    
    agents, frontier, analyzer = demo_7d_pareto_analysis()
    input("\nPress Enter for next demo...")
    
    demo_memory_analysis(agents, analyzer)
    input("\nPress Enter for next demo...")
    
    demo_circuit_depth_analysis(agents, analyzer)
    input("\nPress Enter for next demo...")
    
    demo_variance_stability(agents, analyzer)
    input("\nPress Enter for next demo...")
    
    demo_comprehensive_analysis(agents, analyzer)
    input("\nPress Enter for final demo...")
    
    demo_specialized_plots(agents, frontier)
    
    if args.enhanced:
        demo_enhanced_analysis_with_advanced_techniques(agents, analyzer)
    
    print("\n" + "=" * 70)
    print("✅ All Demos Complete!")
    print("=" * 70)
    
    print("\n📈 Key Takeaways:")
    print("   1. Memory is a HARD constraint for edge deployment")
    print("   2. Circuit depth predicts quantum fragility")
    print("   3. Variance matters - unpredictable = less green")
    print("   4. Multiple 2D plots > one 7D plot for policy decisions")
    print("   5. Each dimension captures different failure modes\n")
    
    if args.enhanced:
        print("   6. Advanced techniques (MODP, RLHF, distillation, etc.) improve ranking")
        print("      by accounting for context and human preferences.\n")
    
    print("Next steps:")
    print("   • Open the HTML plots in your browser")
    print("   • Integrate with your Cinebench pipeline")
    print("   • Test with real quantum/hybrid agents")
    print("   • Use for AgentBeats submission\n")


if __name__ == '__main__':
    main()
