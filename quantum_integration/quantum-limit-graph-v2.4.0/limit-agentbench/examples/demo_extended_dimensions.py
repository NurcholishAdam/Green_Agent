"""
Demo: Extended 7D Pareto Analysis with Memory, Circuit Depth, Variance

Showcases:
1. Memory footprint analysis (edge deployment constraints)
2. Quantum circuit depth (scalability/fragility)
3. Inference variance (stability under repeated execution)
4. Three specialized 2D plots (policy-oriented visualizations)

Run with: python examples/demo_extended_dimensions.py
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from analysis.extended_pareto_analyzer import ExtendedParetoPoint, ExtendedParetoAnalyzer
from visualization.pareto_plotter import ParetoPlotter


def print_section(title):
    """Print formatted section header"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70 + "\n")


def demo_7d_pareto_analysis():
    """Demo 1: 7-dimensional Pareto analysis"""
    print_section("DEMO 1: 7D Pareto Analysis")
    
    print("Scenario: Compare Cinebench classifiers with ALL dimensions\n")
    
    # Create agents with realistic extended dimensions
    agents = [
        # ResNet50: High accuracy, high resource usage
        ExtendedParetoPoint(
            agent_id='ResNet50',
            accuracy=0.94,
            energy_kwh=0.008,
            carbon_co2e_kg=0.0016,
            latency_ms=350,
            memory_mb=512,         # Large memory footprint
            circuit_depth=0,       # Classical (no quantum)
            variance_score=0.05    # Very stable
        ),
        
        # EfficientNet: Balanced
        ExtendedParetoPoint(
            agent_id='EfficientNet',
            accuracy=0.92,
            energy_kwh=0.003,
            carbon_co2e_kg=0.0006,
            latency_ms=180,
            memory_mb=256,         # Medium memory
            circuit_depth=0,
            variance_score=0.08    # Stable
        ),
        
        # MobileNet: Efficient, lower accuracy
        ExtendedParetoPoint(
            agent_id='MobileNet',
            accuracy=0.86,
            energy_kwh=0.001,
            carbon_co2e_kg=0.0002,
            latency_ms=80,
            memory_mb=128,         # Small memory (edge-ready!)
            circuit_depth=0,
            variance_score=0.12    # Moderate stability
        ),
        
        # QuantumHybrid: Experimental quantum-enhanced
        ExtendedParetoPoint(
            agent_id='QuantumHybrid',
            accuracy=0.91,
            energy_kwh=0.004,
            carbon_co2e_kg=0.0008,
            latency_ms=500,
            memory_mb=384,
            circuit_depth=25,      # Quantum circuit!
            variance_score=0.25    # Less stable (quantum noise)
        ),
        
        # FastButUnstable: High variance
        ExtendedParetoPoint(
            agent_id='FastButUnstable',
            accuracy=0.89,
            energy_kwh=0.005,
            carbon_co2e_kg=0.0010,
            latency_ms=100,
            memory_mb=300,
            circuit_depth=0,
            variance_score=0.35    # Unstable!
        ),
        
        # MemoryHog: Uses too much RAM
        ExtendedParetoPoint(
            agent_id='MemoryHog',
            accuracy=0.96,
            energy_kwh=0.010,
            carbon_co2e_kg=0.0020,
            latency_ms=400,
            memory_mb=2048,        # Huge memory!
            circuit_depth=0,
            variance_score=0.06
        )
    ]
    
    # Compute 7D frontier
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
    
    # Analyze with 512 MB constraint (typical edge device)
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
        'max_memory_mb': 512,      # Edge device limit
        'max_circuit_depth': 50,   # Quantum noise limit
        'max_variance': 0.2        # Stability requirement
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
        
        # Plot 1: Accuracy vs Carbon
        print("1️⃣  Accuracy vs Carbon")
        print("   Question: 'What performance per unit environmental cost?'")
        print("   Users: Sustainability reviewers, ESG officers")
        fig1 = plotter.plot_accuracy_vs_carbon(
            agents, frontier,
            save_path='accuracy_vs_carbon.html'
        )
        print("   ✅ Saved to: accuracy_vs_carbon.html\n")
        
        # Plot 2: Latency vs Energy
        print("2️⃣  Latency vs Energy")
        print("   Question: 'Are fast agents inherently wasteful?'")
        print("   Users: Systems engineers, edge teams")
        fig2 = plotter.plot_latency_vs_energy(
            agents, frontier,
            save_path='latency_vs_energy.html'
        )
        print("   ✅ Saved to: latency_vs_energy.html\n")
        
        # Plot 3: Carbon vs Energy (Pure Green!)
        print("3️⃣  Carbon vs Energy (Pure Green Plot)")
        print("   Question: 'Which agents are environmentally efficient?'")
        print("   Users: Green AI researchers, carbon planners")
        fig3 = plotter.plot_carbon_vs_energy(
            agents, frontier,
            save_path='carbon_vs_energy.html'
        )
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
    """Run all demos"""
    print("\n" + "🌟"*35)
    print("Green_Agent: Extended 7D Pareto Analysis")
    print("🌟"*35)
    
    print("\nNew Dimensions:")
    print("  1. Memory Footprint (MB) - Edge deployment constraints")
    print("  2. Circuit Depth - Quantum scalability/fragility")
    print("  3. Variance Score - Stability/predictability")
    print("\nNew Plots:")
    print("  1. Accuracy vs Carbon - Sustainability view")
    print("  2. Latency vs Energy - Systems engineering view")
    print("  3. Carbon vs Energy - Pure green efficiency\n")
    
    input("Press Enter to start demos...")
    
    # Run all demos
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
    
    print("\n" + "="*70)
    print("✅ All Demos Complete!")
    print("="*70)
    
    print("\n📈 Key Takeaways:")
    print("   1. Memory is a HARD constraint for edge deployment")
    print("   2. Circuit depth predicts quantum fragility")
    print("   3. Variance matters - unpredictable = less green")
    print("   4. Multiple 2D plots > one 7D plot for policy decisions")
    print("   5. Each dimension captures different failure modes\n")
    
    print("Next steps:")
    print("   • Open the HTML plots in your browser")
    print("   • Integrate with your Cinebench pipeline")
    print("   • Test with real quantum/hybrid agents")
    print("   • Use for AgentBeats submission\n")


if __name__ == '__main__':
    main()
