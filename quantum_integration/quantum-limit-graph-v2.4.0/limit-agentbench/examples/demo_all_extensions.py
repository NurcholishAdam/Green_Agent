"""
Comprehensive Demo: All Green_Agent Extensions

Demonstrates:
1. Task Complexity Normalization
2. Budget Constraints
3. RLHF Reward Shaping
4. Multi-Layer Reporting

Enhanced with optional advanced techniques (DEMO 7):
- LIMIT Graph metrics
- MODP (Multi-Objective Decision Process)
- RLHF (human feedback)
- Multi-Teacher On-Policy Distillation + MoE gating
- Bio-inspired (evolutionary) optimisation

Run with: python examples/demo_all_extensions.py [--enhanced]
"""

import sys
import argparse
from pathlib import Path
import asyncio
import random
import numpy as np

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from metrics.efficiency_calculator import NormalizedEfficiencyCalculator
from constraints.budget_manager import Budget, BudgetManager
from constraints.budget_enforcer import BudgetEnforcer
from rlhf.reward_shaper import ExecutionMode, RewardShaper
from rlhf.policy_evaluator import PolicyEvaluationEnvironment
from reporting.layered_reporter import LayeredReporter
from reporting.report_generator import ReportGenerator


def print_section(title):
    """Print section header"""
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70 + "\n")


# ============================================================================
# DEMO 1: Task Complexity Normalization (unchanged)
# ============================================================================

def demo_complexity_normalization():
    print_section("DEMO 1: Task Complexity Normalization")
    
    print("Scenario: Compare Cinebench classifiers on different task complexities\n")
    
    results = [
        {
            'agent_id': 'ResNet50',
            'task_id': 'simple_classification',
            'accuracy': 0.90,
            'energy_kwh': 0.002,
            'carbon_kg': 0.0004,
            'latency_ms': 100,
            'trace': {
                'prompt': 'Classify this simple Cinebench score',
                'reasoning': ['Load model', 'Classify'],
                'tool_calls': [],
                'execution_time_ms': 100,
                'context_tokens': 50
            }
        },
        {
            'agent_id': 'ResNet50',
            'task_id': 'complex_classification',
            'accuracy': 0.95,
            'energy_kwh': 0.010,
            'carbon_kg': 0.0020,
            'latency_ms': 500,
            'trace': {
                'prompt': 'Analyze this complex Cinebench benchmark with multiple metrics...',
                'reasoning': [f'Step {i}' for i in range(10)],
                'tool_calls': [{'tool': 'benchmark_db'}, {'tool': 'specs_api'}],
                'execution_time_ms': 500,
                'context_tokens': 500
            }
        }
    ]
    
    print("❌ WITHOUT Complexity Normalization:")
    print(f"  Simple task: {results[0]['energy_kwh']:.4f} kWh - looks 'efficient'")
    print(f"  Complex task: {results[1]['energy_kwh']:.4f} kWh - looks 'wasteful'")
    print(f"  Conclusion: Simple task seems 5x better\n")
    
    calculator = NormalizedEfficiencyCalculator()
    comparison = calculator.compare_across_complexities(results)
    
    print("✅ WITH Complexity Normalization:")
    for ranking in comparison['rankings']:
        print(f"\n  Task: {ranking['task_id']}")
        print(f"    Raw Energy: {ranking['energy_kwh']:.4f} kWh")
        print(f"    Task Complexity: {ranking['task_complexity']:.2f}")
        print(f"    Energy Efficiency: {ranking['energy_efficiency']:.6f}")
        print(f"    → Fair comparison enabled!")
    
    print("\n📊 Summary:")
    print(f"  Avg Energy Efficiency: {comparison['summary']['avg_energy_efficiency']:.6f}")
    print(f"  Avg Accuracy/Watt: {comparison['summary']['avg_accuracy_per_watt']:.2f}")


# ============================================================================
# DEMO 2: Budget Constraints (unchanged)
# ============================================================================

async def demo_budget_constraints():
    print_section("DEMO 2: Budget Constraints")
    
    print("Scenario: Deploy Cinebench classifier with strict energy budget\n")
    
    budget = Budget.eco_budget()
    print(f"Budget: {budget.name}")
    print(f"  Max Energy: {budget.max_energy_wh} Wh")
    print(f"  Max Carbon: {budget.max_carbon_g} g CO₂")
    print(f"  Max Latency: {budget.max_latency_ms} ms\n")
    
    enforcer = BudgetEnforcer(budget)
    
    async def classifier_agent(task):
        return {
            'output': 'classification_result',
            'accuracy': 0.93,
            'metrics': {
                'energy_kwh': 0.003,
                'carbon_kg': 0.0006,
                'latency_ms': 300
            }
        }
    
    print("Executing classifier within budget...\n")
    
    task = {'input': 'benchmark_data'}
    result = await enforcer.execute_with_budget(
        agent_fn=classifier_agent,
        task=task,
        estimated_consumption={
            'energy_wh': 3.0,
            'carbon_g': 0.6,
            'latency_ms': 300
        }
    )
    
    if result['success']:
        print("✅ Execution SUCCESS!")
        print(f"  Actual Energy: {result['actual_consumption']['energy_wh']:.2f} Wh")
        print(f"  Remaining Budget: {result['remaining_budget']['energy_wh']:.2f} Wh")
    else:
        print("❌ Execution BLOCKED!")
        print(f"  Violations: {result['violations']}")
    
    print("\n📊 Budget Report:")
    report = enforcer.get_budget_report()
    util = report['utilization']
    print(f"  Energy Used: {util['energy_wh']:.1%}")
    print(f"  Carbon Used: {util['carbon_g']:.1%}")


# ============================================================================
# DEMO 3: RLHF Reward Shaping (unchanged)
# ============================================================================

def demo_rlhf_reward_shaping():
    print_section("DEMO 3: RLHF Reward Shaping")
    
    print("Scenario: Compare agents across different execution modes\n")
    
    agent_results = [
        {
            'agent_id': 'HighAccuracy_Agent',
            'task_success': 0.95,
            'energy_kwh': 0.010,
            'carbon_kg': 0.0020,
            'latency_ms': 500
        },
        {
            'agent_id': 'Efficient_Agent',
            'task_success': 0.88,
            'energy_kwh': 0.002,
            'carbon_kg': 0.0004,
            'latency_ms': 150
        },
        {
            'agent_id': 'Fast_Agent',
            'task_success': 0.90,
            'energy_kwh': 0.005,
            'carbon_kg': 0.0010,
            'latency_ms': 80
        }
    ]
    
    modes = [ExecutionMode.ECO_MODE, ExecutionMode.FAST_MODE, ExecutionMode.ACCURACY_MODE]
    
    for mode in modes:
        shaper = RewardShaper(mode)
        comparison = shaper.compare_policies(agent_results)
        
        print(f"\n🎯 {mode.value.upper()} MODE:")
        print(f"   Best Agent: {comparison['best_agent']}")
        
        for rank in comparison['rankings'][:2]:
            print(f"\n   #{rank['rank']} {rank['agent_id']}")
            print(f"      Reward: {rank['reward']:.3f}")
            print(f"      Success: {rank['raw_metrics']['task_success']:.2%}")
            print(f"      Energy: {rank['raw_metrics']['energy_kwh']:.4f} kWh")


# ============================================================================
# DEMO 4: Policy Evaluation Environment (unchanged)
# ============================================================================

def demo_policy_evaluation():
    print_section("DEMO 4: Policy Evaluation Environment")
    
    print("Scenario: Evaluate agent policy across all modes\n")
    
    def my_classifier(task):
        return {
            'accuracy': 0.92,
            'energy_kwh': 0.003,
            'carbon_kg': 0.0006,
            'latency_ms': 200
        }
    
    tasks = [{'task_id': f'task_{i}'} for i in range(10)]
    
    env = PolicyEvaluationEnvironment()
    results = env.multi_mode_evaluation(my_classifier, tasks, verbose=False)
    
    print("Results across all execution modes:\n")
    
    for mode, eval_result in results['evaluations'].items():
        print(f"  {mode.upper()}:")
        print(f"    Avg Reward: {eval_result['avg_reward']:.3f}")
        print(f"    Avg Success: {eval_result['avg_task_success']:.2%}")
        print(f"    Total Energy: {eval_result['summary']['total_energy_kwh']:.4f} kWh")
    
    print(f"\n🏆 Best Mode: {results['best_mode']}")
    print(f"\n💡 Recommendations:")
    for mode, rec in results['recommendations'].items():
        print(f"  {mode}: {rec}")


# ============================================================================
# DEMO 5: Multi-Layer Reporting (unchanged)
# ============================================================================

def demo_multi_layer_reporting():
    print_section("DEMO 5: Multi-Layer Reporting")
    
    print("Scenario: Generate transparent three-layer reports\n")
    
    results = [
        {
            'agent_id': 'Agent_A',
            'task_id': 'task_1',
            'accuracy': 0.95,
            'energy_kwh': 0.005,
            'carbon_kg': 0.0010,
            'latency_ms': 300,
            'trace': {
                'prompt': 'Classify benchmark',
                'reasoning': [f'Step {i}' for i in range(5)],
                'tool_calls': [{'tool': 'db'}],
                'execution_time_ms': 300,
                'context_tokens': 200
            }
        },
        {
            'agent_id': 'Agent_B',
            'task_id': 'task_2',
            'accuracy': 0.88,
            'energy_kwh': 0.002,
            'carbon_kg': 0.0004,
            'latency_ms': 150,
            'trace': {
                'prompt': 'Classify benchmark',
                'reasoning': ['Step 1', 'Step 2'],
                'tool_calls': [],
                'execution_time_ms': 150,
                'context_tokens': 100
            }
        }
    ]
    
    reporter = LayeredReporter()
    full_report = reporter.generate_full_report(results, scenario='production')
    
    print("Three-Layer Report Generated:\n")
    
    for agent_report in full_report['reports']:
        print(f"Agent: {agent_report['agent_id']}")
        print(f"  Layer 1 (Raw): Accuracy={agent_report['layer1_raw']['accuracy']:.2%}, "
              f"Energy={agent_report['layer1_raw']['energy_wh']:.2f} Wh")
        print(f"  Layer 2 (Normalized): Energy/Task={agent_report['layer2_normalized']['energy_per_task']:.6f}")
        print(f"  Layer 3 (Scenario): Score={agent_report['layer3_scenario']['weighted_score']:.3f}, "
              f"Rank=#{agent_report['layer3_scenario']['rank']}\n")
    
    print("\n📄 Generating formatted reports...\n")
    
    report_gen = ReportGenerator()
    
    exec_summary = report_gen.generate_executive_summary(full_report)
    print("Executive Summary:")
    print(exec_summary[:500] + "...\n")
    
    print("✅ Full technical and research reports also available")


# ============================================================================
# DEMO 6: Cinebench Integration (All Modules Combined) - unchanged
# ============================================================================

async def demo_cinebench_integration():
    print_section("DEMO 6: Cinebench Integration (All Modules Combined)")
    
    print("Scenario: Complete Cinebench classifier evaluation workflow\n")
    
    budget = Budget(
        max_energy_wh=50.0,
        max_carbon_g=10.0,
        max_latency_ms=5000,
        name="Cinebench Production Budget"
    )
    
    classifiers_results = [
        {
            'agent_id': 'ResNet50',
            'accuracy': 0.94,
            'energy_kwh': 0.008,
            'carbon_kg': 0.0016,
            'latency_ms': 350,
            'task_success': 0.94,
            'trace': {
                'prompt': 'Cinebench classification',
                'reasoning': [f'Step {i}' for i in range(5)],
                'tool_calls': [{'tool': 'vision'}],
                'execution_time_ms': 350,
                'context_tokens': 300
            }
        },
        {
            'agent_id': 'EfficientNet',
            'accuracy': 0.92,
            'energy_kwh': 0.003,
            'carbon_kg': 0.0006,
            'latency_ms': 180,
            'task_success': 0.92,
            'trace': {
                'prompt': 'Cinebench classification',
                'reasoning': [f'Step {i}' for i in range(3)],
                'tool_calls': [{'tool': 'vision'}],
                'execution_time_ms': 180,
                'context_tokens': 200
            }
        },
        {
            'agent_id': 'MobileNet',
            'accuracy': 0.86,
            'energy_kwh': 0.001,
            'carbon_kg': 0.0002,
            'latency_ms': 80,
            'task_success': 0.86,
            'trace': {
                'prompt': 'Cinebench classification',
                'reasoning': ['Quick check'],
                'tool_calls': [],
                'execution_time_ms': 80,
                'context_tokens': 100
            }
        }
    ]
    
    print("Step 1: Normalize by Complexity")
    print("-" * 40)
    calculator = NormalizedEfficiencyCalculator()
    comparison = calculator.compare_across_complexities(classifiers_results)
    
    for rank in comparison['rankings']:
        print(f"  {rank['agent_id']}: "
              f"Accuracy/Watt={rank['accuracy_per_watt']:.2f}, "
              f"Efficiency={rank['composite_efficiency']:.3f}")
    
    print(f"\n  Best Normalized: {comparison['summary']['best_agent']}")
    
    print("\nStep 2: Check Budget Compliance")
    print("-" * 40)
    
    for result in classifiers_results:
        energy_wh = result['energy_kwh'] * 1000
        carbon_g = result['carbon_kg'] * 1000
        
        fits_budget = (energy_wh <= budget.max_energy_wh and 
                      carbon_g <= budget.max_carbon_g and
                      result['latency_ms'] <= budget.max_latency_ms)
        
        status = "✅" if fits_budget else "❌"
        print(f"  {status} {result['agent_id']}: "
              f"E={energy_wh:.1f}Wh, C={carbon_g:.1f}g, L={result['latency_ms']:.0f}ms")
    
    print("\nStep 3: RLHF Mode Selection")
    print("-" * 40)
    
    eco_shaper = RewardShaper(ExecutionMode.ECO_MODE)
    eco_comparison = eco_shaper.compare_policies(classifiers_results)
    
    fast_shaper = RewardShaper(ExecutionMode.FAST_MODE)
    fast_comparison = fast_shaper.compare_policies(classifiers_results)
    
    print(f"  Eco Mode Best: {eco_comparison['best_agent']}")
    print(f"  Fast Mode Best: {fast_comparison['best_agent']}")
    
    print("\nStep 4: Generate Multi-Layer Report")
    print("-" * 40)
    
    reporter = LayeredReporter()
    full_report = reporter.generate_full_report(classifiers_results, 'production')
    
    print(f"  Top Ranked: {full_report['reports'][0]['agent_id']}")
    print(f"  L3 Score: {full_report['reports'][0]['layer3_scenario']['weighted_score']:.3f}")
    
    print("\n🎯 FINAL RECOMMENDATION:")
    print("-" * 40)
    print(f"  Production Deployment: {full_report['reports'][0]['agent_id']}")
    print(f"  Reason: Best balance of accuracy, efficiency, and budget compliance")
    print(f"  Accuracy: {full_report['reports'][0]['layer1_raw']['accuracy']:.1%}")
    print(f"  Energy: {full_report['reports'][0]['layer1_raw']['energy_wh']:.2f} Wh")
    print(f"  Within Budget: ✅")


# ============================================================================
# DEMO 7: Advanced Techniques (LIMIT Graph, MODP, RLHF, Distillation, MoE, Bio-inspired)
# ============================================================================

def demo_advanced_techniques():
    print_section("DEMO 7: Advanced Techniques Integration")
    
    # 1. LIMIT Graph metrics (simulated)
    graph_metrics = {
        "centrality": 0.7,
        "connectivity": 0.6,
        "density": 0.5
    }
    print("🌐 LIMIT Graph Metrics:")
    for k, v in graph_metrics.items():
        print(f"   {k}: {v}")
    
    # 2. RLHF human feedback
    human_feedback = 0.8  # prefers accuracy
    print(f"\n👤 RLHF Feedback: {human_feedback} (prefers accuracy)")
    
    # 3. Sample agents (simplified data, similar to demo 6)
    agents = [
        {'id': 'ResNet50', 'accuracy': 0.94, 'energy_kwh': 0.008, 'carbon_kg': 0.0016, 'latency_ms': 350},
        {'id': 'EfficientNet', 'accuracy': 0.92, 'energy_kwh': 0.003, 'carbon_kg': 0.0006, 'latency_ms': 180},
        {'id': 'MobileNet', 'accuracy': 0.86, 'energy_kwh': 0.001, 'carbon_kg': 0.0002, 'latency_ms': 80},
    ]
    
    # 4. MODP: evolve weights using bio-inspired genetic algorithm
    print("\n🧬 Evolving MODP weights (genetic algorithm)...")
    n_obj = 4  # accuracy, energy, carbon, latency
    pop_size = 20
    generations = 10
    mutation_rate = 0.3
    crossover_rate = 0.7
    
    def fitness(weights):
        score = 0
        for a in agents:
            acc = a['accuracy']
            energy = 1 - min(a['energy_kwh']*1000/10.0, 1)
            carbon = 1 - min(a['carbon_kg']*1000/2.0, 1)
            latency = 1 - min(a['latency_ms']/500.0, 1)
            vec = np.array([acc, energy, carbon, latency])
            score += np.dot(weights, vec)
        return score / len(agents)
    
    population = [np.random.dirichlet(np.ones(n_obj)) for _ in range(pop_size)]
    best_weights = population[0]
    for gen in range(generations):
        scores = [fitness(w) for w in population]
        best_idx = np.argmax(scores)
        best_weights = population[best_idx]
        # create next gen
        new_pop = [best_weights]  # elitism
        while len(new_pop) < pop_size:
            p1 = population[random.randint(0, pop_size-1)]
            p2 = population[random.randint(0, pop_size-1)]
            if random.random() < crossover_rate:
                alpha = random.random()
                child = alpha * p1 + (1 - alpha) * p2
            else:
                child = p1.copy()
            child += np.random.dirichlet(np.ones(n_obj)) * mutation_rate
            child = child / child.sum()
            new_pop.append(child)
        population = new_pop
    
    print(f"   Evolved weights (accuracy, energy, carbon, latency): {np.round(best_weights, 3)}")
    
    # 5. Multi-teacher distillation with MoE gating (simplified)
    print("\n🎓 Multi-Teacher Distillation + MoE:")
    
    # Three simple teachers
    def rule_teacher(agent):
        # Prefer high accuracy if centrality high
        if graph_metrics["centrality"] > 0.5:
            return agent['accuracy']
        else:
            return 1 - min(agent['energy_kwh']*1000/10.0, 1)
    
    def rlhf_teacher(agent):
        if human_feedback > 0.5:
            return agent['accuracy']
        else:
            return 1 - min(agent['energy_kwh']*1000/10.0, 1)
    
    def historical_teacher(agent):
        return 0.7 * agent['accuracy'] + 0.3 * (1 - min(agent['energy_kwh']*1000/10.0, 1))
    
    # Gating weights (fixed for demo)
    gate_weights = np.array([0.4, 0.4, 0.2])
    
    def moe_score(agent):
        scores = np.array([rule_teacher(agent), rlhf_teacher(agent), historical_teacher(agent)])
        return np.dot(gate_weights, scores)
    
    # 6. Combine MODP and MoE scores
    print("\n📊 Final Enhanced Ranking:")
    final_scores = {}
    for a in agents:
        # MODP score
        acc = a['accuracy']
        energy = 1 - min(a['energy_kwh']*1000/10.0, 1)
        carbon = 1 - min(a['carbon_kg']*1000/2.0, 1)
        latency = 1 - min(a['latency_ms']/500.0, 1)
        vec = np.array([acc, energy, carbon, latency])
        modp = np.dot(best_weights, vec)
        moe = moe_score(a)
        final = 0.6 * modp + 0.4 * moe
        final_scores[a['id']] = final
        print(f"   {a['id']}: MODP={modp:.3f}, MoE={moe:.3f}, Final={final:.3f}")
    
    best_agent = max(final_scores, key=final_scores.get)
    print(f"\n🏆 Best Agent with Advanced Techniques: {best_agent}")


# ============================================================================
# MAIN
# ============================================================================

async def main():
    parser = argparse.ArgumentParser(description="Green_Agent Extensions Demo")
    parser.add_argument("--enhanced", action="store_true",
                        help="Include advanced techniques demo (DEMO 7)")
    args = parser.parse_args()

    print("\n" + "🌟"*35)
    print("Green_Agent Extensions - Complete Demo")
    print("🌟"*35 + "\n")
    
    print("This demo showcases all 4 extension modules:")
    print("  1. Task Complexity Normalization")
    print("  2. Budget Constraints")
    print("  3. RLHF Reward Shaping")
    print("  4. Multi-Layer Reporting")
    print("  5. Policy Evaluation")
    print("  6. Complete Cinebench Integration")
    if args.enhanced:
        print("  7. Advanced Techniques (LIMIT Graph, MODP, RLHF, Distillation, MoE, Bio-inspired)")
    
    input("Press Enter to start demos...")
    
    demo_complexity_normalization()
    input("\nPress Enter for next demo...")
    
    await demo_budget_constraints()
    input("\nPress Enter for next demo...")
    
    demo_rlhf_reward_shaping()
    input("\nPress Enter for next demo...")
    
    demo_policy_evaluation()
    input("\nPress Enter for next demo...")
    
    demo_multi_layer_reporting()
    input("\nPress Enter for next demo...")
    
    await demo_cinebench_integration()
    
    if args.enhanced:
        demo_advanced_techniques()
    
    print("\n" + "="*70)
    print("✅ All Demos Complete!")
    print("="*70)
    print("\nNext steps:")
    print("  1. Run tests: pytest tests/")
    print("  2. Integrate with your Cinebench pipeline")
    print("  3. Deploy to AgentBeats\n")


if __name__ == '__main__':
    asyncio.run(main())
