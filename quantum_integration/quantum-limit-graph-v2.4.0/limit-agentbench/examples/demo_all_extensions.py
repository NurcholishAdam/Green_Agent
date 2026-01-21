"""
Comprehensive Demo: All Green_Agent Extensions

Demonstrates:
1. Task Complexity Normalization
2. Budget Constraints
3. RLHF Reward Shaping
4. Multi-Layer Reporting

Run with: python examples/demo_all_extensions.py
"""

import sys
from pathlib import Path
import asyncio

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
# DEMO 1: Task Complexity Normalization
# ============================================================================

def demo_complexity_normalization():
    print_section("DEMO 1: Task Complexity Normalization")
    
    print("Scenario: Compare Cinebench classifiers on different task complexities\n")
    
    # Simulate results from different task complexities
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
            'energy_kwh': 0.010,  # 5x more energy
            'carbon_kg': 0.0020,
            'latency_ms': 500,
            'trace': {
                'prompt': 'Analyze this complex Cinebench benchmark with multiple metrics...',
                'reasoning': [f'Step {i}' for i in range(10)],  # More reasoning
                'tool_calls': [{'tool': 'benchmark_db'}, {'tool': 'specs_api'}],
                'execution_time_ms': 500,
                'context_tokens': 500
            }
        }
    ]
    
    # Without normalization
    print("❌ WITHOUT Complexity Normalization:")
    print(f"  Simple task: {results[0]['energy_kwh']:.4f} kWh - looks 'efficient'")
    print(f"  Complex task: {results[1]['energy_kwh']:.4f} kWh - looks 'wasteful'")
    print(f"  Conclusion: Simple task seems 5x better\n")
    
    # With normalization
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
# DEMO 2: Budget Constraints
# ============================================================================

async def demo_budget_constraints():
    print_section("DEMO 2: Budget Constraints")
    
    print("Scenario: Deploy Cinebench classifier with strict energy budget\n")
    
    # Create eco-friendly budget
    budget = Budget.eco_budget()
    print(f"Budget: {budget.name}")
    print(f"  Max Energy: {budget.max_energy_wh} Wh")
    print(f"  Max Carbon: {budget.max_carbon_g} g CO₂")
    print(f"  Max Latency: {budget.max_latency_ms} ms\n")
    
    # Create enforcer
    enforcer = BudgetEnforcer(budget)
    
    # Mock agent function
    async def classifier_agent(task):
        return {
            'output': 'classification_result',
            'accuracy': 0.93,
            'metrics': {
                'energy_kwh': 0.003,  # 3 Wh
                'carbon_kg': 0.0006,  # 0.6 g
                'latency_ms': 300
            }
        }
    
    # Execute with budget enforcement
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
    
    # Show budget report
    print("\n📊 Budget Report:")
    report = enforcer.get_budget_report()
    util = report['utilization']
    print(f"  Energy Used: {util['energy_wh']:.1%}")
    print(f"  Carbon Used: {util['carbon_g']:.1%}")


# ============================================================================
# DEMO 3: RLHF Reward Shaping
# ============================================================================

def demo_rlhf_reward_shaping():
    print_section("DEMO 3: RLHF Reward Shaping")
    
    print("Scenario: Compare agents across different execution modes\n")
    
    # Sample agent results
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
    
    # Test different modes
    modes = [ExecutionMode.ECO_MODE, ExecutionMode.FAST_MODE, ExecutionMode.ACCURACY_MODE]
    
    for mode in modes:
        shaper = RewardShaper(mode)
        comparison = shaper.compare_policies(agent_results)
        
        print(f"\n🎯 {mode.value.upper()} MODE:")
        print(f"   Best Agent: {comparison['best_agent']}")
        
        for rank in comparison['rankings'][:2]:  # Top 2
            print(f"\n   #{rank['rank']} {rank['agent_id']}")
            print(f"      Reward: {rank['reward']:.3f}")
            print(f"      Success: {rank['raw_metrics']['task_success']:.2%}")
            print(f"      Energy: {rank['raw_metrics']['energy_kwh']:.4f} kWh")


# ============================================================================
# DEMO 4: Policy Evaluation Environment
# ============================================================================

def demo_policy_evaluation():
    print_section("DEMO 4: Policy Evaluation Environment")
    
    print("Scenario: Evaluate agent policy across all modes\n")
    
    # Mock agent policy
    def my_classifier(task):
        return {
            'accuracy': 0.92,
            'energy_kwh': 0.003,
            'carbon_kg': 0.0006,
            'latency_ms': 200
        }
    
    # Create test tasks
    tasks = [{'task_id': f'task_{i}'} for i in range(10)]
    
    # Evaluate across all modes
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
# DEMO 5: Multi-Layer Reporting
# ============================================================================

def demo_multi_layer_reporting():
    print_section("DEMO 5: Multi-Layer Reporting")
    
    print("Scenario: Generate transparent three-layer reports\n")
    
    # Sample evaluation results
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
    
    # Generate full report
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
    
    # Generate formatted reports
    print("\n📄 Generating formatted reports...\n")
    
    report_gen = ReportGenerator()
    
    # Executive summary
    exec_summary = report_gen.generate_executive_summary(full_report)
    print("Executive Summary:")
    print(exec_summary[:500] + "...\n")  # First 500 chars
    
    print("✅ Full technical and research reports also available")


# ============================================================================
# DEMO 6: Cinebench Integration (All Modules Combined)
# ============================================================================

async def demo_cinebench_integration():
    print_section("DEMO 6: Cinebench Integration (All Modules Combined)")
    
    print("Scenario: Complete Cinebench classifier evaluation workflow\n")
    
    # Step 1: Define budget
    budget = Budget(
        max_energy_wh=50.0,  # 50 Wh for batch
        max_carbon_g=10.0,   # 10g CO₂
        max_latency_ms=5000, # 5s per task
        name="Cinebench Production Budget"
    )
    
    # Step 2: Mock classifiers with different characteristics
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
    
    # Test eco mode
    eco_shaper = RewardShaper(ExecutionMode.ECO_MODE)
    eco_comparison = eco_shaper.compare_policies(classifiers_results)
    
    # Test fast mode
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
# MAIN
# ============================================================================

async def main():
    """Run all demos"""
    print("\n" + "🌟"*35)
    print("Green_Agent Extensions - Complete Demo")
    print("🌟"*35 + "\n")
    
    print("This demo showcases all 4 extension modules:")
    print("  1. Task Complexity Normalization")
    print("  2. Budget Constraints")
    print("  3. RLHF Reward Shaping")
    print("  4. Multi-Layer Reporting")
    print("  5. Policy Evaluation")
    print("  6. Complete Cinebench Integration\n")
    
    input("Press Enter to start demos...")
    
    # Run demos
    demo_complexity_normalization()
    input("\nPress Enter for next demo...")
    
    await demo_budget_constraints()
    input("\nPress Enter for next demo...")
    
    demo_rlhf_reward_shaping()
    input("\nPress Enter for next demo...")
    
    demo_policy_evaluation()
    input("\nPress Enter for next demo...")
    
    demo_multi_layer_reporting()
    input("\nPress Enter for final demo...")
    
    await demo_cinebench_integration()
    
    print("\n" + "="*70)
    print("✅ All Demos Complete!")
    print("="*70)
    print("\nNext steps:")
    print("  1. Run tests: pytest tests/")
    print("  2. Integrate with your Cinebench pipeline")
    print("  3. Deploy to AgentBeats\n")


if __name__ == '__main__':
    asyncio.run(main())
